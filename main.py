"""
AstrBot 下载加速器 (astrbot_plugin_dl_accel)

接管 AstrBot 统一的下载函数 _RepoZipUpdater._download_file，让插件安装、
插件更新、AstrBot 核心更新这三条链路自动走：

    自动探测的代理  →  HTTP Range 分块并行  →  磁盘缓存

设计原则：加速器永远不能成为「装不上东西」的原因。
任何一步失败都会就地降级（分块 → 单连接 → 原生实现），
最坏情况与没装这个插件完全一致。
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import os
import shutil
import tempfile
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star, register
from astrbot.core.utils.astrbot_path import get_astrbot_data_path

try:  # 极老版本可能没有这个模块
    from astrbot.core.star.filter.command import GreedyStr
except Exception:  # pragma: no cover

    class GreedyStr(str):
        """兜底实现：把整串参数当成一个字符串。"""


PLUGIN_NAME = "astrbot_plugin_dl_accel"
PLUGIN_VERSION = "1.0.0"

# 默认接管的域名：AstrBot 官方插件市场的对象存储 + GitHub 系
DEFAULT_ALLOW_HOSTS = (
    "astrbot-cloud-test-s3.astrbot.app",
    "astrbot-plugins-s3.astrbot.app",
    "github.com",
    "codeload.github.com",
    "api.github.com",
    "objects.githubusercontent.com",
    "raw.githubusercontent.com",
    "release-assets.githubusercontent.com",
    "github-releases.githubusercontent.com",
)

# 只有这些域名才会被送进用户配置的反代前缀，避免把任意地址交给第三方
GITHUB_HOSTS = frozenset(
    {
        "github.com",
        "codeload.github.com",
        "www.github.com",
        "raw.githubusercontent.com",
        "objects.githubusercontent.com",
        "release-assets.githubusercontent.com",
        "github-releases.githubusercontent.com",
        "gist.githubusercontent.com",
    }
)

# 自动探测本机代理时扫的端口，按常见程度排序
DEFAULT_PROXY_PORTS = (7890, 7897, 10809, 10808, 1080, 2080, 6152, 8889, 33210, 8118)
# 这些端口在 Clash / V2Ray 等客户端上通常是 SOCKS5 而不是 HTTP
SOCKS_PORTS = frozenset({10808, 1080, 7891})

UA = "astrbot-plugin-installer"

# 代理探测结果的缓存时间（秒）；失败也要过期，用户可能中途才开代理
PROXY_CACHE_TTL = 300.0

# 保存原始实现，terminate() 时还原
_ORIGINAL_DOWNLOAD_FILE = None
_ACTIVE_ACCEL: "DownloadAccelerator | None" = None


def _human(size: float) -> str:
    """字节数转可读字符串。"""
    value = float(size)
    for unit in ("B", "KB", "MB", "GB"):
        if abs(value) < 1024 or unit == "GB":
            return f"{value:.1f}{unit}"
        value /= 1024
    return f"{value:.1f}GB"


def _host_of(url: str) -> str:
    try:
        return (urlparse(url).hostname or "").lower()
    except Exception:
        return ""


def _socks_available() -> bool:
    """httpx 的 socks5 支持需要 socksio。"""
    try:
        import socksio  # noqa: F401

        return True
    except Exception:
        return False


def _normalize_proxy(value: str) -> str:
    """把裸 host:port 补成完整 URL。"""
    text = str(value or "").strip()
    if not text:
        return ""
    if "://" not in text:
        text = "http://" + text
    return text


class DownloadAccelerator:
    """下载加速的核心实现。"""

    def __init__(self, config: Any = None) -> None:
        self.config: dict[str, Any] = dict(config or {})
        self.cache_dir = (
            Path(get_astrbot_data_path()) / "plugin_data" / PLUGIN_NAME / "cache"
        )

        self.enabled = bool(self.config.get("enabled", True))
        self.cache_enabled = bool(self.config.get("cache_enabled", True))
        try:
            self.cache_dir.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            # 缓存目录建不出来不该拖垮插件：降级为不缓存，只做并行加速
            logger.warning(f"[dl_accel] 缓存目录不可用，本次运行不启用缓存：{exc}")
            self.cache_enabled = False

        self.parts = max(1, min(32, int(self.config.get("parallel_parts", 8) or 8)))
        self.min_size = max(0, int(self.config.get("min_size_kb", 512) or 512)) * 1024
        self.cache_max_bytes = (
            max(0, int(self.config.get("cache_max_mb", 2048) or 2048)) * 1024 * 1024
        )
        self.allow_hosts = self._parse_hosts(self.config.get("allow_hosts"))
        self.proxy = _normalize_proxy(self.config.get("proxy", ""))
        self.auto_probe_proxy = bool(self.config.get("auto_probe_proxy", True))
        self.mirror_prefix = str(self.config.get("mirror_prefix", "") or "").strip()

        self.stat_downloads = 0
        self.stat_cache_hits = 0
        self.stat_fallbacks = 0
        self.stat_bytes = 0

        # 由插件在 initialize() 时注入，用来读 AstrBot 全局配置
        self._config_getter = None

        self._proxy_ok: bool | None = None
        self._proxy_checked_at = 0.0
        self._probed: str | None = None  # 探测到的代理；"" 表示确认没有
        self._probed_at = 0.0
        self._probe_lock: asyncio.Lock | None = None

    # ---------------- 配置 ----------------

    @staticmethod
    def _parse_hosts(raw: Any) -> set[str]:
        if isinstance(raw, (list, tuple)):
            items = [str(x) for x in raw]
        else:
            items = str(raw or "").replace("，", ",").replace("\n", ",").split(",")
        hosts = {i.strip().lower() for i in items if str(i).strip()}
        return hosts or set(DEFAULT_ALLOW_HOSTS)

    def should_handle(self, url: str) -> bool:
        if not self.enabled:
            return False
        if not url.lower().startswith(("http://", "https://")):
            return False
        return _host_of(url) in self.allow_hosts

    def _mirror_url(self, url: str) -> str | None:
        """把 GitHub 地址套到用户配置的加速前缀上；不适用返回 None。"""
        if not self.mirror_prefix:
            return None
        if _host_of(url) not in GITHUB_HOSTS:
            return None
        prefix = self.mirror_prefix.rstrip("/")
        if not prefix.lower().startswith(("http://", "https://")):
            return None
        return f"{prefix}/{url}"

    # ---------------- 代理 ----------------

    def _env_proxy(self) -> str:
        for key in ("https_proxy", "HTTPS_PROXY", "http_proxy", "HTTP_PROXY"):
            value = os.environ.get(key, "").strip()
            if value:
                return _normalize_proxy(value)
        return ""

    def _global_proxy(self) -> str:
        """读 AstrBot 全局「HTTP 代理」设置，跟随使用者的既有配置。"""
        getter = self._config_getter
        if getter is None:
            return ""
        try:
            config = getter()
        except Exception:
            return ""
        if config is None:
            return ""
        try:
            value = config.get("http_proxy", "")
        except Exception:
            try:
                value = config["http_proxy"]
            except Exception:
                return ""
        return _normalize_proxy(value)

    @staticmethod
    async def _tcp_open(port: int, timeout: float = 0.35) -> bool:
        try:
            _, writer = await asyncio.wait_for(
                asyncio.open_connection("127.0.0.1", port), timeout=timeout
            )
        except Exception:
            return False
        try:
            writer.close()
        except Exception:
            pass
        return True

    async def _verify_proxy(self, proxy: str) -> bool:
        """用一个极小的请求确认它真的能出网，而不是随便一个监听端口。"""
        try:
            async with httpx.AsyncClient(
                proxy=proxy,
                timeout=httpx.Timeout(connect=6.0, read=10.0, write=10.0, pool=6.0),
                follow_redirects=False,
                headers={"User-Agent": UA, "Accept-Encoding": "identity"},
            ) as client:
                response = await client.get("https://api.github.com/")
                return response.status_code in (200, 301, 302, 403, 404)
        except Exception:
            return False

    async def _probe_local_proxy(self) -> str | None:
        """扫本机常见代理端口，返回第一个可用的代理地址。"""
        now = time.time()
        if self._probed is not None and now - self._probed_at < PROXY_CACHE_TTL:
            return self._probed or None

        if self._probe_lock is None:
            self._probe_lock = asyncio.Lock()
        async with self._probe_lock:
            now = time.time()
            if self._probed is not None and now - self._probed_at < PROXY_CACHE_TTL:
                return self._probed or None

            flags = await asyncio.gather(
                *(self._tcp_open(port) for port in DEFAULT_PROXY_PORTS)
            )
            open_ports = [
                port for port, opened in zip(DEFAULT_PROXY_PORTS, flags) if opened
            ]

            socks_ok = _socks_available()
            found: str | None = None
            for port in open_ports:
                if port in SOCKS_PORTS:
                    schemes = ["socks5", "http"] if socks_ok else ["http"]
                else:
                    schemes = ["http", "socks5"] if socks_ok else ["http"]
                for scheme in schemes:
                    candidate = f"{scheme}://127.0.0.1:{port}"
                    if await self._verify_proxy(candidate):
                        found = candidate
                        break
                if found:
                    break

            self._probed = found or ""
            self._probed_at = time.time()
            if found:
                logger.info(f"[dl_accel] 自动探测到本机代理：{found}")
            else:
                logger.info("[dl_accel] 未探测到可用的本机代理，将直连下载")
            return found

    async def proxy_candidates(self, allow_probe: bool = True) -> list[str | None]:
        """按优先级返回尝试通道，末尾永远是 None（直连）。

        顺序：手填 → AstrBot 全局设置 → 环境变量 → 自动探测。
        代理刚失败过（120 秒内）时把直连提到前面，避免每个包都先撞一次墙；
        但代理仍保留在列表里，因为偶尔抖一下不代表彻底不可用。
        """
        proxies: list[str] = []
        for value in (self.proxy, self._global_proxy(), self._env_proxy()):
            if value and value not in proxies:
                proxies.append(value)

        if not proxies and self.auto_probe_proxy and allow_probe:
            probed = await self._probe_local_proxy()
            if probed:
                proxies.append(probed)

        recently_failed = (
            self._proxy_ok is False and time.time() - self._proxy_checked_at < 120
        )
        if recently_failed and proxies:
            return [None, *proxies]
        return [*proxies, None]

    def _make_client(self, proxy: str | None, timeout: float) -> httpx.AsyncClient:
        kwargs: dict[str, Any] = {
            "follow_redirects": True,
            # 不设总时长上限：大文件本来就该慢慢下，真正的兜底靠 read 超时
            "timeout": httpx.Timeout(
                connect=15.0,
                read=min(max(timeout, 30.0), 300.0),
                write=120.0,
                pool=30.0,
            ),
            "verify": True,
            "headers": {"User-Agent": UA, "Accept-Encoding": "identity"},
        }
        # 通道是显式指定的，不要让环境变量里的 http_proxy / no_proxy 再插一脚，
        # 否则「选直连」这条通道可能被环境变量悄悄改道，回退逻辑就失真了。
        kwargs["trust_env"] = False
        if proxy:
            kwargs["proxy"] = proxy
        return httpx.AsyncClient(**kwargs)

    # ---------------- 缓存 ----------------

    def _cache_path(self, url: str) -> Path:
        key = hashlib.sha256(url.encode("utf-8")).hexdigest()[:40]
        return self.cache_dir / f"{key}.bin"

    def _cache_meta_path(self, url: str) -> Path:
        return self._cache_path(url).with_suffix(".json")

    def _cache_get(self, url: str) -> Path | None:
        if not self.cache_enabled:
            return None
        path = self._cache_path(url)
        try:
            if not path.is_file():
                return None
            size = path.stat().st_size
            if size <= 0:
                return None
            meta_path = self._cache_meta_path(url)
            if meta_path.is_file():
                try:
                    meta = json.loads(meta_path.read_text(encoding="utf-8"))
                except Exception:
                    meta = {}
                expected = int(meta.get("size", 0) or 0)
                if expected and expected != size:
                    # 记录的大小对不上：缓存被截断过，当作没有
                    path.unlink(missing_ok=True)
                    meta_path.unlink(missing_ok=True)
                    return None
            os.utime(path, None)
            return path
        except OSError:
            return None

    def _cache_put(self, url: str, source: Path) -> None:
        if not self.cache_enabled:
            return
        dest = self._cache_path(url)
        tmp = dest.with_suffix(".part")
        try:
            shutil.copyfile(source, tmp)
            os.replace(tmp, dest)
            self._cache_meta_path(url).write_text(
                json.dumps(
                    {"url": url, "size": dest.stat().st_size, "at": int(time.time())},
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
        except OSError as exc:
            logger.debug(f"[dl_accel] 写缓存失败 {url}: {exc}")
            try:
                tmp.unlink(missing_ok=True)
            except OSError:
                pass
            return
        self._cache_trim()

    def _cache_trim(self) -> None:
        # 顺手清理上次异常退出残留的半截文件
        try:
            for leftover in self.cache_dir.glob("*.part"):
                try:
                    if time.time() - leftover.stat().st_mtime > 3600:
                        leftover.unlink()
                except OSError:
                    continue
        except OSError:
            pass

        if self.cache_max_bytes <= 0:
            return
        try:
            files = [p for p in self.cache_dir.glob("*.bin") if p.is_file()]
        except OSError:
            return
        total = sum(p.stat().st_size for p in files)
        if total <= self.cache_max_bytes:
            return
        files.sort(key=lambda item: item.stat().st_mtime)
        for path in files:
            if total <= self.cache_max_bytes:
                break
            try:
                size = path.stat().st_size
                path.unlink()
                path.with_suffix(".json").unlink(missing_ok=True)
                total -= size
            except OSError:
                continue

    def cache_stats(self) -> tuple[int, int]:
        count = 0
        total = 0
        try:
            for path in self.cache_dir.glob("*.bin"):
                if path.is_file():
                    count += 1
                    total += path.stat().st_size
        except OSError:
            pass
        return count, total

    def clear_cache(self) -> tuple[int, int]:
        count = 0
        total = 0
        try:
            for path in list(self.cache_dir.iterdir()):
                if path.is_file():
                    try:
                        size = path.stat().st_size
                        path.unlink()
                        count += 1
                        total += size
                    except OSError:
                        continue
        except OSError:
            pass
        return count, total

    # ---------------- 进度 ----------------

    @staticmethod
    async def _emit(callback, payload: dict) -> None:
        if not callback:
            return
        try:
            result = callback(payload)
            if asyncio.iscoroutine(result):
                await result
        except Exception as exc:  # 进度回调出错绝不能影响下载
            logger.debug(f"[dl_accel] 进度回调异常: {exc}")

    # ---------------- 下载 ----------------

    async def download(self, url: str, path: str, timeout: float, progress_callback) -> None:
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)

        cached = self._cache_get(url)
        if cached is not None:
            size = cached.stat().st_size
            await self._emit(
                progress_callback,
                {"url": url, "downloaded": 0, "total": size, "percent": 0, "speed": 0},
            )
            shutil.copyfile(cached, target)
            self.stat_cache_hits += 1
            self.stat_bytes += size
            await self._emit(
                progress_callback,
                {"url": url, "downloaded": size, "total": size, "percent": 1, "speed": 0},
            )
            logger.info(f"[dl_accel] 缓存命中 {_human(size)} {url}")
            return

        started = time.time()
        await self._fetch(url, target, timeout, progress_callback)
        size = target.stat().st_size
        self.stat_downloads += 1
        self.stat_bytes += size
        elapsed = max(time.time() - started, 0.001)
        logger.info(
            f"[dl_accel] 下载完成 {_human(size)} 用时 {elapsed:.2f}s "
            f"({size / 1024 / elapsed:.0f} KB/s) {url}"
        )
        self._cache_put(url, target)

    async def _fetch(self, url: str, target: Path, timeout: float, progress_callback) -> None:
        """依次尝试各通道，全部失败才抛最后一个错误。"""
        proxies = await self.proxy_candidates()
        attempts: list[tuple[str, str | None, str]] = [
            (url, proxy, "直连" if proxy is None else f"代理 {proxy}") for proxy in proxies
        ]
        mirror = self._mirror_url(url)
        if mirror:
            attempts.append((mirror, None, "镜像"))

        last_error: Exception | None = None
        for attempt_url, proxy, label in attempts:
            try:
                await self._fetch_once(
                    attempt_url, target, timeout, progress_callback, proxy
                )
                return
            except asyncio.CancelledError:
                raise
            except httpx.HTTPStatusError as exc:
                target.unlink(missing_ok=True)
                last_error = exc
                status = exc.response.status_code
                logger.warning(f"[dl_accel] {label} 返回 HTTP {status}")
                if proxy is None and attempt_url == url and 400 <= status < 500:
                    # 源站明确说没有/没权限，换通道不会有别的结果
                    raise
            except Exception as exc:
                last_error = exc
                target.unlink(missing_ok=True)
                logger.warning(f"[dl_accel] {label} 失败: {exc}")
                if proxy is not None:
                    self._proxy_ok = False
                    self._proxy_checked_at = time.time()

        if last_error is not None:
            raise last_error
        raise RuntimeError("没有可用的下载通道")

    async def _fetch_once(
        self,
        url: str,
        target: Path,
        timeout: float,
        progress_callback,
        proxy: str | None,
    ) -> None:
        async with self._make_client(proxy, timeout) as client:
            size, supports_ranges = await self._probe(client, url)
            if (
                size is not None
                and size >= self.min_size
                and supports_ranges
                and self.parts > 1
            ):
                try:
                    await self._fetch_parallel(
                        client, url, target, size, timeout, progress_callback
                    )
                    return
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.warning(f"[dl_accel] 分块下载失败，退回单连接: {exc}")
                    target.unlink(missing_ok=True)
            await self._fetch_single(
                client, url, target, size or 0, timeout, progress_callback
            )

    async def _probe(self, client: httpx.AsyncClient, url: str) -> tuple[int | None, bool]:
        """用 Range 探一个字节，返回 (总大小, 服务端是否支持分段)。

        必须用 stream：万一服务端忽略 Range 把整个文件回过来，
        用 client.get 会把几百 MB 读进内存。
        """
        try:
            async with client.stream(
                "GET", url, headers={"Range": "bytes=0-0"}
            ) as response:
                if response.status_code == 206:
                    content_range = response.headers.get("content-range", "")
                    total = 0
                    if "/" in content_range:
                        try:
                            total = int(content_range.rsplit("/", 1)[1])
                        except ValueError:
                            total = 0
                    return (total or None), True
                if response.status_code == 200:
                    return int(response.headers.get("content-length", 0) or 0), False
                return None, False
        except Exception as exc:
            logger.debug(f"[dl_accel] 探测失败 {url}: {exc}")
            return None, False

    async def _fetch_single(
        self,
        client: httpx.AsyncClient,
        url: str,
        target: Path,
        total_size: int,
        timeout: float,
        progress_callback,
    ) -> None:
        downloaded = 0
        started = time.time()
        last_emit = 0.0
        await self._emit(
            progress_callback,
            {"url": url, "downloaded": 0, "total": total_size, "percent": 0, "speed": 0},
        )
        with target.open("wb") as handle:
            async with client.stream("GET", url) as response:
                response.raise_for_status()
                if not total_size:
                    total_size = int(response.headers.get("content-length", 0) or 0)
                async for chunk in response.aiter_bytes(65536):
                    handle.write(chunk)
                    downloaded += len(chunk)
                    now = time.time()
                    if now - last_emit >= 0.2:
                        last_emit = now
                        elapsed = max(now - started, 0.001)
                        await self._emit(
                            progress_callback,
                            {
                                "url": url,
                                "downloaded": downloaded,
                                "total": total_size,
                                "percent": downloaded / total_size if total_size else 0,
                                "speed": downloaded / 1024 / elapsed,
                            },
                        )
        if total_size and downloaded != total_size:
            raise RuntimeError(f"下载长度不符 期望 {total_size} 实得 {downloaded}")
        await self._emit(
            progress_callback,
            {
                "url": url,
                "downloaded": downloaded,
                "total": downloaded,
                "percent": 1,
                "speed": 0,
            },
        )

    async def _fetch_parallel(
        self,
        client: httpx.AsyncClient,
        url: str,
        target: Path,
        size: int,
        timeout: float,
        progress_callback,
    ) -> None:
        parts = min(self.parts, max(2, math.ceil(size / (256 * 1024))))
        chunk = size // parts
        ranges: list[tuple[int, int]] = []
        for index in range(parts):
            start = index * chunk
            end = size - 1 if index == parts - 1 else (start + chunk - 1)
            ranges.append((start, end))

        shard_dir = Path(tempfile.mkdtemp(prefix=".dlaccel-", dir=str(target.parent)))
        shards = [shard_dir / f"part{index}" for index in range(parts)]

        state = {
            "downloaded": 0,
            "started": time.time(),
            "last": 0.0,
            "lock": asyncio.Lock(),
        }
        await self._emit(
            progress_callback,
            {"url": url, "downloaded": 0, "total": size, "percent": 0, "speed": 0},
        )

        async def fetch_shard(index: int, start: int, end: int) -> None:
            last_error: Exception | None = None
            for attempt in range(3):
                written = 0
                try:
                    headers = {"Range": f"bytes={start}-{end}"}
                    with shards[index].open("wb") as handle:
                        async with client.stream("GET", url, headers=headers) as response:
                            if response.status_code != 206:
                                raise RuntimeError(
                                    f"分段请求返回 {response.status_code}"
                                )
                            async for chunk_bytes in response.aiter_bytes(65536):
                                handle.write(chunk_bytes)
                                written += len(chunk_bytes)
                                # 只在锁内改计数，上报放到锁外，
                                # 免得 8 个分片为了报一次进度互相排队
                                async with state["lock"]:
                                    state["downloaded"] += len(chunk_bytes)
                                    now = time.time()
                                    payload = None
                                    if now - state["last"] >= 0.2:
                                        state["last"] = now
                                        elapsed = max(now - state["started"], 0.001)
                                        payload = {
                                            "url": url,
                                            "downloaded": state["downloaded"],
                                            "total": size,
                                            "percent": state["downloaded"] / size,
                                            "speed": (
                                                state["downloaded"] / 1024 / elapsed
                                            ),
                                        }
                                if payload is not None:
                                    await self._emit(progress_callback, payload)
                    expected = end - start + 1
                    if written != expected:
                        raise RuntimeError(f"分段长度不符 期望 {expected} 实得 {written}")
                    return
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    last_error = exc
                    async with state["lock"]:
                        state["downloaded"] -= written
                    if attempt < 2:
                        await asyncio.sleep(0.4 * (attempt + 1))
            if last_error is not None:
                raise last_error

        try:
            await asyncio.gather(
                *(fetch_shard(i, start, end) for i, (start, end) in enumerate(ranges))
            )
            with target.open("wb") as handle:
                for shard in shards:
                    with shard.open("rb") as source:
                        shutil.copyfileobj(source, handle, 1024 * 1024)
        finally:
            shutil.rmtree(shard_dir, ignore_errors=True)

        final_size = target.stat().st_size
        if final_size != size:
            raise RuntimeError(f"合并后大小不符 期望 {size} 实得 {final_size}")
        await self._emit(
            progress_callback,
            {
                "url": url,
                "downloaded": final_size,
                "total": final_size,
                "percent": 1,
                "speed": 0,
            },
        )

    # ---------------- 预热 ----------------

    def market_entries(self) -> list[dict[str, Any]]:
        path = Path(get_astrbot_data_path()) / "plugins.json"
        if not path.is_file():
            return []
        try:
            data = json.loads(path.read_text(encoding="utf-8-sig"))
        except Exception:
            return []
        payload = data.get("data") if isinstance(data, dict) and "data" in data else data
        if not isinstance(payload, dict):
            return []
        entries = [
            value
            for key, value in payload.items()
            if key != "$meta" and isinstance(value, dict)
        ]
        entries.sort(key=lambda item: int(item.get("download_count", 0) or 0), reverse=True)
        return entries

    async def prewarm(self, limit: int = 50) -> dict[str, int]:
        result = {"hit": 0, "fetched": 0, "fail": 0}
        urls: list[str] = []
        for entry in self.market_entries():
            candidate = str(entry.get("download_url", "") or "")
            if candidate.startswith(("http://", "https://")) and candidate not in urls:
                urls.append(candidate)
            if len(urls) >= limit:
                break
        if not urls:
            return result

        semaphore = asyncio.Semaphore(4)
        try:
            warm_dir = Path(
                tempfile.mkdtemp(prefix="dlaccel-warm-", dir=str(self.cache_dir))
            )
        except OSError:
            warm_dir = self.cache_dir

        async def warm(one: str) -> None:
            async with semaphore:
                if self._cache_get(one) is not None:
                    result["hit"] += 1
                    return
                tmp = warm_dir / (
                    hashlib.sha256(one.encode("utf-8")).hexdigest()[:16] + ".bin"
                )
                try:
                    await self._fetch(one, tmp, 300.0, None)
                    self._cache_put(one, tmp)
                    result["fetched"] += 1
                except Exception as exc:
                    logger.warning(f"[dl_accel] 预热失败 {one}: {exc}")
                    result["fail"] += 1
                finally:
                    tmp.unlink(missing_ok=True)

        try:
            await asyncio.gather(*(warm(one) for one in urls))
        finally:
            if warm_dir != self.cache_dir:
                shutil.rmtree(warm_dir, ignore_errors=True)
        return result

    # ---------------- 诊断 ----------------

    async def diagnose(self) -> str:
        """逐个通道实测连通性与延迟，帮使用者判断该填什么代理。"""
        lines = ["下载通道诊断", ""]
        proxies = await self.proxy_candidates()
        channels: list[tuple[str, str | None]] = [
            ("直连", None),
            *[(f"代理 {p}", p) for p in proxies if p],
        ]
        url = "https://api.github.com/"
        for label, proxy in channels:
            started = time.time()
            try:
                async with self._make_client(proxy, 20.0) as client:
                    response = await client.get(url)
                cost = (time.time() - started) * 1000
                lines.append(f"{label}：HTTP {response.status_code} / {cost:.0f}ms")
            except Exception as exc:
                lines.append(f"{label}：不可用（{type(exc).__name__}: {exc}）")
        lines.append("")
        lines.append(f"反代前缀：{self.mirror_prefix or '未配置'}")
        lines.append(f"接管域名：{len(self.allow_hosts)} 个")
        return "\n".join(lines)

    # ---------------- 状态 ----------------

    async def status_text(self) -> str:
        count, total = self.cache_stats()
        proxies = await self.proxy_candidates()
        shown = [p for p in proxies if p]
        proxy_line = (
            " → ".join(shown) + "（失败自动回退直连）"
            if shown
            else "直连（未检测到可用代理）"
        )
        return "\n".join(
            [
                f"下载加速器 v{PLUGIN_VERSION}",
                f"接管状态：{'已启用' if self.enabled else '已关闭'}",
                f"通道：{proxy_line}",
                f"镜像：{self.mirror_prefix or '未启用'}",
                f"分块：{self.parts} 路（≥{_human(self.min_size)} 才分块）",
                f"缓存：{count} 个 / {_human(total)} / 上限 {_human(self.cache_max_bytes)}",
                f"本次已加速 {self.stat_downloads} 次，缓存命中 {self.stat_cache_hits} 次，"
                f"回退原生 {self.stat_fallbacks} 次，共 {_human(self.stat_bytes)}",
            ]
        )


async def _patched_download_file(self, url, path, timeout=1800.0, progress_callback=None):
    """替换 _RepoZipUpdater._download_file 的实现。"""
    accel = _ACTIVE_ACCEL
    if accel is not None and accel.should_handle(str(url)):
        try:
            await accel.download(str(url), str(path), timeout, progress_callback)
            return
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            accel.stat_fallbacks += 1
            logger.warning(f"[dl_accel] 加速失败，回退原生下载：{url} -> {exc}")
            try:
                Path(path).unlink(missing_ok=True)
            except OSError:
                pass
    if _ORIGINAL_DOWNLOAD_FILE is None:
        raise RuntimeError("下载加速器未正确初始化")
    return await _ORIGINAL_DOWNLOAD_FILE(self, url, path, timeout, progress_callback)


@register(
    PLUGIN_NAME,
    "shengbuding",
    "接管 AstrBot 的插件/核心下载：自动代理 + Range 分块并行 + 磁盘缓存，"
    "下载失败自动回退原生实现。",
    PLUGIN_VERSION,
    "https://github.com/shengbuding/astrbot_plugin_dl_accel",
)
class DLAcclPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig | dict | None = None):
        super().__init__(context)
        self.config = config or {}
        self.accel: DownloadAccelerator | None = None

    async def initialize(self) -> None:
        global _ORIGINAL_DOWNLOAD_FILE, _ACTIVE_ACCEL

        try:
            from astrbot.core.zip_updater import _RepoZipUpdater
        except Exception as exc:
            logger.error(
                f"[dl_accel] 当前 AstrBot 版本没有 _RepoZipUpdater，加速器不启用：{exc}"
            )
            return

        if _ORIGINAL_DOWNLOAD_FILE is None:
            _ORIGINAL_DOWNLOAD_FILE = _RepoZipUpdater._download_file

        self.accel = DownloadAccelerator(self.config)
        # 注入全局配置读取钩子，用于跟随使用者已有的 AstrBot 全局 HTTP 代理
        try:
            self.accel._config_getter = self.context.get_config
        except Exception:
            pass

        _ACTIVE_ACCEL = self.accel
        _RepoZipUpdater._download_file = _patched_download_file

        try:
            self.context.register_web_api(
                f"{PLUGIN_NAME}/status",
                self.api_status,
                ["GET"],
                "下载加速器状态",
            )
        except Exception as exc:
            logger.debug(f"[dl_accel] 注册 Web API 失败（不影响下载加速）：{exc}")

        count, total = self.accel.cache_stats()
        logger.info(
            f"[dl_accel] v{PLUGIN_VERSION} 已接管下载（{self.accel.parts} 路分块，"
            f"缓存 {count} 个 / {_human(total)}）"
        )

    async def terminate(self) -> None:
        global _ACTIVE_ACCEL

        _ACTIVE_ACCEL = None
        try:
            from astrbot.core.zip_updater import _RepoZipUpdater
        except Exception:
            return

        if _ORIGINAL_DOWNLOAD_FILE is not None:
            _RepoZipUpdater._download_file = _ORIGINAL_DOWNLOAD_FILE
        logger.info("[dl_accel] 下载加速已还原为 AstrBot 原生实现")

    # ---------------- 面板 API ----------------

    async def api_status(self):
        if self.accel is None:
            return {"status": "error", "message": "未初始化（当前 AstrBot 版本可能不支持）"}
        count, total = self.accel.cache_stats()
        proxies = await self.accel.proxy_candidates(allow_probe=False)
        return {
            "status": "ok",
            "data": {
                "version": PLUGIN_VERSION,
                "enabled": self.accel.enabled,
                "proxy": self.accel.proxy,
                "proxies": [p for p in proxies if p],
                "mirror": self.accel.mirror_prefix,
                "parts": self.accel.parts,
                "cache_count": count,
                "cache_bytes": total,
                "downloads": self.accel.stat_downloads,
                "cache_hits": self.accel.stat_cache_hits,
                "fallbacks": self.accel.stat_fallbacks,
            },
        }

    # ---------------- 指令 ----------------

    @staticmethod
    def _is_admin(event: AstrMessageEvent) -> bool:
        try:
            return bool(event.is_admin())
        except Exception:
            return False

    @filter.command("dlaccel")
    async def cmd_dlaccel(self, event: AstrMessageEvent, args: GreedyStr = ""):
        if self.accel is None:
            yield event.plain_result("下载加速器未初始化（当前 AstrBot 版本可能不支持）。")
            return

        text = str(args or "").strip()
        keyword = text.split()[0].lower() if text else ""

        if keyword in ("", "status", "状态", "stats"):
            yield event.plain_result(await self.accel.status_text())
            return

        if keyword in ("clear", "clean", "清空"):
            if not self._is_admin(event):
                yield event.plain_result("只有管理员可以清空下载缓存。")
                return
            count, total = self.accel.clear_cache()
            yield event.plain_result(f"已清空下载缓存：{count} 个文件 / {_human(total)}。")
            return

        if keyword in ("off", "关闭"):
            if not self._is_admin(event):
                yield event.plain_result("只有管理员可以临时关闭加速。")
                return
            self.accel.enabled = False
            yield event.plain_result("下载加速已临时关闭（重启实例后恢复配置值）。")
            return

        if keyword in ("on", "开启"):
            if not self._is_admin(event):
                yield event.plain_result("只有管理员可以临时开启加速。")
                return
            self.accel.enabled = True
            yield event.plain_result("下载加速已开启。")
            return

        if keyword in ("prewarm", "预热"):
            if not self._is_admin(event):
                yield event.plain_result("只有管理员可以预热缓存。")
                return
            tokens = text.split()
            limit = 50
            if len(tokens) > 1:
                try:
                    limit = max(1, min(300, int(tokens[1])))
                except ValueError:
                    limit = 50
            yield event.plain_result(
                f"开始预热插件市场下载量前 {limit} 名的安装包，请稍候……"
            )
            result = await self.accel.prewarm(limit)
            count, total = self.accel.cache_stats()
            yield event.plain_result(
                f"预热完成：新下载 {result['fetched']} 个，已在缓存 {result['hit']} 个，"
                f"失败 {result['fail']} 个。当前缓存 {count} 个 / {_human(total)}。"
            )
            return

        if keyword in ("diag", "诊断"):
            if not self._is_admin(event):
                yield event.plain_result("只有管理员可以运行通道诊断。")
                return
            yield event.plain_result("正在逐个通道实测连通性，请稍候……")
            yield event.plain_result(await self.accel.diagnose())
            return

        yield event.plain_result(
            "未知子命令。可用：status / clear / on / off / prewarm N / diag"
        )
