# AstrBot 下载加速器

> 接管 AstrBot 的插件与核心下载：**自动代理 + 多路分块并行 + 本地缓存**，失败自动回退原生实现。

在国内网络环境下，AstrBot 从 GitHub / 官方对象存储拉取插件包时经常只有几十 KB/s，一个 5 MB 的插件要等好几分钟，大一点的包还会直接超时失败。这个插件在原下载链路上加了一层"加速通道"，把速度拉回到可用水平，同时保证**永远不会因为加速器自己出问题，让本来能装上的插件装不上**。

---

## 它做了什么

AstrBot 的所有下载都汇聚到一个函数：`astrbot.core.zip_updater._RepoZipUpdater._download_file`。
插件安装、插件更新、AstrBot 核心更新三条链路全部经过它。本插件在初始化时替换掉它，于是这三条链路一起被接管：

```
插件市场安装 ─┐
插件更新     ─┼─→ _download_file ─→ [本插件] 代理 → 分块并行 → 缓存
核心更新     ─┘                        │
                                       └─ 任一步失败 ─→ 原生实现
```

### 三层加速

| 层次 | 做法 | 适用场景 |
| --- | --- | --- |
| 通道 | 自动探测本机代理，也能跟随 AstrBot 全局 HTTP 代理 | 本机有 Clash / V2Ray / Mihomo 等代理客户端 |
| 传输 | HTTP Range 把文件切成 N 段并发下载 | 单连接被限速，多连接能叠加带宽 |
| 复用 | 下载完成的包留在本地磁盘缓存 | 重复安装同一个包、版本回滚 |

### 三层都有兜底

- 代理连不上 → 自动换下一个通道，最后无条件回退**直连**
- 服务端不支持 Range（返回 200 而不是 206）→ 自动降级为**单连接**
- 分块下载任何一片失败 → 每片重试 3 次，仍失败则整文件回退**单连接**
- 单连接也失败 → 回退 **AstrBot 原生下载**
- 原生下载也失败 → 抛出的异常与没装这个插件时**完全一致**

任何一层出问题都只是"没变快"，不会变成"装不上"。

---

## 安装

### 方式一：从仓库地址安装（推荐）

面板 → 插件管理 → 安装插件 → **从 URL / 仓库安装**，填：

```
https://github.com/shengbuding/astrbot_plugin_dl_accel
```

点安装即可。也可以直接用 Release 里的打包文件：

```
https://github.com/shengbuding/astrbot_plugin_dl_accel/releases/download/v1.0.0/astrbot_plugin_dl_accel.zip
```

### 方式二：手动安装

把本仓库整个目录放进 AstrBot 的插件目录：

```
<AstrBot 数据目录>/plugins/astrbot_plugin_dl_accel/
```

然后在面板 → 插件管理里点「重载插件」或直接重启 AstrBot。

### 方式三：插件市场

市场收录正在走流程，**当前还搜不到**，请先用方式一或方式二。收录通过后，在面板 → 插件市场搜索 **下载加速器** 或 **Download Accelerator** 即可一键安装。

安装后在日志里能看到这一行，说明接管成功：

```
[dl_accel] v1.0.0 已接管下载（8 路分块，缓存 0 个 / 0.0B）
```

---

## 配置

全部配置项在面板 → 插件管理 → 下载加速器 → 配置里，**装完不用改任何东西就能用**。

| 配置项 | 默认 | 说明 |
| --- | --- | --- |
| `enabled` | 开 | 总开关。关掉立即恢复原生下载。 |
| `proxy` | 空 | **留空 = 自动**。依次尝试 AstrBot 全局 HTTP 代理 → 环境变量 → 扫描本机常见端口。 |
| `auto_probe_proxy` | 开 | 没配代理时自动扫描本机端口并实测能否出网。没有代理软件的用户保持开启即可。 |
| `mirror_prefix` | 空 | GitHub 反代前缀，如 `https://ghfast.top`。只对 GitHub 域名生效。 |
| `parallel_parts` | 8 | 分块并行数。代理限流时可降到 4，带宽充裕可提到 16。 |
| `min_size_kb` | 512 | 小于这个大小不分块，直接单连接。 |
| `cache_enabled` | 开 | 本地缓存开关。 |
| `cache_max_mb` | 2048 | 缓存上限，超出后按最久未使用清理。 |
| `allow_hosts` | 见下 | 允许接管的域名，逗号分隔。不在名单里的地址直接走原生下载。 |

默认接管域名：

```
astrbot-cloud-test-s3.astrbot.app, astrbot-plugins-s3.astrbot.app,
github.com, codeload.github.com, api.github.com,
objects.githubusercontent.com, raw.githubusercontent.com,
release-assets.githubusercontent.com, github-releases.githubusercontent.com
```

### 我该填什么代理？

**大多数情况下什么都不用填。** 插件会自动扫描这些端口：
`7890, 7897, 10809, 10808, 1080, 2080, 6152, 8889, 33210, 8118`，
并对每个开放端口实测一次 `https://api.github.com/`，确认它真的能出网才采用。

如果自动探测不准，先跑一次诊断：

```
/dlaccel diag
```

输出形如：

```
下载通道诊断

直连：HTTP 200 / 1830ms
代理 socks5://127.0.0.1:10808：HTTP 200 / 240ms

反代前缀：未配置
接管域名：9 个
```

哪条延迟低就用哪条，把对应地址填进 `proxy`。

> **注意**：Windows「系统代理」只能表达 HTTP 代理。如果你的客户端在 10808 上开的是 **SOCKS5**，系统代理设置里填 `127.0.0.1:10808` 是错的——AstrBot 和浏览器都会当成 HTTP 代理去连，结果是连不上。本插件会分别按 SOCKS5 和 HTTP 试一次，所以直接留空让它自己探测更省事。

---

## 指令

| 指令 | 说明 | 权限 |
| --- | --- | --- |
| `/dlaccel` | 查看状态：是否接管、当前通道、缓存占用、命中次数 | 所有人 |
| `/dlaccel diag` | 逐个通道实测连通性与延迟 | 管理员 |
| `/dlaccel clear` | 清空本地下载缓存 | 管理员 |
| `/dlaccel prewarm N` | 预热市场下载量前 N 名的安装包（默认 50，上限 300） | 管理员 |
| `/dlaccel off` / `on` | 临时关闭 / 开启加速（不改配置文件） | 管理员 |

`/dlaccel` 的输出示例：

```
下载加速器 v1.0.0
接管状态：已启用
通道：socks5://127.0.0.1:10808（失败自动回退直连）
镜像：未启用
分块：8 路（≥512.0KB 才分块）
缓存：12 个 / 246.8MB / 上限 2.0GB
本次已加速 3 次，缓存命中 5 次，回退原生 0 次，共 512.3MB
```

---

## HTTP 接口

插件注册了一个只读接口，方便接入面板或监控：

```
GET /api/plug/astrbot_plugin_dl_accel/status
```

```json
{
  "status": "ok",
  "data": {
    "version": "1.0.0",
    "enabled": true,
    "proxy": "",
    "proxies": ["socks5://127.0.0.1:10808"],
    "mirror": "",
    "parts": 8,
    "cache_count": 12,
    "cache_bytes": 258777417,
    "downloads": 3,
    "cache_hits": 5,
    "fallbacks": 0
  }
}
```

---

## 常见问题

**Q：装完感觉没变快？**
先跑 `/dlaccel diag`。如果直连和代理延迟差不多，说明你的网络到 GitHub 本来就通畅，瓶颈不在单连接。这种情况可以试试 `mirror_prefix` 填一个国内 GitHub 反代。

**Q：会不会下到旧版本？**
缓存以**完整 URL** 为键（SHA-256）。不同版本的下载地址不同，不会互相串。另外缓存文件旁边有 `.json` 元数据记录大小，大小对不上会自动丢弃重下。

**Q：缓存占空间吗？**
默认上限 2 GB，超出后按最久未使用自动清理。可以随时 `/dlaccel clear`。

**Q：会不会和 AstrBot 更新冲突？**
不会。核心更新走的是同一个 `_download_file`，本插件只是把它换成更快的实现，函数签名、回调格式、失败行为都与原生一致。

**Q：卸载后会不会有残留？**
停用或卸载时会把下载函数还原成原生实现，立即生效，不需要重启。缓存目录 `<数据目录>/plugin_data/astrbot_plugin_dl_accel/cache` 会保留，可以手动删。

**Q：支持 SOCKS5 吗？**
支持，但需要 `socksio`。AstrBot 自带的 httpx 通常已经带有；万一没有，执行一次：

```bash
<实例 venv>/Scripts/pip install socksio
```

插件检测不到 `socksio` 时会自动跳过 SOCKS5 通道，不会报错。

**Q：git 方式的下载能加速吗？**
不能。AstrBot 还有一条 `_download_repository` / `_clone_repository` 的 git 路径，走的是 `git` 命令而不是 Python 下载函数，本插件不接管。

---

## 兼容性

- AstrBot `>= 4.16.0`
- Python 3.10+
- 依赖：`httpx`（AstrBot 自带）；SOCKS5 需 `socksio`（可选）

## 许可

AGPL-3.0，与 AstrBot 本体一致。
