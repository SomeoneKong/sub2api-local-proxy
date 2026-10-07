# sub2api-local-proxy

codex 客户端与 sub2api 网关之间的本地反向代理，跑在每台客户端机器上（Windows / Linux / macOS），
codex 的 `base_url` 与 `model_catalog_url` 指向它。请求原样转发给网关，只多做三件事：

1. **模型目录缓存**：codex 拉目录时每次先回源；回源失败、超时、网关回 5xx / 429 或目录 codex 用不了时，
   返回最近一次成功的目录。
2. **上行 zstd 压缩**：`/v1/responses` 等请求体压缩后发给网关（网关读请求体时会解压）。
3. **网关暂时不可用时等它恢复**：隧道断、网关重启期间到达的请求先等，拿到响应头之前失败的请求在恢复后重发。

## 为什么需要

- **目录**：codex 0.160.1 配了 `model_catalog_url` 时，拉目录总共只等 5 秒，失败一次就把手上的目录清空
  （既不用过期缓存，也不退回内置目录），app-server 每 270 秒还会强刷一次。网关或隧道抖一下，会话里的模型
  元数据就没了。
- **压缩**：codex 只在 provider 名为 `OpenAI` 且全局登录态是 ChatGPT 类时才压缩请求体，自建网关用不上。
  请求体每轮带完整历史（常见几百 KB 到几 MB），按会话记录实测 zstd 默认级别压到原来的 0.35 左右。
- **连接**：客户端经透明代理的 TUN 模式（如 mihomo / Clash）访问网关时，TCP 握手在本机就完成了。网关不可达时拨号照样成功，表现为
  写出请求后连接被关（EOF）或一直没有响应，codex 看到的是一串连接错误。

## 安装

先在开发机上交叉编译（需要 Go），`dist/` 里是各平台程序与安装脚本，整个目录拷到目标机器：

```bash
./build.sh
```

所有平台的程序、配置、目录缓存都在 `~/.sub2api-local-proxy/`（`bin/`、`config.json`、`cache/`）。
安装脚本重复执行即升级：换程序、重启，配置不动。新程序先暂存并用它校验配置，通过了才停旧实例、替换、
启动，校验不过时正在运行的实例不受影响；启动后检查进程确实在运行，没起来就报错并打印日志。

| 平台 | 安装 | 开机/登录启动 | 日志 |
|---|---|---|---|
| Linux（systemd） | `./install-linux.sh --upstream http://sub2api.example.com:8080` | systemd 用户服务 + linger：开机即启动、不必登录，崩溃后 3 秒拉起；只有首次开 linger 可能要 sudo | `journalctl --user -u sub2api-local-proxy -f` |
| Windows | `pwsh -ExecutionPolicy Bypass -File .\install-windows.ps1 -Upstream http://sub2api.example.com:8080` | 当前用户登录时启动的计划任务，不需要管理员；程序退出后不会自动拉起，要重新登录或 `Start-ScheduledTask sub2api-local-proxy` | `~/.sub2api-local-proxy/proxy.log` |
| macOS | `./install-macos.sh --upstream http://sub2api.example.com:8080` | launchd 用户代理，登录后启动、退出后拉起 | `~/.sub2api-local-proxy/proxy.log` |

卸载：`./install-linux.sh --uninstall`、`.\install-windows.ps1 -Uninstall`、`./install-macos.sh --uninstall`，
都保留 `~/.sub2api-local-proxy/`。Windows 版是无控制台窗口的程序，日志只写文件；macOS 脚本没有在真机上跑过。

### codex 的 profile

```toml
[model_providers.Router]
base_url = "http://127.0.0.1:28770/v1"
model_catalog_url = "http://127.0.0.1:28770/v1/models"
# 其余（wire_api、experimental_bearer_token、supports_standalone_web_search 等）不变

[features]
api_key_model_discovery = true   # 配了 model_catalog_json 的 profile 要删掉那一行，否则不会去拉目录
```

codex 的 HTTP 客户端不会自动绕过 `127.0.0.1`：环境变量里有 `HTTP_PROXY` / `ALL_PROXY` 时，`NO_PROXY` 要含
`127.0.0.1`；Windows 上环境变量为空时会读系统代理设置，而其中的 `<local>`、`127.*` 对 IP 写法的 `127.0.0.1`
不生效，所以开着系统代理的机器同样要设 `NO_PROXY=127.0.0.1,localhost`。

## 配置

`~/.sub2api-local-proxy/config.json`，没写的字段取默认值，拼错的字段会报错。**改了自动生效**（每 2 秒看一次
修改时间与大小）：新请求走新配置，在途请求在旧配置上走完；新配置有误时记日志、继续用原配置。

| 字段 | 默认 | 说明 |
|---|---|---|
| `listen` | `127.0.0.1:28770` | 改了即换监听；新地址不可用时保留原监听，其余改动照常生效 |
| `upstream` | （必填） | 网关地址，不带路径 |
| `wait_for_upstream` | `true` | 网关不可用时是否等待、并在恢复后重发；关掉则失败立即回 502 |
| `upstream_wait` | `60s` | 每个请求最多等多久，超时回 502 |
| `health_path` | `/health` | 探活路径，返回 200 即可用 |
| `response_header_timeout` | `5m` | 发出请求后等响应头的上限，`0s` 不限。网关等账号空位、等上游首包时会合法地迟迟不回响应头（粘性会话等位最长 120 秒），要留足余量 |
| `catalog_cache` | `true` | 目录缓存；关掉则目录请求原样转发。只管带 `client_version` 的 codex 格式目录 |
| `catalog_path` | `/v1/models` | |
| `catalog_timeout` | `3s` | 目录回源超时，超时即用缓存；要小于 codex 的 5 秒 |
| `compress` | `true` | 上行压缩 |
| `compress_paths` | `/v1/responses`、`/v1/responses/compact`、`/v1/alpha/search` | 只能填网关读请求体时会解压的路径 |
| `compress_min_bytes` | `1024` | 更短的不压缩 |
| `zstd_level` | `3` | 1–22；实测最高档比默认只再省约 10%，耗时多 5–10 倍 |
| `log_file` | 空（stderr） | 可写 `~/...`；超过 10 MiB 时改名为 `.1` 再开新文件 |

时长写成字符串（`"90s"`、`"2m"`）。换了 `upstream` 或 `health_path` 时，在原地址上等待的请求立即回 502，
codex 重试时就走新地址；只改别的字段时等待照常继续。

## 行为细节

### 网关不可用时

| 情形 | 处理 | 客户端看到 |
|---|---|---|
| 拿到响应头之前连接失败（拨号失败、EOF、reset） | 把网关标为不可用，开始每秒探活 `/health`；请求等探活恢复后重发（请求体已整份缓存），代理自己最多尝试 5 次，过了 `upstream_wait` 不再发 | 恢复后的正常响应；`upstream_wait` 内没恢复则 502 |
| 发出请求后迟迟没有响应头（远端不可达时 TUN 会一直挂住） | 到 `response_header_timeout` 放弃，把网关标为不可用；不重发，网关可能仍在处理 | 504，codex 自己重试 |
| 已知不可用期间到达的新请求 | 先等探活恢复，不去撞网关 | 同上 |
| 响应开始后上游中断 | 不能重发；掐断客户端连接，网关标为不可用 | 流中断（不是正常结束），codex 自己重试，重试的请求会先等网关恢复 |
| 网关回的 HTTP 错误（4xx / 5xx，含 429、503 无可用账号） | 原样转发，不重发 | 网关的原始响应 |
| 目录请求 | 已知不可用时直接返回缓存；否则回源（`catalog_timeout`），失败再用缓存 | 200 的目录，或没有缓存时的错误 |

重发可能让网关收到同一请求两次（它已收下请求、却在回响应头前断开时），与 codex 自己遇到连接错误时重试的
后果相同。本地产生的错误用 OpenAI 的错误格式（`type: local_proxy_error`），codex 能显示出来。

### 模型目录

- 只处理带 `client_version` 的 `GET <catalog_path>`（codex 格式目录）；不带的是普通模型列表，原样转发。
- 缓存按（凭据摘要, 路径, 查询串）分文件存，凭据原文不落盘；不同 key 可能属于不同分组、目录不同，互不混用。
- 回源时带上缓存的 ETag 做条件请求，网关回 304 就返回缓存（目录约 550 KB，没变就不再传）。codex 不发
  `If-None-Match`、也不认 304，对它总是回 200。
- 查询串完全相同的缓存优先；没有时用同一凭据、查询串只差 `client_version` 的最新一份，codex 升级后第一次
  回源就失败也有目录可用。
- 只缓存 codex 收得下的目录（合法 JSON、不超过 1 MiB）。回源拿到 200 但 codex 用不了时返回缓存，没有缓存
  才原样转发；401、404 等原样转发，不用缓存掩盖。

### 压缩

- 只压缩 `compress_paths` 上的 POST、长度在 `compress_min_bytes` 到 64 MiB 之间、本身没有 `Content-Encoding`
  的请求体。网关解压后只读前 64 MiB、超出部分会被静默截断，所以更长的请求体发明文。
- 网关解压得到的请求体与原文逐字节一致；除 `Content-Encoding` 与长度外，请求头原样转发（UA、`originator`、
  `x-codex-*` 都不动），不加 `X-Forwarded-*`（网关默认信任这些头来认客户端 IP）。
- WebSocket 升级原样透传，帧内容不压缩。

## 开发

```bash
go test ./...      # 单元与集成测试（假网关覆盖：压缩、SSE 逐块转发、目录缓存、等待与重发、热加载、WebSocket）
./build.sh         # 交叉编译到 dist/
```

代码：`proxy.go`（转发与压缩）、`upstream.go`（闸门、探活、重发）、`catalog.go`（目录缓存）、
`config.go`（配置）、`app.go`（监听、热加载、日志）。

## 许可证

MIT，见 [LICENSE](LICENSE)。
