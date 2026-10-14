# sub2api-local-proxy

codex 客户端与 sub2api 网关之间的本地反向代理，跑在每台客户端机器上（Windows / Linux / macOS），
codex 的 `base_url` 与 `model_catalog_url` 指向它。请求原样转发给网关，只多做三件事：

1. **模型目录缓存**：codex 拉目录时每次先回源；回源失败、网关回 5xx / 429、目录 codex 用不了或 3 秒内没回时，
   返回最近一次成功的目录，慢的回源在后台做完再刷新缓存。
2. **上行 zstd 压缩**：`/v1/responses` 等请求体压缩后发给网关（网关读请求体时会解压）。
3. **网关暂时不可用时等它恢复**：隧道断、网关重启期间到达的请求先等，拿到响应头之前失败的请求在恢复后重发。
4. **网关排队类 429 自动重发**：会话所在账号一时忙时网关回的 429，在有限预算内退避后重发同一请求。

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

只要程序本身也可以 `go install github.com/SomeoneKong/sub2api-local-proxy@latest`，再手动写配置、设开机启动。

所有平台的程序、配置、目录缓存都在 `~/.sub2api-local-proxy/`（`bin/`、`config.json`、`cache/`）。
安装脚本重复执行即升级：换程序、重启，配置不动。新程序先暂存并用它校验配置，通过了才停旧实例、替换、
启动，校验不过时正在运行的实例不受影响；启动后检查进程确实在运行，没起来就报错并打印日志。
重启会断开正在转发的请求（流式响应中途中断、等响应头的请求失败），客户端按各自的重试处理；改配置不需要重启。

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
| `retry_429` | `true` | 网关排队类 429 是否由代理退避重发（见下文「网关回 429 时」）；关掉则原样转发 |
| `retry_429_codes` | `gateway_queue_full`、`gateway_concurrency_limit`、`""` | `error.type` 为 `rate_limit_error` 时，`error.code` 在这里的才重发；空串表示没有 code |
| `retry_429_max_wait` | `2m` | 从请求发出起算的重发预算：下一次重发会超过它就不再发，把最后一次 429 原样交回 |
| `retry_429_base_delay` | `2s` | 第一次重发前的间隔，之后每次翻倍 |
| `retry_429_max_delay` | `30s` | 间隔上限；实际间隔再乘 0.7–1.3 的随机系数 |
| `catalog_cache` | `true` | 目录缓存；关掉则目录请求原样转发。只管带 `client_version` 的 codex 格式目录 |
| `catalog_path` | `/v1/models` | |
| `catalog_timeout` | `3s` | 目录请求最多等回源多久，到点即用缓存；要小于 codex 的 5 秒 |
| `catalog_fetch_timeout` | `60s` | 目录回源本身的上限：请求不再等之后回源在后台继续，拿到目录照样写缓存；不能小于 `catalog_timeout` |
| `catalog_share_across_keys` | `false` | 本凭据没有缓存时借用其他凭据的缓存；只在全部 key 属于同一分组（目录相同）时打开 |
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
| 网关回的 HTTP 错误（4xx / 5xx，含 503 无可用账号；排队类 429 见下节） | 原样转发，不重发 | 网关的原始响应 |
| 目录请求 | 已知不可用时直接返回缓存；否则等回源最多 `catalog_timeout`，失败或到点用缓存，回源在后台继续 | 200 的目录，或没有缓存时的错误 |

重发可能让网关收到同一请求两次（它已收下请求、却在回响应头前断开时），与 codex 自己遇到连接错误时重试的
后果相同。本地产生的错误用 OpenAI 的错误格式（`type: local_proxy_error`），codex 能显示出来。

### 网关回 429 时

网关按会话粘性把一个会话固定在一个账号上。这个账号并发满时请求在它的队列里等，队满（`gateway_queue_full`）
或等太久（`gateway_concurrency_limit`）就回 429，别的账号此时可能还有空位，所以这种 429 只说明这一个账号一时忙。
codex 对 HTTP 429 一次也不重试，主回合直接失败、工具调用（如检索）直接报错，所以由代理等一等再发：

- 只重发 `error.type` 为 `rate_limit_error`、`error.code` 在 `retry_429_codes` 里的 429（默认还包括没有 code 的，
  即网关报上游账号都在限速）；额度用完、key 的 RPM／时段限额等其它 429，以及超过 8 KiB、带压缩或不是 JSON 的
  429 响应体，都原样转发。
- 429 的响应体要在 10 秒内（且不超过下文的预算截止）读完才能判断。网关的错误体随响应头一起写出，读不完说明
  连接出了问题：这次发送被中止，重发时交回上一次的 429，第一次发送时客户端拿到已读的部分与随后的连接中断。
- 重发的是同一请求，头与请求体不变，粘性键不变，仍回到同一个账号，上游的 prompt cache 不丢。
- 间隔从 `retry_429_base_delay` 起翻倍、不超过 `retry_429_max_delay`，再加 ±30% 抖动；响应带 `Retry-After`
  （秒数或 HTTP 日期）时它是下限，在它之上再加 0–20% 的随机余量。
- 预算从请求进来时起算，第一次发送在网关里排队的时间也算在内：
  - 等完下一个间隔会超过 `retry_429_max_wait` 就不再发，把这次 429 原样交回；
  - 重发出去的请求到 `retry_429_max_wait` 还没拿到响应头就取消，交回上一次的 429；
  - 第一次发送拿到 429 响应头时预算已用完，就不读响应体、原样交回；
  - 所以一个请求拿到响应头的时间最多是「第一次发送的时长」与 `retry_429_max_wait` 中较大的一个。第一次发送
    仍只受 `response_header_timeout` 限制，没遇到 429 的请求不受影响；拿到响应头之后的响应体（SSE）不受预算限制。
  - 更长的拥堵交给调用方处理（例如结束这次会话、释放槽位，过一阵再续跑）。检索请求的 429 常是在忙账号的队列里
    等满约 120 秒后才回的，此时预算已用完，直接交回。
- 等待期间不向客户端发任何字节；codex 在拿到响应头之前不计空闲超时（实测静默 10 分钟仍正常完成）。
- 客户端离开即停止；配置换了网关地址时，正在退避的请求立即回 502，不再向旧地址重发。
- 发生过重发的响应（包括重发后失败、本地回的 502／504）带 `X-Local-Proxy-Retries`（次数）与
  `X-Local-Proxy-Wait-Ms`（退避等待的总毫秒数，不含各次发送在网关里排队的时间）两个头。
- 日志：每次决定重发记一行（code、等待对象、本地请求 ID `lid`、`X-Request-ID`、这次发送用时、间隔）；请求结束的
  那一行在最终回了 429 或重发过时，末尾追加 `lid= code= scope= retries= wait= fail= end= req_id= sess=`。
  - `lid` 是代理给每个请求生成的 ID，同一请求的重发行与结束行用它对上（各次发送的 `X-Request-ID` 互不相同）。
  - `scope` 是等待对象（account／user）；`sess` 是 `session-id` 头的前 8 位。
  - `wait` 是退避等待的总时长。`fail` 是失败发送的总时长：各次收到 429 的发送累计到响应头（包括连接恢复后收到
    429 的那次），重发到预算仍没收到响应头的发送累计到取消；不计等网关恢复、连接失败的发送及其重试间隔。
  - `end` 是结局：`ok` 重发后成功、`pass` 不在重发范围、`budget` 下一次间隔超出预算、`deadline` 重发在预算内
    没拿到响应头、`error` 重发失败或网关已换、`cancel` 退避中客户端离开。
  - 行首的秒数是请求总耗时，含成功那次发送的排队与之后的整段响应体，不能拿它减去 `wait`、`fail` 当作等待。

排队超过网关心跳间隔（默认 10 秒）的流式请求，网关已先回 200 再在流里发 `response.failed`，这种 429 代理看不到，
由 codex 自己的流重试处理。

### 模型目录

- 只处理带 `client_version` 的 `GET <catalog_path>`（codex 格式目录）；不带的是普通模型列表，原样转发。
- 缓存按（凭据摘要, 路径, 查询串）分文件存，凭据原文不落盘；不同 key 可能属于不同分组、目录不同，默认互不混用。
- 回源时带上缓存的 ETag 做条件请求，网关回 304 就返回缓存（目录约 550 KB，没变就不再传）。codex 不发
  `If-None-Match`、也不认 304，对它总是回 200。
- 请求最多等回源 `catalog_timeout`，到点就返回缓存；回源不随请求取消，在后台做到 `catalog_fetch_timeout`，
  拿到目录照样写缓存。网关目录接口比 `catalog_timeout` 慢时，缓存靠它刷新，新 key 的第一份缓存也靠它攒下：
  新 key 上线前带它经代理请求一次目录，等网关回完即预热。比 `catalog_timeout` 慢的回源结束时记一行
  `后台回源` 日志。
- 同一（凭据, 路径, 查询串）同时只有一个回源，后到的请求等同一个结果，网关慢时不会一个请求一次地堆上去。
  客户端自己带条件头（`If-None-Match`、`If-Modified-Since`、`If-Match`、`If-Unmodified-Since`、`If-Range`）或
  `Range` 的请求单独回源、结果原样交给它。
- 配置换了网关地址后，原地址上还没做完的后台回源不再写缓存，盖不掉新地址取得的目录。
- 取缓存的顺序：本凭据查询串完全相同 → 本凭据只差 `client_version` 的最新一份（codex 升级后第一次回源就失败
  也有目录可用）→ 打开 `catalog_share_across_keys` 时，其他凭据查询串完全相同 → 其他凭据只差 `client_version`。
  借用其他凭据的缓存时响应头 `X-Local-Proxy-Catalog` 为 `cache-shared`，日志写明借自哪个凭据摘要。
- 只缓存 codex 收得下的目录（合法 JSON、不超过 1 MiB）。回源拿到 200 但 codex 用不了时返回缓存，没有缓存
  才原样转发；401、404 等原样转发，不用缓存掩盖。
- 目录回源出错（连接失败、响应读不完整等）时把网关标为不可用；回源自己到点或被取消不算，目录接口慢不说明
  网关连不上。

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
