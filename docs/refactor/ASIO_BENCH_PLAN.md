# 网络统一到 Asio，以及压测工具：执行计划

本计划已按第 3 节的 7 个步骤执行。下文第 1 节保留实施前的现状记录；当前用法和验收命令见 [BENCH.md](../BENCH.md)。

| 步骤 | 提交 | 验收记录 |
| --- | --- | --- |
| 1–3 | `b59111e`、`386b3b3`、`d7372d1` | Asio 行协议、协程管理入口和信号正常收尾；各步全量测试和回放余额通过 |
| 4–6 | `853f26e`、`cb67dde`、`6a099b2` | 两个程序入口、控制入口压测、本地 feed 与交易所地址配置；各步全量测试和回放余额通过 |
| 7 | 本次文档提交 | 线程、连接、用法和统计字段同步 |

第 6 步本地验收：`--speed 1 --duration 20` 的 feed 推送回放夹具后，server 的 `book=Live`，余额 BTC `0.01999`、USDT `10.000999`；跳号和断线后均观测到重同步并继续应用增量。

## 0. 目标和已定决策

| # | 决策 |
|---|---|
| D1 | 所有网络收发统一用 **Boost.Asio（C++20 协程）**，包括管理入口 `ControlServer`；不再直接调用 `socket`/`accept`/`recv`/`send` |
| D2 | 只保留两个程序：**`hquant_server`**（原 `hquant_engine`，运行引擎）和 **`hquant_bench`**（连到 server 上的一切：`status`/`history`/`stop` 一次性命令 + 压测） |
| D3 | 删除现在的 `hquant` CLI：它的 `start` 和 server 重复，`status`/`history`/`stop` 并入 bench |
| D4 | 管理协议**保持不变**：本机 Unix socket（`<state_dir>/control.sock`），每条消息一行 JSON，`schema_version` 1/2，字段不变 |
| D5 | server 处理 **SIGINT/SIGTERM**，收到后走和 `stop` 完全相同的正常收尾流程（写 `clean_stopped_at_us`） |
| D6 | 交易所的 host、port、是否 TLS 改为从配置读取，不再写死 |

不在本计划范围：管理协议换成 HTTP/WS、远程访问和鉴权、实盘下单。

### 当前术语

| 术语 | 当前含义或历史名称 |
|---|---|
| `hquant/src/storage/` | `hquant/src/offline/` |
| `hquant/src/shard/` | `hquant/src/service/` |
| `Shard` | `ShardRuntime` |
| `SqliteHistoryReader` / `SqliteHistoryWriter` | `HistoryReader` / `SqliteRecorder` |
| `SimpleSimulatedExchange` | `PaperConnector` |
| `TickLotSize`、`connection_id` | `BookScale`、`stream_epoch` |
| `QuantServer` | 交易核心，已在 `application/quant_server.*` 中组装交易链路 |
| `ControlServer`、`ControlRequest`/`ControlResponse`、`control.sock`、`application/control_server.*`、`kControlBusy` 等 | 管理入口，已完成命名调整 |
| 管理线程 | 设计文档里的"控制线程" |

## 1. 现状（2026-10-01 核对）

| 项 | 现状 | 位置 |
|---|---|---|
| 交易所连接 | Asio + Beast：`HttpClient`、`WebSocketClient`，**只有 TCP 客户端**，没有服务端，也不支持 Unix socket | `base/net.h` |
| `HttpResponse` | 只有 `status`、`body`、`retry_after`，没有响应头 | `base/net.h:41` |
| 管理入口 `ControlServer` | POSIX 阻塞 socket；1 个 accept 线程 + 每个连接 1 个工作线程（最多 16 个）；手写按行分帧（上限 64 KiB）；收发各 2 秒超时 | `application/control_server.cc` |
| CLI 客户端 | POSIX 阻塞 socket，5 秒超时 | `cli/cli.cc:149` |
| history 查询 | 工作线程 `TrySubmit` 后，每 2ms 轮询一次 `TryReceive`，最多 2 秒 | `application/quant_server.cc` 的 history 分支 |
| 实时模式 status | `asio::post` 到分片 `io_context`，再用 `std::future` 阻塞等待，最多 2 秒 | `application/quant_server.cc` 的 status 分支 |
| 交易所地址 | 写死：`data-api.binance.vision:443`、`data-stream.binance.vision:443`，TLS 开启 | `application/quant_server.cc` |
| 信号处理 | **没有**。Ctrl-C/kill 直接杀进程，`run_manifest` 不写 `clean_stopped_at_us`，下次启动会被当作异常退出 | — |
| 线程（回放模式空闲时，实测） | 4 个：主线程、Recorder、HistoryReader、accept | — |

设计文档 `docs/DEPENDENCIES.md:98` 本来就要求"控制线程有自己的 `io_context` 处理 Unix socket"，`docs/ARCHITECTURE.md` 要求 history 结果"异步返回控制线程"。本计划把实现对齐到设计（下文称"管理线程"）。

## 2. 目标结构

### 2.1 net 层：传输 → 分帧 → 协议，客户端和服务端对称

```
              客户端                                   服务端
协议层   HttpClient  WebSocketClient  LineClient     HttpServer  WebSocketServer  LineServer
分帧层   HTTP 报文    WS 帧           一行 JSON+'\n'   （同左）
传输层   ───── TCP / TCP+TLS / Unix socket（asio::local::stream_protocol）─────
```

- **传输层**：把 TCP 和 Unix socket 统一起来（模板参数或一个小的 `Stream` 抽象）。现有 `HttpClient`/`WebSocketClient` 继续只用 TCP，接口不变。
- **`LineClient`**：`Connect(endpoint, deadline)`、`Send(line, deadline)`、`Receive(deadline) -> StatusOr<std::string>`。用 `async_read_until('\n')`，帧上限可配置（默认 64 KiB），超时沿用 `WebSocketClient` 已有的 deadline 写法。
- **`LineServer`**：一个 acceptor 协程，每接受一个连接就 `co_spawn` 一个会话协程。会话里读一行 → 调 handler → 写一行 → 关闭。并发上限（默认 16）、读写超时（默认 2 秒）、帧上限都做成参数。超限时回 `CONTROL_BUSY`，行为和现在一致。
- **`HttpServer`、`WebSocketServer`**（第 6 步才需要）：基于 Beast，供 bench 假扮交易所用。
- **`HttpResponse` 加 `headers`**。
- 错误一律用 `base/error.h` 的错误码（`kNetTimeout`、`kNetNotConnected`、`kNetConcurrentCall` 等）。

### 2.2 server（hquant_server）的线程模型

| 线程 | 改后 | 说明 |
|---|---|---|
| 主线程 | 启动流程，然后等待"停止"事件 | 不再每 20ms 轮询 `stopping` |
| **管理线程** | 新增：独立 `io_context`，运行 `LineServer` 和信号处理 | 取代 accept 线程和所有工作线程 |
| Recorder 线程 | 不变 | |
| HistoryReader 线程 | 不变，但查完后把结果 `post` 回调用方的 executor | 不再被轮询 |
| 分片线程 | 不变（仅实时模式） | |

回放模式空闲时仍是 4 个线程（主线程、管理线程、Recorder、HistoryReader），但请求处理不再创建任何线程。

### 2.3 管理入口改成协程

```cpp
// 改前
using Handler = std::function<ControlResponse(const ControlRequest&)>;
// 改后
using Handler = std::function<asio::awaitable<ControlResponse>(ControlRequest)>;
```

- **status（实时模式）**：在分片 executor 上 `co_spawn` 一个生成状态的任务，以 `use_awaitable` 等待结果；超时 2 秒用 timer 和 `awaitable_operators` 实现。管理线程不阻塞。
- **status（回放模式）**：回放在启动阶段已跑完，可以直接读。
- **history**：`SqliteHistoryReader` 新增异步接口，例如 `Submit(query, executor, callback)`，或者返回 awaitable 的 `Query(query)`。查完把 `HistoryPage` `post` 回管理线程。队列满时仍返回 `HISTORY_QUEUE_FULL`，超时返回 `HISTORY_QUERY_TIMEOUT`。
- **stop / 信号**：管理线程上用 `asio::signal_set` 等 SIGINT、SIGTERM。stop 请求和信号处理器都调用 `QuantServer::RequestStop()`；`QuantServer::Wait()` 负责收尾：停止接收新请求并等会话回复（包括 stop 回复）→ 停行情源和分片 `io_context` 并 join → Writer `Stop()` 写 `clean_stopped_at_us` → Reader 关闭 → 删除 `control.sock`。
- **生命周期**：handler 引用 `QuantServer` 拥有的组件。管理线程和所有会话结束后，才销毁分片、模拟交易所和 Writer。

### 2.4 bench（hquant_bench）

```
hquant_bench status  --state-dir DIR
hquant_bench history --state-dir DIR [--limit N] [--cursor C]
hquant_bench stop    --state-dir DIR
hquant_bench control --state-dir DIR [压测参数]       # 压 ControlServer 管理入口
hquant_bench feed    --listen 127.0.0.1:PORT [参数]  # 假扮交易所，压 QuantServer 交易核心
```

- **一次性命令**：建一个 `io_context`，`co_spawn` 一个 `LineClient` 请求，`io.run()`，打印 `data` 部分后退出。输出格式和现在的 `hquant status/history/stop` 保持一致，退出码规则也一致（参数错误 2，运行错误 1）。
- **线程模型**：`--threads T` 个线程，**每个线程一个 `io_context`**，连接按线程平均分配。不要让多个线程共用一个 `io_context`，以免压测工具自己成为瓶颈。
- **压测重点**：`feed` 测 QuantServer 的行情→撮合→策略→风控→历史主链路；`control` 测管理入口，作为附带的控制面负载。
- **发压方式**：
  - `--mode closed`：闭环，每个客户端收到回复后再发下一个，测最大吞吐；
  - `--mode open --rate R`：开环，按每秒 R 个固定节奏发，不等回复，测高负载下的延迟。
- **通用参数**：`--connections N`、`--duration S`、`--warmup S`（预热阶段不计入统计）、`--mix status=80,history=20`（请求比例）。
- **统计输出**：
  - 每种请求的发送数、成功数；
  - 吞吐（每秒请求数）；
  - 延迟 p50、p90、p99、p999、最大值；
  - 错误按错误码名称分类计数（如 `CONTROL_BUSY`、`HISTORY_QUEUE_FULL`、`NET_TIMEOUT`）。

  默认打印成表格，`--json` 输出一行 JSON，方便脚本收集和比较。
- **延迟直方图**：用固定桶（例如对数桶）的计数直方图，每个线程一份，结束时合并。不要在热路径上加锁或分配内存。

#### bench feed：假扮 Binance

server 的实时模式连接 bench 而不是 Binance（配置见 2.5）。bench 需要实现**引擎实际用到的那部分协议**（以 `market/market_data_stream.cc` 为准）：

| 接口 | 请求 | 回复 / 推送 |
|---|---|---|
| REST 快照 | `GET /api/v3/depth?symbol=SYM&limit=N` | `{"lastUpdateId":L,"bids":[["价格","数量"],...],"asks":[...]}`，价格和数量是十进制字符串 |
| WS | 路径 `/stream?streams=sym@depth/sym@trade` | 每条消息包在 `{"stream":"...","data":{...}}` 里 |
| 深度增量 | — | `data` = `{"e":"depthUpdate","E":时间,"s":"SYM","U":首序号,"u":末序号,"b":[...],"a":[...]}` |
| 公开成交 | — | `data` = `{"e":"trade","E":时间,"s":"SYM","t":成交ID,"p":"价格","q":"数量","m":买方是否挂单方}` |

- **行情来源**：读回放文件（与 `examples/replay_market.json` 相同的格式），把 ticks/lots 换算成十进制字符串后推送；或者用参数生成随机游走的盘口。`--rate` 控制每秒推送条数，`--speed` 控制回放倍速。
- **序号必须连续**；快照的 `lastUpdateId` 必须和推送的序号对得上，否则引擎会不断重新同步。
- **故障注入参数**：`--gap-every N`（每 N 条故意跳号）、`--disconnect-every S`（每 S 秒断开一次）、`--http-429-rate P`（快照请求按概率回 429）。用来压引擎的重新同步和退避逻辑。
- **统计**：bench 只知道自己推了多少条。引擎这一侧的处理量，从 server 的 `status` 读取。建议在 status 里增加计数：已应用的增量条数、重新同步次数、策略调用次数、记录器丢弃数。bench 结束时自动调用一次 status，并打印对照。

### 2.5 交易所地址配置（D6）

`market_specs` 或新增顶层 `exchange_endpoints` 段，例如：

```yaml
exchange_endpoints:
  binance:
    rest: {host: data-api.binance.vision, port: 443, tls: true}
    websocket: {host: data-stream.binance.vision, port: 443, tls: true}
```

不写时默认用上面的公开地址，保证现有配置照常运行。压测时改成 `{host: 127.0.0.1, port: 18080, tls: false}`。新增 `examples/bench_feed.yaml` 作为压测用的 server 配置。`application/config.cc` 负责解析和校验；`quant_server.cc` 改为读配置。

## 3. 执行步骤

每一步都要满足：`bazelisk test //...` 全部通过；`bazelisk run //examples:replay_walkthrough` 最后一行余额为 BTC 0.01999、USDT 10.000999；单独提交。

| 步骤 | 内容 | 验收 |
|---|---|---|
| **1** | net：传输层抽象（TCP + Unix socket）、`LineClient`、`LineServer`；`HttpResponse` 加 `headers` | 新增 `hquant/test/line_stream_test.cc`：正常收发；超过帧上限；读写超时；并发上限时回 BUSY；对端提前断开；Unix socket 和 TCP 各跑一遍 |
| **2** | `ControlServer` 改用 `LineServer` + 管理线程；handler 改为 awaitable；`SqliteHistoryReader` 改为完成后通知；实时模式 status 改为 `co_await` | 删掉 `control_server.cc` 里所有 POSIX socket 代码和工作线程；`control_server_test` 的行为断言全部通过；回放模式空闲时 `ps -M` 仍为 4 个线程，发请求时线程数不增加 |
| **3** | 信号处理器调用 `QuantServer::RequestStop()`（D5）；统一的停止流程 | 新测试：启动 server → 发 SIGTERM → 进程正常退出，`run_manifest.clean_stopped_at_us` 不为空，`control.sock` 已删除；SIGINT 同样验证 |
| **4** | 新建 `apps/hquant_bench.cc`，实现 `status`/`history`/`stop`；`hquant_engine` 改名为 `hquant_server`；删除 `apps/hquant.cc`、`hquant/src/cli/`（参数解析和输出格式化移到 bench 里）；更新 `apps/BUILD.bazel`、`op.sh`、文档中的命令示例 | `cli_test` 迁移为 `bench_cli_test`，覆盖原来的参数解析和输出格式断言；用 bench 对回放 server 跑一遍 status → history → stop |
| **5** | `hquant_bench control`：多线程、闭环和开环、请求比例、统计输出（表格和 JSON） | 对回放 server 跑 `--connections 64 --duration 10`：无崩溃，错误分类正确；`--connections 100`（超过 16）时能看到 `CONTROL_BUSY` 计数 |
| **6** | 交易所地址配置（D6）；net 增加 `HttpServer`、`WebSocketServer`；`hquant_bench feed`；status 增加处理计数 | 本机端到端：bench feed 推 `replay_market.json` → server 订单簿进入 Live、策略挂单、模拟成交，结果与回放模式一致；`--gap-every`、`--disconnect-every` 下 server 能重新同步并继续运行 |
| **7** | 文档：`docs/ARCHITECTURE.md` 的线程表、`docs/CONNECTIONS.md` 的连接表、README 的用法；新增 `docs/BENCH.md`，说明压测命令、参数、输出字段、典型用法 | — |

## 4. 风险和注意事项

| 风险 | 说明 / 做法 |
|---|---|
| 协程捕获引用的生命周期 | handler 和会话协程引用 `QuantServer` 的组件。停止时要先取消 acceptor 和所有会话，join 管理线程，之后才能让这些对象析构 |
| 管理线程不能碰分片状态 | 实时模式下只能通过分片 executor 取数据（`co_spawn` 到分片 executor），不能直接读 |
| Unix socket 路径长度 | macOS 的 `sun_path` 只有 104 字节；`LineClient`/`LineServer` 要显式检查并报 `kCliUsageInvalid` 或 `kControlSocketFailed`，不要让 Asio 抛异常 |
| Asio 异常 | 统一用 `redirect_error` 或 `as_tuple` 拿 `error_code`，转换成 `base/error.h` 的错误码；会话协程里的异常不能让整个管理线程退出 |
| 协议兼容 | 第 4 步之前，新 server 必须能和旧 CLI 互通；第 4 步之后删除旧 CLI。协议格式不变，`control_server_test` 里的报文断言就是兼容性检查 |
| bench 自身成为瓶颈 | 每线程一个 `io_context`；统计用每线程直方图，最后合并；不在发送路径上打印日志 |
| 假交易所协议漂移 | bench feed 的报文生成和 `market/market_data_stream.cc` 的解析器各自独立实现。在 `market_data_stream_test` 里加一条用例：bench 生成的报文必须能被解析器解析 |
| 依赖变化 | `hquant_bench` 和 `ControlServer` 会依赖 Boost.Asio，编译时间和程序体积会增加，这是预期之内的 |

## 5. 完成后的状态

```
hquant_server CONFIG STATE_DIR        运行引擎；Ctrl-C/SIGTERM 正常停止
hquant_bench  status|history|stop     运维命令
hquant_bench  control ...             压 ControlServer 管理入口
hquant_bench  feed ...                假扮交易所，压 QuantServer 交易核心
```

所有网络收发都在 `base/net`（Asio）上；请求处理不再创建线程；管理线程、history 异步返回、停止流程和设计文档一致。
