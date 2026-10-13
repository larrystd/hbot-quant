# hquant

hquant 是一个 C++20 做市进程，用来验证交易链路的正确性和延迟。每个 Shard 负责一个交易对，处理顺序是：行情 → 策略 → 本地准入 → 发单。`TradingAccount` 负责与交易所对账、维护可靠日志，并把回报分发给各个 Shard。系统没有进程内的 Paper 撮合，也没有用真实资金的实盘模式。

## 两种交易模式

| `account.mode` | 行情与交易对端 | 资金 |
| --- | --- | --- |
| `pressure_sim` | 行情来自回放或本机行情服务；本机模拟交易所提供 REST 和私有 WS | 模拟交易所的独立账本 |
| `online_sim` | Binance Spot Demo 的公开行情、交易 REST 和私有 WS | Binance Demo 虚拟资金 |

两种模式共用同一套订单状态、资金状态、日志和 Binance 协议适配器。`online_sim` 把行情、交易和私有流三类连接都固定在 Demo 域名上，没有接入生产环境的配置项。旧的配置值 `external_sim`、`demo`、`live` 已经不再支持。

## 架构

```text
公开行情 WS（depth + trade 一条连接）／回放
  → Market（Syncing/Live 盘口状态机）
  → Strategy（纯函数 Decide）
  → ShardTradingState（风控 + 本 Shard 独占额度预留）
  → TradingAccount::SubmitFromShard（在 Shard 线程上执行）
       ├→ 账户镜像任务入队、SubmitIntent 日志入队
       └→ Shard 内 REST lane → 模拟交易所或 Binance Demo

REST 结果／私有 WS 回报／查单结果
  → TradingAccount（先写 order_journal.sqlite，提交后再应用）
  → 通知订单所属 Shard
```

### 线程

| 线程 | 数量 | 运行什么 |
| --- | --- | --- |
| Shard worker | `min(8, shard 数)` | 行情协程、盘口、策略、本地准入，以及绑定到该 worker 的 REST 连接 |
| Account worker | `min(2, 账户数)` | 账户任务队列、对账副本、全量对账、视图分发、REST 回调 |
| 私有流 | 每个账户 1 个 | Binance user data WebSocket |
| OrderJournal | 1 | SQLite 可靠日志（WAL + `synchronous=FULL`），每批最多等 250 µs 或 256 条 |
| SqliteHistory / ControlServer | 各 1 个 | 观察用历史 / Unix socket 控制面 |

### 三条规则

1. **单线程所有权**：Shard 状态只在 Shard 线程上改，账户状态只在账户线程上改；跨线程一律通过 `post` 投递。
2. **先日志，后状态**：交易所返回的事实（请求结果、回报、成交、余额、查单）先在 `order_journal.sqlite` 里 `COMMIT`，再应用到内存状态。重启时从这份日志重建状态。
3. **热路径本地准入**：启动时把账户资金切分给各个 Shard。下单时 Shard 只检查自己那一份，不等账户线程；账户线程随后异步更新对账副本。

### 行情与重连

公开行情只用一条组合流 WebSocket（`<symbol>@depth/<symbol>@trade`）。**每次重连都会清空盘口**：先等到第一条增量，再拿 REST 快照，按序号把快照和后续增量对齐，至少接上一条增量才进入 Live。交易所不会补发断线期间的增量，所以旧盘口不能继续用。公开成交只是事件，重连后接着收就行，不需要重建。盘口出现序号缺口、两边交叉或 5 秒没有更新时，同样会断开重连。

### 启动就绪

进程启动时先从日志恢复账户和 Shard 的状态，然后等私有流连上，再做一次全量对账：依次核对交易规则、挂单、逐笔订单与成交、余额。只有对账成功后，才打开下单准入。Shard 还会检查自己手上的账户视图版本，必须不早于对账完成时的版本才允许下单。之后每 60 秒做一次周期对账。

## 未知结果与查单兜底

请求已经写出、但迟迟没有返回时，系统无法判断交易所是否收到，所以既不当作成功，也不当作失败：

```text
t = 0     开始写 socket → 标记 writing，写 SubmitWriting 日志，安排 2 秒后查单
t = 2 s   本地超时 → 记 Unknown("local response timeout")，发第一次查单
t = 5 s   HTTP deadline → 结果为 Unknown（已开始写）；如果迟到的 200 先回来，则为 Confirmed
之后       查单按 1、2、4…32 秒退避重查；私有流回报若先到，直接确定状态
```

HTTP 结果分为四类：

| 情况 | 分类 | 后续处理 |
| --- | --- | --- |
| 还没开始写就出错 | `NotSent` | 确定没发出，直接释放资金占用 |
| 已开始写后出错，或 5xx/408/429/418，或错误码 -1000/-1001/-1006/-1007 | `Unknown` | 进入查单兜底 |
| 其它 4xx 的下单请求 | `Rejected` | 订单终结，释放占用 |
| 其它非 200 的撤单请求 | `Unknown` | 查单兜底；两次间隔查询都显示仍挂着，判为 `CancelRejected` |

结果未确定之前，这个 Shard 不会下新单。查单失败时不会把订单当作被拒，只会一直重查。如果订单其实没有到达交易所，下一次周期对账查这笔单会失败，账户随即暂停，需要人工确认。

### 暂停

大多数异常最终都会让账户暂停：`ready=false`，关闭准入。暂停后 Shard 不再下单，也不再撤单，已有挂单留在交易所。暂停期间只能用控制面的 `cancel_all` 撤单（撤单不受准入限制）。恢复的方式是成功完成一次全量对账，触发途径有三种：私有流重连、60 秒周期对账、控制面命令 `resume_trading` / `sync_account_state`。周期对账只有在上一次对账成功后才会安排。所以如果是对账本身失败导致的暂停，例如交易所上有本地不认识的挂单、某笔订单查不到、日志写失败，就需要人工处理。

## Binance Demo 单笔延迟（2026-10-13）

测试条件：Release 二进制，全新状态目录，BTCUSDT 一笔虚拟资金订单，最终状态为 `CANCELED`，成交 0，独立对账 `valid=true`。时间戳取自客户端本机的 `CLOCK_MONOTONIC`。除公开行情两行外，都是**单笔观测值**，不能当作 p50/p99。

| 阶段 | 边界 | 实测 |
| --- | --- | --- |
| 公开行情 | WS 读取返回 → 解析完成（12 个样本） | p50 `0.958 µs`，p99 `5.250 µs` |
| 公开行情 | 解析完成 → 盘口/Shard 处理完成（同一批样本） | p50 `0.500 µs`，p99 `1.083 µs` |
| 策略与准入 | `Strategy::Decide`；生成订单 ID；本地风控与资金预留 | `1.375 µs`；`12 µs`；`6 µs` |
| 发单准备 | 准入 → REST 入队；入队 → 分配到连接；分配 → 开始写 | `11 µs`；`7 µs`；`46 µs` |
| REST 写请求 | 开始写 → 写完 | `171 µs` |
| REST 等响应 | 写完 → 收到完整 HTTP 响应 | `211.827 ms` |
| 下单累计 | 策略产生订单 → 收到完整 HTTP 响应 | `212.080 ms` |
| 私有 `NEW` | 私有 WS 帧完整 → 解析完成；帧完整 → 本地账户记录完成 | `10 µs`；`692 µs` |
| 回报累计 | 策略产生订单 → 本地账户记录 `NEW` | `212.680 ms` |
| 撤单 REST | 入队 → 开始写；写完 → 收到完整 HTTP 响应 | `35 µs`；`213.535 ms` |

本地处理（从行情到开始写 socket）在 100 µs 以内，绝大部分时间花在"写完 → 响应"这一段。这一段包含网络往返和交易所处理时间；Binance 不提供服务端时间戳，所以无法再细分。`SubmitFromShard` 从准入到返回耗时 `157 µs`，这段时间与 REST 的分配和写入重叠，不能直接加进上表的累计值。

## 构建与测试

```sh
bazelisk --batch build //hquant/src:hquant_server
bazelisk --batch test //hquant/test:all
```

本机模拟交易所压测：1000 个标的，运行 30 秒。每次运行都要用一个空的 `--state-dir`。

```sh
bazelisk --batch build --config=release //hquant/src:hquant_server //hquant/bench:trade_chain_bench
bazel-bin/hquant/bench/trade_chain_bench \
  --symbols 1000 --seconds 30 --rest-connections 16 \
  --state-dir /private/tmp/hquant-trade-run
```

压测器在同一个进程里运行模拟交易所，生成 `config.yaml` 后启动 `hquant_server`。状态目录里会留下 `history.sqlite`、`order_journal.sqlite`、`probe.json`、各段 trace CSV 和服务日志。

## Binance 在线模拟

示例配置 `hquant/examples/binance_demo.yaml` 使用 `mode: online_sim`，默认 `max_submissions_per_run: 0`，即只读：对账完成后账户保持不就绪，不会下单。程序固定使用 Demo 的三个 TLS 域名，只允许一个账户、一个标的，单次运行最多下一笔订单；示例另外把额度限制在 20 虚拟 USDT。

Demo 凭据放在被 git 忽略的 `hquant/local/binance_demo_credentials.yaml` 里，不要提交，也不要打印到输出中。
