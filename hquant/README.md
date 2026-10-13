# hquant

`hquant/` 下有五个目录：

| 目录 | 内容 |
| --- | --- |
| `src/` | 服务本体 `hquant_server` |
| `test/` | 单元测试，以及 Demo 验证脚本 |
| `bench/` | 压测器和本机模拟交易所（`bench/venue`） |
| `examples/` | 配置示例 |
| `local/` | 本地凭据，已被 git 忽略 |

## src/ 分层

依赖只能从上往下。`trading/` 只认识 `TradePort` 接口，不认识 Binance。

| 目录 | 职责 | 运行在哪个线程 |
| --- | --- | --- |
| `runtime`、`main.cc`、`config` | 读配置，创建线程、账户、Shard 和交易客户端，把回调连起来；负责回放驱动和按顺序停机 | 主线程 |
| `control_server` | Unix socket 控制面：状态、历史、`set_strategy`、暂停/恢复/对账/全撤、`stop` | 控制线程 |
| `sqlite_history` | 观察用历史记录。队列满时记录 Gap，不影响交易，也不用于恢复 | 历史线程 |
| `shard/` | `BinanceFeed`（一条 WS 同时收 depth 和 trade，加 REST 快照）→ `Market`（Syncing/Live 盘口）→ `Strategy`（纯函数）→ `ShardTradingState`（风控、本 Shard 额度、订单表） | Shard worker |
| `trading/` | 订单状态机 `OrderState`/`OrderManager`、风控 `RiskLimits`、账户资金 `FundsState`、可靠日志 `OrderJournal`、账户 `TradingAccount`（含全量对账 `AccountSync` 和视图分发 `AccountViewPublisher`） | 账户 worker；日志有独立写线程 |
| `venue/binance/` | `BinanceTradeClient`（6 个优先级队列 + N 条 REST lane）、`BinanceUserStream`（私有流）、签名、规则解析、全局限频 `RequestScheduler` | REST 跑在 Shard worker 上，私有流有独立线程 |
| `base/` | 定点数（单位 1e-9，乘除中间结果用 128 位）、强类型 ID、协程式 HTTP/WS 客户端、Quill 日志 | — |

## 一笔订单的生命周期

1. 行情变化或刷新定时器触发后，`Strategy::Decide` 给出决定：撤掉全部订单，或者挂一对新的买卖单。
2. `ShardTradingState::BeginSubmit` 做风控检查，并从本 Shard 的额度里预留资金。
3. `TradingAccount::SubmitFromShard` 在 Shard 线程上依次做三件事：把账户镜像任务放进队列，把 `SubmitIntent` 放进日志队列，然后 `port->Submit`。
4. REST lane 按优先级取出请求，经过限频和签名后写出。开始写的那一刻会通知账户：账户把订单标记为 writing，并安排 2 秒后查单。
5. HTTP 结果分为 `NotSent`、`Unknown`、`Rejected`、`Confirmed` 四类。结果先写日志，再更新账户副本，然后通知 Shard。
6. 私有流推来的回报和成交也先写日志、再应用，然后转给订单所属的 Shard。Shard 发现自己的订单都已撤完，就立即重新报价。
7. 如果结果一直不确定，就靠查单兜底：查单按 1 到 32 秒退避重查，不会把订单当作被拒。账户状态出现任何矛盾，都会暂停准入，直到全量对账成功。

## 测试

```sh
bazelisk --batch test //hquant/test:all
```

测试分两类：

- **C++ 单元测试**：覆盖订单状态、订单管理、资金、风控、日志、限频、规则解析、签名、视图分发、Shard 额度分配和交易客户端停机。
- **Python 脚本**：用于 Demo 环境的只读检查、单笔下单检查和延迟报告。它们需要 Demo 凭据，不在 `bazel test` 里运行。
