# 订单历史改名、QuantServer 整理与多分片：执行计划

本文件给接手的人执行。基线：提交 `eba08a0`（2026-10-01）。

每一步的通用要求：
- `bazelisk test //...` 全部通过；
- `bazelisk run //examples:replay_walkthrough` 最后一行余额为 BTC 0.01999、USDT 10.000999；
- 单独提交；
- 改名步骤只改名字，不改逻辑，`git diff -M` 应只显示改名。

**不改的东西**（所有步骤适用）：
- SQLite 表名和列名（`run_manifest`、`history_records`、`history_gaps` 等），否则已有数据库文件需要迁移；
- `record_codec` 的二进制格式；
- 管理协议里的请求类型字符串 `"history"`，以及 `schema_version`，保证新旧 bench 和 server 互通；
- 错误码的数字。

---

## 第 1 步：QuantServer 整理（`application/quant_server.*`）

| 改动 | 说明 |
|---|---|
| 去掉 `struct Impl` | `quant_server.h` 只有 `launcher.cc` 一处使用，Pimpl 没有收益。成员直接放进 `QuantServer` 的 `private:`；`Assemble`、`StartFeed`、`TimerLoop` 改为 `QuantServer` 的私有方法。成员声明顺序保持不变：`control_server` 仍在最后，保证最先销毁 |
| 拆分 `Assemble()` | 改为 `CreateComponents()`（只创建和连接部件，不处理数据）和 `RunReplay()`（回放模式下跑完回放文件并 Flush）。`Start()` 中：`CreateComponents()`；实时调用 `StartFeed()`，回放调用 `RunReplay()`；然后启动 ControlServer |
| 修复停止时可能永远卡住 | `RequestStop()` 设置 `stopping` 时没有持有 `stop_mutex`，`Wait()` 可能错过通知。改为在 `stop_mutex` 保护下设置 `stopping`，再 `notify_all` |
| 定时器周期判空 | `TimerLoop` 直接解引用 `*strategy->Triggers().timer_period`。改为：策略没有定时器周期时不启动 `TimerLoop` |
| 模式相关的默认值移到配置解析 | `Assemble()` 里有三处 `live ? A : B`：交易规则有效期（24h / 300s）、风控额度有效期（24h / 1h）、订单簿过期时间（5s / 60s）。改为在 `application/config.cc` 解析时按模式填入，`QuantServer` 只读最终值 |
| 简化 `run` 的计算 | 两个分支取的都是系统时间，合并为一个表达式 |

新增测试：`RequestStop` 和 `Wait` 在不同线程上反复并发执行（例如 1000 次），不能卡住。

---

## 第 2 步：history → order_history（只改名）

**目录和文件**

| 现在 | 改成 |
|---|---|
| 目录 `hquant/src/storage/`，Bazel 包 `//hquant/src/storage` | `hquant/src/order_history/`，`//hquant/src/order_history` |
| `storage.h` | `order_history.h` |
| `recorder.{h,cc}`、目标 `:recorder` | `order_history_writer.{h,cc}`、`:order_history_writer` |
| `history.{h,cc}`、目标 `:history` | `order_history_reader.{h,cc}`、`:order_history_reader` |
| `record_codec.{h,cc}`、`schema.sql` | 不变 |

**类型和函数**

| 现在 | 改成 |
|---|---|
| `HistoryRecord` / `HistoryRecordPayload` | `OrderHistoryRecord` / `OrderHistoryRecordPayload` |
| `HistoryWriter` / `SqliteHistoryWriter` | `OrderHistoryWriter` / `SqliteOrderHistoryWriter` |
| `HistoryReader` / `SqliteHistoryReader` | `OrderHistoryReader` / `SqliteOrderHistoryReader` |
| `HistoryGap` | `OrderHistoryGap` |
| `HistoryQuery` / `HistoryPage` | `OrderHistoryQuery` / `OrderHistoryPage` |
| `StorageHealth` | `OrderHistoryWriterHealth` |
| `RunManifest` | `RunInfo` |
| `HistoryRequest` / `HistoryResponse` / `HistoryJson` | `OrderHistoryRequest` / `OrderHistoryResponse` / `OrderHistoryJson` |
| `QuantServer::HistoryAsync` | `OrderHistoryAsync` |
| bench 子命令 `history` | `order-history` |

**错误码**（数字不变，名称字符串同步改，如 `HISTORY_QUEUE_FULL` → `ORDER_HISTORY_QUEUE_FULL`）

| 现在 | 改成 |
|---|---|
| `kStorageConfigInvalid`、`kStorageOpenFailed`、`kStorageWriteFailed`、`kStorageQueueFull`、`kStorageGapPersistFailed`、`kStorageQueryFailed` | `kOrderHistoryConfigInvalid`、`kOrderHistoryOpenFailed`、`kOrderHistoryWriteFailed`、`kOrderHistoryQueueFull`、`kOrderHistoryGapPersistFailed`、`kOrderHistoryQueryFailed` |
| `kHistoryQueueFull`、`kHistoryReaderStopping`、`kHistoryQueryTimeout`、`kHistoryCursorInvalid`、`kHistoryTooManyGaps`、`kHistoryRecordCorrupted` | `kOrderHistoryReaderQueueFull`、`kOrderHistoryReaderStopping`、`kOrderHistoryQueryTimeout`、`kOrderHistoryCursorInvalid`、`kOrderHistoryTooManyGaps`、`kOrderHistoryRecordCorrupted` |

注意：原来有两个"队列满"（`kStorageQueueFull` 是写入队列，`kHistoryQueueFull` 是查询队列），改名后分别叫 `kOrderHistoryQueueFull` 和 `kOrderHistoryReaderQueueFull`，不能撞名。

验收：`grep -rnw "HistoryRecord\|HistoryWriter\|HistoryReader\|HistoryGap\|HistoryQuery\|HistoryPage\|StorageHealth\|RunManifest\|kStorage[A-Z]\w*\|kHistory[A-Z]\w*" hquant apps examples dev` 为空（只检查 C++ 代码）。

---

## 第 3 步：上一次运行的记录（只改名）

| 现在 | 改成 |
|---|---|
| `RecoverySnapshot` | `PreviousRun` |
| `RecoveryContext` | `PreviousRunRecords` |
| `LoadRecoverySnapshot()` | `LoadPreviousRun()` |
| `RecoveryConfidence { Verified, Partial, Unresolved }` | `PreviousRunCompleteness`（值不变） |
| `crash_tail_possible` | `may_have_unwritten_records` |
| `needs_reconciliation` | `needs_order_query` |

---

## 第 4 步：对账 → 查订单（只改名）

| 现在 | 改成 |
|---|---|
| `AccountStreamParser` / `AccountStreamBatch` | `AccountPushParser` / `AccountPushBatch` |
| `ReconciliationClient` | `OrderQueryClient` |
| `ReconciliationClient::Query` / `DiscoverOpenOrders` / `DiscoverRecentOrders` | `QueryOrder` / `ListOpenOrders` / `ListRecentOrders` |
| `ReconciliationTarget` / `ReconciliationBatch` | `OrderToQuery` / `OrderQueryResult` |
| `PlanRestart` / `RestartReconciliationInput` / `RestartReconciliationPlan` | `PlanStartupQueries` / `StartupQueryInput` / `StartupQueryPlan` |
| `ReconciliationState { Confirmed, SubmissionUnknown, ResyncRequired }`（`order/order_tracker.h`） | `ConfirmationState { Confirmed, SubmissionUnknown, NeedsQuery }` |
| `OrderTracker::Reconcile()` | `ApplyQueriedOrder()` |
| `Recovery::Reconcile` | `Recovery::QueryOrder` |
| 错误码 `kReconcileResponseInvalid`、`kReconcileIdentityMismatch`、`kReconcileQueryFailed`、`kReconcileResponseTooLarge`、`kReconcileTargetInvalid` | `kOrderQueryResponseInvalid`、`kOrderQueryIdentityMismatch`、`kOrderQueryFailed`、`kOrderQueryResponseTooLarge`、`kOrderQueryTargetInvalid`（数字不变） |

验收：`grep -rni "reconcil" hquant/src apps` 只剩注释中必要的说明，代码标识符里没有。

---

## 第 5 步：分片打包（不改行为）

让现有的 `Shard` 直接持有一个分片需要的资源。`QuantServer` 持有一组 `Shard`，本步长度仍为 1；不引入额外的分片包装对象。

```cpp
class Shard {
  // 原有的订单簿、订单跟踪和事件处理仍在本类中。
  ShardId id;
  std::unique_ptr<Strategy> strategy;
  std::unique_ptr<SimpleSimulatedExchange> exchange;
  std::unique_ptr<RiskGate> risk;
  // 以下只在实时模式下创建
  std::unique_ptr<boost::asio::io_context> io;
  std::unique_ptr<HttpClient> http;
  std::unique_ptr<WebSocketClient> websocket;
  std::unique_ptr<binance_spot::MarketDataStream> stream;
  absl::Status stream_error;          // 只在本分片线程上读写
  std::thread thread;
};
// QuantServer：
std::vector<std::unique_ptr<Shard>> shards_;          // 每个分片一个
std::unique_ptr<SqliteOrderHistoryWriter> writer_;   // 全局共享（内部已按分片分队列）
std::unique_ptr<SqliteOrderHistoryReader> reader_;   // 全局共享
std::unique_ptr<ControlServer> control_server_;      // 全局共享，最后声明
```

`TimerLoop`、`StartFeed`、行情回调改为针对某一个 `Shard`。

验收：行为完全不变；status 和 order-history 的输出与改动前逐字一致（`run_id` 除外）。

---

## 第 6 步：多个独立分片

范围：**每个分片恰好一个交易对、一个策略、一个独立账户**。分片之间不共享任何交易状态。

### 6.1 配置规则（`application/config.cc` 和 `QuantServer::Create`）

| 规则 | 不满足时 |
|---|---|
| 分片数 1～8，`ShardId` 不重复 | `CONFIG_ASSIGNMENT_INVALID` |
| 每个 `assignments[]` 恰好一个交易对、一个策略、一个账户 | `LAUNCH_MULTIPLE_NOT_SUPPORTED` |
| 一个交易对只能出现在一个分片 | `CONFIG_ASSIGNMENT_INVALID` |
| 一个账户只能出现在一个分片（第 6 步不支持账户跨分片） | `LAUNCH_MULTIPLE_NOT_SUPPORTED` |
| 策略的交易对、账户必须与所在分片一致 | `CONFIG_REFERENCE_INVALID` |
| 用 `RiskBudgetAllocator` 检查：同一账户、同一资产，各分片额度之和不超过上限 | `CONFIG_BUDGET_INVALID` |
| 用 `ValidateRateBudgets` 检查限速额度 | `CONFIG_BUDGET_INVALID` |
| 实时模式：分片数不超过可用 CPU 核数（`std::thread::hardware_concurrency()`） | 启动失败，提示减少分片 |

去掉 `QuantServer::Create` 中"账户、交易对、策略、分片各只能 1 个"的限制，改为上表规则。

### 6.2 创建

- 按 `assignments` 为每个分片创建一个 `Shard`：它持有策略、模拟交易所（只持有本账户的初始余额）和风控（只设置本分片的额度）；
- 策略按配置里的 `strategy` 字段创建（解析后存在 `StrategyId.name` 里），目前只有 `simple_pmm`，写一个小的工厂函数；未知名称报 `CONFIG_FIELD_INVALID`；
- 所有分片共用同一个 `RunId`；记录按分片写入 Writer 各自的队列（`shard_sequence` 各分片独立递增）；
- 订单号生成器带上分片编号，保证不同分片之间不重复（例如 `S<run>-<shard>-<n>`）。注意：这会改变回放生成的订单号，`replay_walkthrough` 和相关测试的期望值要同步更新，并在提交说明里写明。

### 6.3 运行

| 模式 | 做法 |
|---|---|
| 实时 | 每个分片一个 `io_context` 和一个线程；每个分片为自己的交易对创建 `HttpClient`、`WebSocketClient`、`MarketDataStream`（交易所地址用全局的 `binance_endpoints` 配置）；每个分片一个 `TimerLoop` |
| 回放 | **仍在主线程上按顺序执行**，不开分片线程，保证结果可复现。回放文件格式升级到 `schema_version: 2`：每条输入增加 `market` 字段，按交易对找到所属分片后交给它；`timer` 输入可带 `shard` 字段，不带则发给所有分片。`schema_version: 1` 的文件只在单分片配置下接受 |

### 6.4 停止

`RequestStop()`：向**每个**分片的 `io_context` 投递"停止行情流和事件循环"。
`Wait()`：停止 ControlServer 接收新请求 → join **所有**分片线程 → Writer `Stop`（标记正常结束）→ 关闭 Reader → ControlServer 彻底停止并删除 socket。

### 6.5 status

- 实时模式：向每个分片投递取状态的任务，全部返回后合并。返回格式：
  ```json
  {"mode":"simulated","exchange":"simulated","shards":[{"shard":0,"market":"BTCUSDT","book":"Live",...},{"shard":1,...}],
   "recorder_dropped":0,"history_gaps":0,"recorder_error":""}
  ```
  任意一个分片 2 秒内未返回，该分片标记 `"error":"CONTROL_TIMEOUT"`，其他分片照常返回。
- 回放模式：直接读取各分片后合并。
- **兼容**：单分片时，在顶层继续输出原来的字段（`book`、`open_orders`、`balances`、`fees_paid` 等），旧的脚本和测试不受影响。

### 6.6 bench feed

`hquant_bench feed` 支持多个交易对：`--symbols BTCUSDT,ETHUSDT`，每个交易对独立推送深度和成交，WebSocket 路径按交易对区分。

### 6.7 验收

- 新增 `examples/simulated_replay_multi.yaml` 和对应的 v2 回放文件：2 个分片、2 个交易对、2 个账户。
- 新增 `quant_server_multi_shard_test`：
  - 回放 2 个分片：每个分片的余额、订单与分别单独回放时完全一致；
  - 实时 2 个分片（bench feed 推 2 个交易对）：两个分片都进入 Live、都有策略调用，status 合并正确；
  - 停止：所有分片线程都退出，`run_manifest` 标记正常结束；
  - 配置检查：上表每条规则各一个反例。
- 单分片配置（现有的 `simulated_replay.yaml`、`simulated_binance_pmm.yaml`）行为不变，status 顶层字段不变。

---

## 不在本计划范围

- **账户跨分片**：同一账户的订单分布在多个分片，需要把私有推送分发到各分片（`shard/routing` 已有实现，目前只有测试使用），以及模拟盘里跨分片共享余额。
- **一个分片多个交易对**：`Shard` 内的订单簿要从 1 个改为按交易对的 map，`StrategyInput` 要提供多个订单簿。
- 实盘下单网关接入 `Shard`。
- 订单簿内部重构（序号并入状态、缓存预分配）。

## 风险

| 风险 | 做法 |
|---|---|
| 改名时误改了 SQL 字符串或协议字符串 | 改名前用占位符保护 SQL 和协议字符串，改完还原；用"不改的东西"一节的清单逐项检查 |
| 两个"队列满"错误码改名后撞名 | 见第 2 步的注意事项 |
| 多分片后订单号改变，导致测试期望值大面积变化 | 订单号格式的改动单独提交，提交说明写明原因 |
| 某个分片出错拖垮全部分片 | 分片的行情错误只记录在本分片的 `stream_error`，并让本分片的风控紧急停止；不影响其他分片 |
| 停止时某个分片线程卡住 | `Wait()` join 每个线程前记录日志；测试覆盖多分片停止 |
| 实时多分片占满 CPU | 6.1 的核数检查 |
