# QuantServer：把交易核心理清楚

执行状态：第 1–5 步分别提交为 `a4bcaca`、`6e75d37`、`dd12ae7`、`e0eddaf`、`25d2c30`；停止顺序修正见 `a6171d1`。第 6 步与 Asio 计划术语调整在文档提交中完成。回放订单簿原有默认过期时间为 60 秒、实时模式为 5 秒，因此第 4 步分别保留这两个值。

本文件给接手的人执行。前提：命名统一已完成并提交（`b904d28`）。本计划排在 [ASIO_BENCH_PLAN.md](ASIO_BENCH_PLAN.md) **之前**执行；Asio 计划需要按第 6 节调整。

## 0. 结论先行

| # | 决策 |
|---|---|
| Q1 | **QuantServer = 交易核心**：收行情 → 订单簿 → 策略 → 风控 → 下单/撤单 → 成交 → 记录。这是 `hquant_server` 进程存在的意义 |
| Q2 | 现在叫 `QuantServer` 的类只是管理入口（查状态、查历史、停止），**改回 `ControlServer`** |
| Q3 | 交易核心现在没有对应的类，部件是在 `application/launcher.cc` 的两个大函数里用局部变量拼起来的。**新建 `QuantServer` 类**，拥有全部部件，负责组装、运行、停止 |
| Q4 | **行情驱动策略**：行情、公开成交、自己的成交都可以触发策略，不再只靠定时器 |
| Q5 | 本计划**不改变交易结果**：回放模式的最终余额、订单和记录必须与改动前完全一致（Q4 除外，见第 5 步的验收说明） |

## 1. QuantServer 包含什么

```
                         ┌──────────────────────── QuantServer ────────────────────────┐
  行情源                  │                                                              │
  ├ 回放文件 ─────────────┤►  Shard（分片线程）                                             │
  └ 交易所 WS/REST ───────┤     ├ OrderBookSync     订单簿同步与存储                         │
（MarketDataStream）    │     ├ Strategy          决策（SimplePmm）                        │
                         │     ├ ActionExecutor    风控冻结 → 准备 → 记录 → 发出              │
                         │     ├ RiskGate          资金额度与冻结                            │
                         │     ├ OrderTracker      订单状态机                                │
                         │     └ 交易所接口         SimpleSimulatedExchange（实盘：下单网关）   │
                         │                                                              │
                         │   HistoryWriter（Writer 线程） ← 所有记录入队                     │
                         │   HistoryReader（Reader 线程）  只读查询                          │
                         │   ControlServer（管理入口）     status / history / stop          │
                         └──────────────────────────────────────────────────────────────┘
```

| 部件 | 职责 | 线程 | 现状位置 |
|---|---|---|---|
| 行情源 | 回放：读文件、推进 `ReplayClock`；实时：`binance_spot::MarketDataStream` | 回放：调用方线程；实时：分片线程 | `market/replay_feed`、`market/market_data_stream`（第 2 步后） |
| `Shard` | 一个分片的全部交易状态和处理流程 | 分片线程 | `shard/shard.*` |
| `Strategy` | 根据 `StrategyInput` 产出 `ActionBatch` | 分片线程 | `strategy/` |
| `ActionExecutor` | 执行动作：`TryHold` → `PrepareSubmit` → 记录 → `StartPrepared`；撤单 | 分片线程 | `shard/action_executor.*` |
| `RiskGate` | 额度检查、资金冻结、成交记账、解冻 | 分片线程 | `order/risk.*` |
| `OrderTracker` | 订单状态、成交去重 | 分片线程 | `order/order_tracker.*` |
| `SimpleSimulatedExchange` | 本地撮合 | 分片线程 | `order/simulated_exchange.*` |
| `SqliteHistoryWriter` | 记录批量落盘 | Writer 线程 | `storage/` |
| `SqliteHistoryReader` | 历史分页查询 | Reader 线程 | `storage/` |
| `ControlServer` | 管理请求入口 | 管理线程（Asio 计划前：accept 线程 + 每请求一个线程） | `application/quant_server.*`（本计划改名） |

### 三条数据流（都在分片线程上）

1. **行情 → 撮合**：增量 → `OrderBookSync` → `Shard::OnBookApplied` → 模拟交易所用买一卖一撮合已有挂单 → 成交回报 → `OrderTracker` / `RiskGate` / 记录。
2. **触发 → 下单**：触发（现在只有定时器；第 5 步后加入行情和成交）→ `Shard` 组装 `StrategyInput` → 策略 → `ActionExecutor` → 挂单/撤单 → 登记 → 取回报。
3. **回报 → 状态**：模拟交易所 `events_` → `DrainSimulatedExchangeEvents` → `OrderTracker`、`RiskGate`、记录。

## 2. 现状问题（2026-10-01 核对）

| 问题 | 位置 |
|---|---|
| 没有 QuantServer 类；实时模式 `RunSimulatedBinanceEngine`（第 88–300 行）和回放模式（`Launch` 第 304–431 行）各自组装一遍：策略、模拟交易所、风控、Writer、Reader、Shard、handler、主循环，**大段重复** | `application/launcher.cc` |
| 管理入口的 handler 用 `[&]` 捕获十几个局部变量；生命周期全靠"函数还没返回" | 同上，第 223 行、第 387 行 |
| 停止靠主线程每 20ms 轮询 `stopping` | 第 296、429 行 |
| 写死的参数：手续费率 `0.001`、价格类型 `PmmPriceType::Mid`、风控 `fee_buffer_rate 0`；风控规则有效期（实时 24h / 回放 300s）、额度有效期（实时 24h / 回放 1h）；订单簿过期时间 `5'000'000`；Writer 队列 `1024, 64`；Reader `32, 500`；定时器 1 秒 | 第 134–159、205、351–382 行 |
| 策略只由定时器触发；`TriggerPolicy` 已定义但除 `timer_period` 外没人读 | `strategy/strategy.h`、`shard/shard.cc` |
| 管理入口的类名叫 `QuantServer`，与它的实际作用不符 | `application/quant_server.*` |

## 3. 目标结构

```cpp
// application/quant_server.h（第 1 步改名后，此文件名留给交易核心）
class QuantServer {
 public:
  // 校验配置并组装全部部件；不启动线程，不连网络
  static absl::StatusOr<std::unique_ptr<QuantServer>> Create(const AppConfig& config,
                                                             std::string state_dir);
  absl::Status Start();        // 启动 Writer/Reader、行情源（回放：同步跑完）、分片线程、ControlServer
  absl::Status Wait();         // 阻塞到停止完成，返回最终状态
  void RequestStop();          // 线程安全；ControlServer 的 stop、将来的信号处理都调它

  // 供 ControlServer 调用；内部负责投递到正确的线程
  absl::StatusOr<std::string> Status();
  absl::StatusOr<HistoryPage> History(HistoryQuery query);

 private:
  // 行情源二选一：回放文件或交易所行情流
  // 部件：clock、strategy、exchange、risk、writer、reader、shard、io_context、shard_thread、control_server
};
```

- `launcher.cc` 只剩：`Create` → `Start` → `Wait`。`hquant_server` 的 `main` 不变。
- 回放和实时的差别收敛到"行情源"一处：回放源在 `Start` 里同步跑完；实时源 `co_spawn` 到分片 `io_context`。
- 停止顺序固定在 `QuantServer` 里：停 ControlServer 接收新请求 → 停行情源和分片 `io_context`、join 分片线程 → Writer `Stop`（写 `clean_stopped_at_us`）→ Reader 关闭 → 删 socket 文件。
- **成员声明顺序 = 依赖顺序**，析构时逆序销毁：ControlServer 最后声明、最先销毁，保证 handler 不会访问已销毁的部件。

## 4. 执行步骤

每一步：`bazelisk test //...` 全部通过；`bazelisk run //examples:replay_walkthrough` 最后一行余额为 BTC 0.01999、USDT 10.000999（第 5 步按其说明）；单独提交。

### 第 1 步：管理入口改回 ControlServer（只改名）

| 现在 | 改成 |
|---|---|
| `QuantServer` | `ControlServer` |
| `ServerRequest` / `ServerResponse` / `ServerError` / `ServerRequestPayload` / `ServerResponsePayload` | `ControlRequest` / `ControlResponse` / `ControlError` / `ControlRequestPayload` / `ControlResponsePayload` |
| `Encode/DecodeServerRequest`、`Encode/DecodeServerResponse` | `Encode/DecodeControlRequest`、`Encode/DecodeControlResponse` |
| `SendServerRequest` / `MakeServerRequest` / `FormatServerResponse` / `ServerSocketPath` | `SendControlRequest` / `MakeControlRequest` / `FormatControlResponse` / `ControlSocketPath` |
| socket 文件 `quant_server.sock` | `control.sock` |
| `application/quant_server.{h,cc}`、目标 `:quant_server` | `application/control_server.{h,cc}`、`:control_server` |
| 测试 `quant_server_test`、`QuantServerTest`、`QuantServerHistoryTest` | `control_server_test`、`ControlServerTest`、`ControlServerHistoryTest` |
| 错误码 `kServerMessageInvalid` / `kServerBusy` / `kServerTimeout` / `kServerSocketFailed` / `kServerSocketInUse` | `kControlMessageInvalid` / `kControlBusy` / `kControlTimeout` / `kControlSocketFailed` / `kControlSocketInUse`（**数字不变**，名称字符串同步） |

同时更新 `docs/GLOSSARY.md`：
- 删除"QuantServer = 服务接口，不再使用 control"；
- 新增"QuantServer = 交易核心"和"ControlServer = 管理入口"。

参考提交 `ad09523`：它做的正好是反方向的改名，可以对照它的 diff 逐项还原。**不要直接 `git revert`**：之后的提交（目录移动、文档）改过同样的文件，直接 revert 会冲突。

验收：`grep -rn "QuantServer\|ServerRequest\|quant_server" hquant apps examples dev` 为空（文档和本计划除外）。

### 第 2 步：Binance 文件去掉前缀（只改文件名）

不新建目录，不拆分文件；文件内容、类名、命名空间（`hquant::binance_spot`）都不变。以后接其他交易所时，再新增对应的类和文件。

| 现在 | 改成 |
|---|---|
| `market/binance_spot_feed.{h,cc}`、目标 `:binance_spot_feed` | `market/market_data_stream.{h,cc}`、`:market_data_stream` |
| `order/binance_spot_gateway.{h,cc}`、目标 `:binance_spot_gateway` | `order/order_gateway.{h,cc}`、`:order_gateway` |
| `order/binance_spot_account.{h,cc}`、目标 `:binance_spot_account` | `order/account_reports.{h,cc}`、`:account_reports` |

要做的事：
1. `git mv` 改文件名；修改 `market/BUILD.bazel`、`order/BUILD.bazel` 中的目标名。
2. 修改 include 路径和 Bazel 依赖：`application/launcher.cc`、`application/BUILD.bazel`、`hquant/test/`（含 `BUILD.bazel`）、`dev/`。
3. 测试文件同步改名：`binance_spot_feed_test` → `market_data_stream_test`，`binance_spot_gateway_test` → `order_gateway_test`，`binance_spot_account_test` → `account_reports_test`。

验收：
- `bazelisk test //...` 全部通过，示例程序余额不变；
- `git diff -M --stat` 只显示文件改名和 include 路径变化；
- `grep -rn "binance_spot_feed\|binance_spot_gateway\|binance_spot_account" hquant apps examples dev` 为空。

### 第 3 步：抽出 QuantServer 类（不改行为）

1. 新建 `application/quant_server.{h,cc}`，按第 3 节实现。
2. 把 `launcher.cc` 两个函数里的组装代码合并进 `QuantServer::Create`，两种模式共用；差异只在"行情源"。
3. ControlServer 的 handler 改为调用 `QuantServer::Status()`、`History()`、`RequestStop()`。线程处理保持原样：实时模式的 status 仍然 `post` 到分片 `io_context` 并等待；history 仍然 `TrySubmit` 加轮询。这些留给 Asio 计划改。
4. 主线程不再轮询 `stopping`，改为在 `Wait()` 里等条件变量。
5. `launcher.cc` 只剩 `Create` → `Start` → `Wait`。

验收：
- `launcher.cc` 少于 80 行；`QuantServer` 中没有按模式重复的组装代码；
- 回放：`hquant start` 加 status / history / stop 的输出与改动前逐字一致（status 里的 `run_id` 除外）；
- 实时：用 `simulated_binance_test` 覆盖，行为不变。

### 第 4 步：写死的参数进配置

| 参数 | 配置位置 | 默认值（保持现行为） |
|---|---|---|
| 模拟交易所手续费率 | `strategy_configs[].maker_fee_rate`、`simulated_exchange.maker_fee_rate` | `0.001` |
| SimplePmm 参考价类型 | `strategy_configs[].price_type: mid / last` | `mid` |
| 风控 `fee_buffer_rate`、交易规则最长有效期 | `risk.fee_buffer_rate`、`risk.max_rule_age` | `0`；实时 `24h`、回放 `300s` |
| 风控额度有效期 | `risk_budgets[].valid_for` | 实时 `24h`、回放 `1h` |
| 订单簿过期时间 | `market_specs[].stale_after` | `5s` |
| 策略定时器周期 | 由策略 `TriggerPolicy.timer_period` 决定，不再写死 1 秒 | `1s` |
| Writer / Reader 队列大小 | `storage.writer_queue`、`storage.writer_batch`、`storage.reader_queue`、`storage.reader_page_limit` | `1024`、`64`、`32`、`500` |

交易所地址留给 Asio 计划的 D6，本步不做。

验收：不写这些配置时，行为与第 3 步完全一致；`config_test` 覆盖每个新字段的解析和非法值。

### 第 5 步：行情驱动策略

1. `Strategy` 接口改为 `ActionBatch Decide(const StrategyInput&, Trigger why)`，其中 `Trigger ∈ {BookChanged, PublicTraded, OrderUpdated, Traded, Timer}`。`SimplePmm` 改为实现 `Decide`，逻辑不变。
2. `Shard` 新增私有函数 `RunStrategy(Trigger)`：组装输入 → 调用策略 → `ActionExecutor::Execute` → 登记 → 取回报。现有 `OnTimer` 改为调用它。
3. 触发点（**先撮合、再决策**）：
   - `OnBookApplied`：撮合并取回报之后，若 `BookApplyResult.top_changed` 且策略 `book_mode == BboChanged`（或 `EveryAppliedBatch`），调用 `RunStrategy(BookChanged)`；
   - `OnPublicTrade`：撮合之后，若 `on_public_trade`；
   - 处理成交回报后，若 `on_fill`；处理订单状态变化后，若 `on_order_update`。
4. **禁止递归**：`RunStrategy` 执行期间新来的触发只记一个"待处理"标记；本次结束后最多补跑一次。
5. **合并和限频**：实现 `TriggerPolicy.coalesce_window`（窗口内的多次触发合并为一次）和 `min_action_interval`（两次执行之间的最短间隔）。时间一律取自 `Clock`，回放时保持确定性。
6. `SimplePmm::Triggers()` 返回 `book_mode = BboChanged`，并保留 `timer_period`。

验收：
- 新增 `shard_trigger_test`：买一卖一变化时策略被调用；未变化时不调用；策略内下单立即成交时不会递归；合并窗口内多次变化只调用一次；同一输入跑两遍，结果完全相同；
- **回放结果允许变化**：策略被调用得更频繁，但 SimplePmm 受 15 秒刷新间隔限制，预期动作序列不变。如果 `replay_walkthrough` 的余额变了，必须在提交说明里逐条解释原因，不能直接修改期望值了事。

### 第 6 步：文档

- `docs/ARCHITECTURE.md`：补上 QuantServer 的部件图（第 1 节）和三条数据流；线程表里的管理线程使用 ControlServer 的名字。
- `docs/GLOSSARY.md`：见第 1 步。
- `README` 或 `docs/DEVELOPMENT.md`：`hquant start` / `status` / `history` / `stop` 的示例使用 `control.sock`。

## 5. 不在本计划范围

- 多分片（`ShardId` 仍固定一个）、多交易对、多策略；
- 实盘下单网关接入 `Shard`（`Shard` 目前只接受 `SimulatedExchange&`）；
- `OrderBookSync` 内部重构（序号并入状态、缓存改为预分配）；
- 网络统一到 Asio、bench：见 Asio 计划。

## 6. 对 ASIO_BENCH_PLAN.md 的调整

本计划完成后，修改 Asio 计划：

| 位置 | 改成 |
|---|---|
| 全文 `QuantServer` 指管理入口的地方 | `ControlServer` |
| "服务线程" | "管理线程"（运行 `ControlServer` 的 `io_context`） |
| `quant_server.sock`、`application/quant_server.*`、`kServer*`、`SERVER_BUSY` | `control.sock`、`application/control_server.*`、`kControl*`、`CONTROL_BUSY` |
| `hquant_bench server` | `hquant_bench control` |
| 2.3 节"stop / 信号"的停止顺序 | 改为调用 `QuantServer::RequestStop()`，停止顺序以本计划第 3 节为准 |
| 第 3 步（信号处理） | 信号处理器调用 `QuantServer::RequestStop()` |
| 压测重点 | 说明 `bench feed` 压的是 QuantServer 本体（交易核心），`bench control` 只是附带 |

## 7. 风险

| 风险 | 做法 |
|---|---|
| 抽类时改变了组装顺序或参数 | 第 3 步只搬代码；用"改动前后 status / history 输出逐字一致"作为硬性验收 |
| 析构顺序错误导致 handler 访问已销毁的部件 | 成员按依赖顺序声明，ControlServer 最后声明；`Wait()` 返回前确保 ControlServer 已停止 |
| 第 5 步引入递归或调用风暴 | 待处理标记加补跑上限；合并窗口和最短间隔；专项测试 |
| 第 2 步改名时顺手改了逻辑 | 只改文件名和 include；`git diff -M` 应只显示改名 |
| 第 1 步和别人的修改冲突 | 开工前确认工作区干净（`git status` 为空）；第 1 步单独快速完成并提交 |
