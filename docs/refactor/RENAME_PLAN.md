# 命名统一：剩余执行计划

原则：**名字直接说它是什么**，不用"意图""回报""归属"这类需要再解释一遍的词。

本文件给接手的人执行。分支 `rename-cleanup`，不要推送；每完成一步单独提交一次。

## 当前状态（2026-10-01）

已提交（每个提交都编译通过、30 个测试全过）：

| 提交 | 内容 |
|---|---|
| `1d8c7f8` | 基线 |
| `b398bf7` | venue → exchange |
| `c2d3568` | PaperConnector → SimpleSimulatedExchange，mode paper → simulated |
| `0efa14f` | OwnerId → StrategyId{value, name}，owner → strategy_id |
| `7f844bb` | 订单状态 New/PartiallyFilled/Filled → Open/PartiallyTraded/Traded |

**未提交**：错误码改造已全部写入工作区，只差 2 个测试的期望值没更新（现在 28/30 通过）。第 0 步把它收尾并提交。

## 通用规则（每一步都适用）

1. 每步结束必须全部通过：
   ```bash
   bazelisk test //...                          # 30 个测试全过
   bazelisk run //examples:replay_walkthrough   # 最后一行余额：BTC 总额 0.01999，USDT 总额 10.000999
   ```
2. 改完用 `grep` 确认旧名字一个不剩（SQL 列名除外，见下）。
3. 单独提交，提交信息写清"改了什么 → 改成什么"，结尾加 `Co-Authored-By` 行（如果用 Claude）。

### 批量改名工具

`tools/refactor/rename.sh <map> [扩展名正则]`，在仓库根目录运行。map 文件每行是 `<perl 正则><TAB><替换>`，替换里可以用 `$1`…`$9`。默认处理 `cc|h|bazel|bzl|yaml|json|md`。

```bash
printf '%s\t%s\n' '\bOldName\b' 'NewName' > /tmp/x.map
tools/refactor/rename.sh /tmp/x.map 'cc|h'
```

### 已经踩过的坑，务必注意

| 坑 | 怎么避免 |
|---|---|
| 用 `\bfoo\b` 改变量名时，字符串里的同一个词也会被改（曾把 `"hquant/test/fixtures/paper"` 改坏） | 改完运行 `grep -rnoE '"[^"]*\bNEW\b[^"]*"'` 检查所有含新名字的字符串字面量 |
| 注释里泛指的词被误改（曾把"single-thread owner"改成 strategy_id） | 改完检查 `git diff -U0 \| grep '^+.*//'` |
| 同名的第三方标识符被误改（`\bkCancelled\b` 误伤了 `absl::StatusCode::kCancelled`） | 带命名空间限定，或改完 grep 第三方前缀 |
| 结构体字段和变量同名但含义不同（`DecodedClientId::owner_key`、`ClientMapping::owner_key`） | 改结构体字段前，先 `git grep -n 'old_name( = 0)?;'` 找出所有声明了同名字段的结构体 |
| shell heredoc 里写 `\b` 会变成退格字符 | 正则写进文件用 `printf '%s\t%s\n'` 或 Python 脚本，不要在 heredoc 里拼 `\b` |
| **SQLite 表名和列名不能改**（`owner_key`、`market_venue`、`history_records` 等） | 改名前用占位符保护 SQL 字符串，改完再还原 |
| **枚举值顺序不能变**：历史记录按数值存进 SQLite | 只改名字，不调整顺序、不插入 |
| 配置键改名会让用户已有的 yaml 失效 | examples/、测试里的 yaml 一起改，并在提交信息里写明 |

---

## 第 0 步：完成错误码改造（负数）

工作区里已经完成的部分：
- `error.h`/`error.cc` 已重写：125 个码，全部为负数（如 `kDecimalInvalid = -10001`），`ErrorCode` 底层类型是 `int32_t`。
- 退役 11 个码，数字写在 `error.h` 顶部注释里，**永不复用**。
- 改名、拆分的调用点和测试断言都已更新（拆分对照见附录 A）。
- `tools/refactor/error_negative.py` **已经执行过，不要再运行**（再跑会因为原文已变而报错退出）。它做了下面这些事，供审查时对照：
1. `error.h` 新增：
   - `ErrorNumber(code)`：返回负数，给用户看、走服务接口协议；
   - `ErrorFromNumber(int64)`；
   - `StoredErrorNumber(code)`：返回正的绝对值，用于存储；
   - `ErrorFromStoredNumber(uint64)`。
2. 新增 `kCliEngineError = -19010`：`cli.cc` 里"引擎返回了错误"用它，"stop not accepted"仍用 `kCliStopRejected`。
3. Status 里携带的编号改为带符号，`CodeOf` 能解析负数。
4. **SQLite 和 record codec 继续存正的绝对值**，读出时取负。这样旧数据仍能读，存储格式不变。
5. 服务接口协议、CLI 输出、launcher 的 `market_stream_error` 改为输出负数，比如 `HISTORY_QUEUE_FULL (-17007)`。
6. 测试：`error_test` 期望 125 个码，并检查全部为负；`cli_test` 期望 `kCliEngineError` 和 `(-17007)`。

**还要做的：改 2 个测试的期望值**（已在副本上验证，改完 30/30 通过，示例程序余额不变）：

1. `hquant/test/control_test.cc` 第 22 行：服务接口协议现在输出负数。
   ```diff
   - reply->find("\"code\":19008,\"name\":\"CONTROL_BUSY\"")
   + reply->find("\"code\":-19008,\"name\":\"CONTROL_BUSY\"")
   ```
2. `hquant/test/recorder_test.cc` 第 385 行：数据库存的是正的绝对值。
   ```diff
   -            static_cast<int>(ErrorCode::kStorageQueueFull));
   +            static_cast<int>(StoredErrorNumber(ErrorCode::kStorageQueueFull)));
   ```

然后：
```bash
bazelisk test //...                                                # 30/30
grep -rn 'static_cast<uint16_t>' hquant | grep -i 'code\|reason'   # 应为空
```

提交：`Make error codes negative and specific`，正文列出附录 A 的主要改名和拆分。

---

## 第 1 步：下单流程结构（原第 5 组）

| 现在 | 改成 | 中文 |
|---|---|---|
| `OrderIntent` | `PreparedOrder` | 待发订单（已分配订单号、还没发出） |
| `OrderCommand` | `ApprovedOrder` | 风控已批准的订单 |
| `OrderCreated` | `OrderOpened` | 订单已挂上 |
| `OrderFilled` | `OrderTraded` | 订单有一笔成交 |
| `OrderCompleted` | `OrderFullyTraded` | 订单全部成交 |
| `StrategyContext` | `StrategyInput` | 策略输入 |

变量名一起改：`intent` → `prepared`（注意 `intent.` 成员访问和 `result.intent` 字段），`command` → `approved`。

`history` 输出 JSON 里的 `"kind":"intent"` 改成 `"kind":"prepared_order"`（`control.cc` 的 `HistoryJson`）；CLI 测试若检查这个字符串，同步更新。

## 第 2 步：风控（原第 6 组）

| 现在 | 改成 | 中文 |
|---|---|---|
| `RiskLease` | `RiskBudget` | 风控额度 |
| `StaticRiskLeaseBook` | `RiskBudgetAllocator` | 启动时分配并校验各分片额度 |
| `lease_version` | `budget_version` | |
| `SetInitialLease` | `SetInitialBudget` | |
| `RiskReservation` | `FundsHold` | 资金冻结 |
| `ReservationId` | `HoldId` | |
| `ReservationState` / `::Terminal` | `HoldState` / `::Released` | |
| `reservation_id`（字段、变量）、`reservations_` | `hold_id`、`holds_` | |
| `TryReserve` / `TryReserveCode` | `TryHold` / `TryHoldCode` | 冻结 |
| `ConfirmTerminal` | `Release` | 解冻 |
| `ApplyFill` | `ApplyTrade` | 记入成交 |
| `RateLease` / `RateLeaseGrant` | `RateBudget` / `RateBudgetAssignment` | 限速额度 |
| `ValidateStaticRateLeases` | `ValidateRateBudgets` | |
| `StaticRiskLeaseConfig` / `StaticRateLeaseConfig` | `RiskBudgetConfig` / `RateBudgetConfig` | |
| 配置键 `static_risk_leases` / `static_rate_leases` | `risk_budgets` / `rate_budgets` | |

`RiskGate` 保留不改。注释里的 lease 一并改成 budget。

## 第 3 步：动作执行器（原第 7 组）

| 现在 | 改成 |
|---|---|
| `ActionDispatcher` / 成员 `dispatcher_` | `ActionExecutor` / `action_executor_` |
| 方法 `Dispatch` | `Execute` |
| `DispatchContext` / `DispatchResult` | `ActionContext` / `ActionResult` |
| `DecisionId` / 字段 `decision_id`、`next_decision_id_` | `ActionBatchId` / `action_batch_id`、`next_action_batch_id_` |
| `DecisionRecord` | `ActionRecord` |
| `DecisionActionKind` | `ActionKind` |
| 文件 `service/dispatcher.*`、目标 `:dispatcher`、`test/dispatcher_test.cc` | `action_executor.*`、`:action_executor`、`action_executor_test.cc` |

## 第 4 步：行情（原第 8 组）

| 现在 | 改成 | 中文 |
|---|---|---|
| `stream_epoch` | `connection_id` | 第几次连接 |
| `BookScale` | `TickLotSize` | 价格刻度和数量刻度 |
| `quote_per_tick` / `base_per_lot` | `price_per_tick` / `amount_per_lot` | |
| `scale_version` | `tick_lot_version` | |
| 配置键 `book_scale` | `tick_lot_size` | |
| `BookSync` / `BookView` | `OrderBookSync` / `OrderBookView` | |
| `BookSyncState::Buffering` / `::Replaying` | `::WaitingSnapshot` / `::CatchingUp` | 等快照 / 快照后追赶增量 |
| `InputStamp` | `InputTime` | |

回放文件和测试夹具里的 JSON 键同步改：`examples/replay_market.json`、`hquant/test/fixtures/order_book/*.json` 的 `stream_epoch`；配置里的 `book_scale`、`quote_per_tick`、`base_per_lot`。`examples/replay_walkthrough.cc` 里打印状态名的 switch 同步改。

## 第 5 步：记录和存储（原第 9 组）

先改实现类，再改接口，否则会撞名：

| 顺序 | 现在 | 改成 |
|---|---|---|
| 1 | 类 `HistoryReader` | `SqliteHistoryReader` |
| 2 | 接口 `HistoryReaderPort` | `HistoryReader` |
| 3 | `SqliteRecorder` | `SqliteHistoryWriter` |
| 4 | 接口 `RecorderPort` | `HistoryWriter` |
| 5 | `RecordEnvelope` / `RecordPayload` | `HistoryRecord` / `HistoryRecordPayload` |
| 6 | `ExecutorCheckpoint` | `StrategyCheckpoint` |
| 7 | `Checkpoint`（`{state, recorded_at}`） | `RecordedCheckpoint` |

`record_codec` 的二进制格式不变。

## 第 6 步：账户推送（原第 10 组，实盘用）

| 现在 | 改成 |
|---|---|
| `UserDataStream` / `UserDataBatch` | `AccountStreamParser` / `AccountStreamBatch` |
| `PrivateReport` / `RoutedPrivateReport` / `QuarantinedPrivateReport` | `AccountReport` / `RoutedAccountReport` / `QuarantinedAccountReport` |
| `PrivateReportRouter` | `AccountReportRouter` |
| `SignedAccountRest` | `SignedRestClient` |

## 第 7 步：服务接口（原 ControlServer）

`hquant_server` 对外只有这一个服务入口（查状态、查历史、停止，以后还有撤单、停止下单）。它是正常的服务请求，不是"控制面"，所以去掉 control 这个词。

| 现在 | 改成 |
|---|---|
| `ControlServer` | `QuantServer` |
| `ControlRequest` / `ControlResponse` / `ControlError` | `ServerRequest` / `ServerResponse` / `ServerError` |
| `ControlRequestPayload` / `ControlResponsePayload` | `ServerRequestPayload` / `ServerResponsePayload` |
| `EncodeControlRequest` / `DecodeControlRequest` / `EncodeControlResponse` / `DecodeControlResponse` | `EncodeServerRequest` / `DecodeServerRequest` / `EncodeServerResponse` / `DecodeServerResponse` |
| `SendControlRequest` / `MakeControlRequest` / `FormatControlResponse` | `SendServerRequest` / `MakeServerRequest` / `FormatServerResponse` |
| `ControlSocketPath`，socket 文件 `<state_dir>/control.sock` | `ServerSocketPath`，`<state_dir>/quant_server.sock` |
| 文件 `application/control.{h,cc}`、目标 `:control` | `application/quant_server.{h,cc}`、`:quant_server` |
| 测试 `control_test.cc`、`ControlServerTest`、`ControlHistoryTest`、临时目录 `hquant_control_test_XXXXXX` | `quant_server_test.cc`、`QuantServerTest`、`QuantServerHistoryTest`、`hquant_quant_server_test_XXXXXX` |
| 错误码 `kControlMessageInvalid` / `kControlBusy` / `kControlTimeout` / `kControlSocketFailed` / `kControlSocketInUse` | `kServerMessageInvalid` / `kServerBusy` / `kServerTimeout` / `kServerSocketFailed` / `kServerSocketInUse`（**数字不变**，名称字符串同步改为 `SERVER_MESSAGE_INVALID` 等） |
| 局部变量、注释里的 control | server（例如 `control socket` → `server socket`） |

注意：
- `ShardCommand`（`StopNewOrders`、`CancelOwnedOrders`、`RequestShardReport`）是服务线程发给分片的**内部命令**，不属于对外接口，不在本步改名范围。
- 协议报文格式不变，只是 socket 文件名变了。`cli/cli.cc` 和 server 必须一起改，否则连不上。
- `control.cc` 里的 `LegacyErrorName` / `LegacyErrorCode` 处理的是旧版协议的错误名字符串（`bad_request`、`busy` 等），这些是**线上协议值，不改**。

## 第 8 步：目录和总管（原第 11 组）

| 现在 | 改成 |
|---|---|
| `hquant/src/offline/` | `hquant/src/storage/` |
| `hquant/src/service/` | `hquant/src/shard/` |
| `ShardRuntime` | `Shard` |

用 `git mv` 移目录，然后统一替换 `#include "offline/` → `"storage/`、`"service/` → `"shard/"`，以及 Bazel 路径 `//hquant/src/offline` → `//hquant/src/storage`、`//hquant/src/service` → `//hquant/src/shard`。别忘了 `examples/BUILD.bazel`、`apps/BUILD.bazel`、`hquant/test/BUILD.bazel` 和各 BUILD 里的 `visibility`。

## 第 9 步：收尾

1. `docs/` 里的结构体名、目录名按以上全部更新。
2. 新建 `docs/GLOSSARY.md`：术语表，内容见附录 B。
3. `op.sh fmt-all` 格式化，然后 `op.sh fmt-check` 确认。
4. 最后跑一遍 `bazelisk test //...` 和示例程序。

---

## 附录 A：错误码改动对照（第 0 步）

**改名（数字不变，只是取负）**

| 旧 | 新 |
|---|---|
| kCancelled | kNetCancelled（只剩网络 I/O 被取消时用） |
| kNetBusy | kNetConcurrentCall（恢复策略改为 Halt） |
| kHttpStatusUnexpected | kFeedSnapshotHttpError |
| kVenueRateLimited / kVenueIpBanned | kExchangeRateLimited / kExchangeIpBanned |
| kRateLeaseMissing / kRateLeaseExhausted | kRateBudgetMissing / kRateBudgetExhausted |
| kFeedScaleMismatch | kFeedTickSizeMismatch |
| kOrderSlotsFull | kOrderSendQueueFull |
| kOrderVenueRejected | kOrderRejectedByExchange |
| kOrderReportUnattributed / kShardReportUnattributed | kReportOrderUnknown / kShardReportOrderUnknown |
| kClientIdInvalid | kClientOrderIdInvalid |
| kPaperBalanceInsufficient / kPaperInputInvalid | kSimulatedBalanceInsufficient / kSimulatedMarketDataInvalid |
| kRiskLeaseExhausted / kRiskLeaseMissing | kRiskBudgetExhausted / kRiskBudgetMissing |
| kRouteOwnerUnknown / kRouteOwnershipConflict | kRouteStrategyUnknown / kRouteStrategyConflict |
| kStorageOptionsInvalid | kStorageConfigInvalid |
| kHistoryBusy / kHistoryStopping / kHistoryDeadline | kHistoryQueueFull / kHistoryReaderStopping / kHistoryQueryTimeout |
| kRecordEncodingInvalid | kHistoryRecordCorrupted |
| kConfigCredentialsInline / kConfigLeaseInvalid | kConfigContainsSecret / kConfigBudgetInvalid |
| kLaunchUnsupportedTopology | kLaunchMultipleNotSupported |
| kCliCommandFailed | kCliStopRejected，另新增 kCliEngineError |

**退役（数字保留、不复用）**：-10005 Unimplemented、-13001 OrderInvalid、-13002 OrderRuleViolation、-13012 OrderReportConflict、-14002 RiskNotReady、-14003 RiskLimitExceeded、-14006 RiskLeaseStale、-14008 RiskReservationInvalid、-16001 ShardInputInvalid、-16003 StrategyCallbackTimeout、-16004 StrategyDependencyNotReady。

**新增（拆分产生）**

| 码 | 数字 | 用途 |
|---|---|---|
| kDecimalArithmeticFailed | -10006 | 十进制运算失败（溢出、取整失败等），原先分散在 RuleViolation、RiskLimitExceeded、RiskReservationInvalid、Internal 里 |
| kNetNotConnected | -11014 | WebSocket 未连接（从 kNetBusy 拆出） |
| kFeedStopped | -12012 | 行情流被主动停止 |
| kPublicTradeInvalid | -12013 | 公开成交的交易对或方向不对 |
| kOrderStrategyIdInvalid | -13019 | 策略 ID 无效，或与当前分片的策略不符 |
| kOrderAccountInvalid / kOrderMarketInvalid | -13020 / -13021 | 账户、交易对为空或不匹配 |
| kOrderTypeUnsupported | -13022 | 非限价单，或 LIMIT_MAKER 带了 timeInForce |
| kOrderPriceOrAmountInvalid | -13023 | 价格或数量不是正数，或缺少限价 |
| kOrderBelowMinAmount / kOrderBelowMinNotional / kOrderAboveMaxAmount | -13024 / -13025 / -13026 | 低于最小数量 / 低于最小金额 / 超过最大数量 |
| kOrderNotOnTick | -13027 | 价格或数量没对齐步长 |
| kExchangeOrderIdConflict | -13028 | 交易所订单号冲突或变化 |
| kReportAccountMarketMismatch | -13029 | 回报的账户或交易对与订单不符 |
| kOrderStatusConflict | -13030 | 订单状态矛盾 |
| kTradeIdOnOtherOrder | -13031 | 成交编号已属于另一张订单 |
| kTradeExceedsOrderAmount | -13032 | 成交量超过订单数量 |
| kGatewayConfigInvalid | -13033 | 下单网关配置不合法 |
| kRiskMarketNotLive / kRiskAccountStale / kRiskTradingRuleStale | -14009 / -14010 / -14011 | 订单簿不可用 / 账户数据不新鲜 / 交易规则过期 |
| kRiskBudgetExpired / kRiskBudgetRenewalStale | -14012 / -14013 | 额度过期 / 续期的版本或到期时间不比现有的新 |
| kFundsHoldNotFound / kFundsHoldAlreadyReleased / kFundsHoldIncreased / kFundsHoldClientIdConflict | -14014 … -14017 | 资金冻结不存在 / 已解冻 / 成交后反而变大 / 订单号为空或已绑定 |
| kInputTimeInvalid | -16005 | 输入时间为负或不递增 |
| kCliEngineError | -19010 | CLI 收到引擎返回的错误（第 0 步脚本添加） |

## 附录 B：术语表（写进 docs/GLOSSARY.md）

| 英文 | 中文 | 说明 |
|---|---|---|
| exchange | 交易所 | 不再使用 venue |
| simulated exchange | 模拟交易所 | 本地撮合，`SimpleSimulatedExchange` |
| simulated trading | 模拟交易 | `mode: simulated`；不再使用 paper |
| live trading | 实盘交易 | `mode: live`，尚未开放 |
| replay | 回放 | 行情从文件按时间顺序重放 |
| market | 交易对 | 如 BTC-USDT |
| base / quote asset | 基础资产 / 计价资产 | BTC / USDT |
| order book | 订单簿 | |
| best bid / best ask（BBO） | 买一 / 卖一 | |
| tick / lot | 价格刻度 / 数量刻度 | |
| snapshot / diff | 快照 / 增量 | |
| connection_id | 连接编号 | 原 stream_epoch |
| trade | 成交 | **默认指自己订单的成交** |
| public trade | 公开成交 | 市场上别人的成交，属于行情 |
| submit / cancel | 下单 / 撤单 | |
| Open / PartiallyTraded / Traded | 挂单中 / 部分成交 / 全部成交 | 订单状态 |
| prepared order | 待发订单 | 已分配订单号、还没发出 |
| order / trade / balance update | 订单更新 / 成交更新 / 余额更新 | 交易所发来的三种账户消息 |
| strategy_id | 策略 ID | 数字，编进订单号；不再使用 owner |
| risk budget | 风控额度 | 不再使用 lease |
| funds hold | 资金冻结 | 不再使用 reservation |
| shard | 分片 | 一个线程，独占一组交易对的全部状态 |
| QuantServer | 服务接口 | `hquant_server` 唯一的对外入口；不再使用 control |
| reconciliation | 对账 | 向交易所查询，核对本地和真实状态 |
| submission unknown | 结果未知 | 请求已发出，但不知道交易所是否收到 |
