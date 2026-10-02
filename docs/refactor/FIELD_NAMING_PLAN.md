# 字段命名统一：执行计划

本文件给接手的人执行。基线：提交 `ee340f0`（2026-10-02）。

原则：**字段名和类型名一致，名字直接说出含义。**
- 订单号字段叫 `client_order_id` / `exchange_order_id`，和类型 `ClientOrderId` / `ExchangeOrderId` 对应；
- 数量叫 `quantity`，金额（数量 × 价格）叫 `value`；不再用 `base` / `quote` 表示"数量还是金额"，也不用 `notional`（它的本义是合约的名义价值）。

## 通用要求

每一步：
- 只改名字，不改逻辑，`git diff` 应只有改名；
- `bazelisk test //...` 全部通过；
- `bazelisk run //examples:replay_walkthrough` 最后一行余额为 BTC 0.01999、USDT 10.000999；
- `./op.sh fmt-check` 通过；
- 单独提交。

**不改的东西：**
- Binance 线上协议的字段名：`clientOrderId`、`origClientOrderId`、`executedQty`、`cummulativeQuoteQty`、`MIN_NOTIONAL` 等，都在解析器的字符串里，保持原样；
- SQLite 表名、列名，以及订单历史记录的二进制编码（`record_codec`）：只改 C++ 字段名，编码顺序不变；
- 错误码的数字。

**特别注意：**
- **不要**对 `value` 这个词做全局替换。`StringId::value`、`RunId::value`、`ShardId::value` 到处都在用。只能把 `quote_amount` 这个词替换成 `value`；
- 用 `\b...\b` 整词替换时，要检查字符串字面量和注释是否被误改，`git diff` 逐处过一遍；
- 头文件里的中文注释提到了这些字段名，替换后要读一遍，确认说法通顺。

---

## 第 0 步：先提交注释改动

工作区里有三个文件只加了注释（结构体说明和可能的优化），先单独提交：

```bash
git add hquant/src/base/market.h hquant/src/base/order.h hquant/src/order/order_tracker.h
git commit -m "docs: document core order and market data structures"
```

`hquant/src/market/order_book.h` 也有未提交的改动，那不是本计划的内容，**不要**一起提交，先和改它的人确认。

---

## 第 1 步：订单号字段

| 现在 | 改成 | 主要位置 |
|---|---|---|
| 字段 `client_id`（类型 `ClientOrderId`） | `client_order_id` | `PreparedOrder`、`OrderUpdate`、`TradeUpdate`、`OrderSnapshot`、`CancelOrder`、`ActionResult`、`FundsHold`、`RestingOrder`、`ActionRecord`、`GatewayEvent` 等 |
| 字段 `exchange_id`（类型 `ExchangeOrderId`） | `exchange_order_id` | `OrderSnapshot`、`OrderTracker::TrackedOrder`（`OrderUpdate`、`TradeUpdate` 里已经是这个名字） |
| 字段 `original_client_id`（要查询的订单） | `client_order_id` | `order/account_reports.h` 的 `OrderToQuery` |
| `EncodeClientId` / `DecodeClientId` / `DecodedClientId` | `EncodeClientOrderId` / `DecodeClientOrderId` / `DecodedClientOrderId` | `order/order_gateway.*` |
| 函数参数、局部变量 `client_id`（类型是 `ClientOrderId` 的） | `client_order_id` | 各处，保持一致 |
| 测试夹具 JSON 的键 `"client_id"` | `"client_order_id"` | `hquant/test/fixtures/order_tracker/*.json`、`simulated_exchange/*.json` 等，以及 `hquant/test/fixture_loader.cc` 和读取它的测试 |

规模：`client_id` 约 370 处 / 54 个文件，`exchange_id` 约 50 处 / 6 个文件。

验收：
```bash
grep -rnw "client_id\|exchange_id\|original_client_id" hquant apps examples dev --include='*.cc' --include='*.h' --include='*.json'
```
为空；`grep -rn "EncodeClientId\|DecodeClientId\|DecodedClientId" hquant` 为空。

---

## 第 2 步：数量和金额

| 现在 | 改成 | 中文 |
|---|---|---|
| `OrderRequest::base_amount` | `quantity` | 下单数量（以基础资产计） |
| `TradeUpdate::base_amount` / `quote_amount` | `quantity` / `value` | 这一笔的成交量 / 成交额 |
| `OrderUpdate::cumulative_base` / `cumulative_quote` | `traded_quantity` / `traded_value` | 交易所报告的累计成交量 / 累计成交额 |
| `OrderSnapshot::cumulative_base` / `cumulative_quote` | `traded_quantity` / `traded_value` | 累计成交量 / 累计成交额 |
| `OrderTracker::TrackedOrder::cumulative_base` / `cumulative_quote` | `traded_quantity` / `traded_value` | 同上 |
| `OrderTracker::TrackedOrder::reported_cumulative_base` / `reported_cumulative_quote` | **删除** | 只被赋值、从未被读取。删除时连同 `order_tracker.cc` 里的两处赋值一起删 |
| 测试夹具 JSON 的键 `"base_amount"`、`"quote_amount"`、`"cumulative_base"`、`"cumulative_quote"` | `"quantity"`、`"value"`、`"traded_quantity"`、`"traded_value"` | 夹具文件和 `fixture_loader.cc` |

保持不变：`SimplePmmConfig::order_amount`（策略配置里的每单数量）、`SimulatedExchange::OnPublicTrade` 的参数 `public_amount`、`BookLevel::quantity_lots`。

规模：`base_amount` 约 100 处，`quote_amount` 约 25 处，`cumulative_*` 约 140 处。

验收：
```bash
grep -rnw "base_amount\|quote_amount\|cumulative_base\|cumulative_quote\|reported_cumulative_base\|reported_cumulative_quote" hquant apps examples dev --include='*.cc' --include='*.h' --include='*.json'
```
为空。

---

## 第 3 步：最小下单金额

| 现在 | 改成 |
|---|---|
| `TradingRule::min_notional` | `min_order_value` |
| 配置键 `min_notional`（`application/config.cc`、`examples/*.yaml`、测试里的 yaml） | `min_order_value` |
| 错误码 `kOrderBelowMinNotional`，名称 `ORDER_BELOW_MIN_NOTIONAL` | `kOrderBelowMinOrderValue`，`ORDER_BELOW_MIN_ORDER_VALUE`（数字不变） |
| 错误消息里的 "min notional" / "minimum notional" | "minimum order value" |

配置键改名会让用户已有的 yaml 失效，提交说明里写明。

验收：`grep -rni "notional" hquant/src apps examples` 只剩 Binance 协议字符串（如果有），代码标识符和配置键里没有。

---

## 第 4 步：order-history 命令的输出字段（执行前确认）

`hquant_bench order-history` 输出的 JSON 是对外格式，改了会影响读取它的脚本。**执行前先和需求方确认要不要改。** 确认要改时：

| 现在 | 改成 | 位置 |
|---|---|---|
| `"client_id"` | `"client_order_id"` | `application/control_server.cc` 的 `OrderHistoryJson`（约第 490–507 行） |
| prepared_order 和 trade 行里的 `"amount"` | `"quantity"` | 同上 |

同步更新断言这些字段的测试（`control_server_test`、bench 相关测试）和 `docs/` 里的输出示例。

---

## 风险

| 风险 | 做法 |
|---|---|
| 全局替换 `value` 破坏 `StringId::value` 等 | 只替换 `quote_amount` 这个词，禁止替换 `value` |
| 误改 Binance 协议字符串 | 协议字符串都是驼峰（`clientOrderId`）或全大写（`MIN_NOTIONAL`），整词替换下划线名字不会命中；替换后 `git diff` 检查 `order_gateway.cc`、`account_reports.cc`、`market_data_stream.cc` |
| 订单历史编码被改动 | `record_codec.cc` 只改字段访问的名字，读写顺序和类型不变；`order_history_test`、`recovery_test` 必须通过 |
| 夹具 JSON 和加载代码不一致 | 夹具的键和 `fixture_loader.cc` 在同一个提交里改 |
