# 订单模型设计

本文只描述"订单"相关的结构体：每个结构体为什么存在、每个字段给谁用、它们之间的依赖和组合顺序，以及订单跟踪器对外提供的接口。行情、余额、风控额度不在本文范围内。

## 1. 设计原则

1. **按内容命名**：名字说明它装的是什么，而不是它处在流程的哪个阶段。
2. **依赖只能由基础指向组合**：基础结构体不包含、也不知道上层结构体。
3. **每个结构体、每个字段都要有读取者**。没有读取者的不保留；能从别处得到的信息，不重复存放。
4. **订单的执行状态只在订单跟踪器内部**。外面只需要"这张单是什么"和"它还活跃吗"，不需要细节。

## 2. 总览

```
第 1 层  基础值        ClientOrderId  ExchangeTradeId  StrategyId  AccountId  MarketId  AssetId
                       Side  TimeInForce  ExchangeOrderStatus  Decimal  EventTime
                              │
第 2 层  只由第 1 层组成
                       SubmitOrder  CancelOrder        策略的指令
                       Order                           一张订单（不变）
                       TradeFee                        一项手续费
                              │
第 3 层                Trade（包含 TradeFee）          我们的一笔成交
                              │
第 4 层                OrderUpdate（可能带一笔 Trade）  交易所发来的订单消息

订单跟踪器接口         SendResult  ReportSource  UpdateResult（第 6 节）
```

| 层 | 名字 | 种类 | 一句话含义 | 谁创建 | 谁读取 |
|---|---|---|---|---|---|
| 1 | `ClientOrderId` | 字符串 | 本地订单号：发送前由我们生成，下单时发给交易所，交易所在回报里原样带回。唯一用来确定订单的编号 | 网关 | 订单跟踪器、订单历史、策略（撤单） |
| 1 | `ExchangeTradeId` | 字符串 | 交易所给每次撮合的编号；一张订单可能有多笔成交，各有一个编号 | 交易所 / 模拟交易所 | 订单跟踪器（去重）、订单历史 |
| 1 | `StrategyId` | 数字 + 名字 | 哪个策略。Binance 的本地订单号里编码了它的数字部分，模拟盘的没有 | 配置 | 订单历史 |
| 1 | `AccountId` | 字符串 | 哪个账户 | 配置 | 路由、订单历史、重启后查询 |
| 1 | `MarketId` | 结构体 | 哪个交易对：交易所 + 品种 + 交易所符号（`BTCUSDT`） | 配置 | 路由、订单历史、重启后查询 |
| 1 | `AssetId` | 字符串 | 哪种资产（`BTC`、`USDT`、`BNB`） | 配置；手续费资产来自交易所 | 风控、订单历史 |
| 1 | `Side` | 枚举 | 买或卖 | 策略 | 风控、网关、模拟交易所（撮合）、Shard（成交时记账） |
| 1 | `TimeInForce` | 枚举 | 限价单进入交易所后，能立刻成交的部分成交完，剩下没成交的部分怎么处理：`Gtc` 剩下的一直挂着，直到成交或撤单 / `Ioc` 剩下的马上撤销 / `Fok` 不能全部立刻成交就整张作废 / `PostOnly` 只许挂着等别人来成交，会立刻成交就整张拒绝 | 策略 | 网关。模拟交易所目前不读它，全部按 `Gtc` 撮合 |
| 1 | `ExchangeOrderStatus` | 枚举 | 交易所说的订单状态：`Open` / `PartiallyTraded` / `Traded` / `Canceled` / `Rejected` / `Expired` | 解析器 / 模拟交易所 | 订单跟踪器、订单历史 |
| 1 | `Decimal` | 十进制数 | 价格、数量、金额、手续费，精确小数 | — | 各处 |
| 1 | `EventTime` | 结构体 | 消息的时间：交易所时间（可能没有）、接收时的 UTC 时间和单调时钟 | 解析器 / 网关 / 模拟交易所 | 订单历史 |
| 2 | `SubmitOrder` | 指令 | 策略说"我要下这样一张单"：买卖、数量、价格、怎么挂 | 策略 | 风控（冻结资金）、网关（生成 `Order`） |
| 2 | `CancelOrder` | 指令 | 策略说"撤掉这张单"：本地订单号 | 策略 | ActionExecutor、网关 |
| 2 | `Order` | 实体，不变 | 一张已分配订单号的订单：`SubmitOrder` 的内容 + 订单号、策略、账户、交易对 | 网关 | 订单跟踪器、订单历史、策略（`ActiveOrders`）、Shard（`Find`） |
| 2 | `TradeFee` | 值 | 一笔成交的一项手续费：资产 + 金额（正数支付，负数返佣） | 解析器 / 模拟交易所 | 订单历史 |
| 3 | `Trade` | 值 | 我们订单的一笔成交：成交编号、价格、数量、金额、手续费、是否挂单方 | 解析器 / 模拟交易所 | 订单跟踪器（去重、累计数量）、风控（数量、金额）、订单历史 |
| 4 | `OrderUpdate` | 消息 | 交易所说"订单 X 现在是状态 Y"；如果这次是因为成交，同时带上这一笔 `Trade` | 账户推送解析器、网关（下单响应）、订单查询、模拟交易所 | 路由、订单跟踪器、订单历史 |
| 接口 | `SendResult` | 枚举 | 发送结果：`NotSent` 能证明没发出 / `Unknown` 发出了但不知道交易所是否收到 | ActionExecutor、网关 | 订单跟踪器 |
| 接口 | `ReportSource` | 枚举 | 消息来源：`Push` 推送 / `Query` 主动查询 | Shard | 订单跟踪器 |
| 接口 | `UpdateResult` | 枚举 | 订单跟踪器处理完后的结论：`Ignored` 订单没变 / `Updated` 订单变了但没结束 / `Finished` 订单结束且成交明细到齐 | 订单跟踪器 | Shard |

### 依据的事实

| 事实 | 位置 |
|---|---|
| 一个分片只有一个账户、一个交易对、一个策略 | `shard/shard.h:109-110` |
| 所有回报都带本地订单号：模拟交易所、下单响应、下单被拒、账户推送、订单查询、成交查询 | `simulated_exchange.cc:75,287`、`order_gateway.cc:481,582`、`account_reports.cc:149,353,407` |
| 查成交需要的交易所订单号，是先按本地订单号查订单、从查询响应里拿到的 | `account_reports.cc:526-560` |
| 策略只用订单的本地订单号（撤单）和"是否活跃" | `strategy/simple_pmm.cc:21-35` |
| 订单跟踪器之外，没有代码区分各种细分状态，也没有代码读已成交数量 | 搜索 `OrderDisplayState::`、`traded_quantity` |
| Shard 处理成交时只读订单的买卖方向和策略；策略只用来标注订单历史的记录 | `shard/shard.cc:368,390` |
| 每笔成交都伴随一条订单状态：账户推送的一条成交消息同时带成交明细和订单状态；模拟交易所每笔成交后紧接着发订单状态 | `account_reports.cc:170,192`、`simulated_exchange.cc:284-296` |
| 唯一的例外是成交查询（`myTrades`）只返回成交，但它总是和订单查询一起做，订单状态来自同一次查询 | `account_reports.cc:526-560` |

## 3. 策略的指令

### 3.1 `SubmitOrder`：下单指令

```cpp
struct SubmitOrder {
  Side side;
  Decimal quantity;
  Decimal price;
  TimeInForce time_in_force;
};
```

| 字段 | 含义 | 读取者 |
|---|---|---|
| `side` | 买或卖 | 风控（冻结哪种资产）、网关 |
| `quantity` | 下单数量，以基础资产计 | 风控（冻结金额、最小数量）、网关 |
| `price` | 限价 | 风控（冻结金额、最小下单金额）、网关 |
| `time_in_force` | 怎么挂 | 网关 |

- **创建者**：策略。用完即丢：变成 `Order`，或者被风控拒绝。
- **没有 `account`、`market`**：分片只有一个账户、一个交易对，策略填了也只能填这一个。
- **没有 `strategy_id`**：分片只有一个策略。
- **没有 `OrderType`**：现在所有订单都是限价单，`price` 必填。原来 `type`（`Limit` / `LimitMaker`）和 `time_in_force` 两个字段有一种非法组合：`LimitMaker` 不能带有效期，网关要专门检查（`order_gateway.cc:233`）。把"只挂单不吃单"作为 `TimeInForce::PostOnly` 后，非法组合写不出来。网关把 `PostOnly` 翻译成 Binance 的 `type=LIMIT_MAKER`，其余翻译成 `type=LIMIT` 加对应的 `timeInForce`。

### 3.2 `CancelOrder`：撤单指令

```cpp
struct CancelOrder {
  ClientOrderId client_order_id;
};
```

| 字段 | 含义 | 读取者 |
|---|---|---|
| `client_order_id` | 要撤哪张单 | ActionExecutor → 网关 |

## 4. `Order`：一张订单

一张已分配订单号的单。**创建后所有字段都不变。**

```cpp
struct Order {
  ClientOrderId client_order_id;
  StrategyId strategy_id;
  AccountId account;
  MarketId market;
  Side side;
  Decimal quantity;
  Decimal price;
  TimeInForce time_in_force;
};
```

| 字段 | 含义 | 读取者 |
|---|---|---|
| `client_order_id` | 本地订单号 | 订单跟踪器（索引）、订单历史、策略（撤单） |
| `strategy_id` | 下这张单的策略 | 订单历史 |
| `account`、`market` | 账户和交易对 | 订单历史、重启后查询（`symbol` + `origClientOrderId`） |
| `side` | 买或卖 | Shard（成交时风控记账，决定加减哪种资产） |
| `quantity`、`price`、`time_in_force` | 下的是什么单，经过交易规则取整后的值 | 订单跟踪器（成交是否超过下单量）、订单历史、网关 |

- **创建者**：网关的 `PrepareSubmit`。输入是 `SubmitOrder`，加上本地订单号、策略、账户、交易对，输出 `Order`。
- **为什么 `SubmitOrder` 里没有 `account`、`market`、`strategy_id`，这里却有**：`Order` 要写进订单历史。重启后，对结果不确定的订单要按**当时**下单的交易对去查，不能按现在的配置。所以写进去的记录必须自己说清楚是哪个账户、哪个交易对、哪个策略。
- **没有交易所订单号**：订单跟踪器不用它（见第 2 节的依据）。
- **没有创建时间**：订单历史的每条记录自带接收时间。

## 5. 交易所的消息

交易所发来的关于我们订单的消息只有一种：`OrderUpdate`。如果这条消息是因为成交而产生的，它带着这一笔成交 `Trade`。

为什么不把状态和成交拆成两个消息：两者有 4 个字段完全相同（账户、交易对、订单号、时间），而且每笔成交都伴随一条订单状态（见第 2 节的依据）。拆开只会让同样的字段写两遍，订单跟踪器还要分两次处理同一个事件。

### 5.1 `ExchangeOrderStatus`：交易所说的订单状态

`Open`、`PartiallyTraded`、`Traded`、`Canceled`、`Rejected`、`Expired`。

由解析器从线上字符串（`NEW`、`FILLED` 等）翻译而来；模拟交易所直接生成。

### 5.2 `TradeFee`：一项手续费

```cpp
struct TradeFee {
  AssetId asset;
  Decimal signed_amount;   // 正数为支付，负数为返佣
};
```

| 字段 | 含义 | 读取者 |
|---|---|---|
| `asset` | 手续费的资产 | 订单历史 |
| `signed_amount` | 金额，带符号 | 订单历史 |

为什么 `Trade` 里是列表：同一笔成交可能有多项费用，或者用不同资产支付（例如开启平台币抵扣）。

### 5.3 `Trade`：我们订单的一笔成交

一次撮合产生一笔成交；一张订单可能有多笔。市场上别人之间的成交是行情（`PublicTrade`），不在此列。

```cpp
struct Trade {
  ExchangeTradeId exchange_trade_id;
  Decimal price;
  Decimal quantity;
  Decimal value;
  std::vector<TradeFee> fees;
  std::optional<bool> maker;
};
```

| 字段 | 含义 | 读取者 |
|---|---|---|
| `exchange_trade_id` | 成交编号 | 订单跟踪器（去掉重复推送） |
| `price` | 这一笔的成交价 | 订单历史 |
| `quantity` | 这一笔的成交量 | 订单跟踪器（累计成交量）、风控（记账） |
| `value` | 这一笔的成交额，以交易所报告为准 | 风控（买单花掉多少计价资产） |
| `fees` | 这一笔的手续费 | 订单历史 |
| `maker` | 我们是否是挂单方 | 订单历史（见第 9 节待定） |

它不带账户、交易对、订单号、时间：这些由外面的 `OrderUpdate` 给出。

### 5.4 `OrderUpdate`：订单消息

```cpp
struct OrderUpdate {
  AccountId account;
  MarketId market;
  ClientOrderId client_order_id;
  ExchangeOrderStatus status;
  std::optional<Decimal> executed_quantity;
  std::optional<Trade> trade;
  EventTime time;
};
```

| 字段 | 含义 | 读取者 |
|---|---|---|
| `account`、`market` | 消息所属的账户和交易对 | 路由（交给哪个分片，`shard/routing.cc`） |
| `client_order_id` | 哪张单，必填 | 订单跟踪器 |
| `status` | 交易所说的状态（成交时是成交之后的状态） | 订单跟踪器 |
| `executed_quantity` | 交易所报告的这张单累计成交量（如果提供） | 订单跟踪器（和自己累加的成交量对比，判断成交明细是否到齐） |
| `trade` | 这次是因为成交才发来的，就带上这一笔；挂上、撤销、被拒时为空 | 订单跟踪器、风控、订单历史 |
| `time` | 消息的时间 | 订单历史 |

**创建者**：
- 模拟交易所：挂上、撤销时不带成交；撮合时带成交，状态为 `Traded`；
- 账户推送：Binance 一条 `executionReport` 生成一个 `OrderUpdate`，`x=TRADE` 时带成交；
- 网关：下单响应、下单被拒，不带成交；
- 订单查询 + 成交查询：查到 N 笔成交，就生成 N 个带成交的 `OrderUpdate`，状态和累计成交量都取订单查询的结果；没有成交时生成一个不带成交的。

**`trade.quantity` 和 `executed_quantity` 的区别**：前者是这一笔，后者是到目前为止的累计。例如买 1 BTC 的单第二笔成交 0.5 后：`trade.quantity = 0.5`，`executed_quantity = 0.8`。

## 6. 订单跟踪器

### 6.1 接口

```cpp
enum class SendResult { NotSent, Unknown };   // 发送前就失败 / 发出后不知道是否收到
enum class ReportSource { Push, Query };      // 推送 / 主动查询的结果

enum class UpdateResult {
  Ignored,    // 订单没变：重复或过期的消息
  Updated,    // 订单变了，还没结束
  Finished,   // 订单就此结束，且成交明细已到齐
};

class OrderTracker {
 public:
  // 写：每个方法对应一种现实中的事件
  absl::Status Add(const Order& order);
  absl::StatusOr<UpdateResult> OnSendResult(const ClientOrderId&, SendResult);
  absl::StatusOr<UpdateResult> OnCancelRequested(const ClientOrderId&);
  absl::StatusOr<UpdateResult> Apply(const OrderUpdate&, ReportSource);

  // 读
  const Order* Find(const ClientOrderId&) const;
  bool HasTrade(const ExchangeTradeId&) const;
  std::vector<const Order*> ActiveOrders() const;
  std::vector<ClientOrderId> OrdersToQuery() const;
};
```

| 接口 | 调用者 | 为什么需要 |
|---|---|---|
| `Add` | ActionExecutor | 新订单从这里进入跟踪 |
| `OnSendResult` | ActionExecutor、网关事件 | `NotSent`：订单直接失败，资金可以释放；`Unknown`：资金继续冻结，等回报或查询 |
| `OnCancelRequested` | ActionExecutor | 记下"正在撤单"，撤单结果到达前订单仍算活跃 |
| `Apply(OrderUpdate, source)` | Shard | 处理交易所的消息：带成交时先去重、累计成交量、检查是否超过下单量，再更新状态。只有 `Query` 来源能消除"回报矛盾、需要查询"，所以要标明来源 |
| `Find` | Shard | 处理成交时取买卖方向 |
| `HasTrade` | Shard | `Apply` 前判断成交是否已记账；重复成交即使带来新的订单状态，也不能再次记账 |
| `ActiveOrders` | Shard（交给策略）、管理接口（挂单数） | 只含未结束的订单，不随运行时间增长。策略用它决定撤哪些单 |
| `OrdersToQuery` | 实盘的查询调度 | 结果未知、或回报矛盾的订单，需要向交易所查询 |

`UpdateResult` 的每个值对应 Shard 的一个动作：
- `Ignored`：Shard 不记账、不写订单历史；
- `Updated`：Shard 记账、写订单历史；
- `Finished`：在 `Updated` 的基础上，再让风控释放这张单剩余的资金冻结。

用枚举而不是两个 bool：原来的 `changed = false, finished = true` 是不可能出现的组合，三个值正好覆盖所有情况。

### 6.2 内部（外面看不到）

每张订单在订单跟踪器内部是一项：

```cpp
struct Entry {
  Order order;
  OrderStatus status;          // 内部枚举，见下表
  Decimal executed_quantity;   // 由逐笔成交累计
};
std::map<ClientOrderId, Entry> orders_;     // 唯一的索引
std::map<ExchangeTradeId, ClientOrderId> seen_trades_;  // 去重并检测跨订单重复编号
```

`OrderStatus` 是订单跟踪器根据回报推导出的结论：

| 值 | 含义 | 活跃吗 |
|---|---|---|
| `PendingCreate` | 已登记，交易所还没确认 | 是 |
| `Open` | 挂单中 | 是 |
| `PartiallyTraded` | 部分成交，仍在挂 | 是 |
| `PendingCancel` | 已请求撤单，还没确认 | 是 |
| `SubmissionUnknown` | 已发出，不知道交易所是否收到 | 是（资金继续冻结；需要查询） |
| `NeedsQuery` | 回报之间矛盾，本地状态不可信 | 是（需要查询） |
| `AwaitingTrades` | 交易所说已全部成交，但成交明细没到齐 | 否（资金暂不释放） |
| `Traded` / `Canceled` / `Failed` / `Expired` | 结束 | 否 |

为什么需要它，又不放到外面：交易所只知道自己的状态，`PendingCancel`、`SubmissionUnknown`、`NeedsQuery`、`AwaitingTrades` 是只有我们才知道的情况，订单跟踪器要靠它们决定怎么处理下一条回报。但外面只用到三个结论：是否活跃（`ActiveOrders`）、是否需要查询（`OrdersToQuery`）、是否结束（`UpdateResult::Finished`），所以不必把细分状态交出去。

## 7. 一张订单的完整流程

```
策略                  SubmitOrder{Buy, 0.01, 60000, Gtc}
ActionExecutor        风控按 SubmitOrder 冻结 600 USDT
网关 PrepareSubmit    SubmitOrder + 本地订单号 + 策略 + 账户 + 交易对 → Order
订单历史               记下 Order（发出前）
订单跟踪器 Add         Entry{Order, PendingCreate, 0}
网关                  发出
交易所                OrderUpdate{Open}                  → Apply → Open，返回 Updated
                      OrderUpdate{Traded, trade: 0.01}   → Apply → 累计 0.01，Traded，返回 Finished
Shard                 有 trade → 用 Find 取到 Buy，让风控记账
Shard                 Finished → 风控释放剩余冻结
策略                  下次运行时，ActiveOrders() 里已经没有这张单
```

## 8. 与现有代码的对应

| 现在 | 本设计 | 说明 |
|---|---|---|
| `OrderRequest` | 去掉 | 字段分别进入 `SubmitOrder` 和 `Order`，不再作为嵌套结构 |
| `SubmitOrder{strategy_id, request}` | `SubmitOrder`（扁平） | 去掉 `strategy_id`、`account`、`market` |
| `OrderType` + `std::optional<TimeInForce>` | `TimeInForce`（新增 `PostOnly`） | `LimitMaker` → `PostOnly` |
| `limit_price`（`optional`） | `price`（必填） | |
| `ApprovedOrder` | 去掉 | 资金冻结编号由 Shard 自己保存；过期时间作为 `PrepareSubmit` 的参数 |
| `PreparedOrder` | `Order` | 去掉 `created_at_utc`、`config_revision`、`executor_checkpoint` |
| `StrategyCheckpoint` | 去掉 | 没有策略使用 |
| `OrderSnapshot` | 去掉 | 外面只拿 `const Order*` |
| `OrderDisplayState` | 订单跟踪器内部的 `OrderStatus` | 和原来的生命周期、确认状态、撤单标志合成一个枚举 |
| `TrackerResult` | `UpdateResult` | 不再带订单副本和事件，从结构体变成三个值的枚举 |
| `OrderOpened` 等 5 个事件、`TrackedOrderEvent` | 去掉 | 没有使用者 |
| `MarketEvent` | 去掉 | 没有使用者 |
| `TradeUpdate` | `OrderUpdate::trade`（类型 `Trade`） | 两个消息合成一个；成交明细只保留成交独有的 6 个字段 |
| `OrderTracker::ApplyTradeUpdate` | 并入 `Apply(OrderUpdate)` | |
| `OrderUpdate` / `TradeUpdate` 的 `client_order_id`（`optional`） | 必填 | 所有来源都有 |
| `OrderUpdate` / `TradeUpdate` 的 `exchange_order_id` | 去掉 | 账户查询内部需要时，用局部变量保存 |
| `OrderUpdate::exchange_status` | `status` | 改名 |
| `OrderUpdate::traded_quantity` | `executed_quantity` | 改名 |
| `OrderUpdate::traded_value` | 去掉 | 没有读取者 |
| 订单跟踪器的 `exchange_index_` | 去掉 | 只有本地订单号一个索引 |
| 订单跟踪器内部的 `traded_value`、`fees_by_asset`、`last_update_time`、`created_emitted`、`terminal_emitted` | 去掉 | 没有读取者 |
| 订单跟踪器的 `Register`、`RequestCancel`、`FailBeforeWrite`、`MarkSubmissionUnknown`、`ApplyOrderUpdate`、`ApplyTradeUpdate`、`ApplyQueriedOrder`、`Snapshot`、`OrdersNeedingQuery` | 第 6.1 节的 4 个写接口、4 个读接口 | |
| Shard 的 `order_ids_`、`OrderViews()` | 去掉 | 由 `ActiveOrders()` 取代 |

## 9. 待定

1. **`Trade::maker`**：只写进订单历史，代码里没有读取者。要不要留，取决于是否需要事后统计挂单成交和吃单成交。
2. **已成交数量对外**：将来如果有策略需要"剩余数量"（比如按剩余量改价），那时再让 `ActiveOrders` 带上它。
3. **订单历史的格式（已定）**：二进制编码升到 v3；新记录写 `Order` 和含成交的 `OrderUpdate`，仍可读取 v1/v2 的旧记录。旧字段仅保留在历史兼容结构体中。
4. **`seen_trades_` 的清理**：已结束订单的成交编号要保留多久才能删，需要定一个规则（例如订单结束后再保留一段时间）。
