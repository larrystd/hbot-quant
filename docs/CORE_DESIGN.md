# 模块、核心结构体与 Python/C++ 语义对照

状态：接口设计基线。整体架构图见 [ARCHITECTURE_DIAGRAM.md](ARCHITECTURE_DIAGRAM.md)，依赖和网络模型见 [DEPENDENCIES.md](DEPENDENCIES.md)，迁移顺序见 [PLAN.md](PLAN.md)。对应 Python 源码版本为 `2.17.0` / `9af100d6822da7d2d0291a906c730ef172284ee2`。

## 1. 模块边界

```text
hbot/           每个子目录是一个 Bazel package：头文件、实现、BUILD.bazel 和单元测试放在一起
  base/         Decimal、强类型 ID、时间、Status、JsonWriter、日志（Quill 封装）
  model/        TradingPair、TradingRule、Order、Trade、Fee、Balance、Position
  event/        类型化事件、订阅句柄、事件队列
  market_data/  OrderBook、Snapshot/Diff、Candles、MarketDataService
  net/          HTTP/WS/TLS、连接复用、超时与安全重试、限速、时间同步、重连
  connector/    IExchangeConnector、ConnectorManager、OrderTracker、venue adapters
  storage/      IOrderIntentStore、IHistoryQuery、SQLiteRecorder、schema migration、订单状态恢复
  strategy/     IStrategy、Controller、Executor、Orchestrator
  risk/         RiskGate：单笔上限、总敞口、紧急停止、未知订单额度占用，与交易所无关
  backtest/     HistoricalDataProvider、ReplayClock、ExecutorSimulator
  app/          TradingEngine、交易/网络两个 io_context、引擎状态机、ControlServer
  cli/          参数解析、输出、进程控制
apps/           //apps:hbot、//apps:hbot-engine 可执行程序
tests/          跨模块集成测试、协议 mock 服务器、重放与差分夹具（fixtures/）
dev/            开发期 Bazel smoke target、协议调试工具和本地验证脚本
third_party/    BCR 之外的依赖的 BUILD 文件，例如 mpdecimal
```

头文件按工作区路径包含，例如 `#include "hbot/base/decimal.h"`。下面的依赖方向由各 package 的 `visibility` 强制，违反时构建失败。

依赖方向：`base` 被其余模块使用；`model` 依赖 `base`；`event`、`market_data` 依赖 `model`；`net` 只依赖 `base`；`connector` 依赖 `net`、`model`、`event`、`market_data` 和 `//hbot/storage:order_intent_store` 抽象接口；`storage:sqlite_recorder` 实现该接口及独立的 `//hbot/storage:history_query` 读接口，并消费 `model`/`event`；`strategy` 依赖抽象 connector、model 和 event；`risk` 依赖抽象 connector 与 model；`backtest` 复用 model/strategy/risk 与模拟盘连接器，并注入模拟数据与时间；`app` 装配各模块，`cli` 依赖 app。`strategy` 不引用 Binance 等具体 adapter，`app` 不承载交易所特定字段。

运行时所有权：`TradingEngine` 拥有 ConnectorManager、策略、`trade_io_context`、`net_io_context` 和 Recorder；ConnectorManager 拥有连接器。连接器的 OrderTracker、盘口与风险相关状态属于交易线程；公开 WS、私有 WS、各 REST 连接及共享限速器属于网络池中的各自 strand。ExecutorOrchestrator 在交易线程拥有执行器。通过 RAII 订阅句柄撤销事件监听，不让回调持有已销毁对象。交易线程独自运行 `trade_io_context` 并修改所有交易状态；默认两个网络线程共同运行 `net_io_context`，彼此通过有界消息通道投递按值数据；Recorder 通过有界 SPSC 队列在独立线程访问 SQLite，Quill 后台线程负责日志。线程模型见 [ARCHITECTURE_DIAGRAM.md](ARCHITECTURE_DIAGRAM.md) 第 2 节。

## 2. 数据契约

下列是设计签名，正式头文件实现时加入构造校验、序列化版本和单元测试。`StatusOr<T>` 均为 `absl::StatusOr<T>`；`Timestamp` 是 UTC epoch 微秒，超时/退避另用单调时钟。

```cpp
using Timestamp = std::chrono::sys_time<std::chrono::microseconds>;
enum class RoundingMode { HalfEven, HalfUp, Down, Up, Floor, Ceiling };
struct ArithmeticContext {
  std::uint32_t precision = 28;
  RoundingMode rounding = RoundingMode::HalfEven;
};

class Decimal {
 public:
  Decimal(const Decimal&);            // 深拷贝
  Decimal(Decimal&&) noexcept;
  Decimal& operator=(const Decimal&); // 深拷贝
  Decimal& operator=(Decimal&&) noexcept;
  ~Decimal();                         // 在实现文件中定义
  static absl::StatusOr<Decimal> Parse(std::string_view text);
  absl::StatusOr<Decimal> Divide(const Decimal& rhs, const ArithmeticContext& context) const;
  absl::StatusOr<Decimal> Rescale(std::int32_t scale, RoundingMode mode) const;
  absl::StatusOr<Decimal> RoundToIncrement(const Decimal& increment, RoundingMode mode) const;
  std::string ToString() const;
 private:
  struct Impl;
  std::unique_ptr<Impl> value_;       // Impl 持有 mpd_t；只有实现文件包含 mpdecimal.h
};

struct ConnectorId { std::string value; };
struct AssetId { std::string value; };
struct ClientOrderId { std::string value; };
struct ExchangeOrderId { std::string value; };
struct ExchangeTradeId { std::string value; };
struct TradingPair { AssetId base; AssetId quote; };
struct MarketId { ConnectorId connector; TradingPair pair; };

enum class Side { Buy, Sell };
enum class TradeType { Buy, Sell, Range };
enum class OrderType { Market, Limit, LimitMaker, AmmSwap, AmmAdd, AmmRemove };
enum class PositionAction { Nil, Open, Close };
enum class PositionSide { Long, Short, Both };
enum class PositionMode { Hedge, OneWay };
enum class OrderState {
  PendingCreate, Open, PendingCancel, PartiallyFilled,
  Filled, Canceled, Failed, PendingApproval, Approved, Created, Completed
};
```

`Decimal` 封装 libmpdec（CPython `decimal` 的底层实现）；字符串解析、乘除、比较和舍入均复用 libmpdec 的语义，状态标志转换为 `absl::Status`。`ArithmeticContext` 首版采用与 Python 默认 `decimal` 一致的 28 位有效数字、half-even 舍入、`Emax=999999`/`Emin=-999999`；具体连接器显式指定的舍入规则优先。`Rescale` 按十进制位数取整，`RoundToIncrement` 按交易所步长取整，两者不能混用，例如 `0.05` 是步长，不等于保留两位小数。限制输入位数与 scale，拒绝 NaN/Infinity；Python 用 `Decimal("NaN")` 表示缺失价格的地方改用 `std::optional<Decimal>`。所有交易数值在本项目 JSON/SQLite 中保存为字符串。交易所给出的毫秒时间戳转换为 `Timestamp`，不在领域对象中保存 `double` 秒数。

入站 JSON 的价格、数量、费用可能是字符串，也可能是原生 JSON 数字。adapter 用 simdjson On-Demand 解析，对原生数字取 `raw_json_token()` 的原始十进制文本、对字符串取其值，再构造 `Decimal`。禁止先解析为 `double` 再转换为 `Decimal`。出站请求由 `JsonWriter` 按交易所要求把 `Decimal` 写成字符串或原文数字。YAML 数值同样先读标量文本。序列号/时间戳解析为有范围检查的整数；无法表示的消息返回协议错误。

| 结构体 | 主要字段 | Python 对照 / 约束 |
| --- | --- | --- |
| `TradingRule` | `MarketId`、最小/最大数量、价格/base/quote 数量步长、最小名义金额/订单价值、最大价格有效位数、支持的订单类型、买卖抵押资产 | [`TradingRule`](../../hummingbot/hummingbot/connector/trading_rule.pyx)；未知上限与非必需步长用 `std::optional`，实际生效的步长必须为正 |
| `TokenAmount`、`TradeFee` | 资产/数量；百分比、百分比资产、固定费用列表、`AddedToCost` / `DeductedFromReturns` | [`TradeFeeBase`](../../hummingbot/hummingbot/core/data_type/trade_fee.py)；费用资产和费用模式必须显式保留 |
| `BookLevel`、`BookSnapshot`、`BookDiff`、`BookScale` | 价格、数量、交易对、首/末序号、交易所时间、行情价格/数量步长 | [`OrderBookMessage`](../../hummingbot/hummingbot/core/data_type/order_book_message.py)；adapter 负责原始序号和行情步长规则，核心只接收规范化快照/增量 |
| `Candle` | 起始时间、周期、OHLC、base/quote volume、成交数、taker 买量 | [`CandlesBase`](../../hummingbot/hummingbot/data_feed/candles_feed/candles_base.py)；历史和实时使用同一列定义 |
| `OrderRequest` | `MarketId`、方向、类型、数量、可选价格、开/平仓标志、杠杆 | [`ExchangePyBase.buy/sell`](../../hummingbot/hummingbot/connector/exchange_py_base.py)；连接器接收请求时生成并返回本地 ID，交易所确认另发事件 |
| `RecoveryContext`、`OrderIntent` | 稳定的策略/执行器 ID、版本化配置引用、执行器状态快照；本地订单 ID、原始请求与创建时间 | 本地独有的归属和控制状态；发送时异步交给 Recorder，不等待 SQLite；连接器不解析执行器快照 |
| `OrderUpdate` | 本地/交易所 ID、市场、状态、更新时间、原因 | [`OrderUpdate`](../../hummingbot/hummingbot/core/data_type/in_flight_order.py)；至少一个 ID 存在，状态转移受检查 |
| `TradeUpdate` | 三种 ID、市场、成交时间、价格、base/quote 数量、费用、maker/taker | [`TradeUpdate`](../../hummingbot/hummingbot/core/data_type/in_flight_order.py)；在已跟踪订单内按 trade ID 去重，持久化唯一键包含 connector、交易对、client order ID、trade ID |
| `TrackedOrder` | 原始请求、交易所 ID、当前状态、累计成交量/金额、已见成交 ID、创建/更新时间 | [`InFlightOrder`](../../hummingbot/hummingbot/core/data_type/in_flight_order.py)；仅 OrderTracker 可变更，外部只读快照 |
| `Balance`、`Position` | 资产可用/总额；市场、方向、数量、开仓价、杠杆等 | 现货与衍生品字段分离；余额和持仓不能靠成交事件推断为交易所最终真值 |
| `ExecutorAction` | `controller_id` + `std::variant<Create, Stop, Store>` | [`executor_actions.py`](../../hummingbot/hummingbot/strategy_v2/models/executor_actions.py)；每个动作携带类型化配置 |

`AccountEvent` 使用 `std::variant<OrderCreated, OrderFilled, OrderCompleted, OrderCanceled, OrderFailed, FundingPayment, ...>`，并附 `EventMeta {market, exchange_time, receive_time, local_sequence}`。公开成交 `PublicTrade` 是另一类市场数据事件，绝不转换为本账户的 `OrderFilled`。事件消费端各自处理同一事件：策略/执行器更新决策，Recorder 写入 SQLite。

这些领域结构体的首版字段如下；`OrderTracker` 才能修改 `TrackedOrder`，其余模块获取值快照。

```cpp
struct TradingRule {
  MarketId market;
  Decimal min_order_size, price_increment, base_increment;
  std::optional<Decimal> max_order_size, quote_increment;
  Decimal min_notional, min_order_value;
  std::optional<int> max_price_significant_digits;
  bool supports_limit, supports_market;
  AssetId buy_collateral, sell_collateral;
};

struct TokenAmount { AssetId asset; Decimal amount; };
enum class FeeMode { AddedToCost, DeductedFromReturns };
struct TradeFee {
  FeeMode mode;
  Decimal percent;
  std::optional<AssetId> percent_asset;
  std::vector<TokenAmount> flat_fees;
};

struct OrderRequest {
  MarketId market;
  TradeType trade_type;
  OrderType type;
  Decimal amount;
  std::optional<Decimal> price;
  PositionAction position_action;
  int leverage;
};
struct RecoveryContext {
  std::string strategy_id;
  std::optional<std::string> executor_id;
  std::string config_revision;       // 稳定配置版本标识；恢复时须能找到对应配置
  std::uint32_t state_schema_version;
  std::string executor_state;        // 由执行器序列化；无执行器时为空
};
struct OrderIntent {
  ClientOrderId client_id;
  OrderRequest request;
  RecoveryContext recovery;
  Timestamp created_at;
};
struct OrderUpdate {
  MarketId market;
  Timestamp exchange_time;
  OrderState state;
  std::optional<ClientOrderId> client_id;
  std::optional<ExchangeOrderId> exchange_id;
  std::optional<std::string> reason;
};
struct TradeUpdate {
  MarketId market;
  ClientOrderId client_id;
  ExchangeOrderId exchange_id;
  ExchangeTradeId trade_id;
  Timestamp exchange_time;
  Decimal price, base_amount, quote_amount;
  TradeFee fee;
  bool is_taker;
};
struct BookLevel { Decimal price, amount; };
struct BookDiff {
  MarketId market;
  std::uint64_t first_sequence, last_sequence;
  Timestamp exchange_time;
  std::vector<BookLevel> bids, asks;
};
struct BookSnapshot {
  MarketId market;
  std::uint64_t last_sequence;
  Timestamp exchange_time;
  std::vector<BookLevel> bids, asks;
};
struct BookScale {
  Decimal price_step, amount_step;  // adapter 为该市场的行情流提供，不直接复用下单步长
};
enum class ReconciliationState { Confirmed, SubmissionUnknown, ResyncRequired };
struct TrackedOrder {
  ClientOrderId client_id;
  OrderRequest request;
  std::optional<ExchangeOrderId> exchange_id;
  OrderState state;
  ReconciliationState reconciliation_state;
  Decimal executed_base, executed_quote;
  std::vector<TradeUpdate> fills;  // OrderTracker 内另建 trade ID 索引
  Timestamp created_at, updated_at;
};
struct Balance {
  AssetId asset;
  Decimal total, available;
  Timestamp as_of;
};
struct Position {
  MarketId market;
  PositionSide side;
  Decimal amount, entry_price;
  int leverage;
  std::optional<Decimal> unrealized_pnl;
  Timestamp as_of;
};
```

`OrderUpdate` 允许只有一种订单 ID，但必须能在 `OrderTracker` 找到唯一订单；同时给出两种 ID 时都必须与已跟踪订单一致。不能确定归属或与已知 ID 冲突的更新进入对账队列。`TrackedOrder` 单独保存 `ReconciliationState`，不把网络超时伪装为 Python `OrderState::Failed`。交易所特有的 `misc_updates` 留在 adapter 的类型化附加信息中；每种新增字段都要决定是否需要提升到通用领域模型。`BookSnapshot` 与 `BookDiff` 共用档位字段，但快照序号只有一个 `last_sequence`；先由 adapter 消化各交易所首末序号规则。`OrderBook` 用 adapter 提供的 `BookScale` 做精确整除和 `int64` 范围检查；若行情流没有固定可验证的步长，该市场先标为不支持，不用下单步长强行量化或无限重取快照。

`OrderTracker` 在 `TrackedOrder` 外维护按 trade ID 的索引，用于常数时间去重；序列化时成交按明确的时间和 ID 排序。现货余额与衍生品持仓分别查询，未知字段用 `optional`，不能用 0 冒充交易所未返回的数据。首批交易所 adapter 必须提供正的价格与 base 数量步长；未知最大下单量、非必需的 quote 步长才为空值，不能把 Python 的极大/极小哨兵原样带入交易域。

规范化 `Decimal` 在数值比较中忽略末尾零；发送交易所请求时由 adapter 根据规则格式化数量和价格，签名必须覆盖最终发送的原始字节。枚举与状态持久化使用显式字符串映射，不依赖 C++ 枚举序号；`absl::flat_hash_map` 等哈希容器的遍历顺序不用于生成策略动作顺序，排序按市场、时间和本地序号显式执行。

## 3. 核心接口

`net` 对连接器暴露 `IHttpClient`/`IWebSocketClient`，连接器不直接持有 Beast socket。HTTP 请求保留签名后的原始 `target`、头与 body 字节；`origin` 是规范化的 scheme/host/port，作为连接复用键。下列 `RequestOptions` 由 adapter 按交易所端点填写，`IHttpClient` 统一执行，不让每个 adapter 重写连接池、超时与重试。

```cpp
enum class HttpRetryClass { Never, IdempotentRead };
struct RequestOptions {
  std::chrono::milliseconds connect_timeout, tls_timeout, write_timeout, read_timeout, total_timeout;
  std::size_t max_response_bytes;
  HttpRetryClass retry_class = HttpRetryClass::Never;
  std::string rate_limit_bucket;
  std::uint32_t weight = 1;
};
struct HttpRequest {
  std::string method, origin, target;
  std::vector<std::pair<std::string, std::string>> headers;
  std::string body;
};
struct HttpResponse {
  int status_code;
  std::vector<std::pair<std::string, std::string>> headers;
  std::string body;
};
class IHttpClient {
 public:
  virtual ~IHttpClient() = default;
  virtual boost::asio::awaitable<absl::StatusOr<HttpResponse>> Execute(
      HttpRequest request, RequestOptions options) = 0;
};
```

`total_timeout` 使用单调时钟，覆盖排队、限速等待、DNS/连接/TLS 和收发；阶段超时不能延长总期限。`IdempotentRead` 才允许有次数上限的指数退避与随机扰动，并服从交易所 `Retry-After`；签名写请求默认 `Never`。429/交易所权重响应反馈给连接器级限速器。HTTP 发送超时只能表示结果未知，订单由 `OrderTracker` 按同一个本地 ID 对账，不自动创建第二个订单。

`//hbot/storage:order_intent_store` 只暴露非阻塞入队端口，不暴露 SQLite 连接。订单意图、执行器检查点与账户事件按交易线程的发生顺序尝试入队；队列满立即返回错误，后台提交失败通过 `StorageFault` 投递给交易线程上的 `TradingEngine`。这两种存储故障都只改变历史完整性和恢复能力，不阻止当前进程继续发送已经通过风控的订单。读接口 `IHistoryQuery` 也经交易线程进入 Recorder 命令队列，在 Recorder 线程执行带分页/行数上限的 `SELECT`，结果投递回发起查询的 ControlServer strand；只读请求排队失败返回 `Busy`。

```cpp
class IOrderIntentStore {
 public:
  virtual ~IOrderIntentStore() = default;
  virtual absl::Status EnqueueIntent(OrderIntent intent) = 0;
  virtual absl::Status EnqueueCheckpoint(RecoveryContext state) = 0;
  virtual absl::Status EnqueueEvent(AccountEvent event) = 0;
};

struct HistoryQuery {
  std::optional<MarketId> market;
  Timestamp from;
  Timestamp until;
  std::uint32_t limit;                 // 入口按配置上限校验
  std::optional<std::string> cursor;
};
struct HistoryPage {
  std::vector<AccountEvent> events;
  std::optional<std::string> next_cursor;
  bool incomplete;                    // 有已知存储缺口或未核实区间
};
class IHistoryQuery {
 public:
  virtual ~IHistoryQuery() = default;
  // 仅由交易线程调用；reply_executor 是 ControlServer 连接的 strand。
  virtual absl::Status EnqueueHistory(
      HistoryQuery query, boost::asio::any_io_executor reply_executor,
      absl::AnyInvocable<void(absl::StatusOr<HistoryPage>)> reply) = 0;
};
```

```cpp
class IExchangeConnector {
 public:
  virtual ~IExchangeConnector() = default;
  virtual boost::asio::awaitable<absl::Status> Start() = 0;
  virtual boost::asio::awaitable<absl::Status> Stop() = 0;
  virtual absl::StatusOr<ClientOrderId> Submit(
      OrderRequest request, RecoveryContext recovery) = 0;
  virtual absl::Status Cancel(const ClientOrderId& id) = 0;
  virtual absl::StatusOr<BookSnapshot> GetOrderBook(const TradingPair& pair) const = 0;
  virtual absl::StatusOr<Balance> GetBalance(const AssetId& asset) const = 0;
};

class IStrategy {
 public:
  virtual ~IStrategy() = default;
  virtual absl::Status OnStart(Timestamp now) = 0;
  virtual absl::Status OnTick(Timestamp now) = 0;
  virtual absl::Status OnEvent(const AccountEvent& event) = 0;
  virtual boost::asio::awaitable<absl::Status> OnStop() = 0;
};
```

`Submit()` 由连接器生成满足交易所格式限制且跨重启不复用的 `ClientOrderId`，放入 `TrackedOrder` 并返回；`OrderRequest` 不带 ID，`RecoveryContext` 由策略/执行器提供，包含稳定配置版本标识和当前执行器状态快照。`Cancel()` 使用已有 ID。两者的同步返回只表示命令进入本地发送流程，**不表示已落盘或交易所接单**；交易所接单、成交、撤单由后续事件说明。若网络发送队列或风险检查拒绝，立即返回错误；Recorder 队列失败只告警。网络超时后订单进入待核对状态；连接器按原本地 ID 查询交易所，明确结果后才允许下一步决策。接口实现会另定义异步余额/规则刷新；上面的同步读取只返回最新已确认快照。

**策略执行入口：** `SchedulerLoop()` 在 `trade_io_context` 等待定时器，醒来后直接调用策略的 `OnTick()`，并运行 Controller/Executor 的控制循环；`EventBus` 在同一交易线程处理 `AccountEvent` 时直接调用 `OnEvent()`。这些回调只读写交易线程中的内存快照和状态，不访问网络 socket 或 SQLite。`OnStop()` 的协程也只在交易 executor 上恢复；如需等待撤单结果，只异步等待交易事件。实盘连接器的 `Start()`/`Stop()` 在交易 executor 上编排生命周期，真正的公开流、私有流、REST 网关循环分别用 `co_spawn(对应的 strand, ...)` 在 `net_io_context` 运行，其结果按值投递回交易线程。网络协程的 `co_await` 让出网络工作线程，恢复时仍受各自 strand 串行约束，但不固定在原 OS 线程。

**实盘新单不等待 SQLite：** `Submit()` 先确保有界网络命令队列能接受该单，再生成本地 ID、登记 `PendingCreate`，把命令投递到网关 strand；若命令队列满，同步拒绝并撤销 RiskGate 的额度预留。同一份 `OrderIntent`（请求、归属、配置版本、执行器状态快照）以非阻塞方式尝试交给 Recorder。网关 strand 异步签名和发送，HTTP/私有 WS 回报通过有界入站队列交给交易线程的 OrderTracker。Recorder 在 WAL + `synchronous=NORMAL` 下后台批量写入；提交后不回调策略，也不触发发单。入队或写入失败时告警、累计丢失计数并将当前运行的历史标记为不完整，但不阻止下单/撤单。当前进程继续依靠内存中的 OrderTracker、RiskGate 和执行器状态运行；若此时崩溃或重启，缺失的执行器状态可能无法自动恢复，必须先对账并暂停受影响的状态型执行器。`Cancel()` 同样异步发送并记录；撤单命令队列满时保持原订单状态，不虚标 `PendingCancel`。撤单结果未知时对账。订单簿队列写满时清除旧增量并重新同步快照；私有账户事件不能静默丢弃，要降级并补查；公开成交流允许在记录丢弃计数后限流。

SQLite 的消费路径只有异步 `history`/报告查询和重启时的状态恢复、交易所对账；`status` 查询直接读取交易线程内存。数据库保存交易所不知道的本地归属、配置版本、执行器检查点，以及已接收的账户事实，方便在交易所历史窗口外追踪。它不是实盘交易的同步状态机；详情见 [ARCHITECTURE_DIAGRAM.md](ARCHITECTURE_DIAGRAM.md) 第 2.2–2.3 节。

本地订单 ID 可以附带紧凑、稳定的策略/执行器编码，例如 `HB1-<strategy_code>-<executor_code>-<unique>`，由 adapter 校验各交易所的字符、长度和改写规则；归属编码须来自稳定配置，不能是 SQLite 行号。编码是数据库缺失时的额外归属线索。`unique` 由进程启动时生成的密码学随机 `run_id` 与进程内单调序号组合，避免依赖 SQLite 分配或重启后从 1 复用。若交易所长度容不下归属编码与唯一部分，优先保证唯一性。交易所并不知道本地配置版本、执行器止盈止损/DCA 状态，也不保证 ID 本身具备跨所有历史订单的去重语义；同一个 ID 的写请求超时后也不能盲目重发。**重启对账：** 恢复可用的本地意图与检查点，再按交易所支持的市场和时间窗口分页查询挂单、历史订单、成交及余额。无状态策略可在账户状态核实后重建；状态型执行器仅在归属、配置和内部状态均可验证时恢复，缺失状态不凭 ID 猜测。历史回报可补录，补录区间标记为不完整直到核对结束。具体故障分级见 [ARCHITECTURE_DIAGRAM.md](ARCHITECTURE_DIAGRAM.md) 第 6 节。

`IClock` 有 `RealtimeClock` 和 `ReplayClock`；策略 tick 与 Controller 的独立控制循环由 Scheduler 调度。回测使用相同的强类型动作/事件，但由 `ExecutorSimulator` 处理动作。配置层把 YAML 映射到版本化 `StrategyConfig`、`ConnectorConfig`、`ControllerConfig`，缺字段或非法范围返回 `Status`，不会把任意 JSON/YAML 字典传到交易内核。

## 4. Python 与 C++：保留的行为和需要改变的实现

| 主题 | 当前 Python/Cython | C++ 决定 | 验收方式 |
| --- | --- | --- | --- |
| 运行调度 | `asyncio` 任务 + Cython `Clock` tick | 专用交易线程用 Asio 定时器触发 `OnTick`，网络池用 Asio 协程收发；事件通过有界队列进入交易线程 | 记录同一输入下 tick、事件和动作的因果顺序；允许调度时间窗口差异 |
| 精度 | 下单/费用用 `Decimal`；订单簿行内部为 `float` | 交易域使用 libmpdec Decimal；adapter 输出的 `BookSnapshot`/`BookDiff` 仍为 Decimal，进入 `OrderBook` 时按行情流 `BookScale` 转成 `int64` ticks/lots 存入 `absl::btree_map`，整除和溢出均显式检查 | 金额精确一致；盘口结果按行情流精度比较，记录 Python float 差异 |
| 协议数值 | Python adapter 可从字符串直接构造 `Decimal` | JSON/YAML 保留价格、数量、费用的原始文本；simdjson 取原始数字文本，任何 `double` 都不能进入 Decimal | 含长小数和原生 JSON 数字的报文解析后无精度损失 |
| 状态与错误 | 可变 `dict`、`None`、异常；Pydantic 做配置校验 | 强类型结构体、`optional`、`variant`、`absl::StatusOr`；入口处显式校验 | 无效配置、非法状态转移和溢出均有确定错误码 |
| 订单提交 | `buy/sell` 先返回 client ID，`safe_ensure_future` 再发请求 | `Submit` 先给 ID，立即异步发送；意图和回报由后台 Recorder 批量落盘 | 不能把本地 ID 当成交易所确认；注入 Recorder 故障时发送延迟不增加且继续下单 |
| 完成判定 | `InFlightOrder` 对 Decimal 使用 `math.isclose`，另有按 `1e-8` 量化的完成信号 | `FillCompletionPolicy` 显式规定容差和交易所终态优先级，不经 `double` | 边界成交量与 Python 行为逐例比较；改变判定的用例写入差异清单 |
| 回报校验 | Python `InFlightOrder` 可凭任一 ID 接受回报，并直接改写状态 | 两种 ID 同时出现必须都匹配；乱序/冲突状态进入对账，不让终态静默回退 | 重放冲突 ID、晚到 `OPEN`、部分成交与撤单竞态 |
| 事件监听 | PubSub/弱引用回调；策略与 Recorder 分别监听 | 类型化事件总线 + RAII 订阅句柄 + 有界队列 | 重启/停止后无悬空回调；慢消费者不会无限占内存 |
| 动态扩展 | `importlib` 按名称载入 Python 策略/连接器 | 首版静态工厂注册；后续版本化 C ABI 插件 | 配置名称到工厂映射明确；未知类型在启动前失败 |
| 数据库存储 | SQLAlchemy 模型 + SQLite | SQLite C API + RAII 事务/迁移；意图、检查点和账户回报均后台批量记录 | 故障时标记历史缺口；重启对账时不凭缺失的状态自动恢复执行器 |
| 并发内存 | GIL/事件循环管理多数对象生命周期 | 显式所有权、交易线程串行更新交易状态、各 socket/限速器 strand 保护自己管辖的网络对象、跨线程传值、RAII | ASan/UBSan、竞态与关闭测试通过 |
| 回测 | DataFrame 按 K 线推进，独立执行器模拟器 | `ReplayClock` + 历史 Candle 流 + 类型化模拟器 | 同数据重复运行一致；记录与 Python 模型的撮合差异 |

需要保持的外部语义：配置中的交易对和策略参数、客户端订单 ID 与交易所订单 ID 的区分、创建/部分成交/完成/取消/失败事件、手续费资产、订单恢复、不重复下单、CLI 的状态/历史数据含义。可改变的内部实现：对象所有权、事件队列、数据库访问方式、线程模型、插件加载方式。

## 5. 首批契约测试

1. `Decimal`：十进制字符串往返、负数、极大/极小值、除法上下文、十进制位数与 `0.05` 步长的区别；与 Python `Decimal` 用同一输入比较。超出允许的位数/scale 返回错误。
2. 入站数值：JSON 字符串与原生数字 `0.123456789012345678901`、科学记数法、YAML 标量分别解析，确保 `Decimal` 直接从原始文本构造；越界整数按协议错误处理。
3. `OrderTracker`：接单前成交、重复 trade ID、部分成交、撤单与成交竞态、REST/WS 乱序、重启恢复；每个成交只记账一次。
4. `OrderBook`：快照 + 增量、旧增量丢弃、序号缺口重同步、数量为零删档；公开成交只更新市场事件。
5. `TradingEngine`：连接器 ready 后策略开始交易；停止时先停新动作、再处理执行器与挂单、最后关闭网络和 Recorder。
6. `BacktestEngine`：相同历史 Candle、配置、费用和随机种子重复运行，动作序列与结果可复现。
7. 崩溃恢复：在发单后、意图或回报落盘前强制终止进程；重启后按客户端 ID、市场、历史订单和成交对账；无状态策略在核实后重建，缺失检查点的状态型执行器不自动恢复，也不重复下单。
8. HTTP 客户端：同源连接复用、各阶段与总超时、429/`Retry-After`、读取请求有界重试、写请求超时后对账，均用本地 mock 服务器验证。
9. 存储故障：Recorder 队列满或 SQLite 写入失败时仍能下单与撤单，内存风险检查继续生效，告警和丢失计数可见；恢复写入后补录可查历史，重启时缺失执行器状态不得自动恢复，客户端订单 ID 跨重启不复用。
