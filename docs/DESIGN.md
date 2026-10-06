# hquant 1.0 设计

运行路径：Binance 公开行情或文件回放 → Shard → SQLite 历史。Control 负责查询状态、历史、修改策略参数和停止。每个 Shard 固定一个市场、一个 Strategy 和一个模拟账户。

本版只做模拟盘。`executor.mode` 是必填配置，取值 `paper` 或 `live`；`live` 为接入真实交易所预留，当前在配置校验时被拒绝。本文只描述架构与运行机制，构建、测试和性能验证见 [DEVELOPMENT.md](DEVELOPMENT.md)。

## 1. 组成与职责

~~~mermaid
flowchart LR
    IN["行情输入<br/>Binance / 回放文件"] --> M
    subgraph ONE["一个 Shard（共 N 个，共用最多 8 个工作线程）"]
        M["Market<br/>维护盘口"] -->|"通知：盘口变了 / 有成交"| SH["Shard<br/>按顺序调度"]
        SH -->|"① 撮合已有挂单<br/>（仅模拟盘）"| PE["PaperExchange<br/>模拟交易所：订单、余额、撮合"]
        SH -->|"② 做决定"| ST["Strategy<br/>撤单 / 下单 / 不动"]
        SH -->|"③ 执行决定"| EX["Executor<br/>校验规则和额度"]
        EX -->|"下单 / 撤单"| PE
        PE -.->|"订单回报、成交、余额变化<br/>排队，稍后处理"| SH
    end
    SH -->|"写历史"| H["SQLite 历史"]
    C["Control 客户端"] --> R["Runtime / ControlServer"]
    R -->|"投递请求"| SH
    R -->|"查询"| H
~~~

图中实线是同步调用：调用方等对方处理完才继续往下走。①②③ 是 Shard 收到一次行情通知后的调用顺序。

虚线是异步投递。PaperExchange 在撮合、下单、撤单时会产生账户事件，包括订单回报、成交和余额变化，代码里统称 AccountEvent。它不直接调用 Shard，而是调用 Shard 在构造它时传入的回调 EventSink；这个回调只把事件排进 Shard 线程的任务队列。等当前这一轮处理全部结束，Shard 才逐条取出事件写历史。这样设计是为了和实盘一致：真实交易所的成交回报也是之后从另一条通道到达的，不会在下单调用里直接返回。

| 组件 | 职责 | 持有 |
| --- | --- | --- |
| Runtime | 创建工作线程和 Shard，按 ID 路由回放与 Control 请求，负责启动和停止顺序 | `io_context` × min(8, 分片数)、工作线程、全部 Shard、SqliteHistory、ControlServer |
| Shard | 一个市场的调度者：接收行情通知、定时器、回放和 Control 请求，按固定顺序调用撮合、决策、执行和记录；管理自身停止 | Market、Strategy、PaperExchange、Executor、策略定时器；引用所属 `io_context` |
| Market | 维护盘口：连接代次、快照与增量对齐、过期检测，向 Shard 发通知 | BinanceFeed、OrderBook、BookView、增量缓冲 |
| BinanceFeed | Binance REST 快照与 WebSocket 增量/成交的读取、解析、重连 | HTTP 与 WebSocket 客户端 |
| Strategy | 根据盘口和账户视图做一次决定：无动作、撤单或下单 | 策略配置、`next_refresh_at` |
| Executor | 我方执行：按交易规则和资金上限校验，把决定派发给交易所，提供账户视图 | 交易规则、资金上限；引用 PaperExchange |
| PaperExchange | 模拟交易所：活动订单与余额的唯一保存处，用行情撮合，经 EventSink 推送账户事件 | 活动订单、资产总额、订单与成交 ID |
| SqliteHistory | 异步写入历史，只读分页查询 | 写线程、读线程、两个 SQLite 连接 |
| ControlServer | Unix socket 上的一行一个 JSON 请求，JSON 编解码只在这里 | 监听 socket、服务线程 |
| ReplayReader | 解析回放文件，逐条交给 Runtime 并等待处理完成 | 无长期状态 |

Shard 只决定"什么时候调用谁"，不保存订单或余额。撮合属于交易所一侧，由 Shard 直接调用 PaperExchange，且只在 `paper` 模式下进行；Executor 只负责我方的校验和派发。

### 1.1 模拟盘与实盘

系统里有两份订单数据，不能混淆：

| | Market 的 OrderBook | PaperExchange 的活动订单 |
| --- | --- | --- |
| 里面是谁的单 | 交易所上其他人的单 | 只有我们自己的单 |
| 数据来源 | 交易所推送的快照和增量 | Strategy 决定、Executor 下单 |
| 作用 | 本地复制一份交易所的公开盘口 | 记录我们的挂单和余额 |

**模拟盘（`paper`，当前唯一可用的模式）。** 我们的订单只存在于 PaperExchange，不发往交易所。由此有两个直接结果：

1. 行情里永远不会出现我们的成交。盘口变化和公开成交都来自其他人之间的交易，交易所不知道我们的订单。
2. 我们的订单不影响市场。它不进入交易所盘口，不消耗别人的挂单，也不推动价格。

所以成交只能在本地推断。更新 OrderBook 只是同步市场，不涉及我们的订单；Market 通知 Shard 之后，Shard 调用 `PaperExchange::OnMarket`，用更新后的盘口和公开成交回答一个问题：如果我们的订单真的挂在交易所，现在会不会已经成交。推断规则见 5.6。

这个模型有几处简化，读模拟结果时要知道：

- 推断出的成交不会从 OrderBook 里扣掉对应数量。下一条行情到来时，同一档的可见数量会重新可用。
- 下单时不立即撮合，要等下一条行情才检查是否成交。即使挂单价已经穿过对手价，也是在下一条行情时才按对手价成交。
- 同价位的排队位置未知。按公开成交撮合时，成交价等于挂单价也算成交，相当于假设我们排在队首。
- 不模拟我们的订单对价格的冲击。

**实盘（`live`，预留，尚未实现，配置校验会拒绝）。** 设计上的差别：

| | 模拟盘 | 实盘 |
| --- | --- | --- |
| 订单去向 | 本地 PaperExchange | 交易所 |
| 是否进入市场盘口、影响市场 | 否 | 是 |
| 成交由谁决定 | PaperExchange 根据行情推断 | 交易所撮合 |
| 成交如何得知 | PaperExchange 经 EventSink 推送 AccountEvent | 交易所私有账户流推送，转换成同样的 AccountEvent |
| Shard 是否调用 `PaperExchange::OnMarket` | 是 | 否 |
| 行情里有没有我们的成交 | 没有 | 公开成交里可能包含，但只能以私有账户回报为准 |
| 余额来源 | 配置的初始余额，本地记账 | 交易所账户 |

两种模式共用 Market、Strategy、Executor 的校验和额度、CommandResult、AccountEvent 的语义，以及 Shard 处理账户事件的入口 `OnAccountEvent`。下单命令只返回"是否已派发"，订单是否被接受、是否成交，一律通过后续的账户事件得知；模拟盘也按这个方式工作，所以接入实盘时 Shard 的处理顺序不用改变，只需要把 PaperExchange 换成交易所适配器，并去掉本地撮合这一步。

## 2. 线程模型

| 线程 | 数量 | 运行内容 |
| --- | --- | --- |
| 主线程 | 1 | 解析参数和配置、构造并启动 Runtime；回放时运行 ReplayReader，实时时轮询停止信号；最后等待 Runtime 结束 |
| Shard 工作线程 | min(8, 分片数) | 各自运行一个 `io_context`。第 i 个 Shard 使用第 `i % 线程数` 个 `io_context` |
| SQLite 写线程 | 1 | 从写队列批量取记录，每批最多 64 条，一个事务提交 |
| SQLite 读线程 | 1 | 处理 history 查询，队列最多 32 个请求 |
| Control 线程 | 1 | 依次服务客户端，一个连接处理完再接受下一个 |

一个 Shard 的全部处理都在它所属的 `io_context` 上串行执行：Market 的 Binance 读取协程与盘口过期检查、Shard 的策略定时器、回放输入、Control 投递的请求、账户事件。同一个 `io_context` 上的多个 Shard 之间也串行。

**同步调用链。** Market 更新盘口后同步调用 Shard，Shard 同步调用 PaperExchange、Strategy、Executor，中途不 `co_await`。回调返回前，Market 不读下一条行情。

**跨线程只能投递。** 回放、status、set_strategy、stop 都用 `asio::post` 投递到所属 `io_context`，调用方通过回调或 `std::promise` 等待结果。

**账户事件异步投递。** PaperExchange 产生的 AccountEvent 不在调用栈内处理，而是由 EventSink 再 `post` 一个任务到同一 `io_context`，在当前回调结束后由 `Shard::OnAccountEvent` 处理。这与将来实盘时私有账户流独立到达的方式一致。

## 3. 目录与类型归属

~~~text
hquant/src/
  main.cc                       # 进程入口：gflags、读取配置、启动 Runtime
  config.{h,cc}                 # AppConfig 与 YAML 解析、校验
  runtime.{h,cc}                # 工作线程、Shard 构建与路由、启动和停止
  replay_reader.{h,cc}          # 文件回放输入
  sqlite_history.{h,cc}         # 异步写入、只读查询
  control_server.{h,cc}         # Unix socket、JSON 边界
  base/
    fixed.{h,cc}                # 十进制文本与 uint64 定点数运算
    ids.h                       # ShardId、MarketId、OrderId、MonoTime、InputTime
    net.{h,cc}                  # REST/WebSocket 通用传输
  shard/
    shard.{h,cc}                # Shard；DecisionStrategy、RunDecisionCycle
    market.{h,cc}               # 盘口状态与同步
    market_type.h               # 行情输入、通知、盘口视图等值类型
    order_book.{h,cc}           # 买卖档位
    binance_feed.{h,cc}         # REST 快照、WebSocket 公开数据
    strategy.{h,cc}             # 刷新时间与策略决定
    executor.{h,cc}             # 校验、派发、账户视图
    paper_exchange.{h,cc}       # 活动订单、模拟账户、撮合
~~~

数据类型跟随拥有它的模块：

| 文件 | 类型 |
| --- | --- |
| `market_type.h` | FeedConnected、FeedDisconnected、BookSnapshot、BookDiff、PublicTrade、MarketInput、Side、BookState、BookView、MarketNotice |
| `strategy.h` | ExecutionView、NoAction、CancelOrders、LimitOrderRequest、SubmitOrders、StrategyDecision |
| `executor.h` | CommandResult |
| `paper_exchange.h` | Order、OrderState、OrderReport、Fill、BalanceUpdate、AccountEvent |
| `shard.h` | DecisionStrategy、RunDecisionCycle、TimerTick、ReplayRecord、StrategyPatch、ShardStatus |
| `sqlite_history.h` | HistoryKind、HistoryRecord、HistoryPage、HistoryHealth |

base 只放真正跨模块的数值和 ID。

配置中的金额、数量和比例用 `uint64_t` 定点数，缩放因子 10^9，字段名用 `_nanos` 或 `_ppb` 标明单位。十进制文本解析拒绝超过九位小数和整数溢出，不经过 `double`。盘口保持整数 ticks/lots：网络上的价格和数量先精确转为定点数，再检查能否被 `price_per_tick`、`amount_per_lot` 整除。所有乘除和加减都检查溢出。

## 4. 关键结构

### 4.1 配置

~~~cpp
struct InputConfig {
  InputSource source;                 // Replay 或 BinancePublic
  std::string replay_file;
  std::string rest_host, websocket_host;
  uint16_t rest_port, websocket_port;
  bool tls;
};

struct MarketConfig {
  MarketId id;                        // 交易对，如 BTCUSDT
  std::string base_asset, quote_asset;
  uint64_t price_per_tick_nanos;      // 盘口价格单位
  uint64_t amount_per_lot_nanos;      // 盘口数量单位
  TradingRule rule;                   // 价格步长、数量步长、最小数量、最小金额
  int64_t stale_after_ms;             // 盘口多久没更新算过期
  size_t max_buffered_diffs;          // 等快照时最多缓存多少条增量
};

struct StrategyConfig {
  uint64_t order_amount_nanos;
  uint64_t bid_spread_ppb, ask_spread_ppb;
  int64_t refresh_ms;                 // 必须大于 0
};

enum class TradingMode { Paper, Live };
struct ExecutorConfig {
  TradingMode mode;                   // 必填；Live 当前被拒绝
  std::string account;                // 账户名，各 Shard 不得重复
  std::map<std::string, uint64_t> initial_balances_nanos;
  std::map<std::string, uint64_t> hard_limits_nanos;
  uint64_t maker_fee_rate_ppb;
};

struct ShardConfig {
  ShardId id;                         // [0, 1000)
  MarketConfig market;
  StrategyConfig strategy;
  ExecutorConfig executor;
};

struct AppConfig {
  InputConfig input;
  std::vector<ShardConfig> shards;    // 1 到 1000 个
  std::string sqlite_path;            // STATE_DIR/history.sqlite
  std::string control_socket;         // STATE_DIR/control.sock
};
~~~

一个 Shard 的配置放在一个对象里，避免多组平行数组按下标对齐。

### 4.2 行情值类型

MarketInput 是已解析的行情，只出现在 BinanceFeed、ReplayRecord 和 Market 内部。MarketNotice 是 Market 更新完状态后给 Shard 的通知，不携带盘口；Shard 需要盘口时同步读取 `Market::View()`。

~~~cpp
struct FeedConnected    { uint64_t connection_epoch; };
struct FeedDisconnected { uint64_t connection_epoch; std::string reason; };
struct BookSnapshot { MarketId market; uint64_t connection_epoch; uint64_t last_sequence;
                      std::vector<BookLevel> bids, asks; };
struct BookDiff     { MarketId market; uint64_t connection_epoch;
                      uint64_t first_sequence, last_sequence;
                      std::vector<BookLevel> bids, asks; };   // 数量 0 表示删除该档
struct PublicTrade  { MarketId market; uint64_t connection_epoch; uint64_t trade_id;
                      uint64_t price_ticks, quantity_lots; Side aggressor; };
using MarketInput = std::variant<FeedConnected, BookSnapshot, BookDiff,
                                 PublicTrade, FeedDisconnected>;

struct BookView {
  BookState state;                          // Syncing 或 Live
  std::optional<uint64_t> last_sequence;
  std::optional<BookLevel> best_bid, best_ask;
  std::optional<BookLevel> last_trade;
  std::optional<MonoTime> last_book_at;     // 用于过期检测
};

struct MarketNotice {
  enum class Kind { BookChanged, PublicTrade, BookUnavailable } kind;
  std::optional<PublicTrade> trade;         // 仅 PublicTrade 通知有值
};
~~~

### 4.3 OrderBook 与 Market

OrderBook 是交易所公开盘口在本地的副本，里面只有其他人的挂单，没有我们的订单（见 1.1）。它只由 Market 修改。

~~~cpp
class OrderBook {
 public:
  void Reset();                                           // 清空两侧
  bool ApplySnapshot(span<const BookLevel> bids, span<const BookLevel> asks);  // 清空后整体写入
  bool ApplyDiff(span<const BookLevel> bids, span<const BookLevel> asks);      // 逐档覆盖，数量 0 删档
  bool IsTradable() const;                                // 两侧非空且买一 < 卖一
  std::optional<BookLevel> BestBid() const, BestAsk() const;
 private:
  absl::btree_map<uint64_t, uint64_t> bids_;              // 价格 tick → 数量 lot，买一在末尾
  absl::btree_map<uint64_t, uint64_t> asks_;              // 价格 tick → 数量 lot，卖一在开头
};
~~~

两个 Apply 都先拒绝价格为 0 的档位（快照还拒绝数量为 0），应用后返回 `IsTradable()`。返回 false 时，Market 认为盘口已与交易所不一致，进入 Syncing 并重连。增量给的是每档的绝对数量，所以同一条增量重复应用结果不变。

~~~cpp
class Market {
 public:
  Market(asio::io_context&, const MarketConfig&, const InputConfig&,
         std::function<void(const MarketNotice&)> notify);
  awaitable<void> RunBinance();                     // 实时：启动 BinanceFeed 和过期检查
  void OnReplayInput(const MarketInput&, MonoTime);  // 回放：同一套处理
  bool CheckStale(MonoTime now);                    // 过期则进入 Syncing，返回 true
  const BookView& View() const;
  void Stop();
 private:
  void HandleInput(const MarketInput&, MonoTime);   // 按类型分发到 OnConnected/OnSnapshot/OnDiff/OnTrade/OnDisconnected
  void ApplyBuffered(MonoTime);                     // 有快照后补上缓存的增量
  void EnterSyncing(std::string reason, bool reconnect);
  void RefreshView();                               // OrderBook 买一卖一 → BookView

  std::function<void(const MarketNotice&)> notify_; // Shard::OnMarket
  OrderBook book_;
  BookView view_;                                   // 对外只读
  std::deque<BookDiff> buffered_diffs_;             // 等快照时缓存的增量
  std::optional<uint64_t> sequence_;                // 已应用到的序号
  uint64_t connection_epoch_;                       // 当前连接代次
  bool snapshot_ready_, needs_resync_;
  asio::steady_timer stale_timer_;
  BinanceFeed feed_;
};
~~~

Market 的对外边界只有两样：通知回调 `notify_`，和只读的 `View()`。它不调用 Strategy、Executor 或 PaperExchange。状态转换规则见 6.1。

### 4.4 策略接口与决策循环

~~~cpp
struct ExecutionView {
  std::vector<OrderId> active_order_ids;
  uint64_t base_available_nanos, quote_available_nanos;
  uint64_t base_budget_nanos, quote_budget_nanos;   // 受 hard_limits 约束的可下单额度
  uint64_t maker_fee_rate_ppb;
  uint64_t price_per_tick_nanos, price_increment_nanos, base_increment_nanos;
  uint64_t min_base_amount_nanos, min_order_value_nanos;
};

struct NoAction {};
struct CancelOrders { std::vector<OrderId> ids; };
struct LimitOrderRequest { Side side; uint64_t price_nanos; uint64_t quantity_nanos; };
struct SubmitOrders { std::vector<LimitOrderRequest> orders; };
using StrategyDecision = std::variant<NoAction, CancelOrders, SubmitOrders>;

// 策略的固定接口：输入盘口视图、账户视图和当前时间，输出一个决定。
template <typename S>
concept DecisionStrategy = requires(S& s, const BookView& book,
                                    const ExecutionView& execution, MonoTime now) {
  { s.Decide(book, execution, now) } -> std::same_as<absl::StatusOr<StrategyDecision>>;
};

// 决策一次并派发；若是撤单且盘口仍 Live，用撤单后的视图再决策一次。
template <DecisionStrategy S, typename Record>
absl::StatusOr<uint64_t> RunDecisionCycle(S& strategy, Executor& executor,
                                          const BookView& book, MonoTime now,
                                          Record&& record);
~~~

策略不使用基类，接口由 `DecisionStrategy` concept 约束；`shard.cc` 用 `static_assert(DecisionStrategy<Strategy>)` 在编译期检查。1.0 的 Strategy 是具体类，以中间价加减价差生成买卖两侧的限价单，除配置外只保存 `next_refresh_at`。命名用 SubmitOrders 而不是 quote：做市里的 quote 指挂单报价，交易对里又指计价资产，容易混淆。

### 4.5 Executor

~~~cpp
class Executor {
 public:
  Executor(const MarketConfig&, const ExecutorConfig&, PaperExchange& exchange);
  StatusOr<std::vector<CommandResult>> Dispatch(const StrategyDecision&, const BookView&);
  StatusOr<std::vector<CommandResult>> CancelAll();   // set_strategy 和停止时使用
  ExecutionView View() const;                         // 给 Strategy 的账户视图
 private:
  uint64_t Budget(const std::string& asset) const;    // 受 hard_limits 约束的额度
  std::string ValidateOrder(const LimitOrderRequest&, const BookView&) const;
  MarketConfig market_;
  ExecutorConfig config_;
  PaperExchange& exchange_;                           // 不拥有；Shard 持有
};

struct CommandResult {                      // 表示本地派发结果
  enum class Kind { Submit, Cancel } kind;
  OrderId order_id;
  Side side;
  uint64_t price_nanos, quantity_nanos;
  bool dispatched;                          // 已送到交易所；不表示被接受或成交
  std::string reason;                       // 本地拒绝原因
};
~~~

Executor 是我方的执行者：校验交易规则和资金上限，把决定派发给交易所。它不保存订单、冻结额或余额，全部从 PaperExchange 读取；也不做撮合。规则见 5.5。

### 4.6 PaperExchange 与账户事件

~~~cpp
class PaperExchange {
 public:
  using EventSink = std::function<void(AccountEvent)>;
  PaperExchange(const MarketConfig&, const ExecutorConfig&, EventSink);
  StatusOr<OrderId> Place(const LimitOrderRequest&);    // 接受或拒绝都经 EventSink 回报
  Status Cancel(const std::vector<OrderId>&);
  Status CancelAll();
  Status OnMarket(const MarketNotice&, const BookView&); // 分发到下面两个撮合入口
  Status OnBookBbo(const BookView&);                    // 用买一卖一撮合
  Status OnPublicTrade(const PublicTrade&);             // 用公开成交撮合
  const std::map<uint64_t, Order>& ActiveOrders() const;
  uint64_t Total(const std::string& asset) const;
  uint64_t Available(const std::string& asset) const;   // 总额 − 活动订单占用
 private:
  Status FillMatches(const std::vector<Match>&);        // 原子结算一批成交
  std::map<std::string, uint64_t> totals_;              // 资产 → 总额
  std::map<uint64_t, Order> active_orders_;             // 我们的活动订单
  EventSink event_sink_;                                // Shard 传入：post 到所属 io_context
  uint64_t next_order_id_, next_trade_id_;
};

struct Order {
  OrderId id; Side side;
  uint64_t price_nanos, quantity_nanos;     // 提交后不变
  uint64_t remaining_nanos;                 // 未成交数量
  uint64_t reserved_nanos;                  // 剩余部分占用的资金
};
enum class OrderState { Rejected, Accepted, PartiallyFilled, Filled, Cancelled };
struct OrderReport   { Order order; OrderState state; std::string reason; };
struct Fill          { OrderId order_id; Side side; uint64_t trade_id;
                       uint64_t price_nanos, quantity_nanos, gross_nanos, fee_nanos; };
struct BalanceUpdate { std::string asset; uint64_t total_nanos, available_nanos; };
using AccountEvent = std::variant<OrderReport, Fill, BalanceUpdate>;
~~~

PaperExchange 扮演交易所：它是我们的活动订单和余额的唯一保存处，可用额由总额减去活动订单占用推导，不另存冻结表。所有状态变化都只通过 EventSink 报出，命令的返回值只表示调用是否成功。撮合和结算规则见 5.6。

公开成交（PublicTrade）是市场数据，本账户的成交是 Fill，两者不混用。

### 4.7 Shard

~~~cpp
class Shard {
 public:
  Shard(asio::io_context& io, const ShardConfig&, const InputConfig&,
        SqliteHistory&, std::string run_id);
  absl::Status Start();                     // 实时：启动行情协程和策略定时器
  void PostReplay(ReplayRecord, std::function<void(absl::Status)> done);
  void PostStatus(std::function<void(ShardStatus)> done);
  void PostStrategyUpdate(uint64_t expected_version, StrategyPatch,
                          std::function<void(absl::StatusOr<StrategyUpdateResult>)> done);
  void RequestStop();
 private:
  void OnMarket(const MarketNotice&);       // Market 通知入口
  void OnStrategyTimer();                   // 定时刷新入口
  void OnAccountEvent(const AccountEvent&); // 账户事件入口：写历史
  absl::StatusOr<uint64_t> RunStrategyDecision();  // 取时间后调用 RunDecisionCycle

  asio::io_context& io_;                    // Runtime 持有
  Market market_;
  Strategy strategy_;
  PaperExchange exchange_;                  // 必须先于 executor_ 构造
  Executor executor_;                       // 引用 exchange_
  asio::steady_timer strategy_timer_;
};
~~~

### 4.8 类型与所有者

| 类型 | 关键字段 | 所有者 |
| --- | --- | --- |
| BookSnapshot / BookDiff | 连接代次、序号、买卖档 | BinanceFeed 或 ReplayReader 生成，Market 消费 |
| PublicTrade | 连接代次、成交 ID、价格、数量、主动方 | 同上 |
| OrderBook | 交易所公开盘口的本地副本：两侧价格 → 数量；不含我们的订单 | Market |
| BookView | 同步状态、序号、买一卖一、最近成交、盘口更新时间；只在当前事件内借用 | Market |
| ExecutionView | 活动订单、可用余额、可下单额度、交易规则 | Executor 从 PaperExchange 读取后生成 |
| StrategyDecision | 无动作 / 待撤订单 ID / 待下单的方向、价格、数量 | Strategy 输出 |
| CommandResult | 本地命令是否派发及拒绝原因 | Executor 生成 |
| Order | ID、方向、价格、数量、剩余量、占用 | PaperExchange |
| AccountEvent | 订单报告 / 成交 / 余额 | PaperExchange 产生，经 EventSink 交给 Shard |
| ShardStatus | 运行与盘口状态、计数、活动订单数、余额、最近错误 | Shard 生成 |
| HistoryRecord | run、shard、分片内序号、时间、命令或回报 | Shard 生成，SqliteHistory 保存 |

## 5. 一次事件的处理顺序

### 5.1 行情触发

~~~text
BinanceFeed 或回放把一条 MarketInput 交给 Market::HandleInput
  → 按类型分发，在这里更新 OrderBook：
      快照：OnSnapshot（回放为 OnReplaySnapshot）→ OrderBook::ApplySnapshot，清空后整体写入
      增量：OnDiff，或缓存后由 ApplyBuffered 补上 → OrderBook::ApplyDiff，逐档覆盖，数量 0 删档
      成交：OnTrade，只记录最近成交，不改 OrderBook
      连接 / 断线：OnConnected / OnDisconnected → EnterSyncing → OrderBook::Reset
  → Market::RefreshView：把 OrderBook 的买一卖一复制到 BookView
  （或者 CheckStale 判定过期 → EnterSyncing → OrderBook::Reset）
  → Market 同步调用 Shard::OnMarket(MarketNotice)
  → [仅 paper] PaperExchange::OnMarket(notice, Market::View())，撮合已有挂单
      └ 账户事件经 EventSink 投递，暂不处理
  → RunStrategyDecision → RunDecisionCycle
      → Strategy::Decide(BookView, Executor::View(), now)
      → Executor::Dispatch(decision)，返回 CommandResult
      → 记录本批命令结果
      → 若刚才撤单、盘口仍 Live：再 Decide 一次，再 Dispatch，再记录
  → 本次回调结束
  → io_context 依次执行已投递的 Shard::OnAccountEvent，写账户事件历史
~~~

OrderBook 只由 Market 修改，而且在通知 Shard 之前已经改完。Shard、Strategy、PaperExchange 只读 BookView 里的买一卖一，不接触 OrderBook 本身。

| MarketNotice | PaperExchange（paper 模式） | Strategy |
| --- | --- | --- |
| BookChanged | 用更新后的买一卖一撮合 | 根据最新盘口决定是否刷新 |
| PublicTrade | 用本次公开成交撮合；盘口不是 Live 也处理 | 盘口不是 Live 时只撤单不下单 |
| BookUnavailable | 不撮合 | 撤掉全部挂单，不下新单 |

### 5.2 定时器触发

实时模式下 `strategy_timer_` 每 `refresh_ms` 触发一次 `OnStrategyTimer`，直接进入 RunDecisionCycle，不撮合。回放模式没有墙上时间定时器，由回放文件中的 `timer` 记录驱动：先 `Market::CheckStale(回放时间)`，盘口刚变为过期时已经通过 BookUnavailable 走了 5.1，不再触发第二次决策；否则调用 `OnStrategyTimer`。

### 5.3 决策循环

RunDecisionCycle 是 `shard.h` 中的自由函数。一次 Decide 只能返回一种动作，而撤单会释放资金、改变可下单额度，所以"撤旧单、挂新单"分两次决定：

1. 用当前视图 Decide，Dispatch，立即把命令结果交给 `record` 写历史。
2. 如果第一次不是撤单，或盘口不是 Live，结束。
3. 检查活动订单已清空，否则返回内部错误，Shard 停止。
4. 用撤单后的视图再 Decide。若又要求撤单，返回内部错误。
5. Dispatch 第二次的决定并记录。

最多两次 Decide，不循环。每批命令派发后立即记录，第二批出错时第一批的记录不会丢失。返回两批命令的总数，set_strategy 用它回报动作数。

`now` 由 Shard 提供：回放时为当前记录的 `at_us`，实时为 `steady_clock`。

### 5.4 Strategy::Decide

Strategy 除配置外只保存 `next_refresh_at`，初始为空。是否有活动订单由 ExecutionView 提供，Strategy 不保存订单阶段。

| Decide 时的条件 | 返回值与时间更新 |
| --- | --- |
| 盘口不是 Live | 清空 next_refresh_at；有活动订单则返回撤全部订单，否则 NoAction |
| 盘口 Live，未到 next_refresh_at | NoAction |
| 盘口 Live，已到刷新时间，仍有活动订单 | 返回撤全部订单；next_refresh_at 不变 |
| 盘口 Live，已到刷新时间或为空，没有活动订单 | 按盘口和额度返回 SubmitOrders，没有有效订单则 NoAction；两种情况都把 next_refresh_at 设为 now + refresh_ms |

下单价格：中间价 = 买一 + (卖一 − 买一) / 2；买价 = 中间价 × (1 − bid_spread) 向下取整到价格步长；卖价 = 中间价 × (1 + ask_spread) 向上取整到价格步长。数量为 order_amount 向下取整到数量步长。买单要求金额加手续费不超过 quote 额度，卖单要求数量不超过 base 额度，且金额都不低于最小金额；不满足的一侧不下单。

刷新时间前若挂单全部成交，等到刷新时间再下单；新单被拒绝也等下一次刷新。

### 5.5 Executor::Dispatch

| 决定 | 处理 |
| --- | --- |
| NoAction | 返回空 |
| CancelOrders | 逐张检查：订单已不存在（例如刚成交）则记 `dispatched=false`、原因 `cancel order missing`；存在则 `PaperExchange::Cancel({id})` |
| SubmitOrders | 逐张校验，通过才 `PaperExchange::Place`；未通过记 `dispatched=false` 和原因，不调用交易所 |

下单校验依次为：盘口是 Live；价格、数量非零；价格和数量对齐步长；数量不低于最小数量；金额不低于最小金额；买单金额加手续费不超过 quote 额度，卖单数量不超过 base 额度。每张单都用 PaperExchange 的最新余额计算额度，前一单派发后下一单的额度随之减少。

可下单额度对每种资产计算：

~~~text
已用 = max(0, 初始总额 − 当前总额)
冻结 = 当前总额 − 当前可用额
额度 = min(当前可用额, max(0, hard_limit − 已用 − 冻结))
~~~

即本资产从开始至今的净消耗加上挂单冻结，不能超过 hard_limit。Executor 不另存订单、冻结表或余额，全部从 PaperExchange 读取。

业务上的拒绝（额度不足、订单已不存在）只写在 CommandResult 里；只有 PaperExchange 调用本身出错才返回错误，Shard 随即停止。

### 5.6 PaperExchange

PaperExchange 是活动订单和余额的唯一保存处，可用额由总额减去活动订单的 `reserved_nanos` 推导。每次 Place、Cancel、OnBookBbo、OnPublicTrade 都原子完成：先在副本上计算订单和余额，全部成功才提交，然后按顺序把事件交给 EventSink；中途出错则内部状态不变。

**Place**：买单占用 = 金额（向上取整）+ 手续费（向上取整）；手续费非零时，再为每个最小数量单位多留 1 nano，覆盖多次部分成交各自向上取整的手续费。卖单占用 = 数量。可用额不足时，订单号照常分配，推送 `OrderReport(Rejected)`，不冻结资金；否则加入活动订单，推送 `OrderReport(Accepted)` 和一条余额。

**Cancel**：订单不存在或重复时返回错误。成功后移除订单、释放剩余占用，推送 `OrderReport(Cancelled)`，再推送 base 和 quote 余额。

**OnBookBbo**（BookChanged）：只看买一和卖一。

- 买单价格 ≥ 卖一：按卖一价成交，数量取订单剩余量与卖一可见数量的较小者。
- 卖单价格 ≤ 买一：按买一价成交，数量取订单剩余量与买一可见数量的较小者。
- 同一档的可见数量按订单 ID 顺序分给多张订单，用完为止。可见数量先向下取整到数量步长。

**OnPublicTrade**（PublicTrade）：只撮合与主动方方向相反的挂单。

- 买单价格 ≥ 成交价，或卖单价格 ≤ 成交价：按挂单价成交。
- 数量取订单剩余量与本笔公开成交剩余数量的较小者，按订单 ID 顺序分配。

两种撮合互补：我方挂单只存在于本地，真实市场中被扫过的价位只能从公开成交看到；真实卖一落到我方买价以下而没有产生成交的情况，只能从盘口看到。

每次部分成交产生一条 Fill 和一条带剩余量的 `OrderReport(PartiallyFilled)`；剩余量归零时为 `OrderReport(Filled)` 并移除订单。一次撮合的全部订单处理完后，追加 base 和 quote 两条余额。trade_id 在本 PaperExchange 内递增，不重复。提交前检查每种资产的冻结额不超过总额，否则视为内部错误。

公开行情只能支撑模拟成交假设；实盘成交必须由交易所私有账户事件确认。

### 5.7 账户事件与历史顺序

EventSink 把每个 AccountEvent `post` 到 Shard 的 `io_context`。`Shard::OnAccountEvent` 只做一件事：转换为 HistoryRecord 并放入写队列，不修改任何状态。因此同一条输入产生的历史中，命令记录在前，账户事件在后。

回放时，PostReplay 在处理完输入后再 `post` 完成回调，排在这条输入产生的全部账户事件之后，所以读取器读下一条记录前，本条的账户事件已经全部写入队列。回放结果因此是确定的。

## 6. 状态机

本版只有三处跨事件的生命周期：Market 盘口、单张模拟订单、Shard。Strategy 的刷新时间是定时门控，规则见 5.4。

### 6.1 Market 盘口同步

Syncing 表示盘口不能用于新下单，Live 表示快照和增量连续、买一卖一有效。重连、等待快照、追增量都属于 Syncing，用连接代次、`snapshot_ready`、增量缓冲区分，不另设状态。`needs_resync` 表示当前连接已不可信，在下一次 FeedConnected 之前忽略该连接的快照和增量。

~~~mermaid
stateDiagram-v2
    [*] --> Syncing: 启动
    Syncing --> Syncing: 等输入或重新同步
    Syncing --> Live: 实时：快照与缓存增量接上；回放：完整快照有效
    Live --> Live: 连续增量；回放：序号为当前+1 的完整快照
    Live --> Syncing: 新连接、断线、断档、过期或无效盘口
    Syncing --> [*]: Shard 停止
    Live --> [*]: Shard 停止
~~~

| 转换 | 触发条件 | Market 动作与通知 |
| --- | --- | --- |
| Syncing → Syncing | 新连接；快照或增量尚不足以接上；缓冲满、断档或无效 | 新连接清空旧数据并记下新 connection_epoch。当前连接的增量先缓存；有快照后丢弃比快照旧的增量，检查首条满足 first_sequence ≤ S+1 ≤ last_sequence 并逐条连续。出错清空并要求重连。不通知 Shard |
| Syncing → Live | 实时：快照到达且至少一条缓存增量接上；回放：完整快照有效 | 更新序号与盘口时间，发一次 BookChanged |
| Live → Live | 当前连接的增量连续且应用后盘口有效；回放：完整快照序号为当前 +1 | 更新盘口、序号与盘口时间，发 BookChanged。已覆盖的旧增量直接忽略 |
| Live → Syncing | 当前连接断线或换代、增量缺口、盘口过期、盘口交叉或某侧为空 | 清空盘口，发一次 BookUnavailable；实时模式由 BinanceFeed 断开重连 |

要点：

- 实时模式下，单有快照不进入 Live，必须至少接上一条增量；Live 时到达的快照忽略。
- 回放快照走 `OnReplaySnapshot`，直接替换整个盘口并进入 Live；已有序号时要求快照序号为当前 +1。
- 增量的价位给的是绝对数量，与已应用部分重叠的区间重复应用结果不变。
- 旧 connection_epoch 的输入、重复的 FeedConnected 直接忽略。
- PublicTrade 不改变盘口状态或序号，不延长盘口有效期；交易对不符或数据非法时只记录错误，不触发重连。
- `Market::Stop` 由 Shard 停止流程调用，不是第三个盘口状态。

### 6.2 单张模拟订单

~~~mermaid
stateDiagram-v2
    [*] --> Active: OrderAccepted
    [*] --> Rejected: OrderRejected
    Active --> Active: Fill + PartiallyFilled，剩余量减少
    Active --> Filled: Fill + Filled，剩余量归零
    Active --> Cancelled: CancelOrders、set_strategy 或 Shard 停止
~~~

| 转换 | 条件与效果 |
| --- | --- |
| 进入 Active | PaperExchange 接受 Place，加入活动订单，产生 OrderAccepted 和余额事件 |
| Active → Active | 撮合命中部分数量，更新余额与剩余占用，产生 Fill、PartiallyFilled |
| Active → Filled | 剩余量归零，移除活动订单，产生 Fill、Filled |
| Active → Cancelled | Cancel 移除活动订单、释放剩余占用，产生 Cancelled 和余额事件 |
| 未进入 Active | Executor 本地拒绝：只有未派发的 CommandResult；PaperExchange 拒绝：产生 OrderRejected，不冻结资金 |

结束只是从活动订单中移除并写历史，不另存终结状态。

### 6.3 Shard 生命周期

运行阶段由 `stop_requested` 和工作线程是否结束推导，不另设枚举。

~~~mermaid
stateDiagram-v2
    [*] --> Running: Shard Start()
    Running --> Stopping: Control stop、信号、回放结束或内部错误
    Stopping --> Stopping: 重复停止请求
    Stopping --> Exited: 所属 io_context 没有剩余工作，工作线程结束
    Exited --> [*]: Runtime join 后关闭 SQLite 与 Control
~~~

| 阶段 | 允许的处理 |
| --- | --- |
| Running | 按第 5 节处理行情、定时器、回放和 Control 请求 |
| Stopping | `stop_requested` 已置位；不再调用 Strategy；StopOnThread 停止 Market 和定时器、撤掉全部活动订单并记录；新的回放、status、set_strategy 请求直接返回停止结果 |
| Exited | 工作线程已结束；Runtime 刷新并关闭 SQLite，再关闭 Control socket |

内部错误（撮合失败、决策循环出错等）走 StopFromError：记录错误、置 `stop_requested`、立即执行 StopOnThread；回放的完成回调会带回这个错误。重复停止不再撤单。

## 7. 时序

### 7.1 启动

~~~mermaid
sequenceDiagram
    participant Main as main
    participant R as Runtime
    participant H as SqliteHistory
    participant W as 工作线程
    participant S as Shard
    participant C as ControlServer
    Main->>Main: 解析 gflags，LoadConfig，应用策略覆盖，创建 state_dir
    Main->>R: 构造 Runtime(AppConfig)
    R->>R: 创建 min(8, N) 个 io_context，按 i % 线程数构造 Shard
    Main->>R: Start()
    R->>H: Start()：打开写/读连接，建表，启动写线程和读线程
    R->>W: 每个 io_context 启动一个工作线程
    loop 每个 Shard
        R->>S: Start()
        opt 实时模式
            S->>S: co_spawn Market::RunBinance；启动策略定时器
        end
    end
    R->>C: Start()：绑定 control.sock（已存在则失败），启动服务线程
    alt 回放
        Main->>R: RunReplay()，见 7.4
    else 实时
        Main->>Main: 每 100ms 检查信号或 Control stop
    end
~~~

任一步失败，Runtime 先停止已启动的部分再返回错误。

### 7.2 Binance 盘口同步

~~~mermaid
sequenceDiagram
    participant F as BinanceFeed
    participant WS as Binance WebSocket
    participant REST as Binance REST
    participant M as Market
    F->>F: connection_epoch + 1
    F->>M: FeedConnected(epoch)：清空盘口，进入 Syncing
    F->>WS: 连接 <symbol>@depth / <symbol>@trade
    loop 直到收到第一条 depthUpdate
        WS-->>F: 公开成交
        F->>M: PublicTrade
    end
    WS-->>F: 第一条 depthUpdate
    F->>M: BookDiff：缓存
    loop 最多 3 次
        F->>REST: GET /api/v3/depth?limit=1000
        alt 429 / 418
            F->>F: 记下退避时间，按故障处理
        else 快照序号 + 1 < 首条增量 first_sequence
            F->>F: 快照太旧，重新请求
        else
            F->>M: BookSnapshot：重建盘口，用缓存增量对齐
        end
    end
    loop 正常运行
        WS-->>F: depthUpdate / trade
        F->>M: BookDiff / PublicTrade
        opt Market 要求重新同步
            F->>F: 按故障处理
        end
    end
    Note over F,M: 故障：断开 WebSocket，向 Market 发 FeedDisconnected；<br/>等待 max(退避, 服务端要求) 后重新开始，退避从 250ms 翻倍到 30s
~~~

Market 处理每条输入后，把是否需要重新同步返回给 BinanceFeed。读取超时为 30 秒，连接和 REST 请求超时为 5 秒。盘口过期由 Market 自己的定时检查发现，走 BookUnavailable，并让 BinanceFeed 断开重连。

### 7.3 实时行情处理

~~~mermaid
sequenceDiagram
    participant F as BinanceFeed
    participant M as Market
    participant S as Shard
    participant X as PaperExchange
    participant P as Strategy
    participant E as Executor
    F->>M: MarketInput（同一调用栈）
    M->>M: HandleInput：快照/增量写入 OrderBook，成交只记最近成交
    M->>M: RefreshView：买一卖一复制到 BookView
    alt 断档、断线、过期或无效盘口
        M->>M: 清空盘口，进入 Syncing
        opt 此前是 Live
            M->>S: OnMarket(BookUnavailable)
        end
    else 等待快照
        M->>M: 缓存增量，不通知
    else 盘口已更新或收到公开成交
        M->>S: OnMarket(BookChanged / PublicTrade)
    end
    opt Shard 收到通知
        S->>M: View()
        opt paper 模式，且不是 BookUnavailable
            S->>X: OnMarket(notice, BookView)
            X->>X: 撮合；原子更新订单和余额
            X-->>S: EventSink 投递账户事件（稍后处理）
        end
        S->>P: Decide(BookView, Executor::View(), now)
        P-->>S: NoAction / CancelOrders / SubmitOrders
        opt 有动作
            S->>E: Dispatch(decision)
            E->>X: Cancel 或校验后 Place
            X-->>S: EventSink 投递账户事件（稍后处理）
            E-->>S: CommandResult，写历史
        end
        opt 刚才撤单，活动订单已清空，盘口仍 Live
            S->>P: 再次 Decide(撤单后的视图)
            P-->>S: NoAction / SubmitOrders
            opt SubmitOrders
                S->>E: Dispatch
                E->>X: 校验后 Place
                E-->>S: CommandResult，写历史
            end
        end
    end
    M-->>F: 是否需要重新同步
    Note over S: 回调返回后，io_context 依次执行 OnAccountEvent，写账户事件历史
~~~

Market 不调用 Strategy、Executor 或 PaperExchange。

### 7.4 文件回放

~~~mermaid
sequenceDiagram
    participant F as ReplayReader（主线程）
    participant R as Runtime
    participant S as Shard（工作线程）
    participant M as Market
    F->>F: 解析一条记录，检查时间严格递增，确定目标 Shard
    F->>R: sink(ReplayRecord)
    R->>S: PostReplay(record, done)
    R->>R: 等待 done
    S->>S: 检查目标与时间顺序；虚拟时钟 = at_us
    alt 行情输入（subscribe / snapshot / diff / public_trade / disconnect）
        S->>M: OnReplayInput(MarketInput, now)
        opt 产生通知
            M->>S: OnMarket(notice)，同 7.3
        end
    else timer
        S->>M: CheckStale(now)
        alt 盘口刚过期
            M->>S: OnMarket(BookUnavailable)
        else
            S->>S: OnStrategyTimer()
        end
    end
    S->>S: post 完成回调（排在本条产生的账户事件之后）
    S->>S: 依次处理账户事件
    S-->>R: done(Ok / 停止 / 致命错误)
    R-->>F: 返回状态
    opt 正常
        F->>F: 读取下一条
    end
~~~

回放文件格式：`schema_version` 与 `inputs` 数组；每条有 `at_us`、`ordinal`、`kind`，按 (at_us, ordinal) 严格递增。记录用 `shard` 或 `market` 指定目标；只有一个 Shard 时可省略。没有目标的 `timer` 发给所有 Shard。`public_trade` 省略 `connection_id` 时沿用该 Shard 最近一次 `subscribe` 的连接代次。

ReplayReader 不直接修改盘口、订单或账户。文件读完后，主线程发起停止。回放模式下历史写队列满时等待而不丢弃，见第 8 节。

### 7.5 定时刷新、撤单和重新下单

~~~mermaid
sequenceDiagram
    participant T as 策略定时器 / 回放 timer
    participant S as Shard
    participant P as Strategy
    participant E as Executor
    participant X as PaperExchange
    T->>S: OnStrategyTimer
    S->>P: Decide(最新盘口、订单、余额)
    alt 撤单
        P-->>S: CancelOrders
        S->>E: Dispatch(CancelOrders)
        E->>X: Cancel，释放占用
        X-->>S: 投递 Cancelled 与余额事件
        E-->>S: CommandResult，写历史
        opt 已无活动订单，盘口仍 Live
            S->>P: 再次 Decide(撤单后的视图)
            P-->>S: SubmitOrders / NoAction
        end
    else 下单
        P-->>S: SubmitOrders
    else 未到时间，或盘口不可用且无挂单
        P-->>S: NoAction
    end
    opt 当前决定为 SubmitOrders
        S->>E: Dispatch(SubmitOrders)
        E->>E: 逐单校验规则与额度
        E->>X: Place：冻结并登记，或拒绝
        X-->>S: 投递 Accepted / Rejected 与余额事件
        E-->>S: CommandResult，写历史
    end
    Note over T,S: 实时模式下定时器随后重新安排下一次触发
~~~

定时器只触发决策，不撮合。每次触发最多撤单一次、下单一次。

### 7.6 status 与 history

~~~mermaid
sequenceDiagram
    participant C as Control 客户端
    participant A as ControlServer / Runtime
    participant S as Shard
    participant D as SQLite 读线程
    alt status
        C->>A: {"op":"status"}
        loop 每个 Shard，依次
            A->>S: PostStatus
            S-->>A: ShardStatus（在所属线程当前任务结束后生成）
        end
        A-->>C: 各 Shard 状态 + 写队列健康状态
    else history
        C->>A: {"op":"history","limit","cursor"}
        A->>D: 排队只读查询
        D-->>A: 记录 + next_cursor
        A-->>C: JSON 历史页
    end
~~~

status 不进入交易决策；已停止的 Shard 直接返回只含停止标志的状态。history 只走 SQLite 读连接，不进入任何 Shard 的事件循环。

### 7.7 set_strategy

~~~mermaid
sequenceDiagram
    participant C as Control 客户端
    participant A as ControlServer / Runtime
    participant S as 目标 Shard
    participant P as Strategy
    participant E as Executor
    C->>A: set_strategy(shard_id, expected_version, patch)
    A->>A: 解析允许字段与类型，十进制值须为字符串
    A->>S: PostStrategyUpdate，等待结果
    S->>S: 校验版本；复制当前配置，应用 patch，校验完整候选
    alt 版本不符或参数非法
        S-->>A: 错误；配置和版本不变
    else 提交
        S->>P: 替换配置，清空 next_refresh_at
        S->>S: 版本 + 1；实时模式重新安排定时器
        S->>E: CancelAll，记录命令
        S->>P: RunDecisionCycle(新参数)
        opt 有新单
            S->>E: Dispatch(SubmitOrders)
        end
        S-->>A: 新版本 + 本次命令数
    end
    A-->>C: 同一 request_id 的响应
~~~

`expected_version` 初始为 1，每次成功提交加 1。多个字段作为一个 patch 整体生效，任一无效则全部不生效。只允许修改 `order_amount`、`bid_spread`、`ask_spread`、`refresh_ms`；盘口、交易规则、账户、费率和存储配置运行中不变。更新与该 Shard 的行情事件串行，一次决策不会混用新旧参数。

### 7.8 stop

~~~mermaid
sequenceDiagram
    participant C as Control 客户端 / 信号 / 回放结束
    participant Main as main
    participant R as Runtime
    participant S as Shard
    participant E as Executor
    participant H as SqliteHistory
    participant CS as ControlServer
    C->>R: RequestStop（Control stop 立即回复 accepted）
    loop 每个 Shard
        R->>S: RequestStop：置 stop_requested，post StopOnThread
    end
    S->>S: Market::Stop，取消策略定时器
    S->>E: CancelAll
    E-->>S: CommandResult，写历史；账户事件随后处理
    S->>S: 释放 work_guard
    Note over S: io_context 处理完剩余任务后 run() 返回
    Main->>R: Wait()
    R->>R: join 全部工作线程
    R->>H: FlushAndStop：写完队列，关闭读写连接
    R->>CS: Stop：关闭并删除 control.sock
~~~

`accepted` 只表示停止请求已受理；历史落盘在 Shard 全部退出后完成。`stop_requested` 是单向标志，重复 stop 不再撤单。

## 8. 历史落库

| 项 | 规则 |
| --- | --- |
| 写入方 | 只有 Shard（`Record`），在所属线程调用 `TryPush` |
| 记录内容 | 命令结果（Submit/Cancel 命令）和账户事件（订单报告、成交、余额）；每条带 run_id、shard、分片内递增序号、时间 |
| 时间 | 回放用记录的 `at_us`；实时用系统时间 |
| 队列容量 | max(8192, 分片数 × 64) |
| 队列满：回放 | 等待写线程腾出空间，不丢记录，保证回放历史完整 |
| 队列满：实时 | 不阻塞交易线程，丢弃该记录，`dropped` 计数加一，并把连续丢失的序号段写入 `history_gaps` 表 |
| 写线程 | 每批最多 64 条记录加待写的缺口，一个事务提交；写失败后停止写入并记录错误，status 中可见 |
| 存储格式 | `uint64` 数值以文本存储，避免 SQLite 有符号整数溢出；同一 (run_id, shard, 序号) 唯一 |
| 查询 | 读线程独立连接；按自增 id 分页，`limit` 在 [1, 500]，`cursor` 为上一页的 `next_cursor` |

## 9. 对外接口

### 9.1 启动参数

由 gflags 在 `main.cc` 解析，其他模块不读全局 flags。

| 参数 | 说明 |
| --- | --- |
| `--config` | 必填，YAML 配置路径 |
| `--state_dir` | 必填，存放 `history.sqlite` 和 `control.sock` |
| `--strategy_shard` | 指定下列覆盖值作用的 Shard |
| `--order_amount`、`--bid_spread`、`--ask_spread`、`--refresh_ms` | 覆盖该 Shard 的策略参数；必须与 `--strategy_shard` 一起使用 |

覆盖值在构造 Runtime 前应用到 AppConfig，并重新整体校验。

### 9.2 YAML 配置

~~~yaml
schema_version: 1
input:
  source: replay                 # replay | binance_public
  replay_file: hquant/examples/replay_market.json
shards:
  - id: 0
    market:
      symbol: BTCUSDT
      base_asset: BTC
      quote_asset: USDT
      price_per_tick: "0.01"
      amount_per_lot: "0.001"
      stale_after_ms: 30000
      max_buffered_diffs: 1024
      trading_rule:
        price_increment: "0.01"
        base_increment: "0.001"
        min_base_amount: "0.001"
        min_order_value: "5"
    strategy:
      order_amount: "0.1"
      bid_spread: "0.001"
      ask_spread: "0.001"
      refresh_ms: 15000
    executor:
      mode: paper                # paper | live（live 当前被拒绝）
      account: simulated
      initial_balances: { BTC: "0.2", USDT: "2000" }
      hard_limits: { BTC: "0.2", USDT: "2000" }
      maker_fee_rate: "0.001"
~~~

校验规则：

- 出现未知字段即拒绝；十进制值按定点数精确解析。
- 分片 1 到 1000 个，ID 在 [0, 1000) 且不重复；`account` 不重复。
- `price_increment` 是 `price_per_tick` 的整数倍，`base_increment` 是 `amount_per_lot` 的整数倍；各步长和最小值非零。
- `refresh_ms` 与 `stale_after_ms` 大于 0；价差和费率小于 1。
- `initial_balances` 与 `hard_limits` 必须包含该市场的两种资产。
- 实时模式下 symbol 只能是大写字母和数字。

### 9.3 Control

`STATE_DIR/control.sock` 上一行一个 JSON 请求，每个请求带 `request_id`，响应带同一 `request_id` 和 `ok`。单个请求不超过 64 KiB，收发超时 2 秒，一次服务一个客户端。

| op | 参数 | 响应 |
| --- | --- | --- |
| `status` | 无 | 每个 Shard 的运行与盘口状态、计数、活动订单数、余额、最近错误；写队列的 `queued`、`dropped`、`error` |
| `history` | `limit`（默认 100）、`cursor`（默认 0） | 记录数组和 `next_cursor`；数额字段为字符串，避免 JSON 客户端丢失 `uint64` 精度 |
| `set_strategy` | `shard_id`、`expected_version`、`patch` | 新的 `config_version` 和本次命令数；流程见 7.7 |
| `stop` | 无 | `accepted: true`；流程见 7.8 |
