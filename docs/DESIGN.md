# hquant 1.0 设计

目标运行路径：Binance 公开行情或文件回放 → Shard → SQLite 历史。Control 负责查询状态、历史、修改策略参数和停止。每个 Shard 固定一个市场、一个 Strategy 和一个模拟账户；本版不接真实交易所下单。

## 1. 三个主要部分

~~~mermaid
flowchart LR
    I["行情输入<br/>Binance / 回放"] --> S["Shard × N<br/>每个 Shard 一个线程<br/>Market · Strategy · Executor"]
    S --> H["SQLite 历史"]
    C["Control 客户端"] --> R["Runtime / Control"]
    R --> S
    R --> H
~~~

每个 Shard 线程独占本分片的交易状态。Market 管连接、解析和盘口；Strategy 生成限价下单或撤单意图；Executor 管订单、风险、模拟交易和账户。SQLite 与 Control 不进入每次策略决策的调用链。

每个 Shard 有自己的 io_context 和唯一工作线程。Market 的 Binance 读取与盘口过期检查、Shard 的策略定时器都在这个线程上运行。实时行情由 Market 协程读取并直接更新 Market 内的 OrderBook，然后同步通知 Shard；不存在“Market 把事件交给 Shard，Shard 再交回 Market”的步骤。Shard 收到通知后同步调用 Executor、Strategy，中途不 co_await。跨线程的回放和 Control 请求必须先投递到 Shard 的 io_context。

## 2. 目录

目标主干保持扁平，不为 Market、Strategy、Executor 再各建一层目录：

~~~text
hquant/src/
  main.cc                       # 进程入口：读取配置，启动 Runtime
  config.{h,cc}                 # AppConfig 与配置解析
  runtime.{h,cc}                # 构建、路由、启动和停止 Shard
  replay_reader.{h,cc}          # 文件回放输入
  sqlite_history.{h,cc}         # 异步写入、只读查询
  control_server.{h,cc}         # Unix socket、JSON 边界
  base/
    fixed.{h,cc}               # 十进制文本转 uint64 定点数
    ids.h                     # ShardId、MarketId、OrderId 等值类型
    net.{h,cc}                # REST/WebSocket 通用传输
  shard/
    shard.{h,cc}              # 线程、事件循环、调用顺序
    market.{h,cc}             # 行情状态与盘口同步
    market_type.h            # 行情输入和通知值类型
    order_book.{h,cc}
    binance_feed.{h,cc}       # REST 快照、WebSocket 公开数据
    strategy.{h,cc}           # 刷新时间与策略决策
    executor.{h,cc}           # 下单、撤单、校验与账户视图
    paper_exchange.{h,cc}    # 唯一的活动订单和模拟账户、撮合
~~~

数据类型跟随拥有它的模块：盘口事件放在 market_type.h，策略决定放在 strategy.h，订单及账户回报放在 executor.h。base 只放真正跨模块的数值和 ID，不再用一个大 market.h / order.h 混放接口和业务结构。网络或 SQLite 实现需要拆小文件时，仍放在所属目录。

配置中的金额、数量和比例用 `uint64_t` 定点数，缩放因子为 10^9；字段名用 `_nanos` 或 `_ppb` 明示单位。十进制文本解析时拒绝超出九位有效小数和整数溢出，不经过 `double`。盘口保持整数 ticks/lots，网络价格和数量先精确转为定点数，再检查能否被配置的 tick/lot 整除。Market 和 Shard 直接持有各自状态，不使用 `Impl`/pimpl。

## 3. 关键结构体及归属

以下是目标字段，不照搬当前头文件。一个 Shard 的配置放在一个对象中，避免多组平行数组按下标对齐。

~~~cpp
struct ShardConfig {
  ShardId id;
  MarketConfig market;       // 市场、交易规则、盘口过期时间
  StrategyConfig strategy;   // 价差、数量、刷新周期（必须大于 0）
  ExecutorConfig executor;   // 账户、初始余额、资金上限、费率
};

struct AppConfig {
  InputConfig input;         // Binance 或回放文件
  std::vector<ShardConfig> shards;
  std::string sqlite_path;
  std::string control_socket;
};

~~~

启动参数由 gflags 在 `main.cc` 解析。`--config` 提供完整 YAML，`--state_dir` 指定运行目录；显式指定 `--strategy_shard` 时，可用 `--order_amount`、`--bid_spread`、`--ask_spread`、`--refresh_ms` 覆盖该分片的策略参数。入口把覆盖值解析并校验成 `AppConfig` 后构造 Runtime；Shard 不读取全局 flags。运行中修改经 Control `set_strategy` 投递到目标 Shard，规则见第 6.5 节。

Market 和 Shard 的边界固定如下。MarketInput 是 Market 内部处理的已解析行情，MarketNotice 是 Market 完成状态更新后给 Shard 的通知。通知里的 PublicTrade 供模拟撮合使用，盘口由 Shard 同步读取 Market::View()。

~~~cpp
struct FeedConnected {
  uint64_t connection_epoch;
};
struct FeedDisconnected {
  uint64_t connection_epoch;
  ErrorCode reason;
};
struct TimerTick {};
using MarketInput = std::variant<FeedConnected, BookSnapshot, BookDiff,
                                 PublicTrade, FeedDisconnected>;

struct MarketNotice {
  enum class Kind { BookChanged, PublicTrade, BookUnavailable } kind;
  std::optional<PublicTrade> trade;           // 仅 PublicTrade 通知有值
};

struct InputTime {
  int64_t at_us;
  uint64_t ordinal;                      // 同一时刻的文件顺序
};

struct ReplayRecord {
  ShardId target;
  InputTime time;
  std::variant<MarketInput, TimerTick> body;
};

struct NoAction {};
struct CancelOrders { std::vector<OrderId> ids; };
struct LimitOrderRequest { Side side; uint64_t price_nanos; uint64_t quantity_nanos; };
struct SubmitOrders { std::vector<LimitOrderRequest> orders; };
using StrategyDecision = std::variant<NoAction, CancelOrders, SubmitOrders>;

class Strategy {
 public:
  StrategyDecision Decide(const BookView&, const ExecutionView&, MonoTime now);
 private:
  StrategyConfig config_;
  std::optional<MonoTime> next_refresh_at_;  // 空值表示盘口恢复后可立即下单
};

class Market {
 public:
  Market(MarketConfig, std::function<void(const MarketNotice&)> notify);
  awaitable<void> RunBinance();                 // 读入、解析、更新盘口、通知 Shard
  void OnReplayInput(const MarketInput&);      // 回放：在 Shard 线程上做同样的更新与通知
  bool CheckStale(MonoTime now);               // 若刚变为不可交易：通知 Shard，返回 true
  const BookView& View() const;
  void Stop();                                 // 取消行情 I/O 和过期定时器
 private:
  void HandleInput(const MarketInput&);       // 上述两种输入共用；仅 Market 调用
  BinanceFeed feed_;                           // REST / WebSocket 输入
  OrderBook book_;
  std::function<void(const MarketNotice&)> notify_; // Shard::OnMarket，同步调用
};

struct ShardStatus;
class Shard {
 public:
  void PostReplay(ReplayRecord, std::function<void(ErrorCode)> done); // 跨线程入口
  void RequestStop();                          // 跨线程投递停止命令
  ShardStatus CaptureStatus() const;           // 只在所属线程调用
 private:
  void OnMarket(const MarketNotice&);         // Market 通知入口
  void OnStrategyTimer();                     // 策略刷新入口
  asio::io_context io_;
  Market market_;
  Strategy strategy_;
  Executor executor_;                         // 资金检查与 PaperExchange
  std::thread worker_;
};
~~~

Shard 构造 Market 时传入一个指向 Shard::OnMarket 的回调。Market 在所属 Shard 线程同步调用它；回调返回前不处理下一条行情。PostReplay 在运行时 asio::post 到该线程，完成 Market 通知及交易处理后才调用 done；停止中的已排队请求和停止后的新请求也各调用一次 done，返回停止错误。Shard::OnMarket 内不 co_await；撤单后最多再调用一次 Strategy::Decide，不递归调用 OnMarket。

| 类型 | 关键字段 | 所有者 |
| --- | --- | --- |
| BookSnapshot | market、连接代次、盘口序号、买卖档 | Market |
| BookDiff | market、连接代次、首尾序号、变动档；数量 0 表示删除 | Market |
| PublicTrade | market、连接代次、价格、数量、主动方、时间 | Market |
| BookView | 同步状态、连续序号、买一卖一、最近成交；只在当前事件内借用 | Market |
| CancelOrders / SubmitOrders | 待撤的订单 ID / 待下单的方向、价格、数量 | Strategy 输出 |
| Order | ID、市场、账户、方向、价格、数量；提交后核心字段不变 | PaperExchange（Executor 内部） |
| CommandResult | 本地命令是否派发及本地拒绝原因；不表示交易所接受或成交 | Executor 生成 |
| OrderReport / Fill / BalanceUpdate | 拒绝、接受、部分成交、完成、取消 / 唯一成交 ID 与费用 / 资产总额与可用额 | PaperExchange 产生，Shard 经账户事件入口处理 |
| ExecutionView | 活动订单、可用余额、资金占用；只在当前事件内借用 PaperExchange 数据 | Executor 提供 |
| ShardStatus | 运行状态、盘口状态、活动订单数、余额、最近错误 | Shard 生成 |
| HistoryRecord | run、shard、分片内序号、时间、动作或回报 | Shard 生成，SQLite 保存 |

MarketInput 只用于 Binance Feed 内部和 ReplayRecord。交易处理只接收 MarketNotice 并读取 BookView；Shard 不维护第二份原始行情事件队列。BookChanged 只在盘口同步完成或 Live 盘口更新后发出；PublicTrade 通知携带本次公开成交；BookUnavailable 只在盘口从可交易变为不可交易时发出。1.0 的 Strategy 是具体类，根据参考价、价差和可用资金生成买卖两侧的限价下单意图，不引入策略基类。旧名 quote 在做市中指买卖挂单报价，在交易对中又是 quote asset（计价资产）；这里的动作传的是订单方向、价格、数量，故命名 SubmitOrders。一次 Decide 只返回无动作、撤旧单或提交新单中的一种。AccountEvent 分为订单报告、逐笔成交、余额变更；公开成交不是本账户的 Fill。

## 4. Shard 的固定处理顺序

~~~text
Market::HandleInput 更新盘口 / 最近成交，或 Market::CheckStale 标记过期
  → Market 同步调用 Shard::OnMarket(MarketNotice)
  → Shard 读取 Market::View()
  → Executor 调用 PaperExchange，用新行情撮合已有模拟订单
  → Strategy 读取最新 BookView + ExecutionView，作一次决定
  → Executor 执行撤单或新单，返回本地 CommandResult
  → Shard 记录本地命令结果；PaperExchange 的 EventSink 已投递账户事件
  → 若刚才撤单且全部结束、盘口仍可交易，再调用一次 Strategy::Decide
  → 若第二次决定下单，Executor 执行并记录本地命令结果
  → 当前回调结束后，Shard::OnAccountEvent 逐条处理已投递事件
~~~

Market 处理完输入、确定新状态后才发通知。快照前缓存增量时不通知；断档、断线或过期导致盘口从 Live 离开时发一次 BookUnavailable，并由 Market 自己重新连接或取快照。Market 的通知回调和 Shard::OnMarket 都在所属 Shard 线程上同步执行。Shard 只负责通知之后的交易顺序，不另存订单或账户余额。

| MarketNotice | Executor | Strategy |
| --- | --- | --- |
| BookChanged | 用更新后的盘口撮合已有模拟订单 | 根据最新盘口决定是否刷新 |
| PublicTrade | 用本次公开成交撮合已有模拟订单；即使盘口暂不可交易也处理已有订单 | 只在盘口 Live 时允许新单 |
| BookUnavailable | 不用失效盘口撮合 | 撤掉旧挂单，不提交新单 |

PaperExchange 是 Executor 的内部组件，也是活动订单和模拟账户余额的唯一保存处；冻结额度从活动订单的剩余量推导。`Executor::Dispatch` 只返回本地 `CommandResult`：`dispatched` 表示指令已送到模拟交易所，不表示订单被接受或成交。PaperExchange 原子更新内部状态，再经 EventSink 把 `AccountEvent` 投递到 Shard 的 `io_context`；Shard 通过 `OnAccountEvent` 路径处理。回放在这一批事件处理完后才读取下一条输入。未来实盘适配器可从交易所私有账户流向同一入口提供事件。调用位置固定如下：

| Executor 入口 | Executor 内部调用 |
| --- | --- |
| OnMarket(BookChanged) | PaperExchange::OnBookBbo，按可见数量模拟撮合并生成账户事件 |
| OnMarket(PublicTrade) | PaperExchange::OnPublicTrade，按公开成交量模拟撮合并生成账户事件 |
| OnMarket(BookUnavailable) | 不调用 PaperExchange |
| Dispatch(CancelOrders) | PaperExchange::Cancel，返回本地命令结果；账户事件另行处理 |
| Dispatch(SubmitOrders) | 校验规则和资金上限后调用 PaperExchange::Place，返回本地命令结果；账户事件另行处理 |

Executor 不保留第二份活动订单、冻结表或账户余额；PaperExchange 更新后，Strategy 才能读取新的 ExecutionView。每次部分成交生成一条 Fill 和一条带剩余量的 OrderReport，trade_id 不重复。单次操作若无法同时完成订单、余额及对应事件的更新，Shard 进入停止流程，不执行第二次决策。

Strategy 除配置外只保存 `next_refresh_at`，初始为空。配置解析拒绝不大于 0 的刷新周期。是否有活动订单由 ExecutionView 提供，Strategy 不保存订单阶段或 `requote_pending`。

| Decide 时的条件 | 返回值与时间更新 |
| --- | --- |
| 盘口不是 Live | 清空 next_refresh_at；有活动订单则返回包含全部活动订单 ID 的 CancelOrders，否则返回 NoAction |
| 盘口 Live，但未到 next_refresh_at | NoAction |
| 盘口 Live，刷新时间已到，仍有活动订单 | 返回包含全部活动订单 ID 的 CancelOrders；next_refresh_at 不变 |
| 盘口 Live，刷新时间已到或为空，没有活动订单 | 根据当前盘口和余额返回 SubmitOrders；没有有效订单则返回 NoAction；两种结果都将 next_refresh_at 设为当前时间加刷新周期 |

撤单在同一次 PaperExchange 调用中完成。目标订单若仍留在活动集合中，Shard 视为内部错误并停止；否则在盘口仍 Live 时立即进行第二次 Decide，最多两次，不循环。刷新时间前若订单全部成交，等到刷新时间再下单。新单被拒绝也等下一次刷新。

SubmitOrders 按订单顺序逐单执行。Executor 用 PaperExchange 的最新余额校验规则和资金上限，前一单派发后重新计算下一单额度；本地校验拒绝不调用 Place，由 Shard 记录命令结果。PaperExchange 也可通过 OrderRejected 事件拒绝已派发请求。Place、Cancel、OnBookBbo、OnPublicTrade 每次调用必须原子完成：订单、余额与生成的账户事件一起提交。一次行情只按可见数量成交，剩余量继续留在活动订单；后续行情可再次部分成交，撤单只释放剩余占用。

买单按金额及最坏情况手续费占用计价资产，卖单按数量占用基础资产。对每种资产，Executor 从 PaperExchange 的余额计算 `已用 = max(0, 初始总额 − 当前总额)`、`冻结 = 当前总额 − 当前可用额`，可下单额度为 `min(当前可用额, max(0, hard_limit − 已用 − 冻结))`。Executor 不另存资金占用表。

## 5. 状态机

本版只有三处跨事件的生命周期转换：Market 盘口、单张模拟订单、Shard。Strategy 的刷新时间是定时门控，决定规则见第 4 节。

### 5.1 Market 盘口同步

Syncing 表示盘口不能用于新下单，Live 表示快照和增量连续、买一卖一有效。重连、等待快照和追增量都属于 Syncing，由连接代次、快照序号和增量缓冲区分，不增加状态。

~~~mermaid
stateDiagram-v2
    [*] --> Syncing: 启动
    Syncing --> Syncing: 等输入或重新同步
    Syncing --> Live: 快照与缓存增量全部接上，盘口有效
    Live --> Live: 连续增量
    Live --> Syncing: 新连接、断线、断档、过期或无效盘口
    Syncing --> [*]: Shard 停止
    Live --> [*]: Shard 停止
~~~

| 转换 | 触发条件 | Market 动作与通知 |
| --- | --- | --- |
| Syncing → Syncing | 新连接；快照或增量尚不足以接上；缓存断档或无效 | 新连接清空旧数据并更新 connection_epoch；当前连接的增量先缓存。有快照后检查首条增量满足 first_sequence ≤ S+1 ≤ last_sequence，并检查后续全部连续；缺口或无效盘口丢弃本轮快照重新同步。不通知 Shard |
| Syncing → Live | 快照及目前缓存的全部增量连续，最终买一卖一有效 | 应用快照和增量，更新连续序号与盘口时间，发一次 BookChanged |
| Live → Live | 当前连接的增量满足 first_sequence ≤ 当前序号+1 ≤ last_sequence，应用后盘口有效 | 更新盘口、序号与盘口时间，发 BookChanged；已过期的旧增量直接忽略 |
| Live → Syncing | 当前连接断线或换代、增量有缺口、盘口过期或盘口无效 | 清空不可用盘口，发一次 BookUnavailable，开始重新连接或取快照 |

快照先到或增量先到都走同一套衔接检查；缓存里后续任何一条断档，都不能短暂进入 Live。旧 connection_epoch 的输入、重复 FeedConnected、Live 时到达的快照直接忽略。PublicTrade 不改变盘口状态或序号，也不延长盘口有效期；Market 通知 Shard 后，Executor 可用它撮合已有订单。Market::Stop 由 Shard 停止流程调用，不是第三个盘口状态。

### 5.2 单张模拟订单

订单在 PaperExchange 接受 Place 请求时进入 active_orders，同时投递 OrderAccepted 事件。本地 `CommandResult::dispatched` 只表示提交请求通过本地校验。活动订单保存原始数量、剩余数量和剩余资金占用；每次模拟撮合可部分成交。订单报告、成交和余额变化作为独立账户事件交给 Shard。

~~~mermaid
stateDiagram-v2
    [*] --> Active: OrderAccepted
    [*] --> Rejected: OrderRejected
    Active --> Active: Fill + PartiallyFilled，减少剩余量
    Active --> Filled: Fill + Filled，剩余量归零
    Active --> Cancelled: CancelOrders 或 Shard 停止
~~~

| 转换 | 条件与效果 |
| --- | --- |
| 进入 Active | PaperExchange 接受请求，加入 active_orders 并产生 OrderAccepted 和余额事件 |
| Active → Active | OnBookBbo 或 OnPublicTrade 命中部分数量，更新余额与剩余占用，产生 Fill、PartiallyFilled 和余额事件 |
| Active → 结束：成交 | 剩余数量归零，移除活动订单，产生 Fill、Filled 和余额事件 |
| Active → 结束：取消 | Cancel 移除活动订单，释放剩余占用，产生 Cancelled 和余额事件 |
| 未进入 Active | Executor 本地拒绝产生未派发的 CommandResult；PaperExchange 拒绝已派发请求产生 OrderRejected 事件，不冻结资金 |

结束只是从 active_orders 移除并写历史，不另存 Done 状态。PaperExchange 是活动订单和余额的唯一所有者；事件与内部数据矛盾时，Shard 进入停止流程。公开行情只能支撑模拟成交假设，不能证明真实账户成交；实盘成交必须由交易所私有账户事件确认。

### 5.3 Shard 生命周期

Shard 的运行阶段用于拦截新策略动作并保证停止顺序。它由 stop_requested 和工作线程是否结束推导，不需要单独的生命周期枚举。

~~~mermaid
stateDiagram-v2
    [*] --> Running: 工作线程启动
    Running --> Stopping: Control stop、回放结束或内部错误
    Stopping --> Stopping: 重复停止请求
    Stopping --> Exited: I/O 结束、活动单终结、回放回调完成、历史已提交写队列
    Exited --> [*]: Runtime join 工作线程
~~~

| 阶段 | 允许的处理 |
| --- | --- |
| Running | 按第 4 节处理行情、定时器和回放；允许 Strategy::Decide |
| Stopping | stop_requested 已置位；不再调用 Strategy；停止 Market 和定时器、取消活动订单、记录终结回报；所有已排队的回放请求都回调停止错误。重复 stop 不再撤单 |
| Exited | Shard 工作线程已结束；Runtime join 后刷新并关闭 SQLite，再关闭 Control socket |

停止后的新回放请求直接返回停止错误。SQLite 写入器和 Control socket 只有资源关闭顺序，不加入交易状态机。

## 6. 输入和请求时序

### 6.1 Binance 公开行情

~~~mermaid
sequenceDiagram
    participant B as Binance REST/WS
    participant M as Market 协程与盘口
    participant S as Shard
    participant E as Executor
    participant P as Strategy
    B-->>M: 连接状态 / 快照 / 增量 / 公开成交
    M->>M: 解析并 HandleInput，更新盘口或最近成交
    alt 序号断档或断线
        M->>M: 标记不可交易；自行重连或重新取快照
        opt 此前盘口是 Live
            M->>S: OnMarket(BookUnavailable)
        end
    else 快照、增量或公开成交已应用
        M->>S: OnMarket(BookChanged / PublicTrade)
    else 等待快照
        M->>M: 缓存增量，不通知 Shard
    end
    opt Shard 收到通知
        S->>M: View()，只读
        M-->>S: 当前 BookView
        S->>E: OnMarket(notice, BookView)
        E->>E: BookChanged / PublicTrade 时调用 PaperExchange；内部更新订单和账户
        E-->>S: 模拟账户事件；Shard 逐条处理
        S->>P: Decide(BookView, ExecutionView)
        P-->>S: NoAction / CancelOrders / SubmitOrders
        opt 首次决定有动作
            S->>E: Dispatch(decision)
            E->>E: Place 或 Cancel；PaperExchange 更新内部状态
            E-->>S: 本地命令结果；账户事件另行处理
        end
        opt 首次决定为 CancelOrders，旧单已清空、盘口 Live 且无错误
            S->>P: 再次 Decide(撤单后的最新视图)
            P-->>S: NoAction / SubmitOrders
            opt SubmitOrders
                S->>E: Dispatch(SubmitOrders)
                E->>E: Place；PaperExchange 更新内部状态
                E-->>S: 本地命令结果；账户事件另行处理
            end
        end
        S->>S: 写历史；本条输入结束
    end
~~~

盘口过期由 Market 自己的定时检查触发，走同一个 BookUnavailable 通知。快照前的增量由 Market 缓存；快照与增量序号接上后才进入可交易状态。Market 不调用 Strategy 或 Executor。盘口不可交易时，Strategy 可撤旧单，Executor 拒绝新单。

### 6.2 文件回放

~~~mermaid
sequenceDiagram
    participant F as ReplayReader
    participant S as Shard
    participant M as Market
    participant E as Executor
    participant P as Strategy
    F->>S: 投递记录 + InputTime
    S->>S: asio::post 到本线程；推进虚拟时钟
    alt 行情输入：连接、快照、增量、公开成交或断线
        S->>M: OnReplayInput(MarketInput)
        M->>M: HandleInput，更新盘口或最近成交
        opt 产生 MarketNotice
            M->>S: OnMarket(notice)，同步调用
            S->>E: OnMarket(notice, Market::View())
            E->>E: BookChanged / PublicTrade 时调用 PaperExchange；内部更新状态
            E-->>S: 模拟账户事件；Shard 逐条处理
            S->>P: Decide(最新视图)
            P-->>S: 决定
            opt 有动作
                S->>E: Dispatch(决定)
                E-->>S: 本地命令结果；账户事件另行处理
            end
            opt 撤单后旧单已清空、盘口 Live 且无错误
                S->>P: 再次 Decide(撤单后的最新视图)
                P-->>S: NoAction / SubmitOrders
                opt SubmitOrders
                    S->>E: Dispatch(SubmitOrders)
                    E-->>S: 本地命令结果；账户事件另行处理
                end
            end
        end
    else 定时事件
        S->>M: CheckStale(virtual_now)
        opt 盘口刚变为不可交易
            M->>S: OnMarket(BookUnavailable)
        end
        opt 盘口状态未改变
            S->>S: OnStrategyTimer()
        end
    end
    S-->>F: 本条完成或已停止（完成回调）
    opt 正常完成
        F->>F: 读取下一条
    end
~~~

回放读取器等待 Shard 完成本条处理再读下一条。Market::OnReplayInput 与 RunBinance 共用 HandleInput；区别只在输入来源。ReplayReader 不直接修改盘口、订单或账户，回放也不启动墙上时间定时器。定时事件先检查盘口是否过期；若 Market 已因过期通知 Shard，本次不再触发第二次策略决定。文件读到末尾后由 Runtime 发起停止。停止时，尚未执行的回放投递也必须各调用一次完成回调，读取器收到停止错误后退出。

### 6.3 定时刷新、撤单和重新下单

~~~mermaid
sequenceDiagram
    participant T as 定时器
    participant S as Shard
    participant P as Strategy
    participant E as Executor
    T->>S: TimerTick
    S->>P: Decide(最新盘口、订单、余额)
    alt 决定撤单
        P-->>S: CancelOrders
        S->>E: Dispatch(CancelOrders)
        E->>E: PaperExchange::Cancel；内部更新订单和账户
        E-->>S: 本地命令结果；另行产生取消和余额事件
        S->>S: 分别记录命令和账户事件
        opt 已无活动旧单，盘口仍 Live 且无错误
            S->>P: 再次 Decide(撤单后的最新视图)
            P-->>S: SubmitOrders / NoAction
        end
    else 决定下单
        P-->>S: SubmitOrders
    else 未到时间或盘口不可交易且无旧单
        P-->>S: 无动作
    end
    opt 当前决定为 SubmitOrders
        S->>E: Dispatch(SubmitOrders)
        E->>E: Executor 校验规则与额度
        E->>E: PaperExchange::Place；内部冻结、登记或拒绝
        E-->>S: 本地命令结果；另行产生订单和余额事件
        S->>S: 分别记录命令和账户事件
    end
~~~

定时器只触发 Strategy 决策，不直接操作 PaperExchange。行情触发模拟撮合，Strategy 决定撤单或下单；两条路径都经过 Executor。命令结果与账户事件分开记录；PaperExchange 将同一次操作的状态更新和事件生成原子提交。每条输入最多撤单一次、下单一次。

### 6.4 status 与 history

~~~mermaid
sequenceDiagram
    participant C as Control 客户端
    participant A as ControlServer / Runtime
    participant S as Shard
    participant D as SQLite
    alt status
        C->>A: status(request_id)
        loop 每个 Shard
            A->>S: 投递状态快照请求
            S-->>A: ShardStatus
        end
        A-->>C: JSON 状态 + writer 健康状态
    else history
        C->>A: history(limit, cursor)
        A->>D: 只读分页查询
        D-->>A: 记录 + next_cursor
        A-->>C: JSON 历史页
    end
~~~

status 在 Shard 当前事件结束后取快照；history 只走 SQLite 读连接，不进入交易事件循环。JSON 编解码只在 ControlServer 边界。

### 6.5 set_strategy

~~~mermaid
sequenceDiagram
    participant C as Control 客户端
    participant A as ControlServer / Runtime
    participant S as 目标 Shard
    participant P as Strategy
    participant E as Executor
    C->>A: set_strategy(shard_id, expected_version, patch)
    A->>A: 解析允许字段和数值类型
    A->>S: 投递 StrategyPatch
    S->>S: 复制当前配置，应用 patch，校验完整候选值
    alt 版本不符或参数非法
        S-->>A: 错误；原配置和版本不变
    else 提交成功
        S->>P: 替换配置，清空 next_refresh_at
        S->>E: 撤销旧单，记录本地命令结果和取消事件
        S->>P: 用当前盘口和余额决定是否重新下单
        opt 有新单
            S->>E: 执行新单
        end
        S-->>A: 新版本和本次动作结果
    end
    A-->>C: 同一 request_id 的响应
~~~

`expected_version` 是目标 Shard 的策略配置版本，初始值为 1；每次成功提交加 1。多个字段作为一个 patch 提交，任一字段无效则整组不生效。只允许修改下单数量、买卖价差和刷新周期；盘口、交易规则、账户、费率和存储配置在运行中不变。配置提交与该 Shard 的行情事件串行，不会在一次决策中混用新旧参数。参数更新复用第 4 节的撤单、第二次决策和回报记录流程。

### 6.6 stop

~~~mermaid
sequenceDiagram
    participant C as Control 客户端
    participant A as ControlServer / Runtime
    participant S as Shard
    participant E as Executor
    participant D as SQLite
    C->>A: stop(request_id)
    A-->>C: accepted
    A->>S: 投递停止请求
    S->>S: 标记 stop_requested，停止策略新动作，结束行情和定时协程
    S->>E: 终止模拟活跃订单
    E-->>S: 订单及账户回报
    S->>S: 写终结历史
    S-->>A: Shard 线程结束
    A->>D: flush 并关闭
    A->>A: 关闭 Control socket
~~~

accepted 仅表示停止请求已受理；最终历史落盘由 Runtime 在 Shard 退出后完成。`stop_requested` 是单向标志，重复 stop 不再执行第二次撤单；置位后 Shard 不再调用 Strategy，并在退出事件循环前完成已排队的回放回调。status 的运行中、停止中、已退出由该标志和线程是否结束推导，不另存一套生命周期枚举。
