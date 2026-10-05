# hquant 1.0 设计

目标运行路径：Binance 公开行情或文件回放 → Shard → SQLite 历史。Control 负责查询状态、历史和停止。每个 Shard 固定一个市场、一个 Strategy 和一个模拟账户；本版不接真实交易所下单。

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
    decimal.{h,cc}             # 精确数值
    ids.h                     # ShardId、MarketId、OrderId 等值类型
  shard/
    shard.{h,cc}              # 线程、事件循环、调用顺序
    market.{h,cc}             # 行情状态与盘口同步
    order_book.{h,cc}
    binance_feed.{h,cc}       # REST 快照、WebSocket 公开数据
    strategy.{h,cc}           # 刷新时间与策略决策
    executor.{h,cc}           # 下单、撤单、校验与账户视图
    paper_exchange.{h,cc}    # 唯一的活动订单和模拟账户、撮合
~~~

数据类型跟随拥有它的模块：盘口事件放在 market.h，策略决定放在 strategy.h，订单及账户回报放在 executor.h。base 只放真正跨模块的数值和 ID，不再用一个大 market.h / order.h 混放接口和业务结构。网络或 SQLite 实现需要拆小文件时，仍放在所属目录。

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
struct LimitOrderRequest { Side side; Decimal price; Decimal quantity; };
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
| OrderReport / Fill / BalanceUpdate | 接受或终结报告与累计成交 / 唯一成交 ID 与费用 / 资产总额与可用额 | PaperExchange 产生，Executor 转交 |
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
  → Executor 执行撤单或新单，取得完整的模拟回报
  → Shard 记录动作和回报
  → 若刚才撤单且全部结束、盘口仍可交易，再调用一次 Strategy::Decide
  → 若第二次决定下单，Executor 执行并取得回报；本条输入到此结束
~~~

Market 处理完输入、确定新状态后才发通知。快照前缓存增量时不通知；断档、断线或过期导致盘口从 Live 离开时发一次 BookUnavailable，并由 Market 自己重新连接或取快照。Market 的通知回调和 Shard::OnMarket 都在所属 Shard 线程上同步执行。Shard 只负责通知之后的交易顺序，不另存订单或账户余额。

| MarketNotice | Executor | Strategy |
| --- | --- | --- |
| BookChanged | 用更新后的盘口撮合已有模拟订单 | 根据最新盘口决定是否刷新 |
| PublicTrade | 用本次公开成交撮合已有模拟订单；即使盘口暂不可交易也处理已有订单 | 只在盘口 Live 时允许新单 |
| BookUnavailable | 不用失效盘口撮合 | 撤掉旧挂单，不提交新单 |

PaperExchange 是 Executor 的内部组件，也是活动订单和模拟账户余额的唯一保存处；冻结额度从活动订单推导。它的方法先更新自己的订单和账户，再同步返回本次完整的 AccountEvent；Executor 从更新后的 PaperExchange 构造 ExecutionView，并把回报交还 Shard。调用位置固定如下：

| Executor 入口 | Executor 内部调用 |
| --- | --- |
| OnMarket(BookChanged) | PaperExchange::OnBookBbo，取回完整回报 |
| OnMarket(PublicTrade) | PaperExchange::OnPublicTrade，取回完整回报 |
| OnMarket(BookUnavailable) | 不调用 PaperExchange |
| Execute(CancelOrders) | PaperExchange::Cancel，取回完整回报 |
| Execute(SubmitOrders) | 校验规则和资金上限后调用 PaperExchange::Place，取回完整回报 |

Executor 先检查交易规则和可用资金，再生成订单 ID 并调用 PaperExchange；挂单登记、成交和取消都由 PaperExchange 完成，冻结额度随活动订单变化。Executor 不保留第二份活动订单、冻结表或账户余额。一次 PaperExchange 调用结束后，Strategy 才能读取新的 ExecutionView。PaperExchange 保证同一批回报包含本次订单终结所需的全部成交明细，且 trade_id 不重复。若返回的回报与更新后的订单或余额互相矛盾，Shard 进入停止流程，不执行第二次决策。

盘口未同步、已过期、断线或序号断档时停止新单并重新取快照。Executor 完整处理 PaperExchange 的撤单回报后，Shard 才能用**最新**盘口、活动订单和余额进行第二次决策。

## 5. 必要的状态与处理规则

### 5.1 Market：同步中、可交易

Market 的盘口状态只有 Syncing 和 Live。Syncing 同时涵盖重连、等快照和等第一条衔接增量；这些步骤用 connection_epoch、可选的盘口序号和增量缓冲表示，不各设一个状态。Market 还保存最后一次有效盘口更新时间和同步失败原因。

~~~mermaid
stateDiagram-v2
    [*] --> Syncing
    Syncing --> Syncing: 等快照、等增量或重新连接
    Syncing --> Live: 快照与增量序号接上
    Live --> Live: 连续增量
    Live --> Syncing: 断档、断线、超时或无效盘口
~~~

| 输入 | Market 处理 |
| --- | --- |
| 新连接 | 更新 connection_epoch，清空旧盘口、序号和缓冲，进入 Syncing；若原来是 Live，发一次 BookUnavailable |
| Syncing 收到增量 | 当前连接的增量按到达顺序缓存；若已有快照，立即尝试从快照序号衔接全部缓存增量；缓冲满则清空并重新同步 |
| Syncing 收到快照，序号 S | 保存快照，丢弃 last_sequence ≤ S 的旧增量，并尝试从 S+1 衔接全部缓存增量；若尚无增量则继续等待 |
| Syncing 尝试衔接 | 首个剩余增量须满足 first_sequence ≤ S+1 ≤ last_sequence，后续每条须连续；任一处缺口或最终盘口无效，都丢弃本轮快照并重新同步；全部接上后才进入 Live，发一次 BookChanged |
| Live 收到增量 | 旧增量忽略；满足 first_sequence ≤ 当前序号+1 ≤ last_sequence 才应用并发 BookChanged；缺口立即回到 Syncing |
| 断线、无效盘口或盘口过期 | 清空旧盘口并重新连接、取快照；只有从 Live 离开时才发 BookUnavailable |

旧 connection_epoch 的输入始终忽略，重复的 FeedConnected 不重置当前盘口；Live 时到达的快照也忽略。有效公开成交不推进盘口序号，也不能延长盘口有效期；新连接在 Syncing 时收到的公开成交仍可供 Executor 撮合已有订单，Strategy 此时不能下新单。Market::CheckStale 只根据最后一次有效盘口更新判断过期。

### 5.2 Strategy：一个运行时字段

Strategy 除配置外只保存 `next_refresh_at`，初始为空，表示盘口可交易后立即尝试下单。它表示下次允许刷新挂单的时间；配置解析拒绝不大于 0 的刷新周期。SubmitOrders 是一组买卖方向的限价单请求；是否有活动订单由 Executor 的 ExecutionView 提供，Strategy 不另存订单阶段，也不保存 `requote_pending`。

| 条件 | Strategy 返回 |
| --- | --- |
| 盘口不是 Live | 清空 next_refresh_at；有活动订单则返回包含全部活动订单 ID 的 CancelOrders，否则无动作 |
| 盘口 Live，next_refresh_at 尚未到达 | 无动作 |
| 盘口 Live，刷新时间已到，仍有活动订单 | 返回包含全部活动订单 ID 的 CancelOrders；不同时提交新单，也不推进 next_refresh_at |
| 盘口 Live，刷新时间已到或为空，没有活动订单 | 用当前盘口和余额计算 SubmitOrders；若无有效订单则返回 NoAction；无论结果如何，都把 next_refresh_at 设为当前时间加刷新周期 |

刷新时间到达且有活动单时，Shard 先按 Strategy 的决定撤单。PaperExchange 在同一次调用中给出完整结果；全部目标订单必须随调用结束，若仍有目标订单留在活动集合中就是内部错误，Shard 停止。活动单已清空、盘口仍 Live 且没有内部错误时，Shard 立刻再调用一次 Decide，用撤单后的余额生成新单。最多两次 Decide，不循环。刷新时间前若订单全部成交，剩余时间内不补单，到刷新时间再下单；新单被拒绝也等下一次刷新。

### 5.3 Executor：活动订单集合

1.0 的 PaperExchange 是本地同步组件：Place、Cancel、OnBookBbo、OnPublicTrade 每次调用都先更新内部订单与余额，再返回本次完整的 AccountEvent 批次。单次调用必须原子完成：成功时状态与回报一起提交，可预期的拒绝不改变状态；若发生部分更新或回报矛盾，Shard 停止。本版 SubmitOrders 按订单逐单调用 Place，不要求整组全成功。PaperExchange 用 `active_orders` 保存仍在挂的单，订单终结后移除并返回终结报告。Executor 转交回报给 Shard 记历史，不再维护另一份订单状态。无需订单生命周期状态枚举；订单报告携带终结原因和累计成交量。

| 处理点 | Executor 动作 |
| --- | --- |
| Executor::Execute(SubmitOrders) | 按订单顺序逐单处理：从 PaperExchange 的最新余额计算可用资金，校验规则和资金上限，生成订单 ID，再调用 Place；前一单冻结后重新计算下一单额度 |
| Executor 校验拒绝 | 返回该订单的拒绝结果，由 Shard 记历史；不调用 Place，不改变 PaperExchange |
| PaperExchange::Place | 接受时加入 active_orders，使可用额扣除最坏情况支出；拒绝时不改变活动订单或余额，返回 Rejected 报告 |
| PaperExchange 撮合出 Fill | 扣除实际支出，释放对应冻结，更新余额；本版一次成交整张订单 |
| PaperExchange::Cancel 或订单成交 | 从 active_orders 移除订单，释放剩余冻结，返回包含全部 Fill 和终结报告的批次 |
| Executor 收到互相矛盾的回报 | 记录内部错误，通知 Shard 进入停止流程；本地模拟器不得在之后补发本次成交明细 |

本版模拟撮合一次成交整张挂单，不产生跨事件的部分成交。买单按金额及最坏情况手续费冻结计价资产，卖单按数量冻结基础资产。对每种资产，Executor 从 PaperExchange 的余额计算 `已用 = max(0, 初始总额 − 当前总额)`、`冻结 = 当前总额 − 当前可用额`，再取 `min(当前可用额, max(0, hard_limit − 已用 − 冻结))` 作为可下单额度；卖出或撤单释放的额度可再次使用。Executor 不另存资金占用表。`active_orders` 是否包含订单，就是唯一需要长期保存的订单生命周期信息。

### 5.4 还有哪些状态转换

| 对象 | 逻辑转换 | 保存方式 |
| --- | --- | --- |
| Market 盘口 | Syncing ↔ Live | 唯一显式状态枚举；决定能否新下单 |
| 下单请求与模拟订单 | 请求 → 活动订单 → 结束；校验或 Place 拒绝则请求直接结束，不创建活动订单 | 活动订单在 PaperExchange 的 active_orders 中；结束或拒绝原因写入动作结果和历史 |
| Shard 生命周期 | 运行 → 停止中 → 已退出 | stop_requested 和工作线程是否结束；停止请求不可逆 |

这三处在逻辑上都有状态转换。Strategy 的 `next_refresh_at` 是定时门控，不另设阶段；余额是数值数据，订单报告的种类是事件。Executor 不再复制订单状态或账户状态。内部错误和 Control 的 stop 都走同一条停止流程。SQLite 写入器和 Control socket 的启动、关闭由 Runtime 管理，不加入交易状态机。

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
        E-->>S: 完整的模拟成交和账户回报
        S->>P: Decide(BookView, ExecutionView)
        P-->>S: NoAction / CancelOrders / SubmitOrders
        opt 首次决定有动作
            S->>E: Execute(decision)
            E->>E: Place 或 Cancel；PaperExchange 更新内部状态
            E-->>S: 动作结果与完整回报
        end
        opt 首次决定为 CancelOrders，旧单已清空、盘口 Live 且无错误
            S->>P: 再次 Decide(撤单后的最新视图)
            P-->>S: NoAction / SubmitOrders
            opt SubmitOrders
                S->>E: Execute(SubmitOrders)
                E->>E: Place；PaperExchange 更新内部状态
                E-->>S: 下单结果与完整回报
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
            E-->>S: 完整回报
            S->>P: Decide(最新视图)
            P-->>S: 决定
            opt 有动作
                S->>E: Execute(决定)
                E-->>S: 动作结果与回报
            end
            opt 撤单后旧单已清空、盘口 Live 且无错误
                S->>P: 再次 Decide(撤单后的最新视图)
                P-->>S: NoAction / SubmitOrders
                opt SubmitOrders
                    S->>E: Execute(SubmitOrders)
                    E-->>S: 下单结果与回报
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

回放读取器等待 Shard 完成本条处理再读下一条。Market::OnReplayInput 与 RunBinance 共用 HandleInput；区别只在输入来源。ReplayReader 不直接修改盘口、订单或账户，回放也不启动墙上时间定时器。定时事件先检查盘口是否过期；若 Market 已因过期通知 Shard，本次不再触发第二次策略决定。停止时，尚未执行的回放投递也必须各调用一次完成回调，读取器收到停止错误后退出。

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
        S->>E: Execute(CancelOrders)
        E->>E: PaperExchange::Cancel；内部更新订单和账户
        E-->>S: 动作结果、回报、当前活动订单数
        S->>S: 记录撤单动作和回报
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
        S->>E: Execute(SubmitOrders)
        E->>E: Executor 校验规则与额度
        E->>E: PaperExchange::Place；内部冻结、登记或拒绝
        E-->>S: 动作结果和回报
        S->>S: 记录下单动作和回报
    end
~~~

定时器只触发 Strategy 决策，不直接操作 PaperExchange。行情触发撮合，Strategy 决定触发撤单或下单；两条路径都经过 Executor。PaperExchange 的本地调用先完成订单和账户更新，再同步返回完整回报批次；每条输入最多撤单一次、下单一次。回报不齐视为内部错误并阻止新单。

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

### 6.5 stop

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
