# hquant 1.0 设计

目标运行路径：Binance 公开行情或文件回放 → Shard → SQLite 历史。Control 负责查询状态、历史和停止。每个 Shard 固定一个市场、一个 Simple PMM 策略和一个模拟账户；本版不接真实交易所下单。

## 1. 只有三个主要部分

~~~mermaid
flowchart LR
    I["行情输入<br/>Binance / 回放"] --> S["Shard × N<br/>每个 Shard 一个线程<br/>Market → Strategy → Executor"]
    S --> H["SQLite 历史"]
    C["Control 客户端"] --> R["Runtime / Control"]
    R --> S
    R --> H
~~~

Shard 是交易状态的唯一所有者。Market 管连接、解析和盘口；Strategy 只计算报价或撤单意图；Executor 管订单、风险、模拟交易和账户。SQLite 与 Control 不进入每次策略决策的调用链。

每个 Shard 有自己的 io_context 和唯一工作线程。实时行情读取、定时器、Shard 事件循环是这个 io_context 上的协程。协程等待 I/O 时可以交错；处理一条事件时同步调用 Market、Strategy、Executor，中途不 co_await，也不从回调重新进入事件处理。跨线程调用只投递消息，不能直接读写 Shard 对象。

## 2. 目录

目标主干保持扁平，不为 Market、Strategy、Executor 再各建一层目录：

~~~text
hquant/src/
  base/
    decimal.{h,cc}             # 精确数值
    ids.h                     # ShardId、MarketId、OrderId 等值类型
  shard/
    shard.{h,cc}              # 线程、事件循环、调用顺序
    market.{h,cc}             # 行情状态与盘口同步
    order_book.{h,cc}
    binance_feed.{h,cc}       # REST 快照、WebSocket 公开数据
    simple_pmm.{h,cc}         # 策略状态与决策
    executor.{h,cc}           # 下单、撤单、账户视图
    order_tracker.{h,cc}      # 订单状态与成交去重
    paper_exchange.{h,cc}    # 模拟撮合与账本
  app/
    config.{h,cc}
    runtime.{h,cc}            # 构建、路由、启动和停止 Shard
    replay_reader.{h,cc}
    sqlite_history.{h,cc}     # 异步写入、只读查询
    control_server.{h,cc}     # Unix socket、JSON 边界
apps/
  hquant_server.cc
~~~

数据类型跟随拥有它的模块：盘口事件放在 market.h，策略决定放在 simple_pmm.h，订单及账户回报放在 executor.h。base 只放真正跨模块的数值和 ID，不再用一个大 market.h / order.h 混放接口和业务结构。网络或 SQLite 实现需要拆小文件时，仍放在所属目录。

## 3. 关键结构体及归属

以下是目标字段，不照搬当前头文件。一个 Shard 的配置放在一个对象中，避免多组平行数组按下标对齐。

~~~cpp
struct ShardConfig {
  ShardId id;
  MarketConfig market;       // 市场、交易规则、盘口过期时间
  PmmConfig strategy;        // 价差、数量、刷新周期
  PaperConfig executor;      // 账户、初始余额、资金上限、费率
};

struct AppConfig {
  InputConfig input;         // Binance 或回放文件
  std::vector<ShardConfig> shards;
  std::string sqlite_path;
  std::string control_socket;
};

class Shard {
  asio::io_context io_;
  std::thread worker_;
  Market market_;             // 内含 OrderBook
  SimplePmm strategy_;
  Executor executor_;         // 内含订单跟踪、资金检查、PaperExchange
};
~~~

| 类型 | 关键字段 | 所有者 |
| --- | --- | --- |
| BookSnapshot | market、连接代次、盘口序号、买卖档 | Market |
| BookDiff | market、连接代次、首尾序号、变动档；数量 0 表示删除 | Market |
| PublicTrade | market、价格、数量、主动方、时间 | Market |
| BookView | 同步状态、连续序号、买一卖一、最近成交；只在当前事件内借用 | Market |
| CancelOrders / SubmitQuotes | 待撤的订单 ID / 待报的方向、价格、数量 | Strategy 输出 |
| Order | ID、市场、账户、方向、价格、数量；提交后核心字段不变 | Executor |
| OrderStatus / Fill / BalanceUpdate | 状态与累计成交 / 唯一成交 ID 与费用 / 资产总额与可用额 | Executor |
| ExecutionView | 活跃订单、可用余额、资金占用；只在当前事件内借用 | Executor |
| ShardStatus | 盘口状态、活跃订单数、余额、最近错误 | Shard 生成 |
| HistoryRecord | run、shard、分片内序号、时间、动作或回报 | Shard 生成，SQLite 保存 |

MarketEvent 只包含快照、增量、公开成交和连接状态。Strategy 的一次决定只有三种结果：无动作、撤旧单、提交新报价。不能在同一次决定中同时撤单和重新报价。AccountEvent 分为订单状态、逐笔成交、余额变更；公开成交不是本账户的 Fill。

## 4. Shard 的固定处理顺序

~~~text
取一条输入
  → Market 更新盘口或检查过期
  → Executor 用新行情撮合已有模拟订单，并入账回报
  → Strategy 读取最新 BookView + ExecutionView，作一次决定
  → Executor 执行撤单或新单，并入账即时回报
  → Shard 记录动作和回报
  → 必要时排入下一条“重新决策”内部事件
~~~

Shard 只负责这个顺序，不另存订单冻结表或账户余额。Executor 内部先检查交易规则与可用资金，再生成订单 ID、冻结资金、登记订单，最后交给模拟交易所。模拟交易所返回的回报先由 Executor 更新 OrderTracker、资金和余额，然后 Strategy 才能再次读取状态。订单状态先到而成交明细未齐时，订单继续占用资金；成交用 trade_id 去重。

盘口未同步、已过期、断线或序号断档时停止新报价并重新取快照。撤单请求发出后仍可能成交；只有旧单真正结束且成交已入账，Simple PMM 才用**最新**盘口和余额提交替代报价。

## 5. 输入和请求时序

### 5.1 Binance 公开行情

~~~mermaid
sequenceDiagram
    participant B as Binance REST/WS
    participant M as Market 协程与盘口
    participant S as Shard 事件循环
    participant E as Executor
    participant P as Simple PMM
    B-->>M: 快照 / 增量 / 公开成交
    M->>S: 投递 MarketEvent
    S->>M: Apply(event)
    alt 序号断档、断线或盘口过期
        M-->>S: Stale
        S->>M: 请求重新连接或取快照
    else 行情已应用
        M-->>S: BookView + 触发原因
        S->>E: OnMarket(event, BookView)
        E-->>S: 已入账的模拟成交和账户回报
        S->>P: Decide(BookView, ExecutionView)
        P-->>S: 无动作 / 撤单 / 新报价（盘口有效时）
        opt 有动作
            S->>E: Execute(decision)
            E-->>S: 动作结果与已入账回报
        end
        S->>S: 写历史；必要时排入重新决策
    else 等待快照
        M-->>S: 盘口不可交易
    end
~~~

快照前的增量由 Market 自己缓存；快照与增量序号接上后才进入可交易状态。行情协程只生产事件，不直接调用策略或改订单。

### 5.2 文件回放

~~~mermaid
sequenceDiagram
    participant F as ReplayReader
    participant S as Shard
    participant M as Market
    participant P as Strategy / Executor
    F->>S: 投递记录 + InputTime
    S->>S: 推进虚拟时钟
    alt 快照、增量或公开成交
        S->>M: Apply(event)
        S->>P: 按 5.1 的顺序处理
    else 定时事件
        S->>M: 检查盘口过期
        S->>P: 按 5.3 的顺序处理
    end
    S-->>F: 本条处理完成
    F->>F: 读取下一条
~~~

回放读取器等待确认再读下一条，以保持文件顺序；它不直接修改盘口、订单或账户。回放不启动墙上时间定时器。

### 5.3 定时刷新、撤单和重新报价

~~~mermaid
sequenceDiagram
    participant T as 定时器
    participant S as Shard
    participant P as Simple PMM
    participant E as Executor
    participant X as PaperExchange
    T->>S: TimerTick
    S->>P: Decide(最新盘口、订单、余额)
    alt 还有旧订单
        P-->>S: CancelOrders
        S->>E: Execute(CancelOrders)
        E->>X: Cancel
        X-->>E: 订单状态 / 成交 / 余额回报
        E->>E: 更新跟踪、资金、账户
        E-->>S: 旧单是否全部结束
        opt 旧单全部结束
            S->>S: 排入重新决策事件
            S->>P: Decide(最新盘口、订单、余额)
            P-->>S: SubmitQuotes
        end
    else 没有旧订单且盘口有效
        P-->>S: SubmitQuotes
    end
    opt 得到 SubmitQuotes
        S->>E: Execute(SubmitQuotes)
        E->>E: 规则检查 → 冻结资金 → 登记订单
        E->>X: Place
        X-->>E: 订单状态 / 成交 / 余额回报
        E->>E: 入账并更新订单
        E-->>S: 动作结果和回报
    end
    S->>S: 记录历史
~~~

如果撤单尚未确认或成交明细尚未齐，重新决策只会继续等待。PaperExchange 的即时回报作为 Executor 调用结果处理，不通过同步回调重入 Shard。

### 5.4 status 与 history

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

### 5.5 stop

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
    S->>S: 停止策略新动作，结束行情和定时协程
    S->>E: 终止模拟活跃订单
    E-->>S: 订单及账户回报
    S-->>A: Shard 线程结束
    A->>D: flush 并关闭
    A->>A: 关闭 Control socket
~~~

accepted 仅表示停止请求已受理；最终历史落盘由 Runtime 在 Shard 退出后完成。
