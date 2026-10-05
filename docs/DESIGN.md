# hquant 1.0 设计

状态：逐项讨论中的设计稿。当前只定义第 1 项：Shard 的线程与协程模型。目录、配置、结构体和存储细节尚未定稿。参考代码基线为本仓库 aa109b7；[当前实现](README.md)记录基线行为。

## 第 1 项：一个 Shard，一个线程

Runtime 可以创建多个 Shard。每个 Shard 独占一个 OS 线程和一个 boost::asio::io_context；该 io_context 只由这一个线程运行。Shard 内的行情、策略、Executor、盘口、订单、风控和模拟账户都归这个线程处理。不同 Shard 不共享可变交易状态。

~~~text
Runtime
  ├─ Shard 0 ── 线程 0 / io_context 0
  │    ├─ Market     行情协程 + 盘口
  │    ├─ Strategy   策略状态和决策
  │    ├─ Executor   下单、撤单、订单状态、风控、模拟交易
  │    └─ Shard::Run 事件处理协程
  ├─ Shard 1 ── 线程 1 / io_context 1
  │    └─ 同样的内部结构
  └─ SQLite / Control：Shard 外部的辅助组件
~~~

Shard 是线程与生命周期的所有者，也是内部模块的装配者。Market、Strategy、Executor 是 Shard 的成员模块；它们不各自启动线程。SQLite writer 和 Control 可以有自己的线程，但只能通过投递请求、事件或快照与 Shard 交互，不能从外部线程直接读取或修改 Shard 的交易状态。

### 协程如何分工

~~~text
同一个 Shard 线程 / 同一个 io_context

Market::Run() ── co_await 网络或回放输入 ──┐
TimerLoop()  ── co_await 定时器 ──────────┼──▶ Shard 事件队列
Control 请求 ── asio::post ──────────────┘          │
                                                     ▼
                                    Shard::Run() ── co_await 下一个事件
                                                     │
                          Market::Apply → Executor::OnMarket
                                      → Strategy::Decide → Executor::Execute
                                                     │
                                      回报处理、状态记录、下一次触发
~~~

Market::Run 负责 WebSocket/REST 的异步读取、解析、重连，或读取回放记录；它只把输入投递到 Shard 事件队列。Market 内的盘口由 Shard::Run 调用 Market::Apply 更新。这样 Market 的 I/O 协程可以挂起，盘口也不会在策略决策中途被另一条协程改动。

Shard::Run 是交易状态的唯一处理入口。它等待事件时可以 co_await；取到事件后，同步完成该事件的盘口更新、模拟撮合、账户回报、策略决策和动作执行，再等待下一个事件。Strategy::Decide 是本地计算，不需要为了形式而写成协程。v1.0 的模拟 Executor 也同步完成一次动作；将来若有异步交易所请求，完成结果必须作为新事件回到 Shard::Run。

同一线程上的协程会在 co_await 处交错执行，因此“一个线程”本身不足以保证处理顺序。规则是：**Market::Run 不改交易状态；Shard::Run 处理一个事件的过程中不 co_await，也不允许回调重入。** 模拟交易所产生的订单、成交、余额回报先作为结果返回，由 Shard::Run 按顺序处理。外部控制请求先 post 到该 io_context，再进入事件队列。

### 一个行情事件的处理顺序

1. Shard::Run 取出一条行情事件，调用 Market::Apply 更新盘口和同步状态。
2. Executor 根据新盘口或公开成交撮合已有模拟订单，返回订单、成交和余额回报；Shard::Run 先处理这些回报。
3. Strategy 读取此时的盘口、活跃订单、可用余额，产生动作。
4. Executor 检查规则和资金、登记订单与冻结，再执行下单或撤单。执行产生的即时回报在本轮末尾处理，不直接回调 Strategy。
5. 回报若要求再次决策，放入下一条内部触发事件；不在当前调用栈递归运行策略。

撤单申请不等于撤单完成。Simple PMM 要等旧单终态和资金释放，再基于最新状态重算报价。提交新单前要先完成订单登记和资金关联；即时成交不能抢在登记之前进入回报处理。这两点在后续 Executor 模块设计中细化。

### 回放与验证

回放也驱动同一个 Shard::Run 事件处理协程，使用虚拟时钟；读取下一条回放记录前，等待上一条在所属 Shard 中处理完成。每个 Shard 仍由自己的线程处理。多 Shard 文件的路由和跨 Shard 顺序留到行情模块设计时确定。

当前实时路径的 Shard::StartFeed 已经为每个 Shard 建 io_context/线程，并在其上 co_spawn 行情和定时协程；这部分是可沿用的方向。当前 QuantServer::RunReplay 直接在启动线程调用 Shard::OnSnapshot/OnDiff，和实时路径不同；当前 SimulatedTrading 的同步回调还需要 RunStep/queued_reports_ 防重入。迁移时应让两种输入走同一个事件入口。

这一项的验收条件：

- 实时、回放和 Control 投递最终都在所属 Shard 线程处理交易状态。
- Market I/O 挂起或恢复时，不会在一次策略决策中途改变盘口。
- 即时成交、撤单回报不会同步重入 Strategy 或抢在订单和资金登记之前处理。
- 一个 Shard 的阻塞或故障不直接读写另一个 Shard 的状态。

下一项再定 Market、Strategy、Executor 各自的具体字段、接口和目录；在这项线程与事件所有权确定前，不预先固定那些结构。
