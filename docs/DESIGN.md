# hquant 1.0 核心设计

v1.0 以 Shard 为交易核心。一个 Shard 独占一个线程、一个 boost::asio::io_context，内部包含 Market、Strategy、Executor。Runtime 负责创建和关闭 Shard；SQLite 与 Control 是外围组件。

## Shard 的形状

~~~text
Runtime
  ├─ Shard 0：线程 0，只运行 io_context 0
  │    ├─ Market    行情连接、解析、盘口
  │    ├─ Strategy  策略状态和报价决策
  │    ├─ Executor  下单、撤单、订单状态、资金、模拟交易
  │    └─ Shard     事件队列与处理顺序
  ├─ Shard 1：线程 1，只运行 io_context 1
  │    └─ 相同结构，独立交易状态
  └─ SQLite / Control
~~~

一个 Shard 内的 Market、Strategy、Executor 都在所属 io_context 的同一个线程上运行，不各自启动线程。盘口、订单、余额、风险占用和策略状态不跨 Shard 共享。其他线程要请求状态或停止 Shard，先把请求投递到该 Shard 的 io_context；其他线程不直接访问内部对象。

## 三个内部模块

| 模块 | 拥有什么 | 对 Shard 提供什么 |
| --- | --- | --- |
| Market | 行情连接与解析器、订单簿同步状态、L2 盘口、最近公开成交 | 把外部行情转成事件；应用一条事件后提供只读盘口视图和就绪状态 |
| Strategy | Simple PMM 参数、刷新时间、待撤单/待重新报价等策略状态 | 根据盘口、活跃订单和余额，给出下单或撤单意图；不直接调用交易所 |
| Executor | 订单登记与状态、资金冻结、模拟交易所和账户余额 | 检查并执行策略意图；根据盘口/公开成交撮合旧单；返回订单、成交、余额回报 |

Shard 本身只负责持有这些模块、驱动事件顺序和生命周期，不再把盘口算法、PMM 计算或订单风控写进 Shard::Run。

## 协程与状态处理

~~~text
同一个 Shard 线程 / 同一个 io_context

Market::Run() ── co_await WebSocket / REST / 回放输入 ──┐
TimerLoop()  ── co_await 定时器 ────────────────────────┼──▶ Shard 事件队列
外部请求     ── asio::post ────────────────────────────┘          │
                                                                 ▼
                                                Shard::Run() ── co_await 事件
                                                                 │
                                         Market::Apply → Executor::OnMarket
                                                       → Strategy::Decide
                                                       → Executor::Execute
~~~

Market::Run 是 I/O 协程：等待数据、解析消息、处理连接与重连，只向 Shard 投递已经解析的行情事件。Market 的盘口属于 Market 模块，但只能由 Shard::Run 调用 Market::Apply 更新。TimerLoop 也只投递定时事件。

Shard::Run 是交易状态的唯一入口。它等待下一条事件时可以 co_await；开始处理一条事件后，调用各模块完成全部状态变更，再等待下一条。Strategy::Decide 是本地计算；v1.0 的模拟 Executor 执行也是本地计算，因此这两处不需要人为引入 co_await。同一线程上的协程会在 co_await 处交错，所以一次事件处理中不挂起，也不允许同步回调重新进入 Shard::Run。

## 事件在 Shard 内怎样流动

行情事件到达后，Shard::Run 先调用 Market::Apply 更新盘口。Executor 再按新盘口或公开成交撮合已有模拟订单；订单、成交和余额回报先更新 Executor 的状态。随后 Strategy 读取最新盘口、活跃订单和余额，产生意图，Executor 检查资金与交易规则并执行。模拟交易产生的即时回报由 Executor 返回，仍在这一轮按顺序处理，不直接回调 Strategy。

订单回报或定时事件也进入同一个事件队列。回报若需要重新报价，形成后续策略触发；策略不会在 Executor 的调用栈中递归运行。撤单请求不会立即释放资金，旧单确认结束后 Strategy 再用最新状态计算新报价。新单发送前，Executor 已完成订单登记和资金关联。

回放与实时行情使用相同的事件处理入口。回放使用虚拟时钟，投递一条记录后等待 Shard 处理完成，再推进到下一条；实时行情使用 Shard 的 io_context 等待网络和定时器。两种来源都只改变 Market 的输入方式，不改变 Market → Strategy → Executor 的处理顺序。

SQLite writer 从 Shard 接收历史记录并异步写入。Control 把请求投递给目标 Shard，收到状态快照后再响应客户端。它们不参与 Shard 内部交易状态的直接修改。
