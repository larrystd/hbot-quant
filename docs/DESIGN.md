# hquant 重设计方案

状态：设计稿，尚未迁移代码。参考基线为本仓库 redesign 分支的 aa109b7：它从原仓库 poc 分支克隆，并包含 2026-10-05 原仓库工作区的未提交改动。[当前实现](README.md)只说明基线行为；本文规定重设计后的目标。

## 1. 目标和边界

第一阶段交付一个能运行的模拟交易系统：文件回放或 Binance 公开行情，1～8 个 Shard，每个 Shard 固定一个市场、一个模拟账户、一个 Simple PMM 策略；保留 SQLite 订单历史，以及 status、history、stop 管理接口。行情来源与是否真实下单是两个独立概念。第一阶段始终使用模拟交易所，Binance 公开行情不会触发真实订单。

保留已有的 Decimal 金额计算、ticks/lots 整数盘口、深度序号同步、订单回报去重、静态资金预算和异步 SQLite 写入。目标是重新划清代码所有权并修正事件顺序，而不是替换这些已经有用的行为。真实下单、账户推送、恢复对账在单独阶段设计；第一阶段的运行图里不放空的 LiveTrading、路由器或模式接口。

这次重设计针对基线里能定位到的混乱：

| 当前代码 | 具体问题 | 目标 |
| --- | --- | --- |
| base/market.h、base/order.h | 行情数据、交易规则、订单数据、Gateway 接口和模拟交易接口混在两个头里。 | 值类型进入 model，状态和行为进入 engine，行情协议进入 feed。 |
| application/config.h、quant_server.cc | 平行配置数组重复引用 ID，实际只允许单元素，创建时连续取 front()。 | 每个 ShardConfig 直接包含唯一的市场、账户、PMM 和预算。 |
| shard/shard.h、shard.cc | Shard 同时持有 HTTP/WebSocket、线程、盘口、策略、回报队列和历史记录。 | 网络归 feed；Shard 只拥有串行交易状态；历史序号归 HistoryRecorder。 |
| StrategyId、BalanceUpdate、PublicTrade | 策略 ID 混入名称；余额更新只是 Balance 别名且被忽略；公开成交方向可空但 Shard 又强制要求它存在。 | 身份、算法、余额事件和必填字段各自明确。 |
| SimplePmm、ActionExecutor、Shard::RunStrategy | 同一批次先撤单再下单，却在撤单回报处理前检查可用资金；新单资金关联又在同步回报处理后才写入 holds_。 | 撤单确认后重新计算报价；提交前登记资金和订单；回报统一排队。 |

## 2. 总体结构

~~~text
┌──────────────────────┐    标准化行情事件     ┌─────────────────────────────────┐    历史记录/状态快照    ┌─────────────────────┐
│ 行情输入             │ ──────────────────▶ │ Shard Engine（每分片一个所有者）│ ───────────────────▶ │ SQLite / Control    │
│ Replay / Binance     │                      │ Book → PMM → OrderFlow → Paper   │                      │ 写入、查询、停止      │
│ 解析、重连、快照获取 │ ◀── 重新取快照命令 ─── │ Tracker、Risk、账户视图         │                      │                     │
└──────────────────────┘                      └─────────────────────────────────┘                      └─────────────────────┘
~~~

行情模块负责连接、协议解析和取得快照，不拥有订单簿。Shard Engine 独占可变的盘口、策略、订单、资金冻结和模拟账户处理顺序。SQLite 和 Control 在旁路工作，不阻塞每一次策略决策。app 只装配和关闭这三块，不处理行情或交易规则。

## 3. 目录与依赖

~~~text
apps/
  hquant_server.cc                 # 仅解析参数、启动 Runtime
hquant/src/
  model/                           # 跨边界的值类型；无网络、线程、SQLite
    decimal.{h,cc}
    ids.h                          # 单一含义的 ID
    market.h                       # MarketId、MarketSpec、刻度、规则
    market_event.h                 # 快照、增量、公开成交、行情故障
    order.h                        # 意图、订单、订单状态、成交
    account_event.h                # 独立的余额变更
    time.h
  feed/                            # 行情输入
    replay.{h,cc}
    binance_public.{h,cc}
    binance_codec.{h,cc}
    net/                            # 仅 feed 使用的 HTTP/WebSocket 实现
  engine/                          # Shard 独占的交易状态
    shard.{h,cc}
    event_queue.{h,cc}
    book.{h,cc}
    simple_pmm.{h,cc}
    order_flow.{h,cc}
    order_tracker.{h,cc}
    risk.{h,cc}
    paper_exchange.{h,cc}
    history_recorder.{h,cc}
    status.h
  storage/
    history.h                      # 记录和查询 DTO、非阻塞写入端口
    sqlite_writer.{h,cc}
    sqlite_reader.{h,cc}
    record_codec.{h,cc}
    legacy_codec.{h,cc}            # 旧库读取专用
    schema.sql
  control/
    server.{h,cc}
    protocol.{h,cc}                # JSON 与类型化请求/响应互转
  app/
    config.{h,cc}
    runtime.{h,cc}
hquant/test/
  model/ feed/ engine/ storage/ control/ app/
~~~

依赖方向固定为 model → feed/engine/storage/control → app；engine 可以使用 storage/history.h 的非阻塞写入端口，storage 不包含 Shard。control 只认识类型化状态和历史查询，由 app 连接到实际组件。每个值类型只有一个定义位置；不再用 base/market.h、base/order.h、base/types.h 容纳不相干的类型，也不把网络工具放进公共 base。Bazel 目标按目录拆分，禁止环依赖。

## 4. 配置与结构体

当前 AppConfig 用 assignments、accounts、market_specs、strategy_configs 等平行数组反复写同一组 ID；校验又要求每组只有一个元素，运行时大量取 front()。目标配置直接表达实际约束：

~~~cpp
struct StrategyId { uint64_t value; };     // 仅身份；算法种类不属于 ID
struct ShardConfig {
  ShardId id;
  MarketConfig market;                     // 市场 ID、base/quote、刻度、交易规则
  PaperAccountConfig account;              // 账户 ID、初始余额、模拟手续费
  PmmConfig pmm;                           // strategy_id、价差、数量、刷新周期
  RiskLimits risk;                         // 本分片各资产的静态额度
};
struct AppConfig {
  FeedConfig feed;                         // Replay 文件或 Binance 公开行情
  StorageConfig storage;
  ControlConfig control;
  std::vector<ShardConfig> shards;
};
~~~

配置版本升为 2。加载时验证：Shard 数量 1～8；ID 唯一；StrategyId 仍满足客户端订单 ID 编码的 48 位限制；账户不跨 Shard；市场、刻度、规则、资产和资金预算自洽；策略参数合法。旧 YAML 必须明确迁移，不做悄悄猜测或运行时 front() 取值。示例配置随代码迁移一起更新。

配置 v2 的最小形状和字段名如下；同一个市场、账户、策略信息不能再分散到多个顶层数组：

~~~yaml
schema_version: 2
feed:
  kind: replay
  file: examples/replay_market.json
storage:
  path: state/orders.sqlite3
control:
  state_dir: state
shards:
  - id: 0
    market:
      exchange: binance
      symbol: BTCUSDT
      base: BTC
      quote: USDT
      price_per_tick: "0.01"
      amount_per_lot: "0.000001"
      stale_after_ms: 5000
      rule:
        price_increment: "0.01"
        base_increment: "0.000001"
        min_base_amount: "0.000001"
        min_order_value: "5"
    account:
      id: paper-0
      balances: {BTC: "1", USDT: "1000"}
      maker_fee_rate: "0.001"
    pmm:
      strategy_id: 1
      order_amount: "0.01"
      bid_spread: "0.001"
      ask_spread: "0.001"
      refresh_ms: 1000
    risk:
      budgets: {BTC: "1", USDT: "1000"}
~~~

类型边界也按用途切开：

| 类型 | 目标含义 |
| --- | --- |
| MarketEvent | BookSnapshot、BookDiff、PublicTrade、FeedFault。保留 connection_id、连续序号和 tick_lot_version；PublicTrade.side 必填，解析失败就在 feed 拒绝。 |
| OrderIntent / Order | 前者仅是策略要买卖的价格和数量；后者是分配了 client_order_id、账户、市场和策略 ID 的不可变订单。 |
| OrderStatusReport / FillReport | 状态与成交分开。状态可先于成交明细到达，Tracker 根据累计成交量等待缺失的 Fill；成交按 exchange_trade_id 去重。 |
| BalanceUpdate | 独立结构体，含账户、资产、total、available、事件时间；不再是 Balance 的别名，也不会被 Shard 忽略。 |
| HistoryRecord | 只包含当前写入的动作、订单、状态、成交、余额和缺口；v1～v3 旧记录只在 storage/legacy_codec 中解码。 |

Book、OrderTracker、RiskGate 是有状态对象，不混进 model。RiskGate 自己保存 client_order_id → 资金冻结的关联；Shard 不再维护第二份 holds_ 映射。PaperExchange 保存模拟账本并发出初始余额和后续 BalanceUpdate；策略只从 Shard 的账户视图读取已处理的余额事件。

## 5. 单分片事件顺序

所有进入 Shard 的事件先进入有界队列，在该 Shard 的一个执行线程上按入队顺序处理。回放由虚拟时钟按 (at_us, ordinal) 推进同一处理函数；实时行情由 feed 投递，定时事件由 app 的调度器投递。Shard 不创建 HTTP/WebSocket 或 Asio 连接。队列溢出、连接中断、深度序号缺口都会让盘口进入不可下单状态，并向 feed 请求重新取快照；队列已满时通过保留的故障标记通知 Shard，不能静默丢一个增量后继续报价。

1. **盘口事件：** BookSync 校验连接、刻度和序号，应用更新；PaperExchange 根据最新盘口撮合旧单；先处理其订单、成交、余额回报，再决定是否触发 PMM。公开成交也按这一顺序处理；它本身不等于本账户成交。
2. **策略刷新：** PMM 先检查盘口和账户是否可用。如果旧单存在，只发撤单请求并记住“待重新报价”；撤单请求不释放资金。等旧单确认结束、成交明细齐全、冻结释放后，用当时最新盘口和余额重新计算价格与数量，再提交新单。撤单失败或结果未确定时不抢先挂替代单。
3. **提交订单：** OrderFlow 量化并验证意图，先生成 client_order_id，按该 ID 冻结资金并登记 Tracker，然后调用 PaperExchange。PaperExchange 产生的回报追加到队列，不能在提交调用栈中重入 Shard。明确未发出的失败会回滚登记和冻结。
4. **账户回报：** Tracker 先去重并推进状态；确认的新 Fill 更新资金占用；真正结束后 RiskGate 释放冻结；BalanceUpdate 更新账户视图；最后产生策略重新评估请求。状态先报完成但成交明细未齐时保留冻结。
5. **定时与触发：** 定时器负责盘口过期和 PMM 刷新。节流只能合并重复触发，必须保留待重新报价状态，不能像当前最多补跑两轮后直接丢掉剩余触发。

HistoryRecorder 是每个 Shard 唯一的历史序号和缺口所有者。OrderFlow、Tracker 和 Shard 不各自实现 Record/Gap；它们把结构化结果交给 HistoryRecorder，由它分配 shard_sequence 并非阻塞地投递 SQLite writer。

## 6. 线程、存储和管理

实时模式中，每个 Shard 一个状态线程；feed 的网络 I/O 与 Shard 状态线程分开；SQLite 单 writer 线程、单 reader 线程；Control 单独处理 Unix socket。回放模式使用虚拟时钟逐条驱动 Shard，输入顺序和结果可重复。app/Runtime 创建组件并持有生命周期：先验证配置和打开数据库，启动 Shard 与管理端，再启动 feed；停止时先停 feed，通知 Shard 完成队列处理并汇合，然后 flush writer，关闭 reader 和管理 socket。

SQLite 写入保持有界、批量、WAL。队列满或持久化失败时记录精确的 (run, shard, first_seq, last_seq) 缺口并反映在状态中。新写入记录使用 v4；旧 v1～v3 数据库只通过 legacy_codec/reader 读取，不把迁移字段放回 engine 的活动事件类型。Control 保留 status、history、stop 请求；status 先收集每个 Shard 的类型化 StatusSnapshot（盘口状态、活跃订单、预算、错误、历史健康），再由 control/protocol 统一编码 JSON，不能拼接或删改已有 JSON 字符串。

## 7. 从当前文件迁移

| 当前文件或目录 | 目标处理 |
| --- | --- |
| base/types.h、base/market.h、base/order.h | 拆入 model 的小头文件；网关接口和交易所实现不属于 model。 |
| market/market_data_stream、market/replay_feed | 移入 feed；market/order_book 移入 engine/book。 |
| order/risk、order/order_tracker、order/simulated_exchange | 移入 engine，由 OrderFlow 串起登记、冻结、回报。 |
| shard/shard、shard/action_executor、strategy/simple_pmm | 收敛为 engine/shard、order_flow、simple_pmm；网络对象从 Shard 移走。未使用的 ShardCommand/ShardReport 不原样保留。 |
| shard/trading_mode、simulated_trading、live_trading、routing；order/order_gateway、account_reports | 第一阶段不进入新运行图。真实交易所接入时再基于实际账户回报和恢复流程单独设计。 |
| order_history | SQLite 和编解码进 storage；序号与缺口进 engine/history_recorder；旧格式进 legacy_codec。 |
| application/config、quant_server、control_server、launcher | 分成 app/config、app/runtime、control/server；Runtime 不再拼状态 JSON 或直接处理回放记录。 |
| base/net、net_server、line_stream、rate_limit | 仅把实际使用的传输代码放到 feed/net 或 control；未接入的限流和实盘代码不混入公共层。 |

## 8. 实施顺序与验收

1. **形状先落地：** 拆 model 类型和 AppConfig v2，建新目录与 Bazel 依赖；让 replay + Paper 的最小链路编译运行。迁移现有示例配置和可复用 fixture。
2. **修正核心顺序：** 实现 Shard 队列、OrderFlow、资金冻结归属和 PMM 撤单后重报；以回放逐条核对订单、成交、余额和历史序号。
3. **接回外部输入与旁路：** 移入 Binance 公开行情、BookSync 重同步、SQLite 旧库读取与 Control；完成多 Shard 启停和状态汇总。

验收必须覆盖：满额预算下撤单确认后成功重报；提交后立刻成交时冻结正确释放；订单完成状态先于成交明细；重复成交不重复扣款；盘口断档/队列溢出后禁止新单并重同步；多 Shard 回放精确路由；SQLite 缺口和分页可见；status/history/stop 在运行与停止过程中可用。最终通过 //apps:hquant_server 构建及 //hquant/test:all 测试，并确认旧配置得到明确错误、新配置示例可运行。每一步以可运行的回放链路为提交边界。
