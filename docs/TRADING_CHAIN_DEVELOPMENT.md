# hquant 账户级交易链路开发文档

状态：P0–P4 的本机实现和 P5 本机故障、30 秒及 180 秒验收已完成；非生产交易所账户验证尚未进行，`live` 仍禁止启动。实测与文件变更见[首轮实施记录](TRADING_CHAIN_IMPLEMENTATION.md)及[后续开发记录](TRADING_CHAIN_FOLLOWUP.md)。本文保留原开发契约；架构与前后目录全图见[交易链路设计](TRADING_CHAIN_DESIGN.md)。

## 1. 起点、目标和边界

| 项目 | 当前代码 | 目标代码 |
| --- | --- | --- |
| 账户归属 | `Shard` 各自持有 `PaperExchange`；配置拒绝账户复用 | `Runtime` 每个账户构建一个 `TradingAccount`，多个 Shard 引用同一账户 |
| 下单语义 | `Executor::Dispatch` 同步调用 `PaperExchange::Place/Cancel` | Shard 提交意图；账户统一预留资金、分配 ID、异步发送；交易所回报决定最终状态 |
| 撤单换单 | `RunDecisionCycle` 撤单返回后立即再决策 | 撤单进入 `PendingCancel`；确认释放资金后通知所属 Shard 再决策 |
| 订单与资金 | Paper 自己保存活动订单和余额；`OnAccountEvent` 主要写历史 | `OrderState` 和 `FundsState` 由账户串行更新；Paper 和外部交易所都发标准化回报 |
| 外部交易 | 只有公开行情连接，`live` 校验时被拒绝 | 一个账户复用交易 REST 连接和一条私有 WS；第一轮只连本机模拟交易所 |
| 历史和恢复 | `SqliteHistory::TryPush` 可产生缺口，不能做恢复依据 | 历史继续用于查询；实盘另有可靠 `OrderJournal`，重启先同步后开新单 |

第一轮不改策略报价公式和原始行情样本。`external_sim` 使用 Binance Spot 的交易请求及账户事件格式，模拟交易所保存独立账本。`live` 在第一轮仍拒绝启动。Paper v1 配置与回放继续可用。

## 2. 开发后的目录和依赖方向

现有交易文件主要在 `src/shard/`：`shard`、`executor`、`strategy`、`market`、`binance_feed` 和 `paper_exchange`。配置、Runtime、SQLite、Control 位于 `src/` 根目录。目标目录如下；`★` 是首个外部链路节点，`◇` 是后续实盘能力节点，`→` 是移动。

~~~text
hquant/
  src/
    config.h / config.cc                修改：schema v2、账户引用与模式校验
    runtime.h / runtime.cc              修改：按账户组装、路由和生命周期
    sqlite_history.h / sqlite_history.cc 修改：账户观察历史
    control_server.h / control_server.cc 修改：账户快照与控制
    main.cc / perf_probe.h              修改：入口和分段指标
    base/
      ids.h                              修改：区分本地、交易所和历史 ID
    shard/
      shard.h / shard.cc                 修改：事件驱动决策、账户视图
      executor.h / executor.cc           修改：不再直接读写 Paper
      strategy.h / strategy.cc           修改：异步决策门禁
      market_type.h / market.cc          修改：中立类型和 bench_seq
      binance_feed.h / binance_feed.cc   修改：来源序号透传；公开行情职责不变
      BUILD.bazel
    trading/
      trading_event.h                   ★ 中立命令、回报、余额事件
      trade_port.h                      ★ 异步发送和查询边界
      order_state.h / order_state.cc    ★ 订单生命周期与成交去重
      funds_state.h / funds_state.cc    ★ 账户级资金与预留
      trading_account.h / trading_account.cc ★ 账户唯一写入者
      order_journal.h / order_journal.cc ◇ 可靠意图与回报
      trading_account_sync.cc          ◇ 启动、断线、未知状态同步
      risk_limits.h / risk_limits.cc   ◇ 账户风险政策
      BUILD.bazel
    venue/binance/
      trade_client.h / trade_client.cc ★ 签名 REST、异步发送与查询
      user_stream.h / user_stream.cc   ★ 私有 WS 与事件解包
      auth.h / auth.cc                 ★ 签名和请求时间
      request_scheduler.h / request_scheduler.cc ◇ 请求额度与退避
      rules.h / rules.cc               ◇ 动态交易规则
      BUILD.bazel
    sim/
      paper_exchange.h / paper_exchange.cc → 账户级 Paper 交易所账本
      BUILD.bazel
  bench/
    live_ws_bench.py                    修改：保留原行情 case，启动完整链路
    mock_binance_server.py             ★ 独立 HTTP 下单与私有 WS
    mock_venue_ledger.py               ★ 模拟交易所自己的订单、成交、余额
    validate_trade_chain.py            ★ 三方账本和历史核对
  test/
    order_state_test.cc / funds_state_test.cc / binance_auth_test.cc ★
    e2e_trade.py / run_trade_e2e.cc    ★ 真 socket 链路
    order_journal_test.cc / trading_recovery_test.cc / fault_trade.py ◇
    BUILD.bazel                         修改：注册各阶段测试
~~~

`trading/` 不包含 `shard/`、`sim/` 或 `venue/` 的实现头；`venue/binance/` 不直接修改订单或资金状态。`Runtime` 负责注入 `TradePort`，并把账户视图投递给相关 Shard。`src/` 根目录不新增交易类文件。`OrderState` 是组件名，旧的订单状态枚举改叫 `OrderStatus`；账户同步方法名用 `TradingAccount::SyncFromVenue`，不引入 `Reconciler` 类。

## 3. 先固定跨模块契约

### 3.1 ID、意图和回报

`ClientOrderId` 在下单前由安全随机源生成，格式为 `hq` 加 128 位随机值的十六进制编码；生成失败就拒绝本地下单。账户、运行和本地序号分别保存在日志与历史中，不依赖客户端 ID 编码这些信息。`ExchangeOrderId` 是交易所回报后才有的另一字段。原 `OrderId` 数值类型只用于本地订单引用，不能充当交易所 ID。账户键至少包含 `account_id + client_order_id`；成交去重键包含 `account_id + symbol + exchange_trade_id`。

中立值类型放在 `trading/trading_event.h`：`OrderIntent`、`CancelIntent`、`OrderReport`、`Fill`、`BalanceSnapshot/BalanceDelta` 与 `AccountEvent`。意图包含 `account_id`、`shard_id`、`symbol`、`decision_id`、可选的 `source_bench_seq`、订单参数和本地生成时间。没有行情来源的定时器决定不填 `source_bench_seq`。所有价格、数量、费用在边界精确解析为现有定点整数，保留费用资产；计算使用检查溢出的中间值。

`TradePort` 只暴露异步 `Submit`、`Cancel` 和 `Query`。方法返回“本地是否接纳到有界发送队列”，完成回调带交易所 REST 结果或查询结果；它不能直接把订单置为 Accepted，也不能修改 FundsState。私有 WS 经 `BinanceUserStream` 解包为 `AccountEvent` 后投递到同一个账户执行序列。Paper 实现同一 `TradePort`，另外接收行情撮合通知。

| 边界 | 输入 | 返回／输出 | 禁止的隐含语义 |
| --- | --- | --- | --- |
| Shard → TradingAccount | `OrderIntent` 或 `CancelIntent`，含决策和来源 ID | 本地接纳／拒绝结果；随后收到账户视图版本 | 入账户队列不等于交易所接受 |
| TradingAccount → TradePort | 带 `client_order_id` 的命令或查询 | 入发送队列结果；异步 REST／查询回调 | `Send` 成功不等于接受或撤单完成 |
| UserStream／Paper → TradingAccount | 标准化 `AccountEvent` | 账户内顺序应用，发布新视图，写观察历史 | 不由适配器直接改资金 |
| TradingAccount → Shard | 与该标的有关的只读账户视图、版本、变更原因 | Shard 在本线程决定是否继续策略 | 不在账户线程直接运行 Strategy |

一个策略决定可能有两笔订单。账户逐笔做最终资金检查和提交，逐笔返回结果；第二笔失败不得回滚已经送出的第一笔。Shard 的本地预算只用于减少无效请求，账户里的检查才有权威性。

### 3.2 账户视图和事件门禁

`ExecutionView` 扩展为该标的的未终结订单及其提交／撤单阶段、账户级可用资金、账户健康状态和单调 `account_revision`。Shard 只持有副本。`PendingSubmit`、活动、`PendingCancel` 和任何结果未知的订单都应阻止同一策略周期重复挂单；`PendingCancel` 不再生成第二个撤单。取消确认并释放预留后，账户发一个新的 revision，Shard 再在当前盘口上决策。

当前 `Strategy::Decide` 在构造 SubmitOrders 前就推进 `next_refresh_at_`。改造时要让刷新时间只在订单意图被账户接纳后生效；本地队列满、规则拒绝或账户暂停时允许以后重试，但要做节流，避免每笔行情都重复请求。策略配置更新也复用相同门禁，不假设撤单瞬间完成。

## 4. 订单状态机与资金状态机

订单主状态建议为 `PendingSubmit`、`Accepted`、`PartiallyFilled`、`Filled`、`Rejected`、`Cancelled`。撤单是与订单主状态正交的操作阶段：`None`、`PendingCancel`、`CancelUnknown`、`CancelRejected`。下单超时用 `submit_unknown` 标志保留 `PendingSubmit`；超时从来不等于交易所拒绝。一个订单即使处于 `PendingCancel`，仍可能继续成交。

| 收到的事实 | 订单／撤单阶段 | 资金动作 | 后续动作 |
| --- | --- | --- | --- |
| 账户入口或发送队列拒绝 | 不生成可发送订单 | 释放尚未提交的本地预留 | 把失败回给 Shard，按节流规则重试 |
| 发送队列接纳 | `PendingSubmit` | 保留全额预留 | 等待 REST 或私有回报 |
| 明确的接受回报 | `Accepted`，保存 `exchange_order_id` | 保留剩余订单占用；待余额证据确认锁定已反映 | 更新视图；不因接受而下第二笔重复订单 |
| 明确的拒单 | `Rejected` | 释放本地预留 | 记录拒因；通知 Shard |
| 首次有效成交 | `PartiallyFilled` 或 `Filled` | 按剩余量改预留；按成交数量和费用资产记账 | 用交易所成交 ID 去重 |
| 撤单请求接纳 | 主状态不变，撤单为 `PendingCancel` | 保留剩余量占用 | 禁止重复撤单和立即换单 |
| 撤单确认 | `Cancelled`，撤单完成 | 释放剩余占用；核对最终累计成交 | 通知 Shard 重新决策 |
| 下单或撤单超时／断线 | 保留原主状态，标记对应操作未知 | 不释放预留 | 按原 client_order_id 查询订单、成交和余额 |
| 撤单与成交竞态 | 根据最终累计成交成为部分成交、全成或已撤 | 只对新成交和最终剩余量调整一次 | 查询结果与推送归并后再允许换单 |

REST 明确响应和私有 `executionReport` 都可能先到。按交易所订单 ID、客户端订单 ID、成交 ID 和累计成交量合并，终态不能被晚到的旧状态倒退；无法解释的旧回报触发同步。查询“订单不存在”单次结果不足以证明订单没有成交，也不能用另一个 client_order_id 盲目重发。账户事件包括 `outboundAccountPosition`、`balanceUpdate`；当前 Spot 文档还列出 `externalLockUpdate`，实盘阶段必须将它纳入资金核对和暂停条件。协议依据：[Spot User Data Stream](https://developers.binance.com/en/docs/products/spot/user-data-stream)。

### FundsState 的账本规则

FundsState 每资产保存交易所报告的 `free/locked/total`、报告时间／版本、由已应用成交形成的本地投影，以及每个未终结订单的本地预留和“交易所锁定是否已被证实反映”标记。它是 TradingAccount 内的纯状态组件，没有线程、socket 或独立的交易所余额账本。

资金检查在账户执行序列上原子完成：买单预留报价资产的限价成本及费用余量，卖单预留基础资产；两个标的争同一报价资产时按账户顺序扣除。计算可再用资金时，只从交易所 `free` 扣尚未证明已反映的本地预留及不确定占用。交易所 `free=40, locked=60` 且已证实包含订单 A 的 60 时，不能再从 40 减一次 A；余额消息晚到或不能归因时宁可保留预留、暂停新单并查询，不做乐观释放。

`executionReport` 的成交是订单与资金的增量事实；`outboundAccountPosition` 是所列资产的绝对余额报告，不能把同一成交再加一次，也不能因其只含变动资产就覆盖全账户。`balanceUpdate` 和 `externalLockUpdate` 可能来自本策略之外的资金变动，差额无法解释时整个账户离开 Ready。任何时刻要求：预留非负、累计成交不超过下单量、可发送新单所需资产不超过保守可用额、同一成交只影响资金一次。

## 5. 线程、队列和回放完成边界

每个 TradingAccount 固定归属一个 Asio 执行序列，订单状态、资金状态和账户序号只有它能写；Shard 继续使用现有至多 8 个行情工作线程。账户入口、交易发送、私有事件与历史队列都有显式容量。容量在配置中定义，压测输出峰值、拒绝数和等待时间；撤单与查询保留发送槽位，不被大量新单占满。Shard 把值对象入账户队列就返回，不等待 HTTP 或 SQLite。

私有事件入口不能静默丢回报。入口逼近容量时停止新单，暂停或重建私有订阅，并在恢复后查询补漏。发送队列满时尚未发包的订单不得留作“交易所状态未知”；账户返回本地拒绝并释放预留。已写入可靠日志但发送结果未知的订单必须查询，不能据本地队列状态推断交易所状态。

Paper 模式的原顺序是“Market 更新 → Paper 撮合 → Strategy 决策”。Paper 移到共享账户后，Shard 应使用异步续接保持这个顺序：Market 更新后把本次行情和决策 ID 投给账户；账户驱动 Paper 撮合、应用全部回报并返回新视图；Shard 再做策略决定、提交命令。回放读取器现有的逐条完成回调要等本次 Paper 撮合、订单／账户事件应用和观察历史入队完成，才能读取下一条输入；不能在行情刚进队时就回调完成。实时外部模式可以用最近一次账户视图决策，最终资金检查仍在账户内完成。

账户视图更新用 revision 去重。Shard 在待接纳决定未回调、撤单未决和本轮已发意图时，不因每条行情重复发同一命令；回调或终态事件只触发受控的一轮新决策。回调必须通过 Runtime 路由回所属 Shard 的执行序列，销毁和停机时不可再调用失效对象。

## 6. Binance 协议适配与模拟交易所

本轮固定协议子集：签名 REST 的限价 GTC 新单、按原 client_order_id 撤单、查订单、查成交和查账户余额；私有 WS 使用 Spot WebSocket API 的签名订阅，处理订阅确认及 `executionReport`、`outboundAccountPosition`、`balanceUpdate`。编码、签名、事件字段与错误分类由同一组 `venue/binance/` 代码处理真实对端和本机模拟对端。具体请求字段和认证样例以[Spot REST API](https://developers.binance.com/en/docs/products/spot/rest-api)及[Spot User Data Stream 订阅](https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/ws-api/user-data-stream)为准，提交适配器时将官方样例固定为协议测试夹具。

`BinanceTradeClient` 负责 REST 请求编码、固定数量的保活连接、异步 I/O、响应分类与查询。`BinanceUserStream` 负责订阅、重连、订阅状态和事件解包；它不读取公开行情。`BinanceAuth` 共享签名与时钟校验。网络错误、5xx、超时和解析失败要区分“明确拒绝”与“结果未知”；只有明确交易所事实才能改变订单终态。`external_sim` 只允许 loopback 端点，模拟密钥仅作测试签名验证；真实凭据通过外部引用提供，不能进入日志或历史。

`mock_binance_server.py` 在压力器进程内提供独立 HTTP 与私有 WS socket。`mock_venue_ledger.py` 自己维护订单、成交和资产 free/locked，不复用 quantserver 的 OrderState 或 FundsState。脚本至少覆盖：接受、明确拒绝、部分成交、完全成交、撤单、撤单后成交、成交后撤单、余额消息晚到、重复回报、查询补漏。第一个 30 秒门禁固定正常及竞态脚本；断线、丢响应、限流和进程崩溃放后续故障门禁。Mock 需要记录每个实际收到的 `client_order_id` 和 socket 接收时间，不能用客户端“已入队”的数量充当下单吞吐。

## 7. 历史、可靠日志和启动同步

### 观察历史：第一轮就要有

`SqliteHistory` 增加账户事件查询视图，至少保存 `run_id`、`account_id`、账户单调序号、`shard_id`、symbol、两个订单 ID、成交 ID、事件阶段、数量、费用资产、可选 `bench_seq` 和本地时间。旧 `history_records` 继续供 v1 读取；账户事件不能复制到 1000 个 Shard 各记一份。第一轮结束后 `validate_trade_chain.py` 检查账户事件序号连续、无 `history_gaps`、每个模拟交易所订单和成交都有对应的本地终态和历史记录。若历史队列满，压测失败；不能把缺口解释为业务成功。

### 可靠日志：实盘能力节点

`OrderJournal` 与可丢观察历史分开，持久保存意图、client_order_id、发包阶段、回报、成交去重键、账户序号和必要的资金投影。账户先预留资金，将 PendingSubmit／PendingCancel 意图放入有界日志队列，然后立即交给交易发送队列；不等待 SQLite COMMIT。回报去重、订单和资金状态变更、已应用序号仍在可靠提交后才发布。意图写失败要记录 `client_order_id`、暂停新单并查未决订单；已发出命令不能回滚为“未下单”。进程在意图提交前崩溃的恢复缺口须单独验收。日志写入在专用异步执行序列完成，不阻塞 Shard。

`TradingAccount::SyncFromVenue` 在启动、私有流重连、未知结果、余额差异时运行。顺序是：恢复日志的未终结订单与预留 → 建立私有订阅并缓存新事件 → 查询活动订单、未决订单、成交和余额 → 按 ID 去重并归并查询与推送 → 对账通过后切 Ready。查询中发现账户外订单或未解释余额时维持 Paused；绝不自动把陌生订单归为本策略。重启时凡是日志已提交而交易所结果不确定的命令，先按原 client_order_id 查，不能生成新 ID 重发。

## 8. 配置、控制与运行状态

配置升级为 `schema_version: 2`：顶层 `accounts` 包含账户 ID、`paper/external_sim/live` 模式、交易端点、私有流端点和账户级资金／风险配置；每个 Shard 只引用 `account_id`。v1 Paper 配置解析为每 Shard 一个账户，现有回放配置继续通过。`external_sim` 端点必须是 loopback；`live` 需要凭据引用、可靠日志路径、动态交易规则和风控配置，且在后续门禁通过之前仍被配置校验拒绝。测试初始余额只适用 Paper 和模拟交易所；实盘余额以交易所查询为准。

账户健康状态至少有 `Starting`、`Syncing`、`Ready`、`Paused`、`Stopping`。仅 Ready 且该标的行情新鲜时允许新单；非 Ready 状态仍允许撤单与查询。Control 第一轮增加只读分页账户快照：状态、私有流健康、未知订单、订单数、各资产 free/locked/预留、队列深度、最后回报与错误。后续增加 `pause_new_orders`、`cancel_all`、`sync_account_state`、`resume_trading` 和安全停机；恢复交易必须重新校验私有流、日志、资金和订单差异。

停机顺序：阻止新单 → 停策略定时器 → 送撤单意图 → 继续消费回报和查询至期限 → 可靠保存未决状态 → 关闭私有流和交易连接 → 排空历史。期限到达只报告 Unknown，不宣称撤单成功。

## 9. 分阶段提交计划

每个阶段结束应能构建并运行自己的验收测试，不把第一轮压测通过当作 live 可用。下表中的路径以 `hquant/` 为基准。

| 阶段 | 主要文件 | 交付与验收 |
| --- | --- | --- |
| P0 契约与配置 | `src/base/ids.h`，`src/config.*`，`src/trading/trading_event.h`，`src/trading/trade_port.h`，各 BUILD | 固定 ID、事件、异步接口和 schema v2；v1 Paper 配置仍能解析；v2 共享账户配置合法，live 仍拒绝 |
| P1 状态与 Paper | `src/trading/order_state.*`、`funds_state.*`、`trading_account.*`，`src/sim/paper_exchange.*`，`src/shard/{shard,executor,strategy,market_type}.*`，`src/runtime.*`，对应单测与回放 | 账户统一处理 Paper 回报；跨标的资金不超用；待撤单不重复；原回放逐输入确定且无历史缺口 |
| P2 外部接口与第一轮压测 | `src/venue/binance/{auth,trade_client,user_stream}.*`，`src/shard/{market,binance_feed}.*`，`src/sqlite_history.*`，`src/control_server.*`，`src/perf_probe.h`，`src/main.cc`，`bench/{make_market_cases,live_ws_bench,mock_binance_server,mock_venue_ledger,validate_trade_chain}.py`，`test/{e2e_trade.py,run_trade_e2e.cc}` | 同一 1000 标的、30 秒 case 经真实 socket 走完整订单链；三方逐项核对；分段延迟和 QPS 单独报告 |
| P3 可靠恢复 | `src/trading/{order_journal,trading_account_sync}.*`，`test/{order_journal_test,trading_recovery_test}.*` | 崩溃前后无重复新单或成交入账；未知结果保留占用并先查询；私有流断线时不发新单 |
| P4 实盘约束与控制 | `src/trading/risk_limits.*`，`src/venue/binance/{request_scheduler,rules}.*`，`src/control_server.*`，`src/config.*` | 动态规则、全账户资金和敞口、请求额度、控制动作通过故障测试；重跑原 30 秒 case，量化可靠日志成本 |
| P5 环境验证 | `test/fault_trade.py`，压测报告与运行手册 | 故障注入、长时间运行和非生产账户小规模验证通过后，才允许打开 live 配置门禁 |

按当前目录方案，P0–P2 约涉及 **27 个新增或迁移文件**（含测试和 BUILD；其中 Paper 两个文件为迁移），并修改约二十个已有文件；P3–P4 另增约 **12 个文件**。这是文件范围估计，最终按编译依赖和测试结果调整，不能把它理解成只需给现有 Executor 加一个 HTTP 调用。

## 10. 测试矩阵和第一轮通过条件

| 测试 | 必须观察的事实 | 失败条件 |
| --- | --- | --- |
| 状态机单测 | 私有回报先于 REST、重复成交、部分成交后撤单、撤单与成交竞态、超时未知 | 重复扣款、终态倒退、未知状态释放预留 |
| 资金单测 | 两个标的争同一资产；买卖不同资产；余额报告晚到；费用资产变化 | 账户超额接纳、交易所锁定与本地预留重复计算、负余额 |
| 协议测试 | 官方样例签名与字段、私有订阅确认、三类账户事件与错误分类 | Mock 专用 JSON 通过而真实协议样例不通过 |
| Paper 回放 | 原 `e2e_replay.py` 加共享账户用例；每次输入完成后账户状态已应用 | 同一输入多次回放订单／余额不同，或已有回放断言退化 |
| 真 socket e2e | HTTP 收单、私有 WS 事件、查询、Control 快照、SQLite | 只在进程内 Paper 产生命令，或把入队当成交付 |
| 30 秒压力 case | 原 1000 标的样本和 bench_seq，模拟交易所确实收到非零挂单／撤单；事件脚本覆盖各状态 | 任一订单、成交、余额、历史序号对不上，或队列无界／丢私有回报 |
| 恢复与故障 | 日志已提交但未发包、已发包未收到响应、回报未提交、断线、磁盘失败 | 生成新 ID 重发、在差异未解释前开新单、无法查询／撤单 |

第一轮分别输出行情发送→Shard 同步处理完成、行情发送→模拟交易所收到对应挂单／撤单、私有事件发送→TradingAccount 应用、触发行情→对应确认或成交已应用。`client_order_id` 关联命令和回报，`source_bench_seq` 只标注确由行情触发的订单；一条行情可对应零单或多单。订单延迟只统计真实产生的命令，报告样本数及 p50/p95/p99，不用零单行情冲淡尾延迟。另报告行情、挂单、撤单、成交、各类回报 QPS、账户队列等待、发包时间、REST 响应时间、私有流积压及历史写入延迟。

30 秒停止行情后仍要排空未决回报和必要查询，然后由 `validate_trade_chain.py` 比较模拟账本、Control 账户快照和 SQLite。逐个核对 client/exchange/trade ID、订单累计成交与剩余量、每资产 free/locked/total、本地预留、账户历史连续序号和缺口计数。第一轮通过标准是**正确性零差异、无未解释 Unknown、无观察历史缺口、队列容量从未突破上限**；性能报告先给实测分布和现有行情门槛，不凭空设交易 p99 目标。P3 完成后用同一 case 重测可靠日志路径。

## 11. 开发时的检查清单

1. 编译 `//hquant/src:hquant_server`；运行现有 `//hquant/test:end_to_end`，作为每阶段回归基线。
2. 每次移动中立类型后检查 Bazel 依赖方向：`trading_event` 不依赖 shard、sim 或 venue；TradingAccount 不包含 Binance 或 Paper 实现头。
3. 每个会导致发送的路径都能指出唯一的账户级资金检查、队列接纳点和对应 client_order_id；每个释放预留的路径都能指出明确的交易所事实。
4. 每个网络超时、私有流断线和日志失败分支都能说明账户状态、是否允许新单、是否仍可撤单／查询。
5. 每条压测订单能从 `bench_seq`（若有）追到客户端意图、模拟交易所收包、私有回报、账户应用和 SQLite；无订单的行情只参与行情指标。

本文件是实施顺序和验收契约；后续若 Binance 官方字段或订阅方式变化，先更新协议夹具、适配器测试与模拟交易所，再更新本文对应接口，不让 Mock 协议单独漂移。
