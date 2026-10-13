# hquant 交易链路设计 V2：账户级订单跟踪

状态：V2 本机实现与设计说明，基于 2026-10-11 工作区代码。`OrderManager`、按单查单、发送阶段、资金覆盖与外部模拟交易所已落地；本机及两笔真实 Binance Demo 虚拟资金成交的实测见第 8 节。旧版 [交易链路设计](TRADING_CHAIN_DESIGN.md)记录了从进程内 Paper 到外部交易接口的第一轮方案；Demo API 验证明细见[Binance Demo 验证记录](../operations/BINANCE_DEMO_VALIDATION.md)。当前交易模式只有 `pressure_sim`（压力器模拟）与 `online_sim`（Binance Spot Demo 在线模拟）；不提供真实资金实盘模式。旧文档中的 `live` 曾指预留的实盘模式，以此处定义为准。

## 与 V1 设计的区别

| 主题 | V1 设计 | V2 设计 |
| --- | --- | --- |
| 起点 | 每个 Shard 的进程内 Paper 下单，目标是建成共享账户和外部交易链路 | 共享账户、REST、私有 WS、可靠日志及全账户同步已在代码中；目标是整理订单跟踪并补齐边界语义 |
| 订单所有权 | `TradingAccount` 直接协调 `OrderState`、资金和回报 | `OrderManager` 专门管理订单、撤单请求、ID、期限和按单查询；`TradingAccount` 负责串行编排 |
| 结果路径 | 架构图主要展示外部下单和私有回报 | 明确画出 REST 结果、私有 WS、查单结果三路汇入 `OrderJournal`，再更新订单、资金和 Shard |
| SQLite | V1 将可靠日志列为后续能力，历史作为观察库 | 两份 SQLite 均已存在；V2 明确发单前意图提交、回报应用前事实提交，以及两侧观察历史的旁路写入 |
| 未知订单 | 依靠全账户 `SyncFromVenue()` 补漏 | 增加单笔期限和限速查单；全账户同步继续处理启动、断线及周期核对 |
| 资金占用 | 提出交易所余额与本地预留不能重复扣减 | 设计可核对的占用覆盖金额／版本、余额晚到处理和释放门禁 |
| Paper | 保留进程内 Paper 撮合与旧配置兼容 | 移除 Paper 模式和 schema v1 交易分支；外部模拟交易所通过真实 socket 完成正确性测试 |
| 验收 | 首轮 1000 标的、30 秒外部链路 | 在同一 case 上重新核对故障语义；真实 Demo 的成交路径另以单笔小额订单验收 |

## 1. 现状、目标与差距

当前 `schema_version: 2` 已允许多个 Shard 共用一个 `TradingAccount`。公开行情经 `Shard::OnMarket()` 驱动策略；`Shard::RunAccountDecision()` 把挂单、撤单交给账户。账户在 [`trading_account.cc`](../../hquant/src/trading/trading_account.cc) 中完成风险检查、调用 [`client_order_id.cc`](../../hquant/src/trading/client_order_id.cc) 生成随机 `client_order_id`、预留资金、记可靠意图、调用 `TradePort`；[`trade_client.cc`](../../hquant/src/venue/binance/trade_client.cc) 负责签名 REST 和有界异步发送队列；[`user_stream.cc`](../../hquant/src/venue/binance/user_stream.cc) 收私有 WS。REST 结果、私有事件和查询结果回到账户，先记 `OrderJournal`，再应用到 `OrderState`、`FundsState` 和账户视图。可靠日志和回放在 [`trading_account_journal.cc`](../../hquant/src/trading/trading_account_journal.cc)；启动、断线和周期全账户同步由 [`AccountSync`](../../hquant/src/trading/account_sync.cc) 执行；[`AccountViewPublisher`](../../hquant/src/trading/account_view_publisher.cc) 负责构造并推送 Shard 视图。两者由 TradingAccount 组合持有，运行在账户的同一 I/O 线程。

V1 外部链路曾在本机模拟交易所的 1000 标的、30 秒 case 中验证；V2 已重新运行同一 case，并用两笔 BTCUSDT 虚拟资金成交验证了真实 Binance Spot Demo 的挂单、私有成交回报、非空成交查询、余额对账及重启。配置值 `online_sim` 仅允许固定 Binance Demo 域名、Demo 凭据、一个账户和一个标的；示例默认零笔挂单，单次上限一笔。

V2 解决**订单跟踪的所有权和故障语义**。下表的左列是 V2 开发前的外部交易链路，右列是本轮实现：

| V2 开发前 | V2 本轮实现 |
| --- | --- |
| `TradingAccount` 直接持有 `OrderState`、本地 ID 映射、活动订单集合，并在 `ProcessSubmit`、`ProcessCancel`、`SubmitResponse`、`CancelResponse`、`ApplyEvent` 和 `RestoreJournal` 中分散处理订单状态 | 新增每账户一个 `OrderManager`，集中维护订单与撤单请求、ID 映射、超时、查询进度和去重；`TradingAccount` 保留单写入者、持久化和账户协调职责 |
| 未知结果触发 `SyncFromVenue()`；正常状态每 60 秒全账户同步 | 未决订单由 `OrderManager` 按单安排有界、退避的查询；全账户同步继续负责启动、断线和周期核对 |
| `FundsHold.reflected_by_venue` 是布尔值，外部模式不能精确表达交易所余额中已经覆盖多少预留 | 按资产和订单维护“本地尚未被交易所余额覆盖的占用”；只有经私有余额或同步快照核对后才消除重复占用 |
| `TradePort::Submit/Cancel` 的 `bool` 只表示本地队列接纳，后续只有最终 `Confirmed/Rejected/Unknown` | 发送端另报告“确定未发出／开始发包／结果未知”的阶段，供订单超时和停机恢复使用；入队绝不等于交易所接受 |
| `Shard::OnAccountView()` 主要靠“没有活动订单”决定撤单后能否换单 | 账户视图给出每个撤单请求的待确认、未知或已解决状态；确认订单终态且资金可用后才换单，拒撤后可重新查询或重试 |
| schema v1 的 Shard 直连 Paper 和 schema v2 的 `PaperTradePort` 两条本地撮合路径仍在构建 | 删除这两条路径和 `TradingMode::Paper`，订单正确性只用外部模拟交易所与 Demo 路径验证 |

## 2. 两条主链与补偿链

一个 Shard 对应一个行情标的；一个交易账户可以服务多个 Shard，但只维护一份订单投影、资金投影、交易客户端和私有回报连接。以下 `OrderJournal` 的两个方框指向**同一个** `order_journal.sqlite`；`SqliteHistory` 指 `history.sqlite`，是另一份库。

### 2.1 行情到下单／撤单

```text
┌───────────┐  ┌───────────────────────────────┐  ┌───────────────────────────┐
│ 公开行情 WS│→│ Shard::OnMarket / Strategy      │→│ TradingAccount 账户入口队列│
└───────────┘  │ 或策略定时器触发                │  │ 每账户串行、跨标的 RiskLimits│
               └───────────────────────────────┘  └─────────────┬─────────────┘
                                                                ↓
                                                   ┌───────────────────────────┐
                                                   │ ClientOrderId 生成器      │
                                                   │ 生成 128 位随机 ID         │
                                                   │ OrderManager 建立订单状态  │
                                                   └─────────────┬─────────────┘
                                                                 ↓
                                                   ┌───────────────────────────┐
                                                   │ FundsState                │
                                                   │ 检查共享可用额、预留资金    │
                                                   └─────────────┬─────────────┘
                                                                 ↓
                                                   ┌───────────────────────────┐
                                                   │ OrderJournal（SQLite）     │
                                                   │ SubmitIntent/CancelIntent  │
                                                   │ 入有界队列后即允许发命令    │
                                                   └────────┬───────────┬──────┘
                                                            │           └→┌─────────────────────────┐
                                                            │             │ SqliteHistory（SQLite）  │
                                                            │             │ 异步记录命令历史         │
                                                            ↓             └─────────────────────────┘
                                                   ┌───────────────────────────┐
                                                   │ TradePort 发送接口        │
                                                   └─────────────┬─────────────┘
                                                                 ↓
                                                   ┌───────────────────────────┐
                                                   │ BinanceTradeClient        │
                                                   │ 有界队列＋REST 挂单／撤单  │
                                                   └─────────────┬─────────────┘
                                                                 ↓
                                                            ┌──────────┐
                                                            │ 交易所   │
                                                            └──────────┘
```

`FundsState` 的预留先在账户的串行执行序列中占位，防止并发 Shard 重复花钱；可靠意图提交失败则释放未发出的占位并暂停账户。`OrderJournal` 提交后才调用 `TradePort`。发送队列拒绝时写 `SubmitAbort`／`CancelAbort` 后撤销本地请求；若无法可靠记录撤销，保持未知并暂停。`SqliteHistory` 异步记录，**不作为发包门禁，也不作为恢复来源**。

### 2.2 交易所结果到账户和策略

```text
┌────────────────────────┐
│ REST 挂单／撤单响应     │────┐
├────────────────────────┤    │   ┌───────────────────────┐  ┌──────────────────────────┐
│ 私有 WS 订单／成交／余额│────┼→│ TradingAccount        │→│ OrderJournal（SQLite）    │
├────────────────────────┤    │   │ 有界账户事件队列      │  │ 结果／状态变化事实先提交 │
│ 查订单／成交／余额结果  │────┘   └───────────────────────┘  └────────────┬─────────────┘
└────────────────────────┘                                             ↓
                                                            ┌──────────────────────┐
                                                            │ OrderManager         │
                                                            │ 按 ID 归并、去重、校验│
                                                            └───────────┬──────────┘
                                                                        ↓
                                                            ┌──────────────────────┐
                                                            │ FundsState           │
                                                            │ 调整预留与资产投影   │
                                                            └───────────┬──────────┘
                                                                        ↓
                                                            ┌──────────────────────┐
                                                            │ TradingAccount       │
                                                            │ 更新账户视图与健康   │
                                                            └────────┬────────┬────┘
                                                                     │        └→┌───────────────────────┐
                                                                     │          │ SqliteHistory（SQLite）│
                                                                     │          │ 异步记录回报与余额    │
                                                                     ↓          └───────────────────────┘
                                                         ┌──────────────────────────┐
                                                         │ 相关 Shard 收到账户视图  │
                                                         │ 确认可换单后重新决策     │
                                                         └──────────────────────────┘
```

REST 响应、私有 WS 和查单结果可能先后颠倒，必须用同一个 `client_order_id` 归并，`exchange_order_id` 单独保存。会改变订单或资金的查询事实先落盘再应用；完全相同的只读查询可以只记录指标。`OrderManager` 维护订单状态与查单计划，`TradingAccount` 在同一个账户执行序列中调用 `FundsState`，两者都不能绕过日志直接发布新视图。

### 2.3 超时、断线和重启

```text
┌──────────────────────────────┐
│ OrderManager 最早到期计时器  │→ TradePort → REST 按 client_order_id 查单 ─┐
│ PendingSubmit / PendingCancel│                                          │
└──────────────────────────────┘                                          │
┌──────────────────────────────┐                                          ├→ 2.2 的账户事件队列
│ 启动：回放 OrderJournal      │→ 私有 WS 建立并缓存事件 → 查未决单／成交／余额┤
└──────────────────────────────┘                                          │
┌──────────────────────────────┐                                          │
│ 私有 WS 断线／周期全量同步   │→ 暂停新单 → 查规则／活动单／成交／余额 ──────┘
└──────────────────────────────┘
```

公开行情只驱动报价决策，**不推断真实订单是否已接受、成交或撤销**。`bench_seq` 只把触发决策的行情与订单关联，用于压测延迟；定时器触发的订单没有来源 `bench_seq`。超时和断线期间继续管理已有订单、撤单与查询，但不在状态未知时换一个新 ID 盲目重发。

## 3. 模块所有权

| 模块 | 唯一职责 | 不负责 |
| --- | --- | --- |
| `Shard`、`Strategy` | 接公开行情和账户视图，按单标的决定报价、撤单和换单 | 判断交易所订单终态、记账户余额 |
| `TradingAccount` | 每账户单写入者；入口风控、事件排序、日志提交回调、资金效果与暂停门禁 | 具体 Binance JSON、按标的创建交易连接 |
| `AccountViewPublisher` | 按资产合并受影响的 Shard，构造只读视图并推送 | 修改订单、资金或交易所状态 |
| `AccountSync` | 启动、断线和周期全账户同步，核对规则、活动单、逐笔成交与余额 | 代替 `OrderManager` 的日常单笔超时跟踪 |
| **`OrderManager`** | 持有 `OrderState`、本地／客户端 ID 映射、交易所 ID 的订单字段、活动订单、提交和撤单阶段、期限与查单计划；归并标准化订单与成交事件 | SQLite、socket、交易所余额、策略报价 |
| `OrderState` | 纯订单状态转换、累计成交量与 `trade_id` 去重、矛盾回报校验 | 计时、查询调度、资金和持久化 |
| `FundsState` | 每资产交易所余额、本地资金预留、成交待反映扣减及共享可用额 | 判断订单是否成交、维护交易所独立账本 |
| `RiskLimits` | 单笔、标的和账户限额，以及交易规则校验 | 占用真实可用资金 |
| `OrderJournal` | 单账户连续序号的可靠意图和回报事实，提交成功后回调；恢复来源 | 历史查询报表、发送网络请求 |
| `SqliteHistory` | 命令、回报、余额、阶段时间的异步观察历史 | 决定能否发单、崩溃恢复 |
| `TradePort` / `BinanceTradeClient` | 前者定义发送与查询接口；后者管理有界队列、签名 REST、请求额度及明确的发送阶段／结果 | 写订单状态或余额 |
| `BinanceUserStream` | 每账户私有 WS 的订阅、重连、解析和投递 | 直接修改账户投影 |
| `TradingAccount::SyncFromVenue` | 调用 `AccountSync::Start()`，保持账户控制入口 | 自行保存同步状态或执行全量查询 |

不新增 `order/`、`account/`、`exchange/` 目录，也不叫 `Reconciler`。交易所无关的类继续放在 `src/trading/`；具体协议继续放在 `src/venue/binance/`，保留 `venue` 目录名。本机模拟交易所和 Demo 走相同的外部 REST／私有 WS 路径。V2 不再提供进程内 Paper 模式，已删除 `src/sim/`。

V2 前 `PaperExchange` 保存进程内订单和资金，schema v1 的 Shard 直接持有它；schema v2 的 Paper 账户通过 `PaperTradePort` 调用它。两条 Paper 路径已经删除。V2 的 `pressure_sim` 使用 `bench/venue/mock_binance_server.py` 作为**独立账本、真实 socket**的交易对端；`online_sim` 使用 Binance Demo API。行情回放仍可作为行情输入，订单测试接外部模拟交易所。

协议入口按账户复用交易连接和请求预算：

| 通道 | 当前代码使用的接口 | 进入 V2 的位置 |
| --- | --- | --- |
| 公开行情 | 深度快照、公开深度与逐笔交易 WS | `Shard` 的 Market 和 Strategy；仅驱动决策 |
| 签名 REST | `POST /api/v3/order`、`DELETE /api/v3/order` | `TradePort` 回调产生提交或撤单结果 |
| 签名 REST 查询 | `GET /api/v3/order`、`GET /api/v3/myTrades`、`GET /api/v3/openOrders` | 单笔查询进入 `OrderManager`；全账户查询由 `SyncFromVenue()` 编排 |
| 签名 REST 查询 | `GET /api/v3/account` | 余额事实经日志与核对后应用到 `FundsState` |
| 公开 REST 查询 | `GET /api/v3/exchangeInfo` | `SyncFromVenue()` 校验交易规则后更新账户规则视图 |
| 私有 WS | `executionReport`、`outboundAccountPosition`、`balanceUpdate` | 标准化为订单、成交和余额事件，经账户事件队列与日志后应用 |

## 4. 订单跟踪与取消换单

### 4.1 一张订单、两组状态

`OrderManager` 以 `(account_id, client_order_id)` 查订单，保存 `exchange_order_id`、所属 Shard、标的、方向、原始数量、累计成交、剩余数量、成交 ID 集合、来源 `bench_seq` 和最近的回报版本。订单生命周期与撤单请求是两组状态，避免把“请求撤单”误写成“订单已撤”。

| 维度 | 典型状态 | 进入条件 |
| --- | --- | --- |
| 订单 | `PendingSubmit` → `Accepted` → `PartiallyFilled` → `Filled` | 已提交可靠意图；随后收到匹配的 REST 成功响应、私有回报或有效查询事实；成交按 ID 去重 |
| 订单拒绝 | `PendingSubmit` → `Rejected` | 明确的交易所拒绝或经查询确认的拒绝；本地队列拒绝单独记 `SubmitAbort` |
| 撤单请求 | `None` → `PendingCancel` → `Confirmed` / `Rejected` / `Unknown` | 撤单意图持久化后等待交易所结果；撤单期间订单仍可能部分或全部成交 |
| 网络结果 | `Queued`、`DispatchStarted`、`ResultUnknown`、`DefinitelyNotSent` | 用于判断是否可本地撤销请求；它不是交易所订单状态 |

明确匹配 `client_order_id` 和订单参数的 REST 成功响应可以作为交易所确认；私有 WS 可能先到，不能被晚到的 REST 超时覆盖。`TradePort::Submit/Cancel` 返回 `true` 只表示**本地队列接纳**。发送队列中的命令在客户端停机或连接失败时，需要给账户一个“确定未发出”或“可能已发出”的结果；后者始终按未知处理。

`OrderManager` 按账户维护到期队列并给出下一到期时间，`TradingAccount` 只持有一个 `steady_timer` 调用 `TakeDue()`，不为每笔订单创建 socket 或线程。到期通过请求调度器查询原 `client_order_id`，并查询该单成交；查单无结果或超时不自动视为拒单。撤单未知期间不重复发撤单；确认撤单、成交终态或确认订单仍在账并允许重试后，Shard 才继续决策。

本轮 `OrderManager` 提供状态变化和查询计划，`TradingAccount::ApplyEvent()` 在日志提交后根据状态变化调整 `FundsState`、记录历史并发布 Shard 视图。`OrderManager` 不直接写资金或 SQLite，可单独测试订单状态。资金效果对象化尚未实现，继续由账户编排代码显式处理。

### 4.2 关键函数的改动

| 入口 | 当前 | V2 |
| --- | --- | --- |
| `Shard::OnMarket()` | 外部模式由行情或定时器直接调用策略；Paper 另走 `TryMarket()` | 删除 Paper 的 `TryMarket()` 与直接撮合分支；行情只驱动策略，不跟踪真实订单 |
| `Shard::RunAccountDecision()` | `TrySubmit/TryCancel`；`OnAccountView()` 主要看活动订单是否为空 | 仍发送策略意图；根据账户给出的撤单结果和资金就绪门禁决定换单或重试 |
| `TradingAccount::ProcessSubmit()` | 自己建 ID、`OrderState`、`FundsHold`、日志和发包 | 风控与资金占位后调用 `OrderManager` 建订单；可靠意图提交后交 `TradePort`；账户继续编排资金和日志 |
| `TradingAccount::ProcessCancel()` | 自己 `BeginCancel`、写日志、发包 | 由 `OrderManager` 检查是否已有待确认撤单，避免重复；确认前不释放资金 |
| `TradingAccount::SubmitResponse()` / `CancelResponse()` | 自己解释 REST 结果与未知状态 | 先记结果，再委托 `OrderManager` 归并并应用资金效果 |
| `TradingAccount::ApplyEvent()` | 直接调用 `OrderState` 并处理预留、历史、Shard 通知 | 订单／成交交 `OrderManager`；余额交 `FundsState`；账户按结果处理资金并统一发布视图与历史 |
| `TradingAccount::SyncFromVenue()` | 恢复和未知结果都发起全账户同步 | 保留全账户同步；单笔期限查询交 `OrderManager`，同步结果复用同一归并入口 |

## 5. 资金、SQLite 与故障边界

### 5.1 FundsState 同时处理交易所余额和本地占用

交易所的 `free/locked/total` 是外部账本，`FundsState` 是本地安全投影。买单预留报价资产和可确定的手续费余量，卖单预留基础资产；手续费若可能从第三种资产扣除，需要相应余额或风险余量。`PendingSubmit`、活动单、`PendingCancel` 和结果未知都保留占用。部分成交缩小剩余预留，并在余额事件反映成交前保持临时扣减。

V2 将当前 [`FundsHold.reflected_by_venue`](../../hquant/src/trading/funds_state.h) 的布尔语义改成可核对的覆盖金额／版本：

```text
可用于新订单的资产金额
  = max(0, 最近可信交易所 free
            − 尚未被该余额快照覆盖的本地订单预留
            − 尚未被该余额快照覆盖的成交与手续费扣减)
```

例如账户原有 100 USDT，订单 A 待提交预留 60，可再用 40；若交易所确认 A 在账、可信余额变为 `free=40, locked=60`，A 的 60 已包含在交易所锁定中，不再从 40 重复扣减。**REST 接受本身不足以证明余额已更新**。本轮用每笔覆盖金额、每资产尚未覆盖的预留和余额版本做保守扣减；旧版本快照不能覆盖新状态。全账户同步完成后，才依据核实过的余额快照结算待反映成交扣减。

`FundsState` 另保存一份独立于交易所余额回报的资产总额：新状态目录以配置的**全部非零资产**为基线，仅以已确认成交的基础资产、报价资产及手续费资产变化推进。私有余额事件按事件时间与成交时间比较，避免“REST 查成交先于较早私有余额回报”时误报；全量账户查询及停机最终查询仍要求与**最新**成交投影完全相等。未配置的非零资产、真正账户外资金变动或无法解释的余额会暂停新单。第三资产手续费和账户外借记已在本机模拟交易所用例中验证；真实 Demo 的这些故障场景仍未验收。重启同一状态目录必须保留最初的配置基线，不能改成当前余额，否则重放成交会重复计算。

### 5.2 两份 SQLite 的写入位置

| 数据库 | 文件 | 写入时机与用途 |
| --- | --- | --- |
| 可靠日志 | `order_journal.sqlite`，实现为 [`OrderJournal`](../../hquant/src/trading/order_journal.cc) | 发包前将 `SubmitIntent`／`CancelIntent` 放入有界写队列，不等待 SQLite COMMIT；REST 结果、私有订单／成交／余额事件仍在应用前提交。SQLite WAL + `synchronous=FULL` 用于已写入记录的持久性。 |
| 观察历史 | `history.sqlite`，实现为 [`SqliteHistory`](../../hquant/src/sqlite_history.cc) | 命令进入发送链路后异步记历史；回报应用后异步记订单、成交、余额与阶段。可查询和核对历史序号；写入成功不代表交易所成功，写入失败不能用这份库恢复订单。 |

账户日志序号必须连续。意图入日志队列后可立即进入 TradePort；发送队列明确拒绝时异步记 `Abort`，并立即撤销未发出的本地状态。订单超时从实际开始写 socket 计时，不能把仍在交易客户端队列中的订单提前当成交易所结果未知。意图提交失败时 Quill 记录带 `client_order_id` 的错误、账户暂停新单，并对仍未决的订单启动查单；已入队不能当作未发出。会改变订单或资金的回报仍先可靠提交再应用、发布账户视图、写观察历史。**进程在意图 COMMIT 前崩溃可能丢失本地 `client_order_id`**：启动同步可发现陌生活动单并暂停，但已终结订单仍需交易所历史排查；当前实现不保证这个窗口的自动恢复。

### 5.3 同步和停机

启动顺序：`OrderJournal` 回放订单与占用 → 建立私有 WS 并缓存事件 → 查询交易规则、全账户活动单、未决订单、逐笔成交和余额 → 将状态变化记日志并归并 → 消费或重新同步缓存事件 → 差异消除后进入 `Ready`。私有 WS 断线、余额矛盾或定期检查重用全量同步；未知订单先按单查询。交易所出现本地未记录的活动单、资金无法解释或查询不完整时保持暂停。

正常停机先停止新单和策略定时器，再管理已知订单，继续收回报和查单直至确认或达到期限；未决意图保留在日志中供下次启动查询。不能把“撤单已入队”写成“撤单成功”。

## 6. 目录与文件变更

只列交易链路相关文件；`src/` 根目录不增加交易类文件。

**V2 开发前目录：**

```text
hquant/
  src/
    runtime.h / runtime.cc
    sqlite_history.h / sqlite_history.cc
    config.h / config.cc
    perf_probe.h
    shard/
      shard.h / shard.cc
      strategy.h / strategy.cc
      binance_feed.h / binance_feed.cc
      executor.h / executor.cc
    trading/
      trading_account.h / trading_account.cc / trading_account_sync.cc
      order_state.h / order_state.cc
      funds_state.h / funds_state.cc
      order_journal.h / order_journal.cc
      risk_limits.h / risk_limits.cc
      trade_port.h / trading_event.h
      BUILD.bazel
    venue/binance/
      trade_client.h / trade_client.cc
      user_stream.h / user_stream.cc
      request_scheduler.h / request_scheduler.cc
      auth.h / auth.cc
      rules.h / rules.cc
      BUILD.bazel
    sim/
      paper_exchange.h / paper_exchange.cc
      paper_trade_port.h / paper_trade_port.cc
      BUILD.bazel
  bench/
    mock_binance_server.py / validate_trade_chain.py / trade_chain_bench.py
  test/
    order_state_test.cc / funds_state_test.cc / trading_account_test.cc
    order_journal_test.cc / e2e_trade.py
```

**V2 实施后目录：**新增 `OrderManager`，保留 `venue/binance/`；`sim/` 和旧 `shard/executor.*` 已删除。`src/` 根目录没有新增交易类。

```text
hquant/
  src/
    runtime.* / sqlite_history.* / config.* / perf_probe.h       修改
    shard/shard.*                                                修改
    shard/strategy.* / shard/binance_feed.*                      保留
    trading/
      order_manager.h / order_manager.cc                         新增
      order_state.* / funds_state.* / trading_event.h            修改
      trading_account.* / trading_account_journal.cc            修改
      account_sync.* / account_view_publisher.*                  新增
      order_journal.* / trade_port.h / BUILD.bazel               修改
    venue/binance/                                               保留目录名
      trade_client.* / user_stream.*                             修改
      auth.* / rules.* / request_scheduler.* / BUILD.bazel       保留
  bench/
    market_wire.py / mock_binance_server.py                      外部行情和交易模拟
    trade_chain_bench.py / validate_trade_chain.py               逐单、逐笔、逐资产核对
    make_market_cases.py / validate_market_cases.py              行情样本生成与检查
  test/
    order_manager_test.cc                                        新增
    order_state_test.cc / funds_state_test.cc                    扩充
    order_journal_test.cc / e2e_trade.py                         保留并扩充
    mock_public_server.py / run_trade_e2e.cc / BUILD.bazel      本机真实 socket 测试
```

| 文件 | 具体改动 |
| --- | --- |
| `trading/order_manager.h/.cc`、`trading/BUILD.bazel` | 新的每账户订单跟踪器、期限队列和标准化订单／成交事件归并；依赖 `OrderState` 和中立事件类型，不依赖 Binance 或 SQLite。资金效果仍由 `TradingAccount` 编排。 |
| `trading/trading_account.h/.cc` | 把 `orders_`、订单 ID 映射、活动单集合和结果解释迁入 `OrderManager`；保留风险、FundsState、事件队列，并组合持有 `AccountSync` 与 `AccountViewPublisher`。 |
| `trading/trading_account_journal.cc` | 意图、回报的可靠提交回调与启动日志回放。 |
| `trading/account_sync.*` | 启动、断线和周期全账户同步；查询事实通过账户的持久化回调应用。 |
| `trading/account_view_publisher.*` | 合并资产变化并向相关 Shard 发布只读账户视图。 |
| `trading/order_state.*` | 保留纯状态机；补足 REST／私有 WS／查单乱序、部分成交后撤单、终态重复事件的规则。 |
| `trading/funds_state.*` | 用覆盖金额／版本代替单一 `reflected_by_venue` 标记；细化成交待反映扣减、手续费资产和迟到余额快照。 |
| `trading/trade_port.h`、`trading/trading_event.h`、`venue/binance/trade_client.*` | 删除仅供 Paper 使用的 `OnMarket` 与 `MarketMatchInput`；明确本地入队、开始写 socket、确定未发出和结果未知；维持账户级请求额度。 |
| `venue/binance/user_stream.*` | 保持标准化事件入口；补充重连后回报版本与余额顺序信息，保证旧事件不会覆盖新状态。 |
| `trading/order_journal.*` | 为需要重放的发送结果、超时与查询事实补足事件字段；保持提交后回调和连续序号。 |
| `sqlite_history.*`、`perf_probe.h` | 历史明确区分意图、入队、发包、REST 确认、私有回报应用、查询恢复；分别统计可靠提交与观察历史提交。 |
| `shard/shard.*`、删除 `shard/executor.*` | 删除 Shard 直连 Paper、`TryMarket()` 和旧同步 `Executor` 分支；账户视图中的撤单状态与资金门禁驱动换单。 |
| 删除 `sim/paper_exchange.*`、`sim/paper_trade_port.*`、`sim/BUILD.bazel` | 进程内撮合及适配器全部退出构建；外部模拟交易所仍在 `bench/`，通过 `venue/binance/` 协议适配器访问。 |
| `config.*`、`runtime.*`、`src/BUILD.bazel` | 只保留 schema v2 的外部账户模式，移除 `TradingMode::Paper`、schema v1 Paper 解析及构建注入；把仍需的费率等字段从旧 `ExecutorConfig` 迁入账户视图／配置。当前 `online_sim` 只连接固定 Binance Demo 域名；不存在生产实盘分支。 |
| `perf_probe.h`、`main.cc`、`shard/binance_feed.*` | 删除 `paper_match` 与 `live_paper` 等本地撮合指标，保留行情、日志、交易请求和私有回报的分段计时。 |
| `bench/market/make_market_cases.py`、`bench/market/market_wire.py`、`bench/market/validate_market_cases.py`、`bench/trade_chain_bench.py` | 原行情样本继续可用；生成 `market_shards.yaml`，运行时转换为 schema v2 + 外部模拟交易所配置。旧 case 的 `config.yaml` 也可转换。`live_ws_bench.py` 已删除，公共行情工具移入 `market_wire.py`。 |
| 删除 `test/e2e_replay.py`、`test/trading_account_test.cc`，新增 `test/mock_public_server.py`、`test/order_manager_test.cc`，修改 `test/BUILD.bazel`、`examples/replay.yaml`、`examples/binance_public.yaml` | 移除旧 schema v1/Paper 测试配置；订单断言通过外部 socket 用例和纯状态测试完成。 |
| `bench/venue/mock_binance_server.py`、`bench/validate_trade_chain.py`、`test/e2e_trade.py` | 逐 ID、成交、余额和日志序号核对；增加队列拒绝、回报乱序、撤单成交竞态、余额晚到和崩溃恢复故障场景。 |

## 7. 开发计划

第 1–6 项已完成本机实现与验收；第 5 项还增加成交时间投影，第三资产手续费与账户外借记的本地正确性用例已通过。第 7 项的两笔真实 Demo 虚拟资金成交已通过，其中新版本验证了成交时间在私有回报、可靠日志与独立查成交之间一致；Demo 中的第三资产手续费、账户外活动和断线故障尚未验收。

1. **迁移旧 Paper 测试输入。** 保留有价值的行情回放样本，把需要订单断言的 case 接到 `bench/venue/mock_binance_server.py`；旧 `schema_version: 1` 配置和 Paper 专用断言退出目标测试集。先建立可重复的外部链路基线。
2. **删除进程内 Paper 路径。** 删除 `sim/`、`shard/executor.*`、两条 Shard Paper 分支、账户 `TryMarket()`、`TradePort::OnMarket()`、`MarketMatchInput`、schema v1／Paper 配置和相关 Bazel 依赖。保持 `venue/binance/` 与外部模拟交易所链路可构建、可运行。
3. **抽出 `OrderManager`。** 迁移订单状态、ID 映射、撤单阶段和视图查询；由 `TradingAccount` 按日志提交顺序调用它。通过订单状态、账户状态与本机 socket 用例验证行为。
4. **补齐发送与未知结果语义。** `TradePort` 报告确定未发出与可能已发出；`OrderManager` 按单计时、退避查询；撤单结果决定重试或换单。覆盖丢 REST 响应、私有回报先到、查单先于撤单、队列停机。
5. **修正资金覆盖与日志回放。** 核对交易所 `locked` 和本地预留，覆盖余额晚到与第三资产手续费；回放日志后先查账再开放新单。不能解释的差异保持暂停。
6. **先跑本机正确性，再测性能。** 沿用 1000 标的、30 秒 case，核对模拟交易所独立账本、`OrderManager`／`FundsState`、`order_journal.sqlite` 和 `history.sqlite`；分段报告日志提交、队列等待、发包、REST 结果、私有回报应用与历史提交。
7. **单独验收 `online_sim` 的 Demo 成交路径。** 一个 BTCUSDT 标的、最多一笔小额虚拟资金订单，按[Demo 验证手册](../operations/BINANCE_DEMO_VALIDATION.md)核对真实 `TRADE` 回报、非空成交查询、余额与两份 SQLite。1000 标的压测只对本机模拟交易所运行。

通过条件是：任一时刻每笔订单只有一个 `client_order_id` 和可解释状态；未决命令的资金不会被第二个 Shard 复用；确认撤单前不会换单；REST、私有 WS、查单和重启回放得到相同订单／资金终态；可靠日志无序号缺口；观察历史的缺口明确可见；请求队列、查询次数与账户状态均有上限。行情吞吐和订单性能只能在这些正确性条件满足后解释。

## 8. 本轮本机验收

使用 C++20 构建，通过 `bazelisk --batch test //hquant/test:all` 的 10 个目标，包含签名、规则、风险、订单状态、资金、可靠日志与本机 REST／私有 WS 故障用例。另用本机端到端用例验证：已有成交的状态目录若错误地把 `initial_balances` 改成当前余额，恢复对账会暂停并拒绝新单。新格式的 1000 标的行情 fixture 通过 `make_market_cases.py` 和 `validate_market_cases.py`；旧 fixture 仍可由 `trade_chain_bench.py` 转换。

最新本机压测沿用**原有** `stress` case：1000 标的、30 秒、30 万条行情处理完成通知。独立模拟交易所经真实 socket 收到 3800 次挂单和 1728 次撤单，产生 3703 笔成交、9449 条私有订单回报，覆盖拒单、部分成交、撤单中成交和重复回报。`history.sqlite` 共 49584 条账户事件，`order_journal.sqlite` 共 46105 条可靠记录；两库完整性、日志连续序号、逐订单 ID、成交 ID、价格数量、每资产余额和最终账户状态核对均无错误。结果在 `/private/tmp/hquant-trade-projection-1000-04/trade_chain_result.json`，模拟器独立账本在同目录 `venue_snapshot.json`，`valid: true`。

| 分段指标 | 本轮 p99 |
| --- | ---: |
| 行情发出 → 模拟交易所收到实际订单 | 2980 ms |
| 模拟交易所发私有回报 → 账户应用 | 1239 ms |
| 行情发出 → 对应私有回报应用 | 3863 ms |
| 交易发送队列等待 | 2613 ms |
| socket 写入 / REST 响应等待 | 1.9 / 3.2 ms |

这些是本机正确性负载下的观测值；订单队列等待主导延迟，不能据此推断真实交易所性能。1000 标的压测没有访问真实 Demo API。新版本又以单标的、最多一笔、20 虚拟 USDT 硬额度完成真实 Demo 成交验收：订单状态 `FILLED`，一笔 `executionReport: TRADE` 已应用，签名 `myTrades` 返回一笔，成交 ID、数量、价格、手续费、资产和成交时间与可靠日志及 SQLite 一致，独立账户余额匹配，未结订单为零。同一状态目录保留原始基线只读重启，回放订单和成交后全账户同步完成，没有新挂单或日志缺口。具体记录见[Demo 验证文档](../operations/BINANCE_DEMO_VALIDATION.md)。Demo 环境的丢响应、私有流断线、第三资产手续费和账户外变动仍是扩大运行范围前的缺口。

## 9. 开发 Prompt

下面保留本轮开发使用的 Prompt，供后续复查和扩展测试时参考。

```text
在 /Users/larry/project/open_project/hummingbot-cpp-redesign 按 docs/architecture/TRADING_CHAIN_DESIGN_V2.md 实施交易链路 V2。使用 C++20，并遵循现有 hquant 命名、Bazel 目标和账户单写入者风格。

目标：交易只走外部接口。移除进程内 PaperExchange、PaperTradePort、TradingMode::Paper、schema v1 Paper 兼容、Shard 直连 Executor/Paper、TradingAccount::TryMarket 和 TradePort::OnMarket；迁移依赖 Paper 的测试与配置。保留 src/venue/binance/ 目录及其 Binance 协议适配器。正确性测试使用 bench/venue/mock_binance_server.py 的独立账本和真实 REST／私有 WS socket。

新增 src/trading/order_manager.h/.cc，按账户集中持有 OrderState、订单 ID 映射、提交与撤单阶段、超时期限及查单计划。TradingAccount 负责风险、FundsState、OrderJournal、事件队列、全账户 SyncFromVenue 和视图发布。意图入日志队列后发送；REST、私有 WS、查单产生的状态变化先记日志再应用 OrderManager 和 FundsState；history.sqlite 只记异步观察历史。入发送队列不算交易所接受；未知结果保留资金并按原 client_order_id 查询，不生成新 ID 重发。PendingCancel 期间不重复撤单，确认订单与资金状态后才让 Shard 换单。

按文档第 7 节顺序开发。补充单元、故障和真实 socket 集成测试，再运行现有 1000 标的、30 秒本机外部交易 case 做逐单、逐成交、逐资产和两份 SQLite 核对。不要对真实 Demo 做压力测试；保持 `online_sim` 的单笔限额，且只允许 Binance Demo 域名与虚拟账户。不要输出或提交凭据。交付时列出实际删除／新增／修改文件、测试命令及结果、真实 Demo 验证结果与剩余缺口。
```
