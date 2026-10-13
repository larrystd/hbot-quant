# hquant 架构：路径、异常与模块（2026-10-13）

本文按 `hquant/src` 当前代码写成（约 1.1 万行，Bazel 构建），分四部分：

- **A 路径篇**：讲一条消息或一笔订单在系统里怎么走。
- **B 异常篇**：按"异常、行为、处理与恢复"三列，讲出错时系统做什么。
- **C 模块篇**：讲每个文件负责什么、归哪个线程、守哪些规则。
- **D 设计取舍**。

代码位置写作 `目录/文件:行号`，路径相对 `hquant/src/`。

## 0. 总览

hquant 是一个做市进程。每个 **Shard** 负责一个交易对：接 Binance 公开行情，用一个简单的双边挂单策略产生订单，订单经 **TradingAccount** 和 **TradePort** 发给交易所。交易模式只有两种模拟：

- `PressureSim`：连本地模拟交易所 `bench/venue`，用于压测。
- `OnlineSim`：连 Binance Spot Demo，使用虚拟资金。

### 0.1 分层与依赖

依赖只能从上往下。`trading/` 不认识 Binance，只认识抽象接口 `TradePort`；`venue/binance/` 实现这个接口；`runtime` 在启动时把两边接起来。

```text
main.cc
  └─ runtime            组装：创建线程、账户、Shard、交易客户端，连好回调
       ├─ control_server   Unix socket 控制面
       ├─ sqlite_history   观察用历史（可丢，异步）
       ├─ replay_reader    文件回放输入
       ├─ shard/           行情 → 策略 → Shard 本地交易状态
       │    └─ 依赖 trading/（含具体类 TradingAccount）
       ├─ venue/binance/   TradePort 的 Binance 实现：REST、私有流、签名、限频
       │    └─ 依赖 trading/trade_port.h
       └─ trading/         与交易所无关：订单状态机、资金、风控、可靠日志、账户
            └─ base/       定点数、ID 类型、网络、日志
```

### 0.2 线程模型

每个对象只归一个线程所有，跨线程基本都靠 `boost::asio::post` 投递任务。线程在 `runtime.cc:195` 的 `Runtime::Start` 里创建。

| 线程 | 数量 | 拥有/运行的东西 |
| --- | --- | --- |
| Shard worker | `min(8, shard 数)` | 多个 Shard 共用一个 `io_context`。上面跑行情协程（`BinanceFeed`）、`Market`、`Strategy`、`ShardTradingState`，以及绑定到这个 worker 的 `BinanceTradeClient` 的 REST 连接（下单请求就在这里写出） |
| Account worker | `min(2, 账户数)` | `TradingAccount` 的任务队列、对账副本（`OrderManager` + `FundsState`）、`AccountSync`、`AccountViewPublisher`、REST 回调。空闲时先忙等 50 µs 再睡（`RunAccountContext`，`runtime.cc:44`） |
| 私有流线程 | 每账户 1 个 | Binance user data WebSocket（`BinanceUserStream`），只在第一个 `BinanceTradeClient` 上开启 |
| REST 线程 | 每个 client 0 或 1 个 | 只有不绑定 Shard worker 的 client 才自建（账户下没有 Shard 时） |
| OrderJournal | 1 | SQLite 可靠日志的写线程，凑批提交（最多等 250 µs 或凑满 256 条） |
| SqliteHistory | 1 写 + 1 读 | 观察用历史记录，异步写入 |
| ControlServer | 1 | Unix socket，按行读 JSON 请求 |
| 主线程 | 1 | 回放模式下逐条读文件；实时模式下等待信号 |

### 0.3 三条贯穿全系统的规则

1. **单线程所有权**：Shard 的状态只在 Shard 线程上改，账户的状态只在账户线程上改；别的线程要改，就 post 一个任务过去。唯一的例外是 `SubmitFromShard`/`CancelFromShard`：它们在 Shard 线程上直接调用账户的方法，靠锁和"只入队、不改状态"来保证安全。
2. **先日志，后状态**：交易所来的事实（请求结果、回报、成交、余额、查单结果）先写 `OrderJournal`，`COMMIT` 成功后才应用到状态（`trading/trading_account.cc:944`）。进程崩溃后靠日志重建。
3. **热路径本地准入**：账户资金在启动时就按 Shard 切好（`ResolveShardAllocations`）。下单时 Shard 只查自己那一份额度，不用等账户线程回话；账户线程事后异步更新它的对账副本。

## A. 路径篇

### A1 启动与就绪

Shard 什么时候才能下第一笔单？要等**私有流连上、并完成一次全量对账**之后。

1. **主线程**：`main.cc:487` 解析 gflags，`LoadConfig` 读 YAML 并校验，然后构造 `Runtime`。
2. **主线程，Runtime 构造**（`runtime.cc` 约 95–190 行）：
   - 创建 `SqliteHistory`、`OrderJournal`（与 history 放在同一目录，文件名 `order_journal.sqlite`）、`ControlServer`。
   - `ResolveShardAllocations`（`config.cc:539`）把每个账户的资金切给它的 Shard：显式配置的额度先扣，剩下的在相关 Shard 间平分。
   - 账户轮流分配到账户 worker。Shard 在同一账户内轮转分配到 Shard worker，可用 worker 数为 `min(Shard worker 数, rest_connections)`。
   - 对每个 Shard 调 `SetViewSink`/`SetTradeSink`。账户以后要通知这个 Shard 时，就调这些 lambda，lambda 内部是 `Shard::PostXxx`。
3. **主线程，恢复**（`Runtime::Start`，`runtime.cc:195`）：`journal_.Start()`；对每个账户 `journal_.Load(id)` 读出日志，先调账户的 `RestoreJournal`，再对这个账户下的每个 Shard 调 `RestoreTradingJournal`。
4. **主线程，建交易客户端**：所有账户共用一个 `RequestScheduler`。每个账户按它的 Shard 所在的 Shard worker 各建一个 `BinanceTradeClient`，REST 连接数在这些 client 之间平分，REST 协程跑在对应 Shard worker 的 `io_context` 上。只有第一个 client 开私有流、拉交易规则。
5. **主线程**：启动 history 线程和所有 worker 线程；`shard->Start()`，实时模式下 `co_spawn(market_.RunBinance())`，并启动策略定时器；然后给每个 Shard 投递一份初始 `AccountView`，最后启动控制面。
6. **私有流线程**：连上并订阅成功 → `on_health(true)` → `TradingAccount::OnConnectionState`（`trading/trading_account.cc:920`）。
7. **账户线程**：`SyncFromVenue` → `AccountSync::Start`（`trading/account_sync.cc:54`）：先关准入，然后依次查交易规则 → 挂单 → 逐单查单和成交 → 余额。每一条查到的事实都走 `PersistAndApply`。
8. **账户线程**：`FinishSync`（`account_sync.cc:387`）。如果对账期间又来了私有回报，就整轮重来，最多 3 次。否则设 `ready = true`、`EnableAdmission()`，并把 `min_ready_revision_` 设为当前视图版本；60 秒后再做一次周期对账。
9. **Shard 线程**：收到一份 `ready=true` 且版本号 ≥ `min_ready_revision_` 的 `AccountView` 之后，门控才放行。

> 为什么要比较视图版本：准入开关是一个原子变量，但 Shard 下单依据的是它手里那份 `AccountView`。如果 Shard 手里还是对账前的旧视图，就算准入已经打开，也会因为版本太旧被拒绝。

### A2 行情：一帧 WebSocket 消息怎么变成一次策略调用

整条路径在**同一个 Shard worker 线程**上同步完成，中间没有队列。

公开行情只用**一条** WebSocket 连接，同时订阅两个流：`<symbol>@depth`（盘口增量）和 `<symbol>@trade`（公开成交），见 `binance_feed.cc:52`。

1. `BinanceFeed::RunCycle`（`shard/binance_feed.cc:350`）每次（重）连接时，先 `++epoch_` 并发出 `FeedConnected{epoch}`，然后 `websocket_.Reconnect`。
2. `Market::OnConnected` 收到新的 epoch 后，清空盘口、增量缓冲和序号，回到 `Syncing`。**每次重连都要重新拿快照、重建盘口**，因为交易所不会补发断线期间的增量。旧连接上残留的消息带着旧 epoch，会被直接丢弃。
3. 先读 WS，等到**第一条 depthUpdate**，记下它的 `first_sequence`。这期间收到的公开成交照常转发。
4. 再发 REST `GET /api/v3/depth` 拿快照，最多试 3 次。如果快照比第一条增量还旧（`last + 1 < first_update`），就重新拿。
5. 进入主循环：用 simdjson 解析，分成 `BookDiff` 或 `PublicTrade`；价格和数量在解析时就换算成整数 tick/lot。
6. `Market::HandleInput`（`shard/market.cc:241`）分派：
   - `OnDiff`：**Live** 时检查序号能否接上（`first ≤ seq + 1`），能接上就应用，并发 `BookChanged`。**Syncing** 时先缓冲，由 `ApplyBuffered` 等快照对齐。
   - `OnSnapshot`：只在 Syncing 时有效。用快照重建盘口后接上缓冲的增量，**至少接上一条增量才切到 Live**。
   - `OnTrade`：不改盘口，只更新 `last_trade`，并发 `PublicTrade`。公开成交是事件，不是状态：重连后不需要重建，漏掉的成交也不补。
7. 遇到以下情况，`EnterSyncing(reason, reconnect=true)` 让 Feed 断开重来：序号缺口、盘口交叉或单边为空、增量缓冲满、市场不匹配，或者 `StaleLoop` 发现 Live 状态下 5 秒没有更新。
8. `notify_` → `Shard::OnMarket`（`shard/shard.cc:306`）→ `RunStrategyDecision`。每次盘口变化、每笔公开成交都会让策略跑一次，策略自己靠 `refresh_ms` 节流。

### A3 下单：从策略决定到 HTTP 请求写出

这一段几乎全部在 **Shard 线程**上完成。

1. **门控**（`shard/shard.cc:77`）：有在途的提交或撤单（`trading_.Pending()`）、`account_view_.ready` 为假、或 `AdmissionEnabled(revision)` 为假，都直接返回。所以同一个 Shard 同一时刻只有一批请求在途。
2. **组装 `ExecutionView`**：活跃订单、本 Shard 的可用额和预算（来自 `ShardTradingState` 自己的余额）、手续费率、精度、最小下单量和最小下单金额。
3. **策略** `Strategy::Decide`（`shard/strategy.cc:11`），逻辑见 A6。
4. **生成客户订单号** `NewClientOrderId`：`"hq"` 加 16 字节随机数，转成 32 位十六进制。
5. **本地准入** `ShardTradingState::BeginSubmit`：
   - `RiskLimits::Check`：检查精度、最小金额、交易所实时规则、单笔金额上限、活跃订单数、同一计价资产的挂单总值，比对对象是本 Shard 自己的订单表。
   - `ReserveAmount`：卖单占用 base 数量；买单占用 `ceil(价×量) + ceil(手续费)`。
   - `Reserve` 从本 Shard 额度里扣出这笔占用；本地订单号为 `(shard_id+1) << 40 | 序号`，订单以 `PendingSubmit` 状态入表。
6. **交给账户** `TradingAccount::SubmitFromShard`（`trading/trading_account.cc:152`），仍在 Shard 线程上执行：
   - 持读锁 `admission_gate_mutex_`，复查准入和视图版本，并选定 TradePort。
   - OnlineSim 模式下，用 CAS 检查本次运行的下单次数上限。
   - `TryEnqueue(镜像任务)`：账户线程稍后执行 `ReserveAllocated` + `order_manager_.Begin` + 写历史 + 刷新视图。
   - `Persist(SubmitIntent)`：**只入队**，落盘结果之后再回调。
   - `port->Submit(command, done, write_started)`。

   任何一步失败都会回滚：Shard 侧 `AbortSubmit`，账户侧补一个 abort 镜像，必要时再写一条 `SubmitAbort`。
7. **REST 排队** `BinanceTradeClient::Enqueue`（`venue/binance/trade_client.cc:309`）：队列给撤单和查询预留了容量。请求放进 6 个优先级队列之一，顺序为：撤单 > 查单 > 查成交 > 挂单列表/规则 > 余额 > 下单。
8. **发送** `Drain`（`trade_client.cc:554`）：下单在队列里等太久就返回 `NotSent`；`scheduler_->Reserve` 预扣请求权重；签名后 `co_await http.Send`。开始写 socket 的那一刻，`on_write_start` 把 `write_started` post 到账户线程。

> 这时同一笔订单同时存在于两处：Shard 的 `ShardTradingState`（决定还能不能下单）和账户的 `OrderManager`（与交易所对账、查单）。两份状态由同一串事件推进：账户先收到事件、写日志，再通知 Shard。

### A4 请求结果回程：HTTP 响应 → 账户 → Shard

1. **Shard 线程**：`BinanceTradeClient::Complete`（`trade_client.cc:660`）把结果分成四类。

   | 情况 | 结果 |
   | --- | --- |
   | 网络错误，且还没开始写 | `NotSent`（确定没发出） |
   | 网络错误，但已开始写；或 5xx/408/429/418；或错误码 -1000/-1001/-1006/-1007 | `Unknown`（可能已执行） |
   | 其它非 200，下单 | `Rejected` |
   | 其它非 200，撤单 | `Unknown` |
   | 200 且客户订单号一致 | `Confirmed`；撤单还带回剩余数量 `origQty − executedQty` |

   `CompleteTrade` 把完成回调 post 到账户 io。
2. **账户线程**：`TryEnqueue(PersistSubmitResponse)` → 写一条 `SubmitOutcome`。
3. **日志线程**：凑批后 `BEGIN IMMEDIATE … COMMIT`（`trading/order_journal.cc:287`），提交成功后把回调 post 回账户 io。
4. **账户线程**：`NotifyResponse` → `Shard::PostTradeResponse`；同时 `SubmitResponse`（`trading_account.cc:708`）更新账户副本。如果私有回报已经先到，迟到的 HTTP 超时不会把已确认的订单改回 Unknown。
5. **Shard 线程**：`ShardTradingState::ApplyResponse` 按同样的规则推进 Shard 侧的订单；订单进入终态就释放占用。

**写出通知**：`on_write_start` → 账户线程 `OnWriting`（`trading_account.cc:853`）做三件事：标记 `submit_writing`、`Arm` 一个 2 秒后到期的查单计划、写一条 `SubmitWriting`；然后 `NotifyWriting` 通知 Shard。有了这个标记，就能区分"请求根本没发出"和"已经写出、结果未知"两种情况。

### A5 私有回报：交易所推送 → 账户 → Shard

1. **私有流线程**：`BinanceUserStream` 用签名订阅 `userDataStream.subscribe.signature`，收到的消息按类型转换：`executionReport` 转成 `OrderReport`（有成交时再加一个 `Fill`），`outboundAccountPosition` 转成 `BalanceUpdate`，`balanceUpdate`/`externalLockUpdate` 转成 `BalanceDelta`。
2. **私有流线程**：`TradingAccount::OnVenueEvent`（`trading_account.cc:1124`）→ `TryEnqueue`；如果队列满，就暂停账户。
3. **账户线程**：如果正在对账，先把事件缓冲起来；否则 `PersistAndApply(event, from_venue=true)`。
4. **日志线程**：写入并提交。
5. **账户线程**：`ApplyEvent`（`trading_account.cc:1143`）推进 `OrderManager`/`FundsState`。如果是交易所推来的 `BalanceDelta`（外部充提或锁定），直接暂停。最后 `NotifyEvent`：只有 `OrderReport`/`Fill` 会转给订单所属的 Shard。
6. **Shard 线程**：`Shard::PostTradeEvent`（`shard/shard.cc:184`）→ `ShardTradingState::ApplyEvent`。回报进入终态就释放占用；成交则在本 Shard 的余额上做借贷，并把占用缩小到剩余数量对应的金额。Shard 侧发现任何矛盾，都会 `ReportShardError` 并停止这个 Shard。
7. **Shard 线程**：如果正在等撤单完成，而且已经没有在途请求、也没有挂单，就立刻再跑一次策略。

`FlushViews` → `AccountViewPublisher::Flush`（`trading/account_view_publisher.cc:79`）：只给"脏"的 Shard 发新视图，每次发送视图版本号加 1。Shard 收到比自己手里更旧的视图会直接丢弃。

### A6 刷新循环与撤单

`Strategy::Decide` 按以下顺序判断：

1. 盘口不是 Live：有活跃订单就全部撤掉；没有就 `NoAction`。
2. 还没到 `next_refresh_at_`：`NoAction`。
3. 有活跃订单：**全部撤掉**。这一步不更新刷新时间，所以撤完之后马上满足"已到时间"。
4. 没有活跃订单：设 `next_refresh_at_ = now + refresh_ms`，然后计算：
   - 中间价 `mid = bid + (ask − bid) / 2`；
   - 买价 `mid × (1 − bid_spread)`，向下取整到价格精度；
   - 卖价 `mid × (1 + ask_spread)`，向上取整；
   - 数量按数量精度向下取整。

   买单要满足"金额加手续费不超过 quote 预算"，卖单要满足"数量不超过 base 预算"。

```text
定时器(refresh_ms) 或 行情通知
  → Decide = CancelOrders → 每单 BeginCancel + CancelFromShard，awaiting_cancel_ = true
  → 撤单结果/回报陆续到达（A4/A5）
  → 发现 awaiting_cancel_ 且无在途且无挂单
  → 立刻 Decide = SubmitOrders（新的一对买卖单）
```

### A7 超时与未知结果：查单兜底

1. **账户线程**：订单开始写网络时 `OrderManager::Arm`，2 秒后到期（`trading/order_manager.cc:137`）。
2. `ScheduleOrderQuery` 把唯一的查单定时器设到最早的到期时间。正在对账、私有流断开、或正在停机时，不查单。
3. 到期后 `QueryDueOrder`（`trading_account.cc:998`）：如果订单还在等 HTTP 结果，先记一次本地超时（`Unknown`，原因是 "local response timeout"），再发查单。
4. `QueryOrderResult`：查到结果就经 `PersistAndApply` 应用。查不到不会编造拒单，而是按 1、2、4…32 秒退避重查。

各种结局怎么处理，见 B1。

### A8 断线与全量对账

- 私有流断开 → 中止对账，取消查单定时器，然后 `Pause`。Shard 不再做任何决策，挂单留在交易所。
- 重连 → 重复 A1 的第 7–8 步。
- 对账期间收到的私有回报先缓冲；如果有回报，这一轮作废、重来，最多 3 次。

### A9 控制面

控制线程按行读取 JSON，按 `op` 分派：

| op | 去向 | 说明 |
| --- | --- | --- |
| `clock` / `status` | 各 Shard 和账户的 `PostStatus` | 在各自线程上取快照 |
| `history` | `SqliteHistory::Query` | 分页读取 |
| `set_strategy` | `Shard::PostStrategyUpdate`（`shard/shard.cc:465`） | 带 `expected_version`，版本不一致就拒绝；成功后立刻决策一次 |
| `pause_new_orders` / `resume_trading` / `sync_account_state` / `cancel_all` | `TradingAccount::PostControl`（`trading_account.cc:513`） | 必须带操作人和原因；先写 `Control` 日志；恢复时触发全量对账；`cancel_all` 通知各 Shard 撤单 |
| `account_controls` | `OrderJournal::QueryControls` | 查人工操作记录 |
| `stop` | `Runtime::RequestStop` | 同 A11 |

### A10 文件回放

`Runtime::RunReplay`（`runtime.cc:405`）逐条读取记录，`PostReplay` 发到目标 Shard，**等这一条处理完才读下一条**，所以回放结果可以复现。Shard 会检查时间单调递增；策略的"现在"取记录里的时间。

### A11 停机

1. `Runtime::RequestStop`（`runtime.cc:424`）：通知所有账户 `PrepareStop`，所有 Shard `RequestStop`。
2. Shard 执行 `StopOnThread`（`shard/shard.cc:513`）：停行情，撤掉所有挂单；最多等 30 秒。
3. `Runtime::Wait`（`runtime.cc:431`）：依次等 Shard 停完 → 取账户最终状态 → 停账户 → join 线程 → 刷写 history 和日志。

## B. 异常篇

### B0 先理解"暂停"

- **做了什么**：`TradingAccount::Pause(reason)`（`trading_account.cc:1411`）设 `ready=false`、关准入、记下 `error`，并给所有 Shard 发新视图。
- **Shard 的表现**：门控看到 `ready=false` 就直接返回，**既不下单也不撤单**。挂单留在交易所，可能继续成交；成交回报照常处理。
- **暂停期间怎么撤单**：`CancelFromShard` 不检查准入，所以 `cancel_all` 和停机流程仍然能把撤单发出去。
- **怎么恢复**：成功完成一次全量对账就自动恢复（人工暂停的除外）。触发对账的方式：私有流重连；60 秒周期定时器（**只有上一次对账成功，才会安排下一次**）；`resume_trading` / `sync_account_state`。

暂停分两类：**周期对账能修好的**（外部余额变动、账户队列溢出），60 秒内会自动恢复；**对账本身会失败的**（查不到某笔订单、交易所有本地不认识的挂单、日志写不进去），会一直停着，直到人工处理。

### B1 请求已发出，迟迟没有返回

```text
t = 0     开始写 socket
          → 账户 OnWriting：submit_writing = true，写 SubmitWriting，Arm 查单（2 秒后到期）
t = 2 s   查单定时器到期，订单仍是 PendingSubmit
          → 写 SubmitOutcome(Unknown, "local response timeout")，标记 submit_unknown，发第一次查单
t = 5 s   HTTP deadline 到期
          → 已开始写，结果为 Unknown → 写日志 → 通知 Shard → Shard 标记 submit_unknown
          （若此前迟到的 200 回来，则 Confirmed → Accepted）
之后       查单按 1、2、4、8、16、32 秒退避，32 秒封顶，一直重查
          私有流回报若先到，直接确定状态，查单计划随之撤销
```

| 结局 | 行为 | 处理与恢复 |
| --- | --- | --- |
| 查到订单（已挂、已成交或已撤） | 查单结果和补查到的成交经 `PersistAndApply` 应用（`trading_account.cc:1052`），再通知 Shard | 自动。Shard 的 `Pending()` 变为假，恢复决策 |
| 查不到（订单没到达交易所），或查询一直失败 | 不编造拒单，一直退避重查；这个 Shard 一直不能下新单（`PendingFor` 把 PendingSubmit 和 Unknown 都算作在途） | 下一次周期对账查这笔单也会失败，账户暂停（"restored order query failed"），之后不会自动恢复，需要人工确认 |

2 秒的本地超时只更新账户副本，不通知 Shard；但 Shard 的门控对 PendingSubmit 和 Unknown 一视同仁，所以两边的行为是一致的。

### B2 下单与撤单请求的异常

| 异常 | 行为 | 处理与恢复 |
| --- | --- | --- |
| 本地队列积压 | 下单排队超过 30 秒（PressureSim）或 5 秒（OnlineSim）→ `NotSent`；撤单没有排队超时 | 释放占用，记 `LocalRejected`；下个刷新周期重新报价 |
| 本地发送队列满 | `Submit` 返回 false → 写 `SubmitAbort`，回滚账户镜像，Shard `AbortSubmit` | 下一次行情事件重试；撤单和查询有预留容量 |
| 连接失败，或开始写之前就出错 | `NotSent` | 同第一行 |
| 5xx/408/429/418，或错误码 -1000/-1001/-1006/-1007 | `Unknown`；429/418 还会暂停所有出口请求：按 `Retry-After`，没有就 429 等 1 秒、418 等 2 分钟 | 走查单兜底 |
| 下单被拒（其它 4xx） | `Rejected`，释放占用 | 下个周期重新报价；规则类拒绝会反复出现，要看 `last_error` |
| 下单前违反风控或规则 | `BeginSubmit` 返回错误，请求不发出 | 不影响账户 |
| 200 但内容对不上 | `Unknown` | 查单兜底 |
| 撤单返回非 200 | 一律 `Unknown`，记 `CancelUnknown`，安排查单 | 查到已撤或已成交就进终态；间隔两次查询都仍挂着，就判 `CancelRejected`，下一轮重撤 |
| 撤单回复的剩余数量比本地少 | 保留撤单待确认状态和占用 | 等私有流推来成交和终态回报 |
| 撤单没发出（NotSent） | 撤销撤单状态 | 每个行情事件都会重试撤单 |

### B3 交易所连接异常

| 异常 | 行为 | 处理与恢复 |
| --- | --- | --- |
| 公开行情 WS 断开、30 秒无数据、解析失败 | `Fault` → `FeedDisconnected` → 盘口回到 Syncing；如果之前是 Live，就发 `BookUnavailable`；策略要求撤掉全部挂单（账户就绪时才会真的发出） | 退避重连：250 ms 起翻倍，最长 30 s，然后重新拿快照对齐。**这个退避在整个进程生命周期内不会重置** |
| 盘口过期（Live 状态下 5 秒没更新） | `StaleLoop` → 进入 Syncing 并取消 WS 读取 | 同上 |
| 序号缺口、盘口交叉、增量缓冲满 | `EnterSyncing(reason, reconnect=true)` | 同上 |
| 快照请求返回 429/418 | 下次重连至少等 1 分钟（429）或 5 分钟（418） | 自动；如果 `max_snapshot_requests_per_run` 用完，行情协程退出 |
| 私有流断开 | 中止对账，取消查单，`Pause` | 退避重连：100 ms 起翻倍，最长 30 s；连接持续超过 1 分钟后退避重置。重连后全量对账，成功则自动恢复；断线期间只能用 `cancel_all` 撤单 |
| 私有流订阅被拒 | `PermissionDenied`，不再重连（`user_stream.cc:239`） | 修好 API key 后重启 |
| 交易所完全连不上 | 私有流断开导致暂停；还没写出的请求 `NotSent`，已写出的 `Unknown`；查单也失败，只能退避 | 交易所恢复后，行情重新对齐，私有流重连并对账；没送到交易所的订单落入 B1 的"查不到" |
| 只有 REST 不通 | 账户不暂停；下单 `NotSent`，撤单发不出去，每个行情事件重试 | REST 恢复后自动恢复 |

### B4 本地资源与持久化异常

| 异常 | 行为 | 处理与恢复 |
| --- | --- | --- |
| 可靠日志写失败 | 错误会一直保留，之后所有 `Append` 都失败 → 每个依赖日志的动作都会触发暂停 | 不能自动恢复；修好磁盘后重启 |
| 日志队列满（32768） | 下单时放弃这笔订单；处理回报时暂停 | 积压消化后靠对账恢复 |
| 账户任务队列满（8192） | 暂停（"account event queue overflow; state synchronization required"） | 下一次对账时自动恢复 |
| 对账期间私有回报缓冲满 | 放弃本轮对账，暂停 | 重连，或执行 `sync_account_state` |
| 观察历史队列满 | 记一个 Gap，不影响交易 | 不需要处理 |

### B5 状态不一致

| 异常 | 行为 | 处理与恢复 |
| --- | --- | --- |
| 外部余额变动 | 交易所推来的 `BalanceDelta` 触发暂停 | 60 秒周期对账自动恢复 |
| 交易所有本地不认识或对不上的挂单 | 对账暂停（"unrecognized or conflicting exchange open order"） | 人工处理这笔订单，然后 `resume_trading` |
| 本地有未终结的订单，交易所查不到 | 对账暂停（"restored order query failed"） | 人工处理，见 B1 |
| 三轮对账都被新回报打断 | 暂停，并且不再安排周期对账 | 执行 `sync_account_state`，或等私有流重连 |
| 账户副本前后矛盾 | `Pause(原因)` | 对账，或人工处理 |
| Shard 侧状态矛盾 | 账户关准入并暂停；该 Shard 执行 `StopFromError`，永久停止 | 重启进程 |
| 策略计算溢出 | 该 Shard 执行 `StopFromError` | 检查参数后重启 |

### B6 进程级异常

| 异常 | 行为 | 处理与恢复 |
| --- | --- | --- |
| 进程崩溃 | 已提交的日志都在 | 重启后：Load 日志，重建两份状态 → 等私有流连上 → 全量对账，对得上就自动恢复。以下两种情况会停在暂停状态：一是"请求已发出、意图还没落盘"的订单，会被当作交易所上本地不认识的挂单；二是交易所从没收到的订单，查单会失败 |
| 停机时交易所连不上 | 撤单发不出去，等 30 秒后放弃，记录 "shutdown left unconfirmed account orders" | 人工检查交易所上的残留挂单 |
| OnlineSim 达到下单次数上限 | 不再下单；上限设为 0 时，以只读验证方式运行 | 设计如此 |

## C. 模块篇

### C1 base/

- **fixed.h/.cc**：金额和比例统一用 `uint64_t` 表示，单位 1e-9。乘除的中间结果用 128 位，溢出时返回错误。盘口价格和数量则用整数 tick/lot。
- **ids.h**：用强类型区分 `ShardId`（上限 1000）、`MarketId`、`OrderId`、`ClientOrderId`、`ExchangeOrderId`；时间类型有 `MonoTime`（微秒）和用于回放的 `InputTime`。
- **net.h/.cc**：基于 Beast + OpenSSL 的协程式 `HttpClient` 和 `WebSocketClient`。HTTP 按需建连；GET 请求遇到"复用的旧连接已失效"时，会换新连接重试一次。下单和撤单请求不重试。
- **log.h/.cc**：Quill 异步日志。

### C2 shard/

- **market_type.h**：`MarketInput`（连接、快照、增量、成交、断开）带 `connection_epoch`；`BookView` 是给策略看的只读视图。
- **order_book.h/.cc**：盘口是两个 `btree_map<tick, lot>`，数量为 0 表示删除该价位；每次应用后检查两边非空且不交叉。
- **binance_feed.h/.cc**：一条组合流 WS（depth + trade）加 REST 快照，在一个协程里完成"连接 → 等增量 → 拿快照 → 主循环"。
- **market.h/.cc**：Syncing/Live 状态机，负责序号对齐、缓冲、过期检测；另外计算盘口和成交的 FNV 摘要，用于比对回放结果。
- **strategy.h/.cc**：纯函数 `Decide`，不做任何 IO。
- **trading_state.h/.cc**：Shard 本地的余额（**只随成交变化**）、占用和订单表；负责准入、应用结果和回报、从日志恢复。只由 Shard worker 写。
- **shard.h/.cc**：外壳，把上面几个模块组装起来，所有跨线程入口都在这里：`PostTradeEvent`、`PostTradeResponse`、`PostAccountView`、`PostCancelAll`、`PostStrategyUpdate`、`PostReplay` 等。

### C3 trading/

- **trading_event.h**：订单、回报、成交、余额事件；`AccountEvent` 是它们的 variant。
- **trade_port.h**：交易所抽象接口。返回 true 只表示本地队列接受了请求；返回 false 时不会调用完成回调。
- **client_order_id.h/.cc**：`hq` 加 32 位十六进制随机串。
- **order_state.h/.cc**：订单状态机，包括 `CancelPhase`、写出标记、按成交 ID 去重。
- **order_manager.h/.cc**：订单索引，`OpenFor`/`PendingFor`，以及查单计划（首次 2 秒，之后退避，上限 32 秒）。
- **risk_limits.h/.cc**：下单前风控；只在 Shard 准入时调用，所以各项限额按 Shard 生效。
- **funds_state.h/.cc**：账户级资金对账，可用额 = `max(0, free − 未反映的占用 − 未反映的成交借记)`。
- **order_journal.h/.cc**：SQLite（WAL + `synchronous=FULL`），共 13 种 `JournalKind`；凑批写入，单个事务提交，提交成功后才回调；一旦写失败，之后一直返回失败。
- **trading_account.h/.cc（+ _journal.cc）**：账户对象，维护对账副本、准入、任务队列（容量 8192，每批最多处理 64 个），负责先写日志再应用、分发、查单和暂停。锁：`queue_mutex_` 只保护队列；`state_mutex_`（递归锁）保护状态；`admission_gate_mutex_`（读写锁）保护准入开关；`journal_mutex_` 保证日志序号的顺序。
- **account_sync.h/.cc**：全量对账，用 `generation_` 让旧回调失效；对账成功才打开准入。
- **account_view_publisher.h/.cc**：只给脏的 Shard 发视图；`PublishedAssetView`（seqlock）目前只有测试在读，Shard 用的是自己的余额。

### C4 venue/binance/

- **auth**：HMAC-SHA256 签名，`recvWindow=5000`。
- **rules**：把 `exchangeInfo` 解析成 `VenueRule`。
- **request_scheduler**：全进程共享，按权重计数（挂单列表 80，余额/成交/规则 20，查单 4，下单和撤单 1），并统计每 10 秒的下单次数；遇到 429/418 时整体暂停。
- **trade_client**：`TradePort` 的实现，6 个优先级队列加 N 条 `RestLane`；所有完成回调都 post 到账户 io。
- **user_stream**：私有流，签名订阅，解析四种事件，断线自动重连。

### C5 顶层

- **config**：解析 YAML（schema v2）和 `ResolveShardAllocations`。
- **runtime**：组装各模块，分配线程，驱动回放，转发控制请求，按顺序停机。
- **control_server**：Unix socket 控制面。
- **sqlite_history**：观察用历史，**不是**可靠日志；队列满时记 Gap，回放模式下会等待。
- **replay_reader**：逐条读回放文件，读一条等一条。
- **perf_probe**：压测采样容器；不开压测时为空指针。
- **main.cc**：命令行参数、信号处理、启动和收尾。

## D. 设计取舍

| 选择 | 得到什么 | 代价 |
| --- | --- | --- |
| 资金按 Shard 预先切分，在 Shard 本地准入 | 热路径不跨线程、不等待 | 额度不能动态调配；同一笔钱要记两本账 |
| REST 连接跑在 Shard worker 上 | 从决策到写 socket 不用换线程 | HTTP 收发要占用 Shard 线程的时间 |
| 每个 Shard 同一时刻只有一批请求在途 | 状态简单，不会叠单 | 撤单完成之前不能挂新单 |
| 先写日志，再改状态 | 崩溃后能恢复，人工操作可追溯 | 每条回报都要等一次 SQLite 提交（批量提交可以摊薄这个开销） |
| 下单意图入队后立刻发送 | 落盘延迟不进入热路径 | 有一个窗口期：请求已发出，意图还没落盘。这时崩溃的话，重启后的对账会发现一笔本地不认识的挂单，账户暂停，要人工处理 |
| Shard 直接依赖具体的 `TradingAccount` 类 | 调用直接 | 两者耦合；Shard 测试需要一个真实的账户对象 |
| 出错一律暂停 | 不带着错误的状态继续交易 | 一处出问题，整个账户停止接单 |

建议的读代码顺序：

1. `shard/strategy.cc`
2. `shard/market.cc`
3. `shard/shard.cc:77`
4. `trading/trading_account.cc:152`
5. `venue/binance/trade_client.cc:554`
6. `trading/trading_account.cc:944`
7. `shard/trading_state.cc`
8. `trading/account_sync.cc`
