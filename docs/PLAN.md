# Hummingbot C++ 重写计划

> 本文的线程与网络池设计属于旧方案。当前分片架构及多 Agent 开发排程分别见 [新架构](new_arch/ARCHITECTURE.md) 与 [并行开发计划](new_arch/DEVELOPMENT_PLAN.md)。

## 目标与基线

- 源码基线：`../../hummingbot` 的 `9af100d6822da7d2d0291a906c730ef172284ee2`，包版本 `2.17.0`。运行链路参考 [`ARCHITECTURE.md`](../../hummingbot/docs/ARCHITECTURE.md)。后续源仓库更新须单独评估，避免迁移目标不断漂移。
- 目标：在本目录构建以 C++20 为运行核心的 Hummingbot。最终覆盖实时交易、模拟盘、V2 Controller 回测、持久化和命令行操作；运行时不依赖 Python 或 Cython。
- 第一个可用版本：一个现货连接器的公开行情、本地模拟盘、`simple_pmm` 等价策略、订单/成交记录，以及可启动、观察、停止机器人的 CLI。其后才接入真实下单。
- 各阶段按可观察行为验收：配置、事件、订单状态、余额、费用和结果。保留源项目 Apache-2.0 许可证及必要的来源说明。

## 目标架构

模块依赖、线程、行情和实盘订单的数据流详见 [ARCHITECTURE_DIAGRAM.md](ARCHITECTURE_DIAGRAM.md)。

```text
CLI / 配置
    ↓
TradingEngine ── 交易线程 / Scheduler ── 风控 / 生命周期
    ├─ ConnectorManager ── IExchangeConnector ── REST / WebSocket / Gateway
    │   ├─ OrderBook + CandleFeed
    │   └─ OrderTracker + Balance / TradingRules
    ├─ Strategy ── Controller ── ExecutorOrchestrator ── Executor
    ├─ 网络 I/O 池 ── REST / WS 协程，向交易线程投递规范化消息
    ├─ OrderIntentStore ── 意图与检查点非阻塞入队 ── SQLiteRecorder
    ├─ EventBus ── 同步调用策略 OnEvent / 异步记录账户回报
    ├─ Quill ── 后台诊断日志
    └─ BacktestEngine ── HistoricalDataProvider + ExecutorSimulator
```

| Python 基线 | C++ 模块 | 首要职责 |
| --- | --- | --- |
| `TradingCore`、`HummingbotApplication`、`Clock` | `TradingEngine`、`IClock`、`Scheduler` | 装配、启动、tick、优雅停止与可注入时间 |
| `ConnectorManager`、`ExchangePyBase` | `ConnectorManager`、`IExchangeConnector` | 交易所适配、限速、认证、行情与订单接口 |
| `OrderBookTracker`、`MarketDataProvider` | `OrderBookService`、`MarketDataService` | 快照/增量、K 线、价格和深度查询 |
| `InFlightOrder`、`ClientOrderTracker`、`MarketEvent` | `Order`、`OrderTracker`、类型化 `EventBus` | 状态机、成交去重、订单事件 |
| `StrategyV2Base`、Controller、Executor | `IStrategy`、`IController`、`IExecutor` | 决策、动作队列和订单执行 |
| `MarketsRecorder`、SQL models | `IOrderIntentStore`、`SQLiteRecorder` | 意图、执行器检查点及订单/成交回报异步批量记录；报告存储缺口 |
| `BacktestingEngineBase` | `BacktestEngine` | 历史数据驱动和确定性结果 |

关键约束：

1. 金额、价格、数量和手续费使用有明确精度与舍入规则的十进制类型；序列化为十进制字符串。浮点数只用于非资金指标。
2. 专用交易线程由 Scheduler 调用策略 `OnTick`、由 EventBus 调用 `OnEvent`；网络 I/O 池默认两个线程运行 REST/WS 协程，每个 socket 和共享限速器各有 strand，规范化消息通过有界队列进入交易线程。公开订单簿按交易所序号应用增量，缺口触发重新取快照。
3. `client_order_id`、`exchange_order_id`、`trade_id` 分开保存。重复回报不重复记账，乱序回报可恢复或进入待核对状态。
4. 交易线程不直接访问 SQLite，也不等待提交：实盘新单通过风控后立即投递网络池异步发送，意图、归属、执行器检查点和交易所回报尽力交给 Recorder 后台批量记录。SQLite 提交不触发策略或订单发送；其数据用于 `history`/报告以及重启时结合交易所事实对账。Recorder 故障时继续交易并告警、标记历史缺口；当前进程保持内存风险控制，重启时缺失检查点的状态型执行器不自动恢复。客户端订单 ID 不依赖 SQLite 保持跨重启唯一，并可附带紧凑归属编码；写请求结果未知时不盲目重发。
5. 撤销挂单、关闭执行器和主动平仓分别定义。停止程序不会隐式把账户持仓全部平掉。
6. 配置和持久化格式加版本号；现有 YAML 配置和加密凭据提供迁移工具，凭据不得出现在日志、命令参数或测试夹具中。

## 技术基线

依赖和网络模型已定在 [DEPENDENCIES.md](DEPENDENCIES.md)，模块边界、结构体及 Python/C++ 语义对照已定在 [CORE_DESIGN.md](CORE_DESIGN.md)。根目录 [`MODULE.bazel`](../MODULE.bazel)、[`.bazelversion`](../.bazelversion) 与 `MODULE.bazel.lock` 锁定构建和依赖版本。首批采用 C++20、Bazel 9（Bzlmod）、Abseil、Quill、Boost.Asio/Beast、OpenSSL 3.5、zlib、libmpdec、simdjson、yaml-cpp、SQLite、CLI11；GoogleTest 与 Google Benchmark 为开发依赖。构建兼容性仍需在阶段 1 实测。

## 分阶段交付与验收

| 阶段 | 工作 | 可验收结果 |
| --- | --- | --- |
| 0. 冻结行为基线 | 列出当前 CLI、V1/V2、连接器、模拟盘、回测功能矩阵；从 Python 单测和捕获的脱敏消息生成订单簿、订单状态、策略动作的 golden fixtures；定 C++ 接口与配置版本 | 一份逐项迁移清单；每个首批功能有输入、期望事件和误差规则；依赖版本已锁定 |
| 1. 工程与交易内核 | 建 Bazel 工程、CI、测试框架；实现 Decimal、时间、交易对、交易规则、事件、订单状态机、交易线程调度器和可注入时钟 | macOS/Linux 可编译；状态机与舍入测试通过；`OnTick`/`OnEvent` 只在交易线程执行；无网络条件下可重放事件 |
| 2. 行情接入 | 实现网络 I/O 池与各连接的 strand、REST 连接复用、超时和幂等读取重试，WS 重连、公开 Binance 现货适配器、跨线程有界投递、快照与深度增量、公开成交、Candle feed | 本地 mock 覆盖复用、429、超时与取消；双网络线程下交易状态无竞态；重放乱序/重复/缺口消息时盘口正确；队列满时重新同步 |
| 3. 模拟盘 MVP | 实现虚拟余额、模拟撮合、手续费、`simple_pmm`、基础 CLI、SQLite 订单/成交与重启恢复 | 从配置启动到下单、成交、日志、历史查询、停止全程可运行；确定性重放得到相同结果 |
| 4. 首个实盘连接器 | 先只读验证余额和规则，再做认证下单/撤单、异步批量记录、启动对账、私有用户流、REST 补查、限速与时钟同步 | 测试环境中创建、部分成交、完成、撤销、断线及重启对账均通过；Recorder 队列满/写入失败仍能发单且显示历史缺口；发出后、记录落盘前被终止可按交易所历史核对；写请求超时不盲目重发 |
| 5. V2 执行框架 | 实现 Controller 动作队列和 ExecutorOrchestrator；先迁移 Order/Position，再迁移 DCA/Grid 等执行器与典型 Controller；异步记录风险相关状态快照 | 相同输入下与 Python 基线的动作序列、订单状态和费用计算一致；重启后能验证并恢复执行器状态，无法验证时不自动运行；控制器可安全停止 |
| 6. 回测 | 历史 K 线提供者、模拟时间、Position/DCA/Grid/Order 模拟器、结果与 PnL 汇总 | 同一数据与配置重复运行结果一致；与 Python 回测基线逐项对比并记录模型差异 |
| 7. 扩展覆盖 | 按业务优先级迁移更多现货/永续/Gateway 连接器、V1 策略、更多 V2 执行器，以及 CLI/TUI、远程接口 | 每个新增模块都通过统一连接器/策略契约、故障注入和端到端回归；功能矩阵明确剩余差异 |

阶段 3 是第一个可交付版本。阶段 4 的真实下单仅在模拟盘、回放、故障注入与交易所测试环境均达标后启用。阶段 7 需要按实际使用的连接器和策略逐项排期；源码目录数量不能直接当作已完成的交易所数量。

## 验证门槛

- **差分重放：** 同一脱敏 REST/WS 事件流分别送入 Python 与 C++，比较最优买卖价、订单状态、余额、成交和策略动作；资金数值要求十进制精确一致，盘口先按交易所步长量化再比较，时间差仅允许在明确定义的调度窗口内。
- **故障注入：** 覆盖断线重连、消息重复/乱序/缺口、HTTP 超时与 429、限速、部分成交、撤单与成交竞态、网络发送后但记录落盘前的进程中断、网络入站/命令队列满、Recorder 队列满与磁盘写入失败，以及客户端订单 ID 跨重启不复用；验证存储故障不增加发单延迟，`history` 查询不阻塞策略回调。
- **实盘启用门槛：** 当前运行内存风险检查正常、所有未完成订单能与交易所核对；重启时归属、执行器状态或账户事实未知的部分不自动运行；配置最大订单额、总敞口与紧急停止；先用隔离的测试账户验证完整链路。
- **回归：** 单元测试、模拟服务器集成测试、确定性回测和 Sanitizer 检查纳入 CI。外部 API 不作为常规单元测试前提。

## 首批实施任务

1. 在已有的 `MODULE.bazel`、`.bazelrc`、`MODULE.bazel.lock`、`third_party/mpdecimal/` 基础上，于 `dev/` 建 `//dev:dependency_smoke`，再建 `hbot/` 各模块的 `BUILD.bazel`、`apps/` 和 CI；核对锁文件并纳入版本控制，验证 macOS/Linux 构建及 ASan/TSan 配置。只有真正链接各首批依赖的 target 通过，才算依赖选型完成编译验证。
2. 实现 `Decimal`、强类型订单 ID、`TradingPair`、`TradingRule`、`OrderUpdate`、`TradeUpdate`、`AccountEvent`/`PublicTrade`、`RecoveryContext`/`OrderIntent` 的 C++ 契约。
3. 从 Python 基线提取首批脱敏 golden fixtures：盘口快照/增量、部分成交与撤单竞态、`simple_pmm` 报价。
4. 实现并验证 `OrderTracker` 和可注入时钟；之后开始公开行情适配器。
