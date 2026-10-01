# Hummingbot C++ 开发实施文档

状态：实施说明。多 Agent 排程见 [DEVELOPMENT_PLAN.md](DEVELOPMENT_PLAN.md)，逐文件目录、target 和字段契约见 [STRUCTURE_AND_TYPES.md](STRUCTURE_AND_TYPES.md)。运行时、线程和风险语义以 [ARCHITECTURE.md](ARCHITECTURE.md) 为准；订单簿同步与内存结构以 [ORDER_BOOK.md](ORDER_BOOK.md) 为准；依赖版本以 [MODULE.bazel](../../MODULE.bazel) 为准。`../PLAN.md`、`../CORE_DESIGN.md` 和 `../DEPENDENCIES.md` 中的单交易线程、网络线程池、Recorder 兼做查询等描述属于旧方案，不能作为本实施文档的线程与模块所有权依据。实现中发现冲突时，先更新新架构文档和对应测试，再改代码。

## 1. 目标与首版边界

Python 行为基线固定为 `../../../hummingbot` 的 `9af100d6822da7d2d0291a906c730ef172284ee2`（2.17.0）。迁移的是可观察交易行为：订单状态、资金和费用、策略触发、动作顺序、恢复结果；不迁移 Python 的 GIL、每秒 Clock 调度、`asyncio.Queue` 和 SQLAlchemy 对象结构。

**第一条可运行链路：** Binance 现货公开行情 → 订单簿 → `simple_pmm` 的定时刷新 → 风控 → PaperConnector → 订单/成交事件 → SQLite 历史 → `hbot status/history/stop`。先用录制行情和本地协议服务器验证，再连接真实公开行情。此阶段不向交易所发送实盘订单。

运行时按架构默认配置最多 8 个分片槽位，只启动实际分到市场的分片。第一个模拟盘用一个活跃分片完成纵向链路；**实盘验收必须覆盖多个活跃分片和同账户共享资源**。生产配置默认忙轮询，开发与 CI 可选择阻塞 `run()`；核数不足按架构规定报警并降级或拒绝启动。

首版实盘范围为一个现货连接器、固定的策略与市场配置、静态账户额度和限速额度切分。XEMM、V2 Controller/Executor、回测、更多交易所和动态额度再分配在首条实盘链路稳定后逐项交付。配置中必须显式区分 `paper` 与 `live`，启动日志和 `status` 显示当前模式、账户、市场及是否允许新单。

## 2. 代码包与依赖方向

一个目录对应一个 Bazel package。下表是职责概览；准确文件名、target 和首版/后续边界以 [STRUCTURE_AND_TYPES.md](STRUCTURE_AND_TYPES.md) 为准。

| 包 / target | 负责什么 | 不负责什么 |
| --- | --- | --- |
| `//hbot/base:decimal`, `:ids`, `:logging`, `:json_writer` | libmpdec RAII、强类型 ID、时间、Quill 封装、出站 JSON | 交易所规则与线程调度 |
| `//hbot/model:domain` | `MarketId`、`BookScale`、`TradingRule`、订单/成交/费用/余额类型 | socket、SQLite、策略逻辑 |
| `//hbot/event:events` | 公开行情与账户回报的类型化事件、事件元数据 | 跨线程通用事件总线 |
| `//hbot/market_data:order_book` | L1/L2 同步状态机、整数 ticks/lots 价位簿、只读 `BookView` | 原始交易所协议解析 |
| `//hbot/net:transport`, `:rate_limit` | Asio/Beast HTTP、WS、TLS、连接复用、期限、取消、本地限速器 | 策略决策、订单归属 |
| `//hbot/connector:api`, `:order_tracker` | 连接器契约、通用订单状态机、成交去重、归属查询端口 | 具体交易所的签名和路径 |
| `//hbot/connector/binance_spot/public/...`、`order_gateway/...`、`private/...` | 分包实现 Binance 公开协议、签名/下单、私有回报/对账 | 跨交易所业务状态机 |
| `//hbot/connector/paper:paper` | 虚拟余额、模拟撮合、费用及私有回报生成 | 实盘网络发送 |
| `//hbot/strategy:api`, `:simple_pmm` | `TriggerPolicy`、`ActionBatch`、策略实例及首个策略 | 自行改订单簿或执行 SQL |
| `//hbot/risk:risk_gate` | 订单准入、分片资金租约、未知结果敞口、紧急停止 | 向交易所直接发单 |
| `//hbot/runtime:shard`, `:dispatcher` | 一个分片的 `io_context`、状态所有权、协程装配、动作执行、队列 drain | 全局 SQLite 连接 |
| `//hbot/storage:ports`, `:sqlite_recorder`, `:history_reader` | 有界入队接口、WAL 单写者、独立只读查询与版本化 schema | 决定策略何时发单 |
| `//hbot/control:protocol` | CLI 与 ControlServer 共用的 Unix socket 请求/响应 | 第二份交易状态 |
| `//hbot/app:engine`, `:control_server`、`//hbot/app/config:config` | 分片分配、启动/停止、账户共享资源、Unix socket 控制、YAML 装配 | 具体策略算法 |
| `//hbot/cli:cli`, `//apps:hbot`, `//apps:hbot-engine` | 前台命令和服务进程入口 | 交易状态的第二份所有者 |
| `//hbot/backtest:replay` | 后续阶段的模拟时钟与数据回放 | 另一套订单簿或订单状态机 |
| `//dev:dependency_smoke`, `//tests/...` | 依赖链接验证、协议 mock、重放与跨模块验收 | 正式运行链路 |

`strategy` 只依赖领域类型、只读市场视图与动作契约；`risk` 只依赖领域类型和只读订单/账户视图；它们都不能引用 Binance 适配器。`runtime` 把策略、风险、连接器抽象和记录端口装在一起；`app` 装配具体实现。用 Bazel `visibility` 阻止反向依赖。`absl::flat_hash_map` 用于索引，业务顺序用输入序号、`btree_map` 或显式排序，不能依赖哈希表遍历顺序。

## 3. 先固定的数据契约

在写网络和策略之前，先让以下类型在无网络的测试中稳定下来。它们在内存中使用强类型；进程间、SQLite 和 JSON 用带版本的显式字段，不直接序列化 Abseil 对象或 C++ 枚举数值。

| 契约 | 必有字段或规则 | 校验责任 |
| --- | --- | --- |
| `MarketId`、`OwnerId`、`RunId` | 市场；稳定策略/执行器归属；一次进程运行的唯一部分 | `ShardId` 只用于当前进程路由，不能充当持久归属 |
| `BookScale`、`BookSnapshot`、`BookDiff` | 行情步长、规范化首/末序号、ticks/lots、交易所时间与接收时间 | adapter 转换并检查可整除、范围；订单簿检查连续性 |
| `BookView` | 最优价、前 N 档、状态、新鲜度与当前序号 | 只在所属分片回调期间有效；跨分片只能使用发布的不可变快照 |
| `TradingRule`、`OrderRequest`、`TradeFee` | 下单步长、最小金额、方向、价格、数量、费用资产 | 金额/手续费用精确 `Decimal`；`BookScale` 不替代下单规则 |
| `TriggerPolicy`、`ActionBatch` | 触发源、合并窗口、冷却时间、有序 `Submit/Cancel/...` 动作 | 分片按确定顺序在策略返回后统一分发 |
| `ClientOrderId`、`ExchangeOrderId`、`ExchangeTradeId` | 三种 ID 分离；客户端 ID 含稳定归属和跨重启唯一部分 | adapter 按目标交易所字符/长度约束编码，不能只靠 SQLite 自增号 |
| `OrderIntent`、`RecoveryContext` | 原请求、归属、配置版本、必要执行器检查点 | 发送前尽力非阻塞入队；没有检查点的状态型执行器不自动恢复 |
| `OrderUpdate`、`TradeUpdate`、`TrackedOrder` | 双订单 ID、trade ID、状态、累计成交、费用和时间 | `OrderTracker` 唯一修改；先去重并核对身份，再更新资金和通知策略 |
| `ShardReport`、`StorageHealth` | 本地额度已用/预留、数据新鲜度、队列水位、历史缺口 | 带版本与时间；控制线程不能直接读分片可变对象 |

`Decimal` 与 Python 基线按明确的精度、舍入和异常规则做差分；非资金指标才使用浮点。订单簿 L2 首版按 [ORDER_BOOK.md](ORDER_BOOK.md) 的连续数组加远端 `absl::btree_map` 溢出区实施；L3 和股票交易时段规则留在扩展阶段。L2 数组重新居中、容量上限及异常报文回退必须有重放和基准数据，不能假定稳定运行绝不分配内存。

## 4. 线程、协程与所有权

| 执行位置 | 数量 | 唯一可变状态与任务 |
| --- | --- | --- |
| 分片线程 | 配置最多 8；只启动有市场的分片 | 每分片一个 `io_context`，独占其 socket、盘口、OrderTracker、策略、RiskGate 和本地限速桶 |
| 控制线程 | 1 | Unix socket、启动/停止命令、账户额度与健康汇总；通过有界命令队列控制分片 |
| Recorder 线程 | 1 | SQLite WAL 写连接，轮转消费各分片 SPSC，批量写入 |
| HistoryReader 线程 | 1 | 独立只读 SQLite 连接，有界分页查询；结果投回控制线程 |
| Quill 后台线程 | 1 | 诊断日志格式化和落盘 |
| 计算工作线程 | 0..M | 后续 V2 长耗时指标；只读不可变带版本快照，结果回分片复核 |

每个分片在自己的 `io_context` 上 `co_spawn` 公开 WS 循环、私有 WS 循环、REST 请求、快照加载、重连、余额/规则刷新、对账及策略定时器。`co_await` 挂起的是协程，不占一个专用等待线程；恢复的 handler 仍在该分片的同一执行器上运行。相同 socket 的读写、连接池槽位和超时取消由该分片串行管理，不在另一个线程碰触。策略回调是同步、短时执行，禁止在其中同步 DNS/HTTP、读取 SQLite 或等待未来结果。

忙轮询循环调用 `poll()` 并有工作保活，处理一轮就检查有界命令队列；阻塞模式调用 `run()`，生产者使队列从空变为非空时用 `asio::post` 唤醒目标分片。两种模式都限制单次 drain 数量，防止命令、私有回报或 Recorder 通知淹没 socket 事件。先实现并测通阻塞模式，再实现默认忙轮询和 Linux 绑核；在 macOS 上用阻塞模式完成正确性测试。

协程必须有明确的所有者、取消信号、总期限和退出等待。关闭顺序是：禁止新策略动作 → 继续处理必要撤单和回报 → 取消连接/定时器并等待分片协程结束 → drain 或标记 Recorder 余量 → 停 HistoryReader 与控制连接 → flush Quill。强制退出后的恢复路径单独测试；优雅退出不能假装成所有订单已成交或持仓已清零。

## 5. 三条实现路径

### 5.1 行情到动作

1. 公开 WS 协程接收字节；adapter 用 simdjson 解析原始十进制文本和序号，转换为该市场的 `BookScale`、整数 ticks/lots 和规范化事件。
2. 订单簿先缓存增量、异步取快照，再按序号回放。缺口、交叉或缓存溢出使该市场进入重同步；不可用/过期市场停止相关策略的新单，已知订单撤单仍可继续。
3. 在一个逻辑输入批次内先完成所有盘口更新，再按每个策略的 `TriggerPolicy` 通知一次。`simple_pmm` 的报价刷新由定时器驱动，默认 15 秒；盘口事件只更新可读取的价格。
4. 策略返回有序 `ActionBatch`；`ActionDispatcher` 检查依赖就绪、最小动作间隔、规则、分片额度和限速。拒绝动作有明确原因和统计，不能静默跳过。

### 5.2 动作到实盘订单及回报

1. 风险门预留最坏敞口；网关在本分片取得有界连接槽/发送资格，生成跨重启唯一的客户端订单 ID，登记 `PendingCreate`。组包和签名采用最终发送字节；发起写入前同步失败要撤销预留并产生本地失败事件。
2. 发起网络写入前对 `OrderIntent` 执行一次 `try_push`。队列满或存储故障时更新 `StorageHealth` 和历史缺口，**不等待 SQLite 提交，也不阻止通过风控的订单发送**。
3. 在所属分片发起 `async_write`；此时只表示操作已发起。写完成、HTTP 响应、私有 WS 接单和成交是后续事件，可能乱序。若无法证明请求未送达，进入 `SubmissionUnknown`，保留敞口，用原客户端 ID 补查，不生成新 ID 盲发。
4. `OrderTracker` 对 trade ID 去重，先更新订单与风险敞口，再发 `OnFill/OnOrderUpdate`。XEMM 等后续策略可在 `OnFill` 返回对冲动作，dispatcher 在回调结束后执行；公开成交绝不变成本账户成交。

PaperConnector 使用相同的请求、Tracker、风控和事件接口，只把网络网关替换为可重放的模拟撮合。模拟时间由 `ReplayClock` 注入，不能依赖真实 socket 收包批次决定策略语义。

### 5.3 SQLite、控制命令和重启

Recorder 的每分片 SPSC 记录带 `run_id`、稳定 owner、分片本地序号、交易所时间、接收时间及 schema 版本。单分片内顺序确定；不同分片没有全局订单序。HistoryReader 负责分页 `history` 和启动检查点查询，控制线程不执行 SQL，分片不因提交完成而触发策略或发单。

启动依次校验配置和分片分配、建立 `run_id`/归属索引、读取可用本地意图与检查点、连接交易所并补查挂单/成交/余额、对账后才打开新单门。SQLite 缺口、交易所历史窗口不足、未知订单归属或状态型执行器检查点缺失时，把受影响的策略保持暂停并报告原因。运行中 Recorder 失败则当前内存交易状态继续运作，`status/history` 显示不完整；重启不能承诺把交易所不可查询的内部执行器状态补回来。

同一账户跨分片的实盘启用前，控制线程必须按账户/币种分配**互不重叠的静态资金租约**，把在途与结果未知订单计入占用；账户级私有流由指定读取分片按稳定归属转发，未知归属或队列满则降级并补查。限速首版在各分片静态分配本地令牌，保留下单/撤单余量，并用原子全局熔断处理超限；动态借用全局余量以后再做。

## 6. 交付顺序与完成条件

每一阶段保持 `bazel build //...`、本阶段测试和可运行演示通过。下阶段只依赖上一阶段已经固定的数据契约；有意改变 Python 可观察行为时，在差分报告中写明原因。

| 阶段 | 主要提交内容 | 必须能演示或验证的结果 |
| --- | --- | --- |
| D0 工程与基线 | `//dev:dependency_smoke` 真正链接首批依赖；配置 Bazel/CI；从 Python 提取脱敏盘口、订单、`simple_pmm` fixtures | macOS 与 Linux 构建；关键依赖可链接；夹具记录输入、期望结果和来源版本 |
| D1 领域内核 | Decimal、ID/时间/规则、类型化事件、OrderTracker、L2 订单簿及状态机 | Decimal 边界逐位一致；快照+增量、重复/缺口、成交先到/撤单竞态可确定重放 |
| D2 分片与模拟撮合 | `ShardRuntime`、两种事件循环、`TriggerPolicy`、dispatcher、RiskGate、PaperConnector、ReplayClock | 无外网时 `simple_pmm` 定时动作、成交和余额可重放；策略及订单只在所属分片修改 |
| D3 存储与进程控制 | Recorder、HistoryReader、schema 版本、`hbot-engine`、`hbot start/status/history/stop` | 订单/成交可查询；`history` 大查询不阻塞 `stop`；队列满/写失败可见但模拟交易继续 |
| D4 公开行情模拟盘 MVP | Asio/Beast 传输、Binance 公开 WS/REST 快照、重连、同步状态机、PaperConnector 端到端 | 本地 HTTP/WS mock 和真实公开行情均可驱动 paper；断线/缺口时相关市场停止新单并恢复；录制流可复现动作 |
| D5 多分片与账户边界 | 多活跃分片、静态资金/限速切分、归属索引、账户级私有流路由、全局熔断、CPU 模式/绑核 | 同账户多市场不重复授予资金；回报归属正确；队列满/未知归属降级；8 分片压力数据有记录 |
| D6 首个实盘连接器 | Binance 现货只读余额/规则、签名下单/撤单、私有流、查询对账、客户端 ID 规则 | 隔离测试环境跑创建、部分成交、撤销、重连、进程中断与恢复；结果未知不盲重发；通过实盘启用门槛 |
| D7 扩展 | XEMM、V2 Controller/Executor、回测及更多连接器按业务优先级逐项增加 | 每个新策略/连接器复用同一契约，并新增 Python 差分夹具和故障测试 |

D0 的第一项具体改动是在已有的 [`dev/`](../../dev/README.md) 中增加 `dependency_smoke.cc` 与 `//dev:dependency_smoke`，实际引用 Abseil、Quill、Asio/Beast、OpenSSL、libmpdec、simdjson、yaml-cpp、SQLite 和 CLI11，执行 `bazel build //dev:dependency_smoke` 与 `bazel test //dev:dependency_smoke`。macOS 编译和直接运行已通过；Linux 构建仍待验证。`MODULE.bazel.lock` 只说明解析过依赖，不能替代编译与链接验证。

## 7. 贯穿各阶段的验证

| 风险 | 测试输入 | 验收判断 |
| --- | --- | --- |
| Python/C++ 行为漂移 | 固定的规范化行情、定时器和私有回报夹具 | 比较动作种类/顺序、订单状态、余额、费用；明确列出调度语义差异 |
| 订单簿错误 | 快照前增量、重叠序号、重复、缺口、交叉、过期和缓存溢出 | 进入正确状态；不可用市场不放新单；重同步后恢复确定 |
| 网络不确定性 | 本地 mock 的连接复用、超时、取消、HTTP/WS 乱序、429/418、TLS 失败 | 有界资源使用；未知提交结果不盲重发；全局熔断阻止受影响请求 |
| 账户过量下单 | 两个以上分片同时使用同账户/币种，注入未知订单和延迟回报 | 租约总额不超过保守可交易上限；未知结果继续占额度 |
| 历史缺失与恢复 | 发单后落盘前终止、Recorder 队列满/磁盘失败、检查点缺失 | 运行中不等写库；缺口可见；重启后无法验证的状态型执行器不自动运行 |
| 并发与延迟 | 阻塞/忙轮询两模式、多个活跃分片、8 分片压力 | ASan/UBSan、TSan 通过；记录 T0→T2、T2→T3、T3→T4、队列水位、回调耗时与 p99 |

正常单元测试和协议集成测试不依赖公网。Bazel targets 建好后，开发门槛为 `bazel test //...`；另在支持的平台运行 `bazel test --config=asan //...` 与 `bazel test --config=tsan //...`。性能测试保留测试机器、核数、事件循环模式、分片数和输入流，不能只报告一个没有上下文的延迟数字。交易所的连接/限速/客户端 ID 规则以接入时的协议验证和隔离环境测试为准。

## 8. 当前状态与下一步

目前仓库已有 Bazel 依赖声明、锁文件、第三方 libmpdec BUILD、macOS 上通过的依赖 smoke 和架构文档；`hbot/`、`apps/`、`tests/` 的正式实现尚未建立。下一步完成 D0 的 Linux 构建与 Python 行为夹具，再提交 D1 的领域类型与状态机。任何声称“模拟盘可用”或“可以实盘”的里程碑，都以第 6 节对应的可运行演示和第 7 节故障验证为准。
