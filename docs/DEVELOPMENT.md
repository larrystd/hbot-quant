# 开发指南：写法、协作与验证

本文说明怎么写代码、怎么分工与集成。关口和进度见 [ROADMAP.md](ROADMAP.md)；运行时语义以 [ARCHITECTURE.md](ARCHITECTURE.md) 为准；逐文件目录、Bazel target 与字段以 [STRUCTURE_AND_TYPES.md](STRUCTURE_AND_TYPES.md) 为准；订单簿以 [ORDER_BOOK.md](ORDER_BOOK.md) 为准。实现中发现冲突，先更新文档和对应测试，再改代码。

## 1. C++ 语言与实现约束

**全项目以 `-std=c++20` 编译。** 普通领域代码优先用简明的 C++17/20 写法；网络协程需要 C++20，不承诺能用 `-std=c++17` 编译；不得引入 C++23 作为构建要求。

| 场景 | 首选写法 |
| --- | --- |
| 领域值与所有权 | `enum class`、强类型 ID、RAII、`std::unique_ptr`、`std::optional`、`std::variant`、`std::vector`；避免共享可变对象和拥有所有权的裸指针 |
| 参数、时间与只读视图 | `std::string_view`、`std::span`、`std::chrono`；返回视图时明确生命周期，策略不能把 `OrderBookView` 保存到回调之后 |
| 异步网络 | C++20 `co_await` 与 Asio `awaitable`/`co_spawn`；每个协程绑定所属分片执行器，定义取消、超时与退出等待；订单簿、OrderTracker、策略和风控保持普通同步函数 |
| 线程与跨分片 | 分片内独占可变状态；跨分片只用有界消息或明确同步语义的不可变快照 |
| 错误与容器 | 按 [ERRORS.md](ERRORS.md) 用 `ErrorCode` 表达业务原因、`Recovery` 决定处理方式，`absl::Status/StatusOr` 承载跨边界错误；热路径拒单直接返回 `ErrorCode`。索引用 `absl::flat_hash_map`，有序价位用 `absl::btree_map`；哈希遍历次序不能决定交易动作顺序 |
| 格式与兼容性 | 资金域用 libmpdec `Decimal`，JSON/SQLite 用十进制字符串；`std::format`、C++20 模块和较新的 chrono 时区 API 不作为基础能力，先在 macOS/Linux 工具链验证再用 |

第三方异常只在 adapter、YAML 或网络边界转换为 `Status`；业务状态机不靠异常表达正常分支。优先写可读的显式状态转移，不为使用模板、ranges 或协程而抽象。每个模块只直接依赖自己用到的库。格式化用 `./op.sh fmt`（LLVM 20 的 clang-format）。

### 1.1 包职责

`hquant/src/` 下每个模块是一个 Bazel package；文件与 target 清单见 [STRUCTURE_AND_TYPES.md](STRUCTURE_AND_TYPES.md) 第 2 节。`hquant/test/` 统一拥有单元、协议、夹具和端到端测试。

| 包 / target | 负责什么 | 不负责什么 |
| --- | --- | --- |
| `//hquant/src/base:{error,types,market,order}` | 业务错误码与恢复方式、Decimal、强 ID/时间、行情与订单拥有值、网关端口 | socket 操作、SQLite、策略逻辑 |
| `//hquant/src/base:{net,rate_limit}` | HTTP、WS、TLS、本地限速与全局熔断 | 策略决策、订单归属 |
| `//hquant/src/market:{order_book,replay_feed,binance_spot_feed}` | L2 簿和只读视图、固定行情回放、Binance 公开行情 | 下单与账户私有回报 |
| `//hquant/src/order:{order_tracker,risk,simulated_exchange}` | 双 ID 跟踪、成交去重、风险额度、模拟盘 | 分片线程调度 |
| `//hquant/src/order:{binance_spot_gateway,binance_spot_account}` | Binance 签名/下撤单与私有回报/对账 | 跨交易所通用状态机 |
| `//hquant/src/strategy:{strategy,simple_pmm}` | `TriggerPolicy`、`ActionBatch`、`simple_pmm` | 修改订单簿或执行 SQL |
| `//hquant/src/shard:{shard,action_executor,routing}` | 分片 `io_context`、动作执行、账户级回报路由 | 全局 SQLite 连接与具体组件装配 |
| `//hquant/src/storage:{storage,record_codec,recorder,history}` | 入队端口、WAL 写入、分页查询、恢复与 schema | 决定策略何时发单 |
| `//hquant/src/application:{config,quant_server,launcher}` | YAML 配置、控制协议/服务、线程启动与组件装配 | 策略算法 |
| `//hquant/src/cli:cli`、`//apps:{hquant,hquant_engine}` | 前台命令和进程入口 | 交易状态 |
| `//hquant/test/...`、`//dev/...` | 单元、协议、夹具、端到端和契约编译 | 正式运行链路 |

`strategy` 只读取市场的 `OrderBookView` 和基础值类型；`market` 与 `order` 互不依赖。`shard` 通过端口运行分片，`application` 装配具体实现。用 Bazel `visibility` 约束反向依赖。

## 2. 线程、协程与关闭

| 执行位置 | 数量 | 唯一可变状态与任务 |
| --- | --- | --- |
| 分片线程 | 配置最多 8；只启动分到市场的分片 | 一个 `io_context`，独占其 socket、盘口、OrderTracker、策略、RiskGate 和本地限速桶 |
| 控制线程 | 1 | Unix socket、启动/停止命令、账户额度与健康汇总；通过有界命令队列控制分片 |
| Recorder 线程 | 1 | SQLite WAL 写连接，轮转消费各分片 SPSC，批量写入 |
| HistoryReader 线程 | 1 | 独立只读连接，有界分页查询；结果投回控制线程 |
| Quill 后台线程 | 1 | 诊断日志格式化和落盘 |
| 计算工作线程 | 0..M | 后续 V2 长耗时指标；只读不可变带版本快照，结果回分片复核 |

- 每个分片在自己的 `io_context` 上 `co_spawn` 公开 WS、私有 WS、REST 请求、快照加载、重连、余额/规则刷新、对账及策略定时器。相同 socket 的读写、连接池槽位和超时取消由该分片串行管理。策略回调同步、短时，禁止同步 DNS/HTTP、读 SQLite 或等待未来结果。
- 忙轮询循环调用 `poll()` 并有工作保活，每轮检查有界命令队列；阻塞模式调用 `run()`，生产者使队列从空变非空时 `asio::post` 唤醒。两种模式都限制单次 drain 数量。**先实现并测通阻塞模式，再实现忙轮询和 Linux 绑核**；macOS 上用阻塞模式做正确性测试。
- 协程必须有明确的所有者、取消信号、总期限和退出等待。**关闭顺序：** 禁止新策略动作 → 继续处理必要撤单和回报 → 取消连接/定时器并等待分片协程结束 → drain 或标记 Recorder 余量 → 停 HistoryReader 与控制连接 → flush Quill。强制退出后的恢复路径单独测试；优雅退出不能假装所有订单已成交或持仓已清零。

### 2.1 三条实现路径的要点

**行情到动作：** adapter 用 simdjson 解析原始十进制文本和序号，按 `TickLotSize` 转整数事件 → 订单簿缓存增量、异步取快照、按序号回放，缺口/交叉/溢出进入重同步 → 一个输入批次内先完成全部盘口更新，再按各策略 `TriggerPolicy` 通知一次 → 策略返回 `ActionBatch`，ActionExecutor 检查依赖就绪、最小动作间隔、规则、分片额度和限速，拒绝要有原因和统计。

**动作到实盘订单：** 风险门预留最坏敞口 → 网关取得有界连接槽、生成 client ID、登记 `PendingCreate`，按最终字节签名；发起写入前的同步失败撤销预留并产生本地失败事件 → `try_push(PreparedOrder)`，失败只标记缺口 → 发起 `async_write`；写完成、HTTP 响应、私有 WS 回报是后续可能乱序的事件；无法证明请求未送达则 `SubmissionUnknown` → OrderTracker 去重并先更新订单与风险，再回调 `OnFill/OnOrderUpdate`。模拟盘 用相同接口，只把网关换成可重放撮合。

**启动与多分片：** 校验配置和分片分配 → 建立 `RunId` 与归属索引 → 读取可用意图与检查点 → 连接交易所补查挂单/成交/余额 → 对账后才打开新单门（详见 [ARCHITECTURE.md](ARCHITECTURE.md) 第 11.3 节）。同账户跨分片前，控制线程按账户/币种分配互不重叠的静态资金额度，账户级私有流由指定分片按 strategy_id 转发，限速按分片静态分配并保留撤单余量，429/418 触发全局熔断。

## 3. 多 Agent 协作规则

1. **按模块独占写入：** `hquant/src/{base,market,order,strategy,shard,application,storage,cli}/` 各有一个 `BUILD.bazel`；并行任务须指定文件所有者，避免两个 Agent 同时修改同一文件。
2. **测试集中管理：** 单元和端到端测试平铺在 `hquant/test/`，夹具分在 `hquant/test/fixtures/{order_book,order_tracker,simple_pmm,simulated_exchange,v1}/`。集成负责人统一维护 `hquant/test/BUILD.bazel` 和 fixture loader；并行实现任务写各自的新测试文件。
3. **公共接口先同步：** 发现头文件或 target 依赖问题时，先说明字段、调用方和测试场景，再由该模块负责人修改。`apps/`、`dev/`、根 Bazel 文件和跨模块文档由集成负责人统一检查。
4. **交付完整模块：** 实现、BUILD、对外 API 说明、离线单元/协议测试、运行命令和未解决问题。局部测试运行 `bazel test //hquant/test:<name>`；集成后跑 `bazel test //...`、端到端回放和相关 sanitizer。
5. **按依赖顺序集成：** `base → market/order/storage → strategy → shard → application/cli → apps`。共享 Bazel 输出目录可能串行化并发构建。

仓库已有 Git 基线。并行开发使用互不冲突的文件所有权或独立 worktree，并以小批次合并与验证。

### 3.1 提交与集成门槛

每个任务包写明：公共契约版本、修改路径、Bazel target、局部测试结果、夹具来源、已知差异。集成时检查依赖方向和 `visibility`：`strategy` 不依赖具体交易所；`net` 不碰策略/资金；SQLite 实现只在装配边界注入；共享夹具只读。

| 范围 | 必跑的最小验证 |
| --- | --- |
| 每个包 | `bazel test //hquant/test:<相关测试 target>` 或对应 fixture runner；无外网；告警和内存错误消除 |
| 每次集成 | `bazel build //...`、`bazel test //...`（`./op.sh ci`）、可见性检查；macOS 与 Linux 均留结果 |
| G1 | 虚拟时钟差分回放；Decimal、动作顺序、订单状态、余额和手续费逐步断言；Recorder 故障仍能推进模拟盘 |
| G2 | 本地 REST/WS mock 注入重复、乱序、断线、超时、429、盘口缺口；不就绪市场停止新单、撤单继续 |
| G3 | 两个以上活跃分片与同账户/币种；额度总量、未知单占用、路由/队列满、忙轮询与阻塞模式；Linux TSan 与 8 分片压力 |
| G4 | 隔离账户中的接单、部分成交、撤单、写超时、`SubmissionUnknown`、进程中断与重启对账 |

Sanitizer：`bazel test --config=asan //...`、`bazel test --config=tsan //...`（smoke 用 `./op.sh asan|tsan`）。性能结果附机器、核数、分片数、输入流与事件循环模式。
