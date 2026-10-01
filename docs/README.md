# HQuant C++ 文档

本目录是 HQuant 的设计与开发文档。项目以 Hummingbot 的 Python 实现为行为基线，使用 C++ 重写。架构采用**分片线程**模型：每个分片线程独占其策略需要的行情、订单、资金状态和 socket。早期的“单交易线程 + 网络 I/O 线程池”方案已废弃，其中仍然成立的内容已并入下列文档。

当前代码按职责放在 `hquant/src/`，全部测试与夹具放在 `hquant/test/`。

## 1. 文档地图

| 文档 | 内容 | 什么时候读 |
| --- | --- | --- |
| [ROADMAP.md](ROADMAP.md) | 目标、Python 基线、交付版本、开发关口 G0–G5、当前进度、验证与实盘启用门槛 | 想知道做到哪一步、下一步做什么 |
| [ARCHITECTURE.md](ARCHITECTURE.md) | 进程与线程、分片内部、热路径、回报路径、多分片资源、策略触发、状态机、持久化与重启对账、延迟测量 | 理解运行时语义；线程、发单和风险语义以此为准 |
| [CONNECTIONS.md](CONNECTIONS.md) | 程序与交易所之间的五种通道、连接数量、顺序保证、断线处理 | 接入或调试交易所连接 |
| [ORDER_BOOK.md](ORDER_BOOK.md) | 行情类型、同步状态机、增量应用规则、L2/L3 内存结构、策略读取方式 | 实现或调试订单簿 |
| [STRUCTURE_AND_TYPES.md](STRUCTURE_AND_TYPES.md) | 目录与 Bazel 包、依赖方向、关键数据类型与不变量、Python/C++ 语义对照、契约测试 | 写代码前确认文件、target 和字段 |
| [DEPENDENCIES.md](DEPENDENCIES.md) | 第三方依赖选型与版本、Bazel 约定、各库的使用边界 | 改构建、引入或升级依赖 |
| [DEVELOPMENT.md](DEVELOPMENT.md) | C++ 写法约束、协程与关闭规则、多 Agent 协作、任务包与排程、工作单、测试命令 | 领取任务、提交与集成 |
| [ERRORS.md](ERRORS.md) | 业务错误码：编号规则（10000 起，每类一个千位段）、处理方式、完整错误码表、迁移步骤 | 新增或处理错误时 |
| [refactor/PLAN.md](refactor/PLAN.md) | 目录重构计划：目标结构、新旧文件对应、分阶段步骤与检查项 | 执行或审查目录重构 |

文档冲突时的优先级：运行时与线程语义以 `ARCHITECTURE.md` 为准；文件名、target 与字段以 `STRUCTURE_AND_TYPES.md` 为准；订单簿算法以 `ORDER_BOOK.md` 为准；依赖版本以仓库根目录的 [`MODULE.bazel`](../MODULE.bazel) 为准。实现中发现冲突，先更新文档和对应测试，再改代码。

## 2. 一句话架构

```text
hquant（CLI）──Unix socket──▶ hquant_engine
                              ├─ 分片线程 × ≤8：行情 WS / 下单连接 / 私有 WS → 订单簿 / OrderTracker / 策略 / 分片风控 → 异步写
                              ├─ 控制线程：ControlServer、账户额度租约、健康汇总
                              ├─ Recorder 线程：SQLite WAL 批量写（不阻塞发单）
                              ├─ HistoryReader 线程：只读分页查询（history / 启动恢复）
                              └─ Quill 线程：诊断日志
```

## 3. 基线与范围

- Python 行为基线：`../../hummingbot` 的 `9af100d6822da7d2d0291a906c730ef172284ee2`（包版本 `2.17.0`）。迁移的是**可观察交易行为**（订单状态、资金、费用、策略触发、动作顺序、恢复结果），不迁移 GIL、`asyncio`、每秒 `Clock` 轮询或 SQLAlchemy 对象。
- 首条链路：Binance 现货 + `simple_pmm` + Paper，随后是多分片下的隔离环境实盘。XEMM、V2 Controller/Executor、回测和更多连接器在其后逐项迁移。
- 构建：C++20、Bazel 9.2.0（Bzlmod），依赖见 [DEPENDENCIES.md](DEPENDENCIES.md)。

## 4. 快速运行

固定行情 Paper（离线）：

```bash
bazel run //apps:hquant -- start   --config examples/simulated_replay.yaml --state-dir /tmp/hquant-paper-demo
bazel run //apps:hquant -- status  --state-dir /tmp/hquant-paper-demo
bazel run //apps:hquant -- history --state-dir /tmp/hquant-paper-demo --limit 20
bazel run //apps:hquant -- stop    --state-dir /tmp/hquant-paper-demo
```

真实公开行情驱动 Paper：把配置换成 `examples/simulated_binance_pmm.yaml`。`start` 在前台运行，其余命令在另一个终端执行。全量测试：`bazel test //...`。

本机验收记录见 [`dev/VALIDATION_2026-09-27.md`](../dev/VALIDATION_2026-09-27.md)，Linux CI 状态见 [`dev/LINUX_VALIDATION.md`](../dev/LINUX_VALIDATION.md)。

## 5. 常用术语

| 术语 | 含义 |
| --- | --- |
| 分片（shard） | 一个 OS 线程 + 一个 `io_context`，独占若干市场的行情、订单、策略、风控与 socket |
| owner | 稳定的策略/执行器归属 `{strategy_id, strategy_id, executor_id?}`；跨重启不变，分片号不是 owner |
| `BookScale` | 行情流的价格/数量步长，用于把盘口转成整数 ticks/lots；不同于下单规则 `TradingRule` |
| `ActionBatch` | 策略回调返回的有序动作列表，由 `ActionDispatcher` 在回调结束后逐个验证执行 |
| `SubmissionUnknown` | 写请求结果不明；保留最坏敞口，用原 client ID 补查，绝不换 ID 重发 |
| `AwaitingFills` | 交易所已报 Filled 但成交明细未齐，由定时器/REST 补查 |
| 租约（lease） | 控制线程静态分给各分片的资金/限速额度；热路径只查本分片租约 |
| 历史缺口 | Recorder 入队或写入失败造成的记录缺失；继续交易，但 `status/history` 显示不完整 |
