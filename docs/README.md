# hquant 文档索引

文档按主题存放。历史设计与测试报告保留原时间口径；判断当前工作区实现时，先看代码和 [2026-10-12 发单链路性能优化记录](performance/TRADING_PATH_OPTIMIZATION_2026-10-12.md)（[HTML](performance/TRADING_PATH_OPTIMIZATION_2026-10-12.html)）。

## 架构与设计 · `architecture/`

| 文档 | 内容 |
| --- | --- |
| [交易链路设计 V2](architecture/TRADING_CHAIN_DESIGN_V2.md) · [HTML](architecture/TRADING_CHAIN_DESIGN_V2.html) | 账户级订单跟踪、资金、日志、恢复的设计；写于后续 Shard 本地发单改造之前。 |
| [交易系统设计提案](architecture/TRADING_CHAIN_DESIGN.md) · [HTML](architecture/TRADING_CHAIN_DESIGN.html) | 较早的完整交易系统方案，属历史设计。 |
| [hquant 1.0 设计](architecture/DESIGN.md) · [HTML](architecture/DESIGN.html) | 旧 Paper 架构；不代表当前运行路径。 |
| [Python hummingbot 与 hquant 对照](architecture/PYTHON_VS_CPP.md) · [HTML](architecture/PYTHON_VS_CPP.html) | 旧版本目录、能力和策略计算对照。 |
| [成熟交易系统差距审查](architecture/TRADING_SYSTEM_GAP_REVIEW.md) | 架构差距与历史问题。 |

V2 HTML 的本地生成器为 [`architecture/render_trading_chain_v2.py`](architecture/render_trading_chain_v2.py)。

## 开发与实施记录 · `development/`

| 文档 | 内容 |
| --- | --- |
| [hquant 1.0 开发](development/DEVELOPMENT.md) | 旧版本开发记录。 |
| [交易链路开发](development/TRADING_CHAIN_DEVELOPMENT.md) · [HTML](development/TRADING_CHAIN_DEVELOPMENT.html) | 账户级交易链路的分阶段开发。 |
| [首轮交易链路实施](development/TRADING_CHAIN_IMPLEMENTATION.md) | 第一轮实现与本机测试。 |
| [后续开发与验收](development/TRADING_CHAIN_FOLLOWUP.md) | 故障、恢复、对账和验证记录。 |

## 运行与外部验证 · `operations/`

| 文档 | 内容 |
| --- | --- |
| [外部模拟交易链路运行手册](operations/TRADING_CHAIN_OPERATIONS.md) | 配置、启动、控制与故障操作。 |
| [Binance Spot Demo 验证记录](operations/BINANCE_DEMO_VALIDATION.md) | Demo API 状态、实际执行记录与待验证项。 |

## 性能、压测与优化 · `performance/`

| 文档 | 内容 |
| --- | --- |
| [发单链路性能问题与优化记录](performance/TRADING_PATH_OPTIMIZATION_2026-10-12.md) · [HTML](performance/TRADING_PATH_OPTIMIZATION_2026-10-12.html) | 当前架构、分层延迟、已实施改动与复现方法。 |
| [账户视图与 REST 队列改造计划](performance/TRADE_PATH_POOL_AND_VIEW_PLAN.md) | 早期排队问题、连接池实验和两轮改动。 |
| [当前问题与定位证据（2026-10-06）](performance/CURRENT_ISSUES_2026-10-06.md) · [HTML](performance/CURRENT_ISSUES_2026-10-06.html) | 历史 WS/内核可读事件诊断。 |
| [旧测试报告（2026-10-06）](performance/TEST_REPORT_2026-10-06.html) | Paper 架构行情压测；其 34 µs ACK 指标不是发单成交延迟。 |
| [端到端正确性与性能摸底](performance/PERFORMANCE_2026-10-06.md) | 旧文件回放、SQLite 与分段耗时。 |
| [行情回放场景](performance/MARKET_CASES.md) · [结果](performance/MARKET_CASE_RESULTS_2026-10-06.md) | 三组 1000 标的行情数据及验证。 |

原始 JSON/TXT 结果集中在 [`performance/data/`](performance/data/)：`TEST_REPORT_DATA`、`STRESS_FILL_RUN`、`PHASE_TRACE`、`KEVENT_TRACE`、`KEVENT_BURSTS`、`SELECTED_TEST_RESULTS`、`market_case_results`、`perf_results` 和两份 `TRANSPORT_CONTROL` 记录。报告中的相对链接已指向这些原始文件。

