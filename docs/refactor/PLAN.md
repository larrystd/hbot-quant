# 目录重构开发计划

状态：已执行（2026-10-01）。本次只调整目录、文件和构建结构，**不改变任何交易行为**。`docs/STRUCTURE_AND_TYPES.md` 第 2 节已改为引用本文的目标结构。

## 1. 为什么要改

现在 `hquant/` 下有 19 个目录、约 75 个非测试源文件，问题有三个：

- 目录按演进过程长出来，不按职责划分。例如 `connector/` 同时放抽象接口、通用订单状态机和具体交易所；`runtime/` 和 `app/` 从名字看不出区别；`app/engine.cc` 实际只负责进程启动。
- 小目录、小文件过多：`event/` 只有 1 个头文件，`model/fee.h` 只有 14 行，`connector/simulated_exchange/` 只有 3 个文件。
- 测试散落在各模块目录和根目录 `tests/` 两处。

## 2. 目标结构

规则：

1. `hquant/` 下只有 `src/` 和 `test/`。
2. 一个目录如果不超过 5 个文件，就不单独建目录。
3. 小文件按主题合并，单个文件只讲一件事。
4. 同一交易所在不同模块用后缀区分：`_feed`（行情）、`_gateway`（下单）、`_account`（账户回报）。

```text
hquant/
├── src/
│   ├── base/                          基础：与业务无关
│   │   ├── types.h / .cc              基础值类型：Decimal、各种 ID、时间与时钟
│   │   ├── market.h                   行情数据类型：市场、行情步长、盘口快照/增量、公开成交、交易规则
│   │   ├── order.h                    订单数据类型：下单请求、意图、回报、成交、手续费、余额、账户事件；下单网关接口
│   │   ├── net.h / .cc                网络：HTTP、WebSocket、TLS
│   │   └── rate_limit.h / .cc         限速：分片令牌桶、全局熔断
│   ├── market/                        行情
│   │   ├── order_book.h / .cc         订单簿：价位簿、同步状态机、只读视图
│   │   ├── replay_feed.h / .cc        回放行情：读固定行情文件
│   │   └── binance_spot_feed.h / .cc  Binance 行情：深度解析、WS 订阅、REST 快照、重连
│   ├── order/                         订单
│   │   ├── order_tracker.h / .cc      订单状态机：双 ID 核对、成交去重、结果未知
│   │   ├── risk.h / .cc               风控：准入检查、敞口预留、资金租约
│   │   ├── paper.h / .cc              模拟盘：虚拟余额、撮合、手续费
│   │   ├── binance_spot_gateway.h / .cc  Binance 下单：订单号编码、签名、异步下单撤单
│   │   └── binance_spot_account.h / .cc  Binance 账户回报：私有回报流、对账
│   ├── strategy/                      策略
│   │   ├── strategy.h                 策略接口、触发规则、动作列表、策略上下文
│   │   └── simple_pmm.h / .cc         简单做市
│   ├── service/                       交易服务：分片线程里怎么跑
│   │   ├── shard.h / .cc              分片主循环、分片命令与状态报告、回放时钟
│   │   ├── dispatcher.h / .cc         动作分发：按顺序检查并执行下单/撤单
│   │   └── routing.h / .cc            跨分片回报路由：订单归属索引、私有回报转发
│   ├── application/                   进程
│   │   ├── launcher.h / .cc           启动器：装配组件、创建线程、启动与关闭
│   │   ├── control.h / .cc            控制：消息定义、编解码、控制服务、status/history 输出
│   │   └── config.h / .cc             配置：YAML → AppConfig
│   ├── offline/                       离线
│   │   ├── storage.h                  存储接口：记录入队、历史查询、缺口与健康状态
│   │   ├── recorder.h / .cc           后台批量写 SQLite
│   │   ├── history.h / .cc            读历史：分页查询、重启恢复
│   │   ├── record_codec.h / .cc       记录序列化，读写共用
│   │   └── schema.sql                 建表脚本
│   └── cli/                           命令行：本次只换位置，内容不动
└── test/                              所有测试，平铺
    ├── <模块>_test.cc                 单元测试，见第 3.2 节
    ├── simulated_replay_test.cc、simulated_binance_test.cc、multi_shard_test.cc、recovery_test.cc   端到端
    ├── fixture_loader.h / .cc、fixture_schema_test.cc
    └── fixtures/                      Python 对照夹具（JSON）
```

依赖方向，由各 package 的 `visibility` 强制：

```text
base ◀── market ◀── strategy
base ◀── order
base ◀── offline
service     ──▶ market、order、strategy、offline
application ──▶ service 和各具体实现
apps        ──▶ application、cli
```

`strategy` 只能读 `market` 的只读盘口视图；`market` 与 `order` 互不依赖；只有 `application` 认识 Binance、Paper、SQLite 的具体类型。

**include 写法：** 每个 `cc_library` 设 `strip_include_prefix = "/hquant/src"`，代码写 `#include "base/types.h"`。

**Bazel：** `src/` 下每个模块一个 `BUILD.bazel`，合并后的每组文件仍是一个独立的 `cc_library`（如 `//hquant/src/order:risk`），可见性按 target 控制。`test/` 一个 `BUILD.bazel`，管全部测试与夹具。

## 3. 文件对应表

### 3.1 源文件

| 现在 | 目标 |
| --- | --- |
| `base/decimal.{h,cc}`、`base/ids.h`、`base/clock.h` | `src/base/types.{h,cc}` |
| `model/market.h`、`model/book_event.h`、`model/trading_rule.h` | `src/base/market.h` |
| `model/order.h`、`model/fee.h`、`model/balance.h`、`event/events.h`、`connector/api.h` | `src/base/order.h` |
| `net/api.h`、`net/http_client.*`、`net/websocket_client.*`、`net/tls_config.*` | `src/base/net.{h,cc}` |
| `net/rate_limit.*` | `src/base/rate_limit.{h,cc}` |
| `market_data/order_book.*`、`market_data/book_sync.*`、`market_data/book_view.h` | `src/market/order_book.{h,cc}` |
| `app/engine.cc` 中的 `ReplayFile`、`Levels` 等回放函数 | `src/market/replay_feed.{h,cc}` |
| `connector/binance_spot/public/depth_parser.*`、`market_data_stream.*` | `src/market/binance_spot_feed.{h,cc}` |
| `connector/order_tracker.*` | `src/order/order_tracker.{h,cc}` |
| `risk/risk_gate.*`、`risk/lease.*` | `src/order/risk.{h,cc}` |
| `connector/simulated_exchange/paper_connector.*` | `src/order/paper.{h,cc}` |
| `connector/binance_spot/order_gateway/{client_id_codec,signer,order_gateway}.*` | `src/order/binance_spot_gateway.{h,cc}` |
| `connector/binance_spot/private/{user_data_stream,reconciliation}.*`、`json_fields.h` | `src/order/binance_spot_account.{h,cc}` |
| `strategy/api.h` | `src/strategy/strategy.h` |
| `strategy/simple_pmm.*` | `src/strategy/simple_pmm.{h,cc}` |
| `runtime/shard.*`、`runtime/shard_command.h`、`runtime/replay_clock.h` | `src/service/shard.{h,cc}` |
| `runtime/dispatcher.*` | `src/service/dispatcher.{h,cc}` |
| `routing/order_ownership_index.*`、`routing/private_report_router.*` | `src/service/routing.{h,cc}` |
| `app/engine.*`（去掉回放与 JSON 输出后） | `src/application/launcher.{h,cc}` |
| `control/protocol.h`、`control/wire_codec.*`、`app/control_server.*`、`engine.cc` 中的 `StatusJson`/`HistoryJson` | `src/application/control.{h,cc}` |
| `app/config/config.*` | `src/application/config.{h,cc}` |
| `storage/ports.h` | `src/offline/storage.h` |
| `storage/sqlite_recorder.*` | `src/offline/recorder.{h,cc}` |
| `storage/history_reader.*`、`storage/recovery.*` | `src/offline/history.{h,cc}` |
| `storage/record_codec.*` | `src/offline/record_codec.{h,cc}` |
| `storage/migrations/001_init.sql` | `src/offline/schema.sql` |
| `cli/*` | `src/cli/*`（内容不变） |

### 3.2 测试文件

基线：35 个测试 target，65 个 gtest 用例（`TEST`/`TEST_F`/`TEST_P`）。合并后测试 target 变少，**用例数必须仍为 65**。

| 现在 | 目标 |
| --- | --- |
| `base/decimal_test.cc`、`model/domain_test.cc` | `test/types_test.cc` |
| `net/http_client_test.cc`、`net/websocket_client_test.cc` | `test/net_test.cc` |
| `net/rate_limit_test.cc` | `test/rate_limit_test.cc` |
| `market_data/order_book_test.cc`、`market_data/book_sync_test.cc` | `test/order_book_test.cc` |
| `binance_spot/public/{depth_parser,market_data_stream,f1_adapter}_test.cc` | `test/binance_spot_feed_test.cc` |
| `connector/order_tracker_test.cc` | `test/order_tracker_test.cc` |
| `risk/risk_gate_test.cc`、`risk/multi_shard_lease_test.cc` | `test/risk_test.cc` |
| `connector/simulated_exchange/simulated_exchange_test.cc` | `test/simulated_exchange_test.cc` |
| `binance_spot/order_gateway/order_gateway_test.cc` | `test/binance_spot_gateway_test.cc` |
| `binance_spot/private/private_test.cc` | `test/binance_spot_account_test.cc` |
| `strategy/simple_pmm_test.cc` | `test/simple_pmm_test.cc` |
| `runtime/replay_clock_test.cc` | `test/shard_test.cc` |
| `runtime/dispatcher_test.cc` | `test/dispatcher_test.cc` |
| `routing/routing_test.cc` | `test/routing_test.cc` |
| `control/protocol_test.cc`、`app/control_server_test.cc` | `test/control_test.cc` |
| `app/config/config_test.cc` | `test/config_test.cc` |
| `storage/storage_test.cc` | `test/recorder_test.cc` |
| `storage/recovery_test.cc` | `test/history_test.cc` |
| `cli/cli_test.cc` | `test/cli_test.cc` |
| `tests/integration/*_test.cc` | `test/*_test.cc`（文件名不变） |
| `tests/fixtures/{fixture_loader.*,fixture_schema_test.cc}` | `test/` |
| `tests/fixtures/{order_book,order_tracker,simple_pmm,paper,v1}/`、`DIFFERENCES.md` | `test/fixtures/` |

## 4. 执行阶段

每个阶段结束都要满足第 5 节的检查项，再进入下一阶段。

### 阶段 0：基线

1. `git init`，加 `.gitignore`（排除 `bazel-*`、`compile_commands.json`、`.cache/`），提交当前全部文件作为基线。
2. 记录基线：`bazel build //...` 的 target 数、`bazel test //...` 结果（35 个测试 target 全过）、gtest 用例数 65。
3. 新建分支 `refactor/layout`。

### 阶段 1：搭骨架

1. 建 `hquant/src/{base,market,order,strategy,service,application,offline,cli}/` 和 `hquant/test/`。
2. 在根 `BUILD.bazel` 或 `tools/` 加一个小宏 `hquant_cc_library`，统一设置 `strip_include_prefix`、项目告警选项和默认 `visibility`，避免每个 BUILD 重复写。
3. 骨架阶段旧目录照常编译；新旧两套同时存在，直到某个模块迁移完成后删除旧目录。

### 阶段 2：自底向上迁移模块

顺序：`base` → `market` → `order` → `strategy` → `offline` → `service` → `application` → `cli`。下游依赖上游，顺序不能颠倒。

每个模块按同一套步骤：

1. 按第 3.1 节合并文件：头文件按依赖顺序拼接，去掉重复的 `#include` 和 `namespace` 包装；`.cc` 中的匿名命名空间辅助函数如有重名（例如 `Statement`、`Reset`、`PlainStream`、`ExchangeKey`），合并时改为不冲突的名字。
2. 写该模块的 `BUILD.bazel`，按第 2 节设 `visibility`。
3. 用脚本按旧路径 → 新路径对照表，替换全仓库的 `#include` 和 Bazel label（包括 `apps/`、`dev/`、`tests/` 中的引用）。
4. 按第 3.2 节移动并合并对应测试到 `hquant/test/`，在 `hquant/test/BUILD.bazel` 中登记。
5. 删除旧目录，跑第 5 节检查，提交一次 commit（例如 `refactor: move base`）。

### 阶段 3：拆分启动代码

把 `app/engine.cc`（657 行）按职责拆开，属于代码改动但不改行为：

1. `ReplayFile`、`Levels` 等回放函数移到 `src/market/replay_feed.{h,cc}`，对外给出按时间产出快照与增量的接口。
2. `StatusJson`、`HistoryJson`、`BookStateName`、`OrderStatusName` 等输出函数移到 `src/application/control.cc`。
3. 剩余部分成为 `src/application/launcher.{h,cc}`：`RunPaperEngine` 与 `RunSimulatedBinanceEngine` 合并为 `Launch(const AppConfig&, const std::string& state_dir)`，内部按配置的模式和行情来源分支；`EngineConfig` 改名 `AppConfig`。
4. `apps/hquant_engine.cc` 只调用 `Launch`。

### 阶段 4：工具与文档

1. 更新路径引用：
   - `op.sh` 的 `SOURCE_ROOTS`（`hquant apps tests dev` → `hquant apps dev`）；
   - `tools/compdb.py` 的 `SOURCE_ROOTS`；
   - 根 `BUILD.bazel` 的注释；
   - `.github/workflows/linux-bazel.yml` 中若有具体路径一并修改。
2. 更新 `dev/contract_compile_test.cc` 的 include 列表，改为新的公共头文件。
3. 更新夹具 JSON 和 README 中 `source_files` 里的 `hquant/...` 与 `tests/fixtures/...` 路径。
4. 重新生成 `compile_commands.json`（`./op.sh compdb`）。
5. 文档：
   - `docs/STRUCTURE_AND_TYPES.md` 第 2 节换成第 2 节的目标结构，第 2.1 节依赖图同步；
   - `docs/DEVELOPMENT.md` 的包职责表与目录所有权；
   - `docs/ARCHITECTURE.md`、`docs/README.md` 中出现的旧包名；
   - 代码地图页面重新生成。

## 5. 每阶段的检查项

| 检查 | 通过标准 |
| --- | --- |
| 构建 | `bazel build //...` 无错误、无新增告警 |
| 测试 | `bazel test //...` 全部通过 |
| 用例数 | `grep -hcE '^(TEST\|TEST_F\|TEST_P)\(' hquant/test/*.cc` 加上尚未迁移的旧测试，合计等于 65 |
| 旧路径残留 | 旧路径（`hquant/model/`、`hquant/connector/`、`hquant/runtime/` 等）在代码、BUILD、脚本、夹具中搜索不到（`docs/refactor/` 本文除外） |
| 依赖方向 | 用 `bazel query 'deps(//hquant/src/strategy/...)'` 等确认没有违反第 2 节的依赖 |
| 行为 | `bazel run //apps:hquant -- start --config examples/simulated_replay.yaml ...` 跑通 `start/status/history/stop`，输出与基线一致 |
| Sanitizer | 最后一个阶段结束后跑 `bazel test --config=asan //...`（macOS） |

## 6. 风险与应对

| 风险 | 应对 |
| --- | --- |
| 合并 `.cc` 后匿名命名空间里的辅助函数或类型重名 | 合并时逐个检查，按所属功能加前缀；编译器会报重定义 |
| 合并头文件引入循环 include | 按第 2 节依赖方向排列内容；`base/order.h` 只依赖 `base/types.h` 与 `base/market.h` |
| 合并测试时漏掉用例 | 每阶段核对用例总数 65 |
| `visibility` 收紧后已有调用方编译失败 | 说明存在违反分层的依赖，记录下来；本次先放宽到原有范围，另开任务修正 |
| 大范围替换 include 时误改 | 替换脚本只匹配完整的旧路径字符串，每个模块单独提交，便于用 `git diff` 检查和回滚 |
| Bazel 缓存残留旧 target | 迁移完成后 `bazel clean` 再全量构建一次 |

## 7. 不在本次范围

以下另开任务，不混入目录重构：

- `market` 增加 K 线（Bar、BarSeries、成交聚合、历史加载）与 Binance K 线流。
- 策略接口拆为高频 `BookStrategy`（盘口驱动）与中低频 `BarStrategy`（K 线驱动），新增 CTA 示例策略与指标。
- `risk` 中跨分片资金租约的"授予"部分移到 `service`。
- CLI 结构调整，以及可执行文件 `hquant-engine` 是否改名。

## 8. 执行记录（2026-10-01）

- 仓库执行前已经有 Git 初始提交，因此没有重复 `git init`；创建了 `refactor/layout` 分支。执行前工作区已有未提交的文档与夹具改动，迁移时保留了这些内容。为避免将原有改动混入自动提交，本次没有按模块创建 commit。
- `hquant/` 现只有 `src/` 和 `test/`。测试统一为 25 个 `hquant/test` target；加上 `dev` 的 3 个测试 target，全仓库为 28 个，保留了 65 个 gtest 用例。30 份夹具 JSON 从旧目录逐字节迁移。
- `bazel clean` 后重新执行 `bazel build //...` 和 `bazel test //...`，均通过；`bazel test --config=asan //...` 的 28 个测试 target 也全部通过。CLI 回放模式的 `start/status/history/stop` 已逐项运行成功。
- 重新生成了 `compile_commands.json`，并通过 `./op.sh fmt-check`、旧路径扫描和 Bazel 模块直接依赖检查。
- 目标结构中的 `strategy`、`cli` 虽少于 5 个文件，仍保留独立目录，因为它们是明确的 Bazel 模块边界。迁移过程中保留旧目录直至新 target 可构建；“旧路径零残留”在最终阶段检查。
