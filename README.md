# hquant

hquant 是一个 C++20 写的做市进程，用来验证交易链路的正确性和延迟。每个 Shard 负责一个交易对，处理顺序是：接行情 → 维护盘口 → 策略双边挂单 → 本地风控和资金预留 → 发单。账户层负责与交易所对账、写可靠日志、处理回报。

系统**不支持真实资金交易**，只有下面两种模拟模式。

## 支持什么

### 两种交易模式

| | 压力器模拟 `pressure_sim` | 在线模拟 `online_sim` |
| --- | --- | --- |
| 用途 | 本机大规模压测：正确性和延迟 | 对接真实 Binance 协议：Binance Spot Demo，使用虚拟资金 |
| 交易对端 | 本机 C++ 模拟交易所 `SimVenue`（`hquant/bench/venue`），有撮合引擎和独立账本 | Binance Spot Demo（`demo-api` / `demo-stream` / `demo-ws-api.binance.com`，TLS） |
| 行情 | 模拟交易所推送；也可以用文件回放（`input.source: replay`） | Demo 公开行情 |
| 规模 | 已测试 1000 个标的、8 个 Shard 线程、16 条 REST 连接 | 1 个账户、1 个标的，单次运行最多 1 笔订单 |
| 下单开关 | 默认就会下单 | 默认 `max_submissions_per_run: 0`，只读 |
| 怎么跑 | `trade_chain_bench` 一条命令完成：启动交易所、启动服务、验收 | 先手动预检，再起服务；也可以用 `demo_single_order_check.py` 做单笔验收 |

两种模式共用同一套代码：订单状态机、资金、风控、可靠日志、对账和 Binance 协议适配器（签名 REST、私有 WS、`exchangeInfo` 规则解析）都是同一份。`online_sim` 的三类连接都固定在 Demo 域名上，代码里没有能接生产环境的配置项。旧的模式名 `external_sim`、`demo`、`live` 已经不再支持。

### 两种行情输入

| `input.source` | 说明 |
| --- | --- |
| `binance_public` | 用一条 WebSocket 组合流同时收 `depth` 和 `trade`，再用 REST 拿 `/api/v3/depth` 快照对齐。压力器和 Demo 都用这个 |
| `replay` | 从文件逐条回放行情；读一条、处理完再读下一条，结果可复现（示例 `hquant/examples/replay.yaml`） |

### 策略

内置一个双边挂单策略：取最优买卖价的中间价，按 `bid_spread` / `ask_spread` 在两侧各挂一笔限价单，每 `refresh_ms` 撤掉旧单、重新报价。盘口不可用（重连、序号缺口、行情过期）时，撤掉全部挂单。策略参数可以在启动时用命令行覆盖，也可以在运行中通过控制面修改。

## 架构

### 数据流

```text
公开行情 WS（depth + trade 一条连接）／回放文件
  → Market            盘口状态机：Syncing（等快照对齐）／Live
  → Strategy          纯函数：NoAction／撤全部／挂一对买卖单
  → ShardTradingState 风控 + 从本 Shard 的独占额度里预留资金
  → TradingAccount::SubmitFromShard（在 Shard 线程上执行）
       ├→ 账户镜像任务入队、下单意图写入日志队列
       └→ REST lane（同一个 Shard 线程）→ 模拟交易所／Binance Demo

REST 结果／私有 WS 回报／查单结果
  → TradingAccount：先写 order_journal.sqlite 并 COMMIT，再更新状态
  → 通知订单所属的 Shard
```

### 线程

| 线程 | 数量 | 运行什么 |
| --- | --- | --- |
| Shard worker | `min(8, Shard 数)` | 行情协程、盘口、策略、本地准入，以及该 worker 名下的 REST 连接。多个 Shard 共用一个线程 |
| Account worker | `min(2, 账户数)` | 账户任务队列、对账副本（订单和资金）、全量对账、视图分发、REST 回调 |
| 私有流 | 每个账户 1 个 | Binance user data WebSocket |
| OrderJournal | 1 | SQLite 可靠日志（WAL，`synchronous=FULL`），凑批提交 |
| History / Control | 各 1 个 | 观察用历史库；Unix socket 控制面 |

### 设计规则

1. **单线程所有权**：Shard 的状态只在 Shard 线程上改，账户状态只在账户线程上改，跨线程一律用 `post` 投递任务。
2. **先日志，后状态**：交易所返回的事实（请求结果、回报、成交、余额、查单）先写进 `order_journal.sqlite` 并提交，再改内存状态。重启时从日志重建。
3. **热路径本地准入**：启动时把账户资金切分给各个 Shard。下单时 Shard 只查自己那一份，不等账户线程回复；账户线程随后异步维护与交易所对账的副本。

### 源码分层（`hquant/src`）

| 目录 | 职责 |
| --- | --- |
| `shard/` | `BinanceFeed`（行情连接）、`Market` + `OrderBook`（盘口）、`Strategy`、`ShardTradingState`（本地准入）、`Shard`（跨线程入口） |
| `trading/` | 与交易所无关：`OrderState`/`OrderManager`（订单状态与查单计划）、`RiskLimits`、`FundsState`、`OrderJournal`、`TradingAccount`、`AccountSync`（全量对账）、`AccountViewPublisher` |
| `venue/binance/` | `BinanceTradeClient`（6 个优先级队列 + N 条 REST lane）、`BinanceUserStream`（私有流）、签名、规则解析、全局限频 |
| `base/` | 定点数（1e-9 单位）、强类型 ID、协程式 HTTP/WS 客户端、Quill 日志 |
| 顶层 | `runtime`（组装各模块）、`config`、`control_server`、`sqlite_history`、`replay_reader`、`main.cc` |

### 关键行为

- **启动就绪**：进程启动后先从日志恢复状态，然后等私有流连上，再做一次全量对账，依次核对交易规则、挂单、每笔订单及其成交、余额。对账成功后才允许下单。之后每 60 秒对账一次。
- **行情重连**：每次重连都清空盘口，重新拿快照，再接上后续增量。断线期间的增量交易所不会补发，所以旧盘口不能再用。遇到序号缺口、盘口交叉、或 5 秒没有更新，也会触发重连。
- **结果未知**：请求已经写出去但没有收到响应时，系统不把它当成功，也不当失败。2 秒后开始查单，之后按 1→32 秒退避重查；私有流回报先到的话，以回报为准。结果确定之前，这个 Shard 不会再下新单。
- **暂停**：大多数异常（私有流断开、队列溢出、日志写失败、账本对不上）都会让账户暂停。暂停期间不下单，也不自动撤单；需要撤单只能用控制命令 `cancel_all`。对账成功后会自动恢复。如果是对账本身失败导致的暂停，就需要人工处理。

## 使用

### 构建和单元测试

```sh
bazelisk --batch build //hquant/src:hquant_server
bazelisk --batch test //hquant/test:all
```

测试覆盖订单状态、订单管理、资金、风控、日志、限频、规则解析、签名、视图分发、Shard 额度分配、交易客户端停机，以及端到端交易链路（`trade_chain_smoke`、`trade_end_to_end`）。模拟交易所自己的测试：

```sh
bazelisk --batch test //hquant/bench/venue:matching_engine_test //hquant/bench/venue:sim_venue_market_test
```

### 压力器模拟（`pressure_sim`）

一条命令跑完：启动模拟交易所 → 生成配置 → 启动 `hquant_server` → 压测 → 清场 → 验收。

```sh
bazelisk --batch build --config=release //hquant/src:hquant_server //hquant/bench:trade_chain_bench
bazel-bin/hquant/bench/trade_chain_bench \
  --symbols 1000 --seconds 30 --rest-connections 16 \
  --server bazel-bin/hquant/src/hquant_server \
  --state-dir /private/tmp/hquant-trade-run
```

| 参数 | 含义 |
| --- | --- |
| `--symbols` | 标的数量，每个标的对应一个 Shard |
| `--seconds` | 压测时长 |
| `--rest-connections` | 交易 REST 连接总数，平均分给各个 Shard worker |
| `--server` | 被测服务的二进制路径 |
| `--state-dir` | 状态目录，**每次运行都要用一个新的空目录** |

运行结束后先看 `valid` 是否为 `true`，再检查各项错误数、拒绝数，以及服务退出码是否为 0。状态目录里会留下 `history.sqlite`、`order_journal.sqlite`、`probe.json`、各段延迟 trace CSV 和服务日志。

### Binance Demo 在线模拟（`online_sim`）

**1. 准备凭据。** 在 Binance Spot Demo 上新建一个 API key，复制模板并限制权限：

```sh
cp hquant/examples/binance_demo_credentials.yaml.example hquant/local/binance_demo_credentials.yaml
chmod 600 hquant/local/binance_demo_credentials.yaml
# 填入 api_key / api_secret
```

`hquant/local/` 已被 git 忽略。凭据不要提交，也不要打印到输出里。

**2. 预检（只读，不下单）。**

```sh
python3 hquant/test/demo_preflight.py --market    # 公开接口：时间、规则、深度快照、公开 WS
python3 hquant/test/demo_preflight.py --private   # 签名查询账户和未结订单，订阅一次私有 WS
```

`--private` 会输出账户里所有非零资产。把它们的实际总额填进 `hquant/examples/binance_demo.yaml` 的 `initial_balances`，并确认账户上没有未结订单。

**3. 只读运行。**

```sh
bazel-bin/hquant/src/hquant_server \
  --config=hquant/examples/binance_demo.yaml \
  --state_dir=/private/tmp/hquant-demo-readonly
```

预期结果：私有流连上，对账完成，余额和交易规则都匹配；`ready=false`，`error="demo read-only validation"`，不会挂单。`hquant/test/demo_readonly_service.py --config … --binary … --state-dir …` 可以自动检查这些状态和两个 SQLite 库。

**4. 单笔订单验收。** 这个脚本会在一个新的状态目录里临时把 `max_submissions_per_run` 设为 1。它挂一笔单，观察几秒后用 `cancel_all` 撤单，再独立查单、查成交、查余额，与本地记录对账，最后删除临时配置：

```sh
python3 hquant/test/demo_single_order_check.py \
  --readonly-config hquant/examples/binance_demo.yaml \
  --binary bazel-bin/hquant/src/hquant_server \
  --state-dir /private/tmp/hquant-demo-order-$(date +%s)
```

如果要看这笔订单的分段延迟，运行 `hquant/test/demo_latency_report.py --state-dir … --client-id …`。

Demo 的示例配置额度是 20 虚拟 USDT，请求预算为每分钟 200 权重、每 10 秒 1 次下单。不要把 1000 标的的压测配置指向 Demo。

### 直接运行服务

```sh
bazel-bin/hquant/src/hquant_server --config=<yaml> --state_dir=<目录> \
  [--strategy_shard=0 --order_amount=0.0001 --bid_spread=0.001 --ask_spread=0.001 --refresh_ms=15000]
```

`--config` 和 `--state_dir` 是必填参数，方括号里是可选的策略覆盖参数。`hquant/examples/` 下有三份示例配置：`replay.yaml`（回放）、`binance_public.yaml`（压力器模式）、`binance_demo.yaml`（Demo）。

状态目录里的文件：

| 文件 | 内容 |
| --- | --- |
| `order_journal.sqlite` | 可靠日志，重启时靠它恢复。**不要删除** |
| `history.sqlite` | 观察用历史，记录下单、撤单、回报、成交、余额等事件 |
| `control.sock` | 控制面 socket |

### 运行中控制

往状态目录下的 `control.sock` 写入一行 JSON，服务返回一行 JSON：

```json
{"op":"status","request_id":1}
{"op":"pause_new_orders","request_id":2,"account_id":"demo","operator":"larry","reason":"检查账户"}
{"op":"sync_account_state","request_id":3,"account_id":"demo","operator":"larry","reason":"核对订单和余额"}
{"op":"resume_trading","request_id":4,"account_id":"demo","operator":"larry","reason":"核对完成"}
{"op":"cancel_all","request_id":5,"account_id":"demo","operator":"larry","reason":"清场"}
{"op":"stop","request_id":6}
```

各命令的说明：

- `status`：返回每个 Shard 和每个账户的状态。出问题时先看账户的 `ready`、`syncing`、`unknown_orders`、`error` 和队列健康。
- 账户操作（暂停、对账、恢复、全撤）必须带 `operator` 和 `reason`，执行前先写入可靠日志。
- `set_strategy` 在运行中修改策略参数，需要带当前版本号 `expected_version`。
- `history` 和 `account_controls` 分页查询历史记录和人工操作记录。

调诊断日志时设置 `QUILL_LOG_LEVEL=tracel1`，会打印每笔订单和每条回报的处理过程。压测时保持默认的 info 级别。

## 测试结果

测试环境：MacBook Air（Apple M2，24 GB 内存，macOS 26.1，arm64），Bazel 9.2.0，Apple clang 17.0.0，`--config=release`（`-O2 -DNDEBUG -g1`）。

### 压力器：正确性（2026-10-12）

规模：1 个账户、1000 个标的、8 个 Shard 线程、16 条 REST 连接，运行 30 秒。

负载：模拟交易所每 120 ms 给每个标的做一次深档挂撤或主动扫单，共 305,000 条计时行情（250,000 条深度、55,000 条成交）。策略首轮为每个标的挂买卖各一笔；15 秒后，900 个标的刷新报价。

| 验收项 | 结果 |
| --- | --- |
| 行情完整 | 305,000 条计时行情都有对应的处理确认，坏帧 0；公开 WS 总帧数 318,200 |
| 请求送达 | 下单 3800 笔、撤单 3600 笔；客户端和交易所两侧各记录 7400 条 REST 请求，逐笔对上，没有遗漏 |
| 撮合与回报 | 账户成交 400 次；私有回报 7800 条（3800 `NEW`、3600 `CANCELED`、200 `PARTIALLY_FILLED`、200 `FILLED`）；没有重复成交，也没有遗留回报 |
| 账户结清 | 1001 项资产的 `total/free/locked` 全部与模拟交易所一致；挂单、未知单、待写日志、超时、队列过期均为 0 |
| 落盘 | `history.sqlite` 42,003 条、`order_journal.sqlite` 41,605 条，序号都连续，两个库 `integrity_check=ok` |

结论：`valid=true`，全部通过。

### 压力器：下单延迟（2026-10-12）

样本：首轮每个标的先发出的那一笔订单，共 1000 笔。计时起点是模拟交易所发出触发行情，终点是交易所读完整个下单 HTTP 请求。本地是 loopback，不走 TLS。

| 阶段 | p50 | p99 |
| --- | ---: | ---: |
| 行情发出 → Shard 收到完整 WS 帧 | 19 µs | 67 µs |
| 完整帧 → 解析完成 | 4 µs | 28 µs |
| 解析完成 → 本地准入完成（策略、订单 ID、风控、预留） | 3 µs | 28 µs |
| REST 入队 → 开始写 HTTP | 5 µs | 52 µs |
| 开始写 → 交易所读完整请求 | 30 µs | 106 µs |
| **行情发出 → 交易所读完整下单请求** | **74 µs** | **212 µs** |

每个阶段的分位数是单独排序算出来的，各行不能相加；总耗时是先按订单逐笔相加，再取分位数。这一轮没有启用高优先级 QoS，也没有启用实时 I/O。

两项改造带来的变化：

| 改造 | 前 | 后 |
| --- | --- | --- |
| 下单准入从账户线程挪到 Shard 本地 | 等账户线程开始处理，就要 80 µs（p50）/ 1.282 ms（p99） | 本地风控加预留只要 1 / 13 µs |
| REST 连接从专用线程挪到 Shard 线程 | 入队 → 派发 33 / 133 µs | 1 / 15 µs |

全链路的 p50/p99 依次为：180 µs / 2.862 ms → 123 / 269 µs → 74 / 212 µs。

### Binance Demo：正确性（2026-10-12）

| 环节 | 结果 |
| --- | --- |
| 只读入场 | 盘口进入 Live，私有流可用，规则和余额全部匹配；下单 0 |
| 挂单后撤单 | `NEW → CANCELED`，成交 0；交易所查单为 `CANCELED`，余额回到入场值 |
| 挂单后成交 | `NEW → TRADE/FILLED`，买入 0.0001 BTC，价格 82739.22 USDT，手续费 0.0000001 BTC；成交只入账一次 |
| 独立对账 | 停机后用签名 GET 查订单、成交、未结订单和全账户余额，结果与本地 SQLite 逐项一致 |
| 同目录重启 | 重放日志后余额和订单都正确；没有新发单，也没有重复入账 |

五项都是 `valid=true`。

### Binance Demo：单笔延迟（2026-10-13）

BTCUSDT 一笔虚拟资金订单，结果为 `CANCELED`，成交 0，独立对账 `valid=true`。除行情那一行外，其余都是**单笔观测值**，不是分位数。

| 阶段 | 实测 |
| --- | --- |
| 公开行情：WS 读取 → 解析完成；解析 → Shard 处理完成（12 个样本） | p50 0.958 µs / 0.500 µs |
| 策略决策；生成订单 ID；本地风控与预留 | 1.375 µs；12 µs；6 µs |
| 准入 → REST 入队 → 派发 → 开始写 | 11 µs；7 µs；46 µs |
| REST 写请求 | 171 µs |
| 写完 → 收到 HTTP 响应（网络往返 + 交易所处理） | 211.827 ms |
| **策略产生订单 → HTTP 响应** | **212.080 ms** |
| 私有 `NEW`：帧完整 → 本地账户入账 | 692 µs |
| 撤单：写完 → HTTP 响应 | 213.535 ms |

本地处理（从行情到开始写 socket）在 100 µs 以内，绝大部分时间在网络往返和交易所处理上。Binance 不提供服务端时间戳，这一段没法再细分。

## 已知限制

- `online_sim` 目前只支持 1 个账户、1 个标的，单次运行最多 1 笔订单。丢 HTTP 响应、私有 WS 断线、限流、第三资产手续费这几条异常路径，只在本机验证过，还没在 Demo 上复现。
- 资金按 Shard 预先切分，一个 Shard 闲置的额度不会借给别的 Shard。
- 下单意图进入日志队列后就立即发送，不等它落盘。如果进程恰好在这段窗口期崩溃，重启后的对账会发现交易所上有一笔本地不认识的挂单，账户随即暂停，需要人工处理。
- 如果订单其实没到交易所（查单一直查不到），对应的 Shard 会一直等这笔单的结果、不再下新单。下一次周期对账查不到这笔单时，账户暂停，需要人工确认。
- 公开行情的重连退避（250 ms 起，最长 30 s）在整个进程生命周期内不会重置。
