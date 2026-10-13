# Python hummingbot 与 hquant 对照

hquant 没有翻译整个 Python hummingbot，只重写了其中一条链路：**Binance 公开行情 → 纯做市策略 → 模拟盘撮合 → 写历史**。本文先逐项对照两边的目录、交易所、策略、架构和能力，再用同一组参数、同一段行情，看两边分别怎么算、算出什么，差别出现在哪一步。

Python 代码位置以 `hummingbot/` 为根，C++ 代码位置以 `hquant/src/` 为根。

## 1. 目录对照

### 1.1 Python hummingbot 的目录

只统计 `hummingbot/` 包内的 `.py` 和 `.pyx`，共约 17 万行。仓库根目录的 `test/` 下另有约 20 万行测试，不计入。

| 目录 | 行数 | 内容 |
| --- | --- | --- |
| `connector/exchange/` | 55,282 | 26 个现货交易所 + `paper_trade` 模拟交易所，见 1.3 |
| `connector/derivative/` | 41,262 | 20 个永续合约交易所，见 1.3 |
| `connector/`（其余） | 约 13,900 | 所有交易所共用的基类和工具：`connector_base.pyx`、`exchange_base.pyx`、`exchange_py_base.py`、`client_order_tracker.py`、`budget_checker.py`、`trading_rule.pyx`、`time_synchronizer.py`、`markets_recorder.py`；`gateway/` 是 DEX 网关客户端（3,211 行）；`test_support/` 是测试辅助（4,260 行） |
| `strategy/` | 15,426 | V1 策略，见 1.4 |
| `strategy_v2/` | 12,804 | V2 框架：`controllers/` 1,525、`executors/` 8,268、`backtesting/` 2,299、`models/`、`utils/` |
| `core/` | 13,305 | 公共底座，下面逐项列出 |
| `client/` | 11,774 | 交互式应用：`command/` 4,799（20 个命令）、`config/` 3,199（配置向导和校验）、`ui/` 2,270（prompt_toolkit 界面） |
| `data_feed/` | 9,753 | 外部数据：`candles_feed/` 7,507（29 个 K 线源）、CoinGecko、CoinCap、强平数据、钱包追踪 |
| `cli/` | 3,033 | `hbot` 非交互命令行，可脚本化，输出 Markdown 或 JSON |
| `remote_iface/` | 1,436 | MQTT 远程控制 |
| `model/` | 1,143 | SQLAlchemy 表：`order.py`、`trade_fill.py`、`market_state.py`、`position.py`、`funding_payment.py` 等，加数据库迁移 |
| `logger/`、`notifier/`、`user/` | 约 560 | 日志、通知、用户余额 |

`core/` 内部：

| 子目录或文件 | 行数 | 内容 |
| --- | --- | --- |
| `clock.pyx`、`time_iterator.pyx`、`network_iterator.pyx` | — | 全局时钟；所有交易所和策略都是 `TimeIterator`，由时钟统一 tick |
| `pubsub.pyx`、`event/` | 476 | 事件发布订阅，事件类型定义 |
| `data_type/` | 3,961 | 盘口 `order_book.pyx`、盘口同步 `order_book_tracker.py`、用户数据流 `user_stream_tracker.py`、订单状态 `in_flight_order.py`、手续费 `trade_fee.py` |
| `gateway/` | 3,120 | Gateway 服务的 HTTP 客户端 |
| `rate_oracle/` | 1,607 | 汇率换算，17 个价格源 |
| `web_assistant/` | 813 | REST 和 WebSocket 的统一封装 |
| `api_throttler/` | 335 | 按交易所规则限制请求频率 |
| `trading_core.py` | — | 把时钟、交易所、策略、记录器组装起来 |

仓库根目录还有：`controllers/`（V2 controller 实现，见 1.4）、`scripts/`（18 个示例脚本）、`conf/`（配置模板）、`bin/`（启动入口 `hummingbot.py`、`hbot`）。

### 1.2 hquant 的目录

| 文件 | 行数 | 内容 | Python 里对应的位置 |
| --- | --- | --- | --- |
| `src/main.cc` | 392 | 进程入口，解析 gflags | `bin/hummingbot.py` |
| `src/config.{h,cc}` | 488 | YAML 配置解析和校验 | `client/config/` |
| `src/runtime.{h,cc}` | 331 | 创建线程和 Shard，路由请求，启动和停止 | `core/trading_core.py` |
| `src/replay_reader.{h,cc}` | 240 | 读回放文件 | 无（Python V2 回测走 `strategy_v2/backtesting/`，原理不同） |
| `src/sqlite_history.{h,cc}` | 650 | 后台线程写 SQLite，分页查询 | `connector/markets_recorder.py` + `model/` |
| `src/control_server.{h,cc}` | 399 | Unix socket JSON 控制 | `client/command/`、`remote_iface/mqtt.py` |
| `src/base/fixed.{h,cc}` | 114 | 10^9 定点数运算 | Python 直接用 `Decimal` |
| `src/base/net.{h,cc}` | 769 | REST 和 WebSocket 传输 | `core/web_assistant/` |
| `src/base/ids.h` | — | ID 和时间类型 | — |
| `src/shard/binance_feed.{h,cc}` | 595 | Binance 快照、增量、公开成交 | `connector/exchange/binance/binance_api_order_book_data_source.py` |
| `src/shard/order_book.{h,cc}` | 125 | 买卖档位 | `core/data_type/order_book.pyx` |
| `src/shard/market.{h,cc}` | 390 | 盘口同步状态机 | `core/data_type/order_book_tracker.py` |
| `src/shard/strategy.{h,cc}` | 136 | 做市决策 | `strategy/pure_market_making/` |
| `src/shard/executor.{h,cc}` | 175 | 下单前校验、资金上限 | `connector/budget_checker.py`、`trading_rule.pyx` |
| `src/shard/paper_exchange.{h,cc}` | 370 | 模拟交易所 | `connector/exchange/paper_trade/` |
| `src/shard/shard.{h,cc}` | 630 | 一个交易对的调度 | Python 没有对应物，由 `Clock` + 事件机制完成 |
| `test/` | 约 600 | 7 个端到端测试 | `test/`（约 20 万行，以单元测试为主） |
| `bench/` | 约 2,300 | 压测工具和行情生成器 | 无 |

`src/` 合计约 6,600 行。

### 1.3 交易所清单

**Python 现货**（`connector/exchange/`，26 个 + 模拟盘）：

| 交易所 | 行数 | 交易所 | 行数 | 交易所 | 行数 |
| --- | --- | --- | --- | --- | --- |
| `backpack` | 1,318 | `dexalot` | 1,644 | `kucoin` | 1,356 |
| `binance` | 1,289 | `foxbit` | 2,032 | `lambdaplex` | 1,290 |
| `bing_x` | 1,713 | `gate_io` | 1,201 | `lighter` | 1,889 |
| `bitget` | 1,595 | `gemini` | 2,350 | `mexc` | 2,173 |
| `bitmart` | 1,167 | `htx` | 1,106 | `ndax` | 1,724 |
| `bitrue` | 1,641 | `hyperliquid` | 2,393 | `okx` | 1,199 |
| `bitstamp` | 1,311 | `injective_v2` | 4,810 | `xrpl` | 9,377 |
| `btc_markets` | 1,420 | `kraken` | 1,580 | `paper_trade` | 1,197（模拟盘） |
| `bybit` | 1,841 | `coinbase_advanced_trade` | 2,664 | | |
| `derive` | 2,002 | | | | |

**Python 永续合约**（`connector/derivative/`，20 个）：

| 交易所 | 行数 | 交易所 | 行数 |
| --- | --- | --- | --- |
| `aevo_perpetual` | 1,641 | `gate_io_perpetual` | 1,528 |
| `architect_perpetual` | 1,263 | `grvt_perpetual` | 1,680 |
| `backpack_perpetual` | 1,665 | `hyperliquid_perpetual` | 2,840 |
| `binance_perpetual` | 1,828 | `injective_v2_perpetual` | 1,307 |
| `bitget_perpetual` | 2,201 | `kalshi_perpetual` | 1,433 |
| `bitmart_perpetual` | 1,604 | `kucoin_perpetual` | 2,168 |
| `bybit_perpetual` | 2,397 | `lighter_perpetual` | 2,253 |
| `decibel_perpetual` | 2,900 | `okx_perpetual` | 2,325 |
| `derive_perpetual` | 2,143 | `pacifica_perpetual` | 2,558 |
| `dydx_v4_perpetual` | 2,480 | `evedex_perpetual` | 2,951 |

**Python DEX 网关**：`connector/gateway/` 连接一个独立运行的 Gateway 服务，由 Gateway 去对接链上的 AMM 和 CLMM 池子。

**每个交易所目录里有什么**，以 `binance/` 为例，共 1,289 行：

| 文件 | 作用 |
| --- | --- |
| `binance_exchange.py` | 下单、撤单、查余额、订单状态更新，继承 `ExchangePyBase` |
| `binance_api_order_book_data_source.py` | 公开行情：盘口快照、增量、公开成交 |
| `binance_api_user_stream_data_source.py` | 私有推送：我方订单回报、成交、余额变化 |
| `binance_auth.py` | API 签名 |
| `binance_constants.py` | URL、限流规则、订单状态映射 |
| `binance_web_utils.py`、`binance_utils.py` | 请求构造、配置项 |
| `binance_order_book.py` | 把 Binance 消息转换成统一的盘口消息 |

永续合约目录多一个 `*_derivative.py`，负责持仓、杠杆、资金费率。

**hquant**：只有 Binance 现货的**公开行情**，在 `shard/binance_feed.cc`，对应上表中 `binance_api_order_book_data_source.py` 和 `binance_order_book.py` 两个文件。没有下单、私有推送和签名，交易由 `PaperExchange` 在本地模拟。

### 1.4 策略清单

**Python V1 策略**（`strategy/`，每个目录一种）：

| 策略 | 行数 | 做什么 |
| --- | --- | --- |
| `pure_market_making` | 2,251 | 单交易所买卖双边挂单。**hquant 的 `Strategy` 对应这个** |
| `avellaneda_market_making` | 1,863 | 按 Avellaneda-Stoikov 模型，根据库存和波动率算报价 |
| `perpetual_market_making` | 1,554 | 永续合约上做市 |
| `cross_exchange_market_making` | 2,325 | 在一个交易所挂单，成交后到另一个交易所对冲 |
| `cross_exchange_mining` | 847 | 跨交易所做市的流动性挖矿版本 |
| `liquidity_mining` | 859 | 多个交易对同时挂单赚流动性奖励 |
| `amm_arb` | 958 | CEX 与 AMM 之间套利 |
| `spot_perpetual_arbitrage` | 845 | 现货与永续之间套利 |
| `hedge` | 855 | 按持仓自动对冲 |

`strategy/` 根目录还有共用部件：`strategy_base.pyx`、`strategy_py_base.pyx`、`hanging_orders_tracker.py`、`order_tracker.pyx`、价格源 `order_book_asset_price_delegate.pyx`、`api_asset_price_delegate.pyx`。

**Python V2 框架**（`strategy_v2/` + 根目录 `controllers/`）把策略拆成两层：

- **controller 决定做什么**，在根目录 `controllers/` 下：
  - 做市：`pmm_simple`、`pmm_dynamic`、`dman_maker_v2`
  - 方向性交易：`bollinger_v1`、`bollinger_v2`、`bollingrid`、`dman_v3`、`macd_bb_v1`、`supertrend_v1`、`ai_livestream`
  - 通用：`arbitrage_controller`、`grid_strike`、`multi_grid_strike`、`hedge_asset`、`pmm_v1`、`pmm_mister`、`quantum_grid_allocator`、`stat_arb`、`xemm_multiple_levels`、`lp_rebalancer`
- **executor 负责怎么执行**，在 `strategy_v2/executors/` 下：`order_executor`、`position_executor`、`dca_executor`、`grid_executor`、`twap_executor`、`xemm_executor`、`arbitrage_executor`、`lp_executor`。由 `executor_orchestrator.py` 统一管理

`scripts/` 下另有 18 个可直接运行的脚本，例如 `simple_pmm.py`、`simple_xemm.py`、`simple_vwap.py`、`v2_funding_rate_arb.py`。

**hquant**：只有一个 `Strategy`，功能相当于 `pure_market_making` 只开一档、关闭所有附加功能。

## 2. 架构对照

### 2.1 Python：插件式平台

```text
bin/hummingbot.py
  └─ client/hummingbot_application.py      交互式应用，20 个命令
       └─ core/trading_core.py             组装下面所有部件
            ├─ Clock（core/clock.pyx）     每秒 tick 一次
            │    ├─ connector  × N         都是 TimeIterator
            │    └─ strategy               也是 TimeIterator
            ├─ MarketsRecorder             订阅交易所事件，写 SQLite
            └─ RateOracle / data_feed      汇率、K 线
```

一个交易所 connector 内部也分了层：

```text
ConnectorBase (connector_base.pyx)       余额、事件发布、订单跟踪接口
  └─ ExchangeBase (exchange_base.pyx)    下单接口 buy/sell/cancel、盘口查询
       └─ ExchangePyBase (exchange_py_base.py, 1,137 行)
            │  通用流程：下单 → 等回报 → 轮询对账 → 更新 InFlightOrder
            │  依赖：ClientOrderTracker、OrderBookTracker、UserStreamTracker、
            │        AsyncThrottler、TimeSynchronizer、WebAssistantsFactory
            └─ BinanceExchange (binance_exchange.py)   只填 Binance 特有的部分
```

关键设计：

1. **统一接口，任意组合**。策略只调用 `buy()`、`sell()`、`cancel()`、`get_price()`，不知道底下是哪个交易所。同一个 `pure_market_making` 可以跑在 Binance、OKX 或 Hyperliquid 上。
2. **新增交易所 = 继承 `ExchangePyBase`，填一组固定的方法**：URL、签名、下单请求格式、回报解析。公共流程不用重写。这就是每个交易所 1,200 到 2,500 行的原因。
3. **统一时钟驱动**。所有组件都注册到 `Clock`，每秒被 tick 一次。组件之间不直接调用，靠事件通信：交易所发布 `OrderFilledEvent`，策略和 `MarketsRecorder` 各自订阅。
4. **真实下单的复杂度主要在交易所这一层**。`InFlightOrder` 跟踪每张单的状态；`UserStreamTracker` 接私有推送；推送丢失时，定期用 REST 轮询对账；`AsyncThrottler` 防止超出交易所的请求频率限制；`TimeSynchronizer` 校准服务器时间，用于签名。

### 2.2 hquant：固定管线

```text
main.cc
  └─ Runtime                             线程池，最多 8 个 io_context
       ├─ Shard[0]  BTCUSDT  ─┐
       ├─ Shard[1]  ETHUSDT   ├─ 每个 Shard 内部结构完全相同
       ├─ ...                ─┘
       ├─ SqliteHistory                  写线程 + 读线程
       └─ ControlServer                  Unix socket

一个 Shard 内部：
  Market ──通知──▶ Shard ──① 撮合──▶ PaperExchange
  (BinanceFeed          ──② 决定──▶ Strategy
   + OrderBook)         ──③ 执行──▶ Executor ──▶ PaperExchange
                        ◀──账户事件（排队）── PaperExchange
```

关键设计：

1. **没有抽象层**。`Market`、`Strategy`、`Executor`、`PaperExchange` 都是具体类，Shard 按值持有，没有基类和虚函数。唯一的接口约束是编译期的 concept `DecisionStrategy`。
2. **按交易对横向切分**。Python 是一个循环处理所有交易对；hquant 每个交易对一个 Shard，状态互不共享，可以分到不同线程上。
3. **调用顺序写死**。行情来了，就是 撮合 → 决定 → 执行，在同一个调用栈里做完。没有时钟，也没有事件订阅。
4. **没有真实下单**，所以没有签名、私有推送、订单对账、限流。交易所这一层只剩 `binance_feed.cc` 的 595 行。

### 2.3 一张单从产生到入库，两边经过哪些模块

| 步骤 | Python（`pure_market_making` + `paper_trade`） | hquant |
| --- | --- | --- |
| 行情进入 | `BinanceAPIOrderBookDataSource` → asyncio 队列 → `OrderBookTracker._track_single_book` → `OrderBook.apply_diffs` | `BinanceFeed` → `Market::OnDiff` → `OrderBook::ApplyDiff` |
| 触发策略 | 等 `Clock` 下一次 tick → `PureMarketMakingStrategy.c_tick` | `Market` 立即调用 `Shard::OnMarket` |
| 算价格 | `c_create_base_proposal` + 5 个修正步骤 | `Strategy::Decide` |
| 下单 | `StrategyBase.buy_with_specific_market` → `PaperTradeExchange.c_buy` | `Executor::Dispatch` → `PaperExchange::Place` |
| 撮合 | `PaperTradeExchange.c_tick` 或公开成交事件 | 下一条行情到达时 `PaperExchange::OnMarket` |
| 成交通知 | `c_trigger_event(ORDER_FILLED_EVENT)` → 所有订阅者 | `EventSink` → `post` 到 Shard 线程 → `Shard::OnAccountEvent` |
| 入库 | `MarketsRecorder` → SQLAlchemy → `Order`、`TradeFill` 表 | `Shard::Record` → `SqliteHistory` 队列 → 后台线程写 `history_records` |

### 2.4 为什么 Python 有 17 万行

| 来源 | 约占 | hquant 是否有 |
| --- | --- | --- |
| 46 个交易所适配 + DEX 网关 | 11 万行（65%） | 只有 Binance 公开行情，595 行 |
| 9 种 V1 策略 + V2 框架 + 20 个 controller | 2.8 万行 | 1 个策略，136 行 |
| 交互式应用、非交互 CLI、MQTT | 1.6 万行 | 4 个 JSON 命令 |
| K 线、外部价格源（`data_feed/`） | 1 万行 | 无 |
| 底座：时钟、事件、盘口、限流、网络、汇率（`core/`） | 1.3 万行 | 有对应，但只实现用到的部分 |

Python 的代码量主要花在**通用性**上：适配多个交易所、支持多种策略组合、处理真实下单的各种异常、提供面向用户的操作界面。hquant 只保留了一条链路，把精力放在这条链路的正确性和性能上。

## 3. 能力对照

### 3.1 两边都有的能力

| 能力 | Python | hquant |
| --- | --- | --- |
| Binance 现货公开行情 | 快照 + 增量 + 公开成交，维护本地盘口 | 同左 |
| 纯做市 | 中间价 ± 价差，双边挂单，到期撤单重挂 | 同左，只有一档 |
| 交易规则 | 价格步长、数量步长、最小数量、最小金额 | 同左 |
| 下单前预算检查 | `BudgetChecker` | `Executor` 的资金上限检查 |
| 模拟盘 | `paper_trade`：本地余额、手续费、用行情推断成交 | `PaperExchange`：同左 |
| 历史落库 | SQLite | SQLite |
| 运行中操作 | 查状态、改参数、停止 | 查状态、查历史、改参数、停止 |

这些能力两边都有，但具体做法不同，第 5、6 节的场景里会看到差别。

### 3.2 Python 特有的能力

| 能力 | 说明 |
| --- | --- |
| 真实下单 | 26 个现货交易所、20 个永续合约交易所，外加 DEX 网关 |
| 真实下单配套 | API 签名、私有推送（订单回报、成交、余额）、推送丢失时的 REST 轮询对账、请求限流、服务器时间校准 |
| 永续合约 | 持仓、杠杆、资金费率 |
| 多种策略 | 9 种 V1 策略、V2 的 20 个 controller 和 8 种 executor、18 个示例脚本 |
| 回测 | `strategy_v2/backtesting/`，用 K 线数据模拟 V2 的 executor |
| 外部数据 | 29 个 K 线源、17 个汇率源、CoinGecko、CoinCap、自定义 API 价格 |
| 用户界面 | 交互式 CLI、配置向导、加密保存 API key、非交互 `hbot` 命令行、MQTT 远程控制 |

**pure_market_making 的参数**，Python 有 44 个，hquant 只有 4 个（`order_amount`、`bid_spread`、`ask_spread`、`refresh_ms`）。Python 多出的主要是：

| 参数 | 作用 |
| --- | --- |
| `order_levels`、`order_level_spread`、`order_level_amount` | 每边挂多档，每档价差和数量递增 |
| `order_refresh_tolerance_pct` | 新旧价格差在容忍范围内就不撤单 |
| `max_order_age` | 订单存活太久强制撤单重挂 |
| `minimum_spread` | 挂单离中间价太近就撤单 |
| `inventory_skew_enabled`、`inventory_target_base_pct` | 按库存偏离目标的程度，调整买卖两边的数量 |
| `hanging_orders_enabled` | 一边成交后，另一边的单保留不撤 |
| `filled_order_delay` | 成交后等一段时间再挂新单 |
| `ping_pong_enabled` | 买单成交后只挂卖单，反之亦然 |
| `price_ceiling`、`price_floor`、`moving_price_band_enabled` | 价格超出区间就只挂一边 |
| `price_source`、`price_type` | 参考价可以用最新成交价、最优买卖价、库存成本价、其他交易所或自定义 API |
| `order_optimization_enabled` | 把挂单价贴到盘口最优价前面一个 tick |
| `add_transaction_costs` | 把手续费计入报价 |
| `take_if_crossed` | 允许报价穿过对手价成为吃单 |
| `order_override` | 直接手写每张单的价差和数量 |

### 3.3 Python 做得更好的地方

| 方面 | 为什么 |
| --- | --- |
| 可扩展性 | 新增交易所只需继承 `ExchangePyBase`、填一组固定方法；新策略只依赖统一接口，可以跑在任意交易所上。hquant 新增交易所或策略需要改 Shard 的组装代码 |
| 策略功能 | 见上表。hquant 的策略只覆盖 pure_market_making 最基本的部分 |
| 少撤单 | 有价格容忍度，行情小幅波动时旧单保留，撤单和下单次数少。hquant 每次刷新都全撤全挂 |
| 真实交易的健壮性 | 推送丢了能靠 REST 轮询补回来，请求太快会自动限流。这些都经过多个交易所的实盘使用。hquant 还没有实盘 |
| 易用性 | 配置向导、交互命令、文档、社区，非程序员也能上手 |
| 测试覆盖 | 约 20 万行测试，以单元测试为主，覆盖每个交易所和策略 |

### 3.4 hquant 特有的能力

| 能力 | 说明 | 代码位置 |
| --- | --- | --- |
| 盘口序号检查 | 增量序号不连续、盘口交叉或单边为空，立即清空盘口重新同步 | `shard/market.cc` 的 `OnDiff` |
| 盘口状态机 | 明确的 `Syncing` / `Live` 两态，加上连接代次和过期检测。盘口不是 `Live` 时，策略自动撤掉所有挂单 | `shard/market.cc`、`shard/strategy.cc` |
| 部分成交 | 成交量受对手最优档数量或公开成交数量限制，订单可以分多次成交 | `shard/paper_exchange.cc` 的 `OnBookBbo`、`OnPublicTrade` |
| 下单即冻结资金 | 下单时冻结挂单金额、手续费和舍入余量，余额不够当场拒单 | `PaperExchange::BuyReserve`、`Place` |
| 成交事务化 | 一批成交先在副本上算完，并检查冻结金额不超过余额，全部通过才提交 | `PaperExchange::FillMatches` |
| 事件级回放 | 回放文件逐条记录快照、增量、公开成交和定时器，带微秒时间和序号。同一个文件每次跑出相同的历史；时间倒退的文件直接拒绝 | `replay_reader.cc`、`Shard::PostReplay` |
| 多交易对分片 | 一个进程最多 1000 个 Shard，每个 Shard 有自己的策略和账户，分到最多 8 个线程上并行处理 | `runtime.cc` |
| 历史序号 | 每个 Shard 的历史记录序号连续；下单命令和"订单被接受/被拒"分开记录；回放模式写队列满时等待，不丢记录 | `Shard::Record`、`sqlite_history.cc` |
| 配置版本号 | 改参数时必须带 `expected_version`，两个管理请求不会互相覆盖 | `Shard::PostStrategyUpdate` |
| 工程验证 | 黑盒端到端测试、ASan/UBSan/TSan 冒烟测试、1000 个交易对的压测 | `test/`、`dev/`、`bench/` |

### 3.5 hquant 做得更好的地方

| 方面 | 为什么 |
| --- | --- |
| 反应速度 | 行情到达后在同一个调用栈里完成撮合、决策、下单。Python 要等 `Clock` 的下一次 tick，默认 1 秒 |
| 盘口正确性 | 丢增量会被发现并重新同步。Python 不检查序号，丢了一条增量后，本地盘口可能一直是错的 |
| 模拟盘真实度 | 支持部分成交，成交量受市场实际数量限制，盘口穿价时按对手价成交。Python 只要价格穿过就整单成交，模拟结果偏乐观 |
| 资金一致性 | 下单时就冻结资金（含手续费），成交整批校验后提交。Python 下单时不检查余额，到成交时才发现钱不够 |
| 报价不吃亏 | 卖价向上取整，实际价差不会小于配置值。Python 两边都向下取整，卖价可能比配置的更靠近中间价 |
| 可复现 | 事件级回放，结果完全确定，适合做回归测试。Python 的 V2 回测基于 K 线，粒度粗，也不覆盖盘口撮合 |
| 数值 | 整数定点数，每一步都检查溢出，舍入方向明确。Python 金额用 `Decimal` 也是精确的，但 Cython 盘口里是 `double`，并且速度慢得多 |
| 多交易对扩展 | 按交易对分片到多个线程。Python 一个 asyncio 循环处理所有交易对，只能用单核 |
| 出错行为 | 撮合或决策出错就停掉这个 Shard，不带着错误状态继续交易。Python 多数异常只记日志，下一次 tick 继续运行 |

## 4. 同一组参数

用 `hquant/examples/replay.yaml` 的参数，Python 版填成等价的配置：

| 含义 | Python 配置 | hquant 配置 |
| --- | --- | --- |
| 交易对 | `BTC-USDT` | `BTCUSDT` |
| 价格步长 | 0.01 | `price_increment: "0.01"` |
| 数量步长 | 0.001 | `base_increment: "0.001"` |
| 每单数量 | `order_amount: 0.1` | `order_amount: "0.1"` |
| 买卖价差 0.1% | `bid_spread: 0.1`、`ask_spread: 0.1`（单位是 %） | `bid_spread: "0.001"`、`ask_spread: "0.001"`（单位是比例） |
| 刷新周期 | `order_refresh_time: 15`（秒） | `refresh_ms: 15000` |
| 初始余额 | BTC 0.2，USDT 2000 | BTC 0.2，USDT 2000 |

注意价差的单位不同：Python 填 `0.1` 表示 0.1%，`start.py` 里会除以 100；hquant 直接填比例 `0.001`。

同一个数字在两边的存法：

| 值 | Python | hquant |
| --- | --- | --- |
| 价格 99.9 | `Decimal("99.9")` | `uint64_t` 值 `99'900'000'000`（乘以 10^9） |
| 价差 0.1% | `Decimal("0.001")` | `bid_spread_ppb = 1'000'000` |
| 盘口一档 99.99 × 0.1 BTC | `OrderBookEntry(99.99, 0.1)`，内部是 `double` | `BookLevel{9999, 100}`（9999 个 tick，100 个 lot） |

## 5. 跟着一段行情走

### 第 0 步：开始挂单

盘口：

| | 价格 | 数量 |
| --- | --- | --- |
| 最优卖 | 100.01 | 0.1 BTC |
| 最优买 | 99.99 | 0.1 BTC |

两边算法一样：中间价 100.00，买价 = 100.00 × (1 − 0.001) = 99.90，卖价 = 100.00 × (1 + 0.001) = 100.10。

| | 买单 | 卖单 |
| --- | --- | --- |
| Python | 0.1 @ 99.90 | 0.1 @ 100.10 |
| hquant | 0.1 @ 99.90 | 0.1 @ 100.10 |

结果相同，但**触发时机**不同。

**Python**：`Clock` 每秒 tick 一次（`core/clock.pyx` 的 `run_til`），依次调用每个组件的 `c_tick`。策略在 `c_tick` 里检查"到没到下单时间、有没有活动挂单"，满足了才下单。盘口在两次 tick 之间变化，策略看不到，要等下一次 tick。

```python
# core/clock.pyx，简化
while True:
    await asyncio.sleep(next_tick_time - now)   # 睡到下一个整秒
    for child_iterator in self._current_context:
        child_iterator.c_tick(self._current_tick)  # 交易所、策略……依次 tick
```

**hquant**：没有统一的时钟。每来一条行情，`Market` 更新盘口后立即调用 `Shard::OnMarket`，在同一个调用栈里依次完成撮合、决策、下单（`shard/shard.cc`）。另外有一个定时器按 `refresh_ms` 触发刷新。

```cpp
// shard/shard.cc，简化
void Shard::OnMarket(const MarketNotice& notice) {
  exchange_.OnMarket(notice, market_.View());  // ① 用新行情撮合已有挂单
  RunStrategyDecision();                       // ② 策略决定 ③ Executor 下单/撤单
}
```

### 第 1 步：中间价不在价格步长上

换一个盘口：最优买 99.99，最优卖 100.02。中间价 100.005。

- 原始买价 = 100.005 × 0.999 = 99.904995
- 原始卖价 = 100.005 × 1.001 = 100.105005

两个价格都要对齐到 0.01：

| | 买价 | 卖价 | 实际卖出价差 |
| --- | --- | --- | --- |
| Python | 向下取整 → 99.90 | **向下取整 → 100.10** | 0.095%，比配置的 0.1% 小 |
| hquant | 向下取整 → 99.90 | **向上取整 → 100.11** | 0.105%，不小于配置值 |

Python 的 `c_quantize_order_price` 对买卖两边都是 `(price // quantum) * quantum`。hquant 在 `shard/strategy.cc` 里，买价用除法向下取整，卖价用 `RoundUp` 向上取整，保证报价离中间价至少有配置的价差。

### 第 2 步：盘口穿过我们的买单

回到第 0 步的挂单：买 0.1 @ 99.90，卖 0.1 @ 100.10。

来了一条增量，盘口变成：

| | 价格 | 数量 |
| --- | --- | --- |
| 最优卖 | **99.80** | 0.1 BTC |
| 最优买 | 99.70 | 0.1 BTC |

有人愿意以 99.80 卖出，低于我们的买价 99.90。如果我们的买单真的挂在交易所，它会成交。

| | 什么时候成交 | 成交价 | 成交量 |
| --- | --- | --- | --- |
| Python | 下一次 tick，最多晚 1 秒 | **99.90**（自己的挂单价） | 0.1，整单 |
| hquant | 这条增量处理时，立即 | **99.80**（对手最优价） | min(0.1, 最优卖档数量 0.1) = 0.1 |

**Python**（`connector/exchange/paper_trade/paper_trade_exchange.pyx` 的 `c_process_crossed_limit_orders`）：每次 tick 检查对手最优价有没有穿过挂单价，穿过就按挂单价整单成交。

**hquant**（`shard/paper_exchange.cc` 的 `OnBookBbo`）：

```cpp
if (order.side == Side::Buy && order.price_nanos >= *ask) {
  const uint64_t quantity = std::min(order.remaining_nanos, ask_available);
  matches.push_back(Match{order.id, quantity, *ask});   // 成交价是对手价 *ask
  ask_available -= quantity;
}
```

如果最优卖档只有 **0.04 BTC**：

| | 结果 |
| --- | --- |
| Python | 仍然整单成交 0.1 |
| hquant | 成交 0.04，订单变成部分成交，剩余 0.06 继续挂着 |

### 第 3 步：一笔公开成交扫到我们的卖单

卖单 0.1 @ 100.10 还挂着。市场上发生了一笔公开成交：**主动买入 0.01 BTC @ 100.15**。

成交价 100.15 高于我们的卖价，说明有人愿意出更高的价买入，我们的卖单会被吃到。

| | 成交价 | 成交量 | 订单状态 |
| --- | --- | --- | --- |
| Python | 100.10 | **0.1，整单** | 完全成交 |
| hquant | 100.10 | **min(0.1, 公开成交量 0.01) = 0.01** | 部分成交，剩余 0.09 |

**Python**（`c_match_trade_to_limit_orders`）：公开成交价穿过挂单价，就按挂单价整单成交，不看这笔成交本身有多大。

**hquant**（`OnPublicTrade`）：成交量不超过这笔公开成交的数量；成交的主动方必须和我们的挂单方向相反。

```cpp
if (order.side == trade.aggressor) continue;          // 主动买只能吃卖单
if (order.side == Side::Sell && order.price_nanos <= *price) {
  const uint64_t fill = std::min(order.remaining_nanos, available);
  matches.push_back(Match{order.id, fill, order.price_nanos});
  available -= fill;
}
```

边界情况：如果公开成交价**正好等于** 100.10，Python 不成交（要求严格穿过），hquant 成交（`<=`）。

### 第 4 步：15 秒到了，刷新

假设盘口又回到中间价 100.00 附近。

**Python**（`strategy/pure_market_making/pure_market_making.pyx` 的 `c_tick`）：

1. 生成新报价，再依次做档位、价格、数量、预算、防吃单修正
2. `c_cancel_active_orders`：如果设置了 `order_refresh_tolerance_pct`，新旧价格差在容忍范围内，**旧单保留不撤**
3. 撤单后，`c_to_create_orders` 要求已经没有活动挂单，才会下新单。撤单还没确认时，新单留到之后的 tick 再挂

**hquant**（`shard/shard.h` 的 `RunDecisionCycle`）：

1. `Decide` 发现到期、有活动挂单，返回 `CancelOrders`（剩余 0.09 的卖单）
2. Executor 撤单，资金立即解冻
3. 同一次调用里，用撤单后的账户视图再调用一次 `Decide`，返回 `SubmitOrders`，挂上新的一对单

hquant 每次刷新都撤掉全部挂单再重挂，没有价格容忍度。

### 第 5 步：丢了一条增量

增量的序号本该是 11、12、13，结果收到 11 之后直接收到了 13。

**Python**（`core/data_type/order_book_tracker.py` 的 `_track_single_book`）：直接应用 13，不检查序号是否连续。本地盘口从此可能和交易所不一致，直到收到下一份快照。

**hquant**（`shard/market.cc` 的 `OnDiff`）：

```cpp
if (input.first_sequence > *sequence_ + 1 || !book_.ApplyDiff(...)) {
  EnterSyncing("depth gap or invalid book", true);   // 清空盘口，重新同步
  return;
}
```

盘口状态从 `Live` 变成 `Syncing`，Shard 收到 `BookUnavailable` 通知。策略看到盘口不是 `Live`，撤掉全部挂单。重新拿到快照、接上增量后，才恢复挂单。

### 小结

| 步骤 | Python | hquant |
| --- | --- | --- |
| 0 挂单 | 99.90 / 100.10，每秒 tick 时检查 | 99.90 / 100.10，行情一到就检查 |
| 1 卖价取整 | 100.10（向下取整，价差变小） | 100.11（向上取整，价差不小于配置值） |
| 2 盘口穿过买单 | 0.1 @ 99.90 | 0.1 @ 99.80；档位不够时部分成交 |
| 3 公开成交 0.01 | 卖单整单成交 0.1 | 只成交 0.01，剩 0.09 |
| 4 刷新 | 价格在容忍范围内不撤；撤单确认后再挂 | 全撤，同一次调用里重挂 |
| 5 丢增量 | 继续应用 | 重新同步，期间撤掉挂单 |

同一段行情，hquant 的模拟成交量通常比 Python 少：成交量受对手盘口和公开成交量限制。

## 6. 结构上的区别

### 线程

| | Python | hquant |
| --- | --- | --- |
| 事件循环 | 一个 asyncio 循环，所有交易对共用 | 每个交易对一个 Shard，共用最多 8 个 `io_context` 线程 |
| 并发规则 | 单线程，天然串行 | 同一个 Shard 的事件串行；跨线程只能 `post` 到 Shard 所属线程 |

### 策略接口

**Python**：策略继承 `StrategyBase`，在 `c_tick` 里自己查盘口、查余额、下单撤单。成交通过 `did_fill_order` 之类的回调通知策略。

**hquant**：策略只做一件事：给定盘口和账户快照，返回一个决定。不直接下单，也收不到成交回调。

```cpp
using StrategyDecision = std::variant<NoAction, CancelOrders, SubmitOrders>;

absl::StatusOr<StrategyDecision> Decide(const BookView& book,
                                        const ExecutionView& execution,
                                        MonoTime now);
```

下单、撤单由 Executor 执行，`RunDecisionCycle` 负责"撤完再决定一次"。

### 资金冻结

以第 0 步的买单 0.1 @ 99.90、手续费 0.1% 为例：

| | 下单时 | 成交时 |
| --- | --- | --- |
| Python | 不检查余额。`on_hold_balances` 按 0.1 × 99.90 = 9.99 USDT 计算占用，不含手续费 | 余额不够就撤单 |
| hquant | 冻结 9.99 + 手续费 0.00999 + 舍入余量，余额不够当场拒单 | 先在副本上算完所有成交，全部校验通过才一次性提交 |

hquant 的下单冻结在 `PaperExchange::BuyReserve`，成交提交在 `FillMatches`。

### 出错时

**Python**：多数地方 `try/except` 之后记日志然后继续运行。比如 `Clock` 某次 tick 抛异常，只记一条日志，下一秒照常 tick。

**hquant**：函数返回 `absl::Status` / `StatusOr`。撮合或决策返回错误时，`Shard::StopFromError` 停掉这个 Shard，不带着错误状态继续交易。

### 历史和控制

| | Python | hquant |
| --- | --- | --- |
| 历史 | SQLAlchemy，`Order`、`TradeFill`、`MarketState` 等多张表，另写 CSV | 一张 `history_records` 表，`kind` 区分 9 种记录；后台线程批量写；每个 Shard 序号连续 |
| 改参数 | CLI 输入 `config bid_spread 0.2` | `set_strategy` 请求，带 `expected_version`，版本不符就拒绝 |
