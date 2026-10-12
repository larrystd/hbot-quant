# hquant

C++20 交易链路正确性与性能验证项目。行情驱动 `Market → Strategy → TradingAccount`；账户统一跟踪订单、资金和回报，并把可靠事件写入 `order_journal.sqlite`，把查询历史写入 `history.sqlite`。没有进程内 Paper 撮合，也没有真实资金实盘模式。

## 两种交易模式

| `account.mode` | 含义 | 行情与交易对端 | 资金 |
| --- | --- | --- | --- |
| `pressure_sim` | 压力器模拟 | 行情回放／本机行情服务；本机模拟交易所提供 REST 和私有 WS | 模拟交易所独立账本 |
| `online_sim` | 在线模拟 | Binance Spot Demo 的公开行情、交易 REST 和私有 WS | Binance Demo 虚拟资金 |

两种模式使用同一套订单状态、资金状态、日志和 Binance 协议适配器。`online_sim` 的行情与订单簿接近实盘，但不是实盘订单簿；代码把市场、交易和私有连接都锁定在 Demo 域名。旧配置值 `external_sim`、`demo`、`live` 不再支持。

```text
行情 WS／回放 → Market → Strategy → TradingAccount
                                  ├→ OrderManager + FundsState
                                  ├→ OrderJournal / SqliteHistory
                                  └→ BinanceTradeClient → 压力器或 Binance Demo

压力器或 Binance Demo → 私有 WS／REST 结果／查单结果 → TradingAccount → Shard
```

## 构建与本机测试

```sh
bazelisk --batch build //hquant/src:hquant_server
bazelisk --batch test //hquant/test:all
```

1000 标的、30 秒正确性压力 case 只连接本机模拟交易所：

```sh
python3 hquant/bench/market/make_market_cases.py /private/tmp/hquant-market-cases/stress --case stress
python3 hquant/bench/trade_chain_bench.py /private/tmp/hquant-market-cases/stress \
  --symbols 1000 --duration-seconds 30 --state-dir /private/tmp/hquant-trade-run
```

每次运行使用空的 `--state-dir`。结果在 `trade_chain_result.json`，模拟交易所独立账本在 `venue_snapshot.json`。具体核对项和控制命令见[运行手册](docs/TRADING_CHAIN_OPERATIONS.md)。
Bench 目录分工见[说明](hquant/bench/README.md)。

## Binance 在线模拟

[示例配置](hquant/examples/binance_demo.yaml)使用 `mode: online_sim`，默认 `max_submissions_per_run: 0`，即只读。程序固定 Demo 的三个 TLS 域名，只允许一个账户、一个标的（示例为 BTCUSDT），单次最多一笔订单；示例另设 20 虚拟 USDT 的额度。Demo 凭据放在被忽略的 `hquant/local/binance_demo_credentials.yaml`，不要提交或输出。

验证步骤、已测试 API、虚拟资金成交和余额对账记录见[Binance Demo 验证文档](docs/BINANCE_DEMO_VALIDATION.md)。设计和修改前后链路见[V2 设计文档](docs/TRADING_CHAIN_DESIGN_V2.md)（[HTML](docs/TRADING_CHAIN_DESIGN_V2.html)）。`docs/DESIGN.md` 和 `docs/TRADING_CHAIN_DESIGN.md` 记录旧版 Paper／实盘预留方案，不代表当前配置或运行方式。
