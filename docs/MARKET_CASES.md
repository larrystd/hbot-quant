# 股票行情回放数据：纽交所、上交所、压力

三组都用 **1000 只虚构股票**。每条盘口消息都是完整快照，包含买 10 档和卖 10 档，即 20 个 `[价格 tick, 数量 lot]`。逐笔成交是独立消息，包含股票、成交 ID、价格、数量和主动买卖方向，不携带盘口档位。每笔成交 100 股。

| Case | 时间跨度 | 每只股票的盘口节奏 | 每只股票的成交节奏 | 完整十档盘口消息 | 逐笔成交 | 行情消息合计 | 含订阅的回放输入 | 盘口价量对 |
| --- | ---: | --- | --- | ---: | ---: | ---: | ---: | ---: |
| `nyse` | 3 分钟 | 每秒 1 条 | 每 10 秒 1 笔 | 180,000 | 18,000 | 198,000 | 199,000 | 3,600,000 |
| `sse` | 3 分钟 | 每 3 秒 1 条 | 每 30 秒 1 笔 | 60,000 | 6,000 | 66,000 | 67,000 | 1,200,000 |
| `stress` | 30 秒 | 每秒 8 条 | 每秒 2 笔 | 240,000 | 60,000 | 300,000 | 301,000 | 4,800,000 |

盘口总数包括每只股票的首条快照；每只股票另有一条 `subscribe`，所以回放输入比行情消息多 1000 条。没有用于衔接序号的空差分。每次盘口快照的买卖两侧各恰好 10 档，价格严格排序、数量为正；快照序号逐只股票连续。成交价取当时卖一（主动买）或买一（主动卖），成交 ID 逐只股票连续。

生成器把 1000 只股票的消息错开排入各自的间隔：盘口消息使用 `slot × interval + shard × 100µs`；成交消息使用 `slot × interval + shard × interval / 1000`。`at_us` 是计划到达时间。当前 `RunReplay` 等待每条输入处理完成后立即发送下一条，**不按时间戳限速**；表中的每秒条数是数据节奏，不是已经测得的实时吞吐。

`nyse` 的每秒盘口频率参考 [NYSE OpenBook Aggregated](https://www.nyse.com/data-products/catalog/openbook-aggregated)；`sse` 的每 3 秒盘口频率参考 [上交所 Level-2 产品说明](https://www.sseinfo.com/services/assortment/level2/)。股票、成交频率、价格、数量及压力速率都是测试选择，不是交易所录制行情。这些回放也不包含网络接收、集合竞价或真实券商私有订单回报。

生成和校验：

```sh
python3 hquant/bench/make_market_cases.py /tmp/hquant-market-cases --case all
python3 hquant/bench/validate_market_cases.py /tmp/hquant-market-cases/nyse
python3 hquant/bench/validate_market_cases.py /tmp/hquant-market-cases/sse
python3 hquant/bench/validate_market_cases.py /tmp/hquant-market-cases/stress
```

每组输出 `replay.json`、`config.yaml`、`manifest.json`。校验器核对全部 1000 只股票的订阅、每条消息的时间、每条快照的 20 档、序号、成交价格与数量、总数和最终买卖一。端到端执行还需检查 Shard 处理数、SQLite 历史记录和序号缺口。
