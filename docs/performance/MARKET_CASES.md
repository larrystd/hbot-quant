# 股票行情回放数据：纽交所、上交所、压力

三组都用 **1000 只虚构股票**。每条盘口消息都是完整快照，包含买 10 档和卖 10 档，即 20 个 `[价格 tick, 数量 lot]`。逐笔成交是独立消息，包含股票、成交 ID、价格、数量和主动买卖方向，不携带盘口档位。NYSE、SSE 每笔公开成交 100 股；压力 case 中 900 只股票每笔 100 股，另外 100 只每笔 1 股。

| Case | 时间跨度 | 每只股票的盘口节奏 | 每只股票的成交节奏 | 完整十档盘口消息 | 逐笔成交 | 行情消息合计 | 含订阅的回放输入 | 盘口价量对 |
| --- | ---: | --- | --- | ---: | ---: | ---: | ---: | ---: |
| `nyse` | 3 分钟 | 每秒 1 条 | 每 10 秒 1 笔 | 180,000 | 18,000 | 198,000 | 199,000 | 3,600,000 |
| `sse` | 3 分钟 | 每 3 秒 1 条 | 每 30 秒 1 笔 | 60,000 | 6,000 | 66,000 | 67,000 | 1,200,000 |
| `stress` | 30 秒 | 每秒 8 条 | 每秒 2 笔 | 240,000 | 60,000 | 300,000 | 301,000 | 4,800,000 |

盘口总数包括每只股票的首条快照；每只股票另有一条 `subscribe`，所以回放输入比行情消息多 1000 条。没有用于衔接序号的空差分。每次盘口快照的买卖两侧各恰好 10 档，价格严格排序、数量为正；快照序号逐只股票连续。成交价取当时卖一（主动买）或买一（主动卖），成交 ID 逐只股票连续。

压力 case 中，Shard ID 为 0、10、…、990 的 100 只股票配置 2 股挂单量、`0.00005` 报价价差和 60 秒刷新；其余 900 只配置 1 股挂单量、`0.001` 价差和 15 秒刷新。公开逐笔成交共 60,000 条，只是行情输入负载。当前 `bench/venue/` 模拟交易所按独立脚本产生订单成交，并不按这些公开逐笔撮合，因此不再使用旧 Paper 路径的固定成交笔数作为交易链路验收值。

第一只被选中的股票首个盘口买一为 99.99 元、卖一为 100.01 元，后续公开逐笔成交在买一和卖一之间交替。交易链路压测核对模拟交易所实际收到的订单、独立账本、私有回报和本地记录；模拟成交不代表真实交易所排队成交。

生成器把 1000 只股票的消息错开排入各自的间隔：盘口消息使用 `slot × interval + shard × 100µs`；成交消息使用 `slot × interval + shard × interval / 1000`。`at_us` 是计划到达时间。当前性能测试由 `trade_chain_bench.py` 调用 `market/ws_sender.cc`，按计划时间经 WebSocket 发送全部公开行情消息，包括首条盘口；`subscribe` 只用于样本描述，不作为行情帧发送。

`nyse` 的每秒盘口频率参考 [NYSE OpenBook Aggregated](https://www.nyse.com/data-products/catalog/openbook-aggregated)；`sse` 的每 3 秒盘口频率参考 [上交所 Level-2 产品说明](https://www.sseinfo.com/services/assortment/level2/)。股票、成交频率、价格、数量及压力速率都是测试选择，不是交易所录制行情。这些回放也不包含网络接收、集合竞价或真实券商私有订单回报。

生成和校验：

```sh
python3 hquant/bench/market/make_market_cases.py /tmp/hquant-market-cases --case all
python3 hquant/bench/market/validate_market_cases.py /tmp/hquant-market-cases/nyse
python3 hquant/bench/market/validate_market_cases.py /tmp/hquant-market-cases/sse
python3 hquant/bench/market/validate_market_cases.py /tmp/hquant-market-cases/stress
```

每组输出 `replay.json`、`market_shards.yaml`、`manifest.json`。校验器核对全部 1000 只股票的订阅、每条消息的时间、每条快照的 20 档、序号、成交价格与数量、总数和最终买卖一。`trade_chain_bench.py` 在运行时从 `market_shards.yaml` 生成实际 `config.yaml`；目录分工见 [Bench 说明](../../hquant/bench/README.md)。
