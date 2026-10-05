# hquant 当前架构

本页记录重设计前的代码基线。新仓库的核心设计见 [hquant 1.0 设计](DESIGN.md)。

本文描述 `hquant/src` 当前接入 `hquant_server` 的运行链路。设计以源码为准；下文“当前范围”列出尚未接入的组件。

## 总览

```text
┌────────────────────┐      ┌───────────────────────────────┐      ┌──────────────────────┐
│ 行情输入           │ ───▶ │ Shard Engine                  │ ───▶ │ SQLite / Control     │
│ 文件回放或 Binance │      │ 盘口、策略、风控、模拟交易、    │      │ 历史、状态、停止     │
│ 公开行情           │      │ 订单状态                      │      │                      │
└────────────────────┘      └───────────────────────────────┘      └──────────────────────┘
```

行情推动分片决策。分片把订单和动作记录送入历史队列；ControlServer 查询分片状态与历史，并接收停止命令。SQLite 写入和管理请求不参与每次策略决策。

入口是 [`apps/hquant_server.cc`](../apps/hquant_server.cc)：读取 YAML 配置，调用 [`Launch`](../hquant/src/application/launcher.cc)，由 [`QuantServer`](../hquant/src/application/quant_server.cc) 创建、启动并关闭组件。

## 行情输入

配置的 `market_data_source` 选择以下一种来源，两者最终都向 `Shard` 提交订单簿更新或公开成交：

| 来源 | 当前路径 |
| --- | --- |
| 文件回放 | [`ReadReplayFile`](../hquant/src/market/replay_feed.cc) 按 `InputTime` 顺序读取；`QuantServer::RunReplay` 推进 `ReplayClock`，再调用对应分片的 `Subscribe`、`OnSnapshot`、`OnDiff`、`OnPublicTrade` 或 `OnTimer`。回放在 `Start()` 中同步完成，之后才启动 ControlServer。 |
| Binance 公开行情 | 每个分片的 [`MarketDataStream`](../hquant/src/market/market_data_stream.cc) 接收 WebSocket 深度和公开成交，使用 REST 深度快照完成同步。`DepthParser` 将十进制文本精确转换为整数 ticks/lots；连接中断、序号缺口或无效盘口触发重新同步。 |

[`OrderBookSync`](../hquant/src/market/order_book.cc) 拥有分片的 L2 盘口。新连接先缓存增量，快照到达后按序追平；只接受连续更新，丢弃旧增量。盘口可能处于 `WaitingSnapshot`、`CatchingUp`、`Live`、`Stale`、`Resyncing` 等状态。只有 `Live` 盘口允许新单通过风控。盘口的近端价位放在整数数组中，远端价位放在有序映射中。

## Shard Engine

[`QuantServer::Create`](../hquant/src/application/quant_server.cc) 当前允许 1～8 个分片；每个分片恰好对应 **1 个市场、1 个策略、1 个账户**，同一账户不能分配给多个分片。每个 [`Shard`](../hquant/src/shard/shard.h) 独占自己的 `OrderBookSync`、策略实例、`RiskGate`、`OrderTracker` 和 `ActionExecutor`；`QuantServer` 持有该分片专用的模拟交易所。交易状态在所属分片内串行更新。

### 事件处理顺序

1. **盘口更新：** `Shard::AfterBookApply` 先让模拟交易所按最新买一卖一撮合已有挂单，并处理产生的账户回报；随后按策略的触发规则决定是否运行策略。
2. **公开成交：** `Shard::OnPublicTrade` 更新最近成交价，让模拟交易所尝试撮合，再检查策略触发规则。公开成交本身不是本账户的成交。
3. **定时器：** 检查盘口是否过期，再运行策略。实时行情的定时器由分片的 Asio 事件循环驱动；回放由文件中的定时器输入驱动。
4. **策略决策：** `Shard::RunStrategy` 组装盘口、活跃订单、模拟账户余额、规则、时间和就绪状态，调用 `Strategy::Decide` 得到有序动作批次。执行动作期间的回报先排队，步骤结束后按顺序处理；回报引起的新触发最多补跑一轮。

当前装配的唯一策略是 [`SimplePmm`](../hquant/src/strategy/simple_pmm.cc)。它在盘口顶部变化和定时器触发时检查刷新周期；首个就绪输入只完成初始化。刷新时先请求撤销旧单，再以中间价或最近成交价为参考生成买卖两侧报价。

### 下单与回报

[`ActionExecutor`](../hquant/src/shard/action_executor.cc) 按策略给出的顺序执行撤单和下单。下单先按交易规则向下量化价格与数量，再由 [`RiskGate`](../hquant/src/order/risk.cc) 检查盘口、账户新鲜度、规则时效、最小交易量和静态资金额度，并冻结最坏情况支出。通过后依次准备订单、登记 [`OrderTracker`](../hquant/src/order/order_tracker.cc)、启动提交，同时把订单和动作结果非阻塞地送入历史队列。

当前发送出口是 [`SimulatedTrading`](../hquant/src/shard/simulated_trading.cc)，它转发给 [`SimpleSimulatedExchange`](../hquant/src/order/simulated_exchange.cc)。模拟交易所根据相反方向的盘口触价或公开成交穿价撮合；匹配时按挂单自身限价**全额成交**，更新余额、手续费并产生账户回报。

回报返回 `Shard` 后，`OrderTracker` 根据客户端订单 ID 和成交 ID 去重、推进订单状态。结束状态先于成交明细到达时，订单进入 `AwaitingTrades`，待明细齐全再结束并释放资金冻结；冲突或结果不确定的状态保留待查询标记。`Shard` 对确认的成交更新风控占用，并把订单回报送入历史队列。

## SQLite 与 Control

[`SqliteOrderHistoryWriter`](../hquant/src/order_history/order_history_writer.cc) 为每个分片提供有界单生产者队列。分片分配本地递增序号后调用 `TryPush`，无需等待 SQLite 提交。Writer 线程攒批写入 WAL 数据库；队列满或写入失败会登记历史缺口。单独的 [`SqliteOrderHistoryReader`](../hquant/src/order_history/order_history_reader.cc) 线程用只读连接处理有界分页查询。

[`ControlServer`](../hquant/src/application/control_server.cc) 在独立线程上通过 `STATE_DIR/control.sock` 提供按行分帧的 JSON 请求：`status`、`history`、`stop`。实时模式的状态读取会投递到分片线程；历史查询由 Reader 完成。收到停止请求后，`QuantServer` 停止新的管理接入并汇合分片，随后结束 Writer 和 Reader，最后移除管理 socket。

## 线程与生命周期

| 工作 | 所属线程 |
| --- | --- |
| 文件回放及其分片事件 | 启动调用线程，按输入顺序执行；回放完成后开放管理接口 |
| Binance 公开行情、定时器、对应分片状态 | 每个分片一个 Asio `io_context` 线程 |
| 历史批量写入 | Writer 线程 |
| 历史查询 | Reader 线程 |
| Unix socket 管理请求及信号 | ControlServer 线程 |

`QuantServer` 拥有这些组件及其生命周期。实时公开行情使用阻塞式 Asio 事件循环；回放使用虚拟时钟，按文件输入顺序推进事件时间。

## 当前范围

- 配置只接受 `mode: simulated`。Binance 公开行情可以驱动模拟交易，但不会发送真实订单。
- [`LiveTrading`](../hquant/src/shard/live_trading.h)、[`BinanceOrderGateway`](../hquant/src/order/order_gateway.h)、[账户推送和查询](../hquant/src/order/account_reports.h)、[跨分片回报路由](../hquant/src/shard/routing.h)已有独立代码，尚未由 `QuantServer` 接入运行链路。
- 运行时只创建 `SimplePmm`；配置和静态额度按当前的一市场、一策略、一账户分片约束校验。
- 历史读取包含前次运行记录的读取能力，但当前启动链没有执行交易所对账和自动恢复。

查看具体行为时，从 [`QuantServer::Start`](../hquant/src/application/quant_server.cc)、[`Shard`](../hquant/src/shard/shard.cc)、[`ActionExecutor`](../hquant/src/shard/action_executor.cc) 三处顺序阅读即可。
