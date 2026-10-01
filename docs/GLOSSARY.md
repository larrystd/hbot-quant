# 术语表

本表以当前代码和配置使用的术语为准。

| 英文 | 中文 | 说明 |
| --- | --- | --- |
| exchange | 交易所 | 交易与行情的外部来源 |
| simulated exchange | 模拟交易所 | 本地撮合，`SimpleSimulatedExchange` |
| simulated trading | 模拟交易 | `mode: simulated` |
| live trading | 实盘交易 | `mode: live`，尚未开放 |
| replay | 回放 | 行情从文件按时间顺序重放 |
| market | 交易对 | 如 BTC-USDT |
| base / quote asset | 基础资产 / 计价资产 | BTC / USDT |
| order book | 订单簿 | |
| best bid / best ask（BBO） | 买一 / 卖一 | |
| tick / lot | 价格刻度 / 数量刻度 | 行情使用 `TickLotSize` 定义步长 |
| snapshot / diff | 快照 / 增量 | |
| connection_id | 连接编号 | 每次行情连接的序号；不同连接的深度序号不可直接比较 |
| trade | 成交 | 默认指自己订单的成交 |
| public trade | 公开成交 | 市场上其他订单的成交，属于行情 |
| submit / cancel | 下单 / 撤单 | |
| Open / PartiallyTraded / Traded | 挂单中 / 部分成交 / 全部成交 | 订单状态 |
| prepared order | 待发订单 | `PreparedOrder`：已分配客户端订单号，尚未发出 |
| order / trade / balance update | 订单更新 / 成交更新 / 余额更新 | 交易所发来的三种账户消息 |
| strategy_id | 策略 ID | 稳定的数字标识，编入客户端订单号 |
| risk budget | 风控额度 | `RiskBudget`：分片的资金上限和有效期 |
| funds hold | 资金冻结 | `FundsHold`：订单占用的最坏敞口 |
| shard | 分片 | 一个线程独占一组交易对的交易状态 |
| QuantServer | 交易核心 | 负责组装和运行行情、分片、策略、风控、成交、历史记录与管理入口 |
| ControlServer | 管理入口 | 通过 `<state_dir>/control.sock` 提供状态、历史和停止请求 |
| hquant_bench control | 管理入口压测 | 闭环或开环发出 `status/history` 请求，记录吞吐、延迟和错误码 |
| hquant_bench feed | 本地行情压测 | 在 HTTP/WS 端口模拟 Binance 公开行情，驱动 QuantServer 交易链 |
| reconciliation | 对账 | 向交易所查询，核对本地和真实状态 |
| submission unknown | 结果未知 | 请求已发出，但不知道交易所是否收到；按原 ID 对账 |
