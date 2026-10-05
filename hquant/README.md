# hquant

新入口采用每个 Shard 一个 `io_context` 工作线程。Market 同步通知 Shard，Shard 依次调用 PaperExchange 撮合、Strategy 决策和 Executor 派发命令。PaperExchange 独占模拟订单与余额；命令结果只表示本地派发，订单接受、拒绝、部分成交、完成、取消以及余额变化走独立的账户事件路径。回放逐条等待目标 Shard 完成，历史异步写入 SQLite，Control 通过 Unix socket 提供查询和管理操作。

在仓库根目录构建并运行示例回放：

```sh
bazelisk build //hquant/src:hquant_server
bazel-bin/hquant/src/hquant_server \
  --config=hquant/examples/replay.yaml \
  --state_dir=/tmp/hquant-replay
```

回放完成后会停止 Shard、刷新 `history.sqlite` 并移除 `control.sock`。实时公开行情可把配置换为 `hquant/examples/binance_public.yaml`，用 `SIGINT`、`SIGTERM` 或 Control 的 `stop` 结束。

启动时可用 `--strategy_shard=0` 配合 `--order_amount`、`--bid_spread`、`--ask_spread`、`--refresh_ms` 覆盖对应 YAML 策略值。金额、数量和比例按 10^9 缩放为 `uint64_t`；盘口价位和数量保持整数 ticks/lots，所有计算检查范围。

运行时 Control 在 `STATE_DIR/control.sock` 接受一行一个 JSON 请求：

```json
{"op":"status","request_id":1}
{"op":"history","request_id":2,"limit":100,"cursor":0}
{"op":"set_strategy","request_id":3,"shard_id":0,"expected_version":1,"patch":{"bid_spread":"0.002","refresh_ms":12000}}
{"op":"stop","request_id":4}
```

`set_strategy` 的 patch 还可包含 `order_amount` 和 `ask_spread`。十进制值须为字符串；成功后配置版本加一，Shard 撤旧单并按新参数决策。`history` 中 `dispatched` 表示命令通过本地校验并送到模拟交易所；订单是否接受及成交要看随后的订单和成交记录。`remaining_nanos` 记录部分成交或取消时的剩余量。数额字段以字符串返回，避免 JSON 客户端丢失 `uint64_t` 精度。

模拟撮合根据公开盘口和成交量估算部分成交；真实订单只能由交易所私有账户回报确认。因此模拟与未来实盘保持相同的命令/账户事件语义，成交结果仍是模型假设。

示例回放中的一笔卖单在历史里依次出现：`submit_command`（已派发）、`order_accepted`、`fill`（成交 0.01 BTC）、`order_partially_filled`（剩余 0.09 BTC），最后停止时出现 `cancel_command` 和 `order_cancelled`。命令记录与订单状态记录不能互相替代。

Binance REST/WebSocket 通用传输位于 `//hquant/src/base:net`，新入口的构建图只引用 `hquant/src` 内模块。
