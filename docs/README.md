# hquant 当前架构

当前入口是 [`hquant/src/main.cc`](../hquant/src/main.cc)，构建目标为 `//hquant/src:hquant_server`。运行示例见 [`hquant/README.md`](../hquant/README.md)，模块与线程边界见 [设计文档](DESIGN.md)，开发约束见 [开发文档](DEVELOPMENT.md)。

## 运行路径

`main` 读取 YAML 和 gflags 覆盖值，构造 [`Runtime`](../hquant/src/runtime.cc)。Runtime 启动 SQLite 历史、每个 Shard 的工作线程和 Unix socket Control。每个 Shard 固定一个市场、一个策略和一个模拟账户。

- 文件回放由 [`ReadReplayFile`](../hquant/src/replay_reader.cc) 按输入顺序读取，每条记录投递到目标 Shard，并等待该条记录及其账户事件处理完成。
- Binance 公开行情由 [`BinanceFeed`](../hquant/src/shard/binance_feed.cc) 通过 REST 快照和 WebSocket 深度、成交驱动 [`Market`](../hquant/src/shard/market.cc)。网络传输位于 [`base/net`](../hquant/src/base/net.h)。
- [`Shard`](../hquant/src/shard/shard.cc) 在线程内串行处理行情和定时器，调用 [`Strategy`](../hquant/src/shard/strategy.cc) 生成决定，再由 [`Executor`](../hquant/src/shard/executor.cc) 校验并派发命令。
- [`PaperExchange`](../hquant/src/shard/paper_exchange.cc) 独占活动订单和模拟余额。命令结果只表示本地派发；订单接受、拒绝、部分成交、完成、取消及余额更新作为独立 `AccountEvent` 投递回 Shard。
- [`SqliteHistory`](../hquant/src/sqlite_history.cc) 异步写入命令、订单、成交和余额记录；[`ControlServer`](../hquant/src/control_server.cc) 提供 `status`、`history`、`set_strategy` 和 `stop`。

价格、数量和金额用缩放 10^9 的 `uint64_t` 表示；盘口价位与数量用整数 ticks/lots。公开成交只用于模拟撮合，不能当作真实账户成交确认。当前运行路径只使用本地模拟账户，不发送真实订单。
