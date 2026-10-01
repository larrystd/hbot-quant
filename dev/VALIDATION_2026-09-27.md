# 2026-09-27 本地验收记录

环境：macOS arm64，本机 Bazel 9.2.0，C++20。代码目录尚无 Git 元数据；本记录不对应 CI commit。

| 检查 | 结果 |
| --- | --- |
| `bazelisk --batch build --spawn_strategy=local //...` | 通过，78 个 target 完成分析与构建 |
| `bazelisk --batch test --spawn_strategy=local --strategy=TestRunner=local //... --test_output=errors` | 35 个测试 target 全部通过；包括 `simulated_replay`、`simulated_binance`、`multi_shard`、`recovery` |
| 固定行情 Paper CLI | `start/status/history/stop` 成功；SQLite 记录订单、成交、费用和 run ID；同一 state 目录重启可查询旧历史 |
| 真实公开行情 Paper CLI | 使用 `data-stream.binance.vision:443` 和 `data-api.binance.vision:443`；BTCUSDT 盘口到 `Live`，挂出两侧 Paper 单；超过 15 秒刷新周期后旧两单取消、新两单挂出；历史与状态可查，`stop` 正常退出 |
| Linux x86_64、sanitizer 和 G3 多活跃分片装配/压力 | 尚未运行；目前 G3 测试只覆盖两个独立 RiskGate 与八路由组件，见 [LINUX_VALIDATION.md](LINUX_VALIDATION.md) |
| G4 隔离账户试单与重启对账 | 尚未运行；`mode=live` 仍拒绝启动 |

本机生产域名 `stream.binance.com` 的 WS 握手及 `api.binance.com` REST 均返回 HTTP 451，错误内容指出当前网络位置受限；公开数据专用域名分别返回 WS 101 和 REST 200。公开行情专用域名只用于行情和快照，Paper 订单始终留在本地。这里的联机记录只证明本次短跑，不代表长期网络稳定性或交易所账户验收。
