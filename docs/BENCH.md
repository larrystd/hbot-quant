# 本地交易链路压测

`hquant_server` 运行交易链，`hquant_bench` 提供一次性管理命令、管理入口负载测试和本地行情源。先构建两个程序：

```sh
bazelisk build //apps:hquant_server //apps:hquant_bench
```

## 管理命令

```sh
bazel-bin/apps/hquant_server examples/simulated_replay.yaml /tmp/hquant-demo
# 在另一个终端：
bazel-bin/apps/hquant_bench status --state-dir /tmp/hquant-demo
bazel-bin/apps/hquant_bench history --state-dir /tmp/hquant-demo --limit 20
bazel-bin/apps/hquant_bench stop --state-dir /tmp/hquant-demo
```

管理协议仍是 `<state_dir>/control.sock` 上的一行 JSON 请求和一行 JSON 响应。参数错误退出码为 2，连接或服务错误为 1。

## 压管理入口：control

先启动回放 server，然后运行：

```sh
bazel-bin/apps/hquant_bench control --state-dir /tmp/hquant-demo \
  --mode closed --connections 64 --threads 4 --duration 10 \
  --warmup 2 --mix status=80,history=20

bazel-bin/apps/hquant_bench control --state-dir /tmp/hquant-demo \
  --mode open --rate 1000 --connections 100 --threads 4 \
  --duration 10 --json
```

| 参数 | 默认 | 含义 |
| --- | --- | --- |
| `--state-dir DIR` | 必填 | server 状态目录 |
| `--mode closed\|open` | `closed` | 闭环连接收到响应后再发下一条；开环按固定全局速率发请求 |
| `--connections N` | 16 | 闭环客户端数量；开环最大在途请求数 |
| `--threads T` | 1 | 压测线程数，每线程独立 `io_context` |
| `--duration S` | 10 | 计入统计的秒数 |
| `--warmup S` | 0 | 预热秒数，不计入统计 |
| `--mix status=N,history=M` | `80,20` | 请求比例，两个整数百分比之和为 100 |
| `--rate R` | 无 | 开环每秒计划发出的请求数；闭环不可设置 |
| `--json` | 关闭 | 输出单行 JSON，便于脚本收集 |

输出分别列出 `status` 和 `history` 的 `sent`、`success`、`rps`（成功数除以统计秒数），以及延迟 `p50_us`、`p90_us`、`p99_us`、`p999_us`、`max_us`。分位数来自每线程固定桶直方图，显示桶的上界；最大值是实际观测值。`errors` 按错误码名汇总，如 `CONTROL_BUSY`、`HISTORY_QUEUE_FULL`、`NET_TIMEOUT`。`client_saturated` 是开环计划发送时已达到本地在途上限的次数，不属于 server 的 `CONTROL_BUSY`。管理入口默认最多 16 个并发会话；100 个客户端是否遇到 `CONTROL_BUSY` 取决于请求处理时间。

## 压交易链：feed

`feed` 在一个 TCP 端口上同时提供 Binance 格式的 REST 深度快照和 WS 深度增量、公开成交。示例配置 [`examples/bench_feed.yaml`](../examples/bench_feed.yaml) 将两条连接指向 `127.0.0.1:18080`；先启动 feed，再启动 server：

```sh
# 终端 1：运行 20 秒，并在结束时读取 server status
bazel-bin/apps/hquant_bench feed --listen 127.0.0.1:18080 \
  --fixture examples/replay_market.json --rate 10 --speed 1 \
  --duration 20 --state-dir /tmp/hquant-feed --json

# 终端 2：在 feed 已监听后启动
bazel-bin/apps/hquant_server examples/bench_feed.yaml /tmp/hquant-feed

# feed 结束后停止 server
bazel-bin/apps/hquant_bench stop --state-dir /tmp/hquant-feed
```

| 参数 | 默认 | 含义 |
| --- | --- | --- |
| `--listen IP:PORT` | 必填 | 本地 HTTP/WS 监听地址；须与 server 配置相同 |
| `--fixture FILE` | `examples/replay_market.json` | 回放输入，须含快照和紧随其后的首条增量 |
| `--symbol SYM` | `BTCUSDT` | Binance 原生交易对；须与 server 市场一致 |
| `--price-per-tick` / `--amount-per-lot` | `0.01` / `0.001` | 将回放的整数 ticks/lots 转成十进制文本；须与 server 配置一致 |
| `--rate R` | 10 | 每秒计划推送的 WS 消息数；空闲时发送连续序号的空增量保鲜 |
| `--speed X` | 1 | 回放事件时间倍速；本地启动预留 2 秒，让 server 的策略定时器就绪 |
| `--duration S` | 20 | feed 运行秒数，到时关闭监听并报告统计 |
| `--gap-every N` | 0 | 每 N 条深度增量故意跳过一个序号，触发重新同步 |
| `--disconnect-every S` | 0 | 每个 WS 会话连接 S 秒后断开，触发重连 |
| `--http-429-rate P` | 0 | 快照请求返回 HTTP 429 的比例，范围 0–1 |
| `--state-dir DIR` | 无 | 结束时自动查询 server status 并包含在报告中 |
| `--json` | 关闭 | 输出单行 JSON |

feed 报告含 `depth_sent`、`trades_sent`（生成的推送消息数）、`ws_connections`、`disconnects`、`snapshots`、`http_429`。提供 `--state-dir` 时再附 `server_status`：`book`、`balances`、`fees_paid`、`applied_diffs`、`resyncs`、`strategy_invocations`、`recorder_dropped` 等。压测端的发送数与 server 的应用数可能不同：初始缓存增量、断线、跳号和未完成的网络写都会造成差异。

用上述夹具以 `--speed 1 --duration 20` 跑通时，server 最终余额应为 BTC `0.01999`、USDT `10.000999`。该夹具的价格变化在策略 15 秒刷新后推送，以保持与离线回放的订单和费用一致。故障注入时关注 `resyncs` 增长、随后 `book` 返回 `Live` 且 `applied_diffs` 继续增加。

未设置 `exchange_endpoints` 时，server 仍连接默认的 Binance 公开 REST/WS 地址并使用 TLS；本地 feed 不接收私有下单请求。
