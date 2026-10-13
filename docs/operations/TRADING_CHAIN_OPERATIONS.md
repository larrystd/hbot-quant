# 外部模拟交易链路运行手册

适用范围：`account.mode: pressure_sim` 的本机压力器模拟交易所验收。`account.mode: online_sim` 专指 Binance Spot Demo 虚拟资金交易，须使用固定 Demo 域名；真实资金实盘没有对应模式。不要把 `pressure_sim` 的 loopback 端点改成外部地址。

## 构建和验收

在仓库根目录执行：

```sh
bazelisk --batch build //hquant/src:hquant_server
bazelisk --batch test //hquant/test:order_manager_test //hquant/test:trade_end_to_end
python3 hquant/bench/market/make_market_cases.py /private/tmp/hquant-market-cases/stress --case stress
python3 hquant/bench/trade_chain_bench.py /private/tmp/hquant-market-cases/stress --symbols 1000 --duration-seconds 30 --state-dir /private/tmp/hquant-trade-run
```

每次压测使用空的 `--state-dir`。结果在该目录的 `trade_chain_result.json`，独立交易所账本在 `venue_snapshot.json`；检查 `valid: true`、`trade_chain.errors: []`、`server_exit: 0`，以及账户、交易和日志队列的拒绝数均为零。压测逐项核对模拟交易所账本、Control 账户快照、`history.sqlite`、`order_journal.sqlite` 和 `bench_seq` 完成通知。

## 运行中查看与控制

运行日志由 Quill 异步输出到 stderr，默认级别为 info。排查单笔链路时可设置 `QUILL_LOG_LEVEL=tracel1`，日志会带上行情 `bench_seq`、订单 `client_order_id`、账户回报和查单事件；压测吞吐时保持默认级别。Quill 是诊断日志，订单恢复仍以 `order_journal.sqlite` 为准。意图入日志队列后立即进入交易发送队列，超时从实际 socket 写入开始计时。压力器模拟模式的提交队列过期时间为 30 秒，在线模拟仍为 5 秒；过期前未写 socket 的命令按本地未发送处理。若进程在 SQLite COMMIT 前崩溃，本地可能没有该订单 ID，当前只能用交易所历史人工核实已终结订单。

Control 使用状态目录中的 `control.sock`，访问权限为当前用户。下例中的请求均为一行 JSON，以换行结束：

```json
{"op":"status","request_id":1}
{"op":"account_controls","request_id":2,"account_id":"shared","limit":50,"cursor":0}
{"op":"pause_new_orders","request_id":3,"account_id":"shared","operator":"larry","reason":"检查账户差异"}
{"op":"sync_account_state","request_id":4,"account_id":"shared","operator":"larry","reason":"核对订单和余额"}
{"op":"resume_trading","request_id":5,"account_id":"shared","operator":"larry","reason":"完成核对"}
{"op":"cancel_all","request_id":6,"account_id":"shared","operator":"larry","reason":"停止未结订单"}
{"op":"stop","request_id":7}
```

`account_controls` 的 `next_cursor` 用于下一页；记录包含日志序号、动作码、`action_name`、操作者、原因和时间。`cancel_all` 的 `cancel_requests_enqueued` 只表示撤单请求进入发送链路；通过后续私有回报或主动对账确认最终状态。`pause_new_orders` 和 `cancel_all` 的人工暂停会在重启后恢复。`resume_trading` 会重新查询规则、订单、成交和余额，只有账户重新进入 Ready 才会发新单。

新状态目录的 `initial_balances` 应取交易所全部非零资产的当前总额；重启已有状态目录时保持首次启动时的原始基线，可靠日志会重放其后的成交。修改旧目录的基线会把成交计算两次并触发对账差异；本机重启用例已验证服务保持暂停且不发新单。第三资产手续费只允许扣已配置的资产；本机用例已验证 BNB 手续费和账户外借记导致的暂停。

## 故障处理与停机

私有流断开、请求结果未知、账本差异、日志写失败或规则不兼容时，先看 `status.accounts` 的 `ready`、`syncing`、`unknown_orders`、`error` 和队列健康。未知订单保持资金占用，按原 `client_order_id` 定时查单与成交；不要用新 ID 猜测重发。私有流断开后恢复或执行 `sync_account_state` 会做全账户同步。持续差异需要保留状态目录中的 `order_journal.sqlite`、`history.sqlite`、服务日志和模拟交易所账本进行调查。

正常停止先禁止新单，再尝试撤销未结订单，等待私有回报；到 30 秒期限仍未确认的订单会作为未决状态报告并留给下次启动对账。不可在未复制状态目录的情况下删除 `order_journal.sqlite`。日志写失败时服务退出码可能非零，这是验收应识别的失败信号。

## 扩大在线 Demo 验证前的剩余工作

两笔在线 Demo 虚拟资金成交的签名请求、私有流、规则、手续费、余额和限额已验证；新版本还核对了成交时间与可靠日志、独立查成交一致，并在同一状态目录只读重启。剩余异常路径、每条的期望行为和已有／待补证据见[Demo 验证文档的验收清单](BINANCE_DEMO_VALIDATION.md#尚未完成的验收)。当前 `online_sim` 仍限制一个账户、一个标的、单次最多一笔订单；未完成清单前不标记整个交易系统“全部验收通过”。
