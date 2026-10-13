# Binance Spot Demo 小流量正确性验证

目标：先验证真实 Demo 接口、私有回报与账户状态，再考虑订单性能。`account.mode: online_sim` 在本项目中专指 Binance Spot Demo 在线虚拟资金交易；没有真实资金实盘模式。配置只允许一个账户、一个 shard（当前验证用 BTCUSDT），并锁定 Binance Demo 的三个 TLS 域名。

Binance 官方要求使用同一套 Spot API 文档并替换为 Demo 域名，但区别不只在地址：Demo 使用单独的 API 凭据、虚拟资金和自己的订单簿；官方称价格与订单簿“接近”实盘，明确不等于实盘数据。因此 `online_sim` 能验证在线协议和账户链路，不能把 Demo 成交或策略表现当作真实资金交易结果。[Binance Spot Demo 官方说明](https://github.com/binance/binance-spot-api-docs/blob/master/demo-mode/general-info.md)

## 配置文件

- [服务配置](../../hquant/examples/binance_demo.yaml)：默认 `max_submissions_per_run: 0`，只读。交易请求限为每分钟 200 权重、每 10 秒 1 次挂单；程序拒绝更高值。公开行情使用 Demo WS，深度快照只取一个标的、100 档且整次运行最多请求三次。
- [密钥模板](../../hquant/examples/binance_demo_credentials.yaml.example)：凭据放在被 `.gitignore` 排除的 `hquant/local/binance_demo_credentials.yaml`。程序要求普通文件且只有文件所有者有访问权限；不会将密钥写到日志。当前测试文件已经填好，权限为 `0600`。测试后宜轮换此前在聊天中出现过的密钥。
- 新状态目录的 `initial_balances` 必须填写该 Demo 账户**所有非零资产**的当前总额（`free + locked`），本次只读账户还包含 USDC。服务启动时逐项对账；不一致会保持暂停。重启已有状态目录时必须保留它首次启动时的原始余额基线：可靠日志会重放其后的成交，再次把当前余额写作基线会重复计算。`hard_limits` 是策略可用额度，不等于账户总余额。
- `demo_preflight.py` 的 `configured_balances_match` 只适合**新状态目录的入场检查**。已有成交的状态目录应保留原始基线，用独立账户查询和服务内恢复对账确认当前余额；不要为了让预检显示匹配而修改旧目录的基线。

```sh
mkdir -p hquant/local
cp hquant/examples/binance_demo_credentials.yaml.example hquant/local/binance_demo_credentials.yaml
chmod 600 hquant/local/binance_demo_credentials.yaml
# 在本机编辑上述文件，填写 Demo API key/secret；不要提交或发送到聊天。
```

## 验证顺序

1. 运行 `python3 hquant/test/demo_preflight.py --market`。只调用一次 `exchangeInfo?symbol=BTCUSDT`、一次 `/api/v3/time`、一次 `depth?symbol=BTCUSDT&limit=100`，并建立一条短暂的公开行情 WS，检查 `depthUpdate` 和 `trade`。不读密钥、不下单、不重试。
2. 填好密钥后运行 `python3 hquant/test/demo_preflight.py --private`。额外查询一次账户、一次全账户未结订单，并订阅一次私有 WS；无下单或撤单。脚本输出配置资产总额、未配置的非零资产及其金额、配置是否匹配、未结订单数。HTTP 429/418 直接停止。
3. 将服务配置的 `initial_balances` 改成输出的实际总额，包含所有非零资产；确认账户没有未结订单。构建并运行 `bazelisk --batch build //hquant/src:hquant_server`，使用全新的状态目录启动：

   ```sh
   bazel-bin/hquant/src/hquant_server \
     --config=hquant/examples/binance_demo.yaml \
     --state_dir=/private/tmp/hquant-demo-readonly
   ```

   先观察 `status.accounts`：私有连接成功、同步结束、余额和交易规则匹配。只读模式下 `ready=false`、`error="demo read-only validation"` 是预期门禁；不会发挂单。一个市场连接和一个私有连接足够；不要用 1000 标的压力配置访问 Demo。
   可用 [demo_readonly_service.py](../../hquant/test/demo_readonly_service.py) 在新状态目录自动核对只读状态与两份 SQLite。
4. 读写验证前先在 `0` 的配置下再次运行私有预检；通过后才将 `max_submissions_per_run` 改为 `1`，使用**另一个空状态目录**启动。第一笔限价单保持未成交：核对挂单响应、`executionReport: NEW`、`GET /api/v3/order`，再通过 `cancel_all` 触发该订单的 `DELETE /api/v3/order`，核对撤单响应、`executionReport: CANCELED`、未结订单、余额和 SQLite。若订单自行成交，按实际终态核对，不能把未发生的撤单标为通过。完成后恢复 `max_submissions_per_run: 0`。
   [demo_single_order_check.py](../../hquant/test/demo_single_order_check.py) 可从只读配置在新状态目录临时生成一笔上限配置，清场后独立查单、查成交、查余额，并删除临时配置。
5. 成交路径单独验收：使用新的状态目录和最多一笔小额订单，等待真实 Demo 撮合后核对 `executionReport: TRADE`、`GET /api/v3/myTrades` 非空、资金变化和 SQLite。当前策略只在中间价两侧挂单，不能保证自然成交；2026-10-11 的 V2 测试已取得一笔真实 Demo 成交样本。

`--market --private` 同时使用时，预检最多五次 REST 请求、一条公开 WS 和一次私有 WS 订阅；服务账户同步每轮依次查询单标的规则、全账户未结订单和余额，每 60 秒才周期重查。行情快照请求另设整次运行最多三次的门禁。预检与服务分别计预算，连续运行时至少间隔一分钟。任何 429/418、规则差异、余额差异、连接失败或未知订单都停止下单并人工核对，不自动扩大请求量。

## 当前代码使用的 Binance 接口

| 接口 | 用途 | Demo 实测状态 |
| --- | --- | --- |
| `GET /api/v3/time` | 预检本机时钟；服务本身不调用 | 2026-10-13 预检通过，时钟偏差 157 ms |
| `GET /api/v3/exchangeInfo?symbol=BTCUSDT` | 读取交易规则 | 2026-10-13 预检通过 |
| `GET /api/v3/depth?symbol=BTCUSDT&limit=100` | 建立本地订单簿快照 | 2026-10-13 服务内盘口同步通过 |
| `wss://demo-stream.binance.com/stream?streams=btcusdt@depth/btcusdt@trade` | 接收深度增量和逐笔交易 | 2026-10-13 服务内处理公开 WS 124 帧；2026-10-12 公开成交已单独验证 |
| `GET /api/v3/account` | 同步资金 | 2026-10-13 撤单后独立查询与本地余额逐资产一致；2026-10-12 成交后对账也通过 |
| `GET /api/v3/openOrders` | 同步全账户未结订单 | 2026-10-13 撤单后独立查询为空 |
| `wss://demo-ws-api.binance.com/ws-api/v3` 的 `userDataStream.subscribe.signature` | 订阅 `executionReport`、`outboundAccountPosition`、`balanceUpdate` 等回报 | 2026-10-13 `NEW`、`CANCELED` 已接收并入账；2026-10-12 `TRADE/FILLED` 也通过 |
| `POST /api/v3/order` | 限价挂单 | 2026-10-13 一笔小额虚拟资金订单获接受；此前成交分支也通过 |
| `GET /api/v3/order` | 按 `origClientOrderId` 对账订单 | 2026-10-13 独立查询为 `CANCELED`，订单 ID 匹配 |
| `DELETE /api/v3/order` | 按 `origClientOrderId` 撤单 | 2026-10-13 `cancel_all` 逐笔撤单成功 |
| `GET /api/v3/myTrades` | 按 `orderId` 对账成交 | 2026-10-13 撤单订单返回空；2026-10-12 成交 ID、数量和手续费核对通过 |

项目没有调用 `POST /api/v3/order/test` 或 `DELETE /api/v3/openOrders`。本地 Control 的 `cancel_all` 会对已知订单逐笔调用 `DELETE /api/v3/order`。公开接口探测只核对网络、响应结构和配置规则，不能替代服务内解析、状态更新和 SQLite 对账。

2026-10-11 的公开探测执行了 `python3 -B hquant/test/demo_preflight.py --market`：3 次公开 GET、1 条公开 WS、0 次签名请求、0 笔订单；返回 `clock_skew_ms=499`，深度快照有效，WS 收到 `depthUpdate` 和 `trade`。

本机续跑状态：`hquant/local/binance_demo_credentials.yaml` 已填入用户此前提供的 Demo 凭据，权限为 `0600`，被 `.gitignore` 排除；不要输出或提交其内容。签名预检实际完成了 2 次签名 GET 和 1 次私有 WS 订阅，得到 BTC 总额 0、USDT 总额 5000、未结订单 0；预检退出码为 1，仅因示例配置的 USDT 初始余额仍是 0。账户专用配置已写入忽略目录的 `hquant/local/binance_demo_run.yaml`。使用它启动 C++ 服务只读运行约 6 秒：`private_connected=true`、`syncing=false`、`book_state=live`、深度增量 2 条、交易事件 5 条、未结订单 0，正常退出。运行状态目录为 `/private/tmp/hquant-demo-readonly-cnqt0f1k`。

真实 Demo 验证：`hquant/local/binance_demo_order.yaml` 限定 BTCUSDT、单次最多 1 笔、额度 20 虚拟 USDT。第一笔买入限价单 `hq501246744611291` 获接受，Demo 订单 ID `72218202064`，随后撤销且未成交。私有 WS 挂单及撤单回报都应用到账户状态；独立签名查询确认订单 `CANCELED`、`myTrades` 为空、未结订单 0、USDT 和 BTC 总额及锁定量与 SQLite 一致。状态目录 `/private/tmp/hquant-demo-order-VmGD1k`；历史事件 1724 条，序号连续且无缺口。第二笔在 `hquant/local/binance_demo_fill.yaml` 中尝试等待自然成交，Demo 订单 ID `72218927076`，最终也撤销且未成交；服务端账户同步完成、未结订单 0、未知订单 0，USDT 总额与可用额均为 5000 虚拟 USDT、锁定 0，BTC 仍为 0。状态目录 `/private/tmp/hquant-demo-fill-IV0sAj`，服务已正常停止。两个订单均未产生真实资金扣款。

V2 修改后再次使用只读配置运行 `demo_preflight.py --private`：2 次公开 GET、2 次签名 GET、1 次私有 WS 订阅、0 笔订单；BTC 总额 0、USDT 总额 5000 虚拟资产，未结订单 0，配置余额匹配。随后使用 `hquant/local/binance_demo_fill.yaml` 的单账户、单 BTCUSDT、每次最多一笔挂单、20 虚拟 USDT 硬额度运行 V2 服务。买单 `hq501246880022591` 获 Demo 订单 ID `72242172916`，最终状态 `FILLED`，成交量 `0.00010000 BTC`。私有 WS 的 `NEW`、`TRADE/FILLED` 回报已应用；独立签名查单为 `FILLED`，`myTrades` 返回一笔，未结订单 0。本地 `history.sqlite` 有一笔成交、2 条已应用私有订单回报、无历史缺口；`order_journal.sqlite` 有 1724 条连续记录，SQLite 完整性检查通过。成交后独立账户查询为 BTC `free=0.00009990, locked=0`、USDT `free=4991.69867500, locked=0`，与 SQLite 最后余额事件逐项相等。服务正常退出，最终账户无活动或未知订单。状态与结果在 `/var/folders/01/_wwhzsg161sfcb5h8qz5g4hh0000gn/T/hquant-demo-v2-p7ppkjw6` 的 `demo_validation_result.json`、两份 SQLite 和服务日志中；该账户使用 Binance Demo 虚拟资产。验证后已将 `hquant/local/binance_demo_fill.yaml` 的 `max_submissions_per_run` 恢复为 `0`。

2026-10-11 本轮只读复验：首次 `demo_preflight.py --private` 完成 2 次公开 GET、2 次签名 GET、1 次私有 WS 订阅，零订单、零未结订单；BTC `0.00009990`、USDT `4991.69867500` 与配置一致，但查出未配置的 USDC `5000.00000000`，预检按设计退出 1。将 USDC 纳入**新状态目录**使用的 `hquant/local/binance_demo_run.yaml` 基线并设零交易额度后，再次预检退出 0，三项非零资产均匹配，零未结订单。随后 C++ 只读服务在 `/private/tmp/hquant-demo-projection-ro-01` 连接私有流、完成全账户同步，报告 `ready=false`、`error="demo read-only validation"`、活动／未知订单 0；验收脚本误把 Demo 返回的大量零余额资产视为差异，触发本地断言并结束服务进程。该次失败属于验收脚本，未发送订单。

间隔一分钟后用**同一**状态目录重启 C++ 服务，验证异常退出后的可靠日志回放与全账户同步。Demo 返回 856 个资产条目，只有 BTC `0.00009990`、USDT `4991.69867500`、USDC `5000.00000000` 非零，均与原始基线一致。私有流已连接、同步结束，活动／未知订单均为零；只读门禁仍保持 `ready=false`。正常停机退出码 0；`order_journal.sqlite` 有 2568 条连续记录、两份 SQLite `integrity_check=ok`、历史缺口 0、挂单／撤单意图 0。整轮只读运行未发送交易请求。随后又执行了新版本代码的单笔 Demo 成交测试。

准备单笔读写验收前，又使用 `hquant/local/binance_demo_run.yaml` 做一次只读预检：2 次公开 GET、2 次签名 GET、1 次私有 WS 订阅，BTC/USDT/USDC 仍与配置一致，未结订单 0，零订单。单笔验收配置放在被忽略的 `hquant/local/binance_demo_acceptance.yaml`：仅 BTCUSDT、单次最多 1 笔、20 虚拟 USDT 硬额度；验收后恢复零笔。

新版本 C++ 单笔读写运行使用全新状态目录 `/private/tmp/hquant-demo-projection-order-01`。订单 `hq501246038061851` 获 Demo 订单 ID `72255660989`，以 `83164.00 USDT/BTC` 买入 `0.00010000 BTC`，私有回报确认 `FILLED`；本地成交 ID `320955062`，手续费 `0.00000010 BTC`。服务正常停机，活动／未知订单 0，`history.sqlite` 中一条挂单、一次接受、一笔成交、一次完全成交和两条已应用私有订单回报；最终本地余额 BTC `0.00019980`、USDT `4983.38227500`、USDC `5000.00000000`，均无锁定。两份 SQLite 完整，可靠日志 1724 条连续、历史缺口 0。单笔配置已恢复 `max_submissions_per_run: 0`。

间隔一分钟后对该订单执行 4 次**只读签名 GET**：`GET /api/v3/order` 返回 `FILLED` 且客户／交易所订单 ID、价格、成交数量与 SQLite 一致；`GET /api/v3/myTrades` 返回一笔，成交 ID、数量、价格、BTC 手续费与 SQLite 一致，`time=1791719939756` 与 `OrderJournal` 成交记录的 `transaction_ms` 完全相等；全账户 `GET /api/v3/openOrders` 返回空；`GET /api/v3/account` 的 BTC、USDT、USDC 三项非零余额与 SQLite 最后余额事件逐项相同。核对结果保存于该目录 `independent_check.json`，`valid=true`，未发送任何交易请求。

再间隔一分钟，用恢复为 `max_submissions_per_run: 0` 的**原配置余额基线**在同一订单状态目录重启 C++ 服务：私有流连接且全账户同步结束，BTC `0.00019980`、USDT `4983.38227500`、USDC `5000.00000000` 与独立查询一致；活动／未知订单 0。正常停机退出码 0，可靠日志从 1724 续到 3436 条且序号连续，历史缺口 0，原挂单与成交各只有一条，没有新订单。结果在 `restart_check.json`。这一轮的新版本 C++ Demo 正常成交、私有回报、交易查询、资金归因、两份 SQLite 和重启回放已闭合。

2026-10-12 正确性补测第一次只读预检：使用旧 `binance_demo_run.yaml` 执行 `demo_preflight.py --market --private`，完成 5 次 REST（其中 2 次签名 GET）、公开与私有 WS 各 1 条，订单 0；规则、时钟（偏差 276 ms）、深度快照及公开 `depthUpdate`/`trade`、私有订阅均通过，未结订单 0。查到账户 BTC `0.00019980`、USDT `4983.38227500`、USDC `5000.00000000`；旧配置仍是上次成交前的 BTC `0.00009990`、USDT `4991.69867500`，因此余额基线校验按设计失败。未修改旧状态目录的基线；下一步用独立新配置和新状态目录复检。

2026-10-12 第二次只读预检：在忽略目录中新建 `binance_demo_correctness_20261012.yaml`，以当前三项资产总额作为**新状态目录**的基线，`max_submissions_per_run: 0`。`demo_preflight.py --private` 完成 4 次 REST（2 次签名 GET）和 1 次私有 WS 订阅，零订单、零未结订单；规则与余额均匹配，退出码 0。旧配置和旧状态目录基线未改。

2026-10-12 首次 C++ 只读服务检查：新状态目录 `/private/tmp/hquant-demo-correctness-20261012-readonly`；服务完成私有连接、账户同步及盘口同步，并正常退出，日志未见交易请求。验收脚本在服务退出后错误地要求普通 Demo 运行生成压测专用的 `probe.json`，因此脚本退出 1；这是验收脚本错误，不计作服务通过。已移除该要求，待用另一个新状态目录复跑。

2026-10-12 C++ 只读复跑通过：使用 [只读验收脚本](../../hquant/test/demo_readonly_service.py)和新状态目录 `/private/tmp/hquant-demo-correctness-20261012-readonly-b`。私有连接成功、盘口 `live`（深度 4 条、公开成交 33 条），账户同步完成，`ready=false` 且只读门禁有效；活动／未知订单 0，BTC、USDT、USDC 三项余额与独立签名预检一致。服务退出码 0；`history.sqlite` 无缺口或订单事件，`order_journal.sqlite` 有 1712 条连续记录且无订单意图，两库 `integrity_check=ok`；实际下单 0。机器可读结果见该目录 `result.json`。

2026-10-12 单笔订单前再次执行只读私有预检时，Demo `/api/v3/account` 返回 HTTP 400；脚本未重试，也未启动下单服务。当前无法将这次预检计为通过；已给预检脚本增加只打印 Binance 错误码的诊断，待间隔后复查。

间隔后再次执行同一只读预检：4 次 REST（其中 2 次签名 GET）、1 次私有 WS 订阅，余额与配置匹配，未结订单 0，退出码 0；上一轮 HTTP 400 未复现，原因未确认。该次仍未发送订单。

随后执行一次单笔小额 Demo 验收，状态目录 `/private/tmp/hquant-demo-correctness-20261012-order`。C++ 服务只发送 1 笔 BTCUSDT 订单 `hqcbc59c31b54eeea3648eef34ae3d0219`，Demo 订单 ID `72624963969`；私有 `NEW` 和 `CANCELED` 均写入本地历史，未成交。验收脚本的 `cancel_all` 控制请求遗漏必填的 `account_id/operator/reason`，因而脚本退出 1；服务停止清场时成功撤单并正常退出。两份 SQLite `integrity_check=ok`，本地各有 1 次提交和撤单；尚需独立 Demo 查单、查余额后才能判为通过。控制请求脚本已修正。

对这笔订单的首次独立查询中，`GET /api/v3/order` 和 `GET /api/v3/myTrades` 返回后，`GET /api/v3/openOrders` 返回 HTTP 400、Binance 错误码 `-1021`，故未完成全账户对账；没有重试或发送新订单。独立只读查询的签名有效期已从 5 秒增至 10 秒（8 秒网络超时），待间隔后完成剩余核对。

间隔后对同一订单完成四次独立签名 GET（查单、成交、全账户未结订单、余额）：订单 `CANCELED`，`myTrades` 为空，未结订单 0；BTC `0.00019980`、USDT `4983.38227500`、USDC `5000.00000000` 的 `free/locked/total` 与 `history.sqlite` 最后余额事件逐项相等，三项锁定额均为 0。`history.sqlite` 与 `order_journal.sqlite` 各有 1726 条连续记录、`integrity_check=ok`，订单只提交一次、撤单一次、无成交；结果见 `/private/tmp/hquant-demo-correctness-20261012-order/independent_check.json`，`valid=true`。整个补测仅新增这一笔 Demo 虚拟资金订单，且已撤销。

真实成交分支补测前，用独立的零笔配置 `binance_demo_correctness_fill_20261012.yaml`（报价价差 `0`、刷新 120 秒；余额基线仍是上次查得的三项资产）执行只读私有预检：4 次 REST（2 次签名 GET）、1 次私有 WS 订阅，余额匹配，未结订单 0，退出码 0。该配置本身不下单；后续脚本只会在新状态目录临时启用一笔上限。

2026-10-12 真实成交补测通过：新状态目录 `/private/tmp/hquant-demo-correctness-20261012-fill` 仅发一笔 BTCUSDT 买单 `hq52d4d13c13d23c7d04ea3584a01d75dd`，Demo 订单 ID `72626673505`。私有流收到 `NEW` 和 `TRADE/FILLED`；成交 ID `321511253`，`0.00010000 BTC × 82739.22 USDT`，手续费 `0.00000010 BTC`。验收脚本的 `result.json` 为 `valid=true`：订单 1、成交 1、撤单 0、未结／未知订单 0；两份 SQLite 各 1725 条记录、序号连续且完整、历史缺口 0。间隔后 4 次独立签名 GET 核对查单、成交、全账户未结订单及余额，`independent_check.json` 也为 `valid=true`；最终 BTC `0.00029970`、USDT `4975.10835300`、USDC `5000.00000000`，全部可用且锁定为 0，与本地逐资产相等。测试仅使用 Demo 虚拟资金。

同一状态目录以原始余额基线、`max_submissions_per_run: 0` 只读重启后，私有连接和盘口恢复，余额仍与独立查询一致；活动／未知订单 0，没有再次发单或重复记成交。前次 `cancel_all` 留下的持久状态显示 `manually paused`；这不影响零笔门禁和对账。重启后的 `order_journal.sqlite` 共 5149 条连续记录，挂单意图与成交各仍只有 1 条；`history.sqlite` 三次运行分别有 1725、1712、1712 条连续记录，历史缺口 0，两库完整性检查通过。结果见 `restart_check.json`。

2026-10-13 Demo 客户端延迟采样：旧只读配置的余额基线仍是上次成交前数值，第一次私有预检按设计报不匹配，**未启动下单服务**。新建权限 `0600` 的 `hquant/local/binance_demo_latency_20261013.yaml`，以 BTC `0.00029970`、USDT `4975.10835300`、USDC `5000.00000000` 为新状态目录基线；私有预检通过：4 次 REST（2 次签名 GET）、1 次私有 WS 订阅、未结订单 0、挂单 0。间隔后运行 Release C++ 服务，状态目录 `/private/tmp/hquant-demo-latency-20261013-a1`：服务内交易 REST 6 次（查询 4、挂单 1、撤单 1），另有公开盘口快照；独立对账再做 4 次签名 GET。唯一订单 `hqcd09873e0b6f5b7e6b22b282ac4b3e1b`、交易所订单 ID `72750884979`，收到 `NEW → CANCELED`，成交 0，未结／未知订单 0；两库完整且序号连续，`result.json` 为 `valid=true`。取消是修正后的 `cancel_all` 控制请求触发，本次该控制链路通过。探针 `probe.json`、`rest_client_trace.csv`、`private_client_trace.csv` 和 [逐段结果](/private/tmp/hquant-demo-latency-20261013-a1/latency.json)记录客户端单笔耗时：下单 REST 入队→完整 HTTP 响应 212.051 ms，订单请求→私有 `NEW` 本地记录 212.680 ms；服务端收单时间不可见。订单观察 2 秒后撤销，私有 `CANCELED` 时间不代表成交速度；本轮没有新增 `FILLED` 延迟样本。

## 尚未完成的验收

V2 旧版本的一笔 Demo 成交与重启结果保存在其原状态目录的 `restart_validation_result.json`。2026-10-12 的单笔撤单、单笔成交、独立查询和成交后同目录只读重启均已通过。当前 Demo 补测配置已恢复为零笔挂单；旧配置再次使用前要按状态目录的原始基线和全账户非零资产核对。1000 标的、30 秒场景只对本机模拟交易所运行。

以下表格是**尚未完成的验收清单**，不是已有 Demo 结果。本机模拟交易所覆盖了部分故障，但本机通过不能替代 Demo 验证。状态只在取得可复核的请求、回报、账户和 SQLite 证据后改为通过；无法在 Demo 可控复现的场景保持“Demo 未验证”，不能用一次正常运行推定通过。

### 在线模拟：待验证的异常路径

| 场景与注入点 | 期望行为与通过条件 | 现有证据 / 状态 |
| --- | --- | --- |
| `POST /api/v3/order` 已可能到达交易所，但响应丢失或超时；私有 `NEW`/`TRADE` 也可能晚到或丢失 | 保留原 `client_order_id`、`PendingSubmit` 的未知结果标志和资金占用；不以新 ID 重发，不把超时当拒单。按原 ID 查订单、按交易所订单 ID 查成交；回报或查询确认后只应用一次成交和手续费。一直查不明时保持未决、禁止该 shard 再下单。 | 本机 `test_lost_submit_response_and_private_events_recover_by_query`、`test_ambiguous_submit_and_cancel_resolve_without_duplicate_submit` 已通过；Demo 未验证。 |
| `DELETE /api/v3/order` 响应丢失，且撤单与部分／全部成交竞态 | `PendingCancel` 进入结果未知后仍占用剩余资金；未知期间不重复撤单、不挂替代单。先按原订单 ID 查订单和成交；确认 `CANCELED` 后按实际成交量结算并释放剩余占用；至少两次间隔且**成功**的查询证明订单仍开放，才允许将此次撤单视为失败并重新决策。晚到的 HTTP/私有回报不能倒退已确认终态。 | 本机故障及竞态用例已覆盖；Demo 的普通撤单已通过，但丢响应和成交竞态未验证。当前代码按查询尝试次数判断第二次查询，夹杂查询失败时可能提前判定，需补负例并修正。 |
| 私有 WS 断线，或重连期间漏掉 `executionReport`、余额事件 | 连接断开立即暂停新单，`private_connected=false`。重连后先查规则、全账户未结订单、逐单状态／成交和余额；按成交 ID 去重、补齐遗漏回报，资金总额须与原始基线加已确认成交及手续费相符。全部对账完成且连接健康才恢复；查询失败或差异未消除则继续暂停。 | 本机 `test_private_reconnect_reconciles_before_trading`、漏回报查单用例已通过；Demo 未验证。 |
| 同一 Demo 账户被其他客户端下单、成交或转账，导致未知未结订单或无法归因的资产变化 | 发现未知订单或余额总额与本系统成交投影不符时暂停新单，保留原始余额基线和现场。不得把当前余额改成旧状态目录的新基线，也不得自动把账户外成交记为本系统成交；须人工核对外部活动，再决定如何迁移／重建状态。仅调用 `resume_trading` 不能绕过仍存在的差异。 | 本机 `test_external_balance_change_pauses_before_new_order`、错误基线重启用例已通过；Demo 未验证。 |
| 成交手续费扣在 BTC/USDT 之外的第三资产，例如 BNB | 新状态目录先配置账户所有非零资产；成交按实际 `fee_asset` 扣一次，私有余额、全量账户查询、`OrderJournal`、`history.sqlite` 与独立 `myTrades` 逐资产一致。手续费资产未配置或余额无法归因时暂停新单，不静默忽略费用。 | 本机 `test_third_asset_fee_matches_venue_and_sqlite` 已通过；Demo 目前只观察到 BTC 手续费，第三资产手续费未验证。 |

Demo 异常试验保持单账户、单 BTCUSDT、最多一笔小额虚拟资金订单；按本页“验证顺序”先用零笔配置预检，并控制请求间隔。丢响应和断线应通过可控的客户端／网络故障复现，不能靠反复真实下单碰运气。若 Demo 无法稳定提供账户外活动或第三资产手续费样本，就保留上述未验证状态和本机证据，不制造大流量或绕开交易限额。每次真实 Demo 检查后，在本页记录时间、实际请求数、订单 ID、故障点、最终对账结果和状态目录。

### 系统层：仍需专项验收或补齐的能力

| 场景 | 期望行为与通过条件 | 当前状态 |
| --- | --- | --- |
| 发送队列满、挂单排队过期、429/418 或查询失败 | 明确未发出的命令在可靠记录 `Abort` 后释放占用；可能已发出的命令保持未知并按原 ID 对账。限流按服务端退避，不因查询暂时失败猜测拒单或密集重试。 | 本机队列／限流路径已有测试；跨进程共享出口的**总请求预算**尚未实现／验收。Demo 不主动制造 429/418。 |
| 真实磁盘满、SQLite I/O 错误或历史写入失败 | 发送意图落可靠日志前不能发包；写入失败后暂停新单，已可能发送的订单保留未决并在恢复后对账。服务状态和退出码必须暴露故障，历史缺口不能报为完整审计。 | 本机 SQLite 写入触发器故障用例已通过；真实磁盘／文件系统故障与恢复未验收。 |
| 在提交意图、开始写 socket、交易所接受、回报入日志、撤单等节点崩溃 | 用同一状态目录和**首次启动的余额基线**重启；日志序号连续、两份 SQLite 完整；逐个原 `client_order_id` 查单和成交，不重复挂单或扣费；未知结果保持占用，直到核实。 | 本机已有未结订单重启、成交后同目录 Demo 只读重启；逐节点崩溃矩阵及跨机器恢复未验收。 |
| 长时间运行、日志增长及成交时间窗口上限 | 明确可靠日志保留／压缩和恢复方案；长时间运行中订单、资金、历史一致且队列和存储不会无限增长。单资产自上次全量对账后达到 8192 条成交时间记录上限时，应安全暂停或先完成对账，不能丢失成交后继续交易。 | 仅有本机 1000 标的、30 秒压力 case；长期运行、日志压缩和上限恢复未验收。 |
| 策略库存、费用后净损益与风险边界 | 单独定义库存上限、费用和成交归因、净损益及风险指标；用可复核样本验收。交易链路正确与一次 Demo 成交不证明策略收益。 | 属于策略验收，尚未完成；不计入上述订单协议通过项。 |

**全部验收通过的门槛：** 前两表每个必需项都有可复核结果，或明确调整了受支持的运行范围；`status.accounts`、交易所独立查询、逐 ID 成交与手续费、资产总额、`order_journal.sqlite` 序号和 `history.sqlite` 一致。正常单笔成交、正常撤单和 30 秒本机压测已通过，不能替代这些异常路径。
