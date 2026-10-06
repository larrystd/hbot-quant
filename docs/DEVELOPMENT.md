# hquant 1.0 开发文档

本文件说明如何把 [设计](DESIGN.md) 落成代码。目标是 C++20、Shard 共用最多 8 个 Asio 工作线程并在所属线程串行处理事件；1.0 只做 Binance 公开行情、文件回放和本地模拟交易。

## 1. 代码边界

主干保持 `hquant/src/main.cc`、`config.{h,cc}`、`runtime.{h,cc}`、`replay_reader.{h,cc}`、`sqlite_history.{h,cc}`、`control_server.{h,cc}`，以及 `base/` 和 `shard/`。`shard/` 内只有 Market、Strategy、Executor、PaperExchange 和盘口实现；具体文件见设计文档第 2 节。入口是 `//hquant/src:hquant_server`。

| 所有者 | 持有的数据 | 对外方法 |
| --- | --- | --- |
| Runtime | Shard、Control、SQLite 的生命周期与路由 | `Start`、`Stop`、`Wait`、投递状态或配置请求 |
| Shard | 所属 `io_context` 的引用、Market、Strategy、Executor、策略定时器 | `OnMarket`、`OnTimer`、`PostReplay`、`UpdateStrategy`、`RequestStop` |
| Market | 连接代次、盘口、快照序号、增量缓冲、最近成交 | 行情协程、`OnReplayInput`、`View`、`CheckStale` |
| Strategy | 配置和 `next_refresh_at` | `Decide`、`UpdateConfig` |
| Executor | PaperExchange、交易规则、静态资金上限 | `OnMarket`、`Execute`、`View` |
| PaperExchange | 活动订单与模拟账户余额 | `Place`、`Cancel`、`OnBookBbo`、`OnPublicTrade` |

Market 更新盘口后同步通知 Shard。Shard 同步调用 Executor、Strategy、Executor；这些调用中不 `co_await`。跨线程请求只用 `asio::post` 进入所属 Shard。单个输入处理完成前，不处理该 Shard 的下一条输入。

## 2. gflags 与配置生效

采用 gflags 作为进程入口的命令行解析器，Bazel Bzlmod 固定 `gflags@2.3.1`。只在 `main.cc` 定义、解析和读取 `FLAGS_*`；解析结果立即转换为有类型的 `AppConfig`，其他模块不依赖 gflags 头文件。gflags 自带的 `--flagfile` 可用于启动参数文件。实现依据是 [gflags 官方用法](https://gflags.github.io/gflags/)和 [Bazel Central Registry 的模块声明](https://registry.bazel.build/modules/gflags/)。

| 启动参数 | 类型 | 用途 |
| --- | --- | --- |
| `--config` | string，必填 | YAML 配置路径，提供市场、分片和账户静态配置 |
| `--state_dir` | string，必填 | SQLite 与 Control socket 的目录 |
| `--strategy_shard` | int32，可选 | 下列策略覆盖值要应用的 Shard ID |
| `--order_amount` | string，可选 | 十进制下单数量，精确解析为 `uint64_t` 纳单位 |
| `--bid_spread` / `--ask_spread` | string，可选 | 十进制价差；必须与 `--strategy_shard` 一起使用 |
| `--refresh_ms` | int64，可选 | 正整数刷新周期；必须与 `--strategy_shard` 一起使用 |

启动顺序固定为：解析 gflags → 读取 YAML → 只把显式提供的策略 flag 覆盖到指定 Shard → 校验完整 `AppConfig` → 构造 Runtime。没有指定覆盖 flag 时，YAML 值不变。十进制数按 10^9 缩放为 `uint64_t`，解析和计算都检查范围，不经过 `double`。多个 Shard 不使用隐含的“全部覆盖”；必须明确 Shard ID。运行过程中不让交易线程读取或写入全局 `FLAGS_*`。

### 运行中修改

动态修改经 Control 的 `set_strategy` 请求完成；gflags 负责启动值，不充当跨线程共享配置。请求只允许修改 `order_amount`、`bid_spread`、`ask_spread`、`refresh_ms`：

~~~json
{"op":"set_strategy","request_id":42,"shard_id":1,"expected_version":3,"patch":{"bid_spread":"0.002","refresh_ms":15000}}
~~~

Control 解析 JSON 后形成 `StrategyPatch` 值对象，通过 Runtime 投递到对应 Shard。Shard 在自己的线程上复制当前配置、应用 patch、校验完整候选值；失败则原配置不变，成功则递增 `config_version`、清空 `next_refresh_at`，使用当前盘口和余额进行一次正常策略决策。旧单先同步撤销，再决定新单。返回值包括新的版本与生效结果；`expected_version` 不匹配则拒绝，避免两个管理请求互相覆盖。Market 连接参数、交易规则、账户初始余额、资金上限、费率和存储路径运行中不可修改。

Runtime 不用 `gflags::SetCommandLineOption` 向工作线程传播配置；这样可以在 Shard 的事件边界上一次性提交一组有类型参数，而不是让策略在一次决策中读到不同版本的全局 flag。

## 3. 热点路径与依赖

- 使用具体类和有穷的 `std::variant` 输入/决定；不为 Market、Strategy、Executor 再加基类、虚函数工厂或通用事件总线。
- 盘口价位与数量保留整数 ticks/lots。网络文本在 Market 边界精确解析一次；策略价格、订单校验和账户金额用 `uint64_t` 定点数，乘法使用扩展中间值并检查溢出。
- `BookView`、`ExecutionView` 是只在当前同步调用中有效的只读视图；不复制整个订单簿，不在视图中保存可跨事件失效的指针。
- 每个 Shard 独占交易状态；`StrategyConfig` 在 Shard 线程内按值替换，不给盘口或订单字段加逐字段锁。SQLite 写入与 Control 查询不阻塞行情回调。
- Market 和 Shard 直接声明所持有的状态字段，不引入 `Impl`/pimpl。
- 增量缓存和历史队列有容量上限；缓存溢出使 Market 重新同步，历史队列满时记录明确错误。每次输入的决策最多两轮：撤旧单后可再决策一次，不递归触发下一轮。
- 头文件只暴露本模块的数据与方法。前置声明能替代包含时使用前置声明；网络、SQLite、JSON、gflags 依赖留在对应 `.cc` 或边界文件。按解析、状态更新、动作生成、回报整理拆函数，避免一个函数同时处理协议、交易决策与存储。
- RAII 管理线程、socket、SQLite statement 与事务；时间使用 `std::chrono`，错误以明确返回值传递。核心交易流程不使用异常作正常分支。
- 注释只解释不能从代码直接看出的约束：哪个线程拥有状态、回调何时同步完成、序号衔接条件、额度计算和停止顺序。函数名与类型已经表达清楚的步骤不逐行复述；公开方法只在调用条件容易误用时写简短注释。

## 4. 实现顺序

1. **入口与配置**：加入 gflags 依赖、新 `main.cc` 和扁平 `AppConfig`；完成启动 flag 覆盖与静态校验。
2. **Shard 核心**：Runtime 创建共享的 `io_context` 和工作线程，并把每个 Shard 固定分配给一个线程；实现同步的 `OnMarket`、定时入口、跨线程投递及有序停止。
3. **Market**：把 Binance REST/WS 解析、快照/增量同步和回放输入收进一个具体 `Market`；只保留 Syncing/Live 两态。
4. **Strategy 与 Executor**：实现具体 `Strategy`、限价单决定、PaperExchange 账户事件、活动订单与余额的唯一所有权；移除目标路径上的重复资金冻结表。模拟下单回执与订单事件使用实盘可采用的分离语义。
5. **回放、历史、Control**：回放按输入时间逐条投递并等待完成；SQLite 异步写入；Control 提供 `status`、`history`、`set_strategy`、`stop`，动态配置只在 Shard 线程提交。
6. **收口构建图**：新二进制只链接 1.0 所需模块；旧架构文件退出新入口的依赖图，文档和示例指向新入口。

本轮先完成代码和必要的开发文档；测试与验收工作后置。

## 5. 分模块开发 prompt

以下 prompt 用于逐块实现或审查代码。每次只带相关头文件、实现文件和本设计的对应章节，避免把旧架构整体复制进新主干。

**Shard / 调用顺序**

> 按 DESIGN.md 第 3、4、5 节实现 Runtime 持有共享 io_context 和工作线程、每个 Shard 固定归属一个 io_context。Market 更新完状态后同步回调 Shard；Shard 在一个事件里依次做模拟撮合、Strategy 决策、Executor 执行，撤单后最多再决策一次。跨线程入口只投递值对象到所属线程。保留实际必要的状态，避免 Strategy/TradingMode 等虚基类和第二份订单、余额状态。标出线程归属和回调完成边界，其他显然的步骤不加注释。

**Market / 行情**

> 按 DESIGN.md 第 5.1、6.1、6.2 节实现 Market 的 Syncing/Live 转换。盘口用整数 ticks/lots；实时输入的快照与增量严格按序衔接，缓存设上限；文件回放还允许连续序号的完整快照直接替换盘口。状态更新完成后才发 BookChanged、PublicTrade 或 BookUnavailable；不要把原始行情绕 Shard 再送回 Market。仅对序号边界和失效通知加必要注释。

**Strategy / Executor / PaperExchange**

> 按 DESIGN.md 第 4、5.2 节实现具体 Strategy、Executor、PaperExchange。Strategy 除配置只保存 next_refresh_at；PaperExchange 唯一持有活动订单和余额，命令只返回本地派发结果，订单接受、拒绝、部分成交、完成、取消及余额变更进入独立的 AccountEvent 路径。Executor 做交易规则和额度校验，不维护第二份账。价格、数量、费用用 `uint64_t` 定点数并检查运算溢出，盘口档位继续用整数 ticks/lots。拆开价格计算、订单校验、资金检查和回报生成函数；注释重点写清部分成交后的剩余量和资金占用。

**配置 / Control / 存储**

> 按 DEVELOPMENT.md 第 2 节实现 gflags 启动覆盖和 Control 的 set_strategy。FLAGS_* 只在 main.cc 读取，立刻转换成有类型配置；运行中 patch 整体校验后在 Shard 线程一次提交并递增版本。SQLite 写入不阻塞行情回调，status 在目标 Shard 线程取快照。不要引入通用配置注册表或事件总线；注释只说明配置提交原子性、队列满时行为和资源关闭顺序。
