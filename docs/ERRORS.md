# 错误码

状态：已实现（2026-10-01）。本文定义 hquant 自己的 106 个业务错误码，取代直接使用 absl 的通用错误码表达业务原因。跨模块错误仍以 `absl::Status` / `absl::StatusOr<T>` 承载；风控和限速等内部热路径直接返回 `ErrorCode`。

## 1. 为什么要自定义

现状（`hquant/src` 与 `apps`）：约 380 处直接构造 absl 错误，其中 `InvalidArgument` 196 处、`FailedPrecondition` 69 处。absl 的错误码只是通用分类，同一个 `InvalidArgument` 既可能是配置写错，也可能是一条行情报文损坏，还可能是下单数量不合法。它无法回答两个业务问题：**出了什么事**，**该怎么处理**。

这已经造成实际问题：`market/binance_spot_feed.cc` 的 `Run()` 遇到 `InvalidArgument` 或 `OutOfRange` 就永久停止行情流。配置错误应该停止，但一条损坏的 WebSocket 报文同样返回 `InvalidArgument`，结果行情流不再重连，该市场一直停在不可用状态。`service/routing.cc` 也是靠 absl 错误码反推路由失败原因，其他错误码都会被静默归为"归属冲突"。

## 2. 编号规则

- `0` 表示成功，不出现在表中。
- 业务错误码从 `10000` 开始，**每一类占一个千位段**：`10000` 通用、`11000` 网络与限速、`12000` 行情……`19000` 控制与命令行。
- 段内从 `xx001` 起顺序编号，`xx900` 以后留给该类的内部错误。
- 编号一旦发布（写入 SQLite、控制协议、日志），**不得复用或改变含义**。废弃的编号保留并标注"废弃"；新增只往段尾追加。
- 每个错误码有一个稳定的大写名字（如 `FEED_MESSAGE_INVALID`），日志、CLI、SQLite 输出名字和数字两者；代码中枚举名为 `kFeedMessageInvalid`。

| 段 | 类别 | 主要来源 |
| --- | --- | --- |
| 10000 | 通用 | `base/types`，各模块共用的序号耗尽、取消、内部错误 |
| 11000 | 网络与限速 | `base/net`、`base/rate_limit` |
| 12000 | 行情 | `market/*` |
| 13000 | 订单 | `order/order_tracker`、`order/paper`、`order/binance_spot_gateway` |
| 14000 | 风控与额度 | `order/risk` |
| 15000 | 账户回报、对账与路由 | `order/binance_spot_account`、`service/routing` |
| 16000 | 分片服务 | `service/shard`、`service/dispatcher` |
| 17000 | 存储 | `offline/*` |
| 18000 | 配置与进程 | `application/config`、`application/launcher` |
| 19000 | 控制与命令行 | `application/control`、`cli` |

## 3. 处理方式

每个错误码登记一种处理方式。调用方按处理方式分支，不逐个枚举错误码。

| 处理方式 | 含义 | 典型调用方动作 |
| --- | --- | --- |
| `Retry` | 暂时性故障，同一操作稍后可能成功 | 退避后重试；超过次数上限升级为 `Halt` |
| `Resync` | 本地派生状态不可信，需要重新同步 | 丢弃本地盘口，重新订阅和取快照；有次数上限，防止无限重同步 |
| `Reconcile` | 订单或账户事实不确定 | 保留最坏敞口，按原 ID 向交易所查询并核对；不得重发 |
| `Reject` | 本次请求不成立，系统其余部分正常 | 拒绝该请求，记录原因并计数 |
| `Degrade` | 继续运行，但能力降低 | 告警，标记缺口或暂停受影响的市场/账户新单 |
| `Halt` | 继续运行不安全或没有意义 | 停止该组件；启动阶段则拒绝启动；运行中触发紧急停止 |

## 4. 接口

```cpp
// base/error.h
enum class ErrorCode : uint16_t { kOk = 0, /* 见第 5 节 */ };
enum class Recovery : uint8_t { Retry, Resync, Reconcile, Reject, Degrade, Halt };

struct ErrorInfo {
  ErrorCode code;
  std::string_view name;       // "FEED_MESSAGE_INVALID"
  Recovery recovery;
  absl::StatusCode canonical;  // 对应的 absl 错误码，供通用工具使用
};

const ErrorInfo& Info(ErrorCode code);
std::span<const ErrorInfo> AllErrorInfo();

// 构造：canonical 取自表，业务码写入 Status payload（类型 URL "type.hquant/error"）
absl::Status Error(ErrorCode code, std::string_view message);

// 读取：没有业务码的 Status（第三方库直接返回的）视为 kInternal
ErrorCode CodeOf(const absl::Status& status);
Recovery RecoveryOf(const absl::Status& status);

// 系统错误映射：std::error_code / errno → 业务码，消息带上下文（如路径）
absl::Status ErrorFromSystem(ErrorCode code, std::error_code ec, std::string_view context);
```

使用规则：

1. 业务代码统一用 `Error(code, message)` 构造错误。除 `base/error.cc` 外，不直接调用 `absl::XxxError`；用 grep 检查加入 CI。
2. 第三方库或系统调用返回的错误，在边界处转换为业务码（例如 `net` 把 Beast 的超时转成 `NET_TIMEOUT`）。
3. **热路径的拒绝**（风控、限速、市场未就绪、动作分发）直接返回 `ErrorCode`，不构造带消息的 `absl::Status`，避免每次拒单都分配内存；拒单统计按错误码计数。
4. 消息文本只给人看，写清对象和值（市场、订单号、路径），代码不解析消息。
5. 错误码按"调用方需要区分的业务情况"划分，不是一个报错位置一个码。程序内部不变量被破坏统一用 `INTERNAL`。

payload 中的业务码以十进制 ASCII 编码；读取到未知编号、非法 payload 或第三方无 payload 错误时归为 `INTERNAL`。`CodeOf(OkStatus())` 返回 `kOk`，调用方只对非 OK 状态使用 `RecoveryOf`。

## 5. 错误码表

"absl" 列是 `canonical`。"现有来源"列出当前代码中将改用该码的位置，供迁移时对照。

### 10000 通用

| 码 | 名称 | 处理 | absl | 含义 | 现有来源 |
| --- | --- | --- | --- | --- | --- |
| 10001 | `DECIMAL_INVALID` | Reject | InvalidArgument | 十进制解析失败：空、非法字符、非正步长、比较失败 | `base/types` |
| 10002 | `DECIMAL_PRECISION_EXCEEDED` | Reject | OutOfRange | 超过 28 位有效数字 | `base/types` |
| 10003 | `SEQUENCE_EXHAUSTED` | Halt | ResourceExhausted | 本地递增序号耗尽：分片记录序号、预留 ID、客户端订单号序号、行情 epoch | `service/shard`、`order/risk`、`order/binance_spot_gateway`、`market/binance_spot_feed` |
| 10004 | `CANCELLED` | Reject | Cancelled | 组件正在停止，操作被取消 | `base/net`、`market/binance_spot_feed` |
| 10005 | `UNIMPLEMENTED` | Reject | Unimplemented | 尚未支持的功能 | — |
| 10900 | `INTERNAL` | Halt | Internal | 程序不变量被破坏，属于缺陷 | `order/risk`（预留对应的租约消失）及未转换的第三方错误 |

### 11000 网络与限速

| 码 | 名称 | 处理 | absl | 含义 | 现有来源 |
| --- | --- | --- | --- | --- | --- |
| 11001 | `NET_TLS_CONFIG_INVALID` | Halt | FailedPrecondition | TLS 上下文、CA 文件或 SNI 设置失败 | `base/net` |
| 11002 | `NET_TARGET_INVALID` | Reject | InvalidArgument | HTTP 方法、目标路径或 WebSocket 目标非法 | `base/net` |
| 11003 | `NET_TIMEOUT` | Retry | DeadlineExceeded | 连接、请求或读写超时 | `base/net` |
| 11004 | `NET_UNAVAILABLE` | Retry | Unavailable | 连接断开、不可达、陈旧连接重试耗尽 | `base/net` |
| 11005 | `NET_BUSY` | Retry | FailedPrecondition | 同一连接上已有进行中的操作 | `base/net` |
| 11006 | `NET_TLS_VERIFY_FAILED` | Halt | Unavailable | 证书链或主机名校验失败；不得携带凭据重试 | `base/net`（目前混在 `NET_UNAVAILABLE` 中，需拆出） |
| 11007 | `HTTP_STATUS_UNEXPECTED` | Retry | Unavailable | 交易所返回非预期 HTTP 状态 | `market/binance_spot_feed` |
| 11008 | `VENUE_RATE_LIMITED` | Retry | ResourceExhausted | 交易所限速（429）或快照被限流；按 `Retry-After` 退避 | `market/binance_spot_feed` |
| 11009 | `VENUE_IP_BANNED` | Halt | ResourceExhausted | 交易所封禁（418）；触发全局熔断 | 新增 |
| 11010 | `RATE_BREAKER_OPEN` | Reject | Unavailable | 全局熔断中，暂停该类请求 | `RateDecision::BreakerOpen` |
| 11011 | `RATE_LEASE_MISSING` | Reject | FailedPrecondition | 分片没有该类请求的限速租约 | `RateDecision::NoLease`、`base/rate_limit` |
| 11012 | `RATE_LEASE_EXHAUSTED` | Reject | ResourceExhausted | 分片限速额度用完 | `RateDecision::Exhausted`、`order/binance_spot_gateway` |
| 11013 | `RATE_CONFIG_INVALID` | Halt | InvalidArgument | 限速容量、租约授予或请求权重非法、重复或超过全局容量 | `base/rate_limit`、`RateDecision::InvalidWeight` |

### 12000 行情

| 码 | 名称 | 处理 | absl | 含义 | 现有来源 |
| --- | --- | --- | --- | --- | --- |
| 12001 | `FEED_CONFIG_INVALID` | Halt | InvalidArgument | 行情流配置、`BookScale` 或 epoch 非法 | `market/binance_spot_feed` |
| 12002 | `FEED_MESSAGE_INVALID` | Resync | InvalidArgument | 报文无法解析或字段非法：JSON、事件类型、价位、序号区间、成交字段 | `market/binance_spot_feed`、`BookApplyReason::InvalidMessage` |
| 12003 | `FEED_SCALE_MISMATCH` | Resync | OutOfRange | 价格或数量不能按 `BookScale` 整除或超出 64 位范围；连续出现超过阈值时升级为 `FEED_CONFIG_INVALID` | `market/binance_spot_feed`、`BookApplyReason::InvalidScale` |
| 12004 | `FEED_WRONG_MARKET` | Resync | InvalidArgument | 报文不属于该行情流配置的市场 | `market/binance_spot_feed` |
| 12005 | `BOOK_SEQUENCE_GAP` | Resync | Aborted | 增量序号缺口、缓冲增量无效、快照接不上首条增量 | `market/binance_spot_feed`、`BookApplyReason::Gap` |
| 12006 | `BOOK_CROSSED` | Resync | Aborted | 连续交易时段买一 ≥ 卖一 | `BookApplyReason::CrossedBook` |
| 12007 | `BOOK_BUFFER_OVERFLOW` | Resync | ResourceExhausted | 快照到达前缓存的增量超过上限 | `BookApplyReason::BufferOverflow` |
| 12008 | `BOOK_STALE` | Degrade | Unavailable | 超过阈值没有更新；该市场暂停新单 | `market/order_book` 的 Stale 状态 |
| 12009 | `FEED_DISCONNECTED` | Retry | Unavailable | 行情连接断开 | `BookApplyReason::Disconnected` |
| 12010 | `REPLAY_FILE_NOT_FOUND` | Halt | NotFound | 回放行情文件无法读取 | `market/replay_feed` |
| 12011 | `REPLAY_FILE_INVALID` | Halt | InvalidArgument | 回放文件 JSON、schema、字段或输入类型非法 | `market/replay_feed` |

`BookApplyReason::OldDiff` 是正常丢弃旧增量，不是错误，保留为普通枚举值。

### 13000 订单

| 码 | 名称 | 处理 | absl | 含义 | 现有来源 |
| --- | --- | --- | --- | --- | --- |
| 13001 | `ORDER_INVALID` | Reject | InvalidArgument | 订单字段或类型组合非法：缺限价、`LIMIT_MAKER` 带 TIF、方向/类型不支持 | `service/dispatcher`、`order/risk`、`order/paper`、`order/binance_spot_gateway`、`order/order_tracker` |
| 13002 | `ORDER_RULE_VIOLATION` | Reject | FailedPrecondition | 不满足交易规则：未对齐步长、低于最小数量或最小名义金额、高于最大数量 | `order/risk`、`order/paper`、`order/binance_spot_gateway` |
| 13003 | `ORDER_DUPLICATE` | Reject | AlreadyExists | 客户端订单号已被跟踪或发生碰撞 | `order/order_tracker`、`order/paper`、`order/binance_spot_gateway` |
| 13004 | `ORDER_NOT_FOUND` | Reject | NotFound | 订单未被跟踪，或预备发送的订单已不存在 | `order/order_tracker`、`order/paper`、`order/binance_spot_gateway` |
| 13005 | `ORDER_NOT_CANCELABLE` | Reject | FailedPrecondition | 订单已是终态，或当前状态不可撤销、不可中止 | `order/order_tracker`、`order/binance_spot_gateway` |
| 13006 | `ORDER_CANCEL_PENDING` | Reject | AlreadyExists | 撤单请求已在途 | `order/binance_spot_gateway` |
| 13007 | `ORDER_SLOTS_FULL` | Reject | ResourceExhausted | 下单或撤单发送槽已满 | `order/binance_spot_gateway` |
| 13008 | `ORDER_EXPIRED_BEFORE_SEND` | Reject | DeadlineExceeded | 订单在写入网络前已过期 | `order/binance_spot_gateway` |
| 13009 | `ORDER_SUBMISSION_UNKNOWN` | Reconcile | Unavailable | 请求可能已发出但结果未知；保留敞口，按原 ID 补查 | `order/binance_spot_gateway` |
| 13010 | `ORDER_VENUE_REJECTED` | Reject | FailedPrecondition | 交易所明确拒绝该订单 | 新增（目前在网关事件中） |
| 13011 | `ORDER_REPORT_INVALID` | Reconcile | InvalidArgument | 订单或成交回报缺少订单 ID、状态未知、字段非法、缺手续费资产 | `order/order_tracker`、`order/binance_spot_gateway` |
| 13012 | `ORDER_REPORT_CONFLICT` | Reconcile | FailedPrecondition | 回报与已跟踪订单冲突：账户或市场不符、交易所 ID 改变、终态冲突、成交超过委托量 | `order/order_tracker` |
| 13013 | `ORDER_REPORT_UNATTRIBUTED` | Reconcile | NotFound | 回报找不到所属订单 | `order/order_tracker` |
| 13014 | `CLIENT_ID_INVALID` | Reject | InvalidArgument | 客户端订单号编码或解码失败、格式不支持、字段溢出 | `order/binance_spot_gateway` |
| 13015 | `SIGNING_FAILED` | Halt | Internal | 签名输入非法或 HMAC 计算失败 | `order/binance_spot_gateway` |
| 13016 | `ORDER_RECOVERY_INVALID` | Reconcile | FailedPrecondition | 重启时恢复的历史订单非法或与当前状态冲突 | `order/binance_spot_gateway` |
| 13017 | `PAPER_BALANCE_INSUFFICIENT` | Reject | ResourceExhausted | 模拟盘可用余额不足 | `order/paper` |
| 13018 | `PAPER_INPUT_INVALID` | Reject | InvalidArgument | 模拟撮合收到的 BBO 或公开成交非法 | `order/paper` |

### 14000 风控与额度

| 码 | 名称 | 处理 | absl | 含义 | 现有来源 |
| --- | --- | --- | --- | --- | --- |
| 14001 | `RISK_EMERGENCY_STOPPED` | Reject | FailedPrecondition | 紧急停止中，拒绝所有新单 | `order/risk` |
| 14002 | `RISK_NOT_READY` | Reject | FailedPrecondition | 市场、账户或交易规则未就绪或已过期 | `order/risk` |
| 14003 | `RISK_LIMIT_EXCEEDED` | Reject | ResourceExhausted | 超过单笔上限或总敞口上限 | `order/risk` |
| 14004 | `RISK_LEASE_EXHAUSTED` | Reject | ResourceExhausted | 本分片资金租约额度不足 | `order/risk` |
| 14005 | `RISK_LEASE_MISSING` | Reject | FailedPrecondition | 缺少该账户/资产的资金租约或账户上限 | `order/risk` |
| 14006 | `RISK_LEASE_STALE` | Reject | FailedPrecondition | 租约版本或有效期过期，或账户未就绪无法续期 | `order/risk` |
| 14007 | `RISK_CONFIG_INVALID` | Halt | InvalidArgument | 账户上限、租约或风控设置非法、重复、属于其他分片 | `order/risk` |
| 14008 | `RISK_RESERVATION_INVALID` | Halt | FailedPrecondition | 预留状态不一致：未知预留、无法关联订单号、成交时预留增加 | `order/risk` |

### 15000 账户回报、对账与路由

| 码 | 名称 | 处理 | absl | 含义 | 现有来源 |
| --- | --- | --- | --- | --- | --- |
| 15001 | `ACCOUNT_MESSAGE_INVALID` | Reconcile | InvalidArgument | 私有回报报文非法：JSON、时间戳、订单身份、成交 ID、手续费资产、余额字段 | `order/binance_spot_account` |
| 15002 | `ACCOUNT_EVENT_UNSUPPORTED` | Reject | Unimplemented | 不支持的私有事件类型；记录后忽略 | `order/binance_spot_account` |
| 15003 | `RECONCILE_RESPONSE_INVALID` | Reconcile | InvalidArgument | 查单、查成交、查挂单的 REST 响应非法 | `order/binance_spot_account` |
| 15004 | `RECONCILE_IDENTITY_MISMATCH` | Reconcile | FailedPrecondition | 查询结果与目标订单身份不符，或同一订单号出现在两个市场 | `order/binance_spot_account` |
| 15005 | `RECONCILE_QUERY_FAILED` | Retry | Unavailable | 对账查询请求失败 | `order/binance_spot_account` |
| 15006 | `RECONCILE_RESPONSE_TOO_LARGE` | Reconcile | ResourceExhausted | 查询结果超过上限；缩小时间窗口后重查 | `order/binance_spot_account` |
| 15007 | `RECONCILE_TARGET_INVALID` | Halt | InvalidArgument | 对账目标、市场集合或时间窗口参数非法 | `order/binance_spot_account` |
| 15008 | `ROUTE_OWNER_UNKNOWN` | Reconcile | NotFound | 回报找不到所属 owner 或路由；隔离并补查 | `service/routing`、`RouteFailure::UnknownOwnership` |
| 15009 | `ROUTE_OWNERSHIP_CONFLICT` | Reconcile | FailedPrecondition | 订单号、交易所 ID、owner 与账户之间的归属冲突；隔离并补查 | `service/routing`、`RouteFailure::ConflictingOwnership` |
| 15010 | `ROUTE_REPORT_INVALID` | Reconcile | InvalidArgument | 回报身份字段非法或源序号耗尽 | `service/routing`、`RouteFailure::InvalidReport` |
| 15011 | `ROUTE_QUEUE_FULL` | Degrade | ResourceExhausted | 跨分片转发队列已满；暂停受影响账户新单并补查 | `RouteFailure::QueueFull` |
| 15012 | `ROUTE_CONFIG_INVALID` | Halt | InvalidArgument | owner 路由注册非法或重复 | `service/routing` |

### 16000 分片服务

| 码 | 名称 | 处理 | absl | 含义 | 现有来源 |
| --- | --- | --- | --- | --- | --- |
| 16001 | `SHARD_INPUT_INVALID` | Reject | InvalidArgument | 输入时间为负、回放输入顺序不递增、公开成交市场或方向非法 | `service/shard` |
| 16002 | `SHARD_REPORT_UNATTRIBUTED` | Halt | FailedPrecondition | 模拟盘回报缺少客户端订单号，属于装配缺陷 | `service/shard` |
| 16003 | `STRATEGY_CALLBACK_TIMEOUT` | Degrade | DeadlineExceeded | 策略回调超出时间预算；停止该策略新动作 | 新增 |
| 16004 | `STRATEGY_DEPENDENCY_NOT_READY` | Reject | FailedPrecondition | 策略依赖的市场、私有流或余额不可用，拒绝其新单 | 新增（目前分散在风控） |

### 17000 存储

| 码 | 名称 | 处理 | absl | 含义 | 现有来源 |
| --- | --- | --- | --- | --- | --- |
| 17001 | `STORAGE_OPTIONS_INVALID` | Halt | InvalidArgument | Recorder、HistoryReader 或恢复参数非法 | `offline/recorder`、`offline/history` |
| 17002 | `STORAGE_OPEN_FAILED` | Degrade | Unavailable | 无法打开数据库；继续交易并标记历史不完整 | `offline/history`、`offline/recorder` |
| 17003 | `STORAGE_WRITE_FAILED` | Degrade | Unavailable | SQLite 写入或提交失败（含测试注入故障）；标记缺口 | `offline/recorder` |
| 17004 | `STORAGE_QUEUE_FULL` | Degrade | ResourceExhausted | 记录队列已满，记录被丢弃；标记缺口 | `GapReason::QueueFull` |
| 17005 | `STORAGE_GAP_PERSIST_FAILED` | Degrade | Unavailable | 历史缺口本身无法持久化 | `offline/recorder` |
| 17006 | `STORAGE_QUERY_FAILED` | Reject | Internal | SQLite 查询执行失败 | `offline/history` |
| 17007 | `HISTORY_BUSY` | Reject | ResourceExhausted | 历史查询队列已满，返回 Busy | `offline/history` |
| 17008 | `HISTORY_STOPPING` | Reject | FailedPrecondition | HistoryReader 正在停止 | `offline/history` |
| 17009 | `HISTORY_DEADLINE` | Reject | DeadlineExceeded | 历史或缺口查询超时、被中断 | `offline/history` |
| 17010 | `HISTORY_CURSOR_INVALID` | Reject | InvalidArgument | 分页游标为空或非法 | `offline/history` |
| 17011 | `HISTORY_TOO_MANY_GAPS` | Reject | ResourceExhausted | 缺口数量超过单次可报告的上限 | `offline/history` |
| 17012 | `RECOVERY_MANIFEST_NOT_FOUND` | Reconcile | NotFound | 没有上次运行的 manifest（首次运行或上次未写入）；全量对账 | `offline/history` |
| 17013 | `RECOVERY_DATA_CORRUPTED` | Reconcile | DataLoss | manifest、提交序号、缺口记录或恢复记录损坏、不一致 | `offline/history`、`GapReason::CrashTail` |
| 17014 | `RECORD_ENCODING_INVALID` | Reconcile | DataLoss | 记录序列化或反序列化失败、字段非法、尾部多余字节 | `offline/record_codec` |

`GapReason::WriteFailed` 对应 `STORAGE_WRITE_FAILED`，`GapReason::Unknown` 对应 `INTERNAL`。

### 18000 配置与进程

| 码 | 名称 | 处理 | absl | 含义 | 现有来源 |
| --- | --- | --- | --- | --- | --- |
| 18001 | `CONFIG_FILE_NOT_FOUND` | Halt | NotFound | 配置文件不存在 | `application/config` |
| 18002 | `CONFIG_SYNTAX_INVALID` | Halt | InvalidArgument | YAML 无法解析，或根节点不是 map | `application/config` |
| 18003 | `CONFIG_SCHEMA_UNSUPPORTED` | Halt | InvalidArgument | `schema_version` 不受支持 | `application/config` |
| 18004 | `CONFIG_FIELD_INVALID` | Halt | InvalidArgument | 字段缺失、类型错误或取值越界：标量、整数、小数、时间间隔、价差、余额、`scale_version`、`loop_mode`、行情来源 | `application/config` |
| 18005 | `CONFIG_REFERENCE_INVALID` | Halt | InvalidArgument | 引用不存在或重复：账户、市场、策略 owner key、分片、市场依赖 | `application/config` |
| 18006 | `CONFIG_ASSIGNMENT_INVALID` | Halt | InvalidArgument | 分片分配违反规则：分片号越界、空分片、市场重复分配、策略依赖跨分片 | `application/config` |
| 18007 | `CONFIG_MODE_NOT_ALLOWED` | Halt | FailedPrecondition | 模式未显式声明为 paper，或 live 尚未开放 | `application/config` |
| 18008 | `CONFIG_CREDENTIALS_INLINE` | Halt | InvalidArgument | 配置文件中直接写了凭据 | `application/config` |
| 18009 | `CONFIG_LEASE_INVALID` | Halt | InvalidArgument | 静态资金或限速租约配置非法 | `application/config` |
| 18010 | `LAUNCH_UNSUPPORTED_TOPOLOGY` | Halt | FailedPrecondition | 当前启动模式不支持该配置组合（例如公开行情 Paper 只支持单分片单市场） | `application/launcher` |
| 18011 | `STATE_DIR_UNAVAILABLE` | Halt | 按系统错误映射 | 状态目录或存储目录无法创建或访问；消息带路径和系统错误 | `application/launcher` |

### 19000 控制与命令行

| 码 | 名称 | 处理 | absl | 含义 | 现有来源 |
| --- | --- | --- | --- | --- | --- |
| 19001 | `CONTROL_MESSAGE_INVALID` | Reject | InvalidArgument | 控制帧、JSON、版本、请求或响应类型非法 | `application/control` |
| 19002 | `CONTROL_SOCKET_FAILED` | Halt | Unavailable | 控制 socket 创建、绑定或监听失败 | `application/control` |
| 19003 | `CONTROL_SOCKET_IN_USE` | Halt | AlreadyExists | 控制 socket 已被占用，可能已有引擎在运行 | `application/control` |
| 19004 | `CLI_USAGE_INVALID` | Reject | InvalidArgument | 命令或参数错误：未知命令、缺值、`state_dir` 缺失、socket 路径过长、history limit 越界 | `cli` |
| 19005 | `CLI_ENGINE_UNREACHABLE` | Retry | Unavailable | 无法连接引擎，或连接在收到响应前关闭 | `cli` |
| 19006 | `CLI_RESPONSE_INVALID` | Reject | DataLoss | 引擎响应帧过大、头部不匹配或类型不符 | `cli` |
| 19007 | `CLI_COMMAND_FAILED` | Reject | FailedPrecondition | 引擎返回错误或拒绝 stop；消息带引擎给出的错误码名 | `cli` |
| 19008 | `CONTROL_BUSY` | Retry | ResourceExhausted | 控制请求并发额度用尽；退避后重试 | `application/control` |
| 19009 | `CONTROL_TIMEOUT` | Retry | DeadlineExceeded | 控制请求等待或执行超时；退避后重试 | `application/control` |

## 6. 现有枚举与字符串的归并

| 现有 | 处理 |
| --- | --- |
| `BookApplyReason` | `Gap`、`CrossedBook`、`InvalidScale`、`InvalidMessage`、`BufferOverflow`、`Disconnected` 改为返回 `ErrorCode`；`None`、`OldDiff` 保留为结果状态 |
| `RateDecision` | `Granted` 之外的值改为 `ErrorCode`：`RATE_LEASE_MISSING`、`RATE_LEASE_EXHAUSTED`、`RATE_BREAKER_OPEN`、`RATE_CONFIG_INVALID` |
| `RouteFailure` | 改为 `ErrorCode`（150xx）；`OrderOwnershipIndex::Resolve` 直接返回业务码，不再从 absl 码反推 |
| `GapReason` | 持久化字段改存 `ErrorCode`；旧值按第 5 节 17000 段映射 |
| `DispatchResult::reason`（字符串） | 改为 `ErrorCode`，消息另存；拒单统计按码计数 |
| `ControlError{"bad_request" / "internal"}` | 改为携带 `ErrorCode`，线上格式输出数字和名字 |
| `binance_spot_account` 的 `unresolved_reason`、launcher 的 `stream_error`（字符串） | 改为保存 `absl::Status`，展示时输出码名与消息 |

## 7. 落地步骤

1. 实现 `base/error.{h,cc}` 和 `test/error_test.cc`：表的完整性（每个码唯一、名字唯一、都有 `Info`）、payload 往返、无业务码的 Status 读取为 `INTERNAL`、`ErrorFromSystem` 的映射。
2. 先改有按错误码分支的三处，修复行情流问题：
   - `market/binance_spot_feed`：`Run()` 按 `RecoveryOf` 分支，`Halt` 停止、`Resync` 重新同步并计数、`Retry` 退避重连；补"损坏报文后恢复"和"配置错误停止"两个测试。
   - `service/routing`：按第 6 节改为业务码。
   - `service/dispatcher`：`IsNotFound` 改为判断 `ORDER_NOT_FOUND`。
3. 按模块替换其余错误构造，顺序同目录重构：`base` → `market` → `order` → `strategy` → `offline` → `service` → `application` → `cli`。每个模块完成后跑 `bazel test //...`。
4. 归并第 6 节的枚举与字符串；控制协议和 SQLite schema 中的原因字段改存错误码，schema 版本加一。
5. CI 增加检查：`hquant/src` 中除 `base/error.cc` 外不出现 `absl::[A-Za-z]+Error(`。
6. 更新文档：`DEVELOPMENT.md` 第 1 节"错误与容器"改为引用本文；`STRUCTURE_AND_TYPES.md` 补 `base/error.h`。

## 8. 持久化与协议兼容

- SQLite schema 和记录二进制格式升为 v2。新 `HistoryGap.reason`、`DecisionRecord.reason` 保存业务码数字；决策文字单独保存在 `message`，成功决策的原因码为 `0`。旧 v1 数据仍可读取；Recorder 打开旧数据库时迁移 0–3 的缺口原因枚举值。
- 控制协议升为 v2，错误帧含 `code` 数字、`name` 大写名和 `message`。v2 解码拒绝未知编号及编号与名称不符。服务端仍接收 v1 请求并以 v1 格式回复；新客户端可读取 v1 响应。
- CI 执行 `dev/check_error_construction.py`，阻止 `hquant/src` 与 `apps` 新增直接 `absl::XxxError(...)` 构造。
