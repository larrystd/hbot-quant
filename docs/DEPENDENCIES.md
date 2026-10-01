# C++ 依赖、构建与使用边界

状态：选型已定，版本已按 Bazel Central Registry（BCR）核对（2026-09-27）；macOS arm64 上 `//dev:dependency_smoke` 已实际链接全部首批依赖并通过，Linux x86_64 待 CI 验证。依赖版本以 [`MODULE.bazel`](../MODULE.bazel) 与 `MODULE.bazel.lock` 为准，本文说明选型理由与使用边界。线程模型见 [ARCHITECTURE.md](ARCHITECTURE.md)，目录与 target 见 [STRUCTURE_AND_TYPES.md](STRUCTURE_AND_TYPES.md)。

## 1. 决定一览

| 领域 | 选型 | 版本（BCR 模块） | 理由 |
| --- | --- | --- | --- |
| 构建 | Bazel + Bzlmod | Bazel `9.2.0`；`rules_cc 0.2.25`、`platforms 1.1.0`、`bazel_skylib 1.9.2` | 可重现、增量、按 target 管理依赖；Sanitizer 用 `--config` 切换 |
| 语言 | C++20 | macOS：Apple Clang 17；Linux：CI 工具链 | 协程 + `std::chrono`；不依赖 C++23 |
| 基础库 | Abseil | `abseil-cpp 20260817.0`（LTS） | 容器、`Status/StatusOr`、字符串、同步原语 |
| 日志 | Quill | `quill 13.0.0` | 热路径只做二进制入队，格式化与 I/O 在后台线程 |
| 测试 | GoogleTest / GoogleMock | `googletest 1.18.0.bcr.1` | 单元、协议 mock、重放；`dev_dependency` |
| 基准 | Google Benchmark | `google_benchmark 1.9.5` | Decimal、订单簿、JSON 解析基线；`dev_dependency` |
| 十进制 | libmpdec（mpdecimal） | `4.0.1`，BCR 无模块，自带 BUILD | 与 CPython `decimal` 同一实现，差分重放可逐位一致 |
| 异步网络 | Boost.Asio + Boost.Beast | `boost.asio` / `boost.beast 1.90.0.bcr.1` | TCP/DNS/TLS/定时器/C++20 协程、HTTP/1.1、WebSocket 一套运行时 |
| TLS/加密 | OpenSSL 3.5 LTS | `openssl 3.5.8.bcr.0` | TLS、HMAC、Ed25519、PBKDF2、AES-CTR、Keccak-256；LTS 支持到 2030 |
| 压缩 | zlib | `zlib 1.3.2` | gzip/deflate REST 响应和 WS 二进制帧 |
| 入站 JSON | simdjson | `simdjson 4.6.9` | On-Demand 解析，可取数字原始文本 |
| 出站 JSON | 自研 `JsonWriter` | — | 体量小；按原文输出 Decimal，签名覆盖最终字节 |
| 配置 | yaml-cpp | `yaml-cpp 0.9.0.bcr.1` | 兼容现有 YAML；只在配置边界使用 |
| 存储 | SQLite C API | `sqlite3 3.53.4` | 意图、检查点、订单/成交历史；项目内 RAII 封装 |
| 命令行 | CLI11 | `cli11 2.6.2` | `start/status/stop/history` 子命令 |

**明确不用：** CMake/vcpkg（由 Bazel 取代）、Boost.Multiprecision（由 libmpdec 取代）、nlohmann/json（入站 simdjson，出站自研）、Abseil Logging 与 Abseil Flags（分别由 Quill、CLI11 负责）、BoringSSL（无稳定版本号且缺 Keccak）。

## 2. 构建：Bazel

### 2.1 约定

- `.bazelversion` 固定 `9.2.0`，统一用 bazelisk；只用 Bzlmod，不建 `WORKSPACE`。常用操作封装在根目录 `op.sh`（`build`、`test`、`ci`、`asan`、`tsan`、`fmt-check`、`compdb`、`doctor`）。
- C++ 规则一律 `load("@rules_cc//cc:defs.bzl", ...)`。
- 提交 `MODULE.bazel.lock`；依赖升级单独进行：改版本 → 全量构建 → 回归测试 → 更新本表。
- 第三方库只通过 `@module//:target` 引入；项目代码不直接写第三方头文件路径。模块依赖方向靠 `visibility` 强制（见 [STRUCTURE_AND_TYPES.md](STRUCTURE_AND_TYPES.md) 第 2.1 节）。
- libmpdec 不在 BCR：`MODULE.bazel` 用 `http_archive` 固定 sha256 下载，BUILD 文件在 `third_party/mpdecimal/mpdecimal.BUILD`。
- macOS 最低部署目标 10.15（Quill 13 使用的 `std::filesystem` 在更早版本不可用）。
- 项目代码的告警等级写在项目自己的 `copts` 里，只作用于 `//hquant/...`，不作用于第三方库。
- IDE 的 `compile_commands.json` 由 `tools/compdb.py`（`./op.sh compdb`）从 Bazel 的 `CppCompile` 动作生成。

### 2.2 `.bazelrc`

```text
common --enable_bzlmod
build  --cxxopt=-std=c++20 --host_cxxopt=-std=c++20
build  --macos_minimum_os=10.15
build  --@boost.asio//:ssl=openssl           # Asio 的 SSL 后端，默认是 no_ssl
build  --copt=-fno-omit-frame-pointer
test   --test_output=errors

build:asan  --copt=-fsanitize=address,undefined --linkopt=-fsanitize=address,undefined
build:asan  --copt=-fno-sanitize-recover=all
build:tsan  --copt=-fsanitize=thread --linkopt=-fsanitize=thread
build:release -c opt --copt=-g1

try-import %workspace%/user.bazelrc
```

## 3. 各依赖的使用边界

### 3.1 Abseil

| 使用 | 不使用 |
| --- | --- |
| `absl::flat_hash_map/set`（身份索引）、`node_hash_map`（需指针稳定时）、`absl::btree_map/set`（有序，订单簿远端价位）、`absl::InlinedVector` | Abseil Logging、Abseil Flags |
| `absl::Status`、`absl::StatusOr<T>`：可恢复错误 | 用异常表达业务错误 |
| `absl::StrCat`、`StrFormat`、`StrSplit` | Abseil 时间类型（领域时间用 `std::chrono`） |
| `absl::Mutex`、`CondVar`：只用于确需同步的跨线程队列或等待器 | 在分片状态机内部加锁 |
| `absl::AnyInvocable`、`absl::FunctionRef`、`absl::Cleanup` | — |

1. `flat_hash_*` 的遍历顺序每个进程都不同，**不得**决定策略动作、订单提交或序列化的顺序；需要确定顺序时用 `btree_map`、输入序号或显式排序。
2. Abseil 类型不越过进程边界：SQLite、JSON、YAML、控制协议里都不出现 Abseil 对象。
3. 业务代码不抛异常；第三方库（yaml-cpp、Beast 部分接口）抛出的异常在边界层转换为 `Status`。

### 3.2 Quill 日志

- 统一入口封装 Quill 的 Frontend/Backend 初始化；业务代码使用项目宏，不直接查找 Quill logger。
- 各分片线程是前端生产者，Quill 后台线程负责格式化与写 sink。Frontend 队列首版统一用有界丢弃队列并统计丢弃量，日志反压不能阻塞分片。行情调试日志在入队前按级别过滤。
- 订单、成交、余额等业务事实由 SQLite Recorder 尽力记录；Quill 日志不承担交易审计或恢复职责。
- 默认一行一条，带 `connector`、`market`、`client_order_id`、`exchange_order_id`、`seq` 等字段；另配 JSON sink 供机器解析。
- `Decimal`、强类型 ID、枚举通过 codec/formatter 特化输出。凭据类型 `Secret` 不提供 formatter，只有认证/签名边界可访问明文字节，错误路径做脱敏测试。
- 用 rotating file sink 按大小和日期轮转；退出时 flush 后再停后台线程。

### 3.3 GoogleTest

- 单元测试与跨模块链路测试统一放在 `hquant/test/`，夹具放在 `hquant/test/fixtures/`，以 `data` 属性引入。
- 协议测试用 GoogleMock 模拟传输端口；集成测试用 Beast 起本地 HTTP/WS mock 服务器，不访问外网。
- 差分重放夹具由 Python 基线离线生成并入库（已脱敏）；C++ 测试只读这些文件，不在测试中运行 Python。
- CI：`bazel test //...`，另跑 `--config=asan` 和 `--config=tsan`。

### 3.4 libmpdec

- C++ `Decimal` 封装 C API，状态标志转为 `absl::Status`；默认上下文与 Python 一致（详见 [STRUCTURE_AND_TYPES.md](STRUCTURE_AND_TYPES.md) 第 7.1 节）。
- `mpdecimal.BUILD` 用 `genrule` 生成 `mpdecimal.h`（替换 `@MPD_HEADER_CONFIG@`），采用 `CONFIG_64 + ANSI`，同一份代码覆盖 arm64 与 x86_64。许可证 BSD-2，一并收录 `COPYRIGHT.txt`。
- 订单簿不在热路径做 Decimal 运算：adapter 按行情 `TickLotSize` 把价格/数量精确转成 `int64` ticks/lots，不能整除或越界的报文是协议错误。

### 3.5 网络：Asio + Beast + OpenSSL + zlib

- **协程与线程：** 每个分片一个 `io_context`，该分片的公开 WS、私有 WS、REST 连接池、快照、重连、轮询和策略定时器都 `co_spawn` 在它上面；**不建独立的网络线程池**。`co_await` 挂起协程而不占线程，恢复后仍在同一分片线程执行。控制线程另有自己的 `io_context` 处理 Unix socket。参考 [Boost.Asio C++20 协程](https://www.boost.org/latest/doc/html/boost_asio/overview/composition/cpp20_coroutines.html)。
- **REST：** `//hquant/src/base:net` 实现 HTTP 客户端，按 origin 与 TLS 配置维护有上限的 HTTP/1.1 keep-alive 连接池；单连接同一时间只执行一个请求。期限、取消、重试和限速规则见 [STRUCTURE_AND_TYPES.md](STRUCTURE_AND_TYPES.md) 第 7.3 节与 [ARCHITECTURE.md](ARCHITECTURE.md) 第 5.5 节。
- **WebSocket：** Beast 负责 HTTP/1.1 和 WebSocket（含 permessage-deflate）；gzip/deflate 的 REST 响应体和 WS 二进制帧用 zlib 解压。
- **OpenSSL 3.5 的用途：**
  - TLS：校验证书链和主机名（`asio::ssl::host_name_verification`），设置 SNI（`SSL_set_tlsext_host_name`）。
  - 交易所签名：HMAC-SHA256/512、Ed25519。
  - 凭据迁移：Python 默认写出 PBKDF2-SHA256 + AES-128-CTR + Keccak-256 的以太坊 keyfile v3，源实现也能生成 scrypt KDF。文件是十六进制包裹的 JSON，按 `kdf` 字段选择算法；AES-CTR 的 IV 须左侧补零到 16 字节。Keccak-256 需 OpenSSL ≥ 3.2。
- **CA 证书：** Bazel 构建的 OpenSSL 读不了 macOS 钥匙串，也不能假定默认 `OPENSSLDIR`。配置项 `tls.ca_file` 必须显式给出，或使用随程序发布、固定哈希的 Mozilla CA 包；Linux 可配置为系统路径。证书校验失败时绝不发送带凭据的请求。
- HTTP/2、链上签名（secp256k1，BCR 无模块）、FIX 等需要时在传输端口或 adapter 之后另加实现，不改动交易核心。

### 3.6 JSON

- **入站：** 交易所 REST/WS 报文一律用 simdjson On-Demand 解析；价格、数量、费用取原始文本交给 `Decimal::Parse`，绝不经过 `double`。每个 socket/解析任务持有自己的 `ondemand::parser`，按要求预留缓冲区尾部 padding。
- **出站：** `JsonWriter` 负责字符串转义，并按交易所格式把 Decimal 写成数字或字符串；签名覆盖 writer 产出的最终字节。
- 测试夹具和 CLI 的 JSON 输出同样用 simdjson 读、`JsonWriter` 写，不引入第二个 JSON 库。

### 3.7 yaml-cpp、SQLite、CLI11

- **yaml-cpp：** 只在 `//hquant/src/application:config` 使用。数值先以标量文本读出，再转换为强类型 `AppConfig`；缺字段或非法值返回 `Status`。配置带 `schema_version`。
- **SQLite：**
  - C API + RAII 封装（连接、语句、事务），WAL + `PRAGMA synchronous=NORMAL`。
  - **写：** 只有 Recorder 线程持有写连接；每个分片一条有界 SPSC 队列，Recorder 轮转消费、批量提交。分片 `try_push` 失败或后台事务失败都只标记历史缺口，不阻止已通过风控的订单。
  - **读：** HistoryReader 线程持有独立只读连接，处理分页 `history` 与启动恢复查询，限制行数与耗时并及时结束读事务（长读事务会阻碍 WAL checkpoint）。控制线程不执行 SQL。
  - 交易数值存十进制字符串；schema 带版本，建表脚本在 `hquant/src/storage/schema.sql`。Python 的 `SqliteDecimal(6)` 会截断精度，所以不直接读写 Python 数据库，只提供一次性导入工具。
- **CLI11：** 只在 `//hquant/src/cli:cli` 使用。

## 4. 仍需实测的项目

1. Linux x86_64 上 `bazel build //...`、`bazel test //...`，以及 ASan/UBSan、TSan（见 [`dev/LINUX_VALIDATION.md`](../dev/LINUX_VALIDATION.md)）。
2. `--@boost.asio//:ssl=openssl` 与 `openssl 3.5.8` 在 Linux 上解析与链接（boost.asio 模块自身声明 `openssl 3.5.4.bcr.1`，MVS 选 3.5.8）。
3. Decimal 与 Python 的逐位一致性，以基线 CPython 内置的 libmpdec 版本为准。
4. 用 Python 生成的 PBKDF2、scrypt 与前导零 IV keyfile v3 夹具验证凭据解密。
5. 基准：Quill 在分片线程上的入队延迟与丢弃计数、simdjson 解析深度消息耗时、Recorder 入队 p99 与 8 分片同时写入时的积压。

## 5. 后续按需引入的依赖

链上签名（secp256k1）、HTTP/2、FIX、Parquet（回测历史数据；首版回测只读 CSV）等，由具体连接器或阶段声明为可选依赖，不进入首批现货模拟盘构建。引入门槛：许可证、BCR 是否有模块（没有则固定哈希并自带 BUILD）、macOS/Linux 都支持、维护活跃、有故障恢复方案。
