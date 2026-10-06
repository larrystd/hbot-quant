# 三组 1000 股票行情回放：正确性与性能摸底（2026-10-06）

数据定义见 [MARKET_CASES.md](MARKET_CASES.md)，本次原始探针结果及 SQLite 核对见 [market_case_results_2026-10-06.json](market_case_results_2026-10-06.json)。运行环境沿用 [此前性能报告](PERFORMANCE_2026-10-06.md) 的 Apple M2 MacBook Air、24 GB 内存和 macOS 26.1；Bazel 使用 `--config=release`。以下性能数据每组只有 **1 次测量**。

## 数据与端到端正确性

| Case | 时间戳跨度 | 股票 | 每条盘口深度 | 盘口快照 | 逐笔成交 | 总输入 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `nyse` | 3 分钟 | 1000 | 买 10＋卖 10 | 180,000 | 18,000 | 199,000 |
| `sse` | 3 分钟 | 1000 | 买 10＋卖 10 | 60,000 | 6,000 | 67,000 |
| `stress` | 30 秒 | 1000 | 买 10＋卖 10 | 240,000 | 60,000 | 301,000 |

独立校验器逐条核对三个文件的时间、股票、20 档排序和数量、快照序号、成交 ID、成交价、成交数量、每只股票的消息节奏以及最终买卖一；三组全部通过。随后用带探针的真实 `Runtime → Shard → Market → PaperExchange → Strategy → Executor → SQLite` 路径完整回放。`Shard` 已处理输入数分别等于 199,000、67,000、301,000。SQLite 分别写入 166,000、166,000、26,000 条历史记录，均覆盖 1000 个 Shard（最大 ID 为 999）；每个 Shard 的历史序号从 1 连续到记录数，`history_gaps` 均为 0。黑盒套件的 7 个端到端测试在 Release 及 ASan/UBSan 构建下都通过，包括新增的连续完整快照触发重新报价用例。

## 吞吐与延迟

未限速回放按每条输入处理完成后立即发送下一条。`回放 QPS` 是该基准进程从第二次读取文件到所有输入处理完毕的输入数除以壁钟时间，包含 JSON 读取、解析和回调等待，不包含初始化及最后 SQLite 刷盘。`回调延迟` 从投递到 Shard 和账户事件处理完毕，不保证对应历史已经提交。

| Case | 回放耗时 | 回放 QPS | 回调 p50 / p99 | SQLite 历史记录 | 历史队列峰值 |
| --- | ---: | ---: | ---: | ---: | ---: |
| `nyse` | 4.027 s | 49,414/s | 18.17 / 27.46 µs | 166,000 | 8192/8192 |
| `sse` | 1.403 s | 47,758/s | 18.25 / 29.75 µs | 166,000 | 8192/8192 |
| `stress` | 5.947 s | 50,618/s | 18.13 / 26.63 µs | 26,000 | 6338/8192 |

另以 `--pace` 按消息的 `at_us` 时间戳投递 30 秒压力 case：301,000 条输入用 **30.140 s** 处理完成，平均 **9987 输入/s**（包含时间 0 的 1000 条订阅）；全部写入 26,000 条历史记录，历史序号无缺口。回调 p50/p99 为 **20.00/44.71 µs**。计划时间到实际投递的迟到 p50/p99 为 **29.9/174.0 µs**，p999 为 13.8 ms、最大 18.0 ms；短暂迟到主要出现在集中到达和系统调度时，未形成持续积压。此处投递器仍是串行等待确认的本地文件驱动，并非网络接收。

## 分段与瓶颈

以下为压力 case 未限速回放的分段 p50/p99（µs）：

| 阶段 | p50 | p99 |
| --- | ---: | ---: |
| Post→Shard | 2.46 | 6.29 |
| Shard 输入处理（含十档盘口） | 1.54 | 3.08 |
| 处理结束→完成回调 | 12.46 | 17.00 |
| 完成回调→回放线程醒来 | 1.71 | 4.42 |

在这组机器和串行回放驱动下，**完成回调的再次调度与逐条等待**占回调延迟的大部分；十档盘口本身的 Shard 处理时间较小。JSON 单独读取解析 301,000 条约 0.238 s，也不是主要阶段。纽约和上交所 case 把 3 分钟输入压缩进 4.0/1.4 秒，SQLite 队列触顶且采用回放背压，历史输入到提交的 p99 约 75–76 ms；这不能代表按真实 3 分钟节奏投递时的落盘延迟。按时间戳投递的压力 case 历史队列峰值为 5679，输入到提交 p99 为 55.2 ms。

## 复现与边界

```sh
python3 hquant/bench/make_market_cases.py /tmp/hquant-market-cases --case all
python3 hquant/bench/validate_market_cases.py /tmp/hquant-market-cases/nyse
python3 hquant/bench/validate_market_cases.py /tmp/hquant-market-cases/sse
python3 hquant/bench/validate_market_cases.py /tmp/hquant-market-cases/stress
bazelisk --batch build --config=release //hquant/src:hquant_server //hquant/bench:replay_bench
mkdir -p /tmp/hquant-market-cases/stress/state
bazel-bin/hquant/bench/replay_bench /tmp/hquant-market-cases/stress/config.yaml /tmp/hquant-market-cases/stress/state --pace
```

数据是确定性合成盘口和公开成交，没有交易所网络协议、私有订单回报或真实下单。市场 case 只跑了未限速回放；它们的计划输入速率低于已按时间戳跑通的压力 case，但不能据此把本地测量当作生产容量保证。macOS 没有 Linux `perf`，本次用逐阶段计时探针定位瓶颈；本报告没有新的调用栈采样。在当前受限 shell 中，Bazel 已输出 `Build completed successfully` 后会在退出阶段因 `sysctl` 被沙箱拒绝而返回 37；生成的二进制已直接运行并完成上述测试。
