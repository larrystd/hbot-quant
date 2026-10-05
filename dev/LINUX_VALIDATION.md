# Linux x86_64 CI 验证

记录日期：2026-09-27。此工作区所在主机是 macOS arm64，未安装可运行 Linux x86_64 二进制的容器或虚拟机。以下 Linux 结果均为**未验证**，不能当作通过记录。首次 GitHub Actions 运行后，应补入运行链接、镜像及工具链版本、结果和失败日志。

| 检查项 | Bazel 命令 | 当前结果 |
| --- | --- | --- |
| Linux x86_64 全依赖及业务目标构建 | `bazel build --jobs=2 //...` | 未验证；等待 CI |
| Linux x86_64 全部测试 | `bazel test --jobs=2 //...` | 未验证；等待 CI |
| AddressSanitizer + UndefinedBehaviorSanitizer 工具链和运行时 | `bazel test --jobs=2 --config=asan //dev:sanitizer_smoke` | 未验证；等待 CI |
| ThreadSanitizer 工具链和运行时 | `bazel test --jobs=2 --config=tsan //dev:sanitizer_smoke` | 未验证；等待 CI |

CI 工作流位于 [linux-bazel.yml](../.github/workflows/linux-bazel.yml)，使用 `ubuntu-24.04` x86_64 runner 和仓库 `.bazelversion` 指定的 Bazel 9.2.0。Sanitizer 检查只证明配置、链接和运行时对一个并发 C++ 测试可用；业务目标的 sanitizer 结果仍须单独验证，例如：

当前 CI 只对 `//dev:sanitizer_smoke` 运行 sanitizer；业务目标的 sanitizer 验证尚未执行。
