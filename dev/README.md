# 开发辅助目录

`dev/` 放开发期使用的 Bazel smoke target、协议调试工具和本地验证脚本。这里的程序不进入正式运行链路。

开发关口与进度见 [路线图](../docs/ROADMAP.md)；写法、协作与任务排程见 [开发指南](../docs/DEVELOPMENT.md)；文档总览见 [docs/README.md](../docs/README.md)。

- 交易核心、连接器和策略实现放在 `hquant/src/`，可执行程序放在 `apps/`。
- 单元与集成测试统一放在 `hquant/test/`；可复用的测试夹具放在 `hquant/test/fixtures/`。
- `//dev:dependency_smoke` 已实际链接 Abseil、Quill、Asio/Beast、OpenSSL、libmpdec、simdjson、yaml-cpp、SQLite 和 CLI11，并纳入 `bazel test //...`；macOS 已编译运行通过。
- Linux x86_64 全量构建、测试和 sanitizer smoke 由 [GitHub Actions](../.github/workflows/linux-bazel.yml) 执行；当前验证状态见 [LINUX_VALIDATION.md](LINUX_VALIDATION.md)。
