# 开发辅助目录

运行入口和全部业务代码位于 [`hquant/src`](../hquant/src)。`//dev:sanitizer_smoke` 用于检查 sanitizer 工具链；Linux CI 执行 `bazel build //...`、`bazel test //...` 和 sanitizer smoke。当前运行架构见 [`docs/README.md`](../docs/README.md)。

[`VALIDATION_2026-09-27.md`](VALIDATION_2026-09-27.md) 是重设计前的历史验收记录，不能用来判断当前实现的通过状态。
