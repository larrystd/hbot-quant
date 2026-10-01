#pragma once

#include <cstdint>
#include <span>
#include <string>
#include <string_view>

#include "absl/status/statusor.h"

namespace hquant {

struct ControlBenchOptions {
  enum class Mode { Closed, Open } mode = Mode::Closed;
  std::string state_dir;
  uint32_t connections = 16;
  uint32_t threads = 1;
  uint32_t duration_seconds = 10;
  uint32_t warmup_seconds = 0;
  uint32_t rate = 0;
  uint32_t status_percent = 80;
  bool json = false;
};

absl::StatusOr<ControlBenchOptions> ParseControlBenchArguments(
    std::span<const std::string_view> arguments);

// Runs the load and returns its table or one-line JSON report.
absl::StatusOr<std::string> RunControlBench(const ControlBenchOptions& options);

}  // namespace hquant
