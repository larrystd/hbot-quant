#pragma once

#include <cstdint>
#include <span>
#include <string>
#include <string_view>

#include "absl/status/statusor.h"
#include "hbot/app/config/config.h"
#include "hbot/control/protocol.h"

namespace hbot {

enum class CliVerb { Start, Status, History, Stop };

struct CliOptions {
  CliVerb verb = CliVerb::Status;
  std::string state_dir;
  std::string config_path;
  uint32_t history_limit = 20;
  std::string history_cursor;
};

absl::StatusOr<CliOptions> ParseCliArguments(
    std::span<const std::string_view> arguments);
absl::StatusOr<EngineConfig> PrepareStart(const CliOptions& options);
absl::StatusOr<ControlRequest> MakeControlRequest(const CliOptions& options,
                                                  uint64_t request_id);
std::string ControlSocketPath(std::string_view state_dir);
absl::StatusOr<ControlResponse> SendControlRequest(
    const std::string& state_dir, const ControlRequest& request);
absl::StatusOr<std::string> FormatControlResponse(
    const CliOptions& options, const ControlResponse& response,
    uint64_t request_id);

}  // namespace hbot
