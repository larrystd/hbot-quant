#pragma once

#include <cstdint>
#include <span>
#include <string>
#include <string_view>

#include "absl/status/statusor.h"
#include "application/config.h"
#include "application/quant_server.h"

namespace hquant {

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
absl::StatusOr<AppConfig> PrepareStart(const CliOptions& options);
absl::StatusOr<ServerRequest> MakeServerRequest(const CliOptions& options,
                                                uint64_t request_id);
std::string ServerSocketPath(std::string_view state_dir);
absl::StatusOr<ServerResponse> SendServerRequest(const std::string& state_dir,
                                                 const ServerRequest& request);
absl::StatusOr<std::string> FormatServerResponse(const CliOptions& options,
                                                 const ServerResponse& response,
                                                 uint64_t request_id);

}  // namespace hquant
