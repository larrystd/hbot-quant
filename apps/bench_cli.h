#pragma once

#include <cstdint>
#include <span>
#include <string>
#include <string_view>

#include "absl/status/statusor.h"
#include "application/control_server.h"
#include "boost/asio/awaitable.hpp"
#include "boost/asio/io_context.hpp"

namespace hquant {

enum class CliVerb { Status, History, Stop };

struct CliOptions {
  CliVerb verb = CliVerb::Status;
  std::string state_dir;
  uint32_t history_limit = 20;
  std::string history_cursor;
};

absl::StatusOr<CliOptions> ParseCliArguments(
    std::span<const std::string_view> arguments);
absl::StatusOr<ControlRequest> MakeControlRequest(const CliOptions& options,
                                                  uint64_t request_id);
std::string ControlSocketPath(std::string_view state_dir);
boost::asio::awaitable<absl::StatusOr<ControlResponse>> ExchangeControlRequest(
    boost::asio::io_context& io, std::string socket_path,
    const ControlRequest& request);
absl::StatusOr<ControlResponse> SendControlRequest(
    const std::string& state_dir, const ControlRequest& request);
absl::StatusOr<std::string> FormatControlResponse(
    const CliOptions& options, const ControlResponse& response,
    uint64_t request_id);

}  // namespace hquant
