#include "apps/bench_cli.h"

#include <sys/un.h>

#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <variant>

#include "absl/status/status.h"
#include "application/control_server.h"
#include "base/error.h"
#include "base/line_stream.h"
#include "boost/asio/co_spawn.hpp"
#include "boost/asio/use_future.hpp"

namespace hquant {
namespace {

absl::StatusOr<uint32_t> OrderHistoryLimit(std::string_view text) {
  uint32_t number = 0;
  auto [end, error] =
      std::from_chars(text.data(), text.data() + text.size(), number);
  if (error != std::errc{} || end != text.data() + text.size() || number == 0 ||
      number > 500) {
    return Error(ErrorCode::kCliUsageInvalid,
                 "history limit must be in [1,500]");
  }
  return number;
}

}  // namespace

absl::StatusOr<CliOptions> ParseCliArguments(
    std::span<const std::string_view> arguments) {
  if (arguments.empty())
    return Error(ErrorCode::kCliUsageInvalid, "missing command");
  CliOptions options;
  if (arguments[0] == "status")
    options.verb = CliVerb::Status;
  else if (arguments[0] == "order-history")
    options.verb = CliVerb::OrderHistory;
  else if (arguments[0] == "stop")
    options.verb = CliVerb::Stop;
  else
    return Error(ErrorCode::kCliUsageInvalid, "unknown command");

  bool state_seen = false, limit_seen = false, cursor_seen = false;
  for (size_t index = 1; index < arguments.size(); index += 2) {
    if (index + 1 >= arguments.size()) {
      return Error(ErrorCode::kCliUsageInvalid, "option needs value");
    }
    const auto key = arguments[index];
    const auto value = arguments[index + 1];
    if (value.empty())
      return Error(ErrorCode::kCliUsageInvalid, "empty option value");
    if (key == "--state-dir" && !state_seen) {
      options.state_dir = std::string(value);
      state_seen = true;
    } else if (key == "--limit" && !limit_seen &&
               options.verb == CliVerb::OrderHistory) {
      auto limit = OrderHistoryLimit(value);
      if (!limit.ok()) return limit.status();
      options.order_history_limit = *limit;
      limit_seen = true;
    } else if (key == "--cursor" && !cursor_seen &&
               options.verb == CliVerb::OrderHistory) {
      options.order_history_cursor = std::string(value);
      cursor_seen = true;
    } else {
      return Error(ErrorCode::kCliUsageInvalid,
                   "unknown, duplicate, or misplaced option");
    }
  }
  if (!state_seen)
    return Error(ErrorCode::kCliUsageInvalid, "--state-dir is required");
  return options;
}

absl::StatusOr<ControlRequest> MakeControlRequest(const CliOptions& options,
                                                  uint64_t request_id) {
  if (options.state_dir.empty() || request_id == 0) {
    return Error(ErrorCode::kCliUsageInvalid, "invalid server command");
  }
  ControlRequest request;
  request.request_id = request_id;
  if (options.verb == CliVerb::Status)
    request.payload = StatusRequest{};
  else if (options.verb == CliVerb::OrderHistory) {
    if (options.order_history_limit == 0 || options.order_history_limit > 500) {
      return Error(ErrorCode::kCliUsageInvalid, "history limit out of range");
    }
    request.payload = OrderHistoryRequest{options.order_history_limit,
                                          options.order_history_cursor};
  } else
    request.payload = StopRequest{};
  return request;
}

std::string ControlSocketPath(std::string_view state_dir) {
  std::string path(state_dir);
  if (!path.empty() && path.back() != '/') path.push_back('/');
  return path + "control.sock";
}

boost::asio::awaitable<absl::StatusOr<ControlResponse>> ExchangeControlRequest(
    boost::asio::io_context& io, std::string path,
    const ControlRequest& request) {
  auto encoded = EncodeControlRequest(request);
  if (!encoded.ok()) co_return encoded.status();
  LineClient client(io, LineEndpoint::Unix(std::move(path)), 1 << 20);
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  auto status = co_await client.Connect(deadline);
  if (!status.ok()) co_return status;
  status = co_await client.Send(*encoded, deadline);
  if (!status.ok()) co_return status;
  auto response = co_await client.Receive(deadline);
  if (!response.ok()) co_return response.status();
  auto decoded = DecodeControlResponse(*response);
  if (!decoded.ok())
    co_return Error(
        ErrorCode::kCliResponseInvalid,
        "invalid server response: " + std::string(decoded.status().message()));
  co_return decoded;
}

absl::StatusOr<ControlResponse> SendControlRequest(
    const std::string& state_dir, const ControlRequest& request) {
  if (state_dir.empty())
    return Error(ErrorCode::kCliUsageInvalid, "state_dir required");
  const std::string path = ControlSocketPath(state_dir);
  if (path.size() >= sizeof(sockaddr_un::sun_path)) {
    return Error(ErrorCode::kCliUsageInvalid, "server socket path too long");
  }
  boost::asio::io_context io;
  auto result = boost::asio::co_spawn(
      io, ExchangeControlRequest(io, path, request), boost::asio::use_future);
  io.run();
  try {
    return result.get();
  } catch (const std::exception& error) {
    return Error(ErrorCode::kCliEngineUnreachable, error.what());
  }
}

absl::StatusOr<std::string> FormatControlResponse(
    const CliOptions& options, const ControlResponse& response,
    uint64_t request_id) {
  if ((response.schema_version != 1 && response.schema_version != 2) ||
      response.request_id != request_id) {
    return Error(ErrorCode::kCliResponseInvalid,
                 "server response header mismatch");
  }
  if (const auto* error = std::get_if<ControlError>(&response.payload)) {
    if (error->code == ErrorCode::kOk ||
        Info(error->code).code != error->code) {
      return Error(ErrorCode::kCliResponseInvalid, "unknown server error code");
    }
    return Error(ErrorCode::kCliEngineError,
                 std::string(Info(error->code).name) + " (" +
                     std::to_string(ErrorNumber(error->code)) +
                     "): " + error->message);
  }
  if (options.verb == CliVerb::Status) {
    const auto* value = std::get_if<StatusResponse>(&response.payload);
    if (!value)
      return Error(ErrorCode::kCliResponseInvalid,
                   "unexpected status response");
    return value->json + "\n";
  }
  if (options.verb == CliVerb::OrderHistory) {
    const auto* value = std::get_if<OrderHistoryResponse>(&response.payload);
    if (!value)
      return Error(ErrorCode::kCliResponseInvalid,
                   "unexpected history response");
    return value->json + "\n";
  }
  if (options.verb == CliVerb::Stop) {
    const auto* value = std::get_if<StopResponse>(&response.payload);
    if (!value || !value->accepted)
      return Error(ErrorCode::kCliStopRejected, "stop not accepted");
    return std::string("stop requested\n");
  }
  return Error(ErrorCode::kCliResponseInvalid, "unexpected server response");
}

}  // namespace hquant
