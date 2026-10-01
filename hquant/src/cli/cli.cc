#include "cli/cli.h"

#include <sys/socket.h>
#include <sys/time.h>
#include <sys/un.h>
#include <unistd.h>

#include <cerrno>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <string>
#include <string_view>
#include <utility>
#include <variant>

#include "absl/status/status.h"
#include "application/control.h"
#include "base/error.h"

namespace hquant {
namespace {

class Socket {
 public:
  explicit Socket(int fd) : fd_(fd) {}
  ~Socket() {
    if (fd_ >= 0) ::close(fd_);
  }
  Socket(const Socket&) = delete;
  Socket& operator=(const Socket&) = delete;
  int get() const { return fd_; }

 private:
  int fd_;
};

absl::StatusOr<uint32_t> HistoryLimit(std::string_view text) {
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

absl::Status IoError(const char* action) {
  return Error(ErrorCode::kCliEngineUnreachable,
               std::string(action) + ": " + std::strerror(errno));
}

}  // namespace

absl::StatusOr<CliOptions> ParseCliArguments(
    std::span<const std::string_view> arguments) {
  if (arguments.empty())
    return Error(ErrorCode::kCliUsageInvalid, "missing command");
  CliOptions options;
  if (arguments[0] == "start")
    options.verb = CliVerb::Start;
  else if (arguments[0] == "status")
    options.verb = CliVerb::Status;
  else if (arguments[0] == "history")
    options.verb = CliVerb::History;
  else if (arguments[0] == "stop")
    options.verb = CliVerb::Stop;
  else
    return Error(ErrorCode::kCliUsageInvalid, "unknown command");

  bool state_seen = false, config_seen = false, limit_seen = false,
       cursor_seen = false;
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
    } else if (key == "--config" && !config_seen &&
               options.verb == CliVerb::Start) {
      options.config_path = std::string(value);
      config_seen = true;
    } else if (key == "--limit" && !limit_seen &&
               options.verb == CliVerb::History) {
      auto limit = HistoryLimit(value);
      if (!limit.ok()) return limit.status();
      options.history_limit = *limit;
      limit_seen = true;
    } else if (key == "--cursor" && !cursor_seen &&
               options.verb == CliVerb::History) {
      options.history_cursor = std::string(value);
      cursor_seen = true;
    } else {
      return Error(ErrorCode::kCliUsageInvalid,
                   "unknown, duplicate, or misplaced option");
    }
  }
  if (!state_seen || (options.verb == CliVerb::Start && !config_seen)) {
    return Error(ErrorCode::kCliUsageInvalid,
                 "--state-dir and start --config are required");
  }
  return options;
}

absl::StatusOr<AppConfig> PrepareStart(const CliOptions& options) {
  if (options.verb != CliVerb::Start || options.config_path.empty() ||
      options.state_dir.empty()) {
    return Error(ErrorCode::kCliUsageInvalid, "invalid start command");
  }
  return LoadConfig(options.config_path);
}

absl::StatusOr<ControlRequest> MakeControlRequest(const CliOptions& options,
                                                  uint64_t request_id) {
  if (options.verb == CliVerb::Start || options.state_dir.empty() ||
      request_id == 0) {
    return Error(ErrorCode::kCliUsageInvalid, "invalid control command");
  }
  ControlRequest request;
  request.request_id = request_id;
  if (options.verb == CliVerb::Status)
    request.payload = StatusRequest{};
  else if (options.verb == CliVerb::History) {
    if (options.history_limit == 0 || options.history_limit > 500) {
      return Error(ErrorCode::kCliUsageInvalid, "history limit out of range");
    }
    request.payload =
        HistoryRequest{options.history_limit, options.history_cursor};
  } else
    request.payload = StopRequest{};
  return request;
}

std::string ControlSocketPath(std::string_view state_dir) {
  std::string path(state_dir);
  if (!path.empty() && path.back() != '/') path.push_back('/');
  return path + "control.sock";
}

absl::StatusOr<ControlResponse> SendControlRequest(
    const std::string& state_dir, const ControlRequest& request) {
  if (state_dir.empty())
    return Error(ErrorCode::kCliUsageInvalid, "state_dir required");
  auto encoded = EncodeControlRequest(request);
  if (!encoded.ok()) return encoded.status();
  const std::string path = ControlSocketPath(state_dir);
  sockaddr_un address{};
  if (path.size() >= sizeof(address.sun_path)) {
    return Error(ErrorCode::kCliUsageInvalid, "control socket path too long");
  }
  address.sun_family = AF_UNIX;
  std::memcpy(address.sun_path, path.c_str(), path.size() + 1);
  Socket socket(::socket(AF_UNIX, SOCK_STREAM, 0));
  if (socket.get() < 0) return IoError("socket");
#if defined(SO_NOSIGPIPE)
  int no_sigpipe = 1;
  if (::setsockopt(socket.get(), SOL_SOCKET, SO_NOSIGPIPE, &no_sigpipe,
                   sizeof(no_sigpipe)) < 0) {
    return IoError("socket no-sigpipe");
  }
#endif
  timeval timeout{5, 0};
  if (::setsockopt(socket.get(), SOL_SOCKET, SO_RCVTIMEO, &timeout,
                   sizeof(timeout)) < 0 ||
      ::setsockopt(socket.get(), SOL_SOCKET, SO_SNDTIMEO, &timeout,
                   sizeof(timeout)) < 0) {
    return IoError("socket timeout");
  }
  if (::connect(socket.get(), reinterpret_cast<const sockaddr*>(&address),
                sizeof(address)) < 0)
    return IoError("connect");
  encoded->push_back('\n');
  size_t sent = 0;
  while (sent < encoded->size()) {
#if defined(MSG_NOSIGNAL)
    constexpr int kSendFlags = MSG_NOSIGNAL;
#else
    constexpr int kSendFlags = 0;
#endif
    const ssize_t written = ::send(socket.get(), encoded->data() + sent,
                                   encoded->size() - sent, kSendFlags);
    if (written < 0 && errno == EINTR) continue;
    if (written <= 0) return IoError("send");
    sent += static_cast<size_t>(written);
  }
  std::string response;
  response.reserve(4096);
  char buffer[4096];
  while (response.size() <= (1 << 20)) {
    const ssize_t received = ::recv(socket.get(), buffer, sizeof(buffer), 0);
    if (received < 0 && errno == EINTR) continue;
    if (received < 0) return IoError("receive");
    if (received == 0)
      return Error(ErrorCode::kCliEngineUnreachable,
                   "control socket closed before response");
    response.append(buffer, static_cast<size_t>(received));
    const auto newline = response.find('\n');
    if (newline != std::string::npos) {
      if (newline > (1 << 20))
        return Error(ErrorCode::kCliResponseInvalid, "control frame too large");
      auto decoded =
          DecodeControlResponse(std::string_view(response.data(), newline));
      if (!decoded.ok())
        return Error(ErrorCode::kCliResponseInvalid,
                     "invalid control response: " +
                         std::string(decoded.status().message()));
      return decoded;
    }
  }
  return Error(ErrorCode::kCliResponseInvalid, "control frame too large");
}

absl::StatusOr<std::string> FormatControlResponse(
    const CliOptions& options, const ControlResponse& response,
    uint64_t request_id) {
  if ((response.schema_version != 1 && response.schema_version != 2) ||
      response.request_id != request_id) {
    return Error(ErrorCode::kCliResponseInvalid,
                 "control response header mismatch");
  }
  if (const auto* error = std::get_if<ControlError>(&response.payload)) {
    if (error->code == ErrorCode::kOk ||
        Info(error->code).code != error->code) {
      return Error(ErrorCode::kCliResponseInvalid,
                   "unknown control error code");
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
  if (options.verb == CliVerb::History) {
    const auto* value = std::get_if<HistoryResponse>(&response.payload);
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
  return Error(ErrorCode::kCliUsageInvalid, "start has no control response");
}

}  // namespace hquant
