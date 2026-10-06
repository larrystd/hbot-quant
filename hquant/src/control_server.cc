#include "hquant/control_server.h"

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <exception>
#include <filesystem>
#include <limits>
#include <poll.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <string>
#include <string_view>
#include <utility>

#include "absl/status/status.h"
#include "hquant/base/fixed.h"
#include "hquant/perf_probe.h"
#include "simdjson.h"

namespace hquant::v1 {
namespace {

std::string JsonString(std::string_view value) {
  std::string result = "\"";
  constexpr char hex[] = "0123456789abcdef";
  for (unsigned char byte : value) {
    switch (byte) {
      case '"': result += "\\\""; break;
      case '\\': result += "\\\\"; break;
      case '\n': result += "\\n"; break;
      case '\r': result += "\\r"; break;
      case '\t': result += "\\t"; break;
      default:
        if (byte < 0x20) {
          result += "\\u00";
          result += hex[byte >> 4];
          result += hex[byte & 15];
        } else {
          result += static_cast<char>(byte);
        }
    }
  }
  return result + '"';
}

std::string Error(uint64_t request_id, std::string_view message) {
  return "{\"request_id\":" + std::to_string(request_id) +
         ",\"ok\":false,\"error\":" + JsonString(message) + "}";
}

std::string Success(uint64_t request_id) {
  return "{\"request_id\":" + std::to_string(request_id) + ",\"ok\":true";
}

std::string JsonUint(uint64_t value) { return JsonString(std::to_string(value)); }

std::string JsonLevels(const std::vector<BookLevel>& levels) {
  std::string result = "[";
  for (const BookLevel& level : levels) {
    if (result.size() != 1) result += ',';
    result += '[' + std::to_string(level.price_ticks) + ',' +
              std::to_string(level.quantity_lots) + ']';
  }
  return result + ']';
}

const char* BookStateName(BookState state) {
  switch (state) {
    case BookState::Syncing: return "syncing";
    case BookState::Live: return "live";
  }
  return "unknown";
}

const char* HistoryKindName(HistoryKind kind) {
  switch (kind) {
    case HistoryKind::SubmitCommand: return "submit_command";
    case HistoryKind::CancelCommand: return "cancel_command";
    case HistoryKind::OrderAccepted: return "order_accepted";
    case HistoryKind::OrderRejected: return "order_rejected";
    case HistoryKind::OrderPartiallyFilled: return "order_partially_filled";
    case HistoryKind::OrderFilled: return "order_filled";
    case HistoryKind::OrderCancelled: return "order_cancelled";
    case HistoryKind::Fill: return "fill";
    case HistoryKind::Balance: return "balance";
  }
  return "unknown";
}

std::string JsonHealth(const HistoryHealth& health) {
  return "{\"queued\":" + std::to_string(health.queued) +
         ",\"dropped\":" + std::to_string(health.dropped) +
         ",\"error\":" + JsonString(health.error) + "}";
}

bool SendLine(int fd, std::string_view line) {
  std::string text(line);
  text.push_back('\n');
  size_t written = 0;
  while (written < text.size()) {
#ifdef MSG_NOSIGNAL
    constexpr int flags = MSG_NOSIGNAL;
#else
    constexpr int flags = 0;
#endif
    const ssize_t count = ::send(fd, text.data() + written,
                                 text.size() - written, flags);
    if (count < 0 && errno == EINTR) continue;
    if (count <= 0) return false;
    written += static_cast<size_t>(count);
  }
  return true;
}

}  // namespace

ControlServer::ControlServer(std::string socket_path, ControlHandlers handlers)
    : socket_path_(std::move(socket_path)), handlers_(std::move(handlers)) {}

ControlServer::~ControlServer() { Stop(); }

absl::Status ControlServer::Start() {
  if (listen_fd_ >= 0 || thread_.joinable()) {
    return absl::FailedPreconditionError("control server already started");
  }
  sockaddr_un address{};
  address.sun_family = AF_UNIX;
  if (socket_path_.size() >= sizeof(address.sun_path)) {
    return absl::InvalidArgumentError("control socket path too long");
  }
  if (std::filesystem::exists(socket_path_)) {
    return absl::AlreadyExistsError("control socket path already exists");
  }
  std::memcpy(address.sun_path, socket_path_.c_str(), socket_path_.size() + 1);
  listen_fd_ = ::socket(AF_UNIX, SOCK_STREAM, 0);
  if (listen_fd_ < 0) return absl::InternalError("create control socket failed");
  const bool bound = ::bind(listen_fd_, reinterpret_cast<sockaddr*>(&address),
                            sizeof(address)) == 0;
  if (!bound || ::listen(listen_fd_, 8) != 0) {
    const std::string error = std::strerror(errno);
    ::close(listen_fd_);
    listen_fd_ = -1;
    if (bound) ::unlink(socket_path_.c_str());
    return absl::InternalError("bind/listen control socket: " + error);
  }
  stopping_ = false;
  try {
    thread_ = std::thread([this] { Run(); });
  } catch (const std::exception& error) {
    ::close(listen_fd_);
    listen_fd_ = -1;
    ::unlink(socket_path_.c_str());
    return absl::ResourceExhaustedError(error.what());
  }
  return absl::OkStatus();
}

void ControlServer::Stop() {
  stopping_ = true;
  if (thread_.joinable()) thread_.join();
  if (listen_fd_ >= 0) {
    ::close(listen_fd_);
    listen_fd_ = -1;
    ::unlink(socket_path_.c_str());
  }
}

void ControlServer::Run() {
  while (!stopping_) {
    pollfd descriptor{listen_fd_, POLLIN, 0};
    const int ready = ::poll(&descriptor, 1, 200);
    if (ready < 0 && errno == EINTR) continue;
    if (ready <= 0 || !(descriptor.revents & POLLIN)) continue;
    const int client = ::accept(listen_fd_, nullptr, nullptr);
    if (client < 0) continue;
#ifdef SO_NOSIGPIPE
    int no_sigpipe = 1;
    ::setsockopt(client, SOL_SOCKET, SO_NOSIGPIPE, &no_sigpipe,
                 sizeof(no_sigpipe));
#endif
    timeval timeout{2, 0};
    ::setsockopt(client, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    ::setsockopt(client, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
    ServeClient(client);
    ::close(client);
  }
}

void ControlServer::ServeClient(int client_fd) {
  std::string pending;
  char buffer[4096];
  while (!stopping_) {
    const ssize_t count = ::recv(client_fd, buffer, sizeof(buffer), 0);
    if (count < 0 && errno == EINTR) continue;
    if (count <= 0) return;
    pending.append(buffer, static_cast<size_t>(count));
    if (pending.size() > 65536) {
      SendLine(client_fd, Error(0, "control request too large"));
      return;
    }
    size_t end = 0;
    while ((end = pending.find('\n')) != std::string::npos) {
      const std::string response = HandleLine(
          std::string_view(pending.data(), end));
      if (!SendLine(client_fd, response)) return;
      pending.erase(0, end + 1);
    }
  }
}

std::string ControlServer::HandleLine(std::string_view line) {
  simdjson::dom::parser parser;
  simdjson::dom::element request;
  if (parser.parse(line).get(request)) return Error(0, "invalid JSON");
  uint64_t request_id = 0;
  if (request["request_id"].get(request_id)) {
    return Error(0, "request_id must be an unsigned integer");
  }
  std::string_view op;
  if (request["op"].get(op)) return Error(request_id, "op is required");
  if (op == "clock") {
    return Success(request_id) +
           ",\"steady_ns\":" + JsonUint(PerfNowNs()) +
           ",\"bench_ns\":" + JsonUint(BenchClockNowNs()) + "}";
  }
  if (op == "status") {
    auto shards = handlers_.status();
    auto response = Success(request_id) + ",\"shards\":[";
    for (const ShardStatus& shard : shards) {
      if (&shard != &shards.front()) response += ',';
      response += "{\"id\":" + std::to_string(shard.id.value) +
          ",\"stop_requested\":" + (shard.stop_requested ? "true" : "false") +
          ",\"worker_exited\":" + (shard.worker_exited ? "true" : "false") +
          ",\"book_state\":" + JsonString(BookStateName(shard.book_state)) +
          ",\"connection_epoch\":" + std::to_string(shard.connection_epoch) +
          ",\"last_book_sequence\":" + std::to_string(shard.last_book_sequence) +
          ",\"applied_depth_events\":" + std::to_string(shard.applied_depth_events) +
          ",\"accepted_trades\":" + std::to_string(shard.accepted_trades) +
          ",\"last_trade_id\":" + std::to_string(shard.last_trade_id) +
          ",\"depth_digest\":" + JsonUint(shard.depth_digest) +
          ",\"trade_digest\":" + JsonUint(shard.trade_digest) +
          ",\"bids\":" + JsonLevels(shard.bids) +
          ",\"asks\":" + JsonLevels(shard.asks) +
          ",\"market_notices\":" + std::to_string(shard.market_notices) +
          ",\"strategy_timer_ticks\":" + std::to_string(shard.strategy_timer_ticks) +
          ",\"config_version\":" + std::to_string(shard.config_version) +
          ",\"active_orders\":" + std::to_string(shard.active_orders) +
          ",\"base_total_nanos\":" + JsonUint(shard.base_total_nanos) +
          ",\"base_available_nanos\":" + JsonUint(shard.base_available_nanos) +
          ",\"quote_total_nanos\":" + JsonUint(shard.quote_total_nanos) +
          ",\"quote_available_nanos\":" + JsonUint(shard.quote_available_nanos) +
          ",\"last_error\":" + JsonString(shard.last_error) + "}";
    }
    return response + "],\"history_health\":" +
           JsonHealth(handlers_.history_health()) + "}";
  }
  if (op == "history") {
    uint64_t limit = 100, cursor = 0;
    if (!request["limit"].error() && request["limit"].get(limit)) {
      return Error(request_id, "limit must be unsigned");
    }
    if (!request["cursor"].error() && request["cursor"].get(cursor)) {
      return Error(request_id, "cursor must be unsigned");
    }
    if (limit > std::numeric_limits<uint32_t>::max()) {
      return Error(request_id, "limit out of range");
    }
    auto page = handlers_.history(static_cast<uint32_t>(limit), cursor);
    if (!page.ok()) return Error(request_id, page.status().message());
    auto response = Success(request_id) +
                    ",\"next_cursor\":" + std::to_string(page->next_cursor) +
                    ",\"records\":[";
    for (const HistoryRecord& record : page->records) {
      if (&record != &page->records.front()) response += ',';
      response += "{\"run_id\":" + JsonString(record.run_id) +
          ",\"shard\":" + std::to_string(record.shard.value) +
          ",\"sequence\":" + std::to_string(record.sequence) +
          ",\"at_us\":" + std::to_string(record.at_us) +
          ",\"kind\":" + JsonString(HistoryKindName(record.kind)) +
          ",\"order_id\":" + JsonUint(record.order_id) +
          ",\"trade_id\":" + JsonUint(record.trade_id) +
          ",\"price_nanos\":" + JsonUint(record.price_nanos) +
          ",\"quantity_nanos\":" + JsonUint(record.quantity_nanos) +
          ",\"remaining_nanos\":" + JsonUint(record.remaining_nanos) +
          ",\"fee_nanos\":" + JsonUint(record.fee_nanos) +
          ",\"total_nanos\":" + JsonUint(record.total_nanos) +
          ",\"available_nanos\":" + JsonUint(record.available_nanos) +
          ",\"asset\":" + JsonString(record.asset) +
          ",\"message\":" + JsonString(record.message) +
          ",\"dispatched\":" + (record.dispatched ? "true" : "false") +
          ",\"buy\":" + (record.buy ? "true" : "false") + "}";
    }
    return response + "]}";
  }
  if (op == "set_strategy") {
    uint64_t shard_id = 0, expected_version = 0;
    if (request["shard_id"].get(shard_id) || shard_id >= kMaxShards ||
        request["expected_version"].get(expected_version)) {
      return Error(request_id, "invalid shard_id or expected_version");
    }
    simdjson::dom::object patch_object;
    if (request["patch"].get(patch_object)) {
      return Error(request_id, "patch object is required");
    }
    StrategyPatch patch;
    bool any = false;
    for (auto field : patch_object) {
      const std::string_view name = field.key;
      if (name == "order_amount" || name == "bid_spread" ||
          name == "ask_spread") {
        std::string_view value;
        if (field.value.get(value)) {
          return Error(request_id, "strategy decimals must be strings");
        }
        auto parsed = ParseFixed(value);
        if (!parsed.ok()) return Error(request_id, parsed.status().message());
        if (name == "order_amount") patch.order_amount_nanos = *parsed;
        if (name == "bid_spread") patch.bid_spread_ppb = *parsed;
        if (name == "ask_spread") patch.ask_spread_ppb = *parsed;
      } else if (name == "refresh_ms") {
        int64_t value = 0;
        if (field.value.get(value)) {
          return Error(request_id, "refresh_ms must be an integer");
        }
        patch.refresh_ms = value;
      } else {
        return Error(request_id, "unknown strategy patch field");
      }
      any = true;
    }
    if (!any) return Error(request_id, "empty strategy patch");
    auto result = handlers_.set_strategy(
        ShardId{static_cast<uint16_t>(shard_id)}, expected_version,
        std::move(patch));
    if (!result.ok()) return Error(request_id, result.status().message());
    return Success(request_id) +
           ",\"config_version\":" + std::to_string(result->config_version) +
           ",\"actions\":" + std::to_string(result->actions) + "}";
  }
  if (op == "stop") {
    handlers_.stop();
    return Success(request_id) + ",\"accepted\":true}";
  }
  return Error(request_id, "unknown op");
}

}  // namespace hquant::v1
