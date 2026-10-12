#include "hquant/control_server.h"

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <exception>
#include <filesystem>
#include <limits>
#include <poll.h>
#include <sys/socket.h>
#include <sys/stat.h>
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

std::string_view AccountActionName(uint64_t value) {
  switch (value) {
    case static_cast<uint64_t>(AccountAction::PauseNewOrders):
      return "pause_new_orders";
    case static_cast<uint64_t>(AccountAction::ResumeTrading):
      return "resume_trading";
    case static_cast<uint64_t>(AccountAction::SyncAccountState):
      return "sync_account_state";
    case static_cast<uint64_t>(AccountAction::CancelAll):
      return "cancel_all";
  }
  return "unknown";
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

std::string JsonAccounts(const std::vector<AccountStatus>& accounts) {
  std::string result = "[";
  for (const AccountStatus& account : accounts) {
    if (result.size() != 1) result += ',';
    result += "{\"id\":" + JsonString(account.id) +
              ",\"revision\":" + JsonUint(account.revision) +
              ",\"ready\":" + (account.ready ? "true" : "false") +
              ",\"private_connected\":" +
              (account.private_connected ? "true" : "false") +
              ",\"syncing\":" + (account.syncing ? "true" : "false") +
              ",\"manual_paused\":" +
              (account.manual_paused ? "true" : "false") +
              ",\"pending_journal\":" +
              std::to_string(account.pending_journal) +
              ",\"account_queue_peak\":" +
              std::to_string(account.account_queue_peak) +
              ",\"account_queue_rejections\":" +
              std::to_string(account.account_queue_rejections) +
              ",\"journal_pending_peak\":" +
              std::to_string(account.journal_pending_peak) +
              ",\"journal_rejections\":" +
              std::to_string(account.journal_rejections) +
              ",\"trade_queue\":{\"queued\":" +
              std::to_string(account.trade_queue.queued) +
              ",\"peak\":" +
              std::to_string(account.trade_queue.peak) +
              ",\"rejected\":" +
              std::to_string(account.trade_queue.rejected) + "}" +
              ",\"queued\":" + std::to_string(account.queued) +
              ",\"open_orders\":" +
              std::to_string(account.open_orders) +
              ",\"unknown_orders\":" +
              std::to_string(account.unknown_orders) +
              ",\"error\":" + JsonString(account.error) +
              ",\"balances\":{";
    bool first = true;
    for (const auto& [asset, balance] : account.balances) {
      if (!first) result += ',';
      first = false;
      result += JsonString(asset) + ":{\"total_nanos\":" +
                JsonUint(balance.total_nanos) +
                ",\"free_nanos\":" + JsonUint(balance.free_nanos) +
                ",\"locked_nanos\":" + JsonUint(balance.locked_nanos) +
                ",\"available_nanos\":" +
                JsonUint(account.available_nanos.at(asset)) +
                ",\"reserved_nanos\":" +
                JsonUint(account.reserved_nanos.at(asset)) + '}';
    }
    result += "}}";
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
    case HistoryKind::SubmitUnknown: return "submit_unknown";
    case HistoryKind::CancelUnknown: return "cancel_unknown";
    case HistoryKind::CancelRejected: return "cancel_rejected";
    case HistoryKind::LocalRejected: return "local_rejected";
    case HistoryKind::PrivateReportApplied: return "private_report_applied";
    case HistoryKind::SubmitWriting: return "submit_writing";
    case HistoryKind::CancelWriting: return "cancel_writing";
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
  std::memcpy(address.sun_path, socket_path_.c_str(), socket_path_.size() + 1);
  struct stat existing{};
  if (::lstat(socket_path_.c_str(), &existing) == 0) {
    if (!S_ISSOCK(existing.st_mode)) {
      return absl::AlreadyExistsError("control socket path is occupied");
    }
    const int probe = ::socket(AF_UNIX, SOCK_STREAM, 0);
    if (probe < 0) return absl::InternalError("probe control socket failed");
    const int connected =
        ::connect(probe, reinterpret_cast<sockaddr*>(&address),
                  sizeof(address));
    const int probe_error = errno;
    ::close(probe);
    if (connected == 0 || probe_error != ECONNREFUSED) {
      return absl::AlreadyExistsError("control socket is in use");
    }
    if (::unlink(socket_path_.c_str()) != 0) {
      return absl::InternalError("remove stale control socket failed");
    }
  } else if (errno != ENOENT) {
    return absl::InternalError("inspect control socket failed");
  }
  listen_fd_ = ::socket(AF_UNIX, SOCK_STREAM, 0);
  if (listen_fd_ < 0) return absl::InternalError("create control socket failed");
  const bool bound = ::bind(listen_fd_, reinterpret_cast<sockaddr*>(&address),
                            sizeof(address)) == 0;
  if (!bound || ::chmod(socket_path_.c_str(), 0600) != 0 ||
      ::listen(listen_fd_, 8) != 0) {
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
    return response + "],\"accounts\":" +
           JsonAccounts(handlers_.account_status()) +
           ",\"history_health\":" +
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
  if (op == "pause_new_orders" || op == "resume_trading" ||
      op == "sync_account_state" || op == "cancel_all") {
    std::string_view account_id, operator_id, reason;
    if (request["account_id"].get(account_id) || account_id.empty() ||
        request["operator"].get(operator_id) || operator_id.empty() ||
        request["reason"].get(reason) || reason.empty() ||
        reason.size() > 256 || operator_id.size() > 64) {
      return Error(request_id,
                   "account_id, operator and reason are required");
    }
    const AccountAction action =
        op == "pause_new_orders" ? AccountAction::PauseNewOrders :
        op == "resume_trading" ? AccountAction::ResumeTrading :
        op == "sync_account_state" ? AccountAction::SyncAccountState :
                                     AccountAction::CancelAll;
    auto result = handlers_.account_action(
        std::string(account_id), action, std::string(operator_id),
        std::string(reason));
    if (!result.ok()) return Error(request_id, result.status().message());
    return Success(request_id) +
           ",\"accepted\":true,\"cancel_requests_enqueued\":" +
           std::to_string(*result) + "}";
  }
  if (op == "account_controls") {
    std::string_view account_id;
    uint64_t cursor = 0, limit = 50;
    if (request["account_id"].get(account_id) || account_id.empty() ||
        (!request["cursor"].error() && request["cursor"].get(cursor)) ||
        (!request["limit"].error() && request["limit"].get(limit)) ||
        limit == 0 || limit > 100) {
      return Error(request_id, "invalid account audit query");
    }
    auto page = handlers_.account_controls(
        std::string(account_id), static_cast<uint32_t>(limit), cursor);
    if (!page.ok()) return Error(request_id, page.status().message());
    auto response = Success(request_id) +
                    ",\"next_cursor\":" +
                    std::to_string(page->next_cursor) +
                    ",\"records\":[";
    for (const JournalEntry& entry : page->records) {
      if (&entry != &page->records.front()) response += ',';
      response += "{\"sequence\":" + std::to_string(entry.sequence) +
                  ",\"action\":" + std::to_string(entry.result) +
                  ",\"action_name\":" +
                  JsonString(AccountActionName(entry.result)) +
                  ",\"operator\":" + JsonString(entry.asset) +
                  ",\"reason\":" + JsonString(entry.message) +
                  ",\"at_ms\":" +
                  std::to_string(entry.transaction_ms) + "}";
    }
    return response + "]}";
  }
  if (op == "stop") {
    handlers_.stop();
    return Success(request_id) + ",\"accepted\":true}";
  }
  return Error(request_id, "unknown op");
}

}  // namespace hquant::v1
