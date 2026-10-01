#include "application/control.h"

#include <cstdint>
#include <string>
#include <string_view>
#include <variant>

#include "absl/status/status.h"
#include "base/error.h"
#include "base/market.h"
#include "base/order.h"
#include "offline/history.h"
#include "offline/recorder.h"
#include "order/simulated_exchange.h"
#include "service/shard.h"
#include "simdjson.h"

namespace hquant {
namespace {

std::string EscapeWire(std::string_view input) {
  constexpr char hex[] = "0123456789abcdef";
  std::string result;
  result.reserve(input.size() + 2);
  result.push_back('"');
  for (unsigned char character : input) {
    switch (character) {
      case '"':
        result += "\\\"";
        break;
      case '\\':
        result += "\\\\";
        break;
      case '\n':
        result += "\\n";
        break;
      case '\r':
        result += "\\r";
        break;
      case '\t':
        result += "\\t";
        break;
      default:
        if (character < 0x20) {
          result += "\\u00";
          result.push_back(hex[character >> 4]);
          result.push_back(hex[character & 15]);
        } else {
          result.push_back(static_cast<char>(character));
        }
    }
  }
  result.push_back('"');
  return result;
}

absl::StatusOr<simdjson::dom::element> Parse(std::string_view json,
                                             simdjson::dom::parser* parser) {
  simdjson::dom::element root;
  if (parser->parse(json).get(root))
    return Error(ErrorCode::kControlMessageInvalid, "invalid control JSON");
  simdjson::dom::object object;
  if (root.get(object))
    return Error(ErrorCode::kControlMessageInvalid,
                 "control frame must be object");
  return root;
}

absl::StatusOr<std::string> JsonObject(std::string_view text) {
  simdjson::dom::parser parser;
  auto root = Parse(text, &parser);
  if (!root.ok()) return root.status();
  return simdjson::minify(*root);
}

absl::StatusOr<std::pair<uint32_t, uint64_t>> Header(
    simdjson::dom::element root) {
  uint64_t version = 0, request_id = 0;
  if (root["schema_version"].get(version) || (version != 1 && version != 2) ||
      root["request_id"].get(request_id)) {
    return Error(ErrorCode::kControlMessageInvalid,
                 "unsupported control frame header");
  }
  return std::pair<uint32_t, uint64_t>{static_cast<uint32_t>(version),
                                       request_id};
}

ErrorCode LegacyErrorCode(std::string_view name) {
  if (name == "bad_request" || name == "frame_too_large")
    return ErrorCode::kControlMessageInvalid;
  if (name == "busy") return ErrorCode::kControlBusy;
  if (name == "timeout") return ErrorCode::kControlTimeout;
  if (name == "history") return ErrorCode::kStorageQueryFailed;
  return ErrorCode::kInternal;
}

std::string_view LegacyErrorName(ErrorCode code) {
  if (code == ErrorCode::kControlMessageInvalid) return "bad_request";
  if (code == ErrorCode::kControlBusy) return "busy";
  if (code == ErrorCode::kControlTimeout) return "timeout";
  const auto value = static_cast<uint16_t>(code);
  if (value >= 17000 && value < 18000) return "history";
  return "internal";
}

}  // namespace

absl::StatusOr<std::string> EncodeControlRequest(
    const ControlRequest& request) {
  if (request.schema_version != 1 && request.schema_version != 2)
    return Error(ErrorCode::kControlMessageInvalid,
                 "unsupported control version");
  std::string frame =
      "{\"schema_version\":" + std::to_string(request.schema_version) +
      ",\"request_id\":" + std::to_string(request.request_id);
  if (std::holds_alternative<StatusRequest>(request.payload)) {
    frame += ",\"kind\":\"status\"}";
  } else if (const auto* history =
                 std::get_if<HistoryRequest>(&request.payload)) {
    if (history->limit == 0 || history->limit > 500) {
      return Error(ErrorCode::kControlMessageInvalid,
                   "history limit out of range");
    }
    frame +=
        ",\"kind\":\"history\",\"limit\":" + std::to_string(history->limit) +
        ",\"cursor\":" + EscapeWire(history->cursor) + "}";
  } else {
    frame += ",\"kind\":\"stop\"}";
  }
  return frame;
}

absl::StatusOr<ControlRequest> DecodeControlRequest(std::string_view json) {
  simdjson::dom::parser parser;
  auto root = Parse(json, &parser);
  if (!root.ok()) return root.status();
  auto header = Header(*root);
  if (!header.ok()) return header.status();
  std::string_view kind;
  if ((*root)["kind"].get(kind))
    return Error(ErrorCode::kControlMessageInvalid, "missing request kind");
  ControlRequest request;
  request.schema_version = header->first;
  request.request_id = header->second;
  if (kind == "status")
    request.payload = StatusRequest{};
  else if (kind == "stop")
    request.payload = StopRequest{};
  else if (kind == "history") {
    uint64_t limit = 0;
    std::string_view cursor;
    if ((*root)["limit"].get(limit) || limit == 0 || limit > 500 ||
        (*root)["cursor"].get(cursor)) {
      return Error(ErrorCode::kControlMessageInvalid,
                   "invalid history request");
    }
    request.payload =
        HistoryRequest{static_cast<uint32_t>(limit), std::string(cursor)};
  } else
    return Error(ErrorCode::kControlMessageInvalid, "unknown request kind");
  return request;
}

absl::StatusOr<std::string> EncodeControlResponse(
    const ControlResponse& response) {
  if (response.schema_version != 1 && response.schema_version != 2)
    return Error(ErrorCode::kControlMessageInvalid,
                 "unsupported control version");
  std::string frame =
      "{\"schema_version\":" + std::to_string(response.schema_version) +
      ",\"request_id\":" + std::to_string(response.request_id);
  if (const auto* status = std::get_if<StatusResponse>(&response.payload)) {
    auto json = JsonObject(status->json);
    if (!json.ok()) return json.status();
    frame += ",\"kind\":\"status\",\"data\":" + *json + "}";
  } else if (const auto* history =
                 std::get_if<HistoryResponse>(&response.payload)) {
    auto json = JsonObject(history->json);
    if (!json.ok()) return json.status();
    frame += ",\"kind\":\"history\",\"data\":" + *json + "}";
  } else if (const auto* stop = std::get_if<StopResponse>(&response.payload)) {
    frame += std::string(",\"kind\":\"stop\",\"accepted\":") +
             (stop->accepted ? "true}" : "false}");
  } else {
    const auto& error = std::get<ControlError>(response.payload);
    if (error.code == ErrorCode::kOk || Info(error.code).code != error.code) {
      return Error(ErrorCode::kControlMessageInvalid,
                   "invalid control error code");
    }
    if (response.schema_version == 1) {
      frame += ",\"kind\":\"error\",\"code\":" +
               EscapeWire(LegacyErrorName(error.code)) +
               ",\"message\":" + EscapeWire(error.message) + "}";
    } else {
      frame += ",\"kind\":\"error\",\"code\":" +
               std::to_string(static_cast<uint16_t>(error.code)) +
               ",\"name\":" + EscapeWire(Info(error.code).name) +
               ",\"message\":" + EscapeWire(error.message) + "}";
    }
  }
  return frame;
}

absl::StatusOr<ControlResponse> DecodeControlResponse(std::string_view json) {
  simdjson::dom::parser parser;
  auto root = Parse(json, &parser);
  if (!root.ok()) return root.status();
  auto header = Header(*root);
  if (!header.ok()) return header.status();
  std::string_view kind;
  if ((*root)["kind"].get(kind))
    return Error(ErrorCode::kControlMessageInvalid, "missing response kind");
  ControlResponse response;
  response.schema_version = header->first;
  response.request_id = header->second;
  if (kind == "status" || kind == "history") {
    simdjson::dom::element data;
    simdjson::dom::object object;
    if ((*root)["data"].get(data) || data.get(object)) {
      return Error(ErrorCode::kControlMessageInvalid, "invalid response data");
    }
    if (kind == "status")
      response.payload = StatusResponse{simdjson::minify(data)};
    else
      response.payload = HistoryResponse{simdjson::minify(data)};
  } else if (kind == "stop") {
    bool accepted = false;
    if ((*root)["accepted"].get(accepted))
      return Error(ErrorCode::kControlMessageInvalid, "invalid stop response");
    response.payload = StopResponse{accepted};
  } else if (kind == "error") {
    std::string_view message;
    if ((*root)["message"].get(message)) {
      return Error(ErrorCode::kControlMessageInvalid, "invalid error response");
    }
    ErrorCode code;
    if (response.schema_version == 1) {
      std::string_view legacy;
      if ((*root)["code"].get(legacy))
        return Error(ErrorCode::kControlMessageInvalid,
                     "invalid legacy error response");
      code = LegacyErrorCode(legacy);
    } else {
      uint64_t number = 0;
      std::string_view name;
      if ((*root)["code"].get(number) || number > UINT16_MAX ||
          (*root)["name"].get(name)) {
        return Error(ErrorCode::kControlMessageInvalid,
                     "invalid error code or name");
      }
      code = static_cast<ErrorCode>(number);
      if (code == ErrorCode::kOk || Info(code).code != code ||
          Info(code).name != name) {
        return Error(ErrorCode::kControlMessageInvalid,
                     "error code and name mismatch");
      }
    }
    response.payload = ControlError{code, std::string(message)};
  } else
    return Error(ErrorCode::kControlMessageInvalid, "unknown response kind");
  return response;
}

}  // namespace hquant

#include <sys/socket.h>
#include <sys/time.h>
#include <sys/un.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <system_error>
#include <utility>

namespace hquant {
namespace {

constexpr size_t kMaxFrame = 64 * 1024;
constexpr int kMaxConcurrent = 16;

bool SendAll(int fd, std::string_view bytes) {
  size_t offset = 0;
  while (offset < bytes.size()) {
#if defined(MSG_NOSIGNAL)
    constexpr int kFlags = MSG_NOSIGNAL;
#else
    constexpr int kFlags = 0;
#endif
    const ssize_t sent =
        send(fd, bytes.data() + offset, bytes.size() - offset, kFlags);
    if (sent <= 0) return false;
    offset += static_cast<size_t>(sent);
  }
  return true;
}

void SendError(int fd, ErrorCode code, std::string message,
               uint32_t schema_version = 2) {
  ControlResponse response;
  response.schema_version = schema_version;
  response.payload = ControlError{code, std::move(message)};
  auto frame = EncodeControlResponse(response);
  if (frame.ok()) SendAll(fd, *frame + "\n");
}

}  // namespace

ControlServer::ControlServer(std::string path, Handler handler, int listen_fd)
    : socket_path_(std::move(path)),
      handler_(std::move(handler)),
      listen_fd_(listen_fd) {}

absl::StatusOr<std::unique_ptr<ControlServer>> ControlServer::Start(
    std::string socket_path, Handler handler) {
  if (!handler || socket_path.empty() ||
      socket_path.size() >= sizeof(sockaddr_un::sun_path)) {
    return Error(ErrorCode::kControlSocketFailed,
                 "invalid control socket path or handler");
  }
  const int fd = socket(AF_UNIX, SOCK_STREAM, 0);
  if (fd < 0)
    return ErrorFromSystem(ErrorCode::kControlSocketFailed,
                           std::error_code(errno, std::generic_category()),
                           "control socket");
  sockaddr_un address{};
  address.sun_family = AF_UNIX;
  std::memcpy(address.sun_path, socket_path.c_str(), socket_path.size() + 1);
  if (access(socket_path.c_str(), F_OK) == 0) {
    const int probe = socket(AF_UNIX, SOCK_STREAM, 0);
    const bool in_use =
        probe >= 0 && connect(probe, reinterpret_cast<sockaddr*>(&address),
                              sizeof(address)) == 0;
    if (probe >= 0) close(probe);
    if (in_use) {
      close(fd);
      return Error(ErrorCode::kControlSocketInUse, "control socket is in use");
    }
    if (unlink(socket_path.c_str()) != 0) {
      close(fd);
      return ErrorFromSystem(ErrorCode::kControlSocketFailed,
                             std::error_code(errno, std::generic_category()),
                             "remove stale control socket: " + socket_path);
    }
  }
  if (bind(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0 ||
      listen(fd, kMaxConcurrent) != 0) {
    const std::error_code error(errno, std::generic_category());
    close(fd);
    unlink(socket_path.c_str());
    return ErrorFromSystem(ErrorCode::kControlSocketFailed, error,
                           "bind/listen control socket: " + socket_path);
  }
  auto server = std::unique_ptr<ControlServer>(
      new ControlServer(std::move(socket_path), std::move(handler), fd));
  server->accept_thread_ =
      std::thread([self = server.get()] { self->AcceptLoop(); });
  return server;
}

ControlServer::~ControlServer() { Stop(); }

void ControlServer::Stop() {
  if (stopping_.exchange(true)) return;
  if (listen_fd_ >= 0) {
    shutdown(listen_fd_, SHUT_RDWR);
    close(listen_fd_);
    listen_fd_ = -1;
  }
  if (accept_thread_.joinable()) accept_thread_.join();
  {
    std::lock_guard lock(workers_mutex_);
    for (auto& worker : workers_)
      if (worker.thread.joinable()) worker.thread.join();
  }
  unlink(socket_path_.c_str());
}

void ControlServer::AcceptLoop() {
  while (!stopping_) {
    const int client = accept(listen_fd_, nullptr, nullptr);
    if (client < 0) {
      if (errno == EINTR) continue;
      break;
    }
    if (active_.fetch_add(1) >= kMaxConcurrent) {
      active_.fetch_sub(1);
      SendError(client, ErrorCode::kControlBusy,
                "control server request limit reached");
      close(client);
      continue;
    }
    timeval timeout{2, 0};
    setsockopt(client, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    setsockopt(client, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
#if defined(SO_NOSIGPIPE)
    int no_sigpipe = 1;
    setsockopt(client, SOL_SOCKET, SO_NOSIGPIPE, &no_sigpipe,
               sizeof(no_sigpipe));
#endif
    std::lock_guard lock(workers_mutex_);
    for (auto it = workers_.begin(); it != workers_.end();) {
      if ((*it).done->load()) {
        if ((*it).thread.joinable()) (*it).thread.join();
        it = workers_.erase(it);
      } else
        ++it;
    }
    auto done = std::make_shared<std::atomic<bool>>(false);
    workers_.push_back(Worker{std::thread([this, client, done] {
                                HandleConnection(client);
                                close(client);
                                active_.fetch_sub(1);
                                *done = true;
                              }),
                              std::move(done)});
  }
}

void ControlServer::HandleConnection(int fd) {
  std::string frame;
  frame.reserve(1024);
  char buffer[4096];
  while (frame.size() <= kMaxFrame) {
    const ssize_t read_bytes = recv(fd, buffer, sizeof(buffer), 0);
    if (read_bytes <= 0) return;
    frame.append(buffer, static_cast<size_t>(read_bytes));
    const size_t newline = frame.find('\n');
    if (newline != std::string::npos) {
      frame.resize(newline);
      break;
    }
  }
  if (frame.size() > kMaxFrame) {
    SendError(fd, ErrorCode::kControlMessageInvalid,
              "control frame exceeds 64 KiB");
    return;
  }
  auto request = DecodeControlRequest(frame);
  if (!request.ok()) {
    SendError(fd, ErrorCode::kControlMessageInvalid,
              std::string(request.status().message()));
    return;
  }
  auto response = handler_(*request);
  response.schema_version = request->schema_version;
  response.request_id = request->request_id;
  auto encoded = EncodeControlResponse(response);
  if (!encoded.ok()) {
    SendError(fd, ErrorCode::kInternal, std::string(encoded.status().message()),
              request->schema_version);
    return;
  }
  SendAll(fd, *encoded + "\n");
}

}  // namespace hquant

namespace hquant {

std::string EscapeJson(std::string_view text) {
  constexpr char hex[] = "0123456789abcdef";
  std::string result = "\"";
  for (unsigned char value : text) {
    if (value == '"' || value == '\\') {
      result.push_back('\\');
      result.push_back(static_cast<char>(value));
    } else if (value == '\n')
      result += "\\n";
    else if (value == '\r')
      result += "\\r";
    else if (value == '\t')
      result += "\\t";
    else if (value < 0x20) {
      result += "\\u00";
      result.push_back(hex[value >> 4]);
      result.push_back(hex[value & 15]);
    } else
      result.push_back(static_cast<char>(value));
  }
  return result + "\"";
}

const char* BookStateName(BookSyncState state) {
  switch (state) {
    case BookSyncState::Subscribing:
      return "Subscribing";
    case BookSyncState::Buffering:
      return "Buffering";
    case BookSyncState::Replaying:
      return "Replaying";
    case BookSyncState::Live:
      return "Live";
    case BookSyncState::Stale:
      return "Stale";
    case BookSyncState::Resyncing:
      return "Resyncing";
  }
  return "Unknown";
}

std::string StatusJson(const ShardRuntime& shard, const SimpleSimulatedExchange& sim_exchange,
                       const MarketSpec& market,
                       const SqliteRecorder& recorder) {
  const auto health = recorder.Health();
  return "{\"mode\":\"simulated\",\"exchange\":\"simulated\",\"book\":" +
         EscapeJson(BookStateName(shard.Book().State())) +
         ",\"active_shards\":1,\"strategy\":\"simple_pmm\",\"open_orders\":" +
         std::to_string(sim_exchange.OpenOrders().size()) +
         ",\"recorder_dropped\":" + std::to_string(health.dropped_count) +
         ",\"history_gaps\":" + std::to_string(health.gap_ranges.size()) +
         ",\"recorder_error\":" + EscapeJson(health.last_error) +
         ",\"balances\":{" + EscapeJson(market.base_asset.value) + ":" +
         EscapeJson(sim_exchange.BalanceOf(market.base_asset).ToString()) + "," +
         EscapeJson(market.quote_asset.value) + ":" +
         EscapeJson(sim_exchange.BalanceOf(market.quote_asset).ToString()) +
         "},\"fees_paid\":{" + EscapeJson(market.base_asset.value) + ":" +
         EscapeJson(sim_exchange.FeesPaid(market.base_asset).ToString()) + "," +
         EscapeJson(market.quote_asset.value) + ":" +
         EscapeJson(sim_exchange.FeesPaid(market.quote_asset).ToString()) + "}}";
}

const char* OrderStatusName(ExchangeOrderStatus status) {
  switch (status) {
    case ExchangeOrderStatus::New:
      return "New";
    case ExchangeOrderStatus::PartiallyFilled:
      return "PartiallyFilled";
    case ExchangeOrderStatus::Filled:
      return "Filled";
    case ExchangeOrderStatus::Canceled:
      return "Canceled";
    case ExchangeOrderStatus::Rejected:
      return "Rejected";
    case ExchangeOrderStatus::Expired:
      return "Expired";
  }
  return "Unknown";
}

std::string HistoryJson(const HistoryPage& page) {
  std::string json = "{\"rows\":[";
  for (size_t index = 0; index < page.rows.size(); ++index) {
    const auto& row = page.rows[index];
    if (index) json += ",";
    std::string kind;
    std::string details;
    if (const auto* intent = std::get_if<OrderIntent>(&row.payload)) {
      kind = "intent";
      details =
          ",\"client_id\":" + EscapeJson(intent->client_id.value) +
          ",\"amount\":" + EscapeJson(intent->request.base_amount.ToString());
    } else if (const auto* update = std::get_if<OrderUpdate>(&row.payload)) {
      kind = "order";
      if (update->client_id)
        details = ",\"client_id\":" + EscapeJson(update->client_id->value);
      details +=
          ",\"status\":" + EscapeJson(OrderStatusName(update->exchange_status));
    } else if (const auto* trade = std::get_if<TradeUpdate>(&row.payload)) {
      kind = "trade";
      details = ",\"trade_id\":" + EscapeJson(trade->exchange_trade_id.value) +
                ",\"amount\":" + EscapeJson(trade->base_amount.ToString()) +
                ",\"price\":" + EscapeJson(trade->price.ToString()) +
                ",\"fees\":[";
      for (size_t fee = 0; fee < trade->fees.size(); ++fee) {
        if (fee) details += ",";
        details += "{\"asset\":" + EscapeJson(trade->fees[fee].asset.value) +
                   ",\"amount\":" +
                   EscapeJson(trade->fees[fee].signed_amount.ToString()) + "}";
      }
      details += "]";
    } else if (const auto* decision =
                   std::get_if<DecisionRecord>(&row.payload)) {
      kind = "decision";
      details = std::string(",\"accepted\":") +
                (decision->accepted ? "true" : "false") + ",\"reason\":" +
                std::to_string(static_cast<uint16_t>(decision->reason)) +
                ",\"reason_name\":" + EscapeJson(Info(decision->reason).name) +
                ",\"message\":" + EscapeJson(decision->message);
    } else if (std::holds_alternative<Checkpoint>(row.payload))
      kind = "checkpoint";
    else {
      kind = "gap";
      const auto& gap = std::get<HistoryGap>(row.payload);
      details =
          ",\"reason\":" + std::to_string(static_cast<uint16_t>(gap.reason)) +
          ",\"reason_name\":" + EscapeJson(Info(gap.reason).name);
    }
    json += "{\"run_id\":" + std::to_string(row.run_id.value) +
            ",\"sequence\":" + std::to_string(row.shard_sequence) +
            ",\"kind\":" + EscapeJson(kind) + details + "}";
  }
  json += "],\"next_cursor\":";
  json += page.next_cursor ? EscapeJson(*page.next_cursor) : "null";
  json += ",\"incomplete_ranges\":[";
  for (size_t index = 0; index < page.incomplete_ranges.size(); ++index) {
    if (index) json += ",";
    const auto& gap = page.incomplete_ranges[index];
    json += "{\"first\":" + std::to_string(gap.first_seq) +
            ",\"last\":" + std::to_string(gap.last_seq) +
            ",\"reason\":" + std::to_string(static_cast<uint16_t>(gap.reason)) +
            ",\"reason_name\":" + EscapeJson(Info(gap.reason).name) + "}";
  }
  return json + "]}";
}

}  // namespace hquant
