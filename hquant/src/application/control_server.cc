#include "application/control_server.h"

#include <cstdint>
#include <string>
#include <string_view>
#include <variant>

#include "absl/status/status.h"
#include "base/error.h"
#include "base/market.h"
#include "base/order.h"
#include "order/simulated_exchange.h"
#include "shard/shard.h"
#include "simdjson.h"
#include "storage/history.h"
#include "storage/recorder.h"

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
    return Error(ErrorCode::kControlMessageInvalid, "invalid server JSON");
  simdjson::dom::object object;
  if (root.get(object))
    return Error(ErrorCode::kControlMessageInvalid,
                 "server frame must be object");
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
                 "unsupported server frame header");
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
  const auto value = -ErrorNumber(code);
  if (value >= 17000 && value < 18000) return "history";
  return "internal";
}

}  // namespace

absl::StatusOr<std::string> EncodeControlRequest(const ControlRequest& request) {
  if (request.schema_version != 1 && request.schema_version != 2)
    return Error(ErrorCode::kControlMessageInvalid,
                 "unsupported server version");
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
      return Error(ErrorCode::kControlMessageInvalid, "invalid history request");
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
                 "unsupported server version");
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
                   "invalid server error code");
    }
    if (response.schema_version == 1) {
      frame += ",\"kind\":\"error\",\"code\":" +
               EscapeWire(LegacyErrorName(error.code)) +
               ",\"message\":" + EscapeWire(error.message) + "}";
    } else {
      frame += ",\"kind\":\"error\",\"code\":" +
               std::to_string(ErrorNumber(error.code)) +
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
      int64_t number = 0;
      std::string_view name;
      if ((*root)["code"].get(number) || (*root)["name"].get(name)) {
        return Error(ErrorCode::kControlMessageInvalid,
                     "invalid error code or name");
      }
      const auto known = ErrorFromNumber(number);
      code = known.value_or(ErrorCode::kInternal);
      if (!known || Info(code).name != name) {
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

#include <utility>
#include <csignal>

#include "boost/asio/co_spawn.hpp"
#include "boost/asio/post.hpp"

namespace hquant {
namespace {

std::string ErrorFrame(ErrorCode code, std::string message,
                       uint32_t schema_version = 2) {
  ControlResponse response;
  response.schema_version = schema_version;
  response.payload = ControlError{code, std::move(message)};
  auto encoded = EncodeControlResponse(response);
  return encoded.ok() ? *encoded : std::string{};
}

}  // namespace

ControlServer::ControlServer(std::string path, Handler handler)
    : socket_path_(std::move(path)), handler_(std::move(handler)) {}

absl::StatusOr<std::unique_ptr<ControlServer>> ControlServer::Start(
    std::string socket_path, Handler handler,
    std::function<void(int)> on_signal) {
  if (!handler) {
    return Error(ErrorCode::kControlSocketFailed, "missing control handler");
  }
  auto server = std::unique_ptr<ControlServer>(
      new ControlServer(std::move(socket_path), std::move(handler)));
  if (on_signal) {
    server->signals_ = std::make_unique<boost::asio::signal_set>(
        server->io_, SIGINT, SIGTERM);
    server->signals_->async_wait(
        [callback = std::move(on_signal)](
            const boost::system::error_code& ec, int signal) {
          if (!ec) callback(signal);
        });
  }
  LineServer::Options options;
  options.max_sessions = 16;
  options.max_frame = 64 * 1024;
  options.read_timeout = std::chrono::seconds(2);
  options.write_timeout = std::chrono::seconds(2);
  options.busy_reply = ErrorFrame(ErrorCode::kControlBusy,
                                  "server request limit reached");
  options.invalid_reply = ErrorFrame(ErrorCode::kControlMessageInvalid,
                                     "server frame exceeds 64 KiB");
  auto line = LineServer::Start(
      server->io_, LineEndpoint::Unix(server->socket_path_),
      [self = server.get()](std::string frame)
          -> boost::asio::awaitable<std::string> {
        co_return co_await self->HandleFrame(std::move(frame));
      },
      std::move(options));
  if (!line.ok()) return line.status();
  server->line_server_ = std::move(*line);
  server->thread_ = std::thread([self = server.get()] { self->io_.run(); });
  return server;
}

ControlServer::~ControlServer() { Stop(); }

void ControlServer::Stop(bool remove_socket) {
  if (line_server_) line_server_->Stop(remove_socket);
  if (signals_) {
    if (thread_.joinable())
      boost::asio::post(io_, [this] { signals_->cancel(); });
    else
      signals_->cancel();
  }
  if (thread_.joinable()) thread_.join();
}

boost::asio::awaitable<std::string> ControlServer::HandleFrame(
    std::string frame) {
  auto request = DecodeControlRequest(frame);
  if (!request.ok()) {
    co_return ErrorFrame(ErrorCode::kControlMessageInvalid,
                         std::string(request.status().message()));
  }
  const uint32_t schema_version = request->schema_version;
  const uint64_t request_id = request->request_id;
  auto response = co_await handler_(std::move(*request));
  response.schema_version = schema_version;
  response.request_id = request_id;
  auto encoded = EncodeControlResponse(response);
  if (!encoded.ok()) {
    co_return ErrorFrame(ErrorCode::kInternal,
                         std::string(encoded.status().message()),
                         schema_version);
  }
  co_return *encoded;
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
    case BookSyncState::WaitingSnapshot:
      return "WaitingSnapshot";
    case BookSyncState::CatchingUp:
      return "CatchingUp";
    case BookSyncState::Live:
      return "Live";
    case BookSyncState::Stale:
      return "Stale";
    case BookSyncState::Resyncing:
      return "Resyncing";
  }
  return "Unknown";
}

std::string StatusJson(const Shard& shard,
                       const SimpleSimulatedExchange& sim_exchange,
                       const MarketSpec& market,
                       const SqliteHistoryWriter& recorder) {
  const auto health = recorder.Health();
  return "{\"mode\":\"simulated\",\"exchange\":\"simulated\",\"book\":" +
         EscapeJson(BookStateName(shard.Book().State())) +
         ",\"active_shards\":1,\"strategy\":\"simple_pmm\",\"open_orders\":" +
         std::to_string(sim_exchange.OpenOrders().size()) +
         ",\"recorder_dropped\":" + std::to_string(health.dropped_count) +
         ",\"history_gaps\":" + std::to_string(health.gap_ranges.size()) +
         ",\"recorder_error\":" + EscapeJson(health.last_error) +
         ",\"balances\":{" + EscapeJson(market.base_asset.value) + ":" +
         EscapeJson(sim_exchange.BalanceOf(market.base_asset).ToString()) +
         "," + EscapeJson(market.quote_asset.value) + ":" +
         EscapeJson(sim_exchange.BalanceOf(market.quote_asset).ToString()) +
         "},\"fees_paid\":{" + EscapeJson(market.base_asset.value) + ":" +
         EscapeJson(sim_exchange.FeesPaid(market.base_asset).ToString()) + "," +
         EscapeJson(market.quote_asset.value) + ":" +
         EscapeJson(sim_exchange.FeesPaid(market.quote_asset).ToString()) +
         "}}";
}

const char* OrderStatusName(ExchangeOrderStatus status) {
  switch (status) {
    case ExchangeOrderStatus::Open:
      return "Open";
    case ExchangeOrderStatus::PartiallyTraded:
      return "PartiallyTraded";
    case ExchangeOrderStatus::Traded:
      return "Traded";
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
    if (const auto* prepared = std::get_if<PreparedOrder>(&row.payload)) {
      kind = "prepared_order";
      details =
          ",\"client_id\":" + EscapeJson(prepared->client_id.value) +
          ",\"amount\":" + EscapeJson(prepared->request.base_amount.ToString());
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
    } else if (const auto* decision = std::get_if<ActionRecord>(&row.payload)) {
      kind = "decision";
      details = std::string(",\"accepted\":") +
                (decision->accepted ? "true" : "false") +
                ",\"reason\":" + std::to_string(ErrorNumber(decision->reason)) +
                ",\"reason_name\":" + EscapeJson(Info(decision->reason).name) +
                ",\"message\":" + EscapeJson(decision->message);
    } else if (std::holds_alternative<RecordedCheckpoint>(row.payload))
      kind = "checkpoint";
    else {
      kind = "gap";
      const auto& gap = std::get<HistoryGap>(row.payload);
      details = ",\"reason\":" + std::to_string(ErrorNumber(gap.reason)) +
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
            ",\"reason\":" + std::to_string(ErrorNumber(gap.reason)) +
            ",\"reason_name\":" + EscapeJson(Info(gap.reason).name) + "}";
  }
  return json + "]}";
}

}  // namespace hquant
