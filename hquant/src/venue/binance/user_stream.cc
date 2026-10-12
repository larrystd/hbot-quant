#include "hquant/venue/binance/user_stream.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "boost/asio/co_spawn.hpp"
#include "boost/asio/detached.hpp"
#include "boost/asio/redirect_error.hpp"
#include "boost/asio/use_awaitable.hpp"
#include "hquant/base/fixed.h"
#include "hquant/perf_probe.h"
#include "hquant/venue/binance/auth.h"
#include "simdjson.h"

namespace hquant::v1 {
namespace {

absl::Status Bad(std::string_view field) {
  return absl::DataLossError("invalid Binance user event field: " +
                             std::string(field));
}

absl::StatusOr<std::string_view> Text(simdjson::dom::element object,
                                      const char* field) {
  std::string_view value;
  if (object[field].get(value)) return Bad(field);
  return value;
}

absl::StatusOr<uint64_t> Fixed(simdjson::dom::element object,
                               const char* field) {
  auto value = Text(object, field);
  if (!value.ok()) return value.status();
  auto number = ParseFixed(*value);
  if (!number.ok()) return Bad(field);
  return *number;
}

absl::StatusOr<uint64_t> Unsigned(simdjson::dom::element object,
                                  const char* field) {
  uint64_t value = 0;
  if (object[field].get(value)) return Bad(field);
  return value;
}

absl::StatusOr<BalanceDelta> Delta(simdjson::dom::element object,
                                   bool external_lock) {
  auto asset = Text(object, "a");
  auto raw = Text(object, "d");
  auto at = Unsigned(object, "T");
  if (!asset.ok()) return asset.status();
  if (!raw.ok()) return raw.status();
  if (!at.ok()) return at.status();
  const bool credit = raw->empty() || raw->front() != '-';
  const std::string_view positive = credit ? *raw : raw->substr(1);
  auto amount = ParseFixed(positive);
  if (!amount.ok()) return Bad("d");
  return BalanceDelta{std::string(*asset), *amount, credit, external_lock, *at};
}

absl::StatusOr<OrderStatus> Status(std::string_view status) {
  if (status == "NEW") return OrderStatus::Accepted;
  if (status == "PARTIALLY_FILLED") return OrderStatus::PartiallyFilled;
  if (status == "FILLED") return OrderStatus::Filled;
  if (status == "CANCELED" || status == "EXPIRED" ||
      status == "EXPIRED_IN_MATCH")
    return OrderStatus::Cancelled;
  if (status == "REJECTED") return OrderStatus::Rejected;
  return Bad("X");
}

const char* StatusName(OrderStatus status) {
  switch (status) {
    case OrderStatus::PendingSubmit: return "PENDING_SUBMIT";
    case OrderStatus::Rejected: return "REJECTED";
    case OrderStatus::Accepted: return "NEW";
    case OrderStatus::PartiallyFilled: return "PARTIALLY_FILLED";
    case OrderStatus::Filled: return "FILLED";
    case OrderStatus::Cancelled: return "CANCELED";
  }
  return "UNKNOWN";
}

}  // namespace

absl::StatusOr<std::vector<AccountEvent>> ParseBinanceUserFrame(
    std::string_view frame) {
  simdjson::dom::parser parser;
  simdjson::dom::element root;
  if (parser.parse(frame).get(root)) return Bad("JSON");
  simdjson::dom::element event;
  if (!root["event"].get(event) || !root["data"].get(event)) {
    root = event;
  }
  auto kind = Text(root, "e");
  if (!kind.ok()) return kind.status();
  std::vector<AccountEvent> result;
  if (*kind == "outboundAccountPosition") {
    auto version = Unsigned(root, "u");
    if (!version.ok()) return version.status();
    simdjson::dom::array balances;
    if (root["B"].get(balances)) return Bad("B");
    for (simdjson::dom::element row : balances) {
      auto asset = Text(row, "a");
      auto free = Fixed(row, "f");
      auto locked = Fixed(row, "l");
      if (!asset.ok()) return asset.status();
      if (!free.ok()) return free.status();
      if (!locked.ok()) return locked.status();
      auto total = CheckedAdd(*free, *locked);
      if (!total.ok()) return total.status();
      result.emplace_back(
          BalanceUpdate{std::string(*asset), *total, *free, *locked, *version});
    }
    return result;
  }
  if (*kind == "balanceUpdate" || *kind == "externalLockUpdate") {
    auto delta = Delta(root, *kind == "externalLockUpdate");
    if (!delta.ok()) return delta.status();
    result.emplace_back(std::move(*delta));
    return result;
  }
  if (*kind == "listStatus") return result;
  if (*kind == "eventStreamTerminated") {
    return absl::UnavailableError("Binance user stream terminated");
  }
  if (*kind != "executionReport") return Bad("e");

  auto symbol = Text(root, "s");
  auto client = Text(root, "c");
  auto side = Text(root, "S");
  auto execution = Text(root, "x");
  auto raw_status = Text(root, "X");
  auto quantity = Fixed(root, "q");
  auto price = Fixed(root, "p");
  auto cumulative = Fixed(root, "z");
  auto exchange_id = Unsigned(root, "i");
  auto event_time = Unsigned(root, "T");
  if (!symbol.ok()) return symbol.status();
  if (!client.ok()) return client.status();
  if (!side.ok() || (*side != "BUY" && *side != "SELL")) return Bad("S");
  if (!execution.ok()) return execution.status();
  if (!raw_status.ok()) return raw_status.status();
  if (!quantity.ok()) return quantity.status();
  if (!price.ok()) return price.status();
  if (!cumulative.ok() || *cumulative > *quantity) return Bad("z");
  if (!exchange_id.ok()) return exchange_id.status();
  if (!event_time.ok()) return event_time.status();
  auto status = Status(*raw_status);
  if (!status.ok()) return status.status();
  std::string_view original;
  if (!root["C"].get(original) && !original.empty()) client = original;

  Order order;
  order.market = MarketId{std::string(*symbol)};
  order.client_order_id = ClientOrderId{std::string(*client)};
  order.exchange_order_id = ExchangeOrderId{*exchange_id};
  order.side = *side == "BUY" ? Side::Buy : Side::Sell;
  order.price_nanos = *price;
  order.quantity_nanos = *quantity;
  order.remaining_nanos = *quantity - *cumulative;

  if (*execution == "TRADE") {
    auto last_quantity = Fixed(root, "l");
    auto last_price = Fixed(root, "L");
    auto gross = Fixed(root, "Y");
    auto fee = Fixed(root, "n");
    auto trade_id = Unsigned(root, "t");
    if (!last_quantity.ok()) return last_quantity.status();
    if (!last_price.ok()) return last_price.status();
    if (!gross.ok()) return gross.status();
    if (!fee.ok()) return fee.status();
    if (!trade_id.ok()) return trade_id.status();
    std::string_view fee_asset;
    (void)root["N"].get(fee_asset);
    if (*fee > 0 && fee_asset.empty()) return Bad("N");
    result.emplace_back(Fill{{},
                             order.side,
                             *trade_id,
                             *last_price,
                             *last_quantity,
                             *gross,
                             *fee,
                             order.market,
                             order.client_order_id,
                             std::string(fee_asset),
                             *event_time});
  }
  std::string reason;
  std::string_view reject;
  if (!root["r"].get(reject) && reject != "NONE") {
    reason = std::string(reject);
  }
  result.emplace_back(
      OrderReport{std::move(order), *status, std::move(reason), *event_time});
  return result;
}

BinanceUserStream::BinanceUserStream(boost::asio::io_context& io,
                                     const AccountConfig& config,
                                     std::string api_key,
                                     std::string api_secret, EventSink on_event,
                                     HealthSink on_health, TradePerf* perf)
    : io_(io),
      websocket_(io, config.private_ws_host,
                 std::to_string(config.private_ws_port), "/ws-api/v3",
                 TlsConfig{config.tls, true, {}, config.private_ws_host}),
      reconnect_timer_(io),
      api_key_(std::move(api_key)),
      api_secret_(std::move(api_secret)),
      on_event_(std::move(on_event)),
      on_health_(std::move(on_health)),
      perf_(perf) {}

void BinanceUserStream::Start() {
  boost::asio::co_spawn(io_, Run(), boost::asio::detached);
}

void BinanceUserStream::Stop() {
  stopped_ = true;
  reconnect_timer_.cancel();
  websocket_.Cancel();
}

boost::asio::awaitable<void> BinanceUserStream::Run() {
  auto backoff = std::chrono::milliseconds(100);
  while (!stopped_) {
    const auto connected_at = std::chrono::steady_clock::now();
    absl::Status status = co_await RunOnce();
    if (stopped_) break;
    on_health_(false, std::string(status.message()));
    websocket_.Cancel();
    if (absl::IsPermissionDenied(status)) break;
    if (std::chrono::steady_clock::now() - connected_at >
        std::chrono::minutes(1)) {
      backoff = std::chrono::milliseconds(100);
    }
    reconnect_timer_.expires_after(backoff);
    boost::system::error_code error;
    co_await reconnect_timer_.async_wait(
        boost::asio::redirect_error(boost::asio::use_awaitable, error));
    backoff = std::min(backoff * 2, std::chrono::milliseconds(30'000));
  }
}

boost::asio::awaitable<absl::Status> BinanceUserStream::RunOnce() {
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  auto connected = co_await websocket_.Connect(deadline);
  if (!connected.ok()) {
    co_return connected;
  }
  const uint64_t timestamp = BinanceTimestampMs();
  const std::string payload =
      "apiKey=" + api_key_ + "&timestamp=" + std::to_string(timestamp);
  const std::string request =
      "{\"id\":\"hquant-subscribe\",\"method\":"
      "\"userDataStream.subscribe.signature\",\"params\":{\"apiKey\":\"" +
      api_key_ + "\",\"timestamp\":" + std::to_string(timestamp) +
      ",\"signature\":\"" + BinanceHmacSignature(api_secret_, payload) + "\"}}";
  auto sent = co_await websocket_.WriteText(request, deadline);
  if (!sent.ok()) {
    co_return sent;
  }
  auto confirmation = co_await websocket_.Read(deadline);
  if (!confirmation.ok()) {
    co_return confirmation.status();
  }
  simdjson::dom::parser parser;
  simdjson::dom::element root;
  uint64_t status = 0;
  if (parser.parse(*confirmation).get(root) || root["status"].get(status) ||
      status != 200) {
    co_return absl::PermissionDeniedError(
        "private stream subscription rejected");
  }
  on_health_(true, {});
  while (!stopped_) {
    const uint64_t read_started_ns = perf_ ? BenchClockNowNs() : 0;
    uint64_t frame_complete_ns = 0;
    WebSocketReadTrace read_trace;
    auto message = co_await websocket_.Read(
        std::chrono::steady_clock::now() + std::chrono::minutes(30),
        perf_ ? &frame_complete_ns : nullptr, perf_ ? &read_trace : nullptr);
    if (!message.ok()) {
      co_return message.status();
    }
    const uint64_t frame_seq = ++next_frame_seq_;
    auto events = ParseBinanceUserFrame(*message);
    const uint64_t parsed_ns = perf_ ? BenchClockNowNs() : 0;
    if (!events.ok()) {
      co_return events.status();
    }
    uint32_t event_index = 0;
    for (AccountEvent& event : *events) {
      TradePerfSnapshot::PrivateClientTrace trace;
      if (perf_) {
        trace.frame_seq = frame_seq;
        trace.event_index = event_index;
        trace.read_started_ns = read_started_ns;
        trace.read_armed_ns = read_trace.armed_ns;
        trace.first_socket_read_ns = read_trace.first_socket_read_ns;
        trace.last_socket_read_ns = read_trace.last_socket_read_ns;
        trace.socket_reads = read_trace.socket_reads;
        trace.frame_complete_ns = frame_complete_ns;
        trace.parsed_ns = parsed_ns;
        trace.frame_bytes = message->size();
        if (const auto* report = std::get_if<OrderReport>(&event)) {
          trace.kind = "ORDER";
          trace.client_id = report->order.client_order_id.value;
          trace.status = StatusName(report->state);
        } else if (const auto* fill = std::get_if<Fill>(&event)) {
          trace.kind = "FILL";
          trace.client_id = fill->client_order_id.value;
        } else if (const auto* balance = std::get_if<BalanceUpdate>(&event)) {
          trace.kind = "BALANCE";
          trace.client_id = balance->asset;
        } else {
          trace.kind = "DELTA";
        }
        trace.callback_enter_ns = BenchClockNowNs();
      }
      on_event_(std::move(event));
      if (perf_) {
        trace.callback_done_ns = BenchClockNowNs();
        perf_->RecordPrivateClient(std::move(trace));
      }
      ++event_index;
    }
  }
  co_return absl::CancelledError("private stream stopped");
}

}  // namespace hquant::v1
