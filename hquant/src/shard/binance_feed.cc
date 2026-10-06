#include "hquant/shard/binance_feed.h"

#include <algorithm>
#include <cctype>
#include <charconv>
#include <chrono>
#include <cerrno>
#include <limits>
#include <sys/socket.h>
#include <string>
#include <string_view>
#include <utility>

#include "absl/status/status.h"
#include "boost/asio/redirect_error.hpp"
#include "boost/asio/use_awaitable.hpp"
#include "hquant/base/fixed.h"
#include "simdjson.h"

namespace hquant::v1 {
namespace {

absl::Status Invalid(std::string_view message) {
  return absl::InvalidArgumentError(std::string(message));
}

absl::StatusOr<simdjson::dom::element> Root(simdjson::dom::parser& parser,
                                            std::string_view json) {
  simdjson::dom::element root;
  if (parser.parse(json).get(root)) return Invalid("invalid Binance JSON");
  simdjson::dom::element data;
  if (!root["data"].get(data)) return data;
  return root;
}

absl::StatusOr<std::string_view> Text(simdjson::dom::element root,
                                      const char* field) {
  std::string_view value;
  if (root[field].get(value))
    return Invalid(std::string("missing string ") + field);
  return value;
}

absl::StatusOr<uint64_t> Unsigned(simdjson::dom::element root,
                                  const char* field) {
  uint64_t value = 0;
  if (root[field].get(value))
    return Invalid(std::string("missing integer ") + field);
  return value;
}

std::string StreamTarget(std::string symbol) {
  std::transform(symbol.begin(), symbol.end(), symbol.begin(),
                 [](unsigned char value) {
                   return static_cast<char>(std::tolower(value));
                 });
  return "/stream?streams=" + symbol + "@depth/" + symbol + "@trade";
}

}  // namespace

BinanceFeed::BinanceFeed(boost::asio::io_context& io,
                         const MarketConfig& market, const InputConfig& input,
                         InputHandler on_input, ErrorHandler on_error,
                         ShardPerf* perf)
    : market_(market),
      input_(input),
      on_input_(std::move(on_input)),
      on_error_(std::move(on_error)),
      perf_(perf),
      http_(io, input.rest_host, std::to_string(input.rest_port),
            ::hquant::v1::TlsConfig{input.tls, true, {}, input.rest_host}),
      websocket_(
          io, input.websocket_host, std::to_string(input.websocket_port),
          StreamTarget(market.id.symbol),
          ::hquant::v1::TlsConfig{input.tls, true, {}, input.websocket_host}),
      retry_timer_(io) {}

MonoTime BinanceFeed::Now() {
  return std::chrono::time_point_cast<std::chrono::microseconds>(
      std::chrono::steady_clock::now());
}

uint64_t BinanceFeed::ProbeNowNs() const {
  return perf_->bench_ack_fd >= 0 ? BenchClockNowNs() : PerfNowNs();
}

void BinanceFeed::RecordWireTiming(std::string_view message, uint64_t read_ns,
                                   uint64_t parsed_ns, uint64_t done_ns) {
  if (!perf_) return;
  ++perf_->live_wire_messages;
  // Sampling keeps the probe from dominating the 10k/s case.
  if (perf_->live_wire_messages % 10 != 0) return;
  perf_->read_to_parse_samples_ns.push_back(parsed_ns - read_ns);
  perf_->parse_to_market_samples_ns.push_back(done_ns - parsed_ns);
  // Completion-ACK runs use the per-frame packet timestamps. The legacy
  // sender-clock estimate drifts on this host and is not a validity gate.
  if (perf_->bench_ack_fd >= 0) return;
  constexpr std::string_view marker = "\"bench_sent_ns\":";
  const size_t position = message.find(marker);
  if (position == std::string_view::npos) return;
  const char* first = message.data() + position + marker.size();
  uint64_t sent_ns = 0;
  const auto [last, error] =
      std::from_chars(first, message.data() + message.size(), sent_ns);
  if (error != std::errc{} || last == first ||
      read_ns - sent_ns > 60'000'000'000ULL) {
    ++perf_->live_bad_sender_timestamps;
    return;
  }
  constexpr std::string_view schedule_marker = "\"bench_at_us\":";
  const size_t schedule_position = message.find(schedule_marker);
  if (schedule_position == std::string_view::npos) {
    ++perf_->live_bad_sender_timestamps;
    return;
  }
  const char* schedule_first =
      message.data() + schedule_position + schedule_marker.size();
  uint64_t at_us = 0;
  const auto [schedule_last, schedule_error] =
      std::from_chars(schedule_first, message.data() + message.size(), at_us);
  if (schedule_error != std::errc{} || schedule_last == schedule_first ||
      at_us / 1'000'000 > std::numeric_limits<uint32_t>::max()) {
    ++perf_->live_bad_sender_timestamps;
    return;
  }
  perf_->wire_to_read_samples_ns.push_back(read_ns - sent_ns);
  perf_->wire_to_market_samples_ns.push_back(done_ns - sent_ns);
  perf_->live_schedule_seconds.push_back(
      static_cast<uint32_t>(at_us / 1'000'000));
}

void BinanceFeed::SendBenchAck(std::string_view message,
                               uint64_t frame_complete_ns, uint64_t read_ns,
                               uint64_t parsed_ns, uint64_t done_ns,
                               const WebSocketReadTrace& read_trace) {
  if (!perf_ || perf_->bench_ack_fd < 0) return;
  constexpr std::string_view marker = "\"bench_seq\":";
  const size_t position = message.find(marker);
  if (position == std::string_view::npos) return;  // Startup/prewarm frame.
  const char* first = message.data() + position + marker.size();
  uint64_t sequence = 0;
  const auto [last, error] =
      std::from_chars(first, message.data() + message.size(), sequence);
  if (error != std::errc{} || last == first || sequence == 0 ||
      frame_complete_ns > read_ns || read_ns > parsed_ns ||
      parsed_ns > done_ns ||
      (read_trace.socket_reads > 0 &&
       (read_trace.armed_ns > read_trace.first_socket_read_ns ||
        read_trace.first_socket_read_ns > read_trace.last_socket_read_ns ||
        read_trace.last_socket_read_ns > frame_complete_ns))) {
    ++perf_->bench_ack_errors;
    ++perf_->bench_ack_bad_frame;
    return;
  }
  // Fixed network-order packet: original seven fields plus read arm, first
  // and last TCP read callbacks, number of underlying reads, and the last
  // kqueue read event return for this socket when the optional probe runs.
  unsigned char packet[96];
  const uint64_t fields[5] = {sequence, frame_complete_ns, read_ns,
                              parsed_ns, done_ns};
  for (size_t i = 0; i < 5; ++i) {
    for (size_t j = 0; j < 8; ++j) {
      packet[i * 8 + j] = static_cast<unsigned char>(fields[i] >> (56 - j * 8));
    }
  }
  const uint64_t tcp_fields[5] = {
      read_trace.armed_ns, read_trace.first_socket_read_ns,
      read_trace.last_socket_read_ns, read_trace.socket_reads,
      read_trace.kevent_read_ready_ns};
  for (size_t i = 0; i < 5; ++i) {
    for (size_t j = 0; j < 8; ++j) {
      packet[56 + i * 8 + j] =
          static_cast<unsigned char>(tcp_fields[i] >> (56 - j * 8));
    }
  }
  const uint64_t ack_send_ns = BenchClockNowNs();
  const uint64_t ack_send_uptime_ns = BenchUptimeNowNs();
  for (size_t j = 0; j < 8; ++j) {
    packet[40 + j] = static_cast<unsigned char>(ack_send_ns >> (56 - j * 8));
    packet[48 + j] =
        static_cast<unsigned char>(ack_send_uptime_ns >> (56 - j * 8));
  }
  if (::send(perf_->bench_ack_fd, packet, sizeof(packet), MSG_DONTWAIT) ==
      static_cast<ssize_t>(sizeof(packet))) {
    ++perf_->bench_ack_sent;
  } else {
    ++perf_->bench_ack_errors;
    ++perf_->bench_ack_send_errors;
    perf_->bench_ack_last_errno = errno;
  }
}

void BinanceFeed::RecordBenchHandlerPhases(uint64_t parsed_ns,
                                           uint64_t done_ns) {
  if (!perf_ || perf_->bench_ack_fd < 0) return;
  const auto& phases = perf_->live_handler_phases;
  const uint64_t measured_shard =
      phases.paper_ns + phases.account_view_ns + phases.strategy_ns +
      phases.executor_ns + phases.command_history_ns;
  if (done_ns < parsed_ns ||
      phases.market_total_ns > done_ns - parsed_ns ||
      phases.shard_total_ns > phases.market_total_ns ||
      measured_shard > phases.shard_total_ns) {
    ++perf_->live_phase_invalid;
    return;
  }
  perf_->live_feed_dispatch_samples_ns.push_back(
      done_ns - parsed_ns - phases.market_total_ns);
  perf_->live_market_samples_ns.push_back(
      phases.market_total_ns - phases.shard_total_ns);
  perf_->live_paper_samples_ns.push_back(phases.paper_ns);
  perf_->live_account_view_samples_ns.push_back(phases.account_view_ns);
  perf_->live_strategy_samples_ns.push_back(phases.strategy_ns);
  perf_->live_executor_samples_ns.push_back(phases.executor_ns);
  perf_->live_command_history_samples_ns.push_back(phases.command_history_ns);
  perf_->live_shard_other_samples_ns.push_back(
      phases.shard_total_ns - measured_shard);
}

absl::StatusOr<uint64_t> BinanceFeed::PriceTicks(std::string_view text) const {
  auto value = ParseFixed(text);
  if (!value.ok()) return value.status();
  if (*value == 0 || *value % market_.price_per_tick_nanos != 0) {
    return Invalid("price not aligned to price_per_tick");
  }
  return *value / market_.price_per_tick_nanos;
}

absl::StatusOr<uint64_t> BinanceFeed::QuantityLots(
    std::string_view text) const {
  auto value = ParseFixed(text);
  if (!value.ok()) return value.status();
  if (*value % market_.amount_per_lot_nanos != 0) {
    return Invalid("quantity not aligned to amount_per_lot");
  }
  return *value / market_.amount_per_lot_nanos;
}

absl::StatusOr<std::vector<BookLevel>> BinanceFeed::ParseLevels(
    simdjson::dom::element root, const char* key, bool allow_zero) const {
  simdjson::dom::array raw_levels;
  if (root[key].get(raw_levels))
    return Invalid(std::string("missing levels ") + key);
  std::vector<BookLevel> levels;
  levels.reserve(raw_levels.size());
  for (auto raw_level : raw_levels) {
    simdjson::dom::array pair;
    if (raw_level.get(pair) || pair.size() != 2)
      return Invalid("invalid level pair");
    std::string_view price_text;
    std::string_view quantity_text;
    if (pair.at(0).get(price_text) || pair.at(1).get(quantity_text)) {
      return Invalid("level price and quantity must be strings");
    }
    auto price = PriceTicks(price_text);
    auto quantity = QuantityLots(quantity_text);
    if (!price.ok()) return price.status();
    if (!quantity.ok()) return quantity.status();
    if (!allow_zero && *quantity == 0)
      return Invalid("zero quantity in snapshot");
    levels.push_back(BookLevel{*price, *quantity});
  }
  return levels;
}

absl::StatusOr<BookSnapshot> BinanceFeed::ParseSnapshot(
    std::string_view json) const {
  simdjson::dom::parser parser;
  auto root = Root(parser, json);
  if (!root.ok()) return root.status();
  auto sequence = Unsigned(*root, "lastUpdateId");
  if (!sequence.ok() || *sequence == 0 ||
      *sequence == std::numeric_limits<uint64_t>::max()) {
    return Invalid("invalid snapshot sequence");
  }
  auto bids = ParseLevels(*root, "bids", false);
  auto asks = ParseLevels(*root, "asks", false);
  if (!bids.ok()) return bids.status();
  if (!asks.ok()) return asks.status();
  return BookSnapshot{market_.id, epoch_, *sequence, std::move(*bids),
                      std::move(*asks)};
}

absl::StatusOr<BookDiff> BinanceFeed::ParseDiff(
    simdjson::dom::element root) const {
  auto kind = Text(root, "e");
  auto symbol = Text(root, "s");
  auto first = Unsigned(root, "U");
  auto last = Unsigned(root, "u");
  if (!kind.ok() || *kind != "depthUpdate" || !symbol.ok() ||
      *symbol != market_.id.symbol || !first.ok() || !last.ok() ||
      *first == 0 || *first > *last ||
      *last == std::numeric_limits<uint64_t>::max()) {
    return Invalid("invalid depth event");
  }
  auto bids = ParseLevels(root, "b", true);
  auto asks = ParseLevels(root, "a", true);
  if (!bids.ok()) return bids.status();
  if (!asks.ok()) return asks.status();
  return BookDiff{market_.id, epoch_,           *first,
                  *last,      std::move(*bids), std::move(*asks)};
}

absl::StatusOr<PublicTrade> BinanceFeed::ParseTrade(
    simdjson::dom::element root) const {
  auto kind = Text(root, "e");
  auto symbol = Text(root, "s");
  if (!kind.ok() || (*kind != "trade" && *kind != "aggTrade") || !symbol.ok() ||
      *symbol != market_.id.symbol) {
    return Invalid("invalid trade event");
  }
  auto price_text = Text(root, "p");
  auto quantity_text = Text(root, "q");
  if (!price_text.ok()) return price_text.status();
  if (!quantity_text.ok()) return quantity_text.status();
  auto price = PriceTicks(*price_text);
  auto quantity = QuantityLots(*quantity_text);
  auto id = Unsigned(root, *kind == "trade" ? "t" : "a");
  bool buyer_maker = false;
  if (!price.ok()) return price.status();
  if (!quantity.ok() || *quantity == 0)
    return Invalid("invalid trade quantity");
  if (!id.ok() || root["m"].get(buyer_maker)) {
    return Invalid("invalid trade id or side");
  }
  return PublicTrade{market_.id, epoch_,
                     *id,        *price,
                     *quantity,  buyer_maker ? Side::Sell : Side::Buy};
}

absl::StatusOr<std::string> BinanceFeed::EventKind(
    simdjson::dom::element root) const {
  std::string_view kind;
  if (root["e"].get(kind)) {
    simdjson::dom::element acknowledgement;
    if (!root["result"].get(acknowledgement)) return std::string{};
    return Invalid("WebSocket event type missing");
  }
  return std::string(kind);
}

absl::Status BinanceFeed::Fault(absl::Status status) {
  websocket_.Cancel();
  on_input_(FeedDisconnected{epoch_, std::string(status.message())}, Now());
  return status;
}

boost::asio::awaitable<absl::Status> BinanceFeed::RunCycle() {
  if (stopped_) co_return absl::CancelledError("market stopped");
  retry_after_ = std::chrono::steady_clock::duration::zero();
  if (epoch_ == std::numeric_limits<uint64_t>::max()) {
    co_return absl::OutOfRangeError("connection epoch exhausted");
  }
  ++epoch_;
  on_input_(FeedConnected{epoch_}, Now());
  auto connected = co_await websocket_.Reconnect(
      std::chrono::steady_clock::now() + std::chrono::seconds(5));
  if (!connected.ok()) co_return Fault(connected);

  uint64_t first_update = 0;
  while (!stopped_ && first_update == 0) {
    uint64_t frame_complete_ns = 0;
    WebSocketReadTrace read_trace;
    auto message = co_await websocket_.Read(std::chrono::steady_clock::now() +
                                            std::chrono::seconds(30),
        perf_ && perf_->bench_ack_fd >= 0 ? &frame_complete_ns : nullptr,
        perf_ && perf_->bench_ack_fd >= 0 ? &read_trace : nullptr);
    if (!message.ok()) co_return Fault(message.status());
    const uint64_t read_ns = perf_ ? ProbeNowNs() : 0;
    simdjson::dom::parser parser;
    auto root = Root(parser, *message);
    if (!root.ok()) co_return Fault(root.status());
    auto kind = EventKind(*root);
    if (!kind.ok()) co_return Fault(kind.status());
    if (kind->empty()) continue;
    if (*kind == "trade" || *kind == "aggTrade") {
      auto trade = ParseTrade(*root);
      if (!trade.ok()) co_return Fault(trade.status());
      const uint64_t parsed_ns = perf_ ? ProbeNowNs() : 0;
      if (perf_ && perf_->bench_ack_fd >= 0) perf_->live_handler_phases = {};
      on_input_(*trade, Now());
      if (perf_) {
        const uint64_t done_ns = ProbeNowNs();
        RecordBenchHandlerPhases(parsed_ns, done_ns);
        SendBenchAck(*message, frame_complete_ns, read_ns, parsed_ns, done_ns,
                     read_trace);
        RecordWireTiming(*message, read_ns, parsed_ns, done_ns);
      }
      continue;
    }
    if (*kind != "depthUpdate")
      co_return Fault(Invalid("unexpected WebSocket event"));
    auto diff = ParseDiff(*root);
    if (!diff.ok()) co_return Fault(diff.status());
    first_update = diff->first_sequence;
    const uint64_t parsed_ns = perf_ ? ProbeNowNs() : 0;
    if (perf_ && perf_->bench_ack_fd >= 0) perf_->live_handler_phases = {};
    const bool resync = on_input_(*diff, Now());
    if (perf_) {
      const uint64_t done_ns = ProbeNowNs();
      RecordBenchHandlerPhases(parsed_ns, done_ns);
      SendBenchAck(*message, frame_complete_ns, read_ns, parsed_ns, done_ns,
                   read_trace);
      RecordWireTiming(*message, read_ns, parsed_ns, done_ns);
    }
    if (resync)
      co_return Fault(Invalid("depth buffer requires resync"));
  }
  if (stopped_) co_return absl::CancelledError("market stopped");

  bool accepted_snapshot = false;
  for (int attempt = 0; attempt < 3 && !stopped_; ++attempt) {
    ::hquant::v1::HttpRequest request;
    request.method = "GET";
    request.target =
        "/api/v3/depth?symbol=" + market_.id.symbol + "&limit=1000";
    request.deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    auto response = co_await http_.Send(std::move(request));
    if (!response.ok()) co_return Fault(response.status());
    if (response->status == 429 || response->status == 418) {
      retry_after_ = std::max<std::chrono::steady_clock::duration>(
          response->retry_after, response->status == 418
                                     ? std::chrono::minutes(5)
                                     : std::chrono::minutes(1));
      co_return Fault(
          absl::ResourceExhaustedError("Binance snapshot request throttled"));
    }
    if (response->status != 200) {
      co_return Fault(absl::UnavailableError("Binance snapshot HTTP status " +
                                             std::to_string(response->status)));
    }
    auto snapshot = ParseSnapshot(response->body);
    if (!snapshot.ok()) co_return Fault(snapshot.status());
    if (snapshot->last_sequence + 1 < first_update) continue;
    if (on_input_(*snapshot, Now()))
      co_return Fault(Invalid("snapshot requires resync"));
    accepted_snapshot = true;
    break;
  }
  if (!accepted_snapshot)
    co_return Fault(Invalid("snapshot older than depth stream"));

  while (!stopped_) {
    uint64_t frame_complete_ns = 0;
    WebSocketReadTrace read_trace;
    auto message = co_await websocket_.Read(std::chrono::steady_clock::now() +
                                            std::chrono::seconds(30),
        perf_ && perf_->bench_ack_fd >= 0 ? &frame_complete_ns : nullptr,
        perf_ && perf_->bench_ack_fd >= 0 ? &read_trace : nullptr);
    if (!message.ok()) co_return Fault(message.status());
    const uint64_t read_ns = perf_ ? ProbeNowNs() : 0;
    simdjson::dom::parser parser;
    auto root = Root(parser, *message);
    if (!root.ok()) co_return Fault(root.status());
    auto kind = EventKind(*root);
    if (!kind.ok()) co_return Fault(kind.status());
    if (kind->empty()) continue;
    if (*kind == "trade" || *kind == "aggTrade") {
      auto trade = ParseTrade(*root);
      if (!trade.ok()) co_return Fault(trade.status());
      const uint64_t parsed_ns = perf_ ? ProbeNowNs() : 0;
      if (perf_ && perf_->bench_ack_fd >= 0) perf_->live_handler_phases = {};
      on_input_(*trade, Now());
      if (perf_) {
        const uint64_t done_ns = ProbeNowNs();
        RecordBenchHandlerPhases(parsed_ns, done_ns);
        SendBenchAck(*message, frame_complete_ns, read_ns, parsed_ns, done_ns,
                     read_trace);
        RecordWireTiming(*message, read_ns, parsed_ns, done_ns);
      }
    } else if (*kind == "depthUpdate") {
      auto diff = ParseDiff(*root);
      if (!diff.ok()) co_return Fault(diff.status());
      const uint64_t parsed_ns = perf_ ? ProbeNowNs() : 0;
      if (perf_ && perf_->bench_ack_fd >= 0) perf_->live_handler_phases = {};
      const bool resync = on_input_(*diff, Now());
      if (perf_) {
        const uint64_t done_ns = ProbeNowNs();
        RecordBenchHandlerPhases(parsed_ns, done_ns);
        SendBenchAck(*message, frame_complete_ns, read_ns, parsed_ns, done_ns,
                     read_trace);
        RecordWireTiming(*message, read_ns, parsed_ns, done_ns);
      }
      if (resync)
        co_return Fault(Invalid("depth sequence requires resync"));
    } else {
      co_return Fault(Invalid("unexpected WebSocket event"));
    }
  }
  co_return absl::OkStatus();
}

boost::asio::awaitable<void> BinanceFeed::Run() {
  auto delay = std::chrono::milliseconds(250);
  while (!stopped_) {
    auto result = co_await RunCycle();
    if (stopped_) break;
    if (!result.ok()) on_error_(std::string(result.message()));
    retry_timer_.expires_after(
        std::max<std::chrono::steady_clock::duration>(delay, retry_after_));
    boost::system::error_code error;
    co_await retry_timer_.async_wait(
        boost::asio::redirect_error(boost::asio::use_awaitable, error));
    if (error || stopped_) break;
    delay = std::min(delay * 2,
                     std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::seconds(30)));
  }
}

void BinanceFeed::RequestResync() { websocket_.Cancel(); }

void BinanceFeed::Stop() {
  stopped_ = true;
  websocket_.Cancel();
  http_.Cancel();
  retry_timer_.cancel();
}

}  // namespace hquant::v1
