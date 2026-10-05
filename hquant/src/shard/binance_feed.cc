#include "hquant/shard/binance_feed.h"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <limits>
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
                         InputHandler on_input, ErrorHandler on_error)
    : market_(market),
      input_(input),
      on_input_(std::move(on_input)),
      on_error_(std::move(on_error)),
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
    std::string_view json, const char* key, bool allow_zero) const {
  simdjson::dom::parser parser;
  auto root = Root(parser, json);
  if (!root.ok()) return root.status();
  simdjson::dom::array raw_levels;
  if ((*root)[key].get(raw_levels))
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
  auto bids = ParseLevels(json, "bids", false);
  auto asks = ParseLevels(json, "asks", false);
  if (!bids.ok()) return bids.status();
  if (!asks.ok()) return asks.status();
  return BookSnapshot{market_.id, epoch_, *sequence, std::move(*bids),
                      std::move(*asks)};
}

absl::StatusOr<BookDiff> BinanceFeed::ParseDiff(std::string_view json) const {
  simdjson::dom::parser parser;
  auto root = Root(parser, json);
  if (!root.ok()) return root.status();
  auto kind = Text(*root, "e");
  auto symbol = Text(*root, "s");
  auto first = Unsigned(*root, "U");
  auto last = Unsigned(*root, "u");
  if (!kind.ok() || *kind != "depthUpdate" || !symbol.ok() ||
      *symbol != market_.id.symbol || !first.ok() || !last.ok() ||
      *first == 0 || *first > *last ||
      *last == std::numeric_limits<uint64_t>::max()) {
    return Invalid("invalid depth event");
  }
  auto bids = ParseLevels(json, "b", true);
  auto asks = ParseLevels(json, "a", true);
  if (!bids.ok()) return bids.status();
  if (!asks.ok()) return asks.status();
  return BookDiff{market_.id, epoch_,           *first,
                  *last,      std::move(*bids), std::move(*asks)};
}

absl::StatusOr<PublicTrade> BinanceFeed::ParseTrade(
    std::string_view json) const {
  simdjson::dom::parser parser;
  auto root = Root(parser, json);
  if (!root.ok()) return root.status();
  auto kind = Text(*root, "e");
  auto symbol = Text(*root, "s");
  if (!kind.ok() || (*kind != "trade" && *kind != "aggTrade") || !symbol.ok() ||
      *symbol != market_.id.symbol) {
    return Invalid("invalid trade event");
  }
  auto price_text = Text(*root, "p");
  auto quantity_text = Text(*root, "q");
  if (!price_text.ok()) return price_text.status();
  if (!quantity_text.ok()) return quantity_text.status();
  auto price = PriceTicks(*price_text);
  auto quantity = QuantityLots(*quantity_text);
  auto id = Unsigned(*root, *kind == "trade" ? "t" : "a");
  bool buyer_maker = false;
  if (!price.ok()) return price.status();
  if (!quantity.ok() || *quantity == 0)
    return Invalid("invalid trade quantity");
  if (!id.ok() || (*root)["m"].get(buyer_maker)) {
    return Invalid("invalid trade id or side");
  }
  return PublicTrade{market_.id, epoch_,
                     *id,        *price,
                     *quantity,  buyer_maker ? Side::Sell : Side::Buy};
}

absl::StatusOr<std::string> BinanceFeed::EventKind(
    std::string_view json) const {
  simdjson::dom::parser parser;
  auto root = Root(parser, json);
  if (!root.ok()) return root.status();
  std::string_view kind;
  if ((*root)["e"].get(kind)) {
    simdjson::dom::element acknowledgement;
    if (!(*root)["result"].get(acknowledgement)) return std::string{};
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
    auto message = co_await websocket_.Read(std::chrono::steady_clock::now() +
                                            std::chrono::seconds(30));
    if (!message.ok()) co_return Fault(message.status());
    auto kind = EventKind(*message);
    if (!kind.ok()) co_return Fault(kind.status());
    if (kind->empty()) continue;
    if (*kind == "trade" || *kind == "aggTrade") {
      auto trade = ParseTrade(*message);
      if (!trade.ok()) co_return Fault(trade.status());
      on_input_(*trade, Now());
      continue;
    }
    if (*kind != "depthUpdate")
      co_return Fault(Invalid("unexpected WebSocket event"));
    auto diff = ParseDiff(*message);
    if (!diff.ok()) co_return Fault(diff.status());
    first_update = diff->first_sequence;
    if (on_input_(*diff, Now()))
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
    auto message = co_await websocket_.Read(std::chrono::steady_clock::now() +
                                            std::chrono::seconds(30));
    if (!message.ok()) co_return Fault(message.status());
    auto kind = EventKind(*message);
    if (!kind.ok()) co_return Fault(kind.status());
    if (kind->empty()) continue;
    if (*kind == "trade" || *kind == "aggTrade") {
      auto trade = ParseTrade(*message);
      if (!trade.ok()) co_return Fault(trade.status());
      on_input_(*trade, Now());
    } else if (*kind == "depthUpdate") {
      auto diff = ParseDiff(*message);
      if (!diff.ok()) co_return Fault(diff.status());
      if (on_input_(*diff, Now()))
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
