#include "market/binance_spot_feed.h"

#include <algorithm>
#include <charconv>
#include <cstdint>
#include <limits>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "base/error.h"
#include "base/types.h"
#include "simdjson.h"

namespace hquant::binance_spot {
namespace {

absl::Status Invalid(std::string_view reason) {
  return Error(ErrorCode::kFeedMessageInvalid, reason);
}

absl::StatusOr<std::string_view> Text(simdjson::dom::element object,
                                      const char* key) {
  std::string_view value;
  if (object[key].get(value))
    return Invalid(std::string("missing string ") + key);
  return value;
}

absl::StatusOr<uint64_t> Unsigned(simdjson::dom::element object,
                                  const char* key) {
  uint64_t value;
  if (object[key].get(value))
    return Invalid(std::string("missing unsigned ") + key);
  return value;
}

absl::StatusOr<simdjson::dom::element> ParseRoot(simdjson::dom::parser* parser,
                                                 std::string_view json) {
  simdjson::dom::element root;
  if (parser->parse(json).get(root)) return Invalid("invalid JSON");
  simdjson::dom::element data;
  if (!root["data"].get(data)) return data;
  return root;
}

// Decimal::ToString may use exponent notation after normalization. Parse it as
// an exact integer without any binary floating-point conversion.
absl::StatusOr<uint64_t> IntegerDecimal(std::string_view text) {
  if (text.empty() || text[0] == '-')
    return Invalid("negative integer decimal");
  const size_t exponent_at = text.find_first_of("eE");
  const auto mantissa = text.substr(0, exponent_at);
  int exponent = 0;
  if (exponent_at != std::string_view::npos) {
    auto exp = text.substr(exponent_at + 1);
    if (!exp.empty() && exp[0] == '+') exp.remove_prefix(1);
    const auto [end, ec] =
        std::from_chars(exp.data(), exp.data() + exp.size(), exponent);
    if (ec != std::errc{} || end != exp.data() + exp.size()) {
      return Invalid("invalid integer exponent");
    }
  }
  std::string digits;
  int fractional = 0;
  bool point = false;
  for (char character : mantissa) {
    if (character == '.') {
      if (point) return Invalid("invalid integer decimal");
      point = true;
    } else if (character >= '0' && character <= '9') {
      digits += character;
      if (point) ++fractional;
    } else
      return Invalid("invalid integer decimal");
  }
  if (digits.empty()) return Invalid("empty integer decimal");
  int shift = exponent - fractional;
  while (shift < 0 && !digits.empty() && digits.back() == '0') {
    digits.pop_back();
    ++shift;
  }
  if (shift < 0) return Invalid("non-integral scale quotient");
  if (shift > 20)
    return Error(ErrorCode::kFeedTickSizeMismatch,
                 "scale quotient overflows uint64");
  uint64_t value = 0;
  for (char digit : digits) {
    const unsigned d = digit - '0';
    if (value > (std::numeric_limits<uint64_t>::max() - d) / 10) {
      return Error(ErrorCode::kFeedTickSizeMismatch,
                   "scale quotient overflows uint64");
    }
    value = value * 10 + d;
  }
  for (int i = 0; i < shift; ++i) {
    if (value > std::numeric_limits<uint64_t>::max() / 10) {
      return Error(ErrorCode::kFeedTickSizeMismatch,
                   "scale quotient overflows uint64");
    }
    value *= 10;
  }
  return value;
}

absl::StatusOr<uint64_t> ToUnits(std::string_view raw, const Decimal& quantum) {
  auto value = Decimal::Parse(raw);
  if (!value.ok())
    return Error(ErrorCode::kFeedMessageInvalid, value.status().message());
  if (!value->IsNonnegative()) return Invalid("negative level");
  auto rounded = value->Quantize(quantum, RoundingMode::Down);
  if (!rounded.ok())
    return Error(ErrorCode::kFeedTickSizeMismatch, rounded.status().message());
  auto exact = rounded->Compare(*value);
  if (!exact.ok())
    return Error(ErrorCode::kFeedTickSizeMismatch, exact.status().message());
  if (*exact != 0)
    return Error(ErrorCode::kFeedTickSizeMismatch,
                 "raw level not divisible by BookScale");
  auto quotient = value->Divide(quantum);
  if (!quotient.ok())
    return Error(ErrorCode::kFeedTickSizeMismatch, quotient.status().message());
  return IntegerDecimal(quotient->ToString());
}

absl::StatusOr<std::vector<BookLevel>> Levels(simdjson::dom::element object,
                                              const char* key,
                                              const BookScale& scale,
                                              bool allow_zero_quantity) {
  simdjson::dom::array raw_levels;
  if (object[key].get(raw_levels))
    return Invalid(std::string("missing levels ") + key);
  std::vector<BookLevel> levels;
  levels.reserve(raw_levels.size());
  for (auto raw_level : raw_levels) {
    simdjson::dom::array pair;
    if (raw_level.get(pair) || pair.size() != 2)
      return Invalid("invalid level pair");
    std::string_view price_text, quantity_text;
    if (pair.at(0).get(price_text) || pair.at(1).get(quantity_text)) {
      return Invalid("level price and quantity must be strings");
    }
    auto price = ToUnits(price_text, scale.quote_per_tick);
    auto quantity = ToUnits(quantity_text, scale.base_per_lot);
    if (!price.ok()) return price.status();
    if (!quantity.ok()) return quantity.status();
    if (*price == 0 || *price > uint64_t(std::numeric_limits<int64_t>::max()) ||
        (!allow_zero_quantity && *quantity == 0)) {
      return Invalid("level price or quantity out of range");
    }
    levels.push_back(
        BookLevel{PriceTicks{int64_t(*price)}, QuantityLots{*quantity}});
  }
  return levels;
}

void ExchangeEventTime(simdjson::dom::element object, EventTime* time) {
  uint64_t millis;
  if (!object["E"].get(millis) &&
      millis <= uint64_t(std::numeric_limits<int64_t>::max() / 1000)) {
    time->exchange_utc = UtcTime(std::chrono::microseconds(millis * 1000));
  }
}

}  // namespace

absl::StatusOr<BookSnapshot> DepthParser::ParseSnapshot(
    std::string_view json, uint64_t stream_epoch, EventTime received) const {
  if (!scale_.IsValid() || stream_epoch == 0)
    return Error(ErrorCode::kFeedConfigInvalid, "invalid BookScale or epoch");
  simdjson::dom::parser parser;
  auto parsed = ParseRoot(&parser, json);
  if (!parsed.ok()) return parsed.status();
  auto sequence = Unsigned(*parsed, "lastUpdateId");
  if (!sequence.ok() || *sequence == 0)
    return Invalid("invalid snapshot sequence");
  auto bids = Levels(*parsed, "bids", scale_, false);
  auto asks = Levels(*parsed, "asks", scale_, false);
  if (!bids.ok()) return bids.status();
  if (!asks.ok()) return asks.status();
  BookSnapshot snapshot;
  snapshot.market = market_;
  snapshot.scale_version = scale_.scale_version;
  snapshot.stream_epoch = stream_epoch;
  snapshot.last_sequence = *sequence;
  snapshot.bids = std::move(*bids);
  snapshot.asks = std::move(*asks);
  snapshot.time = received;
  return snapshot;
}

absl::StatusOr<BookDiff> DepthParser::ParseDiff(std::string_view json,
                                                uint64_t stream_epoch,
                                                EventTime received) const {
  if (!scale_.IsValid() || stream_epoch == 0)
    return Error(ErrorCode::kFeedConfigInvalid, "invalid BookScale or epoch");
  simdjson::dom::parser parser;
  auto parsed = ParseRoot(&parser, json);
  if (!parsed.ok()) return parsed.status();
  auto type = Text(*parsed, "e");
  auto symbol = Text(*parsed, "s");
  if (!type.ok() || *type != "depthUpdate" || !symbol.ok() ||
      *symbol != market_.native_symbol) {
    return Error(ErrorCode::kFeedWrongMarket,
                 "not a depthUpdate for configured market");
  }
  auto first = Unsigned(*parsed, "U");
  auto last = Unsigned(*parsed, "u");
  if (!first.ok() || !last.ok() || *first == 0 || *first > *last) {
    return Invalid("invalid depth interval");
  }
  auto bids = Levels(*parsed, "b", scale_, true);
  auto asks = Levels(*parsed, "a", scale_, true);
  if (!bids.ok()) return bids.status();
  if (!asks.ok()) return asks.status();
  ExchangeEventTime(*parsed, &received);
  BookDiff diff;
  diff.market = market_;
  diff.scale_version = scale_.scale_version;
  diff.stream_epoch = stream_epoch;
  diff.first_sequence = *first;
  diff.last_sequence = *last;
  diff.bids = std::move(*bids);
  diff.asks = std::move(*asks);
  diff.time = received;
  return diff;
}

absl::StatusOr<PublicTrade> DepthParser::ParseTrade(std::string_view json,
                                                    EventTime received) const {
  if (!scale_.IsValid())
    return Error(ErrorCode::kFeedConfigInvalid, "invalid BookScale");
  simdjson::dom::parser parser;
  auto parsed = ParseRoot(&parser, json);
  if (!parsed.ok()) return parsed.status();
  auto type = Text(*parsed, "e");
  auto symbol = Text(*parsed, "s");
  if (!type.ok() || (*type != "trade" && *type != "aggTrade") || !symbol.ok() ||
      *symbol != market_.native_symbol) {
    return Error(ErrorCode::kFeedWrongMarket,
                 "not a trade for configured market");
  }
  auto price_text = Text(*parsed, "p");
  auto quantity_text = Text(*parsed, "q");
  if (!price_text.ok()) return price_text.status();
  if (!quantity_text.ok()) return quantity_text.status();
  auto price = ToUnits(*price_text, scale_.quote_per_tick);
  auto quantity = ToUnits(*quantity_text, scale_.base_per_lot);
  if (!price.ok()) return price.status();
  if (!quantity.ok()) return quantity.status();
  if (*price == 0 || *price > uint64_t(std::numeric_limits<int64_t>::max()) ||
      *quantity == 0)
    return Invalid("invalid trade price or quantity");
  bool buyer_maker = false;
  if ((*parsed)["m"].get(buyer_maker))
    return Invalid("missing trade maker flag");
  auto trade_id = Unsigned(*parsed, *type == "trade" ? "t" : "a");
  if (!trade_id.ok()) return trade_id.status();
  ExchangeEventTime(*parsed, &received);
  PublicTrade trade;
  trade.market = market_;
  trade.public_trade_id = std::to_string(*trade_id);
  trade.price_ticks = PriceTicks{int64_t(*price)};
  trade.quantity_lots = QuantityLots{*quantity};
  trade.side = buyer_maker ? Side::Sell : Side::Buy;
  trade.time = received;
  return trade;
}

}  // namespace hquant::binance_spot

#include "boost/asio.hpp"
#include "market/order_book.h"

namespace hquant::binance_spot {
namespace {

absl::StatusOr<std::string> EventKind(std::string_view json) {
  simdjson::dom::parser parser;
  simdjson::dom::element root;
  if (parser.parse(json).get(root)) {
    return Error(ErrorCode::kFeedMessageInvalid, "invalid WebSocket JSON");
  }
  simdjson::dom::element data;
  if (!root["data"].get(data)) root = data;
  std::string_view kind;
  if (root["e"].get(kind)) {
    simdjson::dom::element acknowledgement;
    if (!root["result"].get(acknowledgement)) return std::string{};
    return Error(ErrorCode::kFeedMessageInvalid,
                 "WebSocket message lacks event type");
  }
  return std::string(kind);
}

}  // namespace

MarketDataStream::MarketDataStream(StreamConfig config, DepthParser parser,
                                   HttpTransport& snapshot_http,
                                   WebSocketClient& websocket, BookSync& book,
                                   const Clock& clock,
                                   StreamCallbacks callbacks)
    : config_(std::move(config)),
      parser_(std::move(parser)),
      snapshot_http_(snapshot_http),
      websocket_(websocket),
      book_(book),
      clock_(clock),
      callbacks_(std::move(callbacks)) {}

EventTime MarketDataStream::Now() const {
  return EventTime{{}, clock_.UtcNow(), clock_.MonoNow()};
}

void MarketDataStream::Report(const BookApplyResult& result) const {
  if (callbacks_.on_book) callbacks_.on_book(result);
}

absl::Status MarketDataStream::Fault(absl::Status status) {
  websocket_.Cancel();
  if (book_.View().State() != BookSyncState::Resyncing) {
    Report(book_.OnDisconnect());
  }
  if (callbacks_.on_error) callbacks_.on_error(status);
  return status;
}

boost::asio::awaitable<absl::Status> MarketDataStream::RunCycle(
    size_t max_depth_messages) {
  if (stopped_) co_return Error(ErrorCode::kFeedStopped, "market stream stopped");
  cycle_became_live_ = false;
  retry_after_ = std::chrono::steady_clock::duration::zero();
  if (epoch_ == std::numeric_limits<uint64_t>::max()) {
    co_return Fault(
        Error(ErrorCode::kSequenceExhausted, "stream epoch exhausted"));
  }
  if (config_.symbol.empty() ||
      config_.symbol != parser_.Market().native_symbol ||
      config_.snapshot_limit == 0 || config_.snapshot_limit > 5000 ||
      config_.max_snapshot_attempts == 0 ||
      config_.reconnect_delay <= std::chrono::steady_clock::duration::zero() ||
      config_.max_reconnect_delay < config_.reconnect_delay) {
    co_return Error(ErrorCode::kFeedConfigInvalid,
                    "invalid Binance stream config");
  }
  ++epoch_;
  Report(book_.Subscribe(epoch_));
  auto connected = co_await websocket_.Reconnect(
      std::chrono::steady_clock::now() + config_.connect_timeout);
  if (!connected.ok()) co_return Fault(connected);

  size_t depth_count = 0;
  uint64_t first_update = 0;
  while (!stopped_ && first_update == 0) {
    auto text = co_await websocket_.Read(std::chrono::steady_clock::now() +
                                         config_.read_timeout);
    if (!text.ok()) co_return Fault(text.status());
    const auto kind = EventKind(*text);
    if (!kind.ok()) co_return Fault(kind.status());
    if (*kind == "trade" || *kind == "aggTrade") {
      auto trade = parser_.ParseTrade(*text, Now());
      if (!trade.ok()) co_return Fault(trade.status());
      if (callbacks_.on_trade) callbacks_.on_trade(*trade);
      continue;
    }
    if (kind->empty()) continue;  // e.g. subscription acknowledgement
    auto diff = parser_.ParseDiff(*text, epoch_, Now());
    if (!diff.ok()) co_return Fault(diff.status());
    first_update = diff->first_sequence;
    auto result = book_.OnDiff(*diff);
    Report(result);
    if (result.state == BookSyncState::Resyncing) {
      co_return Fault(Error(result.reason, "buffered depth invalid"));
    }
    ++depth_count;
  }
  if (stopped_)
    co_return Fault(Error(ErrorCode::kFeedStopped, "market stream stopped"));

  bool snapshot_applied = false;
  for (unsigned attempt = 0; attempt < config_.max_snapshot_attempts;
       ++attempt) {
    HttpRequest request;
    request.method = "GET";
    request.target = "/api/v3/depth?symbol=" + config_.symbol +
                     "&limit=" + std::to_string(config_.snapshot_limit);
    request.deadline =
        std::chrono::steady_clock::now() + config_.snapshot_timeout;
    auto response = co_await snapshot_http_.Send(std::move(request));
    if (!response.ok()) co_return Fault(response.status());
    if (response->status == 429 || response->status == 418) {
      retry_after_ = response->retry_after;
      co_return Fault(Error(response->status == 418
                                ? ErrorCode::kExchangeIpBanned
                                : ErrorCode::kExchangeRateLimited,
                            "Binance depth snapshot throttled"));
    }
    if (response->status != 200) {
      co_return Fault(Error(ErrorCode::kFeedSnapshotHttpError,
                            "Binance depth snapshot HTTP status " +
                                std::to_string(response->status)));
    }
    auto snapshot = parser_.ParseSnapshot(response->body, epoch_, Now());
    if (!snapshot.ok()) co_return Fault(snapshot.status());
    if (snapshot->last_sequence < first_update) continue;
    auto result = book_.OnSnapshot(*snapshot);
    if (result.state == BookSyncState::Live) cycle_became_live_ = true;
    Report(result);
    if (result.state == BookSyncState::Resyncing) {
      co_return Fault(Error(result.reason, "snapshot replay failed"));
    }
    snapshot_applied = true;
    break;
  }
  if (!snapshot_applied) {
    co_return Fault(Error(ErrorCode::kBookSequenceGap,
                          "snapshot remains older than first WS update"));
  }

  while (!stopped_ &&
         (max_depth_messages == 0 || depth_count < max_depth_messages)) {
    auto text = co_await websocket_.Read(std::chrono::steady_clock::now() +
                                         config_.read_timeout);
    if (!text.ok()) co_return Fault(text.status());
    const auto kind = EventKind(*text);
    if (!kind.ok()) co_return Fault(kind.status());
    if (*kind == "trade" || *kind == "aggTrade") {
      auto trade = parser_.ParseTrade(*text, Now());
      if (!trade.ok()) co_return Fault(trade.status());
      if (callbacks_.on_trade) callbacks_.on_trade(*trade);
      continue;
    }
    if (kind->empty()) continue;
    auto diff = parser_.ParseDiff(*text, epoch_, Now());
    if (!diff.ok()) co_return Fault(diff.status());
    auto result = book_.OnDiff(*diff);
    if (result.state == BookSyncState::Live) cycle_became_live_ = true;
    Report(result);
    if (result.state == BookSyncState::Resyncing) {
      co_return Fault(Error(result.reason, "depth sequence or book invalid"));
    }
    ++depth_count;
  }
  if (stopped_)
    co_return Fault(Error(ErrorCode::kFeedStopped, "market stream stopped"));
  co_return absl::OkStatus();
}

boost::asio::awaitable<void> MarketDataStream::Run() {
  auto executor = co_await boost::asio::this_coro::executor;
  boost::asio::steady_timer timer(executor);
  auto retry_delay = config_.reconnect_delay;
  unsigned consecutive_resyncs = 0;
  unsigned consecutive_scale_mismatches = 0;
  while (!stopped_) {
    auto result = co_await RunCycle();
    if (stopped_) break;
    if (result.ok()) continue;
    const Recovery recovery = RecoveryOf(result);
    if (recovery != Recovery::Retry && recovery != Recovery::Resync) break;
    if (cycle_became_live_) {
      retry_delay = config_.reconnect_delay;
      consecutive_resyncs = 0;
    }
    if (recovery == Recovery::Resync) {
      ++consecutive_resyncs;
      consecutive_scale_mismatches =
          CodeOf(result) == ErrorCode::kFeedTickSizeMismatch
              ? consecutive_scale_mismatches + 1
              : 0;
      if (consecutive_scale_mismatches >= 3) {
        if (callbacks_.on_error)
          callbacks_.on_error(Error(ErrorCode::kFeedConfigInvalid,
                                    "repeated BookScale mismatch"));
        break;
      }
      if (consecutive_resyncs >= 8) break;
    }
    timer.expires_after(std::max(retry_delay, retry_after_));
    boost::system::error_code ec;
    co_await timer.async_wait(
        boost::asio::redirect_error(boost::asio::use_awaitable, ec));
    if (retry_delay < config_.max_reconnect_delay) {
      retry_delay = retry_delay >= config_.max_reconnect_delay / 2
                        ? config_.max_reconnect_delay
                        : retry_delay * 2;
    }
  }
}

void MarketDataStream::Stop() {
  stopped_ = true;
  websocket_.Cancel();
}

}  // namespace hquant::binance_spot
