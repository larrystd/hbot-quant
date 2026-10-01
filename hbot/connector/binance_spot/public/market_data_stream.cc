#include "hbot/connector/binance_spot/public/market_data_stream.h"

#include <limits>
#include <string>
#include <utility>

#include "absl/status/status.h"
#include "boost/asio.hpp"
#include "hbot/market_data/book_view.h"
#include "simdjson.h"

namespace hbot::binance_spot {
namespace {

absl::StatusOr<std::string> EventKind(std::string_view json) {
  simdjson::dom::parser parser;
  simdjson::dom::element root;
  if (parser.parse(json).get(root)) {
    return absl::InvalidArgumentError("invalid WebSocket JSON");
  }
  simdjson::dom::element data;
  if (!root["data"].get(data)) root = data;
  std::string_view kind;
  if (root["e"].get(kind)) {
    simdjson::dom::element acknowledgement;
    if (!root["result"].get(acknowledgement)) return std::string{};
    return absl::InvalidArgumentError("WebSocket message lacks event type");
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
  if (stopped_) co_return absl::CancelledError("market stream stopped");
  cycle_became_live_ = false;
  if (epoch_ == std::numeric_limits<uint64_t>::max()) {
    co_return Fault(absl::OutOfRangeError("stream epoch exhausted"));
  }
  if (config_.symbol.empty() ||
      config_.symbol != parser_.Market().native_symbol ||
      config_.snapshot_limit == 0 || config_.snapshot_limit > 5000 ||
      config_.max_snapshot_attempts == 0 ||
      config_.reconnect_delay <= std::chrono::steady_clock::duration::zero() ||
      config_.max_reconnect_delay < config_.reconnect_delay) {
    co_return absl::InvalidArgumentError("invalid Binance stream config");
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
      co_return Fault(absl::AbortedError("buffered depth invalid"));
    }
    ++depth_count;
  }
  if (stopped_) co_return Fault(absl::CancelledError("market stream stopped"));

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
      co_return Fault(
          absl::ResourceExhaustedError("Binance depth snapshot throttled"));
    }
    if (response->status != 200) {
      co_return Fault(
          absl::UnavailableError("Binance depth snapshot HTTP status " +
                                 std::to_string(response->status)));
    }
    auto snapshot = parser_.ParseSnapshot(response->body, epoch_, Now());
    if (!snapshot.ok()) co_return Fault(snapshot.status());
    if (snapshot->last_sequence < first_update) continue;
    auto result = book_.OnSnapshot(*snapshot);
    if (result.state == BookSyncState::Live) cycle_became_live_ = true;
    Report(result);
    if (result.state == BookSyncState::Resyncing) {
      co_return Fault(absl::AbortedError("snapshot replay failed"));
    }
    snapshot_applied = true;
    break;
  }
  if (!snapshot_applied) {
    co_return Fault(
        absl::AbortedError("snapshot remains older than first WS update"));
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
      co_return Fault(absl::AbortedError("depth sequence or book invalid"));
    }
    ++depth_count;
  }
  if (stopped_) co_return Fault(absl::CancelledError("market stream stopped"));
  co_return absl::OkStatus();
}

boost::asio::awaitable<void> MarketDataStream::Run() {
  auto executor = co_await boost::asio::this_coro::executor;
  boost::asio::steady_timer timer(executor);
  auto retry_delay = config_.reconnect_delay;
  while (!stopped_) {
    auto result = co_await RunCycle();
    if (stopped_) break;
    if (result.ok()) continue;
    if (result.code() == absl::StatusCode::kInvalidArgument ||
        result.code() == absl::StatusCode::kOutOfRange)
      break;
    if (cycle_became_live_) retry_delay = config_.reconnect_delay;
    timer.expires_after(retry_delay);
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

}  // namespace hbot::binance_spot
