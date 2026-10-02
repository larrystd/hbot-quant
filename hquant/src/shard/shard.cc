#include "shard/shard.h"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <limits>
#include <string>
#include <utility>
#include <variant>

#include "absl/status/status.h"
#include "base/net.h"
#include "boost/asio/co_spawn.hpp"
#include "boost/asio/detached.hpp"
#include "boost/asio/post.hpp"
#include "boost/asio/steady_timer.hpp"
#include "boost/asio/use_awaitable.hpp"
#include "market/market_data_stream.h"
#include "order/simulated_exchange.h"

namespace hquant {

Shard::Shard(Config config, const Clock& clock, Strategy& strategy,
             SimulatedExchange& exchange, RiskGate& risk,
             OrderHistoryWriter& recorder)
    : config_(std::move(config)),
      clock_(clock),
      strategy_(strategy),
      exchange_(exchange),
      risk_(risk),
      recorder_(recorder),
      book_(config_.market.market, config_.scale.tick_lot_version, 8192, 1024,
            config_.stale_after_us),
      action_executor_(risk_, exchange_, recorder_, config_.run, config_.shard,
                       shard_sequence_) {
  origin_mono_ = clock_.MonoNow();
}

Shard::Shard(Config config, const Clock& clock,
             std::unique_ptr<Strategy> strategy,
             std::unique_ptr<SimpleSimulatedExchange> exchange,
             std::unique_ptr<RiskGate> risk, OrderHistoryWriter& recorder)
    : config_(std::move(config)),
      owned_strategy_(std::move(strategy)),
      owned_exchange_(std::move(exchange)),
      owned_risk_(std::move(risk)),
      clock_(clock),
      strategy_(*owned_strategy_),
      exchange_(*owned_exchange_),
      risk_(*owned_risk_),
      recorder_(recorder),
      book_(config_.market.market, config_.scale.tick_lot_version, 8192, 1024,
            config_.stale_after_us),
      action_executor_(risk_, exchange_, recorder_, config_.run, config_.shard,
                       shard_sequence_) {
  origin_mono_ = clock_.MonoNow();
}

Shard::~Shard() {
  RequestStop();
  Join();
}

const SimpleSimulatedExchange& Shard::OwnedExchange() const {
  return *owned_exchange_;
}

uint64_t Shard::AppliedDiffs() const {
  return stream_ ? stream_->AppliedDiffs() : 0;
}

uint64_t Shard::Resyncs() const {
  return stream_ ? stream_->Resyncs() : 0;
}

boost::asio::awaitable<void> Shard::TimerLoop() {
  boost::asio::steady_timer timer(*io_);
  uint64_t ordinal = 0;
  while (!stopping_) {
    timer.expires_after(*strategy_.Triggers().timer_period);
    boost::system::error_code ec;
    co_await timer.async_wait(
        boost::asio::redirect_error(boost::asio::use_awaitable, ec));
    if (ec || stopping_) break;
    const auto at_us = (clock_.MonoNow() - origin_mono_).count();
    auto result = OnTimer({at_us, ++ordinal});
    if (!result.ok()) {
      stream_error_ = result.status();
      risk_.EmergencyStop();
    }
  }
}

void Shard::StartFeed(FeedEndpoint rest, FeedEndpoint websocket) {
  io_ = std::make_unique<boost::asio::io_context>();
  const TlsConfig rest_tls{rest.tls, true, {}, rest.host};
  const TlsConfig ws_tls{websocket.tls, true, {}, websocket.host};
  http_ = std::make_unique<HttpClient>(*io_, rest.host,
                                       std::to_string(rest.port), rest_tls);
  std::string symbol = config_.market.market.native_symbol;
  std::transform(symbol.begin(), symbol.end(), symbol.begin(),
                 [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
  websocket_ = std::make_unique<WebSocketClient>(
      *io_, websocket.host, std::to_string(websocket.port),
      "/stream?streams=" + symbol + "@depth/" + symbol + "@trade", ws_tls);
  binance_spot::StreamConfig stream_config;
  stream_config.symbol = config_.market.market.native_symbol;
  stream_ = std::make_unique<binance_spot::MarketDataStream>(
      stream_config,
      binance_spot::DepthParser(config_.market.market, config_.scale),
      *http_, *websocket_, MutableBookSync(), clock_,
      binance_spot::StreamCallbacks{
          [this](const BookApplyResult& result) {
            if (result.state == BookSyncState::Live)
              stream_error_ = absl::OkStatus();
            auto status = OnBookApplied(result);
            if (!status.ok()) {
              stream_error_ = status;
              risk_.EmergencyStop();
            }
          },
          [this](const PublicTrade& trade) {
            auto status = OnPublicTrade(trade);
            if (!status.ok()) {
              stream_error_ = status;
              risk_.EmergencyStop();
            }
          },
          [this](const absl::Status& status) { stream_error_ = status; }});
  boost::asio::co_spawn(*io_, stream_->Run(), boost::asio::detached);
  if (strategy_.Triggers().timer_period)
    boost::asio::co_spawn(*io_, TimerLoop(), boost::asio::detached);
  thread_ = std::thread([this] { io_->run(); });
}

void Shard::RequestStop() {
  if (stopping_.exchange(true)) return;
  if (io_) {
    boost::asio::post(*io_, [this] {
      if (stream_) stream_->Stop();
      io_->stop();
    });
  }
}

void Shard::Join() {
  if (thread_.joinable()) thread_.join();
}

BookApplyResult Shard::Subscribe(uint64_t connection_id) {
  return book_.Subscribe(connection_id);
}

absl::StatusOr<Decimal> Shard::Price(PriceTicks ticks) const {
  auto count = Decimal::Parse(std::to_string(ticks.value));
  if (!count.ok()) return count.status();
  return count->Multiply(config_.scale.price_per_tick);
}

absl::StatusOr<Decimal> Shard::Amount(QuantityLots lots) const {
  auto count = Decimal::Parse(std::to_string(lots.value));
  if (!count.ok()) return count.status();
  return count->Multiply(config_.scale.amount_per_lot);
}

absl::Status Shard::UpdateSimulatedExchangeBbo() {
  if (book_.View().State() != BookSyncState::Live) return absl::OkStatus();
  const auto bid = book_.View().BestBid();
  const auto ask = book_.View().BestAsk();
  if (!bid || !ask) return absl::OkStatus();
  auto bid_price = Price(bid->price_ticks);
  auto ask_price = Price(ask->price_ticks);
  if (!bid_price.ok()) return bid_price.status();
  if (!ask_price.ok()) return ask_price.status();
  auto status = exchange_.OnBookBbo(*bid_price, *ask_price);
  if (!status.ok()) return status;
  return DrainSimulatedExchangeEvents();
}

absl::Status Shard::OnSnapshot(const BookSnapshot& snapshot) {
  return AfterBookApply(book_.OnSnapshot(snapshot));
}

absl::Status Shard::OnDiff(const BookDiff& diff) {
  return AfterBookApply(book_.OnDiff(diff));
}

absl::Status Shard::OnBookApplied(const BookApplyResult& result) {
  return AfterBookApply(result);
}

absl::Status Shard::AfterBookApply(const BookApplyResult& result) {
  auto matched = UpdateSimulatedExchangeBbo();
  if (!matched.ok()) return matched;
  const auto policy = strategy_.Triggers();
  const bool book_trigger =
      result.applied &&
      (policy.book_mode == BookTriggerMode::EveryAppliedBatch ||
       (policy.book_mode == BookTriggerMode::BboChanged && result.top_changed));
  return RunPendingTrigger(book_trigger
                               ? std::optional<Trigger>(Trigger::BookChanged)
                               : std::nullopt);
}

absl::Status Shard::OnPublicTrade(const PublicTrade& trade) {
  if (trade.market != config_.market.market || !trade.side) {
    return Error(ErrorCode::kPublicTradeInvalid,
                 "public trade market or side invalid");
  }
  auto price = Price(trade.price_ticks);
  auto amount = Amount(trade.quantity_lots);
  if (!price.ok()) return price.status();
  if (!amount.ok()) return amount.status();
  last_trade_price_ = *price;
  auto status = exchange_.OnPublicTrade(*trade.side, *price, *amount);
  if (!status.ok()) return status;
  status = DrainSimulatedExchangeEvents();
  if (!status.ok()) return status;
  return RunPendingTrigger(strategy_.Triggers().on_public_trade
                               ? std::optional<Trigger>(Trigger::PublicTraded)
                               : std::nullopt);
}

std::vector<OrderSnapshot> Shard::OrderViews() const {
  std::vector<OrderSnapshot> result;
  for (const auto& id : order_ids_) {
    auto snapshot = tracker_.Snapshot(id);
    if (snapshot) result.push_back(std::move(*snapshot));
  }
  return result;
}

std::vector<Balance> Shard::BalanceViews() const {
  std::vector<Balance> result;
  for (const AssetId& asset :
       {config_.market.base_asset, config_.market.quote_asset}) {
    result.push_back(Balance{config_.account, asset, exchange_.BalanceOf(asset),
                             exchange_.AvailableBalance(asset),
                             EventTime{{}, clock_.UtcNow(), clock_.MonoNow()}});
  }
  return result;
}

absl::StatusOr<std::vector<ActionResult>> Shard::OnTimer(InputTime stamp) {
  if (stamp.at_us < 0)
    return Error(ErrorCode::kInputTimeInvalid, "negative input time");
  const auto now_us = clock_.MonoNow().time_since_epoch().count();
  book_.OnTimer(now_us > 0 ? static_cast<uint64_t>(now_us) : 0);
  return RunStrategy(Trigger::Timer, stamp);
}

absl::Status Shard::RunPendingTrigger(std::optional<Trigger> explicit_trigger) {
  if (!pending_trigger_ && !explicit_trigger) return absl::OkStatus();
  const Trigger why = pending_trigger_ ? *pending_trigger_ : *explicit_trigger;
  pending_trigger_.reset();
  const auto elapsed = clock_.MonoNow() - origin_mono_;
  InputTime stamp{std::max<int64_t>(0, elapsed.count()), ++event_ordinal_};
  auto result = RunStrategy(why, stamp);
  return result.ok() ? absl::OkStatus() : result.status();
}

absl::StatusOr<std::vector<ActionResult>> Shard::RunStrategy(
    Trigger why, InputTime stamp, bool allow_followup) {
  if (in_strategy_) {
    pending_trigger_ = why;
    return std::vector<ActionResult>{};
  }
  std::vector<ActionResult> all_results;
  for (int pass = 0; pass < (allow_followup ? 2 : 1); ++pass) {
    const auto policy = strategy_.Triggers();
    const MonoTime now = clock_.MonoNow();
    if (last_strategy_at_) {
      const auto elapsed = now - *last_strategy_at_;
      if ((policy.coalesce_window && elapsed < *policy.coalesce_window) ||
          elapsed < policy.min_action_interval)
        break;
    }
    last_strategy_at_ = now;
    in_strategy_ = true;
    auto execute = [&]() -> absl::StatusOr<std::vector<ActionResult>> {
      auto orders = OrderViews();
      auto balances = BalanceViews();
      StrategyInput strategy_input{book_.View(),
                                   config_.scale,
                                   config_.rule,
                                   last_trade_price_,
                                   book_.View().State() == BookSyncState::Live,
                                   orders,
                                   balances,
                                   stamp,
                                   clock_};
      ++strategy_invocations_;
      auto batch = strategy_.Decide(strategy_input, why);
      ActionContext action_context{
          config_.strategy_id,  ActionBatchId{next_action_batch_id_++},
          config_.market,       config_.rule,
          clock_.UtcNow(),      clock_.MonoNow(),
          strategy_input.ready, true};
      auto results = action_executor_.Execute(batch, action_context);
      for (const auto& result : results) {
        if (!result.prepared) continue;
        auto registered = tracker_.Register(*result.prepared);
        if (!registered.ok()) return registered.status();
        order_ids_.push_back(result.prepared->client_order_id);
        if (result.hold_id)
          holds_.emplace(result.prepared->client_order_id.value, *result.hold_id);
      }
      auto status = DrainSimulatedExchangeEvents();
      if (!status.ok()) return status;
      return results;
    };
    auto results = execute();
    in_strategy_ = false;
    if (!results.ok()) return results.status();
    all_results.insert(all_results.end(), results->begin(), results->end());
    if (!pending_trigger_) break;
    why = *pending_trigger_;
    pending_trigger_.reset();
  }
  pending_trigger_.reset();
  return all_results;
}

absl::Status Shard::Record(OrderHistoryRecordPayload payload,
                           const StrategyId& strategy_id) {
  if (shard_sequence_ == std::numeric_limits<uint64_t>::max()) {
    risk_.EmergencyStop();
    return Error(ErrorCode::kSequenceExhausted, "shard sequence exhausted");
  }
  const uint64_t sequence = ++shard_sequence_;
  OrderHistoryRecord record;
  record.run_id = config_.run;
  record.shard = config_.shard;
  record.shard_sequence = sequence;
  record.strategy_id = strategy_id;
  record.received_at_utc = clock_.UtcNow();
  record.payload = std::move(payload);
  if (!recorder_.TryPush(std::move(record))) AddGap(sequence);
  return absl::OkStatus();
}

void Shard::AddGap(uint64_t sequence) {
  if (!local_gaps_.empty() && local_gaps_.back().last_seq + 1 == sequence) {
    local_gaps_.back().last_seq = sequence;
  } else {
    local_gaps_.push_back(OrderHistoryGap{config_.run, config_.shard, sequence,
                                     sequence, ErrorCode::kOrderHistoryQueueFull});
  }
}

absl::Status Shard::ProcessAccountEvent(const AccountEvent& event) {
  if (const auto* trade = std::get_if<TradeUpdate>(&event)) {
    if (!trade->client_order_id)
      return Error(ErrorCode::kShardReportOrderUnknown,
                   "Simulated trade without client ID");
    auto updated = tracker_.ApplyTradeUpdate(*trade);
    if (!updated.ok()) return updated.status();
    auto it = holds_.find(trade->client_order_id->value);
    if (it != holds_.end()) {
      const bool buy = updated->snapshot.request.side == Side::Buy;
      auto status = risk_.ApplyTrade(
          it->second,
          buy ? config_.market.quote_asset : config_.market.base_asset,
          buy ? trade->quote_amount : trade->base_amount, Decimal());
      if (!status.ok()) return status;
    }
    auto status = Record(*trade, updated->snapshot.strategy_id);
    if (status.ok() && strategy_.Triggers().on_fill)
      pending_trigger_ = Trigger::Traded;
    return status;
  }
  if (const auto* update = std::get_if<OrderUpdate>(&event)) {
    if (!update->client_order_id)
      return Error(ErrorCode::kShardReportOrderUnknown,
                   "Simulated order without client ID");
    auto updated = tracker_.ApplyOrderUpdate(*update);
    if (!updated.ok()) return updated.status();
    if (update->exchange_status == ExchangeOrderStatus::Traded ||
        update->exchange_status == ExchangeOrderStatus::Canceled ||
        update->exchange_status == ExchangeOrderStatus::Rejected ||
        update->exchange_status == ExchangeOrderStatus::Expired) {
      auto it = holds_.find(update->client_order_id->value);
      if (it != holds_.end()) {
        auto status = risk_.Release(it->second);
        if (!status.ok()) return status;
        holds_.erase(it);
      }
    }
    auto status = Record(*update, updated->snapshot.strategy_id);
    if (status.ok() && strategy_.Triggers().on_order_update &&
        !pending_trigger_)
      pending_trigger_ = Trigger::OrderUpdated;
    return status;
  }
  return absl::OkStatus();
}

absl::Status Shard::DrainSimulatedExchangeEvents() {
  for (const auto& event : exchange_.DrainEvents()) {
    auto status = ProcessAccountEvent(event);
    if (!status.ok()) return status;
  }
  return absl::OkStatus();
}

}  // namespace hquant
