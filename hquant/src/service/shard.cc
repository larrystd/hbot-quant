#include "service/shard.h"

#include <limits>
#include <string>
#include <utility>
#include <variant>

#include "absl/status/status.h"

namespace hquant {

ShardRuntime::ShardRuntime(Config config, const Clock& clock,
                           Strategy& strategy, SimulatedExchange& exchange,
                           RiskGate& risk, RecorderPort& recorder)
    : config_(std::move(config)),
      clock_(clock),
      strategy_(strategy),
      exchange_(exchange),
      risk_(risk),
      recorder_(recorder),
      book_(config_.market.market, config_.scale.scale_version, 8192, 1024,
            config_.stale_after_us),
      dispatcher_(risk_, exchange_, recorder_, config_.run, config_.shard,
                  shard_sequence_) {}

BookApplyResult ShardRuntime::Subscribe(uint64_t stream_epoch) {
  return book_.Subscribe(stream_epoch);
}

absl::StatusOr<Decimal> ShardRuntime::Price(PriceTicks ticks) const {
  auto count = Decimal::Parse(std::to_string(ticks.value));
  if (!count.ok()) return count.status();
  return count->Multiply(config_.scale.quote_per_tick);
}

absl::StatusOr<Decimal> ShardRuntime::Amount(QuantityLots lots) const {
  auto count = Decimal::Parse(std::to_string(lots.value));
  if (!count.ok()) return count.status();
  return count->Multiply(config_.scale.base_per_lot);
}

absl::Status ShardRuntime::RefreshPaperBbo() {
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
  return DrainPaperEvents();
}

absl::Status ShardRuntime::OnSnapshot(const BookSnapshot& snapshot) {
  book_.OnSnapshot(snapshot);
  return RefreshPaperBbo();
}

absl::Status ShardRuntime::OnDiff(const BookDiff& diff) {
  book_.OnDiff(diff);
  return RefreshPaperBbo();
}

absl::Status ShardRuntime::OnPublicTrade(const PublicTrade& trade) {
  if (trade.market != config_.market.market || !trade.side) {
    return Error(ErrorCode::kShardInputInvalid,
                 "public trade market or side invalid");
  }
  auto price = Price(trade.price_ticks);
  auto amount = Amount(trade.quantity_lots);
  if (!price.ok()) return price.status();
  if (!amount.ok()) return amount.status();
  last_trade_price_ = *price;
  auto status = exchange_.OnPublicTrade(*trade.side, *price, *amount);
  if (!status.ok()) return status;
  return DrainPaperEvents();
}

std::vector<OrderSnapshot> ShardRuntime::OrderViews() const {
  std::vector<OrderSnapshot> result;
  for (const auto& id : order_ids_) {
    auto snapshot = tracker_.Snapshot(id);
    if (snapshot) result.push_back(std::move(*snapshot));
  }
  return result;
}

std::vector<Balance> ShardRuntime::BalanceViews() const {
  std::vector<Balance> result;
  for (const AssetId& asset :
       {config_.market.base_asset, config_.market.quote_asset}) {
    result.push_back(Balance{config_.account, asset, exchange_.BalanceOf(asset),
                             exchange_.AvailableBalance(asset),
                             EventTime{{}, clock_.UtcNow(), clock_.MonoNow()}});
  }
  return result;
}

absl::StatusOr<std::vector<DispatchResult>> ShardRuntime::OnTimer(
    InputStamp stamp) {
  if (stamp.at_us < 0)
    return Error(ErrorCode::kShardInputInvalid, "negative input time");
  const auto now_us = clock_.MonoNow().time_since_epoch().count();
  book_.OnTimer(now_us > 0 ? static_cast<uint64_t>(now_us) : 0);
  auto orders = OrderViews();
  auto balances = BalanceViews();
  StrategyContext strategy_context{book_.View(),
                                   config_.scale,
                                   config_.rule,
                                   last_trade_price_,
                                   book_.View().State() == BookSyncState::Live,
                                   orders,
                                   balances,
                                   stamp,
                                   clock_};
  auto batch = strategy_.OnTimer(strategy_context);
  DispatchContext dispatch_context{
      config_.owner,          DecisionId{next_decision_id_++},
      config_.market,         config_.rule,
      clock_.UtcNow(),        clock_.MonoNow(),
      strategy_context.ready, true};
  auto results = dispatcher_.Dispatch(batch, dispatch_context);
  for (const auto& result : results) {
    if (!result.intent) continue;
    auto registered = tracker_.Register(*result.intent);
    if (!registered.ok()) return registered.status();
    order_ids_.push_back(result.intent->client_id);
    if (result.reservation_id) {
      reservations_.emplace(result.intent->client_id.value,
                            *result.reservation_id);
    }
  }
  auto status = DrainPaperEvents();
  if (!status.ok()) return status;
  return results;
}

absl::Status ShardRuntime::Record(RecordPayload payload, const OwnerId& owner) {
  if (shard_sequence_ == std::numeric_limits<uint64_t>::max()) {
    risk_.EmergencyStop();
    return Error(ErrorCode::kSequenceExhausted, "shard sequence exhausted");
  }
  const uint64_t sequence = ++shard_sequence_;
  RecordEnvelope record;
  record.run_id = config_.run;
  record.shard = config_.shard;
  record.shard_sequence = sequence;
  record.owner = owner;
  record.received_at_utc = clock_.UtcNow();
  record.payload = std::move(payload);
  if (!recorder_.TryPush(std::move(record))) AddGap(sequence);
  return absl::OkStatus();
}

void ShardRuntime::AddGap(uint64_t sequence) {
  if (!local_gaps_.empty() && local_gaps_.back().last_seq + 1 == sequence) {
    local_gaps_.back().last_seq = sequence;
  } else {
    local_gaps_.push_back(HistoryGap{config_.run, config_.shard, sequence,
                                     sequence, ErrorCode::kStorageQueueFull});
  }
}

absl::Status ShardRuntime::ProcessAccountEvent(const AccountEvent& event) {
  if (const auto* trade = std::get_if<TradeUpdate>(&event)) {
    if (!trade->client_id)
      return Error(ErrorCode::kShardReportUnattributed,
                   "Paper trade without client ID");
    auto updated = tracker_.ApplyTradeUpdate(*trade);
    if (!updated.ok()) return updated.status();
    auto it = reservations_.find(trade->client_id->value);
    if (it != reservations_.end()) {
      const bool buy = updated->snapshot.request.side == Side::Buy;
      auto status = risk_.ApplyFill(
          it->second,
          buy ? config_.market.quote_asset : config_.market.base_asset,
          buy ? trade->quote_amount : trade->base_amount, Decimal());
      if (!status.ok()) return status;
    }
    return Record(*trade, updated->snapshot.owner);
  }
  if (const auto* update = std::get_if<OrderUpdate>(&event)) {
    if (!update->client_id)
      return Error(ErrorCode::kShardReportUnattributed,
                   "Paper order without client ID");
    auto updated = tracker_.ApplyOrderUpdate(*update);
    if (!updated.ok()) return updated.status();
    if (update->exchange_status == ExchangeOrderStatus::Filled ||
        update->exchange_status == ExchangeOrderStatus::Canceled ||
        update->exchange_status == ExchangeOrderStatus::Rejected ||
        update->exchange_status == ExchangeOrderStatus::Expired) {
      auto it = reservations_.find(update->client_id->value);
      if (it != reservations_.end()) {
        auto status = risk_.ConfirmTerminal(it->second);
        if (!status.ok()) return status;
        reservations_.erase(it);
      }
    }
    return Record(*update, updated->snapshot.owner);
  }
  return absl::OkStatus();
}

absl::Status ShardRuntime::DrainPaperEvents() {
  for (const auto& event : exchange_.DrainEvents()) {
    auto status = ProcessAccountEvent(event);
    if (!status.ok()) return status;
  }
  return absl::OkStatus();
}

}  // namespace hquant
