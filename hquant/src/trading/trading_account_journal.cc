#include <algorithm>
#include <cstdint>
#include <limits>
#include <string>
#include <utility>

#include "hquant/trading/trading_account.h"

namespace hquant::v1 {

bool TradingAccount::Persist(JournalEntry entry,
                             std::function<void(absl::Status)> done) {
  if (!journal_ || replaying_) {
    done(absl::OkStatus());
    return true;
  }
  std::lock_guard journal_lock(journal_mutex_);
  if (journal_sequence_ ==
      static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
    return false;
  }
  entry.sequence = ++journal_sequence_;
  const size_t pending = pending_journal_.fetch_add(1) + 1;
  if (journal_->Append(
          config_.id, std::move(entry), io_,
          GuardState([this, done = std::move(done)](absl::Status status) {
            --pending_journal_;
            done(std::move(status));
          }))) {
    size_t peak = journal_pending_peak_.load();
    while (pending > peak &&
           !journal_pending_peak_.compare_exchange_weak(peak, pending)) {}
    return true;
  }
  --pending_journal_;
  ++journal_rejections_;
  --journal_sequence_;
  return false;
}

void TradingAccount::PersistSubmitResponse(ClientOrderId id,
                                           TradeResponse response) {
  JournalEntry entry;
  entry.kind = JournalKind::SubmitOutcome;
  entry.client_id = id.value;
  entry.result = static_cast<uint64_t>(response.result);
  entry.exchange_id = response.exchange_order_id.value;
  entry.message = response.reason;
  entry.remaining = response.remaining_nanos.value_or(0);
  if (!Persist(std::move(entry), [this, id, response = std::move(response)](
                                     absl::Status status) mutable {
        if (!status.ok()) {
          Pause("submit response journal failed: " +
                std::string(status.message()));
          return;
        }
        NotifyResponse(id, false, response);
        SubmitResponse(id, std::move(response));
      })) {
    Pause("submit response journal queue unavailable");
  }
}

void TradingAccount::PersistCancelResponse(ClientOrderId id,
                                           TradeResponse response) {
  JournalEntry entry;
  entry.kind = JournalKind::CancelOutcome;
  entry.client_id = id.value;
  entry.result = static_cast<uint64_t>(response.result);
  entry.exchange_id = response.exchange_order_id.value;
  entry.message = response.reason;
  entry.remaining = response.remaining_nanos.value_or(0);
  entry.credit = response.remaining_nanos.has_value();
  if (!Persist(std::move(entry), [this, id, response = std::move(response)](
                                     absl::Status status) mutable {
        if (!status.ok()) {
          Pause("cancel response journal failed: " +
                std::string(status.message()));
          return;
        }
        NotifyResponse(id, true, response);
        CancelResponse(id, std::move(response));
      })) {
    Pause("cancel response journal queue unavailable");
  }
}

absl::Status TradingAccount::RestoreJournal(
    const std::vector<JournalEntry>& entries) {
  if (!journal_) return absl::OkStatus();
  replaying_ = true;
  for (const JournalEntry& entry : entries) {
    if (entry.sequence != journal_sequence_ + 1) {
      replaying_ = false;
      return absl::DataLossError("order journal sequence gap");
    }
    journal_sequence_ = entry.sequence;
    const ClientOrderId id{entry.client_id};
    absl::Status status;
    switch (entry.kind) {
      case JournalKind::SubmitIntent: {
        if (entry.local_id == 0 ||
            entry.local_id == std::numeric_limits<uint64_t>::max() ||
            entry.shard > std::numeric_limits<uint16_t>::max() ||
            entry.side > 1) {
          status = absl::DataLossError("invalid journal submit intent");
          break;
        }
        const auto market = markets_.find(static_cast<uint16_t>(entry.shard));
        if (market == markets_.end() ||
            market->second.id.symbol != entry.symbol) {
          status = absl::DataLossError("journal submit market mismatch");
          break;
        }
        TrackedOrder order;
        order.client_order_id = id;
        order.local_order_id = OrderId{entry.local_id};
        order.shard = ShardId{static_cast<uint16_t>(entry.shard)};
        order.market = market->second.id;
        order.side = entry.side == 0 ? Side::Buy : Side::Sell;
        order.price_nanos = entry.price;
        order.quantity_nanos = entry.quantity;
        order.source_bench_seq = entry.source_bench_seq;
        status = funds_.Reserve(id, entry.asset, entry.amount);
        if (status.ok()) status = order_manager_.Begin(order);
        break;
      }
      case JournalKind::SubmitAbort: {
        const TrackedOrder* order = order_manager_.Find(id);
        if (!order) {
          status = absl::DataLossError("journal submit abort missing order");
          break;
        }
        status = order_manager_.AbortSubmit(id);
        if (status.ok()) status = funds_.Release(id);
        break;
      }
      case JournalKind::CancelIntent:
        status = order_manager_.BeginCancel(id, entry.source_bench_seq,
                                            entry.transaction_ms);
        break;
      case JournalKind::CancelAbort:
        status = order_manager_.AbortCancel(id);
        break;
      case JournalKind::SubmitOutcome:
      case JournalKind::CancelOutcome: {
        if (entry.result >
            static_cast<uint64_t>(TradeResponse::Result::NotSent)) {
          status = absl::DataLossError("invalid journal trade response");
          break;
        }
        TradeResponse response{static_cast<TradeResponse::Result>(entry.result),
                               ExchangeOrderId{entry.exchange_id},
                               entry.message};
        if (entry.kind == JournalKind::CancelOutcome && entry.credit) {
          response.remaining_nanos = entry.remaining;
        }
        if (entry.kind == JournalKind::SubmitOutcome) {
          SubmitResponse(id, std::move(response));
        } else {
          CancelResponse(id, std::move(response));
        }
        break;
      }
      case JournalKind::OrderReport: {
        if (entry.side > 1 ||
            entry.result > static_cast<uint64_t>(OrderStatus::Cancelled)) {
          status = absl::DataLossError("invalid journal order report");
          break;
        }
        Order order;
        order.market = MarketId{entry.symbol};
        order.client_order_id = id;
        order.exchange_order_id = ExchangeOrderId{entry.exchange_id};
        order.side = entry.side == 0 ? Side::Buy : Side::Sell;
        order.price_nanos = entry.price;
        order.quantity_nanos = entry.quantity;
        order.remaining_nanos = entry.remaining;
        ApplyEvent(OrderReport{
            std::move(order), static_cast<OrderStatus>(entry.result),
            entry.message, entry.transaction_ms, entry.credit});
        break;
      }
      case JournalKind::Fill:
        if (entry.side > 1) {
          status = absl::DataLossError("invalid journal fill side");
          break;
        }
        ApplyEvent(Fill{{},
                        entry.side == 0 ? Side::Buy : Side::Sell,
                        entry.trade_id,
                        entry.price,
                        entry.quantity,
                        entry.gross,
                        entry.fee,
                        MarketId{entry.symbol},
                        id,
                        entry.fee_asset,
                        entry.transaction_ms});
        break;
      case JournalKind::Balance:
        ApplyEvent(BalanceUpdate{entry.asset, entry.total, entry.available,
                                 entry.locked, entry.transaction_ms});
        break;
      case JournalKind::BalanceDelta:
        ApplyEvent(BalanceDelta{entry.asset, entry.amount, entry.credit,
                                entry.external_lock, entry.transaction_ms});
        break;
      case JournalKind::Control:
        if (entry.result > static_cast<uint64_t>(AccountAction::CancelAll)) {
          status = absl::DataLossError("invalid journal account control");
          break;
        }
        if (entry.result ==
                static_cast<uint64_t>(AccountAction::PauseNewOrders) ||
            entry.result == static_cast<uint64_t>(AccountAction::CancelAll)) {
          manual_paused_ = true;
          ready_ = false;
        } else if (entry.result ==
                   static_cast<uint64_t>(AccountAction::ResumeTrading)) {
          manual_paused_ = false;
          error_.clear();
        }
        break;
      case JournalKind::SubmitWriting:
      case JournalKind::CancelWriting:
        status = order_manager_.MarkWriting(
            id, entry.kind == JournalKind::CancelWriting);
        break;
    }
    if (!status.ok() || !error_.empty()) {
      replaying_ = false;
      return status.ok() ? absl::DataLossError(error_) : status;
    }
  }
  replaying_ = false;
  ready_ = false;
  if (manual_paused_) error_ = "manually paused";
  PublishAllAssetViews();
  return absl::OkStatus();
}

}  // namespace hquant::v1
