#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "hquant/config.h"
#include "hquant/trading/order_journal.h"
#include "hquant/trading/order_manager.h"
#include "hquant/trading/trade_port.h"

namespace hquant::v1 {

// Only the shard worker writes this state. The account owns the exchange
// reconciliation copy but never grants funds during a market callback.
class ShardTradingState {
 public:
  struct Admission {
    TrackedOrder order;
    std::string asset;
    uint64_t amount_nanos = 0;
  };

  ShardTradingState(const ShardConfig& shard, ShardAllocation allocation,
                    AccountConfig::RiskPolicy risk, uint64_t maker_fee_rate_ppb);

  void SetVenueRule(std::optional<VenueRule> rule) {
    venue_rule_ = std::move(rule);
  }
  absl::StatusOr<Admission> BeginSubmit(ClientOrderId id,
                                        const LimitOrderRequest& request,
                                        uint64_t source_bench_seq);
  absl::Status AbortSubmit(const ClientOrderId& id);
  absl::StatusOr<CancelCommand> BeginCancel(OrderId local_id,
                                            uint64_t source_bench_seq);
  absl::Status AbortCancel(const ClientOrderId& id);
  absl::Status MarkWriting(const ClientOrderId& id, bool cancel);
  absl::Status ApplyResponse(const ClientOrderId& id, bool cancel,
                             const TradeResponse& response);
  absl::Status ApplyEvent(const AccountEvent& event);
  absl::Status RestoreJournal(const std::vector<JournalEntry>& entries);

  std::vector<OrderId> OpenOrders() const { return orders_.OpenFor(shard_.id); }
  bool Pending() const { return orders_.PendingFor(shard_.id); }
  const TrackedOrder* FindLocal(OrderId id) const {
    return orders_.FindLocal(id);
  }
  uint64_t Total(const std::string& asset) const;
  uint64_t Available(const std::string& asset) const;
  uint64_t Budget(const std::string& asset) const { return Available(asset); }

 private:
  struct Hold {
    std::string asset;
    uint64_t amount_nanos = 0;
  };
  absl::StatusOr<uint64_t> ReserveAmount(const LimitOrderRequest& request) const;
  absl::Status Reserve(const ClientOrderId& id, std::string asset,
                       uint64_t amount);
  absl::Status SetHold(const ClientOrderId& id, uint64_t amount);
  absl::Status Release(const ClientOrderId& id);
  absl::Status BeginRestored(const JournalEntry& entry);
  absl::Status ApplyReport(const OrderReport& report);
  absl::Status ApplyFill(const Fill& fill);
  static Order ReportOrder(const TrackedOrder& tracked);

  ShardConfig shard_;
  AccountConfig::RiskPolicy risk_;
  uint64_t maker_fee_rate_ppb_ = 0;
  std::map<uint16_t, MarketConfig> markets_;
  std::map<std::string, uint64_t> balances_;
  std::map<std::string, uint64_t> reserved_;
  std::map<std::string, Hold> holds_;
  std::optional<VenueRule> venue_rule_;
  std::optional<uint64_t> max_active_orders_;
  std::optional<uint64_t> max_open_value_per_quote_nanos_;
  uint64_t next_local_sequence_ = 1;
  OrderManager orders_;
};

}  // namespace hquant::v1
