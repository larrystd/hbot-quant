#pragma once

#include <chrono>
#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <utility>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "hbot/base/clock.h"
#include "hbot/base/decimal.h"
#include "hbot/base/ids.h"
#include "hbot/model/market.h"
#include "hbot/model/order.h"
#include "hbot/model/trading_rule.h"
#include "hbot/risk/lease.h"

namespace hbot {

class RiskGate {
 public:
  struct Settings {
    ShardId shard;
    Decimal fee_buffer_rate;
    std::chrono::seconds max_rule_age{300};
  };

  explicit RiskGate(Settings settings);
  absl::Status SetInitialLease(RiskLease lease);
  // Reconciliation may extend the expiry and version, never the allocation.
  absl::Status RenewAfterReconciliation(RiskLease lease, UtcTime now,
                                        bool account_fresh);
  absl::StatusOr<RiskReservation> TryReserve(const OwnerId& owner,
                                             const OrderRequest& request,
                                             const MarketSpec& market,
                                             const TradingRule& rule,
                                             UtcTime now, bool market_live,
                                             bool account_fresh);
  absl::Status AttachClientId(ReservationId reservation,
                              ClientOrderId client_id);
  absl::Status MarkSubmissionUnknown(ReservationId reservation);
  // Move a verified spent amount from outstanding hold into realized usage.
  absl::Status ApplyFill(ReservationId reservation, const AssetId& spent_asset,
                         const Decimal& actual_spent,
                         const Decimal& remaining_worst_case);
  absl::Status ConfirmTerminal(ReservationId reservation);
  absl::StatusOr<Decimal> Available(const AccountId& account,
                                    const AssetId& asset) const;
  void EmergencyStop() { emergency_stop_ = true; }
  bool emergency_stopped() const { return emergency_stop_; }

 private:
  using LeaseKey = std::pair<std::string, std::string>;
  struct LeaseState {
    RiskLease lease;
    Decimal reserved;
    Decimal realized;
  };

  LeaseKey Key(const AccountId& account, const AssetId& asset) const;
  absl::StatusOr<Decimal> Requirement(const OrderRequest& request) const;
  Settings settings_;
  std::map<LeaseKey, LeaseState> leases_;
  std::map<uint64_t, RiskReservation> reservations_;
  uint64_t next_reservation_id_ = 1;
  bool emergency_stop_ = false;
};

}  // namespace hbot
