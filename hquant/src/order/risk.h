#pragma once

#include <chrono>
#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>

#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "base/error.h"
#include "base/market.h"
#include "base/order.h"
#include "base/types.h"

namespace hquant {

struct RiskLease {
  AccountId account;
  AssetId asset;
  ShardId shard;
  uint64_t lease_version = 0;
  Decimal hard_limit;
  UtcTime valid_until_utc{};
};

enum class ReservationState { Active, SubmissionUnknown, Terminal };

struct RiskReservation {
  ReservationId reservation_id;
  std::optional<ClientOrderId> client_id;
  AccountId account;
  OwnerId owner;
  uint64_t lease_version = 0;
  absl::flat_hash_map<AssetId, Decimal> per_asset_worst_case;
  ReservationState state = ReservationState::Active;
};

// Owned by the control thread. This checks the complete static allocation
// before individual shard gates receive their leases. No runtime transfer or
// increase of a shard's allocation is permitted.
class StaticRiskLeaseBook {
 public:
  absl::Status SetConservativeLimit(const AccountId& account,
                                    const AssetId& asset, const Decimal& limit);
  absl::Status GrantInitial(RiskLease lease, UtcTime now);
  absl::Status RenewAfterReconciliation(RiskLease lease, UtcTime now,
                                        bool account_fresh);
  absl::StatusOr<RiskLease> FindLease(const AccountId& account,
                                      const AssetId& asset,
                                      ShardId shard) const;
  absl::StatusOr<Decimal> GrantedTotal(const AccountId& account,
                                       const AssetId& asset) const;

 private:
  using AccountAssetKey = std::pair<std::string, std::string>;
  using LeaseKey = std::tuple<std::string, std::string, uint8_t>;
  static AccountAssetKey Key(const AccountId& account, const AssetId& asset);
  static LeaseKey Key(const RiskLease& lease);

  std::map<AccountAssetKey, Decimal> limits_;
  std::map<AccountAssetKey, Decimal> totals_;
  std::map<LeaseKey, RiskLease> leases_;
};

}  // namespace hquant

namespace hquant {

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
  // Hot-path reservation: rejection returns only a code. On success, writes
  // the reservation to out. The optional detail is a static diagnostic.
  ErrorCode TryReserveCode(const OwnerId& owner, const OrderRequest& request,
                           const MarketSpec& market, const TradingRule& rule,
                           UtcTime now, bool market_live, bool account_fresh,
                           RiskReservation* out,
                           std::string_view* detail = nullptr);
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
  Settings settings_;
  std::map<LeaseKey, LeaseState> leases_;
  std::map<uint64_t, RiskReservation> reservations_;
  uint64_t next_reservation_id_ = 1;
  bool emergency_stop_ = false;
};

}  // namespace hquant
