#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <tuple>

#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "hbot/base/clock.h"
#include "hbot/base/decimal.h"
#include "hbot/base/ids.h"

namespace hbot {

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

}  // namespace hbot
