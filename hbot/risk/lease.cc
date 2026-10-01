#include "hbot/risk/lease.h"

#include <utility>

#include "absl/status/status.h"

namespace hbot {

StaticRiskLeaseBook::AccountAssetKey StaticRiskLeaseBook::Key(
    const AccountId& account, const AssetId& asset) {
  return {account.value, asset.value};
}

StaticRiskLeaseBook::LeaseKey StaticRiskLeaseBook::Key(const RiskLease& lease) {
  return {lease.account.value, lease.asset.value, lease.shard.value};
}

absl::Status StaticRiskLeaseBook::SetConservativeLimit(const AccountId& account,
                                                       const AssetId& asset,
                                                       const Decimal& limit) {
  if (account.value.empty() || asset.value.empty() || !limit.IsNonnegative()) {
    return absl::InvalidArgumentError("invalid conservative account limit");
  }
  if (!limits_.emplace(Key(account, asset), limit).second) {
    return absl::AlreadyExistsError("conservative account limit already set");
  }
  return absl::OkStatus();
}

absl::Status StaticRiskLeaseBook::GrantInitial(RiskLease lease, UtcTime now) {
  if (lease.account.value.empty() || lease.asset.value.empty() ||
      !lease.shard.IsValid() || lease.lease_version == 0 ||
      !lease.hard_limit.IsNonnegative() || lease.valid_until_utc <= now) {
    return absl::InvalidArgumentError("invalid initial risk lease");
  }
  const auto account_asset = Key(lease.account, lease.asset);
  const auto limit = limits_.find(account_asset);
  if (limit == limits_.end()) {
    return absl::FailedPreconditionError("conservative account limit missing");
  }
  if (leases_.contains(Key(lease))) {
    return absl::AlreadyExistsError("shard risk lease already granted");
  }
  const auto old_total = totals_.find(account_asset);
  const Decimal current =
      old_total == totals_.end() ? Decimal() : old_total->second;
  auto proposed = current.Add(lease.hard_limit);
  if (!proposed.ok()) return proposed.status();
  auto within_limit = proposed->Compare(limit->second);
  if (!within_limit.ok()) return within_limit.status();
  if (*within_limit > 0) {
    return absl::ResourceExhaustedError(
        "static risk leases exceed account limit");
  }
  totals_.insert_or_assign(account_asset, *proposed);
  leases_.emplace(Key(lease), std::move(lease));
  return absl::OkStatus();
}

absl::Status StaticRiskLeaseBook::RenewAfterReconciliation(RiskLease lease,
                                                           UtcTime now,
                                                           bool account_fresh) {
  if (!account_fresh) {
    return absl::FailedPreconditionError(
        "account facts stale for lease renewal");
  }
  auto existing = leases_.find(Key(lease));
  if (existing == leases_.end())
    return absl::NotFoundError("risk lease missing");
  if (lease.lease_version <= existing->second.lease_version ||
      lease.valid_until_utc <= now ||
      lease.valid_until_utc <= existing->second.valid_until_utc) {
    return absl::FailedPreconditionError("risk lease version or expiry stale");
  }
  auto same_limit = lease.hard_limit.Compare(existing->second.hard_limit);
  if (!same_limit.ok()) return same_limit.status();
  if (*same_limit != 0) {
    return absl::FailedPreconditionError(
        "static risk allocation cannot change");
  }
  existing->second = std::move(lease);
  return absl::OkStatus();
}

absl::StatusOr<RiskLease> StaticRiskLeaseBook::FindLease(
    const AccountId& account, const AssetId& asset, ShardId shard) const {
  const auto it = leases_.find({account.value, asset.value, shard.value});
  if (it == leases_.end()) return absl::NotFoundError("risk lease missing");
  return it->second;
}

absl::StatusOr<Decimal> StaticRiskLeaseBook::GrantedTotal(
    const AccountId& account, const AssetId& asset) const {
  const auto key = Key(account, asset);
  if (!limits_.contains(key)) {
    return absl::NotFoundError("conservative account limit missing");
  }
  const auto it = totals_.find(key);
  return it == totals_.end() ? Decimal() : it->second;
}

}  // namespace hbot
