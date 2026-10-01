#include "hbot/risk/risk_gate.h"

#include <utility>

#include "absl/status/status.h"

namespace hbot {

RiskGate::RiskGate(Settings settings) : settings_(std::move(settings)) {}

RiskGate::LeaseKey RiskGate::Key(const AccountId& account,
                                 const AssetId& asset) const {
  return {account.value, asset.value};
}

absl::Status RiskGate::SetInitialLease(RiskLease lease) {
  if (!settings_.shard.IsValid() || lease.shard != settings_.shard ||
      lease.account.value.empty() || lease.asset.value.empty() ||
      lease.lease_version == 0 || !lease.hard_limit.IsNonnegative()) {
    return absl::InvalidArgumentError("invalid risk lease");
  }
  const auto key = Key(lease.account, lease.asset);
  if (leases_.contains(key))
    return absl::AlreadyExistsError("duplicate risk lease");
  leases_.emplace(key, LeaseState{std::move(lease), Decimal(), Decimal()});
  return absl::OkStatus();
}

absl::Status RiskGate::RenewAfterReconciliation(RiskLease lease, UtcTime now,
                                                bool account_fresh) {
  if (emergency_stop_ || !account_fresh) {
    return absl::FailedPreconditionError("account not ready for lease renewal");
  }
  if (lease.shard != settings_.shard) {
    return absl::InvalidArgumentError("risk lease belongs to another shard");
  }
  auto it = leases_.find(Key(lease.account, lease.asset));
  if (it == leases_.end()) return absl::NotFoundError("risk lease missing");
  if (lease.lease_version <= it->second.lease.lease_version ||
      lease.valid_until_utc <= now ||
      lease.valid_until_utc <= it->second.lease.valid_until_utc) {
    return absl::FailedPreconditionError("risk lease version or expiry stale");
  }
  auto same_limit = lease.hard_limit.Compare(it->second.lease.hard_limit);
  if (!same_limit.ok()) return same_limit.status();
  if (*same_limit != 0) {
    return absl::FailedPreconditionError(
        "static risk allocation cannot change");
  }
  it->second.lease = std::move(lease);
  return absl::OkStatus();
}

absl::StatusOr<Decimal> RiskGate::Requirement(
    const OrderRequest& request) const {
  if (!request.base_amount.IsStrictlyPositive() || !request.limit_price ||
      !request.limit_price->IsStrictlyPositive()) {
    return absl::InvalidArgumentError("invalid limit amount or price");
  }
  absl::StatusOr<Decimal> principal =
      request.side == Side::Buy
          ? request.base_amount.Multiply(*request.limit_price)
          : absl::StatusOr<Decimal>(request.base_amount);
  if (!principal.ok()) return principal.status();
  auto buffer = principal->Multiply(settings_.fee_buffer_rate);
  if (!buffer.ok()) return buffer.status();
  return principal->Add(*buffer);
}

absl::StatusOr<RiskReservation> RiskGate::TryReserve(
    const OwnerId& owner, const OrderRequest& request, const MarketSpec& market,
    const TradingRule& rule, UtcTime now, bool market_live,
    bool account_fresh) {
  if (emergency_stop_) return absl::FailedPreconditionError("emergency stop");
  if (!settings_.fee_buffer_rate.IsNonnegative() ||
      settings_.max_rule_age.count() < 0) {
    return absl::FailedPreconditionError("invalid risk settings");
  }
  if (!market_live || !account_fresh) {
    return absl::FailedPreconditionError("market or account not ready");
  }
  if (!owner.IsValid() || request.account.value.empty() ||
      request.market != market.market || request.market != rule.market ||
      (request.type != OrderType::Limit &&
       request.type != OrderType::LimitMaker)) {
    return absl::InvalidArgumentError("invalid order identity or type");
  }
  if (rule.revision == 0 || now < rule.observed_at ||
      now - rule.observed_at > settings_.max_rule_age ||
      !rule.price_increment.IsStrictlyPositive() ||
      !rule.base_increment.IsStrictlyPositive()) {
    return absl::FailedPreconditionError("trading rule stale or invalid");
  }
  if (!request.limit_price || !request.limit_price->IsStrictlyPositive() ||
      !request.base_amount.IsStrictlyPositive()) {
    return absl::InvalidArgumentError("invalid limit order values");
  }
  auto price =
      request.limit_price->Quantize(rule.price_increment, RoundingMode::Down);
  auto amount =
      request.base_amount.Quantize(rule.base_increment, RoundingMode::Down);
  if (!price.ok()) return price.status();
  if (!amount.ok()) return amount.status();
  auto price_cmp = price->Compare(*request.limit_price);
  auto amount_cmp = amount->Compare(request.base_amount);
  if (!price_cmp.ok() || !amount_cmp.ok() || *price_cmp != 0 ||
      *amount_cmp != 0) {
    return absl::InvalidArgumentError("order not aligned with trading rule");
  }
  auto min_amount = request.base_amount.Compare(rule.min_base_amount);
  if (!min_amount.ok()) return min_amount.status();
  if (*min_amount < 0)
    return absl::FailedPreconditionError("below minimum amount");
  if (rule.max_base_amount) {
    auto max_amount = request.base_amount.Compare(*rule.max_base_amount);
    if (!max_amount.ok()) return max_amount.status();
    if (*max_amount > 0)
      return absl::FailedPreconditionError("above maximum amount");
  }
  auto notional = request.base_amount.Multiply(*request.limit_price);
  if (!notional.ok()) return notional.status();
  auto min_notional = notional->Compare(rule.min_notional);
  if (!min_notional.ok()) return min_notional.status();
  if (*min_notional < 0)
    return absl::FailedPreconditionError("below minimum notional");

  const AssetId& spent_asset =
      request.side == Side::Buy ? market.quote_asset : market.base_asset;
  auto it = leases_.find(Key(request.account, spent_asset));
  if (it == leases_.end())
    return absl::FailedPreconditionError("missing asset lease");
  LeaseState& lease = it->second;
  if (now >= lease.lease.valid_until_utc) {
    return absl::FailedPreconditionError("asset lease expired");
  }
  auto needed = Requirement(request);
  if (!needed.ok()) return needed.status();
  auto available = Available(request.account, spent_asset);
  if (!available.ok()) return available.status();
  auto enough = available->Compare(*needed);
  if (!enough.ok()) return enough.status();
  if (*enough < 0) return absl::ResourceExhaustedError("risk lease exhausted");
  if (next_reservation_id_ == 0) {
    EmergencyStop();
    return absl::ResourceExhaustedError("reservation id exhausted");
  }
  const ReservationId id{next_reservation_id_++};
  auto new_reserved = lease.reserved.Add(*needed);
  if (!new_reserved.ok()) return new_reserved.status();
  RiskReservation reservation;
  reservation.reservation_id = id;
  reservation.account = request.account;
  reservation.owner = owner;
  reservation.lease_version = lease.lease.lease_version;
  reservation.per_asset_worst_case.emplace(spent_asset, *needed);
  reservations_.emplace(id.value, reservation);
  lease.reserved = *new_reserved;
  return reservation;
}

absl::Status RiskGate::AttachClientId(ReservationId reservation,
                                      ClientOrderId client_id) {
  auto it = reservations_.find(reservation.value);
  if (it == reservations_.end() ||
      it->second.state == ReservationState::Terminal ||
      client_id.value.empty() || it->second.client_id) {
    return absl::FailedPreconditionError("cannot attach client id");
  }
  it->second.client_id = std::move(client_id);
  return absl::OkStatus();
}

absl::Status RiskGate::MarkSubmissionUnknown(ReservationId reservation) {
  auto it = reservations_.find(reservation.value);
  if (it == reservations_.end() ||
      it->second.state == ReservationState::Terminal) {
    return absl::FailedPreconditionError("unknown reservation");
  }
  it->second.state = ReservationState::SubmissionUnknown;
  return absl::OkStatus();
}

absl::Status RiskGate::ApplyFill(ReservationId reservation,
                                 const AssetId& spent_asset,
                                 const Decimal& actual_spent,
                                 const Decimal& remaining_worst_case) {
  auto it = reservations_.find(reservation.value);
  if (it == reservations_.end() ||
      it->second.state == ReservationState::Terminal ||
      !actual_spent.IsNonnegative() || !remaining_worst_case.IsNonnegative()) {
    return absl::FailedPreconditionError("invalid fill reservation");
  }
  auto hold = it->second.per_asset_worst_case.find(spent_asset);
  auto lease = leases_.find(Key(it->second.account, spent_asset));
  if (hold == it->second.per_asset_worst_case.end() || lease == leases_.end()) {
    return absl::FailedPreconditionError("missing fill asset lease");
  }
  auto decreasing = hold->second.Compare(remaining_worst_case);
  if (!decreasing.ok()) return decreasing.status();
  if (*decreasing < 0)
    return absl::InvalidArgumentError("hold cannot increase on fill");
  auto less_hold = lease->second.reserved.Subtract(hold->second);
  if (!less_hold.ok()) return less_hold.status();
  auto new_hold = less_hold->Add(remaining_worst_case);
  if (!new_hold.ok()) return new_hold.status();
  auto spent = lease->second.realized.Add(actual_spent);
  if (!spent.ok()) return spent.status();
  lease->second.reserved = *new_hold;
  lease->second.realized = *spent;
  hold->second = remaining_worst_case;
  auto available = Available(it->second.account, spent_asset);
  if (!available.ok()) return available.status();
  if (!available->IsNonnegative()) EmergencyStop();
  return absl::OkStatus();
}

absl::Status RiskGate::ConfirmTerminal(ReservationId reservation) {
  auto it = reservations_.find(reservation.value);
  if (it == reservations_.end() ||
      it->second.state == ReservationState::Terminal) {
    return absl::FailedPreconditionError(
        "reservation already terminal or missing");
  }
  for (const auto& [asset, amount] : it->second.per_asset_worst_case) {
    auto lease = leases_.find(Key(it->second.account, asset));
    if (lease == leases_.end())
      return absl::InternalError("reservation lease disappeared");
    auto remaining = lease->second.reserved.Subtract(amount);
    if (!remaining.ok()) return remaining.status();
    lease->second.reserved = *remaining;
  }
  it->second.per_asset_worst_case.clear();
  it->second.state = ReservationState::Terminal;
  return absl::OkStatus();
}

absl::StatusOr<Decimal> RiskGate::Available(const AccountId& account,
                                            const AssetId& asset) const {
  auto it = leases_.find(Key(account, asset));
  if (it == leases_.end()) return absl::NotFoundError("asset lease missing");
  auto after_reserved =
      it->second.lease.hard_limit.Subtract(it->second.reserved);
  if (!after_reserved.ok()) return after_reserved.status();
  return after_reserved->Subtract(it->second.realized);
}

}  // namespace hbot
