#include "order/risk.h"

#include <utility>

#include "absl/status/status.h"
#include "base/error.h"

namespace hquant {

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
    return Error(ErrorCode::kRiskConfigInvalid,
                 "invalid conservative account limit");
  }
  if (!limits_.emplace(Key(account, asset), limit).second) {
    return Error(ErrorCode::kRiskConfigInvalid,
                 "conservative account limit already set");
  }
  return absl::OkStatus();
}

absl::Status StaticRiskLeaseBook::GrantInitial(RiskLease lease, UtcTime now) {
  if (lease.account.value.empty() || lease.asset.value.empty() ||
      !lease.shard.IsValid() || lease.lease_version == 0 ||
      !lease.hard_limit.IsNonnegative() || lease.valid_until_utc <= now) {
    return Error(ErrorCode::kRiskConfigInvalid, "invalid initial risk lease");
  }
  const auto account_asset = Key(lease.account, lease.asset);
  const auto limit = limits_.find(account_asset);
  if (limit == limits_.end()) {
    return Error(ErrorCode::kRiskBudgetMissing,
                 "conservative account limit missing");
  }
  if (leases_.contains(Key(lease))) {
    return Error(ErrorCode::kRiskConfigInvalid,
                 "shard risk lease already granted");
  }
  const auto old_total = totals_.find(account_asset);
  const Decimal current =
      old_total == totals_.end() ? Decimal() : old_total->second;
  auto proposed = current.Add(lease.hard_limit);
  if (!proposed.ok())
    return Error(ErrorCode::kRiskConfigInvalid,
                 "static risk lease total cannot be calculated");
  auto within_limit = proposed->Compare(limit->second);
  if (!within_limit.ok())
    return Error(ErrorCode::kRiskConfigInvalid,
                 "static risk lease limit cannot be compared");
  if (*within_limit > 0) {
    return Error(ErrorCode::kRiskConfigInvalid,
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
    return Error(ErrorCode::kRiskAccountStale,
                 "account facts stale for lease renewal");
  }
  auto existing = leases_.find(Key(lease));
  if (existing == leases_.end())
    return Error(ErrorCode::kRiskBudgetMissing, "risk lease missing");
  if (lease.lease_version <= existing->second.lease_version ||
      lease.valid_until_utc <= now ||
      lease.valid_until_utc <= existing->second.valid_until_utc) {
    return Error(ErrorCode::kRiskBudgetRenewalStale,
                 "risk lease version or expiry stale");
  }
  auto same_limit = lease.hard_limit.Compare(existing->second.hard_limit);
  if (!same_limit.ok())
    return Error(ErrorCode::kRiskConfigInvalid,
                 "risk lease limits cannot be compared");
  if (*same_limit != 0) {
    return Error(ErrorCode::kRiskConfigInvalid,
                 "static risk allocation cannot change");
  }
  existing->second = std::move(lease);
  return absl::OkStatus();
}

absl::StatusOr<RiskLease> StaticRiskLeaseBook::FindLease(
    const AccountId& account, const AssetId& asset, ShardId shard) const {
  const auto it = leases_.find({account.value, asset.value, shard.value});
  if (it == leases_.end())
    return Error(ErrorCode::kRiskBudgetMissing, "risk lease missing");
  return it->second;
}

absl::StatusOr<Decimal> StaticRiskLeaseBook::GrantedTotal(
    const AccountId& account, const AssetId& asset) const {
  const auto key = Key(account, asset);
  if (!limits_.contains(key)) {
    return Error(ErrorCode::kRiskBudgetMissing,
                 "conservative account limit missing");
  }
  const auto it = totals_.find(key);
  return it == totals_.end() ? Decimal() : it->second;
}

}  // namespace hquant

namespace hquant {

RiskGate::RiskGate(Settings settings) : settings_(std::move(settings)) {}

RiskGate::LeaseKey RiskGate::Key(const AccountId& account,
                                 const AssetId& asset) const {
  return {account.value, asset.value};
}

absl::Status RiskGate::SetInitialLease(RiskLease lease) {
  if (!settings_.shard.IsValid() || lease.shard != settings_.shard ||
      lease.account.value.empty() || lease.asset.value.empty() ||
      lease.lease_version == 0 || !lease.hard_limit.IsNonnegative()) {
    return Error(ErrorCode::kRiskConfigInvalid, "invalid risk lease");
  }
  const auto key = Key(lease.account, lease.asset);
  if (leases_.contains(key))
    return Error(ErrorCode::kRiskConfigInvalid, "duplicate risk lease");
  leases_.emplace(key, LeaseState{std::move(lease), Decimal(), Decimal()});
  return absl::OkStatus();
}

absl::Status RiskGate::RenewAfterReconciliation(RiskLease lease, UtcTime now,
                                                bool account_fresh) {
  if (emergency_stop_)
    return Error(ErrorCode::kRiskEmergencyStopped, "emergency stop");
  if (!account_fresh) {
    return Error(ErrorCode::kRiskAccountStale,
                 "account not ready for lease renewal");
  }
  if (lease.shard != settings_.shard) {
    return Error(ErrorCode::kRiskConfigInvalid,
                 "risk lease belongs to another shard");
  }
  auto it = leases_.find(Key(lease.account, lease.asset));
  if (it == leases_.end())
    return Error(ErrorCode::kRiskBudgetMissing, "risk lease missing");
  if (lease.lease_version <= it->second.lease.lease_version ||
      lease.valid_until_utc <= now ||
      lease.valid_until_utc <= it->second.lease.valid_until_utc) {
    return Error(ErrorCode::kRiskBudgetRenewalStale,
                 "risk lease version or expiry stale");
  }
  auto same_limit = lease.hard_limit.Compare(it->second.lease.hard_limit);
  if (!same_limit.ok())
    return Error(ErrorCode::kRiskConfigInvalid,
                 "risk lease limits cannot be compared");
  if (*same_limit != 0) {
    return Error(ErrorCode::kRiskConfigInvalid,
                 "static risk allocation cannot change");
  }
  it->second.lease = std::move(lease);
  return absl::OkStatus();
}

absl::StatusOr<RiskReservation> RiskGate::TryReserve(
    const StrategyId& strategy_id, const OrderRequest& request, const MarketSpec& market,
    const TradingRule& rule, UtcTime now, bool market_live,
    bool account_fresh) {
  RiskReservation reservation;
  std::string_view detail;
  const ErrorCode code =
      TryReserveCode(strategy_id, request, market, rule, now, market_live,
                     account_fresh, &reservation, &detail);
  if (code != ErrorCode::kOk) return Error(code, detail);
  return reservation;
}

ErrorCode RiskGate::TryReserveCode(
    const StrategyId& strategy_id, const OrderRequest& request, const MarketSpec& market,
    const TradingRule& rule, UtcTime now, bool market_live, bool account_fresh,
    RiskReservation* out, std::string_view* detail) {
  const auto reject = [detail](ErrorCode code, std::string_view message) {
    if (detail) *detail = message;
    return code;
  };
  if (!out) return reject(ErrorCode::kInternal, "null risk reservation output");
  if (emergency_stop_)
    return reject(ErrorCode::kRiskEmergencyStopped, "emergency stop");
  if (!settings_.fee_buffer_rate.IsNonnegative() ||
      settings_.max_rule_age.count() < 0) {
    return reject(ErrorCode::kRiskConfigInvalid, "invalid risk settings");
  }
  if (!market_live)
    return reject(ErrorCode::kRiskMarketNotLive, "order book is not live");
  if (!account_fresh)
    return reject(ErrorCode::kRiskAccountStale, "account facts are stale");
  if (!strategy_id.IsValid())
    return reject(ErrorCode::kOrderStrategyIdInvalid, "invalid strategy ID");
  if (request.account.value.empty())
    return reject(ErrorCode::kOrderAccountInvalid, "empty order account");
  if (request.market != market.market || request.market != rule.market)
    return reject(ErrorCode::kOrderMarketInvalid,
                  "order market does not match market spec or trading rule");
  if (request.type != OrderType::Limit && request.type != OrderType::LimitMaker)
    return reject(ErrorCode::kOrderTypeUnsupported, "only limit orders");
  if (rule.revision == 0 || now < rule.observed_at ||
      now - rule.observed_at > settings_.max_rule_age ||
      !rule.price_increment.IsStrictlyPositive() ||
      !rule.base_increment.IsStrictlyPositive()) {
    return reject(ErrorCode::kRiskTradingRuleStale,
                  "trading rule stale or invalid");
  }
  if (!request.limit_price || !request.limit_price->IsStrictlyPositive() ||
      !request.base_amount.IsStrictlyPositive()) {
    return reject(ErrorCode::kOrderPriceOrAmountInvalid,
                  "limit price and amount must be positive");
  }
  auto price =
      request.limit_price->Quantize(rule.price_increment, RoundingMode::Down);
  auto amount =
      request.base_amount.Quantize(rule.base_increment, RoundingMode::Down);
  if (!price.ok())
    return reject(ErrorCode::kDecimalArithmeticFailed, "price quantization failed");
  if (!amount.ok())
    return reject(ErrorCode::kDecimalArithmeticFailed, "amount quantization failed");
  auto price_cmp = price->Compare(*request.limit_price);
  auto amount_cmp = amount->Compare(request.base_amount);
  if (!price_cmp.ok() || !amount_cmp.ok() || *price_cmp != 0 ||
      *amount_cmp != 0) {
    return reject(ErrorCode::kOrderNotOnTick,
                  "order not aligned with trading rule");
  }
  auto min_amount = request.base_amount.Compare(rule.min_base_amount);
  if (!min_amount.ok())
    return reject(ErrorCode::kDecimalArithmeticFailed,
                  "minimum amount comparison failed");
  if (*min_amount < 0)
    return reject(ErrorCode::kOrderBelowMinAmount, "below minimum amount");
  if (rule.max_base_amount) {
    auto max_amount = request.base_amount.Compare(*rule.max_base_amount);
    if (!max_amount.ok())
      return reject(ErrorCode::kDecimalArithmeticFailed,
                    "maximum amount comparison failed");
    if (*max_amount > 0)
      return reject(ErrorCode::kOrderAboveMaxAmount, "above maximum amount");
  }
  auto notional = request.base_amount.Multiply(*request.limit_price);
  if (!notional.ok())
    return reject(ErrorCode::kDecimalArithmeticFailed,
                  "notional calculation failed");
  auto min_notional = notional->Compare(rule.min_notional);
  if (!min_notional.ok())
    return reject(ErrorCode::kDecimalArithmeticFailed,
                  "minimum notional comparison failed");
  if (*min_notional < 0)
    return reject(ErrorCode::kOrderBelowMinNotional, "below minimum notional");

  const AssetId& spent_asset =
      request.side == Side::Buy ? market.quote_asset : market.base_asset;
  auto it = leases_.find(Key(request.account, spent_asset));
  if (it == leases_.end())
    return reject(ErrorCode::kRiskBudgetMissing, "missing asset lease");
  LeaseState& lease = it->second;
  if (now >= lease.lease.valid_until_utc) {
    return reject(ErrorCode::kRiskBudgetExpired, "asset lease expired");
  }
  absl::StatusOr<Decimal> principal =
      request.side == Side::Buy
          ? request.base_amount.Multiply(*request.limit_price)
          : absl::StatusOr<Decimal>(request.base_amount);
  if (!principal.ok())
    return reject(ErrorCode::kDecimalArithmeticFailed,
                  "principal calculation failed");
  auto buffer = principal->Multiply(settings_.fee_buffer_rate);
  if (!buffer.ok())
    return reject(ErrorCode::kDecimalArithmeticFailed,
                  "fee buffer calculation failed");
  auto needed = principal->Add(*buffer);
  if (!needed.ok())
    return reject(ErrorCode::kDecimalArithmeticFailed,
                  "required amount calculation failed");
  auto after_reserved = lease.lease.hard_limit.Subtract(lease.reserved);
  if (!after_reserved.ok())
    return reject(ErrorCode::kDecimalArithmeticFailed,
                  "reserved amount calculation failed");
  auto available = after_reserved->Subtract(lease.realized);
  if (!available.ok())
    return reject(ErrorCode::kDecimalArithmeticFailed,
                  "available amount calculation failed");
  auto enough = available->Compare(*needed);
  if (!enough.ok())
    return reject(ErrorCode::kDecimalArithmeticFailed,
                  "available amount comparison failed");
  if (*enough < 0)
    return reject(ErrorCode::kRiskBudgetExhausted, "risk lease exhausted");
  if (next_reservation_id_ == 0) {
    EmergencyStop();
    return reject(ErrorCode::kSequenceExhausted, "reservation id exhausted");
  }
  auto new_reserved = lease.reserved.Add(*needed);
  if (!new_reserved.ok())
    return reject(ErrorCode::kDecimalArithmeticFailed,
                  "reservation total calculation failed");
  const ReservationId id{next_reservation_id_++};
  RiskReservation reservation;
  reservation.reservation_id = id;
  reservation.account = request.account;
  reservation.strategy_id = strategy_id;
  reservation.lease_version = lease.lease.lease_version;
  reservation.per_asset_worst_case.emplace(spent_asset, *needed);
  reservations_.emplace(id.value, reservation);
  lease.reserved = *new_reserved;
  *out = std::move(reservation);
  if (detail) *detail = {};
  return ErrorCode::kOk;
}

absl::Status RiskGate::AttachClientId(ReservationId reservation,
                                      ClientOrderId client_id) {
  auto it = reservations_.find(reservation.value);
  if (it == reservations_.end())
    return Error(ErrorCode::kFundsHoldNotFound, "unknown reservation");
  if (it->second.state == ReservationState::Terminal)
    return Error(ErrorCode::kFundsHoldAlreadyReleased,
                 "reservation already released");
  if (client_id.value.empty() || it->second.client_id) {
    return Error(ErrorCode::kFundsHoldClientIdConflict,
                 "client ID empty or already attached");
  }
  it->second.client_id = std::move(client_id);
  return absl::OkStatus();
}

absl::Status RiskGate::MarkSubmissionUnknown(ReservationId reservation) {
  auto it = reservations_.find(reservation.value);
  if (it == reservations_.end())
    return Error(ErrorCode::kFundsHoldNotFound, "unknown reservation");
  if (it->second.state == ReservationState::Terminal)
    return Error(ErrorCode::kFundsHoldAlreadyReleased,
                 "reservation already released");
  it->second.state = ReservationState::SubmissionUnknown;
  return absl::OkStatus();
}

absl::Status RiskGate::ApplyFill(ReservationId reservation,
                                 const AssetId& spent_asset,
                                 const Decimal& actual_spent,
                                 const Decimal& remaining_worst_case) {
  auto it = reservations_.find(reservation.value);
  if (it == reservations_.end())
    return Error(ErrorCode::kFundsHoldNotFound, "unknown reservation");
  if (it->second.state == ReservationState::Terminal)
    return Error(ErrorCode::kFundsHoldAlreadyReleased,
                 "reservation already released");
  if (!actual_spent.IsNonnegative() || !remaining_worst_case.IsNonnegative()) {
    return Error(ErrorCode::kOrderPriceOrAmountInvalid,
                 "fill amounts must not be negative");
  }
  auto hold = it->second.per_asset_worst_case.find(spent_asset);
  auto lease = leases_.find(Key(it->second.account, spent_asset));
  if (hold == it->second.per_asset_worst_case.end() || lease == leases_.end()) {
    return Error(ErrorCode::kRiskBudgetMissing,
                 "missing fill asset lease");
  }
  auto decreasing = hold->second.Compare(remaining_worst_case);
  if (!decreasing.ok())
    return Error(ErrorCode::kDecimalArithmeticFailed,
                 "fill reservation amount cannot be compared");
  if (*decreasing < 0)
    return Error(ErrorCode::kFundsHoldIncreased,
                 "hold cannot increase on fill");
  auto less_hold = lease->second.reserved.Subtract(hold->second);
  if (!less_hold.ok())
    return Error(ErrorCode::kDecimalArithmeticFailed,
                 "reserved amount cannot be reduced on fill");
  auto new_hold = less_hold->Add(remaining_worst_case);
  if (!new_hold.ok())
    return Error(ErrorCode::kDecimalArithmeticFailed,
                 "remaining reservation cannot be calculated");
  auto spent = lease->second.realized.Add(actual_spent);
  if (!spent.ok())
    return Error(ErrorCode::kDecimalArithmeticFailed,
                 "realized spent amount cannot be calculated");
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
  if (it == reservations_.end())
    return Error(ErrorCode::kFundsHoldNotFound, "unknown reservation");
  if (it->second.state == ReservationState::Terminal)
    return Error(ErrorCode::kFundsHoldAlreadyReleased,
                 "reservation already released");
  for (const auto& [asset, amount] : it->second.per_asset_worst_case) {
    auto lease = leases_.find(Key(it->second.account, asset));
    if (lease == leases_.end())
      return Error(ErrorCode::kRiskBudgetMissing, "reservation lease disappeared");
    auto remaining = lease->second.reserved.Subtract(amount);
    if (!remaining.ok())
      return Error(ErrorCode::kDecimalArithmeticFailed,
                   "terminal reservation amount cannot be released");
    lease->second.reserved = *remaining;
  }
  it->second.per_asset_worst_case.clear();
  it->second.state = ReservationState::Terminal;
  return absl::OkStatus();
}

absl::StatusOr<Decimal> RiskGate::Available(const AccountId& account,
                                            const AssetId& asset) const {
  auto it = leases_.find(Key(account, asset));
  if (it == leases_.end())
    return Error(ErrorCode::kRiskBudgetMissing, "asset lease missing");
  auto after_reserved =
      it->second.lease.hard_limit.Subtract(it->second.reserved);
  if (!after_reserved.ok())
    return Error(ErrorCode::kDecimalArithmeticFailed,
                 "available reserved amount cannot be calculated");
  auto available = after_reserved->Subtract(it->second.realized);
  if (!available.ok())
    return Error(ErrorCode::kDecimalArithmeticFailed,
                 "available realized amount cannot be calculated");
  return *available;
}

}  // namespace hquant
