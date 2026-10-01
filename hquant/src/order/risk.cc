#include "order/risk.h"

#include <utility>

#include "absl/status/status.h"
#include "base/error.h"

namespace hquant {

RiskBudgetAllocator::AccountAssetKey RiskBudgetAllocator::Key(
    const AccountId& account, const AssetId& asset) {
  return {account.value, asset.value};
}

RiskBudgetAllocator::BudgetKey RiskBudgetAllocator::Key(
    const RiskBudget& budget) {
  return {budget.account.value, budget.asset.value, budget.shard.value};
}

absl::Status RiskBudgetAllocator::SetConservativeLimit(const AccountId& account,
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

absl::Status RiskBudgetAllocator::GrantInitial(RiskBudget budget, UtcTime now) {
  if (budget.account.value.empty() || budget.asset.value.empty() ||
      !budget.shard.IsValid() || budget.budget_version == 0 ||
      !budget.hard_limit.IsNonnegative() || budget.valid_until_utc <= now) {
    return Error(ErrorCode::kRiskConfigInvalid, "invalid initial risk budget");
  }
  const auto account_asset = Key(budget.account, budget.asset);
  const auto limit = limits_.find(account_asset);
  if (limit == limits_.end()) {
    return Error(ErrorCode::kRiskBudgetMissing,
                 "conservative account limit missing");
  }
  if (budgets_.contains(Key(budget))) {
    return Error(ErrorCode::kRiskConfigInvalid,
                 "shard risk budget already granted");
  }
  const auto old_total = totals_.find(account_asset);
  const Decimal current =
      old_total == totals_.end() ? Decimal() : old_total->second;
  auto proposed = current.Add(budget.hard_limit);
  if (!proposed.ok())
    return Error(ErrorCode::kRiskConfigInvalid,
                 "static risk budget total cannot be calculated");
  auto within_limit = proposed->Compare(limit->second);
  if (!within_limit.ok())
    return Error(ErrorCode::kRiskConfigInvalid,
                 "static risk budget limit cannot be compared");
  if (*within_limit > 0) {
    return Error(ErrorCode::kRiskConfigInvalid,
                 "static risk budgets exceed account limit");
  }
  totals_.insert_or_assign(account_asset, *proposed);
  budgets_.emplace(Key(budget), std::move(budget));
  return absl::OkStatus();
}

absl::Status RiskBudgetAllocator::RenewAfterReconciliation(RiskBudget budget,
                                                           UtcTime now,
                                                           bool account_fresh) {
  if (!account_fresh) {
    return Error(ErrorCode::kRiskAccountStale,
                 "account facts stale for budget renewal");
  }
  auto existing = budgets_.find(Key(budget));
  if (existing == budgets_.end())
    return Error(ErrorCode::kRiskBudgetMissing, "risk budget missing");
  if (budget.budget_version <= existing->second.budget_version ||
      budget.valid_until_utc <= now ||
      budget.valid_until_utc <= existing->second.valid_until_utc) {
    return Error(ErrorCode::kRiskBudgetRenewalStale,
                 "risk budget version or expiry stale");
  }
  auto same_limit = budget.hard_limit.Compare(existing->second.hard_limit);
  if (!same_limit.ok())
    return Error(ErrorCode::kRiskConfigInvalid,
                 "risk budget limits cannot be compared");
  if (*same_limit != 0) {
    return Error(ErrorCode::kRiskConfigInvalid,
                 "static risk allocation cannot change");
  }
  existing->second = std::move(budget);
  return absl::OkStatus();
}

absl::StatusOr<RiskBudget> RiskBudgetAllocator::FindBudget(
    const AccountId& account, const AssetId& asset, ShardId shard) const {
  const auto it = budgets_.find({account.value, asset.value, shard.value});
  if (it == budgets_.end())
    return Error(ErrorCode::kRiskBudgetMissing, "risk budget missing");
  return it->second;
}

absl::StatusOr<Decimal> RiskBudgetAllocator::GrantedTotal(
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

RiskGate::BudgetKey RiskGate::Key(const AccountId& account,
                                  const AssetId& asset) const {
  return {account.value, asset.value};
}

absl::Status RiskGate::SetInitialBudget(RiskBudget budget) {
  if (!settings_.shard.IsValid() || budget.shard != settings_.shard ||
      budget.account.value.empty() || budget.asset.value.empty() ||
      budget.budget_version == 0 || !budget.hard_limit.IsNonnegative()) {
    return Error(ErrorCode::kRiskConfigInvalid, "invalid risk budget");
  }
  const auto key = Key(budget.account, budget.asset);
  if (budgets_.contains(key))
    return Error(ErrorCode::kRiskConfigInvalid, "duplicate risk budget");
  budgets_.emplace(key, BudgetState{std::move(budget), Decimal(), Decimal()});
  return absl::OkStatus();
}

absl::Status RiskGate::RenewAfterReconciliation(RiskBudget budget, UtcTime now,
                                                bool account_fresh) {
  if (emergency_stop_)
    return Error(ErrorCode::kRiskEmergencyStopped, "emergency stop");
  if (!account_fresh) {
    return Error(ErrorCode::kRiskAccountStale,
                 "account not ready for budget renewal");
  }
  if (budget.shard != settings_.shard) {
    return Error(ErrorCode::kRiskConfigInvalid,
                 "risk budget belongs to another shard");
  }
  auto it = budgets_.find(Key(budget.account, budget.asset));
  if (it == budgets_.end())
    return Error(ErrorCode::kRiskBudgetMissing, "risk budget missing");
  if (budget.budget_version <= it->second.budget.budget_version ||
      budget.valid_until_utc <= now ||
      budget.valid_until_utc <= it->second.budget.valid_until_utc) {
    return Error(ErrorCode::kRiskBudgetRenewalStale,
                 "risk budget version or expiry stale");
  }
  auto same_limit = budget.hard_limit.Compare(it->second.budget.hard_limit);
  if (!same_limit.ok())
    return Error(ErrorCode::kRiskConfigInvalid,
                 "risk budget limits cannot be compared");
  if (*same_limit != 0) {
    return Error(ErrorCode::kRiskConfigInvalid,
                 "static risk allocation cannot change");
  }
  it->second.budget = std::move(budget);
  return absl::OkStatus();
}

absl::StatusOr<FundsHold> RiskGate::TryHold(const StrategyId& strategy_id,
                                            const OrderRequest& request,
                                            const MarketSpec& market,
                                            const TradingRule& rule,
                                            UtcTime now, bool market_live,
                                            bool account_fresh) {
  FundsHold hold;
  std::string_view detail;
  const ErrorCode code =
      TryHoldCode(strategy_id, request, market, rule, now, market_live,
                  account_fresh, &hold, &detail);
  if (code != ErrorCode::kOk) return Error(code, detail);
  return hold;
}

ErrorCode RiskGate::TryHoldCode(const StrategyId& strategy_id,
                                const OrderRequest& request,
                                const MarketSpec& market,
                                const TradingRule& rule, UtcTime now,
                                bool market_live, bool account_fresh,
                                FundsHold* out, std::string_view* detail) {
  const auto reject = [detail](ErrorCode code, std::string_view message) {
    if (detail) *detail = message;
    return code;
  };
  if (!out) return reject(ErrorCode::kInternal, "null risk hold output");
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
    return reject(ErrorCode::kDecimalArithmeticFailed,
                  "price quantization failed");
  if (!amount.ok())
    return reject(ErrorCode::kDecimalArithmeticFailed,
                  "amount quantization failed");
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
  auto it = budgets_.find(Key(request.account, spent_asset));
  if (it == budgets_.end())
    return reject(ErrorCode::kRiskBudgetMissing, "missing asset budget");
  BudgetState& budget = it->second;
  if (now >= budget.budget.valid_until_utc) {
    return reject(ErrorCode::kRiskBudgetExpired, "asset budget expired");
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
  auto after_reserved = budget.budget.hard_limit.Subtract(budget.reserved);
  if (!after_reserved.ok())
    return reject(ErrorCode::kDecimalArithmeticFailed,
                  "reserved amount calculation failed");
  auto available = after_reserved->Subtract(budget.realized);
  if (!available.ok())
    return reject(ErrorCode::kDecimalArithmeticFailed,
                  "available amount calculation failed");
  auto enough = available->Compare(*needed);
  if (!enough.ok())
    return reject(ErrorCode::kDecimalArithmeticFailed,
                  "available amount comparison failed");
  if (*enough < 0)
    return reject(ErrorCode::kRiskBudgetExhausted, "risk budget exhausted");
  if (next_hold_id_ == 0) {
    EmergencyStop();
    return reject(ErrorCode::kSequenceExhausted, "hold id exhausted");
  }
  auto new_reserved = budget.reserved.Add(*needed);
  if (!new_reserved.ok())
    return reject(ErrorCode::kDecimalArithmeticFailed,
                  "hold total calculation failed");
  const HoldId id{next_hold_id_++};
  FundsHold hold;
  hold.hold_id = id;
  hold.account = request.account;
  hold.strategy_id = strategy_id;
  hold.budget_version = budget.budget.budget_version;
  hold.per_asset_worst_case.emplace(spent_asset, *needed);
  holds_.emplace(id.value, hold);
  budget.reserved = *new_reserved;
  *out = std::move(hold);
  if (detail) *detail = {};
  return ErrorCode::kOk;
}

absl::Status RiskGate::AttachClientId(HoldId hold, ClientOrderId client_id) {
  auto it = holds_.find(hold.value);
  if (it == holds_.end())
    return Error(ErrorCode::kFundsHoldNotFound, "unknown hold");
  if (it->second.state == HoldState::Released)
    return Error(ErrorCode::kFundsHoldAlreadyReleased, "hold already released");
  if (client_id.value.empty() || it->second.client_id) {
    return Error(ErrorCode::kFundsHoldClientIdConflict,
                 "client ID empty or already attached");
  }
  it->second.client_id = std::move(client_id);
  return absl::OkStatus();
}

absl::Status RiskGate::MarkSubmissionUnknown(HoldId hold) {
  auto it = holds_.find(hold.value);
  if (it == holds_.end())
    return Error(ErrorCode::kFundsHoldNotFound, "unknown hold");
  if (it->second.state == HoldState::Released)
    return Error(ErrorCode::kFundsHoldAlreadyReleased, "hold already released");
  it->second.state = HoldState::SubmissionUnknown;
  return absl::OkStatus();
}

absl::Status RiskGate::ApplyTrade(HoldId hold, const AssetId& spent_asset,
                                  const Decimal& actual_spent,
                                  const Decimal& remaining_worst_case) {
  auto it = holds_.find(hold.value);
  if (it == holds_.end())
    return Error(ErrorCode::kFundsHoldNotFound, "unknown hold");
  if (it->second.state == HoldState::Released)
    return Error(ErrorCode::kFundsHoldAlreadyReleased, "hold already released");
  if (!actual_spent.IsNonnegative() || !remaining_worst_case.IsNonnegative()) {
    return Error(ErrorCode::kOrderPriceOrAmountInvalid,
                 "fill amounts must not be negative");
  }
  auto held_asset = it->second.per_asset_worst_case.find(spent_asset);
  auto budget = budgets_.find(Key(it->second.account, spent_asset));
  if (held_asset == it->second.per_asset_worst_case.end() ||
      budget == budgets_.end()) {
    return Error(ErrorCode::kRiskBudgetMissing, "missing fill asset budget");
  }
  auto decreasing = held_asset->second.Compare(remaining_worst_case);
  if (!decreasing.ok())
    return Error(ErrorCode::kDecimalArithmeticFailed,
                 "fill hold amount cannot be compared");
  if (*decreasing < 0)
    return Error(ErrorCode::kFundsHoldIncreased,
                 "hold cannot increase on fill");
  auto less_hold = budget->second.reserved.Subtract(held_asset->second);
  if (!less_hold.ok())
    return Error(ErrorCode::kDecimalArithmeticFailed,
                 "reserved amount cannot be reduced on fill");
  auto new_hold = less_hold->Add(remaining_worst_case);
  if (!new_hold.ok())
    return Error(ErrorCode::kDecimalArithmeticFailed,
                 "remaining hold cannot be calculated");
  auto spent = budget->second.realized.Add(actual_spent);
  if (!spent.ok())
    return Error(ErrorCode::kDecimalArithmeticFailed,
                 "realized spent amount cannot be calculated");
  budget->second.reserved = *new_hold;
  budget->second.realized = *spent;
  held_asset->second = remaining_worst_case;
  auto available = Available(it->second.account, spent_asset);
  if (!available.ok()) return available.status();
  if (!available->IsNonnegative()) EmergencyStop();
  return absl::OkStatus();
}

absl::Status RiskGate::Release(HoldId hold) {
  auto it = holds_.find(hold.value);
  if (it == holds_.end())
    return Error(ErrorCode::kFundsHoldNotFound, "unknown hold");
  if (it->second.state == HoldState::Released)
    return Error(ErrorCode::kFundsHoldAlreadyReleased, "hold already released");
  for (const auto& [asset, amount] : it->second.per_asset_worst_case) {
    auto budget = budgets_.find(Key(it->second.account, asset));
    if (budget == budgets_.end())
      return Error(ErrorCode::kRiskBudgetMissing, "hold budget disappeared");
    auto remaining = budget->second.reserved.Subtract(amount);
    if (!remaining.ok())
      return Error(ErrorCode::kDecimalArithmeticFailed,
                   "terminal hold amount cannot be released");
    budget->second.reserved = *remaining;
  }
  it->second.per_asset_worst_case.clear();
  it->second.state = HoldState::Released;
  return absl::OkStatus();
}

absl::StatusOr<Decimal> RiskGate::Available(const AccountId& account,
                                            const AssetId& asset) const {
  auto it = budgets_.find(Key(account, asset));
  if (it == budgets_.end())
    return Error(ErrorCode::kRiskBudgetMissing, "asset budget missing");
  auto after_reserved =
      it->second.budget.hard_limit.Subtract(it->second.reserved);
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
