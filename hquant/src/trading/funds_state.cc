#include "hquant/trading/funds_state.h"

#include <algorithm>
#include <limits>
#include <string>
#include <utility>

#include "absl/status/status.h"
#include "hquant/base/fixed.h"

namespace hquant::v1 {

namespace {
constexpr size_t kRecentProjectedLimit = 8192;
}

FundsState::FundsState(std::map<std::string, uint64_t> initial_balances) {
  for (const auto& [asset, amount] : initial_balances) {
    venue_assets_.emplace(asset, VenueAsset{AssetFunds{amount, amount, 0}, 0});
    AssetTracking& tracking = asset_tracking_[asset];
    tracking.projected_total_nanos = amount;
    tracking.timed_baseline_total_nanos = amount;
    tracking.recent_projected_totals.push_back(amount);
  }
}

std::map<std::string, uint64_t> FundsState::ProjectedTotals() const {
  std::map<std::string, uint64_t> totals;
  for (const auto& [asset, tracking] : asset_tracking_) {
    if (tracking.projected_total_nanos) {
      totals.emplace(asset, *tracking.projected_total_nanos);
    }
  }
  return totals;
}

std::map<std::string, AssetFunds> FundsState::Assets() const {
  std::map<std::string, AssetFunds> balances;
  for (const auto& [asset, venue] : venue_assets_) {
    balances.emplace(asset, venue.balance);
  }
  return balances;
}

const AssetFunds* FundsState::Asset(const std::string& asset) const {
  const auto found = venue_assets_.find(asset);
  return found == venue_assets_.end() ? nullptr : &found->second.balance;
}

const FundsHold* FundsState::Hold(const ClientOrderId& id) const {
  const auto found = holds_.find(id.value);
  return found == holds_.end() ? nullptr : &found->second;
}

uint64_t FundsState::Available(const std::string& asset) const {
  const AssetFunds* balance = Asset(asset);
  if (!balance) return 0;
  uint64_t available = balance->free_nanos;
  const auto found = asset_tracking_.find(asset);
  if (found == asset_tracking_.end()) return available;
  const AssetTracking& tracking = found->second;
  if (tracking.pending_debit_nanos >= available) return 0;
  available -= tracking.pending_debit_nanos;
  return tracking.unreflected_nanos >= available
             ? 0
             : available - tracking.unreflected_nanos;
}

uint64_t FundsState::PendingDebit(const std::string& asset) const {
  const auto found = asset_tracking_.find(asset);
  return found == asset_tracking_.end() ? 0 : found->second.pending_debit_nanos;
}

uint64_t FundsState::Reserved(const std::string& asset) const {
  const auto found = asset_tracking_.find(asset);
  return found == asset_tracking_.end() ? 0 : found->second.reserved_nanos;
}

absl::Status FundsState::Reserve(const ClientOrderId& id, std::string asset,
                                 uint64_t amount_nanos) {
  if (id.value.empty() || asset.empty() || amount_nanos == 0) {
    return absl::InvalidArgumentError("invalid funds reservation");
  }
  if (holds_.contains(id.value)) {
    return absl::AlreadyExistsError("duplicate funds reservation");
  }
  if (Available(asset) < amount_nanos) {
    return absl::ResourceExhaustedError("account funds exhausted");
  }
  AssetTracking& tracking = asset_tracking_[asset];
  uint64_t& reserved = tracking.reserved_nanos;
  uint64_t& unreflected = tracking.unreflected_nanos;
  if (amount_nanos > std::numeric_limits<uint64_t>::max() - reserved ||
      amount_nanos > std::numeric_limits<uint64_t>::max() - unreflected) {
    return absl::OutOfRangeError("funds reservation overflow");
  }
  reserved += amount_nanos;
  unreflected += amount_nanos;
  holds_.emplace(id.value, FundsHold{std::move(asset), amount_nanos, 0, false});
  return absl::OkStatus();
}

absl::Status FundsState::SetHold(const ClientOrderId& id,
                                 uint64_t amount_nanos) {
  auto found = holds_.find(id.value);
  if (found == holds_.end()) {
    return absl::NotFoundError("funds reservation missing");
  }
  if (amount_nanos > found->second.amount_nanos) {
    return absl::InvalidArgumentError("funds hold cannot increase");
  }
  const uint64_t difference = found->second.amount_nanos - amount_nanos;
  AssetTracking& tracking = asset_tracking_.at(found->second.asset);
  tracking.reserved_nanos -= difference;
  const uint64_t covered_before = found->second.covered_nanos;
  found->second.covered_nanos =
      std::min(found->second.covered_nanos, amount_nanos);
  tracking.covered_nanos -= covered_before - found->second.covered_nanos;
  if (found->second.covered_nanos == 0)
    tracking.covered_holds.erase(id.value);
  if (found->second.venue_accepted &&
      found->second.covered_nanos < amount_nanos)
    tracking.accepted_uncovered_holds.insert(id.value);
  else
    tracking.accepted_uncovered_holds.erase(id.value);
  tracking.unreflected_nanos -=
      difference - (covered_before - found->second.covered_nanos);
  found->second.amount_nanos = amount_nanos;
  return absl::OkStatus();
}

absl::Status FundsState::MarkVenueAccepted(const ClientOrderId& id) {
  auto found = holds_.find(id.value);
  if (found == holds_.end()) {
    return absl::NotFoundError("funds reservation missing");
  }
  found->second.venue_accepted = true;
  if (found->second.covered_nanos < found->second.amount_nanos)
    asset_tracking_.at(found->second.asset)
        .accepted_uncovered_holds.insert(id.value);
  // Only a subsequent balance observation may cover the local hold.
  return absl::OkStatus();
}

absl::Status FundsState::Release(const ClientOrderId& id) {
  auto found = holds_.find(id.value);
  if (found == holds_.end()) {
    return absl::NotFoundError("funds reservation missing");
  }
  AssetTracking& tracking = asset_tracking_.at(found->second.asset);
  tracking.reserved_nanos -= found->second.amount_nanos;
  tracking.covered_nanos -= found->second.covered_nanos;
  tracking.unreflected_nanos -=
      found->second.amount_nanos - found->second.covered_nanos;
  tracking.covered_holds.erase(id.value);
  tracking.accepted_uncovered_holds.erase(id.value);
  holds_.erase(found);
  return absl::OkStatus();
}

absl::Status FundsState::DebitUntilBalance(const std::string& asset,
                                           uint64_t amount_nanos) {
  if (asset.empty()) return absl::InvalidArgumentError("empty debit asset");
  uint64_t& pending = asset_tracking_[asset].pending_debit_nanos;
  if (amount_nanos > std::numeric_limits<uint64_t>::max() - pending) {
    return absl::OutOfRangeError("pending debit overflow");
  }
  pending += amount_nanos;
  return absl::OkStatus();
}

absl::Status FundsState::RecordFillTotals(const Fill& fill,
                                          const std::string& base_asset,
                                          const std::string& quote_asset) {
  std::map<std::string, uint64_t> changed;
  const auto adjust = [this, &changed](const std::string& asset,
                                       uint64_t amount,
                                       bool credit) -> absl::Status {
    const auto original = asset_tracking_.find(asset);
    if (original == asset_tracking_.end() ||
        !original->second.projected_total_nanos) {
      return absl::DataLossError("fill uses unconfigured asset: " + asset);
    }
    auto [found, inserted] =
        changed.try_emplace(asset, *original->second.projected_total_nanos);
    (void)inserted;
    auto next = credit ? CheckedAdd(found->second, amount)
                       : CheckedSubtract(found->second, amount);
    if (!next.ok()) return next.status();
    found->second = *next;
    return absl::OkStatus();
  };
  const bool buy = fill.side == Side::Buy;
  if (auto status = adjust(buy ? quote_asset : base_asset,
                           buy ? fill.gross_nanos : fill.quantity_nanos, false);
      !status.ok()) {
    return status;
  }
  if (auto status = adjust(buy ? base_asset : quote_asset,
                           buy ? fill.quantity_nanos : fill.gross_nanos, true);
      !status.ok()) {
    return status;
  }
  if (fill.fee_nanos != 0) {
    if (auto status = adjust(fill.fee_asset, fill.fee_nanos, false);
        !status.ok()) {
      return status;
    }
  }
  for (const auto& [asset, total] : changed) {
    if (asset_tracking_.at(asset).timed_effects.size() >=
        kRecentProjectedLimit) {
      return absl::ResourceExhaustedError("fill history requires account sync");
    }
  }
  for (const auto& [asset, total] : changed) {
    AssetTracking& tracking = asset_tracking_.at(asset);
    const uint64_t previous = *tracking.projected_total_nanos;
    tracking.timed_effects.push_back(
        TimedEffect{fill.transaction_ms,
                    total >= previous ? total - previous : previous - total,
                    total >= previous});
    tracking.projected_total_nanos = total;
    auto& recent = tracking.recent_projected_totals;
    recent.push_back(total);
    if (recent.size() > kRecentProjectedLimit) recent.pop_front();
  }
  return absl::OkStatus();
}

bool FundsState::MatchesProjectedTotal(const BalanceUpdate& balance) const {
  const auto found = asset_tracking_.find(balance.asset);
  return balance.total_nanos ==
         (found == asset_tracking_.end() || !found->second.projected_total_nanos
              ? 0
              : *found->second.projected_total_nanos);
}

bool FundsState::MatchesProjectedAt(const BalanceUpdate& balance) const {
  if (balance.version_ms == 0) {
    return MatchesRecentProjectedTotal(balance);
  }
  const auto found = asset_tracking_.find(balance.asset);
  if (found == asset_tracking_.end() || !found->second.projected_total_nanos) {
    return balance.total_nanos == 0;
  }
  const AssetTracking& tracking = found->second;
  __int128 expected = tracking.timed_baseline_total_nanos;
  for (const TimedEffect& effect : tracking.timed_effects) {
    if (effect.transaction_ms > balance.version_ms) continue;
    expected += effect.credit ? static_cast<__int128>(effect.amount_nanos)
                              : -static_cast<__int128>(effect.amount_nanos);
  }
  return expected >= 0 &&
         expected <=
             static_cast<__int128>(std::numeric_limits<uint64_t>::max()) &&
         static_cast<uint64_t>(expected) == balance.total_nanos;
}

bool FundsState::MatchesRecentProjectedTotal(
    const BalanceUpdate& balance) const {
  const auto found = asset_tracking_.find(balance.asset);
  return found == asset_tracking_.end() ||
                 found->second.recent_projected_totals.empty()
             ? balance.total_nanos == 0
             : std::find(found->second.recent_projected_totals.begin(),
                         found->second.recent_projected_totals.end(),
                         balance.total_nanos) !=
                   found->second.recent_projected_totals.end();
}

void FundsState::ConfirmRecentProjectedTotal(const BalanceUpdate& balance) {
  const auto found = asset_tracking_.find(balance.asset);
  if (found == asset_tracking_.end()) return;
  auto& recent = found->second.recent_projected_totals;
  for (size_t index = recent.size(); index > 0; --index) {
    if (recent[index - 1] == balance.total_nanos) {
      recent.erase(recent.begin(), recent.begin() + index - 1);
      return;
    }
  }
}

absl::Status FundsState::ApplyBalance(const BalanceUpdate& balance) {
  if (balance.asset.empty() || balance.available_nanos > balance.total_nanos ||
      balance.locked_nanos > balance.total_nanos ||
      balance.available_nanos >
          std::numeric_limits<uint64_t>::max() - balance.locked_nanos ||
      balance.available_nanos + balance.locked_nanos != balance.total_nanos) {
    return absl::DataLossError("invalid venue balance");
  }
  if (IsStaleBalance(balance)) return absl::OkStatus();
  const AssetFunds* previous = Asset(balance.asset);
  const uint64_t previously_locked = previous ? previous->locked_nanos : 0;
  const uint64_t reflected_debit =
      previous && previous->total_nanos > balance.total_nanos
          ? previous->total_nanos - balance.total_nanos
          : 0;
  const auto tracking = asset_tracking_.find(balance.asset);
  if (tracking != asset_tracking_.end()) {
    uint64_t& pending = tracking->second.pending_debit_nanos;
    pending -= std::min(pending, reflected_debit);
  }
  VenueAsset& venue = venue_assets_[balance.asset];
  venue.balance = AssetFunds{balance.total_nanos, balance.available_nanos,
                             balance.locked_nanos};
  if (balance.version_ms != 0) {
    venue.balance_version_ms = balance.version_ms;
  }
  ReconcileLocked(balance.asset, balance.locked_nanos > previously_locked
                                     ? balance.locked_nanos - previously_locked
                                     : 0);
  return absl::OkStatus();
}

bool FundsState::IsStaleBalance(const BalanceUpdate& balance) const {
  const auto found = venue_assets_.find(balance.asset);
  return balance.version_ms != 0 && found != venue_assets_.end() &&
         found->second.balance_version_ms != 0 &&
         balance.version_ms < found->second.balance_version_ms;
}

void FundsState::SettleVerifiedSnapshot() {
  for (auto& [asset, tracking] : asset_tracking_) {
    tracking.pending_debit_nanos = 0;
    tracking.timed_effects.clear();
    if (tracking.projected_total_nanos) {
      tracking.timed_baseline_total_nanos = *tracking.projected_total_nanos;
      tracking.recent_projected_totals = {*tracking.projected_total_nanos};
    }
  }
  for (const auto& [asset, venue] : venue_assets_) {
    ReconcileLocked(asset, venue.balance.locked_nanos);
  }
}

void FundsState::ReconcileLocked(const std::string& asset,
                                 uint64_t newly_locked_nanos) {
  const AssetFunds* balance = Asset(asset);
  if (!balance) return;
  const auto tracking = asset_tracking_.find(asset);
  if (tracking == asset_tracking_.end()) return;
  AssetTracking& state = tracking->second;
  while (state.covered_nanos > balance->locked_nanos) {
    const std::string id = *state.covered_holds.begin();
    FundsHold& hold = holds_.at(id);
    const uint64_t removed =
        std::min(state.covered_nanos - balance->locked_nanos,
                 hold.covered_nanos);
    hold.covered_nanos -= removed;
    state.covered_nanos -= removed;
    state.unreflected_nanos += removed;
    if (hold.covered_nanos == 0) state.covered_holds.erase(id);
    if (hold.venue_accepted && hold.covered_nanos < hold.amount_nanos)
      state.accepted_uncovered_holds.insert(id);
  }
  uint64_t available_coverage =
      std::min(newly_locked_nanos, balance->locked_nanos - state.covered_nanos);
  while (available_coverage != 0 &&
         !state.accepted_uncovered_holds.empty()) {
    const std::string id = *state.accepted_uncovered_holds.begin();
    FundsHold& hold = holds_.at(id);
    const uint64_t added =
        std::min(available_coverage, hold.amount_nanos - hold.covered_nanos);
    hold.covered_nanos += added;
    state.covered_nanos += added;
    state.unreflected_nanos -= added;
    state.covered_holds.insert(id);
    if (hold.covered_nanos == hold.amount_nanos)
      state.accepted_uncovered_holds.erase(id);
    available_coverage -= added;
  }
}

absl::Status FundsState::ApplyDelta(const BalanceDelta& delta) {
  auto found = venue_assets_.find(delta.asset);
  if (delta.asset.empty() || found == venue_assets_.end()) {
    return absl::DataLossError("delta for unknown asset");
  }
  AssetFunds& balance = found->second.balance;
  const uint64_t amount = delta.amount_nanos;
  if (delta.external_lock) {
    if (delta.credit) {
      if (amount > balance.free_nanos ||
          amount > std::numeric_limits<uint64_t>::max() - balance.locked_nanos)
        return absl::DataLossError("external lock exceeds free funds");
      balance.free_nanos -= amount;
      balance.locked_nanos += amount;
    } else {
      if (amount > balance.locked_nanos ||
          amount > std::numeric_limits<uint64_t>::max() - balance.free_nanos)
        return absl::DataLossError("external unlock exceeds locked funds");
      balance.locked_nanos -= amount;
      balance.free_nanos += amount;
    }
  } else if (delta.credit) {
    if (amount > std::numeric_limits<uint64_t>::max() - balance.free_nanos ||
        amount > std::numeric_limits<uint64_t>::max() - balance.total_nanos)
      return absl::OutOfRangeError("balance credit overflow");
    balance.free_nanos += amount;
    balance.total_nanos += amount;
  } else {
    if (amount > balance.free_nanos || amount > balance.total_nanos)
      return absl::DataLossError("balance debit exceeds free funds");
    balance.free_nanos -= amount;
    balance.total_nanos -= amount;
  }
  return absl::OkStatus();
}

}  // namespace hquant::v1
