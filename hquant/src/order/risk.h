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

struct RiskBudget {
  AccountId account;
  AssetId asset;
  ShardId shard;
  uint64_t budget_version = 0;
  Decimal hard_limit;
  UtcTime valid_until_utc{};
};

enum class HoldState { Active, SubmissionUnknown, Released };

struct FundsHold {
  HoldId hold_id;
  std::optional<ClientOrderId> client_id;
  AccountId account;
  StrategyId strategy_id;
  uint64_t budget_version = 0;
  absl::flat_hash_map<AssetId, Decimal> per_asset_worst_case;
  HoldState state = HoldState::Active;
};

// Owned by the control thread. This checks the complete static allocation
// before individual shard gates receive their budgets. No runtime transfer or
// increase of a shard's allocation is permitted.
class RiskBudgetAllocator {
 public:
  absl::Status SetConservativeLimit(const AccountId& account,
                                    const AssetId& asset, const Decimal& limit);
  absl::Status GrantInitial(RiskBudget budget, UtcTime now);
  absl::Status RenewAfterOrderQuery(RiskBudget budget, UtcTime now,
                                        bool account_fresh);
  absl::StatusOr<RiskBudget> FindBudget(const AccountId& account,
                                        const AssetId& asset,
                                        ShardId shard) const;
  absl::StatusOr<Decimal> GrantedTotal(const AccountId& account,
                                       const AssetId& asset) const;

 private:
  using AccountAssetKey = std::pair<std::string, std::string>;
  using BudgetKey = std::tuple<std::string, std::string, uint8_t>;
  static AccountAssetKey Key(const AccountId& account, const AssetId& asset);
  static BudgetKey Key(const RiskBudget& budget);

  std::map<AccountAssetKey, Decimal> limits_;
  std::map<AccountAssetKey, Decimal> totals_;
  std::map<BudgetKey, RiskBudget> budgets_;
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
  absl::Status SetInitialBudget(RiskBudget budget);
  // Reconciliation may extend the expiry and version, never the allocation.
  absl::Status RenewAfterOrderQuery(RiskBudget budget, UtcTime now,
                                        bool account_fresh);
  absl::StatusOr<FundsHold> TryHold(const StrategyId& strategy_id,
                                    const OrderRequest& request,
                                    const MarketSpec& market,
                                    const TradingRule& rule, UtcTime now,
                                    bool market_live, bool account_fresh);
  // Hot-path hold: rejection returns only a code. On success, writes
  // the hold to out. The optional detail is a static diagnostic.
  ErrorCode TryHoldCode(const StrategyId& strategy_id,
                        const OrderRequest& request, const MarketSpec& market,
                        const TradingRule& rule, UtcTime now, bool market_live,
                        bool account_fresh, FundsHold* out,
                        std::string_view* detail = nullptr);
  absl::Status AttachClientId(HoldId hold, ClientOrderId client_id);
  absl::Status MarkSubmissionUnknown(HoldId hold);
  // Move a verified spent amount from outstanding hold into realized usage.
  absl::Status ApplyTrade(HoldId hold, const AssetId& spent_asset,
                          const Decimal& actual_spent,
                          const Decimal& remaining_worst_case);
  absl::Status Release(HoldId hold);
  absl::StatusOr<Decimal> Available(const AccountId& account,
                                    const AssetId& asset) const;
  void EmergencyStop() { emergency_stop_ = true; }
  bool emergency_stopped() const { return emergency_stop_; }

 private:
  using BudgetKey = std::pair<std::string, std::string>;
  struct BudgetState {
    RiskBudget budget;
    Decimal reserved;
    Decimal realized;
  };

  BudgetKey Key(const AccountId& account, const AssetId& asset) const;
  Settings settings_;
  std::map<BudgetKey, BudgetState> budgets_;
  std::map<uint64_t, FundsHold> holds_;
  uint64_t next_hold_id_ = 1;
  bool emergency_stop_ = false;
};

}  // namespace hquant
