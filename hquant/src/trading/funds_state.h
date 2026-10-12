#pragma once

#include <cstdint>
#include <deque>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "hquant/trading/trading_event.h"

namespace hquant::v1 {

struct AssetFunds {
  uint64_t total_nanos = 0;
  uint64_t free_nanos = 0;
  uint64_t locked_nanos = 0;
};

struct FundsHold {
  std::string asset;
  uint64_t amount_nanos = 0;
  // 已计入交易所 locked 的占用。
  uint64_t covered_nanos = 0;
  bool venue_accepted = false;
};

// 账户级资金状态：交易所余额用于计算可用额，成交投影用于对账。
// 可用额 = max(0, free - 未反映的订单占用 - 未反映的成交借记)。
class FundsState {
 public:
  explicit FundsState(std::map<std::string, uint64_t> initial_balances);

  absl::Status Reserve(const ClientOrderId& id, std::string asset,
                       uint64_t amount_nanos);
  absl::Status SetHold(const ClientOrderId& id, uint64_t amount_nanos);
  absl::Status MarkVenueAccepted(const ClientOrderId& id);
  absl::Status Release(const ClientOrderId& id);
  absl::Status DebitUntilBalance(const std::string& asset,
                                 uint64_t amount_nanos);
  absl::Status RecordFillTotals(const Fill& fill, const std::string& base_asset,
                                const std::string& quote_asset);
  bool MatchesProjectedTotal(const BalanceUpdate& balance) const;
  bool MatchesProjectedAt(const BalanceUpdate& balance) const;
  bool MatchesRecentProjectedTotal(const BalanceUpdate& balance) const;
  void ConfirmRecentProjectedTotal(const BalanceUpdate& balance);
  std::map<std::string, uint64_t> ProjectedTotals() const;
  absl::Status ApplyBalance(const BalanceUpdate& balance);
  bool IsStaleBalance(const BalanceUpdate& balance) const;
  void SettleVerifiedSnapshot();
  absl::Status ApplyDelta(const BalanceDelta& delta);
  uint64_t Available(const std::string& asset) const;
  uint64_t Reserved(const std::string& asset) const;
  uint64_t PendingDebit(const std::string& asset) const;
  const AssetFunds* Asset(const std::string& asset) const;
  const FundsHold* Hold(const ClientOrderId& id) const;
  std::map<std::string, AssetFunds> Assets() const;

 private:
  struct TimedEffect {
    uint64_t transaction_ms = 0;
    uint64_t amount_nanos = 0;
    bool credit = false;
  };

  struct VenueAsset {
    // 当前余额；初始值来自配置。
    AssetFunds balance;
    // 最近余额回报时间，用于忽略乱序回报。
    uint64_t balance_version_ms = 0;
  };

  struct AssetTracking {
    // 成交推算总额；空值表示未配置该资产。
    std::optional<uint64_t> projected_total_nanos;
    // 最近一次全量对账的总额。
    uint64_t timed_baseline_total_nanos = 0;
    // 对账后成交的时间与资产增减。
    std::vector<TimedEffect> timed_effects;
    // 无时间戳余额回报可匹配的近期总额。
    std::deque<uint64_t> recent_projected_totals;
    // 全部订单占用，供账户额度检查。
    uint64_t reserved_nanos = 0;
    // 尚未计入交易所 locked 的订单占用。
    uint64_t unreflected_nanos = 0;
    // 已归因于本地订单的交易所 locked，占用集合按客户订单 ID 排序。
    uint64_t covered_nanos = 0;
    std::set<std::string> covered_holds;
    std::set<std::string> accepted_uncovered_holds;
    // 已成交、尚未计入交易所余额的借记。
    uint64_t pending_debit_nanos = 0;
  };

  void ReconcileLocked(const std::string& asset, uint64_t newly_locked_nanos);

  // 资产 → 当前余额与回报时间。
  std::map<std::string, VenueAsset> venue_assets_;
  // 资产 → 成交投影与资金占用汇总。
  std::map<std::string, AssetTracking> asset_tracking_;
  // 客户订单 ID → 单笔资金占用。
  std::map<std::string, FundsHold> holds_;
};

}  // namespace hquant::v1
