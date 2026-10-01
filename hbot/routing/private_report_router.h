#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <variant>
#include <vector>

#include "absl/status/statusor.h"
#include "hbot/base/ids.h"
#include "hbot/model/order.h"
#include "hbot/routing/order_ownership_index.h"

namespace hbot {

using PrivateReport = std::variant<OrderUpdate, TradeUpdate>;

struct RoutedPrivateReport {
  OwnerId owner;
  ShardId target_shard;
  uint64_t source_sequence = 0;
  PrivateReport report;
};

enum class RouteDisposition { Local, Forwarded, Quarantined };
enum class RouteFailure {
  None,
  UnknownOwnership,
  ConflictingOwnership,
  QueueFull,
  InvalidReport
};

struct RouteResult {
  RouteDisposition disposition = RouteDisposition::Quarantined;
  RouteFailure failure = RouteFailure::None;
  AccountId account;
  uint64_t source_sequence = 0;
  std::optional<RoutedPrivateReport> local_report;
  std::optional<ShardId> wake_shard;
  bool pause_account = false;
  bool request_reconcile = false;
  bool quarantine_dropped = false;
};

struct QuarantinedPrivateReport {
  PrivateReport report;
  RouteFailure reason = RouteFailure::UnknownOwnership;
  uint64_t source_sequence = 0;
};

// Route is called only by the account private-stream reader. Each target shard
// alone calls TryPop for its SPSC queue. The caller posts one drain handler
// when wake_shard is set; both blocking and busy loops bound each drain.
class PrivateReportRouter {
 public:
  struct Options {
    ShardId reader_shard;
    size_t queue_capacity_per_shard = 1024;
    size_t quarantine_capacity = 1024;
  };

  static absl::StatusOr<std::unique_ptr<PrivateReportRouter>> Create(
      Options options, OrderOwnershipIndex& index);
  PrivateReportRouter(const PrivateReportRouter&) = delete;
  PrivateReportRouter& operator=(const PrivateReportRouter&) = delete;

  RouteResult Route(PrivateReport report);
  std::optional<RoutedPrivateReport> TryPop(ShardId target_shard);
  std::optional<QuarantinedPrivateReport> TryPopQuarantined();
  bool IsAccountPaused(const AccountId& account) const;
  // Call only after REST/order/trade reconciliation has verified the gap.
  void MarkReconciled(const AccountId& account);
  size_t QuarantineSize() const { return quarantine_.size(); }
  uint64_t DroppedQuarantineCount() const { return dropped_quarantine_count_; }

 private:
  struct Queue {
    explicit Queue(size_t capacity) : slots(capacity) {}
    std::vector<std::optional<RoutedPrivateReport>> slots;
    alignas(64) std::atomic<size_t> head{0};
    alignas(64) std::atomic<size_t> tail{0};
  };

  PrivateReportRouter(Options options, OrderOwnershipIndex& index);
  RouteResult Quarantine(PrivateReport report, RouteResult result);

  Options options_;
  OrderOwnershipIndex& index_;
  std::array<std::unique_ptr<Queue>, 8> queues_;
  std::deque<QuarantinedPrivateReport> quarantine_;
  std::set<std::string> paused_accounts_;
  uint64_t next_source_sequence_ = 1;
  uint64_t dropped_quarantine_count_ = 0;
};

}  // namespace hbot
