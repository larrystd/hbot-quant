#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <tuple>
#include <variant>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "base/error.h"
#include "base/market.h"
#include "base/order.h"
#include "base/types.h"

namespace hquant {

struct OrderRoute {
  StrategyId strategy_id;
  ShardId current_shard;
};

// The private-stream reader owns this index. Registration and lookup must be
// serialized on that reader; other shards send registrations to it as messages.
// A codec validates the complete exchange client ID before returning its stable
// strategy_id key. A shard hint encoded in that ID is never used for routing.
class OrderStrategyIndex {
 public:
  using DecodeStrategyId =
      std::function<std::optional<uint64_t>(const ClientOrderId&)>;

  explicit OrderStrategyIndex(DecodeStrategyId decode_strategy_id = {});

  // Calling again with the same strategy_id/account changes its current shard after
  // a restart. Historical client/exchange mappings keep their logical strategy_id.
  absl::Status SetStrategyRoute(StrategyId strategy_id, AccountId account, ShardId shard);
  absl::Status RegisterClient(ClientOrderId client_id, AccountId account,
                              MarketId market, const StrategyId& strategy_id);
  absl::Status RegisterExchange(
      AccountId account, MarketId market, ExchangeOrderId exchange_id,
      const StrategyId& strategy_id,
      std::optional<ClientOrderId> client_id = std::nullopt);

  absl::StatusOr<OrderRoute> Resolve(
      const AccountId& account, const MarketId& market,
      const std::optional<ClientOrderId>& client_id,
      const std::optional<ExchangeOrderId>& exchange_id) const;
  bool KnowsAccount(const AccountId& account) const;

 private:
  struct StrategyRoute {
    StrategyId strategy_id;
    AccountId account;
    ShardId shard;
  };
  struct ClientMapping {
    AccountId account;
    MarketId market;
    uint64_t strategy_id = 0;
  };
  using ExchangeKey =
      std::tuple<std::string, std::string, int, std::string, std::string>;
  static ExchangeKey Key(const AccountId& account, const MarketId& market,
                         const ExchangeOrderId& exchange_id);

  DecodeStrategyId decode_strategy_id_;
  std::map<uint64_t, StrategyRoute> strategy_routes_;
  std::map<std::string, ClientMapping> clients_;
  std::map<ExchangeKey, uint64_t> exchanges_;
};

using PrivateReport = std::variant<OrderUpdate, TradeUpdate>;

struct RoutedPrivateReport {
  StrategyId strategy_id;
  ShardId target_shard;
  uint64_t source_sequence = 0;
  PrivateReport report;
};

enum class RouteDisposition { Local, Forwarded, Quarantined };

struct RouteResult {
  RouteDisposition disposition = RouteDisposition::Quarantined;
  ErrorCode failure = ErrorCode::kOk;
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
  ErrorCode reason = ErrorCode::kRouteStrategyUnknown;
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
      Options options, OrderStrategyIndex& index);
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

  PrivateReportRouter(Options options, OrderStrategyIndex& index);
  RouteResult Quarantine(PrivateReport report, RouteResult result);

  Options options_;
  OrderStrategyIndex& index_;
  std::array<std::unique_ptr<Queue>, 8> queues_;
  std::deque<QuarantinedPrivateReport> quarantine_;
  std::set<std::string> paused_accounts_;
  uint64_t next_source_sequence_ = 1;
  uint64_t dropped_quarantine_count_ = 0;
};

}  // namespace hquant
