#pragma once

#include <chrono>
#include <cstddef>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "base/net.h"
#include "base/order.h"
#include "boost/asio/awaitable.hpp"

namespace hquant::binance_spot {

struct AccountPushBatch {
  // For executionReport TRADE, the fill precedes its status update. The
  // OrderTracker handles duplicate trade IDs and either arrival order.
  std::vector<AccountEvent> events;
  bool stream_terminated = false;
  bool account_resync_required = false;
};

// Pure JSON adapter. Network subscription/reconnect and account-level routing
// belong to their owning components; this object holds no mutable order state.
class AccountPushParser {
 public:
  AccountPushParser(AccountId account, ExchangeId exchange)
      : account_(std::move(account)), exchange_(std::move(exchange)) {}

  absl::StatusOr<AccountPushBatch> Parse(std::string_view json,
                                         EventTime received) const;

 private:
  AccountId account_;
  ExchangeId exchange_;
};

}  // namespace hquant::binance_spot

namespace hquant::binance_spot {

struct OrderToQuery {
  AccountId account;
  MarketId market;
  ClientOrderId client_order_id;
};

struct StartupQueryInput {
  AccountId account;
  std::vector<MarketId> assigned_markets;
  std::vector<PreparedOrder> persisted_prepared_orders;
  std::vector<OrderSnapshot> live_snapshots;
  bool history_complete = true;
  bool previous_run_clean = true;
  bool executor_checkpoints_complete = true;
};

struct StartupQueryPlan {
  std::vector<OrderToQuery> known_orders;
  // A crash/history gap can omit the prepared order itself. Scan these markets
  // for exchange orders before resuming trading or stateful executors.
  std::vector<MarketId> markets_to_scan;
  bool pause_stateful_executors = false;
};

absl::StatusOr<StartupQueryPlan> PlanStartupQueries(
    const StartupQueryInput& input);

// The implementation adds timestamp, recvWindow, signature and API key,
// applies rate limits, and performs GET through the existing HTTP transport.
// The private package never logs or stores credentials.
class SignedRestClient {
 public:
  virtual ~SignedRestClient() = default;
  virtual boost::asio::awaitable<absl::StatusOr<HttpResponse>> GetSigned(
      std::string path_and_query,
      std::chrono::steady_clock::time_point deadline) = 0;
};

struct OrderQueryResult {
  OrderToQuery target;
  // Present only when complete. Incomplete queries can still return verified
  // partial trades, but cannot confirm an uncertain submission.
  std::optional<OrderUpdate> order;
  std::vector<TradeUpdate> trades;
  bool complete = false;
  // Incomplete/404/429/transport failures preserve SubmissionUnknown and its
  // funds hold. A caller applies trade details before the order status.
  absl::Status unresolved_status;
};

class OrderQueryClient {
 public:
  struct Limits {
    size_t max_trade_pages = 8;
    size_t trades_per_page = 1000;
    size_t max_response_bytes = 2 * 1024 * 1024;
  };

  explicit OrderQueryClient(SignedRestClient& rest) : rest_(rest) {}
  OrderQueryClient(SignedRestClient& rest, Limits limits)
      : rest_(rest), limits_(limits) {}

  boost::asio::awaitable<absl::StatusOr<OrderQueryResult>> QueryOrder(
      OrderToQuery target, EventTime received,
      std::chrono::steady_clock::time_point deadline);

  // Returns exchange-discovered client IDs after an unclean restart. The
  // caller still validates stable strategy_id identity and queries each
  // original ID.
  boost::asio::awaitable<absl::StatusOr<std::vector<OrderToQuery>>>
  ListOpenOrders(AccountId account, MarketId market,
                 std::chrono::steady_clock::time_point deadline);
  // Scan a bounded 24-hour window for orders missing from local history after
  // a crash. A full 1000-row page is deliberately reported as incomplete.
  boost::asio::awaitable<absl::StatusOr<std::vector<OrderToQuery>>>
  ListRecentOrders(AccountId account, MarketId market, UtcTime start,
                   UtcTime end, std::chrono::steady_clock::time_point deadline);

 private:
  SignedRestClient& rest_;
  Limits limits_;
};

}  // namespace hquant::binance_spot
