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

// Binance 现货实盘的账户信息来源，分两部分：
//   1. 账户推送解析（AccountPushParser）：把账户 WebSocket 推来的 JSON
//      （订单状态、成交、余额变化）解析成 OrderUpdate / Balance；
//   2. 主动查询（OrderQueryClient）：对结果不确定的订单，向交易所 REST 接口查询
//      订单状态和成交；重启时决定要查哪些订单（PlanStartupQueries）。
//
// 现状：运行时没有任何地方使用，只有 account_reports_test、recovery_test 使用。
// 实盘目前被有意关闭；将来由 LiveTrading（shard/live_trading.h）使用，解析和
// 查询得到的回报交给 Shard 的回报入口。模拟盘的回报来自模拟交易所，不经过这里。

namespace hquant::binance_spot {

// 解析一条账户推送的结果。
//   events：订单回报（成交时带这一笔成交）和余额变化；
//   stream_terminated：交易所通知推送流结束，需要重新订阅；
//   account_resync_required：推送可能漏了消息，需要重新查询账户状态。
struct AccountPushBatch {
  // Each executionReport contributes one order update, including its trade
  // when the report describes a fill.
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

// ---- 主动查询 ----

// 要查询的一张订单。
struct OrderToQuery {
  AccountId account;
  MarketId market;
  ClientOrderId client_order_id;
};

// 重启时决定要查哪些订单的输入：本次负责的交易对、订单历史里的订单、
// 还活跃的订单，以及上次是否正常退出、历史是否完整。
struct StartupQueryInput {
  AccountId account;
  std::vector<MarketId> assigned_markets;
  std::vector<Order> persisted_orders;
  std::vector<Order> live_orders;
  bool history_complete = true;
  bool previous_run_clean = true;
  bool executor_checkpoints_complete = true;
};

// 重启时的查询计划：要逐个查询的订单，以及需要整体扫描的交易对。
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

// 查询一张订单的结果。
struct OrderQueryResult {
  OrderToQuery target;
  // Present only when complete. Incomplete queries can still return verified
  // partial trades, but cannot confirm an uncertain submission.
  std::vector<OrderUpdate> updates;
  bool complete = false;
  // Incomplete/404/429/transport failures preserve SubmissionUnknown and its
  // funds hold. A caller may apply verified partial trades while unresolved.
  absl::Status unresolved_status;
};

// 查询客户端：QueryOrder 查一张订单的状态和全部成交；ListOpenOrders、
// ListRecentOrders 在重启后找出本地历史里没有记录的订单。
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
