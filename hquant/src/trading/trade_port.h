#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "hquant/trading/trading_event.h"

namespace hquant::v1 {

struct SubmitCommand {
  MarketId market;
  ClientOrderId client_order_id;
  LimitOrderRequest order;
};

struct CancelCommand {
  MarketId market;
  ClientOrderId client_order_id;
  ExchangeOrderId exchange_order_id;
};

struct QueryCommand {
  MarketId market;
  ClientOrderId client_order_id;
  ExchangeOrderId exchange_order_id;
};

struct TradeResponse {
  enum class Result { Confirmed, Rejected, Unknown, NotSent } result;
  ExchangeOrderId exchange_order_id;
  std::string reason;
  std::optional<uint64_t> remaining_nanos;
};

struct VenueRule {
  MarketId market;
  std::string base_asset;
  std::string quote_asset;
  bool trading = false;
  uint64_t price_increment_nanos = 0;
  uint64_t min_price_nanos = 0;
  uint64_t max_price_nanos = 0;
  uint64_t base_increment_nanos = 0;
  uint64_t min_base_amount_nanos = 0;
  uint64_t max_base_amount_nanos = 0;
  uint64_t min_order_value_nanos = 0;
  uint64_t max_order_value_nanos = 0;
};

struct TradeQueueHealth {
  size_t queued = 0;
  size_t peak = 0;
  uint64_t rejected = 0;
};

// A true return means the bounded local send queue accepted the command.
// Completion may arrive later. A false return must not invoke completion.
// Venue events are delivered independently and may arrive first.
class TradePort {
 public:
  using Completion = std::function<void(TradeResponse)>;
  using WriteStarted = std::function<void()>;
  using QueryCompletion =
      std::function<void(absl::StatusOr<OrderReport>)>;
  using TradesCompletion =
      std::function<void(absl::StatusOr<std::vector<Fill>>)>;
  using BalancesCompletion =
      std::function<void(absl::StatusOr<std::vector<BalanceUpdate>>)>;
  using OpenOrdersCompletion =
      std::function<void(absl::StatusOr<std::vector<OrderReport>>)>;
  using RulesCompletion =
      std::function<void(absl::StatusOr<std::vector<VenueRule>>)>;
  virtual ~TradePort() = default;
  virtual bool Submit(SubmitCommand command, Completion done,
                      WriteStarted write_started = {}) = 0;
  virtual bool Cancel(CancelCommand command, Completion done,
                      WriteStarted write_started = {}) = 0;
  virtual bool Query(QueryCommand, QueryCompletion) { return false; }
  virtual bool QueryTrades(QueryCommand, TradesCompletion) { return false; }
  virtual bool QueryBalances(BalancesCompletion) { return false; }
  virtual bool QueryOpenOrders(OpenOrdersCompletion) { return false; }
  virtual bool QueryRules(RulesCompletion) { return false; }
  virtual TradeQueueHealth QueueHealth() const { return {}; }
  virtual void Stop() = 0;
};

}  // namespace hquant::v1
