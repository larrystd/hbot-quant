#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "hbot/base/clock.h"
#include "hbot/base/decimal.h"
#include "hbot/base/ids.h"
#include "hbot/model/book_event.h"
#include "hbot/model/fee.h"

namespace hbot {

enum class OrderType { Limit, LimitMaker };
enum class TimeInForce { Gtc, Ioc, Fok };
enum class ExchangeOrderStatus {
  New,
  PartiallyFilled,
  Filled,
  Canceled,
  Rejected,
  Expired
};
enum class OrderDisplayState {
  PendingCreate,
  Open,
  PartiallyFilled,
  PendingCancel,
  SubmissionUnknown,
  AwaitingFills,
  Filled,
  Canceled,
  Failed,
  Expired
};

struct OrderRequest {
  AccountId account;
  MarketId market;
  Side side = Side::Buy;
  OrderType type = OrderType::Limit;
  Decimal base_amount;
  std::optional<Decimal> limit_price;
  std::optional<TimeInForce> time_in_force;
};

struct OrderCommand {
  OwnerId owner;
  OrderRequest request;
  ReservationId reservation_id;
  DecisionId decision_id;
  MonoTime expires_at_mono{};
};

struct ExecutorCheckpoint {
  uint32_t schema_version = 0;
  OwnerId owner;
  uint64_t config_revision = 0;
  std::string payload;
};

struct OrderIntent {
  ClientOrderId client_id;
  OwnerId owner;
  OrderRequest request;
  uint64_t config_revision = 0;
  UtcTime created_at_utc{};
  std::optional<ExecutorCheckpoint> executor_checkpoint;
};

struct OrderUpdate {
  AccountId account;
  MarketId market;
  std::optional<ClientOrderId> client_id;
  std::optional<ExchangeOrderId> exchange_order_id;
  ExchangeOrderStatus exchange_status = ExchangeOrderStatus::New;
  std::optional<Decimal> cumulative_base;
  std::optional<Decimal> cumulative_quote;
  EventTime time;
};

struct TradeUpdate {
  AccountId account;
  MarketId market;
  std::optional<ClientOrderId> client_id;
  std::optional<ExchangeOrderId> exchange_order_id;
  ExchangeTradeId exchange_trade_id;
  Decimal price;
  Decimal base_amount;
  Decimal quote_amount;
  std::vector<TradeFee> fees;
  std::optional<bool> maker;
  EventTime time;
};

// Neutral read-only value shared by Tracker, strategy, risk, and control.
// The mutable TrackedOrder remains private to connector/order_tracker.h.
struct OrderSnapshot {
  ClientOrderId client_id;
  std::optional<ExchangeOrderId> exchange_id;
  OwnerId owner;
  OrderRequest request;
  OrderDisplayState display_state = OrderDisplayState::PendingCreate;
  Decimal cumulative_base;
  Decimal cumulative_quote;
  std::vector<TradeFee> fees;
  EventTime last_update_time;
};

}  // namespace hbot
