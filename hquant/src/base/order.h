#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <variant>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "base/market.h"
#include "base/types.h"

namespace hquant {

struct TradeFee {
  AssetId asset;
  // Positive means fee paid; negative means rebate received.
  Decimal signed_amount;
};

struct Balance {
  AccountId account;
  AssetId asset;
  Decimal total;
  Decimal available;
  EventTime time;
};

using BalanceUpdate = Balance;

enum class OrderType { Limit, LimitMaker };
enum class TimeInForce { Gtc, Ioc, Fok };
enum class ExchangeOrderStatus {
  Open,
  PartiallyTraded,
  Traded,
  Canceled,
  Rejected,
  Expired
};
enum class OrderDisplayState {
  PendingCreate,
  Open,
  PartiallyTraded,
  PendingCancel,
  SubmissionUnknown,
  AwaitingTrades,
  Traded,
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

struct ApprovedOrder {
  StrategyId strategy_id;
  OrderRequest request;
  HoldId hold_id;
  ActionBatchId action_batch_id;
  MonoTime expires_at_mono{};
};

struct ExecutorCheckpoint {
  uint32_t schema_version = 0;
  StrategyId strategy_id;
  uint64_t config_revision = 0;
  std::string payload;
};

struct PreparedOrder {
  ClientOrderId client_id;
  StrategyId strategy_id;
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
  ExchangeOrderStatus exchange_status = ExchangeOrderStatus::Open;
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
  StrategyId strategy_id;
  OrderRequest request;
  OrderDisplayState display_state = OrderDisplayState::PendingCreate;
  Decimal cumulative_base;
  Decimal cumulative_quote;
  std::vector<TradeFee> fees;
  EventTime last_update_time;
};

using MarketEvent = std::variant<BookSnapshot, BookDiff, PublicTrade>;
using AccountEvent = std::variant<OrderUpdate, TradeUpdate, BalanceUpdate>;

struct OrderOpened {
  OrderSnapshot order;
};
struct OrderTraded {
  OrderSnapshot order;
  TradeUpdate trade;
};
struct OrderFullyTraded {
  OrderSnapshot order;
};
struct OrderCanceled {
  OrderSnapshot order;
};
struct OrderFailed {
  OrderSnapshot order;
  std::string reason;
};
using TrackedOrderEvent =
    std::variant<OrderOpened, OrderTraded, OrderFullyTraded, OrderCanceled,
                 OrderFailed>;

// Methods are called only by the owning shard. Preparing reserves a bounded
// send slot, fixes the client ID, and registers PendingCreate, but sends no
// bytes. The caller attaches the FundsHold and attempts to record the
// PreparedOrder before StartPrepared. A failed StartPrepared proves no write
// was initiated; uncertain write outcomes arrive later as account events.
class OrderGateway {
 public:
  virtual ~OrderGateway() = default;
  virtual absl::StatusOr<PreparedOrder> PrepareSubmit(
      ApprovedOrder approved) = 0;
  virtual absl::Status StartPrepared(const ClientOrderId& client_id) = 0;
  virtual absl::Status AbortPrepared(const ClientOrderId& client_id) = 0;
  virtual absl::Status StartCancel(const StrategyId& strategy_id,
                                   const ClientOrderId& client_id) = 0;
};

// Local Simulated exchange port used by replay and public-market Simulated
// shards. The runtime depends on this interface; app selects the concrete
// SimpleSimulatedExchange.
class SimulatedExchange : public OrderGateway {
 public:
  virtual absl::Status OnBookBbo(const Decimal& bid, const Decimal& ask) = 0;
  virtual absl::Status OnPublicTrade(Side aggressor, const Decimal& price,
                                     const Decimal& public_amount) = 0;
  virtual Decimal BalanceOf(const AssetId& asset) const = 0;
  virtual Decimal AvailableBalance(const AssetId& asset) const = 0;
  virtual std::vector<AccountEvent> DrainEvents() = 0;
};

}  // namespace hquant
