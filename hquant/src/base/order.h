#pragma once

#include <optional>
#include <variant>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "base/market.h"
#include "base/types.h"

namespace hquant {

enum class TimeInForce { Gtc, Ioc, Fok, PostOnly };
enum class ExchangeOrderStatus {
  Open,
  PartiallyTraded,
  Traded,
  Canceled,
  Rejected,
  Expired
};

struct SubmitOrder {
  Side side = Side::Buy;
  Decimal quantity;
  Decimal price;
  TimeInForce time_in_force = TimeInForce::Gtc;
};

struct CancelOrder {
  ClientOrderId client_order_id;
};

// These fields describe the order as it was accepted for submission and never
// change. Exchange execution state belongs to the tracker.
struct Order {
  ClientOrderId client_order_id;
  StrategyId strategy_id;
  AccountId account;
  MarketId market;
  Side side = Side::Buy;
  Decimal quantity;
  Decimal price;
  TimeInForce time_in_force = TimeInForce::Gtc;
};

struct TradeFee {
  AssetId asset;
  Decimal signed_amount;
};

struct Trade {
  ExchangeTradeId exchange_trade_id;
  Decimal price;
  Decimal quantity;
  Decimal value;
  std::vector<TradeFee> fees;
  std::optional<bool> maker;
};

struct OrderUpdate {
  AccountId account;
  MarketId market;
  ClientOrderId client_order_id;
  ExchangeOrderStatus status = ExchangeOrderStatus::Open;
  std::optional<Decimal> executed_quantity;
  std::optional<Trade> trade;
  EventTime time;
};

struct Balance {
  AccountId account;
  AssetId asset;
  Decimal total;
  Decimal available;
  EventTime time;
};
using BalanceUpdate = Balance;
using AccountEvent = std::variant<OrderUpdate, BalanceUpdate>;

enum class SendResult { NotSent, Unknown };
enum class ReportSource { Push, Query };
enum class UpdateResult { Ignored, Updated, Finished };

class OrderGateway {
 public:
  virtual ~OrderGateway() = default;
  virtual absl::StatusOr<Order> PrepareSubmit(const SubmitOrder& request,
                                              const StrategyId& strategy_id,
                                              MonoTime expires_at_mono) = 0;
  // A failure here proves that no bytes were sent. Later unknown outcomes are
  // delivered as gateway events.
  virtual absl::Status StartPrepared(const ClientOrderId& client_order_id) = 0;
  virtual absl::Status AbortPrepared(const ClientOrderId& client_order_id) = 0;
  virtual absl::Status StartCancel(const ClientOrderId& client_order_id) = 0;
};

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
