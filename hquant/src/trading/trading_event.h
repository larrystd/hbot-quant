#pragma once

#include <cstdint>
#include <string>
#include <variant>

#include "hquant/base/ids.h"

namespace hquant::v1 {

enum class Side { Buy, Sell };

struct LimitOrderRequest {
  Side side = Side::Buy;
  uint64_t price_nanos = 0;
  uint64_t quantity_nanos = 0;
};

struct Order {
  OrderId id;
  Side side = Side::Buy;
  uint64_t price_nanos = 0;
  uint64_t quantity_nanos = 0;
  uint64_t remaining_nanos = 0;
  uint64_t reserved_nanos = 0;
  MarketId market;
  ClientOrderId client_order_id;
  ExchangeOrderId exchange_order_id;
};

enum class OrderStatus {
  PendingSubmit,
  Rejected,
  Accepted,
  PartiallyFilled,
  Filled,
  Cancelled,
};

struct OrderReport {
  Order order;
  OrderStatus state = OrderStatus::Accepted;
  std::string reason;
  uint64_t event_time_ms = 0;
  bool authoritative_query = false;
};

struct Fill {
  OrderId order_id;
  Side side = Side::Buy;
  uint64_t trade_id = 0;
  uint64_t price_nanos = 0;
  uint64_t quantity_nanos = 0;
  uint64_t gross_nanos = 0;
  uint64_t fee_nanos = 0;
  MarketId market;
  ClientOrderId client_order_id;
  std::string fee_asset;
  uint64_t transaction_ms = 0;
};

struct BalanceUpdate {
  std::string asset;
  uint64_t total_nanos = 0;
  uint64_t available_nanos = 0;
  uint64_t locked_nanos = 0;
  uint64_t version_ms = 0;
};

struct BalanceDelta {
  std::string asset;
  uint64_t amount_nanos = 0;
  bool credit = true;
  bool external_lock = false;
  uint64_t transaction_time_ms = 0;
};

using AccountEvent =
    std::variant<OrderReport, Fill, BalanceUpdate, BalanceDelta>;

}  // namespace hquant::v1
