#pragma once

#include <variant>

#include "hbot/model/balance.h"
#include "hbot/model/book_event.h"
#include "hbot/model/order.h"

namespace hbot {

using MarketEvent = std::variant<BookSnapshot, BookDiff, PublicTrade>;
using AccountEvent = std::variant<OrderUpdate, TradeUpdate, BalanceUpdate>;

struct OrderCreated {
  OrderSnapshot order;
};
struct OrderFilled {
  OrderSnapshot order;
  TradeUpdate trade;
};
struct OrderCompleted {
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
    std::variant<OrderCreated, OrderFilled, OrderCompleted, OrderCanceled,
                 OrderFailed>;

}  // namespace hbot
