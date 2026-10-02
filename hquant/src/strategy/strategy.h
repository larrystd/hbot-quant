#pragma once

#include <chrono>
#include <cstdint>
#include <optional>
#include <span>
#include <variant>
#include <vector>

#include "base/market.h"
#include "base/order.h"
#include "base/types.h"
#include "market/order_book.h"

namespace hquant {

using StrategyAction = std::variant<SubmitOrder, CancelOrder>;
struct ActionBatch {
  std::vector<StrategyAction> ordered;
};

enum class BookTriggerMode { None, BboChanged, EveryAppliedBatch };
enum class Trigger { BookChanged, PublicTraded, OrderUpdated, Traded, Timer };
struct TriggerPolicy {
  BookTriggerMode book_mode = BookTriggerMode::None;
  bool on_public_trade = false;
  bool on_order_update = false;
  bool on_fill = false;
  std::optional<std::chrono::microseconds> timer_period;
  std::optional<std::chrono::microseconds> coalesce_window;
  std::chrono::microseconds min_action_interval{0};
};

struct StrategyInput {
  const OrderBookView& book;
  const TickLotSize& tick_lot_size;
  const TradingRule& trading_rule;
  std::optional<Decimal> last_trade_price;
  bool ready = false;
  std::span<const Order* const> orders;
  std::span<const Balance> balances;
  InputTime input;
  const Clock& clock;
};

class Strategy {
 public:
  virtual ~Strategy() = default;
  virtual TriggerPolicy Triggers() const = 0;
  virtual ActionBatch Decide(const StrategyInput& context, Trigger why) = 0;
};

}  // namespace hquant
