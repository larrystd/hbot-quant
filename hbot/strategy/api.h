#pragma once

#include <chrono>
#include <cstdint>
#include <optional>
#include <span>
#include <variant>
#include <vector>

#include "hbot/base/clock.h"
#include "hbot/market_data/book_view.h"
#include "hbot/model/balance.h"
#include "hbot/model/order.h"
#include "hbot/model/trading_rule.h"

namespace hbot {

struct SubmitOrder {
  OwnerId owner;
  OrderRequest request;
};
struct CancelOrder {
  OwnerId owner;
  ClientOrderId client_id;
};
using StrategyAction = std::variant<SubmitOrder, CancelOrder>;
struct ActionBatch {
  std::vector<StrategyAction> ordered;
};

enum class BookTriggerMode { None, BboChanged, EveryAppliedBatch };
struct TriggerPolicy {
  BookTriggerMode book_mode = BookTriggerMode::None;
  bool on_public_trade = false;
  bool on_order_update = false;
  bool on_fill = false;
  std::optional<std::chrono::microseconds> timer_period;
  std::optional<std::chrono::microseconds> coalesce_window;
  std::chrono::microseconds min_action_interval{0};
};

struct StrategyContext {
  const BookView& book;
  const BookScale& book_scale;
  const TradingRule& trading_rule;
  std::optional<Decimal> last_trade_price;
  bool ready = false;
  std::span<const OrderSnapshot> orders;
  std::span<const Balance> balances;
  InputStamp input;
  const Clock& clock;
};

class Strategy {
 public:
  virtual ~Strategy() = default;
  virtual TriggerPolicy Triggers() const = 0;
  virtual ActionBatch OnTimer(const StrategyContext& context) = 0;
};

}  // namespace hbot
