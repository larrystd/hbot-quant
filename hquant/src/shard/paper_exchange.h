#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <string>
#include <variant>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "hquant/base/ids.h"
#include "hquant/config.h"
#include "hquant/shard/market_type.h"
#include "hquant/shard/strategy.h"

namespace hquant::v1 {

struct Order {
  OrderId id;
  Side side = Side::Buy;
  uint64_t price_nanos = 0;
  uint64_t quantity_nanos = 0;
  uint64_t remaining_nanos = 0;
  uint64_t reserved_nanos = 0;
};

enum class OrderState { Rejected, Accepted, PartiallyFilled, Filled, Cancelled };
struct OrderReport {
  Order order;
  OrderState state = OrderState::Accepted;
  std::string reason;
};
struct Fill {
  OrderId order_id;
  Side side = Side::Buy;
  uint64_t trade_id = 0;
  uint64_t price_nanos = 0;
  uint64_t quantity_nanos = 0;
  uint64_t gross_nanos = 0;
  uint64_t fee_nanos = 0;
};
struct BalanceUpdate {
  std::string asset;
  uint64_t total_nanos = 0;
  uint64_t available_nanos = 0;
};
using AccountEvent = std::variant<OrderReport, Fill, BalanceUpdate>;

class PaperExchange {
 public:
  using EventSink = std::function<void(AccountEvent)>;
  PaperExchange(const MarketConfig& market, const ExecutorConfig& config,
                EventSink event_sink);

  // Commands return only their local dispatch result. Order/account updates
  // are delivered through EventSink, the same event path a live adapter feeds.
  absl::StatusOr<OrderId> Place(const LimitOrderRequest& request);
  absl::Status Cancel(const std::vector<OrderId>& ids);
  absl::Status CancelAll();
  // Matches active orders against the market change described by notice.
  absl::Status OnMarket(const MarketNotice& notice, const BookView& book);
  absl::Status OnBookBbo(const BookView& book);
  absl::Status OnPublicTrade(const PublicTrade& trade);

  const std::map<uint64_t, Order>& ActiveOrders() const { return active_orders_; }
  uint64_t Total(const std::string& asset) const;
  uint64_t Available(const std::string& asset) const;

 private:
  struct Match {
    OrderId id;
    uint64_t quantity_nanos = 0;
    uint64_t price_nanos = 0;
  };
  absl::Status FillMatches(const std::vector<Match>& matches);
  absl::StatusOr<uint64_t> BuyReserve(uint64_t price_nanos,
                                      uint64_t quantity_nanos) const;
  absl::StatusOr<uint64_t> LotsToQuantity(uint64_t lots) const;
  BalanceUpdate BalanceOf(const std::string& asset) const;
  std::string CollateralAsset(Side side) const;

  MarketConfig market_;
  ExecutorConfig config_;
  std::map<std::string, uint64_t> totals_;
  std::map<uint64_t, Order> active_orders_;
  EventSink event_sink_;
  uint64_t next_order_id_ = 1;
  uint64_t next_trade_id_ = 1;
};

}  // namespace hquant::v1
