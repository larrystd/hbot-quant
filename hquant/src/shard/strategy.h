#pragma once

#include <optional>
#include <variant>
#include <vector>

#include "absl/status/statusor.h"
#include "hquant/base/ids.h"
#include "hquant/config.h"
#include "hquant/shard/market_type.h"

namespace hquant::v1 {

struct ExecutionView {
  std::vector<OrderId> active_order_ids;
  uint64_t base_available_nanos = 0;
  uint64_t quote_available_nanos = 0;
  uint64_t base_budget_nanos = 0;
  uint64_t quote_budget_nanos = 0;
  uint64_t maker_fee_rate_ppb = 0;
  uint64_t price_per_tick_nanos = 0;
  uint64_t price_increment_nanos = 0;
  uint64_t base_increment_nanos = 0;
  uint64_t min_base_amount_nanos = 0;
  uint64_t min_order_value_nanos = 0;
};

struct NoAction {};
struct CancelOrders { std::vector<OrderId> ids; };
struct LimitOrderRequest {
  Side side = Side::Buy;
  uint64_t price_nanos = 0;
  uint64_t quantity_nanos = 0;
};
struct SubmitOrders { std::vector<LimitOrderRequest> orders; };
using StrategyDecision = std::variant<NoAction, CancelOrders, SubmitOrders>;

class Strategy {
 public:
  explicit Strategy(StrategyConfig config) : config_(config) {}
  absl::StatusOr<StrategyDecision> Decide(const BookView& book,
                                            const ExecutionView& execution,
                                            MonoTime now);
  void UpdateConfig(StrategyConfig config) {
    config_ = config;
    next_refresh_at_.reset();
  }
  const StrategyConfig& Config() const { return config_; }

 private:
  StrategyConfig config_;
  std::optional<MonoTime> next_refresh_at_;
};

}  // namespace hquant::v1
