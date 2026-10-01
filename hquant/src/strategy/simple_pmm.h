#pragma once

#include <chrono>
#include <cstdint>
#include <optional>

#include "base/market.h"
#include "strategy/strategy.h"

namespace hquant {

enum class PmmPriceType { Mid, Last };

struct SimplePmmConfig {
  StrategyId strategy_id;
  AccountId account;
  MarketSpec market;
  Decimal order_amount;
  Decimal bid_spread;
  Decimal ask_spread;
  std::chrono::microseconds refresh_interval{std::chrono::seconds(15)};
  PmmPriceType price_type = PmmPriceType::Mid;
  Decimal maker_fee_rate;
  bool buy_fee_from_returns = true;
  std::chrono::microseconds timer_period{std::chrono::seconds(1)};
};

class SimplePmm final : public Strategy {
 public:
  explicit SimplePmm(SimplePmmConfig config) : config_(std::move(config)) {}

  TriggerPolicy Triggers() const override;
  ActionBatch Decide(const StrategyInput& context, Trigger why) override;
  std::optional<int64_t> NextRefreshAtUs() const { return next_refresh_at_us_; }

 private:
  std::optional<Decimal> ReferencePrice(const StrategyInput& context) const;
  Decimal Available(const StrategyInput& context, const AssetId& asset) const;
  bool Active(const OrderSnapshot& order) const;

  SimplePmmConfig config_;
  bool ready_to_trade_ = false;
  std::optional<int64_t> next_refresh_at_us_;
};

}  // namespace hquant
