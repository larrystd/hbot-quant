#pragma once

#include <chrono>
#include <cstdint>
#include <optional>

#include "base/market.h"
#include "strategy/strategy.h"

namespace hquant {

enum class PmmPriceType { Mid, Last };

struct SimplePmmConfig {
  OwnerId owner;
  AccountId account;
  MarketSpec market;
  Decimal order_amount;
  Decimal bid_spread;
  Decimal ask_spread;
  std::chrono::microseconds refresh_interval{std::chrono::seconds(15)};
  PmmPriceType price_type = PmmPriceType::Mid;
  Decimal maker_fee_rate;
  bool buy_fee_from_returns = true;
};

class SimplePmm final : public Strategy {
 public:
  explicit SimplePmm(SimplePmmConfig config) : config_(std::move(config)) {}

  TriggerPolicy Triggers() const override;
  ActionBatch OnTimer(const StrategyContext& context) override;
  std::optional<int64_t> NextRefreshAtUs() const { return next_refresh_at_us_; }

 private:
  std::optional<Decimal> ReferencePrice(const StrategyContext& context) const;
  Decimal Available(const StrategyContext& context, const AssetId& asset) const;
  bool Active(const OrderSnapshot& order) const;

  SimplePmmConfig config_;
  bool ready_to_trade_ = false;
  std::optional<int64_t> next_refresh_at_us_;
};

}  // namespace hquant
