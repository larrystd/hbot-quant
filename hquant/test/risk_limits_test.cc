#include <cstdlib>
#include <iostream>
#include <map>

#include "hquant/trading/risk_limits.h"

namespace {

using namespace hquant::v1;

void Require(bool condition, const char* message) {
  if (condition) return;
  std::cerr << message << '\n';
  std::exit(1);
}

MarketConfig Market(const char* symbol, const char* base) {
  MarketConfig market;
  market.id = MarketId{symbol};
  market.base_asset = base;
  market.quote_asset = "USDT";
  market.rule = {1'000'000'000ULL, 1'000'000'000ULL,
                 1'000'000'000ULL, 10'000'000'000ULL};
  return market;
}

}  // namespace

int main() {
  MarketConfig btc = Market("BTCUSDT", "BTC");
  MarketConfig eth = Market("ETHUSDT", "ETH");
  std::map<uint16_t, MarketConfig> markets{{1, btc}, {2, eth}};
  OrderState orders;
  TrackedOrder existing;
  existing.client_order_id = ClientOrderId{"btc-1"};
  existing.market = btc.id;
  existing.shard = ShardId{1};
  existing.price_nanos = 60'000'000'000ULL;
  existing.quantity_nanos = 1'000'000'000ULL;
  Require(orders.Begin(existing).ok(), "failed to create existing order");

  AccountConfig::RiskPolicy policy;
  policy.max_order_value_nanos = 70'000'000'000ULL;
  policy.max_open_value_per_quote_nanos = 100'000'000'000ULL;
  policy.max_active_orders = 2;
  policy.max_symbol_active_orders = 1;
  Require(!RiskLimits::Check(policy, btc,
                             {Side::Buy, 20'000'000'000ULL,
                              1'000'000'000ULL}, markets, orders).ok(),
          "per-symbol active order limit was bypassed");
  Require(!RiskLimits::Check(policy, eth,
                             {Side::Buy, 50'000'000'000ULL,
                              1'000'000'000ULL}, markets, orders).ok(),
          "shared quote exposure limit was bypassed");
  Require(RiskLimits::Check(policy, eth,
                            {Side::Buy, 40'000'000'000ULL,
                             1'000'000'000ULL}, markets, orders).ok(),
          "valid cross-symbol order was rejected");
  Require(!RiskLimits::Check(policy, eth,
                             {Side::Buy, 5'000'000'000ULL,
                              1'000'000'000ULL}, markets, orders).ok(),
          "venue minimum notional was bypassed");
}
