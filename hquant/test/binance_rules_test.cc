#include <cstdlib>
#include <iostream>

#include "hquant/venue/binance/rules.h"

int main() {
  constexpr auto sample = R"({"symbols":[{"symbol":"BTCUSDT",
"status":"TRADING","baseAsset":"BTC","quoteAsset":"USDT","filters":[
{"filterType":"PRICE_FILTER","minPrice":"0.01","maxPrice":"1000000",
"tickSize":"0.01"},{"filterType":"LOT_SIZE","minQty":"0.001",
"maxQty":"100","stepSize":"0.001"},{"filterType":"MIN_NOTIONAL",
"minNotional":"5"}]}]})";
  auto result = hquant::v1::ParseBinanceExchangeInfo(sample);
  if (!result.ok() || result->size() != 1 ||
      (*result)[0].price_increment_nanos != 10'000'000ULL ||
      (*result)[0].base_increment_nanos != 1'000'000ULL ||
      (*result)[0].min_order_value_nanos != 5'000'000'000ULL) {
    std::cerr << "Binance exchangeInfo rules were parsed incorrectly\n";
    return EXIT_FAILURE;
  }
  constexpr auto incomplete = R"({"symbols":[{"symbol":"BTCUSDT",
"status":"TRADING","baseAsset":"BTC","quoteAsset":"USDT",
"filters":[]}]})";
  if (hquant::v1::ParseBinanceExchangeInfo(incomplete).ok()) {
    std::cerr << "incomplete Binance rules were accepted\n";
    return EXIT_FAILURE;
  }
  constexpr auto demo = R"({"symbols":[{"symbol":"BTCUSDT",
"status":"TRADING","baseAsset":"BTC","quoteAsset":"USDT","filters":[
{"filterType":"PRICE_FILTER","minPrice":"0.01000000",
"maxPrice":"1000000.00000000","tickSize":"0.01000000"},
{"filterType":"LOT_SIZE","minQty":"0.00001000",
"maxQty":"100.00000000","stepSize":"0.00001000"},
{"filterType":"NOTIONAL","minNotional":"5.00000000",
"maxNotional":"9000000.00000000"}]}]})";
  auto demo_rule = hquant::v1::ParseBinanceExchangeInfo(demo);
  if (!demo_rule.ok() || demo_rule->size() != 1 ||
      (*demo_rule)[0].base_increment_nanos != 10'000ULL ||
      (*demo_rule)[0].min_order_value_nanos != 5'000'000'000ULL) {
    std::cerr << "Demo NOTIONAL and lot rules were parsed incorrectly\n";
    return EXIT_FAILURE;
  }
}
