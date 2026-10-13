#include <cstdlib>
#include <iostream>
#include <string>

#include "hquant/config.h"
#include "hquant/shard/trading_state.h"

namespace {

void Require(bool condition, const char* message) {
  if (!condition) {
    std::cerr << message << '\n';
    std::exit(1);
  }
}

const char* kConfig = R"yaml(
schema_version: 2
input:
  source: replay
  replay_file: test.json
accounts:
  - id: shared
    mode: pressure_sim
    venue: binance_spot
    trading_rest: {host: 127.0.0.1, port: 19001, tls: false}
    private_ws: {host: 127.0.0.1, port: 19002, tls: false}
    initial_balances: {BTC: "1", ETH: "1", USD: "100"}
    hard_limits: {BTC: "1", ETH: "1", USD: "100"}
    maker_fee_rate: "0"
    risk: {max_active_orders: 4, max_open_value_per_quote: "100"}
shards:
  - id: 0
    account_id: shared
    allocation:
      assets: {BTC: "1", USD: "30"}
      max_active_orders: 2
      max_open_value_per_quote: "30"
    market:
      symbol: BTCUSD
      base_asset: BTC
      quote_asset: USD
      price_per_tick: "0.01"
      amount_per_lot: "1"
      trading_rule: {price_increment: "0.01", base_increment: "1", min_base_amount: "1", min_order_value: "1"}
    strategy: {order_amount: "1", bid_spread: "0", ask_spread: "0", refresh_ms: 1000}
  - id: 1
    account_id: shared
    allocation:
      assets: {ETH: "1", USD: "70"}
      max_active_orders: 2
      max_open_value_per_quote: "70"
    market:
      symbol: ETHUSD
      base_asset: ETH
      quote_asset: USD
      price_per_tick: "0.01"
      amount_per_lot: "1"
      trading_rule: {price_increment: "0.01", base_increment: "1", min_base_amount: "1", min_order_value: "1"}
    strategy: {order_amount: "1", bid_spread: "0", ask_spread: "0", refresh_ms: 1000}
)yaml";

std::string Replace(std::string text, const std::string& before,
                    const std::string& after) {
  const auto position = text.find(before);
  Require(position != std::string::npos, "allocation fixture missing field");
  text.replace(position, before.size(), after);
  return text;
}

}  // namespace

int main() {
  using namespace hquant::v1;
  auto config = ParseConfig(kConfig, "/tmp/hquant-shard-allocation-test");
  Require(config.ok(), "configured shard allocations rejected");
  auto resolved = ResolveShardAllocations(*config);
  Require(resolved.ok(), "cannot resolve shard allocations");
  constexpr uint64_t unit = 1'000'000'000;
  Require(resolved->at(0).assets_nanos.at("USD") == 30 * unit &&
              resolved->at(1).assets_nanos.at("USD") == 70 * unit &&
              resolved->at(0).max_active_orders == 2 &&
              resolved->at(1).max_open_value_per_quote_nanos == 70 * unit,
          "configured allocations changed during resolution");
  ShardTradingState first(config->shards[0], resolved->at(0),
                          config->accounts[0].risk, 0);
  ShardTradingState second(config->shards[1], resolved->at(1),
                           config->accounts[0].risk, 0);
  auto a = first.BeginSubmit(ClientOrderId{"first"},
                             LimitOrderRequest{Side::Buy, 10 * unit, 3 * unit},
                             1);
  auto b = second.BeginSubmit(ClientOrderId{"second"},
                              LimitOrderRequest{Side::Buy, 10 * unit, 7 * unit},
                              2);
  Require(a.ok() && b.ok() && first.Available("USD") == 0 &&
              second.Available("USD") == 0,
          "shards did not reserve their independent allocations");
  Require(!first.BeginSubmit(ClientOrderId{"extra"},
                             LimitOrderRequest{Side::Buy, 10 * unit, unit}, 3)
               .ok(),
          "shard overspent its allocation");
  Require(a->order.local_order_id.value != b->order.local_order_id.value,
          "different shards assigned the same local order ID");
  Require(first.ApplyResponse(a->order.client_order_id, false,
                              TradeResponse{TradeResponse::Result::Confirmed,
                                            ExchangeOrderId{10}})
              .ok(),
          "accepted order was not applied to shard state");
  Fill partial;
  partial.order_id = a->order.local_order_id;
  partial.side = Side::Buy;
  partial.trade_id = 11;
  partial.price_nanos = 10 * unit;
  partial.quantity_nanos = unit;
  partial.gross_nanos = 10 * unit;
  partial.market = a->order.market;
  partial.client_order_id = a->order.client_order_id;
  partial.fee_asset = "USD";
  Require(first.ApplyEvent(AccountEvent{partial}).ok() &&
              first.Total("USD") == 20 * unit &&
              first.Total("BTC") == 2 * unit &&
              first.Available("USD") == 0,
          "partial fill did not debit funds and retain remaining hold");
  Require(first.ApplyEvent(AccountEvent{partial}).ok() &&
              first.Total("USD") == 20 * unit &&
              first.Total("BTC") == 2 * unit,
          "duplicate trade ID debited shard funds twice");
  Require(first.BeginCancel(a->order.local_order_id, 4).ok(),
          "partially filled order could not be canceled");
  TradeResponse canceled{TradeResponse::Result::Confirmed,
                         ExchangeOrderId{10}};
  canceled.remaining_nanos = 2 * unit;
  Require(first.ApplyResponse(a->order.client_order_id, true, canceled).ok() &&
              first.Available("USD") == 20 * unit,
          "cancel did not release only the unfilled reservation");
  auto rejected = first.BeginSubmit(
      ClientOrderId{"rejected"},
      LimitOrderRequest{Side::Buy, 10 * unit, unit}, 5);
  Require(rejected.ok() && first.Available("USD") == 10 * unit,
          "new order did not reserve remaining shard funds");
  Require(first.ApplyResponse(rejected->order.client_order_id, false,
                              TradeResponse{TradeResponse::Result::Rejected,
                                            {}, "venue rejected"})
              .ok() &&
              first.Available("USD") == 20 * unit,
          "rejected order did not release its reservation");
  Require(second.AbortSubmit(b->order.client_order_id).ok() &&
              second.Available("USD") == 70 * unit &&
              first.Available("USD") == 20 * unit,
          "another shard changed this shard's allocation");

  const std::string oversized =
      Replace(kConfig, "assets: {ETH: \"1\", USD: \"70\"}",
              "assets: {ETH: \"1\", USD: \"80\"}");
  Require(!ParseConfig(oversized, "/tmp/hquant-shard-allocation-test").ok(),
          "oversubscribed account asset allocation accepted");
  const std::string too_many_orders =
      Replace(kConfig, "max_active_orders: 2\n      max_open_value_per_quote: \"70\"",
              "max_active_orders: 3\n      max_open_value_per_quote: \"70\"");
  Require(!ParseConfig(too_many_orders,
                       "/tmp/hquant-shard-allocation-test")
               .ok(),
          "oversubscribed account order slots accepted");
}
