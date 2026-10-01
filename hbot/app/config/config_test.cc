#include "hbot/app/config/config.h"

#include <cstdlib>
#include <iostream>
#include <string>

namespace {

constexpr const char* kValid = R"yaml(
schema_version: 1
mode: paper
loop_mode: blocking
storage_path: /tmp/hbot-paper/history.sqlite
replay_fixture: examples/paper_market.json
accounts:
  - account: paper
    initial_balances:
      BTC: "1"
      USDT: "100"
market_specs:
  - market: BTC-USDT
    venue: binance
    base_asset: BTC
    quote_asset: USDT
    book_scale:
      quote_per_tick: "0.01"
      base_per_lot: "0.001"
      scale_version: 1
    trading_rule:
      price_increment: "0.01"
      base_increment: "0.001"
      min_base_amount: "0.001"
      min_notional: "0.01"
  - market: ETH-USDT
    venue: binance
    base_asset: ETH
    quote_asset: USDT
    book_scale:
      quote_per_tick: "0.01"
      base_per_lot: "0.001"
      scale_version: 1
    trading_rule:
      price_increment: "0.01"
      base_increment: "0.001"
      min_base_amount: "0.001"
      min_notional: "0.01"
strategy_configs:
  - owner_key: 1
    strategy: simple_pmm
    account: paper
    markets: [BTC-USDT, ETH-USDT]
    order_amount: "0.01"
    bid_spread: "0.001"
    ask_spread: "0.001"
    refresh_interval: 15s
assignments:
  - shard: 0
    markets: [BTC-USDT, ETH-USDT]
    owners: [1]
    accounts: [paper]
static_risk_leases:
  - account: paper
    asset: USDT
    shard: 0
    hard_limit: "50"
static_rate_leases:
  - account: paper
    shard: 0
    ip: local
    endpoint: order
    limit: 100
    cancel_reserve: 20
    window_us: 1000000
)yaml";

void Require(bool condition, const char* label) {
  if (!condition) {
    std::cerr << label << '\n';
    std::exit(1);
  }
}

std::string Replace(std::string text, const std::string& before,
                    const std::string& after) {
  const auto at = text.find(before);
  Require(at != std::string::npos, "test replacement missing");
  text.replace(at, before.size(), after);
  return text;
}

}  // namespace

int main() {
  auto valid = hbot::ParseConfig(kValid);
  Require(valid.ok(), "valid G1 config");
  Require(valid->mode == hbot::EngineMode::Paper, "paper mode");
  Require(valid->assignments.size() == 1 &&
              valid->assignments[0].markets.size() == 2 &&
              valid->strategy_configs[0].refresh_interval.count() == 15'000'000,
          "parsed assignment and timer");
  Require(valid->static_risk_leases.size() == 1 &&
              valid->static_rate_leases.size() == 1,
          "parsed leases");
  Require(valid->replay_fixture == "examples/paper_market.json",
          "replay source");
  auto public_market = hbot::ParseConfig(
      Replace(kValid, "replay_fixture: examples/paper_market.json",
              "market_data_source: binance_public"));
  Require(public_market.ok() &&
              public_market->market_data_source ==
                  hbot::MarketDataSource::BinancePublic &&
              !public_market->replay_fixture,
          "explicit public source");
  auto conflicting_sources = hbot::ParseConfig(
      Replace(kValid, "replay_fixture: examples/paper_market.json",
              "market_data_source: binance_public\nreplay_fixture: "
              "examples/paper_market.json"));
  Require(!conflicting_sources.ok(), "conflicting data sources rejected");

  auto live = hbot::ParseConfig(Replace(kValid, "mode: paper", "mode: live"));
  Require(!live.ok(), "live disabled until G4");
  auto implicit_mode = hbot::ParseConfig(Replace(kValid, "mode: paper\n", ""));
  Require(!implicit_mode.ok(), "mode must be explicit");
  auto duplicate_owner =
      hbot::ParseConfig(Replace(kValid, "assignments:\n",
                                "  - owner_key: 1\n    strategy: simple_pmm\n"
                                "    account: paper\n    markets: [BTC-USDT]\n"
                                "    order_amount: \"0.01\"\n"
                                "    bid_spread: \"0.001\"\n"
                                "    ask_spread: \"0.001\"\n"
                                "    refresh_interval: 15s\nassignments:\n"));
  Require(!duplicate_owner.ok(), "duplicate owner rejected");
  auto split_market = hbot::ParseConfig(
      Replace(kValid, "markets: [BTC-USDT, ETH-USDT]\n    owners: [1]",
              "markets: [BTC-USDT]\n    owners: [1]\n    accounts: [paper]\n"
              "  - shard: 1\n    markets: [ETH-USDT]\n    owners: []"));
  Require(!split_market.ok(), "cross-shard strategy dependency rejected");
  auto overgrant = hbot::ParseConfig(
      Replace(kValid, "hard_limit: \"50\"", "hard_limit: \"101\""));
  Require(!overgrant.ok(), "lease aggregate over balance rejected");
  auto inline_secret = hbot::ParseConfig(
      Replace(kValid, "    initial_balances:\n",
              "    api_key: forbidden\n    initial_balances:\n"));
  Require(!inline_secret.ok(), "inline secret rejected");
  auto bad_version = hbot::ParseConfig(
      Replace(kValid, "schema_version: 1", "schema_version: 2"));
  Require(!bad_version.ok(), "schema version rejected");
  auto missing_market_data = hbot::ParseConfig(
      Replace(kValid, "replay_fixture: examples/paper_market.json\n", ""));
  Require(!missing_market_data.ok(),
          "paper start without market data rejected");
  auto empty_storage = hbot::ParseConfig(
      Replace(kValid, "storage_path: /tmp/hbot-paper/history.sqlite",
              "storage_path: \"\""));
  Require(!empty_storage.ok(), "empty storage path rejected");
  return 0;
}
