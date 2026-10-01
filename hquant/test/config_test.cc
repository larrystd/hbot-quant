#include "application/config.h"

#include <cstdlib>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "base/error.h"

namespace {

constexpr const char* kValid = R"yaml(
schema_version: 1
mode: simulated
loop_mode: blocking
storage_path: /tmp/hquant-simulated_exchange/history.sqlite
replay_fixture: examples/replay_market.json
accounts:
  - account: simulated
    initial_balances:
      BTC: "1"
      USDT: "100"
market_specs:
  - market: BTC-USDT
    exchange: binance
    base_asset: BTC
    quote_asset: USDT
    tick_lot_size:
      price_per_tick: "0.01"
      amount_per_lot: "0.001"
      tick_lot_version: 1
    trading_rule:
      price_increment: "0.01"
      base_increment: "0.001"
      min_base_amount: "0.001"
      min_notional: "0.01"
  - market: ETH-USDT
    exchange: binance
    base_asset: ETH
    quote_asset: USDT
    tick_lot_size:
      price_per_tick: "0.01"
      amount_per_lot: "0.001"
      tick_lot_version: 1
    trading_rule:
      price_increment: "0.01"
      base_increment: "0.001"
      min_base_amount: "0.001"
      min_notional: "0.01"
strategy_configs:
  - strategy_id: 1
    strategy: simple_pmm
    account: simulated
    markets: [BTC-USDT, ETH-USDT]
    order_amount: "0.01"
    bid_spread: "0.001"
    ask_spread: "0.001"
    refresh_interval: 15s
assignments:
  - shard: 0
    markets: [BTC-USDT, ETH-USDT]
    strategy_ids: [1]
    accounts: [simulated]
risk_budgets:
  - account: simulated
    asset: USDT
    shard: 0
    hard_limit: "50"
rate_budgets:
  - account: simulated
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
  auto valid = hquant::ParseConfig(kValid);
  Require(valid.ok(), "valid G1 config");
  Require(valid->mode == hquant::EngineMode::Simulated, "simulated mode");
  Require(valid->assignments.size() == 1 &&
              valid->assignments[0].markets.size() == 2 &&
              valid->strategy_configs[0].refresh_interval.count() == 15'000'000,
          "parsed assignment and timer");
  Require(valid->risk_budgets.size() == 1 && valid->rate_budgets.size() == 1,
          "parsed budgets");
  Require(valid->replay_fixture == "examples/replay_market.json",
          "replay source");
  Require(valid->strategy_configs[0].maker_fee_rate.ToString() == "0.001" &&
              valid->strategy_configs[0].price_type == hquant::PmmPriceType::Mid &&
              valid->strategy_configs[0].timer_period.count() == 1'000'000 &&
              valid->simulated_exchange.maker_fee_rate.ToString() == "0.001" &&
              valid->risk.fee_buffer_rate.ToString() == "0" &&
              !valid->risk.max_rule_age &&
              !valid->market_specs[0].stale_after &&
              !valid->risk_budgets[0].valid_for &&
              valid->storage.writer_queue == 1024 &&
              valid->storage.writer_batch == 64 &&
              valid->storage.reader_queue == 32 &&
              valid->storage.reader_page_limit == 500,
          "new field defaults");
  std::string configured = kValid;
  configured = Replace(configured, "storage_path: /tmp/hquant-simulated_exchange/history.sqlite",
                       "storage_path: /tmp/hquant-simulated_exchange/history.sqlite\n"
                       "storage: {writer_queue: 128, writer_batch: 16, reader_queue: 8, reader_page_limit: 25}\n"
                       "risk: {fee_buffer_rate: '0.02', max_rule_age: 2h}\n"
                       "simulated_exchange: {maker_fee_rate: '0.003'}");
  configured = Replace(configured, "    trading_rule:\n",
                       "    stale_after: 7s\n    trading_rule:\n");
  configured = Replace(configured, "    refresh_interval: 15s\n",
                       "    refresh_interval: 15s\n    maker_fee_rate: '0.004'\n"
                       "    price_type: last\n    timer_period: 250ms\n");
  configured = Replace(configured, "    hard_limit: \"50\"\n",
                       "    hard_limit: \"50\"\n    valid_for: 3h\n");
  auto custom = hquant::ParseConfig(configured);
  Require(custom.ok(), "custom settings parsed");
  Require(custom->market_specs[0].stale_after->count() == 7'000'000 &&
              custom->strategy_configs[0].maker_fee_rate.ToString() == "0.004" &&
              custom->strategy_configs[0].price_type == hquant::PmmPriceType::Last &&
              custom->strategy_configs[0].timer_period.count() == 250'000 &&
              custom->simulated_exchange.maker_fee_rate.ToString() == "0.003" &&
              custom->risk.fee_buffer_rate.ToString() == "0.02" &&
              custom->risk.max_rule_age->count() == 7'200'000'000 &&
              custom->risk_budgets[0].valid_for->count() == 10'800'000'000 &&
              custom->storage.writer_queue == 128 &&
              custom->storage.writer_batch == 16 &&
              custom->storage.reader_queue == 8 &&
              custom->storage.reader_page_limit == 25,
          "custom settings values");
  for (const auto& [field, bad] :
       std::vector<std::pair<std::string, std::string>>{
           {"stale_after: 7s", "stale_after: 0s"},
           {"maker_fee_rate: '0.004'", "maker_fee_rate: '1'"},
           {"price_type: last", "price_type: mark"},
           {"timer_period: 250ms", "timer_period: 0ms"},
           {"maker_fee_rate: '0.003'", "maker_fee_rate: '-1'"},
           {"fee_buffer_rate: '0.02'", "fee_buffer_rate: '1'"},
           {"max_rule_age: 2h", "max_rule_age: 500ms"},
           {"valid_for: 3h", "valid_for: 0h"},
           {"writer_queue: 128", "writer_queue: 0"},
           {"writer_batch: 16", "writer_batch: 0"},
           {"reader_queue: 8", "reader_queue: 0"},
           {"reader_page_limit: 25", "reader_page_limit: 0"},
       }) {
    auto invalid = hquant::ParseConfig(Replace(configured, field, bad));
    Require(!invalid.ok(), "invalid custom setting rejected");
  }
  auto public_market = hquant::ParseConfig(
      Replace(kValid, "replay_fixture: examples/replay_market.json",
              "market_data_source: binance_public"));
  Require(public_market.ok() &&
              public_market->market_data_source ==
                  hquant::MarketDataSource::BinancePublic &&
              !public_market->replay_fixture,
          "explicit public source");
  auto conflicting_sources = hquant::ParseConfig(
      Replace(kValid, "replay_fixture: examples/replay_market.json",
              "market_data_source: binance_public\nreplay_fixture: "
              "examples/replay_market.json"));
  Require(!conflicting_sources.ok(), "conflicting data sources rejected");

  auto live =
      hquant::ParseConfig(Replace(kValid, "mode: simulated", "mode: live"));
  Require(!live.ok(), "live disabled until G4");
  Require(
      hquant::CodeOf(live.status()) == hquant::ErrorCode::kConfigModeNotAllowed,
      "live mode code");
  auto implicit_mode =
      hquant::ParseConfig(Replace(kValid, "mode: simulated\n", ""));
  Require(!implicit_mode.ok(), "mode must be explicit");
  auto duplicate_strategy = hquant::ParseConfig(
      Replace(kValid, "assignments:\n",
              "  - strategy_id: 1\n    strategy: simple_pmm\n"
              "    account: simulated\n    markets: [BTC-USDT]\n"
              "    order_amount: \"0.01\"\n"
              "    bid_spread: \"0.001\"\n"
              "    ask_spread: \"0.001\"\n"
              "    refresh_interval: 15s\nassignments:\n"));
  Require(!duplicate_strategy.ok(), "duplicate strategy_id rejected");
  auto split_market = hquant::ParseConfig(Replace(
      kValid, "markets: [BTC-USDT, ETH-USDT]\n    strategy_ids: [1]",
      "markets: [BTC-USDT]\n    strategy_ids: [1]\n    accounts: [simulated]\n"
      "  - shard: 1\n    markets: [ETH-USDT]\n    strategy_ids: []"));
  Require(!split_market.ok(), "cross-shard strategy dependency rejected");
  Require(hquant::CodeOf(split_market.status()) ==
              hquant::ErrorCode::kConfigAssignmentInvalid,
          "assignment code");
  auto overgrant = hquant::ParseConfig(
      Replace(kValid, "hard_limit: \"50\"", "hard_limit: \"101\""));
  Require(!overgrant.ok(), "budget aggregate over balance rejected");
  Require(hquant::CodeOf(overgrant.status()) ==
              hquant::ErrorCode::kConfigBudgetInvalid,
          "budget code");
  auto inline_secret = hquant::ParseConfig(
      Replace(kValid, "    initial_balances:\n",
              "    api_key: forbidden\n    initial_balances:\n"));
  Require(!inline_secret.ok(), "inline secret rejected");
  Require(hquant::CodeOf(inline_secret.status()) ==
              hquant::ErrorCode::kConfigContainsSecret,
          "inline secret code");
  auto bad_version = hquant::ParseConfig(
      Replace(kValid, "schema_version: 1", "schema_version: 2"));
  Require(!bad_version.ok(), "schema version rejected");
  Require(hquant::CodeOf(bad_version.status()) ==
              hquant::ErrorCode::kConfigSchemaUnsupported,
          "schema code");
  auto missing_market_data = hquant::ParseConfig(
      Replace(kValid, "replay_fixture: examples/replay_market.json\n", ""));
  Require(!missing_market_data.ok(),
          "simulated start without market data rejected");
  auto empty_storage = hquant::ParseConfig(Replace(
      kValid, "storage_path: /tmp/hquant-simulated_exchange/history.sqlite",
      "storage_path: \"\""));
  Require(!empty_storage.ok(), "empty storage path rejected");
  return 0;
}
