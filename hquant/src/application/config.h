#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "absl/status/statusor.h"
#include "base/market.h"
#include "base/types.h"
#include "strategy/simple_pmm.h"

namespace hquant {

enum class EngineMode { Simulated, Live };
enum class LoopMode { Blocking, Busy };
enum class MarketDataSource { Replay, BinancePublic };

struct AccountConfig {
  AccountId account;
  std::map<std::string, Decimal> initial_balances;
};

struct MarketConfig {
  MarketSpec spec;
  TickLotSize tick_lot_size;
  TradingRule trading_rule;
  std::optional<std::chrono::microseconds> stale_after;
};

struct StrategyConfig {
  StrategyId strategy_id;
  AccountId account;
  std::vector<MarketId> markets;
  Decimal order_amount;
  Decimal bid_spread;
  Decimal ask_spread;
  std::chrono::microseconds refresh_interval{0};
  Decimal maker_fee_rate = *Decimal::Parse("0.001");
  PmmPriceType price_type = PmmPriceType::Mid;
  std::chrono::microseconds timer_period{std::chrono::seconds(1)};
};

struct ShardAssignment {
  ShardId shard;
  std::vector<MarketId> markets;
  std::vector<StrategyId> strategy_ids;
  std::vector<AccountId> accounts;
};

struct StaticRiskBudgetConfig {
  AccountId account;
  AssetId asset;
  ShardId shard;
  Decimal hard_limit;
  std::optional<std::chrono::microseconds> valid_for;
};

struct RiskConfig {
  Decimal fee_buffer_rate = *Decimal::Parse("0");
  std::optional<std::chrono::microseconds> max_rule_age;
};

struct SimulatedExchangeOptions {
  Decimal maker_fee_rate = *Decimal::Parse("0.001");
};

struct StorageOptions {
  size_t writer_queue = 1024;
  size_t writer_batch = 64;
  size_t reader_queue = 32;
  uint32_t reader_page_limit = 500;
};

struct StaticRateBudgetConfig {
  AccountId account;
  ShardId shard;
  std::string ip;
  std::string endpoint;
  uint32_t limit = 0;
  uint32_t cancel_reserve = 0;
  std::chrono::microseconds window{0};
};

struct AppConfig {
  uint32_t schema_version = 1;
  EngineMode mode = EngineMode::Simulated;
  LoopMode loop_mode = LoopMode::Blocking;
  MarketDataSource market_data_source = MarketDataSource::Replay;
  std::vector<ShardAssignment> assignments;
  std::vector<AccountConfig> accounts;
  std::vector<MarketConfig> market_specs;
  std::vector<StrategyConfig> strategy_configs;
  std::vector<StaticRiskBudgetConfig> risk_budgets;
  std::vector<StaticRateBudgetConfig> rate_budgets;
  RiskConfig risk;
  SimulatedExchangeOptions simulated_exchange;
  StorageOptions storage;
  std::string storage_path;
  std::optional<std::string> replay_fixture;
};

// YAML is parsed only at this boundary. G1 explicitly permits Simulated mode;
// live admission is deferred until the isolated G4 gateway is ready.
absl::StatusOr<AppConfig> ParseConfig(std::string_view yaml_text);
absl::StatusOr<AppConfig> LoadConfig(const std::string& path);

}  // namespace hquant
