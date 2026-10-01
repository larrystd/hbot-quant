#pragma once

#include <chrono>
#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "absl/status/statusor.h"
#include "base/market.h"
#include "base/types.h"

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
  BookScale book_scale;
  TradingRule trading_rule;
};

struct StrategyConfig {
  OwnerId owner;
  AccountId account;
  std::vector<MarketId> markets;
  Decimal order_amount;
  Decimal bid_spread;
  Decimal ask_spread;
  std::chrono::microseconds refresh_interval{0};
};

struct ShardAssignment {
  ShardId shard;
  std::vector<MarketId> markets;
  std::vector<OwnerId> owners;
  std::vector<AccountId> accounts;
};

struct StaticRiskLeaseConfig {
  AccountId account;
  AssetId asset;
  ShardId shard;
  Decimal hard_limit;
};

struct StaticRateLeaseConfig {
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
  std::vector<StaticRiskLeaseConfig> static_risk_leases;
  std::vector<StaticRateLeaseConfig> static_rate_leases;
  std::string storage_path;
  std::optional<std::string> replay_fixture;
};

// YAML is parsed only at this boundary. G1 explicitly permits Simulated mode;
// live admission is deferred until the isolated G4 gateway is ready.
absl::StatusOr<AppConfig> ParseConfig(std::string_view yaml_text);
absl::StatusOr<AppConfig> LoadConfig(const std::string& path);

}  // namespace hquant
