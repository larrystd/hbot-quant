#pragma once

#include <cstddef>
#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "hquant/base/ids.h"

namespace hquant::v1 {

enum class InputSource { Replay, BinancePublic };

struct InputConfig {
  InputSource source = InputSource::Replay;
  std::string replay_file;
  std::string rest_host = "data-api.binance.vision";
  std::string websocket_host = "data-stream.binance.vision";
  uint16_t rest_port = 443;
  uint16_t websocket_port = 443;
  bool tls = true;
  uint64_t depth_snapshot_limit = 1000;
  uint64_t max_snapshot_requests_per_run = 0;
};

struct TradingRule {
  uint64_t price_increment_nanos = 0;
  uint64_t base_increment_nanos = 0;
  uint64_t min_base_amount_nanos = 0;
  uint64_t min_order_value_nanos = 0;
};

struct MarketConfig {
  MarketId id;
  std::string base_asset;
  std::string quote_asset;
  uint64_t price_per_tick_nanos = 0;
  uint64_t amount_per_lot_nanos = 0;
  TradingRule rule;
  int64_t stale_after_ms = 5000;
  size_t max_buffered_diffs = 1024;
};

struct StrategyConfig {
  uint64_t order_amount_nanos = 0;
  uint64_t bid_spread_ppb = 0;
  uint64_t ask_spread_ppb = 0;
  int64_t refresh_ms = 15000;
};

// OnlineSim connects to Binance Spot Demo with virtual funds, never production.
enum class TradingMode { PressureSim, OnlineSim };

struct ApiCredentials {
  std::string key;
  std::string secret;
};

struct AccountConfig {
  struct RiskPolicy {
    uint64_t max_order_value_nanos = 0;
    uint64_t max_open_value_per_quote_nanos = 0;
    uint64_t max_active_orders = 0;
    uint64_t max_symbol_active_orders = 0;
  };
  std::string id;
  TradingMode mode = TradingMode::PressureSim;
  std::map<std::string, uint64_t> initial_balances_nanos;
  std::map<std::string, uint64_t> hard_limits_nanos;
  uint64_t maker_fee_rate_ppb = 0;
  std::string trading_rest_host;
  uint16_t trading_rest_port = 0;
  size_t rest_connections = 1;
  std::string private_ws_host;
  uint16_t private_ws_port = 0;
  bool tls = true;
  std::string api_key_env;
  std::string api_secret_env;
  std::string api_credentials_file;
  uint64_t max_submissions_per_run = 0;
  RiskPolicy risk;
  uint64_t request_weight_per_minute = 0;
  uint64_t order_placements_per_ten_seconds = 0;
};

struct ShardConfig {
  ShardId id;
  MarketConfig market;
  StrategyConfig strategy;
  std::string account_id;
  // A shard spends only its own allocation. Missing entries are divided among
  // shards of the same account when the runtime creates them.
  std::map<std::string, uint64_t> asset_limits_nanos;
  std::optional<uint64_t> max_active_orders;
  std::optional<uint64_t> max_open_value_per_quote_nanos;
};

struct ShardAllocation {
  std::map<std::string, uint64_t> assets_nanos;
  std::optional<uint64_t> max_active_orders;
  std::optional<uint64_t> max_open_value_per_quote_nanos;
};

struct AppConfig {
  uint64_t schema_version = 2;
  InputConfig input;
  std::vector<AccountConfig> accounts;
  std::vector<ShardConfig> shards;
  std::string sqlite_path;
  std::string control_socket;
};

struct StrategyOverrides {
  std::optional<ShardId> shard;
  std::optional<uint64_t> order_amount_nanos;
  std::optional<uint64_t> bid_spread_ppb;
  std::optional<uint64_t> ask_spread_ppb;
  std::optional<int64_t> refresh_ms;
};

absl::StatusOr<AppConfig> ParseConfig(std::string_view yaml_text,
                                      std::string_view state_dir);
absl::StatusOr<AppConfig> LoadConfig(const std::string& path,
                                     std::string_view state_dir);
absl::StatusOr<ApiCredentials> LoadApiCredentialsFile(const std::string& path);
absl::Status ApplyStrategyOverrides(AppConfig& config,
                                    const StrategyOverrides& overrides);
absl::Status ValidateConfig(const AppConfig& config);
absl::StatusOr<std::map<uint16_t, ShardAllocation>> ResolveShardAllocations(
    const AppConfig& config);
absl::Status ValidateStrategyConfig(const StrategyConfig& config);

}  // namespace hquant::v1
