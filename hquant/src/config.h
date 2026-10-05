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

// Paper：PaperExchange 用行情撮合挂单；Live：成交由真实交易所回报，不做本地撮合。
enum class TradingMode { Paper, Live };

struct ExecutorConfig {
  TradingMode mode = TradingMode::Paper;
  std::string account;
  std::map<std::string, uint64_t> initial_balances_nanos;
  std::map<std::string, uint64_t> hard_limits_nanos;
  uint64_t maker_fee_rate_ppb = 0;
};

struct ShardConfig {
  ShardId id;
  MarketConfig market;
  StrategyConfig strategy;
  ExecutorConfig executor;
};

struct AppConfig {
  InputConfig input;
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
absl::Status ApplyStrategyOverrides(AppConfig& config,
                                    const StrategyOverrides& overrides);
absl::Status ValidateConfig(const AppConfig& config);
absl::Status ValidateStrategyConfig(const StrategyConfig& config);

}  // namespace hquant::v1
