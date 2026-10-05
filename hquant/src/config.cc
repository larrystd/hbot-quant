#include "hquant/config.h"

#include <algorithm>
#include <charconv>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <limits>
#include <set>
#include <string>
#include <string_view>
#include <utility>

#include "absl/status/status.h"
#include "hquant/base/fixed.h"
#include "yaml-cpp/yaml.h"

namespace hquant::v1 {
namespace {

absl::Status Invalid(std::string_view message) {
  return absl::InvalidArgumentError(std::string(message));
}

absl::Status CheckKeys(const YAML::Node& node,
                       std::initializer_list<std::string_view> allowed) {
  if (!node || !node.IsMap()) return Invalid("expected YAML mapping");
  for (const auto& item : node) {
    if (!item.first.IsScalar()) return Invalid("mapping key must be a string");
    const std::string key = item.first.Scalar();
    bool found = false;
    for (std::string_view candidate : allowed) {
      if (candidate == key) found = true;
    }
    if (!found) return Invalid("unknown configuration field: " + key);
  }
  return absl::OkStatus();
}

absl::StatusOr<YAML::Node> MapField(const YAML::Node& node, const char* field) {
  const YAML::Node value = node[field];
  if (!value || !value.IsMap()) return Invalid(std::string("expected map: ") + field);
  return value;
}

absl::StatusOr<std::string> Scalar(const YAML::Node& node, const char* field) {
  const YAML::Node value = node[field];
  if (!value || !value.IsScalar() || value.Scalar().empty()) {
    return Invalid(std::string("missing scalar: ") + field);
  }
  return value.Scalar();
}

absl::StatusOr<uint64_t> Unsigned(const YAML::Node& node, const char* field) {
  auto text = Scalar(node, field);
  if (!text.ok()) return text.status();
  uint64_t value = 0;
  const auto [end, error] = std::from_chars(text->data(), text->data() + text->size(), value);
  if (error != std::errc{} || end != text->data() + text->size()) {
    return Invalid(std::string("invalid unsigned integer: ") + field);
  }
  return value;
}

absl::StatusOr<uint64_t> Fixed(const YAML::Node& node, const char* field) {
  auto text = Scalar(node, field);
  if (!text.ok()) return text.status();
  auto value = ParseFixed(*text);
  if (!value.ok()) {
    return Invalid(std::string("invalid fixed decimal ") + field + ": " +
                   std::string(value.status().message()));
  }
  return *value;
}

absl::StatusOr<std::map<std::string, uint64_t>> FixedMap(
    const YAML::Node& node, const char* field) {
  auto map = MapField(node, field);
  if (!map.ok()) return map.status();
  std::map<std::string, uint64_t> values;
  for (const auto& item : *map) {
    if (!item.first.IsScalar() || !item.second.IsScalar() ||
        item.first.Scalar().empty()) {
      return Invalid(std::string("invalid asset amount in ") + field);
    }
    auto amount = ParseFixed(item.second.Scalar());
    if (!amount.ok()) return Invalid(std::string("invalid amount in ") + field);
    if (!values.emplace(item.first.Scalar(), *amount).second) {
      return Invalid(std::string("duplicate asset in ") + field);
    }
  }
  return values;
}

absl::StatusOr<MarketConfig> ParseMarket(const YAML::Node& node) {
  if (auto status = CheckKeys(node, {"symbol", "base_asset", "quote_asset",
                                     "price_per_tick", "amount_per_lot",
                                     "stale_after_ms", "max_buffered_diffs",
                                     "trading_rule"}); !status.ok()) return status;
  MarketConfig market;
  auto symbol = Scalar(node, "symbol");
  auto base = Scalar(node, "base_asset");
  auto quote = Scalar(node, "quote_asset");
  auto price_tick = Fixed(node, "price_per_tick");
  auto amount_lot = Fixed(node, "amount_per_lot");
  auto rule = MapField(node, "trading_rule");
  if (!symbol.ok()) return symbol.status();
  if (!base.ok()) return base.status();
  if (!quote.ok()) return quote.status();
  if (!price_tick.ok()) return price_tick.status();
  if (!amount_lot.ok()) return amount_lot.status();
  if (!rule.ok()) return rule.status();
  if (auto status = CheckKeys(*rule, {"price_increment", "base_increment",
                                      "min_base_amount", "min_order_value"});
      !status.ok()) return status;
  auto price_increment = Fixed(*rule, "price_increment");
  auto base_increment = Fixed(*rule, "base_increment");
  auto min_base = Fixed(*rule, "min_base_amount");
  auto min_value = Fixed(*rule, "min_order_value");
  if (!price_increment.ok()) return price_increment.status();
  if (!base_increment.ok()) return base_increment.status();
  if (!min_base.ok()) return min_base.status();
  if (!min_value.ok()) return min_value.status();
  market.id.symbol = *symbol;
  market.base_asset = *base;
  market.quote_asset = *quote;
  market.price_per_tick_nanos = *price_tick;
  market.amount_per_lot_nanos = *amount_lot;
  market.rule = {*price_increment, *base_increment, *min_base, *min_value};
  if (node["stale_after_ms"]) {
    auto value = Unsigned(node, "stale_after_ms");
    if (!value.ok() || *value > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
      return Invalid("stale_after_ms out of range");
    }
    market.stale_after_ms = static_cast<int64_t>(*value);
  }
  if (node["max_buffered_diffs"]) {
    auto value = Unsigned(node, "max_buffered_diffs");
    if (!value.ok() || *value > 1'000'000) return Invalid("max_buffered_diffs out of range");
    market.max_buffered_diffs = static_cast<size_t>(*value);
  }
  return market;
}

absl::StatusOr<StrategyConfig> ParseStrategy(const YAML::Node& node) {
  if (auto status = CheckKeys(node, {"order_amount", "bid_spread",
                                     "ask_spread", "refresh_ms"});
      !status.ok()) return status;
  auto amount = Fixed(node, "order_amount");
  auto bid = Fixed(node, "bid_spread");
  auto ask = Fixed(node, "ask_spread");
  auto refresh = Unsigned(node, "refresh_ms");
  if (!amount.ok()) return amount.status();
  if (!bid.ok()) return bid.status();
  if (!ask.ok()) return ask.status();
  if (!refresh.ok() || *refresh > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
    return Invalid("refresh_ms out of range");
  }
  return StrategyConfig{*amount, *bid, *ask, static_cast<int64_t>(*refresh)};
}

absl::StatusOr<ExecutorConfig> ParseExecutor(const YAML::Node& node) {
  if (auto status = CheckKeys(node, {"mode", "account", "initial_balances",
                                     "hard_limits", "maker_fee_rate"});
      !status.ok()) return status;
  // mode is required: paper vs live must never be decided by a default.
  auto mode = Scalar(node, "mode");
  if (!mode.ok()) return mode.status();
  if (*mode != "paper" && *mode != "live") {
    return Invalid("executor.mode must be paper or live");
  }
  auto account = Scalar(node, "account");
  auto balances = FixedMap(node, "initial_balances");
  auto limits = FixedMap(node, "hard_limits");
  auto fee = Fixed(node, "maker_fee_rate");
  if (!account.ok()) return account.status();
  if (!balances.ok()) return balances.status();
  if (!limits.ok()) return limits.status();
  if (!fee.ok()) return fee.status();
  return ExecutorConfig{*mode == "live" ? TradingMode::Live : TradingMode::Paper,
                        *account, std::move(*balances), std::move(*limits), *fee};
}

absl::StatusOr<InputConfig> ParseInput(const YAML::Node& node) {
  if (auto status = CheckKeys(node, {"source", "replay_file", "rest_host",
                                     "websocket_host", "rest_port",
                                     "websocket_port", "tls"});
      !status.ok()) return status;
  InputConfig input;
  auto source = Scalar(node, "source");
  if (!source.ok()) return source.status();
  if (*source == "replay") {
    input.source = InputSource::Replay;
    auto file = Scalar(node, "replay_file");
    if (!file.ok()) return file.status();
    input.replay_file = *file;
  } else if (*source == "binance_public") {
    input.source = InputSource::BinancePublic;
  } else {
    return Invalid("input.source must be replay or binance_public");
  }
  if (node["rest_host"]) {
    auto value = Scalar(node, "rest_host");
    if (!value.ok()) return value.status();
    input.rest_host = *value;
  }
  if (node["websocket_host"]) {
    auto value = Scalar(node, "websocket_host");
    if (!value.ok()) return value.status();
    input.websocket_host = *value;
  }
  if (node["rest_port"]) {
    auto value = Unsigned(node, "rest_port");
    if (!value.ok() || *value == 0 || *value > 65535) return Invalid("invalid rest_port");
    input.rest_port = static_cast<uint16_t>(*value);
  }
  if (node["websocket_port"]) {
    auto value = Unsigned(node, "websocket_port");
    if (!value.ok() || *value == 0 || *value > 65535) return Invalid("invalid websocket_port");
    input.websocket_port = static_cast<uint16_t>(*value);
  }
  if (node["tls"]) {
    auto value = Scalar(node, "tls");
    if (!value.ok() || (*value != "true" && *value != "false")) {
      return Invalid("input.tls must be true or false");
    }
    input.tls = *value == "true";
  }
  return input;
}

}  // namespace

absl::Status ValidateStrategyConfig(const StrategyConfig& strategy) {
  if (strategy.order_amount_nanos == 0 || strategy.refresh_ms <= 0 ||
      strategy.refresh_ms > std::numeric_limits<int64_t>::max() / 1000 ||
      strategy.bid_spread_ppb >= kFixedScale ||
      strategy.ask_spread_ppb >= kFixedScale) {
    return Invalid("invalid strategy configuration");
  }
  return absl::OkStatus();
}

absl::Status ValidateConfig(const AppConfig& config) {
  if (config.sqlite_path.empty() || config.control_socket.empty()) {
    return Invalid("state_dir must be nonempty");
  }
  if (config.input.source == InputSource::Replay && config.input.replay_file.empty()) {
    return Invalid("replay_file required");
  }
  if (config.input.source == InputSource::BinancePublic &&
      (config.input.rest_host.empty() || config.input.websocket_host.empty())) {
    return Invalid("Binance hosts required");
  }
  if (config.shards.empty() || config.shards.size() > 8) {
    return Invalid("shards must contain one to eight entries");
  }
  std::set<uint8_t> ids;
  std::set<std::string> accounts;
  for (const ShardConfig& shard : config.shards) {
    if (shard.id.value >= 8 || !ids.insert(shard.id.value).second) {
      return Invalid("invalid or duplicate shard id");
    }
    const MarketConfig& market = shard.market;
    if (config.input.source == InputSource::BinancePublic &&
        !std::all_of(market.id.symbol.begin(), market.id.symbol.end(),
                     [](char ch) {
                       return (ch >= 'A' && ch <= 'Z') ||
                              (ch >= '0' && ch <= '9');
                     })) {
      return Invalid("Binance symbol must contain uppercase letters and digits");
    }
    if (market.id.symbol.empty() || market.base_asset.empty() ||
        market.quote_asset.empty() || market.base_asset == market.quote_asset ||
        market.price_per_tick_nanos == 0 || market.amount_per_lot_nanos == 0 ||
        market.stale_after_ms <= 0 || market.max_buffered_diffs == 0) {
      return Invalid("invalid market configuration");
    }
    if (market.rule.price_increment_nanos == 0 ||
        market.rule.base_increment_nanos == 0 ||
        market.rule.min_base_amount_nanos == 0 ||
        market.rule.min_order_value_nanos == 0 ||
        market.rule.price_increment_nanos % market.price_per_tick_nanos != 0 ||
        market.rule.base_increment_nanos % market.amount_per_lot_nanos != 0) {
      return Invalid("invalid trading rule or tick/lot alignment");
    }
    const StrategyConfig& strategy = shard.strategy;
    if (auto status = ValidateStrategyConfig(strategy); !status.ok()) return status;
    const ExecutorConfig& executor = shard.executor;
    // 目前只有 PaperExchange，没有真实交易所适配器。放行 live 会让订单仍进
    // PaperExchange 却不撮合，看起来像实盘实际什么都没发生，所以先拒绝。
    if (executor.mode == TradingMode::Live) {
      return Invalid("executor.mode live is not implemented");
    }
    if (executor.account.empty() || !accounts.insert(executor.account).second ||
        executor.maker_fee_rate_ppb >= kFixedScale) {
      return Invalid("invalid or duplicate account configuration");
    }
    for (const std::string& asset : {market.base_asset, market.quote_asset}) {
      if (!executor.initial_balances_nanos.contains(asset) ||
          !executor.hard_limits_nanos.contains(asset)) {
        return Invalid("missing balance or hard limit for market asset");
      }
    }
  }
  return absl::OkStatus();
}

absl::StatusOr<AppConfig> ParseConfig(std::string_view yaml_text,
                                      std::string_view state_dir) {
  if (state_dir.empty()) return Invalid("state_dir must be nonempty");
  try {
    const YAML::Node root = YAML::Load(std::string(yaml_text));
    if (auto status = CheckKeys(root, {"schema_version", "input", "shards"});
        !status.ok()) return status;
    auto version = Unsigned(root, "schema_version");
    if (!version.ok() || *version != 1) return Invalid("unsupported schema_version");
    auto input_node = MapField(root, "input");
    if (!input_node.ok()) return input_node.status();
    auto input = ParseInput(*input_node);
    if (!input.ok()) return input.status();
    const YAML::Node shards_node = root["shards"];
    if (!shards_node || !shards_node.IsSequence()) return Invalid("shards must be a sequence");
    AppConfig config;
    config.input = std::move(*input);
    const std::filesystem::path state_path{std::string(state_dir)};
    config.sqlite_path = (state_path / "history.sqlite").string();
    config.control_socket = (state_path / "control.sock").string();
    for (const YAML::Node& node : shards_node) {
      if (auto status = CheckKeys(node, {"id", "market", "strategy", "executor"});
          !status.ok()) return status;
      auto id = Unsigned(node, "id");
      auto market_node = MapField(node, "market");
      auto strategy_node = MapField(node, "strategy");
      auto executor_node = MapField(node, "executor");
      if (!id.ok() || *id >= 8) return Invalid("shard id must be in [0,7]");
      if (!market_node.ok()) return market_node.status();
      if (!strategy_node.ok()) return strategy_node.status();
      if (!executor_node.ok()) return executor_node.status();
      auto market = ParseMarket(*market_node);
      auto strategy = ParseStrategy(*strategy_node);
      auto executor = ParseExecutor(*executor_node);
      if (!market.ok()) return market.status();
      if (!strategy.ok()) return strategy.status();
      if (!executor.ok()) return executor.status();
      config.shards.push_back(ShardConfig{ShardId{static_cast<uint8_t>(*id)},
                                          std::move(*market), std::move(*strategy),
                                          std::move(*executor)});
    }
    if (auto status = ValidateConfig(config); !status.ok()) return status;
    return config;
  } catch (const YAML::Exception& error) {
    return Invalid(std::string("invalid YAML: ") + error.what());
  }
}

absl::StatusOr<AppConfig> LoadConfig(const std::string& path,
                                     std::string_view state_dir) {
  std::ifstream file(path);
  if (!file) return absl::NotFoundError("cannot open config: " + path);
  const std::string text((std::istreambuf_iterator<char>(file)),
                         std::istreambuf_iterator<char>());
  if (!file.eof() && file.fail()) return absl::DataLossError("cannot read config: " + path);
  return ParseConfig(text, state_dir);
}

absl::Status ApplyStrategyOverrides(AppConfig& config,
                                    const StrategyOverrides& overrides) {
  const bool any = overrides.order_amount_nanos || overrides.bid_spread_ppb ||
                   overrides.ask_spread_ppb || overrides.refresh_ms;
  if (any && !overrides.shard) return Invalid("strategy_shard required for overrides");
  if (!overrides.shard) return absl::OkStatus();
  for (ShardConfig& shard : config.shards) {
    if (shard.id != *overrides.shard) continue;
    AppConfig candidate = config;
    for (ShardConfig& target : candidate.shards) {
      if (target.id != *overrides.shard) continue;
      if (overrides.order_amount_nanos) target.strategy.order_amount_nanos = *overrides.order_amount_nanos;
      if (overrides.bid_spread_ppb) target.strategy.bid_spread_ppb = *overrides.bid_spread_ppb;
      if (overrides.ask_spread_ppb) target.strategy.ask_spread_ppb = *overrides.ask_spread_ppb;
      if (overrides.refresh_ms) target.strategy.refresh_ms = *overrides.refresh_ms;
    }
    if (auto status = ValidateConfig(candidate); !status.ok()) return status;
    config = std::move(candidate);
    return absl::OkStatus();
  }
  return Invalid("strategy_shard not found");
}

}  // namespace hquant::v1
