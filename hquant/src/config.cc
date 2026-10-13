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
#include <tuple>
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
  if (!value || !value.IsMap())
    return Invalid(std::string("expected map: ") + field);
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
  const auto [end, error] =
      std::from_chars(text->data(), text->data() + text->size(), value);
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

absl::StatusOr<std::map<std::string, uint64_t>> FixedMap(const YAML::Node& node,
                                                         const char* field) {
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
  if (auto status =
          CheckKeys(node, {"symbol", "base_asset", "quote_asset",
                           "price_per_tick", "amount_per_lot", "stale_after_ms",
                           "max_buffered_diffs", "trading_rule"});
      !status.ok())
    return status;
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
      !status.ok())
    return status;
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
    if (!value.ok() ||
        *value > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
      return Invalid("stale_after_ms out of range");
    }
    market.stale_after_ms = static_cast<int64_t>(*value);
  }
  if (node["max_buffered_diffs"]) {
    auto value = Unsigned(node, "max_buffered_diffs");
    if (!value.ok() || *value > 1'000'000)
      return Invalid("max_buffered_diffs out of range");
    market.max_buffered_diffs = static_cast<size_t>(*value);
  }
  return market;
}

absl::StatusOr<StrategyConfig> ParseStrategy(const YAML::Node& node) {
  if (auto status = CheckKeys(
          node, {"order_amount", "bid_spread", "ask_spread", "refresh_ms"});
      !status.ok())
    return status;
  auto amount = Fixed(node, "order_amount");
  auto bid = Fixed(node, "bid_spread");
  auto ask = Fixed(node, "ask_spread");
  auto refresh = Unsigned(node, "refresh_ms");
  if (!amount.ok()) return amount.status();
  if (!bid.ok()) return bid.status();
  if (!ask.ok()) return ask.status();
  if (!refresh.ok() ||
      *refresh > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
    return Invalid("refresh_ms out of range");
  }
  return StrategyConfig{*amount, *bid, *ask, static_cast<int64_t>(*refresh)};
}

absl::StatusOr<AccountConfig> ParseAccount(const YAML::Node& node) {
  if (auto status = CheckKeys(
          node, {"id", "mode", "venue", "trading_rest", "private_ws",
                 "rest_connections", "initial_balances", "hard_limits",
                 "maker_fee_rate", "api_key_env", "api_secret_env",
                 "api_credentials_file", "max_submissions_per_run", "risk",
                 "request_limits"});
      !status.ok())
    return status;
  auto id = Scalar(node, "id");
  auto mode = Scalar(node, "mode");
  auto balances = FixedMap(node, "initial_balances");
  auto limits = FixedMap(node, "hard_limits");
  auto fee = Fixed(node, "maker_fee_rate");
  if (!id.ok()) return id.status();
  if (!mode.ok()) return mode.status();
  if (!balances.ok()) return balances.status();
  if (!limits.ok()) return limits.status();
  if (!fee.ok()) return fee.status();
  AccountConfig account;
  account.id = *id;
  if (node["rest_connections"]) {
    auto connections = Unsigned(node, "rest_connections");
    if (!connections.ok() || *connections < 1 || *connections > 32)
      return Invalid("rest_connections must be 1..32");
    account.rest_connections = static_cast<size_t>(*connections);
  }
  account.initial_balances_nanos = std::move(*balances);
  account.hard_limits_nanos = std::move(*limits);
  account.maker_fee_rate_ppb = *fee;
  if (node["risk"]) {
    auto risk = MapField(node, "risk");
    if (!risk.ok()) return risk.status();
    if (auto status =
            CheckKeys(*risk, {"max_order_value", "max_open_value_per_quote",
                              "max_active_orders", "max_symbol_active_orders"});
        !status.ok())
      return status;
    if ((*risk)["max_order_value"]) {
      auto value = Fixed(*risk, "max_order_value");
      if (!value.ok()) return value.status();
      account.risk.max_order_value_nanos = *value;
    }
    if ((*risk)["max_open_value_per_quote"]) {
      auto value = Fixed(*risk, "max_open_value_per_quote");
      if (!value.ok()) return value.status();
      account.risk.max_open_value_per_quote_nanos = *value;
    }
    if ((*risk)["max_active_orders"]) {
      auto value = Unsigned(*risk, "max_active_orders");
      if (!value.ok()) return value.status();
      account.risk.max_active_orders = *value;
    }
    if ((*risk)["max_symbol_active_orders"]) {
      auto value = Unsigned(*risk, "max_symbol_active_orders");
      if (!value.ok()) return value.status();
      account.risk.max_symbol_active_orders = *value;
    }
  }
  if (node["request_limits"]) {
    auto limits = MapField(node, "request_limits");
    if (!limits.ok()) return limits.status();
    if (auto status =
            CheckKeys(*limits, {"weight_per_minute", "orders_per_ten_seconds"});
        !status.ok())
      return status;
    if ((*limits)["weight_per_minute"]) {
      auto value = Unsigned(*limits, "weight_per_minute");
      if (!value.ok() || *value < 80) {
        return Invalid("request weight limit must be at least 80");
      }
      account.request_weight_per_minute = *value;
    }
    if ((*limits)["orders_per_ten_seconds"]) {
      auto value = Unsigned(*limits, "orders_per_ten_seconds");
      if (!value.ok() || *value == 0) {
        return Invalid("order placement limit must be positive");
      }
      account.order_placements_per_ten_seconds = *value;
    }
  }
  if (*mode == "pressure_sim") {
    account.mode = TradingMode::PressureSim;
  } else if (*mode == "online_sim") {
    account.mode = TradingMode::OnlineSim;
  } else {
    return Invalid("account.mode must be pressure_sim or online_sim");
  }
  auto venue = Scalar(node, "venue");
  if (!venue.ok() || *venue != "binance_spot") {
    return Invalid("account.venue must be binance_spot");
  }
  auto rest = MapField(node, "trading_rest");
  auto stream = MapField(node, "private_ws");
  if (!rest.ok()) return rest.status();
  if (!stream.ok()) return stream.status();
  for (const auto& [endpoint, label, host, port] :
       {std::tuple<const YAML::Node*, const char*, std::string*, uint16_t*>{
            &*rest, "trading_rest", &account.trading_rest_host,
            &account.trading_rest_port},
        {&*stream, "private_ws", &account.private_ws_host,
         &account.private_ws_port}}) {
    if (auto status = CheckKeys(*endpoint, {"host", "port", "tls"});
        !status.ok())
      return status;
    auto parsed_host = Scalar(*endpoint, "host");
    auto parsed_port = Unsigned(*endpoint, "port");
    if (!parsed_host.ok()) return parsed_host.status();
    if (!parsed_port.ok() || *parsed_port == 0 || *parsed_port > 65535) {
      return Invalid(std::string("invalid port in ") + label);
    }
    *host = *parsed_host;
    *port = static_cast<uint16_t>(*parsed_port);
    if ((*endpoint)["tls"]) {
      auto tls = Scalar(*endpoint, "tls");
      if (!tls.ok() || (*tls != "true" && *tls != "false")) {
        return Invalid(std::string("invalid tls in ") + label);
      }
      if (account.mode == TradingMode::PressureSim && *tls != "false") {
        return Invalid("pressure_sim endpoints must disable TLS");
      }
      if (account.mode == TradingMode::OnlineSim && *tls != "true") {
        return Invalid("online_sim Demo endpoints require TLS");
      }
    }
  }
  if (account.mode == TradingMode::PressureSim) account.tls = false;
  if (node["api_key_env"]) {
    auto value = Scalar(node, "api_key_env");
    if (!value.ok()) return value.status();
    account.api_key_env = *value;
  }
  if (node["api_secret_env"]) {
    auto value = Scalar(node, "api_secret_env");
    if (!value.ok()) return value.status();
    account.api_secret_env = *value;
  }
  if (node["api_credentials_file"]) {
    auto value = Scalar(node, "api_credentials_file");
    if (!value.ok()) return value.status();
    account.api_credentials_file = *value;
  }
  if (node["max_submissions_per_run"]) {
    auto value = Unsigned(node, "max_submissions_per_run");
    if (!value.ok()) return value.status();
    account.max_submissions_per_run = *value;
  }
  return account;
}

absl::StatusOr<InputConfig> ParseInput(const YAML::Node& node) {
  if (auto status = CheckKeys(
          node, {"source", "replay_file", "rest_host", "websocket_host",
                 "rest_port", "websocket_port", "tls", "depth_snapshot_limit",
                 "max_snapshot_requests_per_run"});
      !status.ok())
    return status;
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
    if (!value.ok() || *value == 0 || *value > 65535)
      return Invalid("invalid rest_port");
    input.rest_port = static_cast<uint16_t>(*value);
  }
  if (node["websocket_port"]) {
    auto value = Unsigned(node, "websocket_port");
    if (!value.ok() || *value == 0 || *value > 65535)
      return Invalid("invalid websocket_port");
    input.websocket_port = static_cast<uint16_t>(*value);
  }
  if (node["tls"]) {
    auto value = Scalar(node, "tls");
    if (!value.ok() || (*value != "true" && *value != "false")) {
      return Invalid("input.tls must be true or false");
    }
    input.tls = *value == "true";
  }
  if (node["depth_snapshot_limit"]) {
    auto value = Unsigned(node, "depth_snapshot_limit");
    if (!value.ok() || *value == 0 || *value > 5000) {
      return Invalid("depth_snapshot_limit out of range");
    }
    input.depth_snapshot_limit = *value;
  }
  if (node["max_snapshot_requests_per_run"]) {
    auto value = Unsigned(node, "max_snapshot_requests_per_run");
    if (!value.ok()) return value.status();
    input.max_snapshot_requests_per_run = *value;
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
  if (config.schema_version != 2) {
    return Invalid("unsupported schema_version");
  }
  if (config.sqlite_path.empty() || config.control_socket.empty()) {
    return Invalid("state_dir must be nonempty");
  }
  if (config.input.source == InputSource::Replay &&
      config.input.replay_file.empty()) {
    return Invalid("replay_file required");
  }
  if (config.input.source == InputSource::BinancePublic &&
      (config.input.rest_host.empty() || config.input.websocket_host.empty())) {
    return Invalid("Binance hosts required");
  }
  if (config.shards.empty() || config.shards.size() > kMaxShards) {
    return Invalid("shards must contain one to 1000 entries");
  }
  std::set<uint16_t> ids;
  std::set<std::pair<std::string, std::string>> account_symbols;
  std::set<std::string> accounts;
  bool demo = false;
  {
    if (config.accounts.empty()) return Invalid("accounts must be nonempty");
    for (const AccountConfig& account : config.accounts) {
      if (account.id.empty() || !accounts.insert(account.id).second ||
          account.maker_fee_rate_ppb >= kFixedScale) {
        return Invalid("invalid or duplicate account configuration");
      }
      if (account.mode == TradingMode::OnlineSim) {
        demo = true;
        if (account.trading_rest_host != "demo-api.binance.com" ||
            account.private_ws_host != "demo-ws-api.binance.com" ||
            account.trading_rest_port != 443 ||
            account.private_ws_port != 443 || !account.tls ||
            account.api_credentials_file.empty() ||
            !account.api_key_env.empty() || !account.api_secret_env.empty() ||
            account.max_submissions_per_run > 1 ||
            account.rest_connections != 1 ||
            account.request_weight_per_minute == 0 ||
            account.request_weight_per_minute > 200 ||
            account.order_placements_per_ten_seconds != 1) {
          return Invalid(
              "online_sim requires fixed Binance Spot Demo TLS endpoints, "
              "credentials file, one REST connection, at most one "
              "submission, and low request limits");
        }
      } else if (!account.api_credentials_file.empty() ||
                 account.max_submissions_per_run != 0) {
        return Invalid(
            "Demo credentials and submission cap require online_sim mode");
      }
      if (account.mode == TradingMode::PressureSim &&
          (account.trading_rest_host != "127.0.0.1" ||
           account.private_ws_host != "127.0.0.1" || account.tls ||
           account.trading_rest_port == 0 || account.private_ws_port == 0)) {
        return Invalid("pressure_sim must use loopback trading endpoints");
      }
    }
  }
  if (demo &&
      (config.accounts.size() != 1 || config.shards.size() != 1 ||
       config.input.source != InputSource::BinancePublic ||
       config.input.rest_host != "demo-api.binance.com" ||
       config.input.websocket_host != "demo-stream.binance.com" ||
       config.input.rest_port != 443 || config.input.websocket_port != 443 ||
       !config.input.tls || config.input.depth_snapshot_limit != 100 ||
       config.input.max_snapshot_requests_per_run == 0 ||
       config.input.max_snapshot_requests_per_run > 3)) {
    return Invalid(
        "demo validation requires one account, one shard, and "
        "fixed Demo market endpoints");
  }
  for (const ShardConfig& shard : config.shards) {
    if (shard.id.value >= kMaxShards || !ids.insert(shard.id.value).second) {
      return Invalid("invalid or duplicate shard id");
    }
    const MarketConfig& market = shard.market;
    if (std::any_of(config.accounts.begin(), config.accounts.end(),
                    [&](const AccountConfig& account) {
                      return account.id == shard.account_id &&
                             (account.mode == TradingMode::PressureSim ||
                              account.mode == TradingMode::OnlineSim);
                    }) &&
        !std::all_of(
            market.id.symbol.begin(), market.id.symbol.end(), [](char ch) {
              return (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9');
            })) {
      return Invalid("trading symbol must use Binance format");
    }
    if (config.input.source == InputSource::BinancePublic &&
        !std::all_of(
            market.id.symbol.begin(), market.id.symbol.end(), [](char ch) {
              return (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9');
            })) {
      return Invalid(
          "Binance symbol must contain uppercase letters and digits");
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
    if (auto status = ValidateStrategyConfig(strategy); !status.ok())
      return status;
    if (shard.account_id.empty() || !accounts.contains(shard.account_id)) {
      return Invalid("shard references missing account");
    }
    if (!account_symbols.emplace(shard.account_id, market.id.symbol).second) {
      return Invalid("duplicate trading symbol within account");
    }
    const auto account =
        std::find_if(config.accounts.begin(), config.accounts.end(),
                     [&](const AccountConfig& value) {
                       return value.id == shard.account_id;
                     });
    for (const std::string& asset : {market.base_asset, market.quote_asset}) {
      const bool present = account != config.accounts.end() &&
                           account->initial_balances_nanos.contains(asset) &&
                           account->hard_limits_nanos.contains(asset);
      if (!present) {
        return Invalid("missing balance or hard limit for market asset");
      }
    }
  }
  if (auto allocations = ResolveShardAllocations(config); !allocations.ok()) {
    return allocations.status();
  }
  return absl::OkStatus();
}

absl::StatusOr<std::map<uint16_t, ShardAllocation>> ResolveShardAllocations(
    const AppConfig& config) {
  std::map<uint16_t, ShardAllocation> result;
  for (const AccountConfig& account : config.accounts) {
    std::vector<const ShardConfig*> members;
    for (const ShardConfig& shard : config.shards) {
      if (shard.account_id == account.id) {
        members.push_back(&shard);
        result.try_emplace(shard.id.value);
      }
    }
    if (members.empty()) continue;

    for (const auto& [asset, initial] : account.initial_balances_nanos) {
      const auto hard = account.hard_limits_nanos.find(asset);
      const uint64_t limit =
          hard == account.hard_limits_nanos.end()
              ? initial
              : std::min(initial, hard->second);
      bool market_asset = false;
      for (const ShardConfig* shard : members) {
        market_asset |= shard->market.base_asset == asset ||
                        shard->market.quote_asset == asset;
      }
      std::vector<const ShardConfig*> automatic;
      uint64_t explicit_total = 0;
      for (const ShardConfig* shard : members) {
        const auto configured = shard->asset_limits_nanos.find(asset);
        if (configured != shard->asset_limits_nanos.end()) {
          if (configured->second > limit - explicit_total) {
            return Invalid("shard asset allocations exceed account limit: " +
                           asset);
          }
          explicit_total += configured->second;
          result.at(shard->id.value).assets_nanos[asset] = configured->second;
        } else if (!market_asset || shard->market.base_asset == asset ||
                   shard->market.quote_asset == asset) {
          automatic.push_back(shard);
        }
      }
      const uint64_t remaining = limit - explicit_total;
      for (size_t i = 0; i < automatic.size(); ++i) {
        result.at(automatic[i]->id.value).assets_nanos[asset] =
            remaining / automatic.size() +
            (i < remaining % automatic.size() ? 1 : 0);
      }
    }
    for (const ShardConfig* shard : members) {
      for (const auto& [asset, amount] : shard->asset_limits_nanos) {
        (void)amount;
        if (!account.initial_balances_nanos.contains(asset)) {
          return Invalid("shard allocation uses unconfigured asset: " + asset);
        }
      }
    }

    const auto distribute = [&](const std::vector<const ShardConfig*>& group,
                                uint64_t global_limit, bool active)
        -> absl::Status {
      uint64_t explicit_total = 0;
      std::vector<const ShardConfig*> automatic;
      for (const ShardConfig* shard : group) {
        const auto configured = active ? shard->max_active_orders
                                       : shard->max_open_value_per_quote_nanos;
        if (configured) {
          if (global_limit != 0 && *configured > global_limit - explicit_total)
            return Invalid(active ? "shard active order limits exceed account"
                                  : "shard quote limits exceed account");
          if (*configured > std::numeric_limits<uint64_t>::max() -
                                explicit_total)
            return Invalid("shard risk allocation overflow");
          explicit_total += *configured;
          (active ? result.at(shard->id.value).max_active_orders
                  : result.at(shard->id.value)
                        .max_open_value_per_quote_nanos) = *configured;
        } else {
          automatic.push_back(shard);
        }
      }
      if (global_limit == 0) return absl::OkStatus();
      const uint64_t remaining = global_limit - explicit_total;
      for (size_t i = 0; i < automatic.size(); ++i) {
        (active ? result.at(automatic[i]->id.value).max_active_orders
                : result.at(automatic[i]->id.value)
                      .max_open_value_per_quote_nanos) =
            remaining / automatic.size() +
            (i < remaining % automatic.size() ? 1 : 0);
      }
      return absl::OkStatus();
    };
    if (auto status = distribute(members, account.risk.max_active_orders, true);
        !status.ok())
      return status;
    std::map<std::string, std::vector<const ShardConfig*>> quote_groups;
    for (const ShardConfig* shard : members) {
      quote_groups[shard->market.quote_asset].push_back(shard);
    }
    for (const auto& [quote, group] : quote_groups) {
      (void)quote;
      if (auto status = distribute(
              group, account.risk.max_open_value_per_quote_nanos, false);
          !status.ok())
        return status;
    }
  }
  return result;
}

absl::StatusOr<AppConfig> ParseConfig(std::string_view yaml_text,
                                      std::string_view state_dir) {
  if (state_dir.empty()) return Invalid("state_dir must be nonempty");
  try {
    const YAML::Node root = YAML::Load(std::string(yaml_text));
    auto version = Unsigned(root, "schema_version");
    if (!version.ok() || *version != 2) {
      return Invalid("unsupported schema_version");
    }
    if (auto status =
            CheckKeys(root, {"schema_version", "input", "accounts", "shards"});
        !status.ok())
      return status;
    auto input_node = MapField(root, "input");
    if (!input_node.ok()) return input_node.status();
    auto input = ParseInput(*input_node);
    if (!input.ok()) return input.status();
    const YAML::Node shards_node = root["shards"];
    if (!shards_node || !shards_node.IsSequence())
      return Invalid("shards must be a sequence");
    AppConfig config;
    config.schema_version = *version;
    config.input = std::move(*input);
    const std::filesystem::path state_path{std::string(state_dir)};
    config.sqlite_path = (state_path / "history.sqlite").string();
    config.control_socket = (state_path / "control.sock").string();
    const YAML::Node account_nodes = root["accounts"];
    if (!account_nodes || !account_nodes.IsSequence()) {
      return Invalid("accounts must be a sequence");
    }
    for (const YAML::Node& node : account_nodes) {
      auto account = ParseAccount(node);
      if (!account.ok()) return account.status();
      config.accounts.push_back(std::move(*account));
    }
    for (const YAML::Node& node : shards_node) {
      if (auto status =
              CheckKeys(node, {"id", "market", "strategy", "account_id",
                               "allocation"});
          !status.ok())
        return status;
      auto id = Unsigned(node, "id");
      auto market_node = MapField(node, "market");
      auto strategy_node = MapField(node, "strategy");
      if (!id.ok() || *id >= kMaxShards) {
        return Invalid("shard id must be in [0,999]");
      }
      if (!market_node.ok()) return market_node.status();
      if (!strategy_node.ok()) return strategy_node.status();
      auto market = ParseMarket(*market_node);
      auto strategy = ParseStrategy(*strategy_node);
      if (!market.ok()) return market.status();
      if (!strategy.ok()) return strategy.status();
      ShardConfig shard;
      shard.id = ShardId{static_cast<uint16_t>(*id)};
      shard.market = std::move(*market);
      shard.strategy = std::move(*strategy);
      auto account_id = Scalar(node, "account_id");
      if (!account_id.ok()) return account_id.status();
      shard.account_id = *account_id;
      if (node["allocation"]) {
        auto allocation = MapField(node, "allocation");
        if (!allocation.ok()) return allocation.status();
        if (auto status = CheckKeys(
                *allocation,
                {"assets", "max_active_orders", "max_open_value_per_quote"});
            !status.ok())
          return status;
        if ((*allocation)["assets"]) {
          auto assets = FixedMap(*allocation, "assets");
          if (!assets.ok()) return assets.status();
          shard.asset_limits_nanos = std::move(*assets);
        }
        if ((*allocation)["max_active_orders"]) {
          auto value = Unsigned(*allocation, "max_active_orders");
          if (!value.ok()) return value.status();
          shard.max_active_orders = *value;
        }
        if ((*allocation)["max_open_value_per_quote"]) {
          auto value = Fixed(*allocation, "max_open_value_per_quote");
          if (!value.ok()) return value.status();
          shard.max_open_value_per_quote_nanos = *value;
        }
      }
      config.shards.push_back(std::move(shard));
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
  if (!file.eof() && file.fail())
    return absl::DataLossError("cannot read config: " + path);
  auto config = ParseConfig(text, state_dir);
  if (!config.ok()) return config.status();
  for (AccountConfig& account : config->accounts) {
    if (account.api_credentials_file.empty()) continue;
    std::filesystem::path credentials(account.api_credentials_file);
    if (credentials.is_relative()) {
      credentials = std::filesystem::path(path).parent_path() / credentials;
    }
    account.api_credentials_file = credentials.lexically_normal().string();
  }
  return config;
}

absl::StatusOr<ApiCredentials> LoadApiCredentialsFile(const std::string& path) {
  std::error_code error;
  const auto status = std::filesystem::symlink_status(path, error);
  if (error || !std::filesystem::is_regular_file(status) ||
      std::filesystem::is_symlink(status)) {
    return Invalid("API credentials file must be a regular file");
  }
  constexpr auto forbidden =
      std::filesystem::perms::group_all | std::filesystem::perms::others_all;
  if ((status.permissions() & forbidden) != std::filesystem::perms::none) {
    return Invalid("API credentials file must have owner-only permissions");
  }
  std::ifstream file(path);
  if (!file) return Invalid("cannot open API credentials file");
  const std::string text((std::istreambuf_iterator<char>(file)),
                         std::istreambuf_iterator<char>());
  if (text.size() > 4096 || (!file.eof() && file.fail())) {
    return Invalid("invalid API credentials file");
  }
  try {
    const YAML::Node root = YAML::Load(text);
    if (auto checked = CheckKeys(root, {"api_key", "api_secret"});
        !checked.ok())
      return checked;
    auto key = Scalar(root, "api_key");
    auto secret = Scalar(root, "api_secret");
    if (!key.ok() || !secret.ok() || *key == "REPLACE_ME" ||
        *secret == "REPLACE_ME") {
      return Invalid("API credentials file is incomplete");
    }
    return ApiCredentials{std::move(*key), std::move(*secret)};
  } catch (const YAML::Exception&) {
    return Invalid("invalid API credentials YAML");
  }
}

absl::Status ApplyStrategyOverrides(AppConfig& config,
                                    const StrategyOverrides& overrides) {
  const bool any = overrides.order_amount_nanos || overrides.bid_spread_ppb ||
                   overrides.ask_spread_ppb || overrides.refresh_ms;
  if (any && !overrides.shard)
    return Invalid("strategy_shard required for overrides");
  if (!overrides.shard) return absl::OkStatus();
  for (ShardConfig& shard : config.shards) {
    if (shard.id != *overrides.shard) continue;
    AppConfig candidate = config;
    for (ShardConfig& target : candidate.shards) {
      if (target.id != *overrides.shard) continue;
      if (overrides.order_amount_nanos)
        target.strategy.order_amount_nanos = *overrides.order_amount_nanos;
      if (overrides.bid_spread_ppb)
        target.strategy.bid_spread_ppb = *overrides.bid_spread_ppb;
      if (overrides.ask_spread_ppb)
        target.strategy.ask_spread_ppb = *overrides.ask_spread_ppb;
      if (overrides.refresh_ms)
        target.strategy.refresh_ms = *overrides.refresh_ms;
    }
    if (auto status = ValidateConfig(candidate); !status.ok()) return status;
    config = std::move(candidate);
    return absl::OkStatus();
  }
  return Invalid("strategy_shard not found");
}

}  // namespace hquant::v1
