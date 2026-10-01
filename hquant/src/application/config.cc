#include "application/config.h"

#include <algorithm>
#include <charconv>
#include <cstdint>
#include <fstream>
#include <iterator>
#include <limits>
#include <map>
#include <set>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>

#include "absl/status/status.h"
#include "base/error.h"
#include "yaml-cpp/yaml.h"

namespace hquant {
namespace {

absl::StatusOr<std::string> Scalar(const YAML::Node& node, const char* field) {
  const auto value = node[field];
  if (!value || !value.IsScalar() || value.Scalar().empty()) {
    return Error(ErrorCode::kConfigFieldInvalid,
                 std::string("missing scalar: ") + field);
  }
  return value.Scalar();
}

absl::StatusOr<uint64_t> Unsigned(const YAML::Node& node, const char* field) {
  auto value = Scalar(node, field);
  if (!value.ok()) return value.status();
  uint64_t number = 0;
  const char* first = value->data();
  const char* last = first + value->size();
  auto [end, error] = std::from_chars(first, last, number);
  if (error != std::errc{} || end != last) {
    return Error(ErrorCode::kConfigFieldInvalid,
                 std::string("invalid integer: ") + field);
  }
  return number;
}

absl::StatusOr<Decimal> Number(const YAML::Node& node, const char* field,
                               bool positive) {
  auto value = Scalar(node, field);
  if (!value.ok()) return value.status();
  auto number = Decimal::Parse(*value);
  if (!number.ok())
    return Error(ErrorCode::kConfigFieldInvalid,
                 std::string("invalid decimal: ") + field);
  if (positive ? !number->IsStrictlyPositive() : !number->IsNonnegative()) {
    return Error(ErrorCode::kConfigFieldInvalid,
                 std::string("decimal out of range: ") + field);
  }
  return *number;
}

absl::StatusOr<YAML::Node> Sequence(const YAML::Node& node, const char* field) {
  const auto value = node[field];
  if (!value || !value.IsSequence()) {
    return Error(ErrorCode::kConfigFieldInvalid,
                 std::string("missing sequence: ") + field);
  }
  return value;
}

absl::StatusOr<YAML::Node> Mapping(const YAML::Node& node, const char* field) {
  const auto value = node[field];
  if (!value || !value.IsMap()) {
    return Error(ErrorCode::kConfigFieldInvalid,
                 std::string("missing map: ") + field);
  }
  return value;
}

absl::StatusOr<ShardId> Shard(const YAML::Node& node, const char* field) {
  auto value = Unsigned(node, field);
  if (!value.ok()) return value.status();
  if (*value >= 8)
    return Error(ErrorCode::kConfigFieldInvalid, "shard must be in [0,7]");
  return ShardId{static_cast<uint8_t>(*value)};
}

absl::StatusOr<std::chrono::microseconds> RefreshInterval(
    const YAML::Node& node) {
  uint64_t us = 0;
  if (node["refresh_interval_us"]) {
    auto number = Unsigned(node, "refresh_interval_us");
    if (!number.ok()) return number.status();
    us = *number;
  } else {
    auto text = Scalar(node, "refresh_interval");
    if (!text.ok()) return text.status();
    std::string_view value(*text);
    uint64_t multiplier = 0;
    if (value.ends_with("ms")) {
      value.remove_suffix(2);
      multiplier = 1'000;
    } else if (value.ends_with('s')) {
      value.remove_suffix(1);
      multiplier = 1'000'000;
    } else {
      return Error(ErrorCode::kConfigFieldInvalid,
                   "refresh_interval must use s or ms");
    }
    uint64_t count = 0;
    auto [end, error] =
        std::from_chars(value.data(), value.data() + value.size(), count);
    if (error != std::errc{} || end != value.data() + value.size() ||
        count > static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) /
                    multiplier) {
      return Error(ErrorCode::kConfigFieldInvalid, "invalid refresh_interval");
    }
    us = count * multiplier;
  }
  if (us == 0 ||
      us > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
    return Error(ErrorCode::kConfigFieldInvalid,
                 "refresh interval out of range");
  }
  return std::chrono::microseconds(static_cast<int64_t>(us));
}

absl::StatusOr<std::chrono::microseconds> Duration(const YAML::Node& node,
                                                   const char* field) {
  auto scalar = Scalar(node, field);
  if (!scalar.ok()) return scalar.status();
  std::string_view value(*scalar);
  uint64_t multiplier = 0;
  if (value.ends_with("us")) {
    value.remove_suffix(2);
    multiplier = 1;
  } else if (value.ends_with("ms")) {
    value.remove_suffix(2);
    multiplier = 1'000;
  } else if (value.ends_with('s')) {
    value.remove_suffix(1);
    multiplier = 1'000'000;
  } else if (value.ends_with('m')) {
    value.remove_suffix(1);
    multiplier = 60'000'000;
  } else if (value.ends_with('h')) {
    value.remove_suffix(1);
    multiplier = 3'600'000'000;
  } else {
    return Error(ErrorCode::kConfigFieldInvalid,
                 std::string("invalid duration unit: ") + field);
  }
  uint64_t count = 0;
  auto [end, error] =
      std::from_chars(value.data(), value.data() + value.size(), count);
  if (error != std::errc{} || end != value.data() + value.size() ||
      count == 0 ||
      count > static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) /
                  multiplier) {
    return Error(ErrorCode::kConfigFieldInvalid,
                 std::string("invalid duration: ") + field);
  }
  return std::chrono::microseconds(static_cast<int64_t>(count * multiplier));
}

absl::StatusOr<size_t> PositiveSize(const YAML::Node& node, const char* field) {
  auto value = Unsigned(node, field);
  if (!value.ok()) return value.status();
  if (*value == 0 || *value > std::numeric_limits<uint32_t>::max())
    return Error(ErrorCode::kConfigFieldInvalid,
                 std::string("invalid queue size: ") + field);
  return static_cast<size_t>(*value);
}

absl::Status CheckRate(const Decimal& rate, const char* field) {
  auto one = Decimal::Parse("1");
  auto compare = rate.Compare(*one);
  if (!compare.ok()) return compare.status();
  if (*compare >= 0)
    return Error(ErrorCode::kConfigFieldInvalid,
                 std::string("rate must be below one: ") + field);
  return absl::OkStatus();
}

absl::Status ParseExchangeEndpoint(const YAML::Node& node,
                                   ExchangeEndpoint* endpoint) {
  if (!node.IsMap())
    return Error(ErrorCode::kConfigFieldInvalid,
                 "exchange endpoint must be a map");
  auto host = Scalar(node, "host");
  auto port = Unsigned(node, "port");
  if (!host.ok()) return host.status();
  if (!port.ok() || *port == 0 || *port > 65535)
    return Error(ErrorCode::kConfigFieldInvalid, "exchange port out of range");
  if (host->find('/') != std::string::npos ||
      host->find(':') != std::string::npos)
    return Error(ErrorCode::kConfigFieldInvalid, "invalid exchange host");
  auto tls = Scalar(node, "tls");
  if (!tls.ok()) return tls.status();
  if (*tls != "true" && *tls != "false")
    return Error(ErrorCode::kConfigFieldInvalid,
                 "exchange tls must be boolean");
  *endpoint =
      ExchangeEndpoint{*host, static_cast<uint16_t>(*port), *tls == "true"};
  return absl::OkStatus();
}

absl::StatusOr<MarketId> NamedMarket(
    const YAML::Node& node, const std::map<std::string, MarketId>& markets) {
  if (!node.IsScalar())
    return Error(ErrorCode::kConfigFieldInvalid, "market name must be scalar");
  const auto found = markets.find(node.Scalar());
  if (found == markets.end())
    return Error(ErrorCode::kConfigReferenceInvalid,
                 "unknown market dependency");
  return found->second;
}

absl::StatusOr<std::vector<MarketId>> MarketList(
    const YAML::Node& node, const char* field,
    const std::map<std::string, MarketId>& markets) {
  auto items = Sequence(node, field);
  if (!items.ok()) return items.status();
  std::vector<MarketId> result;
  std::set<std::string> unique;
  for (const auto& item : *items) {
    auto market = NamedMarket(item, markets);
    if (!market.ok()) return market.status();
    if (!unique.insert(market->native_symbol).second) {
      return Error(ErrorCode::kConfigReferenceInvalid,
                   "duplicate market dependency");
    }
    result.push_back(std::move(*market));
  }
  return result;
}

absl::Status CheckBudgetTotals(const AppConfig& config) {
  std::map<std::pair<std::string, std::string>, Decimal> totals;
  for (const auto& budget : config.risk_budgets) {
    const auto key = std::pair{budget.account.value, budget.asset.value};
    const auto found = totals.find(key);
    if (found == totals.end()) {
      totals.emplace(key, budget.hard_limit);
    } else {
      auto sum = found->second.Add(budget.hard_limit);
      if (!sum.ok()) return sum.status();
      found->second = *sum;
    }
  }
  for (const auto& [key, granted] : totals) {
    const auto account = std::find_if(
        config.accounts.begin(), config.accounts.end(),
        [&](const auto& item) { return item.account.value == key.first; });
    if (account == config.accounts.end())
      return Error(ErrorCode::kConfigBudgetInvalid, "budget account unknown");
    const auto balance = account->initial_balances.find(key.second);
    if (balance == account->initial_balances.end()) {
      return Error(ErrorCode::kConfigBudgetInvalid,
                   "budget asset has no conservative balance");
    }
    auto compare = granted.Compare(balance->second);
    if (!compare.ok()) return compare.status();
    if (*compare > 0)
      return Error(ErrorCode::kConfigBudgetInvalid,
                   "static risk budgets exceed account balance");
  }
  return absl::OkStatus();
}

}  // namespace

absl::StatusOr<AppConfig> ParseConfig(std::string_view yaml_text) {
  try {
    const YAML::Node root = YAML::Load(std::string(yaml_text));
    if (!root.IsMap())
      return Error(ErrorCode::kConfigSyntaxInvalid,
                   "config root must be a map");
    AppConfig config;
    auto version = Unsigned(root, "schema_version");
    if (!version.ok()) return version.status();
    if (*version != 1)
      return Error(ErrorCode::kConfigSchemaUnsupported,
                   "unsupported config schema_version");
    auto mode = Scalar(root, "mode");
    if (!mode.ok()) return mode.status();
    if (*mode == "live")
      return Error(ErrorCode::kConfigModeNotAllowed,
                   "live mode is unavailable before G4");
    if (*mode != "simulated")
      return Error(ErrorCode::kConfigModeNotAllowed,
                   "mode must explicitly be simulated");
    config.mode = EngineMode::Simulated;
    auto loop_mode = Scalar(root, "loop_mode");
    if (!loop_mode.ok()) return loop_mode.status();
    if (*loop_mode == "blocking")
      config.loop_mode = LoopMode::Blocking;
    else if (*loop_mode == "busy")
      config.loop_mode = LoopMode::Busy;
    else
      return Error(ErrorCode::kConfigFieldInvalid, "unknown loop_mode");
    auto storage_path = Scalar(root, "storage_path");
    if (!storage_path.ok()) return storage_path.status();
    config.storage_path = *storage_path;
    if (root["storage"]) {
      auto storage = Mapping(root, "storage");
      if (!storage.ok()) return storage.status();
      if ((*storage)["writer_queue"]) {
        auto value = PositiveSize(*storage, "writer_queue");
        if (!value.ok()) return value.status();
        config.storage.writer_queue = *value;
      }
      if ((*storage)["writer_batch"]) {
        auto value = PositiveSize(*storage, "writer_batch");
        if (!value.ok()) return value.status();
        config.storage.writer_batch = *value;
      }
      if ((*storage)["reader_queue"]) {
        auto value = PositiveSize(*storage, "reader_queue");
        if (!value.ok()) return value.status();
        config.storage.reader_queue = *value;
      }
      if ((*storage)["reader_page_limit"]) {
        auto value = PositiveSize(*storage, "reader_page_limit");
        if (!value.ok()) return value.status();
        config.storage.reader_page_limit = static_cast<uint32_t>(*value);
      }
      if (config.storage.writer_batch > config.storage.writer_queue)
        return Error(ErrorCode::kConfigFieldInvalid,
                     "writer_batch exceeds writer_queue");
    }
    if (root["simulated_exchange"]) {
      auto exchange = Mapping(root, "simulated_exchange");
      if (!exchange.ok()) return exchange.status();
      if ((*exchange)["maker_fee_rate"]) {
        auto rate = Number(*exchange, "maker_fee_rate", false);
        if (!rate.ok()) return rate.status();
        auto valid = CheckRate(*rate, "maker_fee_rate");
        if (!valid.ok()) return valid;
        config.simulated_exchange.maker_fee_rate = *rate;
      }
    }
    if (root["risk"]) {
      auto risk = Mapping(root, "risk");
      if (!risk.ok()) return risk.status();
      if ((*risk)["fee_buffer_rate"]) {
        auto rate = Number(*risk, "fee_buffer_rate", false);
        if (!rate.ok()) return rate.status();
        auto valid = CheckRate(*rate, "fee_buffer_rate");
        if (!valid.ok()) return valid;
        config.risk.fee_buffer_rate = *rate;
      }
      if ((*risk)["max_rule_age"]) {
        auto age = Duration(*risk, "max_rule_age");
        if (!age.ok()) return age.status();
        if (age->count() < 1'000'000 || age->count() % 1'000'000 != 0)
          return Error(ErrorCode::kConfigFieldInvalid,
                       "max_rule_age must be whole seconds");
        config.risk.max_rule_age = *age;
      }
    }
    if (root["market_data_source"]) {
      auto source = Scalar(root, "market_data_source");
      if (!source.ok()) return source.status();
      if (*source == "replay")
        config.market_data_source = MarketDataSource::Replay;
      else if (*source == "binance_public") {
        config.market_data_source = MarketDataSource::BinancePublic;
      } else
        return Error(ErrorCode::kConfigFieldInvalid,
                     "unsupported market_data_source");
    }
    if (root["exchange_endpoints"]) {
      auto endpoints = Mapping(root, "exchange_endpoints");
      if (!endpoints.ok()) return endpoints.status();
      auto binance = Mapping(*endpoints, "binance");
      if (!binance.ok()) return binance.status();
      if ((*binance)["rest"]) {
        auto status = ParseExchangeEndpoint((*binance)["rest"],
                                            &config.binance_endpoints.rest);
        if (!status.ok()) return status;
      }
      if ((*binance)["websocket"]) {
        auto status = ParseExchangeEndpoint(
            (*binance)["websocket"], &config.binance_endpoints.websocket);
        if (!status.ok()) return status;
      }
    }
    if (config.market_data_source == MarketDataSource::Replay) {
      auto replay_fixture = Scalar(root, "replay_fixture");
      if (!replay_fixture.ok()) {
        return Error(ErrorCode::kConfigFieldInvalid,
                     "simulated replay requires replay_fixture");
      }
      config.replay_fixture = *replay_fixture;
    } else if (root["replay_fixture"]) {
      return Error(ErrorCode::kConfigFieldInvalid,
                   "public market data cannot also use replay_fixture");
    }

    std::set<std::string> accounts_by_name;
    auto accounts = Sequence(root, "accounts");
    if (!accounts.ok()) return accounts.status();
    for (const auto& item : *accounts) {
      if (!item.IsMap())
        return Error(ErrorCode::kConfigFieldInvalid, "account must be a map");
      if (item["api_key"] || item["secret_key"] || item["private_key"]) {
        return Error(ErrorCode::kConfigContainsSecret,
                     "inline credentials are not allowed");
      }
      auto name = Scalar(item, "account");
      if (!name.ok()) return name.status();
      if (!accounts_by_name.insert(*name).second)
        return Error(ErrorCode::kConfigReferenceInvalid, "duplicate account");
      auto balances = Mapping(item, "initial_balances");
      if (!balances.ok()) return balances.status();
      AccountConfig account;
      account.account = AccountId(*name);
      for (const auto& entry : *balances) {
        if (!entry.first.IsScalar() || !entry.second.IsScalar()) {
          return Error(ErrorCode::kConfigFieldInvalid,
                       "balance must be asset to decimal text");
        }
        auto balance = Decimal::Parse(entry.second.Scalar());
        if (!balance.ok() || !balance->IsNonnegative()) {
          return Error(ErrorCode::kConfigFieldInvalid,
                       "invalid initial balance");
        }
        account.initial_balances.emplace(entry.first.Scalar(), *balance);
      }
      config.accounts.push_back(std::move(account));
    }
    if (config.accounts.empty())
      return Error(ErrorCode::kConfigReferenceInvalid,
                   "at least one account required");

    std::map<std::string, MarketId> markets_by_name;
    auto market_specs = Sequence(root, "market_specs");
    if (!market_specs.ok()) return market_specs.status();
    for (const auto& item : *market_specs) {
      if (!item.IsMap())
        return Error(ErrorCode::kConfigFieldInvalid,
                     "market spec must be a map");
      auto name = Scalar(item, "market");
      auto exchange = Scalar(item, "exchange");
      auto base = Scalar(item, "base_asset");
      auto quote = Scalar(item, "quote_asset");
      if (!name.ok()) return name.status();
      if (!exchange.ok()) return exchange.status();
      if (!base.ok()) return base.status();
      if (!quote.ok()) return quote.status();
      if (markets_by_name.contains(*name))
        return Error(ErrorCode::kConfigReferenceInvalid, "duplicate market");
      MarketConfig market;
      market.spec.market =
          MarketId{ExchangeId(*exchange), InstrumentKind::Spot, *name};
      market.spec.base_asset = AssetId(*base);
      market.spec.quote_asset = AssetId(*quote);
      const YAML::Node scale =
          item["tick_lot_size"] ? item["tick_lot_size"] : item;
      auto tick = Number(scale, "price_per_tick", true);
      auto lot = Number(scale, "amount_per_lot", true);
      auto tick_lot_version = Unsigned(scale, "tick_lot_version");
      if (!tick.ok()) return tick.status();
      if (!lot.ok()) return lot.status();
      if (!tick_lot_version.ok()) return tick_lot_version.status();
      if (*tick_lot_version == 0)
        return Error(ErrorCode::kConfigFieldInvalid,
                     "tick_lot_version must be positive");
      market.tick_lot_size = TickLotSize{*tick, *lot, *tick_lot_version};
      const YAML::Node rule =
          item["trading_rule"] ? item["trading_rule"] : item;
      auto price_increment = Number(rule, "price_increment", true);
      auto base_increment = Number(rule, "base_increment", true);
      auto min_base = Number(rule, "min_base_amount", false);
      auto min_notional = Number(rule, "min_notional", false);
      if (!price_increment.ok()) return price_increment.status();
      if (!base_increment.ok()) return base_increment.status();
      if (!min_base.ok()) return min_base.status();
      if (!min_notional.ok()) return min_notional.status();
      market.trading_rule.market = market.spec.market;
      market.trading_rule.price_increment = *price_increment;
      market.trading_rule.base_increment = *base_increment;
      market.trading_rule.min_base_amount = *min_base;
      market.trading_rule.min_notional = *min_notional;
      if (item["stale_after"]) {
        auto age = Duration(item, "stale_after");
        if (!age.ok()) return age.status();
        market.stale_after = *age;
      }
      markets_by_name.emplace(*name, market.spec.market);
      config.market_specs.push_back(std::move(market));
    }
    if (config.market_specs.empty())
      return Error(ErrorCode::kConfigReferenceInvalid,
                   "at least one market required");

    std::map<uint64_t, StrategyId> strategies_by_id;
    auto strategies = Sequence(root, "strategy_configs");
    if (!strategies.ok()) return strategies.status();
    for (const auto& item : *strategies) {
      if (!item.IsMap())
        return Error(ErrorCode::kConfigFieldInvalid, "strategy must be a map");
      auto key = Unsigned(item, "strategy_id");
      auto strategy = Scalar(item, "strategy");
      auto account = Scalar(item, "account");
      if (!key.ok()) return key.status();
      if (!strategy.ok()) return strategy.status();
      if (!account.ok()) return account.status();
      if (*key == 0 || *key >= (uint64_t{1} << 48) ||
          strategies_by_id.contains(*key)) {
        return Error(ErrorCode::kConfigReferenceInvalid,
                     "invalid or duplicate strategy_id");
      }
      if (*strategy != "simple_pmm" || !accounts_by_name.contains(*account)) {
        return Error(ErrorCode::kConfigReferenceInvalid,
                     "unsupported strategy or unknown account");
      }
      auto dependencies = MarketList(item, "markets", markets_by_name);
      if (!dependencies.ok()) return dependencies.status();
      if (dependencies->empty())
        return Error(ErrorCode::kConfigFieldInvalid, "strategy needs market");
      auto amount = Number(item, "order_amount", true);
      auto bid_spread = Number(item, "bid_spread", false);
      auto ask_spread = Number(item, "ask_spread", false);
      auto refresh = RefreshInterval(item);
      if (!amount.ok()) return amount.status();
      if (!bid_spread.ok()) return bid_spread.status();
      if (!ask_spread.ok()) return ask_spread.status();
      if (!refresh.ok()) return refresh.status();
      auto one = Decimal::Parse("1");
      auto bid_compare = bid_spread->Compare(*one);
      auto ask_compare = ask_spread->Compare(*one);
      if (!bid_compare.ok()) return bid_compare.status();
      if (!ask_compare.ok()) return ask_compare.status();
      if (*bid_compare >= 0 || *ask_compare >= 0) {
        return Error(ErrorCode::kConfigFieldInvalid,
                     "strategy spread must be below one");
      }
      StrategyConfig config_entry;
      config_entry.strategy_id = StrategyId{*key, StrategyName(*strategy)};
      config_entry.account = AccountId(*account);
      config_entry.markets = std::move(*dependencies);
      config_entry.order_amount = *amount;
      config_entry.bid_spread = *bid_spread;
      config_entry.ask_spread = *ask_spread;
      config_entry.refresh_interval = *refresh;
      if (item["maker_fee_rate"]) {
        auto rate = Number(item, "maker_fee_rate", false);
        if (!rate.ok()) return rate.status();
        auto valid = CheckRate(*rate, "maker_fee_rate");
        if (!valid.ok()) return valid;
        config_entry.maker_fee_rate = *rate;
      }
      if (item["price_type"]) {
        auto value = Scalar(item, "price_type");
        if (!value.ok()) return value.status();
        if (*value == "mid")
          config_entry.price_type = PmmPriceType::Mid;
        else if (*value == "last")
          config_entry.price_type = PmmPriceType::Last;
        else
          return Error(ErrorCode::kConfigFieldInvalid,
                       "price_type must be mid or last");
      }
      if (item["timer_period"]) {
        auto period = Duration(item, "timer_period");
        if (!period.ok()) return period.status();
        config_entry.timer_period = *period;
      }
      strategies_by_id.emplace(*key, config_entry.strategy_id);
      config.strategy_configs.push_back(std::move(config_entry));
    }
    if (config.strategy_configs.empty())
      return Error(ErrorCode::kConfigReferenceInvalid,
                   "at least one strategy required");

    std::map<std::string, uint8_t> market_shard;
    std::map<uint64_t, uint8_t> strategy_shard;
    std::set<uint8_t> shard_ids;
    auto assignments = Sequence(root, "assignments");
    if (!assignments.ok()) return assignments.status();
    for (const auto& item : *assignments) {
      if (!item.IsMap())
        return Error(ErrorCode::kConfigFieldInvalid,
                     "assignment must be a map");
      auto shard = Shard(item, "shard");
      if (!shard.ok()) return shard.status();
      if (!shard_ids.insert(shard->value).second)
        return Error(ErrorCode::kConfigAssignmentInvalid, "duplicate shard");
      ShardAssignment assignment;
      assignment.shard = *shard;
      auto markets = MarketList(item, "markets", markets_by_name);
      if (!markets.ok()) return markets.status();
      if (markets->empty())
        return Error(ErrorCode::kConfigAssignmentInvalid,
                     "empty shard markets");
      for (auto& market : *markets) {
        if (!market_shard.emplace(market.native_symbol, shard->value).second) {
          return Error(ErrorCode::kConfigAssignmentInvalid,
                       "market assigned twice");
        }
        assignment.markets.push_back(std::move(market));
      }
      auto strategy_ids = Sequence(item, "strategy_ids");
      if (!strategy_ids.ok()) return strategy_ids.status();
      for (const auto& strategy_node : *strategy_ids) {
        if (!strategy_node.IsScalar())
          return Error(ErrorCode::kConfigFieldInvalid,
                       "strategy_id key must be scalar");
        const std::string text = strategy_node.Scalar();
        uint64_t key = 0;
        auto [end, error] =
            std::from_chars(text.data(), text.data() + text.size(), key);
        if (error != std::errc{} || end != text.data() + text.size()) {
          return Error(ErrorCode::kConfigAssignmentInvalid,
                       "invalid assigned strategy_id key");
        }
        const auto strategy_id = strategies_by_id.find(key);
        if (strategy_id == strategies_by_id.end() ||
            !strategy_shard.emplace(key, shard->value).second) {
          return Error(ErrorCode::kConfigAssignmentInvalid,
                       "unknown or duplicate assigned strategy_id");
        }
        assignment.strategy_ids.push_back(strategy_id->second);
      }
      auto account_nodes = Sequence(item, "accounts");
      if (!account_nodes.ok()) return account_nodes.status();
      std::set<std::string> unique_accounts;
      for (const auto& account_node : *account_nodes) {
        if (!account_node.IsScalar() ||
            !accounts_by_name.contains(account_node.Scalar()) ||
            !unique_accounts.insert(account_node.Scalar()).second) {
          return Error(ErrorCode::kConfigAssignmentInvalid,
                       "unknown or duplicate assigned account");
        }
        assignment.accounts.emplace_back(account_node.Scalar());
      }
      config.assignments.push_back(std::move(assignment));
    }
    if (config.assignments.empty() || config.assignments.size() > 8 ||
        market_shard.size() != markets_by_name.size() ||
        strategy_shard.size() != strategies_by_id.size()) {
      return Error(ErrorCode::kConfigAssignmentInvalid,
                   "incomplete or oversized shard assignment");
    }
    for (const auto& strategy : config.strategy_configs) {
      const uint8_t shard = strategy_shard.at(strategy.strategy_id.value);
      const auto assignment = std::find_if(
          config.assignments.begin(), config.assignments.end(),
          [&](const auto& item) { return item.shard.value == shard; });
      if (assignment == config.assignments.end())
        return Error(ErrorCode::kConfigAssignmentInvalid, "missing shard");
      if (std::none_of(
              assignment->accounts.begin(), assignment->accounts.end(),
              [&](const auto& item) { return item == strategy.account; })) {
        return Error(ErrorCode::kConfigAssignmentInvalid,
                     "strategy account not assigned to shard");
      }
      for (const auto& market : strategy.markets) {
        if (market_shard.at(market.native_symbol) != shard) {
          return Error(ErrorCode::kConfigAssignmentInvalid,
                       "strategy market on another shard");
        }
      }
    }

    const auto account_on_shard = [&](std::string_view account, uint8_t shard) {
      const auto assignment = std::find_if(
          config.assignments.begin(), config.assignments.end(),
          [&](const auto& item) { return item.shard.value == shard; });
      return assignment != config.assignments.end() &&
             std::any_of(
                 assignment->accounts.begin(), assignment->accounts.end(),
                 [&](const auto& item) { return item.value == account; });
    };

    if (root["risk_budgets"]) {
      auto budgets = Sequence(root, "risk_budgets");
      if (!budgets.ok()) return budgets.status();
      std::set<std::tuple<std::string, std::string, uint8_t>> unique;
      for (const auto& item : *budgets) {
        auto account = Scalar(item, "account");
        auto asset = Scalar(item, "asset");
        auto shard = Shard(item, "shard");
        auto limit = Number(item, "hard_limit", false);
        if (!account.ok()) return account.status();
        if (!asset.ok()) return asset.status();
        if (!shard.ok()) return shard.status();
        if (!limit.ok()) return limit.status();
        if (!accounts_by_name.contains(*account) ||
            !shard_ids.contains(shard->value) ||
            !account_on_shard(*account, shard->value) ||
            !unique.insert({*account, *asset, shard->value}).second) {
          return Error(ErrorCode::kConfigBudgetInvalid,
                       "invalid static risk budget");
        }
        StaticRiskBudgetConfig entry{AccountId(*account), AssetId(*asset),
                                     *shard, *limit};
        if (item["valid_for"]) {
          auto duration = Duration(item, "valid_for");
          if (!duration.ok()) return duration.status();
          entry.valid_for = *duration;
        }
        config.risk_budgets.push_back(std::move(entry));
      }
    }
    auto budget_status = CheckBudgetTotals(config);
    if (!budget_status.ok()) return budget_status;

    if (root["rate_budgets"]) {
      auto budgets = Sequence(root, "rate_budgets");
      if (!budgets.ok()) return budgets.status();
      for (const auto& item : *budgets) {
        auto account = Scalar(item, "account");
        auto shard = Shard(item, "shard");
        auto ip = Scalar(item, "ip");
        auto endpoint = Scalar(item, "endpoint");
        auto limit = Unsigned(item, "limit");
        auto reserve = Unsigned(item, "cancel_reserve");
        auto window = Unsigned(item, "window_us");
        if (!account.ok()) return account.status();
        if (!shard.ok()) return shard.status();
        if (!ip.ok()) return ip.status();
        if (!endpoint.ok()) return endpoint.status();
        if (!limit.ok()) return limit.status();
        if (!reserve.ok()) return reserve.status();
        if (!window.ok()) return window.status();
        if (!accounts_by_name.contains(*account) ||
            !shard_ids.contains(shard->value) ||
            !account_on_shard(*account, shard->value) || *limit == 0 ||
            *limit > UINT32_MAX || *reserve > *limit || *window == 0 ||
            *window > static_cast<uint64_t>(INT64_MAX)) {
          return Error(ErrorCode::kConfigBudgetInvalid,
                       "invalid static rate budget");
        }
        config.rate_budgets.push_back(
            {AccountId(*account), *shard, *ip, *endpoint,
             static_cast<uint32_t>(*limit), static_cast<uint32_t>(*reserve),
             std::chrono::microseconds(static_cast<int64_t>(*window))});
      }
    }
    return config;
  } catch (const YAML::Exception&) {
    return Error(ErrorCode::kConfigSyntaxInvalid, "invalid YAML configuration");
  }
}

absl::StatusOr<AppConfig> LoadConfig(const std::string& path) {
  std::ifstream file(path);
  if (!file)
    return Error(ErrorCode::kConfigFileNotFound,
                 "config file not found: " + path);
  const std::string content(std::istreambuf_iterator<char>{file}, {});
  return ParseConfig(content);
}

}  // namespace hquant
