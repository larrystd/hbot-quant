#include "hbot/app/config/config.h"

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
#include "yaml-cpp/yaml.h"

namespace hbot {
namespace {

absl::StatusOr<std::string> Scalar(const YAML::Node& node, const char* field) {
  const auto value = node[field];
  if (!value || !value.IsScalar() || value.Scalar().empty()) {
    return absl::InvalidArgumentError(std::string("missing scalar: ") + field);
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
    return absl::InvalidArgumentError(std::string("invalid integer: ") + field);
  }
  return number;
}

absl::StatusOr<Decimal> Number(const YAML::Node& node, const char* field,
                               bool positive) {
  auto value = Scalar(node, field);
  if (!value.ok()) return value.status();
  auto number = Decimal::Parse(*value);
  if (!number.ok())
    return absl::InvalidArgumentError(std::string("invalid decimal: ") + field);
  if (positive ? !number->IsStrictlyPositive() : !number->IsNonnegative()) {
    return absl::InvalidArgumentError(std::string("decimal out of range: ") +
                                      field);
  }
  return *number;
}

absl::StatusOr<YAML::Node> Sequence(const YAML::Node& node, const char* field) {
  const auto value = node[field];
  if (!value || !value.IsSequence()) {
    return absl::InvalidArgumentError(std::string("missing sequence: ") +
                                      field);
  }
  return value;
}

absl::StatusOr<YAML::Node> Mapping(const YAML::Node& node, const char* field) {
  const auto value = node[field];
  if (!value || !value.IsMap()) {
    return absl::InvalidArgumentError(std::string("missing map: ") + field);
  }
  return value;
}

absl::StatusOr<ShardId> Shard(const YAML::Node& node, const char* field) {
  auto value = Unsigned(node, field);
  if (!value.ok()) return value.status();
  if (*value >= 8) return absl::InvalidArgumentError("shard must be in [0,7]");
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
      return absl::InvalidArgumentError("refresh_interval must use s or ms");
    }
    uint64_t count = 0;
    auto [end, error] =
        std::from_chars(value.data(), value.data() + value.size(), count);
    if (error != std::errc{} || end != value.data() + value.size() ||
        count > static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) /
                    multiplier) {
      return absl::InvalidArgumentError("invalid refresh_interval");
    }
    us = count * multiplier;
  }
  if (us == 0 ||
      us > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
    return absl::InvalidArgumentError("refresh interval out of range");
  }
  return std::chrono::microseconds(static_cast<int64_t>(us));
}

absl::StatusOr<MarketId> NamedMarket(
    const YAML::Node& node, const std::map<std::string, MarketId>& markets) {
  if (!node.IsScalar())
    return absl::InvalidArgumentError("market name must be scalar");
  const auto found = markets.find(node.Scalar());
  if (found == markets.end())
    return absl::InvalidArgumentError("unknown market dependency");
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
      return absl::InvalidArgumentError("duplicate market dependency");
    }
    result.push_back(std::move(*market));
  }
  return result;
}

absl::Status CheckLeaseTotals(const EngineConfig& config) {
  std::map<std::pair<std::string, std::string>, Decimal> totals;
  for (const auto& lease : config.static_risk_leases) {
    const auto key = std::pair{lease.account.value, lease.asset.value};
    const auto found = totals.find(key);
    if (found == totals.end()) {
      totals.emplace(key, lease.hard_limit);
    } else {
      auto sum = found->second.Add(lease.hard_limit);
      if (!sum.ok()) return sum.status();
      found->second = *sum;
    }
  }
  for (const auto& [key, granted] : totals) {
    const auto account = std::find_if(
        config.accounts.begin(), config.accounts.end(),
        [&](const auto& item) { return item.account.value == key.first; });
    if (account == config.accounts.end())
      return absl::InvalidArgumentError("lease account unknown");
    const auto balance = account->initial_balances.find(key.second);
    if (balance == account->initial_balances.end()) {
      return absl::InvalidArgumentError(
          "lease asset has no conservative balance");
    }
    auto compare = granted.Compare(balance->second);
    if (!compare.ok()) return compare.status();
    if (*compare > 0)
      return absl::InvalidArgumentError(
          "static risk leases exceed account balance");
  }
  return absl::OkStatus();
}

}  // namespace

absl::StatusOr<EngineConfig> ParseConfig(std::string_view yaml_text) {
  try {
    const YAML::Node root = YAML::Load(std::string(yaml_text));
    if (!root.IsMap())
      return absl::InvalidArgumentError("config root must be a map");
    EngineConfig config;
    auto version = Unsigned(root, "schema_version");
    if (!version.ok()) return version.status();
    if (*version != 1)
      return absl::InvalidArgumentError("unsupported config schema_version");
    auto mode = Scalar(root, "mode");
    if (!mode.ok()) return mode.status();
    if (*mode == "live")
      return absl::FailedPreconditionError(
          "live mode is unavailable before G4");
    if (*mode != "paper")
      return absl::InvalidArgumentError("mode must explicitly be paper");
    config.mode = EngineMode::Paper;
    auto loop_mode = Scalar(root, "loop_mode");
    if (!loop_mode.ok()) return loop_mode.status();
    if (*loop_mode == "blocking")
      config.loop_mode = LoopMode::Blocking;
    else if (*loop_mode == "busy")
      config.loop_mode = LoopMode::Busy;
    else
      return absl::InvalidArgumentError("unknown loop_mode");
    auto storage_path = Scalar(root, "storage_path");
    if (!storage_path.ok()) return storage_path.status();
    config.storage_path = *storage_path;
    if (root["market_data_source"]) {
      auto source = Scalar(root, "market_data_source");
      if (!source.ok()) return source.status();
      if (*source == "replay")
        config.market_data_source = MarketDataSource::Replay;
      else if (*source == "binance_public") {
        config.market_data_source = MarketDataSource::BinancePublic;
      } else
        return absl::InvalidArgumentError("unsupported market_data_source");
    }
    if (config.market_data_source == MarketDataSource::Replay) {
      auto replay_fixture = Scalar(root, "replay_fixture");
      if (!replay_fixture.ok()) {
        return absl::FailedPreconditionError(
            "paper replay requires replay_fixture");
      }
      config.replay_fixture = *replay_fixture;
    } else if (root["replay_fixture"]) {
      return absl::InvalidArgumentError(
          "public market data cannot also use replay_fixture");
    }

    std::set<std::string> accounts_by_name;
    auto accounts = Sequence(root, "accounts");
    if (!accounts.ok()) return accounts.status();
    for (const auto& item : *accounts) {
      if (!item.IsMap())
        return absl::InvalidArgumentError("account must be a map");
      if (item["api_key"] || item["secret_key"] || item["private_key"]) {
        return absl::InvalidArgumentError("inline credentials are not allowed");
      }
      auto name = Scalar(item, "account");
      if (!name.ok()) return name.status();
      if (!accounts_by_name.insert(*name).second)
        return absl::InvalidArgumentError("duplicate account");
      auto balances = Mapping(item, "initial_balances");
      if (!balances.ok()) return balances.status();
      AccountConfig account;
      account.account = AccountId(*name);
      for (const auto& entry : *balances) {
        if (!entry.first.IsScalar() || !entry.second.IsScalar()) {
          return absl::InvalidArgumentError(
              "balance must be asset to decimal text");
        }
        auto balance = Decimal::Parse(entry.second.Scalar());
        if (!balance.ok() || !balance->IsNonnegative()) {
          return absl::InvalidArgumentError("invalid initial balance");
        }
        account.initial_balances.emplace(entry.first.Scalar(), *balance);
      }
      config.accounts.push_back(std::move(account));
    }
    if (config.accounts.empty())
      return absl::InvalidArgumentError("at least one account required");

    std::map<std::string, MarketId> markets_by_name;
    auto market_specs = Sequence(root, "market_specs");
    if (!market_specs.ok()) return market_specs.status();
    for (const auto& item : *market_specs) {
      if (!item.IsMap())
        return absl::InvalidArgumentError("market spec must be a map");
      auto name = Scalar(item, "market");
      auto venue = Scalar(item, "venue");
      auto base = Scalar(item, "base_asset");
      auto quote = Scalar(item, "quote_asset");
      if (!name.ok()) return name.status();
      if (!venue.ok()) return venue.status();
      if (!base.ok()) return base.status();
      if (!quote.ok()) return quote.status();
      if (markets_by_name.contains(*name))
        return absl::InvalidArgumentError("duplicate market");
      MarketConfig market;
      market.spec.market =
          MarketId{VenueId(*venue), InstrumentKind::Spot, *name};
      market.spec.base_asset = AssetId(*base);
      market.spec.quote_asset = AssetId(*quote);
      const YAML::Node scale = item["book_scale"] ? item["book_scale"] : item;
      auto tick = Number(scale, "quote_per_tick", true);
      auto lot = Number(scale, "base_per_lot", true);
      auto scale_version = Unsigned(scale, "scale_version");
      if (!tick.ok()) return tick.status();
      if (!lot.ok()) return lot.status();
      if (!scale_version.ok()) return scale_version.status();
      if (*scale_version == 0)
        return absl::InvalidArgumentError("scale_version must be positive");
      market.book_scale = BookScale{*tick, *lot, *scale_version};
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
      markets_by_name.emplace(*name, market.spec.market);
      config.market_specs.push_back(std::move(market));
    }
    if (config.market_specs.empty())
      return absl::InvalidArgumentError("at least one market required");

    std::map<uint64_t, OwnerId> owners_by_key;
    auto strategies = Sequence(root, "strategy_configs");
    if (!strategies.ok()) return strategies.status();
    for (const auto& item : *strategies) {
      if (!item.IsMap())
        return absl::InvalidArgumentError("strategy must be a map");
      auto key = Unsigned(item, "owner_key");
      auto strategy = Scalar(item, "strategy");
      auto account = Scalar(item, "account");
      if (!key.ok()) return key.status();
      if (!strategy.ok()) return strategy.status();
      if (!account.ok()) return account.status();
      if (*key == 0 || *key >= (uint64_t{1} << 48) ||
          owners_by_key.contains(*key)) {
        return absl::InvalidArgumentError("invalid or duplicate owner_key");
      }
      if (*strategy != "simple_pmm" || !accounts_by_name.contains(*account)) {
        return absl::InvalidArgumentError(
            "unsupported strategy or unknown account");
      }
      auto dependencies = MarketList(item, "markets", markets_by_name);
      if (!dependencies.ok()) return dependencies.status();
      if (dependencies->empty())
        return absl::InvalidArgumentError("strategy needs market");
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
        return absl::InvalidArgumentError("strategy spread must be below one");
      }
      StrategyConfig config_entry;
      config_entry.owner = OwnerId{*key, StrategyId(*strategy), std::nullopt};
      config_entry.account = AccountId(*account);
      config_entry.markets = std::move(*dependencies);
      config_entry.order_amount = *amount;
      config_entry.bid_spread = *bid_spread;
      config_entry.ask_spread = *ask_spread;
      config_entry.refresh_interval = *refresh;
      owners_by_key.emplace(*key, config_entry.owner);
      config.strategy_configs.push_back(std::move(config_entry));
    }
    if (config.strategy_configs.empty())
      return absl::InvalidArgumentError("at least one strategy required");

    std::map<std::string, uint8_t> market_shard;
    std::map<uint64_t, uint8_t> owner_shard;
    std::set<uint8_t> shard_ids;
    auto assignments = Sequence(root, "assignments");
    if (!assignments.ok()) return assignments.status();
    for (const auto& item : *assignments) {
      if (!item.IsMap())
        return absl::InvalidArgumentError("assignment must be a map");
      auto shard = Shard(item, "shard");
      if (!shard.ok()) return shard.status();
      if (!shard_ids.insert(shard->value).second)
        return absl::InvalidArgumentError("duplicate shard");
      ShardAssignment assignment;
      assignment.shard = *shard;
      auto markets = MarketList(item, "markets", markets_by_name);
      if (!markets.ok()) return markets.status();
      if (markets->empty())
        return absl::InvalidArgumentError("empty shard markets");
      for (auto& market : *markets) {
        if (!market_shard.emplace(market.native_symbol, shard->value).second) {
          return absl::InvalidArgumentError("market assigned twice");
        }
        assignment.markets.push_back(std::move(market));
      }
      auto owners = Sequence(item, "owners");
      if (!owners.ok()) return owners.status();
      for (const auto& owner_node : *owners) {
        if (!owner_node.IsScalar())
          return absl::InvalidArgumentError("owner key must be scalar");
        const std::string text = owner_node.Scalar();
        uint64_t key = 0;
        auto [end, error] =
            std::from_chars(text.data(), text.data() + text.size(), key);
        if (error != std::errc{} || end != text.data() + text.size()) {
          return absl::InvalidArgumentError("invalid assigned owner key");
        }
        const auto owner = owners_by_key.find(key);
        if (owner == owners_by_key.end() ||
            !owner_shard.emplace(key, shard->value).second) {
          return absl::InvalidArgumentError(
              "unknown or duplicate assigned owner");
        }
        assignment.owners.push_back(owner->second);
      }
      auto account_nodes = Sequence(item, "accounts");
      if (!account_nodes.ok()) return account_nodes.status();
      std::set<std::string> unique_accounts;
      for (const auto& account_node : *account_nodes) {
        if (!account_node.IsScalar() ||
            !accounts_by_name.contains(account_node.Scalar()) ||
            !unique_accounts.insert(account_node.Scalar()).second) {
          return absl::InvalidArgumentError(
              "unknown or duplicate assigned account");
        }
        assignment.accounts.emplace_back(account_node.Scalar());
      }
      config.assignments.push_back(std::move(assignment));
    }
    if (config.assignments.empty() || config.assignments.size() > 8 ||
        market_shard.size() != markets_by_name.size() ||
        owner_shard.size() != owners_by_key.size()) {
      return absl::InvalidArgumentError(
          "incomplete or oversized shard assignment");
    }
    for (const auto& strategy : config.strategy_configs) {
      const uint8_t shard = owner_shard.at(strategy.owner.owner_key);
      const auto assignment = std::find_if(
          config.assignments.begin(), config.assignments.end(),
          [&](const auto& item) { return item.shard.value == shard; });
      if (assignment == config.assignments.end())
        return absl::InvalidArgumentError("missing shard");
      if (std::none_of(
              assignment->accounts.begin(), assignment->accounts.end(),
              [&](const auto& item) { return item == strategy.account; })) {
        return absl::InvalidArgumentError(
            "strategy account not assigned to shard");
      }
      for (const auto& market : strategy.markets) {
        if (market_shard.at(market.native_symbol) != shard) {
          return absl::InvalidArgumentError("strategy market on another shard");
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

    if (root["static_risk_leases"]) {
      auto leases = Sequence(root, "static_risk_leases");
      if (!leases.ok()) return leases.status();
      std::set<std::tuple<std::string, std::string, uint8_t>> unique;
      for (const auto& item : *leases) {
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
          return absl::InvalidArgumentError("invalid static risk lease");
        }
        config.static_risk_leases.push_back(
            {AccountId(*account), AssetId(*asset), *shard, *limit});
      }
    }
    auto lease_status = CheckLeaseTotals(config);
    if (!lease_status.ok()) return lease_status;

    if (root["static_rate_leases"]) {
      auto leases = Sequence(root, "static_rate_leases");
      if (!leases.ok()) return leases.status();
      for (const auto& item : *leases) {
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
          return absl::InvalidArgumentError("invalid static rate lease");
        }
        config.static_rate_leases.push_back(
            {AccountId(*account), *shard, *ip, *endpoint,
             static_cast<uint32_t>(*limit), static_cast<uint32_t>(*reserve),
             std::chrono::microseconds(static_cast<int64_t>(*window))});
      }
    }
    return config;
  } catch (const YAML::Exception&) {
    return absl::InvalidArgumentError("invalid YAML configuration");
  }
}

absl::StatusOr<EngineConfig> LoadConfig(const std::string& path) {
  std::ifstream file(path);
  if (!file) return absl::NotFoundError("config file not found");
  const std::string content(std::istreambuf_iterator<char>{file}, {});
  return ParseConfig(content);
}

}  // namespace hbot
