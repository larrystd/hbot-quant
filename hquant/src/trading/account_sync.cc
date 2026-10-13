#include "hquant/trading/account_sync.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>

#include "boost/asio/post.hpp"
#include "hquant/base/fixed.h"

namespace hquant::v1 {

AccountSync::AccountSync(Inputs inputs)
    : inputs_(std::move(inputs)), sync_timer_(inputs_.io) {}

void AccountSync::Disconnect() {
  inputs_.disable_admission();
  sync_timer_.cancel();
  ++generation_;
  syncing_ = false;
  attempts_ = 0;
  buffered_private_.clear();
}

bool AccountSync::BufferPrivate(AccountEvent event, size_t capacity) {
  if (buffered_private_.size() >= capacity) {
    // 丢失回报后不能继续发布 Ready。
    ++generation_;
    syncing_ = false;
    return false;
  }
  buffered_private_.push_back(std::move(event));
  return true;
}

void AccountSync::CancelTimer() { sync_timer_.cancel(); }

struct AccountSync::State {
  uint64_t generation = 0;
  std::vector<ClientOrderId> orders;
  size_t order_index = 0;
  OrderReport report;
  std::vector<Fill> trades;
  size_t trade_index = 0;
  std::vector<BalanceUpdate> balances;
  size_t balance_index = 0;
  std::map<std::string, uint64_t> expected_totals;
};

void AccountSync::Start() {
  sync_timer_.cancel();
  inputs_.order_query_timer.cancel();
  inputs_.disable_admission();
  if (inputs_.quiescing) return;
  if (!inputs_.account_queue_empty()) {
    boost::asio::post(inputs_.io, GuardState([this] { Start(); }));
    return;
  }
  if (!inputs_.journal || !inputs_.port() || !inputs_.private_connected) {
    inputs_.pause("cannot synchronize account without trading connection");
    return;
  }
  if (++attempts_ > 3) {
    inputs_.pause("private events changed during three account sync attempts");
    return;
  }
  // 新代数使上一轮异步查询的回调失效。
  const uint64_t generation = ++generation_;
  syncing_ = true;
  inputs_.ready = false;
  inputs_.error.clear();
  inputs_.publish_all();
  auto state = std::make_shared<State>();
  state->generation = generation;
  state->expected_totals = inputs_.funds.ProjectedTotals();
  if (!inputs_.port()->QueryRules(
          GuardState([this, state](absl::StatusOr<std::vector<VenueRule>> response) {
            SyncRules(state, std::move(response));
          }))) {
    syncing_ = false;
    inputs_.pause("trading rules query queue unavailable");
  }
}

void AccountSync::SyncRules(std::shared_ptr<State> state,
                            absl::StatusOr<std::vector<VenueRule>> response) {
  if (state->generation != generation_ || !syncing_) return;
  if (!response.ok()) {
    syncing_ = false;
    inputs_.pause("trading rules query failed: " +
                  std::string(response.status().message()));
    return;
  }
  std::map<std::string, VenueRule> rules;
  for (VenueRule& rule : *response) {
    if (!rules.emplace(rule.market.symbol, std::move(rule)).second) {
      syncing_ = false;
      inputs_.pause("duplicate symbol in trading rules");
      return;
    }
  }
  for (const auto& [shard, market] : inputs_.markets) {
    const auto found = rules.find(market.id.symbol);
    if (found == rules.end() || !found->second.trading ||
        found->second.base_asset != market.base_asset ||
        found->second.quote_asset != market.quote_asset ||
        found->second.price_increment_nanos == 0 ||
        found->second.base_increment_nanos == 0 ||
        (market.rule.price_increment_nanos %
                 found->second.price_increment_nanos !=
             0 &&
         found->second.price_increment_nanos %
                 market.rule.price_increment_nanos !=
             0) ||
        (market.rule.base_increment_nanos %
                 found->second.base_increment_nanos !=
             0 &&
         found->second.base_increment_nanos %
                 market.rule.base_increment_nanos !=
             0) ||
        found->second.min_price_nanos %
                std::max(market.rule.price_increment_nanos,
                         found->second.price_increment_nanos) !=
            0 ||
        found->second.min_base_amount_nanos %
                std::max(market.rule.base_increment_nanos,
                         found->second.base_increment_nanos) !=
            0) {
      syncing_ = false;
      inputs_.pause("configured market differs from current venue rule: " +
                    market.id.symbol);
      return;
    }
  }
  inputs_.venue_rules = std::move(rules);
  if (!inputs_.port()->QueryOpenOrders(
          GuardState([this, state](absl::StatusOr<std::vector<OrderReport>> response) {
            SyncOpenOrders(state, std::move(response));
          }))) {
    syncing_ = false;
    inputs_.pause("open orders query queue unavailable");
  }
}

void AccountSync::SyncOpenOrders(
    std::shared_ptr<State> state,
    absl::StatusOr<std::vector<OrderReport>> response) {
  if (state->generation != generation_ || !syncing_) return;
  if (!response.ok()) {
    syncing_ = false;
    inputs_.pause("open orders query failed: " +
                  std::string(response.status().message()));
    return;
  }
  for (const OrderReport& report : *response) {
    const TrackedOrder* local =
        inputs_.orders.Find(report.order.client_order_id);
    if (!local || local->Terminal() || local->market != report.order.market ||
        local->side != report.order.side ||
        local->price_nanos != report.order.price_nanos ||
        local->quantity_nanos != report.order.quantity_nanos ||
        (local->exchange_order_id.value != 0 &&
         local->exchange_order_id != report.order.exchange_order_id)) {
      syncing_ = false;
      inputs_.pause("unrecognized or conflicting exchange open order");
      return;
    }
  }
  for (const auto& [id, order] : inputs_.orders.Orders()) {
    if (!order.Terminal()) state->orders.push_back(ClientOrderId{id});
  }
  SyncNextOrder(std::move(state));
}

void AccountSync::SyncNextOrder(std::shared_ptr<State> state) {
  if (state->generation != generation_ || !syncing_) return;
  if (state->order_index >= state->orders.size()) {
    SyncBalances(std::move(state));
    return;
  }
  const ClientOrderId id = state->orders[state->order_index];
  const TrackedOrder* order = inputs_.orders.Find(id);
  if (!order) {
    syncing_ = false;
    inputs_.pause("restored order disappeared during synchronization");
    return;
  }
  const QueryCommand query{order->market, id, order->exchange_order_id};
  if (!inputs_.port()->Query(
          query, GuardState([this, state, query](absl::StatusOr<OrderReport> result) {
            if (state->generation != generation_ || !syncing_) return;
            if (!result.ok()) {
              syncing_ = false;
              inputs_.pause("restored order query failed: " +
                            std::string(result.status().message()));
              return;
            }
            state->report = std::move(*result);
            state->report.authoritative_query = true;
            state->trades.clear();
            state->trade_index = 0;
            if (state->report.order.exchange_order_id.value == 0) {
              SyncNextTrade(state);
              return;
            }
            QueryCommand trades_query = query;
            trades_query.exchange_order_id =
                state->report.order.exchange_order_id;
            if (!inputs_.port()->QueryTrades(
                    trades_query,
                    GuardState([this, state](absl::StatusOr<std::vector<Fill>> trades) {
                      if (state->generation != generation_ || !syncing_) return;
                      if (!trades.ok()) {
                        syncing_ = false;
                        inputs_.pause("restored trades query failed: " +
                                      std::string(trades.status().message()));
                        return;
                      }
                      state->trades = std::move(*trades);
                      SyncNextTrade(state);
                    }))) {
              syncing_ = false;
              inputs_.pause("restored trades query queue unavailable");
            }
          }))) {
    syncing_ = false;
    inputs_.pause("restored order query queue unavailable");
  }
}

void AccountSync::SyncNextTrade(std::shared_ptr<State> state) {
  if (state->generation != generation_ || !syncing_) return;
  if (state->trade_index >= state->trades.size()) {
    if (!inputs_.persist_and_apply(
            AccountEvent{state->report}, false,
            [this, state](absl::Status status) {
              if (state->generation != generation_ || !syncing_) return;
              if (!status.ok()) {
                syncing_ = false;
                inputs_.pause("restored order report invalid: " +
                              std::string(status.message()));
                return;
              }
              ++state->order_index;
              SyncNextOrder(state);
            })) {
      syncing_ = false;
      inputs_.pause("restored order report journal queue unavailable");
    }
    return;
  }
  const Fill fill = state->trades[state->trade_index];
  const TrackedOrder* order = inputs_.orders.Find(fill.client_order_id);
  if (!order) {
    syncing_ = false;
    inputs_.pause("restored fill has unknown order");
    return;
  }
  if (!order->trade_ids.contains(fill.trade_id)) {
    const auto market = inputs_.markets.find(order->shard.value);
    if (market == inputs_.markets.end()) {
      syncing_ = false;
      inputs_.pause("restored fill market missing");
      return;
    }
    auto adjust = [&state](const std::string& asset, uint64_t amount,
                           bool credit) -> bool {
      auto found = state->expected_totals.find(asset);
      if (found == state->expected_totals.end()) return false;
      auto next = credit ? CheckedAdd(found->second, amount)
                         : CheckedSubtract(found->second, amount);
      if (!next.ok()) return false;
      found->second = *next;
      return true;
    };
    const std::string& base = market->second.base_asset;
    const std::string& quote = market->second.quote_asset;
    const bool valid = fill.side == Side::Buy
                           ? adjust(quote, fill.gross_nanos, false) &&
                                 adjust(base, fill.quantity_nanos, true)
                           : adjust(base, fill.quantity_nanos, false) &&
                                 adjust(quote, fill.gross_nanos, true);
    if (!valid || (fill.fee_nanos != 0 &&
                   !adjust(fill.fee_asset, fill.fee_nanos, false))) {
      syncing_ = false;
      inputs_.pause("restored fill balance projection invalid");
      return;
    }
  }
  if (!inputs_.persist_and_apply(
          AccountEvent{fill}, false, [this, state](absl::Status status) {
            if (state->generation != generation_ || !syncing_) return;
            if (!status.ok()) {
              syncing_ = false;
              inputs_.pause("restored fill invalid: " +
                            std::string(status.message()));
              return;
            }
            ++state->trade_index;
            SyncNextTrade(state);
          })) {
    syncing_ = false;
    inputs_.pause("restored fill journal queue unavailable");
  }
}

void AccountSync::SyncBalances(std::shared_ptr<State> state) {
  if (state->generation != generation_ || !syncing_) return;
  if (!inputs_.port()->QueryBalances(
          GuardState([this, state](absl::StatusOr<std::vector<BalanceUpdate>> result) {
            if (state->generation != generation_ || !syncing_) return;
            if (!result.ok()) {
              syncing_ = false;
              inputs_.pause("account balance query failed: " +
                            std::string(result.status().message()));
              return;
            }
            state->balances = std::move(*result);
            std::set<std::string> seen;
            for (const BalanceUpdate& balance : state->balances) {
              if (!seen.insert(balance.asset).second) {
                syncing_ = false;
                inputs_.pause("duplicate asset in account balance query");
                return;
              }
              const auto expected = state->expected_totals.find(balance.asset);
              if ((expected == state->expected_totals.end() &&
                   balance.total_nanos != 0) ||
                  (expected != state->expected_totals.end() &&
                   expected->second != balance.total_nanos)) {
                syncing_ = false;
                inputs_.pause("unexplained account balance difference: " +
                              balance.asset);
                return;
              }
              if (balance.locked_nanos >
                  inputs_.funds.Reserved(balance.asset)) {
                syncing_ = false;
                inputs_.pause("venue locked funds exceed shard reservations: " +
                              balance.asset);
                return;
              }
            }
            for (const auto& [asset, total] : state->expected_totals) {
              if (!seen.contains(asset)) {
                syncing_ = false;
                inputs_.pause("account balance query omitted asset: " + asset);
                return;
              }
            }
            state->balance_index = 0;
            SyncNextBalance(state);
          }))) {
    syncing_ = false;
    inputs_.pause("account balance query queue unavailable");
  }
}

void AccountSync::SyncNextBalance(std::shared_ptr<State> state) {
  if (state->generation != generation_ || !syncing_) return;
  if (state->balance_index >= state->balances.size()) {
    FinishSync(std::move(state));
    return;
  }
  AccountEvent event = state->balances[state->balance_index];
  if (!inputs_.persist_and_apply(
          std::move(event), false, [this, state](absl::Status status) {
            if (state->generation != generation_ || !syncing_) return;
            if (!status.ok()) {
              syncing_ = false;
              inputs_.pause("account balance snapshot invalid: " +
                            std::string(status.message()));
              return;
            }
            ++state->balance_index;
            SyncNextBalance(state);
          })) {
    syncing_ = false;
    inputs_.pause("account balance journal queue unavailable");
  }
}

void AccountSync::FinishSync(std::shared_ptr<State> state) {
  if (state->generation != generation_ || !syncing_) return;
  if (!buffered_private_.empty() || inputs_.pending_private_events() != 0) {
    // 快照期间来了新回报，重新查询后才能发布 Ready。
    buffered_private_.clear();
    Start();
    return;
  }
  syncing_ = false;
  attempts_ = 0;
  inputs_.funds.SettleVerifiedSnapshot();
  const bool demo_read_only = inputs_.config.mode == TradingMode::OnlineSim &&
                              inputs_.config.max_submissions_per_run == 0;
  inputs_.ready = !inputs_.manual_paused && !demo_read_only;
  inputs_.error = inputs_.manual_paused ? "manually paused"
                  : demo_read_only      ? "demo read-only validation"
                                        : "";
  inputs_.publish_all();
  if (inputs_.ready) inputs_.enable_admission();
  inputs_.orders.RearmPending();
  inputs_.schedule_order_query();
  if (!inputs_.quiescing) {
    sync_timer_.expires_after(std::chrono::seconds(60));
    sync_timer_.async_wait(GuardState([this](const boost::system::error_code& error) {
      if (!error && inputs_.private_connected && !inputs_.quiescing &&
          !syncing_) {
        Start();
      }
    }));
  }
}

}  // namespace hquant::v1
