#include "application/quant_server.h"

#include <chrono>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <thread>
#include <string>
#include <utility>
#include <variant>

#include "application/control_server.h"
#include "base/error.h"
#include "base/rate_limit.h"
#include "boost/asio/post.hpp"
#include "boost/asio/steady_timer.hpp"
#include "boost/asio/this_coro.hpp"
#include "boost/asio/use_awaitable.hpp"
#include "market/replay_feed.h"
#include "order_history/order_history_reader.h"
#include "order_history/order_history_writer.h"
#include "order/risk.h"
#include "order/simulated_exchange.h"
#include "shard/shard.h"
#include "strategy/simple_pmm.h"

namespace hquant {
namespace {

class SystemClock final : public Clock {
 public:
  UtcTime UtcNow() const override {
    return std::chrono::time_point_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now());
  }
  MonoTime MonoNow() const override {
    return std::chrono::time_point_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now());
  }
};

absl::Status ApplyReplayInput(const ReplayInput& input,
                              const MarketConfig& market, ReplayClock& clock,
                              Shard& shard) {
  const EventTime time{{}, clock.UtcNow(), clock.MonoNow()};
  if (const auto* subscribe = std::get_if<ReplaySubscribe>(&input.payload)) {
    shard.Subscribe(subscribe->connection_id);
  } else if (const auto* snapshot =
                 std::get_if<ReplaySnapshot>(&input.payload)) {
    return shard.OnSnapshot({market.spec.market,
                             market.tick_lot_size.tick_lot_version,
                             snapshot->connection_id, snapshot->last_sequence,
                             snapshot->bids, snapshot->asks, time});
  } else if (const auto* diff = std::get_if<ReplayDiff>(&input.payload)) {
    return shard.OnDiff({market.spec.market,
                         market.tick_lot_size.tick_lot_version,
                         diff->connection_id, diff->first_sequence,
                         diff->last_sequence, diff->bids, diff->asks, time});
  } else if (std::holds_alternative<ReplayTimer>(input.payload)) {
    auto result = shard.OnTimer(input.stamp);
    if (!result.ok()) return result.status();
  } else if (const auto* trade =
                 std::get_if<ReplayPublicTrade>(&input.payload)) {
    return shard.OnPublicTrade({market.spec.market,
                                {},
                                trade->price_ticks,
                                trade->quantity_lots,
                                trade->side,
                                time});
  }
  return absl::OkStatus();
}

absl::StatusOr<std::unique_ptr<Strategy>> MakeStrategy(
    const StrategyConfig& config, const AccountConfig& account,
    const MarketConfig& market) {
  if (config.strategy_id.name.value != "simple_pmm")
    return Error(ErrorCode::kConfigFieldInvalid, "unknown strategy name");
  return std::make_unique<SimplePmm>(SimplePmmConfig{
      config.strategy_id, account.account, market.spec, config.order_amount,
      config.bid_spread, config.ask_spread, config.refresh_interval,
      config.price_type, config.maker_fee_rate, true, config.timer_period});
}

std::string AggregateStatusJson(const std::vector<std::string>& entries,
                                const SqliteOrderHistoryWriter& writer) {
  const auto health = writer.Health();
  std::string json =
      "{\"mode\":\"simulated\",\"exchange\":\"simulated\",\"shards\":[";
  for (size_t index = 0; index < entries.size(); ++index) {
    if (index) json += ',';
    json += entries[index];
  }
  json += "],\"active_shards\":" + std::to_string(entries.size()) +
          ",\"recorder_dropped\":" + std::to_string(health.dropped_count) +
          ",\"history_gaps\":" + std::to_string(health.gap_ranges.size()) +
          ",\"recorder_error\":" + EscapeJson(health.last_error) + "}";
  return json;
}

}  // namespace

absl::Status QuantServer::CreateComponents() {
  live = config.market_data_source == MarketDataSource::BinancePublic;
  if (live) {
    clock = std::make_unique<SystemClock>();
  } else {
    const UtcTime origin(std::chrono::microseconds(1'000'000));
    auto replay = std::make_unique<ReplayClock>(
        origin, MonoTime(std::chrono::microseconds(1'000'000)));
    replay_clock = replay.get();
    clock = std::move(replay);
  }
  started = clock->UtcNow();
  run = RunId{static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count())};
  auto opened = SqliteOrderHistoryWriter::Open({storage_path, run, started,
                                               config.storage.writer_queue,
                                               config.storage.writer_batch});
  if (!opened.ok()) return opened.status();
  writer = std::move(*opened);
  for (const auto& assignment : config.assignments) {
    const auto& market_id = assignment.markets.front();
    const auto& strategy_id = assignment.strategy_ids.front();
    const auto& account_id = assignment.accounts.front();
    const auto& account = *std::find_if(
        config.accounts.begin(), config.accounts.end(),
        [&](const auto& item) { return item.account == account_id; });
    const auto& market = *std::find_if(
        config.market_specs.begin(), config.market_specs.end(),
        [&](const auto& item) { return item.spec.market == market_id; });
    const auto& strategy_config = *std::find_if(
        config.strategy_configs.begin(), config.strategy_configs.end(),
        [&](const auto& item) { return item.strategy_id == strategy_id; });
    auto rule = market.trading_rule;
    rule.revision = 1;
    rule.observed_at = started;
    auto strategy = MakeStrategy(strategy_config, account, market);
    if (!strategy.ok()) return strategy.status();
    auto exchange = std::make_unique<SimpleSimulatedExchange>(
        SimulatedExchangeConfig{
            account.account, market.spec, rule, account.initial_balances,
            config.simulated_exchange.maker_fee_rate, true,
            [run = run, shard = assignment.shard,
             next_id = uint64_t{1}](Side) mutable {
              return ClientOrderId("S" + std::to_string(run.value) + "-" +
                                   std::to_string(shard.value) + "-" +
                                   std::to_string(next_id++));
            }},
        *clock);
    auto risk = std::make_unique<RiskGate>(RiskGate::Settings{
        assignment.shard, config.risk.fee_buffer_rate,
        std::chrono::duration_cast<std::chrono::seconds>(
            *config.risk.max_rule_age)});
    for (const auto& budget : config.risk_budgets) {
      if (budget.shard != assignment.shard) continue;
      auto status = risk->SetInitialBudget(
          {budget.account, budget.asset, budget.shard, 1, budget.hard_limit,
           started + *budget.valid_for});
      if (!status.ok()) return status;
    }
    Shard::Config shard_config{run, assignment.shard,
                               strategy_config.strategy_id, account.account,
                               market.spec, market.tick_lot_size, rule};
    shard_config.stale_after_us = market.stale_after->count();
    shards.push_back(std::make_unique<Shard>(
        shard_config, *clock, std::move(*strategy), std::move(exchange),
        std::move(risk), *writer));
  }
  auto read = SqliteOrderHistoryReader::Open(
      {storage_path, config.storage.reader_queue,
       config.storage.reader_page_limit});
  if (!read.ok()) return read.status();
  reader = std::move(*read);
  return absl::OkStatus();
}

absl::Status QuantServer::RunReplay() {
  auto replay = ReadReplayFile(
      *config.replay_fixture,
      [&](const ReplayInput& input) -> absl::Status {
        auto advanced = replay_clock->Advance(input.stamp);
        if (!advanced.ok()) return advanced;
        if (std::holds_alternative<ReplayTimer>(input.payload)) {
          bool found = false;
          for (auto& shard : shards) {
            if (input.shard && *input.shard != shard->id()) continue;
            found = true;
            auto result = shard->OnTimer(input.stamp);
            if (!result.ok()) return result.status();
          }
          if (!found)
            return Error(ErrorCode::kReplayFileInvalid,
                         "timer targets an unknown shard");
          return absl::OkStatus();
        }
        for (auto& shard : shards) {
          if (input.market &&
              *input.market != shard->market().market.native_symbol)
            continue;
          const auto market = std::find_if(
              config.market_specs.begin(), config.market_specs.end(),
              [&](const auto& item) {
                return item.spec.market == shard->market().market;
              });
          return ApplyReplayInput(input, *market, *replay_clock, *shard);
        }
        return Error(ErrorCode::kReplayFileInvalid,
                     "input targets an unknown market");
      },
      shards.size() == 1);
  if (!replay.ok()) return replay;
  return writer->Flush();
}

QuantServer::QuantServer(AppConfig config, std::string state_dir,
                         std::string storage_path)
    : config(std::move(config)),
      state_dir(std::move(state_dir)),
      storage_path(std::move(storage_path)) {}

absl::StatusOr<std::unique_ptr<QuantServer>> QuantServer::Create(
    const AppConfig& config, std::string state_dir) {
  const bool live =
      config.market_data_source == MarketDataSource::BinancePublic;
  if (config.mode != EngineMode::Simulated ||
      (live && config.loop_mode != LoopMode::Blocking) ||
      (!live && !config.replay_fixture)) {
    return Error(ErrorCode::kLaunchMultipleNotSupported,
                 "unsupported trading mode or missing replay fixture");
  }
  if (config.assignments.empty() || config.assignments.size() > 8) {
    return Error(ErrorCode::kConfigAssignmentInvalid,
                 "shard count must be in [1,8]");
  }
  if (live && config.assignments.size() >
                  std::max(1u, std::thread::hardware_concurrency())) {
    return Error(ErrorCode::kLaunchMultipleNotSupported,
                 "reduce shards to available CPU cores");
  }
  std::set<uint8_t> shard_ids;
  std::set<std::string> markets;
  std::set<std::string> accounts;
  std::set<uint64_t> strategies;
  for (const auto& assignment : config.assignments) {
    if (!assignment.shard.IsValid() ||
        !shard_ids.insert(assignment.shard.value).second)
      return Error(ErrorCode::kConfigAssignmentInvalid,
                   "invalid or duplicate shard ID");
    if (assignment.markets.size() != 1 ||
        assignment.strategy_ids.size() != 1 ||
        assignment.accounts.size() != 1)
      return Error(ErrorCode::kLaunchMultipleNotSupported,
                   "each shard needs one market, strategy and account");
    const auto& market_id = assignment.markets.front();
    const auto& account_id = assignment.accounts.front();
    const auto& strategy_id = assignment.strategy_ids.front();
    if (!markets.insert(market_id.native_symbol).second)
      return Error(ErrorCode::kConfigAssignmentInvalid,
                   "market assigned twice");
    if (!accounts.insert(account_id.value).second)
      return Error(ErrorCode::kLaunchMultipleNotSupported,
                   "account assigned twice");
    if (!strategies.insert(strategy_id.value).second)
      return Error(ErrorCode::kConfigAssignmentInvalid,
                   "strategy assigned twice");
    const auto market = std::find_if(
        config.market_specs.begin(), config.market_specs.end(),
        [&](const auto& item) { return item.spec.market == market_id; });
    const auto account = std::find_if(
        config.accounts.begin(), config.accounts.end(),
        [&](const auto& item) { return item.account == account_id; });
    const auto strategy = std::find_if(
        config.strategy_configs.begin(), config.strategy_configs.end(),
        [&](const auto& item) { return item.strategy_id == strategy_id; });
    if (market == config.market_specs.end() ||
        account == config.accounts.end() ||
        strategy == config.strategy_configs.end() ||
        strategy->account != account_id || strategy->markets.size() != 1 ||
        strategy->markets.front() != market_id ||
        (live && market_id.exchange != ExchangeId("binance")))
      return Error(ErrorCode::kConfigReferenceInvalid,
                   "shard strategy, market or account mismatch");
    if (strategy_id.name.value != "simple_pmm")
      return Error(ErrorCode::kConfigFieldInvalid, "unknown strategy name");
  }
  if (markets.size() != config.market_specs.size() ||
      accounts.size() != config.accounts.size() ||
      strategies.size() != config.strategy_configs.size())
    return Error(ErrorCode::kConfigAssignmentInvalid,
                 "incomplete shard assignment");
  if (!config.risk.max_rule_age ||
      std::any_of(config.market_specs.begin(), config.market_specs.end(),
                  [](const auto& market) { return !market.stale_after; }) ||
      std::any_of(config.risk_budgets.begin(), config.risk_budgets.end(),
                  [](const auto& budget) { return !budget.valid_for; }))
    return Error(ErrorCode::kConfigFieldInvalid,
                 "configuration defaults are missing");
  RiskBudgetAllocator allocator;
  for (const auto& account : config.accounts) {
    for (const auto& [asset, balance] : account.initial_balances) {
      auto status = allocator.SetConservativeLimit(account.account,
                                                   AssetId(asset), balance);
      if (!status.ok())
        return Error(ErrorCode::kConfigBudgetInvalid, status.message());
    }
  }
  const UtcTime budget_now{std::chrono::microseconds(0)};
  for (const auto& budget : config.risk_budgets) {
    auto assignment = std::find_if(
        config.assignments.begin(), config.assignments.end(),
        [&](const auto& item) { return item.shard == budget.shard; });
    if (assignment == config.assignments.end() ||
        assignment->accounts.front() != budget.account)
      return Error(ErrorCode::kConfigBudgetInvalid,
                   "risk budget does not belong to its shard");
    auto status = allocator.GrantInitial(
        {budget.account, budget.asset, budget.shard, 1, budget.hard_limit,
         budget_now + *budget.valid_for}, budget_now);
    if (!status.ok())
      return Error(ErrorCode::kConfigBudgetInvalid, status.message());
  }
  if (config.assignments.size() > 1 && !config.rate_budgets.empty() &&
      config.rate_capacities.empty())
    return Error(ErrorCode::kConfigBudgetInvalid,
                 "multi-shard rate capacities are required");
  std::vector<RateBudgetAssignment> rate_grants;
  for (const auto& budget : config.rate_budgets) {
    const auto assignment = std::find_if(
        config.assignments.begin(), config.assignments.end(),
        [&](const auto& item) { return item.shard == budget.shard; });
    if (assignment == config.assignments.end() ||
        assignment->accounts.front() != budget.account)
      return Error(ErrorCode::kConfigBudgetInvalid,
                   "rate budget does not belong to its shard");
    rate_grants.push_back(
        {{budget.account.value, budget.ip, budget.endpoint}, budget.shard, 1,
         {budget.limit, budget.cancel_reserve, budget.window, 0}});
  }
  auto rate_status = ValidateRateBudgets(config.rate_capacities, rate_grants);
  if (!rate_status.ok())
    return Error(ErrorCode::kConfigBudgetInvalid, rate_status.message());
  std::error_code error;
  std::filesystem::create_directories(state_dir, error);
  if (error)
    return ErrorFromSystem(ErrorCode::kStateDirUnavailable, error,
                           state_dir);
  const auto path =
      std::filesystem::path(config.storage_path).is_absolute()
          ? std::filesystem::path(config.storage_path)
          : std::filesystem::path(state_dir) / config.storage_path;
  std::filesystem::create_directories(path.parent_path(), error);
  if (error)
    return ErrorFromSystem(ErrorCode::kStateDirUnavailable, error,
                           path.parent_path().string());
  return std::unique_ptr<QuantServer>(
      new QuantServer(config, std::move(state_dir), path.string()));
}

QuantServer::~QuantServer() {
  if (started_server && !waited) {
    RequestStop();
    (void)Wait();
  }
  for (auto& shard : shards) shard->Join();
}

absl::Status QuantServer::Start() {
  auto status = CreateComponents();
  if (!status.ok()) return status;
  if (live) {
    const auto& rest = config.binance_endpoints.rest;
    const auto& ws = config.binance_endpoints.websocket;
    for (auto& shard : shards)
      shard->StartFeed({rest.host, rest.port, rest.tls},
                       {ws.host, ws.port, ws.tls});
  } else {
    status = RunReplay();
    if (!status.ok()) return status;
  }
  auto handler =
      [this](
          ControlRequest request) -> boost::asio::awaitable<ControlResponse> {
    ControlResponse response;
    response.request_id = request.request_id;
    if (std::holds_alternative<StatusRequest>(request.payload)) {
      auto result = co_await StatusAsync();
      if (result.ok())
        response.payload = StatusResponse{*result};
      else
        response.payload = ControlError{CodeOf(result.status()),
                                        std::string(result.status().message())};
    } else if (const auto* history =
                   std::get_if<OrderHistoryRequest>(&request.payload)) {
      OrderHistoryQuery query;
      query.request_id = request.request_id;
      query.page_size = history->limit;
      if (!history->cursor.empty()) query.cursor = history->cursor;
      auto result = co_await OrderHistoryAsync(std::move(query));
      if (result.ok())
        response.payload = OrderHistoryResponse{OrderHistoryJson(*result)};
      else
        response.payload = ControlError{CodeOf(result.status()),
                                        std::string(result.status().message())};
    } else {
      RequestStop();
      response.payload = StopResponse{true};
    }
    co_return response;
  };
  auto opened = ControlServer::Start(state_dir + "/control.sock",
                                     handler, [this](int) { RequestStop(); });
  if (!opened.ok()) {
    RequestStop();
    for (auto& shard : shards) shard->Join();
    return opened.status();
  }
  control_server = std::move(*opened);
  started_server = true;
  return absl::OkStatus();
}

absl::Status QuantServer::Wait() {
  if (waited) return absl::OkStatus();
  std::unique_lock lock(stop_mutex);
  stop_cv.wait(lock, [&] { return stopping.load(); });
  lock.unlock();
  if (control_server) control_server->Stop(false);
  for (auto& shard : shards) shard->Join();
  auto stopped = writer->Stop(clock->UtcNow());
  reader.reset();
  if (control_server) {
    control_server->Stop();
    control_server.reset();
  }
  waited = true;
  return stopped;
}

void QuantServer::RequestStop() {
  {
    std::lock_guard lock(stop_mutex);
    if (stopping.exchange(true)) return;
  }
  for (auto& shard : shards) shard->RequestStop();
  stop_cv.notify_all();
}

boost::asio::awaitable<absl::StatusOr<std::string>> QuantServer::StatusAsync() {
  if (shards.size() > 1) {
    std::vector<std::string> entries(shards.size());
    if (!live) {
      for (size_t index = 0; index < shards.size(); ++index)
        entries[index] = ShardStatusJson(
            *shards[index], shards[index]->OwnedExchange(), false);
      co_return AggregateStatusJson(entries, *writer);
    }
    auto executor = co_await boost::asio::this_coro::executor;
    auto ready = std::make_shared<boost::asio::steady_timer>(executor);
    auto results = std::make_shared<std::vector<std::optional<std::string>>>(
        shards.size());
    auto remaining = std::make_shared<size_t>(shards.size());
    ready->expires_after(std::chrono::seconds(2));
    for (size_t index = 0; index < shards.size(); ++index) {
      Shard* shard = shards[index].get();
      boost::asio::post(*shard->LiveIo(),
                        [shard, index, executor, results, remaining, ready] {
                          auto json = ShardStatusJson(
                              *shard, shard->OwnedExchange(), true);
                          boost::asio::post(
                              executor,
                              [index, results, remaining, ready,
                               json = std::move(json)]() mutable {
                                (*results)[index] = std::move(json);
                                if (--*remaining == 0) ready->cancel();
                              });
                        });
    }
    boost::system::error_code ec;
    co_await ready->async_wait(
        boost::asio::redirect_error(boost::asio::use_awaitable, ec));
    for (size_t index = 0; index < shards.size(); ++index) {
      if ((*results)[index]) {
        entries[index] = std::move(*(*results)[index]);
      } else {
        entries[index] =
            "{\"shard\":" + std::to_string(shards[index]->id().value) +
            ",\"market\":" +
            EscapeJson(shards[index]->market().market.native_symbol) +
            ",\"error\":\"CONTROL_TIMEOUT\"}";
      }
    }
    co_return AggregateStatusJson(entries, *writer);
  }
  const auto& market = config.market_specs.front();
  if (!live) {
    co_return StatusJson(*shards.front(), shards.front()->OwnedExchange(),
                         market.spec, *writer);
  }
  auto executor = co_await boost::asio::this_coro::executor;
  auto ready = std::make_shared<boost::asio::steady_timer>(executor);
  auto result = std::make_shared<std::optional<absl::StatusOr<std::string>>>();
  ready->expires_after(std::chrono::seconds(2));
  boost::asio::post(*shards.front()->LiveIo(), [this, ready, result, executor] {
    auto json =
        StatusJson(*shards.front(), shards.front()->OwnedExchange(),
                   config.market_specs.front().spec, *writer);
    json.pop_back();
    json +=
        ",\"applied_diffs\":" +
        std::to_string(shards.front()->AppliedDiffs()) +
        ",\"resyncs\":" + std::to_string(shards.front()->Resyncs()) +
        ",\"strategy_invocations\":" +
        std::to_string(shards.front()->strategy_invocations()) + "}";
    if (!shards.front()->stream_error().ok()) {
      json.pop_back();
      const ErrorCode code = CodeOf(shards.front()->stream_error());
      json += ",\"market_stream_error\":{\"code\":" +
              std::to_string(ErrorNumber(code)) +
              ",\"name\":" + EscapeJson(Info(code).name) +
              ",\"message\":" +
              EscapeJson(shards.front()->stream_error().message()) +
              "}}";
    }
    boost::asio::post(executor,
                      [ready, result, json = std::move(json)]() mutable {
                        *result = std::move(json);
                        ready->cancel();
                      });
  });
  boost::system::error_code ec;
  co_await ready->async_wait(
      boost::asio::redirect_error(boost::asio::use_awaitable, ec));
  if (result->has_value()) co_return std::move(**result);
  co_return Error(ErrorCode::kControlTimeout, "status query timed out");
}

boost::asio::awaitable<absl::StatusOr<OrderHistoryPage>> QuantServer::OrderHistoryAsync(
    OrderHistoryQuery query) {
  auto executor = co_await boost::asio::this_coro::executor;
  auto ready = std::make_shared<boost::asio::steady_timer>(executor);
  auto result = std::make_shared<std::optional<OrderHistoryPage>>();
  ready->expires_after(std::chrono::seconds(2));
  auto accepted = reader->TrySubmitAsync(
      std::move(query), executor, [ready, result](OrderHistoryPage page) mutable {
        *result = std::move(page);
        ready->cancel();
      });
  if (!accepted.ok()) co_return accepted;
  boost::system::error_code ec;
  co_await ready->async_wait(
      boost::asio::redirect_error(boost::asio::use_awaitable, ec));
  if (!result->has_value())
    co_return Error(ErrorCode::kControlTimeout, "history query timed out");
  if (!(**result).status.ok()) co_return (**result).status;
  co_return std::move(**result);
}

}  // namespace hquant
