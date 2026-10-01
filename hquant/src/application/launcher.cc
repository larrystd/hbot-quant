#include "application/launcher.h"

#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <filesystem>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <variant>
#include <vector>

#include "absl/status/status.h"
#include "application/control.h"
#include "base/error.h"
#include "base/net.h"
#include "boost/asio/co_spawn.hpp"
#include "boost/asio/detached.hpp"
#include "boost/asio/post.hpp"
#include "boost/asio/steady_timer.hpp"
#include "boost/asio/use_awaitable.hpp"
#include "market/binance_spot_feed.h"
#include "market/replay_feed.h"
#include "offline/history.h"
#include "offline/recorder.h"
#include "order/simulated_exchange.h"
#include "order/risk.h"
#include "service/shard.h"
#include "strategy/simple_pmm.h"

namespace hquant {
namespace {

absl::Status ApplyReplayInput(const ReplayInput& input,
                              const MarketConfig& market, ReplayClock& clock,
                              ShardRuntime& shard) {
  auto advanced = clock.Advance(input.stamp);
  if (!advanced.ok()) return advanced;
  const EventTime time{{}, clock.UtcNow(), clock.MonoNow()};
  if (const auto* subscribe = std::get_if<ReplaySubscribe>(&input.payload)) {
    shard.Subscribe(subscribe->stream_epoch);
  } else if (const auto* snapshot =
                 std::get_if<ReplaySnapshot>(&input.payload)) {
    BookSnapshot event{market.spec.market,
                       market.book_scale.scale_version,
                       snapshot->stream_epoch,
                       snapshot->last_sequence,
                       snapshot->bids,
                       snapshot->asks,
                       time};
    return shard.OnSnapshot(event);
  } else if (const auto* diff = std::get_if<ReplayDiff>(&input.payload)) {
    BookDiff event{market.spec.market,  market.book_scale.scale_version,
                   diff->stream_epoch,  diff->first_sequence,
                   diff->last_sequence, diff->bids,
                   diff->asks,          time};
    return shard.OnDiff(event);
  } else if (std::holds_alternative<ReplayTimer>(input.payload)) {
    auto result = shard.OnTimer(input.stamp);
    if (!result.ok()) return result.status();
  } else if (const auto* trade =
                 std::get_if<ReplayPublicTrade>(&input.payload)) {
    PublicTrade event{market.spec.market,   {},          trade->price_ticks,
                      trade->quantity_lots, trade->side, time};
    return shard.OnPublicTrade(event);
  }
  return absl::OkStatus();
}

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

absl::Status RunSimulatedBinanceEngine(const AppConfig& config,
                                  const std::string& state_dir) {
  if (config.mode != EngineMode::Simulated ||
      config.loop_mode != LoopMode::Blocking || config.accounts.size() != 1 ||
      config.market_specs.size() != 1 || config.strategy_configs.size() != 1 ||
      config.assignments.size() != 1) {
    return Error(
        ErrorCode::kLaunchMultipleNotSupported,
        "public Simulated v1 requires one blocking shard, account, market and "
        "strategy");
  }
  const auto& account = config.accounts.front();
  const auto& market = config.market_specs.front();
  const auto& strategy_config = config.strategy_configs.front();
  const auto& assignment = config.assignments.front();
  if (market.spec.market.exchange != ExchangeId("binance") ||
      strategy_config.account != account.account ||
      strategy_config.markets.size() != 1 ||
      strategy_config.markets.front() != market.spec.market ||
      assignment.shard.value >= 8) {
    return Error(ErrorCode::kLaunchMultipleNotSupported,
                 "public Simulated market or assignment mismatch");
  }
  std::error_code error;
  std::filesystem::create_directories(state_dir, error);
  if (error)
    return ErrorFromSystem(ErrorCode::kStateDirUnavailable, error, state_dir);
  const auto storage_path =
      std::filesystem::path(config.storage_path).is_absolute()
          ? std::filesystem::path(config.storage_path)
          : std::filesystem::path(state_dir) / config.storage_path;
  std::filesystem::create_directories(storage_path.parent_path(), error);
  if (error)
    return ErrorFromSystem(ErrorCode::kStateDirUnavailable, error,
                           storage_path.parent_path().string());

  SystemClock clock;
  const UtcTime started = clock.UtcNow();
  const MonoTime started_mono = clock.MonoNow();
  const RunId run{static_cast<uint64_t>(started.time_since_epoch().count())};
  auto rule = market.trading_rule;
  rule.revision = 1;
  rule.observed_at = started;
  SimplePmm strategy({strategy_config.strategy_id, account.account, market.spec,
                      strategy_config.order_amount, strategy_config.bid_spread,
                      strategy_config.ask_spread,
                      strategy_config.refresh_interval, PmmPriceType::Mid,
                      *Decimal::Parse("0.001"), true});
  SimpleSimulatedExchange sim_exchange(
      {account.account, market.spec, rule, account.initial_balances,
       *Decimal::Parse("0.001"), true,
       [run, next_id = uint64_t{1}](Side) mutable {
         return ClientOrderId("P" + std::to_string(run.value) + "-" +
                              std::to_string(next_id++));
       }},
      clock);
  RiskGate risk(
      {assignment.shard, *Decimal::Parse("0"), std::chrono::hours(24)});
  for (const auto& lease : config.static_risk_leases) {
    auto status = risk.SetInitialLease({lease.account, lease.asset, lease.shard,
                                        1, lease.hard_limit,
                                        started + std::chrono::hours(24)});
    if (!status.ok()) return status;
  }
  auto recorder =
      SqliteRecorder::Open({storage_path.string(), run, started, 1024, 64});
  if (!recorder.ok()) return recorder.status();
  ShardRuntime shard(
      {run, assignment.shard, strategy_config.strategy_id, account.account,
       market.spec, market.book_scale, rule, 5'000'000},
      clock, strategy, sim_exchange, risk, **recorder);
  auto reader = HistoryReader::Open({storage_path.string(), 32, 500});
  if (!reader.ok()) return reader.status();

  boost::asio::io_context io;
  const TlsConfig rest_tls{true, true, {}, "data-api.binance.vision"};
  const TlsConfig ws_tls{true, true, {}, "data-stream.binance.vision"};
  HttpClient http(io, "data-api.binance.vision", "443", rest_tls);
  std::string symbol = market.spec.market.native_symbol;
  std::transform(
      symbol.begin(), symbol.end(), symbol.begin(),
      [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
  WebSocketClient websocket(
      io, "data-stream.binance.vision", "443",
      "/stream?streams=" + symbol + "@depth/" + symbol + "@trade", ws_tls);
  binance_spot::StreamConfig stream_config;
  stream_config.symbol = market.spec.market.native_symbol;
  absl::Status stream_error;
  binance_spot::MarketDataStream stream(
      stream_config,
      binance_spot::DepthParser(market.spec.market, market.book_scale), http,
      websocket, shard.MutableBookSync(), clock,
      {[&](const BookApplyResult& result) {
         if (result.state == BookSyncState::Live)
           stream_error = absl::OkStatus();
         auto status = shard.OnBookApplied();
         if (!status.ok()) {
           stream_error = status;
           risk.EmergencyStop();
         }
       },
       [&](const PublicTrade& trade) {
         auto status = shard.OnPublicTrade(trade);
         if (!status.ok()) {
           stream_error = status;
           risk.EmergencyStop();
         }
       },
       [&](const absl::Status& status) { stream_error = status; }});
  std::atomic<bool> stopping{false};
  boost::asio::co_spawn(io, stream.Run(), boost::asio::detached);
  boost::asio::co_spawn(
      io,
      [&]() -> boost::asio::awaitable<void> {
        boost::asio::steady_timer timer(io);
        uint64_t ordinal = 0;
        while (!stopping) {
          timer.expires_after(std::chrono::seconds(1));
          boost::system::error_code ec;
          co_await timer.async_wait(
              boost::asio::redirect_error(boost::asio::use_awaitable, ec));
          if (ec || stopping) break;
          const auto elapsed = clock.MonoNow() - started_mono;
          const auto at_us = elapsed.count();
          auto result = shard.OnTimer({at_us, ++ordinal});
          if (!result.ok()) {
            stream_error = result.status();
            risk.EmergencyStop();
          }
        }
      },
      boost::asio::detached);
  std::thread shard_thread([&] { io.run(); });

  std::mutex history_mutex;
  auto handler = [&](const ControlRequest& request) -> ControlResponse {
    ControlResponse response;
    response.request_id = request.request_id;
    if (std::holds_alternative<StatusRequest>(request.payload)) {
      auto result = std::make_shared<std::promise<std::string>>();
      auto ready = result->get_future();
      boost::asio::post(io, [&, result] {
        auto json = StatusJson(shard, sim_exchange, market.spec, **recorder);
        if (!stream_error.ok()) {
          json.pop_back();
          const ErrorCode code = CodeOf(stream_error);
          json += ",\"market_stream_error\":{\"code\":" +
                  std::to_string(ErrorNumber(code)) +
                  ",\"name\":" + EscapeJson(Info(code).name) +
                  ",\"message\":" + EscapeJson(stream_error.message()) + "}}";
        }
        result->set_value(std::move(json));
      });
      if (ready.wait_for(std::chrono::seconds(2)) !=
          std::future_status::ready) {
        response.payload =
            ControlError{ErrorCode::kControlTimeout, "status query timed out"};
      } else {
        response.payload = StatusResponse{ready.get()};
      }
    } else if (const auto* history =
                   std::get_if<HistoryRequest>(&request.payload)) {
      std::lock_guard lock(history_mutex);
      HistoryQuery query;
      query.request_id = request.request_id;
      query.page_size = history->limit;
      if (!history->cursor.empty()) query.cursor = history->cursor;
      auto accepted = (**reader).TrySubmit(query);
      if (!accepted.ok()) {
        response.payload =
            ControlError{CodeOf(accepted), std::string(accepted.message())};
      } else {
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(2);
        while (std::chrono::steady_clock::now() < deadline) {
          if (auto page = (**reader).TryReceive()) {
            if (!page->status.ok()) {
              response.payload = ControlError{
                  CodeOf(page->status), std::string(page->status.message())};
            } else
              response.payload = HistoryResponse{HistoryJson(*page)};
            return response;
          }
          std::this_thread::sleep_for(std::chrono::milliseconds(2));
        }
        response.payload =
            ControlError{ErrorCode::kControlTimeout, "history query timed out"};
      }
    } else {
      stopping = true;
      boost::asio::post(io, [&] {
        stream.Stop();
        io.stop();
      });
      response.payload = StopResponse{true};
    }
    return response;
  };
  auto server = ControlServer::Start(state_dir + "/control.sock", handler);
  if (!server.ok()) {
    stopping = true;
    boost::asio::post(io, [&] {
      stream.Stop();
      io.stop();
    });
    shard_thread.join();
    return server.status();
  }
  while (!stopping) std::this_thread::sleep_for(std::chrono::milliseconds(20));
  (*server)->Stop();
  shard_thread.join();
  return (*recorder)->Stop(clock.UtcNow());
}

}  // namespace

absl::Status Launch(const AppConfig& config, const std::string& state_dir) {
  if (config.market_data_source == MarketDataSource::BinancePublic) {
    return RunSimulatedBinanceEngine(config, state_dir);
  }
  if (config.mode != EngineMode::Simulated || config.accounts.size() != 1 ||
      config.market_specs.size() != 1 || config.strategy_configs.size() != 1 ||
      config.assignments.size() != 1 || !config.replay_fixture) {
    return Error(
        ErrorCode::kLaunchMultipleNotSupported,
        "G1 replay requires one Simulated account, market, strategy and shard");
  }
  const auto& account = config.accounts.front();
  const auto& market = config.market_specs.front();
  const auto& strategy_config = config.strategy_configs.front();
  const auto& assignment = config.assignments.front();
  if (strategy_config.account != account.account ||
      strategy_config.markets.size() != 1 ||
      strategy_config.markets.front() != market.spec.market ||
      assignment.shard.value >= 8) {
    return Error(ErrorCode::kLaunchMultipleNotSupported,
                 "G1 account, market or shard assignment mismatch");
  }
  std::error_code error;
  std::filesystem::create_directories(state_dir, error);
  if (error)
    return ErrorFromSystem(ErrorCode::kStateDirUnavailable, error, state_dir);
  const auto storage_path =
      std::filesystem::path(config.storage_path).is_absolute()
          ? std::filesystem::path(config.storage_path)
          : std::filesystem::path(state_dir) / config.storage_path;
  std::filesystem::create_directories(storage_path.parent_path(), error);
  if (error)
    return ErrorFromSystem(ErrorCode::kStateDirUnavailable, error,
                           storage_path.parent_path().string());

  const UtcTime origin(std::chrono::microseconds(1'000'000));
  const RunId run{static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count())};
  ReplayClock clock(origin, MonoTime(std::chrono::microseconds(1'000'000)));
  auto rule = market.trading_rule;
  rule.observed_at = origin;
  rule.revision = 1;
  SimplePmm strategy({strategy_config.strategy_id, account.account, market.spec,
                      strategy_config.order_amount, strategy_config.bid_spread,
                      strategy_config.ask_spread,
                      strategy_config.refresh_interval, PmmPriceType::Mid,
                      *Decimal::Parse("0.001"), true});
  SimpleSimulatedExchange sim_exchange(
      {account.account, market.spec, rule, account.initial_balances,
       *Decimal::Parse("0.001"), true,
       [run, next_id = uint64_t{1}](Side) mutable {
         return ClientOrderId("P" + std::to_string(run.value) + "-" +
                              std::to_string(next_id++));
       }},
      clock);
  RiskGate risk(
      {assignment.shard, *Decimal::Parse("0"), std::chrono::seconds(300)});
  for (const auto& lease : config.static_risk_leases) {
    auto status = risk.SetInitialLease({lease.account, lease.asset, lease.shard,
                                        1, lease.hard_limit,
                                        origin + std::chrono::hours(1)});
    if (!status.ok()) return status;
  }
  auto recorder =
      SqliteRecorder::Open({storage_path.string(), run, origin, 1024, 64});
  if (!recorder.ok()) return recorder.status();
  ShardRuntime shard({run, assignment.shard, strategy_config.strategy_id,
                      account.account, market.spec, market.book_scale, rule},
                     clock, strategy, sim_exchange, risk, **recorder);
  auto replay =
      ReadReplayFile(*config.replay_fixture, [&](const ReplayInput& input) {
        return ApplyReplayInput(input, market, clock, shard);
      });
  if (!replay.ok()) return replay;
  auto flush = (*recorder)->Flush();
  if (!flush.ok()) return flush;
  auto reader = HistoryReader::Open({storage_path.string(), 32, 500});
  if (!reader.ok()) return reader.status();

  std::atomic<bool> stopping{false};
  std::mutex history_mutex;
  auto handler = [&](const ControlRequest& request) -> ControlResponse {
    ControlResponse response;
    response.request_id = request.request_id;
    if (std::holds_alternative<StatusRequest>(request.payload)) {
      response.payload =
          StatusResponse{StatusJson(shard, sim_exchange, market.spec, **recorder)};
    } else if (const auto* history =
                   std::get_if<HistoryRequest>(&request.payload)) {
      std::lock_guard lock(history_mutex);
      HistoryQuery query;
      query.request_id = request.request_id;
      query.page_size = history->limit;
      if (!history->cursor.empty()) query.cursor = history->cursor;
      auto accepted = (**reader).TrySubmit(query);
      if (!accepted.ok()) {
        response.payload =
            ControlError{CodeOf(accepted), std::string(accepted.message())};
      } else {
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(2);
        while (std::chrono::steady_clock::now() < deadline) {
          if (auto page = (**reader).TryReceive()) {
            if (!page->status.ok()) {
              response.payload = ControlError{
                  CodeOf(page->status), std::string(page->status.message())};
            } else
              response.payload = HistoryResponse{HistoryJson(*page)};
            return response;
          }
          std::this_thread::sleep_for(std::chrono::milliseconds(2));
        }
        response.payload =
            ControlError{ErrorCode::kControlTimeout, "history query timed out"};
      }
    } else {
      stopping = true;
      response.payload = StopResponse{true};
    }
    return response;
  };
  auto server = ControlServer::Start(state_dir + "/control.sock", handler);
  if (!server.ok()) return server.status();
  while (!stopping) std::this_thread::sleep_for(std::chrono::milliseconds(20));
  (*server)->Stop();
  return (*recorder)->Stop(clock.UtcNow());
}

}  // namespace hquant
