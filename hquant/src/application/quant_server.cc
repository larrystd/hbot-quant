#include "application/quant_server.h"

#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <variant>

#include "application/control_server.h"
#include "base/error.h"
#include "base/net.h"
#include "boost/asio/co_spawn.hpp"
#include "boost/asio/detached.hpp"
#include "boost/asio/post.hpp"
#include "boost/asio/steady_timer.hpp"
#include "boost/asio/this_coro.hpp"
#include "boost/asio/use_awaitable.hpp"
#include "market/market_data_stream.h"
#include "market/replay_feed.h"
#include "order/risk.h"
#include "order/simulated_exchange.h"
#include "shard/shard.h"
#include "storage/history.h"
#include "storage/recorder.h"
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
  auto advanced = clock.Advance(input.stamp);
  if (!advanced.ok()) return advanced;
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

}  // namespace

struct QuantServer::Impl {
  AppConfig config;
  std::string state_dir;
  std::string storage_path;
  bool live = false;
  UtcTime started{};
  MonoTime started_mono{};
  RunId run{};
  std::unique_ptr<Clock> clock;
  ReplayClock* replay_clock = nullptr;
  std::unique_ptr<SimplePmm> strategy;
  std::unique_ptr<SimpleSimulatedExchange> exchange;
  std::unique_ptr<RiskGate> risk;
  std::unique_ptr<SqliteHistoryWriter> writer;
  std::unique_ptr<Shard> shard;
  std::unique_ptr<SqliteHistoryReader> reader;
  std::unique_ptr<boost::asio::io_context> io;
  std::unique_ptr<HttpClient> http;
  std::unique_ptr<WebSocketClient> websocket;
  std::unique_ptr<binance_spot::MarketDataStream> stream;
  std::thread shard_thread;
  std::mutex stop_mutex;
  std::condition_variable stop_cv;
  std::atomic<bool> stopping{false};
  bool started_server = false;
  bool waited = false;
  absl::Status stream_error;
  // Last member: destroy the endpoint before its handler's dependencies.
  std::unique_ptr<ControlServer> control_server;

  absl::Status Assemble() {
    const auto& account = config.accounts.front();
    const auto& market = config.market_specs.front();
    const auto& strategy_config = config.strategy_configs.front();
    const auto& assignment = config.assignments.front();
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
    started_mono = clock->MonoNow();
    run = RunId{static_cast<uint64_t>(
        live ? started.time_since_epoch().count()
             : std::chrono::duration_cast<std::chrono::microseconds>(
                   std::chrono::system_clock::now().time_since_epoch())
                   .count())};
    auto rule = market.trading_rule;
    rule.revision = 1;
    rule.observed_at = started;
    strategy = std::make_unique<SimplePmm>(SimplePmmConfig{
        strategy_config.strategy_id, account.account, market.spec,
        strategy_config.order_amount, strategy_config.bid_spread,
        strategy_config.ask_spread, strategy_config.refresh_interval,
        strategy_config.price_type, strategy_config.maker_fee_rate, true,
        strategy_config.timer_period});
    exchange = std::make_unique<SimpleSimulatedExchange>(
        SimulatedExchangeConfig{
            account.account, market.spec, rule, account.initial_balances,
            config.simulated_exchange.maker_fee_rate, true,
            [run = run, next_id = uint64_t{1}](Side) mutable {
              return ClientOrderId("P" + std::to_string(run.value) + "-" +
                                   std::to_string(next_id++));
            }},
        *clock);
    risk = std::make_unique<RiskGate>(RiskGate::Settings{
        assignment.shard, config.risk.fee_buffer_rate,
        config.risk.max_rule_age
            ? std::chrono::duration_cast<std::chrono::seconds>(
                  *config.risk.max_rule_age)
            : (live ? std::chrono::hours(24) : std::chrono::seconds(300))});
    for (const auto& budget : config.risk_budgets) {
      auto status = risk->SetInitialBudget(
          {budget.account, budget.asset, budget.shard, 1, budget.hard_limit,
           started + budget.valid_for.value_or(live ? std::chrono::hours(24)
                                                    : std::chrono::hours(1))});
      if (!status.ok()) return status;
    }
    auto opened = SqliteHistoryWriter::Open({storage_path, run, started,
                                             config.storage.writer_queue,
                                             config.storage.writer_batch});
    if (!opened.ok()) return opened.status();
    writer = std::move(*opened);
    Shard::Config shard_config{run,
                               assignment.shard,
                               strategy_config.strategy_id,
                               account.account,
                               market.spec,
                               market.tick_lot_size,
                               rule};
    shard_config.stale_after_us = market.stale_after
                                      ? market.stale_after->count()
                                      : (live ? 5'000'000 : 60'000'000);
    shard = std::make_unique<Shard>(shard_config, *clock, *strategy, *exchange,
                                    *risk, *writer);
    if (!live) {
      auto replay =
          ReadReplayFile(*config.replay_fixture, [&](const ReplayInput& input) {
            return ApplyReplayInput(input, market, *replay_clock, *shard);
          });
      if (!replay.ok()) return replay;
      auto flush = writer->Flush();
      if (!flush.ok()) return flush;
    }
    auto read =
        SqliteHistoryReader::Open({storage_path, config.storage.reader_queue,
                                   config.storage.reader_page_limit});
    if (!read.ok()) return read.status();
    reader = std::move(*read);
    return absl::OkStatus();
  }

  boost::asio::awaitable<void> TimerLoop() {
    boost::asio::steady_timer timer(*io);
    uint64_t ordinal = 0;
    while (!stopping) {
      timer.expires_after(*strategy->Triggers().timer_period);
      boost::system::error_code ec;
      co_await timer.async_wait(
          boost::asio::redirect_error(boost::asio::use_awaitable, ec));
      if (ec || stopping) break;
      const auto at_us = (clock->MonoNow() - started_mono).count();
      auto result = shard->OnTimer({at_us, ++ordinal});
      if (!result.ok()) {
        stream_error = result.status();
        risk->EmergencyStop();
      }
    }
  }

  void StartFeed() {
    const auto& market = config.market_specs.front();
    io = std::make_unique<boost::asio::io_context>();
    const auto& rest = config.binance_endpoints.rest;
    const auto& ws = config.binance_endpoints.websocket;
    const TlsConfig rest_tls{rest.tls, true, {}, rest.host};
    const TlsConfig ws_tls{ws.tls, true, {}, ws.host};
    http = std::make_unique<HttpClient>(*io, rest.host,
                                        std::to_string(rest.port), rest_tls);
    std::string symbol = market.spec.market.native_symbol;
    std::transform(
        symbol.begin(), symbol.end(), symbol.begin(),
        [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    websocket = std::make_unique<WebSocketClient>(
        *io, ws.host, std::to_string(ws.port),
        "/stream?streams=" + symbol + "@depth/" + symbol + "@trade", ws_tls);
    binance_spot::StreamConfig stream_config;
    stream_config.symbol = market.spec.market.native_symbol;
    stream = std::make_unique<binance_spot::MarketDataStream>(
        stream_config,
        binance_spot::DepthParser(market.spec.market, market.tick_lot_size),
        *http, *websocket, shard->MutableBookSync(), *clock,
        binance_spot::StreamCallbacks{
            [&](const BookApplyResult& result) {
              if (result.state == BookSyncState::Live)
                stream_error = absl::OkStatus();
              auto status = shard->OnBookApplied(result);
              if (!status.ok()) {
                stream_error = status;
                risk->EmergencyStop();
              }
            },
            [&](const PublicTrade& trade) {
              auto status = shard->OnPublicTrade(trade);
              if (!status.ok()) {
                stream_error = status;
                risk->EmergencyStop();
              }
            },
            [&](const absl::Status& status) { stream_error = status; }});
    boost::asio::co_spawn(*io, stream->Run(), boost::asio::detached);
    boost::asio::co_spawn(*io, TimerLoop(), boost::asio::detached);
    shard_thread = std::thread([&] { io->run(); });
  }
};

QuantServer::QuantServer(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}

absl::StatusOr<std::unique_ptr<QuantServer>> QuantServer::Create(
    const AppConfig& config, std::string state_dir) {
  const bool live =
      config.market_data_source == MarketDataSource::BinancePublic;
  if (config.mode != EngineMode::Simulated || config.accounts.size() != 1 ||
      config.market_specs.size() != 1 || config.strategy_configs.size() != 1 ||
      config.assignments.size() != 1 ||
      (live && config.loop_mode != LoopMode::Blocking) ||
      (!live && !config.replay_fixture)) {
    return Error(ErrorCode::kLaunchMultipleNotSupported,
                 live ? "public Simulated v1 requires one blocking shard, "
                        "account, market and strategy"
                      : "G1 replay requires one Simulated account, market, "
                        "strategy and shard");
  }
  const auto& account = config.accounts.front();
  const auto& market = config.market_specs.front();
  const auto& strategy = config.strategy_configs.front();
  const auto& assignment = config.assignments.front();
  if ((live && market.spec.market.exchange != ExchangeId("binance")) ||
      strategy.account != account.account || strategy.markets.size() != 1 ||
      strategy.markets.front() != market.spec.market ||
      assignment.shard.value >= 8) {
    return Error(ErrorCode::kLaunchMultipleNotSupported,
                 live ? "public Simulated market or assignment mismatch"
                      : "G1 account, market or shard assignment mismatch");
  }
  auto impl = std::make_unique<Impl>();
  impl->config = config;
  impl->state_dir = std::move(state_dir);
  std::error_code error;
  std::filesystem::create_directories(impl->state_dir, error);
  if (error)
    return ErrorFromSystem(ErrorCode::kStateDirUnavailable, error,
                           impl->state_dir);
  const auto path =
      std::filesystem::path(config.storage_path).is_absolute()
          ? std::filesystem::path(config.storage_path)
          : std::filesystem::path(impl->state_dir) / config.storage_path;
  std::filesystem::create_directories(path.parent_path(), error);
  if (error)
    return ErrorFromSystem(ErrorCode::kStateDirUnavailable, error,
                           path.parent_path().string());
  impl->storage_path = path.string();
  return std::unique_ptr<QuantServer>(new QuantServer(std::move(impl)));
}

QuantServer::~QuantServer() {
  if (impl_->started_server && !impl_->waited) {
    RequestStop();
    (void)Wait();
  }
  if (impl_->shard_thread.joinable()) impl_->shard_thread.join();
}

absl::Status QuantServer::Start() {
  auto status = impl_->Assemble();
  if (!status.ok()) return status;
  if (impl_->live) impl_->StartFeed();
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
                   std::get_if<HistoryRequest>(&request.payload)) {
      HistoryQuery query;
      query.request_id = request.request_id;
      query.page_size = history->limit;
      if (!history->cursor.empty()) query.cursor = history->cursor;
      auto result = co_await HistoryAsync(std::move(query));
      if (result.ok())
        response.payload = HistoryResponse{HistoryJson(*result)};
      else
        response.payload = ControlError{CodeOf(result.status()),
                                        std::string(result.status().message())};
    } else {
      RequestStop();
      response.payload = StopResponse{true};
    }
    co_return response;
  };
  auto opened = ControlServer::Start(impl_->state_dir + "/control.sock",
                                     handler, [this](int) { RequestStop(); });
  if (!opened.ok()) {
    RequestStop();
    if (impl_->shard_thread.joinable()) impl_->shard_thread.join();
    return opened.status();
  }
  impl_->control_server = std::move(*opened);
  impl_->started_server = true;
  return absl::OkStatus();
}

absl::Status QuantServer::Wait() {
  if (impl_->waited) return absl::OkStatus();
  std::unique_lock lock(impl_->stop_mutex);
  impl_->stop_cv.wait(lock, [&] { return impl_->stopping.load(); });
  lock.unlock();
  if (impl_->control_server) impl_->control_server->Stop(false);
  if (impl_->shard_thread.joinable()) impl_->shard_thread.join();
  auto stopped = impl_->writer->Stop(impl_->clock->UtcNow());
  impl_->reader.reset();
  if (impl_->control_server) {
    impl_->control_server->Stop();
    impl_->control_server.reset();
  }
  impl_->waited = true;
  return stopped;
}

void QuantServer::RequestStop() {
  if (impl_->stopping.exchange(true)) return;
  if (impl_->io) {
    boost::asio::post(*impl_->io, [this] {
      if (impl_->stream) impl_->stream->Stop();
      impl_->io->stop();
    });
  }
  impl_->stop_cv.notify_all();
}

boost::asio::awaitable<absl::StatusOr<std::string>> QuantServer::StatusAsync() {
  const auto& market = impl_->config.market_specs.front();
  if (!impl_->live) {
    co_return StatusJson(*impl_->shard, *impl_->exchange, market.spec,
                         *impl_->writer);
  }
  auto executor = co_await boost::asio::this_coro::executor;
  auto ready = std::make_shared<boost::asio::steady_timer>(executor);
  auto result = std::make_shared<std::optional<absl::StatusOr<std::string>>>();
  ready->expires_after(std::chrono::seconds(2));
  boost::asio::post(*impl_->io, [this, ready, result, executor] {
    auto json =
        StatusJson(*impl_->shard, *impl_->exchange,
                   impl_->config.market_specs.front().spec, *impl_->writer);
    json.pop_back();
    json +=
        ",\"applied_diffs\":" + std::to_string(impl_->stream->AppliedDiffs()) +
        ",\"resyncs\":" + std::to_string(impl_->stream->Resyncs()) +
        ",\"strategy_invocations\":" +
        std::to_string(impl_->shard->strategy_invocations()) + "}";
    if (!impl_->stream_error.ok()) {
      json.pop_back();
      const ErrorCode code = CodeOf(impl_->stream_error);
      json += ",\"market_stream_error\":{\"code\":" +
              std::to_string(ErrorNumber(code)) +
              ",\"name\":" + EscapeJson(Info(code).name) +
              ",\"message\":" + EscapeJson(impl_->stream_error.message()) +
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

boost::asio::awaitable<absl::StatusOr<HistoryPage>> QuantServer::HistoryAsync(
    HistoryQuery query) {
  auto executor = co_await boost::asio::this_coro::executor;
  auto ready = std::make_shared<boost::asio::steady_timer>(executor);
  auto result = std::make_shared<std::optional<HistoryPage>>();
  ready->expires_after(std::chrono::seconds(2));
  auto accepted = impl_->reader->TrySubmitAsync(
      std::move(query), executor, [ready, result](HistoryPage page) mutable {
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
