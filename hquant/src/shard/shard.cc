#include "hquant/shard/shard.h"

#include <chrono>
#include <exception>
#include <limits>
#include <string>
#include <type_traits>
#include <utility>

#include "absl/status/status.h"
#include "boost/asio/co_spawn.hpp"
#include "boost/asio/detached.hpp"
#include "boost/asio/post.hpp"

namespace hquant::v1 {

static_assert(DecisionStrategy<Strategy>);

Shard::Shard(boost::asio::io_context& io, const ShardConfig& config,
             const InputConfig& input,
             SqliteHistory& history, std::string run_id, ShardPerf* perf)
    : config_(config),
      input_(input),
      history_(history),
      run_id_(std::move(run_id)),
      io_(io),
      work_guard_(boost::asio::make_work_guard(io_)),
      market_(io_, config_.market, input_,
              [this](const MarketNotice& notice) { OnMarket(notice); }, perf),
      strategy_(config_.strategy),
      exchange_(config_.market, config_.executor,
                [this](AccountEvent event) {
                  boost::asio::post(io_, [this, event = std::move(event)] {
                    OnAccountEvent(event);
                  });
                }),
      executor_(config_.market, config_.executor, exchange_),
      strategy_timer_(io_),
      perf_(perf) {}

Shard::~Shard() {
  RequestStop();
  Join();
}

absl::Status Shard::Start() {
  if (stop_requested_)
    return absl::FailedPreconditionError("shard already stopped");
  if (started_.exchange(true))
    return absl::FailedPreconditionError("shard already started");
  if (input_.source == InputSource::BinancePublic) {
    boost::asio::co_spawn(io_, market_.RunBinance(), boost::asio::detached);
    ScheduleStrategyTimer();
  }
  return absl::OkStatus();
}

int64_t Shard::EventAtUs() const {
  if (input_.source == InputSource::Replay && last_input_time_) {
    return last_input_time_->at_us;
  }
  return std::chrono::duration_cast<std::chrono::microseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

void Shard::Record(HistoryRecord record) {
  if (history_sequence_ ==
      static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
    last_error_ = "history sequence exhausted";
    return;
  }
  record.run_id = run_id_;
  record.shard = config_.id;
  record.sequence = ++history_sequence_;
  record.at_us = EventAtUs();
  if (perf_) {
    record.perf_input_origin_ns =
        current_input_origin_ns_ ? current_input_origin_ns_ : PerfNowNs();
  }
  const uint64_t enqueue_begin = perf_ ? PerfNowNs() : 0;
  if (!history_.TryPush(std::move(record))) {
    const auto health = history_.Health();
    last_error_ = health.error.empty() ? "history queue full" : health.error;
  }
  if (perf_) {
    ++perf_->history_records;
    const uint64_t elapsed = PerfNowNs() - enqueue_begin;
    perf_->history_enqueue_ns += elapsed;
    perf_->history_enqueue_samples_ns.push_back(elapsed);
  }
}

void Shard::OnAccountEvent(const AccountEvent& event) {
  const uint64_t begin = perf_ ? PerfNowNs() : 0;
  HistoryRecord record;
  std::visit(
      [&](const auto& value) {
        using T = std::decay_t<decltype(value)>;
        if constexpr (std::is_same_v<T, OrderReport>) {
          switch (value.state) {
            case OrderState::Rejected:
              record.kind = HistoryKind::OrderRejected;
              break;
            case OrderState::Accepted:
              record.kind = HistoryKind::OrderAccepted;
              break;
            case OrderState::PartiallyFilled:
              record.kind = HistoryKind::OrderPartiallyFilled;
              break;
            case OrderState::Filled:
              record.kind = HistoryKind::OrderFilled;
              break;
            case OrderState::Cancelled:
              record.kind = HistoryKind::OrderCancelled;
              break;
          }
          record.order_id = value.order.id.value;
          record.price_nanos = value.order.price_nanos;
          record.quantity_nanos = value.order.quantity_nanos;
          record.remaining_nanos = value.order.remaining_nanos;
          record.buy = value.order.side == Side::Buy;
          record.message = value.reason;
        } else if constexpr (std::is_same_v<T, Fill>) {
          record.kind = HistoryKind::Fill;
          record.order_id = value.order_id.value;
          record.buy = value.side == Side::Buy;
          record.trade_id = value.trade_id;
          record.price_nanos = value.price_nanos;
          record.quantity_nanos = value.quantity_nanos;
          record.fee_nanos = value.fee_nanos;
        } else if constexpr (std::is_same_v<T, BalanceUpdate>) {
          record.kind = HistoryKind::Balance;
          record.asset = value.asset;
          record.total_nanos = value.total_nanos;
          record.available_nanos = value.available_nanos;
        }
      },
      event);
  Record(std::move(record));
  if (perf_) {
    ++perf_->account_events;
    const uint64_t elapsed = PerfNowNs() - begin;
    perf_->account_event_ns += elapsed;
    perf_->account_event_samples_ns.push_back(elapsed);
  }
}

void Shard::RecordCommands(const std::vector<CommandResult>& commands) {
  for (const CommandResult& command : commands) {
    HistoryRecord record;
    record.kind = command.kind == CommandResult::Kind::Submit
                      ? HistoryKind::SubmitCommand
                      : HistoryKind::CancelCommand;
    record.order_id = command.order_id.value;
    record.price_nanos = command.price_nanos;
    record.quantity_nanos = command.quantity_nanos;
    record.dispatched = command.dispatched;
    record.buy = command.side == Side::Buy;
    record.message = command.reason;
    Record(std::move(record));
  }
}

absl::StatusOr<uint64_t> Shard::RunStrategyDecision(
    LiveHandlerPhases* phases) {
  if (stop_requested_) return uint64_t{0};
  const MonoTime now =
      input_.source == InputSource::Replay && last_input_time_
          ? MonoTime(std::chrono::microseconds(last_input_time_->at_us))
          : std::chrono::time_point_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now());
  return RunDecisionCycle(strategy_, executor_, market_.View(), now,
                          [this](const std::vector<CommandResult>& commands) {
                            RecordCommands(commands);
                          }, phases);
}

void Shard::StopFromError(absl::Status error) {
  last_error_ = std::string(error.message());
  fatal_status_ = std::move(error);
  stop_requested_ = true;
  StopOnThread();
}

void Shard::OnMarket(const MarketNotice& notice) {
  LiveHandlerPhases* phases = perf_ && perf_->bench_ack_fd >= 0
                                  ? &perf_->live_handler_phases
                                  : nullptr;
  BenchPhaseTimer shard_timer(phases ? &phases->shard_total_ns : nullptr);
  ++market_notices_;
  if (notice.kind == MarketNotice::Kind::BookUnavailable) {
    last_error_ = market_.LastError();
  }
  if (stop_requested_) return;
  // Only paper trading matches locally; live fills arrive from the exchange.
  if (config_.executor.mode == TradingMode::Paper) {
    const uint64_t match_begin = perf_ ? PerfNowNs() : 0;
    auto status = [&] {
      BenchPhaseTimer timer(phases ? &phases->paper_ns : nullptr);
      return exchange_.OnMarket(notice, market_.View());
    }();
    if (perf_) {
      const uint64_t elapsed = PerfNowNs() - match_begin;
      perf_->paper_match_ns += elapsed;
      perf_->paper_match_samples_ns.push_back(elapsed);
    }
    if (!status.ok()) {
      StopFromError(status);
      return;
    }
  }
  const uint64_t decision_begin = perf_ ? PerfNowNs() : 0;
  auto result = RunStrategyDecision(phases);
  if (perf_) {
    const uint64_t elapsed = PerfNowNs() - decision_begin;
    perf_->decision_ns += elapsed;
    perf_->decision_samples_ns.push_back(elapsed);
  }
  if (!result.ok()) StopFromError(result.status());
}

void Shard::OnStrategyTimer() {
  ++strategy_timer_ticks_;
  if (stop_requested_) return;
  const uint64_t decision_begin = perf_ ? PerfNowNs() : 0;
  auto result = RunStrategyDecision();
  if (perf_) {
    const uint64_t elapsed = PerfNowNs() - decision_begin;
    perf_->decision_ns += elapsed;
    perf_->decision_samples_ns.push_back(elapsed);
  }
  if (!result.ok()) StopFromError(result.status());
}

void Shard::ScheduleStrategyTimer() {
  if (stop_requested_) return;
  strategy_timer_.expires_after(
      std::chrono::milliseconds(config_.strategy.refresh_ms));
  strategy_timer_.async_wait([this](const boost::system::error_code& error) {
    if (error || stop_requested_) return;
    OnStrategyTimer();
    ScheduleStrategyTimer();
  });
}

void Shard::PostReplay(ReplayRecord record,
                       std::function<void(absl::Status)> done) {
  if (!started_ || stop_requested_) {
    done(absl::CancelledError("shard stopped"));
    return;
  }
  boost::asio::post(io_, [this, record = std::move(record),
                          done = std::move(done)]() mutable {
    if (stop_requested_) {
      done(absl::CancelledError("shard stopped"));
      return;
    }
    if (record.target != config_.id || record.time.at_us < 0 ||
        (last_input_time_ &&
         (record.time.at_us < last_input_time_->at_us ||
          (record.time.at_us == last_input_time_->at_us &&
           record.time.ordinal <= last_input_time_->ordinal)))) {
      done(absl::InvalidArgumentError("invalid replay target or time order"));
      return;
    }
    last_input_time_ = record.time;
    const uint64_t input_begin = perf_ ? PerfNowNs() : 0;
    if (perf_) {
      current_input_origin_ns_ = input_begin;
      if (record.perf_post_ns) {
        perf_->post_to_shard_samples_ns.push_back(input_begin -
                                                  record.perf_post_ns);
      }
    }
    const MonoTime now(std::chrono::microseconds(record.time.at_us));
    const uint64_t market_begin = perf_ ? PerfNowNs() : 0;
    if (const auto* input = std::get_if<MarketInput>(&record.body)) {
      market_.OnReplayInput(*input, now);
    } else if (!market_.CheckStale(now)) {
      OnStrategyTimer();
    }
    const uint64_t handler_done_ns = perf_ ? PerfNowNs() : 0;
    if (perf_) {
      perf_->market_path_ns += handler_done_ns - market_begin;
      ++perf_->inputs;
      const uint64_t elapsed = handler_done_ns - input_begin;
      perf_->input_dispatch_ns += elapsed;
      perf_->input_handler_samples_ns.push_back(elapsed);
    }
    // Account events generated by this input are queued before its replay
    // completion. This keeps deterministic replay while preserving the
    // independent private-event delivery path used by live execution.
    boost::asio::post(
        io_, [this, handler_done_ns, done = std::move(done)]() mutable {
          if (perf_) {
            perf_->handler_to_completion_samples_ns.push_back(PerfNowNs() -
                                                              handler_done_ns);
            current_input_origin_ns_ = 0;
          }
          if (fatal_status_) {
            done(*fatal_status_);
          } else if (stop_requested_) {
            done(absl::CancelledError("shard stopped"));
          } else {
            done(absl::OkStatus());
          }
        });
  });
}

ShardStatus Shard::CaptureStatus() const {
  ShardStatus status;
  status.id = config_.id;
  status.stop_requested = stop_requested_;
  status.worker_exited = worker_exited_;
  status.book_state = market_.View().state;
  status.connection_epoch = market_.ConnectionEpoch();
  status.last_book_sequence = market_.View().last_sequence.value_or(0);
  status.applied_depth_events = market_.AppliedDepthEvents();
  status.accepted_trades = market_.AcceptedTrades();
  status.last_trade_id = market_.LastTradeId();
  status.depth_digest = market_.DepthDigest();
  status.trade_digest = market_.TradeDigest();
  status.bids = market_.Bids();
  status.asks = market_.Asks();
  status.market_notices = market_notices_;
  status.strategy_timer_ticks = strategy_timer_ticks_;
  status.config_version = config_version_;
  status.active_orders = exchange_.ActiveOrders().size();
  status.base_total_nanos = exchange_.Total(config_.market.base_asset);
  status.base_available_nanos = exchange_.Available(config_.market.base_asset);
  status.quote_total_nanos = exchange_.Total(config_.market.quote_asset);
  status.quote_available_nanos =
      exchange_.Available(config_.market.quote_asset);
  status.last_error = last_error_.empty() ? market_.LastError() : last_error_;
  return status;
}

void Shard::PostStatus(std::function<void(ShardStatus)> done) {
  if (stop_requested_ || !started_) {
    ShardStatus status;
    status.id = config_.id;
    status.stop_requested = stop_requested_;
    status.worker_exited = worker_exited_;
    done(std::move(status));
    return;
  }
  boost::asio::post(
      io_, [this, done = std::move(done)]() mutable { done(CaptureStatus()); });
}

void Shard::PostStrategyUpdate(
    uint64_t expected_version, StrategyPatch patch,
    std::function<void(absl::StatusOr<StrategyUpdateResult>)> done) {
  if (!started_ || stop_requested_) {
    done(absl::CancelledError("shard stopped"));
    return;
  }
  boost::asio::post(io_, [this, expected_version, patch = std::move(patch),
                          done = std::move(done)]() mutable {
    if (stop_requested_) {
      done(absl::CancelledError("shard stopped"));
      return;
    }
    if (expected_version != config_version_) {
      done(absl::FailedPreconditionError("strategy config version mismatch"));
      return;
    }
    if (config_version_ == std::numeric_limits<uint64_t>::max()) {
      done(absl::OutOfRangeError("strategy config version exhausted"));
      return;
    }
    StrategyConfig candidate = config_.strategy;
    if (patch.order_amount_nanos)
      candidate.order_amount_nanos = *patch.order_amount_nanos;
    if (patch.bid_spread_ppb) candidate.bid_spread_ppb = *patch.bid_spread_ppb;
    if (patch.ask_spread_ppb) candidate.ask_spread_ppb = *patch.ask_spread_ppb;
    if (patch.refresh_ms) candidate.refresh_ms = *patch.refresh_ms;
    if (auto status = ValidateStrategyConfig(candidate); !status.ok()) {
      done(status);
      return;
    }
    config_.strategy = candidate;
    strategy_.UpdateConfig(candidate);
    ++config_version_;
    if (input_.source == InputSource::BinancePublic) {
      strategy_timer_.cancel();
      ScheduleStrategyTimer();
    }
    auto cancelled = executor_.CancelAll();
    if (!cancelled.ok()) {
      StopFromError(cancelled.status());
      done(cancelled.status());
      return;
    }
    const uint64_t cancelled_count = cancelled->size();
    RecordCommands(*cancelled);
    auto result = RunStrategyDecision();
    if (!result.ok()) {
      StopFromError(result.status());
      done(result.status());
      return;
    }
    done(StrategyUpdateResult{config_version_, cancelled_count + *result});
  });
}

void Shard::StopOnThread() {
  if (stopped_on_thread_) return;
  stopped_on_thread_ = true;
  market_.Stop();
  strategy_timer_.cancel();
  auto cancelled = executor_.CancelAll();
  if (cancelled.ok()) {
    RecordCommands(*cancelled);
  } else {
    last_error_ = std::string(cancelled.status().message());
  }
  work_guard_.reset();
}

void Shard::RequestStop() {
  if (stop_requested_.exchange(true)) return;
  if (started_ && !worker_exited_) {
    boost::asio::post(io_, [this] { StopOnThread(); });
  } else {
    work_guard_.reset();
  }
}

void Shard::Join() {
  worker_exited_ = true;
}

}  // namespace hquant::v1
