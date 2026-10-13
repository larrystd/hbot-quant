#include "hquant/shard/shard.h"

#include <algorithm>
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
#include "hquant/base/log.h"
#include "hquant/trading/client_order_id.h"

namespace hquant::v1 {

Shard::Shard(boost::asio::io_context& io, const ShardConfig& config,
             const InputConfig& input, const AccountConfig& account_config,
             ShardAllocation allocation, ShardPerf* perf,
             TradingAccount* account)
    : config_(config),
      input_(input),
      io_(io),
      work_guard_(boost::asio::make_work_guard(io_)),
      market_(
          io_, config_.market, input_,
          [this](const MarketNotice& notice) { OnMarket(notice); }, perf),
      strategy_(config_.strategy),
      trading_(config_, std::move(allocation), account_config.risk,
               account_config.maker_fee_rate_ppb),
      account_(account),
      strategy_timer_(io_),
      stop_timer_(io_),
      perf_(perf) {}

Shard::AssetPair Shard::ReadAssetViews() const {
  AssetPair values;
  values.base.total_nanos = trading_.Total(config_.market.base_asset);
  values.base.available_nanos =
      trading_.Available(config_.market.base_asset);
  values.base.budget_nanos = trading_.Budget(config_.market.base_asset);
  values.quote.total_nanos = trading_.Total(config_.market.quote_asset);
  values.quote.available_nanos =
      trading_.Available(config_.market.quote_asset);
  values.quote.budget_nanos = trading_.Budget(config_.market.quote_asset);
  return values;
}

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

absl::StatusOr<uint64_t> Shard::RunStrategyDecision(LiveHandlerPhases*,
                                                    uint64_t source_bench_seq) {
  if (stop_requested_) return uint64_t{0};
  if (!account_)
    return absl::FailedPreconditionError("trading account missing");
  return RunAccountDecision(source_bench_seq);
}

absl::StatusOr<uint64_t> Shard::RunAccountDecision(uint64_t source_bench_seq) {
  if (!account_ || trading_.Pending() || !account_view_.ready ||
      !account_->AdmissionEnabled(account_view_.revision)) {
    return uint64_t{0};
  }
  const MonoTime now =
      input_.source == InputSource::Replay && last_input_time_
          ? MonoTime(std::chrono::microseconds(last_input_time_->at_us))
          : std::chrono::time_point_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now());
  ExecutionView view;
  view.active_order_ids = trading_.OpenOrders();
  const AssetPair assets = ReadAssetViews();
  view.base_available_nanos = assets.base.available_nanos;
  view.quote_available_nanos = assets.quote.available_nanos;
  view.base_budget_nanos = assets.base.budget_nanos;
  view.quote_budget_nanos = assets.quote.budget_nanos;
  view.maker_fee_rate_ppb = account_view_.maker_fee_rate_ppb;
  view.price_per_tick_nanos = config_.market.price_per_tick_nanos;
  view.price_increment_nanos = account_view_.rule.price_increment_nanos;
  view.base_increment_nanos = account_view_.rule.base_increment_nanos;
  view.min_base_amount_nanos = account_view_.rule.min_base_amount_nanos;
  view.min_order_value_nanos = account_view_.rule.min_order_value_nanos;
  const uint64_t strategy_begin = perf_ ? PerfNowNs() : 0;
  auto decision = strategy_.Decide(market_.View(), view, now);
  const uint64_t strategy_elapsed = perf_ ? PerfNowNs() - strategy_begin : 0;
  if (!decision.ok()) return decision.status();
  if (std::holds_alternative<NoAction>(*decision)) return uint64_t{0};
  if (perf_) {
    auto& samples = source_bench_seq == 0
                        ? perf_->untagged_action_strategy_samples_ns
                        : perf_->market_action_strategy_samples_ns;
    samples.push_back(strategy_elapsed);
    if (std::holds_alternative<SubmitOrders>(*decision)) {
      auto& submit_samples =
          source_bench_seq == 0
              ? perf_->untagged_submit_strategy_samples_ns
              : perf_->market_submit_strategy_samples_ns;
      submit_samples.push_back(strategy_elapsed);
    }
  }
  uint64_t sent = 0;
  if (const auto* cancel = std::get_if<CancelOrders>(&*decision)) {
    awaiting_cancel_ = true;
    for (OrderId id : cancel->ids) {
      auto command = trading_.BeginCancel(id, source_bench_seq);
      if (!command.ok()) {
        strategy_.ResetRefresh();
        last_error_ = std::string(command.status().message());
        continue;
      }
      const TrackedOrder* order = trading_.FindLocal(id);
      const bool handled = account_->CancelFromShard(
          config_.id, *command, source_bench_seq,
          order ? order->cancel_requested_ms : 0);
      if (!handled) {
        (void)trading_.AbortCancel(command->client_order_id);
        strategy_.ResetRefresh();
        last_error_ = "account cancel unavailable";
        continue;
      }
      ++sent;
    }
    if (sent == 0) awaiting_cancel_ = false;
  } else {
    const auto& submit = std::get<SubmitOrders>(*decision);
    for (const LimitOrderRequest& request : submit.orders) {
      const uint64_t requested_ns = perf_ ? BenchClockNowNs() : 0;
      auto id = NewClientOrderId();
      if (!id.ok()) {
        strategy_.ResetRefresh();
        last_error_ = std::string(id.status().message());
        continue;
      }
      const uint64_t id_ready_ns = perf_ ? BenchClockNowNs() : 0;
      auto admitted =
          trading_.BeginSubmit(std::move(*id), request, source_bench_seq);
      if (!admitted.ok()) {
        strategy_.ResetRefresh();
        last_error_ = std::string(admitted.status().message());
        continue;
      }
      const uint64_t started_ns = perf_ ? BenchClockNowNs() : 0;
      const bool handled = account_->SubmitFromShard(
          admitted->order, admitted->asset, admitted->amount_nanos,
          account_view_.revision);
      if (!handled) {
        (void)trading_.AbortSubmit(admitted->order.client_order_id);
        strategy_.ResetRefresh();
        last_error_ = "account submit unavailable";
        continue;
      }
      if (perf_) {
        LOG_INFO(Logger(),
                 "shard_submit_trace client_id={} requested_ns={} "
                 "id_ready_ns={} started_ns={} finished_ns={} shard={} "
                 "bench_seq={}",
                 admitted->order.client_order_id.value, requested_ns,
                 id_ready_ns, started_ns, BenchClockNowNs(),
                 config_.id.value, source_bench_seq);
      }
      ++sent;
    }
  }
  return sent;
}

void Shard::PostTradeEvent(AccountEvent event) {
  boost::asio::post(io_, [this, event = std::move(event)]() mutable {
    if (auto status = trading_.ApplyEvent(event); !status.ok()) {
      if (account_) account_->ReportShardError(config_.id, status);
      StopFromError(status);
      return;
    }
    if (stopped_on_thread_) StopAccountOrders();
    if (awaiting_cancel_ && !trading_.Pending() &&
        trading_.OpenOrders().empty()) {
      awaiting_cancel_ = false;
      auto result = RunStrategyDecision();
      if (!result.ok()) StopFromError(result.status());
    }
    MaybeCompleteReplay();
  });
}

void Shard::PostTradeResponse(ClientOrderId id, bool cancel,
                              TradeResponse response) {
  boost::asio::post(io_, [this, id = std::move(id), cancel,
                          response = std::move(response)] {
    if (auto status = trading_.ApplyResponse(id, cancel, response);
        !status.ok()) {
      if (account_) account_->ReportShardError(config_.id, status);
      StopFromError(status);
      return;
    }
    if (stopped_on_thread_) StopAccountOrders();
    MaybeCompleteReplay();
  });
}

void Shard::PostTradeWriting(ClientOrderId id, bool cancel) {
  boost::asio::post(io_, [this, id = std::move(id), cancel] {
    if (auto status = trading_.MarkWriting(id, cancel); !status.ok()) {
      if (account_) account_->ReportShardError(config_.id, status);
      StopFromError(status);
    }
  });
}

void Shard::PostCancelAll(std::function<void(uint64_t)> done) {
  boost::asio::post(io_, [this, done = std::move(done)]() mutable {
    uint64_t admitted = 0;
    if (account_) {
      for (OrderId local_id : trading_.OpenOrders()) {
        const TrackedOrder* current = trading_.FindLocal(local_id);
        if (!current || current->cancel == CancelPhase::Pending ||
            current->cancel == CancelPhase::Unknown)
          continue;
        auto command = trading_.BeginCancel(local_id, 0);
        if (!command.ok()) continue;
        const TrackedOrder* order = trading_.FindLocal(local_id);
        if (account_->CancelFromShard(
                config_.id, *command, 0,
                order ? order->cancel_requested_ms : 0)) {
          ++admitted;
        } else {
          (void)trading_.AbortCancel(command->client_order_id);
        }
      }
    }
    done(admitted);
  });
}

absl::Status Shard::RestoreTradingJournal(
    const std::vector<JournalEntry>& entries) {
  return trading_.RestoreJournal(entries);
}

void Shard::PostAccountView(AccountView view) {
  boost::asio::post(io_, [this, view = std::move(view)]() mutable {
    OnAccountView(std::move(view));
  });
}

void Shard::OnAccountView(AccountView view) {
  if (view.revision < account_view_.revision) return;
  account_view_ = std::move(view);
  trading_.SetVenueRule(account_view_.venue_rule);
  if (stopped_on_thread_) {
    StopAccountOrders();
    MaybeCompleteReplay();
    return;
  }
  if (!stop_requested_ && awaiting_cancel_ && !trading_.Pending() &&
      trading_.OpenOrders().empty()) {
    awaiting_cancel_ = false;
    const uint64_t decision_begin = perf_ ? PerfNowNs() : 0;
    auto result = RunStrategyDecision();
    if (perf_) {
      const uint64_t elapsed = PerfNowNs() - decision_begin;
      perf_->decision_ns += elapsed;
      perf_->decision_samples_ns.push_back(elapsed);
      if (result.ok() && *result != 0)
        perf_->account_view_action_decision_samples_ns.push_back(elapsed);
    }
    if (!result.ok()) StopFromError(result.status());
  }
  if (awaiting_cancel_ && !trading_.Pending()) {
    for (OrderId id : trading_.OpenOrders()) {
      const TrackedOrder* order = trading_.FindLocal(id);
      if (order && order->cancel == CancelPhase::Rejected) {
        awaiting_cancel_ = false;
        break;
      }
    }
  }
  MaybeCompleteReplay();
}

void Shard::StopFromError(absl::Status error) {
  LOG_ERROR(Logger(), "shard stopped shard={} reason={}", config_.id.value,
            error.ToString());
  last_error_ = std::string(error.message());
  fatal_status_ = std::move(error);
  stop_requested_ = true;
  StopOnThread();
}

void Shard::OnMarket(const MarketNotice& notice) {
  LOG_TRACE_L1(Logger(), "market shard={} bench_seq={} kind={}",
               config_.id.value, notice.source_bench_seq,
               static_cast<int>(notice.kind));
  LiveHandlerPhases* phases =
      perf_ && perf_->bench_ack_fd >= 0 ? &perf_->live_handler_phases : nullptr;
  BenchPhaseTimer shard_timer(phases ? &phases->shard_total_ns : nullptr);
  ++market_notices_;
  if (notice.kind == MarketNotice::Kind::BookUnavailable) {
    last_error_ = market_.LastError();
  }
  if (stop_requested_) return;
  const uint64_t decision_begin = perf_ ? PerfNowNs() : 0;
  auto result = RunStrategyDecision(phases, notice.source_bench_seq);
  if (perf_) {
    const uint64_t elapsed = PerfNowNs() - decision_begin;
    perf_->decision_ns += elapsed;
    perf_->decision_samples_ns.push_back(elapsed);
    if (result.ok() && *result != 0)
      perf_->market_action_decision_samples_ns.push_back(elapsed);
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
    if (result.ok() && *result != 0)
      perf_->timer_action_decision_samples_ns.push_back(elapsed);
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
    auto complete = [this, handler_done_ns, done = std::move(done)]() mutable {
      if (perf_) {
        perf_->handler_to_completion_samples_ns.push_back(PerfNowNs() -
                                                          handler_done_ns);
      }
      if (fatal_status_) {
        done(*fatal_status_);
      } else if (stop_requested_) {
        done(absl::CancelledError("shard stopped"));
      } else {
        done(absl::OkStatus());
      }
    };
    replay_done_ = std::move(complete);
    MaybeCompleteReplay();
  });
}

void Shard::MaybeCompleteReplay() {
  if (!replay_done_) return;
  auto done = std::move(replay_done_);
  replay_done_ = {};
  boost::asio::post(io_, std::move(done));
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
  status.active_orders = trading_.OpenOrders().size();
  const AssetPair assets = ReadAssetViews();
  status.base_total_nanos = assets.base.total_nanos;
  status.base_available_nanos = assets.base.available_nanos;
  status.quote_total_nanos = assets.quote.total_nanos;
  status.quote_available_nanos = assets.quote.available_nanos;
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
    auto result = RunStrategyDecision();
    if (!result.ok()) {
      StopFromError(result.status());
      done(result.status());
      return;
    }
    done(StrategyUpdateResult{config_version_, *result});
  });
}

void Shard::StopOnThread() {
  if (stopped_on_thread_) return;
  stopped_on_thread_ = true;
  market_.Stop();
  strategy_timer_.cancel();
  stop_timer_.expires_after(std::chrono::seconds(30));
  stop_timer_.async_wait([this](const boost::system::error_code& error) {
    if (error) return;
    if (!trading_.OpenOrders().empty()) {
      last_error_ = "shutdown left unconfirmed account orders";
    }
    FinishStopOnThread();
  });
  StopAccountOrders();
}

void Shard::StopAccountOrders() {
  if (!stopped_on_thread_ || !account_) return;
  const auto open = trading_.OpenOrders();
  if (open.empty()) {
    FinishStopOnThread();
    return;
  }
  if (trading_.Pending()) return;
  for (OrderId id : open) {
    if (!stop_cancel_attempted_.insert(id.value).second) continue;
    auto command = trading_.BeginCancel(id, 0);
    if (!command.ok()) {
      stop_cancel_attempted_.erase(id.value);
      last_error_ = std::string(command.status().message());
      continue;
    }
    const TrackedOrder* order = trading_.FindLocal(id);
    if (!account_->CancelFromShard(config_.id, *command, 0,
                                   order ? order->cancel_requested_ms : 0)) {
      (void)trading_.AbortCancel(command->client_order_id);
      stop_cancel_attempted_.erase(id.value);
      last_error_ = "account shutdown cancel unavailable";
    }
  }
}

void Shard::FinishStopOnThread() {
  stop_timer_.cancel();
  work_guard_.reset();
  CompleteStop();
}

void Shard::RequestStop() {
  if (stop_requested_.exchange(true)) return;
  if (started_ && !worker_exited_) {
    boost::asio::post(io_, [this] { StopOnThread(); });
  } else {
    work_guard_.reset();
    CompleteStop();
  }
}

void Shard::PostStopped(std::function<void()> done) {
  {
    std::lock_guard lock(stop_completion_mutex_);
    if (!stop_complete_) {
      stop_waiters_.push_back(std::move(done));
      return;
    }
  }
  done();
}

void Shard::CompleteStop() {
  std::vector<std::function<void()>> waiters;
  {
    std::lock_guard lock(stop_completion_mutex_);
    if (stop_complete_) return;
    stop_complete_ = true;
    waiters.swap(stop_waiters_);
  }
  for (auto& done : waiters) done();
}

void Shard::Join() { worker_exited_ = true; }

}  // namespace hquant::v1
