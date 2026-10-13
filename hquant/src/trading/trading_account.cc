#include "hquant/trading/trading_account.h"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <format>
#include <functional>
#include <limits>
#include <memory>
#include <set>
#include <string>
#include <type_traits>
#include <utility>

#include "absl/status/status.h"
#include "boost/asio/post.hpp"
#include "hquant/base/fixed.h"
#include "hquant/base/log.h"
#include "hquant/trading/account_sync.h"
#include "hquant/trading/account_view_publisher.h"

namespace hquant::v1 {
namespace {

const bool kTraceSubmitStages = [] {
  const char* value = std::getenv("HQUANT_SUBMIT_STAGE_TRACE");
  return value != nullptr && value[0] == '1' && value[1] == '\0';
}();

JournalEntry JournalForEvent(const AccountEvent& event) {
  JournalEntry entry;
  std::visit(
      [&entry](const auto& value) {
        using T = std::decay_t<decltype(value)>;
        if constexpr (std::is_same_v<T, OrderReport>) {
          entry.kind = JournalKind::OrderReport;
          entry.client_id = value.order.client_order_id.value;
          entry.symbol = value.order.market.symbol;
          entry.exchange_id = value.order.exchange_order_id.value;
          entry.side = value.order.side == Side::Buy ? 0 : 1;
          entry.price = value.order.price_nanos;
          entry.quantity = value.order.quantity_nanos;
          entry.remaining = value.order.remaining_nanos;
          entry.result = static_cast<uint64_t>(value.state);
          entry.message = value.reason;
          entry.transaction_ms = value.event_time_ms;
          entry.credit = value.authoritative_query;
        } else if constexpr (std::is_same_v<T, Fill>) {
          entry.kind = JournalKind::Fill;
          entry.client_id = value.client_order_id.value;
          entry.symbol = value.market.symbol;
          entry.side = value.side == Side::Buy ? 0 : 1;
          entry.trade_id = value.trade_id;
          entry.price = value.price_nanos;
          entry.quantity = value.quantity_nanos;
          entry.gross = value.gross_nanos;
          entry.fee = value.fee_nanos;
          entry.fee_asset = value.fee_asset;
          entry.transaction_ms = value.transaction_ms;
        } else if constexpr (std::is_same_v<T, BalanceUpdate>) {
          entry.kind = JournalKind::Balance;
          entry.asset = value.asset;
          entry.total = value.total_nanos;
          entry.available = value.available_nanos;
          entry.locked = value.locked_nanos;
          entry.transaction_ms = value.version_ms;
        } else {
          entry.kind = JournalKind::BalanceDelta;
          entry.asset = value.asset;
          entry.amount = value.amount_nanos;
          entry.credit = value.credit;
          entry.external_lock = value.external_lock;
          entry.transaction_ms = value.transaction_time_ms;
        }
      },
      event);
  return entry;
}

}  // namespace

TradingAccount::TradingAccount(boost::asio::io_context& io,
                               AccountConfig config,
                               std::vector<ShardConfig> shards,
                               std::string run_id, SqliteHistory* history,
                               size_t queue_capacity, TradePerf* trade_perf)
    : io_(io),
      work_guard_(boost::asio::make_work_guard(io_)),
      final_status_timer_(io_),
      order_query_timer_(io_),
      config_(std::move(config)),
      run_id_(std::move(run_id)),
      history_(history),
      trade_perf_(trade_perf),
      queue_capacity_(queue_capacity),
      funds_(config_.initial_balances_nanos) {
  for (const ShardConfig& shard : shards) {
    markets_.emplace(shard.id.value, shard.market);
  }
  ready_ = false;
  views_ = std::make_unique<AccountViewPublisher>(
      AccountViewPublisher::Inputs{
          config_, markets_, venue_rules_, funds_, order_manager_, ready_,
          replaying_,
          [this](const std::string& asset) { return Budget(asset); }},
      shards);
  sync_ = std::make_unique<AccountSync>(AccountSync::Inputs{
      io_, state_mutex_, config_, markets_, venue_rules_, funds_, order_manager_,
      journal_,
      [this] { return port_.get(); }, order_query_timer_, private_connected_,
      manual_paused_, quiescing_, ready_,
      [this] { DisableAdmission(); },
      [this] { EnableAdmission(); },
      [this] { return AccountQueueEmpty(); },
      [this] { return pending_private_events_.load(); }, error_,
      [this](AccountEvent event, bool from_venue,
             std::function<void(absl::Status)> done) {
        return PersistAndApply(std::move(event), from_venue, std::move(done));
      },
      [this](std::string error) { Pause(std::move(error)); },
      [this] {
        MarkAllDirty();
        FlushViews();
      },
      [this] { ScheduleOrderQuery(); }});
}

TradingAccount::~TradingAccount() = default;

void TradingAccount::SetPort(std::unique_ptr<TradePort> port) {
  port_ = std::move(port);
}

TradePort* TradingAccount::AddShardPort(std::unique_ptr<TradePort> port) {
  TradePort* result = port.get();
  extra_ports_.push_back(std::move(port));
  return result;
}

void TradingAccount::SetShardPort(ShardId shard, TradePort* port) {
  shard_ports_[shard.value] = port;
}

void TradingAccount::SetViewSink(ShardId shard, ViewSink sink) {
  views_->SetSink(shard, std::move(sink));
}

void TradingAccount::SetTradeSink(ShardId shard, TradeSink sink) {
  trade_sinks_[shard.value] = std::move(sink);
}

bool TradingAccount::SubmitFromShard(const TrackedOrder& order,
                                     std::string asset,
                                     uint64_t amount_nanos,
                                     uint64_t view_revision) {
  const bool trace_submit = trade_perf_ != nullptr && kTraceSubmitStages;
  const uint64_t entered_ns = trace_submit ? BenchClockNowNs() : 0;
  std::shared_lock admission_lock(admission_gate_mutex_);
  if (order.shard.value >= kMaxShards) return false;
  TradePort* const port = shard_ports_[order.shard.value]
                              ? shard_ports_[order.shard.value]
                              : port_.get();
  const auto market_it = markets_.find(order.shard.value);
  if (!AdmissionEnabled(view_revision) || !port || !journal_ ||
      market_it == markets_.end() || market_it->second.id != order.market)
    return false;
  if (config_.mode == TradingMode::OnlineSim) {
    uint64_t submissions = demo_submissions_.load();
    do {
      if (submissions >= config_.max_submissions_per_run) return false;
    } while (!demo_submissions_.compare_exchange_weak(submissions,
                                                       submissions + 1));
  }
  const uint64_t checked_ns = trace_submit ? BenchClockNowNs() : 0;
  const ClientOrderId id = order.client_order_id;
  const bool mirror_queued = TryEnqueue([this, order, asset, amount_nanos] {
        if (auto status = funds_.ReserveAllocated(order.client_order_id, asset,
                                                  amount_nanos);
            !status.ok()) {
          Pause("account mirror reservation failed: " +
                std::string(status.message()));
          return;
        }
        if (auto status = order_manager_.Begin(order); !status.ok()) {
          (void)funds_.Release(order.client_order_id);
          Pause("account mirror order failed: " +
                std::string(status.message()));
          return;
        }
        AccountHistoryRecord command;
        command.shard = order.shard;
        command.symbol = order.market.symbol;
        command.kind = HistoryKind::SubmitCommand;
        command.client_order_id = order.client_order_id.value;
        command.source_bench_seq = order.source_bench_seq;
        command.price_nanos = order.price_nanos;
        command.quantity_nanos = order.quantity_nanos;
        command.remaining_nanos = order.quantity_nanos;
        Record(std::move(command));
        MarkAssetDirty(asset);
        views_->MarkShardDirty(order.shard);
        FlushViews();
      });
  const uint64_t mirror_queued_ns = trace_submit ? BenchClockNowNs() : 0;
  if (!mirror_queued) return false;

  const auto abort_mirror = [this, &order, &asset] {
    auto abort = [this, id = order.client_order_id, shard = order.shard,
                  market = order.market, source = order.source_bench_seq,
                  asset] {
      if (order_manager_.Find(id)) {
        (void)order_manager_.AbortSubmit(id);
        (void)funds_.Release(id);
        AccountHistoryRecord rejected;
        rejected.shard = shard;
        rejected.symbol = market.symbol;
        rejected.kind = HistoryKind::LocalRejected;
        rejected.client_order_id = id.value;
        rejected.source_bench_seq = source;
        rejected.message = "trading send queue unavailable";
        Record(std::move(rejected));
        MarkAssetDirty(asset);
        views_->MarkShardDirty(shard);
        FlushViews();
      }
    };
    if (!TryEnqueue(abort)) boost::asio::post(io_, GuardState(abort));
  };
  JournalEntry intent;
  intent.kind = JournalKind::SubmitIntent;
  intent.client_id = id.value;
  intent.symbol = order.market.symbol;
  intent.asset = asset;
  intent.local_id = order.local_order_id.value;
  intent.shard = order.shard.value;
  intent.side = order.side == Side::Buy ? 0 : 1;
  intent.price = order.price_nanos;
  intent.quantity = order.quantity_nanos;
  intent.amount = amount_nanos;
  intent.source_bench_seq = order.source_bench_seq;
  const bool intent_queued = Persist(std::move(intent), [this, id](absl::Status status) {
        if (status.ok()) return;
        Pause("submit intent journal failed: " +
              std::string(status.message()));
        if (const TrackedOrder* order = order_manager_.Find(id);
            order && order->status == OrderStatus::PendingSubmit) {
          (void)order_manager_.MarkSubmitUnknown(id);
          ScheduleOrderQuery();
        }
      });
  const uint64_t intent_queued_ns = trace_submit ? BenchClockNowNs() : 0;
  if (!intent_queued) {
    abort_mirror();
    return false;
  }
  const bool queued = port->Submit(
      SubmitCommand{order.market, id,
                    LimitOrderRequest{order.side, order.price_nanos,
                                      order.quantity_nanos}},
      [this, id](TradeResponse response) {
        if (!TryEnqueue([this, id, response = std::move(response)]() mutable {
              PersistSubmitResponse(id, std::move(response));
            })) {
          boost::asio::post(io_, GuardState([this] {
            Pause("submit response queue unavailable");
          }));
        }
      },
      [this, id] {
        if (!TryEnqueue([this, id] {
              OnWriting(id, false);
              NotifyWriting(id, false);
            })) {
          boost::asio::post(io_, GuardState([this] {
            Pause("submit write callback queue unavailable");
          }));
        }
      });
  const uint64_t rest_returned_ns = trace_submit ? BenchClockNowNs() : 0;
  if (!queued) {
    JournalEntry abort;
    abort.kind = JournalKind::SubmitAbort;
    abort.client_id = id.value;
    if (!Persist(std::move(abort), [this](absl::Status status) {
          if (!status.ok()) Pause("submit abort journal failed");
        }))
      boost::asio::post(io_, GuardState([this] {
        Pause("submit abort journal queue unavailable");
      }));
    abort_mirror();
  }
  if (trace_submit && queued) {
    LOG_INFO(Logger(),
             "submit_stage_trace client_id={} bench_seq={} check_ns={} "
             "mirror_enqueue_ns={} intent_enqueue_ns={} rest_submit_ns={} "
             "total_ns={}",
             id.value, order.source_bench_seq, checked_ns - entered_ns,
             mirror_queued_ns - checked_ns,
             intent_queued_ns - mirror_queued_ns,
             rest_returned_ns - intent_queued_ns,
             rest_returned_ns - entered_ns);
  }
  return queued;
}

bool TradingAccount::CancelFromShard(ShardId shard, CancelCommand command,
                                     uint64_t source_bench_seq,
                                     uint64_t requested_ms) {
  if (shard.value >= kMaxShards) return false;
  TradePort* const port = shard_ports_[shard.value]
                              ? shard_ports_[shard.value]
                              : port_.get();
  if (!port || !journal_) return false;
  const ClientOrderId id = command.client_order_id;
  if (!TryEnqueue([this, id, source_bench_seq, requested_ms] {
        if (auto status = order_manager_.BeginCancel(
                id, source_bench_seq, requested_ms);
            !status.ok()) {
          Pause("account mirror cancel failed: " +
                std::string(status.message()));
          return;
        }
        const TrackedOrder* order = order_manager_.Find(id);
        AccountHistoryRecord history;
        history.shard = order->shard;
        history.symbol = order->market.symbol;
        history.kind = HistoryKind::CancelCommand;
        history.client_order_id = id.value;
        history.exchange_order_id = order->exchange_order_id.value;
        history.source_bench_seq = source_bench_seq;
        history.remaining_nanos = order->Remaining();
        Record(std::move(history));
        views_->MarkShardDirty(order->shard);
        FlushViews();
      }))
    return false;
  const auto abort_mirror = [this, id, shard] {
    auto abort = [this, id, shard] {
      if (const TrackedOrder* order = order_manager_.Find(id);
          order && order->cancel == CancelPhase::Pending) {
        (void)order_manager_.AbortCancel(id);
        views_->MarkShardDirty(shard);
        FlushViews();
      }
    };
    if (!TryEnqueue(abort)) boost::asio::post(io_, GuardState(abort));
  };
  JournalEntry intent;
  intent.kind = JournalKind::CancelIntent;
  intent.client_id = id.value;
  intent.source_bench_seq = source_bench_seq;
  intent.transaction_ms = requested_ms;
  if (!Persist(std::move(intent), [this, id](absl::Status status) {
        if (!status.ok()) {
          Pause("cancel intent journal failed: " +
                std::string(status.message()));
          if (const TrackedOrder* order = order_manager_.Find(id);
              order && !order->Terminal() &&
              order->cancel == CancelPhase::Pending) {
            (void)order_manager_.MarkCancelUnknown(id);
            ScheduleOrderQuery();
          }
        }
      })) {
    abort_mirror();
    return false;
  }
  const bool queued = port->Cancel(
      std::move(command),
      [this, id](TradeResponse response) {
        if (!TryEnqueue([this, id, response = std::move(response)]() mutable {
              PersistCancelResponse(id, std::move(response));
            })) {
          boost::asio::post(io_, GuardState([this] {
            Pause("cancel response queue unavailable");
          }));
        }
      },
      [this, id] {
        if (!TryEnqueue([this, id] {
              OnWriting(id, true);
              NotifyWriting(id, true);
            })) {
          boost::asio::post(io_, GuardState([this] {
            Pause("cancel write callback queue unavailable");
          }));
        }
      });
  if (!queued) {
    JournalEntry abort;
    abort.kind = JournalKind::CancelAbort;
    abort.client_id = id.value;
    if (!Persist(std::move(abort), [this](absl::Status status) {
          if (!status.ok()) Pause("cancel abort journal failed");
        }))
      boost::asio::post(io_, GuardState([this] {
        Pause("cancel abort journal queue unavailable");
      }));
    abort_mirror();
  }
  return queued;
}

void TradingAccount::DisableAdmission() {
  std::unique_lock lock(admission_gate_mutex_);
  admission_enabled_ = false;
}

void TradingAccount::EnableAdmission() {
  std::unique_lock lock(admission_gate_mutex_);
  min_ready_revision_ = views_->Revision();
  admission_enabled_ = true;
}

bool TradingAccount::AccountQueueEmpty() const {
  std::lock_guard lock(queue_mutex_);
  return queue_.empty();
}

void TradingAccount::ReportShardError(ShardId shard, absl::Status error) {
  DisableAdmission();
  std::string message = "shard " + std::to_string(shard.value) +
                        " trading state failed: " + error.ToString();
  if (!TryEnqueue([this, message] { Pause(message); })) {
    boost::asio::post(io_, GuardState([this, message] { Pause(message); }));
  }
}

std::shared_ptr<const PublishedAssetView> TradingAccount::AssetView(
    const std::string& asset) const {
  return views_->AssetView(asset);
}

std::shared_ptr<const std::atomic<uint64_t>> TradingAccount::AssetRevision()
    const {
  return views_->AssetRevision();
}

bool TradingAccount::TryEnqueue(std::function<void()> task) {
  bool schedule = false;
  {
    std::lock_guard lock(queue_mutex_);
    if (stopped_ || queue_.size() >= queue_capacity_) {
      ++queue_rejections_;
      return false;
    }
    queue_.push_back(std::move(task));
    queue_peak_ = std::max(queue_peak_, queue_.size());
    if (!drain_scheduled_) {
      drain_scheduled_ = true;
      schedule = true;
    }
  }
  if (schedule) boost::asio::post(io_, [this] { Drain(); });
  return true;
}

void TradingAccount::Drain() {
  // 每批最多 64 个，给账户 socket 和定时器留执行机会。
  constexpr size_t kBatchSize = 64;
  for (size_t count = 0; count < kBatchSize; ++count) {
    std::function<void()> task;
    {
      std::lock_guard lock(queue_mutex_);
      if (queue_.empty()) {
        drain_scheduled_ = false;
        return;
      }
      task = std::move(queue_.front());
      queue_.pop_front();
    }
    // 队列锁只管队列；回报和控制任务与 Shard 提交共用状态锁。
    std::lock_guard state_lock(state_mutex_);
    task();
  }
  boost::asio::post(io_, [this] { Drain(); });
}

bool TradingAccount::PostView(ShardId shard, ViewSink done) {
  return TryEnqueue(
      [this, shard, done = std::move(done)]() mutable { done(View(shard)); });
}

bool TradingAccount::PostStatus(std::function<void(AccountStatus)> done) {
  return TryEnqueue(
      [this, done = std::move(done)]() mutable { done(Status()); });
}

bool TradingAccount::PostFinalStatus(std::function<void(AccountStatus)> done) {
  return TryEnqueue([this, done = std::move(done)]() mutable {
    if (!journal_ || !port_) {
      done(Status());
      return;
    }
    if (!port_->QueryBalances(
            GuardState([this,
             done](absl::StatusOr<std::vector<BalanceUpdate>> result) mutable {
              if (!result.ok()) {
                Pause("final account balance query failed: " +
                      std::string(result.status().message()));
                done(Status());
                return;
              }
              port_->Stop();
              FinishFinalStatus(std::move(*result), std::move(done));
            }))) {
      Pause("final account balance query queue unavailable");
      done(Status());
    }
  });
}

bool TradingAccount::PostControl(
    AccountAction action, std::string operator_id, std::string reason,
    std::function<void(absl::StatusOr<uint64_t>)> done) {
  return TryEnqueue([this, action, operator_id = std::move(operator_id),
                     reason = std::move(reason),
                     done = std::move(done)]() mutable {
    if (operator_id.empty() || reason.empty()) {
      done(absl::InvalidArgumentError("operator and reason required"));
      return;
    }
    JournalEntry entry;
    entry.kind = JournalKind::Control;
    entry.result = static_cast<uint64_t>(action);
    entry.asset = std::move(operator_id);
    entry.message = reason;
    entry.transaction_ms = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
    if (!Persist(std::move(entry), [this, action, reason = std::move(reason),
                                    done](absl::Status status) mutable {
          if (!status.ok()) {
            Pause("account control journal failed: " +
                  std::string(status.message()));
            done(status);
            return;
          }
          if (action == AccountAction::PauseNewOrders ||
              action == AccountAction::CancelAll) {
            manual_paused_ = true;
            ready_ = false;
            DisableAdmission();
            error_ = "manually paused: " + reason;
            MarkAllDirty();
            FlushViews();
          }
          if (action == AccountAction::PauseNewOrders) {
            done(uint64_t{0});
          } else if (action == AccountAction::ResumeTrading) {
            manual_paused_ = false;
            if (!private_connected_) {
              Pause("private stream unavailable for resume");
              done(absl::UnavailableError(error_));
            } else {
              SyncFromVenue();
              done(uint64_t{0});
            }
          } else if (action == AccountAction::SyncAccountState) {
            if (!private_connected_) {
              done(absl::UnavailableError(
                  "private stream unavailable for sync"));
            } else {
              SyncFromVenue();
              done(uint64_t{0});
            }
          } else {
            if (trade_sinks_.empty()) {
              done(uint64_t{0});
              return;
            }
            auto remaining = std::make_shared<size_t>(trade_sinks_.size());
            auto admitted = std::make_shared<uint64_t>(0);
            for (const auto& [shard, sink] : trade_sinks_) {
              (void)shard;
              if (!sink.cancel_all) {
                if (--*remaining == 0) done(*admitted);
                continue;
              }
              sink.cancel_all([this, remaining, admitted, done](uint64_t count) {
                boost::asio::post(io_, GuardState(
                    [remaining, admitted, done, count] {
                      *admitted += count;
                      if (--*remaining == 0) done(*admitted);
                    }));
              });
            }
          }
        })) {
      Pause("account control journal queue unavailable");
      done(absl::ResourceExhaustedError(error_));
    }
  });
}

void TradingAccount::FinishFinalStatus(
    std::vector<BalanceUpdate> balances,
    std::function<void(AccountStatus)> done) {
  {
    std::lock_guard lock(queue_mutex_);
    if (pending_journal_ != 0 || !queue_.empty()) {
      final_status_timer_.expires_after(std::chrono::milliseconds(1));
      final_status_timer_.async_wait(
          GuardState([this, balances = std::move(balances), done = std::move(done)](
              const boost::system::error_code& error) mutable {
            if (!error) {
              FinishFinalStatus(std::move(balances), std::move(done));
            }
          }));
      return;
    }
  }
  const auto projected_totals = funds_.ProjectedTotals();
  if (balances.empty()) {
    if (!projected_totals.empty()) {
      Pause("final account balance query omitted configured assets");
    }
    done(Status());
    return;
  }
  std::set<std::string> seen;
  for (const BalanceUpdate& balance : balances) {
    if (!seen.insert(balance.asset).second ||
        !funds_.MatchesProjectedTotal(balance)) {
      Pause("final account balance differs from tracked fills: " +
            balance.asset);
      break;
    }
  }
  for (const auto& [asset, total] : projected_totals) {
    (void)total;
    if (!seen.contains(asset)) {
      Pause("final account balance query omitted asset: " + asset);
      break;
    }
  }
  auto remaining = std::make_shared<size_t>(balances.size());
  auto completion =
      std::make_shared<std::function<void(AccountStatus)>>(std::move(done));
  for (BalanceUpdate& balance : balances) {
    if (!PersistAndApply(AccountEvent{std::move(balance)}, false,
                         [this, remaining, completion](absl::Status status) {
                           if (!status.ok() && error_.empty()) {
                             Pause("final account balance journal failed: " +
                                   std::string(status.message()));
                           }
                           if (--*remaining == 0) {
                             if (error_.empty())
                               funds_.SettleVerifiedSnapshot();
                             (*completion)(Status());
                           }
                         })) {
      Pause("final account balance journal queue unavailable");
      if (--*remaining == 0) (*completion)(Status());
    }
  }
}

uint64_t TradingAccount::Budget(const std::string& asset) const {
  const auto initial = config_.initial_balances_nanos.find(asset);
  const auto hard = config_.hard_limits_nanos.find(asset);
  const AssetFunds* balance = funds_.Asset(asset);
  if (initial == config_.initial_balances_nanos.end() ||
      hard == config_.hard_limits_nanos.end() || !balance) {
    return 0;
  }
  const uint64_t reported_used = initial->second > balance->total_nanos
                                     ? initial->second - balance->total_nanos
                                     : 0;
  const uint64_t pending_debit = funds_.PendingDebit(asset);
  if (pending_debit > std::numeric_limits<uint64_t>::max() - reported_used)
    return 0;
  const uint64_t used = reported_used + pending_debit;
  const uint64_t reserved = funds_.Reserved(asset);
  if (used >= hard->second || reserved >= hard->second - used) return 0;
  return std::min(funds_.Available(asset), hard->second - used - reserved);
}

absl::StatusOr<uint64_t> TradingAccount::ReserveAmount(
    const MarketConfig& market, const LimitOrderRequest& request) const {
  if (request.side == Side::Sell) return request.quantity_nanos;
  auto gross =
      MulDivCeil(request.price_nanos, request.quantity_nanos, kFixedScale);
  if (!gross.ok()) return gross.status();
  auto fee = MulDivCeil(*gross, config_.maker_fee_rate_ppb, kFixedScale);
  if (!fee.ok()) return fee.status();
  auto amount = CheckedAdd(*gross, *fee);
  if (!amount.ok()) return amount.status();
  if (config_.maker_fee_rate_ppb == 0) return *amount;
  return CheckedAdd(*amount,
                    request.quantity_nanos / market.rule.base_increment_nanos);
}

Order TradingAccount::ReportOrder(const TrackedOrder& tracked) const {
  Order result;
  result.id = tracked.local_order_id;
  result.side = tracked.side;
  result.price_nanos = tracked.price_nanos;
  result.quantity_nanos = tracked.quantity_nanos;
  result.remaining_nanos = tracked.Remaining();
  result.market = tracked.market;
  result.client_order_id = tracked.client_order_id;
  result.exchange_order_id = tracked.exchange_order_id;
  return result;
}

void TradingAccount::SubmitResponse(ClientOrderId id, TradeResponse response) {
  LOG_TRACE_L1(Logger(), "submit response account={} client_id={} result={}",
               config_.id, id.value, static_cast<int>(response.result));
  const TrackedOrder* order = order_manager_.Find(id);
  if (!order) {
    Pause("submit response for unknown client order id");
    return;
  }
  if (response.result == TradeResponse::Result::NotSent) {
    if (order->status != OrderStatus::PendingSubmit || order->submit_writing) {
      Pause("unsent submit contradicts exchange activity");
      return;
    }
    const ShardId shard = order->shard;
    const std::string symbol = order->market.symbol;
    const uint64_t source = order->source_bench_seq;
    const FundsHold* hold = funds_.Hold(id);
    const std::string asset = hold ? hold->asset : std::string{};
    if (!order_manager_.AbortSubmit(id).ok() || !funds_.Release(id).ok()) {
      Pause("cannot release unsent order");
      return;
    }
    AccountHistoryRecord history;
    history.shard = shard;
    history.symbol = symbol;
    history.kind = HistoryKind::LocalRejected;
    history.client_order_id = id.value;
    history.source_bench_seq = source;
    history.message = response.reason;
    Record(std::move(history));
    MarkAssetDirty(asset);
    views_->MarkShardDirty(shard);
    FlushViews();
    return;
  }
  if (response.result == TradeResponse::Result::Unknown) {
    if (order->status != OrderStatus::PendingSubmit) {
      // The private stream already established the outcome. A late HTTP
      // timeout cannot turn a confirmed order back into an unknown order.
      return;
    }
    (void)order_manager_.MarkSubmitUnknown(id);
    AccountHistoryRecord history;
    history.shard = order->shard;
    history.symbol = order->market.symbol;
    history.kind = HistoryKind::SubmitUnknown;
    history.client_order_id = id.value;
    history.source_bench_seq = order->source_bench_seq;
    history.message = response.reason;
    Record(std::move(history));
    views_->MarkShardDirty(order->shard);
    if (!replaying_ && !sync_->Syncing()) ScheduleOrderQuery();
  } else if (response.result == TradeResponse::Result::Rejected) {
    if (order->status == OrderStatus::Rejected) {
      // The private rejection may have arrived ahead of the HTTP response.
    } else if (order->status != OrderStatus::PendingSubmit) {
      Pause("submit rejection conflicts with private report");
      return;
    } else {
      ApplyEvent(OrderReport{ReportOrder(*order), OrderStatus::Rejected,
                             response.reason});
    }
  } else if (order->status == OrderStatus::PendingSubmit) {
    Order report_order = ReportOrder(*order);
    report_order.exchange_order_id = response.exchange_order_id;
    ApplyEvent(OrderReport{std::move(report_order), OrderStatus::Accepted});
  }
  ScheduleOrderQuery();
  FlushViews();
}

void TradingAccount::CancelResponse(ClientOrderId id, TradeResponse response) {
  LOG_TRACE_L1(Logger(), "cancel response account={} client_id={} result={}",
               config_.id, id.value, static_cast<int>(response.result));
  const TrackedOrder* order = order_manager_.Find(id);
  if (!order) {
    Pause("cancel response for unknown client order id");
    return;
  }
  if (order->Terminal()) {
    // The private report can arrive before a delayed cancel HTTP response.
    return;
  }
  if (response.result == TradeResponse::Result::NotSent) {
    const uint64_t source = order->cancel_source_bench_seq;
    if (order->cancel_writing || !order_manager_.AbortCancel(id).ok()) {
      Pause("unsent cancel contradicts exchange activity");
      return;
    }
    AccountHistoryRecord history;
    history.shard = order->shard;
    history.symbol = order->market.symbol;
    history.kind = HistoryKind::LocalRejected;
    history.client_order_id = id.value;
    history.source_bench_seq = source;
    history.message = response.reason;
    Record(std::move(history));
    views_->MarkShardDirty(order->shard);
    ScheduleOrderQuery();
    FlushViews();
    return;
  }
  if (response.result == TradeResponse::Result::Unknown) {
    (void)order_manager_.MarkCancelUnknown(id);
    AccountHistoryRecord history;
    history.shard = order->shard;
    history.symbol = order->market.symbol;
    history.kind = HistoryKind::CancelUnknown;
    history.client_order_id = id.value;
    history.source_bench_seq = order->cancel_source_bench_seq;
    history.message = response.reason;
    Record(std::move(history));
    views_->MarkShardDirty(order->shard);
    if (!replaying_ && !sync_->Syncing()) ScheduleOrderQuery();
  } else if (response.result == TradeResponse::Result::Rejected) {
    (void)order_manager_.MarkCancelRejected(id);
    AccountHistoryRecord history;
    history.shard = order->shard;
    history.symbol = order->market.symbol;
    history.kind = HistoryKind::CancelRejected;
    history.client_order_id = id.value;
    history.source_bench_seq = order->cancel_source_bench_seq;
    history.message = response.reason;
    Record(std::move(history));
    views_->MarkShardDirty(order->shard);
    if (!replaying_ && !sync_->Syncing()) ScheduleOrderQuery();
  } else if (!order->Terminal()) {
    if (response.remaining_nanos &&
        *response.remaining_nanos < order->Remaining()) {
      // REST can report the final cumulative fill before the ordered private
      // trade event arrives. Keep PendingCancel and its reservation until the
      // trade and terminal report are applied by the user stream.
      FlushViews();
      return;
    }
    Order report_order = ReportOrder(*order);
    if (response.remaining_nanos) {
      report_order.remaining_nanos = *response.remaining_nanos;
    }
    ApplyEvent(OrderReport{std::move(report_order), OrderStatus::Cancelled});
  }
  ScheduleOrderQuery();
  FlushViews();
}

void TradingAccount::OnWriting(ClientOrderId id, bool cancel) {
  if (!order_manager_.MarkWriting(id, cancel).ok()) {
    Pause("send stage has unknown order");
    return;
  }
  order_manager_.Arm(id);
  ScheduleOrderQuery();
  JournalEntry entry;
  entry.kind = cancel ? JournalKind::CancelWriting : JournalKind::SubmitWriting;
  entry.client_id = id.value;
  if (!Persist(std::move(entry), [this, id](absl::Status status) {
        if (!status.ok()) {
          Pause(std::format("send stage journal failed client_id={}: {}",
                            id.value, std::string(status.message())));
        }
      })) {
    Pause("send stage journal queue unavailable");
  }
  const TrackedOrder* order = order_manager_.Find(id);
  AccountHistoryRecord history;
  history.shard = order->shard;
  history.symbol = order->market.symbol;
  history.kind =
      cancel ? HistoryKind::CancelWriting : HistoryKind::SubmitWriting;
  history.client_order_id = id.value;
  history.source_bench_seq =
      cancel ? order->cancel_source_bench_seq : order->source_bench_seq;
  Record(std::move(history));
  LOG_TRACE_L1(Logger(), "socket write account={} client_id={} cancel={}",
               config_.id, id.value, cancel);
}

void TradingAccount::NotifyResponse(const ClientOrderId& id, bool cancel,
                                    TradeResponse response) {
  const TrackedOrder* order = order_manager_.Find(id);
  if (!order) return;
  const auto found = trade_sinks_.find(order->shard.value);
  if (found != trade_sinks_.end() && found->second.response) {
    found->second.response(id, cancel, std::move(response));
  }
}

void TradingAccount::NotifyWriting(const ClientOrderId& id, bool cancel) {
  const TrackedOrder* order = order_manager_.Find(id);
  if (!order) return;
  const auto found = trade_sinks_.find(order->shard.value);
  if (found != trade_sinks_.end() && found->second.writing) {
    found->second.writing(id, cancel);
  }
}

void TradingAccount::NotifyEvent(const AccountEvent& event) {
  if (replaying_) return;
  const ClientOrderId* id = nullptr;
  if (const auto* report = std::get_if<OrderReport>(&event))
    id = &report->order.client_order_id;
  else if (const auto* fill = std::get_if<Fill>(&event))
    id = &fill->client_order_id;
  if (!id) return;
  const TrackedOrder* order = order_manager_.Find(*id);
  if (!order) return;
  const auto found = trade_sinks_.find(order->shard.value);
  if (found != trade_sinks_.end() && found->second.event) {
    found->second.event(event);
  }
}

void TradingAccount::OnConnectionState(bool connected, std::string reason) {
  if (!io_.get_executor().running_in_this_thread()) {
    if (!TryEnqueue([this, connected, reason = std::move(reason)] {
          OnConnectionState(connected, reason);
        })) {
      boost::asio::post(io_, GuardState(
          [this] { Pause("connection state queue overflow"); }));
    }
    return;
  }
  std::lock_guard state_lock(state_mutex_);
  if (!connected) {
    private_connected_ = false;
    sync_->Disconnect();
    order_query_timer_.cancel();
    Pause("private stream unavailable: " + reason);
  } else {
    private_connected_ = true;
    SyncFromVenue();
  }
}

void TradingAccount::SyncFromVenue() { sync_->Start(); }

bool TradingAccount::PersistAndApply(AccountEvent event, bool from_venue,
                                     std::function<void(absl::Status)> done) {
  const uint64_t received_ns = trade_perf_ && from_venue ? PerfNowNs() : 0;
  const bool order_report = std::holds_alternative<OrderReport>(event);
  JournalEntry journal_entry = JournalForEvent(event);
  // 写盘完成前不应用私有回报或查单事实。
  return Persist(std::move(journal_entry), [this, event = std::move(event),
                                            from_venue, received_ns,
                                            order_report, done = std::move(done)](
                                               absl::Status status) mutable {
    const uint64_t commit_ns = received_ns ? PerfNowNs() : 0;
    if (!status.ok()) {
      Pause("account event journal failed: " + std::string(status.message()));
    } else {
      ApplyEvent(event, from_venue);
      if (received_ns) {
        trade_perf_->RecordPrivate(order_report, commit_ns - received_ns,
                                   PerfNowNs() - commit_ns);
      }
      if (const auto* report = std::get_if<OrderReport>(&event)) {
        LOG_TRACE_L1(Logger(),
                     "order report applied account={} client_id={} state={} "
                     "private={}",
                     config_.id, report->order.client_order_id.value,
                     static_cast<int>(report->state), from_venue);
      } else if (const auto* fill = std::get_if<Fill>(&event)) {
        LOG_TRACE_L1(Logger(),
                     "fill applied account={} client_id={} trade_id={}",
                     config_.id, fill->client_order_id.value, fill->trade_id);
      } else {
        LOG_TRACE_L1(Logger(), "balance event applied account={} kind={}",
                     config_.id, event.index());
      }
      ScheduleOrderQuery();
      FlushViews();
      if (!error_.empty()) status = absl::DataLossError(error_);
    }
    if (done) done(std::move(status));
  });
}

void TradingAccount::ScheduleOrderQuery() {
  order_query_timer_.cancel();
  if (replaying_ || sync_->Syncing() || !private_connected_ || !port_ ||
      quiescing_)
    return;
  const auto due = order_manager_.NextDeadline();
  if (!due) return;
  order_query_timer_.expires_at(*due);
  order_query_timer_.async_wait(GuardState([this](const boost::system::error_code& error) {
    if (!error) QueryDueOrder();
  }));
}

void TradingAccount::QueryDueOrder() {
  if (sync_->Syncing() || !private_connected_ || quiescing_ || !port_) return;
  auto query = order_manager_.TakeDue();
  if (!query) {
    ScheduleOrderQuery();
    return;
  }
  LOG_TRACE_L1(Logger(), "order query account={} client_id={}", config_.id,
               query->client_order_id.value);
  auto send_query = [this, query = *query] {
    if (!port_->Query(query, GuardState([this, query](absl::StatusOr<OrderReport> result) {
          QueryOrderResult(query, std::move(result));
        }))) {
      order_manager_.QueryFinished(query.client_order_id);
      ScheduleOrderQuery();
    }
  };
  const TrackedOrder* order = order_manager_.Find(query->client_order_id);
  if (!order) {
    order_manager_.QueryFinished(query->client_order_id);
    ScheduleOrderQuery();
    return;
  }
  const bool submit_timeout =
      order->status == OrderStatus::PendingSubmit && !order->submit_unknown;
  const bool cancel_timeout = order->cancel == CancelPhase::Pending;
  if (submit_timeout || cancel_timeout) {
    JournalEntry entry;
    entry.kind = submit_timeout ? JournalKind::SubmitOutcome
                                : JournalKind::CancelOutcome;
    entry.client_id = query->client_order_id.value;
    entry.result = static_cast<uint64_t>(TradeResponse::Result::Unknown);
    entry.message = "local response timeout";
    if (!Persist(std::move(entry), [this, id = query->client_order_id](
                                       absl::Status status) {
          if (!status.ok()) {
            Pause(std::format("order timeout journal failed client_id={}: {}",
                              id.value, std::string(status.message())));
          }
        })) {
      Pause("order timeout journal queue unavailable");
    }
    TradeResponse response{
        TradeResponse::Result::Unknown, {}, "local response timeout"};
    if (submit_timeout) {
      SubmitResponse(query->client_order_id, std::move(response));
    } else {
      CancelResponse(query->client_order_id, std::move(response));
    }
  }
  send_query();
  ScheduleOrderQuery();
}

void TradingAccount::QueryOrderResult(QueryCommand query,
                                      absl::StatusOr<OrderReport> result) {
  if (!result.ok()) {
    LOG_WARNING(
        Logger(), "order query failed account={} client_id={} reason={}",
        config_.id, query.client_order_id.value, result.status().ToString());
    // A missing or temporarily unavailable order is still unresolved. The
    // manager retries with backoff; it never invents a rejection.
    order_manager_.QueryFinished(query.client_order_id);
    ScheduleOrderQuery();
    return;
  }
  OrderReport report = std::move(*result);
  LOG_INFO(Logger(),
           "order query result account={} client_id={} state={} remaining={}",
           config_.id, query.client_order_id.value,
           static_cast<int>(report.state), report.order.remaining_nanos);
  // One NEW snapshot can race with a cancel request whose HTTP response was
  // lost. Require a second spaced query before permitting another cancel.
  report.authoritative_query =
      order_manager_.RepeatedQuery(query.client_order_id);
  if (report.order.client_order_id != query.client_order_id ||
      report.order.market != query.market) {
    Pause("per-order query returned a different order");
    return;
  }
  if (report.order.exchange_order_id.value == 0) {
    ApplyQueriedTrades(std::move(query), std::move(report), {}, 0);
    return;
  }
  query.exchange_order_id = report.order.exchange_order_id;
  if (!port_->QueryTrades(
          query, GuardState([this, query, report = std::move(report)](
                     absl::StatusOr<std::vector<Fill>> trades) mutable {
            if (!trades.ok()) {
              order_manager_.QueryFinished(query.client_order_id);
              ScheduleOrderQuery();
              return;
            }
            ApplyQueriedTrades(query, std::move(report), std::move(*trades), 0);
          }))) {
    order_manager_.QueryFinished(query.client_order_id);
    ScheduleOrderQuery();
  }
}

void TradingAccount::ApplyQueriedTrades(QueryCommand query, OrderReport report,
                                        std::vector<Fill> trades,
                                        size_t index) {
  if (index == trades.size()) {
    if (!PersistAndApply(AccountEvent{std::move(report)}, false,
                         [this, id = query.client_order_id](absl::Status) {
                           order_manager_.QueryFinished(id);
                           ScheduleOrderQuery();
                         })) {
      Pause("queried order journal queue unavailable");
    }
    return;
  }
  Fill fill = trades[index];
  if (!PersistAndApply(
          AccountEvent{std::move(fill)}, false,
          [this, query = std::move(query), report = std::move(report),
           trades = std::move(trades), index](absl::Status status) mutable {
            if (!status.ok()) return;
            ApplyQueriedTrades(std::move(query), std::move(report),
                               std::move(trades), index + 1);
          })) {
    Pause("queried fill journal queue unavailable");
  }
}

void TradingAccount::OnVenueEvent(AccountEvent event) {
  ++pending_private_events_;
  if (!TryEnqueue([this, event = std::move(event)]() mutable {
        --pending_private_events_;
        if (sync_->Syncing()) {
          if (!sync_->BufferPrivate(std::move(event), queue_capacity_)) {
            Pause("private event buffer overflow during synchronization");
          }
        } else if (!PersistAndApply(std::move(event), true)) {
          Pause("private event journal queue unavailable");
        }
      })) {
    --pending_private_events_;
    boost::asio::post(io_, GuardState([this] {
      Pause("account event queue overflow; state synchronization required");
    }));
  }
}

void TradingAccount::ApplyEvent(const AccountEvent& event, bool from_venue) {
  if (const auto* report = std::get_if<OrderReport>(&event)) {
    const TrackedOrder* previous =
        order_manager_.Find(report->order.client_order_id);
    const OrderStatus previous_status =
        previous ? previous->status : OrderStatus::PendingSubmit;
    const CancelPhase previous_cancel =
        previous ? previous->cancel : CancelPhase::None;
    auto applied = order_manager_.ApplyReport(*report);
    if (!applied.ok()) {
      Pause(std::string(applied.status().message()));
      return;
    }
    const TrackedOrder* order =
        order_manager_.Find(report->order.client_order_id);
    if (!order) return;
    if (previous_cancel == CancelPhase::Unknown &&
        order->cancel == CancelPhase::Rejected) {
      AccountHistoryRecord rejected;
      rejected.shard = order->shard;
      rejected.symbol = order->market.symbol;
      rejected.kind = HistoryKind::CancelRejected;
      rejected.client_order_id = order->client_order_id.value;
      rejected.exchange_order_id = order->exchange_order_id.value;
      rejected.source_bench_seq = order->cancel_source_bench_seq;
      rejected.message = "order remains open after repeated query";
      Record(std::move(rejected));
    }
    if (!order->Terminal() && funds_.Hold(order->client_order_id)) {
      (void)funds_.MarkVenueAccepted(order->client_order_id);
    }
    if (from_venue && (config_.mode == TradingMode::PressureSim ||
                       config_.mode == TradingMode::OnlineSim)) {
      AccountHistoryRecord received;
      received.shard = order->shard;
      received.symbol = order->market.symbol;
      received.kind = HistoryKind::PrivateReportApplied;
      received.client_order_id = order->client_order_id.value;
      received.exchange_order_id = order->exchange_order_id.value;
      received.source_bench_seq = report->state == OrderStatus::Cancelled
                                      ? order->cancel_source_bench_seq
                                      : order->source_bench_seq;
      received.remaining_nanos = report->order.remaining_nanos;
      received.message = std::to_string(static_cast<int>(report->state));
      Record(std::move(received));
    }
    if (*applied && (report->state != OrderStatus::Accepted ||
                     previous_status != OrderStatus::Accepted)) {
      AccountHistoryRecord history;
      history.shard = order->shard;
      history.symbol = order->market.symbol;
      history.client_order_id = order->client_order_id.value;
      history.exchange_order_id = order->exchange_order_id.value;
      history.source_bench_seq = report->state == OrderStatus::Cancelled
                                     ? order->cancel_source_bench_seq
                                     : order->source_bench_seq;
      history.price_nanos = order->price_nanos;
      history.quantity_nanos = order->quantity_nanos;
      history.remaining_nanos = report->order.remaining_nanos;
      history.message = report->reason;
      switch (report->state) {
        case OrderStatus::PendingSubmit:
          history.kind = HistoryKind::SubmitCommand;
          break;
        case OrderStatus::Accepted:
          history.kind = HistoryKind::OrderAccepted;
          break;
        case OrderStatus::PartiallyFilled:
          history.kind = HistoryKind::OrderPartiallyFilled;
          break;
        case OrderStatus::Filled:
          history.kind = HistoryKind::OrderFilled;
          break;
        case OrderStatus::Rejected:
          history.kind = HistoryKind::OrderRejected;
          break;
        case OrderStatus::Cancelled:
          history.kind = HistoryKind::OrderCancelled;
          break;
      }
      Record(std::move(history));
    }
    if ((report->state == OrderStatus::Rejected ||
         report->state == OrderStatus::Cancelled ||
         (report->state == OrderStatus::Filled && order->Remaining() == 0)) &&
        funds_.Hold(order->client_order_id)) {
      const std::string asset = funds_.Hold(order->client_order_id)->asset;
      (void)funds_.Release(order->client_order_id);
      MarkAssetDirty(asset);
    }
    views_->MarkShardDirty(order->shard);
  } else if (const auto* fill = std::get_if<Fill>(&event)) {
    auto applied = order_manager_.ApplyFill(*fill);
    if (!applied.ok()) {
      Pause(std::string(applied.status().message()));
      return;
    }
    if (!*applied) return;
    const TrackedOrder* order = order_manager_.Find(fill->client_order_id);
    if (!order) return;
    if (funds_.Hold(order->client_order_id)) {
      (void)funds_.MarkVenueAccepted(order->client_order_id);
    }
    {
      const auto market = markets_.find(order->shard.value);
      if (market == markets_.end()) {
        Pause("fill for unconfigured market");
        return;
      }
      if (auto status = funds_.RecordFillTotals(
              *fill, market->second.base_asset, market->second.quote_asset);
          !status.ok()) {
        Pause(std::string(status.message()));
        return;
      }
      const std::string& spent_asset = order->side == Side::Buy
                                           ? market->second.quote_asset
                                           : market->second.base_asset;
      const uint64_t spent =
          order->side == Side::Buy ? fill->gross_nanos : fill->quantity_nanos;
      if (auto status = funds_.DebitUntilBalance(spent_asset, spent);
          !status.ok()) {
        Pause(std::string(status.message()));
        return;
      }
      if (fill->fee_nanos != 0) {
        if (auto status =
                funds_.DebitUntilBalance(fill->fee_asset, fill->fee_nanos);
            !status.ok()) {
          Pause(std::string(status.message()));
          return;
        }
      }
    }
    AccountHistoryRecord history;
    history.shard = order->shard;
    history.symbol = order->market.symbol;
    history.kind = HistoryKind::Fill;
    history.client_order_id = order->client_order_id.value;
    history.exchange_order_id = order->exchange_order_id.value;
    history.trade_id = fill->trade_id;
    history.source_bench_seq = order->source_bench_seq;
    history.price_nanos = fill->price_nanos;
    history.quantity_nanos = fill->quantity_nanos;
    history.remaining_nanos = order->Remaining();
    history.fee_nanos = fill->fee_nanos;
    history.asset = fill->fee_asset;
    Record(std::move(history));
    if (funds_.Hold(order->client_order_id)) {
      const std::string asset = funds_.Hold(order->client_order_id)->asset;
      if (order->Remaining() == 0) {
        (void)funds_.Release(order->client_order_id);
      } else {
        const auto market = markets_.find(order->shard.value);
        if (market != markets_.end()) {
          const auto amount = ReserveAmount(
              market->second, LimitOrderRequest{order->side, order->price_nanos,
                                                order->Remaining()});
          if (!amount.ok() ||
              !funds_.SetHold(order->client_order_id, *amount).ok()) {
            Pause("cannot adjust reservation after fill");
            return;
          }
        }
      }
      MarkAssetDirty(asset);
    }
    if (!fill->fee_asset.empty()) MarkAssetDirty(fill->fee_asset);
    views_->MarkShardDirty(order->shard);
  } else if (const auto* balance = std::get_if<BalanceUpdate>(&event)) {
    if (funds_.IsStaleBalance(*balance)) return;
    const bool unexplained = from_venue && !funds_.MatchesProjectedAt(*balance);
    if (auto status = funds_.ApplyBalance(*balance); !status.ok()) {
      Pause(std::string(status.message()));
      return;
    }
    if (from_venue && !unexplained && balance->version_ms == 0) {
      funds_.ConfirmRecentProjectedTotal(*balance);
    }
    if (unexplained) {
      Pause("private balance requires reconciliation: " + balance->asset);
    }
    AccountHistoryRecord history;
    history.shard = ShardId{kMaxShards};
    history.kind = HistoryKind::Balance;
    history.total_nanos = balance->total_nanos;
    history.available_nanos = balance->available_nanos;
    history.locked_nanos = balance->locked_nanos;
    history.asset = balance->asset;
    Record(std::move(history));
    MarkAssetDirty(balance->asset);
  } else if (const auto* delta = std::get_if<BalanceDelta>(&event)) {
    if (auto status = funds_.ApplyDelta(*delta); !status.ok()) {
      Pause(std::string(status.message()));
      return;
    }
    const AssetFunds* balance = funds_.Asset(delta->asset);
    AccountHistoryRecord history;
    history.shard = ShardId{kMaxShards};
    history.kind = HistoryKind::Balance;
    history.asset = delta->asset;
    history.total_nanos = balance->total_nanos;
    history.available_nanos = balance->free_nanos;
    history.locked_nanos = balance->locked_nanos;
    history.message =
        delta->external_lock ? "external_lock_delta" : "balance_delta";
    Record(std::move(history));
    MarkAssetDirty(delta->asset);
    if (from_venue && delta->amount_nanos != 0) {
      Pause("external balance delta requires reconciliation: " + delta->asset);
    }
  }
  NotifyEvent(event);
}

void TradingAccount::MarkAssetDirty(const std::string& asset) {
  views_->MarkAssetDirty(asset);
}

void TradingAccount::MarkAllDirty() { views_->MarkAllDirty(); }

void TradingAccount::FlushViews() { views_->Flush(); }

void TradingAccount::PublishAllAssetViews() { views_->PublishAllAssets(); }

AccountView TradingAccount::View(ShardId shard) const {
  return views_->View(shard);
}

AccountStatus TradingAccount::Status() const {
  AccountStatus status;
  status.id = config_.id;
  status.revision = views_->Revision();
  status.ready = ready_;
  status.private_connected = private_connected_;
  status.syncing = sync_->Syncing();
  status.manual_paused = manual_paused_;
  status.pending_journal = pending_journal_;
  status.journal_pending_peak = journal_pending_peak_;
  status.journal_rejections = journal_rejections_;
  if (port_) status.trade_queue = port_->QueueHealth();
  for (const auto& extra : extra_ports_) {
    const auto health = extra->QueueHealth();
    status.trade_queue.queued += health.queued;
    status.trade_queue.peak += health.peak;
    status.trade_queue.rejected += health.rejected;
  }
  status.error = error_;
  status.balances = funds_.Assets();
  for (const auto& [asset, balance] : status.balances) {
    status.available_nanos.emplace(asset, funds_.Available(asset));
    status.reserved_nanos.emplace(asset, funds_.Reserved(asset));
  }
  {
    std::lock_guard lock(queue_mutex_);
    status.queued = queue_.size();
    status.account_queue_peak = queue_peak_;
    status.account_queue_rejections = queue_rejections_;
  }
  for (const auto& [id, order] : order_manager_.Orders()) {
    if (!order.Terminal()) ++status.open_orders;
    if (order.submit_unknown || order.cancel == CancelPhase::Unknown) {
      ++status.unknown_orders;
    }
  }
  return status;
}

void TradingAccount::Pause(std::string error) {
  if (ready_ || error_ != error) {
    LOG_ERROR(Logger(), "account paused account={} reason={}", config_.id,
              error);
  }
  ready_ = false;
  DisableAdmission();
  error_ = std::move(error);
  MarkAllDirty();
  FlushViews();
}

void TradingAccount::Record(AccountHistoryRecord record) {
  if (replaying_) return;
  if (!history_) return;
  if (history_sequence_ ==
      static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
    Pause("account history sequence exhausted");
    return;
  }
  record.run_id = run_id_;
  record.account_id = config_.id;
  record.sequence = ++history_sequence_;
  record.at_us = std::chrono::duration_cast<std::chrono::microseconds>(
                     std::chrono::system_clock::now().time_since_epoch())
                     .count();
  record.at_mono_ns = BenchClockNowNs();
  if (!history_->TryPushAccount(std::move(record))) {
    Pause("account history enqueue failed");
  }
}

void TradingAccount::RequestStop() {
  quiescing_ = true;
  DisableAdmission();
  {
    std::lock_guard lock(queue_mutex_);
    if (stop_requested_) return;
    stop_requested_ = true;
    queue_.push_back([this] {
      ready_ = false;
      final_status_timer_.cancel();
      sync_->CancelTimer();
      order_query_timer_.cancel();
      if (port_) port_->Stop();
      for (const auto& extra : extra_ports_) extra->Stop();
      MarkAllDirty();
      FlushViews();
      {
        std::lock_guard lock(queue_mutex_);
        stopped_ = true;
      }
      work_guard_.reset();
    });
    if (drain_scheduled_) return;
    drain_scheduled_ = true;
  }
  boost::asio::post(io_, [this] { Drain(); });
}

void TradingAccount::PrepareStop() {
  quiescing_ = true;
  DisableAdmission();
  boost::asio::post(io_, GuardState([this] { sync_->CancelTimer(); }));
}

}  // namespace hquant::v1
