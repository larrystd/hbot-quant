#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "boost/asio/executor_work_guard.hpp"
#include "boost/asio/io_context.hpp"
#include "boost/asio/steady_timer.hpp"
#include "hquant/config.h"
#include "hquant/sqlite_history.h"
#include "hquant/trading/funds_state.h"
#include "hquant/trading/order_journal.h"
#include "hquant/trading/order_manager.h"
#include "hquant/trading/trade_port.h"

namespace hquant::v1 {

class AccountSync;
class AccountViewPublisher;

struct AssetViewValues {
  uint64_t revision = 0;
  uint64_t total_nanos = 0;
  uint64_t available_nanos = 0;
  uint64_t budget_nanos = 0;
};

// 账户线程写入，Shard 线程读取；版本两次相同且为偶数时值才一致。
class PublishedAssetView {
 public:
  AssetViewValues Read() const;
  void Publish(uint64_t total, uint64_t available, uint64_t budget);

 private:
  std::atomic<uint64_t> sequence_{0};
  std::atomic<uint64_t> total_nanos_{0};
  std::atomic<uint64_t> available_nanos_{0};
  std::atomic<uint64_t> budget_nanos_{0};
};

struct AccountView {
  uint64_t revision = 0;
  TradingRule rule;
  std::optional<VenueRule> venue_rule;
  uint64_t maker_fee_rate_ppb = 0;
  std::vector<OrderId> open_order_ids;
  std::vector<OrderId> pending_cancel_ids;
  std::vector<OrderId> unknown_cancel_ids;
  std::vector<OrderId> rejected_cancel_ids;
  bool pending_operation = false;
  bool ready = false;
  uint64_t base_total_nanos = 0;
  uint64_t base_available_nanos = 0;
  uint64_t quote_total_nanos = 0;
  uint64_t quote_available_nanos = 0;
  uint64_t base_budget_nanos = 0;
  uint64_t quote_budget_nanos = 0;
};

struct AccountStatus {
  std::string id;
  uint64_t revision = 0;
  size_t queued = 0;
  size_t open_orders = 0;
  size_t unknown_orders = 0;
  bool ready = true;
  bool private_connected = false;
  bool syncing = false;
  bool manual_paused = false;
  size_t pending_journal = 0;
  size_t account_queue_peak = 0;
  uint64_t account_queue_rejections = 0;
  size_t journal_pending_peak = 0;
  uint64_t journal_rejections = 0;
  TradeQueueHealth trade_queue;
  std::string error;
  std::map<std::string, AssetFunds> balances;
  std::map<std::string, uint64_t> available_nanos;
  std::map<std::string, uint64_t> reserved_nanos;
};

enum class AccountAction : uint8_t {
  PauseNewOrders,
  ResumeTrading,
  SyncAccountState,
  CancelAll,
};

// 每个交易账户一个实例。Shard 本地接纳订单；本对象异步维护交易所对账副本。
class TradingAccount {
 public:
  using ViewSink = std::function<void(AccountView)>;
  struct TradeSink {
    std::function<void(AccountEvent)> event;
    std::function<void(ClientOrderId, bool, TradeResponse)> response;
    std::function<void(ClientOrderId, bool)> writing;
    std::function<void(std::function<void(uint64_t)>)> cancel_all;
  };

  TradingAccount(boost::asio::io_context& io, AccountConfig config,
                 std::vector<ShardConfig> shards, std::string run_id,
                 SqliteHistory* history = nullptr,
                 size_t queue_capacity = 8192, TradePerf* trade_perf = nullptr);
  ~TradingAccount();
  TradingAccount(const TradingAccount&) = delete;
  TradingAccount& operator=(const TradingAccount&) = delete;

  void SetPort(std::unique_ptr<TradePort> port);
  TradePort* AddShardPort(std::unique_ptr<TradePort> port);
  void SetShardPort(ShardId shard, TradePort* port);
  void SetJournal(OrderJournal* journal) { journal_ = journal; }
  absl::Status RestoreJournal(const std::vector<JournalEntry>& entries);
  void SetViewSink(ShardId shard, ViewSink sink);
  void SetTradeSink(ShardId shard, TradeSink sink);
  bool SubmitFromShard(const TrackedOrder& order, std::string asset,
                       uint64_t amount_nanos, uint64_t view_revision);
  bool CancelFromShard(ShardId shard, CancelCommand command,
                       uint64_t source_bench_seq, uint64_t requested_ms);
  bool AdmissionEnabled(uint64_t view_revision) const {
    return admission_enabled_.load(std::memory_order_acquire) &&
           view_revision >=
               min_ready_revision_.load(std::memory_order_acquire);
  }
  void ReportShardError(ShardId shard, absl::Status error);
  std::shared_ptr<const PublishedAssetView> AssetView(
      const std::string& asset) const;
  std::shared_ptr<const std::atomic<uint64_t>> AssetRevision() const;
  bool PostView(ShardId shard, ViewSink done);
  bool PostStatus(std::function<void(AccountStatus)> done);
  bool PostFinalStatus(std::function<void(AccountStatus)> done);
  bool PostControl(AccountAction action, std::string operator_id,
                   std::string reason,
                   std::function<void(absl::StatusOr<uint64_t>)> done);
  // 同线程直接处理，其他线程通过账户任务队列转入 io_。
  void OnVenueEvent(AccountEvent event);
  void OnConnectionState(bool connected, std::string reason);
  void PrepareStop();
  void RequestStop();
  const std::string& Id() const { return config_.id; }

 private:
  template <typename Callback>
  auto GuardState(Callback callback) {
    return [this, callback = std::move(callback)](auto&&... args) mutable {
      std::lock_guard lock(state_mutex_);
      callback(std::forward<decltype(args)>(args)...);
    };
  }

  // 控制、状态与跨线程回报任务仍由账户 io 线程分批执行。
  bool TryEnqueue(std::function<void()> task);
  void Drain();
  void SubmitResponse(ClientOrderId id, TradeResponse response);
  void CancelResponse(ClientOrderId id, TradeResponse response);
  // 交易所事实先提交可靠日志，再更新订单、资金和 Shard 视图。
  bool PersistAndApply(AccountEvent event, bool from_venue,
                       std::function<void(absl::Status)> done = {});
  bool Persist(JournalEntry entry, std::function<void(absl::Status)> done);
  void FinishFinalStatus(std::vector<BalanceUpdate> balances,
                         std::function<void(AccountStatus)> done);
  void PersistSubmitResponse(ClientOrderId id, TradeResponse response);
  void PersistCancelResponse(ClientOrderId id, TradeResponse response);
  void OnWriting(ClientOrderId id, bool cancel);
  // 全账户同步由 AccountSync 执行；单笔超时由 QueryDueOrder 处理。
  void SyncFromVenue();
  void ScheduleOrderQuery();
  void QueryDueOrder();
  void QueryOrderResult(QueryCommand query, absl::StatusOr<OrderReport> result);
  void ApplyQueriedTrades(QueryCommand query, OrderReport report,
                          std::vector<Fill> trades, size_t index);
  void ApplyEvent(const AccountEvent& event, bool from_venue = false);
  void MarkAssetDirty(const std::string& asset);
  void MarkAllDirty();
  void FlushViews();
  void PublishAllAssetViews();
  AccountView View(ShardId shard) const;
  AccountStatus Status() const;
  uint64_t Budget(const std::string& asset) const;
  absl::StatusOr<uint64_t> ReserveAmount(
      const MarketConfig& market, const LimitOrderRequest& request) const;
  Order ReportOrder(const TrackedOrder& tracked) const;
  void Pause(std::string error);
  void Record(AccountHistoryRecord record);
  void NotifyResponse(const ClientOrderId& id, bool cancel,
                      TradeResponse response);
  void NotifyWriting(const ClientOrderId& id, bool cancel);
  void NotifyEvent(const AccountEvent& event);
  void DisableAdmission();
  void EnableAdmission();
  bool AccountQueueEmpty() const;

  boost::asio::io_context& io_;
  boost::asio::executor_work_guard<boost::asio::io_context::executor_type>
      work_guard_;
  boost::asio::steady_timer final_status_timer_;
  boost::asio::steady_timer order_query_timer_;
  AccountConfig config_;
  std::string run_id_;
  SqliteHistory* history_ = nullptr;
  TradePerf* trade_perf_ = nullptr;
  OrderJournal* journal_ = nullptr;
  size_t queue_capacity_;
  std::map<uint16_t, MarketConfig> markets_;
  std::map<std::string, VenueRule> venue_rules_;
  FundsState funds_;
  OrderManager order_manager_;
  std::unique_ptr<TradePort> port_;
  std::vector<std::unique_ptr<TradePort>> extra_ports_;
  std::array<TradePort*, kMaxShards> shard_ports_{};
  // TradePort 可以同步回调；账户状态的同线程重入也使用这同一把锁。
  mutable std::recursive_mutex state_mutex_;
  mutable std::shared_mutex admission_gate_mutex_;
  mutable std::mutex journal_mutex_;
  // 锁只保护账户任务队列、调度标志和队列统计，不保护订单或资金状态。
  mutable std::mutex queue_mutex_;
  std::deque<std::function<void()>> queue_;
  bool drain_scheduled_ = false;
  size_t queue_peak_ = 0;
  uint64_t queue_rejections_ = 0;
  bool stopped_ = false;
  std::atomic<bool> stop_requested_{false};
  std::atomic<bool> quiescing_{false};
  std::atomic<bool> admission_enabled_{false};
  std::atomic<uint64_t> min_ready_revision_{0};
  bool ready_ = true;
  std::atomic<uint64_t> demo_submissions_{0};
  uint64_t history_sequence_ = 0;
  uint64_t journal_sequence_ = 0;
  std::atomic<size_t> pending_journal_{0};
  std::atomic<size_t> pending_private_events_{0};
  std::atomic<size_t> journal_pending_peak_{0};
  std::atomic<uint64_t> journal_rejections_{0};
  bool replaying_ = false;
  bool manual_paused_ = false;
  bool private_connected_ = false;
  std::string error_;
  std::unique_ptr<AccountViewPublisher> views_;
  std::unique_ptr<AccountSync> sync_;
  std::map<uint16_t, TradeSink> trade_sinks_;
};

}  // namespace hquant::v1
