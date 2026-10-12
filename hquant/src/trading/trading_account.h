#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
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

// 每个交易账户一个实例。运行期订单、资金与同步状态只在 io_ 所属线程修改；
// Runtime 在启动前注入交易连接、日志和 Shard 视图回调。
class TradingAccount {
 public:
  using ViewSink = std::function<void(AccountView)>;
  using Admission = std::function<void(bool, std::string)>;

  TradingAccount(boost::asio::io_context& io, AccountConfig config,
                 std::vector<ShardConfig> shards, std::string run_id,
                 SqliteHistory* history = nullptr,
                 size_t queue_capacity = 8192, TradePerf* trade_perf = nullptr);
  ~TradingAccount();
  TradingAccount(const TradingAccount&) = delete;
  TradingAccount& operator=(const TradingAccount&) = delete;

  void SetPort(std::unique_ptr<TradePort> port);
  void SetJournal(OrderJournal* journal) { journal_ = journal; }
  absl::Status RestoreJournal(const std::vector<JournalEntry>& entries);
  void SetViewSink(ShardId shard, ViewSink sink);
  std::shared_ptr<const PublishedAssetView> AssetView(
      const std::string& asset) const;
  std::shared_ptr<const std::atomic<uint64_t>> AssetRevision() const;
  // 返回值仅表示账户任务入队；done 表示本地接纳，不代表交易所确认。
  bool TrySubmit(ShardId shard, MarketId market, LimitOrderRequest order,
                 uint64_t source_bench_seq, Admission done);
  bool TryCancel(ShardId shard, OrderId order, Admission done);
  bool TryCancel(ShardId shard, OrderId order, uint64_t source_bench_seq,
                 Admission done);
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
  struct QueueProbe {
    uint64_t enqueued_ns = 0;
    size_t tasks_ahead = 0;
    bool drain_scheduled = false;
  };
  // 多个调用线程入队，由账户线程分批取出执行。
  bool TryEnqueue(std::function<void()> task, QueueProbe* probe = nullptr);
  void Drain();
  void ProcessSubmit(ShardId shard, const MarketId& market,
                     const LimitOrderRequest& request,
                     uint64_t source_bench_seq, uint64_t requested_ns,
                     uint64_t started_ns, Admission done);
  void ProcessCancel(ShardId shard, OrderId id, uint64_t source_bench_seq,
                     Admission done);
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
  // 锁只保护账户任务队列、调度标志和队列统计，不保护订单或资金状态。
  mutable std::mutex queue_mutex_;
  std::deque<std::function<void()>> queue_;
  bool drain_scheduled_ = false;
  size_t queue_peak_ = 0;
  uint64_t queue_rejections_ = 0;
  bool stopped_ = false;
  std::atomic<bool> stop_requested_{false};
  std::atomic<bool> quiescing_{false};
  // TradePort 同步回调期间暂缓发布中间视图。
  bool processing_port_ = false;
  bool ready_ = true;
  uint64_t demo_submissions_ = 0;
  uint64_t history_sequence_ = 0;
  uint64_t journal_sequence_ = 0;
  size_t pending_journal_ = 0;
  size_t journal_pending_peak_ = 0;
  uint64_t journal_rejections_ = 0;
  bool replaying_ = false;
  bool manual_paused_ = false;
  bool private_connected_ = false;
  std::string error_;
  std::unique_ptr<AccountViewPublisher> views_;
  std::unique_ptr<AccountSync> sync_;
};

}  // namespace hquant::v1
