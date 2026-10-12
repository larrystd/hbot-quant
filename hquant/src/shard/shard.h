#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <variant>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "boost/asio/executor_work_guard.hpp"
#include "boost/asio/io_context.hpp"
#include "boost/asio/steady_timer.hpp"
#include "hquant/config.h"
#include "hquant/perf_probe.h"
#include "hquant/shard/market.h"
#include "hquant/shard/market_type.h"
#include "hquant/shard/strategy.h"
#include "hquant/trading/trading_account.h"

namespace hquant::v1 {

struct TimerTick {};

struct ReplayRecord {
  ShardId target;
  InputTime time;
  std::variant<MarketInput, TimerTick> body;
  uint64_t perf_post_ns = 0;
};

struct StrategyPatch {
  std::optional<uint64_t> order_amount_nanos;
  std::optional<uint64_t> bid_spread_ppb;
  std::optional<uint64_t> ask_spread_ppb;
  std::optional<int64_t> refresh_ms;
};

struct StrategyUpdateResult {
  uint64_t config_version = 0;
  uint64_t actions = 0;
};

struct ShardStatus {
  ShardId id;
  bool stop_requested = false;
  bool worker_exited = false;
  BookState book_state = BookState::Syncing;
  uint64_t connection_epoch = 0;
  uint64_t last_book_sequence = 0;
  uint64_t applied_depth_events = 0;
  uint64_t accepted_trades = 0;
  uint64_t last_trade_id = 0;
  uint64_t depth_digest = 0;
  uint64_t trade_digest = 0;
  std::vector<BookLevel> bids;
  std::vector<BookLevel> asks;
  uint64_t market_notices = 0;
  uint64_t strategy_timer_ticks = 0;
  uint64_t config_version = 1;
  size_t active_orders = 0;
  uint64_t base_total_nanos = 0;
  uint64_t base_available_nanos = 0;
  uint64_t quote_total_nanos = 0;
  uint64_t quote_available_nanos = 0;
  std::string last_error;
};

class Shard {
 public:
  Shard(boost::asio::io_context& io, const ShardConfig& config,
        const InputConfig& input,
        ShardPerf* perf = nullptr,
        TradingAccount* account = nullptr);
  ~Shard();
  Shard(const Shard&) = delete;
  Shard& operator=(const Shard&) = delete;

  absl::Status Start();
  void PostReplay(ReplayRecord record, std::function<void(absl::Status)> done);
  void PostStatus(std::function<void(ShardStatus)> done);
  void PostAccountView(AccountView view);
  void PostStrategyUpdate(
      uint64_t expected_version, StrategyPatch patch,
      std::function<void(absl::StatusOr<StrategyUpdateResult>)> done);
  void RequestStop();
  void PostStopped(std::function<void()> done);
  void Join();
  ShardId Id() const { return config_.id; }

 private:
  void OnMarket(const MarketNotice& notice);
  void OnStrategyTimer();
  void ScheduleStrategyTimer();
  void StopOnThread();
  void StopAccountOrders();
  void FinishStopOnThread();
  void StopFromError(absl::Status error);
  absl::StatusOr<uint64_t> RunStrategyDecision(
      LiveHandlerPhases* phases = nullptr,
      uint64_t source_bench_seq = 0);
  void OnAccountView(AccountView view);
  absl::StatusOr<uint64_t> RunAccountDecision(uint64_t source_bench_seq);
  void AccountRequestDone(bool admitted, std::string reason);
  struct AssetPair {
    AssetViewValues base;
    AssetViewValues quote;
  };
  AssetPair ReadAssetViews() const;
  void MaybeCompleteReplay();
  void CompleteStop();
  ShardStatus CaptureStatus() const;

  ShardConfig config_;
  InputConfig input_;
  boost::asio::io_context& io_;
  boost::asio::executor_work_guard<boost::asio::io_context::executor_type>
      work_guard_;
  Market market_;
  Strategy strategy_;
  TradingAccount* account_ = nullptr;
  AccountView account_view_;
  std::shared_ptr<const PublishedAssetView> base_asset_view_;
  std::shared_ptr<const PublishedAssetView> quote_asset_view_;
  std::shared_ptr<const std::atomic<uint64_t>> asset_revision_;
  uint64_t pending_account_requests_ = 0;
  bool awaiting_cancel_ = false;
  std::function<void()> replay_done_;
  boost::asio::steady_timer strategy_timer_;
  boost::asio::steady_timer stop_timer_;
  std::set<uint64_t> stop_cancel_attempted_;
  std::atomic<bool> started_{false};
  std::atomic<bool> stop_requested_{false};
  std::atomic<bool> worker_exited_{false};
  bool stopped_on_thread_ = false;
  std::mutex stop_completion_mutex_;
  bool stop_complete_ = false;
  std::vector<std::function<void()>> stop_waiters_;
  std::optional<InputTime> last_input_time_;
  uint64_t market_notices_ = 0;
  uint64_t strategy_timer_ticks_ = 0;
  uint64_t config_version_ = 1;
  std::string last_error_;
  std::optional<absl::Status> fatal_status_;
  ShardPerf* perf_ = nullptr;
};

}  // namespace hquant::v1
