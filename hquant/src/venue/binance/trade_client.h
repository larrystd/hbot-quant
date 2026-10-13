#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "boost/asio/awaitable.hpp"
#include "boost/asio/executor_work_guard.hpp"
#include "boost/asio/io_context.hpp"
#include "boost/asio/steady_timer.hpp"
#include "hquant/base/net.h"
#include "hquant/config.h"
#include "hquant/perf_probe.h"
#include "hquant/trading/trade_port.h"
#include "hquant/venue/binance/request_scheduler.h"
#include "hquant/venue/binance/user_stream.h"

namespace hquant::v1 {

// REST sockets can run on their owning Shard worker. Private stream and account
// callbacks remain separate from the outbound order path.
class BinanceTradeClient final : public TradePort {
 public:
  BinanceTradeClient(boost::asio::io_context& io, const AccountConfig& config,
                     std::string api_key, std::string api_secret,
                     BinanceUserStream::EventSink on_event,
                     BinanceUserStream::HealthSink on_health,
                     TradePerf* perf = nullptr, size_t queue_capacity = 8192,
                     std::shared_ptr<RequestScheduler> scheduler = nullptr,
                     std::string rule_symbol = {},
                     boost::asio::io_context* shard_rest_io = nullptr,
                     bool start_private_stream = true);
  ~BinanceTradeClient() override;

  bool Submit(SubmitCommand command, Completion done,
              WriteStarted write_started = {}) override;
  bool Cancel(CancelCommand command, Completion done,
              WriteStarted write_started = {}) override;
  bool Query(QueryCommand command, QueryCompletion done) override;
  bool QueryTrades(QueryCommand command, TradesCompletion done) override;
  bool QueryBalances(BalancesCompletion done) override;
  bool QueryOpenOrders(OpenOrdersCompletion done) override;
  bool QueryRules(RulesCompletion done) override;
  TradeQueueHealth QueueHealth() const override;
  void Stop() override;

 private:
  struct Job {
    enum class Kind {
      Submit,
      Cancel,
      Query,
      Trades,
      Balances,
      OpenOrders,
      Rules
    } kind;
    SubmitCommand submit;
    CancelCommand cancel;
    QueryCommand query;
    Completion done;
    WriteStarted write_started;
    QueryCompletion query_done;
    TradesCompletion trades_done;
    BalancesCompletion balances_done;
    OpenOrdersCompletion open_orders_done;
    RulesCompletion rules_done;
    std::vector<Fill> accumulated_trades;
    uint64_t from_trade_id = 0;
    size_t trade_pages = 0;
    std::chrono::steady_clock::time_point enqueued;
    std::chrono::steady_clock::time_point dispatched;
    std::chrono::steady_clock::time_point write_begin;
    std::chrono::steady_clock::time_point write_done;
    uint64_t enqueued_ns = 0;
    uint64_t dispatched_ns = 0;
    uint64_t write_start_ns = 0;
    uint64_t write_done_ns = 0;
    HttpWriteTrace write_trace;
    uint64_t posted_ns = 0;
    size_t queued_before_post = 0;
    size_t active_lanes_on_post = 0;
  };

  struct RestLane {
    RestLane(boost::asio::io_context& io, const AccountConfig& config)
        : http(io, config.trading_rest_host,
               std::to_string(config.trading_rest_port),
               TlsConfig{config.tls, true, {}, config.trading_rest_host}),
          schedule_timer(io) {}

    HttpClient http;
    boost::asio::steady_timer schedule_timer;
    bool draining = false;
    bool waiting_for_schedule = false;
  };

  bool Enqueue(Job&& job);
  void QueueOnRest(Job&& job);
  void StartDrains();
  static RequestClass Classify(Job::Kind kind);
  static size_t Priority(Job::Kind kind);
  std::deque<Job>& NextQueue();
  Job PopNextJob();
  HttpRequest MakeRequest(Job& job) const;
  boost::asio::awaitable<void> Drain(RestLane& lane);
  void CompleteTrade(Job& job, TradeResponse response);
  void Complete(Job& job, absl::StatusOr<HttpResponse> response);

  boost::asio::io_context& io_;
  boost::asio::io_context owned_rest_io_;
  boost::asio::io_context& rest_io_;
  boost::asio::executor_work_guard<boost::asio::io_context::executor_type>
      rest_guard_;
  boost::asio::executor_work_guard<boost::asio::io_context::executor_type>
      account_guard_;
  boost::asio::io_context private_io_;
  boost::asio::executor_work_guard<boost::asio::io_context::executor_type>
      private_guard_;
  std::vector<std::unique_ptr<RestLane>> rest_lanes_;
  BinanceUserStream user_stream_;
  std::shared_ptr<RequestScheduler> scheduler_;
  std::string api_key_;
  std::string api_secret_;
  std::string rule_symbol_;
  bool private_stream_enabled_ = true;
  size_t queue_capacity_;
  std::chrono::seconds submit_queue_timeout_;
  TradePerf* perf_ = nullptr;
  std::thread rest_worker_;
  std::thread private_worker_;
  std::array<std::deque<Job>, 6> queues_;
  size_t queued_jobs_ = 0;
  size_t active_lanes_ = 0;
  bool rest_stop_processed_ = false;
  bool rest_stop_finished_ = false;
  std::atomic<size_t> accepted_queued_jobs_{0};
  std::atomic<bool> stopped_{false};
  std::atomic<size_t> queue_peak_{0};
  std::atomic<uint64_t> queue_rejections_{0};
  void FinishRestStop();
};

}  // namespace hquant::v1
