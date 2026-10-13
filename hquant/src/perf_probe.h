#pragma once

#include <time.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "hquant/base/ids.h"

namespace hquant::v1 {

// Optional replay diagnostics. Each Shard writes only its own slot; the
// history writer writes HistoryPerf. Read them only after Runtime::Wait().
inline uint64_t PerfNowNs() {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
}

// Benchmark wire timestamps use the same OS clock as Python's
// clock_gettime(CLOCK_MONOTONIC), including across processes.
inline uint64_t BenchClockNowNs() {
  timespec now{};
  ::clock_gettime(CLOCK_MONOTONIC, &now);
  return static_cast<uint64_t>(now.tv_sec) * 1'000'000'000ULL +
         static_cast<uint64_t>(now.tv_nsec);
}

inline uint64_t BenchUptimeNowNs() {
  timespec now{};
#ifdef CLOCK_UPTIME_RAW
  ::clock_gettime(CLOCK_UPTIME_RAW, &now);
#else
  ::clock_gettime(CLOCK_MONOTONIC, &now);
#endif
  return static_cast<uint64_t>(now.tv_sec) * 1'000'000'000ULL +
         static_cast<uint64_t>(now.tv_nsec);
}

// One Shard processes one market input at a time. The live-feed benchmark
// resets this scratch record before dispatching each frame, then snapshots it
// after Market and Shard have returned. Timer decisions are not part of it.
struct LiveHandlerPhases {
  uint64_t market_total_ns = 0;
  uint64_t shard_total_ns = 0;
};

class BenchPhaseTimer {
 public:
  explicit BenchPhaseTimer(uint64_t* elapsed_ns)
      : elapsed_ns_(elapsed_ns),
        begin_ns_(elapsed_ns ? BenchClockNowNs() : 0) {}
  BenchPhaseTimer(const BenchPhaseTimer&) = delete;
  BenchPhaseTimer& operator=(const BenchPhaseTimer&) = delete;
  ~BenchPhaseTimer() {
    if (elapsed_ns_) *elapsed_ns_ += BenchClockNowNs() - begin_ns_;
  }

 private:
  uint64_t* elapsed_ns_;
  uint64_t begin_ns_;
};

struct ShardPerf {
  // Benchmark-only connected UDP socket, one per Shard. Normal feed disables
  // it.
  int bench_ack_fd = -1;
  uint64_t bench_ack_sent = 0;
  uint64_t bench_ack_errors = 0;
  uint64_t bench_ack_bad_frame = 0;
  uint64_t bench_ack_send_errors = 0;
  int bench_ack_last_errno = 0;
  uint64_t inputs = 0;
  uint64_t input_dispatch_ns = 0;
  uint64_t market_path_ns = 0;
  uint64_t decision_ns = 0;
  std::vector<uint64_t> post_to_shard_samples_ns;
  std::vector<uint64_t> input_handler_samples_ns;
  std::vector<uint64_t> handler_to_completion_samples_ns;
  std::vector<uint64_t> decision_samples_ns;
  std::vector<uint64_t> market_action_decision_samples_ns;
  std::vector<uint64_t> timer_action_decision_samples_ns;
  std::vector<uint64_t> account_view_action_decision_samples_ns;
  std::vector<uint64_t> market_action_strategy_samples_ns;
  std::vector<uint64_t> untagged_action_strategy_samples_ns;
  std::vector<uint64_t> market_submit_strategy_samples_ns;
  std::vector<uint64_t> untagged_submit_strategy_samples_ns;
  LiveHandlerPhases live_handler_phases;
  uint64_t live_phase_invalid = 0;
  std::vector<uint64_t> live_feed_dispatch_samples_ns;
  std::vector<uint64_t> live_market_samples_ns;
  std::vector<uint64_t> live_shard_other_samples_ns;
  // Populated only for an instrumented live WebSocket run.
  std::vector<uint64_t> wire_to_read_samples_ns;
  std::vector<uint64_t> read_to_parse_samples_ns;
  std::vector<uint64_t> parse_to_market_samples_ns;
  std::vector<uint64_t> wire_to_market_samples_ns;
  std::vector<uint32_t> live_schedule_seconds;
  uint64_t live_wire_messages = 0;
  uint64_t live_bad_sender_timestamps = 0;
};

struct HistoryPerf {
  // Records are assigned a phase when they enter the writer queue.
  std::atomic<bool> stop_started{false};
  size_t queue_peak = 0;
  size_t queue_peak_before_stop = 0;
  size_t queue_peak_after_stop = 0;
  uint64_t batches = 0;
  uint64_t records_written = 0;
  uint64_t account_records_written = 0;
  uint64_t batch_write_ns = 0;
  std::vector<uint64_t> batch_write_samples_ns;
  std::vector<uint64_t> queue_wait_ns;
  std::vector<uint64_t> enqueue_to_commit_ns;
  // Same writer order as the two latency vectors; split distributions on exit.
  std::vector<uint8_t> after_stop_flags;
  std::vector<uint64_t> input_to_commit_ns;
};

struct TradePerfSnapshot {
  struct SharedViewFlushTrace {
    uint64_t started_ns = 0;
    uint64_t finished_ns = 0;
    size_t shard_count = 0;
  };
  struct RestRequestTrace {
    std::string client_id;
    bool cancel = false;
    uint64_t enqueued_ns = 0;
    uint64_t dispatched_ns = 0;
    uint64_t write_start_ns = 0;
    uint64_t write_done_ns = 0;
    uint64_t posted_ns = 0;
    size_t queued_before_post = 0;
    size_t active_lanes_on_post = 0;
    uint64_t first_write_call_ns = 0;
    uint64_t first_write_return_ns = 0;
    uint32_t write_calls = 0;
    uint64_t response_done_ns = 0;
  };
  struct PrivateClientTrace {
    uint64_t frame_seq = 0;
    uint32_t event_index = 0;
    std::string kind;
    std::string client_id;
    std::string status;
    uint64_t read_started_ns = 0;
    uint64_t read_armed_ns = 0;
    uint64_t first_socket_read_ns = 0;
    uint64_t last_socket_read_ns = 0;
    uint64_t socket_reads = 0;
    uint64_t frame_complete_ns = 0;
    uint64_t parsed_ns = 0;
    uint64_t callback_enter_ns = 0;
    uint64_t callback_done_ns = 0;
    uint64_t frame_bytes = 0;
  };
  std::vector<SharedViewFlushTrace> shared_view_flush_trace;
  std::vector<RestRequestTrace> rest_request_trace;
  std::vector<PrivateClientTrace> private_client_trace;
  std::vector<uint64_t> queue_wait_ns;
  std::vector<uint64_t> submit_queue_wait_ns;
  std::vector<uint64_t> cancel_queue_wait_ns;
  std::vector<uint64_t> query_queue_wait_ns;
  std::vector<uint64_t> connect_wait_ns;
  std::vector<uint64_t> socket_write_ns;
  std::vector<uint64_t> response_wait_ns;
  std::vector<uint64_t> queued_to_response_ns;
  std::vector<uint64_t> private_report_receive_to_commit_ns;
  std::vector<uint64_t> private_report_apply_ns;
  std::vector<uint64_t> private_balance_receive_to_commit_ns;
  std::vector<uint64_t> private_balance_apply_ns;
};

class TradePerf {
 public:
  using TimePoint = std::chrono::steady_clock::time_point;

  void RecordSharedViewFlush(TradePerfSnapshot::SharedViewFlushTrace trace) {
    std::lock_guard lock(mutex_);
    samples_.shared_view_flush_trace.push_back(trace);
  }

  void RecordRestRequest(TradePerfSnapshot::RestRequestTrace trace) {
    std::lock_guard lock(mutex_);
    samples_.rest_request_trace.push_back(std::move(trace));
  }

  void RecordPrivateClient(TradePerfSnapshot::PrivateClientTrace trace) {
    std::lock_guard lock(mutex_);
    samples_.private_client_trace.push_back(std::move(trace));
  }

  void Record(TimePoint queued, TimePoint dispatched, TimePoint write_begin,
              TimePoint write_done, TimePoint response, bool submit,
              bool cancel) {
    std::lock_guard lock(mutex_);
    const uint64_t queue_wait_ns = Nanos(dispatched - queued);
    samples_.queue_wait_ns.push_back(queue_wait_ns);
    if (submit)
      samples_.submit_queue_wait_ns.push_back(queue_wait_ns);
    else if (cancel)
      samples_.cancel_queue_wait_ns.push_back(queue_wait_ns);
    else
      samples_.query_queue_wait_ns.push_back(queue_wait_ns);
    samples_.queued_to_response_ns.push_back(Nanos(response - queued));
    if (write_begin == TimePoint{} || write_done == TimePoint{}) return;
    samples_.connect_wait_ns.push_back(Nanos(write_begin - dispatched));
    samples_.socket_write_ns.push_back(Nanos(write_done - write_begin));
    samples_.response_wait_ns.push_back(Nanos(response - write_done));
  }

  void RecordPrivate(bool order_report, uint64_t receive_to_commit_ns,
                     uint64_t apply_ns) {
    std::lock_guard lock(mutex_);
    if (order_report) {
      samples_.private_report_receive_to_commit_ns.push_back(
          receive_to_commit_ns);
      samples_.private_report_apply_ns.push_back(apply_ns);
    } else {
      samples_.private_balance_receive_to_commit_ns.push_back(
          receive_to_commit_ns);
      samples_.private_balance_apply_ns.push_back(apply_ns);
    }
  }

  TradePerfSnapshot Snapshot() const {
    std::lock_guard lock(mutex_);
    return samples_;
  }

 private:
  static uint64_t Nanos(std::chrono::steady_clock::duration duration) {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count());
  }

  mutable std::mutex mutex_;
  TradePerfSnapshot samples_;
};

struct PerfProbe {
  std::array<ShardPerf, kMaxShards> shards;
  HistoryPerf history;
  TradePerf trade;
  // Benchmark-only CPU accounting for the Runtime io_context workers.
  size_t io_worker_count = 0;
  bool performance_qos_requested = false;
  bool realtime_io_requested = false;
  std::array<int, 10> io_worker_qos_status{};
  std::array<int, 10> io_worker_realtime_status{};
  std::array<bool, 10> io_worker_realtime_verified{};
  std::array<uint64_t, 10> io_worker_cpu_ns{};
  std::array<uint64_t, 10> io_worker_wall_ns{};
};

}  // namespace hquant::v1
