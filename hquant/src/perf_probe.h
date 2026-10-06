#pragma once

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <time.h>
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
  uint64_t paper_ns = 0;
  uint64_t account_view_ns = 0;
  uint64_t strategy_ns = 0;
  uint64_t executor_ns = 0;
  uint64_t command_history_ns = 0;
};

class BenchPhaseTimer {
 public:
  explicit BenchPhaseTimer(uint64_t* elapsed_ns)
      : elapsed_ns_(elapsed_ns), begin_ns_(elapsed_ns ? BenchClockNowNs() : 0) {}
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
  // Benchmark-only connected UDP socket, one per Shard. Normal feed disables it.
  int bench_ack_fd = -1;
  uint64_t bench_ack_sent = 0;
  uint64_t bench_ack_errors = 0;
  uint64_t bench_ack_bad_frame = 0;
  uint64_t bench_ack_send_errors = 0;
  int bench_ack_last_errno = 0;
  uint64_t inputs = 0;
  uint64_t input_dispatch_ns = 0;
  uint64_t market_path_ns = 0;  // includes synchronous match/decision callbacks
  uint64_t paper_match_ns = 0;
  uint64_t decision_ns = 0;  // strategy, executor, and command history enqueue
  uint64_t account_events = 0;
  uint64_t account_event_ns = 0;
  uint64_t history_records = 0;
  uint64_t history_enqueue_ns = 0;
  std::vector<uint64_t> post_to_shard_samples_ns;
  std::vector<uint64_t> input_handler_samples_ns;
  std::vector<uint64_t> handler_to_completion_samples_ns;
  std::vector<uint64_t> paper_match_samples_ns;
  std::vector<uint64_t> decision_samples_ns;
  std::vector<uint64_t> account_event_samples_ns;
  std::vector<uint64_t> history_enqueue_samples_ns;
  LiveHandlerPhases live_handler_phases;
  uint64_t live_phase_invalid = 0;
  std::vector<uint64_t> live_feed_dispatch_samples_ns;
  std::vector<uint64_t> live_market_samples_ns;
  std::vector<uint64_t> live_paper_samples_ns;
  std::vector<uint64_t> live_account_view_samples_ns;
  std::vector<uint64_t> live_strategy_samples_ns;
  std::vector<uint64_t> live_executor_samples_ns;
  std::vector<uint64_t> live_command_history_samples_ns;
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
  size_t queue_peak = 0;
  uint64_t batches = 0;
  uint64_t records_written = 0;
  uint64_t batch_write_ns = 0;
  std::vector<uint64_t> batch_write_samples_ns;
  std::vector<uint64_t> queue_wait_ns;
  std::vector<uint64_t> enqueue_to_commit_ns;
  std::vector<uint64_t> input_to_commit_ns;
};

struct PerfProbe {
  std::array<ShardPerf, kMaxShards> shards;
  HistoryPerf history;
};

}  // namespace hquant::v1
