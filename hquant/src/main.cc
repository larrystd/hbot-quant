#include <algorithm>
#include <csignal>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <map>
#include <string>
#include <thread>
#include <vector>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include "absl/status/status.h"
#include "gflags/gflags.h"
#include "hquant/base/fixed.h"
#include "hquant/config.h"
#include "hquant/perf_probe.h"
#include "hquant/runtime.h"

DEFINE_string(config, "", "YAML configuration path");
DEFINE_string(state_dir, "", "Runtime state directory");
DEFINE_int32(strategy_shard, -1, "Shard ID for strategy overrides");
DEFINE_string(order_amount, "", "Override order amount");
DEFINE_string(bid_spread, "", "Override bid spread");
DEFINE_string(ask_spread, "", "Override ask spread");
DEFINE_int64(refresh_ms, 0, "Override refresh period in milliseconds");
DEFINE_string(bench_output, "", "Write optional live timing probe JSON on exit");
DEFINE_int32(bench_ack_port, 0, "Benchmark-only UDP completion port on loopback");

namespace {

volatile std::sig_atomic_t stop_requested = 0;

struct BenchAckSockets {
  std::vector<int> fds;
  ~BenchAckSockets() {
    for (int fd : fds) ::close(fd);
  }
};

void SignalStop(int) { stop_requested = 1; }

bool Explicit(const char* name) {
  google::CommandLineFlagInfo info;
  return google::GetCommandLineFlagInfo(name, &info) && !info.is_default;
}

absl::StatusOr<hquant::v1::StrategyOverrides> ReadOverrides() {
  hquant::v1::StrategyOverrides overrides;
  if (Explicit("strategy_shard")) {
    if (FLAGS_strategy_shard < 0 ||
        FLAGS_strategy_shard >= hquant::v1::kMaxShards) {
      return absl::InvalidArgumentError("strategy_shard must be in [0,999]");
    }
    overrides.shard = hquant::v1::ShardId{
        static_cast<uint16_t>(FLAGS_strategy_shard)};
  }
  if (Explicit("order_amount")) {
    auto value = hquant::v1::ParseFixed(FLAGS_order_amount);
    if (!value.ok()) return value.status();
    overrides.order_amount_nanos = *value;
  }
  if (Explicit("bid_spread")) {
    auto value = hquant::v1::ParseFixed(FLAGS_bid_spread);
    if (!value.ok()) return value.status();
    overrides.bid_spread_ppb = *value;
  }
  if (Explicit("ask_spread")) {
    auto value = hquant::v1::ParseFixed(FLAGS_ask_spread);
    if (!value.ok()) return value.status();
    overrides.ask_spread_ppb = *value;
  }
  if (Explicit("refresh_ms")) overrides.refresh_ms = FLAGS_refresh_ms;
  return overrides;
}

void Distribution(std::ostream& output, const char* name,
                  std::vector<uint64_t> samples, bool comma = true) {
  std::sort(samples.begin(), samples.end());
  const auto percentile = [&](double fraction) {
    return samples.empty()
               ? 0.0
               : samples[static_cast<size_t>(fraction * (samples.size() - 1))] /
                     1000.0;
  };
  double total_us = 0;
  for (uint64_t value : samples) total_us += value / 1000.0;
  output << "  \"" << name << "\":{\"count\":" << samples.size()
         << ",\"mean_us\":"
         << (samples.empty() ? 0.0 : total_us / samples.size())
         << ",\"p50_us\":" << percentile(0.5)
         << ",\"p95_us\":" << percentile(0.95)
         << ",\"p99_us\":" << percentile(0.99)
         << ",\"p999_us\":" << percentile(0.999)
         << ",\"max_us\":" << percentile(1.0) << '}'
         << (comma ? ",\n" : "\n");
}

bool WriteBenchOutput(const std::string& path,
                      const hquant::v1::PerfProbe& probe,
                      const hquant::v1::AppConfig& config) {
  std::vector<uint64_t> wire_to_read, read_to_parse, parse_to_market;
  std::vector<uint64_t> wire_to_market, paper_match, decision;
  std::vector<uint64_t> history_enqueue;
  std::vector<uint64_t> live_feed_dispatch, live_market, live_paper;
  std::vector<uint64_t> live_account_view, live_strategy, live_executor;
  std::vector<uint64_t> live_command_history, live_shard_other;
  std::map<uint32_t, std::vector<uint64_t>> wire_by_second;
  uint64_t messages = 0, bad_timestamps = 0, account_events = 0;
  uint64_t bench_ack_sent = 0, bench_ack_errors = 0;
  uint64_t bench_ack_bad_frame = 0, bench_ack_send_errors = 0;
  int bench_ack_last_errno = 0;
  uint64_t history_attempted = 0;
  uint64_t live_phase_invalid = 0;
  const auto append = [](std::vector<uint64_t>& dest,
                         const std::vector<uint64_t>& source) {
    dest.insert(dest.end(), source.begin(), source.end());
  };
  for (const auto& shard : config.shards) {
    const auto& perf = probe.shards[shard.id.value];
    messages += perf.live_wire_messages;
    bad_timestamps += perf.live_bad_sender_timestamps;
    bench_ack_sent += perf.bench_ack_sent;
    bench_ack_errors += perf.bench_ack_errors;
    bench_ack_bad_frame += perf.bench_ack_bad_frame;
    bench_ack_send_errors += perf.bench_ack_send_errors;
    if (perf.bench_ack_last_errno) bench_ack_last_errno = perf.bench_ack_last_errno;
    account_events += perf.account_events;
    history_attempted += perf.history_records;
    live_phase_invalid += perf.live_phase_invalid;
    append(wire_to_read, perf.wire_to_read_samples_ns);
    for (size_t i = 0; i < perf.wire_to_read_samples_ns.size() &&
                       i < perf.live_schedule_seconds.size(); ++i) {
      wire_by_second[perf.live_schedule_seconds[i]].push_back(
          perf.wire_to_read_samples_ns[i]);
    }
    append(read_to_parse, perf.read_to_parse_samples_ns);
    append(parse_to_market, perf.parse_to_market_samples_ns);
    append(wire_to_market, perf.wire_to_market_samples_ns);
    append(paper_match, perf.paper_match_samples_ns);
    append(decision, perf.decision_samples_ns);
    append(history_enqueue, perf.history_enqueue_samples_ns);
    append(live_feed_dispatch, perf.live_feed_dispatch_samples_ns);
    append(live_market, perf.live_market_samples_ns);
    append(live_paper, perf.live_paper_samples_ns);
    append(live_account_view, perf.live_account_view_samples_ns);
    append(live_strategy, perf.live_strategy_samples_ns);
    append(live_executor, perf.live_executor_samples_ns);
    append(live_command_history, perf.live_command_history_samples_ns);
    append(live_shard_other, perf.live_shard_other_samples_ns);
  }
  std::ofstream output(path);
  if (!output) return false;
  output << std::fixed << std::setprecision(3)
         << "{\n  \"live_wire_messages\":" << messages
         << ",\n  \"bad_sender_timestamps\":" << bad_timestamps
         << ",\n  \"bench_ack_sent\":" << bench_ack_sent
         << ",\n  \"bench_ack_errors\":" << bench_ack_errors
         << ",\n  \"bench_ack_bad_frame\":" << bench_ack_bad_frame
         << ",\n  \"bench_ack_send_errors\":" << bench_ack_send_errors
         << ",\n  \"bench_ack_last_errno\":" << bench_ack_last_errno
         << ",\n  \"account_events\":" << account_events
         << ",\n  \"history_records_attempted\":" << history_attempted
         << ",\n  \"live_phase_invalid\":" << live_phase_invalid
         << ",\n  \"history_records_written\":" << probe.history.records_written
         << ",\n  \"history_queue_peak\":" << probe.history.queue_peak
         << ",\n  \"sqlite_transactions\":" << probe.history.batches
         << ",\n  \"sqlite_writer_busy_ms\":"
         << probe.history.batch_write_ns / 1e6
         << ",\n";
  struct SecondPeak {
    uint32_t second;
    size_t count;
    uint64_t p99_ns;
  };
  std::vector<SecondPeak> peaks;
  for (auto& [second, samples] : wire_by_second) {
    std::sort(samples.begin(), samples.end());
    peaks.push_back(SecondPeak{
        second, samples.size(),
        samples[static_cast<size_t>(0.99 * (samples.size() - 1))]});
  }
  std::sort(peaks.begin(), peaks.end(), [](const auto& left, const auto& right) {
    return left.p99_ns > right.p99_ns;
  });
  output << "  \"worst_wire_seconds\":[";
  for (size_t i = 0; i < std::min<size_t>(10, peaks.size()); ++i) {
    if (i) output << ',';
    output << "{\"second\":" << peaks[i].second
           << ",\"count\":" << peaks[i].count
           << ",\"p99_us\":" << peaks[i].p99_ns / 1000.0 << '}';
  }
  output << "],\n";
  Distribution(output, "wire_to_read", std::move(wire_to_read));
  Distribution(output, "read_to_parse", std::move(read_to_parse));
  Distribution(output, "parse_to_market", std::move(parse_to_market));
  Distribution(output, "wire_to_market", std::move(wire_to_market));
  Distribution(output, "paper_match", std::move(paper_match));
  Distribution(output, "decision", std::move(decision));
  Distribution(output, "history_enqueue", std::move(history_enqueue));
  Distribution(output, "live_feed_dispatch", std::move(live_feed_dispatch));
  Distribution(output, "live_market", std::move(live_market));
  Distribution(output, "live_paper", std::move(live_paper));
  Distribution(output, "live_account_view", std::move(live_account_view));
  Distribution(output, "live_strategy", std::move(live_strategy));
  Distribution(output, "live_executor", std::move(live_executor));
  Distribution(output, "live_command_history", std::move(live_command_history));
  Distribution(output, "live_shard_other", std::move(live_shard_other));
  Distribution(output, "sqlite_queue_wait", probe.history.queue_wait_ns);
  Distribution(output, "sqlite_enqueue_to_commit",
               probe.history.enqueue_to_commit_ns);
  Distribution(output, "sqlite_batch_write",
               probe.history.batch_write_samples_ns, false);
  output << "}\n";
  return output.good();
}

}  // namespace

int main(int argc, char** argv) {
  google::SetUsageMessage("hquant 1.0 public market / paper runtime");
  google::ParseCommandLineFlags(&argc, &argv, true);
  if (FLAGS_config.empty() || FLAGS_state_dir.empty()) {
    std::cerr << "--config and --state_dir are required\n";
    return 2;
  }
  auto config = hquant::v1::LoadConfig(FLAGS_config, FLAGS_state_dir);
  if (!config.ok()) {
    std::cerr << config.status() << '\n';
    return 2;
  }
  auto overrides = ReadOverrides();
  if (!overrides.ok()) {
    std::cerr << overrides.status() << '\n';
    return 2;
  }
  if (auto status = hquant::v1::ApplyStrategyOverrides(*config, *overrides);
      !status.ok()) {
    std::cerr << status << '\n';
    return 2;
  }
  std::error_code filesystem_error;
  std::filesystem::create_directories(FLAGS_state_dir, filesystem_error);
  if (filesystem_error) {
    std::cerr << "cannot create state_dir: " << filesystem_error.message() << '\n';
    return 2;
  }
  std::signal(SIGINT, SignalStop);
  std::signal(SIGTERM, SignalStop);
  std::unique_ptr<hquant::v1::PerfProbe> probe;
  if (!FLAGS_bench_output.empty()) {
    probe = std::make_unique<hquant::v1::PerfProbe>();
  }
  BenchAckSockets bench_ack_sockets;
  if (FLAGS_bench_ack_port != 0) {
    if (!probe || FLAGS_bench_ack_port < 1 || FLAGS_bench_ack_port > 65535) {
      std::cerr << "--bench_ack_port requires --bench_output and a valid port\n";
      return 2;
    }
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = htons(static_cast<uint16_t>(FLAGS_bench_ack_port));
    for (const auto& shard : config->shards) {
      // One socket per Shard avoids nonblocking send contention between the
      // Shard threads at the fixture's synchronized burst boundaries.
      const int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
      if (fd < 0) {
        std::cerr << "cannot create benchmark acknowledgement socket\n";
        return 2;
      }
      bench_ack_sockets.fds.push_back(fd);
      if (::connect(fd, reinterpret_cast<sockaddr*>(&address),
                    sizeof(address)) != 0) {
        std::cerr << "cannot connect benchmark acknowledgement socket\n";
        return 2;
      }
      probe->shards[shard.id.value].bench_ack_fd = fd;
    }
  }
  hquant::v1::Runtime runtime(*config, probe.get());
  if (auto status = runtime.Start(); !status.ok()) {
    std::cerr << status << '\n';
    return 1;
  }
  absl::Status replay_status = absl::OkStatus();
  if (config->input.source == hquant::v1::InputSource::Replay) {
    replay_status = runtime.RunReplay([] { return stop_requested != 0; });
  } else {
    while (!stop_requested && !runtime.StopRequested()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
  }
  const bool requested_stop = stop_requested || runtime.StopRequested();
  runtime.RequestStop();
  const absl::Status history_status = runtime.Wait();
  if (probe && !WriteBenchOutput(FLAGS_bench_output, *probe, *config)) {
    std::cerr << "cannot write --bench_output\n";
    return 1;
  }
  if (!replay_status.ok() &&
      !(absl::IsCancelled(replay_status) && requested_stop)) {
    std::cerr << replay_status << '\n';
    return 1;
  }
  if (!history_status.ok()) {
    std::cerr << history_status << '\n';
    return 1;
  }
  return 0;
}
