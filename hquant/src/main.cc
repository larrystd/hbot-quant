#include <netinet/in.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <format>
#include <iterator>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "absl/status/status.h"
#include "gflags/gflags.h"
#include "hquant/base/fixed.h"
#include "hquant/base/log.h"
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
DEFINE_string(bench_output, "",
              "Write optional live timing probe JSON on exit");
DEFINE_int32(bench_ack_port, 0,
             "Benchmark-only UDP completion port on loopback");

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
    overrides.shard =
        hquant::v1::ShardId{static_cast<uint16_t>(FLAGS_strategy_shard)};
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

void Distribution(std::string& output, std::string_view name,
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
  std::format_to(std::back_inserter(output),
                 "  \"{}\":{{\"count\":{},\"mean_us\":{:.3f},\"p50_us\":{:.3f},"
                 "\"p95_us\":{:.3f},\"p99_us\":{:.3f},\"p999_us\":{:.3f},"
                 "\"max_us\":{:.3f}}}{}",
                 name, samples.size(),
                 samples.empty() ? 0.0 : total_us / samples.size(),
                 percentile(0.5), percentile(0.95), percentile(0.99),
                 percentile(0.999), percentile(1.0), comma ? ",\n" : "\n");
}

std::string JsonString(std::string_view text) {
  std::string result;
  result.reserve(text.size() + 2);
  result += '"';
  for (unsigned char ch : text) {
    switch (ch) {
      case '"':
        result += "\\\"";
        break;
      case '\\':
        result += "\\\\";
        break;
      case '\b':
        result += "\\b";
        break;
      case '\f':
        result += "\\f";
        break;
      case '\n':
        result += "\\n";
        break;
      case '\r':
        result += "\\r";
        break;
      case '\t':
        result += "\\t";
        break;
      default:
        if (ch < 0x20) {
          std::format_to(std::back_inserter(result), "\\u{:04x}", ch);
        } else {
          result += static_cast<char>(ch);
        }
    }
  }
  result += '"';
  return result;
}

bool WriteTextFile(const std::string& path, std::string_view text) {
  std::FILE* file = std::fopen(path.c_str(), "wb");
  if (!file) return false;
  const bool written =
      std::fwrite(text.data(), 1, text.size(), file) == text.size();
  const bool closed = std::fclose(file) == 0;
  return written && closed;
}

bool WritePrivateClientTrace(
    const std::string& path,
    const std::vector<hquant::v1::TradePerfSnapshot::PrivateClientTrace>& rows) {
  std::string output;
  output.reserve(rows.size() * 210 + 320);
  output += "frame_seq,event_index,kind,client_id,status,read_started_ns,"
            "read_armed_ns,first_socket_read_ns,last_socket_read_ns,"
            "socket_reads,frame_complete_ns,parsed_ns,callback_enter_ns,"
            "callback_done_ns,frame_bytes\n";
  for (const auto& row : rows) {
    std::format_to(
        std::back_inserter(output),
        "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}\n",
        row.frame_seq, row.event_index, row.kind, row.client_id, row.status,
        row.read_started_ns, row.read_armed_ns, row.first_socket_read_ns,
        row.last_socket_read_ns, row.socket_reads, row.frame_complete_ns,
        row.parsed_ns, row.callback_enter_ns, row.callback_done_ns,
        row.frame_bytes);
  }
  return WriteTextFile(path, output);
}

bool WriteRestClientTrace(
    const std::string& path,
    const std::vector<hquant::v1::TradePerfSnapshot::RestRequestTrace>& rows) {
  std::string output;
  output.reserve(rows.size() * 72 + 64);
  output += "client_id,kind,enqueued_ns,dispatched_ns,write_start_ns,write_done_ns,posted_ns,queued_before_post,active_lanes_on_post,first_write_call_ns,first_write_return_ns,write_calls,response_done_ns\n";
  for (const auto& row : rows) {
    std::format_to(std::back_inserter(output), "{},{},{},{},{},{},{},{},{},{},{},{},{}\n", row.client_id,
                   row.cancel ? "CANCEL" : "SUBMIT", row.enqueued_ns,
                   row.dispatched_ns,
                   row.write_start_ns, row.write_done_ns, row.posted_ns,
                   row.queued_before_post, row.active_lanes_on_post,
                   row.first_write_call_ns, row.first_write_return_ns,
                   row.write_calls, row.response_done_ns);
  }
  return WriteTextFile(path, output);
}

bool WriteSharedViewFlushTrace(
    const std::string& path,
    const std::vector<hquant::v1::TradePerfSnapshot::SharedViewFlushTrace>&
        rows) {
  std::string output;
  output.reserve(rows.size() * 56 + 64);
  output += "started_ns,finished_ns,shard_count\n";
  for (const auto& row : rows)
    std::format_to(std::back_inserter(output), "{},{},{}\n", row.started_ns,
                   row.finished_ns, row.shard_count);
  return WriteTextFile(path, output);
}

bool WriteBenchOutput(const std::string& path,
                      const hquant::v1::PerfProbe& probe,
                      const hquant::v1::AppConfig& config,
                      const std::vector<hquant::v1::AccountStatus>& accounts) {
  std::vector<uint64_t> wire_to_read, read_to_parse, parse_to_market;
  std::vector<uint64_t> wire_to_market, decision;
  std::vector<uint64_t> market_action_decision, timer_action_decision;
  std::vector<uint64_t> account_view_action_decision;
  std::vector<uint64_t> market_action_strategy, untagged_action_strategy;
  std::vector<uint64_t> market_submit_strategy, untagged_submit_strategy;
  std::vector<uint64_t> live_feed_dispatch, live_market, live_shard_other;
  std::map<uint32_t, std::vector<uint64_t>> wire_by_second;
  uint64_t messages = 0, bad_timestamps = 0;
  uint64_t bench_ack_sent = 0, bench_ack_errors = 0;
  uint64_t bench_ack_bad_frame = 0, bench_ack_send_errors = 0;
  int bench_ack_last_errno = 0;
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
    if (perf.bench_ack_last_errno)
      bench_ack_last_errno = perf.bench_ack_last_errno;
    live_phase_invalid += perf.live_phase_invalid;
    append(wire_to_read, perf.wire_to_read_samples_ns);
    for (size_t i = 0; i < perf.wire_to_read_samples_ns.size() &&
                       i < perf.live_schedule_seconds.size();
         ++i) {
      wire_by_second[perf.live_schedule_seconds[i]].push_back(
          perf.wire_to_read_samples_ns[i]);
    }
    append(read_to_parse, perf.read_to_parse_samples_ns);
    append(parse_to_market, perf.parse_to_market_samples_ns);
    append(wire_to_market, perf.wire_to_market_samples_ns);
    append(decision, perf.decision_samples_ns);
    append(market_action_decision, perf.market_action_decision_samples_ns);
    append(timer_action_decision, perf.timer_action_decision_samples_ns);
    append(account_view_action_decision,
           perf.account_view_action_decision_samples_ns);
    append(market_action_strategy, perf.market_action_strategy_samples_ns);
    append(untagged_action_strategy,
           perf.untagged_action_strategy_samples_ns);
    append(market_submit_strategy, perf.market_submit_strategy_samples_ns);
    append(untagged_submit_strategy, perf.untagged_submit_strategy_samples_ns);
    append(live_feed_dispatch, perf.live_feed_dispatch_samples_ns);
    append(live_market, perf.live_market_samples_ns);
    append(live_shard_other, perf.live_shard_other_samples_ns);
  }
  if (probe.history.after_stop_flags.size() != probe.history.records_written ||
      probe.history.after_stop_flags.size() !=
          probe.history.queue_wait_ns.size() ||
      probe.history.after_stop_flags.size() !=
          probe.history.enqueue_to_commit_ns.size()) {
    return false;
  }
  std::vector<uint64_t> queue_wait_before_stop, queue_wait_after_stop;
  std::vector<uint64_t> commit_before_stop, commit_after_stop;
  for (size_t i = 0; i < probe.history.after_stop_flags.size(); ++i) {
    if (probe.history.after_stop_flags[i]) {
      queue_wait_after_stop.push_back(probe.history.queue_wait_ns[i]);
      commit_after_stop.push_back(probe.history.enqueue_to_commit_ns[i]);
    } else {
      queue_wait_before_stop.push_back(probe.history.queue_wait_ns[i]);
      commit_before_stop.push_back(probe.history.enqueue_to_commit_ns[i]);
    }
  }
  std::string output;
  output.reserve(8192);
  std::format_to(
      std::back_inserter(output),
      "{{\n  \"live_wire_messages\":{},\n  \"bad_sender_timestamps\":{},"
      "\n  \"bench_ack_sent\":{},\n  \"bench_ack_errors\":{},"
      "\n  \"bench_ack_bad_frame\":{},\n  \"bench_ack_send_errors\":{},"
      "\n  \"bench_ack_last_errno\":{},\n  \"account_events\":{},"
      "\n  \"live_phase_invalid\":{},\n  \"history_records_written\":{},"
      "\n  \"account_records_written\":{},"
      "\n  \"history_records_before_stop\":{},"
      "\n  \"history_records_after_stop\":{},"
      "\n  \"history_queue_peak\":{},"
      "\n  \"history_queue_peak_after_stop\":{},"
      "\n  \"history_queue_peak_all\":{},"
      "\n  \"sqlite_transactions\":{},"
      "\n  \"sqlite_writer_busy_ms\":{:.3f},\n",
      messages, bad_timestamps, bench_ack_sent, bench_ack_errors,
      bench_ack_bad_frame, bench_ack_send_errors, bench_ack_last_errno,
      probe.history.account_records_written, live_phase_invalid,
      probe.history.records_written, probe.history.account_records_written,
      queue_wait_before_stop.size(), queue_wait_after_stop.size(),
      probe.history.queue_peak_before_stop, probe.history.queue_peak_after_stop,
      probe.history.queue_peak, probe.history.batches,
      probe.history.batch_write_ns / 1e6);
  struct SecondPeak {
    uint32_t second;
    size_t count;
    uint64_t p99_ns;
  };
  std::vector<SecondPeak> peaks;
  for (auto& [second, samples] : wire_by_second) {
    std::sort(samples.begin(), samples.end());
    peaks.push_back(
        SecondPeak{second, samples.size(),
                   samples[static_cast<size_t>(0.99 * (samples.size() - 1))]});
  }
  std::sort(peaks.begin(), peaks.end(),
            [](const auto& left, const auto& right) {
              return left.p99_ns > right.p99_ns;
            });
  rusage usage{};
  if (::getrusage(RUSAGE_SELF, &usage) != 0) return false;
  const double server_cpu_seconds =
      usage.ru_utime.tv_sec + usage.ru_utime.tv_usec / 1e6 +
      usage.ru_stime.tv_sec + usage.ru_stime.tv_usec / 1e6;
  std::format_to(
      std::back_inserter(output),
      "  \"performance_qos_requested\":{},\n  \"realtime_io_requested\":{},"
      "\n  \"server_cpu_seconds\":{:.3f},\n  \"io_worker_cpu_seconds\":[",
      probe.performance_qos_requested, probe.realtime_io_requested,
      server_cpu_seconds);
  for (size_t i = 0; i < probe.io_worker_count; ++i) {
    if (i) output += ',';
    std::format_to(std::back_inserter(output), "{:.3f}",
                   probe.io_worker_cpu_ns[i] / 1e9);
  }
  output += "],\n  \"io_worker_wall_seconds\":[";
  for (size_t i = 0; i < probe.io_worker_count; ++i) {
    if (i) output += ',';
    std::format_to(std::back_inserter(output), "{:.3f}",
                   probe.io_worker_wall_ns[i] / 1e9);
  }
  output += "],\n  \"io_worker_qos_status\":[";
  for (size_t i = 0; i < probe.io_worker_count; ++i) {
    if (i) output += ',';
    std::format_to(std::back_inserter(output), "{}",
                   probe.io_worker_qos_status[i]);
  }
  output += "],\n  \"io_worker_realtime_status\":[";
  for (size_t i = 0; i < probe.io_worker_count; ++i) {
    if (i) output += ',';
    std::format_to(std::back_inserter(output), "{}",
                   probe.io_worker_realtime_status[i]);
  }
  output += "],\n  \"io_worker_realtime_verified\":[";
  for (size_t i = 0; i < probe.io_worker_count; ++i) {
    if (i) output += ',';
    std::format_to(std::back_inserter(output), "{}",
                   probe.io_worker_realtime_verified[i]);
  }
  output += "],\n  \"final_accounts\":[";
  for (const auto& account : accounts) {
    if (&account != &accounts.front()) output += ',';
    std::format_to(
        std::back_inserter(output),
        "{{\"id\":{},\"ready\":{},\"queued\":{},\"open_orders\":{},"
        "\"unknown_orders\":{},\"private_connected\":{},\"syncing\":{},"
        "\"manual_paused\":{},\"pending_journal\":{},"
        "\"account_queue_peak\":{},\"account_queue_rejections\":{},"
        "\"journal_pending_peak\":{},\"journal_rejections\":{},"
        "\"trade_queue\":{{\"queued\":{},\"peak\":{},\"rejected\":{}}},"
        "\"error\":{},\"balances\":{{",
        JsonString(account.id), account.ready, account.queued,
        account.open_orders, account.unknown_orders, account.private_connected,
        account.syncing, account.manual_paused, account.pending_journal,
        account.account_queue_peak, account.account_queue_rejections,
        account.journal_pending_peak, account.journal_rejections,
        account.trade_queue.queued, account.trade_queue.peak,
        account.trade_queue.rejected, JsonString(account.error));
    bool first_asset = true;
    for (const auto& [asset, balance] : account.balances) {
      if (!first_asset) output += ',';
      first_asset = false;
      std::format_to(
          std::back_inserter(output),
          "{}:{{\"total_nanos\":{},\"free_nanos\":{},\"locked_nanos\":{},"
          "\"available_nanos\":{},\"reserved_nanos\":{}}}",
          JsonString(asset), balance.total_nanos, balance.free_nanos,
          balance.locked_nanos, account.available_nanos.at(asset),
          account.reserved_nanos.at(asset));
    }
    output += "}}";
  }
  output += "],\n  \"worst_wire_seconds\":[";
  for (size_t i = 0; i < std::min<size_t>(10, peaks.size()); ++i) {
    if (i) output += ',';
    std::format_to(std::back_inserter(output),
                   "{{\"second\":{},\"count\":{},\"p99_us\":{:.3f}}}",
                   peaks[i].second, peaks[i].count, peaks[i].p99_ns / 1000.0);
  }
  output += "],\n";
  Distribution(output, "wire_to_read", std::move(wire_to_read));
  Distribution(output, "read_to_parse", std::move(read_to_parse));
  Distribution(output, "parse_to_market", std::move(parse_to_market));
  Distribution(output, "wire_to_market", std::move(wire_to_market));
  Distribution(output, "decision", std::move(decision));
  Distribution(output, "market_action_decision",
               std::move(market_action_decision));
  Distribution(output, "timer_action_decision",
               std::move(timer_action_decision));
  Distribution(output, "account_view_action_decision",
               std::move(account_view_action_decision));
  Distribution(output, "market_action_strategy",
               std::move(market_action_strategy));
  Distribution(output, "untagged_action_strategy",
               std::move(untagged_action_strategy));
  Distribution(output, "market_submit_strategy",
               std::move(market_submit_strategy));
  Distribution(output, "untagged_submit_strategy",
               std::move(untagged_submit_strategy));
  Distribution(output, "live_feed_dispatch", std::move(live_feed_dispatch));
  Distribution(output, "live_market", std::move(live_market));
  Distribution(output, "live_shard_other", std::move(live_shard_other));
  auto trade = probe.trade.Snapshot();
  if (!WriteSharedViewFlushTrace(
          (std::filesystem::path(path).parent_path() /
           "shared_view_flush_trace.csv")
              .string(),
          trade.shared_view_flush_trace))
    return false;
  if (!WriteRestClientTrace(
          (std::filesystem::path(path).parent_path() /
           "rest_client_trace.csv")
              .string(),
          trade.rest_request_trace))
    return false;
  if (!WritePrivateClientTrace(
          (std::filesystem::path(path).parent_path() /
           "private_client_trace.csv")
              .string(),
          trade.private_client_trace))
    return false;
  Distribution(output, "trade_queue_wait", std::move(trade.queue_wait_ns));
  Distribution(output, "trade_submit_queue_wait",
               std::move(trade.submit_queue_wait_ns));
  Distribution(output, "trade_cancel_queue_wait",
               std::move(trade.cancel_queue_wait_ns));
  Distribution(output, "trade_query_queue_wait",
               std::move(trade.query_queue_wait_ns));
  Distribution(output, "trade_connect_wait", std::move(trade.connect_wait_ns));
  Distribution(output, "trade_socket_write", std::move(trade.socket_write_ns));
  Distribution(output, "trade_response_wait",
               std::move(trade.response_wait_ns));
  Distribution(output, "trade_queued_to_response",
               std::move(trade.queued_to_response_ns));
  Distribution(output, "private_report_receive_to_commit",
               std::move(trade.private_report_receive_to_commit_ns));
  Distribution(output, "private_report_apply",
               std::move(trade.private_report_apply_ns));
  Distribution(output, "private_balance_receive_to_commit",
               std::move(trade.private_balance_receive_to_commit_ns));
  Distribution(output, "private_balance_apply",
               std::move(trade.private_balance_apply_ns));
  // The primary SQLite latency excludes records enqueued after RequestStop.
  Distribution(output, "sqlite_queue_wait", std::move(queue_wait_before_stop));
  Distribution(output, "sqlite_enqueue_to_commit",
               std::move(commit_before_stop));
  Distribution(output, "sqlite_queue_wait_after_stop",
               std::move(queue_wait_after_stop));
  Distribution(output, "sqlite_enqueue_to_commit_after_stop",
               std::move(commit_after_stop));
  Distribution(output, "sqlite_queue_wait_all", probe.history.queue_wait_ns);
  Distribution(output, "sqlite_enqueue_to_commit_all",
               probe.history.enqueue_to_commit_ns);
  Distribution(output, "sqlite_batch_write",
               probe.history.batch_write_samples_ns, false);
  output += "}\n";
  return WriteTextFile(path, output);
}

}  // namespace

int main(int argc, char** argv) {
  google::SetUsageMessage("hquant public market and external trading runtime");
  google::ParseCommandLineFlags(&argc, &argv, true);
  auto* logger = hquant::v1::Logger();
  if (FLAGS_config.empty() || FLAGS_state_dir.empty()) {
    LOG_ERROR(logger, "--config and --state_dir are required");
    return 2;
  }
  auto config = hquant::v1::LoadConfig(FLAGS_config, FLAGS_state_dir);
  if (!config.ok()) {
    LOG_ERROR(logger, "config: {}", config.status().ToString());
    return 2;
  }
  auto overrides = ReadOverrides();
  if (!overrides.ok()) {
    LOG_ERROR(logger, "strategy overrides: {}", overrides.status().ToString());
    return 2;
  }
  if (auto status = hquant::v1::ApplyStrategyOverrides(*config, *overrides);
      !status.ok()) {
    LOG_ERROR(logger, "strategy overrides: {}", status.ToString());
    return 2;
  }
  std::error_code filesystem_error;
  std::filesystem::create_directories(FLAGS_state_dir, filesystem_error);
  if (filesystem_error) {
    LOG_ERROR(logger, "cannot create state_dir: {}",
              filesystem_error.message());
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
      LOG_ERROR(logger,
                "--bench_ack_port requires --bench_output and a valid port");
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
        LOG_ERROR(logger, "cannot create benchmark acknowledgement socket");
        return 2;
      }
      bench_ack_sockets.fds.push_back(fd);
      if (::connect(fd, reinterpret_cast<sockaddr*>(&address),
                    sizeof(address)) != 0) {
        LOG_ERROR(logger, "cannot connect benchmark acknowledgement socket");
        return 2;
      }
      probe->shards[shard.id.value].bench_ack_fd = fd;
    }
  }
  LOG_INFO(logger, "starting runtime: shards={} accounts={}",
           config->shards.size(), config->accounts.size());
  hquant::v1::Runtime runtime(*config, probe.get());
  if (auto status = runtime.Start(); !status.ok()) {
    LOG_ERROR(logger, "runtime start: {}", status.ToString());
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
  if (probe && !WriteBenchOutput(FLAGS_bench_output, *probe, *config,
                                 runtime.FinalAccountStatuses())) {
    LOG_ERROR(logger, "cannot write --bench_output: {}", FLAGS_bench_output);
    return 1;
  }
  if (!replay_status.ok() &&
      !(absl::IsCancelled(replay_status) && requested_stop)) {
    LOG_ERROR(logger, "replay: {}", replay_status.ToString());
    return 1;
  }
  if (!history_status.ok()) {
    LOG_ERROR(logger, "history: {}", history_status.ToString());
    return 1;
  }
  LOG_INFO(logger, "runtime stopped");
  return 0;
}
