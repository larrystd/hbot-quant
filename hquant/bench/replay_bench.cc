// Serial replay diagnostic: its per-record completion wait is part of the
// measured path. Use live_ws_bench.py for WebSocket/REST throughput and latency.
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <future>
#include <iomanip>
#include <iostream>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "hquant/config.h"
#include "hquant/perf_probe.h"
#include "hquant/replay_reader.h"
#include "hquant/runtime.h"

namespace {

struct Distribution {
  uint64_t count = 0;
  double mean_us = 0;
  double p50_us = 0;
  double p95_us = 0;
  double p99_us = 0;
  double p999_us = 0;
  double max_us = 0;
};

Distribution Summarize(std::vector<uint64_t> values) {
  if (values.empty()) return {};
  uint64_t sum = 0;
  for (uint64_t value : values) sum += value;
  std::sort(values.begin(), values.end());
  const auto percentile = [&](double fraction) {
    const size_t index = static_cast<size_t>(fraction * (values.size() - 1));
    return values[index] / 1000.0;
  };
  return Distribution{
      values.size(),         static_cast<double>(sum) / values.size() / 1000.0,
      percentile(0.50),      percentile(0.95),
      percentile(0.99),      percentile(0.999),
      values.back() / 1000.0};
}

void PrintDistribution(const char* name, Distribution value, bool comma) {
  std::cout << "  \"" << name << "\": {"
            << "\"count\":" << value.count << ",\"mean_us\":" << value.mean_us
            << ",\"p50_us\":" << value.p50_us << ",\"p95_us\":" << value.p95_us
            << ",\"p99_us\":" << value.p99_us
            << ",\"p999_us\":" << value.p999_us
            << ",\"max_us\":" << value.max_us << "}" << (comma ? "," : "")
            << '\n';
}

double Seconds(uint64_t nanoseconds) {
  return static_cast<double>(nanoseconds) / 1'000'000'000.0;
}

void Append(std::vector<uint64_t>& target,
            const std::vector<uint64_t>& source) {
  target.insert(target.end(), source.begin(), source.end());
}

}  // namespace

int main(int argc, char** argv) {
  if (argc != 3 && !(argc == 4 && std::string(argv[3]) == "--pace")) {
    std::cerr << "usage: replay_bench CONFIG STATE_DIR [--pace]\n";
    return 2;
  }
  const bool paced = argc == 4;
  auto config = hquant::v1::LoadConfig(argv[1], argv[2]);
  if (!config.ok()) {
    std::cerr << config.status() << '\n';
    return 2;
  }
  if (config->input.source != hquant::v1::InputSource::Replay) {
    std::cerr << "benchmark requires replay input\n";
    return 2;
  }
  std::error_code filesystem_error;
  std::filesystem::create_directories(argv[2], filesystem_error);
  if (filesystem_error) {
    std::cerr << filesystem_error.message() << '\n';
    return 2;
  }

  uint64_t parse_records = 0;
  const uint64_t parse_begin = hquant::v1::PerfNowNs();
  auto parse_status = hquant::v1::ReadReplayFile(
      config->input.replay_file, *config, [&](hquant::v1::ReplayRecord) {
        ++parse_records;
        return absl::OkStatus();
      });
  const uint64_t parse_end = hquant::v1::PerfNowNs();
  if (!parse_status.ok()) {
    std::cerr << parse_status << '\n';
    return 1;
  }

  hquant::v1::PerfProbe probe;
  hquant::v1::Runtime runtime(*config, &probe);
  const uint64_t start_begin = hquant::v1::PerfNowNs();
  if (auto status = runtime.Start(); !status.ok()) {
    std::cerr << status << '\n';
    return 1;
  }
  const uint64_t replay_begin = hquant::v1::PerfNowNs();
  std::vector<uint64_t> roundtrip_ns;
  std::vector<uint64_t> callback_to_get_ns;
  std::vector<uint64_t> schedule_lateness_ns;
  roundtrip_ns.reserve(parse_records);
  callback_to_get_ns.reserve(parse_records);
  if (paced) schedule_lateness_ns.reserve(parse_records);
  std::optional<std::chrono::steady_clock::time_point> schedule_begin;
  auto replay_status = hquant::v1::ReadReplayFile(
      config->input.replay_file, *config, [&](hquant::v1::ReplayRecord record) {
        if (paced) {
          if (!schedule_begin) schedule_begin = std::chrono::steady_clock::now();
          const auto deadline = *schedule_begin +
              std::chrono::microseconds(record.time.at_us);
          std::this_thread::sleep_until(deadline);
          const auto late = std::chrono::steady_clock::now() - deadline;
          schedule_lateness_ns.push_back(static_cast<uint64_t>(
              std::max<int64_t>(0, std::chrono::duration_cast<
                  std::chrono::nanoseconds>(late).count())));
        }
        std::promise<absl::Status> done;
        auto future = done.get_future();
        const uint64_t begin = hquant::v1::PerfNowNs();
        record.perf_post_ns = begin;
        uint64_t callback_at = 0;
        runtime.PostReplay(std::move(record),
                           [&done, &callback_at](absl::Status status) {
                             callback_at = hquant::v1::PerfNowNs();
                             done.set_value(std::move(status));
                           });
        auto status = future.get();
        const uint64_t after_get = hquant::v1::PerfNowNs();
        roundtrip_ns.push_back(after_get - begin);
        callback_to_get_ns.push_back(after_get - callback_at);
        return status;
      });
  const uint64_t replay_end = hquant::v1::PerfNowNs();
  runtime.RequestStop();
  const absl::Status stop_status = runtime.Wait();
  const uint64_t stop_end = hquant::v1::PerfNowNs();
  if (!replay_status.ok() || !stop_status.ok()) {
    std::cerr << (!replay_status.ok() ? replay_status : stop_status) << '\n';
    return 1;
  }

  hquant::v1::ShardPerf shard_total;
  for (const auto& shard : config->shards) {
    const auto& sample = probe.shards[shard.id.value];
    shard_total.inputs += sample.inputs;
    shard_total.input_dispatch_ns += sample.input_dispatch_ns;
    shard_total.market_path_ns += sample.market_path_ns;
    shard_total.paper_match_ns += sample.paper_match_ns;
    shard_total.decision_ns += sample.decision_ns;
    shard_total.account_events += sample.account_events;
    shard_total.account_event_ns += sample.account_event_ns;
    shard_total.history_records += sample.history_records;
    shard_total.history_enqueue_ns += sample.history_enqueue_ns;
    Append(shard_total.post_to_shard_samples_ns,
           sample.post_to_shard_samples_ns);
    Append(shard_total.input_handler_samples_ns,
           sample.input_handler_samples_ns);
    Append(shard_total.handler_to_completion_samples_ns,
           sample.handler_to_completion_samples_ns);
    Append(shard_total.paper_match_samples_ns, sample.paper_match_samples_ns);
    Append(shard_total.decision_samples_ns, sample.decision_samples_ns);
    Append(shard_total.account_event_samples_ns,
           sample.account_event_samples_ns);
    Append(shard_total.history_enqueue_samples_ns,
           sample.history_enqueue_samples_ns);
  }
  const auto& history = probe.history;
  std::cout << std::fixed << std::setprecision(3);
  std::cout
      << "{\n"
      << "  \"paced\": " << (paced ? "true" : "false") << ",\n"
      << "  \"parse_records\": " << parse_records << ",\n"
      << "  \"parse_only_seconds\": " << Seconds(parse_end - parse_begin)
      << ",\n"
      << "  \"startup_seconds\": " << Seconds(replay_begin - start_begin)
      << ",\n"
      << "  \"replay_seconds\": " << Seconds(replay_end - replay_begin) << ",\n"
      << "  \"replay_qps\": "
      << roundtrip_ns.size() / Seconds(replay_end - replay_begin) << ",\n"
      << "  \"flush_stop_seconds\": " << Seconds(stop_end - replay_end) << ",\n"
      << "  \"total_runtime_seconds\": " << Seconds(stop_end - start_begin)
      << ",\n"
      << "  \"deliveries\": " << roundtrip_ns.size() << ",\n"
      << "  \"shard_inputs\": " << shard_total.inputs << ",\n"
      << "  \"shard_input_dispatch_ms\": "
      << shard_total.input_dispatch_ns / 1e6 << ",\n"
      << "  \"market_path_ms\": " << shard_total.market_path_ns / 1e6 << ",\n"
      << "  \"paper_match_ms\": " << shard_total.paper_match_ns / 1e6 << ",\n"
      << "  \"decision_ms\": " << shard_total.decision_ns / 1e6 << ",\n"
      << "  \"account_event_ms\": " << shard_total.account_event_ns / 1e6
      << ",\n"
      << "  \"history_enqueue_ms\": " << shard_total.history_enqueue_ns / 1e6
      << ",\n"
      << "  \"account_events\": " << shard_total.account_events << ",\n"
      << "  \"history_records_attempted\": " << shard_total.history_records
      << ",\n"
      << "  \"history_records_written\": " << history.records_written << ",\n"
      << "  \"history_queue_peak\": " << history.queue_peak << ",\n"
      << "  \"history_batches\": " << history.batches << ",\n"
      << "  \"history_batch_write_ms\": " << history.batch_write_ns / 1e6
      << ",\n";
  PrintDistribution("roundtrip", Summarize(std::move(roundtrip_ns)), true);
  PrintDistribution("schedule_lateness",
                    Summarize(std::move(schedule_lateness_ns)), true);
  PrintDistribution("post_to_shard",
                    Summarize(std::move(shard_total.post_to_shard_samples_ns)),
                    true);
  PrintDistribution("input_handler",
                    Summarize(std::move(shard_total.input_handler_samples_ns)),
                    true);
  PrintDistribution(
      "handler_to_completion",
      Summarize(std::move(shard_total.handler_to_completion_samples_ns)), true);
  PrintDistribution("callback_to_get", Summarize(std::move(callback_to_get_ns)),
                    true);
  PrintDistribution("paper_match",
                    Summarize(std::move(shard_total.paper_match_samples_ns)),
                    true);
  PrintDistribution(
      "decision", Summarize(std::move(shard_total.decision_samples_ns)), true);
  PrintDistribution("account_event",
                    Summarize(std::move(shard_total.account_event_samples_ns)),
                    true);
  PrintDistribution(
      "history_enqueue",
      Summarize(std::move(shard_total.history_enqueue_samples_ns)), true);
  PrintDistribution("sqlite_batch_write",
                    Summarize(history.batch_write_samples_ns), true);
  PrintDistribution("enqueue_to_commit",
                    Summarize(history.enqueue_to_commit_ns), true);
  PrintDistribution("input_to_commit", Summarize(history.input_to_commit_ns),
                    false);
  std::cout << "}\n";
  return 0;
}
