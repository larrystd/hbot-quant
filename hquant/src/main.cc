#include <csignal>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <string>
#include <thread>

#include "absl/status/status.h"
#include "gflags/gflags.h"
#include "hquant/base/fixed.h"
#include "hquant/config.h"
#include "hquant/runtime.h"

DEFINE_string(config, "", "YAML configuration path");
DEFINE_string(state_dir, "", "Runtime state directory");
DEFINE_int32(strategy_shard, -1, "Shard ID for strategy overrides");
DEFINE_string(order_amount, "", "Override order amount");
DEFINE_string(bid_spread, "", "Override bid spread");
DEFINE_string(ask_spread, "", "Override ask spread");
DEFINE_int64(refresh_ms, 0, "Override refresh period in milliseconds");

namespace {

volatile std::sig_atomic_t stop_requested = 0;

void SignalStop(int) { stop_requested = 1; }

bool Explicit(const char* name) {
  google::CommandLineFlagInfo info;
  return google::GetCommandLineFlagInfo(name, &info) && !info.is_default;
}

absl::StatusOr<hquant::v1::StrategyOverrides> ReadOverrides() {
  hquant::v1::StrategyOverrides overrides;
  if (Explicit("strategy_shard")) {
    if (FLAGS_strategy_shard < 0 || FLAGS_strategy_shard >= 8) {
      return absl::InvalidArgumentError("strategy_shard must be in [0,7]");
    }
    overrides.shard = hquant::v1::ShardId{
        static_cast<uint8_t>(FLAGS_strategy_shard)};
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
  hquant::v1::Runtime runtime(*config);
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
