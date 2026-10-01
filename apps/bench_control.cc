#include "apps/bench_control.h"

#include <sys/un.h>

#include <algorithm>
#include <array>
#include <bit>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <variant>
#include <vector>

#include "application/control_server.h"
#include "apps/bench_cli.h"
#include "base/error.h"
#include "boost/asio/co_spawn.hpp"
#include "boost/asio/detached.hpp"
#include "boost/asio/redirect_error.hpp"
#include "boost/asio/steady_timer.hpp"
#include "boost/asio/use_awaitable.hpp"

namespace hquant {
namespace {
namespace asio = boost::asio;
using Clock = std::chrono::steady_clock;
constexpr size_t kHistogramBuckets = 4096;

absl::StatusOr<uint32_t> Positive(std::string_view value, uint32_t maximum) {
  uint32_t number = 0;
  const auto [end, ec] =
      std::from_chars(value.data(), value.data() + value.size(), number);
  if (ec != std::errc{} || end != value.data() + value.size() || number == 0 ||
      number > maximum)
    return Error(ErrorCode::kCliUsageInvalid, "invalid positive number");
  return number;
}

size_t Bucket(uint64_t us) {
  if (us < 1024) return us;
  const unsigned exponent = std::bit_width(us) - 1;
  const uint64_t base = uint64_t{1} << exponent;
  return std::min<size_t>(kHistogramBuckets - 1, 1024 + (exponent - 10) * 64 +
                                                     (us - base) * 64 / base);
}

uint64_t BucketUpper(size_t bucket) {
  if (bucket < 1024) return bucket;
  const unsigned exponent = 10 + (bucket - 1024) / 64;
  const uint64_t base = uint64_t{1} << exponent;
  return base + ((bucket - 1024) % 64 + 1) * base / 64;
}

struct ErrorCount {
  ErrorCode code = ErrorCode::kOk;
  uint64_t count = 0;
};
struct RequestStats {
  uint64_t sent = 0;
  uint64_t success = 0;
  uint64_t max_us = 0;
  std::array<uint64_t, kHistogramBuckets> histogram{};
  std::array<ErrorCount, 32> errors{};
  void AddError(ErrorCode code) {
    for (auto& entry : errors) {
      if (entry.code == code || entry.code == ErrorCode::kOk) {
        entry.code = code;
        ++entry.count;
        return;
      }
    }
    ++errors.back().count;
  }
  void AddLatency(uint64_t us) {
    ++histogram[Bucket(us)];
    max_us = std::max(max_us, us);
  }
  uint64_t Quantile(uint64_t numerator, uint64_t denominator) const {
    uint64_t total = 0;
    for (uint64_t count : histogram) total += count;
    if (total == 0) return 0;
    const uint64_t target = (total * numerator + denominator - 1) / denominator;
    uint64_t cumulative = 0;
    for (size_t i = 0; i < histogram.size(); ++i) {
      cumulative += histogram[i];
      if (cumulative >= target) return BucketUpper(i);
    }
    return max_us;
  }
  void Merge(const RequestStats& other) {
    sent += other.sent;
    success += other.success;
    max_us = std::max(max_us, other.max_us);
    for (size_t i = 0; i < histogram.size(); ++i)
      histogram[i] += other.histogram[i];
    for (const auto& error : other.errors) {
      if (!error.count) continue;
      for (auto& entry : errors) {
        if (entry.code == error.code || entry.code == ErrorCode::kOk) {
          entry.code = error.code;
          entry.count += error.count;
          break;
        }
      }
    }
  }
};

struct ThreadStats {
  std::array<RequestStats, 2> requests;
  uint64_t client_saturated = 0;
};

asio::awaitable<void> OneRequest(asio::io_context& io, const std::string& path,
                                 ThreadStats& stats, bool history,
                                 uint64_t request_id, bool measure,
                                 size_t* in_flight) {
  struct Release {
    size_t* count;
    ~Release() {
      if (count) --*count;
    }
  } release{in_flight};
  const auto started = Clock::now();
  auto& result = stats.requests[history ? 1 : 0];
  if (measure) ++result.sent;
  ControlRequest request;
  request.request_id = request_id;
  if (history)
    request.payload = OrderHistoryRequest{20, ""};
  else
    request.payload = StatusRequest{};
  auto response = co_await ExchangeControlRequest(io, path, request);
  if (!measure) co_return;
  const auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                           Clock::now() - started)
                           .count();
  result.AddLatency(std::max<int64_t>(0, elapsed));
  if (!response.ok()) {
    result.AddError(CodeOf(response.status()));
  } else if (response->request_id != request_id ||
             (history &&
              !std::holds_alternative<OrderHistoryResponse>(response->payload)) ||
             (!history &&
              !std::holds_alternative<StatusResponse>(response->payload))) {
    if (const auto* error = std::get_if<ControlError>(&response->payload))
      result.AddError(error->code);
    else
      result.AddError(ErrorCode::kCliResponseInvalid);
  } else {
    ++result.success;
  }
}

asio::awaitable<void> ClosedClient(asio::io_context& io,
                                   const std::string& path,
                                   const ControlBenchOptions& options,
                                   ThreadStats& stats,
                                   Clock::time_point measure_start,
                                   Clock::time_point end, uint64_t client_id,
                                   uint64_t& sequence) {
  while (Clock::now() < end) {
    const bool history = (sequence++ % 100) >= options.status_percent;
    co_await OneRequest(io, path, stats, history, client_id,
                        Clock::now() >= measure_start, nullptr);
  }
}

asio::awaitable<void> OpenScheduler(
    asio::io_context& io, const std::string& path,
    const ControlBenchOptions& options, ThreadStats& stats,
    Clock::time_point measure_start, Clock::time_point end,
    uint32_t thread_index, size_t capacity, uint64_t& sequence) {
  size_t in_flight = 0;
  asio::steady_timer timer(io);
  const auto start =
      measure_start - std::chrono::seconds(options.warmup_seconds);
  uint64_t tick = thread_index;
  while (true) {
    const auto scheduled =
        start + std::chrono::nanoseconds(static_cast<int64_t>(
                    (tick * 1000000000ULL) / options.rate));
    if (scheduled >= end) break;
    timer.expires_at(scheduled);
    boost::system::error_code ec;
    co_await timer.async_wait(asio::redirect_error(asio::use_awaitable, ec));
    if (ec) break;
    const bool measure = scheduled >= measure_start;
    if (in_flight >= capacity) {
      if (measure) ++stats.client_saturated;
    } else {
      const bool history = (sequence++ % 100) >= options.status_percent;
      ++in_flight;
      asio::co_spawn(
          io,
          OneRequest(io, path, stats, history, tick + 1, measure, &in_flight),
          asio::detached);
    }
    tick += options.threads;
  }
  // The io_context drains launched requests before its thread exits.
  while (in_flight != 0) {
    timer.expires_after(std::chrono::milliseconds(10));
    co_await timer.async_wait(asio::use_awaitable);
  }
}

void PrintRequest(std::ostringstream& out, std::string_view name,
                  const RequestStats& stats, uint32_t seconds, bool json) {
  if (json) {
    out << '"' << name << "\":{";
    out << "\"sent\":" << stats.sent << ",\"success\":" << stats.success
        << ",\"rps\":" << (double(stats.success) / seconds)
        << ",\"p50_us\":" << stats.Quantile(50, 100)
        << ",\"p90_us\":" << stats.Quantile(90, 100)
        << ",\"p99_us\":" << stats.Quantile(99, 100)
        << ",\"p999_us\":" << stats.Quantile(999, 1000)
        << ",\"max_us\":" << stats.max_us << ",\"errors\":{";
    bool first = true;
    for (const auto& error : stats.errors)
      if (error.count) {
        if (!first) out << ',';
        first = false;
        out << '"' << Info(error.code).name << "\":" << error.count;
      }
    out << "}}";
  } else {
    out << name << " sent=" << stats.sent << " success=" << stats.success
        << " rps=" << double(stats.success) / seconds
        << " p50/p90/p99/p999/max(us)=" << stats.Quantile(50, 100) << '/'
        << stats.Quantile(90, 100) << '/' << stats.Quantile(99, 100) << '/'
        << stats.Quantile(999, 1000) << '/' << stats.max_us << '\n';
    for (const auto& error : stats.errors)
      if (error.count)
        out << "  " << Info(error.code).name << '=' << error.count << '\n';
  }
}

}  // namespace

absl::StatusOr<ControlBenchOptions> ParseControlBenchArguments(
    std::span<const std::string_view> args) {
  ControlBenchOptions options;
  bool seen[8]{};
  for (size_t i = 0; i < args.size(); ++i) {
    if (args[i] == "--json") {
      if (options.json)
        return Error(ErrorCode::kCliUsageInvalid, "duplicate --json");
      options.json = true;
      continue;
    }
    if (i + 1 == args.size())
      return Error(ErrorCode::kCliUsageInvalid, "option needs value");
    auto key = args[i];
    auto value = args[++i];
    int index = key == "--state-dir"     ? 0
                : key == "--connections" ? 1
                : key == "--threads"     ? 2
                : key == "--duration"    ? 3
                : key == "--warmup"      ? 4
                : key == "--mode"        ? 5
                : key == "--rate"        ? 6
                : key == "--mix"         ? 7
                                         : -1;
    if (index < 0 || seen[index])
      return Error(ErrorCode::kCliUsageInvalid, "unknown or duplicate option");
    seen[index] = true;
    if (index == 0)
      options.state_dir = value;
    else if (index == 1 || index == 2 || index == 3 || index == 6) {
      auto number = Positive(value, index == 6 ? 1000000 : 10000);
      if (!number.ok()) return number.status();
      if (index == 1) options.connections = *number;
      if (index == 2) options.threads = *number;
      if (index == 3) options.duration_seconds = *number;
      if (index == 6) options.rate = *number;
    } else if (index == 4) {
      if (value == "0")
        options.warmup_seconds = 0;
      else {
        auto number = Positive(value, 3600);
        if (!number.ok()) return number.status();
        options.warmup_seconds = *number;
      }
    } else if (index == 5) {
      if (value == "open")
        options.mode = ControlBenchOptions::Mode::Open;
      else if (value == "closed")
        options.mode = ControlBenchOptions::Mode::Closed;
      else
        return Error(ErrorCode::kCliUsageInvalid,
                     "mode must be open or closed");
    } else {
      const auto comma = value.find(',');
      if (comma == std::string_view::npos || value.substr(0, 7) != "status=" ||
          value.substr(comma + 1, 8) != "history=")
        return Error(ErrorCode::kCliUsageInvalid,
                     "mix must be status=N,history=N");
      const auto parse_percent =
          [](std::string_view text) -> absl::StatusOr<uint32_t> {
        if (text == "0") return 0;
        return Positive(text, 100);
      };
      auto status = parse_percent(value.substr(7, comma - 7));
      auto history = parse_percent(value.substr(comma + 9));
      if (!status.ok() || !history.ok() || *status + *history != 100)
        return Error(ErrorCode::kCliUsageInvalid,
                     "mix percentages must total 100");
      options.status_percent = *status;
    }
  }
  if (options.state_dir.empty() || options.threads > options.connections ||
      (options.mode == ControlBenchOptions::Mode::Open && options.rate == 0) ||
      (options.mode == ControlBenchOptions::Mode::Closed && options.rate != 0))
    return Error(ErrorCode::kCliUsageInvalid, "invalid control bench options");
  if (ControlSocketPath(options.state_dir).size() >=
      sizeof(sockaddr_un::sun_path))
    return Error(ErrorCode::kCliUsageInvalid, "server socket path too long");
  return options;
}

absl::StatusOr<std::string> RunControlBench(
    const ControlBenchOptions& options) {
  const std::string path = ControlSocketPath(options.state_dir);
  std::vector<ThreadStats> stats(options.threads);
  const auto start = Clock::now() + std::chrono::milliseconds(100);
  const auto measure_start =
      start + std::chrono::seconds(options.warmup_seconds);
  const auto end =
      measure_start + std::chrono::seconds(options.duration_seconds);
  std::vector<std::thread> threads;
  threads.reserve(options.threads);
  for (uint32_t index = 0; index < options.threads; ++index) {
    threads.emplace_back([&, index] {
      asio::io_context io;
      uint64_t sequence = index;
      if (options.mode == ControlBenchOptions::Mode::Closed) {
        for (uint32_t client = index; client < options.connections;
             client += options.threads)
          asio::co_spawn(io,
                         ClosedClient(io, path, options, stats[index],
                                      measure_start, end, client + 1, sequence),
                         asio::detached);
      } else {
        const size_t capacity =
            (options.connections + options.threads - 1 - index) /
            options.threads;
        asio::co_spawn(
            io,
            OpenScheduler(io, path, options, stats[index], measure_start, end,
                          index, capacity, sequence),
            asio::detached);
      }
      io.run();
    });
  }
  for (auto& thread : threads) thread.join();
  ThreadStats total;
  for (const auto& thread : stats) {
    for (size_t i = 0; i < 2; ++i) total.requests[i].Merge(thread.requests[i]);
    total.client_saturated += thread.client_saturated;
  }
  std::ostringstream out;
  if (options.json) {
    out << "{\"mode\":\""
        << (options.mode == ControlBenchOptions::Mode::Closed ? "closed"
                                                              : "open")
        << "\",\"duration_s\":" << options.duration_seconds << ',';
    PrintRequest(out, "status", total.requests[0], options.duration_seconds,
                 true);
    out << ',';
    PrintRequest(out, "history", total.requests[1], options.duration_seconds,
                 true);
    out << ",\"client_saturated\":" << total.client_saturated << '}';
  } else {
    out << "control "
        << (options.mode == ControlBenchOptions::Mode::Closed ? "closed"
                                                              : "open")
        << " duration=" << options.duration_seconds << "s\n";
    PrintRequest(out, "status", total.requests[0], options.duration_seconds,
                 false);
    PrintRequest(out, "history", total.requests[1], options.duration_seconds,
                 false);
    out << "client_saturated=" << total.client_saturated << '\n';
  }
  return out.str();
}

}  // namespace hquant
