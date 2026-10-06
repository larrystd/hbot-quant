#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <string>
#include <string_view>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "boost/asio/awaitable.hpp"
#include "boost/asio/io_context.hpp"
#include "boost/asio/steady_timer.hpp"
#include "hquant/base/net.h"
#include "hquant/config.h"
#include "hquant/perf_probe.h"
#include "hquant/shard/market_type.h"
#include "simdjson.h"

namespace hquant::v1 {

class BinanceFeed {
 public:
  using InputHandler = std::function<bool(const MarketInput&, MonoTime)>;
  using ErrorHandler = std::function<void(std::string)>;

  BinanceFeed(boost::asio::io_context& io, const MarketConfig& market,
              const InputConfig& input, InputHandler on_input,
              ErrorHandler on_error, ShardPerf* perf = nullptr);
  BinanceFeed(const BinanceFeed&) = delete;
  BinanceFeed& operator=(const BinanceFeed&) = delete;

  boost::asio::awaitable<void> Run();
  void RequestResync();
  void Stop();

 private:
  boost::asio::awaitable<absl::Status> RunCycle();
  absl::Status Fault(absl::Status status);
  absl::StatusOr<std::string> EventKind(simdjson::dom::element root) const;
  absl::StatusOr<BookSnapshot> ParseSnapshot(std::string_view json) const;
  absl::StatusOr<BookDiff> ParseDiff(simdjson::dom::element root) const;
  absl::StatusOr<PublicTrade> ParseTrade(simdjson::dom::element root) const;
  absl::StatusOr<std::vector<BookLevel>> ParseLevels(simdjson::dom::element root,
                                                     const char* key,
                                                     bool allow_zero) const;
  absl::StatusOr<uint64_t> PriceTicks(std::string_view text) const;
  absl::StatusOr<uint64_t> QuantityLots(std::string_view text) const;
  static MonoTime Now();
  uint64_t ProbeNowNs() const;
  void RecordWireTiming(std::string_view message, uint64_t read_ns,
                        uint64_t parsed_ns, uint64_t done_ns);
  void SendBenchAck(std::string_view message, uint64_t frame_complete_ns,
                    uint64_t read_ns,
                    uint64_t parsed_ns, uint64_t done_ns,
                    const WebSocketReadTrace& read_trace);
  void RecordBenchHandlerPhases(uint64_t parsed_ns, uint64_t done_ns);

  MarketConfig market_;
  InputConfig input_;
  InputHandler on_input_;
  ErrorHandler on_error_;
  ShardPerf* perf_ = nullptr;
  ::hquant::v1::HttpClient http_;
  ::hquant::v1::WebSocketClient websocket_;
  boost::asio::steady_timer retry_timer_;
  std::chrono::steady_clock::duration retry_after_{};
  uint64_t epoch_ = 0;
  bool stopped_ = false;
};

}  // namespace hquant::v1
