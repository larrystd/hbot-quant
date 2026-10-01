#pragma once

#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "absl/status/statusor.h"
#include "base/market.h"
#include "base/types.h"

namespace hquant {

struct FeedBenchOptions {
  std::string address;
  uint16_t port = 0;
  std::string fixture = "examples/replay_market.json";
  std::string state_dir;
  std::string symbol = "BTCUSDT";
  std::string price_per_tick = "0.01";
  std::string amount_per_lot = "0.001";
  uint32_t rate = 10;
  uint32_t duration_seconds = 20;
  double speed = 1;
  uint32_t gap_every = 0;
  uint32_t disconnect_every = 0;
  double http_429_rate = 0;
  bool json = false;
};

absl::StatusOr<FeedBenchOptions> ParseFeedBenchArguments(
    std::span<const std::string_view> arguments);
absl::StatusOr<std::string> RunFeedBench(const FeedBenchOptions& options);

// Wire builders used by the local feed and its parser contract test.
std::string BenchSnapshotBody(uint64_t sequence,
                              const std::vector<BookLevel>& bids,
                              const std::vector<BookLevel>& asks,
                              const Decimal& tick, const Decimal& lot);
std::string BenchDepthFrame(std::string symbol, uint64_t first, uint64_t last,
                            const std::vector<BookLevel>& bids,
                            const std::vector<BookLevel>& asks,
                            const Decimal& tick, const Decimal& lot);
std::string BenchTradeFrame(std::string symbol, uint64_t trade_id,
                            PriceTicks price, QuantityLots amount, Side side,
                            const Decimal& tick, const Decimal& lot);

}  // namespace hquant
