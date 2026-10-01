#include "apps/bench_feed.h"

#include <algorithm>
#include <cctype>
#include <charconv>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include "application/control_server.h"
#include "apps/bench_cli.h"
#include "base/error.h"
#include "base/net_server.h"
#include "base/types.h"
#include "boost/asio/redirect_error.hpp"
#include "boost/asio/steady_timer.hpp"
#include "boost/asio/this_coro.hpp"
#include "boost/asio/use_awaitable.hpp"
#include "market/replay_feed.h"

namespace hquant {
namespace {
namespace asio = boost::asio;
using Clock = std::chrono::steady_clock;

absl::StatusOr<uint32_t> Number(std::string_view text, uint32_t max) {
  uint32_t value = 0;
  const auto [end, ec] =
      std::from_chars(text.data(), text.data() + text.size(), value);
  if (ec != std::errc{} || end != text.data() + text.size() || value == 0 ||
      value > max)
    return Error(ErrorCode::kCliUsageInvalid, "invalid positive number");
  return value;
}

absl::StatusOr<double> Fraction(std::string_view text, double min, double max) {
  try {
    size_t parsed = 0;
    const double value = std::stod(std::string(text), &parsed);
    if (parsed == text.size() && std::isfinite(value) && value >= min &&
        value <= max)
      return value;
  } catch (...) {
  }
  return Error(ErrorCode::kCliUsageInvalid, "invalid decimal option");
}

absl::StatusOr<std::string> Scaled(int64_t units, const Decimal& quantum) {
  auto count = Decimal::Parse(std::to_string(units));
  if (!count.ok()) return count.status();
  auto value = count->Multiply(quantum);
  if (!value.ok()) return value.status();
  return value->ToString();
}

std::string WireLevels(const std::vector<BookLevel>& levels,
                       const Decimal& tick, const Decimal& lot) {
  std::string result = "[";
  for (const auto& level : levels) {
    if (result.size() > 1) result += ',';
    auto price = Scaled(level.price_ticks.value, tick);
    auto amount = Scaled(level.quantity_lots.value, lot);
    result += "[\"" + *price + "\",\"" + *amount + "\"]";
  }
  return result + "]";
}

struct FeedState {
  FeedBenchOptions options;
  Decimal tick;
  Decimal lot;
  ReplaySnapshot snapshot;
  ReplayDiff first_diff;
  std::vector<ReplayInput> events;
  std::map<uint64_t, size_t> next_event;
  std::map<uint64_t, uint64_t> sequence;
  std::map<uint64_t, uint64_t> depth_count;
  std::map<uint64_t, Clock::time_point> connected_at;
  uint64_t sent_depth = 0;
  uint64_t sent_trade = 0;
  uint64_t ws_connections = 0;
  uint64_t disconnects = 0;
  uint64_t snapshots = 0;
  uint64_t http_429 = 0;
  uint64_t http_requests = 0;

  FeedState(FeedBenchOptions config, Decimal price_tick, Decimal amount_lot)
      : options(std::move(config)),
        tick(std::move(price_tick)),
        lot(std::move(amount_lot)) {}

  std::string Depth(uint64_t first, uint64_t last,
                    const std::vector<BookLevel>& bids,
                    const std::vector<BookLevel>& asks) const {
    return BenchDepthFrame(options.symbol, first, last, bids, asks, tick, lot);
  }

  std::string Trade(const ReplayPublicTrade& trade, uint64_t trade_id) const {
    return BenchTradeFrame(options.symbol, trade_id, trade.price_ticks,
                           trade.quantity_lots, trade.side, tick, lot);
  }

  HttpResponse Http(std::string method, std::string target) {
    ++http_requests;
    if (method != "GET" ||
        target.find("/api/v3/depth?symbol=" + options.symbol) != 0)
      return {404, "not found"};
    const uint64_t value = (http_requests * 48271ULL) % 1000003ULL;
    if (double(value) / 1000003.0 < options.http_429_rate) {
      ++http_429;
      HttpResponse response{429, "rate limited"};
      response.headers.emplace_back("Retry-After", "1");
      return response;
    }
    ++snapshots;
    return {200, BenchSnapshotBody(snapshot.last_sequence, snapshot.bids,
                                   snapshot.asks, tick, lot)};
  }

  asio::awaitable<std::optional<std::string>> Next(uint64_t session,
                                                   uint64_t ordinal,
                                                   std::string target) {
    if (target.find("/stream?streams=") != 0) co_return std::nullopt;
    if (ordinal == 0) {
      ++ws_connections;
      connected_at[session] = Clock::now();
      sequence[session] = first_diff.last_sequence;
      depth_count[session] = 1;
      next_event[session] = 0;
      ++sent_depth;
      co_return Depth(snapshot.last_sequence, first_diff.last_sequence,
                      first_diff.bids, first_diff.asks);
    }
    asio::steady_timer timer(co_await asio::this_coro::executor);
    timer.expires_at(connected_at.at(session) +
                     std::chrono::nanoseconds(static_cast<int64_t>(
                         ordinal * 1000000000ULL / options.rate)));
    boost::system::error_code ec;
    co_await timer.async_wait(asio::redirect_error(asio::use_awaitable, ec));
    if (ec) co_return std::nullopt;
    const auto elapsed = Clock::now() - connected_at.at(session);
    if (options.disconnect_every &&
        elapsed >= std::chrono::seconds(options.disconnect_every)) {
      ++disconnects;
      co_return std::nullopt;
    }
    auto& cursor = next_event[session];
    while (cursor < events.size() &&
           !std::holds_alternative<ReplayDiff>(events[cursor].payload) &&
           !std::holds_alternative<ReplayPublicTrade>(events[cursor].payload))
      ++cursor;
    if (cursor < events.size()) {
      // Allow the server's 15s strategy refresh to precede the price change.
      // Its startup may lag this feed's startup by part of a second.
      const double due_seconds = 2.0 + double(events[cursor].stamp.at_us) /
                                           1'000'000.0 / options.speed;
      if (std::chrono::duration<double>(elapsed).count() >= due_seconds) {
        const auto& event = events[cursor++];
        if (const auto* trade =
                std::get_if<ReplayPublicTrade>(&event.payload)) {
          ++sent_trade;
          co_return Trade(*trade, sent_trade);
        }
        const auto& diff = std::get<ReplayDiff>(event.payload);
        uint64_t next = ++sequence[session];
        if (options.gap_every &&
            ++depth_count[session] % options.gap_every == 0)
          next = ++sequence[session];
        ++sent_depth;
        co_return Depth(next, next, diff.bids, diff.asks);
      }
    }
    uint64_t next = ++sequence[session];
    if (options.gap_every && ++depth_count[session] % options.gap_every == 0)
      next = ++sequence[session];
    ++sent_depth;
    co_return Depth(next, next, {}, {});
  }
};

}  // namespace

std::string BenchSnapshotBody(uint64_t sequence,
                              const std::vector<BookLevel>& bids,
                              const std::vector<BookLevel>& asks,
                              const Decimal& tick, const Decimal& lot) {
  return "{\"lastUpdateId\":" + std::to_string(sequence) +
         ",\"bids\":" + WireLevels(bids, tick, lot) +
         ",\"asks\":" + WireLevels(asks, tick, lot) + "}";
}

std::string BenchDepthFrame(std::string symbol, uint64_t first, uint64_t last,
                            const std::vector<BookLevel>& bids,
                            const std::vector<BookLevel>& asks,
                            const Decimal& tick, const Decimal& lot) {
  std::string stream = symbol;
  std::transform(
      stream.begin(), stream.end(), stream.begin(),
      [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
  return "{\"stream\":\"" + stream +
         "@depth\",\"data\":{\"e\":\"depthUpdate\",\"E\":1,\"s\":\"" + symbol +
         "\",\"U\":" + std::to_string(first) +
         ",\"u\":" + std::to_string(last) +
         ",\"b\":" + WireLevels(bids, tick, lot) +
         ",\"a\":" + WireLevels(asks, tick, lot) + "}}";
}

std::string BenchTradeFrame(std::string symbol, uint64_t trade_id,
                            PriceTicks price, QuantityLots amount, Side side,
                            const Decimal& tick, const Decimal& lot) {
  std::string stream = symbol;
  std::transform(
      stream.begin(), stream.end(), stream.begin(),
      [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
  auto price_text = Scaled(price.value, tick);
  auto amount_text = Scaled(amount.value, lot);
  return "{\"stream\":\"" + stream +
         "@trade\",\"data\":{\"e\":\"trade\",\"E\":1,\"s\":\"" + symbol +
         "\",\"t\":" + std::to_string(trade_id) + ",\"p\":\"" + *price_text +
         "\",\"q\":\"" + *amount_text +
         "\",\"m\":" + (side == Side::Sell ? "true" : "false") + "}}";
}

absl::StatusOr<FeedBenchOptions> ParseFeedBenchArguments(
    std::span<const std::string_view> args) {
  FeedBenchOptions options;
  bool seen[13]{};
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
    int index = key == "--listen"             ? 0
                : key == "--fixture"          ? 1
                : key == "--state-dir"        ? 2
                : key == "--symbol"           ? 3
                : key == "--price-per-tick"   ? 4
                : key == "--amount-per-lot"   ? 5
                : key == "--rate"             ? 6
                : key == "--duration"         ? 7
                : key == "--speed"            ? 8
                : key == "--gap-every"        ? 9
                : key == "--disconnect-every" ? 10
                : key == "--http-429-rate"    ? 11
                : key == "--symbols"          ? 12
                                              : -1;
    if (index < 0 || seen[index])
      return Error(ErrorCode::kCliUsageInvalid, "unknown or duplicate option");
    seen[index] = true;
    if (index == 0) {
      const auto colon = value.rfind(':');
      if (colon == std::string_view::npos)
        return Error(ErrorCode::kCliUsageInvalid, "listen must be IP:PORT");
      options.address = value.substr(0, colon);
      auto port = Number(value.substr(colon + 1), 65535);
      if (!port.ok()) return port.status();
      options.port = *port;
    } else if (index == 1)
      options.fixture = value;
    else if (index == 2)
      options.state_dir = value;
    else if (index == 3)
      options.symbol = value;
    else if (index == 12) {
      std::string symbols(value);
      size_t start = 0;
      while (start < symbols.size()) {
        const size_t comma = symbols.find(',', start);
        const auto symbol = symbols.substr(start, comma - start);
        if (symbol.empty() || symbol.find('"') != std::string::npos ||
            std::find(options.symbols.begin(), options.symbols.end(), symbol) !=
                options.symbols.end())
          return Error(ErrorCode::kCliUsageInvalid, "invalid --symbols");
        options.symbols.push_back(symbol);
        if (comma == std::string::npos) break;
        start = comma + 1;
      }
      if (symbols.empty() || symbols.back() == ',' ||
          options.symbols.size() > 8)
        return Error(ErrorCode::kCliUsageInvalid, "invalid --symbols");
    }
    else if (index == 4)
      options.price_per_tick = value;
    else if (index == 5)
      options.amount_per_lot = value;
    else if (index == 6 || index == 7 || index == 9 || index == 10) {
      if ((index == 9 || index == 10) && value == "0") continue;
      auto number = Number(value, index == 6 ? 1000000 : 100000);
      if (!number.ok()) return number.status();
      if (index == 6) options.rate = *number;
      if (index == 7) options.duration_seconds = *number;
      if (index == 9) options.gap_every = *number;
      if (index == 10) options.disconnect_every = *number;
    } else {
      auto number =
          Fraction(value, index == 8 ? 0.001 : 0, index == 8 ? 1000 : 1);
      if (!number.ok()) return number.status();
      if (index == 8) options.speed = *number;
      if (index == 11) options.http_429_rate = *number;
    }
  }
  if (seen[3] && seen[12])
    return Error(ErrorCode::kCliUsageInvalid,
                 "choose --symbol or --symbols");
  if (options.address.empty() || !options.port || options.symbol.empty() ||
      options.symbol.find('"') != std::string::npos)
    return Error(ErrorCode::kCliUsageInvalid, "invalid feed options");
  auto tick = Decimal::Parse(options.price_per_tick);
  auto lot = Decimal::Parse(options.amount_per_lot);
  if (!tick.ok() || !lot.ok() || !tick->IsStrictlyPositive() ||
      !lot->IsStrictlyPositive())
    return Error(ErrorCode::kCliUsageInvalid, "invalid tick or lot size");
  return options;
}

absl::StatusOr<std::string> RunFeedBench(const FeedBenchOptions& options) {
  auto tick = Decimal::Parse(options.price_per_tick);
  auto lot = Decimal::Parse(options.amount_per_lot);
  if (!tick.ok()) return tick.status();
  if (!lot.ok()) return lot.status();
  const std::vector<std::string> symbols =
      options.symbols.empty() ? std::vector<std::string>{options.symbol}
                              : options.symbols;
  if (symbols.empty() || symbols.size() > 8)
    return Error(ErrorCode::kCliUsageInvalid, "invalid feed symbols");
  std::vector<ReplayInput> replay;
  auto loaded = ReadReplayFile(
      options.fixture,
      [&](const ReplayInput& input) {
        replay.push_back(input);
        return absl::OkStatus();
      },
      symbols.size() == 1);
  if (!loaded.ok()) return loaded;
  std::map<std::string, std::unique_ptr<FeedState>> feeds;
  for (const auto& symbol : symbols) {
    if (feeds.contains(symbol))
      return Error(ErrorCode::kCliUsageInvalid, "duplicate feed symbol");
    FeedBenchOptions local = options;
    local.symbol = symbol;
    auto state = std::make_unique<FeedState>(local, *tick, *lot);
    std::vector<ReplayInput> selected;
    for (const auto& event : replay) {
      if (event.market && *event.market != symbol) continue;
      if (!event.market && symbols.size() != 1 &&
          !std::holds_alternative<ReplayTimer>(event.payload))
        return Error(ErrorCode::kReplayFileInvalid,
                     "multi-symbol fixture needs event markets");
      selected.push_back(event);
    }
    size_t first_index = selected.size();
    bool has_snapshot = false;
    for (size_t i = 0; i < selected.size(); ++i) {
      if (const auto* snapshot =
              std::get_if<ReplaySnapshot>(&selected[i].payload)) {
        state->snapshot = *snapshot;
        has_snapshot = true;
      } else if (const auto* diff =
                     std::get_if<ReplayDiff>(&selected[i].payload)) {
        state->first_diff = *diff;
        first_index = i;
        break;
      }
    }
    if (!has_snapshot || first_index == selected.size() ||
        state->first_diff.last_sequence != state->snapshot.last_sequence + 1)
      return Error(ErrorCode::kReplayFileInvalid,
                   "each symbol needs snapshot and next diff");
    state->events.assign(selected.begin() + first_index + 1, selected.end());
    feeds.emplace(symbol, std::move(state));
  }
  asio::io_context io;
  auto server = HttpServer::Start(
      io, options.address, options.port,
      [&](std::string method, std::string target) {
        constexpr std::string_view prefix = "/api/v3/depth?symbol=";
        if (!target.starts_with(prefix)) return HttpResponse{404, "not found"};
        const auto start = prefix.size();
        const auto end = target.find('&', start);
        const auto symbol = target.substr(start, end - start);
        auto feed = feeds.find(symbol);
        if (feed == feeds.end()) return HttpResponse{404, "not found"};
        return feed->second->Http(std::move(method), std::move(target));
      },
      [&](uint64_t session, uint64_t ordinal,
          std::string target) -> asio::awaitable<std::optional<std::string>> {
        constexpr std::string_view prefix = "/stream?streams=";
        if (!target.starts_with(prefix)) co_return std::nullopt;
        const auto start = prefix.size();
        const auto end = target.find('@', start);
        if (end == std::string::npos) co_return std::nullopt;
        auto symbol = target.substr(start, end - start);
        if (target != std::string(prefix) + symbol + "@depth/" + symbol +
                          "@trade")
          co_return std::nullopt;
        std::transform(symbol.begin(), symbol.end(), symbol.begin(),
                       [](unsigned char c) {
                         return static_cast<char>(std::toupper(c));
                       });
        auto feed = feeds.find(symbol);
        if (feed == feeds.end()) co_return std::nullopt;
        co_return co_await feed->second->Next(session, ordinal,
                                              std::move(target));
      });
  if (!server.ok()) return server.status();
  asio::steady_timer stop(io);
  stop.expires_after(std::chrono::seconds(options.duration_seconds));
  stop.async_wait([&](const boost::system::error_code&) {
    (*server)->Stop();
    io.stop();
  });
  io.run();
  std::string status;
  if (!options.state_dir.empty()) {
    ControlRequest request;
    request.request_id = 1;
    request.payload = StatusRequest{};
    auto response = SendControlRequest(options.state_dir, request);
    if (response.ok()) {
      if (const auto* value = std::get_if<StatusResponse>(&response->payload))
        status = value->json;
    }
  }
  uint64_t depth_sent = 0, trades_sent = 0, ws_connections = 0;
  uint64_t disconnects = 0, snapshots = 0, http_429 = 0;
  for (const auto& [_, state] : feeds) {
    depth_sent += state->sent_depth;
    trades_sent += state->sent_trade;
    ws_connections += state->ws_connections;
    disconnects += state->disconnects;
    snapshots += state->snapshots;
    http_429 += state->http_429;
  }
  std::ostringstream out;
  if (options.json) {
    out << "{\"depth_sent\":" << depth_sent
        << ",\"trades_sent\":" << trades_sent
        << ",\"ws_connections\":" << ws_connections
        << ",\"disconnects\":" << disconnects
        << ",\"snapshots\":" << snapshots
        << ",\"http_429\":" << http_429;
    if (!status.empty()) out << ",\"server_status\":" << status;
    out << '}';
  } else {
    out << "feed depth_sent=" << depth_sent
        << " trades_sent=" << trades_sent
        << " ws_connections=" << ws_connections
        << " disconnects=" << disconnects
        << " snapshots=" << snapshots << " http_429=" << http_429
        << '\n';
    if (!status.empty()) out << "server_status=" << status << '\n';
  }
  return out.str();
}

}  // namespace hquant
