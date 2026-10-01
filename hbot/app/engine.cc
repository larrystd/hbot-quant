#include "hbot/app/engine.h"

#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <filesystem>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <variant>
#include <vector>

#include "absl/status/status.h"
#include "boost/asio/co_spawn.hpp"
#include "boost/asio/detached.hpp"
#include "boost/asio/post.hpp"
#include "boost/asio/steady_timer.hpp"
#include "boost/asio/use_awaitable.hpp"
#include "hbot/app/control_server.h"
#include "hbot/connector/binance_spot/public/market_data_stream.h"
#include "hbot/connector/paper/paper_connector.h"
#include "hbot/control/protocol.h"
#include "hbot/net/http_client.h"
#include "hbot/runtime/replay_clock.h"
#include "hbot/runtime/shard.h"
#include "hbot/storage/history_reader.h"
#include "hbot/storage/sqlite_recorder.h"
#include "hbot/strategy/simple_pmm.h"
#include "simdjson.h"

namespace hbot {
namespace {

absl::StatusOr<std::string> String(simdjson::dom::element object,
                                   const char* key) {
  std::string_view value;
  if (object[key].get(value))
    return absl::InvalidArgumentError(std::string("missing ") + key);
  return std::string(value);
}

absl::StatusOr<uint64_t> Unsigned(simdjson::dom::element object,
                                  const char* key) {
  uint64_t value = 0;
  if (object[key].get(value))
    return absl::InvalidArgumentError(std::string("missing ") + key);
  return value;
}

absl::StatusOr<int64_t> Signed(simdjson::dom::element object, const char* key) {
  int64_t value = 0;
  if (object[key].get(value))
    return absl::InvalidArgumentError(std::string("missing ") + key);
  return value;
}

absl::StatusOr<std::vector<BookLevel>> Levels(simdjson::dom::element object,
                                              const char* key) {
  simdjson::dom::array rows;
  if (object[key].get(rows))
    return absl::InvalidArgumentError(std::string("missing ") + key);
  std::vector<BookLevel> levels;
  for (auto row : rows) {
    simdjson::dom::array pair;
    if (row.get(pair) || pair.size() != 2)
      return absl::InvalidArgumentError("invalid book level");
    int64_t price = 0;
    uint64_t amount = 0;
    if (pair.at(0).get(price) || pair.at(1).get(amount)) {
      return absl::InvalidArgumentError("invalid book level number");
    }
    levels.push_back({PriceTicks{price}, QuantityLots{amount}});
  }
  return levels;
}

std::string Escape(std::string_view text) {
  constexpr char hex[] = "0123456789abcdef";
  std::string result = "\"";
  for (unsigned char value : text) {
    if (value == '"' || value == '\\') {
      result.push_back('\\');
      result.push_back(static_cast<char>(value));
    } else if (value == '\n')
      result += "\\n";
    else if (value == '\r')
      result += "\\r";
    else if (value == '\t')
      result += "\\t";
    else if (value < 0x20) {
      result += "\\u00";
      result.push_back(hex[value >> 4]);
      result.push_back(hex[value & 15]);
    } else
      result.push_back(static_cast<char>(value));
  }
  return result + "\"";
}

const char* BookStateName(BookSyncState state) {
  switch (state) {
    case BookSyncState::Subscribing:
      return "Subscribing";
    case BookSyncState::Buffering:
      return "Buffering";
    case BookSyncState::Replaying:
      return "Replaying";
    case BookSyncState::Live:
      return "Live";
    case BookSyncState::Stale:
      return "Stale";
    case BookSyncState::Resyncing:
      return "Resyncing";
  }
  return "Unknown";
}

std::string StatusJson(const ShardRuntime& shard, const PaperConnector& paper,
                       const MarketSpec& market,
                       const SqliteRecorder& recorder) {
  const auto health = recorder.Health();
  return "{\"mode\":\"paper\",\"connector\":\"paper\",\"book\":" +
         Escape(BookStateName(shard.Book().State())) +
         ",\"active_shards\":1,\"strategy\":\"simple_pmm\",\"open_orders\":" +
         std::to_string(paper.OpenOrders().size()) +
         ",\"recorder_dropped\":" + std::to_string(health.dropped_count) +
         ",\"history_gaps\":" + std::to_string(health.gap_ranges.size()) +
         ",\"recorder_error\":" + Escape(health.last_error) +
         ",\"balances\":{" + Escape(market.base_asset.value) + ":" +
         Escape(paper.BalanceOf(market.base_asset).ToString()) + "," +
         Escape(market.quote_asset.value) + ":" +
         Escape(paper.BalanceOf(market.quote_asset).ToString()) +
         "},\"fees_paid\":{" + Escape(market.base_asset.value) + ":" +
         Escape(paper.FeesPaid(market.base_asset).ToString()) + "," +
         Escape(market.quote_asset.value) + ":" +
         Escape(paper.FeesPaid(market.quote_asset).ToString()) + "}}";
}

const char* OrderStatusName(ExchangeOrderStatus status) {
  switch (status) {
    case ExchangeOrderStatus::New:
      return "New";
    case ExchangeOrderStatus::PartiallyFilled:
      return "PartiallyFilled";
    case ExchangeOrderStatus::Filled:
      return "Filled";
    case ExchangeOrderStatus::Canceled:
      return "Canceled";
    case ExchangeOrderStatus::Rejected:
      return "Rejected";
    case ExchangeOrderStatus::Expired:
      return "Expired";
  }
  return "Unknown";
}

std::string HistoryJson(const HistoryPage& page) {
  std::string json = "{\"rows\":[";
  for (size_t index = 0; index < page.rows.size(); ++index) {
    const auto& row = page.rows[index];
    if (index) json += ",";
    std::string kind;
    std::string details;
    if (const auto* intent = std::get_if<OrderIntent>(&row.payload)) {
      kind = "intent";
      details = ",\"client_id\":" + Escape(intent->client_id.value) +
                ",\"amount\":" + Escape(intent->request.base_amount.ToString());
    } else if (const auto* update = std::get_if<OrderUpdate>(&row.payload)) {
      kind = "order";
      if (update->client_id)
        details = ",\"client_id\":" + Escape(update->client_id->value);
      details +=
          ",\"status\":" + Escape(OrderStatusName(update->exchange_status));
    } else if (const auto* trade = std::get_if<TradeUpdate>(&row.payload)) {
      kind = "trade";
      details = ",\"trade_id\":" + Escape(trade->exchange_trade_id.value) +
                ",\"amount\":" + Escape(trade->base_amount.ToString()) +
                ",\"price\":" + Escape(trade->price.ToString()) + ",\"fees\":[";
      for (size_t fee = 0; fee < trade->fees.size(); ++fee) {
        if (fee) details += ",";
        details +=
            "{\"asset\":" + Escape(trade->fees[fee].asset.value) +
            ",\"amount\":" + Escape(trade->fees[fee].signed_amount.ToString()) +
            "}";
      }
      details += "]";
    } else if (const auto* decision =
                   std::get_if<DecisionRecord>(&row.payload)) {
      kind = "decision";
      details = std::string(",\"accepted\":") +
                (decision->accepted ? "true" : "false") +
                ",\"reason\":" + Escape(decision->reason);
    } else if (std::holds_alternative<Checkpoint>(row.payload))
      kind = "checkpoint";
    else
      kind = "gap";
    json += "{\"run_id\":" + std::to_string(row.run_id.value) +
            ",\"sequence\":" + std::to_string(row.shard_sequence) +
            ",\"kind\":" + Escape(kind) + details + "}";
  }
  json += "],\"next_cursor\":";
  json += page.next_cursor ? Escape(*page.next_cursor) : "null";
  json += ",\"incomplete_ranges\":[";
  for (size_t index = 0; index < page.incomplete_ranges.size(); ++index) {
    if (index) json += ",";
    const auto& gap = page.incomplete_ranges[index];
    json += "{\"first\":" + std::to_string(gap.first_seq) +
            ",\"last\":" + std::to_string(gap.last_seq) + "}";
  }
  return json + "]}";
}

absl::Status ReplayFile(const std::string& path, const MarketConfig& market,
                        ReplayClock& clock, ShardRuntime& shard) {
  simdjson::dom::parser parser;
  simdjson::padded_string json;
  if (simdjson::padded_string::load(path).get(json)) {
    return absl::NotFoundError("replay fixture cannot be read: " + path);
  }
  simdjson::dom::element root;
  if (parser.parse(json).get(root))
    return absl::InvalidArgumentError("invalid replay JSON");
  uint64_t version = 0;
  if (root["schema_version"].get(version) || version != 1) {
    return absl::InvalidArgumentError("unsupported replay schema");
  }
  simdjson::dom::array inputs;
  if (root["inputs"].get(inputs))
    return absl::InvalidArgumentError("replay inputs missing");
  for (auto input : inputs) {
    auto at = Signed(input, "at_us");
    auto ordinal = Unsigned(input, "ordinal");
    auto kind = String(input, "kind");
    if (!at.ok()) return at.status();
    if (!ordinal.ok()) return ordinal.status();
    if (!kind.ok()) return kind.status();
    const auto stamp = InputStamp{*at, *ordinal};
    auto advanced = clock.Advance(stamp);
    if (!advanced.ok()) return advanced;
    const EventTime time{{}, clock.UtcNow(), clock.MonoNow()};
    if (*kind == "subscribe") {
      auto epoch = Unsigned(input, "stream_epoch");
      if (!epoch.ok()) return epoch.status();
      shard.Subscribe(*epoch);
    } else if (*kind == "snapshot" || *kind == "diff") {
      auto epoch = Unsigned(input, "stream_epoch");
      auto bids = Levels(input, "bids");
      auto asks = Levels(input, "asks");
      if (!epoch.ok()) return epoch.status();
      if (!bids.ok()) return bids.status();
      if (!asks.ok()) return asks.status();
      if (*kind == "snapshot") {
        auto sequence = Unsigned(input, "last_sequence");
        if (!sequence.ok()) return sequence.status();
        BookSnapshot event{market.spec.market,
                           market.book_scale.scale_version,
                           *epoch,
                           *sequence,
                           *bids,
                           *asks,
                           time};
        auto status = shard.OnSnapshot(event);
        if (!status.ok()) return status;
      } else {
        auto first = Unsigned(input, "first_sequence");
        auto last = Unsigned(input, "last_sequence");
        if (!first.ok()) return first.status();
        if (!last.ok()) return last.status();
        BookDiff event{market.spec.market,
                       market.book_scale.scale_version,
                       *epoch,
                       *first,
                       *last,
                       *bids,
                       *asks,
                       time};
        auto status = shard.OnDiff(event);
        if (!status.ok()) return status;
      }
    } else if (*kind == "timer") {
      auto result = shard.OnTimer(stamp);
      if (!result.ok()) return result.status();
    } else if (*kind == "public_trade") {
      auto price = Signed(input, "price_ticks");
      auto amount = Unsigned(input, "quantity_lots");
      auto side = String(input, "side");
      if (!price.ok()) return price.status();
      if (!amount.ok()) return amount.status();
      if (!side.ok()) return side.status();
      if (*side != "buy" && *side != "sell") {
        return absl::InvalidArgumentError("invalid public trade side");
      }
      PublicTrade event{market.spec.market,
                        {},
                        PriceTicks{*price},
                        QuantityLots{*amount},
                        *side == "buy" ? Side::Buy : Side::Sell,
                        time};
      auto status = shard.OnPublicTrade(event);
      if (!status.ok()) return status;
    } else
      return absl::InvalidArgumentError("unknown replay input kind");
  }
  return absl::OkStatus();
}

class SystemClock final : public Clock {
 public:
  UtcTime UtcNow() const override {
    return std::chrono::time_point_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now());
  }
  MonoTime MonoNow() const override {
    return std::chrono::time_point_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now());
  }
};

absl::Status RunPublicPaperEngine(const EngineConfig& config,
                                  const std::string& state_dir) {
  if (config.mode != EngineMode::Paper ||
      config.loop_mode != LoopMode::Blocking || config.accounts.size() != 1 ||
      config.market_specs.size() != 1 || config.strategy_configs.size() != 1 ||
      config.assignments.size() != 1) {
    return absl::InvalidArgumentError(
        "public Paper v1 requires one blocking shard, account, market and "
        "strategy");
  }
  const auto& account = config.accounts.front();
  const auto& market = config.market_specs.front();
  const auto& strategy_config = config.strategy_configs.front();
  const auto& assignment = config.assignments.front();
  if (market.spec.market.venue != VenueId("binance") ||
      strategy_config.account != account.account ||
      strategy_config.markets.size() != 1 ||
      strategy_config.markets.front() != market.spec.market ||
      assignment.shard.value >= 8) {
    return absl::InvalidArgumentError(
        "public Paper market or assignment mismatch");
  }
  std::error_code error;
  std::filesystem::create_directories(state_dir, error);
  if (error) return absl::InternalError("state directory: " + error.message());
  const auto storage_path =
      std::filesystem::path(config.storage_path).is_absolute()
          ? std::filesystem::path(config.storage_path)
          : std::filesystem::path(state_dir) / config.storage_path;
  std::filesystem::create_directories(storage_path.parent_path(), error);
  if (error)
    return absl::InternalError("storage directory: " + error.message());

  SystemClock clock;
  const UtcTime started = clock.UtcNow();
  const MonoTime started_mono = clock.MonoNow();
  const RunId run{static_cast<uint64_t>(started.time_since_epoch().count())};
  auto rule = market.trading_rule;
  rule.revision = 1;
  rule.observed_at = started;
  SimplePmm strategy({strategy_config.owner, account.account, market.spec,
                      strategy_config.order_amount, strategy_config.bid_spread,
                      strategy_config.ask_spread,
                      strategy_config.refresh_interval, PmmPriceType::Mid,
                      *Decimal::Parse("0.001"), true});
  PaperConnector paper(
      {account.account, market.spec, rule, account.initial_balances,
       *Decimal::Parse("0.001"), true,
       [run, next_id = uint64_t{1}](Side) mutable {
         return ClientOrderId("P" + std::to_string(run.value) + "-" +
                              std::to_string(next_id++));
       }},
      clock);
  RiskGate risk(
      {assignment.shard, *Decimal::Parse("0"), std::chrono::hours(24)});
  for (const auto& lease : config.static_risk_leases) {
    auto status = risk.SetInitialLease({lease.account, lease.asset, lease.shard,
                                        1, lease.hard_limit,
                                        started + std::chrono::hours(24)});
    if (!status.ok()) return status;
  }
  auto recorder =
      SqliteRecorder::Open({storage_path.string(), run, started, 1024, 64});
  if (!recorder.ok()) return recorder.status();
  ShardRuntime shard(
      {run, assignment.shard, strategy_config.owner, account.account,
       market.spec, market.book_scale, rule, 5'000'000},
      clock, strategy, paper, risk, **recorder);
  auto reader = HistoryReader::Open({storage_path.string(), 32, 500});
  if (!reader.ok()) return reader.status();

  boost::asio::io_context io;
  const TlsConfig rest_tls{true, true, {}, "data-api.binance.vision"};
  const TlsConfig ws_tls{true, true, {}, "data-stream.binance.vision"};
  HttpClient http(io, "data-api.binance.vision", "443", rest_tls);
  std::string symbol = market.spec.market.native_symbol;
  std::transform(
      symbol.begin(), symbol.end(), symbol.begin(),
      [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
  WebSocketClient websocket(
      io, "data-stream.binance.vision", "443",
      "/stream?streams=" + symbol + "@depth/" + symbol + "@trade", ws_tls);
  binance_spot::StreamConfig stream_config;
  stream_config.symbol = market.spec.market.native_symbol;
  std::string stream_error;
  binance_spot::MarketDataStream stream(
      stream_config,
      binance_spot::DepthParser(market.spec.market, market.book_scale), http,
      websocket, shard.MutableBookSync(), clock,
      {[&](const BookApplyResult& result) {
         if (result.state == BookSyncState::Live) stream_error.clear();
         auto status = shard.OnBookApplied();
         if (!status.ok()) {
           stream_error = std::string(status.message());
           risk.EmergencyStop();
         }
       },
       [&](const PublicTrade& trade) {
         auto status = shard.OnPublicTrade(trade);
         if (!status.ok()) {
           stream_error = std::string(status.message());
           risk.EmergencyStop();
         }
       },
       [&](const absl::Status& status) {
         stream_error = std::string(status.message());
       }});
  std::atomic<bool> stopping{false};
  boost::asio::co_spawn(io, stream.Run(), boost::asio::detached);
  boost::asio::co_spawn(
      io,
      [&]() -> boost::asio::awaitable<void> {
        boost::asio::steady_timer timer(io);
        uint64_t ordinal = 0;
        while (!stopping) {
          timer.expires_after(std::chrono::seconds(1));
          boost::system::error_code ec;
          co_await timer.async_wait(
              boost::asio::redirect_error(boost::asio::use_awaitable, ec));
          if (ec || stopping) break;
          const auto elapsed = clock.MonoNow() - started_mono;
          const auto at_us = elapsed.count();
          auto result = shard.OnTimer({at_us, ++ordinal});
          if (!result.ok()) {
            stream_error = std::string(result.status().message());
            risk.EmergencyStop();
          }
        }
      },
      boost::asio::detached);
  std::thread shard_thread([&] { io.run(); });

  std::mutex history_mutex;
  auto handler = [&](const ControlRequest& request) -> ControlResponse {
    ControlResponse response;
    response.request_id = request.request_id;
    if (std::holds_alternative<StatusRequest>(request.payload)) {
      auto result = std::make_shared<std::promise<std::string>>();
      auto ready = result->get_future();
      boost::asio::post(io, [&, result] {
        auto json = StatusJson(shard, paper, market.spec, **recorder);
        if (!stream_error.empty()) {
          json.pop_back();
          json += ",\"market_stream_error\":" + Escape(stream_error) + "}";
        }
        result->set_value(std::move(json));
      });
      if (ready.wait_for(std::chrono::seconds(2)) !=
          std::future_status::ready) {
        response.payload = ControlError{"timeout", "status query timed out"};
      } else {
        response.payload = StatusResponse{ready.get()};
      }
    } else if (const auto* history =
                   std::get_if<HistoryRequest>(&request.payload)) {
      std::lock_guard lock(history_mutex);
      HistoryQuery query;
      query.request_id = request.request_id;
      query.page_size = history->limit;
      if (!history->cursor.empty()) query.cursor = history->cursor;
      auto accepted = (**reader).TrySubmit(query);
      if (!accepted.ok()) {
        response.payload =
            ControlError{"history", std::string(accepted.message())};
      } else {
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(2);
        while (std::chrono::steady_clock::now() < deadline) {
          if (auto page = (**reader).TryReceive()) {
            if (!page->status.ok()) {
              response.payload =
                  ControlError{"history", std::string(page->status.message())};
            } else
              response.payload = HistoryResponse{HistoryJson(*page)};
            return response;
          }
          std::this_thread::sleep_for(std::chrono::milliseconds(2));
        }
        response.payload = ControlError{"timeout", "history query timed out"};
      }
    } else {
      stopping = true;
      boost::asio::post(io, [&] {
        stream.Stop();
        io.stop();
      });
      response.payload = StopResponse{true};
    }
    return response;
  };
  auto server = ControlServer::Start(state_dir + "/control.sock", handler);
  if (!server.ok()) {
    stopping = true;
    boost::asio::post(io, [&] {
      stream.Stop();
      io.stop();
    });
    shard_thread.join();
    return server.status();
  }
  while (!stopping) std::this_thread::sleep_for(std::chrono::milliseconds(20));
  (*server)->Stop();
  shard_thread.join();
  return (*recorder)->Stop(clock.UtcNow());
}

}  // namespace

absl::Status RunPaperEngine(const EngineConfig& config,
                            const std::string& state_dir) {
  if (config.market_data_source == MarketDataSource::BinancePublic) {
    return RunPublicPaperEngine(config, state_dir);
  }
  if (config.mode != EngineMode::Paper || config.accounts.size() != 1 ||
      config.market_specs.size() != 1 || config.strategy_configs.size() != 1 ||
      config.assignments.size() != 1 || !config.replay_fixture) {
    return absl::InvalidArgumentError(
        "G1 replay requires one Paper account, market, strategy and shard");
  }
  const auto& account = config.accounts.front();
  const auto& market = config.market_specs.front();
  const auto& strategy_config = config.strategy_configs.front();
  const auto& assignment = config.assignments.front();
  if (strategy_config.account != account.account ||
      strategy_config.markets.size() != 1 ||
      strategy_config.markets.front() != market.spec.market ||
      assignment.shard.value >= 8) {
    return absl::InvalidArgumentError(
        "G1 account, market or shard assignment mismatch");
  }
  std::error_code error;
  std::filesystem::create_directories(state_dir, error);
  if (error) return absl::InternalError("state directory: " + error.message());
  const auto storage_path =
      std::filesystem::path(config.storage_path).is_absolute()
          ? std::filesystem::path(config.storage_path)
          : std::filesystem::path(state_dir) / config.storage_path;
  std::filesystem::create_directories(storage_path.parent_path(), error);
  if (error)
    return absl::InternalError("storage directory: " + error.message());

  const UtcTime origin(std::chrono::microseconds(1'000'000));
  const RunId run{static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count())};
  ReplayClock clock(origin, MonoTime(std::chrono::microseconds(1'000'000)));
  auto rule = market.trading_rule;
  rule.observed_at = origin;
  rule.revision = 1;
  SimplePmm strategy({strategy_config.owner, account.account, market.spec,
                      strategy_config.order_amount, strategy_config.bid_spread,
                      strategy_config.ask_spread,
                      strategy_config.refresh_interval, PmmPriceType::Mid,
                      *Decimal::Parse("0.001"), true});
  PaperConnector paper(
      {account.account, market.spec, rule, account.initial_balances,
       *Decimal::Parse("0.001"), true,
       [run, next_id = uint64_t{1}](Side) mutable {
         return ClientOrderId("P" + std::to_string(run.value) + "-" +
                              std::to_string(next_id++));
       }},
      clock);
  RiskGate risk(
      {assignment.shard, *Decimal::Parse("0"), std::chrono::seconds(300)});
  for (const auto& lease : config.static_risk_leases) {
    auto status = risk.SetInitialLease({lease.account, lease.asset, lease.shard,
                                        1, lease.hard_limit,
                                        origin + std::chrono::hours(1)});
    if (!status.ok()) return status;
  }
  auto recorder =
      SqliteRecorder::Open({storage_path.string(), run, origin, 1024, 64});
  if (!recorder.ok()) return recorder.status();
  ShardRuntime shard({run, assignment.shard, strategy_config.owner,
                      account.account, market.spec, market.book_scale, rule},
                     clock, strategy, paper, risk, **recorder);
  auto replay = ReplayFile(*config.replay_fixture, market, clock, shard);
  if (!replay.ok()) return replay;
  auto flush = (*recorder)->Flush();
  if (!flush.ok()) return flush;
  auto reader = HistoryReader::Open({storage_path.string(), 32, 500});
  if (!reader.ok()) return reader.status();

  std::atomic<bool> stopping{false};
  std::mutex history_mutex;
  auto handler = [&](const ControlRequest& request) -> ControlResponse {
    ControlResponse response;
    response.request_id = request.request_id;
    if (std::holds_alternative<StatusRequest>(request.payload)) {
      response.payload =
          StatusResponse{StatusJson(shard, paper, market.spec, **recorder)};
    } else if (const auto* history =
                   std::get_if<HistoryRequest>(&request.payload)) {
      std::lock_guard lock(history_mutex);
      HistoryQuery query;
      query.request_id = request.request_id;
      query.page_size = history->limit;
      if (!history->cursor.empty()) query.cursor = history->cursor;
      auto accepted = (**reader).TrySubmit(query);
      if (!accepted.ok()) {
        response.payload =
            ControlError{"history", std::string(accepted.message())};
      } else {
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(2);
        while (std::chrono::steady_clock::now() < deadline) {
          if (auto page = (**reader).TryReceive()) {
            if (!page->status.ok()) {
              response.payload =
                  ControlError{"history", std::string(page->status.message())};
            } else
              response.payload = HistoryResponse{HistoryJson(*page)};
            return response;
          }
          std::this_thread::sleep_for(std::chrono::milliseconds(2));
        }
        response.payload = ControlError{"timeout", "history query timed out"};
      }
    } else {
      stopping = true;
      response.payload = StopResponse{true};
    }
    return response;
  };
  auto server = ControlServer::Start(state_dir + "/control.sock", handler);
  if (!server.ok()) return server.status();
  while (!stopping) std::this_thread::sleep_for(std::chrono::milliseconds(20));
  (*server)->Stop();
  return (*recorder)->Stop(clock.UtcNow());
}

}  // namespace hbot
