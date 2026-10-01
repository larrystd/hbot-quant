#pragma once

#include <memory>
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <string>
#include <thread>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "application/config.h"
#include "boost/asio/awaitable.hpp"
#include "boost/asio/io_context.hpp"
#include "order_history/order_history.h"

namespace hquant {

class Clock;
class ReplayClock;
class SimplePmm;
class SimpleSimulatedExchange;
class RiskGate;
class SqliteOrderHistoryWriter;
class SqliteOrderHistoryReader;
class Shard;
class HttpClient;
class WebSocketClient;
class ControlServer;
namespace binance_spot {
class MarketDataStream;
}

// Owns the trading chain, its data source, history, and management endpoint.
class QuantServer {
 public:
  static absl::StatusOr<std::unique_ptr<QuantServer>> Create(
      const AppConfig& config, std::string state_dir);
  ~QuantServer();
  QuantServer(const QuantServer&) = delete;
  QuantServer& operator=(const QuantServer&) = delete;

  absl::Status Start();
  absl::Status Wait();
  void RequestStop();
  boost::asio::awaitable<absl::StatusOr<std::string>> StatusAsync();
  boost::asio::awaitable<absl::StatusOr<OrderHistoryPage>> OrderHistoryAsync(
      OrderHistoryQuery query);

 private:
  explicit QuantServer(AppConfig config, std::string state_dir,
                       std::string storage_path);
  absl::Status CreateComponents();
  absl::Status RunReplay();
  boost::asio::awaitable<void> TimerLoop();
  void StartFeed();

  AppConfig config;
  std::string state_dir;
  std::string storage_path;
  bool live = false;
  UtcTime started{};
  MonoTime started_mono{};
  RunId run{};
  std::unique_ptr<Clock> clock;
  ReplayClock* replay_clock = nullptr;
  std::unique_ptr<SimplePmm> strategy;
  std::unique_ptr<SimpleSimulatedExchange> exchange;
  std::unique_ptr<RiskGate> risk;
  std::unique_ptr<SqliteOrderHistoryWriter> writer;
  std::unique_ptr<Shard> shard;
  std::unique_ptr<SqliteOrderHistoryReader> reader;
  std::unique_ptr<boost::asio::io_context> io;
  std::unique_ptr<HttpClient> http;
  std::unique_ptr<WebSocketClient> websocket;
  std::unique_ptr<binance_spot::MarketDataStream> stream;
  std::thread shard_thread;
  std::mutex stop_mutex;
  std::condition_variable stop_cv;
  std::atomic<bool> stopping{false};
  bool started_server = false;
  bool waited = false;
  absl::Status stream_error;
  // Destroy the endpoint before its handler's dependencies.
  std::unique_ptr<ControlServer> control_server;
};

}  // namespace hquant
