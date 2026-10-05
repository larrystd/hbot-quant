#pragma once

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "application/config.h"
#include "boost/asio/awaitable.hpp"
#include "order_history/order_history.h"

namespace hquant {

class SimpleSimulatedExchange;

class Clock;
class ReplayClock;
class SqliteOrderHistoryWriter;
class SqliteOrderHistoryReader;
class Shard;
class ControlServer;

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

  AppConfig config;
  std::string state_dir;
  std::string storage_path;
  bool live = false;
  UtcTime started{};
  RunId run{};
  std::unique_ptr<Clock> clock;
  ReplayClock* replay_clock = nullptr;
  std::unique_ptr<SqliteOrderHistoryWriter> writer;
  // 模拟盘每个分片一个模拟交易所，下标和 shards 一致；实盘为空。
  // 放在 shards 前面，保证分片先销毁。
  std::vector<std::unique_ptr<SimpleSimulatedExchange>> simulated_exchanges;
  std::vector<std::unique_ptr<Shard>> shards;
  std::unique_ptr<SqliteOrderHistoryReader> reader;
  std::mutex stop_mutex;
  std::condition_variable stop_cv;
  std::atomic<bool> stopping{false};
  bool started_server = false;
  bool waited = false;
  // Destroy the endpoint before its handler's dependencies.
  std::unique_ptr<ControlServer> control_server;
};

}  // namespace hquant
