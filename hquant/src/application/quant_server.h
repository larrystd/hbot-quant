#pragma once

#include <memory>
#include <string>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "application/config.h"
#include "boost/asio/awaitable.hpp"
#include "storage/storage.h"

namespace hquant {

// Owns one trading chain, its data source, history, and management endpoint.
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
  boost::asio::awaitable<absl::StatusOr<HistoryPage>> HistoryAsync(
      HistoryQuery query);

 private:
  struct Impl;
  explicit QuantServer(std::unique_ptr<Impl> impl);
  std::unique_ptr<Impl> impl_;
};

}  // namespace hquant
