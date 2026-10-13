#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace hquant::bench {

struct VenuePorts {
  uint16_t public_rest = 0;
  uint16_t public_ws = 0;
  uint16_t trade_rest = 0;
  uint16_t private_ws = 0;
};

struct VenueTrace {
  bool performance_qos_requested = false;
  int market_io_qos_status = 0;
  int rest_io_qos_status = 0;
  uint64_t market_messages = 0;
  uint64_t market_actions = 0;
  uint64_t account_submits = 0;
  uint64_t account_cancels = 0;
  uint64_t account_fills = 0;
  uint64_t private_reports = 0;
  std::vector<uint64_t> market_send_ns;
  struct AccountStamp {
    std::string client_id;
    std::string status;
    uint64_t at_ns = 0;
    uint64_t exchange_order_id = 0;
    uint64_t trade_id = 0;
    uint64_t write_done_ns = 0;
    uint64_t first_socket_read_ns = 0;
    uint64_t last_socket_read_ns = 0;
    uint32_t socket_reads = 0;
    uint64_t read_ready_ns = 0;
  };
  std::vector<AccountStamp> requests;
  std::vector<AccountStamp> reports;
  struct MatchStamp {
    uint64_t trade_id = 0;
    uint64_t action_ns = 0;
    uint64_t market_seq = 0;
  };
  std::vector<MatchStamp> matches;
  struct BalanceStamp {
    std::string asset;
    int64_t free_nanos = 0;
    int64_t locked_nanos = 0;
  };
  std::vector<BalanceStamp> balances;
};

// One loopback exchange process: public market and account trading share one
// matching engine. Owns all socket I/O on a dedicated C++ io_context thread.
class SimVenue {
 public:
  explicit SimVenue(std::size_t symbols);
  ~SimVenue();
  SimVenue(const SimVenue&) = delete;
  SimVenue& operator=(const SimVenue&) = delete;

  VenuePorts ports() const;
  static std::string Symbol(std::size_t shard);
  void Start();
  bool Ready() const;
  void BeginReplay(uint32_t duration_seconds);
  bool ReplayDone() const;
  uint64_t MarketMessageCount() const;
  uint64_t PrivateReportCount() const;
  uint64_t PendingPrivateReports() const;
  VenueTrace StopAndCollect();

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace hquant::bench
