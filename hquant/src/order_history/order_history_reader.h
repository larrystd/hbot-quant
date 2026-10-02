#pragma once

#include <condition_variable>
#include <cstddef>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "absl/status/statusor.h"
#include "boost/asio/any_io_executor.hpp"
#include "order_history/order_history.h"

struct sqlite3;

namespace hquant {

class SqliteOrderHistoryReader final : public OrderHistoryReader {
 public:
  struct Options {
    std::string path;
    size_t queue_capacity = 32;
    uint32_t max_page_size = 500;
  };

  static absl::StatusOr<std::unique_ptr<SqliteOrderHistoryReader>> Open(
      Options options);
  ~SqliteOrderHistoryReader() override;
  SqliteOrderHistoryReader(const SqliteOrderHistoryReader&) = delete;
  SqliteOrderHistoryReader& operator=(const SqliteOrderHistoryReader&) = delete;

  absl::Status TrySubmit(OrderHistoryQuery query) override;
  absl::Status TrySubmitAsync(OrderHistoryQuery query,
                              boost::asio::any_io_executor executor,
                              std::function<void(OrderHistoryPage)> completion);
  // nullopt means no response yet. Every accepted query eventually returns a
  // page with matching request_id and OK or non-OK status.
  std::optional<OrderHistoryPage> TryReceive() override;

 private:
  SqliteOrderHistoryReader(Options options, sqlite3* db, int storage_version);
  void Run();
  OrderHistoryPage Query(const OrderHistoryQuery& query);

  Options options_;
  sqlite3* db_ = nullptr;
  int storage_version_ = 0;
  std::mutex mutex_;
  std::condition_variable cv_;
  struct PendingQuery {
    OrderHistoryQuery query;
    boost::asio::any_io_executor executor;
    std::function<void(OrderHistoryPage)> completion;
  };
  std::deque<PendingQuery> pending_;
  std::deque<OrderHistoryPage> results_;
  size_t outstanding_ = 0;
  bool stopping_ = false;
  std::thread worker_;
};

// A read-only, point-in-time view of one previous run. Recorded order updates
// and trades are historical evidence; the caller must reconcile against the
// exchange before treating them as current state. Loading never sends orders.
struct PreviousRun {
  RunInfo manifest;
  PreviousRunRecords context;
  std::vector<OrderHistoryGap> gaps;
  // An unclean run may have accepted or attempted records after the last
  // durable sequence. Its exact tail length cannot be inferred from SQLite.
  bool may_have_unwritten_records = false;
  bool needs_order_query = true;
};

absl::StatusOr<PreviousRun> LoadPreviousRun(const std::string& path,
                                            RunId run_id);

}  // namespace hquant
