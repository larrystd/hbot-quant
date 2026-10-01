#pragma once

#include <condition_variable>
#include <cstddef>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "absl/status/statusor.h"
#include "storage/storage.h"

struct sqlite3;

namespace hquant {

class SqliteHistoryReader final : public HistoryReader {
 public:
  struct Options {
    std::string path;
    size_t queue_capacity = 32;
    uint32_t max_page_size = 500;
  };

  static absl::StatusOr<std::unique_ptr<SqliteHistoryReader>> Open(
      Options options);
  ~SqliteHistoryReader() override;
  SqliteHistoryReader(const SqliteHistoryReader&) = delete;
  SqliteHistoryReader& operator=(const SqliteHistoryReader&) = delete;

  absl::Status TrySubmit(HistoryQuery query) override;
  // nullopt means no response yet. Every accepted query eventually returns a
  // page with matching request_id and OK or non-OK status.
  std::optional<HistoryPage> TryReceive() override;

 private:
  SqliteHistoryReader(Options options, sqlite3* db, int storage_version);
  void Run();
  HistoryPage Query(const HistoryQuery& query);

  Options options_;
  sqlite3* db_ = nullptr;
  int storage_version_ = 0;
  std::mutex mutex_;
  std::condition_variable cv_;
  std::deque<HistoryQuery> pending_;
  std::deque<HistoryPage> results_;
  size_t outstanding_ = 0;
  bool stopping_ = false;
  std::thread worker_;
};

// A read-only, point-in-time view of one previous run. Recorded order updates
// and trades are historical evidence; the caller must reconcile against the
// exchange before treating them as current state. Loading never sends orders.
struct RecoverySnapshot {
  RunManifest manifest;
  RecoveryContext context;
  std::vector<HistoryGap> gaps;
  // An unclean run may have accepted or attempted records after the last
  // durable sequence. Its exact tail length cannot be inferred from SQLite.
  bool crash_tail_possible = false;
  bool needs_reconciliation = true;
};

absl::StatusOr<RecoverySnapshot> LoadRecoverySnapshot(const std::string& path,
                                                      RunId run_id);

}  // namespace hquant
