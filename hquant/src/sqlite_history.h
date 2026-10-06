#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <future>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "hquant/base/ids.h"
#include "hquant/perf_probe.h"

struct sqlite3;
struct sqlite3_stmt;

namespace hquant::v1 {

enum class HistoryKind {
  SubmitCommand = 1,
  CancelCommand = 2,
  OrderAccepted = 3,
  OrderFilled = 4,
  OrderCancelled = 5,
  Fill = 6,
  Balance = 7,
  OrderRejected = 8,
  OrderPartiallyFilled = 9,
};

struct HistoryRecord {
  std::string run_id;
  ShardId shard;
  uint64_t sequence = 0;
  int64_t at_us = 0;
  HistoryKind kind = HistoryKind::SubmitCommand;
  uint64_t order_id = 0;
  uint64_t trade_id = 0;
  uint64_t price_nanos = 0;
  uint64_t quantity_nanos = 0;
  uint64_t remaining_nanos = 0;
  uint64_t fee_nanos = 0;
  uint64_t total_nanos = 0;
  uint64_t available_nanos = 0;
  std::string asset;
  std::string message;
  bool dispatched = false;
  bool buy = false;
  uint64_t perf_input_origin_ns = 0;
  uint64_t perf_enqueue_ns = 0;
};

struct HistoryPage {
  std::vector<HistoryRecord> records;
  uint64_t next_cursor = 0;
};

struct HistoryHealth {
  size_t queued = 0;
  uint64_t dropped = 0;
  std::string error;
};

class SqliteHistory {
 public:
  explicit SqliteHistory(std::string path, size_t queue_capacity = 8192,
                         HistoryPerf* perf = nullptr,
                         bool wait_for_space = false);
  ~SqliteHistory();
  SqliteHistory(const SqliteHistory&) = delete;
  SqliteHistory& operator=(const SqliteHistory&) = delete;

  absl::Status Start();
  bool TryPush(HistoryRecord record);
  absl::StatusOr<HistoryPage> Query(uint32_t limit, uint64_t cursor);
  HistoryHealth Health() const;
  absl::Status FlushAndStop();

 private:
  struct Gap {
    std::string run_id;
    ShardId shard;
    uint64_t first = 0;
    uint64_t last = 0;
  };
  struct QueryJob {
    uint32_t limit = 0;
    uint64_t cursor = 0;
    std::promise<absl::StatusOr<HistoryPage>> done;
  };
  void WriterLoop();
  void ReaderLoop();
  absl::Status WriteBatch(const std::vector<HistoryRecord>& records,
                          const std::vector<Gap>& gaps,
                          sqlite3_stmt* record_statement,
                          sqlite3_stmt* gap_statement);
  absl::StatusOr<HistoryPage> ReadPage(uint32_t limit, uint64_t cursor);

  std::string path_;
  size_t queue_capacity_;
  HistoryPerf* perf_ = nullptr;
  bool wait_for_space_ = false;
  mutable std::mutex mutex_;
  std::condition_variable writer_cv_;
  std::condition_variable space_cv_;
  std::deque<HistoryRecord> writer_queue_;
  std::vector<Gap> gaps_;
  uint64_t dropped_ = 0;
  std::string error_;
  std::atomic<bool> started_{false};
  bool stopping_ = false;
  std::thread writer_thread_;
  sqlite3* writer_db_ = nullptr;

  std::mutex reader_mutex_;
  std::condition_variable reader_cv_;
  std::deque<QueryJob> reader_queue_;
  bool reader_stopping_ = false;
  std::thread reader_thread_;
  sqlite3* reader_db_ = nullptr;
};

}  // namespace hquant::v1
