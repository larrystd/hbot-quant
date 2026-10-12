#include "hquant/sqlite_history.h"

#include <algorithm>
#include <charconv>
#include <exception>
#include <limits>
#include <string>
#include <utility>

#include "absl/status/status.h"
#include "sqlite3.h"

namespace hquant::v1 {
namespace {

constexpr const char* kSchema = R"sql(
PRAGMA journal_mode=WAL;
PRAGMA synchronous=FULL;
CREATE TABLE IF NOT EXISTS history_records (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL,
  shard INTEGER NOT NULL,
  shard_sequence INTEGER NOT NULL,
  at_us INTEGER NOT NULL,
  kind INTEGER NOT NULL,
  order_id TEXT NOT NULL,
  trade_id TEXT NOT NULL,
  price_nanos TEXT NOT NULL,
  quantity_nanos TEXT NOT NULL,
  fee_nanos TEXT NOT NULL,
  total_nanos TEXT NOT NULL,
  available_nanos TEXT NOT NULL,
  asset TEXT NOT NULL,
  message TEXT NOT NULL,
  dispatched INTEGER NOT NULL,
  buy INTEGER NOT NULL,
  remaining_nanos TEXT NOT NULL DEFAULT '0',
  UNIQUE(run_id, shard, shard_sequence)
);
CREATE TABLE IF NOT EXISTS history_gaps (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL,
  shard INTEGER NOT NULL,
  first_seq TEXT NOT NULL,
  last_seq TEXT NOT NULL,
  reason TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS account_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL,
  account_id TEXT NOT NULL,
  account_sequence INTEGER NOT NULL,
  shard INTEGER NOT NULL,
  symbol TEXT NOT NULL,
  at_us INTEGER NOT NULL,
  at_mono_ns TEXT NOT NULL DEFAULT '0',
  kind INTEGER NOT NULL,
  client_order_id TEXT NOT NULL,
  exchange_order_id TEXT NOT NULL,
  trade_id TEXT NOT NULL,
  source_bench_seq TEXT NOT NULL,
  price_nanos TEXT NOT NULL,
  quantity_nanos TEXT NOT NULL,
  remaining_nanos TEXT NOT NULL,
  fee_nanos TEXT NOT NULL,
  total_nanos TEXT NOT NULL,
  available_nanos TEXT NOT NULL,
  locked_nanos TEXT NOT NULL,
  asset TEXT NOT NULL,
  message TEXT NOT NULL,
  UNIQUE(run_id, account_id, account_sequence)
);
)sql";

constexpr const char* kInsertRecord = R"sql(
INSERT INTO history_records
(run_id, shard, shard_sequence, at_us, kind, order_id, trade_id,
 price_nanos, quantity_nanos, fee_nanos, total_nanos, available_nanos,
 asset, message, dispatched, buy, remaining_nanos)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
)sql";

constexpr const char* kInsertGap = R"sql(
INSERT INTO history_gaps(run_id, shard, first_seq, last_seq, reason)
VALUES (?, ?, ?, ?, 'queue_full')
)sql";

constexpr const char* kInsertAccount = R"sql(
INSERT INTO account_events
(run_id, account_id, account_sequence, shard, symbol, at_us, at_mono_ns, kind,
 client_order_id, exchange_order_id, trade_id, source_bench_seq,
 price_nanos, quantity_nanos, remaining_nanos, fee_nanos, total_nanos,
 available_nanos, locked_nanos, asset, message)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
)sql";

absl::Status SqliteError(sqlite3* db, const char* context) {
  return absl::InternalError(std::string(context) + ": " + sqlite3_errmsg(db));
}

absl::Status Exec(sqlite3* db, const char* sql) {
  char* message = nullptr;
  const int code = sqlite3_exec(db, sql, nullptr, nullptr, &message);
  if (code == SQLITE_OK) return absl::OkStatus();
  std::string text = message ? message : sqlite3_errmsg(db);
  sqlite3_free(message);
  return absl::InternalError(text);
}

absl::Status MigrateHistoryColumns(sqlite3* db) {
  sqlite3_stmt* statement = nullptr;
  if (sqlite3_prepare_v2(db, "PRAGMA table_info(history_records)", -1,
                         &statement, nullptr) != SQLITE_OK) {
    return SqliteError(db, "inspect history schema");
  }
  bool remaining = false;
  bool dispatched = false;
  bool accepted = false;
  int code = SQLITE_OK;
  while ((code = sqlite3_step(statement)) == SQLITE_ROW) {
    const unsigned char* name = sqlite3_column_text(statement, 1);
    if (!name) continue;
    const std::string_view column(reinterpret_cast<const char*>(name));
    if (column == "remaining_nanos") remaining = true;
    if (column == "dispatched") dispatched = true;
    if (column == "accepted") accepted = true;
  }
  sqlite3_finalize(statement);
  if (code != SQLITE_DONE) return SqliteError(db, "inspect history schema");
  if (!dispatched) {
    if (!accepted)
      return absl::DataLossError("history dispatch column missing");
    if (auto status = Exec(db,
                           "ALTER TABLE history_records RENAME COLUMN "
                           "accepted TO dispatched");
        !status.ok()) {
      return status;
    }
  }
  if (remaining) return absl::OkStatus();
  return Exec(db,
              "ALTER TABLE history_records ADD COLUMN "
              "remaining_nanos TEXT NOT NULL DEFAULT '0'");
}

absl::Status MigrateAccountColumns(sqlite3* db) {
  sqlite3_stmt* statement = nullptr;
  if (sqlite3_prepare_v2(db, "PRAGMA table_info(account_events)", -1,
                         &statement, nullptr) != SQLITE_OK) {
    return SqliteError(db, "inspect account schema");
  }
  bool monotonic = false;
  int code = SQLITE_OK;
  while ((code = sqlite3_step(statement)) == SQLITE_ROW) {
    const unsigned char* name = sqlite3_column_text(statement, 1);
    if (name && std::string_view(reinterpret_cast<const char*>(name)) ==
                    "at_mono_ns") monotonic = true;
  }
  sqlite3_finalize(statement);
  if (code != SQLITE_DONE) return SqliteError(db, "inspect account schema");
  if (monotonic) return absl::OkStatus();
  return Exec(db, "ALTER TABLE account_events ADD COLUMN "
                  "at_mono_ns TEXT NOT NULL DEFAULT '0'");
}

void BindText(sqlite3_stmt* statement, int index, const std::string& text) {
  sqlite3_bind_text(statement, index, text.c_str(),
                    static_cast<int>(text.size()), SQLITE_TRANSIENT);
}

void BindUint(sqlite3_stmt* statement, int index, uint64_t value) {
  char digits[20];
  const auto [end, error] =
      std::to_chars(digits, digits + sizeof(digits), value);
  (void)error;  // 20 decimal digits fit every uint64_t value.
  sqlite3_bind_text(statement, index, digits, static_cast<int>(end - digits),
                    SQLITE_TRANSIENT);
}

absl::StatusOr<uint64_t> ReadUint(sqlite3_stmt* statement, int index) {
  const unsigned char* raw = sqlite3_column_text(statement, index);
  if (!raw) return absl::DataLossError("history unsigned field missing");
  const std::string_view text(reinterpret_cast<const char*>(raw));
  uint64_t value = 0;
  const auto [end, error] =
      std::from_chars(text.data(), text.data() + text.size(), value);
  if (error != std::errc{} || end != text.data() + text.size()) {
    return absl::DataLossError("history unsigned field invalid");
  }
  return value;
}

std::string ReadText(sqlite3_stmt* statement, int index) {
  const unsigned char* raw = sqlite3_column_text(statement, index);
  return raw ? reinterpret_cast<const char*>(raw) : std::string{};
}

}  // namespace

SqliteHistory::SqliteHistory(std::string path, size_t queue_capacity,
                             HistoryPerf* perf, bool wait_for_space)
    : path_(std::move(path)),
      queue_capacity_(queue_capacity),
      perf_(perf),
      wait_for_space_(wait_for_space) {}

SqliteHistory::~SqliteHistory() { (void)FlushAndStop(); }

absl::Status SqliteHistory::Start() {
  if (started_) return absl::FailedPreconditionError("history already started");
  if (stopping_) return absl::FailedPreconditionError("history cannot restart");
  if (queue_capacity_ == 0)
    return absl::InvalidArgumentError("history queue capacity zero");
  if (sqlite3_open_v2(
          path_.c_str(), &writer_db_,
          SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_NOMUTEX,
          nullptr) != SQLITE_OK) {
    auto error = SqliteError(writer_db_, "open history writer");
    sqlite3_close(writer_db_);
    writer_db_ = nullptr;
    return error;
  }
  sqlite3_busy_timeout(writer_db_, 5000);
  if (auto status = Exec(writer_db_, kSchema); !status.ok()) {
    sqlite3_close(writer_db_);
    writer_db_ = nullptr;
    return status;
  }
  if (auto status = MigrateHistoryColumns(writer_db_); !status.ok()) {
    sqlite3_close(writer_db_);
    writer_db_ = nullptr;
    return status;
  }
  if (auto status = MigrateAccountColumns(writer_db_); !status.ok()) {
    sqlite3_close(writer_db_);
    writer_db_ = nullptr;
    return status;
  }
  // The UNIQUE constraint already creates the same index. Older databases
  // may still have the redundant explicit copy.
  if (auto status =
          Exec(writer_db_, "DROP INDEX IF EXISTS history_records_run_shard");
      !status.ok()) {
    sqlite3_close(writer_db_);
    writer_db_ = nullptr;
    return status;
  }
  if (sqlite3_open_v2(path_.c_str(), &reader_db_,
                      SQLITE_OPEN_READONLY | SQLITE_OPEN_NOMUTEX,
                      nullptr) != SQLITE_OK) {
    auto error = SqliteError(reader_db_, "open history reader");
    sqlite3_close(reader_db_);
    reader_db_ = nullptr;
    sqlite3_close(writer_db_);
    writer_db_ = nullptr;
    return error;
  }
  sqlite3_busy_timeout(reader_db_, 5000);
  started_ = true;
  try {
    writer_thread_ = std::thread([this] { WriterLoop(); });
    reader_thread_ = std::thread([this] { ReaderLoop(); });
  } catch (const std::exception& error) {
    {
      std::lock_guard lock(mutex_);
      stopping_ = true;
    }
    writer_cv_.notify_one();
    if (writer_thread_.joinable()) writer_thread_.join();
    sqlite3_close(reader_db_);
    sqlite3_close(writer_db_);
    reader_db_ = nullptr;
    writer_db_ = nullptr;
    started_ = false;
    return absl::ResourceExhaustedError(std::string("start history threads: ") +
                                        error.what());
  }
  return absl::OkStatus();
}

bool SqliteHistory::TryPush(HistoryRecord record) {
  std::unique_lock lock(mutex_);
  if (!started_ || stopping_ || !error_.empty()) return false;
  if (wait_for_space_) {
    space_cv_.wait(lock, [this] {
      return writer_queue_.size() + account_queue_.size() < queue_capacity_ || stopping_ ||
             !error_.empty();
    });
    if (stopping_ || !error_.empty()) return false;
  }
  const bool wake_writer = writer_queue_.empty() && account_queue_.empty() &&
                           gaps_.empty();
  if (writer_queue_.size() + account_queue_.size() >= queue_capacity_) {
    ++dropped_;
    if (!gaps_.empty() && gaps_.back().run_id == record.run_id &&
        gaps_.back().shard == record.shard &&
        gaps_.back().last + 1 == record.sequence) {
      gaps_.back().last = record.sequence;
    } else if (gaps_.size() < queue_capacity_) {
      gaps_.push_back(
          Gap{record.run_id, record.shard, record.sequence, record.sequence});
    } else {
      error_ = "history gap queue full";
    }
    lock.unlock();
    if (wake_writer) writer_cv_.notify_one();
    return false;
  }
  if (perf_) {
    record.perf_enqueue_ns = PerfNowNs();
    record.perf_after_stop =
        perf_->stop_started.load(std::memory_order_acquire);
  }
  const bool after_stop = record.perf_after_stop;
  writer_queue_.push_back(std::move(record));
  if (perf_) {
    perf_->queue_peak = std::max(perf_->queue_peak, writer_queue_.size());
    size_t& phase_peak = after_stop ? perf_->queue_peak_after_stop
                                    : perf_->queue_peak_before_stop;
    phase_peak = std::max(phase_peak, writer_queue_.size());
  }
  lock.unlock();
  if (wake_writer) writer_cv_.notify_one();
  return true;
}

bool SqliteHistory::TryPushAccount(AccountHistoryRecord record) {
  std::lock_guard lock(mutex_);
  if (!started_ || stopping_ || !error_.empty()) return false;
  if (writer_queue_.size() + account_queue_.size() >= queue_capacity_) {
    ++dropped_;
    error_ = "account history queue full";
    return false;
  }
  const bool wake_writer = writer_queue_.empty() && account_queue_.empty() &&
                           gaps_.empty();
  account_queue_.push_back(std::move(record));
  if (wake_writer) writer_cv_.notify_one();
  return true;
}

HistoryHealth SqliteHistory::Health() const {
  std::lock_guard lock(mutex_);
  return HistoryHealth{writer_queue_.size() + account_queue_.size(),
                       dropped_, error_};
}

absl::Status SqliteHistory::WriteBatch(
    const std::vector<HistoryRecord>& records,
    const std::vector<AccountHistoryRecord>& account_records,
    const std::vector<Gap>& gaps, sqlite3_stmt* record_statement,
    sqlite3_stmt* account_statement, sqlite3_stmt* gap_statement) {
  if (auto status = Exec(writer_db_, "BEGIN IMMEDIATE"); !status.ok())
    return status;
  for (const HistoryRecord& record : records) {
    sqlite3_reset(record_statement);
    BindText(record_statement, 1, record.run_id);
    sqlite3_bind_int(record_statement, 2, record.shard.value);
    sqlite3_bind_int64(record_statement, 3,
                       static_cast<sqlite3_int64>(record.sequence));
    sqlite3_bind_int64(record_statement, 4, record.at_us);
    sqlite3_bind_int(record_statement, 5, static_cast<int>(record.kind));
    BindUint(record_statement, 6, record.order_id);
    BindUint(record_statement, 7, record.trade_id);
    BindUint(record_statement, 8, record.price_nanos);
    BindUint(record_statement, 9, record.quantity_nanos);
    BindUint(record_statement, 10, record.fee_nanos);
    BindUint(record_statement, 11, record.total_nanos);
    BindUint(record_statement, 12, record.available_nanos);
    BindText(record_statement, 13, record.asset);
    BindText(record_statement, 14, record.message);
    sqlite3_bind_int(record_statement, 15, record.dispatched);
    sqlite3_bind_int(record_statement, 16, record.buy);
    BindUint(record_statement, 17, record.remaining_nanos);
    if (sqlite3_step(record_statement) != SQLITE_DONE) {
      auto error = SqliteError(writer_db_, "insert history record");
      sqlite3_reset(record_statement);
      (void)Exec(writer_db_, "ROLLBACK");
      return error;
    }
  }
  sqlite3_reset(record_statement);
  for (const AccountHistoryRecord& record : account_records) {
    sqlite3_reset(account_statement);
    BindText(account_statement, 1, record.run_id);
    BindText(account_statement, 2, record.account_id);
    sqlite3_bind_int64(account_statement, 3,
                       static_cast<sqlite3_int64>(record.sequence));
    sqlite3_bind_int(account_statement, 4, record.shard.value);
    BindText(account_statement, 5, record.symbol);
    sqlite3_bind_int64(account_statement, 6, record.at_us);
    BindUint(account_statement, 7, record.at_mono_ns);
    sqlite3_bind_int(account_statement, 8, static_cast<int>(record.kind));
    BindText(account_statement, 9, record.client_order_id);
    BindUint(account_statement, 10, record.exchange_order_id);
    BindUint(account_statement, 11, record.trade_id);
    BindUint(account_statement, 12, record.source_bench_seq);
    BindUint(account_statement, 13, record.price_nanos);
    BindUint(account_statement, 14, record.quantity_nanos);
    BindUint(account_statement, 15, record.remaining_nanos);
    BindUint(account_statement, 16, record.fee_nanos);
    BindUint(account_statement, 17, record.total_nanos);
    BindUint(account_statement, 18, record.available_nanos);
    BindUint(account_statement, 19, record.locked_nanos);
    BindText(account_statement, 20, record.asset);
    BindText(account_statement, 21, record.message);
    if (sqlite3_step(account_statement) != SQLITE_DONE) {
      auto error = SqliteError(writer_db_, "insert account event");
      sqlite3_reset(account_statement);
      (void)Exec(writer_db_, "ROLLBACK");
      return error;
    }
  }
  sqlite3_reset(account_statement);
  for (const Gap& gap : gaps) {
    sqlite3_reset(gap_statement);
    BindText(gap_statement, 1, gap.run_id);
    sqlite3_bind_int(gap_statement, 2, gap.shard.value);
    BindUint(gap_statement, 3, gap.first);
    BindUint(gap_statement, 4, gap.last);
    if (sqlite3_step(gap_statement) != SQLITE_DONE) {
      auto error = SqliteError(writer_db_, "insert history gap");
      sqlite3_reset(gap_statement);
      (void)Exec(writer_db_, "ROLLBACK");
      return error;
    }
  }
  if (!gaps.empty()) sqlite3_reset(gap_statement);
  return Exec(writer_db_, "COMMIT");
}

void SqliteHistory::WriterLoop() {
  sqlite3_stmt* record_statement = nullptr;
  sqlite3_stmt* account_statement = nullptr;
  sqlite3_stmt* gap_statement = nullptr;
  if (sqlite3_prepare_v2(writer_db_, kInsertRecord, -1, &record_statement,
                         nullptr) != SQLITE_OK ||
      sqlite3_prepare_v2(writer_db_, kInsertAccount, -1, &account_statement,
                         nullptr) != SQLITE_OK ||
      sqlite3_prepare_v2(writer_db_, kInsertGap, -1, &gap_statement, nullptr) !=
          SQLITE_OK) {
    const auto error = SqliteError(writer_db_, "prepare history writer");
    sqlite3_finalize(record_statement);
    sqlite3_finalize(account_statement);
    sqlite3_finalize(gap_statement);
    std::lock_guard lock(mutex_);
    error_ = std::string(error.message());
    stopping_ = true;
    space_cv_.notify_all();
    return;
  }
  std::vector<HistoryRecord> records;
  records.reserve(64);
  std::vector<AccountHistoryRecord> account_records;
  account_records.reserve(64);
  std::vector<Gap> gaps;
  for (;;) {
    records.clear();
    account_records.clear();
    gaps.clear();
    {
      std::unique_lock lock(mutex_);
      writer_cv_.wait(lock, [this] {
        return stopping_ || !writer_queue_.empty() ||
               !account_queue_.empty() || !gaps_.empty();
      });
      if (writer_queue_.empty() && account_queue_.empty() && gaps_.empty() &&
          stopping_) break;
      const size_t count = std::min<size_t>(writer_queue_.size(), 64);
      for (size_t i = 0; i < count; ++i) {
        records.push_back(std::move(writer_queue_.front()));
        writer_queue_.pop_front();
      }
      const size_t account_count = std::min<size_t>(account_queue_.size(), 64);
      for (size_t i = 0; i < account_count; ++i) {
        account_records.push_back(std::move(account_queue_.front()));
        account_queue_.pop_front();
      }
      gaps.swap(gaps_);
    }
    if (wait_for_space_) space_cv_.notify_all();
    const uint64_t write_begin = perf_ ? PerfNowNs() : 0;
    if (auto status =
            WriteBatch(records, account_records, gaps, record_statement,
                       account_statement, gap_statement);
        !status.ok()) {
      std::lock_guard lock(mutex_);
      error_ = std::string(status.message());
      writer_queue_.clear();
      account_queue_.clear();
      gaps_.clear();
      stopping_ = true;
      space_cv_.notify_all();
      break;
    }
    if (perf_) {
      const uint64_t committed_at = PerfNowNs();
      ++perf_->batches;
      perf_->records_written += records.size();
      perf_->account_records_written += account_records.size();
      const uint64_t elapsed = committed_at - write_begin;
      perf_->batch_write_ns += elapsed;
      perf_->batch_write_samples_ns.push_back(elapsed);
      for (const HistoryRecord& record : records) {
        const uint64_t queue_wait = write_begin - record.perf_enqueue_ns;
        const uint64_t to_commit = committed_at - record.perf_enqueue_ns;
        perf_->queue_wait_ns.push_back(queue_wait);
        perf_->enqueue_to_commit_ns.push_back(to_commit);
        perf_->after_stop_flags.push_back(record.perf_after_stop);
        perf_->input_to_commit_ns.push_back(committed_at -
                                            record.perf_input_origin_ns);
      }
    }
  }
  sqlite3_finalize(record_statement);
  sqlite3_finalize(account_statement);
  sqlite3_finalize(gap_statement);
}

absl::StatusOr<HistoryPage> SqliteHistory::ReadPage(uint32_t limit,
                                                    uint64_t cursor) {
  if (cursor > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
    return absl::InvalidArgumentError("history cursor out of range");
  }
  sqlite3_stmt* statement = nullptr;
  const char* sql = R"sql(
SELECT id, run_id, shard, shard_sequence, at_us, kind, order_id, trade_id,
       price_nanos, quantity_nanos, fee_nanos, total_nanos, available_nanos,
       asset, message, dispatched, buy, remaining_nanos
FROM history_records WHERE id > ? ORDER BY id LIMIT ?
)sql";
  if (sqlite3_prepare_v2(reader_db_, sql, -1, &statement, nullptr) !=
      SQLITE_OK) {
    return SqliteError(reader_db_, "prepare history query");
  }
  sqlite3_bind_int64(statement, 1, static_cast<sqlite3_int64>(cursor));
  sqlite3_bind_int(statement, 2, static_cast<int>(limit));
  HistoryPage page;
  page.next_cursor = cursor;
  for (;;) {
    const int code = sqlite3_step(statement);
    if (code == SQLITE_DONE) break;
    if (code != SQLITE_ROW) {
      auto error = SqliteError(reader_db_, "query history");
      sqlite3_finalize(statement);
      return error;
    }
    HistoryRecord record;
    page.next_cursor =
        static_cast<uint64_t>(sqlite3_column_int64(statement, 0));
    record.run_id = ReadText(statement, 1);
    record.shard.value =
        static_cast<uint16_t>(sqlite3_column_int(statement, 2));
    record.sequence = static_cast<uint64_t>(sqlite3_column_int64(statement, 3));
    record.at_us = sqlite3_column_int64(statement, 4);
    record.kind = static_cast<HistoryKind>(sqlite3_column_int(statement, 5));
    auto order_id = ReadUint(statement, 6);
    auto trade_id = ReadUint(statement, 7);
    auto price = ReadUint(statement, 8);
    auto quantity = ReadUint(statement, 9);
    auto remaining = ReadUint(statement, 17);
    auto fee = ReadUint(statement, 10);
    auto total = ReadUint(statement, 11);
    auto available = ReadUint(statement, 12);
    if (!order_id.ok() || !trade_id.ok() || !price.ok() || !quantity.ok() ||
        !remaining.ok() || !fee.ok() || !total.ok() || !available.ok()) {
      sqlite3_finalize(statement);
      return absl::DataLossError("corrupt history numeric field");
    }
    record.order_id = *order_id;
    record.trade_id = *trade_id;
    record.price_nanos = *price;
    record.quantity_nanos = *quantity;
    record.remaining_nanos = *remaining;
    record.fee_nanos = *fee;
    record.total_nanos = *total;
    record.available_nanos = *available;
    record.asset = ReadText(statement, 13);
    record.message = ReadText(statement, 14);
    record.dispatched = sqlite3_column_int(statement, 15) != 0;
    record.buy = sqlite3_column_int(statement, 16) != 0;
    page.records.push_back(std::move(record));
  }
  sqlite3_finalize(statement);
  return page;
}

void SqliteHistory::ReaderLoop() {
  for (;;) {
    QueryJob job;
    {
      std::unique_lock lock(reader_mutex_);
      reader_cv_.wait(
          lock, [this] { return reader_stopping_ || !reader_queue_.empty(); });
      if (reader_queue_.empty() && reader_stopping_) break;
      job = std::move(reader_queue_.front());
      reader_queue_.pop_front();
    }
    job.done.set_value(ReadPage(job.limit, job.cursor));
  }
}

absl::StatusOr<HistoryPage> SqliteHistory::Query(uint32_t limit,
                                                 uint64_t cursor) {
  if (limit == 0 || limit > 500) {
    return absl::InvalidArgumentError("history limit must be in [1,500]");
  }
  QueryJob job;
  job.limit = limit;
  job.cursor = cursor;
  auto future = job.done.get_future();
  {
    std::lock_guard lock(reader_mutex_);
    if (!started_ || reader_stopping_) {
      return absl::FailedPreconditionError("history reader stopped");
    }
    if (reader_queue_.size() >= 32) {
      return absl::ResourceExhaustedError("history reader queue full");
    }
    reader_queue_.push_back(std::move(job));
  }
  reader_cv_.notify_one();
  return future.get();
}

absl::Status SqliteHistory::FlushAndStop() {
  if (!started_) return absl::OkStatus();
  {
    std::lock_guard lock(mutex_);
    stopping_ = true;
  }
  writer_cv_.notify_one();
  space_cv_.notify_all();
  if (writer_thread_.joinable()) writer_thread_.join();
  {
    std::lock_guard lock(reader_mutex_);
    reader_stopping_ = true;
  }
  reader_cv_.notify_one();
  if (reader_thread_.joinable()) reader_thread_.join();
  sqlite3_close(reader_db_);
  sqlite3_close(writer_db_);
  reader_db_ = nullptr;
  writer_db_ = nullptr;
  started_ = false;
  std::lock_guard lock(mutex_);
  return error_.empty() ? absl::OkStatus() : absl::InternalError(error_);
}

}  // namespace hquant::v1
