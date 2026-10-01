#include "storage/recorder.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base/error.h"
#include "sqlite3.h"
#include "storage/record_codec.h"

namespace hquant {
namespace {

constexpr char kSchema[] = R"sql(
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS schema_migrations(version INTEGER PRIMARY KEY, applied_at_us INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS run_manifest(run_id TEXT PRIMARY KEY, started_at_us INTEGER NOT NULL,
 clean_stopped_at_us INTEGER, history_complete INTEGER NOT NULL CHECK(history_complete IN (0,1)));
CREATE TABLE IF NOT EXISTS committed_sequence(run_id TEXT NOT NULL, shard INTEGER NOT NULL,
 last_seq INTEGER NOT NULL, PRIMARY KEY(run_id,shard), FOREIGN KEY(run_id) REFERENCES run_manifest(run_id));
CREATE TABLE IF NOT EXISTS history_records(id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT NOT NULL,
 shard INTEGER NOT NULL, shard_sequence INTEGER NOT NULL, schema_version INTEGER NOT NULL,
 owner_key INTEGER, received_at_us INTEGER NOT NULL, exchange_at_us INTEGER, account TEXT,
 market_venue TEXT, market_kind INTEGER, market_symbol TEXT, payload_kind INTEGER NOT NULL,
 record_blob BLOB NOT NULL, UNIQUE(run_id,shard,shard_sequence),
 FOREIGN KEY(run_id) REFERENCES run_manifest(run_id));
CREATE INDEX IF NOT EXISTS history_records_time_id ON history_records(received_at_us,id);
CREATE INDEX IF NOT EXISTS history_records_account_id ON history_records(account,id);
CREATE INDEX IF NOT EXISTS history_records_owner_id ON history_records(owner_key,id);
CREATE INDEX IF NOT EXISTS history_records_market_id ON history_records(market_venue,market_kind,market_symbol,id);
-- In schema version 2, reason contains an ErrorCode rather than a GapReason ordinal.
CREATE TABLE IF NOT EXISTS history_gaps(id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT NOT NULL,
 shard INTEGER NOT NULL, first_seq INTEGER NOT NULL, last_seq INTEGER NOT NULL, reason INTEGER NOT NULL,
 UNIQUE(run_id,shard,first_seq,last_seq,reason), FOREIGN KEY(run_id) REFERENCES run_manifest(run_id));
)sql";

absl::Status SqlError(sqlite3* db, std::string prefix,
                      ErrorCode code = ErrorCode::kStorageWriteFailed) {
  return Error(code, std::move(prefix) + ": " +
                         (db ? sqlite3_errmsg(db) : "no database"));
}
absl::Status Exec(sqlite3* db, const char* sql,
                  ErrorCode code = ErrorCode::kStorageWriteFailed) {
  char* error = nullptr;
  const int rc = sqlite3_exec(db, sql, nullptr, nullptr, &error);
  if (rc == SQLITE_OK) return absl::OkStatus();
  std::string message = error ? error : sqlite3_errmsg(db);
  sqlite3_free(error);
  return Error(code, message);
}
void Rollback(sqlite3* db) {
  char* error = nullptr;
  sqlite3_exec(db, "ROLLBACK", nullptr, nullptr, &error);
  sqlite3_free(error);
}
using Statement = std::unique_ptr<sqlite3_stmt, decltype(&sqlite3_finalize)>;
absl::StatusOr<Statement> Prepare(sqlite3* db, const char* sql) {
  sqlite3_stmt* statement = nullptr;
  if (sqlite3_prepare_v2(db, sql, -1, &statement, nullptr) != SQLITE_OK)
    return SqlError(db, "prepare");
  return Statement(statement, &sqlite3_finalize);
}
void Text(sqlite3_stmt* statement, int index, const std::string& value) {
  sqlite3_bind_text(statement, index, value.data(),
                    static_cast<int>(value.size()), SQLITE_TRANSIENT);
}
int64_t Us(UtcTime value) { return value.time_since_epoch().count(); }

}  // namespace

SqliteHistoryWriter::SqliteHistoryWriter(Options options, sqlite3* db)
    : options_(std::move(options)), db_(db) {
  for (auto& queue : queues_) {
    queue = std::make_unique<Queue>(options_.queue_capacity_per_shard);
  }
  manifest_.run_id = options_.run_id;
  manifest_.started_at_utc = options_.started_at_utc;
}

absl::StatusOr<std::unique_ptr<SqliteHistoryWriter>> SqliteHistoryWriter::Open(
    Options options) {
  if (options.path.empty() || !options.run_id.IsValid() ||
      options.queue_capacity_per_shard == 0 || options.batch_size == 0 ||
      options.queue_capacity_per_shard > 1'000'000 ||
      options.batch_size > 10'000)
    return Error(ErrorCode::kStorageConfigInvalid, "invalid recorder options");
  sqlite3* db = nullptr;
  if (sqlite3_open_v2(
          options.path.c_str(), &db,
          SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_NOMUTEX,
          nullptr) != SQLITE_OK) {
    absl::Status error =
        SqlError(db, "open recorder database", ErrorCode::kStorageOpenFailed);
    sqlite3_close(db);
    return error;
  }
  sqlite3_busy_timeout(db, 1000);
  auto schema = Exec(db, kSchema, ErrorCode::kStorageOpenFailed);
  if (!schema.ok()) {
    sqlite3_close(db);
    return schema;
  }
  // Existing version 1 rows use 0..3; convert them before publishing version 2.
  auto version_query =
      Prepare(db, "SELECT MAX(version) FROM schema_migrations");
  if (!version_query.ok()) {
    sqlite3_close(db);
    return version_query.status();
  }
  if (sqlite3_step(version_query->get()) != SQLITE_ROW) {
    auto error = SqlError(db, "read storage schema version");
    version_query->reset();
    sqlite3_close(db);
    return error;
  }
  const int version =
      sqlite3_column_type(version_query->get(), 0) == SQLITE_NULL
          ? 0
          : sqlite3_column_int(version_query->get(), 0);
  version_query->reset();
  if (version < 0 || version > 2) {
    sqlite3_close(db);
    return Error(ErrorCode::kStorageConfigInvalid,
                 "unsupported storage schema version");
  }
  if (version < 2) {
    auto migration = Exec(db, "BEGIN IMMEDIATE");
    if (migration.ok() && version == 1) {
      migration = Exec(db,
                       "UPDATE history_gaps SET reason=CASE reason "
                       "WHEN 0 THEN 17004 WHEN 1 THEN 17003 "
                       "WHEN 2 THEN 17013 WHEN 3 THEN 10900 END "
                       "WHERE reason BETWEEN 0 AND 3");
    }
    if (migration.ok()) {
      migration = Exec(db,
                       "INSERT INTO schema_migrations(version,applied_at_us) "
                       "VALUES(2,unixepoch('subsec')*1000000)");
    }
    if (migration.ok()) migration = Exec(db, "COMMIT");
    if (!migration.ok()) {
      Rollback(db);
      sqlite3_close(db);
      return migration;
    }
  }
  auto statement =
      Prepare(db,
              "INSERT INTO run_manifest(run_id,started_at_us,history_complete) "
              "VALUES(?1,?2,1)");
  if (!statement.ok()) {
    sqlite3_close(db);
    return statement.status();
  }
  Text(statement->get(), 1, std::to_string(options.run_id.value));
  sqlite3_bind_int64(statement->get(), 2, Us(options.started_at_utc));
  if (sqlite3_step(statement->get()) != SQLITE_DONE) {
    auto error = SqlError(db, "insert run manifest (run ID may already exist)");
    statement->reset();
    sqlite3_close(db);
    return error;
  }
  statement->reset();
  auto recorder = std::unique_ptr<SqliteHistoryWriter>(
      new SqliteHistoryWriter(std::move(options), db));
  recorder->worker_ = std::thread([self = recorder.get()] { self->Run(); });
  return recorder;
}

SqliteHistoryWriter::~SqliteHistoryWriter() {
  {
    std::lock_guard lock(wake_mutex_);
    stopping_ = true;
  }
  wake_cv_.notify_one();
  if (worker_.joinable()) worker_.join();
  if (db_) sqlite3_close(db_);
}

bool SqliteHistoryWriter::TryPush(HistoryRecord record) {
  if (!record.shard.IsValid() || record.run_id != options_.run_id ||
      record.shard_sequence == 0 || record.schema_version != 2)
    return false;
  Queue& queue = *queues_[record.shard.value];
  const uint64_t previous =
      queue.last_attempted_seq.load(std::memory_order_relaxed);
  if (record.shard_sequence <= previous) return false;
  queue.last_attempted_seq.store(record.shard_sequence,
                                 std::memory_order_relaxed);
  const size_t tail = queue.tail.load(std::memory_order_relaxed);
  const size_t head = queue.head.load(std::memory_order_acquire);
  if (tail - head == queue.slots.size()) {
    queue.last_dropped_seq.store(record.shard_sequence,
                                 std::memory_order_release);
    queue.dropped_count.fetch_add(1, std::memory_order_relaxed);
    wake_cv_.notify_one();
    return false;
  }
  pending_records_.fetch_add(1, std::memory_order_relaxed);
  const size_t depth = pending_records_.load(std::memory_order_relaxed);
  size_t peak = queue_watermark_.load(std::memory_order_relaxed);
  while (depth > peak && !queue_watermark_.compare_exchange_weak(
                             peak, depth, std::memory_order_relaxed)) {
  }
  queue.slots[tail % queue.slots.size()] = std::move(record);
  queue.tail.store(tail + 1, std::memory_order_release);
  wake_cv_.notify_one();
  return true;
}

bool SqliteHistoryWriter::Pop(uint8_t shard, HistoryRecord* record) {
  Queue& queue = *queues_[shard];
  const size_t head = queue.head.load(std::memory_order_relaxed);
  if (head == queue.tail.load(std::memory_order_acquire)) return false;
  *record = std::move(*queue.slots[head % queue.slots.size()]);
  queue.slots[head % queue.slots.size()].reset();
  queue.head.store(head + 1, std::memory_order_release);
  return true;
}

void SqliteHistoryWriter::AddGap(const HistoryGap& gap) {
  if (gap.first_seq > gap.last_seq) return;
  pending_gaps_.push_back(gap);
  std::lock_guard lock(state_mutex_);
  health_.gap_ranges.push_back(gap);
  manifest_.history_complete = false;
}

void SqliteHistoryWriter::SetError(const absl::Status& status) {
  if (status.ok()) return;
  std::lock_guard lock(state_mutex_);
  health_.last_error = std::string(status.message());
}

absl::Status SqliteHistoryWriter::WriteBatch(
    const std::vector<HistoryRecord>& batch) {
  if (batch.empty()) return absl::OkStatus();
  if (fail_writes_for_testing_.load(std::memory_order_acquire))
    return Error(ErrorCode::kStorageWriteFailed,
                 "injected SQLite write failure");
  auto begin = Exec(db_, "BEGIN IMMEDIATE");
  if (!begin.ok()) return begin;
  auto statement = Prepare(db_, R"sql(INSERT INTO history_records(
    run_id,shard,shard_sequence,schema_version,owner_key,received_at_us,exchange_at_us,
    account,market_venue,market_kind,market_symbol,payload_kind,record_blob)
    VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13))sql");
  auto sequence =
      Prepare(db_, R"sql(INSERT INTO committed_sequence(run_id,shard,last_seq)
    VALUES(?1,?2,?3) ON CONFLICT(run_id,shard) DO UPDATE SET last_seq=excluded.last_seq)sql");
  if (!statement.ok() || !sequence.ok()) {
    Rollback(db_);
    return statement.ok() ? sequence.status() : statement.status();
  }
  std::array<uint64_t, 8> committed{};
  const std::string run_id = std::to_string(options_.run_id.value);
  for (const auto& record : batch) {
    sqlite3_stmt* s = statement->get();
    sqlite3_reset(s);
    sqlite3_clear_bindings(s);
    auto index = storage_internal::IndexRecord(record);
    const std::string blob = storage_internal::EncodeRecord(record);
    Text(s, 1, run_id);
    sqlite3_bind_int(s, 2, record.shard.value);
    sqlite3_bind_int64(s, 3, static_cast<sqlite3_int64>(record.shard_sequence));
    sqlite3_bind_int(s, 4, record.schema_version);
    if (record.strategy_id)
      sqlite3_bind_int64(s, 5,
                         static_cast<sqlite3_int64>(record.strategy_id->value));
    else
      sqlite3_bind_null(s, 5);
    sqlite3_bind_int64(s, 6, Us(record.received_at_utc));
    if (record.exchange_at_utc)
      sqlite3_bind_int64(s, 7, Us(*record.exchange_at_utc));
    else
      sqlite3_bind_null(s, 7);
    if (index.account)
      Text(s, 8, *index.account);
    else
      sqlite3_bind_null(s, 8);
    if (index.market) {
      Text(s, 9, index.market->exchange.value);
      sqlite3_bind_int(s, 10, static_cast<int>(index.market->instrument_kind));
      Text(s, 11, index.market->native_symbol);
    } else {
      sqlite3_bind_null(s, 9);
      sqlite3_bind_null(s, 10);
      sqlite3_bind_null(s, 11);
    }
    sqlite3_bind_int(s, 12, index.payload_kind);
    sqlite3_bind_blob(s, 13, blob.data(), static_cast<int>(blob.size()),
                      SQLITE_TRANSIENT);
    if (sqlite3_step(s) != SQLITE_DONE) {
      auto error = SqlError(db_, "insert history record");
      Rollback(db_);
      return error;
    }
    committed[record.shard.value] = record.shard_sequence;
  }
  for (uint8_t shard = 0; shard < 8; ++shard) {
    if (committed[shard] == 0) continue;
    sqlite3_stmt* s = sequence->get();
    sqlite3_reset(s);
    sqlite3_clear_bindings(s);
    Text(s, 1, run_id);
    sqlite3_bind_int(s, 2, shard);
    sqlite3_bind_int64(s, 3, static_cast<sqlite3_int64>(committed[shard]));
    if (sqlite3_step(s) != SQLITE_DONE) {
      auto error = SqlError(db_, "update committed sequence");
      Rollback(db_);
      return error;
    }
  }
  auto commit = Exec(db_, "COMMIT");
  if (!commit.ok()) {
    Rollback(db_);
    return commit;
  }
  {
    std::lock_guard lock(state_mutex_);
    for (uint8_t shard = 0; shard < 8; ++shard) {
      if (committed[shard] != 0) {
        manifest_.last_committed_seq_by_shard[shard] = committed[shard];
        health_.last_committed_seq_by_shard[shard] = committed[shard];
      }
    }
  }
  return absl::OkStatus();
}

absl::Status SqliteHistoryWriter::PersistPendingGaps() {
  if (pending_gaps_.empty()) return absl::OkStatus();
  if (fail_writes_for_testing_.load(std::memory_order_acquire))
    return Error(ErrorCode::kStorageGapPersistFailed,
                 "injected SQLite write failure");
  auto begin = Exec(db_, "BEGIN IMMEDIATE");
  if (!begin.ok()) return begin;
  auto statement = Prepare(db_,
                           "INSERT OR IGNORE INTO "
                           "history_gaps(run_id,shard,first_seq,last_seq,"
                           "reason) VALUES(?1,?2,?3,?4,?5)");
  if (!statement.ok()) {
    Rollback(db_);
    return statement.status();
  }
  for (const auto& gap : pending_gaps_) {
    sqlite3_stmt* s = statement->get();
    sqlite3_reset(s);
    sqlite3_clear_bindings(s);
    Text(s, 1, std::to_string(gap.run_id.value));
    sqlite3_bind_int(s, 2, gap.shard.value);
    sqlite3_bind_int64(s, 3, static_cast<sqlite3_int64>(gap.first_seq));
    sqlite3_bind_int64(s, 4, static_cast<sqlite3_int64>(gap.last_seq));
    sqlite3_bind_int(s, 5, static_cast<int>(StoredErrorNumber(gap.reason)));
    if (sqlite3_step(s) != SQLITE_DONE) {
      auto error = SqlError(db_, "insert history gap");
      Rollback(db_);
      return error;
    }
  }
  const std::string update_sql =
      "UPDATE run_manifest SET history_complete=0 WHERE run_id='" +
      std::to_string(options_.run_id.value) + "'";
  auto update = Exec(db_, update_sql.c_str());
  if (!update.ok()) {
    Rollback(db_);
    return update;
  }
  auto commit = Exec(db_, "COMMIT");
  if (!commit.ok()) {
    Rollback(db_);
    return commit;
  }
  pending_gaps_.clear();
  return absl::OkStatus();
}

absl::Status SqliteHistoryWriter::FinishManifest(UtcTime clean_time) {
  if (!pending_gaps_.empty())
    return Error(ErrorCode::kStorageGapPersistFailed,
                 "history gaps could not be persisted");
  if (fail_writes_for_testing_.load(std::memory_order_acquire))
    return Error(ErrorCode::kStorageWriteFailed,
                 "injected SQLite write failure");
  auto statement =
      Prepare(db_,
              "UPDATE run_manifest SET "
              "clean_stopped_at_us=?1,history_complete=?2 WHERE run_id=?3");
  if (!statement.ok()) return statement.status();
  sqlite3_bind_int64(statement->get(), 1, Us(clean_time));
  bool complete;
  {
    std::lock_guard lock(state_mutex_);
    complete = manifest_.history_complete;
  }
  sqlite3_bind_int(statement->get(), 2, complete ? 1 : 0);
  Text(statement->get(), 3, std::to_string(options_.run_id.value));
  if (sqlite3_step(statement->get()) != SQLITE_DONE)
    return SqlError(db_, "finish run manifest");
  {
    std::lock_guard lock(state_mutex_);
    manifest_.clean_stopped_at_utc = clean_time;
  }
  return absl::OkStatus();
}

void SqliteHistoryWriter::Run() {
  uint8_t round_robin = 0;
  for (;;) {
    if (pause_worker_for_testing_.load(std::memory_order_acquire)) {
      std::unique_lock lock(wake_mutex_);
      worker_paused_ = true;
      wake_cv_.notify_all();
      wake_cv_.wait(lock, [this] {
        return !pause_worker_for_testing_.load(std::memory_order_acquire) ||
               stopping_;
      });
      worker_paused_ = false;
    }
    std::vector<HistoryRecord> batch;
    batch.reserve(options_.batch_size);
    for (uint8_t checked = 0; checked < 8 && batch.size() < options_.batch_size;
         ++checked) {
      uint8_t shard = (round_robin + checked) % 8;
      HistoryRecord record;
      while (batch.size() < options_.batch_size && Pop(shard, &record)) {
        if (record.shard_sequence > last_observed_seq_[shard] + 1) {
          const uint64_t last_dropped =
              queues_[shard]->last_dropped_seq.load(std::memory_order_acquire);
          AddGap(HistoryGap{options_.run_id, ShardId{shard},
                            last_observed_seq_[shard] + 1,
                            record.shard_sequence - 1,
                            last_dropped >= record.shard_sequence - 1
                                ? ErrorCode::kStorageQueueFull
                                : ErrorCode::kInternal});
        }
        last_observed_seq_[shard] =
            std::max(last_observed_seq_[shard], record.shard_sequence);
        if (const auto* reported_gap = std::get_if<HistoryGap>(&record.payload))
          AddGap(*reported_gap);
        batch.push_back(std::move(record));
      }
    }
    round_robin = (round_robin + 1) % 8;
    for (uint8_t shard = 0; shard < 8; ++shard) {
      Queue& queue = *queues_[shard];
      if (queue.head.load(std::memory_order_acquire) !=
          queue.tail.load(std::memory_order_acquire))
        continue;
      const uint64_t dropped =
          queue.last_dropped_seq.load(std::memory_order_acquire);
      if (dropped > last_observed_seq_[shard]) {
        AddGap(HistoryGap{options_.run_id, ShardId{shard},
                          last_observed_seq_[shard] + 1, dropped,
                          ErrorCode::kStorageQueueFull});
        last_observed_seq_[shard] = dropped;
      }
      queue.last_accounted_dropped_seq.store(dropped,
                                             std::memory_order_release);
    }
    {
      std::lock_guard lock(wake_mutex_);
      processing_ = !batch.empty() || !pending_gaps_.empty();
    }
    if (!batch.empty()) {
      auto status = WriteBatch(batch);
      if (!status.ok()) {
        SetError(status);
        {
          std::lock_guard lock(state_mutex_);
          health_.dropped_count += batch.size();
        }
        for (const auto& record : batch) {
          AddGap(HistoryGap{options_.run_id, record.shard,
                            record.shard_sequence, record.shard_sequence,
                            ErrorCode::kStorageWriteFailed});
        }
      }
      pending_records_.fetch_sub(batch.size(), std::memory_order_release);
    }
    auto gap_status = PersistPendingGaps();
    if (!gap_status.ok()) SetError(gap_status);
    {
      std::lock_guard lock(wake_mutex_);
      processing_ = false;
    }
    wake_cv_.notify_all();
    std::unique_lock lock(wake_mutex_);
    if (stopping_ && pending_records_.load(std::memory_order_acquire) == 0) {
      if (stop_time_) {
        lock.unlock();
        auto status =
            gap_status.ok() ? FinishManifest(*stop_time_) : gap_status;
        lock.lock();
        final_status_ = status;
      }
      break;
    }
    if (pending_records_.load(std::memory_order_acquire) == 0)
      wake_cv_.wait_for(lock, std::chrono::milliseconds(10));
  }
  wake_cv_.notify_all();
}

absl::Status SqliteHistoryWriter::Flush() {
  std::unique_lock lock(wake_mutex_);
  wake_cv_.notify_one();
  wake_cv_.wait(lock, [this] {
    if (pending_records_.load(std::memory_order_acquire) != 0 || processing_)
      return false;
    for (const auto& queue : queues_) {
      if (queue->last_accounted_dropped_seq.load(std::memory_order_acquire) <
          queue->last_dropped_seq.load(std::memory_order_acquire))
        return false;
    }
    return true;
  });
  std::lock_guard state_lock(state_mutex_);
  if (!health_.last_error.empty())
    return Error(ErrorCode::kStorageWriteFailed, health_.last_error);
  return absl::OkStatus();
}

absl::Status SqliteHistoryWriter::Stop(UtcTime clean_stopped_at_utc) {
  {
    std::lock_guard lock(wake_mutex_);
    if (!worker_.joinable()) return final_status_;
    stop_time_ = clean_stopped_at_utc;
    stopping_ = true;
    pause_worker_for_testing_.store(false, std::memory_order_release);
  }
  wake_cv_.notify_one();
  worker_.join();
  return final_status_;
}

StorageHealth SqliteHistoryWriter::Health() const {
  std::lock_guard lock(state_mutex_);
  StorageHealth copy = health_;
  for (const auto& queue : queues_)
    copy.dropped_count += queue->dropped_count.load(std::memory_order_acquire);
  copy.queue_watermark = queue_watermark_.load(std::memory_order_acquire);
  return copy;
}

RunManifest SqliteHistoryWriter::Manifest() const {
  std::lock_guard lock(state_mutex_);
  return manifest_;
}

void SqliteHistoryWriter::SetWriteFailureForTesting(bool enabled) {
  fail_writes_for_testing_.store(enabled, std::memory_order_release);
}

void SqliteHistoryWriter::PauseWorkerForTesting(bool paused) {
  pause_worker_for_testing_.store(paused, std::memory_order_release);
  wake_cv_.notify_one();
  if (paused) {
    std::unique_lock lock(wake_mutex_);
    wake_cv_.wait(lock, [this] { return worker_paused_; });
  }
}

}  // namespace hquant
