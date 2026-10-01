#include "storage/history.h"

#include <algorithm>
#include <array>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <limits>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include "absl/status/status.h"
#include "base/error.h"
#include "boost/asio/post.hpp"
#include "sqlite3.h"
#include "storage/record_codec.h"

namespace hquant {
namespace {

using HistoryStatement =
    std::unique_ptr<sqlite3_stmt, decltype(&sqlite3_finalize)>;

ErrorCode LegacyGapReason(int reason) {
  switch (reason) {
    case 0:
      return ErrorCode::kStorageQueueFull;
    case 1:
      return ErrorCode::kStorageWriteFailed;
    case 2:
      return ErrorCode::kRecoveryDataCorrupted;
    default:
      return ErrorCode::kInternal;
  }
}

bool ValidGapReason(int reason, int version) {
  if (version == 1) return reason >= 0 && reason <= 3;
  return reason > 0 &&
         ErrorFromStoredNumber(static_cast<uint64_t>(reason)).has_value();
}

absl::StatusOr<int> StorageVersion(sqlite3* db) {
  sqlite3_stmt* raw = nullptr;
  if (sqlite3_prepare_v2(db, "SELECT MAX(version) FROM schema_migrations", -1,
                         &raw, nullptr) != SQLITE_OK)
    return Error(ErrorCode::kStorageQueryFailed, sqlite3_errmsg(db));
  HistoryStatement statement(raw, &sqlite3_finalize);
  if (sqlite3_step(statement.get()) != SQLITE_ROW)
    return Error(ErrorCode::kStorageQueryFailed, sqlite3_errmsg(db));
  const int version = sqlite3_column_type(statement.get(), 0) == SQLITE_NULL
                          ? 0
                          : sqlite3_column_int(statement.get(), 0);
  if (version != 1 && version != 2)
    return Error(ErrorCode::kStorageConfigInvalid,
                 "unsupported storage schema version");
  return version;
}
absl::StatusOr<HistoryStatement> PrepareHistory(sqlite3* db,
                                                const std::string& sql) {
  sqlite3_stmt* value = nullptr;
  if (sqlite3_prepare_v2(db, sql.c_str(), -1, &value, nullptr) != SQLITE_OK)
    return Error(ErrorCode::kStorageQueryFailed, sqlite3_errmsg(db));
  return HistoryStatement(value, &sqlite3_finalize);
}
void Text(sqlite3_stmt* statement, int index, const std::string& value) {
  sqlite3_bind_text(statement, index, value.data(),
                    static_cast<int>(value.size()), SQLITE_TRANSIENT);
}
absl::StatusOr<uint64_t> ParseUnsigned(std::string_view value) {
  if (value.empty())
    return Error(ErrorCode::kHistoryCursorInvalid, "empty history cursor");
  uint64_t parsed = 0;
  auto [end, error] =
      std::from_chars(value.data(), value.data() + value.size(), parsed);
  if (error != std::errc{} || end != value.data() + value.size() ||
      parsed > static_cast<uint64_t>(std::numeric_limits<sqlite3_int64>::max()))
    return Error(ErrorCode::kHistoryCursorInvalid, "invalid history cursor");
  return parsed;
}
int Progress(void* context) {
  auto* deadline = static_cast<MonoTime*>(context);
  return std::chrono::steady_clock::now() >= *deadline;
}
class ProgressGuard {
 public:
  ProgressGuard(sqlite3* db, MonoTime* deadline) : db_(db) {
    sqlite3_progress_handler(db_, 1000, Progress, deadline);
  }
  ~ProgressGuard() { sqlite3_progress_handler(db_, 0, nullptr, nullptr); }

 private:
  sqlite3* db_;
};

}  // namespace

SqliteHistoryReader::SqliteHistoryReader(Options options, sqlite3* db,
                                         int storage_version)
    : options_(std::move(options)),
      db_(db),
      storage_version_(storage_version) {}

absl::StatusOr<std::unique_ptr<SqliteHistoryReader>> SqliteHistoryReader::Open(
    Options options) {
  if (options.path.empty() || options.queue_capacity == 0 ||
      options.max_page_size == 0 || options.max_page_size > 500)
    return Error(ErrorCode::kStorageConfigInvalid,
                 "invalid history reader options");
  sqlite3* db = nullptr;
  if (sqlite3_open_v2(options.path.c_str(), &db,
                      SQLITE_OPEN_READONLY | SQLITE_OPEN_NOMUTEX,
                      nullptr) != SQLITE_OK) {
    absl::Status error =
        Error(ErrorCode::kStorageOpenFailed,
              db ? sqlite3_errmsg(db) : "open history database");
    sqlite3_close(db);
    return error;
  }
  sqlite3_busy_timeout(db, 250);
  auto storage_version = StorageVersion(db);
  if (!storage_version.ok()) {
    sqlite3_close(db);
    return storage_version.status();
  }
  auto reader = std::unique_ptr<SqliteHistoryReader>(
      new SqliteHistoryReader(std::move(options), db, *storage_version));
  reader->worker_ = std::thread([self = reader.get()] { self->Run(); });
  return reader;
}

SqliteHistoryReader::~SqliteHistoryReader() {
  {
    std::lock_guard lock(mutex_);
    stopping_ = true;
  }
  sqlite3_interrupt(db_);
  cv_.notify_one();
  if (worker_.joinable()) worker_.join();
  sqlite3_close(db_);
}

absl::Status SqliteHistoryReader::TrySubmit(HistoryQuery query) {
  if (query.page_size == 0 || query.page_size > options_.max_page_size)
    return Error(ErrorCode::kStorageConfigInvalid,
                 "history page size outside configured bound");
  if (query.cursor && !ParseUnsigned(*query.cursor).ok())
    return Error(ErrorCode::kHistoryCursorInvalid, "invalid history cursor");
  std::lock_guard lock(mutex_);
  if (stopping_)
    return Error(ErrorCode::kHistoryReaderStopping,
                 "history reader is stopping");
  if (outstanding_ == options_.queue_capacity)
    return Error(ErrorCode::kHistoryQueueFull, "history query queue is full");
  pending_.push_back({std::move(query), {}, {}});
  ++outstanding_;
  cv_.notify_one();
  return absl::OkStatus();
}

absl::Status SqliteHistoryReader::TrySubmitAsync(
    HistoryQuery query, boost::asio::any_io_executor executor,
    std::function<void(HistoryPage)> completion) {
  if (!completion || query.page_size == 0 ||
      query.page_size > options_.max_page_size)
    return Error(ErrorCode::kStorageConfigInvalid,
                 "history page size outside configured bound");
  if (query.cursor && !ParseUnsigned(*query.cursor).ok())
    return Error(ErrorCode::kHistoryCursorInvalid, "invalid history cursor");
  std::lock_guard lock(mutex_);
  if (stopping_)
    return Error(ErrorCode::kHistoryReaderStopping,
                 "history reader is stopping");
  if (outstanding_ == options_.queue_capacity)
    return Error(ErrorCode::kHistoryQueueFull, "history query queue is full");
  pending_.push_back(
      {std::move(query), std::move(executor), std::move(completion)});
  ++outstanding_;
  cv_.notify_one();
  return absl::OkStatus();
}

std::optional<HistoryPage> SqliteHistoryReader::TryReceive() {
  std::lock_guard lock(mutex_);
  if (results_.empty()) return std::nullopt;
  HistoryPage result = std::move(results_.front());
  results_.pop_front();
  --outstanding_;
  return result;
}

void SqliteHistoryReader::Run() {
  for (;;) {
    PendingQuery pending;
    {
      std::unique_lock lock(mutex_);
      cv_.wait(lock, [this] { return stopping_ || !pending_.empty(); });
      if (stopping_) break;
      pending = std::move(pending_.front());
      pending_.pop_front();
    }
    HistoryPage result = Query(pending.query);
    if (pending.completion) {
      {
        std::lock_guard lock(mutex_);
        --outstanding_;
      }
      boost::asio::post(
          pending.executor,
          [completion = std::move(pending.completion),
           page = std::move(result)]() mutable { completion(std::move(page)); });
      continue;
    }
    {
      std::lock_guard lock(mutex_);
      results_.push_back(std::move(result));
    }
  }
}

HistoryPage SqliteHistoryReader::Query(const HistoryQuery& query) {
  HistoryPage page;
  page.request_id = query.request_id;
  const auto now = std::chrono::time_point_cast<std::chrono::microseconds>(
      std::chrono::steady_clock::now());
  MonoTime deadline =
      query.deadline == MonoTime{}
          ? now + std::chrono::seconds(1)
          : std::min(query.deadline, now + std::chrono::seconds(1));
  if (deadline <= now) {
    page.status = Error(ErrorCode::kHistoryQueryTimeout,
                        "history query deadline elapsed");
    return page;
  }
  ProgressGuard progress(db_, &deadline);
  uint64_t cursor = 0;
  if (query.cursor) {
    auto parsed = ParseUnsigned(*query.cursor);
    if (!parsed.ok()) {
      page.status = parsed.status();
      return page;
    }
    cursor = *parsed;
  }
  std::string sql =
      "SELECT id,schema_version,record_blob FROM history_records WHERE id>?1";
  int next = 2;
  if (query.account) sql += " AND account=?" + std::to_string(next++);
  if (query.strategy_id) sql += " AND owner_key=?" + std::to_string(next++);
  if (query.market) {
    sql += " AND market_venue=?" + std::to_string(next++);
    sql += " AND market_kind=?" + std::to_string(next++);
    sql += " AND market_symbol=?" + std::to_string(next++);
  }
  if (query.from_utc) sql += " AND received_at_us>=?" + std::to_string(next++);
  if (query.through_utc)
    sql += " AND received_at_us<=?" + std::to_string(next++);
  sql += " ORDER BY id LIMIT ?" + std::to_string(next);
  auto statement = PrepareHistory(db_, sql);
  if (!statement.ok()) {
    page.status = statement.status();
    return page;
  }
  sqlite3_stmt* s = statement->get();
  sqlite3_bind_int64(s, 1, static_cast<sqlite3_int64>(cursor));
  int bind = 2;
  if (query.account) Text(s, bind++, query.account->value);
  if (query.strategy_id)
    sqlite3_bind_int64(s, bind++,
                       static_cast<sqlite3_int64>(query.strategy_id->value));
  if (query.market) {
    Text(s, bind++, query.market->exchange.value);
    sqlite3_bind_int(s, bind++,
                     static_cast<int>(query.market->instrument_kind));
    Text(s, bind++, query.market->native_symbol);
  }
  if (query.from_utc)
    sqlite3_bind_int64(s, bind++, query.from_utc->time_since_epoch().count());
  if (query.through_utc)
    sqlite3_bind_int64(s, bind++,
                       query.through_utc->time_since_epoch().count());
  sqlite3_bind_int(s, bind, static_cast<int>(query.page_size + 1));
  uint64_t last_id = cursor;
  bool has_more = false;
  for (;;) {
    const int rc = sqlite3_step(s);
    if (rc == SQLITE_DONE) break;
    if (rc != SQLITE_ROW) {
      page.status =
          rc == SQLITE_INTERRUPT
              ? Error(ErrorCode::kHistoryQueryTimeout,
                      "history query interrupted")
              : Error(ErrorCode::kStorageQueryFailed, sqlite3_errmsg(db_));
      return page;
    }
    if (page.rows.size() == query.page_size) {
      has_more = true;
      break;
    }
    last_id = static_cast<uint64_t>(sqlite3_column_int64(s, 0));
    const int indexed_version = sqlite3_column_int(s, 1);
    const auto* data = static_cast<const char*>(sqlite3_column_blob(s, 2));
    const int size = sqlite3_column_bytes(s, 2);
    auto decoded = storage_internal::DecodeRecord(std::string_view(data, size));
    if (!decoded.ok()) {
      page.status = decoded.status();
      page.rows.clear();
      return page;
    }
    if (decoded->schema_version != indexed_version) {
      page.status = Error(ErrorCode::kRecoveryDataCorrupted,
                          "record schema version does not match index");
      page.rows.clear();
      return page;
    }
    page.rows.push_back(std::move(*decoded));
  }
  statement->reset();
  if (has_more) page.next_cursor = std::to_string(last_id);

  auto gaps =
      PrepareHistory(db_,
                     "SELECT run_id,shard,first_seq,last_seq,reason FROM "
                     "history_gaps ORDER BY id LIMIT 501");
  if (!gaps.ok()) {
    page.status = gaps.status();
    page.rows.clear();
    return page;
  }
  for (;;) {
    const int rc = sqlite3_step(gaps->get());
    if (rc == SQLITE_DONE) break;
    if (rc != SQLITE_ROW) {
      page.status =
          rc == SQLITE_INTERRUPT
              ? Error(ErrorCode::kHistoryQueryTimeout, "gap query interrupted")
              : Error(ErrorCode::kStorageQueryFailed, sqlite3_errmsg(db_));
      page.rows.clear();
      return page;
    }
    if (page.incomplete_ranges.size() == 500) {
      page.status = Error(ErrorCode::kHistoryTooManyGaps,
                          "too many history gaps to report");
      page.rows.clear();
      return page;
    }
    const auto* raw_id =
        reinterpret_cast<const char*>(sqlite3_column_text(gaps->get(), 0));
    auto run_id =
        ParseUnsigned(raw_id ? std::string_view(raw_id) : std::string_view{});
    if (!run_id.ok()) {
      page.status = run_id.status();
      page.rows.clear();
      return page;
    }
    HistoryGap gap;
    gap.run_id.value = *run_id;
    gap.shard.value = static_cast<uint8_t>(sqlite3_column_int(gaps->get(), 1));
    gap.first_seq = static_cast<uint64_t>(sqlite3_column_int64(gaps->get(), 2));
    gap.last_seq = static_cast<uint64_t>(sqlite3_column_int64(gaps->get(), 3));
    const int reason = sqlite3_column_int(gaps->get(), 4);
    if (!ValidGapReason(reason, storage_version_)) {
      page.status = Error(ErrorCode::kRecoveryDataCorrupted,
                          "invalid persisted history gap reason");
      page.rows.clear();
      return page;
    }
    gap.reason = storage_version_ == 1 ? LegacyGapReason(reason)
                                       : *ErrorFromStoredNumber(reason);
    page.incomplete_ranges.push_back(gap);
  }
  return page;
}

}  // namespace hquant

namespace hquant {
namespace {

using Database = std::unique_ptr<sqlite3, decltype(&sqlite3_close)>;
using RecoveryStatement =
    std::unique_ptr<sqlite3_stmt, decltype(&sqlite3_finalize)>;

absl::Status SqlError(sqlite3* db, std::string_view operation,
                      ErrorCode code = ErrorCode::kStorageQueryFailed) {
  return Error(code, std::string(operation) + ": " +
                         (db ? sqlite3_errmsg(db) : "no database"));
}

absl::StatusOr<RecoveryStatement> PrepareRecovery(sqlite3* db, const char* sql,
                                                  RunId run_id) {
  sqlite3_stmt* raw = nullptr;
  if (sqlite3_prepare_v2(db, sql, -1, &raw, nullptr) != SQLITE_OK) {
    return SqlError(db, "prepare recovery query");
  }
  RecoveryStatement statement(raw, &sqlite3_finalize);
  const std::string id = std::to_string(run_id.value);
  sqlite3_bind_text(statement.get(), 1, id.data(), static_cast<int>(id.size()),
                    SQLITE_TRANSIENT);
  return statement;
}

bool Terminal(ExchangeOrderStatus status) {
  return status == ExchangeOrderStatus::Traded ||
         status == ExchangeOrderStatus::Canceled ||
         status == ExchangeOrderStatus::Rejected ||
         status == ExchangeOrderStatus::Expired;
}

void AddUncovered(RecoverySnapshot* snapshot, uint8_t shard, uint64_t first,
                  uint64_t last) {
  if (first > last) return;
  uint64_t cursor = first;
  std::vector<HistoryGap> known;
  for (const auto& gap : snapshot->gaps) {
    if (gap.shard.value == shard && gap.last_seq >= first &&
        gap.first_seq <= last) {
      known.push_back(gap);
    }
  }
  std::sort(known.begin(), known.end(),
            [](const HistoryGap& a, const HistoryGap& b) {
              return a.first_seq < b.first_seq;
            });
  for (const auto& gap : known) {
    if (gap.first_seq > cursor) {
      snapshot->gaps.push_back({snapshot->manifest.run_id, ShardId{shard},
                                cursor, std::min(last, gap.first_seq - 1),
                                ErrorCode::kInternal});
    }
    if (gap.last_seq >= last) return;
    cursor = std::max(cursor, gap.last_seq + 1);
  }
  if (cursor <= last) {
    snapshot->gaps.push_back({snapshot->manifest.run_id, ShardId{shard}, cursor,
                              last, ErrorCode::kInternal});
  }
}

}  // namespace

absl::StatusOr<RecoverySnapshot> LoadRecoverySnapshot(const std::string& path,
                                                      RunId run_id) {
  if (path.empty() || !run_id.IsValid()) {
    return Error(ErrorCode::kStorageConfigInvalid,
                 "invalid recovery path or run ID");
  }
  sqlite3* raw = nullptr;
  if (sqlite3_open_v2(path.c_str(), &raw,
                      SQLITE_OPEN_READONLY | SQLITE_OPEN_NOMUTEX,
                      nullptr) != SQLITE_OK) {
    const auto error =
        SqlError(raw, "open recovery database", ErrorCode::kStorageOpenFailed);
    if (raw) sqlite3_close(raw);
    return error;
  }
  Database db(raw, &sqlite3_close);
  sqlite3_busy_timeout(db.get(), 1000);
  if (sqlite3_exec(db.get(), "BEGIN", nullptr, nullptr, nullptr) != SQLITE_OK) {
    return SqlError(db.get(), "begin recovery snapshot");
  }
  int storage_version = 0;
  {
    sqlite3_stmt* raw_version = nullptr;
    if (sqlite3_prepare_v2(db.get(),
                           "SELECT MAX(version) FROM schema_migrations", -1,
                           &raw_version, nullptr) != SQLITE_OK) {
      return SqlError(db.get(), "read storage schema version");
    }
    RecoveryStatement version(raw_version, &sqlite3_finalize);
    const int rc = sqlite3_step(version.get());
    if (rc != SQLITE_ROW)
      return SqlError(db.get(), "read storage schema version");
    if (sqlite3_column_type(version.get(), 0) == SQLITE_NULL ||
        (sqlite3_column_int(version.get(), 0) != 1 &&
         sqlite3_column_int(version.get(), 0) != 2)) {
      return Error(ErrorCode::kStorageConfigInvalid,
                   "unsupported storage schema version");
    }
    storage_version = sqlite3_column_int(version.get(), 0);
  }

  RecoverySnapshot snapshot;
  snapshot.manifest.run_id = run_id;
  snapshot.context.run_id = run_id;
  {
    auto statement = PrepareRecovery(db.get(), R"sql(
      SELECT started_at_us,clean_stopped_at_us,history_complete
      FROM run_manifest WHERE run_id=?1)sql",
                                     run_id);
    if (!statement.ok()) return statement.status();
    const int rc = sqlite3_step(statement->get());
    if (rc == SQLITE_DONE)
      return Error(ErrorCode::kRecoveryManifestNotFound,
                   "run manifest not found");
    if (rc != SQLITE_ROW) return SqlError(db.get(), "read run manifest");
    snapshot.manifest.started_at_utc = UtcTime{
        std::chrono::microseconds{sqlite3_column_int64(statement->get(), 0)}};
    if (sqlite3_column_type(statement->get(), 1) != SQLITE_NULL) {
      snapshot.manifest.clean_stopped_at_utc = UtcTime{
          std::chrono::microseconds{sqlite3_column_int64(statement->get(), 1)}};
    }
    const int complete = sqlite3_column_int(statement->get(), 2);
    if (complete != 0 && complete != 1) {
      return Error(ErrorCode::kRecoveryDataCorrupted,
                   "invalid manifest completeness flag");
    }
    snapshot.manifest.history_complete = complete == 1;
  }
  {
    auto statement = PrepareRecovery(db.get(), R"sql(
      SELECT shard,last_seq FROM committed_sequence WHERE run_id=?1)sql",
                                     run_id);
    if (!statement.ok()) return statement.status();
    for (;;) {
      const int rc = sqlite3_step(statement->get());
      if (rc == SQLITE_DONE) break;
      if (rc != SQLITE_ROW)
        return SqlError(db.get(), "read committed sequence");
      const int shard = sqlite3_column_int(statement->get(), 0);
      const sqlite3_int64 sequence = sqlite3_column_int64(statement->get(), 1);
      if (shard < 0 || shard >= 8 || sequence < 1) {
        return Error(ErrorCode::kRecoveryDataCorrupted,
                     "invalid committed sequence");
      }
      snapshot.manifest.last_committed_seq_by_shard[shard] =
          static_cast<uint64_t>(sequence);
    }
  }
  {
    auto statement = PrepareRecovery(db.get(), R"sql(
      SELECT shard,first_seq,last_seq,reason FROM history_gaps
      WHERE run_id=?1 ORDER BY shard,first_seq)sql",
                                     run_id);
    if (!statement.ok()) return statement.status();
    for (;;) {
      const int rc = sqlite3_step(statement->get());
      if (rc == SQLITE_DONE) break;
      if (rc != SQLITE_ROW) return SqlError(db.get(), "read history gap");
      const int shard = sqlite3_column_int(statement->get(), 0);
      const sqlite3_int64 first = sqlite3_column_int64(statement->get(), 1);
      const sqlite3_int64 last = sqlite3_column_int64(statement->get(), 2);
      const int reason = sqlite3_column_int(statement->get(), 3);
      if (shard < 0 || shard >= 8 || first < 1 || last < first ||
          !ValidGapReason(reason, storage_version)) {
        return Error(ErrorCode::kRecoveryDataCorrupted,
                     "invalid persisted history gap");
      }
      snapshot.gaps.push_back(
          {run_id, ShardId{static_cast<uint8_t>(shard)},
           static_cast<uint64_t>(first), static_cast<uint64_t>(last),
           storage_version == 1 ? LegacyGapReason(reason)
                                : *ErrorFromStoredNumber(reason)});
    }
  }

  std::array<uint64_t, 8> last_seen{};
  std::map<std::string, bool> terminal_by_client;
  {
    auto statement = PrepareRecovery(db.get(), R"sql(
      SELECT shard,shard_sequence,schema_version,record_blob FROM history_records
      WHERE run_id=?1 ORDER BY id)sql",
                                     run_id);
    if (!statement.ok()) return statement.status();
    for (;;) {
      const int rc = sqlite3_step(statement->get());
      if (rc == SQLITE_DONE) break;
      if (rc != SQLITE_ROW) return SqlError(db.get(), "read recovery record");
      const int shard = sqlite3_column_int(statement->get(), 0);
      const sqlite3_int64 sequence = sqlite3_column_int64(statement->get(), 1);
      const int indexed_version = sqlite3_column_int(statement->get(), 2);
      const char* blob =
          static_cast<const char*>(sqlite3_column_blob(statement->get(), 3));
      const int size = sqlite3_column_bytes(statement->get(), 3);
      if (shard < 0 || shard >= 8 || sequence < 1 || !blob || size < 1) {
        return Error(ErrorCode::kRecoveryDataCorrupted,
                     "invalid recovery record index or blob");
      }
      auto decoded = storage_internal::DecodeRecord(
          std::string_view(blob, static_cast<size_t>(size)));
      if (!decoded.ok())
        return Error(ErrorCode::kRecoveryDataCorrupted,
                     decoded.status().message());
      if (decoded->run_id != run_id || decoded->shard.value != shard ||
          decoded->shard_sequence != static_cast<uint64_t>(sequence) ||
          (decoded->schema_version != 1 && decoded->schema_version != 2) ||
          decoded->schema_version != indexed_version ||
          static_cast<uint64_t>(sequence) <= last_seen[shard]) {
        return Error(ErrorCode::kRecoveryDataCorrupted,
                     "recovery record does not match index");
      }
      if (static_cast<uint64_t>(sequence) > last_seen[shard] + 1) {
        AddUncovered(&snapshot, shard, last_seen[shard] + 1,
                     static_cast<uint64_t>(sequence) - 1);
      }
      last_seen[shard] = static_cast<uint64_t>(sequence);
      if (const auto* prepared =
              std::get_if<PreparedOrder>(&decoded->payload)) {
        if (prepared->client_id.value.empty()) {
          return Error(ErrorCode::kRecoveryDataCorrupted,
                       "empty client ID in recovered prepared order");
        }
        snapshot.context.recovered_prepared_orders.push_back(*prepared);
        if (prepared->executor_checkpoint) {
          snapshot.context.checkpoints.push_back(RecordedCheckpoint{
              *prepared->executor_checkpoint, prepared->created_at_utc});
        }
      } else if (const auto* checkpoint =
                     std::get_if<RecordedCheckpoint>(&decoded->payload)) {
        snapshot.context.checkpoints.push_back(*checkpoint);
      } else if (const auto* order =
                     std::get_if<OrderUpdate>(&decoded->payload)) {
        snapshot.context.exchange_orders.push_back(*order);
        if (order->client_id) {
          terminal_by_client[order->client_id->value] =
              Terminal(order->exchange_status);
        }
      } else if (const auto* trade =
                     std::get_if<TradeUpdate>(&decoded->payload)) {
        snapshot.context.exchange_trades.push_back(*trade);
      }
    }
  }
  for (const auto& [shard, committed] :
       snapshot.manifest.last_committed_seq_by_shard) {
    if (last_seen[shard] > committed) {
      return Error(ErrorCode::kRecoveryDataCorrupted,
                   "record exceeds committed sequence");
    }
    if (last_seen[shard] < committed) {
      AddUncovered(&snapshot, shard, last_seen[shard] + 1, committed);
    }
  }
  for (uint8_t shard = 0; shard < 8; ++shard) {
    if (last_seen[shard] != 0 &&
        !snapshot.manifest.last_committed_seq_by_shard.contains(shard)) {
      return Error(ErrorCode::kRecoveryDataCorrupted,
                   "record lacks committed sequence");
    }
  }
  snapshot.crash_tail_possible =
      !snapshot.manifest.clean_stopped_at_utc.has_value();
  const bool incomplete = !snapshot.manifest.history_complete ||
                          !snapshot.gaps.empty() ||
                          snapshot.crash_tail_possible;
  for (const auto& prepared : snapshot.context.recovered_prepared_orders) {
    const auto terminal = terminal_by_client.find(prepared.client_id.value);
    if (incomplete || terminal == terminal_by_client.end() ||
        !terminal->second) {
      snapshot.context.unresolved_ids.push_back(prepared.client_id);
    }
  }
  snapshot.context.confidence =
      incomplete ? RecoveryConfidence::Unresolved : RecoveryConfidence::Partial;
  snapshot.needs_reconciliation =
      incomplete || !snapshot.context.recovered_prepared_orders.empty();
  return snapshot;
}

}  // namespace hquant
