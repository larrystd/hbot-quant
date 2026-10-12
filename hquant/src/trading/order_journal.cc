#include "hquant/trading/order_journal.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <limits>
#include <string_view>
#include <utility>

#include "boost/asio/post.hpp"
#include "sqlite3.h"

namespace hquant::v1 {
namespace {

constexpr size_t kMaxJournalBatch = 256;

absl::Status Error(sqlite3* db, std::string_view action) {
  return absl::InternalError(std::string(action) + ": " + sqlite3_errmsg(db));
}

absl::Status Exec(sqlite3* db, const char* sql) {
  if (sqlite3_exec(db, sql, nullptr, nullptr, nullptr) == SQLITE_OK) {
    return absl::OkStatus();
  }
  return Error(db, "order journal SQL");
}

void PutUint(std::string& result, uint64_t value) {
  for (unsigned byte = 0; byte < 8; ++byte) {
    result.push_back(static_cast<char>(value & 0xff));
    value >>= 8;
  }
}

bool GetUint(std::string_view& input, uint64_t& value) {
  if (input.size() < 8) return false;
  value = 0;
  for (unsigned byte = 0; byte < 8; ++byte) {
    value |= static_cast<uint64_t>(static_cast<unsigned char>(input[byte]))
             << (8 * byte);
  }
  input.remove_prefix(8);
  return true;
}

void PutString(std::string& result, const std::string& value) {
  PutUint(result, value.size());
  result += value;
}

bool GetString(std::string_view& input, std::string& value) {
  uint64_t size = 0;
  if (!GetUint(input, size) || size > input.size()) return false;
  value.assign(input.data(), static_cast<size_t>(size));
  input.remove_prefix(static_cast<size_t>(size));
  return true;
}

std::string Encode(const JournalEntry& entry) {
  std::string result;
  result.reserve(200);
  result.push_back(1);  // Binary format version.
  for (uint64_t value : std::array<uint64_t, 19>{
           entry.local_id, entry.shard, entry.side, entry.price, entry.quantity,
           entry.remaining, entry.exchange_id, entry.trade_id, entry.gross,
           entry.fee, entry.total, entry.available, entry.locked, entry.amount,
           entry.source_bench_seq, entry.transaction_ms, entry.result,
           entry.credit ? 1u : 0u, entry.external_lock ? 1u : 0u}) {
    PutUint(result, value);
  }
  PutString(result, entry.client_id);
  PutString(result, entry.symbol);
  PutString(result, entry.asset);
  PutString(result, entry.fee_asset);
  PutString(result, entry.message);
  return result;
}

absl::StatusOr<JournalEntry> Decode(JournalKind kind, uint64_t sequence,
                                    std::string_view input) {
  if (input.empty() || input.front() != 1) {
    return absl::DataLossError("unknown order journal record format");
  }
  input.remove_prefix(1);
  JournalEntry entry;
  entry.kind = kind;
  entry.sequence = sequence;
  std::array<uint64_t*, 17> fields{&entry.local_id,
                                   &entry.shard,
                                   &entry.side,
                                   &entry.price,
                                   &entry.quantity,
                                   &entry.remaining,
                                   &entry.exchange_id,
                                   &entry.trade_id,
                                   &entry.gross,
                                   &entry.fee,
                                   &entry.total,
                                   &entry.available,
                                   &entry.locked,
                                   &entry.amount,
                                   &entry.source_bench_seq,
                                   &entry.transaction_ms,
                                   &entry.result};
  for (uint64_t* field : fields) {
    if (!GetUint(input, *field)) {
      return absl::DataLossError("truncated order journal record");
    }
  }
  uint64_t credit = 0, external_lock = 0;
  if (!GetUint(input, credit) || !GetUint(input, external_lock) || credit > 1 ||
      external_lock > 1 || !GetString(input, entry.client_id) ||
      !GetString(input, entry.symbol) || !GetString(input, entry.asset) ||
      !GetString(input, entry.fee_asset) || !GetString(input, entry.message) ||
      !input.empty()) {
    return absl::DataLossError("invalid order journal record");
  }
  entry.credit = credit != 0;
  entry.external_lock = external_lock != 0;
  return entry;
}

}  // namespace

OrderJournal::OrderJournal(std::string path, size_t capacity)
    : path_(std::move(path)), capacity_(capacity) {}

OrderJournal::~OrderJournal() { (void)FlushAndStop(); }

absl::Status OrderJournal::Start() {
  if (started_ || stopping_) {
    return absl::FailedPreconditionError("order journal already started");
  }
  if (capacity_ == 0) {
    return absl::InvalidArgumentError("order journal capacity is zero");
  }
  if (sqlite3_open_v2(
          path_.c_str(), &writer_,
          SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_NOMUTEX,
          nullptr) != SQLITE_OK) {
    absl::Status error = Error(writer_, "open order journal");
    sqlite3_close(writer_);
    writer_ = nullptr;
    return error;
  }
  sqlite3_busy_timeout(writer_, 5000);
  if (absl::Status status =
          Exec(writer_,
               "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;"
               "CREATE TABLE IF NOT EXISTS events("
               "account TEXT NOT NULL, sequence INTEGER NOT NULL,"
               "kind INTEGER NOT NULL, payload BLOB NOT NULL,"
               "PRIMARY KEY(account, sequence));");
      !status.ok()) {
    sqlite3_close(writer_);
    writer_ = nullptr;
    return status;
  }
  if (sqlite3_open_v2(path_.c_str(), &reader_,
                      SQLITE_OPEN_READONLY | SQLITE_OPEN_NOMUTEX,
                      nullptr) != SQLITE_OK) {
    absl::Status error = Error(reader_, "open order journal reader");
    sqlite3_close(reader_);
    reader_ = nullptr;
    sqlite3_close(writer_);
    writer_ = nullptr;
    return error;
  }
  sqlite3_busy_timeout(reader_, 5000);
  started_ = true;
  worker_ = std::thread([this] { WriterLoop(); });
  return absl::OkStatus();
}

absl::StatusOr<std::vector<JournalEntry>> OrderJournal::Load(
    const std::string& account) {
  if (!reader_) return absl::FailedPreconditionError("journal not started");
  sqlite3_stmt* statement = nullptr;
  if (sqlite3_prepare_v2(reader_,
                         "SELECT sequence, kind, payload FROM events "
                         "WHERE account=? ORDER BY sequence",
                         -1, &statement, nullptr) != SQLITE_OK) {
    return Error(reader_, "read order journal");
  }
  sqlite3_bind_text(statement, 1, account.c_str(),
                    static_cast<int>(account.size()), SQLITE_TRANSIENT);
  std::vector<JournalEntry> result;
  int code = SQLITE_OK;
  uint64_t expected = 1;
  while ((code = sqlite3_step(statement)) == SQLITE_ROW) {
    const uint64_t sequence =
        static_cast<uint64_t>(sqlite3_column_int64(statement, 0));
    const int kind = sqlite3_column_int(statement, 1);
    const void* blob = sqlite3_column_blob(statement, 2);
    const int bytes = sqlite3_column_bytes(statement, 2);
    if (sequence != expected++ || kind < 0 ||
        kind > static_cast<int>(JournalKind::CancelWriting) || !blob ||
        bytes <= 0) {
      sqlite3_finalize(statement);
      return absl::DataLossError("order journal sequence or record invalid");
    }
    auto entry = Decode(static_cast<JournalKind>(kind), sequence,
                        std::string_view(static_cast<const char*>(blob),
                                         static_cast<size_t>(bytes)));
    if (!entry.ok()) {
      sqlite3_finalize(statement);
      return entry.status();
    }
    result.push_back(std::move(*entry));
  }
  if (code != SQLITE_DONE) {
    absl::Status error = Error(reader_, "read order journal");
    sqlite3_finalize(statement);
    return error;
  }
  sqlite3_finalize(statement);
  return result;
}

absl::StatusOr<JournalControlPage> OrderJournal::QueryControls(
    const std::string& account, uint32_t limit, uint64_t cursor) {
  if (!reader_) return absl::FailedPreconditionError("journal not started");
  if (limit == 0 || limit > 100 ||
      cursor > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
    return absl::InvalidArgumentError("invalid control audit page");
  }
  sqlite3_stmt* statement = nullptr;
  if (sqlite3_prepare_v2(reader_,
                         "SELECT sequence, payload FROM events "
                         "WHERE account=? AND kind=? AND sequence>? "
                         "ORDER BY sequence LIMIT ?",
                         -1, &statement, nullptr) != SQLITE_OK) {
    return Error(reader_, "query account control audit");
  }
  sqlite3_bind_text(statement, 1, account.c_str(),
                    static_cast<int>(account.size()), SQLITE_TRANSIENT);
  sqlite3_bind_int(statement, 2, static_cast<int>(JournalKind::Control));
  sqlite3_bind_int64(statement, 3, static_cast<sqlite3_int64>(cursor));
  sqlite3_bind_int(statement, 4, static_cast<int>(limit));
  JournalControlPage result;
  result.next_cursor = cursor;
  int code = SQLITE_OK;
  while ((code = sqlite3_step(statement)) == SQLITE_ROW) {
    const uint64_t sequence =
        static_cast<uint64_t>(sqlite3_column_int64(statement, 0));
    const void* blob = sqlite3_column_blob(statement, 1);
    const int bytes = sqlite3_column_bytes(statement, 1);
    if (!blob || bytes <= 0 || sequence <= result.next_cursor) {
      sqlite3_finalize(statement);
      return absl::DataLossError("invalid account control audit record");
    }
    auto entry = Decode(JournalKind::Control, sequence,
                        std::string_view(static_cast<const char*>(blob),
                                         static_cast<size_t>(bytes)));
    if (!entry.ok()) {
      sqlite3_finalize(statement);
      return entry.status();
    }
    result.next_cursor = sequence;
    result.records.push_back(std::move(*entry));
  }
  if (code != SQLITE_DONE) {
    absl::Status error = Error(reader_, "query account control audit");
    sqlite3_finalize(statement);
    return error;
  }
  sqlite3_finalize(statement);
  return result;
}

bool OrderJournal::Append(const std::string& account, JournalEntry entry,
                          boost::asio::io_context& callback_io,
                          Completion done) {
  {
    std::lock_guard lock(mutex_);
    if (!started_ || stopping_ || !error_.ok() || queue_.size() >= capacity_) {
      return false;
    }
    queue_.push_back(
        Pending{account, std::move(entry), &callback_io, std::move(done)});
  }
  cv_.notify_one();
  return true;
}

absl::Status OrderJournal::WriteBatch(std::vector<Pending>& batch) {
  // 整批一笔事务，只有 COMMIT 成功才可回报成功。
  if (absl::Status status = Exec(writer_, "BEGIN IMMEDIATE"); !status.ok()) {
    return status;
  }
  sqlite3_stmt* statement = nullptr;
  if (sqlite3_prepare_v2(writer_,
                         "INSERT INTO events(account, sequence, kind, payload)"
                         " VALUES(?,?,?,?)",
                         -1, &statement, nullptr) != SQLITE_OK) {
    (void)Exec(writer_, "ROLLBACK");
    return Error(writer_, "prepare order journal insert");
  }
  for (const Pending& item : batch) {
    if (item.entry.sequence == 0 ||
        item.entry.sequence >
            static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
      sqlite3_finalize(statement);
      (void)Exec(writer_, "ROLLBACK");
      return absl::InvalidArgumentError("order journal sequence invalid");
    }
    const std::string payload = Encode(item.entry);
    sqlite3_bind_text(statement, 1, item.account.c_str(),
                      static_cast<int>(item.account.size()), SQLITE_TRANSIENT);
    sqlite3_bind_int64(statement, 2,
                       static_cast<sqlite3_int64>(item.entry.sequence));
    sqlite3_bind_int(statement, 3, static_cast<int>(item.entry.kind));
    sqlite3_bind_blob(statement, 4, payload.data(),
                      static_cast<int>(payload.size()), SQLITE_TRANSIENT);
    if (sqlite3_step(statement) != SQLITE_DONE) {
      absl::Status error = Error(writer_, "write order journal");
      sqlite3_finalize(statement);
      (void)Exec(writer_, "ROLLBACK");
      return error;
    }
    sqlite3_reset(statement);
    sqlite3_clear_bindings(statement);
  }
  sqlite3_finalize(statement);
  return Exec(writer_, "COMMIT");
}

void OrderJournal::WriterLoop() {
  for (;;) {
    std::vector<Pending> batch;
    {
      std::unique_lock lock(mutex_);
      cv_.wait(lock, [this] { return stopping_ || !queue_.empty(); });
      // 停机后不再等凑批，排空队列即退出。
      if (queue_.empty()) break;
      // 至多等待 250 微秒，或凑满一批后立即写入。
      if (!stopping_ && queue_.size() < kMaxJournalBatch) {
        cv_.wait_for(lock, std::chrono::microseconds(250), [this] {
          return stopping_ || queue_.size() >= kMaxJournalBatch;
        });
      }
      const size_t count = std::min(kMaxJournalBatch, queue_.size());
      batch.reserve(count);
      for (size_t index = 0; index < count; ++index) {
        batch.push_back(std::move(queue_.front()));
        queue_.pop_front();
      }
    }
    // 锁外写盘；失败后不再尝试写入，逐批回报同一个错误。
    absl::Status status = error_.ok() ? WriteBatch(batch) : error_;
    if (!status.ok()) {
      std::lock_guard lock(mutex_);
      error_ = status;
    }
    // 回调回到账户 io；入队和写入完成不是同一时刻。
    for (Pending& item : batch) {
      boost::asio::post(
          *item.callback_io,
          [done = std::move(item.done), status]() mutable { done(status); });
    }
  }
}

absl::Status OrderJournal::FlushAndStop() {
  {
    std::lock_guard lock(mutex_);
    if (!started_) return error_;
    stopping_ = true;
  }
  cv_.notify_one();
  if (worker_.joinable()) worker_.join();
  if (reader_) sqlite3_close(reader_);
  if (writer_) sqlite3_close(writer_);
  reader_ = nullptr;
  writer_ = nullptr;
  started_ = false;
  return error_;
}

}  // namespace hquant::v1
