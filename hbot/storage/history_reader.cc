#include "hbot/storage/history_reader.h"

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include "absl/status/status.h"
#include "hbot/storage/record_codec.h"
#include "sqlite3.h"

namespace hbot {
namespace {

using Statement = std::unique_ptr<sqlite3_stmt, decltype(&sqlite3_finalize)>;
absl::StatusOr<Statement> Prepare(sqlite3* db, const std::string& sql) {
  sqlite3_stmt* value = nullptr;
  if (sqlite3_prepare_v2(db, sql.c_str(), -1, &value, nullptr) != SQLITE_OK)
    return absl::InternalError(sqlite3_errmsg(db));
  return Statement(value, &sqlite3_finalize);
}
void Text(sqlite3_stmt* statement, int index, const std::string& value) {
  sqlite3_bind_text(statement, index, value.data(),
                    static_cast<int>(value.size()), SQLITE_TRANSIENT);
}
absl::StatusOr<uint64_t> ParseUnsigned(std::string_view value) {
  if (value.empty()) return absl::InvalidArgumentError("empty history cursor");
  uint64_t parsed = 0;
  auto [end, error] =
      std::from_chars(value.data(), value.data() + value.size(), parsed);
  if (error != std::errc{} || end != value.data() + value.size() ||
      parsed > static_cast<uint64_t>(std::numeric_limits<sqlite3_int64>::max()))
    return absl::InvalidArgumentError("invalid history cursor");
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

HistoryReader::HistoryReader(Options options, sqlite3* db)
    : options_(std::move(options)), db_(db) {}

absl::StatusOr<std::unique_ptr<HistoryReader>> HistoryReader::Open(
    Options options) {
  if (options.path.empty() || options.queue_capacity == 0 ||
      options.max_page_size == 0 || options.max_page_size > 500)
    return absl::InvalidArgumentError("invalid history reader options");
  sqlite3* db = nullptr;
  if (sqlite3_open_v2(options.path.c_str(), &db,
                      SQLITE_OPEN_READONLY | SQLITE_OPEN_NOMUTEX,
                      nullptr) != SQLITE_OK) {
    absl::Status error =
        absl::InternalError(db ? sqlite3_errmsg(db) : "open history database");
    sqlite3_close(db);
    return error;
  }
  sqlite3_busy_timeout(db, 250);
  auto reader =
      std::unique_ptr<HistoryReader>(new HistoryReader(std::move(options), db));
  reader->worker_ = std::thread([self = reader.get()] { self->Run(); });
  return reader;
}

HistoryReader::~HistoryReader() {
  {
    std::lock_guard lock(mutex_);
    stopping_ = true;
  }
  sqlite3_interrupt(db_);
  cv_.notify_one();
  if (worker_.joinable()) worker_.join();
  sqlite3_close(db_);
}

absl::Status HistoryReader::TrySubmit(HistoryQuery query) {
  if (query.page_size == 0 || query.page_size > options_.max_page_size)
    return absl::InvalidArgumentError(
        "history page size outside configured bound");
  if (query.cursor && !ParseUnsigned(*query.cursor).ok())
    return absl::InvalidArgumentError("invalid history cursor");
  std::lock_guard lock(mutex_);
  if (stopping_)
    return absl::FailedPreconditionError("history reader is stopping");
  if (outstanding_ == options_.queue_capacity)
    return absl::ResourceExhaustedError("history query queue is full");
  pending_.push_back(std::move(query));
  ++outstanding_;
  cv_.notify_one();
  return absl::OkStatus();
}

std::optional<HistoryPage> HistoryReader::TryReceive() {
  std::lock_guard lock(mutex_);
  if (results_.empty()) return std::nullopt;
  HistoryPage result = std::move(results_.front());
  results_.pop_front();
  --outstanding_;
  return result;
}

void HistoryReader::Run() {
  for (;;) {
    HistoryQuery query;
    {
      std::unique_lock lock(mutex_);
      cv_.wait(lock, [this] { return stopping_ || !pending_.empty(); });
      if (stopping_) break;
      query = std::move(pending_.front());
      pending_.pop_front();
    }
    HistoryPage result = Query(query);
    {
      std::lock_guard lock(mutex_);
      results_.push_back(std::move(result));
    }
  }
}

HistoryPage HistoryReader::Query(const HistoryQuery& query) {
  HistoryPage page;
  page.request_id = query.request_id;
  const auto now = std::chrono::time_point_cast<std::chrono::microseconds>(
      std::chrono::steady_clock::now());
  MonoTime deadline =
      query.deadline == MonoTime{}
          ? now + std::chrono::seconds(1)
          : std::min(query.deadline, now + std::chrono::seconds(1));
  if (deadline <= now) {
    page.status = absl::DeadlineExceededError("history query deadline elapsed");
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
  std::string sql = "SELECT id,record_blob FROM history_records WHERE id>?1";
  int next = 2;
  if (query.account) sql += " AND account=?" + std::to_string(next++);
  if (query.owner) sql += " AND owner_key=?" + std::to_string(next++);
  if (query.market) {
    sql += " AND market_venue=?" + std::to_string(next++);
    sql += " AND market_kind=?" + std::to_string(next++);
    sql += " AND market_symbol=?" + std::to_string(next++);
  }
  if (query.from_utc) sql += " AND received_at_us>=?" + std::to_string(next++);
  if (query.through_utc)
    sql += " AND received_at_us<=?" + std::to_string(next++);
  sql += " ORDER BY id LIMIT ?" + std::to_string(next);
  auto statement = Prepare(db_, sql);
  if (!statement.ok()) {
    page.status = statement.status();
    return page;
  }
  sqlite3_stmt* s = statement->get();
  sqlite3_bind_int64(s, 1, static_cast<sqlite3_int64>(cursor));
  int bind = 2;
  if (query.account) Text(s, bind++, query.account->value);
  if (query.owner)
    sqlite3_bind_int64(s, bind++,
                       static_cast<sqlite3_int64>(query.owner->owner_key));
  if (query.market) {
    Text(s, bind++, query.market->venue.value);
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
              ? absl::DeadlineExceededError("history query interrupted")
              : absl::InternalError(sqlite3_errmsg(db_));
      return page;
    }
    if (page.rows.size() == query.page_size) {
      has_more = true;
      break;
    }
    last_id = static_cast<uint64_t>(sqlite3_column_int64(s, 0));
    const auto* data = static_cast<const char*>(sqlite3_column_blob(s, 1));
    const int size = sqlite3_column_bytes(s, 1);
    auto decoded = storage_internal::DecodeRecord(std::string_view(data, size));
    if (!decoded.ok()) {
      page.status = decoded.status();
      page.rows.clear();
      return page;
    }
    page.rows.push_back(std::move(*decoded));
  }
  statement->reset();
  if (has_more) page.next_cursor = std::to_string(last_id);

  auto gaps = Prepare(db_,
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
      page.status = rc == SQLITE_INTERRUPT
                        ? absl::DeadlineExceededError("gap query interrupted")
                        : absl::InternalError(sqlite3_errmsg(db_));
      page.rows.clear();
      return page;
    }
    if (page.incomplete_ranges.size() == 500) {
      page.status =
          absl::ResourceExhaustedError("too many history gaps to report");
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
    gap.reason = static_cast<GapReason>(sqlite3_column_int(gaps->get(), 4));
    page.incomplete_ranges.push_back(gap);
  }
  return page;
}

}  // namespace hbot
