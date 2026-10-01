#include "hbot/storage/recovery.h"

#include <algorithm>
#include <array>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include "absl/status/status.h"
#include "hbot/storage/record_codec.h"
#include "sqlite3.h"

namespace hbot {
namespace {

using Database = std::unique_ptr<sqlite3, decltype(&sqlite3_close)>;
using Statement = std::unique_ptr<sqlite3_stmt, decltype(&sqlite3_finalize)>;

absl::Status SqlError(sqlite3* db, std::string_view operation) {
  return absl::InternalError(std::string(operation) + ": " +
                             (db ? sqlite3_errmsg(db) : "no database"));
}

absl::StatusOr<Statement> Prepare(sqlite3* db, const char* sql, RunId run_id) {
  sqlite3_stmt* raw = nullptr;
  if (sqlite3_prepare_v2(db, sql, -1, &raw, nullptr) != SQLITE_OK) {
    return SqlError(db, "prepare recovery query");
  }
  Statement statement(raw, &sqlite3_finalize);
  const std::string id = std::to_string(run_id.value);
  sqlite3_bind_text(statement.get(), 1, id.data(), static_cast<int>(id.size()),
                    SQLITE_TRANSIENT);
  return statement;
}

bool Terminal(ExchangeOrderStatus status) {
  return status == ExchangeOrderStatus::Filled ||
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
                                GapReason::Unknown});
    }
    if (gap.last_seq >= last) return;
    cursor = std::max(cursor, gap.last_seq + 1);
  }
  if (cursor <= last) {
    snapshot->gaps.push_back({snapshot->manifest.run_id, ShardId{shard}, cursor,
                              last, GapReason::Unknown});
  }
}

}  // namespace

absl::StatusOr<RecoverySnapshot> LoadRecoverySnapshot(const std::string& path,
                                                      RunId run_id) {
  if (path.empty() || !run_id.IsValid()) {
    return absl::InvalidArgumentError("invalid recovery path or run ID");
  }
  sqlite3* raw = nullptr;
  if (sqlite3_open_v2(path.c_str(), &raw,
                      SQLITE_OPEN_READONLY | SQLITE_OPEN_NOMUTEX,
                      nullptr) != SQLITE_OK) {
    const auto error = SqlError(raw, "open recovery database");
    if (raw) sqlite3_close(raw);
    return error;
  }
  Database db(raw, &sqlite3_close);
  sqlite3_busy_timeout(db.get(), 1000);
  if (sqlite3_exec(db.get(), "BEGIN", nullptr, nullptr, nullptr) != SQLITE_OK) {
    return SqlError(db.get(), "begin recovery snapshot");
  }
  {
    sqlite3_stmt* raw_version = nullptr;
    if (sqlite3_prepare_v2(db.get(),
                           "SELECT MAX(version) FROM schema_migrations", -1,
                           &raw_version, nullptr) != SQLITE_OK) {
      return SqlError(db.get(), "read storage schema version");
    }
    Statement version(raw_version, &sqlite3_finalize);
    const int rc = sqlite3_step(version.get());
    if (rc != SQLITE_ROW)
      return SqlError(db.get(), "read storage schema version");
    if (sqlite3_column_type(version.get(), 0) == SQLITE_NULL ||
        sqlite3_column_int(version.get(), 0) != 1) {
      return absl::FailedPreconditionError(
          "unsupported storage schema version");
    }
  }

  RecoverySnapshot snapshot;
  snapshot.manifest.run_id = run_id;
  snapshot.context.run_id = run_id;
  {
    auto statement = Prepare(db.get(), R"sql(
      SELECT started_at_us,clean_stopped_at_us,history_complete
      FROM run_manifest WHERE run_id=?1)sql",
                             run_id);
    if (!statement.ok()) return statement.status();
    const int rc = sqlite3_step(statement->get());
    if (rc == SQLITE_DONE) return absl::NotFoundError("run manifest not found");
    if (rc != SQLITE_ROW) return SqlError(db.get(), "read run manifest");
    snapshot.manifest.started_at_utc = UtcTime{
        std::chrono::microseconds{sqlite3_column_int64(statement->get(), 0)}};
    if (sqlite3_column_type(statement->get(), 1) != SQLITE_NULL) {
      snapshot.manifest.clean_stopped_at_utc = UtcTime{
          std::chrono::microseconds{sqlite3_column_int64(statement->get(), 1)}};
    }
    const int complete = sqlite3_column_int(statement->get(), 2);
    if (complete != 0 && complete != 1) {
      return absl::DataLossError("invalid manifest completeness flag");
    }
    snapshot.manifest.history_complete = complete == 1;
  }
  {
    auto statement = Prepare(db.get(), R"sql(
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
        return absl::DataLossError("invalid committed sequence");
      }
      snapshot.manifest.last_committed_seq_by_shard[shard] =
          static_cast<uint64_t>(sequence);
    }
  }
  {
    auto statement = Prepare(db.get(), R"sql(
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
      if (shard < 0 || shard >= 8 || first < 1 || last < first || reason < 0 ||
          reason > static_cast<int>(GapReason::Unknown)) {
        return absl::DataLossError("invalid persisted history gap");
      }
      snapshot.gaps.push_back({run_id, ShardId{static_cast<uint8_t>(shard)},
                               static_cast<uint64_t>(first),
                               static_cast<uint64_t>(last),
                               static_cast<GapReason>(reason)});
    }
  }

  std::array<uint64_t, 8> last_seen{};
  std::map<std::string, bool> terminal_by_client;
  {
    auto statement = Prepare(db.get(), R"sql(
      SELECT shard,shard_sequence,record_blob FROM history_records
      WHERE run_id=?1 ORDER BY id)sql",
                             run_id);
    if (!statement.ok()) return statement.status();
    for (;;) {
      const int rc = sqlite3_step(statement->get());
      if (rc == SQLITE_DONE) break;
      if (rc != SQLITE_ROW) return SqlError(db.get(), "read recovery record");
      const int shard = sqlite3_column_int(statement->get(), 0);
      const sqlite3_int64 sequence = sqlite3_column_int64(statement->get(), 1);
      const char* blob =
          static_cast<const char*>(sqlite3_column_blob(statement->get(), 2));
      const int size = sqlite3_column_bytes(statement->get(), 2);
      if (shard < 0 || shard >= 8 || sequence < 1 || !blob || size < 1) {
        return absl::DataLossError("invalid recovery record index or blob");
      }
      auto decoded = storage_internal::DecodeRecord(
          std::string_view(blob, static_cast<size_t>(size)));
      if (!decoded.ok()) return absl::DataLossError(decoded.status().message());
      if (decoded->run_id != run_id || decoded->shard.value != shard ||
          decoded->shard_sequence != static_cast<uint64_t>(sequence) ||
          decoded->schema_version != 1 ||
          static_cast<uint64_t>(sequence) <= last_seen[shard]) {
        return absl::DataLossError("recovery record does not match index");
      }
      if (static_cast<uint64_t>(sequence) > last_seen[shard] + 1) {
        AddUncovered(&snapshot, shard, last_seen[shard] + 1,
                     static_cast<uint64_t>(sequence) - 1);
      }
      last_seen[shard] = static_cast<uint64_t>(sequence);
      if (const auto* intent = std::get_if<OrderIntent>(&decoded->payload)) {
        if (intent->client_id.value.empty()) {
          return absl::DataLossError("empty client ID in recovered intent");
        }
        snapshot.context.recovered_intents.push_back(*intent);
        if (intent->executor_checkpoint) {
          snapshot.context.checkpoints.push_back(
              Checkpoint{*intent->executor_checkpoint, intent->created_at_utc});
        }
      } else if (const auto* checkpoint =
                     std::get_if<Checkpoint>(&decoded->payload)) {
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
      return absl::DataLossError("record exceeds committed sequence");
    }
    if (last_seen[shard] < committed) {
      AddUncovered(&snapshot, shard, last_seen[shard] + 1, committed);
    }
  }
  for (uint8_t shard = 0; shard < 8; ++shard) {
    if (last_seen[shard] != 0 &&
        !snapshot.manifest.last_committed_seq_by_shard.contains(shard)) {
      return absl::DataLossError("record lacks committed sequence");
    }
  }
  snapshot.crash_tail_possible =
      !snapshot.manifest.clean_stopped_at_utc.has_value();
  const bool incomplete = !snapshot.manifest.history_complete ||
                          !snapshot.gaps.empty() ||
                          snapshot.crash_tail_possible;
  for (const auto& intent : snapshot.context.recovered_intents) {
    const auto terminal = terminal_by_client.find(intent.client_id.value);
    if (incomplete || terminal == terminal_by_client.end() ||
        !terminal->second) {
      snapshot.context.unresolved_ids.push_back(intent.client_id);
    }
  }
  snapshot.context.confidence =
      incomplete ? RecoveryConfidence::Unresolved : RecoveryConfidence::Partial;
  snapshot.needs_reconciliation =
      incomplete || !snapshot.context.recovered_intents.empty();
  return snapshot;
}

}  // namespace hbot
