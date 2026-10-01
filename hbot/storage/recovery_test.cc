#include "hbot/storage/recovery.h"

#include <unistd.h>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <stdexcept>
#include <string>

#include "gtest/gtest.h"
#include "hbot/base/decimal.h"
#include "hbot/storage/sqlite_recorder.h"
#include "sqlite3.h"

namespace hbot {
namespace {

class TemporaryDatabase {
 public:
  TemporaryDatabase() {
    path_ =
        (std::filesystem::temp_directory_path() / "hbot_recovery_test_XXXXXX")
            .string();
    int fd = mkstemp(path_.data());
    if (fd < 0) throw std::runtime_error("mkstemp failed");
    close(fd);
  }
  ~TemporaryDatabase() {
    std::filesystem::remove(path_);
    std::filesystem::remove(path_ + "-wal");
    std::filesystem::remove(path_ + "-shm");
  }
  const std::string& path() const { return path_; }

 private:
  std::string path_;
};

UtcTime At(int64_t us) { return UtcTime{std::chrono::microseconds{us}}; }
Decimal D(const char* value) { return *Decimal::Parse(value); }
OwnerId Owner() { return OwnerId{1, StrategyId{"recovery"}, std::nullopt}; }
MarketId Market() {
  return MarketId{VenueId{"binance"}, InstrumentKind::Spot, "BTCUSDT"};
}

RecordEnvelope Intent(uint64_t sequence, std::string client = "B1") {
  OrderIntent intent;
  intent.client_id = ClientOrderId{std::move(client)};
  intent.owner = Owner();
  intent.request.account = AccountId{"A1"};
  intent.request.market = Market();
  intent.request.side = Side::Buy;
  intent.request.type = OrderType::Limit;
  intent.request.base_amount = D("0.01");
  intent.request.limit_price = D("100");
  intent.created_at_utc = At(1000 + sequence);
  intent.config_revision = 7;
  intent.executor_checkpoint =
      ExecutorCheckpoint{1, Owner(), 7, "intent state"};
  RecordEnvelope record;
  record.run_id = RunId{21};
  record.shard = ShardId{0};
  record.shard_sequence = sequence;
  record.owner = Owner();
  record.received_at_utc = At(1000 + sequence);
  record.payload = std::move(intent);
  return record;
}

TEST(RecoveryTest, CleanRunLoadsTypedContextAndOpenClientIds) {
  TemporaryDatabase db;
  auto recorder = SqliteRecorder::Open({db.path(), RunId{21}, At(100), 8, 4});
  ASSERT_TRUE(recorder.ok()) << recorder.status();
  ASSERT_TRUE((*recorder)->TryPush(Intent(1, "B1")));
  RecordEnvelope filled = Intent(2);
  OrderUpdate update;
  update.account = AccountId{"A1"};
  update.market = Market();
  update.client_id = ClientOrderId{"B1"};
  update.exchange_status = ExchangeOrderStatus::Filled;
  filled.payload = update;
  ASSERT_TRUE((*recorder)->TryPush(std::move(filled)));
  ASSERT_TRUE((*recorder)->TryPush(Intent(3, "B2")));
  RecordEnvelope checkpoint = Intent(4);
  checkpoint.payload =
      Checkpoint{ExecutorCheckpoint{1, Owner(), 8, "latest state"}, At(1004)};
  ASSERT_TRUE((*recorder)->TryPush(std::move(checkpoint)));
  ASSERT_TRUE((*recorder)->Stop(At(2000)).ok());
  recorder->reset();

  auto recovered = LoadRecoverySnapshot(db.path(), RunId{21});
  ASSERT_TRUE(recovered.ok()) << recovered.status();
  EXPECT_TRUE(recovered->manifest.history_complete);
  ASSERT_TRUE(recovered->manifest.clean_stopped_at_utc);
  EXPECT_EQ(recovered->manifest.last_committed_seq_by_shard.at(0), 4);
  EXPECT_FALSE(recovered->crash_tail_possible);
  EXPECT_TRUE(recovered->gaps.empty());
  EXPECT_EQ(recovered->context.recovered_intents.size(), 2);
  EXPECT_EQ(recovered->context.exchange_orders.size(), 1);
  EXPECT_EQ(recovered->context.checkpoints.size(), 3);
  ASSERT_EQ(recovered->context.unresolved_ids.size(), 1);
  EXPECT_EQ(recovered->context.unresolved_ids[0].value, "B2");
  EXPECT_EQ(recovered->context.confidence, RecoveryConfidence::Partial);
  EXPECT_TRUE(recovered->needs_reconciliation);
}

TEST(RecoveryTest, UncleanStopReportsPossibleCrashTailWithoutInventingRange) {
  TemporaryDatabase db;
  {
    auto recorder = SqliteRecorder::Open({db.path(), RunId{21}, At(100), 8, 1});
    ASSERT_TRUE(recorder.ok()) << recorder.status();
    ASSERT_TRUE((*recorder)->TryPush(Intent(1)));
    ASSERT_TRUE((*recorder)->Flush().ok());
    // Destruction simulates process termination without a clean stop marker.
  }
  auto recovered = LoadRecoverySnapshot(db.path(), RunId{21});
  ASSERT_TRUE(recovered.ok()) << recovered.status();
  EXPECT_TRUE(recovered->crash_tail_possible);
  EXPECT_FALSE(recovered->manifest.clean_stopped_at_utc);
  EXPECT_TRUE(recovered->gaps.empty());
  EXPECT_EQ(recovered->manifest.last_committed_seq_by_shard.at(0), 1);
  ASSERT_EQ(recovered->context.unresolved_ids.size(), 1);
  EXPECT_EQ(recovered->context.confidence, RecoveryConfidence::Unresolved);
}

TEST(RecoveryTest, DistinguishesQueueDropFromDiskWriteFailure) {
  TemporaryDatabase db;
  auto recorder = SqliteRecorder::Open({db.path(), RunId{21}, At(100), 1, 1});
  ASSERT_TRUE(recorder.ok()) << recorder.status();
  (*recorder)->PauseWorkerForTesting(true);
  ASSERT_TRUE((*recorder)->TryPush(Intent(1)));
  ASSERT_FALSE((*recorder)->TryPush(Intent(2)));
  (*recorder)->PauseWorkerForTesting(false);
  ASSERT_TRUE((*recorder)->Flush().ok());
  (*recorder)->SetWriteFailureForTesting(true);
  ASSERT_TRUE((*recorder)->TryPush(Intent(3)));
  ASSERT_FALSE((*recorder)->Flush().ok());
  (*recorder)->SetWriteFailureForTesting(false);
  ASSERT_TRUE((*recorder)->TryPush(Intent(4)));
  ASSERT_TRUE((*recorder)->Stop(At(2000)).ok());

  auto recovered = LoadRecoverySnapshot(db.path(), RunId{21});
  ASSERT_TRUE(recovered.ok()) << recovered.status();
  EXPECT_FALSE(recovered->manifest.history_complete);
  EXPECT_FALSE(recovered->crash_tail_possible);
  bool queue_gap = false, disk_gap = false;
  for (const auto& gap : recovered->gaps) {
    queue_gap |= gap.first_seq == 2 && gap.reason == GapReason::QueueFull;
    disk_gap |= gap.first_seq == 3 && gap.reason == GapReason::WriteFailed;
  }
  EXPECT_TRUE(queue_gap);
  EXPECT_TRUE(disk_gap);
  EXPECT_EQ(recovered->context.confidence, RecoveryConfidence::Unresolved);
}

TEST(RecoveryTest, RejectsCorruptedRecordAndMissingRun) {
  TemporaryDatabase db;
  auto recorder = SqliteRecorder::Open({db.path(), RunId{21}, At(100), 8, 1});
  ASSERT_TRUE(recorder.ok()) << recorder.status();
  ASSERT_TRUE((*recorder)->TryPush(Intent(1)));
  ASSERT_TRUE((*recorder)->Stop(At(2000)).ok());
  recorder->reset();
  EXPECT_EQ(LoadRecoverySnapshot(db.path(), RunId{22}).status().code(),
            absl::StatusCode::kNotFound);
  sqlite3* raw = nullptr;
  ASSERT_EQ(sqlite3_open(db.path().c_str(), &raw), SQLITE_OK);
  ASSERT_EQ(
      sqlite3_exec(
          raw,
          "UPDATE history_records SET record_blob=x'00' WHERE shard_sequence=1",
          nullptr, nullptr, nullptr),
      SQLITE_OK);
  sqlite3_close(raw);
  EXPECT_EQ(LoadRecoverySnapshot(db.path(), RunId{21}).status().code(),
            absl::StatusCode::kDataLoss);
}

TEST(RecoveryTest, RejectsUnknownStorageSchemaVersion) {
  TemporaryDatabase db;
  auto recorder = SqliteRecorder::Open({db.path(), RunId{21}, At(100), 8, 1});
  ASSERT_TRUE(recorder.ok()) << recorder.status();
  ASSERT_TRUE((*recorder)->Stop(At(2000)).ok());
  recorder->reset();
  sqlite3* raw = nullptr;
  ASSERT_EQ(sqlite3_open(db.path().c_str(), &raw), SQLITE_OK);
  ASSERT_EQ(
      sqlite3_exec(
          raw,
          "INSERT INTO schema_migrations(version,applied_at_us) VALUES(2,0)",
          nullptr, nullptr, nullptr),
      SQLITE_OK);
  sqlite3_close(raw);
  EXPECT_EQ(LoadRecoverySnapshot(db.path(), RunId{21}).status().code(),
            absl::StatusCode::kFailedPrecondition);
}

}  // namespace
}  // namespace hbot
