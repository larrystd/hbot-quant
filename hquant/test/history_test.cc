#include <unistd.h>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <stdexcept>
#include <string>

#include "base/error.h"
#include "base/types.h"
#include "gtest/gtest.h"
#include "order_history/order_history_reader.h"
#include "order_history/order_history_writer.h"
#include "sqlite3.h"

namespace hquant {
namespace {

class TemporaryDatabase {
 public:
  TemporaryDatabase() {
    path_ =
        (std::filesystem::temp_directory_path() / "hquant_recovery_test_XXXXXX")
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
StrategyId MakeStrategyId() { return StrategyId{1, StrategyName{"recovery"}}; }
MarketId Market() {
  return MarketId{ExchangeId{"binance"}, InstrumentKind::Spot, "BTCUSDT"};
}

OrderHistoryRecord PreparedRecord(uint64_t sequence,
                                  std::string client = "B1") {
  LegacyPreparedOrder prepared;
  prepared.client_order_id = ClientOrderId{std::move(client)};
  prepared.strategy_id = MakeStrategyId();
  prepared.request.account = AccountId{"A1"};
  prepared.request.market = Market();
  prepared.request.side = Side::Buy;
  prepared.request.type = uint8_t{0};
  prepared.request.quantity = D("0.01");
  prepared.request.limit_price = D("100");
  prepared.created_at_utc = At(1000 + sequence);
  prepared.config_revision = 7;
  prepared.executor_checkpoint =
      LegacyStrategyCheckpoint{1, MakeStrategyId(), 7, "prepared state"};
  OrderHistoryRecord record;
  record.run_id = RunId{21};
  record.shard = ShardId{0};
  record.shard_sequence = sequence;
  record.strategy_id = MakeStrategyId();
  record.received_at_utc = At(1000 + sequence);
  record.payload = std::move(prepared);
  return record;
}

TEST(RecoveryTest, CleanRunLoadsTypedContextAndOpenClientIds) {
  TemporaryDatabase db;
  auto recorder =
      SqliteOrderHistoryWriter::Open({db.path(), RunId{21}, At(100), 8, 4});
  ASSERT_TRUE(recorder.ok()) << recorder.status();
  ASSERT_TRUE((*recorder)->TryPush(PreparedRecord(1, "B1")));
  OrderHistoryRecord filled = PreparedRecord(2);
  LegacyOrderUpdate update;
  update.account = AccountId{"A1"};
  update.market = Market();
  update.client_order_id = ClientOrderId{"B1"};
  update.exchange_status = ExchangeOrderStatus::Traded;
  filled.payload = update;
  ASSERT_TRUE((*recorder)->TryPush(std::move(filled)));
  ASSERT_TRUE((*recorder)->TryPush(PreparedRecord(3, "B2")));
  OrderHistoryRecord checkpoint = PreparedRecord(4);
  checkpoint.payload = LegacyRecordedCheckpoint{
      LegacyStrategyCheckpoint{1, MakeStrategyId(), 8, "latest state"},
      At(1004)};
  ASSERT_TRUE((*recorder)->TryPush(std::move(checkpoint)));
  ASSERT_TRUE((*recorder)->Stop(At(2000)).ok());
  recorder->reset();

  auto recovered = LoadPreviousRun(db.path(), RunId{21});
  ASSERT_TRUE(recovered.ok()) << recovered.status();
  EXPECT_TRUE(recovered->manifest.history_complete);
  ASSERT_TRUE(recovered->manifest.clean_stopped_at_utc);
  EXPECT_EQ(recovered->manifest.last_committed_seq_by_shard.at(0), 4);
  EXPECT_FALSE(recovered->may_have_unwritten_records);
  EXPECT_TRUE(recovered->gaps.empty());
  EXPECT_EQ(recovered->context.recovered_legacy_orders.size(), 2);
  EXPECT_EQ(recovered->context.legacy_exchange_orders.size(), 1);
  EXPECT_EQ(recovered->context.legacy_checkpoints.size(), 3);
  ASSERT_EQ(recovered->context.unresolved_ids.size(), 1);
  EXPECT_EQ(recovered->context.unresolved_ids[0].value, "B2");
  EXPECT_EQ(recovered->context.confidence, PreviousRunCompleteness::Partial);
  EXPECT_TRUE(recovered->needs_order_query);
}

TEST(RecoveryTest, UncleanStopReportsPossibleCrashTailWithoutInventingRange) {
  TemporaryDatabase db;
  {
    auto recorder =
        SqliteOrderHistoryWriter::Open({db.path(), RunId{21}, At(100), 8, 1});
    ASSERT_TRUE(recorder.ok()) << recorder.status();
    ASSERT_TRUE((*recorder)->TryPush(PreparedRecord(1)));
    ASSERT_TRUE((*recorder)->Flush().ok());
    // Destruction simulates process termination without a clean stop marker.
  }
  auto recovered = LoadPreviousRun(db.path(), RunId{21});
  ASSERT_TRUE(recovered.ok()) << recovered.status();
  EXPECT_TRUE(recovered->may_have_unwritten_records);
  EXPECT_FALSE(recovered->manifest.clean_stopped_at_utc);
  EXPECT_TRUE(recovered->gaps.empty());
  EXPECT_EQ(recovered->manifest.last_committed_seq_by_shard.at(0), 1);
  ASSERT_EQ(recovered->context.unresolved_ids.size(), 1);
  EXPECT_EQ(recovered->context.confidence, PreviousRunCompleteness::Unresolved);
}

TEST(RecoveryTest, DistinguishesQueueDropFromDiskWriteFailure) {
  TemporaryDatabase db;
  auto recorder =
      SqliteOrderHistoryWriter::Open({db.path(), RunId{21}, At(100), 1, 1});
  ASSERT_TRUE(recorder.ok()) << recorder.status();
  (*recorder)->PauseWorkerForTesting(true);
  ASSERT_TRUE((*recorder)->TryPush(PreparedRecord(1)));
  ASSERT_FALSE((*recorder)->TryPush(PreparedRecord(2)));
  (*recorder)->PauseWorkerForTesting(false);
  ASSERT_TRUE((*recorder)->Flush().ok());
  (*recorder)->SetWriteFailureForTesting(true);
  ASSERT_TRUE((*recorder)->TryPush(PreparedRecord(3)));
  ASSERT_FALSE((*recorder)->Flush().ok());
  (*recorder)->SetWriteFailureForTesting(false);
  ASSERT_TRUE((*recorder)->TryPush(PreparedRecord(4)));
  ASSERT_TRUE((*recorder)->Stop(At(2000)).ok());

  auto recovered = LoadPreviousRun(db.path(), RunId{21});
  ASSERT_TRUE(recovered.ok()) << recovered.status();
  EXPECT_FALSE(recovered->manifest.history_complete);
  EXPECT_FALSE(recovered->may_have_unwritten_records);
  bool queue_gap = false, disk_gap = false;
  for (const auto& gap : recovered->gaps) {
    queue_gap |=
        gap.first_seq == 2 && gap.reason == ErrorCode::kOrderHistoryQueueFull;
    disk_gap |=
        gap.first_seq == 3 && gap.reason == ErrorCode::kOrderHistoryWriteFailed;
  }
  EXPECT_TRUE(queue_gap);
  EXPECT_TRUE(disk_gap);
  EXPECT_EQ(recovered->context.confidence, PreviousRunCompleteness::Unresolved);
}

TEST(RecoveryTest, RejectsCorruptedRecordAndMissingRun) {
  TemporaryDatabase db;
  auto recorder =
      SqliteOrderHistoryWriter::Open({db.path(), RunId{21}, At(100), 8, 1});
  ASSERT_TRUE(recorder.ok()) << recorder.status();
  ASSERT_TRUE((*recorder)->TryPush(PreparedRecord(1)));
  ASSERT_TRUE((*recorder)->Stop(At(2000)).ok());
  recorder->reset();
  EXPECT_EQ(LoadPreviousRun(db.path(), RunId{22}).status().code(),
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
  EXPECT_EQ(LoadPreviousRun(db.path(), RunId{21}).status().code(),
            absl::StatusCode::kDataLoss);
}

TEST(RecoveryTest, RejectsUnknownStorageSchemaVersion) {
  TemporaryDatabase db;
  auto recorder =
      SqliteOrderHistoryWriter::Open({db.path(), RunId{21}, At(100), 8, 1});
  ASSERT_TRUE(recorder.ok()) << recorder.status();
  ASSERT_TRUE((*recorder)->Stop(At(2000)).ok());
  recorder->reset();
  sqlite3* raw = nullptr;
  ASSERT_EQ(sqlite3_open(db.path().c_str(), &raw), SQLITE_OK);
  ASSERT_EQ(
      sqlite3_exec(
          raw,
          "INSERT INTO schema_migrations(version,applied_at_us) VALUES(3,0)",
          nullptr, nullptr, nullptr),
      SQLITE_OK);
  sqlite3_close(raw);
  EXPECT_EQ(LoadPreviousRun(db.path(), RunId{21}).status().code(),
            absl::StatusCode::kInvalidArgument);
}

}  // namespace
}  // namespace hquant
