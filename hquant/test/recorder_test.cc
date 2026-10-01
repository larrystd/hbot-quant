#include "offline/recorder.h"

#include <unistd.h>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>

#include "base/error.h"
#include "base/types.h"
#include "gtest/gtest.h"
#include "offline/history.h"
#include "sqlite3.h"

namespace hquant {
namespace {

class TemporaryDatabase {
 public:
  TemporaryDatabase() {
    path_ =
        (std::filesystem::temp_directory_path() / "hquant_storage_test_XXXXXX")
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
MonoTime MonoAt(int64_t us) { return MonoTime{std::chrono::microseconds{us}}; }
Decimal D(const char* text) { return *Decimal::Parse(text); }
MarketId Market() {
  return MarketId{ExchangeId{"simulated"}, InstrumentKind::Spot, "BTCUSDT"};
}
StrategyId MakeStrategyId() { return StrategyId{1, StrategyName{"simple_pmm"}}; }

RecordEnvelope Intent(uint64_t sequence, RunId run = RunId{11},
                      ShardId shard = ShardId{0}) {
  OrderIntent intent;
  intent.client_id = ClientOrderId{"B1"};
  intent.strategy_id = MakeStrategyId();
  intent.request.account = AccountId{"A1"};
  intent.request.market = Market();
  intent.request.side = Side::Buy;
  intent.request.type = OrderType::Limit;
  intent.request.base_amount = D("0.01");
  intent.request.limit_price = D("99.9");
  intent.created_at_utc = At(1000);
  intent.config_revision = 7;
  intent.executor_checkpoint = ExecutorCheckpoint{1, MakeStrategyId(), 7, "checkpoint"};
  RecordEnvelope record;
  record.run_id = run;
  record.shard = shard;
  record.shard_sequence = sequence;
  record.strategy_id = MakeStrategyId();
  record.received_at_utc = At(1000 + sequence);
  record.payload = std::move(intent);
  return record;
}

RecordEnvelope Update(uint64_t sequence) {
  RecordEnvelope record = Intent(sequence);
  OrderUpdate update;
  update.account = AccountId{"A1"};
  update.market = Market();
  update.client_id = ClientOrderId{"B1"};
  update.exchange_order_id = ExchangeOrderId{"E1"};
  update.exchange_status = ExchangeOrderStatus::PartiallyTraded;
  update.cumulative_base = D("0.004");
  update.cumulative_quote = D("0.3996");
  update.time = EventTime{At(999), At(1000 + sequence), MonoAt(42)};
  record.payload = std::move(update);
  return record;
}

RecordEnvelope Trade(uint64_t sequence) {
  RecordEnvelope record = Intent(sequence);
  TradeUpdate trade;
  trade.account = AccountId{"A1"};
  trade.market = Market();
  trade.client_id = ClientOrderId{"B1"};
  trade.exchange_order_id = ExchangeOrderId{"E1"};
  trade.exchange_trade_id = ExchangeTradeId{"T1"};
  trade.price = D("99.9");
  trade.base_amount = D("0.004");
  trade.quote_amount = D("0.3996");
  trade.fees.push_back(TradeFee{AssetId{"USDT"}, D("0.0004")});
  trade.maker = true;
  trade.time = EventTime{At(999), At(1000 + sequence), MonoAt(43)};
  record.payload = std::move(trade);
  return record;
}

std::optional<HistoryPage> WaitPage(HistoryReader& reader) {
  for (int i = 0; i < 1000; ++i) {
    if (auto page = reader.TryReceive()) return page;
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  return std::nullopt;
}

TEST(StorageTest, PersistsTypedRecordsAndPagesThroughReadOnlyConnection) {
  TemporaryDatabase db;
  auto recorder = SqliteRecorder::Open({db.path(), RunId{11}, At(100), 8, 2});
  ASSERT_TRUE(recorder.ok()) << recorder.status();
  EXPECT_TRUE((*recorder)->TryPush(Intent(1)));
  EXPECT_TRUE((*recorder)->TryPush(Update(2)));
  EXPECT_TRUE((*recorder)->TryPush(Trade(3)));
  RecordEnvelope checkpoint = Intent(4);
  checkpoint.payload =
      Checkpoint{ExecutorCheckpoint{1, MakeStrategyId(), 7, "state"}, At(1004)};
  EXPECT_TRUE((*recorder)->TryPush(std::move(checkpoint)));
  ASSERT_TRUE((*recorder)->Flush().ok());
  auto reader = HistoryReader::Open({db.path(), 4, 2});
  ASSERT_TRUE(reader.ok()) << reader.status();
  HistoryQuery query;
  query.request_id = 1;
  query.page_size = 2;
  ASSERT_TRUE((*reader)->TrySubmit(query).ok());
  auto first = WaitPage(**reader);
  ASSERT_TRUE(first);
  ASSERT_TRUE(first->status.ok()) << first->status;
  ASSERT_EQ(first->rows.size(), 2);
  EXPECT_TRUE(std::holds_alternative<OrderIntent>(first->rows[0].payload));
  EXPECT_TRUE(std::holds_alternative<OrderUpdate>(first->rows[1].payload));
  ASSERT_TRUE(first->next_cursor);
  query.request_id = 2;
  query.cursor = first->next_cursor;
  ASSERT_TRUE((*reader)->TrySubmit(query).ok());
  auto second = WaitPage(**reader);
  ASSERT_TRUE(second);
  ASSERT_TRUE(second->status.ok()) << second->status;
  ASSERT_EQ(second->rows.size(), 2);
  const auto& trade = std::get<TradeUpdate>(second->rows[0].payload);
  EXPECT_EQ(trade.exchange_trade_id.value, "T1");
  EXPECT_EQ(trade.quote_amount.ToString(), "0.3996");
  ASSERT_EQ(trade.fees.size(), 1);
  EXPECT_EQ(trade.fees[0].signed_amount.ToString(), "0.0004");
  EXPECT_TRUE(std::holds_alternative<Checkpoint>(second->rows[1].payload));
  EXPECT_FALSE(second->next_cursor);

  HistoryQuery filtered;
  filtered.request_id = 3;
  filtered.account = AccountId{"A1"};
  filtered.market = Market();
  filtered.page_size = 2;
  ASSERT_TRUE((*reader)->TrySubmit(filtered).ok());
  auto page = WaitPage(**reader);
  ASSERT_TRUE(page);
  ASSERT_TRUE(page->status.ok());
  EXPECT_EQ(page->rows.size(), 2);
  EXPECT_TRUE(page->next_cursor);
  EXPECT_TRUE((*recorder)->Stop(At(2000)).ok());
  auto manifest = (*recorder)->Manifest();
  ASSERT_TRUE(manifest.clean_stopped_at_utc);
  EXPECT_TRUE(manifest.history_complete);
  EXPECT_EQ(manifest.last_committed_seq_by_shard.at(0), 4);
}

TEST(StorageTest, QueueFullCreatesDurableGapWithoutBlockingProducer) {
  TemporaryDatabase db;
  auto recorder = SqliteRecorder::Open({db.path(), RunId{11}, At(100), 1, 1});
  ASSERT_TRUE(recorder.ok()) << recorder.status();
  (*recorder)->PauseWorkerForTesting(true);
  EXPECT_TRUE((*recorder)->TryPush(Intent(1)));
  EXPECT_FALSE((*recorder)->TryPush(Intent(2)));
  (*recorder)->PauseWorkerForTesting(false);
  ASSERT_TRUE((*recorder)->Flush().ok());
  auto health = (*recorder)->Health();
  EXPECT_EQ(health.dropped_count, 1);
  ASSERT_EQ(health.gap_ranges.size(), 1);
  EXPECT_EQ(health.gap_ranges[0].first_seq, 2);
  EXPECT_EQ(health.gap_ranges[0].last_seq, 2);
  EXPECT_EQ(health.gap_ranges[0].reason, ErrorCode::kStorageQueueFull);
  EXPECT_FALSE((*recorder)->Manifest().history_complete);
  ASSERT_TRUE((*recorder)->Stop(At(2000)).ok());
  auto reader = HistoryReader::Open({db.path(), 2, 2});
  ASSERT_TRUE(reader.ok());
  HistoryQuery query;
  query.page_size = 2;
  ASSERT_TRUE((*reader)->TrySubmit(query).ok());
  auto page = WaitPage(**reader);
  ASSERT_TRUE(page);
  ASSERT_TRUE(page->status.ok()) << page->status;
  ASSERT_EQ(page->incomplete_ranges.size(), 1);
  EXPECT_EQ(page->incomplete_ranges[0].first_seq, 2);
}

TEST(StorageTest, TwoShardProducersKeepIndependentSequenceOrder) {
  TemporaryDatabase db;
  auto recorder =
      SqliteRecorder::Open({db.path(), RunId{11}, At(100), 256, 32});
  ASSERT_TRUE(recorder.ok()) << recorder.status();
  std::thread first([&] {
    for (uint64_t sequence = 1; sequence <= 100; ++sequence)
      EXPECT_TRUE(
          (*recorder)->TryPush(Intent(sequence, RunId{11}, ShardId{0})));
  });
  std::thread second([&] {
    for (uint64_t sequence = 1; sequence <= 100; ++sequence)
      EXPECT_TRUE(
          (*recorder)->TryPush(Intent(sequence, RunId{11}, ShardId{1})));
  });
  first.join();
  second.join();
  ASSERT_TRUE((*recorder)->Flush().ok());
  const auto manifest = (*recorder)->Manifest();
  EXPECT_EQ(manifest.last_committed_seq_by_shard.at(0), 100);
  EXPECT_EQ(manifest.last_committed_seq_by_shard.at(1), 100);
  EXPECT_TRUE(manifest.history_complete);
  auto reader = HistoryReader::Open({db.path(), 2, 250});
  ASSERT_TRUE(reader.ok());
  HistoryQuery query;
  query.page_size = 250;
  ASSERT_TRUE((*reader)->TrySubmit(query).ok());
  auto page = WaitPage(**reader);
  ASSERT_TRUE(page);
  ASSERT_TRUE(page->status.ok()) << page->status;
  ASSERT_EQ(page->rows.size(), 200);
  uint64_t last[2] = {0, 0};
  for (const auto& record : page->rows) {
    ASSERT_LT(record.shard.value, 2);
    EXPECT_EQ(record.shard_sequence, ++last[record.shard.value]);
  }
  EXPECT_EQ(last[0], 100);
  EXPECT_EQ(last[1], 100);
  EXPECT_TRUE((*recorder)->Stop(At(2000)).ok());
}

TEST(StorageTest, DecisionRecordsPreserveActionOrderAndRejectionReason) {
  TemporaryDatabase db;
  auto recorder = SqliteRecorder::Open({db.path(), RunId{11}, At(100), 8, 8});
  ASSERT_TRUE(recorder.ok()) << recorder.status();
  RecordEnvelope first = Intent(1);
  first.payload = DecisionRecord{DecisionId{19},
                                 MakeStrategyId(),
                                 0,
                                 DecisionActionKind::Cancel,
                                 true,
                                 ErrorCode::kOk,
                                 "cancel requested",
                                 ClientOrderId{"B1"}};
  RecordEnvelope second = Intent(2);
  second.payload = DecisionRecord{DecisionId{19},
                                  MakeStrategyId(),
                                  1,
                                  DecisionActionKind::Submit,
                                  false,
                                  ErrorCode::kRiskLeaseExhausted,
                                  "InsufficientBalance",
                                  std::nullopt};
  EXPECT_TRUE((*recorder)->TryPush(std::move(first)));
  EXPECT_TRUE((*recorder)->TryPush(std::move(second)));
  ASSERT_TRUE((*recorder)->Flush().ok());
  auto reader = HistoryReader::Open({db.path(), 2, 2});
  ASSERT_TRUE(reader.ok());
  HistoryQuery query;
  query.strategy_id = MakeStrategyId();
  query.page_size = 2;
  ASSERT_TRUE((*reader)->TrySubmit(query).ok());
  auto page = WaitPage(**reader);
  ASSERT_TRUE(page);
  ASSERT_TRUE(page->status.ok()) << page->status;
  ASSERT_EQ(page->rows.size(), 2);
  const auto& cancel = std::get<DecisionRecord>(page->rows[0].payload);
  const auto& submit = std::get<DecisionRecord>(page->rows[1].payload);
  EXPECT_EQ(cancel.decision_id.value, 19);
  EXPECT_EQ(cancel.action_index, 0);
  EXPECT_EQ(cancel.action_kind, DecisionActionKind::Cancel);
  EXPECT_TRUE(cancel.accepted);
  EXPECT_EQ(cancel.reason, ErrorCode::kOk);
  EXPECT_EQ(cancel.message, "cancel requested");
  ASSERT_TRUE(cancel.client_id);
  EXPECT_EQ(cancel.client_id->value, "B1");
  EXPECT_EQ(submit.action_index, 1);
  EXPECT_EQ(submit.action_kind, DecisionActionKind::Submit);
  EXPECT_FALSE(submit.accepted);
  EXPECT_EQ(submit.reason, ErrorCode::kRiskLeaseExhausted);
  EXPECT_EQ(submit.message, "InsufficientBalance");
  EXPECT_FALSE(submit.client_id);
  EXPECT_TRUE((*recorder)->Stop(At(2000)).ok());
}

TEST(StorageTest, WriteFailureMarksGapAndHistoryQueryReportsErrors) {
  TemporaryDatabase db;
  auto recorder = SqliteRecorder::Open({db.path(), RunId{11}, At(100), 2, 2});
  ASSERT_TRUE(recorder.ok()) << recorder.status();
  (*recorder)->SetWriteFailureForTesting(true);
  EXPECT_TRUE((*recorder)->TryPush(Intent(1)));
  EXPECT_FALSE((*recorder)->Flush().ok());
  auto health = (*recorder)->Health();
  EXPECT_FALSE(health.last_error.empty());
  ASSERT_FALSE(health.gap_ranges.empty());
  EXPECT_EQ(health.gap_ranges[0].reason, ErrorCode::kStorageWriteFailed);
  (*recorder)->SetWriteFailureForTesting(false);
  EXPECT_TRUE((*recorder)->TryPush(Intent(2)));
  EXPECT_FALSE((*recorder)
                   ->Flush()
                   .ok());  // historical write failure remains observable
  ASSERT_TRUE((*recorder)->Stop(At(2000)).ok());
  auto reader = HistoryReader::Open({db.path(), 2, 2});
  ASSERT_TRUE(reader.ok());
  HistoryQuery expired;
  expired.request_id = 7;
  expired.page_size = 2;
  expired.deadline = MonoAt(1);
  ASSERT_TRUE((*reader)->TrySubmit(expired).ok());
  auto error = WaitPage(**reader);
  ASSERT_TRUE(error);
  EXPECT_EQ(error->request_id, 7);
  EXPECT_EQ(error->status.code(), absl::StatusCode::kDeadlineExceeded);
  HistoryQuery query;
  query.request_id = 8;
  query.page_size = 2;
  ASSERT_TRUE((*reader)->TrySubmit(query).ok());
  auto page = WaitPage(**reader);
  ASSERT_TRUE(page);
  ASSERT_TRUE(page->status.ok()) << page->status;
  ASSERT_EQ(page->rows.size(), 1);
  EXPECT_EQ(page->rows[0].shard_sequence, 2);
  ASSERT_EQ(page->incomplete_ranges.size(), 1);
  EXPECT_EQ(page->incomplete_ranges[0].reason, ErrorCode::kStorageWriteFailed);
}

TEST(StorageTest, ReadsAndMigratesLegacyGapReasons) {
  TemporaryDatabase db;
  auto first = SqliteRecorder::Open({db.path(), RunId{11}, At(100), 1, 1});
  ASSERT_TRUE(first.ok()) << first.status();
  (*first)->PauseWorkerForTesting(true);
  ASSERT_TRUE((*first)->TryPush(Intent(1)));
  ASSERT_FALSE((*first)->TryPush(Intent(2)));
  (*first)->PauseWorkerForTesting(false);
  ASSERT_TRUE((*first)->Stop(At(2000)).ok());
  first->reset();

  sqlite3* raw = nullptr;
  ASSERT_EQ(sqlite3_open(db.path().c_str(), &raw), SQLITE_OK);
  ASSERT_EQ(sqlite3_exec(raw,
                         "UPDATE history_gaps SET reason=0; "
                         "DELETE FROM schema_migrations WHERE version=2; "
                         "INSERT INTO schema_migrations(version,applied_at_us) "
                         "VALUES(1,0)",
                         nullptr, nullptr, nullptr),
            SQLITE_OK);
  sqlite3_close(raw);

  {
    auto reader = HistoryReader::Open({db.path(), 2, 2});
    ASSERT_TRUE(reader.ok()) << reader.status();
    HistoryQuery query;
    query.page_size = 2;
    ASSERT_TRUE((*reader)->TrySubmit(query).ok());
    auto page = WaitPage(**reader);
    ASSERT_TRUE(page);
    ASSERT_TRUE(page->status.ok()) << page->status;
    ASSERT_EQ(page->incomplete_ranges.size(), 1);
    EXPECT_EQ(page->incomplete_ranges[0].reason, ErrorCode::kStorageQueueFull);
  }

  auto second = SqliteRecorder::Open({db.path(), RunId{12}, At(3000), 2, 1});
  ASSERT_TRUE(second.ok()) << second.status();
  ASSERT_TRUE((*second)->Stop(At(4000)).ok());
  second->reset();
  ASSERT_EQ(sqlite3_open(db.path().c_str(), &raw), SQLITE_OK);
  sqlite3_stmt* statement = nullptr;
  ASSERT_EQ(sqlite3_prepare_v2(raw, "SELECT reason FROM history_gaps", -1,
                               &statement, nullptr),
            SQLITE_OK);
  ASSERT_EQ(sqlite3_step(statement), SQLITE_ROW);
  EXPECT_EQ(sqlite3_column_int(statement, 0),
            static_cast<int>(ErrorCode::kStorageQueueFull));
  sqlite3_finalize(statement);
  ASSERT_EQ(
      sqlite3_prepare_v2(raw, "SELECT MAX(version) FROM schema_migrations", -1,
                         &statement, nullptr),
      SQLITE_OK);
  ASSERT_EQ(sqlite3_step(statement), SQLITE_ROW);
  EXPECT_EQ(sqlite3_column_int(statement, 0), 2);
  sqlite3_finalize(statement);
  sqlite3_close(raw);
}

}  // namespace
}  // namespace hquant
