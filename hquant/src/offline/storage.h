#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <variant>
#include <vector>

#include "absl/status/status.h"
#include "base/error.h"
#include "base/order.h"
#include "base/types.h"

namespace hquant {

struct HistoryGap {
  RunId run_id;
  ShardId shard;
  uint64_t first_seq = 0;
  uint64_t last_seq = 0;
  ErrorCode reason = ErrorCode::kInternal;
};

struct Checkpoint {
  ExecutorCheckpoint state;
  UtcTime recorded_at{};
};
enum class ActionKind { Submit, Cancel };
struct ActionRecord {
  ActionBatchId action_batch_id;
  StrategyId strategy_id;
  uint32_t action_index = 0;
  ActionKind action_kind = ActionKind::Submit;
  bool accepted = false;
  ErrorCode reason = ErrorCode::kOk;
  std::string message;
  std::optional<ClientOrderId> client_id;
};
using RecordPayload = std::variant<PreparedOrder, OrderUpdate, TradeUpdate,
                                   Checkpoint, HistoryGap, ActionRecord>;

struct RecordEnvelope {
  uint32_t schema_version = 2;
  RunId run_id;
  ShardId shard;
  uint64_t shard_sequence = 0;
  std::optional<StrategyId> strategy_id;
  UtcTime received_at_utc{};
  std::optional<UtcTime> exchange_at_utc;
  RecordPayload payload;
};

struct StorageHealth {
  uint64_t dropped_count = 0;
  std::map<uint8_t, uint64_t> last_committed_seq_by_shard;
  std::vector<HistoryGap> gap_ranges;
  std::string last_error;
  uint64_t queue_watermark = 0;
};

struct RunManifest {
  RunId run_id;
  UtcTime started_at_utc{};
  std::optional<UtcTime> clean_stopped_at_utc;
  bool history_complete = true;
  std::map<uint8_t, uint64_t> last_committed_seq_by_shard;
};

struct HistoryQuery {
  uint64_t request_id = 0;
  std::optional<AccountId> account;
  std::optional<StrategyId> strategy_id;
  std::optional<MarketId> market;
  std::optional<UtcTime> from_utc;
  std::optional<UtcTime> through_utc;
  std::optional<std::string> cursor;
  uint32_t page_size = 100;
  MonoTime deadline{};
};

struct HistoryPage {
  uint64_t request_id = 0;
  absl::Status status = absl::OkStatus();
  std::vector<RecordEnvelope> rows;
  std::optional<std::string> next_cursor;
  std::vector<HistoryGap> incomplete_ranges;
};

enum class RecoveryConfidence { Verified, Partial, Unresolved };
struct RecoveryContext {
  RunId run_id;
  std::vector<PreparedOrder> recovered_prepared_orders;
  std::vector<Checkpoint> checkpoints;
  std::vector<OrderUpdate> exchange_orders;
  std::vector<TradeUpdate> exchange_trades;
  std::vector<Balance> balances;
  std::vector<ClientOrderId> unresolved_ids;
  RecoveryConfidence confidence = RecoveryConfidence::Unresolved;
};

// A shard calls TryPush without blocking or waiting for a database commit.
class RecorderPort {
 public:
  virtual ~RecorderPort() = default;
  virtual bool TryPush(RecordEnvelope record) = 0;
};

// Query submission and result collection are bounded and run off shard threads.
class HistoryReaderPort {
 public:
  virtual ~HistoryReaderPort() = default;
  virtual absl::Status TrySubmit(HistoryQuery query) = 0;
  virtual std::optional<HistoryPage> TryReceive() = 0;
};

}  // namespace hquant
