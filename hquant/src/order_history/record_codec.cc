#include "order_history/record_codec.h"

#include <chrono>
#include <cstdint>
#include <limits>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

#include "absl/status/status.h"
#include "base/error.h"

namespace hquant::storage_internal {
namespace {

constexpr uint8_t kVersion = 2;
constexpr uint8_t kLegacyVersion = 1;

ErrorCode LegacyGapReason(uint8_t reason) {
  switch (reason) {
    case 0:
      return ErrorCode::kOrderHistoryQueueFull;
    case 1:
      return ErrorCode::kOrderHistoryWriteFailed;
    case 2:
      return ErrorCode::kRecoveryDataCorrupted;
    default:
      return ErrorCode::kInternal;
  }
}

// Stored values are magnitudes; 0 means kOk where a record allows it.
bool ValidErrorCode(uint64_t value) {
  return value == 0 || ErrorFromStoredNumber(value).has_value();
}

ErrorCode StoredCode(uint64_t value) {
  return value == 0 ? ErrorCode::kOk : *ErrorFromStoredNumber(value);
}
int64_t Us(UtcTime value) { return value.time_since_epoch().count(); }
int64_t Us(MonoTime value) { return value.time_since_epoch().count(); }
UtcTime Utc(int64_t value) { return UtcTime{std::chrono::microseconds{value}}; }
MonoTime Mono(int64_t value) {
  return MonoTime{std::chrono::microseconds{value}};
}

class Writer {
 public:
  void Byte(uint8_t value) { bytes_.push_back(static_cast<char>(value)); }
  void U64(uint64_t value) {
    for (int i = 0; i < 8; ++i) Byte(static_cast<uint8_t>(value >> (8 * i)));
  }
  void I64(int64_t value) { U64(static_cast<uint64_t>(value)); }
  void String(std::string_view value) {
    U64(value.size());
    bytes_.append(value);
  }
  void DecimalValue(const Decimal& value) { String(value.ToString()); }
  void StrategyIdField(const StrategyId& strategy) {
    U64(strategy.value);
    String(strategy.name.value);
    Byte(0);  // Former executor-present flag; kept so old records decode.
  }
  void Market(const MarketId& market) {
    String(market.exchange.value);
    Byte(static_cast<uint8_t>(market.instrument_kind));
    String(market.native_symbol);
  }
  void Time(const EventTime& time) {
    Byte(time.exchange_utc.has_value());
    if (time.exchange_utc) I64(Us(*time.exchange_utc));
    I64(Us(time.receive_utc));
    I64(Us(time.receive_mono));
  }
  void Request(const OrderRequest& request) {
    String(request.account.value);
    Market(request.market);
    Byte(static_cast<uint8_t>(request.side));
    Byte(static_cast<uint8_t>(request.type));
    DecimalValue(request.base_amount);
    Byte(request.limit_price.has_value());
    if (request.limit_price) DecimalValue(*request.limit_price);
    Byte(request.time_in_force.has_value());
    if (request.time_in_force)
      Byte(static_cast<uint8_t>(*request.time_in_force));
  }
  void CheckpointValue(const StrategyCheckpoint& checkpoint) {
    U64(checkpoint.schema_version);
    StrategyIdField(checkpoint.strategy_id);
    U64(checkpoint.config_revision);
    String(checkpoint.payload);
  }
  std::string Finish() && { return std::move(bytes_); }

 private:
  std::string bytes_;
};

class Reader {
 public:
  explicit Reader(std::string_view bytes) : bytes_(bytes) {}
  bool Byte(uint8_t* value) {
    if (position_ == bytes_.size()) return false;
    *value = static_cast<uint8_t>(bytes_[position_++]);
    return true;
  }
  bool U64(uint64_t* value) {
    if (bytes_.size() - position_ < 8) return false;
    *value = 0;
    for (int i = 0; i < 8; ++i) {
      *value |= uint64_t{static_cast<uint8_t>(bytes_[position_++])} << (8 * i);
    }
    return true;
  }
  bool I64(int64_t* value) {
    uint64_t raw = 0;
    if (!U64(&raw)) return false;
    *value = static_cast<int64_t>(raw);
    return true;
  }
  bool String(std::string* value) {
    uint64_t size = 0;
    if (!U64(&size) || size > bytes_.size() - position_) return false;
    *value = std::string(bytes_.substr(position_, size));
    position_ += size;
    return true;
  }
  bool DecimalValue(Decimal* value) {
    std::string text;
    if (!String(&text)) return false;
    auto parsed = Decimal::Parse(text);
    if (!parsed.ok()) return false;
    *value = std::move(*parsed);
    return true;
  }
  bool StrategyIdField(StrategyId* strategy) {
    uint8_t executor_present = 0;
    // Records with the former executor id are not produced and are rejected.
    return U64(&strategy->value) && String(&strategy->name.value) &&
           Byte(&executor_present) && executor_present == 0;
  }
  bool Market(MarketId* market) {
    uint8_t kind = 0;
    if (!String(&market->exchange.value) || !Byte(&kind) || kind != 0 ||
        !String(&market->native_symbol))
      return false;
    market->instrument_kind = static_cast<InstrumentKind>(kind);
    return true;
  }
  bool Time(EventTime* time) {
    uint8_t present = 0;
    int64_t value = 0;
    if (!Byte(&present) || present > 1) return false;
    if (present) {
      if (!I64(&value)) return false;
      time->exchange_utc = Utc(value);
    }
    if (!I64(&value)) return false;
    time->receive_utc = Utc(value);
    if (!I64(&value)) return false;
    time->receive_mono = Mono(value);
    return true;
  }
  bool Request(OrderRequest* request) {
    uint8_t side = 0, type = 0, present = 0;
    if (!String(&request->account.value) || !Market(&request->market) ||
        !Byte(&side) || side > 1 || !Byte(&type) || type > 1 ||
        !DecimalValue(&request->base_amount) || !Byte(&present) || present > 1)
      return false;
    request->side = static_cast<Side>(side);
    request->type = static_cast<OrderType>(type);
    if (present) {
      request->limit_price.emplace();
      if (!DecimalValue(&*request->limit_price)) return false;
    }
    if (!Byte(&present) || present > 1) return false;
    if (present) {
      uint8_t tif = 0;
      if (!Byte(&tif) || tif > 2) return false;
      request->time_in_force = static_cast<TimeInForce>(tif);
    }
    return true;
  }
  bool CheckpointValue(StrategyCheckpoint* checkpoint) {
    uint64_t version = 0;
    if (!U64(&version) || version > std::numeric_limits<uint32_t>::max() ||
        !StrategyIdField(&checkpoint->strategy_id) ||
        !U64(&checkpoint->config_revision) || !String(&checkpoint->payload))
      return false;
    checkpoint->schema_version = static_cast<uint32_t>(version);
    return true;
  }
  bool Done() const { return position_ == bytes_.size(); }

 private:
  std::string_view bytes_;
  size_t position_ = 0;
};

}  // namespace

std::string EncodeRecord(const OrderHistoryRecord& record) {
  Writer out;
  out.Byte(kVersion);
  out.U64(record.schema_version);
  out.U64(record.run_id.value);
  out.Byte(record.shard.value);
  out.U64(record.shard_sequence);
  out.Byte(record.strategy_id.has_value());
  if (record.strategy_id) out.StrategyIdField(*record.strategy_id);
  out.I64(Us(record.received_at_utc));
  out.Byte(record.exchange_at_utc.has_value());
  if (record.exchange_at_utc) out.I64(Us(*record.exchange_at_utc));
  out.Byte(static_cast<uint8_t>(record.payload.index()));
  std::visit(
      [&out](const auto& payload) {
        using T = std::decay_t<decltype(payload)>;
        if constexpr (std::is_same_v<T, PreparedOrder>) {
          out.String(payload.client_order_id.value);
          out.StrategyIdField(payload.strategy_id);
          out.Request(payload.request);
          out.U64(payload.config_revision);
          out.I64(Us(payload.created_at_utc));
          out.Byte(payload.executor_checkpoint.has_value());
          if (payload.executor_checkpoint)
            out.CheckpointValue(*payload.executor_checkpoint);
        } else if constexpr (std::is_same_v<T, OrderUpdate>) {
          out.String(payload.account.value);
          out.Market(payload.market);
          out.Byte(payload.client_order_id.has_value());
          if (payload.client_order_id) out.String(payload.client_order_id->value);
          out.Byte(payload.exchange_order_id.has_value());
          if (payload.exchange_order_id)
            out.String(payload.exchange_order_id->value);
          out.Byte(static_cast<uint8_t>(payload.exchange_status));
          out.Byte(payload.cumulative_base.has_value());
          if (payload.cumulative_base)
            out.DecimalValue(*payload.cumulative_base);
          out.Byte(payload.cumulative_quote.has_value());
          if (payload.cumulative_quote)
            out.DecimalValue(*payload.cumulative_quote);
          out.Time(payload.time);
        } else if constexpr (std::is_same_v<T, TradeUpdate>) {
          out.String(payload.account.value);
          out.Market(payload.market);
          out.Byte(payload.client_order_id.has_value());
          if (payload.client_order_id) out.String(payload.client_order_id->value);
          out.Byte(payload.exchange_order_id.has_value());
          if (payload.exchange_order_id)
            out.String(payload.exchange_order_id->value);
          out.String(payload.exchange_trade_id.value);
          out.DecimalValue(payload.price);
          out.DecimalValue(payload.base_amount);
          out.DecimalValue(payload.quote_amount);
          out.U64(payload.fees.size());
          for (const auto& fee : payload.fees) {
            out.String(fee.asset.value);
            out.DecimalValue(fee.signed_amount);
          }
          out.Byte(payload.maker.has_value());
          if (payload.maker) out.Byte(*payload.maker);
          out.Time(payload.time);
        } else if constexpr (std::is_same_v<T, RecordedCheckpoint>) {
          out.CheckpointValue(payload.state);
          out.I64(Us(payload.recorded_at));
        } else if constexpr (std::is_same_v<T, OrderHistoryGap>) {
          out.U64(payload.run_id.value);
          out.Byte(payload.shard.value);
          out.U64(payload.first_seq);
          out.U64(payload.last_seq);
          out.U64(StoredErrorNumber(payload.reason));
        } else if constexpr (std::is_same_v<T, ActionRecord>) {
          out.U64(payload.action_batch_id.value);
          out.StrategyIdField(payload.strategy_id);
          out.U64(payload.action_index);
          out.Byte(static_cast<uint8_t>(payload.action_kind));
          out.Byte(payload.accepted);
          out.U64(StoredErrorNumber(payload.reason));
          out.String(payload.message);
          out.Byte(payload.client_order_id.has_value());
          if (payload.client_order_id) out.String(payload.client_order_id->value);
        }
      },
      record.payload);
  return std::move(out).Finish();
}

absl::StatusOr<OrderHistoryRecord> DecodeRecord(std::string_view bytes) {
  Reader in(bytes);
  OrderHistoryRecord record;
  uint8_t version = 0, shard = 0, present = 0, kind = 0;
  uint64_t schema_version = 0;
  int64_t timestamp = 0;
  if (!in.Byte(&version) ||
      (version != kVersion && version != kLegacyVersion) ||
      !in.U64(&schema_version) || schema_version != version ||
      schema_version > std::numeric_limits<uint32_t>::max() ||
      !in.U64(&record.run_id.value) || !in.Byte(&shard) || shard >= 8 ||
      !in.U64(&record.shard_sequence) || !in.Byte(&present) || present > 1)
    return Error(ErrorCode::kOrderHistoryRecordCorrupted, "invalid record header");
  record.schema_version = static_cast<uint32_t>(schema_version);
  record.shard.value = shard;
  if (present) {
    record.strategy_id.emplace();
    if (!in.StrategyIdField(&*record.strategy_id))
      return Error(ErrorCode::kOrderHistoryRecordCorrupted, "invalid strategy_id");
  }
  if (!in.I64(&timestamp) || !in.Byte(&present) || present > 1)
    return Error(ErrorCode::kOrderHistoryRecordCorrupted, "invalid record time");
  record.received_at_utc = Utc(timestamp);
  if (present) {
    if (!in.I64(&timestamp))
      return Error(ErrorCode::kOrderHistoryRecordCorrupted, "invalid exchange time");
    record.exchange_at_utc = Utc(timestamp);
  }
  if (!in.Byte(&kind) || kind > 5)
    return Error(ErrorCode::kOrderHistoryRecordCorrupted, "invalid payload kind");
  if (kind == 0) {
    PreparedOrder value;
    if (!in.String(&value.client_order_id.value) ||
        !in.StrategyIdField(&value.strategy_id) ||
        !in.Request(&value.request) || !in.U64(&value.config_revision) ||
        !in.I64(&timestamp) || !in.Byte(&present) || present > 1)
      return Error(ErrorCode::kOrderHistoryRecordCorrupted,
                   "invalid prepared order");
    value.created_at_utc = Utc(timestamp);
    if (present) {
      value.executor_checkpoint.emplace();
      if (!in.CheckpointValue(&*value.executor_checkpoint))
        return Error(ErrorCode::kOrderHistoryRecordCorrupted,
                     "invalid prepared order checkpoint");
    }
    record.payload = std::move(value);
  } else if (kind == 1) {
    OrderUpdate value;
    uint8_t status = 0;
    if (!in.String(&value.account.value) || !in.Market(&value.market) ||
        !in.Byte(&present) || present > 1)
      return Error(ErrorCode::kOrderHistoryRecordCorrupted, "invalid order update");
    if (present) {
      value.client_order_id.emplace();
      if (!in.String(&value.client_order_id->value))
        return Error(ErrorCode::kOrderHistoryRecordCorrupted, "invalid client ID");
    }
    if (!in.Byte(&present) || present > 1)
      return Error(ErrorCode::kOrderHistoryRecordCorrupted,
                   "invalid exchange ID flag");
    if (present) {
      value.exchange_order_id.emplace();
      if (!in.String(&value.exchange_order_id->value))
        return Error(ErrorCode::kOrderHistoryRecordCorrupted, "invalid exchange ID");
    }
    if (!in.Byte(&status) || status > 5 || !in.Byte(&present) || present > 1)
      return Error(ErrorCode::kOrderHistoryRecordCorrupted, "invalid order status");
    value.exchange_status = static_cast<ExchangeOrderStatus>(status);
    if (present) {
      value.cumulative_base.emplace();
      if (!in.DecimalValue(&*value.cumulative_base))
        return Error(ErrorCode::kOrderHistoryRecordCorrupted,
                     "invalid cumulative base");
    }
    if (!in.Byte(&present) || present > 1)
      return Error(ErrorCode::kOrderHistoryRecordCorrupted,
                   "invalid cumulative quote flag");
    if (present) {
      value.cumulative_quote.emplace();
      if (!in.DecimalValue(&*value.cumulative_quote))
        return Error(ErrorCode::kOrderHistoryRecordCorrupted,
                     "invalid cumulative quote");
    }
    if (!in.Time(&value.time))
      return Error(ErrorCode::kOrderHistoryRecordCorrupted,
                   "invalid order update time");
    record.payload = std::move(value);
  } else if (kind == 2) {
    TradeUpdate value;
    if (!in.String(&value.account.value) || !in.Market(&value.market) ||
        !in.Byte(&present) || present > 1)
      return Error(ErrorCode::kOrderHistoryRecordCorrupted, "invalid trade update");
    if (present) {
      value.client_order_id.emplace();
      if (!in.String(&value.client_order_id->value))
        return Error(ErrorCode::kOrderHistoryRecordCorrupted,
                     "invalid trade client ID");
    }
    if (!in.Byte(&present) || present > 1)
      return Error(ErrorCode::kOrderHistoryRecordCorrupted,
                   "invalid trade exchange ID flag");
    if (present) {
      value.exchange_order_id.emplace();
      if (!in.String(&value.exchange_order_id->value))
        return Error(ErrorCode::kOrderHistoryRecordCorrupted,
                     "invalid trade exchange ID");
    }
    uint64_t count = 0;
    if (!in.String(&value.exchange_trade_id.value) ||
        !in.DecimalValue(&value.price) ||
        !in.DecimalValue(&value.base_amount) ||
        !in.DecimalValue(&value.quote_amount) || !in.U64(&count) ||
        count > 1000)
      return Error(ErrorCode::kOrderHistoryRecordCorrupted, "invalid trade values");
    for (uint64_t i = 0; i < count; ++i) {
      TradeFee fee;
      if (!in.String(&fee.asset.value) || !in.DecimalValue(&fee.signed_amount))
        return Error(ErrorCode::kOrderHistoryRecordCorrupted, "invalid trade fee");
      value.fees.push_back(std::move(fee));
    }
    if (!in.Byte(&present) || present > 1)
      return Error(ErrorCode::kOrderHistoryRecordCorrupted, "invalid maker flag");
    if (present) {
      uint8_t maker = 0;
      if (!in.Byte(&maker) || maker > 1)
        return Error(ErrorCode::kOrderHistoryRecordCorrupted, "invalid maker value");
      value.maker = maker != 0;
    }
    if (!in.Time(&value.time))
      return Error(ErrorCode::kOrderHistoryRecordCorrupted, "invalid trade time");
    record.payload = std::move(value);
  } else if (kind == 3) {
    RecordedCheckpoint value;
    if (!in.CheckpointValue(&value.state) || !in.I64(&timestamp))
      return Error(ErrorCode::kOrderHistoryRecordCorrupted, "invalid checkpoint");
    value.recorded_at = Utc(timestamp);
    record.payload = std::move(value);
  } else if (kind == 4) {
    OrderHistoryGap value;
    uint8_t legacy_reason = 0;
    uint64_t reason = 0;
    if (!in.U64(&value.run_id.value) || !in.Byte(&value.shard.value) ||
        value.shard.value >= 8 || !in.U64(&value.first_seq) ||
        !in.U64(&value.last_seq) ||
        (version == kLegacyVersion
             ? (!in.Byte(&legacy_reason) || legacy_reason > 3)
             : (!in.U64(&reason) || reason == 0 || !ValidErrorCode(reason))))
      return Error(ErrorCode::kOrderHistoryRecordCorrupted, "invalid history gap");
    value.reason = version == kLegacyVersion ? LegacyGapReason(legacy_reason)
                                             : StoredCode(reason);
    record.payload = std::move(value);
  } else {
    ActionRecord value;
    uint64_t action_index = 0;
    uint8_t action_kind = 0, accepted = 0;
    uint64_t reason = 0;
    if (!in.U64(&value.action_batch_id.value) ||
        !in.StrategyIdField(&value.strategy_id) || !in.U64(&action_index) ||
        action_index > std::numeric_limits<uint32_t>::max() ||
        !in.Byte(&action_kind) || action_kind > 1 || !in.Byte(&accepted) ||
        accepted > 1 ||
        (version == kVersion &&
         (!in.U64(&reason) || !ValidErrorCode(reason))) ||
        !in.String(&value.message) || !in.Byte(&present) || present > 1)
      return Error(ErrorCode::kOrderHistoryRecordCorrupted,
                   "invalid decision record");
    value.action_index = static_cast<uint32_t>(action_index);
    value.action_kind = static_cast<ActionKind>(action_kind);
    value.accepted = accepted != 0;
    // Legacy records retained arbitrary reason text; preserve it verbatim.
    value.reason =
        version == kLegacyVersion
            ? (value.accepted ? ErrorCode::kOk : ErrorCode::kInternal)
            : StoredCode(reason);
    if (present) {
      value.client_order_id.emplace();
      if (!in.String(&value.client_order_id->value))
        return Error(ErrorCode::kOrderHistoryRecordCorrupted,
                     "invalid decision client ID");
    }
    record.payload = std::move(value);
  }
  if (!in.Done())
    return Error(ErrorCode::kOrderHistoryRecordCorrupted, "trailing record bytes");
  return record;
}

RecordIndex IndexRecord(const OrderHistoryRecord& record) {
  RecordIndex index;
  index.payload_kind = static_cast<int>(record.payload.index());
  if (const auto* value = std::get_if<PreparedOrder>(&record.payload)) {
    index.account = value->request.account.value;
    index.market = value->request.market;
  } else if (const auto* value = std::get_if<OrderUpdate>(&record.payload)) {
    index.account = value->account.value;
    index.market = value->market;
  } else if (const auto* value = std::get_if<TradeUpdate>(&record.payload)) {
    index.account = value->account.value;
    index.market = value->market;
  }
  return index;
}

}  // namespace hquant::storage_internal
