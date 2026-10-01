#pragma once

#include <optional>
#include <string>
#include <string_view>

#include "absl/status/statusor.h"
#include "offline/storage.h"

namespace hquant::storage_internal {

// Versioned, length-prefixed, owning encoding. No SQLite pointer or borrowed
// buffer survives DecodeRecord.
std::string EncodeRecord(const HistoryRecord& record);
absl::StatusOr<HistoryRecord> DecodeRecord(std::string_view bytes);

struct RecordIndex {
  int payload_kind = 0;
  std::optional<std::string> account;
  std::optional<MarketId> market;
};
RecordIndex IndexRecord(const HistoryRecord& record);

}  // namespace hquant::storage_internal
