#pragma once

#include <string>
#include <vector>

#include "absl/status/statusor.h"
#include "hbot/storage/ports.h"

namespace hbot {

// A read-only, point-in-time view of one previous run. Recorded order updates
// and trades are historical evidence; the caller must reconcile against the
// exchange before treating them as current state. Loading never sends orders.
struct RecoverySnapshot {
  RunManifest manifest;
  RecoveryContext context;
  std::vector<HistoryGap> gaps;
  // An unclean run may have accepted or attempted records after the last
  // durable sequence. Its exact tail length cannot be inferred from SQLite.
  bool crash_tail_possible = false;
  bool needs_reconciliation = true;
};

absl::StatusOr<RecoverySnapshot> LoadRecoverySnapshot(const std::string& path,
                                                      RunId run_id);

}  // namespace hbot
