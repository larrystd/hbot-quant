#pragma once

#include <functional>
#include <string>

#include "absl/status/status.h"
#include "hquant/config.h"
#include "hquant/shard/shard.h"

namespace hquant::v1 {

using ReplaySink = std::function<absl::Status(ReplayRecord)>;

// Calls sink in file order and waits for its completion before parsing the next
// record. The sink routes the value object to the target Shard thread.
absl::Status ReadReplayFile(const std::string& path, const AppConfig& config,
                            const ReplaySink& sink);

}  // namespace hquant::v1
