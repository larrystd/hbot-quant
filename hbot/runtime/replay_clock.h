#pragma once

#include <optional>

#include "absl/status/status.h"
#include "hbot/base/clock.h"

namespace hbot {

// Owns both time domains used by the same deterministic input stream.
class ReplayClock final : public Clock {
 public:
  ReplayClock(UtcTime utc_origin, MonoTime mono_origin)
      : utc_origin_(utc_origin), mono_origin_(mono_origin) {}

  absl::Status Advance(InputStamp stamp) {
    if (stamp.at_us < 0 || (last_ && (stamp.at_us < last_->at_us ||
                                      (stamp.at_us == last_->at_us &&
                                       stamp.ordinal <= last_->ordinal)))) {
      return absl::InvalidArgumentError("replay input order must increase");
    }
    last_ = stamp;
    return absl::OkStatus();
  }

  UtcTime UtcNow() const override {
    return utc_origin_ + std::chrono::microseconds(last_ ? last_->at_us : 0);
  }
  MonoTime MonoNow() const override {
    return mono_origin_ + std::chrono::microseconds(last_ ? last_->at_us : 0);
  }
  std::optional<InputStamp> LastInput() const { return last_; }

 private:
  UtcTime utc_origin_;
  MonoTime mono_origin_;
  std::optional<InputStamp> last_;
};

}  // namespace hbot
