#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "absl/status/statusor.h"
#include "hbot/base/clock.h"

namespace hbot::fixtures {

struct FixtureStep {
  InputStamp stamp;
  std::string event_json;
  std::string output_json;
  std::string risk_expectation_json;
};

struct FixtureCase {
  std::string family;
  std::string case_id;
  std::string baseline_commit;
  std::vector<std::string> source_files;
  std::string expectation_kind;
  std::string divergence_reason;
  std::string setup_json;
  std::vector<FixtureStep> steps;
};

absl::StatusOr<FixtureCase> LoadFixture(const std::string& path);

}  // namespace hbot::fixtures
