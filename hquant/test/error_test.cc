#include "base/error.h"

#include <set>
#include <string>
#include <system_error>

#include "absl/strings/cord.h"
#include "gtest/gtest.h"

namespace hquant {
namespace {

TEST(ErrorTest, RegistryHasUniqueStableEntries) {
  std::set<int32_t> numbers;
  std::set<std::string> names;
  EXPECT_EQ(AllErrorInfo().size(), 125);
  for (const auto& item : AllErrorInfo()) {
    EXPECT_NE(item.code, ErrorCode::kOk);
    EXPECT_LT(ErrorNumber(item.code), 0);
    EXPECT_TRUE(numbers.insert(ErrorNumber(item.code)).second);
    EXPECT_TRUE(names.insert(std::string(item.name)).second);
    EXPECT_EQ(Info(item.code).code, item.code);
    EXPECT_NE(item.canonical, absl::StatusCode::kOk);
  }
  EXPECT_EQ(Info(ErrorCode::kInternal).name, "INTERNAL");
  EXPECT_EQ(Info(static_cast<ErrorCode>(-19999)).code, ErrorCode::kInternal);
}

TEST(ErrorTest, StatusPayloadRoundTripAndFallback) {
  auto status = Error(ErrorCode::kFeedMessageInvalid, "bad depth JSON");
  EXPECT_EQ(status.code(), absl::StatusCode::kInvalidArgument);
  EXPECT_EQ(CodeOf(status), ErrorCode::kFeedMessageInvalid);
  EXPECT_EQ(RecoveryOf(status), Recovery::Resync);
  EXPECT_EQ(status.message(), "bad depth JSON");
  EXPECT_EQ(CodeOf(absl::OkStatus()), ErrorCode::kOk);
  EXPECT_TRUE(Error(ErrorCode::kOk, "ignored").ok());

  absl::Status external(absl::StatusCode::kInvalidArgument, "external");
  EXPECT_EQ(CodeOf(external), ErrorCode::kInternal);
  EXPECT_EQ(RecoveryOf(external), Recovery::Halt);
  external.SetPayload("type.hquant/error", absl::Cord("-19999"));
  EXPECT_EQ(CodeOf(external), ErrorCode::kInternal);
  external.SetPayload("type.hquant/error", absl::Cord("-12002x"));
  EXPECT_EQ(CodeOf(external), ErrorCode::kInternal);
  external.SetPayload("type.hquant/error", absl::Cord());
  EXPECT_EQ(CodeOf(external), ErrorCode::kInternal);
}

TEST(ErrorTest, SystemErrorKeepsCodeAndContext) {
  auto status = ErrorFromSystem(
      ErrorCode::kStateDirUnavailable,
      std::make_error_code(std::errc::permission_denied), "/state/run");
  EXPECT_EQ(CodeOf(status), ErrorCode::kStateDirUnavailable);
  EXPECT_EQ(RecoveryOf(status), Recovery::Halt);
  EXPECT_EQ(status.code(), absl::StatusCode::kPermissionDenied);
  EXPECT_NE(status.message().find("/state/run"), std::string::npos);
}

}  // namespace
}  // namespace hquant
