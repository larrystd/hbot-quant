#include "order_history/record_codec.h"

#include <string>

#include "base/error.h"
#include "gtest/gtest.h"

namespace hquant {
namespace {

OrderHistoryRecord Record(OrderHistoryRecordPayload payload) {
  OrderHistoryRecord record;
  record.run_id = RunId{42};
  record.shard = ShardId{0};
  record.shard_sequence = 1;
  record.payload = std::move(payload);
  return record;
}

TEST(RecordCodecTest, VersionTwoPreservesFullWidthReasonCodesAndMessages) {
  auto gap = Record(
      OrderHistoryGap{RunId{42}, ShardId{0}, 2, 3, ErrorCode::kOrderHistoryQueueFull});
  const std::string gap_blob = storage_internal::EncodeRecord(gap);
  ASSERT_EQ(static_cast<unsigned char>(gap_blob[0]), 2);
  auto decoded_gap = storage_internal::DecodeRecord(gap_blob);
  ASSERT_TRUE(decoded_gap.ok()) << decoded_gap.status();
  EXPECT_EQ(std::get<OrderHistoryGap>(decoded_gap->payload).reason,
            ErrorCode::kOrderHistoryQueueFull);
  std::string invalid_gap = gap_blob;
  invalid_gap[invalid_gap.size() - 8] = 0;
  invalid_gap[invalid_gap.size() - 7] = 0;
  auto rejected_gap = storage_internal::DecodeRecord(invalid_gap);
  ASSERT_FALSE(rejected_gap.ok());
  EXPECT_EQ(CodeOf(rejected_gap.status()), ErrorCode::kOrderHistoryRecordCorrupted);

  auto decision = Record(
      ActionRecord{ActionBatchId{7}, StrategyId{1, StrategyName{"test"}}, 0,
                   ActionKind::Submit, false, ErrorCode::kRiskBudgetExhausted,
                   "insufficient quote", std::nullopt});
  auto decoded_decision =
      storage_internal::DecodeRecord(storage_internal::EncodeRecord(decision));
  ASSERT_TRUE(decoded_decision.ok()) << decoded_decision.status();
  const auto& value = std::get<ActionRecord>(decoded_decision->payload);
  EXPECT_EQ(value.reason, ErrorCode::kRiskBudgetExhausted);
  EXPECT_EQ(value.message, "insufficient quote");
}

TEST(RecordCodecTest, ReadsLegacyGapAndPreservesArbitraryDecisionText) {
  auto gap = Record(
      OrderHistoryGap{RunId{42}, ShardId{0}, 2, 3, ErrorCode::kOrderHistoryQueueFull});
  std::string old_gap = storage_internal::EncodeRecord(gap);
  old_gap[0] = 1;  // Codec version.
  old_gap[1] = 1;  // Envelope schema version (little endian uint64).
  old_gap.resize(old_gap.size() - 8);
  old_gap.push_back(0);  // GapReason::QueueFull in version 1.
  auto decoded_gap = storage_internal::DecodeRecord(old_gap);
  ASSERT_TRUE(decoded_gap.ok()) << decoded_gap.status();
  EXPECT_EQ(decoded_gap->schema_version, 1);
  EXPECT_EQ(std::get<OrderHistoryGap>(decoded_gap->payload).reason,
            ErrorCode::kOrderHistoryQueueFull);

  constexpr std::string_view kLegacyText = "Unmapped legacy rejection";
  auto decision = Record(
      ActionRecord{ActionBatchId{7}, StrategyId{1, StrategyName{"test"}}, 0,
                   ActionKind::Submit, false, ErrorCode::kRiskBudgetExhausted,
                   std::string(kLegacyText), std::nullopt});
  std::string old_decision = storage_internal::EncodeRecord(decision);
  old_decision[0] = 1;
  old_decision[1] = 1;
  const size_t code_position =
      old_decision.size() - 1 - kLegacyText.size() - 8 - 8;
  old_decision.erase(code_position, 8);  // Version 1 stored only reason text.
  auto decoded_decision = storage_internal::DecodeRecord(old_decision);
  ASSERT_TRUE(decoded_decision.ok()) << decoded_decision.status();
  const auto& value = std::get<ActionRecord>(decoded_decision->payload);
  EXPECT_FALSE(value.accepted);
  EXPECT_EQ(value.reason, ErrorCode::kInternal);
  EXPECT_EQ(value.message, kLegacyText);
}

}  // namespace
}  // namespace hquant
