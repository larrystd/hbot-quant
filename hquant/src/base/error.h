#pragma once

#include <cstdint>
#include <span>
#include <string_view>
#include <system_error>

#include "absl/status/status.h"

namespace hquant {

enum class ErrorCode : uint16_t {
  kOk = 0,
  kDecimalInvalid = 10001,
  kDecimalPrecisionExceeded = 10002,
  kSequenceExhausted = 10003,
  kCancelled = 10004,
  kUnimplemented = 10005,
  kInternal = 10900,
  kNetTlsConfigInvalid = 11001,
  kNetTargetInvalid = 11002,
  kNetTimeout = 11003,
  kNetUnavailable = 11004,
  kNetBusy = 11005,
  kNetTlsVerifyFailed = 11006,
  kHttpStatusUnexpected = 11007,
  kVenueRateLimited = 11008,
  kVenueIpBanned = 11009,
  kRateBreakerOpen = 11010,
  kRateLeaseMissing = 11011,
  kRateLeaseExhausted = 11012,
  kRateConfigInvalid = 11013,
  kFeedConfigInvalid = 12001,
  kFeedMessageInvalid = 12002,
  kFeedScaleMismatch = 12003,
  kFeedWrongMarket = 12004,
  kBookSequenceGap = 12005,
  kBookCrossed = 12006,
  kBookBufferOverflow = 12007,
  kBookStale = 12008,
  kFeedDisconnected = 12009,
  kReplayFileNotFound = 12010,
  kReplayFileInvalid = 12011,
  kOrderInvalid = 13001,
  kOrderRuleViolation = 13002,
  kOrderDuplicate = 13003,
  kOrderNotFound = 13004,
  kOrderNotCancelable = 13005,
  kOrderCancelPending = 13006,
  kOrderSlotsFull = 13007,
  kOrderExpiredBeforeSend = 13008,
  kOrderSubmissionUnknown = 13009,
  kOrderVenueRejected = 13010,
  kOrderReportInvalid = 13011,
  kOrderReportConflict = 13012,
  kOrderReportUnattributed = 13013,
  kClientIdInvalid = 13014,
  kSigningFailed = 13015,
  kOrderRecoveryInvalid = 13016,
  kPaperBalanceInsufficient = 13017,
  kPaperInputInvalid = 13018,
  kRiskEmergencyStopped = 14001,
  kRiskNotReady = 14002,
  kRiskLimitExceeded = 14003,
  kRiskLeaseExhausted = 14004,
  kRiskLeaseMissing = 14005,
  kRiskLeaseStale = 14006,
  kRiskConfigInvalid = 14007,
  kRiskReservationInvalid = 14008,
  kAccountMessageInvalid = 15001,
  kAccountEventUnsupported = 15002,
  kReconcileResponseInvalid = 15003,
  kReconcileIdentityMismatch = 15004,
  kReconcileQueryFailed = 15005,
  kReconcileResponseTooLarge = 15006,
  kReconcileTargetInvalid = 15007,
  kRouteOwnerUnknown = 15008,
  kRouteOwnershipConflict = 15009,
  kRouteReportInvalid = 15010,
  kRouteQueueFull = 15011,
  kRouteConfigInvalid = 15012,
  kShardInputInvalid = 16001,
  kShardReportUnattributed = 16002,
  kStrategyCallbackTimeout = 16003,
  kStrategyDependencyNotReady = 16004,
  kStorageOptionsInvalid = 17001,
  kStorageOpenFailed = 17002,
  kStorageWriteFailed = 17003,
  kStorageQueueFull = 17004,
  kStorageGapPersistFailed = 17005,
  kStorageQueryFailed = 17006,
  kHistoryBusy = 17007,
  kHistoryStopping = 17008,
  kHistoryDeadline = 17009,
  kHistoryCursorInvalid = 17010,
  kHistoryTooManyGaps = 17011,
  kRecoveryManifestNotFound = 17012,
  kRecoveryDataCorrupted = 17013,
  kRecordEncodingInvalid = 17014,
  kConfigFileNotFound = 18001,
  kConfigSyntaxInvalid = 18002,
  kConfigSchemaUnsupported = 18003,
  kConfigFieldInvalid = 18004,
  kConfigReferenceInvalid = 18005,
  kConfigAssignmentInvalid = 18006,
  kConfigModeNotAllowed = 18007,
  kConfigCredentialsInline = 18008,
  kConfigLeaseInvalid = 18009,
  kLaunchUnsupportedTopology = 18010,
  kStateDirUnavailable = 18011,
  kControlMessageInvalid = 19001,
  kControlSocketFailed = 19002,
  kControlSocketInUse = 19003,
  kCliUsageInvalid = 19004,
  kCliEngineUnreachable = 19005,
  kCliResponseInvalid = 19006,
  kCliCommandFailed = 19007,
  kControlBusy = 19008,
  kControlTimeout = 19009,
};

enum class Recovery : uint8_t {
  Retry,
  Resync,
  Reconcile,
  Reject,
  Degrade,
  Halt
};

struct ErrorInfo {
  ErrorCode code;
  std::string_view name;
  Recovery recovery;
  absl::StatusCode canonical;
};

// Unknown values safely resolve to INTERNAL. kOk has no recovery action.
const ErrorInfo& Info(ErrorCode code);
std::span<const ErrorInfo> AllErrorInfo();
absl::Status Error(ErrorCode code, std::string_view message);
ErrorCode CodeOf(const absl::Status& status);
Recovery RecoveryOf(const absl::Status& status);
absl::Status ErrorFromSystem(ErrorCode code, std::error_code ec,
                             std::string_view context);

}  // namespace hquant
