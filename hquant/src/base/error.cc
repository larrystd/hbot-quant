#include "base/error.h"

#include <array>
#include <charconv>
#include <string>
#include <system_error>

#include "absl/strings/cord.h"

namespace hquant {
namespace {

constexpr std::string_view kPayloadType = "type.hquant/error";
constexpr ErrorInfo kOkInfo{ErrorCode::kOk, "OK", Recovery::Reject,
                            absl::StatusCode::kOk};
constexpr std::array<ErrorInfo, 106> kErrors{{
    {ErrorCode::kDecimalInvalid, "DECIMAL_INVALID", Recovery::Reject,
     absl::StatusCode::kInvalidArgument},
    {ErrorCode::kDecimalPrecisionExceeded, "DECIMAL_PRECISION_EXCEEDED",
     Recovery::Reject, absl::StatusCode::kOutOfRange},
    {ErrorCode::kSequenceExhausted, "SEQUENCE_EXHAUSTED", Recovery::Halt,
     absl::StatusCode::kResourceExhausted},
    {ErrorCode::kCancelled, "CANCELLED", Recovery::Reject,
     absl::StatusCode::kCancelled},
    {ErrorCode::kUnimplemented, "UNIMPLEMENTED", Recovery::Reject,
     absl::StatusCode::kUnimplemented},
    {ErrorCode::kInternal, "INTERNAL", Recovery::Halt,
     absl::StatusCode::kInternal},
    {ErrorCode::kNetTlsConfigInvalid, "NET_TLS_CONFIG_INVALID", Recovery::Halt,
     absl::StatusCode::kFailedPrecondition},
    {ErrorCode::kNetTargetInvalid, "NET_TARGET_INVALID", Recovery::Reject,
     absl::StatusCode::kInvalidArgument},
    {ErrorCode::kNetTimeout, "NET_TIMEOUT", Recovery::Retry,
     absl::StatusCode::kDeadlineExceeded},
    {ErrorCode::kNetUnavailable, "NET_UNAVAILABLE", Recovery::Retry,
     absl::StatusCode::kUnavailable},
    {ErrorCode::kNetBusy, "NET_BUSY", Recovery::Retry,
     absl::StatusCode::kFailedPrecondition},
    {ErrorCode::kNetTlsVerifyFailed, "NET_TLS_VERIFY_FAILED", Recovery::Halt,
     absl::StatusCode::kUnavailable},
    {ErrorCode::kHttpStatusUnexpected, "HTTP_STATUS_UNEXPECTED",
     Recovery::Retry, absl::StatusCode::kUnavailable},
    {ErrorCode::kVenueRateLimited, "VENUE_RATE_LIMITED", Recovery::Retry,
     absl::StatusCode::kResourceExhausted},
    {ErrorCode::kVenueIpBanned, "VENUE_IP_BANNED", Recovery::Halt,
     absl::StatusCode::kResourceExhausted},
    {ErrorCode::kRateBreakerOpen, "RATE_BREAKER_OPEN", Recovery::Reject,
     absl::StatusCode::kUnavailable},
    {ErrorCode::kRateLeaseMissing, "RATE_LEASE_MISSING", Recovery::Reject,
     absl::StatusCode::kFailedPrecondition},
    {ErrorCode::kRateLeaseExhausted, "RATE_LEASE_EXHAUSTED", Recovery::Reject,
     absl::StatusCode::kResourceExhausted},
    {ErrorCode::kRateConfigInvalid, "RATE_CONFIG_INVALID", Recovery::Halt,
     absl::StatusCode::kInvalidArgument},
    {ErrorCode::kFeedConfigInvalid, "FEED_CONFIG_INVALID", Recovery::Halt,
     absl::StatusCode::kInvalidArgument},
    {ErrorCode::kFeedMessageInvalid, "FEED_MESSAGE_INVALID", Recovery::Resync,
     absl::StatusCode::kInvalidArgument},
    {ErrorCode::kFeedScaleMismatch, "FEED_SCALE_MISMATCH", Recovery::Resync,
     absl::StatusCode::kOutOfRange},
    {ErrorCode::kFeedWrongMarket, "FEED_WRONG_MARKET", Recovery::Resync,
     absl::StatusCode::kInvalidArgument},
    {ErrorCode::kBookSequenceGap, "BOOK_SEQUENCE_GAP", Recovery::Resync,
     absl::StatusCode::kAborted},
    {ErrorCode::kBookCrossed, "BOOK_CROSSED", Recovery::Resync,
     absl::StatusCode::kAborted},
    {ErrorCode::kBookBufferOverflow, "BOOK_BUFFER_OVERFLOW", Recovery::Resync,
     absl::StatusCode::kResourceExhausted},
    {ErrorCode::kBookStale, "BOOK_STALE", Recovery::Degrade,
     absl::StatusCode::kUnavailable},
    {ErrorCode::kFeedDisconnected, "FEED_DISCONNECTED", Recovery::Retry,
     absl::StatusCode::kUnavailable},
    {ErrorCode::kReplayFileNotFound, "REPLAY_FILE_NOT_FOUND", Recovery::Halt,
     absl::StatusCode::kNotFound},
    {ErrorCode::kReplayFileInvalid, "REPLAY_FILE_INVALID", Recovery::Halt,
     absl::StatusCode::kInvalidArgument},
    {ErrorCode::kOrderInvalid, "ORDER_INVALID", Recovery::Reject,
     absl::StatusCode::kInvalidArgument},
    {ErrorCode::kOrderRuleViolation, "ORDER_RULE_VIOLATION", Recovery::Reject,
     absl::StatusCode::kFailedPrecondition},
    {ErrorCode::kOrderDuplicate, "ORDER_DUPLICATE", Recovery::Reject,
     absl::StatusCode::kAlreadyExists},
    {ErrorCode::kOrderNotFound, "ORDER_NOT_FOUND", Recovery::Reject,
     absl::StatusCode::kNotFound},
    {ErrorCode::kOrderNotCancelable, "ORDER_NOT_CANCELABLE", Recovery::Reject,
     absl::StatusCode::kFailedPrecondition},
    {ErrorCode::kOrderCancelPending, "ORDER_CANCEL_PENDING", Recovery::Reject,
     absl::StatusCode::kAlreadyExists},
    {ErrorCode::kOrderSlotsFull, "ORDER_SLOTS_FULL", Recovery::Reject,
     absl::StatusCode::kResourceExhausted},
    {ErrorCode::kOrderExpiredBeforeSend, "ORDER_EXPIRED_BEFORE_SEND",
     Recovery::Reject, absl::StatusCode::kDeadlineExceeded},
    {ErrorCode::kOrderSubmissionUnknown, "ORDER_SUBMISSION_UNKNOWN",
     Recovery::Reconcile, absl::StatusCode::kUnavailable},
    {ErrorCode::kOrderVenueRejected, "ORDER_VENUE_REJECTED", Recovery::Reject,
     absl::StatusCode::kFailedPrecondition},
    {ErrorCode::kOrderReportInvalid, "ORDER_REPORT_INVALID",
     Recovery::Reconcile, absl::StatusCode::kInvalidArgument},
    {ErrorCode::kOrderReportConflict, "ORDER_REPORT_CONFLICT",
     Recovery::Reconcile, absl::StatusCode::kFailedPrecondition},
    {ErrorCode::kOrderReportUnattributed, "ORDER_REPORT_UNATTRIBUTED",
     Recovery::Reconcile, absl::StatusCode::kNotFound},
    {ErrorCode::kClientIdInvalid, "CLIENT_ID_INVALID", Recovery::Reject,
     absl::StatusCode::kInvalidArgument},
    {ErrorCode::kSigningFailed, "SIGNING_FAILED", Recovery::Halt,
     absl::StatusCode::kInternal},
    {ErrorCode::kOrderRecoveryInvalid, "ORDER_RECOVERY_INVALID",
     Recovery::Reconcile, absl::StatusCode::kFailedPrecondition},
    {ErrorCode::kPaperBalanceInsufficient, "PAPER_BALANCE_INSUFFICIENT",
     Recovery::Reject, absl::StatusCode::kResourceExhausted},
    {ErrorCode::kPaperInputInvalid, "PAPER_INPUT_INVALID", Recovery::Reject,
     absl::StatusCode::kInvalidArgument},
    {ErrorCode::kRiskEmergencyStopped, "RISK_EMERGENCY_STOPPED",
     Recovery::Reject, absl::StatusCode::kFailedPrecondition},
    {ErrorCode::kRiskNotReady, "RISK_NOT_READY", Recovery::Reject,
     absl::StatusCode::kFailedPrecondition},
    {ErrorCode::kRiskLimitExceeded, "RISK_LIMIT_EXCEEDED", Recovery::Reject,
     absl::StatusCode::kResourceExhausted},
    {ErrorCode::kRiskLeaseExhausted, "RISK_LEASE_EXHAUSTED", Recovery::Reject,
     absl::StatusCode::kResourceExhausted},
    {ErrorCode::kRiskLeaseMissing, "RISK_LEASE_MISSING", Recovery::Reject,
     absl::StatusCode::kFailedPrecondition},
    {ErrorCode::kRiskLeaseStale, "RISK_LEASE_STALE", Recovery::Reject,
     absl::StatusCode::kFailedPrecondition},
    {ErrorCode::kRiskConfigInvalid, "RISK_CONFIG_INVALID", Recovery::Halt,
     absl::StatusCode::kInvalidArgument},
    {ErrorCode::kRiskReservationInvalid, "RISK_RESERVATION_INVALID",
     Recovery::Halt, absl::StatusCode::kFailedPrecondition},
    {ErrorCode::kAccountMessageInvalid, "ACCOUNT_MESSAGE_INVALID",
     Recovery::Reconcile, absl::StatusCode::kInvalidArgument},
    {ErrorCode::kAccountEventUnsupported, "ACCOUNT_EVENT_UNSUPPORTED",
     Recovery::Reject, absl::StatusCode::kUnimplemented},
    {ErrorCode::kReconcileResponseInvalid, "RECONCILE_RESPONSE_INVALID",
     Recovery::Reconcile, absl::StatusCode::kInvalidArgument},
    {ErrorCode::kReconcileIdentityMismatch, "RECONCILE_IDENTITY_MISMATCH",
     Recovery::Reconcile, absl::StatusCode::kFailedPrecondition},
    {ErrorCode::kReconcileQueryFailed, "RECONCILE_QUERY_FAILED",
     Recovery::Retry, absl::StatusCode::kUnavailable},
    {ErrorCode::kReconcileResponseTooLarge, "RECONCILE_RESPONSE_TOO_LARGE",
     Recovery::Reconcile, absl::StatusCode::kResourceExhausted},
    {ErrorCode::kReconcileTargetInvalid, "RECONCILE_TARGET_INVALID",
     Recovery::Halt, absl::StatusCode::kInvalidArgument},
    {ErrorCode::kRouteOwnerUnknown, "ROUTE_OWNER_UNKNOWN", Recovery::Reconcile,
     absl::StatusCode::kNotFound},
    {ErrorCode::kRouteOwnershipConflict, "ROUTE_OWNERSHIP_CONFLICT",
     Recovery::Reconcile, absl::StatusCode::kFailedPrecondition},
    {ErrorCode::kRouteReportInvalid, "ROUTE_REPORT_INVALID",
     Recovery::Reconcile, absl::StatusCode::kInvalidArgument},
    {ErrorCode::kRouteQueueFull, "ROUTE_QUEUE_FULL", Recovery::Degrade,
     absl::StatusCode::kResourceExhausted},
    {ErrorCode::kRouteConfigInvalid, "ROUTE_CONFIG_INVALID", Recovery::Halt,
     absl::StatusCode::kInvalidArgument},
    {ErrorCode::kShardInputInvalid, "SHARD_INPUT_INVALID", Recovery::Reject,
     absl::StatusCode::kInvalidArgument},
    {ErrorCode::kShardReportUnattributed, "SHARD_REPORT_UNATTRIBUTED",
     Recovery::Halt, absl::StatusCode::kFailedPrecondition},
    {ErrorCode::kStrategyCallbackTimeout, "STRATEGY_CALLBACK_TIMEOUT",
     Recovery::Degrade, absl::StatusCode::kDeadlineExceeded},
    {ErrorCode::kStrategyDependencyNotReady, "STRATEGY_DEPENDENCY_NOT_READY",
     Recovery::Reject, absl::StatusCode::kFailedPrecondition},
    {ErrorCode::kStorageOptionsInvalid, "STORAGE_OPTIONS_INVALID",
     Recovery::Halt, absl::StatusCode::kInvalidArgument},
    {ErrorCode::kStorageOpenFailed, "STORAGE_OPEN_FAILED", Recovery::Degrade,
     absl::StatusCode::kUnavailable},
    {ErrorCode::kStorageWriteFailed, "STORAGE_WRITE_FAILED", Recovery::Degrade,
     absl::StatusCode::kUnavailable},
    {ErrorCode::kStorageQueueFull, "STORAGE_QUEUE_FULL", Recovery::Degrade,
     absl::StatusCode::kResourceExhausted},
    {ErrorCode::kStorageGapPersistFailed, "STORAGE_GAP_PERSIST_FAILED",
     Recovery::Degrade, absl::StatusCode::kUnavailable},
    {ErrorCode::kStorageQueryFailed, "STORAGE_QUERY_FAILED", Recovery::Reject,
     absl::StatusCode::kInternal},
    {ErrorCode::kHistoryBusy, "HISTORY_BUSY", Recovery::Reject,
     absl::StatusCode::kResourceExhausted},
    {ErrorCode::kHistoryStopping, "HISTORY_STOPPING", Recovery::Reject,
     absl::StatusCode::kFailedPrecondition},
    {ErrorCode::kHistoryDeadline, "HISTORY_DEADLINE", Recovery::Reject,
     absl::StatusCode::kDeadlineExceeded},
    {ErrorCode::kHistoryCursorInvalid, "HISTORY_CURSOR_INVALID",
     Recovery::Reject, absl::StatusCode::kInvalidArgument},
    {ErrorCode::kHistoryTooManyGaps, "HISTORY_TOO_MANY_GAPS", Recovery::Reject,
     absl::StatusCode::kResourceExhausted},
    {ErrorCode::kRecoveryManifestNotFound, "RECOVERY_MANIFEST_NOT_FOUND",
     Recovery::Reconcile, absl::StatusCode::kNotFound},
    {ErrorCode::kRecoveryDataCorrupted, "RECOVERY_DATA_CORRUPTED",
     Recovery::Reconcile, absl::StatusCode::kDataLoss},
    {ErrorCode::kRecordEncodingInvalid, "RECORD_ENCODING_INVALID",
     Recovery::Reconcile, absl::StatusCode::kDataLoss},
    {ErrorCode::kConfigFileNotFound, "CONFIG_FILE_NOT_FOUND", Recovery::Halt,
     absl::StatusCode::kNotFound},
    {ErrorCode::kConfigSyntaxInvalid, "CONFIG_SYNTAX_INVALID", Recovery::Halt,
     absl::StatusCode::kInvalidArgument},
    {ErrorCode::kConfigSchemaUnsupported, "CONFIG_SCHEMA_UNSUPPORTED",
     Recovery::Halt, absl::StatusCode::kInvalidArgument},
    {ErrorCode::kConfigFieldInvalid, "CONFIG_FIELD_INVALID", Recovery::Halt,
     absl::StatusCode::kInvalidArgument},
    {ErrorCode::kConfigReferenceInvalid, "CONFIG_REFERENCE_INVALID",
     Recovery::Halt, absl::StatusCode::kInvalidArgument},
    {ErrorCode::kConfigAssignmentInvalid, "CONFIG_ASSIGNMENT_INVALID",
     Recovery::Halt, absl::StatusCode::kInvalidArgument},
    {ErrorCode::kConfigModeNotAllowed, "CONFIG_MODE_NOT_ALLOWED",
     Recovery::Halt, absl::StatusCode::kFailedPrecondition},
    {ErrorCode::kConfigCredentialsInline, "CONFIG_CREDENTIALS_INLINE",
     Recovery::Halt, absl::StatusCode::kInvalidArgument},
    {ErrorCode::kConfigLeaseInvalid, "CONFIG_LEASE_INVALID", Recovery::Halt,
     absl::StatusCode::kInvalidArgument},
    {ErrorCode::kLaunchUnsupportedTopology, "LAUNCH_UNSUPPORTED_TOPOLOGY",
     Recovery::Halt, absl::StatusCode::kFailedPrecondition},
    {ErrorCode::kStateDirUnavailable, "STATE_DIR_UNAVAILABLE", Recovery::Halt,
     absl::StatusCode::kUnavailable},
    {ErrorCode::kControlMessageInvalid, "CONTROL_MESSAGE_INVALID",
     Recovery::Reject, absl::StatusCode::kInvalidArgument},
    {ErrorCode::kControlSocketFailed, "CONTROL_SOCKET_FAILED", Recovery::Halt,
     absl::StatusCode::kUnavailable},
    {ErrorCode::kControlSocketInUse, "CONTROL_SOCKET_IN_USE", Recovery::Halt,
     absl::StatusCode::kAlreadyExists},
    {ErrorCode::kCliUsageInvalid, "CLI_USAGE_INVALID", Recovery::Reject,
     absl::StatusCode::kInvalidArgument},
    {ErrorCode::kCliEngineUnreachable, "CLI_ENGINE_UNREACHABLE",
     Recovery::Retry, absl::StatusCode::kUnavailable},
    {ErrorCode::kCliResponseInvalid, "CLI_RESPONSE_INVALID", Recovery::Reject,
     absl::StatusCode::kDataLoss},
    {ErrorCode::kCliCommandFailed, "CLI_COMMAND_FAILED", Recovery::Reject,
     absl::StatusCode::kFailedPrecondition},
    {ErrorCode::kControlBusy, "CONTROL_BUSY", Recovery::Retry,
     absl::StatusCode::kResourceExhausted},
    {ErrorCode::kControlTimeout, "CONTROL_TIMEOUT", Recovery::Retry,
     absl::StatusCode::kDeadlineExceeded},
}};

const ErrorInfo& InternalInfo() {
  for (const auto& item : kErrors) {
    if (item.code == ErrorCode::kInternal) return item;
  }
  return kOkInfo;  // Unreachable: the registry includes INTERNAL.
}

absl::Status MakeError(ErrorCode code, absl::StatusCode canonical,
                       std::string_view message) {
  absl::Status status(canonical, std::string(message));
  status.SetPayload(kPayloadType,
                    absl::Cord(std::to_string(static_cast<uint16_t>(code))));
  return status;
}

}  // namespace

const ErrorInfo& Info(ErrorCode code) {
  if (code == ErrorCode::kOk) return kOkInfo;
  for (const auto& item : kErrors) {
    if (item.code == code) return item;
  }
  return InternalInfo();
}

std::span<const ErrorInfo> AllErrorInfo() { return kErrors; }

absl::Status Error(ErrorCode code, std::string_view message) {
  if (code == ErrorCode::kOk) return absl::OkStatus();
  const auto& info = Info(code);
  return MakeError(info.code, info.canonical, message);
}

ErrorCode CodeOf(const absl::Status& status) {
  if (status.ok()) return ErrorCode::kOk;
  auto payload = status.GetPayload(kPayloadType);
  if (!payload) return ErrorCode::kInternal;
  const std::string_view encoded = payload->Flatten();
  if (encoded.empty()) return ErrorCode::kInternal;
  uint16_t number = 0;
  const auto [end, ec] =
      std::from_chars(encoded.data(), encoded.data() + encoded.size(), number);
  if (ec != std::errc{} || end != encoded.data() + encoded.size() ||
      number == 0)
    return ErrorCode::kInternal;
  const auto code = static_cast<ErrorCode>(number);
  return Info(code).code == code ? code : ErrorCode::kInternal;
}

Recovery RecoveryOf(const absl::Status& status) {
  return Info(CodeOf(status)).recovery;
}

absl::Status ErrorFromSystem(ErrorCode code, std::error_code ec,
                             std::string_view context) {
  std::string message(context);
  if (ec) {
    if (!message.empty()) message += ": ";
    message += ec.message();
  }
  if (code == ErrorCode::kOk) return absl::OkStatus();
  const auto& info = Info(code);
  auto canonical = info.canonical;
  if (info.code == ErrorCode::kStateDirUnavailable) {
    if (ec == std::errc::permission_denied)
      canonical = absl::StatusCode::kPermissionDenied;
    else if (ec == std::errc::no_such_file_or_directory)
      canonical = absl::StatusCode::kNotFound;
    else if (ec == std::errc::file_exists)
      canonical = absl::StatusCode::kAlreadyExists;
    else if (ec == std::errc::no_space_on_device)
      canonical = absl::StatusCode::kResourceExhausted;
  }
  return MakeError(info.code, canonical, message);
}

}  // namespace hquant
