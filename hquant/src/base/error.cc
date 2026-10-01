#include "base/error.h"

#include <array>
#include <charconv>
#include <limits>
#include <string>
#include <system_error>

#include "absl/strings/cord.h"

namespace hquant {
namespace {

constexpr std::string_view kPayloadType = "type.hquant/error";
constexpr ErrorInfo kOkInfo{ErrorCode::kOk, "OK", Recovery::Reject,
                            absl::StatusCode::kOk};
constexpr std::array<ErrorInfo, 125> kErrors{{
    {ErrorCode::kDecimalInvalid, "DECIMAL_INVALID", Recovery::Reject,
     absl::StatusCode::kInvalidArgument},
    {ErrorCode::kDecimalPrecisionExceeded, "DECIMAL_PRECISION_EXCEEDED",
     Recovery::Reject, absl::StatusCode::kOutOfRange},
    {ErrorCode::kSequenceExhausted, "SEQUENCE_EXHAUSTED", Recovery::Halt,
     absl::StatusCode::kResourceExhausted},
    {ErrorCode::kNetCancelled, "NET_CANCELLED", Recovery::Reject,
     absl::StatusCode::kCancelled},
    {ErrorCode::kDecimalArithmeticFailed, "DECIMAL_ARITHMETIC_FAILED",
     Recovery::Halt, absl::StatusCode::kOutOfRange},
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
    {ErrorCode::kNetConcurrentCall, "NET_CONCURRENT_CALL", Recovery::Halt,
     absl::StatusCode::kFailedPrecondition},
    {ErrorCode::kNetTlsVerifyFailed, "NET_TLS_VERIFY_FAILED", Recovery::Halt,
     absl::StatusCode::kUnavailable},
    {ErrorCode::kFeedSnapshotHttpError, "FEED_SNAPSHOT_HTTP_ERROR",
     Recovery::Retry, absl::StatusCode::kUnavailable},
    {ErrorCode::kExchangeRateLimited, "EXCHANGE_RATE_LIMITED", Recovery::Retry,
     absl::StatusCode::kResourceExhausted},
    {ErrorCode::kExchangeIpBanned, "EXCHANGE_IP_BANNED", Recovery::Halt,
     absl::StatusCode::kResourceExhausted},
    {ErrorCode::kRateBreakerOpen, "RATE_BREAKER_OPEN", Recovery::Reject,
     absl::StatusCode::kUnavailable},
    {ErrorCode::kRateBudgetMissing, "RATE_BUDGET_MISSING", Recovery::Reject,
     absl::StatusCode::kFailedPrecondition},
    {ErrorCode::kRateBudgetExhausted, "RATE_BUDGET_EXHAUSTED", Recovery::Reject,
     absl::StatusCode::kResourceExhausted},
    {ErrorCode::kRateConfigInvalid, "RATE_CONFIG_INVALID", Recovery::Halt,
     absl::StatusCode::kInvalidArgument},
    {ErrorCode::kNetNotConnected, "NET_NOT_CONNECTED", Recovery::Retry,
     absl::StatusCode::kUnavailable},
    {ErrorCode::kFeedConfigInvalid, "FEED_CONFIG_INVALID", Recovery::Halt,
     absl::StatusCode::kInvalidArgument},
    {ErrorCode::kFeedMessageInvalid, "FEED_MESSAGE_INVALID", Recovery::Resync,
     absl::StatusCode::kInvalidArgument},
    {ErrorCode::kFeedTickSizeMismatch, "FEED_TICK_SIZE_MISMATCH",
     Recovery::Resync, absl::StatusCode::kOutOfRange},
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
    {ErrorCode::kFeedStopped, "FEED_STOPPED", Recovery::Reject,
     absl::StatusCode::kCancelled},
    {ErrorCode::kPublicTradeInvalid, "PUBLIC_TRADE_INVALID", Recovery::Reject,
     absl::StatusCode::kInvalidArgument},
    {ErrorCode::kOrderDuplicate, "ORDER_DUPLICATE", Recovery::Reject,
     absl::StatusCode::kAlreadyExists},
    {ErrorCode::kOrderNotFound, "ORDER_NOT_FOUND", Recovery::Reject,
     absl::StatusCode::kNotFound},
    {ErrorCode::kOrderNotCancelable, "ORDER_NOT_CANCELABLE", Recovery::Reject,
     absl::StatusCode::kFailedPrecondition},
    {ErrorCode::kOrderCancelPending, "ORDER_CANCEL_PENDING", Recovery::Reject,
     absl::StatusCode::kAlreadyExists},
    {ErrorCode::kOrderSendQueueFull, "ORDER_SEND_QUEUE_FULL", Recovery::Reject,
     absl::StatusCode::kResourceExhausted},
    {ErrorCode::kOrderExpiredBeforeSend, "ORDER_EXPIRED_BEFORE_SEND",
     Recovery::Reject, absl::StatusCode::kDeadlineExceeded},
    {ErrorCode::kOrderSubmissionUnknown, "ORDER_SUBMISSION_UNKNOWN",
     Recovery::QueryOrder, absl::StatusCode::kUnavailable},
    {ErrorCode::kOrderRejectedByExchange, "ORDER_REJECTED_BY_EXCHANGE",
     Recovery::Reject, absl::StatusCode::kFailedPrecondition},
    {ErrorCode::kOrderReportInvalid, "ORDER_REPORT_INVALID",
     Recovery::QueryOrder, absl::StatusCode::kInvalidArgument},
    {ErrorCode::kReportOrderUnknown, "REPORT_ORDER_UNKNOWN",
     Recovery::QueryOrder, absl::StatusCode::kNotFound},
    {ErrorCode::kClientOrderIdInvalid, "CLIENT_ORDER_ID_INVALID",
     Recovery::Reject, absl::StatusCode::kInvalidArgument},
    {ErrorCode::kSigningFailed, "SIGNING_FAILED", Recovery::Halt,
     absl::StatusCode::kInternal},
    {ErrorCode::kOrderRecoveryInvalid, "ORDER_RECOVERY_INVALID",
     Recovery::QueryOrder, absl::StatusCode::kFailedPrecondition},
    {ErrorCode::kSimulatedBalanceInsufficient, "SIMULATED_BALANCE_INSUFFICIENT",
     Recovery::Reject, absl::StatusCode::kResourceExhausted},
    {ErrorCode::kSimulatedMarketDataInvalid, "SIMULATED_MARKET_DATA_INVALID",
     Recovery::Reject, absl::StatusCode::kInvalidArgument},
    {ErrorCode::kOrderStrategyIdInvalid, "ORDER_STRATEGY_ID_INVALID",
     Recovery::Reject, absl::StatusCode::kInvalidArgument},
    {ErrorCode::kOrderAccountInvalid, "ORDER_ACCOUNT_INVALID", Recovery::Reject,
     absl::StatusCode::kInvalidArgument},
    {ErrorCode::kOrderMarketInvalid, "ORDER_MARKET_INVALID", Recovery::Reject,
     absl::StatusCode::kInvalidArgument},
    {ErrorCode::kOrderTypeUnsupported, "ORDER_TYPE_UNSUPPORTED",
     Recovery::Reject, absl::StatusCode::kInvalidArgument},
    {ErrorCode::kOrderPriceOrAmountInvalid, "ORDER_PRICE_OR_AMOUNT_INVALID",
     Recovery::Reject, absl::StatusCode::kInvalidArgument},
    {ErrorCode::kOrderBelowMinAmount, "ORDER_BELOW_MIN_AMOUNT",
     Recovery::Reject, absl::StatusCode::kFailedPrecondition},
    {ErrorCode::kOrderBelowMinNotional, "ORDER_BELOW_MIN_NOTIONAL",
     Recovery::Reject, absl::StatusCode::kFailedPrecondition},
    {ErrorCode::kOrderAboveMaxAmount, "ORDER_ABOVE_MAX_AMOUNT",
     Recovery::Reject, absl::StatusCode::kFailedPrecondition},
    {ErrorCode::kOrderNotOnTick, "ORDER_NOT_ON_TICK", Recovery::Reject,
     absl::StatusCode::kFailedPrecondition},
    {ErrorCode::kExchangeOrderIdConflict, "EXCHANGE_ORDER_ID_CONFLICT",
     Recovery::QueryOrder, absl::StatusCode::kFailedPrecondition},
    {ErrorCode::kReportAccountMarketMismatch, "REPORT_ACCOUNT_MARKET_MISMATCH",
     Recovery::QueryOrder, absl::StatusCode::kFailedPrecondition},
    {ErrorCode::kOrderStatusConflict, "ORDER_STATUS_CONFLICT",
     Recovery::QueryOrder, absl::StatusCode::kFailedPrecondition},
    {ErrorCode::kTradeIdOnOtherOrder, "TRADE_ID_ON_OTHER_ORDER",
     Recovery::QueryOrder, absl::StatusCode::kFailedPrecondition},
    {ErrorCode::kTradeExceedsOrderAmount, "TRADE_EXCEEDS_ORDER_AMOUNT",
     Recovery::QueryOrder, absl::StatusCode::kFailedPrecondition},
    {ErrorCode::kGatewayConfigInvalid, "GATEWAY_CONFIG_INVALID", Recovery::Halt,
     absl::StatusCode::kInvalidArgument},
    {ErrorCode::kRiskEmergencyStopped, "RISK_EMERGENCY_STOPPED",
     Recovery::Reject, absl::StatusCode::kFailedPrecondition},
    {ErrorCode::kRiskBudgetExhausted, "RISK_BUDGET_EXHAUSTED", Recovery::Reject,
     absl::StatusCode::kResourceExhausted},
    {ErrorCode::kRiskBudgetMissing, "RISK_BUDGET_MISSING", Recovery::Reject,
     absl::StatusCode::kFailedPrecondition},
    {ErrorCode::kRiskConfigInvalid, "RISK_CONFIG_INVALID", Recovery::Halt,
     absl::StatusCode::kInvalidArgument},
    {ErrorCode::kRiskMarketNotLive, "RISK_MARKET_NOT_LIVE", Recovery::Reject,
     absl::StatusCode::kFailedPrecondition},
    {ErrorCode::kRiskAccountStale, "RISK_ACCOUNT_STALE", Recovery::Reject,
     absl::StatusCode::kFailedPrecondition},
    {ErrorCode::kRiskTradingRuleStale, "RISK_TRADING_RULE_STALE",
     Recovery::Reject, absl::StatusCode::kFailedPrecondition},
    {ErrorCode::kRiskBudgetExpired, "RISK_BUDGET_EXPIRED", Recovery::Reject,
     absl::StatusCode::kFailedPrecondition},
    {ErrorCode::kRiskBudgetRenewalStale, "RISK_BUDGET_RENEWAL_STALE",
     Recovery::Reject, absl::StatusCode::kFailedPrecondition},
    {ErrorCode::kFundsHoldNotFound, "FUNDS_HOLD_NOT_FOUND", Recovery::Halt,
     absl::StatusCode::kFailedPrecondition},
    {ErrorCode::kFundsHoldAlreadyReleased, "FUNDS_HOLD_ALREADY_RELEASED",
     Recovery::Halt, absl::StatusCode::kFailedPrecondition},
    {ErrorCode::kFundsHoldIncreased, "FUNDS_HOLD_INCREASED", Recovery::Halt,
     absl::StatusCode::kFailedPrecondition},
    {ErrorCode::kFundsHoldClientIdConflict, "FUNDS_HOLD_CLIENT_ID_CONFLICT",
     Recovery::Halt, absl::StatusCode::kFailedPrecondition},
    {ErrorCode::kAccountMessageInvalid, "ACCOUNT_MESSAGE_INVALID",
     Recovery::QueryOrder, absl::StatusCode::kInvalidArgument},
    {ErrorCode::kAccountEventUnsupported, "ACCOUNT_EVENT_UNSUPPORTED",
     Recovery::Reject, absl::StatusCode::kUnimplemented},
    {ErrorCode::kOrderQueryResponseInvalid, "ORDER_QUERY_RESPONSE_INVALID",
     Recovery::QueryOrder, absl::StatusCode::kInvalidArgument},
    {ErrorCode::kOrderQueryIdentityMismatch, "ORDER_QUERY_IDENTITY_MISMATCH",
     Recovery::QueryOrder, absl::StatusCode::kFailedPrecondition},
    {ErrorCode::kOrderQueryFailed, "ORDER_QUERY_FAILED",
     Recovery::Retry, absl::StatusCode::kUnavailable},
    {ErrorCode::kOrderQueryResponseTooLarge, "ORDER_QUERY_RESPONSE_TOO_LARGE",
     Recovery::QueryOrder, absl::StatusCode::kResourceExhausted},
    {ErrorCode::kOrderQueryTargetInvalid, "ORDER_QUERY_TARGET_INVALID",
     Recovery::Halt, absl::StatusCode::kInvalidArgument},
    {ErrorCode::kRouteStrategyUnknown, "ROUTE_STRATEGY_UNKNOWN",
     Recovery::QueryOrder, absl::StatusCode::kNotFound},
    {ErrorCode::kRouteStrategyConflict, "ROUTE_STRATEGY_CONFLICT",
     Recovery::QueryOrder, absl::StatusCode::kFailedPrecondition},
    {ErrorCode::kRouteReportInvalid, "ROUTE_REPORT_INVALID",
     Recovery::QueryOrder, absl::StatusCode::kInvalidArgument},
    {ErrorCode::kRouteQueueFull, "ROUTE_QUEUE_FULL", Recovery::Degrade,
     absl::StatusCode::kResourceExhausted},
    {ErrorCode::kRouteConfigInvalid, "ROUTE_CONFIG_INVALID", Recovery::Halt,
     absl::StatusCode::kInvalidArgument},
    {ErrorCode::kShardReportOrderUnknown, "SHARD_REPORT_ORDER_UNKNOWN",
     Recovery::Halt, absl::StatusCode::kFailedPrecondition},
    {ErrorCode::kInputTimeInvalid, "INPUT_TIME_INVALID", Recovery::Reject,
     absl::StatusCode::kInvalidArgument},
    {ErrorCode::kOrderHistoryConfigInvalid, "ORDER_HISTORY_CONFIG_INVALID", Recovery::Halt,
     absl::StatusCode::kInvalidArgument},
    {ErrorCode::kOrderHistoryOpenFailed, "ORDER_HISTORY_OPEN_FAILED", Recovery::Degrade,
     absl::StatusCode::kUnavailable},
    {ErrorCode::kOrderHistoryWriteFailed, "ORDER_HISTORY_WRITE_FAILED", Recovery::Degrade,
     absl::StatusCode::kUnavailable},
    {ErrorCode::kOrderHistoryQueueFull, "ORDER_HISTORY_QUEUE_FULL", Recovery::Degrade,
     absl::StatusCode::kResourceExhausted},
    {ErrorCode::kOrderHistoryGapPersistFailed, "ORDER_HISTORY_GAP_PERSIST_FAILED",
     Recovery::Degrade, absl::StatusCode::kUnavailable},
    {ErrorCode::kOrderHistoryQueryFailed, "ORDER_HISTORY_QUERY_FAILED", Recovery::Reject,
     absl::StatusCode::kInternal},
    {ErrorCode::kOrderHistoryReaderQueueFull, "ORDER_HISTORY_READER_QUEUE_FULL", Recovery::Reject,
     absl::StatusCode::kResourceExhausted},
    {ErrorCode::kOrderHistoryReaderStopping, "ORDER_HISTORY_READER_STOPPING",
     Recovery::Reject, absl::StatusCode::kFailedPrecondition},
    {ErrorCode::kOrderHistoryQueryTimeout, "ORDER_HISTORY_QUERY_TIMEOUT", Recovery::Reject,
     absl::StatusCode::kDeadlineExceeded},
    {ErrorCode::kOrderHistoryCursorInvalid, "ORDER_HISTORY_CURSOR_INVALID",
     Recovery::Reject, absl::StatusCode::kInvalidArgument},
    {ErrorCode::kOrderHistoryTooManyGaps, "ORDER_HISTORY_TOO_MANY_GAPS", Recovery::Reject,
     absl::StatusCode::kResourceExhausted},
    {ErrorCode::kRecoveryManifestNotFound, "RECOVERY_MANIFEST_NOT_FOUND",
     Recovery::QueryOrder, absl::StatusCode::kNotFound},
    {ErrorCode::kRecoveryDataCorrupted, "RECOVERY_DATA_CORRUPTED",
     Recovery::QueryOrder, absl::StatusCode::kDataLoss},
    {ErrorCode::kOrderHistoryRecordCorrupted, "ORDER_HISTORY_RECORD_CORRUPTED",
     Recovery::QueryOrder, absl::StatusCode::kDataLoss},
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
    {ErrorCode::kConfigContainsSecret, "CONFIG_CONTAINS_SECRET", Recovery::Halt,
     absl::StatusCode::kInvalidArgument},
    {ErrorCode::kConfigBudgetInvalid, "CONFIG_BUDGET_INVALID", Recovery::Halt,
     absl::StatusCode::kInvalidArgument},
    {ErrorCode::kLaunchMultipleNotSupported, "LAUNCH_MULTIPLE_NOT_SUPPORTED",
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
    {ErrorCode::kCliStopRejected, "CLI_STOP_REJECTED", Recovery::Reject,
     absl::StatusCode::kFailedPrecondition},
    {ErrorCode::kControlBusy, "CONTROL_BUSY", Recovery::Retry,
     absl::StatusCode::kResourceExhausted},
    {ErrorCode::kControlTimeout, "CONTROL_TIMEOUT", Recovery::Retry,
     absl::StatusCode::kDeadlineExceeded},
    {ErrorCode::kCliEngineError, "CLI_ENGINE_ERROR", Recovery::Reject,
     absl::StatusCode::kFailedPrecondition},
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
                    absl::Cord(std::to_string(ErrorNumber(code))));
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
  int64_t number = 0;
  const auto [end, ec] =
      std::from_chars(encoded.data(), encoded.data() + encoded.size(), number);
  if (ec != std::errc{} || end != encoded.data() + encoded.size())
    return ErrorCode::kInternal;
  return ErrorFromNumber(number).value_or(ErrorCode::kInternal);
}

std::optional<ErrorCode> ErrorFromNumber(int64_t number) {
  if (number >= 0 || number < std::numeric_limits<int32_t>::min())
    return std::nullopt;
  const auto code = static_cast<ErrorCode>(number);
  if (Info(code).code != code) return std::nullopt;
  return code;
}

std::optional<ErrorCode> ErrorFromStoredNumber(uint64_t stored) {
  if (stored == 0 ||
      stored > static_cast<uint64_t>(std::numeric_limits<int32_t>::max()))
    return std::nullopt;
  return ErrorFromNumber(-static_cast<int64_t>(stored));
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
