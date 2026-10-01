#pragma once

#include <cstdint>
#include <optional>
#include <span>
#include <string_view>
#include <system_error>

#include "absl/status/status.h"

namespace hquant {

// Retired numbers. They may appear in recorded history and must never be
// reassigned: -10005 (Unimplemented), -13001 (OrderInvalid), -13002
// (OrderRuleViolation), -13012 (OrderReportConflict), -14002 (RiskNotReady),
// -14003 (RiskLimitExceeded), -14006 (RiskLeaseStale), -14008
// (RiskReservationInvalid), -16001 (ShardInputInvalid), -16003
// (StrategyCallbackTimeout), -16004 (StrategyDependencyNotReady).
enum class ErrorCode : int32_t {
  kOk = 0,
  kDecimalInvalid = -10001,
  kDecimalPrecisionExceeded = -10002,
  kSequenceExhausted = -10003,
  kNetCancelled = -10004,
  kDecimalArithmeticFailed = -10006,
  kInternal = -10900,
  kNetTlsConfigInvalid = -11001,
  kNetTargetInvalid = -11002,
  kNetTimeout = -11003,
  kNetUnavailable = -11004,
  kNetConcurrentCall = -11005,
  kNetTlsVerifyFailed = -11006,
  kFeedSnapshotHttpError = -11007,
  kExchangeRateLimited = -11008,
  kExchangeIpBanned = -11009,
  kRateBreakerOpen = -11010,
  kRateBudgetMissing = -11011,
  kRateBudgetExhausted = -11012,
  kRateConfigInvalid = -11013,
  kNetNotConnected = -11014,
  kFeedConfigInvalid = -12001,
  kFeedMessageInvalid = -12002,
  kFeedTickSizeMismatch = -12003,
  kFeedWrongMarket = -12004,
  kBookSequenceGap = -12005,
  kBookCrossed = -12006,
  kBookBufferOverflow = -12007,
  kBookStale = -12008,
  kFeedDisconnected = -12009,
  kReplayFileNotFound = -12010,
  kReplayFileInvalid = -12011,
  kFeedStopped = -12012,
  kPublicTradeInvalid = -12013,
  kOrderDuplicate = -13003,
  kOrderNotFound = -13004,
  kOrderNotCancelable = -13005,
  kOrderCancelPending = -13006,
  kOrderSendQueueFull = -13007,
  kOrderExpiredBeforeSend = -13008,
  kOrderSubmissionUnknown = -13009,
  kOrderRejectedByExchange = -13010,
  kOrderReportInvalid = -13011,
  kReportOrderUnknown = -13013,
  kClientOrderIdInvalid = -13014,
  kSigningFailed = -13015,
  kOrderRecoveryInvalid = -13016,
  kSimulatedBalanceInsufficient = -13017,
  kSimulatedMarketDataInvalid = -13018,
  kOrderStrategyIdInvalid = -13019,
  kOrderAccountInvalid = -13020,
  kOrderMarketInvalid = -13021,
  kOrderTypeUnsupported = -13022,
  kOrderPriceOrAmountInvalid = -13023,
  kOrderBelowMinAmount = -13024,
  kOrderBelowMinNotional = -13025,
  kOrderAboveMaxAmount = -13026,
  kOrderNotOnTick = -13027,
  kExchangeOrderIdConflict = -13028,
  kReportAccountMarketMismatch = -13029,
  kOrderStatusConflict = -13030,
  kTradeIdOnOtherOrder = -13031,
  kTradeExceedsOrderAmount = -13032,
  kGatewayConfigInvalid = -13033,
  kRiskEmergencyStopped = -14001,
  kRiskBudgetExhausted = -14004,
  kRiskBudgetMissing = -14005,
  kRiskConfigInvalid = -14007,
  kRiskMarketNotLive = -14009,
  kRiskAccountStale = -14010,
  kRiskTradingRuleStale = -14011,
  kRiskBudgetExpired = -14012,
  kRiskBudgetRenewalStale = -14013,
  kFundsHoldNotFound = -14014,
  kFundsHoldAlreadyReleased = -14015,
  kFundsHoldIncreased = -14016,
  kFundsHoldClientIdConflict = -14017,
  kAccountMessageInvalid = -15001,
  kAccountEventUnsupported = -15002,
  kReconcileResponseInvalid = -15003,
  kReconcileIdentityMismatch = -15004,
  kReconcileQueryFailed = -15005,
  kReconcileResponseTooLarge = -15006,
  kReconcileTargetInvalid = -15007,
  kRouteStrategyUnknown = -15008,
  kRouteStrategyConflict = -15009,
  kRouteReportInvalid = -15010,
  kRouteQueueFull = -15011,
  kRouteConfigInvalid = -15012,
  kShardReportOrderUnknown = -16002,
  kInputTimeInvalid = -16005,
  kStorageConfigInvalid = -17001,
  kStorageOpenFailed = -17002,
  kStorageWriteFailed = -17003,
  kStorageQueueFull = -17004,
  kStorageGapPersistFailed = -17005,
  kStorageQueryFailed = -17006,
  kHistoryQueueFull = -17007,
  kHistoryReaderStopping = -17008,
  kHistoryQueryTimeout = -17009,
  kHistoryCursorInvalid = -17010,
  kHistoryTooManyGaps = -17011,
  kRecoveryManifestNotFound = -17012,
  kRecoveryDataCorrupted = -17013,
  kHistoryRecordCorrupted = -17014,
  kConfigFileNotFound = -18001,
  kConfigSyntaxInvalid = -18002,
  kConfigSchemaUnsupported = -18003,
  kConfigFieldInvalid = -18004,
  kConfigReferenceInvalid = -18005,
  kConfigAssignmentInvalid = -18006,
  kConfigModeNotAllowed = -18007,
  kConfigContainsSecret = -18008,
  kConfigBudgetInvalid = -18009,
  kLaunchMultipleNotSupported = -18010,
  kStateDirUnavailable = -18011,
  kServerMessageInvalid = -19001,
  kServerSocketFailed = -19002,
  kServerSocketInUse = -19003,
  kCliUsageInvalid = -19004,
  kCliEngineUnreachable = -19005,
  kCliResponseInvalid = -19006,
  kCliStopRejected = -19007,
  kServerBusy = -19008,
  kServerTimeout = -19009,
  kCliEngineError = -19010,
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

// The negative number shown to users and sent over the control socket.
inline int32_t ErrorNumber(ErrorCode code) {
  return static_cast<int32_t>(code);
}
// Known code for a number from ErrorNumber; nullopt for kOk or unknown values.
std::optional<ErrorCode> ErrorFromNumber(int64_t number);
// History storage keeps the positive magnitude so rows written before codes
// became negative still decode.
inline uint64_t StoredErrorNumber(ErrorCode code) {
  return static_cast<uint64_t>(-static_cast<int64_t>(code));
}
// Known code for a stored magnitude; nullopt for 0 or unknown values.
std::optional<ErrorCode> ErrorFromStoredNumber(uint64_t stored);
Recovery RecoveryOf(const absl::Status& status);
absl::Status ErrorFromSystem(ErrorCode code, std::error_code ec,
                             std::string_view context);

}  // namespace hquant
