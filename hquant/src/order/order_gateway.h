#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "base/error.h"
#include "base/market.h"
#include "base/net.h"
#include "base/order.h"
#include "base/rate_limit.h"
#include "base/types.h"
#include "boost/asio.hpp"

namespace hquant::binance_spot {

struct DecodedClientOrderId {
  uint64_t strategy_id = 0;
  RunId run;
  ShardId shard_hint;
  uint32_t shard_sequence = 0;
};

// Candidate Binance Spot codec: H + 10/13/7 RFC4648 base32 digits. The
// strategy_id key is stable; shard_hint is only a uniqueness/routing hint
// within one run. Enable live trading only after the target exchange accepts
// and echoes this form.
absl::StatusOr<ClientOrderId> EncodeClientOrderId(const StrategyId& strategy_id,
                                                  RunId run, ShardId shard,
                                                  uint32_t shard_sequence);
absl::StatusOr<DecodedClientOrderId> DecodeClientOrderId(
    const ClientOrderId& id);

}  // namespace hquant::binance_spot

namespace hquant::binance_spot {

using QueryParameters = std::vector<std::pair<std::string, std::string>>;

// Percent-encode before signing, then send these exact bytes on the wire.
absl::StatusOr<std::string> CanonicalQuery(const QueryParameters& parameters);
absl::StatusOr<std::string> HmacSha256Hex(std::string_view secret,
                                          std::string_view payload);
absl::StatusOr<std::string> SignedQuery(const QueryParameters& parameters,
                                        std::string_view secret);

}  // namespace hquant::binance_spot

namespace hquant::binance_spot {

enum class GatewayEventKind {
  SubmitConfirmed,
  SubmitRejected,
  SubmissionUnknown,
  CancelConfirmed,
  CancelRejected,
  CancelUnknown,
  BeforeWriteFailed
};

struct GatewayEvent {
  GatewayEventKind kind;
  ClientOrderId client_order_id;
  std::optional<OrderUpdate> update;
  std::string detail;
  ErrorCode code = ErrorCode::kOk;
};

struct BinanceGatewayConfig {
  AccountId account;
  MarketId market;
  TradingRule trading_rule;
  RunId run;
  ShardId shard;
  std::string api_key;
  std::string secret_key;
  uint32_t recv_window_ms = 5000;
  std::chrono::milliseconds request_timeout{5000};
  size_t max_pending_submits = 16;
  size_t max_pending_cancels = 16;
  RateLimiter* rate_limiter = nullptr;
  RateLimitKey order_rate_key;
  RateLimitKey cancel_rate_key;
};

// Shard-owned. Transport must be bound to the same io_context and is injected;
// the gateway itself never selects a host or opens a network connection.
class BinanceOrderGateway final : public OrderGateway {
 public:
  using EventHandler = std::function<void(GatewayEvent)>;

  BinanceOrderGateway(boost::asio::io_context& io, HttpTransport& transport,
                      const Clock& clock, BinanceGatewayConfig config,
                      EventHandler on_event);

  absl::StatusOr<PreparedOrder> PrepareSubmit(ApprovedOrder approved) override;
  absl::Status StartPrepared(const ClientOrderId& client_order_id) override;
  absl::Status AbortPrepared(const ClientOrderId& client_order_id) override;
  absl::Status StartCancel(const StrategyId& strategy_id,
                           const ClientOrderId& client_order_id) override;

  // Recovery loads historical IDs before order creation; a repeated run ID is
  // refused even if its next local sequence has not yet collided.
  absl::Status ObserveHistoricalClientId(const ClientOrderId& client_order_id);
  absl::Status RestoreOrder(PreparedOrder prepared);
  size_t PendingSubmitCount() const { return pending_submits_; }
  size_t PendingCancelCount() const { return pending_cancels_; }

 private:
  enum class State {
    Prepared,
    Queued,
    Sending,
    Submitted,
    Unknown,
    Rejected,
    Terminal
  };
  struct KnownOrder {
    PreparedOrder prepared;
    MonoTime expires_at_mono{};
    State state = State::Prepared;
  };
  struct Work {
    ClientOrderId client_order_id;
    HttpRequest request;
    bool cancel = false;
    MonoTime expires_at_mono{};
  };

  absl::Status ValidateAndQuantize(ApprovedOrder* approved) const;
  absl::StatusOr<HttpRequest> MakeSubmitRequest(const KnownOrder& order) const;
  absl::StatusOr<HttpRequest> MakeCancelRequest(const KnownOrder& order) const;
  absl::StatusOr<OrderUpdate> ParseSuccess(const KnownOrder& order,
                                           const HttpResponse& response) const;
  void Pump();
  boost::asio::awaitable<void> ProcessQueue();
  void Emit(GatewayEvent event) const;

  boost::asio::io_context& io_;
  HttpTransport& transport_;
  const Clock& clock_;
  BinanceGatewayConfig config_;
  EventHandler on_event_;
  std::map<std::string, KnownOrder> known_orders_;
  std::set<std::string> historical_ids_;
  std::set<std::string> pending_cancel_ids_;
  std::deque<Work> submits_;
  std::deque<Work> cancels_;
  uint32_t next_sequence_ = 1;
  size_t pending_submits_ = 0;
  size_t pending_cancels_ = 0;
  bool pumping_ = false;
  bool run_collision_ = false;
};

}  // namespace hquant::binance_spot
