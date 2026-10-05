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

// Binance 现货实盘的下单出口：OrderGateway 接口（base/order.h）的实盘实现，
// 以及它用到的工具（订单号编码、请求签名、网关事件）。
//
// 现状：运行时没有任何地方创建它，只有 order_gateway_test、recovery_test 使用。
// 实盘目前被有意关闭（配置 mode: live 会报错，QuantServer 只创建模拟盘）。
// 将来实盘时，由 QuantServer 创建它并交给 LiveTrading（shard/live_trading.h），
// 它的事件回调转成回报交给 Shard。模拟盘下单走模拟交易所，不经过这里。

namespace hquant::binance_spot {

// 解析本地订单号得到的内容。
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

// ---- 请求签名：Binance 的私有接口要求参数按规则编码后做 HMAC-SHA256 签名 ----
using QueryParameters = std::vector<std::pair<std::string, std::string>>;

// Percent-encode before signing, then send these exact bytes on the wire.
absl::StatusOr<std::string> CanonicalQuery(const QueryParameters& parameters);
absl::StatusOr<std::string> HmacSha256Hex(std::string_view secret,
                                          std::string_view payload);
absl::StatusOr<std::string> SignedQuery(const QueryParameters& parameters,
                                        std::string_view secret);

}  // namespace hquant::binance_spot

namespace hquant::binance_spot {

// ---- 网关事件：一次下单或撤单请求的发送结果，通过回调交给拥有者 ----
//   SubmitConfirmed /
//   CancelConfirmed：交易所接受了请求（下单时带解析出的回报）； SubmitRejected
//   / CancelRejected：交易所明确拒绝（下单时带一条 Rejected 回报）；
//   SubmissionUnknown / CancelUnknown：网络出错、超时或 5xx，不知道交易所是否
//       收到，要靠查询确认；
//   BeforeWriteFailed：还没发出就失败了（例如排队时过期），能证明交易所没收到。
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

// 网关配置：账户、交易对、下单规则、本地订单号的组成部分（运行编号、分片）、
// 密钥、超时、同时在途的请求上限、限流。
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

// 由分片拥有，只在分片线程上调用。它自己不建网络连接，HTTP 由注入的 transport
// 发送（必须绑定在同一个 io_context 上）。
// 流程：PrepareSubmit 取整、检查、生成订单号（不发送）→ StartPrepared 签名并
// 放进发送队列 → ProcessQueue 协程逐个发出（撤单优先）→ 按响应发出网关事件。
class BinanceOrderGateway final : public OrderGateway {
 public:
  using EventHandler = std::function<void(GatewayEvent)>;

  BinanceOrderGateway(boost::asio::io_context& io, HttpTransport& transport,
                      const Clock& clock, BinanceGatewayConfig config,
                      EventHandler on_event);

  absl::StatusOr<Order> PrepareSubmit(const SubmitOrder& request,
                                      const StrategyId& strategy_id,
                                      MonoTime expires_at_mono) override;
  absl::Status StartPrepared(const ClientOrderId& client_order_id) override;
  absl::Status AbortPrepared(const ClientOrderId& client_order_id) override;
  absl::Status StartCancel(const ClientOrderId& client_order_id) override;

  // Recovery loads historical IDs before order creation; a repeated run ID is
  // refused even if its next local sequence has not yet collided.
  absl::Status ObserveHistoricalClientId(const ClientOrderId& client_order_id);
  absl::Status RestoreOrder(Order order);
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
    Order order;
    MonoTime expires_at_mono{};
    State state = State::Prepared;
  };
  struct Work {
    ClientOrderId client_order_id;
    HttpRequest request;
    bool cancel = false;
    MonoTime expires_at_mono{};
  };

  absl::Status ValidateAndQuantize(SubmitOrder* request,
                                   const StrategyId& strategy_id,
                                   MonoTime expires_at_mono) const;
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
