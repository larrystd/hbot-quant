#pragma once

#include <chrono>
#include <cstdint>
#include <mutex>

#include "hquant/base/net.h"

namespace hquant::v1 {

enum class RequestClass { Submit, Cancel, OrderQuery, TradeQuery,
                          Balances, OpenOrders, Rules };

// Share one scheduler among connections using the same outbound IP. A zero
// configured budget leaves that quota to the venue; 429/418 still backs off.
class RequestScheduler {
 public:
  RequestScheduler(uint64_t weight_per_minute = 0,
                   uint64_t orders_per_ten_seconds = 0);

  std::chrono::steady_clock::duration Delay(
      RequestClass kind, std::chrono::steady_clock::time_point now);
  // Atomically checks and reserves request weight and order count.
  std::chrono::steady_clock::duration Reserve(
      RequestClass kind, std::chrono::steady_clock::time_point now);
  void Observe(const HttpResponse& response,
               std::chrono::steady_clock::time_point now);

 private:
  static uint64_t Weight(RequestClass kind);
  std::chrono::steady_clock::duration DelayLocked(
      RequestClass kind, std::chrono::steady_clock::time_point now);

  std::mutex mutex_;
  uint64_t weight_limit_;
  uint64_t order_limit_;
  uint64_t used_weight_ = 0;
  uint64_t placed_orders_ = 0;
  std::chrono::steady_clock::time_point weight_window_;
  std::chrono::steady_clock::time_point order_window_;
  std::chrono::steady_clock::time_point blocked_until_;
};

}  // namespace hquant::v1
