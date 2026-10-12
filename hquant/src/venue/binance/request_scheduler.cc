#include "hquant/venue/binance/request_scheduler.h"

#include <algorithm>

namespace hquant::v1 {

RequestScheduler::RequestScheduler(uint64_t weight_per_minute,
                                   uint64_t orders_per_ten_seconds)
    : weight_limit_(weight_per_minute),
      order_limit_(orders_per_ten_seconds) {}

uint64_t RequestScheduler::Weight(RequestClass kind) {
  switch (kind) {
    case RequestClass::OpenOrders: return 80;
    case RequestClass::Balances: return 20;
    case RequestClass::TradeQuery: return 20;
    case RequestClass::Rules: return 20;
    case RequestClass::OrderQuery: return 4;
    case RequestClass::Submit:
    case RequestClass::Cancel: return 1;
  }
  return 1;
}

std::chrono::steady_clock::duration RequestScheduler::Delay(
    RequestClass kind, std::chrono::steady_clock::time_point now) {
  std::lock_guard lock(mutex_);
  return DelayLocked(kind, now);
}

std::chrono::steady_clock::duration RequestScheduler::DelayLocked(
    RequestClass kind, std::chrono::steady_clock::time_point now) {
  if (weight_window_.time_since_epoch().count() == 0 ||
      now - weight_window_ >= std::chrono::minutes(1)) {
    weight_window_ = now;
    used_weight_ = 0;
  }
  if (order_window_.time_since_epoch().count() == 0 ||
      now - order_window_ >= std::chrono::seconds(10)) {
    order_window_ = now;
    placed_orders_ = 0;
  }
  auto until = blocked_until_;
  if (weight_limit_ != 0 &&
      (Weight(kind) > weight_limit_ ||
       used_weight_ > weight_limit_ - Weight(kind))) {
    until = std::max(until, weight_window_ + std::chrono::minutes(1));
  }
  if (kind == RequestClass::Submit && order_limit_ != 0 &&
      placed_orders_ >= order_limit_) {
    until = std::max(until, order_window_ + std::chrono::seconds(10));
  }
  return until > now ? until - now : std::chrono::steady_clock::duration::zero();
}

std::chrono::steady_clock::duration RequestScheduler::Reserve(
    RequestClass kind, std::chrono::steady_clock::time_point now) {
  std::lock_guard lock(mutex_);
  const auto delay = DelayLocked(kind, now);
  if (delay > std::chrono::steady_clock::duration::zero()) return delay;
  used_weight_ += Weight(kind);
  if (kind == RequestClass::Submit) ++placed_orders_;
  return std::chrono::steady_clock::duration::zero();
}

void RequestScheduler::Observe(const HttpResponse& response,
                               std::chrono::steady_clock::time_point now) {
  if (response.status != 429 && response.status != 418) return;
  std::lock_guard lock(mutex_);
  const auto fallback = response.status == 418 ? std::chrono::minutes(2)
                                                : std::chrono::seconds(1);
  const auto delay = response.retry_after > std::chrono::steady_clock::duration::zero()
                         ? response.retry_after
                         : fallback;
  blocked_until_ = std::max(blocked_until_, now + delay);
}

}  // namespace hquant::v1
