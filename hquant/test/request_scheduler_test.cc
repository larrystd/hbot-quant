#include <chrono>
#include <atomic>
#include <cstdlib>
#include <iostream>
#include <thread>

#include "hquant/venue/binance/request_scheduler.h"

namespace {

using namespace hquant::v1;
using namespace std::chrono_literals;

void Require(bool condition, const char* message) {
  if (condition) return;
  std::cerr << message << '\n';
  std::exit(1);
}

}  // namespace

int main() {
  const auto now = std::chrono::steady_clock::now();
  RequestScheduler scheduler(100, 1);
  Require(scheduler.Reserve(RequestClass::Submit, now) == 0s,
          "first order blocked");
  Require(scheduler.Delay(RequestClass::Submit, now + 1ms) > 9s,
          "order count limit ignored");
  Require(scheduler.Reserve(RequestClass::Cancel, now + 1ms) == 0s,
          "cancel was blocked by order count limit");
  Require(scheduler.Reserve(RequestClass::OpenOrders, now + 2ms) == 0s,
          "first open orders query blocked");
  Require(scheduler.Delay(RequestClass::OpenOrders, now + 3ms) > 59s,
          "request weight limit ignored");
  HttpResponse limited;
  limited.status = 429;
  limited.retry_after = 3s;
  scheduler.Observe(limited, now + 61s);
  Require(scheduler.Delay(RequestClass::Cancel, now + 61s) >= 3s,
          "Retry-After ignored for cancel requests");
  Require(scheduler.Delay(RequestClass::Cancel, now + 65s) == 0s,
          "Retry-After did not expire");

  RequestScheduler shared(0, 1);
  std::atomic<bool> start = false;
  std::atomic<int> accepted = 0;
  const auto reserve = [&] {
    while (!start.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
    if (shared.Reserve(RequestClass::Submit, now) == 0s) ++accepted;
  };
  std::thread first(reserve);
  std::thread second(reserve);
  start.store(true, std::memory_order_release);
  first.join();
  second.join();
  Require(accepted == 1,
          "shared request budget admitted two simultaneous orders");
}
