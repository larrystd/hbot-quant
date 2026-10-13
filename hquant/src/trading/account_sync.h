#pragma once

#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "absl/status/statusor.h"
#include "boost/asio/steady_timer.hpp"
#include "hquant/trading/trading_account.h"

namespace hquant::v1 {

// 启动、重连和周期性全账户查询；查询事实仍交给账户先记日志再应用。
class AccountSync {
 public:
  // 借用账户状态；回调在账户 io 上执行，不另起线程。
  struct Inputs {
    boost::asio::io_context& io;
    std::recursive_mutex& state_mutex;
    const AccountConfig& config;
    const std::map<uint16_t, MarketConfig>& markets;
    std::map<std::string, VenueRule>& venue_rules;
    FundsState& funds;
    OrderManager& orders;
    OrderJournal* const& journal;
    std::function<TradePort*()> port;
    boost::asio::steady_timer& order_query_timer;
    const bool& private_connected;
    const bool& manual_paused;
    const std::atomic<bool>& quiescing;
    bool& ready;
    std::function<void()> disable_admission;
    std::function<void()> enable_admission;
    std::function<bool()> account_queue_empty;
    std::function<size_t()> pending_private_events;
    std::string& error;
    std::function<bool(AccountEvent, bool, std::function<void(absl::Status)>)>
        persist_and_apply;
    std::function<void(std::string)> pause;
    std::function<void()> publish_all;
    std::function<void()> schedule_order_query;
  };

  explicit AccountSync(Inputs inputs);

  void Start();
  void Disconnect();
  bool BufferPrivate(AccountEvent event, size_t capacity);
  bool Syncing() const { return syncing_; }
  void CancelTimer();

 private:
  template <typename Callback>
  auto GuardState(Callback callback) {
    return [this, callback = std::move(callback)](auto&&... args) mutable {
      std::lock_guard lock(inputs_.state_mutex);
      callback(std::forward<decltype(args)>(args)...);
    };
  }

  struct State;
  void SyncRules(std::shared_ptr<State> state,
                 absl::StatusOr<std::vector<VenueRule>> response);
  void SyncOpenOrders(std::shared_ptr<State> state,
                      absl::StatusOr<std::vector<OrderReport>> response);
  void SyncNextOrder(std::shared_ptr<State> state);
  void SyncNextTrade(std::shared_ptr<State> state);
  void SyncBalances(std::shared_ptr<State> state);
  void SyncNextBalance(std::shared_ptr<State> state);
  void FinishSync(std::shared_ptr<State> state);

  Inputs inputs_;
  boost::asio::steady_timer sync_timer_;
  uint64_t generation_ = 0;
  size_t attempts_ = 0;
  bool syncing_ = false;
  std::deque<AccountEvent> buffered_private_;
};

}  // namespace hquant::v1
