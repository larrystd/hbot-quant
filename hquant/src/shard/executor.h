#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "hquant/config.h"
#include "hquant/shard/market_type.h"
#include "hquant/shard/paper_exchange.h"
#include "hquant/shard/strategy.h"

namespace hquant::v1 {

struct CommandResult {
  enum class Kind { Submit, Cancel } kind = Kind::Submit;
  OrderId order_id;
  Side side = Side::Buy;
  uint64_t price_nanos = 0;
  uint64_t quantity_nanos = 0;
  bool dispatched = false;
  std::string reason;
};

// Our side of trading: validates strategy decisions against trading rules and
// budgets, then dispatches them to the exchange. Matching is not done here.
class Executor {
 public:
  // exchange must outlive the Executor.
  Executor(const MarketConfig& market, const ExecutorConfig& config,
           PaperExchange& exchange);

  absl::StatusOr<std::vector<CommandResult>> Dispatch(
      const StrategyDecision& decision, const BookView& book);
  absl::StatusOr<std::vector<CommandResult>> CancelAll();
  ExecutionView View() const;

 private:
  uint64_t Budget(const std::string& asset) const;
  std::string ValidateOrder(const LimitOrderRequest& request,
                            const BookView& book) const;

  MarketConfig market_;
  ExecutorConfig config_;
  PaperExchange& exchange_;
};

}  // namespace hquant::v1
