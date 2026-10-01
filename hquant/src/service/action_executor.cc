#include "service/action_executor.h"

#include <chrono>
#include <limits>
#include <optional>
#include <string>
#include <utility>
#include <variant>

#include "absl/status/status.h"
#include "absl/status/statusor.h"

namespace hquant {
namespace {

void UnwindPrepared(RiskGate& risk, OrderGateway& gateway, HoldId hold,
                    const std::optional<ClientOrderId>& client_id) {
  if (client_id) {
    const auto aborted = gateway.AbortPrepared(*client_id);
    // A gateway may consume the prepared slot before returning a local
    // failure. In that case there is no slot left to abort.
    if (!aborted.ok() && CodeOf(aborted) != ErrorCode::kOrderNotFound)
      risk.EmergencyStop();
  }
  const auto released = risk.Release(hold);
  if (!released.ok()) risk.EmergencyStop();
}

ErrorCode NormalizeOrder(const OrderRequest& raw, const TradingRule& rule,
                         std::optional<OrderRequest>* normalized) {
  if (!raw.limit_price) return ErrorCode::kOrderPriceOrAmountInvalid;
  auto amount =
      raw.base_amount.Quantize(rule.base_increment, RoundingMode::Down);
  auto price =
      raw.limit_price->Quantize(rule.price_increment, RoundingMode::Down);
  if (!amount.ok() || !price.ok()) return ErrorCode::kDecimalArithmeticFailed;
  normalized->emplace(raw);
  (*normalized)->base_amount = *amount;
  (*normalized)->limit_price = *price;
  return ErrorCode::kOk;
}

}  // namespace

bool ActionExecutor::Record(RecordPayload payload,
                              const StrategyId& strategy_id, UtcTime now) {
  if (shard_sequence_ == std::numeric_limits<uint64_t>::max()) {
    risk_.EmergencyStop();
    return false;
  }
  const uint64_t sequence = ++shard_sequence_;
  RecordEnvelope envelope;
  envelope.run_id = run_;
  envelope.shard = shard_;
  envelope.shard_sequence = sequence;
  envelope.strategy_id = strategy_id;
  envelope.received_at_utc = now;
  envelope.payload = std::move(payload);
  if (!recorder_.TryPush(std::move(envelope))) {
    Gap(sequence);
    return false;
  }
  return true;
}

void ActionExecutor::Gap(uint64_t sequence) {
  if (!local_gaps_.empty() && local_gaps_.back().last_seq + 1 == sequence) {
    local_gaps_.back().last_seq = sequence;
    return;
  }
  local_gaps_.push_back(HistoryGap{run_, shard_, sequence, sequence,
                                   ErrorCode::kStorageQueueFull});
}

std::vector<ActionResult> ActionExecutor::Execute(
    const ActionBatch& batch, const ActionContext& context) {
  std::vector<ActionResult> results;
  results.reserve(batch.ordered.size());
  for (uint32_t index = 0; index < batch.ordered.size(); ++index) {
    const StrategyAction& action = batch.ordered[index];
    ActionResult result;
    result.action_index = index;
    const StrategyId& action_strategy_id = std::visit(
        [](const auto& value) -> const StrategyId& {
          return value.strategy_id;
        },
        action);
    const bool submit = std::holds_alternative<SubmitOrder>(action);
    if (action_strategy_id != context.strategy_id ||
        !context.strategy_id.IsValid()) {
      result.reason = ErrorCode::kOrderStrategyIdInvalid;
      result.message = "action strategy ID does not match the shard strategy";
    } else if (const auto* cancel = std::get_if<CancelOrder>(&action)) {
      const auto status =
          gateway_.StartCancel(cancel->strategy_id, cancel->client_id);
      result.accepted = status.ok();
      result.reason = status.ok() ? ErrorCode::kOk : CodeOf(status);
      result.message =
          status.ok() ? "cancel requested" : std::string(status.message());
      result.client_id = cancel->client_id;
    } else {
      const auto& order = std::get<SubmitOrder>(action);
      std::optional<OrderRequest> normalized;
      const ErrorCode normalization_code =
          NormalizeOrder(order.request, context.rule, &normalized);
      if (normalization_code != ErrorCode::kOk) {
        result.reason = normalization_code;
      } else {
        FundsHold hold;
        const ErrorCode reserve_code = risk_.TryHoldCode(
            order.strategy_id, *normalized, context.market, context.rule,
            context.now_utc, context.market_live, context.account_fresh, &hold);
        if (reserve_code != ErrorCode::kOk) {
          result.reason = reserve_code;
        } else {
          ApprovedOrder approved{order.strategy_id, *normalized, hold.hold_id,
                                 context.action_batch_id,
                                 context.now_mono + std::chrono::seconds(1)};
          auto prepared = gateway_.PrepareSubmit(std::move(approved));
          if (!prepared.ok()) {
            UnwindPrepared(risk_, gateway_, hold.hold_id, std::nullopt);
            result.reason = CodeOf(prepared.status());
            result.message = std::string(prepared.status().message());
          } else if (prepared->client_id.value.empty() ||
                     prepared->strategy_id != order.strategy_id ||
                     prepared->request.account != normalized->account ||
                     prepared->request.market != normalized->market) {
            UnwindPrepared(risk_, gateway_, hold.hold_id, prepared->client_id);
            result.reason = ErrorCode::kInternal;
            result.message = "gateway returned mismatched prepared order";
          } else {
            const auto attached =
                risk_.AttachClientId(hold.hold_id, prepared->client_id);
            if (!attached.ok()) {
              UnwindPrepared(risk_, gateway_, hold.hold_id,
                             prepared->client_id);
              result.reason = CodeOf(attached);
              result.message = std::string(attached.message());
            } else {
              result.client_id = prepared->client_id;
              result.prepared = *prepared;
              Record(*prepared, order.strategy_id, context.now_utc);
              const auto started = gateway_.StartPrepared(prepared->client_id);
              result.accepted = started.ok();
              result.reason = started.ok() ? ErrorCode::kOk : CodeOf(started);
              result.message = started.ok() ? "locally submitted"
                                            : std::string(started.message());
              if (started.ok()) {
                result.hold_id = hold.hold_id;
              }
              if (!started.ok()) {
                UnwindPrepared(risk_, gateway_, hold.hold_id,
                               prepared->client_id);
              }
            }
          }
        }
      }
    }
    ActionRecord action_record;
    action_record.action_batch_id = context.action_batch_id;
    action_record.strategy_id = action_strategy_id;
    action_record.action_index = index;
    action_record.action_kind =
        submit ? ActionKind::Submit : ActionKind::Cancel;
    action_record.accepted = result.accepted;
    action_record.reason = result.reason;
    action_record.message = result.message;
    action_record.client_id = result.client_id;
    Record(std::move(action_record), action_strategy_id, context.now_utc);
    results.push_back(std::move(result));
  }
  return results;
}

}  // namespace hquant
