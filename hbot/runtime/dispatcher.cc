#include "hbot/runtime/dispatcher.h"

#include <chrono>
#include <limits>
#include <optional>
#include <string>
#include <utility>
#include <variant>

#include "absl/status/status.h"
#include "absl/status/statusor.h"

namespace hbot {
namespace {

void UnwindPrepared(RiskGate& risk, OrderGateway& gateway,
                    ReservationId reservation,
                    const std::optional<ClientOrderId>& client_id) {
  if (client_id) {
    const auto aborted = gateway.AbortPrepared(*client_id);
    // A gateway may consume the prepared slot before returning a local
    // failure. In that case there is no slot left to abort.
    if (!aborted.ok() && !absl::IsNotFound(aborted)) risk.EmergencyStop();
  }
  const auto released = risk.ConfirmTerminal(reservation);
  if (!released.ok()) risk.EmergencyStop();
}

absl::StatusOr<OrderRequest> NormalizeOrder(const OrderRequest& raw,
                                            const TradingRule& rule) {
  if (!raw.limit_price)
    return absl::InvalidArgumentError("limit price missing");
  auto amount =
      raw.base_amount.Quantize(rule.base_increment, RoundingMode::Down);
  auto price =
      raw.limit_price->Quantize(rule.price_increment, RoundingMode::Down);
  if (!amount.ok()) return amount.status();
  if (!price.ok()) return price.status();
  OrderRequest request = raw;
  request.base_amount = *amount;
  request.limit_price = *price;
  return request;
}

}  // namespace

bool ActionDispatcher::Record(RecordPayload payload, const OwnerId& owner,
                              UtcTime now) {
  if (shard_sequence_ == std::numeric_limits<uint64_t>::max()) {
    risk_.EmergencyStop();
    return false;
  }
  const uint64_t sequence = ++shard_sequence_;
  RecordEnvelope envelope;
  envelope.run_id = run_;
  envelope.shard = shard_;
  envelope.shard_sequence = sequence;
  envelope.owner = owner;
  envelope.received_at_utc = now;
  envelope.payload = std::move(payload);
  if (!recorder_.TryPush(std::move(envelope))) {
    Gap(sequence);
    return false;
  }
  return true;
}

void ActionDispatcher::Gap(uint64_t sequence) {
  if (!local_gaps_.empty() && local_gaps_.back().last_seq + 1 == sequence) {
    local_gaps_.back().last_seq = sequence;
    return;
  }
  local_gaps_.push_back(
      HistoryGap{run_, shard_, sequence, sequence, GapReason::QueueFull});
}

std::vector<DispatchResult> ActionDispatcher::Dispatch(
    const ActionBatch& batch, const DispatchContext& context) {
  std::vector<DispatchResult> results;
  results.reserve(batch.ordered.size());
  for (uint32_t index = 0; index < batch.ordered.size(); ++index) {
    const StrategyAction& action = batch.ordered[index];
    DispatchResult result;
    result.action_index = index;
    const OwnerId& action_owner = std::visit(
        [](const auto& value) -> const OwnerId& { return value.owner; },
        action);
    const bool submit = std::holds_alternative<SubmitOrder>(action);
    if (action_owner != context.owner || !context.owner.IsValid()) {
      result.reason = "action owner mismatch";
    } else if (const auto* cancel = std::get_if<CancelOrder>(&action)) {
      const auto status =
          gateway_.StartCancel(cancel->owner, cancel->client_id);
      result.accepted = status.ok();
      result.reason =
          status.ok() ? "cancel requested" : std::string(status.message());
      result.client_id = cancel->client_id;
    } else {
      const auto& order = std::get<SubmitOrder>(action);
      auto normalized = NormalizeOrder(order.request, context.rule);
      if (!normalized.ok()) {
        result.reason = std::string(normalized.status().message());
      } else {
        auto reservation = risk_.TryReserve(
            order.owner, *normalized, context.market, context.rule,
            context.now_utc, context.market_live, context.account_fresh);
        if (!reservation.ok()) {
          result.reason = std::string(reservation.status().message());
        } else {
          OrderCommand command{order.owner, *normalized,
                               reservation->reservation_id, context.decision_id,
                               context.now_mono + std::chrono::seconds(1)};
          auto intent = gateway_.PrepareSubmit(std::move(command));
          if (!intent.ok()) {
            UnwindPrepared(risk_, gateway_, reservation->reservation_id,
                           std::nullopt);
            result.reason = std::string(intent.status().message());
          } else if (intent->client_id.value.empty() ||
                     intent->owner != order.owner ||
                     intent->request.account != normalized->account ||
                     intent->request.market != normalized->market) {
            UnwindPrepared(risk_, gateway_, reservation->reservation_id,
                           intent->client_id);
            result.reason = "gateway returned mismatched intent";
          } else {
            const auto attached = risk_.AttachClientId(
                reservation->reservation_id, intent->client_id);
            if (!attached.ok()) {
              UnwindPrepared(risk_, gateway_, reservation->reservation_id,
                             intent->client_id);
              result.reason = std::string(attached.message());
            } else {
              result.client_id = intent->client_id;
              result.intent = *intent;
              Record(*intent, order.owner, context.now_utc);
              const auto started = gateway_.StartPrepared(intent->client_id);
              result.accepted = started.ok();
              result.reason = started.ok() ? "locally submitted"
                                           : std::string(started.message());
              if (started.ok()) {
                result.reservation_id = reservation->reservation_id;
              }
              if (!started.ok()) {
                UnwindPrepared(risk_, gateway_, reservation->reservation_id,
                               intent->client_id);
              }
            }
          }
        }
      }
    }
    DecisionRecord decision;
    decision.decision_id = context.decision_id;
    decision.owner = action_owner;
    decision.action_index = index;
    decision.action_kind =
        submit ? DecisionActionKind::Submit : DecisionActionKind::Cancel;
    decision.accepted = result.accepted;
    decision.reason = result.reason;
    decision.client_id = result.client_id;
    Record(std::move(decision), action_owner, context.now_utc);
    results.push_back(std::move(result));
  }
  return results;
}

}  // namespace hbot
