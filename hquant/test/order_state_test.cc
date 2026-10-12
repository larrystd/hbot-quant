#include <cstdlib>
#include <iostream>

#include "hquant/trading/order_state.h"

namespace {

using namespace hquant::v1;

void Require(bool condition, const char* message) {
  if (condition) return;
  std::cerr << message << '\n';
  std::exit(1);
}

Order BaseOrder() {
  Order order;
  order.id = OrderId{7};
  order.side = Side::Buy;
  order.price_nanos = 100'000'000'000ULL;
  order.quantity_nanos = 2'000'000'000ULL;
  order.remaining_nanos = order.quantity_nanos;
  order.market = MarketId{"BTCUSDT"};
  order.client_order_id = ClientOrderId{"test-1"};
  order.exchange_order_id = ExchangeOrderId{7};
  return order;
}

}  // namespace

int main() {
  OrderState state;
  TrackedOrder pending;
  pending.client_order_id = ClientOrderId{"test-1"};
  pending.market = MarketId{"BTCUSDT"};
  pending.side = Side::Buy;
  pending.price_nanos = 100'000'000'000ULL;
  pending.quantity_nanos = 2'000'000'000ULL;
  Require(state.Begin(pending).ok(), "pending order rejected locally");
  Require(!state.Begin(pending).ok(), "duplicate client id admitted");

  Fill first;
  first.order_id = OrderId{7};
  first.side = Side::Buy;
  first.trade_id = 11;
  first.price_nanos = 100'000'000'000ULL;
  first.quantity_nanos = 1'000'000'000ULL;
  first.market = MarketId{"BTCUSDT"};
  first.client_order_id = ClientOrderId{"test-1"};
  auto applied = state.ApplyFill(first);  // Private fill arrives before REST.
  Require(applied.ok() && *applied, "first fill not applied");
  applied = state.ApplyFill(first);
  Require(applied.ok() && !*applied, "duplicate fill applied twice");

  Order report_order = BaseOrder();
  report_order.remaining_nanos = 1'000'000'000ULL;
  auto report = state.ApplyReport(
      OrderReport{report_order, OrderStatus::PartiallyFilled});
  Require(report.ok(), "late REST/private order report failed");
  Require(state.BeginCancel(ClientOrderId{"test-1"}).ok(),
          "cancel was not accepted");
  Require(!state.BeginCancel(ClientOrderId{"test-1"}).ok(),
          "duplicate pending cancel was accepted");

  Fill second = first;
  second.trade_id = 12;
  second.quantity_nanos = 500'000'000ULL;
  applied = state.ApplyFill(second);  // Fill races with pending cancel.
  Require(applied.ok() && *applied, "fill during cancel lost");
  report_order.remaining_nanos = 500'000'000ULL;
  report = state.ApplyReport(OrderReport{report_order, OrderStatus::Cancelled});
  Require(report.ok(), "cancel confirmation failed");
  const TrackedOrder* final = state.Find(ClientOrderId{"test-1"});
  Require(final && final->filled_nanos == 1'500'000'000ULL &&
              final->cancel == CancelPhase::Confirmed &&
              final->status == OrderStatus::Cancelled,
          "cancel/fill final state is inconsistent");
  report = state.ApplyReport(OrderReport{report_order, OrderStatus::Accepted});
  Require(report.ok() && !*report,
          "late nonterminal report changed a cancelled order");
  report = state.ApplyReport(OrderReport{report_order, OrderStatus::Filled});
  Require(!report.ok(), "conflicting terminal status was accepted");

  OrderState still_open;
  TrackedOrder retry_order = pending;
  retry_order.client_order_id = ClientOrderId{"retry-cancel"};
  Require(still_open.Begin(retry_order).ok(), "retry order not begun");
  Order open_report = BaseOrder();
  open_report.client_order_id = retry_order.client_order_id;
  Require(still_open.ApplyReport(
              OrderReport{open_report, OrderStatus::Accepted, {}, 1}).ok(),
          "retry order not accepted");
  Require(still_open.BeginCancel(retry_order.client_order_id, 0, 2).ok(),
          "retry order cancel not begun");
  Require(still_open.MarkCancelUnknown(retry_order.client_order_id).ok(),
          "unknown cancel not recorded");
  Require(!still_open.BeginCancel(retry_order.client_order_id).ok(),
          "unknown cancel was repeated without a venue query");
  auto old_private = still_open.ApplyReport(
      OrderReport{open_report, OrderStatus::Accepted, {}, 1});
  Require(old_private.ok() &&
              still_open.Find(retry_order.client_order_id)->cancel ==
                  CancelPhase::Unknown,
          "old private report resolved an unknown cancel");
  auto newer_private = still_open.ApplyReport(
      OrderReport{open_report, OrderStatus::Accepted, {}, 3});
  Require(newer_private.ok() &&
              still_open.Find(retry_order.client_order_id)->cancel ==
                  CancelPhase::Unknown,
          "newer private report resolved an unknown cancel");
  auto reconciled = still_open.ApplyReport(
      OrderReport{open_report, OrderStatus::Accepted, {}, 1, true});
  Require(reconciled.ok() && *reconciled,
          "still-open venue report did not resolve unknown cancel");
  Require(still_open.BeginCancel(retry_order.client_order_id).ok(),
          "still-open order cannot be cancelled again");
}
