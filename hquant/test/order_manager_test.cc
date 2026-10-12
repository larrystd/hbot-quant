#include "hquant/trading/order_manager.h"

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <string>

namespace {

using namespace hquant::v1;

void Require(bool condition, const char* message) {
  if (condition) return;
  std::cerr << message << '\n';
  std::exit(1);
}

TrackedOrder Pending(ClientOrderId client, OrderId local, ShardId shard) {
  TrackedOrder order;
  order.client_order_id = std::move(client);
  order.local_order_id = local;
  order.shard = shard;
  order.market = MarketId{"BTCUSDT"};
  order.price_nanos = 100'000'000'000ULL;
  order.quantity_nanos = 1'000'000'000ULL;
  return order;
}

OrderReport Report(const TrackedOrder& tracked, OrderStatus state) {
  Order order;
  order.id = tracked.local_order_id;
  order.client_order_id = tracked.client_order_id;
  order.exchange_order_id = ExchangeOrderId{123};
  order.market = tracked.market;
  order.side = tracked.side;
  order.price_nanos = tracked.price_nanos;
  order.quantity_nanos = tracked.quantity_nanos;
  order.remaining_nanos = tracked.Remaining();
  return OrderReport{std::move(order), state};
}

}  // namespace

int main() {
  OrderManager manager;
  const ClientOrderId first{"first"};
  const ClientOrderId second{"second"};
  auto first_local = manager.NextLocalOrderId();
  auto second_local = manager.NextLocalOrderId();
  Require(first_local.ok() && second_local.ok() &&
              first_local->value != second_local->value,
          "local IDs repeated");
  Require(manager.Begin(Pending(first, *first_local, ShardId{1})).ok(),
          "first order not tracked");
  Require(manager.Begin(Pending(second, *second_local, ShardId{2})).ok(),
          "second order not tracked");
  Require(manager.FindLocal(*first_local)->client_order_id == first &&
              manager.OpenFor(ShardId{2}).size() == 1,
          "account-wide order identity is inconsistent");

  const auto now = OrderManager::Clock::now();
  manager.Arm(first, now);
  Require(!manager.TakeDue(now + std::chrono::seconds(1)),
          "pending order queried before its deadline");
  auto query = manager.TakeDue(now + std::chrono::seconds(2));
  Require(query && query->client_order_id == first,
          "pending order deadline did not produce an ID query");
  Require(manager.MarkSubmitUnknown(first).ok(),
          "ambiguous submit not tracked");
  manager.QueryFinished(first);
  auto accepted =
      manager.ApplyReport(Report(*manager.Find(first), OrderStatus::Accepted));
  Require(accepted.ok() && *accepted && !manager.PendingFor(ShardId{1}) &&
              !manager.NextDeadline(),
          "accepted order retained an unresolved submit query");
  Require(manager.BeginCancel(first, 42).ok(), "cancel was not tracked");
  manager.Arm(first, now);
  Require(
      manager.PendingFor(ShardId{1}) && !manager.BeginCancel(first, 43).ok(),
      "pending cancel permitted a duplicate request");
  Require(manager.MarkCancelUnknown(first).ok(),
          "ambiguous cancel not tracked");
  Require(!manager.BeginCancel(first, 44).ok(),
          "unknown cancel permitted a duplicate request");
  Require(!manager.RepeatedQuery(first),
          "unknown cancel was resolved before a venue query");
  auto first_cancel_query = manager.TakeDue(now + std::chrono::seconds(3));
  Require(first_cancel_query && first_cancel_query->client_order_id == first &&
              !manager.RepeatedQuery(first),
          "first cancel query was treated as conclusive");
  manager.QueryFinished(first);
  auto second_cancel_query = manager.TakeDue(now + std::chrono::seconds(5));
  Require(second_cancel_query &&
              second_cancel_query->client_order_id == first &&
              manager.RepeatedQuery(first),
          "spaced cancel query was not marked authoritative");
  auto cancelled =
      manager.ApplyReport(Report(*manager.Find(first), OrderStatus::Cancelled));
  Require(cancelled.ok() && *cancelled && manager.OpenFor(ShardId{1}).empty() &&
              !manager.PendingFor(ShardId{1}),
          "terminal report did not resolve the cancel");
  Require(
      manager.AbortSubmit(second).ok() && manager.OpenFor(ShardId{2}).empty(),
      "unsent order retained its local identity");

  OrderManager deadlines;
  const ClientOrderId early{"early"}, late{"late"};
  Require(deadlines.Begin(Pending(early, OrderId{1}, ShardId{1})).ok() &&
              deadlines.Begin(Pending(late, OrderId{2}, ShardId{2})).ok(),
          "deadline test orders not tracked");
  deadlines.Arm(late, now + std::chrono::seconds(1));
  deadlines.Arm(early, now);
  Require(deadlines.NextDeadline() == now + std::chrono::seconds(2) &&
              deadlines.TakeDue(now + std::chrono::seconds(2))
                      ->client_order_id == early &&
              deadlines.NextDeadline() == now + std::chrono::seconds(3),
          "earliest query deadline was not selected");
  deadlines.QueryFinished(early);
  Require(deadlines.NextDeadline() == now + std::chrono::seconds(3),
          "finished query was not rearmed");
  Require(deadlines.ApplyReport(
              Report(*deadlines.Find(early), OrderStatus::Accepted)).ok() &&
              deadlines.NextDeadline() == now + std::chrono::seconds(3),
          "resolved query remained in the deadline index");
  Require(deadlines.AbortSubmit(late).ok() && !deadlines.NextDeadline(),
          "aborted order retained its query deadline");
}
