#include "hquant/trading/account_view_publisher.h"

#include <atomic>
#include <cstdlib>
#include <iostream>
#include <map>
#include <string>
#include <thread>
#include <vector>

namespace {

using namespace hquant::v1;

void Require(bool condition, const char* message) {
  if (condition) return;
  std::cerr << message << '\n';
  std::exit(1);
}

}  // namespace

int main() {
  AccountConfig config;
  FundsState funds({{"USD", 1000}, {"BASE2", 1000}});
  OrderManager orders;
  std::map<uint16_t, MarketConfig> markets;
  std::map<std::string, VenueRule> venue_rules;
  std::vector<ShardConfig> shards;
  for (uint16_t i = 0; i < 65; ++i) {
    ShardConfig shard;
    shard.id = ShardId{i};
    shard.market.id = MarketId{"BASE" + std::to_string(i) + "USD"};
    shard.market.base_asset = "BASE" + std::to_string(i);
    shard.market.quote_asset = "USD";
    markets.emplace(i, shard.market);
    shards.push_back(std::move(shard));
  }
  bool ready = true;
  bool replaying = false;
  AccountViewPublisher publisher(
      {config, markets, venue_rules, funds, orders, ready, replaying,
       [&funds](const std::string& asset) { return funds.Available(asset); }},
      shards);
  size_t notifications = 0;
  uint16_t last_shard = 0;
  AccountView last_view;
  for (uint16_t i = 0; i < 65; ++i) {
    publisher.SetSink(ShardId{i}, [&, i](AccountView view) {
      ++notifications;
      last_shard = i;
      last_view = std::move(view);
    });
  }
  const auto usd = publisher.AssetView("USD");
  Require(usd && usd->Read().available_nanos == 1000,
          "initial shared funds snapshot is missing");
  Require(!publisher.AssetView("missing"), "unknown asset has a snapshot");

  Require(funds.Reserve(ClientOrderId{"usd-order"}, "USD", 100).ok(),
          "cannot reserve shared funds");
  publisher.MarkAssetDirty("USD");
  publisher.Flush();
  Require(usd->Read().available_nanos == 900,
          "shared funds snapshot was not refreshed");
  Require(notifications == 0,
          "shared funds change broadcast full views to all shards");

  publisher.MarkShardDirty(ShardId{2});
  publisher.Flush();
  Require(notifications == 1 && last_shard == 2 &&
              last_view.quote_available_nanos == 900,
          "order state did not notify only its shard");

  Require(funds.Reserve(ClientOrderId{"base-order"}, "BASE2", 100).ok(),
          "cannot reserve local asset");
  publisher.MarkAssetDirty("BASE2");
  publisher.Flush();
  Require(notifications == 2 && last_shard == 2 &&
              last_view.base_available_nanos == 900,
          "local asset change did not notify its shard");

  PublishedAssetView concurrent;
  std::atomic<bool> done = false;
  std::thread writer([&] {
    for (uint64_t n = 1; n <= 100'000; ++n)
      concurrent.Publish(n, n * 2, n * 3);
    done.store(true, std::memory_order_release);
  });
  while (!done.load(std::memory_order_acquire)) {
    const AssetViewValues value = concurrent.Read();
    Require(value.available_nanos == value.total_nanos * 2 &&
                value.budget_nanos == value.total_nanos * 3,
            "cross-thread asset snapshot contains mixed revisions");
  }
  writer.join();
  Require(concurrent.Read().revision == 100'000,
          "asset snapshot revision missed a publish");

  FundsState paired_funds({{"BASE", 0}, {"USD", 0}});
  std::map<uint16_t, MarketConfig> paired_markets;
  std::vector<ShardConfig> paired_shards(1);
  paired_shards[0].id = ShardId{0};
  paired_shards[0].market.id = MarketId{"BASEUSD"};
  paired_shards[0].market.base_asset = "BASE";
  paired_shards[0].market.quote_asset = "USD";
  paired_markets.emplace(0, paired_shards[0].market);
  AccountViewPublisher paired(
      {config, paired_markets, venue_rules, paired_funds, orders, ready,
       replaying, [](const std::string&) { return uint64_t{0}; }},
      paired_shards);
  const auto base = paired.AssetView("BASE");
  const auto quote = paired.AssetView("USD");
  const auto revision = paired.AssetRevision();
  done.store(false, std::memory_order_release);
  std::thread pair_writer([&] {
    for (uint64_t n = 1; n <= 10'000; ++n) {
      Require(paired_funds.ApplyBalance(BalanceUpdate{"USD", n, n, 0, n}).ok(),
              "cannot update quote balance");
      Require(paired_funds
                  .ApplyBalance(BalanceUpdate{"BASE", n * 2, n * 2, 0, n})
                  .ok(),
              "cannot update base balance");
      paired.MarkAssetDirty("USD");
      paired.MarkAssetDirty("BASE");
      paired.Flush();
    }
    done.store(true, std::memory_order_release);
  });
  while (!done.load(std::memory_order_acquire)) {
    const uint64_t before = revision->load(std::memory_order_seq_cst);
    if (before & 1) continue;
    const AssetViewValues base_values = base->Read();
    const AssetViewValues quote_values = quote->Read();
    if (revision->load(std::memory_order_seq_cst) != before) continue;
    Require(base_values.total_nanos == quote_values.total_nanos * 2,
            "base and quote snapshots came from different account updates");
  }
  pair_writer.join();
}
