#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "hquant/trading/trading_account.h"

namespace hquant::v1 {

// 账户线程发布资产快照；仅把局部订单变化投递到受影响的 Shard。
class AccountViewPublisher {
 public:
  using ViewSink = TradingAccount::ViewSink;

  // 只借用账户状态；sink 负责把局部视图投递到 Shard 线程。
  struct Inputs {
    const AccountConfig& config;
    const std::map<uint16_t, MarketConfig>& markets;
    const std::map<std::string, VenueRule>& venue_rules;
    const FundsState& funds;
    const OrderManager& orders;
    const bool& ready;
    const bool& replaying;
    std::function<uint64_t(const std::string&)> budget;
  };

  AccountViewPublisher(Inputs inputs, const std::vector<ShardConfig>& shards);

  void SetSink(ShardId shard, ViewSink sink);
  void MarkShardDirty(ShardId shard);
  void MarkAssetDirty(const std::string& asset);
  void MarkAllDirty();
  void Flush();
  void PublishAllAssets();
  std::shared_ptr<const PublishedAssetView> AssetView(
      const std::string& asset) const;
  std::shared_ptr<const std::atomic<uint64_t>> AssetRevision() const {
    return asset_revision_;
  }
  AccountView View(ShardId shard) const;
  uint64_t Revision() const { return revision_; }

 private:
  Inputs inputs_;
  std::map<std::string, std::vector<uint16_t>> asset_shards_;
  std::map<std::string, std::shared_ptr<PublishedAssetView>> asset_views_;
  std::shared_ptr<std::atomic<uint64_t>> asset_revision_ =
      std::make_shared<std::atomic<uint64_t>>(0);
  std::map<uint16_t, ViewSink> sinks_;
  std::set<std::string> dirty_assets_;
  std::set<uint16_t> dirty_shards_;
  uint64_t revision_ = 0;
};

}  // namespace hquant::v1
