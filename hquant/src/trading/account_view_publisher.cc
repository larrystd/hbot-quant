#include "hquant/trading/account_view_publisher.h"

#include <algorithm>
#include <utility>

namespace hquant::v1 {

AssetViewValues PublishedAssetView::Read() const {
  for (;;) {
    const uint64_t before = sequence_.load(std::memory_order_seq_cst);
    if (before & 1) continue;
    AssetViewValues values;
    values.revision = before / 2;
    values.total_nanos = total_nanos_.load(std::memory_order_seq_cst);
    values.available_nanos = available_nanos_.load(std::memory_order_seq_cst);
    values.budget_nanos = budget_nanos_.load(std::memory_order_seq_cst);
    if (sequence_.load(std::memory_order_seq_cst) == before) return values;
  }
}

void PublishedAssetView::Publish(uint64_t total, uint64_t available,
                                 uint64_t budget) {
  sequence_.fetch_add(1, std::memory_order_seq_cst);
  total_nanos_.store(total, std::memory_order_seq_cst);
  available_nanos_.store(available, std::memory_order_seq_cst);
  budget_nanos_.store(budget, std::memory_order_seq_cst);
  sequence_.fetch_add(1, std::memory_order_seq_cst);
}

AccountViewPublisher::AccountViewPublisher(
    Inputs inputs, const std::vector<ShardConfig>& shards)
    : inputs_(std::move(inputs)) {
  for (const ShardConfig& shard : shards) {
    asset_shards_[shard.market.base_asset].push_back(shard.id.value);
    asset_shards_[shard.market.quote_asset].push_back(shard.id.value);
  }
  for (const auto& entry : asset_shards_)
    asset_views_.emplace(entry.first, std::make_shared<PublishedAssetView>());
  PublishAllAssets();
}

std::shared_ptr<const PublishedAssetView> AccountViewPublisher::AssetView(
    const std::string& asset) const {
  const auto found = asset_views_.find(asset);
  return found == asset_views_.end() ? nullptr : found->second;
}

void AccountViewPublisher::PublishAllAssets() {
  asset_revision_->fetch_add(1, std::memory_order_seq_cst);
  for (const auto& [asset, snapshot] : asset_views_) {
    const AssetFunds* funds = inputs_.funds.Asset(asset);
    snapshot->Publish(funds ? funds->total_nanos : 0,
                      inputs_.funds.Available(asset), inputs_.budget(asset));
  }
  asset_revision_->fetch_add(1, std::memory_order_seq_cst);
  dirty_assets_.clear();
}

void AccountViewPublisher::SetSink(ShardId shard, ViewSink sink) {
  sinks_[shard.value] = std::move(sink);
}

void AccountViewPublisher::MarkShardDirty(ShardId shard) {
  dirty_shards_.insert(shard.value);
}

void AccountViewPublisher::MarkAssetDirty(const std::string& asset) {
  const auto found = asset_shards_.find(asset);
  if (found == asset_shards_.end()) return;
  dirty_assets_.insert(asset);
  if (found->second.size() <= 64)
    dirty_shards_.insert(found->second.begin(), found->second.end());
}

void AccountViewPublisher::MarkAllDirty() {
  for (const auto& [shard, sink] : sinks_) dirty_shards_.insert(shard);
}

void AccountViewPublisher::Flush() {
  if (inputs_.replaying ||
      (dirty_shards_.empty() && dirty_assets_.empty())) return;
  if (!dirty_assets_.empty()) {
    asset_revision_->fetch_add(1, std::memory_order_seq_cst);
    for (const std::string& asset : dirty_assets_) {
      const AssetFunds* funds = inputs_.funds.Asset(asset);
      asset_views_.at(asset)->Publish(
          funds ? funds->total_nanos : 0, inputs_.funds.Available(asset),
          inputs_.budget(asset));
    }
    asset_revision_->fetch_add(1, std::memory_order_seq_cst);
  }
  dirty_assets_.clear();
  ++revision_;
  for (uint16_t shard : dirty_shards_) {
    const auto found = sinks_.find(shard);
    if (found != sinks_.end()) found->second(View(ShardId{shard}));
  }
  dirty_shards_.clear();
}

AccountView AccountViewPublisher::View(ShardId shard) const {
  AccountView view;
  view.revision = revision_;
  view.ready = inputs_.ready;
  view.maker_fee_rate_ppb = inputs_.config.maker_fee_rate_ppb;
  const auto found = inputs_.markets.find(shard.value);
  if (found == inputs_.markets.end()) return view;
  const MarketConfig& market = found->second;
  view.rule = market.rule;
  const auto venue_rule = inputs_.venue_rules.find(market.id.symbol);
  if (venue_rule != inputs_.venue_rules.end()) {
    view.venue_rule = venue_rule->second;
    view.rule.price_increment_nanos =
        std::max(view.rule.price_increment_nanos,
                 venue_rule->second.price_increment_nanos);
    view.rule.base_increment_nanos =
        std::max(view.rule.base_increment_nanos,
                 venue_rule->second.base_increment_nanos);
    view.rule.min_base_amount_nanos =
        std::max(view.rule.min_base_amount_nanos,
                 venue_rule->second.min_base_amount_nanos);
    view.rule.min_order_value_nanos =
        std::max(view.rule.min_order_value_nanos,
                 venue_rule->second.min_order_value_nanos);
  }
  view.open_order_ids = inputs_.orders.OpenFor(shard);
  view.pending_operation = inputs_.orders.PendingFor(shard);
  for (OrderId id : view.open_order_ids) {
    const TrackedOrder* order = inputs_.orders.FindLocal(id);
    if (!order) continue;
    switch (order->cancel) {
      case CancelPhase::Pending:
        view.pending_cancel_ids.push_back(id);
        break;
      case CancelPhase::Unknown:
        view.unknown_cancel_ids.push_back(id);
        break;
      case CancelPhase::Rejected:
        view.rejected_cancel_ids.push_back(id);
        break;
      case CancelPhase::None:
      case CancelPhase::Confirmed:
        break;
    }
  }
  if (const AssetFunds* base = inputs_.funds.Asset(market.base_asset)) {
    view.base_total_nanos = base->total_nanos;
  }
  if (const AssetFunds* quote = inputs_.funds.Asset(market.quote_asset)) {
    view.quote_total_nanos = quote->total_nanos;
  }
  view.base_available_nanos = inputs_.funds.Available(market.base_asset);
  view.quote_available_nanos = inputs_.funds.Available(market.quote_asset);
  view.base_budget_nanos = inputs_.budget(market.base_asset);
  view.quote_budget_nanos = inputs_.budget(market.quote_asset);
  return view;
}

}  // namespace hquant::v1
