// 回放演示：读取 paper_replay.yaml + paper_market.json，逐条喂给 ShardRuntime，
// 每处理完一条行情输入就打印：输入内容 → 产生的记录 → 盘口 → 挂单 → 余额。
//
// 运行（在仓库根目录）：
//   bazel run //examples:replay_walkthrough
//   bazel run //examples:replay_walkthrough -- <config.yaml> <market.json>
//
// 组装方式与 application/launcher.cc 的回放模式相同，区别是：
//   - 不写 SQLite，记录直接打印到屏幕（PrintingRecorder）；
//   - 不启动控制服务，跑完即退出。

#include <iostream>
#include <string>
#include <variant>

#include "application/config.h"
#include "base/error.h"
#include "market/replay_feed.h"
#include "order/paper.h"
#include "order/risk.h"
#include "service/shard.h"
#include "strategy/simple_pmm.h"

namespace {

using namespace hquant;

// Decimal::ToString 会输出科学计数法（10 → "1E+1"），这里转成普通写法便于阅读。
std::string Plain(const Decimal& value) {
  std::string text = value.ToString();
  const size_t e = text.find_first_of("eE");
  if (e == std::string::npos) return text;
  const int exponent = std::stoi(text.substr(e + 1));
  std::string mantissa = text.substr(0, e);
  const bool negative = !mantissa.empty() && mantissa[0] == '-';
  if (negative) mantissa.erase(0, 1);
  const size_t dot = mantissa.find('.');
  std::string digits = mantissa;
  int point = static_cast<int>(mantissa.size());
  if (dot != std::string::npos) {
    digits.erase(dot, 1);
    point = static_cast<int>(dot);
  }
  point += exponent;
  if (point <= 0) {
    digits = "0." + std::string(-point, '0') + digits;
  } else if (point >= static_cast<int>(digits.size())) {
    digits += std::string(point - digits.size(), '0');
  } else {
    digits.insert(point, ".");
  }
  return (negative ? "-" : "") + digits;
}

Decimal D(const char* text) { return *Decimal::Parse(text); }

const char* StateName(BookSyncState state) {
  switch (state) {
    case BookSyncState::Subscribing: return "Subscribing";
    case BookSyncState::Buffering: return "Buffering（等快照）";
    case BookSyncState::Replaying: return "Replaying（等第一条增量）";
    case BookSyncState::Live: return "Live（可用）";
    case BookSyncState::Stale: return "Stale（太久没更新）";
    case BookSyncState::Resyncing: return "Resyncing（作废，需重新订阅）";
  }
  return "?";
}

const char* StatusName(ExchangeOrderStatus status) {
  switch (status) {
    case ExchangeOrderStatus::New: return "New（已挂上）";
    case ExchangeOrderStatus::PartiallyFilled: return "PartiallyFilled";
    case ExchangeOrderStatus::Filled: return "Filled（全部成交）";
    case ExchangeOrderStatus::Canceled: return "Canceled（已撤）";
    case ExchangeOrderStatus::Rejected: return "Rejected（被拒）";
    case ExchangeOrderStatus::Expired: return "Expired";
  }
  return "?";
}

const char* SideName(Side side) { return side == Side::Buy ? "买" : "卖"; }

// ticks/lots 是整数，乘以 BookScale 换算成真实价格/数量。
std::string Price(const BookScale& scale, PriceTicks ticks) {
  auto value = D(std::to_string(ticks.value).c_str()).Multiply(scale.quote_per_tick);
  return value.ok() ? Plain(*value) : "?";
}
std::string Amount(const BookScale& scale, QuantityLots lots) {
  auto value = D(std::to_string(lots.value).c_str()).Multiply(scale.base_per_lot);
  return value.ok() ? Plain(*value) : "?";
}

std::string Levels(const BookScale& scale, const std::vector<BookLevel>& levels) {
  if (levels.empty()) return "无变化";
  std::string text;
  for (const auto& level : levels) {
    if (!text.empty()) text += ", ";
    text += std::to_string(level.price_ticks.value) + "(" +
            Price(scale, level.price_ticks) + ") x " +
            Amount(scale, level.quantity_lots);
    if (level.quantity_lots.value == 0) text += "[删除该价位]";
  }
  return text;
}

// 一条回放输入的中文描述。
std::string Describe(const ReplayInput& input, const BookScale& scale) {
  return std::visit(
      [&](const auto& payload) -> std::string {
        using T = std::decay_t<decltype(payload)>;
        if constexpr (std::is_same_v<T, ReplaySubscribe>) {
          return "subscribe：开始订阅，连接编号 epoch=" +
                 std::to_string(payload.stream_epoch);
        } else if constexpr (std::is_same_v<T, ReplaySnapshot>) {
          return "snapshot：完整订单簿，截至序号 " +
                 std::to_string(payload.last_sequence) + "\n      买: " +
                 Levels(scale, payload.bids) + "\n      卖: " +
                 Levels(scale, payload.asks);
        } else if constexpr (std::is_same_v<T, ReplayDiff>) {
          return "diff：增量，序号 " + std::to_string(payload.first_sequence) +
                 "~" + std::to_string(payload.last_sequence) + "\n      买: " +
                 Levels(scale, payload.bids) + "\n      卖: " +
                 Levels(scale, payload.asks);
        } else if constexpr (std::is_same_v<T, ReplayTimer>) {
          return "timer：定时器，叫醒策略";
        } else {
          return std::string("public_trade：市场上有人成交，主动方=") +
                 SideName(payload.side) + "，价格 " +
                 Price(scale, payload.price_ticks) + "，数量 " +
                 Amount(scale, payload.quantity_lots);
        }
      },
      input.payload);
}

// 代替 SqliteRecorder：shard 想记录的每一条都直接打印出来。
class PrintingRecorder final : public RecorderPort {
 public:
  bool TryPush(RecordEnvelope record) override {
    std::cout << "    记录 #" << record.shard_sequence << "  ";
    std::visit(
        [](const auto& payload) {
          using T = std::decay_t<decltype(payload)>;
          if constexpr (std::is_same_v<T, OrderIntent>) {
            std::cout << "[意图] 准备发出 " << payload.client_id.value << "："
                      << SideName(payload.request.side) << " "
                      << Plain(payload.request.base_amount) << " @ "
                      << Plain(*payload.request.limit_price);
          } else if constexpr (std::is_same_v<T, DecisionRecord>) {
            std::cout << "[决策] "
                      << (payload.action_kind == DecisionActionKind::Submit
                              ? "下单"
                              : "撤单")
                      << (payload.accepted ? " 已受理" : " 被拒绝");
            if (!payload.accepted || !payload.message.empty()) {
              std::cout << "（" << Info(payload.reason).name;
              if (!payload.message.empty()) std::cout << ": " << payload.message;
              std::cout << "）";
            }
            if (payload.client_id) std::cout << " " << payload.client_id->value;
          } else if constexpr (std::is_same_v<T, OrderUpdate>) {
            std::cout << "[回报] " << payload.client_id->value << " -> "
                      << StatusName(payload.exchange_status);
          } else if constexpr (std::is_same_v<T, TradeUpdate>) {
            std::cout << "[成交] " << payload.client_id->value << " 成交 "
                      << Plain(payload.base_amount) << " @ "
                      << Plain(payload.price);
            for (const auto& fee : payload.fees) {
              std::cout << "，手续费 " << Plain(fee.signed_amount) << " "
                        << fee.asset.value;
            }
          } else {
            std::cout << "[其他]";
          }
        },
        record.payload);
    std::cout << "\n";
    return true;
  }
};

// 与 launcher.cc 中 ApplyReplayInput 相同：先拨时钟，再按类型交给 shard。
absl::Status Apply(const ReplayInput& input, const MarketConfig& market,
                   ReplayClock& clock, ShardRuntime& shard) {
  auto advanced = clock.Advance(input.stamp);
  if (!advanced.ok()) return advanced;
  const EventTime time{{}, clock.UtcNow(), clock.MonoNow()};
  if (const auto* p = std::get_if<ReplaySubscribe>(&input.payload)) {
    shard.Subscribe(p->stream_epoch);
  } else if (const auto* p = std::get_if<ReplaySnapshot>(&input.payload)) {
    return shard.OnSnapshot({market.spec.market, market.book_scale.scale_version,
                             p->stream_epoch, p->last_sequence, p->bids,
                             p->asks, time});
  } else if (const auto* p = std::get_if<ReplayDiff>(&input.payload)) {
    return shard.OnDiff({market.spec.market, market.book_scale.scale_version,
                         p->stream_epoch, p->first_sequence, p->last_sequence,
                         p->bids, p->asks, time});
  } else if (std::holds_alternative<ReplayTimer>(input.payload)) {
    auto results = shard.OnTimer(input.stamp);
    if (!results.ok()) return results.status();
    if (results->empty()) std::cout << "    （策略这次没有任何动作）\n";
  } else if (const auto* p = std::get_if<ReplayPublicTrade>(&input.payload)) {
    return shard.OnPublicTrade({market.spec.market, {}, p->price_ticks,
                                p->quantity_lots, p->side, time});
  }
  return absl::OkStatus();
}

void PrintState(const ShardRuntime& shard, const PaperConnector& paper,
                const RiskGate& risk, const AccountId& account,
                const MarketConfig& market) {
  const auto& scale = market.book_scale;
  const auto& book = shard.Book();
  std::cout << "  盘口: " << StateName(book.State());
  if (auto bid = book.BestBid()) {
    std::cout << "  买一 " << Price(scale, bid->price_ticks);
  }
  if (auto ask = book.BestAsk()) {
    std::cout << "  卖一 " << Price(scale, ask->price_ticks);
  }
  std::cout << "\n  挂单:";
  const auto orders = paper.OpenOrders();
  if (orders.empty()) std::cout << " 无";
  for (const auto& order : orders) {
    std::cout << " [" << order.client_id.value << " "
              << SideName(order.request.side) << " "
              << Plain(order.request.base_amount) << " @ "
              << Plain(*order.request.limit_price) << "]";
  }
  std::cout << "\n  余额:";
  for (const AssetId& asset : {market.spec.base_asset, market.spec.quote_asset}) {
    auto lease = risk.Available(account, asset);
    std::cout << " " << asset.value << " 总额 " << Plain(paper.BalanceOf(asset))
              << " / 可用 " << Plain(paper.AvailableBalance(asset))
              << " / 风控剩余额度 " << (lease.ok() ? Plain(*lease) : "?") << ";";
  }
  std::cout << "\n";
}

absl::Status Run(const std::string& config_path, const std::string& market_path) {
  // ---- 1. 读配置（与 hquant start 用同一个 yaml）----
  auto config = LoadConfig(config_path);
  if (!config.ok()) return config.status();
  if (config->accounts.size() != 1 || config->market_specs.size() != 1 ||
      config->strategy_configs.size() != 1 || config->assignments.size() != 1) {
    return absl::InvalidArgumentError("示例只支持 1 个账户/交易对/策略/分片");
  }
  const auto& account = config->accounts.front();
  const auto& market = config->market_specs.front();
  const auto& strategy_config = config->strategy_configs.front();
  const auto& assignment = config->assignments.front();

  // ---- 2. 组装零件 ----
  // 假时钟：每条输入把时间拨到它的 at_us，所以每次运行结果都一样。
  const UtcTime origin(std::chrono::microseconds(1'000'000));
  ReplayClock clock(origin, MonoTime(std::chrono::microseconds(1'000'000)));

  auto rule = market.trading_rule;
  rule.observed_at = origin;
  rule.revision = 1;

  // 策略：只负责"想做什么"，返回下单/撤单列表。
  SimplePmm strategy({strategy_config.owner, account.account, market.spec,
                      strategy_config.order_amount, strategy_config.bid_spread,
                      strategy_config.ask_spread,
                      strategy_config.refresh_interval, PmmPriceType::Mid,
                      D("0.001"), true});
  // 模拟交易所：手续费 0.1%，client ID 用默认的 P1、P2……
  PaperConnector paper({account.account, market.spec, rule,
                        account.initial_balances, D("0.001"), true, {}},
                       clock);
  // 风控：每种资产最多能用多少。
  RiskGate risk({assignment.shard, D("0"), std::chrono::seconds(300)});
  for (const auto& lease : config->static_risk_leases) {
    auto status = risk.SetInitialLease({lease.account, lease.asset, lease.shard,
                                        1, lease.hard_limit,
                                        origin + std::chrono::hours(1)});
    if (!status.ok()) return status;
  }
  PrintingRecorder recorder;
  // 总管：把订单簿、策略、风控、交易所、记录器串起来。
  ShardRuntime shard({RunId{1}, assignment.shard, strategy_config.owner,
                      account.account, market.spec, market.book_scale, rule},
                     clock, strategy, paper, risk, recorder);

  std::cout << "初始状态\n";
  PrintState(shard, paper, risk, account.account, market);

  // ---- 3. 逐条回放 ----
  int index = 0;
  return ReadReplayFile(market_path, [&](const ReplayInput& input) {
    std::cout << "\n==== 输入 " << ++index << "  t=" << input.stamp.at_us
              << "us  " << Describe(input, market.book_scale) << "\n";
    auto status = Apply(input, market, clock, shard);
    if (!status.ok()) return status;
    PrintState(shard, paper, risk, account.account, market);
    return absl::OkStatus();
  });
}

}  // namespace

int main(int argc, char** argv) {
  const std::string config = argc > 1 ? argv[1] : "examples/paper_replay.yaml";
  const std::string market = argc > 2 ? argv[2] : "examples/paper_market.json";
  auto status = Run(config, market);
  if (!status.ok()) {
    std::cerr << "失败: " << status << "\n";
    return 1;
  }
  return 0;
}
