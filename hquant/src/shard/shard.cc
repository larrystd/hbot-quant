#include "shard/shard.h"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <limits>
#include <string>
#include <utility>
#include <variant>

#include "absl/status/status.h"
#include "base/net.h"
#include "boost/asio/co_spawn.hpp"
#include "boost/asio/detached.hpp"
#include "boost/asio/post.hpp"
#include "boost/asio/steady_timer.hpp"
#include "boost/asio/use_awaitable.hpp"
#include "market/market_data_stream.h"
#include "shard/trading_mode.h"
#include "strategy/strategy.h"

namespace hquant {

// 执行一个步骤：步骤中到达的回报先排队，步骤结束后按顺序处理。
template <typename Step>
absl::Status Shard::RunStep(Step&& step) {
  step_running_ = true;
  absl::Status status = step();
  step_running_ = false;
  if (!status.ok()) return status;
  return ProcessQueuedReports();
}

Shard::Shard(Config config, const Clock& clock, Strategy& strategy,
             std::unique_ptr<TradingMode> trading, RiskGate& risk,
             OrderHistoryWriter& recorder)
    : config_(std::move(config)),
      clock_(clock),
      strategy_(strategy),
      trading_(std::move(trading)),
      risk_(risk),
      recorder_(recorder),
      book_(config_.market.market, config_.scale.tick_lot_version, 8192, 1024,
            config_.stale_after_us),
      action_executor_(risk_, trading_->Gateway(), tracker_, recorder_,
                       config_.run, config_.shard, shard_sequence_) {
  origin_mono_ = clock_.MonoNow();
  trading_->SetReportHandler(
      [this](const AccountEvent& report) { OnReport(report); });
}

Shard::Shard(Config config, const Clock& clock,
             std::unique_ptr<Strategy> strategy,
             std::unique_ptr<TradingMode> trading,
             std::unique_ptr<RiskGate> risk, OrderHistoryWriter& recorder)
    : config_(std::move(config)),
      owned_strategy_(std::move(strategy)),
      owned_risk_(std::move(risk)),
      clock_(clock),
      strategy_(*owned_strategy_),
      trading_(std::move(trading)),
      risk_(*owned_risk_),
      recorder_(recorder),
      book_(config_.market.market, config_.scale.tick_lot_version, 8192, 1024,
            config_.stale_after_us),
      action_executor_(risk_, trading_->Gateway(), tracker_, recorder_,
                       config_.run, config_.shard, shard_sequence_) {
  origin_mono_ = clock_.MonoNow();
  trading_->SetReportHandler(
      [this](const AccountEvent& report) { OnReport(report); });
}

// 实时行情的接收方：行情模块解析好的数据从这里进入 Shard。订单簿由 Shard
// 自己改，之后的处理（撮合、决定跑不跑策略）和回放走同一条路 AfterBookApply。
// 行情模块在协程里调用它，出错无法返回给调用者：记进 stream_error_，并让风控
// 紧急停止。
class Shard::StreamReceiver final : public MarketDataReceiver {
 public:
  explicit StreamReceiver(Shard& shard) : shard_(shard) {}

  BookApplyResult OnConnect(uint64_t connection_id) override {
    return Apply(shard_.book_.Subscribe(connection_id));
  }
  BookApplyResult OnSnapshot(const BookSnapshot& snapshot) override {
    return Apply(shard_.book_.OnSnapshot(snapshot));
  }
  BookApplyResult OnDiff(const BookDiff& diff) override {
    return Apply(shard_.book_.OnDiff(diff));
  }
  BookApplyResult OnDisconnect() override {
    return Apply(shard_.book_.OnDisconnect());
  }
  void OnPublicTrade(const PublicTrade& trade) override {
    Check(shard_.OnPublicTrade(trade));
  }
  void OnStreamError(const absl::Status& status) override {
    shard_.stream_error_ = status;
  }

 private:
  BookApplyResult Apply(BookApplyResult result) {
    if (result.state == BookSyncState::Live)
      shard_.stream_error_ = absl::OkStatus();
    Check(shard_.AfterBookApply(result));
    return result;
  }
  void Check(absl::Status status) {
    if (status.ok()) return;
    shard_.stream_error_ = std::move(status);
    shard_.risk_.EmergencyStop();
  }

  Shard& shard_;
};

MarketDataReceiver& Shard::Receiver() {
  if (!receiver_) receiver_ = std::make_unique<StreamReceiver>(*this);
  return *receiver_;
}

Shard::~Shard() {
  RequestStop();
  Join();
}

uint64_t Shard::AppliedDiffs() const {
  return stream_ ? stream_->AppliedDiffs() : 0;
}

uint64_t Shard::Resyncs() const { return stream_ ? stream_->Resyncs() : 0; }

boost::asio::awaitable<void> Shard::TimerLoop() {
  boost::asio::steady_timer timer(*io_);
  uint64_t ordinal = 0;
  while (!stopping_) {
    timer.expires_after(*strategy_.Triggers().timer_period);
    boost::system::error_code ec;
    co_await timer.async_wait(
        boost::asio::redirect_error(boost::asio::use_awaitable, ec));
    if (ec || stopping_) break;
    const auto at_us = (clock_.MonoNow() - origin_mono_).count();
    auto result = OnTimer({at_us, ++ordinal});
    if (!result.ok()) {
      stream_error_ = result.status();
      risk_.EmergencyStop();
    }
  }
}

void Shard::StartFeed(FeedEndpoint rest, FeedEndpoint websocket) {
  io_ = std::make_unique<boost::asio::io_context>();
  const TlsConfig rest_tls{rest.tls, true, {}, rest.host};
  const TlsConfig ws_tls{websocket.tls, true, {}, websocket.host};
  http_ = std::make_unique<HttpClient>(*io_, rest.host,
                                       std::to_string(rest.port), rest_tls);
  std::string symbol = config_.market.market.native_symbol;
  std::transform(
      symbol.begin(), symbol.end(), symbol.begin(),
      [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
  websocket_ = std::make_unique<WebSocketClient>(
      *io_, websocket.host, std::to_string(websocket.port),
      "/stream?streams=" + symbol + "@depth/" + symbol + "@trade", ws_tls);
  binance_spot::StreamConfig stream_config;
  stream_config.symbol = config_.market.market.native_symbol;
  stream_ = std::make_unique<binance_spot::MarketDataStream>(
      stream_config,
      binance_spot::DepthParser(config_.market.market, config_.scale), *http_,
      *websocket_, Receiver(), clock_);
  boost::asio::co_spawn(*io_, stream_->Run(), boost::asio::detached);
  trading_->Start(*io_);
  if (strategy_.Triggers().timer_period)
    boost::asio::co_spawn(*io_, TimerLoop(), boost::asio::detached);
  thread_ = std::thread([this] { io_->run(); });
}

void Shard::RequestStop() {
  if (stopping_.exchange(true)) return;
  if (io_) {
    boost::asio::post(*io_, [this] {
      if (stream_) stream_->Stop();
      io_->stop();
    });
  }
}

void Shard::Join() {
  if (thread_.joinable()) thread_.join();
}

BookApplyResult Shard::Subscribe(uint64_t connection_id) {
  return book_.Subscribe(connection_id);
}

absl::StatusOr<Decimal> Shard::Price(PriceTicks ticks) const {
  auto count = Decimal::Parse(std::to_string(ticks.value));
  if (!count.ok()) return count.status();
  return count->Multiply(config_.scale.price_per_tick);
}

absl::Status Shard::OnSnapshot(const BookSnapshot& snapshot) {
  return AfterBookApply(book_.OnSnapshot(snapshot));
}

absl::Status Shard::OnDiff(const BookDiff& diff) {
  return AfterBookApply(book_.OnDiff(diff));
}

absl::Status Shard::AfterBookApply(const BookApplyResult& result) {
  auto matched = RunStep([&] { return trading_->OnBook(book_.View()); });
  if (!matched.ok()) return matched;
  const auto policy = strategy_.Triggers();
  const bool book_trigger =
      result.applied &&
      (policy.book_mode == BookTriggerMode::EveryAppliedBatch ||
       (policy.book_mode == BookTriggerMode::BboChanged && result.top_changed));
  return RunPendingTrigger(book_trigger
                               ? std::optional<Trigger>(Trigger::BookChanged)
                               : std::nullopt);
}

absl::Status Shard::OnPublicTrade(const PublicTrade& trade) {
  if (trade.market != config_.market.market || !trade.side) {
    return Error(ErrorCode::kPublicTradeInvalid,
                 "public trade market or side invalid");
  }
  auto price = Price(trade.price_ticks);
  if (!price.ok()) return price.status();
  last_trade_price_ = *price;
  auto matched = RunStep([&] { return trading_->OnPublicTrade(trade); });
  if (!matched.ok()) return matched;
  return RunPendingTrigger(strategy_.Triggers().on_public_trade
                               ? std::optional<Trigger>(Trigger::PublicTraded)
                               : std::nullopt);
}

absl::StatusOr<std::vector<ActionResult>> Shard::OnTimer(InputTime stamp) {
  if (stamp.at_us < 0)
    return Error(ErrorCode::kInputTimeInvalid, "negative input time");
  const auto now_us = clock_.MonoNow().time_since_epoch().count();
  book_.OnTimer(now_us > 0 ? static_cast<uint64_t>(now_us) : 0);
  return RunStrategy(Trigger::Timer, stamp);
}

absl::Status Shard::RunPendingTrigger(std::optional<Trigger> explicit_trigger) {
  if (!pending_trigger_ && !explicit_trigger) return absl::OkStatus();
  const Trigger why = pending_trigger_ ? *pending_trigger_ : *explicit_trigger;
  pending_trigger_.reset();
  const auto elapsed = clock_.MonoNow() - origin_mono_;
  InputTime stamp{std::max<int64_t>(0, elapsed.count()), ++event_ordinal_};
  auto result = RunStrategy(why, stamp);
  return result.ok() ? absl::OkStatus() : result.status();
}

// 运行一次策略：准备输入 → 策略决策 → 执行动作 → 处理动作立刻带来的回报。
// 回报可能再次触发策略（例如撤单确认、成交），这时紧接着再跑一轮，最多两轮，
// 避免策略和回报之间无限循环。
absl::StatusOr<std::vector<ActionResult>> Shard::RunStrategy(
    Trigger why, InputTime stamp, bool allow_followup) {
  // 策略正在运行时又被要求运行：不嵌套调用，记下原因，留给下面的第二轮。
  if (in_strategy_) {
    pending_trigger_ = why;
    return std::vector<ActionResult>{};
  }
  std::vector<ActionResult> all_results;
  for (int pass = 0; pass < (allow_followup ? 2 : 1); ++pass) {
    const auto policy = strategy_.Triggers();
    const MonoTime now = clock_.MonoNow();
    // 节流：距离上次运行太近就不跑。原因：
    //   - 行情可能在一毫秒内连续更新多次，每次都跑策略，会对同一个价格反复
    //     撤单、挂单，白白消耗交易所的请求额度（超限会被限流甚至封 IP）；
    //   - 刚发出的单还没得到交易所确认，又基于不完整的状态再决策一次，容易
    //     重复下单或撤到还没挂上的单；
    //   - 策略和执行都在分片线程上，跑得太频繁会挤占处理行情的时间。
    // coalesce_window：这段时间内的多次触发合并成一次；
    // min_action_interval：两次运行之间的最短间隔。
    // 注意：被节流跳过的触发不会保留，函数结束时会被清掉，策略要等下一次
    // 行情、成交或定时器才会再运行。
    if (last_strategy_at_) {
      const auto elapsed = now - *last_strategy_at_;
      if ((policy.coalesce_window && elapsed < *policy.coalesce_window) ||
          elapsed < policy.min_action_interval)
        break;
    }
    last_strategy_at_ = now;
    in_strategy_ = true;
    // 写成 lambda，是为了无论成功还是失败，之后都能把 in_strategy_ 改回 false。
    auto execute = [&]() -> absl::StatusOr<std::vector<ActionResult>> {
      auto orders = tracker_.ActiveOrders();
      auto balances = trading_->Balances();
      StrategyInput strategy_input{book_.View(),
                                   config_.scale,
                                   config_.rule,
                                   last_trade_price_,
                                   book_.View().State() == BookSyncState::Live,
                                   orders,
                                   balances,
                                   stamp,
                                   clock_};
      ++strategy_invocations_;
      auto batch = strategy_.Decide(strategy_input, why);
      ActionContext action_context{config_.strategy_id,
                                   config_.account,
                                   ActionBatchId{next_action_batch_id_++},
                                   config_.market,
                                   config_.rule,
                                   clock_.UtcNow(),
                                   clock_.MonoNow(),
                                   strategy_input.ready,
                                   true};
      std::vector<ActionResult> results;
      auto executed = RunStep([&] {
        results = action_executor_.Execute(batch, action_context);
        return absl::OkStatus();
      });
      if (!executed.ok()) return executed;
      // 记下每张新订单对应的资金冻结，成交或结束时据此释放。
      // 执行期间到达的回报已在 RunStep 结束时处理，可能设置了
      // pending_trigger_， 下面据此决定是否再跑一轮。
      for (const auto& result : results) {
        if (!result.order) continue;
        if (result.hold_id)
          holds_.emplace(result.order->client_order_id.value, *result.hold_id);
      }
      return results;
    };
    auto results = execute();
    in_strategy_ = false;
    if (!results.ok()) return results.status();
    all_results.insert(all_results.end(), results->begin(), results->end());
    // 运行期间产生了新的触发：用它再跑一轮。
    if (!pending_trigger_) break;
    why = *pending_trigger_;
    pending_trigger_.reset();
  }
  // 最多两轮；第二轮之后、或被节流时剩下的触发都丢掉。
  pending_trigger_.reset();
  return all_results;
}

absl::Status Shard::Record(OrderHistoryRecordPayload payload,
                           const StrategyId& strategy_id) {
  if (shard_sequence_ == std::numeric_limits<uint64_t>::max()) {
    risk_.EmergencyStop();
    return Error(ErrorCode::kSequenceExhausted, "shard sequence exhausted");
  }
  const uint64_t sequence = ++shard_sequence_;
  OrderHistoryRecord record;
  record.run_id = config_.run;
  record.shard = config_.shard;
  record.shard_sequence = sequence;
  record.strategy_id = strategy_id;
  record.received_at_utc = clock_.UtcNow();
  record.payload = std::move(payload);
  if (!recorder_.TryPush(std::move(record))) AddGap(sequence);
  return absl::OkStatus();
}

void Shard::AddGap(uint64_t sequence) {
  if (!local_gaps_.empty() && local_gaps_.back().last_seq + 1 == sequence) {
    local_gaps_.back().last_seq = sequence;
  } else {
    local_gaps_.push_back(OrderHistoryGap{config_.run, config_.shard, sequence,
                                          sequence,
                                          ErrorCode::kOrderHistoryQueueFull});
  }
}

absl::Status Shard::ProcessAccountEvent(const AccountEvent& event) {
  if (const auto* update = std::get_if<OrderUpdate>(&event)) {
    if (update->client_order_id.value.empty())
      return Error(ErrorCode::kShardReportOrderUnknown,
                   "order report without client ID");
    const hquant::Order* order = tracker_.Find(update->client_order_id);
    if (!order)
      return Error(ErrorCode::kShardReportOrderUnknown,
                   "order report for untracked order");
    const bool new_trade =
        update->trade && !tracker_.HasTrade(update->trade->exchange_trade_id);
    auto updated = tracker_.Apply(*update, ReportSource::Push);
    if (!updated.ok()) return updated.status();
    if (*updated == UpdateResult::Ignored) return absl::OkStatus();
    if (new_trade) {
      auto it = holds_.find(update->client_order_id.value);
      if (it != holds_.end()) {
        const bool buy = order->side == Side::Buy;
        auto status = risk_.ApplyTrade(
            it->second,
            buy ? config_.market.quote_asset : config_.market.base_asset,
            buy ? update->trade->value : update->trade->quantity, Decimal());
        if (!status.ok()) return status;
      }
      if (strategy_.Triggers().on_fill) pending_trigger_ = Trigger::Traded;
    }
    if (*updated == UpdateResult::Finished) {
      auto it = holds_.find(update->client_order_id.value);
      if (it != holds_.end()) {
        auto status = risk_.Release(it->second);
        if (!status.ok()) return status;
        holds_.erase(it);
      }
    }
    OrderUpdate recorded = *update;
    if (!new_trade) recorded.trade.reset();
    auto status = Record(std::move(recorded), order->strategy_id);
    if (status.ok() && strategy_.Triggers().on_order_update &&
        !pending_trigger_)
      pending_trigger_ = Trigger::OrderUpdated;
    return status;
  }
  return absl::OkStatus();
}

// 回报的入口，模拟盘和实盘相同。Shard 正在执行一个步骤（处理行情、执行策略
// 动作）时，回报先排队，等步骤结束再处理，避免步骤做到一半时状态被改；
// 不在步骤中（例如实盘的回报从网络到达）时立即处理。
void Shard::OnReport(const AccountEvent& report) {
  queued_reports_.push_back(report);
  if (step_running_) return;
  auto status = ProcessQueuedReports();
  if (!status.ok()) {
    stream_error_ = status;
    risk_.EmergencyStop();
  }
}

// 按到达顺序处理排队的回报。出错时丢弃剩下的回报并返回错误。
absl::Status Shard::ProcessQueuedReports() {
  while (!queued_reports_.empty()) {
    AccountEvent report = std::move(queued_reports_.front());
    queued_reports_.pop_front();
    auto status = ProcessAccountEvent(report);
    if (!status.ok()) {
      queued_reports_.clear();
      return status;
    }
  }
  return absl::OkStatus();
}

}  // namespace hquant
