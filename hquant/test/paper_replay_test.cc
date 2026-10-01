#include <unistd.h>

#include <chrono>
#include <filesystem>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "base/error.h"
#include "gtest/gtest.h"
#include "offline/history.h"
#include "offline/recorder.h"
#include "order/paper.h"
#include "order/risk.h"
#include "service/shard.h"
#include "strategy/simple_pmm.h"

namespace hquant {
namespace {

Decimal D(const char* text) { return *Decimal::Parse(text); }

class TemporaryDatabase {
 public:
  TemporaryDatabase() {
    path_ =
        (std::filesystem::temp_directory_path() / "hquant_paper_replay_XXXXXX")
            .string();
    const int fd = mkstemp(path_.data());
    if (fd < 0) throw std::runtime_error("mkstemp failed");
    close(fd);
  }
  ~TemporaryDatabase() {
    std::filesystem::remove(path_);
    std::filesystem::remove(path_ + "-wal");
    std::filesystem::remove(path_ + "-shm");
  }
  const std::string& path() const { return path_; }

 private:
  std::string path_;
};

std::optional<HistoryPage> WaitPage(HistoryReader& reader) {
  for (int i = 0; i < 1000; ++i) {
    if (auto page = reader.TryReceive()) return page;
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  return std::nullopt;
}

std::string ReplayOnce() {
  TemporaryDatabase database;
  const UtcTime origin(std::chrono::microseconds(1'000'000));
  ReplayClock clock(origin, MonoTime(std::chrono::microseconds(1'000'000)));
  const AccountId account("PAPER");
  const MarketId market{ExchangeId("paper"), InstrumentKind::Spot, "BTCUSDT"};
  const MarketSpec spec{market, AssetId("BTC"), AssetId("USDT")};
  const OwnerId owner{1, StrategyId("simple_pmm"), std::nullopt};
  const BookScale scale{D("0.01"), D("0.001"), 1};
  const TradingRule rule{market,    D("0.01"),    D("0.001"), D("0.001"),
                         D("0.01"), std::nullopt, 1,          origin};
  SimplePmmConfig strategy_config{owner,
                                  account,
                                  spec,
                                  D("0.01"),
                                  D("0.001"),
                                  D("0.001"),
                                  std::chrono::seconds(15),
                                  PmmPriceType::Mid,
                                  D("0.001"),
                                  true};
  SimplePmm strategy(strategy_config);
  PaperConfig paper_config{
      account,    spec, rule, {{"BTC", D("0.02")}, {"USDT", D("10")}},
      D("0.001"), true, {}};
  PaperConnector paper(std::move(paper_config), clock);
  RiskGate risk({ShardId{0}, D("0"), std::chrono::seconds(300)});
  if (!risk.SetInitialLease({account, AssetId("BTC"), ShardId{0}, 1, D("0.02"),
                             origin + std::chrono::hours(1)})
           .ok() ||
      !risk.SetInitialLease({account, AssetId("USDT"), ShardId{0}, 1, D("10"),
                             origin + std::chrono::hours(1)})
           .ok()) {
    throw std::runtime_error("risk lease setup failed");
  }
  auto recorder =
      SqliteRecorder::Open({database.path(), RunId{1}, origin, 64, 8});
  if (!recorder.ok())
    throw std::runtime_error(std::string(recorder.status().message()));
  ShardRuntime shard({RunId{1}, ShardId{0}, owner, account, spec, scale, rule},
                     clock, strategy, paper, risk, **recorder);

  if (!clock.Advance({0, 1}).ok() ||
      shard.Subscribe(1).state != BookSyncState::Buffering) {
    throw std::runtime_error("subscribe failed");
  }
  BookSnapshot snapshot;
  snapshot.market = market;
  snapshot.scale_version = 1;
  snapshot.stream_epoch = 1;
  snapshot.last_sequence = 10;
  snapshot.bids = {{{9999}, {100}}};
  snapshot.asks = {{{10001}, {100}}};
  snapshot.time = EventTime{{}, clock.UtcNow(), clock.MonoNow()};
  if (!shard.OnSnapshot(snapshot).ok())
    throw std::runtime_error("snapshot failed");
  BookDiff bridge;
  bridge.market = market;
  bridge.scale_version = 1;
  bridge.stream_epoch = 1;
  bridge.first_sequence = 11;
  bridge.last_sequence = 11;
  bridge.time = snapshot.time;
  if (!shard.OnDiff(bridge).ok() ||
      shard.Book().State() != BookSyncState::Live) {
    throw std::runtime_error("bridge failed");
  }
  auto first = shard.OnTimer({0, 1});
  if (!first.ok() || !first->empty())
    throw std::runtime_error("ready tick failed");
  if (!clock.Advance({1, 1}).ok()) throw std::runtime_error("clock failed");
  auto opening = shard.OnTimer({1, 1});
  if (!opening.ok() || opening->size() != 2 || !(*opening)[0].accepted ||
      !(*opening)[1].accepted) {
    throw std::runtime_error("initial quote failed");
  }
  auto open = paper.OpenOrders();
  if (open.size() != 2 || !open[0].request.limit_price ||
      !open[1].request.limit_price ||
      *open[0].request.limit_price->Compare(D("99.9")) != 0 ||
      *open[1].request.limit_price->Compare(D("100.1")) != 0) {
    throw std::runtime_error("initial quote price failed");
  }
  const auto first_buy = open[0].client_id;
  if (!clock.Advance({15'000'001, 1}).ok())
    throw std::runtime_error("clock failed");
  auto refresh = shard.OnTimer({15'000'001, 1});
  if (!refresh.ok() || refresh->size() != 4) {
    throw std::runtime_error("refresh action count failed");
  }
  for (const auto& action : *refresh) {
    if (!action.accepted)
      throw std::runtime_error(
          "refresh action rejected: " + std::string(Info(action.reason).name) +
          ": " + action.message);
  }
  if (paper.OpenOrders().size() != 2 || !shard.Order(first_buy) ||
      shard.Order(first_buy)->display_state != OrderDisplayState::Canceled) {
    throw std::runtime_error("cancel and requote failed");
  }

  if (!clock.Advance({15'000'002, 1}).ok())
    throw std::runtime_error("clock failed");
  bridge.first_sequence = 12;
  bridge.last_sequence = 12;
  bridge.bids = {{{9999}, {0}}, {{9970}, {100}}};
  bridge.asks = {{{10001}, {0}}, {{9980}, {100}}};
  bridge.time = EventTime{{}, clock.UtcNow(), clock.MonoNow()};
  if (!shard.OnDiff(bridge).ok() || paper.OpenOrders().size() != 1) {
    throw std::runtime_error("Paper BBO fill failed");
  }
  if (*paper.BalanceOf(AssetId("BTC")).Compare(D("0.02999")) != 0 ||
      *paper.BalanceOf(AssetId("USDT")).Compare(D("9.001")) != 0 ||
      *paper.FeesPaid(AssetId("BTC")).Compare(D("0.00001")) != 0) {
    throw std::runtime_error("Paper balances or fees wrong");
  }
  if (!(*recorder)->Flush().ok())
    throw std::runtime_error("SQLite flush failed");
  auto reader = HistoryReader::Open({database.path(), 8, 100});
  if (!reader.ok()) throw std::runtime_error("history reader failed");
  HistoryQuery query;
  query.request_id = 1;
  query.page_size = 100;
  if (!(**reader).TrySubmit(query).ok())
    throw std::runtime_error("history query failed");
  auto page = WaitPage(**reader);
  if (!page || !page->status.ok())
    throw std::runtime_error("history page failed");
  size_t intents = 0, trades = 0, decisions = 0;
  for (const auto& row : page->rows) {
    if (std::holds_alternative<OrderIntent>(row.payload)) ++intents;
    if (std::holds_alternative<DecisionRecord>(row.payload)) ++decisions;
    if (const auto* trade = std::get_if<TradeUpdate>(&row.payload)) {
      ++trades;
      if (trade->fees.size() != 1 ||
          *trade->fees[0].signed_amount.Compare(D("0.00001")) != 0) {
        throw std::runtime_error("historical fee wrong");
      }
    }
  }
  if (intents != 4 || decisions != 6 || trades != 1 ||
      !shard.local_gaps().empty()) {
    throw std::runtime_error("history types or gaps wrong");
  }
  if (!(*recorder)->Stop(clock.UtcNow()).ok()) {
    throw std::runtime_error("SQLite stop failed");
  }
  return std::to_string(shard.shard_sequence()) + ":" +
         paper.BalanceOf(AssetId("BTC")).ToString() + ":" +
         paper.BalanceOf(AssetId("USDT")).ToString() + ":" +
         paper.FeesPaid(AssetId("BTC")).ToString();
}

TEST(PaperReplayTest, DeterministicBookStrategyRiskPaperTrackerAndSqlite) {
  EXPECT_EQ(ReplayOnce(), ReplayOnce());
}

}  // namespace
}  // namespace hquant
