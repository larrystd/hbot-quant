#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <map>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <tuple>
#include <utility>
#include <variant>
#include <vector>

#include "application/config.h"
#include "application/quant_server.h"
#include "apps/bench_cli.h"
#include "apps/bench_feed.h"
#include "base/error.h"
#include "gtest/gtest.h"
#include "order_history/record_codec.h"
#include "sqlite3.h"

namespace {

using namespace hquant;

std::filesystem::path RunfilesRoot() {
  const char* srcdir = std::getenv("TEST_SRCDIR");
  const char* workspace = std::getenv("TEST_WORKSPACE");
  if (srcdir && workspace) return std::filesystem::path(srcdir) / workspace;
  return std::filesystem::current_path();
}

std::string ReadFile(const std::filesystem::path& path) {
  std::ifstream file(path);
  return {std::istreambuf_iterator<char>(file), {}};
}

std::string ReplaceOnce(std::string text, std::string_view from,
                        std::string_view to) {
  const auto at = text.find(from);
  if (at == std::string::npos) throw std::runtime_error("replacement missing");
  text.replace(at, from.size(), to);
  return text;
}

uint64_t Metric(std::string_view json, std::string_view key) {
  const auto at = json.find(key);
  if (at == std::string_view::npos) return 0;
  return std::stoull(std::string(json.substr(at + key.size())));
}

std::string Balances(std::string_view json, size_t start = 0) {
  const auto key = json.find("\"balances\":{", start);
  if (key == std::string_view::npos) return {};
  const auto end = json.find('}', key);
  if (end == std::string_view::npos) return {};
  return std::string(json.substr(key, end - key + 1));
}

class TemporaryDirectory {
 public:
  TemporaryDirectory() {
    path_ = "/tmp/hquant_multi_shard_XXXXXX";
    if (!mkdtemp(path_.data())) throw std::runtime_error("mkdtemp failed");
  }
  ~TemporaryDirectory() { std::filesystem::remove_all(path_); }
  const std::string& path() const { return path_; }

 private:
  std::string path_;
};

std::string GetStatus(const std::string& directory) {
  ControlRequest request;
  request.request_id = 1;
  request.payload = StatusRequest{};
  auto response = SendControlRequest(directory, request);
  if (!response.ok())
    throw std::runtime_error(std::string(response.status().message()));
  const auto* status = std::get_if<StatusResponse>(&response->payload);
  if (!status) throw std::runtime_error("status request failed");
  return status->json;
}

using Row = std::tuple<int64_t, int64_t, int>;
struct RunResult {
  std::string status;
  std::map<int, std::vector<Row>> rows;
  std::map<int, std::vector<std::string>> orders;
  bool clean = false;
};

RunResult InspectRun(const std::string& path, std::string status) {
  sqlite3* db = nullptr;
  if (sqlite3_open_v2(path.c_str(), &db, SQLITE_OPEN_READONLY, nullptr) !=
      SQLITE_OK)
    throw std::runtime_error("open history failed");
  RunResult result;
  result.status = std::move(status);
  sqlite3_stmt* query = nullptr;
  const char* sql =
      "SELECT shard,shard_sequence,received_at_us,payload_kind,record_blob "
      "FROM history_records ORDER BY shard,shard_sequence";
  if (sqlite3_prepare_v2(db, sql, -1, &query, nullptr) != SQLITE_OK)
    throw std::runtime_error("prepare history failed");
  while (sqlite3_step(query) == SQLITE_ROW) {
    const int shard = sqlite3_column_int(query, 0);
    result.rows[shard].emplace_back(sqlite3_column_int64(query, 1),
                                    sqlite3_column_int64(query, 2),
                                    sqlite3_column_int(query, 3));
    const auto* blob = static_cast<const char*>(sqlite3_column_blob(query, 4));
    const int length = sqlite3_column_bytes(query, 4);
    auto record =
        storage_internal::DecodeRecord(std::string_view(blob, length));
    if (!record.ok()) throw std::runtime_error("decode history failed");
    if (const auto* order = std::get_if<PreparedOrder>(&record->payload)) {
      result.orders[shard].push_back(
          std::to_string(static_cast<int>(order->request.side)) + ":" +
          order->request.quantity.ToString() + ":" +
          (order->request.limit_price ? order->request.limit_price->ToString()
                                      : std::string("market")));
    }
  }
  sqlite3_finalize(query);
  if (sqlite3_prepare_v2(
          db, "SELECT clean_stopped_at_us IS NOT NULL FROM run_manifest", -1,
          &query, nullptr) != SQLITE_OK)
    throw std::runtime_error("prepare manifest failed");
  result.clean =
      sqlite3_step(query) == SQLITE_ROW && sqlite3_column_int(query, 0) == 1;
  sqlite3_finalize(query);
  sqlite3_close(db);
  return result;
}

RunResult RunReplay(AppConfig config) {
  TemporaryDirectory directory;
  auto server = QuantServer::Create(config, directory.path());
  if (!server.ok())
    throw std::runtime_error(std::string(server.status().message()));
  auto started = (*server)->Start();
  if (!started.ok()) throw std::runtime_error(std::string(started.message()));
  const auto status = GetStatus(directory.path());
  (*server)->RequestStop();
  auto stopped = (*server)->Wait();
  if (!stopped.ok()) throw std::runtime_error(std::string(stopped.message()));
  return InspectRun(directory.path() + "/history.sqlite", status);
}

uint16_t FreePort() {
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) throw std::runtime_error("socket failed");
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = 0;
  if (bind(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0)
    throw std::runtime_error("bind failed");
  socklen_t size = sizeof(address);
  if (getsockname(fd, reinterpret_cast<sockaddr*>(&address), &size) != 0)
    throw std::runtime_error("getsockname failed");
  close(fd);
  return ntohs(address.sin_port);
}

TEST(QuantServerMultiShardTest, ReplayMatchesIndependentShardsAndStopsCleanly) {
  const auto root = RunfilesRoot();
  auto loaded =
      LoadConfig((root / "examples/simulated_replay_multi.yaml").string());
  ASSERT_TRUE(loaded.ok()) << loaded.status();
  loaded->replay_fixture =
      (root / "examples/replay_market_multi.json").string();
  const auto multi = RunReplay(*loaded);
  EXPECT_TRUE(multi.clean);
  ASSERT_EQ(multi.rows.size(), 2);
  EXPECT_EQ(multi.rows.at(0).size(), 20);
  EXPECT_EQ(multi.rows.at(1).size(), 20);
  EXPECT_NE(multi.status.find("\"active_shards\":2"), std::string::npos);
  EXPECT_NE(multi.status.find("\"market\":\"BTC-USDT\",\"book\":\"Live\""),
            std::string::npos);
  EXPECT_NE(multi.status.find("\"market\":\"ETH-USDT\",\"book\":\"Live\""),
            std::string::npos);
  EXPECT_NE(multi.status.find("\"BTC\":\"0.01999\",\"USDT\":\"10.000999\""),
            std::string::npos);
  EXPECT_NE(multi.status.find("\"ETH\":\"0.01999\",\"USDT\":\"10.000999\""),
            std::string::npos);
  for (int index = 0; index < 2; ++index) {
    AppConfig isolated = *loaded;
    isolated.assignments = {loaded->assignments[index]};
    isolated.accounts = {loaded->accounts[index]};
    isolated.market_specs = {loaded->market_specs[index]};
    isolated.strategy_configs = {loaded->strategy_configs[index]};
    isolated.risk_budgets.erase(
        std::remove_if(isolated.risk_budgets.begin(),
                       isolated.risk_budgets.end(),
                       [&](const auto& budget) {
                         return budget.shard != isolated.assignments[0].shard;
                       }),
        isolated.risk_budgets.end());
    isolated.rate_budgets.erase(
        std::remove_if(isolated.rate_budgets.begin(),
                       isolated.rate_budgets.end(),
                       [&](const auto& budget) {
                         return budget.shard != isolated.assignments[0].shard;
                       }),
        isolated.rate_budgets.end());
    isolated.replay_fixture = (root / "examples/replay_market.json").string();
    const auto single = RunReplay(isolated);
    EXPECT_TRUE(single.clean);
    const int shard = isolated.assignments[0].shard.value;
    EXPECT_EQ(multi.rows.at(shard), single.rows.at(shard));
    EXPECT_EQ(multi.orders.at(shard), single.orders.at(shard));
    const auto shard_start =
        multi.status.find("\"shard\":" + std::to_string(shard));
    ASSERT_NE(shard_start, std::string::npos);
    EXPECT_EQ(Balances(multi.status, shard_start), Balances(single.status));
    EXPECT_NE(single.status.find("\"book\":\"Live\""), std::string::npos);
    EXPECT_NE(single.status.find("\"open_orders\":0"), std::string::npos);
    EXPECT_EQ(single.status.find("\"shards\""), std::string::npos);
    const auto base = isolated.market_specs[0].spec.base_asset.value;
    EXPECT_NE(single.status.find("\"" + base + "\":\"0.01999\""),
              std::string::npos);
  }
}

TEST(QuantServerMultiShardTest, RejectsInvalidAssignmentsAndBudgets) {
  const auto root = RunfilesRoot();
  const auto yaml = ReadFile(root / "examples/simulated_replay_multi.yaml");
  const std::vector<std::tuple<std::string, std::string, ErrorCode>> cases = {
      {"  - shard: 1\n    markets: [ETH-USDT]",
       "  - shard: 0\n    markets: [ETH-USDT]",
       ErrorCode::kConfigAssignmentInvalid},
      {"    markets: [ETH-USDT]\n    strategy_ids: [2]",
       "    markets: [BTC-USDT, ETH-USDT]\n    strategy_ids: [2]",
       ErrorCode::kLaunchMultipleNotSupported},
      {"    markets: [ETH-USDT]\n    strategy_ids: [2]",
       "    markets: [BTC-USDT]\n    strategy_ids: [2]",
       ErrorCode::kConfigAssignmentInvalid},
      {"    strategy_ids: [2]\n    accounts: [eth_account]",
       "    strategy_ids: [2]\n    accounts: [btc_account]",
       ErrorCode::kLaunchMultipleNotSupported},
      {"    account: eth_account\n    markets: [ETH-USDT]",
       "    account: btc_account\n    markets: [ETH-USDT]",
       ErrorCode::kConfigReferenceInvalid},
      {"    account: eth_account\n    markets: [ETH-USDT]",
       "    account: eth_account\n    markets: [BTC-USDT]",
       ErrorCode::kConfigReferenceInvalid},
      {"    asset: USDT\n    shard: 1\n    hard_limit: \"10\"",
       "    asset: USDT\n    shard: 1\n    hard_limit: \"11\"",
       ErrorCode::kConfigBudgetInvalid},
      {"strategy: simple_pmm", "strategy: unknown",
       ErrorCode::kConfigFieldInvalid},
  };
  for (const auto& [from, to, expected] : cases) {
    const auto parsed = ParseConfig(ReplaceOnce(yaml, from, to));
    ASSERT_FALSE(parsed.ok()) << from;
    EXPECT_EQ(CodeOf(parsed.status()), expected) << from;
  }
  const auto capped = ParseConfig(
      ReplaceOnce(yaml,
                  "rate_capacities:\n  - account: btc_account\n    ip: local\n"
                  "    endpoint: order\n    limit: 100",
                  "rate_capacities:\n  - account: btc_account\n    ip: local\n"
                  "    endpoint: order\n    limit: 50"));
  ASSERT_FALSE(capped.ok());
  EXPECT_EQ(CodeOf(capped.status()), ErrorCode::kConfigBudgetInvalid);
  const auto no_capacities =
      ParseConfig(yaml.substr(0, yaml.find("rate_capacities:")));
  ASSERT_FALSE(no_capacities.ok());
  EXPECT_EQ(CodeOf(no_capacities.status()), ErrorCode::kConfigBudgetInvalid);
  auto valid = ParseConfig(yaml);
  ASSERT_TRUE(valid.ok()) << valid.status();
  valid->assignments.clear();
  TemporaryDirectory directory;
  auto empty = QuantServer::Create(*valid, directory.path());
  ASSERT_FALSE(empty.ok());
  EXPECT_EQ(CodeOf(empty.status()), ErrorCode::kConfigAssignmentInvalid);
  valid->assignments.resize(9);
  auto too_many = QuantServer::Create(*valid, directory.path());
  ASSERT_FALSE(too_many.ok());
  EXPECT_EQ(CodeOf(too_many.status()), ErrorCode::kConfigAssignmentInvalid);
  auto direct = ParseConfig(yaml);
  ASSERT_TRUE(direct.ok());
  direct->assignments[1].shard = direct->assignments[0].shard;
  auto duplicate_shard = QuantServer::Create(*direct, directory.path());
  ASSERT_FALSE(duplicate_shard.ok());
  EXPECT_EQ(CodeOf(duplicate_shard.status()),
            ErrorCode::kConfigAssignmentInvalid);
  direct = ParseConfig(yaml);
  ASSERT_TRUE(direct.ok());
  direct->assignments[0].markets.clear();
  auto missing_market = QuantServer::Create(*direct, directory.path());
  ASSERT_FALSE(missing_market.ok());
  EXPECT_EQ(CodeOf(missing_market.status()),
            ErrorCode::kLaunchMultipleNotSupported);
  direct = ParseConfig(yaml);
  ASSERT_TRUE(direct.ok());
  direct->rate_capacities.front().global_limit = 50;
  auto rate_overgrant = QuantServer::Create(*direct, directory.path());
  ASSERT_FALSE(rate_overgrant.ok());
  EXPECT_EQ(CodeOf(rate_overgrant.status()), ErrorCode::kConfigBudgetInvalid);
  direct = ParseConfig(yaml);
  ASSERT_TRUE(direct.ok());
  direct->rate_budgets.front().account =
      direct->assignments[1].accounts.front();
  auto wrong_rate_owner = QuantServer::Create(*direct, directory.path());
  ASSERT_FALSE(wrong_rate_owner.ok());
  EXPECT_EQ(CodeOf(wrong_rate_owner.status()), ErrorCode::kConfigBudgetInvalid);
  direct = ParseConfig(yaml);
  ASSERT_TRUE(direct.ok());
  direct->risk_budgets.back().hard_limit = *Decimal::Parse("11");
  auto risk_overgrant = QuantServer::Create(*direct, directory.path());
  ASSERT_FALSE(risk_overgrant.ok());
  EXPECT_EQ(CodeOf(risk_overgrant.status()), ErrorCode::kConfigBudgetInvalid);
}

TEST(QuantServerMultiShardTest, LiveFeedReportsBothShardsAndStopsCleanly) {
  if (std::thread::hardware_concurrency() < 2) GTEST_SKIP();
  const auto root = RunfilesRoot();
  auto config =
      LoadConfig((root / "examples/simulated_replay_multi.yaml").string());
  ASSERT_TRUE(config.ok()) << config.status();
  const uint16_t port = FreePort();
  config->market_data_source = MarketDataSource::BinancePublic;
  config->replay_fixture.reset();
  config->binance_endpoints.rest = {"127.0.0.1", port, false};
  config->binance_endpoints.websocket = {"127.0.0.1", port, false};
  for (size_t i = 0; i < config->market_specs.size(); ++i) {
    auto& market = config->market_specs[i];
    market.spec.market.exchange = ExchangeId("binance");
    market.spec.market.native_symbol = i == 0 ? "BTCUSDT" : "ETHUSDT";
    market.trading_rule.market = market.spec.market;
    config->strategy_configs[i].markets = {market.spec.market};
    config->assignments[i].markets = {market.spec.market};
  }
  TemporaryDirectory directory;
  FeedBenchOptions feed_options;
  feed_options.address = "127.0.0.1";
  feed_options.port = port;
  feed_options.fixture =
      (root / "examples/replay_market_multi_feed.json").string();
  feed_options.symbols = {"BTCUSDT", "ETHUSDT"};
  feed_options.price_per_tick = "0.01";
  feed_options.amount_per_lot = "0.001";
  feed_options.rate = 20;
  feed_options.duration_seconds = 5;
  feed_options.json = true;
  auto server = QuantServer::Create(*config, directory.path());
  ASSERT_TRUE(server.ok()) << server.status();
  ASSERT_TRUE((*server)->Start().ok());
  std::optional<absl::StatusOr<std::string>> feed_result;
  std::thread feed([&] { feed_result = RunFeedBench(feed_options); });
  std::string status;
  bool both_live = false;
  for (int attempt = 0; attempt < 80; ++attempt) {
    try {
      status = GetStatus(directory.path());
    } catch (const std::runtime_error&) {
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
      continue;
    }
    if (status.find("\"active_shards\":2") != std::string::npos &&
        status.find("\"market\":\"BTCUSDT\",\"book\":\"Live\"") !=
            std::string::npos &&
        status.find("\"market\":\"ETHUSDT\",\"book\":\"Live\"") !=
            std::string::npos &&
        status.find("\"strategy_invocations\":0") == std::string::npos) {
      both_live = true;
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  EXPECT_TRUE(both_live) << status;
  (*server)->RequestStop();
  EXPECT_TRUE((*server)->Wait().ok());
  feed.join();
  ASSERT_TRUE(feed_result.has_value());
  EXPECT_TRUE(feed_result->ok()) << feed_result->status();
  if (feed_result->ok()) {
    EXPECT_GE(Metric(feed_result->value(), "\"ws_connections\":"), 2);
    EXPECT_GE(Metric(feed_result->value(), "\"snapshots\":"), 2);
  }
  EXPECT_TRUE(InspectRun(directory.path() + "/history.sqlite", status).clean);
}

TEST(QuantServerMultiShardTest, V1ReplayIsRejectedForMultipleShards) {
  const auto root = RunfilesRoot();
  auto config =
      LoadConfig((root / "examples/simulated_replay_multi.yaml").string());
  ASSERT_TRUE(config.ok());
  config->replay_fixture = (root / "examples/replay_market.json").string();
  TemporaryDirectory directory;
  auto server = QuantServer::Create(*config, directory.path());
  ASSERT_TRUE(server.ok());
  auto started = (*server)->Start();
  ASSERT_FALSE(started.ok());
  EXPECT_EQ(CodeOf(started), ErrorCode::kReplayFileInvalid);
}

TEST(QuantServerMultiShardTest, RejectsUnknownReplayRoutes) {
  const auto root = RunfilesRoot();
  auto config =
      LoadConfig((root / "examples/simulated_replay_multi.yaml").string());
  ASSERT_TRUE(config.ok());
  const auto fixture = ReadFile(root / "examples/replay_market_multi.json");
  for (const auto& [from, to] :
       std::vector<std::pair<std::string, std::string>>{
           {"\"market\": \"ETH-USDT\"", "\"market\": \"UNKNOWN\""},
           {"\"shard\": 1", "\"shard\": 7"},
       }) {
    TemporaryDirectory directory;
    const auto path = std::filesystem::path(directory.path()) / "invalid.json";
    std::ofstream(path) << ReplaceOnce(fixture, from, to);
    config->replay_fixture = path.string();
    auto server = QuantServer::Create(*config, directory.path());
    ASSERT_TRUE(server.ok());
    auto started = (*server)->Start();
    EXPECT_FALSE(started.ok());
    EXPECT_EQ(CodeOf(started), ErrorCode::kReplayFileInvalid);
  }
}

}  // namespace
