#include "apps/bench_feed.h"

#include <array>
#include <string>
#include <string_view>
#include <vector>

#include "gtest/gtest.h"

namespace {

TEST(BenchFeedTest, ParsesEndpointAndFaultOptions) {
  const std::array<std::string_view, 10> options{"--listen",
                                                 "127.0.0.1:18080",
                                                 "--rate",
                                                 "100",
                                                 "--speed",
                                                 "2",
                                                 "--gap-every",
                                                 "30",
                                                 "--disconnect-every",
                                                 "5"};
  auto parsed = hquant::ParseFeedBenchArguments(options);
  ASSERT_TRUE(parsed.ok()) << parsed.status();
  EXPECT_EQ(parsed->port, 18080);
  EXPECT_EQ(parsed->rate, 100);
  EXPECT_EQ(parsed->gap_every, 30);
  EXPECT_EQ(parsed->disconnect_every, 5);
  const std::array<std::string_view, 4> bad_port{"--listen", "127.0.0.1:0",
                                                 "--rate", "100"};
  EXPECT_FALSE(hquant::ParseFeedBenchArguments(bad_port).ok());
  const std::array<std::string_view, 4> bad_probability{
      "--listen", "127.0.0.1:18080", "--http-429-rate", "2"};
  EXPECT_FALSE(hquant::ParseFeedBenchArguments(bad_probability).ok());

  const std::array<std::string_view, 4> symbols{"--listen", "127.0.0.1:18080",
                                                "--symbols", "BTCUSDT,ETHUSDT"};
  auto multi = hquant::ParseFeedBenchArguments(symbols);
  ASSERT_TRUE(multi.ok()) << multi.status();
  EXPECT_EQ(multi->symbols, (std::vector<std::string>{"BTCUSDT", "ETHUSDT"}));
  const std::array<std::string_view, 4> duplicate{
      "--listen", "127.0.0.1:18080", "--symbols", "BTCUSDT,BTCUSDT"};
  EXPECT_FALSE(hquant::ParseFeedBenchArguments(duplicate).ok());
}

}  // namespace
