#include "hquant/bench/venue/sim_venue.h"

#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/websocket.hpp>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdlib>
#include <future>
#include <iostream>
#include <map>
#include <set>
#include <string>
#include <string_view>
#include <vector>

#include "simdjson.h"

namespace {
namespace net = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
namespace websocket = beast::websocket;
using tcp = net::ip::tcp;
using Ws = websocket::stream<beast::tcp_stream>;
using Book = std::map<double, double>;

void Check(bool condition, const char* message) {
  if (!condition) {
    std::cerr << message << '\n';
    std::exit(1);
  }
}

std::string_view String(simdjson::dom::element value) {
  std::string_view text;
  Check(!value.get(text), "invalid string in public market frame");
  return text;
}

double Number(simdjson::dom::element value) {
  return std::stod(std::string(String(value)));
}

void ApplyLevels(simdjson::dom::element value, Book& book, size_t& zero_rows) {
  simdjson::dom::array rows;
  Check(!value.get(rows), "missing public book levels");
  for (auto row : rows) {
    simdjson::dom::array pair;
    Check(!row.get(pair) && pair.size() == 2, "invalid book level");
    const double price = Number(pair.at(0));
    const double quantity = Number(pair.at(1));
    Check(price > 0 && quantity >= 0, "invalid book price or quantity");
    if (quantity == 0) {
      ++zero_rows;
      book.erase(price);
    } else {
      book[price] = quantity;
    }
  }
}

std::vector<std::string> ReadFrames(Ws& ws, size_t count) {
  std::vector<std::string> result;
  result.reserve(count);
  for (size_t i = 0; i < count; ++i) {
    beast::flat_buffer buffer;
    ws.next_layer().expires_after(std::chrono::seconds(8));
    ws.read(buffer);
    result.push_back(beast::buffers_to_string(buffer.data()));
  }
  return result;
}

std::string Snapshot(net::io_context& io, uint16_t port,
                     std::string_view symbol) {
  beast::tcp_stream stream(io);
  stream.expires_after(std::chrono::seconds(5));
  stream.connect({net::ip::make_address("127.0.0.1"), port});
  http::request<http::empty_body> request{
      http::verb::get, "/api/v3/depth?symbol=" + std::string(symbol), 11};
  request.set(http::field::host, "127.0.0.1");
  http::write(stream, request);
  beast::flat_buffer buffer;
  http::response<http::string_body> response;
  http::read(stream, buffer, response);
  Check(response.result() == http::status::ok, "depth snapshot failed");
  return response.body();
}

void CheckFrames(const std::vector<std::string>& frames,
                 const std::string& symbol, bool special,
                 const std::string& snapshot, std::set<uint64_t>& sequences) {
  const size_t expected = special ? 41 : 31;
  Check(frames.size() == expected, "wrong public frame count for symbol");
  Book bids, asks;
  uint64_t last_update = 0;
  size_t tagged_depth = 0, trades = 0, untagged_depth = 0;
  size_t deep_places = 0, deep_cancels = 0;
  size_t pending_trades = 0, zero_rows_total = 0;
  double pending_quantity = 0;
  for (const auto& body : frames) {
    simdjson::dom::parser parser;
    simdjson::dom::element root;
    Check(!parser.parse(body).get(root), "invalid public market JSON");
    Check(String(root["s"]) == symbol, "wrong public market symbol");
    const auto kind = String(root["e"]);
    if (kind == "trade") {
      uint64_t seq = 0;
      Check(!root["bench_seq"].get(seq) && seq != 0 &&
                sequences.insert(seq).second,
            "missing or duplicate public trade sequence");
      ++trades;
      ++pending_trades;
      pending_quantity += Number(root["q"]);
      Check(Number(root["p"]) > 0, "invalid public trade price");
      bool buyer_maker = false;
      Check(!root["m"].get(buyer_maker) &&
                buyer_maker == ((tagged_depth / 5) % 2 == 0),
            "external sweep side did not alternate");
    } else {
      Check(kind == "depthUpdate", "unexpected public market event");
      uint64_t first = 0, last = 0;
      Check(!root["U"].get(first) && !root["u"].get(last) &&
                first == last && first != 0 &&
                (last_update == 0 || first == last_update + 1),
            "depth update sequence skipped or regressed");
      last_update = last;
      const Book previous_bids = bids;
      const Book previous_asks = asks;
      size_t zero_rows = 0;
      ApplyLevels(root["b"], bids, zero_rows);
      ApplyLevels(root["a"], asks, zero_rows);
      zero_rows_total += zero_rows;
      uint64_t seq = 0;
      if (!root["bench_seq"].get(seq)) {
        Check(seq != 0 && sequences.insert(seq).second,
              "duplicate timed market sequence");
        if (tagged_depth % 5 == 4) {
          Check(pending_trades == (special ? 2U : 1U) &&
                    pending_quantity == (special ? 2.0 : 3.0),
                "sweep trades do not match marketable order quantity");
        } else {
          Check(pending_trades == 0 && pending_quantity == 0,
                "deep-book action produced a trade");
        }
        if (tagged_depth % 5 != 4) {
          size_t changed = 0;
          double delta = 0;
          auto compare = [&](const Book& before, const Book& after) {
            for (const auto& [price, quantity] : after) {
              const auto old = before.find(price);
              const double prior = old == before.end() ? 0 : old->second;
              if (quantity != prior) {
                ++changed;
                delta = quantity - prior;
              }
            }
          };
          compare(previous_bids, bids);
          compare(previous_asks, asks);
          Check(changed == 1 && (delta == 1 || delta == -1) &&
                    zero_rows == 0,
                "deep-book action changed more than one existing lot");
          if (delta == 1)
            ++deep_places;
          else
            ++deep_cancels;
        }
        pending_trades = 0;
        pending_quantity = 0;
        ++tagged_depth;
      } else {
        ++untagged_depth;
        Check(untagged_depth == 1 ||
                  (special && tagged_depth > 0 && tagged_depth % 5 == 0),
              "unexpected untagged public depth update");
      }
    }
  }
  Check(tagged_depth == 25 && trades == (special ? 10U : 5U) &&
            untagged_depth == (special ? 6U : 1U) &&
            deep_places == 10 && deep_cancels == 10 &&
            zero_rows_total == (special ? 5U : 0U) &&
            pending_trades == 0,
        "scheduled market actions or deleted levels are missing");
  Check(bids.size() >= 10 && asks.size() >= 10,
        "final public order book lost initial levels");
  simdjson::dom::parser parser;
  simdjson::dom::element root;
  Check(!parser.parse(snapshot).get(root), "invalid depth snapshot JSON");
  Book snapshot_bids, snapshot_asks;
  size_t zero_rows = 0;
  ApplyLevels(root["bids"], snapshot_bids, zero_rows);
  ApplyLevels(root["asks"], snapshot_asks, zero_rows);
  Check(zero_rows == 0 && bids == snapshot_bids && asks == snapshot_asks,
        "replayed WS depths differ from independent REST snapshot");
}
}  // namespace

int main() {
  hquant::bench::SimVenue venue(2);
  venue.Start();
  net::io_context io;
  auto connect = [&](const std::string& symbol) {
    Ws ws(io);
    ws.next_layer().expires_after(std::chrono::seconds(5));
    ws.next_layer().connect(
        {net::ip::make_address("127.0.0.1"), venue.ports().public_ws});
    std::string lower = symbol;
    std::transform(lower.begin(), lower.end(), lower.begin(),
                   [](unsigned char ch) { return std::tolower(ch); });
    ws.handshake("127.0.0.1", "/stream?streams=" + lower + "@depth/" +
                                  lower + "@trade");
    return ws;
  };
  const std::string special_name = hquant::bench::SimVenue::Symbol(0);
  const std::string normal_name = hquant::bench::SimVenue::Symbol(1);
  Ws special = connect(special_name);
  Ws normal = connect(normal_name);
  auto special_future = std::async(std::launch::async,
                                   [&] { return ReadFrames(special, 41); });
  auto normal_future = std::async(std::launch::async,
                                  [&] { return ReadFrames(normal, 31); });
  venue.BeginReplay(3);
  auto special_frames = special_future.get();
  auto normal_frames = normal_future.get();
  const auto special_snapshot =
      Snapshot(io, venue.ports().public_rest, special_name);
  const auto normal_snapshot =
      Snapshot(io, venue.ports().public_rest, normal_name);
  const auto trace = venue.StopAndCollect();
  Check(trace.market_messages == 65 && trace.market_actions == 55 &&
            trace.account_submits == 0 && trace.account_fills == 0,
        "public-only replay changed venue account state or event counts");
  std::set<uint64_t> sequences;
  CheckFrames(special_frames, special_name, true, special_snapshot, sequences);
  CheckFrames(normal_frames, normal_name, false, normal_snapshot, sequences);
  Check(sequences.size() == 65 && *sequences.begin() == 1 &&
            *sequences.rbegin() == 65,
        "timed public market sequence has a gap");
  std::cout << "sim venue market replay passed\n";
}
