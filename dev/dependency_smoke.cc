#include <chrono>
#include <cstdint>
#include <string>

#include "CLI/CLI.hpp"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "boost/asio.hpp"
#include "boost/beast/http.hpp"
#include "mpdecimal.h"
#include "openssl/crypto.h"
#include "openssl/ssl.h"
#include "quill/Backend.h"
#include "simdjson.h"
#include "sqlite3.h"
#include "yaml-cpp/yaml.h"
#include "zlib.h"

int main() {
  absl::flat_hash_map<int, int> values{{1, 2}};
  if (values.at(1) != 2 || !absl::OkStatus().ok()) return 1;

  quill::Backend::start();
  quill::Backend::stop();

  boost::asio::io_context io;
  boost::asio::steady_timer timer(io);
  timer.expires_after(std::chrono::milliseconds(0));
  bool timer_fired = false;
  timer.async_wait([&timer_fired](const boost::system::error_code& error) {
    timer_fired = !error;
  });
  io.run();
  if (!timer_fired) return 2;

  boost::beast::http::request<boost::beast::http::string_body> request;
  request.method(boost::beast::http::verb::get);
  request.target("/smoke");
  if (request.target() != "/smoke") return 3;

  if (OPENSSL_init_ssl(0, nullptr) != 1 || OpenSSL_version_num() == 0) return 4;
  if (zlibVersion() == nullptr || mpd_version() == nullptr) return 5;

  simdjson::dom::parser parser;
  const std::string json = R"({"smoke":true})";
  auto document = parser.parse(json);
  if (document.error() != simdjson::SUCCESS) return 6;

  auto config = YAML::Load("smoke: true");
  if (!config["smoke"].as<bool>()) return 7;

  sqlite3* db = nullptr;
  if (sqlite3_open(":memory:", &db) != SQLITE_OK) return 8;
  const int close_result = sqlite3_close(db);
  if (close_result != SQLITE_OK) return 9;

  CLI::App app{"Dependency smoke"};
  bool enabled = false;
  app.add_flag("--smoke", enabled);
  if (app.get_description().empty()) return 10;

  return 0;
}
