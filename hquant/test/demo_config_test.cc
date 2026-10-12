#include <unistd.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <string>

#include "hquant/config.h"

namespace {

void Require(bool condition, const char* message) {
  if (condition) return;
  std::cerr << message << '\n';
  std::exit(1);
}

std::string Replace(std::string text, const std::string& before,
                    const std::string& after) {
  const size_t position = text.find(before);
  Require(position != std::string::npos, "test fixture substring missing");
  text.replace(position, before.size(), after);
  return text;
}

}  // namespace

int main() {
  const char* runfiles = std::getenv("TEST_SRCDIR");
  const char* workspace = std::getenv("TEST_WORKSPACE");
  const std::filesystem::path fixture =
      runfiles && workspace
          ? std::filesystem::path(runfiles) / workspace /
                "hquant/examples/binance_demo.yaml"
          : std::filesystem::path("hquant/examples/binance_demo.yaml");
  std::ifstream input(fixture);
  Require(static_cast<bool>(input), "cannot read Demo fixture");
  const std::string yaml((std::istreambuf_iterator<char>(input)),
                         std::istreambuf_iterator<char>());
  auto parsed = hquant::v1::ParseConfig(yaml, "/tmp/hquant-demo-config-test");
  Require(parsed.ok(), "read-only Demo configuration rejected");
  auto loaded =
      hquant::v1::LoadConfig(fixture.string(), "/tmp/hquant-demo-config-test");
  Require(
      loaded.ok() &&
          std::filesystem::path(loaded->accounts[0].api_credentials_file) ==
              (fixture.parent_path() / "../local/binance_demo_credentials.yaml")
                  .lexically_normal(),
      "Demo credentials path was not resolved relative to config");
  Require(parsed->accounts[0].mode == hquant::v1::TradingMode::OnlineSim &&
              parsed->accounts[0].max_submissions_per_run == 0,
          "online Demo fixture must be read-only");
  Require(
      !hquant::v1::ParseConfig(Replace(yaml, "mode: online_sim", "mode: live"),
                               "/tmp/hquant-demo-config-test")
           .ok(),
      "production-like live mode accepted");
  Require(
      !hquant::v1::ParseConfig(Replace(yaml, "mode: online_sim", "mode: demo"),
                               "/tmp/hquant-demo-config-test")
           .ok(),
      "obsolete demo mode accepted");
  Require(!hquant::v1::ParseConfig(
               Replace(yaml, "mode: online_sim", "mode: external_sim"),
               "/tmp/hquant-demo-config-test")
               .ok(),
          "obsolete external_sim mode accepted");
  Require(!hquant::v1::ParseConfig(
               Replace(yaml, "demo-api.binance.com", "api.binance.com"),
               "/tmp/hquant-demo-config-test")
               .ok(),
          "production REST endpoint accepted in Demo mode");
  Require(!hquant::v1::ParseConfig(
               Replace(yaml, "demo-stream.binance.com", "stream.binance.com"),
               "/tmp/hquant-demo-config-test")
               .ok(),
          "production market endpoint accepted in Demo mode");
  Require(!hquant::v1::ParseConfig(Replace(yaml, "max_submissions_per_run: 0",
                                           "max_submissions_per_run: 2"),
                                   "/tmp/hquant-demo-config-test")
               .ok(),
          "multiple Demo submissions accepted");
  Require(!hquant::v1::ParseConfig(Replace(yaml, "weight_per_minute: 200",
                                           "weight_per_minute: 6000"),
                                   "/tmp/hquant-demo-config-test")
               .ok(),
          "high Demo request budget accepted");

  char path[] = "/tmp/hquant-demo-credentials-XXXXXX";
  const int fd = ::mkstemp(path);
  Require(fd >= 0, "cannot create credentials fixture");
  ::close(fd);
  {
    std::ofstream file(path);
    file << "api_key: local-test-key\napi_secret: local-test-secret\n";
  }
  auto credentials = hquant::v1::LoadApiCredentialsFile(path);
  Require(credentials.ok() && credentials->key == "local-test-key",
          "owner-only credentials file rejected");
  std::filesystem::permissions(path, std::filesystem::perms::group_read,
                               std::filesystem::perm_options::add);
  Require(!hquant::v1::LoadApiCredentialsFile(path).ok(),
          "group-readable credentials file accepted");
  std::filesystem::remove(path);
}
