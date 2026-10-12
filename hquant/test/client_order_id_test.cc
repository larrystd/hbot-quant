#include "hquant/trading/client_order_id.h"

#include <cstdlib>
#include <iostream>
#include <set>
#include <string>

int main() {
  std::set<std::string> ids;
  for (int i = 0; i < 10'000; ++i) {
    auto id = hquant::v1::NewClientOrderId();
    if (!id.ok()) {
      std::cerr << id.status() << '\n';
      return EXIT_FAILURE;
    }
    const std::string& value = id->value;
    if (value.size() != 34 || value.substr(0, 2) != "hq" ||
        value.find_first_not_of("0123456789abcdef", 2) != std::string::npos ||
        !ids.insert(value).second) {
      std::cerr << "invalid or repeated client order ID\n";
      return EXIT_FAILURE;
    }
  }
  return EXIT_SUCCESS;
}
