#include <iostream>
#include <string>

#include "application/config.h"
#include "application/launcher.h"

int main(int argc, char** argv) {
  if (argc != 3) {
    std::cerr << "usage: hquant_engine CONFIG STATE_DIR\n";
    return 2;
  }
  auto config = hquant::LoadConfig(argv[1]);
  if (!config.ok()) {
    std::cerr << config.status() << '\n';
    return 2;
  }
  auto result = hquant::Launch(*config, argv[2]);
  if (!result.ok()) {
    std::cerr << result << '\n';
    return 1;
  }
  return 0;
}
