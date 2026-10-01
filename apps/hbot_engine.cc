#include <iostream>
#include <string>

#include "hbot/app/config/config.h"
#include "hbot/app/engine.h"

int main(int argc, char** argv) {
  if (argc != 3) {
    std::cerr << "usage: hbot_engine CONFIG STATE_DIR\n";
    return 2;
  }
  auto config = hbot::LoadConfig(argv[1]);
  if (!config.ok()) {
    std::cerr << config.status() << '\n';
    return 2;
  }
  auto result = hbot::RunPaperEngine(*config, argv[2]);
  if (!result.ok()) {
    std::cerr << result << '\n';
    return 1;
  }
  return 0;
}
