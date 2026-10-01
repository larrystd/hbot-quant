#include <array>
#include <thread>

int main() {
  std::array<int, 128> values{};
  auto fill = [&values](int begin, int end) {
    for (int index = begin; index < end; ++index) {
      values[index] = index + 1;
    }
  };

  std::thread first(fill, 0, 64);
  std::thread second(fill, 64, 128);
  first.join();
  second.join();

  int sum = 0;
  for (int value : values) {
    sum += value;
  }
  return sum == 8256 ? 0 : 1;
}
