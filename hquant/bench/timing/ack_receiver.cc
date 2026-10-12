// Low-overhead Darwin UDP completion receiver for the live WS benchmark.
// Build with: clang++ -O3 -std=c++17 ack_receiver.cc -o ack_receiver
#include <arpa/inet.h>
#include <mach/mach_time.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include <array>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <vector>

namespace {

constexpr int kTimestampMonotonic = 0x0800;
constexpr int kScmTimestampMonotonic = 0x04;

uint64_t ClockNs(clockid_t clock_id) {
  timespec value{};
  clock_gettime(clock_id, &value);
  return uint64_t(value.tv_sec) * 1'000'000'000 + value.tv_nsec;
}

uint64_t Host64(uint64_t network_value) {
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
  return __builtin_bswap64(network_value);
#else
  return network_value;
#endif
}

int Fail(const char* message) {
  std::perror(message);
  return 1;
}

}  // namespace

int main(int argc, char** argv) {
  if (argc != 3) {
    std::fprintf(stderr, "usage: ack_receiver RECORDS STATUS\n");
    return 2;
  }
  mach_timebase_info_data_t timebase{};
  if (mach_timebase_info(&timebase) != KERN_SUCCESS) {
    return Fail("mach_timebase_info");
  }
  const int fd = socket(AF_INET, SOCK_DGRAM, 0);
  if (fd < 0) return Fail("socket");
  const int requested_buffer = 4 * 1024 * 1024;
  if (setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &requested_buffer,
                 sizeof(requested_buffer)) != 0) return Fail("SO_RCVBUF");
  const int enabled = 1;
  if (setsockopt(fd, SOL_SOCKET, kTimestampMonotonic, &enabled,
                 sizeof(enabled)) != 0) return Fail("SO_TIMESTAMP_MONOTONIC");
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  if (bind(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0) {
    return Fail("bind");
  }
  socklen_t address_size = sizeof(address);
  if (getsockname(fd, reinterpret_cast<sockaddr*>(&address), &address_size) != 0) {
    return Fail("getsockname");
  }
  std::printf("%u\n", unsigned(ntohs(address.sin_port)));
  std::fflush(stdout);

  size_t expected = 0;
  if (std::scanf("%zu", &expected) != 1) return Fail("expected count");
  std::ofstream output(argv[1], std::ios::binary);
  if (!output) return Fail("open records");
  std::vector<std::array<uint64_t, 14>> batch;
  batch.reserve(8192);
  uint64_t malformed = 0;
  uint64_t kernel_timestamped = 0;
  const uint64_t cpu_start = ClockNs(CLOCK_PROCESS_CPUTIME_ID);
  while (kernel_timestamped < expected) {
    std::array<uint64_t, 12> packet{};
    alignas(cmsghdr) char ancillary[CMSG_SPACE(sizeof(uint64_t))]{};
    iovec io{packet.data(), sizeof(packet)};
    msghdr message{};
    message.msg_iov = &io;
    message.msg_iovlen = 1;
    message.msg_control = ancillary;
    message.msg_controllen = sizeof(ancillary);
    const ssize_t size = recvmsg(fd, &message, 0);
    const uint64_t received_ns = ClockNs(CLOCK_MONOTONIC);
    const uint64_t received_uptime_ns = ClockNs(CLOCK_UPTIME_RAW);
    if (size < 0) return Fail("recvmsg");
    if (size != ssize_t(sizeof(packet))) {
      ++malformed;
      continue;
    }
    uint64_t kernel_ticks = 0;
    bool has_kernel_stamp = false;
    for (cmsghdr* control = CMSG_FIRSTHDR(&message); control != nullptr;
         control = CMSG_NXTHDR(&message, control)) {
      if (control->cmsg_level == SOL_SOCKET &&
          control->cmsg_type == kScmTimestampMonotonic &&
          control->cmsg_len >= CMSG_LEN(sizeof(uint64_t))) {
        std::memcpy(&kernel_ticks, CMSG_DATA(control), sizeof(kernel_ticks));
        has_kernel_stamp = true;
        break;
      }
    }
    if (!has_kernel_stamp) {
      ++malformed;
      continue;
    }
    std::array<uint64_t, 14> record{};
    for (size_t i = 0; i < 6; ++i) record[i] = Host64(packet[i]);
    const uint64_t kernel_uptime_ns = uint64_t(
        __uint128_t(kernel_ticks) * timebase.numer / timebase.denom);
    const uint64_t sent_uptime_ns = Host64(packet[6]);
    if (kernel_uptime_ns > received_uptime_ns ||
        sent_uptime_ns > kernel_uptime_ns ||
        received_uptime_ns - kernel_uptime_ns > 60'000'000'000ULL ||
        kernel_uptime_ns - sent_uptime_ns > 60'000'000'000ULL) {
      ++malformed;
      continue;
    }
    record[6] = received_ns;
    record[7] = kernel_uptime_ns - sent_uptime_ns;
    record[8] = received_uptime_ns - kernel_uptime_ns;
    for (size_t i = 0; i < 5; ++i) record[9 + i] = Host64(packet[7 + i]);
    batch.push_back(record);
    ++kernel_timestamped;
    if (batch.size() == 8192) {
      output.write(reinterpret_cast<const char*>(batch.data()),
                   batch.size() * sizeof(batch[0]));
      batch.clear();
    }
  }
  if (!batch.empty()) {
    output.write(reinterpret_cast<const char*>(batch.data()),
                 batch.size() * sizeof(batch[0]));
  }
  output.close();
  if (!output) return Fail("write records");
  std::ofstream status(argv[2]);
  status << "{\"received\":" << kernel_timestamped
         << ",\"malformed\":" << malformed
         << ",\"kernel_timestamped\":" << kernel_timestamped
         << ",\"receiver_cpu_seconds\":"
         << (ClockNs(CLOCK_PROCESS_CPUTIME_ID) - cpu_start) / 1e9 << "}\n";
  close(fd);
  return status ? 0 : 1;
}
