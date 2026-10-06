// Timed WS frame sender for the live benchmark. The Python harness owns the
// accepted TCP sockets and passes them to this process after WS handshakes.
// Build with: clang++ -O3 -std=c++17 ws_sender.cc -o ws_sender
#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include <array>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <string>
#include <vector>

namespace {

uint64_t ClockNs(clockid_t clock_id) {
  timespec value{};
  clock_gettime(clock_id, &value);
  return uint64_t(value.tv_sec) * 1'000'000'000 + value.tv_nsec;
}

template <typename T>
bool ReadValue(const char*& cursor, const char* end, T& value) {
  if (end - cursor < ssize_t(sizeof(T))) return false;
  std::memcpy(&value, cursor, sizeof(T));
  cursor += sizeof(T);
  return true;
}

struct Event {
  uint64_t offset_ns;
  uint32_t shard;
  uint32_t size;
  const char* frame;
};

bool WriteArray(const std::string& path, const std::vector<uint64_t>& values) {
  std::ofstream output(path, std::ios::binary);
  output.write(reinterpret_cast<const char*>(values.data()),
               values.size() * sizeof(values[0]));
  return bool(output);
}

int Fail(const char* message) {
  std::perror(message);
  return 1;
}

}  // namespace

int main(int argc, char** argv) {
  if (argc < 4) {
    std::fprintf(stderr, "usage: ws_sender EVENTS OUTPUT_PREFIX FD...\n");
    return 2;
  }
  const int input = open(argv[1], O_RDONLY);
  if (input < 0) return Fail("open events");
  struct stat file_status{};
  if (fstat(input, &file_status) != 0) return Fail("stat events");
  const size_t file_size = size_t(file_status.st_size);
  const char* mapped = static_cast<const char*>(
      mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, input, 0));
  if (mapped == MAP_FAILED) return Fail("map events");
  const char* cursor = mapped;
  const char* end = mapped + file_size;
  uint64_t count = 0;
  if (!ReadValue(cursor, end, count)) return Fail("event count");
  std::vector<int> sockets;
  sockets.reserve(argc - 3);
  for (int i = 3; i < argc; ++i) sockets.push_back(std::atoi(argv[i]));
  for (int fd : sockets) {
    const int flags = fcntl(fd, F_GETFL);
    if (flags < 0 || !(flags & O_NONBLOCK)) {
      std::fprintf(stderr, "sender socket %d is not nonblocking\n", fd);
      return 2;
    }
  }
  std::vector<Event> events;
  events.reserve(count);
  for (uint64_t i = 0; i < count; ++i) {
    Event event{};
    if (!ReadValue(cursor, end, event.offset_ns) ||
        !ReadValue(cursor, end, event.shard) ||
        !ReadValue(cursor, end, event.size) ||
        event.shard >= sockets.size() ||
        end - cursor < event.size) {
      std::fprintf(stderr, "bad event file at record %llu\n",
                   static_cast<unsigned long long>(i));
      return 2;
    }
    event.frame = cursor;
    cursor += event.size;
    events.push_back(event);
  }
  if (cursor != end) {
    std::fprintf(stderr, "trailing bytes in event file\n");
    return 2;
  }
  // Load every page before the timed interval so fixture IO is excluded.
  volatile unsigned char touched = 0;
  for (size_t offset = 0; offset < file_size; offset += 4096) {
    touched = static_cast<unsigned char>(touched ^ mapped[offset]);
  }
  if (touched == 255) std::fprintf(stderr, "fixture touch: %u\n", touched);

  std::vector<uint64_t> sent_ns, lag_ns, write_ns;
  std::vector<std::array<uint64_t, 3>> cpu_samples;
  sent_ns.reserve(count + 1);
  sent_ns.push_back(0);
  lag_ns.reserve(count);
  write_ns.reserve(count);
  uint64_t eagain_count = 0;
  uint64_t partial_count = 0;
  const uint64_t cpu_start = ClockNs(CLOCK_PROCESS_CPUTIME_ID);
  const uint64_t scheduled_start = ClockNs(CLOCK_MONOTONIC) + 100'000'000;
  cpu_samples.push_back({0, scheduled_start,
                         ClockNs(CLOCK_THREAD_CPUTIME_ID)});
  uint64_t last_write_ns = scheduled_start;
  uint64_t sequence = 0;
  for (const Event& event : events) {
    const uint64_t target = scheduled_start + event.offset_ns;
    uint64_t now = ClockNs(CLOCK_MONOTONIC);
    while (now < target) {
      // The host's nanosleep wakeup varied by several milliseconds even for
      // sub-millisecond gaps, so use one busy-waiting timing thread.
      now = ClockNs(CLOCK_MONOTONIC);
    }
    const uint64_t sent_at = ClockNs(CLOCK_MONOTONIC);
    const int fd = sockets[event.shard];
    size_t offset = 0;
    while (offset < event.size) {
      const ssize_t written = send(fd, event.frame + offset,
                                   event.size - offset, 0);
      if (written > 0) {
        offset += size_t(written);
        if (offset < event.size) ++partial_count;
        continue;
      }
      if (written < 0 && errno == EINTR) continue;
      if (written < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        ++eagain_count;
        pollfd wait_fd{fd, POLLOUT, 0};
        if (poll(&wait_fd, 1, 1000) > 0) continue;
      }
      return Fail("send frame");
    }
    last_write_ns = ClockNs(CLOCK_MONOTONIC);
    sent_ns.push_back(sent_at);
    lag_ns.push_back(sent_at - target);
    write_ns.push_back(last_write_ns - sent_at);
    ++sequence;
    if (sequence % 1000 == 0 || sequence == count) {
      cpu_samples.push_back({sequence, last_write_ns,
                             ClockNs(CLOCK_THREAD_CPUTIME_ID)});
    }
  }
  const uint64_t cpu_end = ClockNs(CLOCK_PROCESS_CPUTIME_ID);
  const std::string prefix(argv[2]);
  if (!WriteArray(prefix + ".sent_ns.bin", sent_ns) ||
      !WriteArray(prefix + ".lag_ns.bin", lag_ns) ||
      !WriteArray(prefix + ".write_ns.bin", write_ns)) {
    return Fail("write sender samples");
  }
  std::ofstream cpu_output(prefix + ".cpu_samples.bin", std::ios::binary);
  cpu_output.write(reinterpret_cast<const char*>(cpu_samples.data()),
                   cpu_samples.size() * sizeof(cpu_samples[0]));
  if (!cpu_output) return Fail("write sender CPU samples");
  std::ofstream status(prefix + ".json");
  status << "{\"sent\":" << count
         << ",\"send_seconds\":"
         << (last_write_ns - scheduled_start) / 1e9
         << ",\"sender_cpu_seconds\":" << (cpu_end - cpu_start) / 1e9
         << ",\"eagain_count\":" << eagain_count
         << ",\"partial_count\":" << partial_count << "}\n";
  munmap(const_cast<char*>(mapped), file_size);
  close(input);
  return status ? 0 : 1;
}
