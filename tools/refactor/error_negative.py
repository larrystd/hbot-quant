"""Step 0 of docs/refactor/RENAME_PLAN.md. Run from repo root: python3 tools/refactor/error_negative.py .

Negative error numbers: helpers, payload, storage magnitude, control wire,
plus the kCliEngineError split."""
import sys

root = sys.argv[1] + "/hquant/"

def edit(path, pairs):
    p = root + path
    s = open(p).read()
    for old, new, *count in pairs:
        n = count[0] if count else 1
        assert s.count(old) == n, f"{path}: {old!r} x{s.count(old)}"
        s = s.replace(old, new)
    open(p, "w").write(s)

# ---- error.h: new code + helpers
edit("src/base/error.h", [
    ("#include <cstdint>\n", "#include <cstdint>\n#include <optional>\n"),
    ("  kControlTimeout = -19009,\n", "  kControlTimeout = -19009,\n  kCliEngineError = -19010,\n"),
    ("""ErrorCode CodeOf(const absl::Status& status);""",
     """ErrorCode CodeOf(const absl::Status& status);

// The negative number shown to users and sent over the control socket.
inline int32_t ErrorNumber(ErrorCode code) { return static_cast<int32_t>(code); }
// Known code for a number from ErrorNumber; nullopt for kOk or unknown values.
std::optional<ErrorCode> ErrorFromNumber(int64_t number);
// History storage keeps the positive magnitude so rows written before codes
// became negative still decode.
inline uint64_t StoredErrorNumber(ErrorCode code) {
  return static_cast<uint64_t>(-static_cast<int64_t>(code));
}
// Known code for a stored magnitude; nullopt for 0 or unknown values.
std::optional<ErrorCode> ErrorFromStoredNumber(uint64_t stored);"""),
])

# ---- error.cc: registry entry, payload encode/decode, helpers
edit("src/base/error.cc", [
    ("""    {ErrorCode::kControlTimeout, "CONTROL_TIMEOUT", Recovery::Retry,
     absl::StatusCode::kDeadlineExceeded},
}};""",
     """    {ErrorCode::kControlTimeout, "CONTROL_TIMEOUT", Recovery::Retry,
     absl::StatusCode::kDeadlineExceeded},
    {ErrorCode::kCliEngineError, "CLI_ENGINE_ERROR", Recovery::Reject,
     absl::StatusCode::kFailedPrecondition},
}};"""),
    ("constexpr std::array<ErrorInfo, 124> kErrors{{", "constexpr std::array<ErrorInfo, 125> kErrors{{"),
    ("absl::Cord(std::to_string(static_cast<uint16_t>(code))));",
     "absl::Cord(std::to_string(ErrorNumber(code))));"),
    ("""  uint16_t number = 0;
  const auto [end, ec] =
      std::from_chars(encoded.data(), encoded.data() + encoded.size(), number);
  if (ec != std::errc{} || end != encoded.data() + encoded.size() ||
      number == 0)
    return ErrorCode::kInternal;
  const auto code = static_cast<ErrorCode>(number);
  return Info(code).code == code ? code : ErrorCode::kInternal;""",
     """  int64_t number = 0;
  const auto [end, ec] =
      std::from_chars(encoded.data(), encoded.data() + encoded.size(), number);
  if (ec != std::errc{} || end != encoded.data() + encoded.size())
    return ErrorCode::kInternal;
  return ErrorFromNumber(number).value_or(ErrorCode::kInternal);"""),
    ("""Recovery RecoveryOf(const absl::Status& status) {""",
     """std::optional<ErrorCode> ErrorFromNumber(int64_t number) {
  if (number >= 0 || number < std::numeric_limits<int32_t>::min())
    return std::nullopt;
  const auto code = static_cast<ErrorCode>(number);
  if (Info(code).code != code) return std::nullopt;
  return code;
}

std::optional<ErrorCode> ErrorFromStoredNumber(uint64_t stored) {
  if (stored == 0 ||
      stored > static_cast<uint64_t>(std::numeric_limits<int32_t>::max()))
    return std::nullopt;
  return ErrorFromNumber(-static_cast<int64_t>(stored));
}

Recovery RecoveryOf(const absl::Status& status) {"""),
    ("#include <charconv>\n", "#include <charconv>\n#include <limits>\n"),
])

# ---- record codec: store magnitude
edit("src/offline/record_codec.cc", [
    ("""bool ValidErrorCode(uint64_t value) {
  if (value > std::numeric_limits<uint16_t>::max()) return false;
  const auto code = static_cast<ErrorCode>(value);
  return code == ErrorCode::kOk || Info(code).code == code;
}""",
     """// Stored values are magnitudes; 0 means kOk where a record allows it.
bool ValidErrorCode(uint64_t value) {
  return value == 0 || ErrorFromStoredNumber(value).has_value();
}

ErrorCode StoredCode(uint64_t value) {
  return value == 0 ? ErrorCode::kOk : *ErrorFromStoredNumber(value);
}"""),
    ("out.U64(static_cast<uint16_t>(payload.reason));", "out.U64(StoredErrorNumber(payload.reason));", 2),
    (": static_cast<ErrorCode>(reason);", ": StoredCode(reason);", 2),
])

# ---- history reader
edit("src/offline/history.cc", [
    ("""  if (reason < 0 || reason > std::numeric_limits<uint16_t>::max()) return false;
  const auto code = static_cast<ErrorCode>(reason);
  return code != ErrorCode::kOk && Info(code).code == code;""",
     """  return reason > 0 &&
         ErrorFromStoredNumber(static_cast<uint64_t>(reason)).has_value();"""),
    (": static_cast<ErrorCode>(reason);", ": *ErrorFromStoredNumber(reason);"),
    (": static_cast<ErrorCode>(reason)});", ": *ErrorFromStoredNumber(reason)});"),
])
edit("src/offline/recorder.cc", [
    ("sqlite3_bind_int(s, 5, static_cast<int>(gap.reason));",
     "sqlite3_bind_int(s, 5, static_cast<int>(StoredErrorNumber(gap.reason)));"),
])

# ---- control wire and display
edit("src/application/control.cc", [
    ("  const auto value = static_cast<uint16_t>(code);\n  if (value >= 17000 && value < 18000) return \"history\";",
     "  const auto value = -ErrorNumber(code);\n  if (value >= 17000 && value < 18000) return \"history\";"),
    ("std::to_string(static_cast<uint16_t>(error.code)) +", "std::to_string(ErrorNumber(error.code)) +"),
    ("""      uint64_t number = 0;
      std::string_view name;
      if ((*root)["code"].get(number) || number > UINT16_MAX ||
          (*root)["name"].get(name)) {
        return Error(ErrorCode::kControlMessageInvalid,
                     "invalid error code or name");
      }
      code = static_cast<ErrorCode>(number);
      if (code == ErrorCode::kOk || Info(code).code != code ||
          Info(code).name != name) {""",
     """      int64_t number = 0;
      std::string_view name;
      if ((*root)["code"].get(number) || (*root)["name"].get(name)) {
        return Error(ErrorCode::kControlMessageInvalid,
                     "invalid error code or name");
      }
      const auto known = ErrorFromNumber(number);
      code = known.value_or(ErrorCode::kInternal);
      if (!known || Info(code).name != name) {"""),
])
edit("src/cli/cli.cc", [
    ("""    return Error(ErrorCode::kCliStopRejected,
                 std::string(Info(error->code).name) + " (" +
                     std::to_string(static_cast<uint16_t>(error->code)) +""",
     """    return Error(ErrorCode::kCliEngineError,
                 std::string(Info(error->code).name) + " (" +
                     std::to_string(ErrorNumber(error->code)) +"""),
])
edit("src/application/launcher.cc", [
    ("std::to_string(static_cast<uint16_t>(code)) +", "std::to_string(ErrorNumber(code)) +"),
])

# ---- tests
edit("test/cli_test.cc", [
    ("""                  hquant::ErrorCode::kCliStopRejected &&
              std::string(command_error.status().message())
                      .find("HISTORY_BUSY (17007)") != std::string::npos,""",
     """                  hquant::ErrorCode::kCliEngineError &&
              std::string(command_error.status().message())
                      .find("HISTORY_QUEUE_FULL (-17007)") != std::string::npos,"""),
])
edit("test/error_test.cc", [
    ("  std::set<uint16_t> numbers;", "  std::set<int32_t> numbers;"),
    ("EXPECT_EQ(AllErrorInfo().size(), 124);", "EXPECT_EQ(AllErrorInfo().size(), 125);"),
    ("    EXPECT_TRUE(numbers.insert(static_cast<uint16_t>(item.code)).second);",
     "    EXPECT_LT(ErrorNumber(item.code), 0);\n    EXPECT_TRUE(numbers.insert(ErrorNumber(item.code)).second);"),
    ("Info(static_cast<ErrorCode>(19999))", "Info(static_cast<ErrorCode>(-19999))"),
    ('absl::Cord("19999")', 'absl::Cord("-19999")'),
    ('absl::Cord("12002x")', 'absl::Cord("-12002x")'),
])
print("negative ok")
