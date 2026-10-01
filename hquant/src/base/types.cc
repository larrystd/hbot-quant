#include "base/types.h"

#include <cstdint>
#include <new>
#include <string>

#include "absl/status/status.h"
#include "base/error.h"

namespace hquant {
namespace {

mpd_context_t Context(int rounding = MPD_ROUND_HALF_EVEN) {
  mpd_context_t context;
  mpd_defaultcontext(&context);
  context.prec = 28;
  context.emax = 999999;
  context.emin = -999999;
  context.round = rounding;
  context.traps = 0;
  context.status = 0;
  return context;
}

absl::Status Validate(const mpd_t* value, uint32_t status) {
  if ((status &
       (MPD_Errors | MPD_Overflow | MPD_Underflow | MPD_Malloc_error)) != 0 ||
      !mpd_isfinite(value)) {
    return Error(ErrorCode::kDecimalInvalid, "invalid or out-of-range decimal");
  }
  return absl::OkStatus();
}

int Rounding(RoundingMode mode) {
  switch (mode) {
    case RoundingMode::Down:
      return MPD_ROUND_DOWN;
    case RoundingMode::Up:
      return MPD_ROUND_UP;
    case RoundingMode::Floor:
      return MPD_ROUND_FLOOR;
    case RoundingMode::Ceiling:
      return MPD_ROUND_CEILING;
    case RoundingMode::HalfEven:
      return MPD_ROUND_HALF_EVEN;
    case RoundingMode::HalfUp:
      return MPD_ROUND_HALF_UP;
  }
  return MPD_ROUND_HALF_EVEN;
}

}  // namespace

void Decimal::Deleter::operator()(mpd_t* value) const { mpd_del(value); }

Decimal::Value Decimal::NewValue() {
  auto context = Context();
  mpd_t* value = mpd_new(&context);
  if (value == nullptr) throw std::bad_alloc();
  return Value(value, Deleter{});
}

Decimal::Decimal() : value_(NewValue()) {
  auto context = Context();
  uint32_t status = 0;
  mpd_qset_string(value_.get(), "0", &context, &status);
}

absl::StatusOr<Decimal> Decimal::Parse(std::string_view text) {
  if (text.empty()) return Error(ErrorCode::kDecimalInvalid, "empty decimal");
  auto value = NewValue();
  auto context = Context();
  uint32_t status = 0;
  const std::string owned(text);
  mpd_qset_string(value.get(), owned.c_str(), &context, &status);
  if (auto error = Validate(value.get(), status); !error.ok()) return error;
  if ((status & (MPD_Inexact | MPD_Rounded)) != 0) {
    return Error(ErrorCode::kDecimalPrecisionExceeded,
                 "decimal exceeds 28 significant digits");
  }
  return Decimal(std::move(value));
}

std::string Decimal::ToString() const {
  auto normalized = NewValue();
  auto context = Context();
  uint32_t status = 0;
  mpd_qreduce(normalized.get(), value_.get(), &context, &status);
  if (!Validate(normalized.get(), status).ok()) throw std::bad_alloc();
  char* text = mpd_to_sci(normalized.get(), 0);
  if (text == nullptr) throw std::bad_alloc();
  std::string result(text);
  mpd_free(text);
  return result;
}

bool Decimal::IsStrictlyPositive() const {
  return !mpd_isnegative(value_.get()) && !mpd_iszero(value_.get());
}

bool Decimal::IsNonnegative() const { return !mpd_isnegative(value_.get()); }

#define HQUANT_DECIMAL_BINARY(method, operation)                               \
  absl::StatusOr<Decimal> Decimal::method(const Decimal& other) const {        \
    auto value = NewValue();                                                   \
    auto context = Context();                                                  \
    uint32_t status = 0;                                                       \
    operation(value.get(), value_.get(), other.value_.get(), &context,         \
              &status);                                                        \
    if (auto error = Validate(value.get(), status); !error.ok()) return error; \
    return Decimal(std::move(value));                                          \
  }

HQUANT_DECIMAL_BINARY(Add, mpd_qadd)
HQUANT_DECIMAL_BINARY(Subtract, mpd_qsub)
HQUANT_DECIMAL_BINARY(Multiply, mpd_qmul)
HQUANT_DECIMAL_BINARY(Divide, mpd_qdiv)
#undef HQUANT_DECIMAL_BINARY

absl::StatusOr<Decimal> Decimal::Quantize(const Decimal& quantum,
                                          RoundingMode rounding) const {
  uint32_t compare_status = 0;
  auto zero = Decimal();
  if (mpd_qcmp(quantum.value_.get(), zero.value_.get(), &compare_status) <= 0 ||
      compare_status != 0) {
    return Error(ErrorCode::kDecimalInvalid, "quantum must be positive");
  }
  auto quotient = NewValue();
  auto integral = NewValue();
  auto result = NewValue();
  auto context = Context(Rounding(rounding));
  uint32_t status = 0;
  mpd_qdiv(quotient.get(), value_.get(), quantum.value_.get(), &context,
           &status);
  if (auto error = Validate(quotient.get(), status); !error.ok()) return error;
  mpd_qround_to_int(integral.get(), quotient.get(), &context, &status);
  if (auto error = Validate(integral.get(), status); !error.ok()) return error;
  mpd_qmul(result.get(), integral.get(), quantum.value_.get(), &context,
           &status);
  if (auto error = Validate(result.get(), status); !error.ok()) return error;
  return Decimal(std::move(result));
}

absl::StatusOr<int> Decimal::Compare(const Decimal& other) const {
  uint32_t status = 0;
  const int comparison = mpd_qcmp(value_.get(), other.value_.get(), &status);
  if (status != 0)
    return Error(ErrorCode::kDecimalInvalid, "decimal comparison failed");
  return comparison;
}

}  // namespace hquant
