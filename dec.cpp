#include "dec.hpp"
#include "exx_int.hpp"
#include <string>
#include <utility>
#include <vector>

#define NBASE 10000
#define HALF_NBASE 5000
#define DEC_DIGITS 4
#define MUL_GUARD_DIGITS 2
#define DIV_GUARD_DIGITS 4

static const arrow::Decimal128 const_one = arrow::Decimal128(1);
static const arrow::Decimal128 const_zero = arrow::Decimal128(0);

void dec128_to_string(__int128_t d128, int scale, char *ret) {
	arrow::Decimal128 dec((uint8_t*) &d128);
	std::string s = dec.ToString(scale);
	strcpy(ret, s.c_str());
}

void dec64_to_string(int64_t d64, int scale, char *ret) {
	arrow::Decimal128 dec(0, d64);
	std::string s = dec.ToString(scale);
	strcpy(ret, s.c_str());
}

// value is a string with value of an integer or float number and return
// precision and scale
std::pair<int32_t, int32_t> dec_get_precision_scale(const std::string &value) {
  int precision, scale;
  precision = scale = 0;
  arrow::Decimal128 dec;

  auto status = arrow::Decimal128::FromString(value, &dec, &precision, &scale);
  CHECKX(status == arrow::Status::OK(), "invalid decimal value format");
  return {precision, scale};
}

std::pair<int32_t, int32_t> dec_ADD_SUB_precision_scale(int p1, int s1, int p2,
                                                        int s2) {
  int precision, scale;
  precision = scale = 0;
  // scale = max(s1, s2);
  // precision = max(p1-s1, p2-s2) + 1 + scale
  // assert(p1 != 0 && p2 != 0);
  scale = std::max(s1, s2);
  precision = std::max(p1 - s1, p2 - s2) + 1 + scale;

  CHECKX(precision <= MAX_DEC128_PRECISION,
         exx::concat("decimal ADD: result precision out of range. precision = ",
                     std::to_string(precision)));
  return {precision, scale};
}

std::pair<int32_t, int32_t> dec_MUL_precision_scale(int p1, int s1, int p2,
                                                    int s2) {
  int precision, scale;
  precision = scale = 0;
  // scale = s1 + s2
  // precision = precision = p1 + p2 + 1
  // assert(p1 != 0 && p2 != 0);
  scale = s1 + s2;
  precision = p1 + p2 + 1;
  CHECKX(precision <= MAX_DEC128_PRECISION,
         exx::concat("decimal MUL: result precision out of range. precision = ",
                     std::to_string(precision)));
  return {precision, scale};
}

std::pair<int32_t, int32_t> dec_DIV_precision_scale(int p1, int s1, int p2,
                                                    int s2) {
  int precision, scale;
  precision = scale = 0;
  // scale = max(4, s1 + p2 - s2 + 1)
  // precision = p1 - s1 + s2 + scale
  // assert(p1 != 0 && p2 != 0);
  scale = std::max(4, s1 + p2 - s2 + 1);
  precision = p1 - s1 + s2 + scale;
  CHECKX(precision <= MAX_DEC128_PRECISION,
         exx::concat("decimal DIV: result precision out of range. precision = ",
                     std::to_string(precision)));
  return {precision, scale};
}

std::pair<int32_t, int32_t> dec_MOD_precision_scale(int p1, int s1, int p2,
                                                    int s2) {
  int p = std::max(p1, p2);
  int s = std::max(s1, s2);
  return {p, s};
}

// ret_precision and ret_scale must be calculated by dec_DIV_precison_scale
// A and B must be normalized with same scale
arrow::Decimal128 div_scalar(const arrow::Decimal128 &A,
                             const arrow::Decimal128 &B, int ret_precision,
                             int ret_scale) {

  arrow::Decimal128 ret;

  CHECKX(B != 0, "division by zero");

  if (A == 0) {
    return ret;
  }

  arrow::Result<std::pair<arrow::Decimal128, arrow::Decimal128>> r =
      A.Divide(B);

  CHECKX(r.ok(), "division failed");

  auto pp = r.ValueOrDie();

  int res_ndigits = (ret_scale + DEC_DIGITS - 1) / DEC_DIGITS;
  res_ndigits = std::max(res_ndigits, 1);
  int rscale = res_ndigits * DEC_DIGITS;

  ret = pp.first;
  ret = ret.IncreaseScaleBy(rscale);

  for (int i = 0; i < res_ndigits; i++) {
    arrow::Decimal128 remainder = pp.second;
    remainder *= NBASE;
    auto r = remainder.Divide(B);
    CHECKX(r.ok(), "division failed");

    pp = r.ValueOrDie();
    auto q = pp.first;
    q *= arrow::Decimal128::GetScaleMultiplier((res_ndigits - i - 1) *
                                               DEC_DIGITS);
    ret += q;
  }

  if (rscale > ret_scale) {
    ret = ret.ReduceScaleBy(rscale - ret_scale);
  } else if (rscale < ret_scale) {
    ret = ret.IncreaseScaleBy(ret_scale - rscale);
  }

  CHECKX(ret.FitsInPrecision(ret_precision), "decimal not fit in precision");
  return ret;
}

arrow::Decimal128 dec_floor(const arrow::Decimal128 &A, int scale) {
  arrow::Decimal128 whole, fraction, ret;
  A.GetWholeAndFraction(scale, &whole, &fraction);
  if (whole.IsNegative() && fraction != const_zero) {
    whole -= const_one;
  }
  ret = whole.IncreaseScaleBy(scale);
  return ret;
}

arrow::Decimal128 dec_ceil(const arrow::Decimal128 &A, int scale) {
  arrow::Decimal128 whole, fraction, ret;
  A.GetWholeAndFraction(scale, &whole, &fraction);
  if (!whole.IsNegative() && fraction != const_zero) {
    whole += const_one;
  }
  ret = whole.IncreaseScaleBy(scale);
  return ret;
}

arrow::Decimal128 dec_mod(const arrow::Decimal128 &A, int Ascale,
                          const arrow::Decimal128 &B, int Bscale) {

  arrow::Decimal128 ret;
  CHECKX(B != 0, "division by zero");

  if (Ascale > Bscale) {
    ret = A % B.IncreaseScaleBy(Ascale - Bscale);
  } else if (Ascale < Bscale) {
    ret = A.IncreaseScaleBy(Bscale - Ascale) % B;
  } else {
    ret = A % B;
  }
  return ret;
}

arrow::Decimal128 dec_round(const arrow::Decimal128 &A, int32_t Ascale,
                            int32_t rscale) {
  CHECKX(rscale > 0 && Ascale >= rscale, "round: invalid scale");

  arrow::Decimal128 ret;
  if (Ascale > rscale) {
    int diff = Ascale - rscale;
    ret = A.ReduceScaleBy(diff);
    ret = ret.IncreaseScaleBy(diff);
  } else {
    ret = A;
  }
  return ret;
}
