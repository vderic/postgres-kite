#ifndef _DEC_HPP
#define _DEC_HPP

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wredundant-move"
#include <arrow/util/decimal.h>
#pragma GCC diagnostic pop

#include <string>
#include <utility>

#define MAX_DEC128_PRECISION 38
#define MAX_DEC64_PRECISION 18

/* decimal */
void dec128_to_string(__int128_t i128, int scale, char *ret);

void dec64_to_string(int64_t d64, int scale, char *ret);

std::pair<int32_t, int32_t> dec_get_precision_scale(const std::string &value);

std::pair<int32_t, int32_t> dec_ADD_SUB_precision_scale(int p1, int s1, int p2,
                                                        int s2);

std::pair<int32_t, int32_t> dec_MUL_precision_scale(int p1, int s1, int p2,
                                                    int s2);

std::pair<int32_t, int32_t> dec_DIV_precision_scale(int p1, int s1, int p2,
                                                    int s2);

std::pair<int32_t, int32_t> dec_MOD_precision_scale(int p1, int s1, int p2,
                                                    int s2);

arrow::Decimal128 div_scalar(const arrow::Decimal128 &A,
                             const arrow::Decimal128 &B, int ret_precision,
                             int ret_scale);

arrow::Decimal128 dec_floor(const arrow::Decimal128 &A, int scale);
arrow::Decimal128 dec_ceil(const arrow::Decimal128 &A, int scale);

arrow::Decimal128 dec_mod(const arrow::Decimal128 &A, int Ascale,
                          const arrow::Decimal128 &B, int Bscale);

arrow::Decimal128 dec_round(const arrow::Decimal128 &A, int32_t Ascale,
                            int32_t rscale);

#endif
