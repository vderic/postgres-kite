#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <math.h>


#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wredundant-move"
#include <arrow/util/decimal.h>
#pragma GCC diagnostic pop

#include "numeric.hpp"
#include "agg.h"
#include "dec.hpp"

int count_digit(std::uint64_t n) noexcept;

void avg_numeric(void *transdata, const void *data, xrg_attr_t *attr) {
	avg_trans_t *accum = (avg_trans_t *) transdata;
	const avg_trans_t *rec = (const avg_trans_t *) data;
	arrow::Decimal128 *sum = (arrow::Decimal128 *) &accum->sum.i128;
	arrow::Decimal128 *value = (arrow::Decimal128 *) &rec->sum.i128;

	accum->count += rec->count;
	*sum += *value;
}

/* sum */
void sum_numeric(void *transdata, const void *data, xrg_attr_t *attr) {
	arrow::Decimal128 *accum = (arrow::Decimal128 *) transdata;
	const arrow::Decimal128 *dec = (const arrow::Decimal128 *) data;
	*accum += *dec;
}

/* min */
void min_numeric(void *transdata, const void *data, xrg_attr_t *attr) {
	arrow::Decimal128 *accum = (arrow::Decimal128 *) transdata;
	const arrow::Decimal128 *dec = (const arrow::Decimal128 *) data;
	if (*accum > *dec) {
		*accum = *dec;
	}
}

/* max */
void max_numeric(void *transdata, const void *data, xrg_attr_t *attr) {
	arrow::Decimal128 *accum = (arrow::Decimal128 *) transdata;
	const arrow::Decimal128 *dec = (const arrow::Decimal128 *) data;
	if (*accum < *dec) {
		*accum = *dec;
	}
}


int count_digit(std::uint64_t n) noexcept {
	int count = 0;
	for (count = 0; n ; count++) {
		n /= 10;
	}
	return count;
}
int get_precision(const arrow::Decimal128 &dec) {
        arrow::Decimal128 out;
	int precision;
        std::string s = dec.ToIntegerString();
        arrow::Decimal128::FromString(s, &out, &precision);
	return precision;
}


int avg_numeric_finalize(char *data, xrg_attr_t *attr, __int128_t *avg, int *precision, int *scale) {
	try {
		avg_trans_t *accum = (avg_trans_t *) data;
        	arrow::Decimal128 *ret = (arrow::Decimal128 *)avg;
		arrow::Decimal128 *sum = (arrow::Decimal128 *) &accum->sum.i128;
		arrow::Decimal128 count(0, accum->count);

		int p1 = get_precision(*sum);
		int s1 = attr->scale;
		int p2 = count_digit(static_cast<uint64_t>(accum->count));
		int s2 = 0;

		auto [p3, s3] = dec_DIV_precision_scale(p1, s1, p2, s2);
		count = count.IncreaseScaleBy(s1);
		*ret = div_scalar(*sum, count, p3, s3);

		*precision = p3;
		*scale = s3;

	} catch (const std::runtime_error &ex) {
		std::cerr << ex.what() << std::endl;
		return 1;
	} catch (...) {
		return 2;
	}

	return 0;
}

#if 0

int get_precision(const arrow::Decimal128 &dec) {
	if (dec.high_bits() == 0) {
		return count_digit(dec.low_bits());
	} 
	return count_digit(static_cast<uint64_t>(dec.high_bits())) + 20;
}

static constexpr int floor_log10_pow2(int e) noexcept {
    return (e * 1262611) >> 22;
}

static constexpr int ceil_log10_pow2(int e) noexcept {
    return e == 0 ? 0 : floor_log10_pow2(e) + 1;
}

struct digit_count_table_holder_t {
  std::uint64_t entry[64];
};

static constexpr digit_count_table_holder_t generate_digit_count_table() {
    digit_count_table_holder_t table{ {} };
    constexpr std::uint64_t pow10[] = {
        1ull,
        10ull,
        100ull,
        1000ull,
        1'0000ull,
        10'0000ull,
        100'0000ull,
        1000'0000ull,
        1'0000'0000ull,
        10'0000'0000ull,
        100'0000'0000ull,
        1000'0000'0000ull,
        1'0000'0000'0000ull,
        10'0000'0000'0000ull,
        100'0000'0000'0000ull,
        1000'0000'0000'0000ull,
        1'0000'0000'0000'0000ull,
        10'0000'0000'0000'0000ull,
        100'0000'0000'0000'0000ull,
        1000'0000'0000'0000'0000ull
    };

    for (int i = 0; i < 64; ++i) {
        auto const ub = std::uint64_t(ceil_log10_pow2(i));
        assert(ub <= 19);
        table.entry[i] = ((ub + 1) << 52) - (pow10[ub] >> (i / 4));
    }

    return table;
}

static constexpr inline auto digit_count_table = generate_digit_count_table();

static int floor_log2(std::uint64_t n) noexcept {
    return 63 ^ __builtin_clzll(n);
}

int count_digit(std::uint64_t n) noexcept {
    return int((digit_count_table.entry[floor_log2(n)] + (n >> (floor_log2(n) / 4))) >> 52);
}

#endif
