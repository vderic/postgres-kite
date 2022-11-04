#include <string.h>
#include "decimal.h"

void decimal128_to_string(__int128 value, int precision, int scale, char *buf, int bufsz) {
	// DECIMAL: Formula: unscaledValue * 10^(-scale)
	// int32: max precision is 9.
	// int64: max precision is 18.
	// int128: max precision is 38.
	// int256: max precision is 76. (not supported).

	assert(scale >= 0 && scale < precision);
	const int sign = (value < 0);
	__uint128_t tmp = (sign ? -value : value);

	char buffer[128];
	char *p = &buffer[sizeof(buffer) - 1];
	*p = 0;

	for (; scale > 0; scale--, precision--) {
		*--p = '0' + (tmp % 10);
		tmp /= 10;
	}

	if (*p) {
		*--p = '.';
	}

	for (; precision > 0 && tmp; precision--) {
		*--p = '0' + (tmp % 10);
		tmp /= 10;
	}

	if (*p == '.' || *p == 0) {
		*--p = '0';
	}

	if (sign) {
		*--p = '-';
	}

	assert(strlen(p) + 1 < bufsz);

	strcpy(buf, p);
}

void decimal64_to_string(int64_t value, int precision, int scale, char *buf, int bufsz) {
	__int128 t = value;
	decimal128_to_string(t, precision, scale, buf, bufsz);
}
