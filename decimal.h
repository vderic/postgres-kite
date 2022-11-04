#ifndef _DECIMAL_H
#define _DECIMAL_H

#ifdef __cplusplus
extern "C" {
#endif

#include <assert.h>
#include <errno.h>
#include <inttypes.h>
#include <stdlib.h>

#define MAX_DEC128_STRLEN 48

void decimal128_to_string(__int128 value, int precision, int scale, char *buf, int bufsz);

void decimal64_to_string(int64_t value, int precision, int scale, char *buf, int bufsz);

#ifdef __cplusplus
}
#endif

#endif
