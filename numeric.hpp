#ifndef KITE_NUMERIC_H_
#define KITE_NUMERIC_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "xrg.h"

void avg_numeric(void *transdata, const void *data, xrg_attr_t *attr);

/* sum */
void sum_numeric(void *transdata, const void *data, xrg_attr_t *attr);

/* min */
void min_numeric(void *transdata, const void *data, xrg_attr_t *attr);

/* max */
void max_numeric(void *transdata, const void *data, xrg_attr_t *attr);

int avg_numeric_finalize(char *data, xrg_attr_t *attr, __int128_t *avg, int *precision, int *scale);

int avg_int64_finalize(int64_t i64sum, int64_t i64count, __int128_t *avg, int *precision, int *scale);

int avg_int128_finalize(__int128_t i128sum, int64_t i64count, __int128_t *avg, int *precision, int *scale);

#ifdef __cplusplus
}
#endif


#endif
