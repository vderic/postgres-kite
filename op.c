#include "agg.h"
#include "numeric.hpp"

int avg_trans_init(int32_t aggfn, avg_trans_t *pt, const void *p1, xrg_attr_t *attr1,
                const void *p2, xrg_attr_t *attr2) {

	switch (attr1->ptyp) {
	case XRG_PTYP_INT64:
		pt->sum.i64 = *((int64_t *) p1);
		break;
	case XRG_PTYP_INT128:
		memcpy(&pt->sum.i128, p1, sizeof(__int128_t));
		break;
	case XRG_PTYP_FP64:
		pt->sum.fp64 = *((double *) p1);
		break;
	default:
		elog(LOG, "avg_trans_init: attr1 type not support %d", attr1->ptyp);
		return 1;
	}

	if (attr2->ptyp != XRG_PTYP_INT64) {
		return 1;
	}
	pt->count = *((int64_t*) p2);

        return 0;
}

#define ASSIGN(T, PA, PB) \
{ \
        T B = *((T *) PB); \
        *((T *) PA) = B;  \
}

#define MAX(T, PA, PB) \
{ \
	T A = *((T *) PA); \
	T B = *((T *) PB); \
	if (B > A) {      \
	    *((T *) PA) = B;  \
	}                  \
} 


#define MIN(T, PA, PB)  \
{ \
        T A = *((T *) PA); \
        T B = *((T *) PB); \
        if (B < A) {      \
            *((T *) PA) = B;  \
        }                  \
} 


#define ADD(T, PA, PB) \
{ \
	*((T *) PA) += *((T *) PB); \
}
	
/* average operations */
static void avg_int128(void *transdata, const void *data, xrg_attr_t *attr) {
	avg_trans_t *accum = (avg_trans_t *) transdata;
	avg_trans_t *rec = (avg_trans_t *) data;
	accum->count += rec->count;
	accum->sum.i128 += rec->sum.i128;
}

static void avg_int64(void *transdata, const void *data, xrg_attr_t *attr) {
	avg_trans_t *accum = (avg_trans_t *) transdata;
	avg_trans_t *rec = (avg_trans_t *) data;
	accum->count += rec->count;
	accum->sum.i64 += rec->sum.i64;
}

static void avg_double(void *transdata, const void *data, xrg_attr_t *attr) {
	avg_trans_t *accum = (avg_trans_t *) transdata;
	avg_trans_t *rec = (avg_trans_t *) data;
	accum->count += rec->count;
	accum->sum.fp64 += rec->sum.fp64;
}


void aggregate(int32_t aggfn, transinfo_t *transinfo, const void *data, xrg_attr_t *attr) {

	switch (aggfn) {
	case 2147: // PG_PROC_count_2147:
		transinfo->flag = 0;
		ADD(int64_t, transinfo->transvalue, data);
		return;
	case 2803: // PG_PROC_count_2803:
		transinfo->flag = 0;
		ADD(int64_t, transinfo->transvalue, data);
		return;
	case 2100: // PG_PROC_avg_2100: /* avg int8 */
		transinfo->flag = 0;
		avg_int128(transinfo->transvalue, data, attr);
		return;
	case 2101: // PG_PROC_avg_2101: /* avg int4 */
	case 2102: // PG_PROC_avg_2102: /* avg int2 */
		transinfo->flag = 0;
		avg_int64(transinfo->transvalue, data, attr);
		return;
	case 2103: // PG_PROC_avg_2103: /* avg numeric */
		transinfo->flag = 0;
		avg_numeric(transinfo->transvalue, data, attr);
		return;
	case 2104: // PG_PROC_avg_2104: /* avg float4 */
	case 2105: // PG_PROC_avg_2105: /* avg float8 */
		/* 2106 is avg interval, not supported yet. */
		transinfo->flag = 0;
		avg_double(transinfo->transvalue, data, attr);
		return;

	case 2107: // PG_PROC_sum_2107: /* sum int8 */
		transinfo->flag = 0;
		ADD(__int128_t, transinfo->transvalue, data);
		return;
	case 2108: // PG_PROC_sum_2108: /* sum int4 */
	case 2109: // PG_PROC_sum_2109: /* sum int2 */
		transinfo->flag = 0;
		ADD(int64_t, transinfo->transvalue, data);
		return;
	case 2110: // PG_PROC_sum_2110: /* sum float4 */
	case 2111: // PG_PROC_sum_2111: /* sum float8 */
		/* 2112 is sum cash, nyi */
		/* 2113 is sum interval, nyi */
		transinfo->flag = 0;
		ADD(double, transinfo->transvalue, data);
		return;
	case 2114: // PG_PROC_sum_2114: /* sum numeric */
		transinfo->flag = 0;
		sum_numeric(transinfo->transvalue, data, attr);
		return;
	case 2115: // PG_PROC_max_2115: /* int8 */
		if (transinfo->flag) {
			ASSIGN(int64_t, transinfo->transvalue, data);
			transinfo->flag = 0;
		} else {
			MAX(int64_t, transinfo->transvalue, data);
		}
		return;
	case 2116: // PG_PROC_max_2116: /* int4 */
		if (transinfo->flag) {
			ASSIGN(int32_t, transinfo->transvalue, data);
			transinfo->flag = 0;
		} else {
			MAX(int32_t, transinfo->transvalue, data);
		}
		return;
	case 2117: // PG_PROC_max_2117: /* int2 */
			   /* 2118 is oid, nyi */
		if (transinfo->flag) {
			ASSIGN(int16_t, transinfo->transvalue, data);
			transinfo->flag = 0;
		} else {
			MAX(int16_t, transinfo->transvalue, data);
		}
		return;
	case 2119: // PG_PROC_max_2119: /* float4 */
		if (transinfo->flag) {
			ASSIGN(float, transinfo->transvalue, data);
			transinfo->flag = 0;
		} else {
			MAX(float, transinfo->transvalue, data);
		}
		return;
	case 2120: // PG_PROC_max_2120: /* float8 */
			   /* 2121 is abstime, nyi */
		if (transinfo->flag) {
			ASSIGN(double, transinfo->transvalue, data);
			transinfo->flag = 0;
		} else {
			MAX(double, transinfo->transvalue, data);
		}
		return;
	case 2122: // PG_PROC_max_2122: /* date, same as int4 */
		if (transinfo->flag) {
			ASSIGN(int32_t, transinfo->transvalue, data);
			transinfo->flag = 0;
		} else {
			MAX(int32_t, transinfo->transvalue, data);
		}
		return;
	case 2123: // PG_PROC_max_2123: /* time, same as int8 */
			   /* 2124 is time tz, nyi */
	case 2125: // PG_PROC_max_2125: /* money/cash, same as int8 */
	case 2126: // PG_PROC_max_2126: /* timestamp, same as int8 */
	case 2127: // PG_PROC_max_2127: /* timestamptz, same as int8 */
			   /* 2128, interval nyi */
			   /* NOTE the following: what about collation? */
			   /* case PG_PROC_max_2129:       text */
		if (transinfo->flag) {
			ASSIGN(int64_t, transinfo->transvalue, data);
			transinfo->flag = 0;
		} else {
			MAX(int64_t, transinfo->transvalue, data);
		}
		return;
	case 2130: // PG_PROC_max_2130: /* numeric */
		/* 2050, any arrray nyi */
		/* 2244, bpchar, nyi */
		/* 2797, tid, nyi */
		if (transinfo->flag) {
			if (attr->ptyp == XRG_PTYP_INT64) {
				ASSIGN(int64_t, transinfo->transvalue, data);
			} else if (attr->ptyp == XRG_PTYP_INT128) {
				ASSIGN(__int128_t, transinfo->transvalue, data);
			}
			transinfo->flag = 0;
		} else {
			max_numeric(transinfo->transvalue, data, attr);
		}
		return;

	case 2131: // PG_PROC_min_2131: /* int8 */
		if (transinfo->flag) {
			ASSIGN(int64_t, transinfo->transvalue, data);
			transinfo->flag = 0;
		} else {
			MIN(int64_t, transinfo->transvalue, data);
		}
		return;
	case 2132: // PG_PROC_min_2132: /* int4 */
		if (transinfo->flag) {
			ASSIGN(int32_t, transinfo->transvalue, data);
			transinfo->flag = 0;
		} else {
			MIN(int32_t, transinfo->transvalue, data);
		}
		return;
	case 2133: // PG_PROC_min_2133: /* int2 */
			   /* 2134 is oid, nyi */
		if (transinfo->flag) {
			ASSIGN(int16_t, transinfo->transvalue, data);
			transinfo->flag = 0;
		} else {
			MIN(int16_t, transinfo->transvalue, data);
		}
		return;
	case 2135: // PG_PROC_min_2135: /* float4 */
		if (transinfo->flag) {
			ASSIGN(float, transinfo->transvalue, data);
			transinfo->flag = 0;
		} else {
			MIN(float, transinfo->transvalue, data);
		}
		return;
	case 2136: // PG_PROC_min_2136: /* float8 */
			   /* 2137 is abstime, nyi */
		if (transinfo->flag) {
			ASSIGN(double, transinfo->transvalue, data);
			transinfo->flag = 0;
		} else {
			MIN(double, transinfo->transvalue, data);
		}
		return;
	case 2138: // PG_PROC_min_2138: /* date */
		if (transinfo->flag) {
			ASSIGN(int32_t, transinfo->transvalue, data);
			transinfo->flag = 0;
		} else {
			MIN(int32_t, transinfo->transvalue, data);
		}
		return;
	case 2139: // PG_PROC_min_2139: /* time */
			   /* 2140 is timetz, nyi */
	case 2141: // PG_PROC_min_2141: /* money/cash */
	case 2142: // PG_PROC_min_2142: /* timestamp */
	case 2143: // PG_PROC_min_2143: /* timestamptz */
			   /* 2144, internval */
			   /* NOTE: text, collation? */
			   /* case PG_PROC_min_2145:       */
		if (transinfo->flag) {
			ASSIGN(int64_t, transinfo->transvalue, data);
			transinfo->flag = 0;
		} else {
			MIN(int64_t, transinfo->transvalue, data);
		}
		return;
	case 2146: // PG_PROC_min_2146: /* numeric */
		/* 2051 is any array, nyi */
		/* 2245 bpchar nyi */
		/* 2798 tid nyi */
		if (transinfo->flag) {
			if (attr->ptyp == XRG_PTYP_INT64) {
				ASSIGN(int64_t, transinfo->transvalue, data);
			} else if (attr->ptyp == XRG_PTYP_INT128) {
				ASSIGN(__int128_t, transinfo->transvalue, data);
			}
			transinfo->flag = 0;
		} else {
			min_numeric(transinfo->transvalue, data, attr);
		}
		return;

	default:
		elog(ERROR, "invalid aggfn %d", aggfn);
		return;
	}
}

   
#if 0
static inline int32_t pg_agg_to_op(int32_t funcid) {
	switch (funcid) {
	case 2147: // PG_PROC_count_2147:
		return XRG_OP_COUNT;
	case 2803: // PG_PROC_count_2803:
		return XRG_OP_COUNT_STAR;

	case 2100: // PG_PROC_avg_2100: /* avg int8 */
		return XRG_OP_AVG_INT128;
	case 2101: // PG_PROC_avg_2101: /* avg int4 */
	case 2102: // PG_PROC_avg_2102: /* avg int2 */
		return XRG_OP_AVG_INT64;
	case 2103: // PG_PROC_avg_2103: /* avg numeric */
		return XRG_OP_AVG_NUMERIC;
	case 2104: // PG_PROC_avg_2104: /* avg float4 */
	case 2105: // PG_PROC_avg_2105: /* avg float8 */
		/* 2106 is avg interval, not supported yet. */
		return XRG_OP_AVG_DOUBLE;

	case 2107: // PG_PROC_sum_2107: /* sum int8 */
		return XRG_OP_SUM_INT128;
	case 2108: // PG_PROC_sum_2108: /* sum int4 */
	case 2109: // PG_PROC_sum_2109: /* sum int2 */
		return XRG_OP_SUM_INT64;
	case 2110: // PG_PROC_sum_2110: /* sum float4 */
	case 2111: // PG_PROC_sum_2111: /* sum float8 */
		/* 2112 is sum cash, nyi */
		/* 2113 is sum interval, nyi */
		return XRG_OP_SUM_DOUBLE;
	case 2114: // PG_PROC_sum_2114: /* sum numeric */
		return XRG_OP_SUM_NUMERIC;

	case 2115: // PG_PROC_max_2115: /* int8 */
	case 2116: // PG_PROC_max_2116: /* int4 */
	case 2117: // PG_PROC_max_2117: /* int2 */
			   /* 2118 is oid, nyi */
	case 2119: // PG_PROC_max_2119: /* float4 */
	case 2120: // PG_PROC_max_2120: /* float8 */
			   /* 2121 is abstime, nyi */
	case 2122: // PG_PROC_max_2122: /* date, same as int4 */
	case 2123: // PG_PROC_max_2123: /* time, same as int8 */
			   /* 2124 is time tz, nyi */
	case 2125: // PG_PROC_max_2125: /* money/cash, same as int8 */
	case 2126: // PG_PROC_max_2126: /* timestamp, same as int8 */
	case 2127: // PG_PROC_max_2127: /* timestamptz, same as int8 */
			   /* 2128, interval nyi */
			   /* NOTE the following: what about collation? */
			   /* case PG_PROC_max_2129:       text */
	case 2130: // PG_PROC_max_2130: /* numeric */
		/* 2050, any arrray nyi */
		/* 2244, bpchar, nyi */
		/* 2797, tid, nyi */
		return XRG_OP_MAX;

	case 2131: // PG_PROC_min_2131: /* int8 */
	case 2132: // PG_PROC_min_2132: /* int4 */
	case 2133: // PG_PROC_min_2133: /* int2 */
			   /* 2134 is oid, nyi */
	case 2135: // PG_PROC_min_2135: /* float4 */
	case 2136: // PG_PROC_min_2136: /* float8 */
			   /* 2137 is abstime, nyi */
	case 2138: // PG_PROC_min_2138: /* date */
	case 2139: // PG_PROC_min_2139: /* time */
			   /* 2140 is timetz, nyi */
	case 2141: // PG_PROC_min_2141: /* money/cash */
	case 2142: // PG_PROC_min_2142: /* timestamp */
	case 2143: // PG_PROC_min_2143: /* timestamptz */
			   /* 2144, internval */
			   /* NOTE: text, collation? */
			   /* case PG_PROC_min_2145:       */
	case 2146: // PG_PROC_min_2146: /* numeric */
		/* 2051 is any array, nyi */
		/* 2245 bpchar nyi */
		/* 2798 tid nyi */
		return XRG_OP_MIN;

	default:
		return -1;
	}
}
#endif
