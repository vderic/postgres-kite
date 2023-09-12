#include <math.h>
#include "agg.h"
#include "decode.h"
#include "decimal.h"
#include "utils/timestamp.h"
#include "utils/numeric.h"
#include "utils/fmgrprotos.h"
#include "utils/array.h"

static size_t xrg_typ_size(int16_t ptyp) {
	switch (ptyp) {
	case XRG_PTYP_INT8:
		return 1;
	case XRG_PTYP_INT16:
		return 2;
	case XRG_PTYP_INT32:
		return 4;
	case XRG_PTYP_INT64:
		return 8;
	case XRG_PTYP_INT128:
		return 16;
	case XRG_PTYP_FP32:
		return 4;
	case XRG_PTYP_FP64:
		return 8;
	case XRG_PTYP_BYTEA:
		return -1;
	default:
		break;
	}

	return 0;
}

static Oid pg_array_to_element_oid(Oid t) {
	static Oid basic_type[] = {BOOLOID,
		INT2OID,
		INT4OID,
		INT8OID,
		DATEOID,
		TIMEOID,
		TIMESTAMPOID,
		TIMESTAMPTZOID,
		FLOAT4OID,
		FLOAT8OID,
		CASHOID,
		INTERVALOID,
		NUMERICOID,
		BPCHAROID, TEXTOID, VARCHAROID};

	static Oid array_type[] = {1000, // BOOLARRAYOID
		INT2ARRAYOID,
		INT4ARRAYOID,
		INT8ARRAYOID,
		1182, // DATEARRAYOID
		1183, // TIMEARRAYOID
		1115, // TIMESTAMPARRAYOID
		1185, // TIMESTAMPTZARRAYOID
		FLOAT4ARRAYOID,
		FLOAT8ARRAYOID,
		791,  // CASHARRAYOID
		1187, // INTERVALARRAYOID
		1231, // NUMERICARRAYOID
		1014, // BPCHARARRAY
		TEXTARRAYOID,
		1015 // VARCHARARRAY
		};

	int narraytypes = sizeof(array_type) / sizeof(Oid);

	for (int i =  0 ; i < narraytypes ; i++) {
		if (array_type[i] == t) {
			return basic_type[i];
		}
	}

	return InvalidOid;
}

static inline Datum decode_int16(char *data) {
	int16_t *p = (int16_t *)data;
	return Int16GetDatum(*p);
}

static inline Datum decode_char(char *p) {
	return CharGetDatum(*p);
}

static inline Datum decode_int32(char *data) {
	int32_t *p = (int32_t *)data;
	return Int32GetDatum(*p);
}

static inline Datum decode_int64(char *data) {
	int64_t *p = (int64_t *)data;
	return Int64GetDatum(*p);
}

static inline Datum decode_int128(char *data) {
	__int128_t *p = (__int128_t *)data;
	return PointerGetDatum(p);
}

static inline Datum decode_float(char *data) {
	float *p = (float *)data;
	return Float4GetDatum(*p);
}

static inline Datum decode_double(char *data) {
	double *p = (double *)data;
	return Float8GetDatum(*p);
}

static inline Datum decode_date(char *data) {
	int32_t d = *((int32_t *)data);
	d -= (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE);
	return Int32GetDatum(d);
}
static inline Datum decode_time(char *data) {
	int64_t t = *((int64_t *)data);
	return Int64GetDatum(t);
}

static Datum decode_timestamp(char *data) {
	int64_t ts = *((int64_t *)data);
	Timestamp epoch_ts = SetEpochTimestamp();
	ts += epoch_ts;
	return Int64GetDatum(ts);
}


static Datum decode_dateav(xrg_array_header_t *arr, int sz, Oid atttypid, int atttypmod) {
	int16_t ptyp = xrg_array_ptyp(arr);
	int16_t ltyp = xrg_array_ltyp(arr);
	int ndim = xrg_array_ndim(arr);
	int ndims = (ndim == 0 ? 0 : *xrg_array_dims(arr));
	char *p = xrg_array_data_ptr(arr);
	int itemsz = xrg_typ_size(ptyp);
	char *nullmap = xrg_array_nullbitmap(arr);

	if (sz != xrg_array_size(arr)) {
		elog(ERROR, "array size does not match with header length");
	}
	if (ltyp != XRG_LTYP_DATE) {
		elog(ERROR, "dateav logical type is not date");
	}
	if (itemsz != sizeof(int32_t)) {
		elog(ERROR, "date item size != 4");
	}

	// allocate aligned buffer
	xrg_array_header_t *ret = (xrg_array_header_t *) palloc(sz);
	memcpy(ret, arr, sz);
	p = xrg_array_data_ptr(ret);
	nullmap = xrg_array_nullbitmap(ret);

	if (ndim == 0) {
		ArrayType *pga = (ArrayType *) ret;
		pga->elemtype = pg_array_to_element_oid(atttypid);
		SET_VARSIZE(pga, sz);
		return PointerGetDatum(pga);
	}

	for (int i = 0 ; i < ndims ; i++) {
		if (!xrg_array_get_isnull(nullmap, i)) {
			*((int32_t *)p) -= (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE);
			p += sizeof(int32_t);
		}
	}
	ArrayType *pga = (ArrayType *) ret;
	pga->elemtype = pg_array_to_element_oid(atttypid);
	SET_VARSIZE(pga, sz);
	return PointerGetDatum(pga);
}

static Datum decode_timestampav(xrg_array_header_t *arr, int sz, Oid atttypid, int atttypmod) {
	Timestamp epoch_ts = SetEpochTimestamp();
	int16_t ptyp = xrg_array_ptyp(arr);
	int16_t ltyp = xrg_array_ltyp(arr);
	int ndim = xrg_array_ndim(arr);
	int ndims = (ndim == 0 ? 0 : *xrg_array_dims(arr));
	char *p = xrg_array_data_ptr(arr);
	int itemsz = xrg_typ_size(ptyp);
	char *nullmap = xrg_array_nullbitmap(arr);

	if (sz != xrg_array_size(arr)) {
		elog(ERROR, "array size does not match with header length");
	}
	if (ltyp != XRG_LTYP_TIMESTAMP) {
		elog(ERROR, "timestampav logical type is not timestamp");
	}
	if (itemsz != sizeof(int64_t)) {
		elog(ERROR, "timestampav item size != 8");
	}

	// allocate aligned buffer
	xrg_array_header_t *ret = (xrg_array_header_t *) palloc(sz);
	memcpy(ret, arr, sz);
	p = xrg_array_data_ptr(ret);
	nullmap = xrg_array_nullbitmap(ret);

	if (ndim == 0) {
		ArrayType *pga = (ArrayType *) ret;
		pga->elemtype = pg_array_to_element_oid(atttypid);
		SET_VARSIZE(pga, sz);
		return PointerGetDatum(pga);
	}

	for (int i = 0 ; i < ndims ; i++) {
		if (!xrg_array_get_isnull(nullmap, i)) {
			*((int64_t *)p) += epoch_ts;
			p += sizeof(int64_t);
		}
	}
	ArrayType *pga = (ArrayType *) ret;
	pga->elemtype = pg_array_to_element_oid(atttypid);
	SET_VARSIZE(pga, sz);
	return PointerGetDatum(pga);
}

static inline int32_t pgva_pack(char *dst, const void *ptr, int32_t len)
{
	SET_VARSIZE(dst, len + 4);
	memcpy(dst+4, ptr, len);
	return len+4;
}

static Datum decode_stringav(xrg_array_header_t *arr, int sz, Oid atttypid, int atttypmod) {
	int16_t ptyp = xrg_array_ptyp(arr);
	int16_t ltyp = xrg_array_ltyp(arr);
	int ndim = xrg_array_ndim(arr);
	int ndims = (ndim == 0 ? 0 : *xrg_array_dims(arr));
	char *p = xrg_array_data_ptr(arr);
	int itemsz = xrg_typ_size(ptyp);
	char *nullmap = xrg_array_nullbitmap(arr);

	if (sz != xrg_array_size(arr)) {
		elog(ERROR, "array size does not match with header length");
	}
	if (ltyp != XRG_LTYP_STRING) {
		elog(ERROR, "stringav logical type is not String");
	}
	if (itemsz != -1) {
		elog(ERROR, "stringav item size != -1");
	}

	if (ndim == 0) {
		ArrayType *ret = (ArrayType *) arr;
		ret->elemtype = pg_array_to_element_oid(atttypid);
		SET_VARSIZE(ret, sz);
		return PointerGetDatum(ret);
	}

	int total = 0;
	for (int i = 0 ; i < ndims ; i++) {
		if (!xrg_array_get_isnull(nullmap, i)) {
			int len = xrg_bytea_len(p);
			total += xrg_align(4, len+4);
			p += len+4;
		}
	}

	ArrayType *pga = 0;
	if (xrg_array_hasnull(arr)) {
		total += ARR_OVERHEAD_WITHNULLS(ndim, ndims);
		pga = (ArrayType *) palloc(total);
		memcpy(pga, arr, ARR_OVERHEAD_WITHNULLS(ndim, ndims));
	} else {
		total += ARR_OVERHEAD_NONULLS(ndim);
		pga = (ArrayType *) palloc(total);
		memcpy(pga, arr, ARR_OVERHEAD_NONULLS(ndim));
	}

	pga->elemtype = pg_array_to_element_oid(atttypid);
	SET_VARSIZE(pga, total);

	char *pgp = ARR_DATA_PTR(pga);
	p = xrg_array_data_ptr(arr);

	for (int i = 0 ; i < ndims ; i++) {
		if (!xrg_array_get_isnull(nullmap, i)) {
			int len = xrg_bytea_len(p);
			const char *data = xrg_bytea_ptr(p);
			pgp += xrg_align(4, pgva_pack(pgp, data, len));
			p += len + 4;
		}
	}

	return PointerGetDatum(pga);
}

static Datum decode_dec64av(xrg_array_header_t *arr, int sz, int precision, int scale, Oid atttypid, int atttypmod) {

	ArrayType *ret = 0;
	FmgrInfo flinfo;

	int16_t ptyp = xrg_array_ptyp(arr);
	int16_t ltyp = xrg_array_ltyp(arr);
	int ndim = xrg_array_ndim(arr);
	int ndims = (ndim == 0 ? 0 : *xrg_array_dims(arr));
	char *p = xrg_array_data_ptr(arr);
	int itemsz = xrg_typ_size(ptyp);
	char dst[MAX_DEC128_STRLEN];
	char *nullmap = xrg_array_nullbitmap(arr);

	if (sz != xrg_array_size(arr)) {
		elog(ERROR, "array size does not match with header length");
	}

	if (!(ltyp == XRG_LTYP_DECIMAL && ptyp == XRG_PTYP_INT64)) {
		elog(ERROR, "dec64av ptyp or ltyp not match");
	}

	if (itemsz != sizeof(int64_t)) {
		elog(ERROR, "dec64 is not 64 bit length");
	}

	StringInfoData str;
	initStringInfo(&str);
	appendStringInfoCharMacro(&str, '{');
	for (int i = 0 ; i < ndims ; i++) {
		if (i > 0) {
			appendStringInfoCharMacro(&str, ',');
		}
		if (!xrg_array_get_isnull(nullmap, i)) {
			int64_t v = *((int64_t *)p);
			decimal64_to_string(v, precision, scale, dst, sizeof(dst));
			appendStringInfoString(&str, dst);
			p += sizeof(int64_t);
		}
	}
	appendStringInfoCharMacro(&str, '}');

	memset(&flinfo, 0, sizeof(FmgrInfo));
	fmgr_info_cxt(fmgr_internal_function("array_in"), &flinfo, CurrentMemoryContext);
	ret = (ArrayType *) InputFunctionCall(&flinfo, str.data, pg_array_to_element_oid(atttypid), atttypmod);
	return PointerGetDatum(ret);
}

static Datum decode_dec128av(xrg_array_header_t *arr, int sz, int precision, int scale, Oid atttypid, int atttypmod) {

	ArrayType *ret = 0;
	FmgrInfo flinfo;

	int16_t ptyp = xrg_array_ptyp(arr);
	int16_t ltyp = xrg_array_ltyp(arr);
	int ndim = xrg_array_ndim(arr);
	int ndims = (ndim == 0 ? 0 : *xrg_array_dims(arr));
	char *p = xrg_array_data_ptr(arr);
	int itemsz = xrg_typ_size(ptyp);
	char dst[MAX_DEC128_STRLEN];
	char *nullmap = xrg_array_nullbitmap(arr);

	if (sz != xrg_array_size(arr)) {
		elog(ERROR, "array size does not match with header length");
	}

	if (!(ltyp == XRG_LTYP_DECIMAL && ptyp == XRG_PTYP_INT128)) {
		elog(ERROR, "dec128av ptyp or ltyp not match");
	}

	if (itemsz != sizeof(__int128_t)) {
		elog(ERROR, "dec128 is not 128 bit length");
	}

	StringInfoData str;
	initStringInfo(&str);
	appendStringInfoCharMacro(&str, '{');
	for (int i = 0 ; i < ndims ; i++) {
		if (i > 0) {
			appendStringInfoCharMacro(&str, ',');
		}
		if (!xrg_array_get_isnull(nullmap, i)) {
			__int128_t v = 0;
			memcpy(&v, p, sizeof(__int128_t));
			decimal128_to_string(v, precision, scale, dst, sizeof(dst));
			appendStringInfoString(&str, dst);
			p += sizeof(__int128_t);
		} else {
			appendStringInfoString(&str, "NULL");
		}
	}

	appendStringInfoCharMacro(&str, '}');

	memset(&flinfo, 0, sizeof(FmgrInfo));
	fmgr_info_cxt(fmgr_internal_function("array_in"), &flinfo, CurrentMemoryContext);
	ret = (ArrayType *) InputFunctionCall(&flinfo, str.data, pg_array_to_element_oid(atttypid), atttypmod);
	return PointerGetDatum(ret);
}

static Datum decode_decimalav(xrg_array_header_t *arr, int sz, int precision, int scale, Oid atttypid, int atttypmod) {
	int16_t ptyp = xrg_array_ptyp(arr);
	int16_t ltyp = xrg_array_ltyp(arr);
	int ndim = xrg_array_ndim(arr);

	if (sz != xrg_array_size(arr)) {
		elog(ERROR, "array size does not match with header length");
	}

	if (ltyp != XRG_LTYP_DECIMAL) {
		elog(ERROR, "dec128av ptyp or ltyp not match");
	}

	if (ndim == 0) {
		ArrayType *ret = (ArrayType *) palloc(sz);
		memcpy(ret, arr, sz);
		ret->elemtype = pg_array_to_element_oid(atttypid);
		SET_VARSIZE(ret, sz);
		return PointerGetDatum(ret);
	}

	switch (ptyp) {
	case XRG_PTYP_INT64:
		return decode_dec64av(arr, sz, precision, scale, atttypid, atttypmod);
		break;
	case XRG_PTYP_INT128:
		return decode_dec128av(arr, sz, precision, scale, atttypid, atttypmod);
		break;
	default:
		break;
	}
	return 0;
}

/*
 * the xrg_vector memory buffer is not same alignment as the array so
 * we have to allocate the required aligned buffer and return to postgres
 */
static Datum decode_defaultav(xrg_array_header_t *arr, int sz, Oid atttypid, int atttypmod) {
	if (sz != xrg_array_size(arr)) {
		elog(ERROR, "array size does not match with header length");
	}

	ArrayType *pga = (ArrayType *) arr;
	pga->elemtype = pg_array_to_element_oid(atttypid);
	SET_VARSIZE(pga, sz);
	ArrayType *ret = (ArrayType *) palloc(sz);
	memcpy(ret, pga, sz);

	return PointerGetDatum(ret);
}

/* decode functions */
int var_decode(char *data, char flag, xrg_attr_t *attr, Oid atttypid, int atttypmod, Datum *pg_datum, bool *pg_isnull, bool int128_to_numeric) {
	int ltyp = attr->ltyp;
	int ptyp = attr->ptyp;
	int precision = attr->precision;
	int scale = attr->scale;

	// data in iter->value[idx] and iter->flag[idx] and iter->attrs[idx].ptyp
	*pg_isnull = (flag & XRG_FLAG_NULL);

	switch (ltyp) {
	case XRG_LTYP_NONE:
		// primitive type. no decode here
		switch (ptyp) {
		case XRG_PTYP_INT8: {
			*pg_datum = decode_char(data);
		} break;
		case XRG_PTYP_INT16: {
			*pg_datum = decode_int16(data);
		} break;
		case XRG_PTYP_INT32: {
			*pg_datum = decode_int32(data);
		} break;
		case XRG_PTYP_INT64: {
			*pg_datum = decode_int64(data);
		} break;
		case XRG_PTYP_INT128: {
		        // SIMPLE_AGG needs numeric and PARTIAL_AGG needs int128
		if (int128_to_numeric) {
                	FmgrInfo flinfo;
                	__int128_t v = *((__int128_t *)data);
                	char dst[MAX_DEC128_STRLEN];
			precision = 38;
			scale = 0;
                	decimal128_to_string(v, precision, scale, dst, sizeof(dst));
                	memset(&flinfo, 0, sizeof(FmgrInfo));
                	flinfo.fn_addr = numeric_in;
                	flinfo.fn_nargs = 3;
                	flinfo.fn_strict = true;
                	*pg_datum = InputFunctionCall(&flinfo, dst, 0, atttypmod);
		} else {
			*pg_datum = decode_int128(data);
		}
		} break;
		case XRG_PTYP_FP32: {
			*pg_datum = decode_float(data);
		} break;
		case XRG_PTYP_FP64: {
			*pg_datum = decode_double(data);
		} break;
		default: {
			elog(ERROR, "decode_var: invalid physcial type %d with NONE logical type", ptyp);
			return -1;
		}
		}
		return 0;
	case XRG_LTYP_DATE: {
		*pg_datum = (flag & XRG_FLAG_NULL) ? 0 : decode_date(data);
	}
		return 0;
	case XRG_LTYP_TIME: {
		*pg_datum = (flag & XRG_FLAG_NULL) ? 0 : decode_time(data);
	}
		return 0;
	case XRG_LTYP_TIMESTAMP: {
		*pg_datum = (flag & XRG_FLAG_NULL) ? 0 : decode_timestamp(data);
	}
		return 0;
	case XRG_LTYP_INTERVAL: {
		*pg_datum = (flag & XRG_FLAG_NULL) ? 0 : PointerGetDatum((__int128_t *)data);
	}
		return 0;
	case XRG_LTYP_DECIMAL:
	case XRG_LTYP_STRING:
	case XRG_LTYP_ARRAY:
		break;
	default: {
		elog(ERROR, "invalid xrg logical type %d", ltyp);
		return -1;
	}
	}

	if (ltyp == XRG_LTYP_DECIMAL && ptyp == XRG_PTYP_INT64) {
		FmgrInfo flinfo;
		int64_t v = *((int64_t *)data);
		char dst[MAX_DEC128_STRLEN];
		decimal64_to_string(v, precision, scale, dst, sizeof(dst));
		memset(&flinfo, 0, sizeof(FmgrInfo));
		flinfo.fn_addr = numeric_in;
		flinfo.fn_nargs = 3;
		flinfo.fn_strict = true;
		*pg_datum = InputFunctionCall(&flinfo, dst, 0, atttypmod);
		return 0;
	}

	if (ltyp == XRG_LTYP_DECIMAL && ptyp == XRG_PTYP_INT128) {
		FmgrInfo flinfo;
		__int128_t v = *((__int128_t *)data);
		char dst[MAX_DEC128_STRLEN];
		decimal128_to_string(v, precision, scale, dst, sizeof(dst));
		memset(&flinfo, 0, sizeof(FmgrInfo));
		flinfo.fn_addr = numeric_in;
		flinfo.fn_nargs = 3;
		flinfo.fn_strict = true;
		*pg_datum = InputFunctionCall(&flinfo, dst, 0, atttypmod);
		return 0;
	}

	if (ltyp == XRG_LTYP_STRING && ptyp == XRG_PTYP_BYTEA) {
		int sz = xrg_bytea_len(data);
		if (flag & XRG_FLAG_NULL) {
			*pg_datum = 0;
		} else {
			SET_VARSIZE(data, sz + VARHDRSZ);
			*pg_datum = PointerGetDatum(data);
		}
		return 0;
	}

	// TODO: date, timestamp, numeric need further processing
	if (ltyp == XRG_LTYP_ARRAY && ptyp == XRG_PTYP_BYTEA) {
		xrg_array_header_t *ptr = (xrg_array_header_t *) xrg_bytea_ptr(data);
		int sz = xrg_bytea_len(data);
		//int16_t array_ptyp = xrg_array_ptyp(ptr);
		int16_t array_ltyp = xrg_array_ltyp(ptr);
		if (flag & XRG_FLAG_NULL) {
			*pg_datum = 0;
		} else {

			switch (array_ltyp) {
			case XRG_LTYP_DATE:
			{
				*pg_datum = decode_dateav(ptr, sz, atttypid, atttypmod);
					break;
			}
			case XRG_LTYP_TIMESTAMP:
			{
				*pg_datum = decode_timestampav(ptr, sz, atttypid, atttypmod);
				break;
			}
			case XRG_LTYP_STRING:
			{
				*pg_datum = decode_stringav(ptr, sz, atttypid, atttypmod);
				break;
			}
			case XRG_LTYP_DECIMAL:
			{
				*pg_datum = decode_decimalav(ptr, sz, precision, scale, atttypid, atttypmod);
				break;
			}
			default:
				*pg_datum = decode_defaultav(ptr, sz, atttypid, atttypmod);
				break;
			}
		}
		return 0;
	}

	return 0;
}

int avg_decode(Oid aggfn, char *data, char flag, xrg_attr_t *attr, Oid atttypid, int atttypmod, Datum *pg_datum, bool *pg_isnull) {

	avg_trans_t *accum = (avg_trans_t *)data;

	switch (aggfn) {
	case 2100: // PG_PROC_avg_2100: /* avg int8 */
	{
		FmgrInfo flinfo;
		if (accum->count == 0) {
			*pg_datum = 0;
			*pg_isnull = true;
			break;
		}

		char dst[MAX_DEC128_STRLEN];
		__int128_t v = accum->sum.i128 / accum->count;
		int precision = 38;
		int scale = 0;
		decimal128_to_string(v, precision, scale, dst, sizeof(dst));
		memset(&flinfo, 0, sizeof(FmgrInfo));
		flinfo.fn_addr = numeric_in;
		flinfo.fn_nargs = 3;
		flinfo.fn_strict = true;
		*pg_datum = InputFunctionCall(&flinfo, dst, 0, atttypmod);
		*pg_isnull = false;
		break;
	}
	case 2101: // PG_PROC_avg_2101: /* avg int4 */
	case 2102: // PG_PROC_avg_2102: /* avg int2 */
	{
		FmgrInfo flinfo;
		if (accum->count == 0) {
			*pg_datum = 0;
			*pg_isnull = true;
			break;
		}

		char dst[MAX_DEC128_STRLEN];
		int64_t v = accum->sum.i64 / accum->count;
		int precision = 38;
		int scale = 0;
		decimal64_to_string(v, precision, scale, dst, sizeof(dst));
		memset(&flinfo, 0, sizeof(FmgrInfo));
		flinfo.fn_addr = numeric_in;
		flinfo.fn_nargs = 3;
		flinfo.fn_strict = true;
		*pg_datum = InputFunctionCall(&flinfo, dst, 0, atttypmod);
		*pg_isnull = false;
		break;
	}
	case 2103: // PG_PROC_avg_2103: /* avg numeric */
	{
		FmgrInfo flinfo;
		if (accum->count == 0) {
			*pg_datum = 0;
			*pg_isnull = true;
			break;
		}
		char dst[MAX_DEC128_STRLEN];
		int precision, scale;
		__int128_t v = 0;
		if (avg_numeric_finalize(data, attr, &v, &precision, &scale) != 0) {
			elog(ERROR, "avg_numeric_finalize error");
		}
		decimal128_to_string(v, precision, scale, dst, sizeof(dst));
		memset(&flinfo, 0, sizeof(FmgrInfo));
		flinfo.fn_addr = numeric_in;
		flinfo.fn_nargs = 3;
		flinfo.fn_strict = true;
		*pg_datum = InputFunctionCall(&flinfo, dst, 0, atttypmod);
		*pg_isnull = false;

		break;
	}
	case 2104: // PG_PROC_avg_2104: /* avg float4 */
	case 2105: // PG_PROC_avg_2105: /* avg float8 */
	{
		if (accum->count == 0) {
			*pg_datum = 0;
			*pg_isnull = true;
			break;
		}
		double avg = accum->sum.fp64 / accum->count;
		*pg_datum = Float8GetDatum(avg);
		*pg_isnull = false;
		break;
	}
	default:
		elog(ERROR, "aggfn %d is not average operation", aggfn);
		return 1;
	}
	return 0;
}
