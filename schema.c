#include "schema.h"
#include "vector.h"
#include "access/tupdesc.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_type_d.h"
#include "utils/lsyscache.h"
#include "foreign/foreign.h"
#include "commands/defrem.h"


static const char *xrg_typ_str(int16_t ptyp, int16_t ltyp, bool is_array) {
	switch (ptyp) {
	case XRG_PTYP_INT8:
		return is_array ? "int8[]" : "int8";
	case XRG_PTYP_INT16:
		return is_array ? "int16[]" : "int16";
	case XRG_PTYP_INT32:
		switch (ltyp) {
		case XRG_LTYP_DATE:
			return is_array ? "date[]" : "date";
		default:
			return is_array ? "int32[]" : "int32";
		}
		break;
	case XRG_PTYP_INT64:
		switch (ltyp) {
		case XRG_LTYP_DECIMAL:
			return is_array ? "decimal[]" : "decimal";
		case XRG_LTYP_TIME:
			return is_array ? "time[]" : "time";
		case XRG_LTYP_TIMESTAMP:
			return is_array ? "timestamp[]" : "timestamp";
		default:
			return is_array ? "int64[]" : "int64";
		}

		break;
	case XRG_PTYP_INT128:
		switch (ltyp) {
		case XRG_LTYP_INTERVAL:
			return is_array ? "interval[]" : "interval";
		case XRG_LTYP_DECIMAL:
			return is_array ? "decimal[]" : "decimal";
		default:
			return is_array ? "int128[]" : "int128";
		}

		break;
	case XRG_PTYP_FP32:
		return is_array ? "float[]" : "float";
	case XRG_PTYP_FP64:
		return is_array ? "double[]" : "double";
	case XRG_PTYP_BYTEA:
		switch (ltyp) {
		case XRG_LTYP_STRING:
			return is_array ? "string[]" : "string";
		default:
			return is_array ? "bytea[]" : "bytea";
		}
		break;
	default:
		break;
	}

	return "";
}

static void pg_typ_to_xrg_typ(Oid t, int32_t typmod, int16_t *ptyp, int16_t *ltyp, int16_t *precision, int16_t *scale, bool *is_array) {
	*is_array = false;
	switch (t) {
	case BOOLOID: {
		*ptyp = XRG_PTYP_INT8;
		*ltyp = XRG_LTYP_NONE;
	}
		return;
	case 1000: // array of BOOLOID
	{
		*ptyp = XRG_PTYP_INT8;
		*ltyp = XRG_LTYP_NONE;
		*is_array = true;
	}
		return;
	case INT2OID: {
		*ptyp = XRG_PTYP_INT16;
		*ltyp = XRG_LTYP_NONE;
	}
		return;
	case INT2ARRAYOID: {
		*ptyp = XRG_PTYP_INT16;
		*ltyp = XRG_LTYP_NONE;
		*is_array = true;
	}
		return;
	case INT4OID: {
		*ptyp = XRG_PTYP_INT32;
		*ltyp = XRG_LTYP_NONE;
	}
		return;
	case INT4ARRAYOID: {
		*ptyp = XRG_PTYP_INT32;
		*ltyp = XRG_LTYP_NONE;
		*is_array = true;
	}
		return;
	case INT8OID: {
		*ptyp = XRG_PTYP_INT64;
		*ltyp = XRG_LTYP_NONE;
	}
		return;
	case INT8ARRAYOID: {
		*ptyp = XRG_PTYP_INT64;
		*ltyp = XRG_LTYP_NONE;
		*is_array = true;
	}
		return;
	case DATEOID: {
		*ptyp = XRG_PTYP_INT32;
		*ltyp = XRG_LTYP_DATE;
	}
		return;
	case 1182: // array of date
	{
		*ptyp = XRG_PTYP_INT32;
		*ltyp = XRG_LTYP_DATE;
		*is_array = true;
	}
		return;
	case TIMEOID: {
		*ptyp = XRG_PTYP_INT64;
		*ltyp = XRG_LTYP_TIME;
	}
		return;
	case 1183: // array of time
	{
		*ptyp = XRG_PTYP_INT64;
		*ltyp = XRG_LTYP_TIME;
		*is_array = true;
	}
		return;
	case TIMESTAMPOID: {
		*ptyp = XRG_PTYP_INT64;
		*ltyp = XRG_LTYP_TIMESTAMP;
	}
		return;
	case 1115: // array of timestamp
	{
		*ptyp = XRG_PTYP_INT64;
		*ltyp = XRG_LTYP_TIMESTAMP;
		*is_array = true;
	}
		return;
	case TIMESTAMPTZOID: {
		*ptyp = XRG_PTYP_INT64;
		*ltyp = XRG_LTYP_TIMESTAMP;
	}
		return;
	case 1185: // array of timestamptz
	{
		*ptyp = XRG_PTYP_INT64;
		*ltyp = XRG_LTYP_TIMESTAMP;
		*is_array = true;
	}
		return;
	case FLOAT4OID: {
		*ptyp = XRG_PTYP_FP32;
		*ltyp = XRG_LTYP_NONE;
	}
		return;
	case FLOAT4ARRAYOID: {
		*ptyp = XRG_PTYP_FP32;
		*ltyp = XRG_LTYP_NONE;
		*is_array = true;
	}
		return;
	case FLOAT8OID: {
		*ptyp = XRG_PTYP_FP64;
		*ltyp = XRG_LTYP_NONE;
	}
		return;
	case FLOAT8ARRAYOID: {
		*ptyp = XRG_PTYP_FP64;
		*ltyp = XRG_LTYP_NONE;
		*is_array = true;
	}
		return;
	case CASHOID: {
		*ptyp = XRG_PTYP_INT128;
		*ltyp = XRG_LTYP_DECIMAL;
	}
		return;
	// case UUIDOD:
	// case 2951: // Array of UUID
	case INTERVALOID: {
		*ptyp = XRG_PTYP_INT128;
		*ltyp = XRG_LTYP_INTERVAL;
	}
		return;
	case 1187: // array of interval
	{
		*ptyp = XRG_PTYP_INT128;
		*ltyp = XRG_LTYP_INTERVAL;
		*is_array = true;
	}
		return;
	case NUMERICOID: {
		// Const will have typmod == -1. Set it to INT64 decimal first.
		*ptyp = XRG_PTYP_INT64;
		if (typmod >= (int32)VARHDRSZ) {
			int32_t tmp = typmod - VARHDRSZ;
			*precision = (tmp >> 16) & 0xFFFF;
			*scale = tmp & 0xFFFF;

			if (*precision <= 18) {
				*ptyp = XRG_PTYP_INT64;
			} else {
				*ptyp = XRG_PTYP_INT128;
			}
		}
		*ltyp = XRG_LTYP_DECIMAL;
	}
		return;
	case 1231: // array of NUMERIC
	{
		*ptyp = XRG_PTYP_INT64;
		*ltyp = XRG_LTYP_DECIMAL;
		*is_array = true;
		if (typmod >= (int32)VARHDRSZ) {
			int32_t tmp = typmod - VARHDRSZ;
			*precision = (tmp >> 16) & 0xFFFF;
			*scale = tmp & 0xFFFF;

			if (*precision <= 18) {
				*ptyp = XRG_PTYP_INT64;
			} else {
				*ptyp = XRG_PTYP_INT128;
			}
		}
	}
		return;
	case BYTEAOID: {
		*ptyp = XRG_PTYP_BYTEA;
		*ltyp = XRG_LTYP_NONE;
	}
		return;
	case BPCHAROID:
	case TEXTOID:
	case VARCHAROID: {
		*ptyp = XRG_PTYP_BYTEA;
		*ltyp = XRG_LTYP_STRING;
	}
		return;
	case TEXTARRAYOID:
	case 1014: // array of bpchar
	case 1015: // arrayof varchar
	{
		*ptyp = XRG_PTYP_BYTEA;
		*ltyp = XRG_LTYP_STRING;
		*is_array = true;
	}
		return;
	default: {
		if (cmp_type_name(t, "vector")) {
			*ptyp = XRG_PTYP_FP32;
			*ltyp = XRG_LTYP_NONE;
			*is_array = true;
			// typmod = ndim
		} else {
			*ptyp = XRG_PTYP_BYTEA;
			*ltyp = XRG_LTYP_STRING;
		}
	}
		return;
	}
}

void kite_build_schema(StringInfo schema, Oid relid, TupleDesc tupdesc) {
	int i;
	char       *colname;
	List       *options;
	ListCell   *lc;

	for (i = 1 ; i <= tupdesc->natts ; i++) {
		int16_t ptyp, ltyp, precision, scale;
		bool is_array = false;
		ptyp = ltyp = precision = scale = 0;

		Form_pg_attribute attr = TupleDescAttr(tupdesc, i - 1);

		pg_typ_to_xrg_typ(attr->atttypid, attr->atttypmod, &ptyp, &ltyp, &precision, &scale, &is_array);
		colname = NameStr(attr->attname);
		/* Use attribute name or column_name option. */
		options = GetForeignColumnOptions(relid, i);

		foreach(lc, options) {
			DefElem    *def = (DefElem *) lfirst(lc);

			if (strcmp(def->defname, "column_name") == 0) {
				colname = defGetString(def);
				break;
			}
		}

		const char *type = xrg_typ_str(ptyp, ltyp, is_array);

		if (strcmp(type, "decimal") == 0 || strcmp(type, "decimal[]") == 0) {
			appendStringInfo(schema, "%s:%s:%d:%d\n", colname, type, precision, scale);

		} else {
			appendStringInfo(schema, "%s:%s\n", colname, type);
		}
	}
}

