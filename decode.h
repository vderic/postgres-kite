#ifndef _DECODE_H_
#define _DECODE_H_

#include "postgres.h"
#include "xrg.h"

/* decode functions */
int var_decode(char *data, char flag, xrg_attr_t *attr, Oid atttypid, int atttypmod, Datum *pg_datum, bool *pg_isnull, bool int128_to_numeric);

int avg_decode(Oid aggfn, char *data, char flag, xrg_attr_t *attr, Oid atttypid, int atttypmod, Datum *pg_datum, bool *pg_isnull);

#endif
