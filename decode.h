#ifndef _DECODE_H_
#define _DECODE_H_

#include "postgres.h"
#include "xrg.h"

/* decode functions */
int var_decode(char *data, char flag, xrg_attr_t *attr, int atttypmod, Datum *pg_datum, bool *pg_isnull);

int avg_decode(Oid aggfn, char *data, char flag, xrg_attr_t *attr, int atttypmod, Datum *pg_datum, bool *pg_isnull);

#endif
