#ifndef JSON_H
#define JSON_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#include "postgres.h"
#include "access/tupdesc.h"
#include "utils/rel.h"
#include "lib/stringinfo.h"
#include "xrg.h"

/* json helper */
void kite_build_schema(StringInfo schema, Oid relid, TupleDesc tupdesc);


#ifdef __cplusplus
}
#endif

#endif
