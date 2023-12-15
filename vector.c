#include "postgres.h"

#include "vector.h"
#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/typcache.h"

Vector *
InitVector(int dim)
{
	Vector	   *result;
	int			size;

	size = VECTOR_SIZE(dim);
	result = (Vector *) palloc0(size);
	SET_VARSIZE(result, size);
	result->dim = dim;

	return result;
}

/*
 * get_type_name
 *        returns the name of the type with the given oid
 *
 * Note: returns a palloc'd copy of the string, or NULL if no such type.
 */
bool
cmp_type_name(Oid oid, const char *type_name)
{
        HeapTuple       tp;

        tp = SearchSysCache(TYPEOID,
                                                ObjectIdGetDatum(oid),
                                                0, 0, 0);
        if (HeapTupleIsValid(tp))
        {
                Form_pg_type typtup = (Form_pg_type) GETSTRUCT(tp);
		bool result = false;

                if (strcmp(NameStr(typtup->typname), type_name) == 0) {
			result = true;
		}
                ReleaseSysCache(tp);
                return result;
        }
        else {
                return false;
	}
}
