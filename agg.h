#ifndef AGG_H
#define AGG_H

#include "postgres.h"
#include "funcapi.h"
#include "nodes/pg_list.h"
#include "xrg.h"
#include "hop/hashagg.h"
#include "kitesdk.h"

typedef struct kite_target_t kite_target_t;
struct kite_target_t {
	Oid aggfn;
	int pgattr;
	List *attrs;
	bool gbykey;
	void *data;
};


typedef struct xrg_agg_t xrg_agg_t;
struct xrg_agg_t {
	int ncol;
	int batchid;
	hagg_t *hagg;
	hagg_iter_t agg_iter;
	int64_t aggdata_memusage;

	List *groupby_attrs;
	List *aggfnoids;
	List *retrieved_attrs;

	xrg_attr_t *attr;
	bool reached_eof;

	int ntlist;
	kite_target_t *tlist;
};

xrg_agg_t *xrg_agg_init(List *retrieved_attrs, List *aggfnoids, List *groupby_attrs);
int xrg_agg_fetch(xrg_agg_t *agg, kite_handle_t *hdl);
int xrg_agg_get_next(xrg_agg_t *agg, AttInMetadata *attinmeta, Datum *datums, bool *flag, int n);

void xrg_agg_destroy(xrg_agg_t *agg);

/* avg_trans_t */
typedef struct avg_trans_t avg_trans_t;
struct avg_trans_t {
	union {
		int64_t i64;
		__int128_t i128;
		double fp64;
	} sum;
	int64_t count;
};

int avg_trans_init(int32_t aggfn, avg_trans_t *pt, const void *p1, xrg_attr_t *attr1,
                const void *p2, xrg_attr_t *attr2);

void aggregate(int32_t aggfn, void *transdata, const void *data, xrg_attr_t *attr);

#endif


