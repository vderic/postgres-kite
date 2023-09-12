#include <limits.h>
#include "agg.h"
#include "decode.h"
#include "hop/komihash.h"

extern bool aggfnoid_is_avg(int aggfnoid);

static const char *record_get_flag(const char *p) {
	return p;
}

static const char *record_get_data(const char *p, int ncol) {
	return p+xrg_align(16, ncol);
}

static const char *column_next(xrg_attr_t *attr, const char *p) {
	if (attr->itemsz > 0) {
		p +=  attr->itemsz;
	} else {
		p += xrg_bytea_len(p) + 4;
	}

	return p;
}

static int get_ncol_from_aggfnoids(List *aggfnoids) {
	ListCell *lc;
	int i = 0;

	foreach (lc, aggfnoids) {
		int fn = lfirst_oid(lc);
		if (aggfnoid_is_avg(fn)) {
			i += 2;
		} else {
			i++;
		}
	}
	return i;
}


static int hagg_keyeq(void *context, void *rec1, void *src2) {
	xrg_agg_t *agg = (xrg_agg_t *) context;
	const char *p1, *p2;
	const char *f1;
	xrg_iter_t *iter = (xrg_iter_t *) src2;
	int itemsz = 0;
	xrg_attr_t *attr = agg->attr;
	int ngrpby = 0;

	if (! agg->groupby_attrs) {
		return 1;
	}

	ngrpby = list_length(agg->groupby_attrs);
	f1 = record_get_flag(rec1);
	p1 = record_get_data(rec1, agg->ncol);
	p2 = 0;
	for (int i = 0, n = 0, k = 0 ; i < agg->ntlist && n < ngrpby && k < iter->nvec ; i++) {

		p2 = iter->value[k];

		if (! agg->tlist[i].gbykey) {
			int top = list_length(agg->tlist[i].attrs);
			for (int j = 0 ; j < top ; j++) {
				p1 = column_next(attr, p1);
				attr++;
				k++;
			}
			continue;
		}

		itemsz = attr->itemsz;

		if (f1[k] != *iter->flag[k]) {
			return 0;
		}

		if (f1[k] & XRG_FLAG_NULL) {
			continue;
		}

		if (itemsz > 0) {
			if (memcmp(p1, p2, itemsz) != 0) {
				return 0;
			}
		} else {
			int itemsz1 = xrg_bytea_len(p1);
			int itemsz2 = xrg_bytea_len(p2);
			const char* ptr1 = xrg_bytea_ptr(p1);
			const char* ptr2 = xrg_bytea_ptr(p2);

			if (itemsz1 != itemsz2 || memcmp(ptr1, ptr2, itemsz1) != 0) {
				return 0;
			}
		}

		p1 = column_next(attr, p1);
		attr++;
		n++;
		k++;
	}

	return 1;
}

static int transdata_size(void *context) {
	ListCell *lc;
	xrg_agg_t *agg = (xrg_agg_t *) context;
	int sz = sizeof(void*) * list_length(agg->aggfnoids);

	xrg_attr_t *attr = agg->attr;
	foreach (lc, agg->aggfnoids) {
		Oid fn = lfirst_oid(lc);
		if (fn > 0) {
			if (aggfnoid_is_avg(fn)) {
				sz += sizeof(avg_trans_t);
				attr += 2;
			} else {
				if (attr->itemsz < 0) {
					elog(ERROR, "transdata_create: aggregate function does not support string");
					return 0;
				}
				sz += attr->itemsz;
				attr++;
			}
		} else {
			attr++;
		}
	}

	return sz;
}


static void *hagg_init(void *context) {
	ListCell *lc;
	char *transdata = 0, *p = 0; 
	void **translist = 0;
	xrg_agg_t *agg = (xrg_agg_t *) context;
	int transsz = transdata_size(context);

	if (transsz == 0) {
		elog(ERROR, "transdata size is 0");
		return 0;
	}

	p = transdata = calloc(transsz, 1);
	if (transdata == 0) {
		elog(ERROR, "transdata_create: out of memory");
		return 0;
	}

	translist = (void **) transdata;

	p += sizeof(void*) * list_length(agg->aggfnoids);

	int i = 0;
	xrg_attr_t *attr = agg->attr;
	foreach (lc, agg->aggfnoids) {
		Oid fn = lfirst_oid(lc);
		if (fn > 0) {
			if (aggfnoid_is_avg(fn)) {
				attr += 2;
				translist[i] = p;
				p += sizeof(avg_trans_t);
			} else {
				translist[i] = p;
				p += attr->itemsz;
				attr++;
			}
		} else {
			translist[i] = 0;
			attr++;
		}
		i++;
	}

	return transdata;
}

static void *hagg_trans(void *context, void *rec, void *data) {
	xrg_agg_t *agg = (xrg_agg_t *) context;
	void **translist = (void  **)data;
	xrg_iter_t *iter = (xrg_iter_t *) rec;
	const char *p = 0;
	xrg_attr_t *attr = agg->attr;

	for (int i = 0, k = 0; i < agg->ntlist && k < iter->nvec ; i++) {
		kite_target_t *tgt = &agg->tlist[i];
		int nkiteattr = list_length(tgt->attrs);
		Oid aggfn = tgt->aggfn;
		void *transdata = translist[i];

		p = iter->value[k];

		if (! transdata) {
			attr++;
			k++;
			continue;
		}

		if (nkiteattr == 1) {
			aggregate(aggfn, transdata, p, attr);
			attr++;
			k++;
			continue;
		} else if (nkiteattr == 2) {
			avg_trans_t pt;
			const char *p1 = p;
			xrg_attr_t *attr1 = attr++;
			const char *p2 = iter->value[++k]; // k+1
			k++;
			xrg_attr_t *attr2 = attr++;

			if (avg_trans_init(aggfn, &pt, p1, attr1, p2, attr2) != 0) {
				elog(ERROR, "avg_trans_init failed");
				return 0;
			}

			aggregate(aggfn, transdata, &pt, attr1);
			continue;
		} else {
			elog(ERROR, "hagg_trans: aggregate functions won't have more than 2 columns");
			return 0;
		}
	}

	return data;
}

static void finalize(void *context, const void *rec, void *data, AttInMetadata *attinmeta,
       	Datum *datums, bool *flags, int ndatum) {
	xrg_agg_t *agg = (xrg_agg_t *) context;
	void **translist = (void  **)data;
	const char *flag = record_get_flag(rec);
	const char *p = record_get_data(rec, agg->ncol);
	xrg_attr_t *attr = agg->attr;
	int n = 0;

	for (int i = 0 ; i < agg->ntlist ; i++) {
		kite_target_t *tgt = &agg->tlist[i];
		int k = tgt->pgattr;
		void *transdata = translist[i];
		Oid aggfn = tgt->aggfn;
		int atttypmod = (attinmeta) ? attinmeta->atttypmods[k-1] : 0;
		//Form_pg_attribute pg_attr = (attinmeta) ? &attinmeta->tupdesc->attrs[k-1] : 0;
		Oid atttypid = (attinmeta) ? attinmeta->tupdesc->attrs[k-1].atttypid : 0;

		// datums[k] =  value[i]
		if (transdata) {
			int top = list_length(tgt->attrs);
			// finalize_aggregate();
			if (aggfnoid_is_avg(aggfn)) {
				//finalize_avg();
				avg_decode(aggfn, transdata, flag[n], attr, atttypid, atttypmod, &datums[k-1], &flags[k-1]);
			} else {
				var_decode(transdata, flag[n], attr, atttypid, atttypmod, &datums[k-1], &flags[k-1], true);
			}

			for (int j = 0 ; j < top ; j++) {
				p = column_next(attr, p);
				attr++;
				n++;
			}
		} else {
			// MUST advance the next pointer first because bytea size header will be altered to match postgres
			const char *p1 = p;
			xrg_attr_t *attr1 = attr;
			p = column_next(attr++, p);
			var_decode((char *) p1, flag[n++], attr1, atttypid, atttypmod, &datums[k-1], &flags[k-1], true);
		}
	}

	if (translist) {
		free(translist);
	}
}

static int hagg_reclen(void *context, const void *src) {
	xrg_iter_t *iter = (xrg_iter_t *) src;
	int sz = 0;

	// null flag
	sz += xrg_align(16, iter->nvec);

	for (int i = 0 ; i < iter->nvec ; i++) {
		if (iter->attr[i].itemsz >= 0) {
			sz += iter->attr[i].itemsz;
		} else {
			sz += xrg_bytea_len(iter->value[i]) + 4;
		}
	}

	return sz;
}

static void hagg_serialize(void *context, const void *src, void *dest, int destsz) {
	xrg_iter_t *iter = (xrg_iter_t *) src;
	char *flag = record_get_flag(dest);
	char *p = record_get_data(dest, iter->nvec);

	for (int i = 0 ; i < iter->nvec ; i++) {
		flag[i] = *iter->flag[i];
	}

	for (int i = 0 ; i < iter->nvec ; i++) {
		if (iter->attr[i].itemsz >= 0) {
			memcpy(p, iter->value[i], iter->attr[i].itemsz);
			p += iter->attr[i].itemsz;
		} else {
			int len = xrg_bytea_len(iter->value[i]) + 4;
			memcpy(p, iter->value[i], len);
			p += len;
		}
	}
}

static void build_tlist(xrg_agg_t *agg) {

	int i = 0, j=0;
	ListCell *lc;
	kite_target_t *tlist = 0;
	int attrlen = list_length(agg->retrieved_attrs);
	int aggfnlen = list_length(agg->aggfnoids);

	if (attrlen != aggfnlen) {
		elog(ERROR, "build_tlist: attrlen != aggfnlen");
		return;
	}
	agg->ntlist = aggfnlen;

	tlist = (kite_target_t*) malloc(sizeof(kite_target_t) * agg->ntlist);
	if (!tlist) {
		elog(ERROR, "out of memory");
		return;
	}

	memset(tlist, 0, sizeof(kite_target_t) * agg->ntlist);

	i = 0;
	foreach (lc, agg->retrieved_attrs) {
		tlist[i].pgattr = lfirst_int(lc);
		i++;
	}

	i = j = 0;
	foreach (lc, agg->aggfnoids) {
		tlist[i].aggfn = lfirst_oid(lc);
		tlist[i].attrs = lappend_int(tlist[i].attrs, j++);
		if (aggfnoid_is_avg(tlist[i].aggfn)) {
			tlist[i].attrs = lappend_int(tlist[i].attrs, j++);
		}
		i++;
	}

	if (agg->groupby_attrs) {
		foreach (lc, agg->groupby_attrs) {
			int gbyidx = lfirst_int(lc);
			for (int i = 0 ; i < agg->ntlist ; i++) {
				int idx = linitial_int(tlist[i].attrs);
				if (gbyidx == idx) {
					tlist[i].gbykey = true;
				}
			}
		}
	}

	agg->tlist = tlist;
}


xrg_agg_t *xrg_agg_init(List *retrieved_attrs, List *aggfnoids, List *groupby_attrs) {

	xrg_agg_t *agg = (xrg_agg_t*) malloc(sizeof(xrg_agg_t));
	Assert(agg);

	hagg_dispatch_t dispatch;
	memset(&dispatch, 0, sizeof(dispatch));

	agg->reached_eof = false;
	agg->attr = 0;
	agg->retrieved_attrs = retrieved_attrs;
	agg->aggfnoids = aggfnoids;
	agg->groupby_attrs = groupby_attrs;
	agg->batchid = 0;
	agg->aggdata_memusage = 0;
	build_tlist(agg);

	memset(&agg->agg_iter, 0, sizeof(agg->agg_iter));

	Assert(aggfnoids);
	agg->ncol = get_ncol_from_aggfnoids(aggfnoids);

	dispatch.keyeq = hagg_keyeq;
	dispatch.init = hagg_init;
	dispatch.trans = hagg_trans;
	dispatch.reclen = hagg_reclen;
	dispatch.serialize = hagg_serialize;
	dispatch.reset = 0;
	dispatch.checkstop = 0;

	agg->hagg = hagg_start(agg, LLONG_MAX, &agg->aggdata_memusage, ".", &dispatch);

	return agg;
}

void xrg_agg_destroy(xrg_agg_t *agg) {
	if (agg) {
		if (agg->hagg) {
			hagg_release(agg->hagg);
		}
		if (agg->attr) {
			free(agg->attr);
		}
		if (agg->tlist) {
			free(agg->tlist);
		}

		free(agg);
	}
}



static int xrg_agg_process(xrg_agg_t *agg, xrg_iter_t *iter) {
	ListCell *lc;

	if (iter->nvec != agg->ncol) {
		elog(ERROR, "xrg_agg_process: number of columns returned from kite not match (%d != %d)", 
				agg->ncol, iter->nvec);
		return 1;
	}

	uint64_t hval = 0; // hash value of the groupby keys

	if (! agg->attr) {
		agg->attr = (xrg_attr_t *) malloc(sizeof(xrg_attr_t) * iter->nvec);
		//agg->ncol = iter->nvec;
		memcpy(agg->attr, iter->attr, sizeof(xrg_attr_t) * iter->nvec);
	}

	foreach (lc, agg->groupby_attrs) {
		int idx = lfirst_int(lc);
		int itemsz = iter->attr[idx].itemsz;
		const char *p = iter->value[idx];
		if (itemsz < 0) {
			itemsz = xrg_bytea_len(iter->value[idx]);
			p = xrg_bytea_ptr(iter->value[idx]);
		}

		hval ^= komihash(p, itemsz, 0);  // XOR

	}

	return hagg_feed(agg->hagg, hval, iter, -1);
}

int xrg_agg_fetch(xrg_agg_t *agg, kite_handle_t *hdl) {
	int ret = 0;
	char errmsg[1024];
	xrg_iter_t *iter;

	// get all data from socket
	if (! agg->reached_eof) {

		while (true) {
			ret = kite_next_row(hdl, &iter, errmsg, sizeof(errmsg));
			if (ret == 0) {
				if (iter == 0) {
					agg->reached_eof = true;
					ret = 0;

					// init iter right after EOF reached
					if (hagg_iter_init(agg->hagg, &agg->agg_iter) != 0) {
						elog(ERROR, "hagg_iter_init failed");
						return 1;
					}
					break;
				}

				if ((ret = xrg_agg_process(agg, iter)) != 0) {
					break;
				}
			} else {
				// error handling
				break;
			}
		}
	}

	return ret;
}


int xrg_agg_get_next(xrg_agg_t *agg, AttInMetadata *attinmeta, Datum *datums, bool *flags, int n) {

	void *rec = 0;
	void *data = 0;

	for (;;) {
		char errmsg[1025];
		hagg_iter_next(&agg->agg_iter, &rec, &data, errmsg, sizeof(errmsg));
		if (rec == 0) {
			break;
		}

		finalize(agg, rec, data, attinmeta, datums, flags, n);
		return 0;
	}

	return 1;

}


