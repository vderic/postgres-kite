/*-------------------------------------------------------------------------
 *
 * postgres_fdw.c
 *		  Foreign-data wrapper for remote PostgreSQL servers
 *
 * Portions Copyright (c) 2012-2022, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  contrib/postgres_fdw/postgres_fdw.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <limits.h>

#include "access/htup_details.h"
#include "access/sysattr.h"
#include "access/table.h"
#include "catalog/pg_class.h"
#include "catalog/pg_opfamily.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/vacuum.h"
#include "executor/execAsync.h"
#include "foreign/fdwapi.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/appendinfo.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "optimizer/prep.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/tlist.h"
#include "parser/parsetree.h"
#include "kite_fdw.h"
#include "storage/latch.h"
#include "utils/builtins.h"
#include "utils/float.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/sampling.h"
#include "utils/selfuncs.h"

#include "kitesdk.h"
#include "nodes/print.h"
#include "xrg.h"
#include "schema.h"
#include "decode.h"
#include "agg.h"

PG_MODULE_MAGIC;

/* Default CPU cost to start up a foreign query. */
#define DEFAULT_FDW_STARTUP_COST 100.0

/* Default CPU cost to process 1 row (above and beyond cpu_tuple_cost). */
#define DEFAULT_FDW_TUPLE_COST 0.01

/* If no remote estimates, assume a sort costs 20% extra */
#define DEFAULT_FDW_SORT_MULTIPLIER 1.2

/*
 * Indexes of FDW-private information stored in fdw_private lists.
 *
 * These items are indexed with the enum FdwScanPrivateIndex, so an item
 * can be fetched with list_nth().  For example, to get the SELECT statement:
 *		sql = strVal(list_nth(fdw_private, FdwScanPrivateSelectSql));
 */
enum FdwScanPrivateIndex {
	/* Relation Schema (as a String node) */
	FdwScanPrivateSchema,
	/* SQL statement to execute remotely (as a String node) */
	FdwScanPrivateSelectSql,
	/* Integer list of attribute numbers retrieved by the SELECT */
	FdwScanPrivateRetrievedAttrs,
	/* Integer representing the desired fetch_size */
	FdwScanPrivateFetchSize,
	/* Integer represeting the number of fragment in kite */
	FdwScanPrivateFragCnt,

	/*
	 * String describing join i.e. names of relations being joined and types
	 * of join, added when the scan is join
	 */
	FdwScanPrivateRelations,

	/* Integer list of aggfnoid retrieved by the SELECT */
	FdwScanPrivateRetrievedAggfnoids,

	/* Integer list of groupby index in Kite by the SELECT */
	FdwScanPrivateRetrievedGroupByAttrs

};

/*
 * Execution state of a foreign scan using postgres_fdw.
 */
typedef struct PgFdwScanState {
	Relation rel;			  /* relcache entry for the foreign table. NULL
								 * for a foreign join scan. */
	TupleDesc tupdesc;		  /* tuple descriptor of scan */
	AttInMetadata *attinmeta; /* attribute datatype conversion metadata */

	/* extracted fdw_private data */
	char *schema;				   /* text of schema */
	char *query;				   /* text of SELECT command */
	List *retrieved_attrs;		   /* list of retrieved attribute numbers */
	List *retrieved_groupby_attrs; /* list of retrieved group by index in kite */
	List *retrieved_aggfnoids;	 /* list of retrieved aggfnoids from target list */

	/* for remote query execution */
	xrg_agg_t *agg;		 /* xrg_agg_t for aggregate */
	kite_request_t *req; /* kite connectino for the scan */

	PgFdwConnState *conn_state; /* extra per-connection state */
	unsigned int cursor_number; /* quasi-unique ID for my cursor */
	bool cursor_exists;			/* have we created the cursor? */
	int numParams;				/* number of parameters passed to query */
	FmgrInfo *param_flinfo;		/* output conversion functions for them */
	List *param_exprs;			/* executable expressions for param values */
	const char **param_values;  /* textual values of query parameters */

	/* for storing result tuples */
	HeapTuple *tuples; /* array of currently-retrieved tuples */
	int num_tuples;	/* # of tuples in array */
	int next_tuple;	/* index of next one to return */

	/* batch-level state, for optimizing rewinds and avoiding useless fetch */
	int fetch_ct_2;   /* Min(# of fetches done, 2) */
	bool eof_reached; /* true if last fetch reached EOF */

	/* for asynchronous execution */
	bool async_capable; /* engage asynchronous-capable logic? */

	/* working memory contexts */
	MemoryContext batch_cxt; /* context holding current batch of tuples */
	MemoryContext temp_cxt;  /* context for per-tuple temporary data */

	int fetch_size; /* number of tuples per fetch */
	int fragcnt;	/* number of fragment in kite */
} PgFdwScanState;

/*
 * Workspace for analyzing a foreign table.
 */
typedef struct PgFdwAnalyzeState {
	Relation rel;			  /* relcache entry for the foreign table */
	AttInMetadata *attinmeta; /* attribute datatype conversion metadata */
	List *retrieved_attrs;	/* attr numbers retrieved by query */

	/* collected sample rows */
	HeapTuple *rows; /* array of size targrows */
	int targrows;	/* target # of sample rows */
	int numrows;	 /* # of sample rows collected */

	/* for random sampling */
	double samplerows;		   /* # of rows fetched */
	double rowstoskip;		   /* # of rows to skip before next sample */
	ReservoirStateData rstate; /* state for reservoir sampling */

	/* working memory contexts */
	MemoryContext anl_cxt;  /* context for per-analyze lifespan data */
	MemoryContext temp_cxt; /* context for per-tuple temporary data */
} PgFdwAnalyzeState;

/*
 * This enum describes what's kept in the fdw_private list for a ForeignPath.
 * We store:
 *
 * 1) Boolean flag showing if the remote query has the final sort
 * 2) Boolean flag showing if the remote query has the LIMIT clause
 */
enum FdwPathPrivateIndex {
	/* has-final-sort flag (as a Boolean node) */
	FdwPathPrivateHasFinalSort,
	/* has-limit flag (as a Boolean node) */
	FdwPathPrivateHasLimit
};

/* Struct for extra information passed to estimate_path_cost_size() */
typedef struct
{
	PathTarget *target;
	bool has_final_sort;
	bool has_limit;
	double limit_tuples;
	int64 count_est;
	int64 offset_est;
} PgFdwPathExtraData;

/*
 * Identify the attribute where data conversion fails.
 */
typedef struct ConversionLocation {
	AttrNumber cur_attno;	  /* attribute number being processed, or 0 */
	Relation rel;			   /* foreign table being processed, or NULL */
	ForeignScanState *fsstate; /* plan node being processed, or NULL */
} ConversionLocation;

/*
 * SQL functions
 */
PG_FUNCTION_INFO_V1(kite_fdw_handler);

/*
 * FDW callback routines
 */
static void postgresGetForeignRelSize(PlannerInfo *root,
	RelOptInfo *baserel,
	Oid foreigntableid);
static void postgresGetForeignPaths(PlannerInfo *root,
	RelOptInfo *baserel,
	Oid foreigntableid);
static ForeignScan *postgresGetForeignPlan(PlannerInfo *root,
	RelOptInfo *foreignrel,
	Oid foreigntableid,
	ForeignPath *best_path,
	List *tlist,
	List *scan_clauses,
	Plan *outer_plan);
static void postgresBeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot *postgresIterateForeignScan(ForeignScanState *node);
static void postgresReScanForeignScan(ForeignScanState *node);
static void postgresEndForeignScan(ForeignScanState *node);

static void postgresGetForeignUpperPaths(PlannerInfo *root,
	UpperRelationKind stage,
	RelOptInfo *input_rel,
	RelOptInfo *output_rel,
	void *extra);

static bool postgresAnalyzeForeignTable(Relation relation,
	AcquireSampleRowsFunc *func,
	BlockNumber *totalpages);

/*
 * Helper functions
 */
static void estimate_path_cost_size(PlannerInfo *root,
	RelOptInfo *foreignrel,
	List *param_join_conds,
	List *pathkeys,
	PgFdwPathExtraData *fpextra,
	double *p_rows, int *p_width,
	Cost *p_startup_cost, Cost *p_total_cost);
static void adjust_foreign_grouping_path_cost(PlannerInfo *root,
	List *pathkeys,
	double retrieved_rows,
	double width,
	double limit_tuples,
	Cost *p_startup_cost,
	Cost *p_run_cost);
static void create_cursor(ForeignScanState *node);
static void fetch_more_data(ForeignScanState *node);
static void prepare_query_params(PlanState *node,
	List *fdw_exprs,
	int numParams,
	FmgrInfo **param_flinfo,
	List **param_exprs,
	const char ***param_values);
static void process_query_params(ExprContext *econtext,
	FmgrInfo *param_flinfo,
	List *param_exprs,
	const char **param_values);
/* KITE */
static HeapTuple make_tuple_from_agg(xrg_agg_t *agg,
	int row,
	Relation rel,
	AttInMetadata *attinmeta,
	List *retrieved_attrs,
	ForeignScanState *fsstate,
	MemoryContext temp_context);

static HeapTuple make_tuple_from_result_row(xrg_iter_t *iter,
	int row,
	Relation rel,
	AttInMetadata *attinmeta,
	List *retrieved_attrs,
	ForeignScanState *fsstate,
	MemoryContext temp_context);

/* END KITE */

static void conversion_error_callback(void *arg);
static bool foreign_grouping_ok(PlannerInfo *root, RelOptInfo *grouped_rel,
	Node *havingQual);
static void add_foreign_grouping_paths(PlannerInfo *root,
	RelOptInfo *input_rel,
	RelOptInfo *grouped_rel,
	GroupPathExtraData *extra);
static void add_foreign_final_paths(PlannerInfo *root,
	RelOptInfo *input_rel,
	RelOptInfo *final_rel,
	FinalPathExtraData *extra);
static void apply_server_options(PgFdwRelationInfo *fpinfo);
static void apply_table_options(PgFdwRelationInfo *fpinfo);
static void merge_fdw_options(PgFdwRelationInfo *fpinfo,
	const PgFdwRelationInfo *fpinfo_o,
	const PgFdwRelationInfo *fpinfo_i);

static int postgresAcquireSampleRowsFunc(Relation relation, int elevel,
	HeapTuple *rows, int targrows,
	double *totalrows,
	double *totaldeadrows);

/*
 * Foreign-data wrapper handler function: return a struct with pointers
 * to my callback routines.
 */
Datum kite_fdw_handler(PG_FUNCTION_ARGS) {
	FdwRoutine *routine = makeNode(FdwRoutine);

	/* Functions for scanning foreign tables */
	routine->GetForeignRelSize = postgresGetForeignRelSize;
	routine->GetForeignPaths = postgresGetForeignPaths;
	routine->GetForeignPlan = postgresGetForeignPlan;
	routine->BeginForeignScan = postgresBeginForeignScan;
	routine->IterateForeignScan = postgresIterateForeignScan;
	routine->ReScanForeignScan = postgresReScanForeignScan;
	routine->EndForeignScan = postgresEndForeignScan;

	/* Support functions for upper relation push-down */
	routine->GetForeignUpperPaths = postgresGetForeignUpperPaths;

	/* Support functions for analyze foreign tables */
	routine->AnalyzeForeignTable = postgresAnalyzeForeignTable;

	PG_RETURN_POINTER(routine);
}

/*
 * postgresGetForeignRelSize
 *		Estimate # of rows and width of the result of the scan
 *
 * We should consider the effect of all baserestrictinfo clauses here, but
 * not any join clauses.
 */
static void
postgresGetForeignRelSize(PlannerInfo *root,
	RelOptInfo *baserel,
	Oid foreigntableid) {
	PgFdwRelationInfo *fpinfo;
	ListCell *lc;
	RangeTblEntry *rte = planner_rt_fetch(baserel->relid, root);

	/*
	 * We use PgFdwRelationInfo to pass various information to subsequent
	 * functions.
	 */
	fpinfo = (PgFdwRelationInfo *)palloc0(sizeof(PgFdwRelationInfo));
	baserel->fdw_private = (void *)fpinfo;

	/* Base foreign tables need to be pushed down always. */
	fpinfo->pushdown_safe = true;

	/* Look up foreign-table catalog info. */
	fpinfo->table = GetForeignTable(foreigntableid);
	fpinfo->server = GetForeignServer(fpinfo->table->serverid);

	/*
	 * Extract user-settable option values.  Note that per-table settings of
	 * use_remote_estimate, fetch_size and async_capable override per-server
	 * settings of them, respectively.
	 */
	fpinfo->use_remote_estimate = false;
	fpinfo->fdw_startup_cost = DEFAULT_FDW_STARTUP_COST;
	fpinfo->fdw_tuple_cost = DEFAULT_FDW_TUPLE_COST;
	fpinfo->shippable_extensions = NIL;
	fpinfo->fetch_size = 100;
	fpinfo->async_capable = false;
	fpinfo->fragcnt = 1;

	apply_server_options(fpinfo);
	apply_table_options(fpinfo);

	/*
	 * If the table or the server is configured to use remote estimates,
	 * identify which user to do remote access as during planning.  This
	 * should match what ExecCheckRTEPerms() does.  If we fail due to lack of
	 * permissions, the query would have failed at runtime anyway.
	 */
	if (fpinfo->use_remote_estimate) {
		Oid userid = rte->checkAsUser ? rte->checkAsUser : GetUserId();

		fpinfo->user = GetUserMapping(userid, fpinfo->server->serverid);
	} else
		fpinfo->user = NULL;

	/*
	 * Identify which baserestrictinfo clauses can be sent to the remote
	 * server and which can't.
	 */
	classifyConditions(root, baserel, baserel->baserestrictinfo,
		&fpinfo->remote_conds, &fpinfo->local_conds);

	/*
	 * Identify which attributes will need to be retrieved from the remote
	 * server.  These include all attrs needed for joins or final output, plus
	 * all attrs used in the local_conds.  (Note: if we end up using a
	 * parameterized scan, it's possible that some of the join clauses will be
	 * sent to the remote and thus we wouldn't really need to retrieve the
	 * columns used in them.  Doesn't seem worth detecting that case though.)
	 */

	fpinfo->attrs_used = NULL;
	pull_varattnos((Node *)baserel->reltarget->exprs, baserel->relid,
		&fpinfo->attrs_used);
	foreach (lc, fpinfo->local_conds) {
		RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

		pull_varattnos((Node *)rinfo->clause, baserel->relid,
			&fpinfo->attrs_used);
	}

	/*
	 * Compute the selectivity and cost of the local_conds, so we don't have
	 * to do it over again for each path.  The best we can do for these
	 * conditions is to estimate selectivity on the basis of local statistics.
	 */
	fpinfo->local_conds_sel = clauselist_selectivity(root,
		fpinfo->local_conds,
		baserel->relid,
		JOIN_INNER,
		NULL);

	cost_qual_eval(&fpinfo->local_conds_cost, fpinfo->local_conds, root);

	/*
	 * Set # of retrieved rows and cached relation costs to some negative
	 * value, so that we can detect when they are set to some sensible values,
	 * during one (usually the first) of the calls to estimate_path_cost_size.
	 */
	fpinfo->retrieved_rows = -1;
	fpinfo->rel_startup_cost = -1;
	fpinfo->rel_total_cost = -1;

	/*
	 * If the table or the server is configured to use remote estimates,
	 * connect to the foreign server and execute EXPLAIN to estimate the
	 * number of rows selected by the restriction clauses, as well as the
	 * average row width.  Otherwise, estimate using whatever statistics we
	 * have locally, in a way similar to ordinary tables.
	 */
	if (fpinfo->use_remote_estimate) {
		/*
		 * Get cost/size estimates with help of remote server.  Save the
		 * values in fpinfo so we don't need to do it again to generate the
		 * basic foreign path.
		 */
		estimate_path_cost_size(root, baserel, NIL, NIL, NULL,
			&fpinfo->rows, &fpinfo->width,
			&fpinfo->startup_cost, &fpinfo->total_cost);

		/* Report estimated baserel size to planner. */
		baserel->rows = fpinfo->rows;
		baserel->reltarget->width = fpinfo->width;
	} else {
		/*
		 * If the foreign table has never been ANALYZEd, it will have
		 * reltuples < 0, meaning "unknown".  We can't do much if we're not
		 * allowed to consult the remote server, but we can use a hack similar
		 * to plancat.c's treatment of empty relations: use a minimum size
		 * estimate of 10 pages, and divide by the column-datatype-based width
		 * estimate to get the corresponding number of tuples.
		 */
		if (baserel->tuples < 0) {
			baserel->pages = 10;
			baserel->tuples =
				(10 * BLCKSZ) / (baserel->reltarget->width +
									MAXALIGN(SizeofHeapTupleHeader));
		}

		/* Estimate baserel size as best we can with local statistics. */
		set_baserel_size_estimates(root, baserel);

		/* Fill in basically-bogus cost estimates for use later. */
		estimate_path_cost_size(root, baserel, NIL, NIL, NULL,
			&fpinfo->rows, &fpinfo->width,
			&fpinfo->startup_cost, &fpinfo->total_cost);
	}

	/*
	 * fpinfo->relation_name gets the numeric rangetable index of the foreign
	 * table RTE.  (If this query gets EXPLAIN'd, we'll convert that to a
	 * human-readable string at that time.)
	 */
	fpinfo->relation_name = psprintf("%u", baserel->relid);

	/* No outer and inner relations. */
	fpinfo->make_outerrel_subquery = false;
	fpinfo->make_innerrel_subquery = false;
	fpinfo->lower_subquery_rels = NULL;
	/* Set the relation index. */
	fpinfo->relation_index = baserel->relid;
}

/*
 * postgresGetForeignPaths
 *		Create possible scan paths for a scan on the foreign table
 */
static void
postgresGetForeignPaths(PlannerInfo *root,
	RelOptInfo *baserel,
	Oid foreigntableid) {
	PgFdwRelationInfo *fpinfo = (PgFdwRelationInfo *)baserel->fdw_private;
	ForeignPath *path;

	/*
	 * Create simplest ForeignScan path node and add it to baserel.  This path
	 * corresponds to SeqScan path of regular tables (though depending on what
	 * baserestrict conditions we were able to send to remote, there might
	 * actually be an indexscan happening there).  We already did all the work
	 * to estimate cost and size of this path.
	 *
	 * Although this path uses no join clauses, it could still have required
	 * parameterization due to LATERAL refs in its tlist.
	 */
	path = create_foreignscan_path(root, baserel,
		NULL, /* default pathtarget */
		fpinfo->rows,
		fpinfo->startup_cost,
		fpinfo->total_cost,
		NIL, /* no pathkeys */
		baserel->lateral_relids,
		NULL, /* no extra plan */
		NIL); /* no fdw_private list */
	add_path(baserel, (Path *)path);
}

/*
 * postgresGetForeignPlan
 *		Create ForeignScan plan node which implements selected best path
 */
static ForeignScan *
postgresGetForeignPlan(PlannerInfo *root,
	RelOptInfo *foreignrel,
	Oid foreigntableid,
	ForeignPath *best_path,
	List *tlist,
	List *scan_clauses,
	Plan *outer_plan) {
	PgFdwRelationInfo *fpinfo = (PgFdwRelationInfo *)foreignrel->fdw_private;
	Index scan_relid;
	List *fdw_private;
	List *remote_exprs = NIL;
	List *local_exprs = NIL;
	List *params_list = NIL;
	List *fdw_scan_tlist = NIL;
	List *fdw_recheck_quals = NIL;
	List *retrieved_attrs = NIL;
	List *retrieved_aggfnoids = NIL;
	List *retrieved_groupby_attrs = NIL;
	StringInfoData sql;
	StringInfoData schema;
	bool has_final_sort = false;
	bool has_limit = false;
	ListCell *lc;

	/*
	 * Get FDW private data created by postgresGetForeignUpperPaths(), if any.
	 */
	if (best_path->fdw_private) {
		has_final_sort = boolVal(list_nth(best_path->fdw_private,
			FdwPathPrivateHasFinalSort));
		has_limit = boolVal(list_nth(best_path->fdw_private,
			FdwPathPrivateHasLimit));
	}

	if (IS_SIMPLE_REL(foreignrel)) {
		/*
		 * For base relations, set scan_relid as the relid of the relation.
		 */
		scan_relid = foreignrel->relid;

		/*
		 * In a base-relation scan, we must apply the given scan_clauses.
		 *
		 * Separate the scan_clauses into those that can be executed remotely
		 * and those that can't.  baserestrictinfo clauses that were
		 * previously determined to be safe or unsafe by classifyConditions
		 * are found in fpinfo->remote_conds and fpinfo->local_conds. Anything
		 * else in the scan_clauses list will be a join clause, which we have
		 * to check for remote-safety.
		 *
		 * Note: the join clauses we see here should be the exact same ones
		 * previously examined by postgresGetForeignPaths.  Possibly it'd be
		 * worth passing forward the classification work done then, rather
		 * than repeating it here.
		 *
		 * This code must match "extract_actual_clauses(scan_clauses, false)"
		 * except for the additional decision about remote versus local
		 * execution.
		 */
		foreach (lc, scan_clauses) {
			RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

			/* Ignore any pseudoconstants, they're dealt with elsewhere */
			if (rinfo->pseudoconstant)
				continue;

			if (list_member_ptr(fpinfo->remote_conds, rinfo))
				remote_exprs = lappend(remote_exprs, rinfo->clause);
			else if (list_member_ptr(fpinfo->local_conds, rinfo))
				local_exprs = lappend(local_exprs, rinfo->clause);
			else if (is_foreign_expr(root, foreignrel, rinfo->clause))
				remote_exprs = lappend(remote_exprs, rinfo->clause);
			else
				local_exprs = lappend(local_exprs, rinfo->clause);
		}

		/*
		 * For a base-relation scan, we have to support EPQ recheck, which
		 * should recheck all the remote quals.
		 */
		fdw_recheck_quals = remote_exprs;
	} else {
		/*
		 * Join relation or upper relation - set scan_relid to 0.
		 */
		scan_relid = 0;

		/*
		 * For a join rel, baserestrictinfo is NIL and we are not considering
		 * parameterization right now, so there should be no scan_clauses for
		 * a joinrel or an upper rel either.
		 */
		Assert(!scan_clauses);

		/*
		 * Instead we get the conditions to apply from the fdw_private
		 * structure.
		 */
		remote_exprs = extract_actual_clauses(fpinfo->remote_conds, false);
		local_exprs = extract_actual_clauses(fpinfo->local_conds, false);

		/*
		 * We leave fdw_recheck_quals empty in this case, since we never need
		 * to apply EPQ recheck clauses.  In the case of a joinrel, EPQ
		 * recheck is handled elsewhere --- see postgresGetForeignJoinPaths().
		 * If we're planning an upperrel (ie, remote grouping or aggregation)
		 * then there's no EPQ to do because SELECT FOR UPDATE wouldn't be
		 * allowed, and indeed we *can't* put the remote clauses into
		 * fdw_recheck_quals because the unaggregated Vars won't be available
		 * locally.
		 */

		/* Build the list of columns to be fetched from the foreign server. */
		fdw_scan_tlist = build_tlist_to_deparse(foreignrel);

		/*
		 * Ensure that the outer plan produces a tuple whose descriptor
		 * matches our scan tuple slot.  Also, remove the local conditions
		 * from outer plan's quals, lest they be evaluated twice, once by the
		 * local plan and once by the scan.
		 */
		if (outer_plan) {
			ListCell *lc;

			/*
			 * Right now, we only consider grouping and aggregation beyond
			 * joins. Queries involving aggregates or grouping do not require
			 * EPQ mechanism, hence should not have an outer plan here.
			 */
			Assert(!IS_UPPER_REL(foreignrel));

			/*
			 * First, update the plan's qual list if possible.  In some cases
			 * the quals might be enforced below the topmost plan level, in
			 * which case we'll fail to remove them; it's not worth working
			 * harder than this.
			 */
			foreach (lc, local_exprs) {
				Node *qual = lfirst(lc);

				outer_plan->qual = list_delete(outer_plan->qual, qual);

				/*
				 * For an inner join the local conditions of foreign scan plan
				 * can be part of the joinquals as well.  (They might also be
				 * in the mergequals or hashquals, but we can't touch those
				 * without breaking the plan.)
				 */
				if (IsA(outer_plan, NestLoop) ||
					IsA(outer_plan, MergeJoin) ||
					IsA(outer_plan, HashJoin)) {
					Join *join_plan = (Join *)outer_plan;

					if (join_plan->jointype == JOIN_INNER)
						join_plan->joinqual = list_delete(join_plan->joinqual,
							qual);
				}
			}

			/*
			 * Now fix the subplan's tlist --- this might result in inserting
			 * a Result node atop the plan tree.
			 */
			outer_plan = change_plan_targetlist(outer_plan, fdw_scan_tlist,
				best_path->path.parallel_safe);
		}
	}

	/*
	 * Build the query string to be sent for execution, and identify
	 * expressions to be sent as parameters.
	 */
	initStringInfo(&sql);
	deparseSelectStmtForRel(&sql, root, foreignrel, fdw_scan_tlist,
		remote_exprs, best_path->path.pathkeys,
		has_final_sort, has_limit, false,
		&retrieved_attrs, &params_list, &retrieved_aggfnoids,
		&retrieved_groupby_attrs);

	//elog(LOG, "FINAL SQL: %s", sql.data);
	/* Remember remote_exprs for possible use by postgresPlanDirectModify */
	fpinfo->final_remote_exprs = remote_exprs;

	/*
	 * build the schema for KITE
	 */
	{
		RelOptInfo *relinfo = IS_UPPER_REL(foreignrel) ? fpinfo->outerrel : foreignrel;
		RangeTblEntry *scanrte = planner_rt_fetch(relinfo->relid, root);
		Relation scanrel = table_open(scanrte->relid, NoLock);
		TupleDesc tupdesc = RelationGetDescr(scanrel);
		initStringInfo(&schema);
		kite_build_schema(&schema, tupdesc);
		table_close(scanrel, NoLock);
	}

	/*
	 * Build the fdw_private list that will be available to the executor.
	 * Items in the list must match order in enum FdwScanPrivateIndex.
	 */
	fdw_private = list_make5(makeString(schema.data),
		makeString(sql.data),
		retrieved_attrs,
		makeInteger(fpinfo->fetch_size),
		makeInteger(fpinfo->fragcnt));

	if (IS_JOIN_REL(foreignrel) || IS_UPPER_REL(foreignrel)) {
		fdw_private = lappend(fdw_private,
			makeString(fpinfo->relation_name));

		fdw_private = lappend(fdw_private,
			retrieved_aggfnoids);

		if (retrieved_groupby_attrs) {
			fdw_private = lappend(fdw_private,
				retrieved_groupby_attrs);
		}
	}

	/*
	 * Create the ForeignScan node for the given relation.
	 *
	 * Note that the remote parameter expressions are stored in the fdw_exprs
	 * field of the finished plan node; we can't keep them in private state
	 * because then they wouldn't be subject to later planner processing.
	 */
	return make_foreignscan(tlist,
		local_exprs,
		scan_relid,
		params_list,
		fdw_private,
		fdw_scan_tlist,
		fdw_recheck_quals,
		outer_plan);
}

/*
 * Construct a tuple descriptor for the scan tuples handled by a foreign join.
 */
static TupleDesc
get_tupdesc_for_join_scan_tuples(ForeignScanState *node) {
	ForeignScan *fsplan = (ForeignScan *)node->ss.ps.plan;
	EState *estate = node->ss.ps.state;
	TupleDesc tupdesc;

	/*
	 * The core code has already set up a scan tuple slot based on
	 * fsplan->fdw_scan_tlist, and this slot's tupdesc is mostly good enough,
	 * but there's one case where it isn't.  If we have any whole-row row
	 * identifier Vars, they may have vartype RECORD, and we need to replace
	 * that with the associated table's actual composite type.  This ensures
	 * that when we read those ROW() expression values from the remote server,
	 * we can convert them to a composite type the local server knows.
	 */
	tupdesc = CreateTupleDescCopy(node->ss.ss_ScanTupleSlot->tts_tupleDescriptor);
	for (int i = 0; i < tupdesc->natts; i++) {
		Form_pg_attribute att = TupleDescAttr(tupdesc, i);
		Var *var;
		RangeTblEntry *rte;
		Oid reltype;

		/* Nothing to do if it's not a generic RECORD attribute */
		if (att->atttypid != RECORDOID || att->atttypmod >= 0)
			continue;

		/*
		 * If we can't identify the referenced table, do nothing.  This'll
		 * likely lead to failure later, but perhaps we can muddle through.
		 */
		var = (Var *)list_nth_node(TargetEntry, fsplan->fdw_scan_tlist,
			i)
				  ->expr;
		if (!IsA(var, Var) || var->varattno != 0)
			continue;
		rte = list_nth(estate->es_range_table, var->varno - 1);
		if (rte->rtekind != RTE_RELATION)
			continue;
		reltype = get_rel_type_id(rte->relid);
		if (!OidIsValid(reltype))
			continue;
		att->atttypid = reltype;
		/* shouldn't need to change anything else */
	}
	return tupdesc;
}

/*
 * postgresBeginForeignScan
 *		Initiate an executor scan of a foreign PostgreSQL table.
 */
static void
postgresBeginForeignScan(ForeignScanState *node, int eflags) {
	ForeignScan *fsplan = (ForeignScan *)node->ss.ps.plan;
	EState *estate = node->ss.ps.state;
	PgFdwScanState *fsstate;
	RangeTblEntry *rte;
	Oid userid;
	ForeignTable *table;
	UserMapping *user;
	int rtindex;
	int numParams;

	/*
	 * Do nothing in EXPLAIN (no ANALYZE) case.  node->fdw_state stays NULL.
	 */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	/*
	 * We'll save private state in node->fdw_state.
	 */
	fsstate = (PgFdwScanState *)palloc0(sizeof(PgFdwScanState));
	node->fdw_state = (void *)fsstate;

	/*
	 * Identify which user to do the remote access as.  This should match what
	 * ExecCheckRTEPerms() does.  In case of a join or aggregate, use the
	 * lowest-numbered member RTE as a representative; we would get the same
	 * result from any.
	 */
	if (fsplan->scan.scanrelid > 0)
		rtindex = fsplan->scan.scanrelid;
	else
		rtindex = bms_next_member(fsplan->fs_relids, -1);
	rte = exec_rt_fetch(rtindex, estate);
	userid = rte->checkAsUser ? rte->checkAsUser : GetUserId();

	/* Get info about foreign table. */
	table = GetForeignTable(rte->relid);
	user = GetUserMapping(userid, table->serverid);

	/* KITE */
	fsstate->req = GetConnection(user, false, &fsstate->conn_state);
	fsstate->cursor_exists = false;
	fsstate->cursor_number = 0;
	/* END KITE */

	/* Get private info created by planner functions. */
	fsstate->schema = strVal(list_nth(fsplan->fdw_private, FdwScanPrivateSchema));
	fsstate->query = strVal(list_nth(fsplan->fdw_private,
		FdwScanPrivateSelectSql));
	fsstate->retrieved_attrs = (List *)list_nth(fsplan->fdw_private,
		FdwScanPrivateRetrievedAttrs);
	fsstate->fetch_size = intVal(list_nth(fsplan->fdw_private,
		FdwScanPrivateFetchSize));
	fsstate->fragcnt = intVal(list_nth(fsplan->fdw_private,
		FdwScanPrivateFragCnt));

	/* Create contexts for batches of tuples and per-tuple temp workspace. */
	fsstate->batch_cxt = AllocSetContextCreate(estate->es_query_cxt,
		"postgres_fdw tuple data",
		ALLOCSET_DEFAULT_SIZES);
	fsstate->temp_cxt = AllocSetContextCreate(estate->es_query_cxt,
		"postgres_fdw temporary data",
		ALLOCSET_SMALL_SIZES);

	if (list_length(fsplan->fdw_private) >= FdwScanPrivateRetrievedAggfnoids + 1) {
		fsstate->retrieved_aggfnoids = (List *)list_nth(fsplan->fdw_private,
			FdwScanPrivateRetrievedAggfnoids);
	}

	if (list_length(fsplan->fdw_private) >= FdwScanPrivateRetrievedGroupByAttrs + 1) {
		fsstate->retrieved_groupby_attrs = (List *)list_nth(fsplan->fdw_private,
			FdwScanPrivateRetrievedGroupByAttrs);
	}

	/*
	 * Get info we'll need for converting data fetched from the foreign server
	 * into local representation and error reporting during that process.
	 */
	if (fsplan->scan.scanrelid > 0) {
		fsstate->rel = node->ss.ss_currentRelation;
		fsstate->tupdesc = RelationGetDescr(fsstate->rel);
	} else {
		fsstate->rel = NULL;
		fsstate->tupdesc = get_tupdesc_for_join_scan_tuples(node);
	}

	fsstate->attinmeta = TupleDescGetAttInMetadata(fsstate->tupdesc);

	/*
	 * Prepare for processing of parameters used in remote query, if any.
	 */
	numParams = list_length(fsplan->fdw_exprs);
	fsstate->numParams = numParams;

#if 1
	/* KITE */
	if (numParams > 0) {
		elog(ERROR, "Statement with muiltple parameters not supported");
	}
	/* END KITE */
#else

	if (numParams > 0)
		prepare_query_params((PlanState *)node,
			fsplan->fdw_exprs,
			numParams,
			&fsstate->param_flinfo,
			&fsstate->param_exprs,
			&fsstate->param_values);

#endif
	/* Set the async-capable flag */
	fsstate->async_capable = node->ss.ps.async_capable;

	/*
	 * Prepare xrg_agg
	 */
	if (fsstate->retrieved_aggfnoids) {
		fsstate->agg = xrg_agg_init(fsstate->retrieved_attrs,
			fsstate->retrieved_aggfnoids,
			fsstate->retrieved_groupby_attrs);
	} else {
		fsstate->agg = NULL;
	}
}

/*
 * postgresIterateForeignScan
 *		Retrieve next row from the result set, or clear tuple slot to indicate
 *		EOF.
 */
static TupleTableSlot *
postgresIterateForeignScan(ForeignScanState *node) {
	PgFdwScanState *fsstate = (PgFdwScanState *)node->fdw_state;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;

	/*
	 * In sync mode, if this is the first call after Begin or ReScan, we need
	 * to create the cursor on the remote side.  In async mode, we would have
	 * already created the cursor before we get here, even if this is the
	 * first call after Begin or ReScan.
	 */
	if (!fsstate->cursor_exists) {
		create_cursor(node);
	}

	/*
	 * Get some more tuples, if we've run out.
	 */
	if (fsstate->next_tuple >= fsstate->num_tuples) {
		/* No point in another fetch if we already detected EOF, though. */
		/* KITE */
		if (!fsstate->eof_reached)
			fetch_more_data(node);

		/* If we didn't get any tuples, must be end of data. */
		if (fsstate->next_tuple >= fsstate->num_tuples)
			return ExecClearTuple(slot);
	}

	/*
	 * Return the next tuple.
	 */
	ExecStoreHeapTuple(fsstate->tuples[fsstate->next_tuple++],
		slot,
		false);

	return slot;
}

/*
 * postgresReScanForeignScan
 *		Restart the scan.
 */
static void
postgresReScanForeignScan(ForeignScanState *node) {
	/* KITE already disable Param so ReScan do nothing even Postgres get this function */
	return;
}

/*
 * postgresEndForeignScan
 *		Finish scanning foreign table and dispose objects used for this scan
 */
static void
postgresEndForeignScan(ForeignScanState *node) {
	PgFdwScanState *fsstate = (PgFdwScanState *)node->fdw_state;

	/* if fsstate is NULL, we are in EXPLAIN; nothing to do */
	if (fsstate == NULL)
		return;

	ReleaseConnection(fsstate->req);
	fsstate->req = NULL;

	if (fsstate->agg) {
		xrg_agg_destroy(fsstate->agg);
		fsstate->agg = 0;
	}

	/* MemoryContexts will be deleted automatically. */
}

/*
 * estimate_path_cost_size
 *		Get cost and size estimates for a foreign scan on given foreign relation
 *		either a base relation or a join between foreign relations or an upper
 *		relation containing foreign relations.
 *
 * param_join_conds are the parameterization clauses with outer relations.
 * pathkeys specify the expected sort order if any for given path being costed.
 * fpextra specifies additional post-scan/join-processing steps such as the
 * final sort and the LIMIT restriction.
 *
 * The function returns the cost and size estimates in p_rows, p_width,
 * p_startup_cost and p_total_cost variables.
 */
static void
estimate_path_cost_size(PlannerInfo *root,
	RelOptInfo *foreignrel,
	List *param_join_conds,
	List *pathkeys,
	PgFdwPathExtraData *fpextra,
	double *p_rows, int *p_width,
	Cost *p_startup_cost, Cost *p_total_cost) {
	PgFdwRelationInfo *fpinfo = (PgFdwRelationInfo *)foreignrel->fdw_private;
	double rows;
	double retrieved_rows;
	int width;
	Cost startup_cost;
	Cost total_cost;

	/* Make sure the core code has set up the relation's reltarget */
	Assert(foreignrel->reltarget);

	/*
	 * If the table or the server is configured to use remote estimates,
	 * connect to the foreign server and execute EXPLAIN to estimate the
	 * number of rows selected by the restriction+join clauses.  Otherwise,
	 * estimate rows using whatever statistics we have locally, in a way
	 * similar to ordinary tables.
	 */
	if (fpinfo->use_remote_estimate) {
		List *remote_param_join_conds;
		List *local_param_join_conds;
		StringInfoData sql;
		/* KITE */
		kite_request_t *req;

		Selectivity local_sel;
		QualCost local_cost;
		List *fdw_scan_tlist = NIL;
		List *remote_conds;

		/* Required only to be passed to deparseSelectStmtForRel */
		List *retrieved_attrs;
		List *retrieved_aggfnoids;
		List *retrieved_groupby_attrs;

		/*
		 * param_join_conds might contain both clauses that are safe to send
		 * across, and clauses that aren't.
		 */
		classifyConditions(root, foreignrel, param_join_conds,
			&remote_param_join_conds, &local_param_join_conds);

		/* Build the list of columns to be fetched from the foreign server. */
		if (IS_JOIN_REL(foreignrel) || IS_UPPER_REL(foreignrel))
			fdw_scan_tlist = build_tlist_to_deparse(foreignrel);
		else
			fdw_scan_tlist = NIL;

		/*
		 * The complete list of remote conditions includes everything from
		 * baserestrictinfo plus any extra join_conds relevant to this
		 * particular path.
		 */
		remote_conds = list_concat(remote_param_join_conds,
			fpinfo->remote_conds);

		/*
		 * Construct EXPLAIN query including the desired SELECT, FROM, and
		 * WHERE clauses. Params and other-relation Vars are replaced by dummy
		 * values, so don't request params_list.
		 */
		initStringInfo(&sql);
		appendStringInfoString(&sql, "EXPLAIN ");
		deparseSelectStmtForRel(&sql, root, foreignrel, fdw_scan_tlist,
			remote_conds, pathkeys,
			fpextra ? fpextra->has_final_sort : false,
			fpextra ? fpextra->has_limit : false,
			false, &retrieved_attrs, NULL, &retrieved_aggfnoids,
			&retrieved_groupby_attrs);

		/* Get the remote estimate */
		/* KITE */
		rows = 0;
		width = 0;
		startup_cost = 0;
		total_cost = 0;
		/* END KITE */

		retrieved_rows = rows;

		/* Factor in the selectivity of the locally-checked quals */
		local_sel = clauselist_selectivity(root,
			local_param_join_conds,
			foreignrel->relid,
			JOIN_INNER,
			NULL);
		local_sel *= fpinfo->local_conds_sel;

		rows = clamp_row_est(rows * local_sel);

		/* Add in the eval cost of the locally-checked quals */
		startup_cost += fpinfo->local_conds_cost.startup;
		total_cost += fpinfo->local_conds_cost.per_tuple * retrieved_rows;
		cost_qual_eval(&local_cost, local_param_join_conds, root);
		startup_cost += local_cost.startup;
		total_cost += local_cost.per_tuple * retrieved_rows;

		/*
		 * Add in tlist eval cost for each output row.  In case of an
		 * aggregate, some of the tlist expressions such as grouping
		 * expressions will be evaluated remotely, so adjust the costs.
		 */
		startup_cost += foreignrel->reltarget->cost.startup;
		total_cost += foreignrel->reltarget->cost.startup;
		total_cost += foreignrel->reltarget->cost.per_tuple * rows;
		if (IS_UPPER_REL(foreignrel)) {
			QualCost tlist_cost;

			cost_qual_eval(&tlist_cost, fdw_scan_tlist, root);
			startup_cost -= tlist_cost.startup;
			total_cost -= tlist_cost.startup;
			total_cost -= tlist_cost.per_tuple * rows;
		}
	} else {
		Cost run_cost = 0;

		/*
		 * We don't support join conditions in this mode (hence, no
		 * parameterized paths can be made).
		 */
		Assert(param_join_conds == NIL);

		/*
		 * We will come here again and again with different set of pathkeys or
		 * additional post-scan/join-processing steps that caller wants to
		 * cost.  We don't need to calculate the cost/size estimates for the
		 * underlying scan, join, or grouping each time.  Instead, use those
		 * estimates if we have cached them already.
		 */
		if (fpinfo->rel_startup_cost >= 0 && fpinfo->rel_total_cost >= 0) {
			Assert(fpinfo->retrieved_rows >= 0);

			rows = fpinfo->rows;
			retrieved_rows = fpinfo->retrieved_rows;
			width = fpinfo->width;
			startup_cost = fpinfo->rel_startup_cost;
			run_cost = fpinfo->rel_total_cost - fpinfo->rel_startup_cost;

			/*
			 * If we estimate the costs of a foreign scan or a foreign join
			 * with additional post-scan/join-processing steps, the scan or
			 * join costs obtained from the cache wouldn't yet contain the
			 * eval costs for the final scan/join target, which would've been
			 * updated by apply_scanjoin_target_to_paths(); add the eval costs
			 * now.
			 */
			if (fpextra && !IS_UPPER_REL(foreignrel)) {
				/* Shouldn't get here unless we have LIMIT */
				Assert(fpextra->has_limit);
				Assert(foreignrel->reloptkind == RELOPT_BASEREL ||
					   foreignrel->reloptkind == RELOPT_JOINREL);
				startup_cost += foreignrel->reltarget->cost.startup;
				run_cost += foreignrel->reltarget->cost.per_tuple * rows;
			}
		} else if (IS_JOIN_REL(foreignrel)) {
			PgFdwRelationInfo *fpinfo_i;
			PgFdwRelationInfo *fpinfo_o;
			QualCost join_cost;
			QualCost remote_conds_cost;
			double nrows;

			/* Use rows/width estimates made by the core code. */
			rows = foreignrel->rows;
			width = foreignrel->reltarget->width;

			/* For join we expect inner and outer relations set */
			Assert(fpinfo->innerrel && fpinfo->outerrel);

			fpinfo_i = (PgFdwRelationInfo *)fpinfo->innerrel->fdw_private;
			fpinfo_o = (PgFdwRelationInfo *)fpinfo->outerrel->fdw_private;

			/* Estimate of number of rows in cross product */
			nrows = fpinfo_i->rows * fpinfo_o->rows;

			/*
			 * Back into an estimate of the number of retrieved rows.  Just in
			 * case this is nuts, clamp to at most nrows.
			 */
			retrieved_rows = clamp_row_est(rows / fpinfo->local_conds_sel);
			retrieved_rows = Min(retrieved_rows, nrows);

			/*
			 * The cost of foreign join is estimated as cost of generating
			 * rows for the joining relations + cost for applying quals on the
			 * rows.
			 */

			/*
			 * Calculate the cost of clauses pushed down to the foreign server
			 */
			cost_qual_eval(&remote_conds_cost, fpinfo->remote_conds, root);
			/* Calculate the cost of applying join clauses */
			cost_qual_eval(&join_cost, fpinfo->joinclauses, root);

			/*
			 * Startup cost includes startup cost of joining relations and the
			 * startup cost for join and other clauses. We do not include the
			 * startup cost specific to join strategy (e.g. setting up hash
			 * tables) since we do not know what strategy the foreign server
			 * is going to use.
			 */
			startup_cost = fpinfo_i->rel_startup_cost + fpinfo_o->rel_startup_cost;
			startup_cost += join_cost.startup;
			startup_cost += remote_conds_cost.startup;
			startup_cost += fpinfo->local_conds_cost.startup;

			/*
			 * Run time cost includes:
			 *
			 * 1. Run time cost (total_cost - startup_cost) of relations being
			 * joined
			 *
			 * 2. Run time cost of applying join clauses on the cross product
			 * of the joining relations.
			 *
			 * 3. Run time cost of applying pushed down other clauses on the
			 * result of join
			 *
			 * 4. Run time cost of applying nonpushable other clauses locally
			 * on the result fetched from the foreign server.
			 */
			run_cost = fpinfo_i->rel_total_cost - fpinfo_i->rel_startup_cost;
			run_cost += fpinfo_o->rel_total_cost - fpinfo_o->rel_startup_cost;
			run_cost += nrows * join_cost.per_tuple;
			nrows = clamp_row_est(nrows * fpinfo->joinclause_sel);
			run_cost += nrows * remote_conds_cost.per_tuple;
			run_cost += fpinfo->local_conds_cost.per_tuple * retrieved_rows;

			/* Add in tlist eval cost for each output row */
			startup_cost += foreignrel->reltarget->cost.startup;
			run_cost += foreignrel->reltarget->cost.per_tuple * rows;
		} else if (IS_UPPER_REL(foreignrel)) {
			RelOptInfo *outerrel = fpinfo->outerrel;
			PgFdwRelationInfo *ofpinfo;
			AggClauseCosts aggcosts;
			double input_rows;
			int numGroupCols;
			double numGroups = 1;

			/* The upper relation should have its outer relation set */
			Assert(outerrel);
			/* and that outer relation should have its reltarget set */
			Assert(outerrel->reltarget);

			/*
			 * This cost model is mixture of costing done for sorted and
			 * hashed aggregates in cost_agg().  We are not sure which
			 * strategy will be considered at remote side, thus for
			 * simplicity, we put all startup related costs in startup_cost
			 * and all finalization and run cost are added in total_cost.
			 */

			ofpinfo = (PgFdwRelationInfo *)outerrel->fdw_private;

			/* Get rows from input rel */
			input_rows = ofpinfo->rows;

			/* Collect statistics about aggregates for estimating costs. */
			MemSet(&aggcosts, 0, sizeof(AggClauseCosts));
			if (root->parse->hasAggs) {
				get_agg_clause_costs(root, AGGSPLIT_SIMPLE, &aggcosts);
			}

			/* Get number of grouping columns and possible number of groups */
			numGroupCols = list_length(root->parse->groupClause);
			numGroups = estimate_num_groups(root,
				get_sortgrouplist_exprs(root->parse->groupClause,
					fpinfo->grouped_tlist),
				input_rows, NULL, NULL);

			/*
			 * Get the retrieved_rows and rows estimates.  If there are HAVING
			 * quals, account for their selectivity.
			 */
			if (root->parse->havingQual) {
				/* Factor in the selectivity of the remotely-checked quals */
				retrieved_rows =
					clamp_row_est(numGroups *
								  clauselist_selectivity(root,
									  fpinfo->remote_conds,
									  0,
									  JOIN_INNER,
									  NULL));
				/* Factor in the selectivity of the locally-checked quals */
				rows = clamp_row_est(retrieved_rows * fpinfo->local_conds_sel);
			} else {
				rows = retrieved_rows = numGroups;
			}

			/* Use width estimate made by the core code. */
			width = foreignrel->reltarget->width;

			/*-----
			 * Startup cost includes:
			 *	  1. Startup cost for underneath input relation, adjusted for
			 *	     tlist replacement by apply_scanjoin_target_to_paths()
			 *	  2. Cost of performing aggregation, per cost_agg()
			 *-----
			 */
			startup_cost = ofpinfo->rel_startup_cost;
			startup_cost += outerrel->reltarget->cost.startup;
			startup_cost += aggcosts.transCost.startup;
			startup_cost += aggcosts.transCost.per_tuple * input_rows;
			startup_cost += aggcosts.finalCost.startup;
			startup_cost += (cpu_operator_cost * numGroupCols) * input_rows;

			/*-----
			 * Run time cost includes:
			 *	  1. Run time cost of underneath input relation, adjusted for
			 *	     tlist replacement by apply_scanjoin_target_to_paths()
			 *	  2. Run time cost of performing aggregation, per cost_agg()
			 *-----
			 */
			run_cost = ofpinfo->rel_total_cost - ofpinfo->rel_startup_cost;
			run_cost += outerrel->reltarget->cost.per_tuple * input_rows;
			run_cost += aggcosts.finalCost.per_tuple * numGroups;
			run_cost += cpu_tuple_cost * numGroups;

			/* Account for the eval cost of HAVING quals, if any */
			if (root->parse->havingQual) {
				QualCost remote_cost;

				/* Add in the eval cost of the remotely-checked quals */
				cost_qual_eval(&remote_cost, fpinfo->remote_conds, root);
				startup_cost += remote_cost.startup;
				run_cost += remote_cost.per_tuple * numGroups;
				/* Add in the eval cost of the locally-checked quals */
				startup_cost += fpinfo->local_conds_cost.startup;
				run_cost += fpinfo->local_conds_cost.per_tuple * retrieved_rows;
			}

			/* Add in tlist eval cost for each output row */
			startup_cost += foreignrel->reltarget->cost.startup;
			run_cost += foreignrel->reltarget->cost.per_tuple * rows;
		} else {
			Cost cpu_per_tuple;

			/* Use rows/width estimates made by set_baserel_size_estimates. */
			rows = foreignrel->rows;
			width = foreignrel->reltarget->width;

			/*
			 * Back into an estimate of the number of retrieved rows.  Just in
			 * case this is nuts, clamp to at most foreignrel->tuples.
			 */
			retrieved_rows = clamp_row_est(rows / fpinfo->local_conds_sel);
			retrieved_rows = Min(retrieved_rows, foreignrel->tuples);

			/*
			 * Cost as though this were a seqscan, which is pessimistic.  We
			 * effectively imagine the local_conds are being evaluated
			 * remotely, too.
			 */
			startup_cost = 0;
			run_cost = 0;
			run_cost += seq_page_cost * foreignrel->pages;

			startup_cost += foreignrel->baserestrictcost.startup;
			cpu_per_tuple = cpu_tuple_cost + foreignrel->baserestrictcost.per_tuple;
			run_cost += cpu_per_tuple * foreignrel->tuples;

			/* Add in tlist eval cost for each output row */
			startup_cost += foreignrel->reltarget->cost.startup;
			run_cost += foreignrel->reltarget->cost.per_tuple * rows;
		}

		/*
		 * Without remote estimates, we have no real way to estimate the cost
		 * of generating sorted output.  It could be free if the query plan
		 * the remote side would have chosen generates properly-sorted output
		 * anyway, but in most cases it will cost something.  Estimate a value
		 * high enough that we won't pick the sorted path when the ordering
		 * isn't locally useful, but low enough that we'll err on the side of
		 * pushing down the ORDER BY clause when it's useful to do so.
		 */
		if (pathkeys != NIL) {
			if (IS_UPPER_REL(foreignrel)) {
				Assert(foreignrel->reloptkind == RELOPT_UPPER_REL &&
					   fpinfo->stage == UPPERREL_GROUP_AGG);
				adjust_foreign_grouping_path_cost(root, pathkeys,
					retrieved_rows, width,
					fpextra->limit_tuples,
					&startup_cost, &run_cost);
			} else {
				startup_cost *= DEFAULT_FDW_SORT_MULTIPLIER;
				run_cost *= DEFAULT_FDW_SORT_MULTIPLIER;
			}
		}

		total_cost = startup_cost + run_cost;

		/* Adjust the cost estimates if we have LIMIT */
		if (fpextra && fpextra->has_limit) {
			adjust_limit_rows_costs(&rows, &startup_cost, &total_cost,
				fpextra->offset_est, fpextra->count_est);
			retrieved_rows = rows;
		}
	}

	/*
	 * If this includes the final sort step, the given target, which will be
	 * applied to the resulting path, might have different expressions from
	 * the foreignrel's reltarget (see make_sort_input_target()); adjust tlist
	 * eval costs.
	 */
	if (fpextra && fpextra->has_final_sort &&
		fpextra->target != foreignrel->reltarget) {
		QualCost oldcost = foreignrel->reltarget->cost;
		QualCost newcost = fpextra->target->cost;

		startup_cost += newcost.startup - oldcost.startup;
		total_cost += newcost.startup - oldcost.startup;
		total_cost += (newcost.per_tuple - oldcost.per_tuple) * rows;
	}

	/*
	 * Cache the retrieved rows and cost estimates for scans, joins, or
	 * groupings without any parameterization, pathkeys, or additional
	 * post-scan/join-processing steps, before adding the costs for
	 * transferring data from the foreign server.  These estimates are useful
	 * for costing remote joins involving this relation or costing other
	 * remote operations on this relation such as remote sorts and remote
	 * LIMIT restrictions, when the costs can not be obtained from the foreign
	 * server.  This function will be called at least once for every foreign
	 * relation without any parameterization, pathkeys, or additional
	 * post-scan/join-processing steps.
	 */
	if (pathkeys == NIL && param_join_conds == NIL && fpextra == NULL) {
		fpinfo->retrieved_rows = retrieved_rows;
		fpinfo->rel_startup_cost = startup_cost;
		fpinfo->rel_total_cost = total_cost;
	}

	/*
	 * Add some additional cost factors to account for connection overhead
	 * (fdw_startup_cost), transferring data across the network
	 * (fdw_tuple_cost per retrieved row), and local manipulation of the data
	 * (cpu_tuple_cost per retrieved row).
	 */
	startup_cost += fpinfo->fdw_startup_cost;
	total_cost += fpinfo->fdw_startup_cost;
	total_cost += fpinfo->fdw_tuple_cost * retrieved_rows;
	total_cost += cpu_tuple_cost * retrieved_rows;

	/*
	 * If we have LIMIT, we should prefer performing the restriction remotely
	 * rather than locally, as the former avoids extra row fetches from the
	 * remote that the latter might cause.  But since the core code doesn't
	 * account for such fetches when estimating the costs of the local
	 * restriction (see create_limit_path()), there would be no difference
	 * between the costs of the local restriction and the costs of the remote
	 * restriction estimated above if we don't use remote estimates (except
	 * for the case where the foreignrel is a grouping relation, the given
	 * pathkeys is not NIL, and the effects of a bounded sort for that rel is
	 * accounted for in costing the remote restriction).  Tweak the costs of
	 * the remote restriction to ensure we'll prefer it if LIMIT is a useful
	 * one.
	 */
	if (!fpinfo->use_remote_estimate &&
		fpextra && fpextra->has_limit &&
		fpextra->limit_tuples > 0 &&
		fpextra->limit_tuples < fpinfo->rows) {
		Assert(fpinfo->rows > 0);
		total_cost -= (total_cost - startup_cost) * 0.05 *
					  (fpinfo->rows - fpextra->limit_tuples) / fpinfo->rows;
	}

	/* Return results. */
	*p_rows = rows;
	*p_width = width;
	*p_startup_cost = startup_cost;
	*p_total_cost = total_cost;
}

/*
 * Adjust the cost estimates of a foreign grouping path to include the cost of
 * generating properly-sorted output.
 */
static void
adjust_foreign_grouping_path_cost(PlannerInfo *root,
	List *pathkeys,
	double retrieved_rows,
	double width,
	double limit_tuples,
	Cost *p_startup_cost,
	Cost *p_run_cost) {
	/*
	 * If the GROUP BY clause isn't sort-able, the plan chosen by the remote
	 * side is unlikely to generate properly-sorted output, so it would need
	 * an explicit sort; adjust the given costs with cost_sort().  Likewise,
	 * if the GROUP BY clause is sort-able but isn't a superset of the given
	 * pathkeys, adjust the costs with that function.  Otherwise, adjust the
	 * costs by applying the same heuristic as for the scan or join case.
	 */
	if (!grouping_is_sortable(root->parse->groupClause) ||
		!pathkeys_contained_in(pathkeys, root->group_pathkeys)) {
		Path sort_path; /* dummy for result of cost_sort */

		cost_sort(&sort_path,
			root,
			pathkeys,
			*p_startup_cost + *p_run_cost,
			retrieved_rows,
			width,
			0.0,
			work_mem,
			limit_tuples);

		*p_startup_cost = sort_path.startup_cost;
		*p_run_cost = sort_path.total_cost - sort_path.startup_cost;
	} else {
		/*
		 * The default extra cost seems too large for foreign-grouping cases;
		 * add 1/4th of that default.
		 */
		double sort_multiplier = 1.0 + (DEFAULT_FDW_SORT_MULTIPLIER - 1.0) * 0.25;

		*p_startup_cost *= sort_multiplier;
		*p_run_cost *= sort_multiplier;
	}
}

/*
 * Create cursor for node's query with current parameter values.
 */
static void
create_cursor(ForeignScanState *node) {
	PgFdwScanState *fsstate = (PgFdwScanState *)node->fdw_state;
	ExprContext *econtext = node->ss.ps.ps_ExprContext;
	int numParams = fsstate->numParams;
	const char **values = fsstate->param_values;
	kite_request_t *req = fsstate->req;
	char errmsg[1024];

	/*
	 * Construct array of query parameter values in text format.  We do the
	 * conversions in the short-lived per-tuple context, so as not to cause a
	 * memory leak over repeated scans.
	 */
	if (numParams > 0) {
		MemoryContext oldcontext;

		oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

		process_query_params(econtext,
			fsstate->param_flinfo,
			fsstate->param_exprs,
			values);

		MemoryContextSwitchTo(oldcontext);
	}

	/* kite_submit */
	req->hdl = kite_submit(req->host, fsstate->schema, fsstate->query, -1, fsstate->fragcnt, errmsg, sizeof(errmsg));
	if (!req->hdl) {
		elog(ERROR, "kite_submit failed");
		return;
	}

	/* Mark the cursor as created, and show no tuples have been retrieved */
	fsstate->cursor_exists = true;
	fsstate->tuples = NULL;
	fsstate->num_tuples = 0;
	fsstate->next_tuple = 0;
	fsstate->fetch_ct_2 = 0;
	fsstate->eof_reached = false;
}

/*
 * Fetch some more rows from the node's cursor.
 */
static void
fetch_more_data(ForeignScanState *node) {
	PgFdwScanState *fsstate = (PgFdwScanState *)node->fdw_state;
	char errmsg[1024];
	MemoryContext oldcontext;

	/*
	 * We'll store the tuples in the batch_cxt.  First, flush the previous
	 * batch.
	 */
	fsstate->tuples = NULL;
	MemoryContextReset(fsstate->batch_cxt);
	oldcontext = MemoryContextSwitchTo(fsstate->batch_cxt);

	PG_TRY();
	{
		kite_handle_t *hdl = fsstate->req->hdl;
		int numrows;
		int i;

		if (fsstate->agg) {
			int batchsz = fsstate->fetch_size;

			xrg_agg_fetch(fsstate->agg, hdl);

			numrows = 0;
			fsstate->tuples = (HeapTuple *)palloc0(batchsz * sizeof(HeapTuple));
			fsstate->num_tuples = batchsz;
			fsstate->next_tuple = 0;
			for (i = 0; i < batchsz; i++) {
				Assert(IsA(node->ss.ps.plan, ForeignScan));
				fsstate->tuples[i] = make_tuple_from_agg(fsstate->agg,
					i,
					fsstate->rel,
					fsstate->attinmeta,
					fsstate->retrieved_attrs,
					node,
					fsstate->temp_cxt);
				if (!fsstate->tuples[i]) {
					break;
				}
				numrows++;
			}

			fsstate->num_tuples = numrows;
			fsstate->next_tuple = 0;
			fsstate->eof_reached = (numrows < batchsz);

		} else {
			int batchsz = fsstate->fetch_size;
			xrg_iter_t *iter = 0;
			int e = 0;
			numrows = 0;

			fsstate->tuples = (HeapTuple *)palloc0(batchsz * sizeof(HeapTuple));
			fsstate->num_tuples = batchsz;
			fsstate->next_tuple = 0;

			for (i = 0; i < batchsz && fsstate->eof_reached == false; i++) {
				Assert(IsA(node->ss.ps.plan, ForeignScan));

				e = kite_next_row(hdl, &iter, errmsg, sizeof(errmsg));
				if (e == 0) {
					if (iter == 0) {
						fsstate->eof_reached = true;
						break;
					}

					fsstate->tuples[i] = make_tuple_from_result_row(iter,
						i,
						fsstate->rel,
						fsstate->attinmeta,
						fsstate->retrieved_attrs,
						node,
						fsstate->temp_cxt);
					numrows++;
				} else {
					// error
					elog(ERROR, "kite_next_row failed");
					fsstate->eof_reached = true;
				}
			}

			//elog(LOG, "fetch_more_data %d row", numrows);
			fsstate->num_tuples = numrows;
			fsstate->next_tuple = 0;

			/* Update fetch_ct_2 */
			if (fsstate->fetch_ct_2 < 2)
				fsstate->fetch_ct_2++;
		}
	}
	PG_FINALLY();
	{
		;
	}
	PG_END_TRY();

	MemoryContextSwitchTo(oldcontext);
}

/*
 * Force assorted GUC parameters to settings that ensure that we'll output
 * data values in a form that is unambiguous to the remote server.
 *
 * This is rather expensive and annoying to do once per row, but there's
 * little choice if we want to be sure values are transmitted accurately;
 * we can't leave the settings in place between rows for fear of affecting
 * user-visible computations.
 *
 * We use the equivalent of a function SET option to allow the settings to
 * persist only until the caller calls reset_transmission_modes().  If an
 * error is thrown in between, guc.c will take care of undoing the settings.
 *
 * The return value is the nestlevel that must be passed to
 * reset_transmission_modes() to undo things.
 */
int set_transmission_modes(void) {
	int nestlevel = NewGUCNestLevel();

	/*
	 * The values set here should match what pg_dump does.  See also
	 * configure_remote_session in connection.c.
	 */
	if (DateStyle != USE_ISO_DATES)
		(void)set_config_option("datestyle", "ISO",
			PGC_USERSET, PGC_S_SESSION,
			GUC_ACTION_SAVE, true, 0, false);
	if (IntervalStyle != INTSTYLE_POSTGRES)
		(void)set_config_option("intervalstyle", "postgres",
			PGC_USERSET, PGC_S_SESSION,
			GUC_ACTION_SAVE, true, 0, false);
	if (extra_float_digits < 3)
		(void)set_config_option("extra_float_digits", "3",
			PGC_USERSET, PGC_S_SESSION,
			GUC_ACTION_SAVE, true, 0, false);

	/*
	 * In addition force restrictive search_path, in case there are any
	 * regproc or similar constants to be printed.
	 */
	(void)set_config_option("search_path", "pg_catalog",
		PGC_USERSET, PGC_S_SESSION,
		GUC_ACTION_SAVE, true, 0, false);

	return nestlevel;
}

/*
 * Undo the effects of set_transmission_modes().
 */
void reset_transmission_modes(int nestlevel) {
	AtEOXact_GUC(true, nestlevel);
}

/*
 * Prepare for processing of parameters used in remote query.
 */
static void
prepare_query_params(PlanState *node,
	List *fdw_exprs,
	int numParams,
	FmgrInfo **param_flinfo,
	List **param_exprs,
	const char ***param_values) {
	int i;
	ListCell *lc;

	Assert(numParams > 0);

	/* Prepare for output conversion of parameters used in remote query. */
	*param_flinfo = (FmgrInfo *)palloc0(sizeof(FmgrInfo) * numParams);

	i = 0;
	foreach (lc, fdw_exprs) {
		Node *param_expr = (Node *)lfirst(lc);
		Oid typefnoid;
		bool isvarlena;

		getTypeOutputInfo(exprType(param_expr), &typefnoid, &isvarlena);
		fmgr_info(typefnoid, &(*param_flinfo)[i]);
		i++;
	}

	/*
	 * Prepare remote-parameter expressions for evaluation.  (Note: in
	 * practice, we expect that all these expressions will be just Params, so
	 * we could possibly do something more efficient than using the full
	 * expression-eval machinery for this.  But probably there would be little
	 * benefit, and it'd require postgres_fdw to know more than is desirable
	 * about Param evaluation.)
	 */
	*param_exprs = ExecInitExprList(fdw_exprs, node);

	/* Allocate buffer for text form of query parameters. */
	*param_values = (const char **)palloc0(numParams * sizeof(char *));
}

/*
 * Construct array of query parameter values in text format.
 */
static void
process_query_params(ExprContext *econtext,
	FmgrInfo *param_flinfo,
	List *param_exprs,
	const char **param_values) {
	int nestlevel;
	int i;
	ListCell *lc;

	nestlevel = set_transmission_modes();

	i = 0;
	foreach (lc, param_exprs) {
		ExprState *expr_state = (ExprState *)lfirst(lc);
		Datum expr_value;
		bool isNull;

		/* Evaluate the parameter expression */
		expr_value = ExecEvalExpr(expr_state, econtext, &isNull);

		/*
		 * Get string representation of each parameter value by invoking
		 * type-specific output function, unless the value is null.
		 */
		if (isNull)
			param_values[i] = NULL;
		else
			param_values[i] = OutputFunctionCall(&param_flinfo[i], expr_value);

		i++;
	}

	reset_transmission_modes(nestlevel);
}

/*
 * Parse options from foreign server and apply them to fpinfo.
 *
 * New options might also require tweaking merge_fdw_options().
 */
static void
apply_server_options(PgFdwRelationInfo *fpinfo) {
	ListCell *lc;

	foreach (lc, fpinfo->server->options) {
		DefElem *def = (DefElem *)lfirst(lc);

		if (strcmp(def->defname, "use_remote_estimate") == 0)
			fpinfo->use_remote_estimate = defGetBoolean(def);
		else if (strcmp(def->defname, "fdw_startup_cost") == 0)
			(void)parse_real(defGetString(def), &fpinfo->fdw_startup_cost, 0,
				NULL);
		else if (strcmp(def->defname, "fdw_tuple_cost") == 0)
			(void)parse_real(defGetString(def), &fpinfo->fdw_tuple_cost, 0,
				NULL);
		else if (strcmp(def->defname, "extensions") == 0)
			fpinfo->shippable_extensions =
				ExtractExtensionList(defGetString(def), false);
		else if (strcmp(def->defname, "fetch_size") == 0)
			(void)parse_int(defGetString(def), &fpinfo->fetch_size, 0, NULL);
		else if (strcmp(def->defname, "async_capable") == 0)
			fpinfo->async_capable = defGetBoolean(def);
		else if (strcmp(def->defname, "fragcnt") == 0)
			(void)parse_int(defGetString(def), &fpinfo->fragcnt, 0, NULL);
	}
}

/*
 * Parse options from foreign table and apply them to fpinfo.
 *
 * New options might also require tweaking merge_fdw_options().
 */
static void
apply_table_options(PgFdwRelationInfo *fpinfo) {
	ListCell *lc;

	foreach (lc, fpinfo->table->options) {
		DefElem *def = (DefElem *)lfirst(lc);

		if (strcmp(def->defname, "use_remote_estimate") == 0)
			fpinfo->use_remote_estimate = defGetBoolean(def);
		else if (strcmp(def->defname, "fetch_size") == 0)
			(void)parse_int(defGetString(def), &fpinfo->fetch_size, 0, NULL);
		else if (strcmp(def->defname, "async_capable") == 0)
			fpinfo->async_capable = defGetBoolean(def);
	}
}

/*
 * Merge FDW options from input relations into a new set of options for a join
 * or an upper rel.
 *
 * For a join relation, FDW-specific information about the inner and outer
 * relations is provided using fpinfo_i and fpinfo_o.  For an upper relation,
 * fpinfo_o provides the information for the input relation; fpinfo_i is
 * expected to NULL.
 */
static void
merge_fdw_options(PgFdwRelationInfo *fpinfo,
	const PgFdwRelationInfo *fpinfo_o,
	const PgFdwRelationInfo *fpinfo_i) {
	/* We must always have fpinfo_o. */
	Assert(fpinfo_o);

	/* fpinfo_i may be NULL, but if present the servers must both match. */
	Assert(!fpinfo_i ||
		   fpinfo_i->server->serverid == fpinfo_o->server->serverid);

	/*
	 * Copy the server specific FDW options.  (For a join, both relations come
	 * from the same server, so the server options should have the same value
	 * for both relations.)
	 */
	fpinfo->fdw_startup_cost = fpinfo_o->fdw_startup_cost;
	fpinfo->fdw_tuple_cost = fpinfo_o->fdw_tuple_cost;
	fpinfo->shippable_extensions = fpinfo_o->shippable_extensions;
	fpinfo->use_remote_estimate = fpinfo_o->use_remote_estimate;
	fpinfo->fetch_size = fpinfo_o->fetch_size;
	fpinfo->fragcnt = fpinfo_o->fragcnt;
	fpinfo->async_capable = fpinfo_o->async_capable;

	/* Merge the table level options from either side of the join. */
	if (fpinfo_i) {
		/*
		 * We'll prefer to use remote estimates for this join if any table
		 * from either side of the join is using remote estimates.  This is
		 * most likely going to be preferred since they're already willing to
		 * pay the price of a round trip to get the remote EXPLAIN.  In any
		 * case it's not entirely clear how we might otherwise handle this
		 * best.
		 */
		fpinfo->use_remote_estimate = fpinfo_o->use_remote_estimate ||
									  fpinfo_i->use_remote_estimate;

		/*
		 * Set fetch size to maximum of the joining sides, since we are
		 * expecting the rows returned by the join to be proportional to the
		 * relation sizes.
		 */
		fpinfo->fetch_size = Max(fpinfo_o->fetch_size, fpinfo_i->fetch_size);

		/* KITE */
		fpinfo->fragcnt = Max(fpinfo_o->fragcnt, fpinfo_i->fragcnt);

		/*
		 * We'll prefer to consider this join async-capable if any table from
		 * either side of the join is considered async-capable.  This would be
		 * reasonable because in that case the foreign server would have its
		 * own resources to scan that table asynchronously, and the join could
		 * also be computed asynchronously using the resources.
		 */
		fpinfo->async_capable = fpinfo_o->async_capable ||
								fpinfo_i->async_capable;
	}
}

/*
 * Assess whether the aggregation, grouping and having operations can be pushed
 * down to the foreign server.  As a side effect, save information we obtain in
 * this function to PgFdwRelationInfo of the input relation.
 */
static bool
foreign_grouping_ok(PlannerInfo *root, RelOptInfo *grouped_rel,
	Node *havingQual) {
	Query *query = root->parse;
	PgFdwRelationInfo *fpinfo = (PgFdwRelationInfo *)grouped_rel->fdw_private;
	PathTarget *grouping_target = grouped_rel->reltarget;
	PgFdwRelationInfo *ofpinfo;
	ListCell *lc;
	int i;
	List *tlist = NIL;

	/* We currently don't support pushing Grouping Sets. */
	if (query->groupingSets)
		return false;

	/* Get the fpinfo of the underlying scan relation. */
	ofpinfo = (PgFdwRelationInfo *)fpinfo->outerrel->fdw_private;

	/*
	 * If underlying scan relation has any local conditions, those conditions
	 * are required to be applied before performing aggregation.  Hence the
	 * aggregate cannot be pushed down.
	 */
	if (ofpinfo->local_conds)
		return false;

	/*
	 * Examine grouping expressions, as well as other expressions we'd need to
	 * compute, and check whether they are safe to push down to the foreign
	 * server.  All GROUP BY expressions will be part of the grouping target
	 * and thus there is no need to search for them separately.  Add grouping
	 * expressions into target list which will be passed to foreign server.
	 *
	 * A tricky fine point is that we must not put any expression into the
	 * target list that is just a foreign param (that is, something that
	 * deparse.c would conclude has to be sent to the foreign server).  If we
	 * do, the expression will also appear in the fdw_exprs list of the plan
	 * node, and setrefs.c will get confused and decide that the fdw_exprs
	 * entry is actually a reference to the fdw_scan_tlist entry, resulting in
	 * a broken plan.  Somewhat oddly, it's OK if the expression contains such
	 * a node, as long as it's not at top level; then no match is possible.
	 */
	i = 0;
	foreach (lc, grouping_target->exprs) {
		Expr *expr = (Expr *)lfirst(lc);
		Index sgref = get_pathtarget_sortgroupref(grouping_target, i);
		ListCell *l;

		/* Check whether this expression is part of GROUP BY clause */
		if (sgref && get_sortgroupref_clause_noerr(sgref, query->groupClause)) {
			TargetEntry *tle;

			/*
			 * If any GROUP BY expression is not shippable, then we cannot
			 * push down aggregation to the foreign server.
			 */
			if (!is_foreign_expr(root, grouped_rel, expr))
				return false;

			/*
			 * If it would be a foreign param, we can't put it into the tlist,
			 * so we have to fail.
			 */
			if (is_foreign_param(root, grouped_rel, expr))
				return false;

			/*
			 * Pushable, so add to tlist.  We need to create a TLE for this
			 * expression and apply the sortgroupref to it.  We cannot use
			 * add_to_flat_tlist() here because that avoids making duplicate
			 * entries in the tlist.  If there are duplicate entries with
			 * distinct sortgrouprefs, we have to duplicate that situation in
			 * the output tlist.
			 */
			tle = makeTargetEntry(expr, list_length(tlist) + 1, NULL, false);
			tle->ressortgroupref = sgref;
			tlist = lappend(tlist, tle);
		} else {
			/*
			 * Non-grouping expression we need to compute.  Can we ship it
			 * as-is to the foreign server?
			 */
			if (is_foreign_expr(root, grouped_rel, expr) &&
				!is_foreign_param(root, grouped_rel, expr)) {
				/* Yes, so add to tlist as-is; OK to suppress duplicates */
				tlist = add_to_flat_tlist(tlist, list_make1(expr));
			} else {
				/* Not pushable as a whole; extract its Vars and aggregates */
				List *aggvars;

				aggvars = pull_var_clause((Node *)expr,
					PVC_INCLUDE_AGGREGATES);

				/*
				 * If any aggregate expression is not shippable, then we
				 * cannot push down aggregation to the foreign server.  (We
				 * don't have to check is_foreign_param, since that certainly
				 * won't return true for any such expression.)
				 */
				if (!is_foreign_expr(root, grouped_rel, (Expr *)aggvars))
					return false;

				/*
				 * Add aggregates, if any, into the targetlist.  Plain Vars
				 * outside an aggregate can be ignored, because they should be
				 * either same as some GROUP BY column or part of some GROUP
				 * BY expression.  In either case, they are already part of
				 * the targetlist and thus no need to add them again.  In fact
				 * including plain Vars in the tlist when they do not match a
				 * GROUP BY column would cause the foreign server to complain
				 * that the shipped query is invalid.
				 */
				foreach (l, aggvars) {
					Expr *expr = (Expr *)lfirst(l);

					if (IsA(expr, Aggref))
						tlist = add_to_flat_tlist(tlist, list_make1(expr));
				}
			}
		}

		i++;
	}

	/*
	 * Classify the pushable and non-pushable HAVING clauses and save them in
	 * remote_conds and local_conds of the grouped rel's fpinfo.
	 */
	if (havingQual) {
		ListCell *lc;

		foreach (lc, (List *)havingQual) {
			Expr *expr = (Expr *)lfirst(lc);
			RestrictInfo *rinfo;

			/*
			 * Currently, the core code doesn't wrap havingQuals in
			 * RestrictInfos, so we must make our own.
			 */
			Assert(!IsA(expr, RestrictInfo));
			rinfo = make_restrictinfo(root,
				expr,
				true,
				false,
				false,
				root->qual_security_level,
				grouped_rel->relids,
				NULL,
				NULL);

#if 1
			/* KITE cannot handle HAVING */
			fpinfo->local_conds = lappend(fpinfo->local_conds, rinfo);
#else
			if (is_foreign_expr(root, grouped_rel, expr))
				fpinfo->remote_conds = lappend(fpinfo->remote_conds, rinfo);
			else
				fpinfo->local_conds = lappend(fpinfo->local_conds, rinfo);
#endif
		}
	}

	/*
	 * If there are any local conditions, pull Vars and aggregates from it and
	 * check whether they are safe to pushdown or not.
	 */
	if (fpinfo->local_conds) {
		List *aggvars = NIL;
		ListCell *lc;

		foreach (lc, fpinfo->local_conds) {
			RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

			aggvars = list_concat(aggvars,
				pull_var_clause((Node *)rinfo->clause,
					PVC_INCLUDE_AGGREGATES));
		}

		foreach (lc, aggvars) {
			Expr *expr = (Expr *)lfirst(lc);

			/*
			 * If aggregates within local conditions are not safe to push
			 * down, then we cannot push down the query.  Vars are already
			 * part of GROUP BY clause which are checked above, so no need to
			 * access them again here.  Again, we need not check
			 * is_foreign_param for a foreign aggregate.
			 */
			if (IsA(expr, Aggref)) {
				if (!is_foreign_expr(root, grouped_rel, expr))
					return false;

				tlist = add_to_flat_tlist(tlist, list_make1(expr));
			}
		}
	}

	/* Store generated targetlist */
	fpinfo->grouped_tlist = tlist;

	/* Safe to pushdown */
	fpinfo->pushdown_safe = true;

	/*
	 * Set # of retrieved rows and cached relation costs to some negative
	 * value, so that we can detect when they are set to some sensible values,
	 * during one (usually the first) of the calls to estimate_path_cost_size.
	 */
	fpinfo->retrieved_rows = -1;
	fpinfo->rel_startup_cost = -1;
	fpinfo->rel_total_cost = -1;

	/*
	 * Set the string describing this grouped relation to be used in EXPLAIN
	 * output of corresponding ForeignScan.  Note that the decoration we add
	 * to the base relation name mustn't include any digits, or it'll confuse
	 * postgresExplainForeignScan.
	 */
	fpinfo->relation_name = psprintf("Aggregate on (%s)",
		ofpinfo->relation_name);

	return true;
}

/*
 * postgresGetForeignUpperPaths
 *		Add paths for post-join operations like aggregation, grouping etc. if
 *		corresponding operations are safe to push down.
 */
static void
postgresGetForeignUpperPaths(PlannerInfo *root, UpperRelationKind stage,
	RelOptInfo *input_rel, RelOptInfo *output_rel,
	void *extra) {
	PgFdwRelationInfo *fpinfo;

	/*
	 * If input rel is not safe to pushdown, then simply return as we cannot
	 * perform any post-join operations on the foreign server.
	 */
	if (!input_rel->fdw_private ||
		!((PgFdwRelationInfo *)input_rel->fdw_private)->pushdown_safe)
		return;

	/* Ignore stages we don't support; and skip any duplicate calls. */
	/* KITE don't support ORDER BY */
	if ((stage != UPPERREL_GROUP_AGG &&
			stage != UPPERREL_FINAL) ||
		output_rel->fdw_private)
		return;

	fpinfo = (PgFdwRelationInfo *)palloc0(sizeof(PgFdwRelationInfo));
	fpinfo->pushdown_safe = false;
	fpinfo->stage = stage;
	output_rel->fdw_private = fpinfo;

	switch (stage) {
	case UPPERREL_GROUP_AGG:
		add_foreign_grouping_paths(root, input_rel, output_rel,
			(GroupPathExtraData *)extra);
		break;
	case UPPERREL_FINAL:
		add_foreign_final_paths(root, input_rel, output_rel,
			(FinalPathExtraData *)extra);
		break;
	default:
		elog(ERROR, "unexpected upper relation: %d", (int)stage);
		break;
	}
}

/*
 * add_foreign_grouping_paths
 *		Add foreign path for grouping and/or aggregation.
 *
 * Given input_rel represents the underlying scan.  The paths are added to the
 * given grouped_rel.
 */
static void
add_foreign_grouping_paths(PlannerInfo *root, RelOptInfo *input_rel,
	RelOptInfo *grouped_rel,
	GroupPathExtraData *extra) {
	Query *parse = root->parse;
	PgFdwRelationInfo *ifpinfo = input_rel->fdw_private;
	PgFdwRelationInfo *fpinfo = grouped_rel->fdw_private;
	ForeignPath *grouppath;
	double rows;
	int width;
	Cost startup_cost;
	Cost total_cost;

	/* Nothing to be done, if there is no grouping or aggregation required. */
	if (!parse->groupClause && !parse->groupingSets && !parse->hasAggs &&
		!root->hasHavingQual)
		return;

	Assert(extra->patype == PARTITIONWISE_AGGREGATE_NONE ||
		   extra->patype == PARTITIONWISE_AGGREGATE_FULL);

	/* save the input_rel as outerrel in fpinfo */
	fpinfo->outerrel = input_rel;

	/*
	 * Copy foreign table, foreign server, user mapping, FDW options etc.
	 * details from the input relation's fpinfo.
	 */
	fpinfo->table = ifpinfo->table;
	fpinfo->server = ifpinfo->server;
	fpinfo->user = ifpinfo->user;
	merge_fdw_options(fpinfo, ifpinfo, NULL);

	/*
	 * Assess if it is safe to push down aggregation and grouping.
	 *
	 * Use HAVING qual from extra. In case of child partition, it will have
	 * translated Vars.
	 */
	if (!foreign_grouping_ok(root, grouped_rel, extra->havingQual))
		return;

	/*
	 * Compute the selectivity and cost of the local_conds, so we don't have
	 * to do it over again for each path.  (Currently we create just a single
	 * path here, but in future it would be possible that we build more paths
	 * such as pre-sorted paths as in postgresGetForeignPaths and
	 * postgresGetForeignJoinPaths.)  The best we can do for these conditions
	 * is to estimate selectivity on the basis of local statistics.
	 */
	fpinfo->local_conds_sel = clauselist_selectivity(root,
		fpinfo->local_conds,
		0,
		JOIN_INNER,
		NULL);

	cost_qual_eval(&fpinfo->local_conds_cost, fpinfo->local_conds, root);

	/* Estimate the cost of push down */
	estimate_path_cost_size(root, grouped_rel, NIL, NIL, NULL,
		&rows, &width, &startup_cost, &total_cost);

	/* Now update this information in the fpinfo */
	fpinfo->rows = rows;
	fpinfo->width = width;
	fpinfo->startup_cost = startup_cost;
	fpinfo->total_cost = total_cost;

	/* Create and add foreign path to the grouping relation. */
	grouppath = create_foreign_upper_path(root,
		grouped_rel,
		grouped_rel->reltarget,
		rows,
		startup_cost,
		total_cost,
		NIL, /* no pathkeys */
		NULL,
		NIL); /* no fdw_private */

	/* Add generated path into grouped_rel by add_path(). */
	add_path(grouped_rel, (Path *)grouppath);
}

/*
 * add_foreign_final_paths
 *		Add foreign paths for performing the final processing remotely.
 *
 * Given input_rel contains the source-data Paths.  The paths are added to the
 * given final_rel.
 */
static void
add_foreign_final_paths(PlannerInfo *root, RelOptInfo *input_rel,
	RelOptInfo *final_rel,
	FinalPathExtraData *extra) {
	Query *parse = root->parse;
	PgFdwRelationInfo *ifpinfo = (PgFdwRelationInfo *)input_rel->fdw_private;
	PgFdwRelationInfo *fpinfo = (PgFdwRelationInfo *)final_rel->fdw_private;
	bool has_final_sort = false;
	List *pathkeys = NIL;
	PgFdwPathExtraData *fpextra;
	bool save_use_remote_estimate = false;
	double rows;
	int width;
	Cost startup_cost;
	Cost total_cost;
	List *fdw_private;
	ForeignPath *final_path;

	/*
	 * Currently, we only support this for SELECT commands
	 */
	if (parse->commandType != CMD_SELECT)
		return;

	/*
	 * No work if there is no FOR UPDATE/SHARE clause and if there is no need
	 * to add a LIMIT node
	 */
	if (!parse->rowMarks && !extra->limit_needed)
		return;

	/* We don't support cases where there are any SRFs in the targetlist */
	if (parse->hasTargetSRFs)
		return;

	/* Save the input_rel as outerrel in fpinfo */
	fpinfo->outerrel = input_rel;

	/*
	 * Copy foreign table, foreign server, user mapping, FDW options etc.
	 * details from the input relation's fpinfo.
	 */
	fpinfo->table = ifpinfo->table;
	fpinfo->server = ifpinfo->server;
	fpinfo->user = ifpinfo->user;
	merge_fdw_options(fpinfo, ifpinfo, NULL);

	/*
	 * If there is no need to add a LIMIT node, there might be a ForeignPath
	 * in the input_rel's pathlist that implements all behavior of the query.
	 * Note: we would already have accounted for the query's FOR UPDATE/SHARE
	 * (if any) before we get here.
	 */
	if (!extra->limit_needed) {
		ListCell *lc;

		Assert(parse->rowMarks);

		/*
		 * Grouping and aggregation are not supported with FOR UPDATE/SHARE,
		 * so the input_rel should be a base, join, or ordered relation; and
		 * if it's an ordered relation, its input relation should be a base or
		 * join relation.
		 */
		Assert(input_rel->reloptkind == RELOPT_BASEREL ||
			   input_rel->reloptkind == RELOPT_JOINREL ||
			   (input_rel->reloptkind == RELOPT_UPPER_REL &&
				   ifpinfo->stage == UPPERREL_ORDERED &&
				   (ifpinfo->outerrel->reloptkind == RELOPT_BASEREL ||
					   ifpinfo->outerrel->reloptkind == RELOPT_JOINREL)));

		foreach (lc, input_rel->pathlist) {
			Path *path = (Path *)lfirst(lc);

			/*
			 * apply_scanjoin_target_to_paths() uses create_projection_path()
			 * to adjust each of its input paths if needed, whereas
			 * create_ordered_paths() uses apply_projection_to_path() to do
			 * that.  So the former might have put a ProjectionPath on top of
			 * the ForeignPath; look through ProjectionPath and see if the
			 * path underneath it is ForeignPath.
			 */
			if (IsA(path, ForeignPath) ||
				(IsA(path, ProjectionPath) &&
					IsA(((ProjectionPath *)path)->subpath, ForeignPath))) {
				/*
				 * Create foreign final path; this gets rid of a
				 * no-longer-needed outer plan (if any), which makes the
				 * EXPLAIN output look cleaner
				 */
				final_path = create_foreign_upper_path(root,
					path->parent,
					path->pathtarget,
					path->rows,
					path->startup_cost,
					path->total_cost,
					path->pathkeys,
					NULL,  /* no extra plan */
					NULL); /* no fdw_private */

				/* and add it to the final_rel */
				add_path(final_rel, (Path *)final_path);

				/* Safe to push down */
				fpinfo->pushdown_safe = true;

				return;
			}
		}

		/*
		 * If we get here it means no ForeignPaths; since we would already
		 * have considered pushing down all operations for the query to the
		 * remote server, give up on it.
		 */
		return;
	}

	Assert(extra->limit_needed);

	/*
	 * If the input_rel is an ordered relation, replace the input_rel with its
	 * input relation
	 */
	if (input_rel->reloptkind == RELOPT_UPPER_REL &&
		ifpinfo->stage == UPPERREL_ORDERED) {
		input_rel = ifpinfo->outerrel;
		ifpinfo = (PgFdwRelationInfo *)input_rel->fdw_private;
		has_final_sort = true;
		pathkeys = root->sort_pathkeys;
	}

	/* The input_rel should be a base, join, or grouping relation */
	Assert(input_rel->reloptkind == RELOPT_BASEREL ||
		   input_rel->reloptkind == RELOPT_JOINREL ||
		   (input_rel->reloptkind == RELOPT_UPPER_REL &&
			   ifpinfo->stage == UPPERREL_GROUP_AGG));

	/*
	 * We try to create a path below by extending a simple foreign path for
	 * the underlying base, join, or grouping relation to perform the final
	 * sort (if has_final_sort) and the LIMIT restriction remotely, which is
	 * stored into the fdw_private list of the resulting path.  (We
	 * re-estimate the costs of sorting the underlying relation, if
	 * has_final_sort.)
	 */

	/*
	 * Assess if it is safe to push down the LIMIT and OFFSET to the remote
	 * server
	 */

	/*
	 * If the underlying relation has any local conditions, the LIMIT/OFFSET
	 * cannot be pushed down.
	 */
	if (ifpinfo->local_conds)
		return;

	/*
	 * Also, the LIMIT/OFFSET cannot be pushed down, if their expressions are
	 * not safe to remote.
	 */
	if (!is_foreign_expr(root, input_rel, (Expr *)parse->limitOffset) ||
		!is_foreign_expr(root, input_rel, (Expr *)parse->limitCount))
		return;

	/* Safe to push down */
	fpinfo->pushdown_safe = true;

	/* Construct PgFdwPathExtraData */
	fpextra = (PgFdwPathExtraData *)palloc0(sizeof(PgFdwPathExtraData));
	fpextra->target = root->upper_targets[UPPERREL_FINAL];
	fpextra->has_final_sort = has_final_sort;
	fpextra->has_limit = extra->limit_needed;
	fpextra->limit_tuples = extra->limit_tuples;
	fpextra->count_est = extra->count_est;
	fpextra->offset_est = extra->offset_est;

	/*
	 * Estimate the costs of performing the final sort and the LIMIT
	 * restriction remotely.  If has_final_sort is false, we wouldn't need to
	 * execute EXPLAIN anymore if use_remote_estimate, since the costs can be
	 * roughly estimated using the costs we already have for the underlying
	 * relation, in the same way as when use_remote_estimate is false.  Since
	 * it's pretty expensive to execute EXPLAIN, force use_remote_estimate to
	 * false in that case.
	 */
	if (!fpextra->has_final_sort) {
		save_use_remote_estimate = ifpinfo->use_remote_estimate;
		ifpinfo->use_remote_estimate = false;
	}
	estimate_path_cost_size(root, input_rel, NIL, pathkeys, fpextra,
		&rows, &width, &startup_cost, &total_cost);
	if (!fpextra->has_final_sort)
		ifpinfo->use_remote_estimate = save_use_remote_estimate;

	/*
	 * Build the fdw_private list that will be used by postgresGetForeignPlan.
	 * Items in the list must match order in enum FdwPathPrivateIndex.
	 */
	fdw_private = list_make2(makeBoolean(has_final_sort),
		makeBoolean(extra->limit_needed));

	/*
	 * Create foreign final path; this gets rid of a no-longer-needed outer
	 * plan (if any), which makes the EXPLAIN output look cleaner
	 */
	final_path = create_foreign_upper_path(root,
		input_rel,
		root->upper_targets[UPPERREL_FINAL],
		rows,
		startup_cost,
		total_cost,
		pathkeys,
		NULL, /* no extra plan */
		fdw_private);

	/* and add it to the final_rel */
	add_path(final_rel, (Path *)final_path);
}

/*
 * Create a tuple from the specified row of the PGresult.
 *
 * rel is the local representation of the foreign table, attinmeta is
 * conversion data for the rel's tupdesc, and retrieved_attrs is an
 * integer list of the table column numbers present in the PGresult.
 * fsstate is the ForeignScan plan node's execution state.
 * temp_context is a working context that can be reset after each tuple.
 *
 * Note: either rel or fsstate, but not both, can be NULL.  rel is NULL
 * if we're processing a remote join, while fsstate is NULL in a non-query
 * context such as ANALYZE, or if we're processing a non-scan query node.
 */
static HeapTuple
make_tuple_from_agg(xrg_agg_t *agg,
	int row,
	Relation rel,
	AttInMetadata *attinmeta,
	List *retrieved_attrs,
	ForeignScanState *fsstate,
	MemoryContext temp_context) {
	HeapTuple tuple;
	TupleDesc tupdesc;
	Datum *values;
	bool *nulls;
	ItemPointer ctid = NULL;
	ConversionLocation errpos;
	ErrorContextCallback errcallback;
	MemoryContext oldcontext;
	ListCell *lc;

	/*
	 * Do the following work in a temp context that we reset after each tuple.
	 * This cleans up not only the data we have direct access to, but any
	 * cruft the I/O functions might leak.
	 */
	oldcontext = MemoryContextSwitchTo(temp_context);

	/*
	 * Get the tuple descriptor for the row.  Use the rel's tupdesc if rel is
	 * provided, otherwise look to the scan node's ScanTupleSlot.
	 */
	if (rel)
		tupdesc = RelationGetDescr(rel);
	else {
		Assert(fsstate);
		tupdesc = fsstate->ss.ss_ScanTupleSlot->tts_tupleDescriptor;
	}

	values = (Datum *)palloc0(tupdesc->natts * sizeof(Datum));
	nulls = (bool *)palloc(tupdesc->natts * sizeof(bool));
	/* Initialize to nulls for any columns not present in result */
	memset(nulls, true, tupdesc->natts * sizeof(bool));

	/*
	 * Set up and install callback to report where conversion error occurs.
	 */
#if 0
	errpos.cur_attno = 0;
	errpos.rel = rel;
	errpos.fsstate = fsstate;
	errcallback.callback = conversion_error_callback;
	errcallback.arg = (void *) &errpos;
	errcallback.previous = error_context_stack;
	error_context_stack = &errcallback;
#endif

	if (xrg_agg_get_next(agg, attinmeta, values, nulls, tupdesc->natts) != 0) {
		MemoryContextSwitchTo(oldcontext);
		MemoryContextReset(temp_context);
		return 0;
	}

#if 0
	/* Uninstall error context callback. */
	error_context_stack = errcallback.previous;

#endif

	/*
	 * Build the result tuple in caller's memory context.
	 */
	MemoryContextSwitchTo(oldcontext);

	tuple = heap_form_tuple(tupdesc, values, nulls);

	/*
	 * If we have a CTID to return, install it in both t_self and t_ctid.
	 * t_self is the normal place, but if the tuple is converted to a
	 * composite Datum, t_self will be lost; setting t_ctid allows CTID to be
	 * preserved during EvalPlanQual re-evaluations (see ROW_MARK_COPY code).
	 */
	if (ctid)
		tuple->t_self = tuple->t_data->t_ctid = *ctid;

	/*
	 * Stomp on the xmin, xmax, and cmin fields from the tuple created by
	 * heap_form_tuple.  heap_form_tuple actually creates the tuple with
	 * DatumTupleFields, not HeapTupleFields, but the executor expects
	 * HeapTupleFields and will happily extract system columns on that
	 * assumption.  If we don't do this then, for example, the tuple length
	 * ends up in the xmin field, which isn't what we want.
	 */
	HeapTupleHeaderSetXmax(tuple->t_data, InvalidTransactionId);
	HeapTupleHeaderSetXmin(tuple->t_data, InvalidTransactionId);
	HeapTupleHeaderSetCmin(tuple->t_data, InvalidTransactionId);

	/* Clean up */
	MemoryContextReset(temp_context);

	return tuple;
}

static HeapTuple
make_tuple_from_result_row(xrg_iter_t *iter,
	int row,
	Relation rel,
	AttInMetadata *attinmeta,
	List *retrieved_attrs,
	ForeignScanState *fsstate,
	MemoryContext temp_context) {
	HeapTuple tuple;
	TupleDesc tupdesc;
	Datum *values;
	bool *nulls;
	ItemPointer ctid = NULL;
	ConversionLocation errpos;
	ErrorContextCallback errcallback;
	MemoryContext oldcontext;
	ListCell *lc;
	int j = 0;

	/*
	 * Do the following work in a temp context that we reset after each tuple.
	 * This cleans up not only the data we have direct access to, but any
	 * cruft the I/O functions might leak.
	 */
	oldcontext = MemoryContextSwitchTo(temp_context);

	/*
	 * Get the tuple descriptor for the row.  Use the rel's tupdesc if rel is
	 * provided, otherwise look to the scan node's ScanTupleSlot.
	 */
	if (rel)
		tupdesc = RelationGetDescr(rel);
	else {
		Assert(fsstate);
		tupdesc = fsstate->ss.ss_ScanTupleSlot->tts_tupleDescriptor;
	}

	values = (Datum *)palloc0(tupdesc->natts * sizeof(Datum));
	nulls = (bool *)palloc(tupdesc->natts * sizeof(bool));
	/* Initialize to nulls for any columns not present in result */
	memset(nulls, true, tupdesc->natts * sizeof(bool));

	/*
	 * Set up and install callback to report where conversion error occurs.
	 */
	errpos.cur_attno = 0;
	errpos.rel = rel;
	errpos.fsstate = fsstate;
	errcallback.callback = conversion_error_callback;
	errcallback.arg = (void *)&errpos;
	errcallback.previous = error_context_stack;
	error_context_stack = &errcallback;

	j = 0;
	foreach (lc, retrieved_attrs) {
		int i = lfirst_int(lc);
		var_decode(iter->value[j], *iter->flag[j], &iter->attr[j], attinmeta->atttypmods[i - 1],
			&values[i - 1], &nulls[i - 1]);
		j++;
	}

	/* Uninstall error context callback. */
	error_context_stack = errcallback.previous;

	/*
	 * Build the result tuple in caller's memory context.
	 */
	MemoryContextSwitchTo(oldcontext);

	tuple = heap_form_tuple(tupdesc, values, nulls);

	/*
	 * If we have a CTID to return, install it in both t_self and t_ctid.
	 * t_self is the normal place, but if the tuple is converted to a
	 * composite Datum, t_self will be lost; setting t_ctid allows CTID to be
	 * preserved during EvalPlanQual re-evaluations (see ROW_MARK_COPY code).
	 */
	if (ctid)
		tuple->t_self = tuple->t_data->t_ctid = *ctid;

	/*
	 * Stomp on the xmin, xmax, and cmin fields from the tuple created by
	 * heap_form_tuple.  heap_form_tuple actually creates the tuple with
	 * DatumTupleFields, not HeapTupleFields, but the executor expects
	 * HeapTupleFields and will happily extract system columns on that
	 * assumption.  If we don't do this then, for example, the tuple length
	 * ends up in the xmin field, which isn't what we want.
	 */
	HeapTupleHeaderSetXmax(tuple->t_data, InvalidTransactionId);
	HeapTupleHeaderSetXmin(tuple->t_data, InvalidTransactionId);
	HeapTupleHeaderSetCmin(tuple->t_data, InvalidTransactionId);

	/* Clean up */
	MemoryContextReset(temp_context);

	return tuple;
}

/*
 * Callback function which is called when error occurs during column value
 * conversion.  Print names of column and relation.
 *
 * Note that this function mustn't do any catalog lookups, since we are in
 * an already-failed transaction.  Fortunately, we can get the needed info
 * from the relation or the query's rangetable instead.
 */
static void
conversion_error_callback(void *arg) {
	ConversionLocation *errpos = (ConversionLocation *)arg;
	Relation rel = errpos->rel;
	ForeignScanState *fsstate = errpos->fsstate;
	const char *attname = NULL;
	const char *relname = NULL;
	bool is_wholerow = false;

	/*
	 * If we're in a scan node, always use aliases from the rangetable, for
	 * consistency between the simple-relation and remote-join cases.  Look at
	 * the relation's tupdesc only if we're not in a scan node.
	 */
	if (fsstate) {
		/* ForeignScan case */
		ForeignScan *fsplan = castNode(ForeignScan, fsstate->ss.ps.plan);
		int varno = 0;
		AttrNumber colno = 0;

		if (fsplan->scan.scanrelid > 0) {
			/* error occurred in a scan against a foreign table */
			varno = fsplan->scan.scanrelid;
			colno = errpos->cur_attno;
		} else {
			/* error occurred in a scan against a foreign join */
			TargetEntry *tle;

			tle = list_nth_node(TargetEntry, fsplan->fdw_scan_tlist,
				errpos->cur_attno - 1);

			/*
			 * Target list can have Vars and expressions.  For Vars, we can
			 * get some information, however for expressions we can't.  Thus
			 * for expressions, just show generic context message.
			 */
			if (IsA(tle->expr, Var)) {
				Var *var = (Var *)tle->expr;

				varno = var->varno;
				colno = var->varattno;
			}
		}

		if (varno > 0) {
			EState *estate = fsstate->ss.ps.state;
			RangeTblEntry *rte = exec_rt_fetch(varno, estate);

			relname = rte->eref->aliasname;

			if (colno == 0)
				is_wholerow = true;
			else if (colno > 0 && colno <= list_length(rte->eref->colnames))
				attname = strVal(list_nth(rte->eref->colnames, colno - 1));
			else if (colno == SelfItemPointerAttributeNumber)
				attname = "ctid";
		}
	} else if (rel) {
		/* Non-ForeignScan case (we should always have a rel here) */
		TupleDesc tupdesc = RelationGetDescr(rel);

		relname = RelationGetRelationName(rel);
		if (errpos->cur_attno > 0 && errpos->cur_attno <= tupdesc->natts) {
			Form_pg_attribute attr = TupleDescAttr(tupdesc,
				errpos->cur_attno - 1);

			attname = NameStr(attr->attname);
		} else if (errpos->cur_attno == SelfItemPointerAttributeNumber)
			attname = "ctid";
	}

	if (relname && is_wholerow)
		errcontext("whole-row reference to foreign table \"%s\"", relname);
	else if (relname && attname)
		errcontext("column \"%s\" of foreign table \"%s\"", attname, relname);
	else
		errcontext("processing expression at position %d in select list",
			errpos->cur_attno);
}

/*
 * Given an EquivalenceClass and a foreign relation, find an EC member
 * that can be used to sort the relation remotely according to a pathkey
 * using this EC.
 *
 * If there is more than one suitable candidate, return an arbitrary
 * one of them.  If there is none, return NULL.
 *
 * This checks that the EC member expression uses only Vars from the given
 * rel and is shippable.  Caller must separately verify that the pathkey's
 * ordering operator is shippable.
 */
EquivalenceMember *
find_em_for_rel(PlannerInfo *root, EquivalenceClass *ec, RelOptInfo *rel) {
	ListCell *lc;

	foreach (lc, ec->ec_members) {
		EquivalenceMember *em = (EquivalenceMember *)lfirst(lc);

		/*
		 * Note we require !bms_is_empty, else we'd accept constant
		 * expressions which are not suitable for the purpose.
		 */
		if (bms_is_subset(em->em_relids, rel->relids) &&
			!bms_is_empty(em->em_relids) &&
			is_foreign_expr(root, rel, em->em_expr))
			return em;
	}

	return NULL;
}

/*
 * Find an EquivalenceClass member that is to be computed as a sort column
 * in the given rel's reltarget, and is shippable.
 *
 * If there is more than one suitable candidate, return an arbitrary
 * one of them.  If there is none, return NULL.
 *
 * This checks that the EC member expression uses only Vars from the given
 * rel and is shippable.  Caller must separately verify that the pathkey's
 * ordering operator is shippable.
 */
EquivalenceMember *
find_em_for_rel_target(PlannerInfo *root, EquivalenceClass *ec,
	RelOptInfo *rel) {
	PathTarget *target = rel->reltarget;
	ListCell *lc1;
	int i;

	i = 0;
	foreach (lc1, target->exprs) {
		Expr *expr = (Expr *)lfirst(lc1);
		Index sgref = get_pathtarget_sortgroupref(target, i);
		ListCell *lc2;

		/* Ignore non-sort expressions */
		if (sgref == 0 ||
			get_sortgroupref_clause_noerr(sgref,
				root->parse->sortClause) == NULL) {
			i++;
			continue;
		}

		/* We ignore binary-compatible relabeling on both ends */
		while (expr && IsA(expr, RelabelType))
			expr = ((RelabelType *)expr)->arg;

		/* Locate an EquivalenceClass member matching this expr, if any */
		foreach (lc2, ec->ec_members) {
			EquivalenceMember *em = (EquivalenceMember *)lfirst(lc2);
			Expr *em_expr;

			/* Don't match constants */
			if (em->em_is_const)
				continue;

			/* Ignore child members */
			if (em->em_is_child)
				continue;

			/* Match if same expression (after stripping relabel) */
			em_expr = em->em_expr;
			while (em_expr && IsA(em_expr, RelabelType))
				em_expr = ((RelabelType *)em_expr)->arg;

			if (!equal(em_expr, expr))
				continue;

			/* Check that expression (including relabels!) is shippable */
			if (is_foreign_expr(root, rel, em->em_expr))
				return em;
		}

		i++;
	}

	return NULL;
}

static bool kite_get_relation_stats(PgFdwRelationInfo *fpinfo, Relation relation, BlockNumber *totalpages, double *totalrows) {
	char errmsg[1024];
	ForeignTable *table;
	UserMapping *user;
	kite_request_t *req;
	StringInfoData sql, schema;
	List *retrieved_attrs, *aggfnoids;
	Datum datum;
	bool flag;
	xrg_agg_t *agg = 0;
	int64_t nrow = 0;

	retrieved_attrs = aggfnoids = NULL;

	/*
	 * Get the connection to use.  We do the remote access as the table's
	 * owner, even if the ANALYZE was started by some other user.
	 */
	table = GetForeignTable(RelationGetRelid(relation));
	user = GetUserMapping(relation->rd_rel->relowner, table->serverid);
	req = GetConnection(user, false, NULL);

	/*
	 * Construct command to get page count for relation.
	 */
	initStringInfo(&sql);
	deparseAnalyzeSizeSql(&sql, relation, &retrieved_attrs, &aggfnoids);

	//elog(LOG, "sql %s", sql.data);
	/*
         * build the schema for KITE
	 */
	{
		TupleDesc tupdesc = RelationGetDescr(relation);
		initStringInfo(&schema);
		kite_build_schema(&schema, tupdesc);
	}
	//elog(LOG, "scheam %s", schema.data);

	req->hdl = kite_submit(req->host, schema.data, sql.data, -1, fpinfo->fragcnt, errmsg, sizeof(errmsg));
	if (!req->hdl) {
		elog(ERROR, "kite_submit failed -- %s", errmsg);
		return false;
	}

	PG_TRY();
	{
		agg = xrg_agg_init(retrieved_attrs, aggfnoids, 0);

		xrg_agg_fetch(agg, req->hdl);

		if (xrg_agg_get_next(agg, 0, &datum, &flag, 1) != 0) {
			elog(ERROR, "kite: select count(*) return no result");
			return false;
		}
		nrow = DatumGetInt64(datum);
		*totalrows = (double)nrow;

		*totalpages = nrow / 200;
		if (*totalpages == 0) {
			*totalpages = 1;
		}

		elog(LOG, "Analyze totalrows = %ld, totalpages = %u", nrow, *totalpages);
	}
	PG_FINALLY();
	{
		if (agg) xrg_agg_destroy(agg);
		ReleaseConnection(req);
	}
	PG_END_TRY();

	return true;
}

/*
 * postgresAnalyzeForeignTable
 *		Test whether analyzing this foreign table is supported
 */
static bool
postgresAnalyzeForeignTable(Relation relation,
	AcquireSampleRowsFunc *func,
	BlockNumber *totalpages) {

	double totalrows = 0;

	/* Return the row-analysis function pointer */
	*func = postgresAcquireSampleRowsFunc;

	//return kite_get_relation_stats(relation, totalpages, &totalrows);
	// give fake number and get the stats in the later stage
	*totalpages = 100;

	return true;
}

/*
 * Acquire a random sample of rows from foreign table managed by postgres_fdw.
 *
 * We fetch the whole table from the remote side and pick out some sample rows.
 *
 * Selected rows are returned in the caller-allocated array rows[],
 * which must have at least targrows entries.
 * The actual number of rows selected is returned as the function result.
 * We also count the total number of rows in the table and return it into
 * *totalrows.  Note that *totaldeadrows is always set to 0.
 *
 * Note that the returned list of rows is not always in order by physical
 * position in the table.  Therefore, correlation estimates derived later
 * may be meaningless, but it's OK because we don't use the estimates
 * currently (the planner only pays attention to correlation for indexscans).
 */
static int
postgresAcquireSampleRowsFunc(Relation relation, int elevel,
	HeapTuple *rows, int targrows,
	double *totalrows,
	double *totaldeadrows) {

	PgFdwAnalyzeState astate;
	PgFdwRelationInfo *fpinfo;
	char errormsg[1024];
	ForeignTable *table;
	UserMapping *user;
	kite_request_t *req;
	StringInfoData sql, schema;
	List *retrieved_attrs;
	BlockNumber totalpages = 0;
	xrg_iter_t *iter = 0;
	int e = 0;
	MemoryContext oldcontext;

	astate.rel = relation;
	astate.attinmeta = TupleDescGetAttInMetadata(RelationGetDescr(relation));
	astate.rows = rows;
	astate.targrows = targrows;
	astate.numrows = 0;
	astate.samplerows = 0;

	fpinfo = (PgFdwRelationInfo *)palloc0(sizeof(PgFdwRelationInfo));
	/* Look up foreign-table catalog info. */
	fpinfo->table = GetForeignTable(relation->rd_id);
	fpinfo->server = GetForeignServer(fpinfo->table->serverid);

	fpinfo->fragcnt = 1;
	apply_server_options(fpinfo);
	apply_table_options(fpinfo);

	if (!kite_get_relation_stats(fpinfo, relation, &totalpages, &astate.samplerows)) {
		elog(ERROR, "kite_Get_relation_stats failed");
		return 0;
	}

	/* Remember ANALYZE context, and create a per-tuple temp context */
	astate.anl_cxt = CurrentMemoryContext;
	astate.temp_cxt = AllocSetContextCreate(CurrentMemoryContext,
		"postgres_fdw temporary data",
		ALLOCSET_SMALL_SIZES);

	/*
	 * Get the connection to use.  We do the remote access as the table's
	 * owner, even if the ANALYZE was started by some other user.
	 */
	table = GetForeignTable(RelationGetRelid(relation));
	user = GetUserMapping(relation->rd_rel->relowner, table->serverid);
	req = GetConnection(user, false, NULL);

	/*
	 * Construct command to get page count for relation.
	 */
	retrieved_attrs = NULL;
	initStringInfo(&sql);
	deparseAnalyzeSql(&sql, relation, &retrieved_attrs, targrows, *totalrows);

	/*
         * build the schema for KITE
	 */
	{
		TupleDesc tupdesc = RelationGetDescr(relation);
		initStringInfo(&schema);
		kite_build_schema(&schema, tupdesc);
	}

	req->hdl = kite_submit(req->host, schema.data, sql.data, -1, fpinfo->fragcnt, errormsg, sizeof(errormsg));
	if (!req->hdl) {
		elog(ERROR, "kite_submit failed -- %s", errormsg);
		return 0;
	}

	PG_TRY();
	{

		oldcontext = MemoryContextSwitchTo(astate.anl_cxt);
		for (int i = 0; i < targrows; i++) {

			e = kite_next_row(req->hdl, &iter, errormsg, sizeof(errormsg));
			if (e == 0) {
				if (iter == 0) {
					break;
				}

				rows[i] = make_tuple_from_result_row(iter,
					i,
					astate.rel,
					astate.attinmeta,
					retrieved_attrs,
					NULL,
					astate.temp_cxt);

				astate.numrows++;
			} else {
				elog(ERROR, "kite_next_row failed");
			}
		}
		MemoryContextSwitchTo(oldcontext);

		elog(LOG, "Analyze sample received %d rows, targrows =%d", astate.numrows, astate.targrows);
	}
	PG_FINALLY();
	{
		ReleaseConnection(req);
	}
	PG_END_TRY();

	/* We assume that we have no dead tuple */
	*totaldeadrows = 0;

	/* We've retrieved total number of rows from foreign server */
	*totalrows = astate.samplerows;

	/*
	 * Emit some interesting relation info
	 */
	ereport(elevel,
		(errmsg("\"%s\": table contains %.0f rows, %d rows in sample",
			RelationGetRelationName(relation),
			astate.samplerows, astate.numrows)));

	return astate.numrows;
}
