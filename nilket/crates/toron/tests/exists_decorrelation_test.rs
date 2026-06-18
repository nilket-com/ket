use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, BooleanArray, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use toron::binder::{PlannerConfig, bind};
use toron::catalog::MockCatalog;
use toron::distributed::determinism::{SortPolicy, canonical_arrow_ipc_digest};
use toron::distributed::task::ArrowIpcDigest;
use toron::error::StorageError;
use toron::exec::Executor;
use toron::ir::{JoinKind, LogicalPlan, TableId};
use toron::sql_planner::SqlPlanner;
use toron::storage::{ScanRequest, ScanStream, StorageScan};
use wendelon::schema::{
	LocalityPolicy, PrimaryKeyDescriptor, RowIdentityPolicy, SchemaEvolutionPolicy, TableObjectId,
	TableSchemaDescriptor,
};

#[test]
fn sql_correlated_exists_lowers_to_left_semi_join() {
	let plan = plan_sql("SELECT * FROM fact WHERE EXISTS (SELECT 1 FROM dim WHERE dim.k = fact.k)");

	let LogicalPlan::Join {
		kind,
		left,
		right,
		on,
		..
	} = plan
	else {
		panic!("expected join plan");
	};
	assert_eq!(kind, JoinKind::LeftSemi);
	assert_eq!(on.len(), 1);
	assert!(matches!(*left, LogicalPlan::Exchange { .. }));
	assert!(matches!(*right, LogicalPlan::Exchange { .. }));
}

#[test]
fn sql_correlated_not_exists_lowers_to_left_anti_join() {
	let plan =
		plan_sql("SELECT * FROM fact WHERE NOT EXISTS (SELECT 1 FROM dim WHERE dim.k = fact.k)");

	let LogicalPlan::Join { kind, on, .. } = plan else {
		panic!("expected join plan");
	};
	assert_eq!(kind, JoinKind::LeftAnti);
	assert_eq!(on.len(), 1);
}

#[tokio::test]
async fn exists_digest_matches_hand_written_left_semi_join() {
	assert_same_digest(
		"SELECT * FROM fact WHERE EXISTS (SELECT 1 FROM dim WHERE dim.k = fact.k)",
		"SELECT * FROM fact SEMI JOIN dim ON dim.k = fact.k",
	)
	.await;
}

#[tokio::test]
async fn not_exists_digest_matches_hand_written_left_anti_join() {
	assert_same_digest(
		"SELECT * FROM fact WHERE NOT EXISTS (SELECT 1 FROM dim WHERE dim.k = fact.k)",
		"SELECT * FROM fact ANTI JOIN dim ON dim.k = fact.k",
	)
	.await;
}

#[tokio::test]
async fn exists_with_inner_only_residual_pushes_filter_on_subquery_side() {
	let plan = plan_sql(
		"SELECT * FROM fact WHERE EXISTS \
		 (SELECT 1 FROM dim WHERE dim.k = fact.k AND dim.active = true)",
	);
	let LogicalPlan::Join { right, .. } = &plan else {
		panic!("expected join plan");
	};
	let LogicalPlan::Exchange { input, .. } = right.as_ref() else {
		panic!("expected right exchange");
	};
	assert!(matches!(input.as_ref(), LogicalPlan::Filter { .. }));

	let batch = run_sql(
		"SELECT * FROM fact WHERE EXISTS \
		 (SELECT 1 FROM dim WHERE dim.k = fact.k AND dim.active = true)",
	)
	.await;
	assert_pk_values(&batch, &[1, 3]);
}

#[tokio::test]
async fn exists_with_outer_residual_preserves_filter_semantics() {
	assert_same_digest(
		"SELECT * FROM fact WHERE fact.flag = true \
		 AND EXISTS (SELECT 1 FROM dim WHERE dim.k = fact.k)",
		"SELECT * FROM fact SEMI JOIN dim ON dim.k = fact.k WHERE fact.flag = true",
	)
	.await;
}

#[test]
fn multiple_exists_conjuncts_lower_to_left_deep_semi_anti_chain() {
	let plan = plan_sql(
		"SELECT * FROM fact \
		 WHERE EXISTS (SELECT 1 FROM dim WHERE dim.k = fact.k) \
		 AND NOT EXISTS (SELECT 1 FROM deny WHERE deny.k = fact.k)",
	);

	let LogicalPlan::Join {
		kind: JoinKind::LeftAnti,
		left,
		..
	} = plan
	else {
		panic!("expected outer anti join");
	};
	let LogicalPlan::Exchange { input, .. } = left.as_ref() else {
		panic!("expected left exchange");
	};
	assert!(matches!(
		input.as_ref(),
		LogicalPlan::Join {
			kind: JoinKind::LeftSemi,
			..
		}
	));
}

#[tokio::test]
async fn multiple_exists_conjuncts_result_independent_of_chain_order() {
	assert_same_digest(
		"SELECT * FROM fact \
		 WHERE EXISTS (SELECT 1 FROM dim WHERE dim.k = fact.k) \
		 AND NOT EXISTS (SELECT 1 FROM deny WHERE deny.k = fact.k)",
		"SELECT * FROM fact \
		 WHERE NOT EXISTS (SELECT 1 FROM deny WHERE deny.k = fact.k) \
		 AND EXISTS (SELECT 1 FROM dim WHERE dim.k = fact.k)",
	)
	.await;
}

#[tokio::test]
async fn multiple_exists_subqueries_can_reuse_inner_aliases() {
	let batch = run_sql(
		"SELECT * FROM fact \
		 WHERE EXISTS (SELECT 1 FROM dim d WHERE d.k = fact.k) \
		 AND EXISTS (SELECT 1 FROM deny d WHERE d.k = fact.k)",
	)
	.await;

	assert_pk_values(&batch, &[3]);
}

#[tokio::test]
async fn exists_duplicate_inner_matches_emit_outer_once() {
	let batch =
		run_sql("SELECT * FROM fact WHERE EXISTS (SELECT 1 FROM dim WHERE dim.k = fact.k)").await;

	assert_pk_values(&batch, &[1, 3, 5]);
}

#[tokio::test]
async fn not_exists_null_correlation_key_preserves_anti_semantics() {
	let batch =
		run_sql("SELECT * FROM fact WHERE NOT EXISTS (SELECT 1 FROM dim WHERE dim.k = fact.k)")
			.await;

	assert_pk_values(&batch, &[2, 4, 6]);
}

#[tokio::test]
async fn exists_select_list_is_semantically_ignored_for_select_one_star_and_inner_column() {
	let one =
		digest_sql("SELECT * FROM fact WHERE EXISTS (SELECT 1 FROM dim WHERE dim.k = fact.k)")
			.await;
	let star =
		digest_sql("SELECT * FROM fact WHERE EXISTS (SELECT * FROM dim WHERE dim.k = fact.k)")
			.await;
	let column =
		digest_sql("SELECT * FROM fact WHERE EXISTS (SELECT dim.k FROM dim WHERE dim.k = fact.k)")
			.await;

	assert_eq!(one, star);
	assert_eq!(one, column);
}

#[test]
fn exists_with_zero_correlation_pairs_lowers_to_keyless_semi() {
	// ADR 256: an uncorrelated EXISTS (zero correlation pairs) now lowers to a keyless LeftSemi join.
	let plan = plan_sql("SELECT * FROM fact WHERE EXISTS (SELECT 1 FROM dim)");
	assert!(
		format!("{plan:?}").contains("LeftSemi"),
		"uncorrelated EXISTS lowers to a keyless LeftSemi: {plan:?}"
	);
}

// ===== ADR 259: correlated IN / NOT IN (membership-subquery decorrelation) =====

#[tokio::test]
async fn correlated_in_decorrelates_to_semi_join() {
	// fact.k = [1,2,3,NULL,4,5], dim.k = [1,1,3,4,NULL]. The correlated subquery for each fact row is
	// {dim.k : dim.k = fact.k}, so fact.pk qualifies iff fact.pk = fact.k AND that value is in dim.k.
	let batch =
		run_sql("SELECT * FROM fact WHERE fact.pk IN (SELECT dim.k FROM dim WHERE dim.k = fact.k)")
			.await;
	assert_pk_values(&batch, &[1, 3]);

	// The correlation changes the answer: uncorrelated, pk=4 (value 4 is in dim.k) is kept; correlated it
	// is dropped because its `k` is NULL → empty correlated subquery.
	let uncorrelated = run_sql("SELECT * FROM fact WHERE fact.pk IN (SELECT dim.k FROM dim)").await;
	assert_pk_values(&uncorrelated, &[1, 3, 4]);
}

#[test]
fn correlated_in_membership_and_correlation_pairs_both_on_join() {
	let plan = plan_sql(
		"SELECT * FROM fact WHERE fact.pk IN (SELECT dim.k FROM dim WHERE dim.k = fact.k)",
	);
	let LogicalPlan::Join { kind, on, .. } = plan else {
		panic!("expected join plan");
	};
	assert_eq!(kind, JoinKind::LeftSemi);
	assert_eq!(
		on.len(),
		2,
		"correlation pair (fact.k, dim.k) + membership pair (fact.pk, dim.k)"
	);
}

#[tokio::test]
async fn correlated_in_with_inner_residual_decorrelates() {
	// The residual `dim.active = false` (a Toron Filter on the inner) removes pk=3's only correlated dim
	// row (k=3 is active), so only pk=1 survives (k=1 has an inactive dim row, pk 11).
	let batch = run_sql(
		"SELECT * FROM fact WHERE fact.pk IN \
		 (SELECT dim.k FROM dim WHERE dim.k = fact.k AND dim.active = false)",
	)
	.await;
	assert_pk_values(&batch, &[1]);
}

#[tokio::test]
async fn correlated_not_in_decorrelates_to_anti_joins() {
	// The complement of `correlated_in_decorrelates_to_semi_join` among non-null-`x` rows (every fact.pk is
	// non-null). pk=4 is kept because its correlated subquery is empty (k is NULL). The null-guard is
	// vacuous here: an equi-correlated subquery can never project a NULL key.
	let batch = run_sql(
		"SELECT * FROM fact WHERE fact.pk NOT IN (SELECT dim.k FROM dim WHERE dim.k = fact.k)",
	)
	.await;
	assert_pk_values(&batch, &[2, 4, 5, 6]);

	// Plan-shape: null-`x` filter → keyed null-guard anti → membership anti (correlation + membership pairs).
	let plan = plan_sql(
		"SELECT * FROM fact WHERE fact.pk NOT IN (SELECT dim.k FROM dim WHERE dim.k = fact.k)",
	);
	let LogicalPlan::Join { kind, on, .. } = plan else {
		panic!("expected membership anti join");
	};
	assert_eq!(kind, JoinKind::LeftAnti);
	assert_eq!(
		on.len(),
		2,
		"membership anti keys on the correlation pair + (fact.pk, dim.k)"
	);
}

#[test]
fn uncorrelated_in_and_not_in_unchanged() {
	// The `correlation_pairs == []` path is byte-identical to 256/258.
	let LogicalPlan::Join { kind, on, .. } =
		plan_sql("SELECT * FROM fact WHERE fact.pk IN (SELECT dim.k FROM dim)")
	else {
		panic!("expected semi join");
	};
	assert_eq!(kind, JoinKind::LeftSemi);
	assert_eq!(
		on.len(),
		1,
		"uncorrelated IN stays a single-key semi-join (256)"
	);

	let LogicalPlan::Join { kind, on, left, .. } =
		plan_sql("SELECT * FROM fact WHERE fact.pk NOT IN (SELECT dim.k FROM dim)")
	else {
		panic!("expected membership anti join");
	};
	assert_eq!(kind, JoinKind::LeftAnti);
	assert_eq!(
		on.len(),
		1,
		"uncorrelated NOT IN membership stays single-key (258)"
	);
	let LogicalPlan::Exchange { input, .. } = *left else {
		panic!("expected exchange above the null-guard");
	};
	let LogicalPlan::Join { on: guard_on, .. } = *input else {
		panic!("expected the keyless null-guard anti");
	};
	assert!(
		guard_on.is_empty(),
		"uncorrelated null-guard stays keyless (258)"
	);
}

#[test]
fn non_equi_correlated_in_and_correlated_scalar_stay_rejected() {
	// Non-equi correlation isn't decorrelated (the classifier only spends equi-pairs).
	let err = SqlPlanner::new()
		.plan(
			"SELECT * FROM fact WHERE fact.pk IN \
			 (SELECT dim.k FROM dim WHERE dim.score > fact.pk)",
		)
		.expect_err("non-equi correlated IN should be unsupported");
	assert!(
		err.to_string()
			.contains("unsupported EXISTS subquery predicate"),
		"{err}"
	);

	// ADR 260 lowers a WHERE-correlated aggregate scalar; a correlation in the aggregate ARGUMENT is
	// undecorrelatable by GROUP BY and stays deferred (a separate mechanism).
	let err = SqlPlanner::new()
		.plan(
			"SELECT * FROM fact WHERE fact.pk > \
			 (SELECT MIN(dim.score + fact.pk) FROM dim WHERE dim.k = fact.k)",
		)
		.expect_err("aggregate-argument correlated scalar should be unsupported");
	assert!(
		err.to_string()
			.contains("correlated scalar subqueries are not supported"),
		"{err}"
	);
}

// ADR 260: a WHERE-correlated aggregate scalar decorrelates to a grouped aggregate (keyed on the
// correlation) recombined with a `LeftOuter`. `MAX(dim.score)` per `fact.k` is
// {k1: 20, k3: 30, k4: 40} (k=2,NULL,5 match no dim group → NULL).
#[tokio::test]
async fn correlated_aggregate_scalar_decorrelates_to_left_outer() {
	let batch = run_sql(
		"SELECT fact.pk FROM fact \
		 WHERE (SELECT MAX(dim.score) FROM dim WHERE dim.k = fact.k) > 25",
	)
	.await;
	assert_pk_values(&batch, &[3, 5]);

	// The correlation changes the answer: the uncorrelated global MAX is 50 (> 25 for every row).
	let uncorrelated =
		run_sql("SELECT fact.pk FROM fact WHERE (SELECT MAX(dim.score) FROM dim) > 25").await;
	assert_pk_values(&uncorrelated, &[1, 2, 3, 4, 5, 6]);
}

#[test]
fn correlated_scalar_recombines_as_keyed_left_outer() {
	let plan =
		plan_sql("SELECT fact.pk, (SELECT MAX(dim.score) FROM dim WHERE dim.k = fact.k) FROM fact");
	let LogicalPlan::Project { input, .. } = plan else {
		panic!("expected projection over the recombine");
	};
	let LogicalPlan::Join {
		kind, on, right, ..
	} = *input
	else {
		panic!("expected a join recombine");
	};
	assert_eq!(kind, JoinKind::LeftOuter);
	assert_eq!(on.len(), 1, "recombine keys on the single correlation pair");
	assert!(
		find_aggregate(&right),
		"the recombined right side is a grouped aggregate, not a keyless scalar Cross"
	);
}

// ADR 260 COUNT empty-group identity: COUNT over an empty correlated group is 0 (not NULL). pk 2, 4, 6
// match no dim group (k=2, NULL, 5), so COALESCE(value, 0) = 0 keeps them; a naive un-wrapped LeftOuter
// would yield NULL and drop all three.
#[tokio::test]
async fn correlated_count_scalar_empty_group_is_zero() {
	let batch = run_sql(
		"SELECT fact.pk FROM fact \
		 WHERE (SELECT COUNT(*) FROM dim WHERE dim.k = fact.k) = 0",
	)
	.await;
	assert_pk_values(&batch, &[2, 4, 6]);
}

// ADR 260: an inner residual (`dim.active = true`) filters the inner rows before aggregation, coexisting
// with the correlation key. It empties k=4's group (its only dim row is inactive), dropping pk=5 vs the
// no-residual {3, 5}.
#[tokio::test]
async fn correlated_aggregate_scalar_with_inner_residual() {
	let batch = run_sql(
		"SELECT fact.pk FROM fact \
		 WHERE (SELECT MAX(dim.score) FROM dim WHERE dim.k = fact.k AND dim.active = true) > 25",
	)
	.await;
	assert_pk_values(&batch, &[3]);
}

// ADR 260 regression: an uncorrelated global-aggregate scalar still folds via ADR 257's keyless `Cross`
// (the `correlation_pairs == []` path), not a `LeftOuter`.
#[test]
fn uncorrelated_global_aggregate_scalar_unchanged() {
	let plan = plan_sql("SELECT fact.pk, (SELECT MAX(dim.score) FROM dim) FROM fact");
	let LogicalPlan::Project { input, .. } = plan else {
		panic!("expected projection over the scalar Cross");
	};
	let LogicalPlan::Join { kind, on, .. } = *input else {
		panic!("expected a Cross join recombine");
	};
	assert_eq!(kind, JoinKind::Cross);
	assert!(on.is_empty(), "the uncorrelated scalar fold stays keyless");
}

#[test]
fn aggregate_arg_and_non_aggregate_correlated_scalar_stay_rejected() {
	// Correlation in the aggregate argument is undecorrelatable by GROUP BY.
	let err = SqlPlanner::new()
		.plan(
			"SELECT fact.pk FROM fact WHERE fact.pk > \
			 (SELECT MAX(dim.score + fact.pk) FROM dim WHERE dim.k = fact.k)",
		)
		.expect_err("aggregate-argument correlation should reject");
	assert!(
		err.to_string()
			.contains("correlated scalar subqueries are not supported"),
		"{err}"
	);

	// A non-aggregate correlated scalar needs a ≤1-row cardinality assert (a separate, deferred cut).
	let err = SqlPlanner::new()
		.plan(
			"SELECT fact.pk FROM fact WHERE fact.pk > \
			 (SELECT dim.score FROM dim WHERE dim.k = fact.k)",
		)
		.expect_err("non-aggregate correlated scalar should reject");
	assert!(
		err.to_string()
			.contains("non-aggregate scalar subqueries are not supported"),
		"{err}"
	);
}

// ADR 260: a scalar subquery pulled from WHERE appends its synthetic `__scalar_N` columns to the plan;
// a bare `SELECT *` would expose them (the frontend is schema-less and cannot project back). Reject
// rather than leak — an explicit projection list lowers correctly (see the execute tests above).
#[tokio::test]
async fn wildcard_over_where_scalar_rejects_rather_than_leaking() {
	let err = SqlPlanner::new()
		.plan(
			"SELECT * FROM fact \
			 WHERE (SELECT MAX(dim.score) FROM dim WHERE dim.k = fact.k) > 25",
		)
		.expect_err("SELECT * over a WHERE scalar should reject, not leak __scalar columns");
	assert!(
		err.to_string()
			.contains("SELECT * with a scalar subquery in WHERE is not supported"),
		"{err}"
	);

	// The explicit-projection form (which drops the synthetic columns via its Project) still works.
	let batch = run_sql(
		"SELECT fact.pk FROM fact \
		 WHERE (SELECT MAX(dim.score) FROM dim WHERE dim.k = fact.k) > 25",
	)
	.await;
	assert_eq!(
		batch.num_columns(),
		1,
		"explicit projection exposes only the listed column"
	);
	assert_pk_values(&batch, &[3, 5]);
}

#[test]
fn exists_with_outer_only_subquery_predicate_is_unsupported() {
	assert_unsupported(
		"SELECT * FROM fact WHERE EXISTS \
		 (SELECT 1 FROM dim WHERE fact.flag = true AND dim.k = fact.k)",
	);
}

#[test]
fn exists_with_unqualified_ambiguous_correlation_is_unsupported() {
	assert_unsupported("SELECT * FROM fact WHERE EXISTS (SELECT 1 FROM dim WHERE k = fact.k)");
	assert_unsupported(
		"SELECT * FROM fact WHERE EXISTS (SELECT 1 FROM fact WHERE fact.k = fact.k)",
	);

	let supported =
		plan_sql("SELECT * FROM fact f WHERE EXISTS (SELECT 1 FROM fact f2 WHERE f2.k = f.k)");
	assert!(matches!(
		supported,
		LogicalPlan::Join {
			kind: JoinKind::LeftSemi,
			..
		}
	));
}

#[test]
fn exists_unsupported_shapes_stay_unsupported() {
	for sql in [
		"SELECT * FROM fact WHERE fact.flag = true OR EXISTS \
		 (SELECT 1 FROM dim WHERE dim.k = fact.k)",
		"SELECT * FROM fact WHERE EXISTS (SELECT 1 FROM dim WHERE dim.k > fact.k)",
		"SELECT * FROM fact WHERE EXISTS (SELECT 1 FROM dim WHERE dim.k + 1 = fact.k)",
		"SELECT * FROM fact WHERE EXISTS (SELECT COUNT(*) FROM dim WHERE dim.k = fact.k)",
		// NB: `EXISTS (SELECT DISTINCT ...)` is INERT per ADR 234 (DISTINCT ignored, still decorrelates),
		// so it is intentionally NOT unsupported and is omitted here.
		"SELECT * FROM fact WHERE EXISTS (SELECT 1 FROM dim WHERE dim.k = fact.k LIMIT 1)",
		"SELECT * FROM fact WHERE EXISTS \
		 (SELECT 1 FROM dim WHERE dim.k = fact.k \
		  AND EXISTS (SELECT 1 FROM deny WHERE deny.k = dim.k))",
		"SELECT * FROM fact WHERE EXISTS \
		 (SELECT 1 FROM dim INNER JOIN deny ON dim.k = deny.k WHERE dim.k = fact.k)",
	] {
		assert_unsupported(sql);
	}
}

#[tokio::test]
async fn exists_above_explicit_from_join_preserves_scope_and_digest() {
	assert_same_digest(
		"SELECT * FROM fact INNER JOIN bridge ON fact.pk = bridge.fact_pk \
		 WHERE EXISTS (SELECT 1 FROM dim WHERE dim.k = fact.k)",
		"SELECT * FROM fact INNER JOIN bridge ON fact.pk = bridge.fact_pk \
		 SEMI JOIN dim ON dim.k = fact.k",
	)
	.await;
}

#[tokio::test]
async fn existing_sql_queries_byte_identical_after_lower_select_refactor() {
	assert_same_digest(
		"SELECT * FROM fact INNER JOIN dim ON fact.k = dim.k WHERE fact.flag = true",
		"SELECT * FROM fact INNER JOIN dim ON fact.k = dim.k WHERE fact.flag = true",
	)
	.await;
}

fn plan_sql(sql: &str) -> LogicalPlan {
	SqlPlanner::new().plan(sql).expect("plan sql")
}

/// Does the subtree contain a grouped `Aggregate` (the ADR 260 decorrelated recombine's right side)?
fn find_aggregate(plan: &LogicalPlan) -> bool {
	match plan {
		LogicalPlan::Aggregate { .. } => true,
		LogicalPlan::Alias { input, .. }
		| LogicalPlan::Exchange { input, .. }
		| LogicalPlan::Filter { input, .. }
		| LogicalPlan::Project { input, .. } => find_aggregate(input),
		LogicalPlan::Join { left, right, .. } => find_aggregate(left) || find_aggregate(right),
		_ => false,
	}
}

fn assert_unsupported(sql: &str) {
	let err = SqlPlanner::new()
		.plan(sql)
		.expect_err("query should be unsupported");
	assert!(err.to_string().contains("unsupported"), "{sql}: {err}");
}

async fn assert_same_digest(left_sql: &str, right_sql: &str) {
	assert_eq!(digest_sql(left_sql).await, digest_sql(right_sql).await);
}

async fn digest_sql(sql: &str) -> ArrowIpcDigest {
	let batch = run_sql(sql).await;
	canonical_arrow_ipc_digest(&[batch], SortPolicy::FullRowLex).expect("digest")
}

async fn run_sql(sql: &str) -> RecordBatch {
	let (catalog, storage) = fixture();
	let logical = SqlPlanner::new().plan(sql).expect("plan sql");
	let bound = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind sql");
	let executor = Executor::new(storage);
	let batches = executor.execute_bound(&bound, None).await.expect("execute");
	assert_eq!(batches.len(), 1);
	batches.into_iter().next().expect("batch")
}

fn assert_pk_values(batch: &RecordBatch, expected: &[i64]) {
	let values = batch
		.column(0)
		.as_any()
		.downcast_ref::<Int64Array>()
		.expect("pk column");
	let mut actual = Vec::with_capacity(values.len());
	for row in 0..values.len() {
		assert!(!values.is_null(row), "pk row {row} should not be null");
		actual.push(values.value(row));
	}
	actual.sort_unstable();

	let mut expected = expected.to_vec();
	expected.sort_unstable();
	assert_eq!(actual, expected);
}

fn fixture() -> (MockCatalog, Arc<FixtureScan>) {
	let fact_schema = fact_schema();
	let dim_schema = dim_schema();
	let deny_schema = deny_schema();
	let bridge_schema = bridge_schema();
	let mut catalog = MockCatalog::new();
	catalog
		.register_descriptor(TableId(1), descriptor(1, "fact", fact_schema.as_ref()))
		.expect("register fact");
	catalog
		.register_descriptor(TableId(2), descriptor(2, "dim", dim_schema.as_ref()))
		.expect("register dim");
	catalog
		.register_descriptor(TableId(3), descriptor(3, "deny", deny_schema.as_ref()))
		.expect("register deny");
	catalog
		.register_descriptor(TableId(4), descriptor(4, "bridge", bridge_schema.as_ref()))
		.expect("register bridge");
	let storage = Arc::new(FixtureScan {
		batches: HashMap::from([
			(TableId(1), fact_batch(fact_schema)),
			(TableId(2), dim_batch(dim_schema)),
			(TableId(3), deny_batch(deny_schema)),
			(TableId(4), bridge_batch(bridge_schema)),
		]),
	});
	(catalog, storage)
}

fn fact_schema() -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new("pk", DataType::Int64, false),
		Field::new("k", DataType::Int64, true),
		Field::new("flag", DataType::Boolean, false),
		Field::new("label", DataType::Utf8, true),
	]))
}

fn dim_schema() -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new("pk", DataType::Int64, false),
		Field::new("k", DataType::Int64, true),
		Field::new("active", DataType::Boolean, false),
		Field::new("score", DataType::Int64, false),
	]))
}

fn deny_schema() -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new("pk", DataType::Int64, false),
		Field::new("k", DataType::Int64, true),
	]))
}

fn bridge_schema() -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new("pk", DataType::Int64, false),
		Field::new("fact_pk", DataType::Int64, false),
	]))
}

fn fact_batch(schema: SchemaRef) -> RecordBatch {
	RecordBatch::try_new(
		schema,
		vec![
			Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6])),
			Arc::new(Int64Array::from(vec![
				Some(1),
				Some(2),
				Some(3),
				None,
				Some(4),
				Some(5),
			])),
			Arc::new(BooleanArray::from(vec![
				true, false, true, true, false, true,
			])),
			Arc::new(StringArray::from(vec![
				Some("f1"),
				Some("f2"),
				Some("f3"),
				Some("fnull"),
				Some("f4"),
				Some("f5"),
			])),
		],
	)
	.expect("fact batch")
}

fn dim_batch(schema: SchemaRef) -> RecordBatch {
	RecordBatch::try_new(
		schema,
		vec![
			Arc::new(Int64Array::from(vec![10, 11, 12, 13, 14])),
			Arc::new(Int64Array::from(vec![
				Some(1),
				Some(1),
				Some(3),
				Some(4),
				None,
			])),
			Arc::new(BooleanArray::from(vec![true, false, true, false, true])),
			Arc::new(Int64Array::from(vec![20, 5, 30, 40, 50])),
		],
	)
	.expect("dim batch")
}

fn deny_batch(schema: SchemaRef) -> RecordBatch {
	RecordBatch::try_new(
		schema,
		vec![
			Arc::new(Int64Array::from(vec![20])),
			Arc::new(Int64Array::from(vec![Some(3)])),
		],
	)
	.expect("deny batch")
}

fn bridge_batch(schema: SchemaRef) -> RecordBatch {
	RecordBatch::try_new(
		schema,
		vec![
			Arc::new(Int64Array::from(vec![100, 101, 102, 103])),
			Arc::new(Int64Array::from(vec![1, 2, 3, 5])),
		],
	)
	.expect("bridge batch")
}

fn descriptor(id: u8, name: &str, schema: &Schema) -> TableSchemaDescriptor {
	TableSchemaDescriptor::new(
		TableObjectId::new([id; 16]),
		1,
		"default",
		name,
		schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("pk")),
		LocalityPolicy::default_table_spread(),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.expect("descriptor")
}

struct FixtureScan {
	batches: HashMap<TableId, RecordBatch>,
}

#[async_trait::async_trait]
impl StorageScan for FixtureScan {
	async fn scan(&self, request: ScanRequest) -> Result<ScanStream, StorageError> {
		let mut batch =
			self.batches
				.get(&request.table_id)
				.cloned()
				.ok_or(StorageError::TableNotFound {
					table: request.table_id,
				})?;

		if let Some(pbc) = request.pbc_predicate {
			batch = wendelon::schema::pbc::execute(&pbc, &batch).map_err(|error| {
				StorageError::InvalidPbcProgram {
					message: format!("{error:?}"),
				}
			})?;
		}

		if let Some(columns) = request.projected_columns {
			let indices = columns
				.into_iter()
				.map(|column| column.0 as usize)
				.collect::<Vec<_>>();
			batch = batch.project(&indices)?;
		}

		Ok(ScanStream::from_batches(
			batch.schema(),
			request.session_id,
			request.query_id,
			vec![batch],
		))
	}
}
