//! ADR 512 — reachable-key reduction for decorrelated scalar aggregates: the execution
//! equivalence battery. Every test runs the same SQL with the reduction off and on and requires
//! identical results; engaged shapes additionally pin the LeftSemi's presence (and its absence
//! with the knob off) at bind level. The fixture is built for the equivalence edge cases: null
//! correlation keys on both sides, duplicate keys, keys absent from the source, keys matched by
//! the outer but empty in the aggregate input, and a COUNT that rides the qualified path's
//! empty-group coalesce.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use toron::binder::{PlannerConfig, bind};
use toron::catalog::MockCatalog;
use toron::error::StorageError;
use toron::exec::Executor;
use toron::ir::{ExecutionPlan, JoinKind, TableId};
use toron::sql_planner::SqlPlanner;
use toron::storage::{ScanRequest, ScanStream, StorageScan};
use wendelon::schema::{
	LocalityPolicy, MultiRowLayoutDescriptor, RecordLayoutPolicy, RowIdentityPolicy,
	SchemaEvolutionPolicy, TableObjectId, TableSchemaDescriptor,
};

// -- differential battery: deferred (unqualified) path --------------------------------------

#[tokio::test]
async fn crossed_trace_avg_is_equivalent_and_engages() {
	// Correlation on the fact key; the source is reached by crossing the comma
	// equality into the filtered src side. Covers duplicate keys (fact pid 1),
	// a key matched by the outer but empty in the aggregate input (pid 2), and
	// null keys on every relation.
	assert_reduction_equivalent_and_engaged(
		"SELECT f_qty FROM fact, src \
		 WHERE f_pid = s_pid AND s_cat = 'X' \
		   AND f_qty < (SELECT AVG(g_qty) FROM gagg WHERE g_pid = f_pid)",
		true,
	)
	.await;
}

#[tokio::test]
async fn direct_trace_avg_is_equivalent_and_engages() {
	// Correlation on the src key; the source is the filtered src itself.
	assert_reduction_equivalent_and_engaged(
		"SELECT f_qty FROM fact, src \
		 WHERE f_pid = s_pid AND s_cat = 'X' \
		   AND f_qty < (SELECT AVG(g_qty) FROM gagg WHERE g_pid = s_pid)",
		true,
	)
	.await;
}

#[tokio::test]
async fn min_with_duplicate_and_null_keys_is_equivalent_and_engages() {
	assert_reduction_equivalent_and_engaged(
		"SELECT f_qty FROM fact, src \
		 WHERE f_pid = s_pid AND s_cat = 'X' \
		   AND f_qty < (SELECT MIN(g_qty) FROM gagg WHERE g_pid = f_pid)",
		true,
	)
	.await;
}

#[tokio::test]
async fn wrapped_aggregate_arithmetic_is_equivalent_and_engages() {
	assert_reduction_equivalent_and_engaged(
		"SELECT f_qty FROM fact, src \
		 WHERE f_pid = s_pid AND s_cat = 'X' \
		   AND f_qty < (SELECT 0.5 * MAX(g_qty) FROM gagg WHERE g_pid = f_pid)",
		true,
	)
	.await;
}

#[tokio::test]
async fn single_table_outer_with_pushed_filter_is_equivalent_and_engages() {
	// The outer is the filtered fact itself; its null-key row reaches the
	// recombine, keeps a NULL scalar, and is dropped by the comparison in both
	// arms. The source build here carries a null key too — plain semi equality
	// drops it, matching the recombine's plain equality.
	assert_reduction_equivalent_and_engaged(
		"SELECT f_qty FROM fact \
		 WHERE f_qty > 0 \
		   AND f_qty < (SELECT MAX(g_qty) FROM gagg WHERE g_pid = f_pid)",
		true,
	)
	.await;
}

#[tokio::test]
async fn unfiltered_outer_declines_and_is_equivalent() {
	// No deterministic filter anywhere on the traceable outer: the rule must
	// decline and both arms bind byte-identical plans.
	assert_reduction_equivalent_and_engaged(
		"SELECT f_qty FROM fact \
		 WHERE f_qty < (SELECT MAX(g_qty) FROM gagg WHERE g_pid = f_pid)",
		false,
	)
	.await;
}

// -- differential battery: qualified path ---------------------------------------------------

#[test]
fn qualified_count_empty_group_coalesce_is_equivalent_and_engages() {
	// COUNT rides only the qualified path (empty-group coalesce to 0). fs pid 2
	// is matched by the outer but has no gagg rows: the recombine misses, the
	// coalesce yields 0, and both arms agree. Deep graph scratch binds need the
	// explicit stack, as elsewhere in the suite.
	std::thread::Builder::new()
		.stack_size(64 * 1024 * 1024)
		.spawn(|| {
			let runtime = tokio::runtime::Builder::new_current_thread()
				.enable_all()
				.build()
				.expect("tokio runtime");
			runtime.block_on(async {
				assert_reduction_equivalent_and_engaged(
					"SELECT f_qty FROM fact INNER JOIN ( \
					   SELECT s_pid FROM src WHERE s_cat = 'X' \
					 ) AS fs ON fs.s_pid = f_pid \
					 WHERE f_qty > (SELECT COUNT(g.g_qty) FROM gagg AS g \
					                WHERE g.g_pid = fs.s_pid)",
					true,
				)
				.await;
			});
		})
		.expect("spawn qualified-count thread")
		.join()
		.expect("qualified-count thread panicked");
}

#[test]
fn qualified_avg_derived_source_is_equivalent_and_engages() {
	std::thread::Builder::new()
		.stack_size(64 * 1024 * 1024)
		.spawn(|| {
			let runtime = tokio::runtime::Builder::new_current_thread()
				.enable_all()
				.build()
				.expect("tokio runtime");
			runtime.block_on(async {
				assert_reduction_equivalent_and_engaged(
					"SELECT f_qty FROM fact INNER JOIN ( \
					   SELECT s_pid FROM src WHERE s_cat = 'X' \
					 ) AS fs ON fs.s_pid = f_pid \
					 WHERE f_qty < (SELECT AVG(g.g_qty) FROM gagg AS g \
					                WHERE g.g_pid = fs.s_pid)",
					true,
				)
				.await;
			});
		})
		.expect("spawn qualified-avg thread")
		.join()
		.expect("qualified-avg thread panicked");
}

// -- harness --------------------------------------------------------------------------------

async fn assert_reduction_equivalent_and_engaged(sql: &str, expect_engaged: bool) {
	let logical = SqlPlanner::new().plan(sql).expect("plan sql");
	let (catalog, storage) = fixture();

	let mut results = Vec::new();
	for reduction in [false, true] {
		let mut config = PlannerConfig::default();
		config.scalar_aggregate_input_reduction = reduction;
		let bound = bind(&logical, &catalog, &config).await.expect("bind sql");
		let semis = count_join_kind(&bound, JoinKind::LeftSemi);
		if reduction && expect_engaged {
			assert!(semis >= 1, "reduction on must insert a LeftSemi for {sql}");
		} else {
			assert_eq!(
				semis, 0,
				"no LeftSemi expected (reduction={reduction}) for {sql}"
			);
		}
		let executor = Executor::new(Arc::clone(&storage) as Arc<dyn StorageScan>);
		let batches = executor.execute_bound(&bound, None).await.expect("execute");
		results.push(collect_f64(&batches));
	}
	assert_eq!(
		results[0], results[1],
		"reduction must not change results for {sql}"
	);
}

fn collect_f64(batches: &[RecordBatch]) -> Vec<Option<u64>> {
	let mut values = Vec::new();
	for batch in batches {
		let column = batch
			.column(0)
			.as_any()
			.downcast_ref::<Float64Array>()
			.expect("f_qty column");
		for row in 0..column.len() {
			values.push(if column.is_null(row) {
				None
			} else {
				Some(column.value(row).to_bits())
			});
		}
	}
	values.sort_unstable();
	values
}

fn count_join_kind(plan: &ExecutionPlan, expected: JoinKind) -> usize {
	match plan {
		ExecutionPlan::Join {
			left, right, kind, ..
		} => {
			usize::from(*kind == expected)
				+ count_join_kind(left, expected)
				+ count_join_kind(right, expected)
		}
		ExecutionPlan::Project { input, .. }
		| ExecutionPlan::Filter { input, .. }
		| ExecutionPlan::Aggregate { input, .. }
		| ExecutionPlan::Exchange { input, .. }
		| ExecutionPlan::Sort { input, .. }
		| ExecutionPlan::Take { input, .. }
		| ExecutionPlan::Repeat { input, .. } => count_join_kind(input, expected),
		ExecutionPlan::Union { left, right, .. } => {
			count_join_kind(left, expected) + count_join_kind(right, expected)
		}
		_ => 0,
	}
}

fn fixture() -> (MockCatalog, Arc<FixtureScan>) {
	let fact = fact_schema();
	let src = src_schema();
	let gagg = gagg_schema();
	let mut catalog = MockCatalog::new();
	catalog
		.register_descriptor(TableId(1), descriptor(1, "fact", fact.as_ref()))
		.expect("register fact");
	catalog
		.register_descriptor(TableId(2), descriptor(2, "src", src.as_ref()))
		.expect("register src");
	catalog
		.register_descriptor(TableId(3), descriptor(3, "gagg", gagg.as_ref()))
		.expect("register gagg");
	let storage = Arc::new(FixtureScan {
		batches: HashMap::from([
			(TableId(1), fact_batch(fact)),
			(TableId(2), src_batch(src)),
			(TableId(3), gagg_batch(gagg)),
		]),
	});
	(catalog, storage)
}

fn fact_schema() -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new("f_pid", DataType::Int64, true),
		Field::new("f_qty", DataType::Float64, false),
	]))
}

fn src_schema() -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new("s_pid", DataType::Int64, true),
		Field::new("s_cat", DataType::Utf8, false),
	]))
}

fn gagg_schema() -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new("g_pid", DataType::Int64, true),
		Field::new("g_qty", DataType::Float64, false),
	]))
}

fn fact_batch(schema: SchemaRef) -> RecordBatch {
	// pid 1 duplicated; pid 2 matched by src but empty in gagg; pid 9 absent
	// from src; one null key.
	RecordBatch::try_new(
		schema,
		vec![
			Arc::new(Int64Array::from(vec![
				Some(1),
				Some(1),
				Some(2),
				None,
				Some(9),
			])),
			Arc::new(Float64Array::from(vec![10.0, 50.0, 200.0, 7.0, 5.0])),
		],
	)
	.expect("fact batch")
}

fn src_batch(schema: SchemaRef) -> RecordBatch {
	// pid 3 filtered out by s_cat; one null key inside the filtered category.
	RecordBatch::try_new(
		schema,
		vec![
			Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(3), None])),
			Arc::new(StringArray::from(vec!["X", "X", "Y", "X"])),
		],
	)
	.expect("src batch")
}

fn gagg_batch(schema: SchemaRef) -> RecordBatch {
	// pid 1 duplicated; pid 2 absent; pids 3 and 9 unreachable through the
	// filtered src; one null key.
	RecordBatch::try_new(
		schema,
		vec![
			Arc::new(Int64Array::from(vec![
				Some(1),
				Some(1),
				Some(3),
				Some(9),
				None,
			])),
			Arc::new(Float64Array::from(vec![10.0, 50.0, 30.0, 40.0, 70.0])),
		],
	)
	.expect("gagg batch")
}

fn descriptor(id: u8, name: &str, schema: &Schema) -> TableSchemaDescriptor {
	// AppendOnly (duplicate and null keys) requires a MultiRow layout.
	TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([id; 16]),
		1,
		"default",
		name,
		schema,
		RowIdentityPolicy::AppendOnly,
		LocalityPolicy::default_table_spread(),
		RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::default()),
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

// -- ADR 515: membership-first composition -------------------------------------------------

#[test]
fn membership_commute_is_equivalent_and_relocates_the_semi() {
	// Deep graph scratch binds on the commuted shape need the explicit stack,
	// per the suite convention.
	std::thread::Builder::new()
		.stack_size(64 * 1024 * 1024)
		.spawn(|| {
			let runtime = tokio::runtime::Builder::new_current_thread()
				.enable_all()
				.build()
				.expect("tokio runtime");
			runtime.block_on(membership_commute_is_equivalent_and_relocates_the_semi_body());
		})
		.expect("spawn membership_commute_is_equivalent_and_relocates_the_semi thread")
		.join()
		.expect("membership_commute_is_equivalent_and_relocates_the_semi thread panicked");
}

async fn membership_commute_is_equivalent_and_relocates_the_semi_body() {
	// Covers null keys (the src null row never matches the membership), keys
	// missing from the body (pid 2) or failing HAVING (pid 3), duplicate body
	// keys (gagg pid 1 twice), and duplicate-fanout atomicity (two fact rows
	// join the surviving src pid 1 and both live or die together).
	let sql = "SELECT f_qty FROM src, fact \
		 WHERE s_pid = f_pid \
		   AND s_pid IN (SELECT g_pid FROM gagg GROUP BY g_pid HAVING SUM(g_qty) > 35)";
	let logical = SqlPlanner::new().plan(sql).expect("plan membership sql");
	let (catalog, storage) = fixture();
	let mut results = Vec::new();
	for commute in [false, true] {
		let mut config = PlannerConfig::default();
		config.membership_first_composition = commute;
		let bound = bind(&logical, &catalog, &config).await.expect("bind");
		assert_eq!(
			count_join_kind(&bound, JoinKind::LeftSemi),
			1,
			"exactly one semi in both arms"
		);
		let root_is_semi = matches!(first_join_kind(&bound), Some(JoinKind::LeftSemi));
		if commute {
			assert!(!root_is_semi, "commute must relocate the semi off the root");
		} else {
			assert!(root_is_semi, "knob off keeps the root semi");
		}
		let executor = Executor::new(Arc::clone(&storage) as Arc<dyn StorageScan>);
		let batches = executor.execute_bound(&bound, None).await.expect("execute");
		results.push(collect_f64(&batches));
	}
	assert_eq!(results[0], results[1], "commute must not change results");
}

#[test]
fn membership_commute_onto_fanout_branch_is_equivalent() {
	// Deep graph scratch binds on the commuted shape need the explicit stack,
	// per the suite convention.
	std::thread::Builder::new()
		.stack_size(64 * 1024 * 1024)
		.spawn(|| {
			let runtime = tokio::runtime::Builder::new_current_thread()
				.enable_all()
				.build()
				.expect("tokio runtime");
			runtime.block_on(membership_commute_onto_fanout_branch_is_equivalent_body());
		})
		.expect("spawn membership_commute_onto_fanout_branch_is_equivalent thread")
		.join()
		.expect("membership_commute_onto_fanout_branch_is_equivalent thread panicked");
}

async fn membership_commute_onto_fanout_branch_is_equivalent_body() {
	// Membership keyed on the fanout branch itself: the semi commutes onto
	// fact, so filter-before-fanout applies to the duplicated side — the two
	// fact pid-1 rows must survive or die atomically, matching the composite
	// filter. (A joined membership body is unreachable from the SQL IN path —
	// the frontend rejects it — so the whitelist decline is defense-in-depth
	// exercised at recognizer level, not from SQL.)
	let sql = "SELECT f_qty FROM src, fact \
		 WHERE s_pid = f_pid \
		   AND f_pid IN (SELECT g_pid FROM gagg GROUP BY g_pid HAVING SUM(g_qty) > 35)";
	let logical = SqlPlanner::new()
		.plan(sql)
		.expect("plan fanout-key membership sql");
	let (catalog, storage) = fixture();
	let mut results = Vec::new();
	for commute in [false, true] {
		let mut config = PlannerConfig::default();
		config.membership_first_composition = commute;
		let bound = bind(&logical, &catalog, &config).await.expect("bind");
		assert_eq!(count_join_kind(&bound, JoinKind::LeftSemi), 1);
		let root_is_semi = matches!(first_join_kind(&bound), Some(JoinKind::LeftSemi));
		if commute {
			assert!(
				!root_is_semi,
				"commute must relocate onto the fanout branch"
			);
		} else {
			assert!(root_is_semi, "knob off keeps the root semi");
		}
		let executor = Executor::new(Arc::clone(&storage) as Arc<dyn StorageScan>);
		let batches = executor.execute_bound(&bound, None).await.expect("execute");
		results.push(collect_f64(&batches));
	}
	assert_eq!(results[0], results[1]);
}

fn first_join_kind(plan: &ExecutionPlan) -> Option<JoinKind> {
	match plan {
		ExecutionPlan::Join { kind, .. } => Some(*kind),
		ExecutionPlan::Project { input, .. }
		| ExecutionPlan::Filter { input, .. }
		| ExecutionPlan::Aggregate { input, .. }
		| ExecutionPlan::Exchange { input, .. }
		| ExecutionPlan::Sort { input, .. }
		| ExecutionPlan::Take { input, .. }
		| ExecutionPlan::Repeat { input, .. } => first_join_kind(input),
		_ => None,
	}
}
