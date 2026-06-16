//! ADR 241 — Window Functions Substrate behavior gates.
//!
//! Fixture `wins(id, grp, score)` with ties so RANK/DENSE_RANK are discriminating:
//! grp "a" scores [10, 10, 20] (a tie at 10); grp "b" scores [5, 5].

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use toron::binder::{PlannerConfig, bind};
use toron::catalog::MockCatalog;
use toron::error::StorageError;
use toron::exec::Executor;
use toron::ir::{LogicalPlan, TableId};
use toron::sql_planner::SqlPlanner;
use toron::storage::{ScanRequest, ScanStream, StorageScan};
use wendelon::schema::{
	LocalityPolicy, PrimaryKeyDescriptor, RowIdentityPolicy, SchemaEvolutionPolicy, TableObjectId,
	TableSchemaDescriptor,
};

// ---------------------------------------------------------------------------
// Behavior gates
// ---------------------------------------------------------------------------

#[tokio::test]
async fn row_number_restarts_and_orders_within_partition() {
	let rows = run_window(
		"SELECT grp, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY score) AS rn FROM wins",
	)
	.await;
	// Per partition the numbers restart at 1 and follow score order.
	assert_eq!(group_values(&rows, "a"), vec![1, 2, 3]);
	assert_eq!(group_values(&rows, "b"), vec![1, 2]);
}

#[tokio::test]
async fn rank_and_dense_rank_differ_on_ties() {
	let ranks =
		run_window("SELECT grp, RANK() OVER (PARTITION BY grp ORDER BY score) AS r FROM wins")
			.await;
	// grp "a" has a tie at score 10 -> RANK 1,1,3 (gap).
	assert_eq!(group_values(&ranks, "a"), vec![1, 1, 3]);
	assert_eq!(group_values(&ranks, "b"), vec![1, 1]);

	let dense = run_window(
		"SELECT grp, DENSE_RANK() OVER (PARTITION BY grp ORDER BY score) AS dr FROM wins",
	)
	.await;
	// DENSE_RANK 1,1,2 (no gap) — the discriminator vs RANK.
	assert_eq!(group_values(&dense, "a"), vec![1, 1, 2]);
	assert_eq!(group_values(&dense, "b"), vec![1, 1]);
}

#[tokio::test]
async fn window_preserves_input_row_count() {
	// 5 input rows -> 5 output rows (the anti-Aggregate property).
	let rows = run_window(
		"SELECT grp, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY score) AS rn FROM wins",
	)
	.await;
	assert_eq!(rows.len(), 5);
}

#[tokio::test]
async fn empty_partition_by_orders_whole_input() {
	// OVER (ORDER BY score) with no PARTITION BY ranks all rows as one partition: scores
	// [5,5,10,10,20] -> RANK 1,1,3,3,5.
	let ranks = run_window("SELECT grp, RANK() OVER (ORDER BY score) AS r FROM wins").await;
	let mut all = ranks.iter().map(|(_, value)| *value).collect::<Vec<_>>();
	all.sort_unstable();
	assert_eq!(all, vec![1, 1, 3, 3, 5]);
}

#[tokio::test]
async fn window_evaluated_after_where() {
	// WHERE filters before the window: only score >= 10 survives, so grp "a" keeps {10,10,20} and
	// grp "b" (scores 5,5) is filtered out entirely.
	let rows = run_window(
		"SELECT grp, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY score) AS rn \
		 FROM wins WHERE score >= 10",
	)
	.await;
	assert_eq!(group_values(&rows, "a"), vec![1, 2, 3]);
	assert!(rows.iter().all(|(grp, _)| grp.as_deref() == Some("a")));
}

#[tokio::test]
async fn shared_over_spec_uses_one_window_node() {
	// Two functions over the same OVER spec collapse to a single Window node with two functions.
	let plan = SqlPlanner::new()
		.plan(
			"SELECT RANK() OVER (PARTITION BY grp ORDER BY score) AS r, \
			        DENSE_RANK() OVER (PARTITION BY grp ORDER BY score) AS dr FROM wins",
		)
		.expect("plan shared-spec window");
	assert_eq!(
		count_window_nodes(&plan),
		1,
		"shared OVER spec -> one Window node"
	);
	if let Some(functions) = first_window_functions(&plan) {
		assert_eq!(
			functions, 2,
			"the single Window node computes both functions"
		);
	} else {
		panic!("expected a Window node");
	}
}

#[tokio::test]
async fn window_alias_can_shadow_input_column_name() {
	// The planner must keep the window column internally distinct so a visible alias that matches an
	// input column still binds and executes.
	let logical = SqlPlanner::new()
		.plan("SELECT id, RANK() OVER (ORDER BY score) AS id FROM wins")
		.expect("plan shadowing window query");
	let (catalog, storage) = fixture();
	let bound = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind shadowing window query");
	let results = Executor::new(storage)
		.execute_bound(&bound, None)
		.await
		.expect("execute shadowing window query");
	assert_eq!(results.len(), 1);
	let batch = &results[0];
	assert_eq!(batch.schema().field(0).name(), "id");
	assert_eq!(batch.schema().field(1).name(), "id");
	let ranks = batch
		.column(1)
		.as_any()
		.downcast_ref::<Int64Array>()
		.expect("window rank column");
	assert_eq!(ranks.values(), &[1, 1, 3, 3, 5]);
}

#[tokio::test]
async fn deferred_window_forms_stay_rejected() {
	// ADR 243 retargets the 242 explicit-frame fence: explicit ROWS frames on aggregate windows now
	// plan, but RANGE/GROUPS units, frames on ranking functions, invalid bound ordering, DISTINCT
	// window aggregates, experimental window aggregates, multiple distinct specs, and the still-fenced
	// window placements stay rejected.
	for sql in [
		// explicit frame on a ranking function (frame-independent)
		"SELECT ROW_NUMBER() OVER (ORDER BY score ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM wins",
		// RANGE explicit frame on an aggregate (→ 244 range window frames)
		"SELECT SUM(score) OVER (ORDER BY score RANGE BETWEEN 2 PRECEDING AND CURRENT ROW) FROM wins",
		// GROUPS explicit frame on an aggregate (→ 244)
		"SELECT SUM(score) OVER (ORDER BY score GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM wins",
		// invalid bound ordering (start after end)
		"SELECT SUM(score) OVER (ORDER BY score ROWS BETWEEN CURRENT ROW AND 1 PRECEDING) FROM wins",
		// DISTINCT window aggregate
		"SELECT SUM(DISTINCT score) OVER (PARTITION BY grp) FROM wins",
		// experimental window aggregate
		"SELECT APPROX_QUANTILE(score, 0.5) OVER (PARTITION BY grp) FROM wins",
		// multiple distinct OVER specs
		"SELECT ROW_NUMBER() OVER (PARTITION BY grp ORDER BY score), \
		        ROW_NUMBER() OVER (PARTITION BY score ORDER BY grp) FROM wins",
		// window inside a derived table
		"SELECT rn FROM (SELECT ROW_NUMBER() OVER (ORDER BY score) AS rn FROM wins) d",
		// window in WHERE
		"SELECT grp FROM wins WHERE ROW_NUMBER() OVER (ORDER BY score) > 1",
	] {
		assert!(
			SqlPlanner::new().plan(sql).is_err(),
			"must stay rejected: {sql}"
		);
	}
}

// ---------------------------------------------------------------------------
// ADR 242 — aggregate window functions (default frame)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn whole_partition_aggregate_window() {
	// No ORDER BY -> one peer group = the whole partition; every row gets the partition total
	// (percent-of-total shape). grp a = SUM(10,10,20) = 40; grp b = SUM(5,5) = 10.
	let rows =
		run_window("SELECT grp, SUM(score) OVER (PARTITION BY grp) AS total FROM wins").await;
	assert_eq!(group_values(&rows, "a"), vec![40, 40, 40]);
	assert_eq!(group_values(&rows, "b"), vec![10, 10]);
}

#[tokio::test]
async fn running_sum_window_is_cumulative() {
	// ORDER BY -> running aggregate, restarting per partition. grp a ordered [10,10,20]; grp b [5,5].
	let rows =
		run_window("SELECT grp, SUM(score) OVER (PARTITION BY grp ORDER BY score) AS r FROM wins")
			.await;
	// grp a: peers at 10 share SUM-through-peer-group = 20, then +20 -> 40.
	assert_eq!(group_values(&rows, "a"), vec![20, 20, 40]);
	// grp b: both rows are peers at 5 -> share 10 (restarted from grp a).
	assert_eq!(group_values(&rows, "b"), vec![10, 10]);
}

#[tokio::test]
async fn running_window_range_ties_share_value() {
	// The RANGE-vs-ROWS discriminator: the two tied score=10 rows in grp a share the value cumulative
	// through the *whole* peer group (20, 20). A naive row-by-row ROWS fold would give 10, 20.
	let rows =
		run_window("SELECT grp, SUM(score) OVER (PARTITION BY grp ORDER BY score) AS r FROM wins")
			.await;
	let a = group_values(&rows, "a");
	assert_eq!(
		a[0], a[1],
		"tied ORDER BY rows must share the cumulative value (RANGE)"
	);
	assert_eq!(
		a[0], 20,
		"RANGE default frame, not a ROWS running fold (which would be 10)"
	);
}

#[tokio::test]
async fn count_star_window_counts_frame_rows() {
	// COUNT(*) counts rows in the frame: running under ORDER BY (through the peer group), so grp a's
	// two tied rows both see 2, then 3; grp b sees 2, 2.
	let rows =
		run_window("SELECT grp, COUNT(*) OVER (PARTITION BY grp ORDER BY score) AS c FROM wins")
			.await;
	assert_eq!(group_values(&rows, "a"), vec![2, 2, 3]);
	assert_eq!(group_values(&rows, "b"), vec![2, 2]);
}

#[tokio::test]
async fn min_max_window_over_partition() {
	let mins = run_window("SELECT grp, MIN(score) OVER (PARTITION BY grp) AS m FROM wins").await;
	assert_eq!(group_values(&mins, "a"), vec![10, 10, 10]);
	assert_eq!(group_values(&mins, "b"), vec![5, 5]);
	let maxes = run_window("SELECT grp, MAX(score) OVER (PARTITION BY grp) AS m FROM wins").await;
	assert_eq!(group_values(&maxes, "a"), vec![20, 20, 20]);
	assert_eq!(group_values(&maxes, "b"), vec![5, 5]);
}

#[tokio::test]
async fn window_aggregate_preserves_row_count() {
	// 5 input rows -> 5 output rows (anti-Aggregate).
	let rows =
		run_window("SELECT grp, SUM(score) OVER (PARTITION BY grp) AS total FROM wins").await;
	assert_eq!(rows.len(), 5);
}

#[tokio::test]
async fn aggregate_window_multiple_functions_share_one_window_node() {
	// Multiple aggregate calls over one OVER spec -> a single Window node computing both.
	let plan = SqlPlanner::new()
		.plan(
			"SELECT SUM(score) OVER (PARTITION BY grp) AS s, \
			        COUNT(*) OVER (PARTITION BY grp) AS c FROM wins",
		)
		.expect("plan shared-spec aggregate windows");
	assert_eq!(
		count_window_nodes(&plan),
		1,
		"shared OVER spec -> one Window node"
	);
	assert_eq!(
		first_window_functions(&plan),
		Some(2),
		"the single Window node computes both aggregate functions"
	);
}

#[tokio::test]
async fn running_avg_window_null_and_empty_semantics() {
	// obs(grp, v): grp a has v = [NULL, 5]; grp b has v = [NULL, NULL] (an all-NULL frame).
	// SUM/AVG over no non-null values -> NULL; COUNT(v) -> 0; COUNT(*) -> the row count.
	let sums = run_obs_window_i64("SELECT grp, SUM(v) OVER (PARTITION BY grp) AS s FROM obs").await;
	assert_eq!(obs_values(&sums, "a"), vec![Some(5), Some(5)]);
	assert_eq!(
		obs_values(&sums, "b"),
		vec![None, None],
		"all-NULL frame SUM -> NULL"
	);

	let avgs = run_obs_window_f64("SELECT grp, AVG(v) OVER (PARTITION BY grp) AS a FROM obs").await;
	assert_eq!(obs_values(&avgs, "a"), vec![Some(5.0), Some(5.0)]);
	assert_eq!(
		obs_values(&avgs, "b"),
		vec![None, None],
		"all-NULL frame AVG -> NULL"
	);

	let counts =
		run_obs_window_i64("SELECT grp, COUNT(v) OVER (PARTITION BY grp) AS c FROM obs").await;
	assert_eq!(obs_values(&counts, "a"), vec![Some(1), Some(1)]);
	assert_eq!(
		obs_values(&counts, "b"),
		vec![Some(0), Some(0)],
		"COUNT(x) over NULLs -> 0"
	);

	let count_star =
		run_obs_window_i64("SELECT grp, COUNT(*) OVER (PARTITION BY grp) AS c FROM obs").await;
	assert_eq!(obs_values(&count_star, "a"), vec![Some(2), Some(2)]);
	assert_eq!(
		obs_values(&count_star, "b"),
		vec![Some(2), Some(2)],
		"COUNT(*) counts rows"
	);
}

// ---------------------------------------------------------------------------
// ADR 243 — rows window frames (explicit ROWS BETWEEN)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn rows_frame_running_does_not_share_ties() {
	// The ROWS-vs-RANGE discriminator. ORDER BY score has a tie at 10 in grp a; the totalized sort puts
	// those two rows at adjacent positions. A `ROWS UNBOUNDED PRECEDING AND CURRENT ROW` cumulative gives
	// each its own frame end -> 10, 20 (distinct on this fixture), where the 242 RANGE default shares 20.
	let rows = run_window(
		"SELECT grp, SUM(score) OVER (PARTITION BY grp ORDER BY score \
		 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS r FROM wins",
	)
	.await;
	assert_eq!(group_values(&rows, "a"), vec![10, 20, 40]);
	assert_eq!(group_values(&rows, "b"), vec![5, 10]);
}

#[tokio::test]
async fn default_frame_aggregate_window_unchanged() {
	// Regression: an absent frame still computes the 242 RANGE default (tied score=10 rows in grp a share
	// the cumulative-through-peer-group value 20). Contrast with rows_frame_running_does_not_share_ties.
	let rows =
		run_window("SELECT grp, SUM(score) OVER (PARTITION BY grp ORDER BY score) AS r FROM wins")
			.await;
	assert_eq!(group_values(&rows, "a"), vec![20, 20, 40]);
	assert_eq!(group_values(&rows, "b"), vec![10, 10]);
}

#[tokio::test]
async fn rows_frame_moving_window_edges_respect_offsets() {
	// ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING is a 3-row centered window, clamped at the edges. grp a
	// ordered by id has scores [10,10,20]: row0 sees [10,10]=20, row1 sees [10,10,20]=40, row2 sees
	// [10,20]=30. grp b [5,5]: both see the 2-row partition = 10.
	let rows = run_window(
		"SELECT grp, SUM(score) OVER (PARTITION BY grp ORDER BY id \
		 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS r FROM wins",
	)
	.await;
	assert_eq!(group_values(&rows, "a"), vec![20, 40, 30]);
	assert_eq!(group_values(&rows, "b"), vec![10, 10]);
}

#[tokio::test]
async fn rows_frame_current_row_and_following() {
	// Look-ahead frame: CURRENT ROW AND 1 FOLLOWING. grp a scores [10,10,20] by id: row0 [10,10]=20,
	// row1 [10,20]=30, row2 [20]=20 (clamped at the last row).
	let rows = run_window(
		"SELECT grp, SUM(score) OVER (PARTITION BY grp ORDER BY id \
		 ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS r FROM wins",
	)
	.await;
	assert_eq!(group_values(&rows, "a"), vec![20, 30, 20]);
	assert_eq!(group_values(&rows, "b"), vec![10, 5]);
}

#[tokio::test]
async fn rows_frame_empty_window_is_null() {
	// A frame entirely past the partition end yields the empty aggregate: COUNT -> 0, SUM -> NULL.
	// grp a by id (positions 0,1,2): ROWS BETWEEN 2 FOLLOWING AND 3 FOLLOWING -> row0 sees position 2
	// (count 1), rows 1 and 2 see nothing (count 0 / SUM NULL).
	let counts = run_window(
		"SELECT grp, COUNT(*) OVER (PARTITION BY grp ORDER BY id \
		 ROWS BETWEEN 2 FOLLOWING AND 3 FOLLOWING) AS c FROM wins",
	)
	.await;
	assert_eq!(group_values(&counts, "a"), vec![1, 0, 0]);

	let sums = run_wins_window_opt_i64(
		"SELECT grp, SUM(score) OVER (PARTITION BY grp ORDER BY id \
		 ROWS BETWEEN 2 FOLLOWING AND 3 FOLLOWING) AS s FROM wins",
	)
	.await;
	assert_eq!(obs_values(&sums, "a"), vec![Some(20), None, None]);
}

#[tokio::test]
async fn rows_frame_multiple_functions_can_differ_in_frame() {
	// The per-call-frame discriminator (fails under a node-level frame): two aggregate windows over ONE
	// OVER spec (same PARTITION/ORDER) with DIFFERENT frames compute in one Window node. A 1-PRECEDING
	// trailing sum differs from the default running sum at the last row of grp a (30 vs 40).
	let sql = "SELECT grp, \
	           SUM(score) OVER (PARTITION BY grp ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS trailing, \
	           SUM(score) OVER (PARTITION BY grp ORDER BY id) AS running FROM wins";
	let plan = SqlPlanner::new().plan(sql).expect("plan two-frame window");
	assert_eq!(
		count_window_nodes(&plan),
		1,
		"one Window node for one OVER spec"
	);
	assert_eq!(
		first_window_functions(&plan),
		Some(2),
		"both differently-framed functions live in the single Window node"
	);

	let (catalog, storage) = fixture();
	let bound = bind(&plan, &catalog, &PlannerConfig::default())
		.await
		.expect("bind two-frame window");
	let results = Executor::new(storage)
		.execute_bound(&bound, None)
		.await
		.expect("execute two-frame window");
	// Collect (grp, trailing, running) and check grp a's last row: trailing 30 != running 40.
	let mut a_trailing = Vec::new();
	let mut a_running = Vec::new();
	for batch in &results {
		let grp = batch
			.column(0)
			.as_any()
			.downcast_ref::<StringArray>()
			.unwrap();
		let trailing = batch
			.column(1)
			.as_any()
			.downcast_ref::<Int64Array>()
			.unwrap();
		let running = batch
			.column(2)
			.as_any()
			.downcast_ref::<Int64Array>()
			.unwrap();
		for row in 0..batch.num_rows() {
			if grp.value(row) == "a" {
				a_trailing.push(trailing.value(row));
				a_running.push(running.value(row));
			}
		}
	}
	assert_eq!(a_trailing, vec![10, 20, 30], "trailing 1-PRECEDING frame");
	assert_eq!(a_running, vec![10, 20, 40], "default running frame");
	assert_ne!(
		a_trailing, a_running,
		"the two frames compute different values"
	);
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

/// Run a wins-fixture window query reading a nullable Int64 window column (for empty-frame NULL checks).
async fn run_wins_window_opt_i64(sql: &str) -> Vec<(Option<String>, Option<i64>)> {
	let logical = SqlPlanner::new().plan(sql).expect("plan wins window query");
	let (catalog, storage) = fixture();
	let bound = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind wins window query");
	let batches = Executor::new(storage)
		.execute_bound(&bound, None)
		.await
		.expect("execute wins window query");
	read_obs_values(&batches, |column, row| {
		let array = column
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("int64 window value column");
		(!array.is_null(row)).then(|| array.value(row))
	})
}

/// Run a window query and return `(grp, window_value)` rows (grp = column 0, window value = column 1).
async fn run_window(sql: &str) -> Vec<(Option<String>, i64)> {
	let logical = SqlPlanner::new().plan(sql).expect("plan window query");
	let (catalog, storage) = fixture();
	let bound = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind window query");
	let results = Executor::new(storage)
		.execute_bound(&bound, None)
		.await
		.expect("execute window query");

	let mut rows = Vec::new();
	for batch in &results {
		let grp = batch
			.column(0)
			.as_any()
			.downcast_ref::<StringArray>()
			.expect("grp utf8 column");
		let value = batch
			.column(1)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("window value int64 column");
		for row in 0..batch.num_rows() {
			let grp = if grp.is_null(row) {
				None
			} else {
				Some(grp.value(row).to_string())
			};
			rows.push((grp, value.value(row)));
		}
	}
	rows
}

/// Window values for one group, in output (sorted-within-partition) order.
fn group_values(rows: &[(Option<String>, i64)], grp: &str) -> Vec<i64> {
	rows.iter()
		.filter(|(g, _)| g.as_deref() == Some(grp))
		.map(|(_, value)| *value)
		.collect()
}

fn count_window_nodes(plan: &LogicalPlan) -> usize {
	match plan {
		LogicalPlan::Window { input, .. } => 1 + count_window_nodes(input),
		LogicalPlan::Project { input, .. }
		| LogicalPlan::Filter { input, .. }
		| LogicalPlan::Exchange { input, .. }
		| LogicalPlan::Aggregate { input, .. }
		| LogicalPlan::Sort { input, .. }
		| LogicalPlan::Distinct { input } => count_window_nodes(input),
		_ => 0,
	}
}

fn first_window_functions(plan: &LogicalPlan) -> Option<usize> {
	match plan {
		LogicalPlan::Window { functions, .. } => Some(functions.len()),
		LogicalPlan::Project { input, .. }
		| LogicalPlan::Filter { input, .. }
		| LogicalPlan::Exchange { input, .. }
		| LogicalPlan::Aggregate { input, .. }
		| LogicalPlan::Sort { input, .. }
		| LogicalPlan::Distinct { input } => first_window_functions(input),
		_ => None,
	}
}

fn wins_schema() -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("grp", DataType::Utf8, true),
		Field::new("score", DataType::Int64, true),
	]))
}

fn fixture() -> (MockCatalog, Arc<FixtureScan>) {
	let schema = wins_schema();
	let mut catalog = MockCatalog::new();
	catalog
		.register_descriptor(
			TableId(1),
			TableSchemaDescriptor::new(
				TableObjectId::new([1; 16]),
				1,
				"default",
				"wins",
				schema.as_ref(),
				RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
				LocalityPolicy::default_table_spread(),
				SchemaEvolutionPolicy::AdditiveOnly,
			)
			.expect("descriptor"),
		)
		.expect("register wins");
	let batch = RecordBatch::try_new(
		schema,
		vec![
			Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
			Arc::new(StringArray::from(vec![
				Some("a"),
				Some("a"),
				Some("a"),
				Some("b"),
				Some("b"),
			])),
			Arc::new(Int64Array::from(vec![
				Some(10),
				Some(10),
				Some(20),
				Some(5),
				Some(5),
			])),
		],
	)
	.expect("wins batch");
	(
		catalog,
		Arc::new(FixtureScan {
			batches: HashMap::from([(TableId(1), batch)]),
		}),
	)
}

/// obs(id, grp, v) with NULLs so aggregate-window NULL/empty semantics are discriminating:
/// grp "a" has v = [NULL, 5]; grp "b" has v = [NULL, NULL] (an all-NULL frame).
fn obs_fixture() -> (MockCatalog, Arc<FixtureScan>) {
	let schema = Arc::new(Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("grp", DataType::Utf8, true),
		Field::new("v", DataType::Int64, true),
	]));
	let mut catalog = MockCatalog::new();
	catalog
		.register_descriptor(
			TableId(1),
			TableSchemaDescriptor::new(
				TableObjectId::new([2; 16]),
				1,
				"default",
				"obs",
				schema.as_ref(),
				RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
				LocalityPolicy::default_table_spread(),
				SchemaEvolutionPolicy::AdditiveOnly,
			)
			.expect("descriptor"),
		)
		.expect("register obs");
	let batch = RecordBatch::try_new(
		schema,
		vec![
			Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
			Arc::new(StringArray::from(vec![
				Some("a"),
				Some("a"),
				Some("b"),
				Some("b"),
			])),
			Arc::new(Int64Array::from(vec![None, Some(5), None, None])),
		],
	)
	.expect("obs batch");
	(
		catalog,
		Arc::new(FixtureScan {
			batches: HashMap::from([(TableId(1), batch)]),
		}),
	)
}

async fn run_obs_window_i64(sql: &str) -> Vec<(Option<String>, Option<i64>)> {
	let batch = run_obs_window(sql).await;
	read_obs_values(&batch, |column, row| {
		let array = column
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("int64 window value column");
		(!array.is_null(row)).then(|| array.value(row))
	})
}

async fn run_obs_window_f64(sql: &str) -> Vec<(Option<String>, Option<f64>)> {
	let batch = run_obs_window(sql).await;
	read_obs_values(&batch, |column, row| {
		let array = column
			.as_any()
			.downcast_ref::<Float64Array>()
			.expect("float64 window value column");
		(!array.is_null(row)).then(|| array.value(row))
	})
}

async fn run_obs_window(sql: &str) -> Vec<RecordBatch> {
	let logical = SqlPlanner::new().plan(sql).expect("plan obs window query");
	let (catalog, storage) = obs_fixture();
	let bound = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind obs window query");
	Executor::new(storage)
		.execute_bound(&bound, None)
		.await
		.expect("execute obs window query")
}

fn read_obs_values<T>(
	batches: &[RecordBatch],
	read: impl Fn(&arrow::array::ArrayRef, usize) -> Option<T>,
) -> Vec<(Option<String>, Option<T>)> {
	let mut rows = Vec::new();
	for batch in batches {
		let grp = batch
			.column(0)
			.as_any()
			.downcast_ref::<StringArray>()
			.expect("grp utf8 column");
		let values = batch.column(1);
		for row in 0..batch.num_rows() {
			let grp = (!grp.is_null(row)).then(|| grp.value(row).to_string());
			rows.push((grp, read(values, row)));
		}
	}
	rows
}

/// Window values for one obs group, in output (sorted-within-partition) order.
fn obs_values<T: Copy>(rows: &[(Option<String>, Option<T>)], grp: &str) -> Vec<Option<T>> {
	rows.iter()
		.filter(|(g, _)| g.as_deref() == Some(grp))
		.map(|(_, value)| *value)
		.collect()
}

struct FixtureScan {
	batches: HashMap<TableId, RecordBatch>,
}

#[async_trait::async_trait]
impl StorageScan for FixtureScan {
	async fn scan(&self, request: ScanRequest) -> Result<ScanStream, StorageError> {
		let mut batch = self
			.batches
			.get(&request.table_id)
			.expect("fixture table")
			.clone();
		// Honor a pushed predicate (ADR 238) so a fully-pushed WHERE actually filters — otherwise the
		// window-after-WHERE gate would pass for the wrong reason.
		if let Some(pbc) = &request.pbc_predicate {
			batch = wendelon::schema::pbc::execute(pbc, &batch).map_err(|error| {
				StorageError::InvalidPbcProgram {
					message: format!("{error:?}"),
				}
			})?;
		}
		if let Some(projected_columns) = request.projected_columns.as_ref() {
			let schema = batch.schema();
			if projected_columns.is_empty() {
				let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
				batch = RecordBatch::try_new_with_options(
					Arc::new(Schema::empty()),
					Vec::new(),
					&options,
				)?;
			} else {
				let fields = projected_columns
					.iter()
					.map(|ordinal| schema.field(ordinal.0 as usize).as_ref().clone())
					.collect::<Vec<_>>();
				let columns = projected_columns
					.iter()
					.map(|ordinal| batch.column(ordinal.0 as usize).clone())
					.collect::<Vec<_>>();
				batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?;
			}
		}
		Ok(ScanStream::from_batches(
			batch.schema(),
			request.session_id,
			request.query_id,
			vec![batch],
		))
	}
}
