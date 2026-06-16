//! ADR 241 — Window Functions Substrate behavior gates.
//!
//! Fixture `wins(id, grp, score)` with ties so RANK/DENSE_RANK are discriminating:
//! grp "a" scores [10, 10, 20] (a tie at 10); grp "b" scores [5, 5].

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, Float64Array, Int64Array, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
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
	// ADR 247 retargets the 245 fence: multiple distinct OVER specs now plan (each gets its own Window
	// node — see multiple_specs_plan_to_distinct_window_nodes). A GROUPS count offset without ORDER BY,
	// multi-key RANGE value offsets, RANGE value offsets on non-arithmetic keys, frames on ranking
	// functions, invalid bound ordering, DISTINCT window aggregates, experimental window aggregates, and
	// the still-fenced window placements stay rejected.
	for sql in [
		// explicit frame on a ranking function (frame-independent)
		"SELECT ROW_NUMBER() OVER (ORDER BY score ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM wins",
		// GROUPS count offset without ORDER BY (peer groups need an order)
		"SELECT SUM(score) OVER (PARTITION BY grp GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM wins",
		// frame EXCLUDE clause (deferred)
		"SELECT SUM(score) OVER (ORDER BY score GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW) FROM wins",
		// multi-key RANGE value offset (value distance over a tuple is undefined)
		"SELECT SUM(score) OVER (ORDER BY grp, score RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM wins",
		// invalid bound ordering (start after end)
		"SELECT SUM(score) OVER (ORDER BY score ROWS BETWEEN CURRENT ROW AND 1 PRECEDING) FROM wins",
		// NTILE with a zero / non-literal bucket count (ADR 249 lands NTILE(n); n must be a positive literal)
		"SELECT NTILE(0) OVER (ORDER BY score) FROM wins",
		"SELECT NTILE(score) OVER (ORDER BY score) FROM wins",
		// LAG without ORDER BY (positional navigation needs an order)
		"SELECT LAG(score) OVER (PARTITION BY grp) FROM wins",
		// IGNORE NULLS on a value function (deferred)
		"SELECT LAG(score) IGNORE NULLS OVER (ORDER BY id) FROM wins",
		// DISTINCT window aggregate
		"SELECT SUM(DISTINCT score) OVER (PARTITION BY grp) FROM wins",
		// experimental window aggregate
		"SELECT APPROX_QUANTILE(score, 0.5) OVER (PARTITION BY grp) FROM wins",
		// window inside a derived table
		"SELECT rn FROM (SELECT ROW_NUMBER() OVER (ORDER BY score) AS rn FROM wins) d",
		// window in WHERE
		"SELECT grp FROM wins WHERE ROW_NUMBER() OVER (ORDER BY score) > 1",
		// ADR 250: window inheritance (WINDOW w2 AS w1) is a spec-merge mechanism, deferred
		"SELECT RANK() OVER w2 FROM wins WINDOW w1 AS (ORDER BY score), w2 AS w1",
		// ADR 250: window extension (OVER (w ...) merging a named base with added clauses), deferred
		"SELECT RANK() OVER (w ORDER BY id) FROM wins WINDOW w AS (PARTITION BY grp)",
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
// ADR 244 — range window frames (explicit RANGE BETWEEN)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn range_offset_differs_from_rows_offset() {
	// RANGE follows value distance; ROWS follows row count. grp a scores [10,10,20] ordered by score.
	// RANGE 1 PRECEDING: the two 10-peers share their value window [9,10] = {10,10} = 20; the 20 sees
	// only [19,20] = {20} = 20. ROWS 1 PRECEDING: position-based 10, 20, 30.
	let range = run_window(
		"SELECT grp, SUM(score) OVER (PARTITION BY grp ORDER BY score \
		 RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS r FROM wins",
	)
	.await;
	assert_eq!(group_values(&range, "a"), vec![20, 20, 20]);
	let rows = run_window(
		"SELECT grp, SUM(score) OVER (PARTITION BY grp ORDER BY score \
		 ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS r FROM wins",
	)
	.await;
	assert_eq!(group_values(&rows, "a"), vec![10, 20, 30]);
	assert_ne!(
		group_values(&range, "a"),
		group_values(&rows, "a"),
		"RANGE value distance differs from ROWS row count"
	);
}

#[tokio::test]
async fn range_offset_extends_across_peer_groups() {
	// A wide value bound pulls neighbouring peer groups into the frame: the score=20 row's RANGE 10
	// PRECEDING window [10,20] includes the two score=10 rows -> SUM 10+10+20 = 40.
	let rows = run_window(
		"SELECT grp, SUM(score) OVER (PARTITION BY grp ORDER BY score \
		 RANGE BETWEEN 10 PRECEDING AND CURRENT ROW) AS r FROM wins",
	)
	.await;
	assert_eq!(group_values(&rows, "a"), vec![20, 20, 40]);
	assert_eq!(group_values(&rows, "b"), vec![10, 10]);
}

#[tokio::test]
async fn range_frame_peer_bounds_bind_on_any_key() {
	// RANGE UNBOUNDED/CURRENT ROW bounds carry no value offset, so they bind on ANY single ORDER BY key
	// (here Utf8 `grp`). RANGE UNBOUNDED PRECEDING AND CURRENT ROW ordered by grp is cumulative through
	// the value-peer group: every 'a' row sees the whole 'a' group (40); 'b' sees 'a' + 'b' (40+10 = 50).
	let rows = run_window(
		"SELECT grp, SUM(score) OVER (ORDER BY grp RANGE UNBOUNDED PRECEDING) AS r FROM wins",
	)
	.await;
	assert_eq!(group_values(&rows, "a"), vec![40, 40, 40]);
	assert_eq!(group_values(&rows, "b"), vec![50, 50]);
}

#[tokio::test]
async fn range_frame_empty_window_is_null() {
	// A value interval past every row: COUNT -> 0, SUM -> NULL.
	let counts = run_window(
		"SELECT grp, COUNT(*) OVER (PARTITION BY grp ORDER BY score \
		 RANGE BETWEEN 100 FOLLOWING AND 200 FOLLOWING) AS c FROM wins",
	)
	.await;
	assert_eq!(group_values(&counts, "a"), vec![0, 0, 0]);
	let sums = run_wins_window_opt_i64(
		"SELECT grp, SUM(score) OVER (PARTITION BY grp ORDER BY score \
		 RANGE BETWEEN 100 FOLLOWING AND 200 FOLLOWING) AS s FROM wins",
	)
	.await;
	assert_eq!(obs_values(&sums, "a"), vec![None, None, None]);
}

#[tokio::test]
async fn temporal_range_interval_offset_windows_by_value() {
	// The reframe's headline: a RANGE INTERVAL offset over a Timestamp key reuses the existing
	// `Timestamp - Interval` arithmetic. tseries ts = days [0, 0, 1, 3], v = [10, 20, 30, 40].
	// RANGE BETWEEN INTERVAL '1' DAY PRECEDING AND CURRENT ROW: the two day-0 rows share [day-1, day0] =
	// {10,20} = 30; day1 sees [day0, day1] = {10,20,30} = 60; day3 sees [day2, day3] = {40} = 40.
	let day = 86_400_000_000_000_i64;
	let schema = Arc::new(Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new(
			"ts",
			DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
			true,
		),
		Field::new("v", DataType::Int64, true),
	]));
	let mut catalog = MockCatalog::new();
	catalog
		.register_descriptor(
			TableId(1),
			TableSchemaDescriptor::new(
				TableObjectId::new([3; 16]),
				1,
				"default",
				"tseries",
				schema.as_ref(),
				RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
				LocalityPolicy::default_table_spread(),
				SchemaEvolutionPolicy::AdditiveOnly,
			)
			.expect("tseries descriptor"),
		)
		.expect("register tseries");
	let batch = RecordBatch::try_new(
		schema.clone(),
		vec![
			Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
			Arc::new(TimestampNanosecondArray::from(vec![0, 0, day, 3 * day]).with_timezone("UTC")),
			Arc::new(Int64Array::from(vec![
				Some(10),
				Some(20),
				Some(30),
				Some(40),
			])),
		],
	)
	.expect("tseries batch");
	let storage = Arc::new(FixtureScan {
		batches: HashMap::from([(TableId(1), batch)]),
	});

	let logical = SqlPlanner::new()
		.plan(
			"SELECT SUM(v) OVER (ORDER BY ts \
			 RANGE BETWEEN INTERVAL '1' DAY PRECEDING AND CURRENT ROW) AS s FROM tseries",
		)
		.expect("plan temporal RANGE");
	let bound = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind temporal RANGE");
	let results = Executor::new(storage)
		.execute_bound(&bound, None)
		.await
		.expect("execute temporal RANGE");
	let mut values = Vec::new();
	for batch in &results {
		let column = batch
			.column(0)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("sum column");
		for row in 0..batch.num_rows() {
			values.push(column.value(row));
		}
	}
	assert_eq!(values, vec![30, 30, 60, 40]);
}

#[tokio::test]
async fn range_value_offset_on_non_arithmetic_key_rejected() {
	// A RANGE value offset needs `key - offset` arithmetic. On a Utf8 ORDER BY key that has none, the
	// frame lowers (no types yet) but is rejected at bind. (Peer/UNBOUNDED RANGE on Utf8 still binds —
	// see range_frame_peer_bounds_bind_on_any_key.)
	let plan = SqlPlanner::new()
		.plan(
			"SELECT SUM(score) OVER (ORDER BY grp RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM wins",
		)
		.expect("a RANGE value offset lowers; the key-type check is at bind");
	let (catalog, _storage) = fixture();
	let bound = bind(&plan, &catalog, &PlannerConfig::default()).await;
	assert!(
		bound.is_err(),
		"RANGE value offset on a non-arithmetic (Utf8) key must be rejected at bind"
	);
}

#[tokio::test]
async fn range_negative_value_offset_is_rejected() {
	// Negative RANGE offsets invert the frame direction instead of acting as a bound magnitude. They must
	// be rejected at lowering, just like ROWS offsets.
	let plan = SqlPlanner::new().plan(
		"SELECT SUM(score) OVER (ORDER BY score RANGE BETWEEN INTERVAL '-1' DAY PRECEDING AND CURRENT ROW) FROM wins",
	);
	assert!(
		plan.is_err(),
		"negative RANGE value offsets must be rejected"
	);
}

#[tokio::test]
async fn range_frame_multiple_functions_can_differ_in_frame() {
	// A ROWS frame and a RANGE frame over one OVER spec compute in one Window node (per-call frame).
	let sql = "SELECT grp, \
	           SUM(score) OVER (PARTITION BY grp ORDER BY score ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS by_rows, \
	           SUM(score) OVER (PARTITION BY grp ORDER BY score RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS by_range \
	           FROM wins";
	let plan = SqlPlanner::new().plan(sql).expect("plan ROWS+RANGE window");
	assert_eq!(
		count_window_nodes(&plan),
		1,
		"one Window node for one OVER spec"
	);
	assert_eq!(first_window_functions(&plan), Some(2));
	let (catalog, storage) = fixture();
	let bound = bind(&plan, &catalog, &PlannerConfig::default())
		.await
		.expect("bind ROWS+RANGE window");
	let results = Executor::new(storage)
		.execute_bound(&bound, None)
		.await
		.expect("execute ROWS+RANGE window");
	let mut by_rows = Vec::new();
	let mut by_range = Vec::new();
	for batch in &results {
		let grp = batch
			.column(0)
			.as_any()
			.downcast_ref::<StringArray>()
			.unwrap();
		let r = batch
			.column(1)
			.as_any()
			.downcast_ref::<Int64Array>()
			.unwrap();
		let g = batch
			.column(2)
			.as_any()
			.downcast_ref::<Int64Array>()
			.unwrap();
		for row in 0..batch.num_rows() {
			if grp.value(row) == "a" {
				by_rows.push(r.value(row));
				by_range.push(g.value(row));
			}
		}
	}
	assert_eq!(by_rows, vec![10, 20, 30], "ROWS 1 PRECEDING frame");
	assert_eq!(by_range, vec![20, 20, 20], "RANGE 1 PRECEDING frame");
	assert_ne!(
		by_rows, by_range,
		"ROWS and RANGE frames differ in one node"
	);
}

// ---------------------------------------------------------------------------
// ADR 245 — groups window frames (explicit GROUPS BETWEEN)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn groups_offset_differs_from_rows_and_range() {
	// The three-way discriminator on grp a (scores [10,10,20] = peer groups {10,10},{20}). For the same
	// `1 PRECEDING AND CURRENT ROW` bound, the score=20 row gets: GROUPS 40 (whole prior group {10,10}
	// + {20}), ROWS 30 (one prior row + itself), RANGE 20 (value window [19,20] = {20}).
	let groups = run_window(
		"SELECT grp, SUM(score) OVER (PARTITION BY grp ORDER BY score \
		 GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) AS g FROM wins",
	)
	.await;
	let rows = run_window(
		"SELECT grp, SUM(score) OVER (PARTITION BY grp ORDER BY score \
		 ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS r FROM wins",
	)
	.await;
	let range = run_window(
		"SELECT grp, SUM(score) OVER (PARTITION BY grp ORDER BY score \
		 RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS r FROM wins",
	)
	.await;
	assert_eq!(
		group_values(&groups, "a"),
		vec![20, 20, 40],
		"GROUPS: whole prior peer group"
	);
	assert_eq!(
		group_values(&rows, "a"),
		vec![10, 20, 30],
		"ROWS: one prior physical row"
	);
	assert_eq!(
		group_values(&range, "a"),
		vec![20, 20, 20],
		"RANGE: value distance"
	);
	assert_ne!(group_values(&groups, "a"), group_values(&rows, "a"));
	assert_ne!(group_values(&groups, "a"), group_values(&range, "a"));
	assert_ne!(group_values(&rows, "a"), group_values(&range, "a"));
}

#[tokio::test]
async fn groups_frame_includes_whole_peer_group_and_tied_rows_share() {
	// The score=20 row's GROUPS 1 PRECEDING frame pulls the *entire* prior peer group (both score=10
	// rows) -> SUM 40; and the two tied score=10 rows share one value (20).
	let g = run_window(
		"SELECT grp, SUM(score) OVER (PARTITION BY grp ORDER BY score \
		 GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) AS g FROM wins",
	)
	.await;
	let a = group_values(&g, "a");
	assert_eq!(a[0], a[1], "tied score=10 rows share one frame value");
	assert_eq!(
		a[2], 40,
		"the whole prior peer group (both 10s) enters the frame, not one row"
	);
}

#[tokio::test]
async fn groups_frame_binds_on_utf8_key() {
	// GROUPS needs only peer-group definition, not value arithmetic, so it binds on a Utf8 ORDER BY key
	// (where a value-offset RANGE rejects). ORDER BY grp -> peer groups {a,a,a},{b,b}. GROUPS UNBOUNDED
	// PRECEDING AND CURRENT ROW: 'a' rows see group {a} (40); 'b' rows see {a}+{b} (40 + 10 = 50).
	let rows = run_window(
		"SELECT grp, SUM(score) OVER (ORDER BY grp GROUPS UNBOUNDED PRECEDING) AS g FROM wins",
	)
	.await;
	assert_eq!(group_values(&rows, "a"), vec![40, 40, 40]);
	assert_eq!(group_values(&rows, "b"), vec![50, 50]);
}

#[tokio::test]
async fn groups_frame_empty_window_is_null() {
	// A group offset past the last group: COUNT -> 0, SUM -> NULL. grp a has 2 groups ({10,10},{20});
	// GROUPS BETWEEN 2 FOLLOWING AND 3 FOLLOWING lands entirely past the partition's groups.
	let counts = run_window(
		"SELECT grp, COUNT(*) OVER (PARTITION BY grp ORDER BY score \
		 GROUPS BETWEEN 2 FOLLOWING AND 3 FOLLOWING) AS c FROM wins",
	)
	.await;
	assert_eq!(group_values(&counts, "a"), vec![0, 0, 0]);
	let sums = run_wins_window_opt_i64(
		"SELECT grp, SUM(score) OVER (PARTITION BY grp ORDER BY score \
		 GROUPS BETWEEN 2 FOLLOWING AND 3 FOLLOWING) AS s FROM wins",
	)
	.await;
	assert_eq!(obs_values(&sums, "a"), vec![None, None, None]);
}

#[tokio::test]
async fn groups_frame_multiple_functions_can_differ_in_frame() {
	// A ROWS frame and a GROUPS frame over one OVER spec compute in one Window node (per-call frame).
	let sql = "SELECT grp, \
	           SUM(score) OVER (PARTITION BY grp ORDER BY score ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS by_rows, \
	           SUM(score) OVER (PARTITION BY grp ORDER BY score GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) AS by_groups \
	           FROM wins";
	let plan = SqlPlanner::new()
		.plan(sql)
		.expect("plan ROWS+GROUPS window");
	assert_eq!(
		count_window_nodes(&plan),
		1,
		"one Window node for one OVER spec"
	);
	assert_eq!(first_window_functions(&plan), Some(2));
	let (catalog, storage) = fixture();
	let bound = bind(&plan, &catalog, &PlannerConfig::default())
		.await
		.expect("bind ROWS+GROUPS window");
	let results = Executor::new(storage)
		.execute_bound(&bound, None)
		.await
		.expect("execute ROWS+GROUPS window");
	let mut by_rows = Vec::new();
	let mut by_groups = Vec::new();
	for batch in &results {
		let grp = batch
			.column(0)
			.as_any()
			.downcast_ref::<StringArray>()
			.unwrap();
		let r = batch
			.column(1)
			.as_any()
			.downcast_ref::<Int64Array>()
			.unwrap();
		let g = batch
			.column(2)
			.as_any()
			.downcast_ref::<Int64Array>()
			.unwrap();
		for row in 0..batch.num_rows() {
			if grp.value(row) == "a" {
				by_rows.push(r.value(row));
				by_groups.push(g.value(row));
			}
		}
	}
	assert_eq!(by_rows, vec![10, 20, 30], "ROWS 1 PRECEDING frame");
	assert_eq!(by_groups, vec![20, 20, 40], "GROUPS 1 PRECEDING frame");
	assert_ne!(
		by_rows, by_groups,
		"ROWS and GROUPS frames differ in one node"
	);
}

// ---------------------------------------------------------------------------
// ADR 246 — value window functions (LAG/LEAD/FIRST_VALUE/LAST_VALUE/NTH_VALUE)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn lag_returns_prior_row_value() {
	// LAG(score) over id-order: the previous row's score, NULL at the partition start, per-partition.
	let rows = run_wins_window_opt_i64(
		"SELECT grp, LAG(score) OVER (PARTITION BY grp ORDER BY id) AS l FROM wins",
	)
	.await;
	assert_eq!(obs_values(&rows, "a"), vec![None, Some(10), Some(10)]);
	assert_eq!(obs_values(&rows, "b"), vec![None, Some(5)]);
}

#[tokio::test]
async fn lead_returns_next_row_value() {
	let rows = run_wins_window_opt_i64(
		"SELECT grp, LEAD(score) OVER (PARTITION BY grp ORDER BY id) AS l FROM wins",
	)
	.await;
	assert_eq!(obs_values(&rows, "a"), vec![Some(10), Some(20), None]);
	assert_eq!(obs_values(&rows, "b"), vec![Some(5), None]);
}

#[tokio::test]
async fn lag_offset_and_default_at_partition_edge() {
	// LAG(score, 2, -1): two rows back, or the default -1 past the partition start (not NULL, not a leak
	// from the prior partition). grp a [10,10,20] -> [-1, -1, 10]; grp b [5,5] -> [-1, -1].
	let rows = run_window(
		"SELECT grp, LAG(score, 2, -1) OVER (PARTITION BY grp ORDER BY id) AS l FROM wins",
	)
	.await;
	assert_eq!(group_values(&rows, "a"), vec![-1, -1, 10]);
	assert_eq!(group_values(&rows, "b"), vec![-1, -1]);
}

#[tokio::test]
async fn lag_ignores_frame() {
	// LAG is positional — an explicit frame is ignored. The framed and unframed forms are identical.
	let framed = run_wins_window_opt_i64(
		"SELECT grp, LAG(score) OVER (PARTITION BY grp ORDER BY id \
		 ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS l FROM wins",
	)
	.await;
	let unframed = run_wins_window_opt_i64(
		"SELECT grp, LAG(score) OVER (PARTITION BY grp ORDER BY id) AS l FROM wins",
	)
	.await;
	assert_eq!(obs_values(&framed, "a"), obs_values(&unframed, "a"));
	assert_eq!(obs_values(&framed, "a"), vec![None, Some(10), Some(10)]);
}

#[tokio::test]
async fn first_last_value_respect_frame() {
	// FIRST/LAST_VALUE are frame-defined. Ordered by score, grp a's peer group {id1,id2} (score 10) and
	// {id3} (score 20). LAST_VALUE(id) over the DEFAULT frame = the last value of the current *peer group*
	// (the tied rows share id 2, NOT their own id); over a whole-partition ROWS frame = the partition's
	// last (id 3). FIRST_VALUE(id) = the partition's first (id 1).
	let last_default = run_window(
		"SELECT grp, LAST_VALUE(id) OVER (PARTITION BY grp ORDER BY score) AS v FROM wins",
	)
	.await;
	assert_eq!(
		group_values(&last_default, "a"),
		vec![2, 2, 3],
		"peer-group last, tied rows share"
	);
	let last_whole = run_window(
		"SELECT grp, LAST_VALUE(id) OVER (PARTITION BY grp ORDER BY score \
		 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS v FROM wins",
	)
	.await;
	assert_eq!(
		group_values(&last_whole, "a"),
		vec![3, 3, 3],
		"whole-partition frame -> partition last"
	);
	assert_ne!(
		group_values(&last_default, "a"),
		group_values(&last_whole, "a"),
		"LAST_VALUE is frame-defined"
	);
	let first = run_window(
		"SELECT grp, FIRST_VALUE(id) OVER (PARTITION BY grp ORDER BY score) AS v FROM wins",
	)
	.await;
	assert_eq!(
		group_values(&first, "a"),
		vec![1, 1, 1],
		"frame start = partition first"
	);
}

#[tokio::test]
async fn nth_value_uses_frame_ordinal_and_null_past_end() {
	// NTH_VALUE(id, 2) over the default frame [partition start, current row]: the 2nd row of the frame, or
	// NULL when the frame has fewer than 2 rows. grp a by id -> [NULL, 2, 2].
	let rows = run_wins_window_opt_i64(
		"SELECT grp, NTH_VALUE(id, 2) OVER (PARTITION BY grp ORDER BY id) AS v FROM wins",
	)
	.await;
	assert_eq!(obs_values(&rows, "a"), vec![None, Some(2), Some(2)]);
	assert_eq!(obs_values(&rows, "b"), vec![None, Some(5)]);
}

#[tokio::test]
async fn value_functions_share_one_window_node() {
	// A navigation function and an aggregate over one OVER spec collapse to a single Window node.
	let plan = SqlPlanner::new()
		.plan(
			"SELECT LAG(score) OVER (PARTITION BY grp ORDER BY id) AS l, \
			        SUM(score) OVER (PARTITION BY grp ORDER BY id) AS s FROM wins",
		)
		.expect("plan LAG + SUM window");
	assert_eq!(
		count_window_nodes(&plan),
		1,
		"one Window node for one OVER spec"
	);
	assert_eq!(first_window_functions(&plan), Some(2));
}

// ---------------------------------------------------------------------------
// ADR 247 — multiple window specifications per query
// ---------------------------------------------------------------------------

#[tokio::test]
async fn multiple_specs_plan_to_distinct_window_nodes() {
	// Two distinct specs (PARTITION BY grp vs no partition) stack into two Window nodes. This rejected
	// on main ("multiple distinct OVER specifications ... not supported").
	let plan = SqlPlanner::new()
		.plan(
			"SELECT grp, SUM(score) OVER (PARTITION BY grp) AS by_grp, \
			        SUM(score) OVER () AS total FROM wins",
		)
		.expect("plan two distinct window specs");
	assert_eq!(
		count_window_nodes(&plan),
		2,
		"two distinct specs -> two stacked Window nodes"
	);
}

#[tokio::test]
async fn same_spec_calls_share_one_window_node() {
	// Two calls that share the exact spec collapse into one Window node (not one node per call).
	let plan = SqlPlanner::new()
		.plan("SELECT RANK() OVER (ORDER BY score), ROW_NUMBER() OVER (ORDER BY score) FROM wins")
		.expect("plan two calls sharing one spec");
	assert_eq!(
		count_window_nodes(&plan),
		1,
		"one shared spec -> one Window node"
	);
	assert_eq!(
		first_window_functions(&plan),
		Some(2),
		"both calls live in the single node"
	);
}

#[tokio::test]
async fn same_partition_order_different_frames_share_one_node() {
	// The grouping key is (PARTITION BY, ORDER BY) — NOT the frame. Two calls with the same
	// partition+order but different per-call ROWS frames share one node (the frame rides the call).
	let plan = SqlPlanner::new()
		.plan(
			"SELECT SUM(score) OVER (ORDER BY score ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), \
			        SUM(score) OVER (ORDER BY score ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM wins",
		)
		.expect("plan two frames sharing partition+order");
	assert_eq!(
		count_window_nodes(&plan),
		1,
		"same partition+order, frame is per-call -> one node"
	);
	assert_eq!(first_window_functions(&plan), Some(2));
}

#[tokio::test]
async fn multiple_specs_compute_independent_values() {
	// by_grp partitions by grp (a -> 40, b -> 10); total has no partition (grand total -> 50). Each
	// column is correct only under its own spec; a single-spec funnel could not produce both. scores =
	// [10,10,20,5,5] = 50 total.
	let batches = run_window_batches(
		"SELECT grp, SUM(score) OVER (PARTITION BY grp) AS by_grp, \
		        SUM(score) OVER () AS total FROM wins",
	)
	.await;
	let mut saw_difference = false;
	for batch in &batches {
		let grp = batch
			.column(0)
			.as_any()
			.downcast_ref::<StringArray>()
			.expect("col0 grp utf8");
		let by_grp = batch
			.column(1)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("col1 by_grp int64");
		let total = batch
			.column(2)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("col2 total int64");
		for row in 0..batch.num_rows() {
			assert_eq!(total.value(row), 50, "grand total over every row");
			match grp.value(row) {
				"a" => assert_eq!(by_grp.value(row), 40, "group a per-group sum"),
				"b" => assert_eq!(by_grp.value(row), 10, "group b per-group sum"),
				other => panic!("unexpected group {other}"),
			}
			saw_difference |= by_grp.value(row) != total.value(row);
		}
	}
	assert!(
		saw_difference,
		"the per-group and grand-total columns must differ -> the two specs are independent"
	);
}

#[tokio::test]
async fn stacked_window_output_order_preserved() {
	// Columns come out in user SELECT order even though the two window columns are computed at different
	// stack depths and interleave with a passthrough column: total (empty spec), grp (passthrough),
	// by_grp (grp spec). The Projection above the stack restores SELECT order; appending window columns
	// at the end would flip the col0/col1 types and fail the downcasts.
	let batches = run_window_batches(
		"SELECT SUM(score) OVER () AS total, grp, \
		        SUM(score) OVER (PARTITION BY grp) AS by_grp FROM wins",
	)
	.await;
	for batch in &batches {
		assert_eq!(batch.num_columns(), 3, "total, grp, by_grp");
		let total = batch
			.column(0)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("col0 total int64 (SELECT order)");
		batch
			.column(1)
			.as_any()
			.downcast_ref::<StringArray>()
			.expect("col1 grp utf8 (SELECT order)");
		let by_grp = batch
			.column(2)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("col2 by_grp int64 (SELECT order)");
		for row in 0..batch.num_rows() {
			assert_eq!(total.value(row), 50);
			assert!(
				matches!(by_grp.value(row), 40 | 10),
				"by_grp is a per-group total"
			);
		}
	}
}

// ---------------------------------------------------------------------------
// ADR 249 — NTILE window bucketing
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ntile_buckets_rows_uniformly() {
	// A true 4-row partition (ids 1-4, ordered by id), 2 buckets, splits evenly: [1,1,2,2].
	let rows =
		run_window("SELECT grp, NTILE(2) OVER (ORDER BY id) AS b FROM wins WHERE id <= 4").await;
	let buckets: Vec<i64> = rows.iter().map(|(_, b)| *b).collect();
	assert_eq!(buckets, vec![1, 1, 2, 2], "4 rows, 2 buckets -> even split");
}

#[tokio::test]
async fn ntile_distributes_remainder_to_first_buckets() {
	// NTILE(3) over 4 rows -> the first bucket gets the remainder row: [1,1,2,3]. A naive
	// `i / (len / k)` bucketing gives [1,1,3,4] and fails here.
	let rows =
		run_window("SELECT grp, NTILE(3) OVER (ORDER BY id) AS b FROM wins WHERE id <= 4").await;
	let buckets: Vec<i64> = rows.iter().map(|(_, b)| *b).collect();
	assert_eq!(
		buckets,
		vec![1, 1, 2, 3],
		"remainder goes to the first bucket"
	);
}

#[tokio::test]
async fn ntile_restarts_per_partition() {
	// Buckets reset per partition: grp a (3 rows) -> [1,1,2] (first bucket gets the remainder),
	// grp b (2 rows) -> [1,2].
	let rows =
		run_window("SELECT grp, NTILE(2) OVER (PARTITION BY grp ORDER BY score) AS b FROM wins")
			.await;
	assert_eq!(group_values(&rows, "a"), vec![1, 1, 2]);
	assert_eq!(group_values(&rows, "b"), vec![1, 2]);
}

#[tokio::test]
async fn ntile_more_buckets_than_rows() {
	// More buckets than rows -> each row its own bucket (no division by zero).
	let rows =
		run_window("SELECT grp, NTILE(10) OVER (PARTITION BY grp ORDER BY score) AS b FROM wins")
			.await;
	assert_eq!(
		group_values(&rows, "a"),
		vec![1, 2, 3],
		"3 rows, 10 buckets -> [1,2,3]"
	);
	assert_eq!(
		group_values(&rows, "b"),
		vec![1, 2],
		"2 rows, 10 buckets -> [1,2]"
	);
}

#[tokio::test]
async fn ntile_without_order_by_is_accepted() {
	// No ORDER BY: NTILE buckets in the executor's totalized order (deterministic), like ROW_NUMBER —
	// not rejected. grp a (3 rows, totalized by id) -> [1,1,2], grp b (2 rows) -> [1,2].
	let rows = run_window("SELECT grp, NTILE(2) OVER (PARTITION BY grp) AS b FROM wins").await;
	assert_eq!(group_values(&rows, "a"), vec![1, 1, 2]);
	assert_eq!(group_values(&rows, "b"), vec![1, 2]);
}

// ---------------------------------------------------------------------------
// ADR 250 — named windows
// ---------------------------------------------------------------------------

#[tokio::test]
async fn named_window_resolves_like_inline_spec() {
	// `OVER w` resolves to the WINDOW-clause definition; identical to the inline spelling.
	let named = run_window(
		"SELECT grp, RANK() OVER w AS r FROM wins WINDOW w AS (PARTITION BY grp ORDER BY score)",
	)
	.await;
	let inline =
		run_window("SELECT grp, RANK() OVER (PARTITION BY grp ORDER BY score) AS r FROM wins")
			.await;
	assert_eq!(named, inline, "OVER w resolves to the inline spec");
	assert_eq!(group_values(&named, "a"), vec![1, 1, 3]);
}

#[tokio::test]
async fn named_window_carries_frame() {
	// A named definition carrying a ROWS frame (243) is inherited by `OVER w` unchanged.
	let named = run_window(
		"SELECT grp, SUM(score) OVER w AS s FROM wins \
		 WINDOW w AS (PARTITION BY grp ORDER BY score ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)",
	)
	.await;
	let inline = run_window(
		"SELECT grp, SUM(score) OVER (PARTITION BY grp ORDER BY score \
		 ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS s FROM wins",
	)
	.await;
	assert_eq!(named, inline, "a named window inherits its frame");
}

#[tokio::test]
async fn named_window_shared_across_calls_is_one_node() {
	// Two calls referencing the same `w` collapse to one Window node (composes with ADR 248 grouping).
	let plan = SqlPlanner::new()
		.plan(
			"SELECT RANK() OVER w, ROW_NUMBER() OVER w FROM wins \
			 WINDOW w AS (PARTITION BY grp ORDER BY score)",
		)
		.expect("plan shared named window");
	assert_eq!(
		count_window_nodes(&plan),
		1,
		"two calls sharing OVER w -> one Window node"
	);
	assert_eq!(first_window_functions(&plan), Some(2));
}

#[tokio::test]
async fn distinct_named_windows_stack() {
	// Distinct named windows stack into two nodes (composes with ADR 248 stacking).
	let plan = SqlPlanner::new()
		.plan(
			"SELECT RANK() OVER w1, ROW_NUMBER() OVER w2 FROM wins \
			 WINDOW w1 AS (PARTITION BY grp ORDER BY score), w2 AS (ORDER BY id)",
		)
		.expect("plan distinct named windows");
	assert_eq!(
		count_window_nodes(&plan),
		2,
		"two distinct named windows -> two stacked nodes"
	);
}

#[tokio::test]
async fn unknown_named_window_rejects() {
	assert!(
		SqlPlanner::new()
			.plan("SELECT RANK() OVER w FROM wins")
			.is_err(),
		"OVER w with no WINDOW clause rejects"
	);
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

/// Run a window query and return the raw projected batches (for multi-column / multi-spec assertions).
async fn run_window_batches(sql: &str) -> Vec<RecordBatch> {
	let logical = SqlPlanner::new().plan(sql).expect("plan window query");
	let (catalog, storage) = fixture();
	let bound = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind window query");
	Executor::new(storage)
		.execute_bound(&bound, None)
		.await
		.expect("execute window query")
}

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
