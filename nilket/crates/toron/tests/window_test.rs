//! ADR 241 — Window Functions Substrate behavior gates.
//!
//! Fixture `wins(id, grp, score)` with ties so RANK/DENSE_RANK are discriminating:
//! grp "a" scores [10, 10, 20] (a tie at 10); grp "b" scores [5, 5].

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, Int64Array, StringArray};
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
	for sql in [
		// explicit frame
		"SELECT ROW_NUMBER() OVER (ORDER BY score ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM wins",
		// aggregate-window
		"SELECT SUM(score) OVER (PARTITION BY grp) FROM wins",
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
// Harness
// ---------------------------------------------------------------------------

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
