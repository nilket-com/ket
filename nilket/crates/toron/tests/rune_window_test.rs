//! ADR 251 — Rune window builder parity gates.
//!
//! The Rune `.window(partition_by, order_by, calls)` verb routes through the same
//! `window_over_exchange` helper the SQL window lowering uses, so a Rune-built window plan must be
//! byte-identical to — and execute identically to — the SQL `OVER (...)` equivalent over the same
//! `wins(id, grp, score)` fixture used by `window_test.rs`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use toron::binder::{PlannerConfig, bind};
use toron::catalog::MockCatalog;
use toron::error::{FrontendError, StorageError};
use toron::exec::Executor;
use toron::ir::{
	LogicalPlan, ScalarValue, TableId, WindowFrame, WindowFrameBound, WindowFrameDelta,
};
use toron::rune_engine::{RuneEngine, RuneSandboxConfig};
use toron::sql_planner::SqlPlanner;
use toron::storage::{ScanRequest, ScanStream, StorageScan};
use wendelon::schema::{
	LocalityPolicy, PrimaryKeyDescriptor, RowIdentityPolicy, SchemaEvolutionPolicy, TableObjectId,
	TableSchemaDescriptor,
};

fn sandbox_config() -> RuneSandboxConfig {
	RuneSandboxConfig {
		wall_timeout: Duration::from_secs(2),
		..RuneSandboxConfig::v1_default()
	}
}

async fn rune_plan(script: &str) -> Result<LogicalPlan, FrontendError> {
	let config = sandbox_config();
	let engine = RuneEngine::with_config(config).expect("engine");
	engine.execute_script(script, &config).await
}

async fn sql_plan(sql: &str) -> LogicalPlan {
	SqlPlanner::new().plan(sql).expect("plan sql window query")
}

/// Bind + execute a logical plan against the `wins` fixture and collect `(grp, window_value)` rows
/// (grp = column 0, window value = column 1). NULL window values are NOT expected by these gates;
/// the value column is read as a non-null Int64.
async fn run_plan(plan: &LogicalPlan) -> Vec<(Option<String>, i64)> {
	let (catalog, storage) = fixture();
	let bound = bind(plan, &catalog, &PlannerConfig::default())
		.await
		.expect("bind window plan");
	let results = Executor::new(storage)
		.execute_bound(&bound, None)
		.await
		.expect("execute window plan");

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
			let grp = (!grp.is_null(row)).then(|| grp.value(row).to_string());
			rows.push((grp, value.value(row)));
		}
	}
	rows
}

/// Sort `(grp, value)` rows into a deterministic order so a Rune plan and a SQL plan are compared
/// independent of per-partition emission order across the exchange.
fn sorted(mut rows: Vec<(Option<String>, i64)>) -> Vec<(Option<String>, i64)> {
	rows.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
	rows
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

// ---------------------------------------------------------------------------
// Parity gates
// ---------------------------------------------------------------------------

#[tokio::test]
async fn rune_window_ranking_matches_sql() {
	// RANK() OVER (PARTITION BY grp ORDER BY score). The fixture has a tie at score 10 in grp "a", so
	// RANK is discriminating (1,1,3). Rune builds the bare Window node; both Rune and SQL project the
	// same (grp, rank) columns so the executed results compare directly.
	let rune = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("wins")
                .window([col("grp")], [col("score").asc()], [rank().named("r")])
                .project([col("grp"), col("r")])
        }
    "#,
	)
	.await
	.expect("Rune ranking window");
	let sql =
		sql_plan("SELECT grp, RANK() OVER (PARTITION BY grp ORDER BY score) AS r FROM wins").await;

	let rune_rows = sorted(run_plan(&rune).await);
	let sql_rows = sorted(run_plan(&sql).await);
	assert_eq!(rune_rows, sql_rows, "Rune RANK must match SQL RANK");
	// And the discriminating values are present (1,1,3 in grp a).
	let a = rune_rows
		.iter()
		.filter(|(g, _)| g.as_deref() == Some("a"))
		.map(|(_, v)| *v)
		.collect::<Vec<_>>();
	assert_eq!(a, vec![1, 1, 3], "RANK with a tie gap in grp a");
}

#[tokio::test]
async fn rune_window_aggregate_with_frame() {
	// sum(col("score")).over().frame(rows(preceding(1), current_row())) -> a trailing 1-row sum. The
	// SQL ROWS-frame equivalent is SUM(score) OVER (... ROWS BETWEEN 1 PRECEDING AND CURRENT ROW).
	// Ordered by id, grp a scores [10,10,20] -> running trailing-2 sum [10,20,30].
	let rune = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("wins")
                .window(
                    [col("grp")],
                    [col("id").asc()],
                    [sum(col("score")).over().frame(rows(preceding(1), current_row())).named("s")]
                )
                .project([col("grp"), col("s")])
        }
    "#,
	)
	.await
	.expect("Rune aggregate window with frame");
	let sql = sql_plan(
		"SELECT grp, SUM(score) OVER (PARTITION BY grp ORDER BY id \
		 ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS s FROM wins",
	)
	.await;

	let rune_rows = sorted(run_plan(&rune).await);
	let sql_rows = sorted(run_plan(&sql).await);
	assert_eq!(
		rune_rows, sql_rows,
		"Rune ROWS-frame aggregate must match SQL"
	);
	let mut a = run_plan(&rune)
		.await
		.into_iter()
		.filter(|(g, _)| g.as_deref() == Some("a"))
		.map(|(_, v)| v)
		.collect::<Vec<_>>();
	a.sort_unstable();
	assert_eq!(a, vec![10, 20, 30], "trailing-2 running sum over grp a");
}

#[tokio::test]
async fn rune_chained_windows_stack() {
	// Two `.window(..)` calls produce two stacked Window nodes (each its own OVER spec).
	let plan = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("wins")
                .window([col("grp")], [col("score").asc()], [rank().named("r")])
                .window([col("grp")], [col("id").asc()], [row_number().named("rn")])
        }
    "#,
	)
	.await
	.expect("Rune chained windows");
	assert_eq!(
		count_window_nodes(&plan),
		2,
		"two .window calls -> two stacked Window nodes"
	);
}

#[tokio::test]
async fn rune_window_ntile_and_value() {
	// ntile(2) divides each ordered partition into 2 near-equal buckets; lag(col, 1) returns the prior
	// row's value. Both build and execute; ntile(2) over grp a (3 rows ordered by id) buckets as
	// [1,1,2] (the first ceil(3/2)=2 rows in bucket 1).
	let ntile = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("wins")
                .window([col("grp")], [col("id").asc()], [ntile(2).named("b")])
                .project([col("grp"), col("b")])
        }
    "#,
	)
	.await
	.expect("Rune ntile window");
	let ntile_sql =
		sql_plan("SELECT grp, NTILE(2) OVER (PARTITION BY grp ORDER BY id) AS b FROM wins").await;
	assert_eq!(
		sorted(run_plan(&ntile).await),
		sorted(run_plan(&ntile_sql).await),
		"Rune NTILE(2) must match SQL"
	);

	// lag(col("score"), 1) reads as a nullable column (NULL at the partition edge), so assert it
	// executes and produces 5 rows rather than reading the non-null value column.
	let lag = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("wins")
                .window([col("grp")], [col("id").asc()], [lag(col("score"), 1).named("l")])
                .project([col("grp"), col("l")])
        }
    "#,
	)
	.await
	.expect("Rune lag window");
	let (catalog, storage) = fixture();
	let bound = bind(&lag, &catalog, &PlannerConfig::default())
		.await
		.expect("bind lag window");
	let results = Executor::new(storage)
		.execute_bound(&bound, None)
		.await
		.expect("execute lag window");
	let total_rows: usize = results.iter().map(RecordBatch::num_rows).sum();
	assert_eq!(total_rows, 5, "LAG window preserves the 5 input rows");
}

#[tokio::test]
async fn rune_window_requires_at_least_one_call() {
	// Empty PARTITION BY / empty ORDER BY are valid; only an empty calls list is rejected.
	let error = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("wins").window([], [], [])
        }
    "#,
	)
	.await
	.expect_err("empty window-call list must be rejected");
	assert!(
		matches!(error, FrontendError::RuneRuntime { .. }),
		"empty window calls panics at runtime: {error:?}"
	);
}

#[tokio::test]
async fn rune_window_empty_partition_is_valid() {
	// OVER (ORDER BY score) with no PARTITION BY: one partition, ranks all 5 rows. Matches the SQL form.
	let rune = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("wins")
                .window([], [col("score").asc()], [rank().named("r")])
                .project([col("grp"), col("r")])
        }
    "#,
	)
	.await
	.expect("Rune empty-partition window");
	let sql = sql_plan("SELECT grp, RANK() OVER (ORDER BY score) AS r FROM wins").await;
	assert_eq!(
		sorted(run_plan(&rune).await),
		sorted(run_plan(&sql).await),
		"empty PARTITION BY window must match SQL"
	);
}

// ---------------------------------------------------------------------------
// `wins` fixture (mirrors window_test.rs): id, grp, score with a tie at score 10 in grp "a".
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// ADR 251 (Rune-finish) — LAG/LEAD literal default + value-offset RANGE
// ---------------------------------------------------------------------------

fn first_window_frame(plan: &LogicalPlan) -> Option<WindowFrame> {
	match plan {
		LogicalPlan::Window { functions, .. } => {
			functions.first().and_then(|call| call.frame.clone())
		}
		LogicalPlan::Project { input, .. }
		| LogicalPlan::Filter { input, .. }
		| LogicalPlan::Exchange { input, .. }
		| LogicalPlan::Aggregate { input, .. }
		| LogicalPlan::Sort { input, .. }
		| LogicalPlan::Distinct { input } => first_window_frame(input),
		_ => None,
	}
}

#[tokio::test]
async fn rune_lag_with_literal_default() {
	// lag(score, 2).default(lit_i64(-1)) matches SQL LAG(score, 2, -1): the partition-edge default
	// reaches the IR as a literal ScalarValue (grp a -> [-1,-1,10], grp b -> [-1,-1]).
	let rune = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("wins")
                .window([col("grp")], [col("score").asc()], [lag(col("score"), 2).with_default(lit_i64(-1)).named("l")])
                .project([col("grp"), col("l")])
        }
    "#,
	)
	.await
	.expect("Rune lag-with-default window");
	let sql = sql_plan(
		"SELECT grp, LAG(score, 2, -1) OVER (PARTITION BY grp ORDER BY score) AS l FROM wins",
	)
	.await;
	assert_eq!(
		sorted(run_plan(&rune).await),
		sorted(run_plan(&sql).await),
		"Rune LAG literal default must match SQL"
	);
}

#[tokio::test]
async fn rune_temporal_range_offset_matches_sql_frame() {
	// preceding_value(days(1)) builds the same INTERVAL '1' DAY value-distance RANGE bound as SQL.
	// The projection wrappers differ between frontends, so compare the frame on the Window node.
	let rune = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("tseries")
                .window([], [col("ts").asc()], [sum(col("v")).over().frame(range(preceding_value(days(1)), current_row())).named("s")])
        }
    "#,
	)
	.await
	.expect("Rune temporal-range window");
	let sql = sql_plan(
		"SELECT SUM(v) OVER (ORDER BY ts RANGE BETWEEN INTERVAL '1' DAY PRECEDING AND CURRENT ROW) AS s FROM tseries",
	)
	.await;
	assert_eq!(
		first_window_frame(&rune),
		first_window_frame(&sql),
		"Rune days(1) RANGE frame must equal SQL INTERVAL '1' DAY RANGE frame"
	);
	assert!(
		matches!(
			first_window_frame(&rune).map(|f| f.start),
			Some(WindowFrameBound::Preceding(WindowFrameDelta::Range(
				ScalarValue::Interval(_)
			)))
		),
		"the Rune offset reached the IR as an INTERVAL value distance"
	);
}
