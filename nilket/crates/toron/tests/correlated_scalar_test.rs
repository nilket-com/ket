//! ADR 282 — the UNqualified correlated scalar subquery (q17's shape: `part, lineitem` with a
//! `(SELECT 0.2 * AVG(l_quantity) FROM lineitem WHERE l_partkey = p_partkey)` scalar). ADR 282 deferred the
//! bare-correlation resolution to the binder; this review pass closes two gaps it left:
//!   - the comma-join bridging equality must not be stranded above the deferred recombine (else the comma
//!     join executes as a materialized cartesian — the OOM the gauge hit at the default scale); and
//!   - an inner-only / constant residual predicate must be kept (parity with the EXISTS classifier), not
//!     rejected as unsupported.
//!
//! ADR 282's only behavioral coverage was the tpch gauge at a scale with NO matching parts (an empty
//! result), so the resolution was never exercised with data. These tests use a tiny part/lineitem fixture
//! with matching rows.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, Decimal128Array, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use toron::binder::{PlannerConfig, bind};
use toron::catalog::{MockCatalog, TableOptimizerFacts, TableRef};
use toron::distributed::budget::ByteCount;
use toron::error::{BindingError, StorageError};
use toron::exec::Executor;
use toron::ir::{ExecutionPlan, JoinStrategy, TableId};
use toron::sql_planner::SqlPlanner;
use toron::storage::{ScanRequest, ScanStream, StorageScan};
use wendelon::schema::{
	LocalityPolicy, MultiRowLayoutDescriptor, RecordLayoutPolicy, RowIdentityPolicy,
	SchemaEvolutionPolicy, TableObjectId, TableSchemaDescriptor,
};

// ADR 282 review — Finding 1: the comma-join bridging equality must lift into a keyed inner join even when a
// deferred scalar recombine sits between the WHERE filter and the comma-join Cross. Discriminating lever
// (per ADR 270): an oversized build makes a keyless Cross fail loud, but a keyed inner join hash-partitions.
// Pre-fix the equality was stranded above the recombine, the comma join stayed a keyless Cross, and this
// bind failed loud on the oversized `lineitem` build (the cartesian that OOM'd the gauge at scale).
#[tokio::test]
async fn comma_join_under_deferred_scalar_lifts_to_keyed_inner() {
	let catalog = oversized_catalog();
	// q17's shape: `part, lineitem` bridged on partkey, with a bare-correlated scalar over lineitem.
	let logical = SqlPlanner::new()
		.plan(
			"SELECT prt.p_id FROM prt, itm \
			 WHERE prt.p_id = itm.l_pid \
			   AND itm.l_qty < (SELECT AVG(l_qty) FROM itm WHERE l_pid = p_id)",
		)
		.expect("plan comma join + deferred correlated scalar");
	let bound = bind(&logical, &catalog, &small_broadcast_config())
		.await
		.expect(
			"the comma equality lifts under the deferred recombine, so the oversized build \
			 hash-partitions instead of failing loud as a keyless Cross",
		);
	assert!(
		join_strategy_count(&bound, JoinStrategy::HashPartitioned) >= 1,
		"the comma join must lift into a keyed (hash-partitioned) inner join, not a keyless Cross"
	);
}

// ADR 282 review — Finding 1 (discriminating control): the SAME oversized build with a non-equi cross-side
// predicate must still fail loud — proving the bind above succeeds because the *equality* lifted, not
// because the oversized build was tolerated for some unrelated reason.
#[tokio::test]
async fn comma_join_under_deferred_scalar_non_equi_still_fails_loud() {
	let catalog = oversized_catalog();
	let logical = SqlPlanner::new()
		.plan(
			"SELECT prt.p_id FROM prt, itm \
			 WHERE prt.p_id < itm.l_pid \
			   AND itm.l_qty < (SELECT AVG(l_qty) FROM itm WHERE l_pid = p_id)",
		)
		.expect("plan non-equi comma join + deferred correlated scalar");
	let err = bind(&logical, &catalog, &small_broadcast_config())
		.await
		.expect_err("a non-equi comma join stays a keyless Cross; the oversized build fails loud");
	assert!(
		matches!(err, BindingError::UnsupportedExpression { ref message } if message.contains("broadcast-eligible")),
		"expected the keyless-cross broadcast-eligibility failure, got {err:?}"
	);
}

// ADR 282 anti-hollow-green: the deferred correlated scalar resolves and computes correct results over
// matching data (the gauge only ever ran it over an empty result). pk=1: 5.0 < MAX(l_qty for pid 1)=50.0 →
// kept. pk=2: 100.0 < MAX(l_qty for pid 2)=200.0 → kept.
#[tokio::test]
async fn deferred_correlated_scalar_executes_over_matching_data() {
	let batch = run_sql(
		"SELECT prt.p_id FROM prt \
		 WHERE prt.p_lim < (SELECT MAX(l_qty) FROM itm WHERE l_pid = p_id)",
	)
	.await;
	assert_id_values(&batch, &[1, 2]);
}

// ADR 282 review — Finding 2: an inner-only residual (`l_kind = 'A'`) in the deferred scalar's WHERE must be kept
// as an inner residual (parity with the EXISTS classifier), filtering the inner rows before aggregation —
// not rejected as "unsupported correlated scalar subquery predicate". The residual empties pid 2's `'A'`
// group (it has only a `'B'` row), so MAX is NULL and `100.0 < NULL` drops pk=2 — vs the no-residual `{1,2}`
// above. Pre-fix this query failed to bind.
#[tokio::test]
async fn deferred_correlated_scalar_with_inner_residual() {
	let batch = run_sql(
		"SELECT prt.p_id FROM prt \
		 WHERE prt.p_lim < (SELECT MAX(l_qty) FROM itm WHERE l_pid = p_id AND l_kind = 'A')",
	)
	.await;
	assert_id_values(&batch, &[1]);
}

#[tokio::test]
async fn deferred_correlated_scalar_with_multi_table_inner_executes() {
	let batch = run_sql(
		"SELECT prt.p_id FROM prt \
		 WHERE prt.p_lim < ( \
		   SELECT MIN(l_qty) \
		   FROM itm, dim \
		   WHERE l_pid = p_id AND l_pid = d_pid AND d_region = 'EUROPE' \
		 )",
	)
	.await;
	assert_id_values(&batch, &[1]);
}

#[tokio::test]
async fn deferred_correlated_scalar_multi_table_inner_is_keyed() {
	let catalog = oversized_catalog();
	let logical = SqlPlanner::new()
		.plan(
			"SELECT prt.p_id FROM prt \
			 WHERE prt.p_lim < ( \
			   SELECT MIN(l_qty) \
			   FROM itm, dim \
			   WHERE l_pid = p_id AND l_pid = d_pid AND d_region = 'EUROPE' \
			 )",
		)
		.expect("plan deferred scalar with a multi-table inner");
	let bound = bind(&logical, &catalog, &small_broadcast_config())
		.await
		.expect("inner residual equality lifts to a keyed join before binding");
	assert!(
		join_strategy_count(&bound, JoinStrategy::HashPartitioned) >= 2,
		"expected both the scalar recombine and the inner comma join to bind as keyed joins"
	);
}

#[test]
fn multi_table_non_aggregate_scalar_still_rejects() {
	let err = SqlPlanner::new()
		.plan(
			"SELECT prt.p_id FROM prt \
			 WHERE prt.p_lim < ( \
			   SELECT l_qty FROM itm, dim WHERE l_pid = p_id AND l_pid = d_pid \
			 )",
		)
		.expect_err("non-aggregate scalar remains unsupported even with a comma-list inner");
	assert!(
		err.to_string()
			.contains("non-aggregate scalar subqueries are not supported"),
		"unexpected error: {err}"
	);
}

#[tokio::test]
async fn deferred_correlated_scalar_decimal_min_executes() {
	let batch = run_sql(
		"SELECT prt.p_id FROM prt \
		 WHERE prt.p_cost_lim < (SELECT MIN(l_cost) FROM itm WHERE l_pid = p_id)",
	)
	.await;
	assert_id_values(&batch, &[1]);
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

async fn run_sql(sql: &str) -> RecordBatch {
	let (catalog, storage) = fixture();
	let logical = SqlPlanner::new().plan(sql).expect("plan sql");
	let bound = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind sql");
	let executor = Executor::new(storage);
	let batches = executor.execute_bound(&bound, None).await.expect("execute");
	assert_eq!(batches.len(), 1, "single-partition result");
	batches.into_iter().next().expect("batch")
}

fn assert_id_values(batch: &RecordBatch, expected: &[i64]) {
	let values = batch
		.column(0)
		.as_any()
		.downcast_ref::<Int64Array>()
		.expect("id column");
	let mut actual = Vec::with_capacity(values.len());
	for row in 0..values.len() {
		assert!(!values.is_null(row), "id row {row} should not be null");
		actual.push(values.value(row));
	}
	actual.sort_unstable();
	let mut expected = expected.to_vec();
	expected.sort_unstable();
	assert_eq!(actual, expected);
}

fn join_strategy_count(plan: &ExecutionPlan, expected: JoinStrategy) -> usize {
	match plan {
		ExecutionPlan::Join {
			left,
			right,
			strategy,
			..
		} => {
			usize::from(*strategy == expected)
				+ join_strategy_count(left, expected)
				+ join_strategy_count(right, expected)
		}
		ExecutionPlan::Project { input, .. }
		| ExecutionPlan::Filter { input, .. }
		| ExecutionPlan::Aggregate { input, .. }
		| ExecutionPlan::Exchange { input, .. }
		| ExecutionPlan::Sort { input, .. }
		| ExecutionPlan::Take { input, .. }
		| ExecutionPlan::Repeat { input, .. } => join_strategy_count(input, expected),
		ExecutionPlan::Union { left, right, .. } => {
			join_strategy_count(left, expected) + join_strategy_count(right, expected)
		}
		_ => 0,
	}
}

fn fixture() -> (MockCatalog, Arc<FixtureScan>) {
	let prt = prt_schema();
	let itm = itm_schema();
	let dim = dim_schema();
	let mut catalog = MockCatalog::new();
	catalog
		.register_descriptor(TableId(1), descriptor(1, "prt", prt.as_ref()))
		.expect("register prt");
	catalog
		.register_descriptor(TableId(2), descriptor(2, "itm", itm.as_ref()))
		.expect("register itm");
	catalog
		.register_descriptor(TableId(3), descriptor(3, "dim", dim.as_ref()))
		.expect("register dim");
	let storage = Arc::new(FixtureScan {
		batches: HashMap::from([
			(TableId(1), prt_batch(prt)),
			(TableId(2), itm_batch(itm)),
			(TableId(3), dim_batch(dim)),
		]),
	});
	(catalog, storage)
}

/// The same two tables with oversized byte facts seeded — the discriminating lever for the comma-join
/// fold (a keyless Cross over an oversized build fails loud; a keyed inner join hash-partitions).
fn oversized_catalog() -> MockCatalog {
	let (mut catalog, _storage) = fixture();
	seed_byte_upper_bound(&mut catalog, "prt", 64 * 1024 * 1024);
	seed_byte_upper_bound(&mut catalog, "itm", 64 * 1024 * 1024);
	seed_byte_upper_bound(&mut catalog, "dim", 64 * 1024 * 1024);
	catalog
}

fn seed_byte_upper_bound(catalog: &mut MockCatalog, table: &str, bytes: u64) {
	catalog
		.seed_optimizer_facts(
			&TableRef::new(table),
			TableOptimizerFacts {
				byte_upper_bound: Some(ByteCount::new(bytes)),
				..TableOptimizerFacts::default()
			},
		)
		.expect("seed optimizer facts");
}

fn small_broadcast_config() -> PlannerConfig {
	PlannerConfig {
		auto_broadcast_max_build_bytes: Some(ByteCount::new(1024 * 1024)),
		..PlannerConfig::default()
	}
}

fn prt_schema() -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new("p_id", DataType::Int64, false),
		Field::new("p_lim", DataType::Float64, false),
		Field::new("p_cost_lim", DataType::Decimal128(15, 2), false),
	]))
}

fn itm_schema() -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new("l_pid", DataType::Int64, false),
		Field::new("l_qty", DataType::Float64, false),
		Field::new("l_kind", DataType::Utf8, false),
		Field::new("l_cost", DataType::Decimal128(15, 2), false),
	]))
}

fn dim_schema() -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new("d_pid", DataType::Int64, false),
		Field::new("d_region", DataType::Utf8, false),
	]))
}

fn prt_batch(schema: SchemaRef) -> RecordBatch {
	RecordBatch::try_new(
		schema,
		vec![
			Arc::new(Int64Array::from(vec![1, 2])),
			Arc::new(Float64Array::from(vec![5.0, 100.0])),
			Arc::new(
				Decimal128Array::from(vec![Some(900), Some(21_000)])
					.with_precision_and_scale(15, 2)
					.expect("p_cost_lim decimal"),
			),
		],
	)
	.expect("prt batch")
}

fn itm_batch(schema: SchemaRef) -> RecordBatch {
	RecordBatch::try_new(
		schema,
		vec![
			Arc::new(Int64Array::from(vec![1, 1, 2])),
			Arc::new(Float64Array::from(vec![10.0, 50.0, 200.0])),
			Arc::new(StringArray::from(vec!["A", "B", "B"])),
			Arc::new(
				Decimal128Array::from(vec![Some(1_000), Some(5_000), Some(20_000)])
					.with_precision_and_scale(15, 2)
					.expect("l_cost decimal"),
			),
		],
	)
	.expect("itm batch")
}

fn dim_batch(schema: SchemaRef) -> RecordBatch {
	RecordBatch::try_new(
		schema,
		vec![
			Arc::new(Int64Array::from(vec![1, 2])),
			Arc::new(StringArray::from(vec!["EUROPE", "ASIA"])),
		],
	)
	.expect("dim batch")
}

fn descriptor(id: u8, name: &str, schema: &Schema) -> TableSchemaDescriptor {
	// AppendOnly (itm has duplicate l_pid, so no single primary key) requires a MultiRow layout.
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
