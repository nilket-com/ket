use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::array::{Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use toron::binder::PlannerConfig;
use toron::catalog::{DistinctCountFact, MockCatalog, TableOptimizerFacts, TableRef};
use toron::coordinator::QueryCoordinator;
use toron::distributed::budget::ByteCount;
use toron::distributed::identity::QueryId;
use toron::distributed::task::RowCount;
use toron::exec::{ExecutionContext, Executor};
use toron::ir::{ColumnOrdinal, LogicalPlan, QueryProfileHint, SessionId, TableId};
use toron::sql_planner::SqlPlanner;
use toron::storage::{HlcBound, InMemoryTableStorage, StorageWrite, TenantId, WriteRequest};
use wendelon::schema::{
	LocalityPolicy, PrimaryKeyDescriptorV1, RowIdentityPolicy, SchemaEvolutionPolicyV1,
	TableObjectId, TableSchemaDescriptor,
};

const STAR_SQL: &str = "SELECT o.id, c.id, p.id, s.id \
	FROM orders o \
	INNER JOIN customers c ON o.customer_id = c.id \
	INNER JOIN products p ON o.product_id = p.id \
	INNER JOIN stores s ON o.store_id = s.id";

#[tokio::test]
async fn explain_analyze_runs_and_captures_star_join_actuals() {
	let batch = analyze_star_batch().await;

	assert_eq!(batch.schema().field(0).name(), "decision_kind");
	assert_eq!(batch.schema().field(4).name(), "estimated_rows");
	assert_eq!(batch.schema().field(5).name(), "actual_rows");
	assert_eq!(batch.schema().field(6).name(), "ratio");

	let kinds = string_values(&batch, 0);
	let targets = string_values(&batch, 1);
	let details = string_values(&batch, 3);
	let estimated = int64_values(&batch, 4);
	let actual = int64_values(&batch, 5);

	assert_eq!(batch.num_rows(), 3);
	assert!(kinds.iter().all(|kind| kind == "join_order_runtime"));
	assert!(targets.iter().any(|target| target.contains("dimension=c")));
	assert!(targets.iter().any(|target| target.contains("dimension=p")));
	assert!(targets.iter().any(|target| target.contains("dimension=s")));
	assert!(actual.iter().all(|rows| *rows == 4));
	assert!(
		estimated
			.iter()
			.zip(actual.iter())
			.all(|(estimate, actual)| estimate != actual),
		"fixture should expose a measured estimate miss: estimated={estimated:?} actual={actual:?}",
	);
	assert!(details.iter().all(|detail| detail.contains("source=")));
}

#[tokio::test]
async fn measured_vs_estimated_record_carries_both_sides() {
	let batch = analyze_star_batch().await;
	let estimated = int64_values(&batch, 4);
	let actual = int64_values(&batch, 5);
	let ratio = float64_values(&batch, 6);
	let sources = string_values(&batch, 7);
	let build_rows = int64_values(&batch, 8);
	let probe_rows = int64_values(&batch, 9);

	assert_eq!(batch.num_rows(), 3);
	for row in 0..batch.num_rows() {
		assert!(estimated[row] > 0);
		assert!(actual[row] > 0);
		assert!(build_rows[row] > 0);
		assert!(probe_rows[row] > 0);
		assert!(!sources[row].is_empty());
		assert!(
			(ratio[row] - (actual[row] as f64 / estimated[row] as f64)).abs() < f64::EPSILON,
			"ratio should be actual/estimated at row {row}"
		);
	}
}

#[tokio::test]
async fn known_miss_star_runtime_evidence_shows_divergence() {
	let batch = analyze_star_batch().await;
	let estimated = int64_values(&batch, 4);
	let actual = int64_values(&batch, 5);

	assert!(
		estimated
			.iter()
			.zip(actual.iter())
			.all(|(estimate, actual)| estimate != actual),
		"fixture must remain a deterministic estimate miss: estimated={estimated:?} actual={actual:?}"
	);
}

#[tokio::test]
async fn measured_record_correlates_by_replay_id() {
	let batch = analyze_star_batch().await;
	let targets = string_values(&batch, 1);
	let dimensions = targets
		.iter()
		.map(|target| {
			target
				.split("dimension=")
				.nth(1)
				.expect("target carries dimension")
				.to_string()
		})
		.collect::<BTreeSet<_>>();
	let replay_ids = targets
		.iter()
		.map(|target| {
			assert!(target.starts_with("join["));
			let close = target.find(']').expect("target carries replay id");
			target[5..close].parse::<u32>().expect("replay id is u32")
		})
		.collect::<BTreeSet<_>>();

	assert_eq!(batch.num_rows(), 3);
	assert_eq!(
		dimensions,
		BTreeSet::from(["c".to_string(), "p".to_string(), "s".to_string()])
	);
	assert_eq!(replay_ids.len(), 3);
}

#[tokio::test]
async fn cockpit_renders_estimated_vs_actual() {
	let batch = analyze_star_batch().await;
	let schema = batch.schema();
	let fields = schema.fields();
	let field_names = fields.iter().map(|field| field.name()).collect::<Vec<_>>();
	let details = string_values(&batch, 3);

	assert_eq!(
		field_names,
		vec![
			"decision_kind",
			"target",
			"outcome",
			"detail",
			"estimated_rows",
			"actual_rows",
			"ratio",
			"estimate_source",
			"build_rows",
			"probe_rows",
		]
	);
	assert!(details.iter().all(|detail| {
		detail.contains("estimated=")
			&& detail.contains("actual=")
			&& detail.contains("ratio=")
			&& detail.contains("source=")
			&& detail.contains("build_rows=")
			&& detail.contains("probe_rows=")
	}));
}

#[tokio::test]
async fn non_star_join_is_not_captured_by_star_analyze_allowlist() {
	let (catalog, storage) = star_fixture().await;
	let coordinator = coordinator(catalog, storage);
	let logical = SqlPlanner::new()
		.plan(
			"EXPLAIN ANALYZE SELECT o.id, c.id \
			 FROM orders o INNER JOIN customers c ON o.customer_id = c.id",
		)
		.expect("plan non-star EXPLAIN ANALYZE");
	let batch = execute_single_batch(&coordinator, &logical).await;

	assert_eq!(batch.num_rows(), 0);
}

#[tokio::test]
async fn runtime_measurement_is_observational() {
	let (catalog, storage) = star_fixture().await;
	let coordinator = coordinator(catalog, storage);
	let planner = SqlPlanner::new();
	let query = planner.plan(STAR_SQL).expect("plan star query");
	let analyze = planner
		.plan(&format!("EXPLAIN ANALYZE {STAR_SQL}"))
		.expect("plan star analyze");

	let before = execute_single_batch(&coordinator, &query).await;
	let _analysis = execute_single_batch(&coordinator, &analyze).await;
	let after = execute_single_batch(&coordinator, &query).await;

	assert_eq!(
		format!("{:?}", before.schema()),
		format!("{:?}", after.schema())
	);
	assert_eq!(int64_values(&before, 0), int64_values(&after, 0));
}

#[tokio::test]
async fn explain_analyze_was_previously_rejected_now_implemented() {
	let logical = SqlPlanner::new()
		.plan("EXPLAIN ANALYZE SELECT * FROM orders")
		.expect("EXPLAIN ANALYZE should plan");

	assert!(matches!(logical, LogicalPlan::ExplainAnalyze { .. }));
}

async fn analyze_star_batch() -> RecordBatch {
	let (catalog, storage) = star_fixture().await;
	let coordinator = coordinator(catalog, storage);
	let logical = SqlPlanner::new()
		.plan(&format!("EXPLAIN ANALYZE {STAR_SQL}"))
		.expect("plan EXPLAIN ANALYZE");

	assert!(matches!(logical, LogicalPlan::ExplainAnalyze { .. }));
	execute_single_batch(&coordinator, &logical).await
}

async fn execute_single_batch(coordinator: &QueryCoordinator, plan: &LogicalPlan) -> RecordBatch {
	let batches = coordinator
		.execute_query(plan, None)
		.await
		.expect("execute query");
	let schema = batches
		.first()
		.map(RecordBatch::schema)
		.expect("at least one batch");
	arrow::compute::concat_batches(&schema, batches.iter()).expect("concat batches")
}

fn coordinator(catalog: Arc<MockCatalog>, storage: Arc<InMemoryTableStorage>) -> QueryCoordinator {
	QueryCoordinator::new(
		catalog,
		Executor::with_context(
			storage,
			ExecutionContext::new(
				SessionId(1),
				QueryId::from_u128(179),
				TenantId(1),
				HlcBound(1),
			),
		),
		PlannerConfig {
			auto_broadcast_max_build_bytes: Some(ByteCount::new(64 * 1024 * 1024)),
			..PlannerConfig::default()
		},
	)
}

async fn star_fixture() -> (Arc<MockCatalog>, Arc<InMemoryTableStorage>) {
	let mut catalog = MockCatalog::new();
	let storage = Arc::new(InMemoryTableStorage::new());

	let orders = descriptor(
		21,
		"orders",
		Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("customer_id", DataType::Int64, false),
			Field::new("product_id", DataType::Int64, false),
			Field::new("store_id", DataType::Int64, false),
		])),
		"id",
	);
	let customers = single_id_descriptor(22, "customers");
	let products = single_id_descriptor(23, "products");
	let stores = single_id_descriptor(24, "stores");

	catalog
		.register_descriptor(TableId(21), orders.clone())
		.expect("register orders");
	catalog
		.register_descriptor(TableId(22), customers.clone())
		.expect("register customers");
	catalog
		.register_descriptor(TableId(23), products.clone())
		.expect("register products");
	catalog
		.register_descriptor(TableId(24), stores.clone())
		.expect("register stores");

	seed_star_facts(&mut catalog);

	write_table(
		storage.as_ref(),
		TableId(21),
		&orders,
		record_batch(
			orders.arrow_schema().expect("orders schema").into(),
			vec![
				Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
				Arc::new(Int64Array::from(vec![10, 20, 10, 30])),
				Arc::new(Int64Array::from(vec![100, 100, 200, 300])),
				Arc::new(Int64Array::from(vec![1000, 2000, 1000, 3000])),
			],
		),
	)
	.await;
	write_table(
		storage.as_ref(),
		TableId(22),
		&customers,
		id_batch(
			customers.arrow_schema().expect("customers schema").into(),
			&[10, 20, 30],
		),
	)
	.await;
	write_table(
		storage.as_ref(),
		TableId(23),
		&products,
		id_batch(
			products.arrow_schema().expect("products schema").into(),
			&[100, 200, 300],
		),
	)
	.await;
	write_table(
		storage.as_ref(),
		TableId(24),
		&stores,
		id_batch(
			stores.arrow_schema().expect("stores schema").into(),
			&[1000, 2000, 3000],
		),
	)
	.await;

	(Arc::new(catalog), storage)
}

fn seed_star_facts(catalog: &mut MockCatalog) {
	for (table, rows, ndv) in [
		("orders", 100_000, vec![(1, 100), (2, 100), (3, 100)]),
		("customers", 5_000, vec![(0, 100)]),
		("products", 1_000, vec![(0, 100)]),
		("stores", 100, vec![(0, 100)]),
	] {
		catalog
			.seed_optimizer_facts(
				&TableRef::new(table),
				TableOptimizerFacts {
					row_upper_bound: Some(RowCount::new(rows)),
					column_ndv: ndv
						.into_iter()
						.map(|(ordinal, distinct)| {
							(ColumnOrdinal(ordinal), DistinctCountFact::Exact(distinct))
						})
						.collect::<BTreeMap<_, _>>(),
					..TableOptimizerFacts::default()
				},
			)
			.expect("seed optimizer facts");
	}
}

async fn write_table(
	storage: &InMemoryTableStorage,
	table_id: TableId,
	descriptor: &TableSchemaDescriptor,
	payload: RecordBatch,
) {
	storage
		.write(WriteRequest {
			session_id: SessionId(1),
			query_id: QueryId::from_u128(179),
			tenant_id: TenantId(1),
			table_id,
			table_object_id: descriptor.table_object_id,
			physical_descriptor_fingerprint: descriptor.physical_descriptor_fingerprint,
			payload,
			profile_hint: Some(QueryProfileHint::Scan),
			trace_context: None,
		})
		.await
		.expect("write fixture table");
}

fn single_id_descriptor(id: u8, name: &str) -> TableSchemaDescriptor {
	descriptor(
		id,
		name,
		Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
		"id",
	)
}

fn descriptor(id: u8, name: &str, schema: SchemaRef, primary_key: &str) -> TableSchemaDescriptor {
	TableSchemaDescriptor::new(
		TableObjectId::new([id; 16]),
		1,
		"default",
		name,
		schema.as_ref(),
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single(primary_key)),
		LocalityPolicy::default_table_spread(),
		SchemaEvolutionPolicyV1::AdditiveOnly,
	)
	.expect("descriptor")
}

fn id_batch(schema: SchemaRef, values: &[i64]) -> RecordBatch {
	record_batch(schema, vec![Arc::new(Int64Array::from(values.to_vec()))])
}

fn record_batch(schema: SchemaRef, columns: Vec<Arc<dyn arrow::array::Array>>) -> RecordBatch {
	RecordBatch::try_new(schema, columns).expect("record batch")
}

fn string_values(batch: &RecordBatch, column: usize) -> Vec<String> {
	let array = batch
		.column(column)
		.as_any()
		.downcast_ref::<StringArray>()
		.expect("string array");
	(0..array.len())
		.map(|row| array.value(row).to_string())
		.collect()
}

fn int64_values(batch: &RecordBatch, column: usize) -> Vec<i64> {
	let array = batch
		.column(column)
		.as_any()
		.downcast_ref::<Int64Array>()
		.expect("int64 array");
	(0..array.len()).map(|row| array.value(row)).collect()
}

fn float64_values(batch: &RecordBatch, column: usize) -> Vec<f64> {
	let array = batch
		.column(column)
		.as_any()
		.downcast_ref::<arrow::array::Float64Array>()
		.expect("float64 array");
	(0..array.len()).map(|row| array.value(row)).collect()
}
