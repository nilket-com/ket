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
	LocalityPolicy, PrimaryKeyDescriptorV1, RowIdentityPolicy, SchemaEvolutionPolicyV1,
	TableObjectId, TableSchemaDescriptor,
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
fn exists_with_zero_correlation_pairs_is_unsupported() {
	assert_unsupported("SELECT * FROM fact WHERE EXISTS (SELECT 1 FROM dim)");
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
		"SELECT * FROM fact WHERE EXISTS (SELECT DISTINCT dim.k FROM dim WHERE dim.k = fact.k)",
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
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("pk")),
		LocalityPolicy::default_table_spread(),
		SchemaEvolutionPolicyV1::AdditiveOnly,
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
