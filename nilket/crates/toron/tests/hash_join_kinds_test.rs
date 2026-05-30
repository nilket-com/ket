use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use toron::binder::join_schema;
use toron::distributed::budget::{ByteCount, QueryBudget, TaskBudget};
use toron::distributed::determinism::{SortPolicy, canonical_arrow_ipc_digest};
use toron::error::StorageError;
use toron::exec::{ExecutionContext, Executor};
use toron::ir::{
	BroadcastReplayEligibility, ColumnOrdinal, ExecutionPlan, JoinKind, JoinReplayId, JoinStrategy,
	QueryProfileHint, TableId,
};
use toron::storage::{ScanRequest, ScanStream, StorageScan};
use wendelon::schema::{
	LocalityPolicy, PrimaryKeyDescriptorV1, RowIdentityPolicy, SchemaEvolutionPolicyV1,
	TableObjectId, TableSchemaDescriptor,
};

#[tokio::test]
async fn inner_join_preserves_duplicate_multiplicity_and_drops_null_keys() {
	let batch = run_join(
		JoinKind::Inner,
		TaskBudget::from_query_budget(QueryBudget::test_default()),
	)
	.await;

	assert_eq!(batch.num_rows(), 5);
	assert_string_values(
		&batch,
		2,
		&[Some("l1"), Some("l2"), Some("l1"), Some("l2"), Some("l4")],
	);
	assert_int32_values(
		&batch,
		5,
		&[Some(101), Some(101), Some(102), Some(102), Some(104)],
	);
}

#[tokio::test]
async fn left_outer_emits_null_filled_unmatched_left() {
	let batch = run_join(
		JoinKind::LeftOuter,
		TaskBudget::from_query_budget(QueryBudget::test_default()),
	)
	.await;

	assert_eq!(batch.num_rows(), 7);
	assert_string_values(
		&batch,
		2,
		&[
			Some("l1"),
			Some("l2"),
			Some("l1"),
			Some("l2"),
			Some("l4"),
			Some("l3"),
			Some("lnull"),
		],
	);
	assert_int32_values(
		&batch,
		5,
		&[
			Some(101),
			Some(101),
			Some(102),
			Some(102),
			Some(104),
			None,
			None,
		],
	);
	assert_eq!(batch.column(5).null_count(), 2);
}

#[tokio::test]
async fn right_outer_emits_null_filled_unmatched_right() {
	let batch = run_join(
		JoinKind::RightOuter,
		TaskBudget::from_query_budget(QueryBudget::test_default()),
	)
	.await;

	assert_eq!(batch.num_rows(), 7);
	assert_string_values(
		&batch,
		2,
		&[
			Some("l1"),
			Some("l2"),
			Some("l1"),
			Some("l2"),
			None,
			Some("l4"),
			None,
		],
	);
	assert_int32_values(
		&batch,
		5,
		&[
			Some(101),
			Some(101),
			Some(102),
			Some(102),
			Some(103),
			Some(104),
			Some(199),
		],
	);
	assert_eq!(batch.column(2).null_count(), 2);
}

#[tokio::test]
async fn full_outer_emits_union_with_unmatched_both_sides() {
	let batch = run_join(
		JoinKind::FullOuter,
		TaskBudget::from_query_budget(QueryBudget::test_default()),
	)
	.await;

	assert_eq!(batch.num_rows(), 9);
	assert_string_values(
		&batch,
		2,
		&[
			Some("l1"),
			Some("l2"),
			Some("l1"),
			Some("l2"),
			None,
			Some("l4"),
			None,
			Some("l3"),
			Some("lnull"),
		],
	);
	assert_int32_values(
		&batch,
		5,
		&[
			Some(101),
			Some(101),
			Some(102),
			Some(102),
			Some(103),
			Some(104),
			Some(199),
			None,
			None,
		],
	);
	assert_eq!(batch.column(2).null_count(), 2);
	assert_eq!(batch.column(5).null_count(), 2);
}

#[tokio::test]
async fn semi_joins_emit_each_preserved_row_at_most_once() {
	let left = run_join(
		JoinKind::LeftSemi,
		TaskBudget::from_query_budget(QueryBudget::test_default()),
	)
	.await;
	let right = run_join(
		JoinKind::RightSemi,
		TaskBudget::from_query_budget(QueryBudget::test_default()),
	)
	.await;

	assert_eq!(left.num_rows(), 3);
	assert_string_values(&left, 2, &[Some("l1"), Some("l2"), Some("l4")]);
	assert_eq!(right.num_rows(), 3);
	assert_int32_values(&right, 2, &[Some(101), Some(102), Some(104)]);
}

#[tokio::test]
async fn anti_joins_emit_unmatched_preserved_rows_once_and_null_keys_never_match() {
	let left = run_join(
		JoinKind::LeftAnti,
		TaskBudget::from_query_budget(QueryBudget::test_default()),
	)
	.await;
	let right = run_join(
		JoinKind::RightAnti,
		TaskBudget::from_query_budget(QueryBudget::test_default()),
	)
	.await;

	assert_eq!(left.num_rows(), 2);
	assert_string_values(&left, 2, &[Some("l3"), Some("lnull")]);
	assert_eq!(right.num_rows(), 2);
	assert_int32_values(&right, 2, &[Some(103), Some(199)]);
}

#[tokio::test]
async fn composite_key_left_outer_and_left_semi() {
	let (executor, left_schema, right_schema) = composite_fixture();
	let left_outer = join_plan(
		left_schema.clone(),
		right_schema.clone(),
		JoinKind::LeftOuter,
		vec![ColumnOrdinal(1), ColumnOrdinal(2)],
		vec![ColumnOrdinal(1), ColumnOrdinal(2)],
	);
	let left_semi = join_plan(
		left_schema,
		right_schema,
		JoinKind::LeftSemi,
		vec![ColumnOrdinal(1), ColumnOrdinal(2)],
		vec![ColumnOrdinal(1), ColumnOrdinal(2)],
	);

	let outer = executor
		.execute_bound(&left_outer, None)
		.await
		.expect("left outer")
		.remove(0);
	let semi = executor
		.execute_bound(&left_semi, None)
		.await
		.expect("left semi")
		.remove(0);

	assert_eq!(outer.num_rows(), 3);
	assert_string_values(&outer, 3, &[Some("a"), Some("c"), Some("b")]);
	assert_int32_values(&outer, 7, &[Some(100), Some(100), None]);
	assert_eq!(semi.num_rows(), 2);
	assert_string_values(&semi, 3, &[Some("a"), Some("c")]);
}

#[tokio::test]
async fn spilled_hash_join_matches_in_memory_digest_for_all_kinds() {
	let memory_budget = TaskBudget::from_query_budget(QueryBudget::test_default());
	let spilled_budget = TaskBudget {
		memory: ByteCount::new(80 * 1024),
		spill_bytes: ByteCount::new(2 * 1024 * 1024),
		..TaskBudget::from_query_budget(QueryBudget::test_default())
	};

	for kind in all_join_kinds() {
		let memory = run_large_join(kind, memory_budget).await;
		let spilled = run_large_join(kind, spilled_budget).await;
		let memory_digest =
			canonical_arrow_ipc_digest(&[memory], SortPolicy::FullRowLex).expect("memory digest");
		let spilled_digest =
			canonical_arrow_ipc_digest(&[spilled], SortPolicy::FullRowLex).expect("spilled digest");

		assert_eq!(spilled_digest, memory_digest, "{kind:?}");
	}
}

async fn run_join(kind: JoinKind, budget: TaskBudget) -> RecordBatch {
	let left_schema = left_schema();
	let right_schema = right_schema();
	let plan = join_plan(
		left_schema.clone(),
		right_schema.clone(),
		kind,
		vec![ColumnOrdinal(1)],
		vec![ColumnOrdinal(1)],
	);
	let executor = Executor::with_context(
		Arc::new(MultiTableScan {
			batches: HashMap::from([
				(TableId(1), left_batch(left_schema)),
				(TableId(2), right_batch(right_schema)),
			]),
		}),
		ExecutionContext::local_test().with_task_budget(budget),
	);
	let results = executor.execute_bound(&plan, None).await.expect("join");

	assert_eq!(results.len(), 1);
	results.into_iter().next().expect("result batch")
}

async fn run_large_join(kind: JoinKind, budget: TaskBudget) -> RecordBatch {
	let left_schema = Arc::new(Schema::new(vec![
		Field::new("pk", DataType::Int64, false),
		Field::new("id", DataType::Int64, true),
	]));
	let right_schema = Arc::new(Schema::new(vec![
		Field::new("pk", DataType::Int64, false),
		Field::new("id", DataType::Int64, true),
	]));
	let left_rows = 2_000usize;
	let left_batch = RecordBatch::try_new(
		left_schema.clone(),
		vec![
			Arc::new(Int64Array::from_iter_values(
				0..i64::try_from(left_rows).expect("rows fit i64"),
			)),
			Arc::new(Int64Array::from(
				(0..left_rows)
					.map(|row| {
						if row % 11 == 0 {
							None
						} else {
							Some((row % 17) as i64)
						}
					})
					.collect::<Vec<_>>(),
			)),
		],
	)
	.expect("left batch");
	let right_batch = RecordBatch::try_new(
		right_schema.clone(),
		vec![
			Arc::new(Int64Array::from_iter_values(0_i64..32)),
			Arc::new(Int64Array::from(
				(0..32)
					.map(|row| {
						if row == 31 {
							None
						} else {
							Some((row % 19) as i64)
						}
					})
					.collect::<Vec<_>>(),
			)),
		],
	)
	.expect("right batch");
	let plan = join_plan(
		left_schema.clone(),
		right_schema.clone(),
		kind,
		vec![ColumnOrdinal(1)],
		vec![ColumnOrdinal(1)],
	);
	let executor = Executor::with_context(
		Arc::new(MultiTableScan {
			batches: HashMap::from([(TableId(1), left_batch), (TableId(2), right_batch)]),
		}),
		ExecutionContext::local_test().with_task_budget(budget),
	);

	executor
		.execute_bound(&plan, None)
		.await
		.expect("large join")
		.remove(0)
}

fn join_plan(
	left_schema: SchemaRef,
	right_schema: SchemaRef,
	kind: JoinKind,
	left_keys: Vec<ColumnOrdinal>,
	right_keys: Vec<ColumnOrdinal>,
) -> ExecutionPlan {
	ExecutionPlan::Join {
		replay_id: JoinReplayId::SYNTHETIC,
		replay_eligibility: BroadcastReplayEligibility::Terminal,
		left: Box::new(scan_plan(TableId(1), left_schema.clone(), 1)),
		right: Box::new(scan_plan(TableId(2), right_schema.clone(), 2)),
		kind,
		left_keys,
		right_keys,
		strategy: JoinStrategy::HashPartitioned,
		output_schema: join_schema(kind, &left_schema, &right_schema),
	}
}

fn left_schema() -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new("pk", DataType::Int64, false),
		Field::new("id", DataType::Int64, true),
		Field::new("left_value", DataType::Utf8, true),
	]))
}

fn right_schema() -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new("pk", DataType::Int64, false),
		Field::new("id", DataType::Int64, true),
		Field::new("right_value", DataType::Int32, true),
	]))
}

fn left_batch(schema: SchemaRef) -> RecordBatch {
	RecordBatch::try_new(
		schema,
		vec![
			Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
			Arc::new(Int64Array::from(vec![
				Some(1),
				Some(1),
				Some(2),
				Some(4),
				None,
			])),
			Arc::new(StringArray::from(vec![
				Some("l1"),
				Some("l2"),
				Some("l3"),
				Some("l4"),
				Some("lnull"),
			])),
		],
	)
	.expect("left batch")
}

fn right_batch(schema: SchemaRef) -> RecordBatch {
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
			Arc::new(Int32Array::from(vec![
				Some(101),
				Some(102),
				Some(103),
				Some(104),
				Some(199),
			])),
		],
	)
	.expect("right batch")
}

fn composite_fixture() -> (Executor, SchemaRef, SchemaRef) {
	let left_schema = Arc::new(Schema::new(vec![
		Field::new("pk", DataType::Int64, false),
		Field::new("k1", DataType::Int64, false),
		Field::new("k2", DataType::Int64, false),
		Field::new("left_value", DataType::Utf8, true),
	]));
	let right_schema = Arc::new(Schema::new(vec![
		Field::new("pk", DataType::Int64, false),
		Field::new("k1", DataType::Int64, false),
		Field::new("k2", DataType::Int64, false),
		Field::new("right_value", DataType::Int32, true),
	]));
	let left_batch = RecordBatch::try_new(
		left_schema.clone(),
		vec![
			Arc::new(Int64Array::from(vec![1, 2, 3])),
			Arc::new(Int64Array::from(vec![1, 1, 1])),
			Arc::new(Int64Array::from(vec![10, 11, 10])),
			Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("c")])),
		],
	)
	.expect("left composite");
	let right_batch = RecordBatch::try_new(
		right_schema.clone(),
		vec![
			Arc::new(Int64Array::from(vec![20])),
			Arc::new(Int64Array::from(vec![1])),
			Arc::new(Int64Array::from(vec![10])),
			Arc::new(Int32Array::from(vec![Some(100)])),
		],
	)
	.expect("right composite");
	let executor = Executor::new(Arc::new(MultiTableScan {
		batches: HashMap::from([(TableId(1), left_batch), (TableId(2), right_batch)]),
	}));

	(executor, left_schema, right_schema)
}

fn scan_plan(table_id: TableId, schema: SchemaRef, object_byte: u8) -> ExecutionPlan {
	let descriptor = TableSchemaDescriptor::new(
		TableObjectId::new([object_byte; 16]),
		1,
		"default",
		"join_test",
		schema.as_ref(),
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("pk")),
		LocalityPolicy::default_table_spread(),
		SchemaEvolutionPolicyV1::AdditiveOnly,
	)
	.expect("descriptor");

	ExecutionPlan::Scan {
		legacy_table_id: table_id,
		table: TableObjectId::new([object_byte; 16]),
		fingerprint: descriptor.physical_descriptor_fingerprint,
		schema,
		projection: None,
		pbc_program: None,
		row_identity: toron::storage::RowIdentityProjection::None,
		row_limit: None,
		profile_hint: QueryProfileHint::Scan,
		primary_key_columns: vec![ColumnOrdinal(0)],
	}
}

fn assert_string_values(batch: &RecordBatch, column: usize, expected: &[Option<&str>]) {
	let values = batch
		.column(column)
		.as_any()
		.downcast_ref::<StringArray>()
		.expect("string column");

	assert_eq!(values.len(), expected.len());
	for (row, expected) in expected.iter().enumerate() {
		match expected {
			Some(expected) => assert_eq!(values.value(row), *expected),
			None => assert!(values.is_null(row), "row {row} should be null"),
		}
	}
}

fn assert_int32_values(batch: &RecordBatch, column: usize, expected: &[Option<i32>]) {
	let values = batch
		.column(column)
		.as_any()
		.downcast_ref::<Int32Array>()
		.expect("int32 column");

	assert_eq!(values.len(), expected.len());
	for (row, expected) in expected.iter().enumerate() {
		match expected {
			Some(expected) => assert_eq!(values.value(row), *expected),
			None => assert!(values.is_null(row), "row {row} should be null"),
		}
	}
}

fn all_join_kinds() -> Vec<JoinKind> {
	vec![
		JoinKind::Inner,
		JoinKind::LeftOuter,
		JoinKind::RightOuter,
		JoinKind::FullOuter,
		JoinKind::LeftSemi,
		JoinKind::RightSemi,
		JoinKind::LeftAnti,
		JoinKind::RightAnti,
	]
}

struct MultiTableScan {
	batches: HashMap<TableId, RecordBatch>,
}

#[async_trait::async_trait]
impl StorageScan for MultiTableScan {
	async fn scan(&self, request: ScanRequest) -> Result<ScanStream, StorageError> {
		let batch =
			self.batches
				.get(&request.table_id)
				.cloned()
				.ok_or(StorageError::TableNotFound {
					table: request.table_id,
				})?;

		Ok(ScanStream::from_batches(
			batch.schema(),
			request.session_id,
			request.query_id,
			vec![batch],
		))
	}
}
