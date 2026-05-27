use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, Int32Array, Int64Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use toron::error::StorageError;
use toron::exec::Executor;
use toron::ir::{ColumnOrdinal, ExecutionPlan, JoinKind, JoinStrategy, QueryProfileHint, TableId};
use toron::storage::{ScanRequest, ScanStream, StorageScan};
use wendelon::schema::{
	LocalityPolicy, PrimaryKeyDescriptorV1, RowIdentityPolicy, SchemaEvolutionPolicyV1,
	TableObjectId, TableSchemaDescriptor,
};

#[tokio::test]
async fn fast_path_parity_int64() {
	let left_schema = Arc::new(Schema::new(vec![
		Field::new("pk", DataType::Int64, false),
		Field::new("id", DataType::Int64, true),
		Field::new("left_value", DataType::Utf8, true),
	]));
	let right_schema = Arc::new(Schema::new(vec![
		Field::new("pk", DataType::Int64, false),
		Field::new("id", DataType::Int64, true),
		Field::new("right_value", DataType::Int32, true),
	]));
	let left_batch = RecordBatch::try_new(
		left_schema.clone(),
		vec![
			Arc::new(Int64Array::from(vec![10, 11, 12, 13, 14])),
			Arc::new(Int64Array::from(vec![
				Some(1),
				Some(1),
				None,
				Some(2),
				Some(3),
			])),
			Arc::new(StringArray::from(vec![
				Some("a"),
				Some("b"),
				Some("null"),
				Some("c"),
				Some("d"),
			])),
		],
	)
	.expect("left batch");
	let right_batch = RecordBatch::try_new(
		right_schema.clone(),
		vec![
			Arc::new(Int64Array::from(vec![20, 21, 22, 23])),
			Arc::new(Int64Array::from(vec![Some(1), Some(3), None, Some(9)])),
			Arc::new(Int32Array::from(vec![
				Some(10),
				Some(30),
				Some(99),
				Some(90),
			])),
		],
	)
	.expect("right batch");

	let results = run_join(
		left_schema,
		right_schema,
		left_batch,
		right_batch,
		vec![ColumnOrdinal(1)],
		vec![ColumnOrdinal(1)],
	);
	let result = results.await;
	assert_string_values(&result, 2, &["a", "b", "d"]);
	assert_int32_values(&result, 5, &[10, 10, 30]);
}

#[tokio::test]
async fn fast_path_parity_int32() {
	let left_schema = Arc::new(Schema::new(vec![
		Field::new("pk", DataType::Int64, false),
		Field::new("id", DataType::Int32, true),
		Field::new("left_value", DataType::Utf8, true),
	]));
	let right_schema = Arc::new(Schema::new(vec![
		Field::new("pk", DataType::Int64, false),
		Field::new("id", DataType::Int32, true),
		Field::new("right_value", DataType::Int32, true),
	]));
	let left_batch = RecordBatch::try_new(
		left_schema.clone(),
		vec![
			Arc::new(Int64Array::from(vec![10, 11, 12, 13])),
			Arc::new(Int32Array::from(vec![Some(4), Some(5), Some(4), None])),
			Arc::new(StringArray::from(vec![
				Some("a"),
				Some("b"),
				Some("c"),
				Some("null"),
			])),
		],
	)
	.expect("left batch");
	let right_batch = RecordBatch::try_new(
		right_schema.clone(),
		vec![
			Arc::new(Int64Array::from(vec![20, 21, 22])),
			Arc::new(Int32Array::from(vec![Some(4), None, Some(5)])),
			Arc::new(Int32Array::from(vec![Some(40), Some(99), Some(50)])),
		],
	)
	.expect("right batch");

	let result = run_join(
		left_schema,
		right_schema,
		left_batch,
		right_batch,
		vec![ColumnOrdinal(1)],
		vec![ColumnOrdinal(1)],
	)
	.await;
	assert_string_values(&result, 2, &["a", "c", "b"]);
	assert_int32_values(&result, 5, &[40, 40, 50]);
}

#[tokio::test]
async fn fast_path_disabled_for_multi_column_key() {
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
			Arc::new(Int64Array::from(vec![1, 1, 2])),
			Arc::new(Int64Array::from(vec![10, 11, 10])),
			Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("c")])),
		],
	)
	.expect("left batch");
	let right_batch = RecordBatch::try_new(
		right_schema.clone(),
		vec![
			Arc::new(Int64Array::from(vec![20, 21, 22])),
			Arc::new(Int64Array::from(vec![1, 2, 1])),
			Arc::new(Int64Array::from(vec![10, 10, 99])),
			Arc::new(Int32Array::from(vec![Some(100), Some(200), Some(990)])),
		],
	)
	.expect("right batch");

	let result = run_join(
		left_schema,
		right_schema,
		left_batch,
		right_batch,
		vec![ColumnOrdinal(1), ColumnOrdinal(2)],
		vec![ColumnOrdinal(1), ColumnOrdinal(2)],
	)
	.await;
	assert_string_values(&result, 3, &["a", "c"]);
	assert_int32_values(&result, 7, &[100, 200]);
}

#[tokio::test]
async fn fast_path_disabled_for_utf8_key() {
	let left_schema = Arc::new(Schema::new(vec![
		Field::new("pk", DataType::Int64, false),
		Field::new("id", DataType::Utf8, true),
		Field::new("left_value", DataType::Utf8, true),
	]));
	let right_schema = Arc::new(Schema::new(vec![
		Field::new("pk", DataType::Int64, false),
		Field::new("id", DataType::Utf8, true),
		Field::new("right_value", DataType::Int32, true),
	]));
	let left_batch = RecordBatch::try_new(
		left_schema.clone(),
		vec![
			Arc::new(Int64Array::from(vec![10, 11, 12])),
			Arc::new(StringArray::from(vec![Some("x"), None, Some("y")])),
			Arc::new(StringArray::from(vec![Some("a"), Some("null"), Some("b")])),
		],
	)
	.expect("left batch");
	let right_batch = RecordBatch::try_new(
		right_schema.clone(),
		vec![
			Arc::new(Int64Array::from(vec![20, 21, 22])),
			Arc::new(StringArray::from(vec![Some("y"), None, Some("x")])),
			Arc::new(Int32Array::from(vec![Some(20), Some(99), Some(10)])),
		],
	)
	.expect("right batch");

	let result = run_join(
		left_schema,
		right_schema,
		left_batch,
		right_batch,
		vec![ColumnOrdinal(1)],
		vec![ColumnOrdinal(1)],
	)
	.await;
	assert_string_values(&result, 2, &["b", "a"]);
	assert_int32_values(&result, 5, &[20, 10]);
}

#[tokio::test]
async fn fast_path_disabled_for_unsupported_primitive() {
	let left_schema = Arc::new(Schema::new(vec![
		Field::new("pk", DataType::Int64, false),
		Field::new("id", DataType::UInt64, true),
		Field::new("left_value", DataType::Utf8, true),
	]));
	let right_schema = Arc::new(Schema::new(vec![
		Field::new("pk", DataType::Int64, false),
		Field::new("id", DataType::UInt64, true),
		Field::new("right_value", DataType::Int32, true),
	]));
	let left_batch = RecordBatch::try_new(
		left_schema.clone(),
		vec![
			Arc::new(Int64Array::from(vec![10, 11, 12])),
			Arc::new(UInt64Array::from(vec![Some(7), Some(8), None])),
			Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("null")])),
		],
	)
	.expect("left batch");
	let right_batch = RecordBatch::try_new(
		right_schema.clone(),
		vec![
			Arc::new(Int64Array::from(vec![20, 21, 22])),
			Arc::new(UInt64Array::from(vec![Some(8), None, Some(7)])),
			Arc::new(Int32Array::from(vec![Some(80), Some(99), Some(70)])),
		],
	)
	.expect("right batch");

	let result = run_join(
		left_schema,
		right_schema,
		left_batch,
		right_batch,
		vec![ColumnOrdinal(1)],
		vec![ColumnOrdinal(1)],
	)
	.await;
	assert_string_values(&result, 2, &["b", "a"]);
	assert_int32_values(&result, 5, &[80, 70]);
}

async fn run_join(
	left_schema: SchemaRef,
	right_schema: SchemaRef,
	left_batch: RecordBatch,
	right_batch: RecordBatch,
	left_keys: Vec<ColumnOrdinal>,
	right_keys: Vec<ColumnOrdinal>,
) -> RecordBatch {
	let output_schema = join_output_schema(&left_schema, &right_schema);
	let plan = ExecutionPlan::Join {
		left: Box::new(scan_plan(TableId(1), left_schema, 1)),
		right: Box::new(scan_plan(TableId(2), right_schema, 2)),
		kind: JoinKind::Inner,
		left_keys,
		right_keys,
		strategy: JoinStrategy::HashPartitioned,
		output_schema,
	};
	let storage = Arc::new(MultiTableScan {
		batches: HashMap::from([(TableId(1), left_batch), (TableId(2), right_batch)]),
	});
	let results = Executor::new(storage)
		.execute_bound(&plan, None)
		.await
		.expect("join");

	assert_eq!(results.len(), 1);
	results.into_iter().next().expect("result batch")
}

fn join_output_schema(left: &SchemaRef, right: &SchemaRef) -> SchemaRef {
	let fields = left
		.fields()
		.iter()
		.chain(right.fields().iter())
		.cloned()
		.collect::<Vec<_>>();
	Arc::new(Schema::new(fields))
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

fn assert_string_values(batch: &RecordBatch, column: usize, expected: &[&str]) {
	let values = batch
		.column(column)
		.as_any()
		.downcast_ref::<StringArray>()
		.expect("string column");

	assert_eq!(values.len(), expected.len());
	for (row, value) in expected.iter().enumerate() {
		assert_eq!(values.value(row), *value);
	}
}

fn assert_int32_values(batch: &RecordBatch, column: usize, expected: &[i32]) {
	let values = batch
		.column(column)
		.as_any()
		.downcast_ref::<Int32Array>()
		.expect("int32 column");

	assert_eq!(values.values(), expected);
}

struct MultiTableScan {
	batches: HashMap<TableId, RecordBatch>,
}

#[async_trait::async_trait]
impl StorageScan for MultiTableScan {
	async fn scan(&self, request: ScanRequest) -> Result<ScanStream, StorageError> {
		let mut batch =
			self.batches
				.get(&request.table_id)
				.cloned()
				.ok_or(StorageError::TableNotFound {
					table: request.table_id,
				})?;

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
