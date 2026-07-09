use arrow::array::{ArrayRef, Decimal128Array, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::sync::Arc;
use toron::distributed::budget::{BudgetDimension, ByteCount, QueryBudget, TaskBudget};
use toron::error::{ExecutionError, StorageError};
use toron::exec::{ExecutionContext, Executor};
use toron::ir::{
	AggregateFunction, BoundAggregateCall, BoundExprProgram, BoundExprProgramBuilder,
	BroadcastReplayEligibility, ColumnName, ColumnOrdinal, ExecutionPlan, JoinKind, JoinReplayId,
	JoinStrategy, QueryProfileHint, ResolvedColumn, TableId,
};
use toron::storage::{RowIdentityProjection, ScanRequest, ScanStream, StorageScan};
use wendelon::schema::{
	LocalityPolicy, PrimaryKeyDescriptor, RowIdentityPolicy, SchemaEvolutionPolicy, TableObjectId,
	TableSchemaDescriptor,
};

const STREAM_CHUNK_ROWS_UNDER_TEST: usize = 8192;

#[derive(Clone, Copy)]
enum StreamingValueKind {
	Float64,
	Decimal,
}

struct BatchStorage {
	batches: HashMap<TableId, RecordBatch>,
}

#[async_trait::async_trait]
impl StorageScan for BatchStorage {
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

#[tokio::test]
async fn reoriented_inner_chain_handles_right_growing_project_join_shape() {
	let rows = STREAM_CHUNK_ROWS_UNDER_TEST + 257;

	for kind in [StreamingValueKind::Float64, StreamingValueKind::Decimal] {
		let (stream_executor, stream_plan) =
			right_growing_inner_chain_fixture(kind, rows, 0, high_test_budget(), true);
		let (materialized_executor, materialized_plan) =
			right_growing_inner_chain_fixture(kind, rows, 0, high_test_budget(), false);
		let streamed = stream_executor
			.execute_bound(&stream_plan, None)
			.await
			.expect("streamed right-growing chain");
		let materialized = materialized_executor
			.execute_bound(&materialized_plan, None)
			.await
			.expect("materialized right-growing chain");
		assert_record_batches_equal(&streamed, &materialized);
	}
}

#[tokio::test]
async fn reoriented_inner_chain_right_growing_shape_removes_materialized_wall() {
	let rows = STREAM_CHUNK_ROWS_UNDER_TEST * 4 + 2_000;
	let budget = TaskBudget {
		memory: ByteCount::new(512 * 1024),
		spill_bytes: ByteCount::new(256 * 1024 * 1024),
		..TaskBudget::from_query_budget(QueryBudget::test_default())
	};

	let (materialized_executor, materialized_plan) =
		right_growing_inner_chain_fixture(StreamingValueKind::Float64, rows, 256, budget, false);
	let materialized_err = materialized_executor
		.execute_bound(&materialized_plan, None)
		.await
		.expect_err("synthetic replay id should keep the materialized wall");
	assert!(
		matches!(
			materialized_err,
			ExecutionError::BudgetExhausted {
				dimension: BudgetDimension::Memory,
				producer: "join output materialization",
				..
			}
		),
		"unexpected materialized error: {materialized_err:?}"
	);

	let (stream_executor, stream_plan) =
		right_growing_inner_chain_fixture(StreamingValueKind::Float64, rows, 256, budget, true);
	let streamed = stream_executor
		.execute_bound(&stream_plan, None)
		.await
		.expect("right-growing chain should stream past the materialized wall");
	assert!(streamed.iter().map(RecordBatch::num_rows).sum::<usize>() > 0);
}

fn high_test_budget() -> TaskBudget {
	TaskBudget {
		memory: ByteCount::new(128 * 1024 * 1024),
		spill_bytes: ByteCount::new(256 * 1024 * 1024),
		..TaskBudget::from_query_budget(QueryBudget::test_default())
	}
}

fn right_growing_inner_chain_fixture(
	kind: StreamingValueKind,
	rows: usize,
	blob_len: usize,
	budget: TaskBudget,
	streaming_replay_ids: bool,
) -> (Executor, ExecutionPlan) {
	let amount_type = match kind {
		StreamingValueKind::Float64 => DataType::Float64,
		StreamingValueKind::Decimal => DataType::Decimal128(20, 2),
	};
	let dim0_schema = keyed_schema("d0_key", "d0_label");
	let dim1_schema = keyed_schema("d1_key", "d1_label");
	let dim2_schema = keyed_schema("d2_key", "d2_label");
	let fact_schema = Arc::new(Schema::new(vec![
		Field::new("d0_key", DataType::Int64, false),
		Field::new("d1_key", DataType::Int64, false),
		Field::new("d2_key", DataType::Int64, false),
		Field::new("amount", amount_type.clone(), true),
		Field::new("blob", DataType::Utf8, false),
	]));

	let amount: ArrayRef = match kind {
		StreamingValueKind::Float64 => Arc::new(Float64Array::from(
			(0..rows)
				.map(|row| match row % 3 {
					0 => Some(1.0e16),
					1 => Some(1.0),
					_ => Some(-1.0e16),
				})
				.collect::<Vec<_>>(),
		)),
		StreamingValueKind::Decimal => Arc::new(
			Decimal128Array::from(
				(0..rows)
					.map(|row| Some(((row % 13) as i128 + 1) * 100))
					.collect::<Vec<_>>(),
			)
			.with_precision_and_scale(20, 2)
			.expect("decimal amount"),
		),
	};
	let blob = "x".repeat(blob_len);
	let fact_batch = RecordBatch::try_new(
		fact_schema.clone(),
		vec![
			Arc::new(Int64Array::from(
				(0..rows)
					.map(|row| if row % 2 == 0 { 0 } else { 1 })
					.collect::<Vec<_>>(),
			)),
			Arc::new(Int64Array::from(
				(0..rows)
					.map(|row| if row % 3 == 0 { 10 } else { 20 })
					.collect::<Vec<_>>(),
			)),
			Arc::new(Int64Array::from(
				(0..rows)
					.map(|row| if row % 5 == 0 { 100 } else { 200 })
					.collect::<Vec<_>>(),
			)),
			amount,
			Arc::new(StringArray::from(vec![blob.as_str(); rows])),
		],
	)
	.expect("fact batch");
	let storage = Arc::new(BatchStorage {
		batches: HashMap::from([
			(TableId(1), fact_batch),
			(
				TableId(2),
				keyed_batch(dim0_schema.clone(), [0, 1], ["A", "B"]),
			),
			(
				TableId(3),
				keyed_batch(dim1_schema.clone(), [10, 20], ["C", "D"]),
			),
			(
				TableId(4),
				keyed_batch(dim2_schema.clone(), [100, 200], ["E", "F"]),
			),
		]),
	});
	let executor = Executor::with_context(
		storage,
		ExecutionContext::local_test().with_task_budget(budget),
	);
	let replay = |id| {
		if streaming_replay_ids {
			JoinReplayId(id)
		} else {
			JoinReplayId::SYNTHETIC
		}
	};

	let dim0 = exchange_plan(scan(TableId(2), dim0_schema.clone(), 2), "d0_key", 0);
	let fact = exchange_plan(scan(TableId(1), fact_schema.clone(), 1), "d0_key", 0);
	let join0 = inner_join_plan(
		replay(0),
		dim0,
		fact,
		vec![ColumnOrdinal(0)],
		vec![ColumnOrdinal(0)],
		concat_test_schema(&dim0_schema, &fact_schema),
	);
	let mut current = exchange_plan(join0, "d1_key", 3);

	let dim1 = exchange_plan(scan(TableId(3), dim1_schema.clone(), 3), "d1_key", 0);
	let join1_schema = concat_test_schema(&dim1_schema, &current.schema());
	let join1 = inner_join_plan(
		replay(1),
		dim1,
		current,
		vec![ColumnOrdinal(0)],
		vec![ColumnOrdinal(3)],
		join1_schema,
	);
	current = exchange_plan(join1, "d2_key", 6);

	let dim2 = exchange_plan(scan(TableId(4), dim2_schema.clone(), 4), "d2_key", 0);
	let join2_schema = concat_test_schema(&dim2_schema, &current.schema());
	let join2 = inner_join_plan(
		replay(2),
		dim2,
		current,
		vec![ColumnOrdinal(0)],
		vec![ColumnOrdinal(6)],
		join2_schema,
	);

	let project_schema = Arc::new(Schema::new(vec![
		Field::new("d2_label", DataType::Utf8, false),
		Field::new("d1_label", DataType::Utf8, false),
		Field::new("d0_label", DataType::Utf8, false),
		Field::new("amount", amount_type.clone(), true),
	]));
	let projected = ExecutionPlan::Project {
		input: Box::new(join2),
		exprs: bound_columns_program(vec![
			("d2_label", ColumnOrdinal(1), DataType::Utf8, false),
			("d1_label", ColumnOrdinal(3), DataType::Utf8, false),
			("d0_label", ColumnOrdinal(5), DataType::Utf8, false),
			("amount", ColumnOrdinal(9), amount_type.clone(), true),
		]),
		output_schema: project_schema,
	};
	let group_expr = bound_columns_program(vec![
		("d2_label", ColumnOrdinal(0), DataType::Utf8, false),
		("d1_label", ColumnOrdinal(1), DataType::Utf8, false),
		("d0_label", ColumnOrdinal(2), DataType::Utf8, false),
	]);
	let plan = ExecutionPlan::Aggregate {
		input: Box::new(ExecutionPlan::Exchange {
			input: Box::new(projected),
			partition_count: 4,
			key: group_expr.clone(),
		}),
		group_expr,
		aggr_expr: vec![BoundAggregateCall {
			function: AggregateFunction::Sum,
			args: bound_column_root("amount", ColumnOrdinal(3), amount_type.clone(), true),
		}],
		output_schema: Arc::new(Schema::new(vec![
			Field::new("d2_label", DataType::Utf8, false),
			Field::new("d1_label", DataType::Utf8, false),
			Field::new("d0_label", DataType::Utf8, false),
			Field::new("sum", amount_type, true),
		])),
	};
	(executor, plan)
}

fn keyed_schema(key: &str, label: &str) -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new(key, DataType::Int64, false),
		Field::new(label, DataType::Utf8, false),
	]))
}

fn keyed_batch(schema: SchemaRef, keys: [i64; 2], labels: [&str; 2]) -> RecordBatch {
	RecordBatch::try_new(
		schema,
		vec![
			Arc::new(Int64Array::from(keys.to_vec())),
			Arc::new(StringArray::from(labels.to_vec())),
		],
	)
	.expect("keyed batch")
}

fn scan(table_id: TableId, schema: SchemaRef, object_byte: u8) -> ExecutionPlan {
	let primary_key = schema.field(0).name().to_string();
	let descriptor = TableSchemaDescriptor::new(
		TableObjectId::new([object_byte; 16]),
		1,
		"default",
		"streaming_regression",
		schema.as_ref(),
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single(primary_key)),
		LocalityPolicy::default_table_spread(),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.expect("descriptor");
	ExecutionPlan::Scan {
		local_table_id: table_id,
		table: TableObjectId::new([object_byte; 16]),
		fingerprint: descriptor.physical_descriptor_fingerprint,
		schema,
		projection: None,
		pbc_program: None,
		row_identity: RowIdentityProjection::None,
		row_limit: None,
		profile_hint: QueryProfileHint::Scan,
		estimated_byte_size: None,
		primary_key_columns: vec![ColumnOrdinal(0)],
		column_ndv: Vec::new(),
		column_range: Vec::new(),
		column_range_distributions: Vec::new(),
		column_heavy_hitters: Vec::new(),
		column_group_top_k: Default::default(),
		column_group_ndv: Default::default(),
	}
}

fn exchange_plan(input: ExecutionPlan, name: &str, ordinal: u32) -> ExecutionPlan {
	ExecutionPlan::Exchange {
		input: Box::new(input),
		partition_count: 4,
		key: bound_column_root(name, ColumnOrdinal(ordinal), DataType::Int64, false),
	}
}

fn inner_join_plan(
	replay_id: JoinReplayId,
	left: ExecutionPlan,
	right: ExecutionPlan,
	left_keys: Vec<ColumnOrdinal>,
	right_keys: Vec<ColumnOrdinal>,
	output_schema: SchemaRef,
) -> ExecutionPlan {
	ExecutionPlan::Join {
		replay_id,
		replay_eligibility: BroadcastReplayEligibility::Terminal,
		left: Box::new(left),
		right: Box::new(right),
		kind: JoinKind::Inner,
		left_keys,
		right_keys,
		match_filter: None,
		null_safe_keys: false,
		strategy: JoinStrategy::HashPartitioned,
		output_schema,
	}
}

fn concat_test_schema(left: &SchemaRef, right: &SchemaRef) -> SchemaRef {
	Arc::new(Schema::new(
		left.fields()
			.iter()
			.chain(right.fields().iter())
			.map(|field| field.as_ref().clone())
			.collect::<Vec<_>>(),
	))
}

fn bound_column_root(
	name: &str,
	ordinal: ColumnOrdinal,
	data_type: DataType,
	nullable: bool,
) -> BoundExprProgram {
	let mut builder = BoundExprProgramBuilder::new();
	let column = builder.append_column(ResolvedColumn {
		name: ColumnName(name.to_string()),
		ordinal,
		data_type,
		nullable,
		pbc_eligible: false,
	});
	builder.push_root(column);
	builder.build()
}

fn bound_columns_program(columns: Vec<(&str, ColumnOrdinal, DataType, bool)>) -> BoundExprProgram {
	let mut builder = BoundExprProgramBuilder::new();
	for (name, ordinal, data_type, nullable) in columns {
		let column = builder.append_column(ResolvedColumn {
			name: ColumnName(name.to_string()),
			ordinal,
			data_type,
			nullable,
			pbc_eligible: false,
		});
		builder.push_root(column);
	}
	builder.build()
}

fn assert_record_batches_equal(left: &[RecordBatch], right: &[RecordBatch]) {
	let left_schema = left.first().expect("left batches").schema();
	let right_schema = right.first().expect("right batches").schema();
	assert_eq!(left_schema, right_schema);
	let left_batch =
		arrow::compute::concat_batches(&left_schema, left.iter()).expect("left concat");
	let right_batch =
		arrow::compute::concat_batches(&right_schema, right.iter()).expect("right concat");
	assert_eq!(left_batch.num_rows(), right_batch.num_rows());
	assert_eq!(left_batch.num_columns(), right_batch.num_columns());
	for column in 0..left_batch.num_columns() {
		assert_eq!(
			format!("{:?}", left_batch.column(column)),
			format!("{:?}", right_batch.column(column)),
			"column {column} differs"
		);
	}
}
