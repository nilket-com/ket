use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arrow::array::{ArrayRef, Decimal128Array, Float64Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use toron::distributed::determinism::{SortPolicy, canonical_arrow_ipc_digest};
use toron::distributed::task::TaskTelemetry;
use toron::error::StorageError;
use toron::exec::{ExecutionContext, Executor};
use toron::ir::{
	AggregateFunction, BoundAggregateCall, BoundBinaryOp, BoundExprProgram,
	BoundExprProgramBuilder, BroadcastReplayEligibility, ColumnName, ColumnOrdinal, ExecutionPlan,
	FilterDisposition, JoinKind, JoinReplayId, JoinStrategy, QueryProfileHint, ResolvedColumn,
	ScalarValue, TableId,
};
use toron::storage::{RowIdentityProjection, ScanRequest, ScanStream, StorageScan};
use wendelon::schema::{
	LocalityPolicy, PrimaryKeyDescriptor, RowIdentityPolicy, SchemaEvolutionPolicy, TableObjectId,
	TableSchemaDescriptor,
};

#[derive(Clone, Copy)]
enum ValueKind {
	Float64,
	Decimal,
}

struct MultiBatchStorage {
	batches: HashMap<TableId, Vec<RecordBatch>>,
}

#[async_trait::async_trait]
impl StorageScan for MultiBatchStorage {
	async fn scan(&self, request: ScanRequest) -> Result<ScanStream, StorageError> {
		let batches =
			self.batches
				.get(&request.table_id)
				.cloned()
				.ok_or(StorageError::TableNotFound {
					table: request.table_id,
				})?;
		let schema = batches
			.first()
			.map(RecordBatch::schema)
			.unwrap_or_else(|| Arc::new(Schema::empty()));
		Ok(ScanStream::from_batches(
			schema,
			request.session_id,
			request.query_id,
			batches,
		))
	}
}

#[tokio::test]
async fn streaming_scan_global_aggregate_matches_materialized_float64_and_decimal_order() {
	for kind in [ValueKind::Float64, ValueKind::Decimal] {
		let (executor, streaming_plan, materialized_plan) = fixture(kind);
		let streamed = executor
			.execute_bound(&streaming_plan, None)
			.await
			.expect("streamed aggregate");
		let materialized = executor
			.execute_bound(&materialized_plan, None)
			.await
			.expect("materialized aggregate");

		assert_eq!(
			canonical_arrow_ipc_digest(&streamed, SortPolicy::None).expect("streamed digest"),
			canonical_arrow_ipc_digest(&materialized, SortPolicy::None)
				.expect("materialized digest")
		);
	}
}

#[tokio::test]
async fn streaming_scan_exchange_grouped_aggregate_matches_materialized_float64_and_decimal_order()
{
	for kind in [ValueKind::Float64, ValueKind::Decimal] {
		let (executor, streaming_plan, materialized_plan) = grouped_fixture(kind);
		let streamed = executor
			.execute_bound(&streaming_plan, None)
			.await
			.expect("streamed grouped aggregate");
		let materialized = executor
			.execute_bound(&materialized_plan, None)
			.await
			.expect("materialized grouped aggregate");

		assert_eq!(
			canonical_arrow_ipc_digest(&streamed, SortPolicy::None).expect("streamed digest"),
			canonical_arrow_ipc_digest(&materialized, SortPolicy::None)
				.expect("materialized digest")
		);
	}
}

#[tokio::test]
async fn streaming_scan_left_outer_grouped_aggregate_matches_materialized_residual_and_null_fill() {
	let (executor, streaming_plan, materialized_plan, _telemetry) =
		left_outer_grouped_fixture(false);
	let streamed = executor
		.execute_bound(&streaming_plan, None)
		.await
		.expect("streamed left outer grouped aggregate");
	let materialized = executor
		.execute_bound(&materialized_plan, None)
		.await
		.expect("materialized left outer grouped aggregate");

	assert_eq!(
		canonical_arrow_ipc_digest(&streamed, SortPolicy::None).expect("streamed digest"),
		canonical_arrow_ipc_digest(&materialized, SortPolicy::None).expect("materialized digest")
	);
}

#[tokio::test]
async fn streaming_scan_left_outer_grouped_aggregate_keeps_all_preserved_columns() {
	let (executor, streaming_plan, materialized_plan, telemetry) = left_outer_grouped_fixture(true);
	let streamed = executor
		.execute_bound(&streaming_plan, None)
		.await
		.expect("streamed all-columns-retained left outer grouped aggregate");
	assert!(
		telemetry
			.lock()
			.expect("task telemetry")
			.counters
			.spill_bytes
			> 0,
		"all-columns-retained alias must enter the streamed-spilled LeftOuter arm"
	);
	let materialized = executor
		.execute_bound(&materialized_plan, None)
		.await
		.expect("materialized all-columns-retained left outer grouped aggregate");

	assert_eq!(
		canonical_arrow_ipc_digest(&streamed, SortPolicy::None).expect("streamed digest"),
		canonical_arrow_ipc_digest(&materialized, SortPolicy::None).expect("materialized digest")
	);
}

#[tokio::test]
async fn streaming_inner_prefix_semi_anti_bare_join_alias_matches_materialized() {
	let (executor, streaming_plan, materialized_plan, telemetry) = inner_prefix_semi_anti_fixture();
	let streamed = executor
		.execute_bound(&streaming_plan, None)
		.await
		.expect("streamed bare intermediate joins");
	assert!(
		telemetry
			.lock()
			.expect("task telemetry")
			.counters
			.spill_bytes
			> 0,
		"bare intermediate aliases must enter the streamed-spilled semi/anti chain"
	);
	let materialized = executor
		.execute_bound(&materialized_plan, None)
		.await
		.expect("materialized bare intermediate joins");

	assert_eq!(
		canonical_arrow_ipc_digest(&streamed, SortPolicy::None).expect("streamed digest"),
		canonical_arrow_ipc_digest(&materialized, SortPolicy::None).expect("materialized digest")
	);
}

fn fixture(kind: ValueKind) -> (Executor, ExecutionPlan, ExecutionPlan) {
	let amount_type = match kind {
		ValueKind::Float64 => DataType::Float64,
		ValueKind::Decimal => DataType::Decimal128(20, 2),
	};
	let schema = Arc::new(Schema::new(vec![
		Field::new("pk", DataType::Int64, false),
		Field::new("amount", amount_type.clone(), true),
		Field::new("keep", DataType::Int64, false),
	]));
	let batch_1 = RecordBatch::try_new(
		schema.clone(),
		vec![
			Arc::new(Int64Array::from(vec![1, 2, 3])),
			amount_batch(kind, &[Some(10_000), Some(100), Some(-10_000)]),
			Arc::new(Int64Array::from(vec![1, 1, 0])),
		],
	)
	.expect("batch 1");
	let batch_2 = RecordBatch::try_new(
		schema.clone(),
		vec![
			Arc::new(Int64Array::from(vec![4, 5, 6])),
			amount_batch(kind, &[Some(200), Some(400), Some(800)]),
			Arc::new(Int64Array::from(vec![1, 0, 1])),
		],
	)
	.expect("batch 2");
	let storage = Arc::new(MultiBatchStorage {
		batches: HashMap::from([(TableId(1), vec![batch_1, batch_2])]),
	});
	let executor = Executor::new(storage);
	let input = ExecutionPlan::Filter {
		input: Box::new(scan_plan(TableId(1), schema)),
		predicate: keep_filter_program(ColumnOrdinal(2)),
		disposition: FilterDisposition::EvaluateInToron,
	};
	let streaming_plan = global_sum_plan(input.clone(), amount_type.clone());
	let materialized_input_schema = input.schema();
	let materialized_plan = global_sum_plan(
		ExecutionPlan::Take {
			input: Box::new(input),
			limit: usize::MAX,
			output_schema: materialized_input_schema,
		},
		amount_type,
	);
	(executor, streaming_plan, materialized_plan)
}

fn grouped_fixture(kind: ValueKind) -> (Executor, ExecutionPlan, ExecutionPlan) {
	let amount_type = match kind {
		ValueKind::Float64 => DataType::Float64,
		ValueKind::Decimal => DataType::Decimal128(20, 2),
	};
	let schema = Arc::new(Schema::new(vec![
		Field::new("group_key", DataType::Int64, false),
		Field::new("amount", amount_type.clone(), true),
		Field::new("keep", DataType::Int64, false),
	]));
	let batch_1 = RecordBatch::try_new(
		schema.clone(),
		vec![
			Arc::new(Int64Array::from(vec![2, 1, 3, 1])),
			amount_batch(
				kind,
				&[
					Some(1_000),
					Some(1_000_000_000_000_000_000),
					Some(50),
					Some(100),
				],
			),
			Arc::new(Int64Array::from(vec![1, 1, 0, 1])),
		],
	)
	.expect("grouped batch 1");
	let batch_2 = RecordBatch::try_new(
		schema.clone(),
		vec![
			Arc::new(Int64Array::from(vec![1, 2, 3, 1])),
			amount_batch(
				kind,
				&[
					Some(200),
					Some(400),
					Some(800),
					Some(-1_000_000_000_000_000_000),
				],
			),
			Arc::new(Int64Array::from(vec![1, 1, 1, 1])),
		],
	)
	.expect("grouped batch 2");
	let storage = Arc::new(MultiBatchStorage {
		batches: HashMap::from([(TableId(2), vec![batch_1, batch_2])]),
	});
	let executor = Executor::new(storage);
	let input = ExecutionPlan::Filter {
		input: Box::new(scan_plan(TableId(2), schema)),
		predicate: keep_filter_program(ColumnOrdinal(2)),
		disposition: FilterDisposition::EvaluateInToron,
	};
	let group_expr = bound_column_root("group_key", ColumnOrdinal(0), DataType::Int64, false);
	let streaming_plan = grouped_aggregate_plan(
		ExecutionPlan::Exchange {
			input: Box::new(input.clone()),
			partition_count: 4,
			key: group_expr.clone(),
		},
		group_expr.clone(),
		amount_type.clone(),
	);
	let materialized_input_schema = input.schema();
	let materialized_plan = grouped_aggregate_plan(
		ExecutionPlan::Exchange {
			input: Box::new(ExecutionPlan::Take {
				input: Box::new(input),
				limit: usize::MAX,
				output_schema: materialized_input_schema,
			}),
			partition_count: 4,
			key: group_expr.clone(),
		},
		group_expr,
		amount_type,
	);
	(executor, streaming_plan, materialized_plan)
}

fn left_outer_grouped_fixture(
	filter_orders_before_join: bool,
) -> (
	Executor,
	ExecutionPlan,
	ExecutionPlan,
	Arc<Mutex<TaskTelemetry>>,
) {
	let customer_schema = Arc::new(Schema::new(vec![Field::new(
		"c_custkey",
		DataType::Int64,
		false,
	)]));
	let orders_schema = Arc::new(Schema::new(vec![
		Field::new("o_orderkey", DataType::Int64, false),
		Field::new("o_custkey", DataType::Int64, false),
		Field::new("keep", DataType::Int64, false),
	]));
	let customer_batch = RecordBatch::try_new(
		customer_schema.clone(),
		vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4]))],
	)
	.expect("customer batch");
	let orders_batch_1 = RecordBatch::try_new(
		orders_schema.clone(),
		vec![
			Arc::new(Int64Array::from(vec![10, 11, 12])),
			Arc::new(Int64Array::from(vec![1, 1, 2])),
			Arc::new(Int64Array::from(vec![1, 1, 0])),
		],
	)
	.expect("orders batch 1");
	let orders_batch_2 = RecordBatch::try_new(
		orders_schema.clone(),
		vec![
			Arc::new(Int64Array::from(vec![13, 14, 15])),
			Arc::new(Int64Array::from(vec![4, 4, 5])),
			Arc::new(Int64Array::from(vec![1, 0, 1])),
		],
	)
	.expect("orders batch 2");
	let storage = Arc::new(MultiBatchStorage {
		batches: HashMap::from([
			(TableId(10), vec![customer_batch]),
			(TableId(11), vec![orders_batch_1, orders_batch_2]),
		]),
	});
	let telemetry = Arc::new(Mutex::new(TaskTelemetry::default()));
	let executor = Executor::with_context(
		storage,
		ExecutionContext::local_test().with_task_telemetry(telemetry.clone()),
	);

	let customer = scan_plan(TableId(10), customer_schema.clone());
	let orders = scan_plan(TableId(11), orders_schema.clone());
	let orders = if filter_orders_before_join {
		ExecutionPlan::Filter {
			input: Box::new(orders),
			predicate: keep_filter_program(ColumnOrdinal(2)),
			disposition: FilterDisposition::EvaluateInToron,
		}
	} else {
		orders
	};
	let materialized_orders = ExecutionPlan::Take {
		input: Box::new(orders.clone()),
		limit: usize::MAX,
		output_schema: orders.schema(),
	};
	let streaming_inner =
		customer_count_by_order_count_plan(customer.clone(), orders, filter_orders_before_join);
	let materialized_inner = customer_count_by_order_count_plan(
		customer,
		materialized_orders,
		filter_orders_before_join,
	);
	let outer_group = bound_column_root("c_count", ColumnOrdinal(1), DataType::Int64, false);
	let streaming_plan = count_distribution_plan(streaming_inner, outer_group.clone());
	let materialized_plan = count_distribution_plan(materialized_inner, outer_group);
	(executor, streaming_plan, materialized_plan, telemetry)
}

fn inner_prefix_semi_anti_fixture() -> (
	Executor,
	ExecutionPlan,
	ExecutionPlan,
	Arc<Mutex<TaskTelemetry>>,
) {
	let scan_schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int64, false)]));
	let batch = RecordBatch::try_new(
		scan_schema.clone(),
		vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
	)
	.expect("chain scan batch");
	let anti_batch = RecordBatch::try_new(
		scan_schema.clone(),
		vec![Arc::new(Int64Array::from(vec![2]))],
	)
	.expect("anti scan batch");
	let storage = Arc::new(MultiBatchStorage {
		batches: HashMap::from([
			(TableId(20), vec![batch.clone()]),
			(TableId(21), vec![batch.clone()]),
			(TableId(22), vec![batch.clone()]),
			(TableId(23), vec![batch.clone()]),
			(TableId(24), vec![batch]),
			(TableId(25), vec![anti_batch]),
		]),
	});
	let telemetry = Arc::new(Mutex::new(TaskTelemetry::default()));
	let executor = Executor::with_context(
		storage,
		ExecutionContext::local_test().with_task_telemetry(telemetry.clone()),
	);
	let build = |streaming_replay_ids: bool| {
		let replay = |id| {
			if streaming_replay_ids {
				JoinReplayId(id)
			} else {
				JoinReplayId::SYNTHETIC
			}
		};
		let scan = |table| scan_plan(table, scan_schema.clone());

		let join0_schema = concat_schema(&scan_schema, &scan_schema);
		let join0 = inner_join_plan(
			replay(0),
			exchange_plan(scan(TableId(20))),
			exchange_plan(scan(TableId(21))),
			join0_schema.clone(),
		);
		let join1_schema = concat_schema(&join0_schema, &scan_schema);
		let join1 = inner_join_plan(
			replay(1),
			exchange_plan(join0),
			exchange_plan(scan(TableId(22))),
			join1_schema.clone(),
		);
		let join2_schema = concat_schema(&join1_schema, &scan_schema);
		let join2 = inner_join_plan(
			replay(2),
			exchange_plan(join1),
			exchange_plan(scan(TableId(23))),
			join2_schema.clone(),
		);
		let residual = equal_columns_program(
			ColumnOrdinal(0),
			ColumnOrdinal(join2_schema.fields().len() as u32),
		);
		let semi = ExecutionPlan::Join {
			replay_id: replay(3),
			replay_eligibility: BroadcastReplayEligibility::Terminal,
			left: Box::new(exchange_plan(join2)),
			right: Box::new(exchange_plan(scan(TableId(24)))),
			kind: JoinKind::LeftSemi,
			left_keys: vec![ColumnOrdinal(0)],
			right_keys: vec![ColumnOrdinal(0)],
			match_filter: Some(residual.clone()),
			null_safe_keys: false,
			strategy: JoinStrategy::HashPartitioned,
			output_schema: join2_schema.clone(),
		};
		let anti = ExecutionPlan::Join {
			replay_id: replay(4),
			replay_eligibility: BroadcastReplayEligibility::Terminal,
			left: Box::new(exchange_plan(semi)),
			right: Box::new(exchange_plan(scan(TableId(25)))),
			kind: JoinKind::LeftAnti,
			left_keys: vec![ColumnOrdinal(0)],
			right_keys: vec![ColumnOrdinal(0)],
			match_filter: Some(residual),
			null_safe_keys: false,
			strategy: JoinStrategy::HashPartitioned,
			output_schema: join2_schema,
		};
		let group_expr = bound_column_root("key", ColumnOrdinal(0), DataType::Int64, false);
		ExecutionPlan::Aggregate {
			input: Box::new(exchange_plan(anti)),
			group_expr,
			aggr_expr: vec![BoundAggregateCall {
				function: AggregateFunction::CountAll,
				args: BoundExprProgramBuilder::new().build(),
			}],
			output_schema: Arc::new(Schema::new(vec![
				Field::new("key", DataType::Int64, false),
				Field::new("count", DataType::Int64, false),
			])),
		}
	};
	(executor, build(true), build(false), telemetry)
}

fn concat_schema(left: &SchemaRef, right: &SchemaRef) -> SchemaRef {
	Arc::new(Schema::new(
		left.fields()
			.iter()
			.chain(right.fields().iter())
			.map(|field| field.as_ref().clone())
			.collect::<Vec<_>>(),
	))
}

fn inner_join_plan(
	replay_id: JoinReplayId,
	left: ExecutionPlan,
	right: ExecutionPlan,
	output_schema: SchemaRef,
) -> ExecutionPlan {
	ExecutionPlan::Join {
		replay_id,
		replay_eligibility: BroadcastReplayEligibility::Terminal,
		left: Box::new(left),
		right: Box::new(right),
		kind: JoinKind::Inner,
		left_keys: vec![ColumnOrdinal(0)],
		right_keys: vec![ColumnOrdinal(0)],
		match_filter: None,
		null_safe_keys: false,
		strategy: JoinStrategy::HashPartitioned,
		output_schema,
	}
}

fn exchange_plan(input: ExecutionPlan) -> ExecutionPlan {
	ExecutionPlan::Exchange {
		input: Box::new(input),
		partition_count: 4,
		key: bound_column_root("key", ColumnOrdinal(0), DataType::Int64, false),
	}
}

fn equal_columns_program(
	left_ordinal: ColumnOrdinal,
	right_ordinal: ColumnOrdinal,
) -> BoundExprProgram {
	let mut builder = BoundExprProgramBuilder::new();
	let left = builder.append_column(ResolvedColumn {
		name: ColumnName("left_key".to_string()),
		ordinal: left_ordinal,
		data_type: DataType::Int64,
		nullable: false,
		pbc_eligible: false,
	});
	let right = builder.append_column(ResolvedColumn {
		name: ColumnName("right_key".to_string()),
		ordinal: right_ordinal,
		data_type: DataType::Int64,
		nullable: false,
		pbc_eligible: false,
	});
	let predicate = builder.append_binary(BoundBinaryOp::Eq, left, right, DataType::Boolean);
	builder.push_root(predicate);
	builder.build()
}

fn customer_count_by_order_count_plan(
	customer: ExecutionPlan,
	orders: ExecutionPlan,
	orders_already_filtered: bool,
) -> ExecutionPlan {
	let join_output_schema = Arc::new(Schema::new(vec![
		Field::new("c_custkey", DataType::Int64, false),
		Field::new("o_orderkey", DataType::Int64, true),
		Field::new("o_custkey", DataType::Int64, true),
		Field::new("keep", DataType::Int64, true),
	]));
	let group_expr = bound_column_root("c_custkey", ColumnOrdinal(0), DataType::Int64, false);
	let join = ExecutionPlan::Join {
		replay_id: JoinReplayId::SYNTHETIC,
		replay_eligibility: BroadcastReplayEligibility::Terminal,
		left: Box::new(customer),
		right: Box::new(orders),
		kind: JoinKind::LeftOuter,
		left_keys: vec![ColumnOrdinal(0)],
		right_keys: vec![ColumnOrdinal(1)],
		match_filter: (!orders_already_filtered).then(|| keep_filter_program(ColumnOrdinal(3))),
		null_safe_keys: false,
		strategy: JoinStrategy::HashPartitioned,
		output_schema: join_output_schema,
	};
	ExecutionPlan::Aggregate {
		input: Box::new(ExecutionPlan::Exchange {
			input: Box::new(join),
			partition_count: 4,
			key: group_expr.clone(),
		}),
		group_expr,
		aggr_expr: vec![BoundAggregateCall {
			function: AggregateFunction::Count,
			args: bound_column_root("o_orderkey", ColumnOrdinal(1), DataType::Int64, true),
		}],
		output_schema: Arc::new(Schema::new(vec![
			Field::new("c_custkey", DataType::Int64, true),
			Field::new("c_count", DataType::Int64, false),
		])),
	}
}

fn count_distribution_plan(input: ExecutionPlan, group_expr: BoundExprProgram) -> ExecutionPlan {
	ExecutionPlan::Aggregate {
		input: Box::new(input),
		group_expr,
		aggr_expr: vec![BoundAggregateCall {
			function: AggregateFunction::CountAll,
			args: BoundExprProgramBuilder::new().build(),
		}],
		output_schema: Arc::new(Schema::new(vec![
			Field::new("c_count", DataType::Int64, true),
			Field::new("custdist", DataType::Int64, false),
		])),
	}
}

fn amount_batch(kind: ValueKind, values: &[Option<i128>]) -> ArrayRef {
	match kind {
		ValueKind::Float64 => Arc::new(Float64Array::from(
			values
				.iter()
				.map(|value| value.map(|value| value as f64 / 100.0))
				.collect::<Vec<_>>(),
		)),
		ValueKind::Decimal => Arc::new(
			Decimal128Array::from(values.to_vec())
				.with_precision_and_scale(20, 2)
				.expect("decimal amount"),
		),
	}
}

fn grouped_aggregate_plan(
	input: ExecutionPlan,
	group_expr: BoundExprProgram,
	amount_type: DataType,
) -> ExecutionPlan {
	ExecutionPlan::Aggregate {
		input: Box::new(input),
		group_expr,
		aggr_expr: vec![
			BoundAggregateCall {
				function: AggregateFunction::Sum,
				args: bound_column_root("amount", ColumnOrdinal(1), amount_type.clone(), true),
			},
			BoundAggregateCall {
				function: AggregateFunction::Avg,
				args: bound_column_root("amount", ColumnOrdinal(1), amount_type.clone(), true),
			},
			BoundAggregateCall {
				function: AggregateFunction::CountAll,
				args: BoundExprProgramBuilder::new().build(),
			},
		],
		output_schema: Arc::new(Schema::new(vec![
			Field::new("group_key", DataType::Int64, true),
			Field::new("sum_amount", amount_type.clone(), true),
			Field::new("avg_amount", amount_type, true),
			Field::new("count_all", DataType::Int64, false),
		])),
	}
}

fn global_sum_plan(input: ExecutionPlan, amount_type: DataType) -> ExecutionPlan {
	ExecutionPlan::Aggregate {
		input: Box::new(input),
		group_expr: BoundExprProgramBuilder::new().build(),
		aggr_expr: vec![BoundAggregateCall {
			function: AggregateFunction::Sum,
			args: bound_column_root("amount", ColumnOrdinal(1), amount_type.clone(), true),
		}],
		output_schema: Arc::new(Schema::new(vec![Field::new(
			"sum_amount",
			amount_type,
			true,
		)])),
	}
}

fn scan_plan(table_id: TableId, schema: SchemaRef) -> ExecutionPlan {
	let descriptor = descriptor(schema.as_ref());
	ExecutionPlan::Scan {
		local_table_id: table_id,
		table: descriptor.table_object_id,
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

fn descriptor(schema: &Schema) -> TableSchemaDescriptor {
	let primary_key = schema.field(0).name().to_string();
	TableSchemaDescriptor::new(
		TableObjectId::new([1; 16]),
		1,
		"default",
		"scan_global_aggregate_test",
		schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single(primary_key)),
		LocalityPolicy::default_table_spread(),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.expect("descriptor")
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

fn keep_filter_program(ordinal: ColumnOrdinal) -> BoundExprProgram {
	let mut builder = BoundExprProgramBuilder::new();
	let keep = builder.append_column(ResolvedColumn {
		name: ColumnName("keep".to_string()),
		ordinal,
		data_type: DataType::Int64,
		nullable: false,
		pbc_eligible: false,
	});
	let zero = builder.append_literal(ScalarValue::Int64(Some(0)));
	let predicate = builder.append_binary(BoundBinaryOp::Gt, keep, zero, DataType::Boolean);
	builder.push_root(predicate);
	builder.build()
}
