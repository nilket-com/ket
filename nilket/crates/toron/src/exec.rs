use crate::aggregate_kernel;
use crate::distributed::budget::{BudgetDimension, ByteCount, QueryBudget, TaskBudget};
use crate::distributed::cancellation::DownstreamCancellationContext;
use crate::distributed::task::{
	JoinRuntimeObservation, JoinRuntimeObservationMode, RowCount, TaskTelemetry,
};
use crate::error::ExecutionError;
#[cfg(test)]
use crate::ir::BroadcastReplayEligibility;
use crate::ir::arithmetic_output_type;
use crate::ir::decimal_division_output_type;
use crate::ir::{
	AggregateFunction, BoundAggregateCall, BoundBinaryOp, BoundExprId, BoundExprNode,
	BoundExprProgram, BoundUnaryOp, BoundWindowCall, ColumnOrdinal, ExecutionPlan,
	ExecutionSortKey, FilterDisposition, JoinKind, JoinReplayId, JoinStrategy, MorselId, QueryId,
	QueryProfileHint, ScalarValue, SessionId, SystemTableKind, TableId, TemporalExtractField,
	TemporalTruncField, ValueFunction, WindowFrame, WindowFrameBound, WindowFrameDelta,
	WindowFrameUnit, WindowFunction, nanos_per_timestamp_unit,
};
#[cfg(test)]
use crate::ir::{Expr, LogicalPlan};
use crate::storage::{
	HlcBound, ScanRequest, ScanStream, ScanTablePin, StorageScan, TenantId, TraceContext,
};
use arrow::array::{
	Array, ArrayRef, BooleanArray, BooleanBuilder, Date32Array, Date32Builder, Decimal128Array,
	DurationNanosecondArray, DurationNanosecondBuilder, Float32Array, Float32Builder, Float64Array,
	Float64Builder, Int8Array, Int16Array, Int32Array, Int32Builder, Int64Array, Int64Builder,
	IntervalYearMonthArray, IntervalYearMonthBuilder, StringArray, StringBuilder,
	TimestampMicrosecondArray, TimestampMicrosecondBuilder, TimestampMillisecondArray,
	TimestampMillisecondBuilder, TimestampNanosecondArray, TimestampNanosecondBuilder, UInt8Array,
	UInt16Array, UInt32Array, UInt64Array,
};
use arrow::compute::kernels::boolean::{and, is_not_null, is_null, not, or, or_kleene};
use arrow::compute::kernels::cmp::{eq, gt, gt_eq, lt, lt_eq, neq};
use arrow::compute::kernels::comparison::{
	ilike as arrow_ilike, like as arrow_like, nilike as arrow_nilike, nlike as arrow_nlike,
};
use arrow::compute::kernels::substring::substring_by_char;
use arrow::compute::{
	SortColumn, SortOptions, concat_batches, filter_record_batch, interleave, lexsort_to_indices,
	take,
};
use arrow::datatypes::{DataType, IntervalUnit, Schema, SchemaRef, TimeUnit};
use arrow::ipc::CompressionType;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use aws_lc_rs::aead::{AES_256_GCM_SIV, Aad, LessSafeKey, Nonce, UnboundKey};
use futures::StreamExt;
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::fmt;
use std::io::{Cursor, ErrorKind};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tracing::Instrument;
#[cfg(test)]
use twox_hash::XxHash3_64;
use wendelon::schema::{
	LocalityPolicy, PrimaryKeyDescriptor, RowIdentityPolicy, RuntimeFilterDescriptor,
	SchemaEvolutionPolicy, TableObjectId, TableSchemaDescriptor,
};
#[cfg(test)]
use wendelon::storage::sketch::KllSketch;
use wendelon::storage::{AsyncFileOps, BlockingFileOps, FileHandle, OpenFlags};

/// ADR 255: the safety cap on recursive-CTE fixpoint iterations. Termination is normally delta-empty
/// (the recursive term self-limits via its `WHERE`); this is the hard net so an unbounded recursion
/// errors instead of hanging.
const RECURSIVE_CTE_MAX_ITERATIONS: usize = 1000;

mod aggregate;
mod expr;
mod join;

pub(in crate::exec) use self::aggregate::{
	AggState, BoundGroupedAggregateGroups, add_avg_value, add_sum_value, avg_state_array,
	avg_state_for_data_type, finalize_global_aggregate_state, init_global_aggregate_state,
	max_state_for_data_type, min_max_state_array, min_state_for_data_type, numeric_value,
	sum_state_array, sum_state_for_data_type, update_max_state, update_min_state,
};
#[cfg(test)]
pub(in crate::exec) use self::aggregate::{aggregate_state_keyword, aggregate_state_output_name};
pub(crate) use self::expr::evaluate_cast;
pub(crate) use self::join::partition_bucket;
use self::join::{execute_hash_join, partition_bucket_with_scratch};

pub(crate) mod runtime_filter;

pub struct Executor {
	storage: Arc<dyn StorageScan>,
	context: ExecutionContext,
}

#[derive(Clone)]
pub struct ExecutionContext {
	pub session_id: SessionId,
	pub query_id: QueryId,
	pub tenant_id: TenantId,
	pub read_bound: HlcBound,
	pub profile_hint: Option<QueryProfileHint>,
	pub trace_context: Option<TraceContext>,
	pub legacy_table_pins: HashMap<TableId, ScanTablePin>,
	pub task_budget: TaskBudget,
	pub task_telemetry: Option<Arc<Mutex<TaskTelemetry>>>,
	pub join_runtime_observation_mode: JoinRuntimeObservationMode,
	pub runtime_filter: Option<RuntimeFilterDescriptor>,
	pub downstream_cancellation: Option<DownstreamCancellationContext>,
	pub partition_parallelism: usize,
	join_spill_file_ops: Arc<dyn AsyncFileOps>,
	join_spill_dir: PathBuf,
}

impl fmt::Debug for ExecutionContext {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("ExecutionContext")
			.field("session_id", &self.session_id)
			.field("query_id", &self.query_id)
			.field("tenant_id", &self.tenant_id)
			.field("read_bound", &self.read_bound)
			.field("profile_hint", &self.profile_hint)
			.field("trace_context", &self.trace_context)
			.field("legacy_table_pins", &self.legacy_table_pins)
			.field("task_budget", &self.task_budget)
			.field(
				"join_runtime_observation_mode",
				&self.join_runtime_observation_mode,
			)
			.field("runtime_filter", &self.runtime_filter)
			.field(
				"downstream_cancellation",
				&self.downstream_cancellation.is_some(),
			)
			.field("partition_parallelism", &self.partition_parallelism)
			.field("join_spill_dir", &self.join_spill_dir)
			.finish_non_exhaustive()
	}
}

impl ExecutionContext {
	pub fn new(
		session_id: SessionId,
		query_id: QueryId,
		tenant_id: TenantId,
		read_bound: HlcBound,
	) -> Self {
		Self {
			session_id,
			query_id,
			tenant_id,
			read_bound,
			profile_hint: None,
			trace_context: None,
			legacy_table_pins: HashMap::new(),
			task_budget: TaskBudget::from_query_budget(QueryBudget::test_default()),
			task_telemetry: None,
			join_runtime_observation_mode: JoinRuntimeObservationMode::Disabled,
			runtime_filter: None,
			downstream_cancellation: None,
			partition_parallelism: default_partition_parallelism(),
			join_spill_file_ops: Arc::new(BlockingFileOps),
			join_spill_dir: crate::exec::join::default_join_spill_dir(),
		}
	}

	pub fn with_task_budget(mut self, task_budget: TaskBudget) -> Self {
		self.task_budget = task_budget;
		self
	}

	pub fn with_task_telemetry(mut self, telemetry: Arc<Mutex<TaskTelemetry>>) -> Self {
		self.task_telemetry = Some(telemetry);
		self
	}

	pub fn with_join_runtime_observation_mode(mut self, mode: JoinRuntimeObservationMode) -> Self {
		self.join_runtime_observation_mode = mode;
		self
	}

	pub fn with_runtime_filter(mut self, runtime_filter: Option<RuntimeFilterDescriptor>) -> Self {
		self.runtime_filter = runtime_filter;
		self
	}

	pub fn with_downstream_cancellation(
		mut self,
		downstream_cancellation: Option<DownstreamCancellationContext>,
	) -> Self {
		self.downstream_cancellation = downstream_cancellation;
		self
	}

	pub fn with_partition_parallelism(mut self, partition_parallelism: usize) -> Self {
		self.partition_parallelism = partition_parallelism.max(1);
		self
	}

	pub fn with_join_spill_store(
		mut self,
		file_ops: Arc<dyn AsyncFileOps>,
		spill_dir: PathBuf,
	) -> Self {
		self.join_spill_file_ops = file_ops;
		self.join_spill_dir = spill_dir;
		self
	}

	pub fn local_test() -> Self {
		let mut context = Self::new(
			SessionId(1),
			QueryId::from_u128(1),
			TenantId(1),
			HlcBound(1),
		);
		context
			.legacy_table_pins
			.insert(TableId(1), local_scan_table_pin(TableId(1)));
		context
	}
}

fn default_partition_parallelism() -> usize {
	if let Ok(raw) = std::env::var("TORON_PARTITION_PARALLELISM") {
		if let Ok(value) = raw.parse::<usize>() {
			return value.max(1);
		}
	}
	std::thread::available_parallelism()
		.map(std::num::NonZeroUsize::get)
		.unwrap_or(1)
		.max(1)
}

impl Executor {
	pub fn new(storage: Arc<dyn StorageScan>) -> Self {
		Self::with_context(storage, ExecutionContext::local_test())
	}

	pub fn with_context(storage: Arc<dyn StorageScan>, context: ExecutionContext) -> Self {
		Self { storage, context }
	}

	pub fn context(&self) -> &ExecutionContext {
		&self.context
	}

	#[allow(dead_code)]
	pub(crate) fn storage(&self) -> &dyn StorageScan {
		self.storage.as_ref()
	}

	pub(crate) fn storage_arc(&self) -> Arc<dyn StorageScan> {
		self.storage.clone()
	}

	pub(in crate::exec) fn bound_scan_request(
		&self,
		plan: &ExecutionPlan,
		morsel: Option<(MorselId, u32)>,
	) -> Option<ScanRequest> {
		let ExecutionPlan::Scan {
			local_table_id,
			table,
			fingerprint,
			projection,
			pbc_program,
			row_limit,
			row_identity,
			profile_hint,
			..
		} = plan
		else {
			return None;
		};
		Some(ScanRequest {
			session_id: self.context.session_id,
			query_id: self.context.query_id,
			tenant_id: self.context.tenant_id,
			table_id: *local_table_id,
			table_object_id: *table,
			physical_descriptor_fingerprint: *fingerprint,
			read_bound: self.context.read_bound,
			projected_columns: projection.clone(),
			pbc_predicate: pbc_program.as_ref().map(|program| program.0.clone()),
			runtime_filter: self.context.runtime_filter.clone(),
			row_identity: *row_identity,
			profile_hint: Some(*profile_hint),
			trace_context: self.context.trace_context,
			morsel,
			row_limit: *row_limit,
			downstream_cancellation: self.context.downstream_cancellation.clone(),
		})
	}

	async fn materialize_scan_stream_with_budget(
		&self,
		stream: ScanStream,
		row_limit: Option<usize>,
	) -> Result<Vec<RecordBatch>, ExecutionError> {
		let task_memory_cap = self.context.task_budget.memory.get();
		let memory_cap = (task_memory_cap >= 64 * 1024 * 1024)
			.then(|| join::join_output_materialization_memory_cap(task_memory_cap));
		let schema = stream.schema.clone();
		let mut stream = stream.into_batches();
		let mut rows = 0usize;
		let mut output_bytes = 0u64;
		let mut batches = Vec::new();
		while row_limit.map(|limit| rows < limit).unwrap_or(true) {
			let Some(batch) = stream.next().await else {
				break;
			};
			let batch = batch?;
			let batch = if let Some(limit) = row_limit {
				let remaining = limit - rows;
				if batch.num_rows() > remaining {
					batch.slice(0, remaining)
				} else {
					batch
				}
			} else {
				batch
			};
			rows = rows.saturating_add(batch.num_rows());
			output_bytes = output_bytes.saturating_add(batch.get_array_memory_size() as u64);
			if let Some(memory_cap) = memory_cap
				&& output_bytes > memory_cap
			{
				return Err(join::budget_exhausted(
					BudgetDimension::Memory,
					"scan output accumulator",
					output_bytes,
					memory_cap,
				));
			}
			batches.push(batch);
		}
		if batches.is_empty() {
			batches.push(RecordBatch::new_empty(schema));
		}
		Ok(batches)
	}
	#[cfg(test)]
	fn legacy_scan_table_pin(&self, table: TableId) -> Result<ScanTablePin, ExecutionError> {
		self.context
			.legacy_table_pins
			.get(&table)
			.copied()
			.ok_or_else(|| ExecutionError::InvalidExpression {
				message: format!("unbound logical scan has no pinned descriptor for {table:?}"),
			})
	}

	#[cfg(test)]
	pub fn execute<'b>(
		&'b self,
		plan: &'b LogicalPlan,
		morsel: Option<(MorselId, u32)>,
	) -> std::pin::Pin<
		Box<dyn std::future::Future<Output = Result<Vec<RecordBatch>, ExecutionError>> + Send + 'b>,
	> {
		Box::pin(async move {
			match plan {
				LogicalPlan::Scan {
					table,
					projection: _,
				} => {
					let pin = self.legacy_scan_table_pin(*table)?;
					let req = ScanRequest {
						session_id: self.context.session_id,
						query_id: self.context.query_id,
						tenant_id: self.context.tenant_id,
						table_id: *table,
						table_object_id: pin.table_object_id,
						physical_descriptor_fingerprint: pin.physical_descriptor_fingerprint,
						read_bound: self.context.read_bound,
						projected_columns: None,
						pbc_predicate: None,
						runtime_filter: self.context.runtime_filter.clone(),
						row_identity: crate::storage::RowIdentityProjection::None,
						profile_hint: self.context.profile_hint,
						trace_context: self.context.trace_context,
						morsel,
						row_limit: None,
						downstream_cancellation: self.context.downstream_cancellation.clone(),
					};
					let span = crate::telemetry::storage_scan_span(&req);
					let stream = self.storage.scan(req).instrument(span).await?;
					Ok(crate::coordinator::materialize_scan_stream(stream).await?)
				}
				LogicalPlan::ScanNamed { .. } => Err(ExecutionError::NotImplemented {
					feature: "unbound named scan execution",
				}),
				LogicalPlan::Alias { input, .. } => self.execute(input, morsel).await,
				LogicalPlan::Filter { input, predicate } => {
					let input_batches = self.execute(input, morsel).await?;
					let mut output = Vec::new();
					for batch in input_batches {
						let mask = self.evaluate_expr(predicate, &batch)?;
						let mask_bool =
							mask.as_any()
								.downcast_ref::<BooleanArray>()
								.ok_or_else(|| ExecutionError::InvalidExpression {
									message: "filter predicate must return boolean".to_string(),
								})?;
						let filtered = filter_record_batch(&batch, mask_bool)?;
						output.push(filtered);
					}
					Ok(output)
				}
				LogicalPlan::Project {
					input,
					exprs,
					output_names,
				} => {
					let input_batches = self.execute(input, morsel).await?;
					let mut output = Vec::new();
					for batch in input_batches {
						let mut cols = Vec::new();
						let mut fields = Vec::new();
						for (i, expr) in exprs.iter().enumerate() {
							let array = self.evaluate_expr(expr, &batch)?;
							cols.push(array.clone());
							let field = arrow::datatypes::Field::new(
								output_names
									.get(i)
									.and_then(|name| name.clone())
									.unwrap_or_else(|| format!("col_{i}")),
								array.data_type().clone(),
								true, // Simplified
							);
							fields.push(field);
						}
						let schema = Arc::new(arrow::datatypes::Schema::new(fields));
						let projected = RecordBatch::try_new(schema, cols)?;
						output.push(projected);
					}
					Ok(output)
				}
				LogicalPlan::Union { left, right } => {
					let mut output = self.execute(left, morsel).await?;
					output.extend(self.execute(right, morsel).await?);
					Ok(output)
				}
				LogicalPlan::Distinct { .. } => Err(ExecutionError::NotImplemented {
					feature: "unbound logical DISTINCT execution",
				}),
				LogicalPlan::Aggregate {
					input,
					group_expr,
					aggr_expr,
				} => {
					let input_batches = self.execute(input, morsel).await?;
					if !group_expr.is_empty() {
						return self.grouped_aggregate(&input_batches, group_expr, aggr_expr);
					}

					// Global aggregation mock
					let mut results_cols = Vec::new();
					let mut results_fields = Vec::new();

					for (func, args) in aggr_expr {
						match func {
							AggregateFunction::CountAll => {
								let mut total: i64 = 0;
								for batch in &input_batches {
									total += batch.num_rows() as i64;
								}
								results_cols
									.push(Arc::new(Int64Array::from(vec![total])) as ArrayRef);
								results_fields.push(arrow::datatypes::Field::new(
									"count",
									arrow::datatypes::DataType::Int64,
									false,
								));
							}
							AggregateFunction::Count => {
								let mut total: i64 = 0;
								for batch in &input_batches {
									let arr = self.evaluate_expr(&args[0], batch)?;
									for row in 0..batch.num_rows() {
										if !arr.is_null(row) {
											total += 1;
										}
									}
								}
								results_cols
									.push(Arc::new(Int64Array::from(vec![total])) as ArrayRef);
								results_fields.push(arrow::datatypes::Field::new(
									"count",
									arrow::datatypes::DataType::Int64,
									false,
								));
							}
							AggregateFunction::Sum => {
								let mut state = AggState::SumInt {
									total: 0,
									seen: false,
								};
								for batch in &input_batches {
									let arr = self.evaluate_expr(&args[0], batch)?;
									for row in 0..batch.num_rows() {
										if let Some(value) = numeric_value(&arr, row)? {
											add_sum_value(&mut state, value)?;
										}
									}
								}
								results_cols.push(sum_state_array(&state, &DataType::Int64)?);
								results_fields.push(arrow::datatypes::Field::new(
									"sum",
									arrow::datatypes::DataType::Int64,
									true,
								));
							}
							AggregateFunction::Avg => {
								// ADR 268: derive the AVG accumulator and output type from the observed
								// argument type (so AVG(Decimal) takes the decimal path), instead of
								// hardcoding AvgFloat/Float64. Empty unbound input keeps the historical
								// Float64 default.
								let mut state = None;
								let mut input_type = None;
								for batch in &input_batches {
									let arr = self.evaluate_expr(&args[0], batch)?;
									if state.is_none() {
										state = Some(avg_state_for_data_type(arr.data_type())?);
										input_type = Some(arr.data_type().clone());
									}
									let state = state
										.as_mut()
										.expect("avg state initialized from observed array");
									for row in 0..batch.num_rows() {
										if let Some(value) = numeric_value(&arr, row)? {
											add_avg_value(state, value)?;
										}
									}
								}
								let (state, output_type) = match (state, input_type) {
									(Some(state), Some(input_type)) => {
										let output_type =
											aggregate_kernel::stable_output_data_type(
												&AggregateFunction::Avg,
												Some(&input_type),
											)
											.unwrap_or(arrow::datatypes::DataType::Float64);
										(state, output_type)
									}
									_ => (
										AggState::AvgFloat {
											total: 0.0,
											count: 0,
										},
										arrow::datatypes::DataType::Float64,
									),
								};
								results_cols.push(avg_state_array(&state, &output_type)?);
								results_fields.push(arrow::datatypes::Field::new(
									"avg",
									output_type,
									true,
								));
							}
							AggregateFunction::Min | AggregateFunction::Max => {
								let mut state = None;
								let mut output_type = None;
								for batch in &input_batches {
									let arr = self.evaluate_expr(&args[0], batch)?;
									if state.is_none() {
										state = Some(match func {
											AggregateFunction::Min => {
												min_state_for_data_type(arr.data_type())?
											}
											AggregateFunction::Max => {
												max_state_for_data_type(arr.data_type())?
											}
											_ => unreachable!("MIN/MAX arm only"),
										});
										output_type = Some(arr.data_type().clone());
									}
									let state = state
										.as_mut()
										.expect("MIN/MAX state initialized from observed array");
									for row in 0..batch.num_rows() {
										match func {
											AggregateFunction::Min => {
												update_min_state(state, &arr, row)?
											}
											AggregateFunction::Max => {
												update_max_state(state, &arr, row)?
											}
											_ => unreachable!("MIN/MAX arm only"),
										}
									}
								}
								let state =
									state.ok_or_else(|| ExecutionError::InvalidExpression {
										message: format!(
											"{} cannot infer output type from empty unbound input",
											aggregate_state_keyword(func)
										),
									})?;
								let output_type =
									output_type.expect("MIN/MAX output type observed");
								results_cols.push(min_max_state_array(&state, &output_type)?);
								results_fields.push(arrow::datatypes::Field::new(
									aggregate_state_output_name(func),
									output_type,
									true,
								));
							}
							AggregateFunction::LinearRegress => {
								// Accumulate OLS state: n, sum_x, sum_y, sum_x2, sum_xy
								let mut n = 0u64;
								let mut sum_x = 0.0f64;
								let mut sum_y = 0.0f64;
								let mut sum_x2 = 0.0f64;
								let mut sum_xy = 0.0f64;

								for batch in &input_batches {
									let y_arr = self.evaluate_expr(&args[0], batch)?;
									let x_arr = self.evaluate_expr(&args[1], batch)?;
									let y_f64 = self.cast_to_f64(&y_arr)?;
									let x_f64 = self.cast_to_f64(&x_arr)?;

									for i in 0..batch.num_rows() {
										if !y_f64.is_null(i) && !x_f64.is_null(i) {
											let y = y_f64.value(i);
											let x = x_f64.value(i);
											n += 1;
											sum_x += x;
											sum_y += y;
											sum_x2 += x * x;
											sum_xy += x * y;
										}
									}
								}

								if n < 2 {
									results_cols
										.push(Arc::new(Float64Array::from(vec![None, None]))
											as ArrayRef);
								} else {
									let slope = (n as f64 * sum_xy - sum_x * sum_y)
										/ (n as f64 * sum_x2 - sum_x * sum_x);
									let intercept = (sum_y - slope * sum_x) / n as f64;
									results_cols
										.push(Arc::new(Float64Array::from(vec![slope, intercept]))
											as ArrayRef);
								}
								results_fields.push(arrow::datatypes::Field::new(
									"linreg",
									arrow::datatypes::DataType::Float64,
									true,
								));
							}
							AggregateFunction::ApproxQuantile(q) => {
								let mut sketch =
									KllSketch::new(0x04, wendelon::schema::KllK::new(200), 42)
										.map_err(|e| ExecutionError::Kernel {
											message: e.to_string(),
										})?;

								for batch in &input_batches {
									let arr = self.evaluate_expr(&args[0], batch)?;
									if let Some(int_arr) = arr.as_any().downcast_ref::<Int32Array>()
									{
										for i in 0..int_arr.len() {
											if !int_arr.is_null(i) {
												sketch.insert(
													int_arr.value(i).to_be_bytes().to_vec(),
												);
											}
										}
									}
								}

								// Simple estimate from KLL: sort and pick
								let values = sketch.retained_values();
								if values.is_empty() {
									results_cols
										.push(Arc::new(Float64Array::from(vec![None])) as ArrayRef);
								} else {
									let mut sorted = values.to_vec();
									sorted.sort();
									let idx = ((sorted.len() as f64 * q).round() as usize)
										.min(sorted.len() - 1);
									let val =
										i32::from_be_bytes(sorted[idx].clone().try_into().unwrap())
											as f64;
									results_cols
										.push(Arc::new(Float64Array::from(vec![val])) as ArrayRef);
								}
								results_fields.push(arrow::datatypes::Field::new(
									"quantile",
									arrow::datatypes::DataType::Float64,
									true,
								));
							}
						}
					}

					let schema = Arc::new(arrow::datatypes::Schema::new(results_fields));
					let result_batch = RecordBatch::try_new(schema, results_cols)?;
					Ok(vec![result_batch])
				}
				LogicalPlan::Exchange {
					input,
					partition_count,
					key,
				} => {
					let input_batches = self.execute(input, morsel).await?;
					let mut partitioned_output = Vec::new();

					for batch in input_batches {
						let num_rows = batch.num_rows();
						if num_rows == 0 {
							continue;
						}

						let key_arrays = key
							.iter()
							.map(|expr| self.evaluate_expr(expr, &batch))
							.collect::<Result<Vec<_>, _>>()?;

						let mut builders: Vec<Vec<u32>> =
							(0..*partition_count).map(|_| Vec::new()).collect();
						let mut partition_scratch = Vec::new();
						for i in 0..num_rows {
							let bucket = partition_bucket_with_scratch(
								&key_arrays,
								i,
								*partition_count,
								&mut partition_scratch,
							)?;
							builders[bucket].push(i as u32);
						}

						for indices in builders {
							if !indices.is_empty() {
								let indices_arr = arrow::array::UInt32Array::from(indices);
								let mut cols = Vec::new();
								for i in 0..batch.num_columns() {
									let col = batch.column(i);
									let taken = arrow::compute::take(col, &indices_arr, None)?;
									cols.push(taken);
								}
								let partitioned = RecordBatch::try_new(batch.schema(), cols)?;
								partitioned_output.push(partitioned);
							}
						}
					}
					Ok(partitioned_output)
				}
				LogicalPlan::Window { .. } => Err(ExecutionError::NotImplemented {
					feature: "unbound logical window execution",
				}),
				LogicalPlan::Sort { .. } => Err(ExecutionError::NotImplemented {
					feature: "unbound logical sort execution",
				}),
				LogicalPlan::Take { input, limit } => {
					if *limit == 0 {
						let input_batches = self.execute(input, morsel).await?;
						let schema = input_batches
							.first()
							.map(RecordBatch::schema)
							.unwrap_or_else(|| Arc::new(arrow::datatypes::Schema::empty()));
						return Ok(vec![RecordBatch::new_empty(schema)]);
					}
					let input_batches = self.execute(input, morsel).await?;
					Ok(take_batches(input_batches, *limit))
				}
				LogicalPlan::Join { .. } => Err(ExecutionError::NotImplemented {
					feature: "unbound logical join execution",
				}),
				// ADR 279: the binder resolves DeferredExists into a real join before execution, so this
				// legacy unbound-logical executor never sees it.
				LogicalPlan::DeferredExists { .. } => Err(ExecutionError::NotImplemented {
					feature: "unbound deferred-EXISTS execution",
				}),
				// ADR 282: the binder resolves DeferredScalarSubquery into a real join before execution.
				LogicalPlan::DeferredScalarSubquery { .. } => Err(ExecutionError::NotImplemented {
					feature: "unbound deferred-scalar-subquery execution",
				}),
				LogicalPlan::SetOpAll { .. } => Err(ExecutionError::NotImplemented {
					feature: "unbound logical set-op ALL execution",
				}),
				LogicalPlan::Repeat { .. } => Err(ExecutionError::NotImplemented {
					feature: "unbound logical repeat execution",
				}),
				// ADR 255: recursive CTEs execute only via the bound fixpoint path (`execute_bound`).
				LogicalPlan::RecursiveCte { .. } => Err(ExecutionError::NotImplemented {
					feature: "unbound recursive CTE execution",
				}),
				LogicalPlan::RecursiveRef { .. } => Err(ExecutionError::NotImplemented {
					feature: "unbound recursive reference execution",
				}),
				LogicalPlan::Explain { .. } => Err(ExecutionError::NotImplemented {
					feature: "unbound EXPLAIN execution",
				}),
				LogicalPlan::ExplainAnalyze { .. } => Err(ExecutionError::NotImplemented {
					feature: "unbound EXPLAIN ANALYZE execution",
				}),
				LogicalPlan::CreateTable { .. } => Err(ExecutionError::NotImplemented {
					feature: "unbound CREATE TABLE execution",
				}),
				LogicalPlan::CreateLogicalType { .. } => Err(ExecutionError::NotImplemented {
					feature: "unbound CREATE LOGICAL TYPE execution",
				}),
				LogicalPlan::CreateTableAsSelect { .. } => Err(ExecutionError::NotImplemented {
					feature: "unbound CREATE TABLE AS SELECT execution",
				}),
				LogicalPlan::DropTable { .. } => Err(ExecutionError::NotImplemented {
					feature: "unbound DROP TABLE execution",
				}),
				LogicalPlan::Delete { .. } => Err(ExecutionError::NotImplemented {
					feature: "unbound DELETE execution",
				}),
				LogicalPlan::Update { .. } => Err(ExecutionError::NotImplemented {
					feature: "unbound UPDATE execution",
				}),
				LogicalPlan::Insert { .. } => Err(ExecutionError::NotImplemented {
					feature: "unbound INSERT execution",
				}),
				LogicalPlan::InsertSelect { .. } => Err(ExecutionError::NotImplemented {
					feature: "unbound INSERT INTO ... SELECT execution",
				}),
			}
		})
	}

	pub fn execute_bound<'b>(
		&'b self,
		plan: &'b ExecutionPlan,
		morsel: Option<(MorselId, u32)>,
	) -> std::pin::Pin<
		Box<dyn std::future::Future<Output = Result<Vec<RecordBatch>, ExecutionError>> + Send + 'b>,
	> {
		Box::pin(async move {
			match plan {
				ExecutionPlan::EmptyRelation { schema } => {
					Ok(vec![RecordBatch::new_empty(schema.clone())])
				}
				ExecutionPlan::SystemTable { schema, rows, .. } => {
					Ok(vec![materialize_scalar_rows(schema, rows)?])
				}
				ExecutionPlan::Scan { .. } => {
					let req = self
						.bound_scan_request(plan, morsel)
						.expect("ExecutionPlan::Scan should build a ScanRequest");
					let row_limit = req.row_limit;
					let span = crate::telemetry::storage_scan_span(&req);
					let stream = self.storage.scan(req).instrument(span).await?;
					self.materialize_scan_stream_with_budget(stream, row_limit)
						.await
				}
				ExecutionPlan::Filter {
					input,
					predicate,
					disposition,
				} => {
					if let Some(output) = self
						.try_streaming_join0_filter(input, predicate, *disposition, morsel)
						.await?
					{
						return Ok(output);
					}
					let input_batches = self.execute_bound(input, morsel).await?;
					if *disposition == FilterDisposition::FullyPushed {
						return Ok(input_batches);
					}

					self.filter_batches_with_bound_predicate(
						predicate,
						input_batches,
						"filter predicate",
					)
				}
				ExecutionPlan::Project {
					input,
					exprs,
					output_schema,
				} => {
					let input_batches = self.execute_bound(input, morsel).await?;
					let mut output = Vec::new();
					for batch in input_batches {
						let cols = self.evaluate_bound_program(exprs, &batch)?;
						output.push(RecordBatch::try_new(output_schema.clone(), cols)?);
					}
					Ok(output)
				}
				ExecutionPlan::Union {
					left,
					right,
					output_schema,
				} => {
					let mut output = self.execute_bound(left, morsel).await?;
					output.extend(self.execute_bound(right, morsel).await?);
					if output.is_empty() {
						return Ok(vec![RecordBatch::new_empty(output_schema.clone())]);
					}
					Ok(output)
				}
				// ADR 255: the local `UNION ALL` fixpoint loop. See `execute_recursive_cte`.
				ExecutionPlan::RecursiveCte {
					seed,
					recursive,
					output_schema,
				} => {
					self.execute_recursive_cte(seed, recursive, output_schema, morsel)
						.await
				}
				// ADR 255: the self-reference leaf is always substituted for a `SystemTable` before the
				// recursive term executes, so it is never reached directly.
				ExecutionPlan::RecursiveRef { .. } => Err(ExecutionError::InvalidExpression {
					message:
						"recursive self-reference (RecursiveRef) reached the executor without \
						substitution"
							.to_string(),
				}),
				ExecutionPlan::Repeat {
					input,
					count_column,
					output_schema,
				} => {
					let input_batches = self.execute_bound(input, morsel).await?;
					execute_repeat(
						&input_batches,
						*count_column,
						output_schema,
						self.context.task_budget,
					)
				}
				ExecutionPlan::Aggregate {
					input,
					group_expr,
					aggr_expr,
					output_schema,
				} => {
					if group_expr.is_empty_roots() {
						if let Some(output) = self
							.try_streaming_global_aggregate(input, aggr_expr, output_schema, morsel)
							.await?
						{
							return Ok(output);
						}
						let input_batches = self.execute_bound(input, morsel).await?;
						self.bound_global_aggregate(&input_batches, aggr_expr, output_schema)
					} else {
						if let Some(output) = self
							.try_streaming_grouped_aggregate(
								input,
								group_expr,
								aggr_expr,
								output_schema,
								morsel,
							)
							.await?
						{
							return Ok(output);
						}
						let input_batches = self.execute_bound(input, morsel).await?;
						self.bound_grouped_aggregate(
							&input_batches,
							group_expr,
							aggr_expr,
							output_schema,
						)
					}
				}
				ExecutionPlan::Exchange {
					input,
					partition_count,
					key,
				} => {
					if let Some(output) = self
						.try_streaming_join0_filter_exchange(input, *partition_count, key, morsel)
						.await?
					{
						return Ok(output);
					}
					self.bound_exchange(input, *partition_count, key, morsel)
						.await
				}
				ExecutionPlan::Sort {
					input,
					order_by,
					limit,
					offset,
					output_schema,
				} => {
					Box::pin(self.bound_sort(
						input,
						order_by,
						*limit,
						*offset,
						output_schema,
						morsel,
					))
					.await
				}
				ExecutionPlan::Window {
					input,
					partition_by,
					order_by,
					functions,
					output_schema,
				} => {
					let input_batches = self.execute_bound(input, morsel).await?;
					execute_window(
						input.schema(),
						&input_batches,
						partition_by,
						order_by,
						functions,
						output_schema,
						self.context.task_budget,
					)
				}
				ExecutionPlan::Take {
					input,
					limit,
					output_schema,
				} => {
					if *limit == 0 {
						return Ok(vec![RecordBatch::new_empty(output_schema.clone())]);
					}
					let input_batches = self.execute_bound(input, morsel).await?;
					Ok(take_batches(input_batches, *limit))
				}
				ExecutionPlan::Join {
					replay_id,
					left,
					right,
					kind,
					left_keys,
					right_keys,
					match_filter,
					null_safe_keys,
					strategy,
					output_schema,
					..
				} => {
					let left_batches = self.execute_bound(left, morsel).await?;
					let right_batches = self.execute_bound(right, morsel).await?;
					execute_hash_join(
						left.schema(),
						right.schema(),
						&left_batches,
						&right_batches,
						*kind,
						left_keys,
						right_keys,
						match_filter.as_ref(),
						*null_safe_keys,
						*replay_id,
						*strategy,
						output_schema,
						self.context.task_budget,
						self.context.task_telemetry.clone(),
						self.context.join_runtime_observation_mode.clone(),
						self.context.join_spill_file_ops.clone(),
						self.context.join_spill_dir.clone(),
					)
					.await
				}
				ExecutionPlan::CreateTable { .. } => Err(ExecutionError::NotImplemented {
					feature: "CREATE TABLE execution is coordinator-only",
				}),
				ExecutionPlan::CreateLogicalType { .. } => Err(ExecutionError::NotImplemented {
					feature: "CREATE LOGICAL TYPE execution is coordinator-only",
				}),
				ExecutionPlan::CreateTableAsSelect { .. } => Err(ExecutionError::NotImplemented {
					feature: "CREATE TABLE AS SELECT execution is coordinator-only",
				}),
				ExecutionPlan::DropTable { .. } => Err(ExecutionError::NotImplemented {
					feature: "DROP TABLE execution is coordinator-only",
				}),
				ExecutionPlan::Delete { .. } => Err(ExecutionError::NotImplemented {
					feature: "DELETE execution is coordinator-only",
				}),
				ExecutionPlan::Update { .. } => Err(ExecutionError::NotImplemented {
					feature: "UPDATE execution is coordinator-only",
				}),
				ExecutionPlan::Insert { .. } => Err(ExecutionError::NotImplemented {
					feature: "INSERT execution is coordinator-only",
				}),
				ExecutionPlan::InsertSelect { .. } => Err(ExecutionError::NotImplemented {
					feature: "INSERT INTO ... SELECT execution is coordinator-only",
				}),
			}
		})
	}

	/// ADR 255: the local `UNION ALL` recursive-CTE fixpoint (semi-naive). Each iteration substitutes the
	/// prior pass's rows (the *working table*) for the `RecursiveRef` leaves in the recursive term — via
	/// the existing `SystemTable` in-memory relation — re-executes, and appends. Termination is
	/// delta-empty (the recursive term self-limits via its `WHERE`) or the iteration cap.
	async fn execute_recursive_cte(
		&self,
		seed: &ExecutionPlan,
		recursive: &ExecutionPlan,
		output_schema: &SchemaRef,
		morsel: Option<(MorselId, u32)>,
	) -> Result<Vec<RecordBatch>, ExecutionError> {
		let mut working = self.execute_bound(seed, morsel).await?;
		let mut result = working.clone();
		for _ in 0..RECURSIVE_CTE_MAX_ITERATIONS {
			if batches_total_rows(&working) == 0 {
				return Ok(finalize_recursive_result(result, output_schema));
			}
			let working_rows = batches_to_scalar_rows(&working, output_schema)?;
			let injected = substitute_recursive_ref(recursive, &working_rows, output_schema);
			let next = self.execute_bound(&injected, morsel).await?;
			if batches_total_rows(&next) == 0 {
				return Ok(finalize_recursive_result(result, output_schema));
			}
			result.extend(next.clone());
			working = next;
		}
		Err(ExecutionError::InvalidExpression {
			message: format!(
				"recursive query did not terminate within {RECURSIVE_CTE_MAX_ITERATIONS} iterations"
			),
		})
	}
}

impl Executor {
	async fn bound_exchange(
		&self,
		input: &ExecutionPlan,
		partition_count: u32,
		key: &BoundExprProgram,
		morsel: Option<(MorselId, u32)>,
	) -> Result<Vec<RecordBatch>, ExecutionError> {
		if partition_count == 0 {
			return Err(ExecutionError::InvalidExpression {
				message: "exchange partition_count must be greater than zero".to_string(),
			});
		}

		let input_batches = self.execute_bound(input, morsel).await?;
		let schema = input.schema();
		let partition_count_usize =
			usize::try_from(partition_count).expect("partition count fits usize");
		let mut partition_batches: Vec<Vec<RecordBatch>> =
			(0..partition_count_usize).map(|_| Vec::new()).collect();

		for batch in input_batches {
			for (partition, partitioned) in
				self.repartition_exchange_batch(&batch, partition_count, key)?
			{
				partition_batches[partition].push(partitioned);
			}
		}

		finalize_exchange_partitions(schema, partition_batches)
	}

	fn repartition_exchange_batch(
		&self,
		batch: &RecordBatch,
		partition_count: u32,
		key: &BoundExprProgram,
	) -> Result<Vec<(usize, RecordBatch)>, ExecutionError> {
		let num_rows = batch.num_rows();
		if num_rows == 0 {
			return Ok(Vec::new());
		}

		let key_arrays = self.evaluate_bound_program(key, batch)?;

		let mut builders: Vec<Vec<u32>> = (0..partition_count).map(|_| Vec::new()).collect();
		let mut partition_scratch = Vec::new();
		for row in 0..num_rows {
			let bucket = partition_bucket_with_scratch(
				&key_arrays,
				row,
				partition_count,
				&mut partition_scratch,
			)?;
			builders[bucket].push(row as u32);
		}

		let mut partitioned = Vec::new();
		for (partition, indices) in builders.into_iter().enumerate() {
			if !indices.is_empty() {
				let indices_arr = arrow::array::UInt32Array::from(indices);
				let mut cols = Vec::new();
				for col in batch.columns() {
					cols.push(arrow::compute::take(col, &indices_arr, None)?);
				}
				partitioned.push((partition, RecordBatch::try_new(batch.schema(), cols)?));
			}
		}
		Ok(partitioned)
	}
}

fn finalize_exchange_partitions(
	schema: arrow::datatypes::SchemaRef,
	partition_batches: Vec<Vec<RecordBatch>>,
) -> Result<Vec<RecordBatch>, ExecutionError> {
	partition_batches
		.into_iter()
		.map(|batches| {
			if batches.is_empty() {
				Ok(RecordBatch::new_empty(schema.clone()))
			} else {
				Ok(arrow::compute::concat_batches(&schema, &batches)?)
			}
		})
		.collect()
}

fn take_batches(batches: Vec<RecordBatch>, limit: usize) -> Vec<RecordBatch> {
	let mut rows = 0usize;
	let mut output = Vec::new();
	for batch in batches {
		if rows >= limit {
			break;
		}
		if batch.num_rows() == 0 {
			continue;
		}
		let remaining = limit - rows;
		if batch.num_rows() <= remaining {
			rows += batch.num_rows();
			output.push(batch);
		} else {
			output.push(batch.slice(0, remaining));
			break;
		}
	}
	output
}

/// ADR 255: total row count across a set of batches (delta-empty termination test).
fn batches_total_rows(batches: &[RecordBatch]) -> usize {
	batches.iter().map(RecordBatch::num_rows).sum()
}

/// ADR 255: the result-finalizer — an empty result still yields one empty batch carrying the schema
/// (mirrors the `Union` arm), so downstream operators see the correct column layout.
fn finalize_recursive_result(
	result: Vec<RecordBatch>,
	output_schema: &SchemaRef,
) -> Vec<RecordBatch> {
	if batches_total_rows(&result) == 0 {
		vec![RecordBatch::new_empty(output_schema.clone())]
	} else {
		result
	}
}

/// ADR 255: clone the recursive term, replacing every `RecursiveRef` leaf with a `SystemTable` built
/// from the working table's rows. The first-cut recursive shape is Filter/Project over a `RecursiveRef`,
/// so only those node kinds recurse; everything else is a pass-through clone (the leaf is the only thing
/// substituted, so deeper structure is irrelevant — but cloning keeps the helper total).
fn substitute_recursive_ref(
	plan: &ExecutionPlan,
	working_rows: &[Vec<ScalarValue>],
	output_schema: &SchemaRef,
) -> ExecutionPlan {
	match plan {
		ExecutionPlan::RecursiveRef { .. } => ExecutionPlan::SystemTable {
			// `OptimizerExplain` is a neutral, side-effect-free in-memory kind; the executor's
			// `SystemTable` arm reads only `schema`/`rows`, never `kind`.
			kind: SystemTableKind::OptimizerExplain,
			schema: output_schema.clone(),
			rows: working_rows.to_vec(),
		},
		ExecutionPlan::Filter {
			input,
			predicate,
			disposition,
		} => ExecutionPlan::Filter {
			input: Box::new(substitute_recursive_ref(input, working_rows, output_schema)),
			predicate: predicate.clone(),
			disposition: *disposition,
		},
		ExecutionPlan::Project {
			input,
			exprs,
			output_schema: project_schema,
		} => ExecutionPlan::Project {
			input: Box::new(substitute_recursive_ref(input, working_rows, output_schema)),
			exprs: exprs.clone(),
			output_schema: project_schema.clone(),
		},
		other => other.clone(),
	}
}

/// ADR 255: convert a working table's batches to scalar rows that round-trip through
/// `materialize_scalar_rows` (the `SystemTable` execution path). Inverts `system_table_array` exactly so
/// the supported type set matches; an unsupported column type is an error rather than a silent drop.
fn batches_to_scalar_rows(
	batches: &[RecordBatch],
	schema: &SchemaRef,
) -> Result<Vec<Vec<ScalarValue>>, ExecutionError> {
	let total = batches_total_rows(batches);
	let mut rows: Vec<Vec<ScalarValue>> = (0..total)
		.map(|_| Vec::with_capacity(schema.fields().len()))
		.collect();
	let mut row_base = 0;
	for batch in batches {
		for (col_index, field) in schema.fields().iter().enumerate() {
			let array = batch.column(col_index);
			for row in 0..batch.num_rows() {
				let value = scalar_value_from_array(array, row, field.data_type(), field.name())?;
				rows[row_base + row].push(value);
			}
		}
		row_base += batch.num_rows();
	}
	Ok(rows)
}

/// ADR 255: read one cell of an arrow array as a `ScalarValue`, covering exactly the type set that
/// `system_table_array` builds (so the SystemTable round-trip is lossless).
fn scalar_value_from_array(
	array: &ArrayRef,
	row: usize,
	data_type: &DataType,
	field_name: &str,
) -> Result<ScalarValue, ExecutionError> {
	macro_rules! downcast {
		($ty:ty) => {
			array.as_any().downcast_ref::<$ty>().ok_or_else(|| {
				ExecutionError::InvalidExpression {
					message: format!(
						"recursive working table column `{field_name}` is not the expected {} array",
						stringify!($ty)
					),
				}
			})?
		};
	}
	let is_null = array.is_null(row);
	let value = match data_type {
		DataType::Boolean => {
			let arr = downcast!(BooleanArray);
			ScalarValue::Boolean(if is_null { None } else { Some(arr.value(row)) })
		}
		DataType::Int32 => {
			let arr = downcast!(Int32Array);
			ScalarValue::Int32(if is_null { None } else { Some(arr.value(row)) })
		}
		DataType::Int64 => {
			let arr = downcast!(Int64Array);
			ScalarValue::Int64(if is_null { None } else { Some(arr.value(row)) })
		}
		DataType::Date32 => {
			let arr = downcast!(Date32Array);
			ScalarValue::Date32(if is_null { None } else { Some(arr.value(row)) })
		}
		DataType::Timestamp(unit, Some(timezone)) if timezone.as_ref() == "UTC" => {
			let value = if is_null {
				None
			} else {
				Some(match unit {
					TimeUnit::Millisecond => downcast!(TimestampMillisecondArray).value(row),
					TimeUnit::Microsecond => downcast!(TimestampMicrosecondArray).value(row),
					TimeUnit::Nanosecond => downcast!(TimestampNanosecondArray).value(row),
					TimeUnit::Second => {
						return Err(ExecutionError::InvalidExpression {
							message: format!(
								"recursive working table column `{field_name}` has unsupported \
								 Timestamp(Second) type"
							),
						});
					}
				})
			};
			ScalarValue::Timestamp { value, unit: *unit }
		}
		DataType::Duration(TimeUnit::Nanosecond) => {
			let arr = downcast!(DurationNanosecondArray);
			ScalarValue::Interval(if is_null { None } else { Some(arr.value(row)) })
		}
		DataType::Interval(IntervalUnit::YearMonth) => {
			let arr = downcast!(IntervalYearMonthArray);
			ScalarValue::CalendarMonthInterval(if is_null { None } else { Some(arr.value(row)) })
		}
		DataType::Float32 => {
			let arr = downcast!(Float32Array);
			ScalarValue::Float32(if is_null { None } else { Some(arr.value(row)) })
		}
		DataType::Float64 => {
			let arr = downcast!(Float64Array);
			ScalarValue::Float64(if is_null { None } else { Some(arr.value(row)) })
		}
		DataType::Utf8 => {
			let arr = downcast!(StringArray);
			ScalarValue::Utf8(if is_null {
				None
			} else {
				Some(arr.value(row).to_string())
			})
		}
		other => {
			return Err(ExecutionError::InvalidExpression {
				message: format!(
					"recursive working table column `{field_name}` has unsupported type {other:?}"
				),
			});
		}
	};
	Ok(value)
}

pub(crate) fn materialize_scalar_rows(
	schema: &SchemaRef,
	rows: &[Vec<ScalarValue>],
) -> Result<RecordBatch, ExecutionError> {
	for (row_index, row) in rows.iter().enumerate() {
		if row.len() != schema.fields().len() {
			return Err(ExecutionError::InvalidExpression {
				message: format!(
					"system table row {} has {} values; expected {}",
					row_index + 1,
					row.len(),
					schema.fields().len()
				),
			});
		}
	}

	let mut columns = Vec::with_capacity(schema.fields().len());
	for (field_index, field) in schema.fields().iter().enumerate() {
		columns.push(system_table_array(
			field.data_type(),
			rows,
			field_index,
			field.name(),
		)?);
	}
	if columns.is_empty() {
		let options = RecordBatchOptions::new().with_row_count(Some(rows.len()));
		Ok(RecordBatch::try_new_with_options(
			schema.clone(),
			columns,
			&options,
		)?)
	} else {
		Ok(RecordBatch::try_new(schema.clone(), columns)?)
	}
}

fn system_table_array(
	data_type: &DataType,
	rows: &[Vec<ScalarValue>],
	field_index: usize,
	field_name: &str,
) -> Result<ArrayRef, ExecutionError> {
	match data_type {
		DataType::Boolean => {
			let mut builder = BooleanBuilder::with_capacity(rows.len());
			for row in rows {
				match system_scalar_at(row, field_index)? {
					ScalarValue::Boolean(Some(value)) => builder.append_value(*value),
					ScalarValue::Boolean(None) => builder.append_null(),
					other => return Err(system_table_type_mismatch(field_name, "Boolean", other)),
				}
			}
			Ok(Arc::new(builder.finish()))
		}
		DataType::Int32 => {
			let mut builder = Int32Builder::with_capacity(rows.len());
			for row in rows {
				match system_scalar_at(row, field_index)? {
					ScalarValue::Int32(Some(value)) => builder.append_value(*value),
					ScalarValue::Int32(None) => builder.append_null(),
					other => return Err(system_table_type_mismatch(field_name, "Int32", other)),
				}
			}
			Ok(Arc::new(builder.finish()))
		}
		DataType::Int64 => {
			let mut builder = Int64Builder::with_capacity(rows.len());
			for row in rows {
				match system_scalar_at(row, field_index)? {
					ScalarValue::Int64(Some(value)) => builder.append_value(*value),
					ScalarValue::Int64(None) => builder.append_null(),
					other => return Err(system_table_type_mismatch(field_name, "Int64", other)),
				}
			}
			Ok(Arc::new(builder.finish()))
		}
		DataType::Date32 => {
			let mut builder = Date32Builder::with_capacity(rows.len());
			for row in rows {
				match system_scalar_at(row, field_index)? {
					ScalarValue::Date32(Some(value)) => builder.append_value(*value),
					ScalarValue::Date32(None) => builder.append_null(),
					other => return Err(system_table_type_mismatch(field_name, "Date32", other)),
				}
			}
			Ok(Arc::new(builder.finish()))
		}
		DataType::Timestamp(unit, Some(timezone)) if timezone.as_ref() == "UTC" => {
			build_system_timestamp_array(*unit, rows, field_index, field_name)
		}
		DataType::Duration(TimeUnit::Nanosecond) => {
			let mut builder = DurationNanosecondBuilder::with_capacity(rows.len());
			for row in rows {
				match system_scalar_at(row, field_index)? {
					ScalarValue::Interval(Some(value)) => builder.append_value(*value),
					ScalarValue::Interval(None) => builder.append_null(),
					other => {
						return Err(system_table_type_mismatch(
							field_name,
							"Duration(Nanosecond)",
							other,
						));
					}
				}
			}
			Ok(Arc::new(builder.finish()))
		}
		DataType::Interval(IntervalUnit::YearMonth) => {
			let mut builder = IntervalYearMonthBuilder::with_capacity(rows.len());
			for row in rows {
				match system_scalar_at(row, field_index)? {
					ScalarValue::CalendarMonthInterval(Some(value)) => builder.append_value(*value),
					ScalarValue::CalendarMonthInterval(None) => builder.append_null(),
					other => {
						return Err(system_table_type_mismatch(
							field_name,
							"Interval(YearMonth)",
							other,
						));
					}
				}
			}
			Ok(Arc::new(builder.finish()))
		}
		DataType::Float32 => {
			let mut builder = Float32Builder::with_capacity(rows.len());
			for row in rows {
				match system_scalar_at(row, field_index)? {
					ScalarValue::Float32(Some(value)) => builder.append_value(*value),
					ScalarValue::Float32(None) => builder.append_null(),
					other => return Err(system_table_type_mismatch(field_name, "Float32", other)),
				}
			}
			Ok(Arc::new(builder.finish()))
		}
		DataType::Float64 => {
			let mut builder = Float64Builder::with_capacity(rows.len());
			for row in rows {
				match system_scalar_at(row, field_index)? {
					ScalarValue::Float64(Some(value)) => builder.append_value(*value),
					ScalarValue::Float64(None) => builder.append_null(),
					other => return Err(system_table_type_mismatch(field_name, "Float64", other)),
				}
			}
			Ok(Arc::new(builder.finish()))
		}
		DataType::Utf8 => {
			let mut builder = StringBuilder::new();
			for row in rows {
				match system_scalar_at(row, field_index)? {
					ScalarValue::Utf8(Some(value)) => builder.append_value(value),
					ScalarValue::Utf8(None) => builder.append_null(),
					other => return Err(system_table_type_mismatch(field_name, "Utf8", other)),
				}
			}
			Ok(Arc::new(builder.finish()))
		}
		other => Err(ExecutionError::InvalidExpression {
			message: format!("unsupported system table column type: {other:?}"),
		}),
	}
}

fn build_system_timestamp_array(
	unit: TimeUnit,
	rows: &[Vec<ScalarValue>],
	field_index: usize,
	field_name: &str,
) -> Result<ArrayRef, ExecutionError> {
	match unit {
		TimeUnit::Millisecond => {
			let mut builder =
				TimestampMillisecondBuilder::with_capacity(rows.len()).with_timezone("UTC");
			for row in rows {
				match system_scalar_at(row, field_index)? {
					ScalarValue::Timestamp {
						value: Some(value),
						unit: TimeUnit::Millisecond,
					} => builder.append_value(*value),
					ScalarValue::Timestamp {
						value: None,
						unit: TimeUnit::Millisecond,
					} => builder.append_null(),
					other => {
						return Err(system_table_type_mismatch(
							field_name,
							"Timestamp(Millisecond, UTC)",
							other,
						));
					}
				}
			}
			Ok(Arc::new(builder.finish()))
		}
		TimeUnit::Microsecond => {
			let mut builder =
				TimestampMicrosecondBuilder::with_capacity(rows.len()).with_timezone("UTC");
			for row in rows {
				match system_scalar_at(row, field_index)? {
					ScalarValue::Timestamp {
						value: Some(value),
						unit: TimeUnit::Microsecond,
					} => builder.append_value(*value),
					ScalarValue::Timestamp {
						value: None,
						unit: TimeUnit::Microsecond,
					} => builder.append_null(),
					other => {
						return Err(system_table_type_mismatch(
							field_name,
							"Timestamp(Microsecond, UTC)",
							other,
						));
					}
				}
			}
			Ok(Arc::new(builder.finish()))
		}
		TimeUnit::Nanosecond => {
			let mut builder =
				TimestampNanosecondBuilder::with_capacity(rows.len()).with_timezone("UTC");
			for row in rows {
				match system_scalar_at(row, field_index)? {
					ScalarValue::Timestamp {
						value: Some(value),
						unit: TimeUnit::Nanosecond,
					} => builder.append_value(*value),
					ScalarValue::Timestamp {
						value: None,
						unit: TimeUnit::Nanosecond,
					} => builder.append_null(),
					other => {
						return Err(system_table_type_mismatch(
							field_name,
							"Timestamp(Nanosecond, UTC)",
							other,
						));
					}
				}
			}
			Ok(Arc::new(builder.finish()))
		}
		TimeUnit::Second => Err(ExecutionError::InvalidExpression {
			message: "unsupported system table timestamp unit: Second".to_string(),
		}),
	}
}

fn system_scalar_at(
	row: &[ScalarValue],
	field_index: usize,
) -> Result<&ScalarValue, ExecutionError> {
	row.get(field_index)
		.ok_or_else(|| ExecutionError::InvalidExpression {
			message: format!("system table row missing column {}", field_index + 1),
		})
}

fn system_table_type_mismatch(
	field_name: &str,
	expected: &'static str,
	actual: &ScalarValue,
) -> ExecutionError {
	ExecutionError::InvalidExpression {
		message: format!(
			"system table column {field_name} expected {expected}, got {:?}",
			actual.data_type()
		),
	}
}

fn concat_or_empty(
	schema: arrow::datatypes::SchemaRef,
	batches: &[RecordBatch],
) -> Result<RecordBatch, ExecutionError> {
	if batches.is_empty() {
		Ok(RecordBatch::new_empty(schema))
	} else {
		Ok(concat_batches(&schema, batches)?)
	}
}

fn execute_repeat(
	input_batches: &[RecordBatch],
	count_column: crate::ir::ColumnOrdinal,
	output_schema: &arrow::datatypes::SchemaRef,
	task_budget: TaskBudget,
) -> Result<Vec<RecordBatch>, ExecutionError> {
	let count_idx = usize::try_from(count_column.0).expect("column ordinal fits usize");
	let mut output = Vec::new();
	for batch in input_batches {
		let count_values = batch
			.column(count_idx)
			.as_any()
			.downcast_ref::<Int64Array>()
			.ok_or_else(|| ExecutionError::InvalidExpression {
				message: format!("REPEAT count column {count_idx} must be Int64"),
			})?;
		let mut output_rows = 0usize;
		for row in 0..batch.num_rows() {
			if count_values.is_null(row) {
				return Err(ExecutionError::InvalidExpression {
					message: "REPEAT count must not be NULL".to_string(),
				});
			}
			let count = count_values.value(row);
			if count < 0 {
				return Err(ExecutionError::InvalidExpression {
					message: format!("REPEAT count must not be negative, got {count}"),
				});
			}
			let count = usize::try_from(count).map_err(|_| ExecutionError::InvalidExpression {
				message: format!("REPEAT count {count} does not fit usize"),
			})?;
			output_rows =
				output_rows
					.checked_add(count)
					.ok_or_else(|| ExecutionError::BudgetExhausted {
						dimension: BudgetDimension::Memory,
						producer: "repeat output row count",
						requested: ByteCount::new(u64::MAX),
						available: task_budget.memory,
						detail: Default::default(),
					})?;
		}

		let input_rows = batch.num_rows().max(1);
		let payload_bytes = batch
			.columns()
			.iter()
			.enumerate()
			.filter(|(idx, _)| *idx != count_idx)
			.map(|(_, column)| column.get_array_memory_size())
			.sum::<usize>();
		let estimated_payload = payload_bytes
			.saturating_mul(output_rows)
			.saturating_div(input_rows);
		let estimated_index = output_rows.saturating_mul(std::mem::size_of::<u32>());
		let estimate =
			u64::try_from(estimated_payload.saturating_add(estimated_index)).unwrap_or(u64::MAX);
		if estimate > task_budget.memory.get() {
			return Err(ExecutionError::BudgetExhausted {
				dimension: BudgetDimension::Memory,
				producer: "repeat output materialization",
				requested: ByteCount::new(estimate),
				available: task_budget.memory,
				detail: Default::default(),
			});
		}

		if output_rows == 0 {
			output.push(RecordBatch::new_empty(output_schema.clone()));
			continue;
		}

		let mut indices = Vec::with_capacity(output_rows);
		for row in 0..batch.num_rows() {
			let count = usize::try_from(count_values.value(row)).map_err(|_| {
				ExecutionError::InvalidExpression {
					message: "REPEAT count does not fit usize".to_string(),
				}
			})?;
			let row_idx = u32::try_from(row).map_err(|_| ExecutionError::InvalidExpression {
				message: "REPEAT input row index exceeds UInt32 take index".to_string(),
			})?;
			indices.extend(std::iter::repeat_n(row_idx, count));
		}
		let indices = UInt32Array::from(indices);
		let columns = batch
			.columns()
			.iter()
			.enumerate()
			.filter(|(idx, _)| *idx != count_idx)
			.map(|(_, column)| take(column, &indices, None))
			.collect::<Result<Vec<_>, _>>()?;
		output.push(RecordBatch::try_new(output_schema.clone(), columns)?);
	}
	if output.is_empty() {
		output.push(RecordBatch::new_empty(output_schema.clone()));
	}
	Ok(output)
}

const SORT_MATERIALIZATION_MEMORY_CAP_MULTIPLIER: u64 = 16;

fn sort_materialization_memory_cap(task_memory: u64) -> u64 {
	// Align sort with the materialization family: join output and retained spill
	// footprint both get the same multiplier over the per-task memory budget.
	task_memory.saturating_mul(SORT_MATERIALIZATION_MEMORY_CAP_MULTIPLIER)
}

fn execute_sort(
	input_schema: arrow::datatypes::SchemaRef,
	input_batches: &[RecordBatch],
	order_by: &[ExecutionSortKey],
	limit: Option<usize>,
	offset: usize,
	output_schema: &arrow::datatypes::SchemaRef,
	task_budget: TaskBudget,
) -> Result<Vec<RecordBatch>, ExecutionError> {
	let row_count = input_batches
		.iter()
		.map(RecordBatch::num_rows)
		.sum::<usize>();
	let estimate = input_batches
		.iter()
		.map(RecordBatch::get_array_memory_size)
		.sum::<usize>()
		.saturating_add(row_count.saturating_mul(std::mem::size_of::<u32>()));
	let estimate = u64::try_from(estimate).unwrap_or(u64::MAX);
	let materialization_cap = sort_materialization_memory_cap(task_budget.memory.get());
	if estimate > materialization_cap {
		return Err(ExecutionError::BudgetExhausted {
			dimension: BudgetDimension::Memory,
			producer: "sort materialization",
			requested: ByteCount::new(estimate),
			available: ByteCount::new(materialization_cap),
			detail: Default::default(),
		});
	}

	let batch = concat_or_empty(input_schema, input_batches)?;
	if batch.num_rows() == 0 || offset >= batch.num_rows() {
		return Ok(vec![RecordBatch::new_empty(output_schema.clone())]);
	}
	if order_by.is_empty() {
		return Err(ExecutionError::InvalidExpression {
			message: "sort requires at least one key".to_string(),
		});
	}

	let sort_columns = order_by
		.iter()
		.map(|key| {
			let ordinal = usize::try_from(key.ordinal.0).expect("column ordinal fits usize");
			let values = batch.columns().get(ordinal).cloned().ok_or_else(|| {
				ExecutionError::InvalidExpression {
					message: format!("sort key column ordinal {ordinal} out of bounds"),
				}
			})?;
			Ok(SortColumn {
				values,
				options: Some(SortOptions {
					descending: key.descending,
					nulls_first: key.nulls_first,
				}),
			})
		})
		.collect::<Result<Vec<_>, ExecutionError>>()?;

	let sorted_indices = lexsort_to_indices(&sort_columns, None)?;
	let remaining = batch.num_rows() - offset;
	let take_count = limit.unwrap_or(remaining).min(remaining);
	let selected = sorted_indices
		.values()
		.iter()
		.skip(offset)
		.take(take_count)
		.copied()
		.collect::<Vec<_>>();
	let indices = UInt32Array::from(selected);
	let columns = batch
		.columns()
		.iter()
		.map(|column| take(column, &indices, None))
		.collect::<Result<Vec<_>, _>>()?;

	Ok(vec![RecordBatch::try_new(output_schema.clone(), columns)?])
}

/// ADR 249: per-row NTILE bucket index (1-based). Each partition is split into `buckets` near-equal
/// groups; the first `len % buckets` hold one extra row. `buckets >= 1` is lowering-enforced, so when
/// `base == 0` (fewer rows than buckets) `boundary == len` and every row takes the first arm — the
/// `else` divisor is never zero.
fn ntile_buckets(
	buckets: u64,
	partition_start: &[usize],
	partition_end: &[usize],
	rows: usize,
) -> Vec<i64> {
	let k = buckets.max(1);
	(0..rows)
		.map(|row| {
			let i = (row - partition_start[row]) as u64;
			let len = (partition_end[row] - partition_start[row] + 1) as u64;
			let base = len / k;
			let rem = len % k;
			let boundary = rem * (base + 1);
			let bucket = if i < boundary {
				i / (base + 1) + 1
			} else {
				rem + (i - boundary) / base + 1
			};
			bucket as i64
		})
		.collect()
}

/// ADR 241: ranking window functions. The input arrives co-located by `partition_by` (the planner put
/// an `Exchange` beneath the `Window` node). We order each partition internally by `[partition_by...,
/// order_by...]`, then stream ranks resetting on each partition-key change. Row-preserving: the output
/// is the input columns (in the new order) plus one Int64 rank column per function.
fn execute_window(
	input_schema: arrow::datatypes::SchemaRef,
	input_batches: &[RecordBatch],
	partition_by: &BoundExprProgram,
	order_by: &[ExecutionSortKey],
	functions: &[BoundWindowCall],
	output_schema: &arrow::datatypes::SchemaRef,
	task_budget: TaskBudget,
) -> Result<Vec<RecordBatch>, ExecutionError> {
	let batch = concat_or_empty(input_schema, input_batches)?;
	let rows = batch.num_rows();
	if rows == 0 {
		return Ok(vec![RecordBatch::new_empty(output_schema.clone())]);
	}

	let estimate = batch.get_array_memory_size().saturating_add(
		rows.saturating_mul(std::mem::size_of::<u32>() + std::mem::size_of::<i64>()),
	);
	let estimate = u64::try_from(estimate).unwrap_or(u64::MAX);
	if estimate > task_budget.memory.get() {
		return Err(ExecutionError::BudgetExhausted {
			dimension: BudgetDimension::Memory,
			producer: "window materialization",
			requested: ByteCount::new(estimate),
			available: task_budget.memory,
			detail: Default::default(),
		});
	}

	let partition_arrays = Executor::evaluate_bound_program_without_storage(partition_by, &batch)?;
	let order_arrays =
		order_by
			.iter()
			.map(|key| {
				let ordinal = key.ordinal.0 as usize;
				batch.columns().get(ordinal).cloned().ok_or_else(|| {
					ExecutionError::InvalidExpression {
						message: format!("window order key ordinal {ordinal} out of bounds"),
					}
				})
			})
			.collect::<Result<Vec<_>, ExecutionError>>()?;

	// Order each partition contiguously, ordered within: sort by partition keys (ascending) then the
	// declared ORDER BY keys.
	let mut sort_columns = partition_arrays
		.iter()
		.map(|values| SortColumn {
			values: values.clone(),
			options: Some(SortOptions {
				descending: false,
				nulls_first: true,
			}),
		})
		.collect::<Vec<_>>();
	for (key, values) in order_by.iter().zip(&order_arrays) {
		sort_columns.push(SortColumn {
			values: values.clone(),
			options: Some(SortOptions {
				descending: key.descending,
				nulls_first: key.nulls_first,
			}),
		});
	}
	// Totalize the sort with every column as an ascending tiebreaker so ROW_NUMBER among peers is
	// deterministic. Peer detection below still uses only `order_by`, so RANK/DENSE_RANK ties are
	// unaffected by the tiebreakers.
	for column in batch.columns() {
		sort_columns.push(SortColumn {
			values: column.clone(),
			options: Some(SortOptions {
				descending: false,
				nulls_first: false,
			}),
		});
	}
	let sorted_indices = if sort_columns.is_empty() {
		(0..u32::try_from(rows).unwrap_or(u32::MAX)).collect::<Vec<u32>>()
	} else {
		lexsort_to_indices(&sort_columns, None)?.values().to_vec()
	};
	let indices = UInt32Array::from(sorted_indices);

	let mut columns = batch
		.columns()
		.iter()
		.map(|column| take(column, &indices, None))
		.collect::<Result<Vec<_>, _>>()?;
	let sorted_partition = take_all(&partition_arrays, &indices)?;
	let sorted_order = take_all(&order_arrays, &indices)?;
	let partition_rows = window_comparison_rows(&sorted_partition)?;
	let order_rows = window_comparison_rows(&sorted_order)?;

	// Partition + peer-group boundaries (peers detected on the user ORDER BY keys only). These drive
	// both the ranking functions and the aggregate windows' RANGE default frame (a peer group is the
	// RANGE unit: ties through the end of the group share one cumulative value).
	let mut is_new_partition = vec![false; rows];
	let mut is_new_peer_group = vec![false; rows];
	for row in 0..rows {
		let new_partition = row == 0 || !window_rows_equal(&partition_rows, row, row - 1);
		// A new peer group starts at a partition boundary or when the ORDER BY value changes.
		let new_peer_group = new_partition || !window_rows_equal(&order_rows, row, row - 1);
		is_new_partition[row] = new_partition;
		is_new_peer_group[row] = new_peer_group;
	}
	let mut is_peer_group_end = vec![false; rows];
	for row in 0..rows {
		is_peer_group_end[row] = row + 1 == rows || is_new_peer_group[row + 1];
	}

	// ADR 243: per-row partition bounds (inclusive) for ROWS-frame slicing.
	let mut partition_start = vec![0_usize; rows];
	let mut current_start = 0_usize;
	for row in 0..rows {
		if is_new_partition[row] {
			current_start = row;
		}
		partition_start[row] = current_start;
	}
	let mut partition_end = vec![rows - 1; rows];
	let mut current_end = rows - 1;
	for row in (0..rows).rev() {
		if row + 1 == rows || is_new_partition[row + 1] {
			current_end = row;
		}
		partition_end[row] = current_end;
	}

	// ADR 244: per-row peer-group bounds (inclusive) for RANGE `CURRENT ROW` resolution (value-peers).
	let mut peer_start = vec![0_usize; rows];
	let mut current_peer_start = 0_usize;
	for row in 0..rows {
		if is_new_peer_group[row] {
			current_peer_start = row;
		}
		peer_start[row] = current_peer_start;
	}
	let mut peer_end = vec![rows - 1; rows];
	let mut current_peer_end = rows - 1;
	for row in (0..rows).rev() {
		if row + 1 == rows || is_new_peer_group[row + 1] {
			current_peer_end = row;
		}
		peer_end[row] = current_peer_end;
	}
	// ADR 244: value-offset RANGE needs exactly one ORDER BY key; peer/UNBOUNDED RANGE works without it.
	let range_inputs = RangeWindowInputs {
		sorted_key: if order_by.len() == 1 {
			sorted_order.first()
		} else {
			None
		},
		descending: order_by.first().is_some_and(|key| key.descending),
		nulls_first: order_by.first().is_none_or(|key| key.nulls_first),
		peer_start: &peer_start,
		peer_end: &peer_end,
	};

	let mut row_number = vec![0_i64; rows];
	let mut rank = vec![0_i64; rows];
	let mut dense_rank = vec![0_i64; rows];
	let (mut rn, mut rk, mut dr) = (0_i64, 0_i64, 0_i64);
	for row in 0..rows {
		if is_new_partition[row] {
			rn = 0;
			rk = 0;
			dr = 0;
		}
		rn += 1;
		if is_new_peer_group[row] {
			rk = rn;
			dr += 1;
		}
		row_number[row] = rn;
		rank[row] = rk;
		dense_rank[row] = dr;
	}

	let input_columns = batch.num_columns();
	for (call_idx, call) in functions.iter().enumerate() {
		match &call.function {
			WindowFunction::RowNumber => {
				columns.push(Arc::new(Int64Array::from(row_number.clone())) as ArrayRef);
			}
			WindowFunction::Rank => {
				columns.push(Arc::new(Int64Array::from(rank.clone())) as ArrayRef);
			}
			WindowFunction::DenseRank => {
				columns.push(Arc::new(Int64Array::from(dense_rank.clone())) as ArrayRef);
			}
			WindowFunction::Ntile { buckets } => {
				// ADR 249: divide each partition into `buckets` near-equal groups from partition geometry.
				columns.push(Arc::new(Int64Array::from(ntile_buckets(
					*buckets,
					&partition_start,
					&partition_end,
					rows,
				))) as ArrayRef);
			}
			WindowFunction::Aggregate(function) => {
				// Evaluate the argument over the sorted partition layout (ranking has no arg, and
				// COUNT(*) carries none).
				let sorted_arg = match &call.arg {
					Some(program) => {
						let evaluated =
							Executor::evaluate_bound_program_without_storage(program, &batch)?;
						let arg = evaluated.into_iter().next().ok_or_else(|| {
							ExecutionError::InvalidExpression {
								message: "window aggregate argument produced no column".to_string(),
							}
						})?;
						Some(take(&arg, &indices, None)?)
					}
					None => None,
				};
				let output_type = output_schema.field(input_columns + call_idx).data_type();
				let column = execute_window_aggregate(
					function,
					sorted_arg.as_ref(),
					call.frame.as_ref(),
					Some(&range_inputs),
					&is_new_partition,
					&is_peer_group_end,
					&partition_start,
					&partition_end,
					rows,
					output_type,
				)?;
				columns.push(column);
			}
			WindowFunction::Value(value) => {
				// ADR 246: evaluate the value expression over the sorted partition layout.
				let program =
					call.arg
						.as_ref()
						.ok_or_else(|| ExecutionError::InvalidExpression {
							message: "value window function requires a value argument".to_string(),
						})?;
				let evaluated = Executor::evaluate_bound_program_without_storage(program, &batch)?;
				let column_value = evaluated.into_iter().next().ok_or_else(|| {
					ExecutionError::InvalidExpression {
						message: "value window argument produced no column".to_string(),
					}
				})?;
				let sorted_value = take(&column_value, &indices, None)?;
				let output_type = output_schema.field(input_columns + call_idx).data_type();
				let column = execute_window_value(
					value,
					&sorted_value,
					call.frame.as_ref(),
					&range_inputs,
					&is_peer_group_end,
					&partition_start,
					&partition_end,
					rows,
					output_type,
					call.ignore_nulls,
				)?;
				columns.push(column);
			}
		}
	}

	Ok(vec![RecordBatch::try_new(output_schema.clone(), columns)?])
}

/// Compute one aggregate-window column. With no explicit frame (`None`), the 242 default frame
/// (`RANGE UNBOUNDED PRECEDING AND CURRENT ROW` — peer-aware cumulative); with an explicit `ROWS` frame
/// (ADR 243), a per-row sliding slice clamped to the partition.
fn execute_window_aggregate(
	function: &AggregateFunction,
	sorted_arg: Option<&ArrayRef>,
	frame: Option<&WindowFrame>,
	range: Option<&RangeWindowInputs<'_>>,
	is_new_partition: &[bool],
	is_peer_group_end: &[bool],
	partition_start: &[usize],
	partition_end: &[usize],
	rows: usize,
	output_type: &arrow::datatypes::DataType,
) -> Result<ArrayRef, ExecutionError> {
	match frame {
		None => execute_window_aggregate_default(
			function,
			sorted_arg,
			is_new_partition,
			is_peer_group_end,
			rows,
			output_type,
		),
		Some(frame) => match frame.unit {
			WindowFrameUnit::Rows => execute_window_aggregate_rows(
				function,
				sorted_arg,
				frame,
				partition_start,
				partition_end,
				rows,
				output_type,
			),
			WindowFrameUnit::Range => {
				let range = range.ok_or_else(|| ExecutionError::InvalidExpression {
					message: "RANGE window frame requires a single ORDER BY key".to_string(),
				})?;
				execute_window_aggregate_range(
					function,
					sorted_arg,
					frame,
					range,
					partition_start,
					partition_end,
					rows,
					output_type,
				)
			}
			WindowFrameUnit::Groups => execute_window_aggregate_groups(
				function,
				sorted_arg,
				frame,
				is_peer_group_end,
				partition_start,
				partition_end,
				rows,
				output_type,
			),
		},
	}
}

/// ADR 242 default frame: a running accumulator over the sorted partition, finalized at each peer-group
/// boundary and assigned to every row of that peer group (tied `ORDER BY` rows share the value).
fn execute_window_aggregate_default(
	function: &AggregateFunction,
	sorted_arg: Option<&ArrayRef>,
	is_new_partition: &[bool],
	is_peer_group_end: &[bool],
	rows: usize,
	output_type: &arrow::datatypes::DataType,
) -> Result<ArrayRef, ExecutionError> {
	let arg_type = sorted_arg.map(|array| array.data_type().clone());
	let mut count: i64 = 0;
	let mut state = fresh_window_agg_state(function, arg_type.as_ref())?;
	let mut group_arrays: Vec<ArrayRef> = Vec::new();
	let mut group_len: usize = 0;
	for row in 0..rows {
		if is_new_partition[row] {
			count = 0;
			state = fresh_window_agg_state(function, arg_type.as_ref())?;
		}
		window_agg_add(function, &mut state, &mut count, sorted_arg, row)?;
		group_len += 1;
		if is_peer_group_end[row] {
			let value = window_agg_finalize(function, &state, count, output_type)?;
			let repeat = UInt32Array::from(vec![0_u32; group_len]);
			group_arrays.push(take(&value, &repeat, None)?);
			group_len = 0;
		}
	}
	concat_window_columns(&group_arrays)
}

/// ADR 243 explicit `ROWS` frame: for each row, re-aggregate a fresh accumulator over the physical
/// frame slice `[lo, hi]` (clamped to the partition); an empty frame yields the empty aggregate.
fn execute_window_aggregate_rows(
	function: &AggregateFunction,
	sorted_arg: Option<&ArrayRef>,
	frame: &WindowFrame,
	partition_start: &[usize],
	partition_end: &[usize],
	rows: usize,
	output_type: &arrow::datatypes::DataType,
) -> Result<ArrayRef, ExecutionError> {
	let arg_type = sorted_arg.map(|array| array.data_type().clone());
	let mut row_arrays: Vec<ArrayRef> = Vec::with_capacity(rows);
	for row in 0..rows {
		let mut count: i64 = 0;
		let mut state = fresh_window_agg_state(function, arg_type.as_ref())?;
		if let Some((lo, hi)) =
			window_frame_slice(frame, row, partition_start[row], partition_end[row])
		{
			for slot in lo..=hi {
				window_agg_add(function, &mut state, &mut count, sorted_arg, slot)?;
			}
		}
		row_arrays.push(window_agg_finalize(function, &state, count, output_type)?);
	}
	concat_window_columns(&row_arrays)
}

fn concat_window_columns(columns: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
	let refs: Vec<&dyn arrow::array::Array> = columns.iter().map(|array| array.as_ref()).collect();
	Ok(arrow::compute::concat(&refs)?)
}

/// ADR 244: the sorted single ORDER BY key + its direction/nulls placement + peer-group bounds that a
/// `RANGE` frame needs (peer bounds resolve `CURRENT ROW`; the key + arithmetic resolve value offsets).
struct RangeWindowInputs<'a> {
	sorted_key: Option<&'a ArrayRef>,
	descending: bool,
	nulls_first: bool,
	peer_start: &'a [usize],
	peer_end: &'a [usize],
}

/// ADR 244: a `RANGE` frame. Each bound resolves to a row index — partition/peer edge for `UNBOUNDED`/
/// `CURRENT ROW` (any key), or a value search over `key ± offset` (numeric/temporal key, reusing Toron's
/// typed arithmetic). The contiguous `[lo, hi]` slice is aggregated with the existing kernels.
fn execute_window_aggregate_range(
	function: &AggregateFunction,
	sorted_arg: Option<&ArrayRef>,
	frame: &WindowFrame,
	range: &RangeWindowInputs<'_>,
	partition_start: &[usize],
	partition_end: &[usize],
	rows: usize,
	output_type: &arrow::datatypes::DataType,
) -> Result<ArrayRef, ExecutionError> {
	let arg_type = sorted_arg.map(|array| array.data_type().clone());
	let slices = range_frame_slices(frame, range, partition_start, partition_end, rows)?;
	let mut row_arrays: Vec<ArrayRef> = Vec::with_capacity(rows);
	for row in 0..rows {
		let mut count: i64 = 0;
		let mut state = fresh_window_agg_state(function, arg_type.as_ref())?;
		if let Some((lo, hi)) = slices[row] {
			for slot in lo..=hi {
				window_agg_add(function, &mut state, &mut count, sorted_arg, slot)?;
			}
		}
		row_arrays.push(window_agg_finalize(function, &state, count, output_type)?);
	}
	concat_window_columns(&row_arrays)
}

/// ADR 244/246: per-row `[lo, hi]` slice (inclusive) of a `RANGE` frame, shared by the aggregate executor
/// and the value functions so `FIRST_VALUE`/`LAST_VALUE`/`NTH_VALUE` frame exactly as `SUM`/… do. Value
/// offsets search the sorted key; `UNBOUNDED`/`CURRENT ROW` use the partition/peer edge (any key). `None`
/// = empty frame. A NULL ORDER BY value collapses value offsets to its NULL peer group.
fn range_frame_slices(
	frame: &WindowFrame,
	range: &RangeWindowInputs<'_>,
	partition_start: &[usize],
	partition_end: &[usize],
	rows: usize,
) -> Result<Vec<Option<(usize, usize)>>, ExecutionError> {
	// One RowConverter (direction + nulls matching the partition sort) encodes both the key and the
	// computed bound values, so comparisons reduce to byte-ordered row comparisons and the encoded key is
	// non-decreasing with row position inside a partition.
	let key_encoding = if bound_is_value_offset(&frame.start) || bound_is_value_offset(&frame.end) {
		let key = range
			.sorted_key
			.ok_or_else(|| ExecutionError::InvalidExpression {
				message: "RANGE value offsets require exactly one ORDER BY key".to_string(),
			})?;
		let converter =
			arrow::row::RowConverter::new(vec![arrow::row::SortField::new_with_options(
				key.data_type().clone(),
				SortOptions {
					descending: range.descending,
					nulls_first: range.nulls_first,
				},
			)])?;
		let key_rows = converter.convert_columns(std::slice::from_ref(key))?;
		let start_rows = range_bound_value_rows(&frame.start, key, range.descending, &converter)?;
		let end_rows = range_bound_value_rows(&frame.end, key, range.descending, &converter)?;
		Some((key, key_rows, start_rows, end_rows))
	} else {
		None
	};

	let mut slices: Vec<Option<(usize, usize)>> = Vec::with_capacity(rows);
	for row in 0..rows {
		let p_start = partition_start[row];
		let p_end = partition_end[row];
		let key_is_null = key_encoding
			.as_ref()
			.is_some_and(|(key, ..)| key.is_null(row));

		let lo = match &frame.start {
			WindowFrameBound::UnboundedPreceding | WindowFrameBound::UnboundedFollowing => {
				Some(p_start)
			}
			WindowFrameBound::CurrentRow => Some(range.peer_start[row]),
			WindowFrameBound::Preceding(_) | WindowFrameBound::Following(_) => {
				if key_is_null {
					Some(range.peer_start[row])
				} else {
					let (_, key_rows, start_rows, _) =
						key_encoding.as_ref().expect("value offset needs key");
					range_lower_index(key_rows, start_rows.as_ref(), row, p_start, p_end)
				}
			}
		};
		let hi = match &frame.end {
			WindowFrameBound::UnboundedFollowing | WindowFrameBound::UnboundedPreceding => {
				Some(p_end)
			}
			WindowFrameBound::CurrentRow => Some(range.peer_end[row]),
			WindowFrameBound::Preceding(_) | WindowFrameBound::Following(_) => {
				if key_is_null {
					Some(range.peer_end[row])
				} else {
					let (_, key_rows, _, end_rows) =
						key_encoding.as_ref().expect("value offset needs key");
					range_upper_index(key_rows, end_rows.as_ref(), row, p_start, p_end)
				}
			}
		};
		slices.push(match (lo, hi) {
			(Some(lo), Some(hi)) if lo <= hi => Some((lo, hi)),
			_ => None,
		});
	}
	Ok(slices)
}

/// Whether a frame bound carries a `RANGE` value offset (vs `UNBOUNDED`/`CURRENT ROW`, which are
/// peer/partition based and need no key arithmetic).
fn bound_is_value_offset(bound: &WindowFrameBound) -> bool {
	matches!(
		bound,
		WindowFrameBound::Preceding(WindowFrameDelta::Range(_))
			| WindowFrameBound::Following(WindowFrameDelta::Range(_))
	)
}

/// Encode the per-row bound value `key ± offset` for a value-offset RANGE bound (`None` for
/// `UNBOUNDED`/`CURRENT ROW`). The op is direction-aware: `PRECEDING` moves toward the sort start
/// (`Sub` ascending, `Add` descending), `FOLLOWING` toward the sort end — so the encoded bound stays
/// ordered with the encoded key.
fn range_bound_value_rows(
	bound: &WindowFrameBound,
	key: &ArrayRef,
	descending: bool,
	converter: &arrow::row::RowConverter,
) -> Result<Option<arrow::row::Rows>, ExecutionError> {
	let (offset, is_following) = match bound {
		WindowFrameBound::Preceding(WindowFrameDelta::Range(offset)) => (offset, false),
		WindowFrameBound::Following(WindowFrameDelta::Range(offset)) => (offset, true),
		_ => return Ok(None),
	};
	let op = if is_following ^ descending {
		expr::ArithmeticOp::Add
	} else {
		expr::ArithmeticOp::Sub
	};
	let offset_array = expr::literal_array(offset, key.len())?;
	let bound_values = expr::evaluate_unbound_arithmetic(op, key.clone(), offset_array)?;
	Ok(Some(
		converter.convert_columns(std::slice::from_ref(&bound_values))?,
	))
}

/// First row index in `[p_start, p_end]` whose encoded key ≥ the row's encoded lower bound (the contiguous
/// interval's start), or `None` when no row reaches the bound (empty frame).
fn range_lower_index(
	key_rows: &arrow::row::Rows,
	bound_rows: Option<&arrow::row::Rows>,
	row: usize,
	p_start: usize,
	p_end: usize,
) -> Option<usize> {
	let bound_rows = bound_rows?;
	let target = bound_rows.row(row);
	(p_start..=p_end).find(|&j| key_rows.row(j) >= target)
}

/// Last row index in `[p_start, p_end]` whose encoded key ≤ the row's encoded upper bound (the contiguous
/// interval's end), or `None` when no row is within the bound (empty frame).
fn range_upper_index(
	key_rows: &arrow::row::Rows,
	bound_rows: Option<&arrow::row::Rows>,
	row: usize,
	p_start: usize,
	p_end: usize,
) -> Option<usize> {
	let bound_rows = bound_rows?;
	let target = bound_rows.row(row);
	(p_start..=p_end).rev().find(|&j| key_rows.row(j) <= target)
}

/// Fold one row's contribution into the window accumulator (counts via `count`, the rest via `state`).
fn window_agg_add(
	function: &AggregateFunction,
	state: &mut Option<AggState>,
	count: &mut i64,
	sorted_arg: Option<&ArrayRef>,
	row: usize,
) -> Result<(), ExecutionError> {
	match function {
		AggregateFunction::CountAll => *count += 1,
		AggregateFunction::Count => {
			if let Some(arg) = sorted_arg
				&& !arg.is_null(row)
			{
				*count += 1;
			}
		}
		AggregateFunction::Sum => {
			if let (Some(arg), Some(state)) = (sorted_arg, state.as_mut())
				&& let Some(value) = numeric_value(arg, row)?
			{
				add_sum_value(state, value)?;
			}
		}
		AggregateFunction::Avg => {
			if let (Some(arg), Some(state)) = (sorted_arg, state.as_mut())
				&& let Some(value) = numeric_value(arg, row)?
			{
				add_avg_value(state, value)?;
			}
		}
		AggregateFunction::Min => {
			if let (Some(arg), Some(state)) = (sorted_arg, state.as_mut()) {
				update_min_state(state, arg, row)?;
			}
		}
		AggregateFunction::Max => {
			if let (Some(arg), Some(state)) = (sorted_arg, state.as_mut()) {
				update_max_state(state, arg, row)?;
			}
		}
		AggregateFunction::LinearRegress | AggregateFunction::ApproxQuantile(_) => {
			return Err(ExecutionError::NotImplemented {
				feature: "window experimental aggregate execution",
			});
		}
	}
	Ok(())
}

/// Finalize a window accumulator into a one-element array (NULL/0 for an empty frame).
fn window_agg_finalize(
	function: &AggregateFunction,
	state: &Option<AggState>,
	count: i64,
	output_type: &arrow::datatypes::DataType,
) -> Result<ArrayRef, ExecutionError> {
	Ok(match function {
		AggregateFunction::CountAll | AggregateFunction::Count => {
			Arc::new(Int64Array::from(vec![count])) as ArrayRef
		}
		AggregateFunction::Sum => {
			sum_state_array(state.as_ref().expect("sum window state"), output_type)?
		}
		AggregateFunction::Avg => {
			avg_state_array(state.as_ref().expect("avg window state"), output_type)?
		}
		AggregateFunction::Min | AggregateFunction::Max => {
			min_max_state_array(state.as_ref().expect("min/max window state"), output_type)?
		}
		AggregateFunction::LinearRegress | AggregateFunction::ApproxQuantile(_) => {
			return Err(ExecutionError::NotImplemented {
				feature: "window experimental aggregate execution",
			});
		}
	})
}

/// ADR 243: the physical `[lo, hi]` (inclusive) slice of a `ROWS` frame for `row`, clamped to the
/// partition `[partition_start, partition_end]`. `None` when the frame is empty (lands outside).
fn window_frame_slice(
	frame: &WindowFrame,
	row: usize,
	partition_start: usize,
	partition_end: usize,
) -> Option<(usize, usize)> {
	let lo = frame_bound_position(&frame.start, row).max(partition_start as i64);
	let hi = frame_bound_position(&frame.end, row).min(partition_end as i64);
	if lo > hi {
		None
	} else {
		Some((lo as usize, hi as usize))
	}
}

fn frame_bound_position(bound: &WindowFrameBound, row: usize) -> i64 {
	let row = row as i64;
	match bound {
		WindowFrameBound::UnboundedPreceding => i64::MIN,
		WindowFrameBound::Preceding(delta) => row.saturating_sub(rows_delta(delta)),
		WindowFrameBound::CurrentRow => row,
		WindowFrameBound::Following(delta) => row.saturating_add(rows_delta(delta)),
		WindowFrameBound::UnboundedFollowing => i64::MAX,
	}
}

/// The physical row count carried by a `ROWS`-frame delta (the only delta kind a `ROWS` frame carries).
fn rows_delta(delta: &WindowFrameDelta) -> i64 {
	match delta {
		WindowFrameDelta::Rows(n) => i64::try_from(*n).unwrap_or(i64::MAX),
		WindowFrameDelta::Range(_) | WindowFrameDelta::Groups(_) => i64::MAX,
	}
}

/// ADR 245: a `GROUPS` frame. Each bound resolves to a peer-group ordinal — partition edge for
/// `UNBOUNDED`, the current group for `CURRENT ROW`, or `g ∓ n` for an `n PRECEDING`/`FOLLOWING` count —
/// clamped to the partition's group range; the `[lo_group, hi_group]` span maps back to a contiguous row
/// slice. No value arithmetic and direction-agnostic (offsets are positional in group space).
fn execute_window_aggregate_groups(
	function: &AggregateFunction,
	sorted_arg: Option<&ArrayRef>,
	frame: &WindowFrame,
	is_peer_group_end: &[bool],
	partition_start: &[usize],
	partition_end: &[usize],
	rows: usize,
	output_type: &arrow::datatypes::DataType,
) -> Result<ArrayRef, ExecutionError> {
	let arg_type = sorted_arg.map(|array| array.data_type().clone());
	let slices = groups_frame_slices(
		frame,
		is_peer_group_end,
		partition_start,
		partition_end,
		rows,
	);
	let mut row_arrays: Vec<ArrayRef> = Vec::with_capacity(rows);
	for row in 0..rows {
		let mut count: i64 = 0;
		let mut state = fresh_window_agg_state(function, arg_type.as_ref())?;
		if let Some((lo, hi)) = slices[row] {
			for slot in lo..=hi {
				window_agg_add(function, &mut state, &mut count, sorted_arg, slot)?;
			}
		}
		row_arrays.push(window_agg_finalize(function, &state, count, output_type)?);
	}
	concat_window_columns(&row_arrays)
}

/// ADR 245/246: per-row `[lo, hi]` slice (inclusive) of a `GROUPS` frame, shared by the aggregate executor
/// and the value functions. Builds a partition-aligned peer-group index + each group's row span, resolves
/// each bound to a group ordinal (clamped to the partition's group range), and maps the span back to rows.
/// `None` = the span lands entirely outside the partition's groups.
fn groups_frame_slices(
	frame: &WindowFrame,
	is_peer_group_end: &[bool],
	partition_start: &[usize],
	partition_end: &[usize],
	rows: usize,
) -> Vec<Option<(usize, usize)>> {
	// A new group starts at row 0 or right after a peer-group end (a partition boundary, also a peer-group
	// end, starts a fresh group), so group ids are partition-aligned.
	let mut group_id = vec![0_usize; rows];
	let mut group_start_row: Vec<usize> = Vec::new();
	let mut group_end_row: Vec<usize> = Vec::new();
	for row in 0..rows {
		if row == 0 || is_peer_group_end[row - 1] {
			group_start_row.push(row);
			group_end_row.push(row);
		}
		let current = group_start_row.len() - 1;
		group_id[row] = current;
		group_end_row[current] = row;
	}

	let mut slices: Vec<Option<(usize, usize)>> = Vec::with_capacity(rows);
	for row in 0..rows {
		let g = group_id[row] as i64;
		let first_group = group_id[partition_start[row]] as i64;
		let last_group = group_id[partition_end[row]] as i64;
		let start_group = groups_bound_group(&frame.start, g, first_group, last_group);
		let end_group = groups_bound_group(&frame.end, g, first_group, last_group);
		// Empty when the span lands entirely outside the partition's groups; otherwise clamp.
		let slice = if start_group <= last_group && end_group >= first_group {
			let lo_group = start_group.max(first_group) as usize;
			let hi_group = end_group.min(last_group) as usize;
			(lo_group <= hi_group).then(|| (group_start_row[lo_group], group_end_row[hi_group]))
		} else {
			None
		};
		slices.push(slice);
	}
	slices
}

/// Resolve a `GROUPS` frame bound to a peer-group ordinal, relative to the current group `g` and the
/// partition's `[first_group, last_group]` range. `PRECEDING`/`FOLLOWING` shift by the group count.
fn groups_bound_group(bound: &WindowFrameBound, g: i64, first_group: i64, last_group: i64) -> i64 {
	match bound {
		WindowFrameBound::UnboundedPreceding => first_group,
		WindowFrameBound::UnboundedFollowing => last_group,
		WindowFrameBound::CurrentRow => g,
		WindowFrameBound::Preceding(delta) => g.saturating_sub(groups_count(delta)),
		WindowFrameBound::Following(delta) => g.saturating_add(groups_count(delta)),
	}
}

/// The peer-group count carried by a `GROUPS`-frame delta (the only delta kind a `GROUPS` frame carries).
fn groups_count(delta: &WindowFrameDelta) -> i64 {
	match delta {
		WindowFrameDelta::Groups(n) => i64::try_from(*n).unwrap_or(i64::MAX),
		WindowFrameDelta::Rows(_) | WindowFrameDelta::Range(_) => i64::MAX,
	}
}

/// ADR 246: per-row frame slice `[lo, hi]` for the frame-aware value functions, dispatching to the SAME
/// slice logic the aggregate executors use (so `FIRST_VALUE`/… frame identically to `SUM`/…). The absent
/// frame is the 242 default (`[partition start, current peer-group end]`).
fn value_frame_slices(
	frame: Option<&WindowFrame>,
	range: &RangeWindowInputs<'_>,
	is_peer_group_end: &[bool],
	partition_start: &[usize],
	partition_end: &[usize],
	rows: usize,
) -> Result<Vec<Option<(usize, usize)>>, ExecutionError> {
	Ok(match frame {
		None => (0..rows)
			.map(|row| Some((partition_start[row], range.peer_end[row])))
			.collect(),
		Some(frame) => match frame.unit {
			WindowFrameUnit::Rows => (0..rows)
				.map(|row| window_frame_slice(frame, row, partition_start[row], partition_end[row]))
				.collect(),
			WindowFrameUnit::Range => {
				range_frame_slices(frame, range, partition_start, partition_end, rows)?
			}
			WindowFrameUnit::Groups => groups_frame_slices(
				frame,
				is_peer_group_end,
				partition_start,
				partition_end,
				rows,
			),
		},
	})
}

/// ADR 246: compute one value-function column. `Lag`/`Lead` take the value at a positional offset
/// (partition-clamped; out of partition → the constant default, else NULL); `FirstValue`/`LastValue`/
/// `NthValue` take the value at the first/last/nth row of the per-row frame slice (NULL for an empty frame
/// or an out-of-range `NthValue`). Assembled by a single `take` over a per-row target index.
/// ADR 252: step `offset` NON-NULL rows from `row` toward the partition edge (back if `backward`),
/// staying within `[lo, hi]`. Returns the offset-th non-NULL row, or `None` if fewer exist. Offset 0
/// is the current row (the LAG/LEAD offset-0 case).
fn nth_non_null_step(
	values: &ArrayRef,
	row: usize,
	offset: usize,
	backward: bool,
	lo: usize,
	hi: usize,
) -> Option<usize> {
	if offset == 0 {
		return Some(row);
	}
	let mut i = row;
	let mut count = 0;
	loop {
		if backward {
			if i == lo {
				return None;
			}
			i -= 1;
		} else {
			if i == hi {
				return None;
			}
			i += 1;
		}
		if !values.is_null(i) {
			count += 1;
			if count == offset {
				return Some(i);
			}
		}
	}
}

fn execute_window_value(
	value_fn: &ValueFunction,
	sorted_value: &ArrayRef,
	frame: Option<&WindowFrame>,
	range: &RangeWindowInputs<'_>,
	is_peer_group_end: &[bool],
	partition_start: &[usize],
	partition_end: &[usize],
	rows: usize,
	output_type: &arrow::datatypes::DataType,
	ignore_nulls: bool,
) -> Result<ArrayRef, ExecutionError> {
	let mut targets: Vec<Option<usize>> = Vec::with_capacity(rows);
	match value_fn {
		ValueFunction::Lag { offset, .. } | ValueFunction::Lead { offset, .. } => {
			let is_lag = matches!(value_fn, ValueFunction::Lag { .. });
			let offset_steps = *offset as usize;
			let offset = i64::try_from(*offset).unwrap_or(i64::MAX);
			for row in 0..rows {
				let target = if ignore_nulls {
					// ADR 252: step `offset` NON-NULL rows toward the partition edge.
					nth_non_null_step(
						sorted_value,
						row,
						offset_steps,
						is_lag,
						partition_start[row],
						partition_end[row],
					)
				} else {
					let t = if is_lag {
						(row as i64).checked_sub(offset)
					} else {
						(row as i64).checked_add(offset)
					};
					t.filter(|t| {
						*t >= partition_start[row] as i64 && *t <= partition_end[row] as i64
					})
					.map(|t| t as usize)
				};
				targets.push(target);
			}
		}
		ValueFunction::FirstValue | ValueFunction::LastValue | ValueFunction::NthValue { .. } => {
			let slices = value_frame_slices(
				frame,
				range,
				is_peer_group_end,
				partition_start,
				partition_end,
				rows,
			)?;
			for slice in &slices {
				targets.push(slice.and_then(|(lo, hi)| {
					if ignore_nulls {
						// ADR 252: first / last / n-th NON-NULL row in the frame slice.
						match value_fn {
							ValueFunction::FirstValue => {
								(lo..=hi).find(|&i| !sorted_value.is_null(i))
							}
							ValueFunction::LastValue => {
								(lo..=hi).rev().find(|&i| !sorted_value.is_null(i))
							}
							ValueFunction::NthValue { n } => (lo..=hi)
								.filter(|&i| !sorted_value.is_null(i))
								.nth((*n as usize).saturating_sub(1)),
							_ => None,
						}
					} else {
						match value_fn {
							ValueFunction::FirstValue => Some(lo),
							ValueFunction::LastValue => Some(hi),
							ValueFunction::NthValue { n } => {
								let idx = lo + (*n as usize) - 1;
								(idx <= hi).then_some(idx)
							}
							_ => None,
						}
					}
				}));
			}
		}
	}

	// Take the value at each target; a `None` target (edge / empty frame / out-of-range) yields NULL.
	let index: UInt32Array = targets
		.iter()
		.map(|target| target.map(|i| u32::try_from(i).unwrap_or(u32::MAX)))
		.collect();
	let taken = take(sorted_value, &index, None)?;

	// LAG/LEAD with an explicit (non-NULL) default: fill the out-of-partition positions with it.
	let default = match value_fn {
		ValueFunction::Lag { default, .. } | ValueFunction::Lead { default, .. } => {
			default.as_ref()
		}
		_ => None,
	};
	match default {
		Some(default) => {
			let default_array = expr::literal_array(default, rows)?;
			let default_array = if default_array.data_type() == output_type {
				default_array
			} else {
				evaluate_cast(&default_array, output_type)?
			};
			let edge_mask: BooleanArray = targets
				.iter()
				.map(|target| Some(target.is_none()))
				.collect();
			Ok(arrow::compute::kernels::zip::zip(
				&edge_mask,
				&default_array.as_ref(),
				&taken.as_ref(),
			)?)
		}
		None => Ok(taken),
	}
}

fn fresh_window_agg_state(
	function: &AggregateFunction,
	arg_type: Option<&arrow::datatypes::DataType>,
) -> Result<Option<AggState>, ExecutionError> {
	let require_arg = || ExecutionError::InvalidExpression {
		message: "window aggregate requires an argument type".to_string(),
	};
	Ok(match function {
		AggregateFunction::CountAll | AggregateFunction::Count => None,
		AggregateFunction::Sum => Some(sum_state_for_data_type(arg_type.ok_or_else(require_arg)?)),
		AggregateFunction::Avg => Some(avg_state_for_data_type(arg_type.ok_or_else(require_arg)?)?),
		AggregateFunction::Min => Some(min_state_for_data_type(arg_type.ok_or_else(require_arg)?)?),
		AggregateFunction::Max => Some(max_state_for_data_type(arg_type.ok_or_else(require_arg)?)?),
		AggregateFunction::LinearRegress | AggregateFunction::ApproxQuantile(_) => None,
	})
}

fn take_all(arrays: &[ArrayRef], indices: &UInt32Array) -> Result<Vec<ArrayRef>, ExecutionError> {
	arrays
		.iter()
		.map(|array| take(array, indices, None).map_err(ExecutionError::from))
		.collect()
}

/// Build a total-order row encoding of `arrays` so adjacent rows can be compared for equality across
/// any column types (including NULLs). Returns `None` when there are no columns (every row is equal).
fn window_comparison_rows(arrays: &[ArrayRef]) -> Result<Option<arrow::row::Rows>, ExecutionError> {
	if arrays.is_empty() {
		return Ok(None);
	}
	let fields = arrays
		.iter()
		.map(|array| arrow::row::SortField::new(array.data_type().clone()))
		.collect::<Vec<_>>();
	let converter = arrow::row::RowConverter::new(fields)?;
	Ok(Some(converter.convert_columns(arrays)?))
}

fn window_rows_equal(rows: &Option<arrow::row::Rows>, a: usize, b: usize) -> bool {
	match rows {
		Some(rows) => rows.row(a) == rows.row(b),
		None => true,
	}
}

fn encode_partition_key_value(
	array: &dyn Array,
	row: usize,
	encoded: &mut Vec<u8>,
) -> Result<(), ExecutionError> {
	if array.is_null(row) {
		encoded.push(0);
		return Ok(());
	}

	match array.data_type() {
		DataType::Boolean => {
			let values = array
				.as_any()
				.downcast_ref::<BooleanArray>()
				.expect("boolean data type");
			encoded.push(1);
			encoded.push(u8::from(values.value(row)));
		}
		DataType::Int8 => {
			let values = array
				.as_any()
				.downcast_ref::<Int8Array>()
				.expect("int8 data type");
			encoded.push(2);
			encoded.extend_from_slice(&values.value(row).to_le_bytes());
		}
		DataType::Int16 => {
			let values = array
				.as_any()
				.downcast_ref::<Int16Array>()
				.expect("int16 data type");
			encoded.push(3);
			encoded.extend_from_slice(&values.value(row).to_le_bytes());
		}
		DataType::Int32 => {
			let values = array
				.as_any()
				.downcast_ref::<Int32Array>()
				.expect("int32 data type");
			encoded.push(4);
			encoded.extend_from_slice(&values.value(row).to_le_bytes());
		}
		DataType::Date32 => {
			let values = array
				.as_any()
				.downcast_ref::<Date32Array>()
				.expect("date32 data type");
			encoded.push(13);
			encoded.extend_from_slice(&values.value(row).to_le_bytes());
		}
		DataType::Timestamp(TimeUnit::Millisecond, Some(timezone))
			if timezone.as_ref() == "UTC" =>
		{
			let values = array
				.as_any()
				.downcast_ref::<TimestampMillisecondArray>()
				.expect("timestamp millisecond data type");
			encoded.push(14);
			encoded.extend_from_slice(&values.value(row).to_le_bytes());
		}
		DataType::Timestamp(TimeUnit::Microsecond, Some(timezone))
			if timezone.as_ref() == "UTC" =>
		{
			let values = array
				.as_any()
				.downcast_ref::<TimestampMicrosecondArray>()
				.expect("timestamp microsecond data type");
			encoded.push(15);
			encoded.extend_from_slice(&values.value(row).to_le_bytes());
		}
		DataType::Timestamp(TimeUnit::Nanosecond, Some(timezone)) if timezone.as_ref() == "UTC" => {
			let values = array
				.as_any()
				.downcast_ref::<TimestampNanosecondArray>()
				.expect("timestamp nanosecond data type");
			encoded.push(16);
			encoded.extend_from_slice(&values.value(row).to_le_bytes());
		}
		DataType::Int64 => {
			let values = array
				.as_any()
				.downcast_ref::<Int64Array>()
				.expect("int64 data type");
			encoded.push(5);
			encoded.extend_from_slice(&values.value(row).to_le_bytes());
		}
		DataType::UInt8 => {
			let values = array
				.as_any()
				.downcast_ref::<UInt8Array>()
				.expect("uint8 data type");
			encoded.push(6);
			encoded.extend_from_slice(&values.value(row).to_le_bytes());
		}
		DataType::UInt16 => {
			let values = array
				.as_any()
				.downcast_ref::<UInt16Array>()
				.expect("uint16 data type");
			encoded.push(7);
			encoded.extend_from_slice(&values.value(row).to_le_bytes());
		}
		DataType::UInt32 => {
			let values = array
				.as_any()
				.downcast_ref::<UInt32Array>()
				.expect("uint32 data type");
			encoded.push(8);
			encoded.extend_from_slice(&values.value(row).to_le_bytes());
		}
		DataType::UInt64 => {
			let values = array
				.as_any()
				.downcast_ref::<UInt64Array>()
				.expect("uint64 data type");
			encoded.push(9);
			encoded.extend_from_slice(&values.value(row).to_le_bytes());
		}
		DataType::Float32 => {
			let values = array
				.as_any()
				.downcast_ref::<Float32Array>()
				.expect("float32 data type");
			encoded.push(10);
			encoded.extend_from_slice(&values.value(row).to_bits().to_le_bytes());
		}
		DataType::Float64 => {
			let values = array
				.as_any()
				.downcast_ref::<Float64Array>()
				.expect("float64 data type");
			encoded.push(11);
			encoded.extend_from_slice(&values.value(row).to_bits().to_le_bytes());
		}
		DataType::Utf8 => {
			let values = array
				.as_any()
				.downcast_ref::<StringArray>()
				.expect("utf8 data type");
			let value = values.value(row).as_bytes();
			encoded.push(12);
			encoded.extend_from_slice(
				&u32::try_from(value.len())
					.map_err(|_| ExecutionError::InvalidExpression {
						message: "partition key string is too large to encode".to_string(),
					})?
					.to_le_bytes(),
			);
			encoded.extend_from_slice(value);
		}
		// ADR 275: Decimal128 group/partition key. One (p,s) per column, so the raw i128 partitions exactly
		// (equal Decimal value → equal bytes → same partition); tag 17 is the next unused after the Timestamp
		// units (14..16).
		DataType::Decimal128(_, _) => {
			let values = array
				.as_any()
				.downcast_ref::<Decimal128Array>()
				.expect("decimal128 data type");
			encoded.push(17);
			encoded.extend_from_slice(&values.value(row).to_le_bytes());
		}
		data_type => {
			return Err(ExecutionError::InvalidExpression {
				message: format!("unsupported exchange partition key type: {data_type:?}"),
			});
		}
	}

	Ok(())
}

// `value` fits a Decimal128 of `precision` iff its magnitude is below 10^precision. precision <= 38, so
// 10^precision is always representable in i128.
fn decimal_fits_precision(value: i128, precision: u8) -> bool {
	match 10_i128.checked_pow(u32::from(precision)) {
		Some(limit) => value.unsigned_abs() < limit.unsigned_abs(),
		None => true,
	}
}

/// ADR 271: scalar `Decimal128 / Decimal128`. A custom i128 loop — NOT Arrow `numeric::div`, which errors on a
/// zero divisor. A NULL operand or a zero divisor yields a NULL slot (ADR 084); the quotient rounds half away
/// from zero (reusing ADR 268's remainder check) in the direction of the true quotient's sign
/// (`sign(num)*sign(divisor)`), so a negative denominator rounds correctly; the output `(p,s)` is the shared
/// `decimal_division_output_type` (so the executor's stamp matches the binder's bound type).
fn local_scan_table_pin(table: TableId) -> ScanTablePin {
	let mut object_id = [0u8; 16];
	object_id[8..].copy_from_slice(&table.0.to_be_bytes());
	let arrow_schema = arrow::datatypes::Schema::new(vec![arrow::datatypes::Field::new(
		"id",
		arrow::datatypes::DataType::Int64,
		false,
	)]);
	let descriptor = TableSchemaDescriptor::new(
		TableObjectId::new(object_id),
		1,
		"local",
		"legacy",
		&arrow_schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
		LocalityPolicy::default_table_spread(),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.expect("local scan descriptor must be valid");
	ScanTablePin {
		table_object_id: descriptor.table_object_id,
		physical_descriptor_fingerprint: descriptor.physical_descriptor_fingerprint,
	}
}

#[cfg(test)]
mod tests {
	use super::join::*;
	use super::*;
	use crate::binder::{PlannerConfig, bind};
	use crate::catalog::{MockCatalog, TableRef};
	use crate::distributed::cancellation::DownstreamAttemptRegistry;
	use crate::distributed::determinism::{SortPolicy, canonical_arrow_ipc_digest};
	use crate::error::StorageError;
	use crate::ir::{
		BoundExprProgram, BoundExprProgramBuilder, ColumnName, ColumnOrdinal, MorselId, PbcProgram,
		QueryId, QueryProfileHint, ResolvedColumn, SessionId, TableId,
	};
	use crate::storage::InMemoryStorageScan;
	use crate::storage::ScanStream;
	use crate::storage::{HlcBound, RowIdentityProjection, TenantId, TraceContext};
	use arrow::array::{
		Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, StringArray,
		UInt8Array, UInt16Array, UInt32Array, UInt64Array,
	};
	use arrow::datatypes::{DataType, Field, Schema};
	use std::hash::{DefaultHasher, Hash, Hasher};
	use std::sync::{Arc, Mutex};
	use wendelon::schema::{
		LocalityPolicy, PrimaryKeyDescriptor, RowIdentityPolicy, RuntimeFilterBloom,
		RuntimeFilterDescriptor, RuntimeFilterKeyType, RuntimeFilterKind, SchemaEvolutionPolicy,
		TableObjectId, TableSchemaDescriptor,
	};

	// ADR 271: the scalar Decimal `/` output type — grounded Arrow shape, validated explicitly.
	// ADR 271: the scalar Decimal `/` kernel — half-away-from-zero rounding (discriminated from truncation),
	// a NEGATIVE denominator (rounds away in the quotient's sign), div-by-zero -> NULL (ADR 084), and NULL
	// operand propagation. The narrow integration fixture can't discriminate these.
	#[tokio::test]
	async fn execute_rejects_unbound_drop_table() {
		let executor = Executor::new(Arc::new(InMemoryStorageScan {
			table_data: RecordBatch::new_empty(Arc::new(Schema::empty())),
		}));
		let plan = LogicalPlan::DropTable {
			name: TableRef::new("sales"),
			if_exists: false,
		};

		let err = executor
			.execute(&plan, None)
			.await
			.expect_err("executor should reject unbound DROP TABLE");

		assert!(matches!(
			err,
			ExecutionError::NotImplemented {
				feature: "unbound DROP TABLE execution"
			}
		));
	}

	#[tokio::test]
	async fn execute_bound_engages_pbc_at_storage_boundary() {
		let batch = batch();
		let requests = Arc::new(Mutex::new(Vec::new()));
		let storage = Arc::new(RecordingStorageScan {
			table_data: batch,
			requests: requests.clone(),
		});
		let context = ExecutionContext::new(
			SessionId(77),
			QueryId::from_u128(88),
			TenantId(99),
			HlcBound(123),
		);
		let executor = Executor::with_context(storage, context);
		let catalog = catalog();
		let logical = LogicalPlan::Filter {
			input: Box::new(LogicalPlan::Scan {
				table: TableId(1),
				projection: None,
			}),
			predicate: Expr::Gt(
				Box::new(Expr::column("amount")),
				Box::new(Expr::Literal(ScalarValue::Int32(Some(100)))),
			),
		};

		let bound = bind(&logical, &catalog, &PlannerConfig::default())
			.await
			.expect("bind");
		let results = executor
			.execute_bound(&bound, None)
			.await
			.expect("execute bound");

		assert_eq!(results[0].num_rows(), 2);
		let observed = requests.lock().unwrap();
		assert_eq!(observed.len(), 1);
		assert_eq!(observed[0].session_id, SessionId(77));
		assert_eq!(observed[0].query_id, QueryId::from_u128(88));
		assert_eq!(observed[0].tenant_id, TenantId(99));
		assert_eq!(observed[0].read_bound, HlcBound(123));
		assert_eq!(observed[0].table_object_id, descriptor().table_object_id);
		assert_eq!(
			observed[0].physical_descriptor_fingerprint,
			descriptor().physical_descriptor_fingerprint
		);
		assert!(observed[0].pbc_predicate.is_some());
		assert_eq!(observed[0].profile_hint, Some(QueryProfileHint::Scan));
	}

	#[tokio::test]
	async fn execute_bound_threads_runtime_filter_to_scan_request() {
		let batch = batch();
		let requests = Arc::new(Mutex::new(Vec::new()));
		let storage = Arc::new(RecordingStorageScan {
			table_data: batch.clone(),
			requests: requests.clone(),
		});
		let descriptor = descriptor();
		let runtime_filter = RuntimeFilterDescriptor {
			key_encoding_version: 1,
			key_types: vec![RuntimeFilterKeyType::Int64],
			probe_key_columns: vec![0],
			kind: RuntimeFilterKind::Bloom(RuntimeFilterBloom {
				hash_seed: 7,
				bit_count: 8,
				hash_count: 1,
				byte_budget: 1,
				target_fp_rate_bp: 100,
				payload: vec![0b1010_1010],
			}),
			probe_descriptor_fingerprint: descriptor.physical_descriptor_fingerprint,
		};
		let context = ExecutionContext::new(
			SessionId(77),
			QueryId::from_u128(88),
			TenantId(99),
			HlcBound(123),
		)
		.with_runtime_filter(Some(runtime_filter.clone()));
		let executor = Executor::with_context(storage, context);
		let bound = ExecutionPlan::Scan {
			local_table_id: TableId(1),
			table: descriptor.table_object_id,
			fingerprint: descriptor.physical_descriptor_fingerprint,
			schema: batch.schema(),
			projection: None,
			pbc_program: None,
			row_identity: crate::storage::RowIdentityProjection::None,
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
		};

		executor
			.execute_bound(&bound, None)
			.await
			.expect("execute bound");

		let observed = requests.lock().unwrap();
		assert_eq!(observed.len(), 1);
		assert_eq!(observed[0].runtime_filter, Some(runtime_filter));
	}

	#[test]
	fn bound_scan_request_carries_every_bound_scan_field() {
		let batch = batch();
		let descriptor = descriptor();
		let runtime_filter = RuntimeFilterDescriptor {
			key_encoding_version: 1,
			key_types: vec![RuntimeFilterKeyType::Int64],
			probe_key_columns: vec![0],
			kind: RuntimeFilterKind::Bloom(RuntimeFilterBloom {
				hash_seed: 11,
				bit_count: 16,
				hash_count: 2,
				byte_budget: 2,
				target_fp_rate_bp: 100,
				payload: vec![0b1010_1010, 0b0101_0101],
			}),
			probe_descriptor_fingerprint: descriptor.physical_descriptor_fingerprint,
		};
		let downstream_registry = DownstreamAttemptRegistry::with_default_limits(None);
		let downstream_context = downstream_registry.context();
		let trace_context = TraceContext {
			trace_id: [3; 16],
			span_id: [4; 8],
		};
		let context = ExecutionContext::new(
			SessionId(77),
			QueryId::from_u128(88),
			TenantId(99),
			HlcBound(123),
		)
		.with_runtime_filter(Some(runtime_filter.clone()))
		.with_downstream_cancellation(Some(downstream_context.clone()));
		let mut context = context;
		context.trace_context = Some(trace_context);
		let executor = Executor::with_context(
			Arc::new(InMemoryStorageScan {
				table_data: RecordBatch::new_empty(Arc::new(Schema::empty())),
			}),
			context,
		);
		let scan = ExecutionPlan::Scan {
			local_table_id: TableId(42),
			table: descriptor.table_object_id,
			fingerprint: descriptor.physical_descriptor_fingerprint,
			schema: batch.schema(),
			projection: Some(vec![ColumnOrdinal(2), ColumnOrdinal(0)]),
			pbc_program: Some(PbcProgram(vec![1, 3, 5, 7])),
			row_identity: RowIdentityProjection::Append,
			row_limit: Some(17),
			profile_hint: QueryProfileHint::PointRead,
			estimated_byte_size: None,
			primary_key_columns: vec![ColumnOrdinal(0)],
			column_ndv: Vec::new(),
			column_range: Vec::new(),
			column_range_distributions: Vec::new(),
			column_heavy_hitters: Vec::new(),
			column_group_top_k: Default::default(),
			column_group_ndv: Default::default(),
		};

		let request = executor
			.bound_scan_request(&scan, Some((MorselId(7), 11)))
			.expect("scan request");

		assert_eq!(request.session_id, SessionId(77));
		assert_eq!(request.query_id, QueryId::from_u128(88));
		assert_eq!(request.tenant_id, TenantId(99));
		assert_eq!(request.table_id, TableId(42));
		assert_eq!(request.table_object_id, descriptor.table_object_id);
		let fingerprint = descriptor.physical_descriptor_fingerprint;
		assert_eq!(request.physical_descriptor_fingerprint, fingerprint);
		assert_eq!(request.read_bound, HlcBound(123));
		assert_eq!(
			request.projected_columns,
			Some(vec![ColumnOrdinal(2), ColumnOrdinal(0)])
		);
		assert_eq!(request.pbc_predicate, Some(vec![1, 3, 5, 7]));
		assert_eq!(request.runtime_filter, Some(runtime_filter));
		assert_eq!(request.row_identity, RowIdentityProjection::Append);
		assert_eq!(request.profile_hint, Some(QueryProfileHint::PointRead));
		assert_eq!(request.trace_context, Some(trace_context));
		assert_eq!(request.morsel, Some((MorselId(7), 11)));
		assert_eq!(request.row_limit, Some(17));
		assert!(
			!request
				.downstream_cancellation
				.as_ref()
				.unwrap()
				.is_cancelled()
		);
	}

	#[tokio::test]
	async fn execute_bound_sends_point_read_profile_for_primary_key_equality() {
		let batch = batch();
		let requests = Arc::new(Mutex::new(Vec::new()));
		let storage = Arc::new(RecordingStorageScan {
			table_data: batch,
			requests: requests.clone(),
		});
		let executor = Executor::new(storage);
		let catalog = catalog();
		let logical = LogicalPlan::Filter {
			input: Box::new(LogicalPlan::Scan {
				table: TableId(1),
				projection: None,
			}),
			predicate: Expr::Eq(
				Box::new(Expr::column("id")),
				Box::new(Expr::Literal(ScalarValue::Int64(Some(2)))),
			),
		};

		let bound = bind(&logical, &catalog, &PlannerConfig::default())
			.await
			.expect("bind");
		let results = executor
			.execute_bound(&bound, None)
			.await
			.expect("execute bound");

		assert_eq!(results[0].num_rows(), 1);
		let observed = requests.lock().unwrap();
		assert_eq!(observed[0].profile_hint, Some(QueryProfileHint::PointRead));
		assert!(observed[0].pbc_predicate.is_some());
	}

	#[tokio::test]
	async fn execute_bound_remaps_project_after_scan_projection() {
		let batch = batch();
		let requests = Arc::new(Mutex::new(Vec::new()));
		let storage = Arc::new(RecordingStorageScan {
			table_data: batch,
			requests: requests.clone(),
		});
		let executor = Executor::new(storage);
		let catalog = catalog();
		let logical = LogicalPlan::Project {
			input: Box::new(LogicalPlan::Filter {
				input: Box::new(LogicalPlan::Scan {
					table: TableId(1),
					projection: None,
				}),
				predicate: Expr::Gt(
					Box::new(Expr::column("amount")),
					Box::new(Expr::Literal(ScalarValue::Int32(Some(100)))),
				),
			}),
			output_names: vec![None],
			exprs: vec![Expr::column("id")],
		};

		let bound = bind(&logical, &catalog, &PlannerConfig::default())
			.await
			.expect("bind");
		let results = executor
			.execute_bound(&bound, None)
			.await
			.expect("execute bound");
		let ids = results[0]
			.column(0)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("id column");

		assert_eq!(ids.values(), &[2, 4]);
		let observed = requests.lock().unwrap();
		assert_eq!(observed[0].projected_columns, Some(vec![ColumnOrdinal(0)]));
		assert!(observed[0].pbc_predicate.is_some());
	}

	#[tokio::test]
	async fn execute_bound_grouped_sum_uses_projected_scan_ordinals() {
		let batch = batch();
		let requests = Arc::new(Mutex::new(Vec::new()));
		let storage = Arc::new(RecordingStorageScan {
			table_data: batch,
			requests: requests.clone(),
		});
		let executor = Executor::new(storage);
		let catalog = catalog();
		let logical = LogicalPlan::Aggregate {
			input: Box::new(LogicalPlan::Scan {
				table: TableId(1),
				projection: None,
			}),
			group_expr: vec![Expr::column("region")],
			aggr_expr: vec![(AggregateFunction::Sum, vec![Expr::column("amount")])],
		};

		let bound = bind(&logical, &catalog, &PlannerConfig::default())
			.await
			.expect("bind");
		let results = executor
			.execute_bound(&bound, None)
			.await
			.expect("execute bound");
		let groups = results[0]
			.column(0)
			.as_any()
			.downcast_ref::<StringArray>()
			.expect("group column");
		let sums = results[0]
			.column(1)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("sum column");

		assert_eq!(groups.value(0), "east");
		assert_eq!(sums.value(0), 150);
		assert_eq!(groups.value(1), "north");
		assert_eq!(sums.value(1), 10);
		assert_eq!(groups.value(2), "west");
		assert_eq!(sums.value(2), 200);
		let observed = requests.lock().unwrap();
		assert_eq!(
			observed[0].projected_columns,
			Some(vec![ColumnOrdinal(1), ColumnOrdinal(2)])
		);
	}

	#[tokio::test]
	async fn execute_bound_sum_all_null_returns_null_and_count_excludes_nulls() {
		let schema = Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("amount", DataType::Int32, true),
			Field::new("region", DataType::Utf8, true),
		]));
		let batch = RecordBatch::try_new(
			schema,
			vec![
				Arc::new(Int64Array::from(vec![1, 2])),
				Arc::new(Int32Array::from(vec![None, None])),
				Arc::new(StringArray::from(vec![Some("east"), Some("west")])),
			],
		)
		.expect("batch");
		let executor = Executor::new(Arc::new(InMemoryStorageScan { table_data: batch }));
		let catalog = catalog();
		let logical = LogicalPlan::Aggregate {
			input: Box::new(LogicalPlan::Scan {
				table: TableId(1),
				projection: None,
			}),
			group_expr: vec![],
			aggr_expr: vec![
				(AggregateFunction::Sum, vec![Expr::column("amount")]),
				(AggregateFunction::Count, vec![Expr::column("amount")]),
			],
		};

		let bound = bind(&logical, &catalog, &PlannerConfig::default())
			.await
			.expect("bind");
		let results = executor
			.execute_bound(&bound, None)
			.await
			.expect("execute bound aggregate");
		let sum = results[0]
			.column(0)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("sum column");
		let count = results[0]
			.column(1)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("count column");

		assert!(sum.is_null(0));
		assert_eq!(count.value(0), 0);
	}

	#[tokio::test]
	async fn execute_bound_sum_all_null_numeric_types_return_nulls() {
		let schema = Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("i8", DataType::Int8, true),
			Field::new("i16", DataType::Int16, true),
			Field::new("i32", DataType::Int32, true),
			Field::new("i64", DataType::Int64, true),
			Field::new("u8", DataType::UInt8, true),
			Field::new("u16", DataType::UInt16, true),
			Field::new("u32", DataType::UInt32, true),
			Field::new("u64", DataType::UInt64, true),
			Field::new("f32", DataType::Float32, true),
			Field::new("f64", DataType::Float64, true),
		]));
		let batch = RecordBatch::try_new(
			schema.clone(),
			vec![
				Arc::new(Int64Array::from(vec![1, 2])),
				Arc::new(Int8Array::from(vec![None::<i8>, None])),
				Arc::new(Int16Array::from(vec![None::<i16>, None])),
				Arc::new(Int32Array::from(vec![None::<i32>, None])),
				Arc::new(Int64Array::from(vec![None::<i64>, None])),
				Arc::new(UInt8Array::from(vec![None::<u8>, None])),
				Arc::new(UInt16Array::from(vec![None::<u16>, None])),
				Arc::new(UInt32Array::from(vec![None::<u32>, None])),
				Arc::new(UInt64Array::from(vec![None::<u64>, None])),
				Arc::new(Float32Array::from(vec![None::<f32>, None])),
				Arc::new(Float64Array::from(vec![None::<f64>, None])),
			],
		)
		.expect("batch");
		let mut catalog = MockCatalog::new();
		catalog
			.register_descriptor(TableId(1), descriptor_with_schema(schema.as_ref()))
			.expect("register descriptor");
		let executor = Executor::new(Arc::new(InMemoryStorageScan { table_data: batch }));
		let logical = LogicalPlan::Aggregate {
			input: Box::new(LogicalPlan::Scan {
				table: TableId(1),
				projection: None,
			}),
			group_expr: vec![],
			aggr_expr: [
				"i8", "i16", "i32", "i64", "u8", "u16", "u32", "u64", "f32", "f64",
			]
			.into_iter()
			.map(|column| (AggregateFunction::Sum, vec![Expr::column(column)]))
			.collect(),
		};

		let bound = bind(&logical, &catalog, &PlannerConfig::default())
			.await
			.expect("bind");
		let results = executor
			.execute_bound(&bound, None)
			.await
			.expect("execute bound aggregate");

		assert_eq!(results[0].num_columns(), 10);
		for column in results[0].columns() {
			assert!(
				column.is_null(0),
				"expected null SUM for {:?}",
				column.data_type()
			);
		}
		assert_eq!(results[0].schema().field(8).data_type(), &DataType::Float64);
		assert_eq!(results[0].schema().field(9).data_type(), &DataType::Float64);
	}

	#[tokio::test]
	async fn execute_bound_count_all_empty_table_returns_single_zero() {
		let schema = Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("amount", DataType::Int32, true),
			Field::new("region", DataType::Utf8, true),
		]));
		let batch = RecordBatch::try_new(
			schema.clone(),
			vec![
				Arc::new(Int64Array::from(Vec::<i64>::new())),
				Arc::new(Int32Array::from(Vec::<Option<i32>>::new())),
				Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
			],
		)
		.expect("empty batch");
		let mut catalog = MockCatalog::new();
		catalog
			.register_descriptor(TableId(1), descriptor_with_schema(schema.as_ref()))
			.expect("register descriptor");
		let executor = Executor::new(Arc::new(InMemoryStorageScan { table_data: batch }));
		let logical = LogicalPlan::Aggregate {
			input: Box::new(LogicalPlan::Scan {
				table: TableId(1),
				projection: None,
			}),
			group_expr: vec![],
			aggr_expr: vec![(AggregateFunction::CountAll, vec![])],
		};

		let bound = bind(&logical, &catalog, &PlannerConfig::default())
			.await
			.expect("bind");
		let results = executor
			.execute_bound(&bound, None)
			.await
			.expect("execute bound aggregate");
		let count = results[0]
			.column(0)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("count column");

		assert_eq!(results[0].num_rows(), 1);
		assert_eq!(count.value(0), 0);
	}

	#[tokio::test]
	async fn execute_bound_grouped_sum_distinguishes_all_null_mixed_and_null_key_groups() {
		let schema = Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("amount", DataType::Int32, true),
			Field::new("region", DataType::Utf8, true),
		]));
		let batch = RecordBatch::try_new(
			schema.clone(),
			vec![
				Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6])),
				Arc::new(Int32Array::from(vec![
					None,
					None,
					None,
					Some(7),
					None,
					Some(3),
				])),
				Arc::new(StringArray::from(vec![
					Some("all_null"),
					Some("all_null"),
					Some("mixed"),
					Some("mixed"),
					None,
					None,
				])),
			],
		)
		.expect("batch");
		let mut catalog = MockCatalog::new();
		catalog
			.register_descriptor(TableId(1), descriptor_with_schema(schema.as_ref()))
			.expect("register descriptor");
		let executor = Executor::new(Arc::new(InMemoryStorageScan { table_data: batch }));
		let logical = LogicalPlan::Aggregate {
			input: Box::new(LogicalPlan::Scan {
				table: TableId(1),
				projection: None,
			}),
			group_expr: vec![Expr::column("region")],
			aggr_expr: vec![(AggregateFunction::Sum, vec![Expr::column("amount")])],
		};

		let bound = bind(&logical, &catalog, &PlannerConfig::default())
			.await
			.expect("bind");
		let results = executor
			.execute_bound(&bound, None)
			.await
			.expect("execute bound aggregate");
		let groups = results[0]
			.column(0)
			.as_any()
			.downcast_ref::<StringArray>()
			.expect("group column");
		let sums = results[0]
			.column(1)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("sum column");

		assert!(groups.is_null(0));
		assert_eq!(sums.value(0), 3);
		assert_eq!(groups.value(1), "all_null");
		assert!(sums.is_null(1));
		assert_eq!(groups.value(2), "mixed");
		assert_eq!(sums.value(2), 7);
	}

	#[tokio::test]
	async fn execute_bound_inner_join_matches_duplicates_and_drops_null_keys() {
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
				Arc::new(Int64Array::from(vec![10, 11, 12, 13])),
				Arc::new(Int64Array::from(vec![Some(1), Some(1), None, Some(2)])),
				Arc::new(StringArray::from(vec![
					Some("a"),
					Some("b"),
					Some("null"),
					Some("c"),
				])),
			],
		)
		.expect("left batch");
		let right_batch = RecordBatch::try_new(
			right_schema.clone(),
			vec![
				Arc::new(Int64Array::from(vec![20, 21, 22])),
				Arc::new(Int64Array::from(vec![Some(1), Some(3), None])),
				Arc::new(Int32Array::from(vec![Some(10), Some(30), Some(99)])),
			],
		)
		.expect("right batch");
		let storage = Arc::new(TableStorageScan {
			batches: std::collections::HashMap::from([
				(TableId(1), left_batch),
				(TableId(2), right_batch),
			]),
		});
		let executor = Executor::new(storage);
		let output_schema = Arc::new(Schema::new(vec![
			Field::new("pk", DataType::Int64, false),
			Field::new("id", DataType::Int64, true),
			Field::new("left_value", DataType::Utf8, true),
			Field::new("pk", DataType::Int64, false),
			Field::new("id", DataType::Int64, true),
			Field::new("right_value", DataType::Int32, true),
		]));
		let plan = ExecutionPlan::Join {
			replay_id: JoinReplayId::SYNTHETIC,
			replay_eligibility: BroadcastReplayEligibility::Terminal,
			left: Box::new(test_scan_plan(TableId(1), left_schema, 1)),
			right: Box::new(test_scan_plan(TableId(2), right_schema, 2)),
			kind: JoinKind::Inner,
			left_keys: vec![ColumnOrdinal(1)],
			right_keys: vec![ColumnOrdinal(1)],
			match_filter: None,
			null_safe_keys: false,
			strategy: JoinStrategy::HashPartitioned,
			output_schema,
		};

		let results = executor.execute_bound(&plan, None).await.expect("join");
		let left_keys = results[0]
			.column(1)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("left key");
		let right_values = results[0]
			.column(5)
			.as_any()
			.downcast_ref::<Int32Array>()
			.expect("right value");

		assert_eq!(results[0].num_rows(), 2);
		assert_eq!(left_keys.values(), &[1, 1]);
		assert_eq!(right_values.values(), &[10, 10]);
	}

	#[tokio::test]
	async fn execute_bound_inner_join_supports_multi_column_keys_and_normalized_digest() {
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
		let output_schema = Arc::new(Schema::new(vec![
			Field::new("pk", DataType::Int64, false),
			Field::new("k1", DataType::Int64, false),
			Field::new("k2", DataType::Int64, false),
			Field::new("left_value", DataType::Utf8, true),
			Field::new("pk", DataType::Int64, false),
			Field::new("k1", DataType::Int64, false),
			Field::new("k2", DataType::Int64, false),
			Field::new("right_value", DataType::Int32, true),
		]));
		let plan = ExecutionPlan::Join {
			replay_id: JoinReplayId::SYNTHETIC,
			replay_eligibility: BroadcastReplayEligibility::Terminal,
			left: Box::new(test_scan_plan(TableId(1), left_schema.clone(), 1)),
			right: Box::new(test_scan_plan(TableId(2), right_schema.clone(), 2)),
			kind: JoinKind::Inner,
			left_keys: vec![ColumnOrdinal(1), ColumnOrdinal(2)],
			right_keys: vec![ColumnOrdinal(1), ColumnOrdinal(2)],
			match_filter: None,
			null_safe_keys: false,
			strategy: JoinStrategy::HashPartitioned,
			output_schema,
		};
		let right_batch = RecordBatch::try_new(
			right_schema.clone(),
			vec![
				Arc::new(Int64Array::from(vec![20, 21])),
				Arc::new(Int64Array::from(vec![1, 1])),
				Arc::new(Int64Array::from(vec![10, 12])),
				Arc::new(Int32Array::from(vec![Some(100), Some(120)])),
			],
		)
		.expect("right batch");
		let left_a = RecordBatch::try_new(
			left_schema.clone(),
			vec![
				Arc::new(Int64Array::from(vec![1, 2, 3])),
				Arc::new(Int64Array::from(vec![1, 1, 1])),
				Arc::new(Int64Array::from(vec![10, 11, 10])),
				Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("c")])),
			],
		)
		.expect("left batch a");
		let left_b = RecordBatch::try_new(
			left_schema,
			vec![
				Arc::new(Int64Array::from(vec![3, 2, 1])),
				Arc::new(Int64Array::from(vec![1, 1, 1])),
				Arc::new(Int64Array::from(vec![10, 11, 10])),
				Arc::new(StringArray::from(vec![Some("c"), Some("b"), Some("a")])),
			],
		)
		.expect("left batch b");

		let results_a = Executor::new(Arc::new(TableStorageScan {
			batches: std::collections::HashMap::from([
				(TableId(1), left_a),
				(TableId(2), right_batch.clone()),
			]),
		}))
		.execute_bound(&plan, None)
		.await
		.expect("join a");
		let results_b = Executor::new(Arc::new(TableStorageScan {
			batches: std::collections::HashMap::from([
				(TableId(1), left_b),
				(TableId(2), right_batch),
			]),
		}))
		.execute_bound(&plan, None)
		.await
		.expect("join b");

		let labels = results_a[0]
			.column(3)
			.as_any()
			.downcast_ref::<StringArray>()
			.expect("left label");
		let right_values = results_a[0]
			.column(7)
			.as_any()
			.downcast_ref::<Int32Array>()
			.expect("right value");

		assert_eq!(results_a[0].num_rows(), 2);
		assert_eq!(labels.value(0), "a");
		assert_eq!(labels.value(1), "c");
		assert_eq!(right_values.values(), &[100, 100]);
		assert_eq!(
			canonical_arrow_ipc_digest(&results_a, SortPolicy::FullRowLex).expect("digest a"),
			canonical_arrow_ipc_digest(&results_b, SortPolicy::FullRowLex).expect("digest b")
		);
	}

	#[tokio::test]
	async fn execute_bound_inner_join_spills_build_when_memory_cap_is_low() {
		let (executor, plan) = duplicate_join_fixture(
			2_000,
			TaskBudget {
				memory: ByteCount::new(80 * 1024),
				spill_bytes: ByteCount::new(1024 * 1024),
				..TaskBudget::from_query_budget(QueryBudget::test_default())
			},
		);

		let results = executor
			.execute_bound(&plan, None)
			.await
			.expect("spilled join");

		assert_eq!(results[0].num_rows(), 2_000);
	}

	#[tokio::test]
	async fn execute_bound_inner_join_spill_cap_exhaustion_is_typed() {
		let (executor, plan) = duplicate_join_fixture(
			2_000,
			TaskBudget {
				memory: ByteCount::new(80 * 1024),
				spill_bytes: ByteCount::new(32),
				..TaskBudget::from_query_budget(QueryBudget::test_default())
			},
		);

		let err = executor
			.execute_bound(&plan, None)
			.await
			.expect_err("spill cap must fail");

		assert!(matches!(
			err,
			ExecutionError::BudgetExhausted {
				dimension: BudgetDimension::SpillBytes,
				..
			}
		));
	}

	#[tokio::test]
	async fn execute_bound_inner_join_spill_index_counts_against_memory_cap() {
		let (executor, plan) = duplicate_join_fixture(
			2_000,
			TaskBudget {
				memory: ByteCount::new(1024),
				spill_bytes: ByteCount::new(1024 * 1024),
				..TaskBudget::from_query_budget(QueryBudget::test_default())
			},
		);

		let err = executor
			.execute_bound(&plan, None)
			.await
			.expect_err("resident spill state must fail memory cap");

		assert!(matches!(
			err,
			ExecutionError::BudgetExhausted {
				dimension: BudgetDimension::Memory,
				..
			}
		));
	}

	#[tokio::test]
	async fn streamed_spilled_inner_join_retrieves_build_columns_and_preserves_order() {
		let rows = 1_024usize;
		let low_budget = TaskBudget {
			memory: ByteCount::new(16 * 1024),
			spill_bytes: ByteCount::new(64 * 1024 * 1024),
			..TaskBudget::from_query_budget(QueryBudget::test_default())
		};
		let (stream_executor, plan) = streamed_inner_join_fixture(rows, low_budget);
		let streamed = stream_executor
			.execute_bound(&plan, None)
			.await
			.expect("streamed spilled inner join");

		let (baseline_executor, baseline_plan) =
			streamed_inner_join_fixture(rows, high_test_budget());
		let materialized = baseline_executor
			.execute_bound(&baseline_plan, None)
			.await
			.expect("materialized inner join");

		assert_eq!(record_batch_row_count(&streamed), rows as u64);
		assert_eq!(
			canonical_arrow_ipc_digest(&streamed, SortPolicy::None).expect("streamed digest"),
			canonical_arrow_ipc_digest(&materialized, SortPolicy::None)
				.expect("materialized digest"),
			"spilled build-column retrieval must match materialized inner join order"
		);
		let labels = streamed[0]
			.column(2)
			.as_any()
			.downcast_ref::<StringArray>()
			.expect("left labels");
		assert_eq!(labels.value(0), "left-0");
		assert_eq!(labels.value(rows - 1), format!("left-{}", rows - 1));
	}

	#[tokio::test]
	async fn streamed_spilled_inner_join_writes_build_batches_without_concat() {
		let build_schema = Arc::new(Schema::new(vec![
			Field::new("pk", DataType::Int64, false),
			Field::new("id", DataType::Int64, false),
			Field::new("payload", DataType::Utf8, false),
		]));
		let batch_a = RecordBatch::try_new(
			build_schema.clone(),
			vec![
				Arc::new(Int64Array::from(vec![1, 2])),
				Arc::new(Int64Array::from(vec![1, 2])),
				Arc::new(StringArray::from(vec![Some("a"), Some("b")])),
			],
		)
		.expect("build batch a");
		let batch_b = RecordBatch::try_new(
			build_schema.clone(),
			vec![
				Arc::new(Int64Array::from(vec![3, 4])),
				Arc::new(Int64Array::from(vec![3, 4])),
				Arc::new(StringArray::from(vec![Some("c"), Some("d")])),
			],
		)
		.expect("build batch b");
		let key_plan = JoinKeyPlan::from_build_schema_and_probe_schema(
			&build_schema,
			&[ColumnOrdinal(1)],
			&build_schema,
			&[ColumnOrdinal(1)],
			false,
		)
		.expect("key plan");
		let budget = high_test_budget();
		let context = ExecutionContext::local_test();
		let mut spilled = SpilledInnerJoinBuild::create(
			budget,
			context.join_spill_file_ops,
			context.join_spill_dir,
		)
		.await
		.expect("create spilled build");

		spilled
			.write_batch_chunks(&batch_a, key_plan, &[ColumnOrdinal(1)], false, budget)
			.await
			.expect("write batch a");
		spilled
			.write_batch_chunks(&batch_b, key_plan, &[ColumnOrdinal(1)], false, budget)
			.await
			.expect("write batch b");

		assert_eq!(
			spilled.input_batches_written(),
			2,
			"regular inner build input must be written batch-by-batch, not concat_or_empty'd first"
		);
		assert_eq!(spilled.input_row_count(), 4);
		spilled.cleanup().await;
	}

	#[tokio::test]
	async fn streamed_spilled_inner_join_output_materialization_is_budgeted() {
		let rows = 1_024usize;
		let (executor, plan) = streamed_inner_join_fixture(
			rows,
			TaskBudget {
				memory: ByteCount::new(1),
				spill_bytes: ByteCount::new(64 * 1024 * 1024),
				..TaskBudget::from_query_budget(QueryBudget::test_default())
			},
		);

		let err = executor
			.execute_bound(&plan, None)
			.await
			.expect_err("oversized streamed output chunk must fail cleanly");

		assert!(
			matches!(
				err,
				ExecutionError::BudgetExhausted {
					dimension: BudgetDimension::Memory,
					producer: "streamed inner join build spill resident state"
						| "streamed inner join output materialization",
					..
				}
			),
			"unexpected streamed inner join error: {err:?}"
		);
	}

	#[tokio::test]
	async fn streamed_spilled_left_semi_join_retrieves_preserved_columns_and_deduplicates() {
		let rows = 1_024usize;
		let low_budget = TaskBudget {
			memory: ByteCount::new(16 * 1024),
			spill_bytes: ByteCount::new(64 * 1024 * 1024),
			..TaskBudget::from_query_budget(QueryBudget::test_default())
		};
		let (stream_executor, plan) = streamed_left_semi_join_fixture(rows, low_budget);
		let streamed = stream_executor
			.execute_bound(&plan, None)
			.await
			.expect("streamed spilled left semi join");

		let (baseline_executor, baseline_plan) =
			streamed_left_semi_join_fixture(rows, high_test_budget());
		let materialized = baseline_executor
			.execute_bound(&baseline_plan, None)
			.await
			.expect("materialized left semi join");

		assert_eq!(record_batch_row_count(&streamed), rows as u64);
		assert_eq!(
			canonical_arrow_ipc_digest(&streamed, SortPolicy::None).expect("streamed digest"),
			canonical_arrow_ipc_digest(&materialized, SortPolicy::None)
				.expect("materialized digest"),
			"spilled left-semi preserved rows must match materialized order and values"
		);
		let labels = streamed[0]
			.column(2)
			.as_any()
			.downcast_ref::<StringArray>()
			.expect("preserved labels");
		assert_eq!(labels.value(0), "left-0");
		assert_eq!(labels.value(rows - 1), format!("left-{}", rows - 1));
	}

	#[tokio::test]
	async fn partitioned_spilled_left_outer_join_matches_materialized_full_row() {
		let rows = 2_048usize;
		let left_schema = Arc::new(Schema::new(vec![
			Field::new("build_key", DataType::Int64, true),
			Field::new("build_id", DataType::Int64, false),
			Field::new("build_label", DataType::Utf8, false),
		]));
		let right_schema = Arc::new(Schema::new(vec![
			Field::new("probe_key", DataType::Int64, false),
			Field::new("probe_value", DataType::Int64, false),
		]));
		let build_keys = (0..rows)
			.map(|row| {
				if row % 257 == 0 {
					None
				} else {
					Some(i64::try_from(row % 193).expect("key fits"))
				}
			})
			.collect::<Vec<_>>();
		let build_ids = (0..rows)
			.map(|row| i64::try_from(row).expect("row fits"))
			.collect::<Vec<_>>();
		let build_labels = (0..rows)
			.map(|row| format!("left-{row}"))
			.collect::<Vec<_>>();
		let left_batch = RecordBatch::try_new(
			left_schema.clone(),
			vec![
				Arc::new(Int64Array::from(build_keys)),
				Arc::new(Int64Array::from(build_ids)),
				Arc::new(StringArray::from(build_labels)),
			],
		)
		.expect("left batch");
		let left_batches = vec![
			left_batch.slice(0, rows / 2),
			left_batch.slice(rows / 2, rows / 2),
		];
		let probe_keys = (0..256)
			.map(|row| i64::try_from(row % 193).expect("probe key fits"))
			.collect::<Vec<_>>();
		let probe_values = (0..256)
			.map(|row| i64::try_from(row * 10).expect("probe value fits"))
			.collect::<Vec<_>>();
		let right_batches = vec![
			RecordBatch::try_new(
				right_schema.clone(),
				vec![
					Arc::new(Int64Array::from(probe_keys)),
					Arc::new(Int64Array::from(probe_values)),
				],
			)
			.expect("right batch"),
		];
		let output_schema = Arc::new(Schema::new(
			left_schema
				.fields()
				.iter()
				.map(|field| field.as_ref().clone())
				.chain(
					right_schema
						.fields()
						.iter()
						.map(|field| Field::new(field.name(), field.data_type().clone(), true)),
				)
				.collect::<Vec<_>>(),
		));
		let low_budget = TaskBudget {
			memory: ByteCount::new(32 * 1024),
			spill_bytes: ByteCount::new(64 * 1024 * 1024),
			..TaskBudget::from_query_budget(QueryBudget::test_default())
		};
		let context = ExecutionContext::local_test();

		let streamed = try_execute_partitioned_spilled_left_outer_join(
			left_schema.clone(),
			right_schema.clone(),
			&left_batches,
			&right_batches,
			JoinKind::LeftOuter,
			&[ColumnOrdinal(0)],
			&[ColumnOrdinal(0)],
			None,
			false,
			JoinReplayId::SYNTHETIC,
			JoinStrategy::HashPartitioned,
			&output_schema,
			low_budget,
			None,
			JoinRuntimeObservationMode::Disabled,
			context.join_spill_file_ops,
			context.join_spill_dir,
		)
		.await
		.expect("partitioned left outer should execute")
		.expect("partitioned route should engage");
		assert_eq!(
			streamed.len(),
			1,
			"partitioned left outer preserves the join executor's single-batch result contract"
		);
		let materialized = execute_hash_join(
			left_schema,
			right_schema,
			&left_batches,
			&right_batches,
			JoinKind::LeftOuter,
			&[ColumnOrdinal(0)],
			&[ColumnOrdinal(0)],
			None,
			false,
			JoinReplayId::SYNTHETIC,
			JoinStrategy::HashPartitioned,
			&output_schema,
			high_test_budget(),
			None,
			JoinRuntimeObservationMode::Disabled,
			ExecutionContext::local_test().join_spill_file_ops,
			ExecutionContext::local_test().join_spill_dir,
		)
		.await
		.expect("materialized left outer");

		assert_eq!(
			record_batch_row_count(&streamed),
			record_batch_row_count(&materialized)
		);
		assert_eq!(
			canonical_arrow_ipc_digest(&streamed, SortPolicy::FullRowLex).expect("streamed digest"),
			canonical_arrow_ipc_digest(&materialized, SortPolicy::FullRowLex)
				.expect("materialized digest"),
			"partitioned spilled left outer must preserve the same rows and null-fill values"
		);
	}

	#[tokio::test]
	async fn partitioned_spilled_left_outer_build_writes_preserved_batches_without_concat() {
		let schema = Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int64, true),
			Field::new("payload", DataType::Utf8, false),
		]));
		let batch_a = RecordBatch::try_new(
			schema.clone(),
			vec![
				Arc::new(Int64Array::from(vec![Some(1), None])),
				Arc::new(StringArray::from(vec!["a", "null-key"])),
			],
		)
		.expect("batch a");
		let batch_b = RecordBatch::try_new(
			schema.clone(),
			vec![
				Arc::new(Int64Array::from(vec![Some(2), Some(3)])),
				Arc::new(StringArray::from(vec!["b", "c"])),
			],
		)
		.expect("batch b");
		let key_plan = JoinKeyPlan::from_build_schema_and_probe_schema(
			&schema,
			&[ColumnOrdinal(0)],
			&schema,
			&[ColumnOrdinal(0)],
			false,
		)
		.expect("key plan");
		let budget = high_test_budget();
		let context = ExecutionContext::local_test();
		let mut spilled = SpilledInnerJoinBuild::create(
			budget,
			context.join_spill_file_ops,
			context.join_spill_dir,
		)
		.await
		.expect("create spill");

		spilled
			.write_preserved_batch_chunks(&batch_a, key_plan, &[ColumnOrdinal(0)], false, budget)
			.await
			.expect("write batch a");
		spilled
			.write_preserved_batch_chunks(&batch_b, key_plan, &[ColumnOrdinal(0)], false, budget)
			.await
			.expect("write batch b");

		assert_eq!(
			spilled.input_batches_written(),
			2,
			"preserved build input must be written batch-by-batch, not concat_or_empty'd first"
		);
		assert_eq!(spilled.input_row_count(), 4);
		assert_eq!(spilled.page_count(), 2);
		assert_eq!(spilled.page_row_count(0).expect("page 0 rows"), 2);
		assert_eq!(spilled.page_row_count(1).expect("page 1 rows"), 2);
		spilled.cleanup().await;
	}

	#[test]
	fn streamed_spill_partition_count_grows_to_keep_partition_resident_bounded() {
		let memory_cap = 64 * 1024 * 1024;
		let sf1_estimate = 121 * 1024 * 1024;
		let sf10_estimate = sf1_estimate * 10;
		let sf1_partitions =
			u64::from(streamed_spill_partition_count(sf1_estimate, memory_cap).unwrap());
		let sf10_partitions =
			u64::from(streamed_spill_partition_count(sf10_estimate, memory_cap).unwrap());
		let target = memory_cap / 2;

		assert!(sf10_partitions > sf1_partitions);
		assert!(sf1_estimate.div_ceil(sf1_partitions) <= target);
		assert!(sf10_estimate.div_ceil(sf10_partitions) <= target);
	}

	#[tokio::test]
	async fn projected_spilled_inner_build_writes_only_retained_columns() {
		let schema = Arc::new(Schema::new(vec![
			Field::new("pk", DataType::Int64, false),
			Field::new("id", DataType::Int64, false),
			Field::new("payload", DataType::Utf8, false),
		]));
		let batch = RecordBatch::try_new(
			schema.clone(),
			vec![
				Arc::new(Int64Array::from(vec![1, 2, 3])),
				Arc::new(Int64Array::from(vec![11, 12, 13])),
				Arc::new(StringArray::from(vec!["wide-a", "wide-b", "wide-c"])),
			],
		)
		.expect("projected spill input");
		let retained = vec![ColumnOrdinal(1)];
		let projected_schema = projected_schema_for_columns(&schema, &retained).expect("schema");
		let key_plan = JoinKeyPlan::from_build_schema_and_probe_schema(
			&projected_schema,
			&[ColumnOrdinal(0)],
			&schema,
			&[ColumnOrdinal(1)],
			false,
		)
		.expect("key plan");
		let budget = high_test_budget();
		let context = ExecutionContext::local_test();
		let mut spilled = SpilledInnerJoinBuild::create(
			budget,
			context.join_spill_file_ops,
			context.join_spill_dir,
		)
		.await
		.expect("create projected spill");

		spilled
			.write_projected_batch_chunks(
				&batch,
				&retained,
				key_plan,
				&[ColumnOrdinal(0)],
				false,
				budget,
			)
			.await
			.expect("write projected page");
		let page = spilled.read_batch_page(0).await.expect("read page");

		assert_eq!(page.num_columns(), 1);
		assert_eq!(page.schema().field(0).name(), "id");
		assert_eq!(spilled.input_batches_written(), 1);
		spilled.cleanup().await;
	}

	#[tokio::test]
	async fn projected_left_semi_grouped_aggregate_prunes_unused_preserved_columns() {
		let rows = CROSS_JOIN_OUTPUT_CHUNK_ROWS + 512;
		let low_budget = TaskBudget {
			memory: ByteCount::new(64 * 1024),
			spill_bytes: ByteCount::new(2 * 1024 * 1024),
			..TaskBudget::from_query_budget(QueryBudget::test_default())
		};
		let telemetry = Arc::new(Mutex::new(TaskTelemetry::default()));
		let (stream_executor, plan) = projected_left_semi_grouped_aggregate_fixture(
			rows,
			4096,
			low_budget,
			telemetry.clone(),
		);
		let streamed = stream_executor
			.execute_bound(&plan, None)
			.await
			.expect("projected left-semi grouped aggregate");

		let (baseline_executor, baseline_plan) = projected_left_semi_grouped_aggregate_fixture(
			rows,
			4096,
			high_test_budget(),
			Arc::new(Mutex::new(TaskTelemetry::default())),
		);
		let materialized = baseline_executor
			.execute_bound(&baseline_plan, None)
			.await
			.expect("materialized left-semi grouped aggregate");

		assert_eq!(
			canonical_arrow_ipc_digest(&streamed, SortPolicy::None).expect("streamed digest"),
			canonical_arrow_ipc_digest(&materialized, SortPolicy::None)
				.expect("materialized digest"),
			"projected spill must preserve grouped aggregate results"
		);
		let spill_bytes = telemetry.lock().unwrap().counters.spill_bytes;
		assert!(
			spill_bytes > 0,
			"path should spill the projected preserved side"
		);
		assert!(
			spill_bytes < low_budget.spill_bytes.get(),
			"projected spill should stay within the unchanged spill budget"
		);
	}

	#[tokio::test]
	async fn streaming_collapsing_aggregate_matches_materialized_float64_and_decimal_order() {
		let rows = CROSS_JOIN_OUTPUT_CHUNK_ROWS + 257;

		let (executor, plan, join, filter, aggr_expr, output_schema) =
			streaming_aggregate_fixture(StreamingValueKind::Float64, rows, 0, high_test_budget());
		let streamed = executor
			.execute_bound(&plan, None)
			.await
			.expect("streaming float aggregate");
		let materialized = materialized_join_filter_aggregate(
			&executor,
			&join,
			&filter,
			&aggr_expr,
			&output_schema,
		)
		.await
		.expect("materialized float aggregate");
		let streamed_float = streamed[0]
			.column(0)
			.as_any()
			.downcast_ref::<Float64Array>()
			.expect("float sum")
			.value(0);
		let materialized_float = materialized[0]
			.column(0)
			.as_any()
			.downcast_ref::<Float64Array>()
			.expect("materialized float sum")
			.value(0);
		assert_eq!(
			streamed_float.to_bits(),
			materialized_float.to_bits(),
			"chunking must preserve Float64 fold order exactly"
		);

		let (executor, plan, join, filter, aggr_expr, output_schema) =
			streaming_aggregate_fixture(StreamingValueKind::Decimal, rows, 0, high_test_budget());
		let streamed = executor
			.execute_bound(&plan, None)
			.await
			.expect("streaming decimal aggregate");
		let materialized = materialized_join_filter_aggregate(
			&executor,
			&join,
			&filter,
			&aggr_expr,
			&output_schema,
		)
		.await
		.expect("materialized decimal aggregate");
		let streamed_decimal = streamed[0]
			.column(0)
			.as_any()
			.downcast_ref::<Decimal128Array>()
			.expect("decimal sum");
		let materialized_decimal = materialized[0]
			.column(0)
			.as_any()
			.downcast_ref::<Decimal128Array>()
			.expect("materialized decimal sum");
		assert_eq!(streamed_decimal.value(0), materialized_decimal.value(0));
		assert_eq!(
			streamed_decimal.data_type(),
			materialized_decimal.data_type()
		);
	}

	#[tokio::test]
	async fn streaming_collapsing_aggregate_covers_spilled_join_route() {
		let rows = CROSS_JOIN_OUTPUT_CHUNK_ROWS + 1_024;
		let spill_budget = TaskBudget {
			memory: ByteCount::new(960 * 1024),
			spill_bytes: ByteCount::new(16 * 1024 * 1024),
			..TaskBudget::from_query_budget(QueryBudget::test_default())
		};
		let telemetry = Arc::new(Mutex::new(TaskTelemetry::default()));
		let context = ExecutionContext::local_test()
			.with_task_budget(spill_budget)
			.with_task_telemetry(telemetry.clone());
		let (executor, plan, _join, filter, aggr_expr, output_schema) =
			streaming_aggregate_fixture_with_context(StreamingValueKind::Float64, rows, 0, context);
		let streamed = executor
			.execute_bound(&plan, None)
			.await
			.expect("streaming aggregate should cover spilled hash join");

		let (baseline_executor, _, baseline_join, baseline_filter, baseline_aggr, baseline_schema) =
			streaming_aggregate_fixture(StreamingValueKind::Float64, rows, 0, high_test_budget());
		let materialized = materialized_join_filter_aggregate(
			&baseline_executor,
			&baseline_join,
			&baseline_filter,
			&baseline_aggr,
			&baseline_schema,
		)
		.await
		.expect("materialized baseline");
		let streamed_value = streamed[0]
			.column(0)
			.as_any()
			.downcast_ref::<Float64Array>()
			.expect("streamed sum")
			.value(0);
		let baseline_value = materialized[0]
			.column(0)
			.as_any()
			.downcast_ref::<Float64Array>()
			.expect("baseline sum")
			.value(0);

		assert_eq!(streamed_value.to_bits(), baseline_value.to_bits());
		assert!(telemetry.lock().unwrap().counters.spill_bytes > 0);
		assert_eq!(filter.roots.len(), 1);
		assert_eq!(aggr_expr.len(), 1);
		assert_eq!(output_schema.fields().len(), 1);
	}

	#[tokio::test]
	async fn streaming_join_filter_grouped_aggregate_uses_valid_group_schema() {
		let rows = CROSS_JOIN_OUTPUT_CHUNK_ROWS + 12_000;
		let low_budget = TaskBudget {
			memory: ByteCount::new(1024 * 1024),
			spill_bytes: ByteCount::new(64 * 1024 * 1024),
			..TaskBudget::from_query_budget(QueryBudget::test_default())
		};
		let (_, aggregate, ..) =
			streaming_aggregate_fixture(StreamingValueKind::Float64, rows, 1024, low_budget);
		let ExecutionPlan::Aggregate {
			input,
			aggr_expr,
			output_schema,
			..
		} = aggregate
		else {
			panic!("aggregate fixture");
		};
		let grouped = ExecutionPlan::Aggregate {
			input,
			group_expr: bound_column_root("keep", ColumnOrdinal(4), DataType::Int64, false),
			aggr_expr,
			output_schema: Arc::new(Schema::new(vec![
				Field::new("keep", DataType::Int64, false),
				output_schema.field(0).as_ref().clone(),
			])),
		};
		let (executor, _, ..) =
			streaming_aggregate_fixture(StreamingValueKind::Float64, rows, 1024, low_budget);

		let results = executor
			.execute_bound(&grouped, None)
			.await
			.expect("grouped aggregate should execute with a valid grouped schema");

		assert_eq!(results[0].num_rows(), 1);
		assert_eq!(results[0].num_columns(), 2);
	}

	#[tokio::test]
	async fn streaming_join0_filter_matches_materialized_float64_and_decimal_order() {
		let rows = CROSS_JOIN_OUTPUT_CHUNK_ROWS + 257;

		let (executor, plan, join, filter) = streaming_filter_fixture(
			StreamingValueKind::Float64,
			rows,
			0,
			high_test_budget(),
			3,
			JoinReplayId(0),
		);
		let streamed = executor
			.execute_bound(&plan, None)
			.await
			.expect("streaming float filter");
		let materialized = materialized_join_filter(&executor, &join, &filter)
			.await
			.expect("materialized float filter");
		assert_record_batches_equal(&streamed, &materialized);

		let (executor, plan, join, filter) = streaming_filter_fixture(
			StreamingValueKind::Decimal,
			rows,
			0,
			high_test_budget(),
			3,
			JoinReplayId(0),
		);
		let streamed = executor
			.execute_bound(&plan, None)
			.await
			.expect("streaming decimal filter");
		let materialized = materialized_join_filter(&executor, &join, &filter)
			.await
			.expect("materialized decimal filter");
		assert_record_batches_equal(&streamed, &materialized);
	}

	#[tokio::test]
	async fn streaming_join0_filter_replaces_raw_join_materialization() {
		let rows = CROSS_JOIN_OUTPUT_CHUNK_ROWS + 2_000;
		let budget = TaskBudget {
			memory: ByteCount::new(4 * 1024 * 1024),
			spill_bytes: ByteCount::new(128 * 1024 * 1024),
			..TaskBudget::from_query_budget(QueryBudget::test_default())
		};
		let (executor, plan, join, _filter) = streaming_filter_fixture(
			StreamingValueKind::Float64,
			rows,
			4096,
			budget,
			64,
			JoinReplayId(0),
		);

		let direct_join_err = executor
			.execute_bound(&join, None)
			.await
			.expect_err("raw join materialization should exceed the output cap");
		assert!(matches!(
			direct_join_err,
			ExecutionError::BudgetExhausted {
				dimension: BudgetDimension::Memory,
				producer: "join output materialization",
				..
			}
		));

		let streamed = executor
			.execute_bound(&plan, None)
			.await
			.expect("filter streaming should avoid the raw join output wall");
		assert!(record_batch_row_count(&streamed) > 0);
	}

	#[tokio::test]
	async fn streaming_join0_filter_accumulator_is_budgeted() {
		let rows = CROSS_JOIN_OUTPUT_CHUNK_ROWS * 2 + 1_000;
		let budget = TaskBudget {
			memory: ByteCount::new(4 * 1024 * 1024),
			spill_bytes: ByteCount::new(128 * 1024 * 1024),
			..TaskBudget::from_query_budget(QueryBudget::test_default())
		};
		let (executor, plan, _join, _filter) = streaming_filter_fixture(
			StreamingValueKind::Float64,
			rows,
			4096,
			budget,
			1,
			JoinReplayId(0),
		);

		let err = executor
			.execute_bound(&plan, None)
			.await
			.expect_err("filtered accumulator must fail cleanly when it remains too large");

		assert!(matches!(
			err,
			ExecutionError::BudgetExhausted {
				dimension: BudgetDimension::Memory,
				producer: "streamed join filter accumulator",
				..
			}
		));
	}

	#[tokio::test]
	async fn streaming_join0_filter_shape_fence_keeps_other_join_ids_materialized() {
		let rows = CROSS_JOIN_OUTPUT_CHUNK_ROWS + 2_000;
		let budget = TaskBudget {
			memory: ByteCount::new(2 * 1024 * 1024),
			spill_bytes: ByteCount::new(128 * 1024 * 1024),
			..TaskBudget::from_query_budget(QueryBudget::test_default())
		};
		let (executor, plan, _join, _filter) = streaming_filter_fixture(
			StreamingValueKind::Float64,
			rows,
			4096,
			budget,
			64,
			JoinReplayId::SYNTHETIC,
		);

		let err = executor
			.execute_bound(&plan, None)
			.await
			.expect_err("non-Join(0) filter shape stays on the materialized path");

		assert!(matches!(
			err,
			ExecutionError::BudgetExhausted {
				dimension: BudgetDimension::Memory,
				producer: "join output materialization",
				..
			}
		));
	}

	#[tokio::test]
	async fn streaming_exchange_join0_filter_matches_materialized_float64_and_decimal_order() {
		let rows = CROSS_JOIN_OUTPUT_CHUNK_ROWS + 257;

		let (executor, filter_plan, join, filter) = streaming_filter_fixture(
			StreamingValueKind::Float64,
			rows,
			0,
			high_test_budget(),
			3,
			JoinReplayId(0),
		);
		let key = bound_column_root("pk", ColumnOrdinal(0), DataType::Int64, false);
		let exchange = exchange_plan(filter_plan, key.clone(), 4);
		let streamed = executor
			.execute_bound(&exchange, None)
			.await
			.expect("streamed float exchange");
		let materialized = materialized_join_filter_exchange(&executor, &join, &filter, &key, 4)
			.await
			.expect("materialized float exchange");
		assert_record_batches_equal(&streamed, &materialized);

		let (executor, filter_plan, join, filter) = streaming_filter_fixture(
			StreamingValueKind::Decimal,
			rows,
			0,
			high_test_budget(),
			3,
			JoinReplayId(0),
		);
		let key = bound_column_root("pk", ColumnOrdinal(0), DataType::Int64, false);
		let exchange = exchange_plan(filter_plan, key.clone(), 4);
		let streamed = executor
			.execute_bound(&exchange, None)
			.await
			.expect("streamed decimal exchange");
		let materialized = materialized_join_filter_exchange(&executor, &join, &filter, &key, 4)
			.await
			.expect("materialized decimal exchange");
		assert_record_batches_equal(&streamed, &materialized);
	}

	#[tokio::test]
	async fn streaming_exchange_join0_filter_replaces_filter_accumulator_with_exchange_budget() {
		let rows = CROSS_JOIN_OUTPUT_CHUNK_ROWS * 4 + 1_000;
		let budget = TaskBudget {
			memory: ByteCount::new(4 * 1024 * 1024),
			spill_bytes: ByteCount::new(128 * 1024 * 1024),
			..TaskBudget::from_query_budget(QueryBudget::test_default())
		};
		let (executor, filter_plan, _join, _filter) = streaming_filter_fixture(
			StreamingValueKind::Float64,
			rows,
			2048,
			budget,
			1,
			JoinReplayId(0),
		);

		let filter_err = executor
			.execute_bound(&filter_plan, None)
			.await
			.expect_err("plain filter path should hit the ADR 340 accumulator");
		assert!(
			matches!(
				filter_err,
				ExecutionError::BudgetExhausted {
					dimension: BudgetDimension::Memory,
					producer: "streamed join filter accumulator",
					..
				}
			),
			"unexpected filter error: {filter_err:?}"
		);

		let key = bound_column_root("pk", ColumnOrdinal(0), DataType::Int64, false);
		let exchange = exchange_plan(filter_plan, key, 4);
		let err = executor
			.execute_bound(&exchange, None)
			.await
			.expect_err("exchange output accumulator must fail cleanly");
		assert!(
			matches!(
				err,
				ExecutionError::BudgetExhausted {
					dimension: BudgetDimension::Memory,
					producer: "streamed exchange output accumulator",
					..
				}
			),
			"unexpected exchange error: {err:?}"
		);
	}

	#[tokio::test]
	async fn streaming_exchange_join0_filter_shape_fence_keeps_other_join_ids_materialized() {
		let rows = CROSS_JOIN_OUTPUT_CHUNK_ROWS * 4 + 1_000;
		let budget = TaskBudget {
			memory: ByteCount::new(4 * 1024 * 1024),
			spill_bytes: ByteCount::new(128 * 1024 * 1024),
			..TaskBudget::from_query_budget(QueryBudget::test_default())
		};
		let (executor, filter_plan, _join, _filter) = streaming_filter_fixture(
			StreamingValueKind::Float64,
			rows,
			2048,
			budget,
			1,
			JoinReplayId::SYNTHETIC,
		);
		let key = bound_column_root("pk", ColumnOrdinal(0), DataType::Int64, false);
		let exchange = exchange_plan(filter_plan, key, 4);

		let err = executor
			.execute_bound(&exchange, None)
			.await
			.expect_err("non-Join(0) exchange shape stays on the materialized path");

		assert!(
			matches!(
				err,
				ExecutionError::BudgetExhausted {
					dimension: BudgetDimension::Memory,
					producer: "join output materialization",
					..
				}
			),
			"unexpected shape-fence error: {err:?}"
		);
	}

	#[tokio::test]
	async fn reoriented_inner_chain_grouped_aggregate_matches_materialized_float64_and_decimal_order()
	 {
		let rows = CROSS_JOIN_OUTPUT_CHUNK_ROWS + 257;

		let (stream_executor, stream_plan) = reoriented_inner_chain_fixture(
			StreamingValueKind::Float64,
			rows,
			0,
			high_test_budget(),
			true,
			false,
		);
		let (materialized_executor, materialized_plan) = reoriented_inner_chain_fixture(
			StreamingValueKind::Float64,
			rows,
			0,
			high_test_budget(),
			false,
			false,
		);
		let streamed = stream_executor
			.execute_bound(&stream_plan, None)
			.await
			.expect("streamed inner-chain float chain");
		let materialized = materialized_executor
			.execute_bound(&materialized_plan, None)
			.await
			.expect("materialized inner-chain float chain");
		assert_record_batches_equal(&streamed, &materialized);
		let (stream_executor, stream_plan) = reoriented_inner_chain_fixture(
			StreamingValueKind::Decimal,
			rows,
			0,
			high_test_budget(),
			true,
			false,
		);
		let (materialized_executor, materialized_plan) = reoriented_inner_chain_fixture(
			StreamingValueKind::Decimal,
			rows,
			0,
			high_test_budget(),
			false,
			false,
		);
		let streamed = stream_executor
			.execute_bound(&stream_plan, None)
			.await
			.expect("streamed inner-chain decimal chain");
		let materialized = materialized_executor
			.execute_bound(&materialized_plan, None)
			.await
			.expect("materialized inner-chain decimal chain");
		assert_record_batches_equal(&streamed, &materialized);
	}
	#[tokio::test]
	async fn reoriented_inner_chain_removes_intermediate_build_accumulator() {
		let rows = 6_000;
		let stream_budget = TaskBudget {
			memory: ByteCount::new(64 * 1024),
			spill_bytes: ByteCount::new(64 * 1024 * 1024),
			..TaskBudget::from_query_budget(QueryBudget::test_default())
		};
		let (stream_executor, stream_plan) = reoriented_inner_chain_fixture(
			StreamingValueKind::Float64,
			rows,
			0,
			stream_budget,
			true,
			true,
		);
		let streamed = stream_executor
			.execute_bound(&stream_plan, None)
			.await
			.expect("streaming chain should avoid materialized intermediate and build-scan walls");
		assert!(record_batch_row_count(&streamed) > 0);
	}
	#[tokio::test]
	async fn reoriented_inner_chain_shape_fence_keeps_other_replay_ids_materialized() {
		let rows = CROSS_JOIN_OUTPUT_CHUNK_ROWS * 4 + 2_000;
		let budget = TaskBudget {
			memory: ByteCount::new(512 * 1024),
			spill_bytes: ByteCount::new(256 * 1024 * 1024),
			..TaskBudget::from_query_budget(QueryBudget::test_default())
		};
		let (executor, plan) = reoriented_inner_chain_fixture(
			StreamingValueKind::Decimal,
			rows,
			256,
			budget,
			false,
			false,
		);

		let err = executor
			.execute_bound(&plan, None)
			.await
			.expect_err("non-matching replay ids stay on the materialized path");
		assert!(
			matches!(
				err,
				ExecutionError::BudgetExhausted {
					dimension: BudgetDimension::Memory,
					producer: "join output materialization",
					..
				}
			),
			"unexpected shape-fence error: {err:?}"
		);
	}
	#[tokio::test]
	async fn correlated_scalar_left_outer_stream_matches_materialized_avg_recombine() {
		let rows = CROSS_JOIN_OUTPUT_CHUNK_ROWS + 257;
		let (stream_executor, stream_plan) =
			correlated_scalar_left_outer_fixture(rows, 0, high_test_budget(), true, 4, true);
		let (materialized_executor, materialized_plan) =
			correlated_scalar_left_outer_fixture(rows, 0, high_test_budget(), false, 4, true);
		let streamed = stream_executor
			.execute_bound(&stream_plan, None)
			.await
			.expect("streamed correlated scalar recombine");
		let materialized = materialized_executor
			.execute_bound(&materialized_plan, None)
			.await
			.expect("materialized correlated scalar recombine");
		assert_record_batches_equal(&streamed, &materialized);
	}
	#[tokio::test]
	async fn correlated_scalar_bare_join0_stream_matches_materialized_recombine() {
		let rows = CROSS_JOIN_OUTPUT_CHUNK_ROWS + 257;
		let (stream_executor, stream_plan) =
			correlated_scalar_left_outer_fixture(rows, 0, high_test_budget(), true, 4, false);
		let (materialized_executor, materialized_plan) =
			correlated_scalar_left_outer_fixture(rows, 0, high_test_budget(), false, 4, false);
		let ExecutionPlan::Aggregate {
			input,
			aggr_expr,
			output_schema,
			..
		} = &stream_plan
		else {
			panic!("correlated scalar fixture must be an aggregate");
		};
		let streamed = stream_executor
			.try_streaming_correlated_scalar_left_outer_global_aggregate(
				input,
				aggr_expr,
				output_schema,
				None,
			)
			.await
			.expect("streamed bare Join(0) correlated scalar recombine")
			.expect("bare Join(0) alias must be eligible for correlated streaming");
		let materialized = materialized_executor
			.execute_bound(&materialized_plan, None)
			.await
			.expect("materialized bare Join(0) correlated scalar recombine");
		assert_record_batches_equal(&streamed, &materialized);
	}
	#[tokio::test]
	async fn correlated_scalar_left_outer_stream_removes_join0_materialization_wall() {
		let rows = CROSS_JOIN_OUTPUT_CHUNK_ROWS * 2;
		let budget = TaskBudget {
			memory: ByteCount::new(2 * 1024 * 1024),
			spill_bytes: ByteCount::new(256 * 1024 * 1024),
			..TaskBudget::from_query_budget(QueryBudget::test_default())
		};
		let (materialized_executor, materialized_plan) =
			correlated_scalar_left_outer_fixture(rows, 2048, budget, false, 1, true);
		let materialized_err = materialized_executor
			.execute_bound(&materialized_plan, None)
			.await
			.expect_err("disabled streaming shape keeps the raw Join(0) output wall");
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
			correlated_scalar_left_outer_fixture(rows, 2048, budget, true, 1, true);
		let streamed = stream_executor
			.execute_bound(&stream_plan, None)
			.await
			.expect("streamed correlated scalar recombine should avoid raw Join(0) output");
		assert_eq!(record_batch_row_count(&streamed), 1);
	}
	#[tokio::test]
	async fn streaming_semi_anti_probe_applies_residual_and_preserves_left_rows() {
		let preserved_schema = Arc::new(Schema::new(vec![
			Field::new("orderkey", DataType::Int64, false),
			Field::new("suppkey", DataType::Int64, false),
			Field::new("tag", DataType::Int64, false),
		]));
		let existence_schema = Arc::new(Schema::new(vec![
			Field::new("orderkey", DataType::Int64, false),
			Field::new("suppkey", DataType::Int64, false),
			Field::new("late", DataType::Int64, false),
		]));
		let preserved_batch = RecordBatch::try_new(
			preserved_schema.clone(),
			vec![
				Arc::new(Int64Array::from(vec![1, 2, 3])),
				Arc::new(Int64Array::from(vec![10, 20, 30])),
				Arc::new(Int64Array::from(vec![100, 200, 300])),
			],
		)
		.expect("preserved batch");
		let existence_batch = RecordBatch::try_new(
			existence_schema,
			vec![
				Arc::new(Int64Array::from(vec![1, 2, 4])),
				Arc::new(Int64Array::from(vec![20, 20, 40])),
				Arc::new(Int64Array::from(vec![1, 1, 1])),
			],
		)
		.expect("existence batch");
		let residual = suppkey_neq_residual_program(ColumnOrdinal(1), ColumnOrdinal(4));
		let mut semi_runtime = semi_anti_runtime(
			JoinKind::LeftSemi,
			preserved_schema.clone(),
			existence_batch.clone(),
			&residual,
		);
		let executor = Executor::new(Arc::new(InMemoryStorageScan {
			table_data: RecordBatch::new_empty(preserved_schema.clone()),
		}));

		let semi = executor
			.probe_streaming_semi_anti(&mut semi_runtime, &preserved_batch)
			.await
			.expect("semi probe");
		assert_eq!(record_batch_row_count(&semi), 1);
		let semi_tags = semi[0]
			.column(2)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("semi tags");
		assert_eq!(semi_tags.values(), &[100]);

		let mut anti_runtime = semi_anti_runtime(
			JoinKind::LeftAnti,
			preserved_schema,
			existence_batch,
			&residual,
		);
		let anti = executor
			.probe_streaming_semi_anti(&mut anti_runtime, &preserved_batch)
			.await
			.expect("anti probe");
		assert_eq!(record_batch_row_count(&anti), 2);
		let anti_tags = anti[0]
			.column(2)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("anti tags");
		assert_eq!(
			anti_tags.values(),
			&[200, 300],
			"same-supplier existence rows must not satisfy the non-equi residual"
		);
	}

	#[tokio::test]
	async fn streaming_left_anti_probe_applies_late_residual_conjunct() {
		let preserved_schema = Arc::new(Schema::new(vec![
			Field::new("orderkey", DataType::Int64, false),
			Field::new("suppkey", DataType::Int64, false),
			Field::new("tag", DataType::Int64, false),
		]));
		let existence_schema = Arc::new(Schema::new(vec![
			Field::new("orderkey", DataType::Int64, false),
			Field::new("suppkey", DataType::Int64, false),
			Field::new("late", DataType::Int64, false),
		]));
		let preserved_batch = RecordBatch::try_new(
			preserved_schema.clone(),
			vec![
				Arc::new(Int64Array::from(vec![1, 2])),
				Arc::new(Int64Array::from(vec![10, 20])),
				Arc::new(Int64Array::from(vec![100, 200])),
			],
		)
		.expect("preserved batch");
		let existence_batch = RecordBatch::try_new(
			existence_schema,
			vec![
				Arc::new(Int64Array::from(vec![1, 2])),
				Arc::new(Int64Array::from(vec![30, 40])),
				Arc::new(Int64Array::from(vec![1, 0])),
			],
		)
		.expect("existence batch");
		let residual = suppkey_neq_and_late_residual_program(
			ColumnOrdinal(1),
			ColumnOrdinal(4),
			ColumnOrdinal(5),
		);
		let mut anti_runtime = semi_anti_runtime(
			JoinKind::LeftAnti,
			preserved_schema.clone(),
			existence_batch,
			&residual,
		);
		let executor = Executor::new(Arc::new(InMemoryStorageScan {
			table_data: RecordBatch::new_empty(preserved_schema),
		}));

		let anti = executor
			.probe_streaming_semi_anti(&mut anti_runtime, &preserved_batch)
			.await
			.expect("anti probe");
		assert_eq!(record_batch_row_count(&anti), 1);
		let tags = anti[0]
			.column(2)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("anti tags");
		assert_eq!(
			tags.values(),
			&[200],
			"on-time existence rows must not satisfy the anti residual"
		);
	}

	#[tokio::test]
	async fn streaming_semi_anti_existence_set_spill_resident_state_is_budgeted() {
		let preserved_schema = Arc::new(Schema::new(vec![
			Field::new("orderkey", DataType::Int64, false),
			Field::new("suppkey", DataType::Int64, false),
		]));
		let existence_schema = Arc::new(Schema::new(vec![
			Field::new("orderkey", DataType::Int64, false),
			Field::new("suppkey", DataType::Int64, false),
			Field::new("blob", DataType::Utf8, false),
		]));
		let existence_batch = RecordBatch::try_new(
			existence_schema.clone(),
			vec![
				Arc::new(Int64Array::from(vec![1])),
				Arc::new(Int64Array::from(vec![2])),
				Arc::new(StringArray::from(vec![Some("x".repeat(64 * 1024))])),
			],
		)
		.expect("existence batch");
		let budget = TaskBudget {
			memory: ByteCount::new(1024),
			spill_bytes: ByteCount::new(1024 * 1024),
			..TaskBudget::from_query_budget(QueryBudget::test_default())
		};
		let executor = Executor::with_context(
			Arc::new(TableStorageScan {
				batches: std::collections::HashMap::from([(TableId(7), existence_batch)]),
			}),
			ExecutionContext::local_test().with_task_budget(budget),
		);
		let residual = suppkey_neq_residual_program(ColumnOrdinal(1), ColumnOrdinal(3));
		let existence_plan =
			test_scan_plan_with_first_field_key(TableId(7), existence_schema.clone(), 7);
		let stage = StreamingSemiAntiStage {
			replay_id: JoinReplayId(3),
			kind: JoinKind::LeftSemi,
			preserved_keys: vec![ColumnOrdinal(0)],
			existence: &existence_plan,
			existence_keys: vec![ColumnOrdinal(0)],
			match_filter: Some(&residual),
			output_schema: preserved_schema,
			strategy: JoinStrategy::HashPartitioned,
		};

		let err = match executor
			.prepare_streaming_semi_anti_runtime(stage, None)
			.await
		{
			Ok(_) => panic!("oversized existence set must fail cleanly"),
			Err(err) => err,
		};

		assert!(
			matches!(
				err,
				ExecutionError::BudgetExhausted {
					dimension: BudgetDimension::Memory,
					producer: "semi/anti existence-set spill resident state",
					..
				}
			),
			"unexpected existence-set error: {err:?}"
		);
	}

	#[tokio::test]
	async fn streaming_semi_anti_existence_build_streams_scan_batches_without_concat() {
		let preserved_schema = Arc::new(Schema::new(vec![
			Field::new("orderkey", DataType::Int64, false),
			Field::new("suppkey", DataType::Int64, false),
		]));
		let existence_schema = Arc::new(Schema::new(vec![
			Field::new("orderkey", DataType::Int64, false),
			Field::new("suppkey", DataType::Int64, false),
			Field::new("late", DataType::Int64, false),
		]));
		let existence_a = RecordBatch::try_new(
			existence_schema.clone(),
			vec![
				Arc::new(Int64Array::from(vec![1, 2])),
				Arc::new(Int64Array::from(vec![10, 20])),
				Arc::new(Int64Array::from(vec![1, 1])),
			],
		)
		.expect("existence batch a");
		let existence_b = RecordBatch::try_new(
			existence_schema.clone(),
			vec![
				Arc::new(Int64Array::from(vec![3, 4])),
				Arc::new(Int64Array::from(vec![30, 40])),
				Arc::new(Int64Array::from(vec![1, 1])),
			],
		)
		.expect("existence batch b");
		let executor = Executor::with_context(
			Arc::new(MultiBatchTableStorageScan {
				batches: std::collections::HashMap::from([(
					TableId(7),
					vec![existence_a, existence_b],
				)]),
			}),
			ExecutionContext::local_test().with_task_budget(high_test_budget()),
		);
		let residual = suppkey_neq_residual_program(ColumnOrdinal(1), ColumnOrdinal(3));
		let existence_plan =
			test_scan_plan_with_first_field_key(TableId(7), existence_schema.clone(), 7);
		let stage = StreamingSemiAntiStage {
			replay_id: JoinReplayId(3),
			kind: JoinKind::LeftSemi,
			preserved_keys: vec![ColumnOrdinal(0)],
			existence: &existence_plan,
			existence_keys: vec![ColumnOrdinal(0)],
			match_filter: Some(&residual),
			output_schema: preserved_schema,
			strategy: JoinStrategy::HashPartitioned,
		};

		let runtime = executor
			.prepare_streaming_semi_anti_runtime(stage, None)
			.await
			.expect("prepare existence runtime")
			.expect("streamable existence plan");
		match runtime.existence {
			StreamingSemiAntiExistence::Spilled(spilled) => {
				assert_eq!(
					spilled.input_batches_written(),
					2,
					"existence scan batches must be written separately, not concat_or_empty'd first"
				);
				assert_eq!(spilled.input_row_count(), 4);
				assert_eq!(spilled.row_count(), 4);
				spilled.cleanup().await;
			}
			StreamingSemiAntiExistence::InMemory { .. }
			| StreamingSemiAntiExistence::RecognizedNeqSummary { .. } => {
				panic!("generic semi/anti preparer must use the streamed-spilled build")
			}
		}
	}

	#[test]
	fn streaming_inner_prefix_semi_anti_shape_fence_rejects_unmatched_topology() {
		let schema = Arc::new(Schema::new(vec![Field::new("pk", DataType::Int64, false)]));
		let join = ExecutionPlan::Join {
			replay_id: JoinReplayId(4),
			replay_eligibility: BroadcastReplayEligibility::Terminal,
			left: Box::new(test_scan_plan(TableId(1), schema.clone(), 1)),
			right: Box::new(test_scan_plan(TableId(2), schema.clone(), 2)),
			kind: JoinKind::LeftAnti,
			left_keys: vec![ColumnOrdinal(0)],
			right_keys: vec![ColumnOrdinal(0)],
			match_filter: Some(bound_column_root(
				"id",
				ColumnOrdinal(0),
				DataType::Int64,
				false,
			)),
			null_safe_keys: false,
			strategy: JoinStrategy::HashPartitioned,
			output_schema: schema,
		};

		assert!(
			extract_streaming_inner_prefix_semi_anti_chain(&join).is_none(),
			"semi/anti streaming must not engage without the full guarded topology"
		);
	}

	#[test]
	fn spill_retained_footprint_budget_guard_fires_before_write() {
		let (left_batch, right_batch) = spill_retained_guard_fixture();
		let mut build_rows = std::collections::HashMap::new();
		build_rows.insert(vec![1, 2, 3, 4], vec![0, 1, 2, 3]);

		let err = guard_spill_retained_entry_memory(&left_batch, &right_batch, &build_rows, 1)
			.expect_err("over-budget spill retained footprint must fail before write");

		assert!(matches!(
			err,
			ExecutionError::BudgetExhausted {
				dimension: BudgetDimension::Memory,
				..
			}
		));
	}

	#[test]
	fn spill_retained_footprint_budget_guard_allows_fitting_entry() {
		let (left_batch, right_batch) = spill_retained_guard_fixture();
		let mut build_rows = std::collections::HashMap::new();
		build_rows.insert(vec![1, 2, 3, 4], vec![0, 1]);

		guard_spill_retained_entry_memory(&left_batch, &right_batch, &build_rows, 1024 * 1024)
			.expect("fitting spill retained footprint");
	}

	#[test]
	fn spill_retained_footprint_counts_build_map_term() {
		let (left_batch, right_batch) = spill_retained_guard_fixture();
		let mut build_rows = std::collections::HashMap::with_capacity(2_048);
		for key in 0..64u32 {
			build_rows.insert(key.to_le_bytes().to_vec(), vec![0; 16]);
		}
		let retained_cap = join_spill_retained_footprint_memory_cap(8 * 1024);
		let retained_inputs = record_batch_memory_bytes(&left_batch)
			.saturating_add(record_batch_memory_bytes(&right_batch));

		assert!(retained_inputs <= retained_cap);
		assert!(
			estimate_spill_retained_entry_bytes(&left_batch, &right_batch, &build_rows)
				> retained_cap
		);
		let err =
			guard_spill_retained_entry_memory(&left_batch, &right_batch, &build_rows, 8 * 1024)
				.expect_err("build map term must count against retained spill footprint");

		assert!(matches!(
			err,
			ExecutionError::BudgetExhausted {
				dimension: BudgetDimension::Memory,
				..
			}
		));
	}

	#[test]
	fn spill_retained_footprint_counts_both_input_batches() {
		let left_schema = Arc::new(Schema::new(vec![Field::new(
			"left_id",
			DataType::Int64,
			false,
		)]));
		let right_schema = Arc::new(Schema::new(vec![Field::new(
			"right_blob",
			DataType::Utf8,
			true,
		)]));
		let left_batch =
			RecordBatch::try_new(left_schema, vec![Arc::new(Int64Array::from(vec![1]))])
				.expect("left batch");
		let right_batch = RecordBatch::try_new(
			right_schema,
			vec![Arc::new(StringArray::from(vec![Some(
				"x".repeat(80 * 1024),
			)]))],
		)
		.expect("right batch");
		let mut build_rows = std::collections::HashMap::new();
		build_rows.insert(vec![1], vec![0]);
		let retained_cap = join_spill_retained_footprint_memory_cap(4 * 1024);
		let left_plus_map = record_batch_memory_bytes(&left_batch)
			.saturating_add(estimate_spill_build_map_bytes(&build_rows));

		assert!(left_plus_map <= retained_cap);
		assert!(
			estimate_spill_retained_entry_bytes(&left_batch, &right_batch, &build_rows)
				> retained_cap
		);
		let err =
			guard_spill_retained_entry_memory(&left_batch, &right_batch, &build_rows, 4 * 1024)
				.expect_err(
					"right input batch is retained even when output would preserve only one side",
				);

		assert!(matches!(
			err,
			ExecutionError::BudgetExhausted {
				dimension: BudgetDimension::Memory,
				..
			}
		));
	}

	#[test]
	fn join_materialization_budget_guard_fires_before_take() {
		let (left_batch, right_batch, output_schema) = materialization_guard_fixture();
		let selection = vec![
			JoinOutputRow::Paired { left: 0, right: 0 },
			JoinOutputRow::Paired { left: 1, right: 1 },
		];

		let err = materialize_join_selection(
			JoinKind::Inner,
			&left_batch,
			&right_batch,
			&selection,
			&output_schema,
			1,
		)
		.expect_err("over-budget materialization must fail before take");

		assert!(matches!(
			err,
			ExecutionError::BudgetExhausted {
				dimension: BudgetDimension::Memory,
				..
			}
		));
	}

	#[test]
	fn join_materialization_budget_guard_allows_fitting_join() {
		let (left_batch, right_batch, output_schema) = materialization_guard_fixture();
		let selection = vec![
			JoinOutputRow::Paired { left: 0, right: 0 },
			JoinOutputRow::Paired { left: 1, right: 1 },
		];

		let batch = materialize_join_selection(
			JoinKind::Inner,
			&left_batch,
			&right_batch,
			&selection,
			&output_schema,
			1024 * 1024,
		)
		.expect("fitting materialization");

		assert_eq!(batch.num_rows(), 2);
		let right_values = batch
			.column(3)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("right value");
		assert_eq!(right_values.values(), &[10, 20]);
	}

	#[test]
	fn reoriented_inner_materialization_can_restore_build_first_schema() {
		let (build_batch, probe_batch, output_schema) = materialization_guard_fixture();
		let selection = vec![
			JoinOutputRow::Paired { left: 0, right: 0 },
			JoinOutputRow::Paired { left: 1, right: 1 },
		];

		let batch = materialize_reoriented_inner_selection(
			&probe_batch,
			&build_batch,
			&selection,
			&output_schema,
			1024 * 1024,
			false,
		)
		.expect("reoriented materialization");

		let left_ids = batch
			.column(0)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("left id");
		let right_values = batch
			.column(3)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("right value");
		assert_eq!(left_ids.values(), &[1, 2]);
		assert_eq!(right_values.values(), &[10, 20]);
	}

	#[test]
	fn join_materialization_budget_guard_counts_only_preserved_semi_side() {
		let left_schema = Arc::new(Schema::new(vec![Field::new(
			"left_id",
			DataType::Int64,
			false,
		)]));
		let right_schema = Arc::new(Schema::new(vec![Field::new(
			"right_blob",
			DataType::Utf8,
			true,
		)]));
		let left_batch = RecordBatch::try_new(
			left_schema.clone(),
			vec![Arc::new(Int64Array::from(vec![1, 2]))],
		)
		.expect("left batch");
		let right_batch = RecordBatch::try_new(
			right_schema,
			vec![Arc::new(StringArray::from(vec![
				Some("x".repeat(16 * 1024)),
				Some("y".repeat(16 * 1024)),
			]))],
		)
		.expect("right batch");
		let selection = vec![
			JoinOutputRow::LeftPreserved { left: 0 },
			JoinOutputRow::LeftPreserved { left: 1 },
		];

		let batch = materialize_join_selection(
			JoinKind::LeftSemi,
			&left_batch,
			&right_batch,
			&selection,
			&left_schema,
			1024,
		)
		.expect("semi join should not charge unmaterialized right side");

		assert_eq!(batch.num_rows(), 2);
		assert_eq!(batch.num_columns(), 1);
	}

	#[test]
	fn cross_join_materialization_chunks_use_budget_guard() {
		let (left_batch, right_batch, output_schema) = materialization_guard_fixture();

		let err = execute_cross_join_match_all(
			&left_batch,
			&right_batch,
			&output_schema,
			1,
			JoinReplayId::SYNTHETIC,
			JoinStrategy::HashPartitioned,
			&None,
			&JoinRuntimeObservationMode::Disabled,
		)
		.expect_err("cross materialization must use the shared guard");

		assert!(matches!(
			err,
			ExecutionError::BudgetExhausted {
				dimension: BudgetDimension::Memory,
				..
			}
		));
	}

	#[test]
	fn join_materialization_estimate_saturates_for_huge_selection() {
		let (left_batch, right_batch, _) = materialization_guard_fixture();

		let estimate = estimate_join_materialization_bytes(
			JoinKind::Inner,
			&left_batch,
			&right_batch,
			usize::MAX,
		);

		assert_eq!(estimate, u64::MAX);
	}

	#[tokio::test]
	async fn execute_bound_exchange_partitions_projected_input() {
		let storage = Arc::new(RecordingStorageScan {
			table_data: batch(),
			requests: Arc::new(Mutex::new(Vec::new())),
		});
		let executor = Executor::new(storage);
		let catalog = catalog();
		let logical = LogicalPlan::Exchange {
			input: Box::new(LogicalPlan::Scan {
				table: TableId(1),
				projection: None,
			}),
			partition_count: 2,
			key: vec![Expr::column("amount")],
		};

		let bound = bind(&logical, &catalog, &PlannerConfig::default())
			.await
			.expect("bind");
		let results = executor
			.execute_bound(&bound, None)
			.await
			.expect("execute bound exchange");
		let rows = results.iter().map(|batch| batch.num_rows()).sum::<usize>();

		assert_eq!(rows, 4);
	}

	#[test]
	fn exchange_partition_bucket_uses_stable_xxh3_64_encoding() {
		let key_arrays = vec![Arc::new(Int32Array::from(vec![Some(42)])) as ArrayRef];
		let mut encoded = vec![4];
		encoded.extend_from_slice(&42i32.to_le_bytes());

		assert_eq!(
			partition_bucket(&key_arrays, 0, 17).expect("partition bucket"),
			(XxHash3_64::oneshot(&encoded) % 17) as usize
		);
	}

	#[test]
	fn exchange_partition_bucket_uses_stable_xxh3_64_encoding_for_int64() {
		let key_arrays = vec![Arc::new(Int64Array::from(vec![Some(42)])) as ArrayRef];
		let mut encoded = vec![5];
		encoded.extend_from_slice(&42i64.to_le_bytes());

		assert_eq!(
			partition_bucket(&key_arrays, 0, 17).expect("partition bucket"),
			(XxHash3_64::oneshot(&encoded) % 17) as usize
		);
	}

	// ADR 275: a Decimal128 partition key encodes as tag 17 + the raw i128 (little-endian). Pins the stable
	// encoding so a future tag/serialization change can't silently re-bucket Decimal keys.
	#[test]
	fn exchange_partition_bucket_uses_stable_xxh3_64_encoding_for_decimal128() {
		let key_arrays = vec![Arc::new(
			Decimal128Array::from(vec![Some(12_345_i128)])
				.with_precision_and_scale(15, 2)
				.expect("decimal column"),
		) as ArrayRef];
		let mut encoded = vec![17];
		encoded.extend_from_slice(&12_345_i128.to_le_bytes());

		assert_eq!(
			partition_bucket(&key_arrays, 0, 17).expect("partition bucket"),
			(XxHash3_64::oneshot(&encoded) % 17) as usize
		);
	}

	#[test]
	fn join_key_type_tag_distinguishes_int32_and_int64() {
		let int32_key = JoinKey::Int32(1);
		let int64_key = JoinKey::Int64(1);

		assert_ne!(int32_key, int64_key);
		assert_ne!(hash_join_key(&int32_key), hash_join_key(&int64_key));
	}

	#[test]
	fn join_key_plan_uses_actual_child_output_array_type() {
		let left = vec![Arc::new(Int64Array::from(vec![1])) as ArrayRef];
		let right = vec![Arc::new(Int64Array::from(vec![1])) as ArrayRef];

		assert_eq!(
			JoinKeyPlan::from_key_arrays(&left, &right, false),
			JoinKeyPlan::Int64
		);
	}

	#[tokio::test]
	async fn execute_grouped_sum_orders_by_group_key() {
		let schema = Arc::new(Schema::new(vec![
			Field::new("region", DataType::Utf8, true),
			Field::new("amount", DataType::Int32, true),
		]));
		let batch = RecordBatch::try_new(
			schema,
			vec![
				Arc::new(StringArray::from(vec![
					Some("west"),
					Some("east"),
					Some("west"),
					Some("east"),
				])),
				Arc::new(Int32Array::from(vec![
					Some(150),
					Some(10),
					Some(100),
					Some(90),
				])),
			],
		)
		.expect("batch");
		let executor = Executor::new(Arc::new(InMemoryStorageScan { table_data: batch }));
		let plan = LogicalPlan::Aggregate {
			input: Box::new(LogicalPlan::Scan {
				table: TableId(1),
				projection: None,
			}),
			group_expr: vec![Expr::column("region")],
			aggr_expr: vec![(AggregateFunction::Sum, vec![Expr::column("amount")])],
		};

		let results = executor.execute(&plan, None).await.expect("execute");
		let groups = results[0]
			.column(0)
			.as_any()
			.downcast_ref::<StringArray>()
			.expect("group column");
		let sums = results[0]
			.column(1)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("sum column");

		assert_eq!(groups.value(0), "east");
		assert_eq!(sums.value(0), 100);
		assert_eq!(groups.value(1), "west");
		assert_eq!(sums.value(1), 250);
	}

	#[tokio::test]
	async fn execute_unbound_grouped_float_min_max_uses_input_output_type() {
		let schema = Arc::new(Schema::new(vec![
			Field::new("region", DataType::Utf8, true),
			Field::new("score", DataType::Float64, true),
		]));
		let batch = RecordBatch::try_new(
			schema,
			vec![
				Arc::new(StringArray::from(vec![
					Some("west"),
					Some("east"),
					Some("west"),
					Some("east"),
				])),
				Arc::new(Float64Array::from(vec![
					Some(2.5),
					Some(f64::NAN),
					Some(1.5),
					Some(3.5),
				])),
			],
		)
		.expect("batch");
		let executor = Executor::new(Arc::new(InMemoryStorageScan { table_data: batch }));
		let plan = LogicalPlan::Aggregate {
			input: Box::new(LogicalPlan::Scan {
				table: TableId(1),
				projection: None,
			}),
			group_expr: vec![Expr::column("region")],
			aggr_expr: vec![
				(AggregateFunction::Min, vec![Expr::column("score")]),
				(AggregateFunction::Max, vec![Expr::column("score")]),
			],
		};

		let results = executor.execute(&plan, None).await.expect("execute");
		assert_eq!(results[0].schema().field(1).data_type(), &DataType::Float64);
		assert_eq!(results[0].schema().field(2).data_type(), &DataType::Float64);
		let groups = results[0]
			.column(0)
			.as_any()
			.downcast_ref::<StringArray>()
			.expect("group column");
		let mins = results[0]
			.column(1)
			.as_any()
			.downcast_ref::<Float64Array>()
			.expect("min column");
		let maxes = results[0]
			.column(2)
			.as_any()
			.downcast_ref::<Float64Array>()
			.expect("max column");

		assert_eq!(groups.value(0), "east");
		assert!(mins.value(0).is_nan());
		assert!(maxes.value(0).is_nan());
		assert_eq!(groups.value(1), "west");
		assert_eq!(mins.value(1), 1.5);
		assert_eq!(maxes.value(1), 2.5);
	}

	fn hash_join_key(key: &JoinKey) -> u64 {
		let mut hasher = DefaultHasher::new();
		key.hash(&mut hasher);
		hasher.finish()
	}

	struct RecordingStorageScan {
		table_data: RecordBatch,
		requests: Arc<Mutex<Vec<ScanRequest>>>,
	}

	#[async_trait::async_trait]
	impl StorageScan for RecordingStorageScan {
		async fn scan(&self, request: ScanRequest) -> Result<ScanStream, StorageError> {
			self.requests.lock().unwrap().push(request.clone());
			let mut batch = self.table_data.clone();
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

	struct TableStorageScan {
		batches: std::collections::HashMap<TableId, RecordBatch>,
	}

	#[async_trait::async_trait]
	impl StorageScan for TableStorageScan {
		async fn scan(&self, request: ScanRequest) -> Result<ScanStream, StorageError> {
			let batch = self.batches.get(&request.table_id).cloned().ok_or(
				StorageError::TableNotFound {
					table: request.table_id,
				},
			)?;
			Ok(ScanStream::from_batches(
				batch.schema(),
				request.session_id,
				request.query_id,
				vec![batch],
			))
		}
	}

	struct MultiBatchTableStorageScan {
		batches: std::collections::HashMap<TableId, Vec<RecordBatch>>,
	}

	#[async_trait::async_trait]
	impl StorageScan for MultiBatchTableStorageScan {
		async fn scan(&self, request: ScanRequest) -> Result<ScanStream, StorageError> {
			let batches = self.batches.get(&request.table_id).cloned().ok_or(
				StorageError::TableNotFound {
					table: request.table_id,
				},
			)?;
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

	fn catalog() -> MockCatalog {
		let mut catalog = MockCatalog::new();
		catalog
			.register_descriptor(TableId(1), descriptor())
			.expect("register descriptor");
		catalog
	}

	fn descriptor() -> TableSchemaDescriptor {
		let schema = Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("amount", DataType::Int32, true),
			Field::new("region", DataType::Utf8, true),
		]);

		TableSchemaDescriptor::new(
			TableObjectId::new([1; 16]),
			1,
			"default",
			"sales",
			&schema,
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
			LocalityPolicy::default_table_spread(),
			SchemaEvolutionPolicy::AdditiveOnly,
		)
		.expect("descriptor")
	}

	fn descriptor_with_schema(schema: &Schema) -> TableSchemaDescriptor {
		TableSchemaDescriptor::new(
			TableObjectId::new([1; 16]),
			1,
			"default",
			"sales",
			schema,
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
			LocalityPolicy::default_table_spread(),
			SchemaEvolutionPolicy::AdditiveOnly,
		)
		.expect("descriptor")
	}

	fn test_scan_plan(table_id: TableId, schema: Arc<Schema>, object_byte: u8) -> ExecutionPlan {
		let descriptor = TableSchemaDescriptor::new(
			TableObjectId::new([object_byte; 16]),
			1,
			"default",
			"join_test",
			schema.as_ref(),
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("pk")),
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
			row_identity: crate::storage::RowIdentityProjection::None,
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

	fn test_scan_plan_with_first_field_key(
		table_id: TableId,
		schema: Arc<Schema>,
		object_byte: u8,
	) -> ExecutionPlan {
		let primary_key = schema.field(0).name().to_string();
		let descriptor = TableSchemaDescriptor::new(
			TableObjectId::new([object_byte; 16]),
			1,
			"default",
			"join_test",
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
			row_identity: crate::storage::RowIdentityProjection::None,
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

	fn duplicate_join_fixture(build_rows: usize, budget: TaskBudget) -> (Executor, ExecutionPlan) {
		let left_schema = Arc::new(Schema::new(vec![
			Field::new("pk", DataType::Int64, false),
			Field::new("id", DataType::Int64, false),
		]));
		let right_schema = Arc::new(Schema::new(vec![
			Field::new("pk", DataType::Int64, false),
			Field::new("id", DataType::Int64, false),
		]));
		let left_batch = RecordBatch::try_new(
			left_schema.clone(),
			vec![
				Arc::new(Int64Array::from(
					(0..i64::try_from(build_rows).expect("rows fit i64")).collect::<Vec<_>>(),
				)),
				Arc::new(Int64Array::from(vec![1; build_rows])),
			],
		)
		.expect("left batch");
		let right_batch = RecordBatch::try_new(
			right_schema.clone(),
			vec![
				Arc::new(Int64Array::from(vec![0])),
				Arc::new(Int64Array::from(vec![1])),
			],
		)
		.expect("right batch");
		let storage = Arc::new(TableStorageScan {
			batches: std::collections::HashMap::from([
				(TableId(1), left_batch),
				(TableId(2), right_batch),
			]),
		});
		let context = ExecutionContext::local_test().with_task_budget(budget);
		let executor = Executor::with_context(storage, context);
		let plan = ExecutionPlan::Join {
			replay_id: JoinReplayId::SYNTHETIC,
			replay_eligibility: BroadcastReplayEligibility::Terminal,
			left: Box::new(test_scan_plan(TableId(1), left_schema, 1)),
			right: Box::new(test_scan_plan(TableId(2), right_schema, 2)),
			kind: JoinKind::Inner,
			left_keys: vec![ColumnOrdinal(1)],
			right_keys: vec![ColumnOrdinal(1)],
			match_filter: None,
			null_safe_keys: false,
			strategy: JoinStrategy::HashPartitioned,
			output_schema: Arc::new(Schema::new(vec![
				Field::new("pk", DataType::Int64, false),
				Field::new("id", DataType::Int64, false),
				Field::new("pk", DataType::Int64, false),
				Field::new("id", DataType::Int64, false),
			])),
		};
		(executor, plan)
	}

	fn streamed_inner_join_fixture(rows: usize, budget: TaskBudget) -> (Executor, ExecutionPlan) {
		let left_schema = Arc::new(Schema::new(vec![
			Field::new("pk", DataType::Int64, false),
			Field::new("id", DataType::Int64, false),
			Field::new("left_label", DataType::Utf8, false),
		]));
		let right_schema = Arc::new(Schema::new(vec![
			Field::new("pk", DataType::Int64, false),
			Field::new("id", DataType::Int64, false),
			Field::new("right_value", DataType::Int64, false),
		]));
		let split = rows / 2;
		let left_batch = |start: usize, len: usize| {
			let keys = (start..start + len)
				.map(|value| i64::try_from(value).expect("row fits i64"))
				.collect::<Vec<_>>();
			RecordBatch::try_new(
				left_schema.clone(),
				vec![
					Arc::new(Int64Array::from(keys.clone())),
					Arc::new(Int64Array::from(keys.clone())),
					Arc::new(StringArray::from(
						(start..start + len)
							.map(|value| Some(format!("left-{value}")))
							.collect::<Vec<_>>(),
					)),
				],
			)
			.expect("left batch")
		};
		let right_keys = (0..rows)
			.map(|value| i64::try_from(value).expect("row fits i64"))
			.collect::<Vec<_>>();
		let right_batch = RecordBatch::try_new(
			right_schema.clone(),
			vec![
				Arc::new(Int64Array::from(right_keys.clone())),
				Arc::new(Int64Array::from(right_keys)),
				Arc::new(Int64Array::from(
					(0..rows)
						.map(|value| i64::try_from(value * 10).expect("row fits i64"))
						.collect::<Vec<_>>(),
				)),
			],
		)
		.expect("right batch");
		let storage = Arc::new(MultiBatchTableStorageScan {
			batches: std::collections::HashMap::from([
				(
					TableId(1),
					vec![left_batch(0, split), left_batch(split, rows - split)],
				),
				(TableId(2), vec![right_batch]),
			]),
		});
		let context = ExecutionContext::local_test().with_task_budget(budget);
		let executor = Executor::with_context(storage, context);
		let output_schema = Arc::new(Schema::new(vec![
			Field::new("pk", DataType::Int64, false),
			Field::new("id", DataType::Int64, false),
			Field::new("left_label", DataType::Utf8, false),
			Field::new("pk", DataType::Int64, false),
			Field::new("id", DataType::Int64, false),
			Field::new("right_value", DataType::Int64, false),
		]));
		let plan = ExecutionPlan::Join {
			replay_id: JoinReplayId::SYNTHETIC,
			replay_eligibility: BroadcastReplayEligibility::Terminal,
			left: Box::new(test_scan_plan(TableId(1), left_schema, 1)),
			right: Box::new(test_scan_plan(TableId(2), right_schema, 2)),
			kind: JoinKind::Inner,
			left_keys: vec![ColumnOrdinal(1)],
			right_keys: vec![ColumnOrdinal(1)],
			match_filter: None,
			null_safe_keys: false,
			strategy: JoinStrategy::HashPartitioned,
			output_schema,
		};
		(executor, plan)
	}

	fn streamed_left_semi_join_fixture(
		rows: usize,
		budget: TaskBudget,
	) -> (Executor, ExecutionPlan) {
		let left_schema = Arc::new(Schema::new(vec![
			Field::new("pk", DataType::Int64, false),
			Field::new("id", DataType::Int64, false),
			Field::new("left_label", DataType::Utf8, false),
		]));
		let right_schema = Arc::new(Schema::new(vec![
			Field::new("pk", DataType::Int64, false),
			Field::new("id", DataType::Int64, false),
		]));
		let split = rows / 2;
		let left_batch = |start: usize, len: usize| {
			let keys = (start..start + len)
				.map(|value| i64::try_from(value).expect("row fits i64"))
				.collect::<Vec<_>>();
			RecordBatch::try_new(
				left_schema.clone(),
				vec![
					Arc::new(Int64Array::from(keys.clone())),
					Arc::new(Int64Array::from(keys)),
					Arc::new(StringArray::from(
						(start..start + len)
							.map(|value| Some(format!("left-{value}")))
							.collect::<Vec<_>>(),
					)),
				],
			)
			.expect("left semi preserved batch")
		};
		let right_keys = (0..rows)
			.flat_map(|value| {
				let value = i64::try_from(value).expect("row fits i64");
				[value, value]
			})
			.collect::<Vec<_>>();
		let right_batch = RecordBatch::try_new(
			right_schema.clone(),
			vec![
				Arc::new(Int64Array::from(
					(0..right_keys.len())
						.map(|value| i64::try_from(value).expect("row fits i64"))
						.collect::<Vec<_>>(),
				)),
				Arc::new(Int64Array::from(right_keys)),
			],
		)
		.expect("left semi existence batch");
		let storage = Arc::new(MultiBatchTableStorageScan {
			batches: std::collections::HashMap::from([
				(
					TableId(1),
					vec![left_batch(0, split), left_batch(split, rows - split)],
				),
				(TableId(2), vec![right_batch]),
			]),
		});
		let context = ExecutionContext::local_test().with_task_budget(budget);
		let executor = Executor::with_context(storage, context);
		let plan = ExecutionPlan::Join {
			replay_id: JoinReplayId::SYNTHETIC,
			replay_eligibility: BroadcastReplayEligibility::Terminal,
			left: Box::new(test_scan_plan(TableId(1), left_schema.clone(), 1)),
			right: Box::new(test_scan_plan(TableId(2), right_schema, 2)),
			kind: JoinKind::LeftSemi,
			left_keys: vec![ColumnOrdinal(1)],
			right_keys: vec![ColumnOrdinal(1)],
			match_filter: None,
			null_safe_keys: false,
			strategy: JoinStrategy::HashPartitioned,
			output_schema: left_schema,
		};
		(executor, plan)
	}

	fn projected_left_semi_grouped_aggregate_fixture(
		rows: usize,
		payload_len: usize,
		budget: TaskBudget,
		telemetry: Arc<Mutex<TaskTelemetry>>,
	) -> (Executor, ExecutionPlan) {
		let left_schema = Arc::new(Schema::new(vec![
			Field::new("pk", DataType::Int64, false),
			Field::new("order_key", DataType::Int64, false),
			Field::new("quantity", DataType::Decimal128(20, 2), false),
			Field::new("payload", DataType::Utf8, false),
		]));
		let right_schema = Arc::new(Schema::new(vec![
			Field::new("pk", DataType::Int64, false),
			Field::new("order_key", DataType::Int64, false),
		]));
		let payload = "x".repeat(payload_len);
		let left_keys = (0..rows)
			.map(|value| i64::try_from(value).expect("row fits i64"))
			.collect::<Vec<_>>();
		let quantity = Decimal128Array::from(
			(0..rows)
				.map(|row| Some(((row % 11) as i128 + 1) * 100))
				.collect::<Vec<_>>(),
		)
		.with_precision_and_scale(20, 2)
		.expect("quantity decimal");
		let left_batch = RecordBatch::try_new(
			left_schema.clone(),
			vec![
				Arc::new(Int64Array::from(left_keys.clone())),
				Arc::new(Int64Array::from(left_keys.clone())),
				Arc::new(quantity),
				Arc::new(StringArray::from(vec![payload.as_str(); rows])),
			],
		)
		.expect("projected left-semi preserved batch");
		let right_keys = (0..rows)
			.flat_map(|value| {
				let value = i64::try_from(value).expect("row fits i64");
				[value, value]
			})
			.collect::<Vec<_>>();
		let right_batch = RecordBatch::try_new(
			right_schema.clone(),
			vec![
				Arc::new(Int64Array::from(
					(0..right_keys.len())
						.map(|value| i64::try_from(value).expect("row fits i64"))
						.collect::<Vec<_>>(),
				)),
				Arc::new(Int64Array::from(right_keys)),
			],
		)
		.expect("projected left-semi existence batch");
		let storage = Arc::new(TableStorageScan {
			batches: std::collections::HashMap::from([
				(TableId(1), left_batch),
				(TableId(2), right_batch),
			]),
		});
		let context = ExecutionContext::local_test()
			.with_task_budget(budget)
			.with_task_telemetry(telemetry);
		let executor = Executor::with_context(storage, context);
		let semi = ExecutionPlan::Join {
			replay_id: JoinReplayId::SYNTHETIC,
			replay_eligibility: BroadcastReplayEligibility::Terminal,
			left: Box::new(test_scan_plan(TableId(1), left_schema.clone(), 1)),
			right: Box::new(test_scan_plan(TableId(2), right_schema, 2)),
			kind: JoinKind::LeftSemi,
			left_keys: vec![ColumnOrdinal(1)],
			right_keys: vec![ColumnOrdinal(1)],
			match_filter: None,
			null_safe_keys: false,
			strategy: JoinStrategy::HashPartitioned,
			output_schema: left_schema,
		};
		let group_expr = bound_column_root("order_key", ColumnOrdinal(1), DataType::Int64, false);
		let aggr_expr = vec![BoundAggregateCall {
			function: AggregateFunction::Sum,
			args: bound_column_root(
				"quantity",
				ColumnOrdinal(2),
				DataType::Decimal128(20, 2),
				false,
			),
		}];
		let output_schema = Arc::new(Schema::new(vec![
			Field::new("order_key", DataType::Int64, false),
			Field::new("sum_quantity", DataType::Decimal128(20, 2), true),
		]));
		let plan = ExecutionPlan::Aggregate {
			input: Box::new(exchange_plan(semi, group_expr.clone(), 1)),
			group_expr,
			aggr_expr,
			output_schema,
		};
		(executor, plan)
	}

	#[derive(Clone, Copy)]
	enum StreamingValueKind {
		Float64,
		Decimal,
	}

	fn high_test_budget() -> TaskBudget {
		TaskBudget {
			memory: ByteCount::new(128 * 1024 * 1024),
			spill_bytes: ByteCount::new(256 * 1024 * 1024),
			..TaskBudget::from_query_budget(QueryBudget::test_default())
		}
	}

	fn streaming_aggregate_fixture(
		kind: StreamingValueKind,
		rows: usize,
		blob_len: usize,
		budget: TaskBudget,
	) -> (
		Executor,
		ExecutionPlan,
		ExecutionPlan,
		BoundExprProgram,
		Vec<BoundAggregateCall>,
		SchemaRef,
	) {
		streaming_aggregate_fixture_with_context(
			kind,
			rows,
			blob_len,
			ExecutionContext::local_test().with_task_budget(budget),
		)
	}

	fn streaming_aggregate_fixture_with_context(
		kind: StreamingValueKind,
		rows: usize,
		blob_len: usize,
		context: ExecutionContext,
	) -> (
		Executor,
		ExecutionPlan,
		ExecutionPlan,
		BoundExprProgram,
		Vec<BoundAggregateCall>,
		SchemaRef,
	) {
		let amount_type = match kind {
			StreamingValueKind::Float64 => DataType::Float64,
			StreamingValueKind::Decimal => DataType::Decimal128(20, 2),
		};
		let left_schema = Arc::new(Schema::new(vec![
			Field::new("pk", DataType::Int64, false),
			Field::new("amount", amount_type.clone(), true),
			Field::new("blob", DataType::Utf8, false),
		]));
		let right_schema = Arc::new(Schema::new(vec![
			Field::new("pk", DataType::Int64, false),
			Field::new("keep", DataType::Int64, false),
		]));
		let keys = (0..i64::try_from(rows).expect("rows fit i64")).collect::<Vec<_>>();
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
						.map(|row| Some(((row % 17) as i128 + 1) * 100))
						.collect::<Vec<_>>(),
				)
				.with_precision_and_scale(20, 2)
				.expect("decimal amount"),
			),
		};
		let blob = "x".repeat(blob_len);
		let left_batch = RecordBatch::try_new(
			left_schema.clone(),
			vec![
				Arc::new(Int64Array::from(keys.clone())),
				amount,
				Arc::new(StringArray::from(vec![blob.as_str(); rows])),
			],
		)
		.expect("left streaming batch");
		let right_batch = RecordBatch::try_new(
			right_schema.clone(),
			vec![
				Arc::new(Int64Array::from(keys)),
				Arc::new(Int64Array::from(vec![1; rows])),
			],
		)
		.expect("right streaming batch");
		let storage = Arc::new(TableStorageScan {
			batches: std::collections::HashMap::from([
				(TableId(1), left_batch),
				(TableId(2), right_batch),
			]),
		});
		let executor = Executor::with_context(storage, context);
		let join_output_schema = Arc::new(Schema::new(
			left_schema
				.fields()
				.iter()
				.chain(right_schema.fields().iter())
				.map(|field| field.as_ref().clone())
				.collect::<Vec<_>>(),
		));
		let join = ExecutionPlan::Join {
			replay_id: JoinReplayId::SYNTHETIC,
			replay_eligibility: BroadcastReplayEligibility::Terminal,
			left: Box::new(test_scan_plan(TableId(1), left_schema, 1)),
			right: Box::new(test_scan_plan(TableId(2), right_schema, 2)),
			kind: JoinKind::Inner,
			left_keys: vec![ColumnOrdinal(0)],
			right_keys: vec![ColumnOrdinal(0)],
			match_filter: None,
			null_safe_keys: false,
			strategy: JoinStrategy::HashPartitioned,
			output_schema: join_output_schema,
		};
		let filter = keep_filter_program(ColumnOrdinal(4));
		let aggregate_type = amount_type;
		let aggr_expr = vec![BoundAggregateCall {
			function: AggregateFunction::Sum,
			args: bound_column_root("amount", ColumnOrdinal(1), aggregate_type.clone(), true),
		}];
		let output_schema = Arc::new(Schema::new(vec![Field::new(
			"sum_amount",
			aggregate_type,
			true,
		)]));
		let plan = ExecutionPlan::Aggregate {
			input: Box::new(ExecutionPlan::Filter {
				input: Box::new(join.clone()),
				predicate: filter.clone(),
				disposition: FilterDisposition::EvaluateInToron,
			}),
			group_expr: BoundExprProgramBuilder::new().build(),
			aggr_expr: aggr_expr.clone(),
			output_schema: output_schema.clone(),
		};
		(executor, plan, join, filter, aggr_expr, output_schema)
	}

	fn streaming_filter_fixture(
		kind: StreamingValueKind,
		rows: usize,
		blob_len: usize,
		budget: TaskBudget,
		keep_every: usize,
		replay_id: JoinReplayId,
	) -> (Executor, ExecutionPlan, ExecutionPlan, BoundExprProgram) {
		let amount_type = match kind {
			StreamingValueKind::Float64 => DataType::Float64,
			StreamingValueKind::Decimal => DataType::Decimal128(20, 2),
		};
		let left_schema = Arc::new(Schema::new(vec![
			Field::new("pk", DataType::Int64, false),
			Field::new("amount", amount_type.clone(), true),
			Field::new("blob", DataType::Utf8, false),
		]));
		let right_schema = Arc::new(Schema::new(vec![
			Field::new("pk", DataType::Int64, false),
			Field::new("keep", DataType::Int64, false),
		]));
		let keys = (0..i64::try_from(rows).expect("rows fit i64")).collect::<Vec<_>>();
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
						.map(|row| Some(((row % 17) as i128 + 1) * 100))
						.collect::<Vec<_>>(),
				)
				.with_precision_and_scale(20, 2)
				.expect("decimal amount"),
			),
		};
		let blob = "x".repeat(blob_len);
		let left_batch = RecordBatch::try_new(
			left_schema.clone(),
			vec![
				Arc::new(Int64Array::from(keys.clone())),
				amount,
				Arc::new(StringArray::from(vec![blob.as_str(); rows])),
			],
		)
		.expect("left streaming filter batch");
		let keep_values = (0..rows)
			.map(|row| {
				if keep_every == 0 || row % keep_every == 0 {
					1
				} else {
					0
				}
			})
			.collect::<Vec<_>>();
		let right_batch = RecordBatch::try_new(
			right_schema.clone(),
			vec![
				Arc::new(Int64Array::from(keys)),
				Arc::new(Int64Array::from(keep_values)),
			],
		)
		.expect("right streaming filter batch");
		let storage = Arc::new(TableStorageScan {
			batches: std::collections::HashMap::from([
				(TableId(1), left_batch),
				(TableId(2), right_batch),
			]),
		});
		let executor = Executor::with_context(
			storage,
			ExecutionContext::local_test().with_task_budget(budget),
		);
		let join_output_schema = Arc::new(Schema::new(
			left_schema
				.fields()
				.iter()
				.chain(right_schema.fields().iter())
				.map(|field| field.as_ref().clone())
				.collect::<Vec<_>>(),
		));
		let join = ExecutionPlan::Join {
			replay_id,
			replay_eligibility: BroadcastReplayEligibility::Terminal,
			left: Box::new(test_scan_plan(TableId(1), left_schema, 1)),
			right: Box::new(test_scan_plan(TableId(2), right_schema, 2)),
			kind: JoinKind::Inner,
			left_keys: vec![ColumnOrdinal(0)],
			right_keys: vec![ColumnOrdinal(0)],
			match_filter: None,
			null_safe_keys: false,
			strategy: JoinStrategy::HashPartitioned,
			output_schema: join_output_schema,
		};
		let filter = keep_filter_program(ColumnOrdinal(4));
		let plan = ExecutionPlan::Filter {
			input: Box::new(join.clone()),
			predicate: filter.clone(),
			disposition: FilterDisposition::EvaluateInToron,
		};
		(executor, plan, join, filter)
	}

	fn reoriented_inner_chain_fixture(
		kind: StreamingValueKind,
		rows: usize,
		blob_len: usize,
		budget: TaskBudget,
		streaming_replay_ids: bool,
		sparse_order_matches: bool,
	) -> (Executor, ExecutionPlan) {
		let amount_type = match kind {
			StreamingValueKind::Float64 => DataType::Float64,
			StreamingValueKind::Decimal => DataType::Decimal128(20, 2),
		};
		let supplier_schema = Arc::new(Schema::new(vec![
			Field::new("s_suppkey", DataType::Int64, false),
			Field::new("s_nationkey", DataType::Int64, false),
		]));
		let lineitem_schema = Arc::new(Schema::new(vec![
			Field::new("l_orderkey", DataType::Int64, false),
			Field::new("l_suppkey", DataType::Int64, false),
			Field::new("amount", amount_type.clone(), true),
			Field::new("keep", DataType::Int64, false),
			Field::new("blob", DataType::Utf8, false),
		]));
		let orders_schema = Arc::new(Schema::new(vec![
			Field::new("o_orderkey", DataType::Int64, false),
			Field::new("o_custkey", DataType::Int64, false),
		]));
		let customer_schema = Arc::new(Schema::new(vec![
			Field::new("c_custkey", DataType::Int64, false),
			Field::new("c_nationkey", DataType::Int64, false),
		]));
		let nation_schema = Arc::new(Schema::new(vec![
			Field::new("n_nationkey", DataType::Int64, false),
			Field::new("n_name", DataType::Utf8, false),
		]));

		let supplier_batch = RecordBatch::try_new(
			supplier_schema.clone(),
			vec![
				Arc::new(Int64Array::from(vec![10, 20])),
				Arc::new(Int64Array::from(vec![1, 2])),
			],
		)
		.expect("supplier batch");
		let keys = (0..rows)
			.map(|row| {
				if sparse_order_matches && row >= 3 {
					-1
				} else {
					i64::try_from(row % 3).expect("row key fits")
				}
			})
			.collect::<Vec<_>>();
		let suppkeys = (0..rows)
			.map(|row| if row % 2 == 0 { 10 } else { 20 })
			.collect::<Vec<_>>();
		let amount: ArrayRef = match kind {
			StreamingValueKind::Float64 => Arc::new(Float64Array::from(vec![Some(1.0); rows])),
			StreamingValueKind::Decimal => Arc::new(
				Decimal128Array::from(vec![Some(100i128); rows])
					.with_precision_and_scale(20, 2)
					.expect("decimal amount"),
			),
		};
		let blob = "x".repeat(blob_len);
		let lineitem_batch = RecordBatch::try_new(
			lineitem_schema.clone(),
			vec![
				Arc::new(Int64Array::from(keys)),
				Arc::new(Int64Array::from(suppkeys)),
				amount,
				Arc::new(Int64Array::from(vec![1; rows])),
				Arc::new(StringArray::from(vec![blob.as_str(); rows])),
			],
		)
		.expect("lineitem batch");
		let orders_rows = if sparse_order_matches { rows } else { 3 };
		let orders_batch = RecordBatch::try_new(
			orders_schema.clone(),
			vec![
				Arc::new(Int64Array::from(
					(0..orders_rows)
						.map(|row| i64::try_from(row).expect("order key fits"))
						.collect::<Vec<_>>(),
				)),
				Arc::new(Int64Array::from(
					(0..orders_rows)
						.map(|row| {
							if row == 1 {
								200
							} else if row <= 2 {
								100
							} else {
								300
							}
						})
						.collect::<Vec<_>>(),
				)),
			],
		)
		.expect("orders batch");
		let customer_batch = RecordBatch::try_new(
			customer_schema.clone(),
			vec![
				Arc::new(Int64Array::from(vec![100, 200])),
				Arc::new(Int64Array::from(vec![2, 1])),
			],
		)
		.expect("customer batch");
		let nation_batch = RecordBatch::try_new(
			nation_schema.clone(),
			vec![
				Arc::new(Int64Array::from(vec![1, 2])),
				Arc::new(StringArray::from(vec!["FRANCE", "GERMANY"])),
			],
		)
		.expect("nation batch");
		let storage = Arc::new(TableStorageScan {
			batches: std::collections::HashMap::from([
				(TableId(1), lineitem_batch),
				(TableId(2), customer_batch),
				(TableId(3), orders_batch),
				(TableId(4), nation_batch),
				(TableId(5), supplier_batch),
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
		let supplier = exchange_plan(
			test_scan_plan_with_first_field_key(TableId(5), supplier_schema.clone(), 5),
			bound_column_root("s_suppkey", ColumnOrdinal(0), DataType::Int64, false),
			4,
		);
		let lineitem = exchange_plan(
			test_scan_plan_with_first_field_key(TableId(1), lineitem_schema.clone(), 1),
			bound_column_root("l_suppkey", ColumnOrdinal(1), DataType::Int64, false),
			4,
		);
		let join0_schema = concat_test_schema(&supplier_schema, &lineitem_schema);
		let join0 = inner_join_plan(
			replay(0),
			supplier,
			lineitem,
			vec![ColumnOrdinal(0)],
			vec![ColumnOrdinal(1)],
			join0_schema,
		);
		let lower_filter = ExecutionPlan::Filter {
			input: Box::new(join0),
			predicate: keep_filter_program(ColumnOrdinal(5)),
			disposition: FilterDisposition::EvaluateInToron,
		};
		let mut current = exchange_plan(
			lower_filter,
			bound_column_root("l_orderkey", ColumnOrdinal(2), DataType::Int64, false),
			4,
		);

		let dimensions = [
			(
				replay(1),
				test_scan_plan_with_first_field_key(TableId(3), orders_schema.clone(), 3),
				bound_column_root("o_orderkey", ColumnOrdinal(0), DataType::Int64, false),
				vec![ColumnOrdinal(2)],
				vec![ColumnOrdinal(0)],
			),
			(
				replay(2),
				test_scan_plan_with_first_field_key(TableId(2), customer_schema.clone(), 2),
				bound_column_root("c_custkey", ColumnOrdinal(0), DataType::Int64, false),
				vec![ColumnOrdinal(8)],
				vec![ColumnOrdinal(0)],
			),
			(
				replay(3),
				test_scan_plan_with_first_field_key(TableId(4), nation_schema.clone(), 4),
				bound_column_root("n_nationkey", ColumnOrdinal(0), DataType::Int64, false),
				vec![ColumnOrdinal(1)],
				vec![ColumnOrdinal(0)],
			),
			(
				replay(4),
				test_scan_plan_with_first_field_key(TableId(4), nation_schema.clone(), 4),
				bound_column_root("n_nationkey", ColumnOrdinal(0), DataType::Int64, false),
				vec![ColumnOrdinal(10)],
				vec![ColumnOrdinal(0)],
			),
		];
		for (replay_id, dimension_scan, dimension_key, left_keys, right_keys) in dimensions {
			let dimension = exchange_plan(dimension_scan, dimension_key, 4);
			let output_schema = concat_test_schema(&current.schema(), &dimension.schema());
			let joined = inner_join_plan(
				replay_id,
				current,
				dimension,
				left_keys,
				right_keys,
				output_schema,
			);
			let next_key = match replay_id.0 {
				1 => bound_column_root("o_custkey", ColumnOrdinal(8), DataType::Int64, false),
				2 => bound_column_root("s_nationkey", ColumnOrdinal(1), DataType::Int64, false),
				3 => bound_column_root("c_nationkey", ColumnOrdinal(10), DataType::Int64, false),
				_ => bound_column_root("s_suppkey", ColumnOrdinal(0), DataType::Int64, false),
			};
			current = if replay_id.0 == 4 {
				joined
			} else {
				exchange_plan(joined, next_key, 4)
			};
		}
		let filtered = ExecutionPlan::Filter {
			input: Box::new(current),
			predicate: keep_filter_program(ColumnOrdinal(0)),
			disposition: FilterDisposition::EvaluateInToron,
		};
		let project_schema = Arc::new(Schema::new(vec![
			Field::new("supp_nation", DataType::Utf8, false),
			Field::new("cust_nation", DataType::Utf8, false),
			Field::new("l_year", DataType::Int64, false),
			Field::new("volume", amount_type.clone(), true),
		]));
		let project_exprs = bound_columns_program(vec![
			("supp_nation", ColumnOrdinal(12), DataType::Utf8, false),
			("cust_nation", ColumnOrdinal(14), DataType::Utf8, false),
			("l_year", ColumnOrdinal(2), DataType::Int64, false),
			("volume", ColumnOrdinal(4), amount_type.clone(), true),
		]);
		let projected = ExecutionPlan::Project {
			input: Box::new(filtered),
			exprs: project_exprs,
			output_schema: project_schema,
		};
		let aggregate_input = exchange_plan(
			projected,
			bound_columns_program(vec![
				("supp_nation", ColumnOrdinal(0), DataType::Utf8, false),
				("cust_nation", ColumnOrdinal(1), DataType::Utf8, false),
				("l_year", ColumnOrdinal(2), DataType::Int64, false),
			]),
			4,
		);
		let group_expr = bound_columns_program(vec![
			("supp_nation", ColumnOrdinal(0), DataType::Utf8, false),
			("cust_nation", ColumnOrdinal(1), DataType::Utf8, false),
			("l_year", ColumnOrdinal(2), DataType::Int64, false),
		]);
		let aggr_expr = vec![BoundAggregateCall {
			function: AggregateFunction::Sum,
			args: bound_column_root("volume", ColumnOrdinal(3), amount_type.clone(), true),
		}];
		let output_schema = Arc::new(Schema::new(vec![
			Field::new("supp_nation", DataType::Utf8, false),
			Field::new("cust_nation", DataType::Utf8, false),
			Field::new("l_year", DataType::Int64, false),
			Field::new("sum", amount_type, true),
		]));
		let plan = ExecutionPlan::Aggregate {
			input: Box::new(aggregate_input),
			group_expr,
			aggr_expr,
			output_schema,
		};
		(executor, plan)
	}
	fn correlated_scalar_left_outer_fixture(
		rows: usize,
		blob_len: usize,
		budget: TaskBudget,
		streaming_replay_id: bool,
		kept_part_keys: i64,
		preserved_filter: bool,
	) -> (Executor, ExecutionPlan) {
		let fact_schema = Arc::new(Schema::new(vec![
			Field::new("partkey", DataType::Int64, false),
			Field::new("quantity", DataType::Float64, false),
			Field::new("extendedprice", DataType::Float64, false),
			Field::new("blob", DataType::Utf8, false),
		]));
		let part_schema = Arc::new(Schema::new(vec![
			Field::new("partkey", DataType::Int64, false),
			Field::new("keep", DataType::Int64, false),
		]));
		let partkeys = (0..rows)
			.map(|row| i64::try_from(row % 4).expect("part key fits"))
			.collect::<Vec<_>>();
		let quantities = (0..rows)
			.map(|row| Some(((row % 5) + 1) as f64))
			.collect::<Vec<_>>();
		let prices = (0..rows)
			.map(|row| Some(((row % 7) + 10) as f64))
			.collect::<Vec<_>>();
		let blob = "x".repeat(blob_len);
		let fact_batch = RecordBatch::try_new(
			fact_schema.clone(),
			vec![
				Arc::new(Int64Array::from(partkeys)),
				Arc::new(Float64Array::from(quantities)),
				Arc::new(Float64Array::from(prices)),
				Arc::new(StringArray::from(vec![blob.as_str(); rows])),
			],
		)
		.expect("fact batch");
		let part_batch = RecordBatch::try_new(
			part_schema.clone(),
			vec![
				Arc::new(Int64Array::from(vec![0, 1, 2, 3])),
				Arc::new(Int64Array::from(
					(0..4)
						.map(|key| if key < kept_part_keys { 1 } else { 0 })
						.collect::<Vec<_>>(),
				)),
			],
		)
		.expect("part batch");
		let storage = Arc::new(TableStorageScan {
			batches: std::collections::HashMap::from([
				(TableId(1), fact_batch),
				(TableId(2), part_batch),
			]),
		});
		let executor = Executor::with_context(
			storage,
			ExecutionContext::local_test().with_task_budget(budget),
		);
		let fact_scan = test_scan_plan_with_first_field_key(TableId(1), fact_schema.clone(), 1);
		let part_scan = test_scan_plan_with_first_field_key(TableId(2), part_schema.clone(), 2);
		let join0_schema = concat_test_schema(&fact_schema, &part_schema);
		let join0 = inner_join_plan(
			if streaming_replay_id {
				JoinReplayId(0)
			} else {
				JoinReplayId::SYNTHETIC
			},
			fact_scan.clone(),
			part_scan,
			vec![ColumnOrdinal(0)],
			vec![ColumnOrdinal(0)],
			join0_schema.clone(),
		);
		let preserved_input = if preserved_filter {
			ExecutionPlan::Filter {
				input: Box::new(join0),
				predicate: keep_filter_program(ColumnOrdinal(5)),
				disposition: FilterDisposition::EvaluateInToron,
			}
		} else {
			join0
		};
		let preserved_exchange = exchange_plan(
			preserved_input,
			bound_column_root("partkey", ColumnOrdinal(0), DataType::Int64, false),
			4,
		);
		let scalar_group_expr =
			bound_column_root("partkey", ColumnOrdinal(0), DataType::Int64, false);
		let scalar_aggr_expr = vec![BoundAggregateCall {
			function: AggregateFunction::Avg,
			args: bound_column_root("quantity", ColumnOrdinal(1), DataType::Float64, false),
		}];
		let scalar_schema = Arc::new(Schema::new(vec![
			Field::new("key_0", DataType::Int64, true),
			Field::new("value", DataType::Float64, true),
		]));
		let scalar = ExecutionPlan::Aggregate {
			input: Box::new(exchange_plan(fact_scan, scalar_group_expr.clone(), 4)),
			group_expr: scalar_group_expr,
			aggr_expr: scalar_aggr_expr,
			output_schema: scalar_schema,
		};
		let recombine_schema = concat_test_schema(&join0_schema, &scalar.schema());
		let recombine = ExecutionPlan::Join {
			replay_id: JoinReplayId(1),
			replay_eligibility: BroadcastReplayEligibility::Terminal,
			left: Box::new(preserved_exchange),
			right: Box::new(scalar),
			kind: JoinKind::LeftOuter,
			left_keys: vec![ColumnOrdinal(0)],
			right_keys: vec![ColumnOrdinal(0)],
			match_filter: None,
			null_safe_keys: false,
			strategy: JoinStrategy::HashPartitioned,
			output_schema: recombine_schema,
		};
		let recombine_filter = ExecutionPlan::Filter {
			input: Box::new(recombine),
			predicate: quantity_lt_avg_program(ColumnOrdinal(1), ColumnOrdinal(7)),
			disposition: FilterDisposition::EvaluateInToron,
		};
		let aggr_expr = vec![BoundAggregateCall {
			function: AggregateFunction::Sum,
			args: bound_column_root("extendedprice", ColumnOrdinal(2), DataType::Float64, false),
		}];
		let output_schema = Arc::new(Schema::new(vec![Field::new(
			"sum_extendedprice",
			DataType::Float64,
			true,
		)]));
		let plan = ExecutionPlan::Aggregate {
			input: Box::new(recombine_filter),
			group_expr: BoundExprProgramBuilder::new().build(),
			aggr_expr,
			output_schema,
		};
		(executor, plan)
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

	fn exchange_plan(
		input: ExecutionPlan,
		key: BoundExprProgram,
		partition_count: u32,
	) -> ExecutionPlan {
		ExecutionPlan::Exchange {
			input: Box::new(input),
			partition_count,
			key,
		}
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

	fn bound_columns_program(
		columns: Vec<(&str, ColumnOrdinal, DataType, bool)>,
	) -> BoundExprProgram {
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

	fn quantity_lt_avg_program(
		quantity_ordinal: ColumnOrdinal,
		avg_ordinal: ColumnOrdinal,
	) -> BoundExprProgram {
		let mut builder = BoundExprProgramBuilder::new();
		let quantity = builder.append_column(ResolvedColumn {
			name: ColumnName("quantity".to_string()),
			ordinal: quantity_ordinal,
			data_type: DataType::Float64,
			nullable: false,
			pbc_eligible: false,
		});
		let avg = builder.append_column(ResolvedColumn {
			name: ColumnName("avg_quantity".to_string()),
			ordinal: avg_ordinal,
			data_type: DataType::Float64,
			nullable: true,
			pbc_eligible: false,
		});
		let predicate = builder.append_binary(BoundBinaryOp::Lt, quantity, avg, DataType::Boolean);
		builder.push_root(predicate);
		builder.build()
	}

	fn suppkey_neq_residual_program(
		preserved_suppkey: ColumnOrdinal,
		existence_suppkey: ColumnOrdinal,
	) -> BoundExprProgram {
		let mut builder = BoundExprProgramBuilder::new();
		let preserved = builder.append_column(ResolvedColumn {
			name: ColumnName("preserved_suppkey".to_string()),
			ordinal: preserved_suppkey,
			data_type: DataType::Int64,
			nullable: false,
			pbc_eligible: false,
		});
		let existence = builder.append_column(ResolvedColumn {
			name: ColumnName("existence_suppkey".to_string()),
			ordinal: existence_suppkey,
			data_type: DataType::Int64,
			nullable: false,
			pbc_eligible: false,
		});
		let predicate =
			builder.append_binary(BoundBinaryOp::Neq, preserved, existence, DataType::Boolean);
		builder.push_root(predicate);
		builder.build()
	}

	fn suppkey_neq_and_late_residual_program(
		preserved_suppkey: ColumnOrdinal,
		existence_suppkey: ColumnOrdinal,
		existence_late: ColumnOrdinal,
	) -> BoundExprProgram {
		let mut builder = BoundExprProgramBuilder::new();
		let preserved = builder.append_column(ResolvedColumn {
			name: ColumnName("preserved_suppkey".to_string()),
			ordinal: preserved_suppkey,
			data_type: DataType::Int64,
			nullable: false,
			pbc_eligible: false,
		});
		let existence = builder.append_column(ResolvedColumn {
			name: ColumnName("existence_suppkey".to_string()),
			ordinal: existence_suppkey,
			data_type: DataType::Int64,
			nullable: false,
			pbc_eligible: false,
		});
		let late = builder.append_column(ResolvedColumn {
			name: ColumnName("late".to_string()),
			ordinal: existence_late,
			data_type: DataType::Int64,
			nullable: false,
			pbc_eligible: false,
		});
		let zero = builder.append_literal(ScalarValue::Int64(Some(0)));
		let different =
			builder.append_binary(BoundBinaryOp::Neq, preserved, existence, DataType::Boolean);
		let late = builder.append_binary(BoundBinaryOp::Gt, late, zero, DataType::Boolean);
		let predicate =
			builder.append_binary(BoundBinaryOp::And, different, late, DataType::Boolean);
		builder.push_root(predicate);
		builder.build()
	}

	fn semi_anti_runtime<'a>(
		kind: JoinKind,
		preserved_schema: SchemaRef,
		existence_batch: RecordBatch,
		residual: &'a BoundExprProgram,
	) -> StreamingSemiAntiRuntime<'a> {
		let existence_key_arrays =
			key_arrays(&existence_batch, &[ColumnOrdinal(0)]).expect("existence keys");
		let key_plan = JoinKeyPlan::from_build_arrays_and_probe_schema(
			&existence_key_arrays,
			&preserved_schema,
			&[ColumnOrdinal(0)],
			false,
		)
		.expect("key plan");
		let existence_rows = build_in_memory_join_rows(
			key_plan,
			&existence_key_arrays,
			existence_batch.num_rows(),
			u64::MAX,
			false,
		)
		.expect("build existence rows")
		.expect("in-memory existence rows");
		StreamingSemiAntiRuntime {
			replay_id: JoinReplayId::SYNTHETIC,
			kind,
			strategy: JoinStrategy::HashPartitioned,
			existence_row_count: existence_batch.num_rows() as u64,
			existence: StreamingSemiAntiExistence::InMemory {
				batch: existence_batch,
				rows: existence_rows,
			},
			key_plan,
			preserved_keys: vec![ColumnOrdinal(0)],
			match_filter: Some(residual),
			output_schema: preserved_schema,
			probe_rows: 0,
			output_rows: 0,
		}
	}

	async fn materialized_join_filter_aggregate(
		executor: &Executor,
		join: &ExecutionPlan,
		filter: &BoundExprProgram,
		aggr_expr: &[BoundAggregateCall],
		output_schema: &SchemaRef,
	) -> Result<Vec<RecordBatch>, ExecutionError> {
		let joined = executor.execute_bound(join, None).await?;
		let filtered =
			executor.filter_batches_with_bound_predicate(filter, joined, "filter predicate")?;
		executor.bound_global_aggregate(&filtered, aggr_expr, output_schema)
	}

	async fn materialized_join_filter_exchange(
		executor: &Executor,
		join: &ExecutionPlan,
		filter: &BoundExprProgram,
		key: &BoundExprProgram,
		partition_count: u32,
	) -> Result<Vec<RecordBatch>, ExecutionError> {
		let filtered = materialized_join_filter(executor, join, filter).await?;
		let schema = join.schema();
		let mut partition_batches: Vec<Vec<RecordBatch>> = (0..usize::try_from(partition_count)
			.expect("partition count fits usize"))
			.map(|_| Vec::new())
			.collect();
		for batch in filtered {
			for (partition, partitioned) in
				executor.repartition_exchange_batch(&batch, partition_count, key)?
			{
				partition_batches[partition].push(partitioned);
			}
		}
		finalize_exchange_partitions(schema, partition_batches)
	}

	async fn materialized_join_filter(
		executor: &Executor,
		join: &ExecutionPlan,
		filter: &BoundExprProgram,
	) -> Result<Vec<RecordBatch>, ExecutionError> {
		let joined = executor.execute_bound(join, None).await?;
		executor.filter_batches_with_bound_predicate(filter, joined, "filter predicate")
	}

	fn assert_record_batches_equal(left: &[RecordBatch], right: &[RecordBatch]) {
		let left_schema = left.first().expect("left batches").schema();
		let right_schema = right.first().expect("right batches").schema();
		assert_eq!(left_schema, right_schema);
		let left_batch = concat_or_empty(left_schema.clone(), left).expect("left concat");
		let right_batch = concat_or_empty(right_schema, right).expect("right concat");
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

	fn materialization_guard_fixture() -> (RecordBatch, RecordBatch, SchemaRef) {
		let left_schema = Arc::new(Schema::new(vec![
			Field::new("left_id", DataType::Int64, false),
			Field::new("left_label", DataType::Utf8, true),
		]));
		let right_schema = Arc::new(Schema::new(vec![
			Field::new("right_id", DataType::Int64, false),
			Field::new("right_value", DataType::Int64, false),
		]));
		let left_batch = RecordBatch::try_new(
			left_schema,
			vec![
				Arc::new(Int64Array::from(vec![1, 2])),
				Arc::new(StringArray::from(vec![Some("alpha"), Some("beta")])),
			],
		)
		.expect("left batch");
		let right_batch = RecordBatch::try_new(
			right_schema,
			vec![
				Arc::new(Int64Array::from(vec![1, 2])),
				Arc::new(Int64Array::from(vec![10, 20])),
			],
		)
		.expect("right batch");
		let output_schema = Arc::new(Schema::new(vec![
			Field::new("left_id", DataType::Int64, false),
			Field::new("left_label", DataType::Utf8, true),
			Field::new("right_id", DataType::Int64, false),
			Field::new("right_value", DataType::Int64, false),
		]));
		(left_batch, right_batch, output_schema)
	}

	fn spill_retained_guard_fixture() -> (RecordBatch, RecordBatch) {
		let left_schema = Arc::new(Schema::new(vec![Field::new(
			"left_id",
			DataType::Int64,
			false,
		)]));
		let right_schema = Arc::new(Schema::new(vec![Field::new(
			"right_id",
			DataType::Int64,
			false,
		)]));
		let left_batch =
			RecordBatch::try_new(left_schema, vec![Arc::new(Int64Array::from(vec![1, 2]))])
				.expect("left batch");
		let right_batch =
			RecordBatch::try_new(right_schema, vec![Arc::new(Int64Array::from(vec![1, 2]))])
				.expect("right batch");
		(left_batch, right_batch)
	}

	fn batch() -> RecordBatch {
		let schema = Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("amount", DataType::Int32, true),
			Field::new("region", DataType::Utf8, true),
		]));

		RecordBatch::try_new(
			schema,
			vec![
				Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
				Arc::new(Int32Array::from(vec![Some(10), Some(150), None, Some(200)])),
				Arc::new(StringArray::from(vec![
					Some("north"),
					Some("east"),
					Some("north"),
					Some("west"),
				])),
			],
		)
		.expect("batch")
	}
}
