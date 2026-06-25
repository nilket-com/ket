use crate::aggregate_kernel;
use crate::coordinator::{materialize_scan_stream, materialize_scan_stream_bounded};
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
	BoundExprProgram, BoundUnaryOp, BoundWindowCall, ExecutionPlan, ExecutionSortKey,
	FilterDisposition, JoinKind, JoinReplayId, JoinStrategy, MorselId, QueryId, QueryProfileHint,
	ScalarValue, SessionId, SystemTableKind, TableId, TemporalExtractField, TemporalTruncField,
	ValueFunction, WindowFrame, WindowFrameBound, WindowFrameDelta, WindowFrameUnit,
	WindowFunction, nanos_per_timestamp_unit,
};
#[cfg(test)]
use crate::ir::{Expr, LogicalPlan};
use crate::storage::{HlcBound, ScanRequest, ScanTablePin, StorageScan, TenantId, TraceContext};
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
	SortColumn, SortOptions, concat_batches, filter_record_batch, lexsort_to_indices, take,
};
use arrow::datatypes::{DataType, IntervalUnit, Schema, SchemaRef, TimeUnit};
use arrow::ipc::CompressionType;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use aws_lc_rs::aead::{AES_256_GCM_SIV, Aad, LessSafeKey, Nonce, UnboundKey};
use futures::StreamExt;
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fmt;
use std::io::{Cursor, ErrorKind};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tracing::Instrument;
use twox_hash::XxHash3_64;
use wendelon::schema::{
	LocalityPolicy, PrimaryKeyDescriptor, RowIdentityPolicy, RuntimeFilterDescriptor,
	SchemaEvolutionPolicy, TableObjectId, TableSchemaDescriptor,
};
#[cfg(test)]
use wendelon::storage::sketch::KllSketch;
use wendelon::storage::{AsyncFileOps, BlockingFileOps, FileHandle, OpenFlags};

const JOIN_SPILL_PAGE_SIZE: usize = 64 * 1024;
const JOIN_SPILL_CACHE_TARGET_BYTES: u64 = 8 * 1024 * 1024;
/// ADR 335: the raw 64 MiB task cap false-trips accepted sf=1 controls; the output guard
/// targets the GiB-scale materializations that cgroup-kill before the engine can report a row.
const JOIN_OUTPUT_MATERIALIZATION_MEMORY_CAP_MULTIPLIER: u64 = 16;
/// ADR 336: Phase 0 measured q05 as the largest green spill-completer at 440,765,522
/// retained bytes and q18 at 1,423,571,737; 16x the default task memory cap sits in that gap.
const JOIN_SPILL_RETAINED_FOOTPRINT_MEMORY_CAP_MULTIPLIER: u64 = 16;
static NEXT_JOIN_SPILL_ID: AtomicU64 = AtomicU64::new(1);

/// ADR 255: the safety cap on recursive-CTE fixpoint iterations. Termination is normally delta-empty
/// (the recursive term self-limits via its `WHERE`); this is the hard net so an unbounded recursion
/// errors instead of hanging.
const RECURSIVE_CTE_MAX_ITERATIONS: usize = 1000;

#[derive(Debug, Clone, Copy)]
enum ArithmeticOp {
	Add,
	Sub,
	Mul,
	Div,
}

impl ArithmeticOp {
	fn name(self) -> &'static str {
		match self {
			Self::Add => "Add",
			Self::Sub => "Sub",
			Self::Mul => "Mul",
			Self::Div => "Div",
		}
	}
}

const F64_I32_LOWER: f64 = i32::MIN as f64;
const F64_I32_UPPER: f64 = 2147483648.0;
const F64_I64_LOWER: f64 = i64::MIN as f64;
const F64_I64_UPPER: f64 = 9223372036854775808.0;

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
			join_spill_file_ops: Arc::new(BlockingFileOps),
			join_spill_dir: std::env::temp_dir().join("toron_join_spill"),
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

	pub(crate) fn filter_batches_with_bound_predicate(
		&self,
		predicate: &BoundExprProgram,
		batches: Vec<RecordBatch>,
		context: &'static str,
	) -> Result<Vec<RecordBatch>, ExecutionError> {
		let mut output = Vec::with_capacity(batches.len());
		for batch in batches {
			let mask = self.evaluate_bound_single_root(predicate, &batch, context)?;
			let mask_bool = mask
				.as_any()
				.downcast_ref::<BooleanArray>()
				.ok_or_else(|| ExecutionError::InvalidExpression {
					message: format!("{context} must return boolean"),
				})?;
			output.push(filter_record_batch(&batch, mask_bool)?);
		}
		Ok(output)
	}

	fn filter_batch_with_bound_predicate(
		&self,
		predicate: &BoundExprProgram,
		batch: RecordBatch,
		context: &'static str,
	) -> Result<RecordBatch, ExecutionError> {
		let mask = self.evaluate_bound_single_root(predicate, &batch, context)?;
		let mask_bool = mask
			.as_any()
			.downcast_ref::<BooleanArray>()
			.ok_or_else(|| ExecutionError::InvalidExpression {
				message: format!("{context} must return boolean"),
			})?;
		Ok(filter_record_batch(&batch, mask_bool)?)
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
					Ok(materialize_scan_stream(stream).await?)
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
						for i in 0..num_rows {
							let bucket = partition_bucket(&key_arrays, i, *partition_count)?;
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
				ExecutionPlan::Scan {
					local_table_id,
					table,
					fingerprint,
					projection,
					pbc_program,
					row_limit,
					row_identity,
					profile_hint,
					..
				} => {
					let req = ScanRequest {
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
					};
					let span = crate::telemetry::storage_scan_span(&req);
					let stream = self.storage.scan(req).instrument(span).await?;
					Ok(match row_limit {
						Some(limit) => materialize_scan_stream_bounded(stream, *limit).await?,
						None => materialize_scan_stream(stream).await?,
					})
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
							.try_streaming_collapsing_global_aggregate(
								input,
								aggr_expr,
								output_schema,
								morsel,
							)
							.await?
						{
							return Ok(output);
						}
						let input_batches = self.execute_bound(input, morsel).await?;
						self.bound_global_aggregate(&input_batches, aggr_expr, output_schema)
					} else {
						if let Some(output) = self
							.try_streaming_inner_prefix_semi_anti_grouped_aggregate(
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
						if let Some(output) = self
							.try_streaming_reoriented_inner_chain_grouped_aggregate(
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
					let input_batches = self.execute_bound(input, morsel).await?;
					execute_sort(
						input.schema(),
						&input_batches,
						order_by,
						*limit,
						*offset,
						output_schema,
						self.context.task_budget,
					)
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

	#[cfg(test)]
	pub fn evaluate_expr(
		&self,
		expr: &Expr,
		batch: &RecordBatch,
	) -> Result<ArrayRef, ExecutionError> {
		match expr {
			Expr::Column(column_ref) => {
				let ordinal = batch.schema().index_of(&column_ref.name.0).map_err(|_| {
					ExecutionError::ColumnNotFound {
						name: column_ref.name.0.clone(),
					}
				})?;
				Ok(batch.column(ordinal).clone())
			}
			Expr::Literal(val) => {
				let num_rows = batch.num_rows();
				match val {
					ScalarValue::Boolean(v) => Ok(Arc::new(BooleanArray::from(vec![*v; num_rows]))),
					ScalarValue::Int32(v) => Ok(Arc::new(Int32Array::from(vec![*v; num_rows]))),
					ScalarValue::Int64(v) => {
						Ok(Arc::new(arrow::array::Int64Array::from(vec![*v; num_rows])))
					}
					ScalarValue::Date32(v) => Ok(Arc::new(Date32Array::from(vec![*v; num_rows]))),
					ScalarValue::Timestamp { value, unit } => {
						Ok(timestamp_literal_array(value, *unit, num_rows))
					}
					ScalarValue::Interval(value) => {
						Ok(Arc::new(DurationNanosecondArray::from(vec![
							*value;
							num_rows
						])))
					}
					ScalarValue::CalendarMonthInterval(value) => {
						Ok(Arc::new(IntervalYearMonthArray::from(vec![
							*value;
							num_rows
						])))
					}
					ScalarValue::Float32(v) => {
						Ok(Arc::new(arrow::array::Float32Array::from(vec![
							*v;
							num_rows
						])))
					}
					ScalarValue::Float64(v) => Ok(Arc::new(Float64Array::from(vec![*v; num_rows]))),
					ScalarValue::Decimal128 {
						value,
						precision,
						scale,
					} => Ok(Arc::new(
						Decimal128Array::from(vec![*value; num_rows])
							.with_precision_and_scale(*precision, *scale)
							.map_err(|error| ExecutionError::Kernel {
								message: error.to_string(),
							})?,
					)),
					ScalarValue::Utf8(v) => Ok(Arc::new(StringArray::from(vec![
						v.clone()
							.as_deref();
						num_rows
					]))),
				}
			}
			Expr::UntypedNull => Err(ExecutionError::Kernel {
				message: "internal: unresolved NULL literal reached execution".to_string(),
			}),
			Expr::Cast(inner, target) => {
				let arr = self.evaluate_expr(inner, batch)?;
				evaluate_cast(&arr, target)
			}
			Expr::TryCast(inner, target) => {
				let arr = self.evaluate_expr(inner, batch)?;
				evaluate_try_cast(&arr, target)
			}
			Expr::LogicalAscription { expr, .. } => self.evaluate_expr(expr, batch),
			Expr::SqlLogicalCast { .. } => Err(ExecutionError::Kernel {
				message: "internal: unresolved SQL logical CAST reached execution".to_string(),
			}),
			Expr::SqlConvert { .. } => Err(ExecutionError::Kernel {
				message: "internal: unresolved SQL CONVERT reached execution".to_string(),
			}),
			Expr::Add(left, right) => {
				let left_arr = self.evaluate_expr(left, batch)?;
				let right_arr = self.evaluate_expr(right, batch)?;
				evaluate_unbound_arithmetic(ArithmeticOp::Add, left_arr, right_arr)
			}
			Expr::Sub(left, right) => {
				let left_arr = self.evaluate_expr(left, batch)?;
				let right_arr = self.evaluate_expr(right, batch)?;
				evaluate_unbound_arithmetic(ArithmeticOp::Sub, left_arr, right_arr)
			}
			Expr::Mul(left, right) => {
				let left_arr = self.evaluate_expr(left, batch)?;
				let right_arr = self.evaluate_expr(right, batch)?;
				evaluate_unbound_arithmetic(ArithmeticOp::Mul, left_arr, right_arr)
			}
			Expr::Div(left, right) => {
				let left_arr = self.evaluate_expr(left, batch)?;
				let right_arr = self.evaluate_expr(right, batch)?;
				evaluate_unbound_arithmetic(ArithmeticOp::Div, left_arr, right_arr)
			}
			Expr::Eq(left, right) => {
				let left_arr = self.evaluate_expr(left, batch)?;
				let right_arr = self.evaluate_expr(right, batch)?;
				let res = eq(&left_arr, &right_arr)?;
				Ok(Arc::new(res))
			}
			Expr::Neq(left, right) => {
				let left_arr = self.evaluate_expr(left, batch)?;
				let right_arr = self.evaluate_expr(right, batch)?;
				let res = neq(&left_arr, &right_arr)?;
				Ok(Arc::new(res))
			}
			Expr::Lt(left, right) => {
				let left_arr = self.evaluate_expr(left, batch)?;
				let right_arr = self.evaluate_expr(right, batch)?;
				let res = lt(&left_arr, &right_arr)?;
				Ok(Arc::new(res))
			}
			Expr::Lte(left, right) => {
				let left_arr = self.evaluate_expr(left, batch)?;
				let right_arr = self.evaluate_expr(right, batch)?;
				let res = lt_eq(&left_arr, &right_arr)?;
				Ok(Arc::new(res))
			}
			Expr::Gt(left, right) => {
				let left_arr = self.evaluate_expr(left, batch)?;
				let right_arr = self.evaluate_expr(right, batch)?;
				let res = gt(&left_arr, &right_arr)?;
				Ok(Arc::new(res))
			}
			Expr::Gte(left, right) => {
				let left_arr = self.evaluate_expr(left, batch)?;
				let right_arr = self.evaluate_expr(right, batch)?;
				let res = gt_eq(&left_arr, &right_arr)?;
				Ok(Arc::new(res))
			}
			Expr::And(left, right) => {
				let left_arr = self.evaluate_expr(left, batch)?;
				let right_arr = self.evaluate_expr(right, batch)?;
				let l_bool = left_arr
					.as_any()
					.downcast_ref::<BooleanArray>()
					.ok_or_else(|| ExecutionError::InvalidExpression {
						message: "AND left operand must be boolean".to_string(),
					})?;
				let r_bool = right_arr
					.as_any()
					.downcast_ref::<BooleanArray>()
					.ok_or_else(|| ExecutionError::InvalidExpression {
						message: "AND right operand must be boolean".to_string(),
					})?;
				let res = and(l_bool, r_bool)?;
				Ok(Arc::new(res))
			}
			Expr::Or(left, right) => {
				let left_arr = self.evaluate_expr(left, batch)?;
				let right_arr = self.evaluate_expr(right, batch)?;
				let l_bool = left_arr
					.as_any()
					.downcast_ref::<BooleanArray>()
					.ok_or_else(|| ExecutionError::InvalidExpression {
						message: "OR left operand must be boolean".to_string(),
					})?;
				let r_bool = right_arr
					.as_any()
					.downcast_ref::<BooleanArray>()
					.ok_or_else(|| ExecutionError::InvalidExpression {
						message: "OR right operand must be boolean".to_string(),
					})?;
				let res = or(l_bool, r_bool)?;
				Ok(Arc::new(res))
			}
			Expr::Not(inner) => {
				let arr = self.evaluate_expr(inner, batch)?;
				let bool_arr = arr.as_any().downcast_ref::<BooleanArray>().ok_or_else(|| {
					ExecutionError::InvalidExpression {
						message: "NOT operand must be boolean".to_string(),
					}
				})?;
				let res = not(bool_arr)?;
				Ok(Arc::new(res))
			}
			Expr::InList {
				expr,
				list,
				negated,
			} => {
				let expr_arr = self.evaluate_expr(expr, batch)?;
				let item_arrs = list
					.iter()
					.map(|item| self.evaluate_expr(item, batch))
					.collect::<Result<Vec<_>, _>>()?;
				evaluate_in_list(&expr_arr, &item_arrs, *negated, batch.num_rows())
			}
			Expr::Like {
				expr,
				pattern,
				negated,
				case_insensitive,
			} => {
				let expr_arr = self.evaluate_expr(expr, batch)?;
				evaluate_like(&expr_arr, pattern, *negated, *case_insensitive)
			}
			Expr::Substring {
				input,
				from,
				length,
			} => {
				let input_arr = self.evaluate_expr(input, batch)?;
				evaluate_substring(&input_arr, *from, *length)
			}
			Expr::Case {
				when_then,
				else_expr,
			} => {
				if when_then.is_empty() {
					return Err(ExecutionError::InvalidExpression {
						message: "CASE must have at least one WHEN branch".to_string(),
					});
				}
				let when_then = when_then
					.iter()
					.map(|(condition, result)| {
						Ok((
							self.evaluate_expr(condition, batch)?,
							self.evaluate_expr(result, batch)?,
						))
					})
					.collect::<Result<Vec<_>, ExecutionError>>()?;
				let else_array = if let Some(else_expr) = else_expr {
					self.evaluate_expr(else_expr, batch)?
				} else {
					let data_type = when_then
						.first()
						.map(|(_, result)| result.data_type().clone())
						.ok_or_else(|| ExecutionError::InvalidExpression {
							message: "CASE must have at least one result branch".to_string(),
						})?;
					arrow::array::new_null_array(&data_type, batch.num_rows())
				};
				evaluate_case(&when_then, else_array)
			}
			Expr::IsNull(inner) => {
				let arr = self.evaluate_expr(inner, batch)?;
				let res = is_null(&arr)?;
				Ok(Arc::new(res))
			}
			Expr::IsNotNull(inner) => {
				let arr = self.evaluate_expr(inner, batch)?;
				let res = is_not_null(&arr)?;
				Ok(Arc::new(res))
			}
			Expr::TimeBucket(interval, inner) => {
				let arr = self.evaluate_expr(inner, batch)?;
				if let Some(int_arr) = arr.as_any().downcast_ref::<Int64Array>() {
					let interval_ms = interval.as_millis() as i64;
					let bucketed: Vec<Option<i64>> = int_arr
						.iter()
						.map(|v| v.map(|v| (v / interval_ms) * interval_ms))
						.collect();
					Ok(Arc::new(Int64Array::from(bucketed)) as ArrayRef)
				} else {
					Err(ExecutionError::InvalidExpression {
						message: "TimeBucket expects Int64 (ms) input".to_string(),
					})
				}
			}
			Expr::AtTimeZone(inner, zone) => {
				let arr = self.evaluate_expr(inner, batch)?;
				evaluate_at_time_zone(&arr, zone)
			}
			Expr::Extract(field, inner) => match inner.as_ref() {
				Expr::AtTimeZone(timestamp, zone) => {
					let arr = self.evaluate_expr(timestamp, batch)?;
					evaluate_extract(*field, Some(zone), &arr)
				}
				_ => {
					let arr = self.evaluate_expr(inner, batch)?;
					if arr.data_type() == &DataType::Date32 {
						evaluate_extract(*field, None, &arr)
					} else {
						Err(ExecutionError::InvalidExpression {
							message: "unbound EXTRACT on Timestamp requires explicit AT TIME ZONE"
								.to_string(),
						})
					}
				}
			},
			Expr::DateTrunc(field, inner) => match inner.as_ref() {
				Expr::AtTimeZone(timestamp, zone) => {
					let arr = self.evaluate_expr(timestamp, batch)?;
					evaluate_date_trunc(*field, zone, &arr)
				}
				_ => Err(ExecutionError::InvalidExpression {
					message: "unbound date_trunc requires explicit AT TIME ZONE".to_string(),
				}),
			},
		}
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

	pub fn evaluate_bound_program(
		&self,
		program: &BoundExprProgram,
		batch: &RecordBatch,
	) -> Result<Vec<ArrayRef>, ExecutionError> {
		Self::evaluate_bound_program_without_storage(program, batch)
	}

	pub fn evaluate_bound_program_without_storage(
		program: &BoundExprProgram,
		batch: &RecordBatch,
	) -> Result<Vec<ArrayRef>, ExecutionError> {
		program
			.validate()
			.map_err(|error| ExecutionError::InvalidExpression {
				message: format!("invalid bound expression program: {error:?}"),
			})?;
		let mut outputs: Vec<Option<ArrayRef>> = vec![None; program.nodes.len()];
		for (idx, node) in program.nodes.iter().enumerate() {
			let output = match node {
				BoundExprNode::Column(column) => {
					let ordinal = column.ordinal.0 as usize;
					batch.columns().get(ordinal).cloned().ok_or_else(|| {
						ExecutionError::InvalidExpression {
							message: format!("column ordinal {ordinal} out of bounds"),
						}
					})?
				}
				BoundExprNode::Literal(literal) => {
					let value = program.literals.get(*literal as usize).ok_or_else(|| {
						ExecutionError::InvalidExpression {
							message: format!("literal slot {literal} out of bounds"),
						}
					})?;
					literal_array(value, batch.num_rows())?
				}
				BoundExprNode::NullLiteral => {
					arrow::array::new_null_array(&DataType::Null, batch.num_rows())
				}
				BoundExprNode::Cast { input, target } => {
					let arr = evaluated_child(&outputs, *input)?;
					evaluate_cast(&arr, target)?
				}
				BoundExprNode::TryCast { input, target } => {
					let arr = evaluated_child(&outputs, *input)?;
					evaluate_try_cast(&arr, target)?
				}
				BoundExprNode::Binary { op, left, right } => {
					let left_arr = evaluated_child(&outputs, *left)?;
					let right_arr = evaluated_child(&outputs, *right)?;
					evaluate_bound_binary(program, op, *left, &left_arr, &right_arr)?
				}
				BoundExprNode::Unary { op, input } => {
					let arr = evaluated_child(&outputs, *input)?;
					evaluate_bound_unary(op, &arr)?
				}
				BoundExprNode::InList {
					expr,
					list,
					negated,
				} => {
					let expr_arr = evaluated_child(&outputs, *expr)?;
					let item_arrs = list
						.iter()
						.map(|item| evaluated_child(&outputs, *item))
						.collect::<Result<Vec<_>, _>>()?;
					evaluate_in_list(&expr_arr, &item_arrs, *negated, batch.num_rows())?
				}
				BoundExprNode::Like {
					expr,
					pattern,
					negated,
					case_insensitive,
				} => {
					let expr_arr = evaluated_child(&outputs, *expr)?;
					evaluate_like(&expr_arr, pattern, *negated, *case_insensitive)?
				}
				BoundExprNode::Substring {
					input,
					from,
					length,
				} => {
					let input_arr = evaluated_child(&outputs, *input)?;
					evaluate_substring(&input_arr, *from, *length)?
				}
				BoundExprNode::Case {
					when_then,
					else_input,
				} => {
					let when_then = when_then
						.iter()
						.map(|(condition, result)| {
							Ok((
								evaluated_child(&outputs, *condition)?,
								evaluated_child(&outputs, *result)?,
							))
						})
						.collect::<Result<Vec<_>, ExecutionError>>()?;
					let else_array = evaluated_child(&outputs, *else_input)?;
					evaluate_case(&when_then, else_array)?
				}
				BoundExprNode::TimeBucket { interval, input } => {
					let arr = evaluated_child(&outputs, *input)?;
					evaluate_time_bucket(*interval, &arr)?
				}
				BoundExprNode::AtTimeZone { input, zone } => {
					let arr = evaluated_child(&outputs, *input)?;
					evaluate_at_time_zone(&arr, zone)?
				}
				BoundExprNode::Extract { field, zone, input } => {
					let arr = evaluated_child(&outputs, *input)?;
					evaluate_extract(*field, zone.as_ref(), &arr)?
				}
				BoundExprNode::DateTrunc { field, zone, input } => {
					let arr = evaluated_child(&outputs, *input)?;
					evaluate_date_trunc(*field, zone, &arr)?
				}
			};
			outputs[idx] = Some(output);
		}
		program
			.roots
			.iter()
			.map(|root| evaluated_child(&outputs, *root))
			.collect()
	}

	fn evaluate_bound_single_root(
		&self,
		program: &BoundExprProgram,
		batch: &RecordBatch,
		context: &'static str,
	) -> Result<ArrayRef, ExecutionError> {
		let mut roots = self.evaluate_bound_program(program, batch)?.into_iter();
		let Some(root) = roots.next() else {
			return Err(ExecutionError::InvalidExpression {
				message: format!("{context} must have exactly one root"),
			});
		};
		if roots.next().is_some() {
			return Err(ExecutionError::InvalidExpression {
				message: format!("{context} must have exactly one root"),
			});
		}
		Ok(root)
	}

	fn bound_global_aggregate(
		&self,
		input_batches: &[RecordBatch],
		aggr_expr: &[BoundAggregateCall],
		output_schema: &arrow::datatypes::SchemaRef,
	) -> Result<Vec<RecordBatch>, ExecutionError> {
		let mut state = init_global_aggregate_state(aggr_expr)?;
		for batch in input_batches {
			self.fold_global_aggregate_state(&mut state, aggr_expr, batch)?;
		}
		finalize_global_aggregate_state(&state, aggr_expr, output_schema)
	}

	fn fold_global_aggregate_state(
		&self,
		state: &mut [AggState],
		aggr_expr: &[BoundAggregateCall],
		batch: &RecordBatch,
	) -> Result<(), ExecutionError> {
		for (state, call) in state.iter_mut().zip(aggr_expr.iter()) {
			match call.function {
				AggregateFunction::CountAll => {
					let AggState::Count(total) = state else {
						return Err(ExecutionError::InvalidExpression {
							message: "COUNT ALL reached non-COUNT accumulator".to_string(),
						});
					};
					*total = total
						.checked_add(i64::try_from(batch.num_rows()).map_err(|_| {
							ExecutionError::Kernel {
								message: "COUNT ALL row count exceeds Int64".to_string(),
							}
						})?)
						.ok_or_else(|| ExecutionError::Kernel {
							message: "COUNT ALL overflow".to_string(),
						})?;
				}
				AggregateFunction::Count => {
					let arr = self.evaluate_bound_single_root(&call.args, batch, "COUNT arg")?;
					let AggState::Count(total) = state else {
						return Err(ExecutionError::InvalidExpression {
							message: "COUNT reached non-COUNT accumulator".to_string(),
						});
					};
					for row in 0..batch.num_rows() {
						if !arr.is_null(row) {
							*total =
								total.checked_add(1).ok_or_else(|| ExecutionError::Kernel {
									message: "COUNT overflow".to_string(),
								})?;
						}
					}
				}
				AggregateFunction::Sum => {
					let arr = self.evaluate_bound_single_root(&call.args, batch, "SUM arg")?;
					for row in 0..batch.num_rows() {
						if let Some(value) = numeric_value(&arr, row)? {
							add_sum_value(state, value)?;
						}
					}
				}
				AggregateFunction::Avg => {
					let arr = self.evaluate_bound_single_root(&call.args, batch, "AVG arg")?;
					for row in 0..batch.num_rows() {
						if let Some(value) = numeric_value(&arr, row)? {
							add_avg_value(state, value)?;
						}
					}
				}
				AggregateFunction::Min => {
					let arr = self.evaluate_bound_single_root(&call.args, batch, "MIN arg")?;
					for row in 0..batch.num_rows() {
						update_min_state(state, &arr, row)?;
					}
				}
				AggregateFunction::Max => {
					let arr = self.evaluate_bound_single_root(&call.args, batch, "MAX arg")?;
					for row in 0..batch.num_rows() {
						update_max_state(state, &arr, row)?;
					}
				}
				AggregateFunction::LinearRegress | AggregateFunction::ApproxQuantile(_) => {
					return Err(ExecutionError::NotImplemented {
						feature: "bound experimental aggregate execution",
					});
				}
			}
		}
		Ok(())
	}
}

fn init_global_aggregate_state(
	aggr_expr: &[BoundAggregateCall],
) -> Result<Vec<AggState>, ExecutionError> {
	init_bound_states(aggr_expr)
}

fn finalize_global_aggregate_state(
	state: &[AggState],
	aggr_expr: &[BoundAggregateCall],
	output_schema: &arrow::datatypes::SchemaRef,
) -> Result<Vec<RecordBatch>, ExecutionError> {
	let mut results_cols = Vec::new();
	for (state, call) in state.iter().zip(aggr_expr.iter()) {
		match call.function {
			AggregateFunction::CountAll => {
				let AggState::Count(total) = state else {
					return Err(ExecutionError::InvalidExpression {
						message: "COUNT ALL reached non-COUNT accumulator".to_string(),
					});
				};
				results_cols.push(Arc::new(Int64Array::from(vec![*total])) as ArrayRef);
			}
			AggregateFunction::Count => {
				let AggState::Count(total) = state else {
					return Err(ExecutionError::InvalidExpression {
						message: "COUNT reached non-COUNT accumulator".to_string(),
					});
				};
				results_cols.push(Arc::new(Int64Array::from(vec![*total])) as ArrayRef);
			}
			AggregateFunction::Sum => {
				let output_type = output_schema.field(results_cols.len()).data_type().clone();
				results_cols.push(sum_state_array(&state, &output_type)?);
			}
			AggregateFunction::Avg => {
				results_cols.push(avg_state_array(
					&state,
					output_schema.field(results_cols.len()).data_type(),
				)?);
			}
			AggregateFunction::Min => {
				results_cols.push(min_max_state_array(
					&state,
					output_schema.field(results_cols.len()).data_type(),
				)?);
			}
			AggregateFunction::Max => {
				results_cols.push(min_max_state_array(
					&state,
					output_schema.field(results_cols.len()).data_type(),
				)?);
			}
			AggregateFunction::LinearRegress | AggregateFunction::ApproxQuantile(_) => {
				return Err(ExecutionError::NotImplemented {
					feature: "bound experimental aggregate execution",
				});
			}
		}
	}

	Ok(vec![RecordBatch::try_new(
		output_schema.clone(),
		results_cols,
	)?])
}

struct ReorientedInnerJoinStage<'a> {
	replay_id: JoinReplayId,
	probe_schema: SchemaRef,
	probe_keys: Vec<crate::ir::ColumnOrdinal>,
	build: &'a ExecutionPlan,
	build_keys: Vec<crate::ir::ColumnOrdinal>,
	output_schema: SchemaRef,
	strategy: JoinStrategy,
	post_filter: Option<&'a BoundExprProgram>,
}

struct ReorientedInnerJoinRuntime<'a> {
	replay_id: JoinReplayId,
	strategy: JoinStrategy,
	build_batch: RecordBatch,
	build_rows: HashMap<JoinKey, BuildRowList>,
	key_plan: JoinKeyPlan,
	probe_keys: Vec<crate::ir::ColumnOrdinal>,
	output_schema: SchemaRef,
	post_filter: Option<&'a BoundExprProgram>,
	probe_rows: u64,
	output_rows: u64,
}

struct StreamingSemiAntiStage<'a> {
	replay_id: JoinReplayId,
	kind: JoinKind,
	preserved_keys: Vec<crate::ir::ColumnOrdinal>,
	existence: &'a ExecutionPlan,
	existence_keys: Vec<crate::ir::ColumnOrdinal>,
	match_filter: &'a BoundExprProgram,
	output_schema: SchemaRef,
	strategy: JoinStrategy,
}

#[allow(dead_code)]
enum StreamingSemiAntiExistence {
	InMemory {
		batch: RecordBatch,
		rows: HashMap<JoinKey, BuildRowList>,
	},
	Spilled(SpilledSemiAntiExistenceBuild),
}

enum StreamingExistenceOp<'a> {
	Filter(&'a BoundExprProgram),
	Project {
		exprs: &'a BoundExprProgram,
		output_schema: SchemaRef,
	},
	Exchange {
		partition_count: u32,
		key: &'a BoundExprProgram,
	},
}

struct StreamingSemiAntiRuntime<'a> {
	replay_id: JoinReplayId,
	kind: JoinKind,
	strategy: JoinStrategy,
	existence: StreamingSemiAntiExistence,
	existence_row_count: u64,
	key_plan: JoinKeyPlan,
	preserved_keys: Vec<crate::ir::ColumnOrdinal>,
	match_filter: &'a BoundExprProgram,
	output_schema: SchemaRef,
	probe_rows: u64,
	output_rows: u64,
}

impl StreamingSemiAntiRuntime<'_> {
	async fn cleanup(self) {
		if let StreamingSemiAntiExistence::Spilled(spilled) = self.existence {
			spilled.cleanup().await;
		}
	}
}

struct StreamingSemiAntiChain<'a> {
	base: &'a ExecutionPlan,
	inner_stages: Vec<ReorientedInnerJoinStage<'a>>,
	semi_stage: StreamingSemiAntiStage<'a>,
	anti_stage: StreamingSemiAntiStage<'a>,
}

fn extract_reoriented_inner_chain(
	top: &ExecutionPlan,
) -> Option<(&ExecutionPlan, Vec<ReorientedInnerJoinStage<'_>>)> {
	let mut current = top;
	let mut stages = Vec::new();
	for expected in (1..=4).rev() {
		let ExecutionPlan::Join {
			replay_id,
			left,
			right,
			kind: JoinKind::Inner,
			left_keys,
			right_keys,
			match_filter: None,
			null_safe_keys: false,
			strategy: JoinStrategy::HashPartitioned,
			output_schema,
			..
		} = current
		else {
			return None;
		};
		if *replay_id != JoinReplayId(expected) {
			return None;
		}
		let ExecutionPlan::Exchange {
			input: left_input, ..
		} = left.as_ref()
		else {
			return None;
		};
		stages.push(ReorientedInnerJoinStage {
			replay_id: *replay_id,
			probe_schema: left.schema(),
			probe_keys: left_keys.clone(),
			build: right.as_ref(),
			build_keys: right_keys.clone(),
			output_schema: output_schema.clone(),
			strategy: JoinStrategy::HashPartitioned,
			post_filter: None,
		});
		if expected == 1 {
			stages.reverse();
			return Some((left.as_ref(), stages));
		}
		current = left_input.as_ref();
	}
	None
}

fn filter_input<'a>(plan: &'a ExecutionPlan) -> Option<(&'a ExecutionPlan, &'a BoundExprProgram)> {
	let ExecutionPlan::Filter {
		input,
		predicate,
		disposition: FilterDisposition::EvaluateInToron,
	} = plan
	else {
		return None;
	};
	Some((input.as_ref(), predicate))
}

fn exchange_input(plan: &ExecutionPlan) -> Option<&ExecutionPlan> {
	let ExecutionPlan::Exchange { input, .. } = plan else {
		return None;
	};
	Some(input.as_ref())
}

fn decompose_streaming_existence_plan(
	plan: &ExecutionPlan,
) -> Option<(&ExecutionPlan, Vec<StreamingExistenceOp<'_>>)> {
	let mut current = plan;
	let mut ops = Vec::new();
	loop {
		match current {
			ExecutionPlan::Exchange {
				input,
				partition_count,
				key,
			} => {
				ops.push(StreamingExistenceOp::Exchange {
					partition_count: *partition_count,
					key,
				});
				current = input.as_ref();
			}
			ExecutionPlan::Filter {
				input,
				predicate,
				disposition,
			} => {
				if *disposition == FilterDisposition::EvaluateInToron {
					ops.push(StreamingExistenceOp::Filter(predicate));
				}
				current = input.as_ref();
			}
			ExecutionPlan::Project {
				input,
				exprs,
				output_schema,
			} => {
				ops.push(StreamingExistenceOp::Project {
					exprs,
					output_schema: output_schema.clone(),
				});
				current = input.as_ref();
			}
			ExecutionPlan::Scan { .. } => {
				ops.reverse();
				return Some((current, ops));
			}
			_ => return None,
		}
	}
}

fn extract_streaming_inner_prefix_semi_anti_chain(
	top: &ExecutionPlan,
) -> Option<StreamingSemiAntiChain<'_>> {
	let ExecutionPlan::Join {
		replay_id: anti_replay_id,
		left: anti_left,
		right: anti_right,
		kind: JoinKind::LeftAnti,
		left_keys: anti_left_keys,
		right_keys: anti_right_keys,
		match_filter: Some(anti_filter),
		null_safe_keys: false,
		strategy: JoinStrategy::HashPartitioned,
		output_schema: anti_output_schema,
		..
	} = top
	else {
		return None;
	};
	if *anti_replay_id != JoinReplayId(4) {
		return None;
	}
	let semi = exchange_input(anti_left)?;
	let ExecutionPlan::Join {
		replay_id: semi_replay_id,
		left: semi_left,
		right: semi_right,
		kind: JoinKind::LeftSemi,
		left_keys: semi_left_keys,
		right_keys: semi_right_keys,
		match_filter: Some(semi_filter),
		null_safe_keys: false,
		strategy: JoinStrategy::HashPartitioned,
		output_schema: semi_output_schema,
		..
	} = semi
	else {
		return None;
	};
	if *semi_replay_id != JoinReplayId(3) {
		return None;
	}

	let (join2, join2_post_filter) = filter_input(exchange_input(semi_left)?)?;
	let ExecutionPlan::Join {
		replay_id: join2_replay_id,
		left: join2_left,
		right: join2_right,
		kind: JoinKind::Inner,
		left_keys: join2_left_keys,
		right_keys: join2_right_keys,
		match_filter: None,
		null_safe_keys: false,
		strategy: JoinStrategy::HashPartitioned,
		output_schema: join2_output_schema,
		..
	} = join2
	else {
		return None;
	};
	if *join2_replay_id != JoinReplayId(2) {
		return None;
	}

	let (join1, join1_post_filter) = filter_input(exchange_input(join2_left)?)?;
	let ExecutionPlan::Join {
		replay_id: join1_replay_id,
		left: join1_left,
		right: join1_right,
		kind: JoinKind::Inner,
		left_keys: join1_left_keys,
		right_keys: join1_right_keys,
		match_filter: None,
		null_safe_keys: false,
		strategy: JoinStrategy::HashPartitioned,
		output_schema: join1_output_schema,
		..
	} = join1
	else {
		return None;
	};
	if *join1_replay_id != JoinReplayId(1) {
		return None;
	}

	Some(StreamingSemiAntiChain {
		base: join1_left.as_ref(),
		inner_stages: vec![
			ReorientedInnerJoinStage {
				replay_id: *join1_replay_id,
				probe_schema: join1_left.schema(),
				probe_keys: join1_left_keys.clone(),
				build: join1_right.as_ref(),
				build_keys: join1_right_keys.clone(),
				output_schema: join1_output_schema.clone(),
				strategy: JoinStrategy::HashPartitioned,
				post_filter: Some(join1_post_filter),
			},
			ReorientedInnerJoinStage {
				replay_id: *join2_replay_id,
				probe_schema: join2_left.schema(),
				probe_keys: join2_left_keys.clone(),
				build: join2_right.as_ref(),
				build_keys: join2_right_keys.clone(),
				output_schema: join2_output_schema.clone(),
				strategy: JoinStrategy::HashPartitioned,
				post_filter: Some(join2_post_filter),
			},
		],
		semi_stage: StreamingSemiAntiStage {
			replay_id: *semi_replay_id,
			kind: JoinKind::LeftSemi,
			preserved_keys: semi_left_keys.clone(),
			existence: semi_right.as_ref(),
			existence_keys: semi_right_keys.clone(),
			match_filter: semi_filter,
			output_schema: semi_output_schema.clone(),
			strategy: JoinStrategy::HashPartitioned,
		},
		anti_stage: StreamingSemiAntiStage {
			replay_id: *anti_replay_id,
			kind: JoinKind::LeftAnti,
			preserved_keys: anti_left_keys.clone(),
			existence: anti_right.as_ref(),
			existence_keys: anti_right_keys.clone(),
			match_filter: anti_filter,
			output_schema: anti_output_schema.clone(),
			strategy: JoinStrategy::HashPartitioned,
		},
	})
}

impl Executor {
	async fn try_streaming_join0_filter(
		&self,
		input: &ExecutionPlan,
		predicate: &BoundExprProgram,
		disposition: FilterDisposition,
		morsel: Option<(MorselId, u32)>,
	) -> Result<Option<Vec<RecordBatch>>, ExecutionError> {
		if disposition == FilterDisposition::FullyPushed {
			return Ok(None);
		}

		let mut output = Vec::new();
		let mut accumulated_bytes = 0u64;
		let accumulator_cap =
			join_output_materialization_memory_cap(self.context.task_budget.memory.get());
		let matched = self
			.try_visit_streaming_join0_filter_chunks(
				input,
				predicate,
				disposition,
				morsel,
				|batch| {
					accumulated_bytes =
						accumulated_bytes.saturating_add(record_batch_memory_bytes(&batch));
					if accumulated_bytes > accumulator_cap {
						return Err(budget_exhausted(
							BudgetDimension::Memory,
							"streamed join filter accumulator",
							accumulated_bytes,
							accumulator_cap,
						));
					}
					if batch.num_rows() > 0 {
						output.push(batch);
					}
					Ok(())
				},
			)
			.await?;
		if matched.is_none() {
			return Ok(None);
		}
		if output.is_empty() {
			output.push(RecordBatch::new_empty(input.schema()));
		}
		Ok(Some(output))
	}

	async fn try_streaming_join0_filter_exchange(
		&self,
		input: &ExecutionPlan,
		partition_count: u32,
		key: &BoundExprProgram,
		morsel: Option<(MorselId, u32)>,
	) -> Result<Option<Vec<RecordBatch>>, ExecutionError> {
		let ExecutionPlan::Filter {
			input: filter_input,
			predicate,
			disposition,
		} = input
		else {
			return Ok(None);
		};
		if partition_count == 0 {
			return Err(ExecutionError::InvalidExpression {
				message: "exchange partition_count must be greater than zero".to_string(),
			});
		}

		let schema = input.schema();
		let partition_count_usize =
			usize::try_from(partition_count).expect("partition count fits usize");
		let mut partition_batches: Vec<Vec<RecordBatch>> =
			(0..partition_count_usize).map(|_| Vec::new()).collect();
		let accumulator_cap =
			join_output_materialization_memory_cap(self.context.task_budget.memory.get());
		let mut accumulated_bytes = 0u64;
		let matched = self
			.try_visit_streaming_join0_filter_chunks(
				filter_input,
				predicate,
				*disposition,
				morsel,
				|batch| {
					for (partition, partitioned) in
						self.repartition_exchange_batch(&batch, partition_count, key)?
					{
						accumulated_bytes = accumulated_bytes
							.saturating_add(record_batch_memory_bytes(&partitioned));
						if accumulated_bytes > accumulator_cap {
							return Err(budget_exhausted(
								BudgetDimension::Memory,
								"streamed exchange output accumulator",
								accumulated_bytes,
								accumulator_cap,
							));
						}
						partition_batches[partition].push(partitioned);
					}
					Ok(())
				},
			)
			.await?;
		if matched.is_none() {
			return Ok(None);
		}
		Ok(Some(finalize_exchange_partitions(
			schema,
			partition_batches,
		)?))
	}

	async fn try_visit_streaming_join0_filter_chunks<F>(
		&self,
		input: &ExecutionPlan,
		predicate: &BoundExprProgram,
		disposition: FilterDisposition,
		morsel: Option<(MorselId, u32)>,
		mut visit_filtered_batch: F,
	) -> Result<Option<()>, ExecutionError>
	where
		F: FnMut(RecordBatch) -> Result<(), ExecutionError>,
	{
		if disposition == FilterDisposition::FullyPushed {
			return Ok(None);
		}

		let ExecutionPlan::Join {
			replay_id,
			left,
			right,
			kind: JoinKind::Inner,
			left_keys,
			right_keys,
			match_filter,
			null_safe_keys,
			strategy,
			output_schema,
			..
		} = input
		else {
			return Ok(None);
		};
		if *replay_id != JoinReplayId(0) {
			return Ok(None);
		}

		let left_batches = self.execute_bound(left, morsel).await?;
		let right_batches = self.execute_bound(right, morsel).await?;
		let left_batch = concat_or_empty(left.schema(), &left_batches)?;
		let right_batch = concat_or_empty(right.schema(), &right_batches)?;
		let left_key_arrays = key_arrays(&left_batch, left_keys)?;
		let right_key_arrays = key_arrays(&right_batch, right_keys)?;
		if estimated_build_hash_bytes(&left_key_arrays, left_batch.num_rows())
			> self.context.task_budget.memory.get()
		{
			return Ok(None);
		}
		let join_key_plan =
			JoinKeyPlan::from_key_arrays(&left_key_arrays, &right_key_arrays, *null_safe_keys);
		let build_started = Instant::now();
		let Some(build_rows) = build_in_memory_join_rows(
			join_key_plan,
			&left_key_arrays,
			left_batch.num_rows(),
			self.context.task_budget.memory.get(),
			*null_safe_keys,
		)?
		else {
			return Ok(None);
		};
		record_task_telemetry(&self.context.task_telemetry, |task| {
			task.counters.build_rows = task
				.counters
				.build_rows
				.saturating_add(left_batch.num_rows() as u64);
			task.counters.probe_rows = task
				.counters
				.probe_rows
				.saturating_add(right_batch.num_rows() as u64);
		});
		record_task_telemetry(&self.context.task_telemetry, |task| {
			task.phases.build_micros = task
				.phases
				.build_micros
				.saturating_add(duration_micros(build_started));
		});
		let probe_started = Instant::now();
		let selection = probe_in_memory_join_rows(
			JoinKind::Inner,
			join_key_plan,
			&build_rows,
			left_batch.num_rows(),
			&right_key_arrays,
			right_batch.num_rows(),
			match_filter.as_ref(),
			&left_batch,
			&right_batch,
			*null_safe_keys,
		)?;
		record_task_telemetry(&self.context.task_telemetry, |task| {
			task.phases.probe_micros = task
				.phases
				.probe_micros
				.saturating_add(duration_micros(probe_started));
			task.counters.matched_rows = task
				.counters
				.matched_rows
				.saturating_add(selection.len() as u64);
		});
		visit_join_selection_filtered_chunks(
			self,
			JoinKind::Inner,
			&left_batch,
			&right_batch,
			&selection,
			output_schema,
			predicate,
			self.context.task_budget.memory.get(),
			&mut visit_filtered_batch,
		)?;
		record_join_runtime_observation(
			&self.context.task_telemetry,
			&self.context.join_runtime_observation_mode,
			JoinRuntimeObservation {
				replay_id: *replay_id,
				strategy: *strategy,
				build_rows: RowCount::new(left_batch.num_rows() as u64),
				probe_rows: RowCount::new(right_batch.num_rows() as u64),
				output_rows: RowCount::new(selection.len() as u64),
			},
		);
		Ok(Some(()))
	}

	async fn try_streaming_collapsing_global_aggregate(
		&self,
		input: &ExecutionPlan,
		aggr_expr: &[BoundAggregateCall],
		output_schema: &arrow::datatypes::SchemaRef,
		morsel: Option<(MorselId, u32)>,
	) -> Result<Option<Vec<RecordBatch>>, ExecutionError> {
		let (join, filter_predicate) = match input {
			ExecutionPlan::Join {
				kind: JoinKind::Inner,
				..
			} => (input, None),
			ExecutionPlan::Filter {
				input,
				predicate,
				disposition,
			} => {
				let ExecutionPlan::Join {
					kind: JoinKind::Inner,
					..
				} = input.as_ref()
				else {
					return Ok(None);
				};
				let filter_predicate = match disposition {
					FilterDisposition::FullyPushed => None,
					FilterDisposition::EvaluateInToron => Some(predicate),
				};
				(input.as_ref(), filter_predicate)
			}
			_ => return Ok(None),
		};

		let ExecutionPlan::Join {
			replay_id,
			left,
			right,
			kind,
			left_keys,
			right_keys,
			match_filter,
			null_safe_keys,
			strategy,
			output_schema: join_output_schema,
			..
		} = join
		else {
			return Ok(None);
		};

		let left_batches = self.execute_bound(left, morsel).await?;
		let right_batches = self.execute_bound(right, morsel).await?;
		let mut state = init_global_aggregate_state(aggr_expr)?;
		execute_hash_join_into_global_aggregate(
			self,
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
			join_output_schema,
			filter_predicate,
			aggr_expr,
			&mut state,
			self.context.task_budget,
			self.context.task_telemetry.clone(),
			self.context.join_runtime_observation_mode.clone(),
			self.context.join_spill_file_ops.clone(),
			self.context.join_spill_dir.clone(),
		)
		.await?;
		finalize_global_aggregate_state(&state, aggr_expr, output_schema).map(Some)
	}

	async fn try_streaming_reoriented_inner_chain_grouped_aggregate(
		&self,
		input: &ExecutionPlan,
		group_expr: &BoundExprProgram,
		aggr_expr: &[BoundAggregateCall],
		output_schema: &arrow::datatypes::SchemaRef,
		morsel: Option<(MorselId, u32)>,
	) -> Result<Option<Vec<RecordBatch>>, ExecutionError> {
		let ExecutionPlan::Exchange {
			input: aggregate_input,
			..
		} = input
		else {
			return Ok(None);
		};
		let ExecutionPlan::Project {
			input: project_input,
			exprs: project_exprs,
			output_schema: project_output_schema,
		} = aggregate_input.as_ref()
		else {
			return Ok(None);
		};
		let ExecutionPlan::Filter {
			input: filter_input,
			predicate: outer_predicate,
			disposition: FilterDisposition::EvaluateInToron,
		} = project_input.as_ref()
		else {
			return Ok(None);
		};
		let Some((base, stages)) = extract_reoriented_inner_chain(filter_input) else {
			return Ok(None);
		};

		let mut runtimes = Vec::with_capacity(stages.len());
		for stage in stages {
			let Some(runtime) = self
				.prepare_reoriented_inner_join_runtime(stage, morsel)
				.await?
			else {
				return Ok(None);
			};
			runtimes.push(runtime);
		}

		let mut groups: BTreeMap<Vec<GroupValue>, Vec<AggState>> = BTreeMap::new();
		let mut group_types = Vec::new();
		let matched = self
			.try_visit_streaming_reoriented_inner_chain_base(base, morsel, |base_batch| {
				let mut batches = vec![base_batch];
				for runtime in runtimes.iter_mut() {
					let mut next = Vec::new();
					for batch in batches {
						next.extend(self.probe_reoriented_inner_join(runtime, &batch)?);
					}
					batches = next;
					if batches.is_empty() {
						break;
					}
				}

				for batch in batches {
					let filtered = self.filter_batch_with_bound_predicate(
						outer_predicate,
						batch,
						"filter predicate",
					)?;
					if filtered.num_rows() == 0 {
						continue;
					}
					let projected = RecordBatch::try_new(
						project_output_schema.clone(),
						self.evaluate_bound_program(project_exprs, &filtered)?,
					)?;
					self.fold_bound_grouped_aggregate_batch(
						&mut groups,
						&mut group_types,
						group_expr,
						aggr_expr,
						&projected,
					)?;
				}
				Ok(())
			})
			.await?;
		if matched.is_none() {
			return Ok(None);
		}

		for runtime in &runtimes {
			record_join_runtime_observation(
				&self.context.task_telemetry,
				&self.context.join_runtime_observation_mode,
				JoinRuntimeObservation {
					replay_id: runtime.replay_id,
					strategy: runtime.strategy,
					build_rows: RowCount::new(runtime.build_batch.num_rows() as u64),
					probe_rows: RowCount::new(runtime.probe_rows),
					output_rows: RowCount::new(runtime.output_rows),
				},
			);
		}

		self.finalize_bound_grouped_aggregate(&groups, &group_types, aggr_expr, output_schema)
			.map(Some)
	}

	async fn try_streaming_inner_prefix_semi_anti_grouped_aggregate(
		&self,
		input: &ExecutionPlan,
		group_expr: &BoundExprProgram,
		aggr_expr: &[BoundAggregateCall],
		output_schema: &arrow::datatypes::SchemaRef,
		morsel: Option<(MorselId, u32)>,
	) -> Result<Option<Vec<RecordBatch>>, ExecutionError> {
		let ExecutionPlan::Exchange {
			input: aggregate_input,
			..
		} = input
		else {
			return Ok(None);
		};
		let Some(chain) = extract_streaming_inner_prefix_semi_anti_chain(aggregate_input) else {
			return Ok(None);
		};

		let mut inner_runtimes = Vec::with_capacity(chain.inner_stages.len());
		for stage in chain.inner_stages {
			let Some(runtime) = self
				.prepare_reoriented_inner_join_runtime(stage, morsel)
				.await?
			else {
				return Ok(None);
			};
			inner_runtimes.push(runtime);
		}
		let Some(mut semi_runtime) = self
			.prepare_streaming_semi_anti_runtime(chain.semi_stage, morsel)
			.await?
		else {
			return Ok(None);
		};
		let Some(mut anti_runtime) = self
			.prepare_streaming_semi_anti_runtime(chain.anti_stage, morsel)
			.await?
		else {
			return Ok(None);
		};

		let mut groups: BTreeMap<Vec<GroupValue>, Vec<AggState>> = BTreeMap::new();
		let mut group_types = Vec::new();
		let ExecutionPlan::Exchange {
			input: base_input,
			partition_count,
			key: base_exchange_key,
		} = chain.base
		else {
			return Ok(None);
		};
		let ExecutionPlan::Filter {
			input: join0,
			predicate: join0_filter,
			disposition,
		} = base_input.as_ref()
		else {
			return Ok(None);
		};
		if *disposition == FilterDisposition::FullyPushed {
			return Ok(None);
		}
		let ExecutionPlan::Join {
			replay_id,
			left,
			right,
			kind: JoinKind::Inner,
			left_keys,
			right_keys,
			match_filter,
			null_safe_keys,
			strategy,
			output_schema: join0_output_schema,
			..
		} = join0.as_ref()
		else {
			return Ok(None);
		};
		if *replay_id != JoinReplayId(0) {
			return Ok(None);
		}

		let left_batches = self.execute_bound(left, morsel).await?;
		let right_batches = self.execute_bound(right, morsel).await?;
		let left_batch = concat_or_empty(left.schema(), &left_batches)?;
		let right_batch = concat_or_empty(right.schema(), &right_batches)?;
		let left_key_arrays = key_arrays(&left_batch, left_keys)?;
		let right_key_arrays = key_arrays(&right_batch, right_keys)?;
		if estimated_build_hash_bytes(&left_key_arrays, left_batch.num_rows())
			> self.context.task_budget.memory.get()
		{
			return Ok(None);
		}
		let join_key_plan =
			JoinKeyPlan::from_key_arrays(&left_key_arrays, &right_key_arrays, *null_safe_keys);
		let build_started = Instant::now();
		let Some(build_rows) = build_in_memory_join_rows(
			join_key_plan,
			&left_key_arrays,
			left_batch.num_rows(),
			self.context.task_budget.memory.get(),
			*null_safe_keys,
		)?
		else {
			return Ok(None);
		};
		record_task_telemetry(&self.context.task_telemetry, |task| {
			task.counters.build_rows = task
				.counters
				.build_rows
				.saturating_add(left_batch.num_rows() as u64);
			task.counters.probe_rows = task
				.counters
				.probe_rows
				.saturating_add(right_batch.num_rows() as u64);
		});
		record_task_telemetry(&self.context.task_telemetry, |task| {
			task.phases.build_micros = task
				.phases
				.build_micros
				.saturating_add(duration_micros(build_started));
		});
		let probe_started = Instant::now();
		let selection = probe_in_memory_join_rows(
			JoinKind::Inner,
			join_key_plan,
			&build_rows,
			left_batch.num_rows(),
			&right_key_arrays,
			right_batch.num_rows(),
			match_filter.as_ref(),
			&left_batch,
			&right_batch,
			*null_safe_keys,
		)?;
		record_task_telemetry(&self.context.task_telemetry, |task| {
			task.phases.probe_micros = task
				.phases
				.probe_micros
				.saturating_add(duration_micros(probe_started));
			task.counters.matched_rows = task
				.counters
				.matched_rows
				.saturating_add(selection.len() as u64);
		});

		for chunk in selection.chunks(CROSS_JOIN_OUTPUT_CHUNK_ROWS) {
			let batch = materialize_join_selection(
				JoinKind::Inner,
				&left_batch,
				&right_batch,
				chunk,
				join0_output_schema,
				self.context.task_budget.memory.get(),
			)?;
			let batch =
				self.filter_batch_with_bound_predicate(join0_filter, batch, "filter predicate")?;
			if batch.num_rows() == 0 {
				continue;
			}
			for (_partition, partitioned) in
				self.repartition_exchange_batch(&batch, *partition_count, base_exchange_key)?
			{
				if partitioned.num_rows() == 0 {
					continue;
				}
				let mut batches = vec![partitioned];
				for runtime in inner_runtimes.iter_mut() {
					let mut next = Vec::new();
					for batch in batches {
						next.extend(self.probe_reoriented_inner_join(runtime, &batch)?);
					}
					batches = next;
					if let Some(predicate) = runtime.post_filter {
						let mut filtered = Vec::new();
						for batch in batches {
							let batch = self.filter_batch_with_bound_predicate(
								predicate,
								batch,
								"filter predicate",
							)?;
							if batch.num_rows() > 0 {
								filtered.push(batch);
							}
						}
						batches = filtered;
					}
					if batches.is_empty() {
						break;
					}
				}

				let mut semi_batches = Vec::new();
				for batch in batches {
					semi_batches.extend(
						self.probe_streaming_semi_anti(&mut semi_runtime, &batch)
							.await?,
					);
				}
				let mut anti_batches = Vec::new();
				for batch in semi_batches {
					anti_batches.extend(
						self.probe_streaming_semi_anti(&mut anti_runtime, &batch)
							.await?,
					);
				}
				for batch in anti_batches {
					self.fold_bound_grouped_aggregate_batch(
						&mut groups,
						&mut group_types,
						group_expr,
						aggr_expr,
						&batch,
					)?;
				}
			}
		}
		record_join_runtime_observation(
			&self.context.task_telemetry,
			&self.context.join_runtime_observation_mode,
			JoinRuntimeObservation {
				replay_id: *replay_id,
				strategy: *strategy,
				build_rows: RowCount::new(left_batch.num_rows() as u64),
				probe_rows: RowCount::new(right_batch.num_rows() as u64),
				output_rows: RowCount::new(selection.len() as u64),
			},
		);

		for runtime in &inner_runtimes {
			record_join_runtime_observation(
				&self.context.task_telemetry,
				&self.context.join_runtime_observation_mode,
				JoinRuntimeObservation {
					replay_id: runtime.replay_id,
					strategy: runtime.strategy,
					build_rows: RowCount::new(runtime.build_batch.num_rows() as u64),
					probe_rows: RowCount::new(runtime.probe_rows),
					output_rows: RowCount::new(runtime.output_rows),
				},
			);
		}
		for runtime in [&semi_runtime, &anti_runtime] {
			record_join_runtime_observation(
				&self.context.task_telemetry,
				&self.context.join_runtime_observation_mode,
				JoinRuntimeObservation {
					replay_id: runtime.replay_id,
					strategy: runtime.strategy,
					build_rows: RowCount::new(runtime.existence_row_count),
					probe_rows: RowCount::new(runtime.probe_rows),
					output_rows: RowCount::new(runtime.output_rows),
				},
			);
		}

		let output =
			self.finalize_bound_grouped_aggregate(&groups, &group_types, aggr_expr, output_schema);
		semi_runtime.cleanup().await;
		anti_runtime.cleanup().await;
		output.map(Some)
	}

	async fn prepare_reoriented_inner_join_runtime<'a>(
		&self,
		stage: ReorientedInnerJoinStage<'a>,
		morsel: Option<(MorselId, u32)>,
	) -> Result<Option<ReorientedInnerJoinRuntime<'a>>, ExecutionError> {
		let build_batches = self.execute_bound(stage.build, morsel).await?;
		let build_batch = concat_or_empty(stage.build.schema(), &build_batches)?;
		let build_key_arrays = key_arrays(&build_batch, &stage.build_keys)?;
		let build_memory_cap =
			join_output_materialization_memory_cap(self.context.task_budget.memory.get());
		let key_plan = JoinKeyPlan::from_build_arrays_and_probe_schema(
			&build_key_arrays,
			&stage.probe_schema,
			&stage.probe_keys,
			false,
		)?;
		let estimated_build_bytes =
			estimated_build_hash_bytes(&build_key_arrays, build_batch.num_rows());
		if estimated_build_bytes > build_memory_cap {
			return Err(budget_exhausted(
				BudgetDimension::Memory,
				"reoriented join build accumulator",
				estimated_build_bytes,
				build_memory_cap,
			));
		}
		let Some(build_rows) = build_in_memory_join_rows(
			key_plan,
			&build_key_arrays,
			build_batch.num_rows(),
			build_memory_cap,
			false,
		)?
		else {
			return Err(budget_exhausted(
				BudgetDimension::Memory,
				"reoriented join build accumulator",
				estimated_build_bytes,
				build_memory_cap,
			));
		};
		record_task_telemetry(&self.context.task_telemetry, |task| {
			task.counters.build_rows = task
				.counters
				.build_rows
				.saturating_add(build_batch.num_rows() as u64);
		});
		Ok(Some(ReorientedInnerJoinRuntime {
			replay_id: stage.replay_id,
			strategy: stage.strategy,
			build_batch,
			build_rows,
			key_plan,
			probe_keys: stage.probe_keys,
			output_schema: stage.output_schema,
			post_filter: stage.post_filter,
			probe_rows: 0,
			output_rows: 0,
		}))
	}

	async fn prepare_streaming_semi_anti_runtime<'a>(
		&self,
		stage: StreamingSemiAntiStage<'a>,
		morsel: Option<(MorselId, u32)>,
	) -> Result<Option<StreamingSemiAntiRuntime<'a>>, ExecutionError> {
		debug_assert!(matches!(
			stage.kind,
			JoinKind::LeftSemi | JoinKind::LeftAnti
		));
		let key_plan = JoinKeyPlan::from_build_schema_and_probe_schema(
			&stage.existence.schema(),
			&stage.existence_keys,
			&stage.output_schema,
			&stage.preserved_keys,
			false,
		)?;
		let mut spilled = SpilledSemiAntiExistenceBuild::create(
			self.context.task_budget,
			self.context.join_spill_file_ops.clone(),
			self.context.join_spill_dir.clone(),
		)
		.await?;
		let Some(()) = self
			.write_streaming_semi_anti_existence(
				stage.existence,
				&stage.existence_keys,
				key_plan,
				morsel,
				&mut spilled,
			)
			.await?
		else {
			spilled.cleanup().await;
			return Ok(None);
		};
		spilled.finish(self.context.task_budget).await?;
		let existence_row_count = spilled.input_row_count();
		record_task_telemetry(&self.context.task_telemetry, |task| {
			task.counters.build_rows = task.counters.build_rows.saturating_add(existence_row_count);
			task.counters.spill_bytes = task
				.counters
				.spill_bytes
				.saturating_add(spilled.spill_bytes());
		});
		Ok(Some(StreamingSemiAntiRuntime {
			replay_id: stage.replay_id,
			kind: stage.kind,
			strategy: stage.strategy,
			existence: StreamingSemiAntiExistence::Spilled(spilled),
			existence_row_count,
			key_plan,
			preserved_keys: stage.preserved_keys,
			match_filter: stage.match_filter,
			output_schema: stage.output_schema,
			probe_rows: 0,
			output_rows: 0,
		}))
	}

	async fn write_streaming_semi_anti_existence(
		&self,
		plan: &ExecutionPlan,
		existence_keys: &[crate::ir::ColumnOrdinal],
		key_plan: JoinKeyPlan,
		morsel: Option<(MorselId, u32)>,
		spilled: &mut SpilledSemiAntiExistenceBuild,
	) -> Result<Option<()>, ExecutionError> {
		let Some((scan_plan, ops)) = decompose_streaming_existence_plan(plan) else {
			return Ok(None);
		};
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
		} = scan_plan
		else {
			return Ok(None);
		};
		let req = ScanRequest {
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
		};
		let span = crate::telemetry::storage_scan_span(&req);
		let stream = self.storage.scan(req).instrument(span).await?;
		let mut stream = stream.into_batches();
		let mut remaining = *row_limit;
		while let Some(batch) = stream.next().await {
			let mut batch = batch?;
			if let Some(limit) = remaining.as_mut() {
				if *limit == 0 {
					break;
				}
				if batch.num_rows() > *limit {
					batch = batch.slice(0, *limit);
				}
				*limit = (*limit).saturating_sub(batch.num_rows());
			}
			let mut batches = vec![batch];
			for op in &ops {
				let mut next = Vec::new();
				for batch in batches {
					match op {
						StreamingExistenceOp::Filter(predicate) => {
							let batch = self.filter_batch_with_bound_predicate(
								predicate,
								batch,
								"filter predicate",
							)?;
							if batch.num_rows() > 0 {
								next.push(batch);
							}
						}
						StreamingExistenceOp::Project {
							exprs,
							output_schema,
						} => {
							let columns = self.evaluate_bound_program(exprs, &batch)?;
							next.push(RecordBatch::try_new(output_schema.clone(), columns)?);
						}
						StreamingExistenceOp::Exchange {
							partition_count,
							key,
						} => {
							if *partition_count == 0 {
								return Err(ExecutionError::InvalidExpression {
									message: "exchange partition_count must be greater than zero"
										.to_string(),
								});
							}
							for (_partition, partitioned) in
								self.repartition_exchange_batch(&batch, *partition_count, key)?
							{
								if partitioned.num_rows() > 0 {
									next.push(partitioned);
								}
							}
						}
					}
				}
				batches = next;
				if batches.is_empty() {
					break;
				}
			}
			for batch in batches {
				spilled
					.write_batch_chunks(&batch, key_plan, existence_keys, self.context.task_budget)
					.await?;
			}
		}
		Ok(Some(()))
	}

	async fn try_visit_streaming_reoriented_inner_chain_base<F>(
		&self,
		base: &ExecutionPlan,
		morsel: Option<(MorselId, u32)>,
		mut visit_batch: F,
	) -> Result<Option<()>, ExecutionError>
	where
		F: FnMut(RecordBatch) -> Result<(), ExecutionError>,
	{
		let ExecutionPlan::Exchange {
			input,
			partition_count,
			key,
		} = base
		else {
			return Ok(None);
		};
		let ExecutionPlan::Filter {
			input: filter_input,
			predicate,
			disposition,
		} = input.as_ref()
		else {
			return Ok(None);
		};

		let matched = self
			.try_visit_streaming_join0_filter_chunks(
				filter_input,
				predicate,
				*disposition,
				morsel,
				|batch| {
					for (_partition, partitioned) in
						self.repartition_exchange_batch(&batch, *partition_count, key)?
					{
						if partitioned.num_rows() > 0 {
							visit_batch(partitioned)?;
						}
					}
					Ok(())
				},
			)
			.await?;
		if matched.is_none() {
			return Ok(None);
		}
		Ok(Some(()))
	}

	async fn probe_streaming_semi_anti(
		&self,
		runtime: &mut StreamingSemiAntiRuntime<'_>,
		preserved_batch: &RecordBatch,
	) -> Result<Vec<RecordBatch>, ExecutionError> {
		if preserved_batch.num_rows() == 0 {
			return Ok(Vec::new());
		}
		let preserved_key_arrays = key_arrays(preserved_batch, &runtime.preserved_keys)?;
		runtime.probe_rows = runtime
			.probe_rows
			.saturating_add(preserved_batch.num_rows() as u64);
		record_task_telemetry(&self.context.task_telemetry, |task| {
			task.counters.probe_rows = task
				.counters
				.probe_rows
				.saturating_add(preserved_batch.num_rows() as u64);
		});

		let mut preserved_by_key: HashMap<JoinKey, Vec<u32>> = HashMap::new();
		for row in 0..preserved_batch.num_rows() {
			if let Some(key) = join_key_at(runtime.key_plan, &preserved_key_arrays, row, false)? {
				preserved_by_key
					.entry(key)
					.or_default()
					.push(join_probe_row_index(row)?);
			}
		}
		let matched_rows = match &mut runtime.existence {
			StreamingSemiAntiExistence::InMemory { batch, rows } => {
				matching_preserved_rows_in_memory(
					preserved_batch,
					&preserved_by_key,
					runtime.match_filter,
					batch,
					rows,
				)?
			}
			StreamingSemiAntiExistence::Spilled(spilled) => {
				spilled
					.matching_preserved_rows(
						preserved_batch,
						&preserved_by_key,
						runtime.match_filter,
					)
					.await?
			}
		};
		let mut selected = Vec::<u32>::new();
		for row in 0..preserved_batch.num_rows() {
			let matched = matched_rows[row];
			if (runtime.kind == JoinKind::LeftSemi && matched)
				|| (runtime.kind == JoinKind::LeftAnti && !matched)
			{
				selected.push(join_probe_row_index(row)?);
			}
		}
		runtime.output_rows = runtime.output_rows.saturating_add(selected.len() as u64);
		record_task_telemetry(&self.context.task_telemetry, |task| {
			task.counters.matched_rows = task
				.counters
				.matched_rows
				.saturating_add(selected.len() as u64);
		});

		let mut output = Vec::new();
		for chunk in selected.chunks(CROSS_JOIN_OUTPUT_CHUNK_ROWS) {
			let batch = materialize_preserved_selection(
				preserved_batch,
				chunk,
				&runtime.output_schema,
				self.context.task_budget.memory.get(),
			)?;
			if batch.num_rows() > 0 {
				output.push(batch);
			}
		}
		Ok(output)
	}

	fn probe_reoriented_inner_join(
		&self,
		runtime: &mut ReorientedInnerJoinRuntime<'_>,
		probe_batch: &RecordBatch,
	) -> Result<Vec<RecordBatch>, ExecutionError> {
		if probe_batch.num_rows() == 0 {
			return Ok(Vec::new());
		}
		let probe_key_arrays = key_arrays(probe_batch, &runtime.probe_keys)?;
		runtime.probe_rows = runtime
			.probe_rows
			.saturating_add(probe_batch.num_rows() as u64);
		record_task_telemetry(&self.context.task_telemetry, |task| {
			task.counters.probe_rows = task
				.counters
				.probe_rows
				.saturating_add(probe_batch.num_rows() as u64);
		});
		let mut selection = probe_in_memory_join_rows(
			JoinKind::Inner,
			runtime.key_plan,
			&runtime.build_rows,
			runtime.build_batch.num_rows(),
			&probe_key_arrays,
			probe_batch.num_rows(),
			None,
			&runtime.build_batch,
			probe_batch,
			false,
		)?;
		runtime.output_rows = runtime.output_rows.saturating_add(selection.len() as u64);
		record_task_telemetry(&self.context.task_telemetry, |task| {
			task.counters.matched_rows = task
				.counters
				.matched_rows
				.saturating_add(selection.len() as u64);
		});
		let mut output = Vec::new();
		for chunk in selection.chunks_mut(CROSS_JOIN_OUTPUT_CHUNK_ROWS) {
			let batch = materialize_reoriented_inner_selection(
				probe_batch,
				&runtime.build_batch,
				chunk,
				&runtime.output_schema,
				self.context.task_budget.memory.get(),
			)?;
			if batch.num_rows() > 0 {
				output.push(batch);
			}
		}
		Ok(output)
	}

	#[cfg(test)]
	fn cast_to_f64(&self, arr: &ArrayRef) -> Result<Float64Array, ExecutionError> {
		if let Some(f64_arr) = arr.as_any().downcast_ref::<Float64Array>() {
			return Ok(f64_arr.clone());
		}
		if let Some(int_arr) = arr.as_any().downcast_ref::<Int32Array>() {
			let values: Vec<Option<f64>> = int_arr.iter().map(|v| v.map(|v| v as f64)).collect();
			return Ok(Float64Array::from(values));
		}
		Err(ExecutionError::InvalidExpression {
			message: format!("cannot cast to f64: {:?}", arr.data_type()),
		})
	}

	#[cfg(test)]
	fn grouped_aggregate(
		&self,
		input_batches: &[RecordBatch],
		group_expr: &[Expr],
		aggr_expr: &[(AggregateFunction, Vec<Expr>)],
	) -> Result<Vec<RecordBatch>, ExecutionError> {
		let mut groups: BTreeMap<Vec<GroupValue>, Vec<AggState>> = BTreeMap::new();
		let mut group_types = Vec::new();
		let mut aggregate_output_types = vec![None; aggr_expr.len()];

		for batch in input_batches {
			let group_arrays = group_expr
				.iter()
				.map(|expr| self.evaluate_expr(expr, batch))
				.collect::<Result<Vec<_>, _>>()?;
			if group_types.is_empty() {
				group_types = group_arrays
					.iter()
					.map(|array| array.data_type().clone())
					.collect();
			}

			let aggr_arrays = aggr_expr
				.iter()
				.map(|(function, args)| match function {
					AggregateFunction::CountAll => Ok(None),
					AggregateFunction::Count
					| AggregateFunction::Sum
					| AggregateFunction::Min
					| AggregateFunction::Max
					| AggregateFunction::Avg => Ok(Some(self.evaluate_expr(&args[0], batch)?)),
					AggregateFunction::LinearRegress | AggregateFunction::ApproxQuantile(_) => {
						Err(ExecutionError::NotImplemented {
							feature: "grouped experimental aggregate",
						})
					}
				})
				.collect::<Result<Vec<_>, _>>()?;
			for (agg_idx, ((function, _), input)) in
				aggr_expr.iter().zip(aggr_arrays.iter()).enumerate()
			{
				if aggregate_output_types[agg_idx].is_none()
					&& let Some(array) = input
				{
					aggregate_output_types[agg_idx] =
						Some(legacy_aggregate_output_type(function, array.data_type()));
				}
			}

			for row in 0..batch.num_rows() {
				let key = group_arrays
					.iter()
					.map(|array| group_value(array, row))
					.collect::<Result<Vec<_>, _>>()?;
				let states = match groups.entry(key) {
					std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
					std::collections::btree_map::Entry::Vacant(entry) => {
						entry.insert(init_states(aggr_expr, &aggr_arrays)?)
					}
				};
				for (state, input) in states.iter_mut().zip(aggr_arrays.iter()) {
					match input {
						None => {
							if let AggState::Count(total) = state {
								*total += 1;
							}
						}
						Some(array) => match state {
							AggState::Count(total) => {
								if !array.is_null(row) {
									*total += 1;
								}
							}
							AggState::SumInt { .. } | AggState::SumFloat { .. } => {
								if let Some(value) = numeric_value(array, row)? {
									add_sum_value(state, value)?;
								}
							}
							AggState::AvgInt { .. }
							| AggState::AvgFloat { .. }
							| AggState::AvgDecimal { .. } => {
								if let Some(value) = numeric_value(array, row)? {
									add_avg_value(state, value)?;
								}
							}
							AggState::MinInt32 { .. }
							| AggState::MinInt64 { .. }
							| AggState::MinFloat32 { .. }
							| AggState::MinFloat64 { .. }
							| AggState::MinDecimal128 { .. } => {
								update_min_state(state, array, row)?;
							}
							AggState::MaxInt32 { .. }
							| AggState::MaxInt64 { .. }
							| AggState::MaxFloat32 { .. }
							| AggState::MaxFloat64 { .. }
							| AggState::MaxDecimal128 { .. } => {
								update_max_state(state, array, row)?;
							}
						},
					}
				}
			}
		}

		let keys = groups.keys().cloned().collect::<Vec<_>>();
		let mut columns = Vec::new();
		let mut fields = Vec::new();

		for (idx, data_type) in group_types.iter().enumerate() {
			columns.push(group_column(&keys, idx, data_type)?);
			fields.push(arrow::datatypes::Field::new(
				format!("group_{idx}"),
				data_type.clone(),
				true,
			));
		}

		for (agg_idx, (function, _)) in aggr_expr.iter().enumerate() {
			match function {
				AggregateFunction::CountAll | AggregateFunction::Count => {
					let values = groups
						.values()
						.map(|states| match states[agg_idx] {
							AggState::Count(value) => value,
							AggState::SumInt { .. }
							| AggState::SumFloat { .. }
							| AggState::MinInt32 { .. }
							| AggState::MinInt64 { .. }
							| AggState::MinFloat32 { .. }
							| AggState::MinFloat64 { .. }
							| AggState::MinDecimal128 { .. }
							| AggState::MaxInt32 { .. }
							| AggState::MaxInt64 { .. }
							| AggState::MaxFloat32 { .. }
							| AggState::MaxFloat64 { .. }
							| AggState::MaxDecimal128 { .. }
							| AggState::AvgInt { .. }
							| AggState::AvgFloat { .. }
							| AggState::AvgDecimal { .. } => 0,
						})
						.collect::<Vec<_>>();
					columns.push(Arc::new(Int64Array::from(values)) as ArrayRef);
				}
				AggregateFunction::Sum => {
					let output_type = aggregate_output_types[agg_idx]
						.clone()
						.unwrap_or(DataType::Int64);
					columns.push(sum_column_from_groups(&groups, agg_idx, &output_type)?);
				}
				AggregateFunction::Min | AggregateFunction::Max => {
					let output_type = aggregate_output_types[agg_idx]
						.clone()
						.unwrap_or(DataType::Int64);
					columns.push(min_max_column_from_groups(&groups, agg_idx, &output_type)?);
				}
				AggregateFunction::Avg => {
					let output_type = aggregate_output_types[agg_idx]
						.clone()
						.unwrap_or(DataType::Float64);
					columns.push(avg_column_from_groups(&groups, agg_idx, &output_type)?);
				}
				AggregateFunction::LinearRegress | AggregateFunction::ApproxQuantile(_) => {
					return Err(ExecutionError::NotImplemented {
						feature: "grouped experimental aggregate",
					});
				}
			}
			let field_name = aggregate_state_output_name(function);
			let field_type = aggregate_output_types[agg_idx]
				.clone()
				.unwrap_or(match function {
					AggregateFunction::Avg => arrow::datatypes::DataType::Float64,
					_ => arrow::datatypes::DataType::Int64,
				});
			fields.push(arrow::datatypes::Field::new(
				field_name,
				field_type,
				!matches!(
					function,
					AggregateFunction::CountAll | AggregateFunction::Count
				),
			));
		}

		let schema = Arc::new(arrow::datatypes::Schema::new(fields));
		Ok(vec![RecordBatch::try_new(schema, columns)?])
	}

	fn bound_grouped_aggregate(
		&self,
		input_batches: &[RecordBatch],
		group_expr: &BoundExprProgram,
		aggr_expr: &[BoundAggregateCall],
		output_schema: &arrow::datatypes::SchemaRef,
	) -> Result<Vec<RecordBatch>, ExecutionError> {
		let mut groups: BTreeMap<Vec<GroupValue>, Vec<AggState>> = BTreeMap::new();
		let mut group_types = Vec::new();

		for batch in input_batches {
			self.fold_bound_grouped_aggregate_batch(
				&mut groups,
				&mut group_types,
				group_expr,
				aggr_expr,
				batch,
			)?;
		}

		self.finalize_bound_grouped_aggregate(&groups, &group_types, aggr_expr, output_schema)
	}

	fn fold_bound_grouped_aggregate_batch(
		&self,
		groups: &mut BTreeMap<Vec<GroupValue>, Vec<AggState>>,
		group_types: &mut Vec<DataType>,
		group_expr: &BoundExprProgram,
		aggr_expr: &[BoundAggregateCall],
		batch: &RecordBatch,
	) -> Result<(), ExecutionError> {
		let group_arrays = self.evaluate_bound_program(group_expr, batch)?;
		if group_types.is_empty() {
			*group_types = group_arrays
				.iter()
				.map(|array| array.data_type().clone())
				.collect();
		}

		let aggr_arrays = aggr_expr
			.iter()
			.map(|call| match call.function {
				AggregateFunction::CountAll => Ok(None),
				AggregateFunction::Count
				| AggregateFunction::Sum
				| AggregateFunction::Min
				| AggregateFunction::Max
				| AggregateFunction::Avg => Ok(Some(self.evaluate_bound_single_root(
					&call.args,
					batch,
					"aggregate arg",
				)?)),
				AggregateFunction::LinearRegress | AggregateFunction::ApproxQuantile(_) => {
					Err(ExecutionError::NotImplemented {
						feature: "bound grouped experimental aggregate",
					})
				}
			})
			.collect::<Result<Vec<_>, _>>()?;

		for row in 0..batch.num_rows() {
			let key = group_arrays
				.iter()
				.map(|array| group_value(array, row))
				.collect::<Result<Vec<_>, _>>()?;
			let states = match groups.entry(key) {
				std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
				std::collections::btree_map::Entry::Vacant(entry) => {
					entry.insert(init_bound_states(aggr_expr)?)
				}
			};
			for (state, input) in states.iter_mut().zip(aggr_arrays.iter()) {
				match input {
					None => {
						if let AggState::Count(total) = state {
							*total += 1;
						}
					}
					Some(array) => match state {
						AggState::Count(total) => {
							if !array.is_null(row) {
								*total += 1;
							}
						}
						AggState::SumInt { .. } | AggState::SumFloat { .. } => {
							if let Some(value) = numeric_value(array, row)? {
								add_sum_value(state, value)?;
							}
						}
						AggState::AvgInt { .. }
						| AggState::AvgFloat { .. }
						| AggState::AvgDecimal { .. } => {
							if let Some(value) = numeric_value(array, row)? {
								add_avg_value(state, value)?;
							}
						}
						AggState::MinInt32 { .. }
						| AggState::MinInt64 { .. }
						| AggState::MinFloat32 { .. }
						| AggState::MinFloat64 { .. }
						| AggState::MinDecimal128 { .. } => {
							update_min_state(state, array, row)?;
						}
						AggState::MaxInt32 { .. }
						| AggState::MaxInt64 { .. }
						| AggState::MaxFloat32 { .. }
						| AggState::MaxFloat64 { .. }
						| AggState::MaxDecimal128 { .. } => {
							update_max_state(state, array, row)?;
						}
					},
				}
			}
		}

		Ok(())
	}

	fn finalize_bound_grouped_aggregate(
		&self,
		groups: &BTreeMap<Vec<GroupValue>, Vec<AggState>>,
		group_types: &[DataType],
		aggr_expr: &[BoundAggregateCall],
		output_schema: &arrow::datatypes::SchemaRef,
	) -> Result<Vec<RecordBatch>, ExecutionError> {
		let keys = groups.keys().cloned().collect::<Vec<_>>();
		let mut columns = Vec::new();
		for (idx, data_type) in group_types.iter().enumerate() {
			columns.push(group_column(&keys, idx, data_type)?);
		}

		for (agg_idx, call) in aggr_expr.iter().enumerate() {
			match call.function {
				AggregateFunction::CountAll | AggregateFunction::Count => {
					let values = groups
						.values()
						.map(|states| match states[agg_idx] {
							AggState::Count(value) => value,
							AggState::SumInt { .. }
							| AggState::SumFloat { .. }
							| AggState::MinInt32 { .. }
							| AggState::MinInt64 { .. }
							| AggState::MinFloat32 { .. }
							| AggState::MinFloat64 { .. }
							| AggState::MinDecimal128 { .. }
							| AggState::MaxInt32 { .. }
							| AggState::MaxInt64 { .. }
							| AggState::MaxFloat32 { .. }
							| AggState::MaxFloat64 { .. }
							| AggState::MaxDecimal128 { .. }
							| AggState::AvgInt { .. }
							| AggState::AvgFloat { .. }
							| AggState::AvgDecimal { .. } => 0,
						})
						.collect::<Vec<_>>();
					columns.push(Arc::new(Int64Array::from(values)) as ArrayRef);
				}
				AggregateFunction::Sum => {
					let output_type = output_schema.field(columns.len()).data_type().clone();
					columns.push(sum_column_from_groups(&groups, agg_idx, &output_type)?);
				}
				AggregateFunction::Min | AggregateFunction::Max => {
					let output_type = output_schema.field(columns.len()).data_type().clone();
					columns.push(min_max_column_from_groups(&groups, agg_idx, &output_type)?);
				}
				AggregateFunction::Avg => {
					let output_type = output_schema.field(columns.len()).data_type().clone();
					columns.push(avg_column_from_groups(&groups, agg_idx, &output_type)?);
				}
				AggregateFunction::LinearRegress | AggregateFunction::ApproxQuantile(_) => {
					return Err(ExecutionError::NotImplemented {
						feature: "bound grouped experimental aggregate",
					});
				}
			}
		}

		Ok(vec![RecordBatch::try_new(output_schema.clone(), columns)?])
	}

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
		for row in 0..num_rows {
			let bucket = partition_bucket(&key_arrays, row, partition_count)?;
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

fn matching_preserved_rows_in_memory(
	preserved_batch: &RecordBatch,
	preserved_by_key: &HashMap<JoinKey, Vec<u32>>,
	match_filter: &BoundExprProgram,
	existence_batch: &RecordBatch,
	existence_rows: &HashMap<JoinKey, BuildRowList>,
) -> Result<Vec<bool>, ExecutionError> {
	let mut matched = vec![false; preserved_batch.num_rows()];
	let mut residual = JoinMatchResidual::new(Some(match_filter), preserved_batch, existence_batch);
	for (key, preserved_rows) in preserved_by_key {
		let Some(matches) = existence_rows.get(key) else {
			continue;
		};
		for preserved_row in preserved_rows {
			if matched[*preserved_row as usize] {
				continue;
			}
			for existence_row in matches.rows() {
				if residual.matches(*preserved_row, existence_row)? {
					matched[*preserved_row as usize] = true;
					break;
				}
			}
		}
	}
	Ok(matched)
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

fn evaluated_child(
	outputs: &[Option<ArrayRef>],
	id: BoundExprId,
) -> Result<ArrayRef, ExecutionError> {
	outputs
		.get(id.0 as usize)
		.and_then(|array| array.as_ref())
		.cloned()
		.ok_or_else(|| ExecutionError::InvalidExpression {
			message: format!("bound expression child {:?} has not been evaluated", id),
		})
}

fn literal_array(value: &ScalarValue, num_rows: usize) -> Result<ArrayRef, ExecutionError> {
	Ok(match value {
		ScalarValue::Boolean(value) => Arc::new(BooleanArray::from(vec![*value; num_rows])),
		ScalarValue::Int32(value) => Arc::new(Int32Array::from(vec![*value; num_rows])),
		ScalarValue::Int64(value) => Arc::new(Int64Array::from(vec![*value; num_rows])),
		ScalarValue::Date32(value) => Arc::new(Date32Array::from(vec![*value; num_rows])),
		ScalarValue::Timestamp { value, unit } => timestamp_literal_array(value, *unit, num_rows),
		ScalarValue::Interval(value) => {
			Arc::new(DurationNanosecondArray::from(vec![*value; num_rows]))
		}
		ScalarValue::CalendarMonthInterval(value) => {
			Arc::new(IntervalYearMonthArray::from(vec![*value; num_rows]))
		}
		ScalarValue::Float32(value) => Arc::new(Float32Array::from(vec![*value; num_rows])),
		ScalarValue::Float64(value) => Arc::new(Float64Array::from(vec![*value; num_rows])),
		ScalarValue::Decimal128 {
			value,
			precision,
			scale,
		} => Arc::new(
			Decimal128Array::from(vec![*value; num_rows])
				.with_precision_and_scale(*precision, *scale)
				.map_err(|error| ExecutionError::Kernel {
					message: error.to_string(),
				})?,
		),
		ScalarValue::Utf8(value) => {
			Arc::new(StringArray::from(vec![value.clone().as_deref(); num_rows]))
		}
	})
}

fn timestamp_literal_array(value: &Option<i64>, unit: TimeUnit, num_rows: usize) -> ArrayRef {
	match unit {
		TimeUnit::Millisecond => {
			Arc::new(TimestampMillisecondArray::from(vec![*value; num_rows]).with_timezone("UTC"))
		}
		TimeUnit::Microsecond => {
			Arc::new(TimestampMicrosecondArray::from(vec![*value; num_rows]).with_timezone("UTC"))
		}
		TimeUnit::Nanosecond => {
			Arc::new(TimestampNanosecondArray::from(vec![*value; num_rows]).with_timezone("UTC"))
		}
		TimeUnit::Second => unreachable!("TimestampSecond is not a supported Toron timestamp unit"),
	}
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

fn evaluate_bound_binary(
	program: &BoundExprProgram,
	op: &BoundBinaryOp,
	left: BoundExprId,
	left_arr: &ArrayRef,
	right_arr: &ArrayRef,
) -> Result<ArrayRef, ExecutionError> {
	match op {
		BoundBinaryOp::Add | BoundBinaryOp::Sub | BoundBinaryOp::Mul | BoundBinaryOp::Div => {
			let arithmetic_op = match op {
				BoundBinaryOp::Add => ArithmeticOp::Add,
				BoundBinaryOp::Sub => ArithmeticOp::Sub,
				BoundBinaryOp::Mul => ArithmeticOp::Mul,
				BoundBinaryOp::Div => ArithmeticOp::Div,
				_ => unreachable!("arithmetic op matched"),
			};
			if let Some(result) = evaluate_temporal_arithmetic(arithmetic_op, left_arr, right_arr)?
			{
				return Ok(result);
			}
			// ADR 263: Decimal128 * Decimal128 — Arrow's numeric `mul` widens to
			// `Decimal128(p1+p2+1, s1+s2)`, matching the binder's bound output type (the operands keep
			// their own (p,s), so this bypasses `evaluate_arithmetic`'s operands-equal-output path). Q6
			// slice: only `*`.
			if matches!(arithmetic_op, ArithmeticOp::Mul)
				&& matches!(left_arr.data_type(), DataType::Decimal128(_, _))
				&& matches!(right_arr.data_type(), DataType::Decimal128(_, _))
			{
				return Ok(arrow::compute::kernels::numeric::mul(left_arr, right_arr)?);
			}
			// ADR 266: Decimal128 +/- Decimal128 — Arrow's numeric `add`/`sub` carry the scale and add one
			// precision digit, matching the binder's same-(p,s) bound output type (operands keep their
			// (p,s), bypassing `evaluate_arithmetic`'s operands-equal-output path). Q1 slice: only `+`/`-`.
			if matches!(arithmetic_op, ArithmeticOp::Add | ArithmeticOp::Sub)
				&& matches!(left_arr.data_type(), DataType::Decimal128(_, _))
				&& matches!(right_arr.data_type(), DataType::Decimal128(_, _))
			{
				return Ok(match arithmetic_op {
					ArithmeticOp::Add => {
						arrow::compute::kernels::numeric::add(left_arr, right_arr)?
					}
					ArithmeticOp::Sub => {
						arrow::compute::kernels::numeric::sub(left_arr, right_arr)?
					}
					_ => unreachable!("add/sub matched"),
				});
			}
			// ADR 271: Decimal128 / Decimal128 — a custom Toron i128 divide (NOT Arrow `numeric::div`, which
			// errors on a zero divisor). div-by-zero -> NULL (ADR 084), half-away-from-zero rounding (ADR
			// 268), stamped with the shared validated output (p,s).
			if matches!(arithmetic_op, ArithmeticOp::Div)
				&& matches!(left_arr.data_type(), DataType::Decimal128(_, _))
				&& matches!(right_arr.data_type(), DataType::Decimal128(_, _))
			{
				return evaluate_decimal_division(left_arr, right_arr);
			}
			let left_type =
				program
					.node_type(left)
					.ok_or_else(|| ExecutionError::InvalidExpression {
						message: format!("missing type for bound expression {:?}", left),
					})?;
			evaluate_arithmetic(arithmetic_op, left_arr, right_arr, left_type)
		}
		BoundBinaryOp::Eq => Ok(Arc::new(eq(left_arr, right_arr)?)),
		BoundBinaryOp::Neq => Ok(Arc::new(neq(left_arr, right_arr)?)),
		BoundBinaryOp::Lt => Ok(Arc::new(lt(left_arr, right_arr)?)),
		BoundBinaryOp::Lte => Ok(Arc::new(lt_eq(left_arr, right_arr)?)),
		BoundBinaryOp::Gt => Ok(Arc::new(gt(left_arr, right_arr)?)),
		BoundBinaryOp::Gte => Ok(Arc::new(gt_eq(left_arr, right_arr)?)),
		BoundBinaryOp::And => {
			let left_bool = as_boolean(left_arr, "AND left operand")?;
			let right_bool = as_boolean(right_arr, "AND right operand")?;
			Ok(Arc::new(and(left_bool, right_bool)?))
		}
		BoundBinaryOp::Or => {
			let left_bool = as_boolean(left_arr, "OR left operand")?;
			let right_bool = as_boolean(right_arr, "OR right operand")?;
			Ok(Arc::new(or(left_bool, right_bool)?))
		}
	}
}

fn evaluate_bound_unary(op: &BoundUnaryOp, arr: &ArrayRef) -> Result<ArrayRef, ExecutionError> {
	match op {
		BoundUnaryOp::Not => {
			let bool_arr = as_boolean(arr, "NOT operand")?;
			Ok(Arc::new(not(bool_arr)?))
		}
		BoundUnaryOp::IsNull => Ok(Arc::new(is_null(arr)?)),
		BoundUnaryOp::IsNotNull => Ok(Arc::new(is_not_null(arr)?)),
	}
}

fn evaluate_in_list(
	expr_arr: &ArrayRef,
	item_arrs: &[ArrayRef],
	negated: bool,
	num_rows: usize,
) -> Result<ArrayRef, ExecutionError> {
	let mut combined: Option<BooleanArray> = None;
	for item_arr in item_arrs {
		let mask = eq(expr_arr, item_arr)?;
		combined = Some(match combined {
			Some(current) => or_kleene(&current, &mask)?,
			None => mask,
		});
	}
	let mask = combined.unwrap_or_else(|| BooleanArray::from(vec![Some(false); num_rows]));
	if negated {
		Ok(Arc::new(not(&mask)?))
	} else {
		Ok(Arc::new(mask))
	}
}

/// SQL CASE reads like a lazy conditional, but this vectorized evaluator computes every branch
/// array eagerly and then zips by each WHEN mask. A hard-erroring kernel in a branch fails the
/// query even for rows where that branch is not selected. Integer div-by-zero is safe because
/// ADR 084 makes it yield NULL. Per-branch masked execution is a deferred follow-on; do not
/// treat CASE as a lazy error fence.
fn evaluate_case(
	when_then: &[(ArrayRef, ArrayRef)],
	else_array: ArrayRef,
) -> Result<ArrayRef, ExecutionError> {
	let mut acc = else_array;
	for (condition, result) in when_then.iter().rev() {
		let mask = as_boolean(condition, "CASE WHEN condition")?;
		acc = arrow::compute::kernels::zip::zip(mask, &result.as_ref(), &acc.as_ref())?;
	}
	Ok(acc)
}

fn evaluate_like(
	expr_arr: &ArrayRef,
	pattern: &str,
	negated: bool,
	case_insensitive: bool,
) -> Result<ArrayRef, ExecutionError> {
	if expr_arr.data_type() != &DataType::Utf8 {
		return Err(ExecutionError::InvalidExpression {
			message: format!("LIKE expects Utf8 input, got {:?}", expr_arr.data_type()),
		});
	}
	let pattern = StringArray::new_scalar(pattern);
	let result = match (negated, case_insensitive) {
		(false, false) => arrow_like(expr_arr, &pattern)?,
		(true, false) => arrow_nlike(expr_arr, &pattern)?,
		(false, true) => arrow_ilike(expr_arr, &pattern)?,
		(true, true) => arrow_nilike(expr_arr, &pattern)?,
	};
	Ok(Arc::new(result) as ArrayRef)
}

fn evaluate_substring(
	input_arr: &ArrayRef,
	from: i64,
	length: Option<u64>,
) -> Result<ArrayRef, ExecutionError> {
	if from <= 0 {
		return Err(ExecutionError::InvalidExpression {
			message: "SUBSTRING FROM must be >= 1".to_string(),
		});
	}
	let Some(input) = input_arr.as_any().downcast_ref::<StringArray>() else {
		return Err(ExecutionError::InvalidExpression {
			message: format!(
				"SUBSTRING expects Utf8 input, got {:?}",
				input_arr.data_type()
			),
		});
	};
	let start = from - 1;
	let result = substring_by_char(input, start, length)?;
	Ok(Arc::new(result) as ArrayRef)
}

fn evaluate_at_time_zone(arr: &ArrayRef, zone: &str) -> Result<ArrayRef, ExecutionError> {
	parse_temporal_execution_zone(zone)?;
	if let Some(array) = arr.as_any().downcast_ref::<TimestampMillisecondArray>() {
		return Ok(Arc::new(array.clone().with_timezone(zone.to_string())) as ArrayRef);
	}
	if let Some(array) = arr.as_any().downcast_ref::<TimestampMicrosecondArray>() {
		return Ok(Arc::new(array.clone().with_timezone(zone.to_string())) as ArrayRef);
	}
	if let Some(array) = arr.as_any().downcast_ref::<TimestampNanosecondArray>() {
		return Ok(Arc::new(array.clone().with_timezone(zone.to_string())) as ArrayRef);
	}
	Err(ExecutionError::InvalidExpression {
		message: format!(
			"AT TIME ZONE expects Timestamp input, got {:?}",
			arr.data_type()
		),
	})
}

fn evaluate_extract(
	field: TemporalExtractField,
	zone: Option<&String>,
	arr: &ArrayRef,
) -> Result<ArrayRef, ExecutionError> {
	if let Some(zone) = zone {
		let timezone = parse_temporal_execution_zone(zone)?;
		if let Some(array) = arr.as_any().downcast_ref::<TimestampMillisecondArray>() {
			return timestamp_extract_array(array, TimeUnit::Millisecond, timezone, field);
		}
		if let Some(array) = arr.as_any().downcast_ref::<TimestampMicrosecondArray>() {
			return timestamp_extract_array(array, TimeUnit::Microsecond, timezone, field);
		}
		if let Some(array) = arr.as_any().downcast_ref::<TimestampNanosecondArray>() {
			return timestamp_extract_array(array, TimeUnit::Nanosecond, timezone, field);
		}
		return Err(ExecutionError::InvalidExpression {
			message: format!("EXTRACT expects Timestamp input, got {:?}", arr.data_type()),
		});
	}
	let Some(array) = arr.as_any().downcast_ref::<Date32Array>() else {
		return Err(ExecutionError::InvalidExpression {
			message: format!(
				"Date32 EXTRACT expects Date32 input, got {:?}",
				arr.data_type()
			),
		});
	};
	if !field.is_date32_supported() {
		return Err(ExecutionError::InvalidExpression {
			message: format!("EXTRACT {} is not supported for Date32", field.as_str()),
		});
	}
	let mut builder = Int32Builder::with_capacity(array.len());
	for value in array.iter() {
		match value {
			Some(value) => {
				builder.append_value(crate::date32::date32_extract(value, field).map_err(
					|error| ExecutionError::InvalidExpression {
						message: error.to_string(),
					},
				)?)
			}
			None => builder.append_null(),
		}
	}
	Ok(Arc::new(builder.finish()) as ArrayRef)
}

fn timestamp_extract_array<T>(
	array: &T,
	unit: TimeUnit,
	timezone: chrono_tz::Tz,
	field: TemporalExtractField,
) -> Result<ArrayRef, ExecutionError>
where
	for<'a> &'a T: IntoIterator<Item = Option<i64>>,
	T: Array,
{
	let mut builder = Int32Builder::with_capacity(array.len());
	for value in array {
		match value {
			Some(value) => builder.append_value(
				crate::timestamp::timestamp_extract(value, unit, timezone, field).map_err(
					|error| ExecutionError::InvalidExpression {
						message: error.to_string(),
					},
				)?,
			),
			None => builder.append_null(),
		}
	}
	Ok(Arc::new(builder.finish()) as ArrayRef)
}

fn evaluate_date_trunc(
	field: TemporalTruncField,
	zone: &str,
	arr: &ArrayRef,
) -> Result<ArrayRef, ExecutionError> {
	let timezone = parse_temporal_execution_zone(zone)?;
	if let Some(array) = arr.as_any().downcast_ref::<TimestampMillisecondArray>() {
		return timestamp_trunc_array(array, TimeUnit::Millisecond, timezone, field);
	}
	if let Some(array) = arr.as_any().downcast_ref::<TimestampMicrosecondArray>() {
		return timestamp_trunc_array(array, TimeUnit::Microsecond, timezone, field);
	}
	if let Some(array) = arr.as_any().downcast_ref::<TimestampNanosecondArray>() {
		return timestamp_trunc_array(array, TimeUnit::Nanosecond, timezone, field);
	}
	Err(ExecutionError::InvalidExpression {
		message: format!(
			"date_trunc expects Timestamp input, got {:?}",
			arr.data_type()
		),
	})
}

fn timestamp_trunc_array<T>(
	array: &T,
	unit: TimeUnit,
	timezone: chrono_tz::Tz,
	field: TemporalTruncField,
) -> Result<ArrayRef, ExecutionError>
where
	for<'a> &'a T: IntoIterator<Item = Option<i64>>,
	T: Array,
{
	let values = array
		.into_iter()
		.map(|value| {
			value
				.map(|value| {
					crate::timestamp::timestamp_trunc(value, unit, timezone, field).map_err(
						|error| ExecutionError::InvalidExpression {
							message: error.to_string(),
						},
					)
				})
				.transpose()
		})
		.collect::<Result<Vec<_>, ExecutionError>>()?;
	Ok(match unit {
		TimeUnit::Millisecond => {
			Arc::new(TimestampMillisecondArray::from(values).with_timezone("UTC")) as ArrayRef
		}
		TimeUnit::Microsecond => {
			Arc::new(TimestampMicrosecondArray::from(values).with_timezone("UTC")) as ArrayRef
		}
		TimeUnit::Nanosecond => {
			Arc::new(TimestampNanosecondArray::from(values).with_timezone("UTC")) as ArrayRef
		}
		TimeUnit::Second => unreachable!("TimestampSecond is not a supported Toron timestamp unit"),
	})
}

fn parse_temporal_execution_zone(zone: &str) -> Result<chrono_tz::Tz, ExecutionError> {
	crate::timestamp::parse_session_timezone(zone).map_err(|error| {
		ExecutionError::InvalidExpression {
			message: error.to_string(),
		}
	})
}

fn evaluate_time_bucket(
	interval: std::time::Duration,
	arr: &ArrayRef,
) -> Result<ArrayRef, ExecutionError> {
	if let Some(int_arr) = arr.as_any().downcast_ref::<Int64Array>() {
		let interval_ms = interval.as_millis() as i64;
		let bucketed: Vec<Option<i64>> = int_arr
			.iter()
			.map(|value| value.map(|value| (value / interval_ms) * interval_ms))
			.collect();
		Ok(Arc::new(Int64Array::from(bucketed)) as ArrayRef)
	} else {
		Err(ExecutionError::InvalidExpression {
			message: "TimeBucket expects Int64 (ms) input".to_string(),
		})
	}
}

fn duration_micros(started: Instant) -> u64 {
	u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX)
}

fn record_task_telemetry(
	telemetry: &Option<Arc<Mutex<TaskTelemetry>>>,
	update: impl FnOnce(&mut TaskTelemetry),
) {
	let Some(telemetry) = telemetry else {
		return;
	};
	let lock_started = Instant::now();
	if let Ok(mut telemetry) = telemetry.lock() {
		telemetry.phases.telemetry_lock_micros = telemetry
			.phases
			.telemetry_lock_micros
			.saturating_add(duration_micros(lock_started));
		update(&mut telemetry);
	}
}

fn record_join_runtime_observation(
	telemetry: &Option<Arc<Mutex<TaskTelemetry>>>,
	mode: &JoinRuntimeObservationMode,
	observation: JoinRuntimeObservation,
) {
	if !mode.measures(observation.replay_id) {
		return;
	}
	record_task_telemetry(telemetry, |task| {
		task.join_observations.push(observation);
	});
}

fn record_batch_row_count(batches: &[RecordBatch]) -> u64 {
	batches
		.iter()
		.map(|batch| batch.num_rows() as u64)
		.sum::<u64>()
}

#[allow(clippy::too_many_arguments)]
async fn execute_hash_join(
	left_schema: arrow::datatypes::SchemaRef,
	right_schema: arrow::datatypes::SchemaRef,
	left_batches: &[RecordBatch],
	right_batches: &[RecordBatch],
	kind: JoinKind,
	left_keys: &[crate::ir::ColumnOrdinal],
	right_keys: &[crate::ir::ColumnOrdinal],
	match_filter: Option<&BoundExprProgram>,
	null_safe_keys: bool,
	replay_id: JoinReplayId,
	strategy: JoinStrategy,
	output_schema: &arrow::datatypes::SchemaRef,
	task_budget: TaskBudget,
	telemetry: Option<Arc<Mutex<TaskTelemetry>>>,
	join_runtime_observation_mode: JoinRuntimeObservationMode,
	spill_file_ops: Arc<dyn AsyncFileOps>,
	spill_dir: PathBuf,
) -> Result<Vec<RecordBatch>, ExecutionError> {
	if left_keys.len() != right_keys.len() {
		return Err(ExecutionError::InvalidExpression {
			message: "join key arity mismatch".to_string(),
		});
	}

	let left_batch = concat_or_empty(left_schema, left_batches)?;
	let right_batch = concat_or_empty(right_schema, right_batches)?;

	// ADR 239: CROSS is keyless — route through the explicit match-all path instead of the hash
	// build/probe. The hash path would bucket every row under `join_key_bytes([])`, which is the
	// single-bucket distributed trap; the explicit path emits every build×probe pair deliberately.
	// One bounded path serves both the in-memory and would-be-spilled cases, so they cannot diverge.
	if matches!(kind, JoinKind::Cross) {
		return execute_cross_join_match_all(
			&left_batch,
			&right_batch,
			output_schema,
			task_budget.memory.get(),
			replay_id,
			strategy,
			&telemetry,
			&join_runtime_observation_mode,
		);
	}

	let left_key_arrays = key_arrays(&left_batch, left_keys)?;
	let right_key_arrays = key_arrays(&right_batch, right_keys)?;
	record_task_telemetry(&telemetry, |task| {
		task.counters.build_rows = task
			.counters
			.build_rows
			.saturating_add(left_batch.num_rows() as u64);
		task.counters.probe_rows = task
			.counters
			.probe_rows
			.saturating_add(right_batch.num_rows() as u64);
	});

	if estimated_build_hash_bytes(&left_key_arrays, left_batch.num_rows())
		> task_budget.memory.get()
	{
		let result = execute_spilled_hash_join(
			kind,
			&left_batch,
			&right_batch,
			&left_key_arrays,
			&right_key_arrays,
			output_schema,
			match_filter,
			null_safe_keys,
			task_budget,
			telemetry.clone(),
			spill_file_ops,
			spill_dir,
		)
		.await;
		if let Ok(batches) = &result {
			record_join_runtime_observation(
				&telemetry,
				&join_runtime_observation_mode,
				JoinRuntimeObservation {
					replay_id,
					strategy,
					build_rows: RowCount::new(left_batch.num_rows() as u64),
					probe_rows: RowCount::new(right_batch.num_rows() as u64),
					output_rows: RowCount::new(record_batch_row_count(batches)),
				},
			);
		}
		return result;
	}

	let join_key_plan =
		JoinKeyPlan::from_key_arrays(&left_key_arrays, &right_key_arrays, null_safe_keys);

	let build_started = Instant::now();
	let Some(build_rows) = build_in_memory_join_rows(
		join_key_plan,
		&left_key_arrays,
		left_batch.num_rows(),
		task_budget.memory.get(),
		null_safe_keys,
	)?
	else {
		let result = execute_spilled_hash_join(
			kind,
			&left_batch,
			&right_batch,
			&left_key_arrays,
			&right_key_arrays,
			output_schema,
			match_filter,
			null_safe_keys,
			task_budget,
			telemetry.clone(),
			spill_file_ops,
			spill_dir,
		)
		.await;
		if let Ok(batches) = &result {
			record_join_runtime_observation(
				&telemetry,
				&join_runtime_observation_mode,
				JoinRuntimeObservation {
					replay_id,
					strategy,
					build_rows: RowCount::new(left_batch.num_rows() as u64),
					probe_rows: RowCount::new(right_batch.num_rows() as u64),
					output_rows: RowCount::new(record_batch_row_count(batches)),
				},
			);
		}
		return result;
	};
	record_task_telemetry(&telemetry, |task| {
		task.phases.build_micros = task
			.phases
			.build_micros
			.saturating_add(duration_micros(build_started));
	});

	let probe_started = Instant::now();
	let selection = probe_in_memory_join_rows(
		kind,
		join_key_plan,
		&build_rows,
		left_batch.num_rows(),
		&right_key_arrays,
		right_batch.num_rows(),
		match_filter,
		&left_batch,
		&right_batch,
		null_safe_keys,
	)?;
	record_task_telemetry(&telemetry, |task| {
		task.phases.probe_micros = task
			.phases
			.probe_micros
			.saturating_add(duration_micros(probe_started));
		task.counters.matched_rows = task
			.counters
			.matched_rows
			.saturating_add(selection.len() as u64);
	});

	let output = vec![materialize_join_selection(
		kind,
		&left_batch,
		&right_batch,
		&selection,
		output_schema,
		task_budget.memory.get(),
	)?];
	record_join_runtime_observation(
		&telemetry,
		&join_runtime_observation_mode,
		JoinRuntimeObservation {
			replay_id,
			strategy,
			build_rows: RowCount::new(left_batch.num_rows() as u64),
			probe_rows: RowCount::new(right_batch.num_rows() as u64),
			output_rows: RowCount::new(record_batch_row_count(&output)),
		},
	);
	Ok(output)
}

#[allow(clippy::too_many_arguments)]
async fn execute_hash_join_into_global_aggregate(
	executor: &Executor,
	left_schema: arrow::datatypes::SchemaRef,
	right_schema: arrow::datatypes::SchemaRef,
	left_batches: &[RecordBatch],
	right_batches: &[RecordBatch],
	kind: JoinKind,
	left_keys: &[crate::ir::ColumnOrdinal],
	right_keys: &[crate::ir::ColumnOrdinal],
	match_filter: Option<&BoundExprProgram>,
	null_safe_keys: bool,
	replay_id: JoinReplayId,
	strategy: JoinStrategy,
	join_output_schema: &arrow::datatypes::SchemaRef,
	filter_predicate: Option<&BoundExprProgram>,
	aggr_expr: &[BoundAggregateCall],
	aggregate_state: &mut [AggState],
	task_budget: TaskBudget,
	telemetry: Option<Arc<Mutex<TaskTelemetry>>>,
	join_runtime_observation_mode: JoinRuntimeObservationMode,
	spill_file_ops: Arc<dyn AsyncFileOps>,
	spill_dir: PathBuf,
) -> Result<(), ExecutionError> {
	if left_keys.len() != right_keys.len() {
		return Err(ExecutionError::InvalidExpression {
			message: "join key arity mismatch".to_string(),
		});
	}
	if kind != JoinKind::Inner {
		return Err(ExecutionError::InvalidExpression {
			message: "streaming collapsing aggregate supports only inner joins".to_string(),
		});
	}

	let left_batch = concat_or_empty(left_schema, left_batches)?;
	let right_batch = concat_or_empty(right_schema, right_batches)?;
	let left_key_arrays = key_arrays(&left_batch, left_keys)?;
	let right_key_arrays = key_arrays(&right_batch, right_keys)?;
	record_task_telemetry(&telemetry, |task| {
		task.counters.build_rows = task
			.counters
			.build_rows
			.saturating_add(left_batch.num_rows() as u64);
		task.counters.probe_rows = task
			.counters
			.probe_rows
			.saturating_add(right_batch.num_rows() as u64);
	});

	if estimated_build_hash_bytes(&left_key_arrays, left_batch.num_rows())
		> task_budget.memory.get()
	{
		let result = execute_spilled_hash_join_into_global_aggregate(
			executor,
			kind,
			&left_batch,
			&right_batch,
			&left_key_arrays,
			&right_key_arrays,
			join_output_schema,
			filter_predicate,
			aggr_expr,
			aggregate_state,
			match_filter,
			null_safe_keys,
			task_budget,
			telemetry.clone(),
			spill_file_ops,
			spill_dir,
		)
		.await;
		if let Ok(output_rows) = &result {
			record_join_runtime_observation(
				&telemetry,
				&join_runtime_observation_mode,
				JoinRuntimeObservation {
					replay_id,
					strategy,
					build_rows: RowCount::new(left_batch.num_rows() as u64),
					probe_rows: RowCount::new(right_batch.num_rows() as u64),
					output_rows: RowCount::new(*output_rows),
				},
			);
		}
		return result.map(|_| ());
	}

	let join_key_plan =
		JoinKeyPlan::from_key_arrays(&left_key_arrays, &right_key_arrays, null_safe_keys);

	let build_started = Instant::now();
	let Some(build_rows) = build_in_memory_join_rows(
		join_key_plan,
		&left_key_arrays,
		left_batch.num_rows(),
		task_budget.memory.get(),
		null_safe_keys,
	)?
	else {
		let result = execute_spilled_hash_join_into_global_aggregate(
			executor,
			kind,
			&left_batch,
			&right_batch,
			&left_key_arrays,
			&right_key_arrays,
			join_output_schema,
			filter_predicate,
			aggr_expr,
			aggregate_state,
			match_filter,
			null_safe_keys,
			task_budget,
			telemetry.clone(),
			spill_file_ops,
			spill_dir,
		)
		.await;
		if let Ok(output_rows) = &result {
			record_join_runtime_observation(
				&telemetry,
				&join_runtime_observation_mode,
				JoinRuntimeObservation {
					replay_id,
					strategy,
					build_rows: RowCount::new(left_batch.num_rows() as u64),
					probe_rows: RowCount::new(right_batch.num_rows() as u64),
					output_rows: RowCount::new(*output_rows),
				},
			);
		}
		return result.map(|_| ());
	};
	record_task_telemetry(&telemetry, |task| {
		task.phases.build_micros = task
			.phases
			.build_micros
			.saturating_add(duration_micros(build_started));
	});

	let probe_started = Instant::now();
	let selection = probe_in_memory_join_rows(
		kind,
		join_key_plan,
		&build_rows,
		left_batch.num_rows(),
		&right_key_arrays,
		right_batch.num_rows(),
		match_filter,
		&left_batch,
		&right_batch,
		null_safe_keys,
	)?;
	record_task_telemetry(&telemetry, |task| {
		task.phases.probe_micros = task
			.phases
			.probe_micros
			.saturating_add(duration_micros(probe_started));
		task.counters.matched_rows = task
			.counters
			.matched_rows
			.saturating_add(selection.len() as u64);
	});

	fold_join_selection_chunks_into_global_aggregate(
		executor,
		kind,
		&left_batch,
		&right_batch,
		&selection,
		join_output_schema,
		filter_predicate,
		aggr_expr,
		aggregate_state,
		task_budget.memory.get(),
	)?;
	record_join_runtime_observation(
		&telemetry,
		&join_runtime_observation_mode,
		JoinRuntimeObservation {
			replay_id,
			strategy,
			build_rows: RowCount::new(left_batch.num_rows() as u64),
			probe_rows: RowCount::new(right_batch.num_rows() as u64),
			output_rows: RowCount::new(selection.len() as u64),
		},
	);
	Ok(())
}

/// Maximum output rows accumulated before a CROSS-join batch is flushed — bounds the match-all
/// selection allocation so a large cartesian streams out in chunks (ADR 239 D5).
const CROSS_JOIN_OUTPUT_CHUNK_ROWS: usize = 8192;

/// Explicit keyless match-all for `JoinKind::Cross` (ADR 239 D5): emit every build×probe pair.
/// Unlike the hash path this never keys on `join_key_bytes([])`, so a CROSS join cannot be mistaken
/// for — or silently degrade into — a single-bucket empty-key hash join. The output is chunked to
/// bound allocation; because there is exactly one match-all path, in-memory and would-be-spilled
/// execution produce identical cartesian rows.
fn execute_cross_join_match_all(
	left_batch: &RecordBatch,
	right_batch: &RecordBatch,
	output_schema: &arrow::datatypes::SchemaRef,
	memory_cap: u64,
	replay_id: JoinReplayId,
	strategy: JoinStrategy,
	telemetry: &Option<Arc<Mutex<TaskTelemetry>>>,
	join_runtime_observation_mode: &JoinRuntimeObservationMode,
) -> Result<Vec<RecordBatch>, ExecutionError> {
	let left_rows = left_batch.num_rows();
	let right_rows = right_batch.num_rows();
	record_task_telemetry(telemetry, |task| {
		task.counters.build_rows = task.counters.build_rows.saturating_add(left_rows as u64);
		task.counters.probe_rows = task.counters.probe_rows.saturating_add(right_rows as u64);
	});

	let mut output = Vec::new();
	if left_rows > 0 && right_rows > 0 {
		let mut selection: Vec<JoinOutputRow> = Vec::new();
		for left in 0..left_rows {
			let left = join_build_row_index(left)?;
			for right in 0..right_rows {
				let right = join_probe_row_index(right)?;
				selection.push(JoinOutputRow::Paired { left, right });
				if selection.len() >= CROSS_JOIN_OUTPUT_CHUNK_ROWS {
					output.push(materialize_join_selection(
						JoinKind::Cross,
						left_batch,
						right_batch,
						&selection,
						output_schema,
						memory_cap,
					)?);
					selection.clear();
				}
			}
		}
		if !selection.is_empty() {
			output.push(materialize_join_selection(
				JoinKind::Cross,
				left_batch,
				right_batch,
				&selection,
				output_schema,
				memory_cap,
			)?);
		}
	}
	if output.is_empty() {
		output.push(RecordBatch::new_empty(output_schema.clone()));
	}

	let output_rows = record_batch_row_count(&output);
	record_task_telemetry(telemetry, |task| {
		task.counters.matched_rows = task.counters.matched_rows.saturating_add(output_rows);
	});
	record_join_runtime_observation(
		telemetry,
		join_runtime_observation_mode,
		JoinRuntimeObservation {
			replay_id,
			strategy,
			build_rows: RowCount::new(left_rows as u64),
			probe_rows: RowCount::new(right_rows as u64),
			output_rows: RowCount::new(output_rows),
		},
	);
	Ok(output)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JoinOutputRow {
	Paired { left: u32, right: u32 },
	LeftFillNull { left: u32 },
	RightFillNull { right: u32 },
	LeftPreserved { left: u32 },
	RightPreserved { right: u32 },
}

fn materialize_join_selection(
	kind: JoinKind,
	left_batch: &RecordBatch,
	right_batch: &RecordBatch,
	selection: &[JoinOutputRow],
	output_schema: &arrow::datatypes::SchemaRef,
	memory_cap: u64,
) -> Result<RecordBatch, ExecutionError> {
	if selection.is_empty() {
		return Ok(RecordBatch::new_empty(output_schema.clone()));
	}

	let estimated_output_bytes =
		estimate_join_materialization_bytes(kind, left_batch, right_batch, selection.len());
	let materialization_cap = join_output_materialization_memory_cap(memory_cap);
	if estimated_output_bytes > materialization_cap {
		return Err(budget_exhausted(
			BudgetDimension::Memory,
			"join output materialization",
			estimated_output_bytes,
			materialization_cap,
		));
	}

	let output_columns = match kind {
		JoinKind::Inner
		| JoinKind::Cross
		| JoinKind::LeftOuter
		| JoinKind::RightOuter
		| JoinKind::FullOuter => {
			let mut left_indices = Vec::with_capacity(selection.len());
			let mut right_indices = Vec::with_capacity(selection.len());
			for row in selection {
				match *row {
					JoinOutputRow::Paired { left, right } => {
						left_indices.push(Some(left));
						right_indices.push(Some(right));
					}
					JoinOutputRow::LeftFillNull { left } => {
						left_indices.push(Some(left));
						right_indices.push(None);
					}
					JoinOutputRow::RightFillNull { right } => {
						left_indices.push(None);
						right_indices.push(Some(right));
					}
					JoinOutputRow::LeftPreserved { .. } | JoinOutputRow::RightPreserved { .. } => {
						return Err(invalid_join_selection(kind, row));
					}
				}
			}
			let left_indices = UInt32Array::from(left_indices);
			let right_indices = UInt32Array::from(right_indices);
			let mut columns =
				Vec::with_capacity(left_batch.num_columns() + right_batch.num_columns());
			for column in left_batch.columns() {
				columns.push(arrow::compute::take(column, &left_indices, None)?);
			}
			for column in right_batch.columns() {
				columns.push(arrow::compute::take(column, &right_indices, None)?);
			}
			columns
		}
		JoinKind::LeftSemi | JoinKind::LeftAnti => {
			let mut left_indices = Vec::with_capacity(selection.len());
			for row in selection {
				match *row {
					JoinOutputRow::LeftPreserved { left } => left_indices.push(Some(left)),
					_ => return Err(invalid_join_selection(kind, row)),
				}
			}
			let left_indices = UInt32Array::from(left_indices);
			let mut columns = Vec::with_capacity(left_batch.num_columns());
			for column in left_batch.columns() {
				columns.push(arrow::compute::take(column, &left_indices, None)?);
			}
			columns
		}
		JoinKind::RightSemi | JoinKind::RightAnti => {
			let mut right_indices = Vec::with_capacity(selection.len());
			for row in selection {
				match *row {
					JoinOutputRow::RightPreserved { right } => right_indices.push(Some(right)),
					_ => return Err(invalid_join_selection(kind, row)),
				}
			}
			let right_indices = UInt32Array::from(right_indices);
			let mut columns = Vec::with_capacity(right_batch.num_columns());
			for column in right_batch.columns() {
				columns.push(arrow::compute::take(column, &right_indices, None)?);
			}
			columns
		}
	};

	Ok(RecordBatch::try_new(output_schema.clone(), output_columns)?)
}

fn materialize_reoriented_inner_selection(
	probe_batch: &RecordBatch,
	build_batch: &RecordBatch,
	selection: &[JoinOutputRow],
	output_schema: &arrow::datatypes::SchemaRef,
	memory_cap: u64,
) -> Result<RecordBatch, ExecutionError> {
	if selection.is_empty() {
		return Ok(RecordBatch::new_empty(output_schema.clone()));
	}

	let estimated_output_bytes = estimate_join_materialization_bytes(
		JoinKind::Inner,
		probe_batch,
		build_batch,
		selection.len(),
	);
	let materialization_cap = join_output_materialization_memory_cap(memory_cap);
	if estimated_output_bytes > materialization_cap {
		return Err(budget_exhausted(
			BudgetDimension::Memory,
			"reoriented join output materialization",
			estimated_output_bytes,
			materialization_cap,
		));
	}

	let mut probe_indices = Vec::with_capacity(selection.len());
	let mut build_indices = Vec::with_capacity(selection.len());
	for row in selection {
		match *row {
			JoinOutputRow::Paired { left, right } => {
				build_indices.push(Some(left));
				probe_indices.push(Some(right));
			}
			_ => return Err(invalid_join_selection(JoinKind::Inner, row)),
		}
	}
	let probe_indices = UInt32Array::from(probe_indices);
	let build_indices = UInt32Array::from(build_indices);
	let mut columns = Vec::with_capacity(probe_batch.num_columns() + build_batch.num_columns());
	for column in probe_batch.columns() {
		columns.push(arrow::compute::take(column, &probe_indices, None)?);
	}
	for column in build_batch.columns() {
		columns.push(arrow::compute::take(column, &build_indices, None)?);
	}

	Ok(RecordBatch::try_new(output_schema.clone(), columns)?)
}

fn materialize_preserved_selection(
	preserved_batch: &RecordBatch,
	selection: &[u32],
	output_schema: &arrow::datatypes::SchemaRef,
	memory_cap: u64,
) -> Result<RecordBatch, ExecutionError> {
	if selection.is_empty() {
		return Ok(RecordBatch::new_empty(output_schema.clone()));
	}
	let estimated_output_bytes = join_take_index_bytes(selection.len()).saturating_add(
		estimate_selected_batch_bytes(preserved_batch, selection.len()),
	);
	let materialization_cap = join_output_materialization_memory_cap(memory_cap);
	if estimated_output_bytes > materialization_cap {
		return Err(budget_exhausted(
			BudgetDimension::Memory,
			"semi/anti preserved output materialization",
			estimated_output_bytes,
			materialization_cap,
		));
	}
	let indices = UInt32Array::from(selection.iter().copied().map(Some).collect::<Vec<_>>());
	let mut columns = Vec::with_capacity(preserved_batch.num_columns());
	for column in preserved_batch.columns() {
		columns.push(arrow::compute::take(column, &indices, None)?);
	}
	Ok(RecordBatch::try_new(output_schema.clone(), columns)?)
}

fn estimate_join_materialization_bytes(
	kind: JoinKind,
	left_batch: &RecordBatch,
	right_batch: &RecordBatch,
	output_rows: usize,
) -> u64 {
	let mut bytes = 0u64;
	match kind {
		JoinKind::Inner
		| JoinKind::Cross
		| JoinKind::LeftOuter
		| JoinKind::RightOuter
		| JoinKind::FullOuter => {
			bytes = bytes
				.saturating_add(join_take_index_bytes(output_rows))
				.saturating_add(join_take_index_bytes(output_rows))
				.saturating_add(estimate_selected_batch_bytes(left_batch, output_rows))
				.saturating_add(estimate_selected_batch_bytes(right_batch, output_rows));
		}
		JoinKind::LeftSemi | JoinKind::LeftAnti => {
			bytes = bytes
				.saturating_add(join_take_index_bytes(output_rows))
				.saturating_add(estimate_selected_batch_bytes(left_batch, output_rows));
		}
		JoinKind::RightSemi | JoinKind::RightAnti => {
			bytes = bytes
				.saturating_add(join_take_index_bytes(output_rows))
				.saturating_add(estimate_selected_batch_bytes(right_batch, output_rows));
		}
	}
	bytes
}

fn join_output_materialization_memory_cap(memory_cap: u64) -> u64 {
	memory_cap.saturating_mul(JOIN_OUTPUT_MATERIALIZATION_MEMORY_CAP_MULTIPLIER)
}

fn join_take_index_bytes(rows: usize) -> u64 {
	let rows = u64::try_from(rows).unwrap_or(u64::MAX);
	rows.saturating_mul(std::mem::size_of::<u32>() as u64)
		.saturating_add(rows.saturating_add(7) / 8)
}

fn estimate_selected_batch_bytes(batch: &RecordBatch, output_rows: usize) -> u64 {
	batch
		.columns()
		.iter()
		.map(|column| estimate_selected_column_bytes(column, batch.num_rows(), output_rows))
		.fold(0u64, u64::saturating_add)
}

fn estimate_selected_column_bytes(
	column: &ArrayRef,
	source_rows: usize,
	output_rows: usize,
) -> u64 {
	if output_rows == 0 {
		return 0;
	}

	let output_rows = u64::try_from(output_rows).unwrap_or(u64::MAX);
	let bytes_per_row = if source_rows == 0 {
		empty_source_column_row_width(column.data_type())
	} else {
		let source_bytes = u64::try_from(column.get_array_memory_size()).unwrap_or(u64::MAX);
		let source_rows = u64::try_from(source_rows).unwrap_or(u64::MAX);
		source_bytes.saturating_add(source_rows.saturating_sub(1)) / source_rows
	};
	bytes_per_row.saturating_mul(output_rows)
}

fn empty_source_column_row_width(data_type: &DataType) -> u64 {
	match data_type {
		DataType::Boolean | DataType::Int8 | DataType::UInt8 => 1,
		DataType::Int16 | DataType::UInt16 => 2,
		DataType::Int32 | DataType::UInt32 | DataType::Float32 | DataType::Date32 => 4,
		DataType::Int64
		| DataType::UInt64
		| DataType::Float64
		| DataType::Timestamp(_, _)
		| DataType::Duration(_) => 8,
		DataType::Decimal128(_, _) => 16,
		DataType::Utf8 | DataType::Binary => 8,
		DataType::LargeUtf8 | DataType::LargeBinary => 16,
		_ => 16,
	}
}

fn invalid_join_selection(kind: JoinKind, row: &JoinOutputRow) -> ExecutionError {
	ExecutionError::Kernel {
		message: format!("invalid {kind:?} join selection row: {row:?}"),
	}
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum JoinKey {
	Int32(i32),
	Int64(i64),
	Generic(Vec<u8>),
}

#[derive(Debug, Clone)]
struct BuildRowList {
	first: u32,
	overflow: Option<Vec<u32>>,
}

impl BuildRowList {
	fn new(first: u32) -> Self {
		Self {
			first,
			overflow: None,
		}
	}

	fn push(&mut self, row: u32) {
		self.overflow.get_or_insert_with(Vec::new).push(row);
	}

	fn rows(&self) -> impl Iterator<Item = u32> + '_ {
		std::iter::once(self.first).chain(self.overflow.iter().flatten().copied())
	}

	#[cfg(test)]
	fn overflow_len(&self) -> usize {
		self.overflow.as_ref().map(Vec::len).unwrap_or(0)
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JoinKeyPlan {
	Int32,
	Int64,
	Generic,
}

impl JoinKeyPlan {
	fn from_key_arrays(
		left_key_arrays: &[ArrayRef],
		right_key_arrays: &[ArrayRef],
		null_safe_keys: bool,
	) -> Self {
		if null_safe_keys {
			return Self::Generic;
		}
		if left_key_arrays.len() != 1 || right_key_arrays.len() != 1 {
			return Self::Generic;
		}
		if left_key_arrays[0].data_type() != right_key_arrays[0].data_type() {
			return Self::Generic;
		}
		match left_key_arrays[0].data_type() {
			DataType::Int32 => Self::Int32,
			DataType::Int64 => Self::Int64,
			_ => Self::Generic,
		}
	}

	fn from_build_arrays_and_probe_schema(
		build_key_arrays: &[ArrayRef],
		probe_schema: &SchemaRef,
		probe_keys: &[crate::ir::ColumnOrdinal],
		null_safe_keys: bool,
	) -> Result<Self, ExecutionError> {
		if null_safe_keys {
			return Ok(Self::Generic);
		}
		if build_key_arrays.len() != 1 || probe_keys.len() != 1 {
			return Ok(Self::Generic);
		}
		let probe_index = usize::try_from(probe_keys[0].0).expect("column ordinal fits usize");
		let Some(probe_field) = probe_schema.fields().get(probe_index) else {
			return Err(ExecutionError::InvalidExpression {
				message: format!("probe join key ordinal {probe_index} out of bounds"),
			});
		};
		if build_key_arrays[0].data_type() != probe_field.data_type() {
			return Ok(Self::Generic);
		}
		Ok(match build_key_arrays[0].data_type() {
			DataType::Int32 => Self::Int32,
			DataType::Int64 => Self::Int64,
			_ => Self::Generic,
		})
	}

	fn from_build_schema_and_probe_schema(
		build_schema: &SchemaRef,
		build_keys: &[crate::ir::ColumnOrdinal],
		probe_schema: &SchemaRef,
		probe_keys: &[crate::ir::ColumnOrdinal],
		null_safe_keys: bool,
	) -> Result<Self, ExecutionError> {
		if null_safe_keys {
			return Ok(Self::Generic);
		}
		if build_keys.len() != 1 || probe_keys.len() != 1 {
			return Ok(Self::Generic);
		}
		let build_index = usize::try_from(build_keys[0].0).expect("column ordinal fits usize");
		let probe_index = usize::try_from(probe_keys[0].0).expect("column ordinal fits usize");
		let Some(build_field) = build_schema.fields().get(build_index) else {
			return Err(ExecutionError::InvalidExpression {
				message: format!("build join key ordinal {build_index} out of bounds"),
			});
		};
		let Some(probe_field) = probe_schema.fields().get(probe_index) else {
			return Err(ExecutionError::InvalidExpression {
				message: format!("probe join key ordinal {probe_index} out of bounds"),
			});
		};
		if build_field.data_type() != probe_field.data_type() {
			return Ok(Self::Generic);
		}
		Ok(match build_field.data_type() {
			DataType::Int32 => Self::Int32,
			DataType::Int64 => Self::Int64,
			_ => Self::Generic,
		})
	}
}

fn join_key_at(
	join_key_plan: JoinKeyPlan,
	key_arrays: &[ArrayRef],
	row: usize,
	null_safe_keys: bool,
) -> Result<Option<JoinKey>, ExecutionError> {
	Ok(match join_key_plan {
		JoinKeyPlan::Int32 => {
			let values = single_join_key_array::<Int32Array>(key_arrays, "Int32")?;
			if values.is_null(row) {
				None
			} else {
				Some(JoinKey::Int32(values.value(row)))
			}
		}
		JoinKeyPlan::Int64 => {
			let values = single_join_key_array::<Int64Array>(key_arrays, "Int64")?;
			if values.is_null(row) {
				None
			} else {
				Some(JoinKey::Int64(values.value(row)))
			}
		}
		JoinKeyPlan::Generic if null_safe_keys => {
			Some(JoinKey::Generic(join_key_bytes_null_safe(key_arrays, row)?))
		}
		JoinKeyPlan::Generic => join_key_bytes(key_arrays, row)?.map(JoinKey::Generic),
	})
}

fn build_in_memory_join_rows(
	join_key_plan: JoinKeyPlan,
	left_key_arrays: &[ArrayRef],
	rows: usize,
	memory_cap: u64,
	null_safe_keys: bool,
) -> Result<Option<HashMap<JoinKey, BuildRowList>>, ExecutionError> {
	let mut build_rows = HashMap::with_capacity(rows);
	let mut inserted_rows = 0usize;

	match join_key_plan {
		JoinKeyPlan::Int32 => {
			let values = single_join_key_array::<Int32Array>(left_key_arrays, "Int32")?;
			for row in 0..rows {
				if values.is_null(row) {
					continue;
				}
				if !join_build_memory_fits(
					left_key_arrays,
					inserted_rows.saturating_add(1),
					memory_cap,
				) {
					return Ok(None);
				}
				insert_join_build_row(
					&mut build_rows,
					JoinKey::Int32(values.value(row)),
					join_build_row_index(row)?,
				);
				inserted_rows = inserted_rows.saturating_add(1);
			}
		}
		JoinKeyPlan::Int64 => {
			let values = single_join_key_array::<Int64Array>(left_key_arrays, "Int64")?;
			for row in 0..rows {
				if values.is_null(row) {
					continue;
				}
				if !join_build_memory_fits(
					left_key_arrays,
					inserted_rows.saturating_add(1),
					memory_cap,
				) {
					return Ok(None);
				}
				insert_join_build_row(
					&mut build_rows,
					JoinKey::Int64(values.value(row)),
					join_build_row_index(row)?,
				);
				inserted_rows = inserted_rows.saturating_add(1);
			}
		}
		JoinKeyPlan::Generic => {
			for row in 0..rows {
				let key = if null_safe_keys {
					join_key_bytes_null_safe(left_key_arrays, row)?
				} else {
					let Some(key) = join_key_bytes(left_key_arrays, row)? else {
						continue;
					};
					key
				};
				if !join_build_memory_fits(
					left_key_arrays,
					inserted_rows.saturating_add(1),
					memory_cap,
				) {
					return Ok(None);
				}
				insert_join_build_row(
					&mut build_rows,
					JoinKey::Generic(key),
					join_build_row_index(row)?,
				);
				inserted_rows = inserted_rows.saturating_add(1);
			}
		}
	}

	Ok(Some(build_rows))
}

fn probe_in_memory_join_rows(
	kind: JoinKind,
	join_key_plan: JoinKeyPlan,
	build_rows: &HashMap<JoinKey, BuildRowList>,
	left_rows: usize,
	right_key_arrays: &[ArrayRef],
	rows: usize,
	match_filter: Option<&BoundExprProgram>,
	left_batch: &RecordBatch,
	right_batch: &RecordBatch,
	null_safe_keys: bool,
) -> Result<Vec<JoinOutputRow>, ExecutionError> {
	let mut selection = Vec::new();
	let mut matched_build_rows = build_match_bitmap(kind, left_rows);
	let mut residual = JoinMatchResidual::new(match_filter, left_batch, right_batch);

	match join_key_plan {
		JoinKeyPlan::Int32 => {
			let values = single_join_key_array::<Int32Array>(right_key_arrays, "Int32")?;
			for row in 0..rows {
				if values.is_null(row) {
					append_unmatched_probe_row(kind, row, &mut selection)?;
				} else {
					let key = JoinKey::Int32(values.value(row));
					let matched = if let Some(matches) = build_rows.get(&key) {
						append_join_matches_for_kind(
							kind,
							row,
							matches.rows(),
							matched_build_rows.as_deref_mut(),
							&mut selection,
							&mut residual,
						)?
					} else {
						false
					};
					if !matched {
						append_unmatched_probe_row(kind, row, &mut selection)?;
					}
				}
			}
		}
		JoinKeyPlan::Int64 => {
			let values = single_join_key_array::<Int64Array>(right_key_arrays, "Int64")?;
			for row in 0..rows {
				if values.is_null(row) {
					append_unmatched_probe_row(kind, row, &mut selection)?;
				} else {
					let key = JoinKey::Int64(values.value(row));
					let matched = if let Some(matches) = build_rows.get(&key) {
						append_join_matches_for_kind(
							kind,
							row,
							matches.rows(),
							matched_build_rows.as_deref_mut(),
							&mut selection,
							&mut residual,
						)?
					} else {
						false
					};
					if !matched {
						append_unmatched_probe_row(kind, row, &mut selection)?;
					}
				}
			}
		}
		JoinKeyPlan::Generic => {
			for row in 0..rows {
				let key = if null_safe_keys {
					join_key_bytes_null_safe(right_key_arrays, row)?
				} else {
					let Some(key) = join_key_bytes(right_key_arrays, row)? else {
						append_unmatched_probe_row(kind, row, &mut selection)?;
						continue;
					};
					key
				};
				let key = JoinKey::Generic(key);
				let matched = if let Some(matches) = build_rows.get(&key) {
					append_join_matches_for_kind(
						kind,
						row,
						matches.rows(),
						matched_build_rows.as_deref_mut(),
						&mut selection,
						&mut residual,
					)?
				} else {
					false
				};
				if !matched {
					append_unmatched_probe_row(kind, row, &mut selection)?;
				}
			}
		}
	}

	append_build_side_after_probe_rows(
		kind,
		left_rows,
		matched_build_rows.as_deref(),
		&mut selection,
	)?;

	Ok(selection)
}

fn single_join_key_array<'a, T: 'static>(
	key_arrays: &'a [ArrayRef],
	label: &'static str,
) -> Result<&'a T, ExecutionError> {
	key_arrays[0]
		.as_any()
		.downcast_ref::<T>()
		.ok_or_else(|| ExecutionError::InvalidExpression {
			message: format!("join key array is not {label}"),
		})
}

fn insert_join_build_row(build_rows: &mut HashMap<JoinKey, BuildRowList>, key: JoinKey, row: u32) {
	match build_rows.entry(key) {
		Entry::Occupied(mut entry) => entry.get_mut().push(row),
		Entry::Vacant(entry) => {
			entry.insert(BuildRowList::new(row));
		}
	}
}

fn append_join_matches_for_kind(
	kind: JoinKind,
	probe_row: usize,
	matches: impl Iterator<Item = u32>,
	mut matched_build_rows: Option<&mut [u64]>,
	selection: &mut Vec<JoinOutputRow>,
	residual: &mut JoinMatchResidual<'_>,
) -> Result<bool, ExecutionError> {
	let right_index = join_probe_row_index(probe_row)?;
	let mut matched = false;
	for left_index in matches {
		if !residual.matches(left_index, right_index)? {
			continue;
		}
		matched = true;
		if let Some(bitmap) = matched_build_rows.as_deref_mut() {
			set_build_row_matched(bitmap, left_index);
		}
		match kind {
			JoinKind::Inner
			| JoinKind::Cross
			| JoinKind::LeftOuter
			| JoinKind::RightOuter
			| JoinKind::FullOuter => selection.push(JoinOutputRow::Paired {
				left: left_index,
				right: right_index,
			}),
			JoinKind::LeftSemi | JoinKind::LeftAnti | JoinKind::RightSemi | JoinKind::RightAnti => {
			}
		}
	}
	if matched && kind == JoinKind::RightSemi {
		selection.push(JoinOutputRow::RightPreserved { right: right_index });
	}
	Ok(matched)
}

struct JoinMatchResidual<'a> {
	program: Option<&'a BoundExprProgram>,
	left_batch: &'a RecordBatch,
	right_batch: &'a RecordBatch,
	schema: Option<SchemaRef>,
}

impl<'a> JoinMatchResidual<'a> {
	fn new(
		program: Option<&'a BoundExprProgram>,
		left_batch: &'a RecordBatch,
		right_batch: &'a RecordBatch,
	) -> Self {
		let schema = program.map(|_| {
			let fields = left_batch
				.schema()
				.fields()
				.iter()
				.chain(right_batch.schema().fields().iter())
				.map(|field| field.as_ref().clone())
				.collect::<Vec<_>>();
			Arc::new(Schema::new(fields))
		});
		Self {
			program,
			left_batch,
			right_batch,
			schema,
		}
	}

	fn matches(&mut self, left_row: u32, right_row: u32) -> Result<bool, ExecutionError> {
		let Some(program) = self.program else {
			return Ok(true);
		};
		let batch = join_match_pair_batch(
			self.left_batch,
			self.right_batch,
			left_row,
			right_row,
			self.schema
				.as_ref()
				.expect("residual schema exists when program exists")
				.clone(),
		)?;
		let mut roots =
			Executor::evaluate_bound_program_without_storage(program, &batch)?.into_iter();
		let Some(root) = roots.next() else {
			return Err(ExecutionError::InvalidExpression {
				message: "JOIN match filter must have exactly one root".to_string(),
			});
		};
		if roots.next().is_some() {
			return Err(ExecutionError::InvalidExpression {
				message: "JOIN match filter must have exactly one root".to_string(),
			});
		}
		let bools = root
			.as_any()
			.downcast_ref::<BooleanArray>()
			.ok_or_else(|| ExecutionError::InvalidExpression {
				message: "JOIN match filter must return Boolean".to_string(),
			})?;
		Ok(bools.is_valid(0) && bools.value(0))
	}
}

fn join_match_pair_batch(
	left_batch: &RecordBatch,
	right_batch: &RecordBatch,
	left_row: u32,
	right_row: u32,
	schema: SchemaRef,
) -> Result<RecordBatch, ExecutionError> {
	let left_index = UInt32Array::from(vec![left_row]);
	let right_index = UInt32Array::from(vec![right_row]);
	let mut columns = Vec::with_capacity(left_batch.num_columns() + right_batch.num_columns());
	for column in left_batch.columns() {
		columns.push(take(column, &left_index, None)?);
	}
	for column in right_batch.columns() {
		columns.push(take(column, &right_index, None)?);
	}
	Ok(RecordBatch::try_new(schema, columns)?)
}

fn append_unmatched_probe_row(
	kind: JoinKind,
	probe_row: usize,
	selection: &mut Vec<JoinOutputRow>,
) -> Result<(), ExecutionError> {
	let right_index = join_probe_row_index(probe_row)?;
	match kind {
		JoinKind::RightOuter | JoinKind::FullOuter => {
			selection.push(JoinOutputRow::RightFillNull { right: right_index });
		}
		JoinKind::RightAnti => {
			selection.push(JoinOutputRow::RightPreserved { right: right_index });
		}
		JoinKind::Inner
		| JoinKind::Cross
		| JoinKind::LeftOuter
		| JoinKind::LeftSemi
		| JoinKind::RightSemi
		| JoinKind::LeftAnti => {}
	}
	Ok(())
}

fn append_build_side_after_probe_rows(
	kind: JoinKind,
	left_rows: usize,
	matched_build_rows: Option<&[u64]>,
	selection: &mut Vec<JoinOutputRow>,
) -> Result<(), ExecutionError> {
	let Some(bitmap) = matched_build_rows else {
		return Ok(());
	};
	for row in 0..left_rows {
		let matched = build_row_matched(bitmap, row);
		match kind {
			JoinKind::LeftOuter | JoinKind::FullOuter if !matched => {
				selection.push(JoinOutputRow::LeftFillNull {
					left: join_build_row_index(row)?,
				});
			}
			JoinKind::LeftSemi if matched => {
				selection.push(JoinOutputRow::LeftPreserved {
					left: join_build_row_index(row)?,
				});
			}
			JoinKind::LeftAnti if !matched => {
				selection.push(JoinOutputRow::LeftPreserved {
					left: join_build_row_index(row)?,
				});
			}
			_ => {}
		}
	}
	Ok(())
}

fn build_match_bitmap(kind: JoinKind, rows: usize) -> Option<Vec<u64>> {
	match kind {
		JoinKind::LeftOuter | JoinKind::FullOuter | JoinKind::LeftSemi | JoinKind::LeftAnti => {
			Some(vec![0; rows.div_ceil(64)])
		}
		JoinKind::Inner
		| JoinKind::Cross
		| JoinKind::RightOuter
		| JoinKind::RightSemi
		| JoinKind::RightAnti => None,
	}
}

fn set_build_row_matched(bitmap: &mut [u64], row: u32) {
	let row = row as usize;
	let word = row / 64;
	let bit = row % 64;
	if let Some(slot) = bitmap.get_mut(word) {
		*slot |= 1u64 << bit;
	}
}

fn build_row_matched(bitmap: &[u64], row: usize) -> bool {
	let word = row / 64;
	let bit = row % 64;
	bitmap
		.get(word)
		.is_some_and(|slot| (*slot & (1u64 << bit)) != 0)
}

fn join_build_row_index(row: usize) -> Result<u32, ExecutionError> {
	u32::try_from(row).map_err(|_| ExecutionError::Kernel {
		message: "join build row index exceeds UInt32 take index".to_string(),
	})
}

fn join_probe_row_index(row: usize) -> Result<u32, ExecutionError> {
	u32::try_from(row).map_err(|_| ExecutionError::Kernel {
		message: "join probe row index exceeds UInt32 take index".to_string(),
	})
}

fn join_build_memory_fits(key_arrays: &[ArrayRef], rows: usize, memory_cap: u64) -> bool {
	estimated_build_hash_bytes(key_arrays, rows) <= memory_cap
}

#[allow(clippy::too_many_arguments)]
async fn execute_spilled_hash_join(
	kind: JoinKind,
	left_batch: &RecordBatch,
	right_batch: &RecordBatch,
	left_key_arrays: &[ArrayRef],
	right_key_arrays: &[ArrayRef],
	output_schema: &arrow::datatypes::SchemaRef,
	match_filter: Option<&BoundExprProgram>,
	null_safe_keys: bool,
	task_budget: TaskBudget,
	telemetry: Option<Arc<Mutex<TaskTelemetry>>>,
	spill_file_ops: Arc<dyn AsyncFileOps>,
	spill_dir: PathBuf,
) -> Result<Vec<RecordBatch>, ExecutionError> {
	let mut build_rows: HashMap<Vec<u8>, Vec<u32>> = HashMap::new();
	let build_started = Instant::now();
	for row in 0..left_batch.num_rows() {
		let key = if null_safe_keys {
			join_key_bytes_null_safe(left_key_arrays, row)?
		} else {
			let Some(key) = join_key_bytes(left_key_arrays, row)? else {
				continue;
			};
			key
		};
		build_rows
			.entry(key)
			.or_default()
			.push(u32::try_from(row).map_err(|_| ExecutionError::Kernel {
				message: "join build row index exceeds UInt32 take index".to_string(),
			})?);
	}
	record_task_telemetry(&telemetry, |task| {
		task.phases.build_micros = task
			.phases
			.build_micros
			.saturating_add(duration_micros(build_started));
	});
	if build_rows.is_empty() {
		let mut selection = Vec::new();
		for row in 0..right_batch.num_rows() {
			append_unmatched_probe_row(kind, row, &mut selection)?;
		}
		append_build_side_after_probe_rows(
			kind,
			left_batch.num_rows(),
			build_match_bitmap(kind, left_batch.num_rows()).as_deref(),
			&mut selection,
		)?;
		return Ok(vec![materialize_join_selection(
			kind,
			left_batch,
			right_batch,
			&selection,
			output_schema,
			task_budget.memory.get(),
		)?]);
	}

	guard_spill_retained_entry_memory(
		left_batch,
		right_batch,
		&build_rows,
		task_budget.memory.get(),
	)?;

	let spill_started = Instant::now();
	let mut spilled =
		SpilledJoinBuild::write(build_rows, task_budget, spill_file_ops, spill_dir).await?;
	record_task_telemetry(&telemetry, |task| {
		task.phases.spill_micros = task
			.phases
			.spill_micros
			.saturating_add(duration_micros(spill_started));
		task.counters.spill_bytes = task
			.counters
			.spill_bytes
			.saturating_add(spilled.spill_bytes());
	});
	let probe_started = Instant::now();
	let result = probe_spilled_hash_join(
		kind,
		&mut spilled,
		right_batch,
		right_key_arrays,
		left_batch,
		output_schema,
		match_filter,
		null_safe_keys,
		task_budget.memory.get(),
	)
	.await;
	record_task_telemetry(&telemetry, |task| {
		task.phases.probe_micros = task
			.phases
			.probe_micros
			.saturating_add(duration_micros(probe_started));
		if let Ok(batches) = &result {
			let matched_rows = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
			task.counters.matched_rows = task
				.counters
				.matched_rows
				.saturating_add(matched_rows as u64);
		}
	});
	spilled.cleanup().await;
	result
}

#[allow(clippy::too_many_arguments)]
async fn execute_spilled_hash_join_into_global_aggregate(
	executor: &Executor,
	kind: JoinKind,
	left_batch: &RecordBatch,
	right_batch: &RecordBatch,
	left_key_arrays: &[ArrayRef],
	right_key_arrays: &[ArrayRef],
	join_output_schema: &arrow::datatypes::SchemaRef,
	filter_predicate: Option<&BoundExprProgram>,
	aggr_expr: &[BoundAggregateCall],
	aggregate_state: &mut [AggState],
	match_filter: Option<&BoundExprProgram>,
	null_safe_keys: bool,
	task_budget: TaskBudget,
	telemetry: Option<Arc<Mutex<TaskTelemetry>>>,
	spill_file_ops: Arc<dyn AsyncFileOps>,
	spill_dir: PathBuf,
) -> Result<u64, ExecutionError> {
	let mut build_rows: HashMap<Vec<u8>, Vec<u32>> = HashMap::new();
	let build_started = Instant::now();
	for row in 0..left_batch.num_rows() {
		let key = if null_safe_keys {
			join_key_bytes_null_safe(left_key_arrays, row)?
		} else {
			let Some(key) = join_key_bytes(left_key_arrays, row)? else {
				continue;
			};
			key
		};
		build_rows
			.entry(key)
			.or_default()
			.push(u32::try_from(row).map_err(|_| ExecutionError::Kernel {
				message: "join build row index exceeds UInt32 take index".to_string(),
			})?);
	}
	record_task_telemetry(&telemetry, |task| {
		task.phases.build_micros = task
			.phases
			.build_micros
			.saturating_add(duration_micros(build_started));
	});
	if build_rows.is_empty() {
		let mut selection = Vec::new();
		for row in 0..right_batch.num_rows() {
			append_unmatched_probe_row(kind, row, &mut selection)?;
		}
		append_build_side_after_probe_rows(
			kind,
			left_batch.num_rows(),
			build_match_bitmap(kind, left_batch.num_rows()).as_deref(),
			&mut selection,
		)?;
		let output_rows = selection.len() as u64;
		fold_join_selection_chunks_into_global_aggregate(
			executor,
			kind,
			left_batch,
			right_batch,
			&selection,
			join_output_schema,
			filter_predicate,
			aggr_expr,
			aggregate_state,
			task_budget.memory.get(),
		)?;
		return Ok(output_rows);
	}

	guard_spill_retained_entry_memory(
		left_batch,
		right_batch,
		&build_rows,
		task_budget.memory.get(),
	)?;

	let spill_started = Instant::now();
	let mut spilled =
		SpilledJoinBuild::write(build_rows, task_budget, spill_file_ops, spill_dir).await?;
	record_task_telemetry(&telemetry, |task| {
		task.phases.spill_micros = task
			.phases
			.spill_micros
			.saturating_add(duration_micros(spill_started));
		task.counters.spill_bytes = task
			.counters
			.spill_bytes
			.saturating_add(spilled.spill_bytes());
	});
	let probe_started = Instant::now();
	let result = probe_spilled_hash_join_into_global_aggregate(
		executor,
		kind,
		&mut spilled,
		right_batch,
		right_key_arrays,
		left_batch,
		join_output_schema,
		filter_predicate,
		aggr_expr,
		aggregate_state,
		match_filter,
		null_safe_keys,
		task_budget.memory.get(),
	)
	.await;
	record_task_telemetry(&telemetry, |task| {
		task.phases.probe_micros = task
			.phases
			.probe_micros
			.saturating_add(duration_micros(probe_started));
		if let Ok(output_rows) = &result {
			task.counters.matched_rows = task.counters.matched_rows.saturating_add(*output_rows);
		}
	});
	spilled.cleanup().await;
	result
}

async fn probe_spilled_hash_join(
	kind: JoinKind,
	spilled: &mut SpilledJoinBuild,
	right_batch: &RecordBatch,
	right_key_arrays: &[ArrayRef],
	left_batch: &RecordBatch,
	output_schema: &arrow::datatypes::SchemaRef,
	match_filter: Option<&BoundExprProgram>,
	null_safe_keys: bool,
	memory_cap: u64,
) -> Result<Vec<RecordBatch>, ExecutionError> {
	let mut selection = Vec::new();
	let mut matched_build_rows = build_match_bitmap(kind, left_batch.num_rows());
	let mut residual = JoinMatchResidual::new(match_filter, left_batch, right_batch);
	for row in 0..right_batch.num_rows() {
		let key = if null_safe_keys {
			join_key_bytes_null_safe(right_key_arrays, row)?
		} else {
			let Some(key) = join_key_bytes(right_key_arrays, row)? else {
				append_unmatched_probe_row(kind, row, &mut selection)?;
				continue;
			};
			key
		};
		let matches = spilled.lookup(&key).await?;
		let matched = append_join_matches_for_kind(
			kind,
			row,
			matches.into_iter(),
			matched_build_rows.as_deref_mut(),
			&mut selection,
			&mut residual,
		)?;
		if !matched {
			append_unmatched_probe_row(kind, row, &mut selection)?;
		}
	}
	append_build_side_after_probe_rows(
		kind,
		left_batch.num_rows(),
		matched_build_rows.as_deref(),
		&mut selection,
	)?;

	Ok(vec![materialize_join_selection(
		kind,
		left_batch,
		right_batch,
		&selection,
		output_schema,
		memory_cap,
	)?])
}

#[allow(clippy::too_many_arguments)]
async fn probe_spilled_hash_join_into_global_aggregate(
	executor: &Executor,
	kind: JoinKind,
	spilled: &mut SpilledJoinBuild,
	right_batch: &RecordBatch,
	right_key_arrays: &[ArrayRef],
	left_batch: &RecordBatch,
	join_output_schema: &arrow::datatypes::SchemaRef,
	filter_predicate: Option<&BoundExprProgram>,
	aggr_expr: &[BoundAggregateCall],
	aggregate_state: &mut [AggState],
	match_filter: Option<&BoundExprProgram>,
	null_safe_keys: bool,
	memory_cap: u64,
) -> Result<u64, ExecutionError> {
	let mut selection = Vec::new();
	let mut matched_build_rows = build_match_bitmap(kind, left_batch.num_rows());
	let mut residual = JoinMatchResidual::new(match_filter, left_batch, right_batch);
	for row in 0..right_batch.num_rows() {
		let key = if null_safe_keys {
			join_key_bytes_null_safe(right_key_arrays, row)?
		} else {
			let Some(key) = join_key_bytes(right_key_arrays, row)? else {
				append_unmatched_probe_row(kind, row, &mut selection)?;
				continue;
			};
			key
		};
		let matches = spilled.lookup(&key).await?;
		let matched = append_join_matches_for_kind(
			kind,
			row,
			matches.into_iter(),
			matched_build_rows.as_deref_mut(),
			&mut selection,
			&mut residual,
		)?;
		if !matched {
			append_unmatched_probe_row(kind, row, &mut selection)?;
		}
	}
	append_build_side_after_probe_rows(
		kind,
		left_batch.num_rows(),
		matched_build_rows.as_deref(),
		&mut selection,
	)?;

	let output_rows = selection.len() as u64;
	fold_join_selection_chunks_into_global_aggregate(
		executor,
		kind,
		left_batch,
		right_batch,
		&selection,
		join_output_schema,
		filter_predicate,
		aggr_expr,
		aggregate_state,
		memory_cap,
	)?;
	Ok(output_rows)
}

#[allow(clippy::too_many_arguments)]
fn fold_join_selection_chunks_into_global_aggregate(
	executor: &Executor,
	kind: JoinKind,
	left_batch: &RecordBatch,
	right_batch: &RecordBatch,
	selection: &[JoinOutputRow],
	join_output_schema: &arrow::datatypes::SchemaRef,
	filter_predicate: Option<&BoundExprProgram>,
	aggr_expr: &[BoundAggregateCall],
	aggregate_state: &mut [AggState],
	memory_cap: u64,
) -> Result<(), ExecutionError> {
	for chunk in selection.chunks(CROSS_JOIN_OUTPUT_CHUNK_ROWS) {
		let batch = materialize_join_selection(
			kind,
			left_batch,
			right_batch,
			chunk,
			join_output_schema,
			memory_cap,
		)?;
		let batch = if let Some(predicate) = filter_predicate {
			executor.filter_batch_with_bound_predicate(predicate, batch, "filter predicate")?
		} else {
			batch
		};
		executor.fold_global_aggregate_state(aggregate_state, aggr_expr, &batch)?;
	}
	Ok(())
}

#[allow(clippy::too_many_arguments)]
fn visit_join_selection_filtered_chunks<F>(
	executor: &Executor,
	kind: JoinKind,
	left_batch: &RecordBatch,
	right_batch: &RecordBatch,
	selection: &[JoinOutputRow],
	join_output_schema: &arrow::datatypes::SchemaRef,
	filter_predicate: &BoundExprProgram,
	memory_cap: u64,
	visit_filtered_batch: &mut F,
) -> Result<(), ExecutionError>
where
	F: FnMut(RecordBatch) -> Result<(), ExecutionError>,
{
	for chunk in selection.chunks(CROSS_JOIN_OUTPUT_CHUNK_ROWS) {
		let batch = materialize_join_selection(
			kind,
			left_batch,
			right_batch,
			chunk,
			join_output_schema,
			memory_cap,
		)?;
		let batch = executor.filter_batch_with_bound_predicate(
			filter_predicate,
			batch,
			"filter predicate",
		)?;
		if batch.num_rows() > 0 {
			visit_filtered_batch(batch)?;
		}
	}
	Ok(())
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
	if estimate > task_budget.memory.get() {
		return Err(ExecutionError::BudgetExhausted {
			dimension: BudgetDimension::Memory,
			producer: "sort materialization",
			requested: ByteCount::new(estimate),
			available: task_budget.memory,
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
		ArithmeticOp::Add
	} else {
		ArithmeticOp::Sub
	};
	let offset_array = literal_array(offset, key.len())?;
	let bound_values = evaluate_unbound_arithmetic(op, key.clone(), offset_array)?;
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
			let default_array = literal_array(default, rows)?;
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

fn key_arrays(
	batch: &RecordBatch,
	keys: &[crate::ir::ColumnOrdinal],
) -> Result<Vec<ArrayRef>, ExecutionError> {
	keys.iter()
		.map(|key| {
			let ordinal = usize::try_from(key.0).expect("column ordinal fits usize");
			batch
				.columns()
				.get(ordinal)
				.cloned()
				.ok_or_else(|| ExecutionError::InvalidExpression {
					message: format!("join key column ordinal {ordinal} out of bounds"),
				})
		})
		.collect()
}

fn join_key_bytes(key_arrays: &[ArrayRef], row: usize) -> Result<Option<Vec<u8>>, ExecutionError> {
	let mut encoded = Vec::new();
	for array in key_arrays {
		if array.is_null(row) {
			return Ok(None);
		}
		encode_partition_key_value(array.as_ref(), row, &mut encoded)?;
	}
	Ok(Some(encoded))
}

fn join_key_bytes_null_safe(
	key_arrays: &[ArrayRef],
	row: usize,
) -> Result<Vec<u8>, ExecutionError> {
	let mut encoded = Vec::new();
	for array in key_arrays {
		encode_partition_key_value(array.as_ref(), row, &mut encoded)?;
	}
	Ok(encoded)
}

fn estimated_build_hash_bytes(key_arrays: &[ArrayRef], rows: usize) -> u64 {
	let key_width = key_arrays
		.iter()
		.map(|array| match array.data_type() {
			DataType::Boolean | DataType::Int8 | DataType::UInt8 => 1,
			DataType::Int16 | DataType::UInt16 => 2,
			DataType::Int32 | DataType::UInt32 | DataType::Float32 => 4,
			DataType::Date32 => 4,
			DataType::Int64 | DataType::UInt64 | DataType::Float64 | DataType::Timestamp(_, _) => 8,
			DataType::Utf8 => 32,
			_ => 16,
		})
		.sum::<usize>();
	u64::try_from(rows.saturating_mul(112 + key_width)).unwrap_or(u64::MAX)
}

fn estimate_spill_retained_entry_bytes(
	left_batch: &RecordBatch,
	right_batch: &RecordBatch,
	build_rows: &HashMap<Vec<u8>, Vec<u32>>,
) -> u64 {
	record_batch_memory_bytes(left_batch)
		.saturating_add(record_batch_memory_bytes(right_batch))
		.saturating_add(estimate_spill_build_map_bytes(build_rows))
}

fn guard_spill_retained_entry_memory(
	left_batch: &RecordBatch,
	right_batch: &RecordBatch,
	build_rows: &HashMap<Vec<u8>, Vec<u32>>,
	memory_cap: u64,
) -> Result<(), ExecutionError> {
	let retained_bytes = estimate_spill_retained_entry_bytes(left_batch, right_batch, build_rows);
	let retained_cap = join_spill_retained_footprint_memory_cap(memory_cap);
	if retained_bytes > retained_cap {
		return Err(budget_exhausted(
			BudgetDimension::Memory,
			"join spill retained entry",
			retained_bytes,
			retained_cap,
		));
	}
	Ok(())
}

fn join_spill_retained_footprint_memory_cap(memory_cap: u64) -> u64 {
	memory_cap.saturating_mul(JOIN_SPILL_RETAINED_FOOTPRINT_MEMORY_CAP_MULTIPLIER)
}

fn record_batch_memory_bytes(batch: &RecordBatch) -> u64 {
	u64::try_from(batch.get_array_memory_size()).unwrap_or(u64::MAX)
}

fn estimate_spill_build_map_bytes(build_rows: &HashMap<Vec<u8>, Vec<u32>>) -> u64 {
	let bucket_bytes = build_rows
		.capacity()
		.saturating_mul(std::mem::size_of::<(Vec<u8>, Vec<u32>)>().saturating_add(16));
	build_rows
		.iter()
		.map(|(key, rows)| {
			key.capacity()
				.saturating_add(rows.capacity().saturating_mul(std::mem::size_of::<u32>()))
		})
		.fold(
			u64::try_from(bucket_bytes).unwrap_or(u64::MAX),
			|total, bytes| total.saturating_add(u64::try_from(bytes).unwrap_or(u64::MAX)),
		)
}

#[derive(Debug, Clone, Copy)]
struct JoinSpillPage {
	offset: u64,
	ciphertext_len: usize,
	plaintext_len: usize,
}

#[derive(Debug, Clone)]
struct CachedJoinSpillPage {
	bytes: Vec<u8>,
}

struct SpilledJoinBuild {
	spill_id: u64,
	path: PathBuf,
	file_ops: Arc<dyn AsyncFileOps>,
	file: FileHandle,
	key: LessSafeKey,
	pages: Vec<JoinSpillPage>,
	index: HashMap<Vec<u8>, Vec<usize>>,
	cache: HashMap<usize, CachedJoinSpillPage>,
	lru: VecDeque<usize>,
	cache_bytes: u64,
	cache_cap: u64,
}

impl SpilledJoinBuild {
	async fn write(
		build_rows: HashMap<Vec<u8>, Vec<u32>>,
		task_budget: TaskBudget,
		file_ops: Arc<dyn AsyncFileOps>,
		spill_dir: PathBuf,
	) -> Result<Self, ExecutionError> {
		let spill_id = NEXT_JOIN_SPILL_ID.fetch_add(1, Ordering::SeqCst);
		let path = spill_dir.join(format!("spill_{}_{}.join", std::process::id(), spill_id));
		match Self::write_inner(
			spill_id,
			path.clone(),
			build_rows,
			task_budget,
			file_ops.clone(),
		)
		.await
		{
			Ok(spill) => Ok(spill),
			Err(error) => {
				let _ = file_ops.unlink(&path).await;
				Err(error)
			}
		}
	}

	async fn write_inner(
		spill_id: u64,
		path: PathBuf,
		build_rows: HashMap<Vec<u8>, Vec<u32>>,
		task_budget: TaskBudget,
		file_ops: Arc<dyn AsyncFileOps>,
	) -> Result<Self, ExecutionError> {
		let spill_dir = path.parent().ok_or_else(|| ExecutionError::Kernel {
			message: "join spill path has no parent directory".to_string(),
		})?;
		file_ops
			.create_dir_all(spill_dir)
			.await
			.map_err(|error| join_spill_io_error("create_dir_all", error))?;
		let mut key_bytes = [0u8; 32];
		aws_lc_rs::rand::fill(&mut key_bytes).map_err(|error| ExecutionError::Kernel {
			message: format!("join spill key generation failed: {error:?}"),
		})?;
		let unbound = UnboundKey::new(&AES_256_GCM_SIV, &key_bytes).map_err(|error| {
			ExecutionError::Kernel {
				message: format!("join spill key construction failed: {error:?}"),
			}
		})?;
		let key = LessSafeKey::new(unbound);
		let file = file_ops
			.open(
				&path,
				OpenFlags {
					read: true,
					write: true,
					create_new: true,
					..OpenFlags::default()
				},
			)
			.await
			.map_err(|error| ExecutionError::Kernel {
				message: format!("join spill open failed: {error}"),
			})?;
		let mut pages = Vec::new();
		let mut index: HashMap<Vec<u8>, Vec<usize>> = HashMap::new();
		let mut current_entries: Vec<(Vec<u8>, Vec<u32>)> = Vec::new();
		let mut current_size = std::mem::size_of::<u32>();
		let mut spill_bytes = 0u64;
		let mut writer_peak = 0u64;
		let mut next_offset = 0u64;

		for (join_key, rows) in build_rows {
			let entry_size = encoded_join_spill_entry_size(&join_key, &rows);
			if !current_entries.is_empty()
				&& current_size.saturating_add(entry_size) > JOIN_SPILL_PAGE_SIZE
			{
				let page_index = pages.len();
				let page = write_join_spill_page(
					file_ops.as_ref(),
					&file,
					&key,
					spill_id,
					page_index,
					next_offset,
					&current_entries,
				)
				.await?;
				spill_bytes = spill_bytes.saturating_add(page.ciphertext_len as u64);
				writer_peak = writer_peak.max(page.plaintext_len as u64);
				if spill_bytes > task_budget.spill_bytes.get() {
					return Err(budget_exhausted(
						BudgetDimension::SpillBytes,
						"join spill writer",
						spill_bytes,
						task_budget.spill_bytes.get(),
					));
				}
				for (key, _) in &current_entries {
					index.entry(key.clone()).or_default().push(page_index);
				}
				next_offset = next_offset.saturating_add(page.ciphertext_len as u64);
				pages.push(page);
				current_entries.clear();
				current_size = std::mem::size_of::<u32>();
			}

			current_size = current_size.saturating_add(entry_size);
			current_entries.push((join_key, rows));
		}

		if !current_entries.is_empty() {
			let page_index = pages.len();
			let page = write_join_spill_page(
				file_ops.as_ref(),
				&file,
				&key,
				spill_id,
				page_index,
				next_offset,
				&current_entries,
			)
			.await?;
			spill_bytes = spill_bytes.saturating_add(page.ciphertext_len as u64);
			writer_peak = writer_peak.max(page.plaintext_len as u64);
			if spill_bytes > task_budget.spill_bytes.get() {
				return Err(budget_exhausted(
					BudgetDimension::SpillBytes,
					"join spill writer",
					spill_bytes,
					task_budget.spill_bytes.get(),
				));
			}
			for (key, _) in &current_entries {
				index.entry(key.clone()).or_default().push(page_index);
			}
			next_offset = next_offset.saturating_add(page.ciphertext_len as u64);
			pages.push(page);
		}
		file_ops
			.set_len(&file, next_offset)
			.await
			.map_err(|error| join_spill_io_error("set_len", error))?;
		file_ops
			.fdatasync(&file)
			.await
			.map_err(|error| join_spill_io_error("fdatasync", error))?;

		let index_bytes = join_spill_index_bytes(&index);
		let resident_without_cache = index_bytes
			.saturating_add(writer_peak)
			.saturating_add(JOIN_SPILL_PAGE_SIZE as u64);
		if resident_without_cache > task_budget.memory.get() {
			return Err(budget_exhausted(
				BudgetDimension::Memory,
				"join spill writer resident state",
				resident_without_cache,
				task_budget.memory.get(),
			));
		}
		let cache_cap = JOIN_SPILL_CACHE_TARGET_BYTES.min(
			task_budget
				.memory
				.get()
				.saturating_sub(resident_without_cache),
		);

		Ok(Self {
			spill_id,
			path,
			file_ops,
			file,
			key,
			pages,
			index,
			cache: HashMap::new(),
			lru: VecDeque::new(),
			cache_bytes: 0,
			cache_cap,
		})
	}

	fn spill_bytes(&self) -> u64 {
		self.pages
			.iter()
			.map(|page| page.ciphertext_len as u64)
			.sum()
	}

	async fn lookup(&mut self, join_key: &[u8]) -> Result<Vec<u32>, ExecutionError> {
		let Some(page_indices) = self.index.get(join_key).cloned() else {
			return Ok(Vec::new());
		};
		let mut rows = Vec::new();
		for page_index in page_indices {
			let page = self.read_page(page_index).await?;
			rows.extend(decode_join_spill_page_for_key(&page, join_key)?);
		}
		Ok(rows)
	}

	async fn read_page(&mut self, page_index: usize) -> Result<Vec<u8>, ExecutionError> {
		if let Some(cached) = self.cache.get(&page_index) {
			let bytes = cached.bytes.clone();
			self.touch_cache_entry(page_index);
			return Ok(bytes);
		}

		let page = self
			.pages
			.get(page_index)
			.copied()
			.ok_or_else(|| ExecutionError::Kernel {
				message: format!("join spill page {page_index} is missing"),
			})?;
		let mut ciphertext = read_plain_exact(
			self.file_ops.as_ref(),
			&self.file,
			page.offset,
			page.ciphertext_len,
		)
		.await?;
		let nonce = Nonce::assume_unique_for_key(join_spill_nonce(self.spill_id, page_index));
		let plaintext = self
			.key
			.open_in_place(nonce, Aad::empty(), &mut ciphertext)
			.map_err(|error| ExecutionError::Kernel {
				message: format!("join spill decrypt failed: {error:?}"),
			})?
			.to_vec();
		self.insert_cache_entry(page_index, plaintext.clone());
		Ok(plaintext)
	}

	async fn cleanup(self) {
		let file_ops = self.file_ops.clone();
		let path = self.path.clone();
		drop(self);
		let _ = file_ops.unlink(&path).await;
	}

	fn touch_cache_entry(&mut self, page_index: usize) {
		self.lru.retain(|cached| *cached != page_index);
		self.lru.push_back(page_index);
	}

	fn insert_cache_entry(&mut self, page_index: usize, bytes: Vec<u8>) {
		let bytes_len = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
		if self.cache_cap == 0 || bytes_len > self.cache_cap {
			return;
		}
		while self.cache_bytes.saturating_add(bytes_len) > self.cache_cap {
			let Some(evicted) = self.lru.pop_front() else {
				break;
			};
			if let Some(page) = self.cache.remove(&evicted) {
				self.cache_bytes = self
					.cache_bytes
					.saturating_sub(u64::try_from(page.bytes.len()).unwrap_or(u64::MAX));
			}
		}
		self.cache_bytes = self.cache_bytes.saturating_add(bytes_len);
		self.cache.insert(page_index, CachedJoinSpillPage { bytes });
		self.touch_cache_entry(page_index);
	}
}

#[derive(Debug, Clone, Copy)]
struct SpilledExistenceRowRef {
	page_index: usize,
	row: u32,
}

struct SpilledSemiAntiExistenceBuild {
	spill_id: u64,
	path: PathBuf,
	file_ops: Arc<dyn AsyncFileOps>,
	file: FileHandle,
	key: LessSafeKey,
	pages: Vec<JoinSpillPage>,
	index: HashMap<JoinKey, Vec<SpilledExistenceRowRef>>,
	cache: HashMap<usize, CachedJoinSpillPage>,
	lru: VecDeque<usize>,
	cache_bytes: u64,
	cache_cap: u64,
	spill_bytes: u64,
	writer_peak: u64,
	rows: u64,
	input_rows: u64,
	input_batches_written: u64,
	next_offset: u64,
}

impl SpilledSemiAntiExistenceBuild {
	async fn create(
		task_budget: TaskBudget,
		file_ops: Arc<dyn AsyncFileOps>,
		spill_dir: PathBuf,
	) -> Result<Self, ExecutionError> {
		let spill_id = NEXT_JOIN_SPILL_ID.fetch_add(1, Ordering::SeqCst);
		let path = spill_dir.join(format!(
			"spill_{}_{}.existence",
			std::process::id(),
			spill_id
		));
		match Self::create_inner(spill_id, path.clone(), task_budget, file_ops.clone()).await {
			Ok(spill) => Ok(spill),
			Err(error) => {
				let _ = file_ops.unlink(&path).await;
				Err(error)
			}
		}
	}

	async fn create_inner(
		spill_id: u64,
		path: PathBuf,
		task_budget: TaskBudget,
		file_ops: Arc<dyn AsyncFileOps>,
	) -> Result<Self, ExecutionError> {
		let spill_dir = path.parent().ok_or_else(|| ExecutionError::Kernel {
			message: "semi/anti existence spill path has no parent directory".to_string(),
		})?;
		file_ops
			.create_dir_all(spill_dir)
			.await
			.map_err(|error| join_spill_io_error("create_dir_all", error))?;
		let mut key_bytes = [0u8; 32];
		aws_lc_rs::rand::fill(&mut key_bytes).map_err(|error| ExecutionError::Kernel {
			message: format!("semi/anti existence spill key generation failed: {error:?}"),
		})?;
		let unbound = UnboundKey::new(&AES_256_GCM_SIV, &key_bytes).map_err(|error| {
			ExecutionError::Kernel {
				message: format!("semi/anti existence spill key construction failed: {error:?}"),
			}
		})?;
		let key = LessSafeKey::new(unbound);
		let file = file_ops
			.open(
				&path,
				OpenFlags {
					read: true,
					write: true,
					create_new: true,
					..OpenFlags::default()
				},
			)
			.await
			.map_err(|error| ExecutionError::Kernel {
				message: format!("semi/anti existence spill open failed: {error}"),
			})?;
		let cache_cap = JOIN_SPILL_CACHE_TARGET_BYTES.min(task_budget.memory.get());
		Ok(Self {
			spill_id,
			path,
			file_ops,
			file,
			key,
			pages: Vec::new(),
			index: HashMap::new(),
			cache: HashMap::new(),
			lru: VecDeque::new(),
			cache_bytes: 0,
			cache_cap,
			spill_bytes: 0,
			writer_peak: 0,
			rows: 0,
			input_rows: 0,
			input_batches_written: 0,
			next_offset: 0,
		})
	}

	async fn write_batch_chunks(
		&mut self,
		batch: &RecordBatch,
		key_plan: JoinKeyPlan,
		existence_keys: &[crate::ir::ColumnOrdinal],
		task_budget: TaskBudget,
	) -> Result<(), ExecutionError> {
		if batch.num_rows() == 0 {
			return Ok(());
		}
		self.input_batches_written = self.input_batches_written.saturating_add(1);
		self.input_rows = self.input_rows.saturating_add(batch.num_rows() as u64);
		for offset in (0..batch.num_rows()).step_by(CROSS_JOIN_OUTPUT_CHUNK_ROWS) {
			let len = CROSS_JOIN_OUTPUT_CHUNK_ROWS.min(batch.num_rows() - offset);
			let chunk = batch.slice(offset, len);
			self.write_page_batch(&chunk, key_plan, existence_keys, task_budget)
				.await?;
		}
		Ok(())
	}

	async fn write_page_batch(
		&mut self,
		batch: &RecordBatch,
		key_plan: JoinKeyPlan,
		existence_keys: &[crate::ir::ColumnOrdinal],
		task_budget: TaskBudget,
	) -> Result<(), ExecutionError> {
		if batch.num_rows() == 0 {
			return Ok(());
		}
		let key_arrays = key_arrays(batch, existence_keys)?;
		let mut refs = Vec::new();
		for row in 0..batch.num_rows() {
			let Some(key) = join_key_at(key_plan, &key_arrays, row, false)? else {
				continue;
			};
			refs.push((key, join_build_row_index(row)?));
		}
		if refs.is_empty() {
			return Ok(());
		}

		let page_index = self.pages.len();
		let plaintext = encode_record_batch_ipc(batch)?;
		let page = write_join_spill_plaintext_page(
			self.file_ops.as_ref(),
			&self.file,
			&self.key,
			self.spill_id,
			page_index,
			self.next_offset,
			plaintext,
		)
		.await?;
		self.spill_bytes = self.spill_bytes.saturating_add(page.ciphertext_len as u64);
		self.writer_peak = self.writer_peak.max(page.plaintext_len as u64);
		if self.spill_bytes > task_budget.spill_bytes.get() {
			return Err(budget_exhausted(
				BudgetDimension::SpillBytes,
				"semi/anti existence-set spill writer",
				self.spill_bytes,
				task_budget.spill_bytes.get(),
			));
		}
		for (key, row) in refs {
			self.index
				.entry(key)
				.or_default()
				.push(SpilledExistenceRowRef { page_index, row });
			self.rows = self.rows.saturating_add(1);
		}
		self.next_offset = self.next_offset.saturating_add(page.ciphertext_len as u64);
		self.pages.push(page);
		self.guard_resident_state(task_budget.memory.get())?;
		Ok(())
	}

	async fn finish(&self, task_budget: TaskBudget) -> Result<(), ExecutionError> {
		self.file_ops
			.set_len(&self.file, self.next_offset)
			.await
			.map_err(|error| join_spill_io_error("set_len", error))?;
		self.file_ops
			.fdatasync(&self.file)
			.await
			.map_err(|error| join_spill_io_error("fdatasync", error))?;
		self.guard_resident_state(task_budget.memory.get())
	}

	fn guard_resident_state(&self, memory_cap: u64) -> Result<(), ExecutionError> {
		let resident = semi_anti_existence_spill_index_bytes(&self.index)
			.saturating_add(self.writer_peak)
			.saturating_add(JOIN_SPILL_PAGE_SIZE as u64);
		let cap = join_spill_retained_footprint_memory_cap(memory_cap);
		if resident > cap {
			return Err(budget_exhausted(
				BudgetDimension::Memory,
				"semi/anti existence-set spill resident state",
				resident,
				cap,
			));
		}
		Ok(())
	}

	#[cfg(test)]
	fn row_count(&self) -> u64 {
		self.rows
	}

	fn input_row_count(&self) -> u64 {
		self.input_rows
	}

	fn spill_bytes(&self) -> u64 {
		self.spill_bytes
	}

	#[cfg(test)]
	fn input_batches_written(&self) -> u64 {
		self.input_batches_written
	}

	async fn matching_preserved_rows(
		&mut self,
		preserved_batch: &RecordBatch,
		preserved_by_key: &HashMap<JoinKey, Vec<u32>>,
		match_filter: &BoundExprProgram,
	) -> Result<Vec<bool>, ExecutionError> {
		let mut page_work: BTreeMap<usize, Vec<(Vec<u32>, Vec<u32>)>> = BTreeMap::new();
		for (key, preserved_rows) in preserved_by_key {
			let Some(row_refs) = self.index.get(key) else {
				continue;
			};
			let mut rows_by_page: BTreeMap<usize, Vec<u32>> = BTreeMap::new();
			for row_ref in row_refs {
				rows_by_page
					.entry(row_ref.page_index)
					.or_default()
					.push(row_ref.row);
			}
			for (page_index, existence_rows) in rows_by_page {
				page_work
					.entry(page_index)
					.or_default()
					.push((preserved_rows.clone(), existence_rows));
			}
		}

		let mut matched = vec![false; preserved_batch.num_rows()];
		for (page_index, groups) in page_work {
			let existence_batch = self.read_batch_page(page_index).await?;
			let mut residual =
				JoinMatchResidual::new(Some(match_filter), preserved_batch, &existence_batch);
			for (preserved_rows, existence_rows) in groups {
				for preserved_row in preserved_rows {
					if matched[preserved_row as usize] {
						continue;
					}
					for existence_row in &existence_rows {
						if residual.matches(preserved_row, *existence_row)? {
							matched[preserved_row as usize] = true;
							break;
						}
					}
				}
			}
		}
		Ok(matched)
	}

	async fn read_batch_page(&mut self, page_index: usize) -> Result<RecordBatch, ExecutionError> {
		let bytes = self.read_page(page_index).await?;
		decode_record_batch_ipc(&bytes)
	}

	async fn read_page(&mut self, page_index: usize) -> Result<Vec<u8>, ExecutionError> {
		if let Some(cached) = self.cache.get(&page_index) {
			let bytes = cached.bytes.clone();
			self.touch_cache_entry(page_index);
			return Ok(bytes);
		}

		let page = self
			.pages
			.get(page_index)
			.copied()
			.ok_or_else(|| ExecutionError::Kernel {
				message: format!("semi/anti existence spill page {page_index} is missing"),
			})?;
		let mut ciphertext = read_plain_exact(
			self.file_ops.as_ref(),
			&self.file,
			page.offset,
			page.ciphertext_len,
		)
		.await?;
		let nonce = Nonce::assume_unique_for_key(join_spill_nonce(self.spill_id, page_index));
		let plaintext = self
			.key
			.open_in_place(nonce, Aad::empty(), &mut ciphertext)
			.map_err(|error| ExecutionError::Kernel {
				message: format!("semi/anti existence spill decrypt failed: {error:?}"),
			})?
			.to_vec();
		self.insert_cache_entry(page_index, plaintext.clone());
		Ok(plaintext)
	}

	async fn cleanup(self) {
		let file_ops = self.file_ops.clone();
		let path = self.path.clone();
		drop(self);
		let _ = file_ops.unlink(&path).await;
	}

	fn touch_cache_entry(&mut self, page_index: usize) {
		self.lru.retain(|cached| *cached != page_index);
		self.lru.push_back(page_index);
	}

	fn insert_cache_entry(&mut self, page_index: usize, bytes: Vec<u8>) {
		let bytes_len = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
		if self.cache_cap == 0 || bytes_len > self.cache_cap {
			return;
		}
		while self.cache_bytes.saturating_add(bytes_len) > self.cache_cap {
			let Some(evicted) = self.lru.pop_front() else {
				break;
			};
			if let Some(page) = self.cache.remove(&evicted) {
				self.cache_bytes = self
					.cache_bytes
					.saturating_sub(u64::try_from(page.bytes.len()).unwrap_or(u64::MAX));
			}
		}
		self.cache_bytes = self.cache_bytes.saturating_add(bytes_len);
		self.cache.insert(page_index, CachedJoinSpillPage { bytes });
		self.touch_cache_entry(page_index);
	}
}

async fn write_join_spill_page(
	file_ops: &dyn AsyncFileOps,
	file: &FileHandle,
	key: &LessSafeKey,
	spill_id: u64,
	page_index: usize,
	offset: u64,
	entries: &[(Vec<u8>, Vec<u32>)],
) -> Result<JoinSpillPage, ExecutionError> {
	let plaintext = encode_join_spill_page(entries)?;
	write_join_spill_plaintext_page(file_ops, file, key, spill_id, page_index, offset, plaintext)
		.await
}

async fn write_join_spill_plaintext_page(
	file_ops: &dyn AsyncFileOps,
	file: &FileHandle,
	key: &LessSafeKey,
	spill_id: u64,
	page_index: usize,
	offset: u64,
	mut plaintext: Vec<u8>,
) -> Result<JoinSpillPage, ExecutionError> {
	let plaintext_len = plaintext.len();
	let nonce = Nonce::assume_unique_for_key(join_spill_nonce(spill_id, page_index));
	key.seal_in_place_append_tag(nonce, Aad::empty(), &mut plaintext)
		.map_err(|error| ExecutionError::Kernel {
			message: format!("join spill encrypt failed: {error:?}"),
		})?;
	let ciphertext_len = plaintext.len();
	write_plain_all(file_ops, file, offset, plaintext).await?;
	Ok(JoinSpillPage {
		offset,
		ciphertext_len,
		plaintext_len,
	})
}

async fn write_plain_all(
	file_ops: &dyn AsyncFileOps,
	file: &FileHandle,
	offset: u64,
	bytes: Vec<u8>,
) -> Result<(), ExecutionError> {
	let mut written_total = 0usize;
	while written_total < bytes.len() {
		let written = file_ops
			.write_plain_at(
				file,
				offset.saturating_add(written_total as u64),
				bytes[written_total..].to_vec(),
			)
			.await
			.map_err(|error| join_spill_io_error("write", error))?;
		if written == 0 {
			return Err(join_spill_io_error(
				"write",
				std::io::Error::new(
					ErrorKind::WriteZero,
					"join spill write completed with zero bytes",
				),
			));
		}
		written_total = written_total.saturating_add(written);
	}
	Ok(())
}

async fn read_plain_exact(
	file_ops: &dyn AsyncFileOps,
	file: &FileHandle,
	offset: u64,
	len: usize,
) -> Result<Vec<u8>, ExecutionError> {
	let mut out = Vec::with_capacity(len);
	while out.len() < len {
		let chunk = file_ops
			.read_plain_at(
				file,
				offset.saturating_add(out.len() as u64),
				len.saturating_sub(out.len()),
			)
			.await
			.map_err(|error| join_spill_io_error("read", error))?;
		if chunk.is_empty() {
			return Err(join_spill_io_error(
				"read",
				std::io::Error::new(ErrorKind::UnexpectedEof, "join spill page truncated"),
			));
		}
		out.extend_from_slice(&chunk);
	}
	Ok(out)
}

fn join_spill_io_error(operation: &'static str, error: std::io::Error) -> ExecutionError {
	ExecutionError::Kernel {
		message: format!("join spill {operation} failed: {error}"),
	}
}

fn encode_join_spill_page(entries: &[(Vec<u8>, Vec<u32>)]) -> Result<Vec<u8>, ExecutionError> {
	let mut out = Vec::new();
	out.extend_from_slice(
		&u32::try_from(entries.len())
			.map_err(|_| ExecutionError::Kernel {
				message: "join spill page has too many entries".to_string(),
			})?
			.to_le_bytes(),
	);
	for (join_key, rows) in entries {
		out.extend_from_slice(
			&u32::try_from(join_key.len())
				.map_err(|_| ExecutionError::Kernel {
					message: "join spill key is too large".to_string(),
				})?
				.to_le_bytes(),
		);
		out.extend_from_slice(join_key);
		out.extend_from_slice(
			&u32::try_from(rows.len())
				.map_err(|_| ExecutionError::Kernel {
					message: "join spill row list is too large".to_string(),
				})?
				.to_le_bytes(),
		);
		for row in rows {
			out.extend_from_slice(&row.to_le_bytes());
		}
	}
	Ok(out)
}

fn decode_join_spill_page_for_key(
	page: &[u8],
	lookup_key: &[u8],
) -> Result<Vec<u32>, ExecutionError> {
	let mut cursor = 0usize;
	let entries = read_u32_le(page, &mut cursor)? as usize;
	let mut rows = Vec::new();
	for _ in 0..entries {
		let key_len = read_u32_le(page, &mut cursor)? as usize;
		let key_end = cursor
			.checked_add(key_len)
			.ok_or_else(join_spill_decode_error)?;
		let key = page
			.get(cursor..key_end)
			.ok_or_else(join_spill_decode_error)?;
		cursor = key_end;
		let row_count = read_u32_le(page, &mut cursor)? as usize;
		for _ in 0..row_count {
			let row = read_u32_le(page, &mut cursor)?;
			if key == lookup_key {
				rows.push(row);
			}
		}
	}
	Ok(rows)
}

fn encode_record_batch_ipc(batch: &RecordBatch) -> Result<Vec<u8>, ExecutionError> {
	let mut buffer = Vec::new();
	{
		let options =
			IpcWriteOptions::default().try_with_compression(Some(CompressionType::LZ4_FRAME))?;
		let mut writer = StreamWriter::try_new_with_options(&mut buffer, &batch.schema(), options)?;
		writer.write(batch)?;
		writer.finish()?;
	}
	Ok(buffer)
}

fn decode_record_batch_ipc(bytes: &[u8]) -> Result<RecordBatch, ExecutionError> {
	let cursor = Cursor::new(bytes);
	let mut reader = StreamReader::try_new(cursor, None)?;
	let Some(batch) = reader.next() else {
		return Err(ExecutionError::Kernel {
			message: "semi/anti existence spill page contained no batch".to_string(),
		});
	};
	let batch = batch?;
	if reader.next().is_some() {
		return Err(ExecutionError::Kernel {
			message: "semi/anti existence spill page contained multiple batches".to_string(),
		});
	}
	Ok(batch)
}

fn read_u32_le(page: &[u8], cursor: &mut usize) -> Result<u32, ExecutionError> {
	let end = cursor.checked_add(4).ok_or_else(join_spill_decode_error)?;
	let bytes = page.get(*cursor..end).ok_or_else(join_spill_decode_error)?;
	*cursor = end;
	Ok(u32::from_le_bytes(bytes.try_into().expect("four bytes")))
}

fn join_spill_decode_error() -> ExecutionError {
	ExecutionError::Kernel {
		message: "join spill page decode failed".to_string(),
	}
}

fn encoded_join_spill_entry_size(join_key: &[u8], rows: &[u32]) -> usize {
	std::mem::size_of::<u32>()
		.saturating_add(join_key.len())
		.saturating_add(std::mem::size_of::<u32>())
		.saturating_add(rows.len().saturating_mul(std::mem::size_of::<u32>()))
}

fn join_spill_index_bytes(index: &HashMap<Vec<u8>, Vec<usize>>) -> u64 {
	index
		.iter()
		.map(|(key, pages)| {
			u64::try_from(
				64usize
					.saturating_add(key.len())
					.saturating_add(pages.len().saturating_mul(std::mem::size_of::<usize>())),
			)
			.unwrap_or(u64::MAX)
		})
		.fold(0u64, u64::saturating_add)
}

fn semi_anti_existence_spill_index_bytes(
	index: &HashMap<JoinKey, Vec<SpilledExistenceRowRef>>,
) -> u64 {
	index
		.iter()
		.map(|(key, refs)| {
			u64::try_from(
				64usize
					.saturating_add(join_key_heap_bytes(key))
					.saturating_add(
						refs.len()
							.saturating_mul(std::mem::size_of::<SpilledExistenceRowRef>()),
					),
			)
			.unwrap_or(u64::MAX)
		})
		.fold(0u64, u64::saturating_add)
}

fn join_key_heap_bytes(key: &JoinKey) -> usize {
	match key {
		JoinKey::Int32(_) => std::mem::size_of::<i32>(),
		JoinKey::Int64(_) => std::mem::size_of::<i64>(),
		JoinKey::Generic(bytes) => bytes.len(),
	}
}

fn join_spill_nonce(spill_id: u64, page_index: usize) -> [u8; 12] {
	let mut nonce = [0u8; 12];
	nonce[..8].copy_from_slice(&spill_id.to_le_bytes());
	nonce[8..].copy_from_slice(&u32::try_from(page_index).unwrap_or(u32::MAX).to_le_bytes());
	nonce
}

fn budget_exhausted(
	dimension: BudgetDimension,
	source: &'static str,
	requested: u64,
	available: u64,
) -> ExecutionError {
	ExecutionError::BudgetExhausted {
		dimension,
		producer: source,
		requested: ByteCount::new(requested),
		available: ByteCount::new(available),
	}
}

pub(crate) fn partition_bucket(
	key_arrays: &[ArrayRef],
	row: usize,
	partition_count: u32,
) -> Result<usize, ExecutionError> {
	if partition_count == 0 {
		return Err(ExecutionError::InvalidExpression {
			message: "exchange partition_count must be greater than zero".to_string(),
		});
	}

	Ok((partition_key_hash(key_arrays, row)? % u64::from(partition_count)) as usize)
}

fn partition_key_hash(key_arrays: &[ArrayRef], row: usize) -> Result<u64, ExecutionError> {
	let mut encoded = Vec::new();
	for array in key_arrays {
		encode_partition_key_value(array.as_ref(), row, &mut encoded)?;
	}
	Ok(XxHash3_64::oneshot(&encoded))
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum GroupValue {
	Null,
	Boolean(bool),
	Int32(i32),
	Int64(i64),
	Float32(u32),
	Float64(u64),
	Date32(i32),
	Timestamp(TimeUnit, i64),
	Utf8(String),
	// ADR 275: a Decimal128 group key. The raw i128 is exact (no float-bit canonicalization), so equal Decimal
	// values group together and reconstruct losslessly.
	Decimal128(i128),
}

#[derive(Debug, Clone, Copy)]
enum AggState {
	Count(i64),
	SumInt {
		total: i128,
		seen: bool,
	},
	SumFloat {
		total: f64,
		seen: bool,
	},
	MinInt32 {
		value: Option<i32>,
	},
	MinInt64 {
		value: Option<i64>,
	},
	MinFloat32 {
		value: Option<f32>,
	},
	MinFloat64 {
		value: Option<f64>,
	},
	MinDecimal128 {
		value: Option<i128>,
	},
	MaxInt32 {
		value: Option<i32>,
	},
	MaxInt64 {
		value: Option<i64>,
	},
	MaxFloat32 {
		value: Option<f32>,
	},
	MaxFloat64 {
		value: Option<f64>,
	},
	MaxDecimal128 {
		value: Option<i128>,
	},
	AvgInt {
		total: i128,
		count: u64,
	},
	AvgFloat {
		total: f64,
		count: u64,
	},
	// ADR 268: AVG(Decimal) accumulates the raw unscaled i128 (like SumInt) and carries the input scale so
	// the finalize can rescale to the widened output scale before dividing.
	AvgDecimal {
		total: i128,
		count: u64,
		input_scale: i8,
	},
}

#[derive(Debug, Clone, Copy)]
enum NumericValue {
	Int(i128),
	Float(f64),
}

#[cfg(test)]
fn init_states(
	aggr_expr: &[(AggregateFunction, Vec<Expr>)],
	aggr_arrays: &[Option<ArrayRef>],
) -> Result<Vec<AggState>, ExecutionError> {
	aggr_expr
		.iter()
		.zip(aggr_arrays.iter())
		.map(|((function, _), array)| match function {
			AggregateFunction::CountAll | AggregateFunction::Count => Ok(AggState::Count(0)),
			AggregateFunction::Sum => Ok(array
				.as_ref()
				.map(|array| sum_state_for_data_type(array.data_type()))
				.unwrap_or(AggState::SumInt {
					total: 0,
					seen: false,
				})),
			AggregateFunction::Min => array
				.as_ref()
				.map(|array| min_state_for_data_type(array.data_type()))
				.transpose()
				.map(|state| state.unwrap_or(AggState::MinInt64 { value: None })),
			AggregateFunction::Max => array
				.as_ref()
				.map(|array| max_state_for_data_type(array.data_type()))
				.transpose()
				.map(|state| state.unwrap_or(AggState::MaxInt64 { value: None })),
			AggregateFunction::Avg => array
				.as_ref()
				.ok_or_else(|| ExecutionError::InvalidExpression {
					message: "AVG requires one argument".to_string(),
				})
				.and_then(|array| avg_state_for_data_type(array.data_type())),
			AggregateFunction::LinearRegress | AggregateFunction::ApproxQuantile(_) => {
				Ok(AggState::SumInt {
					total: 0,
					seen: false,
				})
			}
		})
		.collect()
}

#[cfg(test)]
fn legacy_aggregate_output_type(function: &AggregateFunction, input_type: &DataType) -> DataType {
	match function {
		AggregateFunction::CountAll | AggregateFunction::Count => DataType::Int64,
		AggregateFunction::Sum => match input_type {
			DataType::Float32 | DataType::Float64 => DataType::Float64,
			_ => DataType::Int64,
		},
		AggregateFunction::Min | AggregateFunction::Max => input_type.clone(),
		// ADR 268: delegate to the kernel so AVG(Decimal) yields the widened Decimal type, not Float64.
		AggregateFunction::Avg => {
			aggregate_kernel::stable_output_data_type(function, Some(input_type))
				.unwrap_or(DataType::Float64)
		}
		AggregateFunction::LinearRegress | AggregateFunction::ApproxQuantile(_) => {
			DataType::Float64
		}
	}
}

fn init_bound_states(aggr_expr: &[BoundAggregateCall]) -> Result<Vec<AggState>, ExecutionError> {
	aggr_expr.iter().map(init_bound_state_for_call).collect()
}

fn init_bound_state_for_call(call: &BoundAggregateCall) -> Result<AggState, ExecutionError> {
	match call.function {
		AggregateFunction::CountAll | AggregateFunction::Count => Ok(AggState::Count(0)),
		AggregateFunction::Sum => Ok(call
			.args
			.root_type(0)
			.map(sum_state_for_data_type)
			.unwrap_or(AggState::SumInt {
				total: 0,
				seen: false,
			})),
		AggregateFunction::Min => call
			.args
			.root_type(0)
			.map(min_state_for_data_type)
			.transpose()
			.map(|state| state.unwrap_or(AggState::MinInt64 { value: None })),
		AggregateFunction::Max => call
			.args
			.root_type(0)
			.map(max_state_for_data_type)
			.transpose()
			.map(|state| state.unwrap_or(AggState::MaxInt64 { value: None })),
		AggregateFunction::Avg => avg_state_for_data_type(first_arg_type(call)?),
		AggregateFunction::LinearRegress | AggregateFunction::ApproxQuantile(_) => {
			Ok(AggState::SumInt {
				total: 0,
				seen: false,
			})
		}
	}
}

fn first_arg_type(call: &BoundAggregateCall) -> Result<&DataType, ExecutionError> {
	call.args
		.root_type(0)
		.ok_or_else(|| ExecutionError::InvalidExpression {
			message: format!("{:?} requires one argument", call.function),
		})
}

fn group_value(array: &ArrayRef, row: usize) -> Result<GroupValue, ExecutionError> {
	if array.is_null(row) {
		return Ok(GroupValue::Null);
	}
	if let Some(array) = array.as_any().downcast_ref::<BooleanArray>() {
		return Ok(GroupValue::Boolean(array.value(row)));
	}
	if let Some(array) = array.as_any().downcast_ref::<Int32Array>() {
		return Ok(GroupValue::Int32(array.value(row)));
	}
	if let Some(array) = array.as_any().downcast_ref::<Int64Array>() {
		return Ok(GroupValue::Int64(array.value(row)));
	}
	if let Some(array) = array.as_any().downcast_ref::<Float32Array>() {
		return Ok(GroupValue::Float32(canonical_group_f32_bits(
			array.value(row),
		)));
	}
	if let Some(array) = array.as_any().downcast_ref::<Float64Array>() {
		return Ok(GroupValue::Float64(canonical_group_f64_bits(
			array.value(row),
		)));
	}
	if let Some(array) = array.as_any().downcast_ref::<Date32Array>() {
		return Ok(GroupValue::Date32(array.value(row)));
	}
	if let Some(array) = array.as_any().downcast_ref::<TimestampMillisecondArray>() {
		return Ok(GroupValue::Timestamp(
			TimeUnit::Millisecond,
			array.value(row),
		));
	}
	if let Some(array) = array.as_any().downcast_ref::<TimestampMicrosecondArray>() {
		return Ok(GroupValue::Timestamp(
			TimeUnit::Microsecond,
			array.value(row),
		));
	}
	if let Some(array) = array.as_any().downcast_ref::<TimestampNanosecondArray>() {
		return Ok(GroupValue::Timestamp(
			TimeUnit::Nanosecond,
			array.value(row),
		));
	}
	if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
		return Ok(GroupValue::Utf8(array.value(row).to_string()));
	}
	// ADR 275: Decimal128 group key — the raw i128 keys the group exactly.
	if let Some(array) = array.as_any().downcast_ref::<Decimal128Array>() {
		return Ok(GroupValue::Decimal128(array.value(row)));
	}
	Err(ExecutionError::InvalidExpression {
		message: format!("unsupported GROUP BY type: {:?}", array.data_type()),
	})
}

fn canonical_group_f32_bits(value: f32) -> u32 {
	if value == 0.0 {
		0.0f32.to_bits()
	} else if value.is_nan() {
		f32::NAN.to_bits()
	} else {
		value.to_bits()
	}
}

fn canonical_group_f64_bits(value: f64) -> u64 {
	if value == 0.0 {
		0.0f64.to_bits()
	} else if value.is_nan() {
		f64::NAN.to_bits()
	} else {
		value.to_bits()
	}
}

fn sum_state_for_data_type(data_type: &DataType) -> AggState {
	match data_type {
		DataType::Float32 | DataType::Float64 => AggState::SumFloat {
			total: 0.0,
			seen: false,
		},
		_ => AggState::SumInt {
			total: 0,
			seen: false,
		},
	}
}

fn min_state_for_data_type(data_type: &DataType) -> Result<AggState, ExecutionError> {
	match data_type {
		DataType::Int32 => Ok(AggState::MinInt32 { value: None }),
		DataType::Int64 => Ok(AggState::MinInt64 { value: None }),
		DataType::Float32 => Ok(AggState::MinFloat32 { value: None }),
		DataType::Float64 => Ok(AggState::MinFloat64 { value: None }),
		DataType::Decimal128(_, _) => Ok(AggState::MinDecimal128 { value: None }),
		_ => Err(ExecutionError::InvalidExpression {
			message: format!("MIN does not support input type {:?}", data_type),
		}),
	}
}

fn max_state_for_data_type(data_type: &DataType) -> Result<AggState, ExecutionError> {
	match data_type {
		DataType::Int32 => Ok(AggState::MaxInt32 { value: None }),
		DataType::Int64 => Ok(AggState::MaxInt64 { value: None }),
		DataType::Float32 => Ok(AggState::MaxFloat32 { value: None }),
		DataType::Float64 => Ok(AggState::MaxFloat64 { value: None }),
		DataType::Decimal128(_, _) => Ok(AggState::MaxDecimal128 { value: None }),
		_ => Err(ExecutionError::InvalidExpression {
			message: format!("MAX does not support input type {:?}", data_type),
		}),
	}
}

fn avg_state_for_data_type(data_type: &DataType) -> Result<AggState, ExecutionError> {
	match data_type {
		DataType::Float32 | DataType::Float64 => Ok(AggState::AvgFloat {
			total: 0.0,
			count: 0,
		}),
		DataType::Int8
		| DataType::Int16
		| DataType::Int32
		| DataType::Int64
		| DataType::UInt8
		| DataType::UInt16
		| DataType::UInt32
		| DataType::UInt64 => Ok(AggState::AvgInt { total: 0, count: 0 }),
		// ADR 268: AVG(Decimal) — raw i128 accumulator carrying the input scale for the rescale-and-divide
		// finalize. Validate through the same kernel rule used by bind/type derivation so an invalid
		// high-scale Decimal cannot form a Decimal state and later fall back to Float64 materialization.
		DataType::Decimal128(_, scale)
			if aggregate_kernel::stable_output_data_type(
				&AggregateFunction::Avg,
				Some(data_type),
			)
			.is_some() =>
		{
			Ok(AggState::AvgDecimal {
				total: 0,
				count: 0,
				input_scale: *scale,
			})
		}
		_ => Err(ExecutionError::InvalidExpression {
			message: format!("AVG does not support input type {:?}", data_type),
		}),
	}
}

fn add_sum_value(state: &mut AggState, value: NumericValue) -> Result<(), ExecutionError> {
	match (state, value) {
		(AggState::SumInt { total, seen }, NumericValue::Int(value)) => {
			*total = total
				.checked_add(value)
				.ok_or_else(|| ExecutionError::Kernel {
					message: "integer SUM overflow".to_string(),
				})?;
			*seen = true;
			Ok(())
		}
		(AggState::SumFloat { total, seen }, NumericValue::Float(value)) => {
			*total += value;
			*seen = true;
			Ok(())
		}
		(AggState::SumFloat { total, seen }, NumericValue::Int(value)) => {
			*total += value as f64;
			*seen = true;
			Ok(())
		}
		(AggState::SumInt { .. }, NumericValue::Float(_)) => {
			Err(ExecutionError::InvalidExpression {
				message: "floating SUM value reached integer accumulator".to_string(),
			})
		}
		(AggState::Count(_), _)
		| (AggState::MinInt32 { .. }, _)
		| (AggState::MinInt64 { .. }, _)
		| (AggState::MinFloat32 { .. }, _)
		| (AggState::MinFloat64 { .. }, _)
		| (AggState::MinDecimal128 { .. }, _)
		| (AggState::MaxInt32 { .. }, _)
		| (AggState::MaxInt64 { .. }, _)
		| (AggState::MaxFloat32 { .. }, _)
		| (AggState::MaxFloat64 { .. }, _)
		| (AggState::MaxDecimal128 { .. }, _)
		| (AggState::AvgInt { .. }, _)
		| (AggState::AvgFloat { .. }, _)
		| (AggState::AvgDecimal { .. }, _) => Err(ExecutionError::InvalidExpression {
			message: "SUM value reached non-SUM accumulator".to_string(),
		}),
	}
}

fn add_avg_value(state: &mut AggState, value: NumericValue) -> Result<(), ExecutionError> {
	match (state, value) {
		(AggState::AvgInt { total, count }, NumericValue::Int(value)) => {
			*total = total
				.checked_add(value)
				.ok_or_else(|| ExecutionError::Kernel {
					message: "integer AVG overflow".to_string(),
				})?;
			*count = count.checked_add(1).ok_or_else(|| ExecutionError::Kernel {
				message: "AVG count overflow".to_string(),
			})?;
			Ok(())
		}
		(AggState::AvgFloat { total, count }, NumericValue::Float(value)) => {
			*total += value;
			*count = count.checked_add(1).ok_or_else(|| ExecutionError::Kernel {
				message: "AVG count overflow".to_string(),
			})?;
			Ok(())
		}
		(AggState::AvgFloat { total, count }, NumericValue::Int(value)) => {
			*total += value as f64;
			*count = count.checked_add(1).ok_or_else(|| ExecutionError::Kernel {
				message: "AVG count overflow".to_string(),
			})?;
			Ok(())
		}
		(AggState::AvgInt { .. }, NumericValue::Float(_)) => {
			Err(ExecutionError::InvalidExpression {
				message: "floating AVG value reached integer accumulator".to_string(),
			})
		}
		// ADR 268: a Decimal value enters the i128 accumulator as its raw unscaled integer.
		(AggState::AvgDecimal { total, count, .. }, NumericValue::Int(value)) => {
			*total = total
				.checked_add(value)
				.ok_or_else(|| ExecutionError::Kernel {
					message: "decimal AVG overflow".to_string(),
				})?;
			*count = count.checked_add(1).ok_or_else(|| ExecutionError::Kernel {
				message: "AVG count overflow".to_string(),
			})?;
			Ok(())
		}
		(AggState::AvgDecimal { .. }, NumericValue::Float(_)) => {
			Err(ExecutionError::InvalidExpression {
				message: "floating AVG value reached decimal accumulator".to_string(),
			})
		}
		(AggState::Count(_), _)
		| (AggState::SumInt { .. }, _)
		| (AggState::SumFloat { .. }, _)
		| (AggState::MinInt32 { .. }, _)
		| (AggState::MinInt64 { .. }, _)
		| (AggState::MinFloat32 { .. }, _)
		| (AggState::MinFloat64 { .. }, _)
		| (AggState::MinDecimal128 { .. }, _)
		| (AggState::MaxInt32 { .. }, _)
		| (AggState::MaxInt64 { .. }, _)
		| (AggState::MaxFloat32 { .. }, _)
		| (AggState::MaxFloat64 { .. }, _)
		| (AggState::MaxDecimal128 { .. }, _) => Err(ExecutionError::InvalidExpression {
			message: "AVG value reached non-AVG accumulator".to_string(),
		}),
	}
}

// ADR 022 pins sticky-NaN and IEEE totalOrder signed-zero semantics
// for Float MIN/MAX instead of Rust's one-sided f32/f64 min/max helpers.
#[inline]
fn f32_min_sticky_total(current: f32, candidate: f32) -> f32 {
	if current.is_nan() || candidate.is_nan() {
		return f32::NAN;
	}
	match current.total_cmp(&candidate) {
		std::cmp::Ordering::Greater => candidate,
		_ => current,
	}
}

#[inline]
fn f32_max_sticky_total(current: f32, candidate: f32) -> f32 {
	if current.is_nan() || candidate.is_nan() {
		return f32::NAN;
	}
	match current.total_cmp(&candidate) {
		std::cmp::Ordering::Less => candidate,
		_ => current,
	}
}

#[inline]
fn f64_min_sticky_total(current: f64, candidate: f64) -> f64 {
	if current.is_nan() || candidate.is_nan() {
		return f64::NAN;
	}
	match current.total_cmp(&candidate) {
		std::cmp::Ordering::Greater => candidate,
		_ => current,
	}
}

#[inline]
fn f64_max_sticky_total(current: f64, candidate: f64) -> f64 {
	if current.is_nan() || candidate.is_nan() {
		return f64::NAN;
	}
	match current.total_cmp(&candidate) {
		std::cmp::Ordering::Less => candidate,
		_ => current,
	}
}

fn update_min_state(
	state: &mut AggState,
	array: &ArrayRef,
	row: usize,
) -> Result<(), ExecutionError> {
	if array.is_null(row) {
		return Ok(());
	}
	match state {
		AggState::MinInt32 { value } => {
			let candidate = array
				.as_any()
				.downcast_ref::<Int32Array>()
				.ok_or_else(|| ExecutionError::InvalidExpression {
					message: "MIN(Int32) array type mismatch".to_string(),
				})?
				.value(row);
			*value = Some(value.map_or(candidate, |current| current.min(candidate)));
			Ok(())
		}
		AggState::MinInt64 { value } => {
			let candidate = array
				.as_any()
				.downcast_ref::<Int64Array>()
				.ok_or_else(|| ExecutionError::InvalidExpression {
					message: "MIN(Int64) array type mismatch".to_string(),
				})?
				.value(row);
			*value = Some(value.map_or(candidate, |current| current.min(candidate)));
			Ok(())
		}
		AggState::MinFloat32 { value } => {
			let candidate = array
				.as_any()
				.downcast_ref::<Float32Array>()
				.ok_or_else(|| ExecutionError::InvalidExpression {
					message: "MIN(Float32) array type mismatch".to_string(),
				})?
				.value(row);
			*value = Some(value.map_or(candidate, |current| {
				f32_min_sticky_total(current, candidate)
			}));
			Ok(())
		}
		AggState::MinFloat64 { value } => {
			let candidate = array
				.as_any()
				.downcast_ref::<Float64Array>()
				.ok_or_else(|| ExecutionError::InvalidExpression {
					message: "MIN(Float64) array type mismatch".to_string(),
				})?
				.value(row);
			*value = Some(value.map_or(candidate, |current| {
				f64_min_sticky_total(current, candidate)
			}));
			Ok(())
		}
		AggState::MinDecimal128 { value } => {
			let candidate = array
				.as_any()
				.downcast_ref::<Decimal128Array>()
				.ok_or_else(|| ExecutionError::InvalidExpression {
					message: "MIN(Decimal128) array type mismatch".to_string(),
				})?
				.value(row);
			*value = Some(value.map_or(candidate, |current| current.min(candidate)));
			Ok(())
		}
		_ => Err(ExecutionError::InvalidExpression {
			message: "MIN value reached non-MIN accumulator".to_string(),
		}),
	}
}

fn update_max_state(
	state: &mut AggState,
	array: &ArrayRef,
	row: usize,
) -> Result<(), ExecutionError> {
	if array.is_null(row) {
		return Ok(());
	}
	match state {
		AggState::MaxInt32 { value } => {
			let candidate = array
				.as_any()
				.downcast_ref::<Int32Array>()
				.ok_or_else(|| ExecutionError::InvalidExpression {
					message: "MAX(Int32) array type mismatch".to_string(),
				})?
				.value(row);
			*value = Some(value.map_or(candidate, |current| current.max(candidate)));
			Ok(())
		}
		AggState::MaxInt64 { value } => {
			let candidate = array
				.as_any()
				.downcast_ref::<Int64Array>()
				.ok_or_else(|| ExecutionError::InvalidExpression {
					message: "MAX(Int64) array type mismatch".to_string(),
				})?
				.value(row);
			*value = Some(value.map_or(candidate, |current| current.max(candidate)));
			Ok(())
		}
		AggState::MaxFloat32 { value } => {
			let candidate = array
				.as_any()
				.downcast_ref::<Float32Array>()
				.ok_or_else(|| ExecutionError::InvalidExpression {
					message: "MAX(Float32) array type mismatch".to_string(),
				})?
				.value(row);
			*value = Some(value.map_or(candidate, |current| {
				f32_max_sticky_total(current, candidate)
			}));
			Ok(())
		}
		AggState::MaxFloat64 { value } => {
			let candidate = array
				.as_any()
				.downcast_ref::<Float64Array>()
				.ok_or_else(|| ExecutionError::InvalidExpression {
					message: "MAX(Float64) array type mismatch".to_string(),
				})?
				.value(row);
			*value = Some(value.map_or(candidate, |current| {
				f64_max_sticky_total(current, candidate)
			}));
			Ok(())
		}
		AggState::MaxDecimal128 { value } => {
			let candidate = array
				.as_any()
				.downcast_ref::<Decimal128Array>()
				.ok_or_else(|| ExecutionError::InvalidExpression {
					message: "MAX(Decimal128) array type mismatch".to_string(),
				})?
				.value(row);
			*value = Some(value.map_or(candidate, |current| current.max(candidate)));
			Ok(())
		}
		_ => Err(ExecutionError::InvalidExpression {
			message: "MAX value reached non-MAX accumulator".to_string(),
		}),
	}
}

fn min_max_state_array(
	state: &AggState,
	output_type: &DataType,
) -> Result<ArrayRef, ExecutionError> {
	match (state, output_type) {
		(AggState::MinInt32 { value }, DataType::Int32)
		| (AggState::MaxInt32 { value }, DataType::Int32) => {
			Ok(Arc::new(Int32Array::from(vec![*value])) as ArrayRef)
		}
		(AggState::MinInt64 { value }, DataType::Int64)
		| (AggState::MaxInt64 { value }, DataType::Int64) => {
			Ok(Arc::new(Int64Array::from(vec![*value])) as ArrayRef)
		}
		(AggState::MinFloat32 { value }, DataType::Float32)
		| (AggState::MaxFloat32 { value }, DataType::Float32) => {
			Ok(Arc::new(Float32Array::from(vec![*value])) as ArrayRef)
		}
		(AggState::MinFloat64 { value }, DataType::Float64)
		| (AggState::MaxFloat64 { value }, DataType::Float64) => {
			Ok(Arc::new(Float64Array::from(vec![*value])) as ArrayRef)
		}
		(
			AggState::MinDecimal128 { value } | AggState::MaxDecimal128 { value },
			DataType::Decimal128(precision, scale),
		) => Ok(Arc::new(
			Decimal128Array::from(vec![*value])
				.with_precision_and_scale(*precision, *scale)
				.map_err(|error| ExecutionError::Kernel {
					message: error.to_string(),
				})?,
		) as ArrayRef),
		(other, data_type) => Err(ExecutionError::InvalidExpression {
			message: format!("cannot materialize {other:?} as MIN/MAX output {data_type:?}"),
		}),
	}
}

fn sum_state_array(state: &AggState, data_type: &DataType) -> Result<ArrayRef, ExecutionError> {
	match (state, data_type) {
		(AggState::SumInt { total, seen }, DataType::Int64) => {
			let value = if *seen {
				Some(i64::try_from(*total).map_err(|_| ExecutionError::Kernel {
					message: "integer SUM result does not fit Int64".to_string(),
				})?)
			} else {
				None
			};
			Ok(Arc::new(Int64Array::from(vec![value])) as ArrayRef)
		}
		(AggState::SumFloat { total, seen }, DataType::Float64) => {
			Ok(Arc::new(Float64Array::from(vec![seen.then_some(*total)])) as ArrayRef)
		}
		(AggState::SumInt { total, seen }, DataType::Float64) => {
			Ok(Arc::new(Float64Array::from(vec![seen.then_some(*total as f64)])) as ArrayRef)
		}
		// ADR 263: SUM(Decimal128) finalizes the i128 accumulator back to the column's (precision,
		// scale) (Q6 slice — global aggregate, no GROUP BY).
		(AggState::SumInt { total, seen }, DataType::Decimal128(precision, scale)) => Ok(Arc::new(
			Decimal128Array::from(vec![seen.then_some(*total)])
				.with_precision_and_scale(*precision, *scale)
				.map_err(|error| ExecutionError::Kernel {
					message: error.to_string(),
				})?,
		)
			as ArrayRef),
		(other, data_type) => Err(ExecutionError::InvalidExpression {
			message: format!("cannot materialize {other:?} as SUM output {data_type:?}"),
		}),
	}
}

fn sum_column_from_groups(
	groups: &BTreeMap<Vec<GroupValue>, Vec<AggState>>,
	agg_idx: usize,
	data_type: &DataType,
) -> Result<ArrayRef, ExecutionError> {
	match data_type {
		DataType::Int64 => {
			let values = groups
				.values()
				.map(|states| match states[agg_idx] {
					AggState::SumInt { total, seen } => {
						if seen {
							Ok::<Option<i64>, ExecutionError>(Some(i64::try_from(total).map_err(
								|_| ExecutionError::Kernel {
									message: "integer SUM result does not fit Int64".to_string(),
								},
							)?))
						} else {
							Ok(None)
						}
					}
					AggState::SumFloat { .. }
					| AggState::Count(_)
					| AggState::MinInt32 { .. }
					| AggState::MinInt64 { .. }
					| AggState::MinFloat32 { .. }
					| AggState::MinFloat64 { .. }
					| AggState::MinDecimal128 { .. }
					| AggState::MaxInt32 { .. }
					| AggState::MaxInt64 { .. }
					| AggState::MaxFloat32 { .. }
					| AggState::MaxFloat64 { .. }
					| AggState::MaxDecimal128 { .. }
					| AggState::AvgInt { .. }
					| AggState::AvgFloat { .. }
					| AggState::AvgDecimal { .. } => Ok(None),
				})
				.collect::<Result<Vec<_>, _>>()?;
			Ok(Arc::new(Int64Array::from(values)) as ArrayRef)
		}
		DataType::Float64 => {
			let values = groups
				.values()
				.map(|states| match states[agg_idx] {
					AggState::SumFloat { total, seen } => seen.then_some(total),
					AggState::SumInt { total, seen } => seen.then_some(total as f64),
					AggState::Count(_)
					| AggState::MinInt32 { .. }
					| AggState::MinInt64 { .. }
					| AggState::MinFloat32 { .. }
					| AggState::MinFloat64 { .. }
					| AggState::MinDecimal128 { .. }
					| AggState::MaxInt32 { .. }
					| AggState::MaxInt64 { .. }
					| AggState::MaxFloat32 { .. }
					| AggState::MaxFloat64 { .. }
					| AggState::MaxDecimal128 { .. }
					| AggState::AvgInt { .. }
					| AggState::AvgFloat { .. }
					| AggState::AvgDecimal { .. } => None,
				})
				.collect::<Vec<_>>();
			Ok(Arc::new(Float64Array::from(values)) as ArrayRef)
		}
		// ADR 263: grouped SUM(Decimal128) mirrors the global finalize — the per-group i128 `SumInt`
		// accumulator carries the raw running sum at the column's shared scale. Keeps bind and execute
		// in lockstep (the SUM-Decimal domain accepts Decimal128 at bind, so execute must too).
		DataType::Decimal128(precision, scale) => {
			let values = groups
				.values()
				.map(|states| match states[agg_idx] {
					AggState::SumInt { total, seen } => seen.then_some(total),
					AggState::SumFloat { .. }
					| AggState::Count(_)
					| AggState::MinInt32 { .. }
					| AggState::MinInt64 { .. }
					| AggState::MinFloat32 { .. }
					| AggState::MinFloat64 { .. }
					| AggState::MinDecimal128 { .. }
					| AggState::MaxInt32 { .. }
					| AggState::MaxInt64 { .. }
					| AggState::MaxFloat32 { .. }
					| AggState::MaxFloat64 { .. }
					| AggState::MaxDecimal128 { .. }
					| AggState::AvgInt { .. }
					| AggState::AvgFloat { .. }
					| AggState::AvgDecimal { .. } => None,
				})
				.collect::<Vec<_>>();
			Ok(Arc::new(
				Decimal128Array::from(values)
					.with_precision_and_scale(*precision, *scale)
					.map_err(|error| ExecutionError::Kernel {
						message: error.to_string(),
					})?,
			) as ArrayRef)
		}
		_ => Err(ExecutionError::InvalidExpression {
			message: format!("unsupported SUM output type: {data_type:?}"),
		}),
	}
}

fn min_max_column_from_groups(
	groups: &BTreeMap<Vec<GroupValue>, Vec<AggState>>,
	agg_idx: usize,
	output_type: &DataType,
) -> Result<ArrayRef, ExecutionError> {
	match output_type {
		DataType::Int32 => {
			let values = groups
				.values()
				.map(|states| match states[agg_idx] {
					AggState::MinInt32 { value } | AggState::MaxInt32 { value } => value,
					_ => None,
				})
				.collect::<Vec<_>>();
			Ok(Arc::new(Int32Array::from(values)) as ArrayRef)
		}
		DataType::Int64 => {
			let values = groups
				.values()
				.map(|states| match states[agg_idx] {
					AggState::MinInt64 { value } | AggState::MaxInt64 { value } => value,
					_ => None,
				})
				.collect::<Vec<_>>();
			Ok(Arc::new(Int64Array::from(values)) as ArrayRef)
		}
		DataType::Float32 => {
			let values = groups
				.values()
				.map(|states| match states[agg_idx] {
					AggState::MinFloat32 { value } | AggState::MaxFloat32 { value } => value,
					_ => None,
				})
				.collect::<Vec<_>>();
			Ok(Arc::new(Float32Array::from(values)) as ArrayRef)
		}
		DataType::Float64 => {
			let values = groups
				.values()
				.map(|states| match states[agg_idx] {
					AggState::MinFloat64 { value } | AggState::MaxFloat64 { value } => value,
					_ => None,
				})
				.collect::<Vec<_>>();
			Ok(Arc::new(Float64Array::from(values)) as ArrayRef)
		}
		DataType::Decimal128(precision, scale) => {
			let values = groups
				.values()
				.map(|states| match states[agg_idx] {
					AggState::MinDecimal128 { value } | AggState::MaxDecimal128 { value } => value,
					_ => None,
				})
				.collect::<Vec<_>>();
			Ok(Arc::new(
				Decimal128Array::from(values)
					.with_precision_and_scale(*precision, *scale)
					.map_err(|error| ExecutionError::Kernel {
						message: error.to_string(),
					})?,
			) as ArrayRef)
		}
		_ => Err(ExecutionError::InvalidExpression {
			message: format!("unsupported MIN/MAX output type: {output_type:?}"),
		}),
	}
}

fn avg_finalize(state: &AggState) -> Result<Option<f64>, ExecutionError> {
	match state {
		AggState::AvgInt { total, count } => {
			Ok((*count != 0).then_some(*total as f64 / *count as f64))
		}
		AggState::AvgFloat { total, count } => Ok((*count != 0).then_some(*total / *count as f64)),
		_ => Err(ExecutionError::InvalidExpression {
			message: "AVG finalize over non-AVG state".to_string(),
		}),
	}
}

// ADR 268: finalize AVG(Decimal). Rescale the i128 sum from the input scale to the widened output scale,
// then divide by the count rounding **half away from zero**, all in checked integer arithmetic. Returns the
// raw unscaled i128 (the caller stamps `(out_precision, out_scale)` onto the Decimal128 array).
fn avg_decimal_finalize(
	state: &AggState,
	out_precision: u8,
	out_scale: i8,
) -> Result<Option<i128>, ExecutionError> {
	let AggState::AvgDecimal {
		total,
		count,
		input_scale,
	} = state
	else {
		return Err(ExecutionError::InvalidExpression {
			message: "decimal AVG finalize over non-decimal-AVG state".to_string(),
		});
	};
	if *count == 0 {
		return Ok(None);
	}
	let scale_delta = i32::from(out_scale) - i32::from(*input_scale);
	if scale_delta < 0 {
		return Err(ExecutionError::Kernel {
			message: "AVG decimal output scale is below input scale".to_string(),
		});
	}
	let scale_overflow = || ExecutionError::Kernel {
		message: "AVG decimal scale overflow".to_string(),
	};
	let multiplier = 10_i128
		.checked_pow(scale_delta as u32)
		.ok_or_else(scale_overflow)?;
	let scaled = total.checked_mul(multiplier).ok_or_else(scale_overflow)?;
	let count = i128::from(*count);
	let mut quotient = scaled / count;
	let remainder = scaled % count;
	// Half away from zero: round up when twice the magnitude of the remainder reaches the divisor.
	if remainder.unsigned_abs() * 2 >= count.unsigned_abs() {
		quotient = quotient
			.checked_add(scaled.signum())
			.ok_or_else(|| ExecutionError::Kernel {
				message: "AVG decimal rounding overflow".to_string(),
			})?;
	}
	if !decimal_fits_precision(quotient, out_precision) {
		return Err(ExecutionError::Kernel {
			message: "AVG decimal result does not fit output precision".to_string(),
		});
	}
	Ok(Some(quotient))
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
fn evaluate_decimal_division(
	left_arr: &ArrayRef,
	right_arr: &ArrayRef,
) -> Result<ArrayRef, ExecutionError> {
	let (DataType::Decimal128(p1, s1), DataType::Decimal128(p2, s2)) =
		(left_arr.data_type(), right_arr.data_type())
	else {
		return Err(ExecutionError::InvalidExpression {
			message: "decimal division over non-decimal operands".to_string(),
		});
	};
	let (out_precision, out_scale) =
		decimal_division_output_type(*p1, *s1, *p2, *s2).ok_or_else(|| ExecutionError::Kernel {
			message: "decimal division output type out of range".to_string(),
		})?;
	let left = left_arr
		.as_any()
		.downcast_ref::<Decimal128Array>()
		.expect("checked Decimal128 numerator");
	let right = right_arr
		.as_any()
		.downcast_ref::<Decimal128Array>()
		.expect("checked Decimal128 denominator");
	let overflow = || ExecutionError::Kernel {
		message: "decimal division scale overflow".to_string(),
	};
	// `decimal_division_output_type` guarantees this exponent is in `0..=38`, so `10^scale_delta` fits i128.
	let scale_delta = i32::from(out_scale) + i32::from(*s2) - i32::from(*s1);
	let multiplier = 10_i128
		.checked_pow(u32::try_from(scale_delta).map_err(|_| overflow())?)
		.ok_or_else(overflow)?;
	let mut values: Vec<Option<i128>> = Vec::with_capacity(left.len());
	for index in 0..left.len() {
		if left.is_null(index) || right.is_null(index) {
			values.push(None);
			continue;
		}
		let divisor = right.value(index);
		if divisor == 0 {
			values.push(None); // ADR 084: div-by-zero -> NULL
			continue;
		}
		let scaled = left
			.value(index)
			.checked_mul(multiplier)
			.ok_or_else(overflow)?;
		let mut quotient = scaled / divisor;
		let remainder = scaled % divisor;
		if remainder.unsigned_abs() * 2 >= divisor.unsigned_abs() {
			quotient = quotient
				.checked_add(scaled.signum() * divisor.signum())
				.ok_or_else(|| ExecutionError::Kernel {
					message: "decimal division rounding overflow".to_string(),
				})?;
		}
		if !decimal_fits_precision(quotient, out_precision) {
			return Err(ExecutionError::Kernel {
				message: "decimal division result does not fit output precision".to_string(),
			});
		}
		values.push(Some(quotient));
	}
	Ok(Arc::new(
		Decimal128Array::from(values).with_precision_and_scale(out_precision, out_scale)?,
	))
}

fn avg_state_array(state: &AggState, output_type: &DataType) -> Result<ArrayRef, ExecutionError> {
	match output_type {
		DataType::Decimal128(precision, scale) => {
			let value = avg_decimal_finalize(state, *precision, *scale)?;
			Ok(Arc::new(
				Decimal128Array::from(vec![value])
					.with_precision_and_scale(*precision, *scale)
					.map_err(|error| ExecutionError::Kernel {
						message: error.to_string(),
					})?,
			) as ArrayRef)
		}
		_ => Ok(Arc::new(Float64Array::from(vec![avg_finalize(state)?])) as ArrayRef),
	}
}

fn avg_column_from_groups(
	groups: &BTreeMap<Vec<GroupValue>, Vec<AggState>>,
	agg_idx: usize,
	output_type: &DataType,
) -> Result<ArrayRef, ExecutionError> {
	match output_type {
		DataType::Decimal128(precision, scale) => {
			let values = groups
				.values()
				.map(|states| avg_decimal_finalize(&states[agg_idx], *precision, *scale))
				.collect::<Result<Vec<_>, _>>()?;
			Ok(Arc::new(
				Decimal128Array::from(values)
					.with_precision_and_scale(*precision, *scale)
					.map_err(|error| ExecutionError::Kernel {
						message: error.to_string(),
					})?,
			) as ArrayRef)
		}
		_ => {
			let values = groups
				.values()
				.map(|states| avg_finalize(&states[agg_idx]))
				.collect::<Result<Vec<_>, _>>()?;
			Ok(Arc::new(Float64Array::from(values)) as ArrayRef)
		}
	}
}

#[cfg(test)]
fn aggregate_state_keyword(function: &AggregateFunction) -> &'static str {
	aggregate_kernel::stable_spec_for_function(function)
		.map(|spec| spec.sql_name)
		.unwrap_or("aggregate")
}

#[cfg(test)]
fn aggregate_state_output_name(function: &AggregateFunction) -> &'static str {
	if let Some(spec) = aggregate_kernel::stable_spec_for_function(function) {
		return spec.output_name;
	}
	match function {
		AggregateFunction::LinearRegress => "linreg",
		AggregateFunction::ApproxQuantile(_) => "quantile",
		AggregateFunction::CountAll
		| AggregateFunction::Count
		| AggregateFunction::Sum
		| AggregateFunction::Min
		| AggregateFunction::Max
		| AggregateFunction::Avg => unreachable!("stable aggregate metadata missing"),
	}
}

fn numeric_value(array: &ArrayRef, row: usize) -> Result<Option<NumericValue>, ExecutionError> {
	if array.is_null(row) {
		return Ok(None);
	}
	if let Some(array) = array.as_any().downcast_ref::<Int8Array>() {
		return Ok(Some(NumericValue::Int(array.value(row) as i128)));
	}
	if let Some(array) = array.as_any().downcast_ref::<Int16Array>() {
		return Ok(Some(NumericValue::Int(array.value(row) as i128)));
	}
	if let Some(array) = array.as_any().downcast_ref::<Int32Array>() {
		return Ok(Some(NumericValue::Int(array.value(row) as i128)));
	}
	if let Some(array) = array.as_any().downcast_ref::<Int64Array>() {
		return Ok(Some(NumericValue::Int(array.value(row) as i128)));
	}
	if let Some(array) = array.as_any().downcast_ref::<UInt8Array>() {
		return Ok(Some(NumericValue::Int(array.value(row) as i128)));
	}
	if let Some(array) = array.as_any().downcast_ref::<UInt16Array>() {
		return Ok(Some(NumericValue::Int(array.value(row) as i128)));
	}
	if let Some(array) = array.as_any().downcast_ref::<UInt32Array>() {
		return Ok(Some(NumericValue::Int(array.value(row) as i128)));
	}
	if let Some(array) = array.as_any().downcast_ref::<UInt64Array>() {
		return Ok(Some(NumericValue::Int(array.value(row) as i128)));
	}
	if let Some(array) = array.as_any().downcast_ref::<Float32Array>() {
		return Ok(Some(NumericValue::Float(array.value(row) as f64)));
	}
	if let Some(array) = array.as_any().downcast_ref::<Float64Array>() {
		return Ok(Some(NumericValue::Float(array.value(row))));
	}
	// ADR 263: a Decimal128 value enters the i128 accumulator as its raw (unscaled) integer; the
	// SUM finalize re-applies the column's (precision, scale). (Q6 slice: SUM over Decimal.)
	if let Some(array) = array.as_any().downcast_ref::<Decimal128Array>() {
		return Ok(Some(NumericValue::Int(array.value(row))));
	}
	Err(ExecutionError::InvalidExpression {
		message: format!(
			"aggregate expects numeric input, got {:?}",
			array.data_type()
		),
	})
}

fn group_column(
	keys: &[Vec<GroupValue>],
	idx: usize,
	data_type: &arrow::datatypes::DataType,
) -> Result<ArrayRef, ExecutionError> {
	match data_type {
		arrow::datatypes::DataType::Boolean => Ok(Arc::new(BooleanArray::from(
			keys.iter()
				.map(|key| match &key[idx] {
					GroupValue::Null => Ok(None),
					GroupValue::Boolean(value) => Ok(Some(*value)),
					other => Err(group_type_error(data_type, other)),
				})
				.collect::<Result<Vec<_>, _>>()?,
		))),
		arrow::datatypes::DataType::Int32 => Ok(Arc::new(Int32Array::from(
			keys.iter()
				.map(|key| match &key[idx] {
					GroupValue::Null => Ok(None),
					GroupValue::Int32(value) => Ok(Some(*value)),
					other => Err(group_type_error(data_type, other)),
				})
				.collect::<Result<Vec<_>, _>>()?,
		))),
		arrow::datatypes::DataType::Int64 => Ok(Arc::new(Int64Array::from(
			keys.iter()
				.map(|key| match &key[idx] {
					GroupValue::Null => Ok(None),
					GroupValue::Int64(value) => Ok(Some(*value)),
					other => Err(group_type_error(data_type, other)),
				})
				.collect::<Result<Vec<_>, _>>()?,
		))),
		arrow::datatypes::DataType::Float32 => Ok(Arc::new(Float32Array::from(
			keys.iter()
				.map(|key| match &key[idx] {
					GroupValue::Null => Ok(None),
					GroupValue::Float32(value) => Ok(Some(f32::from_bits(*value))),
					other => Err(group_type_error(data_type, other)),
				})
				.collect::<Result<Vec<_>, _>>()?,
		))),
		arrow::datatypes::DataType::Float64 => Ok(Arc::new(Float64Array::from(
			keys.iter()
				.map(|key| match &key[idx] {
					GroupValue::Null => Ok(None),
					GroupValue::Float64(value) => Ok(Some(f64::from_bits(*value))),
					other => Err(group_type_error(data_type, other)),
				})
				.collect::<Result<Vec<_>, _>>()?,
		))),
		arrow::datatypes::DataType::Date32 => Ok(Arc::new(Date32Array::from(
			keys.iter()
				.map(|key| match &key[idx] {
					GroupValue::Null => Ok(None),
					GroupValue::Date32(value) => Ok(Some(*value)),
					other => Err(group_type_error(data_type, other)),
				})
				.collect::<Result<Vec<_>, _>>()?,
		))),
		arrow::datatypes::DataType::Timestamp(TimeUnit::Millisecond, Some(timezone))
			if timezone.as_ref() == "UTC" =>
		{
			Ok(Arc::new(
				TimestampMillisecondArray::from(
					keys.iter()
						.map(|key| match &key[idx] {
							GroupValue::Null => Ok(None),
							GroupValue::Timestamp(TimeUnit::Millisecond, value) => Ok(Some(*value)),
							other => Err(group_type_error(data_type, other)),
						})
						.collect::<Result<Vec<_>, _>>()?,
				)
				.with_timezone("UTC"),
			))
		}
		arrow::datatypes::DataType::Timestamp(TimeUnit::Microsecond, Some(timezone))
			if timezone.as_ref() == "UTC" =>
		{
			Ok(Arc::new(
				TimestampMicrosecondArray::from(
					keys.iter()
						.map(|key| match &key[idx] {
							GroupValue::Null => Ok(None),
							GroupValue::Timestamp(TimeUnit::Microsecond, value) => Ok(Some(*value)),
							other => Err(group_type_error(data_type, other)),
						})
						.collect::<Result<Vec<_>, _>>()?,
				)
				.with_timezone("UTC"),
			))
		}
		arrow::datatypes::DataType::Timestamp(TimeUnit::Nanosecond, Some(timezone))
			if timezone.as_ref() == "UTC" =>
		{
			Ok(Arc::new(
				TimestampNanosecondArray::from(
					keys.iter()
						.map(|key| match &key[idx] {
							GroupValue::Null => Ok(None),
							GroupValue::Timestamp(TimeUnit::Nanosecond, value) => Ok(Some(*value)),
							other => Err(group_type_error(data_type, other)),
						})
						.collect::<Result<Vec<_>, _>>()?,
				)
				.with_timezone("UTC"),
			))
		}
		arrow::datatypes::DataType::Utf8 => Ok(Arc::new(StringArray::from(
			keys.iter()
				.map(|key| match &key[idx] {
					GroupValue::Null => Ok(None),
					GroupValue::Utf8(value) => Ok(Some(value.as_str())),
					other => Err(group_type_error(data_type, other)),
				})
				.collect::<Result<Vec<_>, _>>()?,
		))),
		// ADR 275: rebuild the Decimal128 group key, carrying the exact (precision, scale) from the column type.
		arrow::datatypes::DataType::Decimal128(precision, scale) => {
			let values = keys
				.iter()
				.map(|key| match &key[idx] {
					GroupValue::Null => Ok(None),
					GroupValue::Decimal128(value) => Ok(Some(*value)),
					other => Err(group_type_error(data_type, other)),
				})
				.collect::<Result<Vec<_>, _>>()?;
			Ok(Arc::new(
				Decimal128Array::from(values)
					.with_precision_and_scale(*precision, *scale)
					.map_err(|error| ExecutionError::InvalidExpression {
						message: format!("decimal128 group column: {error}"),
					})?,
			))
		}
		_ => Err(ExecutionError::InvalidExpression {
			message: format!("unsupported GROUP BY type: {data_type:?}"),
		}),
	}
}

fn group_type_error(data_type: &arrow::datatypes::DataType, value: &GroupValue) -> ExecutionError {
	ExecutionError::InvalidExpression {
		message: format!("group key value {value:?} does not match type {data_type:?}"),
	}
}

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

fn evaluate_unbound_arithmetic(
	op: ArithmeticOp,
	left: ArrayRef,
	right: ArrayRef,
) -> Result<ArrayRef, ExecutionError> {
	if let Some(result) = evaluate_temporal_arithmetic(op, &left, &right)? {
		return Ok(result);
	}
	let output_type =
		arithmetic_output_type(left.data_type(), right.data_type()).ok_or_else(|| {
			ExecutionError::InvalidExpression {
				message: format!(
					"arithmetic operands must be Int32, Int64, Float32, or Float64, got {:?} and {:?}",
					left.data_type(),
					right.data_type()
				),
			}
		})?;
	let left = if left.data_type() == &output_type {
		left
	} else {
		evaluate_cast(&left, &output_type)?
	};
	let right = if right.data_type() == &output_type {
		right
	} else {
		evaluate_cast(&right, &output_type)?
	};
	evaluate_arithmetic(op, &left, &right, &output_type)
}

pub(crate) fn evaluate_cast(
	array: &ArrayRef,
	target: &DataType,
) -> Result<ArrayRef, ExecutionError> {
	if array.data_type() == target {
		return Ok(array.clone());
	}

	match (array.data_type(), target) {
		(DataType::Int32, DataType::Int64) => cast_i32_to_i64(array),
		(DataType::Int32, DataType::Float32) => cast_i32_to_f32(array),
		(DataType::Int32, DataType::Float64) => cast_i32_to_f64(array),
		(DataType::Int64, DataType::Int32) => cast_i64_to_i32(array),
		(DataType::Int64, DataType::Float32) => cast_i64_to_f32(array),
		(DataType::Int64, DataType::Float64) => cast_i64_to_f64(array),
		(DataType::Float32, DataType::Int32) => cast_f32_to_i32(array),
		(DataType::Float32, DataType::Int64) => cast_f32_to_i64(array),
		(DataType::Float32, DataType::Float64) => cast_f32_to_f64(array),
		(DataType::Float64, DataType::Int32) => cast_f64_to_i32(array),
		(DataType::Float64, DataType::Int64) => cast_f64_to_i64(array),
		(DataType::Float64, DataType::Float32) => cast_f64_to_f32(array),
		(DataType::Int32, DataType::Decimal128(_, _))
		| (DataType::Int64, DataType::Decimal128(_, _)) => arrow::compute::cast(array, target)
			.map_err(|error| cast_failed(array.data_type(), target, error)),
		(DataType::Decimal128(_, _), DataType::Decimal128(_, _)) => {
			arrow::compute::cast(array, target)
				.map_err(|error| cast_failed(array.data_type(), target, error))
		}
		(source, target) => Err(cast_failed(
			source,
			target,
			"unsupported combination (binder gate failure)",
		)),
	}
}

fn evaluate_try_cast(array: &ArrayRef, target: &DataType) -> Result<ArrayRef, ExecutionError> {
	if array.data_type() == target {
		return Ok(array.clone());
	}

	match (array.data_type(), target) {
		(DataType::Int32, DataType::Int64) => cast_i32_to_i64(array),
		(DataType::Int32, DataType::Float32) => cast_i32_to_f32(array),
		(DataType::Int32, DataType::Float64) => cast_i32_to_f64(array),
		(DataType::Int64, DataType::Float32) => cast_i64_to_f32(array),
		(DataType::Int64, DataType::Float64) => cast_i64_to_f64(array),
		(DataType::Float32, DataType::Float64) => cast_f32_to_f64(array),
		(DataType::Int64, DataType::Int32) => try_cast_i64_to_i32(array),
		(DataType::Float64, DataType::Float32) => try_cast_f64_to_f32(array),
		(DataType::Float32, DataType::Int32) => try_cast_f32_to_i32(array),
		(DataType::Float32, DataType::Int64) => try_cast_f32_to_i64(array),
		(DataType::Float64, DataType::Int32) => try_cast_f64_to_i32(array),
		(DataType::Float64, DataType::Int64) => try_cast_f64_to_i64(array),
		(source, target) => Err(cast_failed(
			source,
			target,
			"unsupported combination (binder gate failure)",
		)),
	}
}

fn cast_failed(source: &DataType, target: &DataType, cause: impl fmt::Display) -> ExecutionError {
	ExecutionError::Kernel {
		message: format!("cast failed: {source:?} to {target:?}: {cause}"),
	}
}

fn cast_i32_to_i64(array: &ArrayRef) -> Result<ArrayRef, ExecutionError> {
	let array = as_int32(array, "Cast source")?;
	let values = array.iter().map(|v| v.map(i64::from)).collect::<Vec<_>>();
	Ok(Arc::new(Int64Array::from(values)) as ArrayRef)
}

fn cast_i32_to_f32(array: &ArrayRef) -> Result<ArrayRef, ExecutionError> {
	let array = as_int32(array, "Cast source")?;
	let values = array
		.iter()
		.map(|v| v.map(|v| v as f32))
		.collect::<Vec<_>>();
	Ok(Arc::new(Float32Array::from(values)) as ArrayRef)
}

fn cast_i32_to_f64(array: &ArrayRef) -> Result<ArrayRef, ExecutionError> {
	let array = as_int32(array, "Cast source")?;
	let values = array.iter().map(|v| v.map(f64::from)).collect::<Vec<_>>();
	Ok(Arc::new(Float64Array::from(values)) as ArrayRef)
}

fn cast_i64_to_i32(array: &ArrayRef) -> Result<ArrayRef, ExecutionError> {
	let array = as_int64(array, "Cast source")?;
	let values = array
		.iter()
		.map(|value| -> Result<Option<i32>, ExecutionError> {
			let Some(value) = value else {
				return Ok(None);
			};
			if value < i32::MIN as i64 || value > i32::MAX as i64 {
				return Err(cast_failed(
					&DataType::Int64,
					&DataType::Int32,
					format!("value {value} does not fit"),
				));
			}
			Ok(Some(value as i32))
		})
		.collect::<Result<Vec<_>, _>>()?;
	Ok(Arc::new(Int32Array::from(values)) as ArrayRef)
}

fn cast_i64_to_f32(array: &ArrayRef) -> Result<ArrayRef, ExecutionError> {
	let array = as_int64(array, "Cast source")?;
	let values = array
		.iter()
		.map(|v| v.map(|v| v as f32))
		.collect::<Vec<_>>();
	Ok(Arc::new(Float32Array::from(values)) as ArrayRef)
}

fn cast_i64_to_f64(array: &ArrayRef) -> Result<ArrayRef, ExecutionError> {
	let array = as_int64(array, "Cast source")?;
	let values = array
		.iter()
		.map(|v| v.map(|v| v as f64))
		.collect::<Vec<_>>();
	Ok(Arc::new(Float64Array::from(values)) as ArrayRef)
}

fn cast_f32_to_i32(array: &ArrayRef) -> Result<ArrayRef, ExecutionError> {
	let array = as_float32(array, "Cast source")?;
	let values = array
		.iter()
		.map(|value| -> Result<Option<i32>, ExecutionError> {
			let Some(value) = value else {
				return Ok(None);
			};
			if value.is_nan() {
				return Err(cast_failed(
					&DataType::Float32,
					&DataType::Int32,
					"NaN has no integer image",
				));
			}
			if value.is_infinite() {
				return Err(cast_failed(
					&DataType::Float32,
					&DataType::Int32,
					"Inf has no integer image",
				));
			}
			let trunc = f64::from(value).trunc();
			if !(F64_I32_LOWER..F64_I32_UPPER).contains(&trunc) {
				return Err(cast_failed(
					&DataType::Float32,
					&DataType::Int32,
					format!("value {value} truncated to {trunc} does not fit Int32"),
				));
			}
			Ok(Some(trunc as i32))
		})
		.collect::<Result<Vec<_>, _>>()?;
	Ok(Arc::new(Int32Array::from(values)) as ArrayRef)
}

fn cast_f32_to_i64(array: &ArrayRef) -> Result<ArrayRef, ExecutionError> {
	let array = as_float32(array, "Cast source")?;
	let values = array
		.iter()
		.map(|value| -> Result<Option<i64>, ExecutionError> {
			let Some(value) = value else {
				return Ok(None);
			};
			if value.is_nan() {
				return Err(cast_failed(
					&DataType::Float32,
					&DataType::Int64,
					"NaN has no integer image",
				));
			}
			if value.is_infinite() {
				return Err(cast_failed(
					&DataType::Float32,
					&DataType::Int64,
					"Inf has no integer image",
				));
			}
			let trunc = f64::from(value).trunc();
			if !(F64_I64_LOWER..F64_I64_UPPER).contains(&trunc) {
				return Err(cast_failed(
					&DataType::Float32,
					&DataType::Int64,
					format!("value {value} truncated to {trunc} does not fit Int64"),
				));
			}
			Ok(Some(trunc as i64))
		})
		.collect::<Result<Vec<_>, _>>()?;
	Ok(Arc::new(Int64Array::from(values)) as ArrayRef)
}

fn cast_f32_to_f64(array: &ArrayRef) -> Result<ArrayRef, ExecutionError> {
	let array = as_float32(array, "Cast source")?;
	let values = array.iter().map(|v| v.map(f64::from)).collect::<Vec<_>>();
	Ok(Arc::new(Float64Array::from(values)) as ArrayRef)
}

fn cast_f64_to_i32(array: &ArrayRef) -> Result<ArrayRef, ExecutionError> {
	let array = as_float64(array, "Cast source")?;
	let values = array
		.iter()
		.map(|value| -> Result<Option<i32>, ExecutionError> {
			let Some(value) = value else {
				return Ok(None);
			};
			if value.is_nan() {
				return Err(cast_failed(
					&DataType::Float64,
					&DataType::Int32,
					"NaN has no integer image",
				));
			}
			if value.is_infinite() {
				return Err(cast_failed(
					&DataType::Float64,
					&DataType::Int32,
					"Inf has no integer image",
				));
			}
			let trunc = value.trunc();
			if !(F64_I32_LOWER..F64_I32_UPPER).contains(&trunc) {
				return Err(cast_failed(
					&DataType::Float64,
					&DataType::Int32,
					format!("value {value} truncated to {trunc} does not fit Int32"),
				));
			}
			Ok(Some(trunc as i32))
		})
		.collect::<Result<Vec<_>, _>>()?;
	Ok(Arc::new(Int32Array::from(values)) as ArrayRef)
}

fn cast_f64_to_i64(array: &ArrayRef) -> Result<ArrayRef, ExecutionError> {
	let array = as_float64(array, "Cast source")?;
	let values = array
		.iter()
		.map(|value| -> Result<Option<i64>, ExecutionError> {
			let Some(value) = value else {
				return Ok(None);
			};
			if value.is_nan() {
				return Err(cast_failed(
					&DataType::Float64,
					&DataType::Int64,
					"NaN has no integer image",
				));
			}
			if value.is_infinite() {
				return Err(cast_failed(
					&DataType::Float64,
					&DataType::Int64,
					"Inf has no integer image",
				));
			}
			let trunc = value.trunc();
			if !(F64_I64_LOWER..F64_I64_UPPER).contains(&trunc) {
				return Err(cast_failed(
					&DataType::Float64,
					&DataType::Int64,
					format!("value {value} truncated to {trunc} does not fit Int64"),
				));
			}
			Ok(Some(trunc as i64))
		})
		.collect::<Result<Vec<_>, _>>()?;
	Ok(Arc::new(Int64Array::from(values)) as ArrayRef)
}

fn cast_f64_to_f32(array: &ArrayRef) -> Result<ArrayRef, ExecutionError> {
	let array = as_float64(array, "Cast source")?;
	let values = array
		.iter()
		.map(|value| -> Result<Option<f32>, ExecutionError> {
			let Some(value) = value else {
				return Ok(None);
			};
			if value.is_finite() && value.abs() > f32::MAX as f64 {
				return Err(cast_failed(
					&DataType::Float64,
					&DataType::Float32,
					format!("finite value {value} overflows Float32"),
				));
			}
			Ok(Some(value as f32))
		})
		.collect::<Result<Vec<_>, _>>()?;
	Ok(Arc::new(Float32Array::from(values)) as ArrayRef)
}

fn try_cast_i64_to_i32(array: &ArrayRef) -> Result<ArrayRef, ExecutionError> {
	let array = as_int64(array, "TryCast source")?;
	let values = array
		.iter()
		.map(|value| -> Option<i32> {
			let value = value?;
			if value < i32::MIN as i64 || value > i32::MAX as i64 {
				return None;
			}
			Some(value as i32)
		})
		.collect::<Vec<_>>();
	Ok(Arc::new(Int32Array::from(values)) as ArrayRef)
}

fn try_cast_f64_to_f32(array: &ArrayRef) -> Result<ArrayRef, ExecutionError> {
	let array = as_float64(array, "TryCast source")?;
	let values = array
		.iter()
		.map(|value| -> Option<f32> {
			let value = value?;
			if value.is_finite() && value.abs() > f32::MAX as f64 {
				return None;
			}
			Some(value as f32)
		})
		.collect::<Vec<_>>();
	Ok(Arc::new(Float32Array::from(values)) as ArrayRef)
}

fn try_cast_f32_to_i32(array: &ArrayRef) -> Result<ArrayRef, ExecutionError> {
	let array = as_float32(array, "TryCast source")?;
	let values = array
		.iter()
		.map(|value| -> Option<i32> {
			let value = value?;
			if value.is_nan() {
				return None;
			}
			if value.is_infinite() {
				return None;
			}
			let trunc = f64::from(value).trunc();
			if !(F64_I32_LOWER..F64_I32_UPPER).contains(&trunc) {
				return None;
			}
			Some(trunc as i32)
		})
		.collect::<Vec<_>>();
	Ok(Arc::new(Int32Array::from(values)) as ArrayRef)
}

fn try_cast_f32_to_i64(array: &ArrayRef) -> Result<ArrayRef, ExecutionError> {
	let array = as_float32(array, "TryCast source")?;
	let values = array
		.iter()
		.map(|value| -> Option<i64> {
			let value = value?;
			if value.is_nan() {
				return None;
			}
			if value.is_infinite() {
				return None;
			}
			let trunc = f64::from(value).trunc();
			if !(F64_I64_LOWER..F64_I64_UPPER).contains(&trunc) {
				return None;
			}
			Some(trunc as i64)
		})
		.collect::<Vec<_>>();
	Ok(Arc::new(Int64Array::from(values)) as ArrayRef)
}

fn try_cast_f64_to_i32(array: &ArrayRef) -> Result<ArrayRef, ExecutionError> {
	let array = as_float64(array, "TryCast source")?;
	let values = array
		.iter()
		.map(|value| -> Option<i32> {
			let value = value?;
			if value.is_nan() {
				return None;
			}
			if value.is_infinite() {
				return None;
			}
			let trunc = value.trunc();
			if !(F64_I32_LOWER..F64_I32_UPPER).contains(&trunc) {
				return None;
			}
			Some(trunc as i32)
		})
		.collect::<Vec<_>>();
	Ok(Arc::new(Int32Array::from(values)) as ArrayRef)
}

fn try_cast_f64_to_i64(array: &ArrayRef) -> Result<ArrayRef, ExecutionError> {
	let array = as_float64(array, "TryCast source")?;
	let values = array
		.iter()
		.map(|value| -> Option<i64> {
			let value = value?;
			if value.is_nan() {
				return None;
			}
			if value.is_infinite() {
				return None;
			}
			let trunc = value.trunc();
			if !(F64_I64_LOWER..F64_I64_UPPER).contains(&trunc) {
				return None;
			}
			Some(trunc as i64)
		})
		.collect::<Vec<_>>();
	Ok(Arc::new(Int64Array::from(values)) as ArrayRef)
}

fn evaluate_arithmetic(
	op: ArithmeticOp,
	left: &ArrayRef,
	right: &ArrayRef,
	output_type: &DataType,
) -> Result<ArrayRef, ExecutionError> {
	if left.len() != right.len() {
		return Err(ExecutionError::InvalidExpression {
			message: format!(
				"arithmetic operand length mismatch: left {}, right {}",
				left.len(),
				right.len()
			),
		});
	}
	if left.data_type() != output_type || right.data_type() != output_type {
		return Err(ExecutionError::InvalidExpression {
			message: format!(
				"arithmetic operands must already be {:?}, got {:?} and {:?}",
				output_type,
				left.data_type(),
				right.data_type()
			),
		});
	}

	match output_type {
		DataType::Int32 => evaluate_int32_arithmetic(op, left, right),
		DataType::Int64 => evaluate_int64_arithmetic(op, left, right),
		DataType::Float32 => evaluate_float32_arithmetic(op, left, right),
		DataType::Float64 => evaluate_float64_arithmetic(op, left, right),
		other => Err(ExecutionError::InvalidExpression {
			message: format!("unsupported arithmetic output type: {other:?}"),
		}),
	}
}

fn evaluate_temporal_arithmetic(
	op: ArithmeticOp,
	left: &ArrayRef,
	right: &ArrayRef,
) -> Result<Option<ArrayRef>, ExecutionError> {
	let left_is_calendar = is_calendar_month_interval_type_exec(left.data_type());
	let right_is_calendar = is_calendar_month_interval_type_exec(right.data_type());
	if left_is_calendar || right_is_calendar {
		if left.len() != right.len() {
			return Err(ExecutionError::InvalidExpression {
				message: format!(
					"temporal arithmetic operand length mismatch: left {}, right {}",
					left.len(),
					right.len()
				),
			});
		}
		return Ok(Some(evaluate_calendar_arithmetic(op, left, right)?));
	}
	let left_unit = temporal_timestamp_unit(left.data_type())?;
	let right_unit = temporal_timestamp_unit(right.data_type())?;
	let left_is_interval = left.data_type() == &DataType::Duration(TimeUnit::Nanosecond);
	let right_is_interval = right.data_type() == &DataType::Duration(TimeUnit::Nanosecond);
	if left_unit.is_none() && right_unit.is_none() && !left_is_interval && !right_is_interval {
		return Ok(None);
	}
	if left.len() != right.len() {
		return Err(ExecutionError::InvalidExpression {
			message: format!(
				"temporal arithmetic operand length mismatch: left {}, right {}",
				left.len(),
				right.len()
			),
		});
	}
	if (left_is_interval || right_is_interval)
		&& matches!(op, ArithmeticOp::Mul | ArithmeticOp::Div)
	{
		return Err(ExecutionError::InvalidExpression {
			message: "Interval multiplication and division are deferred".to_string(),
		});
	}

	match op {
		ArithmeticOp::Add => match (left_unit, right_unit, left_is_interval, right_is_interval) {
			(Some(unit), None, false, true) => {
				let durations = as_duration_nanos(right, "Timestamp + Interval right operand")?;
				Ok(Some(evaluate_timestamp_interval(
					op, left, durations, unit,
				)?))
			}
			(None, Some(unit), true, false) => {
				let durations = as_duration_nanos(left, "Interval + Timestamp left operand")?;
				Ok(Some(evaluate_timestamp_interval(
					op, right, durations, unit,
				)?))
			}
			(None, None, true, true) => Ok(Some(evaluate_interval_interval(op, left, right)?)),
			(Some(_), Some(_), _, _) => Err(ExecutionError::InvalidExpression {
				message: "Timestamp + Timestamp is not supported".to_string(),
			}),
			_ => Err(ExecutionError::InvalidExpression {
				message: format!(
					"Interval arithmetic expects Interval and UTC Timestamp operands, got {:?} and {:?}",
					left.data_type(),
					right.data_type()
				),
			}),
		},
		ArithmeticOp::Sub => match (left_unit, right_unit, left_is_interval, right_is_interval) {
			(Some(unit), None, false, true) => {
				let durations = as_duration_nanos(right, "Timestamp - Interval right operand")?;
				Ok(Some(evaluate_timestamp_interval(
					op, left, durations, unit,
				)?))
			}
			(Some(left_unit), Some(right_unit), _, _) => {
				if left_unit != right_unit {
					return Err(ExecutionError::InvalidExpression {
						message: "mixed-unit Timestamp - Timestamp is deferred in ADR 152"
							.to_string(),
					});
				}
				Ok(Some(evaluate_timestamp_difference(left, right, left_unit)?))
			}
			(None, Some(_), true, false) => Err(ExecutionError::InvalidExpression {
				message: "Interval - Timestamp is not supported".to_string(),
			}),
			(None, None, true, true) => Ok(Some(evaluate_interval_interval(op, left, right)?)),
			_ => Err(ExecutionError::InvalidExpression {
				message: format!(
					"Interval arithmetic expects Interval and UTC Timestamp operands, got {:?} and {:?}",
					left.data_type(),
					right.data_type()
				),
			}),
		},
		ArithmeticOp::Mul | ArithmeticOp::Div => Ok(None),
	}
}

fn evaluate_calendar_arithmetic(
	op: ArithmeticOp,
	left: &ArrayRef,
	right: &ArrayRef,
) -> Result<ArrayRef, ExecutionError> {
	let left_timestamp = calendar_timestamp_context(left.data_type())?;
	let right_timestamp = calendar_timestamp_context(right.data_type())?;
	let left_is_calendar = is_calendar_month_interval_type_exec(left.data_type());
	let right_is_calendar = is_calendar_month_interval_type_exec(right.data_type());
	let left_is_date32 = matches!(left.data_type(), DataType::Date32);
	let right_is_date32 = matches!(right.data_type(), DataType::Date32);

	if (left_is_calendar || right_is_calendar)
		&& matches!(op, ArithmeticOp::Mul | ArithmeticOp::Div)
	{
		return Err(ExecutionError::InvalidExpression {
			message: "CalendarMonthInterval multiplication and division are deferred".to_string(),
		});
	}
	if (left_is_calendar && right.data_type() == &DataType::Duration(TimeUnit::Nanosecond))
		|| (left.data_type() == &DataType::Duration(TimeUnit::Nanosecond) && right_is_calendar)
	{
		return Err(ExecutionError::InvalidExpression {
			message:
				"CalendarMonthInterval and fixed-duration Interval arithmetic is not supported"
					.to_string(),
		});
	}

	match op {
		ArithmeticOp::Add => match (
			left_timestamp,
			right_timestamp,
			left_is_date32,
			right_is_date32,
			left_is_calendar,
			right_is_calendar,
		) {
			(Some((unit, timezone)), None, false, false, false, true) => {
				let intervals = as_interval_year_month(
					right,
					"Timestamp + CalendarMonthInterval right operand",
				)?;
				evaluate_timestamp_calendar_interval(op, left, intervals, unit, timezone)
			}
			(None, Some((unit, timezone)), false, false, true, false) => {
				let intervals =
					as_interval_year_month(left, "CalendarMonthInterval + Timestamp left operand")?;
				evaluate_timestamp_calendar_interval(op, right, intervals, unit, timezone)
			}
			(None, None, true, false, false, true) => {
				let intervals =
					as_interval_year_month(right, "Date32 + CalendarMonthInterval right operand")?;
				evaluate_date32_calendar_interval(op, left, intervals)
			}
			(None, None, false, false, true, true) => {
				evaluate_calendar_interval_interval(op, left, right)
			}
			_ => Err(calendar_arithmetic_mismatch_exec(
				left.data_type(),
				right.data_type(),
			)),
		},
		ArithmeticOp::Sub => match (
			left_timestamp,
			right_timestamp,
			left_is_date32,
			right_is_date32,
			left_is_calendar,
			right_is_calendar,
		) {
			(Some((unit, timezone)), None, false, false, false, true) => {
				let intervals = as_interval_year_month(
					right,
					"Timestamp - CalendarMonthInterval right operand",
				)?;
				evaluate_timestamp_calendar_interval(op, left, intervals, unit, timezone)
			}
			(None, None, true, false, false, true) => {
				let intervals =
					as_interval_year_month(right, "Date32 - CalendarMonthInterval right operand")?;
				evaluate_date32_calendar_interval(op, left, intervals)
			}
			(None, None, false, false, true, true) => {
				evaluate_calendar_interval_interval(op, left, right)
			}
			(None, Some(_), false, false, true, false) => Err(ExecutionError::InvalidExpression {
				message: "CalendarMonthInterval - Timestamp is not supported".to_string(),
			}),
			_ => Err(calendar_arithmetic_mismatch_exec(
				left.data_type(),
				right.data_type(),
			)),
		},
		ArithmeticOp::Mul | ArithmeticOp::Div => Err(ExecutionError::InvalidExpression {
			message: "CalendarMonthInterval multiplication and division are deferred".to_string(),
		}),
	}
}

fn calendar_timestamp_context(
	data_type: &DataType,
) -> Result<Option<(TimeUnit, chrono_tz::Tz)>, ExecutionError> {
	match data_type {
		DataType::Timestamp(
			unit @ (TimeUnit::Millisecond | TimeUnit::Microsecond | TimeUnit::Nanosecond),
			Some(zone),
		) => Ok(Some((*unit, parse_temporal_execution_zone(zone.as_ref())?))),
		DataType::Timestamp(_, None) => Err(ExecutionError::InvalidExpression {
			message: "calendar interval arithmetic does not support Timestamp without timezone"
				.to_string(),
		}),
		DataType::Timestamp(unit, Some(_)) => Err(ExecutionError::InvalidExpression {
			message: format!(
				"calendar interval arithmetic does not support Timestamp unit {unit:?}"
			),
		}),
		_ => Ok(None),
	}
}

fn is_calendar_month_interval_type_exec(data_type: &DataType) -> bool {
	matches!(data_type, DataType::Interval(IntervalUnit::YearMonth))
}

fn evaluate_timestamp_calendar_interval(
	op: ArithmeticOp,
	timestamps: &ArrayRef,
	intervals: &IntervalYearMonthArray,
	unit: TimeUnit,
	timezone: chrono_tz::Tz,
) -> Result<ArrayRef, ExecutionError> {
	if let Some(array) = timestamps
		.as_any()
		.downcast_ref::<TimestampMillisecondArray>()
	{
		return timestamp_calendar_interval_array(op, array, intervals, unit, timezone);
	}
	if let Some(array) = timestamps
		.as_any()
		.downcast_ref::<TimestampMicrosecondArray>()
	{
		return timestamp_calendar_interval_array(op, array, intervals, unit, timezone);
	}
	if let Some(array) = timestamps
		.as_any()
		.downcast_ref::<TimestampNanosecondArray>()
	{
		return timestamp_calendar_interval_array(op, array, intervals, unit, timezone);
	}
	Err(ExecutionError::InvalidExpression {
		message: format!(
			"Timestamp +/- CalendarMonthInterval expects Timestamp input, got {:?}",
			timestamps.data_type()
		),
	})
}

fn timestamp_calendar_interval_array<T>(
	op: ArithmeticOp,
	timestamps: &T,
	intervals: &IntervalYearMonthArray,
	unit: TimeUnit,
	timezone: chrono_tz::Tz,
) -> Result<ArrayRef, ExecutionError>
where
	for<'a> &'a T: IntoIterator<Item = Option<i64>>,
	T: Array,
{
	let data_type = timestamps.data_type().clone();
	let values = timestamps
		.into_iter()
		.zip(intervals.iter())
		.map(|(timestamp, months)| {
			let Some(timestamp) = timestamp else {
				return Ok(None);
			};
			let Some(months) = months else {
				return Ok(None);
			};
			let months = match op {
				ArithmeticOp::Add => months,
				ArithmeticOp::Sub => negate_calendar_months_exec(months, op, &data_type)?,
				ArithmeticOp::Mul | ArithmeticOp::Div => {
					return Err(ExecutionError::InvalidExpression {
						message: "CalendarMonthInterval multiplication and division are deferred"
							.to_string(),
					});
				}
			};
			crate::timestamp::add_calendar_months(timestamp, unit, timezone, months)
				.map(Some)
				.map_err(|error| ExecutionError::InvalidExpression {
					message: error.to_string(),
				})
		})
		.collect::<Result<Vec<_>, ExecutionError>>()?;
	Ok(timestamp_array_from_values(unit, values))
}

fn evaluate_date32_calendar_interval(
	op: ArithmeticOp,
	dates: &ArrayRef,
	intervals: &IntervalYearMonthArray,
) -> Result<ArrayRef, ExecutionError> {
	let Some(dates) = dates.as_any().downcast_ref::<Date32Array>() else {
		return Err(ExecutionError::InvalidExpression {
			message: format!(
				"Date32 +/- CalendarMonthInterval expects Date32 input, got {:?}",
				dates.data_type()
			),
		});
	};
	let data_type = dates.data_type().clone();
	let values = dates
		.iter()
		.zip(intervals.iter())
		.map(|(days, months)| {
			let Some(days) = days else {
				return Ok(None);
			};
			let Some(months) = months else {
				return Ok(None);
			};
			let months = match op {
				ArithmeticOp::Add => months,
				ArithmeticOp::Sub => negate_calendar_months_exec(months, op, &data_type)?,
				ArithmeticOp::Mul | ArithmeticOp::Div => {
					return Err(ExecutionError::InvalidExpression {
						message: "CalendarMonthInterval multiplication and division are deferred"
							.to_string(),
					});
				}
			};
			crate::date32::add_calendar_months(days, months)
				.map(Some)
				.map_err(|error| ExecutionError::InvalidExpression {
					message: error.to_string(),
				})
		})
		.collect::<Result<Vec<_>, ExecutionError>>()?;
	Ok(Arc::new(Date32Array::from(values)) as ArrayRef)
}

fn evaluate_calendar_interval_interval(
	op: ArithmeticOp,
	left: &ArrayRef,
	right: &ArrayRef,
) -> Result<ArrayRef, ExecutionError> {
	let left = as_interval_year_month(left, "CalendarMonthInterval arithmetic left operand")?;
	let right = as_interval_year_month(right, "CalendarMonthInterval arithmetic right operand")?;
	let data_type = DataType::Interval(IntervalUnit::YearMonth);
	let values = left
		.iter()
		.zip(right.iter())
		.map(|(left, right)| {
			let Some(left) = left else {
				return Ok(None);
			};
			let Some(right) = right else {
				return Ok(None);
			};
			let value = match op {
				ArithmeticOp::Add => left.checked_add(right),
				ArithmeticOp::Sub => left.checked_sub(right),
				ArithmeticOp::Mul | ArithmeticOp::Div => None,
			}
			.ok_or_else(|| temporal_arithmetic_overflow_exec(op, &data_type))?;
			Ok(Some(value))
		})
		.collect::<Result<Vec<_>, ExecutionError>>()?;
	Ok(Arc::new(IntervalYearMonthArray::from(values)) as ArrayRef)
}

fn negate_calendar_months_exec(
	months: i32,
	op: ArithmeticOp,
	data_type: &DataType,
) -> Result<i32, ExecutionError> {
	months
		.checked_neg()
		.ok_or_else(|| temporal_arithmetic_overflow_exec(op, data_type))
}

fn calendar_arithmetic_mismatch_exec(left: &DataType, right: &DataType) -> ExecutionError {
	ExecutionError::InvalidExpression {
		message: format!(
			"CalendarMonthInterval arithmetic expects Timestamp, Date32, or CalendarMonthInterval operands, got {left:?} and {right:?}"
		),
	}
}

fn temporal_timestamp_unit(data_type: &DataType) -> Result<Option<TimeUnit>, ExecutionError> {
	match data_type {
		DataType::Timestamp(
			unit @ (TimeUnit::Millisecond | TimeUnit::Microsecond | TimeUnit::Nanosecond),
			Some(timezone),
		) if timezone.as_ref() == "UTC" => Ok(Some(*unit)),
		DataType::Timestamp(_, None) => Err(ExecutionError::InvalidExpression {
			message: "temporal interval arithmetic does not support Timestamp without timezone"
				.to_string(),
		}),
		DataType::Timestamp(_, Some(timezone)) if timezone.as_ref() != "UTC" => {
			Err(ExecutionError::InvalidExpression {
				message: format!(
					"temporal interval arithmetic requires UTC Timestamp, got timezone {timezone}"
				),
			})
		}
		DataType::Timestamp(unit, Some(_)) => Err(ExecutionError::InvalidExpression {
			message: format!(
				"temporal interval arithmetic does not support Timestamp unit {unit:?}"
			),
		}),
		_ => Ok(None),
	}
}

fn evaluate_timestamp_interval(
	op: ArithmeticOp,
	timestamps: &ArrayRef,
	durations: &DurationNanosecondArray,
	unit: TimeUnit,
) -> Result<ArrayRef, ExecutionError> {
	if let Some(array) = timestamps
		.as_any()
		.downcast_ref::<TimestampMillisecondArray>()
	{
		return timestamp_interval_array(op, array, durations, unit);
	}
	if let Some(array) = timestamps
		.as_any()
		.downcast_ref::<TimestampMicrosecondArray>()
	{
		return timestamp_interval_array(op, array, durations, unit);
	}
	if let Some(array) = timestamps
		.as_any()
		.downcast_ref::<TimestampNanosecondArray>()
	{
		return timestamp_interval_array(op, array, durations, unit);
	}
	Err(ExecutionError::InvalidExpression {
		message: format!(
			"Timestamp +/- Interval expects Timestamp input, got {:?}",
			timestamps.data_type()
		),
	})
}

fn timestamp_interval_array<T>(
	op: ArithmeticOp,
	timestamps: &T,
	durations: &DurationNanosecondArray,
	unit: TimeUnit,
) -> Result<ArrayRef, ExecutionError>
where
	for<'a> &'a T: IntoIterator<Item = Option<i64>>,
	T: Array,
{
	let values = timestamps
		.into_iter()
		.zip(durations.iter())
		.map(|(timestamp, interval_nanos)| {
			let Some(timestamp) = timestamp else {
				return Ok(None);
			};
			let Some(interval_nanos) = interval_nanos else {
				return Ok(None);
			};
			let interval_ticks =
				checked_interval_nanos_to_timestamp_ticks_exec(interval_nanos, unit)?;
			let value = match op {
				ArithmeticOp::Add => timestamp.checked_add(interval_ticks),
				ArithmeticOp::Sub => timestamp.checked_sub(interval_ticks),
				ArithmeticOp::Mul | ArithmeticOp::Div => None,
			}
			.ok_or_else(|| temporal_arithmetic_overflow_exec(op, timestamps.data_type()))?;
			Ok(Some(value))
		})
		.collect::<Result<Vec<_>, ExecutionError>>()?;
	Ok(timestamp_array_from_values(unit, values))
}

fn evaluate_timestamp_difference(
	left: &ArrayRef,
	right: &ArrayRef,
	unit: TimeUnit,
) -> Result<ArrayRef, ExecutionError> {
	if let (Some(left), Some(right)) = (
		left.as_any().downcast_ref::<TimestampMillisecondArray>(),
		right.as_any().downcast_ref::<TimestampMillisecondArray>(),
	) {
		return timestamp_difference_array(left, right, unit);
	}
	if let (Some(left), Some(right)) = (
		left.as_any().downcast_ref::<TimestampMicrosecondArray>(),
		right.as_any().downcast_ref::<TimestampMicrosecondArray>(),
	) {
		return timestamp_difference_array(left, right, unit);
	}
	if let (Some(left), Some(right)) = (
		left.as_any().downcast_ref::<TimestampNanosecondArray>(),
		right.as_any().downcast_ref::<TimestampNanosecondArray>(),
	) {
		return timestamp_difference_array(left, right, unit);
	}
	Err(ExecutionError::InvalidExpression {
		message: format!(
			"Timestamp - Timestamp expects matching timestamp arrays, got {:?} and {:?}",
			left.data_type(),
			right.data_type()
		),
	})
}

fn timestamp_difference_array<T>(
	left: &T,
	right: &T,
	unit: TimeUnit,
) -> Result<ArrayRef, ExecutionError>
where
	for<'a> &'a T: IntoIterator<Item = Option<i64>>,
	T: Array,
{
	let nanos_per_unit =
		nanos_per_timestamp_unit(unit).ok_or_else(|| ExecutionError::InvalidExpression {
			message: format!("Timestamp unit {unit:?} is not supported for interval arithmetic"),
		})?;
	let data_type = left.data_type().clone();
	let values = left
		.into_iter()
		.zip(right.into_iter())
		.map(|(left_value, right_value)| {
			let Some(left_value) = left_value else {
				return Ok(None);
			};
			let Some(right_value) = right_value else {
				return Ok(None);
			};
			let ticks = left_value
				.checked_sub(right_value)
				.ok_or_else(|| temporal_arithmetic_overflow_exec(ArithmeticOp::Sub, &data_type))?;
			let nanos = ticks.checked_mul(nanos_per_unit).ok_or_else(|| {
				temporal_arithmetic_overflow_exec(
					ArithmeticOp::Sub,
					&DataType::Duration(TimeUnit::Nanosecond),
				)
			})?;
			Ok(Some(nanos))
		})
		.collect::<Result<Vec<_>, ExecutionError>>()?;
	Ok(Arc::new(DurationNanosecondArray::from(values)) as ArrayRef)
}

fn evaluate_interval_interval(
	op: ArithmeticOp,
	left: &ArrayRef,
	right: &ArrayRef,
) -> Result<ArrayRef, ExecutionError> {
	let left = as_duration_nanos(left, "Interval arithmetic left operand")?;
	let right = as_duration_nanos(right, "Interval arithmetic right operand")?;
	let values = left
		.iter()
		.zip(right.iter())
		.map(|(left, right)| {
			let Some(left) = left else {
				return Ok(None);
			};
			let Some(right) = right else {
				return Ok(None);
			};
			let value = match op {
				ArithmeticOp::Add => left.checked_add(right),
				ArithmeticOp::Sub => left.checked_sub(right),
				ArithmeticOp::Mul | ArithmeticOp::Div => None,
			}
			.ok_or_else(|| {
				temporal_arithmetic_overflow_exec(op, &DataType::Duration(TimeUnit::Nanosecond))
			})?;
			Ok(Some(value))
		})
		.collect::<Result<Vec<_>, ExecutionError>>()?;
	Ok(Arc::new(DurationNanosecondArray::from(values)) as ArrayRef)
}

fn timestamp_array_from_values(unit: TimeUnit, values: Vec<Option<i64>>) -> ArrayRef {
	match unit {
		TimeUnit::Millisecond => {
			Arc::new(TimestampMillisecondArray::from(values).with_timezone("UTC")) as ArrayRef
		}
		TimeUnit::Microsecond => {
			Arc::new(TimestampMicrosecondArray::from(values).with_timezone("UTC")) as ArrayRef
		}
		TimeUnit::Nanosecond => {
			Arc::new(TimestampNanosecondArray::from(values).with_timezone("UTC")) as ArrayRef
		}
		TimeUnit::Second => unreachable!("TimestampSecond is not a supported Toron timestamp unit"),
	}
}

fn checked_interval_nanos_to_timestamp_ticks_exec(
	interval_nanos: i64,
	unit: TimeUnit,
) -> Result<i64, ExecutionError> {
	let nanos_per_unit =
		nanos_per_timestamp_unit(unit).ok_or_else(|| ExecutionError::InvalidExpression {
			message: format!("Timestamp unit {unit:?} is not supported for interval arithmetic"),
		})?;
	if interval_nanos % nanos_per_unit != 0 {
		return Err(ExecutionError::InvalidExpression {
			message: format!(
				"interval nanoseconds {interval_nanos} are not exactly representable at {} precision",
				timestamp_precision_name(unit)
			),
		});
	}
	Ok(interval_nanos / nanos_per_unit)
}

fn timestamp_precision_name(unit: TimeUnit) -> &'static str {
	match unit {
		TimeUnit::Second => "second",
		TimeUnit::Millisecond => "millisecond",
		TimeUnit::Microsecond => "microsecond",
		TimeUnit::Nanosecond => "nanosecond",
	}
}

fn temporal_arithmetic_overflow_exec(op: ArithmeticOp, data_type: &DataType) -> ExecutionError {
	ExecutionError::Kernel {
		message: format!("temporal arithmetic overflow: {}({data_type:?})", op.name()),
	}
}

fn evaluate_int32_arithmetic(
	op: ArithmeticOp,
	left: &ArrayRef,
	right: &ArrayRef,
) -> Result<ArrayRef, ExecutionError> {
	let left = as_int32(left, "Int32 arithmetic left operand")?;
	let right = as_int32(right, "Int32 arithmetic right operand")?;
	let values = (0..left.len())
		.map(|row| -> Result<Option<i32>, ExecutionError> {
			if left.is_null(row) || right.is_null(row) {
				return Ok(None);
			}
			let left = left.value(row);
			let right = right.value(row);
			let value = match op {
				ArithmeticOp::Add => left.checked_add(right),
				ArithmeticOp::Sub => left.checked_sub(right),
				ArithmeticOp::Mul => left.checked_mul(right),
				ArithmeticOp::Div => {
					if right == 0 {
						return Ok(None);
					}
					left.checked_div(right)
				}
			}
			.ok_or_else(|| arithmetic_overflow(op, &DataType::Int32))?;
			Ok(Some(value))
		})
		.collect::<Result<Vec<_>, _>>()?;
	Ok(Arc::new(Int32Array::from(values)) as ArrayRef)
}

fn evaluate_int64_arithmetic(
	op: ArithmeticOp,
	left: &ArrayRef,
	right: &ArrayRef,
) -> Result<ArrayRef, ExecutionError> {
	let left = as_int64(left, "Int64 arithmetic left operand")?;
	let right = as_int64(right, "Int64 arithmetic right operand")?;
	let values = (0..left.len())
		.map(|row| -> Result<Option<i64>, ExecutionError> {
			if left.is_null(row) || right.is_null(row) {
				return Ok(None);
			}
			let left = left.value(row);
			let right = right.value(row);
			let value = match op {
				ArithmeticOp::Add => left.checked_add(right),
				ArithmeticOp::Sub => left.checked_sub(right),
				ArithmeticOp::Mul => left.checked_mul(right),
				ArithmeticOp::Div => {
					if right == 0 {
						return Ok(None);
					}
					left.checked_div(right)
				}
			}
			.ok_or_else(|| arithmetic_overflow(op, &DataType::Int64))?;
			Ok(Some(value))
		})
		.collect::<Result<Vec<_>, _>>()?;
	Ok(Arc::new(Int64Array::from(values)) as ArrayRef)
}

fn evaluate_float32_arithmetic(
	op: ArithmeticOp,
	left: &ArrayRef,
	right: &ArrayRef,
) -> Result<ArrayRef, ExecutionError> {
	let left = as_float32(left, "Float32 arithmetic left operand")?;
	let right = as_float32(right, "Float32 arithmetic right operand")?;
	let values = (0..left.len())
		.map(|row| {
			if left.is_null(row) || right.is_null(row) {
				return None;
			}
			let left = left.value(row);
			let right = right.value(row);
			Some(match op {
				ArithmeticOp::Add => left + right,
				ArithmeticOp::Sub => left - right,
				ArithmeticOp::Mul => left * right,
				ArithmeticOp::Div => left / right,
			})
		})
		.collect::<Vec<_>>();
	Ok(Arc::new(Float32Array::from(values)) as ArrayRef)
}

fn evaluate_float64_arithmetic(
	op: ArithmeticOp,
	left: &ArrayRef,
	right: &ArrayRef,
) -> Result<ArrayRef, ExecutionError> {
	let left = as_float64(left, "Float64 arithmetic left operand")?;
	let right = as_float64(right, "Float64 arithmetic right operand")?;
	let values = (0..left.len())
		.map(|row| {
			if left.is_null(row) || right.is_null(row) {
				return None;
			}
			let left = left.value(row);
			let right = right.value(row);
			Some(match op {
				ArithmeticOp::Add => left + right,
				ArithmeticOp::Sub => left - right,
				ArithmeticOp::Mul => left * right,
				ArithmeticOp::Div => left / right,
			})
		})
		.collect::<Vec<_>>();
	Ok(Arc::new(Float64Array::from(values)) as ArrayRef)
}

fn arithmetic_overflow(op: ArithmeticOp, data_type: &DataType) -> ExecutionError {
	ExecutionError::Kernel {
		message: format!(
			"arithmetic overflow: {}({})",
			op.name(),
			arithmetic_data_type_name(data_type)
		),
	}
}

fn arithmetic_data_type_name(data_type: &DataType) -> &'static str {
	match data_type {
		DataType::Int32 => "Int32",
		DataType::Int64 => "Int64",
		DataType::Float32 => "Float32",
		DataType::Float64 => "Float64",
		_ => "Unsupported",
	}
}

fn as_int32<'a>(
	array: &'a ArrayRef,
	context: &'static str,
) -> Result<&'a Int32Array, ExecutionError> {
	array
		.as_any()
		.downcast_ref::<Int32Array>()
		.ok_or_else(|| ExecutionError::InvalidExpression {
			message: format!("{context} must be Int32"),
		})
}

fn as_int64<'a>(
	array: &'a ArrayRef,
	context: &'static str,
) -> Result<&'a Int64Array, ExecutionError> {
	array
		.as_any()
		.downcast_ref::<Int64Array>()
		.ok_or_else(|| ExecutionError::InvalidExpression {
			message: format!("{context} must be Int64"),
		})
}

fn as_duration_nanos<'a>(
	array: &'a ArrayRef,
	context: &'static str,
) -> Result<&'a DurationNanosecondArray, ExecutionError> {
	array
		.as_any()
		.downcast_ref::<DurationNanosecondArray>()
		.ok_or_else(|| ExecutionError::InvalidExpression {
			message: format!("{context} must be Duration(Nanosecond)"),
		})
}

fn as_interval_year_month<'a>(
	array: &'a ArrayRef,
	context: &'static str,
) -> Result<&'a IntervalYearMonthArray, ExecutionError> {
	array
		.as_any()
		.downcast_ref::<IntervalYearMonthArray>()
		.ok_or_else(|| ExecutionError::InvalidExpression {
			message: format!("{context} must be Interval(YearMonth)"),
		})
}

fn as_float32<'a>(
	array: &'a ArrayRef,
	context: &'static str,
) -> Result<&'a Float32Array, ExecutionError> {
	array
		.as_any()
		.downcast_ref::<Float32Array>()
		.ok_or_else(|| ExecutionError::InvalidExpression {
			message: format!("{context} must be Float32"),
		})
}

fn as_float64<'a>(
	array: &'a ArrayRef,
	context: &'static str,
) -> Result<&'a Float64Array, ExecutionError> {
	array
		.as_any()
		.downcast_ref::<Float64Array>()
		.ok_or_else(|| ExecutionError::InvalidExpression {
			message: format!("{context} must be Float64"),
		})
}

fn as_boolean<'a>(
	array: &'a ArrayRef,
	context: &'static str,
) -> Result<&'a BooleanArray, ExecutionError> {
	array
		.as_any()
		.downcast_ref::<BooleanArray>()
		.ok_or_else(|| ExecutionError::InvalidExpression {
			message: format!("{context} must be boolean"),
		})
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::binder::{PlannerConfig, bind};
	use crate::catalog::{MockCatalog, TableRef};
	use crate::distributed::determinism::{SortPolicy, canonical_arrow_ipc_digest};
	use crate::error::StorageError;
	use crate::ir::{
		BoundExprProgram, BoundExprProgramBuilder, ColumnName, ColumnOrdinal, QueryId,
		QueryProfileHint, ResolvedColumn, SessionId, TableId,
	};
	use crate::storage::InMemoryStorageScan;
	use crate::storage::ScanStream;
	use crate::storage::{HlcBound, TenantId};
	use arrow::array::{
		Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, StringArray,
		UInt8Array, UInt16Array, UInt32Array, UInt64Array,
	};
	use arrow::datatypes::{DataType, Field, Schema};
	use std::hash::{DefaultHasher, Hash, Hasher};
	use std::sync::Mutex;
	use wendelon::schema::{
		LocalityPolicy, PrimaryKeyDescriptor, RowIdentityPolicy, RuntimeFilterBloom,
		RuntimeFilterDescriptor, RuntimeFilterKeyType, RuntimeFilterKind, SchemaEvolutionPolicy,
		TableObjectId, TableSchemaDescriptor,
	};

	// ADR 271: the scalar Decimal `/` output type — grounded Arrow shape, validated explicitly.
	#[test]
	fn decimal_division_output_type_validates() {
		// q14: (38,8) / (32,4) -> scale = 8+4 = 12, precision = min(38, 38-8+4+12) = 38.
		assert_eq!(decimal_division_output_type(38, 8, 32, 4), Some((38, 12)));
		// (10,2) / (10,2) -> scale 6, precision = min(38, 10-2+2+6) = 16.
		assert_eq!(decimal_division_output_type(10, 2, 10, 2), Some((16, 6)));
		// Over-wide numerator scale: s1=36 -> scale 40 > 38 -> rejected (no silent cap).
		assert_eq!(decimal_division_output_type(38, 36, 10, 2), None);
	}

	// ADR 271: the scalar Decimal `/` kernel — half-away-from-zero rounding (discriminated from truncation),
	// a NEGATIVE denominator (rounds away in the quotient's sign), div-by-zero -> NULL (ADR 084), and NULL
	// operand propagation. (The 2-row q14 fixture can't discriminate these.)
	#[test]
	fn evaluate_decimal_division_rounds_half_away_and_nulls() {
		let decimal = |values: Vec<Option<i128>>| -> ArrayRef {
			Arc::new(
				Decimal128Array::from(values)
					.with_precision_and_scale(20, 0)
					.expect("decimal operand"),
			)
		};
		// 1 / 20000 = 0.00005 -> at scale 4, half away from zero -> 0.0001 (truncation would be 0.0000).
		let left = decimal(vec![Some(1), Some(1), Some(1), None]);
		let right = decimal(vec![Some(20_000), Some(-20_000), Some(0), Some(5)]);
		let out = evaluate_decimal_division(&left, &right).expect("decimal division");
		let out = out
			.as_any()
			.downcast_ref::<Decimal128Array>()
			.expect("decimal128 result");
		// (20,0) / (20,0) -> scale 4, precision min(38, 20-0+0+4) = 24.
		assert_eq!(out.data_type(), &DataType::Decimal128(24, 4));
		assert_eq!(
			out.value(0),
			1,
			"half away from zero rounds 0.00005 up to 0.0001"
		);
		assert_eq!(
			out.value(1),
			-1,
			"a negative denominator rounds away from zero -> -0.0001"
		);
		assert!(out.is_null(2), "div-by-zero -> NULL (ADR 084)");
		assert!(out.is_null(3), "a NULL operand -> NULL");
	}

	// ADR 268: the decimal AVG finalize rescales to the widened output scale and divides half away from
	// zero, in pure integer arithmetic. Pins the rounding sign (positive AND negative ties) and the
	// non-divisible case the discriminating execute test can't reach from a 2-row fixture.
	#[test]
	fn avg_decimal_finalize_rounds_half_away_from_zero() {
		let avg = |total: i128, count: u64, input_scale: i8, p: u8, s: i8| {
			avg_decimal_finalize(
				&AggState::AvgDecimal {
					total,
					count,
					input_scale,
				},
				p,
				s,
			)
			.expect("finalize ok")
		};
		// Non-divisible: AVG(0.01, 0.02, 0.02) at scale 2 -> sum raw 5, count 3, widen to scale 6:
		// 5*10^4/3 = 50000/3 = 16666.67 -> 16667 (0.016667).
		assert_eq!(avg(5, 3, 2, 19, 6), Some(16667));
		// Exact: AVG(0.06, 0.05) -> sum 11, count 2 -> 110000/2 = 55000 (0.055000), no rounding.
		assert_eq!(avg(11, 2, 2, 19, 6), Some(55000));
		// Positive tie: 5/2 = 2.5 -> 3 (away from zero).
		assert_eq!(avg(5, 2, 0, 19, 0), Some(3));
		// Negative tie: -5/2 = -2.5 -> -3 (away from zero, not -2).
		assert_eq!(avg(-5, 2, 0, 19, 0), Some(-3));
		// Empty group -> NULL.
		assert_eq!(avg(0, 0, 2, 19, 6), None);
	}

	#[test]
	fn avg_decimal_state_rejects_unrepresentable_widening() {
		assert!(
			matches!(
				avg_state_for_data_type(&DataType::Decimal128(15, 2)),
				Ok(AggState::AvgDecimal { .. })
			),
			"ordinary Decimal128 AVG state is allowed"
		);
		assert!(
			avg_state_for_data_type(&DataType::Decimal128(38, 35)).is_err(),
			"scale 35 would widen to scale 39, which cannot fit Decimal128"
		);
	}

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

	#[test]
	fn numeric_cast_kernels_truncate_preserve_nulls_and_reject_edges() {
		let f64_values =
			Arc::new(Float64Array::from(vec![Some(3.9), Some(-3.9), None])) as ArrayRef;
		let int32_values = evaluate_cast(&f64_values, &DataType::Int32)
			.expect("Float64 to Int32")
			.as_any()
			.downcast_ref::<Int32Array>()
			.expect("Int32 cast")
			.clone();
		assert_eq!(int32_values.value(0), 3);
		assert_eq!(int32_values.value(1), -3);
		assert!(int32_values.is_null(2));

		let int64_values =
			Arc::new(Int64Array::from(vec![Some(i64::MAX), Some(i64::MIN)])) as ArrayRef;
		let float32_values = evaluate_cast(&int64_values, &DataType::Float32)
			.expect("Int64 to Float32")
			.as_any()
			.downcast_ref::<Float32Array>()
			.expect("Float32 cast")
			.clone();
		assert!(float32_values.value(0).is_finite());
		assert!(float32_values.value(1).is_finite());

		let special_f64 = Arc::new(Float64Array::from(vec![
			Some(f64::NAN),
			Some(f64::INFINITY),
			Some(f64::NEG_INFINITY),
			None,
		])) as ArrayRef;
		let special_f32 = evaluate_cast(&special_f64, &DataType::Float32)
			.expect("Float64 special values to Float32")
			.as_any()
			.downcast_ref::<Float32Array>()
			.expect("Float32 cast")
			.clone();
		assert!(special_f32.value(0).is_nan());
		assert!(special_f32.value(1).is_infinite());
		assert!(special_f32.value(2).is_infinite());
		assert!(special_f32.is_null(3));

		let i64_overflow =
			Arc::new(Int64Array::from(vec![Some(i64::from(i32::MAX) + 1)])) as ArrayRef;
		assert_cast_kernel_prefix(
			evaluate_cast(&i64_overflow, &DataType::Int32)
				.expect_err("Int64 narrowing overflow should fail"),
			"cast failed: Int64 to Int32:",
		);

		let f64_int_overflow = Arc::new(Float64Array::from(vec![Some(2147483648.0)])) as ArrayRef;
		assert_cast_kernel_prefix(
			evaluate_cast(&f64_int_overflow, &DataType::Int32)
				.expect_err("Float64 to Int32 exclusive upper bound should fail"),
			"cast failed: Float64 to Int32:",
		);

		let f64_nan = Arc::new(Float64Array::from(vec![Some(f64::NAN)])) as ArrayRef;
		assert_cast_kernel_prefix(
			evaluate_cast(&f64_nan, &DataType::Int64).expect_err("NaN to Int64 should fail"),
			"cast failed: Float64 to Int64: NaN",
		);

		let f64_float_overflow =
			Arc::new(Float64Array::from(vec![Some((f32::MAX as f64) * 2.0)])) as ArrayRef;
		assert_cast_kernel_prefix(
			evaluate_cast(&f64_float_overflow, &DataType::Float32)
				.expect_err("finite Float64 overflow to Float32 should fail"),
			"cast failed: Float64 to Float32:",
		);
	}

	fn assert_cast_kernel_prefix(err: ExecutionError, expected_prefix: &str) {
		match err {
			ExecutionError::Kernel { message } => assert!(
				message.starts_with(expected_prefix),
				"expected cast kernel prefix {expected_prefix:?}, got {message:?}"
			),
			other => panic!("expected kernel error, got {other:?}"),
		}
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
				memory: ByteCount::new(32 * 1024),
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
	async fn streaming_collapsing_aggregate_shape_fence_keeps_grouped_path_materialized() {
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
			output_schema,
		};
		let (executor, _, ..) =
			streaming_aggregate_fixture(StreamingValueKind::Float64, rows, 1024, low_budget);

		let err = executor
			.execute_bound(&grouped, None)
			.await
			.expect_err("grouped aggregate is outside the streaming shape fence");

		assert!(matches!(
			err,
			ExecutionError::BudgetExhausted {
				dimension: BudgetDimension::Memory,
				..
			}
		));
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
		);
		let (materialized_executor, materialized_plan) = reoriented_inner_chain_fixture(
			StreamingValueKind::Float64,
			rows,
			0,
			high_test_budget(),
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
		);
		let (materialized_executor, materialized_plan) = reoriented_inner_chain_fixture(
			StreamingValueKind::Decimal,
			rows,
			0,
			high_test_budget(),
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
		let rows = CROSS_JOIN_OUTPUT_CHUNK_ROWS * 4 + 2_000;
		let budget = TaskBudget {
			memory: ByteCount::new(512 * 1024),
			spill_bytes: ByteCount::new(256 * 1024 * 1024),
			..TaskBudget::from_query_budget(QueryBudget::test_default())
		};

		let (materialized_executor, materialized_plan) =
			reoriented_inner_chain_fixture(StreamingValueKind::Float64, rows, 256, budget, false);
		let materialized_err = materialized_executor
			.execute_bound(&materialized_plan, None)
			.await
			.expect_err("replay-id-disabled chain should keep the materialized wall");
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
			reoriented_inner_chain_fixture(StreamingValueKind::Float64, rows, 256, budget, true);
		let streamed = stream_executor
			.execute_bound(&stream_plan, None)
			.await
			.expect("streaming chain should avoid the materialized intermediate wall");
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
		let (executor, plan) =
			reoriented_inner_chain_fixture(StreamingValueKind::Decimal, rows, 256, budget, false);

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
			match_filter: &residual,
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
			match_filter: &residual,
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
			StreamingSemiAntiExistence::InMemory { .. } => {
				panic!("matched existence path must use the streamed-spilled build")
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

	// ADR 275: group_value reads a Decimal128 key as the raw i128 (NULL handled by the leading null-check).
	#[test]
	fn group_value_reads_decimal128_raw_i128() {
		let array: ArrayRef = Arc::new(
			Decimal128Array::from(vec![Some(12_345_i128), None, Some(-67_800_i128)])
				.with_precision_and_scale(15, 2)
				.expect("decimal column"),
		);
		assert_eq!(
			group_value(&array, 0).expect("group value"),
			GroupValue::Decimal128(12_345)
		);
		assert_eq!(
			group_value(&array, 1).expect("group value"),
			GroupValue::Null
		);
		assert_eq!(
			group_value(&array, 2).expect("group value"),
			GroupValue::Decimal128(-67_800)
		);
	}

	// ADR 275: group_column rebuilds the Decimal128 group key with the EXACT (precision, scale) from the column
	// type and the exact raw i128 values (incl. NULL). The core (p,s) round-trip the q10 digest can't pin.
	#[test]
	fn group_column_rebuilds_decimal128_with_exact_precision_scale_and_values() {
		let keys = vec![
			vec![GroupValue::Decimal128(12_345)],
			vec![GroupValue::Decimal128(-67_800)],
			vec![GroupValue::Null],
		];
		let column = group_column(&keys, 0, &arrow::datatypes::DataType::Decimal128(15, 2))
			.expect("group column");
		assert_eq!(
			column.data_type(),
			&arrow::datatypes::DataType::Decimal128(15, 2)
		);
		let decimals = column
			.as_any()
			.downcast_ref::<Decimal128Array>()
			.expect("decimal128 column");
		assert_eq!(decimals.value(0), 12_345);
		assert_eq!(decimals.value(1), -67_800);
		assert!(decimals.is_null(2));
	}

	#[test]
	fn join_key_type_tag_distinguishes_int32_and_int64() {
		let int32_key = JoinKey::Int32(1);
		let int64_key = JoinKey::Int64(1);

		assert_ne!(int32_key, int64_key);
		assert_ne!(hash_join_key(&int32_key), hash_join_key(&int64_key));
	}

	#[test]
	fn join_build_row_list_stores_unique_row_inline() {
		let mut rows = HashMap::new();

		insert_join_build_row(&mut rows, JoinKey::Int64(7), 11);

		let stored = rows.get(&JoinKey::Int64(7)).expect("row list");
		assert_eq!(stored.rows().collect::<Vec<_>>(), vec![11]);
		assert_eq!(stored.overflow_len(), 0);
	}

	#[test]
	fn join_build_row_list_allocates_overflow_for_duplicates() {
		let mut rows = HashMap::new();

		insert_join_build_row(&mut rows, JoinKey::Int64(7), 11);
		insert_join_build_row(&mut rows, JoinKey::Int64(7), 12);
		insert_join_build_row(&mut rows, JoinKey::Int64(7), 13);

		let stored = rows.get(&JoinKey::Int64(7)).expect("row list");
		assert_eq!(stored.rows().collect::<Vec<_>>(), vec![11, 12, 13]);
		assert_eq!(stored.overflow_len(), 2);
	}

	#[test]
	fn join_build_hash_map_preallocates_for_input_rows() {
		let key_arrays =
			vec![Arc::new(Int64Array::from((0_i64..32).collect::<Vec<_>>())) as ArrayRef];

		let rows = build_in_memory_join_rows(JoinKeyPlan::Int64, &key_arrays, 32, u64::MAX, false)
			.expect("build rows")
			.expect("in memory rows");

		assert!(rows.capacity() >= 32);
		assert!(rows.values().all(|list| list.overflow_len() == 0));
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
			.map(|row| i64::try_from(row % 3).expect("row key fits"))
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
		let orders_batch = RecordBatch::try_new(
			orders_schema.clone(),
			vec![
				Arc::new(Int64Array::from(vec![0, 1, 2])),
				Arc::new(Int64Array::from(vec![100, 200, 100])),
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
			match_filter: residual,
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
