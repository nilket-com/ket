#[cfg(test)]
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
use crate::ir::{
	AggregateFunction, BoundAggregateCall, BoundBinaryOp, BoundExprId, BoundExprNode,
	BoundExprProgram, BoundUnaryOp, BoundWindowCall, ExecutionPlan, ExecutionSortKey,
	FilterDisposition, JoinKind, JoinReplayId, JoinStrategy, MorselId, QueryId, QueryProfileHint,
	ScalarValue, SessionId, TableId, TemporalExtractField, TemporalTruncField, WindowFrame,
	WindowFrameBound, WindowFrameDelta, WindowFrameUnit, WindowFunction, nanos_per_timestamp_unit,
};
#[cfg(test)]
use crate::ir::{Expr, LogicalPlan};
use crate::storage::{HlcBound, ScanRequest, ScanTablePin, StorageScan, TenantId, TraceContext};
use arrow::array::{
	Array, ArrayRef, BooleanArray, BooleanBuilder, Date32Array, Date32Builder,
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
use arrow::compute::{
	SortColumn, SortOptions, concat_batches, filter_record_batch, lexsort_to_indices, take,
};
use arrow::datatypes::{DataType, IntervalUnit, SchemaRef, TimeUnit};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use aws_lc_rs::aead::{AES_256_GCM_SIV, Aad, LessSafeKey, Nonce, UnboundKey};
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fmt;
use std::io::ErrorKind;
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
static NEXT_JOIN_SPILL_ID: AtomicU64 = AtomicU64::new(1);

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
								let mut state = AggState::AvgFloat {
									total: 0.0,
									count: 0,
								};
								for batch in &input_batches {
									let arr = self.evaluate_expr(&args[0], batch)?;
									for row in 0..batch.num_rows() {
										if let Some(value) = numeric_value(&arr, row)? {
											add_avg_value(&mut state, value)?;
										}
									}
								}
								results_cols.push(avg_state_array(&state)?);
								results_fields.push(arrow::datatypes::Field::new(
									"avg",
									arrow::datatypes::DataType::Float64,
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
								results_cols.push(min_max_state_array(&state)?);
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
				LogicalPlan::SetOpAll { .. } => Err(ExecutionError::NotImplemented {
					feature: "unbound logical set-op ALL execution",
				}),
				LogicalPlan::Repeat { .. } => Err(ExecutionError::NotImplemented {
					feature: "unbound logical repeat execution",
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
					legacy_table_id,
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
						table_id: *legacy_table_id,
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
					let input_batches = self.execute_bound(input, morsel).await?;
					if group_expr.is_empty_roots() {
						self.bound_global_aggregate(&input_batches, aggr_expr, output_schema)
					} else {
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
					literal_array(value, batch.num_rows())
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
		let mut results_cols = Vec::new();
		for call in aggr_expr {
			match call.function {
				AggregateFunction::CountAll => {
					let total = input_batches
						.iter()
						.map(|batch| batch.num_rows() as i64)
						.sum::<i64>();
					results_cols.push(Arc::new(Int64Array::from(vec![total])) as ArrayRef);
				}
				AggregateFunction::Count => {
					let mut total = 0i64;
					for batch in input_batches {
						let arr =
							self.evaluate_bound_single_root(&call.args, batch, "COUNT arg")?;
						for row in 0..batch.num_rows() {
							if !arr.is_null(row) {
								total += 1;
							}
						}
					}
					results_cols.push(Arc::new(Int64Array::from(vec![total])) as ArrayRef);
				}
				AggregateFunction::Sum => {
					let mut state = sum_state_for_data_type(first_arg_type(call)?);
					for batch in input_batches {
						let arr = self.evaluate_bound_single_root(&call.args, batch, "SUM arg")?;
						for row in 0..batch.num_rows() {
							if let Some(value) = numeric_value(&arr, row)? {
								add_sum_value(&mut state, value)?;
							}
						}
					}
					let output_type = output_schema.field(results_cols.len()).data_type().clone();
					results_cols.push(sum_state_array(&state, &output_type)?);
				}
				AggregateFunction::Avg => {
					let mut state = avg_state_for_data_type(first_arg_type(call)?)?;
					for batch in input_batches {
						let arr = self.evaluate_bound_single_root(&call.args, batch, "AVG arg")?;
						for row in 0..batch.num_rows() {
							if let Some(value) = numeric_value(&arr, row)? {
								add_avg_value(&mut state, value)?;
							}
						}
					}
					results_cols.push(avg_state_array(&state)?);
				}
				AggregateFunction::Min => {
					let mut state = min_state_for_data_type(first_arg_type(call)?)?;
					for batch in input_batches {
						let arr = self.evaluate_bound_single_root(&call.args, batch, "MIN arg")?;
						for row in 0..batch.num_rows() {
							update_min_state(&mut state, &arr, row)?;
						}
					}
					results_cols.push(min_max_state_array(&state)?);
				}
				AggregateFunction::Max => {
					let mut state = max_state_for_data_type(first_arg_type(call)?)?;
					for batch in input_batches {
						let arr = self.evaluate_bound_single_root(&call.args, batch, "MAX arg")?;
						for row in 0..batch.num_rows() {
							update_max_state(&mut state, &arr, row)?;
						}
					}
					results_cols.push(min_max_state_array(&state)?);
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
							AggState::AvgInt { .. } | AggState::AvgFloat { .. } => {
								if let Some(value) = numeric_value(array, row)? {
									add_avg_value(state, value)?;
								}
							}
							AggState::MinInt32 { .. }
							| AggState::MinInt64 { .. }
							| AggState::MinFloat32 { .. }
							| AggState::MinFloat64 { .. } => {
								update_min_state(state, array, row)?;
							}
							AggState::MaxInt32 { .. }
							| AggState::MaxInt64 { .. }
							| AggState::MaxFloat32 { .. }
							| AggState::MaxFloat64 { .. } => {
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
							| AggState::MaxInt32 { .. }
							| AggState::MaxInt64 { .. }
							| AggState::MaxFloat32 { .. }
							| AggState::MaxFloat64 { .. }
							| AggState::AvgInt { .. }
							| AggState::AvgFloat { .. } => 0,
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
					columns.push(avg_column_from_groups(&groups, agg_idx)?);
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
			let group_arrays = self.evaluate_bound_program(group_expr, batch)?;
			if group_types.is_empty() {
				group_types = group_arrays
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
							AggState::AvgInt { .. } | AggState::AvgFloat { .. } => {
								if let Some(value) = numeric_value(array, row)? {
									add_avg_value(state, value)?;
								}
							}
							AggState::MinInt32 { .. }
							| AggState::MinInt64 { .. }
							| AggState::MinFloat32 { .. }
							| AggState::MinFloat64 { .. } => {
								update_min_state(state, array, row)?;
							}
							AggState::MaxInt32 { .. }
							| AggState::MaxInt64 { .. }
							| AggState::MaxFloat32 { .. }
							| AggState::MaxFloat64 { .. } => {
								update_max_state(state, array, row)?;
							}
						},
					}
				}
			}
		}

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
							| AggState::MaxInt32 { .. }
							| AggState::MaxInt64 { .. }
							| AggState::MaxFloat32 { .. }
							| AggState::MaxFloat64 { .. }
							| AggState::AvgInt { .. }
							| AggState::AvgFloat { .. } => 0,
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
					columns.push(avg_column_from_groups(&groups, agg_idx)?);
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
			let num_rows = batch.num_rows();
			if num_rows == 0 {
				continue;
			}

			let key_arrays = self.evaluate_bound_program(key, &batch)?;

			let mut builders: Vec<Vec<u32>> = (0..partition_count).map(|_| Vec::new()).collect();
			for row in 0..num_rows {
				let bucket = partition_bucket(&key_arrays, row, partition_count)?;
				builders[bucket].push(row as u32);
			}

			for (partition, indices) in builders.into_iter().enumerate() {
				if !indices.is_empty() {
					let indices_arr = arrow::array::UInt32Array::from(indices);
					let mut cols = Vec::new();
					for col in batch.columns() {
						cols.push(arrow::compute::take(col, &indices_arr, None)?);
					}
					partition_batches[partition].push(RecordBatch::try_new(batch.schema(), cols)?);
				}
			}
		}

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

fn literal_array(value: &ScalarValue, num_rows: usize) -> ArrayRef {
	match value {
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
		ScalarValue::Utf8(value) => {
			Arc::new(StringArray::from(vec![value.clone().as_deref(); num_rows]))
		}
	}
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
) -> Result<RecordBatch, ExecutionError> {
	if selection.is_empty() {
		return Ok(RecordBatch::new_empty(output_schema.clone()));
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
	null_safe_keys: bool,
) -> Result<Vec<JoinOutputRow>, ExecutionError> {
	let mut selection = Vec::new();
	let mut matched_build_rows = build_match_bitmap(kind, left_rows);

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
) -> Result<bool, ExecutionError> {
	let right_index = join_probe_row_index(probe_row)?;
	let mut matched = false;
	for left_index in matches {
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
		)?]);
	}

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
		null_safe_keys,
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

async fn probe_spilled_hash_join(
	kind: JoinKind,
	spilled: &mut SpilledJoinBuild,
	right_batch: &RecordBatch,
	right_key_arrays: &[ArrayRef],
	left_batch: &RecordBatch,
	output_schema: &arrow::datatypes::SchemaRef,
	null_safe_keys: bool,
) -> Result<Vec<RecordBatch>, ExecutionError> {
	let mut selection = Vec::new();
	let mut matched_build_rows = build_match_bitmap(kind, left_batch.num_rows());
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
	)?])
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

	// Value-offset bounds need the sorted key + arithmetic; peer/`UNBOUNDED` bounds never touch the key
	// (so peer/whole-partition RANGE binds on any single key, even multi-key or `Utf8`). One RowConverter
	// (direction + nulls matching the partition sort) encodes both the key and the computed bound values,
	// so comparisons reduce to byte-ordered row comparisons and the encoded key is non-decreasing with row
	// position inside a partition.
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

	let mut row_arrays: Vec<ArrayRef> = Vec::with_capacity(rows);
	for row in 0..rows {
		let p_start = partition_start[row];
		let p_end = partition_end[row];
		// A NULL ORDER BY value has no value distance: its frame is just its NULL peer group, so value
		// offsets collapse to the peer boundary (UNBOUNDED bounds still reach the partition edge).
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

		let mut count: i64 = 0;
		let mut state = fresh_window_agg_state(function, arg_type.as_ref())?;
		if let (Some(lo), Some(hi)) = (lo, hi)
			&& lo <= hi
		{
			for slot in lo..=hi {
				window_agg_add(function, &mut state, &mut count, sorted_arg, slot)?;
			}
		}
		row_arrays.push(window_agg_finalize(function, &state, count, output_type)?);
	}
	concat_window_columns(&row_arrays)
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
	let offset_array = literal_array(offset, key.len());
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
		AggregateFunction::Avg => avg_state_array(state.as_ref().expect("avg window state"))?,
		AggregateFunction::Min | AggregateFunction::Max => {
			min_max_state_array(state.as_ref().expect("min/max window state"))?
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
		WindowFrameDelta::Range(_) => i64::MAX,
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

async fn write_join_spill_page(
	file_ops: &dyn AsyncFileOps,
	file: &FileHandle,
	key: &LessSafeKey,
	spill_id: u64,
	page_index: usize,
	offset: u64,
	entries: &[(Vec<u8>, Vec<u32>)],
) -> Result<JoinSpillPage, ExecutionError> {
	let mut plaintext = encode_join_spill_page(entries)?;
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

fn join_spill_nonce(spill_id: u64, page_index: usize) -> [u8; 12] {
	let mut nonce = [0u8; 12];
	nonce[..8].copy_from_slice(&spill_id.to_le_bytes());
	nonce[8..].copy_from_slice(&u32::try_from(page_index).unwrap_or(u32::MAX).to_le_bytes());
	nonce
}

fn budget_exhausted(dimension: BudgetDimension, requested: u64, available: u64) -> ExecutionError {
	ExecutionError::BudgetExhausted {
		dimension,
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
}

#[derive(Debug, Clone, Copy)]
enum AggState {
	Count(i64),
	SumInt { total: i128, seen: bool },
	SumFloat { total: f64, seen: bool },
	MinInt32 { value: Option<i32> },
	MinInt64 { value: Option<i64> },
	MinFloat32 { value: Option<f32> },
	MinFloat64 { value: Option<f64> },
	MaxInt32 { value: Option<i32> },
	MaxInt64 { value: Option<i64> },
	MaxFloat32 { value: Option<f32> },
	MaxFloat64 { value: Option<f64> },
	AvgInt { total: i128, count: u64 },
	AvgFloat { total: f64, count: u64 },
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
		AggregateFunction::Avg => DataType::Float64,
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
		| (AggState::MaxInt32 { .. }, _)
		| (AggState::MaxInt64 { .. }, _)
		| (AggState::MaxFloat32 { .. }, _)
		| (AggState::MaxFloat64 { .. }, _)
		| (AggState::AvgInt { .. }, _)
		| (AggState::AvgFloat { .. }, _) => Err(ExecutionError::InvalidExpression {
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
		(AggState::Count(_), _)
		| (AggState::SumInt { .. }, _)
		| (AggState::SumFloat { .. }, _)
		| (AggState::MinInt32 { .. }, _)
		| (AggState::MinInt64 { .. }, _)
		| (AggState::MinFloat32 { .. }, _)
		| (AggState::MinFloat64 { .. }, _)
		| (AggState::MaxInt32 { .. }, _)
		| (AggState::MaxInt64 { .. }, _)
		| (AggState::MaxFloat32 { .. }, _)
		| (AggState::MaxFloat64 { .. }, _) => Err(ExecutionError::InvalidExpression {
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
		_ => Err(ExecutionError::InvalidExpression {
			message: "MAX value reached non-MAX accumulator".to_string(),
		}),
	}
}

fn min_max_state_array(state: &AggState) -> Result<ArrayRef, ExecutionError> {
	match state {
		AggState::MinInt32 { value } | AggState::MaxInt32 { value } => {
			Ok(Arc::new(Int32Array::from(vec![*value])) as ArrayRef)
		}
		AggState::MinInt64 { value } | AggState::MaxInt64 { value } => {
			Ok(Arc::new(Int64Array::from(vec![*value])) as ArrayRef)
		}
		AggState::MinFloat32 { value } | AggState::MaxFloat32 { value } => {
			Ok(Arc::new(Float32Array::from(vec![*value])) as ArrayRef)
		}
		AggState::MinFloat64 { value } | AggState::MaxFloat64 { value } => {
			Ok(Arc::new(Float64Array::from(vec![*value])) as ArrayRef)
		}
		_ => Err(ExecutionError::InvalidExpression {
			message: "MIN/MAX finalize over non-MIN/MAX state".to_string(),
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
					| AggState::MaxInt32 { .. }
					| AggState::MaxInt64 { .. }
					| AggState::MaxFloat32 { .. }
					| AggState::MaxFloat64 { .. }
					| AggState::AvgInt { .. }
					| AggState::AvgFloat { .. } => Ok(None),
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
					| AggState::MaxInt32 { .. }
					| AggState::MaxInt64 { .. }
					| AggState::MaxFloat32 { .. }
					| AggState::MaxFloat64 { .. }
					| AggState::AvgInt { .. }
					| AggState::AvgFloat { .. } => None,
				})
				.collect::<Vec<_>>();
			Ok(Arc::new(Float64Array::from(values)) as ArrayRef)
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

fn avg_state_array(state: &AggState) -> Result<ArrayRef, ExecutionError> {
	Ok(Arc::new(Float64Array::from(vec![avg_finalize(state)?])) as ArrayRef)
}

fn avg_column_from_groups(
	groups: &BTreeMap<Vec<GroupValue>, Vec<AggState>>,
	agg_idx: usize,
) -> Result<ArrayRef, ExecutionError> {
	let values = groups
		.values()
		.map(|states| avg_finalize(&states[agg_idx]))
		.collect::<Result<Vec<_>, _>>()?;
	Ok(Arc::new(Float64Array::from(values)) as ArrayRef)
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
	use crate::ir::{ColumnOrdinal, QueryId, QueryProfileHint, SessionId, TableId};
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
			legacy_table_id: TableId(1),
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
			legacy_table_id: table_id,
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
