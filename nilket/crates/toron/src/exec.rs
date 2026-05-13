use crate::coordinator::materialize_scan_stream;
use crate::error::ExecutionError;
use crate::ir::{
	AggregateFunction, ExecutionExpr, ExecutionPlan, FilterDisposition, MorselId, QueryId,
	QueryProfileHint, ScalarValue, SessionId, TableId,
};
#[cfg(test)]
use crate::ir::{Expr, LogicalPlan};
use crate::storage::{HlcBound, ScanRequest, ScanTablePin, StorageScan, TenantId, TraceContext};
use arrow::array::{
	Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
	Int64Array, StringArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::compute::filter_record_batch;
use arrow::compute::kernels::boolean::{and, is_not_null, is_null, not, or};
use arrow::compute::kernels::cmp::{eq, gt, gt_eq, lt, lt_eq, neq};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use twox_hash::XxHash3_64;
use wendelon::schema::{
	LocalityPolicy, PrimaryKeyDescriptorV1, RowIdentityPolicy, SchemaEvolutionPolicyV1,
	TableObjectId, TableSchemaDescriptor,
};
#[cfg(test)]
use wendelon::storage::sketch::KllSketch;

pub struct Executor {
	storage: Arc<dyn StorageScan>,
	context: ExecutionContext,
}

#[derive(Debug, Clone)]
pub struct ExecutionContext {
	pub session_id: SessionId,
	pub query_id: QueryId,
	pub tenant_id: TenantId,
	pub read_bound: HlcBound,
	pub profile_hint: Option<QueryProfileHint>,
	pub trace_context: Option<TraceContext>,
	pub legacy_table_pins: HashMap<TableId, ScanTablePin>,
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
		}
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
						profile_hint: self.context.profile_hint,
						trace_context: self.context.trace_context.clone(),
						morsel,
					};
					let stream = self.storage.scan(req).await?;
					Ok(materialize_scan_stream(stream).await?)
				}
				LogicalPlan::ScanNamed { .. } => Err(ExecutionError::NotImplemented {
					feature: "unbound named scan execution",
				}),
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
				LogicalPlan::Project { input, exprs } => {
					let input_batches = self.execute(input, morsel).await?;
					let mut output = Vec::new();
					for batch in input_batches {
						let mut cols = Vec::new();
						let mut fields = Vec::new();
						for (i, expr) in exprs.iter().enumerate() {
							let array = self.evaluate_expr(expr, &batch)?;
							cols.push(array.clone());
							let field = arrow::datatypes::Field::new(
								format!("col_{}", i),
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
										if let Some(value) = sum_value(&arr, row)? {
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
				ExecutionPlan::Scan {
					legacy_table_id,
					table,
					fingerprint,
					projection,
					pbc_program,
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
						profile_hint: Some(*profile_hint),
						trace_context: self.context.trace_context.clone(),
						morsel,
					};
					let stream = self.storage.scan(req).await?;
					Ok(materialize_scan_stream(stream).await?)
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

					let mut output = Vec::new();
					for batch in input_batches {
						let mask = self.evaluate_bound_expr(predicate, &batch)?;
						let mask_bool =
							mask.as_any()
								.downcast_ref::<BooleanArray>()
								.ok_or_else(|| ExecutionError::InvalidExpression {
									message: "filter predicate must return boolean".to_string(),
								})?;
						output.push(filter_record_batch(&batch, mask_bool)?);
					}
					Ok(output)
				}
				ExecutionPlan::Project { input, exprs, .. } => {
					let input_batches = self.execute_bound(input, morsel).await?;
					let mut output = Vec::new();
					for batch in input_batches {
						let mut cols = Vec::new();
						let mut fields = Vec::new();
						for (i, expr) in exprs.iter().enumerate() {
							let array = self.evaluate_bound_expr(expr, &batch)?;
							cols.push(array.clone());
							fields.push(arrow::datatypes::Field::new(
								format!("col_{i}"),
								array.data_type().clone(),
								true,
							));
						}
						let schema = Arc::new(arrow::datatypes::Schema::new(fields));
						output.push(RecordBatch::try_new(schema, cols)?);
					}
					Ok(output)
				}
				ExecutionPlan::Aggregate {
					input,
					group_expr,
					aggr_expr,
					output_schema,
				} => {
					let input_batches = self.execute_bound(input, morsel).await?;
					if group_expr.is_empty() {
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
			Expr::Column(name) => {
				let ordinal = batch
					.schema()
					.index_of(name)
					.map_err(|_| ExecutionError::ColumnNotFound { name: name.clone() })?;
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
		}
	}

	pub fn evaluate_bound_expr(
		&self,
		expr: &ExecutionExpr,
		batch: &RecordBatch,
	) -> Result<ArrayRef, ExecutionError> {
		match expr {
			ExecutionExpr::Column(column) => {
				let ordinal = column.ordinal.0 as usize;
				let col = batch.columns().get(ordinal).ok_or_else(|| {
					ExecutionError::InvalidExpression {
						message: format!("column ordinal {ordinal} out of bounds"),
					}
				})?;
				Ok(col.clone())
			}
			ExecutionExpr::Literal(val) => {
				let num_rows = batch.num_rows();
				match val {
					ScalarValue::Boolean(v) => Ok(Arc::new(BooleanArray::from(vec![*v; num_rows]))),
					ScalarValue::Int32(v) => Ok(Arc::new(Int32Array::from(vec![*v; num_rows]))),
					ScalarValue::Int64(v) => {
						Ok(Arc::new(arrow::array::Int64Array::from(vec![*v; num_rows])))
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
			ExecutionExpr::Eq(left, right) => {
				let left_arr = self.evaluate_bound_expr(left, batch)?;
				let right_arr = self.evaluate_bound_expr(right, batch)?;
				Ok(Arc::new(eq(&left_arr, &right_arr)?))
			}
			ExecutionExpr::Neq(left, right) => {
				let left_arr = self.evaluate_bound_expr(left, batch)?;
				let right_arr = self.evaluate_bound_expr(right, batch)?;
				Ok(Arc::new(neq(&left_arr, &right_arr)?))
			}
			ExecutionExpr::Lt(left, right) => {
				let left_arr = self.evaluate_bound_expr(left, batch)?;
				let right_arr = self.evaluate_bound_expr(right, batch)?;
				Ok(Arc::new(lt(&left_arr, &right_arr)?))
			}
			ExecutionExpr::Lte(left, right) => {
				let left_arr = self.evaluate_bound_expr(left, batch)?;
				let right_arr = self.evaluate_bound_expr(right, batch)?;
				Ok(Arc::new(lt_eq(&left_arr, &right_arr)?))
			}
			ExecutionExpr::Gt(left, right) => {
				let left_arr = self.evaluate_bound_expr(left, batch)?;
				let right_arr = self.evaluate_bound_expr(right, batch)?;
				Ok(Arc::new(gt(&left_arr, &right_arr)?))
			}
			ExecutionExpr::Gte(left, right) => {
				let left_arr = self.evaluate_bound_expr(left, batch)?;
				let right_arr = self.evaluate_bound_expr(right, batch)?;
				Ok(Arc::new(gt_eq(&left_arr, &right_arr)?))
			}
			ExecutionExpr::And(left, right) => {
				let left_arr = self.evaluate_bound_expr(left, batch)?;
				let right_arr = self.evaluate_bound_expr(right, batch)?;
				let left_bool = as_boolean(&left_arr, "AND left operand")?;
				let right_bool = as_boolean(&right_arr, "AND right operand")?;
				Ok(Arc::new(and(left_bool, right_bool)?))
			}
			ExecutionExpr::Or(left, right) => {
				let left_arr = self.evaluate_bound_expr(left, batch)?;
				let right_arr = self.evaluate_bound_expr(right, batch)?;
				let left_bool = as_boolean(&left_arr, "OR left operand")?;
				let right_bool = as_boolean(&right_arr, "OR right operand")?;
				Ok(Arc::new(or(left_bool, right_bool)?))
			}
			ExecutionExpr::Not(inner) => {
				let arr = self.evaluate_bound_expr(inner, batch)?;
				let bool_arr = as_boolean(&arr, "NOT operand")?;
				Ok(Arc::new(not(bool_arr)?))
			}
			ExecutionExpr::IsNull(inner) => {
				let arr = self.evaluate_bound_expr(inner, batch)?;
				Ok(Arc::new(is_null(&arr)?))
			}
			ExecutionExpr::IsNotNull(inner) => {
				let arr = self.evaluate_bound_expr(inner, batch)?;
				Ok(Arc::new(is_not_null(&arr)?))
			}
			ExecutionExpr::TimeBucket(interval, inner) => {
				let arr = self.evaluate_bound_expr(inner, batch)?;
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
		}
	}

	fn bound_global_aggregate(
		&self,
		input_batches: &[RecordBatch],
		aggr_expr: &[crate::ir::AggregateCall],
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
						let arr = self.evaluate_bound_expr(&call.args[0], batch)?;
						for row in 0..batch.num_rows() {
							if !arr.is_null(row) {
								total += 1;
							}
						}
					}
					results_cols.push(Arc::new(Int64Array::from(vec![total])) as ArrayRef);
				}
				AggregateFunction::Sum => {
					let mut state = sum_state_for_data_type(&call.args[0].data_type());
					for batch in input_batches {
						let arr = self.evaluate_bound_expr(&call.args[0], batch)?;
						for row in 0..batch.num_rows() {
							if let Some(value) = sum_value(&arr, row)? {
								add_sum_value(&mut state, value)?;
							}
						}
					}
					let output_type = output_schema.field(results_cols.len()).data_type().clone();
					results_cols.push(sum_state_array(&state, &output_type)?);
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
					AggregateFunction::Count | AggregateFunction::Sum => {
						Ok(Some(self.evaluate_expr(&args[0], batch)?))
					}
					AggregateFunction::LinearRegress | AggregateFunction::ApproxQuantile(_) => {
						Err(ExecutionError::NotImplemented {
							feature: "grouped experimental aggregate",
						})
					}
				})
				.collect::<Result<Vec<_>, _>>()?;

			for row in 0..batch.num_rows() {
				let key = group_arrays
					.iter()
					.map(|array| group_value(array, row))
					.collect::<Result<Vec<_>, _>>()?;
				let states = groups.entry(key).or_insert_with(|| init_states(aggr_expr));
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
								if let Some(value) = sum_value(array, row)? {
									add_sum_value(state, value)?;
								}
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
							AggState::SumInt { .. } | AggState::SumFloat { .. } => 0,
						})
						.collect::<Vec<_>>();
					columns.push(Arc::new(Int64Array::from(values)) as ArrayRef);
				}
				AggregateFunction::Sum => {
					columns.push(sum_column_from_groups(&groups, agg_idx, &DataType::Int64)?);
				}
				AggregateFunction::LinearRegress | AggregateFunction::ApproxQuantile(_) => {
					return Err(ExecutionError::NotImplemented {
						feature: "grouped experimental aggregate",
					});
				}
			}
			let field_name = match function {
				AggregateFunction::CountAll => "count",
				AggregateFunction::Count => "count",
				AggregateFunction::Sum => "sum",
				AggregateFunction::LinearRegress => "linreg",
				AggregateFunction::ApproxQuantile(_) => "quantile",
			};
			fields.push(arrow::datatypes::Field::new(
				field_name,
				arrow::datatypes::DataType::Int64,
				false,
			));
		}

		let schema = Arc::new(arrow::datatypes::Schema::new(fields));
		Ok(vec![RecordBatch::try_new(schema, columns)?])
	}

	fn bound_grouped_aggregate(
		&self,
		input_batches: &[RecordBatch],
		group_expr: &[ExecutionExpr],
		aggr_expr: &[crate::ir::AggregateCall],
		output_schema: &arrow::datatypes::SchemaRef,
	) -> Result<Vec<RecordBatch>, ExecutionError> {
		let mut groups: BTreeMap<Vec<GroupValue>, Vec<AggState>> = BTreeMap::new();
		let mut group_types = Vec::new();

		for batch in input_batches {
			let group_arrays = group_expr
				.iter()
				.map(|expr| self.evaluate_bound_expr(expr, batch))
				.collect::<Result<Vec<_>, _>>()?;
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
					AggregateFunction::Count | AggregateFunction::Sum => {
						Ok(Some(self.evaluate_bound_expr(&call.args[0], batch)?))
					}
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
				let states = groups
					.entry(key)
					.or_insert_with(|| init_bound_states(aggr_expr));
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
								if let Some(value) = sum_value(array, row)? {
									add_sum_value(state, value)?;
								}
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
							AggState::SumInt { .. } | AggState::SumFloat { .. } => 0,
						})
						.collect::<Vec<_>>();
					columns.push(Arc::new(Int64Array::from(values)) as ArrayRef);
				}
				AggregateFunction::Sum => {
					let output_type = output_schema.field(columns.len()).data_type().clone();
					columns.push(sum_column_from_groups(&groups, agg_idx, &output_type)?);
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
		key: &[ExecutionExpr],
		morsel: Option<(MorselId, u32)>,
	) -> Result<Vec<RecordBatch>, ExecutionError> {
		let input_batches = self.execute_bound(input, morsel).await?;
		let mut partitioned_output = Vec::new();

		for batch in input_batches {
			let num_rows = batch.num_rows();
			if num_rows == 0 {
				continue;
			}

			let key_arrays = key
				.iter()
				.map(|expr| self.evaluate_bound_expr(expr, &batch))
				.collect::<Result<Vec<_>, _>>()?;

			let mut builders: Vec<Vec<u32>> = (0..partition_count).map(|_| Vec::new()).collect();
			for row in 0..num_rows {
				let bucket = partition_bucket(&key_arrays, row, partition_count)?;
				builders[bucket].push(row as u32);
			}

			for indices in builders {
				if !indices.is_empty() {
					let indices_arr = arrow::array::UInt32Array::from(indices);
					let mut cols = Vec::new();
					for col in batch.columns() {
						cols.push(arrow::compute::take(col, &indices_arr, None)?);
					}
					partitioned_output.push(RecordBatch::try_new(batch.schema(), cols)?);
				}
			}
		}

		Ok(partitioned_output)
	}
}

fn partition_bucket(
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
	Utf8(String),
}

#[derive(Debug, Clone, Copy)]
enum AggState {
	Count(i64),
	SumInt { total: i128, seen: bool },
	SumFloat { total: f64, seen: bool },
}

#[derive(Debug, Clone, Copy)]
enum SumValue {
	Int(i128),
	Float(f64),
}

#[cfg(test)]
fn init_states(aggr_expr: &[(AggregateFunction, Vec<Expr>)]) -> Vec<AggState> {
	aggr_expr
		.iter()
		.map(|(function, _)| match function {
			AggregateFunction::CountAll => AggState::Count(0),
			AggregateFunction::Count => AggState::Count(0),
			AggregateFunction::Sum => AggState::SumInt {
				total: 0,
				seen: false,
			},
			AggregateFunction::LinearRegress | AggregateFunction::ApproxQuantile(_) => {
				AggState::SumInt {
					total: 0,
					seen: false,
				}
			}
		})
		.collect()
}

fn init_bound_states(aggr_expr: &[crate::ir::AggregateCall]) -> Vec<AggState> {
	aggr_expr
		.iter()
		.map(|call| match call.function {
			AggregateFunction::CountAll => AggState::Count(0),
			AggregateFunction::Count => AggState::Count(0),
			AggregateFunction::Sum => call
				.args
				.first()
				.map(|arg| sum_state_for_data_type(&arg.data_type()))
				.unwrap_or(AggState::SumInt {
					total: 0,
					seen: false,
				}),
			AggregateFunction::LinearRegress | AggregateFunction::ApproxQuantile(_) => {
				AggState::SumInt {
					total: 0,
					seen: false,
				}
			}
		})
		.collect()
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
	if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
		return Ok(GroupValue::Utf8(array.value(row).to_string()));
	}
	Err(ExecutionError::InvalidExpression {
		message: format!("unsupported GROUP BY type: {:?}", array.data_type()),
	})
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

fn add_sum_value(state: &mut AggState, value: SumValue) -> Result<(), ExecutionError> {
	match (state, value) {
		(AggState::SumInt { total, seen }, SumValue::Int(value)) => {
			*total = total
				.checked_add(value)
				.ok_or_else(|| ExecutionError::Kernel {
					message: "integer SUM overflow".to_string(),
				})?;
			*seen = true;
			Ok(())
		}
		(AggState::SumFloat { total, seen }, SumValue::Float(value)) => {
			*total += value;
			*seen = true;
			Ok(())
		}
		(AggState::SumFloat { total, seen }, SumValue::Int(value)) => {
			*total += value as f64;
			*seen = true;
			Ok(())
		}
		(AggState::SumInt { .. }, SumValue::Float(_)) => Err(ExecutionError::InvalidExpression {
			message: "floating SUM value reached integer accumulator".to_string(),
		}),
		(AggState::Count(_), _) => Err(ExecutionError::InvalidExpression {
			message: "SUM value reached count accumulator".to_string(),
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
					AggState::SumFloat { .. } | AggState::Count(_) => Ok(None),
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
					AggState::Count(_) => None,
				})
				.collect::<Vec<_>>();
			Ok(Arc::new(Float64Array::from(values)) as ArrayRef)
		}
		_ => Err(ExecutionError::InvalidExpression {
			message: format!("unsupported SUM output type: {data_type:?}"),
		}),
	}
}

fn sum_value(array: &ArrayRef, row: usize) -> Result<Option<SumValue>, ExecutionError> {
	if array.is_null(row) {
		return Ok(None);
	}
	if let Some(array) = array.as_any().downcast_ref::<Int8Array>() {
		return Ok(Some(SumValue::Int(array.value(row) as i128)));
	}
	if let Some(array) = array.as_any().downcast_ref::<Int16Array>() {
		return Ok(Some(SumValue::Int(array.value(row) as i128)));
	}
	if let Some(array) = array.as_any().downcast_ref::<Int32Array>() {
		return Ok(Some(SumValue::Int(array.value(row) as i128)));
	}
	if let Some(array) = array.as_any().downcast_ref::<Int64Array>() {
		return Ok(Some(SumValue::Int(array.value(row) as i128)));
	}
	if let Some(array) = array.as_any().downcast_ref::<UInt8Array>() {
		return Ok(Some(SumValue::Int(array.value(row) as i128)));
	}
	if let Some(array) = array.as_any().downcast_ref::<UInt16Array>() {
		return Ok(Some(SumValue::Int(array.value(row) as i128)));
	}
	if let Some(array) = array.as_any().downcast_ref::<UInt32Array>() {
		return Ok(Some(SumValue::Int(array.value(row) as i128)));
	}
	if let Some(array) = array.as_any().downcast_ref::<UInt64Array>() {
		return Ok(Some(SumValue::Int(array.value(row) as i128)));
	}
	if let Some(array) = array.as_any().downcast_ref::<Float32Array>() {
		return Ok(Some(SumValue::Float(array.value(row) as f64)));
	}
	if let Some(array) = array.as_any().downcast_ref::<Float64Array>() {
		return Ok(Some(SumValue::Float(array.value(row))));
	}
	Err(ExecutionError::InvalidExpression {
		message: format!("SUM expects numeric input, got {:?}", array.data_type()),
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
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
		LocalityPolicy::default_table_spread(),
		SchemaEvolutionPolicyV1::AdditiveOnly,
	)
	.expect("local scan descriptor must be valid");
	ScanTablePin {
		table_object_id: descriptor.table_object_id,
		physical_descriptor_fingerprint: descriptor.physical_descriptor_fingerprint,
	}
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
	use crate::catalog::MockCatalog;
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
	use std::sync::Mutex;
	use wendelon::schema::{
		LocalityPolicy, PrimaryKeyDescriptorV1, RowIdentityPolicy, SchemaEvolutionPolicyV1,
		TableObjectId, TableSchemaDescriptor,
	};

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
				Box::new(Expr::Column("amount".to_string())),
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
				Box::new(Expr::Column("id".to_string())),
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
					Box::new(Expr::Column("amount".to_string())),
					Box::new(Expr::Literal(ScalarValue::Int32(Some(100)))),
				),
			}),
			exprs: vec![Expr::Column("id".to_string())],
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
		assert_eq!(
			observed[0].projected_columns,
			Some(vec![ColumnOrdinal(0), ColumnOrdinal(1)])
		);
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
			group_expr: vec![Expr::Column("region".to_string())],
			aggr_expr: vec![(
				AggregateFunction::Sum,
				vec![Expr::Column("amount".to_string())],
			)],
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
				(
					AggregateFunction::Sum,
					vec![Expr::Column("amount".to_string())],
				),
				(
					AggregateFunction::Count,
					vec![Expr::Column("amount".to_string())],
				),
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
			.map(|column| {
				(
					AggregateFunction::Sum,
					vec![Expr::Column(column.to_string())],
				)
			})
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
			group_expr: vec![Expr::Column("region".to_string())],
			aggr_expr: vec![(
				AggregateFunction::Sum,
				vec![Expr::Column("amount".to_string())],
			)],
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
			key: vec![Expr::Column("amount".to_string())],
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
			group_expr: vec![Expr::Column("region".to_string())],
			aggr_expr: vec![(
				AggregateFunction::Sum,
				vec![Expr::Column("amount".to_string())],
			)],
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
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
			LocalityPolicy::default_table_spread(),
			SchemaEvolutionPolicyV1::AdditiveOnly,
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
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
			LocalityPolicy::default_table_spread(),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.expect("descriptor")
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
