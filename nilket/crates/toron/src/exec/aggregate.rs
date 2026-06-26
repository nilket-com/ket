use super::*;

impl Executor {
	pub(in crate::exec) fn bound_global_aggregate(
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

	pub(in crate::exec) fn fold_global_aggregate_state(
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

pub(in crate::exec) fn init_global_aggregate_state(
	aggr_expr: &[BoundAggregateCall],
) -> Result<Vec<AggState>, ExecutionError> {
	init_bound_states(aggr_expr)
}

pub(in crate::exec) fn finalize_global_aggregate_state(
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

impl Executor {
	#[cfg(test)]
	pub(in crate::exec) fn cast_to_f64(
		&self,
		arr: &ArrayRef,
	) -> Result<Float64Array, ExecutionError> {
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
	pub(in crate::exec) fn grouped_aggregate(
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

	pub(in crate::exec) fn bound_grouped_aggregate(
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

	pub(in crate::exec) fn fold_bound_grouped_aggregate_batch(
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

	pub(in crate::exec) fn finalize_bound_grouped_aggregate(
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
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(in crate::exec) enum GroupValue {
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
pub(in crate::exec) enum AggState {
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
pub(in crate::exec) enum NumericValue {
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

pub(in crate::exec) fn init_bound_states(
	aggr_expr: &[BoundAggregateCall],
) -> Result<Vec<AggState>, ExecutionError> {
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

pub(in crate::exec) fn group_value(
	array: &ArrayRef,
	row: usize,
) -> Result<GroupValue, ExecutionError> {
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

pub(in crate::exec) fn sum_state_for_data_type(data_type: &DataType) -> AggState {
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

pub(in crate::exec) fn min_state_for_data_type(
	data_type: &DataType,
) -> Result<AggState, ExecutionError> {
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

pub(in crate::exec) fn max_state_for_data_type(
	data_type: &DataType,
) -> Result<AggState, ExecutionError> {
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

pub(in crate::exec) fn avg_state_for_data_type(
	data_type: &DataType,
) -> Result<AggState, ExecutionError> {
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

pub(in crate::exec) fn add_sum_value(
	state: &mut AggState,
	value: NumericValue,
) -> Result<(), ExecutionError> {
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

pub(in crate::exec) fn add_avg_value(
	state: &mut AggState,
	value: NumericValue,
) -> Result<(), ExecutionError> {
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

pub(in crate::exec) fn update_min_state(
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

pub(in crate::exec) fn update_max_state(
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

pub(in crate::exec) fn min_max_state_array(
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

pub(in crate::exec) fn sum_state_array(
	state: &AggState,
	data_type: &DataType,
) -> Result<ArrayRef, ExecutionError> {
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
		// scale) (scalar global aggregate, no GROUP BY).
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
pub(in crate::exec) fn avg_decimal_finalize(
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

pub(in crate::exec) fn avg_state_array(
	state: &AggState,
	output_type: &DataType,
) -> Result<ArrayRef, ExecutionError> {
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
pub(in crate::exec) fn aggregate_state_keyword(function: &AggregateFunction) -> &'static str {
	aggregate_kernel::stable_spec_for_function(function)
		.map(|spec| spec.sql_name)
		.unwrap_or("aggregate")
}

#[cfg(test)]
pub(in crate::exec) fn aggregate_state_output_name(function: &AggregateFunction) -> &'static str {
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

pub(in crate::exec) fn numeric_value(
	array: &ArrayRef,
	row: usize,
) -> Result<Option<NumericValue>, ExecutionError> {
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
	// SUM finalize re-applies the column's (precision, scale). (SUM over Decimal.)
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

pub(in crate::exec) fn group_column(
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

#[cfg(test)]
mod tests {
	use super::*;

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
	// type and the exact raw i128 values (incl. NULL). A digest-only integration check can't pin the core
	// (p,s) round-trip.
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
}
