use std::sync::Arc;

use arrow::array::{
	Array, ArrayRef, BooleanArray, BooleanBuilder, Date32Builder, Float32Array, Float32Builder,
	Float64Array, Float64Builder, Int32Array, Int32Builder, Int64Array, Int64Builder, StringArray,
	StringBuilder, TimestampMicrosecondBuilder, TimestampMillisecondBuilder,
	TimestampNanosecondBuilder,
};
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;

use crate::binder::TABLE_WRITE_MAX_ROWS;
use crate::error::{ExecutionError, StorageError};
use crate::exec::evaluate_cast;
use crate::ir::{
	ColumnOrdinal, LogicalTypeRegistry, LogicalTypeResolver, ScalarValue,
	decode_column_logical_type_with_resolver,
};
use wendelon_client::{
	LogicalTypeRefinementComparisonOp, LogicalTypeRefinementPredicate, LogicalTypeRefinementValue,
};

pub(super) fn affected_count_batch(count: usize) -> Result<RecordBatch, ExecutionError> {
	let value = i64::try_from(count).map_err(|_| ExecutionError::Kernel {
		message: format!("affected row count {count} exceeds Int64 range"),
	})?;
	let schema = Arc::new(arrow::datatypes::Schema::new(vec![
		arrow::datatypes::Field::new("affected", DataType::Int64, false),
	]));
	let array = Arc::new(Int64Array::from(vec![value])) as ArrayRef;
	RecordBatch::try_new(schema, vec![array]).map_err(ExecutionError::from)
}

pub(super) fn materialize_insert_batch(
	schema: &SchemaRef,
	rows: &[Vec<ScalarValue>],
) -> Result<RecordBatch, StorageError> {
	let mut columns = Vec::with_capacity(schema.fields().len());
	for (field_index, field) in schema.fields().iter().enumerate() {
		columns.push(build_array(field.data_type(), rows, field_index)?);
	}
	RecordBatch::try_new(schema.clone(), columns).map_err(StorageError::Arrow)
}

pub(super) fn materialize_insert_select_batch(
	target_schema: &SchemaRef,
	target_columns: &[ColumnOrdinal],
	source_batches: &[RecordBatch],
) -> Result<Option<RecordBatch>, ExecutionError> {
	materialize_write_select_batch(
		"INSERT INTO ... SELECT",
		target_schema,
		target_columns,
		source_batches,
	)
}

pub(super) fn materialize_update_select_batch(
	target_schema: &SchemaRef,
	source_batches: &[RecordBatch],
) -> Result<Option<RecordBatch>, ExecutionError> {
	let target_columns = (0..target_schema.fields().len())
		.map(|index| ColumnOrdinal(u32::try_from(index).expect("column ordinal overflow")))
		.collect::<Vec<_>>();
	materialize_write_select_batch("UPDATE", target_schema, &target_columns, source_batches)
}

fn materialize_write_select_batch(
	operation: &'static str,
	target_schema: &SchemaRef,
	target_columns: &[ColumnOrdinal],
	source_batches: &[RecordBatch],
) -> Result<Option<RecordBatch>, ExecutionError> {
	let total_rows = source_batches
		.iter()
		.map(RecordBatch::num_rows)
		.sum::<usize>();
	if total_rows > TABLE_WRITE_MAX_ROWS {
		return Err(ExecutionError::InvalidExpression {
			message: format!(
				"{operation} produced {total_rows} rows; exceeds maximum {TABLE_WRITE_MAX_ROWS}"
			),
		});
	}
	if total_rows == 0 {
		return Ok(None);
	}

	let mut source_by_target = vec![None; target_schema.fields().len()];
	for (source_index, target_column) in target_columns.iter().enumerate() {
		let target_index = target_column.0 as usize;
		let Some(slot) = source_by_target.get_mut(target_index) else {
			return Err(ExecutionError::InvalidExpression {
				message: format!(
					"INSERT target column ordinal {} out of bounds",
					target_column.0
				),
			});
		};
		if slot.is_some() {
			return Err(ExecutionError::InvalidExpression {
				message: format!("duplicate INSERT target column ordinal {}", target_column.0),
			});
		}
		*slot = Some(source_index);
	}

	let mut coerced_batches = Vec::with_capacity(source_batches.len());
	for batch in source_batches {
		if batch.num_columns() != target_columns.len() {
			return Err(ExecutionError::InvalidExpression {
				message: format!(
					"{operation} batch has {} columns; expected {}",
					batch.num_columns(),
					target_columns.len()
				),
			});
		}

		let mut columns = Vec::with_capacity(target_schema.fields().len());
		for (target_index, field) in target_schema.fields().iter().enumerate() {
			let array = match source_by_target[target_index] {
				Some(source_index) => coerce_insert_select_array(
					operation,
					batch.column(source_index),
					field.data_type(),
					field.name(),
				)?,
				None => {
					if !field.is_nullable() {
						return Err(ExecutionError::InvalidExpression {
							message: format!(
								"{operation} omits non-nullable column: {}",
								field.name()
							),
						});
					}
					typed_null_array(field.data_type(), batch.num_rows(), field.name())?
				}
			};
			columns.push(array);
		}
		validate_non_null_arrays(operation, target_schema, &columns)?;
		coerced_batches.push(RecordBatch::try_new(target_schema.clone(), columns)?);
	}

	let payload = if coerced_batches.len() == 1 {
		coerced_batches.pop().expect("one coerced batch")
	} else {
		arrow::compute::concat_batches(target_schema, coerced_batches.iter())?
	};
	validate_non_null_targets(operation, &payload)?;
	Ok(Some(payload))
}

pub(super) fn materialize_empty_record_batch(
	schema: &SchemaRef,
) -> Result<RecordBatch, ExecutionError> {
	let mut columns = Vec::with_capacity(schema.fields().len());
	for field in schema.fields() {
		columns.push(typed_null_array(field.data_type(), 0, field.name())?);
	}
	RecordBatch::try_new(schema.clone(), columns).map_err(ExecutionError::from)
}

fn coerce_insert_select_array(
	operation: &'static str,
	array: &ArrayRef,
	target: &DataType,
	column: &str,
) -> Result<ArrayRef, ExecutionError> {
	if !is_supported_insert_select_target(target) {
		return Err(ExecutionError::InvalidExpression {
			message: format!("unsupported {operation} target type for column {column}: {target:?}"),
		});
	}
	if array.data_type() == target {
		return Ok(array.clone());
	}
	if is_numeric_type(array.data_type()) && is_numeric_type(target) {
		return evaluate_cast(array, target);
	}
	Err(ExecutionError::InvalidExpression {
		message: format!(
			"{operation} source type {:?} is not compatible with column {column} type {target:?}",
			array.data_type()
		),
	})
}

fn typed_null_array(
	data_type: &DataType,
	rows: usize,
	column: &str,
) -> Result<ArrayRef, ExecutionError> {
	match data_type {
		DataType::Boolean => Ok(Arc::new(BooleanArray::from(vec![None::<bool>; rows]))),
		DataType::Int32 => Ok(Arc::new(Int32Array::from(vec![None::<i32>; rows]))),
		DataType::Int64 => Ok(Arc::new(Int64Array::from(vec![None::<i64>; rows]))),
		DataType::Float32 => Ok(Arc::new(Float32Array::from(vec![None::<f32>; rows]))),
		DataType::Float64 => Ok(Arc::new(Float64Array::from(vec![None::<f64>; rows]))),
		DataType::Utf8 => Ok(Arc::new(StringArray::from(vec![None::<&str>; rows]))),
		target => Err(ExecutionError::InvalidExpression {
			message: format!("unsupported INSERT target type for column {column}: {target:?}"),
		}),
	}
}

fn validate_non_null_targets(
	operation: &'static str,
	batch: &RecordBatch,
) -> Result<(), ExecutionError> {
	validate_non_null_arrays(operation, &batch.schema(), batch.columns())
}

fn validate_non_null_arrays(
	operation: &'static str,
	schema: &SchemaRef,
	columns: &[ArrayRef],
) -> Result<(), ExecutionError> {
	for (field, array) in schema.fields().iter().zip(columns) {
		if !field.is_nullable() && array.null_count() > 0 {
			return Err(ExecutionError::InvalidExpression {
				message: format!(
					"{operation} produced NULL for non-nullable column: {}",
					field.name()
				),
			});
		}
	}
	Ok(())
}

pub(super) fn validate_refined_logical_columns(
	operation: &'static str,
	registry: &LogicalTypeRegistry,
	batch: &RecordBatch,
) -> Result<(), ExecutionError> {
	let resolver = LogicalTypeResolver::with_registry(registry);
	for (column_index, field) in batch.schema().fields().iter().enumerate() {
		let logical_type =
			decode_column_logical_type_with_resolver(field, resolver).map_err(|error| {
				ExecutionError::InvalidExpression {
					message: error.to_string(),
				}
			})?;
		if logical_type.is_naked() {
			continue;
		}
		let Some(spec) = resolver.spec(logical_type) else {
			return Err(ExecutionError::InvalidExpression {
				message: format!(
					"{operation} target column {} references unknown logical type id {}",
					field.name(),
					logical_type.0
				),
			});
		};
		let Some(refinement) = spec.refinement.as_ref() else {
			continue;
		};
		let array = batch.column(column_index);
		for row in 0..batch.num_rows() {
			if array.is_null(row) {
				continue;
			}
			let value = logical_type_refinement_row_value(field.data_type(), array, row)?;
			if !evaluate_logical_type_refinement_predicate(refinement, &value)? {
				return Err(ExecutionError::InvalidExpression {
					message: format!(
						"{operation} violates logical type refinement for column {} ({}) at row {}",
						field.name(),
						spec.name,
						row
					),
				});
			}
		}
	}
	Ok(())
}

fn logical_type_refinement_row_value(
	data_type: &DataType,
	array: &ArrayRef,
	row: usize,
) -> Result<LogicalTypeRefinementValue, ExecutionError> {
	match data_type {
		DataType::Boolean => array
			.as_any()
			.downcast_ref::<BooleanArray>()
			.map(|array| LogicalTypeRefinementValue::Boolean(array.value(row)))
			.ok_or_else(|| refinement_array_type_mismatch(data_type)),
		DataType::Int32 => array
			.as_any()
			.downcast_ref::<Int32Array>()
			.map(|array| LogicalTypeRefinementValue::Int32(array.value(row)))
			.ok_or_else(|| refinement_array_type_mismatch(data_type)),
		DataType::Int64 => array
			.as_any()
			.downcast_ref::<Int64Array>()
			.map(|array| LogicalTypeRefinementValue::Int64(array.value(row)))
			.ok_or_else(|| refinement_array_type_mismatch(data_type)),
		DataType::Float32 => array
			.as_any()
			.downcast_ref::<Float32Array>()
			.map(|array| LogicalTypeRefinementValue::Float32(array.value(row).to_bits()))
			.ok_or_else(|| refinement_array_type_mismatch(data_type)),
		DataType::Float64 => array
			.as_any()
			.downcast_ref::<Float64Array>()
			.map(|array| LogicalTypeRefinementValue::Float64(array.value(row).to_bits()))
			.ok_or_else(|| refinement_array_type_mismatch(data_type)),
		DataType::Utf8 => array
			.as_any()
			.downcast_ref::<StringArray>()
			.map(|array| LogicalTypeRefinementValue::Utf8(array.value(row).to_string()))
			.ok_or_else(|| refinement_array_type_mismatch(data_type)),
		other => Err(ExecutionError::InvalidExpression {
			message: format!("unsupported logical type refinement carrier: {other:?}"),
		}),
	}
}

fn refinement_array_type_mismatch(data_type: &DataType) -> ExecutionError {
	ExecutionError::InvalidExpression {
		message: format!(
			"logical type refinement array storage did not match carrier {data_type:?}"
		),
	}
}

fn evaluate_logical_type_refinement_predicate(
	predicate: &LogicalTypeRefinementPredicate,
	value: &LogicalTypeRefinementValue,
) -> Result<bool, ExecutionError> {
	match predicate {
		LogicalTypeRefinementPredicate::Comparison { left, op, right } => {
			let left = resolve_logical_type_refinement_operand(left, value);
			let right = resolve_logical_type_refinement_operand(right, value);
			compare_logical_type_refinement_values(left, *op, right)
		}
		LogicalTypeRefinementPredicate::And(predicates) => {
			for predicate in predicates {
				if !evaluate_logical_type_refinement_predicate(predicate, value)? {
					return Ok(false);
				}
			}
			Ok(true)
		}
	}
}

fn resolve_logical_type_refinement_operand<'a>(
	operand: &'a LogicalTypeRefinementValue,
	value: &'a LogicalTypeRefinementValue,
) -> &'a LogicalTypeRefinementValue {
	match operand {
		LogicalTypeRefinementValue::Value => value,
		literal => literal,
	}
}

fn compare_logical_type_refinement_values(
	left: &LogicalTypeRefinementValue,
	op: LogicalTypeRefinementComparisonOp,
	right: &LogicalTypeRefinementValue,
) -> Result<bool, ExecutionError> {
	match (left, right) {
		(LogicalTypeRefinementValue::Int64(left), LogicalTypeRefinementValue::Int64(right)) => {
			Ok(compare_i64(*left, op, *right))
		}
		(LogicalTypeRefinementValue::Int32(left), LogicalTypeRefinementValue::Int32(right)) => {
			Ok(compare_i32(*left, op, *right))
		}
		(LogicalTypeRefinementValue::Float64(left), LogicalTypeRefinementValue::Float64(right)) => {
			Ok(compare_f64(
				f64::from_bits(*left),
				op,
				f64::from_bits(*right),
			))
		}
		(LogicalTypeRefinementValue::Float32(left), LogicalTypeRefinementValue::Float32(right)) => {
			Ok(compare_f32(
				f32::from_bits(*left),
				op,
				f32::from_bits(*right),
			))
		}
		(LogicalTypeRefinementValue::Utf8(left), LogicalTypeRefinementValue::Utf8(right)) => {
			compare_eq_only(left, op, right)
		}
		(LogicalTypeRefinementValue::Boolean(left), LogicalTypeRefinementValue::Boolean(right)) => {
			compare_eq_only(left, op, right)
		}
		_ => Err(ExecutionError::InvalidExpression {
			message: "logical type refinement compared incompatible value types".to_string(),
		}),
	}
}

fn compare_i64(left: i64, op: LogicalTypeRefinementComparisonOp, right: i64) -> bool {
	match op {
		LogicalTypeRefinementComparisonOp::Eq => left == right,
		LogicalTypeRefinementComparisonOp::Ne => left != right,
		LogicalTypeRefinementComparisonOp::Lt => left < right,
		LogicalTypeRefinementComparisonOp::Le => left <= right,
		LogicalTypeRefinementComparisonOp::Gt => left > right,
		LogicalTypeRefinementComparisonOp::Ge => left >= right,
	}
}

fn compare_i32(left: i32, op: LogicalTypeRefinementComparisonOp, right: i32) -> bool {
	match op {
		LogicalTypeRefinementComparisonOp::Eq => left == right,
		LogicalTypeRefinementComparisonOp::Ne => left != right,
		LogicalTypeRefinementComparisonOp::Lt => left < right,
		LogicalTypeRefinementComparisonOp::Le => left <= right,
		LogicalTypeRefinementComparisonOp::Gt => left > right,
		LogicalTypeRefinementComparisonOp::Ge => left >= right,
	}
}

fn compare_f64(left: f64, op: LogicalTypeRefinementComparisonOp, right: f64) -> bool {
	match op {
		LogicalTypeRefinementComparisonOp::Eq => left == right,
		LogicalTypeRefinementComparisonOp::Ne => left != right,
		LogicalTypeRefinementComparisonOp::Lt => left < right,
		LogicalTypeRefinementComparisonOp::Le => left <= right,
		LogicalTypeRefinementComparisonOp::Gt => left > right,
		LogicalTypeRefinementComparisonOp::Ge => left >= right,
	}
}

fn compare_f32(left: f32, op: LogicalTypeRefinementComparisonOp, right: f32) -> bool {
	match op {
		LogicalTypeRefinementComparisonOp::Eq => left == right,
		LogicalTypeRefinementComparisonOp::Ne => left != right,
		LogicalTypeRefinementComparisonOp::Lt => left < right,
		LogicalTypeRefinementComparisonOp::Le => left <= right,
		LogicalTypeRefinementComparisonOp::Gt => left > right,
		LogicalTypeRefinementComparisonOp::Ge => left >= right,
	}
}

fn compare_eq_only<T: PartialEq>(
	left: &T,
	op: LogicalTypeRefinementComparisonOp,
	right: &T,
) -> Result<bool, ExecutionError> {
	match op {
		LogicalTypeRefinementComparisonOp::Eq => Ok(left == right),
		LogicalTypeRefinementComparisonOp::Ne => Ok(left != right),
		_ => Err(ExecutionError::InvalidExpression {
			message: "logical type refinement ordered comparison requires a numeric carrier"
				.to_string(),
		}),
	}
}

fn is_supported_insert_select_target(data_type: &DataType) -> bool {
	matches!(
		data_type,
		DataType::Boolean
			| DataType::Int32
			| DataType::Int64
			| DataType::Date32
			| DataType::Float32
			| DataType::Float64
			| DataType::Utf8
	) || crate::timestamp::is_supported_utc_timestamp(data_type)
}

fn is_numeric_type(data_type: &DataType) -> bool {
	matches!(
		data_type,
		DataType::Int32 | DataType::Int64 | DataType::Float32 | DataType::Float64
	)
}

fn build_array(
	data_type: &DataType,
	rows: &[Vec<ScalarValue>],
	field_index: usize,
) -> Result<ArrayRef, StorageError> {
	match data_type {
		DataType::Boolean => {
			let mut builder = BooleanBuilder::with_capacity(rows.len());
			for row in rows {
				match scalar_at(row, field_index)? {
					ScalarValue::Boolean(Some(value)) => builder.append_value(*value),
					ScalarValue::Boolean(None) => builder.append_null(),
					other => return Err(type_mismatch(field_index, "Boolean", other)),
				}
			}
			Ok(Arc::new(builder.finish()))
		}
		DataType::Int32 => {
			let mut builder = Int32Builder::with_capacity(rows.len());
			for row in rows {
				match scalar_at(row, field_index)? {
					ScalarValue::Int32(Some(value)) => builder.append_value(*value),
					ScalarValue::Int32(None) => builder.append_null(),
					other => return Err(type_mismatch(field_index, "Int32", other)),
				}
			}
			Ok(Arc::new(builder.finish()))
		}
		DataType::Int64 => {
			let mut builder = Int64Builder::with_capacity(rows.len());
			for row in rows {
				match scalar_at(row, field_index)? {
					ScalarValue::Int64(Some(value)) => builder.append_value(*value),
					ScalarValue::Int64(None) => builder.append_null(),
					other => return Err(type_mismatch(field_index, "Int64", other)),
				}
			}
			Ok(Arc::new(builder.finish()))
		}
		DataType::Date32 => {
			let mut builder = Date32Builder::with_capacity(rows.len());
			for row in rows {
				match scalar_at(row, field_index)? {
					ScalarValue::Date32(Some(value)) => builder.append_value(*value),
					ScalarValue::Date32(None) => builder.append_null(),
					other => return Err(type_mismatch(field_index, "Date32", other)),
				}
			}
			Ok(Arc::new(builder.finish()))
		}
		DataType::Timestamp(unit, Some(timezone)) if timezone.as_ref() == "UTC" => {
			build_timestamp_array(*unit, rows, field_index)
		}
		DataType::Float32 => {
			let mut builder = Float32Builder::with_capacity(rows.len());
			for row in rows {
				match scalar_at(row, field_index)? {
					ScalarValue::Float32(Some(value)) => builder.append_value(*value),
					ScalarValue::Float32(None) => builder.append_null(),
					other => return Err(type_mismatch(field_index, "Float32", other)),
				}
			}
			Ok(Arc::new(builder.finish()))
		}
		DataType::Float64 => {
			let mut builder = Float64Builder::with_capacity(rows.len());
			for row in rows {
				match scalar_at(row, field_index)? {
					ScalarValue::Float64(Some(value)) => builder.append_value(*value),
					ScalarValue::Float64(None) => builder.append_null(),
					other => return Err(type_mismatch(field_index, "Float64", other)),
				}
			}
			Ok(Arc::new(builder.finish()))
		}
		DataType::Utf8 => {
			let mut builder = StringBuilder::new();
			for row in rows {
				match scalar_at(row, field_index)? {
					ScalarValue::Utf8(Some(value)) => builder.append_value(value),
					ScalarValue::Utf8(None) => builder.append_null(),
					other => return Err(type_mismatch(field_index, "Utf8", other)),
				}
			}
			Ok(Arc::new(builder.finish()))
		}
		other => Err(StorageError::Internal {
			message: format!("unsupported INSERT materialization type: {other:?}"),
		}),
	}
}

fn build_timestamp_array(
	unit: TimeUnit,
	rows: &[Vec<ScalarValue>],
	field_index: usize,
) -> Result<ArrayRef, StorageError> {
	match unit {
		TimeUnit::Millisecond => {
			let mut builder =
				TimestampMillisecondBuilder::with_capacity(rows.len()).with_timezone("UTC");
			for row in rows {
				match scalar_at(row, field_index)? {
					ScalarValue::Timestamp {
						value: Some(value),
						unit: TimeUnit::Millisecond,
					} => builder.append_value(*value),
					ScalarValue::Timestamp {
						value: None,
						unit: TimeUnit::Millisecond,
					} => builder.append_null(),
					other => {
						return Err(type_mismatch(
							field_index,
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
				match scalar_at(row, field_index)? {
					ScalarValue::Timestamp {
						value: Some(value),
						unit: TimeUnit::Microsecond,
					} => builder.append_value(*value),
					ScalarValue::Timestamp {
						value: None,
						unit: TimeUnit::Microsecond,
					} => builder.append_null(),
					other => {
						return Err(type_mismatch(
							field_index,
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
				match scalar_at(row, field_index)? {
					ScalarValue::Timestamp {
						value: Some(value),
						unit: TimeUnit::Nanosecond,
					} => builder.append_value(*value),
					ScalarValue::Timestamp {
						value: None,
						unit: TimeUnit::Nanosecond,
					} => builder.append_null(),
					other => {
						return Err(type_mismatch(
							field_index,
							"Timestamp(Nanosecond, UTC)",
							other,
						));
					}
				}
			}
			Ok(Arc::new(builder.finish()))
		}
		TimeUnit::Second => Err(StorageError::Internal {
			message: "unsupported INSERT timestamp unit: Second".to_string(),
		}),
	}
}

fn scalar_at(row: &[ScalarValue], field_index: usize) -> Result<&ScalarValue, StorageError> {
	row.get(field_index).ok_or_else(|| StorageError::Internal {
		message: format!("INSERT row missing field index {field_index}"),
	})
}

fn type_mismatch(field_index: usize, expected: &str, actual: &ScalarValue) -> StorageError {
	StorageError::Internal {
		message: format!(
			"INSERT materialization type mismatch at field {field_index}: expected {expected}, got {actual:?}"
		),
	}
}
