use std::sync::Arc;

use arrow::array::{
	ArrayRef, BooleanArray, BooleanBuilder, Float32Array, Float32Builder, Float64Array,
	Float64Builder, Int32Array, Int32Builder, Int64Array, Int64Builder, StringArray, StringBuilder,
};
use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;

use crate::binder::TABLE_WRITE_MAX_ROWS;
use crate::error::{ExecutionError, StorageError};
use crate::exec::evaluate_cast;
use crate::ir::{ColumnOrdinal, ScalarValue};

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

fn is_supported_insert_select_target(data_type: &DataType) -> bool {
	matches!(
		data_type,
		DataType::Boolean
			| DataType::Int32
			| DataType::Int64
			| DataType::Float32
			| DataType::Float64
			| DataType::Utf8
	)
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
