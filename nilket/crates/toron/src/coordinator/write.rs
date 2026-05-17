use std::sync::Arc;

use arrow::array::{
	ArrayRef, BooleanBuilder, Float32Builder, Float64Builder, Int32Builder, Int64Builder,
	StringBuilder,
};
use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;

use crate::error::StorageError;
use crate::ir::ScalarValue;

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
