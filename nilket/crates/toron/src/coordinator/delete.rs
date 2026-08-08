use std::sync::Arc;

use arrow::array::{Array, ArrayRef, FixedSizeBinaryArray, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use wendelon::schema::{Identity, STORAGE_ROW_KEY_ID_COLUMN};

use crate::error::{ExecutionError, StorageError};
use crate::storage::{
	DeleteRowsOutcome, DeleteRowsRequest, RowIdentityProjection, ScanRequest, StorageDeleteRows,
	StorageScan,
};

#[allow(dead_code)]
pub(crate) async fn materialize_delete_row_key_ids(
	storage: &dyn StorageScan,
	scan_request: ScanRequest,
) -> Result<Vec<Identity>, StorageError> {
	let batches = materialize_delete_candidate_batches(storage, scan_request).await?;
	extract_delete_row_key_ids(&batches)
}

#[allow(dead_code)]
pub(crate) async fn materialize_delete_candidate_batches(
	storage: &dyn StorageScan,
	scan_request: ScanRequest,
) -> Result<Vec<RecordBatch>, StorageError> {
	if scan_request.row_identity != RowIdentityProjection::Append {
		return Err(StorageError::Internal {
			message: "delete candidate materializer requires row_identity=Append".to_string(),
		});
	}
	super::materialize_scan_stream(storage.scan(scan_request).await?).await
}

pub(crate) fn extract_delete_row_key_ids(
	batches: &[RecordBatch],
) -> Result<Vec<Identity>, StorageError> {
	let mut row_key_ids = Vec::new();
	for batch in batches {
		if batch.num_columns() == 0 {
			return Err(StorageError::Internal {
				message: "delete candidate batch missing row identity column".to_string(),
			});
		}
		let column_index = batch.num_columns() - 1;
		let schema = batch.schema();
		let field = schema.field(column_index);
		if field.name() != STORAGE_ROW_KEY_ID_COLUMN {
			return Err(StorageError::Internal {
				message: format!(
					"delete candidate row identity column must be named {STORAGE_ROW_KEY_ID_COLUMN}"
				),
			});
		}
		if field.data_type() != &DataType::FixedSizeBinary(32) {
			return Err(StorageError::Internal {
				message: "delete candidate row identity column must be FixedSizeBinary(32)"
					.to_string(),
			});
		}
		let column = batch
			.column(column_index)
			.as_any()
			.downcast_ref::<FixedSizeBinaryArray>()
			.ok_or_else(|| StorageError::Internal {
				message: "delete candidate row identity column has unexpected array type"
					.to_string(),
			})?;
		if column.null_count() != 0 {
			return Err(StorageError::Internal {
				message: "delete candidate row identity column must not contain nulls".to_string(),
			});
		}
		for row in 0..column.len() {
			let bytes: [u8; 32] =
				column
					.value(row)
					.try_into()
					.map_err(|_| StorageError::Internal {
						message: "delete candidate row identity value is not 32 bytes".to_string(),
					})?;
			row_key_ids.push(Identity::new(bytes));
		}
	}
	Ok(row_key_ids)
}

pub(crate) fn delete_candidate_user_batches(
	batches: &[RecordBatch],
	user_schema: &SchemaRef,
) -> Result<Vec<RecordBatch>, StorageError> {
	let mut user_batches = Vec::with_capacity(batches.len());
	for batch in batches {
		if batch.num_columns() == 0 {
			return Err(StorageError::Internal {
				message: "delete candidate batch missing row identity column".to_string(),
			});
		}
		let expected_columns = user_schema.fields().len() + 1;
		if batch.num_columns() != expected_columns {
			return Err(StorageError::Internal {
				message: format!(
					"delete candidate batch has {} columns; expected {expected_columns}",
					batch.num_columns()
				),
			});
		}
		let identity_index = batch.num_columns() - 1;
		let field = batch.schema().field(identity_index).clone();
		if field.name() != STORAGE_ROW_KEY_ID_COLUMN {
			return Err(StorageError::Internal {
				message: format!(
					"delete candidate row identity column must be named {STORAGE_ROW_KEY_ID_COLUMN}"
				),
			});
		}
		if field.data_type() != &DataType::FixedSizeBinary(32) {
			return Err(StorageError::Internal {
				message: "delete candidate row identity column must be FixedSizeBinary(32)"
					.to_string(),
			});
		}
		let columns = batch.columns()[..identity_index].to_vec();
		user_batches.push(RecordBatch::try_new(user_schema.clone(), columns)?);
	}
	Ok(user_batches)
}

pub(crate) async fn execute_delete_with_candidates(
	storage: &dyn StorageDeleteRows,
	request: DeleteRowsRequest,
) -> Result<DeleteRowsOutcome, StorageError> {
	if request.row_key_ids.is_empty() {
		return Ok(DeleteRowsOutcome {
			requested: 0,
			deleted: 0,
			already_deleted: 0,
			not_found: 0,
		});
	}
	storage.delete_rows(request).await
}

pub(crate) fn delete_count_batch(count: usize) -> Result<RecordBatch, ExecutionError> {
	let value = i64::try_from(count).map_err(|_| ExecutionError::Kernel {
		message: format!("delete count {count} exceeds Int64 range"),
	})?;
	let schema = Arc::new(Schema::new(vec![Field::new(
		"deleted",
		DataType::Int64,
		false,
	)]));
	let array = Arc::new(Int64Array::from(vec![value])) as ArrayRef;
	RecordBatch::try_new(schema, vec![array]).map_err(ExecutionError::from)
}

#[cfg(test)]
mod tests {
	use super::*;

	use std::sync::Arc;

	use arrow::array::{ArrayRef, Int32Array};
	use arrow::datatypes::{DataType, Field, Schema};
	use arrow::record_batch::RecordBatch;
	use wendelon::schema::{PhysicalDescriptorFingerprint, TableObjectId};

	use crate::ir::{QueryId, SessionId, TableId};
	use crate::storage::{HlcBound, InMemoryTableStorage, StorageWrite, TenantId, WriteRequest};

	#[tokio::test]
	async fn materialize_delete_row_key_ids_extracts_appended_identity_column() {
		let storage = InMemoryTableStorage::new();
		write_rows(&storage).await;

		let row_key_ids = materialize_delete_row_key_ids(
			&storage,
			ScanRequest {
				session_id: SessionId(1),
				query_id: QueryId::from_u128(2),
				tenant_id: TenantId(3),
				table_id: TableId(4),
				table_object_id: TableObjectId::new([5; 16]),
				physical_descriptor_fingerprint: fingerprint([7; 32]),
				read_bound: HlcBound(0),
				projected_columns: None,
				pbc_predicate: None,
				runtime_filter: None,
				row_identity: RowIdentityProjection::Append,
				profile_hint: None,
				trace_context: None,
				morsel: None,
				row_limit: None,
				downstream_cancellation: None,
				deadline: None,
			},
		)
		.await
		.expect("materialize row ids");

		assert_eq!(row_key_ids.len(), 3);
		assert_eq!(&row_key_ids[0].as_bytes()[0..16], &[5; 16]);
		assert_eq!(
			u64::from_be_bytes(row_key_ids[0].as_bytes()[24..32].try_into().unwrap()),
			1
		);
	}

	#[tokio::test]
	async fn execute_delete_with_candidates_short_circuits_empty_candidates() {
		let storage = InMemoryTableStorage::new();

		let outcome = execute_delete_with_candidates(
			&storage,
			DeleteRowsRequest {
				session_id: SessionId(1),
				query_id: QueryId::from_u128(2),
				tenant_id: TenantId(3),
				table_id: TableId(4),
				table_object_id: TableObjectId::new([5; 16]),
				physical_descriptor_fingerprint: fingerprint([7; 32]),
				row_key_ids: Vec::new(),
				profile_hint: None,
				trace_context: None,
			},
		)
		.await
		.expect("empty delete");

		assert_eq!(
			outcome,
			DeleteRowsOutcome {
				requested: 0,
				deleted: 0,
				already_deleted: 0,
				not_found: 0
			}
		);
	}

	async fn write_rows(storage: &InMemoryTableStorage) {
		storage
			.write(WriteRequest {
				session_id: SessionId(1),
				query_id: QueryId::from_u128(2),
				tenant_id: TenantId(3),
				table_id: TableId(4),
				table_object_id: TableObjectId::new([5; 16]),
				physical_descriptor_fingerprint: fingerprint([7; 32]),
				payload: batch(),
				profile_hint: None,
				trace_context: None,
			})
			.await
			.expect("write rows");
	}

	fn batch() -> RecordBatch {
		RecordBatch::try_new(
			Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
			vec![Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef],
		)
		.expect("batch")
	}

	fn fingerprint(bytes: [u8; 32]) -> PhysicalDescriptorFingerprint {
		postcard::from_bytes(&bytes).expect("fingerprint")
	}
}
