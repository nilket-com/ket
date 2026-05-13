use crate::error::StorageError;
use crate::ir::{ColumnOrdinal, MorselId, QueryId, QueryProfileHint, SessionId, TableId};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use futures::stream::{self, BoxStream};
use std::collections::HashMap;
use std::sync::Arc;
use wendelon::schema::pbc::OP_PROJECT;
use wendelon::schema::{PhysicalDescriptorFingerprint, TableObjectId};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TenantId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct HlcBound(pub u64);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TraceContext {
	pub trace_id: [u8; 16],
	pub span_id: [u8; 8],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ScanTablePin {
	pub table_object_id: TableObjectId,
	pub physical_descriptor_fingerprint: PhysicalDescriptorFingerprint,
}

#[derive(Debug, Clone)]
pub struct ScanRequest {
	pub session_id: SessionId,
	pub query_id: QueryId,
	pub tenant_id: TenantId,
	pub table_id: TableId,
	pub table_object_id: TableObjectId,
	pub physical_descriptor_fingerprint: PhysicalDescriptorFingerprint,
	pub read_bound: HlcBound,
	pub projected_columns: Option<Vec<ColumnOrdinal>>,
	pub pbc_predicate: Option<Vec<u8>>,
	pub profile_hint: Option<QueryProfileHint>,
	pub trace_context: Option<TraceContext>,
	pub morsel: Option<(MorselId, u32)>,
}

pub struct ScanStream {
	pub schema: SchemaRef,
	pub session_id: SessionId,
	pub query_id: QueryId,
	batches: BoxStream<'static, Result<RecordBatch, StorageError>>,
}

impl ScanStream {
	pub fn new(
		schema: SchemaRef,
		session_id: SessionId,
		query_id: QueryId,
		batches: BoxStream<'static, Result<RecordBatch, StorageError>>,
	) -> Self {
		Self {
			schema,
			session_id,
			query_id,
			batches,
		}
	}

	pub fn from_batches(
		schema: SchemaRef,
		session_id: SessionId,
		query_id: QueryId,
		batches: Vec<RecordBatch>,
	) -> Self {
		Self::new(
			schema,
			session_id,
			query_id,
			stream::iter(batches.into_iter().map(Ok)).boxed(),
		)
	}

	pub(crate) fn into_batches(self) -> BoxStream<'static, Result<RecordBatch, StorageError>> {
		self.batches
	}
}

#[async_trait::async_trait]
pub trait StorageScan: Send + Sync {
	async fn scan(&self, request: ScanRequest) -> Result<ScanStream, StorageError>;
}

pub struct InMemoryStorageScan {
	pub table_data: RecordBatch,
}

#[async_trait::async_trait]
impl StorageScan for InMemoryStorageScan {
	async fn scan(&self, request: ScanRequest) -> Result<ScanStream, StorageError> {
		let mut batch = self.table_data.clone();

		if let Some((morsel_id, total_morsels)) = request.morsel {
			let total_rows = batch.num_rows();
			let rows_per_morsel =
				(total_rows + total_morsels as usize - 1) / total_morsels as usize;
			let start_row = (morsel_id.0 as usize) * rows_per_morsel;
			let end_row = std::cmp::min(start_row + rows_per_morsel, total_rows);

			if start_row < total_rows {
				batch = batch.slice(start_row, end_row - start_row);
			} else {
				batch = batch.slice(0, 0); // Empty batch for out of bounds morsels
			}
		}

		if let Some(pbc) = request.pbc_predicate {
			batch = wendelon::schema::pbc::execute(&pbc, &batch).map_err(|error| {
				StorageError::InvalidPbcProgram {
					message: format!("{error:?}"),
				}
			})?;
		}

		if let Some(cols) = request.projected_columns {
			let indices = cols
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

pub struct WendelonStorageScan {
	client: Arc<wendelon_client::Client>,
	descriptors: HashMap<TableId, wendelon_client::TableSchemaDescriptor>,
}

impl WendelonStorageScan {
	pub fn new(
		client: Arc<wendelon_client::Client>,
		descriptors: HashMap<TableId, wendelon_client::TableSchemaDescriptor>,
	) -> Self {
		Self {
			client,
			descriptors,
		}
	}
}

#[async_trait::async_trait]
impl StorageScan for WendelonStorageScan {
	async fn scan(&self, request: ScanRequest) -> Result<ScanStream, StorageError> {
		let descriptor =
			self.descriptors
				.get(&request.table_id)
				.ok_or(StorageError::TableNotFound {
					table: request.table_id,
				})?;
		if request.table_object_id != descriptor.table_object_id {
			return Err(StorageError::DescriptorEvolved);
		}
		if request.physical_descriptor_fingerprint != descriptor.physical_descriptor_fingerprint {
			return Err(StorageError::DescriptorEvolved);
		}

		let mut pbc = request.pbc_predicate.unwrap_or_default();
		if let Some(projected_columns) = &request.projected_columns {
			append_projection(&mut pbc, projected_columns)?;
		}

		let mut options = wendelon_client::TableQueryOptions::default();
		if request.read_bound.0 > 0 {
			options.as_of_hlc = Some(wendelon::schema::HybridTimestamp {
				physical_ms: request.read_bound.0,
				logical: 0,
				node_id: 0,
			});
		}
		options.physical_descriptor_fingerprint = Some(request.physical_descriptor_fingerprint);

		let tenant_id = wendelon::schema::TenantId::new(request.tenant_id.0);

		let query_result = self
			.client
			.table_query(tenant_id, descriptor, pbc, options)
			.await?;
		let schema = if let Some(batch) = query_result.batches.first() {
			batch.schema()
		} else {
			Arc::new(
				descriptor
					.arrow_schema()
					.map_err(|error| StorageError::Internal {
						message: error.to_string(),
					})?,
			)
		};

		Ok(ScanStream::from_batches(
			schema,
			request.session_id,
			request.query_id,
			query_result.batches,
		))
	}
}

fn append_projection(
	bytecode: &mut Vec<u8>,
	projected_columns: &[ColumnOrdinal],
) -> Result<(), StorageError> {
	let projection_len =
		u16::try_from(projected_columns.len()).map_err(|_| StorageError::InvalidPbcProgram {
			message: "projection column count exceeds u16::MAX".to_string(),
		})?;

	bytecode.push(OP_PROJECT);
	bytecode.extend_from_slice(&projection_len.to_le_bytes());
	for column in projected_columns {
		bytecode.extend_from_slice(&column.0.to_le_bytes());
	}
	Ok(())
}
