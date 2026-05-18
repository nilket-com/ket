use crate::error::StorageError;
use crate::ir::{ColumnOrdinal, MorselId, QueryId, QueryProfileHint, SessionId, TableId};
use arrow::array::{Array, ArrayRef, FixedSizeBinaryBuilder, UInt32Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use futures::stream::{self, BoxStream};
use serde::{Deserialize, Serialize};
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use wendelon::schema::pbc::OP_PROJECT;
use wendelon::schema::{
	Identity, PhysicalDescriptorFingerprint, RuntimeFilterDescriptor, STORAGE_ROW_KEY_ID_COLUMN,
	TableObjectId, build_runtime_filter_mask_for_batch, validate_runtime_filter_descriptor,
};

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

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RowIdentityProjection {
	#[default]
	None,
	Append,
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
	pub runtime_filter: Option<RuntimeFilterDescriptor>,
	pub row_identity: RowIdentityProjection,
	pub profile_hint: Option<QueryProfileHint>,
	pub trace_context: Option<TraceContext>,
	pub morsel: Option<(MorselId, u32)>,
}

#[derive(Debug)]
pub struct WriteRequest {
	pub session_id: SessionId,
	pub query_id: QueryId,
	pub tenant_id: TenantId,
	pub table_id: TableId,
	pub table_object_id: TableObjectId,
	pub physical_descriptor_fingerprint: PhysicalDescriptorFingerprint,
	pub payload: RecordBatch,
	pub profile_hint: Option<QueryProfileHint>,
	pub trace_context: Option<TraceContext>,
}

#[derive(Debug)]
pub struct DeleteRowsRequest {
	pub session_id: SessionId,
	pub query_id: QueryId,
	pub tenant_id: TenantId,
	pub table_id: TableId,
	pub table_object_id: TableObjectId,
	pub physical_descriptor_fingerprint: PhysicalDescriptorFingerprint,
	pub row_key_ids: Vec<Identity>,
	pub profile_hint: Option<QueryProfileHint>,
	pub trace_context: Option<TraceContext>,
}

#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriteOutcome {
	pub rows: usize,
}

#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteRowsOutcome {
	pub requested: usize,
	pub deleted: usize,
	pub already_deleted: usize,
	pub not_found: usize,
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

#[async_trait::async_trait]
pub trait StorageWrite: Send + Sync {
	async fn write(&self, request: WriteRequest) -> Result<WriteOutcome, StorageError>;
}

#[async_trait::async_trait]
pub trait StorageDeleteRows: Send + Sync {
	async fn delete_rows(
		&self,
		request: DeleteRowsRequest,
	) -> Result<DeleteRowsOutcome, StorageError>;
}

pub struct InMemoryStorageScan {
	pub table_data: RecordBatch,
}

#[async_trait::async_trait]
impl StorageScan for InMemoryStorageScan {
	async fn scan(&self, request: ScanRequest) -> Result<ScanStream, StorageError> {
		scan_in_memory_batch(self.table_data.clone(), None, request)
	}
}

struct InMemoryTableData {
	table_object_id: TableObjectId,
	physical_descriptor_fingerprint: PhysicalDescriptorFingerprint,
	batch: RecordBatch,
	row_key_ids: Vec<Identity>,
	deleted_row_key_ids: HashSet<Identity>,
	next_row_key_ordinal: u64,
}

pub struct InMemoryTableStorage {
	tables: Mutex<HashMap<TableId, InMemoryTableData>>,
}

impl InMemoryTableStorage {
	pub fn new() -> Self {
		Self {
			tables: Mutex::new(HashMap::new()),
		}
	}
}

impl Default for InMemoryTableStorage {
	fn default() -> Self {
		Self::new()
	}
}

#[async_trait::async_trait]
impl StorageWrite for InMemoryTableStorage {
	async fn write(&self, request: WriteRequest) -> Result<WriteOutcome, StorageError> {
		let mut tables = self
			.tables
			.lock()
			.expect("InMemoryTableStorage lock poisoned");
		let rows = request.payload.num_rows();
		match tables.entry(request.table_id) {
			Entry::Vacant(entry) => {
				let row_key_ids = in_memory_row_identities(request.table_object_id, 1, rows);
				entry.insert(InMemoryTableData {
					table_object_id: request.table_object_id,
					physical_descriptor_fingerprint: request.physical_descriptor_fingerprint,
					batch: request.payload,
					row_key_ids,
					deleted_row_key_ids: HashSet::new(),
					next_row_key_ordinal: 1 + rows as u64,
				});
			}
			Entry::Occupied(mut entry) => {
				let data = entry.get_mut();
				if data.table_object_id != request.table_object_id
					|| data.physical_descriptor_fingerprint
						!= request.physical_descriptor_fingerprint
				{
					return Err(StorageError::DescriptorEvolved);
				}
				if data.batch.schema() != request.payload.schema() {
					return Err(StorageError::Internal {
						message: "INSERT payload schema does not match existing table schema"
							.to_string(),
					});
				}
				let appended_row_key_ids =
					in_memory_row_identities(data.table_object_id, data.next_row_key_ordinal, rows);
				data.next_row_key_ordinal += rows as u64;
				data.row_key_ids.extend(appended_row_key_ids);
				let batches = [data.batch.clone(), request.payload];
				data.batch = arrow::compute::concat_batches(&data.batch.schema(), batches.iter())?;
			}
		}
		Ok(WriteOutcome { rows })
	}
}

#[async_trait::async_trait]
impl StorageScan for InMemoryTableStorage {
	async fn scan(&self, request: ScanRequest) -> Result<ScanStream, StorageError> {
		let (batch, identity_context) = {
			let tables = self
				.tables
				.lock()
				.expect("InMemoryTableStorage lock poisoned");
			let data = tables
				.get(&request.table_id)
				.ok_or(StorageError::TableNotFound {
					table: request.table_id,
				})?;
			if data.table_object_id != request.table_object_id {
				return Err(StorageError::DescriptorEvolved);
			}
			if data.physical_descriptor_fingerprint != request.physical_descriptor_fingerprint {
				return Err(StorageError::DescriptorEvolved);
			}
			(
				data.batch.clone(),
				Some(InMemoryIdentityContext {
					row_key_ids: data.row_key_ids.clone(),
					deleted_row_key_ids: data.deleted_row_key_ids.clone(),
				}),
			)
		};
		scan_in_memory_batch(batch, identity_context, request)
	}
}

#[async_trait::async_trait]
impl StorageDeleteRows for InMemoryTableStorage {
	async fn delete_rows(
		&self,
		request: DeleteRowsRequest,
	) -> Result<DeleteRowsOutcome, StorageError> {
		let mut tables = self
			.tables
			.lock()
			.expect("InMemoryTableStorage lock poisoned");
		let data = tables
			.get_mut(&request.table_id)
			.ok_or(StorageError::TableNotFound {
				table: request.table_id,
			})?;
		if data.table_object_id != request.table_object_id {
			return Err(StorageError::DescriptorEvolved);
		}
		if data.physical_descriptor_fingerprint != request.physical_descriptor_fingerprint {
			return Err(StorageError::DescriptorEvolved);
		}

		let live_row_key_ids = data.row_key_ids.iter().copied().collect::<HashSet<_>>();
		let mut deleted = 0;
		let mut already_deleted = 0;
		let mut not_found = 0;
		for row_key_id in &request.row_key_ids {
			if !live_row_key_ids.contains(row_key_id) {
				not_found += 1;
			} else if data.deleted_row_key_ids.contains(row_key_id) {
				already_deleted += 1;
			} else {
				data.deleted_row_key_ids.insert(*row_key_id);
				deleted += 1;
			}
		}

		Ok(DeleteRowsOutcome {
			requested: request.row_key_ids.len(),
			deleted,
			already_deleted,
			not_found,
		})
	}
}

struct InMemoryIdentityContext {
	row_key_ids: Vec<Identity>,
	deleted_row_key_ids: HashSet<Identity>,
}

fn scan_in_memory_batch(
	mut batch: RecordBatch,
	mut identity_context: Option<InMemoryIdentityContext>,
	request: ScanRequest,
) -> Result<ScanStream, StorageError> {
	if request.row_identity == RowIdentityProjection::Append && request.projected_columns.is_some()
	{
		return Err(StorageError::Internal {
			message: "scan with row_identity=Append cannot be combined with projected_columns"
				.to_string(),
		});
	}
	if request.row_identity == RowIdentityProjection::Append && identity_context.is_none() {
		return Err(StorageError::Internal {
			message: "row identity projection requires table storage identity context".to_string(),
		});
	}
	if let Some(context) = &mut identity_context {
		if context.row_key_ids.len() != batch.num_rows() {
			return Err(StorageError::Internal {
				message: "in-memory row identity cardinality does not match batch".to_string(),
			});
		}
		let live_rows = context
			.row_key_ids
			.iter()
			.enumerate()
			.filter_map(|(row, row_key_id)| {
				(!context.deleted_row_key_ids.contains(row_key_id)).then_some(row as u32)
			})
			.collect::<Vec<_>>();
		batch = take_record_batch_rows(&batch, &live_rows)?;
		context.row_key_ids = take_row_key_ids(&context.row_key_ids, &live_rows);
	}

	if let Some((morsel_id, total_morsels)) = request.morsel {
		let total_rows = batch.num_rows();
		let rows_per_morsel = (total_rows + total_morsels as usize - 1) / total_morsels as usize;
		let start_row = (morsel_id.0 as usize) * rows_per_morsel;
		let end_row = std::cmp::min(start_row + rows_per_morsel, total_rows);

		if start_row < total_rows {
			batch = batch.slice(start_row, end_row - start_row);
			if let Some(context) = &mut identity_context {
				context.row_key_ids = context.row_key_ids[start_row..end_row].to_vec();
			}
		} else {
			batch = batch.slice(0, 0);
			if let Some(context) = &mut identity_context {
				context.row_key_ids.clear();
			}
		}
	}

	if let Some(runtime_filter) = &request.runtime_filter {
		if validate_runtime_filter_descriptor(
			runtime_filter,
			batch.schema().as_ref(),
			request.physical_descriptor_fingerprint,
		)
		.is_ok()
		{
			if let Ok(mask) = build_runtime_filter_mask_for_batch(&batch, runtime_filter) {
				if let Some(context) = &mut identity_context {
					context.row_key_ids = filter_row_key_ids(&context.row_key_ids, &mask);
				}
				batch = arrow::compute::filter_record_batch(&batch, &mask)?;
			}
		}
	}

	if let Some(pbc) = request.pbc_predicate {
		if request.row_identity == RowIdentityProjection::Append {
			let program = wendelon::schema::pbc::Program::compile(&pbc).map_err(|error| {
				StorageError::InvalidPbcProgram {
					message: format!("{error:?}"),
				}
			})?;
			let mask = wendelon::schema::pbc::survival_mask(&program, &batch).map_err(|error| {
				StorageError::InvalidPbcProgram {
					message: format!("{error:?}"),
				}
			})?;
			if let Some(context) = &mut identity_context {
				context.row_key_ids = filter_row_key_ids(&context.row_key_ids, &mask);
			}
			batch = wendelon::schema::pbc::execute_program(&program, &batch).map_err(|error| {
				StorageError::InvalidPbcProgram {
					message: format!("{error:?}"),
				}
			})?;
		} else {
			batch = wendelon::schema::pbc::execute(&pbc, &batch).map_err(|error| {
				StorageError::InvalidPbcProgram {
					message: format!("{error:?}"),
				}
			})?;
		}
	}

	if let Some(cols) = request.projected_columns {
		let indices = cols
			.into_iter()
			.map(|column| column.0 as usize)
			.collect::<Vec<_>>();
		batch = batch.project(&indices)?;
	}

	if request.row_identity == RowIdentityProjection::Append {
		let context = identity_context.ok_or_else(|| StorageError::Internal {
			message: "row identity projection requires table storage identity context".to_string(),
		})?;
		batch = append_row_identity_column(batch, &context.row_key_ids)?;
	}

	Ok(ScanStream::from_batches(
		batch.schema(),
		request.session_id,
		request.query_id,
		vec![batch],
	))
}

fn in_memory_row_identities(
	table_object_id: TableObjectId,
	start_ordinal: u64,
	rows: usize,
) -> Vec<Identity> {
	(0..rows)
		.map(|offset| in_memory_row_identity(table_object_id, start_ordinal + offset as u64))
		.collect()
}

fn in_memory_row_identity(table_object_id: TableObjectId, ordinal: u64) -> Identity {
	let mut bytes = [0; 32];
	bytes[0..16].copy_from_slice(table_object_id.as_bytes());
	bytes[24..32].copy_from_slice(&ordinal.to_be_bytes());
	Identity::new(bytes)
}

fn take_record_batch_rows(
	batch: &RecordBatch,
	row_indices: &[u32],
) -> Result<RecordBatch, StorageError> {
	let indices = UInt32Array::from(row_indices.to_vec());
	let columns = batch
		.columns()
		.iter()
		.map(|column| arrow::compute::take(column.as_ref(), &indices, None))
		.collect::<Result<Vec<_>, _>>()?;
	Ok(RecordBatch::try_new(batch.schema(), columns)?)
}

fn take_row_key_ids(row_key_ids: &[Identity], row_indices: &[u32]) -> Vec<Identity> {
	row_indices
		.iter()
		.map(|row| row_key_ids[*row as usize])
		.collect()
}

fn filter_row_key_ids(
	row_key_ids: &[Identity],
	mask: &arrow::array::BooleanArray,
) -> Vec<Identity> {
	row_key_ids
		.iter()
		.enumerate()
		.filter_map(|(row, row_key_id)| {
			(!mask.is_null(row) && mask.value(row)).then_some(*row_key_id)
		})
		.collect()
}

fn append_row_identity_column(
	batch: RecordBatch,
	row_key_ids: &[Identity],
) -> Result<RecordBatch, StorageError> {
	if row_key_ids.len() != batch.num_rows() {
		return Err(StorageError::Internal {
			message: "row identity projection cardinality does not match batch".to_string(),
		});
	}
	let mut builder = FixedSizeBinaryBuilder::with_capacity(row_key_ids.len(), 32);
	for row_key_id in row_key_ids {
		builder
			.append_value(row_key_id.as_bytes())
			.map_err(|error| StorageError::Internal {
				message: format!("row identity append failed: {error}"),
			})?;
	}
	let mut fields = batch
		.schema()
		.fields()
		.iter()
		.map(|field| field.as_ref().clone())
		.collect::<Vec<_>>();
	fields.push(Field::new(
		STORAGE_ROW_KEY_ID_COLUMN,
		DataType::FixedSizeBinary(32),
		false,
	));
	let mut columns = batch.columns().to_vec();
	columns.push(Arc::new(builder.finish()) as ArrayRef);
	Ok(RecordBatch::try_new(
		Arc::new(Schema::new(fields)),
		columns,
	)?)
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
		if request.row_identity == RowIdentityProjection::Append
			&& request.projected_columns.is_some()
		{
			return Err(StorageError::Internal {
				message: "scan with row_identity=Append cannot be combined with projected_columns"
					.to_string(),
			});
		}
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
		options.row_identity = match request.row_identity {
			RowIdentityProjection::None => wendelon_client::RowIdentityProjection::None,
			RowIdentityProjection::Append => wendelon_client::RowIdentityProjection::Append,
		};

		let tenant_id = wendelon::schema::TenantId::new(request.tenant_id.0);

		let query_result = self
			.client
			.table_query_with_runtime_filter(
				tenant_id,
				descriptor,
				pbc,
				request.runtime_filter.clone(),
				options,
			)
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

pub struct WendelonStorageWrite {
	client: Arc<wendelon_client::Client>,
	descriptors: HashMap<TableId, wendelon_client::TableSchemaDescriptor>,
}

impl WendelonStorageWrite {
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
impl StorageWrite for WendelonStorageWrite {
	async fn write(&self, request: WriteRequest) -> Result<WriteOutcome, StorageError> {
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

		let descriptor_schema =
			descriptor
				.arrow_schema()
				.map_err(|error| StorageError::Internal {
					message: format!("descriptor arrow_schema: {error}"),
				})?;
		if request.payload.schema().as_ref() != &descriptor_schema {
			return Err(StorageError::Internal {
				message: "INSERT payload schema does not match descriptor".to_string(),
			});
		}

		let tenant_id = wendelon::schema::TenantId::new(request.tenant_id.0);
		let receipt = self
			.client
			.table_write(
				tenant_id,
				descriptor,
				request.payload,
				wendelon_client::WriteOptions {
					await_finalization: true,
					..Default::default()
				},
			)
			.await
			.map_err(StorageError::from)?;

		Ok(WriteOutcome { rows: receipt.rows })
	}
}

pub struct WendelonStorageDeleteRows {
	client: Arc<wendelon_client::Client>,
	descriptors: HashMap<TableId, wendelon_client::TableSchemaDescriptor>,
}

impl WendelonStorageDeleteRows {
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
impl StorageDeleteRows for WendelonStorageDeleteRows {
	async fn delete_rows(
		&self,
		request: DeleteRowsRequest,
	) -> Result<DeleteRowsOutcome, StorageError> {
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

		let tenant_id = wendelon::schema::TenantId::new(request.tenant_id.0);
		let receipt = self
			.client
			.table_delete(
				tenant_id,
				descriptor,
				request.row_key_ids,
				wendelon_client::WriteOptions {
					await_finalization: true,
					..Default::default()
				},
			)
			.await
			.map_err(StorageError::from)?;

		Ok(DeleteRowsOutcome {
			requested: receipt.requested,
			deleted: receipt.deleted_count as usize,
			already_deleted: receipt.already_deleted_count as usize,
			not_found: receipt.not_found_count as usize,
		})
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

#[cfg(test)]
mod tests {
	use super::*;

	use arrow::array::{ArrayRef, Int32Array};
	use arrow::datatypes::{DataType, Field, Schema};
	use wendelon::schema::{
		RUNTIME_FILTER_HASH_SEED, RUNTIME_FILTER_KEY_ENCODING_VERSION, RuntimeFilterBloom,
		RuntimeFilterKeyType, RuntimeFilterKind, encode_runtime_filter_key,
		insert_runtime_filter_encoded_key,
	};

	#[tokio::test]
	async fn in_memory_runtime_filter_applies_after_morsel_and_before_projection() {
		let storage = InMemoryStorageScan {
			table_data: test_batch(),
		};
		let request = test_request(Some(runtime_filter_for_int32_values(&[4, 6])));

		let stream = storage.scan(request).await.expect("scan");
		let batches = collect_test_batches(stream).await;

		assert_eq!(projected_values(&batches), vec![40, 60]);
	}

	#[tokio::test]
	async fn in_memory_runtime_filter_mismatch_advisory_drops() {
		let storage = InMemoryStorageScan {
			table_data: test_batch(),
		};
		let mut filter = runtime_filter_for_int32_values(&[4, 6]);
		filter.probe_descriptor_fingerprint = fingerprint([8; 32]);

		let stream = storage
			.scan(test_request(Some(filter)))
			.await
			.expect("scan");
		let batches = collect_test_batches(stream).await;

		assert_eq!(projected_values(&batches), vec![40, 50, 60]);
	}

	#[tokio::test]
	async fn in_memory_storage_scan_appends_stable_row_identity() {
		let storage = InMemoryTableStorage::new();
		write_identity_test_batch(&storage).await;

		let first = collect_test_batches(
			storage
				.scan(table_scan_request(RowIdentityProjection::Append))
				.await
				.expect("identity scan"),
		)
		.await;
		let second = collect_test_batches(
			storage
				.scan(table_scan_request(RowIdentityProjection::Append))
				.await
				.expect("identity scan"),
		)
		.await;

		let first_ids = row_key_id_bytes(&first[0]);
		let second_ids = row_key_id_bytes(&second[0]);
		assert_eq!(first_ids, second_ids);
		assert_eq!(
			first[0]
				.schema()
				.field(first[0].num_columns() - 1)
				.name()
				.as_str(),
			STORAGE_ROW_KEY_ID_COLUMN
		);
		assert_eq!(
			first[0]
				.schema()
				.field(first[0].num_columns() - 1)
				.data_type(),
			&DataType::FixedSizeBinary(32)
		);
		assert_eq!(first_ids.len(), 4);
		for (index, row_key_id) in first_ids.iter().enumerate() {
			assert_eq!(&row_key_id[0..16], TableObjectId::new([5; 16]).as_bytes());
			assert_eq!(&row_key_id[16..24], &[0; 8]);
			assert_eq!(
				u64::from_be_bytes(row_key_id[24..32].try_into().expect("ordinal")),
				(index + 1) as u64
			);
		}
	}

	#[tokio::test]
	async fn in_memory_storage_normal_scan_hides_row_identity() {
		let storage = InMemoryTableStorage::new();
		write_identity_test_batch(&storage).await;

		let batches = collect_test_batches(
			storage
				.scan(table_scan_request(RowIdentityProjection::None))
				.await
				.expect("normal scan"),
		)
		.await;

		assert_eq!(batches[0].schema(), identity_test_batch().schema());
		assert!(
			batches[0]
				.schema()
				.column_with_name(STORAGE_ROW_KEY_ID_COLUMN)
				.is_none()
		);
	}

	#[tokio::test]
	async fn in_memory_storage_delete_rows_filters_future_scans() {
		let storage = InMemoryTableStorage::new();
		write_identity_test_batch(&storage).await;
		let identity_batches = collect_test_batches(
			storage
				.scan(table_scan_request(RowIdentityProjection::Append))
				.await
				.expect("identity scan"),
		)
		.await;
		let row_key_ids = row_key_id_values(&identity_batches[0]);
		let deleted_ids = vec![row_key_ids[0], row_key_ids[2]];

		let outcome = storage
			.delete_rows(DeleteRowsRequest {
				session_id: SessionId(1),
				query_id: QueryId::from_u128(2),
				tenant_id: TenantId(3),
				table_id: TableId(4),
				table_object_id: TableObjectId::new([5; 16]),
				physical_descriptor_fingerprint: fingerprint([7; 32]),
				row_key_ids: deleted_ids.clone(),
				profile_hint: None,
				trace_context: None,
			})
			.await
			.expect("delete rows");
		assert_eq!(
			outcome,
			DeleteRowsOutcome {
				requested: 2,
				deleted: 2,
				already_deleted: 0,
				not_found: 0
			}
		);

		let normal = collect_test_batches(
			storage
				.scan(table_scan_request(RowIdentityProjection::None))
				.await
				.expect("normal scan"),
		)
		.await;
		assert_eq!(id_values(&normal[0]), vec![2, 4]);

		let identity = collect_test_batches(
			storage
				.scan(table_scan_request(RowIdentityProjection::Append))
				.await
				.expect("identity scan"),
		)
		.await;
		assert_eq!(id_values(&identity[0]), vec![2, 4]);
		let remaining = row_key_id_values(&identity[0]);
		assert!(!remaining.contains(&deleted_ids[0]));
		assert!(!remaining.contains(&deleted_ids[1]));
	}

	#[tokio::test]
	async fn in_memory_storage_delete_rows_reports_already_deleted_and_not_found() {
		let storage = InMemoryTableStorage::new();
		write_identity_test_batch(&storage).await;
		let identity_batches = collect_test_batches(
			storage
				.scan(table_scan_request(RowIdentityProjection::Append))
				.await
				.expect("identity scan"),
		)
		.await;
		let row_key_id = row_key_id_values(&identity_batches[0])[0];
		let fake = Identity::new([0xFF; 32]);

		let first = delete_identity_rows(&storage, vec![row_key_id]).await;
		let second = delete_identity_rows(&storage, vec![row_key_id, fake]).await;

		assert_eq!(first.deleted, 1);
		assert_eq!(
			second,
			DeleteRowsOutcome {
				requested: 2,
				deleted: 0,
				already_deleted: 1,
				not_found: 1
			}
		);
	}

	#[tokio::test]
	async fn in_memory_storage_identity_scan_rejects_user_projection() {
		let storage = InMemoryTableStorage::new();
		write_identity_test_batch(&storage).await;
		let mut request = table_scan_request(RowIdentityProjection::Append);
		request.projected_columns = Some(vec![ColumnOrdinal(0)]);

		let err = match storage.scan(request).await {
			Ok(_) => panic!("expected projection reject"),
			Err(err) => err,
		};
		assert!(matches!(err, StorageError::Internal { .. }));
		assert_eq!(
			err.to_string(),
			"internal storage error: scan with row_identity=Append cannot be combined with projected_columns"
		);
	}

	async fn collect_test_batches(stream: ScanStream) -> Vec<RecordBatch> {
		let mut batches = Vec::new();
		let mut stream = stream.into_batches();
		while let Some(batch) = stream.next().await {
			batches.push(batch.expect("scan batch"));
		}
		batches
	}

	fn test_batch() -> RecordBatch {
		let schema = Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int32, false),
			Field::new("value", DataType::Int32, false),
		]));
		RecordBatch::try_new(
			schema,
			vec![
				Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6])) as ArrayRef,
				Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50, 60])) as ArrayRef,
			],
		)
		.expect("batch")
	}

	fn identity_test_batch() -> RecordBatch {
		let schema = Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int32, false),
			Field::new("value", DataType::Int32, false),
		]));
		RecordBatch::try_new(
			schema,
			vec![
				Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as ArrayRef,
				Arc::new(Int32Array::from(vec![10, 20, 30, 40])) as ArrayRef,
			],
		)
		.expect("identity test batch")
	}

	async fn write_identity_test_batch(storage: &InMemoryTableStorage) {
		storage
			.write(WriteRequest {
				session_id: SessionId(1),
				query_id: QueryId::from_u128(2),
				tenant_id: TenantId(3),
				table_id: TableId(4),
				table_object_id: TableObjectId::new([5; 16]),
				physical_descriptor_fingerprint: fingerprint([7; 32]),
				payload: identity_test_batch(),
				profile_hint: None,
				trace_context: None,
			})
			.await
			.expect("write identity test batch");
	}

	fn table_scan_request(row_identity: RowIdentityProjection) -> ScanRequest {
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
			row_identity,
			profile_hint: None,
			trace_context: None,
			morsel: None,
		}
	}

	async fn delete_identity_rows(
		storage: &InMemoryTableStorage,
		row_key_ids: Vec<Identity>,
	) -> DeleteRowsOutcome {
		storage
			.delete_rows(DeleteRowsRequest {
				session_id: SessionId(1),
				query_id: QueryId::from_u128(2),
				tenant_id: TenantId(3),
				table_id: TableId(4),
				table_object_id: TableObjectId::new([5; 16]),
				physical_descriptor_fingerprint: fingerprint([7; 32]),
				row_key_ids,
				profile_hint: None,
				trace_context: None,
			})
			.await
			.expect("delete identity rows")
	}

	fn test_request(runtime_filter: Option<RuntimeFilterDescriptor>) -> ScanRequest {
		ScanRequest {
			session_id: SessionId(1),
			query_id: QueryId::from_u128(2),
			tenant_id: TenantId(3),
			table_id: TableId(4),
			table_object_id: TableObjectId::new([5; 16]),
			physical_descriptor_fingerprint: fingerprint([7; 32]),
			read_bound: HlcBound(0),
			projected_columns: Some(vec![ColumnOrdinal(1)]),
			pbc_predicate: None,
			runtime_filter,
			row_identity: RowIdentityProjection::None,
			profile_hint: None,
			trace_context: None,
			morsel: Some((MorselId(1), 2)),
		}
	}

	fn runtime_filter_for_int32_values(values: &[i32]) -> RuntimeFilterDescriptor {
		let bit_count = 1024;
		let hash_count = 3;
		let byte_budget = bit_count / 8;
		let mut payload = vec![0; byte_budget as usize];
		let array = Int32Array::from(values.to_vec());
		for row in 0..array.len() {
			let encoded = encode_runtime_filter_key(&array, row)
				.expect("encode")
				.expect("non-null key");
			insert_runtime_filter_encoded_key(
				&mut payload,
				bit_count,
				hash_count,
				RUNTIME_FILTER_HASH_SEED,
				&encoded,
			);
		}
		RuntimeFilterDescriptor {
			key_encoding_version: RUNTIME_FILTER_KEY_ENCODING_VERSION,
			key_types: vec![RuntimeFilterKeyType::Int32],
			probe_key_columns: vec![0],
			kind: RuntimeFilterKind::Bloom(RuntimeFilterBloom {
				hash_seed: RUNTIME_FILTER_HASH_SEED,
				bit_count,
				hash_count,
				byte_budget,
				target_fp_rate_bp: 100,
				payload,
			}),
			probe_descriptor_fingerprint: fingerprint([7; 32]),
		}
	}

	fn fingerprint(bytes: [u8; 32]) -> PhysicalDescriptorFingerprint {
		postcard::from_bytes(&bytes).expect("fingerprint")
	}

	fn projected_values(batches: &[RecordBatch]) -> Vec<i32> {
		assert_eq!(batches.len(), 1);
		batches[0]
			.column(0)
			.as_any()
			.downcast_ref::<Int32Array>()
			.expect("value column")
			.values()
			.to_vec()
	}

	fn id_values(batch: &RecordBatch) -> Vec<i32> {
		batch
			.column(0)
			.as_any()
			.downcast_ref::<Int32Array>()
			.expect("id int32")
			.values()
			.to_vec()
	}

	fn row_key_id_values(batch: &RecordBatch) -> Vec<Identity> {
		row_key_id_bytes(batch)
			.into_iter()
			.map(Identity::new)
			.collect()
	}

	fn row_key_id_bytes(batch: &RecordBatch) -> Vec<[u8; 32]> {
		let array = batch
			.column(batch.num_columns() - 1)
			.as_any()
			.downcast_ref::<arrow::array::FixedSizeBinaryArray>()
			.expect("fixed size binary row key id");
		(0..array.len())
			.map(|row| array.value(row).try_into().expect("32-byte row id"))
			.collect()
	}
}
