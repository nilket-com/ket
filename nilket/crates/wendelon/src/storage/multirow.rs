use crate::schema::table::{
	MultiRowLayoutDescriptor, STORAGE_ROW_HLC_COLUMN, STORAGE_ROW_KEY_ID_COLUMN,
	STORAGE_ROW_ROUTING_LOCALITY_COLUMN, TableSchemaDescriptorV1, TableWriteAdmission,
	TableWriteRequestMeta, validate_table_write_request_batch,
};
use crate::schema::{
	HybridTimestamp, Identity, Locality, PbcRejectCode, RecordLayoutPolicy, RecordRowIndex,
	TableWriteValidationError,
};
use crate::storage::core::MultiRowIndexEntry;
use arrow::array::{
	ArrayRef, FixedSizeBinaryArray, FixedSizeBinaryBuilder, UInt32Array, UInt64Array, UInt64Builder,
};
use arrow::compute::{concat_batches, take};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use std::collections::BTreeMap;
use std::io::{Cursor, Error, ErrorKind};
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PackedMultiRowRecord {
	pub locality: Locality,
	pub source_row_indices: Vec<usize>,
	pub key_identity: Identity,
	pub row_hlc: HybridTimestamp,
	pub frame_bytes: Vec<u8>,
	pub charged_bytes: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MultiRowSourceRow {
	pub source_row_index: usize,
	pub key_identity: Identity,
	pub routing_locality: Locality,
	pub row_hlc: HybridTimestamp,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StorageRowMetadata {
	pub key_identity: Identity,
	pub routing_locality: Locality,
	pub row_hlc: HybridTimestamp,
	pub record_row_index: RecordRowIndex,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EncodedMultiRowFrame {
	pub frame: Vec<u8>,
	pub rows: Vec<StorageRowMetadata>,
	pub max_row_hlc: HybridTimestamp,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PackedMultiRowStorageFrame {
	pub source_rows: Vec<MultiRowSourceRow>,
	pub encoded: EncodedMultiRowFrame,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct MultiRowSortKey {
	pub routing_locality: Locality,
	pub key_identity: Identity,
	pub row_hlc: HybridTimestamp,
	pub original_ordinal: usize,
}

impl From<StorageRowMetadata> for MultiRowIndexEntry {
	fn from(row: StorageRowMetadata) -> Self {
		Self {
			key_identity: row.key_identity,
			routing_locality: row.routing_locality,
			row_hlc: row.row_hlc,
			record_row_index: row.record_row_index,
		}
	}
}

pub fn pack_record_batch_by_bucket(
	meta: &TableWriteRequestMeta,
	descriptor: &TableSchemaDescriptorV1,
	batch: &RecordBatch,
	row_hlc: HybridTimestamp,
) -> Result<Vec<PackedMultiRowRecord>, TableWriteValidationError> {
	let admission = validate_table_write_request_batch(meta, batch, descriptor)?;
	pack_admitted_record_batch_by_bucket(meta, descriptor, batch, &admission, row_hlc)
}

pub fn pack_admitted_record_batch_by_bucket(
	meta: &TableWriteRequestMeta,
	descriptor: &TableSchemaDescriptorV1,
	batch: &RecordBatch,
	admission: &TableWriteAdmission,
	row_hlc: HybridTimestamp,
) -> Result<Vec<PackedMultiRowRecord>, TableWriteValidationError> {
	let layout = match &descriptor.record_layout_policy {
		RecordLayoutPolicy::MultiRow(layout) => *layout,
		RecordLayoutPolicy::SingleRow => {
			return Err(table_pack_error(
				"multi-row packing requires a MultiRow record_layout_policy",
			));
		}
	};
	let mut buckets: BTreeMap<Locality, Vec<MultiRowSourceRow>> = BTreeMap::new();
	for (row, locality) in admission.localities.iter().copied().enumerate() {
		buckets
			.entry(locality)
			.or_default()
			.push(MultiRowSourceRow {
				source_row_index: row,
				key_identity: admission.key_identities[row],
				routing_locality: locality,
				row_hlc,
			});
	}

	let target_rows = layout.target_rows.get().max(1) as usize;
	let max_rows = layout.max_rows.get().max(1) as usize;
	let max_record_bytes = layout.max_record_bytes.get();
	let mut packed = Vec::new();
	for (locality, mut rows) in buckets {
		rows.sort_by_key(|row| multirow_sort_key(*row));
		let mut cursor = 0;
		while cursor < rows.len() {
			let remaining = rows.len() - cursor;
			let upper = remaining.min(target_rows).min(max_rows).max(1);
			let mut low = 1;
			let mut high = upper;
			let mut best = None;
			while low <= high {
				let mid = (low + high) / 2;
				let encoded = encode_multirow_from_survivors(batch, &rows[cursor..cursor + mid])
					.map_err(table_pack_io_error)?;
				if encoded.frame.len() as u64 <= max_record_bytes || mid == 1 {
					best = Some((mid, encoded));
					low = mid + 1;
				} else {
					high = mid.saturating_sub(1);
				}
			}
			let (take_count, encoded) = best.expect("upper bound is at least one row");
			if encoded.frame.len() as u64 > max_record_bytes {
				return Err(TableWriteValidationError {
					code: PbcRejectCode::PBC_REJECT_BATCH_TOO_LARGE,
					message: format!(
						"single multi-row table row exceeds max_record_bytes: row_bytes={} max_record_bytes={max_record_bytes}",
						encoded.frame.len()
					),
				});
			}
			let first = encoded.rows.first().ok_or_else(|| {
				table_pack_error("multi-row table record cannot be empty after encoding")
			})?;
			packed.push(PackedMultiRowRecord {
				locality,
				source_row_indices: rows[cursor..cursor + take_count]
					.iter()
					.map(|row| row.source_row_index)
					.collect(),
				key_identity: first.key_identity,
				row_hlc: encoded.max_row_hlc,
				charged_bytes: encoded.frame.len() as u64,
				frame_bytes: encoded.frame,
			});
			cursor += take_count;
		}
	}
	let _ = meta;
	Ok(packed)
}

pub fn encode_multirow_from_survivors(
	batch: &RecordBatch,
	survivors: &[MultiRowSourceRow],
) -> std::io::Result<EncodedMultiRowFrame> {
	if survivors.is_empty() {
		return Err(Error::new(
			ErrorKind::InvalidInput,
			"multi-row storage record cannot be empty",
		));
	}
	let mut rows = survivors.to_vec();
	rows.sort_by_key(|row| multirow_sort_key(*row));
	let source_row_indices = rows
		.iter()
		.map(|row| {
			if row.source_row_index >= batch.num_rows() {
				return Err(Error::new(
					ErrorKind::InvalidData,
					format!(
						"multi-row source row index {} out of bounds for {}-row batch",
						row.source_row_index,
						batch.num_rows()
					),
				));
			}
			Ok(row.source_row_index)
		})
		.collect::<std::io::Result<Vec<_>>>()?;
	let taken = take_rows(batch, &source_row_indices)?;
	let storage_batch = append_storage_synthetic_columns_from_rows(&taken, &rows)?;
	let frame = encode_batch(&storage_batch)?;
	let decoded_rows = storage_rows_from_batch(&storage_batch)?;
	let max_row_hlc = decoded_rows
		.iter()
		.map(|row| row.row_hlc)
		.max()
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "empty multi-row metadata"))?;
	Ok(EncodedMultiRowFrame {
		frame,
		rows: decoded_rows,
		max_row_hlc,
	})
}

pub fn pack_multirow_source_rows(
	layout: MultiRowLayoutDescriptor,
	batch: &RecordBatch,
	rows: &[MultiRowSourceRow],
) -> std::io::Result<Vec<PackedMultiRowStorageFrame>> {
	pack_multirow_source_rows_with_caps(
		batch,
		rows,
		layout.target_rows.get().max(1) as usize,
		layout.max_rows.get().max(1) as usize,
		layout.max_record_bytes.get(),
	)
}

pub fn decode_multirow_synthetic_only(
	frame: &[u8],
) -> std::io::Result<Option<Vec<StorageRowMetadata>>> {
	let batch = match decode_batch(frame) {
		Ok(batch) => batch,
		Err(_) => return Ok(None),
	};
	let has_hlc = batch.column_by_name(STORAGE_ROW_HLC_COLUMN).is_some();
	let has_identity = batch.column_by_name(STORAGE_ROW_KEY_ID_COLUMN).is_some();
	let has_locality = batch
		.column_by_name(STORAGE_ROW_ROUTING_LOCALITY_COLUMN)
		.is_some();
	if !(has_hlc || has_identity || has_locality) {
		return Ok(None);
	}
	if !(has_hlc && has_identity && has_locality) {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"multi-row storage record has an incomplete synthetic-column set",
		));
	}
	Ok(Some(storage_rows_from_batch(&batch)?))
}

pub fn merge_multirow_streams(
	left: &[RecordBatch],
	right: &[RecordBatch],
	layout_max_rows: usize,
	max_record_bytes: u64,
) -> std::io::Result<Vec<EncodedMultiRowFrame>> {
	let mut batches = left.to_vec();
	batches.extend_from_slice(right);
	if batches.is_empty() {
		return Ok(Vec::new());
	}
	let schema = batches[0].schema();
	let merged = concat_batches(&schema, &batches)
		.map_err(|err| Error::new(ErrorKind::InvalidData, format!("multi-row merge: {err}")))?;
	let rows = storage_rows_from_batch(&merged)?;
	let mut source_rows = rows
		.iter()
		.enumerate()
		.map(|(row, meta)| MultiRowSourceRow {
			source_row_index: row,
			key_identity: meta.key_identity,
			routing_locality: meta.routing_locality,
			row_hlc: meta.row_hlc,
		})
		.collect::<Vec<_>>();
	source_rows.sort_by_key(|row| multirow_sort_key(*row));

	Ok(pack_multirow_source_rows_with_caps(
		&merged,
		&source_rows,
		layout_max_rows.max(1),
		layout_max_rows.max(1),
		max_record_bytes,
	)?
	.into_iter()
	.map(|packed| packed.encoded)
	.collect())
}

pub fn multirow_sort_key(row: MultiRowSourceRow) -> MultiRowSortKey {
	MultiRowSortKey {
		routing_locality: row.routing_locality,
		key_identity: row.key_identity,
		row_hlc: row.row_hlc,
		original_ordinal: row.source_row_index,
	}
}

pub fn has_storage_synthetic_columns(batch: &RecordBatch) -> bool {
	batch.column_by_name(STORAGE_ROW_HLC_COLUMN).is_some()
		&& batch.column_by_name(STORAGE_ROW_KEY_ID_COLUMN).is_some()
		&& batch
			.column_by_name(STORAGE_ROW_ROUTING_LOCALITY_COLUMN)
			.is_some()
}

pub fn take_rows(batch: &RecordBatch, row_indices: &[usize]) -> std::io::Result<RecordBatch> {
	let indices = UInt32Array::from(
		row_indices
			.iter()
			.map(|row| *row as u32)
			.collect::<Vec<_>>(),
	);
	let columns = batch
		.columns()
		.iter()
		.map(|column| {
			take(column.as_ref(), &indices, None)
				.map_err(|err| Error::new(ErrorKind::InvalidData, format!("multi-row take: {err}")))
		})
		.collect::<std::io::Result<Vec<ArrayRef>>>()?;
	RecordBatch::try_new(batch.schema(), columns).map_err(|err| {
		Error::new(
			ErrorKind::InvalidData,
			format!("multi-row taken batch: {err}"),
		)
	})
}

pub fn append_storage_synthetic_columns(
	batch: &RecordBatch,
	row_indices: &[usize],
	admission: &TableWriteAdmission,
	row_hlc: HybridTimestamp,
) -> std::io::Result<RecordBatch> {
	let source_rows = row_indices
		.iter()
		.map(|row| {
			if *row >= admission.key_identities.len() || *row >= admission.localities.len() {
				return Err(Error::new(
					ErrorKind::InvalidData,
					format!("admission metadata missing source row {row}"),
				));
			}
			Ok(MultiRowSourceRow {
				source_row_index: *row,
				key_identity: admission.key_identities[*row],
				routing_locality: admission.localities[*row],
				row_hlc,
			})
		})
		.collect::<std::io::Result<Vec<_>>>()?;
	append_storage_synthetic_columns_from_rows(batch, &source_rows)
}

pub fn replace_storage_row_hlc_column(
	batch: &RecordBatch,
	row_hlc: HybridTimestamp,
) -> std::io::Result<RecordBatch> {
	if !has_storage_synthetic_columns(batch) {
		return Ok(batch.clone());
	}
	let hlc_bytes = hlc_storage_bytes(row_hlc);
	let mut row_hlc_builder = FixedSizeBinaryBuilder::with_capacity(batch.num_rows(), 16);
	for _ in 0..batch.num_rows() {
		row_hlc_builder
			.append_value(hlc_bytes)
			.map_err(|err| Error::new(ErrorKind::InvalidData, format!("row hlc append: {err}")))?;
	}
	let row_hlc_array: ArrayRef = Arc::new(row_hlc_builder.finish());
	let columns = batch
		.schema()
		.fields()
		.iter()
		.enumerate()
		.map(|(index, field)| {
			if field.name() == STORAGE_ROW_HLC_COLUMN {
				row_hlc_array.clone()
			} else {
				batch.column(index).clone()
			}
		})
		.collect::<Vec<_>>();
	RecordBatch::try_new(batch.schema(), columns).map_err(|err| {
		Error::new(
			ErrorKind::InvalidData,
			format!("storage hlc rewrite: {err}"),
		)
	})
}

pub fn strip_storage_synthetic_columns(batch: &RecordBatch) -> std::io::Result<RecordBatch> {
	if !has_storage_synthetic_columns(batch) {
		return Ok(batch.clone());
	}
	let indices = batch
		.schema()
		.fields()
		.iter()
		.enumerate()
		.filter_map(|(index, field)| {
			(!matches!(
				field.name().as_str(),
				STORAGE_ROW_HLC_COLUMN
					| STORAGE_ROW_KEY_ID_COLUMN
					| STORAGE_ROW_ROUTING_LOCALITY_COLUMN
			))
			.then_some(index)
		})
		.collect::<Vec<_>>();
	batch.project(&indices).map_err(|err| {
		Error::new(
			ErrorKind::InvalidData,
			format!("storage column strip: {err}"),
		)
	})
}

pub fn materialize_record_location_batch(
	batch: &RecordBatch,
	record_row_index: RecordRowIndex,
) -> std::io::Result<RecordBatch> {
	if !has_storage_synthetic_columns(batch) {
		return Ok(batch.clone());
	}
	let row = record_row_index.get() as usize;
	if row >= batch.num_rows() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"record row index {} out of bounds for {}-row storage record",
				row,
				batch.num_rows()
			),
		));
	}
	strip_storage_synthetic_columns(&batch.slice(row, 1))
}

pub fn storage_rows_from_batch(batch: &RecordBatch) -> std::io::Result<Vec<StorageRowMetadata>> {
	if !has_storage_synthetic_columns(batch) {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"multi-row storage record missing synthetic columns",
		));
	}
	let row_hlcs = batch
		.column_by_name(STORAGE_ROW_HLC_COLUMN)
		.and_then(|array| array.as_any().downcast_ref::<FixedSizeBinaryArray>())
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "missing storage row hlc"))?;
	let identities = batch
		.column_by_name(STORAGE_ROW_KEY_ID_COLUMN)
		.and_then(|array| array.as_any().downcast_ref::<FixedSizeBinaryArray>())
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "missing storage row identity"))?;
	let localities = batch
		.column_by_name(STORAGE_ROW_ROUTING_LOCALITY_COLUMN)
		.and_then(|array| array.as_any().downcast_ref::<UInt64Array>())
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "missing storage row locality"))?;
	let mut entries = Vec::with_capacity(batch.num_rows());
	for row in 0..batch.num_rows() {
		let record_row_index = u32::try_from(row).map_err(|_| {
			Error::new(
				ErrorKind::InvalidData,
				"multi-row storage record row index exceeds u32",
			)
		})?;
		entries.push(StorageRowMetadata {
			key_identity: Identity::new(fixed_32_value(
				identities,
				row,
				STORAGE_ROW_KEY_ID_COLUMN,
			)?),
			routing_locality: Locality::new(localities.value(row)),
			row_hlc: decode_storage_hlc(fixed_16_value(row_hlcs, row, STORAGE_ROW_HLC_COLUMN)?),
			record_row_index: RecordRowIndex::new(record_row_index),
		});
	}
	Ok(entries)
}

pub(crate) fn storage_index_entries_from_batch(
	batch: &RecordBatch,
) -> std::io::Result<Vec<MultiRowIndexEntry>> {
	storage_rows_from_batch(batch).map(|rows| rows.into_iter().map(Into::into).collect())
}

pub fn encode_batch(batch: &RecordBatch) -> std::io::Result<Vec<u8>> {
	let mut buf = Vec::new();
	{
		let mut writer = StreamWriter::try_new(&mut buf, &batch.schema()).map_err(|err| {
			Error::new(ErrorKind::InvalidData, format!("arrow stream init: {err}"))
		})?;
		writer.write(batch).map_err(|err| {
			Error::new(ErrorKind::InvalidData, format!("arrow stream write: {err}"))
		})?;
		writer.finish().map_err(|err| {
			Error::new(
				ErrorKind::InvalidData,
				format!("arrow stream finish: {err}"),
			)
		})?;
	}
	Ok(buf)
}

pub fn decode_batch(bytes: &[u8]) -> std::io::Result<RecordBatch> {
	let mut reader = StreamReader::try_new(Cursor::new(bytes), None)
		.map_err(|err| Error::new(ErrorKind::InvalidData, format!("arrow stream open: {err}")))?;
	let batch = match reader.next() {
		Some(Ok(batch)) => Ok(batch),
		Some(Err(err)) => Err(Error::new(
			ErrorKind::InvalidData,
			format!("arrow stream read: {err}"),
		)),
		None => Err(Error::new(
			ErrorKind::InvalidData,
			"arrow stream contained no record batch",
		)),
	}?;
	match reader.next() {
		Some(Ok(_)) => Err(Error::new(
			ErrorKind::InvalidData,
			"arrow stream contained multiple record batches",
		)),
		Some(Err(err)) => Err(Error::new(
			ErrorKind::InvalidData,
			format!("arrow stream read: {err}"),
		)),
		None => Ok(batch),
	}
}

pub fn hlc_storage_bytes(hlc: HybridTimestamp) -> [u8; 16] {
	let mut bytes = [0_u8; 16];
	bytes[..8].copy_from_slice(&hlc.physical_ms.to_le_bytes());
	bytes[8..12].copy_from_slice(&hlc.logical.to_le_bytes());
	bytes[12..16].copy_from_slice(&hlc.node_id.to_le_bytes());
	bytes
}

pub fn decode_storage_hlc(bytes: [u8; 16]) -> HybridTimestamp {
	HybridTimestamp {
		physical_ms: u64::from_le_bytes(bytes[0..8].try_into().expect("fixed width")),
		logical: u32::from_le_bytes(bytes[8..12].try_into().expect("fixed width")),
		node_id: u32::from_le_bytes(bytes[12..16].try_into().expect("fixed width")),
	}
}

fn append_storage_synthetic_columns_from_rows(
	batch: &RecordBatch,
	rows: &[MultiRowSourceRow],
) -> std::io::Result<RecordBatch> {
	if batch.num_rows() != rows.len() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"storage metadata row count mismatch: batch={} metadata={}",
				batch.num_rows(),
				rows.len()
			),
		));
	}
	let mut fields = batch.schema().fields().iter().cloned().collect::<Vec<_>>();
	fields.push(Arc::new(Field::new(
		STORAGE_ROW_HLC_COLUMN,
		DataType::FixedSizeBinary(16),
		false,
	)));
	fields.push(Arc::new(Field::new(
		STORAGE_ROW_KEY_ID_COLUMN,
		DataType::FixedSizeBinary(32),
		false,
	)));
	fields.push(Arc::new(Field::new(
		STORAGE_ROW_ROUTING_LOCALITY_COLUMN,
		DataType::UInt64,
		false,
	)));

	let mut row_hlc_builder = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
	let mut row_identity_builder = FixedSizeBinaryBuilder::with_capacity(rows.len(), 32);
	let mut row_locality_builder = UInt64Builder::with_capacity(rows.len());
	for row in rows {
		row_hlc_builder
			.append_value(hlc_storage_bytes(row.row_hlc))
			.map_err(|err| Error::new(ErrorKind::InvalidData, format!("row hlc append: {err}")))?;
		row_identity_builder
			.append_value(row.key_identity.as_bytes())
			.map_err(|err| {
				Error::new(
					ErrorKind::InvalidData,
					format!("row identity append: {err}"),
				)
			})?;
		row_locality_builder.append_value(row.routing_locality.get());
	}

	let mut columns = batch.columns().to_vec();
	columns.push(Arc::new(row_hlc_builder.finish()) as ArrayRef);
	columns.push(Arc::new(row_identity_builder.finish()) as ArrayRef);
	columns.push(Arc::new(row_locality_builder.finish()) as ArrayRef);
	RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(|err| {
		Error::new(
			ErrorKind::InvalidData,
			format!("storage table batch: {err}"),
		)
	})
}

fn pack_multirow_source_rows_with_caps(
	batch: &RecordBatch,
	rows: &[MultiRowSourceRow],
	target_rows: usize,
	max_rows: usize,
	max_record_bytes: u64,
) -> std::io::Result<Vec<PackedMultiRowStorageFrame>> {
	if rows.is_empty() {
		return Ok(Vec::new());
	}
	let mut buckets: BTreeMap<Locality, Vec<MultiRowSourceRow>> = BTreeMap::new();
	for row in rows {
		buckets.entry(row.routing_locality).or_default().push(*row);
	}

	let mut frames = Vec::new();
	for (_locality, mut bucket_rows) in buckets {
		bucket_rows.sort_by_key(|row| multirow_sort_key(*row));
		let mut cursor = 0;
		while cursor < bucket_rows.len() {
			let remaining = bucket_rows.len() - cursor;
			let upper = remaining.min(target_rows).min(max_rows).max(1);
			let mut low = 1;
			let mut high = upper;
			let mut best = None;
			while low <= high {
				let mid = (low + high) / 2;
				let source_rows = &bucket_rows[cursor..cursor + mid];
				let encoded = encode_multirow_from_survivors(batch, source_rows)?;
				if encoded.frame.len() as u64 <= max_record_bytes || mid == 1 {
					best = Some((mid, encoded));
					low = mid + 1;
				} else {
					high = mid.saturating_sub(1);
				}
			}
			let (take_count, encoded) = best.expect("upper bound is at least one row");
			if encoded.frame.len() as u64 > max_record_bytes {
				return Err(Error::new(
					ErrorKind::InvalidData,
					format!(
						"single multi-row storage row exceeds max_record_bytes: row_bytes={} max_record_bytes={max_record_bytes}",
						encoded.frame.len()
					),
				));
			}
			frames.push(PackedMultiRowStorageFrame {
				source_rows: bucket_rows[cursor..cursor + take_count].to_vec(),
				encoded,
			});
			cursor += take_count;
		}
	}
	Ok(frames)
}

fn fixed_32_value(
	array: &FixedSizeBinaryArray,
	row: usize,
	label: &str,
) -> std::io::Result<[u8; 32]> {
	let value = array.value(row);
	if value.len() != 32 {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!("{label} width mismatch: expected=32 actual={}", value.len()),
		));
	}
	let mut out = [0_u8; 32];
	out.copy_from_slice(value);
	Ok(out)
}

fn fixed_16_value(
	array: &FixedSizeBinaryArray,
	row: usize,
	label: &str,
) -> std::io::Result<[u8; 16]> {
	let value = array.value(row);
	if value.len() != 16 {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!("{label} width mismatch: expected=16 actual={}", value.len()),
		));
	}
	let mut out = [0_u8; 16];
	out.copy_from_slice(value);
	Ok(out)
}

fn table_pack_io_error(err: std::io::Error) -> TableWriteValidationError {
	TableWriteValidationError {
		code: PbcRejectCode::PBC_REJECT_COLUMN_TYPE_MISMATCH,
		message: format!("failed to encode multi-row table payload: {err}"),
	}
}

fn table_pack_error(message: impl Into<String>) -> TableWriteValidationError {
	TableWriteValidationError {
		code: PbcRejectCode::PBC_REJECT_BATCH_TOO_LARGE,
		message: message.into(),
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::schema::{
		LocalityPolicyV1, MultiRowLayoutDescriptor, PrimaryKeyDescriptorV1, RecordLayoutPolicy,
		SchemaEvolutionPolicyV1, TableObjectId, TableSpreadLocalityDescriptorV1, TenantId,
		encode_table_write_row_count,
	};
	use arrow::array::Int64Array;
	use std::sync::Arc;

	fn hlc(ms: u64) -> HybridTimestamp {
		HybridTimestamp {
			physical_ms: ms,
			logical: 0,
			node_id: 1,
		}
	}

	fn descriptor() -> TableSchemaDescriptorV1 {
		let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
		let mut descriptor = TableSchemaDescriptorV1::new(
			TableObjectId::new([0x42; 16]),
			1,
			"analytics",
			"events",
			&schema,
			PrimaryKeyDescriptorV1::single("id"),
			LocalityPolicyV1::TableSpreadProjection(TableSpreadLocalityDescriptorV1 {
				spread_factor: 1,
				prefix_bits: 1,
			}),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.unwrap();
		descriptor.record_layout_policy =
			RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::default());
		descriptor.schema_fingerprint = descriptor.compute_schema_fingerprint().unwrap();
		descriptor
	}

	fn meta(descriptor: &TableSchemaDescriptorV1, rows: usize) -> TableWriteRequestMeta {
		let mut meta: TableWriteRequestMeta = bytemuck::Zeroable::zeroed();
		meta.tenant_id = TenantId::new(7);
		meta.table_object_id = descriptor.table_object_id;
		meta.schema_generation = descriptor.schema_generation;
		meta.row_count = encode_table_write_row_count(rows).unwrap();
		meta.client_hlc = hlc(10);
		meta
	}

	fn batch(values: Vec<i64>) -> RecordBatch {
		RecordBatch::try_new(
			Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
			vec![Arc::new(Int64Array::from(values))],
		)
		.unwrap()
	}

	#[test]
	fn packs_records_with_synthetic_columns_and_deterministic_order() {
		let descriptor = descriptor();
		let batch = batch(vec![3, 1, 2]);
		let meta = meta(&descriptor, batch.num_rows());
		let packed = pack_record_batch_by_bucket(&meta, &descriptor, &batch, meta.client_hlc)
			.expect("pack multi-row");
		assert!(!packed.is_empty());
		let mut logical_rows = 0;
		for record in packed {
			let decoded = decode_batch(&record.frame_bytes).unwrap();
			assert!(has_storage_synthetic_columns(&decoded));
			let rows = storage_rows_from_batch(&decoded).unwrap();
			let mut sorted = rows.clone();
			sorted.sort_by_key(|row| MultiRowSortKey {
				routing_locality: row.routing_locality,
				key_identity: row.key_identity,
				row_hlc: row.row_hlc,
				original_ordinal: row.record_row_index.get() as usize,
			});
			assert_eq!(rows, sorted);
			let user = strip_storage_synthetic_columns(&decoded).unwrap();
			assert_eq!(user.schema().as_ref(), batch.schema().as_ref());
			logical_rows += user.num_rows();
		}
		assert_eq!(logical_rows, batch.num_rows());
	}

	#[test]
	fn survivor_encoding_is_byte_stable() {
		let batch = batch(vec![1, 2, 3]);
		let rows = [
			MultiRowSourceRow {
				source_row_index: 2,
				key_identity: Identity::new([3; 32]),
				routing_locality: Locality::new(2),
				row_hlc: hlc(12),
			},
			MultiRowSourceRow {
				source_row_index: 0,
				key_identity: Identity::new([1; 32]),
				routing_locality: Locality::new(2),
				row_hlc: hlc(11),
			},
		];
		let first = encode_multirow_from_survivors(&batch, &rows).unwrap();
		let second = encode_multirow_from_survivors(&batch, &rows).unwrap();
		assert_eq!(first.frame, second.frame);
		assert_eq!(first.rows, second.rows);
		assert_eq!(first.frame.len(), 1160);
		assert_eq!(
			crate::hash::blake3::digest(&first.frame),
			[
				190, 182, 14, 43, 167, 44, 237, 216, 202, 227, 192, 107, 154, 92, 84, 203, 87, 145,
				186, 111, 24, 178, 48, 13, 152, 185, 218, 122, 104, 44, 124, 225,
			]
		);
	}

	#[test]
	fn merge_multirow_streams_preserves_sorted_hlc_metadata_and_caps() {
		let batch = batch(vec![30, 10, 20, 40]);
		let left = encode_multirow_from_survivors(
			&batch,
			&[
				MultiRowSourceRow {
					source_row_index: 0,
					key_identity: Identity::new([3; 32]),
					routing_locality: Locality::new(7),
					row_hlc: hlc(30),
				},
				MultiRowSourceRow {
					source_row_index: 1,
					key_identity: Identity::new([1; 32]),
					routing_locality: Locality::new(7),
					row_hlc: hlc(10),
				},
			],
		)
		.unwrap();
		let right = encode_multirow_from_survivors(
			&batch,
			&[
				MultiRowSourceRow {
					source_row_index: 2,
					key_identity: Identity::new([2; 32]),
					routing_locality: Locality::new(7),
					row_hlc: hlc(20),
				},
				MultiRowSourceRow {
					source_row_index: 3,
					key_identity: Identity::new([4; 32]),
					routing_locality: Locality::new(7),
					row_hlc: hlc(40),
				},
			],
		)
		.unwrap();
		let left_batch = decode_batch(&left.frame).unwrap();
		let right_batch = decode_batch(&right.frame).unwrap();

		let merged = merge_multirow_streams(&[left_batch], &[right_batch], 2, 1024 * 1024).unwrap();

		assert_eq!(merged.len(), 2);
		let mut all_rows = Vec::new();
		for frame in merged {
			assert!(frame.rows.len() <= 2);
			let decoded = decode_batch(&frame.frame).unwrap();
			assert!(has_storage_synthetic_columns(&decoded));
			assert_eq!(storage_rows_from_batch(&decoded).unwrap(), frame.rows);
			all_rows.extend(frame.rows);
		}
		let mut sorted = all_rows.clone();
		sorted.sort_by_key(|row| MultiRowSortKey {
			routing_locality: row.routing_locality,
			key_identity: row.key_identity,
			row_hlc: row.row_hlc,
			original_ordinal: row.record_row_index.get() as usize,
		});
		assert_eq!(all_rows, sorted);
		assert_eq!(
			all_rows
				.iter()
				.map(|row| row.row_hlc.physical_ms)
				.collect::<Vec<_>>(),
			vec![10, 20, 30, 40]
		);
	}
}
