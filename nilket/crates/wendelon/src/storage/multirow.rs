use crate::schema::table::{
	MultiRowLayoutDescriptor, STORAGE_ROW_HLC_COLUMN, STORAGE_ROW_KEY_ID_COLUMN,
	STORAGE_ROW_ROUTING_LOCALITY_COLUMN, STORAGE_ROW_TABLE_OBJECT_ID_COLUMN,
	STORAGE_ROW_TOMBSTONE_COLUMN, TableObjectId, TableSchemaDescriptorV1, TableWriteAdmission,
	TableWriteRequestMeta, validate_table_write_request_batch,
};
use crate::schema::{
	HybridTimestamp, Identity, Locality, PbcRejectCode, RecordBodyByteOffset,
	RecordBodyCompressionPolicy, RecordLayoutPolicy, RecordRowIndex, SlabId,
	TableWriteValidationError, WriterNodeId,
};
use crate::storage::columnar::TieredRecordId;
use crate::storage::core::MultiRowIndexEntry;
use crate::storage::encoding::directory::{
	build_encoding_directory_for_ipc_stream, compact_encoding_ipc_stream,
	serialize_encoding_directory_value,
};
use crate::storage::encoding::{ENCODING_DIRECTORY_METADATA_KEY, EncodingWriterConfig};
use crate::storage::ipc::{
	EncodedRecordBatch, encode_record_batch, encode_record_batch_bytes,
	encode_record_batches_bytes, record_batch_compression_policy_from_ipc,
};
use crate::storage::row_group::{
	ROW_GROUP_DIRECTORY_METADATA_KEY, RowGroupWriterConfig, build_page_directory_for_ipc_stream,
	serialize_page_directory_value,
};
use arrow::array::{
	Array, ArrayRef, BooleanArray, BooleanBuilder, FixedSizeBinaryArray, FixedSizeBinaryBuilder,
	UInt32Array, UInt64Array, UInt64Builder,
};
use arrow::compute::{concat_batches, take};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::io::{Cursor, Error, ErrorKind};
use std::sync::Arc;
use std::time::Instant;

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
	pub is_tombstone: bool,
	/// ADR 045 Phase 8 — durable per-row binding back to the
	/// originating descriptor's `table_object_id`. `Some` on
	/// every multi-row record (PK + AppendOnly); `None` on
	/// paths that don't stamp it. The encoder stamps
	/// `__row_table_object_id__` iff every row in a record
	/// has `Some`; partial stamping is rejected.
	pub table_object_id: Option<TableObjectId>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StorageRowMetadata {
	pub key_identity: Identity,
	pub routing_locality: Locality,
	pub row_hlc: HybridTimestamp,
	pub is_tombstone: bool,
	pub record_row_index: RecordRowIndex,
	/// ADR 045 Phase 8 — `Some` if the stored record carries
	/// `__row_table_object_id__` (every multi-row record),
	/// `None` otherwise (single-row records).
	pub table_object_id: Option<TableObjectId>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EncodedMultiRowFrame {
	pub frame: Vec<u8>,
	pub rows: Vec<StorageRowMetadata>,
	pub max_row_hlc: HybridTimestamp,
	pub plaintext_byte_len: u64,
	pub encoded_byte_len: u64,
	pub codec: RecordBodyCompressionPolicy,
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
			is_tombstone: row.is_tombstone,
			record_row_index: row.record_row_index,
		}
	}
}

pub fn pack_record_batch_by_bucket(
	meta: &TableWriteRequestMeta,
	descriptor: &TableSchemaDescriptorV1,
	batch: &RecordBatch,
	writer_node_id: WriterNodeId,
	row_hlc: HybridTimestamp,
) -> Result<Vec<PackedMultiRowRecord>, TableWriteValidationError> {
	// ADR 045: the same `writer_node_id` and `row_hlc` feed both
	// admission's identity preimage (for AppendOnly) and the
	// persisted `__row_hlc__` column. The caller MUST pass the
	// admitting server's authenticated `WriterNodeId` and the
	// leader-merged commit HLC.
	let admission =
		validate_table_write_request_batch(meta, batch, descriptor, writer_node_id, row_hlc)?;
	pack_admitted_record_batch_by_bucket(meta, descriptor, batch, &admission, row_hlc)
}

pub fn pack_admitted_record_batch_by_bucket(
	meta: &TableWriteRequestMeta,
	descriptor: &TableSchemaDescriptorV1,
	batch: &RecordBatch,
	admission: &TableWriteAdmission,
	row_hlc: HybridTimestamp,
) -> Result<Vec<PackedMultiRowRecord>, TableWriteValidationError> {
	let row_group_config = RowGroupWriterConfig::default();
	pack_admitted_record_batch_by_bucket_with_row_group_config(
		meta,
		descriptor,
		batch,
		admission,
		row_hlc,
		&row_group_config,
	)
}

pub fn pack_admitted_record_batch_by_bucket_with_row_group_config(
	meta: &TableWriteRequestMeta,
	descriptor: &TableSchemaDescriptorV1,
	batch: &RecordBatch,
	admission: &TableWriteAdmission,
	row_hlc: HybridTimestamp,
	row_group_config: &RowGroupWriterConfig,
) -> Result<Vec<PackedMultiRowRecord>, TableWriteValidationError> {
	pack_admitted_record_batch_by_bucket_with_row_group_and_encoding_config(
		meta,
		descriptor,
		batch,
		admission,
		row_hlc,
		row_group_config,
		&EncodingWriterConfig::default(),
	)
}

pub fn pack_admitted_record_batch_by_bucket_with_row_group_and_encoding_config(
	meta: &TableWriteRequestMeta,
	descriptor: &TableSchemaDescriptorV1,
	batch: &RecordBatch,
	admission: &TableWriteAdmission,
	row_hlc: HybridTimestamp,
	row_group_config: &RowGroupWriterConfig,
	encoding_config: &EncodingWriterConfig,
) -> Result<Vec<PackedMultiRowRecord>, TableWriteValidationError> {
	let layout = match &descriptor.record_layout_policy {
		RecordLayoutPolicy::MultiRow(layout) => *layout,
		RecordLayoutPolicy::SingleRow => {
			return Err(table_pack_error(
				"multi-row packing requires a MultiRow record_layout_policy",
			));
		}
	};
	// ADR 045 Phase 8: stamp `__row_table_object_id__` on every
	// multi-row record (PK + AppendOnly) so descriptor-filtered fast
	// paths can verify table-isolation by byte-compare against the
	// stored column instead of re-deriving identity.
	let stamped_table_object_id = Some(descriptor.table_object_id);
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
				is_tombstone: false,
				table_object_id: stamped_table_object_id,
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
			let mut take_count = upper;
			let mut encoded = encode_multirow_from_survivors_with_row_group_config(
				batch,
				&rows[cursor..cursor + upper],
				descriptor,
				descriptor.record_body_compression_policy,
				row_group_config,
			)
			.map_err(table_pack_io_error)?;
			if encoding_config.enabled {
				encoded = encode_multirow_from_survivors_with_row_group_and_encoding_config(
					batch,
					&rows[cursor..cursor + upper],
					descriptor,
					descriptor.record_body_compression_policy,
					row_group_config,
					encoding_config,
				)
				.map_err(table_pack_io_error)?;
			}
			if encoded.plaintext_byte_len > max_record_bytes && upper > 1 {
				let mut low = 1;
				let mut high = upper - 1;
				let mut best = None;
				while low <= high {
					let mid = (low + high) / 2;
					let candidate = encode_multirow_from_survivors_with_row_group_config(
						batch,
						&rows[cursor..cursor + mid],
						descriptor,
						descriptor.record_body_compression_policy,
						row_group_config,
					)
					.map_err(table_pack_io_error)?;
					let candidate = if encoding_config.enabled {
						encode_multirow_from_survivors_with_row_group_and_encoding_config(
							batch,
							&rows[cursor..cursor + mid],
							descriptor,
							descriptor.record_body_compression_policy,
							row_group_config,
							encoding_config,
						)
						.map_err(table_pack_io_error)?
					} else {
						candidate
					};
					if candidate.plaintext_byte_len <= max_record_bytes || mid == 1 {
						best = Some((mid, candidate));
						low = mid + 1;
					} else {
						high = mid.saturating_sub(1);
					}
				}
				(take_count, encoded) = best.expect("upper bound is at least one row");
			}
			if encoded.plaintext_byte_len > max_record_bytes {
				return Err(TableWriteValidationError {
					code: PbcRejectCode::PBC_REJECT_BATCH_TOO_LARGE,
					message: format!(
						"single multi-row table row exceeds max_record_bytes: row_bytes={} max_record_bytes={max_record_bytes}",
						encoded.plaintext_byte_len
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
				charged_bytes: encoded.encoded_byte_len,
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
	encode_multirow_from_survivors_with_policy(
		batch,
		survivors,
		RecordBodyCompressionPolicy::Uncompressed,
	)
}

pub fn encode_multirow_from_survivors_with_policy(
	batch: &RecordBatch,
	survivors: &[MultiRowSourceRow],
	policy: RecordBodyCompressionPolicy,
) -> std::io::Result<EncodedMultiRowFrame> {
	encode_multirow_from_survivors_with_optional_row_groups(
		batch,
		survivors,
		policy,
		None,
		&RowGroupWriterConfig::default(),
		&EncodingWriterConfig::default(),
	)
}

pub fn encode_multirow_from_survivors_with_row_group_config(
	batch: &RecordBatch,
	survivors: &[MultiRowSourceRow],
	descriptor: &TableSchemaDescriptorV1,
	policy: RecordBodyCompressionPolicy,
	row_group_config: &RowGroupWriterConfig,
) -> std::io::Result<EncodedMultiRowFrame> {
	encode_multirow_from_survivors_with_optional_row_groups(
		batch,
		survivors,
		policy,
		Some(descriptor),
		row_group_config,
		&EncodingWriterConfig::default(),
	)
}

pub fn encode_multirow_from_survivors_with_row_group_and_encoding_config(
	batch: &RecordBatch,
	survivors: &[MultiRowSourceRow],
	descriptor: &TableSchemaDescriptorV1,
	policy: RecordBodyCompressionPolicy,
	row_group_config: &RowGroupWriterConfig,
	encoding_config: &EncodingWriterConfig,
) -> std::io::Result<EncodedMultiRowFrame> {
	encode_multirow_from_survivors_with_optional_row_groups(
		batch,
		survivors,
		policy,
		Some(descriptor),
		row_group_config,
		encoding_config,
	)
}

fn encode_multirow_from_survivors_with_optional_row_groups(
	batch: &RecordBatch,
	survivors: &[MultiRowSourceRow],
	policy: RecordBodyCompressionPolicy,
	descriptor: Option<&TableSchemaDescriptorV1>,
	row_group_config: &RowGroupWriterConfig,
	encoding_config: &EncodingWriterConfig,
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
	let encoded = encode_storage_batch_with_optional_row_groups(
		&storage_batch,
		descriptor,
		policy,
		row_group_config,
		encoding_config,
	)?;
	let decoded_rows = storage_rows_from_batch(&storage_batch)?;
	let max_row_hlc = decoded_rows
		.iter()
		.map(|row| row.row_hlc)
		.max()
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "empty multi-row metadata"))?;
	Ok(EncodedMultiRowFrame {
		frame: encoded.bytes,
		rows: decoded_rows,
		max_row_hlc,
		plaintext_byte_len: encoded.plaintext_byte_len,
		encoded_byte_len: encoded.encoded_byte_len,
		codec: encoded.codec,
	})
}

fn encode_storage_batch_with_optional_row_groups(
	storage_batch: &RecordBatch,
	descriptor: Option<&TableSchemaDescriptorV1>,
	policy: RecordBodyCompressionPolicy,
	row_group_config: &RowGroupWriterConfig,
	encoding_config: &EncodingWriterConfig,
) -> std::io::Result<EncodedRecordBatch> {
	let single = encode_record_batch(storage_batch, policy)?;
	let Some(descriptor) = descriptor else {
		return Ok(single);
	};
	row_group_config.validate()?;
	if !row_group_config.should_page(storage_batch.num_rows(), single.plaintext_byte_len) {
		return Ok(single);
	}
	let batches = split_row_group_batches(storage_batch, row_group_config)?;
	if batches.len() <= 1 {
		return Ok(single);
	}
	encode_row_group_batches_with_directory(
		&batches,
		descriptor,
		policy,
		row_group_config,
		encoding_config,
	)
}

fn split_row_group_batches(
	storage_batch: &RecordBatch,
	row_group_config: &RowGroupWriterConfig,
) -> std::io::Result<Vec<RecordBatch>> {
	let target = row_group_config.target_row_group_rows.get() as usize;
	let mut batches = Vec::new();
	let mut offset = 0_usize;
	while offset < storage_batch.num_rows() {
		let len = target.min(storage_batch.num_rows() - offset);
		batches.push(storage_batch.slice(offset, len));
		offset += len;
	}
	Ok(batches)
}

fn encode_row_group_batches_with_directory(
	base_batches: &[RecordBatch],
	descriptor: &TableSchemaDescriptorV1,
	policy: RecordBodyCompressionPolicy,
	row_group_config: &RowGroupWriterConfig,
	encoding_config: &EncodingWriterConfig,
) -> std::io::Result<EncodedRecordBatch> {
	let mut directory_value = String::new();
	let mut encoding_directory_value = String::new();
	for _ in 0..8 {
		let batches = batches_with_storage_metadata(
			base_batches,
			&directory_value,
			&encoding_directory_value,
		)?;
		let bytes = encode_record_batches_bytes(&batches, policy)?;
		let directory = build_page_directory_for_ipc_stream(
			TieredRecordId {
				slab_id: SlabId::ZERO,
				record_offset: RecordBodyByteOffset::ZERO,
			},
			&bytes,
			&batches,
			descriptor.logical_schema_fingerprint,
			&row_group_config.stats_supported_types,
		)?;
		let (physical_bytes, physical_directory, physical_encoding_directory) =
			if encoding_config.enabled && policy == RecordBodyCompressionPolicy::Uncompressed {
				let (encoding_directory, applied) = build_encoding_directory_for_ipc_stream(
					TieredRecordId {
						slab_id: SlabId::ZERO,
						record_offset: RecordBodyByteOffset::ZERO,
					},
					&bytes,
					&directory,
					&batches,
					encoding_config,
				)?;
				match encoding_directory {
					Some(encoding_directory) => {
						let compacted = compact_encoding_ipc_stream(
							&bytes,
							&directory,
							&encoding_directory,
							&applied,
						)?;
						(
							compacted.bytes,
							compacted.page_directory,
							Some(compacted.encoding_directory),
						)
					}
					None => (bytes.clone(), directory.clone(), None),
				}
			} else {
				(bytes.clone(), directory.clone(), None)
			};
		let next_value = serialize_page_directory_value(&physical_directory)?;
		let next_encoding_value = match physical_encoding_directory {
			Some(encoding_directory) => {
				let value = serialize_encoding_directory_value(&encoding_directory)?;
				if !value.is_empty() {
					crate::metrics::global()
						.observe_storage_l2_encoding_directory_bytes(value.len() as u64);
				};
				value
			}
			None => String::new(),
		};
		if next_value == directory_value && next_encoding_value == encoding_directory_value {
			return encoded_row_group_batches_from_bytes(&batches, policy, physical_bytes);
		}
		directory_value = next_value;
		encoding_directory_value = next_encoding_value;
	}
	crate::metrics::global()
		.observe_storage_l2_row_group_fetch_invariant_violation("page_directory_parse_error");
	Err(Error::new(
		ErrorKind::InvalidData,
		"row-group directory fixed-point encoding did not converge",
	))
}

fn encoded_row_group_batches_from_bytes(
	batches: &[RecordBatch],
	policy: RecordBodyCompressionPolicy,
	bytes: Vec<u8>,
) -> std::io::Result<EncodedRecordBatch> {
	let started = Instant::now();
	let plaintext_byte_len = if policy == RecordBodyCompressionPolicy::Uncompressed {
		bytes.len() as u64
	} else {
		encode_record_batches_bytes(batches, RecordBodyCompressionPolicy::Uncompressed)?.len()
			as u64
	};
	let encoded_byte_len = bytes.len() as u64;
	crate::metrics::global().observe_storage_compression_encode(
		policy,
		plaintext_byte_len,
		encoded_byte_len,
		started.elapsed(),
	);
	Ok(EncodedRecordBatch {
		bytes,
		plaintext_byte_len,
		encoded_byte_len,
		codec: policy,
	})
}

fn batches_with_storage_metadata(
	base_batches: &[RecordBatch],
	directory_value: &str,
	encoding_directory_value: &str,
) -> std::io::Result<Vec<RecordBatch>> {
	let Some(first) = base_batches.first() else {
		return Err(Error::new(
			ErrorKind::InvalidInput,
			"row-group writer requires at least one batch",
		));
	};
	let mut metadata: HashMap<String, String> = first.schema().metadata().clone();
	metadata.insert(
		ROW_GROUP_DIRECTORY_METADATA_KEY.to_string(),
		directory_value.to_string(),
	);
	if !encoding_directory_value.is_empty() {
		metadata.insert(
			ENCODING_DIRECTORY_METADATA_KEY.to_string(),
			encoding_directory_value.to_string(),
		);
	} else {
		metadata.remove(ENCODING_DIRECTORY_METADATA_KEY);
	}
	let schema = Arc::new(Schema::new_with_metadata(
		first.schema().fields().iter().cloned().collect::<Vec<_>>(),
		metadata,
	));
	base_batches
		.iter()
		.map(|batch| {
			if batch.schema().fields() != first.schema().fields() {
				return Err(Error::new(
					ErrorKind::InvalidInput,
					"row-group writer batches must share fields",
				));
			}
			RecordBatch::try_new_with_options(
				schema.clone(),
				batch.columns().to_vec(),
				&RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
			)
			.map_err(|err| {
				Error::new(
					ErrorKind::InvalidData,
					format!("row-group writer schema attach: {err}"),
				)
			})
		})
		.collect()
}

pub fn pack_multirow_source_rows(
	layout: MultiRowLayoutDescriptor,
	batch: &RecordBatch,
	rows: &[MultiRowSourceRow],
) -> std::io::Result<Vec<PackedMultiRowStorageFrame>> {
	pack_multirow_source_rows_with_policy(
		layout,
		batch,
		rows,
		RecordBodyCompressionPolicy::Uncompressed,
	)
}

pub fn pack_multirow_source_rows_with_policy(
	layout: MultiRowLayoutDescriptor,
	batch: &RecordBatch,
	rows: &[MultiRowSourceRow],
	policy: RecordBodyCompressionPolicy,
) -> std::io::Result<Vec<PackedMultiRowStorageFrame>> {
	pack_multirow_source_rows_with_policy_and_row_group_config(
		layout,
		batch,
		rows,
		None,
		policy,
		&RowGroupWriterConfig::default(),
	)
}

pub fn pack_multirow_source_rows_with_policy_and_row_group_config(
	layout: MultiRowLayoutDescriptor,
	batch: &RecordBatch,
	rows: &[MultiRowSourceRow],
	descriptor: Option<&TableSchemaDescriptorV1>,
	policy: RecordBodyCompressionPolicy,
	row_group_config: &RowGroupWriterConfig,
) -> std::io::Result<Vec<PackedMultiRowStorageFrame>> {
	pack_multirow_source_rows_with_policy_and_row_group_and_encoding_config(
		layout,
		batch,
		rows,
		descriptor,
		policy,
		row_group_config,
		&EncodingWriterConfig::default(),
	)
}

pub fn pack_multirow_source_rows_with_policy_and_row_group_and_encoding_config(
	layout: MultiRowLayoutDescriptor,
	batch: &RecordBatch,
	rows: &[MultiRowSourceRow],
	descriptor: Option<&TableSchemaDescriptorV1>,
	policy: RecordBodyCompressionPolicy,
	row_group_config: &RowGroupWriterConfig,
	encoding_config: &EncodingWriterConfig,
) -> std::io::Result<Vec<PackedMultiRowStorageFrame>> {
	pack_multirow_source_rows_with_caps(
		batch,
		rows,
		layout.target_rows.get().max(1) as usize,
		layout.max_rows.get().max(1) as usize,
		layout.max_record_bytes.get(),
		descriptor,
		policy,
		row_group_config,
		encoding_config,
	)
}

pub fn pack_tombstone_multirow_source_rows_with_policy(
	layout: MultiRowLayoutDescriptor,
	rows: &[MultiRowSourceRow],
	policy: RecordBodyCompressionPolicy,
) -> std::io::Result<Vec<PackedMultiRowStorageFrame>> {
	if rows.iter().any(|row| !row.is_tombstone) {
		return Err(Error::new(
			ErrorKind::InvalidInput,
			"tombstone-only multi-row packing received a data row",
		));
	}
	let batch = empty_user_batch_with_row_count(rows.len())?;
	pack_multirow_source_rows_with_policy(layout, &batch, rows, policy)
}

pub fn empty_user_batch_with_row_count(row_count: usize) -> std::io::Result<RecordBatch> {
	RecordBatch::try_new_with_options(
		Arc::new(Schema::empty()),
		Vec::new(),
		&RecordBatchOptions::new().with_row_count(Some(row_count)),
	)
	.map_err(|err| Error::new(ErrorKind::InvalidData, format!("empty user batch: {err}")))
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
	let has_tombstone = batch.column_by_name(STORAGE_ROW_TOMBSTONE_COLUMN).is_some();
	if !(has_hlc || has_identity || has_locality || has_tombstone) {
		return Ok(None);
	}
	if !(has_hlc && has_identity && has_locality && has_tombstone) {
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
			is_tombstone: meta.is_tombstone,
			table_object_id: meta.table_object_id,
		})
		.collect::<Vec<_>>();
	source_rows.sort_by_key(|row| multirow_sort_key(*row));

	Ok(pack_multirow_source_rows_with_caps(
		&merged,
		&source_rows,
		layout_max_rows.max(1),
		layout_max_rows.max(1),
		max_record_bytes,
		None,
		RecordBodyCompressionPolicy::Uncompressed,
		&RowGroupWriterConfig::default(),
		&EncodingWriterConfig::default(),
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
		&& batch.column_by_name(STORAGE_ROW_TOMBSTONE_COLUMN).is_some()
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
	if columns.is_empty() {
		return RecordBatch::try_new_with_options(
			batch.schema(),
			columns,
			&RecordBatchOptions::new().with_row_count(Some(row_indices.len())),
		)
		.map_err(|err| {
			Error::new(
				ErrorKind::InvalidData,
				format!("multi-row taken batch: {err}"),
			)
		});
	}
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
	table_object_id: Option<TableObjectId>,
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
				is_tombstone: false,
				table_object_id,
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
					// ADR 045 Phase 8: optional AppendOnly-only column.
					// Strip if present so user-facing decode stays
					// schema-clean.
					| STORAGE_ROW_TABLE_OBJECT_ID_COLUMN
					| STORAGE_ROW_TOMBSTONE_COLUMN
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
	if storage_row_is_tombstone_at(batch, record_row_index)? {
		return strip_storage_synthetic_columns(&batch.slice(row, 0));
	}
	strip_storage_synthetic_columns(&batch.slice(row, 1))
}

pub fn storage_row_is_tombstone_at(
	batch: &RecordBatch,
	record_row_index: RecordRowIndex,
) -> std::io::Result<bool> {
	let Some(array) = batch.column_by_name(STORAGE_ROW_TOMBSTONE_COLUMN) else {
		return Ok(false);
	};
	let array = array
		.as_any()
		.downcast_ref::<BooleanArray>()
		.ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				"__row_tombstone__ column has unexpected Arrow type",
			)
		})?;
	let row = record_row_index.get() as usize;
	if row >= array.len() {
		return Ok(false);
	}
	Ok(array.value(row))
}

/// ADR 045 Phase 8 — extract the per-row
/// `__row_table_object_id__` value at `record_row_index` if the
/// stored multi-row record carries the column (AppendOnly).
/// Returns `None` for PK-keyed records (which do not stamp the
/// column). The matchers
/// (`table_record_matches_descriptor`,
/// `descriptor_matches_tiered_row`) feed this into AppendOnly
/// descriptor verification.
pub fn storage_row_table_object_id_at(
	batch: &RecordBatch,
	record_row_index: RecordRowIndex,
) -> std::io::Result<Option<TableObjectId>> {
	let Some(array) = batch.column_by_name(STORAGE_ROW_TABLE_OBJECT_ID_COLUMN) else {
		return Ok(None);
	};
	let array = array
		.as_any()
		.downcast_ref::<FixedSizeBinaryArray>()
		.ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				"__row_table_object_id__ column has unexpected Arrow type",
			)
		})?;
	let row = record_row_index.get() as usize;
	if row >= array.len() {
		return Ok(None);
	}
	Ok(Some(TableObjectId::new(fixed_16_value(
		array,
		row,
		STORAGE_ROW_TABLE_OBJECT_ID_COLUMN,
	)?)))
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
	let tombstones = batch
		.column_by_name(STORAGE_ROW_TOMBSTONE_COLUMN)
		.and_then(|array| array.as_any().downcast_ref::<BooleanArray>())
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "missing storage row tombstone"))?;
	// ADR 045 Phase 8 — optional `__row_table_object_id__`
	// (AppendOnly only). When present, decode the per-row
	// 16-byte values; otherwise propagate `None`.
	let table_object_ids = batch
		.column_by_name(STORAGE_ROW_TABLE_OBJECT_ID_COLUMN)
		.map(|array| {
			array
				.as_any()
				.downcast_ref::<FixedSizeBinaryArray>()
				.ok_or_else(|| {
					Error::new(
						ErrorKind::InvalidData,
						"missing storage row table_object_id",
					)
				})
		})
		.transpose()?;
	let mut entries = Vec::with_capacity(batch.num_rows());
	for row in 0..batch.num_rows() {
		let record_row_index = u32::try_from(row).map_err(|_| {
			Error::new(
				ErrorKind::InvalidData,
				"multi-row storage record row index exceeds u32",
			)
		})?;
		let table_object_id = table_object_ids
			.map(|array| -> std::io::Result<TableObjectId> {
				Ok(TableObjectId::new(fixed_16_value(
					array,
					row,
					STORAGE_ROW_TABLE_OBJECT_ID_COLUMN,
				)?))
			})
			.transpose()?;
		entries.push(StorageRowMetadata {
			key_identity: Identity::new(fixed_32_value(
				identities,
				row,
				STORAGE_ROW_KEY_ID_COLUMN,
			)?),
			routing_locality: Locality::new(localities.value(row)),
			row_hlc: decode_storage_hlc(fixed_16_value(row_hlcs, row, STORAGE_ROW_HLC_COLUMN)?),
			is_tombstone: tombstones.value(row),
			record_row_index: RecordRowIndex::new(record_row_index),
			table_object_id,
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
	encode_record_batch_bytes(batch, RecordBodyCompressionPolicy::Uncompressed)
}

pub fn encode_batch_with_policy(
	batch: &RecordBatch,
	policy: RecordBodyCompressionPolicy,
) -> std::io::Result<EncodedRecordBatch> {
	encode_record_batch(batch, policy)
}

pub fn decode_batch(bytes: &[u8]) -> std::io::Result<RecordBatch> {
	let materialized;
	let bytes =
		if crate::storage::encoding::materialize::ipc_stream_contains_encoding_directory(bytes) {
			materialized =
				crate::storage::encoding::materialize::materialize_encoding_ipc_stream(bytes)?;
			materialized.as_slice()
		} else {
			bytes
		};
	let policy = record_batch_compression_policy_from_ipc(bytes).ok();
	let started = Instant::now();
	let mut reader = StreamReader::try_new(Cursor::new(bytes), None)
		.map_err(|err| Error::new(ErrorKind::InvalidData, format!("arrow stream open: {err}")))?;
	let mut batches = Vec::new();
	for next in reader.by_ref() {
		match next {
			Ok(batch) => batches.push(batch),
			Err(err) => {
				let result = Err(Error::new(
					ErrorKind::InvalidData,
					format!("arrow stream read: {err}"),
				));
				if let Some(policy) = policy {
					crate::metrics::global().observe_storage_compression_decode_error(policy);
				}
				return result;
			}
		}
	}
	let result = match batches.as_slice() {
		[] => Err(Error::new(
			ErrorKind::InvalidData,
			"arrow stream contained no record batch",
		)),
		[batch] => Ok(batch.clone()),
		[first, ..] => concat_batches(
			&first.schema(),
			&batches.iter().map(Clone::clone).collect::<Vec<_>>(),
		)
		.map_err(|err| {
			Error::new(
				ErrorKind::InvalidData,
				format!("arrow stream row-group concat: {err}"),
			)
		}),
	};
	match (&result, policy) {
		(Ok(batch), Some(policy)) => crate::metrics::global().observe_storage_compression_decode(
			policy,
			bytes.len() as u64,
			batch.get_array_memory_size() as u64,
			started.elapsed(),
		),
		(Err(_), Some(policy)) => {
			crate::metrics::global().observe_storage_compression_decode_error(policy)
		}
		_ => {}
	}
	result
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
	// ADR 045 Phase 8 — `__row_table_object_id__` is all-or-none
	// per record. Multi-row records (PK + AppendOnly) carry
	// `Some(descriptor.table_object_id)` on every row; single-
	// row records and other paths that don't stamp it carry
	// `None` on every row. A mix is a programming error.
	let some_table_object_id = rows.iter().find_map(|row| row.table_object_id);
	if let Some(stamp) = some_table_object_id {
		if !rows.iter().all(|row| row.table_object_id == Some(stamp)) {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"table_object_id stamping must be all-or-none per record \
				 with a single value (ADR 045 §5 / Phase 8)",
			));
		}
	} else if rows.iter().any(|row| row.table_object_id.is_some()) {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"table_object_id stamping must be all-or-none per record \
			 (ADR 045 §5 / Phase 8)",
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
	fields.push(Arc::new(Field::new(
		STORAGE_ROW_TOMBSTONE_COLUMN,
		DataType::Boolean,
		false,
	)));
	if some_table_object_id.is_some() {
		fields.push(Arc::new(Field::new(
			STORAGE_ROW_TABLE_OBJECT_ID_COLUMN,
			DataType::FixedSizeBinary(16),
			false,
		)));
	}

	let mut row_hlc_builder = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
	let mut row_identity_builder = FixedSizeBinaryBuilder::with_capacity(rows.len(), 32);
	let mut row_locality_builder = UInt64Builder::with_capacity(rows.len());
	let mut row_tombstone_builder = BooleanBuilder::with_capacity(rows.len());
	let mut row_table_object_id_builder =
		some_table_object_id.map(|_| FixedSizeBinaryBuilder::with_capacity(rows.len(), 16));
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
		row_tombstone_builder.append_value(row.is_tombstone);
		if let (Some(builder), Some(table_object_id)) =
			(row_table_object_id_builder.as_mut(), row.table_object_id)
		{
			builder
				.append_value(table_object_id.as_bytes())
				.map_err(|err| {
					Error::new(
						ErrorKind::InvalidData,
						format!("row table_object_id append: {err}"),
					)
				})?;
		}
	}

	let mut columns = batch.columns().to_vec();
	columns.push(Arc::new(row_hlc_builder.finish()) as ArrayRef);
	columns.push(Arc::new(row_identity_builder.finish()) as ArrayRef);
	columns.push(Arc::new(row_locality_builder.finish()) as ArrayRef);
	columns.push(Arc::new(row_tombstone_builder.finish()) as ArrayRef);
	if let Some(mut builder) = row_table_object_id_builder {
		columns.push(Arc::new(builder.finish()) as ArrayRef);
	}
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
	descriptor: Option<&TableSchemaDescriptorV1>,
	policy: RecordBodyCompressionPolicy,
	row_group_config: &RowGroupWriterConfig,
	encoding_config: &EncodingWriterConfig,
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
				let encoded = match descriptor {
					Some(descriptor) => {
						encode_multirow_from_survivors_with_row_group_and_encoding_config(
							batch,
							source_rows,
							descriptor,
							policy,
							row_group_config,
							encoding_config,
						)?
					}
					None => encode_multirow_from_survivors_with_policy(batch, source_rows, policy)?,
				};
				if encoded.plaintext_byte_len <= max_record_bytes || mid == 1 {
					best = Some((mid, encoded));
					low = mid + 1;
				} else {
					high = mid.saturating_sub(1);
				}
			}
			let (take_count, encoded) = best.expect("upper bound is at least one row");
			if encoded.plaintext_byte_len > max_record_bytes {
				return Err(Error::new(
					ErrorKind::InvalidData,
					format!(
						"single multi-row storage row exceeds max_record_bytes: row_bytes={} max_record_bytes={max_record_bytes}",
						encoded.plaintext_byte_len
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
		LocalityPolicyV1, MultiRowLayoutDescriptor, PrimaryKeyDescriptorV1,
		RecordBodyCompressionPolicy, RecordLayoutPolicy, RowIdentityPolicy,
		SchemaEvolutionPolicyV1, TableObjectId, TableSpreadLocalityDescriptorV1, TenantId,
		encode_table_write_row_count,
	};
	use arrow::array::{Int64Array, LargeStringArray};
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
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
			LocalityPolicyV1::TableSpreadProjection(TableSpreadLocalityDescriptorV1 {
				spread_factor: 1,
				prefix_bits: 1,
			}),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.unwrap();
		descriptor.record_layout_policy =
			RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::default());
		descriptor.record_body_compression_policy =
			RecordBodyCompressionPolicy::default_for_layout(&descriptor.record_layout_policy);
		descriptor.refresh_descriptor_fingerprints().unwrap();
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

	fn string_batch(payloads: Vec<&str>) -> RecordBatch {
		RecordBatch::try_new(
			Arc::new(Schema::new(vec![
				Field::new("id", DataType::Int64, false),
				Field::new("payload", DataType::LargeUtf8, false),
			])),
			vec![
				Arc::new(Int64Array::from(
					(0..payloads.len() as i64).collect::<Vec<_>>(),
				)) as ArrayRef,
				Arc::new(LargeStringArray::from(payloads)) as ArrayRef,
			],
		)
		.unwrap()
	}

	fn string_descriptor(max_record_bytes: u64) -> TableSchemaDescriptorV1 {
		let schema = Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("payload", DataType::LargeUtf8, false),
		]);
		let mut descriptor = TableSchemaDescriptorV1::new(
			TableObjectId::new([0x51; 16]),
			1,
			"analytics",
			"strings",
			&schema,
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
			LocalityPolicyV1::TableSpreadProjection(TableSpreadLocalityDescriptorV1 {
				spread_factor: 1,
				prefix_bits: 1,
			}),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.unwrap();
		descriptor.record_layout_policy = RecordLayoutPolicy::MultiRow(
			MultiRowLayoutDescriptor::new(16, 16, max_record_bytes).unwrap(),
		);
		descriptor.record_body_compression_policy =
			RecordBodyCompressionPolicy::default_for_layout(&descriptor.record_layout_policy);
		descriptor.refresh_descriptor_fingerprints().unwrap();
		descriptor
	}

	#[test]
	fn packs_records_with_synthetic_columns_and_deterministic_order() {
		let descriptor = descriptor();
		let batch = batch(vec![3, 1, 2]);
		let meta = meta(&descriptor, batch.num_rows());
		let packed = pack_record_batch_by_bucket(
			&meta,
			&descriptor,
			&batch,
			WriterNodeId::ZERO,
			meta.client_hlc,
		)
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
				is_tombstone: false,
				table_object_id: None,
			},
			MultiRowSourceRow {
				source_row_index: 0,
				key_identity: Identity::new([1; 32]),
				routing_locality: Locality::new(2),
				row_hlc: hlc(11),
				is_tombstone: false,
				table_object_id: None,
			},
		];
		let first = encode_multirow_from_survivors(&batch, &rows).unwrap();
		let second = encode_multirow_from_survivors(&batch, &rows).unwrap();
		assert_eq!(first.frame, second.frame);
		assert_eq!(first.rows, second.rows);
		assert_eq!(first.frame.len(), 1416);
		assert_eq!(
			crate::hash::blake3::digest(&first.frame),
			[
				208, 33, 90, 244, 107, 218, 119, 236, 56, 195, 36, 96, 10, 3, 130, 127, 227, 12, 2,
				152, 69, 239, 181, 212, 44, 129, 35, 80, 103, 14, 36, 222,
			]
		);
	}

	#[test]
	fn lz4_record_batch_carries_arrow_body_compression_metadata_and_roundtrips() {
		let batch = string_batch(vec!["same-value"; 512]);
		let raw = encode_batch(&batch).unwrap();
		let compressed =
			encode_batch_with_policy(&batch, RecordBodyCompressionPolicy::Lz4Frame).unwrap();

		assert_eq!(
			crate::storage::ipc::record_batch_compression_policy_from_ipc(&raw).unwrap(),
			RecordBodyCompressionPolicy::Uncompressed
		);
		assert_eq!(
			crate::storage::ipc::record_batch_compression_policy_from_ipc(&compressed.bytes)
				.unwrap(),
			RecordBodyCompressionPolicy::Lz4Frame
		);
		assert!(compressed.encoded_byte_len < compressed.plaintext_byte_len);
		assert_eq!(decode_batch(&compressed.bytes).unwrap(), batch);
	}

	#[test]
	fn multirow_max_record_bytes_uses_uncompressed_axis_under_lz4() {
		let payload = "a".repeat(32 * 1024);
		let batch = string_batch(vec![payload.as_str()]);
		let descriptor = string_descriptor(4 * 1024);
		let meta = meta(&descriptor, batch.num_rows());
		let source_row = MultiRowSourceRow {
			source_row_index: 0,
			key_identity: Identity::new([1; 32]),
			routing_locality: Locality::new(1),
			row_hlc: hlc(10),
			is_tombstone: false,
			table_object_id: None,
		};
		let encoded = encode_multirow_from_survivors_with_policy(
			&batch,
			&[source_row],
			RecordBodyCompressionPolicy::Lz4Frame,
		)
		.unwrap();
		assert!(encoded.plaintext_byte_len > 4 * 1024);
		assert!(encoded.encoded_byte_len < 4 * 1024);

		let err = pack_record_batch_by_bucket(
			&meta,
			&descriptor,
			&batch,
			WriterNodeId::ZERO,
			meta.client_hlc,
		)
		.expect_err("uncompressed axis must reject");
		assert_eq!(err.code, PbcRejectCode::PBC_REJECT_BATCH_TOO_LARGE);
		assert!(
			err.message
				.contains("single multi-row table row exceeds max_record_bytes")
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
					is_tombstone: false,
					table_object_id: None,
				},
				MultiRowSourceRow {
					source_row_index: 1,
					key_identity: Identity::new([1; 32]),
					routing_locality: Locality::new(7),
					row_hlc: hlc(10),
					is_tombstone: false,
					table_object_id: None,
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
					is_tombstone: false,
					table_object_id: None,
				},
				MultiRowSourceRow {
					source_row_index: 3,
					key_identity: Identity::new([4; 32]),
					routing_locality: Locality::new(7),
					row_hlc: hlc(40),
					is_tombstone: false,
					table_object_id: None,
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

	// ============================================================
	//  ADR 045 — APPEND-ONLY MULTIROW PACKING
	// ============================================================

	fn append_only_descriptor() -> TableSchemaDescriptorV1 {
		use arrow::datatypes::TimeUnit;
		let schema = Schema::new(vec![
			Field::new(
				"event_time",
				DataType::Timestamp(TimeUnit::Millisecond, None),
				false,
			),
			Field::new("payload", DataType::LargeBinary, true),
		]);
		TableSchemaDescriptorV1::new_with_layout(
			TableObjectId::new([0x45; 16]),
			1,
			"analytics",
			"events",
			&schema,
			RowIdentityPolicy::AppendOnly,
			LocalityPolicyV1::default_table_spread(),
			RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::default()),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.unwrap()
	}

	fn append_only_batch(payloads: Vec<Option<&[u8]>>, times: Vec<i64>) -> RecordBatch {
		use arrow::array::{LargeBinaryArray, TimestampMillisecondArray};
		use arrow::datatypes::TimeUnit;
		RecordBatch::try_new(
			Arc::new(Schema::new(vec![
				Field::new(
					"event_time",
					DataType::Timestamp(TimeUnit::Millisecond, None),
					false,
				),
				Field::new("payload", DataType::LargeBinary, true),
			])),
			vec![
				Arc::new(TimestampMillisecondArray::from(times)) as ArrayRef,
				Arc::new(LargeBinaryArray::from(payloads)) as ArrayRef,
			],
		)
		.unwrap()
	}

	#[test]
	fn append_only_packs_into_multirow_with_synthetic_columns() {
		let descriptor = append_only_descriptor();
		let batch = append_only_batch(
			vec![Some(b"a"), Some(b"b"), Some(b"a")],
			vec![10_000, 20_000, 30_000],
		);
		let meta = meta(&descriptor, batch.num_rows());
		let packed = pack_record_batch_by_bucket(
			&meta,
			&descriptor,
			&batch,
			WriterNodeId::ZERO,
			meta.client_hlc,
		)
		.expect("append-only multi-row pack");
		assert!(!packed.is_empty());
		let mut logical_rows = 0;
		let mut all_identities = Vec::new();
		for record in &packed {
			let decoded = decode_batch(&record.frame_bytes).unwrap();
			assert!(has_storage_synthetic_columns(&decoded));
			let rows = storage_rows_from_batch(&decoded).unwrap();
			for row in &rows {
				all_identities.push(row.key_identity);
			}
			logical_rows += rows.len();
		}
		assert_eq!(logical_rows, 3);
		// Identical-payload rows (rows 0 and 2 share payload "a") still
		// must produce distinct identities under ADR 045 §4.
		all_identities.sort();
		all_identities.dedup();
		assert_eq!(
			all_identities.len(),
			3,
			"distinct intra-batch positions must yield distinct identities"
		);
	}

	#[test]
	fn append_only_user_decoded_strips_synthetic_columns() {
		let descriptor = append_only_descriptor();
		let batch = append_only_batch(vec![Some(b"x")], vec![10_000]);
		let meta = meta(&descriptor, batch.num_rows());
		let packed = pack_record_batch_by_bucket(
			&meta,
			&descriptor,
			&batch,
			WriterNodeId::ZERO,
			meta.client_hlc,
		)
		.unwrap();
		let decoded = decode_batch(&packed[0].frame_bytes).unwrap();
		let user = strip_storage_synthetic_columns(&decoded).unwrap();
		assert_eq!(user.schema().as_ref(), batch.schema().as_ref());
	}

	#[test]
	fn append_only_data_rows_decode_with_false_row_tombstone() {
		let descriptor = append_only_descriptor();
		let batch = append_only_batch(vec![Some(b"x"), Some(b"y")], vec![10_000, 20_000]);
		let meta = meta(&descriptor, batch.num_rows());
		let packed = pack_record_batch_by_bucket(
			&meta,
			&descriptor,
			&batch,
			WriterNodeId::ZERO,
			meta.client_hlc,
		)
		.unwrap();
		let decoded = decode_batch(&packed[0].frame_bytes).unwrap();
		let rows = storage_rows_from_batch(&decoded).unwrap();
		assert!(rows.iter().all(|row| !row.is_tombstone));
		for row in 0..rows.len() {
			assert!(
				!storage_row_is_tombstone_at(&decoded, RecordRowIndex::new(row as u32)).unwrap()
			);
		}
	}

	#[test]
	fn tombstone_only_multirow_records_decode_without_user_rows() {
		let layout = MultiRowLayoutDescriptor::new(4, 4, 1024 * 1024).unwrap();
		let table_object_id = TableObjectId::new([0x47; 16]);
		let rows = vec![
			MultiRowSourceRow {
				source_row_index: 0,
				key_identity: Identity::new([1; 32]),
				routing_locality: Locality::new(9),
				row_hlc: hlc(90),
				is_tombstone: true,
				table_object_id: Some(table_object_id),
			},
			MultiRowSourceRow {
				source_row_index: 1,
				key_identity: Identity::new([2; 32]),
				routing_locality: Locality::new(9),
				row_hlc: hlc(90),
				is_tombstone: true,
				table_object_id: Some(table_object_id),
			},
		];
		let packed = pack_tombstone_multirow_source_rows_with_policy(
			layout,
			&rows,
			RecordBodyCompressionPolicy::Uncompressed,
		)
		.unwrap();
		assert_eq!(packed.len(), 1);
		let decoded = decode_batch(&packed[0].encoded.frame).unwrap();
		assert!(has_storage_synthetic_columns(&decoded));
		assert_eq!(
			strip_storage_synthetic_columns(&decoded)
				.unwrap()
				.num_columns(),
			0
		);
		let decoded_rows = storage_rows_from_batch(&decoded).unwrap();
		assert_eq!(decoded_rows.len(), 2);
		assert!(decoded_rows.iter().all(|row| row.is_tombstone));
		let materialized =
			materialize_record_location_batch(&decoded, RecordRowIndex::ZERO).unwrap();
		assert_eq!(materialized.num_columns(), 0);
		assert_eq!(materialized.num_rows(), 0);
	}
}
