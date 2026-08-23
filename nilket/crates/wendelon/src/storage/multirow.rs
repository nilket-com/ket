use crate::schema::PartitionId;
use crate::schema::table::{
	MultiRowLayoutDescriptor, STORAGE_ROW_HLC_COLUMN, STORAGE_ROW_KEY_ID_COLUMN,
	STORAGE_ROW_ROUTING_LOCALITY_COLUMN, STORAGE_ROW_TABLE_OBJECT_ID_COLUMN,
	STORAGE_ROW_TOMBSTONE_COLUMN, TableObjectId, TableSchemaDescriptor, TableWriteAdmission,
	TableWriteRequestMeta, validate_table_write_request_batch,
};
use crate::schema::table::{
	PlacementSemantics, STORAGE_ROW_PARTITION_COLUMN, partition_for_locality,
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
use crate::storage::encoding::{
	ENCODING_DIRECTORY_METADATA_KEY, EncodingWriterConfig, L2ChunkLayoutConfig,
	choose_chunk_size_bytes,
};
use crate::storage::ipc::{
	EncodedRecordBatch, encode_record_batch, encode_record_batch_bytes,
	encode_record_batches_bytes, record_batch_compression_policy_from_ipc,
};
use crate::storage::row_group::{
	ROW_GROUP_DIRECTORY_METADATA_KEY, RowGroupSizingProfile, RowGroupWriterConfig,
	build_page_directory_for_ipc_stream, serialize_page_directory_value,
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
	/// ADR 0090: the partition every row of this record was written under;
	/// `None` is a raw-locality (legacy) record.
	pub partition: Option<PartitionId>,
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
	/// ADR 0090: the partition this row was written under, derived from its
	/// routing locality at the descriptor-aware write boundary. `None` writes a
	/// raw-locality (legacy) row.
	pub partition: Option<PartitionId>,
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
	/// ADR 0090: read from the frame's per-row `__row_partition__` entry;
	/// `None` is a raw-locality row.
	pub partition: Option<PartitionId>,
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
			partition: row.partition,
		}
	}
}

pub fn pack_record_batch_by_bucket(
	meta: &TableWriteRequestMeta,
	descriptor: &TableSchemaDescriptor,
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
	descriptor: &TableSchemaDescriptor,
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
	descriptor: &TableSchemaDescriptor,
	batch: &RecordBatch,
	admission: &TableWriteAdmission,
	row_hlc: HybridTimestamp,
	row_group_config: &RowGroupWriterConfig,
) -> Result<Vec<PackedMultiRowRecord>, TableWriteValidationError> {
	pack_admitted_record_batch_by_bucket_with_row_group_encoding_and_chunk_layout_config(
		meta,
		descriptor,
		batch,
		admission,
		row_hlc,
		row_group_config,
		&EncodingWriterConfig::default(),
		&L2ChunkLayoutConfig::default(),
	)
}

pub fn pack_admitted_record_batch_by_bucket_with_row_group_and_encoding_config(
	meta: &TableWriteRequestMeta,
	descriptor: &TableSchemaDescriptor,
	batch: &RecordBatch,
	admission: &TableWriteAdmission,
	row_hlc: HybridTimestamp,
	row_group_config: &RowGroupWriterConfig,
	encoding_config: &EncodingWriterConfig,
) -> Result<Vec<PackedMultiRowRecord>, TableWriteValidationError> {
	pack_admitted_record_batch_by_bucket_with_row_group_encoding_and_chunk_layout_config(
		meta,
		descriptor,
		batch,
		admission,
		row_hlc,
		row_group_config,
		encoding_config,
		&L2ChunkLayoutConfig::default(),
	)
}

pub fn pack_admitted_record_batch_by_bucket_with_row_group_encoding_and_chunk_layout_config(
	meta: &TableWriteRequestMeta,
	descriptor: &TableSchemaDescriptor,
	batch: &RecordBatch,
	admission: &TableWriteAdmission,
	row_hlc: HybridTimestamp,
	row_group_config: &RowGroupWriterConfig,
	encoding_config: &EncodingWriterConfig,
	chunk_layout_config: &L2ChunkLayoutConfig,
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
	// ADR 0089: packing groups on the DURABLE partition — the id derived once at
	// this boundary and carried on the row — never on an independently
	// recomputed prefix. Rows sharing a partition share one owner in every
	// epoch (ADR 0090), so a shared record never spans owners. A row without a
	// partition keeps its full locality as the bucket key: legacy rows stay one
	// record per locality, exactly as before this record.
	#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
	enum PackBucket {
		Partition(PartitionId),
		RawLocality(Locality),
	}
	let mut buckets: BTreeMap<PackBucket, Vec<MultiRowSourceRow>> = BTreeMap::new();
	for (row, locality) in admission.localities.iter().copied().enumerate() {
		let partition =
			write_partition_for_row(descriptor, locality).map_err(table_pack_io_error)?;
		let bucket = match partition {
			Some(partition) => PackBucket::Partition(partition),
			None => PackBucket::RawLocality(locality),
		};
		buckets.entry(bucket).or_default().push(MultiRowSourceRow {
			source_row_index: row,
			key_identity: admission.key_identities[row],
			routing_locality: locality,
			row_hlc,
			is_tombstone: false,
			partition,
			table_object_id: stamped_table_object_id,
		});
	}

	let target_rows = layout.target_rows.get().max(1) as usize;
	let max_rows = layout.max_rows.get().max(1) as usize;
	let max_record_bytes = layout.max_record_bytes.get();
	let mut packed = Vec::new();
	for (bucket, mut rows) in buckets {
		// ADR 0089: a partition-grouped record's stored locality is its prefix;
		// each row keeps its own full routing locality in the frame and in its
		// index entry, so interval membership stays row-exact.
		let locality = match bucket {
			PackBucket::Partition(partition) => Locality::new(partition.get()),
			PackBucket::RawLocality(locality) => locality,
		};
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
				encoded =
					encode_multirow_from_survivors_with_row_group_encoding_and_chunk_layout_config(
						batch,
						&rows[cursor..cursor + upper],
						descriptor,
						descriptor.record_body_compression_policy,
						row_group_config,
						encoding_config,
						chunk_layout_config,
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
						encode_multirow_from_survivors_with_row_group_encoding_and_chunk_layout_config(
							batch,
							&rows[cursor..cursor + mid],
							descriptor,
							descriptor.record_body_compression_policy,
							row_group_config,
							encoding_config,
							chunk_layout_config,
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
				// ADR 0090 decision 3: a record never spans a partition. Every row of a
				// bucket shares one locality today, so one partition; assert the
				// invariant rather than assume it survives future bucketing.
				partition: {
					let partition = rows[cursor].partition;
					if rows[cursor..cursor + take_count]
						.iter()
						.any(|row| row.partition != partition)
					{
						return Err(table_pack_error("a packed record must not span partitions"));
					}
					partition
				},
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
		&L2ChunkLayoutConfig::default(),
	)
}

pub fn encode_multirow_from_survivors_with_row_group_config(
	batch: &RecordBatch,
	survivors: &[MultiRowSourceRow],
	descriptor: &TableSchemaDescriptor,
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
		&L2ChunkLayoutConfig::default(),
	)
}

pub fn encode_multirow_from_survivors_with_row_group_and_encoding_config(
	batch: &RecordBatch,
	survivors: &[MultiRowSourceRow],
	descriptor: &TableSchemaDescriptor,
	policy: RecordBodyCompressionPolicy,
	row_group_config: &RowGroupWriterConfig,
	encoding_config: &EncodingWriterConfig,
) -> std::io::Result<EncodedMultiRowFrame> {
	encode_multirow_from_survivors_with_row_group_encoding_and_chunk_layout_config(
		batch,
		survivors,
		descriptor,
		policy,
		row_group_config,
		encoding_config,
		&L2ChunkLayoutConfig::default(),
	)
}

pub fn encode_multirow_from_survivors_with_row_group_encoding_and_chunk_layout_config(
	batch: &RecordBatch,
	survivors: &[MultiRowSourceRow],
	descriptor: &TableSchemaDescriptor,
	policy: RecordBodyCompressionPolicy,
	row_group_config: &RowGroupWriterConfig,
	encoding_config: &EncodingWriterConfig,
	chunk_layout_config: &L2ChunkLayoutConfig,
) -> std::io::Result<EncodedMultiRowFrame> {
	encode_multirow_from_survivors_with_optional_row_groups(
		batch,
		survivors,
		policy,
		Some(descriptor),
		row_group_config,
		encoding_config,
		chunk_layout_config,
	)
}

fn encode_multirow_from_survivors_with_optional_row_groups(
	batch: &RecordBatch,
	survivors: &[MultiRowSourceRow],
	policy: RecordBodyCompressionPolicy,
	descriptor: Option<&TableSchemaDescriptor>,
	row_group_config: &RowGroupWriterConfig,
	encoding_config: &EncodingWriterConfig,
	chunk_layout_config: &L2ChunkLayoutConfig,
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
		chunk_layout_config,
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
	descriptor: Option<&TableSchemaDescriptor>,
	policy: RecordBodyCompressionPolicy,
	row_group_config: &RowGroupWriterConfig,
	encoding_config: &EncodingWriterConfig,
	chunk_layout_config: &L2ChunkLayoutConfig,
) -> std::io::Result<EncodedRecordBatch> {
	let Some(descriptor) = descriptor else {
		return encode_record_batch(storage_batch, policy);
	};
	row_group_config.validate()?;
	if !row_group_config.enabled
		|| (storage_batch.num_rows() as u64) < row_group_config.min_paged_record_rows().get()
	{
		return encode_record_batch(storage_batch, policy);
	}
	let single = if row_group_config.profile == RowGroupSizingProfile::Scan {
		if estimate_record_batch_data_bytes(storage_batch)
			< row_group_config.min_paged_record_bytes().get()
		{
			return encode_record_batch(storage_batch, policy);
		}
		None
	} else {
		let single = encode_record_batch(storage_batch, policy)?;
		if single.plaintext_byte_len < row_group_config.min_paged_record_bytes().get() {
			return Ok(single);
		}
		Some(single)
	};
	let batches = split_row_group_batches(storage_batch, row_group_config)?;
	if batches.len() <= 1 && row_group_config.profile != RowGroupSizingProfile::Scan {
		return Ok(single.expect("PointRead preflight encode must be present"));
	}
	encode_row_group_batches_with_directory(
		&batches,
		descriptor,
		policy,
		row_group_config,
		encoding_config,
		chunk_layout_config,
	)
}

fn estimate_record_batch_data_bytes(storage_batch: &RecordBatch) -> u64 {
	storage_batch
		.columns()
		.iter()
		.map(|column| column.get_array_memory_size() as u64)
		.sum()
}

fn split_row_group_batches(
	storage_batch: &RecordBatch,
	row_group_config: &RowGroupWriterConfig,
) -> std::io::Result<Vec<RecordBatch>> {
	let target = row_group_config.target_row_group_rows().get() as usize;
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
	descriptor: &TableSchemaDescriptor,
	policy: RecordBodyCompressionPolicy,
	row_group_config: &RowGroupWriterConfig,
	encoding_config: &EncodingWriterConfig,
	chunk_layout_config: &L2ChunkLayoutConfig,
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
					row_group_config,
				)?;
				match encoding_directory {
					Some(encoding_directory) => {
						let chunk_size_bytes =
							choose_chunk_size_bytes(Some(&encoding_directory), chunk_layout_config);
						let compacted = compact_encoding_ipc_stream(
							&bytes,
							&directory,
							&encoding_directory,
							&applied,
							chunk_size_bytes,
						)?;
						(
							compacted.bytes,
							compacted.page_directory,
							Some(compacted.encoding_directory),
						)
					}
					None => (bytes, directory, None),
				}
			} else {
				(bytes, directory, None)
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
	descriptor: Option<&TableSchemaDescriptor>,
	policy: RecordBodyCompressionPolicy,
	row_group_config: &RowGroupWriterConfig,
) -> std::io::Result<Vec<PackedMultiRowStorageFrame>> {
	pack_multirow_source_rows_with_policy_row_group_encoding_and_chunk_layout_config(
		layout,
		batch,
		rows,
		descriptor,
		policy,
		row_group_config,
		&EncodingWriterConfig::default(),
		&L2ChunkLayoutConfig::default(),
	)
}

pub fn pack_multirow_source_rows_with_policy_and_row_group_and_encoding_config(
	layout: MultiRowLayoutDescriptor,
	batch: &RecordBatch,
	rows: &[MultiRowSourceRow],
	descriptor: Option<&TableSchemaDescriptor>,
	policy: RecordBodyCompressionPolicy,
	row_group_config: &RowGroupWriterConfig,
	encoding_config: &EncodingWriterConfig,
) -> std::io::Result<Vec<PackedMultiRowStorageFrame>> {
	pack_multirow_source_rows_with_policy_row_group_encoding_and_chunk_layout_config(
		layout,
		batch,
		rows,
		descriptor,
		policy,
		row_group_config,
		encoding_config,
		&L2ChunkLayoutConfig::default(),
	)
}

pub fn pack_multirow_source_rows_with_policy_row_group_encoding_and_chunk_layout_config(
	layout: MultiRowLayoutDescriptor,
	batch: &RecordBatch,
	rows: &[MultiRowSourceRow],
	descriptor: Option<&TableSchemaDescriptor>,
	policy: RecordBodyCompressionPolicy,
	row_group_config: &RowGroupWriterConfig,
	encoding_config: &EncodingWriterConfig,
	chunk_layout_config: &L2ChunkLayoutConfig,
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
		chunk_layout_config,
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
			partition: meta.partition,
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
		&L2ChunkLayoutConfig::default(),
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
				// Secondary-index segment rows stay raw-locality: the V1 segment
				// format carries no placement.
				partition: None,
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
					// ADR 0090: per-row partition is storage-only.
					| STORAGE_ROW_PARTITION_COLUMN
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
	// ADR 0090: optional per-row partition; a null entry is a raw-locality row.
	let partitions = batch
		.column_by_name(STORAGE_ROW_PARTITION_COLUMN)
		.map(|array| {
			array.as_any().downcast_ref::<UInt64Array>().ok_or_else(|| {
				Error::new(
					ErrorKind::InvalidData,
					"__row_partition__ column has unexpected Arrow type",
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
			partition: partitions
				.filter(|array| !array.is_null(row))
				.map(|array| PartitionId::new(array.value(row))),
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
	let any_partition = rows.iter().any(|row| row.partition.is_some());
	if any_partition {
		// ADR 0090: nullable, because presence is per ROW — a null entry is a
		// raw-locality row inside a record whose schema carries the column, and
		// partition zero stays a legitimate partition. Omitted entirely when no
		// row is partitioned, so legacy frames are byte-identical to before.
		fields.push(Arc::new(Field::new(
			STORAGE_ROW_PARTITION_COLUMN,
			DataType::UInt64,
			true,
		)));
	}

	let mut row_hlc_builder = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
	let mut row_identity_builder = FixedSizeBinaryBuilder::with_capacity(rows.len(), 32);
	let mut row_locality_builder = UInt64Builder::with_capacity(rows.len());
	let mut row_tombstone_builder = BooleanBuilder::with_capacity(rows.len());
	let mut row_table_object_id_builder =
		some_table_object_id.map(|_| FixedSizeBinaryBuilder::with_capacity(rows.len(), 16));
	let mut row_partition_builder = any_partition.then(|| UInt64Builder::with_capacity(rows.len()));
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
	if let Some(builder) = row_partition_builder.as_mut() {
		for row in rows {
			match row.partition {
				Some(partition) => builder.append_value(partition.get()),
				None => builder.append_null(),
			}
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
	if let Some(mut builder) = row_partition_builder {
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
	descriptor: Option<&TableSchemaDescriptor>,
	policy: RecordBodyCompressionPolicy,
	row_group_config: &RowGroupWriterConfig,
	encoding_config: &EncodingWriterConfig,
	chunk_layout_config: &L2ChunkLayoutConfig,
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
						encode_multirow_from_survivors_with_row_group_encoding_and_chunk_layout_config(
							batch,
							source_rows,
							descriptor,
							policy,
							row_group_config,
							encoding_config,
							chunk_layout_config,
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
		LocalityPolicy, MultiRowLayoutDescriptor, PrimaryKeyDescriptor,
		RecordBodyCompressionPolicy, RecordLayoutPolicy, RowIdentityPolicy, SchemaEvolutionPolicy,
		TableObjectId, TableSpreadLocalityDescriptor, TenantId, encode_table_write_row_count,
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

	fn descriptor() -> TableSchemaDescriptor {
		let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
		let mut descriptor = TableSchemaDescriptor::new(
			TableObjectId::new([0x42; 16]),
			1,
			"analytics",
			"events",
			&schema,
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
			LocalityPolicy::TableSpreadProjection(TableSpreadLocalityDescriptor {
				spread_factor: 1,
				prefix_bits: 1,
			}),
			SchemaEvolutionPolicy::AdditiveOnly,
		)
		.unwrap();
		descriptor.record_layout_policy =
			RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::default());
		descriptor.record_body_compression_policy =
			RecordBodyCompressionPolicy::default_for_layout(&descriptor.record_layout_policy);
		descriptor.refresh_descriptor_fingerprints().unwrap();
		descriptor
	}

	fn meta(descriptor: &TableSchemaDescriptor, rows: usize) -> TableWriteRequestMeta {
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

	fn string_descriptor(max_record_bytes: u64) -> TableSchemaDescriptor {
		let schema = Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("payload", DataType::LargeUtf8, false),
		]);
		let mut descriptor = TableSchemaDescriptor::new(
			TableObjectId::new([0x51; 16]),
			1,
			"analytics",
			"strings",
			&schema,
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
			LocalityPolicy::TableSpreadProjection(TableSpreadLocalityDescriptor {
				spread_factor: 1,
				prefix_bits: 1,
			}),
			SchemaEvolutionPolicy::AdditiveOnly,
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
				partition: None,
			},
			MultiRowSourceRow {
				source_row_index: 0,
				key_identity: Identity::new([1; 32]),
				routing_locality: Locality::new(2),
				row_hlc: hlc(11),
				is_tombstone: false,
				table_object_id: None,
				partition: None,
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
			partition: None,
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
					partition: None,
				},
				MultiRowSourceRow {
					source_row_index: 1,
					key_identity: Identity::new([1; 32]),
					routing_locality: Locality::new(7),
					row_hlc: hlc(10),
					is_tombstone: false,
					table_object_id: None,
					partition: None,
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
					partition: None,
				},
				MultiRowSourceRow {
					source_row_index: 3,
					key_identity: Identity::new([4; 32]),
					routing_locality: Locality::new(7),
					row_hlc: hlc(40),
					is_tombstone: false,
					table_object_id: None,
					partition: None,
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

	fn append_only_descriptor() -> TableSchemaDescriptor {
		use arrow::datatypes::TimeUnit;
		let schema = Schema::new(vec![
			Field::new(
				"event_time",
				DataType::Timestamp(TimeUnit::Millisecond, None),
				false,
			),
			Field::new("payload", DataType::LargeBinary, true),
		]);
		TableSchemaDescriptor::new_with_layout(
			TableObjectId::new([0x45; 16]),
			1,
			"analytics",
			"events",
			&schema,
			RowIdentityPolicy::AppendOnly,
			LocalityPolicy::default_table_spread(),
			RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::default()),
			SchemaEvolutionPolicy::AdditiveOnly,
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
				partition: None,
			},
			MultiRowSourceRow {
				source_row_index: 1,
				key_identity: Identity::new([2; 32]),
				routing_locality: Locality::new(9),
				row_hlc: hlc(90),
				is_tombstone: true,
				table_object_id: Some(table_object_id),
				partition: None,
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

/// ADR 0090: the placement a record frame declares **for one row**.
///
/// Arrow IPC is self-describing, so the frame is durable evidence independent
/// of the persistent index. Presence is per row, not per record: the column
/// holds a value at the rows written under partitioned placement and a null at
/// rows that were not, so partition zero stays a legitimate partition and the
/// requested row — never a neighbour — decides how it routes.
pub fn storage_frame_placement(
	batch: &RecordBatch,
	record_row_index: RecordRowIndex,
) -> std::io::Result<(PlacementSemantics, Option<PartitionId>)> {
	let Some(array) = batch.column_by_name(STORAGE_ROW_PARTITION_COLUMN) else {
		return Ok((PlacementSemantics::RawLocality, None));
	};
	let array = array
		.as_any()
		.downcast_ref::<UInt64Array>()
		.ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				"__row_partition__ column has unexpected Arrow type",
			)
		})?;
	let row = record_row_index.get() as usize;
	if row >= array.len() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"__row_partition__ has {} rows, which does not cover record row {row}",
				array.len()
			),
		));
	}
	if array.is_null(row) {
		return Ok((PlacementSemantics::RawLocality, None));
	}
	Ok((
		PlacementSemantics::Partitioned,
		Some(PartitionId::new(array.value(row))),
	))
}

/// ADR 0090: the one placement resolver. Every read path that materializes a
/// row from a stored record resolves placement here, never from the index
/// alone.
///
/// Length dispatch alone cannot tell a genuine V4 index value from a V5 value
/// truncated back to thirty-six bytes — both read as legacy. The frame is
/// independent durable evidence, so the two carriers must agree, and the
/// agreement is evaluated at the requested `record_row_index`: a neighbouring
/// row's placement never governs.
pub fn resolve_record_placement(
	index_placement: PlacementSemantics,
	index_partition: Option<PartitionId>,
	batch: &RecordBatch,
	record_row_index: RecordRowIndex,
) -> std::io::Result<(PlacementSemantics, Option<PartitionId>)> {
	let (frame_placement, frame_partition) = storage_frame_placement(batch, record_row_index)?;
	match (
		(index_placement, index_partition),
		(frame_placement, frame_partition),
	) {
		((PlacementSemantics::RawLocality, _), (PlacementSemantics::RawLocality, _)) => {
			Ok((PlacementSemantics::RawLocality, None))
		}
		(
			(PlacementSemantics::Partitioned, Some(index_partition)),
			(PlacementSemantics::Partitioned, Some(frame_partition)),
		) if index_partition == frame_partition => {
			Ok((PlacementSemantics::Partitioned, Some(frame_partition)))
		}
		((PlacementSemantics::Partitioned, _), (PlacementSemantics::Partitioned, _)) => {
			Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"record placement disagrees at row {}: index partition \
					 {index_partition:?} but frame partition {frame_partition:?}",
					record_row_index.get()
				),
			))
		}
		((PlacementSemantics::RawLocality, _), (PlacementSemantics::Partitioned, _)) => {
			Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"record frame declares partition {frame_partition:?} at row {} but its \
					 index entry reads as raw-locality; the index value is truncated or \
					 damaged, not an older record",
					record_row_index.get()
				),
			))
		}
		((PlacementSemantics::Partitioned, _), (PlacementSemantics::RawLocality, _)) => {
			Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"index entry declares partition {index_partition:?} but the record \
					 frame carries none at row {}",
					record_row_index.get()
				),
			))
		}
	}
}

/// ADR 0089 point-lookup gate. Packing groups rows that today land in separate
/// records, so resolving a key inside a packed record must return that row and
/// never a neighbour. These build packed records directly through the survivor
/// encoder — the same entry compaction uses — because the bucket path cannot
/// currently produce a record holding more than one row.
#[cfg(test)]
mod adr0089_point_lookup {
	use super::*;
	use arrow::array::Int64Array;

	fn hlc(ms: u64) -> HybridTimestamp {
		HybridTimestamp {
			physical_ms: ms,
			logical: 0,
			node_id: 1,
		}
	}

	fn batch(values: Vec<i64>) -> RecordBatch {
		RecordBatch::try_new(
			Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
			vec![Arc::new(Int64Array::from(values))],
		)
		.unwrap()
	}

	fn identity(id: i64) -> Identity {
		let mut bytes = [0u8; 32];
		bytes[..8].copy_from_slice(&(id as u64).to_be_bytes());
		Identity::new(bytes)
	}

	/// Rows whose localities share a prefix and differ only in the per-row
	/// suffix: exactly the neighbours that packing will place in one record.
	fn neighbouring_rows(ids: &[i64], prefix: u64) -> Vec<MultiRowSourceRow> {
		ids.iter()
			.enumerate()
			.map(|(ordinal, id)| MultiRowSourceRow {
				source_row_index: ordinal,
				key_identity: identity(*id),
				routing_locality: Locality::new(prefix | (*id as u64)),
				row_hlc: hlc(10 + ordinal as u64),
				is_tombstone: false,
				table_object_id: None,
				partition: None,
			})
			.collect()
	}

	fn user_value(record: &RecordBatch, index: RecordRowIndex) -> i64 {
		let row = materialize_record_location_batch(record, index).expect("materialize row");
		let user = strip_storage_synthetic_columns(&row).expect("strip synthetic columns");
		assert_eq!(user.num_rows(), 1, "a located row is exactly one row");
		user.column(0)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("id is Int64")
			.value(0)
	}

	/// Resolve every key through the record's own per-row identity and assert the
	/// row carries that key's value. A lookup that drifted to an adjacent row
	/// would return a neighbour's value and fail here.
	fn assert_every_key_resolves_exactly(record: &RecordBatch, expected: &[i64]) {
		let rows = storage_rows_from_batch(record).expect("storage rows");
		assert_eq!(rows.len(), expected.len(), "record holds every packed row");
		for id in expected {
			let entry = rows
				.iter()
				.find(|row| row.key_identity == identity(*id))
				.unwrap_or_else(|| panic!("key {id} is resolvable inside the record"));
			assert_eq!(
				user_value(record, entry.record_row_index),
				*id,
				"key {id} must resolve to its own row, not a neighbour"
			);
		}
	}

	#[test]
	fn adr0089_neighbouring_keys_sharing_a_prefix_resolve_to_their_own_rows() {
		let ids = vec![11_i64, 12, 13, 14];
		let source = neighbouring_rows(&ids, 0xDEAD_0000_0000_0000);
		let encoded = encode_multirow_from_survivors(&batch(ids.clone()), &source)
			.expect("encode packed record");
		let record = decode_batch(&encoded.frame).expect("decode packed record");

		// Premise: this really is one record holding several rows. Without it the
		// resolution assertions below would be trivially satisfied.
		assert_eq!(
			record.num_rows(),
			ids.len(),
			"the gate requires a genuinely packed record"
		);
		let localities: std::collections::BTreeSet<_> =
			source.iter().map(|row| row.routing_locality).collect();
		assert_eq!(
			localities.len(),
			ids.len(),
			"the packed rows carry distinct localities sharing one prefix"
		);

		assert_every_key_resolves_exactly(&record, &ids);
	}

	#[test]
	fn adr0089_adjacent_row_index_returns_a_different_row() {
		// The discriminator for the assertions above: if every row decoded the
		// same, resolving to a neighbour would be undetectable.
		let ids = vec![21_i64, 22, 23];
		let source = neighbouring_rows(&ids, 0xBEEF_0000_0000_0000);
		let encoded =
			encode_multirow_from_survivors(&batch(ids.clone()), &source).expect("encode packed");
		let record = decode_batch(&encoded.frame).expect("decode packed");
		let first = user_value(&record, RecordRowIndex::ZERO);
		let second = user_value(&record, RecordRowIndex::new(1));
		assert_ne!(
			first, second,
			"adjacent rows must be distinguishable or the gate proves nothing"
		);
	}

	#[test]
	fn adr0089_mixed_packed_and_unpacked_records_each_resolve_exactly() {
		let packed_ids = vec![31_i64, 32, 33];
		let packed_source = neighbouring_rows(&packed_ids, 0xFEED_0000_0000_0000);
		let packed = decode_batch(
			&encode_multirow_from_survivors(&batch(packed_ids.clone()), &packed_source)
				.expect("encode packed")
				.frame,
		)
		.expect("decode packed");

		let lone_ids = vec![34_i64];
		let lone_source = neighbouring_rows(&lone_ids, 0xFEED_0000_0000_0000);
		let lone = decode_batch(
			&encode_multirow_from_survivors(&batch(lone_ids.clone()), &lone_source)
				.expect("encode single")
				.frame,
		)
		.expect("decode single");

		assert_eq!(packed.num_rows(), 3, "packed side holds several rows");
		assert_eq!(lone.num_rows(), 1, "unpacked side holds exactly one row");

		// Both shapes coexist permanently during and after migration, so each must
		// resolve on its own terms rather than by assuming a uniform layout.
		assert_every_key_resolves_exactly(&packed, &packed_ids);
		assert_every_key_resolves_exactly(&lone, &lone_ids);
	}

	#[test]
	fn adr0089_compaction_produced_records_resolve_exactly() {
		// Compaction encodes survivors through this entry point, so a record it
		// produces must resolve identically to a write-path record.
		let ids = vec![41_i64, 42, 43, 44, 45];
		let source = neighbouring_rows(&ids, 0xC0DE_0000_0000_0000);
		let encoded = encode_multirow_from_survivors_with_policy(
			&batch(ids.clone()),
			&source,
			RecordBodyCompressionPolicy::default(),
		)
		.expect("encode compaction record");
		let record = decode_batch(&encoded.frame).expect("decode compaction record");
		assert_eq!(
			record.num_rows(),
			ids.len(),
			"compaction packs every survivor"
		);
		assert_eq!(
			encoded.rows.len(),
			ids.len(),
			"the frame's row metadata covers every packed row"
		);
		assert_every_key_resolves_exactly(&record, &ids);
	}
}

/// ADR 0090 decision 7: a V5 index value truncated back to a V4 length is
/// indistinguishable from a genuine V4 value by length alone. The record frame
/// is independent durable evidence, evaluated at the requested row, and
/// disagreement is refused.
#[cfg(test)]
mod adr0090_placement_crosscheck {
	use super::*;
	use arrow::array::{Int64Array, UInt64Array};
	use arrow::datatypes::{DataType, Field, Schema};

	/// A two-row frame whose per-row partition entries are given explicitly.
	/// `None` is a null entry — that row was written under raw-locality — and
	/// `Some(0)` is partition zero, a legitimate partition. Presence, not value,
	/// is the discriminator.
	fn frame_with_row_partitions(entries: Option<Vec<Option<u64>>>) -> RecordBatch {
		let mut fields = vec![Field::new("id", DataType::Int64, false)];
		let mut columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![1_i64, 2]))];
		if let Some(entries) = entries {
			fields.push(Field::new(
				STORAGE_ROW_PARTITION_COLUMN,
				DataType::UInt64,
				true,
			));
			columns.push(Arc::new(UInt64Array::from(entries)));
		}
		RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
	}

	#[test]
	fn adr0090_frame_placement_is_per_row_presence() {
		let legacy = frame_with_row_partitions(None);
		assert_eq!(
			storage_frame_placement(&legacy, RecordRowIndex::ZERO).unwrap(),
			(PlacementSemantics::RawLocality, None)
		);
		// Partition zero is present, not absent.
		let zero = frame_with_row_partitions(Some(vec![Some(0), None]));
		assert_eq!(
			storage_frame_placement(&zero, RecordRowIndex::ZERO).unwrap(),
			(PlacementSemantics::Partitioned, Some(PartitionId::new(0)))
		);
		// A null entry is raw-locality even though the record carries the column.
		assert_eq!(
			storage_frame_placement(&zero, RecordRowIndex::new(1)).unwrap(),
			(PlacementSemantics::RawLocality, None)
		);
	}

	/// The control ADR 0090 decision 7 was missing. A partitioned frame row whose
	/// index value reads as raw-locality — exactly what a V5 value truncated to
	/// thirty-six bytes produces — is corruption and must be refused. Length
	/// dispatch alone cannot see this, which is why the frame is consulted.
	#[test]
	fn adr0090_a_partitioned_frame_row_with_a_legacy_reading_index_is_refused() {
		let frame = frame_with_row_partitions(Some(vec![
			Some(0x00AB_0000_0000_0000),
			Some(0x00AB_0000_0000_0000),
		]));
		let err = resolve_record_placement(
			// What a truncated V5 index value decodes to: legacy by length.
			PlacementSemantics::RawLocality,
			None,
			&frame,
			RecordRowIndex::ZERO,
		)
		.expect_err("a truncated V5 index value must be refused, not read as legacy");
		assert!(
			err.to_string().contains("truncated or damaged"),
			"refusal must name truncation as the cause, got: {err}"
		);
	}

	#[test]
	fn adr0090_a_genuine_legacy_record_still_reads() {
		// Both carriers agree there is no partition: an older record, not damage.
		let resolved = resolve_record_placement(
			PlacementSemantics::RawLocality,
			None,
			&frame_with_row_partitions(None),
			RecordRowIndex::ZERO,
		)
		.expect("a genuine legacy record must still read");
		assert_eq!(resolved, (PlacementSemantics::RawLocality, None));
	}

	#[test]
	fn adr0090_a_partitioned_record_whose_carriers_agree_reads() {
		let partition = PartitionId::new(0x00AB_0000_0000_0000);
		let resolved = resolve_record_placement(
			PlacementSemantics::Partitioned,
			Some(partition),
			&frame_with_row_partitions(Some(vec![Some(partition.get()), Some(partition.get())])),
			RecordRowIndex::ZERO,
		)
		.expect("agreeing carriers must read");
		assert_eq!(resolved, (PlacementSemantics::Partitioned, Some(partition)));
	}

	/// The requested row governs, never a neighbour. One record, two rows with
	/// different frame entries: each row resolves by its own entry, and an index
	/// claim that matches only the neighbour is refused.
	#[test]
	fn adr0090_the_requested_row_governs_not_a_neighbour() {
		let partition = PartitionId::new(7);
		let mixed = frame_with_row_partitions(Some(vec![Some(partition.get()), None]));

		// Row 0 is partitioned; its own entry agrees.
		assert_eq!(
			resolve_record_placement(
				PlacementSemantics::Partitioned,
				Some(partition),
				&mixed,
				RecordRowIndex::ZERO,
			)
			.expect("row 0 agrees with its own frame entry"),
			(PlacementSemantics::Partitioned, Some(partition))
		);

		// Row 1 is raw-locality; the neighbour's partition must not govern it.
		assert_eq!(
			resolve_record_placement(
				PlacementSemantics::RawLocality,
				None,
				&mixed,
				RecordRowIndex::new(1),
			)
			.expect("row 1 resolves by its own null entry"),
			(PlacementSemantics::RawLocality, None)
		);

		// An index that claims row 1 is partitioned matches only the neighbour's
		// entry, and is refused rather than satisfied by it.
		let err = resolve_record_placement(
			PlacementSemantics::Partitioned,
			Some(partition),
			&mixed,
			RecordRowIndex::new(1),
		)
		.expect_err("a neighbour's frame entry must not satisfy row 1's index claim");
		assert!(err.to_string().contains("carries none at row 1"), "{err}");

		// And two rows carrying different partitions: the index claim matching
		// row 0 must not pass for row 1.
		let disagreeing = frame_with_row_partitions(Some(vec![Some(7), Some(9)]));
		let err = resolve_record_placement(
			PlacementSemantics::Partitioned,
			Some(PartitionId::new(7)),
			&disagreeing,
			RecordRowIndex::new(1),
		)
		.expect_err("row 1's own entry disagrees even though the neighbour matches");
		assert!(err.to_string().contains("disagrees at row 1"), "{err}");
	}

	#[test]
	fn adr0090_disagreeing_partitions_are_refused() {
		let err = resolve_record_placement(
			PlacementSemantics::Partitioned,
			Some(PartitionId::new(1)),
			&frame_with_row_partitions(Some(vec![Some(2), Some(2)])),
			RecordRowIndex::ZERO,
		)
		.expect_err("two carriers naming different partitions is corruption");
		assert!(err.to_string().contains("placement disagrees"), "{err}");
	}

	#[test]
	fn adr0090_an_index_partition_without_a_frame_partition_is_refused() {
		let err = resolve_record_placement(
			PlacementSemantics::Partitioned,
			Some(PartitionId::new(1)),
			&frame_with_row_partitions(None),
			RecordRowIndex::ZERO,
		)
		.expect_err("an index-only partition claim is corruption");
		assert!(err.to_string().contains("carries none"), "{err}");
	}
}

/// ADR 0090: the partition a row is written under, derived at the
/// descriptor-aware write boundary. A MultiRow-layout table whose locality
/// policy carries a prefix writes partitioned rows; identity-projection
/// locality has no prefix and stays raw-locality, as does every SingleRow
/// table.
pub(crate) fn write_partition_for_row(
	descriptor: &TableSchemaDescriptor,
	routing_locality: Locality,
) -> std::io::Result<Option<PartitionId>> {
	let Some(prefix_bits) = crate::schema::table::partitioned_prefix_width(descriptor) else {
		return Ok(None);
	};
	Ok(Some(partition_for_locality(routing_locality, prefix_bits)?))
}

/// ADR 0089: the bucket packer groups on the durable partition, so rows of one
/// partition share records at the layout's target instead of one record per
/// row. Legacy (identity-locality) rows keep their old one-record-per-locality
/// shape.
#[cfg(test)]
mod adr0089_partition_keyed_packing {
	use super::*;
	use crate::schema::TenantId;
	use crate::schema::table::{
		LocalityPolicy, PrimaryKeyDescriptor, RowIdentityPolicy, SchemaEvolutionPolicy,
		TableSpreadLocalityDescriptor, encode_table_write_row_count,
	};
	use arrow::array::Int64Array;

	fn descriptor_with_spread() -> TableSchemaDescriptor {
		let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
		let mut descriptor = TableSchemaDescriptor::new(
			TableObjectId::new([0x59; 16]),
			1,
			"analytics",
			"packed_events",
			&schema,
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
			LocalityPolicy::TableSpreadProjection(TableSpreadLocalityDescriptor {
				spread_factor: 1,
				prefix_bits: 1,
			}),
			SchemaEvolutionPolicy::AdditiveOnly,
		)
		.unwrap();
		descriptor.record_layout_policy =
			RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::new(4, 8, 1024 * 1024).unwrap());
		descriptor.record_body_compression_policy =
			RecordBodyCompressionPolicy::default_for_layout(&descriptor.record_layout_policy);
		descriptor.refresh_descriptor_fingerprints().unwrap();
		descriptor
	}

	fn meta(descriptor: &TableSchemaDescriptor, rows: usize) -> TableWriteRequestMeta {
		let mut meta: TableWriteRequestMeta = bytemuck::Zeroable::zeroed();
		meta.tenant_id = TenantId::new(7);
		meta.table_object_id = descriptor.table_object_id;
		meta.schema_generation = descriptor.schema_generation;
		meta.row_count = encode_table_write_row_count(rows).unwrap();
		meta.client_hlc = HybridTimestamp {
			physical_ms: 10,
			logical: 0,
			node_id: 1,
		};
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
	fn adr0089_rows_of_one_partition_pack_to_the_target() {
		let descriptor = descriptor_with_spread();
		let batch = batch((1..=10).collect());
		let meta = meta(&descriptor, batch.num_rows());
		let packed = pack_record_batch_by_bucket(
			&meta,
			&descriptor,
			&batch,
			WriterNodeId::ZERO,
			meta.client_hlc,
		)
		.expect("pack partitioned rows");

		// Premise: the descriptor really produces partitioned rows.
		let total_rows: usize = packed
			.iter()
			.map(|record| record.source_row_indices.len())
			.sum();
		assert_eq!(total_rows, 10, "every row packs");
		assert!(
			packed.iter().all(|record| record.partition.is_some()),
			"premise: every record is partitioned"
		);

		// Rows grouped per partition at target_rows (4 for the test layout):
		// with spread_factor 1 every row shares one partition, so 10 rows pack
		// to ceil(10/4) = 3 records, not 10.
		let partitions: std::collections::BTreeSet<_> =
			packed.iter().map(|record| record.partition).collect();
		if partitions.len() == 1 {
			assert_eq!(
				packed.len(),
				3,
				"one partition, target 4: ceil(10/4) records"
			);
		} else {
			// Spread landed rows in several partitions; the invariant is then
			// per partition.
			for partition in &partitions {
				let records: Vec<_> = packed
					.iter()
					.filter(|record| record.partition == *partition)
					.collect();
				let rows: usize = records
					.iter()
					.map(|record| record.source_row_indices.len())
					.sum();
				assert_eq!(records.len(), rows.div_ceil(4));
			}
			assert!(
				packed.len() < 10,
				"grouping must beat one record per row: {} records",
				packed.len()
			);
		}
		for record in &packed {
			// The record's stored locality is its partition prefix; every row's
			// own locality stays inside the partition span.
			let partition = record.partition.expect("asserted above");
			assert_eq!(record.locality.get(), partition.get());
		}
	}
}
