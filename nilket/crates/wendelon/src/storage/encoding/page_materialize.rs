use crate::crypto::TieredPlaintextRange;
use crate::schema::{
	EncodingCascadeIndex, IpcBufferIndex, IpcBufferLength, IpcBufferOffset, IpcColumnOrdinal,
	RecordBodyByteLen, RecordBodyByteOffset, RowGroupIndex, RowGroupRowCount,
};
use crate::storage::columnar::{
	ColumnFetchRequirement, ColumnOrdinalMapping, IpcBufferRange, ProjectedRecordBatch,
	ProjectedRecordBufferSet, RecordBufferDirectory, StorageColumn,
	decode_projected_record_batch_from_buffers,
};
use crate::storage::encoding::cascade::{RleByteRunIter, decode_page};
use crate::storage::encoding::{BufferEncoding, EncodingCascade, EncodingPageDescriptor};
use crate::storage::row_group::RecordBodyByteRange;
use arrow::array::{Array, BooleanArray, new_null_array};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatchOptions;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct PageMaterializeKey {
	pub row_group_index: RowGroupIndex,
	pub page_index: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct PageMaterializeBufferKey {
	pub row_group_index: RowGroupIndex,
	pub ipc_column_ordinal: IpcColumnOrdinal,
	pub ipc_buffer_ordinal: IpcBufferIndex,
	pub page_index: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PageMaterializeColumn {
	pub physical_ordinal: usize,
	pub null_buffer_ordinal: IpcBufferIndex,
	pub value_buffer_ordinal: IpcBufferIndex,
	pub bytes_per_value: usize,
	pub data_type: DataType,
	pub nullable: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct PageMaterializeFetch {
	pub key: PageMaterializeBufferKey,
	pub body_range: RecordBodyByteRange,
	pub cascade: EncodingCascade,
	pub page: EncodingPageDescriptor,
	pub bytes_per_value: usize,
	pub data_type: DataType,
}

#[derive(Clone, Debug)]
pub(crate) struct PageMaterializeValidityFetch {
	pub key: PageMaterializeBufferKey,
	pub body_range: RecordBodyByteRange,
	pub page: EncodingPageDescriptor,
	pub source: PageMaterializeValiditySource,
	pub page_row_count: RowGroupRowCount,
}

#[derive(Clone, Debug)]
pub(crate) enum PageMaterializeValiditySource {
	Raw { bit_offset: u8 },
	Encoded { cascade: EncodingCascadeIndex },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PageMaterializeValueSource {
	Decoded,
	AllNullSynthesized,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PageValidityBitmap {
	pub bytes: Vec<u8>,
	pub null_count: usize,
}

pub(crate) fn validity_page_decoded_byte_len(page_value_count: usize) -> usize {
	page_value_count.div_ceil(8)
}

pub(crate) fn synthesize_all_null_page_buffers(
	page_row_count: usize,
	bytes_per_value: usize,
) -> Result<(Vec<u8>, PageValidityBitmap), arrow::error::ArrowError> {
	let value_len = page_row_count.checked_mul(bytes_per_value).ok_or_else(|| {
		arrow::error::ArrowError::SchemaError("all-null page value byte length overflow".into())
	})?;
	Ok((
		vec![0_u8; value_len],
		PageValidityBitmap {
			bytes: vec![0_u8; validity_page_decoded_byte_len(page_row_count)],
			null_count: page_row_count,
		},
	))
}

pub(crate) fn fixed_width_value_bytes(data_type: &DataType) -> Option<usize> {
	match data_type {
		DataType::Int8 | DataType::UInt8 => Some(1),
		DataType::Int16 | DataType::UInt16 => Some(2),
		DataType::Int32
		| DataType::UInt32
		| DataType::Float32
		| DataType::Date32
		| DataType::Time32(_) => Some(4),
		DataType::Int64
		| DataType::UInt64
		| DataType::Float64
		| DataType::Date64
		| DataType::Time64(_)
		| DataType::Duration(_)
		| DataType::Timestamp(_, _) => Some(8),
		DataType::FixedSizeBinary(width) if *width > 0 => usize::try_from(*width).ok(),
		DataType::Decimal128(_, _) => Some(16),
		DataType::Decimal256(_, _) => Some(32),
		_ => None,
	}
}

pub(crate) fn required_projected_physical_ordinals(
	directory: &RecordBufferDirectory,
	requirement: &ColumnFetchRequirement,
) -> Option<Vec<usize>> {
	let user_physical_ordinals = directory
		.ipc_schema
		.fields()
		.iter()
		.enumerate()
		.filter_map(|(physical, field)| {
			StorageColumn::from_name(field.name())
				.is_none()
				.then_some(physical)
		})
		.collect::<Vec<_>>();
	let mut out = Vec::new();
	for descriptor_ordinal in requirement.user_visible().iter() {
		let physical = *user_physical_ordinals.get(descriptor_ordinal as usize)?;
		out.push(physical);
	}
	for storage_column in requirement.storage_columns.iter() {
		let physical = directory
			.storage_column_ordinals
			.get(&storage_column)
			.copied()? as usize;
		out.push(physical);
	}
	out.sort_unstable();
	out.dedup();
	Some(out)
}

pub(crate) fn fixed_width_sparse_projection_column(
	directory: &RecordBufferDirectory,
	physical_ordinal: usize,
) -> Option<PageMaterializeColumn> {
	let field = directory.ipc_schema.fields().get(physical_ordinal)?;
	let nullable = directory
		.column_null_counts
		.get(physical_ordinal)
		.copied()
		.unwrap_or(usize::MAX)
		!= 0;
	let bytes_per_value = fixed_width_value_bytes(field.data_type())?;
	let mapping = directory.column_to_buffers.get(physical_ordinal)?;
	if mapping.buffer_count != 2 {
		return None;
	}
	let null_buffer_ordinal = mapping.first_buffer_ordinal;
	let value_buffer_ordinal =
		IpcBufferIndex::new(mapping.first_buffer_ordinal.get().checked_add(1)?);
	Some(PageMaterializeColumn {
		physical_ordinal,
		null_buffer_ordinal,
		value_buffer_ordinal,
		bytes_per_value,
		data_type: field.data_type().clone(),
		nullable,
	})
}

pub(crate) fn fixed_width_encoding_indexed_value_column(
	directory: &RecordBufferDirectory,
	physical_ordinal: usize,
) -> Option<PageMaterializeColumn> {
	if directory
		.column_null_counts
		.get(physical_ordinal)
		.copied()
		.unwrap_or(usize::MAX)
		!= 0
	{
		return None;
	}
	let mut column = fixed_width_sparse_projection_column(directory, physical_ordinal)?;
	column.nullable = false;
	Some(column)
}

pub(crate) fn encoded_page_body_range(
	buffer_range: RecordBodyByteRange,
	encoding: &BufferEncoding,
	page: EncodingPageDescriptor,
) -> RecordBodyByteRange {
	let encoded_start = u64::from(encoding.encoded_range.start.get());
	let page_start = u64::from(page.byte_range.start.get());
	RecordBodyByteRange {
		start: RecordBodyByteOffset::new(
			buffer_range
				.start
				.get()
				.saturating_add(encoded_start)
				.saturating_add(page_start),
		),
		len: page.byte_range.len,
	}
}

pub(crate) fn validity_bitmap_body_range_for_page(
	validity_buffer_range: RecordBodyByteRange,
	page: EncodingPageDescriptor,
) -> Result<(RecordBodyByteRange, u8), arrow::error::ArrowError> {
	let bit_start = u64::from(page.first_value_index.get());
	let bit_len = u64::from(page.value_count.get());
	let byte_start = bit_start / 8;
	let bit_offset = (bit_start % 8) as u8;
	let byte_len = (u64::from(bit_offset)
		.checked_add(bit_len)
		.and_then(|bits| bits.checked_add(7))
		.ok_or_else(|| {
			arrow::error::ArrowError::SchemaError("validity bitmap byte span overflow".into())
		})?) / 8;
	let byte_end = byte_start.checked_add(byte_len).ok_or_else(|| {
		arrow::error::ArrowError::SchemaError("validity bitmap byte span overflow".into())
	})?;
	if byte_end > validity_buffer_range.len.get() {
		return Err(arrow::error::ArrowError::SchemaError(format!(
			"validity bitmap span {}..{} exceeds buffer length {}",
			byte_start,
			byte_end,
			validity_buffer_range.len.get()
		)));
	}
	let start = validity_buffer_range
		.start
		.get()
		.checked_add(byte_start)
		.ok_or_else(|| {
			arrow::error::ArrowError::SchemaError("validity bitmap body range overflow".into())
		})?;
	Ok((
		RecordBodyByteRange {
			start: RecordBodyByteOffset::new(start),
			len: RecordBodyByteLen::new(byte_len),
		},
		bit_offset,
	))
}

pub(crate) fn rebase_validity_bitmap(
	fetched: &[u8],
	bit_offset: u8,
	rows: usize,
) -> Result<PageValidityBitmap, arrow::error::ArrowError> {
	if bit_offset >= 8 {
		return Err(arrow::error::ArrowError::SchemaError(format!(
			"validity bitmap bit offset {bit_offset} is out of range"
		)));
	}
	let required_bits = usize::from(bit_offset).checked_add(rows).ok_or_else(|| {
		arrow::error::ArrowError::SchemaError("validity bitmap bit count overflow".into())
	})?;
	let required_bytes = required_bits.div_ceil(8);
	if fetched.len() < required_bytes {
		return Err(arrow::error::ArrowError::SchemaError(format!(
			"validity bitmap fetched {} bytes but {} required",
			fetched.len(),
			required_bytes
		)));
	}
	let mut bytes = vec![0_u8; rows.div_ceil(8)];
	let mut null_count = 0_usize;
	for row in 0..rows {
		let source_bit = usize::from(bit_offset) + row;
		let valid = (fetched[source_bit / 8] & (1_u8 << (source_bit % 8))) != 0;
		if valid {
			bytes[row / 8] |= 1_u8 << (row % 8);
		} else {
			null_count = null_count.saturating_add(1);
		}
	}
	Ok(PageValidityBitmap { bytes, null_count })
}

pub(crate) fn decode_rle_validity_page(
	encoded_page: &[u8],
	page_value_count: usize,
) -> Result<PageValidityBitmap, arrow::error::ArrowError> {
	let expected = validity_page_decoded_byte_len(page_value_count);
	let mut bytes = Vec::with_capacity(expected);
	for run in RleByteRunIter::new(encoded_page) {
		let run = run.map_err(|code| {
			arrow::error::ArrowError::SchemaError(format!(
				"encoded validity RLE page rejected: {code:?}"
			))
		})?;
		let end = bytes.len().checked_add(run.run_len).ok_or_else(|| {
			arrow::error::ArrowError::SchemaError(
				"encoded validity decoded byte length overflow".into(),
			)
		})?;
		if end > expected {
			return Err(arrow::error::ArrowError::SchemaError(format!(
				"encoded validity decoded {} bytes but {} expected",
				end, expected
			)));
		}
		bytes.resize(end, run.byte);
	}
	if bytes.len() != expected {
		return Err(arrow::error::ArrowError::SchemaError(format!(
			"encoded validity decoded {} bytes but {} expected",
			bytes.len(),
			expected
		)));
	}
	let null_count = validity_null_count(&bytes, page_value_count)?;
	Ok(PageValidityBitmap { bytes, null_count })
}

pub(crate) fn stitch_validity_pages_into_row_group_bitmap(
	pages: &[(EncodingPageDescriptor, PageValidityBitmap)],
	row_group_row_count: usize,
) -> Result<Vec<u8>, arrow::error::ArrowError> {
	let mut ordered = pages.iter().collect::<Vec<_>>();
	ordered.sort_by_key(|(page, _)| page.first_value_index.get());
	let mut out = vec![0_u8; validity_page_decoded_byte_len(row_group_row_count)];
	let mut expected_next = 0_usize;
	for (page, validity) in ordered {
		let start = page.first_value_index.get() as usize;
		let len = page.value_count.get() as usize;
		if start != expected_next {
			return Err(arrow::error::ArrowError::SchemaError(format!(
				"encoded validity page span starts at {start}, expected {expected_next}"
			)));
		}
		let end = start.checked_add(len).ok_or_else(|| {
			arrow::error::ArrowError::SchemaError("encoded validity row span overflow".into())
		})?;
		if end > row_group_row_count {
			return Err(arrow::error::ArrowError::SchemaError(format!(
				"encoded validity page span {start}..{end} exceeds row group row count {row_group_row_count}"
			)));
		}
		let expected_len = validity_page_decoded_byte_len(len);
		if validity.bytes.len() != expected_len {
			return Err(arrow::error::ArrowError::SchemaError(format!(
				"encoded validity page has {} bytes but {} expected",
				validity.bytes.len(),
				expected_len
			)));
		}
		if len == 0 && row_group_row_count != 0 {
			return Err(arrow::error::ArrowError::SchemaError(
				"encoded validity page has zero rows".into(),
			));
		}
		for row in 0..len {
			let valid = (validity.bytes[row / 8] & (1_u8 << (row % 8))) != 0;
			if valid {
				let target = start + row;
				out[target / 8] |= 1_u8 << (target % 8);
			}
		}
		expected_next = end;
	}
	if expected_next != row_group_row_count {
		return Err(arrow::error::ArrowError::SchemaError(format!(
			"encoded validity stitched {expected_next} rows but {row_group_row_count} expected"
		)));
	}
	Ok(out)
}

fn validity_null_count(
	bytes: &[u8],
	page_value_count: usize,
) -> Result<usize, arrow::error::ArrowError> {
	if bytes.len() != validity_page_decoded_byte_len(page_value_count) {
		return Err(arrow::error::ArrowError::SchemaError(format!(
			"validity bitmap has {} bytes but {} expected for {} rows",
			bytes.len(),
			validity_page_decoded_byte_len(page_value_count),
			page_value_count
		)));
	}
	let mut null_count = 0_usize;
	for row in 0..page_value_count {
		if (bytes[row / 8] & (1_u8 << (row % 8))) == 0 {
			null_count = null_count.saturating_add(1);
		}
	}
	Ok(null_count)
}

pub(crate) fn decode_page_fetch(
	fetch: &PageMaterializeFetch,
	encoded: &[u8],
) -> Result<Vec<u8>, arrow::error::ArrowError> {
	let expected_bytes = fetch
		.page
		.value_count
		.get()
		.checked_mul(fetch.bytes_per_value as u32)
		.ok_or_else(|| {
			arrow::error::ArrowError::SchemaError(
				"encoded page decoded byte length overflow".into(),
			)
		})?;
	decode_page(
		encoded,
		&fetch.data_type,
		&fetch.cascade,
		RecordBodyByteLen::new(u64::from(expected_bytes)),
		fetch.page.value_count,
	)
	.map_err(|code| {
		crate::metrics::global().observe_storage_l2_encoding_invariant_violation(code.as_str());
		arrow::error::ArrowError::SchemaError(format!("encoded page rejected: {code:?}"))
	})
}

pub(crate) fn projected_page_batch_from_decoded_buffers(
	record_directory: &RecordBufferDirectory,
	requirement: &ColumnFetchRequirement,
	columns: &[PageMaterializeColumn],
	page_key: PageMaterializeKey,
	page_row_count: usize,
	decoded_pages: &BTreeMap<PageMaterializeBufferKey, Vec<u8>>,
	decoded_validity_pages: &BTreeMap<PageMaterializeBufferKey, PageValidityBitmap>,
	all_valid_nullable_pages: &BTreeSet<PageMaterializeBufferKey>,
) -> Result<ProjectedRecordBatch, arrow::error::ArrowError> {
	let mut buffers = vec![
		IpcBufferRange {
			offset_in_record: IpcBufferOffset::ZERO,
			length: IpcBufferLength::ZERO,
		};
		record_directory.buffers.len()
	];
	let mut buffer_set = ProjectedRecordBufferSet::new();
	for column in columns {
		buffers[column.null_buffer_ordinal.get() as usize] = IpcBufferRange {
			offset_in_record: IpcBufferOffset::ZERO,
			length: IpcBufferLength::ZERO,
		};
		let key = PageMaterializeBufferKey {
			row_group_index: page_key.row_group_index,
			ipc_column_ordinal: IpcColumnOrdinal::new(column.physical_ordinal as u32),
			ipc_buffer_ordinal: column.value_buffer_ordinal,
			page_index: page_key.page_index,
		};
		let decoded = decoded_pages.get(&key).ok_or_else(|| {
			arrow::error::ArrowError::SchemaError(
				"decoded page missing for projected column".into(),
			)
		})?;
		buffers[column.value_buffer_ordinal.get() as usize] = IpcBufferRange {
			offset_in_record: IpcBufferOffset::ZERO,
			length: IpcBufferLength::new(decoded.len() as u64),
		};
		buffer_set.insert(column.value_buffer_ordinal, decoded.clone());
	}
	let mut column_null_counts = record_directory.column_null_counts.clone();
	for column in columns {
		if let Some(null_count) = column_null_counts.get_mut(column.physical_ordinal) {
			if column.nullable {
				let key = PageMaterializeBufferKey {
					row_group_index: page_key.row_group_index,
					ipc_column_ordinal: IpcColumnOrdinal::new(column.physical_ordinal as u32),
					ipc_buffer_ordinal: column.null_buffer_ordinal,
					page_index: page_key.page_index,
				};
				if let Some(validity) = decoded_validity_pages.get(&key) {
					*null_count = validity.null_count;
					if validity.null_count > 0 {
						buffers[column.null_buffer_ordinal.get() as usize] = IpcBufferRange {
							offset_in_record: IpcBufferOffset::ZERO,
							length: IpcBufferLength::new(validity.bytes.len() as u64),
						};
						buffer_set.insert(column.null_buffer_ordinal, validity.bytes.clone());
					}
				} else if all_valid_nullable_pages.contains(&key) {
					*null_count = 0;
					buffers[column.null_buffer_ordinal.get() as usize] = IpcBufferRange {
						offset_in_record: IpcBufferOffset::ZERO,
						length: IpcBufferLength::ZERO,
					};
				} else {
					return Err(arrow::error::ArrowError::SchemaError(
						"decoded validity page missing for projected column".into(),
					));
				}
			} else {
				*null_count = 0;
			}
		}
	}
	let scoped = RecordBufferDirectory {
		record_id: record_directory.record_id,
		record_plaintext_range: record_directory.record_plaintext_range,
		row_count: page_row_count,
		record_body_compression: crate::schema::RecordBodyCompressionPolicy::Uncompressed,
		ipc_schema: record_directory.ipc_schema.clone(),
		buffers,
		column_to_buffers: record_directory.column_to_buffers.clone(),
		column_null_counts,
		storage_column_ordinals: record_directory.storage_column_ordinals.clone(),
		logical_schema_fingerprint: record_directory.logical_schema_fingerprint,
		page_directory: None,
		encoding_directory: None,
	};
	decode_projected_record_batch_from_buffers(&scoped, requirement, &buffer_set)
}

pub(crate) fn extract_record_body_bytes<B: AsRef<[u8]>>(
	range: RecordBodyByteRange,
	selected_ranges: &[TieredPlaintextRange],
	plaintext_extracts: &[B],
) -> Result<Vec<u8>, arrow::error::ArrowError> {
	if selected_ranges.len() != plaintext_extracts.len() {
		return Err(arrow::error::ArrowError::InvalidArgumentError(format!(
			"encoded page expected {} plaintext extracts, got {}",
			selected_ranges.len(),
			plaintext_extracts.len()
		)));
	}
	let start = range.start.get();
	let end = range
		.end()
		.map_err(|err| arrow::error::ArrowError::SchemaError(err.to_string()))?;
	for (candidate, extract) in selected_ranges.iter().zip(plaintext_extracts.iter()) {
		let candidate_start = candidate.start.get();
		let candidate_end = candidate_start
			.checked_add(candidate.len.get())
			.ok_or_else(|| {
				arrow::error::ArrowError::SchemaError(
					"encoded page plaintext range overflow".into(),
				)
			})?;
		if start >= candidate_start && end <= candidate_end {
			let relative_start = usize::try_from(start - candidate_start).map_err(|_| {
				arrow::error::ArrowError::SchemaError(
					"encoded page relative start exceeds usize".into(),
				)
			})?;
			let relative_end = usize::try_from(end - candidate_start).map_err(|_| {
				arrow::error::ArrowError::SchemaError(
					"encoded page relative end exceeds usize".into(),
				)
			})?;
			let extract = extract.as_ref();
			if relative_end > extract.len() {
				return Err(arrow::error::ArrowError::SchemaError(
					"encoded page range exceeds plaintext extract".into(),
				));
			}
			return Ok(extract[relative_start..relative_end].to_vec());
		}
	}
	Err(arrow::error::ArrowError::SchemaError(
		"encoded page range was not fetched".into(),
	))
}

pub(crate) fn empty_projected_batch_for_row_count(
	record_directory: &RecordBufferDirectory,
	row_count: usize,
) -> Result<ProjectedRecordBatch, arrow::error::ArrowError> {
	let batch = arrow::record_batch::RecordBatch::try_new_with_options(
		std::sync::Arc::new(arrow::datatypes::Schema::empty()),
		Vec::new(),
		&arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(row_count)),
	)?;
	Ok(ProjectedRecordBatch {
		batch,
		ordinal_mapping: ColumnOrdinalMapping::new(vec![
			None;
			record_directory
				.ipc_schema
				.fields()
				.iter()
				.filter(|field| StorageColumn::from_name(field.name()).is_none())
				.count()
		]),
		storage_mapping: BTreeMap::new(),
	})
}

pub(crate) fn empty_projected_batch_for_requirement(
	record_directory: &RecordBufferDirectory,
	requirement: &ColumnFetchRequirement,
	row_count: usize,
) -> Result<ProjectedRecordBatch, arrow::error::ArrowError> {
	if requirement.user_visible().is_empty() && requirement.storage_columns.is_empty() {
		return empty_projected_batch_for_row_count(record_directory, row_count);
	}
	if record_directory.ipc_schema.fields().len() != record_directory.column_to_buffers.len() {
		return Err(arrow::error::ArrowError::SchemaError(format!(
			"empty projected directory has {} columns but IPC schema has {}",
			record_directory.column_to_buffers.len(),
			record_directory.ipc_schema.fields().len()
		)));
	}

	let user_physical_ordinals = record_directory
		.ipc_schema
		.fields()
		.iter()
		.enumerate()
		.filter_map(|(physical, field)| {
			StorageColumn::from_name(field.name())
				.is_none()
				.then_some(physical)
		})
		.collect::<Vec<_>>();
	let mut descriptor_to_physical = vec![None; user_physical_ordinals.len()];
	let mut fields = Vec::new();
	let mut arrays = Vec::new();
	for descriptor_ordinal in requirement.user_visible().iter() {
		let descriptor_index = usize::try_from(descriptor_ordinal).map_err(|_| {
			arrow::error::ArrowError::SchemaError(format!(
				"descriptor column ordinal {descriptor_ordinal} exceeds usize::MAX"
			))
		})?;
		let physical = *user_physical_ordinals
			.get(descriptor_index)
			.ok_or_else(|| {
				arrow::error::ArrowError::SchemaError(format!(
					"descriptor column ordinal {descriptor_ordinal} out of range"
				))
			})?;
		let field = record_directory.ipc_schema.field(physical);
		descriptor_to_physical[descriptor_index] = Some(arrays.len());
		fields.push(field.clone());
		arrays.push(new_null_array(field.data_type(), row_count));
	}

	let mut storage_mapping = BTreeMap::new();
	for storage_column in requirement.storage_columns.iter() {
		let physical = record_directory
			.storage_column_ordinals
			.get(&storage_column)
			.copied()
			.ok_or_else(|| {
				arrow::error::ArrowError::SchemaError(format!(
					"storage column {storage_column:?} missing from record directory"
				))
			})?;
		let physical = usize::try_from(physical).map_err(|_| {
			arrow::error::ArrowError::SchemaError(format!(
				"storage column {storage_column:?} ordinal exceeds usize::MAX"
			))
		})?;
		let field = record_directory.ipc_schema.field(physical);
		if StorageColumn::from_name(field.name()) != Some(storage_column) {
			return Err(arrow::error::ArrowError::SchemaError(format!(
				"storage column {storage_column:?} mapped to incompatible field {}",
				field.name()
			)));
		}
		storage_mapping.insert(storage_column, arrays.len());
		fields.push(field.clone());
		arrays.push(new_null_array(field.data_type(), row_count));
	}

	let batch = arrow::record_batch::RecordBatch::try_new_with_options(
		Arc::new(Schema::new(fields)),
		arrays,
		&RecordBatchOptions::new().with_row_count(Some(row_count)),
	)?;
	Ok(ProjectedRecordBatch {
		batch,
		ordinal_mapping: ColumnOrdinalMapping::new(descriptor_to_physical),
		storage_mapping,
	})
}

pub(crate) fn filtered_predicate_page_batch(
	predicate_batch: &ProjectedRecordBatch,
	page: EncodingPageDescriptor,
	local_mask: &BooleanArray,
) -> Result<ProjectedRecordBatch, arrow::error::ArrowError> {
	let start = page.first_value_index.get() as usize;
	let len = page.value_count.get() as usize;
	if local_mask.len() != len {
		return Err(arrow::error::ArrowError::SchemaError(format!(
			"predicate page mask length {} does not match page row count {}",
			local_mask.len(),
			len
		)));
	}
	if start
		.checked_add(len)
		.is_none_or(|end| end > predicate_batch.batch.num_rows())
	{
		return Err(arrow::error::ArrowError::SchemaError(format!(
			"predicate page row span {}..{} exceeds predicate batch row count {}",
			start,
			start.saturating_add(len),
			predicate_batch.batch.num_rows()
		)));
	}
	let page_batch = predicate_batch.slice(start, len);
	if local_mask.true_count() == 0 {
		return Ok(page_batch.slice(0, 0));
	}
	if local_mask.null_count() == 0 && local_mask.true_count() == local_mask.len() {
		return Ok(page_batch);
	}
	let batch = arrow::compute::filter_record_batch(&page_batch.batch, local_mask)?;
	Ok(ProjectedRecordBatch {
		batch,
		ordinal_mapping: page_batch.ordinal_mapping,
		storage_mapping: page_batch.storage_mapping,
	})
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::schema::{
		BufferValueCount, BufferValueIndex, EncodingPageByteOffset, EncodingPageIndex,
		RecordBodyByteLen, RecordBodyByteOffset,
	};
	use crate::storage::encoding::EncodedBufferByteRange;
	use arrow::array::{ArrayRef, Int64Array, StringArray};
	use arrow::datatypes::Field;
	use arrow::record_batch::RecordBatch;

	fn page(first_value_index: u32, value_count: u32) -> EncodingPageDescriptor {
		EncodingPageDescriptor {
			page_index: EncodingPageIndex::ZERO,
			first_value_index: BufferValueIndex::new(first_value_index),
			value_count: BufferValueCount::new(value_count),
			byte_range: EncodedBufferByteRange {
				start: EncodingPageByteOffset::ZERO,
				len: RecordBodyByteLen::ZERO,
			},
		}
	}

	fn projected_i64_batch(values: std::ops::Range<i64>) -> ProjectedRecordBatch {
		let batch = RecordBatch::try_new(
			Arc::new(Schema::new(vec![Field::new(
				"selector",
				DataType::Int64,
				false,
			)])),
			vec![Arc::new(Int64Array::from_iter_values(values)) as ArrayRef],
		)
		.unwrap();
		ProjectedRecordBatch {
			batch,
			ordinal_mapping: ColumnOrdinalMapping::new(vec![Some(0)]),
			storage_mapping: BTreeMap::new(),
		}
	}

	fn validity_bytes(rows: usize, is_valid: impl Fn(usize) -> bool) -> Vec<u8> {
		let mut bytes = vec![0_u8; validity_page_decoded_byte_len(rows)];
		for row in 0..rows {
			if is_valid(row) {
				bytes[row / 8] |= 1_u8 << (row % 8);
			}
		}
		bytes
	}

	fn rle_bytes(input: &[u8]) -> Vec<u8> {
		let mut out = Vec::new();
		let mut cursor = 0_usize;
		while cursor < input.len() {
			let byte = input[cursor];
			let mut run = 1_usize;
			while cursor + run < input.len() && input[cursor + run] == byte {
				run += 1;
			}
			out.extend_from_slice(&(run as u32).to_be_bytes());
			out.push(byte);
			cursor += run;
		}
		out
	}

	#[test]
	fn decode_rle_validity_page_counts_only_live_bits() {
		for rows in [1_usize, 7, 8, 9, 13, 16, 64, 256, 1027] {
			let all_valid = validity_bytes(rows, |_| true);
			let decoded = decode_rle_validity_page(&rle_bytes(&all_valid), rows).unwrap();
			assert_eq!(decoded.bytes, all_valid, "all-valid rows={rows}");
			assert_eq!(decoded.null_count, 0, "all-valid rows={rows}");

			let all_null = validity_bytes(rows, |_| false);
			let decoded = decode_rle_validity_page(&rle_bytes(&all_null), rows).unwrap();
			assert_eq!(decoded.bytes, all_null, "all-null rows={rows}");
			assert_eq!(decoded.null_count, rows, "all-null rows={rows}");
		}
	}

	#[test]
	fn decode_rle_validity_page_rejects_corrupt_payload() {
		let err = decode_rle_validity_page(&[0, 0, 0, 2], 8).unwrap_err();
		assert!(
			err.to_string()
				.contains("encoded validity RLE page rejected")
		);
	}

	#[test]
	fn stitch_validity_pages_rebases_page_local_bits() {
		let left_page = page(0, 13);
		let right_page = page(13, 5);
		let left = PageValidityBitmap {
			bytes: validity_bytes(13, |row| matches!(row, 0 | 4 | 12)),
			null_count: 10,
		};
		let right = PageValidityBitmap {
			bytes: validity_bytes(5, |row| matches!(row, 0 | 3)),
			null_count: 3,
		};
		let stitched = stitch_validity_pages_into_row_group_bitmap(
			&[(right_page, right), (left_page, left)],
			18,
		)
		.unwrap();
		assert_eq!(
			stitched,
			validity_bytes(18, |row| matches!(row, 0 | 4 | 12 | 13 | 16))
		);
	}

	#[test]
	fn stitch_validity_pages_rejects_gaps_and_overlaps() {
		let gap = stitch_validity_pages_into_row_group_bitmap(
			&[
				(
					page(0, 8),
					PageValidityBitmap {
						bytes: validity_bytes(8, |_| true),
						null_count: 0,
					},
				),
				(
					page(9, 4),
					PageValidityBitmap {
						bytes: validity_bytes(4, |_| true),
						null_count: 0,
					},
				),
			],
			13,
		)
		.unwrap_err();
		assert!(gap.to_string().contains("expected 8"));

		let overlap = stitch_validity_pages_into_row_group_bitmap(
			&[
				(
					page(0, 8),
					PageValidityBitmap {
						bytes: validity_bytes(8, |_| true),
						null_count: 0,
					},
				),
				(
					page(7, 4),
					PageValidityBitmap {
						bytes: validity_bytes(4, |_| true),
						null_count: 0,
					},
				),
			],
			11,
		)
		.unwrap_err();
		assert!(overlap.to_string().contains("expected 8"));
	}

	#[test]
	fn filtered_predicate_page_batch_slices_and_filters_page_rows() {
		let predicate = projected_i64_batch(0..12);
		let filtered = filtered_predicate_page_batch(
			&predicate,
			page(4, 4),
			&BooleanArray::from(vec![true, false, true, false]),
		)
		.unwrap();
		let values = filtered
			.batch
			.column(0)
			.as_any()
			.downcast_ref::<Int64Array>()
			.unwrap();
		assert_eq!(values.values(), &[4, 6]);
		assert_eq!(filtered.ordinal_mapping, predicate.ordinal_mapping);
	}

	#[test]
	fn filtered_predicate_page_batch_keeps_all_true_page_slice() {
		let predicate = projected_i64_batch(0..12);
		let filtered = filtered_predicate_page_batch(
			&predicate,
			page(8, 4),
			&BooleanArray::from(vec![true, true, true, true]),
		)
		.unwrap();
		let values = filtered
			.batch
			.column(0)
			.as_any()
			.downcast_ref::<Int64Array>()
			.unwrap();
		assert_eq!(values.values(), &[8, 9, 10, 11]);
	}

	#[test]
	fn filtered_predicate_page_batch_handles_non_byte_aligned_page_start() {
		let predicate = projected_i64_batch(0..12);
		let filtered = filtered_predicate_page_batch(
			&predicate,
			page(5, 4),
			&BooleanArray::from(vec![false, true, false, true]),
		)
		.unwrap();
		let values = filtered
			.batch
			.column(0)
			.as_any()
			.downcast_ref::<Int64Array>()
			.unwrap();
		assert_eq!(values.values(), &[6, 8]);
	}

	#[test]
	fn filtered_predicate_page_batch_preserves_zero_survivor_schema() {
		let predicate = projected_i64_batch(0..12);
		let filtered = filtered_predicate_page_batch(
			&predicate,
			page(4, 4),
			&BooleanArray::from(vec![false, false, false, false]),
		)
		.unwrap();
		assert_eq!(filtered.batch.num_rows(), 0);
		assert_eq!(filtered.batch.schema(), predicate.batch.schema());
		assert_eq!(filtered.ordinal_mapping, predicate.ordinal_mapping);
	}

	#[test]
	fn filtered_predicate_page_batch_slices_variable_width_predicate_column() {
		let batch = RecordBatch::try_new(
			Arc::new(Schema::new(vec![Field::new("tag", DataType::Utf8, false)])),
			vec![Arc::new(StringArray::from_iter_values([
				"a", "b", "c", "d", "e", "f",
			])) as ArrayRef],
		)
		.unwrap();
		let predicate = ProjectedRecordBatch {
			batch,
			ordinal_mapping: ColumnOrdinalMapping::new(vec![Some(0)]),
			storage_mapping: BTreeMap::new(),
		};
		let filtered = filtered_predicate_page_batch(
			&predicate,
			page(1, 4),
			&BooleanArray::from(vec![true, false, true, true]),
		)
		.unwrap();
		let values = filtered
			.batch
			.column(0)
			.as_any()
			.downcast_ref::<StringArray>()
			.unwrap();
		assert_eq!(values.value(0), "b");
		assert_eq!(values.value(1), "d");
		assert_eq!(values.value(2), "e");
	}

	#[test]
	fn filtered_predicate_page_batch_rejects_bad_page_mask_length() {
		let predicate = projected_i64_batch(0..12);
		let err = filtered_predicate_page_batch(
			&predicate,
			page(4, 4),
			&BooleanArray::from(vec![true, false]),
		)
		.unwrap_err();
		assert!(err.to_string().contains("does not match page row count"));
	}

	#[test]
	fn filtered_predicate_page_batch_rejects_page_span_outside_predicate_batch() {
		let predicate = projected_i64_batch(0..12);
		let err = filtered_predicate_page_batch(
			&predicate,
			page(10, 4),
			&BooleanArray::from(vec![true, true, true, true]),
		)
		.unwrap_err();
		assert!(
			err.to_string()
				.contains("exceeds predicate batch row count")
		);
	}

	#[test]
	fn validity_body_range_handles_non_byte_aligned_page_start() {
		let (range, bit_offset) = validity_bitmap_body_range_for_page(
			RecordBodyByteRange {
				start: RecordBodyByteOffset::new(100),
				len: RecordBodyByteLen::new(300),
			},
			page(1025, 1025),
		)
		.unwrap();
		assert_eq!(bit_offset, 1);
		assert_eq!(
			range,
			RecordBodyByteRange {
				start: RecordBodyByteOffset::new(228),
				len: RecordBodyByteLen::new(129),
			}
		);
	}

	#[test]
	fn validity_body_range_rejects_span_past_buffer_end() {
		let err = validity_bitmap_body_range_for_page(
			RecordBodyByteRange {
				start: RecordBodyByteOffset::new(100),
				len: RecordBodyByteLen::new(1),
			},
			page(8, 1),
		)
		.unwrap_err();
		assert!(err.to_string().contains("exceeds buffer length"));
	}

	#[test]
	fn rebase_validity_bitmap_shifts_to_page_local_zero() {
		let bitmap = rebase_validity_bitmap(&[0b0010_1101], 1, 4).unwrap();
		assert_eq!(bitmap.bytes, vec![0b0000_0110]);
		assert_eq!(bitmap.null_count, 2);
	}

	#[test]
	fn rebase_validity_bitmap_handles_byte_aligned_mixed_page() {
		let bitmap = rebase_validity_bitmap(&[0b0000_0101], 0, 4).unwrap();
		assert_eq!(bitmap.bytes, vec![0b0000_0101]);
		assert_eq!(bitmap.null_count, 2);
	}

	#[test]
	fn rebase_validity_bitmap_preserves_all_valid_page() {
		let bitmap = rebase_validity_bitmap(&[0b1111_1111], 3, 4).unwrap();
		assert_eq!(bitmap.bytes, vec![0b0000_1111]);
		assert_eq!(bitmap.null_count, 0);
	}

	#[test]
	fn rebase_validity_bitmap_counts_all_null_page() {
		let bitmap = rebase_validity_bitmap(&[0], 0, 4).unwrap();
		assert_eq!(bitmap.bytes, vec![0]);
		assert_eq!(bitmap.null_count, 4);
	}

	#[test]
	fn synthesize_all_null_page_buffers_zeroes_value_and_validity_buffers() {
		let (values, validity) = synthesize_all_null_page_buffers(9, 8).unwrap();
		assert_eq!(values, vec![0_u8; 72]);
		assert_eq!(validity.bytes, vec![0_u8; 2]);
		assert_eq!(validity.null_count, 9);
	}

	#[test]
	fn synthesize_all_null_page_buffers_handles_empty_page() {
		let (values, validity) = synthesize_all_null_page_buffers(0, 8).unwrap();
		assert!(values.is_empty());
		assert!(validity.bytes.is_empty());
		assert_eq!(validity.null_count, 0);
	}

	#[test]
	fn synthesize_all_null_page_buffers_rejects_value_length_overflow() {
		let err = synthesize_all_null_page_buffers(usize::MAX, 2).unwrap_err();
		assert!(err.to_string().contains("value byte length overflow"));
	}

	#[test]
	fn rebase_validity_bitmap_handles_bit_offset_seven_tail_page() {
		let bitmap = rebase_validity_bitmap(&[0b1000_0000, 0b0000_0010], 7, 3).unwrap();
		assert_eq!(bitmap.bytes, vec![0b0000_0101]);
		assert_eq!(bitmap.null_count, 1);
	}

	#[test]
	fn rebase_validity_bitmap_handles_alternating_nulls() {
		let bitmap = rebase_validity_bitmap(&[0b0101_0101], 0, 8).unwrap();
		assert_eq!(bitmap.bytes, vec![0b0101_0101]);
		assert_eq!(bitmap.null_count, 4);
	}

	#[test]
	fn rebase_validity_bitmap_handles_clustered_nulls() {
		let bitmap = rebase_validity_bitmap(&[0b1111_0000], 0, 8).unwrap();
		assert_eq!(bitmap.bytes, vec![0b1111_0000]);
		assert_eq!(bitmap.null_count, 4);
	}

	#[test]
	fn rebase_validity_bitmap_rejects_short_fetch() {
		let err = rebase_validity_bitmap(&[], 1, 1).unwrap_err();
		assert!(err.to_string().contains("required"));
	}
}
