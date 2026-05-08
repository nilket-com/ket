use crate::crypto::TieredPlaintextRange;
use crate::schema::{
	IpcBufferIndex, IpcBufferLength, IpcBufferOffset, IpcColumnOrdinal, RecordBodyByteLen,
	RecordBodyByteOffset, RowGroupIndex,
};
use crate::storage::columnar::{
	ColumnFetchRequirement, ColumnOrdinalMapping, IpcBufferRange, ProjectedRecordBatch,
	ProjectedRecordBufferSet, RecordBufferDirectory, StorageColumn,
	decode_projected_record_batch_from_buffers,
};
use crate::storage::encoding::cascade::decode_page;
use crate::storage::encoding::{BufferEncoding, EncodingCascade, EncodingPageDescriptor};
use crate::storage::row_group::RecordBodyByteRange;
use arrow::array::new_null_array;
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatchOptions;
use std::collections::BTreeMap;
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

pub(crate) fn fixed_width_non_null_value_column(
	directory: &RecordBufferDirectory,
	physical_ordinal: usize,
) -> Option<PageMaterializeColumn> {
	let field = directory.ipc_schema.fields().get(physical_ordinal)?;
	if directory
		.column_null_counts
		.get(physical_ordinal)
		.copied()
		.unwrap_or(usize::MAX)
		!= 0
	{
		return None;
	}
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
	})
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
			*null_count = 0;
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
