use super::{
	BufferEncoding, ENCODING_DIRECTORY_FORMAT_VERSION, ENCODING_DIRECTORY_MAGIC,
	EncodedBufferByteRange, EncodingCascade, EncodingCascadeKind, EncodingDirectory,
	EncodingLeafCascade, EncodingPageStats, EncodingRejectCode, EncodingWriterConfig,
};
use crate::crypto::SLAB_ENCRYPTION_DEFAULT_CHUNK_SIZE;
use crate::schema::ChunkSizeBytes;
use crate::schema::{
	BaseValue, BitsPerValue, BufferValueCount, DictValueCount, EncodingCascadeIndex,
	EncodingPageByteOffset, ExceptionCount, Exponent, Factor, IpcBufferIndex, IpcColumnOrdinal,
	LogicalSchemaFingerprint, PageNullCount, RecordBodyByteLen, RecordBodyByteOffset, RowCount,
	RowGroupIndex, RunCount, SimdPageValueCount, SlabId,
};
use crate::storage::columnar::{StorageColumn, TieredRecordId};
use crate::storage::encoding::cascade::estimator::FsstRowSafeEstimatorRouting;
use crate::storage::encoding::chunk_layout::{chunk_pad_for_cursor, default_sparse_engaged_key};
use crate::storage::encoding::compute::{EncodingComputeConfig, evaluator_kind_for_cascade};
use crate::storage::encoding::page::expand_page_descriptors;
use crate::storage::encoding::select::{
	ValuePageRowSpan, WriterCascadeContext, WriterValidityCascadeRouting,
	select_and_encode_buffer_with_value_offsets_and_context, select_validity_cascade,
};
use crate::storage::row_group::{
	BufferPageDirectoryEntry, RecordBodyByteRange, RecordPageDirectory, RowGroupDirectoryEntry,
	RowGroupWriterConfig,
};
use crate::storage::stats::{
	CanonicalScalar, arrow_stats_type_tag, canonical_stats_for_array_slice,
	fixed_width_page_stats_supported_type, read_canonical_scalar, write_canonical_scalar,
};
use arrow::datatypes::Schema;
use arrow::ipc::r#gen::Message::{
	BodyCompression, BodyCompressionArgs, FieldNode, Message as FbMessage, MessageArgs,
	RecordBatch as FbRecordBatch, RecordBatchArgs,
};
use arrow::ipc::r#gen::Schema::Buffer as FbBuffer;
use arrow::ipc::{MessageHeader, root_as_message};
use arrow::record_batch::RecordBatch;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppliedEncodingBuffer {
	pub row_group_index: RowGroupIndex,
	pub ipc_column_ordinal: IpcColumnOrdinal,
	pub ipc_buffer_ordinal: IpcBufferIndex,
	pub absolute_start: usize,
	pub decoded_len: usize,
	pub encoded_bytes: Vec<u8>,
}

pub fn build_encoding_directory_for_ipc_stream(
	record_id: TieredRecordId,
	ipc_bytes: &[u8],
	page_directory: &RecordPageDirectory,
	batches: &[RecordBatch],
	config: &EncodingWriterConfig,
	row_group_config: &RowGroupWriterConfig,
) -> std::io::Result<(Option<EncodingDirectory>, Vec<AppliedEncodingBuffer>)> {
	build_encoding_directory_for_ipc_stream_with_fsst_routing(
		record_id,
		ipc_bytes,
		page_directory,
		batches,
		config,
		row_group_config,
		FsstRowSafeEstimatorRouting::Estimator,
		WriterValidityCascadeRouting::Enabled,
	)
}

#[doc(hidden)]
pub fn build_encoding_directory_for_ipc_stream_exact_fsst_for_bench(
	record_id: TieredRecordId,
	ipc_bytes: &[u8],
	page_directory: &RecordPageDirectory,
	batches: &[RecordBatch],
	config: &EncodingWriterConfig,
	row_group_config: &RowGroupWriterConfig,
) -> std::io::Result<(Option<EncodingDirectory>, Vec<AppliedEncodingBuffer>)> {
	build_encoding_directory_for_ipc_stream_with_fsst_routing(
		record_id,
		ipc_bytes,
		page_directory,
		batches,
		config,
		row_group_config,
		FsstRowSafeEstimatorRouting::ExactOnly,
		WriterValidityCascadeRouting::Disabled,
	)
}

#[doc(hidden)]
#[allow(dead_code)]
pub(crate) fn build_encoding_directory_for_ipc_stream_with_validity_for_bench(
	record_id: TieredRecordId,
	ipc_bytes: &[u8],
	page_directory: &RecordPageDirectory,
	batches: &[RecordBatch],
	config: &EncodingWriterConfig,
	row_group_config: &RowGroupWriterConfig,
) -> std::io::Result<(Option<EncodingDirectory>, Vec<AppliedEncodingBuffer>)> {
	build_encoding_directory_for_ipc_stream_with_fsst_routing(
		record_id,
		ipc_bytes,
		page_directory,
		batches,
		config,
		row_group_config,
		FsstRowSafeEstimatorRouting::Estimator,
		WriterValidityCascadeRouting::Enabled,
	)
}

fn build_encoding_directory_for_ipc_stream_with_fsst_routing(
	record_id: TieredRecordId,
	ipc_bytes: &[u8],
	page_directory: &RecordPageDirectory,
	batches: &[RecordBatch],
	config: &EncodingWriterConfig,
	row_group_config: &RowGroupWriterConfig,
	fsst_routing: FsstRowSafeEstimatorRouting,
	validity_routing: WriterValidityCascadeRouting,
) -> std::io::Result<(Option<EncodingDirectory>, Vec<AppliedEncodingBuffer>)> {
	config.validate()?;
	if !config.enabled {
		return Ok((None, Vec::new()));
	}
	let Some(first_batch) = batches.first() else {
		return Ok((None, Vec::new()));
	};
	let schema = first_batch.schema();
	let mut cascades = Vec::<EncodingCascade>::new();
	let mut buffers = Vec::new();
	let mut applied = Vec::new();
	let mut writer_context = match fsst_routing {
		FsstRowSafeEstimatorRouting::Estimator => Some(WriterCascadeContext::default()),
		FsstRowSafeEstimatorRouting::ExactOnly => None,
	};
	for page in &page_directory.buffer_pages {
		let Some(group) = page_directory
			.row_groups
			.iter()
			.find(|group| group.row_group_index == page.row_group_index)
		else {
			continue;
		};
		if page.range.len.get() == 0 {
			continue;
		}
		if is_validity_buffer(page_directory, page) {
			continue;
		}
		let Some(field) = schema.fields().get(page.ipc_column_ordinal.get() as usize) else {
			continue;
		};
		let start = usize::try_from(page.range.start.get()).map_err(|_| {
			std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				"Encoding source buffer start exceeds usize",
			)
		})?;
		let len = usize::try_from(page.range.len.get()).map_err(|_| {
			std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				"Encoding source buffer length exceeds usize",
			)
		})?;
		let end = start.checked_add(len).ok_or_else(|| {
			std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				"Encoding source buffer overflow",
			)
		})?;
		if end > ipc_bytes.len() {
			return Err(std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				"Encoding source buffer exceeds IPC stream bytes",
			));
		}
		let decoded_value_count = BufferValueCount::new(group.row_count.get());
		let value_offsets = variable_width_value_offsets(
			ipc_bytes,
			page_directory,
			page,
			field.data_type(),
			group,
		)?;
		let context_column = writer_context.as_ref().map(|_| page.ipc_column_ordinal);
		let select_started = Instant::now();
		let encoded = select_and_encode_buffer_with_value_offsets_and_context(
			&ipc_bytes[start..end],
			field.data_type(),
			decoded_value_count,
			config,
			value_offsets.as_deref(),
			writer_context.as_mut(),
			context_column,
		)?;
		crate::metrics::global()
			.observe_storage_l2_encoding_cascade_select(select_started.elapsed());
		if encoded.cascade.kind() == EncodingCascadeKind::Plain {
			crate::metrics::global().observe_storage_l2_encoding_fallback("plain_buffer");
			continue;
		}
		if encoded.bytes.len() > len {
			crate::metrics::global().observe_storage_l2_encoding_fallback("too_many_bytes");
			continue;
		}
		let cascade_label = encoded.cascade.kind().as_str();
		crate::metrics::global().observe_storage_l2_encoding_buffer_encoding(cascade_label);
		crate::metrics::global().observe_storage_l2_encoding_compression_ratio(
			cascade_label,
			len as u64,
			encoded.bytes.len() as u64,
		);
		crate::metrics::global().observe_storage_l2_encoding_bytes("buffer", len as u64);
		crate::metrics::global()
			.observe_storage_l2_encoding_bytes("page", encoded.bytes.len() as u64);
		crate::metrics::global().observe_storage_l2_encoding_bytes(
			"avoided",
			len.saturating_sub(encoded.bytes.len()) as u64,
		);
		let cascade_index = match cascades
			.iter()
			.position(|cascade| *cascade == encoded.cascade)
		{
			Some(index) => index,
			None => {
				cascades.push(encoded.cascade.clone());
				cascades.len() - 1
			}
		};
		let cascade_index = u16::try_from(cascade_index).map_err(|_| {
			std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				"Encoding cascade index exceeds u16",
			)
		})?;
		let page_stats = build_page_stats_for_buffer(
			page_directory,
			batches,
			page,
			group,
			field.name(),
			field.data_type(),
			&encoded.cascade,
			decoded_value_count,
			config.target_simd_page_values,
			&encoded.page_end_offsets,
			row_group_config,
		)?;
		buffers.push(BufferEncoding {
			row_group_index: page.row_group_index,
			ipc_column_ordinal: page.ipc_column_ordinal,
			ipc_buffer_ordinal: page.ipc_buffer_ordinal,
			cascade_index: EncodingCascadeIndex::new(cascade_index),
			decoded_byte_len: encoded.decoded_byte_len,
			decoded_value_count,
			encoded_range: encoded.encoded_range(),
			page_value_count: config.target_simd_page_values,
			page_end_offsets: encoded.page_end_offsets.clone(),
			page_stats,
		});
		applied.push(AppliedEncodingBuffer {
			row_group_index: page.row_group_index,
			ipc_column_ordinal: page.ipc_column_ordinal,
			ipc_buffer_ordinal: page.ipc_buffer_ordinal,
			absolute_start: start,
			decoded_len: len,
			encoded_bytes: encoded.bytes,
		});
	}
	if matches!(validity_routing, WriterValidityCascadeRouting::Enabled)
		&& config.cascade_enabled(EncodingCascadeKind::Rle)
	{
		encode_validity_buffers(
			ipc_bytes,
			page_directory,
			batches,
			&mut cascades,
			&mut buffers,
			&mut applied,
		)?;
	}
	if buffers.is_empty() {
		return Ok((None, Vec::new()));
	}
	if cascades.is_empty() {
		cascades.push(EncodingCascade::Plain);
	}
	Ok((
		Some(EncodingDirectory {
			record_id,
			row_count: page_directory.row_count,
			logical_schema_fingerprint: page_directory.logical_schema_fingerprint,
			target_simd_page_values: config.target_simd_page_values,
			cascades,
			buffers,
		}),
		applied,
	))
}

fn encode_validity_buffers(
	ipc_bytes: &[u8],
	page_directory: &RecordPageDirectory,
	batches: &[RecordBatch],
	cascades: &mut Vec<EncodingCascade>,
	buffers: &mut Vec<BufferEncoding>,
	applied: &mut Vec<AppliedEncodingBuffer>,
) -> std::io::Result<()> {
	for validity_page in page_directory
		.buffer_pages
		.iter()
		.filter(|page| page.range.len.get() > 0 && is_validity_buffer(page_directory, page))
	{
		let Some(group) = page_directory
			.row_groups
			.iter()
			.find(|group| group.row_group_index == validity_page.row_group_index)
		else {
			continue;
		};
		let batch_index = usize::try_from(validity_page.row_group_index.get()).map_err(|_| {
			std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				"Encoding validity row group index exceeds usize",
			)
		})?;
		let Some(batch) = batches.get(batch_index) else {
			continue;
		};
		let column_index = validity_page.ipc_column_ordinal.get() as usize;
		let Some(column) = batch.columns().get(column_index) else {
			continue;
		};
		let column_null_count = column.null_count();
		if column_null_count == 0 {
			continue;
		}
		let Some(first_buffer_ordinal) =
			first_buffer_ordinal_for_column(page_directory, validity_page)
		else {
			continue;
		};
		let value_buffer_ordinal = IpcBufferIndex::new(first_buffer_ordinal.saturating_add(1));
		let Some(value_encoding) = buffers.iter().find(|encoding| {
			encoding.row_group_index == validity_page.row_group_index
				&& encoding.ipc_column_ordinal == validity_page.ipc_column_ordinal
				&& encoding.ipc_buffer_ordinal == value_buffer_ordinal
				&& !encoding.page_end_offsets.is_empty()
				&& encoding.decoded_value_count.get() == group.row_count.get()
		}) else {
			continue;
		};
		let value_page_value_count = value_encoding.page_value_count;
		let value_pages = expand_page_descriptors(value_encoding).map_err(|code| {
			std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				format!("Encoding validity value page descriptors rejected: {code:?}"),
			)
		})?;
		let value_page_row_spans = value_pages
			.iter()
			.map(|page| ValuePageRowSpan {
				first_value_index: page.first_value_index,
				value_count: page.value_count,
			})
			.collect::<Vec<_>>();
		let start = usize::try_from(validity_page.range.start.get()).map_err(|_| {
			std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				"Encoding validity source buffer start exceeds usize",
			)
		})?;
		let len = usize::try_from(validity_page.range.len.get()).map_err(|_| {
			std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				"Encoding validity source buffer length exceeds usize",
			)
		})?;
		let end = start.checked_add(len).ok_or_else(|| {
			std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				"Encoding validity source buffer overflow",
			)
		})?;
		if end > ipc_bytes.len() {
			return Err(std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				"Encoding validity source buffer exceeds IPC stream bytes",
			));
		}
		let Some(plan) = select_validity_cascade(
			&ipc_bytes[start..end],
			&value_page_row_spans,
			column_null_count,
		) else {
			continue;
		};
		let cascade = EncodingCascade::Rle {
			run_count: plan.run_count,
			inner: Box::new(EncodingLeafCascade::Plain),
		};
		let cascade_index = cascade_index_for(cascades, &cascade)?;
		let cascade_label = cascade.kind().as_str();
		crate::metrics::global().observe_storage_l2_encoding_buffer_encoding(cascade_label);
		crate::metrics::global().observe_storage_l2_encoding_compression_ratio(
			cascade_label,
			plan.raw_byte_len as u64,
			plan.encoded_bytes.len() as u64,
		);
		crate::metrics::global()
			.observe_storage_l2_encoding_bytes("buffer", plan.raw_byte_len as u64);
		crate::metrics::global()
			.observe_storage_l2_encoding_bytes("page", plan.encoded_bytes.len() as u64);
		crate::metrics::global().observe_storage_l2_encoding_bytes(
			"avoided",
			plan.raw_byte_len.saturating_sub(plan.encoded_bytes.len()) as u64,
		);
		buffers.push(BufferEncoding {
			row_group_index: validity_page.row_group_index,
			ipc_column_ordinal: validity_page.ipc_column_ordinal,
			ipc_buffer_ordinal: validity_page.ipc_buffer_ordinal,
			cascade_index,
			decoded_byte_len: RecordBodyByteLen::new(plan.raw_byte_len as u64),
			decoded_value_count: BufferValueCount::new(group.row_count.get()),
			encoded_range: EncodedBufferByteRange {
				start: EncodingPageByteOffset::ZERO,
				len: RecordBodyByteLen::new(plan.encoded_bytes.len() as u64),
			},
			page_value_count: value_page_value_count,
			page_end_offsets: plan.page_end_offsets,
			page_stats: Vec::new(),
		});
		applied.push(AppliedEncodingBuffer {
			row_group_index: validity_page.row_group_index,
			ipc_column_ordinal: validity_page.ipc_column_ordinal,
			ipc_buffer_ordinal: validity_page.ipc_buffer_ordinal,
			absolute_start: start,
			decoded_len: len,
			encoded_bytes: plan.encoded_bytes,
		});
	}
	Ok(())
}

fn cascade_index_for(
	cascades: &mut Vec<EncodingCascade>,
	cascade: &EncodingCascade,
) -> std::io::Result<EncodingCascadeIndex> {
	let index = match cascades.iter().position(|candidate| candidate == cascade) {
		Some(index) => index,
		None => {
			cascades.push(cascade.clone());
			cascades.len() - 1
		}
	};
	let index = u16::try_from(index).map_err(|_| {
		std::io::Error::new(
			std::io::ErrorKind::InvalidData,
			"Encoding cascade index exceeds u16",
		)
	})?;
	Ok(EncodingCascadeIndex::new(index))
}

fn variable_width_value_offsets(
	ipc_bytes: &[u8],
	page_directory: &RecordPageDirectory,
	page: &BufferPageDirectoryEntry,
	data_type: &arrow::datatypes::DataType,
	group: &RowGroupDirectoryEntry,
) -> std::io::Result<Option<Vec<u64>>> {
	let Some(offset_width) = variable_width_offset_width(data_type) else {
		return Ok(None);
	};
	let first_buffer_ordinal = page_directory
		.buffer_pages
		.iter()
		.filter(|candidate| {
			candidate.row_group_index == page.row_group_index
				&& candidate.ipc_column_ordinal == page.ipc_column_ordinal
		})
		.map(|candidate| candidate.ipc_buffer_ordinal.get())
		.min();
	let Some(first_buffer_ordinal) = first_buffer_ordinal else {
		return Ok(None);
	};
	if page.ipc_buffer_ordinal.get() != first_buffer_ordinal.saturating_add(2) {
		return Ok(None);
	}
	let offset_buffer_ordinal = IpcBufferIndex::new(first_buffer_ordinal.saturating_add(1));
	let Some(offset_page) = page_directory.buffer_pages.iter().find(|candidate| {
		candidate.row_group_index == page.row_group_index
			&& candidate.ipc_column_ordinal == page.ipc_column_ordinal
			&& candidate.ipc_buffer_ordinal == offset_buffer_ordinal
	}) else {
		return Ok(None);
	};
	let start = usize::try_from(offset_page.range.start.get()).map_err(|_| {
		std::io::Error::new(
			std::io::ErrorKind::InvalidData,
			"Encoding variable-width offset buffer start exceeds usize",
		)
	})?;
	let len = usize::try_from(offset_page.range.len.get()).map_err(|_| {
		std::io::Error::new(
			std::io::ErrorKind::InvalidData,
			"Encoding variable-width offset buffer length exceeds usize",
		)
	})?;
	let end = start.checked_add(len).ok_or_else(|| {
		std::io::Error::new(
			std::io::ErrorKind::InvalidData,
			"Encoding variable-width offset buffer range overflow",
		)
	})?;
	if end > ipc_bytes.len() {
		return Err(std::io::Error::new(
			std::io::ErrorKind::InvalidData,
			"Encoding variable-width offset buffer exceeds IPC stream",
		));
	}
	let expected_offsets = group.row_count.get() as usize + 1;
	if len != expected_offsets.saturating_mul(offset_width) {
		return Ok(None);
	}
	let mut offsets = Vec::with_capacity(expected_offsets);
	let mut previous = 0_u64;
	for raw in ipc_bytes[start..end].chunks_exact(offset_width) {
		let offset = match offset_width {
			4 => {
				let value = i32::from_le_bytes(raw.try_into().expect("fixed width"));
				u64::try_from(value).map_err(|_| {
					std::io::Error::new(
						std::io::ErrorKind::InvalidData,
						"Encoding variable-width offset is negative",
					)
				})?
			}
			8 => {
				let value = i64::from_le_bytes(raw.try_into().expect("fixed width"));
				u64::try_from(value).map_err(|_| {
					std::io::Error::new(
						std::io::ErrorKind::InvalidData,
						"Encoding variable-width offset is negative",
					)
				})?
			}
			_ => unreachable!("variable-width offset width is fixed"),
		};
		if offset < previous {
			return Ok(None);
		}
		previous = offset;
		offsets.push(offset);
	}
	if offsets.last().copied() != Some(page.range.len.get()) {
		return Ok(None);
	}
	Ok(Some(offsets))
}

fn variable_width_offset_width(data_type: &arrow::datatypes::DataType) -> Option<usize> {
	match data_type {
		arrow::datatypes::DataType::Utf8 | arrow::datatypes::DataType::Binary => Some(4),
		arrow::datatypes::DataType::LargeUtf8 | arrow::datatypes::DataType::LargeBinary => Some(8),
		_ => None,
	}
}

fn build_page_stats_for_buffer(
	page_directory: &RecordPageDirectory,
	batches: &[RecordBatch],
	page: &BufferPageDirectoryEntry,
	group: &RowGroupDirectoryEntry,
	field_name: &str,
	data_type: &arrow::datatypes::DataType,
	cascade: &EncodingCascade,
	decoded_value_count: BufferValueCount,
	page_value_count: SimdPageValueCount,
	page_end_offsets: &[EncodingPageByteOffset],
	row_group_config: &RowGroupWriterConfig,
) -> std::io::Result<Vec<EncodingPageStats>> {
	if page_end_offsets.is_empty()
		|| StorageColumn::from_name(field_name).is_some()
		|| !row_group_config.stats_supported_types.contains(data_type)
		|| !fixed_width_page_stats_supported_type(data_type)
		|| !page_stats_cascade_eligible(cascade)
		|| !is_fixed_width_value_buffer(page_directory, page)
	{
		return Ok(Vec::new());
	}
	let batch_index = usize::try_from(page.row_group_index.get()).map_err(|_| {
		std::io::Error::new(
			std::io::ErrorKind::InvalidData,
			"Encoding page-stats row group index exceeds usize",
		)
	})?;
	let Some(batch) = batches.get(batch_index) else {
		return Ok(Vec::new());
	};
	if batch.num_rows() != group.row_count.get() as usize {
		return Ok(Vec::new());
	}
	let column_index = page.ipc_column_ordinal.get() as usize;
	if column_index >= batch.num_columns() {
		return Ok(Vec::new());
	}
	let column = batch.column(column_index);
	let page_rows = page_value_count.get();
	if page_rows == 0 {
		return Ok(Vec::new());
	}
	let mut stats = Vec::with_capacity(page_end_offsets.len());
	for page_index in 0..page_end_offsets.len() {
		let page_index = u32::try_from(page_index).map_err(|_| {
			std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				"Encoding page-stats page index exceeds u32",
			)
		})?;
		let first_row = u64::from(page_index)
			.checked_mul(u64::from(page_rows))
			.ok_or_else(|| {
				std::io::Error::new(
					std::io::ErrorKind::InvalidData,
					"Encoding page-stats row offset overflow",
				)
			})?;
		let remaining = u64::from(decoded_value_count.get()).saturating_sub(first_row);
		if remaining == 0 {
			return Ok(Vec::new());
		}
		let len = remaining.min(u64::from(page_rows));
		let first_row = usize::try_from(first_row).map_err(|_| {
			std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				"Encoding page-stats row offset exceeds usize",
			)
		})?;
		let len = usize::try_from(len).map_err(|_| {
			std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				"Encoding page-stats row count exceeds usize",
			)
		})?;
		let Some(values) =
			canonical_stats_for_array_slice(data_type, column.as_ref(), first_row, len)?
		else {
			return Ok(Vec::new());
		};
		let null_count = u32::try_from(values.null_count).map_err(|_| {
			std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				"Encoding page-stats null count exceeds u32",
			)
		})?;
		stats.push(EncodingPageStats {
			null_count: PageNullCount::new(null_count),
			min: values.min,
			max: values.max,
		});
	}
	Ok(stats)
}

fn is_fixed_width_value_buffer(
	page_directory: &RecordPageDirectory,
	page: &BufferPageDirectoryEntry,
) -> bool {
	first_buffer_ordinal_for_column(page_directory, page)
		.map(|first| page.ipc_buffer_ordinal.get() == first.saturating_add(1))
		.unwrap_or(false)
}

fn is_validity_buffer(
	page_directory: &RecordPageDirectory,
	page: &BufferPageDirectoryEntry,
) -> bool {
	first_buffer_ordinal_for_column(page_directory, page)
		.map(|first| page.ipc_buffer_ordinal.get() == first)
		.unwrap_or(false)
}

fn first_buffer_ordinal_for_column(
	page_directory: &RecordPageDirectory,
	page: &BufferPageDirectoryEntry,
) -> Option<u32> {
	page_directory
		.buffer_pages
		.iter()
		.filter(|candidate| {
			candidate.row_group_index == page.row_group_index
				&& candidate.ipc_column_ordinal == page.ipc_column_ordinal
		})
		.map(|candidate| candidate.ipc_buffer_ordinal.get())
		.min()
}

pub fn page_stats_cascade_eligible(cascade: &EncodingCascade) -> bool {
	if !cascade.emits_pages_in_v1() {
		return false;
	}
	let Some(kind) = evaluator_kind_for_cascade(cascade) else {
		return false;
	};
	EncodingComputeConfig::default()
		.enabled_predicate_evaluators
		.contains(&kind)
}

pub fn apply_encoding_buffers_in_place(ipc_bytes: &mut [u8], applied: &[AppliedEncodingBuffer]) {
	for buffer in applied {
		let end = buffer.absolute_start + buffer.decoded_len;
		let encoded_end = buffer.absolute_start + buffer.encoded_bytes.len();
		ipc_bytes[buffer.absolute_start..encoded_end].copy_from_slice(&buffer.encoded_bytes);
		for byte in &mut ipc_bytes[encoded_end..end] {
			*byte = 0;
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompactedEncodingIpcStream {
	pub bytes: Vec<u8>,
	pub page_directory: RecordPageDirectory,
	pub encoding_directory: EncodingDirectory,
	pub chunk_alignment_pad_bytes: usize,
}

struct PendingCompactBuffer<'a> {
	ipc_column_ordinal: IpcColumnOrdinal,
	ipc_buffer_ordinal: IpcBufferIndex,
	physical_bytes: &'a [u8],
}

struct BuiltCompactBody {
	body: Vec<u8>,
	rewritten_buffers: Vec<FbBuffer>,
	pending_pages: Vec<(IpcColumnOrdinal, IpcBufferIndex, usize, usize)>,
	chunk_alignment_pad_bytes: usize,
}

pub fn compact_encoding_ipc_stream(
	ipc_bytes: &[u8],
	page_directory: &RecordPageDirectory,
	encoding_directory: &EncodingDirectory,
	applied: &[AppliedEncodingBuffer],
	chunk_size_bytes: ChunkSizeBytes,
) -> std::io::Result<CompactedEncodingIpcStream> {
	let applied_by_key = applied
		.iter()
		.map(|buffer| {
			(
				(
					buffer.row_group_index,
					buffer.ipc_column_ordinal,
					buffer.ipc_buffer_ordinal,
				),
				buffer,
			)
		})
		.collect::<BTreeMap<_, _>>();

	let mut physical_encoding_directory = encoding_directory.clone();
	for encoding in &mut physical_encoding_directory.buffers {
		let key = (
			encoding.row_group_index,
			encoding.ipc_column_ordinal,
			encoding.ipc_buffer_ordinal,
		);
		let applied = applied_by_key.get(&key).ok_or_else(|| {
			std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				"Encoding directory buffer missing applied encoded bytes",
			)
		})?;
		encoding.encoded_range.start = EncodingPageByteOffset::ZERO;
		encoding.encoded_range.len = RecordBodyByteLen::try_new(
			u64::try_from(applied.encoded_bytes.len()).map_err(|_| {
				std::io::Error::new(
					std::io::ErrorKind::InvalidData,
					"encoded buffer length exceeds u64",
				)
			})?,
		)?;
	}

	let mut offset = 0_usize;
	let schema_message = read_compact_ipc_message(ipc_bytes, &mut offset)?.ok_or_else(|| {
		std::io::Error::new(
			std::io::ErrorKind::InvalidData,
			"Encoding compact IPC stream ended before schema message",
		)
	})?;
	if schema_message.header_type != MessageHeader::Schema {
		return Err(std::io::Error::new(
			std::io::ErrorKind::InvalidData,
			"Encoding compact IPC stream did not start with schema message",
		));
	}
	if schema_message.body_len != 0 {
		return Err(std::io::Error::new(
			std::io::ErrorKind::InvalidData,
			"Encoding compact schema message unexpectedly has a body",
		));
	}

	let mut out = Vec::with_capacity(ipc_bytes.len());
	out.extend_from_slice(&ipc_bytes[schema_message.prefix_start..schema_message.body_end]);
	let mut physical_row_groups = Vec::with_capacity(page_directory.row_groups.len());
	let mut physical_buffer_pages = Vec::with_capacity(page_directory.buffer_pages.len());
	let align_chunk_starts = chunk_size_bytes != SLAB_ENCRYPTION_DEFAULT_CHUNK_SIZE;
	let chunk_size = chunk_size_bytes.get() as usize;
	let mut chunk_alignment_pad_bytes = 0_usize;
	for group in &page_directory.row_groups {
		let message = read_compact_ipc_message(ipc_bytes, &mut offset)?.ok_or_else(|| {
			std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				"Encoding compact IPC stream ended before expected record batch",
			)
		})?;
		if message.header_type != MessageHeader::RecordBatch {
			return Err(std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				"Encoding compact IPC stream contained non-record-batch message",
			));
		}
		let batch = message.message.header_as_record_batch().ok_or_else(|| {
			std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				"Encoding compact record batch missing header",
			)
		})?;
		let flat_buffers = batch.buffers().ok_or_else(|| {
			std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				"Encoding compact record batch missing buffers",
			)
		})?;
		let mut pending_buffers = Vec::with_capacity(flat_buffers.len());
		for (buffer_ordinal, buffer) in flat_buffers.iter().enumerate() {
			let buffer_ordinal =
				IpcBufferIndex::new(u32::try_from(buffer_ordinal).map_err(|_| {
					std::io::Error::new(
						std::io::ErrorKind::InvalidData,
						"IPC buffer ordinal exceeds u32",
					)
				})?);
			let body_offset = usize::try_from(buffer.offset()).map_err(|_| {
				std::io::Error::new(
					std::io::ErrorKind::InvalidData,
					"negative IPC buffer offset",
				)
			})?;
			let decoded_len = usize::try_from(buffer.length()).map_err(|_| {
				std::io::Error::new(
					std::io::ErrorKind::InvalidData,
					"negative IPC buffer length",
				)
			})?;
			let dense_start = message.body_start.checked_add(body_offset).ok_or_else(|| {
				std::io::Error::new(
					std::io::ErrorKind::InvalidData,
					"IPC buffer absolute start overflow",
				)
			})?;
			let dense_end = dense_start.checked_add(decoded_len).ok_or_else(|| {
				std::io::Error::new(
					std::io::ErrorKind::InvalidData,
					"IPC buffer absolute end overflow",
				)
			})?;
			if dense_end > ipc_bytes.len() {
				return Err(std::io::Error::new(
					std::io::ErrorKind::InvalidData,
					"IPC buffer exceeds dense stream",
				));
			}
			let page = page_directory
				.buffer_pages
				.iter()
				.find(|page| {
					page.row_group_index == group.row_group_index
						&& page.ipc_buffer_ordinal == buffer_ordinal
				})
				.ok_or_else(|| {
					std::io::Error::new(
						std::io::ErrorKind::InvalidData,
						"Encoding compact buffer missing row-group directory entry",
					)
				})?;
			if usize::try_from(page.range.start.get()).ok() != Some(dense_start)
				|| usize::try_from(page.range.len.get()).ok() != Some(decoded_len)
			{
				return Err(std::io::Error::new(
					std::io::ErrorKind::InvalidData,
					"Encoding compact row-group directory does not match dense IPC buffers",
				));
			}
			let key = (
				group.row_group_index,
				page.ipc_column_ordinal,
				buffer_ordinal,
			);
			let physical_bytes = if let Some(applied) = applied_by_key.get(&key) {
				if applied.absolute_start != dense_start || applied.decoded_len != decoded_len {
					return Err(std::io::Error::new(
						std::io::ErrorKind::InvalidData,
						"Encoding compact applied buffer does not match dense IPC buffer",
					));
				}
				applied.encoded_bytes.as_slice()
			} else {
				&ipc_bytes[dense_start..dense_end]
			};
			pending_buffers.push(PendingCompactBuffer {
				ipc_column_ordinal: page.ipc_column_ordinal,
				ipc_buffer_ordinal: buffer_ordinal,
				physical_bytes,
			});
		}
		let prefix_start = out.len();
		let mut body_start_hint = message.body_start;
		let mut stabilized = None;
		for _ in 0..8 {
			let built = build_compact_body_for_record_batch(
				&pending_buffers,
				&physical_encoding_directory,
				group.row_group_index,
				align_chunk_starts,
				chunk_size,
				body_start_hint,
			)?;
			let metadata = build_record_batch_message(
				&message,
				batch,
				&built.rewritten_buffers,
				i64::try_from(built.body.len()).map_err(|_| {
					std::io::Error::new(
						std::io::ErrorKind::InvalidData,
						"physical IPC body length exceeds i64",
					)
				})?,
			)?;
			let metadata_len = aligned_metadata_len(metadata.len())?;
			let body_start = prefix_start
				.checked_add(8)
				.and_then(|start| start.checked_add(metadata_len))
				.ok_or_else(|| {
					std::io::Error::new(
						std::io::ErrorKind::InvalidData,
						"physical IPC body start overflow",
					)
				})?;
			if !align_chunk_starts || body_start == body_start_hint {
				stabilized = Some((built, metadata, body_start));
				break;
			}
			body_start_hint = body_start;
		}
		let Some((built, metadata, body_start)) = stabilized else {
			return Err(std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				"Encoding compact chunk-aligned body start did not stabilize",
			));
		};
		chunk_alignment_pad_bytes = chunk_alignment_pad_bytes
			.checked_add(built.chunk_alignment_pad_bytes)
			.ok_or_else(|| {
				std::io::Error::new(
					std::io::ErrorKind::InvalidData,
					"Encoding compact chunk-alignment pad total overflow",
				)
			})?;
		append_ipc_message(&mut out, &metadata, &built.body)?;
		physical_row_groups.push(RowGroupDirectoryEntry {
			metadata_range: RecordBodyByteRange {
				start: RecordBodyByteOffset::try_new(prefix_start as u64)?,
				len: RecordBodyByteLen::try_new((body_start - prefix_start) as u64)?,
			},
			body_range: RecordBodyByteRange {
				start: RecordBodyByteOffset::try_new(body_start as u64)?,
				len: RecordBodyByteLen::try_new(built.body.len() as u64)?,
			},
			..*group
		});
		for (ipc_column_ordinal, ipc_buffer_ordinal, physical_offset, physical_len) in
			built.pending_pages
		{
			let start = body_start.checked_add(physical_offset).ok_or_else(|| {
				std::io::Error::new(
					std::io::ErrorKind::InvalidData,
					"physical buffer start overflow",
				)
			})?;
			physical_buffer_pages.push(BufferPageDirectoryEntry {
				row_group_index: group.row_group_index,
				ipc_column_ordinal,
				ipc_buffer_ordinal,
				range: RecordBodyByteRange {
					start: RecordBodyByteOffset::try_new(start as u64)?,
					len: RecordBodyByteLen::try_new(physical_len as u64)?,
				},
			});
		}
	}
	append_ipc_stream_end(&mut out);
	Ok(CompactedEncodingIpcStream {
		bytes: out,
		page_directory: RecordPageDirectory {
			record_id: page_directory.record_id,
			row_count: page_directory.row_count,
			row_groups: physical_row_groups,
			buffer_pages: physical_buffer_pages,
			row_group_stats: page_directory.row_group_stats.clone(),
			logical_schema_fingerprint: page_directory.logical_schema_fingerprint,
		},
		encoding_directory: physical_encoding_directory,
		chunk_alignment_pad_bytes,
	})
}

fn build_compact_body_for_record_batch(
	pending_buffers: &[PendingCompactBuffer<'_>],
	physical_encoding_directory: &EncodingDirectory,
	row_group_index: RowGroupIndex,
	align_chunk_starts: bool,
	chunk_size: usize,
	output_body_start: usize,
) -> std::io::Result<BuiltCompactBody> {
	let mut body = Vec::new();
	let mut rewritten_buffers = Vec::with_capacity(pending_buffers.len());
	let mut pending_pages = Vec::with_capacity(pending_buffers.len());
	let mut chunk_alignment_pad_bytes = 0_usize;
	for buffer in pending_buffers {
		if align_chunk_starts
			&& default_sparse_engaged_key(
				physical_encoding_directory,
				row_group_index,
				buffer.ipc_column_ordinal,
				buffer.ipc_buffer_ordinal,
			) {
			let cursor = output_body_start.checked_add(body.len()).ok_or_else(|| {
				std::io::Error::new(
					std::io::ErrorKind::InvalidData,
					"Encoding compact chunk-alignment cursor overflow",
				)
			})?;
			let pad = chunk_pad_for_cursor(cursor, chunk_size);
			chunk_alignment_pad_bytes =
				chunk_alignment_pad_bytes.checked_add(pad).ok_or_else(|| {
					std::io::Error::new(
						std::io::ErrorKind::InvalidData,
						"Encoding compact chunk-alignment pad total overflow",
					)
				})?;
			let new_len = body.len().checked_add(pad).ok_or_else(|| {
				std::io::Error::new(
					std::io::ErrorKind::InvalidData,
					"Encoding compact chunk-alignment body overflow",
				)
			})?;
			body.resize(new_len, 0);
		}
		let physical_offset = body.len();
		body.extend_from_slice(buffer.physical_bytes);
		let physical_len = buffer.physical_bytes.len();
		rewritten_buffers.push(FbBuffer::new(
			i64::try_from(physical_offset).map_err(|_| {
				std::io::Error::new(
					std::io::ErrorKind::InvalidData,
					"physical IPC buffer offset exceeds i64",
				)
			})?,
			i64::try_from(physical_len).map_err(|_| {
				std::io::Error::new(
					std::io::ErrorKind::InvalidData,
					"physical IPC buffer length exceeds i64",
				)
			})?,
		));
		pending_pages.push((
			buffer.ipc_column_ordinal,
			buffer.ipc_buffer_ordinal,
			physical_offset,
			physical_len,
		));
		pad_to_8(&mut body);
	}

	Ok(BuiltCompactBody {
		body,
		rewritten_buffers,
		pending_pages,
		chunk_alignment_pad_bytes,
	})
}

pub fn serialize_encoding_directory_value(
	directory: &EncodingDirectory,
) -> std::io::Result<String> {
	let mut out = Vec::new();
	out.extend_from_slice(&ENCODING_DIRECTORY_MAGIC);
	out.extend_from_slice(&ENCODING_DIRECTORY_FORMAT_VERSION.to_be_bytes());
	write_u64(&mut out, directory.record_id.slab_id.get());
	write_u64(&mut out, directory.record_id.record_offset.get());
	write_u64(&mut out, directory.row_count.get());
	out.extend_from_slice(directory.logical_schema_fingerprint.as_bytes());
	write_u32(&mut out, directory.target_simd_page_values.get());
	write_u32(&mut out, checked_len(directory.cascades.len())?);
	write_u32(&mut out, checked_len(directory.buffers.len())?);
	for cascade in &directory.cascades {
		write_cascade(&mut out, cascade)?;
	}
	for buffer in &directory.buffers {
		write_u32(&mut out, buffer.row_group_index.get());
		write_u32(&mut out, buffer.ipc_column_ordinal.get());
		write_u32(&mut out, buffer.ipc_buffer_ordinal.get());
		write_u16(&mut out, buffer.cascade_index.get());
		write_u64(&mut out, buffer.decoded_byte_len.get());
		write_u32(&mut out, buffer.decoded_value_count.get());
		write_u32(&mut out, buffer.encoded_range.start.get());
		write_u64(&mut out, buffer.encoded_range.len.get());
		write_u32(&mut out, buffer.page_value_count.get());
		write_u32(&mut out, checked_len(buffer.page_end_offsets.len())?);
		for offset in &buffer.page_end_offsets {
			write_u32(&mut out, offset.get());
		}
		write_u32(&mut out, checked_len(buffer.page_stats.len())?);
		for stat in &buffer.page_stats {
			write_u32(&mut out, stat.null_count.get());
			write_canonical_scalar(&mut out, stat.min.as_ref())?;
			write_canonical_scalar(&mut out, stat.max.as_ref())?;
		}
	}
	Ok(BASE64.encode(out))
}

pub fn deserialize_encoding_directory_value(
	value: &str,
) -> Result<EncodingDirectory, EncodingRejectCode> {
	let bytes = BASE64
		.decode(value)
		.map_err(|_| EncodingRejectCode::EncodingDirectoryParseError)?;
	let mut cursor = 0_usize;
	let magic = read_exact(&bytes, &mut cursor, ENCODING_DIRECTORY_MAGIC.len())?;
	if magic != ENCODING_DIRECTORY_MAGIC {
		return Err(EncodingRejectCode::EncodingDirectoryParseError);
	}
	let version = read_u32(&bytes, &mut cursor)?;
	if version != ENCODING_DIRECTORY_FORMAT_VERSION {
		return Err(EncodingRejectCode::EncodingDirectoryParseError);
	}
	let slab_id = SlabId::new(read_u64(&bytes, &mut cursor)?);
	let record_offset = RecordBodyByteOffset::new(read_u64(&bytes, &mut cursor)?);
	let row_count = RowCount::new(read_u64(&bytes, &mut cursor)?);
	let fingerprint = read_exact(&bytes, &mut cursor, 32)?;
	let mut fingerprint_bytes = [0_u8; 32];
	fingerprint_bytes.copy_from_slice(fingerprint);
	let target_simd_page_values = SimdPageValueCount::new(read_u32(&bytes, &mut cursor)?);
	let cascade_count = read_count(&bytes, &mut cursor)?;
	let buffer_count = read_count(&bytes, &mut cursor)?;
	let mut cascades = Vec::with_capacity(cascade_count);
	for _ in 0..cascade_count {
		cascades.push(read_cascade(&bytes, &mut cursor)?);
	}
	let mut buffers = Vec::with_capacity(buffer_count);
	for _ in 0..buffer_count {
		let row_group_index = RowGroupIndex::new(read_u32(&bytes, &mut cursor)?);
		let ipc_column_ordinal = IpcColumnOrdinal::new(read_u32(&bytes, &mut cursor)?);
		let ipc_buffer_ordinal = IpcBufferIndex::new(read_u32(&bytes, &mut cursor)?);
		let cascade_index = EncodingCascadeIndex::new(read_u16(&bytes, &mut cursor)?);
		let decoded_byte_len = RecordBodyByteLen::try_new(read_u64(&bytes, &mut cursor)?)
			.map_err(|_| EncodingRejectCode::PageRangeOverflow)?;
		let decoded_value_count = BufferValueCount::new(read_u32(&bytes, &mut cursor)?);
		let start = EncodingPageByteOffset::new(read_u32(&bytes, &mut cursor)?);
		let len = RecordBodyByteLen::try_new(read_u64(&bytes, &mut cursor)?)
			.map_err(|_| EncodingRejectCode::PageRangeOverflow)?;
		let page_value_count = SimdPageValueCount::new(read_u32(&bytes, &mut cursor)?);
		let page_count = read_count(&bytes, &mut cursor)?;
		let mut page_end_offsets = Vec::with_capacity(page_count);
		for _ in 0..page_count {
			page_end_offsets.push(EncodingPageByteOffset::new(read_u32(&bytes, &mut cursor)?));
		}
		let page_stats_count = read_count(&bytes, &mut cursor)?;
		let mut page_stats = Vec::with_capacity(page_stats_count);
		for _ in 0..page_stats_count {
			page_stats.push(EncodingPageStats {
				null_count: PageNullCount::new(read_u32(&bytes, &mut cursor)?),
				min: read_encoding_scalar(&bytes, &mut cursor)?,
				max: read_encoding_scalar(&bytes, &mut cursor)?,
			});
		}
		buffers.push(BufferEncoding {
			row_group_index,
			ipc_column_ordinal,
			ipc_buffer_ordinal,
			cascade_index,
			decoded_byte_len,
			decoded_value_count,
			encoded_range: EncodedBufferByteRange { start, len },
			page_value_count,
			page_end_offsets,
			page_stats,
		});
	}
	if cursor != bytes.len() {
		return Err(EncodingRejectCode::EncodingDirectoryParseError);
	}
	Ok(EncodingDirectory {
		record_id: TieredRecordId {
			slab_id,
			record_offset,
		},
		row_count,
		logical_schema_fingerprint: LogicalSchemaFingerprint::from_bytes(fingerprint_bytes),
		target_simd_page_values,
		cascades,
		buffers,
	})
}

pub fn validate_encoding_directory(
	directory: &EncodingDirectory,
	page_directory: &RecordPageDirectory,
	schema: &Schema,
	expected_fingerprint: LogicalSchemaFingerprint,
) -> Result<(), EncodingRejectCode> {
	if directory.logical_schema_fingerprint != expected_fingerprint {
		return Err(EncodingRejectCode::EncodingDirectoryParseError);
	}
	if directory.row_count != page_directory.row_count {
		return Err(EncodingRejectCode::BufferEncodingMismatch);
	}
	if directory.target_simd_page_values.get() == 0 || directory.cascades.is_empty() {
		return Err(EncodingRejectCode::EncodingDirectoryParseError);
	}
	if directory.cascades.len() > u16::MAX as usize {
		return Err(EncodingRejectCode::EncodingDirectoryParseError);
	}
	let mut seen = BTreeSet::new();
	for buffer in &directory.buffers {
		if buffer.cascade_index.get() as usize >= directory.cascades.len() {
			return Err(EncodingRejectCode::CascadeUnsupported);
		}
		if !seen.insert((
			buffer.row_group_index,
			buffer.ipc_column_ordinal,
			buffer.ipc_buffer_ordinal,
		)) {
			return Err(EncodingRejectCode::EncodingDirectoryParseError);
		}
		if buffer.ipc_column_ordinal.get() as usize >= schema.fields().len() {
			return Err(EncodingRejectCode::BufferEncodingMismatch);
		}
		let field = schema
			.fields()
			.get(buffer.ipc_column_ordinal.get() as usize)
			.ok_or(EncodingRejectCode::BufferEncodingMismatch)?;
		let Some(page) = page_directory.buffer_pages.iter().find(|page| {
			page.row_group_index == buffer.row_group_index
				&& page.ipc_column_ordinal == buffer.ipc_column_ordinal
				&& page.ipc_buffer_ordinal == buffer.ipc_buffer_ordinal
		}) else {
			return Err(EncodingRejectCode::BufferEncodingMismatch);
		};
		let encoded_end = buffer
			.encoded_range
			.end()
			.map_err(|_| EncodingRejectCode::PageRangeOverflow)?;
		if encoded_end > page.range.len.get() {
			return Err(EncodingRejectCode::PageRangeOverflow);
		}
		if encoded_end > buffer.decoded_byte_len.get() {
			return Err(EncodingRejectCode::PageRangeOverflow);
		}
		validate_page_offsets(
			buffer,
			&directory.cascades[buffer.cascade_index.get() as usize],
		)?;
		validate_page_stats(
			buffer,
			&directory.cascades[buffer.cascade_index.get() as usize],
			field.data_type(),
			page_directory,
		)?;
	}
	Ok(())
}

fn validate_page_offsets(
	buffer: &BufferEncoding,
	cascade: &EncodingCascade,
) -> Result<(), EncodingRejectCode> {
	let mut previous = 0_u32;
	for offset in &buffer.page_end_offsets {
		if offset.get() <= previous || u64::from(offset.get()) > buffer.encoded_range.len.get() {
			return Err(EncodingRejectCode::PageRangeOverflow);
		}
		previous = offset.get();
	}
	if let Some(last) = buffer.page_end_offsets.last() {
		if u64::from(last.get()) != buffer.encoded_range.len.get() {
			return Err(EncodingRejectCode::PageRangeOverflow);
		}
	}
	if cascade.emits_pages_in_v1() {
		if buffer.decoded_byte_len.get() > 0 && buffer.page_end_offsets.is_empty() {
			return Err(EncodingRejectCode::EncodingDirectoryParseError);
		}
	} else if !buffer.page_end_offsets.is_empty() {
		return Err(EncodingRejectCode::EncodingDirectoryParseError);
	}
	Ok(())
}

fn validate_page_stats(
	buffer: &BufferEncoding,
	cascade: &EncodingCascade,
	data_type: &arrow::datatypes::DataType,
	page_directory: &RecordPageDirectory,
) -> Result<(), EncodingRejectCode> {
	if buffer.page_stats.is_empty() {
		return Ok(());
	}
	if buffer.page_stats.len() != buffer.page_end_offsets.len()
		|| buffer.page_end_offsets.is_empty()
	{
		return Err(EncodingRejectCode::EncodingDirectoryParseError);
	}
	if buffer.page_value_count.get() == 0
		|| !fixed_width_page_stats_supported_type(data_type)
		|| !page_stats_cascade_eligible(cascade)
	{
		return Err(EncodingRejectCode::EncodingDirectoryParseError);
	}
	let Some(page) = page_directory.buffer_pages.iter().find(|page| {
		page.row_group_index == buffer.row_group_index
			&& page.ipc_column_ordinal == buffer.ipc_column_ordinal
			&& page.ipc_buffer_ordinal == buffer.ipc_buffer_ordinal
	}) else {
		return Err(EncodingRejectCode::BufferEncodingMismatch);
	};
	if !is_fixed_width_value_buffer(page_directory, page) {
		return Err(EncodingRejectCode::EncodingDirectoryParseError);
	}
	let Some(tag) = arrow_stats_type_tag(data_type) else {
		return Err(EncodingRejectCode::EncodingDirectoryParseError);
	};
	for (page_index, stat) in buffer.page_stats.iter().enumerate() {
		let row_count = page_stats_row_count(buffer, page_index)?;
		if row_count == 0 || stat.null_count.get() > row_count {
			return Err(EncodingRejectCode::EncodingDirectoryParseError);
		}
		let non_null_count = row_count - stat.null_count.get();
		match (non_null_count == 0, stat.min.as_ref(), stat.max.as_ref()) {
			(true, None, None) => {}
			(true, _, _) => return Err(EncodingRejectCode::EncodingDirectoryParseError),
			(false, Some(min), Some(max)) => {
				validate_page_stat_scalar(min, tag)?;
				validate_page_stat_scalar(max, tag)?;
				if min.bytes > max.bytes {
					return Err(EncodingRejectCode::EncodingDirectoryParseError);
				}
			}
			(false, _, _) => return Err(EncodingRejectCode::EncodingDirectoryParseError),
		}
	}
	Ok(())
}

fn page_stats_row_count(
	buffer: &BufferEncoding,
	page_index: usize,
) -> Result<u32, EncodingRejectCode> {
	let page_value_count = u64::from(buffer.page_value_count.get());
	let page_index =
		u64::try_from(page_index).map_err(|_| EncodingRejectCode::EncodingDirectoryParseError)?;
	let first_value = page_index
		.checked_mul(page_value_count)
		.ok_or(EncodingRejectCode::EncodingDirectoryParseError)?;
	let decoded_value_count = u64::from(buffer.decoded_value_count.get());
	if first_value >= decoded_value_count {
		return Err(EncodingRejectCode::EncodingDirectoryParseError);
	}
	u32::try_from((decoded_value_count - first_value).min(page_value_count))
		.map_err(|_| EncodingRejectCode::EncodingDirectoryParseError)
}

fn validate_page_stat_scalar(scalar: &CanonicalScalar, tag: u8) -> Result<(), EncodingRejectCode> {
	if scalar.arrow_type_tag != tag || scalar.truncated {
		return Err(EncodingRejectCode::EncodingDirectoryParseError);
	}
	let Some(width) = fixed_width_stat_byte_len(tag) else {
		return Err(EncodingRejectCode::EncodingDirectoryParseError);
	};
	if scalar.bytes.len() != width {
		return Err(EncodingRejectCode::EncodingDirectoryParseError);
	}
	Ok(())
}

fn fixed_width_stat_byte_len(tag: u8) -> Option<usize> {
	match tag {
		crate::storage::stats::ARROW_STATS_TAG_INT8
		| crate::storage::stats::ARROW_STATS_TAG_UINT8
		| crate::storage::stats::ARROW_STATS_TAG_BOOLEAN => Some(1),
		crate::storage::stats::ARROW_STATS_TAG_INT16
		| crate::storage::stats::ARROW_STATS_TAG_UINT16 => Some(2),
		crate::storage::stats::ARROW_STATS_TAG_INT32
		| crate::storage::stats::ARROW_STATS_TAG_UINT32
		| crate::storage::stats::ARROW_STATS_TAG_DATE32 => Some(4),
		crate::storage::stats::ARROW_STATS_TAG_INT64
		| crate::storage::stats::ARROW_STATS_TAG_UINT64
		| crate::storage::stats::ARROW_STATS_TAG_TIMESTAMP_MILLISECOND
		| crate::storage::stats::ARROW_STATS_TAG_TIMESTAMP_MICROSECOND
		| crate::storage::stats::ARROW_STATS_TAG_TIMESTAMP_NANOSECOND
		| crate::storage::stats::ARROW_STATS_TAG_DATE64 => Some(8),
		_ => None,
	}
}

fn write_cascade(out: &mut Vec<u8>, cascade: &EncodingCascade) -> std::io::Result<()> {
	out.push(cascade.kind() as u8);
	match cascade {
		EncodingCascade::Plain | EncodingCascade::Lz4Frame => {}
		EncodingCascade::BitPacked { bits, base } => {
			out.push(bits.get());
			write_i128(out, base.get());
		}
		EncodingCascade::Rle { run_count, inner } => {
			write_u32(out, run_count.get());
			write_leaf_cascade(out, inner)?;
		}
		EncodingCascade::Dictionary {
			dict_byte_len,
			dict_value_count,
			inner,
		} => {
			write_u64(out, dict_byte_len.get());
			write_u32(out, dict_value_count.get());
			write_leaf_cascade(out, inner)?;
		}
		EncodingCascade::Delta { base, inner } => {
			write_i128(out, base.get());
			write_leaf_cascade(out, inner)?;
		}
		EncodingCascade::FsstWholeBuffer {
			symbol_table_byte_len,
		}
		| EncodingCascade::FsstRowSafe {
			symbol_table_byte_len,
		} => {
			write_u64(out, symbol_table_byte_len.get());
		}
		EncodingCascade::Alp {
			exponent,
			factor,
			exception_count,
			inner,
		} => {
			write_i16(out, exponent.get());
			write_i64(out, factor.get());
			write_u32(out, exception_count.get());
			write_leaf_cascade(out, inner)?;
		}
	}
	Ok(())
}

fn write_leaf_cascade(out: &mut Vec<u8>, cascade: &EncodingLeafCascade) -> std::io::Result<()> {
	out.push(cascade.kind() as u8);
	match cascade {
		EncodingLeafCascade::Plain | EncodingLeafCascade::Lz4Frame => {}
		EncodingLeafCascade::BitPacked { bits, base } => {
			out.push(bits.get());
			write_i128(out, base.get());
		}
	}
	Ok(())
}

fn read_cascade(bytes: &[u8], cursor: &mut usize) -> Result<EncodingCascade, EncodingRejectCode> {
	let kind = read_u8(bytes, cursor)?;
	match kind {
		1 => Ok(EncodingCascade::Plain),
		2 => Ok(EncodingCascade::Lz4Frame),
		3 => Ok(EncodingCascade::BitPacked {
			bits: BitsPerValue::new(read_u8(bytes, cursor)?),
			base: BaseValue::new(read_i128(bytes, cursor)?),
		}),
		4 => Ok(EncodingCascade::Rle {
			run_count: RunCount::new(read_u32(bytes, cursor)?),
			inner: Box::new(read_leaf_cascade(bytes, cursor)?),
		}),
		5 => Ok(EncodingCascade::Dictionary {
			dict_byte_len: RecordBodyByteLen::try_new(read_u64(bytes, cursor)?)
				.map_err(|_| EncodingRejectCode::PageRangeOverflow)?,
			dict_value_count: DictValueCount::new(read_u32(bytes, cursor)?),
			inner: Box::new(read_leaf_cascade(bytes, cursor)?),
		}),
		6 => Ok(EncodingCascade::Delta {
			base: BaseValue::new(read_i128(bytes, cursor)?),
			inner: Box::new(read_leaf_cascade(bytes, cursor)?),
		}),
		7 => Ok(EncodingCascade::FsstWholeBuffer {
			symbol_table_byte_len: RecordBodyByteLen::try_new(read_u64(bytes, cursor)?)
				.map_err(|_| EncodingRejectCode::PageRangeOverflow)?,
		}),
		8 => Ok(EncodingCascade::Alp {
			exponent: Exponent::new(read_i16(bytes, cursor)?),
			factor: Factor::new(read_i64(bytes, cursor)?),
			exception_count: ExceptionCount::new(read_u32(bytes, cursor)?),
			inner: Box::new(read_leaf_cascade(bytes, cursor)?),
		}),
		9 => Ok(EncodingCascade::FsstRowSafe {
			symbol_table_byte_len: RecordBodyByteLen::try_new(read_u64(bytes, cursor)?)
				.map_err(|_| EncodingRejectCode::PageRangeOverflow)?,
		}),
		_ => Err(EncodingRejectCode::CascadeUnsupported),
	}
}

fn read_leaf_cascade(
	bytes: &[u8],
	cursor: &mut usize,
) -> Result<EncodingLeafCascade, EncodingRejectCode> {
	let kind = read_u8(bytes, cursor)?;
	match kind {
		1 => Ok(EncodingLeafCascade::Plain),
		2 => Ok(EncodingLeafCascade::Lz4Frame),
		3 => Ok(EncodingLeafCascade::BitPacked {
			bits: BitsPerValue::new(read_u8(bytes, cursor)?),
			base: BaseValue::new(read_i128(bytes, cursor)?),
		}),
		_ => Err(EncodingRejectCode::CascadeUnsupported),
	}
}

fn checked_len(len: usize) -> std::io::Result<u32> {
	u32::try_from(len).map_err(|_| {
		std::io::Error::new(
			std::io::ErrorKind::InvalidInput,
			"Encoding directory element count exceeds u32::MAX",
		)
	})
}

fn write_u16(out: &mut Vec<u8>, value: u16) {
	out.extend_from_slice(&value.to_be_bytes());
}

fn write_u32(out: &mut Vec<u8>, value: u32) {
	out.extend_from_slice(&value.to_be_bytes());
}

fn write_u64(out: &mut Vec<u8>, value: u64) {
	out.extend_from_slice(&value.to_be_bytes());
}

fn write_i16(out: &mut Vec<u8>, value: i16) {
	out.extend_from_slice(&value.to_be_bytes());
}

fn write_i64(out: &mut Vec<u8>, value: i64) {
	out.extend_from_slice(&value.to_be_bytes());
}

fn write_i128(out: &mut Vec<u8>, value: i128) {
	out.extend_from_slice(&value.to_be_bytes());
}

fn read_exact<'a>(
	bytes: &'a [u8],
	cursor: &mut usize,
	len: usize,
) -> Result<&'a [u8], EncodingRejectCode> {
	let end = cursor
		.checked_add(len)
		.ok_or(EncodingRejectCode::EncodingDirectoryParseError)?;
	if end > bytes.len() {
		return Err(EncodingRejectCode::EncodingDirectoryParseError);
	}
	let out = &bytes[*cursor..end];
	*cursor = end;
	Ok(out)
}

fn read_u8(bytes: &[u8], cursor: &mut usize) -> Result<u8, EncodingRejectCode> {
	Ok(*read_exact(bytes, cursor, 1)?
		.first()
		.ok_or(EncodingRejectCode::EncodingDirectoryParseError)?)
}

fn read_u16(bytes: &[u8], cursor: &mut usize) -> Result<u16, EncodingRejectCode> {
	Ok(u16::from_be_bytes(
		read_exact(bytes, cursor, 2)?
			.try_into()
			.expect("fixed width"),
	))
}

fn read_u32(bytes: &[u8], cursor: &mut usize) -> Result<u32, EncodingRejectCode> {
	Ok(u32::from_be_bytes(
		read_exact(bytes, cursor, 4)?
			.try_into()
			.expect("fixed width"),
	))
}

fn read_u64(bytes: &[u8], cursor: &mut usize) -> Result<u64, EncodingRejectCode> {
	Ok(u64::from_be_bytes(
		read_exact(bytes, cursor, 8)?
			.try_into()
			.expect("fixed width"),
	))
}

fn read_i16(bytes: &[u8], cursor: &mut usize) -> Result<i16, EncodingRejectCode> {
	Ok(i16::from_be_bytes(
		read_exact(bytes, cursor, 2)?
			.try_into()
			.expect("fixed width"),
	))
}

fn read_i64(bytes: &[u8], cursor: &mut usize) -> Result<i64, EncodingRejectCode> {
	Ok(i64::from_be_bytes(
		read_exact(bytes, cursor, 8)?
			.try_into()
			.expect("fixed width"),
	))
}

fn read_i128(bytes: &[u8], cursor: &mut usize) -> Result<i128, EncodingRejectCode> {
	Ok(i128::from_be_bytes(
		read_exact(bytes, cursor, 16)?
			.try_into()
			.expect("fixed width"),
	))
}

fn read_encoding_scalar(
	bytes: &[u8],
	cursor: &mut usize,
) -> Result<Option<CanonicalScalar>, EncodingRejectCode> {
	read_canonical_scalar(bytes, cursor)
		.map_err(|_| EncodingRejectCode::EncodingDirectoryParseError)
}

fn read_count(bytes: &[u8], cursor: &mut usize) -> Result<usize, EncodingRejectCode> {
	usize::try_from(read_u32(bytes, cursor)?)
		.map_err(|_| EncodingRejectCode::EncodingDirectoryParseError)
}

struct CompactIpcMessage<'a> {
	message: arrow::ipc::Message<'a>,
	header_type: MessageHeader,
	prefix_start: usize,
	body_start: usize,
	body_len: usize,
	body_end: usize,
}

fn read_compact_ipc_message<'a>(
	bytes: &'a [u8],
	offset: &mut usize,
) -> std::io::Result<Option<CompactIpcMessage<'a>>> {
	let prefix_start = *offset;
	if bytes.len().saturating_sub(*offset) < 4 {
		return Err(std::io::Error::new(
			std::io::ErrorKind::UnexpectedEof,
			"Encoding compact IPC message prefix truncated",
		));
	}
	let first = u32::from_le_bytes(bytes[*offset..*offset + 4].try_into().expect("fixed width"));
	*offset += 4;
	let metadata_len = if first == u32::MAX {
		if bytes.len().saturating_sub(*offset) < 4 {
			return Err(std::io::Error::new(
				std::io::ErrorKind::UnexpectedEof,
				"Encoding compact IPC continuation prefix truncated",
			));
		}
		let len = u32::from_le_bytes(bytes[*offset..*offset + 4].try_into().expect("fixed width"));
		*offset += 4;
		len as usize
	} else {
		first as usize
	};
	if metadata_len == 0 {
		return Ok(None);
	}
	let metadata_start = *offset;
	let metadata_end = metadata_start.checked_add(metadata_len).ok_or_else(|| {
		std::io::Error::new(
			std::io::ErrorKind::InvalidData,
			"Encoding compact IPC metadata length overflow",
		)
	})?;
	if metadata_end > bytes.len() {
		return Err(std::io::Error::new(
			std::io::ErrorKind::UnexpectedEof,
			"Encoding compact IPC metadata truncated",
		));
	}
	let message = root_as_message(&bytes[metadata_start..metadata_end]).map_err(|err| {
		std::io::Error::new(
			std::io::ErrorKind::InvalidData,
			format!("Encoding compact IPC message flatbuffer parse: {err}"),
		)
	})?;
	let body_len = usize::try_from(message.bodyLength()).map_err(|_| {
		std::io::Error::new(
			std::io::ErrorKind::InvalidData,
			"negative Encoding compact IPC body length",
		)
	})?;
	let body_start = metadata_end;
	let body_end = body_start.checked_add(body_len).ok_or_else(|| {
		std::io::Error::new(
			std::io::ErrorKind::InvalidData,
			"Encoding compact IPC body length overflow",
		)
	})?;
	if body_end > bytes.len() {
		return Err(std::io::Error::new(
			std::io::ErrorKind::UnexpectedEof,
			"Encoding compact IPC body truncated",
		));
	}
	*offset = body_end;
	Ok(Some(CompactIpcMessage {
		header_type: message.header_type(),
		message,
		prefix_start,
		body_start,
		body_len,
		body_end,
	}))
}

fn build_record_batch_message(
	source: &CompactIpcMessage<'_>,
	batch: FbRecordBatch<'_>,
	buffers: &[FbBuffer],
	body_len: i64,
) -> std::io::Result<Vec<u8>> {
	if source.message.custom_metadata().is_some() {
		return Err(std::io::Error::new(
			std::io::ErrorKind::InvalidData,
			"Encoding compact RecordBatch custom metadata is unsupported",
		));
	}
	let nodes = batch.nodes().ok_or_else(|| {
		std::io::Error::new(
			std::io::ErrorKind::InvalidData,
			"Encoding compact RecordBatch missing field nodes",
		)
	})?;
	let node_values = nodes
		.iter()
		.map(|node| FieldNode::new(node.length(), node.null_count()))
		.collect::<Vec<_>>();
	let variadic_values = batch
		.variadicBufferCounts()
		.map(|counts| counts.iter().collect::<Vec<_>>());
	let mut fbb = flatbuffers::FlatBufferBuilder::new();
	let nodes = fbb.create_vector(&node_values);
	let buffers = fbb.create_vector(buffers);
	let variadic_buffer_counts = variadic_values
		.as_ref()
		.map(|values| fbb.create_vector(values));
	let compression = batch.compression().map(|compression| {
		BodyCompression::create(
			&mut fbb,
			&BodyCompressionArgs {
				codec: compression.codec(),
				method: compression.method(),
			},
		)
	});
	let record_batch = FbRecordBatch::create(
		&mut fbb,
		&RecordBatchArgs {
			length: batch.length(),
			nodes: Some(nodes),
			buffers: Some(buffers),
			compression,
			variadicBufferCounts: variadic_buffer_counts,
		},
	);
	let message = FbMessage::create(
		&mut fbb,
		&MessageArgs {
			version: source.message.version(),
			header_type: MessageHeader::RecordBatch,
			header: Some(record_batch.as_union_value()),
			bodyLength: body_len,
			custom_metadata: None,
		},
	);
	fbb.finish(message, None);
	Ok(fbb.finished_data().to_vec())
}

fn append_ipc_message(out: &mut Vec<u8>, metadata: &[u8], body: &[u8]) -> std::io::Result<()> {
	let metadata_len = aligned_metadata_len(metadata.len())?;
	out.extend_from_slice(&u32::MAX.to_le_bytes());
	out.extend_from_slice(
		&u32::try_from(metadata_len)
			.map_err(|_| {
				std::io::Error::new(
					std::io::ErrorKind::InvalidData,
					"Encoding compact IPC metadata length exceeds u32",
				)
			})?
			.to_le_bytes(),
	);
	out.extend_from_slice(metadata);
	for _ in metadata.len()..metadata_len {
		out.push(0);
	}
	out.extend_from_slice(body);
	Ok(())
}

fn append_ipc_stream_end(out: &mut Vec<u8>) {
	out.extend_from_slice(&u32::MAX.to_le_bytes());
	out.extend_from_slice(&0_u32.to_le_bytes());
}

fn aligned_metadata_len(flatbuffer_len: usize) -> std::io::Result<usize> {
	let with_prefix = flatbuffer_len.checked_add(8).ok_or_else(|| {
		std::io::Error::new(
			std::io::ErrorKind::InvalidData,
			"Encoding compact IPC metadata length overflow",
		)
	})?;
	let aligned = align_to_8(with_prefix);
	aligned.checked_sub(8).ok_or_else(|| {
		std::io::Error::new(
			std::io::ErrorKind::InvalidData,
			"Encoding compact IPC aligned metadata underflow",
		)
	})
}

fn pad_to_8(bytes: &mut Vec<u8>) {
	let target = align_to_8(bytes.len());
	bytes.resize(target, 0);
}

fn align_to_8(value: usize) -> usize {
	(value + 7) & !7
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::schema::{RecordRowIndex, RowGroupRowCount, SimdPageValueCount};
	use arrow::array::{ArrayRef, Int64Array};
	use arrow::datatypes::{DataType, Field, Schema};
	use std::sync::Arc;

	fn validity_bitmap(rows: usize, is_valid: impl Fn(usize) -> bool) -> Vec<u8> {
		let mut bytes = vec![0_u8; rows.div_ceil(8)];
		for row in 0..rows {
			if is_valid(row) {
				bytes[row / 8] |= 1_u8 << (row % 8);
			}
		}
		bytes
	}

	fn repeated_i64_value_bytes(rows: usize, value: i64) -> Vec<u8> {
		let mut bytes = Vec::with_capacity(rows * std::mem::size_of::<i64>());
		for _ in 0..rows {
			bytes.extend_from_slice(&value.to_le_bytes());
		}
		bytes
	}

	fn validity_writer_fixture() -> (
		TieredRecordId,
		Vec<u8>,
		RecordPageDirectory,
		Vec<RecordBatch>,
		EncodingWriterConfig,
		RowGroupWriterConfig,
	) {
		let rows = 4096_usize;
		let null_bytes = validity_bitmap(rows, |row| row < 2048);
		let value_bytes = repeated_i64_value_bytes(rows, 0);
		let value_start = null_bytes.len() as u64;
		let mut ipc_bytes = null_bytes.clone();
		ipc_bytes.extend_from_slice(&value_bytes);
		let record_id = TieredRecordId {
			slab_id: SlabId::new(69),
			record_offset: RecordBodyByteOffset::ZERO,
		};
		let fingerprint = LogicalSchemaFingerprint::from_bytes([0x69; 32]);
		let page_directory = RecordPageDirectory {
			record_id,
			row_count: RowCount::new(rows as u64),
			row_groups: vec![RowGroupDirectoryEntry {
				row_group_index: RowGroupIndex::ZERO,
				first_record_row_index: RecordRowIndex::ZERO,
				row_count: RowGroupRowCount::new(rows as u32),
				metadata_range: RecordBodyByteRange {
					start: RecordBodyByteOffset::ZERO,
					len: RecordBodyByteLen::new(64),
				},
				body_range: RecordBodyByteRange {
					start: RecordBodyByteOffset::ZERO,
					len: RecordBodyByteLen::new(ipc_bytes.len() as u64),
				},
			}],
			buffer_pages: vec![
				BufferPageDirectoryEntry {
					row_group_index: RowGroupIndex::ZERO,
					ipc_column_ordinal: IpcColumnOrdinal::ZERO,
					ipc_buffer_ordinal: IpcBufferIndex::ZERO,
					range: RecordBodyByteRange {
						start: RecordBodyByteOffset::ZERO,
						len: RecordBodyByteLen::new(null_bytes.len() as u64),
					},
				},
				BufferPageDirectoryEntry {
					row_group_index: RowGroupIndex::ZERO,
					ipc_column_ordinal: IpcColumnOrdinal::ZERO,
					ipc_buffer_ordinal: IpcBufferIndex::new(1),
					range: RecordBodyByteRange {
						start: RecordBodyByteOffset::new(value_start),
						len: RecordBodyByteLen::new(value_bytes.len() as u64),
					},
				},
			],
			row_group_stats: Vec::new(),
			logical_schema_fingerprint: fingerprint,
		};
		let schema = Arc::new(Schema::new(vec![Field::new(
			"value",
			DataType::Int64,
			true,
		)]));
		let values = (0..rows)
			.map(|row| (row < 2048).then_some(0_i64))
			.collect::<Vec<_>>();
		let batch =
			RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values)) as ArrayRef])
				.unwrap();
		let config = EncodingWriterConfig {
			enabled: true,
			target_simd_page_values: SimdPageValueCount::new(1024),
			min_buffer_bytes_for_encoding: RecordBodyByteLen::new(1),
			enabled_cascades: [EncodingCascadeKind::Plain, EncodingCascadeKind::Rle]
				.into_iter()
				.collect(),
			..EncodingWriterConfig::default()
		};
		(
			record_id,
			ipc_bytes,
			page_directory,
			vec![batch],
			config,
			RowGroupWriterConfig::default(),
		)
	}

	#[test]
	fn production_writer_routing_emits_promoted_validity_buffer() {
		let (record_id, ipc_bytes, page_directory, batches, config, row_group_config) =
			validity_writer_fixture();
		let (directory, _applied) = build_encoding_directory_for_ipc_stream(
			record_id,
			&ipc_bytes,
			&page_directory,
			&batches,
			&config,
			&row_group_config,
		)
		.unwrap();
		let directory = directory.expect("value buffer encoding should be emitted");
		assert!(directory.buffers.iter().any(|encoding| {
			encoding.ipc_buffer_ordinal == IpcBufferIndex::new(1)
				&& !encoding.page_end_offsets.is_empty()
		}));
		let validity = directory
			.buffers
			.iter()
			.find(|encoding| encoding.ipc_buffer_ordinal == IpcBufferIndex::ZERO)
			.expect("promoted production route should emit validity encoding");
		let cascade = directory
			.cascades
			.get(validity.cascade_index.get() as usize)
			.expect("validity cascade");
		assert!(matches!(
			cascade,
			EncodingCascade::Rle { inner, .. }
				if matches!(inner.as_ref(), EncodingLeafCascade::Plain)
		));
		assert_eq!(
			validity
				.page_end_offsets
				.iter()
				.map(|offset| offset.get())
				.collect::<Vec<_>>(),
			vec![5, 10, 15, 20]
		);
	}

	#[test]
	fn bench_routing_emits_page_aligned_rle_validity_buffer() {
		let (record_id, ipc_bytes, page_directory, batches, config, row_group_config) =
			validity_writer_fixture();
		let (directory, applied) = build_encoding_directory_for_ipc_stream_with_validity_for_bench(
			record_id,
			&ipc_bytes,
			&page_directory,
			&batches,
			&config,
			&row_group_config,
		)
		.unwrap();
		let directory = directory.expect("value and validity encodings should be emitted");
		let validity = directory
			.buffers
			.iter()
			.find(|encoding| encoding.ipc_buffer_ordinal == IpcBufferIndex::ZERO)
			.expect("validity encoding");
		let cascade = directory
			.cascades
			.get(validity.cascade_index.get() as usize)
			.expect("validity cascade");
		assert!(matches!(
			cascade,
			EncodingCascade::Rle { inner, .. }
				if matches!(inner.as_ref(), EncodingLeafCascade::Plain)
		));
		assert_eq!(validity.decoded_byte_len, RecordBodyByteLen::new(512));
		assert_eq!(validity.decoded_value_count, BufferValueCount::new(4096));
		assert_eq!(validity.page_value_count, SimdPageValueCount::new(1024));
		assert_eq!(
			validity
				.page_end_offsets
				.iter()
				.map(|offset| offset.get())
				.collect::<Vec<_>>(),
			vec![5, 10, 15, 20]
		);
		assert!(applied.iter().any(|buffer| {
			buffer.ipc_buffer_ordinal == IpcBufferIndex::ZERO
				&& buffer.encoded_bytes.len() == 20
				&& buffer.decoded_len == 512
		}));
	}
}
