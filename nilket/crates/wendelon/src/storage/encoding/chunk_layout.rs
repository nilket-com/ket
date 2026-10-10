use super::{BufferEncoding, EncodingCascade, EncodingDirectory, EncodingLeafCascade};
use crate::crypto::{SLAB_ENCRYPTION_CHUNK_SIZES, SLAB_ENCRYPTION_DEFAULT_CHUNK_SIZE};
use crate::schema::{
	ChunkSizeBytes, IpcBufferIndex, IpcColumnOrdinal, RowGroupIndex, SelectivityFractionMicros,
};
use serde::{Deserialize, Serialize};
use std::io::{Error, ErrorKind};

pub const L2_CHUNK_LAYOUT_MAX_PAD_INFLATION_MICROS_DEFAULT: u32 = 50_000;
pub(crate) const PROMOTED_FLOOR: ChunkSizeBytes = ChunkSizeBytes::new(16 * 1024);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct L2ChunkLayoutConfig {
	pub enabled: bool,
	pub max_pad_inflation_micros: SelectivityFractionMicros,
}

impl Default for L2ChunkLayoutConfig {
	fn default() -> Self {
		Self {
			enabled: true,
			max_pad_inflation_micros: SelectivityFractionMicros::new(
				L2_CHUNK_LAYOUT_MAX_PAD_INFLATION_MICROS_DEFAULT,
			),
		}
	}
}

impl L2ChunkLayoutConfig {
	pub fn validate(&self) -> std::io::Result<()> {
		if self.max_pad_inflation_micros.get() > 1_000_000 {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"l2 chunk layout max_pad_inflation_micros must be <= 1_000_000",
			));
		}
		Ok(())
	}
}

pub fn choose_chunk_size_bytes(
	directory: Option<&EncodingDirectory>,
	config: &L2ChunkLayoutConfig,
) -> ChunkSizeBytes {
	if !config.enabled {
		return SLAB_ENCRYPTION_DEFAULT_CHUNK_SIZE;
	}
	let Some(directory) = directory else {
		return SLAB_ENCRYPTION_DEFAULT_CHUNK_SIZE;
	};

	let threshold =
		max_page_bytes_in_default_engaged_buffers(directory).max(PROMOTED_FLOOR.get() as usize);
	let Some(candidate) = SLAB_ENCRYPTION_CHUNK_SIZES
		.iter()
		.copied()
		.find(|size| *size >= threshold)
	else {
		return SLAB_ENCRYPTION_DEFAULT_CHUNK_SIZE;
	};
	let candidate = ChunkSizeBytes::new(candidate as u32);
	if estimate_pad_inflation(directory, candidate).get() > config.max_pad_inflation_micros.get() {
		return SLAB_ENCRYPTION_DEFAULT_CHUNK_SIZE;
	}
	candidate
}

pub fn choose_chunk_size_bytes_for_directories<'a>(
	directories: impl IntoIterator<Item = &'a EncodingDirectory>,
	config: &L2ChunkLayoutConfig,
) -> ChunkSizeBytes {
	if !config.enabled {
		return SLAB_ENCRYPTION_DEFAULT_CHUNK_SIZE;
	}
	directories
		.into_iter()
		.map(|directory| choose_chunk_size_bytes(Some(directory), config))
		.max_by_key(|chunk_size| chunk_size.get())
		.unwrap_or(SLAB_ENCRYPTION_DEFAULT_CHUNK_SIZE)
}

pub fn estimate_pad_inflation(
	directory: &EncodingDirectory,
	chunk_size_bytes: ChunkSizeBytes,
) -> SelectivityFractionMicros {
	let total_buffer_bytes = directory
		.buffers
		.iter()
		.map(|buffer| buffer.encoded_range.len.get() as u128)
		.sum::<u128>();
	if total_buffer_bytes == 0 {
		return SelectivityFractionMicros::ZERO;
	}
	let pad_bytes = estimate_pad_bytes(directory, chunk_size_bytes) as u128;
	let micros = pad_bytes
		.saturating_mul(1_000_000)
		.saturating_div(total_buffer_bytes)
		.min(u32::MAX as u128);
	SelectivityFractionMicros::new(micros as u32)
}

pub(crate) fn estimate_pad_bytes(
	directory: &EncodingDirectory,
	chunk_size_bytes: ChunkSizeBytes,
) -> usize {
	let chunk_size = chunk_size_bytes.get() as usize;
	if chunk_size == 0 {
		return 0;
	}
	let mut cursor = 0_usize;
	let mut total_pad = 0_usize;
	for buffer in &directory.buffers {
		let pad = if default_sparse_engaged_buffer(directory, buffer) {
			chunk_pad_for_cursor(cursor, chunk_size)
		} else {
			0
		};
		total_pad = total_pad.saturating_add(pad);
		cursor = cursor
			.saturating_add(pad)
			.saturating_add(buffer.encoded_range.len.get() as usize);
	}
	total_pad
}

pub(crate) fn chunk_pad_for_cursor(cursor: usize, chunk_size: usize) -> usize {
	if chunk_size == 0 {
		return 0;
	}
	let remainder = cursor % chunk_size;
	if remainder == 0 {
		0
	} else {
		chunk_size - remainder
	}
}

pub fn align_buffers_to_chunks(
	directory: &EncodingDirectory,
	chunk_size_bytes: ChunkSizeBytes,
	buffers: &[PhysicalBufferLayoutInput],
) -> std::io::Result<AlignedBufferLayout> {
	let chunk_size = usize::try_from(chunk_size_bytes.get()).map_err(|_| {
		Error::new(
			ErrorKind::InvalidInput,
			"chunk size does not fit into usize for buffer alignment",
		)
	})?;
	let mut cursor = 0_usize;
	let mut total_pad = 0_usize;
	let mut entries = Vec::with_capacity(buffers.len());
	for buffer in buffers {
		let pad = if default_sparse_engaged_key(
			directory,
			buffer.row_group_index,
			buffer.ipc_column_ordinal,
			buffer.ipc_buffer_ordinal,
		) {
			chunk_pad_for_cursor(cursor, chunk_size)
		} else {
			0
		};
		total_pad = total_pad.checked_add(pad).ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				"chunk-layout alignment pad total overflow",
			)
		})?;
		cursor = cursor.checked_add(pad).ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				"chunk-layout aligned cursor overflow",
			)
		})?;
		let physical_offset = cursor;
		cursor = cursor.checked_add(buffer.physical_len).ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				"chunk-layout physical buffer cursor overflow",
			)
		})?;
		cursor = round_up_to_8(cursor)?;
		entries.push(AlignedBufferLayoutEntry {
			pad_before_bytes: pad,
			physical_offset,
		});
	}
	Ok(AlignedBufferLayout {
		entries,
		total_pad_bytes: total_pad,
		body_len: cursor,
	})
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PhysicalBufferLayoutInput {
	pub row_group_index: RowGroupIndex,
	pub ipc_column_ordinal: IpcColumnOrdinal,
	pub ipc_buffer_ordinal: IpcBufferIndex,
	pub physical_len: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AlignedBufferLayout {
	pub entries: Vec<AlignedBufferLayoutEntry>,
	pub total_pad_bytes: usize,
	pub body_len: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AlignedBufferLayoutEntry {
	pub pad_before_bytes: usize,
	pub physical_offset: usize,
}

pub(crate) fn default_sparse_engaged_key(
	directory: &EncodingDirectory,
	row_group_index: RowGroupIndex,
	ipc_column_ordinal: IpcColumnOrdinal,
	ipc_buffer_ordinal: IpcBufferIndex,
) -> bool {
	directory
		.buffers
		.iter()
		.find(|buffer| {
			buffer.row_group_index == row_group_index
				&& buffer.ipc_column_ordinal == ipc_column_ordinal
				&& buffer.ipc_buffer_ordinal == ipc_buffer_ordinal
		})
		.is_some_and(|buffer| default_sparse_engaged_buffer(directory, buffer))
}

pub(crate) fn default_sparse_engaged_buffer(
	directory: &EncodingDirectory,
	buffer: &BufferEncoding,
) -> bool {
	directory
		.cascades
		.get(buffer.cascade_index.get() as usize)
		.is_some_and(default_sparse_engaged_cascade)
}

pub(crate) fn default_sparse_engaged_cascade(cascade: &EncodingCascade) -> bool {
	matches!(cascade, EncodingCascade::BitPacked { .. })
		|| matches!(
			cascade,
			EncodingCascade::Dictionary { inner, .. }
				if matches!(
					inner.as_ref(),
					EncodingLeafCascade::Plain | EncodingLeafCascade::BitPacked { .. }
				)
		)
}

fn max_page_bytes_in_default_engaged_buffers(directory: &EncodingDirectory) -> usize {
	let mut max_page_bytes = 0_usize;
	for buffer in &directory.buffers {
		if !default_sparse_engaged_buffer(directory, buffer) {
			continue;
		}
		let mut previous = 0_u32;
		if buffer.page_end_offsets.is_empty() {
			max_page_bytes = max_page_bytes.max(buffer.encoded_range.len.get() as usize);
			continue;
		}
		for end in &buffer.page_end_offsets {
			let width = end.get().saturating_sub(previous) as usize;
			max_page_bytes = max_page_bytes.max(width);
			previous = end.get();
		}
	}
	max_page_bytes
}

fn round_up_to_8(value: usize) -> std::io::Result<usize> {
	value.checked_add(7).map(|v| v & !7).ok_or_else(|| {
		Error::new(
			ErrorKind::InvalidData,
			"chunk-layout IPC alignment cursor overflow",
		)
	})
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::schema::{
		BaseValue, BitsPerValue, BufferValueCount, EncodingCascadeIndex, EncodingPageByteOffset,
		RecordBodyByteLen, RowCount, SimdPageValueCount, SlabId,
	};
	use crate::storage::columnar::TieredRecordId;

	fn directory_with_buffer(cascade: EncodingCascade, page_ends: &[u32]) -> EncodingDirectory {
		let encoded_len = page_ends.last().copied().unwrap_or(4096);
		EncodingDirectory {
			record_id: TieredRecordId {
				slab_id: SlabId::ZERO,
				record_offset: crate::schema::RecordBodyByteOffset::ZERO,
			},
			row_count: RowCount::new(1024),
			logical_schema_fingerprint: crate::schema::LogicalSchemaFingerprint::from_bytes(
				[0; 32],
			),
			target_simd_page_values: SimdPageValueCount::new(1024),
			cascades: vec![cascade],
			buffers: vec![BufferEncoding {
				row_group_index: RowGroupIndex::ZERO,
				ipc_column_ordinal: IpcColumnOrdinal::ZERO,
				ipc_buffer_ordinal: IpcBufferIndex::ZERO,
				cascade_index: EncodingCascadeIndex::ZERO,
				decoded_byte_len: RecordBodyByteLen::new(encoded_len as u64),
				decoded_value_count: BufferValueCount::new(1024),
				encoded_range: crate::storage::encoding::EncodedBufferByteRange {
					start: EncodingPageByteOffset::ZERO,
					len: RecordBodyByteLen::new(encoded_len as u64),
				},
				page_value_count: SimdPageValueCount::new(1024),
				page_end_offsets: page_ends
					.iter()
					.copied()
					.map(EncodingPageByteOffset::new)
					.collect(),
				page_stats: Vec::new(),
			}],
		}
	}

	#[test]
	fn selector_uses_promoted_floor_for_small_pages() {
		let directory = directory_with_buffer(
			EncodingCascade::BitPacked {
				bits: BitsPerValue::new(8),
				base: BaseValue::ZERO,
			},
			&[1024, 2048, 3072, 4096],
		);
		let config = L2ChunkLayoutConfig {
			enabled: true,
			..L2ChunkLayoutConfig::default()
		};
		assert_eq!(
			choose_chunk_size_bytes(Some(&directory), &config),
			ChunkSizeBytes::new(16 * 1024)
		);
	}

	#[test]
	fn selector_returns_default_when_disabled_or_missing_directory() {
		let directory = directory_with_buffer(
			EncodingCascade::BitPacked {
				bits: BitsPerValue::new(8),
				base: BaseValue::ZERO,
			},
			&[4096],
		);
		assert_eq!(
			choose_chunk_size_bytes(
				Some(&directory),
				&L2ChunkLayoutConfig {
					enabled: false,
					..L2ChunkLayoutConfig::default()
				}
			),
			SLAB_ENCRYPTION_DEFAULT_CHUNK_SIZE
		);
		let config = L2ChunkLayoutConfig {
			enabled: true,
			..L2ChunkLayoutConfig::default()
		};
		assert_eq!(
			choose_chunk_size_bytes(None, &config),
			SLAB_ENCRYPTION_DEFAULT_CHUNK_SIZE
		);
	}

	#[test]
	fn selector_bumps_to_next_closed_size_for_large_pages() {
		let directory = directory_with_buffer(
			EncodingCascade::BitPacked {
				bits: BitsPerValue::new(8),
				base: BaseValue::ZERO,
			},
			&[24 * 1024],
		);
		let config = L2ChunkLayoutConfig {
			enabled: true,
			..L2ChunkLayoutConfig::default()
		};
		assert_eq!(
			choose_chunk_size_bytes(Some(&directory), &config),
			ChunkSizeBytes::new(32 * 1024)
		);
	}

	#[test]
	fn multi_directory_selector_uses_largest_required_chunk_size() {
		let small_directory = directory_with_buffer(
			EncodingCascade::BitPacked {
				bits: BitsPerValue::new(8),
				base: BaseValue::ZERO,
			},
			&[4096],
		);
		let large_directory = directory_with_buffer(
			EncodingCascade::BitPacked {
				bits: BitsPerValue::new(8),
				base: BaseValue::ZERO,
			},
			&[24 * 1024],
		);
		let config = L2ChunkLayoutConfig {
			enabled: true,
			..L2ChunkLayoutConfig::default()
		};
		assert_eq!(
			choose_chunk_size_bytes_for_directories([&small_directory, &large_directory], &config),
			ChunkSizeBytes::new(32 * 1024)
		);
	}

	#[test]
	fn multi_directory_selector_keeps_default_when_any_directory_requires_default() {
		let small_directory = directory_with_buffer(
			EncodingCascade::BitPacked {
				bits: BitsPerValue::new(8),
				base: BaseValue::ZERO,
			},
			&[4096],
		);
		let too_large_directory = directory_with_buffer(
			EncodingCascade::BitPacked {
				bits: BitsPerValue::new(8),
				base: BaseValue::ZERO,
			},
			&[96 * 1024],
		);
		let config = L2ChunkLayoutConfig {
			enabled: true,
			..L2ChunkLayoutConfig::default()
		};
		assert_eq!(
			choose_chunk_size_bytes_for_directories(
				[&small_directory, &too_large_directory],
				&config
			),
			SLAB_ENCRYPTION_DEFAULT_CHUNK_SIZE
		);
	}

	#[test]
	fn rle_is_not_default_sparse_engaged_for_chunk_alignment() {
		let directory = directory_with_buffer(
			EncodingCascade::Rle {
				run_count: crate::schema::RunCount::new(8),
				inner: Box::new(EncodingLeafCascade::BitPacked {
					bits: BitsPerValue::new(8),
					base: BaseValue::ZERO,
				}),
			},
			&[4096],
		);
		assert!(!default_sparse_engaged_buffer(
			&directory,
			&directory.buffers[0]
		));
	}
}
