use arrow::datatypes::{DataType, Field, Schema};
use wendelon::crypto::{SLAB_ENCRYPTION_CHUNK_SIZES, SLAB_ENCRYPTION_DEFAULT_CHUNK_SIZE};
use wendelon::schema::{
	BaseValue, BitsPerValue, BufferValueCount, ChunkSizeBytes, DictValueCount,
	EncodingCascadeIndex, EncodingPageByteOffset, IpcBufferIndex, IpcColumnOrdinal, LocalityPolicy,
	LogicalSchemaFingerprint, MultiRowLayoutDescriptor, PrimaryKeyDescriptor, RecordBodyByteLen,
	RecordBodyByteOffset, RecordLayoutPolicy, RowCount, RowGroupIndex, RowIdentityPolicy, RunCount,
	SchemaEvolutionPolicy, SimdPageValueCount, SlabId, TableObjectId,
};
use wendelon::storage::{
	BufferEncoding, EncodedBufferByteRange, EncodingCascade, EncodingDirectory,
	EncodingLeafCascade, L2ChunkLayoutConfig, PhysicalBufferLayoutInput, TieredRecordId,
	align_buffers_to_chunks, choose_chunk_size_bytes, choose_chunk_size_bytes_for_directories,
	estimate_pad_inflation,
};

fn logical_fingerprint() -> LogicalSchemaFingerprint {
	let schema = Schema::new(vec![Field::new("id", DataType::UInt8, false)]);
	wendelon::schema::TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([0x61; 16]),
		1,
		"test",
		"encoding_chunk_layout",
		&schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
		LocalityPolicy::IdentityProjection,
		RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::new(4, 4, 8 * 1024 * 1024).unwrap()),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap()
	.logical_schema_fingerprint
}

fn directory_with_cascades(
	cascades: Vec<EncodingCascade>,
	specs: &[(usize, u32, Vec<u32>)],
) -> EncodingDirectory {
	EncodingDirectory {
		record_id: TieredRecordId {
			slab_id: SlabId::ZERO,
			record_offset: RecordBodyByteOffset::ZERO,
		},
		row_count: RowCount::new(1024),
		logical_schema_fingerprint: logical_fingerprint(),
		target_simd_page_values: SimdPageValueCount::new(1024),
		cascades,
		buffers: specs
			.iter()
			.enumerate()
			.map(|(index, (cascade_index, len, page_ends))| BufferEncoding {
				row_group_index: RowGroupIndex::ZERO,
				ipc_column_ordinal: IpcColumnOrdinal::new(index as u32),
				ipc_buffer_ordinal: IpcBufferIndex::ZERO,
				cascade_index: EncodingCascadeIndex::new(*cascade_index as u16),
				decoded_byte_len: RecordBodyByteLen::new(u64::from(*len)),
				decoded_value_count: BufferValueCount::new(1024),
				encoded_range: EncodedBufferByteRange {
					start: EncodingPageByteOffset::ZERO,
					len: RecordBodyByteLen::new(u64::from(*len)),
				},
				page_value_count: SimdPageValueCount::new(1024),
				page_end_offsets: page_ends
					.iter()
					.copied()
					.map(EncodingPageByteOffset::new)
					.collect(),
				page_stats: Vec::new(),
			})
			.collect(),
	}
}

fn bitpacked_directory(page_ends: &[u32]) -> EncodingDirectory {
	let encoded_len = page_ends.last().copied().unwrap_or(4096);
	directory_with_cascades(
		vec![EncodingCascade::BitPacked {
			bits: BitsPerValue::new(8),
			base: BaseValue::ZERO,
		}],
		&[(0, encoded_len, page_ends.to_vec())],
	)
}

fn enabled_config() -> L2ChunkLayoutConfig {
	L2ChunkLayoutConfig {
		enabled: true,
		..L2ChunkLayoutConfig::default()
	}
}

#[test]
fn selector_respects_promoted_floor_and_closed_set() {
	let config = enabled_config();
	assert_eq!(
		choose_chunk_size_bytes(Some(&bitpacked_directory(&[4096])), &config),
		ChunkSizeBytes::new(16 * 1024)
	);
	assert_eq!(
		choose_chunk_size_bytes(Some(&bitpacked_directory(&[20 * 1024])), &config),
		ChunkSizeBytes::new(32 * 1024)
	);
	assert_eq!(
		choose_chunk_size_bytes(Some(&bitpacked_directory(&[48 * 1024])), &config),
		SLAB_ENCRYPTION_DEFAULT_CHUNK_SIZE
	);
	assert_eq!(
		choose_chunk_size_bytes(
			Some(&bitpacked_directory(&[4096])),
			&L2ChunkLayoutConfig {
				enabled: false,
				..L2ChunkLayoutConfig::default()
			}
		),
		SLAB_ENCRYPTION_DEFAULT_CHUNK_SIZE
	);
}

#[test]
fn multi_directory_selector_uses_largest_required_chunk_size() {
	let config = enabled_config();
	assert_eq!(
		choose_chunk_size_bytes_for_directories(
			[
				&bitpacked_directory(&[4096]),
				&bitpacked_directory(&[24 * 1024])
			],
			&config,
		),
		ChunkSizeBytes::new(32 * 1024)
	);
}

#[test]
fn buffer_alignment_is_deterministic_across_replicas() {
	let directory = directory_with_cascades(
		vec![
			EncodingCascade::BitPacked {
				bits: BitsPerValue::new(8),
				base: BaseValue::ZERO,
			},
			EncodingCascade::Plain,
			EncodingCascade::Dictionary {
				dict_byte_len: RecordBodyByteLen::new(64),
				dict_value_count: DictValueCount::new(4),
				inner: Box::new(EncodingLeafCascade::Plain),
			},
			EncodingCascade::Rle {
				run_count: RunCount::new(8),
				inner: Box::new(EncodingLeafCascade::BitPacked {
					bits: BitsPerValue::new(4),
					base: BaseValue::ZERO,
				}),
			},
		],
		&[
			(0, 1024, vec![1024]),
			(1, 17, Vec::new()),
			(2, 2048, vec![2048]),
			(3, 100, vec![100]),
		],
	);
	let inputs: Vec<_> = directory
		.buffers
		.iter()
		.map(|buffer| PhysicalBufferLayoutInput {
			row_group_index: buffer.row_group_index,
			ipc_column_ordinal: buffer.ipc_column_ordinal,
			ipc_buffer_ordinal: buffer.ipc_buffer_ordinal,
			physical_len: buffer.encoded_range.len.get() as usize,
		})
		.collect();

	for chunk_size in SLAB_ENCRYPTION_CHUNK_SIZES {
		let chunk_size = ChunkSizeBytes::new(chunk_size as u32);
		let left = align_buffers_to_chunks(&directory, chunk_size, &inputs).unwrap();
		let right = align_buffers_to_chunks(&directory, chunk_size, &inputs).unwrap();
		assert_eq!(left, right);
		assert_eq!(left.entries.len(), directory.buffers.len());
		assert_eq!(
			left.entries[0].physical_offset % chunk_size.get() as usize,
			0
		);
		assert_eq!(
			left.entries[2].physical_offset % chunk_size.get() as usize,
			0
		);
		assert_eq!(left.entries[1].pad_before_bytes, 0);
		assert_eq!(left.entries[3].pad_before_bytes, 0);
		assert!(left.total_pad_bytes <= left.body_len);
	}
}

#[test]
fn pad_inflation_is_bounded_and_repeatable() {
	let directory = directory_with_cascades(
		vec![
			EncodingCascade::BitPacked {
				bits: BitsPerValue::new(16),
				base: BaseValue::ZERO,
			},
			EncodingCascade::Dictionary {
				dict_byte_len: RecordBodyByteLen::new(64),
				dict_value_count: DictValueCount::new(8),
				inner: Box::new(EncodingLeafCascade::BitPacked {
					bits: BitsPerValue::new(8),
					base: BaseValue::ZERO,
				}),
			},
		],
		&[
			(0, 1_048_584, vec![1_048_584]),
			(1, 1_048_576, vec![1_048_576]),
		],
	);

	let first = estimate_pad_inflation(&directory, ChunkSizeBytes::new(16 * 1024));
	let second = estimate_pad_inflation(&directory, ChunkSizeBytes::new(16 * 1024));
	assert_eq!(first, second);
	assert!(first.get() > 0);
	assert!(
		first.get()
			<= L2ChunkLayoutConfig::default()
				.max_pad_inflation_micros
				.get()
	);
}
