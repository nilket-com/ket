use arrow::datatypes::{DataType, Field, Schema};
use wendelon::schema::{
	BaseValue, BitsPerValue, BufferValueCount, EncodingCascadeIndex, EncodingPageByteOffset,
	IpcBufferIndex, IpcColumnOrdinal, LocalityPolicy, MultiRowLayoutDescriptor,
	PrimaryKeyDescriptor, RecordBodyByteLen, RecordBodyByteOffset, RecordLayoutPolicy, RowCount,
	RowGroupIndex, RowIdentityPolicy, SchemaEvolutionPolicy, SimdPageValueCount, SlabId,
	TableObjectId,
};
use wendelon::storage::encoding::cascade::encode_buffer;
use wendelon::storage::encoding::materialize::materialize_encoding_buffer;
use wendelon::storage::{
	BufferEncoding, EncodedBufferByteRange, EncodingCascade, EncodingDirectory, TieredRecordId,
};

fn logical_fingerprint() -> wendelon::schema::LogicalSchemaFingerprint {
	let schema = Schema::new(vec![Field::new("id", DataType::UInt8, false)]);
	wendelon::schema::TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([0x57; 16]),
		1,
		"test",
		"encoding_materialize",
		&schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
		LocalityPolicy::IdentityProjection,
		RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::new(4, 4, 8 * 1024 * 1024).unwrap()),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap()
	.logical_schema_fingerprint
}

#[test]
fn materializer_decodes_buffer_relative_encoded_range() {
	let input = (0..4096).map(|index| (index % 8) as u8).collect::<Vec<_>>();
	let cascade = EncodingCascade::BitPacked {
		bits: BitsPerValue::new(3),
		base: BaseValue::ZERO,
	};
	let encoded = encode_buffer(
		&input,
		&DataType::UInt8,
		cascade.clone(),
		BufferValueCount::new(input.len() as u32),
		SimdPageValueCount::new(1024),
	)
	.unwrap();
	let mut encoded_buffer_bytes = vec![0_u8; 4];
	encoded_buffer_bytes.extend_from_slice(&encoded.bytes);
	let directory = EncodingDirectory {
		record_id: TieredRecordId {
			slab_id: SlabId::ZERO,
			record_offset: RecordBodyByteOffset::ZERO,
		},
		row_count: RowCount::new(input.len() as u64),
		logical_schema_fingerprint: logical_fingerprint(),
		target_simd_page_values: SimdPageValueCount::new(1024),
		cascades: vec![cascade],
		buffers: Vec::new(),
	};
	let encoding = BufferEncoding {
		row_group_index: RowGroupIndex::ZERO,
		ipc_column_ordinal: IpcColumnOrdinal::ZERO,
		ipc_buffer_ordinal: IpcBufferIndex::ZERO,
		cascade_index: EncodingCascadeIndex::ZERO,
		decoded_byte_len: RecordBodyByteLen::new(input.len() as u64),
		decoded_value_count: BufferValueCount::new(input.len() as u32),
		encoded_range: EncodedBufferByteRange {
			start: EncodingPageByteOffset::new(4),
			len: RecordBodyByteLen::new(encoded.bytes.len() as u64),
		},
		page_value_count: SimdPageValueCount::new(1024),
		page_end_offsets: encoded.page_end_offsets.clone(),
		page_stats: Vec::new(),
	};
	assert!(encoding.page_end_offsets.len() > 1);

	let decoded = materialize_encoding_buffer(
		&encoded_buffer_bytes,
		&DataType::UInt8,
		&directory,
		&encoding,
	)
	.unwrap();

	assert_eq!(decoded, input);
}
