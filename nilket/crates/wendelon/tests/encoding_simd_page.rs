use wendelon::schema::{
	BufferValueCount, EncodingCascadeIndex, EncodingPageByteOffset, IpcBufferIndex,
	IpcColumnOrdinal, RecordBodyByteLen, RowGroupIndex, SimdPageValueCount,
};
use wendelon::storage::encoding::page::expand_page_descriptors;
use wendelon::storage::{BufferEncoding, EncodedBufferByteRange};

#[test]
fn page_descriptors_expand_prefix_end_offsets_without_serialized_rows() {
	let encoding = BufferEncoding {
		row_group_index: RowGroupIndex::ZERO,
		ipc_column_ordinal: IpcColumnOrdinal::ZERO,
		ipc_buffer_ordinal: IpcBufferIndex::ZERO,
		cascade_index: EncodingCascadeIndex::ZERO,
		decoded_byte_len: RecordBodyByteLen::new(10),
		decoded_value_count: BufferValueCount::new(10),
		encoded_range: EncodedBufferByteRange {
			start: EncodingPageByteOffset::ZERO,
			len: RecordBodyByteLen::new(10),
		},
		page_value_count: SimdPageValueCount::new(4),
		page_end_offsets: vec![
			EncodingPageByteOffset::new(3),
			EncodingPageByteOffset::new(7),
			EncodingPageByteOffset::new(10),
		],
		page_stats: Vec::new(),
	};

	let pages = expand_page_descriptors(&encoding).unwrap();

	assert_eq!(pages.len(), 3);
	assert_eq!(pages[0].first_value_index.get(), 0);
	assert_eq!(pages[0].value_count.get(), 4);
	assert_eq!(pages[0].byte_range.len.get(), 3);
	assert_eq!(pages[1].first_value_index.get(), 4);
	assert_eq!(pages[1].value_count.get(), 4);
	assert_eq!(pages[1].byte_range.start.get(), 3);
	assert_eq!(pages[2].first_value_index.get(), 8);
	assert_eq!(pages[2].value_count.get(), 2);
	assert_eq!(pages[2].byte_range.start.get(), 7);
}
