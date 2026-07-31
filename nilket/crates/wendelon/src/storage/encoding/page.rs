use crate::schema::{BufferValueCount, BufferValueIndex, EncodingPageIndex, RecordBodyByteLen};
use crate::storage::encoding::{
	BufferEncoding, EncodedBufferByteRange, EncodingPageDescriptor, EncodingRejectCode,
};

pub fn expand_page_descriptors(
	encoding: &BufferEncoding,
) -> Result<Vec<EncodingPageDescriptor>, EncodingRejectCode> {
	let mut pages = Vec::with_capacity(encoding.page_end_offsets.len());
	let mut previous_byte = 0_u64;
	let mut first_value = 0_u32;
	for (index, end_offset) in encoding.page_end_offsets.iter().enumerate() {
		let end = u64::from(end_offset.get());
		if end <= previous_byte || end > encoding.encoded_range.len.get() {
			return Err(EncodingRejectCode::PageRangeOverflow);
		}
		let remaining = encoding
			.decoded_value_count
			.get()
			.saturating_sub(first_value);
		let value_count = remaining.min(encoding.page_value_count.get());
		pages.push(EncodingPageDescriptor {
			page_index: EncodingPageIndex::new(index as u32),
			first_value_index: BufferValueIndex::new(first_value),
			value_count: BufferValueCount::new(value_count),
			byte_range: EncodedBufferByteRange {
				start: crate::schema::EncodingPageByteOffset::new(previous_byte as u32),
				len: RecordBodyByteLen::new(end - previous_byte),
			},
		});
		previous_byte = end;
		first_value = first_value.saturating_add(value_count);
	}
	Ok(pages)
}
