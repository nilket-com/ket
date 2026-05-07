use crate::schema::{IpcBufferIndex, RecordBodyByteLen, RowGroupIndex};
use crate::storage::encoding::ENCODING_DIRECTORY_METADATA_KEY;
use crate::storage::encoding::cascade::{decode_buffer, decode_page};
use crate::storage::encoding::directory::{
	deserialize_encoding_directory_value, validate_encoding_directory,
};
use crate::storage::encoding::page::expand_page_descriptors;
use crate::storage::encoding::{
	BufferEncoding, EncodingCascade, EncodingDirectory, EncodingRejectCode, EncodingWriterConfig,
};
use crate::storage::row_group::RecordBodyByteRange;
use crate::storage::row_group::{
	ROW_GROUP_DIRECTORY_METADATA_KEY, deserialize_page_directory_value, validate_page_directory,
};
use arrow::datatypes::DataType;
use arrow::ipc::MessageHeader;
use arrow::ipc::convert::fb_to_schema;
use arrow::ipc::r#gen::Message::{
	BodyCompression, BodyCompressionArgs, FieldNode, Message as FbMessage, MessageArgs,
	RecordBatch as FbRecordBatch, RecordBatchArgs,
};
use arrow::ipc::r#gen::Schema::Buffer as FbBuffer;
use arrow::ipc::root_as_message;
use std::io::{Error, ErrorKind};
use std::time::Instant;

pub fn materialize_encoding_buffer(
	encoded_buffer_bytes: &[u8],
	data_type: &DataType,
	directory: &EncodingDirectory,
	encoding: &BufferEncoding,
) -> Result<Vec<u8>, EncodingRejectCode> {
	let cascade = directory
		.cascades
		.get(encoding.cascade_index.get() as usize)
		.ok_or(EncodingRejectCode::CascadeUnsupported)?;
	let start = usize::try_from(encoding.encoded_range.start.get())
		.map_err(|_| EncodingRejectCode::PageRangeOverflow)?;
	let len = usize::try_from(encoding.encoded_range.len.get())
		.map_err(|_| EncodingRejectCode::PageRangeOverflow)?;
	let end = start
		.checked_add(len)
		.ok_or(EncodingRejectCode::PageRangeOverflow)?;
	if end > encoded_buffer_bytes.len() {
		return Err(EncodingRejectCode::PageRangeOverflow);
	}
	let cascade_label = cascade.kind().as_str();
	let started = Instant::now();
	let encoded = &encoded_buffer_bytes[start..end];
	let decoded = if encoding.page_end_offsets.is_empty() {
		decode_buffer(
			encoded,
			data_type,
			cascade,
			encoding.decoded_byte_len,
			encoding.decoded_value_count,
		)
	} else {
		decode_encoding_pages(encoded, data_type, cascade, encoding)
	};
	if decoded.is_ok() {
		crate::metrics::global()
			.observe_storage_l2_encoding_materialize(cascade_label, started.elapsed());
	}
	decoded
}

fn decode_encoding_pages(
	encoded: &[u8],
	data_type: &DataType,
	cascade: &EncodingCascade,
	encoding: &BufferEncoding,
) -> Result<Vec<u8>, EncodingRejectCode> {
	let Some(bytes_per_value) = decoded_fixed_width_bytes_per_value(encoding) else {
		return decode_buffer(
			encoded,
			data_type,
			cascade,
			encoding.decoded_byte_len,
			encoding.decoded_value_count,
		);
	};
	let pages = expand_page_descriptors(encoding)?;
	let mut out = Vec::with_capacity(encoding.decoded_byte_len.get() as usize);
	for page in pages {
		let start = usize::try_from(page.byte_range.start.get())
			.map_err(|_| EncodingRejectCode::PageRangeOverflow)?;
		let len = usize::try_from(page.byte_range.len.get())
			.map_err(|_| EncodingRejectCode::PageRangeOverflow)?;
		let end = start
			.checked_add(len)
			.ok_or(EncodingRejectCode::PageRangeOverflow)?;
		if end > encoded.len() {
			return Err(EncodingRejectCode::PageRangeOverflow);
		}
		let expected_bytes = usize::try_from(page.value_count.get())
			.map_err(|_| EncodingRejectCode::DecodedLengthMismatch)?
			.checked_mul(bytes_per_value)
			.ok_or(EncodingRejectCode::DecodedLengthMismatch)?;
		let decoded = decode_page(
			&encoded[start..end],
			data_type,
			cascade,
			RecordBodyByteLen::new(expected_bytes as u64),
			page.value_count,
		)?;
		out.extend_from_slice(&decoded);
	}
	if out.len() as u64 != encoding.decoded_byte_len.get() {
		return Err(EncodingRejectCode::DecodedLengthMismatch);
	}
	Ok(out)
}

fn decoded_fixed_width_bytes_per_value(encoding: &BufferEncoding) -> Option<usize> {
	let values = encoding.decoded_value_count.get() as usize;
	let bytes = usize::try_from(encoding.decoded_byte_len.get()).ok()?;
	if values == 0 || bytes == 0 || bytes % values != 0 {
		return None;
	}
	Some(bytes / values)
}

pub fn find_buffer_encoding<'a>(
	directory: &'a EncodingDirectory,
	row_group_index: RowGroupIndex,
	buffer_ordinal: IpcBufferIndex,
	_buffer_range: RecordBodyByteRange,
) -> Option<&'a BufferEncoding> {
	directory.buffers.iter().find(|encoding| {
		encoding.row_group_index == row_group_index && encoding.ipc_buffer_ordinal == buffer_ordinal
	})
}

pub fn should_attempt_encoding_materialization(config: &EncodingWriterConfig) -> bool {
	config.enabled
}

pub fn decoded_len_for_buffer(encoding: &BufferEncoding) -> RecordBodyByteLen {
	encoding.decoded_byte_len
}

pub fn ipc_stream_contains_encoding_directory(bytes: &[u8]) -> bool {
	bytes
		.windows(ENCODING_DIRECTORY_METADATA_KEY.len())
		.any(|window| window == ENCODING_DIRECTORY_METADATA_KEY.as_bytes())
}

pub fn materialize_encoding_ipc_stream(bytes: &[u8]) -> std::io::Result<Vec<u8>> {
	let mut offset = 0_usize;
	let schema_message = read_ipc_message_metadata(bytes, &mut offset)?.ok_or_else(|| {
		Error::new(
			ErrorKind::InvalidData,
			"Encoding IPC stream ended before schema message",
		)
	})?;
	if schema_message.header_type != MessageHeader::Schema {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"Encoding IPC stream did not start with schema message",
		));
	}
	let schema = fb_to_schema(
		schema_message
			.message
			.header_as_schema()
			.ok_or_else(|| Error::new(ErrorKind::InvalidData, "Encoding schema header missing"))?,
	);
	let Some(row_group_value) = schema.metadata().get(ROW_GROUP_DIRECTORY_METADATA_KEY) else {
		return Ok(bytes.to_vec());
	};
	let Some(encoding_value) = schema.metadata().get(ENCODING_DIRECTORY_METADATA_KEY) else {
		return Ok(bytes.to_vec());
	};
	let page_directory = deserialize_page_directory_value(row_group_value).map_err(|code| {
		Error::new(
			ErrorKind::InvalidData,
			format!("Encoding materialize row-group directory rejected: {code:?}"),
		)
	})?;
	let encoding_directory =
		deserialize_encoding_directory_value(encoding_value).map_err(|code| {
			Error::new(
				ErrorKind::InvalidData,
				format!("Encoding materialize directory rejected: {code:?}"),
			)
		})?;
	validate_page_directory(
		&page_directory,
		bytes.len() as u64,
		&schema,
		page_directory.logical_schema_fingerprint,
	)
	.map_err(|code| {
		Error::new(
			ErrorKind::InvalidData,
			format!("Encoding materialize row-group validation rejected: {code:?}"),
		)
	})?;
	validate_encoding_directory(
		&encoding_directory,
		&page_directory,
		&schema,
		encoding_directory.logical_schema_fingerprint,
	)
	.map_err(|code| {
		Error::new(
			ErrorKind::InvalidData,
			format!("Encoding materialize validation rejected: {code:?}"),
		)
	})?;
	let mut out = Vec::with_capacity(bytes.len());
	out.extend_from_slice(&bytes[schema_message.prefix_start..schema_message.body_end]);
	for group in &page_directory.row_groups {
		let message = read_ipc_message_metadata(bytes, &mut offset)?.ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				"Encoding materialize IPC stream ended before expected record batch",
			)
		})?;
		if message.header_type != MessageHeader::RecordBatch {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"Encoding materialize IPC stream contained non-record-batch message",
			));
		}
		let batch = message.message.header_as_record_batch().ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				"Encoding materialize record batch missing header",
			)
		})?;
		let flat_buffers = batch.buffers().ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				"Encoding materialize record batch missing buffers",
			)
		})?;
		let mut body = Vec::new();
		let mut rewritten_buffers = Vec::with_capacity(flat_buffers.len());
		for (buffer_ordinal, buffer) in flat_buffers.iter().enumerate() {
			let buffer_ordinal =
				IpcBufferIndex::new(u32::try_from(buffer_ordinal).map_err(|_| {
					Error::new(ErrorKind::InvalidData, "IPC buffer ordinal exceeds u32")
				})?);
			let physical_offset = usize::try_from(buffer.offset())
				.map_err(|_| Error::new(ErrorKind::InvalidData, "negative IPC buffer offset"))?;
			let physical_len = usize::try_from(buffer.length())
				.map_err(|_| Error::new(ErrorKind::InvalidData, "negative IPC buffer length"))?;
			let start = message
				.body_start
				.checked_add(physical_offset)
				.ok_or_else(|| {
					Error::new(ErrorKind::InvalidData, "Encoding buffer start overflow")
				})?;
			let end = start.checked_add(physical_len).ok_or_else(|| {
				Error::new(ErrorKind::InvalidData, "Encoding buffer end overflow")
			})?;
			if end > bytes.len() {
				return Err(Error::new(
					ErrorKind::InvalidData,
					"Encoding buffer range exceeds IPC stream",
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
					Error::new(
						ErrorKind::InvalidData,
						"Encoding materialize buffer missing row-group directory entry",
					)
				})?;
			let decoded = if let Some(encoding) =
				encoding_directory.buffers.iter().find(|encoding| {
					encoding.row_group_index == group.row_group_index
						&& encoding.ipc_column_ordinal == page.ipc_column_ordinal
						&& encoding.ipc_buffer_ordinal == buffer_ordinal
				}) {
				let field = schema
					.fields()
					.get(encoding.ipc_column_ordinal.get() as usize)
					.ok_or_else(|| {
						Error::new(
							ErrorKind::InvalidData,
							"Encoding column ordinal out of range",
						)
					})?;
				let decoded = materialize_encoding_buffer(
					&bytes[start..end],
					field.data_type(),
					&encoding_directory,
					encoding,
				)
				.map_err(|code| {
					Error::new(
						ErrorKind::InvalidData,
						format!("Encoding materialize buffer rejected: {code:?}"),
					)
				})?;
				if decoded.len() as u64 != encoding.decoded_byte_len.get() {
					return Err(Error::new(
						ErrorKind::InvalidData,
						"Encoding decoded buffer length mismatch",
					));
				}
				decoded
			} else {
				bytes[start..end].to_vec()
			};
			let dense_offset = body.len();
			body.extend_from_slice(&decoded);
			rewritten_buffers.push(FbBuffer::new(
				i64::try_from(dense_offset).map_err(|_| {
					Error::new(
						ErrorKind::InvalidData,
						"dense IPC buffer offset exceeds i64",
					)
				})?,
				i64::try_from(decoded.len()).map_err(|_| {
					Error::new(
						ErrorKind::InvalidData,
						"dense IPC buffer length exceeds i64",
					)
				})?,
			));
			pad_to_8(&mut body);
		}
		let metadata = build_record_batch_message(
			&message,
			batch,
			&rewritten_buffers,
			i64::try_from(body.len()).map_err(|_| {
				Error::new(ErrorKind::InvalidData, "dense IPC body length exceeds i64")
			})?,
		)?;
		append_ipc_message(&mut out, &metadata, &body)?;
	}
	append_ipc_stream_end(&mut out);
	Ok(out)
}

struct IpcMessageMetadata<'a> {
	message: arrow::ipc::Message<'a>,
	header_type: MessageHeader,
	prefix_start: usize,
	body_start: usize,
	body_end: usize,
}

fn read_ipc_message_metadata<'a>(
	bytes: &'a [u8],
	offset: &mut usize,
) -> std::io::Result<Option<IpcMessageMetadata<'a>>> {
	let prefix_start = *offset;
	if bytes.len().saturating_sub(*offset) < 4 {
		return Err(Error::new(
			ErrorKind::UnexpectedEof,
			"Encoding IPC message prefix truncated",
		));
	}
	let first = u32::from_le_bytes(bytes[*offset..*offset + 4].try_into().expect("fixed width"));
	*offset += 4;
	let metadata_len = if first == u32::MAX {
		if bytes.len().saturating_sub(*offset) < 4 {
			return Err(Error::new(
				ErrorKind::UnexpectedEof,
				"Encoding IPC continuation prefix truncated",
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
		Error::new(
			ErrorKind::InvalidData,
			"Encoding IPC metadata length overflow",
		)
	})?;
	if metadata_end > bytes.len() {
		return Err(Error::new(
			ErrorKind::UnexpectedEof,
			"Encoding IPC metadata truncated",
		));
	}
	let message = root_as_message(&bytes[metadata_start..metadata_end]).map_err(|err| {
		Error::new(
			ErrorKind::InvalidData,
			format!("Encoding IPC message flatbuffer parse: {err}"),
		)
	})?;
	let body_len = usize::try_from(message.bodyLength()).map_err(|_| {
		Error::new(
			ErrorKind::InvalidData,
			"negative Encoding IPC message body length",
		)
	})?;
	let body_start = metadata_end;
	let body_end = body_start
		.checked_add(body_len)
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "Encoding IPC body length overflow"))?;
	if body_end > bytes.len() {
		return Err(Error::new(
			ErrorKind::UnexpectedEof,
			"Encoding IPC body truncated",
		));
	}
	*offset = body_end;
	Ok(Some(IpcMessageMetadata {
		header_type: message.header_type(),
		message,
		prefix_start,
		body_start,
		body_end,
	}))
}

fn build_record_batch_message(
	source: &IpcMessageMetadata<'_>,
	batch: FbRecordBatch<'_>,
	buffers: &[FbBuffer],
	body_len: i64,
) -> std::io::Result<Vec<u8>> {
	if source.message.custom_metadata().is_some() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"Encoding materialize RecordBatch custom metadata is unsupported",
		));
	}
	let nodes = batch.nodes().ok_or_else(|| {
		Error::new(
			ErrorKind::InvalidData,
			"Encoding materialize RecordBatch missing field nodes",
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
			.map_err(|_| Error::new(ErrorKind::InvalidData, "IPC metadata exceeds u32"))?
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
	let with_prefix = flatbuffer_len
		.checked_add(8)
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "IPC metadata length overflow"))?;
	align_to_8(with_prefix)
		.checked_sub(8)
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "IPC metadata aligned underflow"))
}

fn pad_to_8(bytes: &mut Vec<u8>) {
	let target = align_to_8(bytes.len());
	bytes.resize(target, 0);
}

fn align_to_8(value: usize) -> usize {
	(value + 7) & !7
}
