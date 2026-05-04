use arrow::array::{Int32Array, LargeBinaryArray, LargeBinaryBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::{MessageHeader, root_as_message};
use arrow::record_batch::RecordBatch;
use std::ops::Range;
use std::sync::Arc;
use tokio::io;
use wendelon::metrics;
use wendelon::network::core::{HRFT_MAGIC, read_frame, write_frame};
use wendelon::network::ipc::{wire_decode_batch, wire_encode_batch};
use wendelon::network::pool::BufPool;
use wendelon::schema::{
	FrameHeader, HRFT_ENVELOPE_VERSION, MsgType, RecordBodyCompressionPolicy,
	decode_arrow_schema_ipc, encode_arrow_schema_ipc,
};
use wendelon::storage::ipc::{
	encode_record_batch, encode_record_batch_bytes, record_batch_compression_policy_from_ipc,
};

fn sample_batch() -> RecordBatch {
	let schema = Arc::new(Schema::new(vec![
		Field::new("id", DataType::Int32, false),
		Field::new("payload", DataType::LargeBinary, false),
	]));
	let ids = Arc::new(Int32Array::from(vec![1, 2, 3]));
	let mut payloads = LargeBinaryBuilder::new();
	payloads.append_value(b"alpha-alpha-alpha-alpha");
	payloads.append_value(b"beta-beta-beta-beta");
	payloads.append_value(b"gamma-gamma-gamma-gamma");
	RecordBatch::try_new(schema, vec![ids, Arc::new(payloads.finish())]).unwrap()
}

fn compressible_batch() -> RecordBatch {
	let schema = Arc::new(Schema::new(vec![
		Field::new("id", DataType::Int32, false),
		Field::new("payload", DataType::LargeBinary, false),
	]));
	let ids = Arc::new(Int32Array::from(vec![10, 20, 30]));
	let mut payloads = LargeBinaryBuilder::new();
	payloads.append_value(vec![b'a'; 16 * 1024]);
	payloads.append_value(vec![b'b'; 16 * 1024]);
	payloads.append_value(vec![b'c'; 16 * 1024]);
	RecordBatch::try_new(schema, vec![ids, Arc::new(payloads.finish())]).unwrap()
}

fn assert_batch_eq(expected: &RecordBatch, actual: &RecordBatch) {
	assert_eq!(expected.num_rows(), actual.num_rows());
	assert_eq!(expected.num_columns(), actual.num_columns());
	assert_eq!(expected.schema().fields(), actual.schema().fields());

	let expected_ids = expected
		.column(0)
		.as_any()
		.downcast_ref::<Int32Array>()
		.unwrap();
	let actual_ids = actual
		.column(0)
		.as_any()
		.downcast_ref::<Int32Array>()
		.unwrap();
	assert_eq!(expected_ids.values(), actual_ids.values());

	let expected_payloads = expected
		.column(1)
		.as_any()
		.downcast_ref::<LargeBinaryArray>()
		.unwrap();
	let actual_payloads = actual
		.column(1)
		.as_any()
		.downcast_ref::<LargeBinaryArray>()
		.unwrap();
	for row in 0..expected.num_rows() {
		assert_eq!(expected_payloads.value(row), actual_payloads.value(row));
	}
}

#[test]
fn fresh_wire_encode_uses_lz4_frame_and_roundtrips() {
	let batch = sample_batch();

	let encoded = wire_encode_batch(&batch).unwrap();
	let policy = record_batch_compression_policy_from_ipc(&encoded).unwrap();
	let decoded = wire_decode_batch(&encoded).unwrap();

	assert_eq!(policy, RecordBodyCompressionPolicy::Lz4Frame);
	assert_batch_eq(&batch, &decoded);
}

#[test]
fn wire_decode_accepts_pre_adr053_uncompressed_ipc() {
	let batch = sample_batch();
	let encoded =
		encode_record_batch_bytes(&batch, RecordBodyCompressionPolicy::Uncompressed).unwrap();

	let policy = record_batch_compression_policy_from_ipc(&encoded).unwrap();
	let decoded = wire_decode_batch(&encoded).unwrap();

	assert_eq!(policy, RecordBodyCompressionPolicy::Uncompressed);
	assert_batch_eq(&batch, &decoded);
}

#[tokio::test]
async fn storage_pass_through_frame_preserves_arrow_payload_bytes() {
	let batch = sample_batch();
	let encoded = encode_record_batch(&batch, RecordBodyCompressionPolicy::Lz4Frame).unwrap();
	let header = FrameHeader {
		magic: HRFT_MAGIC,
		version: HRFT_ENVELOPE_VERSION,
		msg_type: MsgType::READ_RESP.0,
		flags: 0,
		stream_id: 17,
		meta_len: 0,
		arrow_len: encoded.bytes.len() as u32,
		trailing_len: 0,
	};
	let (mut tx, mut rx) = io::duplex(encoded.bytes.len() + 128);
	write_frame(&mut tx, &header, &[], Some(&encoded.bytes), None)
		.await
		.unwrap();
	drop(tx);

	let pool = BufPool::new(2, 1024 * 1024);
	let frame = read_frame(&mut rx, &pool).await.unwrap();
	let arrow = frame.arrow_payload.as_ref().unwrap();
	let decoded = wire_decode_batch(arrow).unwrap();

	assert_eq!(arrow.as_ref(), encoded.bytes.as_slice());
	assert_batch_eq(&batch, &decoded);
}

#[test]
fn descriptor_schema_ipc_stays_schema_only_and_uncompressed() {
	let schema = sample_batch().schema();

	let encoded = encode_arrow_schema_ipc(schema.as_ref()).unwrap();
	let decoded = decode_arrow_schema_ipc(&encoded).unwrap();
	let policy = record_batch_compression_policy_from_ipc(&encoded);

	assert_eq!(schema.fields(), decoded.fields());
	assert!(
		policy.is_err(),
		"schema-only IPC unexpectedly carried a record batch policy"
	);
}

#[test]
fn tampered_lz4_wire_payload_fails_decode_and_increments_metric() {
	let batch = compressible_batch();
	let mut encoded = wire_encode_batch(&batch).unwrap();
	let compressed_range = compressed_body_buffer_range(&encoded);
	let tamper_offset = compressed_range.start;
	encoded[tamper_offset] ^= 0x7f;
	let before = decode_error_metric_value();

	let err = wire_decode_batch(&encoded).unwrap_err();
	let after = decode_error_metric_value();

	assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
	assert!(
		after > before,
		"decode-error metric did not increment: before={before} after={after}"
	);
}

fn decode_error_metric_value() -> u64 {
	let metric = "wendelon_network_compression_decode_error_total{codec=\"lz4_frame\"}";
	metrics::global()
		.render_prometheus()
		.lines()
		.find_map(|line| {
			let (name, value) = line.split_once(' ')?;
			(name == metric).then(|| value.parse().unwrap())
		})
		.unwrap_or(0)
}

fn compressed_body_buffer_range(bytes: &[u8]) -> Range<usize> {
	let mut offset = 0_usize;
	while offset < bytes.len() {
		let Some((message_start, metadata_len)) = read_message_prefix(bytes, &mut offset) else {
			break;
		};
		let message_end = message_start + metadata_len;
		let message = root_as_message(&bytes[message_start..message_end]).unwrap();
		let body_len = usize::try_from(message.bodyLength()).unwrap();
		let body_start = message_end;
		let body_end = body_start + body_len;
		if message.header_type() == MessageHeader::RecordBatch {
			let record_batch = message.header_as_record_batch().unwrap();
			let buffers = record_batch.buffers().unwrap();
			for buffer in buffers {
				let start = body_start + usize::try_from(buffer.offset()).unwrap();
				let len = usize::try_from(buffer.length()).unwrap();
				if len <= 16 || start + len > body_end {
					continue;
				}
				let prefix = i64::from_le_bytes(bytes[start..start + 8].try_into().unwrap());
				if prefix >= 0 {
					return start + 8..start + len;
				}
			}
			panic!("record batch contained no compressed body buffer");
		}
		offset = body_end + pad_to_alignment(8, body_len);
	}
	panic!("record batch message not found");
}

fn read_message_prefix(bytes: &[u8], offset: &mut usize) -> Option<(usize, usize)> {
	let first = u32::from_le_bytes(bytes[*offset..*offset + 4].try_into().unwrap());
	*offset += 4;
	let metadata_len = if first == u32::MAX {
		let len = u32::from_le_bytes(bytes[*offset..*offset + 4].try_into().unwrap());
		*offset += 4;
		len as usize
	} else {
		first as usize
	};
	(metadata_len != 0).then_some((*offset, metadata_len))
}

fn pad_to_alignment(alignment: usize, len: usize) -> usize {
	let mask = alignment - 1;
	((len + mask) & !mask) - len
}
