use std::num::NonZeroU32;
use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use serde_bytes::ByteBuf;
use tokio::io::AsyncWriteExt;
use tokio::io::duplex;
use tokio::time::{Duration, timeout};
use toron::distributed::identity::QueryId;
use toron::sql_wire::{
	ResultBatch, SqlFrame, SqlMessage, SqlProtocolVersion, read_frame, read_request_frame_bounded,
	write_frame,
};

#[tokio::test]
async fn toron_sql_wire_round_trips_arrow_ipc_batch() {
	let schema = Arc::new(Schema::new(vec![Field::new(
		"count",
		DataType::Int64,
		false,
	)]));
	let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![3_i64]))])
		.expect("record batch");
	let mut ipc = Vec::new();
	{
		let mut writer = StreamWriter::try_new(&mut ipc, &batch.schema()).expect("ipc writer");
		writer.write(&batch).expect("write batch");
		writer.finish().expect("finish writer");
	}
	let query_id = QueryId::new();
	let frame = SqlFrame {
		protocol_version: SqlProtocolVersion::Current,
		query_id,
		message: SqlMessage::ResultBatch(ResultBatch {
			batch_index: 0,
			arrow_ipc_bytes: ByteBuf::from(ipc.clone()),
		}),
	};
	let (mut client, mut server) = duplex(4096);

	write_frame(&mut client, &frame).await.expect("write frame");
	let decoded = read_frame(&mut server).await.expect("read frame");

	assert_eq!(decoded, frame);
	match decoded.message {
		SqlMessage::ResultBatch(batch) => assert_eq!(batch.arrow_ipc_bytes.as_ref(), &ipc),
		other => panic!("unexpected message: {other:?}"),
	}
}

#[tokio::test]
async fn oversized_declared_request_length_rejected_before_allocation_or_decode() {
	let (mut client, mut server) = duplex(64);
	let max = NonZeroU32::new(8).expect("nonzero cap");
	client
		.write_all(&(max.get() + 1).to_le_bytes())
		.await
		.expect("write declared length");

	let rejection = timeout(
		Duration::from_millis(100),
		read_request_frame_bounded(&mut server, max),
	)
	.await
	.expect("bounded reader must reject from the prefix without waiting for payload")
	.expect("bounded read result")
	.expect_err("declared oversized request must reject");

	assert_eq!(rejection.declared_len, max.get() + 1);
	assert_eq!(rejection.max_len, max);
}

#[tokio::test]
async fn bounded_request_reader_accepts_frame_at_configured_limit() {
	let query_id = QueryId::new();
	let frame = SqlFrame::execute(query_id, "SELECT 1".to_string(), None);
	let (mut client, mut server) = duplex(4096);

	write_frame(&mut client, &frame).await.expect("write frame");
	let decoded = read_request_frame_bounded(
		&mut server,
		NonZeroU32::new(toron::sql_wire::DEFAULT_SQL_REQUEST_FRAME_BYTES).expect("default cap"),
	)
	.await
	.expect("bounded read")
	.expect("frame below cap");

	assert_eq!(decoded, frame);
}
