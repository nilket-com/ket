use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use serde_bytes::ByteBuf;
use tokio::io::duplex;
use toron::distributed::identity::QueryId;
use toron::sql_wire::{
	ResultBatch, SqlFrame, SqlMessage, SqlProtocolVersion, read_frame, write_frame,
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
