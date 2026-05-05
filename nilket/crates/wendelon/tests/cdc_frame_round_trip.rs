use arrow::array::Int32Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use wendelon::network::cdc::{cdc_emit_empty_frame, cdc_emit_frame, cdc_emit_reject};
use wendelon::network::core::read_frame;
use wendelon::network::pool::BufPool;
use wendelon::schema::{
	CdcCursor, CdcFrameMeta, CdcFrameSource, CdcRejectCode, CdcRejectMeta, CdcSubscriptionId,
	FRAME_FLAG_STREAM_END, HybridTimestamp, Identity, MsgType, RecordRowIndex, RowCount,
};

fn hlc(ms: u64) -> HybridTimestamp {
	HybridTimestamp {
		physical_ms: ms,
		logical: 0,
		node_id: 1,
	}
}

fn batch() -> RecordBatch {
	let schema = Arc::new(Schema::new(vec![Field::new(
		"value",
		DataType::Int32,
		false,
	)]));
	RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))]).unwrap()
}

#[tokio::test]
async fn cdc_frame_round_trip_uses_cdc_msg_type_and_compressed_arrow_path() {
	let (mut client, mut server) = tokio::io::duplex(128 * 1024);
	let payload = batch();
	let meta = CdcFrameMeta {
		subscription_id: CdcSubscriptionId::new(7),
		frame_low_cursor: CdcCursor::new(hlc(10), Identity::new([1; 32]), RecordRowIndex::ZERO),
		frame_high_cursor: CdcCursor::new(hlc(12), Identity::new([2; 32]), RecordRowIndex::ZERO),
		logical_schema_fingerprint: [4; 32],
		event_count: RowCount::new(3),
		source_raw: CdcFrameSource::Ring.as_raw(),
		_pad: [0; 7],
	};

	cdc_emit_frame(&mut server, 0, &meta, &payload, 0)
		.await
		.unwrap();
	drop(server);

	let pool = BufPool::new(4, 1024 * 1024);
	let frame = read_frame(&mut client, &pool).await.unwrap();
	assert_eq!(frame.header.msg_type, MsgType::CDC_FRAME.0);
	let decoded_meta: CdcFrameMeta =
		*bytemuck::from_bytes(&frame.meta[..std::mem::size_of::<CdcFrameMeta>()]);
	assert_eq!(decoded_meta.subscription_id, CdcSubscriptionId::new(7));
	assert_eq!(decoded_meta.source().unwrap(), CdcFrameSource::Ring);
	let decoded = wendelon::network::ipc::wire_decode_batch(
		frame
			.arrow_payload
			.as_ref()
			.expect("cdc frame arrow payload"),
	)
	.unwrap();
	assert_eq!(decoded.num_rows(), 3);
}

#[tokio::test]
async fn cdc_frame_round_trip_reject_is_typed_and_terminal() {
	for code in CdcRejectCode::ALL {
		let (mut client, mut server) = tokio::io::duplex(4096);
		let meta = CdcRejectMeta::new(CdcSubscriptionId::new(99), code);

		cdc_emit_reject(&mut server, 0, &meta).await.unwrap();
		drop(server);

		let pool = BufPool::new(4, 1024 * 1024);
		let frame = read_frame(&mut client, &pool).await.unwrap();
		assert_eq!(frame.header.msg_type, MsgType::CDC_REJECT.0);
		assert_eq!(
			frame.header.flags & FRAME_FLAG_STREAM_END,
			FRAME_FLAG_STREAM_END
		);
		let decoded_meta: CdcRejectMeta =
			*bytemuck::from_bytes(&frame.meta[..std::mem::size_of::<CdcRejectMeta>()]);
		assert_eq!(decoded_meta.code().unwrap(), code);
		assert!(frame.arrow_payload.is_none());
	}
}

#[tokio::test]
async fn cdc_frame_round_trip_empty_stream_end_has_no_arrow_payload() {
	let (mut client, mut server) = tokio::io::duplex(4096);
	let cursor = CdcCursor::new(hlc(50), Identity::new([5; 32]), RecordRowIndex::MAX);
	let meta = CdcFrameMeta {
		subscription_id: CdcSubscriptionId::new(7),
		frame_low_cursor: cursor,
		frame_high_cursor: cursor,
		logical_schema_fingerprint: [4; 32],
		event_count: RowCount::ZERO,
		source_raw: CdcFrameSource::Catchup.as_raw(),
		_pad: [0; 7],
	};

	cdc_emit_empty_frame(&mut server, 0, &meta, true)
		.await
		.unwrap();
	drop(server);

	let pool = BufPool::new(4, 1024 * 1024);
	let frame = read_frame(&mut client, &pool).await.unwrap();
	assert_eq!(frame.header.msg_type, MsgType::CDC_FRAME.0);
	assert_eq!(
		frame.header.flags & FRAME_FLAG_STREAM_END,
		FRAME_FLAG_STREAM_END
	);
	let decoded_meta: CdcFrameMeta =
		*bytemuck::from_bytes(&frame.meta[..std::mem::size_of::<CdcFrameMeta>()]);
	assert_eq!(decoded_meta.frame_high_cursor, cursor);
	assert_eq!(decoded_meta.event_count, RowCount::ZERO);
	assert_eq!(decoded_meta.source().unwrap(), CdcFrameSource::Catchup);
	assert!(frame.arrow_payload.is_none());
}
