#[path = "support/control_ring.rs"]
mod support;

use arrow::array::LargeBinaryArray;
use arrow::array::LargeBinaryBuilder;
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use std::io::{Cursor, Error, ErrorKind};
use std::sync::Arc;
use wendelon::network::core::{HRFT_MAGIC, HrftFrame, read_frame, write_frame};
use wendelon::network::server::HrftService;
use wendelon::schema::{
	ClientWriteId, Consistency, DurabilityLevel, FrameHeader, HybridTimestamp, MsgType, NodeId,
	ReadRequestMeta, ReadResponseMeta, ReadStatus, WriteRequestMeta, WriteResponseMeta,
	WriteStatus, read_key_schema, write_payload_schema,
};

fn hlc(physical_ms: u64, logical: u32, node_id: u32) -> HybridTimestamp {
	HybridTimestamp {
		physical_ms,
		logical,
		node_id,
	}
}

fn write_meta(epoch: u64, key: &[u8], value: &[u8], sequence_no: u64) -> WriteRequestMeta {
	WriteRequestMeta {
		write_id: ClientWriteId {
			client_uuid: [3u8; 16],
			sequence_no,
			idempotency_ttl_ms: 60_000,
			_pad: 0,
		},
		client_hlc: hlc(1, 0, 77),
		expected_epoch: epoch,
		proxy_hop: 0,
		durability: DurabilityLevel::QUORUM,
		await_finalization: 0,
		_pad1: [0; 5],
		proxy_source: NodeId {
			id: 0,
			_reserved: 0,
		},
		key_len: key.len() as u32,
		value_len: value.len() as u32,
	}
}

fn read_meta(expected_epoch: u64, key: &[u8], min_hlc: HybridTimestamp) -> ReadRequestMeta {
	ReadRequestMeta {
		min_hlc,
		expected_epoch,
		consistency: Consistency::LINEARIZABLE,
		_pad: [0; 3],
		key_len: key.len() as u32,
		_pad2: [0; 8],
		as_of_hlc: hlc(0, 0, 0),
	}
}

fn kv_batch(key: &[u8], value: &[u8]) -> RecordBatch {
	let schema = Arc::new(write_payload_schema());
	let mut keys = LargeBinaryBuilder::new();
	keys.append_value(key);
	let mut values = LargeBinaryBuilder::new();
	values.append_value(value);
	RecordBatch::try_new(
		schema,
		vec![Arc::new(keys.finish()), Arc::new(values.finish())],
	)
	.unwrap()
}

fn key_batch(key: &[u8]) -> RecordBatch {
	let schema = Arc::new(read_key_schema());
	let mut keys = LargeBinaryBuilder::new();
	keys.append_value(key);
	RecordBatch::try_new(schema, vec![Arc::new(keys.finish())]).unwrap()
}

fn decode_single_batch(bytes: &[u8]) -> std::io::Result<RecordBatch> {
	let mut reader = StreamReader::try_new(Cursor::new(bytes), None)
		.map_err(|e| Error::new(ErrorKind::InvalidData, format!("arrow open: {e}")))?;
	match reader.next() {
		Some(Ok(batch)) => Ok(batch),
		Some(Err(e)) => Err(Error::new(
			ErrorKind::InvalidData,
			format!("arrow read: {e}"),
		)),
		None => Err(Error::new(
			ErrorKind::InvalidData,
			"arrow stream contained no batch",
		)),
	}
}

fn extract_value(batch: &RecordBatch) -> Vec<u8> {
	let array: &LargeBinaryArray = batch
		.column_by_name("value")
		.unwrap()
		.as_any()
		.downcast_ref()
		.unwrap();
	array.value(0).to_vec()
}

async fn refresh_leases(node: &wendelon::node::WendelonNode) {
	support::wait_for_local_leader(node).await;
	node.data_server
		.refresh_leases_once(&node.ring, &*node.clock, wendelon::node::LEASE_TTL_MS)
		.await;
}

async fn send_bulk_item(
	stream: &mut dyn wendelon::network::transport::TransportStream,
	meta: &WriteRequestMeta,
	batch: &RecordBatch,
) -> std::io::Result<()> {
	let arrow = support::encode_batch(batch)?;
	let header = FrameHeader {
		magic: HRFT_MAGIC,
		version: 1,
		msg_type: MsgType::BULK_WRITE_REQ.0,
		flags: 0,
		stream_id: 0,
		meta_len: std::mem::size_of::<WriteRequestMeta>() as u32,
		arrow_len: arrow.len() as u32,
		trailing_len: 0,
	};
	write_frame(
		stream,
		&header,
		bytemuck::bytes_of(meta),
		Some(&arrow),
		None,
	)
	.await
}

async fn read_bulk_responses(
	stream: &mut dyn wendelon::network::transport::TransportStream,
	pool: &Arc<wendelon::network::pool::BufPool>,
) -> std::io::Result<Vec<HrftFrame>> {
	let mut frames = Vec::new();
	loop {
		match read_frame(stream, pool).await {
			Ok(frame) => frames.push(frame),
			Err(e) if e.kind() == ErrorKind::UnexpectedEof => return Ok(frames),
			Err(e) => return Err(e),
		}
	}
}

#[tokio::test]
async fn bulk_write_round_trips_many_items_over_one_hrft_stream() {
	let node = support::bootstrap_node("bulk-write-large", 1).await;
	refresh_leases(&node).await;

	let service: Arc<dyn HrftService> = node.router.clone();
	let (factory, pool) = support::loopback_factory(node.local_node, service);
	let conn = factory.get_connection(node.local_node).await.unwrap();
	let mut stream = conn.open_bi().await.unwrap();

	let item_count = 24usize;
	let value_len = 256 * 1024;
	let mut expected = Vec::new();
	for seq in 0..item_count {
		let key = format!("bulk-key-{seq}");
		let fill = b'a' + (seq as u8 % 26);
		let value = vec![fill; value_len];
		let meta = write_meta(1, key.as_bytes(), &value, seq as u64 + 1);
		let batch = kv_batch(key.as_bytes(), &value);
		send_bulk_item(&mut *stream, &meta, &batch).await.unwrap();
		expected.push((key.into_bytes(), value));
	}
	stream.finish().await.unwrap();

	let responses = read_bulk_responses(&mut *stream, &pool).await.unwrap();
	assert_eq!(responses.len(), item_count);
	let mut last_hlc = hlc(0, 0, 0);
	for frame in &responses {
		assert_eq!(frame.header.msg_type, MsgType::BULK_WRITE_RESP.0);
		let meta: WriteResponseMeta =
			*bytemuck::from_bytes(&frame.meta[..std::mem::size_of::<WriteResponseMeta>()]);
		assert_eq!(meta.status, WriteStatus::OK);
		assert!(meta.commit_hlc > last_hlc);
		last_hlc = meta.commit_hlc;
	}

	for index in [0usize, item_count - 1] {
		let (key, value) = &expected[index];
		let read_arrow = support::encode_batch(&key_batch(key)).unwrap();
		let read_resp = support::call_frame(
			&factory,
			&pool,
			node.local_node,
			MsgType::READ_REQ,
			bytemuck::bytes_of(&read_meta(1, key, last_hlc)),
			Some(&read_arrow),
			None,
		)
		.await
		.unwrap();
		let read_meta: ReadResponseMeta =
			*bytemuck::from_bytes(&read_resp.meta[..std::mem::size_of::<ReadResponseMeta>()]);
		assert_eq!(read_meta.status, ReadStatus::OK);
		let batch = decode_single_batch(read_resp.arrow_payload.as_ref().unwrap()).unwrap();
		assert_eq!(extract_value(&batch), *value);
	}

	support::abort_node(&node);
}

#[tokio::test]
async fn bulk_write_commit_hlcs_are_merged_by_the_leader() {
	let node = support::bootstrap_node("bulk-write-hlc", 1).await;
	refresh_leases(&node).await;

	let service: Arc<dyn HrftService> = node.router.clone();
	let (factory, pool) = support::loopback_factory(node.local_node, service);
	let conn = factory.get_connection(node.local_node).await.unwrap();
	let mut stream = conn.open_bi().await.unwrap();

	let client_hlc = hlc(1, 0, 77);
	for seq in 0..3u64 {
		let key = format!("hlc-key-{seq}");
		let value = format!("value-{seq}");
		let meta = write_meta(1, key.as_bytes(), value.as_bytes(), seq + 1);
		let batch = kv_batch(key.as_bytes(), value.as_bytes());
		send_bulk_item(&mut *stream, &meta, &batch).await.unwrap();
	}
	stream.finish().await.unwrap();

	let responses = read_bulk_responses(&mut *stream, &pool).await.unwrap();
	assert_eq!(responses.len(), 3);

	let mut last_commit = client_hlc;
	for frame in responses {
		let meta: WriteResponseMeta =
			*bytemuck::from_bytes(&frame.meta[..std::mem::size_of::<WriteResponseMeta>()]);
		assert_eq!(meta.status, WriteStatus::OK);
		assert_eq!(meta.commit_hlc.node_id, node.local_node.id);
		assert!(meta.commit_hlc > client_hlc);
		assert!(meta.commit_hlc > last_commit);
		last_commit = meta.commit_hlc;
	}

	support::abort_node(&node);
}
