#[path = "support/control_ring.rs"]
mod support;

use arrow::array::LargeBinaryBuilder;
use arrow::record_batch::RecordBatch;
use std::io::{Error, ErrorKind};
use std::sync::Arc;
use std::time::Duration;
use wendelon::network::core::{HRFT_MAGIC, read_frame, write_frame};
use wendelon::network::data_server::STREAM_FINALIZATION_BUFFER;
use wendelon::network::server::HrftService;
use wendelon::network::traits::DataRing;
use wendelon::schema::{
	ClientWriteId, DurabilityLevel, FinalizationMarker, FrameHeader, HybridTimestamp, Identity,
	MsgType, NodeId, StreamFinalizationRequestMeta, TenantId, WriteRequestMeta, WriteResponseMeta,
	WriteStatus, write_payload_schema,
};

fn tenant() -> TenantId {
	TenantId::ZERO
}

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
			client_uuid: [5u8; 16],
			sequence_no,
			idempotency_ttl_ms: 60_000,
			_pad: 0,
		},
		client_hlc: hlc(10 + sequence_no, 0, 1),
		expected_epoch: epoch,
		tenant_id: tenant(),
		proxy_hop: 0,
		durability: DurabilityLevel::QUORUM,
		await_finalization: 0,
		_pad1: [0; 5],
		proxy_source: NodeId {
			id: 0,
			_reserved: 0,
		},
		key_identity: Identity::blake3_tenant(tenant(), key),
		key_len: key.len() as u32,
		value_len: value.len() as u32,
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

async fn refresh_leases(node: &wendelon::node::WendelonNode) {
	support::wait_for_local_leader(node).await;
	node.data_server
		.refresh_leases_once(&node.ring, &*node.clock, wendelon::node::LEASE_TTL_MS)
		.await;
}

async fn write_once(
	factory: &Arc<wendelon::network::factory::TransportProviderFactory>,
	pool: &Arc<wendelon::network::pool::BufPool>,
	target: NodeId,
	sequence_no: u64,
) -> WriteResponseMeta {
	let key = format!("stream-fin-key-{sequence_no}");
	let value = format!("stream-fin-value-{sequence_no}");
	let arrow = support::encode_batch(&kv_batch(key.as_bytes(), value.as_bytes())).unwrap();
	let resp = support::call_frame(
		factory,
		pool,
		target,
		MsgType::WRITE_REQ,
		bytemuck::bytes_of(&write_meta(
			1,
			key.as_bytes(),
			value.as_bytes(),
			sequence_no,
		)),
		Some(&arrow),
		None,
	)
	.await
	.unwrap();
	let meta: WriteResponseMeta =
		*bytemuck::from_bytes(&resp.meta[..std::mem::size_of::<WriteResponseMeta>()]);
	assert_eq!(meta.status, WriteStatus::OK);
	meta
}

async fn open_finalization_stream(
	factory: &Arc<wendelon::network::factory::TransportProviderFactory>,
	target: NodeId,
	start_log_index: u64,
	epoch: u64,
) -> std::io::Result<Box<dyn wendelon::network::transport::TransportStream>> {
	let conn = factory.get_connection(target).await?;
	let mut stream = conn.open_bi().await?;
	let request = StreamFinalizationRequestMeta {
		start_log_index,
		epoch,
	};
	let header = FrameHeader {
		magic: HRFT_MAGIC,
		version: 1,
		msg_type: MsgType::STREAM_FIN_REQ.0,
		flags: 0,
		stream_id: 0,
		meta_len: std::mem::size_of::<StreamFinalizationRequestMeta>() as u32,
		arrow_len: 0,
		trailing_len: 0,
	};
	write_frame(
		&mut *stream,
		&header,
		bytemuck::bytes_of(&request),
		None,
		None,
	)
	.await?;
	stream.finish().await?;
	Ok(stream)
}

async fn read_marker(
	stream: &mut dyn wendelon::network::transport::TransportStream,
	pool: &Arc<wendelon::network::pool::BufPool>,
) -> std::io::Result<FinalizationMarker> {
	let frame = read_frame(stream, pool).await?;
	if frame.header.msg_type != MsgType::STREAM_FIN_EVT.0 {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"expected STREAM_FIN_EVT, got msg_type=0x{:02x}",
				frame.header.msg_type
			),
		));
	}
	if frame.meta.len() < std::mem::size_of::<FinalizationMarker>() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"STREAM_FIN_EVT meta too small",
		));
	}
	Ok(*bytemuck::from_bytes(
		&frame.meta[..std::mem::size_of::<FinalizationMarker>()],
	))
}

#[tokio::test]
async fn stream_finalization_replays_after_floor_and_stays_ordered_under_write_load() {
	let node = support::bootstrap_node("stream-finalization-order", 1).await;
	refresh_leases(&node).await;

	let service: Arc<dyn HrftService> = node.router.clone();
	let (factory, pool) = support::loopback_factory(node.local_node, service);

	let seed_a = write_once(&factory, &pool, node.local_node, 1).await;
	let seed_b = write_once(&factory, &pool, node.local_node, 2).await;
	let seed_c = write_once(&factory, &pool, node.local_node, 3).await;

	let mut stream = open_finalization_stream(&factory, node.local_node, seed_a.log_index, 1)
		.await
		.unwrap();

	let live_writes = 48u64;
	let mut expected: Vec<WriteResponseMeta> = vec![seed_b, seed_c];
	for seq in 4..(4 + live_writes) {
		expected.push(write_once(&factory, &pool, node.local_node, seq).await);
	}

	let mut markers = Vec::new();
	for _ in 0..expected.len() {
		markers.push(read_marker(&mut *stream, &pool).await.unwrap());
	}

	let observed_indexes: Vec<u64> = markers.iter().map(|m| m.log_index).collect();
	let expected_indexes: Vec<u64> = expected.iter().map(|m| m.log_index).collect();
	assert_eq!(observed_indexes, expected_indexes);
	assert!(markers.windows(2).all(|w| w[0].log_index < w[1].log_index));
	assert!(markers.iter().all(|m| m.epoch == 1));
	assert_eq!(markers[0].log_index, seed_b.log_index);

	support::abort_node(&node);
}

#[tokio::test]
async fn slow_finalization_consumer_is_dropped_without_stalling_writes() {
	let node = support::bootstrap_node("stream-finalization-slow", 1).await;
	refresh_leases(&node).await;

	let mut rx = node.data_server.stream_finalization(0, 1).await.unwrap();
	let write_count = STREAM_FINALIZATION_BUFFER as u64 + 32;

	let writes = tokio::time::timeout(Duration::from_secs(3), async {
		let mut metas = Vec::new();
		for seq in 1..=write_count {
			let key = format!("slow-consumer-key-{seq}");
			let value = format!("slow-consumer-value-{seq}");
			let meta = node
				.data_server
				.write(
					write_meta(1, key.as_bytes(), value.as_bytes(), seq),
					kv_batch(key.as_bytes(), value.as_bytes()),
				)
				.await
				.unwrap()
				.0;
			metas.push(meta);
		}
		metas
	})
	.await
	.expect("writes must not stall behind finalization subscriber backpressure");

	let drained = tokio::time::timeout(Duration::from_secs(2), async {
		let mut markers = Vec::new();
		while let Some(marker) = rx.recv().await {
			markers.push(marker);
		}
		markers
	})
	.await
	.expect("slow consumer should be dropped and the channel should close");

	assert!(!drained.is_empty());
	assert!(drained.len() <= STREAM_FINALIZATION_BUFFER);
	assert!(drained.windows(2).all(|w| w[0].log_index < w[1].log_index));
	assert!(drained.last().unwrap().log_index < writes.last().unwrap().log_index);

	support::abort_node(&node);
}
