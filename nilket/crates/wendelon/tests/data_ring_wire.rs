#[path = "support/control_ring.rs"]
mod support;

use arrow::array::LargeBinaryArray;
use arrow::array::LargeBinaryBuilder;
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use highway::{HighwayHash, HighwayHasher, Key as HighwayKey};
use std::io::{Cursor, Error, ErrorKind};
use std::sync::Arc;
use wendelon::network::control_server::{AdvanceEpochResponseWire, CONTROL_REASON_GRANTED};
use wendelon::network::server::HrftService;
use wendelon::network::wire;
use wendelon::schema::{
	ClientWriteId, Consistency, DurabilityLevel, HybridTimestamp, MsgType, NodeId, ReadRequestMeta,
	ReadResponseMeta, ReadStatus, WriteRequestMeta, WriteResponseMeta, WriteStatus,
	read_key_schema, write_payload_schema,
};

const HIGHWAY_KEY: HighwayKey = HighwayKey([
	0x5745_4e44_454c_4f4e,
	0x4841_5348_4b45_5901,
	0x4441_5441_5249_4e47,
	0x5354_454c_4c41_524f,
]);

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
			client_uuid: [9u8; 16],
			sequence_no,
			idempotency_ttl_ms: 60_000,
			_pad: 0,
		},
		client_hlc: hlc(10 + sequence_no, 0, 1),
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

fn read_meta(
	expected_epoch: u64,
	key: &[u8],
	consistency: Consistency,
	min_hlc: HybridTimestamp,
) -> ReadRequestMeta {
	ReadRequestMeta {
		min_hlc,
		expected_epoch,
		consistency,
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

fn highway_hash(bytes: &[u8]) -> u64 {
	let mut hasher = HighwayHasher::new(HIGHWAY_KEY);
	hasher.append(bytes);
	hasher.finalize64()
}

async fn refresh_leases(node: &wendelon::node::WendelonNode) {
	support::wait_for_local_leader(node).await;
	node.data_server
		.refresh_leases_once(&node.ring, &*node.clock, wendelon::node::LEASE_TTL_MS)
		.await;
}

async fn wait_for_ring_epoch(node: &wendelon::node::WendelonNode, epoch: u64) {
	let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
	loop {
		if node.epoch_tx.borrow().epoch == epoch && node.ring.current_epoch() == epoch {
			return;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for ring epoch {epoch}; visible={}, ring={}",
			node.epoch_tx.borrow().epoch,
			node.ring.current_epoch()
		);
		tokio::time::sleep(std::time::Duration::from_millis(20)).await;
	}
}

async fn advance_to_two_node_epoch(
	node: &wendelon::node::WendelonNode,
	factory: &Arc<wendelon::network::factory::TransportProviderFactory>,
	pool: &Arc<wendelon::network::pool::BufPool>,
) {
	let tier_bits = wendelon::network::transport::TransportTierSet::empty()
		.with(wendelon::network::transport::TransportTier::QuicIoUring)
		.as_u8();
	let role_bits = wendelon::network::transport::NodeRoleSet::empty()
		.with(wendelon::network::transport::NodeRole::Data)
		.with(wendelon::network::transport::NodeRole::Control)
		.as_u8();
	let batch = support::epoch_config_batch(
		&[
			(1, "127.0.0.1:0", tier_bits, role_bits),
			(2, "127.0.0.1:1", tier_bits, role_bits),
		],
		&[1],
		&[2],
	);
	let arrow = support::encode_batch(&batch).unwrap();
	let resp = support::call_frame(
		factory,
		pool,
		node.local_node,
		MsgType::EPOCH_ADV_REQ,
		&[],
		Some(&arrow),
		None,
	)
	.await
	.unwrap();
	let meta: AdvanceEpochResponseWire = wire::decode(&resp.meta).unwrap();
	assert!(meta.accepted);
	assert_eq!(meta.reason_code, CONTROL_REASON_GRANTED);
	wait_for_ring_epoch(node, 2).await;
}

fn find_key_for_primary(ring: &wendelon::network::ring::RingRouter, target: NodeId) -> Vec<u8> {
	let ranges = ring.primary_ranges_for(target);
	assert!(!ranges.is_empty(), "target node has no primary ranges");
	for i in 0..200_000u64 {
		let candidate = format!("wire-key-{i}");
		let hash = highway_hash(candidate.as_bytes());
		if ranges
			.iter()
			.any(|(start, end)| *start <= hash && hash <= *end)
		{
			return candidate.into_bytes();
		}
	}
	panic!("failed to find a key hashing into node {}", target.id);
}

#[tokio::test]
async fn write_then_read_round_trip_over_hrft() {
	let node = support::bootstrap_node("data-wire-roundtrip", 1).await;
	refresh_leases(&node).await;

	let service: Arc<dyn HrftService> = node.router.clone();
	let (factory, pool) = support::loopback_factory(node.local_node, service);
	let key = b"wire-key";
	let value = b"wire-value";
	let write_arrow = support::encode_batch(&kv_batch(key, value)).unwrap();

	let write_resp = support::call_frame(
		&factory,
		&pool,
		node.local_node,
		MsgType::WRITE_REQ,
		bytemuck::bytes_of(&write_meta(1, key, value, 1)),
		Some(&write_arrow),
		None,
	)
	.await
	.unwrap();
	let write_meta: WriteResponseMeta =
		*bytemuck::from_bytes(&write_resp.meta[..std::mem::size_of::<WriteResponseMeta>()]);
	assert_eq!(write_meta.status, WriteStatus::OK);

	let read_arrow = support::encode_batch(&key_batch(key)).unwrap();
	let read_resp = support::call_frame(
		&factory,
		&pool,
		node.local_node,
		MsgType::READ_REQ,
		bytemuck::bytes_of(&read_meta(
			1,
			key,
			Consistency::LINEARIZABLE,
			write_meta.commit_hlc,
		)),
		Some(&read_arrow),
		None,
	)
	.await
	.unwrap();
	let read_meta: ReadResponseMeta =
		*bytemuck::from_bytes(&read_resp.meta[..std::mem::size_of::<ReadResponseMeta>()]);
	assert_eq!(read_meta.status, ReadStatus::OK);
	let batch = decode_single_batch(read_resp.arrow_payload.as_ref().unwrap()).unwrap();
	assert_eq!(extract_value(&batch), value);

	support::abort_node(&node);
}

#[tokio::test]
async fn write_req_and_read_req_return_epoch_stale_over_hrft() {
	let node = support::bootstrap_node("data-wire-stale", 1).await;
	refresh_leases(&node).await;

	let service: Arc<dyn HrftService> = node.router.clone();
	let (factory, pool) = support::loopback_factory(node.local_node, service);
	let key = b"stale-key";
	let value = b"stale-value";
	let write_arrow = support::encode_batch(&kv_batch(key, value)).unwrap();

	let ok_resp = support::call_frame(
		&factory,
		&pool,
		node.local_node,
		MsgType::WRITE_REQ,
		bytemuck::bytes_of(&write_meta(1, key, value, 1)),
		Some(&write_arrow),
		None,
	)
	.await
	.unwrap();
	let ok_meta: WriteResponseMeta =
		*bytemuck::from_bytes(&ok_resp.meta[..std::mem::size_of::<WriteResponseMeta>()]);
	assert_eq!(ok_meta.status, WriteStatus::OK);

	let stale_write = support::call_frame(
		&factory,
		&pool,
		node.local_node,
		MsgType::WRITE_REQ,
		bytemuck::bytes_of(&write_meta(0, key, value, 2)),
		Some(&write_arrow),
		None,
	)
	.await
	.unwrap();
	let stale_write_meta: WriteResponseMeta =
		*bytemuck::from_bytes(&stale_write.meta[..std::mem::size_of::<WriteResponseMeta>()]);
	assert_eq!(stale_write_meta.status, WriteStatus::EPOCH_STALE);

	let read_arrow = support::encode_batch(&key_batch(key)).unwrap();
	let stale_read = support::call_frame(
		&factory,
		&pool,
		node.local_node,
		MsgType::READ_REQ,
		bytemuck::bytes_of(&read_meta(0, key, Consistency::LINEARIZABLE, hlc(0, 0, 0))),
		Some(&read_arrow),
		None,
	)
	.await
	.unwrap();
	let stale_read_meta: ReadResponseMeta =
		*bytemuck::from_bytes(&stale_read.meta[..std::mem::size_of::<ReadResponseMeta>()]);
	assert_eq!(stale_read_meta.status, ReadStatus::EPOCH_STALE);

	support::abort_node(&node);
}

#[tokio::test]
async fn read_req_returns_e_safe_behind_over_hrft() {
	let node = support::bootstrap_node("data-wire-esafe", 1).await;
	let service: Arc<dyn HrftService> = node.router.clone();
	let (factory, pool) = support::loopback_factory(node.local_node, service);
	let key = b"esafe-key";
	let read_arrow = support::encode_batch(&key_batch(key)).unwrap();

	let resp = support::call_frame(
		&factory,
		&pool,
		node.local_node,
		MsgType::READ_REQ,
		bytemuck::bytes_of(&read_meta(1, key, Consistency::LINEARIZABLE, hlc(5, 0, 1))),
		Some(&read_arrow),
		None,
	)
	.await
	.unwrap();
	let meta: ReadResponseMeta =
		*bytemuck::from_bytes(&resp.meta[..std::mem::size_of::<ReadResponseMeta>()]);
	assert_eq!(meta.status, ReadStatus::E_SAFE_BEHIND);

	support::abort_node(&node);
}

#[tokio::test]
async fn write_req_and_read_req_return_not_leader_for_non_primary_range() {
	let node = support::bootstrap_node("data-wire-not-leader", 1).await;
	refresh_leases(&node).await;

	let service: Arc<dyn HrftService> = node.router.clone();
	let (factory, pool) = support::loopback_factory(node.local_node, service);
	advance_to_two_node_epoch(&node, &factory, &pool).await;

	let remote = NodeId {
		id: 2,
		_reserved: 0,
	};
	let key = find_key_for_primary(&node.ring, remote);
	let value = b"redirect-me";
	let write_arrow = support::encode_batch(&kv_batch(&key, value)).unwrap();

	let write_resp = support::call_frame(
		&factory,
		&pool,
		node.local_node,
		MsgType::WRITE_REQ,
		bytemuck::bytes_of(&write_meta(2, &key, value, 1)),
		Some(&write_arrow),
		None,
	)
	.await
	.unwrap();
	let write_meta: WriteResponseMeta =
		*bytemuck::from_bytes(&write_resp.meta[..std::mem::size_of::<WriteResponseMeta>()]);
	assert_eq!(write_meta.status, WriteStatus::NOT_LEADER);

	let read_arrow = support::encode_batch(&key_batch(&key)).unwrap();
	let read_resp = support::call_frame(
		&factory,
		&pool,
		node.local_node,
		MsgType::READ_REQ,
		bytemuck::bytes_of(&read_meta(2, &key, Consistency::LINEARIZABLE, hlc(0, 0, 0))),
		Some(&read_arrow),
		None,
	)
	.await
	.unwrap();
	let read_meta: ReadResponseMeta =
		*bytemuck::from_bytes(&read_resp.meta[..std::mem::size_of::<ReadResponseMeta>()]);
	assert_eq!(read_meta.status, ReadStatus::NOT_LEADER);

	support::abort_node(&node);
}
