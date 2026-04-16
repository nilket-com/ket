#[path = "support/control_ring.rs"]
mod support;

use arrow::array::{LargeBinaryArray, LargeBinaryBuilder};
use arrow::record_batch::RecordBatch;
use highway::{HighwayHash, HighwayHasher, Key as HighwayKey};
use std::sync::Arc;
use wendelon::client::{
	ClusterPeerWire, ClusterStatusWire, KeyRange, WendelonClient, WriteRequest,
};
use wendelon::network::server::HrftService;
use wendelon::network::transport::{NodeRole, NodeRoleSet, TransportTier, TransportTierSet};
use wendelon::schema::{
	ClientWriteId, Consistency, DurabilityLevel, HybridTimestamp, NodeId, WriteRequestMeta,
	write_payload_schema,
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

fn tier_bits() -> u8 {
	TransportTierSet::empty()
		.with(TransportTier::QuicIoUring)
		.as_u8()
}

fn role_bits() -> u8 {
	NodeRoleSet::empty()
		.with(NodeRole::Data)
		.with(NodeRole::Control)
		.as_u8()
}

fn snapshot(epoch: u64, leader: Option<u32>, peers: &[(u32, &str)]) -> ClusterStatusWire {
	ClusterStatusWire {
		epoch,
		leader,
		replication_factor: 1,
		peers: peers
			.iter()
			.map(|(node_id, address)| ClusterPeerWire {
				node_id: *node_id,
				address: (*address).to_string(),
				tier_bits: tier_bits(),
				role_bits: role_bits(),
			})
			.collect(),
	}
}

fn write_meta(epoch: u64, key: &[u8], value: &[u8], sequence_no: u64) -> WriteRequestMeta {
	WriteRequestMeta {
		write_id: ClientWriteId {
			client_uuid: [0x33; 16],
			sequence_no,
			idempotency_ttl_ms: 60_000,
			_pad: 0,
		},
		client_hlc: hlc(10_000 + sequence_no, 0, 44),
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

fn extract_single_kv(batch: &RecordBatch) -> (Vec<u8>, Vec<u8>) {
	let keys: &LargeBinaryArray = batch
		.column_by_name("key")
		.unwrap()
		.as_any()
		.downcast_ref()
		.unwrap();
	let values: &LargeBinaryArray = batch
		.column_by_name("value")
		.unwrap()
		.as_any()
		.downcast_ref()
		.unwrap();
	assert_eq!(batch.num_rows(), 1, "test expects one stored row per batch");
	(keys.value(0).to_vec(), values.value(0).to_vec())
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

#[tokio::test]
async fn client_query_streams_multiple_batches_and_terminates() {
	let node = support::bootstrap_node("streaming-query-client", 1).await;
	refresh_leases(&node).await;

	let service: Arc<dyn HrftService> = node.router.clone();
	let bootstrap = snapshot(1, Some(1), &[(1, "loopback://node-1")]);
	let client = WendelonClient::new(support::loopback_client_config(
		node.local_node,
		service,
		bootstrap,
	));

	let rows = vec![
		(b"stream-k3".to_vec(), b"value-3".to_vec()),
		(b"stream-k1".to_vec(), b"value-1".to_vec()),
		(b"stream-k2".to_vec(), b"value-2".to_vec()),
	];
	for (idx, (key, value)) in rows.iter().enumerate() {
		client
			.write(WriteRequest {
				meta: write_meta(1, key, value, idx as u64 + 1),
				payload: kv_batch(key, value),
			})
			.await
			.unwrap();
	}

	let mut query = client
		.query(
			Vec::new(),
			KeyRange::full(),
			1,
			hlc(0, 0, 0),
			Consistency::STALE,
			hlc(0, 0, 0),
		)
		.await
		.unwrap();
	assert_eq!(query.rows, rows.len() as u64);

	let mut streamed = Vec::new();
	for _ in 0..rows.len() {
		let batch = query
			.next_batch()
			.await
			.unwrap()
			.expect("stream ended before all batches arrived");
		streamed.push(extract_single_kv(&batch));
	}
	assert!(query.next_batch().await.unwrap().is_none());
	assert!(query.next_batch().await.unwrap().is_none());

	let mut expected = rows.clone();
	expected.sort_by_key(|(key, _)| highway_hash(key));
	assert_eq!(streamed, expected);

	support::abort_node(&node);
}

#[tokio::test]
async fn client_query_empty_result_terminates_cleanly() {
	let node = support::bootstrap_node("streaming-query-empty", 1).await;
	let service: Arc<dyn HrftService> = node.router.clone();
	let bootstrap = snapshot(1, Some(1), &[(1, "loopback://node-1")]);
	let client = WendelonClient::new(support::loopback_client_config(
		node.local_node,
		service,
		bootstrap,
	));

	let mut query = client
		.query(
			Vec::new(),
			KeyRange::full(),
			1,
			hlc(0, 0, 0),
			Consistency::STALE,
			hlc(0, 0, 0),
		)
		.await
		.unwrap();
	assert_eq!(query.rows, 0);
	assert!(query.next_batch().await.unwrap().is_none());
	assert!(query.next_batch().await.unwrap().is_none());

	support::abort_node(&node);
}
