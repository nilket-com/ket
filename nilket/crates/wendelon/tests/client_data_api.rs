#[path = "support/control_ring.rs"]
mod support;

use arrow::array::{LargeBinaryArray, LargeBinaryBuilder};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use wendelon::client::{
	ClusterPeerWire, ClusterStatusWire, ReadRequest, ReadResult, WendelonClient, WriteRequest,
};
use wendelon::network::control_server::{AdvanceEpochResponseWire, CONTROL_REASON_GRANTED};
use wendelon::network::server::HrftService;
use wendelon::network::transport::{NodeRole, NodeRoleSet, TransportTier, TransportTierSet};
use wendelon::network::wire;
use wendelon::schema::{
	ClientWriteId, Consistency, DurabilityLevel, HybridTimestamp, Identity, Locality, MsgType,
	NodeId, ReadRequestMeta, ReadStatus, TenantId, WriteRequestMeta, WriteStatus, read_key_schema,
	write_payload_schema,
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
				capability_bits: 0,
			})
			.collect(),
	}
}

fn write_meta(epoch: u64, key: &[u8], value: &[u8], sequence_no: u64) -> WriteRequestMeta {
	WriteRequestMeta {
		write_id: ClientWriteId {
			client_uuid: [7u8; 16],
			sequence_no,
			idempotency_ttl_ms: 60_000,
			_pad: 0,
		},
		client_hlc: hlc(100 + sequence_no, 0, 99),
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

fn read_meta(
	expected_epoch: u64,
	key: &[u8],
	consistency: Consistency,
	min_hlc: HybridTimestamp,
) -> ReadRequestMeta {
	ReadRequestMeta {
		min_hlc,
		expected_epoch,
		tenant_id: tenant(),
		consistency,
		_pad: [0; 3],
		key_identity: Identity::blake3_tenant(tenant(), key),
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

fn extract_value(result: &ReadResult) -> Vec<u8> {
	let batch = result.value.as_ref().expect("read returned no value batch");
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
	let batch = support::epoch_config_batch(
		&[
			(1, "127.0.0.1:0", tier_bits(), role_bits()),
			(2, "127.0.0.1:1", tier_bits(), role_bits()),
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

fn key_locality(bytes: &[u8]) -> u64 {
	Locality::from(Identity::blake3_tenant(tenant(), bytes)).get()
}

fn find_key_for_primary(ring: &wendelon::network::ring::RingRouter, target: NodeId) -> Vec<u8> {
	let ranges = ring.primary_ranges_for(target);
	assert!(!ranges.is_empty(), "target node has no primary ranges");
	for i in 0..200_000u64 {
		let candidate = format!("client-key-{i}");
		let hash = key_locality(candidate.as_bytes());
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
async fn client_write_and_read_round_trip_over_hrft() {
	let node = support::bootstrap_node("client-data-roundtrip", 1).await;
	refresh_leases(&node).await;

	let service: Arc<dyn HrftService> = node.router.clone();
	let bootstrap = snapshot(1, Some(1), &[(1, "loopback://node-1")]);
	let client = WendelonClient::new(support::loopback_client_config(
		node.local_node,
		service,
		bootstrap,
	));

	let key = b"client-key";
	let value = b"client-value";
	let write = client
		.write(WriteRequest {
			meta: write_meta(1, key, value, 1),
			payload: kv_batch(key, value),
		})
		.await
		.unwrap();
	assert_eq!(write.response.status, WriteStatus::OK);

	let read = client
		.read(ReadRequest {
			meta: read_meta(1, key, Consistency::LINEARIZABLE, write.response.commit_hlc),
			key: key_batch(key),
		})
		.await
		.unwrap();
	assert_eq!(read.response.status, ReadStatus::OK);
	assert_eq!(extract_value(&read), value);

	support::abort_node(&node);
}

#[tokio::test]
async fn client_bulk_write_round_trips_multiple_items() {
	let node = support::bootstrap_node("client-data-bulk", 1).await;
	refresh_leases(&node).await;

	let service: Arc<dyn HrftService> = node.router.clone();
	let bootstrap = snapshot(1, Some(1), &[(1, "loopback://node-1")]);
	let client = WendelonClient::new(support::loopback_client_config(
		node.local_node,
		service,
		bootstrap,
	));

	let requests: Vec<WriteRequest> = (0..4u64)
		.map(|seq| {
			let key = format!("bulk-client-key-{seq}").into_bytes();
			let value = format!("bulk-client-value-{seq}").into_bytes();
			WriteRequest {
				meta: write_meta(1, &key, &value, seq + 1),
				payload: kv_batch(&key, &value),
			}
		})
		.collect();

	let responses = client.bulk_write(requests).await.unwrap();
	assert_eq!(responses.len(), 4);
	for response in &responses {
		assert_eq!(response.response.status, WriteStatus::OK);
	}

	let read = client
		.read(ReadRequest {
			meta: read_meta(
				1,
				b"bulk-client-key-3",
				Consistency::LINEARIZABLE,
				responses.last().unwrap().response.commit_hlc,
			),
			key: key_batch(b"bulk-client-key-3"),
		})
		.await
		.unwrap();
	assert_eq!(read.response.status, ReadStatus::OK);
	assert_eq!(extract_value(&read), b"bulk-client-value-3");

	support::abort_node(&node);
}

#[tokio::test]
async fn client_surfaces_not_leader_statuses_explicitly() {
	let node = support::bootstrap_node("client-data-not-leader", 1).await;
	refresh_leases(&node).await;

	let service: Arc<dyn HrftService> = node.router.clone();
	let (factory, pool) = support::loopback_factory(node.local_node, service.clone());
	advance_to_two_node_epoch(&node, &factory, &pool).await;
	refresh_leases(&node).await;

	let key = find_key_for_primary(
		&node.ring,
		NodeId {
			id: 2,
			_reserved: 0,
		},
	);
	let bootstrap = snapshot(2, Some(1), &[(1, "loopback://node-1")]);
	let client = WendelonClient::new(support::loopback_client_config(
		node.local_node,
		service,
		bootstrap,
	));

	let write = client
		.write(WriteRequest {
			meta: write_meta(2, &key, b"remote-owner", 1),
			payload: kv_batch(&key, b"remote-owner"),
		})
		.await
		.unwrap();
	assert_eq!(write.response.status, WriteStatus::NOT_LEADER);
	assert_eq!(write.finalization, None);

	let read = client
		.read(ReadRequest {
			meta: read_meta(2, &key, Consistency::LINEARIZABLE, hlc(0, 0, 0)),
			key: key_batch(&key),
		})
		.await
		.unwrap();
	assert_eq!(read.response.status, ReadStatus::NOT_LEADER);
	assert!(read.value.is_none());

	support::abort_node(&node);
}
