#[path = "support/control_ring.rs"]
mod support;

use arrow::array::{LargeBinaryArray, LargeBinaryBuilder};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use highway::{HighwayHash, HighwayHasher, Key as HighwayKey};
use std::io::{Error, ErrorKind};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use wendelon::client::{
	ClusterPeerWire, ClusterStatusWire, ControlRingTopologySource, DATA_RETRY_BUDGET,
	E_SAFE_RETRY_BACKOFF, ReadRequest, ReadResult, TopologySource, WendelonClient, WriteRequest,
};
use wendelon::network::core::{HRFT_MAGIC, HrftFrame};
use wendelon::network::pool::PooledBuf;
use wendelon::network::ring::{RingRouter, RingState};
use wendelon::network::server::HrftService;
use wendelon::network::transport::{NodeRole, NodeRoleSet, TransportTier, TransportTierSet};
use wendelon::schema::{
	ClientWriteId, Consistency, DurabilityLevel, FrameHeader, HybridTimestamp, MsgType, NodeId,
	ReadRequestMeta, ReadResponseMeta, ReadStatus, WriteRequestMeta, WriteResponseMeta,
	WriteStatus, read_key_schema, write_payload_schema,
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
			client_uuid: [9u8; 16],
			sequence_no,
			idempotency_ttl_ms: 60_000,
			_pad: 0,
		},
		client_hlc: hlc(1_000 + sequence_no, 0, 55),
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

fn highway_hash(bytes: &[u8]) -> u64 {
	let mut hasher = HighwayHasher::new(HIGHWAY_KEY);
	hasher.append(bytes);
	hasher.finalize64()
}

fn find_key_for_primary(snapshot: &ClusterStatusWire, target: NodeId) -> Vec<u8> {
	let ring = RingRouter::new(RingState::build(
		&snapshot.to_membership(),
		snapshot.replication_factor as usize,
	));
	let ranges = ring.primary_ranges_for(target);
	assert!(!ranges.is_empty(), "target node has no primary ranges");
	for i in 0..200_000u64 {
		let candidate = format!("retry-key-{i}");
		let hash = highway_hash(candidate.as_bytes());
		if ranges
			.iter()
			.any(|(start, end)| *start <= hash && hash <= *end)
		{
			return candidate.into_bytes();
		}
	}
	panic!("failed to find key for node {}", target.id);
}

struct RedirectingService {
	write_status: Option<WriteStatus>,
	read_status: Option<ReadStatus>,
	leader_hint: NodeId,
	calls: AtomicUsize,
}

impl RedirectingService {
	fn for_write(status: WriteStatus, leader_hint: NodeId) -> Self {
		Self {
			write_status: Some(status),
			read_status: None,
			leader_hint,
			calls: AtomicUsize::new(0),
		}
	}

	fn for_read(status: ReadStatus, leader_hint: NodeId) -> Self {
		Self {
			write_status: None,
			read_status: Some(status),
			leader_hint,
			calls: AtomicUsize::new(0),
		}
	}

	fn calls(&self) -> usize {
		self.calls.load(Ordering::SeqCst)
	}
}

#[async_trait]
impl HrftService for RedirectingService {
	async fn handle_frame(
		&self,
		_remote: NodeId,
		frame: HrftFrame,
	) -> std::io::Result<Option<HrftFrame>> {
		self.calls.fetch_add(1, Ordering::SeqCst);
		match MsgType(frame.header.msg_type) {
			MsgType::WRITE_REQ => {
				let req: &WriteRequestMeta =
					bytemuck::from_bytes(&frame.meta[..std::mem::size_of::<WriteRequestMeta>()]);
				let resp = WriteResponseMeta {
					status: self.write_status.ok_or_else(|| {
						Error::new(
							ErrorKind::InvalidData,
							"write redirect status not configured",
						)
					})?,
					_pad1: [0; 7],
					log_index: 0,
					term: 0,
					epoch: req.expected_epoch,
					commit_hlc: hlc(0, 0, 0),
					leader_hint: self.leader_hint,
					echoed_write_id: req.write_id,
					has_finalization: 0,
					_pad2: [0; 7],
				};
				Ok(Some(HrftFrame {
					header: FrameHeader {
						magic: HRFT_MAGIC,
						version: 1,
						msg_type: MsgType::WRITE_RESP.0,
						flags: 0,
						stream_id: frame.header.stream_id,
						meta_len: std::mem::size_of::<WriteResponseMeta>() as u32,
						arrow_len: 0,
						trailing_len: 0,
					},
					meta: PooledBuf::orphan(bytemuck::bytes_of(&resp).to_vec()),
					arrow_payload: None,
					trailing: None,
				}))
			}
			MsgType::READ_REQ => {
				let req: &ReadRequestMeta =
					bytemuck::from_bytes(&frame.meta[..std::mem::size_of::<ReadRequestMeta>()]);
				let resp = ReadResponseMeta {
					status: self.read_status.ok_or_else(|| {
						Error::new(
							ErrorKind::InvalidData,
							"read redirect status not configured",
						)
					})?,
					_pad1: [0; 7],
					value_hlc: req.min_hlc,
					read_at_index: 0,
					epoch: req.expected_epoch,
					leader_hint: self.leader_hint,
					value_len: 0,
					_pad2: [0; 4],
				};
				Ok(Some(HrftFrame {
					header: FrameHeader {
						magic: HRFT_MAGIC,
						version: 1,
						msg_type: MsgType::READ_RESP.0,
						flags: 0,
						stream_id: frame.header.stream_id,
						meta_len: std::mem::size_of::<ReadResponseMeta>() as u32,
						arrow_len: 0,
						trailing_len: 0,
					},
					meta: PooledBuf::orphan(bytemuck::bytes_of(&resp).to_vec()),
					arrow_payload: None,
					trailing: None,
				}))
			}
			other => Err(Error::new(
				ErrorKind::Unsupported,
				format!(
					"redirect service does not handle msg_type=0x{:02x}",
					other.0
				),
			)),
		}
	}
}

struct CountingTopologySource {
	snapshot: Mutex<ClusterStatusWire>,
	fetches: AtomicUsize,
}

impl CountingTopologySource {
	fn new(snapshot: ClusterStatusWire) -> Self {
		Self {
			snapshot: Mutex::new(snapshot),
			fetches: AtomicUsize::new(0),
		}
	}

	fn fetches(&self) -> usize {
		self.fetches.load(Ordering::SeqCst)
	}
}

#[async_trait]
impl TopologySource for CountingTopologySource {
	async fn fetch(&self) -> std::io::Result<ClusterStatusWire> {
		self.fetches.fetch_add(1, Ordering::SeqCst);
		Ok(self.snapshot.lock().unwrap().clone())
	}
}

#[tokio::test]
async fn client_retries_write_after_redirect_with_control_ring_refresh() {
	let node = support::bootstrap_node("client-retry-write", 1).await;
	refresh_leases(&node).await;

	let redirect_node = NodeId {
		id: 2,
		_reserved: 0,
	};
	let stale_snapshot = snapshot(
		1,
		Some(2),
		&[(1, "loopback://node-1"), (2, "loopback://node-2")],
	);
	let key = find_key_for_primary(&stale_snapshot, redirect_node);
	let value = b"retry-write-value";

	let redirect_service = Arc::new(RedirectingService::for_write(
		WriteStatus::NOT_LEADER,
		node.local_node,
	));
	let services: Vec<(NodeId, Arc<dyn HrftService>)> = vec![
		(node.local_node, node.router.clone() as Arc<dyn HrftService>),
		(
			redirect_node,
			redirect_service.clone() as Arc<dyn HrftService>,
		),
	];

	let (topology_factory, topology_pool) = support::loopback_factory_many(services.clone());
	let topology = Arc::new(ControlRingTopologySource::new(
		topology_factory,
		topology_pool,
		vec![node.local_node],
	));
	let client = WendelonClient::new(support::loopback_client_config_many_with_topology(
		stale_snapshot,
		topology,
		services,
	));

	let write = client
		.write(WriteRequest {
			meta: write_meta(1, &key, value, 1),
			payload: kv_batch(&key, value),
		})
		.await
		.unwrap();
	assert_eq!(write.response.status, WriteStatus::OK);
	assert_eq!(redirect_service.calls(), 1);
	assert_eq!(client.leader(), Some(node.local_node));

	let read = client
		.read(ReadRequest {
			meta: read_meta(
				1,
				&key,
				Consistency::LINEARIZABLE,
				write.response.commit_hlc,
			),
			key: key_batch(&key),
		})
		.await
		.unwrap();
	assert_eq!(read.response.status, ReadStatus::OK);
	assert_eq!(extract_value(&read), value);

	support::abort_node(&node);
}

#[tokio::test]
async fn client_refreshes_and_retries_read_after_e_safe_behind() {
	let node = support::bootstrap_node("client-retry-read", 1).await;
	refresh_leases(&node).await;

	let stable_services: Vec<(NodeId, Arc<dyn HrftService>)> =
		vec![(node.local_node, node.router.clone() as Arc<dyn HrftService>)];
	let stable_client = WendelonClient::new(support::loopback_client_config_many(
		snapshot(1, Some(1), &[(1, "loopback://node-1")]),
		stable_services.clone(),
	));
	let stale_snapshot = snapshot(
		1,
		Some(2),
		&[(1, "loopback://node-1"), (2, "loopback://node-2")],
	);
	let redirect_node = NodeId {
		id: 2,
		_reserved: 0,
	};
	let key = find_key_for_primary(&stale_snapshot, redirect_node);
	let value = b"retry-read-value";
	let write = stable_client
		.write(WriteRequest {
			meta: write_meta(1, &key, value, 1),
			payload: kv_batch(&key, value),
		})
		.await
		.unwrap();
	assert_eq!(write.response.status, WriteStatus::OK);

	let redirect_service = Arc::new(RedirectingService::for_read(
		ReadStatus::E_SAFE_BEHIND,
		node.local_node,
	));
	let services: Vec<(NodeId, Arc<dyn HrftService>)> = vec![
		(node.local_node, node.router.clone() as Arc<dyn HrftService>),
		(
			redirect_node,
			redirect_service.clone() as Arc<dyn HrftService>,
		),
	];
	let (topology_factory, topology_pool) = support::loopback_factory_many(services.clone());
	let topology = Arc::new(ControlRingTopologySource::new(
		topology_factory,
		topology_pool,
		vec![node.local_node],
	));
	let client = WendelonClient::new(support::loopback_client_config_many_with_topology(
		stale_snapshot,
		topology,
		services,
	));

	let start = tokio::time::Instant::now();
	let read = client
		.read(ReadRequest {
			meta: read_meta(
				1,
				&key,
				Consistency::LINEARIZABLE,
				write.response.commit_hlc,
			),
			key: key_batch(&key),
		})
		.await
		.unwrap();
	assert_eq!(read.response.status, ReadStatus::OK);
	assert_eq!(extract_value(&read), value);
	assert_eq!(redirect_service.calls(), 1);
	assert!(start.elapsed() >= E_SAFE_RETRY_BACKOFF);

	support::abort_node(&node);
}

#[tokio::test]
async fn client_stops_after_retry_budget_on_unstable_cluster() {
	let redirect_node = NodeId {
		id: 2,
		_reserved: 0,
	};
	let stale_snapshot = snapshot(1, Some(2), &[(2, "loopback://node-2")]);
	let redirect_service = Arc::new(RedirectingService::for_write(
		WriteStatus::NOT_LEADER,
		redirect_node,
	));
	let topology = Arc::new(CountingTopologySource::new(stale_snapshot.clone()));
	let client = WendelonClient::new(support::loopback_client_config_many_with_topology(
		stale_snapshot,
		topology.clone(),
		vec![(
			redirect_node,
			redirect_service.clone() as Arc<dyn HrftService>,
		)],
	));

	let write = client
		.write(WriteRequest {
			meta: write_meta(1, b"retry-budget", b"value", 1),
			payload: kv_batch(b"retry-budget", b"value"),
		})
		.await
		.unwrap();
	assert_eq!(write.response.status, WriteStatus::NOT_LEADER);
	assert_eq!(redirect_service.calls(), DATA_RETRY_BUDGET);
	assert_eq!(topology.fetches(), DATA_RETRY_BUDGET - 1);
}
