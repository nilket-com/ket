#[path = "support/control_ring.rs"]
mod support;

use arrow::array::{LargeBinaryArray, LargeBinaryBuilder};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use std::fs;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use wendelon::client::{
	ClusterPeerWire, ClusterStatusWire, DATA_RETRY_BUDGET, TopologySource, WendelonClient,
	WriteRequest,
};
use wendelon::network::core::{HRFT_MAGIC, HrftFrame};
use wendelon::network::pending_finalizations::{
	PendingFinalizationStore, pending_finalizations_path,
};
use wendelon::network::pool::PooledBuf;
use wendelon::network::server::{HrftService, RequestContext};
use wendelon::network::traits::DataRing;
use wendelon::network::transport::{NodeRole, NodeRoleSet, TransportTier, TransportTierSet};
use wendelon::node::{NodeBootstrapConfig, WendelonNode};
use wendelon::schema::{
	ClientWriteId, Consistency, DurabilityLevel, FrameHeader, HybridTimestamp, Identity, MsgType,
	NodeId, ReadRequestMeta, ReadStatus, TenantId, WriteRequestMeta, WriteResponseMeta,
	WriteStatus, read_key_schema, write_payload_schema,
};
use wendelon::simulator::checkers::{
	RegisterHistoryEntry, RegisterLinearizabilityChecker, RegisterOperation,
};

fn tenant() -> TenantId {
	TenantId::ZERO
}

fn node(id: u32) -> NodeId {
	NodeId { id, _reserved: 0 }
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

fn write_meta(
	expected_epoch: u64,
	key: &[u8],
	value: &[u8],
	sequence_no: u64,
	await_finalization: bool,
) -> WriteRequestMeta {
	WriteRequestMeta {
		write_id: ClientWriteId {
			client_uuid: [0xA5; 16],
			sequence_no,
			idempotency_ttl_ms: 60_000,
			_pad: 0,
		},
		client_hlc: hlc(1_000 + sequence_no, 0, 41),
		expected_epoch,
		tenant_id: tenant(),
		proxy_hop: 0,
		durability: DurabilityLevel::QUORUM,
		await_finalization: if await_finalization { 1 } else { 0 },
		_pad1: [0; 5],
		proxy_source: node(0),
		key_identity: Identity::blake3_tenant(tenant(), key),
		key_len: key.len() as u32,
		value_len: value.len() as u32,
	}
}

fn read_meta(expected_epoch: u64, key: &[u8]) -> ReadRequestMeta {
	ReadRequestMeta {
		min_hlc: hlc(0, 0, 0),
		expected_epoch,
		tenant_id: tenant(),
		consistency: Consistency::STALE,
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

fn extract_value(batch: &RecordBatch) -> Vec<u8> {
	let array: &LargeBinaryArray = batch
		.column_by_name("value")
		.expect("value column must exist")
		.as_any()
		.downcast_ref()
		.expect("value column must be LargeBinaryArray");
	array.value(0).to_vec()
}

async fn wait_for_leader(node: &WendelonNode) {
	support::wait_for_local_leader(node).await;
}

async fn refresh_leases(node: &WendelonNode) {
	wait_for_leader(node).await;
	node.data_server
		.refresh_leases_once(&node.ring, &*node.clock, wendelon::node::LEASE_TTL_MS)
		.await;
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

struct AlwaysBackpressuredWriteService {
	leader_hint: NodeId,
	calls: AtomicUsize,
}

impl AlwaysBackpressuredWriteService {
	fn new(leader_hint: NodeId) -> Self {
		Self {
			leader_hint,
			calls: AtomicUsize::new(0),
		}
	}

	fn calls(&self) -> usize {
		self.calls.load(Ordering::SeqCst)
	}
}

#[async_trait]
impl HrftService for AlwaysBackpressuredWriteService {
	async fn handle_frame(
		&self,
		_context: RequestContext,
		frame: HrftFrame,
	) -> std::io::Result<Option<HrftFrame>> {
		self.calls.fetch_add(1, Ordering::SeqCst);
		match MsgType(frame.header.msg_type) {
			MsgType::WRITE_REQ => {
				let req: &WriteRequestMeta =
					bytemuck::from_bytes(&frame.meta[..std::mem::size_of::<WriteRequestMeta>()]);
				let resp = WriteResponseMeta {
					status: WriteStatus::BACKPRESSURED,
					_pad1: [0; 7],
					log_index: 0,
					term: 0,
					epoch: req.expected_epoch,
					commit_hlc: req.client_hlc,
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
			other => Err(std::io::Error::new(
				std::io::ErrorKind::Unsupported,
				format!(
					"always-backpressured write service does not handle msg_type=0x{:02x}",
					other.0
				),
			)),
		}
	}
}

#[test]
fn property_register_linearizability_checker_accepts_legal_and_rejects_illegal_histories() {
	let legal = vec![
		RegisterHistoryEntry {
			process: 1,
			key: "tenant/register".to_string(),
			started_at: 1,
			completed_at: 2,
			op: RegisterOperation::Write { value: 7 },
		},
		RegisterHistoryEntry {
			process: 2,
			key: "tenant/register".to_string(),
			started_at: 3,
			completed_at: 4,
			op: RegisterOperation::Read { observed: Some(7) },
		},
	];
	RegisterLinearizabilityChecker::check(&legal).expect("legal register history must be accepted");

	let illegal = vec![
		RegisterHistoryEntry {
			process: 1,
			key: "tenant/register".to_string(),
			started_at: 1,
			completed_at: 2,
			op: RegisterOperation::Write { value: 9 },
		},
		RegisterHistoryEntry {
			process: 2,
			key: "tenant/register".to_string(),
			started_at: 3,
			completed_at: 4,
			op: RegisterOperation::Read { observed: None },
		},
	];
	let err = RegisterLinearizabilityChecker::check(&illegal)
		.expect_err("stale post-write read must be rejected");
	assert_eq!(err.key, "tenant/register");
	assert!(err.message.contains("not linearizable"));
}

#[tokio::test]
async fn property_pending_finalization_restart_recovers_without_losing_value() {
	let dir = support::tempdir("property-pending-finalization");
	let audit_path = dir.join("audit.ndjson");
	let cfg = NodeBootstrapConfig::dev(91, dir.clone(), "127.0.0.1:0".into());
	let node = WendelonNode::bootstrap_single_node(cfg.clone())
		.await
		.unwrap();
	refresh_leases(&node).await;

	let store = PendingFinalizationStore::open(pending_finalizations_path(&cfg.data_dir)).unwrap();
	let key = b"property-finalization-key";
	let value = b"property-finalization-value";
	let meta = write_meta(1, key, value, 1, true);

	node.raft.shutdown().await.unwrap();
	let (resp, trailing) = node
		.data_server
		.write(meta, kv_batch(key, value))
		.await
		.unwrap();
	assert_eq!(resp.status, WriteStatus::FINALIZATION_PENDING);
	assert!(trailing.is_none());
	assert_eq!(store.load_all().await.unwrap().len(), 1);

	node.shutdown().await.unwrap();

	let recovered = WendelonNode::bootstrap_single_node(cfg).await.unwrap();
	refresh_leases(&recovered).await;
	// The pre-restart handle is revoked by shutdown; reopen it against the recovered node.
	let store = PendingFinalizationStore::open(pending_finalizations_path(&dir)).unwrap();

	let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
	loop {
		let pending = store.load_all().await.unwrap();
		let (read_resp, batch) = recovered
			.data_server
			.read(read_meta(1, key), key_batch(key))
			.await
			.unwrap();
		if pending.is_empty() && read_resp.status == ReadStatus::OK {
			let batch = batch.expect("successful read must include a value batch");
			assert_eq!(extract_value(&batch), value);
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for interrupted finalization recovery; pending={} read_status={:?}",
			pending.len(),
			read_resp.status
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}

	let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
	loop {
		let audit = fs::read_to_string(&audit_path).unwrap_or_default();
		let finalized_user_write_count = audit
			.lines()
			.filter(|line| {
				line.contains("\"event_type\":\"data.write_finalized\"")
					&& line.contains("\"record_count\":1")
			})
			.count();
		if finalized_user_write_count == 1 {
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for finalization audit evidence; current log: {}",
			audit
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}

	support::abort_node(&recovered);
}

#[tokio::test]
async fn property_duplicate_replay_converges_to_a_legal_terminal_state() {
	let dir = support::tempdir("property-replay-terminal");
	let cfg = NodeBootstrapConfig::dev(92, dir.clone(), "127.0.0.1:0".into());
	let node = WendelonNode::bootstrap_single_node(cfg.clone())
		.await
		.unwrap();
	refresh_leases(&node).await;

	let key = b"property-replay-key";
	let value = b"property-replay-value";
	let meta = write_meta(1, key, value, 1, true);

	node.raft.shutdown().await.unwrap();
	let (first, first_marker) = node
		.data_server
		.write(meta, kv_batch(key, value))
		.await
		.unwrap();
	let (duplicate, duplicate_marker) = node
		.data_server
		.write(meta, kv_batch(key, value))
		.await
		.unwrap();
	assert_eq!(first.status, WriteStatus::FINALIZATION_PENDING);
	assert_eq!(duplicate.status, WriteStatus::FINALIZATION_PENDING);
	assert_eq!(duplicate.commit_hlc, first.commit_hlc);
	assert!(first_marker.is_none());
	assert!(duplicate_marker.is_none());

	node.shutdown().await.unwrap();

	let recovered = WendelonNode::bootstrap_single_node(cfg).await.unwrap();
	refresh_leases(&recovered).await;

	let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
	loop {
		let (resp, marker) = recovered
			.data_server
			.write(meta, kv_batch(key, value))
			.await
			.unwrap();
		assert!(
			matches!(
				resp.status,
				WriteStatus::FINALIZATION_PENDING | WriteStatus::DUPLICATE
			),
			"replay must stay within the legal pending/duplicate state set, got {:?}",
			resp.status
		);
		if resp.status == WriteStatus::DUPLICATE {
			assert!(
				marker.is_some(),
				"terminal duplicate replay must include finalization"
			);
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for duplicate replay to reach terminal state"
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}

	support::abort_node(&recovered);
}

#[tokio::test]
async fn property_backpressure_retries_stay_bounded_without_topology_refresh() {
	let service = Arc::new(AlwaysBackpressuredWriteService::new(node(501)));
	let bootstrap = snapshot(1, Some(501), &[(501, "loopback://node-501")]);
	let topology = Arc::new(CountingTopologySource::new(bootstrap.clone()));
	let client = WendelonClient::new(support::loopback_client_config_with_topology(
		node(501),
		service.clone() as Arc<dyn HrftService>,
		bootstrap,
		topology.clone(),
	));

	let key = b"property-backpressure-key";
	let value = b"property-backpressure-value";
	let result = client
		.write(WriteRequest {
			meta: write_meta(1, key, value, 1, false),
			payload: kv_batch(key, value),
		})
		.await
		.unwrap();

	assert_eq!(result.response.status, WriteStatus::BACKPRESSURED);
	assert_eq!(
		service.calls(),
		DATA_RETRY_BUDGET,
		"backpressure retries must remain bounded by the client retry budget"
	);
	assert_eq!(
		topology.fetches(),
		0,
		"backpressure retries must not refresh topology"
	);
}
