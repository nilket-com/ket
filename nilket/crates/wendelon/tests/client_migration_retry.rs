#[path = "support/control_ring.rs"]
mod support;

use arrow::array::{LargeBinaryArray, LargeBinaryBuilder};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use std::io::ErrorKind;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use wendelon::client::{
	ClusterPeerWire, ClusterStatusWire, DATA_RETRY_BUDGET, KeyRange, MIGRATION_RETRY_BACKOFF,
	ReadRequest, ReadResult, TopologySource, WendelonClient, WriteRequest,
};
use wendelon::network::core::{HRFT_MAGIC, HrftFrame};
use wendelon::network::pool::PooledBuf;
use wendelon::network::server::{HrftService, RequestContext};
use wendelon::network::transport::{NodeRole, NodeRoleSet, TransportTier, TransportTierSet};
use wendelon::schema::{
	ClientWriteId, Consistency, DurabilityLevel, FRAME_FLAG_STREAM_END, FrameHeader,
	HybridTimestamp, Identity, MsgType, NodeId, QueryRequestMeta, QueryResponseMeta,
	ReadRequestMeta, ReadResponseMeta, ReadStatus, TenantId, WriteRequestMeta, read_key_schema,
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
			client_uuid: [0x44; 16],
			sequence_no,
			idempotency_ttl_ms: 60_000,
			_pad: 0,
		},
		client_hlc: hlc(5_000 + sequence_no, 0, 77),
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

fn take_retry(counter: &AtomicUsize) -> bool {
	counter
		.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
			if remaining > 0 {
				Some(remaining - 1)
			} else {
				None
			}
		})
		.is_ok()
}

fn migration_incomplete_read_frame(frame: &HrftFrame, req: &ReadRequestMeta) -> HrftFrame {
	let resp = ReadResponseMeta {
		status: ReadStatus::MIGRATION_INCOMPLETE,
		_pad1: [0; 7],
		value_hlc: req.min_hlc,
		read_at_index: 0,
		epoch: req.expected_epoch,
		leader_hint: NodeId {
			id: 0,
			_reserved: 0,
		},
		value_len: 0,
		_pad2: [0; 4],
	};
	HrftFrame {
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
	}
}

fn migration_incomplete_query_frame(frame: &HrftFrame, req: &QueryRequestMeta) -> HrftFrame {
	let resp = QueryResponseMeta {
		status: ReadStatus::MIGRATION_INCOMPLETE,
		_pad1: [0; 7],
		value_hlc: req.min_hlc,
		epoch: req.expected_epoch,
		leader_hint: NodeId {
			id: 0,
			_reserved: 0,
		},
		rows: 0,
		arrow_len: 0,
		_pad2: [0; 4],
	};
	HrftFrame {
		header: FrameHeader {
			magic: HRFT_MAGIC,
			version: 1,
			msg_type: MsgType::QUERY_RESP.0,
			flags: FRAME_FLAG_STREAM_END,
			stream_id: frame.header.stream_id,
			meta_len: std::mem::size_of::<QueryResponseMeta>() as u32,
			arrow_len: 0,
			trailing_len: 0,
		},
		meta: PooledBuf::orphan(bytemuck::bytes_of(&resp).to_vec()),
		arrow_payload: None,
		trailing: None,
	}
}

struct FlakyMigrationService {
	inner: Arc<dyn HrftService>,
	read_failures_remaining: AtomicUsize,
	query_failures_remaining: AtomicUsize,
	read_calls: AtomicUsize,
	query_calls: AtomicUsize,
}

impl FlakyMigrationService {
	fn new(
		inner: Arc<dyn HrftService>,
		read_failures_remaining: usize,
		query_failures_remaining: usize,
	) -> Self {
		Self {
			inner,
			read_failures_remaining: AtomicUsize::new(read_failures_remaining),
			query_failures_remaining: AtomicUsize::new(query_failures_remaining),
			read_calls: AtomicUsize::new(0),
			query_calls: AtomicUsize::new(0),
		}
	}

	fn read_calls(&self) -> usize {
		self.read_calls.load(Ordering::SeqCst)
	}

	fn query_calls(&self) -> usize {
		self.query_calls.load(Ordering::SeqCst)
	}
}

#[async_trait]
impl HrftService for FlakyMigrationService {
	async fn handle_frame(
		&self,
		context: RequestContext,
		frame: HrftFrame,
	) -> std::io::Result<Option<HrftFrame>> {
		match MsgType(frame.header.msg_type) {
			MsgType::READ_REQ => {
				self.read_calls.fetch_add(1, Ordering::SeqCst);
				if take_retry(&self.read_failures_remaining) {
					let req: &ReadRequestMeta =
						bytemuck::from_bytes(&frame.meta[..std::mem::size_of::<ReadRequestMeta>()]);
					return Ok(Some(migration_incomplete_read_frame(&frame, req)));
				}
			}
			MsgType::QUERY_REQ => {
				self.query_calls.fetch_add(1, Ordering::SeqCst);
				if take_retry(&self.query_failures_remaining) {
					let req: &QueryRequestMeta = bytemuck::from_bytes(
						&frame.meta[..std::mem::size_of::<QueryRequestMeta>()],
					);
					return Ok(Some(migration_incomplete_query_frame(&frame, req)));
				}
			}
			_ => {}
		}
		self.inner.handle_frame(context, frame).await
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
async fn client_retries_read_after_migration_incomplete() {
	let (_node_dir, node) = support::bootstrap_node("client-migration-read", 1).await;
	refresh_leases(&node).await;

	let service = Arc::new(FlakyMigrationService::new(
		node.router.clone() as Arc<dyn HrftService>,
		2,
		0,
	));
	let bootstrap = snapshot(1, Some(1), &[(1, "loopback://node-1")]);
	let client = WendelonClient::new(support::loopback_client_config(
		node.local_node,
		service.clone() as Arc<dyn HrftService>,
		bootstrap,
	));

	let key = b"migration-read-key";
	let value = b"migration-read-value";
	let write = client
		.write(WriteRequest {
			meta: write_meta(1, key, value, 1),
			payload: kv_batch(key, value),
		})
		.await
		.unwrap();

	let start = tokio::time::Instant::now();
	let read = client
		.read(ReadRequest {
			meta: read_meta(1, key, Consistency::STALE, write.response.commit_hlc),
			key: key_batch(key),
		})
		.await
		.unwrap();
	assert_eq!(read.response.status, ReadStatus::OK);
	assert_eq!(extract_value(&read), value);
	assert_eq!(service.read_calls(), 3);
	assert!(start.elapsed() >= MIGRATION_RETRY_BACKOFF * 2);

	support::abort_node(&node);
}

#[tokio::test]
async fn client_retries_query_after_migration_incomplete() {
	let (_node_dir, node) = support::bootstrap_node("client-migration-query", 1).await;
	refresh_leases(&node).await;

	let service = Arc::new(FlakyMigrationService::new(
		node.router.clone() as Arc<dyn HrftService>,
		0,
		2,
	));
	let bootstrap = snapshot(1, Some(1), &[(1, "loopback://node-1")]);
	let client = WendelonClient::new(support::loopback_client_config(
		node.local_node,
		service.clone() as Arc<dyn HrftService>,
		bootstrap,
	));

	let key = b"migration-query-key";
	let value = b"migration-query-value";
	client
		.write(WriteRequest {
			meta: write_meta(1, key, value, 1),
			payload: kv_batch(key, value),
		})
		.await
		.unwrap();

	let start = tokio::time::Instant::now();
	let batches = client
		.query(
			tenant(),
			Vec::new(),
			KeyRange::full(),
			1,
			hlc(0, 0, 0),
			Consistency::STALE,
			hlc(0, 0, 0),
			true,
		)
		.await
		.unwrap()
		.collect_all()
		.await
		.unwrap();
	assert_eq!(batches.len(), 1);
	let batch = &batches[0];
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
	assert_eq!(keys.value(0), key);
	assert_eq!(values.value(0), value);
	assert_eq!(service.query_calls(), 3);
	assert!(start.elapsed() >= MIGRATION_RETRY_BACKOFF * 2);

	support::abort_node(&node);
}

#[tokio::test]
async fn client_reports_query_migration_incomplete_after_retry_budget() {
	let (_node_dir, node) = support::bootstrap_node("client-migration-query-budget", 1).await;
	let service = Arc::new(FlakyMigrationService::new(
		node.router.clone() as Arc<dyn HrftService>,
		0,
		DATA_RETRY_BUDGET + 2,
	));
	let bootstrap = snapshot(1, Some(1), &[(1, "loopback://node-1")]);
	let topology = Arc::new(CountingTopologySource::new(bootstrap.clone()));
	let client = WendelonClient::new(support::loopback_client_config_with_topology(
		node.local_node,
		service.clone() as Arc<dyn HrftService>,
		bootstrap,
		topology.clone(),
	));

	let start = tokio::time::Instant::now();
	let err = client
		.query(
			tenant(),
			Vec::new(),
			KeyRange::full(),
			1,
			hlc(0, 0, 0),
			Consistency::STALE,
			hlc(0, 0, 0),
			true,
		)
		.await
		.unwrap_err();
	assert_eq!(err.kind(), ErrorKind::WouldBlock);
	assert!(err.to_string().contains("migration incomplete"));
	assert_eq!(service.query_calls(), DATA_RETRY_BUDGET);
	assert_eq!(topology.fetches(), 0);
	assert!(start.elapsed() >= MIGRATION_RETRY_BACKOFF * ((DATA_RETRY_BUDGET - 1) as u32));

	support::abort_node(&node);
}
