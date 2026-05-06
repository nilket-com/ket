#[path = "support/control_ring.rs"]
mod support;

use arrow::array::LargeBinaryBuilder;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use bytes::Bytes;
use openraft::storage::{RaftLogStorage, RaftLogStorageExt, RaftStateMachine};
use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId, RaftSnapshotBuilder};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::fs;
use std::io::{ErrorKind, Result};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use wendelon::client::{
	ClusterPeerWire, ClusterStatusWire, DATA_RETRY_BUDGET, KeyRange, MIGRATION_RETRY_BACKOFF,
	TopologySource, WendelonClient, WriteRequest,
};
use wendelon::consensus::raft_storage::{WendelonLogStore, WendelonStateMachine};
use wendelon::consensus::{
	TenantFinalization, UpdateTenantQuota, WendelonRequest, WendelonTypeConfig,
};
use wendelon::network::core::{HRFT_MAGIC, HrftFrame, read_frame, write_frame};
use wendelon::network::pending_finalizations::{
	PendingFinalizationRecord, PendingFinalizationStore, pending_finalizations_path,
};
use wendelon::network::pool::{BufPool, PooledBuf};
use wendelon::network::server::{HrftService, RequestContext};
use wendelon::network::tls::{ActorIdentity, ActorPermission, ActorRole, authorize_actor};
use wendelon::network::transport::{
	NodeRole, NodeRoleSet, TransportProvider, TransportStream, TransportTier, TransportTierSet,
};
use wendelon::node::{NodeBootstrapConfig, WendelonNode};
use wendelon::schema::{
	ClientWriteId, Consistency, DurabilityLevel, FRAME_FLAG_STREAM_END, FinalizationMarker,
	FrameHeader, HybridTimestamp, Identity, LeaseDescriptor, MsgType, NodeId, QueryRequestMeta,
	QueryResponseMeta, ReadStatus, TenantId, WriteRequestMeta, write_payload_schema,
};
use wendelon::simulator::checkers::{
	CounterHistoryEntry, CounterOperation, MonotonicCounterChecker, RegisterHistoryEntry,
	RegisterLinearizabilityChecker, RegisterOperation,
};
use wendelon::simulator::history::{RegisterHistoryArtifact, RegisterHistoryBatch};
use wendelon::simulator::network::{
	InjectedSimulatedTransportFault, SimulatedTransportEventKind, SimulatedTransportFaultAction,
	SimulatedTransportFaultOp, SimulatedTransportNetwork, SimulatedTransportProvider,
};
use wendelon::storage::BlockingFileOps;
use wendelon::storage::l2::{
	InjectedMockObjectStoreFault, MockObjectStore, MockObjectStoreConfig,
	MockObjectStoreFaultAction, MockObjectStoreOp, ObjectStore,
};
use wendelon_client::{Client, ClientError, WriteOptions};

const DEFAULT_LADDER_SEED: u64 = 0x57A1_4D3E_2026_0421;
const SIM_SEED_DIR: &str = "tests/sim/seeds";

#[derive(Debug, Clone, Deserialize)]
struct FrozenSimulatorSeed {
	issue: u64,
	scenario: String,
	seed: u64,
	ops: u64,
	introduced_at: String,
	notes: Option<String>,
}

#[derive(Debug)]
struct SimFailure {
	scenario: String,
	seed: u64,
	iteration: u64,
	message: String,
}

impl SimFailure {
	fn new(
		scenario: impl Into<String>,
		seed: u64,
		iteration: u64,
		message: impl Into<String>,
	) -> Self {
		Self {
			scenario: scenario.into(),
			seed,
			iteration,
			message: message.into(),
		}
	}
}

impl std::fmt::Display for SimFailure {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(
			f,
			"FAILED: seed={} scenario={} iteration={}\nReproduce: WENDELON_SIM_SEED={} cargo test --test simulator_ladder {} -- --ignored --nocapture\n{}",
			self.seed, self.scenario, self.iteration, self.seed, self.scenario, self.message
		)
	}
}

impl std::error::Error for SimFailure {}

#[derive(Clone, Copy, Debug)]
struct CoverageBudget {
	counter_ops: u64,
	register_ops: u64,
	object_store_ops: u64,
	transport_ops: u64,
}

fn node(id: u32) -> NodeId {
	NodeId { id, _reserved: 0 }
}

fn choose_index(rng: &mut StdRng, len: usize) -> usize {
	debug_assert!(len > 0);
	(rng.next_u64() as usize) % len
}

fn small_random_bytes(rng: &mut StdRng, len: usize) -> Vec<u8> {
	let mut out = vec![0_u8; len];
	rng.fill_bytes(&mut out);
	out
}

fn bucket_share(total: u64, numerator: u64, denominator: u64, min: u64, max: u64) -> u64 {
	if total == 0 {
		return 0;
	}
	let mut share = total.saturating_mul(numerator) / denominator;
	if share < min {
		share = min.min(total);
	}
	share.min(max.min(total))
}

fn coverage_budget(total_ops: u64) -> CoverageBudget {
	let mut remaining = total_ops;
	let transport_ops = bucket_share(total_ops, 1, 10_000, 8, 4_096).min(remaining);
	remaining = remaining.saturating_sub(transport_ops);
	let object_store_ops = bucket_share(total_ops, 1, 5_000, 16, 8_192).min(remaining);
	remaining = remaining.saturating_sub(object_store_ops);
	let register_ops = bucket_share(total_ops, 1, 100, 32, 100_000).min(remaining);
	remaining = remaining.saturating_sub(register_ops);
	CoverageBudget {
		counter_ops: remaining,
		register_ops,
		object_store_ops,
		transport_ops,
	}
}

fn ladder_ops_from_env() -> u64 {
	std::env::var("WENDELON_SIM_OPS")
		.ok()
		.and_then(|raw| raw.parse::<u64>().ok())
		.unwrap_or(10_000)
}

fn ladder_seed_from_env() -> u64 {
	std::env::var("WENDELON_SIM_SEED")
		.ok()
		.and_then(|raw| raw.parse::<u64>().ok())
		.unwrap_or(DEFAULT_LADDER_SEED)
}

fn register_history_dir_from_env() -> Option<PathBuf> {
	std::env::var("WENDELON_SIM_HISTORY_DIR")
		.ok()
		.map(|raw| raw.trim().to_string())
		.filter(|raw| !raw.is_empty())
		.map(PathBuf::from)
}

fn is_iso_date(raw: &str) -> bool {
	raw.len() == 10
		&& raw.as_bytes()[4] == b'-'
		&& raw.as_bytes()[7] == b'-'
		&& raw
			.bytes()
			.enumerate()
			.all(|(idx, byte)| matches!(idx, 4 | 7) || byte.is_ascii_digit())
}

fn supported_scenario(name: &str) -> bool {
	matches!(
		name,
		"coverage_ladder"
			| "replay_ordering"
			| "apply_order_races"
			| "interrupted_finalization_recovery"
			| "lazy_migration_pressure"
			| "quota_authorization_boundary"
	)
}

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

async fn refresh_leases(node: &wendelon::node::WendelonNode) {
	support::wait_for_local_leader(node).await;
	node.data_server
		.refresh_leases_once(&node.ring, &*node.clock, wendelon::node::LEASE_TTL_MS)
		.await;
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
	query_failures_remaining: AtomicUsize,
	query_calls: AtomicUsize,
}

impl FlakyMigrationService {
	fn new(inner: Arc<dyn HrftService>, query_failures_remaining: usize) -> Self {
		Self {
			inner,
			query_failures_remaining: AtomicUsize::new(query_failures_remaining),
			query_calls: AtomicUsize::new(0),
		}
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
		if MsgType(frame.header.msg_type) == MsgType::QUERY_REQ {
			self.query_calls.fetch_add(1, Ordering::SeqCst);
			if take_retry(&self.query_failures_remaining) {
				let req: &QueryRequestMeta =
					bytemuck::from_bytes(&frame.meta[..std::mem::size_of::<QueryRequestMeta>()]);
				return Ok(Some(migration_incomplete_query_frame(&frame, req)));
			}
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

struct CountingWriteService {
	inner: Arc<dyn HrftService>,
	write_calls: AtomicUsize,
}

impl CountingWriteService {
	fn new(inner: Arc<dyn HrftService>) -> Self {
		Self {
			inner,
			write_calls: AtomicUsize::new(0),
		}
	}

	fn write_calls(&self) -> usize {
		self.write_calls.load(Ordering::SeqCst)
	}
}

#[async_trait]
impl HrftService for CountingWriteService {
	async fn handle_frame(
		&self,
		context: RequestContext,
		frame: HrftFrame,
	) -> std::io::Result<Option<HrftFrame>> {
		if MsgType(frame.header.msg_type) == MsgType::WRITE_REQ {
			self.write_calls.fetch_add(1, Ordering::SeqCst);
		}
		self.inner.handle_frame(context, frame).await
	}
}

fn log_id(term: u64, index: u64) -> LogId<u32> {
	LogId::new(CommittedLeaderId::new(term, 0), index)
}

fn lease_entry(
	index: u64,
	range_start: u64,
	range_end: u64,
	holder: u32,
) -> Entry<WendelonTypeConfig> {
	let lease = LeaseDescriptor {
		epoch: 1,
		holder: NodeId {
			id: holder,
			_reserved: 0,
		},
		range_start,
		range_end,
		lease_start: HybridTimestamp {
			physical_ms: index,
			logical: 0,
			node_id: holder,
		},
		lease_expiry: HybridTimestamp {
			physical_ms: index + 1_000,
			logical: 0,
			node_id: holder,
		},
		min_commit_index: 0,
		issuer: NodeId {
			id: 1,
			_reserved: 0,
		},
		signature: [0; 64],
	};
	Entry {
		log_id: log_id(1, index),
		payload: EntryPayload::Normal(WendelonRequest::AcquireLease(lease)),
	}
}

fn finalize_entry(index: u64, physical_ms: u64) -> Entry<WendelonTypeConfig> {
	finalize_entry_for_epoch(index, 1, physical_ms)
}

fn finalize_entry_for_epoch(index: u64, epoch: u64, physical_ms: u64) -> Entry<WendelonTypeConfig> {
	Entry {
		log_id: log_id(1, index),
		payload: EntryPayload::Normal(WendelonRequest::Finalize(TenantFinalization {
			marker: FinalizationMarker {
				log_index: index,
				term: 1,
				epoch,
				commit_hlc: HybridTimestamp {
					physical_ms,
					logical: 0,
					node_id: 1,
				},
				payload_hash: [0; 32],
				e_safe_index: index,
				durability: DurabilityLevel::QUORUM,
				_pad: [0; 7],
			},
			tenant_id: TenantId::ZERO,
			key_identity: Identity::ZERO,
			charged_bytes: 0,
			record_count: 1,
		})),
	}
}

async fn append_and_apply(
	store: &mut WendelonLogStore,
	sm: &mut WendelonStateMachine,
	entry: Entry<WendelonTypeConfig>,
) -> std::result::Result<(), openraft::StorageError<u32>> {
	store.blocking_append(vec![entry.clone()]).await?;
	<WendelonStateMachine as RaftStateMachine<WendelonTypeConfig>>::apply(sm, vec![entry])
		.await
		.map(|_| ())
}

fn frozen_seed_paths() -> Result<Vec<PathBuf>> {
	let mut out = Vec::new();
	let dir = Path::new(SIM_SEED_DIR);
	if !dir.exists() {
		return Ok(out);
	}
	for entry in fs::read_dir(dir)? {
		let path = entry?.path();
		if path.extension().and_then(|ext| ext.to_str()) == Some("seed") {
			out.push(path);
		}
	}
	out.sort();
	Ok(out)
}

fn load_frozen_seed(path: &Path) -> Result<FrozenSimulatorSeed> {
	let text = fs::read_to_string(path)?;
	toml::from_str(&text).map_err(|err| std::io::Error::new(ErrorKind::InvalidData, err))
}

fn validate_frozen_seed_file(path: &Path, seed: &FrozenSimulatorSeed) {
	let stem = path
		.file_stem()
		.and_then(|stem| stem.to_str())
		.expect("seed file stem must be valid utf-8");
	let issue_from_name = stem
		.parse::<u64>()
		.expect("seed filename must be the issue number");
	assert_eq!(
		issue_from_name,
		seed.issue,
		"seed file {} must match issue field {}",
		path.display(),
		seed.issue
	);
	assert!(
		supported_scenario(&seed.scenario),
		"seed file {} uses unsupported scenario {}",
		path.display(),
		seed.scenario
	);
	assert!(
		seed.ops > 0,
		"seed file {} must request ops > 0",
		path.display()
	);
	assert!(
		is_iso_date(&seed.introduced_at),
		"seed file {} must use YYYY-MM-DD introduced_at",
		path.display()
	);
	if let Some(notes) = &seed.notes {
		assert!(
			!notes.trim().is_empty(),
			"seed file {} notes must be non-empty when present",
			path.display()
		);
	}
}

fn export_register_history_artifact(
	dir: &Path,
	scenario: &str,
	seed: u64,
	ops: u64,
	batches: &[RegisterHistoryBatch],
) -> Result<PathBuf> {
	let artifact = RegisterHistoryArtifact::new(scenario, seed, ops, batches.to_vec());
	let paths = artifact.write_to_dir(dir)?;
	Ok(paths.json)
}

fn run_counter_scenario(seed: u64, ops: u64) -> std::result::Result<u64, SimFailure> {
	if ops == 0 {
		return Ok(0);
	}
	let scenario = "coverage_ladder";
	let mut rng = StdRng::seed_from_u64(seed);
	let mut clock = 0_u64;
	let mut executed = 0_u64;
	let chunk_size = 8_192_u64;

	while executed < ops {
		let chunk_ops = (ops - executed).min(chunk_size);
		let mut values = [0_u64; 32];
		let mut history = Vec::with_capacity(chunk_ops as usize);
		for offset in 0..chunk_ops {
			let key_index = choose_index(&mut rng, values.len());
			let key = format!("tenant/{:02x}", key_index);
			let process = (rng.next_u64() % 8) + 1;
			let started_at = clock;
			let completed_at = clock + 1;
			clock += 2;
			if rng.next_u64() % 4 == 0 {
				history.push(CounterHistoryEntry {
					process,
					key,
					started_at,
					completed_at,
					op: CounterOperation::Read {
						observed: values[key_index],
					},
				});
			} else {
				let by = (rng.next_u64() % 3) + 1;
				values[key_index] = values[key_index].saturating_add(by);
				history.push(CounterHistoryEntry {
					process,
					key,
					started_at,
					completed_at,
					op: CounterOperation::Add { by },
				});
			}
			let iteration = executed + offset + 1;
			if history.len() == chunk_size as usize || iteration == ops {
				MonotonicCounterChecker::check(&history).map_err(|err| {
					SimFailure::new(
						scenario,
						seed,
						iteration,
						format!("monotonic counter checker rejected chunk: {}", err.message),
					)
				})?;
			}
		}
		executed += chunk_ops;
	}

	Ok(executed)
}

fn run_register_scenario(
	seed: u64,
	ops: u64,
	history_dir: Option<&Path>,
) -> std::result::Result<u64, SimFailure> {
	if ops == 0 {
		return Ok(0);
	}
	let scenario = "coverage_ladder";
	let mut rng = StdRng::seed_from_u64(seed);
	let mut clock = 10_000_u64;
	let mut executed = 0_u64;
	let mut batch_index = 0_u64;
	let mut history_batches = Vec::new();

	while executed < ops {
		let remaining = ops - executed;
		let batch_len = remaining.min(6).max(1);
		let mut states: BTreeMap<String, Option<i64>> = BTreeMap::from([
			("tenant/register/0".to_string(), None),
			("tenant/register/1".to_string(), None),
		]);
		let mut history = Vec::with_capacity(batch_len as usize);
		for offset in 0..batch_len {
			let key = if rng.next_u64() % 2 == 0 {
				"tenant/register/0".to_string()
			} else {
				"tenant/register/1".to_string()
			};
			let current = states.get(&key).copied().flatten();
			let process = (rng.next_u64() % 3) + 1;
			let started_at = clock;
			let completed_at = clock + 1;
			clock += 2;
			let op = if current.is_none() || rng.next_u64() % 10 < 6 {
				let value = (rng.next_u64() % 10_000) as i64;
				states.insert(key.clone(), Some(value));
				RegisterOperation::Write { value }
			} else {
				RegisterOperation::Read { observed: current }
			};
			history.push(RegisterHistoryEntry {
				process,
				key,
				started_at,
				completed_at,
				op,
			});
			let iteration = executed + offset + 1;
			if iteration == ops || history.len() == batch_len as usize {
				if history_dir.is_some() {
					history_batches.push(RegisterHistoryBatch::from_entries(
						batch_index,
						iteration,
						&history,
					));
				}
				RegisterLinearizabilityChecker::check(&history).map_err(|err| {
					let artifact_note = history_dir.and_then(|dir| {
						export_register_history_artifact(dir, scenario, seed, ops, &history_batches)
							.ok()
							.map(|path| format!(" history_json={}", path.display()))
					});
					SimFailure::new(
						scenario,
						seed,
						iteration,
						format!(
							"register checker rejected batch: {}{}",
							err.message,
							artifact_note.unwrap_or_default()
						),
					)
				})?;
				batch_index += 1;
			}
		}
		executed += batch_len;
	}

	if let Some(dir) = history_dir {
		let path = export_register_history_artifact(dir, scenario, seed, ops, &history_batches)
			.map_err(|err| {
				SimFailure::new(
					scenario,
					seed,
					executed.max(1),
					format!("failed to write register history artifact: {err}"),
				)
			})?;
		println!(
			"SIMULATOR_ELLE_HISTORY scenario={} seed={} json={}",
			scenario,
			seed,
			path.display()
		);
	}

	Ok(executed)
}

async fn run_object_store_scenario(seed: u64, ops: u64) -> std::result::Result<u64, SimFailure> {
	if ops == 0 {
		return Ok(0);
	}
	let scenario = "coverage_ladder";
	let mut executed = 0_u64;

	let delayed_store = MockObjectStore::new(MockObjectStoreConfig {
		list_visibility_delay: 1,
		delete_visibility_delay: 1,
		..Default::default()
	});
	let delayed_uri = delayed_store
		.put("delayed/item.wnd", b"abc")
		.await
		.map_err(|err| SimFailure::new(scenario, seed, executed + 1, err.to_string()))?;
	executed += 1;
	if executed < ops {
		let first = delayed_store
			.list("delayed/")
			.await
			.map_err(|err| SimFailure::new(scenario, seed, executed + 1, err.to_string()))?;
		executed += 1;
		if !first.is_empty() {
			return Err(SimFailure::new(
				scenario,
				seed,
				executed,
				"delayed put became visible too early",
			));
		}
	}
	if executed < ops {
		let second = delayed_store
			.list("delayed/")
			.await
			.map_err(|err| SimFailure::new(scenario, seed, executed + 1, err.to_string()))?;
		executed += 1;
		if second.len() != 1 {
			return Err(SimFailure::new(
				scenario,
				seed,
				executed,
				format!(
					"expected delayed put to appear on second list, got {}",
					second.len()
				),
			));
		}
	}
	if executed < ops {
		delayed_store
			.delete(&delayed_uri)
			.await
			.map_err(|err| SimFailure::new(scenario, seed, executed + 1, err.to_string()))?;
		executed += 1;
	}
	if executed < ops {
		let third = delayed_store
			.list("delayed/")
			.await
			.map_err(|err| SimFailure::new(scenario, seed, executed + 1, err.to_string()))?;
		executed += 1;
		if third.len() != 1 {
			return Err(SimFailure::new(
				scenario,
				seed,
				executed,
				format!(
					"expected deleted object to remain visible for one list epoch, got {}",
					third.len()
				),
			));
		}
	}
	if executed < ops {
		let fourth = delayed_store
			.list("delayed/")
			.await
			.map_err(|err| SimFailure::new(scenario, seed, executed + 1, err.to_string()))?;
		executed += 1;
		if !fourth.is_empty() {
			return Err(SimFailure::new(
				scenario,
				seed,
				executed,
				"deleted object remained visible after delay window",
			));
		}
	}

	let store = MockObjectStore::default();
	let mut rng = StdRng::seed_from_u64(seed);
	let mut live: Vec<(String, String, Bytes)> = Vec::new();
	if executed < ops {
		let uri = store
			.put("faulty/item.wnd", b"fault")
			.await
			.map_err(|err| SimFailure::new(scenario, seed, executed + 1, err.to_string()))?;
		executed += 1;
		store.inject(
			InjectedMockObjectStoreFault::on(
				MockObjectStoreOp::GetBytes,
				MockObjectStoreFaultAction::Error {
					kind: ErrorKind::TimedOut,
					message: "simulated timeout".to_string(),
				},
			)
			.for_key("faulty/item.wnd"),
		);
		if executed < ops {
			let err = store
				.get(&uri)
				.await
				.expect_err("one-shot get_bytes fault must fire once");
			executed += 1;
			if err.kind() != ErrorKind::TimedOut {
				return Err(SimFailure::new(
					scenario,
					seed,
					executed,
					format!("expected TimedOut fault, got {:?}", err.kind()),
				));
			}
		}
		if executed < ops {
			let bytes = store
				.get_bytes(&uri)
				.await
				.map_err(|err| SimFailure::new(scenario, seed, executed + 1, err.to_string()))?;
			executed += 1;
			if bytes.as_ref() != b"fault" {
				return Err(SimFailure::new(
					scenario,
					seed,
					executed,
					"one-shot get_bytes fault did not clear",
				));
			}
		}
		live.push((
			"faulty/item.wnd".to_string(),
			uri,
			Bytes::from_static(b"fault"),
		));
	}

	while executed < ops {
		let choice = (rng.next_u64() % 5) as u8;
		if live.is_empty() || choice == 0 {
			let key = format!(
				"tenant/{:02x}/slab_{:016x}.wnd",
				(rng.next_u64() % 4) as u8,
				rng.next_u64()
			);
			let payload_len = ((rng.next_u64() % 24) + 1) as usize;
			let payload = Bytes::from(small_random_bytes(&mut rng, payload_len));
			let uri = store
				.put(&key, &payload)
				.await
				.map_err(|err| SimFailure::new(scenario, seed, executed + 1, err.to_string()))?;
			live.push((key, uri, payload));
			executed += 1;
			continue;
		}

		let idx = choose_index(&mut rng, live.len());
		let (key, uri, payload) = live[idx].clone();
		match choice {
			1 => {
				let meta = store.head(&uri).await.map_err(|err| {
					SimFailure::new(scenario, seed, executed + 1, err.to_string())
				})?;
				if meta.key != key || meta.size_bytes != payload.len() as u64 {
					return Err(SimFailure::new(
						scenario,
						seed,
						executed + 1,
						"head metadata did not match live object state",
					));
				}
			}
			2 => {
				let bytes = store.get_bytes(&uri).await.map_err(|err| {
					SimFailure::new(scenario, seed, executed + 1, err.to_string())
				})?;
				if bytes != payload {
					return Err(SimFailure::new(
						scenario,
						seed,
						executed + 1,
						"get_bytes payload mismatch",
					));
				}
			}
			3 => {
				let start = if payload.is_empty() {
					0
				} else {
					(rng.next_u64() as usize) % payload.len()
				};
				let end = start + ((rng.next_u64() as usize) % (payload.len() - start + 1));
				let range = store
					.get_range(&uri, start as u64..end as u64)
					.await
					.map_err(|err| {
						SimFailure::new(scenario, seed, executed + 1, err.to_string())
					})?;
				if range != payload.slice(start..end) {
					return Err(SimFailure::new(
						scenario,
						seed,
						executed + 1,
						"get_range payload mismatch",
					));
				}
			}
			4 => {
				let prefix = key.rsplit_once('/').map(|(left, _)| left).unwrap_or("");
				let listed = store.list(prefix).await.map_err(|err| {
					SimFailure::new(scenario, seed, executed + 1, err.to_string())
				})?;
				if !listed.iter().any(|entry| entry.key == key) {
					return Err(SimFailure::new(
						scenario,
						seed,
						executed + 1,
						format!("list with prefix {prefix} did not include expected key {key}"),
					));
				}
			}
			_ => {
				store.delete(&uri).await.map_err(|err| {
					SimFailure::new(scenario, seed, executed + 1, err.to_string())
				})?;
				live.swap_remove(idx);
			}
		}
		executed += 1;
	}

	Ok(executed)
}

struct EchoService;

#[async_trait]
impl HrftService for EchoService {
	async fn handle_frame(
		&self,
		_context: RequestContext,
		frame: HrftFrame,
	) -> Result<Option<HrftFrame>> {
		Ok(Some(frame))
	}
}

async fn run_transport_scenario(seed: u64, ops: u64) -> std::result::Result<u64, SimFailure> {
	if ops == 0 {
		return Ok(0);
	}
	let scenario = "coverage_ladder";
	let pool = BufPool::new(32, 64 * 1024);
	let network = Arc::new(SimulatedTransportNetwork::default());
	let provider = SimulatedTransportProvider::new(
		node(1),
		network.clone(),
		pool.clone(),
		TransportTier::QuicIoUring,
	);
	provider
		.publish_local(node(2), "sim://node-2", Arc::new(EchoService))
		.map_err(|err| SimFailure::new(scenario, seed, 0, err.to_string()))?;
	if ops > 4 {
		network.inject(
			InjectedSimulatedTransportFault::on(
				SimulatedTransportFaultOp::Connect,
				SimulatedTransportFaultAction::Delay(Duration::from_millis(1)),
			)
			.from(node(1))
			.to(node(2))
			.after_matches(2),
		);
	}
	let mut saw_delay_fault = false;

	for iteration in 0..ops {
		let conn = provider
			.connect(node(2), "sim://node-2")
			.await
			.map_err(|err| SimFailure::new(scenario, seed, iteration + 1, err.to_string()))?;
		let mut stream: Box<dyn TransportStream> = conn
			.open_bi()
			.await
			.map_err(|err| SimFailure::new(scenario, seed, iteration + 1, err.to_string()))?;
		let payload = iteration.to_le_bytes();
		let header = FrameHeader {
			magic: HRFT_MAGIC,
			version: 1,
			msg_type: MsgType::STATUS_REQ.0,
			flags: 0,
			stream_id: iteration as u32,
			meta_len: payload.len() as u32,
			arrow_len: 0,
			trailing_len: 0,
		};
		write_frame(&mut *stream, &header, &payload, None, None)
			.await
			.map_err(|err| SimFailure::new(scenario, seed, iteration + 1, err.to_string()))?;
		stream
			.finish()
			.await
			.map_err(|err| SimFailure::new(scenario, seed, iteration + 1, err.to_string()))?;
		let frame = read_frame(&mut *stream, &pool)
			.await
			.map_err(|err| SimFailure::new(scenario, seed, iteration + 1, err.to_string()))?;
		if frame.header.msg_type != MsgType::STATUS_REQ.0 || frame.meta.as_ref() != payload {
			return Err(SimFailure::new(
				scenario,
				seed,
				iteration + 1,
				"transport echo frame was corrupted",
			));
		}
		if iteration % 256 == 255 {
			saw_delay_fault |= network.events().iter().any(|event| {
				matches!(
					event.kind,
					SimulatedTransportEventKind::Fault {
						op: SimulatedTransportFaultOp::Connect,
						action: SimulatedTransportFaultAction::Delay(_),
					}
				)
			});
			network.clear_events();
		}
	}

	saw_delay_fault |= network.events().iter().any(|event| {
		matches!(
			event.kind,
			SimulatedTransportEventKind::Fault {
				op: SimulatedTransportFaultOp::Connect,
				action: SimulatedTransportFaultAction::Delay(_),
			}
		)
	});
	if ops > 4 && !saw_delay_fault {
		return Err(SimFailure::new(
			scenario,
			seed,
			ops,
			"expected transport delay fault was never observed",
		));
	}

	Ok(ops)
}

async fn run_replay_ordering_scenario(seed: u64, ops: u64) -> std::result::Result<u64, SimFailure> {
	let iterations = ops.max(1).min(16);
	for iteration in 0..iterations {
		let dir = tempdir(&format!("replay-ordering-{seed}-{iteration}"));
		let wal = dir.join("control.wal");
		let vote = dir.join("vote.wnd");
		let snapshot = dir.join("snapshot.wnd");
		let range_start = 100 * iteration;
		let expected_hlc = 10_000 + iteration;

		let mut store =
			WendelonLogStore::open_with_file_ops(&wal, &vote, &snapshot, Arc::new(BlockingFileOps))
				.await
				.map_err(|err| {
					SimFailure::new(
						"replay_ordering",
						seed,
						iteration + 1,
						format!("open log store failed: {err}"),
					)
				})?;
		store
			.blocking_append(vec![
				lease_entry(1, range_start, range_start + 9, 1),
				finalize_entry(2, expected_hlc),
				lease_entry(3, range_start + 10, range_start + 19, 2),
			])
			.await
			.map_err(|err| {
				SimFailure::new(
					"replay_ordering",
					seed,
					iteration + 1,
					format!("append failed: {err}"),
				)
			})?;
		store
			.save_committed(Some(log_id(1, 2)))
			.await
			.map_err(|err| {
				SimFailure::new(
					"replay_ordering",
					seed,
					iteration + 1,
					format!("save_committed failed: {err}"),
				)
			})?;
		drop(store);

		let mut reopened =
			WendelonLogStore::open_with_file_ops(&wal, &vote, &snapshot, Arc::new(BlockingFileOps))
				.await
				.map_err(|err| {
					SimFailure::new(
						"replay_ordering",
						seed,
						iteration + 1,
						format!("reopen failed: {err}"),
					)
				})?;
		let committed = reopened.read_committed().await.map_err(|err| {
			SimFailure::new(
				"replay_ordering",
				seed,
				iteration + 1,
				format!("read_committed failed: {err}"),
			)
		})?;
		if committed != Some(log_id(1, 2)) {
			return Err(SimFailure::new(
				"replay_ordering",
				seed,
				iteration + 1,
				format!("expected committed log id 2 after reopen, got {committed:?}"),
			));
		}
		let recovered = reopened.recovered_state_machine().await.map_err(|err| {
			SimFailure::new(
				"replay_ordering",
				seed,
				iteration + 1,
				format!("recovered_state_machine failed: {err}"),
			)
		})?;
		let state = recovered.state_snapshot().await;
		if state.last_applied_log_id != Some(log_id(1, 2)) {
			return Err(SimFailure::new(
				"replay_ordering",
				seed,
				iteration + 1,
				format!(
					"expected last_applied log id 2 after reopen, got {:?}",
					state.last_applied_log_id
				),
			));
		}
		if !state.leases.contains_key(&range_start) {
			return Err(SimFailure::new(
				"replay_ordering",
				seed,
				iteration + 1,
				"committed lease entry was not replayed".to_string(),
			));
		}
		if state.leases.contains_key(&(range_start + 10)) {
			return Err(SimFailure::new(
				"replay_ordering",
				seed,
				iteration + 1,
				"uncommitted tail lease was replayed into recovered state".to_string(),
			));
		}
		let watermark = state.e_safe_watermarks.get(&1).copied();
		let expected = HybridTimestamp {
			physical_ms: expected_hlc,
			logical: 0,
			node_id: 1,
		};
		if watermark != Some(expected) {
			return Err(SimFailure::new(
				"replay_ordering",
				seed,
				iteration + 1,
				format!("expected e_safe watermark {expected:?}, got {watermark:?}"),
			));
		}
	}

	Ok(iterations)
}

async fn run_apply_order_races_scenario(
	seed: u64,
	ops: u64,
) -> std::result::Result<u64, SimFailure> {
	let iterations = ops.max(1).min(8);
	for iteration in 0..iterations {
		let target_dir = tempdir(&format!("apply-order-target-{seed}-{iteration}"));
		let wal = target_dir.join("control.wal");
		let vote = target_dir.join("vote.wnd");
		let snapshot_path = target_dir.join("snapshot.wnd");
		let mut target_store = WendelonLogStore::open_with_file_ops(
			&wal,
			&vote,
			&snapshot_path,
			Arc::new(BlockingFileOps),
		)
		.await
		.map_err(|err| {
			SimFailure::new(
				"apply_order_races",
				seed,
				iteration + 1,
				format!("open target log store failed: {err}"),
			)
		})?;
		let mut target_sm = target_store
			.recovered_state_machine()
			.await
			.map_err(|err| {
				SimFailure::new(
					"apply_order_races",
					seed,
					iteration + 1,
					format!("target recovered_state_machine failed: {err}"),
				)
			})?;
		let mut snapshots = target_sm.subscribe_admission_snapshot();
		let initial = snapshots.borrow().clone();

		append_and_apply(
			&mut target_store,
			&mut target_sm,
			finalize_entry(1, 1_000 + iteration),
		)
		.await
		.map_err(|err| {
			SimFailure::new(
				"apply_order_races",
				seed,
				iteration + 1,
				format!("target finalize apply failed: {err}"),
			)
		})?;
		snapshots.changed().await.map_err(|err| {
			SimFailure::new(
				"apply_order_races",
				seed,
				iteration + 1,
				format!("target admission watch failed after finalize: {err}"),
			)
		})?;
		let after_apply = snapshots.borrow().clone();
		if after_apply.current_epoch < initial.current_epoch {
			return Err(SimFailure::new(
				"apply_order_races",
				seed,
				iteration + 1,
				format!(
					"admission snapshot regressed after finalize: initial_epoch={} current_epoch={}",
					initial.current_epoch, after_apply.current_epoch
				),
			));
		}
		let expected_epoch_one = HybridTimestamp {
			physical_ms: 1_000 + iteration,
			logical: 0,
			node_id: 1,
		};
		if after_apply.e_safe_watermarks.get(&1).copied() != Some(expected_epoch_one) {
			return Err(SimFailure::new(
				"apply_order_races",
				seed,
				iteration + 1,
				format!(
					"expected epoch-1 watermark {:?}, got {:?}",
					expected_epoch_one,
					after_apply.e_safe_watermarks.get(&1).copied()
				),
			));
		}

		append_and_apply(
			&mut target_store,
			&mut target_sm,
			lease_entry(2, iteration * 10, iteration * 10 + 9, 1),
		)
		.await
		.map_err(|err| {
			SimFailure::new(
				"apply_order_races",
				seed,
				iteration + 1,
				format!("target lease apply failed: {err}"),
			)
		})?;
		let changed = tokio::time::timeout(Duration::from_millis(50), snapshots.changed()).await;
		if changed.is_ok() {
			return Err(SimFailure::new(
				"apply_order_races",
				seed,
				iteration + 1,
				"lease-only apply unexpectedly published a new admission snapshot".to_string(),
			));
		}

		let source_dir = tempdir(&format!("apply-order-source-{seed}-{iteration}"));
		let source_wal = source_dir.join("control.wal");
		let source_vote = source_dir.join("vote.wnd");
		let source_snapshot = source_dir.join("snapshot.wnd");
		let mut source_store = WendelonLogStore::open_with_file_ops(
			&source_wal,
			&source_vote,
			&source_snapshot,
			Arc::new(BlockingFileOps),
		)
		.await
		.map_err(|err| {
			SimFailure::new(
				"apply_order_races",
				seed,
				iteration + 1,
				format!("open source log store failed: {err}"),
			)
		})?;
		let mut source_sm = source_store
			.recovered_state_machine()
			.await
			.map_err(|err| {
				SimFailure::new(
					"apply_order_races",
					seed,
					iteration + 1,
					format!("source recovered_state_machine failed: {err}"),
				)
			})?;
		append_and_apply(
			&mut source_store,
			&mut source_sm,
			finalize_entry(1, 1_000 + iteration),
		)
		.await
		.map_err(|err| {
			SimFailure::new(
				"apply_order_races",
				seed,
				iteration + 1,
				format!("source epoch-1 finalize failed: {err}"),
			)
		})?;
		append_and_apply(
			&mut source_store,
			&mut source_sm,
			finalize_entry_for_epoch(2, 2, 2_000 + iteration),
		)
		.await
		.map_err(|err| {
			SimFailure::new(
				"apply_order_races",
				seed,
				iteration + 1,
				format!("source epoch-2 finalize failed: {err}"),
			)
		})?;
		let mut builder =
			<WendelonStateMachine as RaftStateMachine<WendelonTypeConfig>>::get_snapshot_builder(
				&mut source_sm,
			)
			.await;
		let built = builder.build_snapshot().await.map_err(|err| {
			SimFailure::new(
				"apply_order_races",
				seed,
				iteration + 1,
				format!("build snapshot failed: {err}"),
			)
		})?;
		let meta = built.meta.clone();
		<WendelonStateMachine as RaftStateMachine<WendelonTypeConfig>>::install_snapshot(
			&mut target_sm,
			&meta,
			built.snapshot,
		)
		.await
		.map_err(|err| {
			SimFailure::new(
				"apply_order_races",
				seed,
				iteration + 1,
				format!("install snapshot failed: {err}"),
			)
		})?;
		snapshots.changed().await.map_err(|err| {
			SimFailure::new(
				"apply_order_races",
				seed,
				iteration + 1,
				format!("target admission watch failed after install_snapshot: {err}"),
			)
		})?;
		let after_install = snapshots.borrow().clone();
		if after_install.current_epoch < after_apply.current_epoch {
			return Err(SimFailure::new(
				"apply_order_races",
				seed,
				iteration + 1,
				format!(
					"snapshot install regressed epoch: apply_epoch={} install_epoch={}",
					after_apply.current_epoch, after_install.current_epoch
				),
			));
		}
		for (epoch, watermark) in after_apply.e_safe_watermarks.iter() {
			if let Some(next) = after_install.e_safe_watermarks.get(epoch) {
				if next < watermark {
					return Err(SimFailure::new(
						"apply_order_races",
						seed,
						iteration + 1,
						format!(
							"snapshot install regressed watermark for epoch {epoch}: before={watermark:?} after={next:?}"
						),
					));
				}
			}
		}
		let expected_epoch_two = HybridTimestamp {
			physical_ms: 2_000 + iteration,
			logical: 0,
			node_id: 1,
		};
		if after_install.e_safe_watermarks.get(&2).copied() != Some(expected_epoch_two) {
			return Err(SimFailure::new(
				"apply_order_races",
				seed,
				iteration + 1,
				format!(
					"expected epoch-2 watermark {:?}, got {:?}",
					expected_epoch_two,
					after_install.e_safe_watermarks.get(&2).copied()
				),
			));
		}
	}

	Ok(iterations)
}

async fn run_interrupted_finalization_recovery_scenario(
	seed: u64,
	ops: u64,
) -> std::result::Result<u64, SimFailure> {
	let iterations = ops.max(1).min(6);
	for iteration in 0..iterations {
		let node_id = 800 + ((seed as u32).wrapping_add(iteration as u32) % 100);
		let cfg = NodeBootstrapConfig::dev(
			node_id,
			tempdir(&format!("interrupted-finalization-{seed}-{iteration}")),
			"127.0.0.1:0".into(),
		);
		let initial = WendelonNode::bootstrap_single_node(cfg.clone())
			.await
			.map_err(|err| {
				SimFailure::new(
					"interrupted_finalization_recovery",
					seed,
					iteration + 1,
					format!("initial bootstrap failed: {err}"),
				)
			})?;
		support::wait_for_local_leader(&initial).await;
		initial.shutdown().await.map_err(|err| {
			SimFailure::new(
				"interrupted_finalization_recovery",
				seed,
				iteration + 1,
				format!("initial shutdown failed: {err}"),
			)
		})?;

		let store = PendingFinalizationStore::open(pending_finalizations_path(&cfg.data_dir))
			.map_err(|err| {
				SimFailure::new(
					"interrupted_finalization_recovery",
					seed,
					iteration + 1,
					format!("open pending-finalization store failed: {err}"),
				)
			})?;
		let record = PendingFinalizationRecord::new(
			ClientWriteId {
				client_uuid: [iteration as u8; 16],
				sequence_no: iteration + 1,
				idempotency_ttl_ms: 60_000,
				_pad: 0,
			},
			true,
			TenantFinalization {
				marker: FinalizationMarker {
					log_index: 2,
					term: 1,
					epoch: 100 + iteration,
					commit_hlc: HybridTimestamp {
						physical_ms: 10_000 + iteration,
						logical: 0,
						node_id,
					},
					payload_hash: [iteration as u8; 32],
					e_safe_index: 2,
					durability: DurabilityLevel::QUORUM,
					_pad: [0; 7],
				},
				tenant_id: TenantId::ZERO,
				key_identity: Identity::blake3_tenant(
					TenantId::ZERO,
					format!("sim-interrupted-finalization-{seed}-{iteration}").as_bytes(),
				),
				charged_bytes: 0,
				record_count: 1,
			},
		);
		store.upsert(record.clone()).await.map_err(|err| {
			SimFailure::new(
				"interrupted_finalization_recovery",
				seed,
				iteration + 1,
				format!("seed pending-finalization record failed: {err}"),
			)
		})?;
		let seeded = store.load_all().await.map_err(|err| {
			SimFailure::new(
				"interrupted_finalization_recovery",
				seed,
				iteration + 1,
				format!("read back seeded pending-finalization record failed: {err}"),
			)
		})?;
		if seeded.len() != 1 {
			return Err(SimFailure::new(
				"interrupted_finalization_recovery",
				seed,
				iteration + 1,
				format!("expected exactly one seeded record, found {}", seeded.len()),
			));
		}

		let recovered = WendelonNode::bootstrap_single_node(cfg.clone())
			.await
			.map_err(|err| {
				SimFailure::new(
					"interrupted_finalization_recovery",
					seed,
					iteration + 1,
					format!("recovery bootstrap failed: {err}"),
				)
			})?;
		support::wait_for_local_leader(&recovered).await;

		let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
		loop {
			let pending = store.load_all().await.map_err(|err| {
				SimFailure::new(
					"interrupted_finalization_recovery",
					seed,
					iteration + 1,
					format!("load pending-finalization store during recovery failed: {err}"),
				)
			})?;
			let snapshot = recovered.state_machine.state_snapshot().await;
			if pending.is_empty()
				&& snapshot
					.e_safe_watermarks
					.get(&record.finalization.marker.epoch)
					.copied()
					.is_some_and(|hlc| hlc >= record.finalization.marker.commit_hlc)
			{
				break;
			}
			if tokio::time::Instant::now() >= deadline {
				recovered.shutdown().await.map_err(|err| {
					SimFailure::new(
						"interrupted_finalization_recovery",
						seed,
						iteration + 1,
						format!("recovery shutdown after timeout failed: {err}"),
					)
				})?;
				return Err(SimFailure::new(
					"interrupted_finalization_recovery",
					seed,
					iteration + 1,
					format!(
						"timed out waiting for startup reconciler to finalize seeded record; pending={} watermark={:?}",
						pending.len(),
						snapshot
							.e_safe_watermarks
							.get(&record.finalization.marker.epoch)
							.copied()
					),
				));
			}
			tokio::time::sleep(Duration::from_millis(20)).await;
		}

		recovered.shutdown().await.map_err(|err| {
			SimFailure::new(
				"interrupted_finalization_recovery",
				seed,
				iteration + 1,
				format!("recovery shutdown failed: {err}"),
			)
		})?;
	}

	Ok(iterations)
}

async fn run_lazy_migration_pressure_scenario(
	seed: u64,
	ops: u64,
) -> std::result::Result<u64, SimFailure> {
	let retries = usize::try_from(ops.max(1).min((DATA_RETRY_BUDGET - 1) as u64)).unwrap();
	let node_id = 600 + ((seed as u32) % 50);
	let node = support::bootstrap_node("sim-lazy-migration", node_id).await;
	let result = async {
		refresh_leases(&node).await;

		let service = Arc::new(FlakyMigrationService::new(
			node.router.clone() as Arc<dyn HrftService>,
			retries,
		));
		let address = format!("loopback://node-{}", node.local_node.id);
		let bootstrap = snapshot(
			1,
			Some(node.local_node.id),
			&[(node.local_node.id, &address)],
		);
		let topology = Arc::new(CountingTopologySource::new(bootstrap.clone()));
		let client = WendelonClient::new(support::loopback_client_config_with_topology(
			node.local_node,
			service.clone() as Arc<dyn HrftService>,
			bootstrap,
			topology.clone(),
		));

		let key = b"sim-migration-key";
		let value = b"sim-migration-value";
		client
			.write(WriteRequest {
				meta: write_meta(1, key, value, 1),
				payload: kv_batch(key, value),
			})
			.await
			.map_err(|err| {
				SimFailure::new(
					"lazy_migration_pressure",
					seed,
					1,
					format!("seed write failed: {err}"),
				)
			})?;

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
			.map_err(|err| {
				SimFailure::new(
					"lazy_migration_pressure",
					seed,
					2,
					format!("query start failed: {err}"),
				)
			})?
			.collect_all()
			.await
			.map_err(|err| {
				SimFailure::new(
					"lazy_migration_pressure",
					seed,
					3,
					format!("query collect failed: {err}"),
				)
			})?;
		if batches.len() != 1 {
			return Err(SimFailure::new(
				"lazy_migration_pressure",
				seed,
				4,
				format!("expected one batch after retries, got {}", batches.len()),
			));
		}
		if service.query_calls() != retries + 1 {
			return Err(SimFailure::new(
				"lazy_migration_pressure",
				seed,
				5,
				format!(
					"expected {} query attempts, got {}",
					retries + 1,
					service.query_calls()
				),
			));
		}
		if topology.fetches() != 0 {
			return Err(SimFailure::new(
				"lazy_migration_pressure",
				seed,
				6,
				format!(
					"migration pressure scenario should not refresh topology, got {} fetches",
					topology.fetches()
				),
			));
		}
		let expected_delay = MIGRATION_RETRY_BACKOFF * (retries as u32);
		if start.elapsed() < expected_delay {
			return Err(SimFailure::new(
				"lazy_migration_pressure",
				seed,
				7,
				format!(
					"expected retry backoff of at least {:?}, observed {:?}",
					expected_delay,
					start.elapsed()
				),
			));
		}
		Ok((retries + 2) as u64)
	}
	.await;
	support::abort_node(&node);
	result
}

async fn run_quota_authorization_boundary_scenario(
	seed: u64,
	_ops: u64,
) -> std::result::Result<u64, SimFailure> {
	let node_id = 700 + ((seed as u32) % 50);
	let node = support::bootstrap_node("sim-quota-auth", node_id).await;
	let result = async {
		refresh_leases(&node).await;
		node.raft
			.client_write(WendelonRequest::UpdateTenantQuota(UpdateTenantQuota {
				tenant_id: TenantId::ZERO,
				quota_bytes: 0,
			}))
			.await
			.map_err(|err| {
				SimFailure::new(
					"quota_authorization_boundary",
					seed,
					1,
					format!("quota update failed: {err}"),
				)
			})?;
		node.data_server.sync_tenant_policy_cache_once().await;

		let service = Arc::new(CountingWriteService::new(
			node.router.clone() as Arc<dyn HrftService>
		));
		let address = format!("loopback://node-{}", node.local_node.id);
		let bootstrap = snapshot(
			1,
			Some(node.local_node.id),
			&[(node.local_node.id, &address)],
		);
		let inner = WendelonClient::new(support::loopback_client_config(
			node.local_node,
			service.clone() as Arc<dyn HrftService>,
			bootstrap,
		));
		let sdk = Client::from_inner(inner);

		let err = sdk
			.write(
				tenant(),
				b"sim-quota-key",
				b"sim-quota-value",
				WriteOptions::default(),
			)
			.await
			.expect_err("quota-zero tenant must reject writes");
		if !matches!(
			err,
			ClientError::QuotaExceeded {
				tenant_id: TenantId::ZERO
			}
		) {
			return Err(SimFailure::new(
				"quota_authorization_boundary",
				seed,
				2,
				format!("expected QuotaExceeded, got {err:?}"),
			));
		}
		if service.write_calls() != 1 {
			return Err(SimFailure::new(
				"quota_authorization_boundary",
				seed,
				3,
				format!(
					"quota boundary should fail without write retries; observed {} write calls",
					service.write_calls()
				),
			));
		}

		let data_node = ActorIdentity::data_node(NodeId {
			id: 7,
			_reserved: 0,
		});
		let operator = ActorIdentity::new(
			ActorRole::Operator,
			None,
			None,
			vec!["cluster".into()],
			vec![9, 8, 7, 6],
		)
		.unwrap();
		let tenant_admin = ActorIdentity::new(
			ActorRole::TenantAdmin,
			None,
			None,
			vec!["cluster".into()],
			vec![1, 2, 3, 4],
		)
		.unwrap();
		let cluster_admin = ActorIdentity::new(
			ActorRole::ClusterAdmin,
			None,
			None,
			vec!["cluster".into()],
			vec![4, 3, 2, 1],
		)
		.unwrap();
		let backup_admin = ActorIdentity::new(
			ActorRole::BackupAdmin,
			None,
			None,
			vec!["cluster".into()],
			vec![5, 5, 5, 5],
		)
		.unwrap();
		let compliance_auditor = ActorIdentity::new(
			ActorRole::ComplianceAuditor,
			None,
			None,
			vec!["cluster".into()],
			vec![6, 6, 6, 6],
		)
		.unwrap();
		let observer = ActorIdentity::new(
			ActorRole::ReadOnlyObserver,
			None,
			None,
			vec!["cluster".into()],
			vec![6, 7, 8, 9],
		)
		.unwrap();
		if authorize_actor(&data_node, ActorPermission::InterNodeTransport).is_err()
			|| authorize_actor(&data_node, ActorPermission::ObservabilityReady).is_err()
			|| authorize_actor(&data_node, ActorPermission::ObservabilityMetrics).is_ok()
			|| authorize_actor(&data_node, ActorPermission::OpsControl).is_ok()
			|| authorize_actor(&operator, ActorPermission::OpsControl).is_err()
			|| authorize_actor(&tenant_admin, ActorPermission::TenantAdminMutation).is_err()
			|| authorize_actor(&tenant_admin, ActorPermission::TenantKekAdmin).is_ok()
			|| authorize_actor(&cluster_admin, ActorPermission::ClusterControl).is_err()
			|| authorize_actor(&cluster_admin, ActorPermission::TenantAdminMutation).is_ok()
			|| authorize_actor(&backup_admin, ActorPermission::TenantKekAdmin).is_err()
			|| authorize_actor(&backup_admin, ActorPermission::ClusterControl).is_ok()
			|| authorize_actor(&compliance_auditor, ActorPermission::AuditRead).is_err()
			|| authorize_actor(&compliance_auditor, ActorPermission::OpsControl).is_ok()
			|| authorize_actor(&operator, ActorPermission::ObservabilityMetrics).is_err()
			|| authorize_actor(&operator, ActorPermission::InterNodeTransport).is_ok()
			|| authorize_actor(&observer, ActorPermission::ObservabilityMetrics).is_err()
			|| authorize_actor(&observer, ActorPermission::OpsControl).is_ok()
		{
			return Err(SimFailure::new(
				"quota_authorization_boundary",
				seed,
				4,
				"authorization boundary matrix regressed".to_string(),
			));
		}

		Ok(10)
	}
	.await;
	support::abort_node(&node);
	result
}

async fn run_coverage_ladder_with_history_export(
	seed: u64,
	ops: u64,
	history_dir: Option<&Path>,
) -> std::result::Result<u64, SimFailure> {
	let budget = coverage_budget(ops);
	let mut executed = 0_u64;
	executed += run_counter_scenario(seed ^ 0xC0A1_C0A1, budget.counter_ops)?;
	executed += run_register_scenario(seed ^ 0xE11E_0001, budget.register_ops, history_dir)?;
	executed += run_object_store_scenario(seed ^ 0x0B5E_C7E0, budget.object_store_ops).await?;
	executed += run_transport_scenario(seed ^ 0x7A4A_F001, budget.transport_ops).await?;
	assert_eq!(
		executed, ops,
		"coverage ladder budget accounting drifted: expected {ops}, executed {executed}"
	);
	Ok(executed)
}

async fn run_coverage_ladder(seed: u64, ops: u64) -> std::result::Result<u64, SimFailure> {
	let history_dir = register_history_dir_from_env();
	run_coverage_ladder_with_history_export(seed, ops, history_dir.as_deref()).await
}

async fn run_named_scenario(
	name: &str,
	seed: u64,
	ops: u64,
) -> std::result::Result<u64, SimFailure> {
	match name {
		"coverage_ladder" => run_coverage_ladder(seed, ops).await,
		"replay_ordering" => run_replay_ordering_scenario(seed, ops).await,
		"apply_order_races" => run_apply_order_races_scenario(seed, ops).await,
		"interrupted_finalization_recovery" => {
			run_interrupted_finalization_recovery_scenario(seed, ops).await
		}
		"lazy_migration_pressure" => run_lazy_migration_pressure_scenario(seed, ops).await,
		"quota_authorization_boundary" => {
			run_quota_authorization_boundary_scenario(seed, ops).await
		}
		other => Err(SimFailure::new(
			other,
			seed,
			0,
			format!("unsupported simulator scenario {other}"),
		)),
	}
}

#[test]
fn simulator_seed_registry_files_are_well_formed() {
	for path in frozen_seed_paths().expect("seed directory must be readable") {
		let seed = load_frozen_seed(&path).expect("seed file must parse");
		validate_frozen_seed_file(&path, &seed);
	}
}

#[tokio::test(flavor = "current_thread")]
async fn simulator_frozen_seed_registry_replays() {
	let paths = frozen_seed_paths().expect("seed directory must be readable");
	for path in paths {
		let seed = load_frozen_seed(&path).expect("seed file must parse");
		validate_frozen_seed_file(&path, &seed);
		run_named_scenario(&seed.scenario, seed.seed, seed.ops)
			.await
			.unwrap_or_else(|err| panic!("{err}"));
	}
}

fn tempdir(tag: &str) -> PathBuf {
	let dir = std::env::temp_dir().join(format!(
		"wendelon-simulator-ladder-{tag}-{}-{}",
		std::process::id(),
		uuid::Uuid::new_v4()
	));
	std::fs::create_dir_all(&dir).unwrap();
	dir
}

#[tokio::test(flavor = "current_thread")]
async fn simulator_register_history_export_surface_writes_json_artifact() {
	let dir = tempdir("register-history");
	let seed = 7_u64;
	let ops = 128_u64;
	let executed = run_coverage_ladder_with_history_export(seed, ops, Some(&dir))
		.await
		.unwrap();
	assert_eq!(executed, ops);

	let mut artifacts = fs::read_dir(&dir)
		.unwrap()
		.map(|entry| entry.unwrap().path())
		.filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("json"))
		.collect::<Vec<_>>();
	artifacts.sort();
	assert_eq!(artifacts.len(), 1, "expected exactly one history artifact");

	let body = fs::read_to_string(&artifacts[0]).unwrap();
	let artifact: RegisterHistoryArtifact = serde_json::from_str(&body).unwrap();
	assert_eq!(artifact.scenario, "coverage_ladder");
	assert_eq!(artifact.seed, seed ^ 0xE11E_0001);
	assert_eq!(artifact.requested_ops, coverage_budget(ops).register_ops);
	assert!(!artifact.batches.is_empty());
	assert!(!artifact.events.is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn simulator_replay_ordering_scenario_is_live() {
	let executed = run_replay_ordering_scenario(0xD401, 4)
		.await
		.unwrap_or_else(|err| panic!("{err}"));
	assert_eq!(executed, 4);
}

#[tokio::test(flavor = "current_thread")]
async fn simulator_apply_order_races_scenario_is_live() {
	let executed = run_apply_order_races_scenario(0xD402, 3)
		.await
		.unwrap_or_else(|err| panic!("{err}"));
	assert_eq!(executed, 3);
}

#[tokio::test(flavor = "current_thread")]
async fn simulator_interrupted_finalization_recovery_scenario_is_live() {
	let executed = run_interrupted_finalization_recovery_scenario(0xD403, 2)
		.await
		.unwrap_or_else(|err| panic!("{err}"));
	assert_eq!(executed, 2);
}

#[tokio::test(flavor = "current_thread")]
async fn simulator_lazy_migration_pressure_scenario_is_live() {
	let retries = 3_u64;
	let executed = run_lazy_migration_pressure_scenario(0xD404, retries)
		.await
		.unwrap_or_else(|err| panic!("{err}"));
	assert!(executed >= retries);
}

#[tokio::test(flavor = "current_thread")]
async fn simulator_quota_authorization_boundary_scenario_is_live() {
	let executed = run_quota_authorization_boundary_scenario(0xD405, 1)
		.await
		.unwrap_or_else(|err| panic!("{err}"));
	assert!(executed >= 1);
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "run via ci/check_simulator_ladder.sh"]
async fn coverage_ladder() {
	let seed = ladder_seed_from_env();
	let ops = ladder_ops_from_env();
	let executed = run_coverage_ladder(seed, ops)
		.await
		.unwrap_or_else(|err| panic!("{err}"));
	println!(
		"SIMULATOR_OK seed={} scenario=coverage_ladder ops={} executed={}",
		seed, ops, executed
	);
}
