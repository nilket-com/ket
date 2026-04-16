//! Data Ring server (ADR 003 — the Stellarator Model).
//!
//! [`WendelonDataServer`] implements the [`DataRing`] trait by
//! bifurcating every client write: the Arrow IPC payload is
//! replicated directly to the data replica set over HRFT streams
//! (ADR 004) and only an 88-byte [`FinalizationMarker`] enters the
//! Raft Control Ring to advance the `E_safe` watermark.
//!
//! ## Why fan-out lives above the slab
//!
//! The slab is the durability boundary for *one* node; quorum is
//! the durability boundary for *the write*. The fan-out wraps
//! the slab append-plus-flush path so the local replica and the
//! remote replicas count toward quorum only after their payload
//! bytes are durably flushed. Payload bytes never enter the Raft
//! log — only the finalization marker does, per the Stellarator
//! invariant.
//!
//! ## V1 scope
//!
//! * Static replica set resolved through [`ReplicaRouter`]. A real
//!   deployment will feed this from the ADR 002 Unified Hash Ring
//!   projection off the Raft `ring(E)` snapshot; today we accept
//!   any `Arc<dyn ReplicaRouter>` so tests can inject a trivial
//!   in-memory topology.
//! * Concurrent dials through [`TransportProviderFactory`]. The
//!   factory already collapses duplicate dials per peer, so we
//!   spawn one task per remote replica without extra coordination.
//! * Quorum = `(RF / 2) + 1`. If the local node is in the replica
//!   set, its append-plus-flush counts as one durable ack.
//!
//! ## Idempotency
//!
//! A [`DashMap`] keyed on `(client_uuid, sequence_no)` deduplicates
//! retries. A hit replays the original [`WriteResponseMeta`] with
//! `status` overwritten to [`WriteStatus::DUPLICATE`] so the client
//! can tell a fresh success apart from a replay.

use crate::consensus::monitor::{NodeTracker, PhysicalClock};
use crate::consensus::raft_storage::WendelonLogStore;
use crate::consensus::{
	MigrationComplete, MigrationLedgerEntry, MigrationState, WendelonRaft, WendelonRequest,
	WendelonResponse, WendelonStateMachine,
};
use crate::network::core::{
	HRFT_MAGIC, read_frame, read_frame_from_header, read_frame_header, write_frame,
};
use crate::network::factory::TransportProviderFactory;
use crate::network::lease_signing::LeaseAuthority;
use crate::network::pool::BufPool;
use crate::network::ring::RingRouter;
use crate::network::router::DataRingServer;
use crate::network::transport::MembershipEpoch;
use crate::network::transport::TransportStream;
use crate::schema::{
	AcquireLeaseRequestMeta, AcquireLeaseResponseMeta, Consistency, FRAME_FLAG_STREAM_END,
	FinalizationMarker, FrameHeader, HybridTimestamp, LeaseDescriptor, LeaseStatus,
	MigrateRequestKind, MigrateRequestMeta, MigrateResponseMeta, MigrateStatus, MsgType, NodeId,
	QueryRequestMeta, QueryResponseMeta, ReadRequestMeta, ReadResponseMeta, ReadStatus,
	ReplicaAckLevel, ReplicaWriteMeta, ReplicaWriteRespMeta, ReplicaWriteStatus,
	StreamFinalizationRequestMeta, WriteRequestMeta, WriteResponseMeta, WriteStatus,
	migration_record_schema, read_key_schema, wpbc, write_payload_schema,
};
use crate::storage::core::{RecordLocation, StorageEngine};
use crate::storage::manager::SlabManager;
use arrow::array::{
	Array, FixedSizeBinaryArray, FixedSizeBinaryBuilder, LargeBinaryArray, LargeBinaryBuilder,
	UInt32Array, UInt32Builder, UInt64Array, UInt64Builder,
};
use arrow::compute::concat_batches;
use arrow::datatypes::Schema;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use dashmap::DashMap;
use highway::{HighwayHash, HighwayHasher, Key as HighwayKey};
use std::io::{Cursor, Error, ErrorKind};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tokio::task::{JoinHandle, JoinSet};

use crate::network::core::HrftFrame;
use crate::network::traits::DataRing;

/// HighwayHash seed. Stable for the lifetime of the process —
/// a real deployment derives this from the ring seed recorded in
/// `RaftEpochEntry` metadata so every node hashes keys the same
/// way; for V1 we pick a fixed constant and document the bind.
const HIGHWAY_KEY: HighwayKey = HighwayKey([
	0x5745_4e44_454c_4f4e,
	0x4841_5348_4b45_5901,
	0x4441_5441_5249_4e47,
	0x5354_454c_4c41_524f,
]);

type IdempotencyKey = ([u8; 16], u64);

// ============================================================
//  MIGRATION REGISTRY (Plan 019)
// ============================================================

/// Transient dedup for concurrent migration pulls (Plan 019 goal #5).
///
/// When a hot read races the background sweeper on the same
/// `(migration_id, key_hash)`, both paths might otherwise issue
/// a `MIGRATE_REQ` and hand the same record to
/// [`SlabManager::append_migration`] twice. `append_migration`
/// is already idempotent — that is the correctness boundary —
/// but serializing pulls here avoids the duplicate RPC round-trip
/// and the duplicate `(key_hash, hlc)` scan that would follow.
///
/// The registry hands out one `Arc<tokio::sync::Mutex<()>>` per
/// inflight key. Callers take the lock, re-check the local index,
/// and only issue a pull when the local index is still a miss.
/// The lock survives past the caller's guard so a subsequent racer
/// can synchronise on the same key without a secondary refresh.
#[derive(Default)]
pub struct MigrationRegistry {
	inflight: DashMap<(u64, u64), Arc<tokio::sync::Mutex<()>>>,
}

impl MigrationRegistry {
	pub fn new() -> Self {
		Self {
			inflight: DashMap::new(),
		}
	}

	/// Obtain the per-key gate. The returned `Arc` is cheap to
	/// clone and safe to hold across `.await`. Every caller for a
	/// given `(migration_id, key_hash)` observes the same lock, so
	/// at most one pull is in flight per key.
	pub fn gate(&self, migration_id: u64, key_hash: u64) -> Arc<tokio::sync::Mutex<()>> {
		self.inflight
			.entry((migration_id, key_hash))
			.or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
			.clone()
	}

	/// Drop the registry slot for `(migration_id, key_hash)`. Called
	/// by `try_migrate_point_read` once the pull has run to
	/// completion so the map's DRAM footprint caps at
	/// `O(concurrent_requests)` rather than growing with every key
	/// ever touched by a migration. Safe because
	/// [`SlabManager::append_migration`] is the real correctness
	/// boundary — a racer that arrives after removal simply mints
	/// a fresh mutex, re-checks the local index (now a hit), and
	/// skips the pull.
	pub fn remove(&self, migration_id: u64, key_hash: u64) {
		self.inflight.remove(&(migration_id, key_hash));
	}
}

/// RAII helper that drops the per-key slot out of the
/// [`MigrationRegistry`] once a pull finishes. Declared before the
/// mutex guard in the caller so the drop order is:
/// `_guard` (release mutex) → `_scope` (remove slot).
struct RegistryScopeGuard<'a> {
	registry: &'a MigrationRegistry,
	migration_id: u64,
	key_hash: u64,
}

impl Drop for RegistryScopeGuard<'_> {
	fn drop(&mut self) {
		self.registry.remove(self.migration_id, self.key_hash);
	}
}

/// Outcome of a lazy point-read migration pull.
///
/// Under quorum replication a single source's negative ack is not
/// authoritative: the key may live on a later candidate that has
/// the write. The classification matters to the caller because
/// `KEY_NOT_FOUND` is a client-visible authoritative miss, while
/// a failed migration must surface `MIGRATION_INCOMPLETE` so the
/// client retries rather than treating the response as ground truth.
///
/// * `Hit` — a source returned OK records that include the
///   requested `as_of_hlc` version; the read can proceed.
/// * `AuthoritativeMiss` — every source was reachable and
///   returned either OK (without the version) or `KEY_NOT_FOUND`.
///   The union of responses proves the key legitimately does not
///   exist at `as_of_hlc`.
/// * `Incomplete` — at least one source failed with a
///   non-authoritative status (transport error, `REDIRECT`,
///   `STALE_LEDGER`, `INTERNAL_ERROR`) and no remaining source
///   produced the requested version. The answer is unknown; the
///   caller must return `MIGRATION_INCOMPLETE`.
enum PointReadOutcome {
	Hit(RecordLocation, Vec<u8>),
	AuthoritativeMiss,
	Incomplete,
}

// ============================================================
//  REPLICA ROUTER
// ============================================================

/// Resolves a `key_hash` to the replica set that should store it.
///
/// The contract is deliberately narrow: return the ordered list of
/// `NodeId`s that form the replica set for this key. The ordering
/// is not consulted by the fan-out (every member must ack), but a
/// stable ordering helps tracing and topology tooling line up.
///
/// A real implementation projects off the Raft `ring(E)` snapshot
/// and filters for data-capable nodes (ADR 002). The V1 fallback
/// is [`StaticReplicaRouter`].
pub trait ReplicaRouter: Send + Sync {
	/// Members of the replica set for `key_hash`. The order is
	/// stable for a given topology so traces align; callers must
	/// not depend on the first entry being the local node.
	fn replicas(&self, key_hash: u64) -> Vec<NodeId>;

	/// Effective replication factor for this router. Exposed so the
	/// quorum math is source-of-truthed in one place.
	fn replication_factor(&self) -> usize;
}

/// Static ring router. Holds the full node list and an RF; resolves
/// each key's replica set as `RF` consecutive nodes starting at
/// `key_hash % nodes.len()`. This is a stand-in for the full ADR 002
/// projection — good enough for a bring-up where membership is
/// stable, fine for tests, and will be swapped for a real snapshot
/// reader once the Unified Hash Ring projection lands.
pub struct StaticReplicaRouter {
	nodes: Vec<NodeId>,
	rf: usize,
}

impl StaticReplicaRouter {
	/// Build a router over `nodes` with replication factor `rf`.
	/// The node list is sorted by numeric id for deterministic
	/// ordering; duplicate ids are deduplicated.
	pub fn new(mut nodes: Vec<NodeId>, rf: usize) -> Self {
		nodes.sort_by_key(|n| n.id);
		nodes.dedup_by_key(|n| n.id);
		let rf = rf.max(1);
		Self { nodes, rf }
	}
}

impl ReplicaRouter for StaticReplicaRouter {
	fn replicas(&self, key_hash: u64) -> Vec<NodeId> {
		if self.nodes.is_empty() {
			return Vec::new();
		}
		let n = self.nodes.len();
		let take = self.rf.min(n);
		let start = (key_hash as usize) % n;
		(0..take).map(|i| self.nodes[(start + i) % n]).collect()
	}

	fn replication_factor(&self) -> usize {
		self.rf.min(self.nodes.len().max(1))
	}
}

// ============================================================
//  SERVER
// ============================================================

/// Data Ring front door. Owns the local slab, a client to the Raft
/// Control Ring, and everything needed to run the bifurcated write
/// path: a [`ReplicaRouter`] to resolve the replica set and a
/// [`TransportProviderFactory`] to open HRFT streams to remote
/// replicas.
pub struct WendelonDataServer {
	slab: Arc<SlabManager>,
	raft: WendelonRaft,
	log_store: WendelonLogStore,
	/// Shared SM handle. Needed so `read` can consult the E_safe
	/// watermark for the epoch the client is asking against; also
	/// hints the "current epoch" for write-side validation.
	state_machine: WendelonStateMachine,
	/// Local node identity, populated into every response's
	/// `leader_hint` and used to decide whether the local slab
	/// append counts toward quorum on a given key.
	local_node: NodeId,
	/// Local physical clock used to merge client HLCs into the
	/// leader's authoritative commit timestamp.
	clock: Arc<dyn PhysicalClock>,
	/// Resolves a `key_hash` to the replica set that should hold
	/// it. Injected so the projection source (Unified Hash Ring
	/// snapshot, static dev topology, test mock) is the caller's
	/// concern, not this file's.
	router: Arc<dyn ReplicaRouter>,
	/// Shared connection factory. Caches sessions per `NodeId`,
	/// negotiates tiers, and collapses duplicate concurrent dials
	/// so spawning one fan-out task per peer is safe without extra
	/// locks here.
	factory: Arc<TransportProviderFactory>,
	/// Shared buffer pool. Inbound REPLICATE frames draw their
	/// payload sections from it and the response `meta` buffer is
	/// encoded into a pooled Vec on the reply path.
	pool: Arc<BufPool>,
	/// Exactly-once cache. `WriteResponseMeta` is `Copy`, so
	/// lookups are O(1) under DashMap's shard striping.
	idempotency: Arc<DashMap<IdempotencyKey, WriteResponseMeta>>,
	/// Last HLC the leader assigned to a committed-or-inflight write.
	/// Protects `max(local_hlc, client_hlc) + 1` across concurrent
	/// write paths so bulk streams and unary writes share one
	/// monotonic authority.
	last_commit_hlc: Arc<Mutex<HybridTimestamp>>,
	/// Optional liveness + drift tracker (ADR 001, ADR 012). When
	/// populated, every inbound peer-to-peer RPC feeds the peer's
	/// HLC physical component into it so the drift monitor and
	/// eviction watchdog can see the cluster.
	tracker: Option<Arc<NodeTracker>>,
	/// Local snapshot of leases granted to this node (plan 018).
	/// Every entry is a `LeaseDescriptor` the Raft Leader has
	/// already committed. The write path and the LINEARIZABLE
	/// read path consult this list before accepting traffic.
	///
	/// The cache is intentionally a small `Vec` rather than a
	/// range-indexed structure: a data node holds at most O(vnodes
	/// / RF) leases, so a linear scan is cheap and overlap-free
	/// by construction (the state machine rejects overlaps on grant).
	leases: Arc<RwLock<Vec<LeaseDescriptor>>>,
	/// When `Some`, every inbound write / LINEARIZABLE read is
	/// fenced against the lease cache. `None` (the default) leaves
	/// the gate disabled so existing single-node tests and the
	/// multi-node bring-up path before a lease manager has spun
	/// up do not spuriously reject traffic. Production bootstrap
	/// calls [`Self::with_lease_gate`] to flip this on after the
	/// lease manager has registered its first grant.
	lease_gate_enabled: bool,
	/// Local lease signing key plus trusted issuer verifier set.
	lease_authority: Arc<LeaseAuthority>,
	/// Transient dedup for concurrent migration pulls (Plan 019).
	/// Correctness still lives inside
	/// [`SlabManager::append_migration`]; this is an optimization
	/// that keeps racing hot reads and sweeper passes from issuing
	/// duplicate `MIGRATE_REQ` round-trips.
	migration_registry: Arc<MigrationRegistry>,
	/// Max records per `RANGE_PAGE` pull. Defaults to
	/// [`DEFAULT_PAGE_MAX_RECORDS`]; tests shrink it so a paging
	/// regression can be exercised without seeding thousands of
	/// records per source. `AtomicU32` so the test-only setter
	/// can tune it after the server is wrapped in `Arc`.
	page_max_records: Arc<std::sync::atomic::AtomicU32>,
	/// Max arrow payload bytes per `RANGE_PAGE` pull. Same rationale
	/// as `page_max_records`.
	page_max_bytes: Arc<std::sync::atomic::AtomicU32>,
}

/// Default max records a single `RANGE_PAGE` response can emit
/// before the server sets `has_more = 1`.
pub const DEFAULT_PAGE_MAX_RECORDS: u32 = 1024;
/// Default max arrow bytes per `RANGE_PAGE` response.
pub const DEFAULT_PAGE_MAX_BYTES: u32 = 4 * 1024 * 1024;
/// Per-subscriber queue depth for committed finalization markers.
pub const STREAM_FINALIZATION_BUFFER: usize = 128;
/// Default wake cadence for the background migration sweeper.
pub const MIGRATION_SWEEP_INTERVAL: Duration = Duration::from_secs(5);
/// Retry delay after a migration sweep pass fails for one entry.
pub const MIGRATION_SWEEP_FAILURE_BACKOFF: Duration = Duration::from_secs(2);
/// Background migration is intentionally conservative: one
/// migration range per pass keeps the sweeper from competing with
/// foreground reads/writes or the tier sweeper for disk/network.
pub const MIGRATION_SWEEP_MAX_PER_PASS: usize = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LeaseGateResult {
	Valid,
	Missing,
	InvalidSignature,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LeaseAcquireOutcome {
	Granted(LeaseDescriptor),
	Denied(LeaseStatus),
}

impl Clone for WendelonDataServer {
	fn clone(&self) -> Self {
		Self {
			slab: self.slab.clone(),
			raft: self.raft.clone(),
			log_store: self.log_store.clone(),
			state_machine: self.state_machine.clone(),
			local_node: self.local_node,
			clock: self.clock.clone(),
			router: self.router.clone(),
			factory: self.factory.clone(),
			pool: self.pool.clone(),
			idempotency: self.idempotency.clone(),
			last_commit_hlc: self.last_commit_hlc.clone(),
			tracker: self.tracker.clone(),
			leases: self.leases.clone(),
			lease_gate_enabled: self.lease_gate_enabled,
			lease_authority: self.lease_authority.clone(),
			migration_registry: self.migration_registry.clone(),
			page_max_records: self.page_max_records.clone(),
			page_max_bytes: self.page_max_bytes.clone(),
		}
	}
}

impl WendelonDataServer {
	pub fn new(
		slab: Arc<SlabManager>,
		raft: WendelonRaft,
		log_store: WendelonLogStore,
		state_machine: WendelonStateMachine,
		local_node: NodeId,
		clock: Arc<dyn PhysicalClock>,
		router: Arc<dyn ReplicaRouter>,
		factory: Arc<TransportProviderFactory>,
		pool: Arc<BufPool>,
	) -> Self {
		Self {
			slab,
			raft,
			log_store,
			state_machine,
			local_node,
			clock,
			router,
			factory,
			pool,
			idempotency: Arc::new(DashMap::new()),
			last_commit_hlc: Arc::new(Mutex::new(HybridTimestamp {
				physical_ms: 0,
				logical: 0,
				node_id: local_node.id,
			})),
			tracker: None,
			leases: Arc::new(RwLock::new(Vec::new())),
			lease_gate_enabled: false,
			lease_authority: Arc::new(LeaseAuthority::dev_single_node(local_node)),
			migration_registry: Arc::new(MigrationRegistry::new()),
			page_max_records: Arc::new(std::sync::atomic::AtomicU32::new(DEFAULT_PAGE_MAX_RECORDS)),
			page_max_bytes: Arc::new(std::sync::atomic::AtomicU32::new(DEFAULT_PAGE_MAX_BYTES)),
		}
	}

	/// Test-only override for the `RANGE_PAGE` paging caps.
	///
	/// Production pulls use [`DEFAULT_PAGE_MAX_RECORDS`] /
	/// [`DEFAULT_PAGE_MAX_BYTES`]; integration tests shrink these
	/// so a paging regression is reachable without seeding
	/// thousands of records. Safe because the caps are a purely
	/// opportunistic chunking boundary — correctness is preserved
	/// regardless of their value. Takes `&self` so tests can tune
	/// it after wrapping in `Arc`.
	#[cfg(test)]
	pub fn set_page_caps_for_test(&self, max_records: u32, max_bytes: u32) {
		use std::sync::atomic::Ordering;
		self.page_max_records.store(max_records, Ordering::Relaxed);
		self.page_max_bytes.store(max_bytes, Ordering::Relaxed);
	}

	/// Wire a [`NodeTracker`] into the server so every inbound peer
	/// RPC ingests the peer's HLC. Builder-style so production
	/// bootstrap can attach the tracker without rewriting every
	/// call site, and tests stay construction-trivial.
	pub fn with_tracker(mut self, tracker: Arc<NodeTracker>) -> Self {
		self.tracker = Some(tracker);
		self
	}

	/// Enable the per-range lease gate (plan 018). Must be toggled
	/// after the lease manager has filed its first grant — turning
	/// it on with an empty cache rejects every write.
	pub fn with_lease_gate(mut self, enabled: bool) -> Self {
		self.lease_gate_enabled = enabled;
		self
	}

	pub fn with_lease_authority(mut self, authority: Arc<LeaseAuthority>) -> Self {
		self.lease_authority = authority;
		self
	}

	/// Shared handle to the lease cache. The lease manager writes
	/// to it; the write/read paths read from it. Exposed so the
	/// manager task (which lives in a separate module) can take an
	/// `Arc` without poking at private fields.
	pub fn lease_cache(&self) -> Arc<RwLock<Vec<LeaseDescriptor>>> {
		self.leases.clone()
	}

	fn merge_commit_hlc(&self, client_hlc: HybridTimestamp) -> HybridTimestamp {
		let now_hlc = HybridTimestamp {
			physical_ms: self.clock.now_ms(),
			logical: 0,
			node_id: self.local_node.id,
		};
		let mut guard = self
			.last_commit_hlc
			.lock()
			.expect("last_commit_hlc mutex poisoned");
		let leader_hlc = (*guard).max(now_hlc);
		let base = leader_hlc.max(client_hlc);
		let merged = if base.logical == u32::MAX {
			HybridTimestamp {
				physical_ms: base.physical_ms.saturating_add(1),
				logical: 0,
				node_id: self.local_node.id,
			}
		} else {
			HybridTimestamp {
				physical_ms: base.physical_ms,
				logical: base.logical + 1,
				node_id: self.local_node.id,
			}
		};
		*guard = merged;
		merged
	}

	fn make_signed_lease(
		&self,
		epoch: u64,
		holder: NodeId,
		range_start: u64,
		range_end: u64,
		lease_start: HybridTimestamp,
		lease_expiry: HybridTimestamp,
	) -> LeaseDescriptor {
		self.lease_authority.sign_lease(LeaseDescriptor {
			epoch,
			holder,
			range_start,
			range_end,
			lease_start,
			lease_expiry,
			min_commit_index: 0,
			issuer: self.local_node,
			signature: [0; 64],
		})
	}

	/// Test-only hook to seed a lease into the local cache without
	/// driving the full Raft grant path. Production code must never
	/// call this — the manager is the sole writer of real leases.
	#[cfg(test)]
	pub fn seed_lease_for_test(&self, lease: LeaseDescriptor) {
		self.leases
			.write()
			.expect("lease cache poisoned")
			.push(lease);
	}

	async fn lease_gate_result(
		&self,
		key_hash: u64,
		epoch: u64,
		now: HybridTimestamp,
	) -> LeaseGateResult {
		let lease = {
			let guard = self.leases.read().expect("lease cache poisoned");
			guard
				.iter()
				.find(|l| {
					l.holder.id == self.local_node.id
						&& l.epoch == epoch
						&& l.range_start <= key_hash
						&& key_hash <= l.range_end
						&& now < l.lease_expiry
				})
				.copied()
		};

		let Some(lease) = lease else {
			return LeaseGateResult::Missing;
		};

		let Some(current_leader) = self.raft.current_leader().await else {
			return LeaseGateResult::Missing;
		};
		if lease.issuer.id != current_leader {
			return LeaseGateResult::InvalidSignature;
		}
		match self.lease_authority.verify_lease(&lease) {
			Ok(()) => LeaseGateResult::Valid,
			Err(_) => LeaseGateResult::InvalidSignature,
		}
	}

	fn cache_granted_lease(&self, lease: LeaseDescriptor) {
		let mut guard = self.leases.write().expect("lease cache poisoned");
		if let Some(slot) = guard
			.iter_mut()
			.find(|l| l.range_start == lease.range_start && l.range_end == lease.range_end)
		{
			*slot = lease;
		} else {
			guard.push(lease);
		}
	}

	async fn request_remote_lease(
		&self,
		leader: NodeId,
		req: AcquireLeaseRequestMeta,
	) -> std::io::Result<LeaseAcquireOutcome> {
		let conn = self.factory.get_connection(leader).await?;
		let mut stream = conn.open_bi().await?;
		let header = FrameHeader {
			magic: HRFT_MAGIC,
			version: 1,
			msg_type: MsgType::LEASE_REQ.0,
			flags: 0,
			stream_id: stream.stream_id() as u32,
			meta_len: std::mem::size_of::<AcquireLeaseRequestMeta>() as u32,
			arrow_len: 0,
			trailing_len: 0,
		};
		write_frame(&mut *stream, &header, bytemuck::bytes_of(&req), None, None).await?;
		stream.finish().await?;
		let frame = read_frame(&mut *stream, &self.pool).await?;
		if frame.header.msg_type != MsgType::LEASE_RESP.0 {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"expected LEASE_RESP, got msg_type=0x{:02x}",
					frame.header.msg_type
				),
			));
		}
		if frame.meta.len() < std::mem::size_of::<AcquireLeaseResponseMeta>() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"LEASE_RESP meta too small",
			));
		}
		let resp: AcquireLeaseResponseMeta =
			*bytemuck::from_bytes(&frame.meta[..std::mem::size_of::<AcquireLeaseResponseMeta>()]);
		if resp.status != LeaseStatus::GRANTED {
			return Ok(LeaseAcquireOutcome::Denied(resp.status));
		}
		let trailing = frame.trailing.ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				"LEASE_RESP granted without trailing LeaseDescriptor",
			)
		})?;
		if trailing.len() < std::mem::size_of::<LeaseDescriptor>() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"LEASE_RESP trailing too small for LeaseDescriptor",
			));
		}
		let lease = *bytemuck::from_bytes(&trailing[..std::mem::size_of::<LeaseDescriptor>()]);
		Ok(LeaseAcquireOutcome::Granted(lease))
	}

	/// Single pass of the lease manager (plan 018):
	///
	///  1. Read the current ring epoch + the set of primary ranges
	///     owned by this node.
	///  2. Drop any cached lease whose epoch no longer matches — the
	///     cluster may have just transitioned to E+1, which invalidates
	///     every prior lease.
	///  3. For each owned range, propose a `WendelonRequest::AcquireLease`
	///     to the Raft Leader. On a `Success` commit, insert/replace the
	///     lease in the local cache. Denials are surfaced as `false` in
	///     the returned bool but are not an error — a concurrent holder
	///     may legitimately still own the range during a handoff.
	///
	/// Returns the number of ranges the node successfully fenced.
	pub async fn refresh_leases_once(
		&self,
		ring: &RingRouter,
		clock: &dyn PhysicalClock,
		ttl_ms: u32,
	) -> usize {
		let snapshot = ring.snapshot();
		let epoch = snapshot.epoch();
		let ranges = snapshot.primary_ranges_for(self.local_node);

		{
			let mut guard = self.leases.write().expect("lease cache poisoned");
			guard.retain(|l| l.epoch == epoch);
		}

		let now_ms = clock.now_ms();
		let now_hlc = HybridTimestamp {
			physical_ms: now_ms,
			logical: 0,
			node_id: self.local_node.id,
		};
		let expiry_hlc = HybridTimestamp {
			physical_ms: now_ms.saturating_add(ttl_ms as u64),
			logical: 0,
			node_id: self.local_node.id,
		};
		let current_leader = self.raft.current_leader().await;

		let mut granted = 0usize;
		for (range_start, range_end) in ranges {
			let request = AcquireLeaseRequestMeta {
				range_start,
				range_end,
				epoch,
				holder: self.local_node,
				requested_hlc: now_hlc,
				ttl_ms,
				_pad: 0,
			};
			let outcome = if current_leader == Some(self.local_node.id) {
				let lease = self.make_signed_lease(
					epoch,
					self.local_node,
					range_start,
					range_end,
					now_hlc,
					expiry_hlc,
				);
				match self
					.raft
					.client_write(WendelonRequest::AcquireLease(lease))
					.await
				{
					Ok(resp) => match resp.data {
						WendelonResponse::Success => Ok(LeaseAcquireOutcome::Granted(lease)),
						WendelonResponse::LeaseDenied => {
							Ok(LeaseAcquireOutcome::Denied(LeaseStatus::DENIED_OVERLAP))
						}
					},
					Err(err) => Err(Error::new(
						ErrorKind::Other,
						format!("lease manager: local AcquireLease commit failed: {err}"),
					)),
				}
			} else if let Some(leader_id) = current_leader {
				self.request_remote_lease(
					NodeId {
						id: leader_id,
						_reserved: 0,
					},
					request,
				)
				.await
			} else {
				continue;
			};
			match outcome {
				Ok(LeaseAcquireOutcome::Granted(lease)) => {
					if Some(lease.issuer.id) == current_leader
						&& self.lease_authority.verify_lease(&lease).is_ok()
					{
						self.cache_granted_lease(lease);
						granted += 1;
					} else {
						tracing::warn!(
							range_start,
							range_end,
							epoch,
							"lease manager: leader returned an invalidly signed lease"
						);
					}
				}
				Ok(LeaseAcquireOutcome::Denied(_status)) => {
					// Another holder (or a stale-epoch request)
					// blocks us — skip, keep the old entry if any.
					// The next tick retries.
				}
				Err(e) => {
					tracing::warn!(
						range_start,
						range_end,
						epoch,
						error = %e,
						"lease manager: client_write failed; will retry next tick"
					);
				}
			}
		}
		granted
	}

	/// Spawn the lease manager background task. Fires on every
	/// `epoch_rx.changed()` event and on a steady `tick` cadence so
	/// a node that missed a watch wake-up (e.g. because its Raft
	/// client_write errored mid-transfer) still renews before
	/// `ttl_ms` elapses.
	///
	/// The task terminates when `epoch_rx` is closed (every sender
	/// dropped). Callers hold the returned [`JoinHandle`] on the
	/// `NodeHandles` struct so dropping the node aborts the loop.
	pub fn spawn_lease_manager(
		self: Arc<Self>,
		ring: Arc<RingRouter>,
		clock: Arc<dyn PhysicalClock>,
		mut epoch_rx: watch::Receiver<Arc<MembershipEpoch>>,
		tick: Duration,
		ttl_ms: u32,
	) -> JoinHandle<()> {
		tokio::spawn(async move {
			// Seed immediately so the cache is warm before the gate
			// is flipped on elsewhere in the bootstrap sequence.
			let _ = self
				.refresh_leases_once(&ring, clock.as_ref(), ttl_ms)
				.await;
			let mut ticker = tokio::time::interval(tick);
			ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
			loop {
				tokio::select! {
					changed = epoch_rx.changed() => {
						if changed.is_err() {
							break; // senders dropped — clean exit
						}
					}
					_ = ticker.tick() => {}
				}
				let _ = self
					.refresh_leases_once(&ring, clock.as_ref(), ttl_ms)
					.await;
			}
		})
	}

	async fn pending_owned_migrations(&self) -> Vec<MigrationLedgerEntry> {
		let mut entries: Vec<_> = self
			.state_machine
			.state_snapshot()
			.await
			.migrations
			.values()
			.filter(|entry| {
				entry.logical_owner.id == self.local_node.id
					&& entry.state != MigrationState::Completed
			})
			.cloned()
			.collect();
		entries.sort_by_key(|entry| (entry.to_epoch, entry.range_start, entry.range_end));
		entries
	}

	async fn drive_migration_to_completion(
		&self,
		entry: &MigrationLedgerEntry,
	) -> std::io::Result<()> {
		self.pull_migration_range(entry, entry.range_start, entry.range_end)
			.await?;
		// The sweeper is the convergence path that declares a
		// migration complete. Flush every local slab before proposing
		// `MigrationComplete` so partially durable backfill never
		// becomes authoritative in the ledger.
		self.slab.fsync_all_local_slabs().await?;

		let completed_at_hlc = self.merge_commit_hlc(HybridTimestamp {
			physical_ms: 0,
			logical: 0,
			node_id: 0,
		});
		self.raft
			.client_write(WendelonRequest::MigrationComplete(MigrationComplete {
				migration_id: entry.migration_id,
				range_start: entry.range_start,
				range_end: entry.range_end,
				to_epoch: entry.to_epoch,
				completed_by: self.local_node,
				completed_at_hlc,
				migration_hlc_floor: entry.migration_hlc_floor,
			}))
			.await
			.map_err(|err| {
				Error::new(
					ErrorKind::Other,
					format!("migration complete commit failed: {err}"),
				)
			})?;
		Ok(())
	}

	async fn reclaim_tombstoned_ranges_once(
		&self,
		max_slabs: usize,
	) -> std::io::Result<crate::storage::manager::TombstoneCompactionReport> {
		let snapshot = self.state_machine.state_snapshot().await;
		self.slab
			.reclaim_tombstoned_ranges(&snapshot.tombstones, max_slabs)
			.await
	}

	/// Spawn the background migration sweeper.
	///
	/// Every node runs this loop, but only entries whose
	/// `logical_owner` is the local node are eligible. The sweeper is
	/// intentionally conservative: it processes at most
	/// `max_migrations_per_pass` entries per tick and backs off a
	/// failed entry before retrying so foreground traffic and the L2
	/// tier sweeper retain priority on network and disk.
	pub fn spawn_migration_sweeper(
		self: Arc<Self>,
		tick: Duration,
		failure_backoff: Duration,
		max_migrations_per_pass: usize,
	) -> JoinHandle<()> {
		let budget = max_migrations_per_pass.max(1);
		tokio::spawn(async move {
			let mut ticker = tokio::time::interval(tick);
			ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
			let mut backoff_until = std::collections::HashMap::<u64, tokio::time::Instant>::new();

			// Seed immediately so a recovered node starts converging
			// pending migrations without waiting for the first tick.
			loop {
				let now = tokio::time::Instant::now();
				let pending = self.pending_owned_migrations().await;
				let mut processed = 0usize;
				for entry in pending {
					if processed >= budget {
						break;
					}
					if backoff_until
						.get(&entry.migration_id)
						.is_some_and(|deadline| *deadline > now)
					{
						continue;
					}

					match self.drive_migration_to_completion(&entry).await {
						Ok(()) => {
							backoff_until.remove(&entry.migration_id);
							processed += 1;
						}
						Err(err) => {
							backoff_until.insert(
								entry.migration_id,
								tokio::time::Instant::now() + failure_backoff,
							);
							tracing::warn!(
								migration_id = entry.migration_id,
								range_start = entry.range_start,
								range_end = entry.range_end,
								error = %err,
								"migration sweeper pass failed; will retry after backoff"
							);
						}
					}
				}

				if let Err(err) = self.reclaim_tombstoned_ranges_once(1).await {
					tracing::warn!(
						error = %err,
						"migration sweeper reclaim pass failed; will retry on next tick"
					);
				}

				ticker.tick().await;
			}
		})
	}

	/// Fan-out write to a single remote replica. Opens one bidi
	/// stream, ships the REPLICATE_REQ frame, and awaits the
	/// REPLICATE_RESP. Every failure — dial, send, protocol,
	/// negative ack — surfaces as `Err`; successful responses carry
	/// an explicit acknowledgement level so the caller can count
	/// only durable acks toward quorum.
	async fn replicate_to_remote(
		factory: Arc<TransportProviderFactory>,
		pool: Arc<BufPool>,
		target: NodeId,
		meta: ReplicaWriteMeta,
		arrow_bytes: Arc<Vec<u8>>,
	) -> std::io::Result<ReplicaAckLevel> {
		let conn = factory.get_connection(target).await?;
		let mut stream = conn.open_bi().await?;

		let meta_bytes = bytemuck::bytes_of(&meta);
		let header = FrameHeader {
			magic: HRFT_MAGIC,
			version: 1,
			msg_type: MsgType::REPLICATE_REQ.0,
			flags: 0,
			stream_id: 0,
			meta_len: meta_bytes.len() as u32,
			arrow_len: arrow_bytes.len() as u32,
			trailing_len: 0,
		};
		write_frame(
			&mut *stream,
			&header,
			meta_bytes,
			Some(arrow_bytes.as_slice()),
			None,
		)
		.await?;
		stream
			.finish()
			.await
			.map_err(|e| Error::new(ErrorKind::BrokenPipe, e))?;

		let resp = read_frame(&mut *stream, &pool).await?;
		if resp.header.msg_type != MsgType::REPLICATE_RESP.0 {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"expected REPLICATE_RESP, got msg_type=0x{:02x}",
					resp.header.msg_type
				),
			));
		}
		if resp.meta.len() < std::mem::size_of::<ReplicaWriteRespMeta>() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"REPLICATE_RESP meta too small",
			));
		}
		let ack: &ReplicaWriteRespMeta =
			bytemuck::from_bytes(&resp.meta[..std::mem::size_of::<ReplicaWriteRespMeta>()]);
		if ack.status != ReplicaWriteStatus::OK {
			return Err(Error::new(
				ErrorKind::Other,
				format!("replica {target:?} rejected write: status={}", ack.status.0),
			));
		}
		Ok(ack.ack_level)
	}

	/// Unary compatibility path for QUERY_REQ. The production HRFT
	/// path goes through [`Self::handle_query_stream`] so large
	/// result sets can flow as multiple `QUERY_RESP` frames without
	/// one-frame concatenation. This helper keeps direct frame-based
	/// tests and any remaining unary callers working by packaging the
	/// entire response into one terminal frame.
	async fn handle_query(&self, frame: HrftFrame) -> std::io::Result<HrftFrame> {
		let (meta, bytecode) = self.decode_query_request_frame(&frame)?;

		let (resp_meta, arrow_bytes) = self
			.execute_query_with_migration_merge(&meta, &bytecode)
			.await?;
		Ok(self.build_query_response_frame(
			frame.header.stream_id,
			resp_meta,
			arrow_bytes,
			FRAME_FLAG_STREAM_END,
		))
	}

	fn decode_query_request_frame(
		&self,
		frame: &HrftFrame,
	) -> std::io::Result<(QueryRequestMeta, Vec<u8>)> {
		if frame.meta.len() < std::mem::size_of::<QueryRequestMeta>() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"QUERY_REQ meta too small",
			));
		}
		let meta: QueryRequestMeta =
			*bytemuck::from_bytes(&frame.meta[..std::mem::size_of::<QueryRequestMeta>()]);
		let bytecode = if meta.bytecode_len == 0 {
			Vec::new()
		} else {
			let trailing = frame.trailing.as_ref().ok_or_else(|| {
				Error::new(
					ErrorKind::InvalidData,
					"QUERY_REQ declared bytecode_len > 0 but trailing is absent",
				)
			})?;
			let want = meta.bytecode_len as usize;
			if trailing.len() < want {
				return Err(Error::new(
					ErrorKind::InvalidData,
					"QUERY_REQ trailing is shorter than declared bytecode_len",
				));
			}
			trailing[..want].to_vec()
		};
		Ok((meta, bytecode))
	}

	fn build_query_response_frame(
		&self,
		stream_id: u32,
		resp: QueryResponseMeta,
		arrow_bytes: Option<Vec<u8>>,
		flags: u16,
	) -> HrftFrame {
		let mut body = self.pool.acquire();
		body.as_mut_vec()
			.extend_from_slice(bytemuck::bytes_of(&resp));
		let arrow_payload = arrow_bytes.map(|bytes| {
			let mut buf = self.pool.acquire();
			buf.as_mut_vec().extend_from_slice(&bytes);
			buf
		});
		let arrow_len = arrow_payload
			.as_ref()
			.map(|buf| buf.len() as u32)
			.unwrap_or(resp.arrow_len);
		HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::QUERY_RESP.0,
				flags,
				stream_id,
				meta_len: body.len() as u32,
				arrow_len,
				trailing_len: 0,
			},
			meta: body,
			arrow_payload,
			trailing: None,
		}
	}

	fn decode_write_request_frame(
		&self,
		frame: &HrftFrame,
		label: &'static str,
	) -> std::io::Result<(WriteRequestMeta, RecordBatch)> {
		if frame.meta.len() < std::mem::size_of::<WriteRequestMeta>() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!("{label} meta too small"),
			));
		}
		let meta: WriteRequestMeta =
			*bytemuck::from_bytes(&frame.meta[..std::mem::size_of::<WriteRequestMeta>()]);
		let payload_bytes = frame.arrow_payload.as_ref().ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				format!("{label} missing Arrow payload"),
			)
		})?;
		let payload = decode_batch(payload_bytes)?;
		validate_write_request_batch(&meta, &payload)?;
		Ok((meta, payload))
	}

	fn build_write_response_frame(
		&self,
		msg_type: MsgType,
		stream_id: u32,
		resp: WriteResponseMeta,
		trailing: Option<FinalizationMarker>,
	) -> HrftFrame {
		let mut meta_buf = self.pool.acquire();
		meta_buf
			.as_mut_vec()
			.extend_from_slice(bytemuck::bytes_of(&resp));
		let trailing = trailing.map(|marker| {
			let mut buf = self.pool.acquire();
			buf.as_mut_vec()
				.extend_from_slice(bytemuck::bytes_of(&marker));
			buf
		});
		let trailing_len = trailing.as_ref().map(|buf| buf.len() as u32).unwrap_or(0);

		HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: msg_type.0,
				flags: 0,
				stream_id,
				meta_len: meta_buf.len() as u32,
				arrow_len: 0,
				trailing_len,
			},
			meta: meta_buf,
			arrow_payload: None,
			trailing,
		}
	}

	fn decode_stream_finalization_request(
		&self,
		frame: &HrftFrame,
	) -> std::io::Result<StreamFinalizationRequestMeta> {
		if frame.meta.len() < std::mem::size_of::<StreamFinalizationRequestMeta>() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"STREAM_FIN_REQ meta too small",
			));
		}
		if frame.arrow_payload.is_some() || frame.trailing.is_some() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"STREAM_FIN_REQ must not carry arrow or trailing payloads",
			));
		}
		Ok(*bytemuck::from_bytes(
			&frame.meta[..std::mem::size_of::<StreamFinalizationRequestMeta>()],
		))
	}

	async fn handle_stream_finalization_stream(
		&self,
		first_header: FrameHeader,
		mut stream: Box<dyn TransportStream>,
	) -> std::io::Result<()> {
		let frame = read_frame_from_header(&mut *stream, &self.pool, first_header).await?;
		let request = self.decode_stream_finalization_request(&frame)?;
		let mut rx = self
			.stream_finalization(request.start_log_index, request.epoch)
			.await?;

		while let Some(marker) = rx.recv().await {
			let header = FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::STREAM_FIN_EVT.0,
				flags: 0,
				stream_id: frame.header.stream_id,
				meta_len: std::mem::size_of::<FinalizationMarker>() as u32,
				arrow_len: 0,
				trailing_len: 0,
			};
			write_frame(
				&mut *stream,
				&header,
				bytemuck::bytes_of(&marker),
				None,
				None,
			)
			.await?;
		}

		stream.finish().await?;
		Ok(())
	}

	async fn handle_query_stream(
		&self,
		first_header: FrameHeader,
		mut stream: Box<dyn TransportStream>,
	) -> std::io::Result<()> {
		let frame = read_frame_from_header(&mut *stream, &self.pool, first_header).await?;
		let (meta, bytecode) = self.decode_query_request_frame(&frame)?;
		let plan = self
			.prepare_query_execution_plan_with_migration(&meta, &bytecode)
			.await?;

		if plan.response.status != ReadStatus::OK || plan.locations.is_empty() {
			let response = self.build_query_response_frame(
				frame.header.stream_id,
				plan.response,
				None,
				FRAME_FLAG_STREAM_END,
			);
			write_frame(&mut *stream, &response.header, &response.meta, None, None).await?;
			stream.finish().await?;
			return Ok(());
		}

		for (index, location) in plan.locations.iter().copied().enumerate() {
			let batch = materialize_query_batch(&self.slab, location, &bytecode).await?;
			let arrow = encode_batch(&batch)?;
			let is_last = index + 1 == plan.locations.len();
			let flags = if is_last { FRAME_FLAG_STREAM_END } else { 0 };
			if index == 0 {
				let response = QueryResponseMeta {
					arrow_len: arrow.len() as u32,
					..plan.response
				};
				let meta_bytes = bytemuck::bytes_of(&response);
				let header = FrameHeader {
					magic: HRFT_MAGIC,
					version: 1,
					msg_type: MsgType::QUERY_RESP.0,
					flags,
					stream_id: frame.header.stream_id,
					meta_len: meta_bytes.len() as u32,
					arrow_len: arrow.len() as u32,
					trailing_len: 0,
				};
				write_frame(&mut *stream, &header, meta_bytes, Some(&arrow), None).await?;
			} else {
				let header = FrameHeader {
					magic: HRFT_MAGIC,
					version: 1,
					msg_type: MsgType::QUERY_RESP.0,
					flags,
					stream_id: frame.header.stream_id,
					meta_len: 0,
					arrow_len: arrow.len() as u32,
					trailing_len: 0,
				};
				write_frame(&mut *stream, &header, &[], Some(&arrow), None).await?;
			}
		}

		stream.finish().await?;
		Ok(())
	}

	async fn handle_bulk_stream(
		&self,
		first_header: FrameHeader,
		mut stream: Box<dyn TransportStream>,
	) -> std::io::Result<()> {
		let mut next_header = Some(first_header);
		loop {
			let header = if let Some(header) = next_header.take() {
				header
			} else {
				match read_frame_header(&mut *stream).await {
					Ok(header) => header,
					Err(e) if e.kind() == ErrorKind::UnexpectedEof => break,
					Err(e) => return Err(e),
				}
			};

			if header.msg_type != MsgType::BULK_WRITE_REQ.0 {
				return Err(Error::new(
					ErrorKind::InvalidData,
					format!(
						"BULK_WRITE stream expected BULK_WRITE_REQ item, got msg_type=0x{:02x}",
						header.msg_type
					),
				));
			}

			let frame = read_frame_from_header(&mut *stream, &self.pool, header).await?;
			let (meta, payload) = self.decode_write_request_frame(&frame, "BULK_WRITE_REQ")?;
			let (resp, trailing) = match self.write(meta, payload).await {
				Ok(ok) => ok,
				Err(_) => (
					error_response(&meta, WriteStatus::INTERNAL_ERROR, self.local_node),
					None,
				),
			};
			let response = self.build_write_response_frame(
				MsgType::BULK_WRITE_RESP,
				frame.header.stream_id,
				resp,
				trailing,
			);
			write_frame(
				&mut *stream,
				&response.header,
				&response.meta,
				response.arrow_payload.as_deref(),
				response.trailing.as_deref(),
			)
			.await?;
		}

		stream.finish().await?;
		Ok(())
	}

	async fn handle_write(&self, frame: HrftFrame) -> std::io::Result<HrftFrame> {
		let (meta, payload) = self.decode_write_request_frame(&frame, "WRITE_REQ")?;

		let (resp, trailing) = match self.write(meta, payload).await {
			Ok(ok) => ok,
			Err(_) => (
				error_response(&meta, WriteStatus::INTERNAL_ERROR, self.local_node),
				None,
			),
		};
		Ok(self.build_write_response_frame(
			MsgType::WRITE_RESP,
			frame.header.stream_id,
			resp,
			trailing,
		))
	}

	async fn handle_read(&self, frame: HrftFrame) -> std::io::Result<HrftFrame> {
		if frame.meta.len() < std::mem::size_of::<ReadRequestMeta>() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"READ_REQ meta too small",
			));
		}
		let meta: ReadRequestMeta =
			*bytemuck::from_bytes(&frame.meta[..std::mem::size_of::<ReadRequestMeta>()]);
		let key_bytes = frame
			.arrow_payload
			.as_ref()
			.ok_or_else(|| Error::new(ErrorKind::InvalidData, "READ_REQ missing Arrow payload"))?;
		let key = decode_batch(key_bytes)?;
		validate_read_request_batch(&meta, &key)?;

		let (resp, value_batch) = self.read(meta, key).await?;
		let arrow_payload = value_batch.map(|batch| {
			let bytes = encode_batch(&batch)?;
			let mut buf = self.pool.acquire();
			buf.as_mut_vec().extend_from_slice(&bytes);
			Ok::<_, std::io::Error>(buf)
		});
		let arrow_payload = match arrow_payload.transpose()? {
			Some(buf) => Some(buf),
			None => None,
		};
		let mut meta_buf = self.pool.acquire();
		meta_buf
			.as_mut_vec()
			.extend_from_slice(bytemuck::bytes_of(&resp));

		Ok(HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::READ_RESP.0,
				flags: 0,
				stream_id: frame.header.stream_id,
				meta_len: meta_buf.len() as u32,
				arrow_len: arrow_payload
					.as_ref()
					.map(|buf| buf.len() as u32)
					.unwrap_or(0),
				trailing_len: 0,
			},
			meta: meta_buf,
			arrow_payload,
			trailing: None,
		})
	}

	/// Handle an inbound MIGRATE_REQ frame (Plan 019).
	///
	/// Administrative peer-to-peer RPC that sources retained
	/// versions from a prior physical holder so the new logical
	/// owner can fill a lazy-migration miss. Bypasses leases —
	/// migration is an internal data-movement protocol — but still
	/// validates against the Raft Migration Ledger so a node that
	/// believes it owns a range cannot bully another replica into
	/// handing over bytes.
	///
	/// Validation order (each failure maps to a distinct status):
	///   1. Ledger lookup by `migration_id` — `STALE_LEDGER` if
	///      absent or `to_epoch` / `migration_hlc_floor` diverge.
	///   2. `logical_owner == requester` — `NOT_AUTHORIZED` if not.
	///   3. Ledger state already `Completed` by a different holder
	///      — `REDIRECT` with `source_hint = completed_by` so the
	///      caller can short-circuit the chain.
	///
	/// On `OK`, the response body is an Arrow IPC stream over
	/// [`migration_record_schema`] with one row per retained
	/// version. Point lookups return every version of the single
	/// key whose hash sits at `range_start`; range pages walk the
	/// inclusive `[range_start, range_end]` window honouring
	/// `max_records` / `max_bytes`, and set `next_cursor` for
	/// pagination.
	async fn handle_migrate(&self, frame: HrftFrame) -> std::io::Result<HrftFrame> {
		if frame.meta.len() < std::mem::size_of::<MigrateRequestMeta>() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"MIGRATE_REQ meta too small",
			));
		}
		let meta: MigrateRequestMeta =
			*bytemuck::from_bytes(&frame.meta[..std::mem::size_of::<MigrateRequestMeta>()]);

		let sm = self.state_machine.state_snapshot().await;
		let (resp_meta, arrow_bytes) = match sm.migrations.get(&meta.migration_id) {
			None => (
				migrate_error_meta(&meta, MigrateStatus::STALE_LEDGER, ZERO_NODE),
				None,
			),
			Some(entry)
				if entry.to_epoch != meta.expected_to_epoch
					|| entry.migration_hlc_floor != meta.migration_hlc_floor
					|| entry.range_start != meta.range_start
					|| entry.range_end != meta.range_end =>
			{
				(
					migrate_error_meta(&meta, MigrateStatus::STALE_LEDGER, ZERO_NODE),
					None,
				)
			}
			Some(entry) if entry.logical_owner.id != meta.requester.id => (
				migrate_error_meta(&meta, MigrateStatus::NOT_AUTHORIZED, ZERO_NODE),
				None,
			),
			Some(entry)
				if entry.state == MigrationState::Completed
					&& entry
						.completed_by
						.map(|c| c.id != self.local_node.id)
						.unwrap_or(false) =>
			{
				let hint = entry.completed_by.unwrap_or(ZERO_NODE);
				(
					migrate_error_meta(&meta, MigrateStatus::REDIRECT, hint),
					None,
				)
			}
			Some(entry) => self.serve_migrate_ok(&meta, entry).await?,
		};

		let mut body = self.pool.acquire();
		body.as_mut_vec()
			.extend_from_slice(bytemuck::bytes_of(&resp_meta));
		let arrow_payload = arrow_bytes.map(|bytes| {
			let mut buf = self.pool.acquire();
			buf.as_mut_vec().extend_from_slice(&bytes);
			buf
		});
		let arrow_len = resp_meta.bytes_returned;
		let header = FrameHeader {
			magic: HRFT_MAGIC,
			version: 1,
			msg_type: MsgType::MIGRATE_RESP.0,
			flags: 0,
			stream_id: frame.header.stream_id,
			meta_len: body.len() as u32,
			arrow_len,
			trailing_len: 0,
		};
		Ok(HrftFrame {
			header,
			meta: body,
			arrow_payload,
			trailing: None,
		})
	}

	/// Build the `OK`-status response body: scan retained versions
	/// per the request's `kind`, materialise every frame off the
	/// slab, and pack them into an Arrow IPC batch.
	async fn serve_migrate_ok(
		&self,
		meta: &MigrateRequestMeta,
		entry: &MigrationLedgerEntry,
	) -> std::io::Result<(MigrateResponseMeta, Option<Vec<u8>>)> {
		// Locations to return. Point lookups scan a single-key
		// window; range pages walk the full ledger window keyed by
		// `(cursor_hash, cursor_hlc)` so fat keys with more retained
		// versions than fit in one page still stream exhaustively.
		let mut locations: Vec<(u64, RecordLocation)> = match meta.kind {
			MigrateRequestKind::POINT_LOOKUP => self.slab.scan_all_versions(
				meta.range_start,
				meta.range_start,
				entry.migration_hlc_floor,
			),
			MigrateRequestKind::RANGE_PAGE => self.slab.scan_all_versions_from(
				meta.range_start,
				meta.range_end,
				entry.migration_hlc_floor,
				meta.cursor_hash,
				meta.cursor_hlc,
			),
			_ => {
				return Ok((
					migrate_error_meta(meta, MigrateStatus::INTERNAL_ERROR, ZERO_NODE),
					None,
				));
			}
		};

		if meta.kind == MigrateRequestKind::POINT_LOOKUP && locations.is_empty() {
			return Ok((
				migrate_error_meta(meta, MigrateStatus::KEY_NOT_FOUND, ZERO_NODE),
				None,
			));
		}

		// Paging caps. Records are sorted (key_hash, hlc ASC), so
		// truncating at the caps yields a deterministic boundary.
		// `(next_cursor_hash, next_cursor_hlc)` names the last
		// emitted MVCC slot so the follow-up resumes strictly after
		// it — including mid-key for fat keys.
		let max_records = if meta.max_records == 0 {
			usize::MAX
		} else {
			meta.max_records as usize
		};
		let max_bytes = if meta.max_bytes == 0 {
			usize::MAX
		} else {
			meta.max_bytes as usize
		};

		let total_locations = locations.len();
		let mut rows: Vec<(u64, RecordLocation, Vec<u8>)> = Vec::new();
		let mut packed_bytes: usize = 0;
		let mut last_key: u64 = 0;
		let mut last_hlc: HybridTimestamp = bytemuck::Zeroable::zeroed();
		let mut truncated = false;
		for (key_hash, loc) in locations.drain(..) {
			if rows.len() >= max_records {
				truncated = true;
				break;
			}
			let frame = self.slab.read(loc).await?;
			if packed_bytes.saturating_add(frame.len()) > max_bytes && !rows.is_empty() {
				truncated = true;
				break;
			}
			packed_bytes = packed_bytes.saturating_add(frame.len());
			last_key = key_hash;
			last_hlc = loc.hlc;
			rows.push((key_hash, loc, frame));
		}
		let emitted_all = rows.len() == total_locations;
		let has_more = meta.kind == MigrateRequestKind::RANGE_PAGE && truncated && !emitted_all;
		let (next_cursor_hash, next_cursor_hlc) = if has_more {
			(last_key, last_hlc)
		} else {
			(0u64, bytemuck::Zeroable::zeroed())
		};

		let arrow_bytes = encode_migration_records(&rows)?;
		let versions_returned = rows.len() as u64;
		let bytes_returned = arrow_bytes.len() as u32;

		let resp = MigrateResponseMeta {
			status: MigrateStatus::OK,
			has_more: if has_more { 1 } else { 0 },
			_pad0: [0; 6],
			migration_id: meta.migration_id,
			source_hint: ZERO_NODE,
			next_cursor_hash,
			next_cursor_hlc,
			versions_returned,
			bytes_returned,
			_pad1: [0; 4],
		};
		Ok((resp, Some(arrow_bytes)))
	}

	/// Read-path lazy-migration fallback (Plan 019 goal #3).
	///
	/// Called when [`SlabManager::lookup_as_of`] returns `None` on
	/// a node that is the current logical owner of the key's
	/// range. Consults the ledger for a pending migration
	/// covering `key_hash`, gates concurrent pulls through the
	/// [`MigrationRegistry`], re-checks the local index after the
	/// gate releases, and — on a still-miss — issues a
	/// `POINT_LOOKUP` `MIGRATE_REQ` to the ledger's preferred
	/// source, ingesting every returned version via
	/// `append_migration`.
	///
	/// Returns a [`PointReadOutcome`]: `Hit` on a version match,
	/// `AuthoritativeMiss` when every source proves the key absent
	/// at `as_of_hlc`, or `Incomplete` when any source failed
	/// non-authoritatively and no remaining candidate satisfied
	/// the read. I/O errors on the local slab surface as `Err`.
	async fn try_migrate_point_read(
		&self,
		key_hash: u64,
		as_of_hlc: HybridTimestamp,
	) -> std::io::Result<PointReadOutcome> {
		let sm = self.state_machine.state_snapshot().await;
		let Some(entry) = pending_owned_entry(&sm.migrations, key_hash, self.local_node) else {
			return Ok(PointReadOutcome::AuthoritativeMiss);
		};

		let gate = self.migration_registry.gate(entry.migration_id, key_hash);
		// Registry cleanup guard — drops AFTER `_guard` releases the
		// mutex, so the inflight slot is freed only once the pull has
		// fully completed. A racer that arrives after removal mints a
		// fresh mutex, re-checks the local index (now a hit post
		// `append_migration`), and skips the RPC.
		let _scope = RegistryScopeGuard {
			registry: &self.migration_registry,
			migration_id: entry.migration_id,
			key_hash,
		};
		let _guard = gate.lock().await;

		// Another task may have filled this key while we waited
		// on the gate. If so, skip the RPC entirely — the fast
		// path is the only ack the caller needed.
		if let Some(location) = self.slab.lookup_as_of(key_hash, as_of_hlc) {
			let bytes = self.slab.read(location).await?;
			return Ok(PointReadOutcome::Hit(location, bytes));
		}

		// Walk the ledger's ordered source candidates. Classify
		// each response into two buckets:
		//
		// * *authoritative* — the source is reachable and replies
		//   that it definitively lacks the version at `as_of_hlc`.
		//   An OK response (records may satisfy the read or not)
		//   and `KEY_NOT_FOUND` both count here. A unanimous
		//   authoritative-miss proves the key absent.
		//
		// * *inconclusive* — a transport error, `REDIRECT`,
		//   `STALE_LEDGER`, or `INTERNAL_ERROR`. The source cannot
		//   speak for the record's existence, so the loop must not
		//   collapse the union into a false negative.
		//
		// After the loop, `any_inconclusive` decides between
		// `AuthoritativeMiss` (safe to return `KEY_NOT_FOUND` to
		// the client) and `Incomplete` (surface
		// `MIGRATION_INCOMPLETE` so the client retries).
		let req = MigrateRequestMeta {
			migration_id: entry.migration_id,
			expected_to_epoch: entry.to_epoch,
			requester: self.local_node,
			range_start: key_hash,
			range_end: key_hash,
			migration_hlc_floor: entry.migration_hlc_floor,
			cursor_hash: 0,
			cursor_hlc: bytemuck::Zeroable::zeroed(),
			max_records: 0,
			max_bytes: 0,
			kind: MigrateRequestKind::POINT_LOOKUP,
			_pad: [0; 7],
		};

		let mut any_inconclusive = false;
		for source in &entry.source_replicas {
			if source.id == self.local_node.id {
				continue;
			}
			match self.migrate_request(*source, &req).await {
				Ok((resp, records)) if resp.status == MigrateStatus::OK => {
					for record in &records {
						self.slab
							.append_migration(
								entry.migration_id,
								record.key_hash,
								record.hlc,
								record.payload_hash,
								&record.frame_bytes,
							)
							.await?;
					}
					if let Some(location) = self.slab.lookup_as_of(key_hash, as_of_hlc) {
						let bytes = self.slab.read(location).await?;
						return Ok(PointReadOutcome::Hit(location, bytes));
					}
					// Source responded OK but did not hold a version
					// of the key that satisfies `as_of_hlc`. Under
					// quorum writes, a different replica may still
					// carry the older retained version we need —
					// fall through and try the next candidate. This
					// still counts as an authoritative miss *for
					// this source* — no bit flipped on
					// `any_inconclusive`.
					continue;
				}
				Ok((resp, _)) if resp.status == MigrateStatus::KEY_NOT_FOUND => {
					// Authoritative miss — the source is reachable
					// and affirmatively has no record under the
					// ledger floor.
					continue;
				}
				// REDIRECT / STALE_LEDGER / INTERNAL_ERROR or a
				// transport error. None of these let us conclude
				// the key is absent; record an inconclusive
				// observation and keep trying remaining sources.
				_ => {
					any_inconclusive = true;
					continue;
				}
			}
		}

		if any_inconclusive {
			Ok(PointReadOutcome::Incomplete)
		} else {
			Ok(PointReadOutcome::AuthoritativeMiss)
		}
	}

	/// Paged `RANGE_PAGE` pull covering `[range_start, range_end]`
	/// for a single ledger entry.
	///
	/// **Drain-all semantic.** Under the bifurcated write path
	/// (§data_server write-side) a replica may ack a write only via
	/// QUORUM (Q-of-RF) rather than RF-of-RF. So any one source is
	/// free to be missing recent writes that another source holds —
	/// a single `has_more == 0` response is *not* authoritative for
	/// the full committed history. This method therefore iterates
	/// every entry in `source_replicas`, pages each until its own
	/// `has_more == 0`, and unions their contributions through
	/// [`SlabManager::append_migration`] (idempotent on matching
	/// `(key_hash, hlc, payload_hash)`).
	///
	/// **Failure surface.** If any source fails mid-drain (transport
	/// error or non-OK `MigrateStatus`), the union is by definition
	/// incomplete — the caller must convert this into a visible
	/// non-OK status to the client rather than silently degrading.
	/// The method still advances to later `source_replicas` so a
	/// healthy fallback can contribute bytes in the same pass, but it
	/// returns `Err(io::Error)` at the end if any source failed;
	/// transient retries are the caller's job, not ours.
	async fn pull_migration_range(
		&self,
		entry: &MigrationLedgerEntry,
		range_start: u64,
		range_end: u64,
	) -> std::io::Result<()> {
		if range_start > range_end {
			return Ok(());
		}
		use std::sync::atomic::Ordering;
		let page_max_records = self.page_max_records.load(Ordering::Relaxed);
		let page_max_bytes = self.page_max_bytes.load(Ordering::Relaxed);

		let mut touched_any_source = false;
		let mut saw_source_failure = false;
		for source in &entry.source_replicas {
			if source.id == self.local_node.id {
				continue;
			}
			touched_any_source = true;
			let mut cursor_hash: u64 = 0;
			let mut cursor_hlc: HybridTimestamp = bytemuck::Zeroable::zeroed();
			loop {
				let req = MigrateRequestMeta {
					migration_id: entry.migration_id,
					expected_to_epoch: entry.to_epoch,
					requester: self.local_node,
					range_start,
					range_end,
					migration_hlc_floor: entry.migration_hlc_floor,
					cursor_hash,
					cursor_hlc,
					max_records: page_max_records,
					max_bytes: page_max_bytes,
					kind: MigrateRequestKind::RANGE_PAGE,
					_pad: [0; 7],
				};
				let (resp, records) = match self.migrate_request(*source, &req).await {
					Ok(ok) => ok,
					Err(err) => {
						saw_source_failure = true;
						tracing::warn!(
							migration_id = entry.migration_id,
							range_start,
							range_end,
							source = source.id,
							error = %err,
							"migration pull source transport failed; trying next candidate"
						);
						break;
					}
				};
				if resp.status != MigrateStatus::OK {
					saw_source_failure = true;
					tracing::warn!(
						migration_id = entry.migration_id,
						range_start,
						range_end,
						source = source.id,
						status = resp.status.0,
						"migration pull source returned non-OK status; trying next candidate"
					);
					break;
				}
				for record in &records {
					self.slab
						.append_migration(
							entry.migration_id,
							record.key_hash,
							record.hlc,
							record.payload_hash,
							&record.frame_bytes,
						)
						.await?;
				}
				if resp.has_more == 0 {
					break;
				}
				cursor_hash = resp.next_cursor_hash;
				cursor_hlc = resp.next_cursor_hlc;
			}
		}
		if !touched_any_source && !entry.source_replicas.is_empty() {
			// Every candidate was the local node itself. That is a
			// ledger-shape problem — the control ring would not seed
			// an entry where the only source is the destination —
			// but fail closed rather than proceed with an empty pull.
			return Err(Error::new(
				ErrorKind::Other,
				format!(
					"migration pull: no non-local source_replicas for migration {}",
					entry.migration_id,
				),
			));
		}
		if saw_source_failure {
			return Err(Error::new(
				ErrorKind::Other,
				format!(
					"migration pull incomplete for migration {}: one or more sources failed",
					entry.migration_id
				),
			));
		}
		Ok(())
	}

	/// Query path with lazy-migration merge (Plan 019 adjustment).
	///
	/// Runs the epoch / linearizability gates *before* any remote
	/// migration pull — a query destined for `EPOCH_STALE` or
	/// `E_SAFE_BEHIND` must not force expensive fan-out I/O first.
	/// Only after the gates pass does the method drain each pending
	/// ledger entry whose range overlaps the query window.
	/// A partial or failed drain becomes
	/// [`ReadStatus::MIGRATION_INCOMPLETE`] instead of a silent
	/// local-only answer.
	pub(crate) async fn execute_query_with_migration_merge(
		&self,
		meta: &QueryRequestMeta,
		bytecode: &[u8],
	) -> std::io::Result<(QueryResponseMeta, Option<Vec<u8>>)> {
		let plan = self
			.prepare_query_execution_plan_with_migration(meta, bytecode)
			.await?;
		if plan.response.status != ReadStatus::OK || plan.locations.is_empty() {
			return Ok((plan.response, None));
		}

		let mut filtered = Vec::with_capacity(plan.locations.len());
		for location in plan.locations {
			filtered.push(materialize_query_batch(&self.slab, location, bytecode).await?);
		}

		let schema = filtered[0].schema();
		let concatenated = concat_batches(&schema, &filtered)
			.map_err(|e| Error::new(ErrorKind::InvalidData, format!("WPBC concat: {e}")))?;
		let arrow_bytes = encode_batch(&concatenated)?;
		let response = QueryResponseMeta {
			arrow_len: arrow_bytes.len() as u32,
			..plan.response
		};
		Ok((response, Some(arrow_bytes)))
	}

	async fn prepare_query_execution_plan_with_migration(
		&self,
		meta: &QueryRequestMeta,
		bytecode: &[u8],
	) -> std::io::Result<QueryExecutionPlan> {
		let sm_peek = self.state_machine.state_snapshot().await;

		// Gate #1: epoch staleness. The authoritative epoch is the
		// highest one the SM has observed an E_safe advance for.
		let current_epoch = sm_peek.e_safe_watermarks.keys().copied().max().unwrap_or(0);
		if meta.expected_epoch < current_epoch {
			return Ok(QueryExecutionPlan {
				response: query_error_meta(meta, ReadStatus::EPOCH_STALE, self.local_node),
				locations: Vec::new(),
			});
		}

		// Gate #2: LINEARIZABLE watermark. The scan (and any
		// migration fan-out it would trigger) is only authoritative
		// once the local SM has observed E_safe at-or-past the
		// client's `min_hlc` for the requested epoch.
		if meta.consistency == Consistency::LINEARIZABLE {
			let watermark = sm_peek
				.e_safe_watermarks
				.get(&meta.expected_epoch)
				.copied()
				.unwrap_or(HybridTimestamp {
					physical_ms: 0,
					logical: 0,
					node_id: 0,
				});
			if watermark < meta.min_hlc {
				return Ok(QueryExecutionPlan {
					response: query_error_meta(meta, ReadStatus::E_SAFE_BEHIND, self.local_node),
					locations: Vec::new(),
				});
			}
		}

		let pending: Vec<MigrationLedgerEntry> = sm_peek
			.migrations
			.values()
			.filter(|e| {
				e.logical_owner.id == self.local_node.id
					&& e.state != MigrationState::Completed
					&& e.range_start <= meta.key_range_end
					&& e.range_end >= meta.key_range_start
			})
			.cloned()
			.collect();
		drop(sm_peek);

		for entry in &pending {
			let rs = entry.range_start.max(meta.key_range_start);
			let re = entry.range_end.min(meta.key_range_end);
			if let Err(_e) = self.pull_migration_range(entry, rs, re).await {
				// Any failure in the fan-out means the local slab
				// is *not* an authoritative view of the query
				// window. Surface it explicitly so clients retry
				// rather than downgrading to a possibly-partial
				// local answer.
				return Ok(QueryExecutionPlan {
					response: query_error_meta(
						meta,
						ReadStatus::MIGRATION_INCOMPLETE,
						self.local_node,
					),
					locations: Vec::new(),
				});
			}
		}

		plan_query_execution(
			&self.slab,
			&self.state_machine,
			self.local_node,
			meta,
			bytecode,
		)
		.await
	}

	/// Issue a single `MIGRATE_REQ` to `target` and decode the
	/// response body into `(meta, records)`. Errors bubble the
	/// transport failure; logical status bytes (KEY_NOT_FOUND,
	/// REDIRECT, STALE_LEDGER, …) come back inside `meta` so the
	/// caller can decide whether to retry another source.
	async fn migrate_request(
		&self,
		target: NodeId,
		req: &MigrateRequestMeta,
	) -> std::io::Result<(MigrateResponseMeta, Vec<MigrationRecord>)> {
		let conn = self.factory.get_connection(target).await?;
		let mut stream = conn.open_bi().await?;

		let meta_bytes = bytemuck::bytes_of(req);
		let header = FrameHeader {
			magic: HRFT_MAGIC,
			version: 1,
			msg_type: MsgType::MIGRATE_REQ.0,
			flags: 0,
			stream_id: 0,
			meta_len: meta_bytes.len() as u32,
			arrow_len: 0,
			trailing_len: 0,
		};
		write_frame(&mut *stream, &header, meta_bytes, None, None).await?;
		stream
			.finish()
			.await
			.map_err(|e| Error::new(ErrorKind::BrokenPipe, e))?;

		let resp = read_frame(&mut *stream, &self.pool).await?;
		if resp.header.msg_type != MsgType::MIGRATE_RESP.0 {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"expected MIGRATE_RESP, got msg_type=0x{:02x}",
					resp.header.msg_type
				),
			));
		}
		if resp.meta.len() < std::mem::size_of::<MigrateResponseMeta>() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"MIGRATE_RESP meta too small",
			));
		}
		let resp_meta: MigrateResponseMeta =
			*bytemuck::from_bytes(&resp.meta[..std::mem::size_of::<MigrateResponseMeta>()]);

		let records = match resp.arrow_payload.as_ref() {
			Some(bytes) if !bytes.is_empty() => decode_migration_records(bytes)?,
			_ => Vec::new(),
		};

		Ok((resp_meta, records))
	}

	/// Handle an inbound REPLICATE_REQ frame: decode the POD
	/// metadata, verify the payload hash, append to the local
	/// slab, and build the REPLICATE_RESP frame. Surfaces both
	/// transient and logical failures via the `status` byte rather
	/// than tearing down the HRFT stream — the sender's ack count
	/// is the only signal that matters for quorum.
	async fn handle_replicate(&self, frame: HrftFrame) -> std::io::Result<HrftFrame> {
		if frame.meta.len() < std::mem::size_of::<ReplicaWriteMeta>() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"REPLICATE_REQ meta too small",
			));
		}
		let meta: ReplicaWriteMeta =
			*bytemuck::from_bytes(&frame.meta[..std::mem::size_of::<ReplicaWriteMeta>()]);
		// Peer liveness/drift ingestion (ADR 001, ADR 012). The HLC
		// originator in `commit_hlc.node_id` is another Data Ring
		// member, so its physical_ms feeds directly into the
		// NodeTracker without further validation.
		if let Some(tracker) = &self.tracker {
			let peer = NodeId {
				id: meta.commit_hlc.node_id,
				_reserved: 0,
			};
			tracker.record_hlc(peer, meta.commit_hlc);
		}
		let arrow = frame.arrow_payload.as_ref().ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				"REPLICATE_REQ missing arrow payload",
			)
		})?;

		let arrow_bytes: &[u8] = &arrow[..];
		let (status, ack_level) = if blake3_32(arrow_bytes) != meta.payload_hash {
			(ReplicaWriteStatus::HASH_MISMATCH, ReplicaAckLevel::NONE)
		} else {
			match self
				.slab
				.append_durable(meta.key_hash, meta.commit_hlc, arrow_bytes)
				.await
			{
				Ok(_) => (ReplicaWriteStatus::OK, ReplicaAckLevel::DURABLE),
				Err(_) => (ReplicaWriteStatus::INTERNAL_ERROR, ReplicaAckLevel::NONE),
			}
		};

		let resp = ReplicaWriteRespMeta {
			status,
			ack_level,
			_pad: [0; 6],
		};
		let mut body = self.pool.acquire();
		body.as_mut_vec()
			.extend_from_slice(bytemuck::bytes_of(&resp));
		let header = FrameHeader {
			magic: HRFT_MAGIC,
			version: 1,
			msg_type: MsgType::REPLICATE_RESP.0,
			flags: 0,
			stream_id: frame.header.stream_id,
			meta_len: body.len() as u32,
			arrow_len: 0,
			trailing_len: 0,
		};
		Ok(HrftFrame {
			header,
			meta: body,
			arrow_payload: None,
			trailing: None,
		})
	}
}

// ============================================================
//  DATA RING SERVER (INBOUND DISPATCH)
// ============================================================

#[async_trait]
impl DataRingServer for WendelonDataServer {
	async fn handle_data_frame(&self, frame: HrftFrame) -> std::io::Result<Option<HrftFrame>> {
		let msg_type = MsgType(frame.header.msg_type);
		match msg_type {
			MsgType::WRITE_REQ => self.handle_write(frame).await.map(Some),
			MsgType::READ_REQ => self.handle_read(frame).await.map(Some),
			MsgType::REPLICATE_REQ => self.handle_replicate(frame).await.map(Some),
			MsgType::QUERY_REQ => self.handle_query(frame).await.map(Some),
			MsgType::MIGRATE_REQ => self.handle_migrate(frame).await.map(Some),
			other => Err(Error::new(
				ErrorKind::Unsupported,
				format!(
					"Data Ring handler has no decoder for msg_type=0x{:02x}",
					other.0
				),
			)),
		}
	}

	async fn handle_data_stream(
		&self,
		first_header: FrameHeader,
		stream: Box<dyn TransportStream>,
		_pool: Arc<BufPool>,
	) -> std::io::Result<()> {
		match MsgType(first_header.msg_type) {
			MsgType::BULK_WRITE_REQ => self.handle_bulk_stream(first_header, stream).await,
			MsgType::QUERY_REQ => self.handle_query_stream(first_header, stream).await,
			MsgType::STREAM_FIN_REQ => {
				self.handle_stream_finalization_stream(first_header, stream)
					.await
			}
			other => Err(Error::new(
				ErrorKind::Unsupported,
				format!(
					"Data Ring stream handler has no decoder for msg_type=0x{:02x}",
					other.0
				),
			)),
		}
	}
}

#[async_trait]
impl DataRing for WendelonDataServer {
	async fn write(
		&self,
		meta: WriteRequestMeta,
		payload: RecordBatch,
	) -> std::io::Result<(WriteResponseMeta, Option<FinalizationMarker>)> {
		// 1. Proxy-hop guard — two-hop cap per ADR 003 §2.
		if meta.proxy_hop >= 2 {
			return Ok((
				error_response(&meta, WriteStatus::PROXY_LIMIT, self.local_node),
				None,
			));
		}

		// 2. Epoch validation. The authoritative current epoch is
		// the highest epoch the SM has seen an E_safe advancement
		// for; a client that believes in an older epoch gets a
		// redirect, not a silent accept.
		let sm = self.state_machine.state_snapshot().await;
		let current_epoch = sm.e_safe_watermarks.keys().copied().max().unwrap_or(0);
		if meta.expected_epoch < current_epoch {
			return Ok((
				error_response(&meta, WriteStatus::EPOCH_STALE, self.local_node),
				None,
			));
		}

		// 3. Idempotency fast path.
		let key = (meta.write_id.client_uuid, meta.write_id.sequence_no);
		if let Some(cached) = self.idempotency.get(&key) {
			let mut resp = *cached.value();
			resp.status = WriteStatus::DUPLICATE;
			return Ok((resp, None));
		}

		// 4. Pull the key out of the Arrow payload and hash it.
		let key_bytes = extract_first_binary(&payload, "key")?;
		let key_hash = highway_hash(key_bytes);

		// 4a. Per-range lease gate (plan 018). When enabled, this
		// node accepts the write only if it holds a valid lease
		// covering `key_hash` in the client's epoch. A node that is
		// not the primary for this key returns `NOT_LEADER` so the
		// Smart Client reroutes; a node that is the primary but
		// whose lease has lapsed returns `LEASE_EXPIRED` so the
		// client retries once the manager has renewed.
		if self.lease_gate_enabled {
			let is_primary = self
				.router
				.replicas(key_hash)
				.first()
				.map(|n| n.id == self.local_node.id)
				.unwrap_or(false);
			if !is_primary {
				return Ok((
					error_response(&meta, WriteStatus::NOT_LEADER, self.local_node),
					None,
				));
			}
			match self
				.lease_gate_result(key_hash, meta.expected_epoch, meta.client_hlc)
				.await
			{
				LeaseGateResult::Valid => {}
				LeaseGateResult::Missing => {
					return Ok((
						error_response(&meta, WriteStatus::LEASE_EXPIRED, self.local_node),
						None,
					));
				}
				LeaseGateResult::InvalidSignature => {
					return Ok((
						error_response(
							&meta,
							WriteStatus::INVALID_LEASE_SIGNATURE,
							self.local_node,
						),
						None,
					));
				}
			}
		}

		// 5. Serialize the RecordBatch back to Arrow IPC. These
		// bytes are what both the local slab and every remote
		// replica store verbatim — hashing once here means every
		// ack is computed against the same authoritative blob.
		let frame_bytes = Arc::new(encode_batch(&payload)?);
		let payload_hash = blake3_32(frame_bytes.as_slice());

		// 6. HLC for this write. The leader is authoritative: it
		// merges the client's timestamp with its own monotonic HLC
		// view and stamps `max(leader_hlc, client_hlc) + 1`.
		let commit_hlc = self.merge_commit_hlc(meta.client_hlc);

		// 7. Resolve the replica set. An empty set is a
		// configuration bug — refuse rather than silently dropping
		// to a pre-Stellarator single-node append.
		let replicas = self.router.replicas(key_hash);
		if replicas.is_empty() {
			return Ok((
				error_response(&meta, WriteStatus::INTERNAL_ERROR, self.local_node),
				None,
			));
		}
		let rf = replicas.len();
		let quorum = (rf / 2) + 1;

		// 8. Local append+flush (if this node is in the replica
		// set) and concurrent fan-out to the remaining replicas.
		// Each remote leg runs on its own task so head-of-line
		// blocking on any single peer only slows *that* leg — the
		// quorum race still completes as soon as the first majority
		// of durable acks arrive.
		let mut durable_acks: usize = 0;
		let local_is_replica = replicas.iter().any(|n| n.id == self.local_node.id);
		if local_is_replica {
			// Drive the local durable path first so the leader's
			// own slab is either flushed or the write aborts before
			// we race any remote finalization condition.
			match self
				.slab
				.append_durable(key_hash, commit_hlc, frame_bytes.as_slice())
				.await
			{
				Ok(_) => durable_acks += 1,
				Err(e) => {
					// A local durability failure is terminal — we
					// cannot finalize a write we could not even put
					// on our own NVMe. Propagate rather than falling
					// back to a pure remote quorum, which would make
					// the local index silently diverge.
					return Err(e);
				}
			}
		}

		let replica_meta = ReplicaWriteMeta {
			commit_hlc,
			epoch: meta.expected_epoch,
			key_hash,
			payload_hash,
		};
		let mut join: JoinSet<std::io::Result<ReplicaAckLevel>> = JoinSet::new();
		for target in replicas.iter().copied() {
			if target.id == self.local_node.id {
				continue;
			}
			let factory = self.factory.clone();
			let pool = self.pool.clone();
			let bytes = frame_bytes.clone();
			join.spawn(async move {
				Self::replicate_to_remote(factory, pool, target, replica_meta, bytes).await
			});
		}

		// 9. Quorum race. Stop polling as soon as the majority is
		// in — outstanding tasks keep running to completion so
		// slower replicas still catch up, but we do not block the
		// client on them.
		while durable_acks < quorum {
			match join.join_next().await {
				Some(Ok(Ok(level))) if level == ReplicaAckLevel::DURABLE => durable_acks += 1,
				Some(Ok(Ok(_))) => { /* append-only/non-durable ack */ }
				Some(_) => { /* leg failed — keep racing */ }
				None => break, // no more remote legs in flight
			}
		}

		if durable_acks < quorum {
			return Ok((
				error_response(&meta, WriteStatus::INTERNAL_ERROR, self.local_node),
				None,
			));
		}

		// 10. Finalization. `log_index` / `term` are unknown until
		// Raft places the entry; submit with zeros and fill them
		// from the ClientWriteResponse below. The SM's
		// `apply_request` only reads `epoch` and `commit_hlc`, so
		// the placeholder zeros are safe.
		let mut marker = FinalizationMarker {
			log_index: 0,
			term: 0,
			epoch: meta.expected_epoch,
			commit_hlc,
			payload_hash,
			e_safe_index: 0,
			_reserved: 0,
		};

		let (status, log_index, term) = match self
			.raft
			.client_write(WendelonRequest::Finalize(marker))
			.await
		{
			Ok(resp) => (
				WriteStatus::OK,
				resp.log_id.index,
				resp.log_id.leader_id.term,
			),
			Err(_) => (WriteStatus::INTERNAL_ERROR, 0, 0),
		};
		marker.log_index = log_index;
		marker.term = term;
		marker.e_safe_index = log_index;

		// 11. Response meta, cached for future idempotent replays.
		let response = WriteResponseMeta {
			status,
			_pad1: [0; 7],
			log_index,
			term,
			epoch: meta.expected_epoch,
			commit_hlc,
			leader_hint: self.local_node,
			echoed_write_id: meta.write_id,
			has_finalization: if meta.await_finalization != 0 { 1 } else { 0 },
			_pad2: [0; 7],
		};
		self.idempotency.insert(key, response);

		let trailing = if meta.await_finalization != 0 && status == WriteStatus::OK {
			Some(marker)
		} else {
			None
		};
		Ok((response, trailing))
	}

	async fn read(
		&self,
		meta: ReadRequestMeta,
		key: RecordBatch,
	) -> std::io::Result<(ReadResponseMeta, Option<RecordBatch>)> {
		let sm = self.state_machine.state_snapshot().await;
		let current_epoch = sm.e_safe_watermarks.keys().copied().max().unwrap_or(0);
		if meta.expected_epoch < current_epoch {
			return Ok((
				error_read_response(&meta, ReadStatus::EPOCH_STALE, self.local_node),
				None,
			));
		}

		// 1. Consistency gate. LINEARIZABLE requires the local SM
		// to have observed an E_safe advancement in this epoch at
		// or beyond the client's min_hlc. SERIALIZABLE and STALE
		// skip the gate — the former trusts the leader's commit
		// frontier, the latter explicitly accepts stale data.
		if meta.consistency == Consistency::LINEARIZABLE {
			let watermark = sm
				.e_safe_watermarks
				.get(&meta.expected_epoch)
				.copied()
				.unwrap_or(HybridTimestamp {
					physical_ms: 0,
					logical: 0,
					node_id: 0,
				});
			if watermark < meta.min_hlc {
				return Ok((
					error_read_response(&meta, ReadStatus::E_SAFE_BEHIND, self.local_node),
					None,
				));
			}
		}

		// 2. Hash the key payload and probe the slab index.
		let key_bytes = extract_first_binary(&key, "key")?;
		let key_hash = highway_hash(key_bytes);

		// 2a. Per-range lease gate for LINEARIZABLE reads (plan 018).
		// A local point-read is only authoritative while a lease
		// fences writes for this range to this node — anything
		// weaker opens a window where a stale follower could serve
		// a pre-failover value.
		if self.lease_gate_enabled && meta.consistency == Consistency::LINEARIZABLE {
			let is_primary = self
				.router
				.replicas(key_hash)
				.first()
				.map(|n| n.id == self.local_node.id)
				.unwrap_or(false);
			if !is_primary {
				return Ok((
					error_read_response(&meta, ReadStatus::NOT_LEADER, self.local_node),
					None,
				));
			}
			match self
				.lease_gate_result(key_hash, meta.expected_epoch, meta.min_hlc)
				.await
			{
				LeaseGateResult::Valid => {}
				LeaseGateResult::Missing => {
					return Ok((
						error_read_response(&meta, ReadStatus::LEASE_EXPIRED, self.local_node),
						None,
					));
				}
				LeaseGateResult::InvalidSignature => {
					return Ok((
						error_read_response(
							&meta,
							ReadStatus::INVALID_LEASE_SIGNATURE,
							self.local_node,
						),
						None,
					));
				}
			}
		}

		let (location, bytes) = match self.slab.lookup_as_of(key_hash, meta.as_of_hlc) {
			Some(loc) => {
				let bytes = self.slab.read(loc).await?;
				(loc, bytes)
			}
			None => {
				// Plan 019 read-path fallback: a local miss on a
				// range we now logically own means the bytes may
				// still live on a prior physical holder. The
				// ledger tells us whether to pull or to surface
				// the miss.
				match self
					.try_migrate_point_read(key_hash, meta.as_of_hlc)
					.await?
				{
					PointReadOutcome::Hit(loc, bytes) => (loc, bytes),
					PointReadOutcome::AuthoritativeMiss => {
						return Ok((
							error_read_response(&meta, ReadStatus::KEY_NOT_FOUND, self.local_node),
							None,
						));
					}
					PointReadOutcome::Incomplete => {
						return Ok((
							error_read_response(
								&meta,
								ReadStatus::MIGRATION_INCOMPLETE,
								self.local_node,
							),
							None,
						));
					}
				}
			}
		};

		// 3. Re-materialise the stored Arrow bytes into a
		// RecordBatch. We then project out just the `value`
		// column, shaping the response to `read_value_schema`.
		let stored = decode_batch(&bytes)?;
		let value_bytes = extract_first_binary(&stored, "value")?;
		let value_batch = build_value_batch(value_bytes)?;
		let value_len = value_bytes.len() as u32;

		let response = ReadResponseMeta {
			status: ReadStatus::OK,
			_pad1: [0; 7],
			value_hlc: location.hlc,
			read_at_index: 0,
			epoch: meta.expected_epoch,
			leader_hint: self.local_node,
			value_len,
			_pad2: [0; 4],
		};
		Ok((response, Some(value_batch)))
	}

	async fn stream_finalization(
		&self,
		start_log_index: u64,
		epoch: u64,
	) -> std::io::Result<mpsc::Receiver<FinalizationMarker>> {
		let mut live = self.state_machine.subscribe_finalizations();
		let applied_upto = self
			.state_machine
			.last_applied_log_index()
			.await
			.unwrap_or(start_log_index);
		let replay = self
			.log_store
			.finalization_markers_after(start_log_index, applied_upto, epoch)
			.await?;
		let current_epoch = self
			.state_machine
			.state_snapshot()
			.await
			.e_safe_watermarks
			.keys()
			.copied()
			.max()
			.unwrap_or(0);
		let (tx, rx) = mpsc::channel(STREAM_FINALIZATION_BUFFER);
		tokio::spawn(async move {
			let mut last_sent = start_log_index;
			for marker in replay {
				if marker.log_index <= last_sent {
					continue;
				}
				if tx.try_send(marker).is_err() {
					tracing::info!(
						start_log_index,
						epoch,
						"stream_finalization subscriber dropped during replay backlog"
					);
					return;
				}
				last_sent = marker.log_index;
			}

			if epoch < current_epoch {
				return;
			}

			loop {
				match live.recv().await {
					Ok(marker) if marker.epoch == epoch && marker.log_index > last_sent => {
						if tx.try_send(marker).is_err() {
							tracing::info!(
								start_log_index,
								epoch,
								last_sent,
								"stream_finalization subscriber dropped after backlog overflow"
							);
							break;
						}
						last_sent = marker.log_index;
					}
					Ok(_) => {}
					Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
						tracing::info!(
							start_log_index,
							epoch,
							last_sent,
							skipped,
							"stream_finalization subscriber lagged and must reconnect"
						);
						break;
					}
					Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
				}
			}
		});
		Ok(rx)
	}

	async fn bulk_write(
		&self,
		mut requests: mpsc::Receiver<(WriteRequestMeta, RecordBatch)>,
	) -> std::io::Result<mpsc::Receiver<WriteResponseMeta>> {
		let server = self.clone();
		let (tx, rx) = mpsc::channel(16);
		tokio::spawn(async move {
			while let Some((meta, payload)) = requests.recv().await {
				let response = match server.write(meta, payload).await {
					Ok((resp, _)) => resp,
					Err(_) => error_response(&meta, WriteStatus::INTERNAL_ERROR, server.local_node),
				};
				if tx.send(response).await.is_err() {
					break;
				}
			}
		});
		Ok(rx)
	}
}

// ============================================================
//  QUERY EXECUTION — INDEPENDENT OF TRANSPORT
// ============================================================

#[derive(Clone, Debug)]
struct QueryExecutionPlan {
	response: QueryResponseMeta,
	locations: Vec<RecordLocation>,
}

/// Plan a [`QueryRequestMeta`] + WPBC program against a local
/// [`SlabManager`] without concatenating the full result set.
///
/// The returned plan contains the stable set of slab locations to
/// revisit during response streaming plus the final response meta
/// fields that are independent of Arrow framing. The caller is free
/// to stream each location's filtered batch separately or to
/// concatenate them for unary compatibility.
async fn plan_query_execution(
	slab: &SlabManager,
	state_machine: &WendelonStateMachine,
	local_node: NodeId,
	meta: &QueryRequestMeta,
	bytecode: &[u8],
) -> std::io::Result<QueryExecutionPlan> {
	// 1. Epoch staleness. The authoritative epoch is the highest
	// one the SM has observed an E_safe advance for; a client
	// believing in a past epoch gets a redirect so it can refresh
	// topology rather than fork the read.
	let sm = state_machine.state_snapshot().await;
	let current_epoch = sm.e_safe_watermarks.keys().copied().max().unwrap_or(0);
	if meta.expected_epoch < current_epoch {
		return Ok(QueryExecutionPlan {
			response: query_error_meta(meta, ReadStatus::EPOCH_STALE, local_node),
			locations: Vec::new(),
		});
	}

	// 2. LINEARIZABLE gate — the scan is only authoritative once
	// the local SM has observed E_safe at-or-past the client's
	// min_hlc for the requested epoch.
	if meta.consistency == Consistency::LINEARIZABLE {
		let watermark = sm
			.e_safe_watermarks
			.get(&meta.expected_epoch)
			.copied()
			.unwrap_or(HybridTimestamp {
				physical_ms: 0,
				logical: 0,
				node_id: 0,
			});
		if watermark < meta.min_hlc {
			return Ok(QueryExecutionPlan {
				response: query_error_meta(meta, ReadStatus::E_SAFE_BEHIND, local_node),
				locations: Vec::new(),
			});
		}
	}

	// 3. Compile WPBC once so per-row evaluation skips repeated
	// decoding when the scan hits many records.
	// `wpbc::execute` already does this internally per call, but
	// we mirror the pattern so a follow-up that batches rows can
	// lift the compile out of the loop without touching semantics.
	let scanned = slab.scan_as_of(meta.key_range_start, meta.key_range_end, meta.as_of_hlc);
	let mut included = Vec::new();
	let mut rows = 0u64;
	let mut max_hlc = HybridTimestamp {
		physical_ms: 0,
		logical: 0,
		node_id: 0,
	};
	for (_key_hash, location) in scanned {
		let result = materialize_query_batch(slab, location, bytecode).await?;
		if result.num_rows() == 0 {
			continue;
		}
		if location.hlc > max_hlc {
			max_hlc = location.hlc;
		}
		rows += result.num_rows() as u64;
		included.push(location);
	}

	// 4. Empty result is a legitimate answer. Clients map
	// KEY_NOT_FOUND to an empty batch on the receive side, so we
	// avoid the cost of encoding a zero-row stream.
	if included.is_empty() {
		return Ok(QueryExecutionPlan {
			response: query_error_meta(meta, ReadStatus::KEY_NOT_FOUND, local_node),
			locations: Vec::new(),
		});
	}

	Ok(QueryExecutionPlan {
		response: QueryResponseMeta {
			status: ReadStatus::OK,
			_pad1: [0; 7],
			value_hlc: max_hlc,
			epoch: meta.expected_epoch,
			leader_hint: local_node,
			rows,
			arrow_len: 0,
			_pad2: [0; 4],
		},
		locations: included,
	})
}

async fn materialize_query_batch(
	slab: &SlabManager,
	location: RecordLocation,
	bytecode: &[u8],
) -> std::io::Result<RecordBatch> {
	let bytes = slab.read(location).await?;
	let batch = decode_batch(&bytes)?;
	wpbc::execute(bytecode, &batch)
}

#[cfg_attr(not(test), allow(dead_code))]
/// Execute a [`QueryRequestMeta`] + WPBC program against a local
/// [`SlabManager`]. Returns the POD response meta plus the
/// concatenated Arrow IPC bytes when `status == OK`.
///
/// Split out from the frame-handler so tests can drive the full
/// query path without standing up an HRFT server or an openraft
/// instance. The streaming server path builds on the same plan and
/// per-location materialization helpers.
pub(crate) async fn execute_query_plan(
	slab: &SlabManager,
	state_machine: &WendelonStateMachine,
	local_node: NodeId,
	meta: &QueryRequestMeta,
	bytecode: &[u8],
) -> std::io::Result<(QueryResponseMeta, Option<Vec<u8>>)> {
	let plan = plan_query_execution(slab, state_machine, local_node, meta, bytecode).await?;
	if plan.response.status != ReadStatus::OK || plan.locations.is_empty() {
		return Ok((plan.response, None));
	}

	let mut filtered = Vec::with_capacity(plan.locations.len());
	for location in plan.locations {
		filtered.push(materialize_query_batch(slab, location, bytecode).await?);
	}

	let schema = filtered[0].schema();
	let concatenated = concat_batches(&schema, &filtered)
		.map_err(|e| Error::new(ErrorKind::InvalidData, format!("WPBC concat: {e}")))?;
	let arrow_bytes = encode_batch(&concatenated)?;
	let response = QueryResponseMeta {
		arrow_len: arrow_bytes.len() as u32,
		..plan.response
	};
	Ok((response, Some(arrow_bytes)))
}

/// Sentinel zero-valued [`NodeId`]. [`NodeId`] doesn't implement
/// `Default`, so the migration code uses this constant whenever
/// the wire schema calls for a "not set" node — e.g.,
/// `source_hint` on non-`REDIRECT` responses.
const ZERO_NODE: NodeId = NodeId {
	id: 0,
	_reserved: 0,
};

/// One decoded row out of the migration Arrow IPC body.
#[derive(Clone, Debug)]
struct MigrationRecord {
	key_hash: u64,
	hlc: HybridTimestamp,
	payload_hash: [u8; 32],
	frame_bytes: Vec<u8>,
}

/// Find a ledger entry pending on the local node whose hash
/// range contains `key_hash`. Returns a clone so the caller can
/// drop the `SmState` snapshot without extending its borrow.
fn pending_owned_entry(
	migrations: &std::collections::HashMap<u64, MigrationLedgerEntry>,
	key_hash: u64,
	local_node: NodeId,
) -> Option<MigrationLedgerEntry> {
	migrations
		.values()
		.find(|entry| {
			entry.logical_owner.id == local_node.id
				&& entry.state != MigrationState::Completed
				&& entry.range_start <= key_hash
				&& key_hash <= entry.range_end
		})
		.cloned()
}

/// Build the zero-body error / redirect response. `status` and
/// `source_hint` are the only interesting fields; everything else
/// echoes the request.
fn migrate_error_meta(
	req: &MigrateRequestMeta,
	status: MigrateStatus,
	source_hint: NodeId,
) -> MigrateResponseMeta {
	MigrateResponseMeta {
		status,
		has_more: 0,
		_pad0: [0; 6],
		migration_id: req.migration_id,
		source_hint,
		next_cursor_hash: 0,
		next_cursor_hlc: bytemuck::Zeroable::zeroed(),
		versions_returned: 0,
		bytes_returned: 0,
		_pad1: [0; 4],
	}
}

/// Pack `(key_hash, RecordLocation, frame_bytes)` rows into an
/// Arrow IPC stream over [`migration_record_schema`]. `payload_hash`
/// is computed here — receivers use it to fast-path
/// `append_migration` idempotency without re-hashing themselves.
fn encode_migration_records(rows: &[(u64, RecordLocation, Vec<u8>)]) -> std::io::Result<Vec<u8>> {
	let mut key_hash = UInt64Builder::with_capacity(rows.len());
	let mut hlc_phys = UInt64Builder::with_capacity(rows.len());
	let mut hlc_log = UInt32Builder::with_capacity(rows.len());
	let mut hlc_node = UInt32Builder::with_capacity(rows.len());
	let mut payload = FixedSizeBinaryBuilder::with_capacity(rows.len(), 32);
	let mut frame = LargeBinaryBuilder::new();

	for (k, loc, bytes) in rows {
		key_hash.append_value(*k);
		hlc_phys.append_value(loc.hlc.physical_ms);
		hlc_log.append_value(loc.hlc.logical);
		hlc_node.append_value(loc.hlc.node_id);
		let hash: [u8; 32] = *blake3::hash(bytes).as_bytes();
		payload
			.append_value(hash)
			.map_err(|e| Error::new(ErrorKind::InvalidData, format!("fsb append: {e}")))?;
		frame.append_value(bytes);
	}

	let schema = Arc::new(migration_record_schema());
	let batch = RecordBatch::try_new(
		schema.clone(),
		vec![
			Arc::new(key_hash.finish()),
			Arc::new(hlc_phys.finish()),
			Arc::new(hlc_log.finish()),
			Arc::new(hlc_node.finish()),
			Arc::new(payload.finish()),
			Arc::new(frame.finish()),
		],
	)
	.map_err(|e| Error::new(ErrorKind::InvalidData, format!("migration batch: {e}")))?;

	let mut buf = Vec::new();
	{
		let mut writer = StreamWriter::try_new(&mut buf, &schema)
			.map_err(|e| Error::new(ErrorKind::InvalidData, format!("arrow init: {e}")))?;
		writer
			.write(&batch)
			.map_err(|e| Error::new(ErrorKind::InvalidData, format!("arrow write: {e}")))?;
		writer
			.finish()
			.map_err(|e| Error::new(ErrorKind::InvalidData, format!("arrow finish: {e}")))?;
	}
	Ok(buf)
}

/// Inverse of [`encode_migration_records`]. Expects an Arrow IPC
/// stream over [`migration_record_schema`]; any column mismatch or
/// missing value surfaces as `InvalidData`.
fn decode_migration_records(bytes: &[u8]) -> std::io::Result<Vec<MigrationRecord>> {
	let mut reader = StreamReader::try_new(Cursor::new(bytes), None)
		.map_err(|e| Error::new(ErrorKind::InvalidData, format!("migrate stream open: {e}")))?;
	let mut out = Vec::new();
	while let Some(batch) = reader.next() {
		let batch = batch
			.map_err(|e| Error::new(ErrorKind::InvalidData, format!("migrate stream read: {e}")))?;
		let key_col: &UInt64Array = batch
			.column(0)
			.as_any()
			.downcast_ref()
			.ok_or_else(|| Error::new(ErrorKind::InvalidData, "key_hash col type"))?;
		let phys_col: &UInt64Array = batch
			.column(1)
			.as_any()
			.downcast_ref()
			.ok_or_else(|| Error::new(ErrorKind::InvalidData, "hlc_physical_ms col type"))?;
		let log_col: &UInt32Array = batch
			.column(2)
			.as_any()
			.downcast_ref()
			.ok_or_else(|| Error::new(ErrorKind::InvalidData, "hlc_logical col type"))?;
		let node_col: &UInt32Array = batch
			.column(3)
			.as_any()
			.downcast_ref()
			.ok_or_else(|| Error::new(ErrorKind::InvalidData, "hlc_node_id col type"))?;
		let hash_col: &FixedSizeBinaryArray = batch
			.column(4)
			.as_any()
			.downcast_ref()
			.ok_or_else(|| Error::new(ErrorKind::InvalidData, "payload_hash col type"))?;
		let frame_col: &LargeBinaryArray = batch
			.column(5)
			.as_any()
			.downcast_ref()
			.ok_or_else(|| Error::new(ErrorKind::InvalidData, "frame_bytes col type"))?;

		for i in 0..batch.num_rows() {
			let hash_slice = hash_col.value(i);
			if hash_slice.len() != 32 {
				return Err(Error::new(ErrorKind::InvalidData, "payload_hash width"));
			}
			let mut payload_hash = [0u8; 32];
			payload_hash.copy_from_slice(hash_slice);
			out.push(MigrationRecord {
				key_hash: key_col.value(i),
				hlc: HybridTimestamp {
					physical_ms: phys_col.value(i),
					logical: log_col.value(i),
					node_id: node_col.value(i),
				},
				payload_hash,
				frame_bytes: frame_col.value(i).to_vec(),
			});
		}
	}
	Ok(out)
}

fn query_error_meta(
	meta: &QueryRequestMeta,
	status: ReadStatus,
	leader_hint: NodeId,
) -> QueryResponseMeta {
	QueryResponseMeta {
		status,
		_pad1: [0; 7],
		value_hlc: HybridTimestamp {
			physical_ms: 0,
			logical: 0,
			node_id: 0,
		},
		epoch: meta.expected_epoch,
		leader_hint,
		rows: 0,
		arrow_len: 0,
		_pad2: [0; 4],
	}
}

// ============================================================
//  ERROR RESPONSE HELPERS
// ============================================================

fn error_response(
	meta: &WriteRequestMeta,
	status: WriteStatus,
	leader_hint: NodeId,
) -> WriteResponseMeta {
	WriteResponseMeta {
		status,
		_pad1: [0; 7],
		log_index: 0,
		term: 0,
		epoch: meta.expected_epoch,
		commit_hlc: meta.client_hlc,
		leader_hint,
		echoed_write_id: meta.write_id,
		has_finalization: 0,
		_pad2: [0; 7],
	}
}

fn error_read_response(
	meta: &ReadRequestMeta,
	status: ReadStatus,
	leader_hint: NodeId,
) -> ReadResponseMeta {
	ReadResponseMeta {
		status,
		_pad1: [0; 7],
		value_hlc: HybridTimestamp {
			physical_ms: 0,
			logical: 0,
			node_id: 0,
		},
		read_at_index: 0,
		epoch: meta.expected_epoch,
		leader_hint,
		value_len: 0,
		_pad2: [0; 4],
	}
}

fn validate_write_request_batch(
	meta: &WriteRequestMeta,
	batch: &RecordBatch,
) -> std::io::Result<()> {
	if batch.schema().as_ref() != &write_payload_schema() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"WRITE_REQ Arrow payload schema mismatch",
		));
	}
	let key_len = extract_first_binary(batch, "key")?.len() as u32;
	if key_len != meta.key_len {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"WRITE_REQ key_len mismatch: meta={} actual={}",
				meta.key_len, key_len
			),
		));
	}
	let value_len = extract_first_binary(batch, "value")?.len() as u32;
	if value_len != meta.value_len {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"WRITE_REQ value_len mismatch: meta={} actual={}",
				meta.value_len, value_len
			),
		));
	}
	Ok(())
}

fn validate_read_request_batch(meta: &ReadRequestMeta, batch: &RecordBatch) -> std::io::Result<()> {
	if batch.schema().as_ref() != &read_key_schema() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"READ_REQ Arrow payload schema mismatch",
		));
	}
	let key_len = extract_first_binary(batch, "key")?.len() as u32;
	if key_len != meta.key_len {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"READ_REQ key_len mismatch: meta={} actual={}",
				meta.key_len, key_len
			),
		));
	}
	Ok(())
}

// ============================================================
//  ARROW / HASH HELPERS
// ============================================================

/// Read the first value of a named `LargeBinary` column from a
/// single-row RecordBatch. Both `write_payload_schema` and
/// `read_key_schema` are single-row / LargeBinary-shaped, so this
/// covers every Data Ring request variant today.
fn extract_first_binary<'a>(
	batch: &'a RecordBatch,
	column: &'static str,
) -> std::io::Result<&'a [u8]> {
	let col = batch
		.column_by_name(column)
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, format!("missing '{column}' column")))?;
	let arr = col
		.as_any()
		.downcast_ref::<LargeBinaryArray>()
		.ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				format!("'{column}' column is not LargeBinary"),
			)
		})?;
	if arr.is_empty() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!("'{column}' column is empty"),
		));
	}
	Ok(arr.value(0))
}

/// Encode a RecordBatch as an Arrow IPC stream.
fn encode_batch(batch: &RecordBatch) -> std::io::Result<Vec<u8>> {
	let mut buf = Vec::new();
	{
		let mut writer = StreamWriter::try_new(&mut buf, &batch.schema())
			.map_err(|e| Error::new(ErrorKind::InvalidData, format!("arrow stream init: {e}")))?;
		writer
			.write(batch)
			.map_err(|e| Error::new(ErrorKind::InvalidData, format!("arrow stream write: {e}")))?;
		writer
			.finish()
			.map_err(|e| Error::new(ErrorKind::InvalidData, format!("arrow stream finish: {e}")))?;
	}
	Ok(buf)
}

/// Inverse of [`encode_batch`]. Returns the first batch in the
/// stream — Data Ring payloads are always single-batch today.
fn decode_batch(bytes: &[u8]) -> std::io::Result<RecordBatch> {
	let mut reader = StreamReader::try_new(Cursor::new(bytes), None)
		.map_err(|e| Error::new(ErrorKind::InvalidData, format!("arrow stream open: {e}")))?;
	match reader.next() {
		Some(Ok(batch)) => Ok(batch),
		Some(Err(e)) => Err(Error::new(
			ErrorKind::InvalidData,
			format!("arrow stream read: {e}"),
		)),
		None => Err(Error::new(
			ErrorKind::InvalidData,
			"arrow stream contained no record batch",
		)),
	}
}

/// Wrap raw value bytes in a one-row RecordBatch shaped to
/// `read_value_schema` so callers speak the same dialect on write
/// and read paths.
fn build_value_batch(value: &[u8]) -> std::io::Result<RecordBatch> {
	let mut builder = LargeBinaryBuilder::new();
	builder.append_value(value);
	let array = builder.finish();
	let schema = Arc::new(Schema::new(vec![arrow::datatypes::Field::new(
		"value",
		arrow::datatypes::DataType::LargeBinary,
		true,
	)]));
	RecordBatch::try_new(schema, vec![Arc::new(array)])
		.map_err(|e| Error::new(ErrorKind::InvalidData, format!("build value batch: {e}")))
}

fn highway_hash(bytes: &[u8]) -> u64 {
	let mut hasher = HighwayHasher::new(HIGHWAY_KEY);
	hasher.append(bytes);
	hasher.finalize64()
}

fn blake3_32(bytes: &[u8]) -> [u8; 32] {
	*blake3::hash(bytes).as_bytes()
}

// ============================================================
//  TESTS
// ============================================================

#[cfg(test)]
mod tests {
	use super::*;
	use crate::schema::wpbc::{OP_AND, OP_GTE, OP_PROJECT, OP_PUSH_COL, OP_PUSH_INT};
	use crate::schema::write_payload_schema;
	use crate::storage::core::StorageEngine;
	use crate::storage::io_uring::spawn_io_thread;
	use crate::storage::manager::SlabManagerConfig;

	fn n(id: u32) -> NodeId {
		NodeId { id, _reserved: 0 }
	}

	fn fresh_dir(tag: &str) -> std::path::PathBuf {
		let dir = std::env::temp_dir().join(format!(
			"wendelon-data-server-{tag}-{}-{}",
			std::process::id(),
			uuid::Uuid::new_v4()
		));
		std::fs::create_dir_all(&dir).unwrap();
		dir
	}

	fn hlc(physical_ms: u64, logical: u32, node_id: u32) -> HybridTimestamp {
		HybridTimestamp {
			physical_ms,
			logical,
			node_id,
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

	async fn make_manager() -> Arc<SlabManager> {
		let dir = fresh_dir("query");
		let io = spawn_io_thread(64).unwrap();
		Arc::new(
			SlabManager::open(SlabManagerConfig {
				dir,
				max_slab_bytes: 1 << 30,
				io,
				l2: None,
			})
			.unwrap(),
		)
	}

	async fn seed_record(
		mgr: &SlabManager,
		key_hash: u64,
		hlc: HybridTimestamp,
		key: &[u8],
		value: &[u8],
	) {
		let batch = kv_batch(key, value);
		let bytes = encode_batch(&batch).unwrap();
		mgr.append(key_hash, hlc, &bytes).await.unwrap();
	}

	fn base_meta() -> QueryRequestMeta {
		QueryRequestMeta {
			min_hlc: hlc(0, 0, 0),
			expected_epoch: 0,
			consistency: Consistency::SERIALIZABLE,
			_pad: [0; 7],
			key_range_start: 0,
			key_range_end: u64::MAX,
			bytecode_len: 0,
			_pad2: [0; 4],
			as_of_hlc: hlc(0, 0, 0),
		}
	}

	#[test]
	fn static_router_wraps_around_ring() {
		let router = StaticReplicaRouter::new(vec![n(1), n(2), n(3), n(4)], 3);
		let replicas = router.replicas(3);
		assert_eq!(replicas.len(), 3);
		assert_eq!(replicas[0], n(4));
		assert_eq!(replicas[1], n(1));
		assert_eq!(replicas[2], n(2));
	}

	#[test]
	fn static_router_dedups_and_sorts() {
		let router = StaticReplicaRouter::new(vec![n(3), n(1), n(3), n(2)], 5);
		let replicas = router.replicas(0);
		assert_eq!(replicas.len(), 3);
		assert_eq!(replicas[0], n(1));
		assert_eq!(replicas[1], n(2));
		assert_eq!(replicas[2], n(3));
	}

	#[test]
	fn static_router_rf_one_is_single_replica() {
		let router = StaticReplicaRouter::new(vec![n(1), n(2), n(3)], 1);
		assert_eq!(router.replicas(0).len(), 1);
	}

	#[test]
	fn static_router_empty_node_list_returns_empty() {
		let router = StaticReplicaRouter::new(vec![], 3);
		assert!(router.replicas(0).is_empty());
	}

	#[tokio::test]
	async fn scan_collects_entries_in_inclusive_range() {
		let mgr = make_manager().await;
		seed_record(&mgr, 10, hlc(1, 0, 1), b"k10", b"v10").await;
		seed_record(&mgr, 50, hlc(2, 0, 1), b"k50", b"v50").await;
		seed_record(&mgr, 99, hlc(3, 0, 1), b"k99", b"v99").await;

		let hits = mgr.scan(10, 50);
		let keys: Vec<u64> = hits.iter().map(|(k, _)| *k).collect();
		assert_eq!(keys, vec![10, 50]);

		let full = mgr.scan(0, u64::MAX);
		assert_eq!(full.len(), 3);

		let miss = mgr.scan(60, 90);
		assert!(miss.is_empty());

		// Inverted range → empty, never panics.
		let inverted = mgr.scan(100, 5);
		assert!(inverted.is_empty());
	}

	#[tokio::test]
	async fn query_passthrough_returns_all_rows_in_range() {
		let mgr = make_manager().await;
		seed_record(&mgr, 1, hlc(1, 0, 1), b"a", b"v1").await;
		seed_record(&mgr, 2, hlc(2, 0, 1), b"b", b"v2").await;
		seed_record(&mgr, 3, hlc(3, 0, 1), b"c", b"v3").await;

		let sm = WendelonStateMachine::new();
		let (resp, arrow) = execute_query_plan(&mgr, &sm, n(1), &base_meta(), &[])
			.await
			.unwrap();
		assert_eq!(resp.status, ReadStatus::OK);
		assert_eq!(resp.rows, 3);
		assert_eq!(resp.value_hlc, hlc(3, 0, 1));
		let bytes = arrow.expect("OK must carry arrow bytes");
		let batch = decode_batch(&bytes).unwrap();
		assert_eq!(batch.num_rows(), 3);
	}

	#[tokio::test]
	async fn query_range_filters_out_of_window_rows() {
		let mgr = make_manager().await;
		seed_record(&mgr, 5, hlc(1, 0, 1), b"a", b"va").await;
		seed_record(&mgr, 42, hlc(2, 0, 1), b"b", b"vb").await;
		seed_record(&mgr, 100, hlc(3, 0, 1), b"c", b"vc").await;

		let sm = WendelonStateMachine::new();
		let mut meta = base_meta();
		meta.key_range_start = 10;
		meta.key_range_end = 50;
		let (resp, _) = execute_query_plan(&mgr, &sm, n(1), &meta, &[])
			.await
			.unwrap();
		assert_eq!(resp.status, ReadStatus::OK);
		assert_eq!(resp.rows, 1);
	}

	#[tokio::test]
	async fn query_empty_scan_returns_key_not_found() {
		let mgr = make_manager().await;
		let sm = WendelonStateMachine::new();
		let (resp, arrow) = execute_query_plan(&mgr, &sm, n(1), &base_meta(), &[])
			.await
			.unwrap();
		assert_eq!(resp.status, ReadStatus::KEY_NOT_FOUND);
		assert_eq!(resp.rows, 0);
		assert_eq!(resp.arrow_len, 0);
		assert!(arrow.is_none());
	}

	#[tokio::test]
	async fn linearizable_gate_fires_when_watermark_behind() {
		let mgr = make_manager().await;
		seed_record(&mgr, 1, hlc(1, 0, 1), b"k", b"v").await;
		let sm = WendelonStateMachine::new();
		// Watermark remains at default (0,0,0); client demands (5,0,0).
		let mut meta = base_meta();
		meta.consistency = Consistency::LINEARIZABLE;
		meta.min_hlc = hlc(5, 0, 0);
		let (resp, arrow) = execute_query_plan(&mgr, &sm, n(1), &meta, &[])
			.await
			.unwrap();
		assert_eq!(resp.status, ReadStatus::E_SAFE_BEHIND);
		assert_eq!(resp.leader_hint, n(1));
		assert!(arrow.is_none());
	}

	#[tokio::test]
	async fn linearizable_gate_passes_once_watermark_catches_up() {
		let mgr = make_manager().await;
		seed_record(&mgr, 1, hlc(1, 0, 1), b"k", b"v").await;
		let sm = WendelonStateMachine::new();
		sm.seed_e_safe_for_test(0, hlc(10, 0, 1)).await;
		let mut meta = base_meta();
		meta.consistency = Consistency::LINEARIZABLE;
		meta.min_hlc = hlc(5, 0, 1);
		let (resp, _) = execute_query_plan(&mgr, &sm, n(1), &meta, &[])
			.await
			.unwrap();
		assert_eq!(resp.status, ReadStatus::OK);
		assert_eq!(resp.rows, 1);
	}

	#[tokio::test]
	async fn epoch_stale_returns_redirect() {
		let mgr = make_manager().await;
		seed_record(&mgr, 1, hlc(1, 0, 1), b"k", b"v").await;
		let sm = WendelonStateMachine::new();
		// The SM has advanced to epoch 7; client still thinks we
		// are on epoch 3 → EPOCH_STALE, not a silent scan.
		sm.seed_e_safe_for_test(7, hlc(1, 0, 1)).await;
		let mut meta = base_meta();
		meta.expected_epoch = 3;
		let (resp, _) = execute_query_plan(&mgr, &sm, n(1), &meta, &[])
			.await
			.unwrap();
		assert_eq!(resp.status, ReadStatus::EPOCH_STALE);
		assert_eq!(resp.leader_hint, n(1));
	}

	#[tokio::test]
	async fn wpbc_filter_drops_rows_below_threshold() {
		// Stored records carry an Int64 `age` column so WPBC can
		// actually compare against a literal — the write-path
		// schema is LargeBinary and WPBC does not support bytes,
		// so tests that exercise filter opcodes use their own
		// schema via direct slab.append.
		use arrow::array::Int64Array;
		use arrow::datatypes::{DataType, Field, Schema};
		let schema = Arc::new(Schema::new(vec![Field::new("age", DataType::Int64, false)]));
		let mgr = make_manager().await;
		for (key_hash, age) in [(1u64, 17i64), (2, 22), (3, 40)] {
			let ages = Int64Array::from(vec![age]);
			let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(ages)]).unwrap();
			let bytes = encode_batch(&batch).unwrap();
			mgr.append(key_hash, hlc(age as u64, 0, 1), &bytes)
				.await
				.unwrap();
		}

		// age >= 18 AND age >= 18 (duplicated to exercise OP_AND)
		let mut bytecode = Vec::new();
		bytecode.push(OP_PUSH_COL);
		bytecode.extend_from_slice(&0u32.to_le_bytes());
		bytecode.push(OP_PUSH_INT);
		bytecode.extend_from_slice(&18i64.to_le_bytes());
		bytecode.push(OP_GTE);
		bytecode.push(OP_PUSH_COL);
		bytecode.extend_from_slice(&0u32.to_le_bytes());
		bytecode.push(OP_PUSH_INT);
		bytecode.extend_from_slice(&18i64.to_le_bytes());
		bytecode.push(OP_GTE);
		bytecode.push(OP_AND);

		let sm = WendelonStateMachine::new();
		let mut meta = base_meta();
		meta.bytecode_len = bytecode.len() as u32;
		let (resp, _) = execute_query_plan(&mgr, &sm, n(1), &meta, &bytecode)
			.await
			.unwrap();
		assert_eq!(resp.status, ReadStatus::OK);
		// Rows 2 (age 22) and 3 (age 40) survive; row 1 (age 17) drops.
		assert_eq!(resp.rows, 2);
	}

	#[tokio::test]
	async fn wpbc_projection_narrows_schema() {
		let mgr = make_manager().await;
		seed_record(&mgr, 1, hlc(1, 0, 1), b"a", b"va").await;
		seed_record(&mgr, 2, hlc(2, 0, 1), b"b", b"vb").await;
		// PROJECT column 0 (the `key` column) only — WPBC compiles
		// a terminal PROJECT with no preceding filter, so every row
		// is kept and only the `key` column survives.
		let mut bytecode = Vec::new();
		bytecode.push(OP_PROJECT);
		bytecode.extend_from_slice(&1u16.to_le_bytes());
		bytecode.extend_from_slice(&0u32.to_le_bytes());

		let sm = WendelonStateMachine::new();
		let mut meta = base_meta();
		meta.bytecode_len = bytecode.len() as u32;
		let (resp, arrow) = execute_query_plan(&mgr, &sm, n(1), &meta, &bytecode)
			.await
			.unwrap();
		assert_eq!(resp.status, ReadStatus::OK);
		assert_eq!(resp.rows, 2);
		let batch = decode_batch(&arrow.unwrap()).unwrap();
		assert_eq!(batch.num_columns(), 1);
		assert_eq!(batch.schema().field(0).name(), "key");
	}

	// ========================================================
	//  LEASE GATE TESTS (plan 018)
	//
	//  These tests drive the Data Ring lease gate end-to-end by
	//  standing up a single-node cluster via
	//  `WendelonNode::bootstrap_single_node`. The gate logic is
	//  exercised against the real `WendelonDataServer` from that
	//  bootstrap (the node pubs it on the struct).
	// ========================================================

	use crate::node::{NodeBootstrapConfig, WendelonNode};
	use crate::schema::{
		ClientWriteId, Consistency as SchemaConsistency, DurabilityLevel, WriteRequestMeta,
	};

	fn bootstrap_tempdir(tag: &str) -> std::path::PathBuf {
		let dir = std::env::temp_dir().join(format!(
			"wendelon-lease-{tag}-{}-{}",
			std::process::id(),
			uuid::Uuid::new_v4()
		));
		std::fs::create_dir_all(&dir).unwrap();
		dir
	}

	fn simple_write_meta(epoch: u64) -> WriteRequestMeta {
		WriteRequestMeta {
			write_id: ClientWriteId {
				client_uuid: [7u8; 16],
				sequence_no: 1,
				idempotency_ttl_ms: 60_000,
				_pad: 0,
			},
			client_hlc: hlc(10, 0, 0),
			expected_epoch: epoch,
			proxy_hop: 0,
			durability: DurabilityLevel::QUORUM,
			await_finalization: 0,
			_pad1: [0; 5],
			proxy_source: NodeId {
				id: 0,
				_reserved: 0,
			},
			key_len: 1,
			value_len: 1,
		}
	}

	fn single_kv_batch() -> RecordBatch {
		let schema = Arc::new(write_payload_schema());
		let mut keys = LargeBinaryBuilder::new();
		keys.append_value(b"k");
		let mut values = LargeBinaryBuilder::new();
		values.append_value(b"v");
		RecordBatch::try_new(
			schema,
			vec![Arc::new(keys.finish()), Arc::new(values.finish())],
		)
		.unwrap()
	}

	fn simple_read_meta(epoch: u64) -> ReadRequestMeta {
		ReadRequestMeta {
			min_hlc: hlc(0, 0, 0),
			expected_epoch: epoch,
			consistency: SchemaConsistency::LINEARIZABLE,
			_pad: [0; 3],
			key_len: 1,
			_pad2: [0; 8],
			as_of_hlc: hlc(0, 0, 0),
		}
	}

	fn single_key_batch() -> RecordBatch {
		let schema = Arc::new(crate::schema::read_key_schema());
		let mut keys = LargeBinaryBuilder::new();
		keys.append_value(b"k");
		RecordBatch::try_new(schema, vec![Arc::new(keys.finish())]).unwrap()
	}

	#[tokio::test]
	async fn write_rejected_when_lease_gate_on_and_cache_empty() {
		let dir = bootstrap_tempdir("empty-cache");
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			11,
			dir,
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();
		// Force the cache empty so the gate can't find a lease.
		node.data_server.lease_cache().write().unwrap().clear();

		let (resp, _) = node
			.data_server
			.write(simple_write_meta(1), single_kv_batch())
			.await
			.unwrap();
		assert_eq!(resp.status, WriteStatus::LEASE_EXPIRED);
		assert_eq!(resp.leader_hint, node.local_node);
	}

	#[tokio::test]
	async fn linearizable_read_rejected_when_lease_missing() {
		let dir = bootstrap_tempdir("lin-read");
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			12,
			dir,
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();
		node.data_server.lease_cache().write().unwrap().clear();

		let (resp, _) = node
			.data_server
			.read(simple_read_meta(1), single_key_batch())
			.await
			.unwrap();
		assert_eq!(resp.status, ReadStatus::LEASE_EXPIRED);
	}

	#[tokio::test]
	async fn refresh_leases_once_clears_stale_epoch_entries() {
		let dir = bootstrap_tempdir("stale-epoch");
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			13,
			dir,
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();
		// Seed a lease from a prior epoch — the cache must drop it
		// on the next refresh tick because the current ring epoch
		// is 1, not 99.
		node.data_server
			.seed_lease_for_test(crate::schema::LeaseDescriptor {
				epoch: 99,
				holder: node.local_node,
				range_start: 0,
				range_end: u64::MAX,
				lease_start: hlc(0, 0, node.local_node.id),
				lease_expiry: hlc(u64::MAX / 2, 0, node.local_node.id),
				min_commit_index: 0,
				issuer: node.local_node,
				signature: [0; 64],
			});
		// Give Raft a moment to elect and ring updater to seed.
		for _ in 0..64 {
			tokio::task::yield_now().await;
			if node.raft.current_leader().await == Some(node.local_node.id) {
				break;
			}
		}
		let granted = node
			.data_server
			.refresh_leases_once(&node.ring, &*node.clock, 60_000)
			.await;
		// Exactly one primary range on a single-node cluster, so one grant.
		assert!(granted >= 1);
		let cache = node.data_server.lease_cache();
		let entries = cache.read().unwrap().clone();
		assert!(
			entries.iter().all(|l| l.epoch == 1),
			"stale-epoch leases must be evicted; cache={entries:?}"
		);
		assert!(entries.iter().any(|l| l.holder.id == node.local_node.id));
	}

	#[tokio::test]
	async fn write_accepted_once_lease_manager_has_granted() {
		let dir = bootstrap_tempdir("accept");
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			14,
			dir,
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();
		// Wait for Raft leadership and drive one explicit refresh
		// so the cache is guaranteed to be warm before the write
		// arrives — the spawned manager is on a 1s tick so tests
		// that race against it would be flaky.
		for _ in 0..64 {
			tokio::task::yield_now().await;
			if node.raft.current_leader().await == Some(node.local_node.id) {
				break;
			}
		}
		node.data_server
			.refresh_leases_once(&node.ring, &*node.clock, 60_000)
			.await;

		let mut meta = simple_write_meta(1);
		// Push client_hlc into the granted window (the manager used
		// `clock.now_ms()` for lease_start; any value at-or-after 0
		// and before expiry is fine).
		meta.client_hlc = hlc(1, 0, 0);
		let (resp, _) = node
			.data_server
			.write(meta, single_kv_batch())
			.await
			.unwrap();
		assert_eq!(resp.status, WriteStatus::OK, "got resp={resp:?}");
	}

	// ========================================================
	//  MIGRATION HELPERS (plan 019)
	// ========================================================

	#[test]
	fn pending_owned_entry_finds_matching_range_owned_by_self() {
		use crate::consensus::{MigrationLedgerEntry, MigrationState};
		use std::collections::HashMap;

		let local = n(7);
		let entry = MigrationLedgerEntry {
			migration_id: 1,
			range_start: 100,
			range_end: 200,
			from_epoch: 1,
			to_epoch: 2,
			logical_owner: local,
			upstream_migration_id: None,
			source_replicas: vec![n(1), n(2)],
			migration_hlc_floor: hlc(0, 0, 0),
			state: MigrationState::Pending,
			completed_by: None,
			completed_at_hlc: None,
		};
		let mut migrations = HashMap::new();
		migrations.insert(1, entry);

		// In-range, owned: hit.
		assert!(pending_owned_entry(&migrations, 150, local).is_some());
		// In-range, owned by someone else: miss.
		assert!(pending_owned_entry(&migrations, 150, n(9)).is_none());
		// Out of range: miss.
		assert!(pending_owned_entry(&migrations, 50, local).is_none());
	}

	#[test]
	fn pending_owned_entry_skips_completed_entries() {
		use crate::consensus::{MigrationLedgerEntry, MigrationState};
		use std::collections::HashMap;

		let local = n(7);
		let entry = MigrationLedgerEntry {
			migration_id: 1,
			range_start: 0,
			range_end: 100,
			from_epoch: 1,
			to_epoch: 2,
			logical_owner: local,
			upstream_migration_id: None,
			source_replicas: vec![n(1)],
			migration_hlc_floor: hlc(0, 0, 0),
			state: MigrationState::Completed,
			completed_by: Some(local),
			completed_at_hlc: Some(hlc(10, 0, 0)),
		};
		let mut migrations = HashMap::new();
		migrations.insert(1, entry);

		// Completed entries are not pull candidates — the caller
		// must treat the local miss as a real not-found rather
		// than re-issuing a MIGRATE_REQ against a drained source.
		assert!(pending_owned_entry(&migrations, 50, local).is_none());
	}

	/// Multi-Page Fat Key (Plan 019 adjustment #2).
	///
	/// A single key carries 100 retained versions. Paging with
	/// `max_records = 10` must surface every version exactly once
	/// across ten pages: the `(cursor_hash, cursor_hlc)` cursor is
	/// what lets the source resume *mid-key* instead of skipping to
	/// the next key on page boundaries.
	#[tokio::test]
	async fn migrate_range_page_streams_every_version_of_fat_key() {
		let mgr = make_manager().await;
		let key: u64 = 42;
		let floor = hlc(0, 0, 0);
		for i in 0..100u32 {
			let v = hlc(i as u64 + 1, 0, 1);
			seed_record(&mgr, key, v, b"k", format!("v{i}").as_bytes()).await;
		}

		// Emulate the paging driver exactly the way `handle_migrate`
		// does: scan, truncate at `max_records`, emit
		// `(next_cursor_hash, next_cursor_hlc)` as the last row's
		// `(key, hlc)`, loop until `has_more == 0`.
		let mut collected: Vec<HybridTimestamp> = Vec::new();
		let mut cursor_hash: u64 = 0;
		let mut cursor_hlc: HybridTimestamp = hlc(0, 0, 0);
		let max_records: usize = 10;
		for _ in 0..15 {
			let locations = mgr.scan_all_versions_from(key, key, floor, cursor_hash, cursor_hlc);
			if locations.is_empty() {
				break;
			}
			let take = locations.len().min(max_records);
			for (h, loc) in locations.iter().take(take) {
				assert_eq!(*h, key);
				collected.push(loc.hlc);
			}
			let truncated = locations.len() > max_records;
			if !truncated {
				break;
			}
			let (last_h, last_loc) = locations[take - 1];
			cursor_hash = last_h;
			cursor_hlc = last_loc.hlc;
		}

		assert_eq!(collected.len(), 100, "every version must cross the wire");
		// HLC is monotonic ascending across the stream.
		for w in collected.windows(2) {
			assert!(w[0] < w[1], "versions must emerge in HLC-ascending order");
		}
		// No duplicates.
		let mut unique = collected.clone();
		unique.sort();
		unique.dedup();
		assert_eq!(unique.len(), 100, "no version may be emitted twice");
	}

	/// Quorum Miss Fallback (Plan 019 adjustment #3).
	///
	/// Exercises the observable inputs to the exhaustive fallback
	/// loop: a source that simply missed the original replication
	/// returns `KEY_NOT_FOUND` for a POINT_LOOKUP, and a source that
	/// holds the key returns a record the destination can ingest
	/// via `append_migration`. After walking both the "empty" and
	/// the "populated" source in ledger order, the destination's
	/// local index carries the key.
	#[tokio::test]
	async fn migrate_point_read_falls_through_missing_source_to_populated_one() {
		let source_empty = make_manager().await;
		let source_full = make_manager().await;
		let destination = make_manager().await;
		let key: u64 = 7;
		let v = hlc(100, 0, 1);
		// Only `source_full` holds the key.
		seed_record(&source_full, key, v, b"k7", b"v7").await;

		let floor = hlc(0, 0, 0);
		// Mirror the inputs `serve_migrate_ok` sees for POINT_LOOKUP:
		// an empty scan on `source_empty` is what becomes
		// `KEY_NOT_FOUND` on the wire, telling the caller to try the
		// next candidate.
		let miss = source_empty.scan_all_versions(key, key, floor);
		assert!(miss.is_empty(), "empty source must not surface a row");

		// Next candidate in the ledger's `source_replicas` list
		// actually has the record. The destination ingests it via
		// the same `append_migration` path the real loop uses.
		let hits = source_full.scan_all_versions(key, key, floor);
		assert_eq!(hits.len(), 1);
		let (h, loc) = hits[0];
		let frame = source_full.read(loc).await.unwrap();
		let payload_hash = *blake3::hash(&frame).as_bytes();
		destination
			.append_migration(1, h, loc.hlc, payload_hash, &frame)
			.await
			.unwrap();

		let found = destination.lookup_as_of(key, v);
		assert!(
			found.is_some(),
			"destination must resolve the key after falling through to a populated source"
		);
	}

	/// Dirty Range Scan (Plan 019 adjustment #1).
	///
	/// Half of the query window lives locally (already migrated or
	/// natively written); the other half lives only on a remote
	/// source. The merge path drains the source via
	/// `scan_all_versions_from` + `append_migration` and a final
	/// `scan_as_of` on the destination then sees the union.
	#[tokio::test]
	async fn query_merge_over_partially_migrated_range_sees_every_row() {
		let destination = make_manager().await;
		let source = make_manager().await;
		let floor = hlc(0, 0, 0);
		let v = hlc(50, 0, 1);

		// Locally-present half: keys 0..50 already on `destination`.
		for k in 0..50u64 {
			seed_record(&destination, k, v, b"k", b"v-local").await;
		}
		// Remote-only half: keys 50..100 live only on the `source`.
		for k in 50..100u64 {
			seed_record(&source, k, v, b"k", b"v-remote").await;
		}

		// Drain the full ledger range from the source into the
		// destination, mimicking `pull_migration_range` without the
		// network. Every record goes through `append_migration`, the
		// idempotent ingest boundary — so a rerun would be a no-op.
		let pulled = source.scan_all_versions_from(0, 99, floor, 0, floor);
		for (h, loc) in pulled {
			let frame = source.read(loc).await.unwrap();
			let payload_hash = *blake3::hash(&frame).as_bytes();
			destination
				.append_migration(1, h, loc.hlc, payload_hash, &frame)
				.await
				.unwrap();
		}

		// A window scan on the destination now sees the union.
		let merged = destination.scan_as_of(0, 99, v);
		let mut keys: Vec<u64> = merged.iter().map(|(k, _)| *k).collect();
		keys.sort();
		keys.dedup();
		assert_eq!(
			keys.len(),
			100,
			"merged scan must cover 0..=99 exactly once"
		);
		assert_eq!(*keys.first().unwrap(), 0);
		assert_eq!(*keys.last().unwrap(), 99);
	}

	/// Regression for High #1: the drain-all semantic.
	///
	/// Under quorum writes a single source can hold only a partial
	/// subset of the committed history. The pull loop must visit
	/// every source and union their contributions — not return on
	/// the first `has_more == 0`.
	///
	/// Two sources each hold a disjoint half of the range; the
	/// destination must see both halves after iterating through the
	/// full candidate list. `append_migration`'s idempotency means
	/// a later source that re-emits a key already ingested from an
	/// earlier source is a no-op, which keeps the drain-all policy
	/// correct rather than double-counting.
	#[tokio::test]
	async fn drain_all_sources_unions_disjoint_halves() {
		let destination = make_manager().await;
		let source_a = make_manager().await;
		let source_b = make_manager().await;
		let floor = hlc(0, 0, 0);
		let v = hlc(10, 0, 1);

		for k in 0..50u64 {
			seed_record(&source_a, k, v, b"k", b"v-a").await;
		}
		for k in 50..100u64 {
			seed_record(&source_b, k, v, b"k", b"v-b").await;
		}

		// Simulate pull_migration_range's drain-all behavior: walk
		// *every* candidate in source_replicas, not just the first.
		for src in [&source_a, &source_b] {
			let rows = src.scan_all_versions_from(0, 99, floor, 0, floor);
			for (h, loc) in rows {
				let frame = src.read(loc).await.unwrap();
				let payload_hash = *blake3::hash(&frame).as_bytes();
				destination
					.append_migration(1, h, loc.hlc, payload_hash, &frame)
					.await
					.unwrap();
			}
		}

		let merged = destination.scan_as_of(0, 99, v);
		let mut keys: Vec<u64> = merged.iter().map(|(k, _)| *k).collect();
		keys.sort();
		keys.dedup();
		assert_eq!(keys.len(), 100, "union must cover both halves");
		assert!(keys.contains(&0));
		assert!(keys.contains(&49));
		assert!(keys.contains(&50));
		assert!(keys.contains(&99));
	}

	/// Regression for High #2: MIGRATION_INCOMPLETE surfaces when the
	/// pull fan-out cannot complete. A ledger entry overlapping the
	/// query window names a source that cannot be reached; the
	/// merge path must fail closed with a visible non-OK status
	/// rather than downgrading to a local-only answer.
	#[tokio::test]
	async fn query_merge_surfaces_migration_incomplete_on_source_failure() {
		use crate::consensus::{MigrationLedgerEntry, MigrationState};

		let dir = bootstrap_tempdir("merge-incomplete");
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			21,
			dir,
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();

		// Seed a ledger entry whose only source is a node id the
		// transport factory does not know about — any MIGRATE_REQ
		// will fail fast and bubble through pull_migration_range.
		let entry = MigrationLedgerEntry {
			migration_id: 99,
			range_start: 0,
			range_end: u64::MAX,
			from_epoch: 0,
			to_epoch: 1,
			logical_owner: node.local_node,
			upstream_migration_id: None,
			source_replicas: vec![NodeId {
				id: 9999,
				_reserved: 0,
			}],
			migration_hlc_floor: hlc(0, 0, 0),
			state: MigrationState::Pending,
			completed_by: None,
			completed_at_hlc: None,
		};
		node.state_machine.seed_migration_for_test(entry).await;

		let (resp, arrow) = node
			.data_server
			.execute_query_with_migration_merge(&base_meta(), &[])
			.await
			.unwrap();

		assert_eq!(
			resp.status,
			ReadStatus::MIGRATION_INCOMPLETE,
			"unreachable source must surface MIGRATION_INCOMPLETE, not a silent OK"
		);
		assert!(arrow.is_none());
	}

	/// Regression for Medium #1: after an OK response that ingested
	/// records but none satisfies `as_of_hlc`, the fallback must
	/// continue to the next source. A later replica under quorum
	/// writes can still carry the older retained version the read
	/// needs. The directly testable observable is that
	/// `lookup_as_of` remains a miss after a newer-only ingest —
	/// which is exactly why the loop must not terminate there.
	#[tokio::test]
	async fn lookup_as_of_miss_after_newer_only_ingest_proves_fallthrough_needed() {
		let destination = make_manager().await;
		let key: u64 = 13;
		let newer = hlc(200, 0, 1);
		let older = hlc(50, 0, 1);

		// First "source" yields only a newer version (> as_of_hlc).
		let frame_newer = {
			let batch = kv_batch(b"k", b"v-new");
			encode_batch(&batch).unwrap()
		};
		let payload_newer = *blake3::hash(&frame_newer).as_bytes();
		destination
			.append_migration(1, key, newer, payload_newer, &frame_newer)
			.await
			.unwrap();

		// Client wants a version ≤ older.physical_ms; newer-only
		// ingest cannot satisfy the read.
		assert!(
			destination.lookup_as_of(key, older).is_none(),
			"newer-only ingest must not be treated as authoritative for older as_of"
		);

		// Second "source" yields the older version; after falling
		// through, the lookup succeeds.
		let frame_older = {
			let batch = kv_batch(b"k", b"v-old");
			encode_batch(&batch).unwrap()
		};
		let payload_older = *blake3::hash(&frame_older).as_bytes();
		destination
			.append_migration(1, key, older, payload_older, &frame_older)
			.await
			.unwrap();
		assert!(
			destination.lookup_as_of(key, older).is_some(),
			"after ingesting from a second source, the as_of read resolves"
		);
	}

	/// Regression for Round 3 High #1: on a local point-read miss
	/// against a pending ledger entry whose every source is
	/// non-authoritative (transport error / REDIRECT / STALE_LEDGER
	/// / INTERNAL_ERROR), the read path must surface
	/// `MIGRATION_INCOMPLETE` rather than collapsing to
	/// `KEY_NOT_FOUND`. A false negative here is client-visible
	/// ground truth — the client cannot distinguish "key does not
	/// exist" from "we could not prove it one way or the other",
	/// so an unreachable source must be a retryable status.
	#[tokio::test]
	async fn point_read_surfaces_migration_incomplete_on_source_failure() {
		use crate::consensus::{MigrationLedgerEntry, MigrationState};

		let dir = bootstrap_tempdir("point-incomplete");
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			23,
			dir,
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();

		// Seed a ledger entry covering the entire hash range whose
		// sole source is an unknown node id — the transport factory
		// cannot dial it, so every candidate produces an
		// inconclusive (non-authoritative) failure.
		let entry = MigrationLedgerEntry {
			migration_id: 101,
			range_start: 0,
			range_end: u64::MAX,
			from_epoch: 0,
			to_epoch: 1,
			logical_owner: node.local_node,
			upstream_migration_id: None,
			source_replicas: vec![NodeId {
				id: 9999,
				_reserved: 0,
			}],
			migration_hlc_floor: hlc(0, 0, 0),
			state: MigrationState::Pending,
			completed_by: None,
			completed_at_hlc: None,
		};
		node.state_machine.seed_migration_for_test(entry).await;

		// SERIALIZABLE bypasses the LINEARIZABLE lease gate so the
		// fallback path actually runs. Local slab is empty → miss →
		// try_migrate_point_read → all sources inconclusive →
		// PointReadOutcome::Incomplete → MIGRATION_INCOMPLETE.
		let mut meta = simple_read_meta(1);
		meta.consistency = SchemaConsistency::SERIALIZABLE;
		let (resp, arrow) = node
			.data_server
			.read(meta, single_key_batch())
			.await
			.unwrap();

		assert_eq!(
			resp.status,
			ReadStatus::MIGRATION_INCOMPLETE,
			"unreachable source must surface MIGRATION_INCOMPLETE, not KEY_NOT_FOUND"
		);
		assert!(arrow.is_none());
	}

	/// Regression for Medium #2: a query rejected by the epoch /
	/// linearizability gate must not force remote migration I/O.
	/// If the gate check fired *after* the pull, a
	/// MIGRATION_INCOMPLETE from the unreachable source would mask
	/// the real reason (EPOCH_STALE). Ordering the gate first means
	/// the expensive path is skipped entirely.
	#[tokio::test]
	async fn epoch_stale_gate_short_circuits_before_migration_pull() {
		use crate::consensus::{MigrationLedgerEntry, MigrationState};

		let dir = bootstrap_tempdir("gate-first");
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			22,
			dir,
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();

		// Advance current_epoch so a request at epoch 0 is stale.
		node.state_machine
			.seed_e_safe_for_test(5, hlc(1, 0, 1))
			.await;

		// Seed a ledger entry that *would* fail the merge pull if
		// the gate didn't fire first — its only source is an
		// unknown node id.
		let entry = MigrationLedgerEntry {
			migration_id: 77,
			range_start: 0,
			range_end: u64::MAX,
			from_epoch: 4,
			to_epoch: 5,
			logical_owner: node.local_node,
			upstream_migration_id: None,
			source_replicas: vec![NodeId {
				id: 9999,
				_reserved: 0,
			}],
			migration_hlc_floor: hlc(0, 0, 0),
			state: MigrationState::Pending,
			completed_by: None,
			completed_at_hlc: None,
		};
		node.state_machine.seed_migration_for_test(entry).await;

		// Client believes it's still on epoch 0 — stale.
		let mut meta = base_meta();
		meta.expected_epoch = 0;
		let (resp, _) = node
			.data_server
			.execute_query_with_migration_merge(&meta, &[])
			.await
			.unwrap();

		assert_eq!(
			resp.status,
			ReadStatus::EPOCH_STALE,
			"gate must fire before any pull; otherwise an unreachable source would mask this as MIGRATION_INCOMPLETE"
		);
	}

	#[tokio::test]
	async fn migration_record_roundtrip_preserves_every_field() {
		// Encode → decode must round-trip every column so the
		// receiver's `append_migration` gets the same (key_hash,
		// hlc, payload_hash, frame_bytes) tuple the sender read
		// off disk.
		let rows = vec![
			(
				7u64,
				crate::storage::core::RecordLocation {
					slab_id: 0,
					offset: 0,
					length: 0,
					hlc: hlc(10, 2, 3),
				},
				b"frame-one".to_vec(),
			),
			(
				42u64,
				crate::storage::core::RecordLocation {
					slab_id: 0,
					offset: 0,
					length: 0,
					hlc: hlc(20, 5, 3),
				},
				b"frame-two".to_vec(),
			),
		];
		let bytes = encode_migration_records(&rows).unwrap();
		let decoded = decode_migration_records(&bytes).unwrap();
		assert_eq!(decoded.len(), 2);
		assert_eq!(decoded[0].key_hash, 7);
		assert_eq!(decoded[0].hlc, hlc(10, 2, 3));
		assert_eq!(decoded[0].frame_bytes, b"frame-one");
		assert_eq!(
			decoded[0].payload_hash,
			*blake3::hash(b"frame-one").as_bytes()
		);
		assert_eq!(decoded[1].key_hash, 42);
		assert_eq!(decoded[1].frame_bytes, b"frame-two");
	}

	// ========================================================
	//  LOOPBACK HRFT HARNESS (drain-all integration)
	//
	//  In-process transport that plugs into the real
	//  `TransportProviderFactory` so the composition path —
	//  `pull_migration_range` → `migrate_request` →
	//  `MsgRouter::handle_frame` → `handle_data_frame` →
	//  `handle_migrate` → `encode_migration_records` — is
	//  exercised without QUIC. `open_bi` returns one end of a
	//  `tokio::io::duplex` pipe; a spawned task reads the
	//  request frame, optionally intercepts it via the
	//  `fail_after_pages` knob, and dispatches the rest to the
	//  registered `HrftService`. Per-target counters let a
	//  mid-drain failure target source B's *second* page after
	//  its first page succeeds (the reviewer's non-blocking
	//  refinement).
	// ========================================================

	use crate::consensus::{MigrationLedgerEntry, MigrationState};
	use crate::network::router::MsgRouter;
	use crate::network::server::HrftService;
	use crate::network::transport::{
		MembershipEpoch, NodeRole, NodeRoleSet, PeerCapabilities, TransportConnection,
		TransportProvider, TransportStream, TransportTier, TransportTierSet,
	};
	use async_trait::async_trait;
	use dashmap::DashMap as LoopbackDashMap;
	use std::collections::HashMap as StdHashMap;
	use std::pin::Pin;
	use std::sync::atomic::{AtomicI32, Ordering as LoopbackOrdering};
	use std::task::{Context as TaskContext, Poll};
	use tokio::io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf};
	use tokio::sync::{Mutex as TokioMutex, watch};

	/// Registry of per-target HRFT services + fault knobs. The
	/// `LoopbackProvider` and every `LoopbackConnection` share this
	/// via `Arc`, so one fault-injection call affects every
	/// subsequent dispatch.
	struct LoopbackHandlers {
		services: LoopbackDashMap<u32, Arc<dyn HrftService>>,
		/// Per-target remaining successful pages before the next
		/// `MIGRATE_REQ` is intercepted with `INTERNAL_ERROR`.
		/// Missing entry = unlimited. Counter decrements before
		/// dispatch; `<= 0` means "fail this page now".
		fail_after: LoopbackDashMap<u32, AtomicI32>,
		/// Per-target count of `MIGRATE_REQ` frames observed by the
		/// loopback. Incremented before fault-injection consults
		/// `fail_after`, so it counts every page attempt (including
		/// the one that gets intercepted with INTERNAL_ERROR).
		/// Tests use this to prove paging actually round-tripped
		/// rather than collapsing to a single oversized response.
		page_count: LoopbackDashMap<u32, AtomicI32>,
		pool: Arc<BufPool>,
	}

	impl LoopbackHandlers {
		fn new(pool: Arc<BufPool>) -> Arc<Self> {
			Arc::new(Self {
				services: LoopbackDashMap::new(),
				fail_after: LoopbackDashMap::new(),
				page_count: LoopbackDashMap::new(),
				pool,
			})
		}

		fn register(&self, node: NodeId, svc: Arc<dyn HrftService>) {
			self.services.insert(node.id, svc);
		}

		/// After `successful_pages` OK responses, the next
		/// `MIGRATE_REQ` to `target` is intercepted and answered
		/// with `INTERNAL_ERROR`. Subsequent pages keep failing —
		/// once the counter is saturated negative, every request
		/// stays in the error branch.
		fn fail_source_after_pages(&self, target: NodeId, successful_pages: u32) {
			self.fail_after
				.insert(target.id, AtomicI32::new(successful_pages as i32));
		}

		/// Number of `MIGRATE_REQ` frames the loopback has observed
		/// for `target`. Used by paging tests to prove that the
		/// page-cap override actually forced multiple round-trips
		/// — without this, a regression that ignored the cap and
		/// returned every record in one page would still satisfy
		/// the coverage assertion.
		fn page_count_for(&self, target: NodeId) -> i32 {
			self.page_count
				.get(&target.id)
				.map(|e| e.load(LoopbackOrdering::SeqCst))
				.unwrap_or(0)
		}
	}

	/// `TransportStream` newtype over `tokio::io::DuplexStream`.
	/// `DuplexStream` is `Unpin`, so the delegation is straight
	/// `Pin::new(&mut self.inner)`.
	struct DuplexTransportStream {
		inner: DuplexStream,
	}

	impl AsyncRead for DuplexTransportStream {
		fn poll_read(
			mut self: Pin<&mut Self>,
			cx: &mut TaskContext<'_>,
			buf: &mut ReadBuf<'_>,
		) -> Poll<std::io::Result<()>> {
			Pin::new(&mut self.inner).poll_read(cx, buf)
		}
	}

	impl AsyncWrite for DuplexTransportStream {
		fn poll_write(
			mut self: Pin<&mut Self>,
			cx: &mut TaskContext<'_>,
			buf: &[u8],
		) -> Poll<std::io::Result<usize>> {
			Pin::new(&mut self.inner).poll_write(cx, buf)
		}
		fn poll_flush(
			mut self: Pin<&mut Self>,
			cx: &mut TaskContext<'_>,
		) -> Poll<std::io::Result<()>> {
			Pin::new(&mut self.inner).poll_flush(cx)
		}
		fn poll_shutdown(
			mut self: Pin<&mut Self>,
			cx: &mut TaskContext<'_>,
		) -> Poll<std::io::Result<()>> {
			Pin::new(&mut self.inner).poll_shutdown(cx)
		}
	}

	#[async_trait]
	impl TransportStream for DuplexTransportStream {
		fn stream_id(&self) -> u64 {
			0
		}
		async fn finish(&mut self) -> std::io::Result<()> {
			use tokio::io::AsyncWriteExt;
			self.inner.shutdown().await
		}
	}

	struct LoopbackConnection {
		remote: NodeId,
		tier: TransportTier,
		handlers: Arc<LoopbackHandlers>,
	}

	#[async_trait]
	impl TransportConnection for LoopbackConnection {
		async fn open_bi(&self) -> std::io::Result<Box<dyn TransportStream>> {
			let (client_side, server_side) = tokio::io::duplex(64 * 1024);
			let handlers = self.handlers.clone();
			let remote = self.remote;
			tokio::spawn(serve_one_frame(server_side, handlers, remote));
			Ok(Box::new(DuplexTransportStream { inner: client_side }))
		}
		async fn accept_bi(&self) -> std::io::Result<Box<dyn TransportStream>> {
			unreachable!("loopback connections never accept server-initiated streams")
		}
		fn remote_id(&self) -> NodeId {
			self.remote
		}
		fn transport_type(&self) -> TransportTier {
			self.tier
		}
		fn is_closed(&self) -> bool {
			false
		}
	}

	async fn serve_one_frame(
		mut stream: DuplexStream,
		handlers: Arc<LoopbackHandlers>,
		remote: NodeId,
	) {
		use tokio::io::AsyncWriteExt;
		let pool = handlers.pool.clone();
		let frame = match read_frame(&mut stream, &pool).await {
			Ok(f) => f,
			Err(_) => return,
		};

		// Fault injection: on a `MIGRATE_REQ`, consume one page of
		// the per-target allowance. Once exhausted (counter <= 0
		// before this request), craft an INTERNAL_ERROR
		// MIGRATE_RESP instead of dispatching — models a source
		// that fails mid-drain after the first page succeeded.
		if frame.header.msg_type == MsgType::MIGRATE_REQ.0 {
			// Count every page attempt (including the one we are
			// about to fail). Tests assert on this to prove paging
			// round-tripped rather than collapsing to one response.
			handlers
				.page_count
				.entry(remote.id)
				.or_insert_with(|| AtomicI32::new(0))
				.fetch_add(1, LoopbackOrdering::SeqCst);

			if let Some(entry) = handlers.fail_after.get(&remote.id) {
				let prev = entry.fetch_sub(1, LoopbackOrdering::SeqCst);
				if prev <= 0 {
					if let Some(resp) = craft_internal_error_response(&frame, &pool) {
						let _ = write_frame(
							&mut stream,
							&resp.header,
							&resp.meta,
							resp.arrow_payload.as_deref(),
							None,
						)
						.await;
					}
					let _ = stream.shutdown().await;
					return;
				}
			}
		}

		let svc = match handlers.services.get(&remote.id).map(|e| e.clone()) {
			Some(s) => s,
			None => return,
		};
		let response = match svc.handle_frame(frame).await {
			Ok(Some(r)) => r,
			_ => return,
		};
		let _ = write_frame(
			&mut stream,
			&response.header,
			&response.meta,
			response.arrow_payload.as_deref(),
			response.trailing.as_deref(),
		)
		.await;
		let _ = stream.shutdown().await;
	}

	fn craft_internal_error_response(frame: &HrftFrame, pool: &Arc<BufPool>) -> Option<HrftFrame> {
		if frame.meta.len() < std::mem::size_of::<MigrateRequestMeta>() {
			return None;
		}
		let req_meta: MigrateRequestMeta =
			*bytemuck::from_bytes(&frame.meta[..std::mem::size_of::<MigrateRequestMeta>()]);
		let resp_meta = migrate_error_meta(&req_meta, MigrateStatus::INTERNAL_ERROR, ZERO_NODE);
		let mut body = pool.acquire();
		body.as_mut_vec()
			.extend_from_slice(bytemuck::bytes_of(&resp_meta));
		Some(HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::MIGRATE_RESP.0,
				flags: 0,
				stream_id: frame.header.stream_id,
				meta_len: body.len() as u32,
				arrow_len: 0,
				trailing_len: 0,
			},
			meta: body,
			arrow_payload: None,
			trailing: None,
		})
	}

	struct LoopbackProvider {
		handlers: Arc<LoopbackHandlers>,
		tier: TransportTier,
	}

	#[async_trait]
	impl TransportProvider for LoopbackProvider {
		async fn connect(
			&self,
			target: NodeId,
			_address: &str,
		) -> std::io::Result<Arc<dyn TransportConnection>> {
			if !self.handlers.services.contains_key(&target.id) {
				return Err(Error::new(
					ErrorKind::NotFound,
					format!("loopback: no service registered for {:?}", target),
				));
			}
			Ok(Arc::new(LoopbackConnection {
				remote: target,
				tier: self.tier,
				handlers: self.handlers.clone(),
			}))
		}
	}

	struct FixedReplicaRouter {
		routes: StdHashMap<u64, Vec<NodeId>>,
		default: Vec<NodeId>,
	}

	impl FixedReplicaRouter {
		fn new(default: Vec<NodeId>) -> Self {
			Self {
				routes: StdHashMap::new(),
				default,
			}
		}

		fn with_route(mut self, key_hash: u64, replicas: Vec<NodeId>) -> Self {
			self.routes.insert(key_hash, replicas);
			self
		}
	}

	impl ReplicaRouter for FixedReplicaRouter {
		fn replicas(&self, key_hash: u64) -> Vec<NodeId> {
			self.routes
				.get(&key_hash)
				.cloned()
				.unwrap_or_else(|| self.default.clone())
		}

		fn replication_factor(&self) -> usize {
			self.routes
				.values()
				.map(|replicas| replicas.len())
				.chain(std::iter::once(self.default.len()))
				.max()
				.unwrap_or(1)
		}
	}

	struct CrashyReplicaState {
		manager: Arc<SlabManager>,
		volatile_cutoffs: StdHashMap<u64, u64>,
	}

	/// Replica harness that appends to a real slab but deliberately
	/// omits the durability barrier. Tests can then "crash" it by
	/// truncating every recorded append-only tail and reopening the
	/// manager from disk, which deterministically models a crash
	/// between append and flush.
	struct CrashyReplicaService {
		dir: std::path::PathBuf,
		io: crate::storage::io_uring::IoRingHandle,
		max_slab_bytes: u64,
		pool: Arc<BufPool>,
		state: TokioMutex<CrashyReplicaState>,
	}

	impl CrashyReplicaService {
		fn open(dir: std::path::PathBuf, pool: Arc<BufPool>) -> Arc<Self> {
			let io = spawn_io_thread(64).unwrap();
			let max_slab_bytes = 1 << 30;
			let manager = Arc::new(
				SlabManager::open(SlabManagerConfig {
					dir: dir.clone(),
					max_slab_bytes,
					io: io.clone(),
					l2: None,
				})
				.unwrap(),
			);
			Arc::new(Self {
				dir,
				io,
				max_slab_bytes,
				pool,
				state: TokioMutex::new(CrashyReplicaState {
					manager,
					volatile_cutoffs: StdHashMap::new(),
				}),
			})
		}

		async fn manager(&self) -> Arc<SlabManager> {
			self.state.lock().await.manager.clone()
		}

		async fn crash_and_recover(&self) {
			let mut state = self.state.lock().await;
			for (slab_id, cutoff) in state.volatile_cutoffs.drain() {
				let path = self.dir.join(format!("slab_{slab_id:016x}.wnd"));
				if !path.exists() {
					continue;
				}
				let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
				file.set_len(cutoff).unwrap();
			}
			state.manager = Arc::new(
				SlabManager::open(SlabManagerConfig {
					dir: self.dir.clone(),
					max_slab_bytes: self.max_slab_bytes,
					io: self.io.clone(),
					l2: None,
				})
				.unwrap(),
			);
		}
	}

	#[async_trait]
	impl HrftService for CrashyReplicaService {
		async fn handle_frame(&self, frame: HrftFrame) -> std::io::Result<Option<HrftFrame>> {
			if frame.header.msg_type != MsgType::REPLICATE_REQ.0 {
				return Err(Error::new(
					ErrorKind::Unsupported,
					format!(
						"crashy replica handles REPLICATE_REQ only, got msg_type=0x{:02x}",
						frame.header.msg_type
					),
				));
			}
			if frame.meta.len() < std::mem::size_of::<ReplicaWriteMeta>() {
				return Err(Error::new(
					ErrorKind::InvalidData,
					"REPLICATE_REQ meta too small",
				));
			}
			let meta: ReplicaWriteMeta =
				*bytemuck::from_bytes(&frame.meta[..std::mem::size_of::<ReplicaWriteMeta>()]);
			let arrow = frame.arrow_payload.as_ref().ok_or_else(|| {
				Error::new(
					ErrorKind::InvalidData,
					"REPLICATE_REQ missing arrow payload",
				)
			})?;

			let manager = self.manager().await;
			let arrow_bytes: &[u8] = &arrow[..];
			let (status, ack_level, location) = if blake3_32(arrow_bytes) != meta.payload_hash {
				(
					ReplicaWriteStatus::HASH_MISMATCH,
					ReplicaAckLevel::NONE,
					None,
				)
			} else {
				match manager
					.append(meta.key_hash, meta.commit_hlc, arrow_bytes)
					.await
				{
					Ok(location) => (
						ReplicaWriteStatus::OK,
						ReplicaAckLevel::APPEND_ONLY,
						Some(location),
					),
					Err(_) => (
						ReplicaWriteStatus::INTERNAL_ERROR,
						ReplicaAckLevel::NONE,
						None,
					),
				}
			};

			if let Some(location) = location {
				let mut state = self.state.lock().await;
				state
					.volatile_cutoffs
					.entry(location.slab_id)
					.and_modify(|cutoff| *cutoff = (*cutoff).min(location.offset))
					.or_insert(location.offset);
			}

			let resp = ReplicaWriteRespMeta {
				status,
				ack_level,
				_pad: [0; 6],
			};
			let mut body = self.pool.acquire();
			body.as_mut_vec()
				.extend_from_slice(bytemuck::bytes_of(&resp));
			Ok(Some(HrftFrame {
				header: FrameHeader {
					magic: HRFT_MAGIC,
					version: 1,
					msg_type: MsgType::REPLICATE_RESP.0,
					flags: 0,
					stream_id: frame.header.stream_id,
					meta_len: body.len() as u32,
					arrow_len: 0,
					trailing_len: 0,
				},
				meta: body,
				arrow_payload: None,
				trailing: None,
			}))
		}
	}

	struct DurabilityFixture {
		leader: Arc<WendelonDataServer>,
		leader_slab: Arc<SlabManager>,
		durable_slab: Arc<SlabManager>,
		crashy: Arc<CrashyReplicaService>,
		_leader_node: WendelonNode,
		_durable_node: WendelonNode,
		_epoch_tx: watch::Sender<Arc<MembershipEpoch>>,
	}

	async fn build_durability_fixture(
		durable_key_hash: u64,
		append_only_key_hash: u64,
	) -> DurabilityFixture {
		let leader_node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			41,
			bootstrap_tempdir("durability-leader"),
			"loopback://41".into(),
		))
		.await
		.unwrap();
		let durable_node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			42,
			bootstrap_tempdir("durability-durable"),
			"loopback://42".into(),
		))
		.await
		.unwrap();

		let leader_id = leader_node.local_node;
		let durable_id = durable_node.local_node;
		let crashy_id = n(43);
		let pool = leader_node.pool.clone();
		let handlers = LoopbackHandlers::new(pool.clone());
		let loopback_provider: Arc<dyn TransportProvider> = Arc::new(LoopbackProvider {
			handlers: handlers.clone(),
			tier: TransportTier::QuicIoUring,
		});

		let tiers = TransportTierSet::empty().with(TransportTier::QuicIoUring);
		let roles = NodeRoleSet::empty().with(NodeRole::Data);
		let mut peers = StdHashMap::new();
		peers.insert(
			leader_id,
			PeerCapabilities {
				address: "loopback://41".into(),
				tiers,
				roles,
			},
		);
		peers.insert(
			durable_id,
			PeerCapabilities {
				address: "loopback://42".into(),
				tiers,
				roles,
			},
		);
		peers.insert(
			crashy_id,
			PeerCapabilities {
				address: "loopback://43".into(),
				tiers,
				roles,
			},
		);
		let epoch = MembershipEpoch { epoch: 1, peers };
		let (epoch_tx, epoch_rx) = watch::channel(Arc::new(epoch));
		let resolver: Arc<dyn crate::network::transport::CapabilityResolver> = Arc::new(
			crate::network::transport::EpochCapabilityResolver::new(epoch_rx.clone()),
		);

		let build_factory = || {
			let mut providers: StdHashMap<TransportTier, Arc<dyn TransportProvider>> =
				StdHashMap::new();
			providers.insert(TransportTier::QuicIoUring, loopback_provider.clone());
			TransportProviderFactory::new(tiers, providers, resolver.clone(), epoch_rx.clone())
		};

		let router: Arc<dyn ReplicaRouter> = Arc::new(
			FixedReplicaRouter::new(vec![leader_id, durable_id, crashy_id])
				.with_route(durable_key_hash, vec![leader_id, durable_id, crashy_id])
				.with_route(append_only_key_hash, vec![leader_id, crashy_id]),
		);

		let leader = Arc::new(WendelonDataServer::new(
			leader_node.slab.clone(),
			leader_node.raft.clone(),
			leader_node.log_store.clone(),
			leader_node.state_machine.clone(),
			leader_id,
			leader_node.clock.clone(),
			router.clone(),
			build_factory(),
			pool.clone(),
		));
		let durable_replica = Arc::new(WendelonDataServer::new(
			durable_node.slab.clone(),
			durable_node.raft.clone(),
			durable_node.log_store.clone(),
			durable_node.state_machine.clone(),
			durable_id,
			durable_node.clock.clone(),
			router,
			build_factory(),
			durable_node.pool.clone(),
		));
		let durable_data_ring: Arc<dyn DataRingServer> = durable_replica;
		let durable_service: Arc<dyn HrftService> = Arc::new(
			MsgRouter::builder()
				.with_data_ring(durable_data_ring)
				.build(),
		);
		let crashy = CrashyReplicaService::open(fresh_dir("crashy-replica"), pool.clone());
		let crashy_service: Arc<dyn HrftService> = crashy.clone();
		handlers.register(durable_id, durable_service);
		handlers.register(crashy_id, crashy_service);

		DurabilityFixture {
			leader,
			leader_slab: leader_node.slab.clone(),
			durable_slab: durable_node.slab.clone(),
			crashy,
			_leader_node: leader_node,
			_durable_node: durable_node,
			_epoch_tx: epoch_tx,
		}
	}

	/// Plan 020 crash-model durability boundary.
	///
	/// One replica intentionally acks `APPEND_ONLY`, then the test
	/// crashes it by truncating the unflushed tail and reopening the
	/// slab manager from disk. The leader must ignore that ack for
	/// quorum durability:
	///
	/// * a write with two real durable replicas (`leader` +
	///   `durable`) returns `OK` and still survives after the crashy
	///   replica loses its copy.
	/// * a write with only `leader` durable plus the crashy
	///   append-only replica does not return `OK`, and after the
	///   simulated crash fewer than quorum copies remain.
	#[tokio::test]
	async fn write_counts_only_durable_quorum_acks_across_replica_crash_boundary() {
		let durable_key = b"durable-quorum";
		let append_only_key = b"append-only";
		let durable_key_hash = highway_hash(durable_key);
		let append_only_key_hash = highway_hash(append_only_key);
		let fx = build_durability_fixture(durable_key_hash, append_only_key_hash).await;

		let durable_value = b"v-durable";
		let mut durable_meta = simple_write_meta(1);
		durable_meta.write_id.sequence_no = 101;
		durable_meta.key_len = durable_key.len() as u32;
		durable_meta.value_len = durable_value.len() as u32;
		let (durable_resp, _) = fx
			.leader
			.write(durable_meta, kv_batch(durable_key, durable_value))
			.await
			.unwrap();
		assert_eq!(durable_resp.status, WriteStatus::OK);

		fx.crashy.crash_and_recover().await;

		let crashy_after_ok = fx.crashy.manager().await;
		let durable_survivors = [
			fx.leader_slab.lookup(durable_key_hash).is_some(),
			fx.durable_slab.lookup(durable_key_hash).is_some(),
			crashy_after_ok.lookup(durable_key_hash).is_some(),
		]
		.into_iter()
		.filter(|present| *present)
		.count();
		assert_eq!(
			durable_survivors, 2,
			"only the two durably-acked replicas should retain the successful write"
		);
		assert!(
			!crashy_after_ok.lookup(durable_key_hash).is_some(),
			"append-only replica must lose its copy across the simulated crash"
		);

		let append_only_value = b"v-append-only";
		let mut append_only_meta = simple_write_meta(1);
		append_only_meta.write_id.sequence_no = 102;
		append_only_meta.key_len = append_only_key.len() as u32;
		append_only_meta.value_len = append_only_value.len() as u32;
		let (append_only_resp, _) = fx
			.leader
			.write(
				append_only_meta,
				kv_batch(append_only_key, append_only_value),
			)
			.await
			.unwrap();
		assert_eq!(
			append_only_resp.status,
			WriteStatus::INTERNAL_ERROR,
			"leader must not treat append-only replica acks as quorum durable"
		);

		fx.crashy.crash_and_recover().await;

		let crashy_after_fail = fx.crashy.manager().await;
		let append_only_survivors = [
			fx.leader_slab.lookup(append_only_key_hash).is_some(),
			fx.durable_slab.lookup(append_only_key_hash).is_some(),
			crashy_after_fail.lookup(append_only_key_hash).is_some(),
		]
		.into_iter()
		.filter(|present| *present)
		.count();
		assert_eq!(
			append_only_survivors, 1,
			"without durable quorum, the write must not survive on a quorum after crash recovery"
		);
	}

	/// Three-node drain-all fixture.
	///
	/// * `dest` — the destination data server; its `pull_migration_range`
	///   is what each test drives.
	/// * `source_a_slab`, `source_b_slab` — seed writes directly
	///   through these to populate each source with disjoint or
	///   overlapping retained versions.
	/// * `handlers` — fault-injection handle. Tests call
	///   `fail_source_after_pages` to model mid-drain failures.
	/// * `source_a_id`, `source_b_id` — NodeId fields for the
	///   ledger entry seed.
	struct DrainAllFixture {
		dest: Arc<WendelonDataServer>,
		source_a_slab: Arc<SlabManager>,
		source_b_slab: Arc<SlabManager>,
		handlers: Arc<LoopbackHandlers>,
		dest_id: NodeId,
		source_a_id: NodeId,
		source_b_id: NodeId,
		// Kept alive for the duration of the test so the tracked
		// state machines and factory cache don't get dropped.
		_nodes: Vec<WendelonNode>,
		_epoch_tx: watch::Sender<Arc<MembershipEpoch>>,
	}

	impl DrainAllFixture {
		/// Seed the same ledger entry on every node's state
		/// machine so each source's `handle_migrate` accepts the
		/// `MIGRATE_REQ` and the destination can reference it
		/// from `pull_migration_range`.
		async fn seed_migration(&self, entry: MigrationLedgerEntry) {
			for node in &self._nodes {
				node.state_machine
					.seed_migration_for_test(entry.clone())
					.await;
			}
			// And on the dest's SM — it is shared with the
			// originating node so the `for` above already covered
			// it, but assert anyway.
			let sm = self.dest.state_machine.state_snapshot().await;
			assert!(sm.migrations.contains_key(&entry.migration_id));
		}

		fn ledger_entry(
			&self,
			migration_id: u64,
			range_start: u64,
			range_end: u64,
		) -> MigrationLedgerEntry {
			MigrationLedgerEntry {
				migration_id,
				range_start,
				range_end,
				from_epoch: 0,
				to_epoch: 1,
				logical_owner: self.dest_id,
				upstream_migration_id: None,
				source_replicas: vec![self.source_a_id, self.source_b_id],
				migration_hlc_floor: hlc(0, 0, 0),
				state: MigrationState::Pending,
				completed_by: None,
				completed_at_hlc: None,
			}
		}
	}

	async fn build_drain_all_fixture(tag: &str) -> DrainAllFixture {
		// 1. Bootstrap three single-node clusters. Each brings its
		//    own slab, raft, state machine, and pool. We reuse
		//    those handles but rebuild the WendelonDataServer on
		//    top of a loopback-backed factory so the three nodes
		//    can talk to each other.
		let dest_node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			31,
			bootstrap_tempdir(&format!("{tag}-dest")),
			"loopback://31".into(),
		))
		.await
		.unwrap();
		let source_a_node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			32,
			bootstrap_tempdir(&format!("{tag}-a")),
			"loopback://32".into(),
		))
		.await
		.unwrap();
		let source_b_node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			33,
			bootstrap_tempdir(&format!("{tag}-b")),
			"loopback://33".into(),
		))
		.await
		.unwrap();

		let dest_id = dest_node.local_node;
		let source_a_id = source_a_node.local_node;
		let source_b_id = source_b_node.local_node;
		let pool = dest_node.pool.clone();

		// 2. Shared loopback registry. Services are registered
		//    after each fresh DataServer is built below.
		let handlers = LoopbackHandlers::new(pool.clone());
		let loopback_provider: Arc<dyn TransportProvider> = Arc::new(LoopbackProvider {
			handlers: handlers.clone(),
			tier: TransportTier::QuicIoUring,
		});

		// 3. Membership epoch visible to every node's resolver —
		//    lists all three peers advertising QuicIoUring so the
		//    factory's tier negotiation picks the loopback
		//    provider.
		let tiers = TransportTierSet::empty().with(TransportTier::QuicIoUring);
		let roles = NodeRoleSet::empty().with(NodeRole::Data);
		let mut peers = StdHashMap::new();
		peers.insert(
			dest_id,
			PeerCapabilities {
				address: "loopback://31".into(),
				tiers,
				roles,
			},
		);
		peers.insert(
			source_a_id,
			PeerCapabilities {
				address: "loopback://32".into(),
				tiers,
				roles,
			},
		);
		peers.insert(
			source_b_id,
			PeerCapabilities {
				address: "loopback://33".into(),
				tiers,
				roles,
			},
		);
		let epoch = MembershipEpoch { epoch: 1, peers };
		let (epoch_tx, epoch_rx) = watch::channel(Arc::new(epoch));

		// 4. Build a shared factory per node (every node dials
		//    through the same loopback). The factory caches
		//    sessions per target, so all three nodes end up
		//    producing their own per-target connections.
		let resolver: Arc<dyn crate::network::transport::CapabilityResolver> = Arc::new(
			crate::network::transport::EpochCapabilityResolver::new(epoch_rx.clone()),
		);
		let mut providers: StdHashMap<TransportTier, Arc<dyn TransportProvider>> =
			StdHashMap::new();
		providers.insert(TransportTier::QuicIoUring, loopback_provider);

		// Each node gets its own factory so invalidation and
		// connection caches are independent — mirrors production.
		let build_factory = || {
			let mut ps = StdHashMap::new();
			ps.insert(
				TransportTier::QuicIoUring,
				providers
					.get(&TransportTier::QuicIoUring)
					.expect("provider registered above")
					.clone(),
			);
			TransportProviderFactory::new(tiers, ps, resolver.clone(), epoch_rx.clone())
		};
		let dest_factory = build_factory();
		let source_a_factory = build_factory();
		let source_b_factory = build_factory();

		// 5. Rebuild each DataServer on top of the loopback
		//    factory. We reuse the slab / raft / state machine /
		//    pool from the bootstrapped node, so the state
		//    machine (which carries the migration ledger) is the
		//    one the bootstrap originally registered.
		let router: Arc<dyn ReplicaRouter> = Arc::new(StaticReplicaRouter::new(
			vec![dest_id, source_a_id, source_b_id],
			1,
		));
		let dest_ds = Arc::new(WendelonDataServer::new(
			dest_node.slab.clone(),
			dest_node.raft.clone(),
			dest_node.log_store.clone(),
			dest_node.state_machine.clone(),
			dest_id,
			dest_node.clock.clone(),
			router.clone(),
			dest_factory,
			pool.clone(),
		));
		let source_a_ds = Arc::new(WendelonDataServer::new(
			source_a_node.slab.clone(),
			source_a_node.raft.clone(),
			source_a_node.log_store.clone(),
			source_a_node.state_machine.clone(),
			source_a_id,
			source_a_node.clock.clone(),
			router.clone(),
			source_a_factory,
			source_a_node.pool.clone(),
		));
		let source_b_ds = Arc::new(WendelonDataServer::new(
			source_b_node.slab.clone(),
			source_b_node.raft.clone(),
			source_b_node.log_store.clone(),
			source_b_node.state_machine.clone(),
			source_b_id,
			source_b_node.clock.clone(),
			router,
			source_b_factory,
			source_b_node.pool.clone(),
		));

		// 6. Wrap each DataServer behind MsgRouter so the
		//    harness hits the full dispatch path
		//    (`handle_frame` → `handle_data_frame` →
		//    `handle_migrate`) rather than calling
		//    `handle_migrate` directly. That covers the
		//    `MsgType::MIGRATE_REQ` arm for free.
		let dest_data_ring: Arc<dyn DataRingServer> = dest_ds.clone();
		let source_a_data_ring: Arc<dyn DataRingServer> = source_a_ds.clone();
		let source_b_data_ring: Arc<dyn DataRingServer> = source_b_ds.clone();
		let dest_svc: Arc<dyn HrftService> =
			Arc::new(MsgRouter::builder().with_data_ring(dest_data_ring).build());
		let source_a_svc: Arc<dyn HrftService> = Arc::new(
			MsgRouter::builder()
				.with_data_ring(source_a_data_ring)
				.build(),
		);
		let source_b_svc: Arc<dyn HrftService> = Arc::new(
			MsgRouter::builder()
				.with_data_ring(source_b_data_ring)
				.build(),
		);
		handlers.register(dest_id, dest_svc);
		handlers.register(source_a_id, source_a_svc);
		handlers.register(source_b_id, source_b_svc);

		DrainAllFixture {
			dest: dest_ds,
			source_a_slab: source_a_node.slab.clone(),
			source_b_slab: source_b_node.slab.clone(),
			handlers,
			dest_id,
			source_a_id,
			source_b_id,
			_nodes: vec![dest_node, source_a_node, source_b_node],
			_epoch_tx: epoch_tx,
		}
	}

	/// Directly append an Arrow-encoded key/value at `hlc` to a
	/// source's slab. Bypasses the write path — we only need the
	/// bytes on disk so the source's `handle_migrate` can scan
	/// them.
	async fn seed_source(slab: &SlabManager, key_hash: u64, hlc_v: HybridTimestamp, value: &[u8]) {
		let batch = kv_batch(&key_hash.to_le_bytes(), value);
		let bytes = encode_batch(&batch).unwrap();
		slab.append(key_hash, hlc_v, &bytes).await.unwrap();
	}

	/// Regression for drain-all union over HRFT.
	///
	/// source_a holds keys 0..50, source_b holds keys 50..100.
	/// After `pull_migration_range`, the destination slab must
	/// contain every key in 0..100 exactly once — proving the
	/// pull loop walks *every* entry in `source_replicas` and
	/// unions their contributions via the real wire format.
	#[tokio::test]
	async fn pull_migration_range_unions_disjoint_halves_over_hrft() {
		let fx = build_drain_all_fixture("disjoint").await;
		let v = hlc(10, 0, 1);
		for k in 0..50u64 {
			seed_source(&fx.source_a_slab, k, v, b"v-a").await;
		}
		for k in 50..100u64 {
			seed_source(&fx.source_b_slab, k, v, b"v-b").await;
		}

		let entry = fx.ledger_entry(1001, 0, 99);
		fx.seed_migration(entry.clone()).await;

		fx.dest
			.pull_migration_range(&entry, 0, 99)
			.await
			.expect("drain-all must succeed when both sources are reachable");

		let merged = fx.dest.slab.scan_as_of(0, 99, v);
		let mut keys: Vec<u64> = merged.iter().map(|(k, _)| *k).collect();
		keys.sort();
		keys.dedup();
		assert_eq!(keys.len(), 100, "union must cover every key in 0..=99");
		assert_eq!(*keys.first().unwrap(), 0);
		assert_eq!(*keys.last().unwrap(), 99);
	}

	/// Regression for idempotent union: overlapping sources
	/// re-emit the same `(key_hash, hlc, payload_hash)` tuples;
	/// `append_migration` must discard the duplicate after the
	/// first source ingested it. Proves the correctness boundary
	/// holds when the wire delivers overlapping rows.
	#[tokio::test]
	async fn pull_migration_range_idempotent_across_overlapping_sources() {
		let fx = build_drain_all_fixture("overlap").await;
		let v = hlc(42, 0, 1);
		for k in 0..75u64 {
			seed_source(&fx.source_a_slab, k, v, b"shared").await;
		}
		for k in 25..100u64 {
			seed_source(&fx.source_b_slab, k, v, b"shared").await;
		}

		let entry = fx.ledger_entry(1002, 0, 99);
		fx.seed_migration(entry.clone()).await;

		fx.dest
			.pull_migration_range(&entry, 0, 99)
			.await
			.expect("drain-all with overlap must succeed");

		let merged = fx.dest.slab.scan_as_of(0, 99, v);
		let mut keys: Vec<u64> = merged.iter().map(|(k, _)| *k).collect();
		keys.sort();
		keys.dedup();
		assert_eq!(
			keys.len(),
			100,
			"overlap union must still resolve to 100 distinct keys, not 75+75"
		);

		// Every key should resolve to exactly one retained version.
		for &k in &keys {
			let versions = fx.dest.slab.scan_all_versions(k, k, hlc(0, 0, 0));
			assert_eq!(
				versions.len(),
				1,
				"key {k}: overlap must not duplicate versions"
			);
		}
	}

	/// Regression for `RANGE_PAGE` paging within a single source.
	///
	/// Shrink the page caps so even a modest record count forces
	/// multiple round-trips. The pull must advance the
	/// `(cursor_hash, cursor_hlc)` correctly between pages and
	/// deliver every record exactly once. The loopback page
	/// counter assertion guards against a regression in which the
	/// cap override is silently ignored — without it the coverage
	/// check passes even if the source returns all 25 rows in one
	/// MIGRATE_RESP.
	#[tokio::test]
	async fn pull_migration_range_pages_through_single_source() {
		let fx = build_drain_all_fixture("paging").await;
		// Tiny cap → 25 keys / 10 per page = 3 round-trips.
		fx.dest.set_page_caps_for_test(10, 64 * 1024);

		let v = hlc(7, 0, 1);
		for k in 0..25u64 {
			seed_source(&fx.source_a_slab, k, v, b"p").await;
		}
		// source_b stays empty — exercises the
		// empty-page/has_more=0 branch in one round-trip.

		let entry = fx.ledger_entry(1003, 0, 24);
		fx.seed_migration(entry.clone()).await;

		fx.dest
			.pull_migration_range(&entry, 0, 24)
			.await
			.expect("paged drain must succeed");

		let merged = fx.dest.slab.scan_as_of(0, 24, v);
		let mut keys: Vec<u64> = merged.iter().map(|(k, _)| *k).collect();
		keys.sort();
		keys.dedup();
		assert_eq!(
			keys.len(),
			25,
			"paged drain must deliver every record across multiple pages"
		);

		// 25 keys at cap 10 ⇒ 3 pages from source_a (10 + 10 + 5).
		// The empty source_b adds exactly 1 page (has_more=0).
		let pages_a = fx.handlers.page_count_for(fx.source_a_id);
		assert!(
			pages_a >= 3,
			"paging cap must force ≥3 MIGRATE_REQ to source_a; observed {pages_a}"
		);
		assert_eq!(
			fx.handlers.page_count_for(fx.source_b_id),
			1,
			"empty source_b should be drained in a single round-trip"
		);
	}

	/// Regression for mid-drain failure surfacing through the
	/// merge path.
	///
	/// source_b is configured to fail on its *second* page after
	/// page 1 succeeded. The end-to-end query must surface
	/// `ReadStatus::MIGRATION_INCOMPLETE` — a distinct branch from
	/// the unreachable-source case already covered by
	/// `query_merge_surfaces_migration_incomplete_on_source_failure`,
	/// because here the source is reachable, accepts page 1, and
	/// only fails partway through. Driving
	/// `execute_query_with_migration_merge` (rather than calling
	/// `pull_migration_range` directly) is the point: it proves
	/// the mid-drain `Err` propagates all the way to the
	/// client-visible status, not just the internal pull return.
	#[tokio::test]
	async fn query_merge_surfaces_migration_incomplete_on_partial_source_failure() {
		let fx = build_drain_all_fixture("partial-fail").await;
		// Small cap so source_b produces >1 page.
		fx.dest.set_page_caps_for_test(5, 64 * 1024);

		let v = hlc(11, 0, 1);
		// source_a exhausts in one page (3 keys, cap 5) — its
		// drain completes before source_b is attempted.
		for k in 0..3u64 {
			seed_source(&fx.source_a_slab, k, v, b"a").await;
		}
		// source_b has 12 keys → 3 pages under cap 5. Page 1
		// succeeds, page 2 is intercepted with INTERNAL_ERROR.
		for k in 100..112u64 {
			seed_source(&fx.source_b_slab, k, v, b"b").await;
		}

		let entry = fx.ledger_entry(1004, 0, u64::MAX);
		fx.seed_migration(entry.clone()).await;

		// Allow source_b exactly one successful page, then fail.
		fx.handlers.fail_source_after_pages(fx.source_b_id, 1);

		let (resp, _arrow) = fx
			.dest
			.execute_query_with_migration_merge(&base_meta(), &[])
			.await
			.expect("merge path returns Ok wrapping a failure status");
		assert_eq!(
			resp.status,
			ReadStatus::MIGRATION_INCOMPLETE,
			"mid-drain failure must surface MIGRATION_INCOMPLETE at the query layer"
		);

		// Confirm the fault model held: source_b saw at least 2
		// page attempts (page 1 OK + page 2 intercepted).
		let pages_b = fx.handlers.page_count_for(fx.source_b_id);
		assert!(
			pages_b >= 2,
			"source_b must have been asked for ≥2 pages before failing; observed {pages_b}"
		);
	}

	/// Regression for the `source_replicas` self-skip.
	///
	/// If the destination is listed among the sources, the pull
	/// must not dial itself. We prove this by configuring the
	/// destination's own entry in the loopback to fail every
	/// `MIGRATE_REQ` — if the loop dialed itself the pull would
	/// return `Err`, but the self-skip means the failure is
	/// never reached and the pull succeeds over source_a alone.
	#[tokio::test]
	async fn pull_migration_range_skips_local_node_in_source_list() {
		let fx = build_drain_all_fixture("skip-local").await;
		let v = hlc(3, 0, 1);
		for k in 0..5u64 {
			seed_source(&fx.source_a_slab, k, v, b"a").await;
		}

		let mut entry = fx.ledger_entry(1005, 0, 4);
		// Put dest_id at the head of source_replicas to prove the
		// skip logic fires even when we're the first candidate.
		entry.source_replicas = vec![fx.dest_id, fx.source_a_id];
		fx.seed_migration(entry.clone()).await;

		// Arm the self-loopback to fail immediately, so a stray
		// self-dial would surface as a pull Err.
		fx.handlers.fail_source_after_pages(fx.dest_id, 0);

		fx.dest
			.pull_migration_range(&entry, 0, 4)
			.await
			.expect("self must be skipped; pull succeeds via source_a");

		let merged = fx.dest.slab.scan_as_of(0, 4, v);
		let mut keys: Vec<u64> = merged.iter().map(|(k, _)| *k).collect();
		keys.sort();
		assert_eq!(keys, vec![0, 1, 2, 3, 4]);
	}
}
