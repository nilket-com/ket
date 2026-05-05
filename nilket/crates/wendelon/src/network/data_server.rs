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
//! retries. Committed writes replay as [`WriteStatus::DUPLICATE`];
//! if the data branch reached quorum but the control branch is still
//! pending, the cache points at the persisted recovery record
//! instead of re-running the write from scratch.

use crate::audit::{
	AuditAuthority, AuditEvent, AuditOperation, AuditOutcome, AuditSchema, AuditSink,
	DataAccessKind, DataAccessPhase, DataAccessSchema, NoopAuditSink,
};
use crate::consensus::monitor::{NodeTracker, PhysicalClock};
use crate::consensus::raft_storage::{SmState, WendelonLogStore};
use crate::consensus::{
	AdmissionSnapshot, HybridLogicalClockDriver, MigrationComplete, MigrationLedgerEntry,
	MigrationState, TenantFinalization, TenantLifecycleState, WendelonRaft, WendelonRequest,
	WendelonResponse, WendelonStateMachine,
};
use crate::governance::{AutonomousActionSurface, authorize_autonomous_action};
use crate::metrics::{
	MigrationProxyReadOutcome, NetworkCompressionPath, PendingFinalizationReplayPath,
	global as metrics,
};
use crate::network::cdc::{
	CdcSubscriptionRegistry, SubscriptionHandle, cdc_emit_checkpoint, cdc_emit_empty_frame,
	cdc_emit_frame, cdc_emit_reject,
};
use crate::network::core::{HRFT_MAGIC, read_frame, read_frame_from_header, write_frame};
use crate::network::factory::TransportProviderFactory;
use crate::network::ipc::{
	observe_wire_encode_error, observe_wire_encode_success, wire_encode_batch_for_path,
};
use crate::network::lease_signing::LeaseAuthority;
use crate::network::pending_finalizations::{
	PendingFinalizationId, PendingFinalizationRecord, PendingFinalizationStore,
	pending_finalizations_path,
};
use crate::network::pool::BufPool;
use crate::network::ring::{RingRouter, RingState};
use crate::network::router::DataRingServer;
use crate::network::server::RequestContext;
use crate::network::tls::{ActorIdentity, ActorRole};
use crate::network::transport::{MembershipEpoch, NodeCapability, NodeRole, TransportStream};
use crate::network::wire;
use crate::schema::{
	AcquireLeaseRequestMeta, AcquireLeaseResponseMeta, AggRejectCode, AggregateBudgets,
	AggregateDiagnostics, AggregateExpression, AggregateExpressionIndex, AggregateFunction,
	AggregateRequest, AggregateResponse, AggregateState, AggregateStatus,
	COLUMN_PROJECTION_RANGE_ARC_BUDGET, CdcCheckpointMeta, CdcCursor, CdcDigestDomain,
	CdcFrameMeta, CdcFrameSource, CdcRejectCode, CdcRejectMeta, CdcSubscribeBound,
	CdcSubscribeRequestMeta, CdcSubscriptionId, ColumnProjectionRangePruning, Consistency,
	DurabilityLevel, FRAME_FLAG_STREAM_END, FinalizationMarker, FrameHeader, HybridTimestamp,
	Identity, LeaseDescriptor, LeaseStatus, Locality, LocalityPolicyV1, LocalityRange,
	MigrateRequestKind, MigrateRequestMeta, MigrateResponseMeta, MigrateStatus, MsgType,
	MultiRowLayoutDescriptor, NodeId, PbcRejectCode, PendingWitnessPullRequestMeta,
	PendingWitnessPullResponseMeta, PendingWitnessPullStatus, QueryRequestMeta, QueryResponseMeta,
	ReadRequestMeta, ReadResponseMeta, ReadStatus, RecordBodyCompressionPolicy, RecordLayoutPolicy,
	RecordRowIndex, ReplicaAckLevel, ReplicaWriteMeta, ReplicaWriteRespMeta, ReplicaWriteStatus,
	RowCount, RowIdentityPolicy, STORAGE_ROW_KEY_ID_COLUMN, SecondaryIndexCatalog,
	SecondaryIndexDescriptor, SecondaryIndexState, SlabId, StreamFinalizationRequestMeta,
	TABLE_WRITE_MAX_ROWS, TableDeleteRowsRequestMeta, TableDeleteRowsResponseMeta, TableObjectId,
	TableQueryRequestMeta, TableReplicaWriteMeta, TableSchemaDescriptorV1, TableWriteAdmission,
	TableWriteRequestMeta, TableWriteResponseMeta, TableWriteValidationError, TenantId,
	WriteRequestMeta, WriteResponseMeta, WriteStatus, WriterNodeId, cdc_cursor_in_catchup_window,
	cdc_event_batch, cdc_tombstone_event_batch, cdc_visible_rows_digest,
	column_projection_predicate_ranges, column_projection_prefix_ranges, encode_aggregate_state,
	encode_table_write_row_count, migration_record_schema, pbc,
	query_request_stats_pruning_enabled, read_key_schema, table_spread_prefix_ranges,
	validate_table_write_request_batch, write_payload_schema,
};
#[cfg(test)]
use crate::schema::{SupersessionMetadata, TombstoneMetadata};
use crate::storage::columnar::{
	ColumnFetchError, ColumnFetchRequirement, ProjectedRecordBatch,
	analyze_aggregate_query_column_requirement, analyze_pbc_column_requirement,
	execute_aggregate_projected_record_batch, execute_pbc_projected_record_batch,
};
use crate::storage::core::{RecordLocation, StorageEngine, is_zero_hlc};
use crate::storage::manager::SlabManager;
use crate::storage::multirow::{
	MultiRowSourceRow, decode_batch, encode_batch, encode_batch_with_policy,
	has_storage_synthetic_columns, materialize_record_location_batch,
	pack_admitted_record_batch_by_bucket, pack_tombstone_multirow_source_rows_with_policy,
	replace_storage_row_hlc_column, storage_index_entries_from_batch, storage_row_is_tombstone_at,
	storage_row_table_object_id_at,
};
use crate::storage::pk_delete::{
	PkAdmissionOutcome, admit_pk_delete_row, admit_pk_write_row, row_visible_for_admission,
};
use crate::storage::stats::{
	COLUMN_STAT_HAS_MAX, COLUMN_STAT_HAS_MIN, COLUMN_STAT_MAX_TRUNCATED, COLUMN_STAT_MIN_TRUNCATED,
	ColumnStatsBlockEntry, ColumnStatsPrune, ParsedColumnStatsBlock,
	column_stats_entry_matches_data_type, decode_column_stats_payload, encode_signed_stat_value,
	prune_by_encoded_any_of, prune_by_encoded_min_max,
};
use crate::storage::{
	CatchupSnapshot, CdcLiveEntry, CdcLiveTailRing, CdcLocalityFrontiers,
	CdcLocalityHeartbeatWorker, DEFAULT_MAX_CDC_LIVE_TAIL_BYTES_PER_LOCALITY,
};
use crate::storage::{
	EffectiveRetention, MvccRetentionPolicy, SecondaryIndexAccessPlan, SecondaryIndexLookupRow,
	TableLookupByIndexRequest, aggregate_record_batches, check_read_against_retention,
	cluster_retention_floor, effective_table_retention, merge_aggregate_responses,
	secondary_index_access_plan_from_program, secondary_index_entry_from_materialized_row,
	validate_expression_types,
};
use arrow::array::{
	Array, FixedSizeBinaryArray, FixedSizeBinaryBuilder, LargeBinaryArray, LargeBinaryBuilder,
	UInt32Array, UInt32Builder, UInt64Array, UInt64Builder,
};
use arrow::compute::{concat_batches, take};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use dashmap::DashMap;
use dashmap::mapref::entry::Entry as DashEntry;
use serde_json::json;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::io::{Cursor, Error, ErrorKind};
use std::path::PathBuf;
#[cfg(test)]
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tokio::sync::{Notify, OwnedMutexGuard, mpsc, watch};
use tokio::task::{JoinHandle, JoinSet, spawn_blocking};
use tracing::Instrument;

use crate::network::core::HrftFrame;
use crate::network::traits::DataRing;

type IdempotencyKey = ([u8; 16], u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DataOperation {
	Write,
	Read,
	Query,
	CdcSubscribe,
	Replicate,
	Migrate,
	PendingWitnessPull,
	StreamFinalization,
}

#[derive(Debug)]
struct CdcCatchupEvent {
	cursor: CdcCursor,
	batch: RecordBatch,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct CdcStagedEntryRef {
	locality: Locality,
	cursor: CdcCursor,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct CdcFinalizationKey {
	epoch: u64,
	commit_hlc: HybridTimestamp,
	payload_hash: [u8; 32],
	durability_raw: u8,
}

impl CdcFinalizationKey {
	fn from_marker(marker: FinalizationMarker) -> Self {
		Self {
			epoch: marker.epoch,
			commit_hlc: marker.commit_hlc,
			payload_hash: marker.payload_hash,
			durability_raw: marker.durability.0,
		}
	}

	fn from_table_replica_meta(meta: &TableReplicaWriteMeta) -> Self {
		Self {
			epoch: meta.epoch,
			commit_hlc: meta.commit_hlc,
			payload_hash: meta.payload_hash,
			durability_raw: meta.durability.0,
		}
	}
}

#[derive(Clone, Copy, Debug)]
#[allow(dead_code)]
enum WriteOutcome {
	DataBranchPending,
	DurableQuorumReached,
	FinalizationCommitted {
		response: WriteResponseMeta,
		trailing: Option<FinalizationMarker>,
	},
	FinalizationRecoverablePending {
		response: WriteResponseMeta,
		pending_finalization_id: PendingFinalizationId,
	},
	FinalizationTerminalFailure {
		response: WriteResponseMeta,
	},
	NoopCommitted {
		response: WriteResponseMeta,
	},
}

impl WriteOutcome {
	fn is_cacheable(self) -> bool {
		matches!(
			self,
			Self::FinalizationCommitted { .. }
				| Self::FinalizationRecoverablePending { .. }
				| Self::FinalizationTerminalFailure { .. }
				| Self::NoopCommitted { .. }
		)
	}
}

#[derive(Clone, Copy, Debug)]
struct CachedWriteOutcome {
	outcome: WriteOutcome,
}

impl CachedWriteOutcome {
	fn new(outcome: WriteOutcome) -> Self {
		debug_assert!(
			outcome.is_cacheable(),
			"non-cacheable write outcome inserted into idempotency cache"
		);
		Self { outcome }
	}

	fn outcome(self) -> WriteOutcome {
		self.outcome
	}
}

#[derive(Clone, Copy, Debug)]
struct CachedTableDeleteOutcome {
	response: TableDeleteRowsResponseMeta,
	trailing: Option<FinalizationMarker>,
}

#[derive(Clone, Copy, Debug)]
struct DurableQuorumTracker {
	required_durable_acks: usize,
	durable_acks: usize,
	leader_local_durable: bool,
}

impl DurableQuorumTracker {
	fn new(required_durable_acks: usize, leader_local_required: bool) -> Self {
		Self {
			required_durable_acks,
			durable_acks: 0,
			leader_local_durable: !leader_local_required,
		}
	}

	fn note_local_durable(&mut self) {
		debug_assert!(
			!self.leader_local_durable,
			"leader-local durable leg counted more than once"
		);
		self.leader_local_durable = true;
		self.durable_acks += 1;
	}

	fn note_remote_ack(&mut self, level: ReplicaAckLevel) {
		if level == ReplicaAckLevel::DURABLE {
			self.durable_acks += 1;
		}
	}

	#[cfg(test)]
	fn durable_ack_count(&self) -> usize {
		self.durable_acks
	}

	fn quorum_reached(&self) -> bool {
		self.durable_acks >= self.required_durable_acks
	}

	fn is_satisfied(&self) -> bool {
		self.quorum_reached() && self.leader_local_durable
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct OrphanWitnessRecoveryStamp {
	term: u64,
	epoch: u64,
}

#[derive(Clone, Debug)]
struct BifurcatedWrite {
	key_identity: Identity,
	locality: Locality,
	frame_bytes: Arc<Vec<u8>>,
	payload_hash: [u8; 32],
	charged_bytes: u64,
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Clone, Debug)]
struct BifurcatedTableWrite {
	children: Vec<BifurcatedTableChildWrite>,
	parent_key_identity: Identity,
	payload_hash: [u8; 32],
	charged_bytes: u64,
	record_count: u32,
	record_body_compression_policy: RecordBodyCompressionPolicy,
}

#[derive(Clone, Debug)]
struct ContentHashBifurcation {
	bifurcated: Option<BifurcatedTableWrite>,
	absorbed_rows: u64,
	stored_rows: u64,
}

#[derive(Clone, Debug)]
struct PrimaryKeyBifurcation {
	bifurcated: BifurcatedTableWrite,
	upsert_no_digest_add_rows: u64,
	resurrected_rows: u64,
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Clone, Debug)]
struct BifurcatedTableChildWrite {
	child_ordinal: u64,
	key_identity: Identity,
	locality: Locality,
	record_row_index: RecordRowIndex,
	frame_bytes: Arc<Vec<u8>>,
	payload_hash: [u8; 32],
	charged_bytes: u64,
}

#[derive(Clone, Debug)]
struct TableReplicaGroup {
	replicas: Vec<NodeId>,
	children: Vec<BifurcatedTableChildWrite>,
}

#[derive(Clone, Debug)]
struct TableReplicaChildPayload {
	child_ordinal: u64,
	key_identity: Identity,
	locality: Locality,
	record_row_index: RecordRowIndex,
	frame_bytes: Arc<Vec<u8>>,
	payload_hash: [u8; 32],
	charged_bytes: u64,
}

#[derive(Clone, Copy, Debug)]
enum PendingFinalizationResolution {
	CommittedNow(FinalizationMarker),
	ObservedApplied(FinalizationMarker),
}

#[derive(Clone, Default)]
struct PendingFinalizationClaims {
	inflight: Arc<DashMap<PendingFinalizationId, ()>>,
}

impl PendingFinalizationClaims {
	fn try_claim(&self, id: PendingFinalizationId) -> Option<PendingFinalizationClaim> {
		match self.inflight.entry(id) {
			DashEntry::Occupied(_) => None,
			DashEntry::Vacant(entry) => {
				entry.insert(());
				Some(PendingFinalizationClaim {
					inflight: self.inflight.clone(),
					id,
				})
			}
		}
	}
}

struct PendingFinalizationClaim {
	inflight: Arc<DashMap<PendingFinalizationId, ()>>,
	id: PendingFinalizationId,
}

impl Drop for PendingFinalizationClaim {
	fn drop(&mut self) {
		self.inflight.remove(&self.id);
	}
}

// ============================================================
//  MIGRATION REGISTRY (Plan 019)
// ============================================================

/// Transient dedup for concurrent migration pulls (Plan 019 goal #5).
///
/// When a hot read races the background sweeper on the same
/// `(migration_id, key_identity)`, both paths might otherwise issue
/// a `MIGRATE_REQ` and hand the same record to
/// [`SlabManager::append_migration`] twice. `append_migration`
/// is already idempotent — that is the correctness boundary —
/// but serializing pulls here avoids the duplicate RPC round-trip
/// and the duplicate `(key_identity, hlc)` scan that would follow.
///
/// The registry hands out one `Arc<tokio::sync::Mutex<()>>` per
/// inflight key. Callers take the lock, re-check the local index,
/// and only issue a pull when the local index is still a miss.
/// The lock survives past the caller's guard so a subsequent racer
/// can synchronise on the same key without a secondary refresh.
#[derive(Default)]
pub struct MigrationRegistry {
	inflight: DashMap<(u64, TenantId, Identity), Arc<tokio::sync::Mutex<()>>>,
}

impl MigrationRegistry {
	pub fn new() -> Self {
		Self {
			inflight: DashMap::new(),
		}
	}

	/// Obtain the per-key gate. The returned `Arc` is cheap to
	/// clone and safe to hold across `.await`. Every caller for a
	/// given `(migration_id, key_identity)` observes the same lock, so
	/// at most one pull is in flight per key.
	pub fn gate(
		&self,
		migration_id: u64,
		tenant_id: TenantId,
		key_identity: Identity,
	) -> Arc<tokio::sync::Mutex<()>> {
		self.inflight
			.entry((migration_id, tenant_id, key_identity))
			.or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
			.clone()
	}

	/// Drop the registry slot for `(migration_id, key_identity)`. Called
	/// by `try_migrate_point_read` once the pull has run to
	/// completion so the map's DRAM footprint caps at
	/// `O(concurrent_requests)` rather than growing with every key
	/// ever touched by a migration. Safe because
	/// [`SlabManager::append_migration`] is the real correctness
	/// boundary — a racer that arrives after removal simply mints
	/// a fresh mutex, re-checks the local index (now a hit), and
	/// skips the pull.
	pub fn remove(&self, migration_id: u64, tenant_id: TenantId, key_identity: Identity) {
		self.inflight
			.remove(&(migration_id, tenant_id, key_identity));
	}
}

/// RAII helper that drops the per-key slot out of the
/// [`MigrationRegistry`] once a pull finishes. Declared before the
/// mutex guard in the caller so the drop order is:
/// `_guard` (release mutex) → `_scope` (remove slot).
struct RegistryScopeGuard<'a> {
	registry: &'a MigrationRegistry,
	migration_id: u64,
	tenant_id: TenantId,
	key_identity: Identity,
}

impl Drop for RegistryScopeGuard<'_> {
	fn drop(&mut self) {
		self.registry
			.remove(self.migration_id, self.tenant_id, self.key_identity);
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct CachedTenantPolicy {
	state: TenantLifecycleState,
	quota_bytes: u64,
	used_bytes: u64,
	replication_factor: u32,
	mean_write_size_bytes: u64,
}

impl Default for CachedTenantPolicy {
	fn default() -> Self {
		Self {
			state: TenantLifecycleState::Creating,
			quota_bytes: 0,
			used_bytes: 0,
			replication_factor: 0,
			mean_write_size_bytes: 1,
		}
	}
}

#[derive(Debug, Default)]
struct TenantPolicyCacheState {
	policies: HashMap<TenantId, CachedTenantPolicy>,
	last_refresh: Option<Instant>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
struct TenantInflight {
	count: u32,
	bytes: u64,
}

struct TenantWriteReservation {
	inflight: Arc<DashMap<TenantId, TenantInflight>>,
	tenant_id: TenantId,
	bytes: u64,
	active: bool,
}

impl Drop for TenantWriteReservation {
	fn drop(&mut self) {
		if !self.active {
			return;
		}
		let Some(mut entry) = self.inflight.get_mut(&self.tenant_id) else {
			return;
		};
		entry.count = entry.count.saturating_sub(1);
		entry.bytes = entry.bytes.saturating_sub(self.bytes);
		if entry.count == 0 && entry.bytes == 0 {
			drop(entry);
			self.inflight.remove(&self.tenant_id);
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DataRequestBackpressureConfig {
	pub max_tenant_concurrent_requests: u32,
	pub max_actor_concurrent_requests: u32,
}

impl Default for DataRequestBackpressureConfig {
	fn default() -> Self {
		Self {
			max_tenant_concurrent_requests: 16,
			max_actor_concurrent_requests: 8,
		}
	}
}

impl DataRequestBackpressureConfig {
	pub fn validate(&self) -> std::io::Result<()> {
		if self.max_tenant_concurrent_requests == 0 {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"data_request_backpressure.max_tenant_concurrent_requests must be greater than zero",
			));
		}
		if self.max_actor_concurrent_requests == 0 {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"data_request_backpressure.max_actor_concurrent_requests must be greater than zero",
			));
		}
		Ok(())
	}
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct ActorRequestKey {
	role: ActorRole,
	key_id: Vec<u8>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BackpressureScope {
	Tenant,
	Actor,
}

struct TenantRequestReservation {
	inflight: Arc<DashMap<TenantId, u32>>,
	active_requests: Arc<AtomicU64>,
	tenant_id: TenantId,
	active: bool,
}

impl Drop for TenantRequestReservation {
	fn drop(&mut self) {
		if !self.active {
			return;
		}
		if let Some(mut entry) = self.inflight.get_mut(&self.tenant_id) {
			*entry = entry.saturating_sub(1);
			if *entry == 0 {
				drop(entry);
				self.inflight.remove(&self.tenant_id);
			}
		}
		let next = self.active_requests.fetch_sub(1, Ordering::Relaxed) - 1;
		metrics()
			.data_ring_backpressure_active_tenant_requests
			.set(next);
	}
}

struct ActorRequestReservation {
	inflight: Arc<DashMap<ActorRequestKey, u32>>,
	active_requests: Arc<AtomicU64>,
	key: ActorRequestKey,
	active: bool,
}

impl Drop for ActorRequestReservation {
	fn drop(&mut self) {
		if !self.active {
			return;
		}
		if let Some(mut entry) = self.inflight.get_mut(&self.key) {
			*entry = entry.saturating_sub(1);
			if *entry == 0 {
				drop(entry);
				self.inflight.remove(&self.key);
			}
		}
		let next = self.active_requests.fetch_sub(1, Ordering::Relaxed) - 1;
		metrics()
			.data_ring_backpressure_active_actor_requests
			.set(next);
	}
}

struct DataRequestAdmission {
	_tenant: Option<TenantRequestReservation>,
	_actor: Option<ActorRequestReservation>,
}

// ============================================================
//  REPLICA ROUTER
// ============================================================

/// Resolves a routing locality to the replica set that should store it.
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
	/// Members of the replica set for `locality`. The order is
	/// stable for a given topology so traces align; callers must
	/// not depend on the first entry being the local node.
	fn replicas_for_locality(&self, locality: Locality) -> Vec<NodeId>;

	/// Effective replication factor for this router. Exposed so the
	/// quorum math is source-of-truthed in one place.
	fn replication_factor(&self) -> usize;
}

/// Static ring router. Holds the full node list and an RF; resolves
/// each locality bucket's replica set as `RF` consecutive nodes
/// starting at `locality % nodes.len()`. This is a stand-in for the
/// full ADR 002
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
	fn replicas_for_locality(&self, locality: Locality) -> Vec<NodeId> {
		if self.nodes.is_empty() {
			return Vec::new();
		}
		let n = self.nodes.len();
		let take = self.rf.min(n);
		let start = (locality.get() as usize) % n;
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
	/// Hot-path admission view published from the state machine so
	/// request gating does not clone the full `SmState`.
	admission_snapshot_rx: watch::Receiver<Arc<AdmissionSnapshot>>,
	/// Local node identity, populated into every response's
	/// `leader_hint` and used to decide whether the local slab
	/// append counts toward quorum on a given key.
	local_node: NodeId,
	/// Local physical clock used to merge client HLCs into the
	/// leader's authoritative commit timestamp.
	clock: Arc<dyn PhysicalClock>,
	/// Audit sink used for node-local background events such as
	/// ADR 013 snapshot pruning.
	audit_sink: Arc<dyn AuditSink>,
	/// ADR 013 retention policy for MVCC history.
	mvcc_retention_policy: MvccRetentionPolicy,
	/// Resolves a routing locality to the replica set that should hold
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
	/// Exactly-once cache. The cached value includes any committed
	/// finalization marker so duplicate retries can replay the same
	/// durability receipt the original caller observed.
	idempotency: Arc<DashMap<IdempotencyKey, CachedWriteOutcome>>,
	/// Same-process count replay for append-only row deletes. The
	/// durable write idempotency cache owns commit replay; this cache
	/// preserves the row-level deleted/already-deleted/not-found counts.
	table_delete_idempotency: Arc<DashMap<IdempotencyKey, CachedTableDeleteOutcome>>,
	/// Per table serialization for ADR 049 content-hash admission.
	/// The visible-membership lookup and the subsequent store/no-op
	/// decision must not race with another write to the same table.
	content_hash_admission_locks:
		Arc<DashMap<(TenantId, TableObjectId), Arc<tokio::sync::Mutex<()>>>>,
	/// Durable local intent log for writes that passed the data
	/// quorum but still need their control-branch finalization
	/// committed through Raft.
	pending_finalizations: Arc<PendingFinalizationStore>,
	/// In-memory claim set that ensures only one task at a time can
	/// drive a given persisted pending finalization through Raft.
	pending_finalization_claims: PendingFinalizationClaims,
	/// Nudges the background reconciler to retry a pending
	/// finalization without waiting for the next periodic tick.
	pending_finalization_wakeup: Arc<Notify>,
	/// Highest `(term, membership_epoch)` pair for which this
	/// leader has completed its orphan-witness recovery sweep.
	orphan_witness_recovery_stamp: Arc<RwLock<Option<OrphanWitnessRecoveryStamp>>>,
	/// Serializes the once-per-term recovery sweep so the
	/// first concurrent writes after an election do not all issue
	/// the same peer pull RPCs.
	orphan_witness_recovery_lock: Arc<tokio::sync::Mutex<()>>,
	/// Once tripped, this node stops trusting its local durability
	/// substrate enough to accept fresh writes.
	local_persistence_distrust_reason: Arc<RwLock<Option<String>>>,
	/// On-disk latch for the local durability distrust fence.
	/// Bootstrap restores the node in degraded mode while this
	/// marker is present.
	local_persistence_distrust_path: PathBuf,
	#[cfg(test)]
	fail_next_local_durable_append: Arc<StdMutex<Option<(ErrorKind, String)>>>,
	/// Shared node-wide HLC driver. The leader merges client HLCs
	/// into this monotonic authority before stamping writes.
	hlc: Arc<HybridLogicalClockDriver>,
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
	/// Reconciled copy of the authoritative tenant table. Reads and
	/// writes consult this cache on the fast path; a background sweep
	/// refreshes it from the state machine.
	tenant_policy_cache: Arc<RwLock<TenantPolicyCacheState>>,
	/// In-flight write reservations per tenant. This keeps quota
	/// overshoot bounded between reconciliation passes.
	tenant_inflight: Arc<DashMap<TenantId, TenantInflight>>,
	/// Hard per-tenant request admission after authentication.
	tenant_request_inflight: Arc<DashMap<TenantId, u32>>,
	/// Hard per-actor request admission for cross-tenant or
	/// operator-style traffic.
	actor_request_inflight: Arc<DashMap<ActorRequestKey, u32>>,
	max_tenant_concurrent_requests: Arc<AtomicU32>,
	max_actor_concurrent_requests: Arc<AtomicU32>,
	active_tenant_requests: Arc<AtomicU64>,
	active_actor_requests: Arc<AtomicU64>,
	tenant_cache_refresh_interval: Duration,
	/// Max records per `RANGE_PAGE` pull. Defaults to
	/// [`DEFAULT_PAGE_MAX_RECORDS`]; tests shrink it so a paging
	/// regression can be exercised without seeding thousands of
	/// records per source. `AtomicU32` so the test-only setter
	/// can tune it after the server is wrapped in `Arc`.
	page_max_records: Arc<std::sync::atomic::AtomicU32>,
	/// Max arrow payload bytes per `RANGE_PAGE` pull. Same rationale
	/// as `page_max_records`.
	page_max_bytes: Arc<std::sync::atomic::AtomicU32>,
	/// Node-local ADR 054 CDC subscription registry.
	cdc_registry: Arc<CdcSubscriptionRegistry>,
	/// Per-source-locality live-tail rings. Created lazily as local
	/// table writes finalize on this node.
	cdc_live_tail_rings: Arc<DashMap<Locality, Arc<CdcLiveTailRing>>>,
	/// Per-source-locality CDC finalization frontier used as the
	/// safe-emission watermark for live tail streams.
	cdc_locality_frontiers: Arc<CdcLocalityFrontiers>,
	/// Admission-staged CDC rows waiting for the existing ADR 003
	/// pending finalization record to commit on the control ring.
	cdc_pending_live_entries: Arc<DashMap<PendingFinalizationId, Vec<CdcStagedEntryRef>>>,
	/// Replica-staged CDC rows waiting for a parent table finalization
	/// marker to apply locally. Split table replica frames cannot safely
	/// persist a parent pending-finalization witness, but they can release
	/// their local rows once that parent marker appears on the control ring.
	cdc_marker_pending_live_entries: Arc<DashMap<CdcFinalizationKey, Vec<CdcStagedEntryRef>>>,
	/// Locality-scoped heartbeat registry. This is intentionally
	/// keyed only by locality, not by `(locality, table)`: one quiet
	/// locality marker advances the CDC watermark for every table
	/// whose source-locality set includes that locality.
	cdc_heartbeat_localities: Arc<DashMap<Locality, ()>>,
	/// Last frontier value observed by the heartbeat quiet check.
	/// If the frontier advances naturally between ticks, the worker
	/// records the new value and skips the synthetic marker for that
	/// interval.
	cdc_heartbeat_observed_frontiers: Arc<DashMap<Locality, HybridTimestamp>>,
}

/// Default max records a single `RANGE_PAGE` response can emit
/// before the server sets `has_more = 1`.
pub const DEFAULT_PAGE_MAX_RECORDS: u32 = 1024;
/// Default max arrow bytes per `RANGE_PAGE` response.
pub const DEFAULT_PAGE_MAX_BYTES: u32 = 4 * 1024 * 1024;
pub const DEFAULT_TENANT_MAX_INFLIGHT_WRITES: u32 = 8;
pub const DEFAULT_TENANT_CACHE_REFRESH_INTERVAL: Duration = Duration::from_millis(100);
pub const DEFAULT_SECONDARY_INDEX_ACTIVE_BUFFER_BYTES: usize = 8 * 1024 * 1024;
const CDC_SOURCE_LOCALITY_ENUMERATION_BUDGET: u64 = 1024;
const CDC_CHECKPOINT_EVENT_CADENCE: u64 = 1024;
const CDC_CHECKPOINT_WALL_CADENCE: Duration = Duration::from_secs(1);
/// Per-subscriber queue depth for committed finalization markers.
pub const STREAM_FINALIZATION_BUFFER: usize = 128;
/// Default wake cadence for the background migration sweeper.
pub const MIGRATION_SWEEP_INTERVAL: Duration = Duration::from_secs(5);
/// Retry delay after a migration sweep pass fails for one entry.
pub const MIGRATION_SWEEP_FAILURE_BACKOFF: Duration = Duration::from_secs(2);
/// Retry cadence for ADR 003 control-branch recovery. Kept short
/// because a pending row is already quorum-durable in the slabs;
/// the remaining work is only the Raft metadata commit.
pub const PENDING_FINALIZATION_RETRY_INTERVAL: Duration = Duration::from_millis(200);
const LOCAL_PERSISTENCE_DISTRUST_REASON: &str = "LocalPersistenceDistrust";
const LOCAL_PERSISTENCE_DISTRUST_FILE: &str = "local_persistence_distrust.flag";
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

fn local_persistence_distrust_path(dir: &std::path::Path) -> PathBuf {
	dir.join(LOCAL_PERSISTENCE_DISTRUST_FILE)
}

fn recover_rwlock_read<'a, T>(
	lock: &'a RwLock<T>,
	name: &'static str,
) -> std::sync::RwLockReadGuard<'a, T> {
	match lock.read() {
		Ok(guard) => guard,
		Err(poisoned) => {
			tracing::error!(lock = name, "recovering poisoned rwlock read guard");
			poisoned.into_inner()
		}
	}
}

fn recover_rwlock_write<'a, T>(
	lock: &'a RwLock<T>,
	name: &'static str,
) -> std::sync::RwLockWriteGuard<'a, T> {
	match lock.write() {
		Ok(guard) => guard,
		Err(poisoned) => {
			tracing::error!(lock = name, "recovering poisoned rwlock write guard");
			poisoned.into_inner()
		}
	}
}

#[derive(Debug)]
struct CdcCheckpointCadence {
	events_since_checkpoint: u64,
	last_checkpoint_at: Instant,
}

impl CdcCheckpointCadence {
	fn new(now: Instant) -> Self {
		Self {
			events_since_checkpoint: 0,
			last_checkpoint_at: now,
		}
	}

	fn note_events(&mut self, event_count: u64) {
		self.events_since_checkpoint = self.events_since_checkpoint.saturating_add(event_count);
	}

	fn checkpoint_due(&self, now: Instant) -> bool {
		self.events_since_checkpoint >= CDC_CHECKPOINT_EVENT_CADENCE
			|| now.duration_since(self.last_checkpoint_at) >= CDC_CHECKPOINT_WALL_CADENCE
	}

	fn mark_checkpoint(&mut self, now: Instant) {
		self.events_since_checkpoint = 0;
		self.last_checkpoint_at = now;
	}
}

impl Clone for WendelonDataServer {
	fn clone(&self) -> Self {
		Self {
			slab: self.slab.clone(),
			raft: self.raft.clone(),
			log_store: self.log_store.clone(),
			state_machine: self.state_machine.clone(),
			admission_snapshot_rx: self.admission_snapshot_rx.clone(),
			local_node: self.local_node,
			clock: self.clock.clone(),
			audit_sink: self.audit_sink.clone(),
			mvcc_retention_policy: self.mvcc_retention_policy,
			router: self.router.clone(),
			factory: self.factory.clone(),
			pool: self.pool.clone(),
			idempotency: self.idempotency.clone(),
			table_delete_idempotency: self.table_delete_idempotency.clone(),
			content_hash_admission_locks: self.content_hash_admission_locks.clone(),
			pending_finalizations: self.pending_finalizations.clone(),
			pending_finalization_claims: self.pending_finalization_claims.clone(),
			pending_finalization_wakeup: self.pending_finalization_wakeup.clone(),
			orphan_witness_recovery_stamp: self.orphan_witness_recovery_stamp.clone(),
			orphan_witness_recovery_lock: self.orphan_witness_recovery_lock.clone(),
			local_persistence_distrust_reason: self.local_persistence_distrust_reason.clone(),
			local_persistence_distrust_path: self.local_persistence_distrust_path.clone(),
			#[cfg(test)]
			fail_next_local_durable_append: self.fail_next_local_durable_append.clone(),
			hlc: self.hlc.clone(),
			tracker: self.tracker.clone(),
			leases: self.leases.clone(),
			lease_gate_enabled: self.lease_gate_enabled,
			lease_authority: self.lease_authority.clone(),
			migration_registry: self.migration_registry.clone(),
			tenant_policy_cache: self.tenant_policy_cache.clone(),
			tenant_inflight: self.tenant_inflight.clone(),
			tenant_request_inflight: self.tenant_request_inflight.clone(),
			actor_request_inflight: self.actor_request_inflight.clone(),
			max_tenant_concurrent_requests: self.max_tenant_concurrent_requests.clone(),
			max_actor_concurrent_requests: self.max_actor_concurrent_requests.clone(),
			active_tenant_requests: self.active_tenant_requests.clone(),
			active_actor_requests: self.active_actor_requests.clone(),
			tenant_cache_refresh_interval: self.tenant_cache_refresh_interval,
			page_max_records: self.page_max_records.clone(),
			page_max_bytes: self.page_max_bytes.clone(),
			cdc_registry: self.cdc_registry.clone(),
			cdc_live_tail_rings: self.cdc_live_tail_rings.clone(),
			cdc_locality_frontiers: self.cdc_locality_frontiers.clone(),
			cdc_pending_live_entries: self.cdc_pending_live_entries.clone(),
			cdc_marker_pending_live_entries: self.cdc_marker_pending_live_entries.clone(),
			cdc_heartbeat_localities: self.cdc_heartbeat_localities.clone(),
			cdc_heartbeat_observed_frontiers: self.cdc_heartbeat_observed_frontiers.clone(),
		}
	}
}

impl WendelonDataServer {
	fn admission_snapshot(&self) -> Arc<AdmissionSnapshot> {
		self.admission_snapshot_rx.borrow().clone()
	}

	async fn content_hash_admission_guard(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
	) -> OwnedMutexGuard<()> {
		let lock = match self
			.content_hash_admission_locks
			.entry((tenant_id, table_object_id))
		{
			DashEntry::Occupied(entry) => entry.get().clone(),
			DashEntry::Vacant(entry) => entry.insert(Arc::new(tokio::sync::Mutex::new(()))).clone(),
		};
		lock.lock_owned().await
	}

	fn refresh_pending_finalization_backlog_metric(&self) {
		metrics().set_pending_finalization_backlog(self.pending_finalizations.count());
	}

	fn counted_write_response(&self, response: WriteResponseMeta) -> WriteResponseMeta {
		metrics().observe_write_outcome(response.status);
		response
	}

	fn counted_write_result(
		&self,
		response: WriteResponseMeta,
		trailing: Option<FinalizationMarker>,
	) -> (WriteResponseMeta, Option<FinalizationMarker>) {
		metrics().observe_write_outcome(response.status);
		(response, trailing)
	}

	fn emit_data_access_audit(
		&self,
		actor: &ActorIdentity,
		tenant_id: TenantId,
		record_identity: Option<Identity>,
		operation: AuditOperation,
		access_kind: DataAccessKind,
		phase: DataAccessPhase,
		detail: DataAccessSchema,
		outcome: Option<AuditOutcome>,
		message: impl Into<String>,
		parameters: Vec<(&'static str, serde_json::Value)>,
	) -> std::io::Result<()> {
		let mut event = AuditEvent::new(
			AuditAuthority::NodeLocal,
			operation,
			self.local_node,
			message,
		)
		.with_actor(actor)
		.with_tenant(tenant_id)
		.with_schema(AuditSchema::DataAccess {
			access_kind,
			phase,
			detail,
		})
		.with_parameters(parameters);
		if let Some(record_identity) = record_identity {
			event = event.with_record_identity(record_identity);
		}
		if let Some(outcome) = outcome {
			event = event.with_outcome(outcome);
		}
		self.audit_sink
			.emit(event)
			.map_err(|err| Error::other(format!("data access audit emit failed: {err}")))
	}

	fn emit_write_requested_audit(
		&self,
		actor: &ActorIdentity,
		meta: &WriteRequestMeta,
		charged_bytes: u64,
	) -> std::io::Result<()> {
		self.emit_data_access_audit(
			actor,
			meta.tenant_id,
			Some(meta.key_identity),
			AuditOperation::DataWrite,
			DataAccessKind::Write,
			DataAccessPhase::Requested,
			DataAccessSchema {
				locality: Some(Locality::from(meta.key_identity).get()),
				locality_end: None,
				durability: Some(meta.durability.0 as u64),
				charged_bytes: Some(charged_bytes),
				returned_bytes: None,
				result_count: None,
			},
			None,
			"requested data write",
			vec![
				("expected_epoch", json!(meta.expected_epoch)),
				("write_sequence", json!(meta.write_id.sequence_no)),
				("await_finalization", json!(meta.await_finalization != 0)),
			],
		)
	}

	fn emit_write_completion_audit(
		&self,
		actor: &ActorIdentity,
		meta: &WriteRequestMeta,
		response: &WriteResponseMeta,
		charged_bytes: u64,
	) -> std::io::Result<()> {
		self.emit_data_access_audit(
			actor,
			meta.tenant_id,
			Some(meta.key_identity),
			AuditOperation::DataWrite,
			DataAccessKind::Write,
			DataAccessPhase::Completed,
			DataAccessSchema {
				locality: Some(Locality::from(meta.key_identity).get()),
				locality_end: None,
				durability: Some(meta.durability.0 as u64),
				charged_bytes: Some(charged_bytes),
				returned_bytes: None,
				result_count: Some(1),
			},
			completed_write_audit_outcome(response.status),
			"completed data write request",
			vec![
				("status", json!(response.status.as_str())),
				("log_index", json!(response.log_index)),
				("term", json!(response.term)),
				("has_finalization", json!(response.has_finalization != 0)),
				(
					"commit_hlc_physical_ms",
					json!(response.commit_hlc.physical_ms),
				),
				("commit_hlc_logical", json!(response.commit_hlc.logical)),
				("commit_hlc_node_id", json!(response.commit_hlc.node_id)),
			],
		)
	}

	fn emit_write_denied_audit(
		&self,
		actor: &ActorIdentity,
		meta: &WriteRequestMeta,
		status: WriteStatus,
		reason: impl Into<String>,
	) -> std::io::Result<()> {
		self.emit_data_access_audit(
			actor,
			meta.tenant_id,
			Some(meta.key_identity),
			AuditOperation::DataWrite,
			DataAccessKind::Write,
			DataAccessPhase::Denied,
			DataAccessSchema {
				locality: Some(Locality::from(meta.key_identity).get()),
				locality_end: None,
				durability: Some(meta.durability.0 as u64),
				charged_bytes: None,
				returned_bytes: None,
				result_count: None,
			},
			Some(AuditOutcome::Denied),
			"denied data write request before execution",
			vec![
				("status", json!(status.as_str())),
				("reason", json!(reason.into())),
			],
		)
	}

	fn emit_table_delete_requested_audit(
		&self,
		actor: &ActorIdentity,
		meta: &TableDeleteRowsRequestMeta,
		distinct_target_count: usize,
	) -> std::io::Result<()> {
		self.emit_data_access_audit(
			actor,
			meta.tenant_id,
			None,
			AuditOperation::DataWrite,
			DataAccessKind::Write,
			DataAccessPhase::Requested,
			DataAccessSchema {
				locality: None,
				locality_end: None,
				durability: Some(meta.durability.0 as u64),
				charged_bytes: None,
				returned_bytes: None,
				result_count: Some(meta.row_count as u64),
			},
			None,
			"requested table row delete",
			vec![
				(
					"table_object_id",
					json!(format_bytes_hex(meta.table_object_id.as_bytes())),
				),
				("schema_generation", json!(meta.schema_generation)),
				("expected_epoch", json!(meta.expected_epoch)),
				("write_sequence", json!(meta.parent_write_id.sequence_no)),
				("await_finalization", json!(meta.await_finalization != 0)),
				("row_count", json!(meta.row_count)),
				("distinct_target_count", json!(distinct_target_count)),
			],
		)
	}

	fn emit_table_delete_completion_audit(
		&self,
		actor: &ActorIdentity,
		meta: &TableDeleteRowsRequestMeta,
		response: &TableDeleteRowsResponseMeta,
		distinct_target_count: usize,
	) -> std::io::Result<()> {
		self.emit_data_access_audit(
			actor,
			meta.tenant_id,
			None,
			AuditOperation::DataWrite,
			DataAccessKind::Write,
			DataAccessPhase::Completed,
			DataAccessSchema {
				locality: None,
				locality_end: None,
				durability: Some(meta.durability.0 as u64),
				charged_bytes: None,
				returned_bytes: None,
				result_count: Some(response.deleted_count as u64),
			},
			completed_write_audit_outcome(response.write.status),
			"completed table row delete",
			vec![
				(
					"table_object_id",
					json!(format_bytes_hex(meta.table_object_id.as_bytes())),
				),
				("schema_generation", json!(meta.schema_generation)),
				("status", json!(response.write.status.as_str())),
				(
					"reject_code",
					json!(PbcRejectCode(response.reject_code).name()),
				),
				("row_count", json!(meta.row_count)),
				("distinct_target_count", json!(distinct_target_count)),
				("deleted_count", json!(response.deleted_count)),
				(
					"already_deleted_count",
					json!(response.already_deleted_count),
				),
				("not_found_count", json!(response.not_found_count)),
				("log_index", json!(response.write.log_index)),
				("term", json!(response.write.term)),
				(
					"has_finalization",
					json!(response.write.has_finalization != 0),
				),
				(
					"commit_hlc_physical_ms",
					json!(response.write.commit_hlc.physical_ms),
				),
				(
					"commit_hlc_logical",
					json!(response.write.commit_hlc.logical),
				),
				(
					"commit_hlc_node_id",
					json!(response.write.commit_hlc.node_id),
				),
			],
		)
	}

	fn emit_read_completion_audit(
		&self,
		actor: &ActorIdentity,
		meta: &ReadRequestMeta,
		response: &ReadResponseMeta,
	) -> std::io::Result<()> {
		let result_count = if response.status == ReadStatus::OK {
			1
		} else {
			0
		};
		self.emit_data_access_audit(
			actor,
			meta.tenant_id,
			Some(meta.key_identity),
			AuditOperation::DataRead,
			DataAccessKind::Read,
			DataAccessPhase::Completed,
			DataAccessSchema {
				locality: Some(Locality::from(meta.key_identity).get()),
				locality_end: None,
				durability: None,
				charged_bytes: None,
				returned_bytes: Some(response.value_len as u64),
				result_count: Some(result_count),
			},
			completed_read_audit_outcome(response.status),
			"completed data read request",
			vec![
				("status", json!(response.status.as_str())),
				("consistency", json!(meta.consistency.0)),
				("expected_epoch", json!(meta.expected_epoch)),
				(
					"value_hlc_physical_ms",
					json!(response.value_hlc.physical_ms),
				),
				("value_hlc_logical", json!(response.value_hlc.logical)),
				("value_hlc_node_id", json!(response.value_hlc.node_id)),
			],
		)
	}

	fn emit_read_denied_audit(
		&self,
		actor: &ActorIdentity,
		meta: &ReadRequestMeta,
		status: ReadStatus,
		reason: impl Into<String>,
	) -> std::io::Result<()> {
		self.emit_data_access_audit(
			actor,
			meta.tenant_id,
			Some(meta.key_identity),
			AuditOperation::DataRead,
			DataAccessKind::Read,
			DataAccessPhase::Denied,
			DataAccessSchema {
				locality: Some(Locality::from(meta.key_identity).get()),
				locality_end: None,
				durability: None,
				charged_bytes: None,
				returned_bytes: None,
				result_count: None,
			},
			Some(AuditOutcome::Denied),
			"denied data read request before execution",
			vec![
				("status", json!(status.as_str())),
				("reason", json!(reason.into())),
			],
		)
	}

	fn emit_query_completion_audit(
		&self,
		actor: &ActorIdentity,
		meta: &QueryRequestMeta,
		response: &QueryResponseMeta,
		returned_bytes: u64,
	) -> std::io::Result<()> {
		self.emit_data_access_audit(
			actor,
			meta.tenant_id,
			None,
			AuditOperation::DataRangeQuery,
			DataAccessKind::RangeQuery,
			DataAccessPhase::Completed,
			DataAccessSchema {
				locality: Some(meta.locality_range_start),
				locality_end: Some(meta.locality_range_end),
				durability: None,
				charged_bytes: None,
				returned_bytes: Some(returned_bytes),
				result_count: Some(response.rows),
			},
			completed_read_audit_outcome(response.status),
			"completed data range query request",
			vec![
				("status", json!(response.status.as_str())),
				("consistency", json!(meta.consistency.0)),
				("expected_epoch", json!(meta.expected_epoch)),
				("bytecode_len", json!(meta.bytecode_len)),
				(
					"value_hlc_physical_ms",
					json!(response.value_hlc.physical_ms),
				),
				("value_hlc_logical", json!(response.value_hlc.logical)),
				("value_hlc_node_id", json!(response.value_hlc.node_id)),
			],
		)
	}

	fn emit_query_denied_audit(
		&self,
		actor: &ActorIdentity,
		meta: &QueryRequestMeta,
		status: ReadStatus,
		reason: impl Into<String>,
	) -> std::io::Result<()> {
		self.emit_data_access_audit(
			actor,
			meta.tenant_id,
			None,
			AuditOperation::DataRangeQuery,
			DataAccessKind::RangeQuery,
			DataAccessPhase::Denied,
			DataAccessSchema {
				locality: Some(meta.locality_range_start),
				locality_end: Some(meta.locality_range_end),
				durability: None,
				charged_bytes: None,
				returned_bytes: None,
				result_count: None,
			},
			Some(AuditOutcome::Denied),
			"denied data range query request before execution",
			vec![
				("status", json!(status.as_str())),
				("reason", json!(reason.into())),
			],
		)
	}

	fn emit_cdc_subscription_opened_audit(
		&self,
		actor: &ActorIdentity,
		req: &CdcSubscribeRequestMeta,
		subscription_id: CdcSubscriptionId,
		bound: CdcSubscribeBound,
	) -> std::io::Result<()> {
		let audit = AuditEvent::new(
			AuditAuthority::NodeLocal,
			AuditOperation::CdcSubscriptionOpened,
			self.local_node,
			"cdc subscription opened",
		)
		.with_actor(actor)
		.with_tenant(req.tenant_id)
		.with_parameters(vec![
			("subscription_id", json!(subscription_id.to_string())),
			(
				"table_object_id",
				json!(format_bytes_hex(req.table_object_id.as_bytes())),
			),
			("start_cursor", cdc_cursor_audit_json(req.start_cursor)),
			("bound", json!(cdc_bound_audit_str(bound))),
			("opened_at_hlc", cdc_hlc_audit_json(self.cdc_audit_hlc())),
		]);
		self.audit_sink
			.emit(audit)
			.map_err(|err| Error::other(format!("cdc subscription open audit emit failed: {err}")))
	}

	fn emit_cdc_subscription_closed_audit(
		&self,
		actor: &ActorIdentity,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		subscription_id: CdcSubscriptionId,
		reason: &'static str,
		final_cursor: CdcCursor,
	) -> std::io::Result<()> {
		let audit = AuditEvent::new(
			AuditAuthority::NodeLocal,
			AuditOperation::CdcSubscriptionClosed,
			self.local_node,
			"cdc subscription closed",
		)
		.with_actor(actor)
		.with_tenant(tenant_id)
		.with_parameters(vec![
			("subscription_id", json!(subscription_id.to_string())),
			(
				"table_object_id",
				json!(format_bytes_hex(table_object_id.as_bytes())),
			),
			("reason", json!(reason)),
			("final_cursor", cdc_cursor_audit_json(final_cursor)),
			("closed_at_hlc", cdc_hlc_audit_json(self.cdc_audit_hlc())),
		]);
		self.audit_sink
			.emit(audit)
			.map_err(|err| Error::other(format!("cdc subscription close audit emit failed: {err}")))
	}

	fn cdc_audit_hlc(&self) -> HybridTimestamp {
		HybridTimestamp {
			physical_ms: self.clock.now_ms(),
			logical: 0,
			node_id: self.local_node.id,
		}
	}

	fn apply_local_persistence_runtime_fence(&self) {
		let runtime = self.raft.runtime_config();
		runtime.heartbeat(false);
		runtime.elect(false);
		runtime.tick(false);
	}

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
	) -> std::io::Result<Self> {
		let hlc = Arc::new(HybridLogicalClockDriver::new(local_node, clock.clone()));
		let pending_finalizations =
			PendingFinalizationStore::open(pending_finalizations_path(slab.data_dir()))?;
		let admission_snapshot_rx = state_machine.subscribe_admission_snapshot();
		let local_persistence_distrust_path = local_persistence_distrust_path(slab.data_dir());
		let local_persistence_distrust_reason = if local_persistence_distrust_path.exists() {
			Some(LOCAL_PERSISTENCE_DISTRUST_REASON.to_string())
		} else {
			None
		};
		let server = Self {
			slab,
			raft,
			log_store,
			state_machine,
			admission_snapshot_rx,
			local_node,
			clock,
			audit_sink: Arc::new(NoopAuditSink),
			mvcc_retention_policy: MvccRetentionPolicy::Disabled,
			router,
			factory,
			pool,
			idempotency: Arc::new(DashMap::new()),
			table_delete_idempotency: Arc::new(DashMap::new()),
			content_hash_admission_locks: Arc::new(DashMap::new()),
			pending_finalizations,
			pending_finalization_claims: PendingFinalizationClaims::default(),
			pending_finalization_wakeup: Arc::new(Notify::new()),
			orphan_witness_recovery_stamp: Arc::new(RwLock::new(None)),
			orphan_witness_recovery_lock: Arc::new(tokio::sync::Mutex::new(())),
			local_persistence_distrust_reason: Arc::new(RwLock::new(
				local_persistence_distrust_reason,
			)),
			local_persistence_distrust_path,
			#[cfg(test)]
			fail_next_local_durable_append: Arc::new(StdMutex::new(None)),
			hlc,
			tracker: None,
			leases: Arc::new(RwLock::new(Vec::new())),
			lease_gate_enabled: false,
			lease_authority: Arc::new(LeaseAuthority::dev_single_node(local_node)),
			migration_registry: Arc::new(MigrationRegistry::new()),
			tenant_policy_cache: Arc::new(RwLock::new(TenantPolicyCacheState::default())),
			tenant_inflight: Arc::new(DashMap::new()),
			tenant_request_inflight: Arc::new(DashMap::new()),
			actor_request_inflight: Arc::new(DashMap::new()),
			max_tenant_concurrent_requests: Arc::new(AtomicU32::new(
				DataRequestBackpressureConfig::default().max_tenant_concurrent_requests,
			)),
			max_actor_concurrent_requests: Arc::new(AtomicU32::new(
				DataRequestBackpressureConfig::default().max_actor_concurrent_requests,
			)),
			active_tenant_requests: Arc::new(AtomicU64::new(0)),
			active_actor_requests: Arc::new(AtomicU64::new(0)),
			tenant_cache_refresh_interval: DEFAULT_TENANT_CACHE_REFRESH_INTERVAL,
			page_max_records: Arc::new(std::sync::atomic::AtomicU32::new(DEFAULT_PAGE_MAX_RECORDS)),
			page_max_bytes: Arc::new(std::sync::atomic::AtomicU32::new(DEFAULT_PAGE_MAX_BYTES)),
			cdc_registry: Arc::new(CdcSubscriptionRegistry::new()),
			cdc_live_tail_rings: Arc::new(DashMap::new()),
			cdc_locality_frontiers: Arc::new(CdcLocalityFrontiers::new()),
			cdc_pending_live_entries: Arc::new(DashMap::new()),
			cdc_marker_pending_live_entries: Arc::new(DashMap::new()),
			cdc_heartbeat_localities: Arc::new(DashMap::new()),
			cdc_heartbeat_observed_frontiers: Arc::new(DashMap::new()),
		};
		server.refresh_pending_finalization_backlog_metric();
		if server.degraded_reason().is_some() {
			tracing::error!(
				node_id = server.local_node.id,
				path = %server.local_persistence_distrust_path.display(),
				"restoring persisted local-persistence distrust fence at bootstrap"
			);
			server.apply_local_persistence_runtime_fence();
		}
		Ok(server)
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

	pub fn with_hlc(mut self, hlc: Arc<HybridLogicalClockDriver>) -> Self {
		self.hlc = hlc;
		self
	}

	pub fn with_audit_sink(mut self, audit_sink: Arc<dyn AuditSink>) -> Self {
		self.audit_sink = audit_sink;
		self
	}

	pub fn with_mvcc_retention_policy(mut self, retention_policy: MvccRetentionPolicy) -> Self {
		self.mvcc_retention_policy = retention_policy;
		self
	}

	pub fn with_mvcc_retention_window(mut self, retention_window: Option<Duration>) -> Self {
		self.mvcc_retention_policy = MvccRetentionPolicy::from_optional_window(retention_window);
		self
	}

	pub fn with_request_backpressure(
		self,
		config: DataRequestBackpressureConfig,
	) -> std::io::Result<Self> {
		config.validate()?;
		self.max_tenant_concurrent_requests
			.store(config.max_tenant_concurrent_requests, Ordering::Relaxed);
		self.max_actor_concurrent_requests
			.store(config.max_actor_concurrent_requests, Ordering::Relaxed);
		Ok(self)
	}

	#[cfg(test)]
	pub fn set_request_backpressure_limits_for_test(&self, tenant_max: u32, actor_max: u32) {
		assert!(tenant_max > 0);
		assert!(actor_max > 0);
		self.max_tenant_concurrent_requests
			.store(tenant_max, Ordering::Relaxed);
		self.max_actor_concurrent_requests
			.store(actor_max, Ordering::Relaxed);
	}

	pub fn degraded_reason(&self) -> Option<String> {
		recover_rwlock_read(
			&self.local_persistence_distrust_reason,
			"local_persistence_distrust_reason",
		)
		.clone()
	}

	async fn persist_local_persistence_distrust_marker(
		&self,
		detail: String,
	) -> std::io::Result<()> {
		let path = self.local_persistence_distrust_path.clone();
		spawn_blocking(move || std::fs::write(path, detail))
			.await
			.map_err(|err| {
				Error::other(format!(
					"local persistence distrust marker task failed: {err}"
				))
			})?
	}

	async fn trip_local_persistence_distrust(&self, detail: impl Into<String>) {
		let detail = detail.into();
		let tripped = {
			let mut guard = recover_rwlock_write(
				&self.local_persistence_distrust_reason,
				"local_persistence_distrust_reason",
			);
			if guard.is_some() {
				false
			} else {
				*guard = Some(LOCAL_PERSISTENCE_DISTRUST_REASON.to_string());
				true
			}
		};
		if !tripped {
			return;
		}

		tracing::error!(
			node_id = self.local_node.id,
			detail = %detail,
			"fencing data writes after local persistence distrust"
		);

		if let Err(err) = self
			.persist_local_persistence_distrust_marker(detail.clone())
			.await
		{
			tracing::error!(
				node_id = self.local_node.id,
				path = %self.local_persistence_distrust_path.display(),
				error = %err,
				"failed to persist local-persistence distrust marker"
			);
		}

		self.apply_local_persistence_runtime_fence();
		if let Err(err) = self.raft.shutdown().await {
			tracing::error!(
				node_id = self.local_node.id,
				error = %err,
				"failed to shut down raft participation after local persistence distrust"
			);
		}
	}

	async fn fenced_write_response(
		&self,
		meta: &WriteRequestMeta,
	) -> (WriteResponseMeta, Option<FinalizationMarker>) {
		let leader_hint = self
			.raft
			.current_leader()
			.await
			.map(|id| NodeId { id, _reserved: 0 })
			.unwrap_or(self.local_node);
		let status = if leader_hint.id != self.local_node.id {
			WriteStatus::NOT_LEADER
		} else {
			WriteStatus::INTERNAL_ERROR
		};
		self.counted_write_result(error_response(meta, status, leader_hint), None)
	}

	#[cfg(test)]
	pub(crate) fn fail_next_local_durable_append_for_test(
		&self,
		kind: ErrorKind,
		message: impl Into<String>,
	) {
		*self
			.fail_next_local_durable_append
			.lock()
			.expect("local durable append failpoint poisoned") = Some((kind, message.into()));
	}

	#[cfg(test)]
	pub(crate) async fn trip_local_persistence_distrust_for_test(&self) {
		self.trip_local_persistence_distrust("test-only local persistence distrust")
			.await;
	}

	fn mvcc_retention_floor_hlc(&self) -> Option<HybridTimestamp> {
		cluster_retention_floor(self.mvcc_retention_policy, self.clock.now_ms())
	}

	fn table_effective_retention_from_state(
		&self,
		state: &crate::consensus::raft_storage::SmState,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
	) -> EffectiveRetention {
		effective_table_retention(
			&self.mvcc_retention_policy,
			state
				.table_lifecycle
				.retention_policy(tenant_id, table_object_id),
			self.clock.now_ms(),
		)
	}

	fn snapshot_request_outside_retention(&self, as_of_hlc: HybridTimestamp) -> bool {
		snapshot_request_outside_retention(as_of_hlc, self.mvcc_retention_floor_hlc())
	}

	async fn reclaim_mvcc_retention_once(
		&self,
		max_slabs: usize,
	) -> std::io::Result<crate::storage::manager::RetentionPruningReport> {
		let retention_floor = self.mvcc_retention_floor_hlc();
		let snapshot = self.state_machine.state_snapshot().await;
		let report = self
			.slab
			.prune_versions_for_retention(
				self.mvcc_retention_policy,
				&snapshot.table_lifecycle,
				&snapshot.table_schemas,
				&snapshot.secondary_indexes,
				self.clock.now_ms(),
				max_slabs,
			)
			.await?;
		if report.pruned_versions > 0 {
			let _permit =
				authorize_autonomous_action(AutonomousActionSurface::L1MvccRetentionPrune);
			let retention_floor_ms = retention_floor
				.map(|floor| floor.physical_ms)
				.unwrap_or_else(|| self.clock.now_ms());
			let audit = AuditEvent::new(
				AuditAuthority::NodeLocal,
				AuditOperation::MvccSnapshotPruned,
				self.local_node,
				format!(
					"retention_floor_ms={} compacted_slabs={} pruned_versions={} reclaimed_bytes={}",
					retention_floor_ms,
					report.compacted_slabs,
					report.pruned_versions,
					report.reclaimed_bytes,
				),
			)
			.with_parameter("retention_floor_ms", retention_floor_ms)
			.with_parameter("compacted_slabs", report.compacted_slabs as u64)
			.with_parameter("pruned_versions", report.pruned_versions as u64)
			.with_parameter("reclaimed_bytes", report.reclaimed_bytes);
			if let Err(err) = self.audit_sink.emit(audit) {
				tracing::warn!(
					error = %err,
					"mvcc retention prune audit emission failed"
				);
			}
		}
		if report.pruned_versions > 0
			|| report.max_lag_seconds > 0
			|| report.ambiguous_rows_retained > 0
		{
			metrics().observe_storage_table_retention_l1_prune(
				report.pruned_data_rows as u64,
				report.pruned_tombstone_rows as u64,
				report.reclaimed_bytes,
				report.pair_drops as u64,
				report.ambiguous_rows_retained as u64,
				report.max_lag_seconds,
			);
		}
		Ok(report)
	}

	pub async fn sync_tenant_policy_cache_once(&self) -> usize {
		let snapshot = self.state_machine.state_snapshot().await;
		let mut guard = recover_rwlock_write(&self.tenant_policy_cache, "tenant_policy_cache");
		let mut next = HashMap::with_capacity(snapshot.tenants.len());
		for (tenant_id, record) in snapshot.tenants {
			let mean_write_size_bytes = guard
				.policies
				.get(&tenant_id)
				.map(|cached| cached.mean_write_size_bytes)
				.unwrap_or(1)
				.max(1);
			next.insert(
				tenant_id,
				CachedTenantPolicy {
					state: record.state,
					quota_bytes: record.quota_bytes,
					used_bytes: record.used_bytes,
					replication_factor: record.replication_factor,
					mean_write_size_bytes,
				},
			);
		}
		guard.policies = next;
		guard.last_refresh = Some(Instant::now());
		guard.policies.len()
	}

	pub fn spawn_tenant_policy_reconciler(self: Arc<Self>, tick: Duration) -> JoinHandle<()> {
		tokio::spawn(async move {
			let _ = self.sync_tenant_policy_cache_once().await;
			let mut ticker = tokio::time::interval(tick);
			ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
			loop {
				ticker.tick().await;
				let _ = self.sync_tenant_policy_cache_once().await;
			}
		})
	}

	/// Shared handle to the lease cache. The lease manager writes
	/// to it; the write/read paths read from it. Exposed so the
	/// manager task (which lives in a separate module) can take an
	/// `Arc` without poking at private fields.
	pub fn lease_cache(&self) -> Arc<RwLock<Vec<LeaseDescriptor>>> {
		self.leases.clone()
	}

	async fn ensure_tenant_policy_fresh(&self) {
		let refresh_needed = {
			let guard = recover_rwlock_read(&self.tenant_policy_cache, "tenant_policy_cache");
			guard
				.last_refresh
				.is_none_or(|last| last.elapsed() >= self.tenant_cache_refresh_interval)
		};
		if refresh_needed {
			let _ = self.sync_tenant_policy_cache_once().await;
		}
	}

	async fn cached_tenant_policy(&self, tenant_id: TenantId) -> Option<CachedTenantPolicy> {
		self.ensure_tenant_policy_fresh().await;
		recover_rwlock_read(&self.tenant_policy_cache, "tenant_policy_cache")
			.policies
			.get(&tenant_id)
			.copied()
	}

	async fn enforce_write_policy(
		&self,
		tenant_id: TenantId,
		estimated_bytes: u64,
	) -> Result<TenantWriteReservation, WriteStatus> {
		let Some(policy) = self.cached_tenant_policy(tenant_id).await else {
			return Err(WriteStatus::TENANT_UNAVAILABLE);
		};
		match policy.state {
			TenantLifecycleState::Active => {}
			TenantLifecycleState::Suspended => return Err(WriteStatus::TENANT_SUSPENDED),
			TenantLifecycleState::Creating
			| TenantLifecycleState::Deleting
			| TenantLifecycleState::Deleted => return Err(WriteStatus::TENANT_UNAVAILABLE),
		}
		if policy.used_bytes >= policy.quota_bytes {
			return Err(WriteStatus::QUOTA_EXCEEDED);
		}

		let inflight = self
			.tenant_inflight
			.get(&tenant_id)
			.map(|entry| *entry.value())
			.unwrap_or_default();
		if inflight.count >= DEFAULT_TENANT_MAX_INFLIGHT_WRITES {
			return Err(WriteStatus::QUOTA_EXCEEDED);
		}
		let mean_write_size = policy.mean_write_size_bytes.max(estimated_bytes).max(1);
		let overshoot_budget =
			(DEFAULT_TENANT_MAX_INFLIGHT_WRITES as u64).saturating_mul(mean_write_size);
		let projected = policy
			.used_bytes
			.saturating_add(inflight.bytes)
			.saturating_add(estimated_bytes);
		if projected > policy.quota_bytes.saturating_add(overshoot_budget) {
			return Err(WriteStatus::QUOTA_EXCEEDED);
		}

		self.tenant_inflight
			.entry(tenant_id)
			.and_modify(|entry| {
				entry.count = entry.count.saturating_add(1);
				entry.bytes = entry.bytes.saturating_add(estimated_bytes);
			})
			.or_insert(TenantInflight {
				count: 1,
				bytes: estimated_bytes,
			});
		Ok(TenantWriteReservation {
			inflight: self.tenant_inflight.clone(),
			tenant_id,
			bytes: estimated_bytes,
			active: true,
		})
	}

	async fn enforce_read_policy(&self, tenant_id: TenantId) -> Result<(), ReadStatus> {
		let Some(policy) = self.cached_tenant_policy(tenant_id).await else {
			return Err(ReadStatus::TENANT_UNAVAILABLE);
		};
		if policy.state.allows_reads() {
			return Ok(());
		}
		Err(ReadStatus::TENANT_UNAVAILABLE)
	}

	fn note_committed_write(&self, tenant_id: TenantId, charged_bytes: u64) {
		let mut guard = recover_rwlock_write(&self.tenant_policy_cache, "tenant_policy_cache");
		let Some(policy) = guard.policies.get_mut(&tenant_id) else {
			return;
		};
		// Conservatively anticipate the next reconciler sweep so a
		// freshly-committed local write tightens quota admission
		// immediately instead of waiting for the authoritative cache
		// refresh.
		policy.used_bytes = policy.used_bytes.saturating_add(charged_bytes);
		policy.mean_write_size_bytes = if policy.mean_write_size_bytes == 0 {
			charged_bytes.max(1)
		} else {
			((policy.mean_write_size_bytes.saturating_mul(7)).saturating_add(charged_bytes)) / 8
		}
		.max(1);
	}

	fn idempotency_key_for(write_id: crate::schema::ClientWriteId) -> IdempotencyKey {
		(write_id.client_uuid, write_id.sequence_no)
	}

	fn pending_finalization_record(
		&self,
		meta: &WriteRequestMeta,
		bifurcated: &BifurcatedWrite,
		commit_hlc: HybridTimestamp,
	) -> PendingFinalizationRecord {
		PendingFinalizationRecord::new(
			meta.write_id,
			meta.await_finalization != 0,
			TenantFinalization {
				marker: FinalizationMarker {
					log_index: 0,
					term: 0,
					epoch: meta.expected_epoch,
					commit_hlc,
					payload_hash: bifurcated.payload_hash,
					e_safe_index: 0,
					durability: meta.durability,
					_pad: [0; 7],
				},
				tenant_id: meta.tenant_id,
				key_identity: bifurcated.key_identity,
				charged_bytes: bifurcated.charged_bytes,
				record_count: 1,
			},
		)
	}

	fn pending_finalization_record_from_replica_meta(
		&self,
		meta: &ReplicaWriteMeta,
	) -> PendingFinalizationRecord {
		PendingFinalizationRecord::new(
			meta.write_id,
			meta.await_finalization != 0,
			TenantFinalization {
				marker: FinalizationMarker {
					log_index: 0,
					term: 0,
					epoch: meta.epoch,
					commit_hlc: meta.commit_hlc,
					payload_hash: meta.payload_hash,
					e_safe_index: 0,
					durability: meta.durability,
					_pad: [0; 7],
				},
				tenant_id: meta.tenant_id,
				key_identity: meta.key_identity,
				charged_bytes: meta.charged_bytes,
				record_count: 1,
			},
		)
	}

	fn pending_finalization_record_for_table(
		&self,
		meta: &TableWriteRequestMeta,
		bifurcated: &BifurcatedTableWrite,
		commit_hlc: HybridTimestamp,
	) -> PendingFinalizationRecord {
		PendingFinalizationRecord::new(
			meta.parent_write_id,
			meta.await_finalization != 0,
			TenantFinalization {
				marker: FinalizationMarker {
					log_index: 0,
					term: 0,
					epoch: meta.expected_epoch,
					commit_hlc,
					payload_hash: bifurcated.payload_hash,
					e_safe_index: 0,
					durability: meta.durability,
					_pad: [0; 7],
				},
				tenant_id: meta.tenant_id,
				key_identity: bifurcated.parent_key_identity,
				charged_bytes: bifurcated.charged_bytes,
				record_count: bifurcated.record_count,
			},
		)
	}

	fn pending_finalization_record_from_table_replica_meta(
		&self,
		meta: &TableReplicaWriteMeta,
	) -> PendingFinalizationRecord {
		PendingFinalizationRecord::new(
			meta.parent_write_id,
			meta.await_finalization != 0,
			TenantFinalization {
				marker: FinalizationMarker {
					log_index: 0,
					term: 0,
					epoch: meta.epoch,
					commit_hlc: meta.commit_hlc,
					payload_hash: meta.payload_hash,
					e_safe_index: 0,
					durability: meta.durability,
					_pad: [0; 7],
				},
				tenant_id: meta.tenant_id,
				key_identity: meta.parent_key_identity,
				charged_bytes: meta.charged_bytes,
				record_count: meta.record_count,
			},
		)
	}

	fn recoverable_pending_response(
		&self,
		write_id: crate::schema::ClientWriteId,
		epoch: u64,
		commit_hlc: HybridTimestamp,
	) -> WriteResponseMeta {
		WriteResponseMeta {
			status: WriteStatus::FINALIZATION_PENDING,
			_pad1: [0; 7],
			log_index: 0,
			term: 0,
			epoch,
			commit_hlc,
			leader_hint: self.local_node,
			echoed_write_id: write_id,
			has_finalization: 0,
			_pad2: [0; 7],
		}
	}

	#[cfg(test)]
	fn terminal_failure_response_for_test(
		&self,
		write_id: crate::schema::ClientWriteId,
		epoch: u64,
		commit_hlc: HybridTimestamp,
	) -> WriteResponseMeta {
		WriteResponseMeta {
			status: WriteStatus::INTERNAL_ERROR,
			_pad1: [0; 7],
			log_index: 0,
			term: 0,
			epoch,
			commit_hlc,
			leader_hint: self.local_node,
			echoed_write_id: write_id,
			has_finalization: 0,
			_pad2: [0; 7],
		}
	}

	fn committed_response_for(
		&self,
		record: &PendingFinalizationRecord,
		marker: FinalizationMarker,
	) -> (WriteResponseMeta, Option<FinalizationMarker>) {
		let response = WriteResponseMeta {
			status: WriteStatus::OK,
			_pad1: [0; 7],
			log_index: marker.log_index,
			term: marker.term,
			epoch: marker.epoch,
			commit_hlc: marker.commit_hlc,
			leader_hint: self.local_node,
			echoed_write_id: record.write_id,
			has_finalization: if record.await_finalization { 1 } else { 0 },
			_pad2: [0; 7],
		};
		let trailing = if record.await_finalization {
			Some(marker)
		} else {
			None
		};
		(response, trailing)
	}

	async fn find_applied_pending_finalization(
		&self,
		record: &PendingFinalizationRecord,
	) -> std::io::Result<Option<FinalizationMarker>> {
		let Some(applied_upto) = self.state_machine.last_applied_log_index().await else {
			return Ok(None);
		};
		self.log_store
			.find_applied_finalization(&record.finalization, applied_upto)
			.await
	}

	async fn resolve_pending_finalization(
		&self,
		record: &PendingFinalizationRecord,
	) -> std::io::Result<PendingFinalizationResolution> {
		if let Some(marker) = self.find_applied_pending_finalization(record).await? {
			return Ok(PendingFinalizationResolution::ObservedApplied(marker));
		}

		if self.raft.current_leader().await != Some(self.local_node.id) {
			return Err(Error::new(
				ErrorKind::WouldBlock,
				"pending finalization is waiting for local leadership",
			));
		}

		match self
			.raft
			.client_write(WendelonRequest::Finalize(record.finalization.clone()))
			.await
		{
			Ok(resp) => {
				let mut marker = record.finalization.marker;
				marker.log_index = resp.log_id.index;
				marker.term = resp.log_id.leader_id.term;
				marker.e_safe_index = resp.log_id.index;
				Ok(PendingFinalizationResolution::CommittedNow(marker))
			}
			Err(err) => {
				if let Some(marker) = self.find_applied_pending_finalization(record).await? {
					return Ok(PendingFinalizationResolution::ObservedApplied(marker));
				}
				Err(Error::other(format!("finalize commit failed: {err}")))
			}
		}
	}

	async fn complete_pending_finalization(
		&self,
		record: &PendingFinalizationRecord,
		resolution: PendingFinalizationResolution,
	) -> std::io::Result<(WriteResponseMeta, Option<FinalizationMarker>)> {
		let marker = match resolution {
			PendingFinalizationResolution::CommittedNow(marker) => {
				self.note_committed_write(
					record.finalization.tenant_id,
					record.finalization.charged_bytes,
				);
				marker
			}
			PendingFinalizationResolution::ObservedApplied(marker) => marker,
		};
		let (response, trailing) = self.committed_response_for(record, marker);
		let outcome = WriteOutcome::FinalizationCommitted { response, trailing };
		self.idempotency.insert(
			Self::idempotency_key_for(record.write_id),
			CachedWriteOutcome::new(outcome),
		);
		self.cdc_release_pending_finalization(record.id).await;
		if let Err(err) = self.pending_finalizations.remove(record.id).await {
			tracing::warn!(
				pending_id = %format_bytes_hex(record.id.as_bytes()),
				error = %err,
				"failed to remove reconciled pending finalization"
			);
		} else {
			self.refresh_pending_finalization_backlog_metric();
		}
		Ok((response, trailing))
	}

	fn recoverable_pending_outcome_for_record(
		&self,
		record: &PendingFinalizationRecord,
	) -> WriteOutcome {
		WriteOutcome::FinalizationRecoverablePending {
			response: self.recoverable_pending_response(
				record.write_id,
				record.finalization.marker.epoch,
				record.finalization.marker.commit_hlc,
			),
			pending_finalization_id: record.id,
		}
	}

	fn duplicate_response_for_outcome(
		&self,
		outcome: WriteOutcome,
	) -> (WriteResponseMeta, Option<FinalizationMarker>) {
		match outcome {
			WriteOutcome::FinalizationCommitted {
				mut response,
				trailing,
			} => {
				response.status = WriteStatus::DUPLICATE;
				(response, trailing)
			}
			WriteOutcome::FinalizationTerminalFailure { mut response } => {
				response.status = WriteStatus::DUPLICATE;
				(response, None)
			}
			WriteOutcome::NoopCommitted { mut response } => {
				response.status = WriteStatus::DUPLICATE;
				(response, None)
			}
			WriteOutcome::FinalizationRecoverablePending { response, .. } => (response, None),
			WriteOutcome::DataBranchPending | WriteOutcome::DurableQuorumReached => {
				debug_assert!(
					false,
					"non-terminal write outcome must not be replayed to duplicate callers"
				);
				(
					WriteResponseMeta {
						status: WriteStatus::INTERNAL_ERROR,
						_pad1: [0; 7],
						log_index: 0,
						term: 0,
						epoch: 0,
						commit_hlc: bytemuck::Zeroable::zeroed(),
						leader_hint: self.local_node,
						echoed_write_id: bytemuck::Zeroable::zeroed(),
						has_finalization: 0,
						_pad2: [0; 7],
					},
					None,
				)
			}
		}
	}

	fn redirected_pending_response(
		&self,
		mut response: WriteResponseMeta,
		leader_hint: NodeId,
	) -> (WriteResponseMeta, Option<FinalizationMarker>) {
		response.status = WriteStatus::NOT_LEADER;
		response.leader_hint = leader_hint;
		(response, None)
	}

	async fn try_reconcile_pending_record_once(
		&self,
		record: &PendingFinalizationRecord,
		path: PendingFinalizationReplayPath,
	) -> std::io::Result<Option<(WriteResponseMeta, Option<FinalizationMarker>)>> {
		let Some(_claim) = self.try_claim_pending_finalization(record.id) else {
			return Ok(None);
		};
		let started = Instant::now();
		let span = tracing::info_span!(
			"data_ring.pending_finalization.reconcile",
			node_id = self.local_node.id,
			tenant_id = record.finalization.tenant_id.get(),
			epoch = record.finalization.marker.epoch,
			path = match path {
				PendingFinalizationReplayPath::DuplicateReplay => "duplicate_replay",
				PendingFinalizationReplayPath::Reconciler => "reconciler",
				PendingFinalizationReplayPath::OrphanWitnessImport => "orphan_witness_import",
			},
			pending_id = %format_bytes_hex(record.id.as_bytes()),
		);
		let result = async {
			match self.resolve_pending_finalization(record).await {
				Ok(resolution) => self
					.complete_pending_finalization(record, resolution)
					.await
					.map(Some),
				Err(err) if err.kind() == ErrorKind::WouldBlock => Ok(None),
				Err(err) => Err(err),
			}
		}
		.instrument(span)
		.await;
		metrics().observe_pending_finalization_replay(path, started.elapsed());
		result
	}

	async fn replay_write_outcome(
		&self,
		outcome: WriteOutcome,
	) -> std::io::Result<(WriteResponseMeta, Option<FinalizationMarker>)> {
		if let WriteOutcome::FinalizationRecoverablePending {
			response,
			pending_finalization_id,
		} = outcome
		{
			if let Some(record) = self
				.pending_finalizations
				.get(pending_finalization_id)
				.await?
			{
				if let Some(leader_hint) = self
					.raft
					.current_leader()
					.await
					.map(|id| NodeId { id, _reserved: 0 })
				{
					if leader_hint.id != self.local_node.id {
						return Ok(self.redirected_pending_response(response, leader_hint));
					}
				}
				match self
					.try_reconcile_pending_record_once(
						&record,
						PendingFinalizationReplayPath::DuplicateReplay,
					)
					.await
				{
					Ok(Some((mut committed_response, trailing))) => {
						committed_response.status = WriteStatus::DUPLICATE;
						return Ok(self.counted_write_result(committed_response, trailing));
					}
					Ok(None) => {}
					Err(err) => {
						tracing::warn!(
							pending_id = %format_bytes_hex(record.id.as_bytes()),
							error = %err,
							"duplicate replay failed to reconcile pending finalization; returning retryable pending status"
						);
						if let Some(leader_hint) = self
							.raft
							.current_leader()
							.await
							.map(|id| NodeId { id, _reserved: 0 })
						{
							if leader_hint.id != self.local_node.id {
								let (redirected, trailing) =
									self.redirected_pending_response(response, leader_hint);
								return Ok(self.counted_write_result(redirected, trailing));
							}
						}
					}
				}
			} else if let Some(cached) = self
				.idempotency
				.get(&Self::idempotency_key_for(response.echoed_write_id))
				.map(|entry| *entry.value())
			{
				match cached.outcome() {
					WriteOutcome::FinalizationRecoverablePending {
						pending_finalization_id: cached_id,
						..
					} if cached_id == pending_finalization_id => {}
					newer_outcome => {
						let (response, trailing) =
							self.duplicate_response_for_outcome(newer_outcome);
						return Ok(self.counted_write_result(response, trailing));
					}
				}
			}
		}
		let (response, trailing) = self.duplicate_response_for_outcome(outcome);
		Ok(self.counted_write_result(response, trailing))
	}

	fn try_claim_pending_finalization(
		&self,
		id: PendingFinalizationId,
	) -> Option<PendingFinalizationClaim> {
		self.pending_finalization_claims.try_claim(id)
	}

	async fn reconcile_pending_finalizations_once(&self) -> std::io::Result<usize> {
		if let Err(err) = self
			.ensure_orphan_witnesses_recovered_for_leadership()
			.await
		{
			tracing::warn!(
				node_id = self.local_node.id,
				error = %err,
				"orphan-witness recovery sweep failed before pending-finalization reconciliation"
			);
		}
		let mut records = self.pending_finalizations.load_all().await?;
		records.sort_by_key(|record| {
			(
				record.finalization.marker.epoch,
				record.finalization.marker.commit_hlc.physical_ms,
				record.finalization.marker.commit_hlc.logical,
				record.finalization.marker.commit_hlc.node_id,
			)
		});

		let mut reconciled = 0usize;
		for record in records {
			match self
				.try_reconcile_pending_record_once(
					&record,
					PendingFinalizationReplayPath::Reconciler,
				)
				.await
			{
				Ok(Some((_response, _trailing))) => {
					reconciled += 1;
				}
				Ok(None) => continue,
				Err(err) => {
					tracing::warn!(
						pending_id = %format_bytes_hex(record.id.as_bytes()),
						error = %err,
						"pending finalization replay failed; will retry"
					);
				}
			}
		}
		Ok(reconciled)
	}

	pub fn spawn_pending_finalization_reconciler(
		self: Arc<Self>,
		tick: Duration,
	) -> JoinHandle<()> {
		tokio::spawn(async move {
			let mut ticker = tokio::time::interval(tick);
			ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
			loop {
				let _ = self.reconcile_pending_finalizations_once().await;
				tokio::select! {
					_ = ticker.tick() => {}
					_ = self.pending_finalization_wakeup.notified() => {}
				}
			}
		})
	}

	pub fn spawn_cdc_finalization_release_observer(self: Arc<Self>) -> JoinHandle<()> {
		tokio::spawn(async move {
			let mut finalizations = self.state_machine.subscribe_finalizations();
			loop {
				match finalizations.recv().await {
					Ok(marker) => {
						self.cdc_release_pending_finalization_marker(marker).await;
					}
					Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
						tracing::warn!(
							skipped,
							"CDC finalization release observer lagged; affected subscribers must recover through catchup"
						);
					}
					Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
				}
			}
		})
	}

	pub fn spawn_cdc_locality_heartbeat_supervisor(
		self: Arc<Self>,
		ring: Arc<RingRouter>,
		interval: Duration,
	) -> JoinHandle<()> {
		tokio::spawn(async move {
			let mut discovery = tokio::time::interval(interval);
			discovery.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
			let mut spawned = BTreeSet::new();
			let mut workers = JoinSet::new();

			loop {
				tokio::select! {
					_ = discovery.tick() => {
						self.register_cdc_heartbeat_localities_for_primary_ranges(&ring);
						for entry in self.cdc_heartbeat_localities.iter() {
							let locality_id = *entry.key();
							if !spawned.insert(locality_id) {
								continue;
							}
							let server = self.clone();
							workers.spawn(async move {
								let worker = CdcLocalityHeartbeatWorker::new(locality_id, interval);
								let quiet_server = server.clone();
								let emit_server = server.clone();
								let result = worker
									.run(
										move |locality_id| {
											let quiet_server = quiet_server.clone();
											async move {
												quiet_server
													.cdc_locality_quiet_since_last_finalization(locality_id)
											}
										},
										move |locality_id| {
											let emit_server = emit_server.clone();
											async move {
												emit_server
													.emit_cdc_heartbeat_finalization_marker(locality_id)
													.await
											}
										},
									)
									.await;
								if let Err(err) = result {
									tracing::warn!(
										locality = locality_id.get(),
										error = %err,
										"CDC locality heartbeat worker stopped"
									);
								}
								locality_id
							});
						}
					}
					Some(joined) = workers.join_next() => {
						match joined {
							Ok(locality_id) => {
								spawned.remove(&locality_id);
							}
							Err(err) => {
								tracing::warn!(error = %err, "CDC locality heartbeat worker join failed");
							}
						}
					}
				}
			}
		})
	}

	#[cfg(test)]
	pub async fn pending_finalization_count_for_test(&self) -> usize {
		self.pending_finalizations
			.load_all()
			.await
			.expect("pending finalizations should load")
			.len()
	}

	fn merge_commit_hlc(&self, client_hlc: HybridTimestamp) -> HybridTimestamp {
		self.hlc.merge(client_hlc)
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
		recover_rwlock_write(&self.leases, "lease_cache").push(lease);
	}

	async fn lease_gate_result(
		&self,
		locality: Locality,
		epoch: u64,
		now: HybridTimestamp,
	) -> LeaseGateResult {
		let lease = {
			let guard = recover_rwlock_read(&self.leases, "lease_cache");
			guard
				.iter()
				.find(|l| {
					l.holder.id == self.local_node.id
						&& l.epoch == epoch
						&& l.range_start <= locality.get()
						&& locality.get() <= l.range_end
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
			Err(err) => {
				tracing::error!(
					holder = lease.holder.id,
					issuer = lease.issuer.id,
					epoch = lease.epoch,
					range_start = lease.range_start,
					range_end = lease.range_end,
					error = %err,
					"lease signature verification failed at data-ring gate"
				);
				LeaseGateResult::InvalidSignature
			}
		}
	}

	fn cache_granted_lease(&self, lease: LeaseDescriptor) {
		let mut guard = recover_rwlock_write(&self.leases, "lease_cache");
		if let Some(slot) = guard
			.iter_mut()
			.find(|l| l.range_start == lease.range_start && l.range_end == lease.range_end)
		{
			*slot = lease;
		} else {
			guard.push(lease);
		}
		metrics()
			.data_ring_lease_cache_entries
			.set(guard.len() as u64);
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

	async fn pull_pending_witnesses_from_peer(
		&self,
		peer: NodeId,
		request_epoch: u64,
	) -> std::io::Result<Vec<PendingFinalizationRecord>> {
		let conn = self.factory.get_connection(peer).await?;
		let mut stream = conn.open_bi().await?;
		let req = PendingWitnessPullRequestMeta {
			requester: self.local_node,
			request_epoch,
		};
		let header = FrameHeader {
			magic: HRFT_MAGIC,
			version: 1,
			msg_type: MsgType::PENDING_WITNESS_REQ.0,
			flags: 0,
			stream_id: stream.stream_id() as u32,
			meta_len: std::mem::size_of::<PendingWitnessPullRequestMeta>() as u32,
			arrow_len: 0,
			trailing_len: 0,
		};
		write_frame(&mut *stream, &header, bytemuck::bytes_of(&req), None, None).await?;
		stream.finish().await?;
		let frame = read_frame(&mut *stream, &self.pool).await?;
		if frame.header.msg_type != MsgType::PENDING_WITNESS_RESP.0 {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"expected PENDING_WITNESS_RESP, got msg_type=0x{:02x}",
					frame.header.msg_type
				),
			));
		}
		if frame.meta.len() < std::mem::size_of::<PendingWitnessPullResponseMeta>() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"PENDING_WITNESS_RESP meta too small",
			));
		}
		let resp: PendingWitnessPullResponseMeta = *bytemuck::from_bytes(
			&frame.meta[..std::mem::size_of::<PendingWitnessPullResponseMeta>()],
		);
		if resp.status != PendingWitnessPullStatus::OK {
			return Err(Error::other(format!(
				"peer {peer:?} failed orphan-witness pull with status={}",
				resp.status.0
			)));
		}
		match frame.arrow_payload.as_ref() {
			Some(bytes) if !bytes.is_empty() => decode_pending_witness_records(bytes),
			_ if resp.witness_count == 0 => Ok(Vec::new()),
			_ => Err(Error::new(
				ErrorKind::InvalidData,
				"PENDING_WITNESS_RESP missing witness payload",
			)),
		}
	}

	async fn import_pending_witnesses_from_peer(
		&self,
		peer: NodeId,
		request_epoch: u64,
	) -> std::io::Result<usize> {
		let started = Instant::now();
		let records = self
			.pull_pending_witnesses_from_peer(peer, request_epoch)
			.await?;
		let mut imported = 0usize;
		for record in records {
			if self.pending_finalizations.get(record.id).await?.is_none() {
				self.pending_finalizations.upsert(record).await?;
				imported += 1;
			}
		}
		if imported > 0 {
			self.refresh_pending_finalization_backlog_metric();
		}
		metrics().observe_pending_finalization_replay(
			PendingFinalizationReplayPath::OrphanWitnessImport,
			started.elapsed(),
		);
		Ok(imported)
	}

	fn current_orphan_witness_recovery_stamp(&self) -> Option<OrphanWitnessRecoveryStamp> {
		let metrics = self.raft.metrics();
		let borrowed = metrics.borrow();
		if borrowed.current_leader != Some(self.local_node.id) {
			return None;
		}
		Some(OrphanWitnessRecoveryStamp {
			term: borrowed.current_term,
			epoch: self.factory.membership_snapshot().epoch,
		})
	}

	async fn ensure_orphan_witnesses_recovered_for_leadership(&self) -> std::io::Result<()> {
		let Some(target) = self.current_orphan_witness_recovery_stamp() else {
			return Ok(());
		};

		if *recover_rwlock_read(
			&self.orphan_witness_recovery_stamp,
			"orphan_witness_recovery_stamp",
		) == Some(target)
		{
			return Ok(());
		}

		let _guard = self.orphan_witness_recovery_lock.lock().await;
		let Some(target) = self.current_orphan_witness_recovery_stamp() else {
			return Ok(());
		};
		if *recover_rwlock_read(
			&self.orphan_witness_recovery_stamp,
			"orphan_witness_recovery_stamp",
		) == Some(target)
		{
			return Ok(());
		}

		let membership = self.factory.membership_snapshot();
		for (peer, caps) in &membership.peers {
			if peer.id == self.local_node.id || !caps.roles.contains(NodeRole::Data) {
				continue;
			}
			self.import_pending_witnesses_from_peer(*peer, target.epoch)
				.await?;
		}

		*recover_rwlock_write(
			&self.orphan_witness_recovery_stamp,
			"orphan_witness_recovery_stamp",
		) = Some(target);
		Ok(())
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
		metrics().data_ring_lease_refresh_passes_total.inc();
		let snapshot = ring.snapshot();
		let epoch = snapshot.epoch();
		let ranges = snapshot.primary_ranges_for(self.local_node);

		{
			let mut guard = recover_rwlock_write(&self.leases, "lease_cache");
			guard.retain(|l| l.epoch == epoch);
			metrics()
				.data_ring_lease_cache_entries
				.set(guard.len() as u64);
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
						WendelonResponse::Error(message) => Err(Error::new(
							ErrorKind::Other,
							format!(
								"lease manager: state machine rejected AcquireLease: {message}"
							),
						)),
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
						metrics().data_ring_lease_grants_total.inc();
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
					metrics().data_ring_lease_denials_total.inc();
					// Another holder (or a stale-epoch request)
					// blocks us — skip, keep the old entry if any.
					// The next tick retries.
				}
				Err(e) => {
					metrics().data_ring_lease_errors_total.inc();
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
				if let Err(err) = self.reclaim_mvcc_retention_once(1).await {
					tracing::warn!(
						error = %err,
						"migration sweeper MVCC retention pass failed; will retry on next tick"
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

	async fn replicate_table_batch_to_remote(
		factory: Arc<TransportProviderFactory>,
		pool: Arc<BufPool>,
		target: NodeId,
		meta: TableReplicaWriteMeta,
		arrow_bytes: Arc<Vec<u8>>,
	) -> std::io::Result<ReplicaAckLevel> {
		let conn = factory.get_connection(target).await?;
		let mut stream = conn.open_bi().await?;

		let meta_bytes = bytemuck::bytes_of(&meta);
		let header = FrameHeader {
			magic: HRFT_MAGIC,
			version: 1,
			msg_type: MsgType::TABLE_REPLICATE_REQ.0,
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
		if resp.header.msg_type != MsgType::TABLE_REPLICATE_RESP.0 {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"expected TABLE_REPLICATE_RESP, got msg_type=0x{:02x}",
					resp.header.msg_type
				),
			));
		}
		if resp.meta.len() < std::mem::size_of::<ReplicaWriteRespMeta>() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"TABLE_REPLICATE_RESP meta too small",
			));
		}
		let ack: &ReplicaWriteRespMeta =
			bytemuck::from_bytes(&resp.meta[..std::mem::size_of::<ReplicaWriteRespMeta>()]);
		if ack.status != ReplicaWriteStatus::OK {
			return Err(Error::new(
				ErrorKind::Other,
				format!(
					"replica {target:?} rejected table batch: status={}",
					ack.status.0
				),
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
	async fn handle_query(
		&self,
		actor: &ActorIdentity,
		frame: HrftFrame,
	) -> std::io::Result<HrftFrame> {
		metrics().data_ring_query_requests_total.inc();
		let (meta, bytecode) = self.decode_query_request_frame(&frame)?;
		let span = tracing::info_span!(
			"data_ring.query.request",
			stream_id = frame.header.stream_id,
			tenant_id = meta.tenant_id.get(),
			expected_epoch = meta.expected_epoch,
			consistency = meta.consistency.0,
		);
		let (resp_meta, arrow_bytes) = self
			.execute_query_with_migration_merge(&meta, &bytecode)
			.instrument(span)
			.await?;
		self.emit_query_completion_audit(
			actor,
			&meta,
			&resp_meta,
			arrow_bytes
				.as_ref()
				.map(|bytes| bytes.len() as u64)
				.unwrap_or(0),
		)?;
		Ok(self.build_query_response_frame(
			frame.header.stream_id,
			resp_meta,
			arrow_bytes,
			FRAME_FLAG_STREAM_END,
		))
	}

	async fn handle_table_query(
		&self,
		actor: &ActorIdentity,
		frame: HrftFrame,
	) -> std::io::Result<HrftFrame> {
		metrics().data_ring_query_requests_total.inc();
		let (meta, bytecode) = self.decode_table_query_request_frame(&frame)?;
		let audit_meta = table_query_as_range_query(&meta);
		let span = tracing::info_span!(
			"data_ring.table_query.request",
			stream_id = frame.header.stream_id,
			tenant_id = meta.tenant_id.get(),
			table_object_id = %format_bytes_hex(meta.table_object_id.as_bytes()),
			schema_generation = meta.schema_generation,
			expected_epoch = meta.expected_epoch,
			consistency = meta.consistency.0,
		);
		let plan = self
			.prepare_table_query_execution_plan_with_migration(&meta, &bytecode)
			.instrument(span)
			.await?;
		if plan.response.status != ReadStatus::OK || plan.batches.is_empty() {
			self.emit_query_completion_audit(actor, &audit_meta, &plan.response, 0)?;
			return Ok(self.build_query_response_frame_with_type(
				MsgType::TABLE_QUERY_RESP,
				frame.header.stream_id,
				plan.response,
				None,
				FRAME_FLAG_STREAM_END,
			));
		}

		let schema = plan.batches[0].schema();
		let concatenated = concat_batches(&schema, &plan.batches)
			.map_err(|e| Error::new(ErrorKind::InvalidData, format!("table query concat: {e}")))?;
		let arrow_bytes =
			wire_encode_batch_for_path(&concatenated, NetworkCompressionPath::QueryResponse)?;
		let response = QueryResponseMeta {
			arrow_len: arrow_bytes.len() as u32,
			..plan.response
		};
		self.emit_query_completion_audit(actor, &audit_meta, &response, arrow_bytes.len() as u64)?;
		Ok(self.build_query_response_frame_with_type(
			MsgType::TABLE_QUERY_RESP,
			frame.header.stream_id,
			response,
			Some(arrow_bytes),
			FRAME_FLAG_STREAM_END,
		))
	}

	async fn handle_aggregate(
		&self,
		actor: &ActorIdentity,
		frame: HrftFrame,
		request: AggregateRequest,
	) -> std::io::Result<HrftFrame> {
		metrics().data_ring_query_requests_total.inc();
		let audit_meta =
			aggregate_as_range_query(&request, self.admission_snapshot().current_epoch);
		let span = tracing::info_span!(
			"data_ring.aggregate.request",
			stream_id = frame.header.stream_id,
			tenant_id = request.tenant_id.get(),
			table_object_id = %format_bytes_hex(request.table_object_id.as_bytes()),
			schema_generation = request.schema_generation,
			consistency = request.consistency.0,
			aggregate_count = request.aggregates.len(),
		);
		let response = if actor.role == ActorRole::DataNode {
			self.execute_aggregate_local(&request)
				.instrument(span)
				.await?
		} else {
			self.execute_aggregate_with_migration_merge(&request)
				.instrument(span)
				.await?
		};
		let returned_bytes = response
			.partials
			.iter()
			.map(|partial| partial.state.len() as u64)
			.sum::<u64>();
		let audit_response = aggregate_response_as_query_meta(
			&request,
			&response,
			self.local_node,
			audit_meta.expected_epoch,
		);
		self.emit_query_completion_audit(actor, &audit_meta, &audit_response, returned_bytes)?;
		self.build_aggregate_response_frame(
			frame.header.stream_id,
			&response,
			FRAME_FLAG_STREAM_END,
		)
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

	fn decode_table_query_request_frame(
		&self,
		frame: &HrftFrame,
	) -> std::io::Result<(TableQueryRequestMeta, Vec<u8>)> {
		if frame.meta.len() < std::mem::size_of::<TableQueryRequestMeta>() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"TABLE_QUERY_REQ meta too small",
			));
		}
		let meta: TableQueryRequestMeta =
			*bytemuck::from_bytes(&frame.meta[..std::mem::size_of::<TableQueryRequestMeta>()]);
		let bytecode = if meta.bytecode_len == 0 {
			Vec::new()
		} else {
			let trailing = frame.trailing.as_ref().ok_or_else(|| {
				Error::new(
					ErrorKind::InvalidData,
					"TABLE_QUERY_REQ declared bytecode_len > 0 but trailing is absent",
				)
			})?;
			let want = meta.bytecode_len as usize;
			if trailing.len() < want {
				return Err(Error::new(
					ErrorKind::InvalidData,
					"TABLE_QUERY_REQ trailing is shorter than declared bytecode_len",
				));
			}
			trailing[..want].to_vec()
		};
		Ok((meta, bytecode))
	}

	fn decode_aggregate_request_frame(
		&self,
		frame: &HrftFrame,
	) -> std::io::Result<AggregateRequest> {
		if !frame.meta.is_empty() || frame.arrow_payload.is_some() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"AGG_REQ must carry only a postcard trailing payload",
			));
		}
		let trailing = frame.trailing.as_ref().ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				"AGG_REQ missing postcard trailing payload",
			)
		})?;
		wire::decode::<AggregateRequest>(trailing)
	}

	fn build_query_response_frame(
		&self,
		stream_id: u32,
		resp: QueryResponseMeta,
		arrow_bytes: Option<Vec<u8>>,
		flags: u16,
	) -> HrftFrame {
		self.build_query_response_frame_with_type(
			MsgType::QUERY_RESP,
			stream_id,
			resp,
			arrow_bytes,
			flags,
		)
	}

	fn build_query_response_frame_with_type(
		&self,
		msg_type: MsgType,
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
				msg_type: msg_type.0,
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

	fn build_aggregate_response_frame(
		&self,
		stream_id: u32,
		response: &AggregateResponse,
		flags: u16,
	) -> std::io::Result<HrftFrame> {
		let trailing = wire::encode_into(&self.pool, response)?;
		Ok(HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::AGG_RESP.0,
				flags,
				stream_id,
				meta_len: 0,
				arrow_len: 0,
				trailing_len: trailing.len() as u32,
			},
			meta: self.pool.acquire(),
			arrow_payload: None,
			trailing: Some(trailing),
		})
	}

	fn aggregate_rejected_response(
		&self,
		request: &AggregateRequest,
		code: AggRejectCode,
	) -> AggregateResponse {
		metrics().observe_agg_rejected();
		AggregateResponse::rejected(request, code)
	}

	fn aggregate_error_frame(
		&self,
		stream_id: u32,
		request: &AggregateRequest,
		code: AggRejectCode,
	) -> std::io::Result<HrftFrame> {
		let response = self.aggregate_rejected_response(request, code);
		self.build_aggregate_response_frame(stream_id, &response, FRAME_FLAG_STREAM_END)
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
		let payload = crate::network::ipc::wire_decode_batch(payload_bytes)?;
		validate_write_request_batch(&meta, &payload)?;
		Ok((meta, payload))
	}

	fn decode_table_write_request_frame(
		&self,
		frame: &HrftFrame,
	) -> std::io::Result<(TableWriteRequestMeta, RecordBatch)> {
		if frame.meta.len() < std::mem::size_of::<TableWriteRequestMeta>() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"TABLE_WRITE_REQ meta too small",
			));
		}
		let meta: TableWriteRequestMeta =
			*bytemuck::from_bytes(&frame.meta[..std::mem::size_of::<TableWriteRequestMeta>()]);
		let payload_bytes = frame.arrow_payload.as_ref().ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				"TABLE_WRITE_REQ missing Arrow payload",
			)
		})?;
		let payload = crate::network::ipc::wire_decode_batch(payload_bytes)?;
		Ok((meta, payload))
	}

	fn decode_table_delete_request_frame(
		&self,
		frame: &HrftFrame,
	) -> std::io::Result<(TableDeleteRowsRequestMeta, Vec<Identity>)> {
		if frame.meta.len() < std::mem::size_of::<TableDeleteRowsRequestMeta>() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"TABLE_DELETE_REQ meta too small",
			));
		}
		let meta: TableDeleteRowsRequestMeta =
			*bytemuck::from_bytes(&frame.meta[..std::mem::size_of::<TableDeleteRowsRequestMeta>()]);
		let payload_bytes = frame.arrow_payload.as_ref().ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				"TABLE_DELETE_REQ missing Arrow payload",
			)
		})?;
		let payload = crate::network::ipc::wire_decode_batch(payload_bytes)?;
		let row_key_ids = decode_table_delete_rows_batch(&meta, &payload)?;
		Ok((meta, row_key_ids))
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

	fn build_table_write_response_frame(
		&self,
		stream_id: u32,
		resp: TableWriteResponseMeta,
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
				msg_type: MsgType::TABLE_WRITE_RESP.0,
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

	fn build_table_delete_response_frame(
		&self,
		stream_id: u32,
		resp: TableDeleteRowsResponseMeta,
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
				msg_type: MsgType::TABLE_DELETE_RESP.0,
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

	fn authorize_actor_for_data(
		&self,
		actor: &ActorIdentity,
		target_tenant: Option<TenantId>,
		op: DataOperation,
	) -> std::io::Result<()> {
		if let Some(claimed_tenant) = actor.claimed_tenant_id()? {
			if target_tenant.is_none() {
				return Err(Error::new(
					ErrorKind::PermissionDenied,
					format!(
						"actor tenant {} is not allowed to access cross-tenant {:?}",
						claimed_tenant.get(),
						op
					),
				));
			}
			if target_tenant
				.map(|target_tenant| target_tenant != claimed_tenant)
				.unwrap_or(false)
			{
				return Err(Error::new(
					ErrorKind::PermissionDenied,
					format!(
						"actor tenant {} is not allowed to target tenant {}",
						claimed_tenant.get(),
						target_tenant.unwrap().get()
					),
				));
			}
		}

		let allowed = match op {
			DataOperation::Write
			| DataOperation::Read
			| DataOperation::Query
			| DataOperation::CdcSubscribe
			| DataOperation::StreamFinalization => matches!(
				actor.role,
				ActorRole::DataNode | ActorRole::Operator | ActorRole::BreakGlassAdmin
			),
			DataOperation::Replicate
			| DataOperation::Migrate
			| DataOperation::PendingWitnessPull => {
				matches!(actor.role, ActorRole::DataNode)
			}
		};
		if allowed {
			return Ok(());
		}

		Err(Error::new(
			ErrorKind::PermissionDenied,
			format!("actor role {} is not allowed for {:?}", actor.role, op),
		))
	}

	fn unauthorized_write_frame(
		&self,
		msg_type: MsgType,
		stream_id: u32,
		meta: &WriteRequestMeta,
	) -> HrftFrame {
		self.build_write_response_frame(
			msg_type,
			stream_id,
			self.counted_write_response(error_response(
				meta,
				WriteStatus::AUTHORIZATION_FAILED,
				self.local_node,
			)),
			None,
		)
	}

	fn backpressured_write_frame(
		&self,
		msg_type: MsgType,
		stream_id: u32,
		meta: &WriteRequestMeta,
	) -> HrftFrame {
		self.build_write_response_frame(
			msg_type,
			stream_id,
			self.counted_write_response(error_response(
				meta,
				WriteStatus::BACKPRESSURED,
				self.local_node,
			)),
			None,
		)
	}

	fn table_write_error_response(
		&self,
		meta: &TableWriteRequestMeta,
		status: WriteStatus,
		reject_code: PbcRejectCode,
	) -> TableWriteResponseMeta {
		TableWriteResponseMeta {
			write: self.counted_write_response(WriteResponseMeta {
				status,
				_pad1: [0; 7],
				log_index: 0,
				term: 0,
				epoch: meta.expected_epoch,
				commit_hlc: meta.client_hlc,
				leader_hint: self.local_node,
				echoed_write_id: meta.parent_write_id,
				has_finalization: 0,
				_pad2: [0; 7],
			}),
			reject_code: reject_code.0,
			_pad: [0; 6],
		}
	}

	fn table_delete_error_response(
		&self,
		meta: &TableDeleteRowsRequestMeta,
		status: WriteStatus,
		reject_code: PbcRejectCode,
	) -> TableDeleteRowsResponseMeta {
		TableDeleteRowsResponseMeta {
			write: self.counted_write_response(WriteResponseMeta {
				status,
				_pad1: [0; 7],
				log_index: 0,
				term: 0,
				epoch: meta.expected_epoch,
				commit_hlc: meta.client_hlc,
				leader_hint: self.local_node,
				echoed_write_id: meta.parent_write_id,
				has_finalization: 0,
				_pad2: [0; 7],
			}),
			deleted_count: 0,
			already_deleted_count: 0,
			not_found_count: 0,
			reject_code: reject_code.0,
			_pad: [0; 10],
		}
	}

	fn unauthorized_read_frame(&self, stream_id: u32, meta: &ReadRequestMeta) -> HrftFrame {
		let mut meta_buf = self.pool.acquire();
		meta_buf
			.as_mut_vec()
			.extend_from_slice(bytemuck::bytes_of(&error_read_response(
				meta,
				ReadStatus::AUTHORIZATION_FAILED,
				self.local_node,
			)));
		HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::READ_RESP.0,
				flags: 0,
				stream_id,
				meta_len: meta_buf.len() as u32,
				arrow_len: 0,
				trailing_len: 0,
			},
			meta: meta_buf,
			arrow_payload: None,
			trailing: None,
		}
	}

	fn backpressured_read_frame(&self, stream_id: u32, meta: &ReadRequestMeta) -> HrftFrame {
		let mut meta_buf = self.pool.acquire();
		meta_buf
			.as_mut_vec()
			.extend_from_slice(bytemuck::bytes_of(&error_read_response(
				meta,
				ReadStatus::BACKPRESSURED,
				self.local_node,
			)));
		HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::READ_RESP.0,
				flags: 0,
				stream_id,
				meta_len: meta_buf.len() as u32,
				arrow_len: 0,
				trailing_len: 0,
			},
			meta: meta_buf,
			arrow_payload: None,
			trailing: None,
		}
	}

	fn unauthorized_query_frame(&self, stream_id: u32, meta: &QueryRequestMeta) -> HrftFrame {
		self.build_query_response_frame(
			stream_id,
			query_error_meta(meta, ReadStatus::AUTHORIZATION_FAILED, self.local_node),
			None,
			FRAME_FLAG_STREAM_END,
		)
	}

	fn backpressured_query_frame(&self, stream_id: u32, meta: &QueryRequestMeta) -> HrftFrame {
		self.build_query_response_frame(
			stream_id,
			query_error_meta(meta, ReadStatus::BACKPRESSURED, self.local_node),
			None,
			FRAME_FLAG_STREAM_END,
		)
	}

	fn table_query_error_frame(
		&self,
		stream_id: u32,
		meta: &TableQueryRequestMeta,
		status: ReadStatus,
	) -> HrftFrame {
		let range_meta = table_query_as_range_query(meta);
		self.build_query_response_frame_with_type(
			MsgType::TABLE_QUERY_RESP,
			stream_id,
			query_error_meta(&range_meta, status, self.local_node),
			None,
			FRAME_FLAG_STREAM_END,
		)
	}

	fn record_backpressure_rejection(&self, scope: BackpressureScope) {
		match scope {
			BackpressureScope::Tenant => metrics()
				.data_ring_backpressure_rejections_tenant_total
				.inc(),
			BackpressureScope::Actor => metrics()
				.data_ring_backpressure_rejections_actor_total
				.inc(),
		}
	}

	fn try_admit_tenant_request(
		&self,
		tenant_id: TenantId,
	) -> Result<TenantRequestReservation, BackpressureScope> {
		let max = self.max_tenant_concurrent_requests.load(Ordering::Relaxed);
		match self.tenant_request_inflight.entry(tenant_id) {
			DashEntry::Occupied(mut entry) => {
				if *entry.get() >= max {
					drop(entry);
					self.record_backpressure_rejection(BackpressureScope::Tenant);
					return Err(BackpressureScope::Tenant);
				}
				let next = entry.get().saturating_add(1);
				*entry.get_mut() = next;
			}
			DashEntry::Vacant(entry) => {
				if max == 0 {
					self.record_backpressure_rejection(BackpressureScope::Tenant);
					return Err(BackpressureScope::Tenant);
				}
				entry.insert(1);
			}
		}
		let next = self.active_tenant_requests.fetch_add(1, Ordering::Relaxed) + 1;
		metrics()
			.data_ring_backpressure_active_tenant_requests
			.set(next);
		Ok(TenantRequestReservation {
			inflight: self.tenant_request_inflight.clone(),
			active_requests: self.active_tenant_requests.clone(),
			tenant_id,
			active: true,
		})
	}

	fn try_admit_actor_request(
		&self,
		actor: &ActorIdentity,
	) -> Result<ActorRequestReservation, BackpressureScope> {
		let key = ActorRequestKey {
			role: actor.role,
			key_id: actor.key_id.clone(),
		};
		let max = self.max_actor_concurrent_requests.load(Ordering::Relaxed);
		match self.actor_request_inflight.entry(key.clone()) {
			DashEntry::Occupied(mut entry) => {
				if *entry.get() >= max {
					drop(entry);
					self.record_backpressure_rejection(BackpressureScope::Actor);
					return Err(BackpressureScope::Actor);
				}
				let next = entry.get().saturating_add(1);
				*entry.get_mut() = next;
			}
			DashEntry::Vacant(entry) => {
				if max == 0 {
					self.record_backpressure_rejection(BackpressureScope::Actor);
					return Err(BackpressureScope::Actor);
				}
				entry.insert(1);
			}
		}
		let next = self.active_actor_requests.fetch_add(1, Ordering::Relaxed) + 1;
		metrics()
			.data_ring_backpressure_active_actor_requests
			.set(next);
		Ok(ActorRequestReservation {
			inflight: self.actor_request_inflight.clone(),
			active_requests: self.active_actor_requests.clone(),
			key,
			active: true,
		})
	}

	fn try_admit_data_request(
		&self,
		actor: &ActorIdentity,
		target_tenant: Option<TenantId>,
		op: DataOperation,
	) -> std::io::Result<Result<DataRequestAdmission, BackpressureScope>> {
		let claimed_tenant = actor.claimed_tenant_id()?;
		let tenant_reservation = match op {
			DataOperation::Write
			| DataOperation::Read
			| DataOperation::Query
			| DataOperation::CdcSubscribe => {
				if let Some(tenant_id) = target_tenant {
					match self.try_admit_tenant_request(tenant_id) {
						Ok(reservation) => Some(reservation),
						Err(scope) => return Ok(Err(scope)),
					}
				} else {
					None
				}
			}
			_ => None,
		};
		let actor_reservation = match op {
			DataOperation::Write
			| DataOperation::Read
			| DataOperation::Query
			| DataOperation::CdcSubscribe
			| DataOperation::StreamFinalization => {
				if claimed_tenant.is_none() {
					match self.try_admit_actor_request(actor) {
						Ok(reservation) => Some(reservation),
						Err(scope) => return Ok(Err(scope)),
					}
				} else {
					None
				}
			}
			_ => None,
		};
		Ok(Ok(DataRequestAdmission {
			_tenant: tenant_reservation,
			_actor: actor_reservation,
		}))
	}

	fn unauthorized_replicate_frame(&self, stream_id: u32) -> HrftFrame {
		let mut body = self.pool.acquire();
		body.as_mut_vec()
			.extend_from_slice(bytemuck::bytes_of(&ReplicaWriteRespMeta {
				status: ReplicaWriteStatus::NOT_AUTHORIZED,
				ack_level: ReplicaAckLevel::NONE,
				_pad: [0; 6],
			}));
		HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::REPLICATE_RESP.0,
				flags: 0,
				stream_id,
				meta_len: body.len() as u32,
				arrow_len: 0,
				trailing_len: 0,
			},
			meta: body,
			arrow_payload: None,
			trailing: None,
		}
	}

	fn unauthorized_migrate_frame(&self, stream_id: u32, meta: &MigrateRequestMeta) -> HrftFrame {
		let mut body = self.pool.acquire();
		body.as_mut_vec()
			.extend_from_slice(bytemuck::bytes_of(&migrate_error_meta(
				meta,
				MigrateStatus::NOT_AUTHORIZED,
				ZERO_NODE,
			)));
		HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::MIGRATE_RESP.0,
				flags: 0,
				stream_id,
				meta_len: body.len() as u32,
				arrow_len: 0,
				trailing_len: 0,
			},
			meta: body,
			arrow_payload: None,
			trailing: None,
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
		frame: HrftFrame,
		mut stream: Box<dyn TransportStream>,
	) -> std::io::Result<()> {
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
		actor: &ActorIdentity,
		frame: HrftFrame,
		mut stream: Box<dyn TransportStream>,
	) -> std::io::Result<()> {
		metrics().data_ring_query_requests_total.inc();
		let (meta, bytecode) = self.decode_query_request_frame(&frame)?;
		let span = tracing::info_span!(
			"data_ring.query.stream",
			stream_id = frame.header.stream_id,
			tenant_id = meta.tenant_id.get(),
			expected_epoch = meta.expected_epoch,
			consistency = meta.consistency.0,
		);
		let plan = self
			.prepare_query_execution_plan_with_migration(&meta, &bytecode)
			.instrument(span)
			.await?;

		if plan.response.status != ReadStatus::OK || plan.batches.is_empty() {
			self.emit_query_completion_audit(actor, &meta, &plan.response, 0)?;
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

		let QueryExecutionPlan {
			response,
			batches,
			deadline,
			..
		} = plan;
		let Some(encoded_batches) = encode_query_stream_batches_with_deadline(batches, deadline)?
		else {
			let budget_resp =
				query_error_meta(&meta, ReadStatus::QUERY_BUDGET_EXCEEDED, self.local_node);
			self.emit_query_completion_audit(actor, &meta, &budget_resp, 0)?;
			let response = self.build_query_response_frame(
				frame.header.stream_id,
				budget_resp,
				None,
				FRAME_FLAG_STREAM_END,
			);
			write_frame(&mut *stream, &response.header, &response.meta, None, None).await?;
			stream.finish().await?;
			return Ok(());
		};
		let total_bytes = encoded_batches.iter().map(|arrow| arrow.len() as u64).sum();
		self.emit_query_completion_audit(actor, &meta, &response, total_bytes)?;
		let batch_count = encoded_batches.len();
		for (index, arrow) in encoded_batches.into_iter().enumerate() {
			let is_last = index + 1 == batch_count;
			let flags = if is_last { FRAME_FLAG_STREAM_END } else { 0 };
			if index == 0 {
				let response = QueryResponseMeta {
					arrow_len: arrow.len() as u32,
					..response
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
		actor: &ActorIdentity,
		first_frame: HrftFrame,
		mut stream: Box<dyn TransportStream>,
	) -> std::io::Result<()> {
		let mut next_frame = Some(first_frame);
		loop {
			let frame = if let Some(frame) = next_frame.take() {
				frame
			} else {
				match read_frame(&mut *stream, &self.pool).await {
					Ok(frame) => frame,
					Err(e) if e.kind() == ErrorKind::UnexpectedEof => break,
					Err(e) => return Err(e),
				}
			};

			if frame.header.msg_type != MsgType::BULK_WRITE_REQ.0 {
				return Err(Error::new(
					ErrorKind::InvalidData,
					format!(
						"BULK_WRITE stream expected BULK_WRITE_REQ item, got msg_type=0x{:02x}",
						frame.header.msg_type
					),
				));
			}

			let (meta, payload) = self.decode_write_request_frame(&frame, "BULK_WRITE_REQ")?;
			let span = tracing::info_span!(
				"data_ring.bulk_write.request",
				stream_id = frame.header.stream_id,
				tenant_id = meta.tenant_id.get(),
				expected_epoch = meta.expected_epoch,
				await_finalization = meta.await_finalization != 0,
				durability = meta.durability.0,
				write_sequence = meta.write_id.sequence_no,
			);
			self.emit_write_requested_audit(actor, &meta, frame.header.arrow_len as u64)?;
			let (resp, trailing) = match self.write(meta, payload).instrument(span).await {
				Ok(ok) => ok,
				Err(_) => self.counted_write_result(
					error_response(&meta, WriteStatus::INTERNAL_ERROR, self.local_node),
					None,
				),
			};
			self.emit_write_completion_audit(actor, &meta, &resp, frame.header.arrow_len as u64)?;
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

	async fn handle_write(
		&self,
		actor: &ActorIdentity,
		frame: HrftFrame,
	) -> std::io::Result<HrftFrame> {
		let (meta, payload) = self.decode_write_request_frame(&frame, "WRITE_REQ")?;
		let span = tracing::info_span!(
			"data_ring.write.request",
			stream_id = frame.header.stream_id,
			tenant_id = meta.tenant_id.get(),
			expected_epoch = meta.expected_epoch,
			await_finalization = meta.await_finalization != 0,
			durability = meta.durability.0,
			write_sequence = meta.write_id.sequence_no,
		);
		self.emit_write_requested_audit(actor, &meta, frame.header.arrow_len as u64)?;
		let (resp, trailing) = match self.write(meta, payload).instrument(span).await {
			Ok(ok) => ok,
			Err(_) => self.counted_write_result(
				error_response(&meta, WriteStatus::INTERNAL_ERROR, self.local_node),
				None,
			),
		};
		self.emit_write_completion_audit(actor, &meta, &resp, frame.header.arrow_len as u64)?;
		Ok(self.build_write_response_frame(
			MsgType::WRITE_RESP,
			frame.header.stream_id,
			resp,
			trailing,
		))
	}

	async fn handle_table_write(
		&self,
		_actor: &ActorIdentity,
		frame: HrftFrame,
	) -> std::io::Result<HrftFrame> {
		let (meta, payload) = self.decode_table_write_request_frame(&frame)?;
		let span = tracing::info_span!(
			"data_ring.table_write.request",
			stream_id = frame.header.stream_id,
			tenant_id = meta.tenant_id.get(),
			expected_epoch = meta.expected_epoch,
			table_object_id = %format_bytes_hex(meta.table_object_id.as_bytes()),
			schema_generation = meta.schema_generation,
			row_count = meta.decoded_row_count(),
			await_finalization = meta.await_finalization != 0,
			durability = meta.durability.0,
			write_sequence = meta.parent_write_id.sequence_no,
		);
		let (resp, trailing) = self.table_write(meta, payload).instrument(span).await?;
		Ok(self.build_table_write_response_frame(frame.header.stream_id, resp, trailing))
	}

	async fn handle_table_delete(
		&self,
		actor: &ActorIdentity,
		frame: HrftFrame,
	) -> std::io::Result<HrftFrame> {
		let (meta, row_key_ids) = self.decode_table_delete_request_frame(&frame)?;
		let distinct_target_count = row_key_ids.iter().copied().collect::<HashSet<_>>().len();
		let span = tracing::info_span!(
			"data_ring.table_delete.request",
			stream_id = frame.header.stream_id,
			tenant_id = meta.tenant_id.get(),
			expected_epoch = meta.expected_epoch,
			table_object_id = %format_bytes_hex(meta.table_object_id.as_bytes()),
			schema_generation = meta.schema_generation,
			row_count = meta.row_count,
			await_finalization = meta.await_finalization != 0,
			durability = meta.durability.0,
			write_sequence = meta.parent_write_id.sequence_no,
		);
		self.emit_table_delete_requested_audit(actor, &meta, distinct_target_count)?;
		let (resp, trailing) = self
			.table_delete(meta, row_key_ids)
			.instrument(span)
			.await?;
		self.emit_table_delete_completion_audit(actor, &meta, &resp, distinct_target_count)?;
		Ok(self.build_table_delete_response_frame(frame.header.stream_id, resp, trailing))
	}

	async fn handle_read(
		&self,
		actor: &ActorIdentity,
		frame: HrftFrame,
	) -> std::io::Result<HrftFrame> {
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
		let key = crate::network::ipc::wire_decode_batch(key_bytes)?;
		validate_read_request_batch(&meta, &key)?;

		let span = tracing::info_span!(
			"data_ring.read.request",
			stream_id = frame.header.stream_id,
			tenant_id = meta.tenant_id.get(),
			expected_epoch = meta.expected_epoch,
			consistency = meta.consistency.0,
		);
		let (resp, value_batch) = self.read(meta, key).instrument(span).await?;
		self.emit_read_completion_audit(actor, &meta, &resp)?;
		let arrow_payload = value_batch.map(|batch| {
			let bytes = wire_encode_batch_for_path(&batch, NetworkCompressionPath::ReadResponse)?;
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
		metrics().data_ring_migrate_requests_total.inc();
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

	async fn handle_pending_witness_pull(&self, frame: HrftFrame) -> std::io::Result<HrftFrame> {
		if frame.meta.len() < std::mem::size_of::<PendingWitnessPullRequestMeta>() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"PENDING_WITNESS_REQ meta too small",
			));
		}
		let req: PendingWitnessPullRequestMeta = *bytemuck::from_bytes(
			&frame.meta[..std::mem::size_of::<PendingWitnessPullRequestMeta>()],
		);
		let (resp_meta, payload) = if self.degraded_reason().is_some() {
			(
				PendingWitnessPullResponseMeta {
					status: PendingWitnessPullStatus::INTERNAL_ERROR,
					_pad1: [0; 7],
					witness_count: 0,
				},
				None,
			)
		} else {
			let records = self.pending_finalizations.load_all().await?;
			let payload = if records.is_empty() {
				None
			} else {
				Some(encode_pending_witness_records(&records)?)
			};
			tracing::debug!(
				requester = ?req.requester,
				request_epoch = req.request_epoch,
				witness_count = records.len(),
				"serving orphan-witness pull"
			);
			(
				PendingWitnessPullResponseMeta {
					status: PendingWitnessPullStatus::OK,
					_pad1: [0; 7],
					witness_count: records.len() as u64,
				},
				payload,
			)
		};

		let mut body = self.pool.acquire();
		body.as_mut_vec()
			.extend_from_slice(bytemuck::bytes_of(&resp_meta));
		let arrow_payload = payload.map(|bytes| {
			let mut buf = self.pool.acquire();
			buf.as_mut_vec().extend_from_slice(&bytes);
			buf
		});
		Ok(HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::PENDING_WITNESS_RESP.0,
				flags: 0,
				stream_id: frame.header.stream_id,
				meta_len: body.len() as u32,
				arrow_len: arrow_payload
					.as_ref()
					.map(|buf| buf.len() as u32)
					.unwrap_or(0),
				trailing_len: 0,
			},
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
		let mut locations: Vec<(TenantId, Identity, Locality, RecordLocation)> = match meta.kind {
			MigrateRequestKind::POINT_LOOKUP => self
				.slab
				.scan_all_versions(
					Locality::new(meta.range_start),
					Locality::new(meta.range_start),
					entry.migration_hlc_floor,
				)
				.into_iter()
				.filter(|(tenant_id, key_identity, _, _)| {
					*tenant_id == meta.tenant_id && *key_identity == meta.key_identity
				})
				.collect(),
			MigrateRequestKind::RANGE_PAGE => self.slab.scan_all_versions_from(
				Locality::new(meta.range_start),
				Locality::new(meta.range_end),
				entry.migration_hlc_floor,
				meta.cursor_identity,
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

		// Paging caps. Records are sorted `(locality, identity, hlc ASC)`,
		// so truncating at the caps yields a deterministic boundary.
		// `(next_cursor_identity, next_cursor_hlc)` names the last
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
		let mut rows: Vec<(TenantId, Identity, Locality, RecordLocation, Vec<u8>)> = Vec::new();
		let mut packed_bytes: usize = 0;
		let mut last_key = (TenantId::ZERO, Identity::ZERO);
		let mut last_hlc: HybridTimestamp = bytemuck::Zeroable::zeroed();
		let mut truncated = false;
		for (tenant_id, key_identity, routing_locality, loc) in locations.drain(..) {
			if rows.len() >= max_records {
				truncated = true;
				break;
			}
			let frame: Vec<u8> = self.slab.read(loc).await?;
			if packed_bytes.saturating_add(frame.len()) > max_bytes && !rows.is_empty() {
				truncated = true;
				break;
			}
			packed_bytes = packed_bytes.saturating_add(frame.len());
			last_key = (tenant_id, key_identity);
			last_hlc = loc.hlc;
			rows.push((tenant_id, key_identity, routing_locality, loc, frame));
		}
		let emitted_all = rows.len() == total_locations;
		let has_more = meta.kind == MigrateRequestKind::RANGE_PAGE && truncated && !emitted_all;
		let (next_cursor_identity, next_cursor_hlc) = if has_more {
			(last_key.1, last_hlc)
		} else {
			(Identity::ZERO, bytemuck::Zeroable::zeroed())
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
			next_cursor_identity,
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
	/// covering `key_identity`, gates concurrent pulls through the
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
		tenant_id: TenantId,
		key_identity: Identity,
		as_of_hlc: HybridTimestamp,
	) -> std::io::Result<PointReadOutcome> {
		let started = Instant::now();
		let key_locality = Locality::from(key_identity).get();
		let span = tracing::info_span!(
			"data_ring.migration.proxy_read",
			node_id = self.local_node.id,
			tenant_id = tenant_id.get(),
			key_locality,
			as_of_hlc_physical_ms = as_of_hlc.physical_ms,
			as_of_hlc_logical = as_of_hlc.logical,
			as_of_hlc_node_id = as_of_hlc.node_id,
			migration_id = tracing::field::Empty,
			source_count = tracing::field::Empty,
			outcome = tracing::field::Empty,
		);
		let result = async {
			let sm = self.state_machine.state_snapshot().await;
			let Some(entry) =
				pending_owned_entry(&sm.migrations, Locality::new(key_locality), self.local_node)
			else {
				return Ok(PointReadOutcome::AuthoritativeMiss);
			};

			span.record("migration_id", entry.migration_id);
			span.record("source_count", entry.source_replicas.len() as i64);

			let gate = self
				.migration_registry
				.gate(entry.migration_id, tenant_id, key_identity);
			// Registry cleanup guard — drops AFTER `_guard` releases
			// the mutex, so the inflight slot is freed only once the
			// pull has fully completed. A racer that arrives after
			// removal mints a fresh mutex, re-checks the local index
			// (now a hit post `append_migration`), and skips the RPC.
			let _scope = RegistryScopeGuard {
				registry: &self.migration_registry,
				migration_id: entry.migration_id,
				tenant_id,
				key_identity,
			};
			let _guard = gate.lock().await;

			// Another task may have filled this key while we waited
			// on the gate. If so, skip the RPC entirely — the fast
			// path is the only ack the caller needed.
			if let Some(location) = self.slab.lookup_as_of(tenant_id, key_identity, as_of_hlc) {
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
				tenant_id,
				range_start: key_locality,
				range_end: key_locality,
				key_identity,
				migration_hlc_floor: entry.migration_hlc_floor,
				cursor_identity: Identity::ZERO,
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
								.append_migration_with_locality(
									entry.migration_id,
									record.tenant_id,
									record.key_identity,
									record.routing_locality,
									record.hlc,
									record.payload_hash,
									&record.frame_bytes,
								)
								.await?;
						}
						if let Some(location) =
							self.slab.lookup_as_of(tenant_id, key_identity, as_of_hlc)
						{
							let bytes = self.slab.read(location).await?;
							return Ok(PointReadOutcome::Hit(location, bytes));
						}
						// Source responded OK but did not hold a
						// version of the key that satisfies
						// `as_of_hlc`. Under quorum writes, a
						// different replica may still carry the older
						// retained version we need — fall through and
						// try the next candidate. This still counts as
						// an authoritative miss *for this source* —
						// no bit flipped on `any_inconclusive`.
						continue;
					}
					Ok((resp, _)) if resp.status == MigrateStatus::KEY_NOT_FOUND => {
						// Authoritative miss — the source is
						// reachable and affirmatively has no record
						// under the ledger floor.
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
		.instrument(span.clone())
		.await;

		match &result {
			Ok(PointReadOutcome::Hit(..)) => {
				span.record("outcome", "hit");
				metrics().observe_migration_proxy_read(
					MigrationProxyReadOutcome::Hit,
					started.elapsed(),
				);
			}
			Ok(PointReadOutcome::AuthoritativeMiss) => {
				span.record("outcome", "authoritative_miss");
				metrics().observe_migration_proxy_read(
					MigrationProxyReadOutcome::AuthoritativeMiss,
					started.elapsed(),
				);
			}
			Ok(PointReadOutcome::Incomplete) => {
				span.record("outcome", "incomplete");
				metrics().observe_migration_proxy_read(
					MigrationProxyReadOutcome::Incomplete,
					started.elapsed(),
				);
			}
			Err(_) => {
				span.record("outcome", "error");
			}
		}

		result
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
	/// `(key_identity, hlc, payload_hash)`).
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
			let mut cursor_identity = Identity::ZERO;
			let mut cursor_hlc: HybridTimestamp = bytemuck::Zeroable::zeroed();
			loop {
				let req = MigrateRequestMeta {
					migration_id: entry.migration_id,
					expected_to_epoch: entry.to_epoch,
					requester: self.local_node,
					tenant_id: TenantId::ZERO,
					range_start,
					range_end,
					key_identity: Identity::ZERO,
					migration_hlc_floor: entry.migration_hlc_floor,
					cursor_identity,
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
						.append_migration_with_locality(
							entry.migration_id,
							record.tenant_id,
							record.key_identity,
							record.routing_locality,
							record.hlc,
							record.payload_hash,
							&record.frame_bytes,
						)
						.await?;
				}
				if resp.has_more == 0 {
					break;
				}
				cursor_identity = resp.next_cursor_identity;
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
		if plan.response.status != ReadStatus::OK || plan.batches.is_empty() {
			return Ok((plan.response, None));
		}

		let QueryExecutionPlan {
			response, batches, ..
		} = plan;
		let schema = batches[0].schema();
		let concatenated = concat_batches(&schema, &batches)
			.map_err(|e| Error::new(ErrorKind::InvalidData, format!("PBC concat: {e}")))?;
		let arrow_bytes =
			wire_encode_batch_for_path(&concatenated, NetworkCompressionPath::QueryResponse)?;
		let response = QueryResponseMeta {
			arrow_len: arrow_bytes.len() as u32,
			..response
		};
		Ok((response, Some(arrow_bytes)))
	}

	async fn prepare_query_execution_plan_with_migration(
		&self,
		meta: &QueryRequestMeta,
		bytecode: &[u8],
	) -> std::io::Result<QueryExecutionPlan> {
		let admission = self.admission_snapshot();

		// Gate #1: epoch staleness. The authoritative epoch is the
		// highest one the SM has observed an E_safe advance for.
		let current_epoch = admission.current_epoch;
		if meta.expected_epoch < current_epoch {
			return Ok(query_plan_error(
				meta,
				ReadStatus::EPOCH_STALE,
				self.local_node,
			));
		}
		if let Err(status) = self.enforce_read_policy(meta.tenant_id).await {
			return Ok(query_plan_error(meta, status, self.local_node));
		}

		// Gate #2: LINEARIZABLE watermark. The scan (and any
		// migration fan-out it would trigger) is only authoritative
		// once the local SM has observed E_safe at-or-past the
		// client's `min_hlc` for the requested epoch.
		if meta.consistency == Consistency::LINEARIZABLE {
			let watermark = admission
				.e_safe_watermarks
				.get(&meta.expected_epoch)
				.copied()
				.unwrap_or(HybridTimestamp {
					physical_ms: 0,
					logical: 0,
					node_id: 0,
				});
			if watermark < meta.min_hlc {
				return Ok(query_plan_error(
					meta,
					ReadStatus::E_SAFE_BEHIND,
					self.local_node,
				));
			}
		}
		if self.snapshot_request_outside_retention(meta.as_of_hlc) {
			return Ok(query_plan_error(
				meta,
				ReadStatus::RETENTION_EXPIRED,
				self.local_node,
			));
		}

		let sm_peek = self.state_machine.state_snapshot().await;
		let pending: Vec<MigrationLedgerEntry> = sm_peek
			.migrations
			.values()
			.filter(|e| {
				e.logical_owner.id == self.local_node.id
					&& e.state != MigrationState::Completed
					&& e.range_start <= meta.locality_range_end
					&& e.range_end >= meta.locality_range_start
			})
			.cloned()
			.collect();
		drop(sm_peek);

		for entry in &pending {
			let rs = entry.range_start.max(meta.locality_range_start);
			let re = entry.range_end.min(meta.locality_range_end);
			if let Err(_e) = self.pull_migration_range(entry, rs, re).await {
				// Any failure in the fan-out means the local slab
				// is *not* an authoritative view of the query
				// window. Surface it explicitly so clients retry
				// rather than downgrading to a possibly-partial
				// local answer.
				return Ok(query_plan_error(
					meta,
					ReadStatus::MIGRATION_INCOMPLETE,
					self.local_node,
				));
			}
		}

		plan_query_execution_with_budget_and_retention_floor(
			&self.slab,
			&self.state_machine,
			self.local_node,
			meta,
			bytecode,
			QueryExecutionBudget::default(),
			self.mvcc_retention_floor_hlc(),
		)
		.await
	}

	async fn prepare_table_query_execution_plan_with_migration(
		&self,
		meta: &TableQueryRequestMeta,
		bytecode: &[u8],
	) -> std::io::Result<QueryExecutionPlan> {
		let range_meta = table_query_as_range_query(meta);
		let admission = self.admission_snapshot();
		let current_epoch = admission.current_epoch;
		if meta.expected_epoch < current_epoch {
			return Ok(query_plan_error(
				&range_meta,
				ReadStatus::EPOCH_STALE,
				self.local_node,
			));
		}
		if let Err(status) = self.enforce_read_policy(meta.tenant_id).await {
			return Ok(query_plan_error(&range_meta, status, self.local_node));
		}
		if meta.consistency == Consistency::LINEARIZABLE {
			let watermark = admission
				.e_safe_watermarks
				.get(&meta.expected_epoch)
				.copied()
				.unwrap_or(HybridTimestamp {
					physical_ms: 0,
					logical: 0,
					node_id: 0,
				});
			if watermark < meta.min_hlc {
				return Ok(query_plan_error(
					&range_meta,
					ReadStatus::E_SAFE_BEHIND,
					self.local_node,
				));
			}
		}
		let sm_peek = self.state_machine.state_snapshot().await;
		let Some(descriptor) = sm_peek
			.table_schemas
			.get(meta.tenant_id, meta.table_object_id, meta.schema_generation)
			.cloned()
		else {
			return Ok(query_plan_error(
				&range_meta,
				ReadStatus::QUERY_INVALID_PROGRAM,
				self.local_node,
			));
		};
		let effective_retention = self.table_effective_retention_from_state(
			&sm_peek,
			meta.tenant_id,
			meta.table_object_id,
		);
		if check_read_against_retention(meta.as_of_hlc, &effective_retention).is_err() {
			metrics().observe_storage_table_retention_read_floor_reject();
			return Ok(query_plan_error(
				&range_meta,
				ReadStatus::RETENTION_EXPIRED,
				self.local_node,
			));
		}
		let program = match pbc::Program::compile(bytecode) {
			Ok(program) => program,
			Err(_) => {
				return Ok(query_plan_error(
					&range_meta,
					ReadStatus::QUERY_INVALID_PROGRAM,
					self.local_node,
				));
			}
		};
		let secondary_index_access_plan = secondary_index_access_plan_from_program(
			&sm_peek.secondary_indexes,
			meta.tenant_id,
			&descriptor,
			&program,
		)
		.ok()
		.flatten();
		let ranges = match table_query_scan_ranges(meta.tenant_id, &descriptor, Some(&program)) {
			Ok(ranges) => ranges,
			Err(status) => return Ok(query_plan_error(&range_meta, status, self.local_node)),
		};
		let pending: Vec<MigrationLedgerEntry> = sm_peek
			.migrations
			.values()
			.filter(|e| {
				e.logical_owner.id == self.local_node.id
					&& e.state != MigrationState::Completed
					&& ranges.iter().any(|range| {
						e.range_start <= range.end.get() && e.range_end >= range.start.get()
					})
			})
			.cloned()
			.collect();
		drop(sm_peek);

		for entry in &pending {
			for range in &ranges {
				let rs = entry.range_start.max(range.start.get());
				let re = entry.range_end.min(range.end.get());
				if rs > re {
					continue;
				}
				if let Err(_e) = self.pull_migration_range(entry, rs, re).await {
					return Ok(query_plan_error(
						&range_meta,
						ReadStatus::MIGRATION_INCOMPLETE,
						self.local_node,
					));
				}
			}
		}

		let user_column_count = match descriptor.arrow_schema() {
			Ok(schema) => schema.fields().len(),
			Err(_) => {
				return Ok(query_plan_error(
					&range_meta,
					ReadStatus::QUERY_INVALID_PROGRAM,
					self.local_node,
				));
			}
		};
		let requirement = match analyze_pbc_column_requirement(&program, user_column_count) {
			Ok(requirement) => requirement,
			Err(_) => {
				return Ok(query_plan_error(
					&range_meta,
					ReadStatus::QUERY_INVALID_PROGRAM,
					self.local_node,
				));
			}
		};

		let projected_execution = QueryProjectedExecution::Pbc {
			requirement,
			user_column_count,
		};
		if let Some(access_plan) = secondary_index_access_plan.as_ref() {
			match plan_table_query_execution_by_secondary_index(
				&self.slab,
				self.local_node,
				&range_meta,
				&program,
				&ranges,
				QueryExecutionBudget::default(),
				&descriptor,
				&projected_execution,
				effective_retention.floor,
				access_plan,
			)
			.await
			{
				Ok(plan) => {
					metrics().observe_storage_secondary_index_query_plan(
						meta.tenant_id,
						access_plan.descriptor.table_object_id,
						access_plan.descriptor.index_object_id,
						"indexed",
					);
					return Ok(plan);
				}
				Err(err) => {
					metrics().observe_storage_secondary_index_query_plan(
						meta.tenant_id,
						access_plan.descriptor.table_object_id,
						access_plan.descriptor.index_object_id,
						"fallback_full_scan",
					);
					tracing::warn!(
						tenant_id = meta.tenant_id.get(),
						table_object_id = %format_bytes_hex(meta.table_object_id.as_bytes()),
						index_object_id = %format_bytes_hex(access_plan.descriptor.index_object_id.as_bytes()),
						error = %err,
						"secondary index table-query execution failed; falling back to full scan"
					);
				}
			}
		}

		let mut plan = plan_query_execution_for_ranges_with_program(
			&self.slab,
			self.local_node,
			&range_meta,
			&program,
			&ranges,
			QueryExecutionBudget::default(),
			Some(&descriptor),
			projected_execution,
			effective_retention.floor,
		)
		.await?;
		plan.secondary_index_access_plan = secondary_index_access_plan;
		Ok(plan)
	}

	async fn execute_aggregate_with_migration_merge(
		&self,
		request: &AggregateRequest,
	) -> std::io::Result<AggregateResponse> {
		let prepared = match self
			.prepare_aggregate_execution_context_with_migration(request)
			.await?
		{
			Ok(prepared) => prepared,
			Err(code) => return Ok(self.aggregate_rejected_response(request, code)),
		};
		let membership = self.factory.membership_snapshot();
		let assignments = match aggregate_owner_subranges(
			&membership,
			self.router.replication_factor(),
			&prepared.ranges,
		) {
			Ok(assignments) if !assignments.is_empty() => assignments,
			Ok(_) => {
				return self.execute_aggregate_local(request).await;
			}
			Err(code) => return Ok(self.aggregate_rejected_response(request, code)),
		};

		if assignments.len()
			> usize::try_from(request.budgets.max_locations.get()).unwrap_or(usize::MAX)
		{
			return Ok(self.aggregate_rejected_response(request, AggRejectCode::BudgetExceeded));
		}
		for target in assignments.keys().copied() {
			if target == self.local_node {
				continue;
			}
			let has_capability = membership.peers.get(&target).is_some_and(|peer| {
				peer.capabilities
					.contains(NodeCapability::PartialAggregatePushdown)
			});
			if !has_capability {
				metrics().agg_skipped_no_capability_total.inc();
				return Ok(
					self.aggregate_rejected_response(request, AggRejectCode::CapabilityUnavailable)
				);
			}
		}

		let mut responses = Vec::with_capacity(assignments.len());
		let mut remote = JoinSet::new();
		for (target, ranges) in assignments {
			let mut subrequest = request.clone();
			subrequest.locality_ranges = ranges;
			if target == self.local_node {
				responses.push(self.execute_aggregate_local(&subrequest).await?);
			} else {
				let server = self.clone();
				remote
					.spawn(async move { server.aggregate_peer_request(target, &subrequest).await });
			}
		}
		while let Some(joined) = remote.join_next().await {
			match joined {
				Ok(Ok(response)) => responses.push(response),
				Ok(Err(err)) => {
					tracing::warn!(
						error = %err,
						"aggregate peer request failed during coordinator merge"
					);
					return Ok(
						self.aggregate_rejected_response(request, AggRejectCode::DeadlineExceeded)
					);
				}
				Err(err) => {
					tracing::warn!(
						error = %err,
						"aggregate peer task failed during coordinator merge"
					);
					return Ok(
						self.aggregate_rejected_response(request, AggRejectCode::DeadlineExceeded)
					);
				}
			}
		}

		match merge_aggregate_responses(request, &responses) {
			Ok(response) => Ok(response),
			Err(code) => Ok(self.aggregate_rejected_response(request, code)),
		}
	}

	async fn execute_aggregate_local(
		&self,
		request: &AggregateRequest,
	) -> std::io::Result<AggregateResponse> {
		let prepared = match self
			.prepare_aggregate_execution_context_with_migration(request)
			.await?
		{
			Ok(prepared) => prepared,
			Err(code) => return Ok(self.aggregate_rejected_response(request, code)),
		};
		if let Some(response) = self
			.try_aggregate_csb_direct_answer(request, &prepared)
			.await?
		{
			return Ok(response);
		}
		let projected_execution =
			match aggregate_projected_execution(&self.slab, request, &prepared) {
				Ok(execution) => execution,
				Err(code) => return Ok(self.aggregate_rejected_response(request, code)),
			};
		let mut folded_request =
			match aggregate_fold_request_for_execution(request, &projected_execution) {
				Ok(request) => request,
				Err(code) => return Ok(self.aggregate_rejected_response(request, code)),
			};
		let plan = plan_query_execution_for_ranges_with_program(
			&self.slab,
			self.local_node,
			&prepared.range_meta,
			&prepared.program,
			&prepared.ranges,
			aggregate_query_budget(&request.budgets),
			Some(&prepared.descriptor),
			projected_execution,
			prepared.retention_floor,
		)
		.await?;
		if plan.response.status != ReadStatus::OK
			&& plan.response.status != ReadStatus::KEY_NOT_FOUND
		{
			return Ok(self.aggregate_rejected_response(
				request,
				aggregate_reject_from_read_status(plan.response.status),
			));
		}

		// The query planner has already applied the predicate while
		// preserving scalar identity/locality context. Fold the filtered
		// batches without re-running the bytecode a second time.
		folded_request.predicate_program = None;
		let result = match aggregate_record_batches(
			&folded_request,
			if plan.response.status == ReadStatus::KEY_NOT_FOUND {
				&[]
			} else {
				&plan.batches
			},
			false,
		) {
			Ok(result) => result,
			Err(code) => return Ok(self.aggregate_rejected_response(request, code)),
		};
		let mut response = result.response;
		response.aggregate_expression_digest = request.expression_digest();
		if plan.response.status == ReadStatus::OK {
			response.max_hlc = plan.response.value_hlc;
		}
		response.diagnostics.slabs_consulted = plan.stats_explain.candidate_slabs_before_stats;
		response.diagnostics.slabs_pruned_by_csb = plan.stats_explain.slabs_pruned_by_stats;
		response.diagnostics.slabs_stats_unavailable = plan
			.stats_explain
			.slabs_scanned_stats_missing
			.saturating_add(plan.stats_explain.slabs_scanned_stats_unsupported)
			.saturating_add(plan.stats_explain.slabs_scanned_predicate_unrepresentable)
			.saturating_add(plan.stats_explain.stats_decrypt_failed_total);
		Ok(response)
	}

	async fn try_aggregate_csb_direct_answer(
		&self,
		request: &AggregateRequest,
		prepared: &PreparedAggregateExecution,
	) -> std::io::Result<Option<AggregateResponse>> {
		if !aggregate_csb_direct_answer_eligible(request, &prepared.ranges) {
			return Ok(None);
		}
		let committed_tombstones = self.state_machine.tombstones_snapshot().await;
		let mut candidate_slab_ids = HashSet::new();
		let mut responses = Vec::new();
		for range in &prepared.ranges {
			let scanned = self.slab.scan_as_of_with_identity_and_locality(
				request.tenant_id,
				range.start,
				range.end,
				request.as_of_hlc,
			);
			for (_identity, _locality, location) in scanned {
				candidate_slab_ids.insert(location.slab_id);
			}
		}
		let mut slab_ids = candidate_slab_ids.into_iter().collect::<Vec<_>>();
		slab_ids.sort_by_key(|slab_id| slab_id.get());
		for slab_id in slab_ids {
			let Some((min_hlc, max_hlc, rows)) = self.slab.slab_hlc_bounds(slab_id) else {
				return Ok(None);
			};
			let bounds = AggregateCsbSlabBounds::from_parts(min_hlc, max_hlc, rows);
			if let Some(floor) = prepared.retention_floor {
				match aggregate_csb_retention_decision(bounds, floor) {
					AggregateCsbRetentionDecision::Direct => {}
					AggregateCsbRetentionDecision::ZeroContribution => {
						let response = match aggregate_csb_zero_response(request, bounds.row_count)
						{
							Ok(response) => response,
							Err(code) => {
								return Ok(Some(self.aggregate_rejected_response(request, code)));
							}
						};
						responses.push(response);
						continue;
					}
					AggregateCsbRetentionDecision::RowFilter => return Ok(None),
				}
			}
			let Some(pointer) = self.slab.tier_pointer(slab_id).await else {
				return Ok(None);
			};
			if pointer.tombstone_metadata.is_untracked() {
				metrics().observe_agg_csb_short_circuit_skipped_untracked_tombstone_metadata();
				return Ok(None);
			}
			if pointer.supersession_metadata.is_untracked() {
				metrics().observe_agg_csb_short_circuit_skipped_supersession_metadata_untracked();
				return Ok(None);
			}
			if self.slab.slab_has_migrated_range_tombstone_at(
				slab_id,
				request.as_of_hlc,
				&committed_tombstones,
			) {
				return Ok(None);
			}
			if self
				.slab
				.slab_has_supersession_at(slab_id, request.as_of_hlc)
			{
				return Ok(None);
			}
			if !crate::storage::slab_visibility_complete(
				bounds.max_hlc,
				pointer.tombstone_metadata,
				pointer.supersession_metadata,
				request.as_of_hlc,
			) {
				return Ok(None);
			}
			let stats = match self
				.slab
				.load_tiered_column_stats_block(&pointer, &prepared.descriptor)
				.await?
			{
				Some(stats) => stats,
				None => return Ok(None),
			};
			let response =
				match aggregate_csb_response_for_stats(request, &prepared.descriptor, &stats) {
					Ok(Some(response)) => response,
					Ok(None) => return Ok(None),
					Err(code) => {
						return Ok(Some(self.aggregate_rejected_response(request, code)));
					}
				};
			responses.push(response);
		}
		if responses.is_empty() {
			return Ok(None);
		}
		let response = merge_aggregate_responses(request, &responses).map_err(|code| {
			Error::new(
				ErrorKind::InvalidData,
				format!("CSB aggregate partial merge failed: {}", code.as_str()),
			)
		})?;
		metrics().agg_csb_short_circuit_total.inc();
		Ok(Some(response))
	}

	async fn prepare_aggregate_execution_context_with_migration(
		&self,
		request: &AggregateRequest,
	) -> std::io::Result<Result<PreparedAggregateExecution, AggRejectCode>> {
		if let Err(code) = request.validate_envelope(false) {
			return Ok(Err(code));
		}
		let admission = self.admission_snapshot();
		let current_epoch = admission.current_epoch;
		let range_meta = aggregate_as_range_query(request, current_epoch);
		if let Err(status) = self.enforce_read_policy(request.tenant_id).await {
			return Ok(Err(aggregate_reject_from_read_status(status)));
		}
		if request.consistency == Consistency::LINEARIZABLE {
			let watermark = admission
				.e_safe_watermarks
				.get(&current_epoch)
				.copied()
				.unwrap_or(HybridTimestamp {
					physical_ms: 0,
					logical: 0,
					node_id: 0,
				});
			if watermark < request.min_hlc {
				return Ok(Err(AggRejectCode::DeadlineExceeded));
			}
		}
		let bytecode = request.predicate_program.as_deref().unwrap_or(&[]);
		let program = match pbc::Program::compile(bytecode) {
			Ok(program) if !program.has_projection() => program,
			_ => return Ok(Err(AggRejectCode::ProgramInvalid)),
		};

		let sm_peek = self.state_machine.state_snapshot().await;
		let descriptor = match sm_peek.table_schemas.get(
			request.tenant_id,
			request.table_object_id,
			request.schema_generation,
		) {
			Some(descriptor) => descriptor.clone(),
			None => {
				let code = sm_peek
					.table_schemas
					.latest_by_id(request.tenant_id, request.table_object_id)
					.map(|latest| {
						if latest.schema_generation > request.schema_generation {
							AggRejectCode::SchemaStale
						} else {
							AggRejectCode::SchemaUnknown
						}
					})
					.unwrap_or(AggRejectCode::SchemaUnknown);
				return Ok(Err(code));
			}
		};
		let effective_retention = self.table_effective_retention_from_state(
			&sm_peek,
			request.tenant_id,
			request.table_object_id,
		);
		if check_read_against_retention(request.as_of_hlc, &effective_retention).is_err() {
			metrics().observe_storage_table_retention_read_floor_reject();
			return Ok(Err(AggRejectCode::RetentionExpired));
		}
		if descriptor.logical_schema_fingerprint != request.logical_schema_fingerprint {
			return Ok(Err(AggRejectCode::SchemaStale));
		}
		let schema = match descriptor.arrow_schema() {
			Ok(schema) => Arc::new(schema),
			Err(_) => return Ok(Err(AggRejectCode::SchemaUnknown)),
		};
		let empty_batch = RecordBatch::new_empty(schema);
		if let Err(code) = validate_expression_types(&request.aggregates, &empty_batch) {
			return Ok(Err(code));
		}

		let ranges = match table_query_scan_ranges(request.tenant_id, &descriptor, Some(&program)) {
			Ok(ranges) => intersect_aggregate_scan_ranges(ranges, &request.locality_ranges),
			Err(status) => return Ok(Err(aggregate_reject_from_read_status(status))),
		};
		let pending: Vec<MigrationLedgerEntry> = sm_peek
			.migrations
			.values()
			.filter(|e| {
				e.logical_owner.id == self.local_node.id
					&& e.state != MigrationState::Completed
					&& ranges.iter().any(|range| {
						e.range_start <= range.end.get() && e.range_end >= range.start.get()
					})
			})
			.cloned()
			.collect();
		drop(sm_peek);

		for entry in &pending {
			for range in &ranges {
				let rs = entry.range_start.max(range.start.get());
				let re = entry.range_end.min(range.end.get());
				if rs > re {
					continue;
				}
				if let Err(_e) = self.pull_migration_range(entry, rs, re).await {
					return Ok(Err(AggRejectCode::DeadlineExceeded));
				}
			}
		}

		Ok(Ok(PreparedAggregateExecution {
			range_meta,
			program,
			descriptor,
			ranges,
			retention_floor: effective_retention.floor,
		}))
	}

	async fn aggregate_peer_request(
		&self,
		target: NodeId,
		request: &AggregateRequest,
	) -> std::io::Result<AggregateResponse> {
		let conn = self.factory.get_connection(target).await?;
		let mut stream = conn.open_bi().await?;
		let trailing = wire::encode_into(&self.pool, request)?;
		let header = FrameHeader {
			magic: HRFT_MAGIC,
			version: 1,
			msg_type: MsgType::AGG_REQ.0,
			flags: 0,
			stream_id: 0,
			meta_len: 0,
			arrow_len: 0,
			trailing_len: trailing.len() as u32,
		};
		write_frame(&mut *stream, &header, &[], None, Some(&trailing)).await?;
		stream
			.finish()
			.await
			.map_err(|e| Error::new(ErrorKind::BrokenPipe, e))?;

		let response = read_frame(&mut *stream, &self.pool).await?;
		if response.header.msg_type != MsgType::AGG_RESP.0 {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"expected AGG_RESP, got msg_type=0x{:02x}",
					response.header.msg_type
				),
			));
		}
		if !response.meta.is_empty() || response.arrow_payload.is_some() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"AGG_RESP must carry only a postcard trailing payload",
			));
		}
		let trailing = response.trailing.as_ref().ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				"AGG_RESP missing postcard trailing payload",
			)
		})?;
		wire::decode::<AggregateResponse>(trailing)
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
		let (status, ack_level) = if self.degraded_reason().is_some() {
			(ReplicaWriteStatus::INTERNAL_ERROR, ReplicaAckLevel::NONE)
		} else if payload_digest(arrow_bytes) != meta.payload_hash {
			(ReplicaWriteStatus::HASH_MISMATCH, ReplicaAckLevel::NONE)
		} else {
			match self
				.slab
				.append_durable(
					meta.tenant_id,
					meta.key_identity,
					meta.commit_hlc,
					arrow_bytes,
				)
				.await
			{
				Ok(_) => {
					let pending = self.pending_finalization_record_from_replica_meta(&meta);
					match self.pending_finalizations.upsert(pending).await {
						Ok(()) => {
							self.refresh_pending_finalization_backlog_metric();
							(ReplicaWriteStatus::OK, ReplicaAckLevel::DURABLE)
						}
						Err(err) => {
							tracing::error!(
								write_id = ?meta.write_id,
								tenant_id = meta.tenant_id.get(),
								error = %err,
								"replica failed to persist pending-finalization witness after durable append"
							);
							self.trip_local_persistence_distrust(format!(
								"replica pending finalization witness persistence failed after durable append: {err}"
							))
							.await;
							(ReplicaWriteStatus::INTERNAL_ERROR, ReplicaAckLevel::NONE)
						}
					}
				}
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

	async fn handle_table_replicate(&self, frame: HrftFrame) -> std::io::Result<HrftFrame> {
		if frame.meta.len() < std::mem::size_of::<TableReplicaWriteMeta>() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"TABLE_REPLICATE_REQ meta too small",
			));
		}
		let meta: TableReplicaWriteMeta =
			*bytemuck::from_bytes(&frame.meta[..std::mem::size_of::<TableReplicaWriteMeta>()]);
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
				"TABLE_REPLICATE_REQ missing arrow payload",
			)
		})?;

		let (status, ack_level) = if self.degraded_reason().is_some() {
			(ReplicaWriteStatus::INTERNAL_ERROR, ReplicaAckLevel::NONE)
		} else {
			match decode_table_replica_children(&arrow[..]).and_then(|children| {
				validate_table_replica_payload(&meta, &children).map(|()| children)
			}) {
				Ok(children) => {
					let append_result = append_table_children_durable(
						&self.slab,
						meta.tenant_id,
						meta.commit_hlc,
						&children,
					)
					.await;
					match append_result {
						Ok(()) => {
							let cdc_staging_ok = if let Some(descriptor) = {
								let state = self.state_machine.state_snapshot().await;
								state
									.table_schemas
									.latest_by_id(meta.tenant_id, meta.table_object_id)
									.cloned()
							} {
								match self
									.stage_cdc_table_replica_children(
										meta.tenant_id,
										meta.table_object_id,
										&descriptor,
										meta.commit_hlc,
										&children,
										false,
									)
									.await
								{
									Ok(staged) => {
										self.cdc_stage_pending_finalization_marker(
											CdcFinalizationKey::from_table_replica_meta(&meta),
											staged,
										);
										true
									}
									Err(err) => {
										tracing::error!(
											write_id = ?meta.parent_write_id,
											tenant_id = meta.tenant_id.get(),
											table_object_id = %format_bytes_hex(meta.table_object_id.as_bytes()),
											error = %err,
											"replica failed to stage CDC rows after durable table append"
										);
										self.trip_local_persistence_distrust(format!(
											"replica CDC staging failed after durable table append: {err}"
										))
										.await;
										false
									}
								}
							} else {
								tracing::warn!(
									write_id = ?meta.parent_write_id,
									tenant_id = meta.tenant_id.get(),
									table_object_id = %format_bytes_hex(meta.table_object_id.as_bytes()),
									"replica skipped CDC staging because table descriptor is unavailable"
								);
								true
							};
							// A split table-replication frame is only proof for its child
							// group; the aggregate pending witness belongs to the coordinator
							// unless this frame carries the whole parent batch.
							if !cdc_staging_ok {
								(ReplicaWriteStatus::INTERNAL_ERROR, ReplicaAckLevel::NONE)
							} else if meta.group_record_count == meta.record_count
								&& meta.group_payload_hash == meta.payload_hash
							{
								let pending =
									self.pending_finalization_record_from_table_replica_meta(&meta);
								match self.pending_finalizations.upsert(pending).await {
									Ok(()) => {
										self.refresh_pending_finalization_backlog_metric();
										(ReplicaWriteStatus::OK, ReplicaAckLevel::DURABLE)
									}
									Err(err) => {
										tracing::error!(
											write_id = ?meta.parent_write_id,
											tenant_id = meta.tenant_id.get(),
											error = %err,
											"replica failed to persist table pending-finalization witness after durable append"
										);
										self.trip_local_persistence_distrust(format!(
											"table replica pending finalization witness persistence failed after durable append: {err}"
										))
										.await;
										(ReplicaWriteStatus::INTERNAL_ERROR, ReplicaAckLevel::NONE)
									}
								}
							} else {
								(ReplicaWriteStatus::OK, ReplicaAckLevel::DURABLE)
							}
						}
						Err(_) => (ReplicaWriteStatus::INTERNAL_ERROR, ReplicaAckLevel::NONE),
					}
				}
				Err(err) => {
					tracing::warn!(
						write_id = ?meta.parent_write_id,
						tenant_id = meta.tenant_id.get(),
						error = %err,
						"replica rejected malformed table batch"
					);
					(ReplicaWriteStatus::HASH_MISMATCH, ReplicaAckLevel::NONE)
				}
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
			msg_type: MsgType::TABLE_REPLICATE_RESP.0,
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
	async fn handle_data_frame(
		&self,
		context: RequestContext,
		frame: HrftFrame,
	) -> std::io::Result<Option<HrftFrame>> {
		let msg_type = MsgType(frame.header.msg_type);
		match msg_type {
			MsgType::WRITE_REQ => {
				if frame.meta.len() < std::mem::size_of::<WriteRequestMeta>() {
					return Err(Error::new(
						ErrorKind::InvalidData,
						"WRITE_REQ meta too small",
					));
				}
				let meta: WriteRequestMeta =
					*bytemuck::from_bytes(&frame.meta[..std::mem::size_of::<WriteRequestMeta>()]);
				if let Err(err) = self.authorize_actor_for_data(
					&context.actor,
					Some(meta.tenant_id),
					DataOperation::Write,
				) {
					self.emit_write_denied_audit(
						&context.actor,
						&meta,
						WriteStatus::AUTHORIZATION_FAILED,
						err.to_string(),
					)?;
					return Ok(Some(self.unauthorized_write_frame(
						MsgType::WRITE_RESP,
						frame.header.stream_id,
						&meta,
					)));
				}
				let _admission = match self.try_admit_data_request(
					&context.actor,
					Some(meta.tenant_id),
					DataOperation::Write,
				)? {
					Ok(admission) => admission,
					Err(scope) => {
						self.emit_write_denied_audit(
							&context.actor,
							&meta,
							WriteStatus::BACKPRESSURED,
							format!(
								"post-auth request admission saturated at {}",
								backpressure_scope_name(scope)
							),
						)?;
						return Ok(Some(self.backpressured_write_frame(
							MsgType::WRITE_RESP,
							frame.header.stream_id,
							&meta,
						)));
					}
				};
				self.handle_write(&context.actor, frame).await.map(Some)
			}
			MsgType::TABLE_WRITE_REQ => {
				if frame.meta.len() < std::mem::size_of::<TableWriteRequestMeta>() {
					return Err(Error::new(
						ErrorKind::InvalidData,
						"TABLE_WRITE_REQ meta too small",
					));
				}
				let meta: TableWriteRequestMeta = *bytemuck::from_bytes(
					&frame.meta[..std::mem::size_of::<TableWriteRequestMeta>()],
				);
				if self
					.authorize_actor_for_data(
						&context.actor,
						Some(meta.tenant_id),
						DataOperation::Write,
					)
					.is_err()
				{
					let resp = self.table_write_error_response(
						&meta,
						WriteStatus::AUTHORIZATION_FAILED,
						PbcRejectCode::NONE,
					);
					return Ok(Some(self.build_table_write_response_frame(
						frame.header.stream_id,
						resp,
						None,
					)));
				}
				let _admission = match self.try_admit_data_request(
					&context.actor,
					Some(meta.tenant_id),
					DataOperation::Write,
				)? {
					Ok(admission) => admission,
					Err(_) => {
						let resp = self.table_write_error_response(
							&meta,
							WriteStatus::BACKPRESSURED,
							PbcRejectCode::NONE,
						);
						return Ok(Some(self.build_table_write_response_frame(
							frame.header.stream_id,
							resp,
							None,
						)));
					}
				};
				self.handle_table_write(&context.actor, frame)
					.await
					.map(Some)
			}
			MsgType::TABLE_DELETE_REQ => {
				if frame.meta.len() < std::mem::size_of::<TableDeleteRowsRequestMeta>() {
					return Err(Error::new(
						ErrorKind::InvalidData,
						"TABLE_DELETE_REQ meta too small",
					));
				}
				let meta: TableDeleteRowsRequestMeta = *bytemuck::from_bytes(
					&frame.meta[..std::mem::size_of::<TableDeleteRowsRequestMeta>()],
				);
				if self
					.authorize_actor_for_data(
						&context.actor,
						Some(meta.tenant_id),
						DataOperation::Write,
					)
					.is_err()
				{
					let resp = self.table_delete_error_response(
						&meta,
						WriteStatus::AUTHORIZATION_FAILED,
						PbcRejectCode::NONE,
					);
					return Ok(Some(self.build_table_delete_response_frame(
						frame.header.stream_id,
						resp,
						None,
					)));
				}
				let _admission = match self.try_admit_data_request(
					&context.actor,
					Some(meta.tenant_id),
					DataOperation::Write,
				)? {
					Ok(admission) => admission,
					Err(_) => {
						let resp = self.table_delete_error_response(
							&meta,
							WriteStatus::BACKPRESSURED,
							PbcRejectCode::NONE,
						);
						return Ok(Some(self.build_table_delete_response_frame(
							frame.header.stream_id,
							resp,
							None,
						)));
					}
				};
				self.handle_table_delete(&context.actor, frame)
					.await
					.map(Some)
			}
			MsgType::TABLE_REPLICATE_REQ => {
				if frame.meta.len() < std::mem::size_of::<TableReplicaWriteMeta>() {
					return Err(Error::new(
						ErrorKind::InvalidData,
						"TABLE_REPLICATE_REQ meta too small",
					));
				}
				let meta: TableReplicaWriteMeta = *bytemuck::from_bytes(
					&frame.meta[..std::mem::size_of::<TableReplicaWriteMeta>()],
				);
				if self
					.authorize_actor_for_data(
						&context.actor,
						Some(meta.tenant_id),
						DataOperation::Replicate,
					)
					.is_err()
				{
					let resp = ReplicaWriteRespMeta {
						status: ReplicaWriteStatus::NOT_AUTHORIZED,
						ack_level: ReplicaAckLevel::NONE,
						_pad: [0; 6],
					};
					let mut body = self.pool.acquire();
					body.as_mut_vec()
						.extend_from_slice(bytemuck::bytes_of(&resp));
					return Ok(Some(HrftFrame {
						header: FrameHeader {
							magic: HRFT_MAGIC,
							version: 1,
							msg_type: MsgType::TABLE_REPLICATE_RESP.0,
							flags: 0,
							stream_id: frame.header.stream_id,
							meta_len: body.len() as u32,
							arrow_len: 0,
							trailing_len: 0,
						},
						meta: body,
						arrow_payload: None,
						trailing: None,
					}));
				}
				self.handle_table_replicate(frame).await.map(Some)
			}
			MsgType::READ_REQ => {
				if frame.meta.len() < std::mem::size_of::<ReadRequestMeta>() {
					return Err(Error::new(
						ErrorKind::InvalidData,
						"READ_REQ meta too small",
					));
				}
				let meta: ReadRequestMeta =
					*bytemuck::from_bytes(&frame.meta[..std::mem::size_of::<ReadRequestMeta>()]);
				if let Err(err) = self.authorize_actor_for_data(
					&context.actor,
					Some(meta.tenant_id),
					DataOperation::Read,
				) {
					self.emit_read_denied_audit(
						&context.actor,
						&meta,
						ReadStatus::AUTHORIZATION_FAILED,
						err.to_string(),
					)?;
					return Ok(Some(
						self.unauthorized_read_frame(frame.header.stream_id, &meta),
					));
				}
				let _admission = match self.try_admit_data_request(
					&context.actor,
					Some(meta.tenant_id),
					DataOperation::Read,
				)? {
					Ok(admission) => admission,
					Err(scope) => {
						self.emit_read_denied_audit(
							&context.actor,
							&meta,
							ReadStatus::BACKPRESSURED,
							format!(
								"post-auth request admission saturated at {}",
								backpressure_scope_name(scope)
							),
						)?;
						return Ok(Some(
							self.backpressured_read_frame(frame.header.stream_id, &meta),
						));
					}
				};
				self.handle_read(&context.actor, frame).await.map(Some)
			}
			MsgType::REPLICATE_REQ => {
				if frame.meta.len() < std::mem::size_of::<ReplicaWriteMeta>() {
					return Err(Error::new(
						ErrorKind::InvalidData,
						"REPLICATE_REQ meta too small",
					));
				}
				let meta: ReplicaWriteMeta =
					*bytemuck::from_bytes(&frame.meta[..std::mem::size_of::<ReplicaWriteMeta>()]);
				if self
					.authorize_actor_for_data(
						&context.actor,
						Some(meta.tenant_id),
						DataOperation::Replicate,
					)
					.is_err()
				{
					return Ok(Some(
						self.unauthorized_replicate_frame(frame.header.stream_id),
					));
				}
				self.handle_replicate(frame).await.map(Some)
			}
			MsgType::QUERY_REQ => {
				if frame.meta.len() < std::mem::size_of::<QueryRequestMeta>() {
					return Err(Error::new(
						ErrorKind::InvalidData,
						"QUERY_REQ meta too small",
					));
				}
				let meta: QueryRequestMeta =
					*bytemuck::from_bytes(&frame.meta[..std::mem::size_of::<QueryRequestMeta>()]);
				if let Err(err) = self.authorize_actor_for_data(
					&context.actor,
					Some(meta.tenant_id),
					DataOperation::Query,
				) {
					self.emit_query_denied_audit(
						&context.actor,
						&meta,
						ReadStatus::AUTHORIZATION_FAILED,
						err.to_string(),
					)?;
					return Ok(Some(
						self.unauthorized_query_frame(frame.header.stream_id, &meta),
					));
				}
				let _admission = match self.try_admit_data_request(
					&context.actor,
					Some(meta.tenant_id),
					DataOperation::Query,
				)? {
					Ok(admission) => admission,
					Err(scope) => {
						self.emit_query_denied_audit(
							&context.actor,
							&meta,
							ReadStatus::BACKPRESSURED,
							format!(
								"post-auth request admission saturated at {}",
								backpressure_scope_name(scope)
							),
						)?;
						return Ok(Some(
							self.backpressured_query_frame(frame.header.stream_id, &meta),
						));
					}
				};
				self.handle_query(&context.actor, frame).await.map(Some)
			}
			MsgType::TABLE_QUERY_REQ => {
				if frame.meta.len() < std::mem::size_of::<TableQueryRequestMeta>() {
					return Err(Error::new(
						ErrorKind::InvalidData,
						"TABLE_QUERY_REQ meta too small",
					));
				}
				let meta: TableQueryRequestMeta = *bytemuck::from_bytes(
					&frame.meta[..std::mem::size_of::<TableQueryRequestMeta>()],
				);
				let audit_meta = table_query_as_range_query(&meta);
				if let Err(err) = self.authorize_actor_for_data(
					&context.actor,
					Some(meta.tenant_id),
					DataOperation::Query,
				) {
					self.emit_query_denied_audit(
						&context.actor,
						&audit_meta,
						ReadStatus::AUTHORIZATION_FAILED,
						err.to_string(),
					)?;
					return Ok(Some(self.table_query_error_frame(
						frame.header.stream_id,
						&meta,
						ReadStatus::AUTHORIZATION_FAILED,
					)));
				}
				let _admission = match self.try_admit_data_request(
					&context.actor,
					Some(meta.tenant_id),
					DataOperation::Query,
				)? {
					Ok(admission) => admission,
					Err(scope) => {
						self.emit_query_denied_audit(
							&context.actor,
							&audit_meta,
							ReadStatus::BACKPRESSURED,
							format!(
								"post-auth request admission saturated at {}",
								backpressure_scope_name(scope)
							),
						)?;
						return Ok(Some(self.table_query_error_frame(
							frame.header.stream_id,
							&meta,
							ReadStatus::BACKPRESSURED,
						)));
					}
				};
				self.handle_table_query(&context.actor, frame)
					.await
					.map(Some)
			}
			MsgType::AGG_REQ => {
				let request = self.decode_aggregate_request_frame(&frame)?;
				let audit_meta =
					aggregate_as_range_query(&request, self.admission_snapshot().current_epoch);
				if let Err(err) = self.authorize_actor_for_data(
					&context.actor,
					Some(request.tenant_id),
					DataOperation::Query,
				) {
					self.emit_query_denied_audit(
						&context.actor,
						&audit_meta,
						ReadStatus::AUTHORIZATION_FAILED,
						err.to_string(),
					)?;
					return Ok(Some(self.aggregate_error_frame(
						frame.header.stream_id,
						&request,
						AggRejectCode::CapabilityUnavailable,
					)?));
				}
				let _admission = match self.try_admit_data_request(
					&context.actor,
					Some(request.tenant_id),
					DataOperation::Query,
				)? {
					Ok(admission) => admission,
					Err(scope) => {
						self.emit_query_denied_audit(
							&context.actor,
							&audit_meta,
							ReadStatus::BACKPRESSURED,
							format!(
								"post-auth request admission saturated at {}",
								backpressure_scope_name(scope)
							),
						)?;
						return Ok(Some(self.aggregate_error_frame(
							frame.header.stream_id,
							&request,
							AggRejectCode::BudgetExceeded,
						)?));
					}
				};
				self.handle_aggregate(&context.actor, frame, request)
					.await
					.map(Some)
			}
			MsgType::MIGRATE_REQ => {
				if frame.meta.len() < std::mem::size_of::<MigrateRequestMeta>() {
					return Err(Error::new(
						ErrorKind::InvalidData,
						"MIGRATE_REQ meta too small",
					));
				}
				let meta: MigrateRequestMeta =
					*bytemuck::from_bytes(&frame.meta[..std::mem::size_of::<MigrateRequestMeta>()]);
				if self
					.authorize_actor_for_data(
						&context.actor,
						Some(meta.tenant_id),
						DataOperation::Migrate,
					)
					.is_err()
				{
					return Ok(Some(
						self.unauthorized_migrate_frame(frame.header.stream_id, &meta),
					));
				}
				self.handle_migrate(frame).await.map(Some)
			}
			MsgType::PENDING_WITNESS_REQ => {
				if frame.meta.len() < std::mem::size_of::<PendingWitnessPullRequestMeta>() {
					return Err(Error::new(
						ErrorKind::InvalidData,
						"PENDING_WITNESS_REQ meta too small",
					));
				}
				if self
					.authorize_actor_for_data(
						&context.actor,
						None,
						DataOperation::PendingWitnessPull,
					)
					.is_err()
				{
					return Err(Error::new(
						ErrorKind::PermissionDenied,
						"pending witness pull requires data-node identity",
					));
				}
				self.handle_pending_witness_pull(frame).await.map(Some)
			}
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
		context: RequestContext,
		first_header: FrameHeader,
		mut stream: Box<dyn TransportStream>,
		_pool: Arc<BufPool>,
	) -> std::io::Result<()> {
		let frame = read_frame_from_header(&mut *stream, &self.pool, first_header).await?;
		match MsgType(first_header.msg_type) {
			MsgType::BULK_WRITE_REQ => {
				if frame.meta.len() < std::mem::size_of::<WriteRequestMeta>() {
					return Err(Error::new(
						ErrorKind::InvalidData,
						"BULK_WRITE_REQ meta too small",
					));
				}
				let meta: WriteRequestMeta =
					*bytemuck::from_bytes(&frame.meta[..std::mem::size_of::<WriteRequestMeta>()]);
				if let Err(err) = self.authorize_actor_for_data(
					&context.actor,
					Some(meta.tenant_id),
					DataOperation::Write,
				) {
					self.emit_write_denied_audit(
						&context.actor,
						&meta,
						WriteStatus::AUTHORIZATION_FAILED,
						err.to_string(),
					)?;
					let response = self.unauthorized_write_frame(
						MsgType::BULK_WRITE_RESP,
						frame.header.stream_id,
						&meta,
					);
					write_frame(
						&mut *stream,
						&response.header,
						&response.meta,
						None,
						response.trailing.as_deref(),
					)
					.await?;
					stream.finish().await?;
					return Ok(());
				}
				let _admission = match self.try_admit_data_request(
					&context.actor,
					Some(meta.tenant_id),
					DataOperation::Write,
				)? {
					Ok(admission) => admission,
					Err(scope) => {
						self.emit_write_denied_audit(
							&context.actor,
							&meta,
							WriteStatus::BACKPRESSURED,
							format!(
								"post-auth request admission saturated at {}",
								backpressure_scope_name(scope)
							),
						)?;
						let response = self.backpressured_write_frame(
							MsgType::BULK_WRITE_RESP,
							frame.header.stream_id,
							&meta,
						);
						write_frame(
							&mut *stream,
							&response.header,
							&response.meta,
							None,
							response.trailing.as_deref(),
						)
						.await?;
						stream.finish().await?;
						return Ok(());
					}
				};
				self.handle_bulk_stream(&context.actor, frame, stream).await
			}
			MsgType::QUERY_REQ => {
				if frame.meta.len() < std::mem::size_of::<QueryRequestMeta>() {
					return Err(Error::new(
						ErrorKind::InvalidData,
						"QUERY_REQ meta too small",
					));
				}
				let meta: QueryRequestMeta =
					*bytemuck::from_bytes(&frame.meta[..std::mem::size_of::<QueryRequestMeta>()]);
				if let Err(err) = self.authorize_actor_for_data(
					&context.actor,
					Some(meta.tenant_id),
					DataOperation::Query,
				) {
					self.emit_query_denied_audit(
						&context.actor,
						&meta,
						ReadStatus::AUTHORIZATION_FAILED,
						err.to_string(),
					)?;
					let response = self.unauthorized_query_frame(frame.header.stream_id, &meta);
					write_frame(&mut *stream, &response.header, &response.meta, None, None).await?;
					stream.finish().await?;
					return Ok(());
				}
				let _admission = match self.try_admit_data_request(
					&context.actor,
					Some(meta.tenant_id),
					DataOperation::Query,
				)? {
					Ok(admission) => admission,
					Err(scope) => {
						self.emit_query_denied_audit(
							&context.actor,
							&meta,
							ReadStatus::BACKPRESSURED,
							format!(
								"post-auth request admission saturated at {}",
								backpressure_scope_name(scope)
							),
						)?;
						let response =
							self.backpressured_query_frame(frame.header.stream_id, &meta);
						write_frame(
							&mut *stream,
							&response.header,
							&response.meta,
							None,
							response.trailing.as_deref(),
						)
						.await?;
						stream.finish().await?;
						return Ok(());
					}
				};
				self.handle_query_stream(&context.actor, frame, stream)
					.await
			}
			MsgType::CDC_SUBSCRIBE_REQ => {
				if frame.meta.len() < std::mem::size_of::<CdcSubscribeRequestMeta>() {
					return Err(Error::new(
						ErrorKind::InvalidData,
						"CDC_SUBSCRIBE_REQ meta too small",
					));
				}
				let meta: CdcSubscribeRequestMeta = *bytemuck::from_bytes(
					&frame.meta[..std::mem::size_of::<CdcSubscribeRequestMeta>()],
				);
				if self
					.authorize_actor_for_data(
						&context.actor,
						Some(meta.tenant_id),
						DataOperation::CdcSubscribe,
					)
					.is_err()
				{
					let reject = CdcRejectMeta::new(
						CdcSubscriptionId::ZERO,
						CdcRejectCode::TenantAuthDenied,
					);
					cdc_emit_reject(&mut *stream, frame.header.stream_id, &reject).await?;
					stream.finish().await?;
					return Ok(());
				}
				let _admission = match self.try_admit_data_request(
					&context.actor,
					Some(meta.tenant_id),
					DataOperation::CdcSubscribe,
				)? {
					Ok(admission) => admission,
					Err(_) => {
						let reject = CdcRejectMeta::new(
							CdcSubscriptionId::ZERO,
							CdcRejectCode::InternalEngineError,
						);
						cdc_emit_reject(&mut *stream, frame.header.stream_id, &reject).await?;
						stream.finish().await?;
						return Ok(());
					}
				};
				self.handle_cdc_subscribe_stream(&context.actor, frame, meta, stream)
					.await
			}
			MsgType::STREAM_FIN_REQ => {
				if self
					.authorize_actor_for_data(
						&context.actor,
						None,
						DataOperation::StreamFinalization,
					)
					.is_err()
				{
					return Err(Error::new(
						ErrorKind::PermissionDenied,
						"actor is not allowed to open STREAM_FIN_REQ",
					));
				}
				let _admission = match self.try_admit_data_request(
					&context.actor,
					None,
					DataOperation::StreamFinalization,
				)? {
					Ok(admission) => admission,
					Err(_) => {
						return Err(Error::new(
							ErrorKind::WouldBlock,
							"data stream finalization request backpressured",
						));
					}
				};
				self.handle_stream_finalization_stream(frame, stream).await
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

impl WendelonDataServer {
	async fn handle_cdc_subscribe_stream(
		&self,
		actor: &ActorIdentity,
		frame: HrftFrame,
		req: CdcSubscribeRequestMeta,
		mut stream: Box<dyn TransportStream>,
	) -> std::io::Result<()> {
		let subscription_id = CdcSubscriptionId::new(
			(u128::from(self.local_node.id) << 64) | u128::from(frame.header.stream_id),
		);
		let bound = match req.bound() {
			Ok(bound) => bound,
			Err(_) => {
				let reject =
					CdcRejectMeta::new(subscription_id, CdcRejectCode::InternalEngineError);
				cdc_emit_reject(&mut *stream, frame.header.stream_id, &reject).await?;
				stream.finish().await?;
				return Ok(());
			}
		};
		let requested_logical_schema_fingerprint = req.decoded_logical_schema_fingerprint();
		let (descriptor, effective_retention) = {
			let state = self.state_machine.state_snapshot().await;
			let descriptor = state
				.table_schemas
				.latest_by_id(req.tenant_id, req.table_object_id)
				.cloned();
			let retention = self.table_effective_retention_from_state(
				&state,
				req.tenant_id,
				req.table_object_id,
			);
			(descriptor, retention)
		};
		let Some(descriptor) = descriptor else {
			let reject = CdcRejectMeta::new(subscription_id, CdcRejectCode::UnknownTable);
			cdc_emit_reject(&mut *stream, frame.header.stream_id, &reject).await?;
			stream.finish().await?;
			return Ok(());
		};
		if descriptor.logical_schema_fingerprint != requested_logical_schema_fingerprint {
			let reject = CdcRejectMeta::new(
				subscription_id,
				CdcRejectCode::SchemaFingerprintIncompatible,
			);
			cdc_emit_reject(&mut *stream, frame.header.stream_id, &reject).await?;
			stream.finish().await?;
			return Ok(());
		}
		if req.start_cursor != CdcCursor::MIN
			&& check_read_against_retention(req.start_cursor.row_hlc, &effective_retention).is_err()
		{
			let reject = CdcRejectMeta::new(subscription_id, CdcRejectCode::BelowRetentionFloor);
			cdc_emit_reject(&mut *stream, frame.header.stream_id, &reject).await?;
			stream.finish().await?;
			return Ok(());
		}
		let source_ranges = match table_query_scan_ranges(req.tenant_id, &descriptor, None) {
			Ok(ranges) => ranges,
			Err(_) => {
				let reject =
					CdcRejectMeta::new(subscription_id, CdcRejectCode::InternalEngineError);
				cdc_emit_reject(&mut *stream, frame.header.stream_id, &reject).await?;
				stream.finish().await?;
				return Ok(());
			}
		};
		if !self.cdc_table_maybe_replicated_here(&source_ranges) {
			let reject = CdcRejectMeta::new(subscription_id, CdcRejectCode::TableNotReplicatedHere);
			cdc_emit_reject(&mut *stream, frame.header.stream_id, &reject).await?;
			stream.finish().await?;
			return Ok(());
		}
		self.register_cdc_heartbeat_localities(&source_ranges);
		let snapshot = CatchupSnapshot::new(self.cdc_capture_snapshot_hlc(&source_ranges).await);
		if let Err(code) = self
			.cdc_open_time_ring_continuity_check(&source_ranges, &snapshot)
			.await
		{
			let reject = CdcRejectMeta::new(subscription_id, code);
			cdc_emit_reject(&mut *stream, frame.header.stream_id, &reject).await?;
			stream.finish().await?;
			return Ok(());
		}
		if let Some(resume_digest) = req.resume_digest() {
			let visible_rows = self
				.compute_cdc_visible_row_keys_at_cursor(
					req.tenant_id,
					&descriptor,
					&source_ranges,
					req.start_cursor,
				)
				.await?;
			let visible_digest =
				cdc_visible_rows_digest(req.tenant_id, descriptor.table_object_id, visible_rows);
			if visible_digest != resume_digest {
				let reject =
					CdcRejectMeta::new(subscription_id, CdcRejectCode::CheckpointDigestMismatch);
				cdc_emit_reject(&mut *stream, frame.header.stream_id, &reject).await?;
				stream.finish().await?;
				return Ok(());
			}
		}

		let mut live_registration = if bound == CdcSubscribeBound::Live {
			Some(self.cdc_registry.register(
				req.tenant_id,
				req.table_object_id,
				snapshot.ceiling(),
				bound,
				requested_logical_schema_fingerprint,
			))
		} else {
			None
		};
		let stream_subscription_id = live_registration
			.as_ref()
			.map(|(handle, _)| handle.subscription_id)
			.unwrap_or(subscription_id);
		if let Err(err) =
			self.emit_cdc_subscription_opened_audit(actor, &req, stream_subscription_id, bound)
		{
			if let Some((handle, _)) = &live_registration {
				self.cdc_registry.unregister(
					req.tenant_id,
					req.table_object_id,
					handle.subscription_id,
					crate::metrics::CdcSubscriptionMetricResult::Cancelled,
				);
			}
			return Err(err);
		}
		let catchup_floor_hlc = if req.start_cursor == CdcCursor::MIN {
			effective_retention
				.floor
				.unwrap_or(req.start_cursor.row_hlc)
		} else {
			req.start_cursor.row_hlc
		};
		let catchup_started = Instant::now();
		let catchup_events = match self
			.collect_cdc_catchup_events(
				req.tenant_id,
				&descriptor,
				&source_ranges,
				req.start_cursor,
				snapshot.ceiling(),
				catchup_floor_hlc,
			)
			.await
		{
			Ok(events) => events,
			Err(err) => {
				if let Some((handle, _)) = &live_registration {
					self.cdc_registry.unregister(
						req.tenant_id,
						req.table_object_id,
						handle.subscription_id,
						crate::metrics::CdcSubscriptionMetricResult::Cancelled,
					);
				}
				tracing::warn!(error = ?err, "CDC catchup collection failed");
				let reject =
					CdcRejectMeta::new(stream_subscription_id, CdcRejectCode::InternalEngineError);
				self.emit_cdc_subscription_closed_audit(
					actor,
					req.tenant_id,
					req.table_object_id,
					stream_subscription_id,
					"reject_internal_engine_error",
					req.start_cursor,
				)?;
				cdc_emit_reject(&mut *stream, frame.header.stream_id, &reject).await?;
				stream.finish().await?;
				return Ok(());
			}
		};
		let checkpoint_visible_rows = self
			.compute_cdc_visible_row_keys_at_cursor(
				req.tenant_id,
				&descriptor,
				&source_ranges,
				snapshot.ceiling(),
			)
			.await?;
		let checkpoint_digest = cdc_visible_rows_digest(
			req.tenant_id,
			descriptor.table_object_id,
			checkpoint_visible_rows,
		);

		match bound {
			CdcSubscribeBound::SnapshotAtOpen => {
				self.emit_cdc_catchup_events(
					&mut *stream,
					frame.header.stream_id,
					subscription_id,
					req.logical_schema_fingerprint,
					&catchup_events,
				)
				.await?;
				let checkpoint = CdcCheckpointMeta {
					subscription_id,
					cursor: snapshot.ceiling(),
					visible_state_digest: checkpoint_digest,
					digest_domain_raw: CdcDigestDomain::TableRowVisible.as_raw(),
					_pad: [0; 4],
				};
				cdc_emit_checkpoint(
					&mut *stream,
					frame.header.stream_id,
					&checkpoint,
					CdcFrameSource::Catchup,
				)
				.await?;
				let end = CdcFrameMeta {
					subscription_id,
					frame_low_cursor: snapshot.ceiling(),
					frame_high_cursor: snapshot.ceiling(),
					logical_schema_fingerprint: req.logical_schema_fingerprint,
					event_count: RowCount::ZERO,
					source_raw: CdcFrameSource::Catchup.as_raw(),
					_pad: [0; 7],
				};
				cdc_emit_empty_frame(&mut *stream, frame.header.stream_id, &end, true).await?;
				self.emit_cdc_subscription_closed_audit(
					actor,
					req.tenant_id,
					req.table_object_id,
					subscription_id,
					"snapshot_complete",
					snapshot.ceiling(),
				)?;
				stream.finish().await?;
				Ok(())
			}
			CdcSubscribeBound::Live => {
				let Some((handle, mut recv)) = live_registration.take() else {
					return Ok(());
				};
				self.emit_cdc_catchup_events(
					&mut *stream,
					frame.header.stream_id,
					handle.subscription_id,
					req.logical_schema_fingerprint,
					&catchup_events,
				)
				.await?;
				let checkpoint = CdcCheckpointMeta {
					subscription_id: handle.subscription_id,
					cursor: snapshot.ceiling(),
					visible_state_digest: checkpoint_digest,
					digest_domain_raw: CdcDigestDomain::TableRowVisible.as_raw(),
					_pad: [0; 4],
				};
				cdc_emit_checkpoint(
					&mut *stream,
					frame.header.stream_id,
					&checkpoint,
					CdcFrameSource::Catchup,
				)
				.await?;
				metrics().observe_cdc_catchup_to_live_handoff(catchup_started.elapsed());
				let mut pending_live_entries = self
					.cdc_collect_ring_entries_after(
						req.tenant_id,
						req.table_object_id,
						&source_ranges,
						snapshot.ceiling(),
					)
					.await;
				let mut checkpoint_cadence = CdcCheckpointCadence::new(Instant::now());
				let mut finalizations = self.state_machine.subscribe_finalizations();
				let mut finalization_stream_open = true;
				loop {
					if let Some(code) = self
						.cdc_live_retention_reject_code(
							req.tenant_id,
							req.table_object_id,
							handle.cursor_high_water.load(),
						)
						.await
					{
						let reject = CdcRejectMeta::new(handle.subscription_id, code);
						let emit =
							cdc_emit_reject(&mut *stream, frame.header.stream_id, &reject).await;
						self.cdc_registry.remove(
							req.tenant_id,
							req.table_object_id,
							handle.subscription_id,
						);
						self.emit_cdc_subscription_closed_audit(
							actor,
							req.tenant_id,
							req.table_object_id,
							handle.subscription_id,
							cdc_reject_audit_reason(code),
							handle.cursor_high_water.load(),
						)?;
						emit?;
						stream.finish().await?;
						return Ok(());
					}
					let safe_emission_hlc = self.cdc_safe_emission_hlc(&source_ranges).await;
					if let Some(recovery_upper) = self
						.cdc_live_recovery_upper_after(
							req.tenant_id,
							req.table_object_id,
							&source_ranges,
							handle.cursor_high_water.load(),
							safe_emission_hlc,
						)
						.await
					{
						let recovery_start = handle.cursor_high_water.load();
						let scan_floor = self
							.cdc_catchup_scan_floor_hlc(
								req.tenant_id,
								req.table_object_id,
								recovery_start,
							)
							.await;
						let recovery_events = match self
							.collect_cdc_catchup_events(
								req.tenant_id,
								&descriptor,
								&source_ranges,
								recovery_start,
								recovery_upper,
								scan_floor,
							)
							.await
						{
							Ok(events) => events,
							Err(err) => {
								tracing::warn!(error = ?err, "CDC live-tail recovery catchup failed");
								let reject = CdcRejectMeta::new(
									handle.subscription_id,
									CdcRejectCode::InternalEngineError,
								);
								let emit =
									cdc_emit_reject(&mut *stream, frame.header.stream_id, &reject)
										.await;
								self.cdc_registry.remove(
									req.tenant_id,
									req.table_object_id,
									handle.subscription_id,
								);
								self.emit_cdc_subscription_closed_audit(
									actor,
									req.tenant_id,
									req.table_object_id,
									handle.subscription_id,
									"reject_internal_engine_error",
									handle.cursor_high_water.load(),
								)?;
								emit?;
								stream.finish().await?;
								return Ok(());
							}
						};
						if self
							.emit_cdc_live_recovery_catchup_events(
								&mut *stream,
								frame.header.stream_id,
								&handle,
								req.logical_schema_fingerprint,
								&recovery_events,
								recovery_upper,
							)
							.await
							.is_err()
						{
							self.emit_cdc_subscription_closed_audit(
								actor,
								req.tenant_id,
								req.table_object_id,
								handle.subscription_id,
								"client_cancel",
								handle.cursor_high_water.load(),
							)?;
							self.cdc_registry.unregister(
								req.tenant_id,
								req.table_object_id,
								handle.subscription_id,
								crate::metrics::CdcSubscriptionMetricResult::Cancelled,
							);
							return Ok(());
						}
					}
					pending_live_entries.extend(
						self.cdc_collect_ring_entries_after(
							req.tenant_id,
							req.table_object_id,
							&source_ranges,
							handle.cursor_high_water.load(),
						)
						.await,
					);
					if self
						.drain_cdc_ready_live_entries(
							&mut *stream,
							frame.header.stream_id,
							&handle,
							req.tenant_id,
							&descriptor,
							&source_ranges,
							&mut pending_live_entries,
							safe_emission_hlc,
							&mut checkpoint_cadence,
						)
						.await
						.is_err()
					{
						self.emit_cdc_subscription_closed_audit(
							actor,
							req.tenant_id,
							req.table_object_id,
							handle.subscription_id,
							"client_cancel",
							handle.cursor_high_water.load(),
						)?;
						self.cdc_registry.unregister(
							req.tenant_id,
							req.table_object_id,
							handle.subscription_id,
							crate::metrics::CdcSubscriptionMetricResult::Cancelled,
						);
						return Ok(());
					}

					tokio::select! {
						maybe_entry = recv.recv() => {
							let Some(entry) = maybe_entry else {
								break;
							};
							pending_live_entries.push(entry);
						}
						() = self.cdc_locality_frontiers.wait_for_advance() => {}
						finalization = finalizations.recv(), if finalization_stream_open => {
							match finalization {
								Ok(_) => {}
								Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
								Err(tokio::sync::broadcast::error::RecvError::Closed) => {
									finalization_stream_open = false;
								}
							}
						}
					}
				}
				self.cdc_registry.unregister(
					req.tenant_id,
					req.table_object_id,
					handle.subscription_id,
					crate::metrics::CdcSubscriptionMetricResult::Closed,
				);
				self.emit_cdc_subscription_closed_audit(
					actor,
					req.tenant_id,
					req.table_object_id,
					handle.subscription_id,
					"closed",
					handle.cursor_high_water.load(),
				)?;
				stream.finish().await?;
				Ok(())
			}
		}
	}

	async fn emit_cdc_catchup_events(
		&self,
		stream: &mut dyn TransportStream,
		stream_id: u32,
		subscription_id: CdcSubscriptionId,
		logical_schema_fingerprint: [u8; 32],
		events: &[CdcCatchupEvent],
	) -> std::io::Result<()> {
		let _ = self;
		for event in events {
			let meta = CdcFrameMeta {
				subscription_id,
				frame_low_cursor: event.cursor,
				frame_high_cursor: event.cursor,
				logical_schema_fingerprint,
				event_count: RowCount::new(event.batch.num_rows() as u64),
				source_raw: CdcFrameSource::Catchup.as_raw(),
				_pad: [0; 7],
			};
			cdc_emit_frame(stream, stream_id, &meta, &event.batch, 0).await?;
		}
		Ok(())
	}

	async fn emit_cdc_live_recovery_catchup_events(
		&self,
		stream: &mut dyn TransportStream,
		stream_id: u32,
		handle: &Arc<SubscriptionHandle>,
		logical_schema_fingerprint: [u8; 32],
		events: &[CdcCatchupEvent],
		recovery_upper: CdcCursor,
	) -> std::io::Result<()> {
		let _ = self;
		for event in events {
			let meta = CdcFrameMeta {
				subscription_id: handle.subscription_id,
				frame_low_cursor: event.cursor,
				frame_high_cursor: event.cursor,
				logical_schema_fingerprint,
				event_count: RowCount::new(event.batch.num_rows() as u64),
				source_raw: CdcFrameSource::Catchup.as_raw(),
				_pad: [0; 7],
			};
			cdc_emit_frame(stream, stream_id, &meta, &event.batch, 0).await?;
			handle.cursor_high_water.advance_max(event.cursor);
		}
		handle.cursor_high_water.advance_max(recovery_upper);
		Ok(())
	}

	async fn drain_cdc_ready_live_entries(
		&self,
		stream: &mut dyn TransportStream,
		stream_id: u32,
		handle: &Arc<SubscriptionHandle>,
		tenant_id: TenantId,
		descriptor: &TableSchemaDescriptorV1,
		source_ranges: &[QueryScanRange],
		pending: &mut Vec<Arc<CdcLiveEntry>>,
		safe_emission_hlc: HybridTimestamp,
		checkpoint_cadence: &mut CdcCheckpointCadence,
	) -> std::io::Result<()> {
		pending.sort_by_key(|entry| entry.cursor);
		pending.dedup_by_key(|entry| entry.cursor);
		while !pending.is_empty() {
			let entry = pending[0].clone();
			if entry.cursor <= handle.cursor_high_water.load() {
				pending.remove(0);
				continue;
			}
			if entry.cursor.row_hlc > safe_emission_hlc {
				break;
			}
			pending.remove(0);
			let meta = CdcFrameMeta {
				subscription_id: handle.subscription_id,
				frame_low_cursor: entry.cursor,
				frame_high_cursor: entry.cursor,
				logical_schema_fingerprint: *entry.logical_schema_fingerprint.as_bytes(),
				event_count: RowCount::new(entry.record_batch.num_rows() as u64),
				source_raw: CdcFrameSource::Ring.as_raw(),
				_pad: [0; 7],
			};
			cdc_emit_frame(stream, stream_id, &meta, &entry.record_batch, 0).await?;
			handle.cursor_high_water.advance_max(entry.cursor);
			checkpoint_cadence.note_events(meta.event_count.get());
			let now = Instant::now();
			if checkpoint_cadence.checkpoint_due(now) {
				let visible_rows = self
					.compute_cdc_visible_row_keys_at_cursor(
						tenant_id,
						descriptor,
						source_ranges,
						entry.cursor,
					)
					.await?;
				let checkpoint = CdcCheckpointMeta {
					subscription_id: handle.subscription_id,
					cursor: entry.cursor,
					visible_state_digest: cdc_visible_rows_digest(
						tenant_id,
						descriptor.table_object_id,
						visible_rows,
					),
					digest_domain_raw: CdcDigestDomain::TableRowVisible.as_raw(),
					_pad: [0; 4],
				};
				cdc_emit_checkpoint(stream, stream_id, &checkpoint, CdcFrameSource::Ring).await?;
				checkpoint_cadence.mark_checkpoint(now);
			}
		}
		Ok(())
	}

	async fn collect_cdc_catchup_events(
		&self,
		tenant_id: TenantId,
		descriptor: &TableSchemaDescriptorV1,
		ranges: &[QueryScanRange],
		start_cursor: CdcCursor,
		upper_cursor: CdcCursor,
		scan_hlc_floor: HybridTimestamp,
	) -> std::io::Result<Vec<CdcCatchupEvent>> {
		let mut candidates = Vec::new();
		let mut seen = HashSet::new();
		for range in ranges {
			for (row_tenant_id, key_identity, locality, location) in
				self.slab
					.scan_all_versions(range.start, range.end, scan_hlc_floor)
			{
				if row_tenant_id != tenant_id {
					continue;
				}
				let cursor = CdcCursor::new(location.hlc, key_identity, location.record_row_index);
				if !cdc_cursor_in_catchup_window(cursor, start_cursor, upper_cursor) {
					continue;
				}
				if seen.insert((row_tenant_id, key_identity, location)) {
					candidates.push((cursor, key_identity, locality, location));
				}
			}
		}
		candidates.sort_by_key(|(cursor, _, _, _)| *cursor);

		let query_meta = cdc_descriptor_match_query_meta(tenant_id, upper_cursor.row_hlc);
		let mut events = Vec::new();
		for (cursor, key_identity, locality, location) in candidates {
			let bytes = self.slab.read(location).await?;
			let storage_batch = decode_batch(&bytes)?;
			let was_multirow = has_storage_synthetic_columns(&storage_batch);
			let stored_table_object_id = if was_multirow {
				storage_row_table_object_id_at(&storage_batch, location.record_row_index)?
			} else {
				None
			};
			if location.is_tombstone {
				if stored_table_object_id != Some(descriptor.table_object_id) {
					continue;
				}
				let source_schema = descriptor.arrow_schema()?;
				let batch = cdc_tombstone_event_batch(
					descriptor.logical_schema_fingerprint,
					location.hlc,
					key_identity,
					descriptor.table_object_id,
					Some(locality),
					location.record_row_index,
					None,
					&source_schema,
				)
				.map_err(|err| Error::new(ErrorKind::InvalidData, err))?;
				events.push(CdcCatchupEvent { cursor, batch });
				continue;
			}
			let materialized =
				materialize_record_location_batch(&storage_batch, location.record_row_index)?;
			if !table_record_matches_descriptor(
				&query_meta,
				descriptor,
				key_identity,
				locality,
				stored_table_object_id,
				&materialized,
			)? {
				continue;
			}
			let batch = cdc_event_batch(
				descriptor.logical_schema_fingerprint,
				location.hlc,
				key_identity,
				descriptor.table_object_id,
				Some(locality),
				location.record_row_index,
				None,
				&materialized,
			)
			.map_err(|err| Error::new(ErrorKind::InvalidData, err))?;
			events.push(CdcCatchupEvent { cursor, batch });
		}
		Ok(events)
	}

	async fn compute_cdc_visible_row_keys_at_cursor(
		&self,
		tenant_id: TenantId,
		descriptor: &TableSchemaDescriptorV1,
		ranges: &[QueryScanRange],
		as_of_cursor: CdcCursor,
	) -> std::io::Result<Vec<Identity>> {
		let mut latest_by_key: HashMap<Identity, (CdcCursor, bool)> = HashMap::new();
		let mut seen = HashSet::new();
		let query_meta = cdc_descriptor_match_query_meta(tenant_id, as_of_cursor.row_hlc);
		for range in ranges {
			for (row_tenant_id, key_identity, locality, location) in
				self.slab
					.scan_all_versions(range.start, range.end, bytemuck::Zeroable::zeroed())
			{
				if row_tenant_id != tenant_id {
					continue;
				}
				let cursor = CdcCursor::new(location.hlc, key_identity, location.record_row_index);
				if cursor > as_of_cursor || !seen.insert((row_tenant_id, key_identity, location)) {
					continue;
				}
				let replace = latest_by_key
					.get(&key_identity)
					.is_none_or(|(previous, _)| cursor > *previous);
				if !replace {
					continue;
				}
				let bytes = self.slab.read(location).await?;
				let storage_batch = decode_batch(&bytes)?;
				let was_multirow = has_storage_synthetic_columns(&storage_batch);
				let stored_table_object_id = if was_multirow {
					storage_row_table_object_id_at(&storage_batch, location.record_row_index)?
				} else {
					None
				};
				if location.is_tombstone {
					if stored_table_object_id == Some(descriptor.table_object_id) {
						latest_by_key.insert(key_identity, (cursor, false));
					}
					continue;
				}
				let materialized =
					materialize_record_location_batch(&storage_batch, location.record_row_index)?;
				if table_record_matches_descriptor(
					&query_meta,
					descriptor,
					key_identity,
					locality,
					stored_table_object_id,
					&materialized,
				)? {
					latest_by_key.insert(key_identity, (cursor, true));
				}
			}
		}

		Ok(latest_by_key
			.into_iter()
			.filter_map(|(row_key_id, (_, visible))| visible.then_some(row_key_id))
			.collect())
	}

	async fn cdc_open_time_ring_continuity_check(
		&self,
		ranges: &[QueryScanRange],
		snapshot: &CatchupSnapshot,
	) -> Result<(), CdcRejectCode> {
		for ring in self.cdc_rings_for_ranges(ranges) {
			ring.open_time_ring_continuity_check(snapshot).await?;
		}
		Ok(())
	}

	async fn cdc_collect_ring_entries_after(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		ranges: &[QueryScanRange],
		after: CdcCursor,
	) -> Vec<Arc<CdcLiveEntry>> {
		let mut entries = Vec::new();
		for ring in self.cdc_rings_for_ranges(ranges) {
			entries.extend(
				ring.eligible_entries_after(
					tenant_id,
					table_object_id,
					after,
					CdcCursor::MAX.row_hlc,
					usize::MAX,
				)
				.await,
			);
		}
		entries.sort_by_key(|entry| entry.cursor);
		entries
	}

	async fn cdc_live_recovery_upper_after(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		ranges: &[QueryScanRange],
		after: CdcCursor,
		safe_emission_hlc: HybridTimestamp,
	) -> Option<CdcCursor> {
		let safe_ceiling = CdcCursor::ceiling_for_hlc(safe_emission_hlc);
		let mut upper = None;
		for ring in self.cdc_rings_for_ranges(ranges) {
			let Some(frontier) = ring
				.evicted_cursor_frontier(tenant_id, table_object_id)
				.await
			else {
				continue;
			};
			let capped = frontier.min(safe_ceiling);
			if capped > after {
				upper = Some(upper.map_or(capped, |current: CdcCursor| current.max(capped)));
			}
		}
		upper
	}

	async fn cdc_catchup_scan_floor_hlc(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		start_cursor: CdcCursor,
	) -> HybridTimestamp {
		if start_cursor != CdcCursor::MIN {
			return start_cursor.row_hlc;
		}
		let state = self.state_machine.state_snapshot().await;
		self.table_effective_retention_from_state(&state, tenant_id, table_object_id)
			.floor
			.unwrap_or(start_cursor.row_hlc)
	}

	async fn cdc_safe_emission_hlc(&self, ranges: &[QueryScanRange]) -> HybridTimestamp {
		if let Some(frontier) = self.cdc_min_locality_frontier_for_ranges(ranges) {
			return frontier;
		}
		let state = self.state_machine.state_snapshot().await;
		cdc_snapshot_hlc_from_state(&state)
	}

	async fn cdc_capture_snapshot_hlc(&self, ranges: &[QueryScanRange]) -> HybridTimestamp {
		if let Some(frontier) = self.cdc_min_locality_frontier_for_ranges(ranges) {
			return frontier;
		}
		let state = self.state_machine.state_snapshot().await;
		cdc_snapshot_hlc_from_state(&state)
	}

	async fn cdc_live_retention_reject_code(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		cursor: CdcCursor,
	) -> Option<CdcRejectCode> {
		let state = self.state_machine.state_snapshot().await;
		let effective_retention =
			self.table_effective_retention_from_state(&state, tenant_id, table_object_id);
		cdc_retention_reject_code_for_cursor(cursor, &effective_retention)
	}

	fn cdc_min_locality_frontier_for_ranges(
		&self,
		ranges: &[QueryScanRange],
	) -> Option<HybridTimestamp> {
		let source_localities = self.cdc_source_localities_for_ranges(ranges);
		self.cdc_locality_frontiers
			.min_for_localities(source_localities.iter().copied())
	}

	fn register_cdc_heartbeat_localities(&self, ranges: &[QueryScanRange]) {
		for locality_id in self.cdc_source_localities_for_ranges(ranges) {
			self.register_cdc_heartbeat_locality(locality_id);
		}
	}

	fn register_cdc_heartbeat_localities_for_primary_ranges(&self, ring: &RingRouter) {
		for locality_id in
			cdc_heartbeat_localities_for_primary_ranges(ring.primary_ranges_for(self.local_node))
		{
			self.register_cdc_heartbeat_locality(locality_id);
		}
	}

	fn register_cdc_heartbeat_locality(&self, locality_id: Locality) {
		self.cdc_heartbeat_localities.insert(locality_id, ());
		if let Some(frontier) = self.cdc_locality_frontiers.get(locality_id) {
			self.cdc_heartbeat_observed_frontiers
				.entry(locality_id)
				.or_insert(frontier);
		}
	}

	fn cdc_locality_quiet_since_last_finalization(&self, locality_id: Locality) -> bool {
		let current = self.cdc_locality_frontiers.get(locality_id);
		let observed = self
			.cdc_heartbeat_observed_frontiers
			.get(&locality_id)
			.map(|entry| *entry.value());
		match (observed, current) {
			(Some(observed), Some(current)) if current > observed => {
				self.cdc_heartbeat_observed_frontiers
					.insert(locality_id, current);
				false
			}
			(None, Some(current)) => {
				self.cdc_heartbeat_observed_frontiers
					.insert(locality_id, current);
				false
			}
			_ => true,
		}
	}

	async fn emit_cdc_heartbeat_finalization_marker(
		&self,
		locality_id: Locality,
	) -> std::io::Result<()> {
		if self.raft.current_leader().await != Some(self.local_node.id) {
			return Ok(());
		}
		let commit_hlc = self.merge_commit_hlc(HybridTimestamp {
			physical_ms: 0,
			logical: 0,
			node_id: 0,
		});
		let epoch = self.admission_snapshot().current_epoch;
		let finalization = TenantFinalization {
			marker: FinalizationMarker {
				log_index: 0,
				term: 0,
				epoch,
				commit_hlc,
				payload_hash: cdc_heartbeat_payload_hash(locality_id, commit_hlc, epoch),
				e_safe_index: 0,
				durability: DurabilityLevel::QUORUM,
				_pad: [0; 7],
			},
			tenant_id: TenantId::ZERO,
			key_identity: cdc_heartbeat_identity(locality_id),
			charged_bytes: 0,
			record_count: 0,
		};
		let response = self
			.raft
			.client_write(WendelonRequest::Finalize(finalization))
			.await
			.map_err(|err| Error::other(format!("cdc heartbeat finalization failed: {err}")))?;
		self.cdc_locality_frontiers.advance(locality_id, commit_hlc);
		self.cdc_heartbeat_observed_frontiers
			.insert(locality_id, commit_hlc);
		tracing::debug!(
			locality = locality_id.get(),
			epoch,
			log_index = response.log_id.index,
			"CDC locality heartbeat finalization committed"
		);
		Ok(())
	}

	fn cdc_table_maybe_replicated_here(&self, ranges: &[QueryScanRange]) -> bool {
		cdc_ranges_maybe_replicated_by_local_node(ranges, self.local_node, |locality| {
			self.router.replicas_for_locality(locality)
		})
	}

	fn cdc_source_localities_for_ranges(&self, ranges: &[QueryScanRange]) -> Vec<Locality> {
		cdc_source_localities_from_ranges_and_rings(
			ranges,
			self.cdc_rings_for_ranges(ranges)
				.into_iter()
				.map(|ring| ring.locality_id()),
		)
	}

	fn cdc_rings_for_ranges(&self, ranges: &[QueryScanRange]) -> Vec<Arc<CdcLiveTailRing>> {
		self.cdc_live_tail_rings
			.iter()
			.filter_map(|entry| {
				cdc_locality_in_ranges(*entry.key(), ranges).then(|| entry.value().clone())
			})
			.collect()
	}

	fn cdc_ring_for_locality(&self, locality: Locality) -> Arc<CdcLiveTailRing> {
		self.cdc_live_tail_rings
			.entry(locality)
			.or_insert_with(|| {
				Arc::new(CdcLiveTailRing::new(
					locality,
					DEFAULT_MAX_CDC_LIVE_TAIL_BYTES_PER_LOCALITY,
				))
			})
			.clone()
	}

	fn cdc_existing_ring_for_locality(&self, locality: Locality) -> Option<Arc<CdcLiveTailRing>> {
		self.cdc_live_tail_rings
			.get(&locality)
			.map(|entry| entry.value().clone())
	}

	fn cdc_release_finalized_entry(&self, locality: Locality, entry: Arc<CdcLiveEntry>) {
		self.cdc_locality_frontiers
			.advance(locality, entry.cursor.row_hlc);
		self.cdc_registry.fanout_finalized_entry(entry);
	}

	async fn cdc_release_staged_entries(&self, staged: Vec<CdcStagedEntryRef>) -> usize {
		let mut released = 0usize;
		for staged_entry in staged {
			self.cdc_locality_frontiers
				.advance(staged_entry.locality, staged_entry.cursor.row_hlc);
			let Some(ring) = self.cdc_existing_ring_for_locality(staged_entry.locality) else {
				metrics().observe_cdc_pending_finalization_evicted();
				continue;
			};
			if let Some(entry) = ring.mark_finalized_entry(staged_entry.cursor).await {
				self.cdc_release_finalized_entry(staged_entry.locality, entry);
				released = released.saturating_add(1);
			} else {
				metrics().observe_cdc_pending_finalization_evicted();
			}
		}
		released
	}

	async fn cdc_release_pending_finalization(&self, pending_id: PendingFinalizationId) -> usize {
		let Some((_, staged)) = self.cdc_pending_live_entries.remove(&pending_id) else {
			return 0;
		};
		self.cdc_release_staged_entries(staged).await
	}

	async fn cdc_release_pending_finalization_marker(&self, marker: FinalizationMarker) -> usize {
		let key = CdcFinalizationKey::from_marker(marker);
		let Some((_, staged)) = self.cdc_marker_pending_live_entries.remove(&key) else {
			return 0;
		};
		self.cdc_release_staged_entries(staged).await
	}

	fn cdc_stage_pending_finalization_marker(
		&self,
		key: CdcFinalizationKey,
		staged: Vec<CdcStagedEntryRef>,
	) {
		if staged.is_empty() {
			return;
		}
		self.cdc_marker_pending_live_entries
			.entry(key)
			.and_modify(|existing| existing.extend(staged.iter().copied()))
			.or_insert(staged);
	}

	#[allow(dead_code)]
	async fn cdc_evict_pending_finalization(&self, pending_id: PendingFinalizationId) -> usize {
		let Some((_, staged)) = self.cdc_pending_live_entries.remove(&pending_id) else {
			return 0;
		};
		let mut evicted = 0usize;
		for staged_entry in staged {
			let Some(ring) = self.cdc_existing_ring_for_locality(staged_entry.locality) else {
				evicted = evicted.saturating_add(1);
				metrics().observe_cdc_pending_finalization_evicted();
				continue;
			};
			let _ = ring.evict_cursor(staged_entry.cursor).await;
			evicted = evicted.saturating_add(1);
			metrics().observe_cdc_pending_finalization_evicted();
		}
		evicted
	}

	async fn stage_cdc_table_children(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		descriptor: &TableSchemaDescriptorV1,
		commit_hlc: HybridTimestamp,
		children: &[BifurcatedTableChildWrite],
		finalized: bool,
	) -> std::io::Result<Vec<CdcStagedEntryRef>> {
		let mut staged = Vec::with_capacity(children.len());
		for child in children {
			self.register_cdc_heartbeat_locality(child.locality);
			let storage_batch = decode_batch(child.frame_bytes.as_slice())?;
			let is_tombstone = storage_row_is_tombstone_at(&storage_batch, child.record_row_index)?;
			let batch = if is_tombstone {
				let source_schema = descriptor.arrow_schema()?;
				Arc::new(
					cdc_tombstone_event_batch(
						descriptor.logical_schema_fingerprint,
						commit_hlc,
						child.key_identity,
						table_object_id,
						Some(child.locality),
						child.record_row_index,
						None,
						&source_schema,
					)
					.map_err(|err| Error::new(ErrorKind::InvalidData, err))?,
				)
			} else {
				let materialized =
					materialize_record_location_batch(&storage_batch, child.record_row_index)?;
				Arc::new(
					cdc_event_batch(
						descriptor.logical_schema_fingerprint,
						commit_hlc,
						child.key_identity,
						table_object_id,
						Some(child.locality),
						child.record_row_index,
						None,
						&materialized,
					)
					.map_err(|err| Error::new(ErrorKind::InvalidData, err))?,
				)
			};
			let entry = Arc::new(
				CdcLiveEntry::new(
					CdcCursor::new(commit_hlc, child.key_identity, child.record_row_index),
					tenant_id,
					table_object_id,
					descriptor.logical_schema_fingerprint,
					batch,
				)
				.with_finalized(finalized),
			);
			let ring = self.cdc_ring_for_locality(child.locality);
			ring.push_arc(entry.clone()).await;
			staged.push(CdcStagedEntryRef {
				locality: child.locality,
				cursor: CdcCursor::new(commit_hlc, child.key_identity, child.record_row_index),
			});
			if finalized {
				self.cdc_release_finalized_entry(child.locality, entry);
			}
		}
		Ok(staged)
	}

	async fn stage_cdc_table_replica_children(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		descriptor: &TableSchemaDescriptorV1,
		commit_hlc: HybridTimestamp,
		children: &[TableReplicaChildPayload],
		finalized: bool,
	) -> std::io::Result<Vec<CdcStagedEntryRef>> {
		let bifurcated_children = children
			.iter()
			.map(|child| BifurcatedTableChildWrite {
				child_ordinal: child.child_ordinal,
				key_identity: child.key_identity,
				locality: child.locality,
				record_row_index: child.record_row_index,
				frame_bytes: child.frame_bytes.clone(),
				payload_hash: child.payload_hash,
				charged_bytes: child.charged_bytes,
			})
			.collect::<Vec<_>>();
		self.stage_cdc_table_children(
			tenant_id,
			table_object_id,
			descriptor,
			commit_hlc,
			&bifurcated_children,
			finalized,
		)
		.await
	}

	async fn write_bifurcated(
		&self,
		meta: WriteRequestMeta,
		bifurcated: BifurcatedWrite,
	) -> std::io::Result<(WriteResponseMeta, Option<FinalizationMarker>)> {
		let key = Self::idempotency_key_for(meta.write_id);

		// Non-primary redirect must happen before the fresh-leader
		// orphan-witness sweep. A node that does not own this range
		// should return `NOT_LEADER` cheaply rather than failing on a
		// peer-sweep precondition that is only relevant to primary-owned
		// replay and write admission.
		let lease_gate_is_primary = if self.lease_gate_enabled {
			self.router
				.replicas_for_locality(bifurcated.locality)
				.first()
				.map(|n| n.id == self.local_node.id)
				.unwrap_or(false)
		} else {
			false
		};
		if self.lease_gate_enabled && !lease_gate_is_primary {
			return Ok(self.counted_write_result(
				error_response(&meta, WriteStatus::NOT_LEADER, self.local_node),
				None,
			));
		}

		// Fresh leaders block cache-miss admission until orphan-witness
		// recovery for the current `(term, membership_epoch)` is
		// complete. Cached outcomes are already authoritative for this
		// node; the barrier exists to close the duplicate-retry window
		// before consulting durable pending witnesses or admitting a
		// fresh primary-owned write across the bifurcation boundary.
		self.ensure_orphan_witnesses_recovered_for_leadership()
			.await?;

		// Restart-safe pending replay path. A recovered server starts
		// with an empty idempotency cache, so consult the durable
		// pending-intent store before crossing the bifurcation boundary
		// again.
		if let Some(record) = self
			.pending_finalizations
			.find_by_write_id(meta.write_id)
			.await?
		{
			let outcome = self.recoverable_pending_outcome_for_record(&record);
			self.idempotency
				.insert(key, CachedWriteOutcome::new(outcome));
			return self.replay_write_outcome(outcome).await;
		}

		// Per-range lease gate (plan 018). When enabled, this node
		// accepts the write only if it holds a valid lease covering
		// the child's routing locality in the client's epoch.
		if self.lease_gate_enabled {
			match self
				.lease_gate_result(bifurcated.locality, meta.expected_epoch, meta.client_hlc)
				.await
			{
				LeaseGateResult::Valid => {}
				LeaseGateResult::Missing => {
					return Ok(self.counted_write_result(
						error_response(&meta, WriteStatus::LEASE_EXPIRED, self.local_node),
						None,
					));
				}
				LeaseGateResult::InvalidSignature => {
					return Ok(self.counted_write_result(
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

		let _quota_reservation = match self
			.enforce_write_policy(meta.tenant_id, bifurcated.charged_bytes)
			.await
		{
			Ok(reservation) => reservation,
			Err(status) => {
				return Ok(
					self.counted_write_result(error_response(&meta, status, self.local_node), None)
				);
			}
		};

		// HLC for this write. The leader is authoritative: it merges
		// the client's timestamp with its own monotonic HLC view and
		// stamps `max(leader_hlc, client_hlc) + 1`.
		let commit_hlc = self.merge_commit_hlc(meta.client_hlc);

		let replicas = self.router.replicas_for_locality(bifurcated.locality);
		if replicas.is_empty() {
			return Ok(self.counted_write_result(
				error_response(&meta, WriteStatus::INTERNAL_ERROR, self.local_node),
				None,
			));
		}
		let rf = replicas.len();
		let quorum = (rf / 2) + 1;
		let local_is_replica = replicas.iter().any(|n| n.id == self.local_node.id);
		let mut quorum_tracker = DurableQuorumTracker::new(quorum, local_is_replica);
		let durable_quorum_started = Instant::now();

		let mut local_durable_task: Option<JoinHandle<std::io::Result<RecordLocation>>> =
			if local_is_replica {
				let slab = self.slab.clone();
				let bytes = bifurcated.frame_bytes.clone();
				let tenant_id = meta.tenant_id;
				let key_identity = bifurcated.key_identity;
				#[cfg(test)]
				let fail_next_local_durable_append = self.fail_next_local_durable_append.clone();
				Some(tokio::spawn(async move {
					#[cfg(test)]
					if let Some((kind, message)) = fail_next_local_durable_append
						.lock()
						.expect("local durable append failpoint poisoned")
						.take()
					{
						return Err(Error::new(
							kind,
							format!("injected local durable append failure: {message}"),
						));
					}
					slab.append_durable(tenant_id, key_identity, commit_hlc, bytes.as_slice())
						.await
				}))
			} else {
				None
			};
		let replica_meta = ReplicaWriteMeta {
			commit_hlc,
			write_id: meta.write_id,
			epoch: meta.expected_epoch,
			tenant_id: meta.tenant_id,
			charged_bytes: bifurcated.charged_bytes,
			key_identity: bifurcated.key_identity,
			payload_hash: bifurcated.payload_hash,
			durability: meta.durability,
			await_finalization: meta.await_finalization,
			_pad: [0; 6],
		};
		let mut join: JoinSet<std::io::Result<ReplicaAckLevel>> = JoinSet::new();
		for target in replicas.iter().copied() {
			if target.id == self.local_node.id {
				continue;
			}
			let factory = self.factory.clone();
			let pool = self.pool.clone();
			let bytes = bifurcated.frame_bytes.clone();
			join.spawn(async move {
				Self::replicate_to_remote(factory, pool, target, replica_meta, bytes).await
			});
		}

		while !quorum_tracker.is_satisfied() {
			tokio::select! {
				local = async {
					match local_durable_task.as_mut() {
						Some(task) => Some(task.await),
						None => None,
					}
				}, if local_durable_task.is_some() => {
					match local {
						Some(Ok(Ok(_location))) => {
							quorum_tracker.note_local_durable();
						}
						Some(Ok(Err(err))) => {
							self.trip_local_persistence_distrust(format!(
								"local durable append returned error: {err}"
							))
							.await;
							return Ok(self.fenced_write_response(&meta).await);
						}
						Some(Err(err)) => {
							self.trip_local_persistence_distrust(format!(
								"local durable append task failed: {err}"
							))
							.await;
							return Ok(self.fenced_write_response(&meta).await);
						}
						None => continue,
					}
					local_durable_task = None;
				}
				remote = join.join_next(), if !join.is_empty() => {
					match remote {
						Some(Ok(Ok(level))) => quorum_tracker.note_remote_ack(level),
						Some(_) => { /* leg failed — keep racing */ }
						None => {}
					}
				}
				else => break,
			}
		}

		if !quorum_tracker.is_satisfied() {
			return Ok(self.counted_write_result(
				error_response(&meta, WriteStatus::INTERNAL_ERROR, self.local_node),
				None,
			));
		}
		metrics().observe_durable_quorum_latency(durable_quorum_started.elapsed());

		let pending = self.pending_finalization_record(&meta, &bifurcated, commit_hlc);
		let _pending_claim = self
			.try_claim_pending_finalization(pending.id)
			.ok_or_else(|| {
				Error::other("pending finalization for this write is already being processed")
			})?;
		if let Err(err) = self.pending_finalizations.upsert(pending.clone()).await {
			tracing::error!(
				pending_id = %format_bytes_hex(pending.id.as_bytes()),
				write_id = ?meta.write_id,
				tenant_id = meta.tenant_id.get(),
				error = %err,
				"failed to persist pending finalization; attempting immediate finalization before fencing node"
			);
			match self.resolve_pending_finalization(&pending).await {
				Ok(resolution) => {
					let committed = self
						.complete_pending_finalization(&pending, resolution)
						.await
						.map(|(response, trailing)| self.counted_write_result(response, trailing));
					self.trip_local_persistence_distrust(format!(
						"pending finalization intent persistence failed after durable quorum: {err}"
					))
					.await;
					return committed;
				}
				Err(finalize_err) => {
					tracing::error!(
						pending_id = %format_bytes_hex(pending.id.as_bytes()),
						write_id = ?meta.write_id,
						tenant_id = meta.tenant_id.get(),
						error = %finalize_err,
						"immediate finalization also failed after pending intent persistence failure; fencing node"
					);
					self.trip_local_persistence_distrust(format!(
						"pending finalization intent persistence failed after durable quorum: {err}; immediate finalization failed: {finalize_err}"
					))
					.await;
					return Ok(self.fenced_write_response(&meta).await);
				}
			}
		} else {
			self.refresh_pending_finalization_backlog_metric();
		}

		match self.resolve_pending_finalization(&pending).await {
			Ok(resolution) => self
				.complete_pending_finalization(&pending, resolution)
				.await
				.map(|(response, trailing)| self.counted_write_result(response, trailing)),
			Err(err) => {
				tracing::warn!(
					pending_id = %format_bytes_hex(pending.id.as_bytes()),
					error = %err,
					"finalization commit failed after the data branch reached durable quorum"
				);
				let response = self.recoverable_pending_response(
					meta.write_id,
					meta.expected_epoch,
					commit_hlc,
				);
				self.idempotency.insert(
					key,
					CachedWriteOutcome::new(WriteOutcome::FinalizationRecoverablePending {
						response,
						pending_finalization_id: pending.id,
					}),
				);
				self.pending_finalization_wakeup.notify_one();
				Ok(self.counted_write_result(response, None))
			}
		}
	}

	fn table_batch_replica_groups(
		&self,
		children: &[BifurcatedTableChildWrite],
	) -> Option<Vec<TableReplicaGroup>> {
		let mut groups: Vec<TableReplicaGroup> = Vec::new();
		for child in children {
			let replicas = self.router.replicas_for_locality(child.locality);
			if replicas.is_empty() {
				return None;
			}
			if let Some(group) = groups.iter_mut().find(|group| group.replicas == replicas) {
				group.children.push(child.clone());
			} else {
				groups.push(TableReplicaGroup {
					replicas,
					children: vec![child.clone()],
				});
			}
		}
		Some(groups)
	}

	fn secondary_index_admission_descriptors(
		catalog: &SecondaryIndexCatalog,
		tenant_id: TenantId,
		table: &TableSchemaDescriptorV1,
	) -> Vec<SecondaryIndexDescriptor> {
		catalog
			.for_table(tenant_id, table.table_object_id)
			.into_iter()
			.filter(|row| {
				matches!(
					row.state,
					SecondaryIndexState::Building | SecondaryIndexState::Ready
				) && row.descriptor.table_schema_generation == table.schema_generation
					&& row.descriptor.logical_schema_fingerprint == table.logical_schema_fingerprint
					&& row.descriptor.validate_against_table(table).is_ok()
			})
			.map(|row| row.descriptor.clone())
			.collect()
	}

	async fn write_table_bifurcated(
		&self,
		meta: TableWriteRequestMeta,
		mut bifurcated: BifurcatedTableWrite,
		mut groups: Vec<TableReplicaGroup>,
		descriptor: &TableSchemaDescriptorV1,
		commit_hlc: HybridTimestamp,
		is_append_only: bool,
		secondary_index_descriptors: Vec<SecondaryIndexDescriptor>,
	) -> std::io::Result<(WriteResponseMeta, Option<FinalizationMarker>)> {
		let mut synthetic = table_write_synthetic_meta(
			&meta,
			bifurcated.parent_key_identity,
			bifurcated.charged_bytes,
		);
		let key = Self::idempotency_key_for(meta.parent_write_id);

		let lease_gate_is_primary_for_all_groups = if self.lease_gate_enabled {
			groups.iter().all(|group| {
				group
					.replicas
					.first()
					.map(|n| n.id == self.local_node.id)
					.unwrap_or(false)
			})
		} else {
			true
		};
		if self.lease_gate_enabled && !lease_gate_is_primary_for_all_groups {
			return Ok(self.counted_write_result(
				error_response(&synthetic, WriteStatus::NOT_LEADER, self.local_node),
				None,
			));
		}

		self.ensure_orphan_witnesses_recovered_for_leadership()
			.await?;

		if let Some(record) = self
			.pending_finalizations
			.find_by_write_id(meta.parent_write_id)
			.await?
		{
			let outcome = self.recoverable_pending_outcome_for_record(&record);
			self.idempotency
				.insert(key, CachedWriteOutcome::new(outcome));
			return self.replay_write_outcome(outcome).await;
		}

		if self.lease_gate_enabled {
			for child in &bifurcated.children {
				match self
					.lease_gate_result(child.locality, meta.expected_epoch, meta.client_hlc)
					.await
				{
					LeaseGateResult::Valid => {}
					LeaseGateResult::Missing => {
						return Ok(self.counted_write_result(
							error_response(&synthetic, WriteStatus::LEASE_EXPIRED, self.local_node),
							None,
						));
					}
					LeaseGateResult::InvalidSignature => {
						return Ok(self.counted_write_result(
							error_response(
								&synthetic,
								WriteStatus::INVALID_LEASE_SIGNATURE,
								self.local_node,
							),
							None,
						));
					}
				}
			}
		}

		// `commit_hlc` was stamped by the caller (see ADR 045 plumbing
		// in `table_write`) and already used during admission and
		// multi-row packing. The restamp call below is now a defensive
		// idempotent guard — it rewrites `__row_hlc__` to the same HLC
		// that admission consumed, so it's effectively a no-op for the
		// table-write path. Kept in place to defend against any future
		// caller that reaches this function without pre-stamping.
		let record_body_compression_policy = bifurcated.record_body_compression_policy;
		restamp_table_write_commit_hlc(
			&mut bifurcated,
			commit_hlc,
			record_body_compression_policy,
		)?;
		synthetic = table_write_synthetic_meta(
			&meta,
			bifurcated.parent_key_identity,
			bifurcated.charged_bytes,
		);
		let _quota_reservation = match self
			.enforce_write_policy(meta.tenant_id, bifurcated.charged_bytes)
			.await
		{
			Ok(reservation) => reservation,
			Err(status) => {
				return Ok(self.counted_write_result(
					error_response(&synthetic, status, self.local_node),
					None,
				));
			}
		};
		groups = self
			.table_batch_replica_groups(&bifurcated.children)
			.ok_or_else(|| {
				Error::new(ErrorKind::InvalidData, "table replica group restamp failed")
			})?;

		if groups.is_empty() {
			return Ok(self.counted_write_result(
				error_response(&synthetic, WriteStatus::INTERNAL_ERROR, self.local_node),
				None,
			));
		}
		let mut quorum_trackers = groups
			.iter()
			.map(|group| {
				let rf = group.replicas.len();
				let quorum = (rf / 2) + 1;
				let local_is_replica = group.replicas.iter().any(|n| n.id == self.local_node.id);
				DurableQuorumTracker::new(quorum, local_is_replica)
			})
			.collect::<Vec<_>>();
		let durable_quorum_started = Instant::now();

		let local_groups = groups
			.iter()
			.enumerate()
			.filter(|(_index, group)| group.replicas.iter().any(|n| n.id == self.local_node.id))
			.map(|(index, group)| (index, group.children.clone()))
			.collect::<Vec<_>>();
		let mut local_durable_task: Option<JoinHandle<std::io::Result<Vec<usize>>>> =
			if local_groups.is_empty() {
				None
			} else {
				let slab = self.slab.clone();
				let tenant_id = meta.tenant_id;
				let secondary_index_descriptors = secondary_index_descriptors.clone();
				#[cfg(test)]
				let fail_next_local_durable_append = self.fail_next_local_durable_append.clone();
				Some(tokio::spawn(async move {
					#[cfg(test)]
					if let Some((kind, message)) = fail_next_local_durable_append
						.lock()
						.expect("local durable append failpoint poisoned")
						.take()
					{
						return Err(Error::new(
							kind,
							format!("injected local durable append failure: {message}"),
						));
					}
					let mut completed = Vec::with_capacity(local_groups.len());
					for (index, children) in local_groups {
						append_bifurcated_table_children_durable(
							&slab,
							tenant_id,
							commit_hlc,
							&children,
							&secondary_index_descriptors,
						)
						.await?;
						completed.push(index);
					}
					Ok(completed)
				}))
			};
		let mut join: JoinSet<std::io::Result<(usize, ReplicaAckLevel)>> = JoinSet::new();
		for (group_index, group) in groups.iter().enumerate() {
			let replica_payload = Arc::new(encode_table_replica_children(&group.children)?);
			let replica_meta = table_replica_meta(&meta, &bifurcated, &group.children, commit_hlc);
			for target in group.replicas.iter().copied() {
				if target.id == self.local_node.id {
					continue;
				}
				let factory = self.factory.clone();
				let pool = self.pool.clone();
				let bytes = replica_payload.clone();
				join.spawn(async move {
					Self::replicate_table_batch_to_remote(
						factory,
						pool,
						target,
						replica_meta,
						bytes,
					)
					.await
					.map(|ack| (group_index, ack))
				});
			}
		}

		while !quorum_trackers
			.iter()
			.all(DurableQuorumTracker::is_satisfied)
		{
			tokio::select! {
				local = async {
					match local_durable_task.as_mut() {
						Some(task) => Some(task.await),
						None => None,
					}
				}, if local_durable_task.is_some() => {
					match local {
						Some(Ok(Ok(completed))) => {
							for index in completed {
								quorum_trackers[index].note_local_durable();
							}
						}
						Some(Ok(Err(err))) => {
							self.trip_local_persistence_distrust(format!(
								"local table durable append returned error: {err}"
							))
							.await;
							return Ok(self.fenced_write_response(&synthetic).await);
						}
						Some(Err(err)) => {
							self.trip_local_persistence_distrust(format!(
								"local table durable append task failed: {err}"
							))
							.await;
							return Ok(self.fenced_write_response(&synthetic).await);
						}
						None => continue,
					}
					local_durable_task = None;
				}
				remote = join.join_next(), if !join.is_empty() => {
					match remote {
						Some(Ok(Ok((index, level)))) => {
							quorum_trackers[index].note_remote_ack(level);
						}
						Some(_) => { /* leg failed — keep racing */ }
						None => {}
					}
				}
				else => break,
			}
		}

		if !quorum_trackers
			.iter()
			.all(DurableQuorumTracker::is_satisfied)
		{
			return Ok(self.counted_write_result(
				error_response(&synthetic, WriteStatus::INTERNAL_ERROR, self.local_node),
				None,
			));
		}
		metrics().observe_durable_quorum_latency(durable_quorum_started.elapsed());

		// ADR 045 Phase 8: append-only batch / row counters fire on
		// the server's success path only — after lease gate,
		// idempotency replay, quota enforcement, and durable quorum
		// have all cleared. The earlier idempotency-replay branch
		// returned early without reaching this point, so a retry
		// against a cached outcome cannot double-count.
		if is_append_only {
			metrics().observe_storage_append_only_batch(meta.decoded_row_count() as u64);
		}

		let pending = self.pending_finalization_record_for_table(&meta, &bifurcated, commit_hlc);
		let _pending_claim = self
			.try_claim_pending_finalization(pending.id)
			.ok_or_else(|| {
				Error::other("pending finalization for this table write is already being processed")
			})?;
		if let Err(err) = self.pending_finalizations.upsert(pending.clone()).await {
			let cdc_staged = self
				.stage_cdc_table_children(
					meta.tenant_id,
					meta.table_object_id,
					descriptor,
					commit_hlc,
					&bifurcated.children,
					false,
				)
				.await?;
			if !cdc_staged.is_empty() {
				self.cdc_pending_live_entries.insert(pending.id, cdc_staged);
			}
			tracing::error!(
				pending_id = %format_bytes_hex(pending.id.as_bytes()),
				write_id = ?meta.parent_write_id,
				tenant_id = meta.tenant_id.get(),
				error = %err,
				"failed to persist table pending finalization; attempting immediate finalization before fencing node"
			);
			match self.resolve_pending_finalization(&pending).await {
				Ok(resolution) => {
					let committed = self
						.complete_pending_finalization(&pending, resolution)
						.await
						.map(|(response, trailing)| self.counted_write_result(response, trailing));
					self.trip_local_persistence_distrust(format!(
						"table pending finalization intent persistence failed after durable quorum: {err}"
					))
					.await;
					return committed;
				}
				Err(finalize_err) => {
					self.cdc_evict_pending_finalization(pending.id).await;
					tracing::error!(
						pending_id = %format_bytes_hex(pending.id.as_bytes()),
						write_id = ?meta.parent_write_id,
						tenant_id = meta.tenant_id.get(),
						error = %finalize_err,
						"immediate table finalization also failed after pending intent persistence failure; fencing node"
					);
					self.trip_local_persistence_distrust(format!(
						"table pending finalization intent persistence failed after durable quorum: {err}; immediate finalization failed: {finalize_err}"
					))
					.await;
					return Ok(self.fenced_write_response(&synthetic).await);
				}
			}
		} else {
			self.refresh_pending_finalization_backlog_metric();
		}
		let cdc_staged = self
			.stage_cdc_table_children(
				meta.tenant_id,
				meta.table_object_id,
				descriptor,
				commit_hlc,
				&bifurcated.children,
				false,
			)
			.await?;
		if !cdc_staged.is_empty() {
			self.cdc_pending_live_entries.insert(pending.id, cdc_staged);
		}

		match self.resolve_pending_finalization(&pending).await {
			Ok(resolution) => self
				.complete_pending_finalization(&pending, resolution)
				.await
				.map(|(response, trailing)| self.counted_write_result(response, trailing)),
			Err(err) => {
				tracing::warn!(
					pending_id = %format_bytes_hex(pending.id.as_bytes()),
					error = %err,
					"table finalization commit failed after the data branch reached durable quorum"
				);
				let response = self.recoverable_pending_response(
					meta.parent_write_id,
					meta.expected_epoch,
					commit_hlc,
				);
				self.idempotency.insert(
					key,
					CachedWriteOutcome::new(WriteOutcome::FinalizationRecoverablePending {
						response,
						pending_finalization_id: pending.id,
					}),
				);
				self.pending_finalization_wakeup.notify_one();
				Ok(self.counted_write_result(response, None))
			}
		}
	}

	async fn table_write(
		&self,
		meta: TableWriteRequestMeta,
		payload: RecordBatch,
	) -> std::io::Result<(TableWriteResponseMeta, Option<FinalizationMarker>)> {
		metrics().data_ring_write_requests_total.inc();
		if meta.proxy_hop >= 2 {
			return Ok((
				self.table_write_error_response(
					&meta,
					WriteStatus::PROXY_LIMIT,
					PbcRejectCode::NONE,
				),
				None,
			));
		}

		let admission = self.admission_snapshot();
		if meta.expected_epoch < admission.current_epoch {
			return Ok((
				self.table_write_error_response(
					&meta,
					WriteStatus::EPOCH_STALE,
					PbcRejectCode::NONE,
				),
				None,
			));
		}

		let key = Self::idempotency_key_for(meta.parent_write_id);
		if let Some(cached) = self.idempotency.get(&key).map(|entry| *entry.value()) {
			let (write, trailing) = self.replay_write_outcome(cached.outcome()).await?;
			return Ok((table_write_response(write, PbcRejectCode::NONE), trailing));
		}

		if self.degraded_reason().is_some() {
			let synthetic = table_write_synthetic_meta(&meta, Identity::ZERO, 0);
			let (write, trailing) = self.fenced_write_response(&synthetic).await;
			return Ok((table_write_response(write, PbcRejectCode::NONE), trailing));
		}

		let (descriptor, effective_retention, secondary_index_descriptors) = {
			let state = self.state_machine.state_snapshot().await;
			let effective_retention = self.table_effective_retention_from_state(
				&state,
				meta.tenant_id,
				meta.table_object_id,
			);
			let descriptor = state
				.table_schemas
				.latest_by_id(meta.tenant_id, meta.table_object_id)
				.cloned();
			let secondary_index_descriptors = descriptor
				.as_ref()
				.map(|descriptor| {
					Self::secondary_index_admission_descriptors(
						&state.secondary_indexes,
						meta.tenant_id,
						descriptor,
					)
				})
				.unwrap_or_default();
			(descriptor, effective_retention, secondary_index_descriptors)
		};
		let Some(descriptor) = descriptor else {
			return Ok((
				self.table_write_error_response(
					&meta,
					WriteStatus::INTERNAL_ERROR,
					PbcRejectCode::PBC_REJECT_SCHEMA_UNKNOWN,
				),
				None,
			));
		};

		// ADR 045: stamp the leader-merged commit HLC *before*
		// admission. The HLC driver guarantees `merge` returns a
		// strictly monotonic HLC per leader call, so two batches
		// from the same admitting node are guaranteed to receive
		// distinct row HLCs even when the client supplies
		// identical `client_hlc` values. This HLC then feeds the
		// append-only identity preimage AND the persisted
		// `__row_hlc__` column — they must agree.
		let commit_hlc = self.merge_commit_hlc(meta.client_hlc);
		// ADR 045 §4 / Phase 8: bind WriterNodeId from the admitting
		// server's authenticated NodeId (ADR 018 mTLS). Not from
		// `meta.proxy_source`, which the wire contract zeros on
		// direct SDK writes.
		let writer_node_id = WriterNodeId::from_node_id(self.local_node);

		let _content_hash_guard = if matches!(
			descriptor.row_identity_policy,
			RowIdentityPolicy::AppendOnlyContentHash
		) {
			Some(
				self.content_hash_admission_guard(meta.tenant_id, meta.table_object_id)
					.await,
			)
		} else {
			None
		};

		let mut content_hash_metrics = None;
		let mut primary_key_metrics = None;
		let bifurcated = match if matches!(
			descriptor.row_identity_policy,
			RowIdentityPolicy::AppendOnlyContentHash
		) {
			self.bifurcate_content_hash_table_write(
				&meta,
				&payload,
				&descriptor,
				&effective_retention,
				commit_hlc,
			)
			.map(|outcome| {
				content_hash_metrics = Some((outcome.absorbed_rows, outcome.stored_rows));
				outcome.bifurcated
			})
		} else if matches!(
			descriptor.row_identity_policy,
			RowIdentityPolicy::PrimaryKey(_)
		) {
			self.bifurcate_primary_key_table_write(
				&meta,
				&payload,
				&descriptor,
				writer_node_id,
				commit_hlc,
			)
			.map(|outcome| {
				primary_key_metrics =
					Some((outcome.upsert_no_digest_add_rows, outcome.resurrected_rows));
				Some(outcome.bifurcated)
			})
		} else {
			bifurcate_table_write(&meta, &payload, &descriptor, writer_node_id, commit_hlc)
				.map(Some)
		} {
			Ok(bifurcated) => bifurcated,
			Err(err) => {
				return Ok((
					self.table_write_error_response(
						&meta,
						table_write_status_for_reject(err.code),
						err.code,
					),
					None,
				));
			}
		};
		let Some(bifurcated) = bifurcated else {
			if let Some((absorbed_rows, stored_rows)) = content_hash_metrics {
				observe_content_hash_admission_metrics(
					meta.tenant_id,
					meta.table_object_id,
					absorbed_rows,
					stored_rows,
				);
			}
			let write = self.counted_write_response(table_write_noop_write_response(
				&meta,
				commit_hlc,
				self.local_node,
			));
			self.idempotency.insert(
				key,
				CachedWriteOutcome::new(WriteOutcome::NoopCommitted { response: write }),
			);
			return Ok((table_write_response(write, PbcRejectCode::NONE), None));
		};
		let Some(groups) = self.table_batch_replica_groups(&bifurcated.children) else {
			return Ok((
				self.table_write_error_response(
					&meta,
					WriteStatus::INTERNAL_ERROR,
					PbcRejectCode::PBC_REJECT_LOCALITY_UNSUPPORTED,
				),
				None,
			));
		};
		let is_append_only = descriptor.is_append_only();
		let content_hash_metric_tenant = meta.tenant_id;
		let content_hash_metric_table = meta.table_object_id;
		let primary_key_metric_tenant = meta.tenant_id;
		let primary_key_metric_table = meta.table_object_id;
		let (write, trailing) = self
			.write_table_bifurcated(
				meta,
				bifurcated,
				groups,
				&descriptor,
				commit_hlc,
				is_append_only,
				secondary_index_descriptors,
			)
			.await?;
		if matches!(
			write.status,
			WriteStatus::OK | WriteStatus::FINALIZATION_PENDING
		) && let Some((absorbed_rows, stored_rows)) = content_hash_metrics
		{
			observe_content_hash_admission_metrics(
				content_hash_metric_tenant,
				content_hash_metric_table,
				absorbed_rows,
				stored_rows,
			);
		}
		if matches!(
			write.status,
			WriteStatus::OK | WriteStatus::FINALIZATION_PENDING
		) && let Some((upsert_no_digest_add_rows, resurrected_rows)) = primary_key_metrics
		{
			metrics().observe_storage_pk_upsert_no_digest_add(
				primary_key_metric_tenant,
				primary_key_metric_table,
				upsert_no_digest_add_rows,
			);
			metrics().observe_storage_pk_delete_resurrections(
				primary_key_metric_tenant,
				primary_key_metric_table,
				resurrected_rows,
			);
		}
		Ok((table_write_response(write, PbcRejectCode::NONE), trailing))
	}

	fn bifurcate_primary_key_table_write(
		&self,
		meta: &TableWriteRequestMeta,
		batch: &RecordBatch,
		descriptor: &TableSchemaDescriptorV1,
		writer_node_id: WriterNodeId,
		row_hlc: HybridTimestamp,
	) -> Result<PrimaryKeyBifurcation, TableWriteValidationError> {
		let admission =
			validate_table_write_request_batch(meta, batch, descriptor, writer_node_id, row_hlc)?;
		let mut upsert_no_digest_add_rows = 0_u64;
		let mut resurrected_rows = 0_u64;
		for key_identity in admission.key_identities.iter().copied() {
			let started = Instant::now();
			let outcome = admit_pk_write_row(
				descriptor,
				meta.tenant_id,
				key_identity,
				row_hlc,
				&self.slab,
			);
			metrics().observe_storage_pk_admission_visibility_lookup(
				meta.tenant_id,
				meta.table_object_id,
				started.elapsed(),
			);
			match outcome {
				PkAdmissionOutcome::UpsertedAlreadyVisible => {
					upsert_no_digest_add_rows = upsert_no_digest_add_rows.saturating_add(1);
				}
				PkAdmissionOutcome::InsertedNewVisible {
					latest_was_tombstone: true,
				} => {
					resurrected_rows = resurrected_rows.saturating_add(1);
				}
				PkAdmissionOutcome::InsertedNewVisible {
					latest_was_tombstone: false,
				} => {}
				PkAdmissionOutcome::DeletedSupersedingVisible
				| PkAdmissionOutcome::DeleteAbsorbedNotVisible { .. } => {
					unreachable!("delete admission outcomes are not produced by PK write admission")
				}
			}
		}
		let bifurcated =
			bifurcate_table_write_with_admission(meta, batch, descriptor, &admission, row_hlc)?;
		Ok(PrimaryKeyBifurcation {
			bifurcated,
			upsert_no_digest_add_rows,
			resurrected_rows,
		})
	}

	fn bifurcate_content_hash_table_write(
		&self,
		meta: &TableWriteRequestMeta,
		batch: &RecordBatch,
		descriptor: &TableSchemaDescriptorV1,
		effective_retention: &EffectiveRetention,
		row_hlc: HybridTimestamp,
	) -> Result<ContentHashBifurcation, TableWriteValidationError> {
		let admission = validate_table_write_request_batch(
			meta,
			batch,
			descriptor,
			WriterNodeId::ZERO,
			row_hlc,
		)?;
		let mut store_rows = Vec::with_capacity(batch.num_rows());
		let mut keys_stored_in_batch = HashSet::with_capacity(batch.num_rows());
		let mut absorbed = 0_u64;
		for (row, key_identity) in admission.key_identities.iter().copied().enumerate() {
			let currently_visible = row_visible_for_admission(
				descriptor,
				meta.tenant_id,
				key_identity,
				row_hlc,
				effective_retention,
				&self.slab,
			);
			if currently_visible || !keys_stored_in_batch.insert(key_identity) {
				absorbed = absorbed.saturating_add(1);
				continue;
			}
			store_rows.push(row);
		}
		let stored = store_rows.len() as u64;

		if store_rows.is_empty() {
			return Ok(ContentHashBifurcation {
				bifurcated: None,
				absorbed_rows: absorbed,
				stored_rows: stored,
			});
		}
		if store_rows.len() == batch.num_rows() {
			let bifurcated =
				bifurcate_table_write_with_admission(meta, batch, descriptor, &admission, row_hlc)?;
			return Ok(ContentHashBifurcation {
				bifurcated: Some(bifurcated),
				absorbed_rows: absorbed,
				stored_rows: stored,
			});
		}

		let filtered_batch = filter_record_batch_rows(batch, &store_rows)?;
		let filtered_admission = TableWriteAdmission {
			canonical_primary_keys: Vec::new(),
			key_identities: store_rows
				.iter()
				.map(|row| admission.key_identities[*row])
				.collect(),
			localities: store_rows
				.iter()
				.map(|row| admission.localities[*row])
				.collect(),
		};
		let bifurcated = bifurcate_table_write_with_admission(
			meta,
			&filtered_batch,
			descriptor,
			&filtered_admission,
			row_hlc,
		)?;
		Ok(ContentHashBifurcation {
			bifurcated: Some(bifurcated),
			absorbed_rows: absorbed,
			stored_rows: stored,
		})
	}

	async fn table_delete(
		&self,
		meta: TableDeleteRowsRequestMeta,
		row_key_ids: Vec<Identity>,
	) -> std::io::Result<(TableDeleteRowsResponseMeta, Option<FinalizationMarker>)> {
		metrics().data_ring_write_requests_total.inc();
		if row_key_ids.is_empty() || row_key_ids.len() != meta.row_count as usize {
			return Ok((
				self.table_delete_error_response(
					&meta,
					WriteStatus::INTERNAL_ERROR,
					PbcRejectCode::PBC_REJECT_ROW_COUNT_MISMATCH,
				),
				None,
			));
		}
		if row_key_ids.len() > TABLE_WRITE_MAX_ROWS {
			return Ok((
				self.table_delete_error_response(
					&meta,
					WriteStatus::PAYLOAD_TOO_LARGE,
					PbcRejectCode::PBC_REJECT_BATCH_TOO_LARGE,
				),
				None,
			));
		}

		let current_leader = self.raft.current_leader().await;
		if current_leader != Some(self.local_node.id) {
			let mut response = self.table_delete_error_response(
				&meta,
				WriteStatus::NOT_LEADER,
				PbcRejectCode::NONE,
			);
			response.write.leader_hint = current_leader
				.map(|id| NodeId { id, _reserved: 0 })
				.unwrap_or(self.local_node);
			return Ok((response, None));
		}

		let admission = self.admission_snapshot();
		if meta.expected_epoch < admission.current_epoch {
			return Ok((
				self.table_delete_error_response(
					&meta,
					WriteStatus::EPOCH_STALE,
					PbcRejectCode::PBC_REJECT_SCHEMA_STALE,
				),
				None,
			));
		}

		let key = Self::idempotency_key_for(meta.parent_write_id);
		if let Some(cached) = self
			.table_delete_idempotency
			.get(&key)
			.map(|entry| *entry.value())
		{
			return Ok((cached.response, cached.trailing));
		}

		if self.degraded_reason().is_some() {
			let synthetic = table_delete_synthetic_write_meta(&meta, Identity::ZERO, 0);
			let (write, trailing) = self.fenced_write_response(&synthetic).await;
			return Ok((
				table_delete_rows_response(write, 0, 0, 0, PbcRejectCode::NONE),
				trailing,
			));
		}

		let (descriptor, effective_retention) = {
			let state = self.state_machine.state_snapshot().await;
			let effective_retention = self.table_effective_retention_from_state(
				&state,
				meta.tenant_id,
				meta.table_object_id,
			);
			let descriptor = state
				.table_schemas
				.latest_by_id(meta.tenant_id, meta.table_object_id)
				.cloned();
			(descriptor, effective_retention)
		};
		let Some(descriptor) = descriptor else {
			return Ok((
				self.table_delete_error_response(
					&meta,
					WriteStatus::INTERNAL_ERROR,
					PbcRejectCode::PBC_REJECT_SCHEMA_UNKNOWN,
				),
				None,
			));
		};
		if descriptor.schema_generation != meta.schema_generation {
			return Ok((
				self.table_delete_error_response(
					&meta,
					WriteStatus::EPOCH_STALE,
					PbcRejectCode::PBC_REJECT_SCHEMA_STALE,
				),
				None,
			));
		}
		let layout = match tombstone_marker_layout(&descriptor) {
			Ok(layout) => layout,
			Err(code) => {
				return Ok((
					self.table_delete_error_response(&meta, WriteStatus::INTERNAL_ERROR, code),
					None,
				));
			}
		};
		let is_primary_key_table = matches!(
			descriptor.row_identity_policy,
			RowIdentityPolicy::PrimaryKey(_)
		);
		if is_primary_key_table {
			metrics().observe_storage_pk_delete_request(meta.tenant_id, meta.table_object_id);
		}

		let delete_hlc = self.merge_commit_hlc(meta.client_hlc);
		let mut seen = HashSet::with_capacity(row_key_ids.len());
		let mut tombstone_rows = Vec::new();
		let mut already_deleted_count = 0_u32;
		let mut not_found_count = 0_u32;
		for row_key_id in row_key_ids {
			if !seen.insert(row_key_id) {
				continue;
			}
			let outcome = if is_primary_key_table {
				let started = Instant::now();
				let outcome = admit_pk_delete_row(
					&descriptor,
					meta.tenant_id,
					row_key_id,
					delete_hlc,
					&effective_retention,
					&self.slab,
				);
				metrics().observe_storage_pk_admission_visibility_lookup(
					meta.tenant_id,
					meta.table_object_id,
					started.elapsed(),
				);
				outcome
			} else {
				let latest = self
					.slab
					.lookup_as_of(meta.tenant_id, row_key_id, delete_hlc);
				crate::storage::pk_delete::pk_delete_outcome_for_latest(latest)
			};
			match outcome {
				PkAdmissionOutcome::DeleteAbsorbedNotVisible {
					latest_was_tombstone: true,
				} => {
					already_deleted_count = already_deleted_count.saturating_add(1);
				}
				PkAdmissionOutcome::DeletedSupersedingVisible => {
					let Some(routing_locality) = self
						.slab
						.routing_locality_for_existing_key(meta.tenant_id, row_key_id)
					else {
						tracing::warn!(
							tenant_id = meta.tenant_id.get(),
							row_key_id = %format_bytes_hex(row_key_id.as_bytes()),
							"append-only row tombstone target is missing cached routing locality"
						);
						return Ok((
							self.table_delete_error_response(
								&meta,
								WriteStatus::INTERNAL_ERROR,
								PbcRejectCode::PBC_REJECT_LOCALITY_UNSUPPORTED,
							),
							None,
						));
					};
					let source_row_index = tombstone_rows.len();
					tombstone_rows.push(MultiRowSourceRow {
						source_row_index,
						key_identity: row_key_id,
						routing_locality,
						row_hlc: delete_hlc,
						is_tombstone: true,
						table_object_id: Some(descriptor.table_object_id),
					});
				}
				PkAdmissionOutcome::DeleteAbsorbedNotVisible {
					latest_was_tombstone: false,
				} => {
					not_found_count = not_found_count.saturating_add(1);
				}
				PkAdmissionOutcome::InsertedNewVisible { .. }
				| PkAdmissionOutcome::UpsertedAlreadyVisible => unreachable!(
					"write admission outcomes are not produced by delete classification"
				),
			}
		}
		let deleted_count = u32::try_from(tombstone_rows.len()).unwrap_or(u32::MAX);
		if tombstone_rows.is_empty() {
			let write = self.counted_write_response(table_delete_noop_write_response(
				&meta,
				delete_hlc,
				self.local_node,
			));
			let response = table_delete_rows_response(
				write,
				deleted_count,
				already_deleted_count,
				not_found_count,
				PbcRejectCode::NONE,
			);
			if is_primary_key_table {
				metrics().observe_storage_pk_delete_outcomes(
					meta.tenant_id,
					meta.table_object_id,
					u64::from(deleted_count),
					u64::from(already_deleted_count),
					u64::from(not_found_count),
				);
			}
			self.table_delete_idempotency.insert(
				key,
				CachedTableDeleteOutcome {
					response,
					trailing: None,
				},
			);
			return Ok((response, None));
		}

		let synthetic_meta = table_delete_synthetic_table_write_meta(&meta, tombstone_rows.len())?;
		let packed = match pack_tombstone_multirow_source_rows_with_policy(
			layout,
			&tombstone_rows,
			descriptor.record_body_compression_policy,
		) {
			Ok(packed) => packed,
			Err(err) => {
				tracing::warn!(
					tenant_id = meta.tenant_id.get(),
					error = %err,
					"failed to pack append-only row tombstones"
				);
				return Ok((
					self.table_delete_error_response(
						&meta,
						WriteStatus::INTERNAL_ERROR,
						PbcRejectCode::PBC_REJECT_TOMBSTONE_PACKING_FAILED,
					),
					None,
				));
			}
		};
		let bifurcated = match bifurcate_table_delete(&meta, &descriptor, packed) {
			Ok(bifurcated) => bifurcated,
			Err(err) => {
				tracing::warn!(
					tenant_id = meta.tenant_id.get(),
					error = %err,
					"failed to bifurcate append-only row tombstones"
				);
				return Ok((
					self.table_delete_error_response(
						&meta,
						WriteStatus::INTERNAL_ERROR,
						PbcRejectCode::PBC_REJECT_TOMBSTONE_PACKING_FAILED,
					),
					None,
				));
			}
		};
		let Some(groups) = self.table_batch_replica_groups(&bifurcated.children) else {
			return Ok((
				self.table_delete_error_response(
					&meta,
					WriteStatus::INTERNAL_ERROR,
					PbcRejectCode::PBC_REJECT_LOCALITY_UNSUPPORTED,
				),
				None,
			));
		};
		let (write, trailing) = self
			.write_table_bifurcated(
				synthetic_meta,
				bifurcated,
				groups,
				&descriptor,
				delete_hlc,
				false,
				Vec::new(),
			)
			.await?;
		let response = table_delete_rows_response(
			write,
			deleted_count,
			already_deleted_count,
			not_found_count,
			PbcRejectCode::NONE,
		);
		if is_primary_key_table {
			metrics().observe_storage_pk_delete_outcomes(
				meta.tenant_id,
				meta.table_object_id,
				u64::from(deleted_count),
				u64::from(already_deleted_count),
				u64::from(not_found_count),
			);
		}
		self.table_delete_idempotency
			.insert(key, CachedTableDeleteOutcome { response, trailing });
		Ok((response, trailing))
	}
}

#[async_trait]
impl DataRing for WendelonDataServer {
	async fn write(
		&self,
		meta: WriteRequestMeta,
		payload: RecordBatch,
	) -> std::io::Result<(WriteResponseMeta, Option<FinalizationMarker>)> {
		metrics().data_ring_write_requests_total.inc();
		// 1. Proxy-hop guard — two-hop cap per ADR 003 §2.
		if meta.proxy_hop >= 2 {
			return Ok(self.counted_write_result(
				error_response(&meta, WriteStatus::PROXY_LIMIT, self.local_node),
				None,
			));
		}

		// 2. Epoch validation. The authoritative current epoch is
		// the highest epoch the SM has seen an E_safe advancement
		// for; a client that believes in an older epoch gets a
		// redirect, not a silent accept.
		let admission = self.admission_snapshot();
		let current_epoch = admission.current_epoch;
		if meta.expected_epoch < current_epoch {
			return Ok(self.counted_write_result(
				error_response(&meta, WriteStatus::EPOCH_STALE, self.local_node),
				None,
			));
		}

		// 3. Idempotency fast path.
		let key = Self::idempotency_key_for(meta.write_id);
		if let Some(cached) = self.idempotency.get(&key).map(|entry| *entry.value()) {
			return self.replay_write_outcome(cached.outcome()).await;
		}

		if self.degraded_reason().is_some() {
			return Ok(self.fenced_write_response(&meta).await);
		}

		// 3a. Decode the payload before cache-miss admission so we can
		// make non-primary redirect decisions without forcing a fresh
		// leader to complete orphan-witness recovery for a range this
		// node does not own.
		//
		// This is still pre-bifurcation bookkeeping: no durable write
		// work has started yet.
		//
		// 4. Cross the ADR 003 bifurcation boundary exactly once.
		// The returned bundle becomes the single source of truth for
		// both the data branch and the control branch.
		let bifurcated = bifurcate_client_write(&meta, &payload)?;
		self.write_bifurcated(meta, bifurcated).await
	}

	async fn table_delete(
		&self,
		meta: TableDeleteRowsRequestMeta,
		row_key_ids: Vec<Identity>,
	) -> std::io::Result<(TableDeleteRowsResponseMeta, Option<FinalizationMarker>)> {
		WendelonDataServer::table_delete(self, meta, row_key_ids).await
	}

	async fn read(
		&self,
		meta: ReadRequestMeta,
		_key: RecordBatch,
	) -> std::io::Result<(ReadResponseMeta, Option<RecordBatch>)> {
		metrics().data_ring_read_requests_total.inc();
		let admission = self.admission_snapshot();
		let current_epoch = admission.current_epoch;
		if meta.expected_epoch < current_epoch {
			return Ok((
				error_read_response(&meta, ReadStatus::EPOCH_STALE, self.local_node),
				None,
			));
		}
		if let Err(status) = self.enforce_read_policy(meta.tenant_id).await {
			return Ok((error_read_response(&meta, status, self.local_node), None));
		}

		// 1. Consistency gate. LINEARIZABLE requires the local SM
		// to have observed an E_safe advancement in this epoch at
		// or beyond the client's min_hlc. SERIALIZABLE and STALE
		// skip the gate — the former trusts the leader's commit
		// frontier, the latter explicitly accepts stale data.
		if meta.consistency == Consistency::LINEARIZABLE {
			let watermark = admission
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

		// 2. Probe the slab index using the ingress-validated Identity.
		// The Arrow key batch is still validated at the frame boundary
		// even though the hot read path no longer needs to re-parse it.
		let key_identity = meta.key_identity;
		let key_locality = Locality::from(key_identity).get();

		// 2a. Per-range lease gate for LINEARIZABLE reads (plan 018).
		// A local point-read is only authoritative while a lease
		// fences writes for this range to this node — anything
		// weaker opens a window where a stale follower could serve
		// a pre-failover value.
		if self.lease_gate_enabled && meta.consistency == Consistency::LINEARIZABLE {
			let is_primary = self
				.router
				.replicas_for_locality(Locality::new(key_locality))
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
				.lease_gate_result(
					Locality::new(key_locality),
					meta.expected_epoch,
					meta.min_hlc,
				)
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

		if self.snapshot_request_outside_retention(meta.as_of_hlc) {
			return Ok((
				error_read_response(&meta, ReadStatus::RETENTION_EXPIRED, self.local_node),
				None,
			));
		}

		let (location, bytes) =
			match self
				.slab
				.lookup_as_of(meta.tenant_id, key_identity, meta.as_of_hlc)
			{
				Some(loc) => {
					if loc.is_tombstone {
						return Ok((
							error_read_response(&meta, ReadStatus::KEY_NOT_FOUND, self.local_node),
							None,
						));
					}
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
						.try_migrate_point_read(meta.tenant_id, key_identity, meta.as_of_hlc)
						.await?
					{
						PointReadOutcome::Hit(loc, bytes) => {
							if loc.is_tombstone {
								return Ok((
									error_read_response(
										&meta,
										ReadStatus::KEY_NOT_FOUND,
										self.local_node,
									),
									None,
								));
							}
							(loc, bytes)
						}
						PointReadOutcome::AuthoritativeMiss => {
							return Ok((
								error_read_response(
									&meta,
									ReadStatus::KEY_NOT_FOUND,
									self.local_node,
								),
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
					Err(_) => server.counted_write_response(error_response(
						&meta,
						WriteStatus::INTERNAL_ERROR,
						server.local_node,
					)),
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
	batches: Vec<RecordBatch>,
	deadline: Instant,
	stats_explain: QueryStatsExplainCounters,
	secondary_index_access_plan: Option<SecondaryIndexAccessPlan>,
}

#[derive(Clone, Debug)]
struct PreparedAggregateExecution {
	range_meta: QueryRequestMeta,
	program: pbc::Program,
	descriptor: TableSchemaDescriptorV1,
	ranges: Vec<QueryScanRange>,
	retention_floor: Option<HybridTimestamp>,
}

#[derive(Clone, Copy, Debug)]
struct QueryScanRange {
	start: Locality,
	end: Locality,
}

fn cdc_snapshot_hlc_from_state(state: &SmState) -> HybridTimestamp {
	state
		.e_safe_watermarks
		.values()
		.copied()
		.max()
		.unwrap_or_else(bytemuck::Zeroable::zeroed)
}

fn cdc_retention_reject_code_for_cursor(
	cursor: CdcCursor,
	effective_retention: &EffectiveRetention,
) -> Option<CdcRejectCode> {
	check_read_against_retention(cursor.row_hlc, effective_retention)
		.err()
		.map(|_| CdcRejectCode::BelowRetentionFloor)
}

fn cdc_bound_audit_str(bound: CdcSubscribeBound) -> &'static str {
	match bound {
		CdcSubscribeBound::Live => "live",
		CdcSubscribeBound::SnapshotAtOpen => "snapshot_at_open",
	}
}

fn cdc_reject_audit_reason(code: CdcRejectCode) -> &'static str {
	match code {
		CdcRejectCode::BelowRetentionFloor => "reject_below_retention_floor",
		CdcRejectCode::RingOverflowDuringCatchup => "reject_ring_overflow_during_catchup",
		CdcRejectCode::SchemaFingerprintIncompatible => "reject_schema_fingerprint_incompatible",
		CdcRejectCode::CheckpointDigestMismatch => "reject_checkpoint_digest_mismatch",
		CdcRejectCode::UnknownTable => "reject_unknown_table",
		CdcRejectCode::TenantAuthDenied => "reject_tenant_auth_denied",
		CdcRejectCode::TableNotReplicatedHere => "reject_table_not_replicated_here",
		CdcRejectCode::InternalEngineError => "reject_internal_engine_error",
	}
}

fn cdc_hlc_audit_json(hlc: HybridTimestamp) -> serde_json::Value {
	json!({
		"physical_ms": hlc.physical_ms,
		"logical": hlc.logical,
		"node_id": hlc.node_id,
	})
}

fn cdc_cursor_audit_json(cursor: CdcCursor) -> serde_json::Value {
	json!({
		"row_hlc": cdc_hlc_audit_json(cursor.row_hlc),
		"row_key_id": format_bytes_hex(cursor.row_key_id.as_bytes()),
		"record_row_index": cursor.record_row_index.get(),
	})
}

fn cdc_descriptor_match_query_meta(
	tenant_id: TenantId,
	as_of_hlc: HybridTimestamp,
) -> QueryRequestMeta {
	QueryRequestMeta {
		min_hlc: bytemuck::Zeroable::zeroed(),
		expected_epoch: 0,
		tenant_id,
		consistency: Consistency::SERIALIZABLE,
		_pad: [0; 7],
		locality_range_start: 0,
		locality_range_end: u64::MAX,
		bytecode_len: 0,
		_pad2: [0; 4],
		as_of_hlc,
	}
}

fn cdc_locality_in_ranges(locality: Locality, ranges: &[QueryScanRange]) -> bool {
	ranges
		.iter()
		.any(|range| locality >= range.start && locality <= range.end)
}

fn cdc_source_localities_from_ranges_and_rings<I>(
	ranges: &[QueryScanRange],
	ring_localities: I,
) -> Vec<Locality>
where
	I: IntoIterator<Item = Locality>,
{
	let mut localities = BTreeSet::new();
	for range in ranges {
		localities.insert(range.start);
		localities.insert(range.end);
		let width = range.end.get().saturating_sub(range.start.get());
		if width <= CDC_SOURCE_LOCALITY_ENUMERATION_BUDGET {
			for value in range.start.get()..=range.end.get() {
				localities.insert(Locality::new(value));
			}
		}
	}
	for locality in ring_localities {
		if cdc_locality_in_ranges(locality, ranges) {
			localities.insert(locality);
		}
	}
	localities.into_iter().collect()
}

fn cdc_heartbeat_localities_for_primary_ranges<I>(ranges: I) -> Vec<Locality>
where
	I: IntoIterator<Item = (u64, u64)>,
{
	let ranges = ranges
		.into_iter()
		.map(|(start, end)| QueryScanRange {
			start: Locality::new(start),
			end: Locality::new(end),
		})
		.collect::<Vec<_>>();
	cdc_source_localities_from_ranges_and_rings(&ranges, std::iter::empty())
}

fn cdc_ranges_maybe_replicated_by_local_node<F>(
	ranges: &[QueryScanRange],
	local_node: NodeId,
	mut replicas_for_locality: F,
) -> bool
where
	F: FnMut(Locality) -> Vec<NodeId>,
{
	for range in ranges {
		let width = range.end.get().saturating_sub(range.start.get());
		if width > CDC_SOURCE_LOCALITY_ENUMERATION_BUDGET {
			return true;
		}
		for value in range.start.get()..=range.end.get() {
			if replicas_for_locality(Locality::new(value))
				.iter()
				.any(|node| node.id == local_node.id)
			{
				return true;
			}
		}
	}
	false
}

struct RebatchGroup {
	schema: Arc<Schema>,
	batches: Vec<RecordBatch>,
	max_hlc: HybridTimestamp,
	input_rows: u64,
}

#[derive(Clone, Debug)]
enum QueryProjectedExecution {
	Disabled,
	Pbc {
		requirement: ColumnFetchRequirement,
		user_column_count: usize,
	},
	Aggregate {
		requirement: ColumnFetchRequirement,
		user_column_count: usize,
	},
}

impl QueryProjectedExecution {
	fn requirement(&self) -> Option<&ColumnFetchRequirement> {
		match self {
			Self::Disabled => None,
			Self::Pbc { requirement, .. } | Self::Aggregate { requirement, .. } => {
				Some(requirement)
			}
		}
	}
}

#[derive(Clone, Copy, Debug)]
struct QueryExecutionBudget {
	max_locations: usize,
	max_rows: u64,
	max_wall_time: Duration,
	stats_pruning_enabled: bool,
}

impl Default for QueryExecutionBudget {
	fn default() -> Self {
		Self {
			max_locations: 4_096,
			max_rows: 65_536,
			max_wall_time: Duration::from_millis(250),
			stats_pruning_enabled: true,
		}
	}
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct QueryStatsExplainCounters {
	candidate_slabs_before_stats: u64,
	slabs_pruned_by_stats: u64,
	slabs_scanned_stats_missing: u64,
	slabs_scanned_stats_unsupported: u64,
	slabs_scanned_predicate_unrepresentable: u64,
	stats_cache_hits: u64,
	stats_cache_misses: u64,
	stats_decrypt_failed_total: u64,
}

#[derive(Default)]
struct QueryStatsPruneCache {
	decisions: HashMap<SlabId, bool>,
	explain: QueryStatsExplainCounters,
}

enum EncodedStatsPredicate {
	Range {
		lower: Option<Vec<u8>>,
		upper: Option<Vec<u8>>,
	},
	AnyOf(Vec<Vec<u8>>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SlabStatsQueryOutcome {
	may_match: bool,
	unsupported: bool,
	predicate_unrepresentable: bool,
}

fn snapshot_request_outside_retention(
	as_of_hlc: HybridTimestamp,
	retention_floor: Option<HybridTimestamp>,
) -> bool {
	retention_floor.is_some_and(|floor| !is_zero_hlc(as_of_hlc) && as_of_hlc < floor)
}

fn query_deadline_exceeded(deadline: Instant) -> bool {
	Instant::now() > deadline
}

fn aggregate_projected_execution(
	slab: &SlabManager,
	request: &AggregateRequest,
	prepared: &PreparedAggregateExecution,
) -> Result<QueryProjectedExecution, AggRejectCode> {
	if !slab.l2_column_fetch_enabled() {
		return Ok(QueryProjectedExecution::Disabled);
	}
	let user_column_count = prepared
		.descriptor
		.arrow_schema()
		.map_err(|_| AggRejectCode::SchemaUnknown)?
		.fields()
		.len();
	let requirement = analyze_aggregate_query_column_requirement(
		&prepared.program,
		&request.aggregates,
		user_column_count,
	)
	.map_err(|_| AggRejectCode::ProgramInvalid)?;
	Ok(QueryProjectedExecution::Aggregate {
		requirement,
		user_column_count,
	})
}

fn aggregate_fold_request_for_execution(
	request: &AggregateRequest,
	projected_execution: &QueryProjectedExecution,
) -> Result<AggregateRequest, AggRejectCode> {
	let mut folded = request.clone();
	if let QueryProjectedExecution::Aggregate { requirement, .. } = projected_execution {
		folded.aggregates =
			remap_aggregate_expressions_for_projected_inputs(&request.aggregates, requirement)?;
	}
	Ok(folded)
}

fn remap_aggregate_expressions_for_projected_inputs(
	expressions: &[AggregateExpression],
	requirement: &ColumnFetchRequirement,
) -> Result<Vec<AggregateExpression>, AggRejectCode> {
	expressions
		.iter()
		.map(|expression| {
			let Some(column) = expression.function()?.column() else {
				return Ok(expression.clone());
			};
			let position = requirement
				.aggregate_columns
				.iter()
				.position(|candidate| candidate == u32::from(column))
				.ok_or(AggRejectCode::ColumnUnknown)?;
			let column_ordinal =
				u16::try_from(position).map_err(|_| AggRejectCode::ColumnUnknown)?;
			Ok(AggregateExpression {
				column_ordinal,
				..expression.clone()
			})
		})
		.collect()
}

async fn read_projected_query_location(
	slab: &SlabManager,
	location: RecordLocation,
	descriptor: &TableSchemaDescriptorV1,
	program: &pbc::Program,
	projected_execution: &QueryProjectedExecution,
) -> std::io::Result<Option<RecordBatch>> {
	let Some(requirement) = projected_execution.requirement() else {
		return Ok(None);
	};
	// ADR 045 §5 / Phase 8.5: descriptor-filtered AppendOnly
	// queries need `__row_table_object_id__` available in the
	// projected fetch so the projected fast path can verify the
	// row's table-isolation binding before returning. Augment the
	// requirement here; the projected fetch then either includes
	// the column (if available on disk) or the verification step
	// below returns `Ok(None)` so the caller falls back to the
	// whole-record path that runs `table_record_matches_descriptor`.
	let requirement = augment_requirement_for_descriptor_verification(requirement, descriptor);
	let projected_read = match slab
		.read_projected_record(location, descriptor, &requirement)
		.await
	{
		Ok(Some(projected_read)) => projected_read,
		Ok(None) => return Ok(None),
		Err(err) if projected_query_can_use_whole_record_fallback(&err) => return Ok(None),
		Err(err) => return Err(err),
	};
	let projected =
		materialize_projected_location_batch(projected_read.projected, location.record_row_index)?;
	if !projected_descriptor_match(descriptor, &projected)? {
		return Ok(None);
	}
	match projected_execution {
		QueryProjectedExecution::Disabled => Ok(None),
		QueryProjectedExecution::Pbc {
			requirement,
			user_column_count,
		} => {
			execute_pbc_projected_record_batch(&projected, program, requirement, *user_column_count)
				.map(Some)
		}
		QueryProjectedExecution::Aggregate { .. } => Ok(None),
	}
}

async fn read_aggregate_query_location(
	slab: &SlabManager,
	meta: &QueryRequestMeta,
	location: RecordLocation,
	key_identity: Identity,
	locality: Locality,
	descriptor: &TableSchemaDescriptorV1,
	program: &pbc::Program,
	requirement: &ColumnFetchRequirement,
	user_column_count: usize,
) -> std::io::Result<Option<RecordBatch>> {
	// ADR 045 §5 / Phase 8.5: ensure descriptor-filtered AppendOnly
	// queries fetch `__row_table_object_id__` so the projected fast
	// path can verify the row's table-isolation binding. See
	// `read_projected_query_location` for the same plumbing.
	let augmented_requirement =
		augment_requirement_for_descriptor_verification(requirement, descriptor);
	match slab
		.read_projected_record(location, descriptor, &augmented_requirement)
		.await
	{
		Ok(Some(projected_read)) => {
			let projected = materialize_projected_location_batch(
				projected_read.projected,
				location.record_row_index,
			)?;
			if !projected_descriptor_match(descriptor, &projected)? {
				// Verification failed (e.g., AppendOnly row from a
				// sibling descriptor with a colliding table-spread
				// prefix). Fall back to the whole-record path which
				// runs the trusted `table_record_matches_descriptor`.
			} else {
				return execute_aggregate_projected_record_batch(
					&projected,
					program,
					requirement,
					user_column_count,
				)
				.map(Some);
			}
		}
		Ok(None) => {}
		Err(err) if projected_query_can_use_whole_record_fallback(&err) => {}
		Err(err) => return Err(err),
	}

	let bytes = slab.read(location).await?;
	let decode_started = Instant::now();
	let storage_batch = decode_batch(&bytes)?;
	let storage_row_count = storage_batch.num_rows();
	let was_multirow = has_storage_synthetic_columns(&storage_batch);
	// ADR 045 Phase 8: extract per-row table_object_id from the
	// unstripped storage batch before materializing the user batch
	// — needed for AppendOnly descriptor isolation across
	// table-spread prefix collisions.
	let stored_table_object_id = if was_multirow {
		storage_row_table_object_id_at(&storage_batch, location.record_row_index)?
	} else {
		None
	};
	let batch = materialize_record_location_batch(&storage_batch, location.record_row_index)?;
	if was_multirow {
		metrics().observe_storage_multirow_decode(
			batch.num_rows() as u64,
			bytes.len() as u64,
			decode_started.elapsed(),
		);
		if storage_row_count <= 1 {
			metrics()
				.storage_multirow_uncompacted_row_records_total
				.inc();
		}
	}
	if !table_record_matches_descriptor(
		meta,
		descriptor,
		key_identity,
		locality,
		stored_table_object_id,
		&batch,
	)? {
		return Ok(None);
	}
	execute_aggregate_full_record_batch(&batch, program, requirement).map(Some)
}

fn execute_aggregate_full_record_batch(
	batch: &RecordBatch,
	program: &pbc::Program,
	requirement: &ColumnFetchRequirement,
) -> std::io::Result<RecordBatch> {
	let aggregate_columns = requirement.aggregate_columns.iter().collect::<Vec<_>>();
	let remapped =
		program.remap_columns_with_project(|column| Ok(column), Some(&aggregate_columns))?;
	pbc::execute_program(&remapped, batch)
}

fn projected_query_can_use_whole_record_fallback(error: &Error) -> bool {
	error
		.get_ref()
		.and_then(|source| source.downcast_ref::<ColumnFetchError>())
		.is_some_and(|error| {
			matches!(
				error,
				ColumnFetchError::SchemaMismatch { .. } | ColumnFetchError::UnsupportedArrowLayout
			)
		})
}

/// ADR 045 §5 / Phase 8.5 — return a `ColumnFetchRequirement`
/// extended with the synthetic columns the projected fast path
/// needs to verify a row belongs to the requested descriptor.
///
/// All multi-row descriptors (PK + AppendOnly) stamp
/// `__row_table_object_id__` on every row, so the projected fetch
/// must include it for descriptor-filtered queries. Single-row
/// descriptors carry no synthetic columns at all; the augmentation
/// is a no-op there and `projected_descriptor_match` returns
/// `Ok(false)` so the caller falls back to the whole-record path.
fn augment_requirement_for_descriptor_verification(
	requirement: &ColumnFetchRequirement,
	descriptor: &TableSchemaDescriptorV1,
) -> ColumnFetchRequirement {
	if !descriptor.record_layout_policy.is_multi_row() {
		return requirement.clone();
	}
	let mut augmented = requirement.clone();
	augmented
		.storage_columns
		.insert(crate::storage::columnar::StorageColumn::RowTableObjectId);
	augmented
}

/// ADR 045 §5 / Phase 8.5 — verify the projected fast path's
/// returned row belongs to the requested descriptor.
///
/// Confirms the stored `__row_table_object_id__` column equals
/// `descriptor.table_object_id`. Both PK and AppendOnly multi-row
/// descriptors stamp this column on every row, so the projected
/// path can byte-compare instead of re-deriving identity.
///
/// If the column is missing (single-row record, which carries no
/// synthetic columns), returns `Ok(false)` so the caller falls
/// back to the whole-record path that runs the trusted
/// `table_record_matches_descriptor`. Single-row PK queries pay
/// the fallback cost; we can re-add a projected-PK fast path
/// later if profiling motivates it.
fn projected_descriptor_match(
	descriptor: &TableSchemaDescriptorV1,
	projected: &ProjectedRecordBatch,
) -> std::io::Result<bool> {
	let Some(physical) =
		projected.storage_physical_for(crate::storage::columnar::StorageColumn::RowTableObjectId)
	else {
		// Single-row record (no synthetic columns) or otherwise
		// missing — force whole-record fallback so the trusted
		// matcher decides.
		return Ok(false);
	};
	let array = projected
		.batch
		.column(physical)
		.as_any()
		.downcast_ref::<arrow::array::FixedSizeBinaryArray>()
		.ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				"__row_table_object_id__ column has unexpected Arrow type",
			)
		})?;
	if array.is_empty() {
		return Ok(false);
	}
	let bytes = array.value(0);
	if bytes.len() != 16 {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"__row_table_object_id__ has unexpected width: {}",
				bytes.len()
			),
		));
	}
	let mut id_bytes = [0u8; 16];
	id_bytes.copy_from_slice(bytes);
	Ok(TableObjectId::new(id_bytes) == descriptor.table_object_id)
}

fn materialize_projected_location_batch(
	projected: ProjectedRecordBatch,
	record_row_index: RecordRowIndex,
) -> std::io::Result<ProjectedRecordBatch> {
	let row = record_row_index.get() as usize;
	if row >= projected.batch.num_rows() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"record row index {} out of bounds for {}-row projected storage record",
				row,
				projected.batch.num_rows()
			),
		));
	}
	Ok(projected.slice(row, 1))
}

async fn table_stats_may_match_location(
	slab: &SlabManager,
	table_descriptor: Option<&TableSchemaDescriptorV1>,
	program: &pbc::Program,
	cache: &mut QueryStatsPruneCache,
	location: RecordLocation,
) -> std::io::Result<bool> {
	let Some(descriptor) = table_descriptor else {
		return Ok(true);
	};
	if let Some(decision) = cache.decisions.get(&location.slab_id) {
		cache.explain.stats_cache_hits = cache.explain.stats_cache_hits.saturating_add(1);
		return Ok(*decision);
	}
	cache.explain.candidate_slabs_before_stats =
		cache.explain.candidate_slabs_before_stats.saturating_add(1);
	cache.explain.stats_cache_misses = cache.explain.stats_cache_misses.saturating_add(1);
	let Some(pointer) = slab.tier_pointer(location.slab_id).await else {
		cache.explain.slabs_scanned_stats_missing =
			cache.explain.slabs_scanned_stats_missing.saturating_add(1);
		cache.decisions.insert(location.slab_id, true);
		return Ok(true);
	};
	let decrypt_failed_before = metrics().pbc_slab_stats_decrypt_failed_total.get();
	let stats = match slab
		.load_tiered_column_stats_block(&pointer, descriptor)
		.await
	{
		Ok(Some(stats)) => stats,
		Ok(None) => {
			cache.explain.slabs_scanned_stats_missing =
				cache.explain.slabs_scanned_stats_missing.saturating_add(1);
			cache.decisions.insert(location.slab_id, true);
			return Ok(true);
		}
		Err(_) => {
			cache.explain.slabs_scanned_stats_missing =
				cache.explain.slabs_scanned_stats_missing.saturating_add(1);
			let decrypt_failed = metrics()
				.pbc_slab_stats_decrypt_failed_total
				.get()
				.saturating_sub(decrypt_failed_before);
			cache.explain.stats_decrypt_failed_total = cache
				.explain
				.stats_decrypt_failed_total
				.saturating_add(decrypt_failed);
			cache.decisions.insert(location.slab_id, true);
			return Ok(true);
		}
	};
	let outcome = match slab_stats_query_outcome(descriptor, program, &stats) {
		Ok(outcome) => outcome,
		Err(_) => {
			metrics().pbc_slab_stats_unsupported_total.inc();
			cache.explain.slabs_scanned_stats_unsupported = cache
				.explain
				.slabs_scanned_stats_unsupported
				.saturating_add(1);
			cache.decisions.insert(location.slab_id, true);
			return Ok(true);
		}
	};
	if outcome.unsupported {
		metrics().pbc_slab_stats_unsupported_total.inc();
		cache.explain.slabs_scanned_stats_unsupported = cache
			.explain
			.slabs_scanned_stats_unsupported
			.saturating_add(1);
	}
	if outcome.predicate_unrepresentable {
		metrics()
			.pbc_slab_stats_predicate_unrepresentable_total
			.inc();
		cache.explain.slabs_scanned_predicate_unrepresentable = cache
			.explain
			.slabs_scanned_predicate_unrepresentable
			.saturating_add(1);
	}
	if !outcome.may_match {
		metrics().pbc_slab_stats_pruned_total.inc();
		cache.explain.slabs_pruned_by_stats = cache.explain.slabs_pruned_by_stats.saturating_add(1);
	}
	cache.decisions.insert(location.slab_id, outcome.may_match);
	Ok(outcome.may_match)
}

#[cfg_attr(not(test), allow(dead_code))]
fn slab_stats_may_match_query(
	descriptor: &TableSchemaDescriptorV1,
	program: &pbc::Program,
	stats: &ParsedColumnStatsBlock,
) -> std::io::Result<bool> {
	Ok(slab_stats_query_outcome(descriptor, program, stats)?.may_match)
}

fn slab_stats_query_outcome(
	descriptor: &TableSchemaDescriptorV1,
	program: &pbc::Program,
	stats: &ParsedColumnStatsBlock,
) -> std::io::Result<SlabStatsQueryOutcome> {
	let Some(predicates) = program.conjunctive_column_predicates() else {
		return Ok(SlabStatsQueryOutcome {
			may_match: true,
			unsupported: false,
			predicate_unrepresentable: true,
		});
	};
	if predicates.is_empty() {
		return Ok(SlabStatsQueryOutcome {
			may_match: true,
			unsupported: false,
			predicate_unrepresentable: false,
		});
	}
	let mut outcome = SlabStatsQueryOutcome {
		may_match: true,
		unsupported: false,
		predicate_unrepresentable: false,
	};
	let schema = descriptor.arrow_schema()?;
	for predicate in predicates {
		let Some(field) = schema.fields().get(predicate.column_index) else {
			outcome.unsupported = true;
			continue;
		};
		let Some(entry) = stats
			.columns
			.iter()
			.find(|entry| entry.column_ordinal as usize == predicate.column_index)
		else {
			outcome.unsupported = true;
			continue;
		};
		if !column_stats_entry_matches_data_type(entry, field.data_type()) {
			outcome.unsupported = true;
			continue;
		}
		let Some(encoded) = encoded_stats_predicate(field.data_type(), &predicate)? else {
			outcome.predicate_unrepresentable = true;
			continue;
		};
		let may_match = match encoded {
			EncodedStatsPredicate::Range { lower, upper } => {
				prune_by_encoded_min_max(entry, lower.as_deref(), upper.as_deref())?
					== ColumnStatsPrune::MayMatch
			}
			EncodedStatsPredicate::AnyOf(values) => {
				prune_by_encoded_any_of(entry, &values)? == ColumnStatsPrune::MayMatch
			}
		};
		if !may_match {
			outcome.may_match = false;
			return Ok(outcome);
		}
	}
	Ok(outcome)
}

fn encoded_stats_predicate(
	data_type: &DataType,
	predicate: &pbc::ColumnPredicateV1,
) -> std::io::Result<Option<EncodedStatsPredicate>> {
	match predicate.comparison {
		pbc::ColumnPredicateComparisonV1::Eq => {
			let Some(value) = encode_stats_literal(data_type, &predicate.literal)? else {
				return Ok(None);
			};
			Ok(Some(EncodedStatsPredicate::AnyOf(vec![value])))
		}
		pbc::ColumnPredicateComparisonV1::Gt | pbc::ColumnPredicateComparisonV1::Gte => {
			let Some(value) = encode_stats_literal(data_type, &predicate.literal)? else {
				return Ok(None);
			};
			Ok(Some(EncodedStatsPredicate::Range {
				lower: Some(value),
				upper: None,
			}))
		}
		pbc::ColumnPredicateComparisonV1::Lt | pbc::ColumnPredicateComparisonV1::Lte => {
			let Some(value) = encode_stats_literal(data_type, &predicate.literal)? else {
				return Ok(None);
			};
			Ok(Some(EncodedStatsPredicate::Range {
				lower: None,
				upper: Some(value),
			}))
		}
		pbc::ColumnPredicateComparisonV1::InList => {
			let pbc::ColumnPredicateLiteralV1::List(literals) = &predicate.literal else {
				return Ok(None);
			};
			let mut encoded = Vec::with_capacity(literals.len());
			for literal in literals {
				let Some(value) = encode_stats_literal(data_type, literal)? else {
					return Ok(None);
				};
				encoded.push(value);
			}
			Ok(Some(EncodedStatsPredicate::AnyOf(encoded)))
		}
	}
}

fn encode_stats_literal(
	data_type: &DataType,
	literal: &pbc::ColumnPredicateLiteralV1,
) -> std::io::Result<Option<Vec<u8>>> {
	match (data_type, literal) {
		(DataType::Int64, pbc::ColumnPredicateLiteralV1::Int(value))
		| (DataType::Int64, pbc::ColumnPredicateLiteralV1::Ts(value))
		| (
			DataType::Timestamp(TimeUnit::Millisecond, _),
			pbc::ColumnPredicateLiteralV1::Int(value),
		)
		| (
			DataType::Timestamp(TimeUnit::Millisecond, _),
			pbc::ColumnPredicateLiteralV1::Ts(value),
		) => encode_signed_stat_value(*value, 8).map(Some),
		(DataType::Int32, pbc::ColumnPredicateLiteralV1::Int(value))
		| (DataType::Int32, pbc::ColumnPredicateLiteralV1::Ts(value)) => {
			let Ok(value) = i32::try_from(*value) else {
				return Ok(None);
			};
			encode_signed_stat_value(i64::from(value), 4).map(Some)
		}
		(DataType::Utf8 | DataType::LargeUtf8, pbc::ColumnPredicateLiteralV1::Str(bytes)) => {
			if std::str::from_utf8(bytes).is_err() {
				return Ok(None);
			}
			Ok(Some(bytes.clone()))
		}
		_ => Ok(None),
	}
}

/// Plan a [`QueryRequestMeta`] + PBC program against a local
/// [`SlabManager`] without concatenating the full result set.
///
/// The returned plan caches the filtered batches discovered during
/// planning plus the final response meta fields that are independent
/// of Arrow framing. Streaming and unary callers can then reuse the
/// same decoded / filtered batches instead of re-reading each slab
/// location a second time.
async fn plan_query_execution_with_budget_and_retention_floor(
	slab: &SlabManager,
	state_machine: &WendelonStateMachine,
	local_node: NodeId,
	meta: &QueryRequestMeta,
	bytecode: &[u8],
	budget: QueryExecutionBudget,
	retention_floor: Option<HybridTimestamp>,
) -> std::io::Result<QueryExecutionPlan> {
	// 1. Epoch staleness. The authoritative epoch is the highest
	// one the SM has observed an E_safe advance for; a client
	// believing in a past epoch gets a redirect so it can refresh
	// topology rather than fork the read.
	let admission = state_machine.current_admission_snapshot();
	let current_epoch = admission.current_epoch;
	if meta.expected_epoch < current_epoch {
		return Ok(query_plan_error(meta, ReadStatus::EPOCH_STALE, local_node));
	}

	// 2. LINEARIZABLE gate — the scan is only authoritative once
	// the local SM has observed E_safe at-or-past the client's
	// min_hlc for the requested epoch.
	if meta.consistency == Consistency::LINEARIZABLE {
		let watermark = admission
			.e_safe_watermarks
			.get(&meta.expected_epoch)
			.copied()
			.unwrap_or(HybridTimestamp {
				physical_ms: 0,
				logical: 0,
				node_id: 0,
			});
		if watermark < meta.min_hlc {
			return Ok(query_plan_error(
				meta,
				ReadStatus::E_SAFE_BEHIND,
				local_node,
			));
		}
	}

	if snapshot_request_outside_retention(meta.as_of_hlc, retention_floor) {
		return Ok(query_plan_error(
			meta,
			ReadStatus::RETENTION_EXPIRED,
			local_node,
		));
	}

	// 3. Compile PBC once so per-row evaluation skips repeated
	// decoding when the scan hits many records.
	// `pbc::execute` already does this internally per call, but
	// we mirror the pattern so a follow-up that batches rows can
	// lift the compile out of the loop without touching semantics.
	let program = match pbc::Program::compile(bytecode) {
		Ok(program) => program,
		Err(_) => {
			return Ok(query_plan_error(
				meta,
				ReadStatus::QUERY_INVALID_PROGRAM,
				local_node,
			));
		}
	};
	let ranges = [QueryScanRange {
		start: Locality::new(meta.locality_range_start),
		end: Locality::new(meta.locality_range_end),
	}];
	plan_query_execution_for_ranges_with_program(
		slab,
		local_node,
		meta,
		&program,
		&ranges,
		budget,
		None,
		QueryProjectedExecution::Disabled,
		None,
	)
	.await
}

async fn plan_table_query_execution_by_secondary_index(
	slab: &SlabManager,
	local_node: NodeId,
	meta: &QueryRequestMeta,
	program: &pbc::Program,
	ranges: &[QueryScanRange],
	budget: QueryExecutionBudget,
	table_descriptor: &TableSchemaDescriptorV1,
	projected_execution: &QueryProjectedExecution,
	logical_retention_floor: Option<HybridTimestamp>,
	access_plan: &SecondaryIndexAccessPlan,
) -> std::io::Result<QueryExecutionPlan> {
	let budget = QueryExecutionBudget {
		stats_pruning_enabled: budget.stats_pruning_enabled
			&& query_request_stats_pruning_enabled(meta),
		..budget
	};
	let started_at = Instant::now();
	let deadline = started_at + budget.max_wall_time;
	let max_rows_per_location = usize::try_from(budget.max_rows).unwrap_or(usize::MAX);
	let mut stats_prune_cache = QueryStatsPruneCache::default();
	let mut batches = Vec::new();
	let mut rows = 0u64;
	let mut scanned_locations = 0usize;
	let mut max_hlc = HybridTimestamp {
		physical_ms: 0,
		logical: 0,
		node_id: 0,
	};
	let mut seen = BTreeSet::new();
	metrics().observe_pbc_locality_arcs_scanned(ranges.len() as u64);

	let request = TableLookupByIndexRequest::from_descriptor(
		meta.tenant_id,
		&access_plan.descriptor,
		access_plan.query.clone(),
		meta.as_of_hlc,
	);
	let lookup = slab
		.table_lookup_by_index(
			&request,
			&access_plan.descriptor,
			SecondaryIndexState::Ready,
		)
		.await?;
	for row in &lookup.rows {
		if logical_retention_floor.is_some_and(|floor| row.row_hlc < floor) {
			metrics().observe_storage_table_retention_read_filtered_rows(1);
			continue;
		}
		if started_at.elapsed() > budget.max_wall_time {
			return Ok(query_plan_error(
				meta,
				ReadStatus::QUERY_BUDGET_EXCEEDED,
				local_node,
			));
		}
		scanned_locations = scanned_locations.saturating_add(1);
		if scanned_locations > budget.max_locations {
			return Ok(query_plan_error(
				meta,
				ReadStatus::QUERY_BUDGET_EXCEEDED,
				local_node,
			));
		}
		let key = secondary_index_result_key(row.row_key_id, row.record_location);
		if !seen.insert(key) {
			continue;
		}
		let Some(result) = read_secondary_index_query_location(
			slab,
			row,
			table_descriptor,
			program,
			projected_execution,
		)
		.await?
		else {
			continue;
		};
		if result.num_rows() == 0 {
			continue;
		}
		if result.num_rows() > max_rows_per_location {
			return Ok(query_plan_error(
				meta,
				ReadStatus::QUERY_BUDGET_EXCEEDED,
				local_node,
			));
		}
		rows = rows.saturating_add(result.num_rows() as u64);
		if row.row_hlc > max_hlc {
			max_hlc = row.row_hlc;
		}
		if rows > budget.max_rows || started_at.elapsed() > budget.max_wall_time {
			return Ok(query_plan_error(
				meta,
				ReadStatus::QUERY_BUDGET_EXCEEDED,
				local_node,
			));
		}
		batches.push(result);
	}

	let tail_source_slabs = slab
		.secondary_index_tail_source_slabs(meta.tenant_id, &access_plan.descriptor)
		.await;
	for range in ranges {
		let scanned = slab.scan_as_of_with_identity_and_locality(
			meta.tenant_id,
			range.start,
			range.end,
			meta.as_of_hlc,
		);
		for (key_identity, locality, location) in scanned {
			if !tail_source_slabs.contains(&location.slab_id) {
				continue;
			}
			if logical_retention_floor.is_some_and(|floor| location.hlc < floor) {
				metrics().observe_storage_table_retention_read_filtered_rows(1);
				continue;
			}
			if started_at.elapsed() > budget.max_wall_time {
				return Ok(query_plan_error(
					meta,
					ReadStatus::QUERY_BUDGET_EXCEEDED,
					local_node,
				));
			}
			if budget.stats_pruning_enabled
				&& !table_stats_may_match_location(
					slab,
					Some(table_descriptor),
					program,
					&mut stats_prune_cache,
					location,
				)
				.await?
			{
				continue;
			}
			scanned_locations = scanned_locations.saturating_add(1);
			if scanned_locations > budget.max_locations {
				return Ok(query_plan_error(
					meta,
					ReadStatus::QUERY_BUDGET_EXCEEDED,
					local_node,
				));
			}
			let key = secondary_index_result_key(key_identity, location);
			if !seen.insert(key) {
				continue;
			}
			let Some(result) = read_table_query_location(
				slab,
				meta,
				location,
				key_identity,
				locality,
				table_descriptor,
				program,
				projected_execution,
			)
			.await?
			else {
				continue;
			};
			if result.num_rows() == 0 {
				continue;
			}
			if result.num_rows() > max_rows_per_location {
				return Ok(query_plan_error(
					meta,
					ReadStatus::QUERY_BUDGET_EXCEEDED,
					local_node,
				));
			}
			rows = rows.saturating_add(result.num_rows() as u64);
			if location.hlc > max_hlc {
				max_hlc = location.hlc;
			}
			if rows > budget.max_rows || started_at.elapsed() > budget.max_wall_time {
				return Ok(query_plan_error(
					meta,
					ReadStatus::QUERY_BUDGET_EXCEEDED,
					local_node,
				));
			}
			batches.push(result);
		}
	}

	if batches.is_empty() {
		let mut plan = query_plan_error(meta, ReadStatus::KEY_NOT_FOUND, local_node);
		plan.stats_explain = stats_prune_cache.explain;
		plan.secondary_index_access_plan = Some(access_plan.clone());
		return Ok(plan);
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
		batches,
		deadline,
		stats_explain: stats_prune_cache.explain,
		secondary_index_access_plan: Some(access_plan.clone()),
	})
}

fn secondary_index_result_key(
	row_key_id: Identity,
	location: RecordLocation,
) -> (Identity, HybridTimestamp, SlabId, u64, u32, RecordRowIndex) {
	(
		row_key_id,
		location.hlc,
		location.slab_id,
		location.offset,
		location.length,
		location.record_row_index,
	)
}

async fn read_secondary_index_query_location(
	slab: &SlabManager,
	row: &SecondaryIndexLookupRow,
	descriptor: &TableSchemaDescriptorV1,
	program: &pbc::Program,
	projected_execution: &QueryProjectedExecution,
) -> std::io::Result<Option<RecordBatch>> {
	if let Some(result) = read_projected_query_location(
		slab,
		row.record_location,
		descriptor,
		program,
		projected_execution,
	)
	.await?
	{
		return Ok(Some(result));
	}
	let bytes = slab.read(row.record_location).await?;
	let storage_batch = decode_batch(&bytes)?;
	let batch =
		materialize_record_location_batch(&storage_batch, row.record_location.record_row_index)?;
	match pbc::execute_program(program, &batch) {
		Ok(result) => Ok(Some(result)),
		Err(_) => Err(Error::new(
			ErrorKind::InvalidData,
			"secondary index residual PBC evaluation failed",
		)),
	}
}

async fn read_table_query_location(
	slab: &SlabManager,
	meta: &QueryRequestMeta,
	location: RecordLocation,
	key_identity: Identity,
	locality: Locality,
	descriptor: &TableSchemaDescriptorV1,
	program: &pbc::Program,
	projected_execution: &QueryProjectedExecution,
) -> std::io::Result<Option<RecordBatch>> {
	if let Some(result) =
		read_projected_query_location(slab, location, descriptor, program, projected_execution)
			.await?
	{
		return Ok(Some(result));
	}
	let bytes = slab.read(location).await?;
	let decode_started = Instant::now();
	let storage_batch = decode_batch(&bytes)?;
	let storage_row_count = storage_batch.num_rows();
	let was_multirow = has_storage_synthetic_columns(&storage_batch);
	let stored_table_object_id = if was_multirow {
		storage_row_table_object_id_at(&storage_batch, location.record_row_index)?
	} else {
		None
	};
	let batch = materialize_record_location_batch(&storage_batch, location.record_row_index)?;
	if was_multirow {
		metrics().observe_storage_multirow_decode(
			batch.num_rows() as u64,
			bytes.len() as u64,
			decode_started.elapsed(),
		);
		if storage_row_count <= 1 {
			metrics()
				.storage_multirow_uncompacted_row_records_total
				.inc();
		}
	}
	if !table_record_matches_descriptor(
		meta,
		descriptor,
		key_identity,
		locality,
		stored_table_object_id,
		&batch,
	)? {
		return Ok(None);
	}
	match pbc::execute_program(program, &batch) {
		Ok(result) => Ok(Some(result)),
		Err(_) => Ok(None),
	}
}

async fn plan_query_execution_for_ranges_with_program(
	slab: &SlabManager,
	local_node: NodeId,
	meta: &QueryRequestMeta,
	program: &pbc::Program,
	ranges: &[QueryScanRange],
	budget: QueryExecutionBudget,
	table_descriptor: Option<&TableSchemaDescriptorV1>,
	projected_execution: QueryProjectedExecution,
	logical_retention_floor: Option<HybridTimestamp>,
) -> std::io::Result<QueryExecutionPlan> {
	let budget = QueryExecutionBudget {
		stats_pruning_enabled: budget.stats_pruning_enabled
			&& query_request_stats_pruning_enabled(meta),
		..budget
	};
	metrics().observe_pbc_locality_arcs_scanned(ranges.len() as u64);
	let mut batches = Vec::new();
	let mut rows = 0u64;
	let mut scanned_locations = 0usize;
	let started_at = Instant::now();
	let deadline = started_at + budget.max_wall_time;
	let mut max_hlc = HybridTimestamp {
		physical_ms: 0,
		logical: 0,
		node_id: 0,
	};
	let mut stats_prune_cache = QueryStatsPruneCache::default();
	let mut rebatch_groups: Vec<RebatchGroup> = Vec::new();
	let max_rows_per_location = usize::try_from(budget.max_rows).unwrap_or(usize::MAX);
	for range in ranges {
		let scanned = slab.scan_as_of_with_identity_and_locality(
			meta.tenant_id,
			range.start,
			range.end,
			meta.as_of_hlc,
		);
		for (key_identity, locality, location) in scanned {
			if logical_retention_floor.is_some_and(|floor| location.hlc < floor) {
				metrics().observe_storage_table_retention_read_filtered_rows(1);
				continue;
			}
			if started_at.elapsed() > budget.max_wall_time {
				return Ok(query_plan_error(
					meta,
					ReadStatus::QUERY_BUDGET_EXCEEDED,
					local_node,
				));
			}
			if budget.stats_pruning_enabled
				&& !table_stats_may_match_location(
					slab,
					table_descriptor,
					program,
					&mut stats_prune_cache,
					location,
				)
				.await?
			{
				continue;
			}
			scanned_locations = scanned_locations.saturating_add(1);
			if scanned_locations > budget.max_locations {
				return Ok(query_plan_error(
					meta,
					ReadStatus::QUERY_BUDGET_EXCEEDED,
					local_node,
				));
			}
			if let Some(descriptor) = table_descriptor {
				if let QueryProjectedExecution::Aggregate {
					requirement,
					user_column_count,
				} = &projected_execution
				{
					if let Some(result) = read_aggregate_query_location(
						slab,
						meta,
						location,
						key_identity,
						locality,
						descriptor,
						program,
						requirement,
						*user_column_count,
					)
					.await?
					{
						if result.num_rows() == 0 {
							continue;
						}
						let result_rows = result.num_rows() as u64;
						rows = rows.saturating_add(result_rows);
						if location.hlc > max_hlc {
							max_hlc = location.hlc;
						}
						if rows > budget.max_rows || started_at.elapsed() > budget.max_wall_time {
							return Ok(query_plan_error(
								meta,
								ReadStatus::QUERY_BUDGET_EXCEEDED,
								local_node,
							));
						}
						batches.push(result);
					}
					continue;
				}
				if let Some(result) = read_projected_query_location(
					slab,
					location,
					descriptor,
					program,
					&projected_execution,
				)
				.await?
				{
					if result.num_rows() == 0 {
						continue;
					}
					let result_rows = result.num_rows() as u64;
					rows = rows.saturating_add(result_rows);
					if location.hlc > max_hlc {
						max_hlc = location.hlc;
					}
					if rows > budget.max_rows || started_at.elapsed() > budget.max_wall_time {
						return Ok(query_plan_error(
							meta,
							ReadStatus::QUERY_BUDGET_EXCEEDED,
							local_node,
						));
					}
					batches.push(result);
					continue;
				}
			}
			let bytes = slab.read(location).await?;
			let decode_started = Instant::now();
			let storage_batch = decode_batch(&bytes)?;
			let storage_row_count = storage_batch.num_rows();
			let was_multirow = has_storage_synthetic_columns(&storage_batch);
			let stored_table_object_id = if was_multirow {
				storage_row_table_object_id_at(&storage_batch, location.record_row_index)?
			} else {
				None
			};
			let batch =
				materialize_record_location_batch(&storage_batch, location.record_row_index)?;
			if was_multirow {
				metrics().observe_storage_multirow_decode(
					batch.num_rows() as u64,
					bytes.len() as u64,
					decode_started.elapsed(),
				);
				if storage_row_count <= 1 {
					metrics()
						.storage_multirow_uncompacted_row_records_total
						.inc();
				}
			}
			if batch.num_rows() > max_rows_per_location {
				return Ok(query_plan_error(
					meta,
					ReadStatus::QUERY_BUDGET_EXCEEDED,
					local_node,
				));
			}
			if let Some(descriptor) = table_descriptor {
				if !table_record_matches_descriptor(
					meta,
					descriptor,
					key_identity,
					locality,
					stored_table_object_id,
					&batch,
				)? {
					continue;
				}
			}
			let input_rows = batch.num_rows() as u64;
			if let Some(group) = rebatch_groups
				.iter_mut()
				.find(|group| group.schema.as_ref() == batch.schema().as_ref())
			{
				if location.hlc > group.max_hlc {
					group.max_hlc = location.hlc;
				}
				group.input_rows = group.input_rows.saturating_add(input_rows);
				group.batches.push(batch);
			} else {
				rebatch_groups.push(RebatchGroup {
					schema: batch.schema(),
					batches: vec![batch],
					max_hlc: location.hlc,
					input_rows,
				});
			}
		}
	}
	for group in rebatch_groups {
		let concatenated = concat_batches(&group.schema, &group.batches)
			.map_err(|e| Error::new(ErrorKind::InvalidData, format!("PBC concat: {e}")))?;
		let result = match pbc::execute_program(program, &concatenated) {
			Ok(result) => result,
			Err(_) => {
				return Ok(query_plan_error(
					meta,
					ReadStatus::QUERY_INVALID_PROGRAM,
					local_node,
				));
			}
		};
		if result.num_rows() == 0 {
			continue;
		}
		if group.max_hlc > max_hlc {
			max_hlc = group.max_hlc;
		}
		rows = rows.saturating_add(result.num_rows() as u64);
		if rows > budget.max_rows || started_at.elapsed() > budget.max_wall_time {
			return Ok(query_plan_error(
				meta,
				ReadStatus::QUERY_BUDGET_EXCEEDED,
				local_node,
			));
		}
		batches.push(result);
	}

	// 4. Empty result is a legitimate answer. Clients map
	// KEY_NOT_FOUND to an empty batch on the receive side, so we
	// avoid the cost of encoding a zero-row stream.
	if batches.is_empty() {
		let mut plan = query_plan_error(meta, ReadStatus::KEY_NOT_FOUND, local_node);
		plan.stats_explain = stats_prune_cache.explain;
		return Ok(plan);
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
		batches,
		deadline,
		stats_explain: stats_prune_cache.explain,
		secondary_index_access_plan: None,
	})
}

fn encode_query_stream_batches_with_deadline(
	batches: Vec<RecordBatch>,
	deadline: Instant,
) -> std::io::Result<Option<Vec<Vec<u8>>>> {
	// Preserve the query protocol's all-or-nothing status model by
	// finishing Arrow serialization before the first QUERY_RESP frame.
	// Once the first frame is on the wire, continuations have no
	// status surface for QUERY_BUDGET_EXCEEDED.
	let mut encoded = Vec::with_capacity(batches.len());
	for batch in batches {
		if query_deadline_exceeded(deadline) {
			return Ok(None);
		}
		let arrow = wire_encode_batch_for_path(&batch, NetworkCompressionPath::QueryResponse)?;
		if query_deadline_exceeded(deadline) {
			return Ok(None);
		}
		encoded.push(arrow);
	}
	Ok(Some(encoded))
}

fn table_record_matches_descriptor(
	meta: &QueryRequestMeta,
	descriptor: &TableSchemaDescriptorV1,
	key_identity: Identity,
	locality: Locality,
	stored_table_object_id: Option<TableObjectId>,
	batch: &RecordBatch,
) -> std::io::Result<bool> {
	if batch.num_rows() != 1 {
		return Ok(false);
	}
	if batch.schema().as_ref() != &descriptor.arrow_schema()? {
		return Ok(false);
	}
	match &descriptor.row_identity_policy {
		// PK-keyed: re-derive identity from the user batch and
		// confirm it matches the stored synthetic columns. PK
		// identity is `(tenant_id, table_object_id)`-bound through
		// the canonical PK preimage, so re-derivation gives table
		// isolation for free. Phase 8 also stamps
		// `__row_table_object_id__` on multi-row PK rows; when
		// present, use it for an early fail before the (more
		// expensive) PK re-derivation.
		RowIdentityPolicy::PrimaryKey(_) => {
			if let Some(stored) = stored_table_object_id
				&& stored != descriptor.table_object_id
			{
				return Ok(false);
			}
			let mut table_meta: TableWriteRequestMeta = bytemuck::Zeroable::zeroed();
			table_meta.tenant_id = meta.tenant_id;
			table_meta.table_object_id = descriptor.table_object_id;
			table_meta.schema_generation = descriptor.schema_generation;
			table_meta.row_count = 1;
			// PK identity does not depend on writer / row_hlc.
			let zero_hlc = HybridTimestamp {
				physical_ms: 0,
				logical: 0,
				node_id: 0,
			};
			let admission = validate_table_write_request_batch(
				&table_meta,
				batch,
				descriptor,
				WriterNodeId::ZERO,
				zero_hlc,
			)
			.map_err(|err| Error::new(ErrorKind::InvalidData, err.to_string()))?;
			Ok(admission.key_identities.as_slice() == [key_identity]
				&& admission.localities.as_slice() == [locality])
		}
		// AppendOnly: identity is engine-synthesized at write
		// time from `(writer_node_id, commit_hlc,
		// intra_batch_index)` and is not recoverable from the
		// user batch alone. Locality is request-level (§4a) and
		// depends on `parent_write_id`, which the matcher does
		// not see. Phase 8 binds rows back to their descriptor
		// via the per-row `__row_table_object_id__` synthetic
		// column — verify equality with `descriptor.table_object_id`
		// so cross-table table-spread prefix collisions cannot
		// leak rows across descriptors.
		RowIdentityPolicy::AppendOnly => {
			let _ = (meta, key_identity, locality);
			Ok(stored_table_object_id == Some(descriptor.table_object_id))
		}
		RowIdentityPolicy::AppendOnlyContentHash => {
			if stored_table_object_id != Some(descriptor.table_object_id) {
				return Ok(false);
			}
			let mut table_meta: TableWriteRequestMeta = bytemuck::Zeroable::zeroed();
			table_meta.tenant_id = meta.tenant_id;
			table_meta.table_object_id = descriptor.table_object_id;
			table_meta.schema_generation = descriptor.schema_generation;
			table_meta.row_count = 1;
			let zero_hlc = HybridTimestamp {
				physical_ms: 0,
				logical: 0,
				node_id: 0,
			};
			let admission = validate_table_write_request_batch(
				&table_meta,
				batch,
				descriptor,
				WriterNodeId::ZERO,
				zero_hlc,
			)
			.map_err(|err| Error::new(ErrorKind::InvalidData, err.to_string()))?;
			Ok(admission.key_identities.as_slice() == [key_identity]
				&& admission.localities.as_slice() == [locality])
		}
	}
}

#[cfg_attr(not(test), allow(dead_code))]
/// Execute a [`QueryRequestMeta`] + PBC program against a local
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
	execute_query_plan_with_budget_and_retention_floor(
		slab,
		state_machine,
		local_node,
		meta,
		bytecode,
		QueryExecutionBudget::default(),
		None,
	)
	.await
}

#[cfg_attr(not(test), allow(dead_code))]
async fn execute_query_plan_with_budget_and_retention_floor(
	slab: &SlabManager,
	state_machine: &WendelonStateMachine,
	local_node: NodeId,
	meta: &QueryRequestMeta,
	bytecode: &[u8],
	budget: QueryExecutionBudget,
	retention_floor: Option<HybridTimestamp>,
) -> std::io::Result<(QueryResponseMeta, Option<Vec<u8>>)> {
	let plan = plan_query_execution_with_budget_and_retention_floor(
		slab,
		state_machine,
		local_node,
		meta,
		bytecode,
		budget,
		retention_floor,
	)
	.await?;
	if plan.response.status != ReadStatus::OK || plan.batches.is_empty() {
		return Ok((plan.response, None));
	}

	let schema = plan.batches[0].schema();
	let concatenated = concat_batches(&schema, &plan.batches)
		.map_err(|e| Error::new(ErrorKind::InvalidData, format!("PBC concat: {e}")))?;
	let arrow_bytes =
		wire_encode_batch_for_path(&concatenated, NetworkCompressionPath::QueryResponse)?;
	let response = QueryResponseMeta {
		arrow_len: arrow_bytes.len() as u32,
		..plan.response
	};
	Ok((response, Some(arrow_bytes)))
}

fn query_plan_error(
	meta: &QueryRequestMeta,
	status: ReadStatus,
	local_node: NodeId,
) -> QueryExecutionPlan {
	QueryExecutionPlan {
		response: query_error_meta(meta, status, local_node),
		batches: Vec::new(),
		deadline: Instant::now(),
		stats_explain: QueryStatsExplainCounters::default(),
		secondary_index_access_plan: None,
	}
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
	tenant_id: TenantId,
	key_identity: Identity,
	routing_locality: Locality,
	hlc: HybridTimestamp,
	payload_hash: [u8; 32],
	frame_bytes: Vec<u8>,
}

/// Find a ledger entry pending on the local node whose locality
/// range contains `locality`. Returns a clone so the caller can
/// drop the `SmState` snapshot without extending its borrow.
fn pending_owned_entry(
	migrations: &std::collections::HashMap<u64, MigrationLedgerEntry>,
	locality: Locality,
	local_node: NodeId,
) -> Option<MigrationLedgerEntry> {
	migrations
		.values()
		.find(|entry| {
			entry.logical_owner.id == local_node.id
				&& entry.state != MigrationState::Completed
				&& entry.range_start <= locality.get()
				&& locality.get() <= entry.range_end
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
		next_cursor_identity: Identity::ZERO,
		next_cursor_hlc: bytemuck::Zeroable::zeroed(),
		versions_returned: 0,
		bytes_returned: 0,
		_pad1: [0; 4],
	}
}

/// Pack `(key_identity, routing_locality, RecordLocation,
/// frame_bytes)` rows into an Arrow IPC stream over
/// [`migration_record_schema`]. `payload_hash` is computed here —
/// receivers use it to fast-path `append_migration` idempotency
/// without re-hashing themselves.
fn encode_migration_records(
	rows: &[(TenantId, Identity, Locality, RecordLocation, Vec<u8>)],
) -> std::io::Result<Vec<u8>> {
	let mut tenant_id = UInt64Builder::with_capacity(rows.len());
	let mut key_identity = FixedSizeBinaryBuilder::with_capacity(rows.len(), 32);
	let mut routing_locality = UInt64Builder::with_capacity(rows.len());
	let mut hlc_phys = UInt64Builder::with_capacity(rows.len());
	let mut hlc_log = UInt32Builder::with_capacity(rows.len());
	let mut hlc_node = UInt32Builder::with_capacity(rows.len());
	let mut payload = FixedSizeBinaryBuilder::with_capacity(rows.len(), 32);
	let mut frame = LargeBinaryBuilder::new();

	for (row_tenant_id, identity, locality, loc, bytes) in rows {
		tenant_id.append_value(row_tenant_id.get());
		key_identity
			.append_value(identity.as_bytes())
			.map_err(|e| Error::new(ErrorKind::InvalidData, format!("identity append: {e}")))?;
		routing_locality.append_value(locality.get());
		hlc_phys.append_value(loc.hlc.physical_ms);
		hlc_log.append_value(loc.hlc.logical);
		hlc_node.append_value(loc.hlc.node_id);
		let hash = crate::hash::content::digest(bytes);
		payload
			.append_value(hash)
			.map_err(|e| Error::new(ErrorKind::InvalidData, format!("fsb append: {e}")))?;
		frame.append_value(bytes);
	}

	let schema = Arc::new(migration_record_schema());
	let batch = RecordBatch::try_new(
		schema.clone(),
		vec![
			Arc::new(tenant_id.finish()),
			Arc::new(key_identity.finish()),
			Arc::new(routing_locality.finish()),
			Arc::new(hlc_phys.finish()),
			Arc::new(hlc_log.finish()),
			Arc::new(hlc_node.finish()),
			Arc::new(payload.finish()),
			Arc::new(frame.finish()),
		],
	)
	.map_err(|e| Error::new(ErrorKind::InvalidData, format!("migration batch: {e}")))?;

	let mut buf = Vec::new();
	let started = Instant::now();
	let options = crate::network::ipc::wire_ipc_write_options().map_err(|err| {
		observe_wire_encode_error(NetworkCompressionPath::MigrationRecords);
		err
	})?;
	{
		let mut writer =
			StreamWriter::try_new_with_options(&mut buf, &schema, options).map_err(|e| {
				observe_wire_encode_error(NetworkCompressionPath::MigrationRecords);
				Error::new(ErrorKind::InvalidData, format!("arrow init: {e}"))
			})?;
		writer.write(&batch).map_err(|e| {
			observe_wire_encode_error(NetworkCompressionPath::MigrationRecords);
			Error::new(ErrorKind::InvalidData, format!("arrow write: {e}"))
		})?;
		writer.finish().map_err(|e| {
			observe_wire_encode_error(NetworkCompressionPath::MigrationRecords);
			Error::new(ErrorKind::InvalidData, format!("arrow finish: {e}"))
		})?;
	}
	observe_wire_encode_success(
		NetworkCompressionPath::MigrationRecords,
		buf.len() as u64,
		started.elapsed(),
	);
	Ok(buf)
}

/// Inverse of [`encode_migration_records`]. Expects an Arrow IPC
/// stream over [`migration_record_schema`]; any column mismatch or
/// missing value surfaces as `InvalidData`.
fn decode_migration_records(bytes: &[u8]) -> std::io::Result<Vec<MigrationRecord>> {
	let mut reader = StreamReader::try_new(Cursor::new(bytes), None).map_err(|e| {
		crate::network::ipc::observe_wire_decode_error_for_bytes(bytes);
		Error::new(ErrorKind::InvalidData, format!("migrate stream open: {e}"))
	})?;
	let mut out = Vec::new();
	while let Some(batch) = reader.next() {
		let batch = batch.map_err(|e| {
			crate::network::ipc::observe_wire_decode_error_for_bytes(bytes);
			Error::new(ErrorKind::InvalidData, format!("migrate stream read: {e}"))
		})?;
		let tenant_col: &UInt64Array = batch
			.column(0)
			.as_any()
			.downcast_ref()
			.ok_or_else(|| Error::new(ErrorKind::InvalidData, "tenant_id col type"))?;
		let key_col: &FixedSizeBinaryArray = batch
			.column(1)
			.as_any()
			.downcast_ref()
			.ok_or_else(|| Error::new(ErrorKind::InvalidData, "key_identity col type"))?;
		let locality_col: &UInt64Array = batch
			.column(2)
			.as_any()
			.downcast_ref()
			.ok_or_else(|| Error::new(ErrorKind::InvalidData, "routing_locality col type"))?;
		let phys_col: &UInt64Array = batch
			.column(3)
			.as_any()
			.downcast_ref()
			.ok_or_else(|| Error::new(ErrorKind::InvalidData, "hlc_physical_ms col type"))?;
		let log_col: &UInt32Array = batch
			.column(4)
			.as_any()
			.downcast_ref()
			.ok_or_else(|| Error::new(ErrorKind::InvalidData, "hlc_logical col type"))?;
		let node_col: &UInt32Array = batch
			.column(5)
			.as_any()
			.downcast_ref()
			.ok_or_else(|| Error::new(ErrorKind::InvalidData, "hlc_node_id col type"))?;
		let hash_col: &FixedSizeBinaryArray = batch
			.column(6)
			.as_any()
			.downcast_ref()
			.ok_or_else(|| Error::new(ErrorKind::InvalidData, "payload_hash col type"))?;
		let frame_col: &LargeBinaryArray = batch
			.column(7)
			.as_any()
			.downcast_ref()
			.ok_or_else(|| Error::new(ErrorKind::InvalidData, "frame_bytes col type"))?;

		for i in 0..batch.num_rows() {
			let identity_slice = key_col.value(i);
			if identity_slice.len() != 32 {
				return Err(Error::new(ErrorKind::InvalidData, "key_identity width"));
			}
			let mut key_identity = [0u8; 32];
			key_identity.copy_from_slice(identity_slice);
			let hash_slice = hash_col.value(i);
			if hash_slice.len() != 32 {
				return Err(Error::new(ErrorKind::InvalidData, "payload_hash width"));
			}
			let mut payload_hash = [0u8; 32];
			payload_hash.copy_from_slice(hash_slice);
			out.push(MigrationRecord {
				tenant_id: TenantId::new(tenant_col.value(i)),
				key_identity: Identity::new(key_identity),
				routing_locality: Locality::new(locality_col.value(i)),
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

fn encode_pending_witness_records(
	records: &[PendingFinalizationRecord],
) -> std::io::Result<Vec<u8>> {
	postcard::to_stdvec(records)
		.map_err(|err| Error::other(format!("encode pending witness records: {err}")))
}

fn decode_pending_witness_records(bytes: &[u8]) -> std::io::Result<Vec<PendingFinalizationRecord>> {
	postcard::from_bytes(bytes).map_err(|err| {
		Error::new(
			ErrorKind::InvalidData,
			format!("decode pending witness records: {err}"),
		)
	})
}

fn backpressure_scope_name(scope: BackpressureScope) -> &'static str {
	match scope {
		BackpressureScope::Tenant => "tenant",
		BackpressureScope::Actor => "actor",
	}
}

fn completed_write_audit_outcome(status: WriteStatus) -> Option<AuditOutcome> {
	match status {
		WriteStatus::OK | WriteStatus::DUPLICATE | WriteStatus::FINALIZATION_PENDING => None,
		_ => Some(AuditOutcome::Error),
	}
}

fn completed_read_audit_outcome(status: ReadStatus) -> Option<AuditOutcome> {
	match status {
		ReadStatus::OK | ReadStatus::KEY_NOT_FOUND => None,
		_ => Some(AuditOutcome::Error),
	}
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

fn table_query_as_range_query(meta: &TableQueryRequestMeta) -> QueryRequestMeta {
	QueryRequestMeta {
		min_hlc: meta.min_hlc,
		expected_epoch: meta.expected_epoch,
		tenant_id: meta.tenant_id,
		consistency: meta.consistency,
		_pad: meta._pad,
		locality_range_start: 0,
		locality_range_end: u64::MAX,
		bytecode_len: meta.bytecode_len,
		_pad2: [0; 4],
		as_of_hlc: meta.as_of_hlc,
	}
}

fn aggregate_as_range_query(request: &AggregateRequest, expected_epoch: u64) -> QueryRequestMeta {
	let (locality_range_start, locality_range_end) = if request.locality_ranges.is_empty() {
		(0, u64::MAX)
	} else {
		let start = request
			.locality_ranges
			.iter()
			.map(|range| range.start.get())
			.min()
			.unwrap_or(0);
		let end = request
			.locality_ranges
			.iter()
			.map(|range| range.end.get())
			.max()
			.unwrap_or(u64::MAX);
		(start, end)
	};
	QueryRequestMeta {
		min_hlc: request.min_hlc,
		expected_epoch,
		tenant_id: request.tenant_id,
		consistency: request.consistency,
		_pad: [0; 7],
		locality_range_start,
		locality_range_end,
		bytecode_len: request
			.predicate_program
			.as_ref()
			.map(|bytecode| bytecode.len() as u32)
			.unwrap_or(0),
		_pad2: [0; 4],
		as_of_hlc: request.as_of_hlc,
	}
}

fn aggregate_response_as_query_meta(
	request: &AggregateRequest,
	response: &AggregateResponse,
	local_node: NodeId,
	epoch: u64,
) -> QueryResponseMeta {
	let status = match response.status {
		AggregateStatus::Success | AggregateStatus::Partial | AggregateStatus::NumericOverflow => {
			ReadStatus::OK
		}
		AggregateStatus::Rejected => response
			.reject_code
			.map(aggregate_reject_to_read_status)
			.unwrap_or(ReadStatus::QUERY_INVALID_PROGRAM),
	};
	QueryResponseMeta {
		status,
		_pad1: [0; 7],
		value_hlc: response.max_hlc,
		epoch,
		leader_hint: local_node,
		rows: response.diagnostics.rows_admitted,
		arrow_len: request.aggregates.len() as u32,
		_pad2: [0; 4],
	}
}

fn intersect_aggregate_scan_ranges(
	table_ranges: Vec<QueryScanRange>,
	requested_ranges: &[LocalityRange],
) -> Vec<QueryScanRange> {
	if requested_ranges.is_empty() {
		return table_ranges;
	}
	let mut out = Vec::new();
	for table_range in table_ranges {
		for requested in requested_ranges {
			let start = table_range.start.get().max(requested.start.get());
			let end = table_range.end.get().min(requested.end.get());
			if start <= end {
				out.push(QueryScanRange {
					start: Locality::new(start),
					end: Locality::new(end),
				});
			}
		}
	}
	out
}

fn aggregate_owner_subranges(
	membership: &MembershipEpoch,
	replication_factor: usize,
	ranges: &[QueryScanRange],
) -> Result<HashMap<NodeId, Vec<LocalityRange>>, AggRejectCode> {
	if ranges.is_empty() {
		return Ok(HashMap::new());
	}
	let ring = RingState::build(membership, replication_factor);
	if ring.data_capable_nodes() == 0 {
		return Ok(HashMap::new());
	}
	let segments = ring.replica_segments();
	if segments.is_empty() {
		return Ok(HashMap::new());
	}
	let mut assignments: HashMap<NodeId, Vec<LocalityRange>> = HashMap::new();
	for range in ranges {
		for segment in &segments {
			let start = range.start.get().max(segment.start);
			let end = range.end.get().min(segment.end);
			if start > end {
				continue;
			}
			let Some(owner) = segment.replicas.first().copied() else {
				return Err(AggRejectCode::CapabilityUnavailable);
			};
			assignments.entry(owner).or_default().push(LocalityRange {
				start: Locality::new(start),
				end: Locality::new(end),
			});
		}
	}
	for ranges in assignments.values_mut() {
		coalesce_locality_ranges(ranges);
	}
	Ok(assignments)
}

fn coalesce_locality_ranges(ranges: &mut Vec<LocalityRange>) {
	ranges.sort_by_key(|range| (range.start.get(), range.end.get()));
	let mut out: Vec<LocalityRange> = Vec::with_capacity(ranges.len());
	for range in ranges.drain(..) {
		if let Some(last) = out.last_mut() {
			if range.start.get() <= last.end.get().saturating_add(1) {
				if range.end > last.end {
					last.end = range.end;
				}
				continue;
			}
		}
		out.push(range);
	}
	*ranges = out;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct AggregateCsbSlabBounds {
	min_hlc: HybridTimestamp,
	max_hlc: HybridTimestamp,
	row_count: u64,
}

impl AggregateCsbSlabBounds {
	fn from_parts(min_hlc: HybridTimestamp, max_hlc: HybridTimestamp, row_count: u64) -> Self {
		Self {
			min_hlc,
			max_hlc,
			row_count,
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AggregateCsbRetentionDecision {
	Direct,
	ZeroContribution,
	RowFilter,
}

fn aggregate_csb_retention_decision(
	bounds: AggregateCsbSlabBounds,
	floor: HybridTimestamp,
) -> AggregateCsbRetentionDecision {
	if bounds.max_hlc < floor {
		AggregateCsbRetentionDecision::ZeroContribution
	} else if bounds.min_hlc >= floor {
		AggregateCsbRetentionDecision::Direct
	} else {
		AggregateCsbRetentionDecision::RowFilter
	}
}

fn aggregate_csb_zero_response(
	request: &AggregateRequest,
	filtered_rows: u64,
) -> Result<AggregateResponse, AggRejectCode> {
	let mut response = aggregate_record_batches(request, &[], false)?.response;
	response.diagnostics.slabs_consulted = 1;
	response.diagnostics.slabs_pruned_by_csb = 1;
	response.diagnostics.rows_scanned = filtered_rows;
	response.diagnostics.rows_filtered_out = filtered_rows;
	Ok(response)
}

fn aggregate_csb_direct_answer_eligible(
	request: &AggregateRequest,
	ranges: &[QueryScanRange],
) -> bool {
	if is_zero_hlc(request.as_of_hlc) {
		return false;
	}
	if request
		.predicate_program
		.as_ref()
		.is_some_and(|bytecode| !bytecode.is_empty())
	{
		return false;
	}
	if ranges.len() != 1 || ranges[0].start != Locality::ZERO || ranges[0].end != Locality::MAX {
		return false;
	}
	request.aggregates.iter().all(|expression| {
		matches!(
			expression.function(),
			Ok(AggregateFunction::CountStar)
				| Ok(AggregateFunction::CountNonNull { .. })
				| Ok(AggregateFunction::Min { .. })
				| Ok(AggregateFunction::Max { .. })
		)
	})
}

fn aggregate_csb_response_for_stats(
	request: &AggregateRequest,
	descriptor: &TableSchemaDescriptorV1,
	stats: &ParsedColumnStatsBlock,
) -> Result<Option<AggregateResponse>, AggRejectCode> {
	if stats.header.table_object_id != request.table_object_id
		|| stats.header.schema_generation != request.schema_generation
		|| stats.header.logical_schema_fingerprint != request.logical_schema_fingerprint
	{
		if stats.header.logical_schema_fingerprint != request.logical_schema_fingerprint {
			metrics().observe_storage_csb_logical_fingerprint_mismatch(
				request.tenant_id,
				request.table_object_id,
			);
			if stats.header.table_object_id == request.table_object_id
				&& stats.header.schema_generation == request.schema_generation
			{
				metrics().observe_storage_csb_pre_adr_051_cliff(
					request.tenant_id,
					request.table_object_id,
				);
			}
		}
		return Err(AggRejectCode::PartialCrossbindFailed);
	}
	let schema = descriptor
		.arrow_schema()
		.map_err(|_| AggRejectCode::SchemaUnknown)?;
	let mut partials = Vec::with_capacity(request.aggregates.len());
	let mut partial_bytes = 0_u64;
	let mut has_truncated_minmax = false;
	for (index, expression) in request.aggregates.iter().enumerate() {
		let function = expression.function()?;
		let state = match function {
			AggregateFunction::CountStar => AggregateState::Count {
				value: stats.header.row_count,
			},
			AggregateFunction::CountNonNull { column } => {
				let Some(entry) = aggregate_csb_column_entry(stats, &schema, usize::from(column))?
				else {
					observe_aggregate_csb_payload_fall_through(request, "column_uncovered");
					return Ok(None);
				};
				let payload = decode_column_stats_payload(entry)
					.map_err(|_| AggRejectCode::PartialCrossbindFailed)?;
				let Some(value) = payload.non_null_count else {
					observe_aggregate_csb_payload_fall_through(request, "missing_count");
					return Ok(None);
				};
				AggregateState::Count { value }
			}
			AggregateFunction::Min { column } => {
				let Some(entry) = aggregate_csb_column_entry(stats, &schema, usize::from(column))?
				else {
					observe_aggregate_csb_payload_fall_through(request, "column_uncovered");
					return Ok(None);
				};
				if entry.stat_flags & COLUMN_STAT_HAS_MIN == 0 {
					observe_aggregate_csb_payload_fall_through(request, "missing_min_max");
					return Ok(None);
				}
				let payload = decode_column_stats_payload(entry)
					.map_err(|_| AggRejectCode::PartialCrossbindFailed)?;
				AggregateState::MinMax {
					value: payload.min,
					arrow_type_tag: entry.arrow_type_tag,
					truncated: entry.stat_flags & COLUMN_STAT_MIN_TRUNCATED != 0,
				}
			}
			AggregateFunction::Max { column } => {
				let Some(entry) = aggregate_csb_column_entry(stats, &schema, usize::from(column))?
				else {
					observe_aggregate_csb_payload_fall_through(request, "column_uncovered");
					return Ok(None);
				};
				if entry.stat_flags & COLUMN_STAT_HAS_MAX == 0 {
					observe_aggregate_csb_payload_fall_through(request, "missing_min_max");
					return Ok(None);
				}
				let payload = decode_column_stats_payload(entry)
					.map_err(|_| AggRejectCode::PartialCrossbindFailed)?;
				AggregateState::MinMax {
					value: payload.max,
					arrow_type_tag: entry.arrow_type_tag,
					truncated: entry.stat_flags & COLUMN_STAT_MAX_TRUNCATED != 0,
				}
			}
			_ => return Ok(None),
		};
		has_truncated_minmax |= state.has_truncated_minmax();
		let state_bytes = encode_aggregate_state(expression.tag, &state)
			.map_err(|_| AggRejectCode::PartialStateTooLarge)?;
		partial_bytes = partial_bytes.saturating_add(state_bytes.len() as u64);
		if partial_bytes > request.budgets.max_partial_state_bytes.get() {
			return Err(AggRejectCode::PartialStateTooLarge);
		}
		partials.push(
			crate::schema::AggregatePartial::new(
				AggregateExpressionIndex::new(index as u16),
				expression.tag,
				state_bytes,
			)
			.map_err(|_| AggRejectCode::PartialStateTooLarge)?,
		);
	}
	Ok(Some(AggregateResponse {
		format_version: request.format_version,
		request_id: request.request_id,
		status: if has_truncated_minmax {
			AggregateStatus::Partial
		} else {
			AggregateStatus::Success
		},
		reject_code: None,
		table_object_id: request.table_object_id,
		schema_generation: request.schema_generation,
		logical_schema_fingerprint: request.logical_schema_fingerprint,
		aggregate_expression_digest: request.expression_digest(),
		as_of_hlc: request.as_of_hlc,
		max_hlc: request.as_of_hlc,
		partials,
		diagnostics: AggregateDiagnostics {
			slabs_consulted: 1,
			slabs_answered_from_csb: 1,
			rows_scanned: stats.header.row_count,
			rows_admitted: stats.header.row_count,
			..AggregateDiagnostics::default()
		},
	}))
}

fn observe_aggregate_csb_payload_fall_through(request: &AggregateRequest, reason: &str) {
	metrics().observe_storage_csb_payload_coverage_fall_through(
		request.tenant_id,
		request.table_object_id,
		reason,
	);
}

fn aggregate_csb_column_entry<'a>(
	stats: &'a ParsedColumnStatsBlock,
	schema: &Schema,
	column: usize,
) -> Result<Option<&'a ColumnStatsBlockEntry>, AggRejectCode> {
	let Some(field) = schema.fields().get(column) else {
		return Err(AggRejectCode::ColumnUnknown);
	};
	let Some(entry) = stats
		.columns
		.iter()
		.find(|entry| entry.column_ordinal as usize == column)
	else {
		return Ok(None);
	};
	if !column_stats_entry_matches_data_type(entry, field.data_type()) {
		return Ok(None);
	}
	Ok(Some(entry))
}

fn aggregate_query_budget(budgets: &AggregateBudgets) -> QueryExecutionBudget {
	let default = QueryExecutionBudget::default();
	let max_locations = usize::try_from(budgets.max_locations.get())
		.unwrap_or(usize::MAX)
		.min(default.max_locations);
	let max_rows = budgets.max_rows.get().min(default.max_rows);
	let max_wall_time = if budgets.max_wall_time_millis.get() == u64::MAX {
		default.max_wall_time
	} else {
		Duration::from_millis(budgets.max_wall_time_millis.get()).min(default.max_wall_time)
	};
	QueryExecutionBudget {
		max_locations,
		max_rows,
		max_wall_time,
		stats_pruning_enabled: true,
	}
}

fn aggregate_reject_from_read_status(status: ReadStatus) -> AggRejectCode {
	match status {
		ReadStatus::QUERY_INVALID_PROGRAM => AggRejectCode::ProgramInvalid,
		ReadStatus::QUERY_BUDGET_EXCEEDED | ReadStatus::BACKPRESSURED => {
			AggRejectCode::BudgetExceeded
		}
		ReadStatus::EPOCH_STALE => AggRejectCode::SchemaStale,
		ReadStatus::TENANT_UNAVAILABLE
		| ReadStatus::AUTHORIZATION_FAILED
		| ReadStatus::NOT_LEADER
		| ReadStatus::LEASE_EXPIRED
		| ReadStatus::INVALID_LEASE_SIGNATURE => AggRejectCode::CapabilityUnavailable,
		ReadStatus::E_SAFE_BEHIND | ReadStatus::MIGRATION_INCOMPLETE => {
			AggRejectCode::DeadlineExceeded
		}
		ReadStatus::RETENTION_EXPIRED => AggRejectCode::RetentionExpired,
		_ => AggRejectCode::BudgetExceeded,
	}
}

fn aggregate_reject_to_read_status(code: AggRejectCode) -> ReadStatus {
	match code {
		AggRejectCode::ProgramInvalid
		| AggRejectCode::UnsupportedFunction
		| AggRejectCode::ColumnUnknown
		| AggRejectCode::TypeUnsupported
		| AggRejectCode::SketchParameterInvalid
		| AggRejectCode::ApproxNotAllowed
		| AggRejectCode::FormatVersion
		| AggRejectCode::TooManyAggregates
		| AggRejectCode::PartialCrossbindFailed
		| AggRejectCode::PartialStateTooLarge
		| AggRejectCode::NumericOverflow => ReadStatus::QUERY_INVALID_PROGRAM,
		AggRejectCode::SchemaStale => ReadStatus::EPOCH_STALE,
		AggRejectCode::SchemaUnknown | AggRejectCode::CapabilityUnavailable => {
			ReadStatus::TENANT_UNAVAILABLE
		}
		AggRejectCode::BudgetExceeded => ReadStatus::QUERY_BUDGET_EXCEEDED,
		AggRejectCode::DeadlineExceeded => ReadStatus::E_SAFE_BEHIND,
		AggRejectCode::RetentionExpired => ReadStatus::RETENTION_EXPIRED,
	}
}

fn table_query_scan_ranges(
	tenant_id: TenantId,
	descriptor: &TableSchemaDescriptorV1,
	program: Option<&pbc::Program>,
) -> Result<Vec<QueryScanRange>, ReadStatus> {
	match descriptor.locality_policy {
		LocalityPolicyV1::TableSpreadProjection(spread) => {
			let ranges = table_spread_prefix_ranges(tenant_id, descriptor.table_object_id, spread)
				.map_err(|_| ReadStatus::QUERY_INVALID_PROGRAM)?;
			Ok(ranges
				.into_iter()
				.map(|(start, end)| QueryScanRange { start, end })
				.collect())
		}
		LocalityPolicyV1::IdentityProjection => Ok(vec![QueryScanRange {
			start: Locality::ZERO,
			end: Locality::MAX,
		}]),
		LocalityPolicyV1::ColumnProjection(ref projection) => {
			let schema = descriptor
				.arrow_schema()
				.map_err(|_| ReadStatus::QUERY_INVALID_PROGRAM)?;
			let ranges = if let Some(program) = program {
				let decomposition_started = Instant::now();
				let pruning = column_projection_predicate_ranges(
					tenant_id,
					descriptor.table_object_id,
					&schema,
					projection,
					program,
					COLUMN_PROJECTION_RANGE_ARC_BUDGET,
				)
				.map_err(|_| ReadStatus::QUERY_INVALID_PROGRAM)?;
				metrics().observe_pbc_locality_decomposition(decomposition_started.elapsed());
				match pruning {
					ColumnProjectionRangePruning::Pruned(ranges) => ranges,
					ColumnProjectionRangePruning::Fallback => column_projection_prefix_ranges(
						tenant_id,
						descriptor.table_object_id,
						projection,
					)
					.map_err(|_| ReadStatus::QUERY_INVALID_PROGRAM)?,
					ColumnProjectionRangePruning::Overflow => {
						metrics().observe_pbc_range_decomposition_overflow();
						column_projection_prefix_ranges(
							tenant_id,
							descriptor.table_object_id,
							projection,
						)
						.map_err(|_| ReadStatus::QUERY_INVALID_PROGRAM)?
					}
				}
			} else {
				column_projection_prefix_ranges(tenant_id, descriptor.table_object_id, projection)
					.map_err(|_| ReadStatus::QUERY_INVALID_PROGRAM)?
			};
			Ok(ranges
				.into_iter()
				.map(|(start, end)| QueryScanRange { start, end })
				.collect())
		}
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

fn table_write_response(
	write: WriteResponseMeta,
	reject_code: PbcRejectCode,
) -> TableWriteResponseMeta {
	TableWriteResponseMeta {
		write,
		reject_code: reject_code.0,
		_pad: [0; 6],
	}
}

fn table_delete_rows_response(
	write: WriteResponseMeta,
	deleted_count: u32,
	already_deleted_count: u32,
	not_found_count: u32,
	reject_code: PbcRejectCode,
) -> TableDeleteRowsResponseMeta {
	TableDeleteRowsResponseMeta {
		write,
		deleted_count,
		already_deleted_count,
		not_found_count,
		reject_code: reject_code.0,
		_pad: [0; 10],
	}
}

fn table_write_synthetic_meta(
	meta: &TableWriteRequestMeta,
	key_identity: Identity,
	charged_bytes: u64,
) -> WriteRequestMeta {
	WriteRequestMeta {
		write_id: meta.parent_write_id,
		client_hlc: meta.client_hlc,
		expected_epoch: meta.expected_epoch,
		tenant_id: meta.tenant_id,
		proxy_hop: meta.proxy_hop,
		durability: meta.durability,
		await_finalization: meta.await_finalization,
		_pad1: [0; 5],
		proxy_source: meta.proxy_source,
		key_identity,
		key_len: 0,
		value_len: u32::try_from(charged_bytes).unwrap_or(u32::MAX),
	}
}

fn table_delete_synthetic_write_meta(
	meta: &TableDeleteRowsRequestMeta,
	key_identity: Identity,
	charged_bytes: u64,
) -> WriteRequestMeta {
	WriteRequestMeta {
		write_id: meta.parent_write_id,
		client_hlc: meta.client_hlc,
		expected_epoch: meta.expected_epoch,
		tenant_id: meta.tenant_id,
		proxy_hop: 0,
		durability: meta.durability,
		await_finalization: meta.await_finalization,
		_pad1: [0; 5],
		proxy_source: NodeId {
			id: 0,
			_reserved: 0,
		},
		key_identity,
		key_len: 0,
		value_len: u32::try_from(charged_bytes).unwrap_or(u32::MAX),
	}
}

fn table_delete_synthetic_table_write_meta(
	meta: &TableDeleteRowsRequestMeta,
	row_count: usize,
) -> std::io::Result<TableWriteRequestMeta> {
	Ok(TableWriteRequestMeta {
		parent_write_id: meta.parent_write_id,
		client_hlc: meta.client_hlc,
		expected_epoch: meta.expected_epoch,
		tenant_id: meta.tenant_id,
		table_object_id: meta.table_object_id,
		schema_generation: meta.schema_generation,
		child_ordinal_base: 0,
		proxy_source: NodeId {
			id: 0,
			_reserved: 0,
		},
		row_count: encode_table_write_row_count(row_count)?,
		proxy_hop: 0,
		durability: meta.durability,
		await_finalization: meta.await_finalization,
		_pad: [0; 3],
	})
}

fn table_delete_noop_write_response(
	meta: &TableDeleteRowsRequestMeta,
	commit_hlc: HybridTimestamp,
	leader_hint: NodeId,
) -> WriteResponseMeta {
	WriteResponseMeta {
		status: WriteStatus::OK,
		_pad1: [0; 7],
		log_index: 0,
		term: 0,
		epoch: meta.expected_epoch,
		commit_hlc,
		leader_hint,
		echoed_write_id: meta.parent_write_id,
		has_finalization: 0,
		_pad2: [0; 7],
	}
}

fn table_write_noop_write_response(
	meta: &TableWriteRequestMeta,
	commit_hlc: HybridTimestamp,
	leader_hint: NodeId,
) -> WriteResponseMeta {
	WriteResponseMeta {
		status: WriteStatus::OK,
		_pad1: [0; 7],
		log_index: 0,
		term: 0,
		epoch: meta.expected_epoch,
		commit_hlc,
		leader_hint,
		echoed_write_id: meta.parent_write_id,
		has_finalization: 0,
		_pad2: [0; 7],
	}
}

fn observe_content_hash_admission_metrics(
	tenant_id: TenantId,
	table_object_id: TableObjectId,
	absorbed_rows: u64,
	stored_rows: u64,
) {
	if absorbed_rows > 0 {
		metrics().observe_storage_content_hash_admission(
			tenant_id,
			table_object_id,
			"absorbed",
			absorbed_rows,
		);
		metrics().observe_storage_content_hash_duplicate_absorption(
			tenant_id,
			table_object_id,
			absorbed_rows,
		);
	}
	if stored_rows > 0 {
		metrics().observe_storage_content_hash_admission(
			tenant_id,
			table_object_id,
			"stored",
			stored_rows,
		);
	}
}

fn tombstone_marker_layout(
	descriptor: &TableSchemaDescriptorV1,
) -> Result<MultiRowLayoutDescriptor, PbcRejectCode> {
	match descriptor.record_layout_policy {
		RecordLayoutPolicy::MultiRow(layout) => Ok(layout),
		RecordLayoutPolicy::SingleRow
			if matches!(
				descriptor.row_identity_policy,
				RowIdentityPolicy::PrimaryKey(_)
			) =>
		{
			Ok(MultiRowLayoutDescriptor::default())
		}
		RecordLayoutPolicy::SingleRow => {
			Err(PbcRejectCode::PBC_REJECT_APPEND_ONLY_REQUIRES_MULTIROW)
		}
	}
}

fn table_write_status_for_reject(code: PbcRejectCode) -> WriteStatus {
	match code {
		PbcRejectCode::PBC_REJECT_SCHEMA_STALE => WriteStatus::EPOCH_STALE,
		PbcRejectCode::PBC_REJECT_BATCH_TOO_LARGE => WriteStatus::PAYLOAD_TOO_LARGE,
		_ => WriteStatus::INTERNAL_ERROR,
	}
}

fn table_replica_child_schema() -> Schema {
	Schema::new(vec![
		Field::new("child_ordinal", DataType::UInt64, false),
		Field::new("key_identity", DataType::FixedSizeBinary(32), false),
		Field::new("routing_locality", DataType::UInt64, false),
		Field::new("record_row_index", DataType::UInt32, false),
		Field::new("payload_hash", DataType::FixedSizeBinary(32), false),
		Field::new("charged_bytes", DataType::UInt64, false),
		Field::new("frame", DataType::LargeBinary, false),
	])
}

fn table_delete_rows_schema() -> Schema {
	Schema::new(vec![Field::new(
		STORAGE_ROW_KEY_ID_COLUMN,
		DataType::FixedSizeBinary(32),
		false,
	)])
}

fn decode_table_delete_rows_batch(
	meta: &TableDeleteRowsRequestMeta,
	batch: &RecordBatch,
) -> std::io::Result<Vec<Identity>> {
	if batch.schema().as_ref() != &table_delete_rows_schema() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"TABLE_DELETE_REQ Arrow payload schema mismatch",
		));
	}
	if batch.num_rows() != meta.row_count as usize {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"TABLE_DELETE_REQ row_count mismatch: meta={} actual={}",
				meta.row_count,
				batch.num_rows()
			),
		));
	}
	if batch.num_rows() == 0 {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"TABLE_DELETE_REQ requires at least one row key id",
		));
	}
	let row_key_ids = batch
		.column_by_name(STORAGE_ROW_KEY_ID_COLUMN)
		.and_then(|array| array.as_any().downcast_ref::<FixedSizeBinaryArray>())
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "missing row key id column"))?;
	if row_key_ids.null_count() != 0 {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"TABLE_DELETE_REQ row key ids must not be null",
		));
	}
	(0..batch.num_rows())
		.map(|row| {
			if row_key_ids.is_null(row) {
				return Err(Error::new(
					ErrorKind::InvalidData,
					"TABLE_DELETE_REQ row key ids must not be null",
				));
			}
			Ok(Identity::new(fixed_32_value(
				row_key_ids,
				row,
				STORAGE_ROW_KEY_ID_COLUMN,
			)?))
		})
		.collect()
}

fn table_write_parent_identity(meta: &TableWriteRequestMeta) -> Identity {
	let mut hasher = ::blake3::Hasher::new();
	hasher.update(b"wendelon.table-write-parent.v1");
	hasher.update(&meta.tenant_id.get().to_le_bytes());
	hasher.update(meta.table_object_id.as_bytes());
	// Canonical idempotency bytes only (no TTL / pad). A retry-
	// stable parent identity must agree across retries with
	// drifted `idempotency_ttl_ms`.
	hasher.update(&meta.parent_write_id.canonical_idempotency_bytes());
	hasher.update(&meta.schema_generation.to_le_bytes());
	hasher.update(&(meta.decoded_row_count() as u64).to_le_bytes());
	Identity::new(*hasher.finalize().as_bytes())
}

fn table_delete_parent_identity(meta: &TableDeleteRowsRequestMeta) -> Identity {
	let mut hasher = ::blake3::Hasher::new();
	hasher.update(b"table.delete-parent.v1");
	hasher.update(&meta.tenant_id.get().to_le_bytes());
	hasher.update(meta.table_object_id.as_bytes());
	hasher.update(&meta.parent_write_id.canonical_idempotency_bytes());
	hasher.update(&meta.schema_generation.to_le_bytes());
	hasher.update(&meta.row_count.to_le_bytes());
	Identity::new(*hasher.finalize().as_bytes())
}

fn table_batch_payload_digest(children: &[BifurcatedTableChildWrite]) -> [u8; 32] {
	let mut hasher = ::blake3::Hasher::new();
	hasher.update(b"wendelon.table-batch-payload.v1");
	hasher.update(&(children.len() as u64).to_le_bytes());
	for child in children {
		hasher.update(&child.child_ordinal.to_le_bytes());
		hasher.update(child.key_identity.as_bytes());
		hasher.update(&child.record_row_index.get().to_le_bytes());
		hasher.update(&child.payload_hash);
		hasher.update(&child.charged_bytes.to_le_bytes());
	}
	*hasher.finalize().as_bytes()
}

fn table_replica_payload_digest(children: &[TableReplicaChildPayload]) -> [u8; 32] {
	let mut hasher = ::blake3::Hasher::new();
	hasher.update(b"wendelon.table-batch-payload.v1");
	hasher.update(&(children.len() as u64).to_le_bytes());
	for child in children {
		hasher.update(&child.child_ordinal.to_le_bytes());
		hasher.update(child.key_identity.as_bytes());
		hasher.update(&child.record_row_index.get().to_le_bytes());
		hasher.update(&child.payload_hash);
		hasher.update(&child.charged_bytes.to_le_bytes());
	}
	*hasher.finalize().as_bytes()
}

fn table_replica_meta(
	meta: &TableWriteRequestMeta,
	bifurcated: &BifurcatedTableWrite,
	group_children: &[BifurcatedTableChildWrite],
	commit_hlc: HybridTimestamp,
) -> TableReplicaWriteMeta {
	let group_charged_bytes = group_children
		.iter()
		.map(|child| child.charged_bytes)
		.fold(0_u64, u64::saturating_add);
	TableReplicaWriteMeta {
		commit_hlc,
		parent_write_id: meta.parent_write_id,
		epoch: meta.expected_epoch,
		tenant_id: meta.tenant_id,
		table_object_id: meta.table_object_id,
		charged_bytes: bifurcated.charged_bytes,
		group_charged_bytes,
		parent_key_identity: bifurcated.parent_key_identity,
		payload_hash: bifurcated.payload_hash,
		group_payload_hash: table_batch_payload_digest(group_children),
		record_count: bifurcated.record_count,
		group_record_count: u32::try_from(group_children.len()).unwrap_or(u32::MAX),
		durability: meta.durability,
		await_finalization: meta.await_finalization,
		_pad: [0; 6],
	}
}

fn restamp_table_children_commit_hlc(
	children: &mut [BifurcatedTableChildWrite],
	commit_hlc: HybridTimestamp,
	policy: RecordBodyCompressionPolicy,
) -> std::io::Result<()> {
	for child in children {
		let decoded = decode_batch(child.frame_bytes.as_slice())?;
		if !has_storage_synthetic_columns(&decoded) {
			continue;
		}
		let rewritten = replace_storage_row_hlc_column(&decoded, commit_hlc)?;
		let encoded = encode_batch_with_policy(&rewritten, policy)?;
		let frame = encoded.bytes;
		child.frame_bytes = Arc::new(frame);
		child.payload_hash = payload_digest(child.frame_bytes.as_slice());
		child.charged_bytes = child.frame_bytes.len() as u64;
	}
	Ok(())
}

fn restamp_table_write_commit_hlc(
	bifurcated: &mut BifurcatedTableWrite,
	commit_hlc: HybridTimestamp,
	policy: RecordBodyCompressionPolicy,
) -> std::io::Result<()> {
	restamp_table_children_commit_hlc(&mut bifurcated.children, commit_hlc, policy)?;
	bifurcated.charged_bytes = bifurcated
		.children
		.iter()
		.map(|child| child.charged_bytes)
		.fold(0_u64, u64::saturating_add);
	bifurcated.payload_hash = table_batch_payload_digest(&bifurcated.children);
	Ok(())
}

fn encode_table_replica_children(
	children: &[BifurcatedTableChildWrite],
) -> std::io::Result<Vec<u8>> {
	let mut child_ordinal = UInt64Builder::with_capacity(children.len());
	let mut key_identity = FixedSizeBinaryBuilder::with_capacity(children.len(), 32);
	let mut routing_locality = UInt64Builder::with_capacity(children.len());
	let mut record_row_index = UInt32Builder::with_capacity(children.len());
	let mut payload_hash = FixedSizeBinaryBuilder::with_capacity(children.len(), 32);
	let mut charged_bytes = UInt64Builder::with_capacity(children.len());
	let mut frame = LargeBinaryBuilder::new();

	for child in children {
		child_ordinal.append_value(child.child_ordinal);
		key_identity
			.append_value(child.key_identity.as_bytes())
			.map_err(|e| Error::new(ErrorKind::InvalidData, format!("identity append: {e}")))?;
		routing_locality.append_value(child.locality.get());
		record_row_index.append_value(child.record_row_index.get());
		payload_hash
			.append_value(child.payload_hash)
			.map_err(|e| Error::new(ErrorKind::InvalidData, format!("payload hash append: {e}")))?;
		charged_bytes.append_value(child.charged_bytes);
		frame.append_value(child.frame_bytes.as_slice());
	}

	let schema = Arc::new(table_replica_child_schema());
	let batch = RecordBatch::try_new(
		schema,
		vec![
			Arc::new(child_ordinal.finish()),
			Arc::new(key_identity.finish()),
			Arc::new(routing_locality.finish()),
			Arc::new(record_row_index.finish()),
			Arc::new(payload_hash.finish()),
			Arc::new(charged_bytes.finish()),
			Arc::new(frame.finish()),
		],
	)
	.map_err(|e| Error::new(ErrorKind::InvalidData, format!("table replica batch: {e}")))?;
	encode_batch(&batch)
}

fn fixed_32_value(
	array: &FixedSizeBinaryArray,
	row: usize,
	label: &str,
) -> std::io::Result<[u8; 32]> {
	let value = array.value(row);
	if value.len() != 32 {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!("{label} width mismatch: expected=32 actual={}", value.len()),
		));
	}
	let mut out = [0_u8; 32];
	out.copy_from_slice(value);
	Ok(out)
}

fn decode_table_replica_children(bytes: &[u8]) -> std::io::Result<Vec<TableReplicaChildPayload>> {
	let batch = decode_batch(bytes)?;
	if batch.schema().as_ref() != &table_replica_child_schema() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"TABLE_REPLICATE_REQ payload schema mismatch",
		));
	}
	let child_ordinal = batch
		.column_by_name("child_ordinal")
		.and_then(|array| array.as_any().downcast_ref::<UInt64Array>())
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "missing child_ordinal column"))?;
	let key_identity = batch
		.column_by_name("key_identity")
		.and_then(|array| array.as_any().downcast_ref::<FixedSizeBinaryArray>())
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "missing key_identity column"))?;
	let routing_locality = batch
		.column_by_name("routing_locality")
		.and_then(|array| array.as_any().downcast_ref::<UInt64Array>())
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "missing routing_locality column"))?;
	let record_row_index = batch
		.column_by_name("record_row_index")
		.and_then(|array| array.as_any().downcast_ref::<UInt32Array>())
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "missing record_row_index column"))?;
	let payload_hash = batch
		.column_by_name("payload_hash")
		.and_then(|array| array.as_any().downcast_ref::<FixedSizeBinaryArray>())
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "missing payload_hash column"))?;
	let charged_bytes = batch
		.column_by_name("charged_bytes")
		.and_then(|array| array.as_any().downcast_ref::<UInt64Array>())
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "missing charged_bytes column"))?;
	let frame = batch
		.column_by_name("frame")
		.and_then(|array| array.as_any().downcast_ref::<LargeBinaryArray>())
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "missing frame column"))?;

	let mut children = Vec::with_capacity(batch.num_rows());
	for row in 0..batch.num_rows() {
		children.push(TableReplicaChildPayload {
			child_ordinal: child_ordinal.value(row),
			key_identity: Identity::new(fixed_32_value(key_identity, row, "key_identity")?),
			locality: Locality::new(routing_locality.value(row)),
			record_row_index: RecordRowIndex::new(record_row_index.value(row)),
			payload_hash: fixed_32_value(payload_hash, row, "payload_hash")?,
			charged_bytes: charged_bytes.value(row),
			frame_bytes: Arc::new(frame.value(row).to_vec()),
		});
	}
	Ok(children)
}

fn validate_table_replica_payload(
	meta: &TableReplicaWriteMeta,
	children: &[TableReplicaChildPayload],
) -> std::io::Result<()> {
	if children.is_empty() || children.len() != meta.group_record_count as usize {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"table replica group_record_count mismatch: meta={} actual={}",
				meta.group_record_count,
				children.len()
			),
		));
	}
	let charged_bytes = children
		.iter()
		.map(|child| child.charged_bytes)
		.fold(0_u64, u64::saturating_add);
	if charged_bytes != meta.group_charged_bytes {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"table replica group_charged_bytes mismatch: meta={} actual={charged_bytes}",
				meta.group_charged_bytes
			),
		));
	}
	for child in children {
		let actual_len = child.frame_bytes.len() as u64;
		if child.charged_bytes != actual_len {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"table child charged_bytes mismatch: meta={} actual={actual_len}",
					child.charged_bytes
				),
			));
		}
		if payload_digest(child.frame_bytes.as_slice()) != child.payload_hash {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"table child payload hash mismatch",
			));
		}
	}
	if table_replica_payload_digest(children) != meta.group_payload_hash {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"table replica group payload hash mismatch",
		));
	}
	Ok(())
}

async fn append_bifurcated_table_children_durable(
	slab: &SlabManager,
	tenant_id: TenantId,
	commit_hlc: HybridTimestamp,
	children: &[BifurcatedTableChildWrite],
	secondary_index_descriptors: &[SecondaryIndexDescriptor],
) -> std::io::Result<()> {
	for child in children {
		let decoded = decode_batch(child.frame_bytes.as_slice())?;
		if has_storage_synthetic_columns(&decoded) {
			let entries = storage_index_entries_from_batch(&decoded)?;
			let base_location = slab
				.append_durable_multirow_with_locality(
					tenant_id,
					child.frame_bytes.as_slice(),
					&entries,
				)
				.await?;
			append_secondary_index_active_buffers_for_storage_batch(
				slab,
				tenant_id,
				secondary_index_descriptors,
				&decoded,
				base_location,
			)
			.await;
			metrics().observe_storage_multirow_record_written(entries.len() as u64);
		} else {
			let location = slab
				.append_durable_with_locality(
					tenant_id,
					child.key_identity,
					child.locality,
					commit_hlc,
					child.frame_bytes.as_slice(),
				)
				.await?;
			append_secondary_index_active_buffers_for_single_row(
				slab,
				tenant_id,
				secondary_index_descriptors,
				&decoded,
				child.key_identity,
				commit_hlc,
				location,
			)
			.await;
		}
	}
	Ok(())
}

async fn append_secondary_index_active_buffers_for_storage_batch(
	slab: &SlabManager,
	tenant_id: TenantId,
	descriptors: &[SecondaryIndexDescriptor],
	batch: &RecordBatch,
	base_location: RecordLocation,
) {
	if descriptors.is_empty() {
		return;
	}
	for descriptor in descriptors {
		if let Err(err) = slab
			.seal_secondary_index_active_buffer_if_source_changed(
				tenant_id,
				descriptor,
				base_location.slab_id,
			)
			.await
		{
			tracing::warn!(
				tenant_id = tenant_id.get(),
				table_object_id = %format_bytes_hex(descriptor.table_object_id.as_bytes()),
				index_object_id = %format_bytes_hex(descriptor.index_object_id.as_bytes()),
				source_slab_id = base_location.slab_id.get(),
				error = %err,
				"secondary index active-buffer seal failed before moving to the next slab"
			);
		}
		if let Err(err) = slab.append_secondary_index_active_buffer_batch(
			tenant_id,
			descriptor,
			batch,
			base_location,
			DEFAULT_SECONDARY_INDEX_ACTIVE_BUFFER_BYTES,
		) {
			tracing::warn!(
				tenant_id = tenant_id.get(),
				table_object_id = %format_bytes_hex(descriptor.table_object_id.as_bytes()),
				index_object_id = %format_bytes_hex(descriptor.index_object_id.as_bytes()),
				error = %err,
				"secondary index active-buffer admission failed after durable table append"
			);
		}
	}
}

async fn append_secondary_index_active_buffers_for_single_row(
	slab: &SlabManager,
	tenant_id: TenantId,
	descriptors: &[SecondaryIndexDescriptor],
	batch: &RecordBatch,
	row_key_id: Identity,
	row_hlc: HybridTimestamp,
	record_location: RecordLocation,
) {
	if descriptors.is_empty() {
		return;
	}
	for descriptor in descriptors {
		if let Err(err) = slab
			.seal_secondary_index_active_buffer_if_source_changed(
				tenant_id,
				descriptor,
				record_location.slab_id,
			)
			.await
		{
			tracing::warn!(
				tenant_id = tenant_id.get(),
				table_object_id = %format_bytes_hex(descriptor.table_object_id.as_bytes()),
				index_object_id = %format_bytes_hex(descriptor.index_object_id.as_bytes()),
				source_slab_id = record_location.slab_id.get(),
				error = %err,
				"secondary index active-buffer seal failed before moving to the next slab"
			);
		}
		let entry = match secondary_index_entry_from_materialized_row(
			descriptor,
			batch,
			0,
			row_key_id,
			row_hlc,
			record_location,
		) {
			Ok(Some(entry)) => entry,
			Ok(None) => continue,
			Err(err) => {
				tracing::warn!(
					tenant_id = tenant_id.get(),
					table_object_id = %format_bytes_hex(descriptor.table_object_id.as_bytes()),
					index_object_id = %format_bytes_hex(descriptor.index_object_id.as_bytes()),
					error = %err,
					"secondary index single-row admission encode failed after durable table append"
				);
				continue;
			}
		};
		if let Err(err) = slab.append_secondary_index_active_buffer_entries(
			tenant_id,
			descriptor,
			vec![entry],
			record_location.slab_id,
			DEFAULT_SECONDARY_INDEX_ACTIVE_BUFFER_BYTES,
		) {
			tracing::warn!(
				tenant_id = tenant_id.get(),
				table_object_id = %format_bytes_hex(descriptor.table_object_id.as_bytes()),
				index_object_id = %format_bytes_hex(descriptor.index_object_id.as_bytes()),
				error = %err,
				"secondary index active-buffer admission failed after durable table append"
			);
		}
	}
}

async fn append_table_children_durable(
	slab: &SlabManager,
	tenant_id: TenantId,
	commit_hlc: HybridTimestamp,
	children: &[TableReplicaChildPayload],
) -> std::io::Result<()> {
	for child in children {
		let decoded = decode_batch(child.frame_bytes.as_slice())?;
		if has_storage_synthetic_columns(&decoded) {
			let entries = storage_index_entries_from_batch(&decoded)?;
			slab.append_durable_multirow_with_locality(
				tenant_id,
				child.frame_bytes.as_slice(),
				&entries,
			)
			.await?;
			metrics().observe_storage_multirow_record_written(entries.len() as u64);
		} else {
			slab.append_durable_with_locality(
				tenant_id,
				child.key_identity,
				child.locality,
				commit_hlc,
				child.frame_bytes.as_slice(),
			)
			.await?;
		}
	}
	Ok(())
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
	validate_key_identity(
		meta.tenant_id,
		meta.key_identity,
		extract_first_binary(batch, "key")?,
	)?;
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
	validate_key_identity(
		meta.tenant_id,
		meta.key_identity,
		extract_first_binary(batch, "key")?,
	)?;
	Ok(())
}

fn validate_key_identity(
	tenant_id: TenantId,
	expected: Identity,
	key_bytes: &[u8],
) -> std::io::Result<()> {
	let computed = Identity::blake3_tenant(tenant_id, key_bytes);
	if expected == computed {
		return Ok(());
	}
	Err(Error::new(
		ErrorKind::InvalidData,
		format!(
			"key_identity mismatch: meta={} computed={}",
			expected, computed
		),
	))
}

// ============================================================
//  ARROW / HASH HELPERS
// ============================================================

fn bifurcate_client_write(
	meta: &WriteRequestMeta,
	batch: &RecordBatch,
) -> std::io::Result<BifurcatedWrite> {
	let key_bytes = extract_first_binary(batch, "key")?;
	// Re-validate at the bifurcation boundary so every future caller
	// that reaches this helper inherits the tamper check even if the
	// ingress-side batch validator changes.
	validate_key_identity(meta.tenant_id, meta.key_identity, key_bytes)?;
	let frame_bytes = Arc::new(encode_batch(batch)?);
	Ok(BifurcatedWrite {
		key_identity: meta.key_identity,
		locality: Locality::from(meta.key_identity),
		payload_hash: payload_digest(frame_bytes.as_slice()),
		charged_bytes: frame_bytes.len() as u64,
		frame_bytes,
	})
}

fn bifurcate_table_delete(
	meta: &TableDeleteRowsRequestMeta,
	descriptor: &TableSchemaDescriptorV1,
	packed: Vec<crate::storage::multirow::PackedMultiRowStorageFrame>,
) -> std::io::Result<BifurcatedTableWrite> {
	if packed.is_empty() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"table delete requires at least one tombstone record",
		));
	}
	let mut children = Vec::with_capacity(packed.len());
	let mut charged_bytes = 0_u64;
	for record in packed {
		let first = record.encoded.rows.first().ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				"tombstone multi-row record cannot be empty",
			)
		})?;
		let key_identity = first.key_identity;
		let locality = first.routing_locality;
		let frame_bytes = Arc::new(record.encoded.frame);
		let child_charged_bytes = record.encoded.encoded_byte_len;
		charged_bytes = charged_bytes.saturating_add(child_charged_bytes);
		children.push(BifurcatedTableChildWrite {
			child_ordinal: children.len() as u64,
			key_identity,
			locality,
			record_row_index: RecordRowIndex::ZERO,
			payload_hash: payload_digest(frame_bytes.as_slice()),
			charged_bytes: child_charged_bytes,
			frame_bytes,
		});
	}
	let record_count = u32::try_from(children.len()).map_err(|_| {
		Error::new(
			ErrorKind::InvalidData,
			"table delete record count exceeds u32",
		)
	})?;
	let payload_hash = table_batch_payload_digest(&children);
	Ok(BifurcatedTableWrite {
		parent_key_identity: table_delete_parent_identity(meta),
		payload_hash,
		children,
		charged_bytes,
		record_count,
		record_body_compression_policy: descriptor.record_body_compression_policy,
	})
}

#[cfg_attr(not(test), allow(dead_code))]
/// Bifurcate an admitted client table write into per-record
/// children.
///
/// `row_hlc` is the leader-merged commit HLC the caller has
/// already stamped via `HybridLogicalClockDriver::merge`. ADR
/// 045 admission feeds it into the append-only identity
/// preimage; the multi-row packer stamps it into the persisted
/// `__row_hlc__` column. The two MUST agree.
fn bifurcate_table_write(
	meta: &TableWriteRequestMeta,
	batch: &RecordBatch,
	descriptor: &TableSchemaDescriptorV1,
	writer_node_id: WriterNodeId,
	row_hlc: HybridTimestamp,
) -> Result<BifurcatedTableWrite, TableWriteValidationError> {
	let observe_encode = matches!(
		&descriptor.locality_policy,
		LocalityPolicyV1::ColumnProjection(_)
	);
	let encode_started = Instant::now();
	let admission =
		validate_table_write_request_batch(meta, batch, descriptor, writer_node_id, row_hlc);
	if observe_encode {
		metrics().observe_pbc_locality_encode(encode_started.elapsed());
	}
	let admission = admission?;
	bifurcate_table_write_with_admission(meta, batch, descriptor, &admission, row_hlc)
}

fn bifurcate_table_write_with_admission(
	meta: &TableWriteRequestMeta,
	batch: &RecordBatch,
	descriptor: &TableSchemaDescriptorV1,
	admission: &TableWriteAdmission,
	row_hlc: HybridTimestamp,
) -> Result<BifurcatedTableWrite, TableWriteValidationError> {
	let mut children = Vec::with_capacity(batch.num_rows());
	let mut charged_bytes = 0_u64;
	match &descriptor.record_layout_policy {
		RecordLayoutPolicy::SingleRow => {
			for row in 0..batch.num_rows() {
				let row_batch = batch.slice(row, 1);
				let encoded =
					encode_batch_with_policy(&row_batch, descriptor.record_body_compression_policy)
						.map_err(|err| TableWriteValidationError {
							code: PbcRejectCode::PBC_REJECT_COLUMN_TYPE_MISMATCH,
							message: format!("failed to encode single-row table payload: {err}"),
						})?;
				let child_charged_bytes = encoded.encoded_byte_len;
				let frame_bytes = Arc::new(encoded.bytes);
				charged_bytes = charged_bytes.saturating_add(child_charged_bytes);
				children.push(BifurcatedTableChildWrite {
					child_ordinal: meta.child_ordinal_base.saturating_add(row as u64),
					key_identity: admission.key_identities[row],
					locality: admission.localities[row],
					record_row_index: RecordRowIndex::ZERO,
					payload_hash: payload_digest(frame_bytes.as_slice()),
					charged_bytes: child_charged_bytes,
					frame_bytes,
				});
			}
		}
		RecordLayoutPolicy::MultiRow(_) => {
			let packed =
				pack_admitted_record_batch_by_bucket(meta, descriptor, batch, &admission, row_hlc)?;
			children.reserve(packed.len());
			for record in packed {
				let frame_bytes = Arc::new(record.frame_bytes);
				let child_charged_bytes = record.charged_bytes;
				charged_bytes = charged_bytes.saturating_add(child_charged_bytes);
				children.push(BifurcatedTableChildWrite {
					child_ordinal: meta
						.child_ordinal_base
						.saturating_add(children.len() as u64),
					key_identity: record.key_identity,
					locality: record.locality,
					record_row_index: RecordRowIndex::ZERO,
					payload_hash: payload_digest(frame_bytes.as_slice()),
					charged_bytes: child_charged_bytes,
					frame_bytes,
				});
			}
		}
	}
	let payload_hash = table_batch_payload_digest(&children);
	let record_count = u32::try_from(children.len()).map_err(|_| TableWriteValidationError {
		code: PbcRejectCode::PBC_REJECT_BATCH_TOO_LARGE,
		message: "table write record count exceeds u32".to_string(),
	})?;
	Ok(BifurcatedTableWrite {
		parent_key_identity: table_write_parent_identity(meta),
		payload_hash,
		children,
		charged_bytes,
		record_count,
		record_body_compression_policy: descriptor.record_body_compression_policy,
	})
}

fn filter_record_batch_rows(
	batch: &RecordBatch,
	rows: &[usize],
) -> Result<RecordBatch, TableWriteValidationError> {
	let indices = rows
		.iter()
		.map(|row| {
			u32::try_from(*row).map_err(|_| TableWriteValidationError {
				code: PbcRejectCode::PBC_REJECT_BATCH_TOO_LARGE,
				message: format!("row index {row} exceeds u32 take index width"),
			})
		})
		.collect::<Result<Vec<_>, _>>()?;
	let indices = UInt32Array::from(indices);
	let columns = batch
		.columns()
		.iter()
		.map(|array| {
			take(array.as_ref(), &indices, None).map_err(|err| TableWriteValidationError {
				code: PbcRejectCode::PBC_REJECT_COLUMN_TYPE_MISMATCH,
				message: format!("content-hash row filter failed: {err}"),
			})
		})
		.collect::<Result<Vec<_>, _>>()?;
	RecordBatch::try_new(batch.schema(), columns).map_err(|err| TableWriteValidationError {
		code: PbcRejectCode::PBC_REJECT_COLUMN_TYPE_MISMATCH,
		message: format!("content-hash filtered RecordBatch invalid: {err}"),
	})
}

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

#[cfg(test)]
fn key_locality(bytes: &[u8]) -> u64 {
	Locality::from(Identity::blake3_tenant(
		crate::schema::TenantId::ZERO,
		bytes,
	))
	.get()
}

fn payload_digest(bytes: &[u8]) -> [u8; 32] {
	crate::hash::content::digest(bytes)
}

fn cdc_heartbeat_identity(locality_id: Locality) -> Identity {
	let mut bytes = [0u8; 32];
	bytes[..8].copy_from_slice(&locality_id.get().to_le_bytes());
	let domain = b"cdc-heartbeat-locality";
	bytes[8..8 + domain.len()].copy_from_slice(domain);
	Identity::new(bytes)
}

fn cdc_heartbeat_payload_hash(
	locality_id: Locality,
	commit_hlc: HybridTimestamp,
	epoch: u64,
) -> [u8; 32] {
	let mut hasher = ::blake3::Hasher::new();
	hasher.update(b"wendelon.cdc.locality-heartbeat.v1");
	hasher.update(&locality_id.get().to_le_bytes());
	hasher.update(&epoch.to_le_bytes());
	hasher.update(&commit_hlc.physical_ms.to_le_bytes());
	hasher.update(&commit_hlc.logical.to_le_bytes());
	hasher.update(&commit_hlc.node_id.to_le_bytes());
	*hasher.finalize().as_bytes()
}

fn format_bytes_hex(bytes: &[u8]) -> String {
	let mut out = String::with_capacity(bytes.len() * 2);
	for byte in bytes {
		use std::fmt::Write as _;
		let _ = write!(&mut out, "{byte:02x}");
	}
	out
}

// ============================================================
//  TESTS
// ============================================================

#[cfg(test)]
mod tests {
	use super::*;
	use crate::crypto::CryptoManager;
	use crate::integration::compute::{
		COMPUTE_STORAGE_CONTRACT_VERSION, SchemaGeneration, SchemaIntentId, SchemaIntentOrigin,
		SchemaMutationKindV1, SchemaObjectKind, SchemaObjectRefV1, SchemaUpdateEnvelopeV1,
		encode_schema_update_envelope_v1,
	};
	use crate::schema::pbc::{
		OP_AND, OP_EQ, OP_GTE, OP_OR, OP_PROJECT, OP_PUSH_COL, OP_PUSH_INT, OP_PUSH_STR,
	};
	use crate::schema::{
		AGG_FORMAT_VERSION, AggregateBudgets, AggregateExpression, AggregateFunctionTag,
		AggregateRequest, AggregateRequestId, AggregateResponse, AggregateStatus, ByteCount,
		ClusteringColumnSpecV1, ColumnLocalityDescriptorV1, DurationMillis, IndexArrowTypeTag,
		IndexKeyEncoding, IndexObjectId, LocalityPolicyV1, LocalityRange, LocationCount,
		MultiRowLayoutDescriptor, PolicyGeneration, PrimaryKeyDescriptorV1, ProjectionFunctionV1,
		RecordLayoutPolicy, RetentionWindowMillis, RowCount, RowIdentityPolicy,
		STORAGE_ROW_HLC_COLUMN, STORAGE_ROW_KEY_ID_COLUMN, STORAGE_ROW_ROUTING_LOCALITY_COLUMN,
		SchemaEvolutionPolicyV1, SecondaryIndexDescriptor, SecondaryIndexKeyDescriptor,
		SecondaryIndexLifecycleComplete, SlabRecordHeader, TableObjectId, TableRetentionPolicy,
		TableSchemaCatalogV1, TableSpreadLocalityDescriptorV1, column_projection_prefix_ranges,
		encode_secondary_index_descriptor, encode_secondary_index_lifecycle_complete,
		encode_table_schema_descriptor_v1, encode_table_write_row_count, table_row_identity,
		table_spread_prefix_ranges, write_payload_schema,
	};
	use crate::storage::core::StorageEngine;
	use crate::storage::io_uring::spawn_io_thread;
	use crate::storage::l2::{
		InjectedMockObjectStoreFault, MockObjectStore, MockObjectStoreFaultAction,
		MockObjectStoreOp, ObjectStore,
	};
	use crate::storage::manager::{SlabManagerConfig, TieredSlabPointer};
	use crate::storage::{IndexQuery, SecondaryIndexCoverageKey, SecondaryIndexIdentity};
	use arrow::array::{Int64Array, LargeStringArray};
	use arrow::datatypes::{DataType, Field};
	use std::io::{Read, Seek, SeekFrom};
	use std::panic::AssertUnwindSafe;
	use std::sync::atomic::{AtomicU64, Ordering};

	fn n(id: u32) -> NodeId {
		NodeId { id, _reserved: 0 }
	}

	struct FixedClock {
		now_ms: u64,
	}

	impl PhysicalClock for FixedClock {
		fn now_ms(&self) -> u64 {
			self.now_ms
		}
	}

	struct AdjustableClock {
		now_ms: AtomicU64,
	}

	impl AdjustableClock {
		fn new(now_ms: u64) -> Self {
			Self {
				now_ms: AtomicU64::new(now_ms),
			}
		}

		fn set(&self, now_ms: u64) {
			self.now_ms.store(now_ms, Ordering::SeqCst);
		}
	}

	impl PhysicalClock for AdjustableClock {
		fn now_ms(&self) -> u64 {
			self.now_ms.load(Ordering::SeqCst)
		}
	}

	fn typed_table_schema() -> Schema {
		Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("name", DataType::LargeUtf8, true),
		])
	}

	fn assert_user_visible_schema_has_no_storage_columns(batch: &RecordBatch) {
		for field in batch.schema().fields() {
			assert!(
				!matches!(
					field.name().as_str(),
					STORAGE_ROW_HLC_COLUMN
						| STORAGE_ROW_KEY_ID_COLUMN
						| STORAGE_ROW_ROUTING_LOCALITY_COLUMN
				),
				"user-visible batch leaked storage synthetic column {}",
				field.name()
			);
		}
	}

	fn typed_table_descriptor() -> TableSchemaDescriptorV1 {
		TableSchemaDescriptorV1::new(
			TableObjectId::new([0x37; 16]),
			3,
			"analytics",
			"events",
			&typed_table_schema(),
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
			LocalityPolicyV1::default_table_spread(),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.unwrap()
	}

	fn multirow_typed_table_descriptor() -> TableSchemaDescriptorV1 {
		let mut descriptor = typed_table_descriptor();
		descriptor.locality_policy =
			LocalityPolicyV1::TableSpreadProjection(TableSpreadLocalityDescriptorV1 {
				spread_factor: 1,
				prefix_bits: 63,
			});
		descriptor.record_layout_policy =
			RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::new(4, 8, 1024 * 1024).unwrap());
		descriptor.record_body_compression_policy =
			RecordBodyCompressionPolicy::default_for_layout(&descriptor.record_layout_policy);
		descriptor.refresh_descriptor_fingerprints().unwrap();
		descriptor.validate().unwrap();
		descriptor
	}

	fn append_only_table_descriptor() -> TableSchemaDescriptorV1 {
		TableSchemaDescriptorV1::new_with_layout(
			TableObjectId::new([0x47; 16]),
			3,
			"analytics",
			"append_events",
			&typed_table_schema(),
			RowIdentityPolicy::AppendOnly,
			LocalityPolicyV1::default_table_spread(),
			RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::new(4, 8, 1024 * 1024).unwrap()),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.unwrap()
	}

	fn content_hash_table_descriptor() -> TableSchemaDescriptorV1 {
		TableSchemaDescriptorV1::new_with_layout(
			TableObjectId::new([0x49; 16]),
			3,
			"analytics",
			"content_events",
			&typed_table_schema(),
			RowIdentityPolicy::AppendOnlyContentHash,
			LocalityPolicyV1::IdentityProjection,
			RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::new(4, 8, 1024 * 1024).unwrap()),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.unwrap()
	}

	fn identity_typed_table_descriptor(id_byte: u8) -> TableSchemaDescriptorV1 {
		TableSchemaDescriptorV1::new(
			TableObjectId::new([id_byte; 16]),
			3,
			"analytics",
			"identity_events",
			&typed_table_schema(),
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
			LocalityPolicyV1::IdentityProjection,
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.unwrap()
	}

	fn column_projection_table_descriptor() -> TableSchemaDescriptorV1 {
		TableSchemaDescriptorV1::new(
			TableObjectId::new([0x38; 16]),
			3,
			"analytics",
			"events",
			&typed_table_schema(),
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
			LocalityPolicyV1::ColumnProjection(ColumnLocalityDescriptorV1 {
				columns: vec![ClusteringColumnSpecV1 {
					name: "id".to_owned(),
					width_bits: 16,
					string_prefix_bytes: None,
				}],
				projection: ProjectionFunctionV1::ZOrder,
				spread_factor: 4,
				prefix_bits: 16,
			}),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.unwrap()
	}

	fn typed_table_batch(ids: Vec<i64>) -> RecordBatch {
		let names: Vec<&str> = ids
			.iter()
			.enumerate()
			.map(|(idx, _)| match idx {
				0 => "codex",
				1 => "claude",
				_ => "gemini",
			})
			.collect();
		RecordBatch::try_new(
			Arc::new(typed_table_schema()),
			vec![
				Arc::new(Int64Array::from(ids)),
				Arc::new(LargeStringArray::from(names)),
			],
		)
		.unwrap()
	}

	fn typed_table_batch_with_names(rows: Vec<(i64, &str)>) -> RecordBatch {
		let ids = rows.iter().map(|(id, _)| *id).collect::<Vec<_>>();
		let names = rows.iter().map(|(_, name)| *name).collect::<Vec<_>>();
		RecordBatch::try_new(
			Arc::new(typed_table_schema()),
			vec![
				Arc::new(Int64Array::from(ids)),
				Arc::new(LargeStringArray::from(names)),
			],
		)
		.unwrap()
	}

	fn same_multirow_locality_ids(descriptor: &TableSchemaDescriptorV1, count: usize) -> Vec<i64> {
		let mut buckets: HashMap<Locality, Vec<i64>> = HashMap::new();
		for id in 0_i64..10_000 {
			let batch = typed_table_batch(vec![id]);
			let meta = typed_table_meta(descriptor, 1);
			let admission = validate_table_write_request_batch(
				&meta,
				&batch,
				descriptor,
				WriterNodeId::ZERO,
				meta.client_hlc,
			)
			.unwrap();
			let bucket = admission.localities[0];
			let ids = buckets.entry(bucket).or_default();
			ids.push(id);
			if ids.len() == count {
				return ids.clone();
			}
		}
		panic!("failed to find {count} ids with shared test locality");
	}

	fn typed_table_meta(
		descriptor: &TableSchemaDescriptorV1,
		row_count: usize,
	) -> TableWriteRequestMeta {
		TableWriteRequestMeta {
			parent_write_id: crate::schema::ClientWriteId {
				client_uuid: [0x44; 16],
				sequence_no: 55,
				idempotency_ttl_ms: 30_000,
				_pad: 0,
			},
			client_hlc: hlc(1, 0, 1),
			expected_epoch: 1,
			tenant_id: tenant(),
			table_object_id: descriptor.table_object_id,
			schema_generation: descriptor.schema_generation,
			child_ordinal_base: 1_000,
			proxy_source: n(0),
			row_count: encode_table_write_row_count(row_count).unwrap(),
			proxy_hop: 0,
			durability: DurabilityLevel::QUORUM,
			await_finalization: 0,
			_pad: [0; 3],
		}
	}

	fn typed_table_schema_update(descriptor: &TableSchemaDescriptorV1) -> Vec<u8> {
		let payload = encode_table_schema_descriptor_v1(descriptor).unwrap();
		let envelope = SchemaUpdateEnvelopeV1 {
			contract_version: COMPUTE_STORAGE_CONTRACT_VERSION,
			origin: SchemaIntentOrigin::ComputeClient,
			tenant_id: tenant(),
			object: SchemaObjectRefV1 {
				kind: SchemaObjectKind::Table,
				namespace: descriptor.namespace.clone(),
				name: descriptor.table_name.clone(),
			},
			expected_generation: SchemaGeneration::ZERO,
			requested_generation: SchemaGeneration::new(descriptor.schema_generation),
			intent_id: SchemaIntentId::new([0x37; 16]),
			mutation: SchemaMutationKindV1::Create,
			payload,
		};
		encode_schema_update_envelope_v1(&envelope).unwrap()
	}

	fn name_secondary_index_descriptor(
		descriptor: &TableSchemaDescriptorV1,
	) -> SecondaryIndexDescriptor {
		SecondaryIndexDescriptor {
			index_object_id: IndexObjectId::new([0x52; 16]),
			table_object_id: descriptor.table_object_id,
			namespace: descriptor.namespace.clone(),
			index_name: "events_name_idx".to_owned(),
			table_schema_generation: descriptor.schema_generation,
			logical_schema_fingerprint: descriptor.logical_schema_fingerprint,
			key: SecondaryIndexKeyDescriptor {
				column_name: "name".to_owned(),
				column_ordinal: 1,
				arrow_type_tag: IndexArrowTypeTag::LargeUtf8,
				encoding: IndexKeyEncoding::OrderedScalar,
			},
		}
	}

	fn secondary_index_schema_update(descriptor: &SecondaryIndexDescriptor) -> Vec<u8> {
		let payload = encode_secondary_index_descriptor(descriptor).unwrap();
		let envelope = SchemaUpdateEnvelopeV1 {
			contract_version: COMPUTE_STORAGE_CONTRACT_VERSION,
			origin: SchemaIntentOrigin::ComputeClient,
			tenant_id: tenant(),
			object: SchemaObjectRefV1 {
				kind: SchemaObjectKind::Index,
				namespace: descriptor.namespace.clone(),
				name: descriptor.index_name.clone(),
			},
			expected_generation: SchemaGeneration::ZERO,
			requested_generation: SchemaGeneration::new(1),
			intent_id: SchemaIntentId::new([0x52; 16]),
			mutation: SchemaMutationKindV1::Create,
			payload,
		};
		encode_schema_update_envelope_v1(&envelope).unwrap()
	}

	fn secondary_index_backfill_complete_schema_update(
		descriptor: &SecondaryIndexDescriptor,
		replica_id: NodeId,
	) -> Vec<u8> {
		let payload = encode_secondary_index_lifecycle_complete(&SecondaryIndexLifecycleComplete {
			index_object_id: descriptor.index_object_id,
			replica_id,
		})
		.unwrap();
		let envelope = SchemaUpdateEnvelopeV1 {
			contract_version: COMPUTE_STORAGE_CONTRACT_VERSION,
			origin: SchemaIntentOrigin::WendelonOperator,
			tenant_id: tenant(),
			object: SchemaObjectRefV1 {
				kind: SchemaObjectKind::Index,
				namespace: descriptor.namespace.clone(),
				name: descriptor.index_name.clone(),
			},
			expected_generation: SchemaGeneration::new(1),
			requested_generation: SchemaGeneration::new(2),
			intent_id: SchemaIntentId::new([0x53; 16]),
			mutation: SchemaMutationKindV1::IndexBackfillComplete,
			payload,
		};
		encode_schema_update_envelope_v1(&envelope).unwrap()
	}

	fn aggregate_count_request(
		descriptor: &TableSchemaDescriptorV1,
		request_id: u128,
		as_of_hlc: HybridTimestamp,
	) -> AggregateRequest {
		AggregateRequest {
			format_version: AGG_FORMAT_VERSION,
			request_id: AggregateRequestId::new(request_id),
			tenant_id: tenant(),
			table_object_id: descriptor.table_object_id,
			schema_generation: descriptor.schema_generation,
			logical_schema_fingerprint: descriptor.logical_schema_fingerprint,
			consistency: Consistency::SERIALIZABLE,
			min_hlc: hlc(1, 0, 1),
			as_of_hlc,
			locality_ranges: vec![LocalityRange::full()],
			predicate_program: None,
			aggregates: vec![AggregateExpression::count_star()],
			budgets: AggregateBudgets {
				max_locations: LocationCount::new(16),
				max_rows: RowCount::new(16),
				max_wall_time_millis: DurationMillis::new(1_000),
				max_partial_state_bytes: ByteCount::new(4_096),
			},
		}
	}

	fn table_query_meta_for_test(
		descriptor: &TableSchemaDescriptorV1,
		as_of_hlc: HybridTimestamp,
		bytecode_len: usize,
	) -> TableQueryRequestMeta {
		TableQueryRequestMeta {
			min_hlc: hlc(0, 0, 0),
			expected_epoch: 1,
			tenant_id: tenant(),
			table_object_id: descriptor.table_object_id,
			schema_generation: descriptor.schema_generation,
			consistency: Consistency::SERIALIZABLE,
			_pad: [0; 7],
			bytecode_len: bytecode_len as u32,
			_pad2: [0; 4],
			as_of_hlc,
		}
	}

	fn pk_eq_program(pk: i64) -> Vec<u8> {
		let mut bytecode = Vec::new();
		bytecode.push(OP_PUSH_COL);
		bytecode.extend_from_slice(&0_u32.to_le_bytes());
		bytecode.push(OP_PUSH_INT);
		bytecode.extend_from_slice(&pk.to_le_bytes());
		bytecode.push(OP_EQ);
		bytecode
	}

	fn name_eq_program(name: &str) -> Vec<u8> {
		let mut bytecode = Vec::new();
		bytecode.push(OP_PUSH_COL);
		bytecode.extend_from_slice(&1_u32.to_le_bytes());
		bytecode.push(OP_PUSH_STR);
		bytecode.extend_from_slice(&(name.len() as u16).to_le_bytes());
		bytecode.extend_from_slice(name.as_bytes());
		bytecode.push(OP_EQ);
		bytecode
	}

	fn count_star_partial_value(response: &AggregateResponse) -> u64 {
		u64::from_be_bytes(
			response.partials[0]
				.state
				.as_slice()
				.try_into()
				.expect("count star partial is u64"),
		)
	}

	async fn aggregate_csb_test_server(
		mgr: Arc<SlabManager>,
		descriptor: &TableSchemaDescriptorV1,
		node_id: u32,
		temp_tag: &str,
		clock: Arc<dyn PhysicalClock>,
		retention_policy: MvccRetentionPolicy,
	) -> (WendelonNode, WendelonDataServer) {
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			node_id,
			bootstrap_tempdir(temp_tag),
			format!("loopback://{node_id}"),
		))
		.await
		.unwrap();
		let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
		loop {
			if node.raft.current_leader().await == Some(node.local_node.id) {
				break;
			}
			assert!(
				tokio::time::Instant::now() < deadline,
				"timed out waiting for single-node leader"
			);
			tokio::time::sleep(Duration::from_millis(20)).await;
		}
		node.raft
			.client_write(WendelonRequest::SchemaUpdate(typed_table_schema_update(
				descriptor,
			)))
			.await
			.unwrap();

		let tiers = TransportTierSet::empty();
		let roles = NodeRoleSet::empty().with(NodeRole::Data);
		let mut peers = std::collections::HashMap::new();
		peers.insert(
			node.local_node,
			PeerCapabilities {
				address: format!("loopback://{node_id}"),
				tiers,
				roles,
				capabilities: crate::network::transport::NodeCapabilitySet::empty()
					.with(NodeCapability::PartialAggregatePushdown),
			},
		);
		let epoch = MembershipEpoch { epoch: 1, peers };
		let (_epoch_tx, epoch_rx) = watch::channel(Arc::new(epoch));
		let resolver: Arc<dyn crate::network::transport::CapabilityResolver> = Arc::new(
			crate::network::transport::EpochCapabilityResolver::new(epoch_rx.clone()),
		);
		let factory = TransportProviderFactory::new(
			tiers,
			std::collections::HashMap::new(),
			resolver,
			epoch_rx,
		);
		let server = WendelonDataServer::new(
			mgr,
			node.raft.clone(),
			node.log_store.clone(),
			node.state_machine.clone(),
			node.local_node,
			clock,
			Arc::new(FixedReplicaRouter::new(vec![node.local_node])),
			factory,
			node.pool.clone(),
		)
		.unwrap()
		.with_mvcc_retention_policy(retention_policy);

		(node, server)
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

	#[test]
	fn pending_finalization_claims_are_exclusive_until_drop() {
		let claims = PendingFinalizationClaims::default();
		let write_id = crate::schema::ClientWriteId {
			client_uuid: [9; 16],
			sequence_no: 42,
			idempotency_ttl_ms: 60_000,
			_pad: 0,
		};
		let finalization = TenantFinalization {
			marker: FinalizationMarker {
				log_index: 0,
				term: 0,
				epoch: 1,
				commit_hlc: HybridTimestamp {
					physical_ms: 123,
					logical: 4,
					node_id: 1,
				},
				payload_hash: [0xAB; 32],
				e_safe_index: 0,
				durability: DurabilityLevel::QUORUM,
				_pad: [0; 7],
			},
			tenant_id: TenantId::ZERO,
			key_identity: Identity::blake3_tenant(TenantId::ZERO, b"claim-key"),
			charged_bytes: 128,
			record_count: 1,
		};
		let id = PendingFinalizationId::for_record(write_id, &finalization);

		let first = claims.try_claim(id).expect("first claim should succeed");
		assert!(
			claims.try_claim(id).is_none(),
			"the same pending finalization must not be processed twice concurrently"
		);
		drop(first);
		assert!(
			claims.try_claim(id).is_some(),
			"dropping the active claim should release the pending finalization"
		);
	}

	fn hlc(physical_ms: u64, logical: u32, node_id: u32) -> HybridTimestamp {
		HybridTimestamp {
			physical_ms,
			logical,
			node_id,
		}
	}

	#[test]
	fn cdc_retention_reject_code_fires_when_cursor_falls_below_floor() {
		let effective = EffectiveRetention {
			policy: TableRetentionPolicy::FixedWindow(
				RetentionWindowMillis::try_new(60_000).unwrap(),
			),
			floor: Some(hlc(100, 0, 0)),
		};

		assert_eq!(
			cdc_retention_reject_code_for_cursor(
				CdcCursor::new(hlc(99, 0, 0), Identity::ZERO, RecordRowIndex::ZERO),
				&effective,
			),
			Some(CdcRejectCode::BelowRetentionFloor)
		);
		assert_eq!(
			cdc_retention_reject_code_for_cursor(
				CdcCursor::new(hlc(100, 0, 0), Identity::ZERO, RecordRowIndex::ZERO),
				&effective,
			),
			None
		);
		assert_eq!(
			cdc_retention_reject_code_for_cursor(CdcCursor::MIN, &effective),
			None
		);
	}

	#[test]
	fn cdc_heartbeat_identity_projects_to_source_locality() {
		for locality_id in [
			Locality::ZERO,
			Locality::new(42),
			Locality::new(0xAABB_CCDD_EEFF_0011),
			Locality::MAX,
		] {
			assert_eq!(
				Locality::from(cdc_heartbeat_identity(locality_id)),
				locality_id
			);
		}
	}

	#[test]
	fn cdc_heartbeat_payload_hash_is_locality_and_hlc_scoped() {
		let base = cdc_heartbeat_payload_hash(Locality::new(7), hlc(100, 0, 1), 3);
		assert_ne!(
			base,
			cdc_heartbeat_payload_hash(Locality::new(8), hlc(100, 0, 1), 3)
		);
		assert_ne!(
			base,
			cdc_heartbeat_payload_hash(Locality::new(7), hlc(101, 0, 1), 3)
		);
		assert_ne!(
			base,
			cdc_heartbeat_payload_hash(Locality::new(7), hlc(100, 0, 1), 4)
		);
	}

	#[test]
	fn cdc_checkpoint_cadence_fires_on_event_count_and_wall_time() {
		let start = Instant::now();
		let mut cadence = CdcCheckpointCadence::new(start);
		cadence.note_events(CDC_CHECKPOINT_EVENT_CADENCE - 1);
		assert!(!cadence.checkpoint_due(start));
		cadence.note_events(1);
		assert!(cadence.checkpoint_due(start));
		cadence.mark_checkpoint(start);
		assert!(!cadence.checkpoint_due(start));
		assert!(cadence.checkpoint_due(start + CDC_CHECKPOINT_WALL_CADENCE));
	}

	#[test]
	fn cdc_finalization_key_ignores_applied_log_coordinates() {
		let pending_marker = FinalizationMarker {
			log_index: 0,
			term: 0,
			epoch: 7,
			commit_hlc: hlc(900, 1, 2),
			payload_hash: [0x5A; 32],
			e_safe_index: 0,
			durability: DurabilityLevel::QUORUM,
			_pad: [0; 7],
		};
		let applied_marker = FinalizationMarker {
			log_index: 42,
			term: 11,
			e_safe_index: 42,
			..pending_marker
		};
		assert_eq!(
			CdcFinalizationKey::from_marker(pending_marker),
			CdcFinalizationKey::from_marker(applied_marker)
		);

		let mut different_payload = applied_marker;
		different_payload.payload_hash = [0xA5; 32];
		assert_ne!(
			CdcFinalizationKey::from_marker(applied_marker),
			CdcFinalizationKey::from_marker(different_payload)
		);
	}

	#[test]
	fn cdc_audit_helpers_pin_lifecycle_reason_and_cursor_shape() {
		assert_eq!(
			cdc_reject_audit_reason(CdcRejectCode::BelowRetentionFloor),
			"reject_below_retention_floor"
		);
		assert_eq!(cdc_bound_audit_str(CdcSubscribeBound::Live), "live");

		let cursor = CdcCursor::new(hlc(123, 4, 5), Identity::ZERO, RecordRowIndex::new(7));
		let value = cdc_cursor_audit_json(cursor);
		assert_eq!(value["row_hlc"]["physical_ms"], 123);
		assert_eq!(value["row_hlc"]["logical"], 4);
		assert_eq!(value["row_hlc"]["node_id"], 5);
		assert_eq!(value["record_row_index"], 7);
		assert_eq!(
			value["row_key_id"],
			"0000000000000000000000000000000000000000000000000000000000000000"
		);
	}

	#[test]
	fn cdc_source_localities_include_cold_exact_ranges_without_rings() {
		let ranges = [
			QueryScanRange {
				start: Locality::new(100),
				end: Locality::new(100),
			},
			QueryScanRange {
				start: Locality::new(200),
				end: Locality::new(202),
			},
		];

		assert_eq!(
			cdc_source_localities_from_ranges_and_rings(&ranges, [Locality::new(101)]),
			vec![
				Locality::new(100),
				Locality::new(200),
				Locality::new(201),
				Locality::new(202),
			]
		);
	}

	#[test]
	fn cdc_source_localities_keep_wide_ranges_bounded_and_include_live_rings() {
		let ranges = [QueryScanRange {
			start: Locality::new(10),
			end: Locality::new(10 + CDC_SOURCE_LOCALITY_ENUMERATION_BUDGET + 10),
		}];

		assert_eq!(
			cdc_source_localities_from_ranges_and_rings(
				&ranges,
				[Locality::new(11), Locality::new(999_999)],
			),
			vec![
				Locality::new(10),
				Locality::new(11),
				Locality::new(10 + CDC_SOURCE_LOCALITY_ENUMERATION_BUDGET + 10),
			]
		);
	}

	#[test]
	fn cdc_heartbeat_primary_ranges_use_bounded_locality_set() {
		assert_eq!(
			cdc_heartbeat_localities_for_primary_ranges([
				(100, 102),
				(1_000, 1_000 + CDC_SOURCE_LOCALITY_ENUMERATION_BUDGET + 10),
			]),
			vec![
				Locality::new(100),
				Locality::new(101),
				Locality::new(102),
				Locality::new(1_000),
				Locality::new(1_000 + CDC_SOURCE_LOCALITY_ENUMERATION_BUDGET + 10),
			]
		);
	}

	#[test]
	fn cdc_replicated_here_rejects_only_when_bounded_ranges_exclude_local_node() {
		let local = n(7);
		let remote = n(8);
		let ranges = [QueryScanRange {
			start: Locality::new(20),
			end: Locality::new(22),
		}];

		assert!(!cdc_ranges_maybe_replicated_by_local_node(
			&ranges,
			local,
			|_| vec![remote]
		));
		assert!(cdc_ranges_maybe_replicated_by_local_node(
			&ranges,
			local,
			|locality| if locality == Locality::new(21) {
				vec![remote, local]
			} else {
				vec![remote]
			}
		));
	}

	#[test]
	fn cdc_replicated_here_accepts_wide_ranges_conservatively() {
		let local = n(7);
		let remote = n(8);
		let ranges = [QueryScanRange {
			start: Locality::new(20),
			end: Locality::new(20 + CDC_SOURCE_LOCALITY_ENUMERATION_BUDGET + 1),
		}];

		assert!(cdc_ranges_maybe_replicated_by_local_node(
			&ranges,
			local,
			|_| vec![remote]
		));
	}

	#[tokio::test]
	async fn cdc_live_recovery_upper_is_table_scoped_and_safe_gated() {
		let dir = bootstrap_tempdir("cdc-live-recovery-upper");
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			46,
			dir,
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();
		let locality = Locality::new(10);
		let descriptor = typed_table_descriptor();
		let target_table = descriptor.table_object_id;
		let other_table = TableObjectId::new([0x99; 16]);
		let ranges = [QueryScanRange {
			start: locality,
			end: locality,
		}];
		let ring = Arc::new(CdcLiveTailRing::new(locality, 1));
		node.data_server
			.cdc_live_tail_rings
			.insert(locality, ring.clone());

		ring.push(CdcLiveEntry::new(
			CdcCursor::new(
				hlc(80, 0, 46),
				Identity::new([0x80; 32]),
				RecordRowIndex::ZERO,
			),
			tenant(),
			other_table,
			descriptor.logical_schema_fingerprint,
			Arc::new(typed_table_batch(vec![80])),
		))
		.await;
		assert_eq!(
			node.data_server
				.cdc_live_recovery_upper_after(
					tenant(),
					target_table,
					&ranges,
					CdcCursor::MIN,
					hlc(100, 0, 46),
				)
				.await,
			None,
			"other-table ring overflow must not force target-table recovery"
		);

		let target_cursor = CdcCursor::new(
			hlc(90, 0, 46),
			Identity::new([0x90; 32]),
			RecordRowIndex::ZERO,
		);
		ring.push(CdcLiveEntry::new(
			target_cursor,
			tenant(),
			target_table,
			descriptor.logical_schema_fingerprint,
			Arc::new(typed_table_batch(vec![90])),
		))
		.await;
		assert_eq!(
			node.data_server
				.cdc_live_recovery_upper_after(
					tenant(),
					target_table,
					&ranges,
					CdcCursor::ceiling_for_hlc(hlc(80, 0, 46)),
					hlc(50, 0, 46),
				)
				.await,
			None,
			"recovery must not emit beyond the safe-emission watermark"
		);
		assert_eq!(
			node.data_server
				.cdc_live_recovery_upper_after(
					tenant(),
					target_table,
					&ranges,
					CdcCursor::ceiling_for_hlc(hlc(80, 0, 46)),
					hlc(100, 0, 46),
				)
				.await,
			Some(target_cursor)
		);

		node.handles.abort();
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

	fn wide_kv_batch(row_count: usize) -> RecordBatch {
		let schema = Arc::new(write_payload_schema());
		let mut keys = LargeBinaryBuilder::new();
		let mut values = LargeBinaryBuilder::new();
		for row in 0..row_count {
			keys.append_value(format!("k{row}").as_bytes());
			values.append_value(format!("v{row}").as_bytes());
		}
		RecordBatch::try_new(
			schema,
			vec![Arc::new(keys.finish()), Arc::new(values.finish())],
		)
		.unwrap()
	}

	fn id(locality: u64) -> Identity {
		let mut bytes = [0u8; 32];
		bytes[..8].copy_from_slice(&locality.to_le_bytes());
		Identity::new(bytes)
	}

	fn tenant() -> TenantId {
		TenantId::ZERO
	}

	fn test_crypto() -> Arc<CryptoManager> {
		let crypto = Arc::new(CryptoManager::new_dev());
		crypto.install_local_only_for_tests(tenant()).unwrap();
		crypto
	}

	struct OrphanWitnessFixture {
		leader_node: WendelonNode,
		follower_node: WendelonNode,
		leader: Arc<WendelonDataServer>,
		follower: Arc<WendelonDataServer>,
		epoch_tx: watch::Sender<Arc<MembershipEpoch>>,
	}

	async fn build_orphan_witness_fixture(tag: &str) -> OrphanWitnessFixture {
		let leader_node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			26,
			bootstrap_tempdir(&format!("{tag}-leader")),
			"loopback://26".into(),
		))
		.await
		.unwrap();
		let follower_node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			27,
			bootstrap_tempdir(&format!("{tag}-follower")),
			"loopback://27".into(),
		))
		.await
		.unwrap();

		let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
		loop {
			if leader_node.raft.current_leader().await == Some(leader_node.local_node.id)
				&& follower_node.raft.current_leader().await == Some(follower_node.local_node.id)
			{
				break;
			}
			assert!(
				tokio::time::Instant::now() < deadline,
				"timed out waiting for single-node leaders"
			);
			tokio::time::sleep(Duration::from_millis(20)).await;
		}

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
			leader_node.local_node,
			PeerCapabilities {
				address: "loopback://26".into(),
				tiers,
				roles,
				capabilities: crate::network::transport::NodeCapabilitySet::empty(),
			},
		);
		peers.insert(
			follower_node.local_node,
			PeerCapabilities {
				address: "loopback://27".into(),
				tiers,
				roles,
				capabilities: crate::network::transport::NodeCapabilitySet::empty(),
			},
		);
		let epoch = MembershipEpoch { epoch: 2, peers };
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
		let router: Arc<dyn ReplicaRouter> = Arc::new(FixedReplicaRouter::new(vec![
			leader_node.local_node,
			follower_node.local_node,
		]));

		let leader = Arc::new(
			WendelonDataServer::new(
				leader_node.slab.clone(),
				leader_node.raft.clone(),
				leader_node.log_store.clone(),
				leader_node.state_machine.clone(),
				leader_node.local_node,
				leader_node.clock.clone(),
				router.clone(),
				build_factory(),
				pool.clone(),
			)
			.unwrap(),
		);
		let follower = Arc::new(
			WendelonDataServer::new(
				follower_node.slab.clone(),
				follower_node.raft.clone(),
				follower_node.log_store.clone(),
				follower_node.state_machine.clone(),
				follower_node.local_node,
				follower_node.clock.clone(),
				router,
				build_factory(),
				follower_node.pool.clone(),
			)
			.unwrap(),
		);

		let follower_data_ring: Arc<dyn DataRingServer> = follower.clone();
		let follower_service: Arc<dyn HrftService> = Arc::new(
			MsgRouter::builder()
				.with_data_ring(follower_data_ring)
				.build(),
		);
		handlers.register(follower_node.local_node, follower_service);

		OrphanWitnessFixture {
			leader_node,
			follower_node,
			leader,
			follower,
			epoch_tx,
		}
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
				rehydration: crate::storage::manager::RehydrationPolicy::default(),
				l2_column_fetch: Default::default(),
				group_commit: crate::storage::GroupCommitConfig::default(),
				crypto: test_crypto(),
			})
			.await
			.unwrap(),
		)
	}

	async fn make_manager_with_l2(l2: Arc<dyn ObjectStore>) -> Arc<SlabManager> {
		let dir = fresh_dir("query-l2");
		let io = spawn_io_thread(64).unwrap();
		let mut rehydration = crate::storage::manager::RehydrationPolicy::default();
		rehydration.promote_after_l2_reads = 0;
		rehydration.tail_read_window_bytes = 256 + 16;
		Arc::new(
			SlabManager::open(SlabManagerConfig {
				dir,
				max_slab_bytes: 1 << 30,
				io,
				l2: Some(l2),
				rehydration,
				l2_column_fetch: Default::default(),
				group_commit: crate::storage::GroupCommitConfig::default(),
				crypto: test_crypto(),
			})
			.await
			.unwrap(),
		)
	}

	async fn make_manager_with_l2_projected_fetch(l2: Arc<dyn ObjectStore>) -> Arc<SlabManager> {
		let dir = fresh_dir("query-l2-projected");
		let io = spawn_io_thread(64).unwrap();
		let mut rehydration = crate::storage::manager::RehydrationPolicy::default();
		rehydration.promote_after_l2_reads = 0;
		rehydration.tail_read_window_bytes = 256 + 16;
		Arc::new(
			SlabManager::open(SlabManagerConfig {
				dir,
				max_slab_bytes: 1 << 30,
				io,
				l2: Some(l2),
				rehydration,
				l2_column_fetch: crate::storage::columnar::L2ColumnFetchConfig {
					enabled: true,
					min_record_bytes: crate::schema::RecordBodyByteLen::try_new(1).unwrap(),
					..Default::default()
				},
				group_commit: crate::storage::GroupCommitConfig::default(),
				crypto: test_crypto(),
			})
			.await
			.unwrap(),
		)
	}

	async fn append_typed_table_rows(
		mgr: &SlabManager,
		descriptor: &TableSchemaDescriptorV1,
		ids: Vec<i64>,
	) -> SlabId {
		let batch = typed_table_batch(ids);
		let mut write_meta: TableWriteRequestMeta = bytemuck::Zeroable::zeroed();
		write_meta.tenant_id = tenant();
		write_meta.table_object_id = descriptor.table_object_id;
		write_meta.schema_generation = descriptor.schema_generation;
		write_meta.row_count = encode_table_write_row_count(batch.num_rows()).unwrap();
		let admission = validate_table_write_request_batch(
			&write_meta,
			&batch,
			&descriptor,
			WriterNodeId::ZERO,
			write_meta.client_hlc,
		)
		.expect("typed rows should validate");
		for row in 0..batch.num_rows() {
			let row_batch = batch.slice(row, 1);
			let frame = encode_batch(&row_batch).unwrap();
			mgr.append_durable_with_locality(
				tenant(),
				admission.key_identities[row],
				admission.localities[row],
				hlc(10 + row as u64, 0, 1),
				&frame,
			)
			.await
			.unwrap();
		}
		mgr.force_seal_active_for_test().await
	}

	async fn append_append_only_rows_for_query(
		mgr: &SlabManager,
		descriptor: &TableSchemaDescriptorV1,
		ids: Vec<i64>,
		row_hlc: HybridTimestamp,
	) -> Vec<Identity> {
		let batch = typed_table_batch(ids);
		let mut write_meta = typed_table_meta(descriptor, batch.num_rows());
		write_meta.client_hlc = row_hlc;
		let admission = validate_table_write_request_batch(
			&write_meta,
			&batch,
			descriptor,
			WriterNodeId::ZERO,
			row_hlc,
		)
		.expect("append-only rows should validate");
		let row_key_ids = admission.key_identities.clone();
		let bifurcated =
			bifurcate_table_write(&write_meta, &batch, descriptor, WriterNodeId::ZERO, row_hlc)
				.expect("append-only rows should bifurcate");
		append_bifurcated_table_children_durable(
			mgr,
			write_meta.tenant_id,
			row_hlc,
			&bifurcated.children,
			&[],
		)
		.await
		.unwrap();
		row_key_ids
	}

	async fn append_append_only_tombstones_for_query(
		mgr: &SlabManager,
		descriptor: &TableSchemaDescriptorV1,
		row_key_ids: &[Identity],
		delete_hlc: HybridTimestamp,
	) {
		let layout = match descriptor.record_layout_policy {
			RecordLayoutPolicy::MultiRow(layout) => layout,
			RecordLayoutPolicy::SingleRow => {
				panic!("append-only tombstones require multi-row layout")
			}
		};
		let rows = row_key_ids
			.iter()
			.enumerate()
			.map(|(source_row_index, row_key_id)| MultiRowSourceRow {
				source_row_index,
				key_identity: *row_key_id,
				routing_locality: mgr
					.routing_locality_for_existing_key(tenant(), *row_key_id)
					.expect("append-only test row must have cached routing locality"),
				row_hlc: delete_hlc,
				is_tombstone: true,
				table_object_id: Some(descriptor.table_object_id),
			})
			.collect::<Vec<_>>();
		let packed = pack_tombstone_multirow_source_rows_with_policy(
			layout,
			&rows,
			descriptor.record_body_compression_policy,
		)
		.expect("row tombstones should pack");
		for record in packed {
			let decoded = decode_batch(&record.encoded.frame).unwrap();
			let entries = storage_index_entries_from_batch(&decoded).unwrap();
			mgr.append_durable_multirow_with_locality(tenant(), &record.encoded.frame, &entries)
				.await
				.unwrap();
		}
	}

	async fn tier_active_slab_with_stats_for_query(
		mgr: &SlabManager,
		l2: Arc<MockObjectStore>,
		descriptor: &TableSchemaDescriptorV1,
		object_name: &str,
	) -> SlabId {
		let slab_id = mgr.force_seal_active_for_test().await;
		let raw = mgr.read_sealed_raw(slab_id).await.unwrap();
		let mut catalog = TableSchemaCatalogV1::default();
		catalog.upsert(tenant(), descriptor.clone()).unwrap();
		let encrypted = mgr
			.encrypt_tiered_object_blob_with_table_stats(slab_id, &raw, &catalog)
			.unwrap();
		let locator = encrypted.stats_locator.expect("stats locator");
		let l2_uri = l2.put(object_name, &encrypted.bytes).await.unwrap();
		mgr.apply_tier_slab(TieredSlabPointer {
			slab_id,
			l2_uri,
			payload_hash: payload_digest(&encrypted.bytes),
			is_encrypted: true,
			tenant_id: Some(encrypted.tenant_id),
			tenant_kek_id: Some(encrypted.tenant_kek_id),
			stats_format_version: Some(locator.stats_format_version),
			stats_block_len: Some(locator.stats_block_len),
			stats_block_hash128: Some(locator.stats_block_hash128),
			tombstone_metadata: TombstoneMetadata::None,
			supersession_metadata: SupersessionMetadata::None,
		})
		.await
		.unwrap();
		slab_id
	}

	fn sorted_i64_column_values(batches: &[RecordBatch], column: &str) -> Vec<i64> {
		let mut values = Vec::new();
		for batch in batches {
			let ids = batch
				.column_by_name(column)
				.unwrap()
				.as_any()
				.downcast_ref::<Int64Array>()
				.unwrap();
			values.extend(ids.values().iter().copied());
		}
		values.sort_unstable();
		values
	}

	async fn tier_typed_table_batch_with_stats(
		l2: Arc<MockObjectStore>,
		ids: Vec<i64>,
		object_name: &str,
	) -> (Arc<SlabManager>, TableSchemaDescriptorV1) {
		let mgr = make_manager_with_l2(l2.clone()).await;
		let descriptor = typed_table_descriptor();
		let slab_id = append_typed_table_rows(&mgr, &descriptor, ids).await;

		let raw = mgr.read_sealed_raw(slab_id).await.unwrap();
		let mut catalog = TableSchemaCatalogV1::default();
		catalog.upsert(tenant(), descriptor.clone()).unwrap();
		let encrypted = mgr
			.encrypt_tiered_object_blob_with_table_stats(slab_id, &raw, &catalog)
			.unwrap();
		let locator = encrypted.stats_locator.expect("stats locator");
		let l2_uri = l2.put(object_name, &encrypted.bytes).await.unwrap();
		mgr.apply_tier_slab(TieredSlabPointer {
			slab_id,
			l2_uri,
			payload_hash: payload_digest(&encrypted.bytes),
			is_encrypted: true,
			tenant_id: Some(encrypted.tenant_id),
			tenant_kek_id: Some(encrypted.tenant_kek_id),
			stats_format_version: Some(locator.stats_format_version),
			stats_block_len: Some(locator.stats_block_len),
			stats_block_hash128: Some(locator.stats_block_hash128),
			tombstone_metadata: TombstoneMetadata::None,
			supersession_metadata: SupersessionMetadata::None,
		})
		.await
		.unwrap();

		(mgr, descriptor)
	}

	async fn tier_identity_typed_table_batch_with_stats(
		l2: Arc<MockObjectStore>,
		ids: Vec<i64>,
		object_name: &str,
	) -> (Arc<SlabManager>, TableSchemaDescriptorV1) {
		let mgr = make_manager_with_l2(l2.clone()).await;
		let descriptor = identity_typed_table_descriptor(0x41);
		let slab_id = append_typed_table_rows(&mgr, &descriptor, ids).await;

		let raw = mgr.read_sealed_raw(slab_id).await.unwrap();
		let mut catalog = TableSchemaCatalogV1::default();
		catalog.upsert(tenant(), descriptor.clone()).unwrap();
		let encrypted = mgr
			.encrypt_tiered_object_blob_with_table_stats(slab_id, &raw, &catalog)
			.unwrap();
		let locator = encrypted.stats_locator.expect("stats locator");
		let l2_uri = l2.put(object_name, &encrypted.bytes).await.unwrap();
		mgr.apply_tier_slab(TieredSlabPointer {
			slab_id,
			l2_uri,
			payload_hash: payload_digest(&encrypted.bytes),
			is_encrypted: true,
			tenant_id: Some(encrypted.tenant_id),
			tenant_kek_id: Some(encrypted.tenant_kek_id),
			stats_format_version: Some(locator.stats_format_version),
			stats_block_len: Some(locator.stats_block_len),
			stats_block_hash128: Some(locator.stats_block_hash128),
			tombstone_metadata: TombstoneMetadata::None,
			supersession_metadata: SupersessionMetadata::None,
		})
		.await
		.unwrap();

		(mgr, descriptor)
	}

	async fn tier_typed_table_batch_without_stats(
		l2: Arc<MockObjectStore>,
		ids: Vec<i64>,
		object_name: &str,
	) -> (Arc<SlabManager>, TableSchemaDescriptorV1) {
		let mgr = make_manager_with_l2(l2.clone()).await;
		let descriptor = typed_table_descriptor();
		let slab_id = append_typed_table_rows(&mgr, &descriptor, ids).await;

		let raw = mgr.read_sealed_raw(slab_id).await.unwrap();
		let encrypted = mgr.encrypt_tiered_object_blob(slab_id, &raw).unwrap();
		assert!(encrypted.stats_locator.is_none());
		let l2_uri = l2.put(object_name, &encrypted.bytes).await.unwrap();
		mgr.apply_tier_slab(TieredSlabPointer {
			slab_id,
			l2_uri,
			payload_hash: payload_digest(&encrypted.bytes),
			is_encrypted: true,
			tenant_id: Some(encrypted.tenant_id),
			tenant_kek_id: Some(encrypted.tenant_kek_id),
			stats_format_version: None,
			stats_block_len: None,
			stats_block_hash128: None,
			tombstone_metadata: TombstoneMetadata::None,
			supersession_metadata: SupersessionMetadata::None,
		})
		.await
		.unwrap();

		(mgr, descriptor)
	}

	async fn seed_record(
		mgr: &SlabManager,
		locality: u64,
		hlc: HybridTimestamp,
		key: &[u8],
		value: &[u8],
	) {
		let batch = kv_batch(key, value);
		let bytes = encode_batch(&batch).unwrap();
		mgr.append(tenant(), id(locality), hlc, &bytes)
			.await
			.unwrap();
	}

	fn base_meta() -> QueryRequestMeta {
		QueryRequestMeta {
			min_hlc: hlc(0, 0, 0),
			expected_epoch: 0,
			tenant_id: tenant(),
			consistency: Consistency::SERIALIZABLE,
			_pad: [0; 7],
			locality_range_start: 0,
			locality_range_end: u64::MAX,
			bytecode_len: 0,
			_pad2: [0; 4],
			as_of_hlc: hlc(0, 0, 0),
		}
	}

	#[test]
	fn static_router_wraps_around_ring() {
		let router = StaticReplicaRouter::new(vec![n(1), n(2), n(3), n(4)], 3);
		let replicas = router.replicas_for_locality(Locality::new(3));
		assert_eq!(replicas.len(), 3);
		assert_eq!(replicas[0], n(4));
		assert_eq!(replicas[1], n(1));
		assert_eq!(replicas[2], n(2));
	}

	#[test]
	fn static_router_dedups_and_sorts() {
		let router = StaticReplicaRouter::new(vec![n(3), n(1), n(3), n(2)], 5);
		let replicas = router.replicas_for_locality(Locality::ZERO);
		assert_eq!(replicas.len(), 3);
		assert_eq!(replicas[0], n(1));
		assert_eq!(replicas[1], n(2));
		assert_eq!(replicas[2], n(3));
	}

	#[test]
	fn static_router_rf_one_is_single_replica() {
		let router = StaticReplicaRouter::new(vec![n(1), n(2), n(3)], 1);
		assert_eq!(router.replicas_for_locality(Locality::ZERO).len(), 1);
	}

	#[test]
	fn static_router_empty_node_list_returns_empty() {
		let router = StaticReplicaRouter::new(vec![], 3);
		assert!(router.replicas_for_locality(Locality::ZERO).is_empty());
	}

	#[tokio::test]
	async fn scan_collects_entries_in_inclusive_range() {
		let mgr = make_manager().await;
		seed_record(&mgr, 10, hlc(1, 0, 1), b"k10", b"v10").await;
		seed_record(&mgr, 50, hlc(2, 0, 1), b"k50", b"v50").await;
		seed_record(&mgr, 99, hlc(3, 0, 1), b"k99", b"v99").await;

		let hits = mgr.scan(tenant(), 10, 50);
		let keys: Vec<u64> = hits.iter().map(|(k, _)| *k).collect();
		assert_eq!(keys, vec![10, 50]);

		let full = mgr.scan(tenant(), 0, u64::MAX);
		assert_eq!(full.len(), 3);

		let miss = mgr.scan(tenant(), 60, 90);
		assert!(miss.is_empty());

		// Inverted range → empty, never panics.
		let inverted = mgr.scan(tenant(), 100, 5);
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
		meta.locality_range_start = 10;
		meta.locality_range_end = 50;
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
	async fn query_as_of_older_than_retention_window_returns_retention_expired() {
		let mgr = make_manager().await;
		seed_record(&mgr, 1, hlc(90, 0, 1), b"a", b"va").await;

		let sm = WendelonStateMachine::new();
		let mut meta = base_meta();
		meta.as_of_hlc = hlc(50, 0, 1);
		let (resp, arrow) = execute_query_plan_with_budget_and_retention_floor(
			&mgr,
			&sm,
			n(1),
			&meta,
			&[],
			QueryExecutionBudget::default(),
			Some(hlc(80, 0, 1)),
		)
		.await
		.unwrap();
		assert_eq!(resp.status, ReadStatus::RETENTION_EXPIRED);
		assert_eq!(resp.rows, 0);
		assert!(arrow.is_none());
	}

	#[tokio::test]
	async fn latest_query_filters_locations_below_logical_retention_floor() {
		let mgr = make_manager().await;
		seed_record(&mgr, 1, hlc(70, 0, 1), b"old", b"old-value").await;
		seed_record(&mgr, 2, hlc(90, 0, 1), b"live", b"live-value").await;

		let program = pbc::Program::compile(&[]).unwrap();
		let plan = plan_query_execution_for_ranges_with_program(
			&mgr,
			n(1),
			&base_meta(),
			&program,
			&[QueryScanRange {
				start: Locality::ZERO,
				end: Locality::MAX,
			}],
			QueryExecutionBudget::default(),
			None,
			QueryProjectedExecution::Disabled,
			Some(hlc(80, 0, 1)),
		)
		.await
		.unwrap();

		assert_eq!(plan.response.status, ReadStatus::OK);
		assert_eq!(plan.response.rows, 1);
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
	async fn pbc_filter_drops_rows_below_threshold() {
		// Stored records carry an Int64 `age` column so PBC can
		// actually compare against a literal — the write-path
		// schema is LargeBinary and PBC does not support bytes,
		// so tests that exercise filter opcodes use their own
		// schema via direct slab.append.
		use arrow::array::Int64Array;
		use arrow::datatypes::{DataType, Field, Schema};
		let schema = Arc::new(Schema::new(vec![Field::new("age", DataType::Int64, false)]));
		let mgr = make_manager().await;
		for (locality, age) in [(1u64, 17i64), (2, 22), (3, 40)] {
			let ages = Int64Array::from(vec![age]);
			let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(ages)]).unwrap();
			let bytes = encode_batch(&batch).unwrap();
			mgr.append(tenant(), id(locality), hlc(age as u64, 0, 1), &bytes)
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
	async fn pbc_projection_narrows_schema() {
		let mgr = make_manager().await;
		seed_record(&mgr, 1, hlc(1, 0, 1), b"a", b"va").await;
		seed_record(&mgr, 2, hlc(2, 0, 1), b"b", b"vb").await;
		// PROJECT column 0 (the `key` column) only — PBC compiles
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

	#[tokio::test]
	async fn storage_metadata_scalar_pbc_opcodes_return_invalid_program() {
		let mgr = make_manager().await;
		seed_record(&mgr, 1, hlc(1, 0, 1), b"a", b"va").await;

		let sm = WendelonStateMachine::new();
		let mut meta = base_meta();
		meta.bytecode_len = 1;
		let (resp, arrow) = execute_query_plan(&mgr, &sm, n(1), &meta, &[0x40])
			.await
			.unwrap();
		assert_eq!(resp.status, ReadStatus::QUERY_INVALID_PROGRAM);
		assert_eq!(resp.rows, 0);
		assert!(arrow.is_none());
	}

	#[tokio::test]
	async fn invalid_pbc_returns_structured_query_status() {
		let mgr = make_manager().await;
		seed_record(&mgr, 1, hlc(1, 0, 1), b"a", b"va").await;

		let sm = WendelonStateMachine::new();
		let mut meta = base_meta();
		meta.bytecode_len = 1;
		let (resp, arrow) = execute_query_plan(&mgr, &sm, n(1), &meta, &[0x7F])
			.await
			.unwrap();
		assert_eq!(resp.status, ReadStatus::QUERY_INVALID_PROGRAM);
		assert_eq!(resp.rows, 0);
		assert_eq!(resp.arrow_len, 0);
		assert!(arrow.is_none());
	}

	#[tokio::test]
	async fn query_budget_exceeded_returns_structured_status() {
		let mgr = make_manager().await;
		seed_record(&mgr, 1, hlc(1, 0, 1), b"a", b"va").await;
		seed_record(&mgr, 2, hlc(2, 0, 1), b"b", b"vb").await;

		let sm = WendelonStateMachine::new();
		let (resp, arrow) = execute_query_plan_with_budget_and_retention_floor(
			&mgr,
			&sm,
			n(1),
			&base_meta(),
			&[],
			QueryExecutionBudget {
				max_locations: 1,
				max_rows: 10,
				max_wall_time: Duration::from_secs(1),
				stats_pruning_enabled: true,
			},
			None,
		)
		.await
		.unwrap();
		assert_eq!(resp.status, ReadStatus::QUERY_BUDGET_EXCEEDED);
		assert_eq!(resp.rows, 0);
		assert_eq!(resp.arrow_len, 0);
		assert!(arrow.is_none());
	}

	#[tokio::test]
	async fn query_budget_counts_scanned_locations_even_when_filter_rejects_rows() {
		use arrow::array::Int64Array;
		use arrow::datatypes::{DataType, Field, Schema};

		let mgr = make_manager().await;
		let schema = Arc::new(Schema::new(vec![Field::new("age", DataType::Int64, false)]));
		for (locality, age) in [(1_u64, 17_i64), (2, 22)] {
			let ages = Int64Array::from(vec![age]);
			let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(ages)]).unwrap();
			let bytes = encode_batch(&batch).unwrap();
			mgr.append(tenant(), id(locality), hlc(age as u64, 0, 1), &bytes)
				.await
				.unwrap();
		}

		let mut bytecode = Vec::new();
		bytecode.push(OP_PUSH_COL);
		bytecode.extend_from_slice(&0_u32.to_le_bytes());
		bytecode.push(OP_PUSH_INT);
		bytecode.extend_from_slice(&99_i64.to_le_bytes());
		bytecode.push(OP_GTE);

		let sm = WendelonStateMachine::new();
		let mut meta = base_meta();
		meta.bytecode_len = bytecode.len() as u32;
		let (resp, arrow) = execute_query_plan_with_budget_and_retention_floor(
			&mgr,
			&sm,
			n(1),
			&meta,
			&bytecode,
			QueryExecutionBudget {
				max_locations: 1,
				max_rows: 10,
				max_wall_time: Duration::from_secs(1),
				stats_pruning_enabled: true,
			},
			None,
		)
		.await
		.unwrap();
		assert_eq!(resp.status, ReadStatus::QUERY_BUDGET_EXCEEDED);
		assert_eq!(resp.rows, 0);
		assert!(arrow.is_none());
	}

	#[tokio::test]
	async fn query_budget_exceeded_when_single_location_batch_exceeds_row_cap() {
		let mgr = make_manager().await;
		let bytes = encode_batch(&wide_kv_batch(11)).unwrap();
		mgr.append(tenant(), id(1), hlc(1, 0, 1), &bytes)
			.await
			.unwrap();

		let sm = WendelonStateMachine::new();
		let (resp, arrow) = execute_query_plan_with_budget_and_retention_floor(
			&mgr,
			&sm,
			n(1),
			&base_meta(),
			&[],
			QueryExecutionBudget {
				max_locations: 10,
				max_rows: 10,
				max_wall_time: Duration::from_secs(1),
				stats_pruning_enabled: true,
			},
			None,
		)
		.await
		.unwrap();
		assert_eq!(resp.status, ReadStatus::QUERY_BUDGET_EXCEEDED);
		assert_eq!(resp.rows, 0);
		assert_eq!(resp.arrow_len, 0);
		assert!(arrow.is_none());
	}

	#[test]
	fn stream_preflight_encode_respects_deadline() {
		let expired = Instant::now() - Duration::from_millis(1);
		let encoded =
			encode_query_stream_batches_with_deadline(vec![single_kv_batch()], expired).unwrap();
		assert!(encoded.is_none());
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
		ClientWriteId, Consistency as SchemaConsistency, DurabilityLevel, SlabId, WriteRequestMeta,
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
			tenant_id: tenant(),
			proxy_hop: 0,
			durability: DurabilityLevel::QUORUM,
			await_finalization: 0,
			_pad1: [0; 5],
			proxy_source: NodeId {
				id: 0,
				_reserved: 0,
			},
			key_identity: Identity::blake3_tenant(tenant(), b"k"),
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
			tenant_id: tenant(),
			consistency: SchemaConsistency::LINEARIZABLE,
			_pad: [0; 3],
			key_identity: Identity::blake3_tenant(tenant(), b"k"),
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

	fn actor(role: ActorRole, tenant_id: Option<&str>) -> ActorIdentity {
		ActorIdentity::new(
			role,
			None,
			tenant_id.map(str::to_string),
			vec!["cluster".to_string()],
			vec![1, 2, 3, 4],
		)
		.unwrap()
	}

	async fn wait_for_audit_log(path: &std::path::Path, needles: &[&str]) -> String {
		let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
		loop {
			let audit = std::fs::read_to_string(path).unwrap_or_default();
			if needles.iter().all(|needle| audit.contains(needle)) {
				return audit;
			}
			assert!(
				tokio::time::Instant::now() < deadline,
				"timed out waiting for audit needles {:?}; current log: {}",
				needles,
				audit
			);
			tokio::time::sleep(Duration::from_millis(20)).await;
		}
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
		// Stop background renewal before forcing the cache empty; under
		// parallel test runners the lease manager can otherwise repopulate
		// the cache between clear() and the write under test.
		node.handles.lease_manager.abort();
		tokio::task::yield_now().await;
		node.data_server.lease_cache().write().unwrap().clear();

		let mut meta = simple_write_meta(1);
		meta.client_hlc = hlc(u64::MAX / 4, 0, node.local_node.id);
		let (resp, _) = node
			.data_server
			.write(meta, single_kv_batch())
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
		let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
		loop {
			if node.raft.current_leader().await == Some(node.local_node.id) {
				break;
			}
			assert!(
				tokio::time::Instant::now() < deadline,
				"timed out waiting for leader"
			);
			tokio::time::sleep(Duration::from_millis(20)).await;
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
		let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
		loop {
			if node.raft.current_leader().await == Some(node.local_node.id) {
				break;
			}
			assert!(
				tokio::time::Instant::now() < deadline,
				"timed out waiting for leader"
			);
			tokio::time::sleep(Duration::from_millis(20)).await;
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

	#[tokio::test]
	async fn finalization_marker_preserves_requested_durability_class() {
		let dir = bootstrap_tempdir("durability-marker");
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			15,
			dir,
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();
		let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
		loop {
			if node.raft.current_leader().await == Some(node.local_node.id) {
				break;
			}
			assert!(
				tokio::time::Instant::now() < deadline,
				"timed out waiting for leader"
			);
			tokio::time::sleep(Duration::from_millis(20)).await;
		}
		node.data_server
			.refresh_leases_once(&node.ring, &*node.clock, 60_000)
			.await;

		let mut meta = simple_write_meta(1);
		meta.client_hlc = hlc(1, 0, 0);
		meta.await_finalization = 1;
		meta.durability = DurabilityLevel::LEADER;

		let (resp, trailing) = node
			.data_server
			.write(meta, single_kv_batch())
			.await
			.unwrap();
		assert_eq!(resp.status, WriteStatus::OK, "got resp={resp:?}");
		assert_eq!(
			trailing
				.expect("await_finalization should return a marker")
				.durability,
			DurabilityLevel::LEADER
		);
	}

	#[test]
	fn bifurcation_rejects_tampered_key_identity() {
		let mut meta = simple_write_meta(1);
		meta.key_identity = Identity::blake3_tenant(tenant(), b"wrong-key");
		let err = bifurcate_client_write(&meta, &single_kv_batch()).unwrap_err();
		assert!(err.to_string().contains("key_identity mismatch"));
	}

	#[test]
	fn table_bifurcation_splits_rows_into_independent_arrow_frames() {
		let descriptor = typed_table_descriptor();
		let batch = typed_table_batch(vec![7, 8]);
		let meta = typed_table_meta(&descriptor, batch.num_rows());
		let bifurcated = bifurcate_table_write(
			&meta,
			&batch,
			&descriptor,
			WriterNodeId::ZERO,
			meta.client_hlc,
		)
		.unwrap();

		assert_eq!(bifurcated.children.len(), 2);
		assert_eq!(
			bifurcated.charged_bytes,
			bifurcated
				.children
				.iter()
				.map(|child| child.charged_bytes)
				.sum::<u64>()
		);
		assert_ne!(
			bifurcated.children[0].key_identity,
			bifurcated.children[1].key_identity
		);
		assert_eq!(bifurcated.children[0].child_ordinal, 1_000);
		assert_eq!(bifurcated.children[1].child_ordinal, 1_001);
		let ranges = table_spread_prefix_ranges(
			meta.tenant_id,
			meta.table_object_id,
			match descriptor.locality_policy {
				LocalityPolicyV1::TableSpreadProjection(spread) => spread,
				_ => unreachable!(),
			},
		)
		.unwrap();

		for (idx, child) in bifurcated.children.iter().enumerate() {
			assert!(
				ranges
					.iter()
					.any(|(start, end)| start.get() <= child.locality.get()
						&& child.locality.get() <= end.get())
			);
			assert_eq!(
				child.payload_hash,
				payload_digest(child.frame_bytes.as_slice())
			);
			let row = decode_batch(child.frame_bytes.as_slice()).unwrap();
			assert_eq!(row.num_rows(), 1);
			assert_eq!(row.schema().as_ref(), &typed_table_schema());
			let ids = row
				.column_by_name("id")
				.unwrap()
				.as_any()
				.downcast_ref::<Int64Array>()
				.unwrap();
			assert_eq!(ids.value(0), if idx == 0 { 7 } else { 8 });
		}
	}

	#[test]
	fn table_bifurcation_packs_multirow_records_with_storage_metadata() {
		let descriptor = multirow_typed_table_descriptor();
		let source_ids = same_multirow_locality_ids(&descriptor, 3);
		let batch = typed_table_batch(source_ids.clone());
		let meta = typed_table_meta(&descriptor, batch.num_rows());
		let bifurcated = bifurcate_table_write(
			&meta,
			&batch,
			&descriptor,
			WriterNodeId::ZERO,
			meta.client_hlc,
		)
		.unwrap();

		assert_eq!(bifurcated.children.len(), 1);
		assert_eq!(bifurcated.record_count, 1);
		let storage = decode_batch(bifurcated.children[0].frame_bytes.as_slice()).unwrap();
		assert_eq!(storage.num_rows(), 3);
		assert!(has_storage_synthetic_columns(&storage));
		let materialized =
			materialize_record_location_batch(&storage, RecordRowIndex::new(1)).unwrap();
		assert_eq!(materialized.schema().as_ref(), &typed_table_schema());
		assert_user_visible_schema_has_no_storage_columns(&materialized);
		let ids = materialized
			.column_by_name("id")
			.unwrap()
			.as_any()
			.downcast_ref::<Int64Array>()
			.unwrap();
		let storage_ids = storage
			.column_by_name("id")
			.unwrap()
			.as_any()
			.downcast_ref::<Int64Array>()
			.unwrap();
		assert_eq!(ids.value(0), storage_ids.value(1));
		assert!(source_ids.contains(&ids.value(0)));
	}

	#[test]
	fn table_bifurcation_charges_encoded_bytes_for_compressed_quota_axis() {
		let lz4_descriptor = multirow_typed_table_descriptor();
		let mut uncompressed_descriptor = lz4_descriptor.clone();
		uncompressed_descriptor.record_body_compression_policy =
			RecordBodyCompressionPolicy::Uncompressed;
		uncompressed_descriptor
			.refresh_descriptor_fingerprints()
			.unwrap();
		uncompressed_descriptor.validate().unwrap();

		let source_ids = same_multirow_locality_ids(&lz4_descriptor, 32);
		let repeated_name = "same-value".repeat(128);
		let names = (0..source_ids.len())
			.map(|_| repeated_name.as_str())
			.collect::<Vec<_>>();
		let batch = RecordBatch::try_new(
			Arc::new(typed_table_schema()),
			vec![
				Arc::new(Int64Array::from(source_ids)) as Arc<dyn Array>,
				Arc::new(LargeStringArray::from(names)) as Arc<dyn Array>,
			],
		)
		.unwrap();

		let uncompressed_meta = typed_table_meta(&uncompressed_descriptor, batch.num_rows());
		let lz4_meta = typed_table_meta(&lz4_descriptor, batch.num_rows());
		let uncompressed = bifurcate_table_write(
			&uncompressed_meta,
			&batch,
			&uncompressed_descriptor,
			WriterNodeId::ZERO,
			uncompressed_meta.client_hlc,
		)
		.unwrap();
		let lz4 = bifurcate_table_write(
			&lz4_meta,
			&batch,
			&lz4_descriptor,
			WriterNodeId::ZERO,
			lz4_meta.client_hlc,
		)
		.unwrap();

		assert_eq!(
			uncompressed.charged_bytes,
			uncompressed
				.children
				.iter()
				.map(|child| child.frame_bytes.len() as u64)
				.sum::<u64>()
		);
		assert_eq!(
			lz4.charged_bytes,
			lz4.children
				.iter()
				.map(|child| child.frame_bytes.len() as u64)
				.sum::<u64>()
		);
		assert!(
			lz4.charged_bytes < uncompressed.charged_bytes,
			"quota accounting should use encoded bytes: lz4={} uncompressed={}",
			lz4.charged_bytes,
			uncompressed.charged_bytes
		);
		for child in &lz4.children {
			assert_eq!(child.charged_bytes, child.frame_bytes.len() as u64);
			assert_eq!(
				crate::storage::ipc::record_batch_compression_policy_from_ipc(&child.frame_bytes)
					.unwrap(),
				RecordBodyCompressionPolicy::Lz4Frame
			);
		}
	}

	#[test]
	fn table_query_scan_ranges_accepts_column_projection_prefixes() {
		let tenant_id = tenant();
		let projection = ColumnLocalityDescriptorV1 {
			columns: vec![ClusteringColumnSpecV1 {
				name: "id".to_owned(),
				width_bits: 16,
				string_prefix_bytes: None,
			}],
			projection: ProjectionFunctionV1::ZOrder,
			spread_factor: 4,
			prefix_bits: 12,
		};
		let descriptor = TableSchemaDescriptorV1::new(
			TableObjectId::new([0x38; 16]),
			3,
			"analytics",
			"events",
			&typed_table_schema(),
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
			LocalityPolicyV1::ColumnProjection(projection.clone()),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.unwrap();
		let ranges = table_query_scan_ranges(tenant_id, &descriptor, None).unwrap();
		let expected =
			column_projection_prefix_ranges(tenant_id, descriptor.table_object_id, &projection)
				.unwrap();

		assert_eq!(ranges.len(), expected.len());
		for (range, (start, end)) in ranges.iter().zip(expected) {
			assert_eq!(range.start, start);
			assert_eq!(range.end, end);
		}
	}

	#[test]
	fn table_query_scan_ranges_prune_column_projection_from_pbc() {
		let tenant_id = tenant();
		let projection = ColumnLocalityDescriptorV1 {
			columns: vec![ClusteringColumnSpecV1 {
				name: "id".to_owned(),
				width_bits: 16,
				string_prefix_bytes: None,
			}],
			projection: ProjectionFunctionV1::ZOrder,
			spread_factor: 4,
			prefix_bits: 12,
		};
		let descriptor = TableSchemaDescriptorV1::new(
			TableObjectId::new([0x39; 16]),
			3,
			"analytics",
			"events",
			&typed_table_schema(),
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
			LocalityPolicyV1::ColumnProjection(projection.clone()),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.unwrap();
		let mut bytecode = vec![OP_PUSH_COL];
		bytecode.extend_from_slice(&0_u32.to_le_bytes());
		bytecode.push(OP_PUSH_INT);
		bytecode.extend_from_slice(&7_i64.to_le_bytes());
		bytecode.push(OP_GTE);
		let program = pbc::Program::compile(&bytecode).unwrap();
		let ranges = table_query_scan_ranges(tenant_id, &descriptor, Some(&program)).unwrap();
		let full =
			column_projection_prefix_ranges(tenant_id, descriptor.table_object_id, &projection)
				.unwrap();

		assert_eq!(ranges.len(), full.len());
		assert!(ranges.iter().zip(full.iter()).all(|(range, (start, end))| {
			start.get() <= range.start.get()
				&& range.end.get() <= end.get()
				&& (range.start != *start || range.end != *end)
		}));
	}

	#[tokio::test]
	async fn table_query_skips_tiered_slab_when_column_stats_cannot_match() {
		let l2 = Arc::new(MockObjectStore::default());
		let mgr = make_manager_with_l2(l2.clone()).await;
		let descriptor = typed_table_descriptor();
		let batch = typed_table_batch(vec![1, 2]);
		let mut write_meta: TableWriteRequestMeta = bytemuck::Zeroable::zeroed();
		write_meta.tenant_id = tenant();
		write_meta.table_object_id = descriptor.table_object_id;
		write_meta.schema_generation = descriptor.schema_generation;
		write_meta.row_count = encode_table_write_row_count(batch.num_rows()).unwrap();
		let admission = validate_table_write_request_batch(
			&write_meta,
			&batch,
			&descriptor,
			WriterNodeId::ZERO,
			write_meta.client_hlc,
		)
		.expect("typed rows should validate");
		for row in 0..batch.num_rows() {
			let row_batch = batch.slice(row, 1);
			let frame = encode_batch(&row_batch).unwrap();
			mgr.append_durable_with_locality(
				tenant(),
				admission.key_identities[row],
				admission.localities[row],
				hlc(10 + row as u64, 0, 1),
				&frame,
			)
			.await
			.unwrap();
		}

		let slab_id = mgr.force_seal_active_for_test().await;
		let raw = mgr.read_sealed_raw(slab_id).await.unwrap();
		let mut catalog = TableSchemaCatalogV1::default();
		catalog.upsert(tenant(), descriptor.clone()).unwrap();
		let encrypted = mgr
			.encrypt_tiered_object_blob_with_table_stats(slab_id, &raw, &catalog)
			.unwrap();
		let locator = encrypted.stats_locator.expect("stats locator");
		let l2_uri = l2
			.put(&format!("stats-prune-{slab_id}.wnd"), &encrypted.bytes)
			.await
			.unwrap();
		l2.inject(InjectedMockObjectStoreFault::on(
			MockObjectStoreOp::GetBytes,
			MockObjectStoreFaultAction::Error {
				kind: ErrorKind::Other,
				message: "stats pruning must not fetch the full L2 object".to_owned(),
			},
		));
		l2.inject(
			InjectedMockObjectStoreFault::on(
				MockObjectStoreOp::GetRange,
				MockObjectStoreFaultAction::Error {
					kind: ErrorKind::Other,
					message: "pruned query must not range-read row ciphertext".to_owned(),
				},
			)
			.after_matches(3),
		);
		mgr.apply_tier_slab(TieredSlabPointer {
			slab_id,
			l2_uri,
			payload_hash: payload_digest(&encrypted.bytes),
			is_encrypted: true,
			tenant_id: Some(encrypted.tenant_id),
			tenant_kek_id: Some(encrypted.tenant_kek_id),
			stats_format_version: Some(locator.stats_format_version),
			stats_block_len: Some(locator.stats_block_len),
			stats_block_hash128: Some(locator.stats_block_hash128),
			tombstone_metadata: TombstoneMetadata::None,
			supersession_metadata: SupersessionMetadata::None,
		})
		.await
		.unwrap();

		let mut bytecode = Vec::new();
		bytecode.push(OP_PUSH_COL);
		bytecode.extend_from_slice(&0_u32.to_le_bytes());
		bytecode.push(OP_PUSH_INT);
		bytecode.extend_from_slice(&100_i64.to_le_bytes());
		bytecode.push(OP_GTE);
		let program = pbc::Program::compile(&bytecode).unwrap();
		let meta = base_meta();
		let ranges = [QueryScanRange {
			start: Locality::ZERO,
			end: Locality::MAX,
		}];
		let pruned_before = metrics().pbc_slab_stats_pruned_total.get();
		let consulted_before = metrics().pbc_slab_stats_consulted_total.get();

		let plan = plan_query_execution_for_ranges_with_program(
			&mgr,
			n(1),
			&meta,
			&program,
			&ranges,
			QueryExecutionBudget::default(),
			Some(&descriptor),
			QueryProjectedExecution::Disabled,
			None,
		)
		.await
		.unwrap();

		assert_eq!(plan.response.status, ReadStatus::KEY_NOT_FOUND);
		assert_eq!(
			plan.stats_explain,
			QueryStatsExplainCounters {
				candidate_slabs_before_stats: 1,
				slabs_pruned_by_stats: 1,
				slabs_scanned_stats_missing: 0,
				slabs_scanned_stats_unsupported: 0,
				slabs_scanned_predicate_unrepresentable: 0,
				stats_cache_hits: 1,
				stats_cache_misses: 1,
				stats_decrypt_failed_total: 0,
			}
		);
		assert!(
			metrics().pbc_slab_stats_consulted_total.get() >= consulted_before + 1,
			"tiered stats should be consulted before L2 row reads"
		);
		assert!(
			metrics().pbc_slab_stats_pruned_total.get() >= pruned_before + 1,
			"the slab-level id min/max should rule out id >= 100"
		);

		l2.inject(InjectedMockObjectStoreFault::on(
			MockObjectStoreOp::Head,
			MockObjectStoreFaultAction::Error {
				kind: ErrorKind::Other,
				message: "cached stats must not HEAD L2".to_owned(),
			},
		));
		l2.inject(InjectedMockObjectStoreFault::on(
			MockObjectStoreOp::GetRange,
			MockObjectStoreFaultAction::Error {
				kind: ErrorKind::Other,
				message: "cached stats must not range-read L2".to_owned(),
			},
		));
		let cache_hits_before = metrics().pbc_slab_stats_cache_hits_total.get();

		let second = plan_query_execution_for_ranges_with_program(
			&mgr,
			n(1),
			&meta,
			&program,
			&ranges,
			QueryExecutionBudget::default(),
			Some(&descriptor),
			QueryProjectedExecution::Disabled,
			None,
		)
		.await
		.unwrap();

		assert_eq!(second.response.status, ReadStatus::KEY_NOT_FOUND);
		assert!(
			metrics().pbc_slab_stats_cache_hits_total.get() >= cache_hits_before + 1,
			"second query should reuse the parsed stats block cached by the slab manager"
		);
	}

	#[tokio::test]
	async fn query_request_flag_disables_table_stats_pruning() {
		let mgr = make_manager().await;
		let descriptor = typed_table_descriptor();
		let batch = typed_table_batch(vec![1, 2]);
		let mut write_meta: TableWriteRequestMeta = bytemuck::Zeroable::zeroed();
		write_meta.tenant_id = tenant();
		write_meta.table_object_id = descriptor.table_object_id;
		write_meta.schema_generation = descriptor.schema_generation;
		write_meta.row_count = encode_table_write_row_count(batch.num_rows()).unwrap();
		let admission = validate_table_write_request_batch(
			&write_meta,
			&batch,
			&descriptor,
			WriterNodeId::ZERO,
			write_meta.client_hlc,
		)
		.expect("typed rows should validate");
		for row in 0..batch.num_rows() {
			let row_batch = batch.slice(row, 1);
			let frame = encode_batch(&row_batch).unwrap();
			mgr.append_durable_with_locality(
				tenant(),
				admission.key_identities[row],
				admission.localities[row],
				hlc(10 + row as u64, 0, 1),
				&frame,
			)
			.await
			.unwrap();
		}

		let program = pbc::Program::compile(&[]).unwrap();
		let mut meta = base_meta();
		meta._pad[0] |= crate::schema::QUERY_REQUEST_FLAG_DISABLE_STATS_PRUNING;
		let plan = plan_query_execution_for_ranges_with_program(
			&mgr,
			n(1),
			&meta,
			&program,
			&[QueryScanRange {
				start: Locality::ZERO,
				end: Locality::MAX,
			}],
			QueryExecutionBudget::default(),
			Some(&descriptor),
			QueryProjectedExecution::Disabled,
			None,
		)
		.await
		.unwrap();

		assert_eq!(plan.response.status, ReadStatus::OK);
		assert_eq!(plan.response.rows, 2);
		assert_eq!(plan.stats_explain, QueryStatsExplainCounters::default());
	}

	#[tokio::test]
	async fn table_stats_admit_slab_then_pbc_filters_rows() {
		let l2 = Arc::new(MockObjectStore::default());
		let mgr = make_manager_with_l2_projected_fetch(l2.clone()).await;
		let descriptor = typed_table_descriptor();
		let slab_id = append_typed_table_rows(&mgr, &descriptor, vec![1, 2]).await;
		let raw = mgr.read_sealed_raw(slab_id).await.unwrap();
		let mut catalog = TableSchemaCatalogV1::default();
		catalog.upsert(tenant(), descriptor.clone()).unwrap();
		let encrypted = mgr
			.encrypt_tiered_object_blob_with_table_stats(slab_id, &raw, &catalog)
			.unwrap();
		let locator = encrypted.stats_locator.expect("stats locator");
		let l2_uri = l2
			.put("stats-admit-pbc.wnd", &encrypted.bytes)
			.await
			.unwrap();
		mgr.apply_tier_slab(TieredSlabPointer {
			slab_id,
			l2_uri,
			payload_hash: payload_digest(&encrypted.bytes),
			is_encrypted: true,
			tenant_id: Some(encrypted.tenant_id),
			tenant_kek_id: Some(encrypted.tenant_kek_id),
			stats_format_version: Some(locator.stats_format_version),
			stats_block_len: Some(locator.stats_block_len),
			stats_block_hash128: Some(locator.stats_block_hash128),
			tombstone_metadata: TombstoneMetadata::None,
			supersession_metadata: SupersessionMetadata::None,
		})
		.await
		.unwrap();

		let mut bytecode = Vec::new();
		bytecode.push(OP_PUSH_COL);
		bytecode.extend_from_slice(&0_u32.to_le_bytes());
		bytecode.push(OP_PUSH_INT);
		bytecode.extend_from_slice(&2_i64.to_le_bytes());
		bytecode.push(OP_GTE);
		let program = pbc::Program::compile(&bytecode).unwrap();
		let user_column_count = descriptor.arrow_schema().unwrap().fields().len();
		let requirement = analyze_pbc_column_requirement(&program, user_column_count).unwrap();
		let plan = plan_query_execution_for_ranges_with_program(
			&mgr,
			n(1),
			&base_meta(),
			&program,
			&[QueryScanRange {
				start: Locality::ZERO,
				end: Locality::MAX,
			}],
			QueryExecutionBudget::default(),
			Some(&descriptor),
			QueryProjectedExecution::Pbc {
				requirement,
				user_column_count,
			},
			None,
		)
		.await
		.unwrap();

		assert_eq!(plan.response.status, ReadStatus::OK);
		assert_eq!(plan.response.rows, 1);
		assert_eq!(
			plan.stats_explain,
			QueryStatsExplainCounters {
				candidate_slabs_before_stats: 1,
				slabs_pruned_by_stats: 0,
				slabs_scanned_stats_missing: 0,
				slabs_scanned_stats_unsupported: 0,
				slabs_scanned_predicate_unrepresentable: 0,
				stats_cache_hits: 1,
				stats_cache_misses: 1,
				stats_decrypt_failed_total: 0,
			}
		);
		let ids = plan.batches[0]
			.column_by_name("id")
			.unwrap()
			.as_any()
			.downcast_ref::<Int64Array>()
			.unwrap();
		assert_eq!(ids.values(), &[2]);
	}

	#[tokio::test]
	async fn aggregate_plan_folds_projected_l2_inputs() {
		let l2 = Arc::new(MockObjectStore::default());
		let mgr = make_manager_with_l2_projected_fetch(l2.clone()).await;
		let descriptor = typed_table_descriptor();
		let slab_id = append_typed_table_rows(&mgr, &descriptor, vec![1, 2]).await;
		let raw = mgr.read_sealed_raw(slab_id).await.unwrap();
		let mut catalog = TableSchemaCatalogV1::default();
		catalog.upsert(tenant(), descriptor.clone()).unwrap();
		let encrypted = mgr
			.encrypt_tiered_object_blob_with_table_stats(slab_id, &raw, &catalog)
			.unwrap();
		let locator = encrypted.stats_locator.expect("stats locator");
		let l2_uri = l2
			.put("stats-admit-aggregate.wnd", &encrypted.bytes)
			.await
			.unwrap();
		mgr.apply_tier_slab(TieredSlabPointer {
			slab_id,
			l2_uri,
			payload_hash: payload_digest(&encrypted.bytes),
			is_encrypted: true,
			tenant_id: Some(encrypted.tenant_id),
			tenant_kek_id: Some(encrypted.tenant_kek_id),
			stats_format_version: Some(locator.stats_format_version),
			stats_block_len: Some(locator.stats_block_len),
			stats_block_hash128: Some(locator.stats_block_hash128),
			tombstone_metadata: TombstoneMetadata::None,
			supersession_metadata: SupersessionMetadata::None,
		})
		.await
		.unwrap();

		let mut predicate = Vec::new();
		predicate.push(OP_PUSH_COL);
		predicate.extend_from_slice(&0_u32.to_le_bytes());
		predicate.push(OP_PUSH_INT);
		predicate.extend_from_slice(&2_i64.to_le_bytes());
		predicate.push(OP_GTE);
		let request = AggregateRequest {
			format_version: AGG_FORMAT_VERSION,
			request_id: AggregateRequestId::new(144),
			tenant_id: tenant(),
			table_object_id: descriptor.table_object_id,
			schema_generation: descriptor.schema_generation,
			logical_schema_fingerprint: descriptor.logical_schema_fingerprint,
			consistency: Consistency::SERIALIZABLE,
			min_hlc: hlc(0, 0, 0),
			as_of_hlc: hlc(0, 0, 0),
			locality_ranges: vec![LocalityRange::full()],
			predicate_program: Some(predicate.clone()),
			aggregates: vec![
				AggregateExpression::count_star(),
				AggregateExpression {
					tag: AggregateFunctionTag::SIGNED_SUM,
					column_ordinal: 0,
					parameter_block: Vec::new(),
				},
			],
			budgets: AggregateBudgets {
				max_locations: LocationCount::new(16),
				max_rows: RowCount::new(16),
				max_wall_time_millis: DurationMillis::new(1_000),
				max_partial_state_bytes: ByteCount::new(4_096),
			},
		};
		let program = pbc::Program::compile(&predicate).unwrap();
		let prepared = PreparedAggregateExecution {
			range_meta: aggregate_as_range_query(&request, 0),
			program,
			descriptor: descriptor.clone(),
			ranges: vec![QueryScanRange {
				start: Locality::ZERO,
				end: Locality::MAX,
			}],
			retention_floor: None,
		};
		let projected_execution = aggregate_projected_execution(&mgr, &request, &prepared).unwrap();
		let mut folded_request =
			aggregate_fold_request_for_execution(&request, &projected_execution).unwrap();
		folded_request.predicate_program = None;

		let plan = plan_query_execution_for_ranges_with_program(
			&mgr,
			n(1),
			&prepared.range_meta,
			&prepared.program,
			&prepared.ranges,
			aggregate_query_budget(&request.budgets),
			Some(&descriptor),
			projected_execution,
			None,
		)
		.await
		.unwrap();

		assert_eq!(plan.response.status, ReadStatus::OK);
		assert_eq!(plan.response.rows, 1);
		assert_eq!(plan.batches[0].num_columns(), 1);
		let result = aggregate_record_batches(&folded_request, &plan.batches, false).unwrap();
		assert_eq!(result.response.status, AggregateStatus::Success);
		assert_eq!(
			result.response.partials[0].state.as_slice(),
			&1_u64.to_be_bytes()
		);
		let expected_sum = encode_aggregate_state(
			AggregateFunctionTag::SIGNED_SUM,
			&AggregateState::SignedSum {
				sum: 2,
				non_null_count: 1,
				overflow: false,
			},
		)
		.unwrap();
		assert_eq!(result.response.partials[1].state, expected_sum);
	}

	#[tokio::test]
	async fn append_only_row_tombstones_are_hidden_from_full_and_projected_queries() {
		let l2 = Arc::new(MockObjectStore::default());
		let mgr = make_manager_with_l2_projected_fetch(l2.clone()).await;
		let descriptor = append_only_table_descriptor();
		let row_key_ids =
			append_append_only_rows_for_query(&mgr, &descriptor, vec![7, 8, 9], hlc(10, 0, 1))
				.await;
		let data_slab_id =
			tier_active_slab_with_stats_for_query(&mgr, l2, &descriptor, "append-delete-query.wnd")
				.await;
		append_append_only_tombstones_for_query(
			&mgr,
			&descriptor,
			&[row_key_ids[1]],
			hlc(20, 0, 1),
		)
		.await;

		let mut before_delete_meta = base_meta();
		before_delete_meta.as_of_hlc = hlc(15, 0, 1);
		let program = pbc::Program::compile(&[]).unwrap();
		let before_delete = plan_query_execution_for_ranges_with_program(
			&mgr,
			n(1),
			&before_delete_meta,
			&program,
			&[QueryScanRange {
				start: Locality::ZERO,
				end: Locality::MAX,
			}],
			QueryExecutionBudget::default(),
			Some(&descriptor),
			QueryProjectedExecution::Disabled,
			None,
		)
		.await
		.unwrap();
		assert_eq!(before_delete.response.status, ReadStatus::OK);
		assert_eq!(
			sorted_i64_column_values(&before_delete.batches, "id"),
			vec![7, 8, 9]
		);

		let mut after_delete_meta = base_meta();
		after_delete_meta.as_of_hlc = hlc(99, 0, 1);
		let full = plan_query_execution_for_ranges_with_program(
			&mgr,
			n(1),
			&after_delete_meta,
			&program,
			&[QueryScanRange {
				start: Locality::ZERO,
				end: Locality::MAX,
			}],
			QueryExecutionBudget::default(),
			Some(&descriptor),
			QueryProjectedExecution::Disabled,
			None,
		)
		.await
		.unwrap();
		assert_eq!(full.response.status, ReadStatus::OK);
		assert_eq!(full.response.rows, 2);
		assert_eq!(sorted_i64_column_values(&full.batches, "id"), vec![7, 9]);

		let pointer = mgr.tier_pointer(data_slab_id).await.unwrap();
		assert_eq!(
			pointer.tombstone_metadata,
			TombstoneMetadata::Earliest(hlc(20, 0, 1))
		);

		let mut predicate = Vec::new();
		predicate.push(OP_PUSH_COL);
		predicate.extend_from_slice(&0_u32.to_le_bytes());
		predicate.push(OP_PUSH_INT);
		predicate.extend_from_slice(&0_i64.to_le_bytes());
		predicate.push(OP_GTE);
		let projected_program = pbc::Program::compile(&predicate).unwrap();
		let user_column_count = descriptor.arrow_schema().unwrap().fields().len();
		let requirement =
			analyze_pbc_column_requirement(&projected_program, user_column_count).unwrap();
		let projected = plan_query_execution_for_ranges_with_program(
			&mgr,
			n(1),
			&after_delete_meta,
			&projected_program,
			&[QueryScanRange {
				start: Locality::ZERO,
				end: Locality::MAX,
			}],
			QueryExecutionBudget::default(),
			Some(&descriptor),
			QueryProjectedExecution::Pbc {
				requirement,
				user_column_count,
			},
			None,
		)
		.await
		.unwrap();
		assert_eq!(projected.response.status, ReadStatus::OK);
		assert_eq!(projected.response.rows, 2);
		assert_eq!(
			sorted_i64_column_values(&projected.batches, "id"),
			vec![7, 9]
		);
	}

	#[tokio::test]
	async fn append_only_row_tombstones_are_hidden_from_projected_aggregate_inputs() {
		let l2 = Arc::new(MockObjectStore::default());
		let mgr = make_manager_with_l2_projected_fetch(l2.clone()).await;
		let descriptor = append_only_table_descriptor();
		let row_key_ids =
			append_append_only_rows_for_query(&mgr, &descriptor, vec![7, 8, 9], hlc(10, 0, 1))
				.await;
		tier_active_slab_with_stats_for_query(&mgr, l2, &descriptor, "append-delete-aggregate.wnd")
			.await;
		append_append_only_tombstones_for_query(
			&mgr,
			&descriptor,
			&[row_key_ids[1]],
			hlc(20, 0, 1),
		)
		.await;

		let mut predicate = Vec::new();
		predicate.push(OP_PUSH_COL);
		predicate.extend_from_slice(&0_u32.to_le_bytes());
		predicate.push(OP_PUSH_INT);
		predicate.extend_from_slice(&0_i64.to_le_bytes());
		predicate.push(OP_GTE);
		let request = AggregateRequest {
			format_version: AGG_FORMAT_VERSION,
			request_id: AggregateRequestId::new(147),
			tenant_id: tenant(),
			table_object_id: descriptor.table_object_id,
			schema_generation: descriptor.schema_generation,
			logical_schema_fingerprint: descriptor.logical_schema_fingerprint,
			consistency: Consistency::SERIALIZABLE,
			min_hlc: hlc(0, 0, 0),
			as_of_hlc: hlc(99, 0, 1),
			locality_ranges: vec![LocalityRange::full()],
			predicate_program: Some(predicate.clone()),
			aggregates: vec![
				AggregateExpression::count_star(),
				AggregateExpression {
					tag: AggregateFunctionTag::SIGNED_SUM,
					column_ordinal: 0,
					parameter_block: Vec::new(),
				},
			],
			budgets: AggregateBudgets {
				max_locations: LocationCount::new(16),
				max_rows: RowCount::new(16),
				max_wall_time_millis: DurationMillis::new(1_000),
				max_partial_state_bytes: ByteCount::new(4_096),
			},
		};
		let program = pbc::Program::compile(&predicate).unwrap();
		let prepared = PreparedAggregateExecution {
			range_meta: aggregate_as_range_query(&request, 0),
			program,
			descriptor: descriptor.clone(),
			ranges: vec![QueryScanRange {
				start: Locality::ZERO,
				end: Locality::MAX,
			}],
			retention_floor: None,
		};
		let projected_execution = aggregate_projected_execution(&mgr, &request, &prepared).unwrap();
		assert!(matches!(
			projected_execution,
			QueryProjectedExecution::Aggregate { .. }
		));
		let mut folded_request =
			aggregate_fold_request_for_execution(&request, &projected_execution).unwrap();
		folded_request.predicate_program = None;

		let plan = plan_query_execution_for_ranges_with_program(
			&mgr,
			n(1),
			&prepared.range_meta,
			&prepared.program,
			&prepared.ranges,
			aggregate_query_budget(&request.budgets),
			Some(&descriptor),
			projected_execution,
			None,
		)
		.await
		.unwrap();

		assert_eq!(plan.response.status, ReadStatus::OK);
		assert_eq!(plan.response.rows, 2);
		let result = aggregate_record_batches(&folded_request, &plan.batches, false).unwrap();
		assert_eq!(result.response.status, AggregateStatus::Success);
		assert_eq!(
			result.response.partials[0].state.as_slice(),
			&2_u64.to_be_bytes()
		);
		let expected_sum = encode_aggregate_state(
			AggregateFunctionTag::SIGNED_SUM,
			&AggregateState::SignedSum {
				sum: 16,
				non_null_count: 2,
				overflow: false,
			},
		)
		.unwrap();
		assert_eq!(result.response.partials[1].state, expected_sum);
	}

	#[tokio::test]
	async fn table_stats_absence_falls_through_to_full_scan() {
		let l2 = Arc::new(MockObjectStore::default());
		let (mgr, descriptor) =
			tier_typed_table_batch_without_stats(l2, vec![1, 2], "stats-missing-scan.wnd").await;

		let mut bytecode = Vec::new();
		bytecode.push(OP_PUSH_COL);
		bytecode.extend_from_slice(&0_u32.to_le_bytes());
		bytecode.push(OP_PUSH_INT);
		bytecode.extend_from_slice(&2_i64.to_le_bytes());
		bytecode.push(OP_GTE);
		let program = pbc::Program::compile(&bytecode).unwrap();
		let plan = plan_query_execution_for_ranges_with_program(
			&mgr,
			n(1),
			&base_meta(),
			&program,
			&[QueryScanRange {
				start: Locality::ZERO,
				end: Locality::MAX,
			}],
			QueryExecutionBudget::default(),
			Some(&descriptor),
			QueryProjectedExecution::Disabled,
			None,
		)
		.await
		.unwrap();

		assert_eq!(plan.response.status, ReadStatus::OK);
		assert_eq!(plan.response.rows, 1);
		assert_eq!(
			plan.stats_explain,
			QueryStatsExplainCounters {
				candidate_slabs_before_stats: 1,
				slabs_pruned_by_stats: 0,
				slabs_scanned_stats_missing: 1,
				slabs_scanned_stats_unsupported: 0,
				slabs_scanned_predicate_unrepresentable: 0,
				stats_cache_hits: 1,
				stats_cache_misses: 1,
				stats_decrypt_failed_total: 0,
			}
		);
		let ids = plan.batches[0]
			.column_by_name("id")
			.unwrap()
			.as_any()
			.downcast_ref::<Int64Array>()
			.unwrap();
		assert_eq!(ids.values(), &[2]);
	}

	#[tokio::test]
	async fn unsupported_stats_predicate_falls_through_to_full_scan() {
		let l2 = Arc::new(MockObjectStore::default());
		let (mgr, descriptor) =
			tier_typed_table_batch_with_stats(l2, vec![1, 2], "stats-or-scan.wnd").await;

		let mut bytecode = Vec::new();
		bytecode.push(OP_PUSH_COL);
		bytecode.extend_from_slice(&0_u32.to_le_bytes());
		bytecode.push(OP_PUSH_INT);
		bytecode.extend_from_slice(&3_i64.to_le_bytes());
		bytecode.push(OP_GTE);
		bytecode.push(OP_PUSH_COL);
		bytecode.extend_from_slice(&0_u32.to_le_bytes());
		bytecode.push(OP_PUSH_INT);
		bytecode.extend_from_slice(&2_i64.to_le_bytes());
		bytecode.push(OP_EQ);
		bytecode.push(OP_OR);
		let program = pbc::Program::compile(&bytecode).unwrap();
		let plan = plan_query_execution_for_ranges_with_program(
			&mgr,
			n(1),
			&base_meta(),
			&program,
			&[QueryScanRange {
				start: Locality::ZERO,
				end: Locality::MAX,
			}],
			QueryExecutionBudget::default(),
			Some(&descriptor),
			QueryProjectedExecution::Disabled,
			None,
		)
		.await
		.unwrap();

		assert_eq!(plan.response.status, ReadStatus::OK);
		assert_eq!(plan.response.rows, 1);
		assert_eq!(
			plan.stats_explain,
			QueryStatsExplainCounters {
				candidate_slabs_before_stats: 1,
				slabs_pruned_by_stats: 0,
				slabs_scanned_stats_missing: 0,
				slabs_scanned_stats_unsupported: 0,
				slabs_scanned_predicate_unrepresentable: 1,
				stats_cache_hits: 1,
				stats_cache_misses: 1,
				stats_decrypt_failed_total: 0,
			}
		);
		let ids = plan.batches[0]
			.column_by_name("id")
			.unwrap()
			.as_any()
			.downcast_ref::<Int64Array>()
			.unwrap();
		assert_eq!(ids.values(), &[2]);
	}

	#[test]
	fn slab_stats_pruning_ignores_mismatched_arrow_type_tag() {
		use crate::storage::stats::{
			ARROW_STATS_TAG_LARGE_UTF8, COLUMN_STAT_HAS_MAX, COLUMN_STAT_HAS_MIN,
			COLUMN_STAT_HAS_NON_NULL_COUNT, COLUMN_STAT_HAS_NULL_COUNT, ColumnStatsBlockEntry,
			ColumnStatsBlockHeader, ParsedColumnStatsBlock, encode_length_prefixed_bytes,
		};

		let descriptor = typed_table_descriptor();
		let mut payload = Vec::new();
		encode_length_prefixed_bytes(&mut payload, &encode_signed_stat_value(1, 8).unwrap())
			.unwrap();
		encode_length_prefixed_bytes(&mut payload, &encode_signed_stat_value(9, 8).unwrap())
			.unwrap();
		payload.extend_from_slice(&0_u64.to_be_bytes());
		payload.extend_from_slice(&2_u64.to_be_bytes());
		let stats = ParsedColumnStatsBlock {
			header: ColumnStatsBlockHeader {
				table_object_id: descriptor.table_object_id,
				schema_generation: descriptor.schema_generation,
				logical_schema_fingerprint: descriptor.logical_schema_fingerprint,
				row_count: 2,
				column_block_count: 1,
			},
			columns: vec![ColumnStatsBlockEntry {
				column_ordinal: 0,
				arrow_type_tag: ARROW_STATS_TAG_LARGE_UTF8,
				stat_flags: COLUMN_STAT_HAS_MIN
					| COLUMN_STAT_HAS_MAX
					| COLUMN_STAT_HAS_NULL_COUNT
					| COLUMN_STAT_HAS_NON_NULL_COUNT,
				payload,
			}],
		};
		let mut bytecode = Vec::new();
		bytecode.push(OP_PUSH_COL);
		bytecode.extend_from_slice(&0_u32.to_le_bytes());
		bytecode.push(OP_PUSH_INT);
		bytecode.extend_from_slice(&100_i64.to_le_bytes());
		bytecode.push(OP_GTE);
		let program = pbc::Program::compile(&bytecode).unwrap();

		assert!(
			slab_stats_may_match_query(&descriptor, &program, &stats).unwrap(),
			"mismatched stat type tags must fail closed instead of pruning"
		);
	}

	#[test]
	fn slab_stats_pruning_respects_truncated_string_max_prefix() {
		use crate::storage::stats::{
			ARROW_STATS_TAG_LARGE_UTF8, COLUMN_STAT_HAS_MAX, COLUMN_STAT_HAS_NON_NULL_COUNT,
			COLUMN_STAT_HAS_NULL_COUNT, COLUMN_STAT_MAX_TRUNCATED, ColumnStatsBlockEntry,
			ColumnStatsBlockHeader, ParsedColumnStatsBlock, encode_length_prefixed_bytes,
		};

		let descriptor = typed_table_descriptor();
		let mut payload = Vec::new();
		encode_length_prefixed_bytes(&mut payload, b"app").unwrap();
		payload.extend_from_slice(&0_u64.to_be_bytes());
		payload.extend_from_slice(&3_u64.to_be_bytes());
		let stats = ParsedColumnStatsBlock {
			header: ColumnStatsBlockHeader {
				table_object_id: descriptor.table_object_id,
				schema_generation: descriptor.schema_generation,
				logical_schema_fingerprint: descriptor.logical_schema_fingerprint,
				row_count: 3,
				column_block_count: 1,
			},
			columns: vec![ColumnStatsBlockEntry {
				column_ordinal: 1,
				arrow_type_tag: ARROW_STATS_TAG_LARGE_UTF8,
				stat_flags: COLUMN_STAT_HAS_MAX
					| COLUMN_STAT_MAX_TRUNCATED
					| COLUMN_STAT_HAS_NULL_COUNT
					| COLUMN_STAT_HAS_NON_NULL_COUNT,
				payload,
			}],
		};

		let mut boundary = Vec::new();
		boundary.push(OP_PUSH_COL);
		boundary.extend_from_slice(&1_u32.to_le_bytes());
		boundary.push(OP_PUSH_STR);
		boundary.extend_from_slice(&5_u16.to_le_bytes());
		boundary.extend_from_slice(b"apple");
		boundary.push(OP_GTE);
		let boundary_program = pbc::Program::compile(&boundary).unwrap();
		assert!(
			slab_stats_may_match_query(&descriptor, &boundary_program, &stats).unwrap(),
			"truncated max prefix 'app' must not prune predicate value 'apple'"
		);

		let mut outside = Vec::new();
		outside.push(OP_PUSH_COL);
		outside.extend_from_slice(&1_u32.to_le_bytes());
		outside.push(OP_PUSH_STR);
		outside.extend_from_slice(&3_u16.to_le_bytes());
		outside.extend_from_slice(b"apq");
		outside.push(OP_GTE);
		let outside_program = pbc::Program::compile(&outside).unwrap();
		assert!(
			!slab_stats_may_match_query(&descriptor, &outside_program, &stats).unwrap(),
			"values at or beyond the prefix successor can still prune safely"
		);
	}

	#[test]
	fn table_bifurcation_rejects_duplicate_pk_before_encoding_children() {
		let descriptor = typed_table_descriptor();
		let batch = typed_table_batch(vec![7, 7]);
		let meta = typed_table_meta(&descriptor, batch.num_rows());
		let err = bifurcate_table_write(
			&meta,
			&batch,
			&descriptor,
			WriterNodeId::ZERO,
			meta.client_hlc,
		)
		.unwrap_err();
		assert_eq!(err.code, PbcRejectCode::PBC_REJECT_DUPLICATE_PK_IN_BATCH);
	}

	#[tokio::test]
	async fn table_write_single_row_commits_typed_arrow_payload() {
		let dir = bootstrap_tempdir("table-write-single-row");
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			37,
			dir,
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();
		let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
		loop {
			if node.raft.current_leader().await == Some(node.local_node.id) {
				break;
			}
			assert!(
				tokio::time::Instant::now() < deadline,
				"timed out waiting for leader"
			);
			tokio::time::sleep(Duration::from_millis(20)).await;
		}
		node.data_server
			.refresh_leases_once(&node.ring, &*node.clock, 60_000)
			.await;

		let descriptor = typed_table_descriptor();
		node.raft
			.client_write(WendelonRequest::SchemaUpdate(typed_table_schema_update(
				&descriptor,
			)))
			.await
			.unwrap();

		let batch = typed_table_batch(vec![7]);
		let meta = typed_table_meta(&descriptor, batch.num_rows());
		let (resp, trailing) = node.data_server.table_write(meta, batch).await.unwrap();
		assert_eq!(resp.write.status, WriteStatus::OK, "got resp={resp:?}");
		assert_eq!(resp.reject_code, PbcRejectCode::NONE.0);
		assert!(trailing.is_none());

		let key_identity =
			table_row_identity(tenant(), descriptor.table_object_id, &7_i64.to_le_bytes());
		let location = node
			.slab
			.lookup(tenant(), key_identity)
			.expect("table row should be indexed by PK-derived identity");
		let stored = decode_batch(&node.slab.read(location).await.unwrap()).unwrap();
		assert_eq!(stored.schema().as_ref(), &typed_table_schema());
		assert_eq!(stored.num_rows(), 1);
		let ids = stored
			.column_by_name("id")
			.unwrap()
			.as_any()
			.downcast_ref::<Int64Array>()
			.unwrap();
		assert_eq!(ids.value(0), 7);

		node.handles.abort();
	}

	#[tokio::test]
	async fn content_hash_table_write_absorbs_visible_duplicate_content() {
		let dir = bootstrap_tempdir("content-hash-table-write-duplicate");
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			49,
			dir,
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();
		let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
		loop {
			if node.raft.current_leader().await == Some(node.local_node.id) {
				break;
			}
			assert!(
				tokio::time::Instant::now() < deadline,
				"timed out waiting for leader"
			);
			tokio::time::sleep(Duration::from_millis(20)).await;
		}
		node.data_server
			.refresh_leases_once(&node.ring, &*node.clock, 60_000)
			.await;

		let descriptor = content_hash_table_descriptor();
		node.raft
			.client_write(WendelonRequest::SchemaUpdate(typed_table_schema_update(
				&descriptor,
			)))
			.await
			.unwrap();

		let batch = typed_table_batch(vec![7]);
		let mut first_meta = typed_table_meta(&descriptor, batch.num_rows());
		first_meta.parent_write_id.sequence_no = 490;
		let admission = validate_table_write_request_batch(
			&first_meta,
			&batch,
			&descriptor,
			WriterNodeId::ZERO,
			first_meta.client_hlc,
		)
		.unwrap();
		let key_identity = admission.key_identities[0];
		assert_eq!(admission.localities[0], Locality::from(key_identity));

		let (first_resp, first_trailing) = node
			.data_server
			.table_write(first_meta, batch.clone())
			.await
			.unwrap();
		assert_eq!(
			first_resp.write.status,
			WriteStatus::OK,
			"got resp={first_resp:?}"
		);
		assert_eq!(first_resp.reject_code, PbcRejectCode::NONE.0);
		assert!(first_trailing.is_none());
		let first_location = node
			.slab
			.lookup(tenant(), key_identity)
			.expect("content-hash row should be indexed by content-derived identity");
		assert!(!first_location.is_tombstone);

		let mut duplicate_meta = typed_table_meta(&descriptor, batch.num_rows());
		duplicate_meta.parent_write_id.sequence_no = 491;
		let (duplicate_resp, duplicate_trailing) = node
			.data_server
			.table_write(duplicate_meta, batch)
			.await
			.unwrap();
		assert_eq!(
			duplicate_resp.write.status,
			WriteStatus::OK,
			"visible duplicate content should be absorbed as idempotent success"
		);
		assert_eq!(duplicate_resp.reject_code, PbcRejectCode::NONE.0);
		assert_eq!(duplicate_resp.write.log_index, 0);
		assert!(duplicate_trailing.is_none());
		assert_eq!(
			node.slab.lookup(tenant(), key_identity),
			Some(first_location),
			"absorbed duplicate must not replace the visible row location"
		);

		node.handles.abort();
	}

	#[tokio::test]
	async fn content_hash_table_write_filters_duplicate_rows_within_batch() {
		let dir = bootstrap_tempdir("content-hash-table-write-in-batch-duplicate");
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			50,
			dir,
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();
		let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
		loop {
			if node.raft.current_leader().await == Some(node.local_node.id) {
				break;
			}
			assert!(
				tokio::time::Instant::now() < deadline,
				"timed out waiting for leader"
			);
			tokio::time::sleep(Duration::from_millis(20)).await;
		}
		node.data_server
			.refresh_leases_once(&node.ring, &*node.clock, 60_000)
			.await;

		let descriptor = content_hash_table_descriptor();
		node.raft
			.client_write(WendelonRequest::SchemaUpdate(typed_table_schema_update(
				&descriptor,
			)))
			.await
			.unwrap();

		let batch = typed_table_batch_with_names(vec![(7, "codex"), (7, "codex")]);
		let mut meta = typed_table_meta(&descriptor, batch.num_rows());
		meta.parent_write_id.sequence_no = 492;
		let admission = validate_table_write_request_batch(
			&meta,
			&batch,
			&descriptor,
			WriterNodeId::ZERO,
			meta.client_hlc,
		)
		.unwrap();
		assert_eq!(admission.key_identities[0], admission.key_identities[1]);
		let key_identity = admission.key_identities[0];

		let (resp, trailing) = node.data_server.table_write(meta, batch).await.unwrap();
		assert_eq!(resp.write.status, WriteStatus::OK, "got resp={resp:?}");
		assert_eq!(resp.reject_code, PbcRejectCode::NONE.0);
		assert!(trailing.is_none());

		let location = node
			.slab
			.lookup(tenant(), key_identity)
			.expect("content-hash row should be indexed after filtering in-batch duplicate");
		assert!(!location.is_tombstone);
		let stored = decode_batch(&node.slab.read(location).await.unwrap()).unwrap();
		assert_eq!(
			stored.num_rows(),
			1,
			"only one physical row should be packed for duplicate content within the same batch"
		);

		node.handles.abort();
	}

	#[tokio::test]
	async fn content_hash_table_reingest_after_logical_retention_floor_stores_new_row() {
		let dir = bootstrap_tempdir("content-hash-logical-retention-reingest");
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			51,
			dir,
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();
		let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
		loop {
			if node.raft.current_leader().await == Some(node.local_node.id) {
				break;
			}
			assert!(
				tokio::time::Instant::now() < deadline,
				"timed out waiting for leader"
			);
			tokio::time::sleep(Duration::from_millis(20)).await;
		}

		let descriptor = content_hash_table_descriptor();
		node.raft
			.client_write(WendelonRequest::SchemaUpdate(typed_table_schema_update(
				&descriptor,
			)))
			.await
			.unwrap();
		node.raft
			.client_write(WendelonRequest::SetTableRetentionPolicy(
				crate::consensus::SetTableRetentionPolicy {
					tenant_id: tenant(),
					table_object_id: descriptor.table_object_id,
					expected_policy_generation: PolicyGeneration::ZERO,
					new_policy: TableRetentionPolicy::FixedWindow(
						RetentionWindowMillis::try_new(60_000).unwrap(),
					),
					updated_at_hlc: hlc(1, 0, 51),
				},
			))
			.await
			.unwrap();

		let clock = Arc::new(AdjustableClock::new(1_000));
		let tiers = TransportTierSet::empty();
		let roles = NodeRoleSet::empty().with(NodeRole::Data);
		let mut peers = std::collections::HashMap::new();
		peers.insert(
			node.local_node,
			PeerCapabilities {
				address: "loopback://51".into(),
				tiers,
				roles,
				capabilities: crate::network::transport::NodeCapabilitySet::empty(),
			},
		);
		let epoch = MembershipEpoch { epoch: 1, peers };
		let (_epoch_tx, epoch_rx) = watch::channel(Arc::new(epoch));
		let resolver: Arc<dyn crate::network::transport::CapabilityResolver> = Arc::new(
			crate::network::transport::EpochCapabilityResolver::new(epoch_rx.clone()),
		);
		let factory = TransportProviderFactory::new(
			tiers,
			std::collections::HashMap::new(),
			resolver,
			epoch_rx,
		);
		let server = WendelonDataServer::new(
			node.slab.clone(),
			node.raft.clone(),
			node.log_store.clone(),
			node.state_machine.clone(),
			node.local_node,
			clock.clone(),
			Arc::new(FixedReplicaRouter::new(vec![node.local_node])),
			factory,
			node.pool.clone(),
		)
		.unwrap();

		let batch = typed_table_batch_with_names(vec![(7, "codex")]);
		let mut first_meta = typed_table_meta(&descriptor, batch.num_rows());
		first_meta.parent_write_id.sequence_no = 493;
		let admission = validate_table_write_request_batch(
			&first_meta,
			&batch,
			&descriptor,
			WriterNodeId::ZERO,
			first_meta.client_hlc,
		)
		.unwrap();
		let key_identity = admission.key_identities[0];

		let (first_resp, _) = server.table_write(first_meta, batch.clone()).await.unwrap();
		assert_eq!(first_resp.write.status, WriteStatus::OK);
		assert_ne!(
			first_resp.write.log_index, 0,
			"initial content-hash write must store a physical row"
		);
		let first_location = server
			.slab
			.lookup(tenant(), key_identity)
			.expect("first content-hash row should be indexed");
		assert!(!first_location.is_tombstone);

		clock.set(121_000);
		let state = server.state_machine.state_snapshot().await;
		let effective_retention = server.table_effective_retention_from_state(
			&state,
			tenant(),
			descriptor.table_object_id,
		);
		assert!(
			!row_visible_for_admission(
				&descriptor,
				tenant(),
				key_identity,
				first_location.hlc,
				&effective_retention,
				&server.slab,
			),
			"old row must be logically hidden before compaction removes the index entry"
		);
		assert_eq!(
			server.slab.lookup(tenant(), key_identity),
			Some(first_location),
			"regression setup requires the expired row to remain physically indexed"
		);

		let mut reingest_meta = typed_table_meta(&descriptor, batch.num_rows());
		reingest_meta.parent_write_id.sequence_no = 494;
		let (reingest_resp, _) = server
			.table_write(reingest_meta, batch.clone())
			.await
			.unwrap();
		assert_eq!(reingest_resp.write.status, WriteStatus::OK);
		assert_ne!(
			reingest_resp.write.log_index, 0,
			"logically expired content must take the store path, not absorb"
		);
		let reingest_location = server
			.slab
			.lookup(tenant(), key_identity)
			.expect("reingested content-hash row should be indexed");
		assert_ne!(
			reingest_location, first_location,
			"reingest after logical retention must write a fresh physical row"
		);
		assert!(!reingest_location.is_tombstone);
		assert!(reingest_location.hlc > first_location.hlc);

		let mut duplicate_meta = typed_table_meta(&descriptor, batch.num_rows());
		duplicate_meta.parent_write_id.sequence_no = 495;
		let (duplicate_resp, _) = server.table_write(duplicate_meta, batch).await.unwrap();
		assert_eq!(duplicate_resp.write.status, WriteStatus::OK);
		assert_eq!(
			duplicate_resp.write.log_index, 0,
			"freshly visible reingested content should absorb subsequent duplicates"
		);
		assert_eq!(
			server.slab.lookup(tenant(), key_identity),
			Some(reingest_location)
		);

		node.handles.abort();
	}

	#[tokio::test]
	async fn column_projection_write_preserves_slab_record_header_layout() {
		let dir = bootstrap_tempdir("column-projection-slab-header");
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			38,
			dir.clone(),
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();
		let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
		loop {
			if node.raft.current_leader().await == Some(node.local_node.id) {
				break;
			}
			assert!(
				tokio::time::Instant::now() < deadline,
				"timed out waiting for leader"
			);
			tokio::time::sleep(Duration::from_millis(20)).await;
		}
		node.data_server
			.refresh_leases_once(&node.ring, &*node.clock, 60_000)
			.await;

		let descriptor = column_projection_table_descriptor();
		node.raft
			.client_write(WendelonRequest::SchemaUpdate(typed_table_schema_update(
				&descriptor,
			)))
			.await
			.unwrap();

		let batch = typed_table_batch(vec![7]);
		let meta = typed_table_meta(&descriptor, batch.num_rows());
		let (resp, trailing) = node.data_server.table_write(meta, batch).await.unwrap();
		assert_eq!(resp.write.status, WriteStatus::OK, "got resp={resp:?}");
		assert_eq!(resp.reject_code, PbcRejectCode::NONE.0);
		assert!(trailing.is_none());

		let key_identity =
			table_row_identity(tenant(), descriptor.table_object_id, &7_i64.to_le_bytes());
		let location = node
			.slab
			.lookup(tenant(), key_identity)
			.expect("table row should be indexed by PK-derived identity");
		let slab_path = dir.join(format!("slab_{:016x}.wnd", location.slab_id.get()));
		let mut file = std::fs::File::open(slab_path).unwrap();
		file.seek(SeekFrom::Start(location.offset)).unwrap();
		let mut header_bytes = vec![0_u8; std::mem::size_of::<SlabRecordHeader>()];
		file.read_exact(&mut header_bytes).unwrap();
		let header = bytemuck::pod_read_unaligned::<SlabRecordHeader>(&header_bytes);

		assert_eq!(std::mem::size_of::<SlabRecordHeader>(), 72);
		assert_eq!(header._pad_align, 0);
		assert_eq!(header.tenant_id, tenant());
		assert_eq!(header.key_identity, key_identity);

		node.handles.abort();
	}

	#[tokio::test]
	async fn table_write_rejects_stale_schema_generation_distinct_from_unknown() {
		let dir = bootstrap_tempdir("table-write-stale-schema");
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			40,
			dir,
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();
		let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
		loop {
			if node.raft.current_leader().await == Some(node.local_node.id) {
				break;
			}
			assert!(
				tokio::time::Instant::now() < deadline,
				"timed out waiting for leader"
			);
			tokio::time::sleep(Duration::from_millis(20)).await;
		}
		node.data_server
			.refresh_leases_once(&node.ring, &*node.clock, 60_000)
			.await;

		let descriptor = typed_table_descriptor();
		node.raft
			.client_write(WendelonRequest::SchemaUpdate(typed_table_schema_update(
				&descriptor,
			)))
			.await
			.unwrap();

		for (sequence_no, schema_generation) in [
			(58_u64, descriptor.schema_generation - 1),
			(59_u64, descriptor.schema_generation + 1),
		] {
			let batch = typed_table_batch(vec![7]);
			let mut meta = typed_table_meta(&descriptor, batch.num_rows());
			meta.parent_write_id.sequence_no = sequence_no;
			meta.schema_generation = schema_generation;
			let (resp, trailing) = node.data_server.table_write(meta, batch).await.unwrap();
			assert_eq!(resp.write.status, WriteStatus::EPOCH_STALE);
			assert_eq!(resp.reject_code, PbcRejectCode::PBC_REJECT_SCHEMA_STALE.0);
			assert!(trailing.is_none());
		}

		node.handles.abort();
	}

	#[tokio::test]
	async fn pk_table_delete_supersedes_single_row_and_allows_resurrection() {
		let dir = bootstrap_tempdir("pk-delete-single-row-supersession");
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			52,
			dir,
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();
		let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
		loop {
			if node.raft.current_leader().await == Some(node.local_node.id) {
				break;
			}
			assert!(
				tokio::time::Instant::now() < deadline,
				"timed out waiting for leader"
			);
			tokio::time::sleep(Duration::from_millis(20)).await;
		}
		node.data_server
			.refresh_leases_once(&node.ring, &*node.clock, 60_000)
			.await;

		let descriptor = typed_table_descriptor();
		node.raft
			.client_write(WendelonRequest::SchemaUpdate(typed_table_schema_update(
				&descriptor,
			)))
			.await
			.unwrap();

		let first_batch = typed_table_batch_with_names(vec![(7, "before-delete")]);
		let mut first_meta = typed_table_meta(&descriptor, first_batch.num_rows());
		first_meta.parent_write_id.sequence_no = 500;
		let (first_resp, _) = node
			.data_server
			.table_write(first_meta, first_batch.clone())
			.await
			.unwrap();
		assert_eq!(first_resp.write.status, WriteStatus::OK);
		let first_admission = validate_table_write_request_batch(
			&first_meta,
			&first_batch,
			&descriptor,
			WriterNodeId::from_node_id(node.local_node),
			first_resp.write.commit_hlc,
		)
		.unwrap();
		let target = first_admission.key_identities[0];
		let missing = Identity::new([0x52; 32]);
		let first_location = node.slab.lookup(tenant(), target).unwrap();
		assert!(!first_location.is_tombstone);

		let mut delete_meta: TableDeleteRowsRequestMeta = bytemuck::Zeroable::zeroed();
		delete_meta.parent_write_id = crate::schema::ClientWriteId {
			client_uuid: [0x52; 16],
			sequence_no: 1,
			idempotency_ttl_ms: 30_000,
			_pad: 0,
		};
		delete_meta.client_hlc = hlc(20, 0, 52);
		delete_meta.expected_epoch = 1;
		delete_meta.tenant_id = tenant();
		delete_meta.table_object_id = descriptor.table_object_id;
		delete_meta.schema_generation = descriptor.schema_generation;
		delete_meta.row_count = 2;
		delete_meta.durability = DurabilityLevel::QUORUM;
		let (delete_resp, trailing) = node
			.data_server
			.table_delete(delete_meta, vec![target, missing])
			.await
			.unwrap();
		assert_eq!(delete_resp.write.status, WriteStatus::OK);
		assert_eq!(delete_resp.reject_code, PbcRejectCode::NONE.0);
		assert_eq!(delete_resp.deleted_count, 1);
		assert_eq!(delete_resp.already_deleted_count, 0);
		assert_eq!(delete_resp.not_found_count, 1);
		assert!(trailing.is_none());

		let tombstone_location = node.slab.lookup(tenant(), target).unwrap();
		assert!(tombstone_location.is_tombstone);
		assert_eq!(tombstone_location.hlc, delete_resp.write.commit_hlc);
		let tombstone_frame = node.slab.read(tombstone_location).await.unwrap();
		let tombstone_batch = decode_batch(&tombstone_frame).unwrap();
		assert!(
			crate::storage::multirow::storage_row_is_tombstone_at(
				&tombstone_batch,
				tombstone_location.record_row_index,
			)
			.unwrap()
		);
		assert_eq!(
			storage_row_table_object_id_at(&tombstone_batch, tombstone_location.record_row_index)
				.unwrap(),
			Some(descriptor.table_object_id)
		);
		assert_eq!(
			materialize_record_location_batch(
				&tombstone_batch,
				tombstone_location.record_row_index
			)
			.unwrap()
			.num_rows(),
			0
		);
		assert!(
			!node
				.slab
				.lookup_as_of(tenant(), target, first_resp.write.commit_hlc)
				.unwrap()
				.is_tombstone
		);

		let mut already_meta = delete_meta;
		already_meta.parent_write_id.sequence_no = 2;
		already_meta.row_count = 1;
		let (already_resp, _) = node
			.data_server
			.table_delete(already_meta, vec![target])
			.await
			.unwrap();
		assert_eq!(already_resp.deleted_count, 0);
		assert_eq!(already_resp.already_deleted_count, 1);
		assert_eq!(already_resp.not_found_count, 0);

		let resurrection_batch = typed_table_batch_with_names(vec![(7, "after-delete")]);
		let mut resurrection_meta = typed_table_meta(&descriptor, resurrection_batch.num_rows());
		resurrection_meta.parent_write_id.sequence_no = 501;
		let (resurrection_resp, _) = node
			.data_server
			.table_write(resurrection_meta, resurrection_batch)
			.await
			.unwrap();
		assert_eq!(resurrection_resp.write.status, WriteStatus::OK);
		let latest_location = node.slab.lookup(tenant(), target).unwrap();
		assert!(!latest_location.is_tombstone);
		assert!(latest_location.hlc > tombstone_location.hlc);
		assert!(
			node.slab
				.slab_has_supersession_at(tombstone_location.slab_id, latest_location.hlc),
			"resurrection must mark the tombstone slab as superseded for CSB safety"
		);
		let latest_frame = node.slab.read(latest_location).await.unwrap();
		let latest_batch = decode_batch(&latest_frame).unwrap();
		let materialized =
			materialize_record_location_batch(&latest_batch, latest_location.record_row_index)
				.unwrap();
		let names = materialized
			.column_by_name("name")
			.unwrap()
			.as_any()
			.downcast_ref::<LargeStringArray>()
			.unwrap();
		assert_eq!(names.value(0), "after-delete");

		node.handles.abort();
	}

	#[tokio::test]
	async fn pk_delete_read_surfaces_track_tombstone_and_resurrection() {
		let dir = bootstrap_tempdir("pk-delete-read-surfaces");
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			53,
			dir,
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();
		let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
		loop {
			if node.raft.current_leader().await == Some(node.local_node.id) {
				break;
			}
			assert!(
				tokio::time::Instant::now() < deadline,
				"timed out waiting for leader"
			);
			tokio::time::sleep(Duration::from_millis(20)).await;
		}
		node.data_server
			.refresh_leases_once(&node.ring, &*node.clock, 60_000)
			.await;

		let descriptor = typed_table_descriptor();
		node.raft
			.client_write(WendelonRequest::SchemaUpdate(typed_table_schema_update(
				&descriptor,
			)))
			.await
			.unwrap();

		let initial = typed_table_batch_with_names(vec![(1, "keep"), (2, "delete-me")]);
		let mut initial_meta = typed_table_meta(&descriptor, initial.num_rows());
		initial_meta.parent_write_id.sequence_no = 530;
		let (initial_resp, _) = node
			.data_server
			.table_write(initial_meta, initial.clone())
			.await
			.unwrap();
		assert_eq!(initial_resp.write.status, WriteStatus::OK);
		let admission = validate_table_write_request_batch(
			&initial_meta,
			&initial,
			&descriptor,
			WriterNodeId::from_node_id(node.local_node),
			initial_resp.write.commit_hlc,
		)
		.unwrap();
		let keep_key = admission.key_identities[0];
		let deleted_key = admission.key_identities[1];

		let mut delete_meta: TableDeleteRowsRequestMeta = bytemuck::Zeroable::zeroed();
		delete_meta.parent_write_id = crate::schema::ClientWriteId {
			client_uuid: [0x53; 16],
			sequence_no: 1,
			idempotency_ttl_ms: 30_000,
			_pad: 0,
		};
		delete_meta.client_hlc = hlc(20, 0, 53);
		delete_meta.expected_epoch = 1;
		delete_meta.tenant_id = tenant();
		delete_meta.table_object_id = descriptor.table_object_id;
		delete_meta.schema_generation = descriptor.schema_generation;
		delete_meta.row_count = 1;
		delete_meta.durability = DurabilityLevel::QUORUM;
		let (delete_resp, _) = node
			.data_server
			.table_delete(delete_meta, vec![deleted_key])
			.await
			.unwrap();
		assert_eq!(delete_resp.write.status, WriteStatus::OK);
		assert_eq!(delete_resp.deleted_count, 1);

		let pre_delete = node
			.slab
			.lookup_as_of(tenant(), deleted_key, initial_resp.write.commit_hlc)
			.expect("deleted key should have a data version before delete");
		assert!(!pre_delete.is_tombstone);
		let at_delete = node
			.slab
			.lookup_as_of(tenant(), deleted_key, delete_resp.write.commit_hlc)
			.expect("deleted key should have a tombstone at delete HLC");
		assert!(at_delete.is_tombstone);
		let scan_at_delete =
			node.slab
				.scan_as_of_with_identity(tenant(), 0, u64::MAX, delete_resp.write.commit_hlc);
		assert!(scan_at_delete.iter().any(|(key, _)| *key == keep_key));
		assert!(!scan_at_delete.iter().any(|(key, _)| *key == deleted_key));
		let versions = node.slab.scan_all_versions(0, u64::MAX, hlc(0, 0, 0));
		assert!(
			versions
				.iter()
				.any(|(_, key, _, location)| *key == deleted_key && !location.is_tombstone)
		);
		assert!(
			versions
				.iter()
				.any(|(_, key, _, location)| *key == deleted_key && location.is_tombstone)
		);

		let pk_program = pk_eq_program(2);
		let deleted_query_meta =
			table_query_meta_for_test(&descriptor, delete_resp.write.commit_hlc, pk_program.len());
		let deleted_query = node
			.data_server
			.prepare_table_query_execution_plan_with_migration(&deleted_query_meta, &pk_program)
			.await
			.unwrap();
		assert_eq!(deleted_query.response.status, ReadStatus::KEY_NOT_FOUND);
		assert_eq!(deleted_query.response.rows, 0);
		let delete_count = node
			.data_server
			.execute_aggregate_local(&aggregate_count_request(
				&descriptor,
				530,
				delete_resp.write.commit_hlc,
			))
			.await
			.unwrap();
		assert_eq!(delete_count.status, AggregateStatus::Success);
		assert_eq!(count_star_partial_value(&delete_count), 1);

		let resurrection = typed_table_batch_with_names(vec![(2, "back")]);
		let mut resurrection_meta = typed_table_meta(&descriptor, resurrection.num_rows());
		resurrection_meta.parent_write_id.sequence_no = 531;
		let (resurrection_resp, _) = node
			.data_server
			.table_write(resurrection_meta, resurrection)
			.await
			.unwrap();
		assert_eq!(resurrection_resp.write.status, WriteStatus::OK);
		let latest = node
			.slab
			.lookup_as_of(tenant(), deleted_key, resurrection_resp.write.commit_hlc)
			.expect("resurrected key should have a data version");
		assert!(!latest.is_tombstone);

		let latest_query_meta = table_query_meta_for_test(
			&descriptor,
			resurrection_resp.write.commit_hlc,
			pk_program.len(),
		);
		let latest_query = node
			.data_server
			.prepare_table_query_execution_plan_with_migration(&latest_query_meta, &pk_program)
			.await
			.unwrap();
		assert_eq!(latest_query.response.status, ReadStatus::OK);
		assert_eq!(latest_query.response.rows, 1);
		let materialized = concat_batches(&Arc::new(typed_table_schema()), &latest_query.batches)
			.expect("query batches concatenate");
		let names = materialized
			.column_by_name("name")
			.unwrap()
			.as_any()
			.downcast_ref::<LargeStringArray>()
			.unwrap();
		assert_eq!(names.value(0), "back");
		let resurrected_count = node
			.data_server
			.execute_aggregate_local(&aggregate_count_request(
				&descriptor,
				531,
				resurrection_resp.write.commit_hlc,
			))
			.await
			.unwrap();
		assert_eq!(resurrected_count.status, AggregateStatus::Success);
		assert_eq!(count_star_partial_value(&resurrected_count), 2);

		node.handles.abort();
	}

	#[tokio::test]
	async fn table_query_plan_carries_ready_secondary_index_access_plan() {
		let dir = bootstrap_tempdir("table-query-secondary-index-plan");
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			54,
			dir,
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();
		let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
		loop {
			if node.raft.current_leader().await == Some(node.local_node.id) {
				break;
			}
			assert!(
				tokio::time::Instant::now() < deadline,
				"timed out waiting for leader"
			);
			tokio::time::sleep(Duration::from_millis(20)).await;
		}
		node.data_server
			.refresh_leases_once(&node.ring, &*node.clock, 60_000)
			.await;

		let descriptor = typed_table_descriptor();
		node.raft
			.client_write(WendelonRequest::SchemaUpdate(typed_table_schema_update(
				&descriptor,
			)))
			.await
			.unwrap();
		let index = name_secondary_index_descriptor(&descriptor);
		node.raft
			.client_write(WendelonRequest::SchemaUpdate(
				secondary_index_schema_update(&index),
			))
			.await
			.unwrap();

		let program = name_eq_program("target");
		let query_meta = table_query_meta_for_test(&descriptor, hlc(0, 0, 0), program.len());
		let building_plan = node
			.data_server
			.prepare_table_query_execution_plan_with_migration(&query_meta, &program)
			.await
			.unwrap();
		assert_eq!(building_plan.response.status, ReadStatus::KEY_NOT_FOUND);
		assert!(building_plan.secondary_index_access_plan.is_none());

		node.raft
			.client_write(WendelonRequest::SchemaUpdate(
				secondary_index_backfill_complete_schema_update(&index, node.local_node),
			))
			.await
			.unwrap();
		let ready_plan = node
			.data_server
			.prepare_table_query_execution_plan_with_migration(&query_meta, &program)
			.await
			.unwrap();
		assert_eq!(ready_plan.response.status, ReadStatus::KEY_NOT_FOUND);
		let access = ready_plan
			.secondary_index_access_plan
			.expect("Ready index should be selected for the name predicate");
		assert_eq!(access.descriptor, index);
		assert_eq!(
			access.query,
			IndexQuery::PointEqual {
				encoded_key: b"target".to_vec(),
			}
		);

		node.handles.abort();
	}

	#[tokio::test]
	async fn table_write_populates_building_secondary_index_active_buffer() {
		let dir = bootstrap_tempdir("table-write-secondary-index-active-buffer");
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			56,
			dir,
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();
		let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
		loop {
			if node.raft.current_leader().await == Some(node.local_node.id) {
				break;
			}
			assert!(
				tokio::time::Instant::now() < deadline,
				"timed out waiting for leader"
			);
			tokio::time::sleep(Duration::from_millis(20)).await;
		}
		node.data_server
			.refresh_leases_once(&node.ring, &*node.clock, 60_000)
			.await;

		let descriptor = typed_table_descriptor();
		node.raft
			.client_write(WendelonRequest::SchemaUpdate(typed_table_schema_update(
				&descriptor,
			)))
			.await
			.unwrap();
		let index = name_secondary_index_descriptor(&descriptor);
		node.raft
			.client_write(WendelonRequest::SchemaUpdate(
				secondary_index_schema_update(&index),
			))
			.await
			.unwrap();

		let active_slab_id = node.slab.active_slab_id().await;
		let batch = typed_table_batch_with_names(vec![(1, "target")]);
		let mut write_meta = typed_table_meta(&descriptor, batch.num_rows());
		write_meta.parent_write_id.sequence_no = 560;
		let (write_resp, _) = node
			.data_server
			.table_write(write_meta, batch)
			.await
			.unwrap();
		assert_eq!(write_resp.write.status, WriteStatus::OK);

		let coverage_key = SecondaryIndexCoverageKey::from(
			SecondaryIndexIdentity::from_descriptor(tenant(), &index),
		);
		let coverage = node
			.slab
			.secondary_index_coverage_snapshot(coverage_key)
			.expect("secondary index coverage should exist after indexed write");
		let active = coverage
			.active_buffer
			.expect("Building index should receive active-buffer entries");
		assert_eq!(active.source_slab_id, active_slab_id);
		assert_eq!(active.entry_count, 1);
		assert!(coverage.uncovered_slabs.is_empty());

		let info = node
			.slab
			.seal_secondary_index_active_buffer(tenant(), &index)
			.await
			.unwrap()
			.expect("active buffer should seal into a sidecar");
		assert_eq!(info.entry_count, 1);
		let segment = node
			.slab
			.load_secondary_index_segment(&info)
			.await
			.unwrap()
			.unwrap();
		assert_eq!(segment.entries[0].encoded_key, b"target".to_vec());

		node.handles.abort();
	}

	#[tokio::test]
	async fn table_write_seals_secondary_index_active_buffer_after_slab_rotation() {
		let dir = bootstrap_tempdir("table-write-secondary-index-active-buffer-seal");
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			57,
			dir,
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();
		let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
		loop {
			if node.raft.current_leader().await == Some(node.local_node.id) {
				break;
			}
			assert!(
				tokio::time::Instant::now() < deadline,
				"timed out waiting for leader"
			);
			tokio::time::sleep(Duration::from_millis(20)).await;
		}
		node.data_server
			.refresh_leases_once(&node.ring, &*node.clock, 60_000)
			.await;

		let descriptor = typed_table_descriptor();
		node.raft
			.client_write(WendelonRequest::SchemaUpdate(typed_table_schema_update(
				&descriptor,
			)))
			.await
			.unwrap();
		let index = name_secondary_index_descriptor(&descriptor);
		node.raft
			.client_write(WendelonRequest::SchemaUpdate(
				secondary_index_schema_update(&index),
			))
			.await
			.unwrap();

		let first_slab_id = node.slab.active_slab_id().await;
		let first_batch = typed_table_batch_with_names(vec![(1, "first")]);
		let mut first_meta = typed_table_meta(&descriptor, first_batch.num_rows());
		first_meta.parent_write_id.sequence_no = 570;
		let (first_resp, _) = node
			.data_server
			.table_write(first_meta, first_batch)
			.await
			.unwrap();
		assert_eq!(first_resp.write.status, WriteStatus::OK);
		assert_eq!(node.slab.force_seal_active_for_test().await, first_slab_id);
		let second_slab_id = node.slab.active_slab_id().await;

		let second_batch = typed_table_batch_with_names(vec![(2, "second")]);
		let mut second_meta = typed_table_meta(&descriptor, second_batch.num_rows());
		second_meta.parent_write_id.sequence_no = 571;
		let (second_resp, _) = node
			.data_server
			.table_write(second_meta, second_batch)
			.await
			.unwrap();
		assert_eq!(second_resp.write.status, WriteStatus::OK);

		let coverage_key = SecondaryIndexCoverageKey::from(
			SecondaryIndexIdentity::from_descriptor(tenant(), &index),
		);
		let coverage = node
			.slab
			.secondary_index_coverage_snapshot(coverage_key)
			.expect("secondary index coverage should exist after indexed writes");
		assert_eq!(coverage.covered_slabs, vec![first_slab_id]);
		let active = coverage
			.active_buffer
			.expect("new active slab should receive its own active buffer");
		assert_eq!(active.source_slab_id, second_slab_id);
		assert_eq!(active.entry_count, 1);
		assert!(coverage.uncovered_slabs.is_empty());

		let segments = node.slab.list_secondary_index_segments(
			tenant(),
			index.table_object_id,
			index.index_object_id,
			index.logical_schema_fingerprint,
		);
		assert_eq!(segments.len(), 1);
		assert_eq!(segments[0].source_slab_id, first_slab_id);
		let segment = node
			.slab
			.load_secondary_index_segment(&segments[0])
			.await
			.unwrap()
			.unwrap();
		assert_eq!(segment.entries.len(), 1);
		assert_eq!(segment.entries[0].encoded_key, b"first".to_vec());

		node.handles.abort();
	}

	#[tokio::test]
	async fn table_query_secondary_index_execution_returns_sidecar_rows() {
		let dir = bootstrap_tempdir("table-query-secondary-index-execution");
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			55,
			dir,
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();
		let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
		loop {
			if node.raft.current_leader().await == Some(node.local_node.id) {
				break;
			}
			assert!(
				tokio::time::Instant::now() < deadline,
				"timed out waiting for leader"
			);
			tokio::time::sleep(Duration::from_millis(20)).await;
		}
		node.data_server
			.refresh_leases_once(&node.ring, &*node.clock, 60_000)
			.await;

		let descriptor = typed_table_descriptor();
		node.raft
			.client_write(WendelonRequest::SchemaUpdate(typed_table_schema_update(
				&descriptor,
			)))
			.await
			.unwrap();

		let batch = typed_table_batch_with_names(vec![(1, "target"), (2, "other")]);
		let mut write_meta = typed_table_meta(&descriptor, batch.num_rows());
		write_meta.parent_write_id.sequence_no = 550;
		let (write_resp, _) = node
			.data_server
			.table_write(write_meta, batch)
			.await
			.unwrap();
		assert_eq!(write_resp.write.status, WriteStatus::OK);
		let sealed_slab_id = node.slab.force_seal_active_for_test().await;

		let index = name_secondary_index_descriptor(&descriptor);
		node.raft
			.client_write(WendelonRequest::SchemaUpdate(
				secondary_index_schema_update(&index),
			))
			.await
			.unwrap();
		let backfill = node
			.slab
			.run_secondary_index_backfill_once(tenant(), &descriptor, &index, 8, 4096)
			.await
			.unwrap();
		assert_eq!(backfill.completed.len(), 1);
		assert_eq!(
			backfill.completed[0].candidate.source_slab_id,
			sealed_slab_id
		);
		assert!(backfill.historical_complete);
		node.raft
			.client_write(WendelonRequest::SchemaUpdate(
				secondary_index_backfill_complete_schema_update(&index, node.local_node),
			))
			.await
			.unwrap();

		let program = name_eq_program("target");
		let query_meta =
			table_query_meta_for_test(&descriptor, write_resp.write.commit_hlc, program.len());
		let plan = node
			.data_server
			.prepare_table_query_execution_plan_with_migration(&query_meta, &program)
			.await
			.unwrap();
		assert_eq!(plan.response.status, ReadStatus::OK);
		assert_eq!(plan.response.rows, 1);
		assert!(plan.secondary_index_access_plan.is_some());
		let materialized = concat_batches(&Arc::new(typed_table_schema()), &plan.batches)
			.expect("query batches concatenate");
		let ids = materialized
			.column_by_name("id")
			.unwrap()
			.as_any()
			.downcast_ref::<Int64Array>()
			.unwrap();
		let names = materialized
			.column_by_name("name")
			.unwrap()
			.as_any()
			.downcast_ref::<LargeStringArray>()
			.unwrap();
		assert_eq!(ids.value(0), 1);
		assert_eq!(names.value(0), "target");

		node.handles.abort();
	}

	#[tokio::test]
	async fn table_delete_emits_tombstone_and_replays_counts() {
		let dir = bootstrap_tempdir("table-delete-row-tombstone");
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			48,
			dir,
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();
		let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
		loop {
			if node.raft.current_leader().await == Some(node.local_node.id) {
				break;
			}
			assert!(
				tokio::time::Instant::now() < deadline,
				"timed out waiting for leader"
			);
			tokio::time::sleep(Duration::from_millis(20)).await;
		}
		node.data_server
			.refresh_leases_once(&node.ring, &*node.clock, 60_000)
			.await;

		let descriptor = append_only_table_descriptor();
		node.raft
			.client_write(WendelonRequest::SchemaUpdate(typed_table_schema_update(
				&descriptor,
			)))
			.await
			.unwrap();

		let batch = typed_table_batch(vec![7, 8, 9]);
		let mut write_meta = typed_table_meta(&descriptor, batch.num_rows());
		write_meta.parent_write_id.sequence_no = 470;
		let (write_resp, _) = node
			.data_server
			.table_write(write_meta, batch.clone())
			.await
			.unwrap();
		assert_eq!(write_resp.write.status, WriteStatus::OK);
		assert_eq!(write_resp.reject_code, PbcRejectCode::NONE.0);
		let admission = validate_table_write_request_batch(
			&write_meta,
			&batch,
			&descriptor,
			WriterNodeId::from_node_id(node.local_node),
			write_resp.write.commit_hlc,
		)
		.unwrap();
		let target = admission.key_identities[1];
		let missing = Identity::new([0xEE; 32]);
		let original_location = node.slab.lookup(tenant(), target).unwrap();
		assert!(!original_location.is_tombstone);

		let mut delete_meta: TableDeleteRowsRequestMeta = bytemuck::Zeroable::zeroed();
		delete_meta.parent_write_id = crate::schema::ClientWriteId {
			client_uuid: [0x47; 16],
			sequence_no: 1,
			idempotency_ttl_ms: 30_000,
			_pad: 0,
		};
		delete_meta.client_hlc = hlc(10, 0, 48);
		delete_meta.expected_epoch = 1;
		delete_meta.tenant_id = tenant();
		delete_meta.table_object_id = descriptor.table_object_id;
		delete_meta.schema_generation = descriptor.schema_generation;
		delete_meta.row_count = 3;
		delete_meta.durability = DurabilityLevel::QUORUM;
		let row_key_ids = vec![target, target, missing];
		let (delete_resp, trailing) = node
			.data_server
			.table_delete(delete_meta, row_key_ids.clone())
			.await
			.unwrap();
		assert_eq!(delete_resp.write.status, WriteStatus::OK);
		assert_eq!(delete_resp.reject_code, PbcRejectCode::NONE.0);
		assert_eq!(delete_resp.deleted_count, 1);
		assert_eq!(delete_resp.already_deleted_count, 0);
		assert_eq!(delete_resp.not_found_count, 1);
		assert!(trailing.is_none());

		let pre_delete = node
			.slab
			.lookup_as_of(tenant(), target, write_resp.write.commit_hlc)
			.unwrap();
		assert!(!pre_delete.is_tombstone);
		let tombstone_location = node.slab.lookup(tenant(), target).unwrap();
		assert!(tombstone_location.is_tombstone);
		assert_eq!(tombstone_location.hlc, delete_resp.write.commit_hlc);
		let tombstone_frame = node.slab.read(tombstone_location).await.unwrap();
		let tombstone_batch = decode_batch(&tombstone_frame).unwrap();
		assert!(
			crate::storage::multirow::storage_row_is_tombstone_at(
				&tombstone_batch,
				tombstone_location.record_row_index,
			)
			.unwrap()
		);
		assert_eq!(
			storage_row_table_object_id_at(&tombstone_batch, tombstone_location.record_row_index)
				.unwrap(),
			Some(descriptor.table_object_id)
		);
		let materialized = materialize_record_location_batch(
			&tombstone_batch,
			tombstone_location.record_row_index,
		)
		.unwrap();
		assert_eq!(materialized.num_rows(), 0);

		let (retry_resp, _) = node
			.data_server
			.table_delete(delete_meta, row_key_ids)
			.await
			.unwrap();
		assert_eq!(retry_resp.deleted_count, 1);
		assert_eq!(retry_resp.not_found_count, 1);

		let mut second_delete = delete_meta;
		second_delete.parent_write_id.sequence_no = 2;
		second_delete.row_count = 1;
		let (already_resp, _) = node
			.data_server
			.table_delete(second_delete, vec![target])
			.await
			.unwrap();
		assert_eq!(already_resp.deleted_count, 0);
		assert_eq!(already_resp.already_deleted_count, 1);
		assert_eq!(already_resp.not_found_count, 0);

		node.handles.abort();
	}

	#[tokio::test]
	async fn table_delete_fails_closed_when_target_locality_is_missing() {
		let dir = bootstrap_tempdir("table-delete-missing-locality");
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			49,
			dir,
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();
		let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
		loop {
			if node.raft.current_leader().await == Some(node.local_node.id) {
				break;
			}
			assert!(
				tokio::time::Instant::now() < deadline,
				"timed out waiting for leader"
			);
			tokio::time::sleep(Duration::from_millis(20)).await;
		}
		node.data_server
			.refresh_leases_once(&node.ring, &*node.clock, 60_000)
			.await;

		let descriptor = append_only_table_descriptor();
		node.raft
			.client_write(WendelonRequest::SchemaUpdate(typed_table_schema_update(
				&descriptor,
			)))
			.await
			.unwrap();

		let batch = typed_table_batch(vec![7]);
		let mut write_meta = typed_table_meta(&descriptor, batch.num_rows());
		write_meta.parent_write_id.sequence_no = 471;
		let (write_resp, _) = node
			.data_server
			.table_write(write_meta, batch.clone())
			.await
			.unwrap();
		assert_eq!(write_resp.write.status, WriteStatus::OK);
		let admission = validate_table_write_request_batch(
			&write_meta,
			&batch,
			&descriptor,
			WriterNodeId::from_node_id(node.local_node),
			write_resp.write.commit_hlc,
		)
		.unwrap();
		let target = admission.key_identities[0];
		let original_location = node.slab.lookup(tenant(), target).unwrap();
		assert!(!original_location.is_tombstone);
		assert!(
			node.slab
				.routing_locality_for_existing_key(tenant(), target)
				.is_some()
		);

		node.slab.forget_routing_locality_for_test(tenant(), target);
		assert!(
			node.slab
				.routing_locality_for_existing_key(tenant(), target)
				.is_none()
		);

		let mut delete_meta: TableDeleteRowsRequestMeta = bytemuck::Zeroable::zeroed();
		delete_meta.parent_write_id = crate::schema::ClientWriteId {
			client_uuid: [0x49; 16],
			sequence_no: 1,
			idempotency_ttl_ms: 30_000,
			_pad: 0,
		};
		delete_meta.client_hlc = hlc(10, 0, 49);
		delete_meta.expected_epoch = 1;
		delete_meta.tenant_id = tenant();
		delete_meta.table_object_id = descriptor.table_object_id;
		delete_meta.schema_generation = descriptor.schema_generation;
		delete_meta.row_count = 1;
		delete_meta.durability = DurabilityLevel::QUORUM;

		let (delete_resp, trailing) = node
			.data_server
			.table_delete(delete_meta, vec![target])
			.await
			.unwrap();
		assert_eq!(delete_resp.write.status, WriteStatus::INTERNAL_ERROR);
		assert_eq!(
			delete_resp.reject_code,
			PbcRejectCode::PBC_REJECT_LOCALITY_UNSUPPORTED.0
		);
		assert_eq!(delete_resp.deleted_count, 0);
		assert_eq!(delete_resp.already_deleted_count, 0);
		assert_eq!(delete_resp.not_found_count, 0);
		assert!(trailing.is_none());

		let latest = node.slab.lookup(tenant(), target).unwrap();
		assert!(!latest.is_tombstone);
		assert_eq!(latest.hlc, original_location.hlc);

		node.handles.abort();
	}

	#[tokio::test]
	async fn table_write_multi_row_commits_one_parent_finalization() {
		let dir = bootstrap_tempdir("table-write-multi-row");
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			38,
			dir,
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();
		let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
		loop {
			if node.raft.current_leader().await == Some(node.local_node.id) {
				break;
			}
			assert!(
				tokio::time::Instant::now() < deadline,
				"timed out waiting for leader"
			);
			tokio::time::sleep(Duration::from_millis(20)).await;
		}
		node.data_server
			.refresh_leases_once(&node.ring, &*node.clock, 60_000)
			.await;

		let descriptor = typed_table_descriptor();
		node.raft
			.client_write(WendelonRequest::SchemaUpdate(typed_table_schema_update(
				&descriptor,
			)))
			.await
			.unwrap();

		let batch = typed_table_batch(vec![7, 8]);
		let mut meta = typed_table_meta(&descriptor, batch.num_rows());
		meta.parent_write_id.sequence_no = 56;
		meta.await_finalization = 1;
		let expected_bifurcated = bifurcate_table_write(
			&meta,
			&batch,
			&descriptor,
			WriterNodeId::ZERO,
			meta.client_hlc,
		)
		.unwrap();
		let (resp, trailing) = node
			.data_server
			.table_write(meta, batch.clone())
			.await
			.unwrap();
		assert_eq!(resp.write.status, WriteStatus::OK, "got resp={resp:?}");
		assert_eq!(resp.reject_code, PbcRejectCode::NONE.0);
		assert_eq!(resp.write.has_finalization, 1);
		assert!(trailing.is_some());

		let expected_finalization = TenantFinalization {
			marker: FinalizationMarker {
				log_index: 0,
				term: 0,
				epoch: meta.expected_epoch,
				commit_hlc: resp.write.commit_hlc,
				payload_hash: expected_bifurcated.payload_hash,
				e_safe_index: 0,
				durability: meta.durability,
				_pad: [0; 7],
			},
			tenant_id: meta.tenant_id,
			key_identity: expected_bifurcated.parent_key_identity,
			charged_bytes: expected_bifurcated.charged_bytes,
			record_count: 2,
		};
		assert!(
			node.log_store
				.find_applied_finalization(&expected_finalization, resp.write.log_index)
				.await
				.unwrap()
				.is_some(),
			"expected one aggregate finalization for the parent table write"
		);

		for id_value in [7_i64, 8_i64] {
			let key_identity = table_row_identity(
				tenant(),
				descriptor.table_object_id,
				&id_value.to_le_bytes(),
			);
			let location = node
				.slab
				.lookup(tenant(), key_identity)
				.expect("table row should be indexed by PK-derived identity");
			let stored = decode_batch(&node.slab.read(location).await.unwrap()).unwrap();
			assert_eq!(stored.schema().as_ref(), &typed_table_schema());
			assert_eq!(stored.num_rows(), 1);
			let ids = stored
				.column_by_name("id")
				.unwrap()
				.as_any()
				.downcast_ref::<Int64Array>()
				.unwrap();
			assert_eq!(ids.value(0), id_value);
		}

		node.handles.abort();
	}

	#[tokio::test]
	async fn table_write_cdc_releases_when_pending_intent_persistence_fails_but_finalize_commits() {
		let dir = bootstrap_tempdir("table-cdc-pending-upsert-fail-finalize-ok");
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			43,
			dir,
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();
		let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
		loop {
			if node.raft.current_leader().await == Some(node.local_node.id) {
				break;
			}
			assert!(
				tokio::time::Instant::now() < deadline,
				"timed out waiting for leader"
			);
			tokio::time::sleep(Duration::from_millis(20)).await;
		}
		node.data_server
			.refresh_leases_once(&node.ring, &*node.clock, 60_000)
			.await;

		let descriptor = typed_table_descriptor();
		node.raft
			.client_write(WendelonRequest::SchemaUpdate(typed_table_schema_update(
				&descriptor,
			)))
			.await
			.unwrap();
		node.data_server
			.pending_finalizations
			.fail_next_upsert_for_test("cdc-table-upsert-fail-finalize-ok");

		let batch = typed_table_batch(vec![700]);
		let mut meta = typed_table_meta(&descriptor, batch.num_rows());
		meta.parent_write_id.sequence_no = 700;
		meta.await_finalization = 1;

		let (resp, trailing) = node.data_server.table_write(meta, batch).await.unwrap();
		assert_eq!(resp.write.status, WriteStatus::OK, "got resp={resp:?}");
		assert!(trailing.is_some());
		assert_eq!(
			node.data_server.degraded_reason().as_deref(),
			Some(LOCAL_PERSISTENCE_DISTRUST_REASON)
		);

		let source_ranges =
			table_query_scan_ranges(meta.tenant_id, &descriptor, None).expect("table ranges");
		let entries = node
			.data_server
			.cdc_collect_ring_entries_after(
				meta.tenant_id,
				meta.table_object_id,
				&source_ranges,
				CdcCursor::MIN,
			)
			.await;
		assert_eq!(entries.len(), 1);
		assert_eq!(entries[0].cursor.row_hlc, resp.write.commit_hlc);
		assert!(entries[0].is_finalized());
		assert!(
			node.data_server.cdc_pending_live_entries.is_empty(),
			"immediate finalization success must release the pending CDC staging record"
		);

		node.handles.abort();
	}

	#[tokio::test]
	async fn table_write_cdc_evicts_when_pending_intent_persistence_and_finalize_fail() {
		let dir = bootstrap_tempdir("table-cdc-pending-upsert-finalize-fail");
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			44,
			dir,
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();
		let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
		loop {
			if node.raft.current_leader().await == Some(node.local_node.id) {
				break;
			}
			assert!(
				tokio::time::Instant::now() < deadline,
				"timed out waiting for leader"
			);
			tokio::time::sleep(Duration::from_millis(20)).await;
		}
		node.data_server
			.refresh_leases_once(&node.ring, &*node.clock, 60_000)
			.await;

		let descriptor = typed_table_descriptor();
		node.raft
			.client_write(WendelonRequest::SchemaUpdate(typed_table_schema_update(
				&descriptor,
			)))
			.await
			.unwrap();
		node.data_server
			.pending_finalizations
			.fail_next_upsert_for_test("cdc-table-upsert-and-finalize-fail");
		node.raft.shutdown().await.unwrap();

		let batch = typed_table_batch(vec![701]);
		let mut meta = typed_table_meta(&descriptor, batch.num_rows());
		meta.parent_write_id.sequence_no = 701;
		meta.await_finalization = 1;

		let (resp, trailing) = node.data_server.table_write(meta, batch).await.unwrap();
		assert_eq!(
			resp.write.status,
			WriteStatus::INTERNAL_ERROR,
			"got resp={resp:?}"
		);
		assert!(trailing.is_none());
		assert_eq!(
			node.data_server.degraded_reason().as_deref(),
			Some(LOCAL_PERSISTENCE_DISTRUST_REASON)
		);

		let source_ranges =
			table_query_scan_ranges(meta.tenant_id, &descriptor, None).expect("table ranges");
		let entries = node
			.data_server
			.cdc_collect_ring_entries_after(
				meta.tenant_id,
				meta.table_object_id,
				&source_ranges,
				CdcCursor::MIN,
			)
			.await;
		assert!(
			entries.is_empty(),
			"terminal immediate-finalization failure must evict staged CDC rows"
		);
		assert!(node.data_server.cdc_pending_live_entries.is_empty());

		node.handles.abort();
	}

	#[tokio::test]
	async fn table_replica_cdc_releases_when_parent_finalization_marker_applies() {
		let dir = bootstrap_tempdir("table-replica-cdc-marker-release");
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			45,
			dir,
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();

		let descriptor = typed_table_descriptor();
		let batch = typed_table_batch(vec![800]);
		let mut meta = typed_table_meta(&descriptor, batch.num_rows());
		meta.parent_write_id.sequence_no = 800;
		let commit_hlc = hlc(800, 0, node.local_node.id);
		let bifurcated =
			bifurcate_table_write(&meta, &batch, &descriptor, WriterNodeId::ZERO, commit_hlc)
				.unwrap();
		let replica_meta = table_replica_meta(&meta, &bifurcated, &bifurcated.children, commit_hlc);
		let replica_children = bifurcated
			.children
			.iter()
			.map(|child| TableReplicaChildPayload {
				child_ordinal: child.child_ordinal,
				key_identity: child.key_identity,
				locality: child.locality,
				record_row_index: child.record_row_index,
				frame_bytes: child.frame_bytes.clone(),
				payload_hash: child.payload_hash,
				charged_bytes: child.charged_bytes,
			})
			.collect::<Vec<_>>();
		let staged = node
			.data_server
			.stage_cdc_table_replica_children(
				meta.tenant_id,
				meta.table_object_id,
				&descriptor,
				commit_hlc,
				&replica_children,
				false,
			)
			.await
			.unwrap();
		assert_eq!(staged.len(), 1);
		node.data_server.cdc_stage_pending_finalization_marker(
			CdcFinalizationKey::from_table_replica_meta(&replica_meta),
			staged,
		);

		let source_ranges =
			table_query_scan_ranges(meta.tenant_id, &descriptor, None).expect("table ranges");
		assert!(
			node.data_server
				.cdc_collect_ring_entries_after(
					meta.tenant_id,
					meta.table_object_id,
					&source_ranges,
					CdcCursor::MIN,
				)
				.await
				.is_empty(),
			"replica CDC row must not be visible before the parent finalization applies"
		);

		let applied_marker = FinalizationMarker {
			log_index: 88,
			term: 2,
			epoch: meta.expected_epoch,
			commit_hlc,
			payload_hash: bifurcated.payload_hash,
			e_safe_index: 88,
			durability: meta.durability,
			_pad: [0; 7],
		};
		assert_eq!(
			node.data_server
				.cdc_release_pending_finalization_marker(applied_marker)
				.await,
			1
		);
		assert!(node.data_server.cdc_marker_pending_live_entries.is_empty());
		assert_eq!(
			node.data_server
				.cdc_locality_frontiers
				.min_for_localities([bifurcated.children[0].locality]),
			Some(commit_hlc)
		);

		let entries = node
			.data_server
			.cdc_collect_ring_entries_after(
				meta.tenant_id,
				meta.table_object_id,
				&source_ranges,
				CdcCursor::MIN,
			)
			.await;
		assert_eq!(entries.len(), 1);
		assert_eq!(entries[0].cursor.row_hlc, commit_hlc);
		assert!(entries[0].is_finalized());

		node.handles.abort();
	}

	#[tokio::test]
	async fn table_query_dispatch_materializes_multirow_records_before_pbc() {
		let dir = bootstrap_tempdir("table-query-rebatch");
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			39,
			dir,
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();
		let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
		loop {
			if node.raft.current_leader().await == Some(node.local_node.id) {
				break;
			}
			assert!(
				tokio::time::Instant::now() < deadline,
				"timed out waiting for leader"
			);
			tokio::time::sleep(Duration::from_millis(20)).await;
		}
		node.data_server
			.refresh_leases_once(&node.ring, &*node.clock, 60_000)
			.await;

		let descriptor = multirow_typed_table_descriptor();
		node.raft
			.client_write(WendelonRequest::SchemaUpdate(typed_table_schema_update(
				&descriptor,
			)))
			.await
			.unwrap();

		let source_ids = same_multirow_locality_ids(&descriptor, 2);
		let batch = typed_table_batch(source_ids.clone());
		let mut write_meta = typed_table_meta(&descriptor, batch.num_rows());
		write_meta.parent_write_id.sequence_no = 57;
		let (write_resp, _) = node
			.data_server
			.table_write(write_meta, batch)
			.await
			.unwrap();
		assert_eq!(write_resp.write.status, WriteStatus::OK);
		let stored_key = table_row_identity(
			tenant(),
			descriptor.table_object_id,
			&source_ids[0].to_le_bytes(),
		);
		let stored_location = node
			.slab
			.lookup(tenant(), stored_key)
			.expect("compressed multi-row frame should be indexed");
		let stored_frame = node.slab.read(stored_location).await.unwrap();
		assert_eq!(
			crate::storage::ipc::record_batch_compression_policy_from_ipc(&stored_frame).unwrap(),
			RecordBodyCompressionPolicy::Lz4Frame
		);
		assert!(has_storage_synthetic_columns(
			&decode_batch(&stored_frame).unwrap()
		));

		let mut bytecode = Vec::new();
		bytecode.push(OP_PUSH_COL);
		bytecode.extend_from_slice(&0_u32.to_le_bytes());
		bytecode.push(OP_PUSH_INT);
		bytecode.extend_from_slice(&source_ids[1].to_le_bytes());
		bytecode.push(OP_GTE);
		let query_meta = TableQueryRequestMeta {
			min_hlc: hlc(0, 0, 0),
			expected_epoch: 1,
			tenant_id: tenant(),
			table_object_id: descriptor.table_object_id,
			schema_generation: descriptor.schema_generation,
			consistency: Consistency::SERIALIZABLE,
			_pad: [0; 7],
			bytecode_len: bytecode.len() as u32,
			_pad2: [0; 4],
			as_of_hlc: hlc(0, 0, 0),
		};
		let mut meta_buf = node.pool.acquire();
		meta_buf
			.as_mut_vec()
			.extend_from_slice(bytemuck::bytes_of(&query_meta));
		let mut trailing = node.pool.acquire();
		trailing.as_mut_vec().extend_from_slice(&bytecode);
		let frame = HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::TABLE_QUERY_REQ.0,
				flags: 0,
				stream_id: 39,
				meta_len: meta_buf.len() as u32,
				arrow_len: 0,
				trailing_len: trailing.len() as u32,
			},
			meta: meta_buf,
			arrow_payload: None,
			trailing: Some(trailing),
		};

		let resp = DataRingServer::handle_data_frame(
			node.data_server.as_ref(),
			RequestContext {
				remote: node.local_node,
				actor: actor(ActorRole::Operator, None),
			},
			frame,
		)
		.await
		.unwrap()
		.unwrap();
		assert_eq!(resp.header.msg_type, MsgType::TABLE_QUERY_RESP.0);
		let meta: QueryResponseMeta =
			*bytemuck::from_bytes(&resp.meta[..std::mem::size_of::<QueryResponseMeta>()]);
		assert_eq!(meta.status, ReadStatus::OK, "got meta={meta:?}");
		assert_eq!(meta.rows, 1);
		let result = decode_batch(resp.arrow_payload.as_ref().expect("query result batch"))
			.expect("query result decodes");
		assert_eq!(result.num_rows(), 1);
		assert_eq!(result.schema().as_ref(), &typed_table_schema());
		assert_user_visible_schema_has_no_storage_columns(&result);
		let ids = result
			.column_by_name("id")
			.unwrap()
			.as_any()
			.downcast_ref::<Int64Array>()
			.unwrap();
		assert_eq!(ids.value(0), source_ids[1]);

		node.handles.abort();
	}

	#[tokio::test]
	async fn aggregate_dispatch_folds_filtered_table_rows() {
		let dir = bootstrap_tempdir("aggregate-dispatch");
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			44,
			dir,
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();
		let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
		loop {
			if node.raft.current_leader().await == Some(node.local_node.id) {
				break;
			}
			assert!(
				tokio::time::Instant::now() < deadline,
				"timed out waiting for leader"
			);
			tokio::time::sleep(Duration::from_millis(20)).await;
		}
		node.data_server
			.refresh_leases_once(&node.ring, &*node.clock, 60_000)
			.await;

		let descriptor = multirow_typed_table_descriptor();
		node.raft
			.client_write(WendelonRequest::SchemaUpdate(typed_table_schema_update(
				&descriptor,
			)))
			.await
			.unwrap();

		let source_ids = same_multirow_locality_ids(&descriptor, 2);
		let batch = typed_table_batch(source_ids.clone());
		let mut write_meta = typed_table_meta(&descriptor, batch.num_rows());
		write_meta.parent_write_id.sequence_no = 58;
		let (write_resp, _) = node
			.data_server
			.table_write(write_meta, batch)
			.await
			.unwrap();
		assert_eq!(write_resp.write.status, WriteStatus::OK);
		let stored_key = table_row_identity(
			tenant(),
			descriptor.table_object_id,
			&source_ids[0].to_le_bytes(),
		);
		let stored_location = node
			.slab
			.lookup(tenant(), stored_key)
			.expect("compressed multi-row frame should be indexed");
		let stored_frame = node.slab.read(stored_location).await.unwrap();
		assert_eq!(
			crate::storage::ipc::record_batch_compression_policy_from_ipc(&stored_frame).unwrap(),
			RecordBodyCompressionPolicy::Lz4Frame
		);
		assert!(has_storage_synthetic_columns(
			&decode_batch(&stored_frame).unwrap()
		));

		let mut predicate = Vec::new();
		predicate.push(OP_PUSH_COL);
		predicate.extend_from_slice(&0_u32.to_le_bytes());
		predicate.push(OP_PUSH_INT);
		predicate.extend_from_slice(&source_ids[1].to_le_bytes());
		predicate.push(OP_GTE);
		let request = AggregateRequest {
			format_version: AGG_FORMAT_VERSION,
			request_id: AggregateRequestId::new(99),
			tenant_id: tenant(),
			table_object_id: descriptor.table_object_id,
			schema_generation: descriptor.schema_generation,
			logical_schema_fingerprint: descriptor.logical_schema_fingerprint,
			consistency: Consistency::SERIALIZABLE,
			min_hlc: hlc(0, 0, 0),
			as_of_hlc: hlc(0, 0, 0),
			locality_ranges: vec![LocalityRange::full()],
			predicate_program: Some(predicate),
			aggregates: vec![
				AggregateExpression::count_star(),
				AggregateExpression {
					tag: AggregateFunctionTag::SIGNED_SUM,
					column_ordinal: 0,
					parameter_block: Vec::new(),
				},
			],
			budgets: AggregateBudgets {
				max_locations: LocationCount::new(16),
				max_rows: RowCount::new(16),
				max_wall_time_millis: DurationMillis::new(1_000),
				max_partial_state_bytes: ByteCount::new(4_096),
			},
		};
		let encoded = wire::encode(&request).unwrap();
		let mut trailing = node.pool.acquire();
		trailing.as_mut_vec().extend_from_slice(&encoded);
		let frame = HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::AGG_REQ.0,
				flags: 0,
				stream_id: 41,
				meta_len: 0,
				arrow_len: 0,
				trailing_len: trailing.len() as u32,
			},
			meta: node.pool.acquire(),
			arrow_payload: None,
			trailing: Some(trailing),
		};

		let resp = DataRingServer::handle_data_frame(
			node.data_server.as_ref(),
			RequestContext {
				remote: node.local_node,
				actor: actor(ActorRole::Operator, None),
			},
			frame,
		)
		.await
		.unwrap()
		.unwrap();
		assert_eq!(resp.header.msg_type, MsgType::AGG_RESP.0);
		let response: AggregateResponse =
			wire::decode(resp.trailing.as_ref().expect("aggregate response trailing")).unwrap();
		assert_eq!(response.status, AggregateStatus::Success);
		assert_eq!(response.request_id, request.request_id);
		assert_eq!(response.partials.len(), 2);
		assert_eq!(response.diagnostics.rows_admitted, 1);
		assert_eq!(response.partials[0].state.as_slice(), &1_u64.to_be_bytes());
		let expected_sum = encode_aggregate_state(
			AggregateFunctionTag::SIGNED_SUM,
			&AggregateState::SignedSum {
				sum: i128::from(source_ids[1]),
				non_null_count: 1,
				overflow: false,
			},
		)
		.unwrap();
		assert_eq!(response.partials[1].state, expected_sum);

		node.handles.abort();
	}

	async fn aggregate_coordinator_fixture(
		node_id: u32,
		peer_capabilities: crate::network::transport::NodeCapabilitySet,
	) -> (
		WendelonNode,
		Arc<WendelonDataServer>,
		Arc<LoopbackHandlers>,
		NodeId,
		AggregateRequest,
	) {
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			node_id,
			bootstrap_tempdir(&format!("aggregate-coordinator-{node_id}")),
			format!("loopback://{node_id}"),
		))
		.await
		.unwrap();
		let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
		loop {
			if node.raft.current_leader().await == Some(node.local_node.id) {
				break;
			}
			assert!(
				tokio::time::Instant::now() < deadline,
				"timed out waiting for single-node leader"
			);
			tokio::time::sleep(Duration::from_millis(20)).await;
		}
		node.data_server
			.refresh_leases_once(&node.ring, &*node.clock, 60_000)
			.await;
		let descriptor = TableSchemaDescriptorV1::new(
			TableObjectId::new([0x40; 16]),
			3,
			"analytics",
			"full_scan",
			&typed_table_schema(),
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
			LocalityPolicyV1::IdentityProjection,
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.unwrap();
		node.raft
			.client_write(WendelonRequest::SchemaUpdate(typed_table_schema_update(
				&descriptor,
			)))
			.await
			.unwrap();

		let pool = node.pool.clone();
		let handlers = LoopbackHandlers::new(pool.clone());
		let loopback_provider: Arc<dyn TransportProvider> = Arc::new(LoopbackProvider {
			handlers: handlers.clone(),
			tier: TransportTier::QuicIoUring,
		});
		let tiers = TransportTierSet::empty().with(TransportTier::QuicIoUring);
		let roles = NodeRoleSet::empty().with(NodeRole::Data);
		let peer = n(node_id + 100);
		let mut peers = std::collections::HashMap::new();
		peers.insert(
			node.local_node,
			PeerCapabilities {
				address: format!("loopback://{node_id}"),
				tiers,
				roles,
				capabilities: crate::network::transport::NodeCapabilitySet::empty()
					.with(NodeCapability::PartialAggregatePushdown),
			},
		);
		peers.insert(
			peer,
			PeerCapabilities {
				address: format!("loopback://{}", node_id + 100),
				tiers,
				roles,
				capabilities: peer_capabilities,
			},
		);
		let epoch = MembershipEpoch { epoch: 1, peers };
		let (_epoch_tx, epoch_rx) = watch::channel(Arc::new(epoch));
		let resolver: Arc<dyn crate::network::transport::CapabilityResolver> = Arc::new(
			crate::network::transport::EpochCapabilityResolver::new(epoch_rx.clone()),
		);
		let mut providers = std::collections::HashMap::new();
		providers.insert(TransportTier::QuicIoUring, loopback_provider);
		let factory = TransportProviderFactory::new(tiers, providers, resolver, epoch_rx);
		let router: Arc<dyn ReplicaRouter> =
			Arc::new(FixedReplicaRouter::new(vec![node.local_node]));
		let coordinator = Arc::new(
			WendelonDataServer::new(
				node.slab.clone(),
				node.raft.clone(),
				node.log_store.clone(),
				node.state_machine.clone(),
				node.local_node,
				node.clock.clone(),
				router,
				factory,
				pool,
			)
			.unwrap(),
		);
		let request = AggregateRequest {
			format_version: AGG_FORMAT_VERSION,
			request_id: AggregateRequestId::new(u128::from(node_id)),
			tenant_id: tenant(),
			table_object_id: descriptor.table_object_id,
			schema_generation: descriptor.schema_generation,
			logical_schema_fingerprint: descriptor.logical_schema_fingerprint,
			consistency: Consistency::SERIALIZABLE,
			min_hlc: hlc(0, 0, 0),
			as_of_hlc: hlc(0, 0, 0),
			locality_ranges: vec![LocalityRange::full()],
			predicate_program: None,
			aggregates: vec![AggregateExpression::count_star()],
			budgets: AggregateBudgets {
				max_locations: LocationCount::new(1_024),
				max_rows: RowCount::new(1_024),
				max_wall_time_millis: DurationMillis::new(1_000),
				max_partial_state_bytes: ByteCount::new(4_096),
			},
		};
		(node, coordinator, handlers, peer, request)
	}

	fn aggregate_request_frame(pool: &Arc<BufPool>, request: &AggregateRequest) -> HrftFrame {
		let encoded = wire::encode(request).unwrap();
		let mut trailing = pool.acquire();
		trailing.as_mut_vec().extend_from_slice(&encoded);
		HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::AGG_REQ.0,
				flags: 0,
				stream_id: 45,
				meta_len: 0,
				arrow_len: 0,
				trailing_len: trailing.len() as u32,
			},
			meta: pool.acquire(),
			arrow_payload: None,
			trailing: Some(trailing),
		}
	}

	async fn dispatch_aggregate_request(
		server: &Arc<WendelonDataServer>,
		request: &AggregateRequest,
	) -> AggregateResponse {
		let frame = aggregate_request_frame(&server.pool, request);
		let resp = DataRingServer::handle_data_frame(
			server.as_ref(),
			RequestContext {
				remote: server.local_node,
				actor: actor(ActorRole::Operator, None),
			},
			frame,
		)
		.await
		.unwrap()
		.unwrap();
		assert_eq!(resp.header.msg_type, MsgType::AGG_RESP.0);
		wire::decode(resp.trailing.as_ref().expect("aggregate response trailing")).unwrap()
	}

	struct StaticAggregateService {
		pool: Arc<BufPool>,
		count: u64,
		mismatched_crossbind: bool,
	}

	#[async_trait::async_trait]
	impl crate::network::server::HrftService for StaticAggregateService {
		async fn handle_frame(
			&self,
			_context: RequestContext,
			frame: HrftFrame,
		) -> std::io::Result<Option<HrftFrame>> {
			assert_eq!(frame.header.msg_type, MsgType::AGG_REQ.0);
			let request: AggregateRequest =
				wire::decode(frame.trailing.as_ref().expect("AGG_REQ trailing")).unwrap();
			let state = self.count.to_be_bytes().to_vec();
			let partial = crate::schema::AggregatePartial::new(
				AggregateExpressionIndex::new(0),
				AggregateFunctionTag::COUNT_STAR,
				state,
			)
			.unwrap();
			let response = AggregateResponse {
				format_version: request.format_version,
				request_id: if self.mismatched_crossbind {
					AggregateRequestId::new(request.request_id.get().saturating_add(1))
				} else {
					request.request_id
				},
				status: AggregateStatus::Success,
				reject_code: None,
				table_object_id: request.table_object_id,
				schema_generation: request.schema_generation,
				logical_schema_fingerprint: request.logical_schema_fingerprint,
				aggregate_expression_digest: request.expression_digest(),
				as_of_hlc: request.as_of_hlc,
				max_hlc: request.as_of_hlc,
				partials: vec![partial],
				diagnostics: AggregateDiagnostics {
					rows_scanned: self.count,
					rows_admitted: self.count,
					..AggregateDiagnostics::default()
				},
			};
			let trailing = wire::encode_into(&self.pool, &response)?;
			Ok(Some(HrftFrame {
				header: FrameHeader {
					magic: HRFT_MAGIC,
					version: 1,
					msg_type: MsgType::AGG_RESP.0,
					flags: FRAME_FLAG_STREAM_END,
					stream_id: frame.header.stream_id,
					meta_len: 0,
					arrow_len: 0,
					trailing_len: trailing.len() as u32,
				},
				meta: self.pool.acquire(),
				arrow_payload: None,
				trailing: Some(trailing),
			}))
		}
	}

	#[tokio::test]
	async fn aggregate_coordinator_rejects_peer_without_partial_capability() {
		let (node, coordinator, _handlers, _peer, request) = aggregate_coordinator_fixture(
			45,
			crate::network::transport::NodeCapabilitySet::empty(),
		)
		.await;

		let response = dispatch_aggregate_request(&coordinator, &request).await;
		assert_eq!(response.status, AggregateStatus::Rejected);
		assert_eq!(
			response.reject_code,
			Some(AggRejectCode::CapabilityUnavailable)
		);

		node.handles.abort();
	}

	#[tokio::test]
	async fn aggregate_coordinator_rejects_remote_failure() {
		let (node, coordinator, _handlers, _peer, request) = aggregate_coordinator_fixture(
			46,
			crate::network::transport::NodeCapabilitySet::empty()
				.with(NodeCapability::PartialAggregatePushdown),
		)
		.await;

		let response = dispatch_aggregate_request(&coordinator, &request).await;
		assert_eq!(response.status, AggregateStatus::Rejected);
		assert_eq!(response.reject_code, Some(AggRejectCode::DeadlineExceeded));

		node.handles.abort();
	}

	#[tokio::test]
	async fn aggregate_coordinator_merges_peer_partial_response() {
		let (node, coordinator, handlers, peer, request) = aggregate_coordinator_fixture(
			47,
			crate::network::transport::NodeCapabilitySet::empty()
				.with(NodeCapability::PartialAggregatePushdown),
		)
		.await;
		handlers.register(
			peer,
			Arc::new(StaticAggregateService {
				pool: coordinator.pool.clone(),
				count: 2,
				mismatched_crossbind: false,
			}),
		);

		let response = dispatch_aggregate_request(&coordinator, &request).await;
		assert_eq!(response.status, AggregateStatus::Success);
		assert_eq!(response.partials.len(), 1);
		assert_eq!(response.partials[0].state.as_slice(), &2_u64.to_be_bytes());
		assert_eq!(response.diagnostics.rows_admitted, 2);

		node.handles.abort();
	}

	#[tokio::test]
	async fn aggregate_coordinator_rejects_peer_crossbind_mismatch() {
		let (node, coordinator, handlers, peer, request) = aggregate_coordinator_fixture(
			48,
			crate::network::transport::NodeCapabilitySet::empty()
				.with(NodeCapability::PartialAggregatePushdown),
		)
		.await;
		handlers.register(
			peer,
			Arc::new(StaticAggregateService {
				pool: coordinator.pool.clone(),
				count: 2,
				mismatched_crossbind: true,
			}),
		);

		let response = dispatch_aggregate_request(&coordinator, &request).await;
		assert_eq!(response.status, AggregateStatus::Rejected);
		assert_eq!(
			response.reject_code,
			Some(AggRejectCode::PartialCrossbindFailed)
		);

		node.handles.abort();
	}

	#[test]
	fn aggregate_reject_mapping_preserves_retention_expired() {
		assert_eq!(
			aggregate_reject_from_read_status(ReadStatus::RETENTION_EXPIRED),
			AggRejectCode::RetentionExpired
		);
		assert_eq!(
			aggregate_reject_to_read_status(AggRejectCode::RetentionExpired),
			ReadStatus::RETENTION_EXPIRED
		);
	}

	#[tokio::test]
	async fn aggregate_retention_floor_reject_uses_retention_expired_code() {
		let mgr = make_manager().await;
		let descriptor = append_only_table_descriptor();
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			52,
			bootstrap_tempdir("aggregate-retention-expired-node"),
			"loopback://52".into(),
		))
		.await
		.unwrap();
		let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
		loop {
			if node.raft.current_leader().await == Some(node.local_node.id) {
				break;
			}
			assert!(
				tokio::time::Instant::now() < deadline,
				"timed out waiting for single-node leader"
			);
			tokio::time::sleep(Duration::from_millis(20)).await;
		}
		node.raft
			.client_write(WendelonRequest::SchemaUpdate(typed_table_schema_update(
				&descriptor,
			)))
			.await
			.unwrap();
		node.raft
			.client_write(WendelonRequest::SetTableRetentionPolicy(
				crate::consensus::SetTableRetentionPolicy {
					tenant_id: tenant(),
					table_object_id: descriptor.table_object_id,
					expected_policy_generation: PolicyGeneration::ZERO,
					new_policy: TableRetentionPolicy::FixedWindow(
						RetentionWindowMillis::try_new(60_000).unwrap(),
					),
					updated_at_hlc: hlc(1, 0, 1),
				},
			))
			.await
			.unwrap();

		let tiers = TransportTierSet::empty();
		let roles = NodeRoleSet::empty().with(NodeRole::Data);
		let mut peers = std::collections::HashMap::new();
		peers.insert(
			node.local_node,
			PeerCapabilities {
				address: "loopback://52".into(),
				tiers,
				roles,
				capabilities: crate::network::transport::NodeCapabilitySet::empty()
					.with(NodeCapability::PartialAggregatePushdown),
			},
		);
		let epoch = MembershipEpoch { epoch: 1, peers };
		let (_epoch_tx, epoch_rx) = watch::channel(Arc::new(epoch));
		let resolver: Arc<dyn crate::network::transport::CapabilityResolver> = Arc::new(
			crate::network::transport::EpochCapabilityResolver::new(epoch_rx.clone()),
		);
		let factory = TransportProviderFactory::new(
			tiers,
			std::collections::HashMap::new(),
			resolver,
			epoch_rx,
		);
		let server = WendelonDataServer::new(
			mgr,
			node.raft.clone(),
			node.log_store.clone(),
			node.state_machine.clone(),
			node.local_node,
			node.clock.clone(),
			Arc::new(FixedReplicaRouter::new(vec![node.local_node])),
			factory,
			node.pool.clone(),
		)
		.unwrap();
		let request = AggregateRequest {
			format_version: AGG_FORMAT_VERSION,
			request_id: AggregateRequestId::new(493),
			tenant_id: tenant(),
			table_object_id: descriptor.table_object_id,
			schema_generation: descriptor.schema_generation,
			logical_schema_fingerprint: descriptor.logical_schema_fingerprint,
			consistency: Consistency::SERIALIZABLE,
			min_hlc: hlc(1, 0, 1),
			as_of_hlc: hlc(1, 0, 1),
			locality_ranges: vec![LocalityRange::full()],
			predicate_program: None,
			aggregates: vec![AggregateExpression::count_star()],
			budgets: AggregateBudgets {
				max_locations: LocationCount::new(16),
				max_rows: RowCount::new(16),
				max_wall_time_millis: DurationMillis::new(1_000),
				max_partial_state_bytes: ByteCount::new(4_096),
			},
		};
		let floor_rejects_before = metrics()
			.storage_table_retention_read_floor_rejects_total
			.get();

		let response = server.execute_aggregate_local(&request).await.unwrap();

		assert_eq!(response.status, AggregateStatus::Rejected);
		assert_eq!(response.reject_code, Some(AggRejectCode::RetentionExpired));
		let query_meta = aggregate_response_as_query_meta(
			&request,
			&response,
			node.local_node,
			node.state_machine
				.current_admission_snapshot()
				.current_epoch,
		);
		assert_eq!(query_meta.status, ReadStatus::RETENTION_EXPIRED);
		assert!(
			metrics()
				.storage_table_retention_read_floor_rejects_total
				.get() >= floor_rejects_before + 1,
			"aggregate retention admission should increment the read-floor reject metric"
		);

		node.handles.abort();
	}

	#[test]
	fn aggregate_csb_response_marks_truncated_min_partial() {
		use crate::schema::aggregate::AGG_MIN_MAX_PREFIX_BYTES;
		use crate::storage::stats::{
			ARROW_STATS_TAG_LARGE_UTF8, COLUMN_STAT_HAS_MIN, COLUMN_STAT_HAS_NON_NULL_COUNT,
			COLUMN_STAT_HAS_NULL_COUNT, COLUMN_STAT_MIN_TRUNCATED, ColumnStatsBlockEntry,
			ColumnStatsBlockHeader, ParsedColumnStatsBlock, encode_length_prefixed_bytes,
		};

		let descriptor = typed_table_descriptor();
		let prefix = vec![b'a'; AGG_MIN_MAX_PREFIX_BYTES];
		let mut payload = Vec::new();
		encode_length_prefixed_bytes(&mut payload, &prefix).unwrap();
		payload.extend_from_slice(&0_u64.to_be_bytes());
		payload.extend_from_slice(&1_u64.to_be_bytes());
		let stats = ParsedColumnStatsBlock {
			header: ColumnStatsBlockHeader {
				table_object_id: descriptor.table_object_id,
				schema_generation: descriptor.schema_generation,
				logical_schema_fingerprint: descriptor.logical_schema_fingerprint,
				row_count: 1,
				column_block_count: 1,
			},
			columns: vec![ColumnStatsBlockEntry {
				column_ordinal: 1,
				arrow_type_tag: ARROW_STATS_TAG_LARGE_UTF8,
				stat_flags: COLUMN_STAT_HAS_MIN
					| COLUMN_STAT_MIN_TRUNCATED
					| COLUMN_STAT_HAS_NULL_COUNT
					| COLUMN_STAT_HAS_NON_NULL_COUNT,
				payload,
			}],
		};
		let request = AggregateRequest {
			format_version: AGG_FORMAT_VERSION,
			request_id: AggregateRequestId::new(491),
			tenant_id: tenant(),
			table_object_id: descriptor.table_object_id,
			schema_generation: descriptor.schema_generation,
			logical_schema_fingerprint: descriptor.logical_schema_fingerprint,
			consistency: Consistency::SERIALIZABLE,
			min_hlc: hlc(1, 0, 1),
			as_of_hlc: hlc(99, 0, 1),
			locality_ranges: vec![LocalityRange::full()],
			predicate_program: None,
			aggregates: vec![AggregateExpression {
				tag: AggregateFunctionTag::MIN,
				column_ordinal: 1,
				parameter_block: Vec::new(),
			}],
			budgets: AggregateBudgets {
				max_locations: LocationCount::new(1),
				max_rows: RowCount::new(1),
				max_wall_time_millis: DurationMillis::new(1_000),
				max_partial_state_bytes: ByteCount::new(4_096),
			},
		};

		let response = aggregate_csb_response_for_stats(&request, &descriptor, &stats)
			.unwrap()
			.unwrap();
		assert_eq!(response.status, AggregateStatus::Partial);
		assert_eq!(
			&response.partials[0].state[4..4 + prefix.len()],
			prefix.as_slice()
		);
		assert_eq!(response.partials[0].state.last(), Some(&1));
		assert_eq!(response.diagnostics.slabs_answered_from_csb, 1);
	}

	#[test]
	fn aggregate_csb_logical_slot_cliff_is_observable() {
		use crate::storage::stats::{ColumnStatsBlockHeader, ParsedColumnStatsBlock};

		let mut descriptor = typed_table_descriptor();
		descriptor.table_object_id = TableObjectId::new([0xEC; 16]);
		descriptor.refresh_descriptor_fingerprints().unwrap();
		let request = aggregate_count_request(&descriptor, 492, hlc(99, 0, 1));
		let mismatch_before = metrics()
			.storage_csb_merge_logical_fingerprint_mismatch_total_for_test(
				request.tenant_id,
				request.table_object_id,
			);
		let cliff_before = metrics().storage_csb_pre_adr_051_cliff_total_for_test(
			request.tenant_id,
			request.table_object_id,
		);
		let stats = ParsedColumnStatsBlock {
			header: ColumnStatsBlockHeader {
				table_object_id: descriptor.table_object_id,
				schema_generation: descriptor.schema_generation,
				logical_schema_fingerprint: crate::schema::LogicalSchemaFingerprint::from_bytes(
					[0xEE; 32],
				),
				row_count: 2,
				column_block_count: 0,
			},
			columns: Vec::new(),
		};

		let err = aggregate_csb_response_for_stats(&request, &descriptor, &stats).unwrap_err();

		assert_eq!(err, AggRejectCode::PartialCrossbindFailed);
		assert_eq!(
			metrics().storage_csb_merge_logical_fingerprint_mismatch_total_for_test(
				request.tenant_id,
				request.table_object_id,
			),
			mismatch_before + 1
		);
		assert_eq!(
			metrics().storage_csb_pre_adr_051_cliff_total_for_test(
				request.tenant_id,
				request.table_object_id,
			),
			cliff_before + 1
		);
	}

	#[tokio::test]
	async fn aggregate_csb_direct_answer_handles_count_min_and_max() {
		let l2 = Arc::new(MockObjectStore::default());
		let (mgr, descriptor) =
			tier_identity_typed_table_batch_with_stats(l2, vec![1, 2], "agg-csb.wnd").await;
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			49,
			bootstrap_tempdir("aggregate-csb-node"),
			"loopback://49".into(),
		))
		.await
		.unwrap();
		let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
		loop {
			if node.raft.current_leader().await == Some(node.local_node.id) {
				break;
			}
			assert!(
				tokio::time::Instant::now() < deadline,
				"timed out waiting for single-node leader"
			);
			tokio::time::sleep(Duration::from_millis(20)).await;
		}
		node.raft
			.client_write(WendelonRequest::SchemaUpdate(typed_table_schema_update(
				&descriptor,
			)))
			.await
			.unwrap();

		let tiers = TransportTierSet::empty();
		let roles = NodeRoleSet::empty().with(NodeRole::Data);
		let mut peers = std::collections::HashMap::new();
		peers.insert(
			node.local_node,
			PeerCapabilities {
				address: "loopback://49".into(),
				tiers,
				roles,
				capabilities: crate::network::transport::NodeCapabilitySet::empty()
					.with(NodeCapability::PartialAggregatePushdown),
			},
		);
		let epoch = MembershipEpoch { epoch: 1, peers };
		let (_epoch_tx, epoch_rx) = watch::channel(Arc::new(epoch));
		let resolver: Arc<dyn crate::network::transport::CapabilityResolver> = Arc::new(
			crate::network::transport::EpochCapabilityResolver::new(epoch_rx.clone()),
		);
		let factory = TransportProviderFactory::new(
			tiers,
			std::collections::HashMap::new(),
			resolver,
			epoch_rx,
		);
		let server = WendelonDataServer::new(
			mgr,
			node.raft.clone(),
			node.log_store.clone(),
			node.state_machine.clone(),
			node.local_node,
			node.clock.clone(),
			Arc::new(FixedReplicaRouter::new(vec![node.local_node])),
			factory,
			node.pool.clone(),
		)
		.unwrap();
		let request = AggregateRequest {
			format_version: AGG_FORMAT_VERSION,
			request_id: AggregateRequestId::new(490),
			tenant_id: tenant(),
			table_object_id: descriptor.table_object_id,
			schema_generation: descriptor.schema_generation,
			logical_schema_fingerprint: descriptor.logical_schema_fingerprint,
			consistency: Consistency::SERIALIZABLE,
			min_hlc: hlc(1, 0, 1),
			as_of_hlc: hlc(99, 0, 1),
			locality_ranges: vec![LocalityRange::full()],
			predicate_program: None,
			aggregates: vec![
				AggregateExpression::count_star(),
				AggregateExpression {
					tag: AggregateFunctionTag::COUNT_NON_NULL,
					column_ordinal: 1,
					parameter_block: Vec::new(),
				},
				AggregateExpression {
					tag: AggregateFunctionTag::MIN,
					column_ordinal: 0,
					parameter_block: Vec::new(),
				},
				AggregateExpression {
					tag: AggregateFunctionTag::MAX,
					column_ordinal: 0,
					parameter_block: Vec::new(),
				},
			],
			budgets: AggregateBudgets {
				max_locations: LocationCount::new(16),
				max_rows: RowCount::new(16),
				max_wall_time_millis: DurationMillis::new(1_000),
				max_partial_state_bytes: ByteCount::new(4_096),
			},
		};

		let metric_before = metrics().agg_csb_short_circuit_total.get();
		let response = server.execute_aggregate_local(&request).await.unwrap();
		assert_eq!(response.status, AggregateStatus::Success);
		assert_eq!(response.diagnostics.slabs_answered_from_csb, 1);
		assert_eq!(response.diagnostics.rows_admitted, 2);
		assert!(
			metrics().agg_csb_short_circuit_total.get() >= metric_before + 1,
			"CSB direct answer should increment the global counter at least once"
		);
		assert_eq!(response.partials[0].state.as_slice(), &2_u64.to_be_bytes());
		assert_eq!(response.partials[1].state.as_slice(), &2_u64.to_be_bytes());
		let min = encode_signed_stat_value(1, 8).unwrap();
		let max = encode_signed_stat_value(2, 8).unwrap();
		assert_eq!(&response.partials[2].state[4..12], min.as_slice());
		assert_eq!(&response.partials[3].state[4..12], max.as_slice());

		node.handles.abort();
	}

	#[tokio::test]
	async fn aggregate_csb_retention_floor_below_slab_allows_direct_answer() {
		let l2 = Arc::new(MockObjectStore::default());
		let (mgr, descriptor) =
			tier_identity_typed_table_batch_with_stats(l2, vec![1, 2], "agg-csb-retention-ok.wnd")
				.await;
		let (node, server) = aggregate_csb_test_server(
			mgr,
			&descriptor,
			53,
			"aggregate-csb-retention-ok-node",
			Arc::new(FixedClock { now_ms: 1_000 }),
			MvccRetentionPolicy::FixedWindow(Duration::from_millis(1_000)),
		)
		.await;
		let request = aggregate_count_request(&descriptor, 494, hlc(99, 0, 1));

		let response = server.execute_aggregate_local(&request).await.unwrap();

		assert_eq!(response.status, AggregateStatus::Success);
		assert_eq!(response.diagnostics.slabs_answered_from_csb, 1);
		assert_eq!(response.diagnostics.rows_admitted, 2);
		assert_eq!(count_star_partial_value(&response), 2);

		node.handles.abort();
	}

	#[tokio::test]
	async fn aggregate_csb_retention_floor_above_slab_returns_zero_contribution() {
		let l2 = Arc::new(MockObjectStore::default());
		let (mgr, descriptor) = tier_identity_typed_table_batch_with_stats(
			l2,
			vec![1, 2],
			"agg-csb-retention-zero.wnd",
		)
		.await;
		let (node, server) = aggregate_csb_test_server(
			mgr,
			&descriptor,
			54,
			"aggregate-csb-retention-zero-node",
			Arc::new(FixedClock { now_ms: 1_000 }),
			MvccRetentionPolicy::FixedWindow(Duration::from_millis(500)),
		)
		.await;
		let request = aggregate_count_request(&descriptor, 495, hlc(900, 0, 1));

		let response = server.execute_aggregate_local(&request).await.unwrap();

		assert_eq!(response.status, AggregateStatus::Success);
		assert_eq!(response.diagnostics.slabs_answered_from_csb, 0);
		assert_eq!(response.diagnostics.slabs_pruned_by_csb, 1);
		assert_eq!(response.diagnostics.rows_admitted, 0);
		assert_eq!(response.diagnostics.rows_filtered_out, 2);
		assert_eq!(count_star_partial_value(&response), 0);

		node.handles.abort();
	}

	#[tokio::test]
	async fn aggregate_csb_retention_floor_straddling_slab_falls_back_to_row_filter() {
		let l2 = Arc::new(MockObjectStore::default());
		let (mgr, descriptor) = tier_identity_typed_table_batch_with_stats(
			l2,
			vec![1, 2],
			"agg-csb-retention-straddle.wnd",
		)
		.await;
		let (node, server) = aggregate_csb_test_server(
			mgr,
			&descriptor,
			55,
			"aggregate-csb-retention-straddle-node",
			Arc::new(FixedClock { now_ms: 1_011 }),
			MvccRetentionPolicy::FixedWindow(Duration::from_millis(1_000)),
		)
		.await;
		let request = aggregate_count_request(&descriptor, 496, hlc(99, 0, 1));
		let prepared = server
			.prepare_aggregate_execution_context_with_migration(&request)
			.await
			.unwrap()
			.unwrap();

		assert!(
			server
				.try_aggregate_csb_direct_answer(&request, &prepared)
				.await
				.unwrap()
				.is_none(),
			"slabs straddling the retention floor must fall back to row filtering"
		);
		let response = server.execute_aggregate_local(&request).await.unwrap();
		assert_eq!(response.status, AggregateStatus::Success);
		assert_eq!(response.diagnostics.slabs_answered_from_csb, 0);
		assert_eq!(response.diagnostics.rows_admitted, 1);
		assert_eq!(count_star_partial_value(&response), 1);

		node.handles.abort();
	}

	#[tokio::test]
	async fn aggregate_csb_direct_answer_falls_through_when_sm_tombstone_affects_slab() {
		let l2 = Arc::new(MockObjectStore::default());
		let (mgr, descriptor) =
			tier_identity_typed_table_batch_with_stats(l2, vec![1, 2], "agg-csb-tombstone.wnd")
				.await;
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			50,
			bootstrap_tempdir("aggregate-csb-tombstone-node"),
			"loopback://50".into(),
		))
		.await
		.unwrap();
		let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
		loop {
			if node.raft.current_leader().await == Some(node.local_node.id) {
				break;
			}
			assert!(
				tokio::time::Instant::now() < deadline,
				"timed out waiting for single-node leader"
			);
			tokio::time::sleep(Duration::from_millis(20)).await;
		}
		node.raft
			.client_write(WendelonRequest::SchemaUpdate(typed_table_schema_update(
				&descriptor,
			)))
			.await
			.unwrap();
		node.state_machine
			.seed_migrated_range_tombstone_for_test(crate::consensus::MigratedRangeTombstone {
				migration_id: 77,
				range_start: 0,
				range_end: u64::MAX,
				to_epoch: 2,
				completed_by: n(50),
				completed_at_hlc: hlc(50, 0, 1),
				migration_hlc_floor: hlc(0, 0, 0),
			})
			.await;

		let tiers = TransportTierSet::empty();
		let roles = NodeRoleSet::empty().with(NodeRole::Data);
		let mut peers = std::collections::HashMap::new();
		peers.insert(
			node.local_node,
			PeerCapabilities {
				address: "loopback://50".into(),
				tiers,
				roles,
				capabilities: crate::network::transport::NodeCapabilitySet::empty()
					.with(NodeCapability::PartialAggregatePushdown),
			},
		);
		let epoch = MembershipEpoch { epoch: 1, peers };
		let (_epoch_tx, epoch_rx) = watch::channel(Arc::new(epoch));
		let resolver: Arc<dyn crate::network::transport::CapabilityResolver> = Arc::new(
			crate::network::transport::EpochCapabilityResolver::new(epoch_rx.clone()),
		);
		let factory = TransportProviderFactory::new(
			tiers,
			std::collections::HashMap::new(),
			resolver,
			epoch_rx,
		);
		let server = WendelonDataServer::new(
			mgr,
			node.raft.clone(),
			node.log_store.clone(),
			node.state_machine.clone(),
			node.local_node,
			node.clock.clone(),
			Arc::new(FixedReplicaRouter::new(vec![node.local_node])),
			factory,
			node.pool.clone(),
		)
		.unwrap();
		let request = AggregateRequest {
			format_version: AGG_FORMAT_VERSION,
			request_id: AggregateRequestId::new(491),
			tenant_id: tenant(),
			table_object_id: descriptor.table_object_id,
			schema_generation: descriptor.schema_generation,
			logical_schema_fingerprint: descriptor.logical_schema_fingerprint,
			consistency: Consistency::SERIALIZABLE,
			min_hlc: hlc(1, 0, 1),
			as_of_hlc: hlc(99, 0, 1),
			locality_ranges: vec![LocalityRange::full()],
			predicate_program: None,
			aggregates: vec![AggregateExpression::count_star()],
			budgets: AggregateBudgets {
				max_locations: LocationCount::new(16),
				max_rows: RowCount::new(16),
				max_wall_time_millis: DurationMillis::new(1_000),
				max_partial_state_bytes: ByteCount::new(4_096),
			},
		};

		let prepared = server
			.prepare_aggregate_execution_context_with_migration(&request)
			.await
			.unwrap()
			.unwrap();
		assert!(
			server
				.try_aggregate_csb_direct_answer(&request, &prepared)
				.await
				.unwrap()
				.is_none(),
			"state-machine tombstones must fence CSB direct answer even when the pointer says None"
		);

		node.handles.abort();
	}

	#[tokio::test]
	async fn aggregate_csb_direct_answer_falls_through_when_tiered_slab_has_superseded_row() {
		let l2 = Arc::new(MockObjectStore::default());
		let (mgr, descriptor) =
			tier_identity_typed_table_batch_with_stats(l2.clone(), vec![1, 2], "agg-csb-old.wnd")
				.await;

		let replacement = typed_table_batch(vec![1]);
		let mut write_meta: TableWriteRequestMeta = bytemuck::Zeroable::zeroed();
		write_meta.tenant_id = tenant();
		write_meta.table_object_id = descriptor.table_object_id;
		write_meta.schema_generation = descriptor.schema_generation;
		write_meta.row_count = encode_table_write_row_count(replacement.num_rows()).unwrap();
		let admission = validate_table_write_request_batch(
			&write_meta,
			&replacement,
			&descriptor,
			WriterNodeId::ZERO,
			write_meta.client_hlc,
		)
		.unwrap();
		let frame = encode_batch(&replacement).unwrap();
		mgr.append_durable_with_locality(
			tenant(),
			admission.key_identities[0],
			admission.localities[0],
			hlc(50, 0, 1),
			&frame,
		)
		.await
		.unwrap();
		let replacement_slab_id = mgr.force_seal_active_for_test().await;
		let raw = mgr.read_sealed_raw(replacement_slab_id).await.unwrap();
		let mut catalog = TableSchemaCatalogV1::default();
		catalog.upsert(tenant(), descriptor.clone()).unwrap();
		let encrypted = mgr
			.encrypt_tiered_object_blob_with_table_stats(replacement_slab_id, &raw, &catalog)
			.unwrap();
		let locator = encrypted.stats_locator.expect("stats locator");
		let l2_uri = l2.put("agg-csb-new.wnd", &encrypted.bytes).await.unwrap();
		mgr.apply_tier_slab(TieredSlabPointer {
			slab_id: replacement_slab_id,
			l2_uri,
			payload_hash: payload_digest(&encrypted.bytes),
			is_encrypted: true,
			tenant_id: Some(encrypted.tenant_id),
			tenant_kek_id: Some(encrypted.tenant_kek_id),
			stats_format_version: Some(locator.stats_format_version),
			stats_block_len: Some(locator.stats_block_len),
			stats_block_hash128: Some(locator.stats_block_hash128),
			tombstone_metadata: TombstoneMetadata::None,
			supersession_metadata: SupersessionMetadata::None,
		})
		.await
		.unwrap();

		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			51,
			bootstrap_tempdir("aggregate-csb-supersession-node"),
			"loopback://51".into(),
		))
		.await
		.unwrap();
		let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
		loop {
			if node.raft.current_leader().await == Some(node.local_node.id) {
				break;
			}
			assert!(
				tokio::time::Instant::now() < deadline,
				"timed out waiting for single-node leader"
			);
			tokio::time::sleep(Duration::from_millis(20)).await;
		}
		node.raft
			.client_write(WendelonRequest::SchemaUpdate(typed_table_schema_update(
				&descriptor,
			)))
			.await
			.unwrap();

		let tiers = TransportTierSet::empty();
		let roles = NodeRoleSet::empty().with(NodeRole::Data);
		let mut peers = std::collections::HashMap::new();
		peers.insert(
			node.local_node,
			PeerCapabilities {
				address: "loopback://51".into(),
				tiers,
				roles,
				capabilities: crate::network::transport::NodeCapabilitySet::empty()
					.with(NodeCapability::PartialAggregatePushdown),
			},
		);
		let epoch = MembershipEpoch { epoch: 1, peers };
		let (_epoch_tx, epoch_rx) = watch::channel(Arc::new(epoch));
		let resolver: Arc<dyn crate::network::transport::CapabilityResolver> = Arc::new(
			crate::network::transport::EpochCapabilityResolver::new(epoch_rx.clone()),
		);
		let factory = TransportProviderFactory::new(
			tiers,
			std::collections::HashMap::new(),
			resolver,
			epoch_rx,
		);
		let server = WendelonDataServer::new(
			mgr,
			node.raft.clone(),
			node.log_store.clone(),
			node.state_machine.clone(),
			node.local_node,
			node.clock.clone(),
			Arc::new(FixedReplicaRouter::new(vec![node.local_node])),
			factory,
			node.pool.clone(),
		)
		.unwrap();
		let request = AggregateRequest {
			format_version: AGG_FORMAT_VERSION,
			request_id: AggregateRequestId::new(492),
			tenant_id: tenant(),
			table_object_id: descriptor.table_object_id,
			schema_generation: descriptor.schema_generation,
			logical_schema_fingerprint: descriptor.logical_schema_fingerprint,
			consistency: Consistency::SERIALIZABLE,
			min_hlc: hlc(1, 0, 1),
			as_of_hlc: hlc(99, 0, 1),
			locality_ranges: vec![LocalityRange::full()],
			predicate_program: None,
			aggregates: vec![AggregateExpression::count_star()],
			budgets: AggregateBudgets {
				max_locations: LocationCount::new(16),
				max_rows: RowCount::new(16),
				max_wall_time_millis: DurationMillis::new(1_000),
				max_partial_state_bytes: ByteCount::new(4_096),
			},
		};

		let prepared = server
			.prepare_aggregate_execution_context_with_migration(&request)
			.await
			.unwrap()
			.unwrap();
		assert!(
			server
				.try_aggregate_csb_direct_answer(&request, &prepared)
				.await
				.unwrap()
				.is_none(),
			"superseded rows in a tiered slab must fence CSB direct answer"
		);

		node.handles.abort();
	}

	#[tokio::test]
	async fn table_write_fans_out_child_groups_to_distinct_replica_sets() {
		let leader_node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			40,
			bootstrap_tempdir("table-fanout-leader"),
			"loopback://40".into(),
		))
		.await
		.unwrap();
		let durable_a_node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			41,
			bootstrap_tempdir("table-fanout-a"),
			"loopback://41".into(),
		))
		.await
		.unwrap();
		let durable_b_node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			42,
			bootstrap_tempdir("table-fanout-b"),
			"loopback://42".into(),
		))
		.await
		.unwrap();
		let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
		loop {
			if leader_node.raft.current_leader().await == Some(leader_node.local_node.id)
				&& durable_a_node.raft.current_leader().await == Some(durable_a_node.local_node.id)
				&& durable_b_node.raft.current_leader().await == Some(durable_b_node.local_node.id)
			{
				break;
			}
			assert!(
				tokio::time::Instant::now() < deadline,
				"timed out waiting for single-node leaders"
			);
			tokio::time::sleep(Duration::from_millis(20)).await;
		}

		let descriptor = typed_table_descriptor();
		leader_node
			.raft
			.client_write(WendelonRequest::SchemaUpdate(typed_table_schema_update(
				&descriptor,
			)))
			.await
			.unwrap();
		let batch = typed_table_batch(vec![7, 8]);
		let mut meta = typed_table_meta(&descriptor, batch.num_rows());
		meta.parent_write_id.sequence_no = 58;
		let expected = bifurcate_table_write(
			&meta,
			&batch,
			&descriptor,
			WriterNodeId::ZERO,
			meta.client_hlc,
		)
		.unwrap();
		assert_eq!(expected.children.len(), 2);

		let pool = leader_node.pool.clone();
		let handlers = LoopbackHandlers::new(pool.clone());
		let loopback_provider: Arc<dyn TransportProvider> = Arc::new(LoopbackProvider {
			handlers: handlers.clone(),
			tier: TransportTier::QuicIoUring,
		});
		let tiers = TransportTierSet::empty().with(TransportTier::QuicIoUring);
		let roles = NodeRoleSet::empty().with(NodeRole::Data);
		let mut peers = StdHashMap::new();
		for (node_id, address) in [
			(leader_node.local_node, "loopback://40"),
			(durable_a_node.local_node, "loopback://41"),
			(durable_b_node.local_node, "loopback://42"),
		] {
			peers.insert(
				node_id,
				PeerCapabilities {
					address: address.into(),
					tiers,
					roles,
					capabilities: crate::network::transport::NodeCapabilitySet::empty(),
				},
			);
		}
		let epoch = MembershipEpoch { epoch: 1, peers };
		let (_epoch_tx, epoch_rx) = watch::channel(Arc::new(epoch));
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
			FixedReplicaRouter::new(vec![leader_node.local_node])
				.with_route(
					expected.children[0].locality,
					vec![leader_node.local_node, durable_a_node.local_node],
				)
				.with_route(
					expected.children[1].locality,
					vec![durable_b_node.local_node],
				),
		);

		let leader = Arc::new(
			WendelonDataServer::new(
				leader_node.slab.clone(),
				leader_node.raft.clone(),
				leader_node.log_store.clone(),
				leader_node.state_machine.clone(),
				leader_node.local_node,
				leader_node.clock.clone(),
				router.clone(),
				build_factory(),
				pool.clone(),
			)
			.unwrap(),
		);
		let durable_a = Arc::new(
			WendelonDataServer::new(
				durable_a_node.slab.clone(),
				durable_a_node.raft.clone(),
				durable_a_node.log_store.clone(),
				durable_a_node.state_machine.clone(),
				durable_a_node.local_node,
				durable_a_node.clock.clone(),
				router.clone(),
				build_factory(),
				durable_a_node.pool.clone(),
			)
			.unwrap(),
		);
		let durable_b = Arc::new(
			WendelonDataServer::new(
				durable_b_node.slab.clone(),
				durable_b_node.raft.clone(),
				durable_b_node.log_store.clone(),
				durable_b_node.state_machine.clone(),
				durable_b_node.local_node,
				durable_b_node.clock.clone(),
				router,
				build_factory(),
				durable_b_node.pool.clone(),
			)
			.unwrap(),
		);
		handlers.register(
			durable_a_node.local_node,
			Arc::new(
				MsgRouter::builder()
					.with_data_ring(durable_a.clone())
					.build(),
			),
		);
		handlers.register(
			durable_b_node.local_node,
			Arc::new(
				MsgRouter::builder()
					.with_data_ring(durable_b.clone())
					.build(),
			),
		);

		let (resp, _) = leader.table_write(meta, batch).await.unwrap();
		assert_eq!(resp.write.status, WriteStatus::OK, "got resp={resp:?}");

		assert!(
			leader_node
				.slab
				.lookup(tenant(), expected.children[0].key_identity)
				.is_some(),
			"leader should store the first child group where it is a replica"
		);
		assert!(
			durable_a_node
				.slab
				.lookup(tenant(), expected.children[0].key_identity)
				.is_some(),
			"durable_a should receive the first child group"
		);
		assert!(
			durable_b_node
				.slab
				.lookup(tenant(), expected.children[1].key_identity)
				.is_some(),
			"durable_b should receive the remote-only child group"
		);
		assert!(
			leader_node
				.slab
				.lookup(tenant(), expected.children[1].key_identity)
				.is_none(),
			"leader must not locally append a child for a replica set it is not in"
		);

		leader_node.handles.abort();
		durable_a_node.handles.abort();
		durable_b_node.handles.abort();
	}

	#[test]
	fn durable_quorum_tracker_requires_local_leg_when_leader_is_replica() {
		let mut tracker = DurableQuorumTracker::new(2, true);
		tracker.note_remote_ack(ReplicaAckLevel::DURABLE);
		tracker.note_remote_ack(ReplicaAckLevel::DURABLE);
		assert_eq!(tracker.durable_ack_count(), 2);
		assert!(tracker.quorum_reached());
		assert!(
			!tracker.is_satisfied(),
			"pure-remote quorum must not satisfy the write when the leader is a replica"
		);

		tracker.note_local_durable();
		assert!(tracker.is_satisfied());
	}

	#[test]
	fn durable_quorum_tracker_allows_remote_quorum_when_local_leg_not_required() {
		let mut tracker = DurableQuorumTracker::new(2, false);
		tracker.note_remote_ack(ReplicaAckLevel::APPEND_ONLY);
		tracker.note_remote_ack(ReplicaAckLevel::DURABLE);
		assert_eq!(tracker.durable_ack_count(), 1);
		assert!(!tracker.is_satisfied());

		tracker.note_remote_ack(ReplicaAckLevel::DURABLE);
		assert_eq!(tracker.durable_ack_count(), 2);
		assert!(tracker.is_satisfied());
	}

	#[tokio::test]
	async fn duplicate_write_replays_finalization_marker_and_emits_one_audit_record() {
		let dir = bootstrap_tempdir("duplicate-finalization");
		let audit_path = dir.join("audit.ndjson");
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			16,
			dir.clone(),
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();
		let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
		loop {
			if node.raft.current_leader().await == Some(node.local_node.id) {
				break;
			}
			assert!(
				tokio::time::Instant::now() < deadline,
				"timed out waiting for leader"
			);
			tokio::time::sleep(Duration::from_millis(20)).await;
		}
		node.data_server
			.refresh_leases_once(&node.ring, &*node.clock, 60_000)
			.await;

		let mut meta = simple_write_meta(1);
		meta.client_hlc = hlc(1, 0, 0);
		meta.await_finalization = 1;

		let first = node
			.data_server
			.write(meta, single_kv_batch())
			.await
			.unwrap();
		let duplicate = node
			.data_server
			.write(meta, single_kv_batch())
			.await
			.unwrap();

		assert_eq!(first.0.status, WriteStatus::OK, "got resp={:?}", first.0);
		assert_eq!(
			duplicate.0.status,
			WriteStatus::DUPLICATE,
			"got resp={:?}",
			duplicate.0
		);
		assert_eq!(first.1, duplicate.1);

		let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
		loop {
			let audit = std::fs::read_to_string(&audit_path).unwrap_or_default();
			if audit
				.matches("\"event_type\":\"data.write_finalized\"")
				.count() == 1
			{
				break;
			}
			assert!(
				tokio::time::Instant::now() < deadline,
				"timed out waiting for write audit; current log: {}",
				audit
			);
			tokio::time::sleep(Duration::from_millis(20)).await;
		}
	}

	#[tokio::test]
	async fn duplicate_write_replays_terminal_failure_without_recovery() {
		let dir = bootstrap_tempdir("duplicate-terminal-failure");
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			18,
			dir,
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();
		let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
		loop {
			if node.raft.current_leader().await == Some(node.local_node.id) {
				break;
			}
			assert!(
				tokio::time::Instant::now() < deadline,
				"timed out waiting for leader"
			);
			tokio::time::sleep(Duration::from_millis(20)).await;
		}
		node.data_server
			.refresh_leases_once(&node.ring, &*node.clock, 60_000)
			.await;

		let mut meta = simple_write_meta(1);
		meta.client_hlc = hlc(1, 0, 0);
		meta.await_finalization = 1;
		let terminal = node.data_server.terminal_failure_response_for_test(
			meta.write_id,
			meta.expected_epoch,
			hlc(2, 0, 0),
		);
		node.data_server.idempotency.insert(
			WendelonDataServer::idempotency_key_for(meta.write_id),
			CachedWriteOutcome::new(WriteOutcome::FinalizationTerminalFailure {
				response: terminal,
			}),
		);

		let (duplicate, trailing) = node
			.data_server
			.write(meta, single_kv_batch())
			.await
			.unwrap();

		assert_eq!(
			duplicate.status,
			WriteStatus::DUPLICATE,
			"got resp={duplicate:?}"
		);
		assert_eq!(duplicate.commit_hlc, terminal.commit_hlc);
		assert_eq!(duplicate.echoed_write_id, terminal.echoed_write_id);
		assert!(trailing.is_none());
		assert_eq!(
			node.data_server.pending_finalization_count_for_test().await,
			0
		);
	}

	#[tokio::test]
	async fn duplicate_write_stays_retryable_while_finalization_is_pending() {
		let dir = bootstrap_tempdir("duplicate-recoverable-pending");
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			19,
			dir,
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();
		let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
		loop {
			if node.raft.current_leader().await == Some(node.local_node.id) {
				break;
			}
			assert!(
				tokio::time::Instant::now() < deadline,
				"timed out waiting for leader"
			);
			tokio::time::sleep(Duration::from_millis(20)).await;
		}
		node.data_server
			.refresh_leases_once(&node.ring, &*node.clock, 60_000)
			.await;
		node.raft.shutdown().await.unwrap();

		let mut meta = simple_write_meta(1);
		meta.client_hlc = hlc(1, 0, 0);
		meta.await_finalization = 1;

		let (first, trailing) = node
			.data_server
			.write(meta, single_kv_batch())
			.await
			.unwrap();
		assert_eq!(first.status, WriteStatus::FINALIZATION_PENDING);
		assert!(trailing.is_none());

		let (duplicate, trailing) = node
			.data_server
			.write(meta, single_kv_batch())
			.await
			.unwrap();
		assert_eq!(duplicate.status, WriteStatus::FINALIZATION_PENDING);
		assert_eq!(duplicate.commit_hlc, first.commit_hlc);
		assert!(trailing.is_none());
		assert_eq!(
			node.data_server.pending_finalization_count_for_test().await,
			1
		);
	}

	#[tokio::test]
	async fn duplicate_write_replays_persisted_pending_intent_after_cache_loss() {
		let dir = bootstrap_tempdir("duplicate-persisted-pending");
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			21,
			dir,
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();
		let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
		loop {
			if node.raft.current_leader().await == Some(node.local_node.id) {
				break;
			}
			assert!(
				tokio::time::Instant::now() < deadline,
				"timed out waiting for leader"
			);
			tokio::time::sleep(Duration::from_millis(20)).await;
		}
		node.data_server
			.refresh_leases_once(&node.ring, &*node.clock, 60_000)
			.await;
		node.raft.shutdown().await.unwrap();

		let mut meta = simple_write_meta(1);
		meta.client_hlc = hlc(1, 0, 0);
		meta.await_finalization = 1;

		let before_bytes = node.slab.local_l1_bytes().await;
		let (first, trailing) = node
			.data_server
			.write(meta, single_kv_batch())
			.await
			.unwrap();
		assert_eq!(first.status, WriteStatus::FINALIZATION_PENDING);
		assert!(trailing.is_none());
		let after_first_bytes = node.slab.local_l1_bytes().await;
		assert!(
			after_first_bytes > before_bytes,
			"first write must durably append local bytes before finalization stalls"
		);

		// Intentionally reuse the shut-down raft handle here: the
		// invariant under test is that a cache-loss replay must not
		// re-append the payload bytes, even when reconciliation falls
		// back to the retryable pending status.
		let replay_server = WendelonDataServer::new(
			node.slab.clone(),
			node.raft.clone(),
			node.log_store.clone(),
			node.state_machine.clone(),
			node.local_node,
			node.clock.clone(),
			node.data_server.router.clone(),
			node.data_server.factory.clone(),
			node.pool.clone(),
		)
		.unwrap();

		let (duplicate, trailing) = replay_server.write(meta, single_kv_batch()).await.unwrap();
		assert_eq!(duplicate.status, WriteStatus::FINALIZATION_PENDING);
		assert_eq!(duplicate.commit_hlc, first.commit_hlc);
		assert!(trailing.is_none());
		assert_eq!(
			node.slab.local_l1_bytes().await,
			after_first_bytes,
			"duplicate replay after cache loss must not append the payload again"
		);
		assert_eq!(replay_server.pending_finalization_count_for_test().await, 1);
	}

	#[tokio::test]
	async fn pending_finalization_replays_after_restart_when_control_branch_failed() {
		let dir = bootstrap_tempdir("pending-finalization-restart");
		let audit_path = dir.join("audit.ndjson");
		let cfg = NodeBootstrapConfig::dev(17, dir.clone(), "127.0.0.1:0".into());
		let node = WendelonNode::bootstrap_single_node(cfg.clone())
			.await
			.unwrap();
		let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
		loop {
			if node.raft.current_leader().await == Some(node.local_node.id) {
				break;
			}
			assert!(
				tokio::time::Instant::now() < deadline,
				"timed out waiting for leader"
			);
			tokio::time::sleep(Duration::from_millis(20)).await;
		}
		node.data_server
			.refresh_leases_once(&node.ring, &*node.clock, 60_000)
			.await;

		let mut meta = simple_write_meta(1);
		meta.client_hlc = hlc(1, 0, 0);
		meta.await_finalization = 1;

		node.raft.shutdown().await.unwrap();

		let (resp, trailing) = node
			.data_server
			.write(meta, single_kv_batch())
			.await
			.unwrap();
		assert_eq!(
			resp.status,
			WriteStatus::FINALIZATION_PENDING,
			"got resp={resp:?}"
		);
		assert!(trailing.is_none());
		assert_eq!(
			node.data_server.pending_finalization_count_for_test().await,
			1
		);
		assert!(
			node.slab.lookup(tenant(), meta.key_identity).is_some(),
			"data branch must stay durable locally across the control-branch failure"
		);

		node.handles.abort();
		drop(node);
		tokio::time::sleep(Duration::from_millis(25)).await;

		let recovered = WendelonNode::bootstrap_single_node(cfg).await.unwrap();
		let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
		loop {
			if recovered.raft.current_leader().await == Some(recovered.local_node.id) {
				break;
			}
			assert!(
				tokio::time::Instant::now() < deadline,
				"timed out waiting for recovered leader"
			);
			tokio::time::sleep(Duration::from_millis(20)).await;
		}
		recovered
			.data_server
			.refresh_leases_once(&recovered.ring, &*recovered.clock, 60_000)
			.await;

		let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
		loop {
			let (read_resp, batch) = recovered
				.data_server
				.read(simple_read_meta(1), single_key_batch())
				.await
				.unwrap();
			if recovered
				.data_server
				.pending_finalization_count_for_test()
				.await == 0 && read_resp.status == ReadStatus::OK
				&& batch.is_some()
			{
				break;
			}
			assert!(
				tokio::time::Instant::now() < deadline,
				"timed out waiting for pending finalization replay; read_resp={read_resp:?}"
			);
			tokio::time::sleep(Duration::from_millis(20)).await;
		}

		let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
		loop {
			let audit = std::fs::read_to_string(&audit_path).unwrap_or_default();
			if audit
				.matches("\"event_type\":\"data.write_finalized\"")
				.count() == 1
			{
				break;
			}
			assert!(
				tokio::time::Instant::now() < deadline,
				"timed out waiting for replayed write audit; current log: {}",
				audit
			);
			tokio::time::sleep(Duration::from_millis(20)).await;
		}

		recovered.handles.abort();
	}

	#[tokio::test]
	async fn leader_recovers_orphan_witnesses_from_peer_before_reconciliation() {
		let fx = build_orphan_witness_fixture("orphan-witness").await;

		let record = PendingFinalizationRecord::new(
			crate::schema::ClientWriteId {
				client_uuid: [0x5A; 16],
				sequence_no: 77,
				idempotency_ttl_ms: 60_000,
				_pad: 0,
			},
			true,
			TenantFinalization {
				marker: FinalizationMarker {
					log_index: 0,
					term: 0,
					epoch: 1,
					commit_hlc: hlc(100, 0, fx.leader_node.local_node.id),
					payload_hash: [0xCD; 32],
					e_safe_index: 0,
					durability: DurabilityLevel::QUORUM,
					_pad: [0; 7],
				},
				tenant_id: tenant(),
				key_identity: id(9001),
				charged_bytes: 512,
				record_count: 1,
			},
		);
		fx.follower
			.pending_finalizations
			.upsert(record.clone())
			.await
			.unwrap();

		let current_term = fx.leader.raft.metrics().borrow().current_term;
		assert!(
			current_term > 0,
			"single-node leader must have entered a positive Raft term"
		);
		*fx.leader
			.orphan_witness_recovery_stamp
			.write()
			.expect("orphan witness recovery stamp poisoned") = Some(OrphanWitnessRecoveryStamp {
			term: current_term - 1,
			epoch: 2,
		});

		assert_eq!(fx.leader.pending_finalization_count_for_test().await, 0);
		fx.leader
			.ensure_orphan_witnesses_recovered_for_leadership()
			.await
			.unwrap();
		assert_eq!(fx.leader.pending_finalization_count_for_test().await, 1);

		let reconciled = fx
			.leader
			.reconcile_pending_finalizations_once()
			.await
			.unwrap();
		assert_eq!(reconciled, 1);
		assert_eq!(fx.leader.pending_finalization_count_for_test().await, 0);

		let applied_upto = fx
			.leader
			.state_machine
			.last_applied_log_index()
			.await
			.expect("finalization must advance the log");
		let marker = fx
			.leader
			.log_store
			.find_applied_finalization(&record.finalization, applied_upto)
			.await
			.unwrap();
		assert!(
			marker.is_some(),
			"leader must finalize the imported orphan witness"
		);

		fx.leader_node.handles.abort();
		fx.follower_node.handles.abort();
	}

	#[tokio::test]
	async fn fresh_leader_recovers_peer_witness_before_duplicate_write_replays() {
		let fx = build_orphan_witness_fixture("orphan-witness-duplicate").await;
		let mut meta = simple_write_meta(1);
		meta.write_id.sequence_no = 88;
		meta.await_finalization = 1;
		let batch = single_kv_batch();
		let bifurcated = bifurcate_client_write(&meta, &batch).unwrap();
		let record = PendingFinalizationRecord::new(
			meta.write_id,
			true,
			TenantFinalization {
				marker: FinalizationMarker {
					log_index: 0,
					term: 0,
					epoch: meta.expected_epoch,
					commit_hlc: hlc(200, 0, fx.leader_node.local_node.id),
					payload_hash: bifurcated.payload_hash,
					e_safe_index: 0,
					durability: meta.durability,
					_pad: [0; 7],
				},
				tenant_id: meta.tenant_id,
				key_identity: meta.key_identity,
				charged_bytes: bifurcated.charged_bytes,
				record_count: 1,
			},
		);
		fx.follower
			.pending_finalizations
			.upsert(record)
			.await
			.unwrap();

		let before_bytes = fx.leader_node.slab.local_l1_bytes().await;
		let (response, trailing) = fx.leader.write(meta, batch).await.unwrap();
		assert_eq!(
			response.status,
			WriteStatus::DUPLICATE,
			"got resp={response:?}"
		);
		assert_eq!(
			fx.leader_node.slab.local_l1_bytes().await,
			before_bytes,
			"fresh-leader recovery must import the peer witness before re-appending payload bytes"
		);
		assert!(trailing.is_some());

		fx.leader_node.handles.abort();
		fx.follower_node.handles.abort();
	}

	#[tokio::test]
	async fn cached_duplicate_replays_without_orphan_witness_sweep() {
		let fx = build_orphan_witness_fixture("orphan-witness-cache-bypass").await;
		let mut meta = simple_write_meta(1);
		meta.await_finalization = 1;
		let batch = single_kv_batch();

		let (first, first_trailing) = fx.leader.write(meta, batch.clone()).await.unwrap();
		assert_eq!(first.status, WriteStatus::OK, "got resp={first:?}");

		let tiers = TransportTierSet::empty().with(TransportTier::QuicIoUring);
		let roles = NodeRoleSet::empty().with(NodeRole::Data);
		let mut peers = fx.epoch_tx.borrow().as_ref().peers.clone();
		peers.insert(
			n(99),
			PeerCapabilities {
				address: "loopback://99".into(),
				tiers,
				roles,
				capabilities: crate::network::transport::NodeCapabilitySet::empty(),
			},
		);
		fx.epoch_tx
			.send(Arc::new(MembershipEpoch { epoch: 3, peers }))
			.unwrap();

		let (duplicate, trailing) = fx.leader.write(meta, batch).await.unwrap();
		assert_eq!(
			duplicate.status,
			WriteStatus::DUPLICATE,
			"cached duplicate replay must not be blocked by an unreachable sweep peer"
		);
		assert_eq!(trailing, first_trailing);

		fx.leader_node.handles.abort();
		fx.follower_node.handles.abort();
	}

	#[tokio::test]
	async fn write_commits_and_fences_when_pending_intent_persistence_fails() {
		let dir = bootstrap_tempdir("pending-intent-upsert-fail");
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			20,
			dir,
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();
		let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
		loop {
			if node.raft.current_leader().await == Some(node.local_node.id) {
				break;
			}
			assert!(
				tokio::time::Instant::now() < deadline,
				"timed out waiting for leader"
			);
			tokio::time::sleep(Duration::from_millis(20)).await;
		}
		node.data_server
			.refresh_leases_once(&node.ring, &*node.clock, 60_000)
			.await;
		node.data_server
			.pending_finalizations
			.fail_next_upsert_for_test("injected-by-b2");

		let mut meta = simple_write_meta(1);
		meta.client_hlc = hlc(1, 0, 0);
		meta.await_finalization = 1;

		let (resp, trailing) = node
			.data_server
			.write(meta, single_kv_batch())
			.await
			.unwrap();

		assert_eq!(resp.status, WriteStatus::OK, "got resp={resp:?}");
		assert!(trailing.is_some());
		assert_eq!(
			node.data_server.pending_finalization_count_for_test().await,
			0
		);
		assert!(
			node.slab.lookup(tenant(), meta.key_identity).is_some(),
			"data branch must remain durable even though finalization intent persistence failed"
		);
		assert_eq!(
			node.data_server.degraded_reason().as_deref(),
			Some(LOCAL_PERSISTENCE_DISTRUST_REASON)
		);

		let (duplicate, trailing) = node
			.data_server
			.write(meta, single_kv_batch())
			.await
			.unwrap();
		assert_eq!(duplicate.status, WriteStatus::DUPLICATE);
		assert!(trailing.is_some());
	}

	#[tokio::test]
	async fn write_fences_when_pending_intent_persistence_and_immediate_finalization_fail() {
		let dir = bootstrap_tempdir("pending-intent-upsert-and-finalize-fail");
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			25,
			dir,
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();
		let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
		loop {
			if node.raft.current_leader().await == Some(node.local_node.id) {
				break;
			}
			assert!(
				tokio::time::Instant::now() < deadline,
				"timed out waiting for leader"
			);
			tokio::time::sleep(Duration::from_millis(20)).await;
		}
		node.data_server
			.refresh_leases_once(&node.ring, &*node.clock, 60_000)
			.await;
		node.data_server
			.pending_finalizations
			.fail_next_upsert_for_test("injected-by-b2");
		node.raft.shutdown().await.unwrap();

		let mut meta = simple_write_meta(1);
		meta.client_hlc = hlc(1, 0, 0);
		meta.await_finalization = 1;

		let (resp, trailing) = node
			.data_server
			.write(meta, single_kv_batch())
			.await
			.unwrap();
		assert_eq!(
			resp.status,
			WriteStatus::INTERNAL_ERROR,
			"got resp={resp:?}"
		);
		assert!(trailing.is_none());
		assert_eq!(
			node.data_server.degraded_reason().as_deref(),
			Some(LOCAL_PERSISTENCE_DISTRUST_REASON)
		);
		assert_eq!(
			node.data_server.pending_finalization_count_for_test().await,
			0
		);
		assert!(
			node.slab.lookup(tenant(), meta.key_identity).is_some(),
			"data branch must remain durable even though finalization cannot be recovered on this node"
		);

		let (duplicate, trailing) = node
			.data_server
			.write(meta, single_kv_batch())
			.await
			.unwrap();
		assert_eq!(duplicate.status, WriteStatus::INTERNAL_ERROR);
		assert!(trailing.is_none());
	}

	#[tokio::test]
	async fn local_persistence_failure_fences_future_writes() {
		let dir = bootstrap_tempdir("local-persistence-fence");
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			22,
			dir,
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();
		let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
		loop {
			if node.raft.current_leader().await == Some(node.local_node.id) {
				break;
			}
			assert!(
				tokio::time::Instant::now() < deadline,
				"timed out waiting for leader"
			);
			tokio::time::sleep(Duration::from_millis(20)).await;
		}
		node.data_server
			.refresh_leases_once(&node.ring, &*node.clock, 60_000)
			.await;

		node.data_server
			.fail_next_local_durable_append_for_test(ErrorKind::PermissionDenied, "b6-fence");

		let mut first_meta = simple_write_meta(1);
		first_meta.client_hlc = hlc(1, 0, 0);
		let before_first = node.slab.local_l1_bytes().await;
		let (first, trailing) = node
			.data_server
			.write(first_meta, single_kv_batch())
			.await
			.unwrap();
		assert_eq!(first.status, WriteStatus::INTERNAL_ERROR);
		assert!(trailing.is_none());
		assert_eq!(
			node.data_server.degraded_reason().as_deref(),
			Some(LOCAL_PERSISTENCE_DISTRUST_REASON)
		);
		assert_eq!(
			node.slab.local_l1_bytes().await,
			before_first,
			"failed leader-local durable append must not publish bytes locally"
		);

		let mut second_meta = simple_write_meta(1);
		second_meta.write_id.sequence_no = 2;
		second_meta.client_hlc = hlc(2, 0, 0);
		let before_second = node.slab.local_l1_bytes().await;
		let (second, trailing) = node
			.data_server
			.write(second_meta, single_kv_batch())
			.await
			.unwrap();
		assert_eq!(second.status, WriteStatus::INTERNAL_ERROR);
		assert!(trailing.is_none());
		assert_eq!(
			node.slab.local_l1_bytes().await,
			before_second,
			"fenced node must reject later writes before attempting another local append"
		);
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
		assert!(pending_owned_entry(&migrations, Locality::new(150), local).is_some());
		// In-range, owned by someone else: miss.
		assert!(pending_owned_entry(&migrations, Locality::new(150), n(9)).is_none());
		// Out of range: miss.
		assert!(pending_owned_entry(&migrations, Locality::new(50), local).is_none());
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
		assert!(pending_owned_entry(&migrations, Locality::new(50), local).is_none());
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
		let mut cursor_identity = Identity::ZERO;
		let mut cursor_hlc: HybridTimestamp = hlc(0, 0, 0);
		let max_records: usize = 10;
		for _ in 0..15 {
			let locations =
				mgr.scan_all_versions_from(key, key, floor, cursor_identity, cursor_hlc);
			if locations.is_empty() {
				break;
			}
			let take = locations.len().min(max_records);
			for (_tenant_id, h, _routing_locality, loc) in locations.iter().take(take) {
				assert_eq!(*h, id(key));
				collected.push(loc.hlc);
			}
			let truncated = locations.len() > max_records;
			if !truncated {
				break;
			}
			let (_last_tenant_id, last_h, _last_routing_locality, last_loc) = locations[take - 1];
			cursor_identity = last_h;
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
		let (tenant_id, h, routing_locality, loc) = hits[0];
		assert_eq!(tenant_id, tenant());
		let frame = source_full.read(loc).await.unwrap();
		let payload_hash = payload_digest(&frame);
		destination
			.append_migration_with_locality(
				1,
				tenant_id,
				h,
				routing_locality,
				loc.hlc,
				payload_hash,
				&frame,
			)
			.await
			.unwrap();

		let found = destination.lookup_as_of(tenant(), id(key), v);
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
		let pulled = source.scan_all_versions_from(0, 99, floor, Identity::ZERO, floor);
		for (tenant_id, h, routing_locality, loc) in pulled {
			let frame = source.read(loc).await.unwrap();
			let payload_hash = payload_digest(&frame);
			destination
				.append_migration_with_locality(
					1,
					tenant_id,
					h,
					routing_locality,
					loc.hlc,
					payload_hash,
					&frame,
				)
				.await
				.unwrap();
		}

		// A window scan on the destination now sees the union.
		let merged = destination.scan_as_of(tenant(), 0, 99, v);
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
			let rows = src.scan_all_versions_from(0, 99, floor, Identity::ZERO, floor);
			for (tenant_id, h, routing_locality, loc) in rows {
				let frame = src.read(loc).await.unwrap();
				let payload_hash = payload_digest(&frame);
				destination
					.append_migration_with_locality(
						1,
						tenant_id,
						h,
						routing_locality,
						loc.hlc,
						payload_hash,
						&frame,
					)
					.await
					.unwrap();
			}
		}

		let merged = destination.scan_as_of(tenant(), 0, 99, v);
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
		let payload_newer = payload_digest(&frame_newer);
		destination
			.append_migration(1, tenant(), id(key), newer, payload_newer, &frame_newer)
			.await
			.unwrap();

		// Client wants a version ≤ older.physical_ms; newer-only
		// ingest cannot satisfy the read.
		assert!(
			destination.lookup_as_of(tenant(), id(key), older).is_none(),
			"newer-only ingest must not be treated as authoritative for older as_of"
		);

		// Second "source" yields the older version; after falling
		// through, the lookup succeeds.
		let frame_older = {
			let batch = kv_batch(b"k", b"v-old");
			encode_batch(&batch).unwrap()
		};
		let payload_older = payload_digest(&frame_older);
		destination
			.append_migration(1, tenant(), id(key), older, payload_older, &frame_older)
			.await
			.unwrap();
		assert!(
			destination.lookup_as_of(tenant(), id(key), older).is_some(),
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
		// receiver's `append_migration` gets the same (key_identity,
		// hlc, payload_hash, frame_bytes) tuple the sender read
		// off disk.
		let rows = vec![
			(
				tenant(),
				id(7),
				Locality::new(70),
				crate::storage::core::RecordLocation {
					slab_id: SlabId::ZERO,
					offset: 0,
					length: 0,
					hlc: hlc(10, 2, 3),
					record_row_index: crate::schema::RecordRowIndex::ZERO,
					is_tombstone: false,
				},
				b"frame-one".to_vec(),
			),
			(
				tenant(),
				id(42),
				Locality::new(420),
				crate::storage::core::RecordLocation {
					slab_id: SlabId::ZERO,
					offset: 0,
					length: 0,
					hlc: hlc(20, 5, 3),
					record_row_index: crate::schema::RecordRowIndex::ZERO,
					is_tombstone: false,
				},
				b"frame-two".to_vec(),
			),
		];
		let bytes = encode_migration_records(&rows).unwrap();
		let policy = crate::storage::ipc::record_batch_compression_policy_from_ipc(&bytes).unwrap();
		assert_eq!(policy, RecordBodyCompressionPolicy::Lz4Frame);
		let decoded = decode_migration_records(&bytes).unwrap();
		assert_eq!(decoded.len(), 2);
		assert_eq!(decoded[0].key_identity, id(7));
		assert_eq!(decoded[0].routing_locality, Locality::new(70));
		assert_eq!(decoded[0].hlc, hlc(10, 2, 3));
		assert_eq!(decoded[0].frame_bytes, b"frame-one");
		assert_eq!(decoded[0].payload_hash, payload_digest(b"frame-one"));
		assert_eq!(decoded[1].key_identity, id(42));
		assert_eq!(decoded[1].routing_locality, Locality::new(420));
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
	use crate::network::tls::ActorIdentity;
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
		actor: ActorIdentity,
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
		fn actor_identity(&self) -> &ActorIdentity {
			&self.actor
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
		let response = match svc
			.handle_frame(
				RequestContext {
					remote: ZERO_NODE,
					actor: ActorIdentity::data_node(ZERO_NODE),
				},
				frame,
			)
			.await
		{
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
				actor: ActorIdentity::data_node(target),
			}))
		}
	}

	struct FixedReplicaRouter {
		routes: StdHashMap<Locality, Vec<NodeId>>,
		default: Vec<NodeId>,
	}

	impl FixedReplicaRouter {
		fn new(default: Vec<NodeId>) -> Self {
			Self {
				routes: StdHashMap::new(),
				default,
			}
		}

		fn with_route(mut self, locality: Locality, replicas: Vec<NodeId>) -> Self {
			self.routes.insert(locality, replicas);
			self
		}
	}

	impl ReplicaRouter for FixedReplicaRouter {
		fn replicas_for_locality(&self, locality: Locality) -> Vec<NodeId> {
			self.routes
				.get(&locality)
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
		volatile_cutoffs: StdHashMap<SlabId, u64>,
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
		async fn open(dir: std::path::PathBuf, pool: Arc<BufPool>) -> Arc<Self> {
			let io = spawn_io_thread(64).unwrap();
			let max_slab_bytes = 1 << 30;
			let manager = Arc::new(
				SlabManager::open(SlabManagerConfig {
					dir: dir.clone(),
					max_slab_bytes,
					io: io.clone(),
					l2: None,
					rehydration: crate::storage::manager::RehydrationPolicy::default(),
					l2_column_fetch: Default::default(),
					group_commit: crate::storage::GroupCommitConfig::default(),
					crypto: test_crypto(),
				})
				.await
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
					rehydration: crate::storage::manager::RehydrationPolicy::default(),
					l2_column_fetch: Default::default(),
					group_commit: crate::storage::GroupCommitConfig::default(),
					crypto: test_crypto(),
				})
				.await
				.unwrap(),
			);
		}
	}

	#[async_trait]
	impl HrftService for CrashyReplicaService {
		async fn handle_frame(
			&self,
			_context: RequestContext,
			frame: HrftFrame,
		) -> std::io::Result<Option<HrftFrame>> {
			if frame.header.msg_type == MsgType::PENDING_WITNESS_REQ.0 {
				let resp = PendingWitnessPullResponseMeta {
					status: PendingWitnessPullStatus::OK,
					_pad1: [0; 7],
					witness_count: 0,
				};
				let mut body = self.pool.acquire();
				body.as_mut_vec()
					.extend_from_slice(bytemuck::bytes_of(&resp));
				return Ok(Some(HrftFrame {
					header: FrameHeader {
						magic: HRFT_MAGIC,
						version: 1,
						msg_type: MsgType::PENDING_WITNESS_RESP.0,
						flags: 0,
						stream_id: frame.header.stream_id,
						meta_len: body.len() as u32,
						arrow_len: 0,
						trailing_len: 0,
					},
					meta: body,
					arrow_payload: None,
					trailing: None,
				}));
			}
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
			let (status, ack_level, location) = if payload_digest(arrow_bytes) != meta.payload_hash
			{
				(
					ReplicaWriteStatus::HASH_MISMATCH,
					ReplicaAckLevel::NONE,
					None,
				)
			} else {
				match manager
					.append(
						meta.tenant_id,
						meta.key_identity,
						meta.commit_hlc,
						arrow_bytes,
					)
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
		durable_key_locality: u64,
		append_only_key_locality: u64,
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
				capabilities: crate::network::transport::NodeCapabilitySet::empty(),
			},
		);
		peers.insert(
			durable_id,
			PeerCapabilities {
				address: "loopback://42".into(),
				tiers,
				roles,
				capabilities: crate::network::transport::NodeCapabilitySet::empty(),
			},
		);
		peers.insert(
			crashy_id,
			PeerCapabilities {
				address: "loopback://43".into(),
				tiers,
				roles,
				capabilities: crate::network::transport::NodeCapabilitySet::empty(),
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
				.with_route(
					Locality::new(durable_key_locality),
					vec![leader_id, durable_id, crashy_id],
				)
				.with_route(
					Locality::new(append_only_key_locality),
					vec![leader_id, crashy_id],
				),
		);

		let leader = Arc::new(
			WendelonDataServer::new(
				leader_node.slab.clone(),
				leader_node.raft.clone(),
				leader_node.log_store.clone(),
				leader_node.state_machine.clone(),
				leader_id,
				leader_node.clock.clone(),
				router.clone(),
				build_factory(),
				pool.clone(),
			)
			.unwrap(),
		);
		let durable_replica = Arc::new(
			WendelonDataServer::new(
				durable_node.slab.clone(),
				durable_node.raft.clone(),
				durable_node.log_store.clone(),
				durable_node.state_machine.clone(),
				durable_id,
				durable_node.clock.clone(),
				router,
				build_factory(),
				durable_node.pool.clone(),
			)
			.unwrap(),
		);
		let durable_data_ring: Arc<dyn DataRingServer> = durable_replica;
		let durable_service: Arc<dyn HrftService> = Arc::new(
			MsgRouter::builder()
				.with_data_ring(durable_data_ring)
				.build(),
		);
		let crashy = CrashyReplicaService::open(fresh_dir("crashy-replica"), pool.clone()).await;
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
		let durable_key_locality = key_locality(durable_key);
		let append_only_key_locality = key_locality(append_only_key);
		let durable_key_identity = Identity::blake3_tenant(tenant(), durable_key);
		let append_only_key_identity = Identity::blake3_tenant(tenant(), append_only_key);
		let fx = build_durability_fixture(durable_key_locality, append_only_key_locality).await;

		let durable_value = b"v-durable";
		let mut durable_meta = simple_write_meta(1);
		durable_meta.write_id.sequence_no = 101;
		durable_meta.key_identity = durable_key_identity;
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
			fx.leader_slab
				.lookup(tenant(), durable_key_identity)
				.is_some(),
			fx.durable_slab
				.lookup(tenant(), durable_key_identity)
				.is_some(),
			crashy_after_ok
				.lookup(tenant(), durable_key_identity)
				.is_some(),
		]
		.into_iter()
		.filter(|present| *present)
		.count();
		assert_eq!(
			durable_survivors, 2,
			"only the two durably-acked replicas should retain the successful write"
		);
		assert!(
			!crashy_after_ok
				.lookup(tenant(), durable_key_identity)
				.is_some(),
			"append-only replica must lose its copy across the simulated crash"
		);

		let append_only_value = b"v-append-only";
		let mut append_only_meta = simple_write_meta(1);
		append_only_meta.write_id.sequence_no = 102;
		append_only_meta.key_identity = append_only_key_identity;
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
			fx.leader_slab
				.lookup(tenant(), append_only_key_identity)
				.is_some(),
			fx.durable_slab
				.lookup(tenant(), append_only_key_identity)
				.is_some(),
			crashy_after_fail
				.lookup(tenant(), append_only_key_identity)
				.is_some(),
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
				capabilities: crate::network::transport::NodeCapabilitySet::empty(),
			},
		);
		peers.insert(
			source_a_id,
			PeerCapabilities {
				address: "loopback://32".into(),
				tiers,
				roles,
				capabilities: crate::network::transport::NodeCapabilitySet::empty(),
			},
		);
		peers.insert(
			source_b_id,
			PeerCapabilities {
				address: "loopback://33".into(),
				tiers,
				roles,
				capabilities: crate::network::transport::NodeCapabilitySet::empty(),
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
		let dest_ds = Arc::new(
			WendelonDataServer::new(
				dest_node.slab.clone(),
				dest_node.raft.clone(),
				dest_node.log_store.clone(),
				dest_node.state_machine.clone(),
				dest_id,
				dest_node.clock.clone(),
				router.clone(),
				dest_factory,
				pool.clone(),
			)
			.unwrap(),
		);
		let source_a_ds = Arc::new(
			WendelonDataServer::new(
				source_a_node.slab.clone(),
				source_a_node.raft.clone(),
				source_a_node.log_store.clone(),
				source_a_node.state_machine.clone(),
				source_a_id,
				source_a_node.clock.clone(),
				router.clone(),
				source_a_factory,
				source_a_node.pool.clone(),
			)
			.unwrap(),
		);
		let source_b_ds = Arc::new(
			WendelonDataServer::new(
				source_b_node.slab.clone(),
				source_b_node.raft.clone(),
				source_b_node.log_store.clone(),
				source_b_node.state_machine.clone(),
				source_b_id,
				source_b_node.clock.clone(),
				router,
				source_b_factory,
				source_b_node.pool.clone(),
			)
			.unwrap(),
		);

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
	async fn seed_source(slab: &SlabManager, locality: u64, hlc_v: HybridTimestamp, value: &[u8]) {
		let batch = kv_batch(&locality.to_le_bytes(), value);
		let bytes = encode_batch(&batch).unwrap();
		slab.append(tenant(), id(locality), hlc_v, &bytes)
			.await
			.unwrap();
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

		let merged = fx.dest.slab.scan_as_of(tenant(), 0, 99, v);
		let mut keys: Vec<u64> = merged.iter().map(|(k, _)| *k).collect();
		keys.sort();
		keys.dedup();
		assert_eq!(keys.len(), 100, "union must cover every key in 0..=99");
		assert_eq!(*keys.first().unwrap(), 0);
		assert_eq!(*keys.last().unwrap(), 99);
	}

	/// Regression for idempotent union: overlapping sources
	/// re-emit the same `(key_identity, hlc, payload_hash)` tuples;
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

		let merged = fx.dest.slab.scan_as_of(tenant(), 0, 99, v);
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

		let merged = fx.dest.slab.scan_as_of(tenant(), 0, 24, v);
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

		let merged = fx.dest.slab.scan_as_of(tenant(), 0, 4, v);
		let mut keys: Vec<u64> = merged.iter().map(|(k, _)| *k).collect();
		keys.sort();
		assert_eq!(keys, vec![0, 1, 2, 3, 4]);
	}

	#[tokio::test]
	async fn dispatcher_rejects_cross_tenant_write_before_handler() {
		let dir = bootstrap_tempdir("auth-write");
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			21,
			dir,
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();

		let mut meta = simple_write_meta(1);
		meta.tenant_id = TenantId::new(9);
		let payload = encode_batch(&single_kv_batch()).unwrap();
		let mut meta_buf = node.pool.acquire();
		meta_buf
			.as_mut_vec()
			.extend_from_slice(bytemuck::bytes_of(&meta));
		let mut arrow_buf = node.pool.acquire();
		arrow_buf.as_mut_vec().extend_from_slice(&payload);
		let frame = HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::WRITE_REQ.0,
				flags: 0,
				stream_id: 1,
				meta_len: meta_buf.len() as u32,
				arrow_len: arrow_buf.len() as u32,
				trailing_len: 0,
			},
			meta: meta_buf,
			arrow_payload: Some(arrow_buf),
			trailing: None,
		};

		let resp = DataRingServer::handle_data_frame(
			node.data_server.as_ref(),
			RequestContext {
				remote: node.local_node,
				actor: actor(ActorRole::Operator, Some("7")),
			},
			frame,
		)
		.await
		.unwrap()
		.unwrap();
		let meta: WriteResponseMeta =
			*bytemuck::from_bytes(&resp.meta[..std::mem::size_of::<WriteResponseMeta>()]);
		assert_eq!(meta.status, WriteStatus::AUTHORIZATION_FAILED);

		node.handles.abort();
	}

	#[tokio::test]
	async fn dispatcher_rejects_read_only_observer_data_read() {
		let dir = bootstrap_tempdir("auth-read");
		let audit_path = dir.join("audit.ndjson");
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			22,
			dir,
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();

		let meta = simple_read_meta(1);
		let key = encode_batch(&single_key_batch()).unwrap();
		let mut meta_buf = node.pool.acquire();
		meta_buf
			.as_mut_vec()
			.extend_from_slice(bytemuck::bytes_of(&meta));
		let mut arrow_buf = node.pool.acquire();
		arrow_buf.as_mut_vec().extend_from_slice(&key);
		let frame = HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::READ_REQ.0,
				flags: 0,
				stream_id: 1,
				meta_len: meta_buf.len() as u32,
				arrow_len: arrow_buf.len() as u32,
				trailing_len: 0,
			},
			meta: meta_buf,
			arrow_payload: Some(arrow_buf),
			trailing: None,
		};

		let resp = DataRingServer::handle_data_frame(
			node.data_server.as_ref(),
			RequestContext {
				remote: node.local_node,
				actor: actor(ActorRole::ReadOnlyObserver, None),
			},
			frame,
		)
		.await
		.unwrap()
		.unwrap();
		let meta: ReadResponseMeta =
			*bytemuck::from_bytes(&resp.meta[..std::mem::size_of::<ReadResponseMeta>()]);
		assert_eq!(meta.status, ReadStatus::AUTHORIZATION_FAILED);
		let audit = wait_for_audit_log(
			&audit_path,
			&[
				"\"event_type\":\"data.read\"",
				"\"phase\":\"denied\"",
				"\"status\":\"authorization_failed\"",
				"\"actor_role\":\"read_only_observer\"",
			],
		)
		.await;
		assert!(audit.contains("\"type\":\"data_access\""));

		node.handles.abort();
	}

	#[tokio::test]
	async fn dispatcher_emits_write_request_and_completion_audit_events() {
		let dir = bootstrap_tempdir("audit-write-events");
		let audit_path = dir.join("audit.ndjson");
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			31,
			dir,
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();
		let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
		loop {
			if node.raft.current_leader().await == Some(node.local_node.id) {
				break;
			}
			assert!(
				tokio::time::Instant::now() < deadline,
				"timed out waiting for leader"
			);
			tokio::time::sleep(Duration::from_millis(20)).await;
		}
		node.data_server
			.refresh_leases_once(&node.ring, &*node.clock, 60_000)
			.await;

		let meta = simple_write_meta(1);
		let payload = encode_batch(&single_kv_batch()).unwrap();
		let mut meta_buf = node.pool.acquire();
		meta_buf
			.as_mut_vec()
			.extend_from_slice(bytemuck::bytes_of(&meta));
		let mut arrow_buf = node.pool.acquire();
		arrow_buf.as_mut_vec().extend_from_slice(&payload);
		let frame = HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::WRITE_REQ.0,
				flags: 0,
				stream_id: 1,
				meta_len: meta_buf.len() as u32,
				arrow_len: arrow_buf.len() as u32,
				trailing_len: 0,
			},
			meta: meta_buf,
			arrow_payload: Some(arrow_buf),
			trailing: None,
		};

		let resp = DataRingServer::handle_data_frame(
			node.data_server.as_ref(),
			RequestContext {
				remote: node.local_node,
				actor: actor(ActorRole::Operator, None),
			},
			frame,
		)
		.await
		.unwrap()
		.unwrap();
		let meta: WriteResponseMeta =
			*bytemuck::from_bytes(&resp.meta[..std::mem::size_of::<WriteResponseMeta>()]);
		assert_eq!(meta.status, WriteStatus::OK);

		let audit = wait_for_audit_log(
			&audit_path,
			&[
				"\"event_type\":\"data.write\"",
				"\"phase\":\"requested\"",
				"\"phase\":\"completed\"",
				"\"status\":\"ok\"",
			],
		)
		.await;
		assert!(audit.contains("\"record_identity\""));

		node.handles.abort();
	}

	#[tokio::test]
	async fn dispatcher_refuses_write_when_data_access_audit_emit_fails() {
		struct RejectingAuditSink;

		impl AuditSink for RejectingAuditSink {
			fn emit(&self, _event: AuditEvent) -> std::io::Result<()> {
				Err(Error::other("simulated data-access audit failure"))
			}
		}

		let dir = bootstrap_tempdir("audit-write-fail-closed");
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			34,
			dir,
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();
		let fail_closed_server = Arc::new(
			WendelonDataServer::new(
				node.slab.clone(),
				node.raft.clone(),
				node.log_store.clone(),
				node.state_machine.clone(),
				node.local_node,
				node.clock.clone(),
				node.data_server.router.clone(),
				node.data_server.factory.clone(),
				node.pool.clone(),
			)
			.unwrap()
			.with_audit_sink(Arc::new(RejectingAuditSink)),
		);

		let meta = simple_write_meta(1);
		let payload = encode_batch(&single_kv_batch()).unwrap();
		let before_bytes = node.slab.local_l1_bytes().await;
		let mut meta_buf = node.pool.acquire();
		meta_buf
			.as_mut_vec()
			.extend_from_slice(bytemuck::bytes_of(&meta));
		let mut arrow_buf = node.pool.acquire();
		arrow_buf.as_mut_vec().extend_from_slice(&payload);
		let frame = HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::WRITE_REQ.0,
				flags: 0,
				stream_id: 1,
				meta_len: meta_buf.len() as u32,
				arrow_len: arrow_buf.len() as u32,
				trailing_len: 0,
			},
			meta: meta_buf,
			arrow_payload: Some(arrow_buf),
			trailing: None,
		};

		let err = DataRingServer::handle_data_frame(
			fail_closed_server.as_ref(),
			RequestContext {
				remote: node.local_node,
				actor: actor(ActorRole::Operator, None),
			},
			frame,
		)
		.await
		.unwrap_err();
		assert!(err.to_string().contains("data access audit emit failed"));
		assert_eq!(
			node.slab.local_l1_bytes().await,
			before_bytes,
			"failed local-chain emission must reject the write before any payload bytes append"
		);

		node.handles.abort();
	}

	#[tokio::test]
	async fn dispatcher_emits_completed_read_audit_event() {
		let dir = bootstrap_tempdir("audit-read-events");
		let audit_path = dir.join("audit.ndjson");
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			32,
			dir,
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();
		let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
		loop {
			if node.raft.current_leader().await == Some(node.local_node.id) {
				break;
			}
			assert!(
				tokio::time::Instant::now() < deadline,
				"timed out waiting for leader"
			);
			tokio::time::sleep(Duration::from_millis(20)).await;
		}
		node.data_server
			.refresh_leases_once(&node.ring, &*node.clock, 60_000)
			.await;
		let (_write_resp, _) = node
			.data_server
			.write(simple_write_meta(1), single_kv_batch())
			.await
			.unwrap();

		let meta = simple_read_meta(1);
		let key = encode_batch(&single_key_batch()).unwrap();
		let mut meta_buf = node.pool.acquire();
		meta_buf
			.as_mut_vec()
			.extend_from_slice(bytemuck::bytes_of(&meta));
		let mut arrow_buf = node.pool.acquire();
		arrow_buf.as_mut_vec().extend_from_slice(&key);
		let frame = HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::READ_REQ.0,
				flags: 0,
				stream_id: 1,
				meta_len: meta_buf.len() as u32,
				arrow_len: arrow_buf.len() as u32,
				trailing_len: 0,
			},
			meta: meta_buf,
			arrow_payload: Some(arrow_buf),
			trailing: None,
		};

		let resp = DataRingServer::handle_data_frame(
			node.data_server.as_ref(),
			RequestContext {
				remote: node.local_node,
				actor: actor(ActorRole::Operator, None),
			},
			frame,
		)
		.await
		.unwrap()
		.unwrap();
		let meta: ReadResponseMeta =
			*bytemuck::from_bytes(&resp.meta[..std::mem::size_of::<ReadResponseMeta>()]);
		assert_eq!(meta.status, ReadStatus::OK);

		let audit = wait_for_audit_log(
			&audit_path,
			&[
				"\"event_type\":\"data.read\"",
				"\"phase\":\"completed\"",
				"\"status\":\"ok\"",
				"\"result_count\":1",
			],
		)
		.await;
		assert!(audit.contains("\"returned_bytes\":1"));

		node.handles.abort();
	}

	#[tokio::test]
	async fn dispatcher_emits_completed_range_query_audit_event() {
		let dir = bootstrap_tempdir("audit-range-query-events");
		let audit_path = dir.join("audit.ndjson");
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			33,
			dir,
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();
		let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
		loop {
			if node.raft.current_leader().await == Some(node.local_node.id) {
				break;
			}
			assert!(
				tokio::time::Instant::now() < deadline,
				"timed out waiting for leader"
			);
			tokio::time::sleep(Duration::from_millis(20)).await;
		}
		node.data_server
			.refresh_leases_once(&node.ring, &*node.clock, 60_000)
			.await;
		let (_write_resp, _) = node
			.data_server
			.write(simple_write_meta(1), single_kv_batch())
			.await
			.unwrap();

		let mut meta = base_meta();
		meta.expected_epoch = 1;
		let mut meta_buf = node.pool.acquire();
		meta_buf
			.as_mut_vec()
			.extend_from_slice(bytemuck::bytes_of(&meta));
		let frame = HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::QUERY_REQ.0,
				flags: 0,
				stream_id: 1,
				meta_len: meta_buf.len() as u32,
				arrow_len: 0,
				trailing_len: 0,
			},
			meta: meta_buf,
			arrow_payload: None,
			trailing: None,
		};

		let resp = DataRingServer::handle_data_frame(
			node.data_server.as_ref(),
			RequestContext {
				remote: node.local_node,
				actor: actor(ActorRole::Operator, None),
			},
			frame,
		)
		.await
		.unwrap()
		.unwrap();
		let meta: QueryResponseMeta =
			*bytemuck::from_bytes(&resp.meta[..std::mem::size_of::<QueryResponseMeta>()]);
		assert_eq!(meta.status, ReadStatus::OK);
		assert_eq!(meta.rows, 1);

		let audit = wait_for_audit_log(
			&audit_path,
			&[
				"\"event_type\":\"data.range_query\"",
				"\"phase\":\"completed\"",
				"\"status\":\"ok\"",
				"\"result_count\":1",
			],
		)
		.await;
		assert!(audit.contains("\"locality_end\":18446744073709551615"));

		node.handles.abort();
	}

	#[tokio::test]
	async fn dispatcher_rejects_tenant_scoped_actor_opening_finalization_stream() {
		use tokio::io::AsyncWriteExt;

		let dir = bootstrap_tempdir("auth-stream-fin");
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			23,
			dir,
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();

		let request = StreamFinalizationRequestMeta {
			start_log_index: 0,
			epoch: 1,
		};
		let first_header = FrameHeader {
			magic: HRFT_MAGIC,
			version: 1,
			msg_type: MsgType::STREAM_FIN_REQ.0,
			flags: 0,
			stream_id: 1,
			meta_len: std::mem::size_of::<StreamFinalizationRequestMeta>() as u32,
			arrow_len: 0,
			trailing_len: 0,
		};
		let (mut client_side, server_side) = tokio::io::duplex(1024);
		client_side
			.write_all(bytemuck::bytes_of(&request))
			.await
			.unwrap();
		client_side.shutdown().await.unwrap();

		let err = DataRingServer::handle_data_stream(
			node.data_server.as_ref(),
			RequestContext {
				remote: node.local_node,
				actor: actor(ActorRole::Operator, Some("7")),
			},
			first_header,
			Box::new(DuplexTransportStream { inner: server_side }),
			node.pool.clone(),
		)
		.await
		.unwrap_err();
		assert_eq!(err.kind(), ErrorKind::PermissionDenied);
		assert!(err.to_string().contains("STREAM_FIN_REQ"));

		node.handles.abort();
	}

	#[tokio::test]
	async fn dispatcher_backpressures_write_when_tenant_limit_is_saturated() {
		let dir = bootstrap_tempdir("bp-write-tenant");
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			24,
			dir,
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();
		node.data_server
			.set_request_backpressure_limits_for_test(1, 1);

		let tenant_actor = actor(ActorRole::Operator, Some("0"));
		let _guard = node
			.data_server
			.try_admit_data_request(&tenant_actor, Some(tenant()), DataOperation::Write)
			.unwrap()
			.unwrap();

		let before = metrics()
			.data_ring_backpressure_rejections_tenant_total
			.get();
		let meta = simple_write_meta(1);
		let payload = encode_batch(&single_kv_batch()).unwrap();
		let mut meta_buf = node.pool.acquire();
		meta_buf
			.as_mut_vec()
			.extend_from_slice(bytemuck::bytes_of(&meta));
		let mut arrow_buf = node.pool.acquire();
		arrow_buf.as_mut_vec().extend_from_slice(&payload);
		let frame = HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::WRITE_REQ.0,
				flags: 0,
				stream_id: 1,
				meta_len: meta_buf.len() as u32,
				arrow_len: arrow_buf.len() as u32,
				trailing_len: 0,
			},
			meta: meta_buf,
			arrow_payload: Some(arrow_buf),
			trailing: None,
		};

		let resp = DataRingServer::handle_data_frame(
			node.data_server.as_ref(),
			RequestContext {
				remote: node.local_node,
				actor: tenant_actor,
			},
			frame,
		)
		.await
		.unwrap()
		.unwrap();
		let meta: WriteResponseMeta =
			*bytemuck::from_bytes(&resp.meta[..std::mem::size_of::<WriteResponseMeta>()]);
		assert_eq!(meta.status, WriteStatus::BACKPRESSURED);
		assert_eq!(
			metrics()
				.data_ring_backpressure_rejections_tenant_total
				.get(),
			before + 1
		);

		node.handles.abort();
	}

	#[tokio::test]
	async fn dispatcher_backpressures_cross_tenant_read_when_actor_limit_is_saturated() {
		let dir = bootstrap_tempdir("bp-read-actor");
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			25,
			dir,
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();
		node.data_server
			.set_request_backpressure_limits_for_test(8, 1);

		let operator = actor(ActorRole::Operator, None);
		let _guard = node
			.data_server
			.try_admit_data_request(&operator, Some(tenant()), DataOperation::Read)
			.unwrap()
			.unwrap();

		let before = metrics()
			.data_ring_backpressure_rejections_actor_total
			.get();
		let mut meta = simple_read_meta(1);
		meta.tenant_id = TenantId::new(9);
		meta.key_identity = Identity::blake3_tenant(meta.tenant_id, b"k");
		let key = encode_batch(&single_key_batch()).unwrap();
		let mut meta_buf = node.pool.acquire();
		meta_buf
			.as_mut_vec()
			.extend_from_slice(bytemuck::bytes_of(&meta));
		let mut arrow_buf = node.pool.acquire();
		arrow_buf.as_mut_vec().extend_from_slice(&key);
		let frame = HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::READ_REQ.0,
				flags: 0,
				stream_id: 1,
				meta_len: meta_buf.len() as u32,
				arrow_len: arrow_buf.len() as u32,
				trailing_len: 0,
			},
			meta: meta_buf,
			arrow_payload: Some(arrow_buf),
			trailing: None,
		};

		let resp = DataRingServer::handle_data_frame(
			node.data_server.as_ref(),
			RequestContext {
				remote: node.local_node,
				actor: operator,
			},
			frame,
		)
		.await
		.unwrap()
		.unwrap();
		let meta: ReadResponseMeta =
			*bytemuck::from_bytes(&resp.meta[..std::mem::size_of::<ReadResponseMeta>()]);
		assert_eq!(meta.status, ReadStatus::BACKPRESSURED);
		assert_eq!(
			metrics()
				.data_ring_backpressure_rejections_actor_total
				.get(),
			before + 1
		);

		node.handles.abort();
	}

	#[tokio::test]
	async fn query_stream_backpressure_response_marks_stream_end() {
		use tokio::io::AsyncWriteExt;

		let dir = bootstrap_tempdir("bp-query-stream-end");
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			26,
			dir,
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();
		node.data_server
			.set_request_backpressure_limits_for_test(1, 1);

		let tenant_actor = actor(ActorRole::Operator, Some("0"));
		let _guard = node
			.data_server
			.try_admit_data_request(&tenant_actor, Some(tenant()), DataOperation::Query)
			.unwrap()
			.unwrap();

		let meta = base_meta();
		let first_header = FrameHeader {
			magic: HRFT_MAGIC,
			version: 1,
			msg_type: MsgType::QUERY_REQ.0,
			flags: 0,
			stream_id: 7,
			meta_len: std::mem::size_of::<QueryRequestMeta>() as u32,
			arrow_len: 0,
			trailing_len: 0,
		};
		let (mut client_side, server_side) = tokio::io::duplex(1024);
		client_side
			.write_all(bytemuck::bytes_of(&meta))
			.await
			.unwrap();
		client_side.shutdown().await.unwrap();

		DataRingServer::handle_data_stream(
			node.data_server.as_ref(),
			RequestContext {
				remote: node.local_node,
				actor: tenant_actor,
			},
			first_header,
			Box::new(DuplexTransportStream { inner: server_side }),
			node.pool.clone(),
		)
		.await
		.unwrap();

		let response = read_frame(&mut client_side, &node.pool).await.unwrap();
		assert_ne!(response.header.flags & FRAME_FLAG_STREAM_END, 0);
		let meta: QueryResponseMeta =
			*bytemuck::from_bytes(&response.meta[..std::mem::size_of::<QueryResponseMeta>()]);
		assert_eq!(meta.status, ReadStatus::BACKPRESSURED);

		node.handles.abort();
	}

	#[tokio::test]
	async fn cached_tenant_policy_recovers_after_cache_lock_poison() {
		let dir = bootstrap_tempdir("tenant-policy-poison");
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			27,
			dir,
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();

		let cache = node.data_server.tenant_policy_cache.clone();
		let _ = std::panic::catch_unwind(AssertUnwindSafe(|| {
			let _guard = cache.write().unwrap();
			panic!("poison tenant policy cache");
		}));

		let _ = node.data_server.cached_tenant_policy(TenantId::ZERO).await;

		node.handles.abort();
	}

	#[tokio::test]
	async fn lease_gate_recovers_after_cache_lock_poison() {
		let dir = bootstrap_tempdir("lease-cache-poison");
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			28,
			dir,
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();

		let lease = node.data_server.make_signed_lease(
			1,
			node.local_node,
			0,
			100,
			hlc(10, 0, node.local_node.id),
			hlc(10_000, 0, node.local_node.id),
		);
		node.data_server.cache_granted_lease(lease);

		let leases = node.data_server.leases.clone();
		let _ = std::panic::catch_unwind(AssertUnwindSafe(|| {
			let _guard = leases.write().unwrap();
			panic!("poison lease cache");
		}));

		assert_eq!(
			node.data_server
				.lease_gate_result(Locality::ZERO, 1, hlc(11, 0, node.local_node.id))
				.await,
			LeaseGateResult::Valid
		);

		node.handles.abort();
	}

	// ============================================================
	//  ADR 045 Phase 8.5 — projected fast path AppendOnly
	//  table-isolation verification
	// ============================================================

	#[test]
	fn augment_requirement_includes_table_object_id_for_append_only() {
		use crate::schema::TenantId;
		let descriptor = TableSchemaDescriptorV1::new_with_layout(
			TableObjectId::new([0x45; 16]),
			1,
			"analytics",
			"events",
			&arrow::datatypes::Schema::new(vec![arrow::datatypes::Field::new(
				"value",
				arrow::datatypes::DataType::Int64,
				false,
			)]),
			RowIdentityPolicy::AppendOnly,
			LocalityPolicyV1::default_table_spread(),
			RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::default()),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.unwrap();
		let _ = TenantId::new(7);
		let req = ColumnFetchRequirement::default();
		let augmented = augment_requirement_for_descriptor_verification(&req, &descriptor);
		assert!(
			augmented
				.storage_columns
				.contains(crate::storage::columnar::StorageColumn::RowTableObjectId),
			"AppendOnly descriptor-filtered fetch must include `__row_table_object_id__`"
		);
	}

	#[test]
	fn augment_requirement_includes_table_object_id_for_pk_multirow() {
		let descriptor = TableSchemaDescriptorV1::new_with_layout(
			TableObjectId::new([0x44; 16]),
			1,
			"analytics",
			"events_pk",
			&arrow::datatypes::Schema::new(vec![
				arrow::datatypes::Field::new("id", arrow::datatypes::DataType::LargeBinary, false),
				arrow::datatypes::Field::new("value", arrow::datatypes::DataType::Int64, false),
			]),
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
			LocalityPolicyV1::default_table_spread(),
			RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::default()),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.unwrap();
		let req = ColumnFetchRequirement::default();
		let augmented = augment_requirement_for_descriptor_verification(&req, &descriptor);
		assert!(
			augmented
				.storage_columns
				.contains(crate::storage::columnar::StorageColumn::RowTableObjectId),
			"multi-row PK descriptor must pull `__row_table_object_id__` into the projected \
			 fetch — Phase 8 stamps it on every multi-row record (PK + AppendOnly)"
		);
	}

	#[test]
	fn augment_requirement_unchanged_for_pk_single_row() {
		let descriptor = TableSchemaDescriptorV1::new(
			TableObjectId::new([0x46; 16]),
			1,
			"analytics",
			"events_pk_single",
			&arrow::datatypes::Schema::new(vec![
				arrow::datatypes::Field::new("id", arrow::datatypes::DataType::LargeBinary, false),
				arrow::datatypes::Field::new("value", arrow::datatypes::DataType::Int64, false),
			]),
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
			LocalityPolicyV1::default_table_spread(),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.unwrap();
		let req = ColumnFetchRequirement::default();
		let augmented = augment_requirement_for_descriptor_verification(&req, &descriptor);
		assert!(
			!augmented
				.storage_columns
				.contains(crate::storage::columnar::StorageColumn::RowTableObjectId),
			"single-row PK records carry no synthetic columns; descriptor-filtered fetch \
			 must not pull a column that is not on disk"
		);
	}

	#[test]
	fn projected_match_falls_back_when_table_object_id_column_missing() {
		// AppendOnly descriptor + projected batch without
		// `__row_table_object_id__` (e.g., a sibling PK record
		// under a colliding table-spread prefix). The verifier
		// must return false so the caller falls back to the
		// whole-record path.
		use std::collections::BTreeMap;
		use std::sync::Arc;
		let descriptor = TableSchemaDescriptorV1::new_with_layout(
			TableObjectId::new([0x45; 16]),
			1,
			"analytics",
			"events",
			&arrow::datatypes::Schema::new(vec![arrow::datatypes::Field::new(
				"value",
				arrow::datatypes::DataType::Int64,
				false,
			)]),
			RowIdentityPolicy::AppendOnly,
			LocalityPolicyV1::default_table_spread(),
			RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::default()),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.unwrap();
		let batch = arrow::record_batch::RecordBatch::try_new(
			Arc::new(arrow::datatypes::Schema::new(vec![
				arrow::datatypes::Field::new("value", arrow::datatypes::DataType::Int64, false),
			])),
			vec![Arc::new(arrow::array::Int64Array::from(vec![1])) as arrow::array::ArrayRef],
		)
		.unwrap();
		let projected = ProjectedRecordBatch {
			batch,
			ordinal_mapping: crate::storage::columnar::ColumnOrdinalMapping::default(),
			storage_mapping: BTreeMap::new(),
		};
		let result = projected_descriptor_match(&descriptor, &projected).unwrap();
		assert!(
			!result,
			"AppendOnly projected verifier must force fallback when \
			 `__row_table_object_id__` is absent from the projected fetch"
		);
	}

	#[test]
	fn projected_match_rejects_mismatched_table_object_id() {
		use std::collections::BTreeMap;
		use std::sync::Arc;
		let descriptor = TableSchemaDescriptorV1::new_with_layout(
			TableObjectId::new([0x45; 16]),
			1,
			"analytics",
			"events_a",
			&arrow::datatypes::Schema::new(vec![arrow::datatypes::Field::new(
				"value",
				arrow::datatypes::DataType::Int64,
				false,
			)]),
			RowIdentityPolicy::AppendOnly,
			LocalityPolicyV1::default_table_spread(),
			RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::default()),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.unwrap();
		// Stored row's table_object_id stamped from a *different*
		// AppendOnly table (same tenant, same Arrow schema,
		// colliding table-spread prefix would put them on the
		// same range).
		let stored_id = [0xBB_u8; 16];
		let mut id_builder = arrow::array::FixedSizeBinaryBuilder::with_capacity(1, 16);
		id_builder.append_value(&stored_id).unwrap();
		let id_array = Arc::new(id_builder.finish()) as arrow::array::ArrayRef;
		let value_array =
			Arc::new(arrow::array::Int64Array::from(vec![1])) as arrow::array::ArrayRef;
		let batch = arrow::record_batch::RecordBatch::try_new(
			Arc::new(arrow::datatypes::Schema::new(vec![
				arrow::datatypes::Field::new("value", arrow::datatypes::DataType::Int64, false),
				arrow::datatypes::Field::new(
					"__row_table_object_id__",
					arrow::datatypes::DataType::FixedSizeBinary(16),
					false,
				),
			])),
			vec![value_array, id_array],
		)
		.unwrap();
		let mut storage_mapping = BTreeMap::new();
		storage_mapping.insert(crate::storage::columnar::StorageColumn::RowTableObjectId, 1);
		let projected = ProjectedRecordBatch {
			batch,
			ordinal_mapping: crate::storage::columnar::ColumnOrdinalMapping::default(),
			storage_mapping,
		};
		let result = projected_descriptor_match(&descriptor, &projected).unwrap();
		assert!(
			!result,
			"AppendOnly projected verifier must reject rows stamped with a \
			 different table_object_id than the candidate descriptor"
		);
	}

	#[test]
	fn projected_match_rejects_pk_multirow_with_mismatched_table_object_id() {
		use std::collections::BTreeMap;
		use std::sync::Arc;
		let schema = arrow::datatypes::Schema::new(vec![
			arrow::datatypes::Field::new("id", arrow::datatypes::DataType::LargeBinary, false),
			arrow::datatypes::Field::new("value", arrow::datatypes::DataType::Int64, false),
		]);
		let descriptor = TableSchemaDescriptorV1::new_with_layout(
			TableObjectId::new([0xAA; 16]),
			1,
			"analytics",
			"events_pk",
			&schema,
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
			LocalityPolicyV1::default_table_spread(),
			RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::default()),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.unwrap();
		// Row stamped with a sibling PK descriptor's table_object_id.
		let stored_id = [0xBB_u8; 16];
		let mut id_builder = arrow::array::FixedSizeBinaryBuilder::with_capacity(1, 16);
		id_builder.append_value(&stored_id).unwrap();
		let stamp_array = Arc::new(id_builder.finish()) as arrow::array::ArrayRef;
		let id_array = Arc::new(arrow::array::LargeBinaryArray::from(vec![&b"k1"[..]]))
			as arrow::array::ArrayRef;
		let value_array =
			Arc::new(arrow::array::Int64Array::from(vec![1])) as arrow::array::ArrayRef;
		let batch = arrow::record_batch::RecordBatch::try_new(
			Arc::new(arrow::datatypes::Schema::new(vec![
				arrow::datatypes::Field::new("id", arrow::datatypes::DataType::LargeBinary, false),
				arrow::datatypes::Field::new("value", arrow::datatypes::DataType::Int64, false),
				arrow::datatypes::Field::new(
					"__row_table_object_id__",
					arrow::datatypes::DataType::FixedSizeBinary(16),
					false,
				),
			])),
			vec![id_array, value_array, stamp_array],
		)
		.unwrap();
		let mut storage_mapping = BTreeMap::new();
		storage_mapping.insert(crate::storage::columnar::StorageColumn::RowTableObjectId, 2);
		let projected = ProjectedRecordBatch {
			batch,
			ordinal_mapping: crate::storage::columnar::ColumnOrdinalMapping::default(),
			storage_mapping,
		};
		let result = projected_descriptor_match(&descriptor, &projected).unwrap();
		assert!(
			!result,
			"PK multi-row projected verifier must reject rows stamped with a \
			 different table_object_id (Phase 8 PK extension)"
		);
	}

	#[test]
	fn projected_match_accepts_same_table_object_id() {
		use std::collections::BTreeMap;
		use std::sync::Arc;
		let descriptor = TableSchemaDescriptorV1::new_with_layout(
			TableObjectId::new([0x45; 16]),
			1,
			"analytics",
			"events_a",
			&arrow::datatypes::Schema::new(vec![arrow::datatypes::Field::new(
				"value",
				arrow::datatypes::DataType::Int64,
				false,
			)]),
			RowIdentityPolicy::AppendOnly,
			LocalityPolicyV1::default_table_spread(),
			RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::default()),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.unwrap();
		let stored_id = [0x45_u8; 16];
		let mut id_builder = arrow::array::FixedSizeBinaryBuilder::with_capacity(1, 16);
		id_builder.append_value(&stored_id).unwrap();
		let id_array = Arc::new(id_builder.finish()) as arrow::array::ArrayRef;
		let value_array =
			Arc::new(arrow::array::Int64Array::from(vec![1])) as arrow::array::ArrayRef;
		let batch = arrow::record_batch::RecordBatch::try_new(
			Arc::new(arrow::datatypes::Schema::new(vec![
				arrow::datatypes::Field::new("value", arrow::datatypes::DataType::Int64, false),
				arrow::datatypes::Field::new(
					"__row_table_object_id__",
					arrow::datatypes::DataType::FixedSizeBinary(16),
					false,
				),
			])),
			vec![value_array, id_array],
		)
		.unwrap();
		let mut storage_mapping = BTreeMap::new();
		storage_mapping.insert(crate::storage::columnar::StorageColumn::RowTableObjectId, 1);
		let projected = ProjectedRecordBatch {
			batch,
			ordinal_mapping: crate::storage::columnar::ColumnOrdinalMapping::default(),
			storage_mapping,
		};
		let result = projected_descriptor_match(&descriptor, &projected).unwrap();
		assert!(
			result,
			"AppendOnly projected verifier must accept rows stamped with the \
			 same table_object_id as the candidate descriptor"
		);
	}

	// ============================================================
	//  ADR 045 Phase 8 idempotency canonicalization — retry-stable
	//  derivations off `ClientWriteId` must hash only canonical
	//  bytes (`client_uuid` + `sequence_no`).
	// ============================================================

	#[test]
	fn table_write_parent_identity_ignores_idempotency_ttl_and_pad() {
		let mut base: TableWriteRequestMeta = bytemuck::Zeroable::zeroed();
		base.tenant_id = TenantId::new(11);
		base.table_object_id = TableObjectId::new([0x91; 16]);
		base.schema_generation = 5;
		base.row_count = 3;
		base.parent_write_id = ClientWriteId {
			client_uuid: [0xAA; 16],
			sequence_no: 17,
			idempotency_ttl_ms: 60_000,
			_pad: 0,
		};
		let mut drifted = base;
		drifted.parent_write_id.idempotency_ttl_ms = 12_345;
		drifted.parent_write_id._pad = 0xDEAD_BEEF;
		assert_eq!(
			table_write_parent_identity(&base),
			table_write_parent_identity(&drifted),
			"table_write_parent_identity must depend only on the canonical \
			 idempotency identity (`client_uuid` + `sequence_no`)"
		);
	}
}
