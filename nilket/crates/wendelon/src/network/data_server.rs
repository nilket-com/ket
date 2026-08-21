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
//! ## Scope and simplifications
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
use crate::client::RowIdentityProjection;
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
use crate::network::core::{
	HRFT_MAGIC, MAX_SECTION_BYTES, read_frame, read_frame_from_header, write_frame,
};
use crate::network::factory::TransportProviderFactory;
use crate::network::ipc::{
	observe_wire_encode_error, observe_wire_encode_success, wire_encode_batch_for_path,
};
use crate::network::lease_signing::LeaseAuthority;
use crate::network::mutation_outcomes::{
	DurableMutationOutcome, MutationOutcomeStore, MutationResolution, mutation_outcomes_path,
};
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
	CounterAddRequestMeta, CounterAddResponseMeta, CounterReadRequestMeta, CounterReadResponseMeta,
	CounterReplicaAck, CounterReplicaPhase, CounterReplicaStatus, CounterReplicateRequestMeta,
	CounterRollbackRequestMeta, FRAME_FLAG_STREAM_END, FinalizationMarker, FrameHeader,
	HybridTimestamp, Identity, LeaseDescriptor, LeaseStatus, Locality, LocalityPolicy,
	LocalityRange, MigrateRequestKind, MigrateRequestMeta, MigrateResponseMeta, MigrateStatus,
	MsgType, MultiRowLayoutDescriptor, MutationCommitKnowledge, MutationOperationKind,
	MutationOutcomePinAction, MutationOutcomePinRequestMeta, MutationOutcomePinResponseMeta,
	MutationOutcomePinStatus, MutationOutcomeRequestMeta, MutationOutcomeResponseMeta,
	MutationResolutionStatus, MutationStopReason, NodeId, PbcRejectCode,
	PendingWitnessPullRequestMeta, PendingWitnessPullResponseMeta, PendingWitnessPullStatus,
	QueryRequestMeta, QueryResponseMeta, ReadRequestMeta, ReadResponseMeta, ReadStatus,
	RecordBodyCompressionPolicy, RecordLayoutPolicy, RecordRowIndex, ReplicaAckLevel,
	ReplicaWriteMeta, ReplicaWriteRespMeta, ReplicaWriteStatus, RowCount, RowIdentityPolicy,
	RuntimeFilterDescriptor, RuntimeFilterDropReason, STORAGE_ROW_KEY_ID_COLUMN,
	SecondaryIndexCatalog, SecondaryIndexDescriptor, SecondaryIndexState, SlabId,
	StreamFinalizationRequestMeta, TABLE_QUERY_TRACE_FLAG_KNOWN_MASK,
	TABLE_QUERY_TRACE_FLAG_PRESENT, TABLE_QUERY_TRACE_FLAG_SAMPLED, TABLE_WRITE_MAX_ROWS,
	TableDeleteRowsRequestMeta, TableDeleteRowsResponseMeta, TableObjectId,
	TableQueryCancelRequestMeta, TableQueryCancelResponseMeta, TableQueryCancelStatus,
	TableQueryRequestId, TableQueryRequestMeta, TableReferenceIntegrityFailureMeta,
	TableReplicaWriteMeta, TableSchemaDescriptor, TableWriteAdmission, TableWriteRequestMeta,
	TableWriteResponseMeta, TableWriteValidationError, TenantId, WriteRequestMeta,
	WriteResponseMeta, WriteStatus, WriterNodeId, build_runtime_filter_mask,
	build_runtime_filter_mask_for_batch, cdc_cursor_in_catchup_window, cdc_event_batch,
	cdc_tombstone_event_batch, cdc_visible_rows_digest, column_projection_predicate_ranges,
	column_projection_prefix_ranges, encode_aggregate_state, encode_table_write_row_count,
	migration_record_schema, pbc, query_request_stats_pruning_enabled, read_key_schema,
	runtime_filter_key_type, table_spread_prefix_ranges, validate_runtime_filter_descriptor,
	validate_table_write_request_batch, write_payload_schema,
};
#[cfg(test)]
use crate::schema::{SupersessionMetadata, TombstoneMetadata};
use crate::storage::columnar::{
	ColumnFetchError, ColumnFetchRequirement, L2FetchStrategy, ProjectedRecordBatch,
	analyze_aggregate_query_column_requirement, analyze_pbc_column_requirement,
	execute_aggregate_projected_record_batch, execute_pbc_projected_record_batch,
};
use crate::storage::core::{RecordLocation, StorageEngine, is_zero_hlc};
use crate::storage::encoding::{EncodingWriterConfig, L2ChunkLayoutConfig};
use crate::storage::manager::{
	EncodedAggregateL2RecordRead, EncodingMaterializeAuditSample, SlabManager,
};
use crate::storage::multirow::{
	MultiRowSourceRow, decode_batch, encode_batch, encode_batch_with_policy,
	has_storage_synthetic_columns, materialize_record_location_batch,
	pack_admitted_record_batch_by_bucket_with_row_group_encoding_and_chunk_layout_config,
	pack_tombstone_multirow_source_rows_with_policy, replace_storage_row_hlc_column,
	storage_index_entries_from_batch, storage_row_is_tombstone_at, storage_row_table_object_id_at,
};
use crate::storage::pk_delete::{
	PkAdmissionOutcome, admit_pk_delete_row, admit_pk_write_row, row_visible_for_admission,
};
use crate::storage::row_group::RowGroupWriterConfig;
use crate::storage::stats::{
	COLUMN_STAT_HAS_MAX, COLUMN_STAT_HAS_MIN, COLUMN_STAT_MAX_TRUNCATED, COLUMN_STAT_MIN_TRUNCATED,
	ColumnStatsBlockEntry, ColumnStatsPrune, ParsedColumnStatsBlock,
	column_stats_entry_matches_data_type, decode_column_stats_payload, encode_signed_stat_value,
	prune_by_encoded_any_of, prune_by_encoded_min_max,
};
use crate::storage::{
	CatchupSnapshot, CdcLiveEntry, CdcLiveTailRing, CdcLocalityFrontiers,
	CdcLocalityHeartbeatWorker, DEFAULT_MAX_CDC_LIVE_TAIL_BYTES_PER_LOCALITY,
	cdc_locality_heartbeat_finalization,
};
use crate::storage::{
	EffectiveRetention, MvccRetentionPolicy, SecondaryIndexAccessPlan, SecondaryIndexLookupRow,
	TableLookupByIndexRequest, aggregate_record_batches, check_read_against_retention,
	cluster_retention_floor, effective_table_retention, merge_aggregate_responses,
	secondary_index_access_plan_from_program, secondary_index_entry_from_materialized_row,
	validate_expression_types,
};
use arrow::array::{
	Array, ArrayRef, FixedSizeBinaryArray, FixedSizeBinaryBuilder, LargeBinaryArray,
	LargeBinaryBuilder, UInt32Array, UInt32Builder, UInt64Array, UInt64Builder,
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
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::io::{Cursor, Error, ErrorKind};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, RwLock};
use std::time::{Duration, Instant};
use tokio::sync::{Notify, OwnedMutexGuard, OwnedSemaphorePermit, Semaphore, mpsc, watch};
use tokio::task::{JoinHandle, JoinSet, spawn_blocking};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, Span, field};

use crate::network::core::HrftFrame;
use crate::network::traits::DataRing;

// ADR 077: handler families and their colocated helpers live in domain submodules;
// the struct, construction, and MsgType dispatch stay in this module root.
mod counter;
mod replica_routing;
pub use replica_routing::{ReplicaRouter, StaticReplicaRouter};
mod table_query;
use table_query::{QueryProjectedExecution, RowGroupFetchAuditCounters};
mod write_replication;
use write_replication::{DurableQuorumTracker, PendingFinalizationClaims, WriteOutcome};
mod descriptor_migration;
mod query_clocks;
use query_clocks::{
	QueryClocks, caller_deadline_from_budget_millis, caller_deadline_from_remaining_millis,
};
mod query_encode;
use query_encode::encode_query_stream_batches_with_deadline;
mod write_clocks;
use write_clocks::{WriteClocks, acquire_mutation_work_permit, bound_orphan_witness_pull};
mod mutation_protocol;
use mutation_protocol::mutation_expiry_response;
mod streaming;
use counter::{CounterParticipantFactory, CounterReplicaNetworkTransport};
use descriptor_migration::MigrationRegistry;

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

impl
	crate::storage::counter::CounterParticipantResolver<
		crate::storage::counter::CounterParticipantKind,
	> for CounterParticipantFactory
{
	fn resolve_recorded(
		&self,
		id: NodeId,
	) -> Option<crate::storage::counter::CounterParticipantKind> {
		Some(self.participant_for(id))
	}
}

#[derive(Clone, Copy, Debug)]
struct CachedTableDeleteOutcome {
	response: TableDeleteRowsResponseMeta,
	trailing: Option<FinalizationMarker>,
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
	/// ADR 542: a node-wide cap on mutation work that can outlive a caller. The
	/// permit moves into a spawned local durable append and is released only when
	/// that work actually finishes, never merely when the caller stops waiting.
	mutation_work_slots: Arc<Semaphore>,
	/// ADR 542 durable terminal/expiry outcomes. Unlike the process-local replay
	/// maps, this store is reopened at bootstrap and supports resolution without
	/// re-submitting the mutation.
	mutation_outcomes: Arc<MutationOutcomeStore>,
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
	/// ADR 075 server-side counter replica. Identity-based, over the same
	/// `SlabManager`; drives counter tentative/commit/rollback for incoming
	/// `COUNTER_REPLICATE_REQ` / `COUNTER_ROLLBACK_REQ`. It intentionally never
	/// touches `pending_finalizations` (a counter tentative carries no witness).
	counter_replica: Arc<crate::storage::counter::CounterReplicaService>,
	/// ADR 075/076: per-tenant counter leaders over production participants.
	/// Fresh per-key routing is derived by the data-server for each add; durable
	/// witnesses keep peer identities so recovery can reconcile the recorded set.
	counter_leaders: Arc<
		DashMap<
			TenantId,
			Arc<
				crate::storage::counter::CounterLeader<
					crate::storage::counter::CounterParticipantKind,
				>,
			>,
		>,
	>,
	/// ADR 075: per-tenant counter-leader init gate. Lazy leader construction is
	/// RECOVERY (file I/O: applied-log + witness load, then reconcile), so
	/// concurrent first requests for a tenant must build exactly one leader —
	/// and never hold a `counter_leaders` entry lock across an await. D4 public
	/// idempotency itself lives in the leader (keyed by the canonical
	/// `ClientWriteId` bytes, durable with the witness/applied log), NOT in a
	/// server-side cache that dies with the process.
	counter_leader_init: Arc<DashMap<TenantId, Arc<tokio::sync::Mutex<()>>>>,
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
	#[cfg(test)]
	delay_next_local_durable_append: Arc<StdMutex<Option<Duration>>>,
	#[cfg(test)]
	orphan_witness_recovery_leadership_override: Arc<StdMutex<Option<bool>>>,
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
	/// Active TABLE_QUERY_REQ attempts and bounded cancel tombstones.
	table_query_cancellation: Arc<ActiveTableQueryRegistry>,
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
const TABLE_QUERY_CANCEL_TOMBSTONE_TTL: Duration = Duration::from_secs(60);
const TABLE_QUERY_CANCEL_TOMBSTONE_CAP: usize = 1024;

type TableQueryCancellationKey = (TenantId, TableQueryRequestId);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TableQueryTombstoneKind {
	Pending,
	Terminal,
}

#[derive(Clone, Copy, Debug)]
struct TableQueryTombstoneOrder {
	key: TableQueryCancellationKey,
	kind: TableQueryTombstoneKind,
	expires_at: Instant,
}

#[derive(Default)]
struct ActiveTableQueryRegistryState {
	active: HashMap<TableQueryCancellationKey, Arc<CancellationToken>>,
	pending: HashMap<TableQueryCancellationKey, Instant>,
	terminal: HashMap<TableQueryCancellationKey, Instant>,
	order: VecDeque<TableQueryTombstoneOrder>,
}

#[derive(Default)]
struct ActiveTableQueryRegistry {
	state: StdMutex<ActiveTableQueryRegistryState>,
}

enum ActiveTableQueryRegistration {
	Active(ActiveTableQueryGuard),
	CancelledBeforeStart,
}

struct ActiveTableQueryGuard {
	registry: Arc<ActiveTableQueryRegistry>,
	key: TableQueryCancellationKey,
	token: Arc<CancellationToken>,
}

impl ActiveTableQueryGuard {
	fn token(&self) -> CancellationToken {
		self.token.as_ref().clone()
	}
}

impl Drop for ActiveTableQueryGuard {
	fn drop(&mut self) {
		self.registry.finish(self.key, &self.token);
	}
}

impl ActiveTableQueryRegistry {
	fn register(
		self: &Arc<Self>,
		tenant_id: TenantId,
		request_id: TableQueryRequestId,
	) -> ActiveTableQueryRegistration {
		let key = (tenant_id, request_id);
		let now = Instant::now();
		let mut state = self.state.lock().unwrap_or_else(|poisoned| {
			tracing::error!("recovering poisoned table-query cancellation registry");
			poisoned.into_inner()
		});
		Self::prune_locked(&mut state, now);
		state.terminal.remove(&key);
		if state.pending.remove(&key).is_some() {
			return ActiveTableQueryRegistration::CancelledBeforeStart;
		}
		let token = Arc::new(CancellationToken::new());
		if let Some(previous) = state.active.insert(key, token.clone()) {
			previous.cancel();
		}
		ActiveTableQueryRegistration::Active(ActiveTableQueryGuard {
			registry: self.clone(),
			key,
			token,
		})
	}

	fn cancel(
		&self,
		tenant_id: TenantId,
		request_id: TableQueryRequestId,
	) -> TableQueryCancelStatus {
		let key = (tenant_id, request_id);
		let now = Instant::now();
		let mut state = self.state.lock().unwrap_or_else(|poisoned| {
			tracing::error!("recovering poisoned table-query cancellation registry");
			poisoned.into_inner()
		});
		Self::prune_locked(&mut state, now);
		if let Some(token) = state.active.get(&key) {
			token.cancel();
			return TableQueryCancelStatus::ACCEPTED;
		}
		if state.terminal.contains_key(&key) {
			return TableQueryCancelStatus::TERMINAL;
		}
		Self::insert_tombstone_locked(&mut state, key, TableQueryTombstoneKind::Pending, now);
		TableQueryCancelStatus::ACCEPTED
	}

	fn finish(&self, key: TableQueryCancellationKey, token: &Arc<CancellationToken>) {
		let now = Instant::now();
		let mut state = self.state.lock().unwrap_or_else(|poisoned| {
			tracing::error!("recovering poisoned table-query cancellation registry");
			poisoned.into_inner()
		});
		let remove = state
			.active
			.get(&key)
			.is_some_and(|active| Arc::ptr_eq(active, token));
		if remove {
			state.active.remove(&key);
			Self::insert_tombstone_locked(&mut state, key, TableQueryTombstoneKind::Terminal, now);
		}
	}

	fn insert_tombstone_locked(
		state: &mut ActiveTableQueryRegistryState,
		key: TableQueryCancellationKey,
		kind: TableQueryTombstoneKind,
		now: Instant,
	) {
		let expires_at = now + TABLE_QUERY_CANCEL_TOMBSTONE_TTL;
		match kind {
			TableQueryTombstoneKind::Pending => {
				state.pending.insert(key, expires_at);
			}
			TableQueryTombstoneKind::Terminal => {
				state.terminal.insert(key, expires_at);
			}
		}
		state.order.push_back(TableQueryTombstoneOrder {
			key,
			kind,
			expires_at,
		});
		Self::prune_locked(state, now);
	}

	fn prune_locked(state: &mut ActiveTableQueryRegistryState, now: Instant) {
		while let Some(entry) = state.order.front().copied() {
			let over_cap = state.pending.len().saturating_add(state.terminal.len())
				> TABLE_QUERY_CANCEL_TOMBSTONE_CAP;
			if entry.expires_at > now && !over_cap {
				break;
			}
			let entry = state.order.pop_front().expect("front was present");
			match entry.kind {
				TableQueryTombstoneKind::Pending => {
					if state.pending.get(&entry.key) == Some(&entry.expires_at) {
						state.pending.remove(&entry.key);
					}
				}
				TableQueryTombstoneKind::Terminal => {
					if state.terminal.get(&entry.key) == Some(&entry.expires_at) {
						state.terminal.remove(&entry.key);
					}
				}
			}
		}
	}
}

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
			mutation_work_slots: self.mutation_work_slots.clone(),
			mutation_outcomes: self.mutation_outcomes.clone(),
			idempotency: self.idempotency.clone(),
			table_delete_idempotency: self.table_delete_idempotency.clone(),
			content_hash_admission_locks: self.content_hash_admission_locks.clone(),
			pending_finalizations: self.pending_finalizations.clone(),
			counter_replica: self.counter_replica.clone(),
			counter_leaders: self.counter_leaders.clone(),
			counter_leader_init: self.counter_leader_init.clone(),
			pending_finalization_claims: self.pending_finalization_claims.clone(),
			pending_finalization_wakeup: self.pending_finalization_wakeup.clone(),
			orphan_witness_recovery_stamp: self.orphan_witness_recovery_stamp.clone(),
			orphan_witness_recovery_lock: self.orphan_witness_recovery_lock.clone(),
			local_persistence_distrust_reason: self.local_persistence_distrust_reason.clone(),
			local_persistence_distrust_path: self.local_persistence_distrust_path.clone(),
			#[cfg(test)]
			fail_next_local_durable_append: self.fail_next_local_durable_append.clone(),
			#[cfg(test)]
			delay_next_local_durable_append: self.delay_next_local_durable_append.clone(),
			#[cfg(test)]
			orphan_witness_recovery_leadership_override: self
				.orphan_witness_recovery_leadership_override
				.clone(),
			hlc: self.hlc.clone(),
			tracker: self.tracker.clone(),
			leases: self.leases.clone(),
			lease_gate_enabled: self.lease_gate_enabled,
			lease_authority: self.lease_authority.clone(),
			migration_registry: self.migration_registry.clone(),
			table_query_cancellation: self.table_query_cancellation.clone(),
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

	fn emit_row_group_fetch_sampled_audit(
		&self,
		actor: &ActorIdentity,
		meta: &QueryRequestMeta,
		counters: &RowGroupFetchAuditCounters,
	) -> std::io::Result<()> {
		if !counters.used_row_group() {
			return Ok(());
		}
		let event = AuditEvent::new(
			AuditAuthority::NodeLocal,
			AuditOperation::RowGroupFetchSampled,
			self.local_node,
			"sampled row-group fetch",
		)
		.with_actor(actor)
		.with_tenant(meta.tenant_id)
		.with_parameters(vec![
			("tier", json!("l2")),
			("row_group_fetches", json!(counters.row_group_fetches)),
			(
				"columnar_buffer_fetches",
				json!(counters.columnar_buffer_fetches),
			),
			("whole_record_fetches", json!(counters.whole_record_fetches)),
			("fallback_fetches", json!(counters.fallback_fetches)),
			("row_groups_fetched", json!(counters.row_groups_fetched)),
			("row_groups_total", json!(counters.row_groups_total)),
			(
				"predicate_fetched_groups",
				json!(counters.predicate_fetched_groups),
			),
			("dominant_strategy", json!(counters.dominant_strategy())),
		]);
		self.audit_sink.emit(event).map_err(|err| {
			Error::other(format!("row-group fetch sampled audit emit failed: {err}"))
		})
	}

	fn emit_row_group_invariant_audit_for_error(
		&self,
		actor: &ActorIdentity,
		tenant_id: TenantId,
		error: &Error,
		context: &'static str,
	) -> std::io::Result<()> {
		let Some(violation_kind) = row_group_invariant_violation_kind(error) else {
			return Ok(());
		};
		let event = AuditEvent::new(
			AuditAuthority::NodeLocal,
			AuditOperation::RowGroupInvariantViolation,
			self.local_node,
			"row-group invariant violation",
		)
		.with_actor(actor)
		.with_tenant(tenant_id)
		.with_outcome(AuditOutcome::Denied)
		.with_parameters(vec![
			("tier", json!("l2")),
			("context", json!(context)),
			("violation_kind", json!(violation_kind)),
			("error", json!(error.to_string())),
		]);
		self.audit_sink.emit(event).map_err(|err| {
			Error::other(format!(
				"row-group invariant violation audit emit failed: {err}"
			))
		})
	}

	fn emit_encoding_materialize_sampled_audit(
		&self,
		actor: &ActorIdentity,
		meta: &QueryRequestMeta,
		samples: &[EncodingMaterializeAuditSample],
	) -> std::io::Result<()> {
		for sample in samples {
			let event = AuditEvent::new(
				AuditAuthority::NodeLocal,
				AuditOperation::EncodingMaterializeSampled,
				self.local_node,
				"sampled Encoding materialization",
			)
			.with_actor(actor)
			.with_tenant(meta.tenant_id)
			.with_parameters(vec![
				("tier", json!("l2")),
				("cascade", json!(sample.cascade)),
				("buffer_bytes_in", json!(sample.buffer_bytes_in)),
				("buffer_bytes_out", json!(sample.buffer_bytes_out)),
			]);
			self.audit_sink.emit(event).map_err(|err| {
				Error::other(format!(
					"Encoding materialize sampled audit emit failed: {err}"
				))
			})?;
		}
		Ok(())
	}

	fn emit_encoding_invariant_audit_for_error(
		&self,
		actor: &ActorIdentity,
		tenant_id: TenantId,
		error: &Error,
		context: &'static str,
	) -> std::io::Result<()> {
		let Some(violation_kind) = encoding_invariant_violation_kind(error) else {
			return Ok(());
		};
		let event = AuditEvent::new(
			AuditAuthority::NodeLocal,
			AuditOperation::EncodingInvariantViolation,
			self.local_node,
			"Encoding invariant violation",
		)
		.with_actor(actor)
		.with_tenant(tenant_id)
		.with_outcome(AuditOutcome::Denied)
		.with_parameters(vec![
			("tier", json!("l2")),
			("context", json!(context)),
			("violation_kind", json!(violation_kind)),
			("error", json!(error.to_string())),
		]);
		self.audit_sink.emit(event).map_err(|err| {
			Error::other(format!(
				"Encoding invariant violation audit emit failed: {err}"
			))
		})
	}

	#[allow(dead_code)]
	fn emit_encoding_compute_sampled_audit(
		&self,
		actor: &ActorIdentity,
		meta: &QueryRequestMeta,
		kernel: &'static str,
		rows_scanned: u64,
		rows_selected: u64,
	) -> std::io::Result<()> {
		let event = AuditEvent::new(
			AuditAuthority::NodeLocal,
			AuditOperation::EncodingComputeSampled,
			self.local_node,
			"sampled Encoding compute",
		)
		.with_actor(actor)
		.with_tenant(meta.tenant_id)
		.with_parameters(vec![
			("tier", json!("l2")),
			("kernel", json!(kernel)),
			("rows_scanned", json!(rows_scanned)),
			("rows_selected", json!(rows_selected)),
		]);
		self.audit_sink.emit(event).map_err(|err| {
			Error::other(format!("Encoding compute sampled audit emit failed: {err}"))
		})
	}

	#[allow(dead_code)]
	fn emit_encoding_compute_invariant_audit(
		&self,
		actor: &ActorIdentity,
		tenant_id: TenantId,
		violation_kind: &'static str,
		context: &'static str,
	) -> std::io::Result<()> {
		let event = AuditEvent::new(
			AuditAuthority::NodeLocal,
			AuditOperation::EncodingComputeInvariantViolation,
			self.local_node,
			"Encoding compute invariant violation",
		)
		.with_actor(actor)
		.with_tenant(tenant_id)
		.with_outcome(AuditOutcome::Denied)
		.with_parameters(vec![
			("tier", json!("l2")),
			("context", json!(context)),
			("violation_kind", json!(violation_kind)),
		]);
		self.audit_sink.emit(event).map_err(|err| {
			Error::other(format!(
				"Encoding compute invariant violation audit emit failed: {err}"
			))
		})
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
		let mutation_outcomes =
			MutationOutcomeStore::open(mutation_outcomes_path(slab.data_dir()))?;
		let admission_snapshot_rx = state_machine.subscribe_admission_snapshot();
		let counter_replica = Arc::new(crate::storage::counter::CounterReplicaService::new(
			slab.clone(),
		));
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
			mutation_work_slots: Arc::new(Semaphore::new(
				std::thread::available_parallelism()
					.map(|parallelism| parallelism.get())
					.unwrap_or(4),
			)),
			mutation_outcomes,
			idempotency: Arc::new(DashMap::new()),
			table_delete_idempotency: Arc::new(DashMap::new()),
			content_hash_admission_locks: Arc::new(DashMap::new()),
			pending_finalizations,
			counter_replica,
			counter_leaders: Arc::new(DashMap::new()),
			counter_leader_init: Arc::new(DashMap::new()),
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
			#[cfg(test)]
			delay_next_local_durable_append: Arc::new(StdMutex::new(None)),
			#[cfg(test)]
			orphan_witness_recovery_leadership_override: Arc::new(StdMutex::new(None)),
			hlc,
			tracker: None,
			leases: Arc::new(RwLock::new(Vec::new())),
			lease_gate_enabled: false,
			lease_authority: Arc::new(LeaseAuthority::dev_single_node(local_node)),
			migration_registry: Arc::new(MigrationRegistry::new()),
			table_query_cancellation: Arc::new(ActiveTableQueryRegistry::default()),
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
	pub(crate) fn delay_next_local_durable_append_for_test(&self, delay: Duration) {
		*self
			.delay_next_local_durable_append
			.lock()
			.expect("local durable append delay failpoint poisoned") = Some(delay);
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
		}
		.with_mutation_outcome(MutationCommitKnowledge::COMMITTED, MutationStopReason::NONE);
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
					.complete_pending_finalization(record, resolution, None)
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
		match self
			.ensure_orphan_witnesses_recovered_for_leadership()
			.await
		{
			Ok(true) => {}
			// Leadership is unavailable, so reconciliation is deferred; zero here
			// does not mean that the pending-finalization backlog was drained.
			Ok(false) => return Ok(0),
			Err(err) => {
				tracing::warn!(
					node_id = self.local_node.id,
					error = %err,
					"orphan-witness recovery sweep failed before pending-finalization reconciliation"
				);
			}
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
			discovery.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
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
		let records = bound_orphan_witness_pull(
			peer,
			self.pull_pending_witnesses_from_peer(peer, request_epoch),
		)
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
		#[cfg(test)]
		if *self
			.orphan_witness_recovery_leadership_override
			.lock()
			.expect("orphan witness leadership override poisoned")
			== Some(false)
		{
			metrics().observe_orphan_witness_recovery_leadership_unconfirmed();
			return None;
		}
		let raft_metrics = self.raft.metrics();
		let borrowed = raft_metrics.borrow();
		if borrowed.current_leader != Some(self.local_node.id) {
			metrics().observe_orphan_witness_recovery_leadership_unconfirmed();
			return None;
		}
		Some(OrphanWitnessRecoveryStamp {
			term: borrowed.current_term,
			epoch: self.factory.membership_snapshot().epoch,
		})
	}

	#[cfg(test)]
	pub(crate) fn set_orphan_witness_recovery_leadership_for_test(&self, confirmed: bool) {
		*self
			.orphan_witness_recovery_leadership_override
			.lock()
			.expect("orphan witness leadership override poisoned") = Some(confirmed);
	}

	async fn ensure_orphan_witnesses_recovered_for_leadership(&self) -> std::io::Result<bool> {
		let Some(target) = self.current_orphan_witness_recovery_stamp() else {
			return Ok(false);
		};

		if *recover_rwlock_read(
			&self.orphan_witness_recovery_stamp,
			"orphan_witness_recovery_stamp",
		) == Some(target)
		{
			return Ok(true);
		}

		let _guard = self.orphan_witness_recovery_lock.lock().await;
		let Some(target) = self.current_orphan_witness_recovery_stamp() else {
			return Ok(false);
		};
		if *recover_rwlock_read(
			&self.orphan_witness_recovery_stamp,
			"orphan_witness_recovery_stamp",
		) == Some(target)
		{
			return Ok(true);
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
		Ok(true)
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
			run_lease_refresh_schedule(&mut epoch_rx, tick, || {
				let server = self.clone();
				let ring = ring.clone();
				let clock = clock.clone();
				async move {
					let _ = server
						.refresh_leases_once(&ring, clock.as_ref(), ttl_ms)
						.await;
				}
			})
			.await;
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
			.reclaim_tombstoned_ranges(
				&snapshot.tombstones,
				&snapshot.table_schemas,
				&snapshot.secondary_indexes,
				max_slabs,
			)
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

	/// ADR 075: ship a `COUNTER_REPLICATE_REQ` (tentative or commit phase) to one
	/// peer and return its `CounterReplicaAck`. Constructs ONLY counter frames and
	/// validates ONLY the counter response meta — it never rides ordinary
	/// `REPLICATE_REQ`. The caller treats the peer as accepted iff
	/// `ack.status == CounterReplicaStatus::OK`.
	async fn replicate_counter_to_remote(
		factory: Arc<TransportProviderFactory>,
		pool: Arc<BufPool>,
		target: NodeId,
		meta: CounterReplicateRequestMeta,
	) -> std::io::Result<CounterReplicaAck> {
		Self::counter_round_trip(
			factory,
			pool,
			target,
			MsgType::COUNTER_REPLICATE_REQ.0,
			bytemuck::bytes_of(&meta),
			MsgType::COUNTER_REPLICATE_RESP.0,
		)
		.await
	}

	/// ADR 075: ship a `COUNTER_ROLLBACK_REQ` to one peer and return its ack.
	/// Counter frames only.
	async fn rollback_counter_to_remote(
		factory: Arc<TransportProviderFactory>,
		pool: Arc<BufPool>,
		target: NodeId,
		meta: CounterRollbackRequestMeta,
	) -> std::io::Result<CounterReplicaAck> {
		Self::counter_round_trip(
			factory,
			pool,
			target,
			MsgType::COUNTER_ROLLBACK_REQ.0,
			bytemuck::bytes_of(&meta),
			MsgType::COUNTER_ROLLBACK_RESP.0,
		)
		.await
	}

	/// Shared one-shot counter request/response over a bidi stream: send a fixed
	/// counter request meta (no arrow payload — the value rides in the meta),
	/// await the matching counter response, and decode the fixed `CounterReplicaAck`.
	async fn counter_round_trip(
		factory: Arc<TransportProviderFactory>,
		pool: Arc<BufPool>,
		target: NodeId,
		req_msg_type: u8,
		meta_bytes: &[u8],
		resp_msg_type: u8,
	) -> std::io::Result<CounterReplicaAck> {
		let conn = factory.get_connection(target).await?;
		let mut stream = conn.open_bi().await?;
		let header = FrameHeader {
			magic: HRFT_MAGIC,
			version: 1,
			msg_type: req_msg_type,
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

		let resp = read_frame(&mut *stream, &pool).await?;
		if resp.header.msg_type != resp_msg_type {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"expected counter resp 0x{resp_msg_type:02x}, got msg_type=0x{:02x}",
					resp.header.msg_type
				),
			));
		}
		if resp.meta.len() < std::mem::size_of::<CounterReplicaAck>() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"counter response meta too small",
			));
		}
		Ok(*bytemuck::from_bytes::<CounterReplicaAck>(
			&resp.meta[..std::mem::size_of::<CounterReplicaAck>()],
		))
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
	) -> std::io::Result<(TableQueryRequestMeta, Vec<u8>, DecodedRuntimeFilter)> {
		Self::decode_table_query_request_frame_parts(frame)
	}

	fn decode_table_query_request_frame_parts(
		frame: &HrftFrame,
	) -> std::io::Result<(TableQueryRequestMeta, Vec<u8>, DecodedRuntimeFilter)> {
		if frame.meta.len() < std::mem::size_of::<TableQueryRequestMeta>() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"TABLE_QUERY_REQ meta too small",
			));
		}
		let meta: TableQueryRequestMeta =
			*bytemuck::from_bytes(&frame.meta[..std::mem::size_of::<TableQueryRequestMeta>()]);
		if meta.request_id.is_zero() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"TABLE_QUERY_REQ request_id must be non-zero",
			));
		}
		let bytecode_len = meta.bytecode_len as usize;
		let runtime_filter_len = meta.runtime_filter_len as usize;
		let declared_trailing_len =
			bytecode_len
				.checked_add(runtime_filter_len)
				.ok_or_else(|| {
					Error::new(
						ErrorKind::InvalidData,
						"TABLE_QUERY_REQ declared trailing lengths overflow usize",
					)
				})?;
		let trailing = if declared_trailing_len == 0 {
			None
		} else {
			Some(frame.trailing.as_ref().ok_or_else(|| {
				Error::new(
					ErrorKind::InvalidData,
					"TABLE_QUERY_REQ declared trailing bytes but trailing is absent",
				)
			})?)
		};
		if let Some(trailing) = trailing
			&& trailing.len() < declared_trailing_len
		{
			return Err(Error::new(
				ErrorKind::InvalidData,
				"TABLE_QUERY_REQ trailing is shorter than declared bytecode/runtime_filter lengths",
			));
		}
		let bytecode = if bytecode_len == 0 {
			Vec::new()
		} else {
			let trailing = trailing.expect("declared trailing bytes are present");
			trailing[..bytecode_len].to_vec()
		};
		let runtime_filter = if runtime_filter_len == 0 {
			DecodedRuntimeFilter::Absent
		} else {
			let trailing = trailing.expect("declared trailing bytes are present");
			let start = bytecode_len;
			let end = start.checked_add(runtime_filter_len).ok_or_else(|| {
				Error::new(
					ErrorKind::InvalidData,
					"TABLE_QUERY_REQ runtime filter range overflows usize",
				)
			})?;
			match postcard::from_bytes::<RuntimeFilterDescriptor>(&trailing[start..end]) {
				Ok(descriptor) => DecodedRuntimeFilter::Decoded(descriptor),
				Err(_) => DecodedRuntimeFilter::DroppedBeforePlanning(
					RuntimeFilterDropReason::DecodeFailed,
				),
			}
		};
		Ok((meta, bytecode, runtime_filter))
	}

	fn decode_table_query_cancel_request_frame(
		&self,
		frame: &HrftFrame,
	) -> std::io::Result<TableQueryCancelRequestMeta> {
		if frame.meta.len() != std::mem::size_of::<TableQueryCancelRequestMeta>() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"TABLE_QUERY_CANCEL_REQ meta size mismatch",
			));
		}
		if frame.arrow_payload.is_some() || frame.trailing.is_some() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"TABLE_QUERY_CANCEL_REQ must not carry payload bytes",
			));
		}
		let meta: TableQueryCancelRequestMeta = *bytemuck::from_bytes(
			&frame.meta[..std::mem::size_of::<TableQueryCancelRequestMeta>()],
		);
		if meta.request_id.is_zero() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"TABLE_QUERY_CANCEL_REQ request_id must be non-zero",
			));
		}
		Ok(meta)
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

	fn build_table_query_cancel_response_frame(
		&self,
		stream_id: u32,
		status: TableQueryCancelStatus,
	) -> HrftFrame {
		let response = TableQueryCancelResponseMeta {
			status,
			_pad: [0; 7],
		};
		let mut body = self.pool.acquire();
		body.as_mut_vec()
			.extend_from_slice(bytemuck::bytes_of(&response));
		HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::TABLE_QUERY_CANCEL_RESP.0,
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

	fn table_query_integrity_error_frame(
		&self,
		stream_id: u32,
		meta: &TableQueryRequestMeta,
		failure: crate::storage::manager::PublishedReferenceIntegrityFailure,
	) -> HrftFrame {
		let range_meta = table_query_as_range_query(meta);
		let response = query_error_meta(
			&range_meta,
			ReadStatus::TABLE_REFERENCE_INTEGRITY_FAILED,
			self.local_node,
		);
		let mut body = self.pool.acquire();
		body.as_mut_vec()
			.extend_from_slice(bytemuck::bytes_of(&response));
		let mut trailing = self.pool.acquire();
		trailing
			.as_mut_vec()
			.extend_from_slice(bytemuck::bytes_of(&failure.wire_meta()));
		HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::TABLE_QUERY_RESP.0,
				flags: FRAME_FLAG_STREAM_END,
				stream_id,
				meta_len: body.len() as u32,
				arrow_len: 0,
				trailing_len: trailing.len() as u32,
			},
			meta: body,
			arrow_payload: None,
			trailing: Some(trailing),
		}
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

	#[cfg(test)]
	async fn prepare_table_query_execution_plan_with_migration(
		&self,
		meta: &TableQueryRequestMeta,
		bytecode: &[u8],
		runtime_filter: DecodedRuntimeFilter,
	) -> std::io::Result<QueryExecutionPlan> {
		let cancellation_token = CancellationToken::new();
		self.prepare_table_query_execution_plan_with_migration_with_cancellation(
			meta,
			bytecode,
			runtime_filter,
			&cancellation_token,
		)
		.await
	}

	async fn prepare_table_query_execution_plan_with_migration_with_cancellation(
		&self,
		meta: &TableQueryRequestMeta,
		bytecode: &[u8],
		runtime_filter: DecodedRuntimeFilter,
		cancellation_token: &CancellationToken,
	) -> std::io::Result<QueryExecutionPlan> {
		self.prepare_table_query_execution_plan_with_migration_with_cancellation_and_stream(
			meta,
			bytecode,
			runtime_filter,
			cancellation_token,
			None,
		)
		.await
	}

	async fn prepare_table_query_execution_plan_with_migration_with_cancellation_and_stream(
		&self,
		meta: &TableQueryRequestMeta,
		bytecode: &[u8],
		runtime_filter: DecodedRuntimeFilter,
		cancellation_token: &CancellationToken,
		mut stream_writer: Option<&mut TableQueryResponseStreamWriter<'_>>,
	) -> std::io::Result<QueryExecutionPlan> {
		let range_meta = table_query_as_range_query(meta);
		let row_limit = match table_query_row_limit(meta) {
			Ok(row_limit) => row_limit,
			Err(status) => return Ok(query_plan_error(&range_meta, status, self.local_node)),
		};
		if cancellation_token.is_cancelled() {
			return Ok(query_plan_error(
				&range_meta,
				ReadStatus::QUERY_CANCELLED,
				self.local_node,
			));
		}
		let row_identity_projection =
			match RowIdentityProjection::from_u8(meta.row_identity_projection) {
				Some(projection) => projection,
				None => {
					return Ok(query_plan_error(
						&range_meta,
						ReadStatus::QUERY_INVALID_PROGRAM,
						self.local_node,
					));
				}
			};
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
		if cancellation_token.is_cancelled() {
			return Ok(query_plan_error(
				&range_meta,
				ReadStatus::QUERY_CANCELLED,
				self.local_node,
			));
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
		if cancellation_token.is_cancelled() {
			return Ok(query_plan_error(
				&range_meta,
				ReadStatus::QUERY_CANCELLED,
				self.local_node,
			));
		}
		if sm_peek
			.table_drop_fences
			.is_fenced(meta.tenant_id, meta.table_object_id)
		{
			return Ok(query_plan_error(
				&range_meta,
				ReadStatus::QUERY_INVALID_PROGRAM,
				self.local_node,
			));
		}
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
		if meta.physical_descriptor_fingerprint
			!= *descriptor.physical_descriptor_fingerprint.as_bytes()
		{
			return Ok(query_plan_error(
				&range_meta,
				ReadStatus::TABLE_DESCRIPTOR_EVOLVED,
				self.local_node,
			));
		}
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
			self.local_node,
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
				if cancellation_token.is_cancelled() {
					return Ok(query_plan_error(
						&range_meta,
						ReadStatus::QUERY_CANCELLED,
						self.local_node,
					));
				}
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

		let schema = match descriptor.arrow_schema() {
			Ok(schema) => schema,
			Err(_) => {
				return Ok(query_plan_error(
					&range_meta,
					ReadStatus::QUERY_INVALID_PROGRAM,
					self.local_node,
				));
			}
		};
		let user_column_count = schema.fields().len();
		let mut requirement = match analyze_pbc_column_requirement(&program, user_column_count) {
			Ok(requirement) => requirement,
			Err(_) => {
				return Ok(query_plan_error(
					&range_meta,
					ReadStatus::QUERY_INVALID_PROGRAM,
					self.local_node,
				));
			}
		};
		let runtime_filter = prepare_runtime_filter_application(
			&runtime_filter,
			&descriptor,
			&schema,
			&mut requirement,
			user_column_count,
		);

		let projected_execution = match row_identity_projection {
			RowIdentityProjection::None => QueryProjectedExecution::Pbc {
				requirement,
				user_column_count,
				runtime_filter,
			},
			RowIdentityProjection::Append => QueryProjectedExecution::Disabled,
		};
		if row_identity_projection == RowIdentityProjection::None {
			if let Some(access_plan) = secondary_index_access_plan.as_ref() {
				if cancellation_token.is_cancelled() {
					return Ok(query_plan_error(
						&range_meta,
						ReadStatus::QUERY_CANCELLED,
						self.local_node,
					));
				}
				match plan_table_query_execution_by_secondary_index(
					&self.slab,
					self.local_node,
					&range_meta,
					&program,
					&ranges,
					table_query_execution_budget(meta),
					row_limit,
					&descriptor,
					&projected_execution,
					effective_retention.floor,
					access_plan,
					Some(cancellation_token),
					stream_writer.as_deref_mut(),
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
						if stream_writer
							.as_ref()
							.is_some_and(|writer| writer.has_started())
						{
							return Err(err);
						}
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
		}

		let mut plan = plan_query_execution_for_ranges_with_program_maybe_cancelled(
			&self.slab,
			self.local_node,
			&range_meta,
			&program,
			&ranges,
			table_query_execution_budget(meta),
			row_limit,
			Some(&descriptor),
			projected_execution,
			row_identity_projection,
			effective_retention.floor,
			Some(cancellation_token),
			stream_writer.as_deref_mut(),
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
			None,
			Some(&prepared.descriptor),
			projected_execution,
			RowIdentityProjection::None,
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
		let mut responses = plan.aggregate_responses.clone();
		if plan.response.status == ReadStatus::KEY_NOT_FOUND
			|| (!plan.batches.is_empty() || responses.is_empty())
		{
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
			let mut dense_response = result.response;
			dense_response.aggregate_expression_digest = request.expression_digest();
			if plan.response.status == ReadStatus::OK && !plan.batches.is_empty() {
				dense_response.max_hlc = plan.response.value_hlc;
			}
			responses.push(dense_response);
		}
		let mut response = match merge_aggregate_responses(request, &responses) {
			Ok(response) => response,
			Err(code) => return Ok(self.aggregate_rejected_response(request, code)),
		};
		if plan.response.status == ReadStatus::OK && response.max_hlc < plan.response.value_hlc {
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
		if sm_peek
			.table_drop_fences
			.is_fenced(request.tenant_id, request.table_object_id)
		{
			return Ok(Err(AggRejectCode::SchemaUnknown));
		}
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

	/// Encode a fixed `CounterReplicaAck` reply frame (shared by the counter
	/// replicate / rollback handlers).
	fn counter_ack_frame(
		&self,
		stream_id: u32,
		resp_msg_type: u8,
		status: CounterReplicaStatus,
		op_id: u64,
	) -> HrftFrame {
		let mut body = self.pool.acquire();
		body.as_mut_vec()
			.extend_from_slice(bytemuck::bytes_of(&CounterReplicaAck {
				status,
				_pad: [0; 7],
				op_id,
			}));
		HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: resp_msg_type,
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

	/// ADR 075/076: the per-tenant counter leader, built lazily — by RECOVERY,
	/// not fresh construction: it loads the tenant's durable counter state (the
	/// applied D4 dedup log + any pending finalization witnesses) and reconciles
	/// partial commits BEFORE serving, so a duplicate `ClientWriteId` retry after
	/// a node restart replays `DUPLICATE` instead of re-adding, and a
	/// witnessed-but-unmarked add becomes readable again. Fresh counter adds pass
	/// a per-key participant plan; recovery resolves the peer identities recorded
	/// in the witness separately from fresh routing. The per-tenant init gate
	/// makes concurrent first requests recover exactly once (recovery does file
	/// I/O — never under a DashMap entry lock across await).
	async fn counter_leader_for(
		&self,
		tenant_id: TenantId,
	) -> std::io::Result<
		Arc<
			crate::storage::counter::CounterLeader<crate::storage::counter::CounterParticipantKind>,
		>,
	> {
		use crate::storage::counter::{
			CounterLeader, CounterParticipantKind, DurableCounterWitnessStore,
			LocalCounterParticipant,
		};
		if let Some(existing) = self.counter_leaders.get(&tenant_id) {
			return Ok(existing.clone());
		}
		let init_gate = self
			.counter_leader_init
			.entry(tenant_id)
			.or_default()
			.clone();
		let _init_guard = init_gate.lock().await;
		if let Some(existing) = self.counter_leaders.get(&tenant_id) {
			return Ok(existing.clone());
		}
		let participant = CounterParticipantKind::Local(LocalCounterParticipant::new(
			self.counter_replica.clone(),
			tenant_id,
		));
		let witness_dir = self
			.slab
			.data_dir()
			.join("counter-witness")
			.join(format!("tenant-{}", tenant_id.get()));
		std::fs::create_dir_all(&witness_dir)?;
		let witness_store = DurableCounterWitnessStore::open(witness_dir)?;
		let leader = Arc::new(CounterLeader::recover(
			self.local_node,
			participant,
			true,
			witness_store,
		)?);
		// Finish any partial commit fan-out (idempotent) before first service.
		let resolver =
			self.counter_participant_factory(tenant_id, self.admission_snapshot().current_epoch);
		leader.reconcile(&resolver).await;
		self.counter_leaders.insert(tenant_id, leader.clone());
		Ok(leader)
	}

	fn counter_participant_factory(
		&self,
		tenant_id: TenantId,
		epoch: u64,
	) -> CounterParticipantFactory {
		CounterParticipantFactory {
			local_node: self.local_node,
			tenant_id,
			epoch,
			local: self.counter_replica.clone(),
			transport: Arc::new(CounterReplicaNetworkTransport {
				factory: self.factory.clone(),
				pool: self.pool.clone(),
			}),
		}
	}

	/// Zero HLC placeholder for counter responses. Authoritative `commit_hlc`
	/// plumbing from the leader is a cut-4b follow-on (the load coordinates the
	/// client needs today are `previous_value` / `current_value`).
	fn zero_counter_hlc() -> HybridTimestamp {
		HybridTimestamp {
			physical_ms: 0,
			logical: 0,
			node_id: 0,
		}
	}

	fn counter_add_response(
		&self,
		meta: &CounterAddRequestMeta,
		status: WriteStatus,
		previous_value: u64,
		current_value: u64,
		commit_hlc: HybridTimestamp,
	) -> CounterAddResponseMeta {
		self.counter_add_response_with_hint(
			meta,
			status,
			previous_value,
			current_value,
			commit_hlc,
			self.local_node,
		)
	}

	fn counter_add_response_with_hint(
		&self,
		meta: &CounterAddRequestMeta,
		status: WriteStatus,
		previous_value: u64,
		current_value: u64,
		commit_hlc: HybridTimestamp,
		leader_hint: NodeId,
	) -> CounterAddResponseMeta {
		CounterAddResponseMeta {
			status,
			_pad1: [0; 7],
			log_index: 0,
			term: 0,
			epoch: meta.expected_epoch,
			commit_hlc,
			leader_hint,
			echoed_write_id: meta.write_id,
			previous_value,
			current_value,
			has_finalization: 0,
			_pad2: [0; 7],
		}
	}

	fn build_counter_add_response_frame(
		&self,
		stream_id: u32,
		resp: CounterAddResponseMeta,
	) -> HrftFrame {
		let mut meta_buf = self.pool.acquire();
		meta_buf
			.as_mut_vec()
			.extend_from_slice(bytemuck::bytes_of(&resp));
		HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::COUNTER_ADD_RESP.0,
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

	fn counter_read_response(
		&self,
		status: ReadStatus,
		value: u64,
		epoch: u64,
	) -> CounterReadResponseMeta {
		CounterReadResponseMeta {
			status,
			_pad1: [0; 7],
			value,
			epoch,
			leader_hint: self.local_node,
		}
	}

	fn build_counter_read_response_frame(
		&self,
		stream_id: u32,
		resp: CounterReadResponseMeta,
	) -> HrftFrame {
		let mut meta_buf = self.pool.acquire();
		meta_buf
			.as_mut_vec()
			.extend_from_slice(bytemuck::bytes_of(&resp));
		HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::COUNTER_READ_RESP.0,
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
}

async fn run_lease_refresh_schedule<F, Fut>(
	epoch_rx: &mut watch::Receiver<Arc<MembershipEpoch>>,
	tick: Duration,
	mut refresh: F,
) where
	F: FnMut() -> Fut,
	Fut: Future<Output = ()>,
{
	// Seed once immediately. `interval()` would also produce an
	// immediately-ready first tick, duplicating this authoritative
	// Raft renewal at startup (ADR 080).
	refresh().await;
	let deadline = tokio::time::sleep(tick);
	tokio::pin!(deadline);
	loop {
		tokio::select! {
			biased;
			changed = epoch_rx.changed() => {
				if changed.is_err() {
					break;
				}
			}
			_ = &mut deadline => {}
		}
		refresh().await;
		// A refresh caused by an epoch change supersedes the prior
		// timer deadline; a delayed runtime likewise schedules from
		// completion instead of bursting missed renewals.
		deadline.as_mut().reset(tokio::time::Instant::now() + tick);
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
						let resp = self.finalize_table_write_attempt(meta, resp).await?;
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
						let resp = self.finalize_table_delete_attempt(meta, resp).await?;
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
			MsgType::MUTATION_OUTCOME_REQ => {
				if frame.meta.len() != std::mem::size_of::<MutationOutcomeRequestMeta>() {
					return Err(Error::new(
						ErrorKind::InvalidData,
						"MUTATION_OUTCOME_REQ meta width mismatch",
					));
				}
				let meta: MutationOutcomeRequestMeta = *bytemuck::from_bytes(&frame.meta);
				self.authorize_actor_for_data(
					&context.actor,
					Some(meta.tenant_id),
					DataOperation::Read,
				)?;
				let _admission = self
					.try_admit_data_request(
						&context.actor,
						Some(meta.tenant_id),
						DataOperation::Read,
					)?
					.map_err(|_| {
						Error::new(ErrorKind::WouldBlock, "mutation resolution backpressured")
					})?;
				let response = self.resolve_mutation_outcome(meta).await?;
				Ok(Some(self.build_mutation_outcome_response_frame(
					frame.header.stream_id,
					response,
				)))
			}
			MsgType::MUTATION_OUTCOME_PIN_REQ => {
				self.handle_mutation_outcome_pin_request(&context.actor, &frame)
					.await
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
			MsgType::COUNTER_REPLICATE_REQ => {
				if frame.meta.len() < std::mem::size_of::<CounterReplicateRequestMeta>() {
					return Err(Error::new(
						ErrorKind::InvalidData,
						"COUNTER_REPLICATE_REQ meta too small",
					));
				}
				let meta: CounterReplicateRequestMeta = *bytemuck::from_bytes(
					&frame.meta[..std::mem::size_of::<CounterReplicateRequestMeta>()],
				);
				if self
					.authorize_actor_for_data(
						&context.actor,
						Some(meta.tenant_id),
						DataOperation::Replicate,
					)
					.is_err()
				{
					return Ok(Some(self.counter_ack_frame(
						frame.header.stream_id,
						MsgType::COUNTER_REPLICATE_RESP.0,
						CounterReplicaStatus::NOT_AUTHORIZED,
						meta.op_id,
					)));
				}
				self.handle_counter_replicate(frame).await.map(Some)
			}
			MsgType::COUNTER_ROLLBACK_REQ => {
				if frame.meta.len() < std::mem::size_of::<CounterRollbackRequestMeta>() {
					return Err(Error::new(
						ErrorKind::InvalidData,
						"COUNTER_ROLLBACK_REQ meta too small",
					));
				}
				let meta: CounterRollbackRequestMeta = *bytemuck::from_bytes(
					&frame.meta[..std::mem::size_of::<CounterRollbackRequestMeta>()],
				);
				if self
					.authorize_actor_for_data(
						&context.actor,
						Some(meta.tenant_id),
						DataOperation::Replicate,
					)
					.is_err()
				{
					return Ok(Some(self.counter_ack_frame(
						frame.header.stream_id,
						MsgType::COUNTER_ROLLBACK_RESP.0,
						CounterReplicaStatus::NOT_AUTHORIZED,
						meta.op_id,
					)));
				}
				self.handle_counter_rollback(frame).await.map(Some)
			}
			MsgType::COUNTER_ADD_REQ => {
				if frame.meta.len() < std::mem::size_of::<CounterAddRequestMeta>() {
					return Err(Error::new(
						ErrorKind::InvalidData,
						"COUNTER_ADD_REQ meta too small",
					));
				}
				let meta: CounterAddRequestMeta = *bytemuck::from_bytes(
					&frame.meta[..std::mem::size_of::<CounterAddRequestMeta>()],
				);
				if self
					.authorize_actor_for_data(
						&context.actor,
						Some(meta.tenant_id),
						DataOperation::Write,
					)
					.is_err()
				{
					return Ok(Some(self.build_counter_add_response_frame(
						frame.header.stream_id,
						self.counter_add_response(
							&meta,
							WriteStatus::AUTHORIZATION_FAILED,
							0,
							0,
							Self::zero_counter_hlc(),
						),
					)));
				}
				let _admission = match self.try_admit_data_request(
					&context.actor,
					Some(meta.tenant_id),
					DataOperation::Write,
				)? {
					Ok(admission) => admission,
					Err(_scope) => {
						return Ok(Some(self.build_counter_add_response_frame(
							frame.header.stream_id,
							self.counter_add_response(
								&meta,
								WriteStatus::BACKPRESSURED,
								0,
								0,
								Self::zero_counter_hlc(),
							),
						)));
					}
				};
				self.handle_counter_add(frame).await.map(Some)
			}
			MsgType::COUNTER_READ_REQ => {
				if frame.meta.len() < std::mem::size_of::<CounterReadRequestMeta>() {
					return Err(Error::new(
						ErrorKind::InvalidData,
						"COUNTER_READ_REQ meta too small",
					));
				}
				let meta: CounterReadRequestMeta = *bytemuck::from_bytes(
					&frame.meta[..std::mem::size_of::<CounterReadRequestMeta>()],
				);
				if self
					.authorize_actor_for_data(
						&context.actor,
						Some(meta.tenant_id),
						DataOperation::Read,
					)
					.is_err()
				{
					return Ok(Some(self.build_counter_read_response_frame(
						frame.header.stream_id,
						self.counter_read_response(ReadStatus::AUTHORIZATION_FAILED, 0, 0),
					)));
				}
				let _admission = match self.try_admit_data_request(
					&context.actor,
					Some(meta.tenant_id),
					DataOperation::Read,
				)? {
					Ok(admission) => admission,
					Err(_scope) => {
						return Ok(Some(self.build_counter_read_response_frame(
							frame.header.stream_id,
							self.counter_read_response(ReadStatus::BACKPRESSURED, 0, 0),
						)));
					}
				};
				self.handle_counter_read(frame).await.map(Some)
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
			MsgType::TABLE_QUERY_CANCEL_REQ => self
				.handle_table_query_cancel(&context.actor, frame)
				.await
				.map(Some),
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
					let response = self.table_query_error_frame(
						frame.header.stream_id,
						&meta,
						ReadStatus::AUTHORIZATION_FAILED,
					);
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
							&audit_meta,
							ReadStatus::BACKPRESSURED,
							format!(
								"post-auth request admission saturated at {}",
								backpressure_scope_name(scope)
							),
						)?;
						let response = self.table_query_error_frame(
							frame.header.stream_id,
							&meta,
							ReadStatus::BACKPRESSURED,
						);
						write_frame(&mut *stream, &response.header, &response.meta, None, None)
							.await?;
						stream.finish().await?;
						return Ok(());
					}
				};
				self.handle_table_query_stream(&context.actor, frame, stream)
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
		descriptor: &TableSchemaDescriptor,
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
		descriptor: &TableSchemaDescriptor,
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
		descriptor: &TableSchemaDescriptor,
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
		let finalization = cdc_locality_heartbeat_finalization(locality_id, commit_hlc, epoch);
		let response = self
			.raft
			.client_write(WendelonRequest::Finalize(finalization))
			.await
			.map_err(|err| Error::other(format!("cdc heartbeat finalization failed: {err}")))?;
		metrics().cdc_locality_heartbeats_committed_total.inc();
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
		descriptor: &TableSchemaDescriptor,
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
		descriptor: &TableSchemaDescriptor,
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
		if !self
			.ensure_orphan_witnesses_recovered_for_leadership()
			.await?
		{
			return Ok(self.counted_write_result(
				error_response(&meta, WriteStatus::NOT_LEADER, self.local_node),
				None,
			));
		}

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
				#[cfg(test)]
				let delay_local_durable_append = self
					.delay_next_local_durable_append
					.lock()
					.expect("local durable append delay failpoint poisoned")
					.take();
				Some(tokio::spawn(async move {
					#[cfg(test)]
					if let Some(delay) = delay_local_durable_append {
						tokio::time::sleep(delay).await;
					}
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
						.complete_pending_finalization(&pending, resolution, None)
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
				.complete_pending_finalization(&pending, resolution, None)
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
		table: &TableSchemaDescriptor,
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
		descriptor: &TableSchemaDescriptor,
		commit_hlc: HybridTimestamp,
		is_append_only: bool,
		secondary_index_descriptors: Vec<SecondaryIndexDescriptor>,
		delete_counts: Option<[u32; 3]>,
		mut clocks: WriteClocks,
		permit: OwnedSemaphorePermit,
	) -> std::io::Result<(WriteResponseMeta, Option<FinalizationMarker>)> {
		let mut synthetic = table_write_synthetic_meta(
			&meta,
			bifurcated.parent_key_identity,
			bifurcated.charged_bytes,
		);
		let key = Self::idempotency_key_for(meta.parent_write_id);
		if let Some(replayed) = self.durable_mutation_replay(&meta).await? {
			return Ok(replayed);
		}
		let mut mutation_permit = Some(permit);
		// Admission and batch construction happen before this helper. They are finite but
		// synchronous, so re-check both clocks before any durable work begins. The permit
		// was acquired before that work and bounds how many such indivisible admissions
		// can exist node-wide; an overrun here is still known-not-committed.
		if let Some(expiry) = clocks.expired() {
			let response = mutation_expiry_response(
				&synthetic,
				expiry,
				MutationCommitKnowledge::KNOWN_NOT_COMMITTED,
			);
			self.record_mutation_outcome(
				meta.parent_write_id,
				meta.tenant_id,
				response,
				None,
				delete_counts,
			)
			.await?;
			return Ok((response, None));
		}

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

		if !self
			.ensure_orphan_witnesses_recovered_for_leadership()
			.await?
		{
			return Ok(self.counted_write_result(
				error_response(&synthetic, WriteStatus::NOT_LEADER, self.local_node),
				None,
			));
		}

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
		let (mut local_durable_task, mut local_progress_rx): (
			Option<JoinHandle<std::io::Result<()>>>,
			Option<mpsc::UnboundedReceiver<usize>>,
		) = if local_groups.is_empty() {
			(None, None)
		} else {
			let (local_progress_tx, local_progress_rx) = mpsc::unbounded_channel();
			let mutation_permit = mutation_permit
				.take()
				.expect("one mutation permit exists before local durable spawn");
			let slab = self.slab.clone();
			let tenant_id = meta.tenant_id;
			let secondary_index_descriptors = secondary_index_descriptors.clone();
			#[cfg(test)]
			let fail_next_local_durable_append = self.fail_next_local_durable_append.clone();
			#[cfg(test)]
			let delay_local_durable_append = self
				.delay_next_local_durable_append
				.lock()
				.expect("local durable append delay failpoint poisoned")
				.take();
			let task = tokio::spawn(async move {
				let _mutation_permit = mutation_permit;
				#[cfg(test)]
				if let Some(delay) = delay_local_durable_append {
					tokio::time::sleep(delay).await;
				}
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
				for (index, children) in local_groups {
					append_bifurcated_table_children_durable(
						&slab,
						tenant_id,
						commit_hlc,
						&children,
						&secondary_index_descriptors,
					)
					.await?;
					let _ = local_progress_tx.send(index);
				}
				Ok(())
			});
			(Some(task), Some(local_progress_rx))
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
			let (wait, expiry) = clocks.next_expiry();
			tokio::select! {
				_ = tokio::time::sleep(wait) => {
					let response = mutation_expiry_response(
						&synthetic,
						expiry,
						MutationCommitKnowledge::INDETERMINATE,
					);
					self.record_mutation_outcome(
						meta.parent_write_id,
						meta.tenant_id,
						response,
						None,
						delete_counts,
					)
						.await?;
					return Ok((response, None));
				}
				local_progress = async {
					match local_progress_rx.as_mut() {
						Some(receiver) => receiver.recv().await,
						None => None,
					}
				}, if local_progress_rx.is_some() => {
					match local_progress {
						Some(index) => {
							quorum_trackers[index].note_local_durable();
							clocks.progressed();
						}
						None => local_progress_rx = None,
					}
				}
				local_done = async {
					match local_durable_task.as_mut() {
						Some(task) => Some(task.await),
						None => None,
					}
				}, if local_durable_task.is_some() => {
					match local_done {
						Some(Ok(Ok(()))) => {}
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
							if level == ReplicaAckLevel::DURABLE {
								clocks.progressed();
							}
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
		if let Err(err) = self
			.pending_finalizations
			.upsert_with_delete_counts(pending.clone(), delete_counts)
			.await
		{
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
						.complete_pending_finalization(&pending, resolution, delete_counts)
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
				.complete_pending_finalization(&pending, resolution, delete_counts)
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
		if let Some((write, trailing)) = self.durable_mutation_replay(&meta).await? {
			return Ok((table_write_response(write, PbcRejectCode::NONE), trailing));
		}
		let clocks = WriteClocks::from_remaining_millis(meta.deadline_remaining_millis);
		if let Some(expiry) = clocks.expired() {
			let synthetic = table_write_synthetic_meta(&meta, Identity::ZERO, 0);
			let write = mutation_expiry_response(
				&synthetic,
				expiry,
				MutationCommitKnowledge::KNOWN_NOT_COMMITTED,
			);
			self.record_mutation_outcome(meta.parent_write_id, meta.tenant_id, write, None, None)
				.await?;
			return Ok((table_write_response(write, PbcRejectCode::NONE), None));
		}
		// Bound the entire admitted mutation, including synchronous bifurcation and
		// encoding—not only the later replica phase. A row-capped batch can still be
		// expensive in bytes, and admitting it outside this pool would recreate the
		// abandoned-work accumulation Decision 6 forbids.
		let mutation_permit = match self.acquire_table_mutation_permit(&meta, clocks).await? {
			Ok(permit) => permit,
			Err(response) => return Ok(response),
		};

		if self.degraded_reason().is_some() {
			let synthetic = table_write_synthetic_meta(&meta, Identity::ZERO, 0);
			let (write, trailing) = self.fenced_write_response(&synthetic).await;
			return Ok((table_write_response(write, PbcRejectCode::NONE), trailing));
		}

		let _secondary_index_admission = self.slab.acquire_secondary_index_admission_guard().await;
		let (descriptor, drop_fenced, effective_retention, secondary_index_descriptors) = {
			let state = self.state_machine.state_snapshot().await;
			let effective_retention = self.table_effective_retention_from_state(
				&state,
				meta.tenant_id,
				meta.table_object_id,
			);
			let drop_fenced = state
				.table_drop_fences
				.is_fenced(meta.tenant_id, meta.table_object_id);
			let descriptor = state
				.table_schemas
				.latest_by_id(meta.tenant_id, meta.table_object_id)
				.cloned()
				.or_else(|| {
					let saga = state
						.ctas_sagas
						.operation_for_object(meta.tenant_id, meta.table_object_id)?;
					(saga.row_write_id.canonical_idempotency_bytes()
						== meta.parent_write_id.canonical_idempotency_bytes())
					.then(|| saga.descriptor.clone())
				});
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
			(
				descriptor,
				drop_fenced,
				effective_retention,
				secondary_index_descriptors,
			)
		};
		if drop_fenced {
			return Ok((
				self.table_write_error_response(
					&meta,
					WriteStatus::INTERNAL_ERROR,
					PbcRejectCode::PBC_REJECT_SCHEMA_UNKNOWN,
				),
				None,
			));
		}
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
		if self
			.slab
			.table_integrity_failure(meta.tenant_id, meta.table_object_id)
			.is_some()
		{
			return Ok((
				self.table_write_error_response(
					&meta,
					WriteStatus::TABLE_REFERENCE_INTEGRITY_FAILED,
					PbcRejectCode::NONE,
				),
				None,
			));
		}

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
			bifurcate_table_write_with_row_group_encoding_and_chunk_layout_config(
				&meta,
				&payload,
				&descriptor,
				writer_node_id,
				commit_hlc,
				self.slab.row_group_writer_config(),
				self.slab.encoding_writer_config(),
				self.slab.l2_chunk_layout_config(),
			)
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
			self.record_mutation_outcome(meta.parent_write_id, meta.tenant_id, write, None, None)
				.await?;
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
				None,
				clocks,
				mutation_permit,
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
		descriptor: &TableSchemaDescriptor,
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
			bifurcate_table_write_with_admission_and_row_group_encoding_and_chunk_layout_config(
				meta,
				batch,
				descriptor,
				&admission,
				row_hlc,
				self.slab.row_group_writer_config(),
				self.slab.encoding_writer_config(),
				self.slab.l2_chunk_layout_config(),
			)?;
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
		descriptor: &TableSchemaDescriptor,
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
				bifurcate_table_write_with_admission_and_row_group_encoding_and_chunk_layout_config(
					meta,
					batch,
					descriptor,
					&admission,
					row_hlc,
					self.slab.row_group_writer_config(),
					self.slab.encoding_writer_config(),
					self.slab.l2_chunk_layout_config(),
				)?;
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
		let bifurcated =
			bifurcate_table_write_with_admission_and_row_group_encoding_and_chunk_layout_config(
				meta,
				&filtered_batch,
				descriptor,
				&filtered_admission,
				row_hlc,
				self.slab.row_group_writer_config(),
				self.slab.encoding_writer_config(),
				self.slab.l2_chunk_layout_config(),
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
		if let Some(replayed) = self.durable_delete_replay(&meta).await? {
			return Ok(replayed);
		}
		let clocks = WriteClocks::from_remaining_millis(meta.deadline_remaining_millis);
		if let Some(expiry) = clocks.expired() {
			let synthetic = table_delete_synthetic_write_meta(&meta, Identity::ZERO, 0);
			let write = mutation_expiry_response(
				&synthetic,
				expiry,
				MutationCommitKnowledge::KNOWN_NOT_COMMITTED,
			);
			self.record_mutation_outcome(
				meta.parent_write_id,
				meta.tenant_id,
				write,
				None,
				Some([0; 3]),
			)
			.await?;
			return Ok((
				table_delete_rows_response(write, 0, 0, 0, PbcRejectCode::NONE),
				None,
			));
		}
		let mutation_permit = match self.acquire_delete_mutation_permit(&meta, clocks).await? {
			Ok(permit) => permit,
			Err(response) => return Ok(response),
		};

		if self.degraded_reason().is_some() {
			let synthetic = table_delete_synthetic_write_meta(&meta, Identity::ZERO, 0);
			let (write, trailing) = self.fenced_write_response(&synthetic).await;
			return Ok((
				table_delete_rows_response(write, 0, 0, 0, PbcRejectCode::NONE),
				trailing,
			));
		}

		let (descriptor, drop_fenced, effective_retention) = {
			let state = self.state_machine.state_snapshot().await;
			let effective_retention = self.table_effective_retention_from_state(
				&state,
				meta.tenant_id,
				meta.table_object_id,
			);
			let drop_fenced = state
				.table_drop_fences
				.is_fenced(meta.tenant_id, meta.table_object_id);
			let descriptor = state
				.table_schemas
				.latest_by_id(meta.tenant_id, meta.table_object_id)
				.cloned();
			(descriptor, drop_fenced, effective_retention)
		};
		if drop_fenced {
			return Ok((
				self.table_delete_error_response(
					&meta,
					WriteStatus::INTERNAL_ERROR,
					PbcRejectCode::PBC_REJECT_SCHEMA_UNKNOWN,
				),
				None,
			));
		}
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
			self.record_mutation_outcome(
				meta.parent_write_id,
				meta.tenant_id,
				write,
				None,
				Some([deleted_count, already_deleted_count, not_found_count]),
			)
			.await?;
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
				Some([deleted_count, already_deleted_count, not_found_count]),
				clocks,
				mutation_permit,
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
		self.record_mutation_outcome(
			meta.parent_write_id,
			meta.tenant_id,
			write,
			trailing,
			Some([deleted_count, already_deleted_count, not_found_count]),
		)
		.await?;
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
enum DecodedRuntimeFilter {
	Absent,
	Decoded(RuntimeFilterDescriptor),
	DroppedBeforePlanning(RuntimeFilterDropReason),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TableQueryTraceCarrier {
	Absent,
	Unsampled,
	Sampled {
		trace_id: [u8; 16],
		parent_span_id: [u8; 8],
	},
}

const TABLE_QUERY_LINKED_SPAN_DOMAIN: &[u8] = b"wendelon.table_query.span.v1";

fn decode_table_query_trace_carrier(meta: &TableQueryRequestMeta) -> TableQueryTraceCarrier {
	let flags = meta.trace_flags;
	let has_trace_id = !all_zero(&meta.trace_id);
	let has_parent_span_id = !all_zero(&meta.trace_parent_span_id);
	if flags == 0 {
		return if has_trace_id || has_parent_span_id {
			TableQueryTraceCarrier::Unsampled
		} else {
			TableQueryTraceCarrier::Absent
		};
	}
	if flags & !TABLE_QUERY_TRACE_FLAG_KNOWN_MASK != 0 {
		return TableQueryTraceCarrier::Unsampled;
	}
	if flags & TABLE_QUERY_TRACE_FLAG_PRESENT == 0 {
		return TableQueryTraceCarrier::Unsampled;
	}
	if flags & TABLE_QUERY_TRACE_FLAG_SAMPLED == 0 {
		return TableQueryTraceCarrier::Unsampled;
	}
	if !has_trace_id || !has_parent_span_id {
		return TableQueryTraceCarrier::Unsampled;
	}
	TableQueryTraceCarrier::Sampled {
		trace_id: meta.trace_id,
		parent_span_id: meta.trace_parent_span_id,
	}
}

fn linked_table_query_span(
	meta: &TableQueryRequestMeta,
	carrier: TableQueryTraceCarrier,
) -> Option<Span> {
	let TableQueryTraceCarrier::Sampled {
		trace_id,
		parent_span_id,
	} = carrier
	else {
		return None;
	};
	let span_id = table_query_linked_span_id(meta, trace_id, parent_span_id);
	let trace_id = hex_trace_id(&trace_id);
	let parent_span_id = hex_span_id(&parent_span_id);
	let span_id = hex_span_id(&span_id);
	Some(tracing::info_span!(
		"wendelon.table.query",
		otel.trace_id = %trace_id,
		otel.parent_span_id = %parent_span_id,
		otel.span_id = %span_id,
		otel.sampled = true,
		wendelon.table_query.consistency = meta.consistency.0 as i64,
		wendelon.table_query.row_identity_projection =
			table_query_row_identity_projection_label(meta.row_identity_projection),
		wendelon.table_query.stats_pruning_enabled = table_query_stats_pruning_enabled(meta),
		wendelon.table_query.runtime_filter = field::Empty,
		wendelon.table_query.planner_path = field::Empty,
		wendelon.table_query.outcome = field::Empty,
		wendelon.table_query.cancellation_outcome = field::Empty,
		wendelon.table_query.rows = field::Empty,
		wendelon.table_query.arrow_bytes = field::Empty,
		wendelon.table_query.batch_count = field::Empty,
		wendelon.table_query.wall_micros = field::Empty,
		wendelon.error_class = field::Empty,
	))
}

fn local_table_query_span(
	meta: &TableQueryRequestMeta,
	stream_id: u32,
	carrier: TableQueryTraceCarrier,
) -> Span {
	match carrier {
		TableQueryTraceCarrier::Unsampled => tracing::info_span!(
			"data_ring.table_query.request",
			otel.sampled = false,
			stream_id = stream_id,
			tenant_id = meta.tenant_id.get(),
			request_id = %format!("{:032x}", meta.request_id.get()),
			table_object_id = %format_bytes_hex(meta.table_object_id.as_bytes()),
			schema_generation = meta.schema_generation,
			expected_epoch = meta.expected_epoch,
			consistency = meta.consistency.0,
		),
		TableQueryTraceCarrier::Absent | TableQueryTraceCarrier::Sampled { .. } => {
			tracing::info_span!(
				"data_ring.table_query.request",
				stream_id = stream_id,
				tenant_id = meta.tenant_id.get(),
				request_id = %format!("{:032x}", meta.request_id.get()),
				table_object_id = %format_bytes_hex(meta.table_object_id.as_bytes()),
				schema_generation = meta.schema_generation,
				expected_epoch = meta.expected_epoch,
				consistency = meta.consistency.0,
			)
		}
	}
}

fn record_linked_table_query_span_response(
	span: &Option<Span>,
	response: &QueryResponseMeta,
	runtime_filter: &'static str,
	planner_path: &'static str,
	cancellation_outcome: &'static str,
	arrow_bytes: usize,
	batch_count: usize,
	started_at: Instant,
) {
	let Some(span) = span else {
		return;
	};
	span.record("wendelon.table_query.runtime_filter", runtime_filter);
	span.record("wendelon.table_query.planner_path", planner_path);
	span.record("wendelon.table_query.outcome", response.status.as_str());
	span.record(
		"wendelon.table_query.cancellation_outcome",
		cancellation_outcome,
	);
	span.record("wendelon.table_query.rows", saturated_i64(response.rows));
	span.record(
		"wendelon.table_query.arrow_bytes",
		saturated_i64(arrow_bytes),
	);
	span.record(
		"wendelon.table_query.batch_count",
		saturated_i64(batch_count),
	);
	span.record(
		"wendelon.table_query.wall_micros",
		saturated_i64(started_at.elapsed().as_micros()),
	);
	if response.status != ReadStatus::OK {
		span.record(
			"wendelon.error_class",
			table_query_error_class(response.status),
		);
	}
}

fn record_linked_table_query_span_error(
	span: &Option<Span>,
	runtime_filter: &'static str,
	started_at: Instant,
) {
	let Some(span) = span else {
		return;
	};
	span.record("wendelon.table_query.runtime_filter", runtime_filter);
	span.record("wendelon.table_query.planner_path", "error");
	span.record("wendelon.table_query.outcome", "internal_error");
	span.record("wendelon.table_query.cancellation_outcome", "none");
	span.record("wendelon.table_query.rows", 0_i64);
	span.record("wendelon.table_query.arrow_bytes", 0_i64);
	span.record("wendelon.table_query.batch_count", 0_i64);
	span.record(
		"wendelon.table_query.wall_micros",
		saturated_i64(started_at.elapsed().as_micros()),
	);
	span.record("wendelon.error_class", "internal");
}

fn table_query_linked_span_id(
	meta: &TableQueryRequestMeta,
	trace_id: [u8; 16],
	parent_span_id: [u8; 8],
) -> [u8; 8] {
	let mut hasher = blake3::Hasher::new();
	hasher.update(TABLE_QUERY_LINKED_SPAN_DOMAIN);
	hasher.update(&trace_id);
	hasher.update(&parent_span_id);
	hasher.update(meta.request_id.as_bytes());
	let mut span_id = [0u8; 8];
	span_id.copy_from_slice(&hasher.finalize().as_bytes()[..8]);
	nonzero_span_id(span_id)
}

fn table_query_runtime_filter_state(runtime_filter: &DecodedRuntimeFilter) -> &'static str {
	match runtime_filter {
		DecodedRuntimeFilter::Absent => "absent",
		DecodedRuntimeFilter::Decoded(_) => "decoded",
		DecodedRuntimeFilter::DroppedBeforePlanning(reason) => reason.as_str(),
	}
}

fn table_query_planner_path(plan: &QueryExecutionPlan) -> &'static str {
	if plan.secondary_index_access_plan.is_some() {
		"secondary_index"
	} else {
		"table_scan"
	}
}

fn table_query_cancellation_outcome(status: ReadStatus) -> &'static str {
	if status == ReadStatus::QUERY_CANCELLED {
		"cancelled"
	} else {
		"none"
	}
}

fn table_query_error_class(status: ReadStatus) -> &'static str {
	match status {
		ReadStatus::OK => "none",
		ReadStatus::QUERY_INVALID_PROGRAM => "invalid_program",
		ReadStatus::QUERY_BUDGET_EXCEEDED => "budget_exceeded",
		ReadStatus::QUERY_CANCELLED => "cancelled",
		ReadStatus::TABLE_DESCRIPTOR_EVOLVED => "descriptor_evolved",
		ReadStatus::AUTHORIZATION_FAILED => "authorization_failed",
		ReadStatus::BACKPRESSURED => "backpressured",
		ReadStatus::RETENTION_EXPIRED => "retention_expired",
		ReadStatus::MIGRATION_INCOMPLETE => "migration_incomplete",
		ReadStatus::NOT_LEADER | ReadStatus::EPOCH_STALE => "retryable_topology",
		_ => "read_status",
	}
}

fn table_query_row_identity_projection_label(value: u8) -> &'static str {
	match RowIdentityProjection::from_u8(value) {
		Some(RowIdentityProjection::None) => "none",
		Some(RowIdentityProjection::Append) => "append",
		None => "unknown",
	}
}

fn table_query_stats_pruning_enabled(meta: &TableQueryRequestMeta) -> bool {
	meta._pad[0] & crate::schema::QUERY_REQUEST_FLAG_DISABLE_STATS_PRUNING == 0
}

fn table_query_row_limit(meta: &TableQueryRequestMeta) -> Result<Option<usize>, ReadStatus> {
	if meta.row_limit_rows == 0 {
		return Ok(None);
	}
	usize::try_from(meta.row_limit_rows)
		.map(Some)
		.map_err(|_| ReadStatus::QUERY_INVALID_PROGRAM)
}

fn saturated_i64(value: impl TryInto<i64>) -> i64 {
	value.try_into().unwrap_or(i64::MAX)
}

fn all_zero(bytes: &[u8]) -> bool {
	bytes.iter().all(|byte| *byte == 0)
}

fn nonzero_span_id(mut span_id: [u8; 8]) -> [u8; 8] {
	if span_id.iter().all(|byte| *byte == 0) {
		span_id[7] = 1;
	}
	span_id
}

fn hex_trace_id(trace_id: &[u8; 16]) -> String {
	hex_bytes(trace_id)
}

fn hex_span_id(span_id: &[u8; 8]) -> String {
	hex_bytes(span_id)
}

fn hex_bytes(bytes: &[u8]) -> String {
	let mut out = String::with_capacity(bytes.len() * 2);
	for byte in bytes {
		out.push(hex_digit(byte >> 4));
		out.push(hex_digit(byte & 0x0f));
	}
	out
}

fn hex_digit(nibble: u8) -> char {
	match nibble {
		0..=9 => char::from(b'0' + nibble),
		10..=15 => char::from(b'a' + nibble - 10),
		_ => unreachable!("nibble is masked to 4 bits"),
	}
}

#[derive(Clone, Debug)]
struct RuntimeFilterApplication {
	descriptor: RuntimeFilterDescriptor,
}

#[derive(Clone, Debug)]
struct QueryExecutionPlan {
	response: QueryResponseMeta,
	batches: Vec<RecordBatch>,
	aggregate_responses: Vec<AggregateResponse>,
	clocks: QueryClocks,
	stats_explain: QueryStatsExplainCounters,
	secondary_index_access_plan: Option<SecondaryIndexAccessPlan>,
	row_group_fetch_audit: RowGroupFetchAuditCounters,
	encoding_materialize_audit: Vec<EncodingMaterializeAuditSample>,
}

/// ADR 0088: how many rows a table-query response batch targets. `SingleRow`
/// is the default layout, so before this the storage unit was accidentally the
/// response unit — a 5,000-row read arrived as 5,000 Arrow frames, paying frame
/// encode, a round trip, and a batch construction once per stored row.
///
/// 4,096 matches Wendelon's existing point row-group target. It is a delivery
/// quantum, not a buffering license: the byte bound below and the per-location
/// `LIMIT` both still cut a batch short.
const TABLE_QUERY_RESPONSE_TARGET_ROWS: usize = 4_096;

/// An **early-flush estimate**, not the limit. Counting sliced in-memory bytes
/// is cheap and keeps a normal batch comfortably clear of the wire cap, but it
/// is not the same quantity the wire measures: encoding may compress or expand,
/// so this can never be what enforces anything. It exists to stop a wide scan
/// from accumulating an enormous batch before the authoritative check runs.
///
/// The hard limit is [`MAX_SECTION_BYTES`], enforced on the **encoded** payload
/// at the flush seam.
const TABLE_QUERY_RESPONSE_ESTIMATE_BYTES: usize = 4 * 1024 * 1024;

/// ADR 0088: coalesces ordered result batches into bounded response batches.
///
/// One accumulator serves both emission paths. Batching only the streaming one
/// would make frame count and memory posture depend on whether the caller
/// asked for a stream, which is not a distinction the table-query value
/// contract makes.
#[derive(Default)]
struct TableQueryResponseAccumulator {
	pending: Vec<RecordBatch>,
	rows: usize,
	bytes: usize,
}

impl TableQueryResponseAccumulator {
	fn pending_schema(&self) -> Option<arrow::datatypes::SchemaRef> {
		self.pending.first().map(RecordBatch::schema)
	}

	/// Slice width for a single result larger than the row target, so one
	/// oversized source batch cannot become one oversized response batch.
	fn slices(result: RecordBatch) -> Vec<RecordBatch> {
		if result.num_rows() <= TABLE_QUERY_RESPONSE_TARGET_ROWS {
			return vec![result];
		}
		let mut slices = Vec::new();
		let mut offset = 0;
		while offset < result.num_rows() {
			let length = TABLE_QUERY_RESPONSE_TARGET_ROWS.min(result.num_rows() - offset);
			slices.push(result.slice(offset, length));
			offset += length;
		}
		slices
	}

	/// Sliced in-memory size. An accounting failure is propagated, never read as
	/// zero: a zero would silently disable the early-flush estimate for exactly
	/// the batches whose size could not be determined.
	fn slice_bytes(batch: &RecordBatch) -> std::io::Result<usize> {
		let mut total = 0usize;
		for column in batch.columns() {
			// `get_slice_memory_size` measures this slice, not the allocation
			// behind it — a sliced batch would otherwise report its parent's
			// full size and flush far too early.
			let bytes = column.to_data().get_slice_memory_size().map_err(|error| {
				std::io::Error::new(
					std::io::ErrorKind::InvalidData,
					format!("table query response size accounting failed: {error}"),
				)
			})?;
			total = total.saturating_add(bytes);
		}
		Ok(total)
	}

	async fn push(
		&mut self,
		result: RecordBatch,
		writer: Option<&mut TableQueryResponseStreamWriter<'_>>,
		batches: &mut Vec<RecordBatch>,
	) -> std::io::Result<()> {
		let mut writer = writer;
		for slice in Self::slices(result) {
			if slice.num_rows() == 0 {
				continue;
			}
			// A schema transition is a flush boundary; unlike Arrow schemas are
			// never concatenated. The descriptorless compatibility path can
			// produce one.
			let schema_changed = self
				.pending_schema()
				.is_some_and(|pending| pending != slice.schema());
			let slice_bytes = Self::slice_bytes(&slice)?;
			let would_exceed = self.rows + slice.num_rows() > TABLE_QUERY_RESPONSE_TARGET_ROWS
				|| self.bytes + slice_bytes > TABLE_QUERY_RESPONSE_ESTIMATE_BYTES;
			if !self.pending.is_empty() && (schema_changed || would_exceed) {
				self.flush(writer.as_deref_mut(), batches).await?;
			}
			self.rows += slice.num_rows();
			self.bytes += slice_bytes;
			self.pending.push(slice);
			// A single slice that is already past the estimate must not sit in
			// the accumulator waiting for a boundary that will never come — the
			// check above only fires when something is already pending.
			if self.bytes >= TABLE_QUERY_RESPONSE_ESTIMATE_BYTES {
				self.flush(writer.as_deref_mut(), batches).await?;
			}
		}
		Ok(())
	}

	async fn flush(
		&mut self,
		writer: Option<&mut TableQueryResponseStreamWriter<'_>>,
		batches: &mut Vec<RecordBatch>,
	) -> std::io::Result<()> {
		if self.pending.is_empty() {
			return Ok(());
		}
		let schema = self.pending[0].schema();
		let combined = if self.pending.len() == 1 {
			self.pending.remove(0)
		} else {
			arrow::compute::concat_batches(&schema, self.pending.iter()).map_err(|error| {
				std::io::Error::new(
					std::io::ErrorKind::InvalidData,
					format!("table query response batching failed: {error}"),
				)
			})?
		};
		self.pending.clear();
		self.rows = 0;
		self.bytes = 0;
		Self::emit_within_frame_cap(combined, writer, batches).await
	}

	/// The authoritative bound. The row target and the byte estimate are both
	/// heuristics over uncompressed shape; only the encoded payload is what the
	/// wire actually carries, so it is measured here and nowhere else.
	///
	/// A batch over the cap is halved and retried rather than refused —
	/// batching must never turn a legal read into a rejected frame. Only a
	/// single row that cannot fit is refused, which is the pre-existing
	/// oversized-payload condition this record does not change.
	async fn emit_within_frame_cap(
		batch: RecordBatch,
		writer: Option<&mut TableQueryResponseStreamWriter<'_>>,
		batches: &mut Vec<RecordBatch>,
	) -> std::io::Result<()> {
		let mut writer = writer;
		// Explicit stack rather than async recursion; the halves are pushed so
		// the earlier rows pop first and response order is preserved.
		let mut remaining = vec![batch];
		while let Some(next) = remaining.pop() {
			if next.num_rows() == 0 {
				continue;
			}
			let encoded = wire_encode_batch_for_path(&next, NetworkCompressionPath::QueryResponse)?;
			if encoded.len() <= MAX_SECTION_BYTES as usize {
				match writer.as_deref_mut() {
					Some(writer) => writer.emit_encoded_batch(encoded).await?,
					None => batches.push(next),
				}
				continue;
			}
			if next.num_rows() == 1 {
				return Err(std::io::Error::new(
					std::io::ErrorKind::InvalidData,
					format!(
						"table query response row encodes to {} bytes, over the {MAX_SECTION_BYTES}-byte frame cap",
						encoded.len()
					),
				));
			}
			let mid = next.num_rows() / 2;
			remaining.push(next.slice(mid, next.num_rows() - mid));
			remaining.push(next.slice(0, mid));
		}
		Ok(())
	}
}

struct TableQueryResponseStreamWriter<'a> {
	stream: &'a mut dyn TransportStream,
	stream_id: u32,
	provisional_response: QueryResponseMeta,
	first_frame_sent: bool,
	batch_count: usize,
	total_arrow_bytes: usize,
}

impl<'a> TableQueryResponseStreamWriter<'a> {
	fn new(
		stream: &'a mut dyn TransportStream,
		stream_id: u32,
		provisional_response: QueryResponseMeta,
	) -> Self {
		Self {
			stream,
			stream_id,
			provisional_response,
			first_frame_sent: false,
			batch_count: 0,
			total_arrow_bytes: 0,
		}
	}

	fn batch_count(&self) -> usize {
		self.batch_count
	}

	fn total_arrow_bytes(&self) -> usize {
		self.total_arrow_bytes
	}

	fn has_started(&self) -> bool {
		self.first_frame_sent
	}

	/// ADR 0088: emit a payload the accumulator has already encoded and checked
	/// against the frame cap. Splitting the encode from the write is what lets
	/// the cap be enforced on the real bytes without encoding twice.
	async fn emit_encoded_batch(&mut self, arrow: Vec<u8>) -> std::io::Result<()> {
		let header = if self.first_frame_sent {
			FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::TABLE_QUERY_RESP.0,
				flags: 0,
				stream_id: self.stream_id,
				meta_len: 0,
				arrow_len: arrow.len() as u32,
				trailing_len: 0,
			}
		} else {
			let response = QueryResponseMeta {
				arrow_len: arrow.len() as u32,
				..self.provisional_response
			};
			let meta_bytes = bytemuck::bytes_of(&response);
			let header = FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::TABLE_QUERY_RESP.0,
				flags: 0,
				stream_id: self.stream_id,
				meta_len: meta_bytes.len() as u32,
				arrow_len: arrow.len() as u32,
				trailing_len: 0,
			};
			write_frame(self.stream, &header, meta_bytes, Some(&arrow), None).await?;
			self.first_frame_sent = true;
			self.batch_count = self.batch_count.saturating_add(1);
			self.total_arrow_bytes = self.total_arrow_bytes.saturating_add(arrow.len());
			return Ok(());
		};
		write_frame(self.stream, &header, &[], Some(&arrow), None).await?;
		self.batch_count = self.batch_count.saturating_add(1);
		self.total_arrow_bytes = self.total_arrow_bytes.saturating_add(arrow.len());
		Ok(())
	}

	async fn emit_terminal(&mut self, response: QueryResponseMeta) -> std::io::Result<()> {
		self.emit_terminal_with_trailing(response, None).await
	}

	async fn emit_terminal_with_integrity(
		&mut self,
		response: QueryResponseMeta,
		failure: TableReferenceIntegrityFailureMeta,
	) -> std::io::Result<()> {
		self.emit_terminal_with_trailing(response, Some(bytemuck::bytes_of(&failure)))
			.await
	}

	async fn emit_terminal_with_trailing(
		&mut self,
		response: QueryResponseMeta,
		trailing: Option<&[u8]>,
	) -> std::io::Result<()> {
		let response = QueryResponseMeta {
			arrow_len: 0,
			..response
		};
		let meta_bytes = bytemuck::bytes_of(&response);
		let header = FrameHeader {
			magic: HRFT_MAGIC,
			version: 1,
			msg_type: MsgType::TABLE_QUERY_RESP.0,
			flags: FRAME_FLAG_STREAM_END,
			stream_id: self.stream_id,
			meta_len: meta_bytes.len() as u32,
			arrow_len: 0,
			trailing_len: trailing.map(|bytes| bytes.len() as u32).unwrap_or(0),
		};
		write_frame(self.stream, &header, meta_bytes, None, trailing).await
	}
}

#[derive(Clone, Debug)]
struct ProjectedQueryLocationRead {
	batch: RecordBatch,
	row_group_fetch_audit: RowGroupFetchAuditCounters,
	encoding_materialize_audit: Vec<EncodingMaterializeAuditSample>,
}

#[derive(Clone, Debug)]
enum AggregateQueryLocationRead {
	Dense(ProjectedQueryLocationRead),
	Encoded {
		response: AggregateResponse,
		row_group_fetch_audit: RowGroupFetchAuditCounters,
	},
}

#[derive(Clone, Debug)]
struct PreparedAggregateExecution {
	range_meta: QueryRequestMeta,
	program: pbc::Program,
	descriptor: TableSchemaDescriptor,
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

struct DescriptorlessRebatchGroup {
	schema: Arc<Schema>,
	batches: Vec<RecordBatch>,
	row_key_ids: Vec<Identity>,
	max_hlc: HybridTimestamp,
}

fn runtime_filter_for_projected_execution(
	projected_execution: &QueryProjectedExecution,
) -> Option<&RuntimeFilterApplication> {
	match projected_execution {
		QueryProjectedExecution::Pbc { runtime_filter, .. } => runtime_filter.as_ref(),
		QueryProjectedExecution::Disabled | QueryProjectedExecution::Aggregate { .. } => None,
	}
}

fn prepare_runtime_filter_application(
	decoded: &DecodedRuntimeFilter,
	descriptor: &TableSchemaDescriptor,
	schema: &Schema,
	requirement: &mut ColumnFetchRequirement,
	user_column_count: usize,
) -> Option<RuntimeFilterApplication> {
	match decoded {
		DecodedRuntimeFilter::Absent => None,
		DecodedRuntimeFilter::DroppedBeforePlanning(reason) => {
			metrics().observe_storage_runtime_filter_present();
			metrics().observe_storage_runtime_filter_advisory_dropped(reason.as_str());
			None
		}
		DecodedRuntimeFilter::Decoded(runtime_filter) => {
			metrics().observe_storage_runtime_filter_present();
			if let Err(reason) = validate_runtime_filter_descriptor(
				runtime_filter,
				schema,
				descriptor.physical_descriptor_fingerprint,
			) {
				metrics().observe_storage_runtime_filter_advisory_dropped(reason.as_str());
				return None;
			}
			for ordinal in &runtime_filter.probe_key_columns {
				let Ok(index) = usize::try_from(*ordinal) else {
					metrics().observe_storage_runtime_filter_advisory_dropped(
						RuntimeFilterDropReason::MaterializationIncompatible.as_str(),
					);
					return None;
				};
				if index >= user_column_count {
					metrics().observe_storage_runtime_filter_advisory_dropped(
						RuntimeFilterDropReason::MaterializationIncompatible.as_str(),
					);
					return None;
				}
			}
			for ordinal in &runtime_filter.probe_key_columns {
				requirement.predicate_columns.insert(*ordinal);
			}
			metrics().observe_storage_runtime_filter_applied();
			Some(RuntimeFilterApplication {
				descriptor: runtime_filter.clone(),
			})
		}
	}
}

fn runtime_filter_mask_for_projected(
	projected: &ProjectedRecordBatch,
	descriptor: &RuntimeFilterDescriptor,
) -> Result<arrow::array::BooleanArray, RuntimeFilterDropReason> {
	let mut key_arrays = Vec::with_capacity(descriptor.probe_key_columns.len());
	for (position, ordinal) in descriptor.probe_key_columns.iter().enumerate() {
		let physical = projected
			.physical_for(*ordinal)
			.ok_or(RuntimeFilterDropReason::MaterializationIncompatible)?;
		let array = projected
			.batch
			.columns()
			.get(physical)
			.ok_or(RuntimeFilterDropReason::MaterializationIncompatible)?;
		if runtime_filter_key_type(array.data_type()) != Some(descriptor.key_types[position]) {
			return Err(RuntimeFilterDropReason::UnsupportedShape);
		}
		key_arrays.push(array.as_ref());
	}
	build_runtime_filter_mask(descriptor, &key_arrays)
}

fn apply_runtime_filter_to_projected(
	projected: ProjectedRecordBatch,
	application: &RuntimeFilterApplication,
) -> std::io::Result<ProjectedRecordBatch> {
	let input_rows = projected.batch.num_rows();
	let mask = match runtime_filter_mask_for_projected(&projected, &application.descriptor) {
		Ok(mask) => mask,
		Err(reason) => {
			metrics().observe_storage_runtime_filter_advisory_dropped(reason.as_str());
			return Ok(projected);
		}
	};
	let row_indices = (0..mask.len())
		.filter(|row| mask.value(*row))
		.collect::<Vec<_>>();
	let filtered = projected.take_rows(&row_indices)?;
	metrics()
		.observe_storage_runtime_filter_rows(input_rows as u64, filtered.batch.num_rows() as u64);
	Ok(filtered)
}

fn apply_runtime_filter_to_batch(
	batch: RecordBatch,
	application: &RuntimeFilterApplication,
) -> std::io::Result<RecordBatch> {
	let input_rows = batch.num_rows();
	let mask = match build_runtime_filter_mask_for_batch(&batch, &application.descriptor) {
		Ok(mask) => mask,
		Err(reason) => {
			metrics().observe_storage_runtime_filter_advisory_dropped(reason.as_str());
			return Ok(batch);
		}
	};
	let filtered = arrow::compute::filter_record_batch(&batch, &mask).map_err(|err| {
		Error::new(
			ErrorKind::InvalidData,
			format!("runtime filter batch filter failed: {err}"),
		)
	})?;
	metrics().observe_storage_runtime_filter_rows(input_rows as u64, filtered.num_rows() as u64);
	Ok(filtered)
}

fn filter_row_key_ids_by_mask(
	row_key_ids: &[Identity],
	mask: &arrow::array::BooleanArray,
) -> Vec<Identity> {
	row_key_ids
		.iter()
		.enumerate()
		.filter_map(|(row, row_key_id)| {
			(!mask.is_null(row) && mask.value(row)).then_some(*row_key_id)
		})
		.collect()
}

fn append_query_row_identity_column(
	batch: RecordBatch,
	row_key_ids: &[Identity],
) -> std::io::Result<RecordBatch> {
	if row_key_ids.len() != batch.num_rows() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"row identity projection cardinality does not match query batch",
		));
	}
	let mut builder = FixedSizeBinaryBuilder::with_capacity(row_key_ids.len(), 32);
	for row_key_id in row_key_ids {
		builder.append_value(row_key_id.as_bytes()).map_err(|err| {
			Error::new(ErrorKind::InvalidData, format!("row key id append: {err}"))
		})?;
	}
	let mut fields = batch
		.schema()
		.fields()
		.iter()
		.map(|field| field.as_ref().clone())
		.collect::<Vec<_>>();
	fields.push(Field::new(
		STORAGE_ROW_KEY_ID_COLUMN,
		DataType::FixedSizeBinary(32),
		false,
	));
	let mut columns = batch.columns().to_vec();
	columns.push(Arc::new(builder.finish()) as ArrayRef);
	RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(|err| {
		Error::new(
			ErrorKind::InvalidData,
			format!("row identity projection batch: {err}"),
		)
	})
}

#[derive(Clone, Copy, Debug)]
struct QueryExecutionBudget {
	/// Total caps, enforced only when `caller_totals` is set: only a caller-provided
	/// budget may cap what a query touches in total (ADR 541). With server defaults
	/// these bound work-per-progress derivations, never totality.
	max_locations: usize,
	max_rows: u64,
	/// ADR 541: the sliding no-progress bound. It is not a lifetime — a progressing
	/// scan resets it on every unit of progress; only a stalled scan expires it.
	max_wall_time: Duration,
	stats_pruning_enabled: bool,
	/// True only for caller-transmitted budgets (the aggregate protocol today).
	caller_totals: bool,
	/// The caller's absolute end-to-end deadline, derived locally from the remaining
	/// duration on the wire. Non-resetting; enforced concurrently with the stall
	/// bound, never as "the tighter" of two comparable durations.
	caller_deadline: Option<Instant>,
}

impl Default for QueryExecutionBudget {
	fn default() -> Self {
		Self {
			max_locations: 4_096,
			max_rows: 65_536,
			max_wall_time: Duration::from_millis(250),
			stats_pruning_enabled: true,
			caller_totals: false,
			caller_deadline: None,
		}
	}
}

/// The typed table-query budget: server defaults for stall and per-progress work,
/// plus the caller's deadline derived locally from the remaining duration on the
/// wire (zero = absent). Deriving a monotonic instant HERE is what keeps cross-host
/// wall clocks out of the protocol.
fn table_query_execution_budget(meta: &TableQueryRequestMeta) -> QueryExecutionBudget {
	let caller_deadline = caller_deadline_from_remaining_millis(meta.deadline_remaining_millis);
	QueryExecutionBudget {
		caller_deadline,
		..QueryExecutionBudget::default()
	}
}

fn row_limit_satisfied(rows: u64, row_limit: Option<usize>) -> bool {
	row_limit.is_some_and(|limit| rows >= limit as u64)
}

fn take_output_batch(
	batch: RecordBatch,
	rows: &mut u64,
	row_limit: Option<usize>,
) -> Option<RecordBatch> {
	let Some(limit) = row_limit else {
		*rows = rows.saturating_add(batch.num_rows() as u64);
		return Some(batch);
	};
	let limit = limit as u64;
	if *rows >= limit {
		return None;
	}
	let remaining = usize::try_from(limit - *rows).unwrap_or(usize::MAX);
	let emit = batch.num_rows().min(remaining);
	*rows = rows.saturating_add(emit as u64);
	if emit == batch.num_rows() {
		Some(batch)
	} else {
		Some(batch.slice(0, emit))
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
		request: request.clone(),
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
	descriptor: &TableSchemaDescriptor,
	program: &pbc::Program,
	projected_execution: &QueryProjectedExecution,
) -> std::io::Result<Option<ProjectedQueryLocationRead>> {
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
		.read_projected_record(location, descriptor, &requirement, Some(program))
		.await
	{
		Ok(Some(projected_read)) => projected_read,
		Ok(None) => return Ok(None),
		Err(err) if projected_query_can_use_whole_record_fallback(&err) => return Ok(None),
		Err(err) => return Err(err),
	};
	let row_group_fetch_audit =
		RowGroupFetchAuditCounters::from_l2_strategy(&projected_read.strategy);
	let encoding_materialize_audit = projected_read.encoding_materialize_audit.clone();
	if projected_read.projected.batch.num_rows() == 0 {
		return Ok(Some(ProjectedQueryLocationRead {
			batch: projected_read.projected.batch,
			row_group_fetch_audit,
			encoding_materialize_audit,
		}));
	}
	let mut projected = materialize_projected_location_batch(
		projected_read.projected,
		projected_read.projected_first_record_row_index,
		location.record_row_index,
	)?;
	if !projected_descriptor_match(descriptor, &projected)? {
		return Ok(None);
	}
	if let Some(runtime_filter) = runtime_filter_for_projected_execution(projected_execution) {
		projected = apply_runtime_filter_to_projected(projected, runtime_filter)?;
	}
	match projected_execution {
		QueryProjectedExecution::Disabled => Ok(None),
		QueryProjectedExecution::Pbc {
			requirement,
			user_column_count,
			..
		} => {
			execute_pbc_projected_record_batch(&projected, program, requirement, *user_column_count)
				.map(|batch| {
					Some(ProjectedQueryLocationRead {
						batch,
						row_group_fetch_audit,
						encoding_materialize_audit,
					})
				})
		}
		QueryProjectedExecution::Aggregate { .. } => Ok(None),
	}
}

async fn try_read_encoded_aggregate_query_location(
	slab: &SlabManager,
	request: &AggregateRequest,
	location: RecordLocation,
	descriptor: &TableSchemaDescriptor,
	program: &pbc::Program,
) -> std::io::Result<Option<AggregateQueryLocationRead>> {
	if !descriptor.record_layout_policy.is_multi_row() {
		return Ok(None);
	}
	let verification_requirement = augment_requirement_for_descriptor_verification(
		&ColumnFetchRequirement::default(),
		descriptor,
	);
	if verification_requirement.storage_columns.is_empty() {
		return Ok(None);
	}
	let Some(verification_read) = slab
		.read_projected_record(location, descriptor, &verification_requirement, None)
		.await?
	else {
		return Ok(None);
	};
	let mut row_group_fetch_audit =
		RowGroupFetchAuditCounters::from_l2_strategy(&verification_read.strategy);
	if verification_read.projected.batch.num_rows() == 0 {
		return Ok(None);
	}
	let projected = materialize_projected_location_batch(
		verification_read.projected,
		verification_read.projected_first_record_row_index,
		location.record_row_index,
	)?;
	if !projected_descriptor_match(descriptor, &projected)? {
		return Ok(None);
	}
	let Some(encoded) = slab
		.read_encoded_aggregate_record(location, descriptor, request, program)
		.await?
	else {
		return Ok(None);
	};
	row_group_fetch_audit.merge(RowGroupFetchAuditCounters::from_l2_strategy(
		&encoded.strategy,
	));
	let response = encoded_aggregate_location_response(request, location, encoded);
	Ok(Some(AggregateQueryLocationRead::Encoded {
		response,
		row_group_fetch_audit,
	}))
}

fn encoded_aggregate_location_response(
	request: &AggregateRequest,
	location: RecordLocation,
	encoded: EncodedAggregateL2RecordRead,
) -> AggregateResponse {
	AggregateResponse {
		format_version: request.format_version,
		request_id: request.request_id,
		status: AggregateStatus::Success,
		reject_code: None,
		table_object_id: request.table_object_id,
		schema_generation: request.schema_generation,
		logical_schema_fingerprint: request.logical_schema_fingerprint,
		aggregate_expression_digest: request.expression_digest(),
		as_of_hlc: request.as_of_hlc,
		max_hlc: if encoded.rows_admitted == 0 {
			HybridTimestamp {
				physical_ms: 0,
				logical: 0,
				node_id: 0,
			}
		} else {
			location.hlc
		},
		partials: encoded.partials,
		diagnostics: AggregateDiagnostics {
			rows_scanned: encoded.rows_admitted,
			rows_admitted: encoded.rows_admitted,
			..AggregateDiagnostics::default()
		},
	}
}

async fn read_aggregate_query_location(
	slab: &SlabManager,
	request: &AggregateRequest,
	meta: &QueryRequestMeta,
	location: RecordLocation,
	key_identity: Identity,
	locality: Locality,
	descriptor: &TableSchemaDescriptor,
	program: &pbc::Program,
	requirement: &ColumnFetchRequirement,
	user_column_count: usize,
) -> std::io::Result<Option<AggregateQueryLocationRead>> {
	if let Some(encoded) =
		try_read_encoded_aggregate_query_location(slab, request, location, descriptor, program)
			.await?
	{
		return Ok(Some(encoded));
	}
	// ADR 045 §5 / Phase 8.5: ensure descriptor-filtered AppendOnly
	// queries fetch `__row_table_object_id__` so the projected fast
	// path can verify the row's table-isolation binding. See
	// `read_projected_query_location` for the same plumbing.
	let augmented_requirement =
		augment_requirement_for_descriptor_verification(requirement, descriptor);
	match slab
		.read_projected_record(location, descriptor, &augmented_requirement, Some(program))
		.await
	{
		Ok(Some(projected_read)) => {
			let row_group_fetch_audit =
				RowGroupFetchAuditCounters::from_l2_strategy(&projected_read.strategy);
			let encoding_materialize_audit = projected_read.encoding_materialize_audit.clone();
			if projected_read.projected.batch.num_rows() == 0 {
				return Ok(Some(AggregateQueryLocationRead::Dense(
					ProjectedQueryLocationRead {
						batch: projected_read.projected.batch,
						row_group_fetch_audit,
						encoding_materialize_audit,
					},
				)));
			}
			let projected = materialize_projected_location_batch(
				projected_read.projected,
				projected_read.projected_first_record_row_index,
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
				.map(|batch| {
					Some(AggregateQueryLocationRead::Dense(
						ProjectedQueryLocationRead {
							batch,
							row_group_fetch_audit,
							encoding_materialize_audit,
						},
					))
				});
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
	execute_aggregate_full_record_batch(&batch, program, requirement).map(|batch| {
		Some(AggregateQueryLocationRead::Dense(
			ProjectedQueryLocationRead {
				batch,
				row_group_fetch_audit: RowGroupFetchAuditCounters::default(),
				encoding_materialize_audit: Vec::new(),
			},
		))
	})
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

fn row_group_invariant_violation_kind(error: &Error) -> Option<&'static str> {
	error
		.get_ref()
		.and_then(|source| source.downcast_ref::<ColumnFetchError>())
		.and_then(|error| error.row_group_reject_code())
		.map(|code| code.as_str())
}

fn encoding_invariant_violation_kind(error: &Error) -> Option<&'static str> {
	error
		.get_ref()
		.and_then(|source| source.downcast_ref::<ColumnFetchError>())
		.and_then(|error| error.encoding_reject_code())
		.map(|code| code.as_str())
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
	descriptor: &TableSchemaDescriptor,
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
	descriptor: &TableSchemaDescriptor,
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
	projected_first_record_row_index: RecordRowIndex,
	record_row_index: RecordRowIndex,
) -> std::io::Result<ProjectedRecordBatch> {
	let row = record_row_index
		.get()
		.checked_sub(projected_first_record_row_index.get())
		.ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				format!(
					"record row index {} precedes projected batch base {}",
					record_row_index.get(),
					projected_first_record_row_index.get()
				),
			)
		})? as usize;
	if row >= projected.batch.num_rows() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"record row index {} out of bounds for projected storage record base {} row_count {}",
				record_row_index.get(),
				projected_first_record_row_index.get(),
				projected.batch.num_rows()
			),
		));
	}
	Ok(projected.slice(row, 1))
}

async fn table_stats_may_match_location(
	slab: &SlabManager,
	table_descriptor: Option<&TableSchemaDescriptor>,
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
	descriptor: &TableSchemaDescriptor,
	program: &pbc::Program,
	stats: &ParsedColumnStatsBlock,
) -> std::io::Result<bool> {
	Ok(slab_stats_query_outcome(descriptor, program, stats)?.may_match)
}

fn slab_stats_query_outcome(
	descriptor: &TableSchemaDescriptor,
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
	predicate: &pbc::ColumnPredicate,
) -> std::io::Result<Option<EncodedStatsPredicate>> {
	match predicate.comparison {
		pbc::ColumnPredicateComparison::Eq => {
			let Some(value) = encode_stats_literal(data_type, &predicate.literal)? else {
				return Ok(None);
			};
			Ok(Some(EncodedStatsPredicate::AnyOf(vec![value])))
		}
		pbc::ColumnPredicateComparison::Gt | pbc::ColumnPredicateComparison::Gte => {
			let Some(value) = encode_stats_literal(data_type, &predicate.literal)? else {
				return Ok(None);
			};
			Ok(Some(EncodedStatsPredicate::Range {
				lower: Some(value),
				upper: None,
			}))
		}
		pbc::ColumnPredicateComparison::Lt | pbc::ColumnPredicateComparison::Lte => {
			let Some(value) = encode_stats_literal(data_type, &predicate.literal)? else {
				return Ok(None);
			};
			Ok(Some(EncodedStatsPredicate::Range {
				lower: None,
				upper: Some(value),
			}))
		}
		pbc::ColumnPredicateComparison::InList => {
			let pbc::ColumnPredicateLiteral::List(literals) = &predicate.literal else {
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
	literal: &pbc::ColumnPredicateLiteral,
) -> std::io::Result<Option<Vec<u8>>> {
	match (data_type, literal) {
		(DataType::Int64, pbc::ColumnPredicateLiteral::Int(value))
		| (DataType::Int64, pbc::ColumnPredicateLiteral::Ts(value))
		| (
			DataType::Timestamp(TimeUnit::Millisecond, _),
			pbc::ColumnPredicateLiteral::Int(value),
		)
		| (DataType::Timestamp(TimeUnit::Millisecond, _), pbc::ColumnPredicateLiteral::Ts(value)) => {
			encode_signed_stat_value(*value, 8).map(Some)
		}
		(DataType::Int32, pbc::ColumnPredicateLiteral::Int(value))
		| (DataType::Int32, pbc::ColumnPredicateLiteral::Ts(value)) => {
			let Ok(value) = i32::try_from(*value) else {
				return Ok(None);
			};
			encode_signed_stat_value(i64::from(value), 4).map(Some)
		}
		(DataType::Utf8 | DataType::LargeUtf8, pbc::ColumnPredicateLiteral::Str(bytes)) => {
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
		None,
		QueryProjectedExecution::Disabled,
		RowIdentityProjection::None,
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
	row_limit: Option<usize>,
	table_descriptor: &TableSchemaDescriptor,
	projected_execution: &QueryProjectedExecution,
	logical_retention_floor: Option<HybridTimestamp>,
	access_plan: &SecondaryIndexAccessPlan,
	cancellation_token: Option<&CancellationToken>,
	mut stream_writer: Option<&mut TableQueryResponseStreamWriter<'_>>,
) -> std::io::Result<QueryExecutionPlan> {
	let budget = QueryExecutionBudget {
		stats_pruning_enabled: budget.stats_pruning_enabled
			&& query_request_stats_pruning_enabled(meta),
		..budget
	};
	let mut clocks = QueryClocks::from_budget(&budget);
	let max_rows_per_location = usize::try_from(budget.max_rows).unwrap_or(usize::MAX);
	let mut stats_prune_cache = QueryStatsPruneCache::default();
	let mut batches = Vec::new();
	// ADR 0088: rows coalesce here instead of one frame per location.
	let mut response_batches = TableQueryResponseAccumulator::default();
	let mut rows = 0u64;
	let mut scanned_locations = 0usize;
	let mut work_locations_since_yield = 0usize;
	let mut work_rows_since_yield = 0u64;
	let mut max_hlc = HybridTimestamp {
		physical_ms: 0,
		logical: 0,
		node_id: 0,
	};
	let mut row_group_fetch_audit = RowGroupFetchAuditCounters::default();
	let mut encoding_materialize_audit = Vec::new();
	let mut seen = BTreeSet::new();
	metrics().observe_pbc_locality_arcs_scanned(ranges.len() as u64);
	if let Some(cancel) = cancellation_token {
		if cancel.is_cancelled() {
			return Ok(query_plan_error(
				meta,
				ReadStatus::QUERY_CANCELLED,
				local_node,
			));
		}
	}

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
	let mut indexed_rows = Vec::new();
	for row in &lookup.rows {
		if let Some(cancel) = cancellation_token {
			if cancel.is_cancelled() {
				return Ok(query_plan_error(
					meta,
					ReadStatus::QUERY_CANCELLED,
					local_node,
				));
			}
		}
		// Examining a row is progress whatever its disposition — retention-filtered,
		// duplicate and rejected rows are work done, and a healthy large no-match
		// pass must never read as a stall.
		clocks.progressed();
		if logical_retention_floor.is_some_and(|floor| row.row_hlc < floor) {
			metrics().observe_storage_table_retention_read_filtered_rows(1);
			continue;
		}
		if let Some(expired) = clocks.expired() {
			return Ok(query_plan_error(meta, expired, local_node));
		}
		scanned_locations = scanned_locations.saturating_add(1);
		if budget.caller_totals && scanned_locations > budget.max_locations {
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
		indexed_rows.push(row.clone());
	}
	'indexed_rows: for indexed_chunk in indexed_rows.chunks(4) {
		if row_limit_satisfied(rows, row_limit) {
			break;
		}
		let chunk_reads = match clocks
			.bound(read_secondary_index_query_locations(
				slab,
				indexed_chunk,
				table_descriptor,
				program,
				projected_execution,
			))
			.await
		{
			Ok(reads) => reads?,
			Err(expired) => return Ok(query_plan_error(meta, expired, local_node)),
		};
		for (row, result) in chunk_reads {
			if let Some(cancel) = cancellation_token {
				if cancel.is_cancelled() {
					return Ok(query_plan_error(
						meta,
						ReadStatus::QUERY_CANCELLED,
						local_node,
					));
				}
			}
			row_group_fetch_audit.merge(result.row_group_fetch_audit);
			encoding_materialize_audit.extend(result.encoding_materialize_audit);
			let result = result.batch;
			if result.num_rows() == 0 {
				continue;
			}
			let Some(result) = take_output_batch(result, &mut rows, row_limit) else {
				break 'indexed_rows;
			};
			if result.num_rows() > max_rows_per_location {
				return Ok(query_plan_error(
					meta,
					ReadStatus::QUERY_BUDGET_EXCEEDED,
					local_node,
				));
			}
			if row.row_hlc > max_hlc {
				max_hlc = row.row_hlc;
			}
			work_rows_since_yield = work_rows_since_yield.saturating_add(result.num_rows() as u64);
			if budget.caller_totals && rows > budget.max_rows {
				return Ok(query_plan_error(
					meta,
					ReadStatus::QUERY_BUDGET_EXCEEDED,
					local_node,
				));
			}
			if let Some(expired) = clocks.expired() {
				return Ok(query_plan_error(meta, expired, local_node));
			}
			// ADR 0088: coalesced rather than emitted per location.
			response_batches
				.push(result, stream_writer.as_deref_mut(), &mut batches)
				.await?;
			if row_limit_satisfied(rows, row_limit) {
				break 'indexed_rows;
			}
		}
	}

	let tail_source_slabs = slab
		.secondary_index_tail_source_slabs(meta.tenant_id, &access_plan.descriptor)
		.await;
	'ranges: for range in ranges {
		if row_limit_satisfied(rows, row_limit) {
			break;
		}
		if let Some(cancel) = cancellation_token {
			if cancel.is_cancelled() {
				return Ok(query_plan_error(
					meta,
					ReadStatus::QUERY_CANCELLED,
					local_node,
				));
			}
		}
		let scanned = slab.scan_as_of_with_identity_and_locality(
			meta.tenant_id,
			range.start,
			range.end,
			meta.as_of_hlc,
		);
		for (key_identity, locality, location) in scanned {
			// Examining a location is progress whatever its disposition — pruned,
			// filtered, duplicate and mismatched locations are work done, and a
			// healthy large no-match tail scan must never read as a stall.
			clocks.progressed();
			work_locations_since_yield = work_locations_since_yield.saturating_add(1);
			if work_locations_since_yield >= budget.max_locations
				|| work_rows_since_yield >= budget.max_rows
			{
				tokio::task::yield_now().await;
				work_locations_since_yield = 0;
				work_rows_since_yield = 0;
			}
			if row_limit_satisfied(rows, row_limit) {
				break 'ranges;
			}
			if let Some(cancel) = cancellation_token {
				if cancel.is_cancelled() {
					return Ok(query_plan_error(
						meta,
						ReadStatus::QUERY_CANCELLED,
						local_node,
					));
				}
			}
			if !tail_source_slabs.contains(&location.slab_id) {
				continue;
			}
			if logical_retention_floor.is_some_and(|floor| location.hlc < floor) {
				metrics().observe_storage_table_retention_read_filtered_rows(1);
				continue;
			}
			if let Some(expired) = clocks.expired() {
				return Ok(query_plan_error(meta, expired, local_node));
			}
			if budget.stats_pruning_enabled {
				let may_match = match clocks
					.bound(table_stats_may_match_location(
						slab,
						Some(table_descriptor),
						program,
						&mut stats_prune_cache,
						location,
					))
					.await
				{
					Ok(may_match) => may_match?,
					Err(expired) => {
						return Ok(query_plan_error(meta, expired, local_node));
					}
				};
				if !may_match {
					continue;
				}
			}
			scanned_locations = scanned_locations.saturating_add(1);
			if budget.caller_totals && scanned_locations > budget.max_locations {
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
			let read = match clocks
				.bound(read_table_query_location(
					slab,
					meta,
					location,
					key_identity,
					locality,
					table_descriptor,
					program,
					projected_execution,
				))
				.await
			{
				Ok(read) => read?,
				Err(expired) => return Ok(query_plan_error(meta, expired, local_node)),
			};
			let Some(result) = read else {
				continue;
			};
			row_group_fetch_audit.merge(result.row_group_fetch_audit);
			encoding_materialize_audit.extend(result.encoding_materialize_audit);
			let result = result.batch;
			if result.num_rows() == 0 {
				continue;
			}
			let Some(result) = take_output_batch(result, &mut rows, row_limit) else {
				break 'ranges;
			};
			// An oversize location terminates only a caller-capped query; with server
			// defaults it is one large unit of bounded work, not a failure.
			if budget.caller_totals && result.num_rows() > max_rows_per_location {
				return Ok(query_plan_error(
					meta,
					ReadStatus::QUERY_BUDGET_EXCEEDED,
					local_node,
				));
			}
			if location.hlc > max_hlc {
				max_hlc = location.hlc;
			}
			if budget.caller_totals && rows > budget.max_rows {
				return Ok(query_plan_error(
					meta,
					ReadStatus::QUERY_BUDGET_EXCEEDED,
					local_node,
				));
			}
			if let Some(expired) = clocks.expired() {
				return Ok(query_plan_error(meta, expired, local_node));
			}
			// ADR 0088: coalesced rather than emitted per location.
			response_batches
				.push(result, stream_writer.as_deref_mut(), &mut batches)
				.await?;
			if row_limit_satisfied(rows, row_limit) {
				break 'ranges;
			}
		}
	}

	if rows == 0 && batches.is_empty() {
		let mut plan = query_plan_error(meta, ReadStatus::KEY_NOT_FOUND, local_node);
		// An empty result is a legitimate answer produced by real work: the live
		// clocks (and their progress marks) belong to it like any other exit.
		plan.clocks = clocks;
		plan.stats_explain = stats_prune_cache.explain;
		plan.secondary_index_access_plan = Some(access_plan.clone());
		plan.row_group_fetch_audit = row_group_fetch_audit;
		plan.encoding_materialize_audit = encoding_materialize_audit;
		return Ok(plan);
	}

	// ADR 0088: the accumulator's tail must reach the caller — a bounded
	// read that stops short of the row target still owes its rows.
	response_batches
		.flush(stream_writer.as_deref_mut(), &mut batches)
		.await?;
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
		aggregate_responses: Vec::new(),
		clocks,
		stats_explain: stats_prune_cache.explain,
		secondary_index_access_plan: Some(access_plan.clone()),
		row_group_fetch_audit,
		encoding_materialize_audit,
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

fn secondary_index_record_fetch_key(location: RecordLocation) -> (SlabId, u64, u32, bool) {
	(
		location.slab_id,
		location.offset,
		location.length,
		location.is_tombstone,
	)
}

async fn read_secondary_index_query_locations(
	slab: &SlabManager,
	rows: &[SecondaryIndexLookupRow],
	descriptor: &TableSchemaDescriptor,
	program: &pbc::Program,
	projected_execution: &QueryProjectedExecution,
) -> std::io::Result<Vec<(SecondaryIndexLookupRow, ProjectedQueryLocationRead)>> {
	let Some(requirement) = projected_execution.requirement() else {
		return read_secondary_index_query_locations_one_by_one(
			slab,
			rows,
			descriptor,
			program,
			projected_execution,
		)
		.await;
	};
	let augmented_requirement =
		augment_requirement_for_descriptor_verification(requirement, descriptor);
	let mut group_order = Vec::new();
	let mut groups: HashMap<(SlabId, u64, u32, bool), Vec<usize>> = HashMap::new();
	for (index, row) in rows.iter().enumerate() {
		let key = secondary_index_record_fetch_key(row.record_location);
		if !groups.contains_key(&key) {
			group_order.push(key);
		}
		groups.entry(key).or_default().push(index);
	}

	let group_inputs = group_order
		.into_iter()
		.map(|key| groups.remove(&key).unwrap_or_default())
		.collect::<Vec<_>>();
	let max_parallel = slab.row_group_fetch_parallelism().clamp(1, 4);
	let mut out = Vec::new();
	for chunk in group_inputs.chunks(max_parallel) {
		match chunk.len() {
			0 => {}
			1 => out.extend(
				read_secondary_index_query_location_group(
					slab,
					rows,
					&chunk[0],
					descriptor,
					program,
					projected_execution,
					&augmented_requirement,
				)
				.await?,
			),
			2 => {
				let (a, b) = tokio::join!(
					read_secondary_index_query_location_group(
						slab,
						rows,
						&chunk[0],
						descriptor,
						program,
						projected_execution,
						&augmented_requirement,
					),
					read_secondary_index_query_location_group(
						slab,
						rows,
						&chunk[1],
						descriptor,
						program,
						projected_execution,
						&augmented_requirement,
					)
				);
				out.extend(a?);
				out.extend(b?);
			}
			3 => {
				let (a, b, c) = tokio::join!(
					read_secondary_index_query_location_group(
						slab,
						rows,
						&chunk[0],
						descriptor,
						program,
						projected_execution,
						&augmented_requirement,
					),
					read_secondary_index_query_location_group(
						slab,
						rows,
						&chunk[1],
						descriptor,
						program,
						projected_execution,
						&augmented_requirement,
					),
					read_secondary_index_query_location_group(
						slab,
						rows,
						&chunk[2],
						descriptor,
						program,
						projected_execution,
						&augmented_requirement,
					)
				);
				out.extend(a?);
				out.extend(b?);
				out.extend(c?);
			}
			_ => {
				let (a, b, c, d) = tokio::join!(
					read_secondary_index_query_location_group(
						slab,
						rows,
						&chunk[0],
						descriptor,
						program,
						projected_execution,
						&augmented_requirement,
					),
					read_secondary_index_query_location_group(
						slab,
						rows,
						&chunk[1],
						descriptor,
						program,
						projected_execution,
						&augmented_requirement,
					),
					read_secondary_index_query_location_group(
						slab,
						rows,
						&chunk[2],
						descriptor,
						program,
						projected_execution,
						&augmented_requirement,
					),
					read_secondary_index_query_location_group(
						slab,
						rows,
						&chunk[3],
						descriptor,
						program,
						projected_execution,
						&augmented_requirement,
					)
				);
				out.extend(a?);
				out.extend(b?);
				out.extend(c?);
				out.extend(d?);
			}
		}
	}
	Ok(out)
}

async fn read_secondary_index_query_location_group(
	slab: &SlabManager,
	rows: &[SecondaryIndexLookupRow],
	indices: &[usize],
	descriptor: &TableSchemaDescriptor,
	program: &pbc::Program,
	projected_execution: &QueryProjectedExecution,
	augmented_requirement: &ColumnFetchRequirement,
) -> std::io::Result<Vec<(SecondaryIndexLookupRow, ProjectedQueryLocationRead)>> {
	let locations = indices
		.iter()
		.map(|index| rows[*index].record_location)
		.collect::<Vec<_>>();
	match slab
		.read_projected_indexed_rows(&locations, descriptor, augmented_requirement)
		.await
	{
		Ok(Some(projected_read)) => {
			if projected_read.projected.batch.num_rows() != indices.len() {
				return Err(Error::new(
					ErrorKind::InvalidData,
					format!(
						"projected indexed row fetch returned {} rows for {} hits",
						projected_read.projected.batch.num_rows(),
						indices.len()
					),
				));
			}
			let mut out = Vec::new();
			let mut audit = RowGroupFetchAuditCounters::from_l2_strategy(&projected_read.strategy);
			let mut encoding_materialize_audit = projected_read.encoding_materialize_audit;
			for (position, index) in indices.iter().enumerate() {
				let row = rows[*index].clone();
				let projected = projected_read.projected.slice(position, 1);
				let result = if projected_descriptor_match(descriptor, &projected)? {
					execute_secondary_index_projected_row(projected, program, projected_execution)?
				} else {
					read_secondary_index_query_location(
						slab,
						&row,
						descriptor,
						program,
						projected_execution,
					)
					.await?
				};
				if let Some(mut result) = result {
					result.row_group_fetch_audit.merge(audit);
					audit = RowGroupFetchAuditCounters::default();
					result
						.encoding_materialize_audit
						.append(&mut encoding_materialize_audit);
					out.push((row, result));
				}
			}
			Ok(out)
		}
		Ok(None) => {
			let fallback_rows = indices
				.iter()
				.map(|index| rows[*index].clone())
				.collect::<Vec<_>>();
			read_secondary_index_query_locations_one_by_one(
				slab,
				&fallback_rows,
				descriptor,
				program,
				projected_execution,
			)
			.await
		}
		Err(err) if projected_query_can_use_whole_record_fallback(&err) => {
			let fallback_rows = indices
				.iter()
				.map(|index| rows[*index].clone())
				.collect::<Vec<_>>();
			read_secondary_index_query_locations_one_by_one(
				slab,
				&fallback_rows,
				descriptor,
				program,
				projected_execution,
			)
			.await
		}
		Err(err) => Err(err),
	}
}

async fn read_secondary_index_query_locations_one_by_one(
	slab: &SlabManager,
	rows: &[SecondaryIndexLookupRow],
	descriptor: &TableSchemaDescriptor,
	program: &pbc::Program,
	projected_execution: &QueryProjectedExecution,
) -> std::io::Result<Vec<(SecondaryIndexLookupRow, ProjectedQueryLocationRead)>> {
	let mut out = Vec::new();
	for row in rows {
		if let Some(result) =
			read_secondary_index_query_location(slab, row, descriptor, program, projected_execution)
				.await?
		{
			out.push((row.clone(), result));
		}
	}
	Ok(out)
}

fn execute_secondary_index_projected_row(
	mut projected: ProjectedRecordBatch,
	program: &pbc::Program,
	projected_execution: &QueryProjectedExecution,
) -> std::io::Result<Option<ProjectedQueryLocationRead>> {
	match projected_execution {
		QueryProjectedExecution::Pbc {
			requirement,
			user_column_count,
			runtime_filter,
		} => {
			if let Some(runtime_filter) = runtime_filter {
				projected = apply_runtime_filter_to_projected(projected, runtime_filter)?;
			}
			execute_pbc_projected_record_batch(&projected, program, requirement, *user_column_count)
				.map(|batch| {
					Some(ProjectedQueryLocationRead {
						batch,
						row_group_fetch_audit: RowGroupFetchAuditCounters::default(),
						encoding_materialize_audit: Vec::new(),
					})
				})
		}
		QueryProjectedExecution::Disabled | QueryProjectedExecution::Aggregate { .. } => Ok(None),
	}
}

async fn read_secondary_index_query_location(
	slab: &SlabManager,
	row: &SecondaryIndexLookupRow,
	descriptor: &TableSchemaDescriptor,
	program: &pbc::Program,
	projected_execution: &QueryProjectedExecution,
) -> std::io::Result<Option<ProjectedQueryLocationRead>> {
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
	let mut batch =
		materialize_record_location_batch(&storage_batch, row.record_location.record_row_index)?;
	if let Some(runtime_filter) = runtime_filter_for_projected_execution(projected_execution) {
		batch = apply_runtime_filter_to_batch(batch, runtime_filter)?;
	}
	match pbc::execute_program(program, &batch) {
		Ok(batch) => Ok(Some(ProjectedQueryLocationRead {
			batch,
			row_group_fetch_audit: RowGroupFetchAuditCounters::default(),
			encoding_materialize_audit: Vec::new(),
		})),
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
	descriptor: &TableSchemaDescriptor,
	program: &pbc::Program,
	projected_execution: &QueryProjectedExecution,
) -> std::io::Result<Option<ProjectedQueryLocationRead>> {
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
	let mut batch = materialize_record_location_batch(&storage_batch, location.record_row_index)?;
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
	if let Some(runtime_filter) = runtime_filter_for_projected_execution(projected_execution) {
		batch = apply_runtime_filter_to_batch(batch, runtime_filter)?;
	}
	match pbc::execute_program(program, &batch) {
		Ok(batch) => Ok(Some(ProjectedQueryLocationRead {
			batch,
			row_group_fetch_audit: RowGroupFetchAuditCounters::default(),
			encoding_materialize_audit: Vec::new(),
		})),
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
	row_limit: Option<usize>,
	table_descriptor: Option<&TableSchemaDescriptor>,
	projected_execution: QueryProjectedExecution,
	row_identity_projection: RowIdentityProjection,
	logical_retention_floor: Option<HybridTimestamp>,
) -> std::io::Result<QueryExecutionPlan> {
	plan_query_execution_for_ranges_with_program_maybe_cancelled(
		slab,
		local_node,
		meta,
		program,
		ranges,
		budget,
		row_limit,
		table_descriptor,
		projected_execution,
		row_identity_projection,
		logical_retention_floor,
		None,
		None,
	)
	.await
}

async fn plan_query_execution_for_ranges_with_program_maybe_cancelled(
	slab: &SlabManager,
	local_node: NodeId,
	meta: &QueryRequestMeta,
	program: &pbc::Program,
	ranges: &[QueryScanRange],
	budget: QueryExecutionBudget,
	row_limit: Option<usize>,
	table_descriptor: Option<&TableSchemaDescriptor>,
	projected_execution: QueryProjectedExecution,
	row_identity_projection: RowIdentityProjection,
	logical_retention_floor: Option<HybridTimestamp>,
	cancellation_token: Option<&CancellationToken>,
	mut stream_writer: Option<&mut TableQueryResponseStreamWriter<'_>>,
) -> std::io::Result<QueryExecutionPlan> {
	let budget = QueryExecutionBudget {
		stats_pruning_enabled: budget.stats_pruning_enabled
			&& query_request_stats_pruning_enabled(meta),
		..budget
	};
	metrics().observe_pbc_locality_arcs_scanned(ranges.len() as u64);
	let mut batches = Vec::new();
	// ADR 0088: rows coalesce here instead of one frame per location.
	let mut response_batches = TableQueryResponseAccumulator::default();
	let mut aggregate_responses = Vec::new();
	let mut rows = 0u64;
	let mut scanned_locations = 0usize;
	let mut work_locations_since_yield = 0usize;
	let mut work_rows_since_yield = 0u64;
	let mut clocks = QueryClocks::from_budget(&budget);
	let mut max_hlc = HybridTimestamp {
		physical_ms: 0,
		logical: 0,
		node_id: 0,
	};
	let mut row_group_fetch_audit = RowGroupFetchAuditCounters::default();
	let mut encoding_materialize_audit = Vec::new();
	let mut stats_prune_cache = QueryStatsPruneCache::default();
	let mut descriptorless_rebatch_groups: Vec<DescriptorlessRebatchGroup> = Vec::new();
	let max_rows_per_location = usize::try_from(budget.max_rows).unwrap_or(usize::MAX);
	if let Some(cancel) = cancellation_token {
		if cancel.is_cancelled() {
			return Ok(query_plan_error(
				meta,
				ReadStatus::QUERY_CANCELLED,
				local_node,
			));
		}
	}
	'ranges: for range in ranges {
		if row_limit_satisfied(rows, row_limit) {
			break;
		}
		if let Some(cancel) = cancellation_token {
			if cancel.is_cancelled() {
				return Ok(query_plan_error(
					meta,
					ReadStatus::QUERY_CANCELLED,
					local_node,
				));
			}
		}
		let scanned = slab.scan_as_of_with_identity_and_locality(
			meta.tenant_id,
			range.start,
			range.end,
			meta.as_of_hlc,
		);
		for (key_identity, locality, location) in scanned {
			// Examining a location is progress whatever its disposition — pruned,
			// filtered, duplicate, mismatched and zero-result locations are work
			// done, and a healthy large no-match scan must never read as a stall.
			clocks.progressed();
			// ADR 541: with server defaults the caps bound work per yield, not
			// totality. Cooperative yields keep long scans well-behaved on the
			// runtime without ever terminating them.
			work_locations_since_yield = work_locations_since_yield.saturating_add(1);
			if work_locations_since_yield >= budget.max_locations
				|| work_rows_since_yield >= budget.max_rows
			{
				tokio::task::yield_now().await;
				work_locations_since_yield = 0;
				work_rows_since_yield = 0;
			}
			if row_limit_satisfied(rows, row_limit) {
				break 'ranges;
			}
			if let Some(cancel) = cancellation_token {
				if cancel.is_cancelled() {
					return Ok(query_plan_error(
						meta,
						ReadStatus::QUERY_CANCELLED,
						local_node,
					));
				}
			}
			if logical_retention_floor.is_some_and(|floor| location.hlc < floor) {
				metrics().observe_storage_table_retention_read_filtered_rows(1);
				continue;
			}
			if let Some(expired) = clocks.expired() {
				return Ok(query_plan_error(meta, expired, local_node));
			}
			if budget.stats_pruning_enabled {
				let may_match = match clocks
					.bound(table_stats_may_match_location(
						slab,
						table_descriptor,
						program,
						&mut stats_prune_cache,
						location,
					))
					.await
				{
					Ok(may_match) => may_match?,
					Err(expired) => {
						return Ok(query_plan_error(meta, expired, local_node));
					}
				};
				if !may_match {
					continue;
				}
			}
			scanned_locations = scanned_locations.saturating_add(1);
			if budget.caller_totals && scanned_locations > budget.max_locations {
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
					request,
				} = &projected_execution
				{
					let aggregate_read = match clocks
						.bound(read_aggregate_query_location(
							slab,
							request,
							meta,
							location,
							key_identity,
							locality,
							descriptor,
							program,
							requirement,
							*user_column_count,
						))
						.await
					{
						Ok(read) => read?,
						Err(expired) => {
							return Ok(query_plan_error(meta, expired, local_node));
						}
					};
					if let Some(result) = aggregate_read {
						match result {
							AggregateQueryLocationRead::Dense(result) => {
								row_group_fetch_audit.merge(result.row_group_fetch_audit);
								encoding_materialize_audit
									.extend(result.encoding_materialize_audit);
								let result = result.batch;
								if result.num_rows() == 0 {
									continue;
								}
								let Some(result) = take_output_batch(result, &mut rows, row_limit)
								else {
									break 'ranges;
								};
								if location.hlc > max_hlc {
									max_hlc = location.hlc;
								}
								clocks.progressed();
								if budget.caller_totals && rows > budget.max_rows {
									return Ok(query_plan_error(
										meta,
										ReadStatus::QUERY_BUDGET_EXCEEDED,
										local_node,
									));
								}
								if let Some(expired) = clocks.expired() {
									return Ok(query_plan_error(meta, expired, local_node));
								}
								// ADR 0088: coalesced rather than emitted per location.
								response_batches
									.push(result, stream_writer.as_deref_mut(), &mut batches)
									.await?;
								if row_limit_satisfied(rows, row_limit) {
									break 'ranges;
								}
							}
							AggregateQueryLocationRead::Encoded {
								response,
								row_group_fetch_audit: audit,
							} => {
								row_group_fetch_audit.merge(audit);
								let result_rows = response.diagnostics.rows_admitted;
								rows = rows.saturating_add(result_rows);
								if response.max_hlc > max_hlc {
									max_hlc = response.max_hlc;
								}
								clocks.progressed();
								if budget.caller_totals && rows > budget.max_rows {
									return Ok(query_plan_error(
										meta,
										ReadStatus::QUERY_BUDGET_EXCEEDED,
										local_node,
									));
								}
								if let Some(expired) = clocks.expired() {
									return Ok(query_plan_error(meta, expired, local_node));
								}
								aggregate_responses.push(response);
							}
						}
					}
					continue;
				}
				let projected_read = match clocks
					.bound(read_projected_query_location(
						slab,
						location,
						descriptor,
						program,
						&projected_execution,
					))
					.await
				{
					Ok(read) => read?,
					Err(expired) => {
						return Ok(query_plan_error(meta, expired, local_node));
					}
				};
				if let Some(result) = projected_read {
					if let Some(cancel) = cancellation_token {
						if cancel.is_cancelled() {
							return Ok(query_plan_error(
								meta,
								ReadStatus::QUERY_CANCELLED,
								local_node,
							));
						}
					}
					row_group_fetch_audit.merge(result.row_group_fetch_audit);
					encoding_materialize_audit.extend(result.encoding_materialize_audit);
					let result = result.batch;
					if result.num_rows() == 0 {
						continue;
					}
					let Some(result) = take_output_batch(result, &mut rows, row_limit) else {
						break 'ranges;
					};
					if location.hlc > max_hlc {
						max_hlc = location.hlc;
					}
					clocks.progressed();
					if budget.caller_totals && rows > budget.max_rows {
						return Ok(query_plan_error(
							meta,
							ReadStatus::QUERY_BUDGET_EXCEEDED,
							local_node,
						));
					}
					if let Some(expired) = clocks.expired() {
						return Ok(query_plan_error(meta, expired, local_node));
					}
					// ADR 0088: coalesced rather than emitted per location.
					response_batches
						.push(result, stream_writer.as_deref_mut(), &mut batches)
						.await?;
					if row_limit_satisfied(rows, row_limit) {
						break 'ranges;
					}
					continue;
				}
			}
			if let Some(cancel) = cancellation_token {
				if cancel.is_cancelled() {
					return Ok(query_plan_error(
						meta,
						ReadStatus::QUERY_CANCELLED,
						local_node,
					));
				}
			}
			let bytes = match clocks.bound(slab.read(location)).await {
				Ok(bytes) => bytes?,
				Err(expired) => return Ok(query_plan_error(meta, expired, local_node)),
			};
			let decode_started = Instant::now();
			let storage_batch = decode_batch(&bytes)?;
			let storage_row_count = storage_batch.num_rows();
			let was_multirow = has_storage_synthetic_columns(&storage_batch);
			let stored_table_object_id = if was_multirow {
				storage_row_table_object_id_at(&storage_batch, location.record_row_index)?
			} else {
				None
			};
			let mut batch =
				materialize_record_location_batch(&storage_batch, location.record_row_index)?;
			let batch_row_key_ids = if row_identity_projection == RowIdentityProjection::Append {
				vec![key_identity; batch.num_rows()]
			} else {
				Vec::new()
			};
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
			// An oversize location terminates only a caller-capped query; with server
			// defaults it is one large unit of bounded work, not a failure.
			if budget.caller_totals && batch.num_rows() > max_rows_per_location {
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
			if let Some(runtime_filter) =
				runtime_filter_for_projected_execution(&projected_execution)
			{
				batch = apply_runtime_filter_to_batch(batch, runtime_filter)?;
				if batch.num_rows() == 0 {
					continue;
				}
			}
			if table_descriptor.is_none() {
				// ADR 120 D2(c): descriptor-backed typed-table queries
				// take the per-location path below. Descriptorless legacy
				// queries keep the pre-ADR-120 schema-grouping behavior
				// because there is no table descriptor to assert against.
				if let Some(group) = descriptorless_rebatch_groups
					.iter_mut()
					.find(|group| group.schema.as_ref() == batch.schema().as_ref())
				{
					if location.hlc > group.max_hlc {
						group.max_hlc = location.hlc;
					}
					group.batches.push(batch);
					group.row_key_ids.extend(batch_row_key_ids);
				} else {
					descriptorless_rebatch_groups.push(DescriptorlessRebatchGroup {
						schema: batch.schema(),
						batches: vec![batch],
						row_key_ids: batch_row_key_ids,
						max_hlc: location.hlc,
					});
				}
				// A location appended into a rebatch group is a unit of progress; a
				// long descriptorless scan must never stall while it is working.
				clocks.progressed();
				continue;
			}

			// ADR 120 descriptor-backed whole-record path: after
			// `table_record_matches_descriptor` has asserted the table
			// descriptor schema, execute and emit each location before
			// the next storage read so bounded consumers bound storage
			// work.
			let mut row_key_ids = Vec::new();
			if row_identity_projection == RowIdentityProjection::Append {
				let mask = match pbc::survival_mask(program, &batch) {
					Ok(mask) => mask,
					Err(_) => {
						return Ok(query_plan_error(
							meta,
							ReadStatus::QUERY_INVALID_PROGRAM,
							local_node,
						));
					}
				};
				row_key_ids = filter_row_key_ids_by_mask(&batch_row_key_ids, &mask);
			}
			let mut result = match pbc::execute_program(program, &batch) {
				Ok(result) => result,
				Err(_) => {
					return Ok(query_plan_error(
						meta,
						ReadStatus::QUERY_INVALID_PROGRAM,
						local_node,
					));
				}
			};
			if row_identity_projection == RowIdentityProjection::Append {
				result = append_query_row_identity_column(result, &row_key_ids)?;
			}
			if let Some(cancel) = cancellation_token {
				if cancel.is_cancelled() {
					return Ok(query_plan_error(
						meta,
						ReadStatus::QUERY_CANCELLED,
						local_node,
					));
				}
			}
			if result.num_rows() == 0 {
				continue;
			}
			if location.hlc > max_hlc {
				max_hlc = location.hlc;
			}
			let Some(result) = take_output_batch(result, &mut rows, row_limit) else {
				break 'ranges;
			};
			work_rows_since_yield = work_rows_since_yield.saturating_add(result.num_rows() as u64);
			if budget.caller_totals && rows > budget.max_rows {
				return Ok(query_plan_error(
					meta,
					ReadStatus::QUERY_BUDGET_EXCEEDED,
					local_node,
				));
			}
			if let Some(expired) = clocks.expired() {
				return Ok(query_plan_error(meta, expired, local_node));
			}
			// ADR 0088: coalesced rather than emitted per location.
			response_batches
				.push(result, stream_writer.as_deref_mut(), &mut batches)
				.await?;
			if row_limit_satisfied(rows, row_limit) {
				break 'ranges;
			}
		}
	}

	for group in descriptorless_rebatch_groups {
		if row_limit_satisfied(rows, row_limit) {
			break;
		}
		if let Some(cancel) = cancellation_token {
			if cancel.is_cancelled() {
				return Ok(query_plan_error(
					meta,
					ReadStatus::QUERY_CANCELLED,
					local_node,
				));
			}
		}
		let concatenated = concat_batches(&group.schema, &group.batches)
			.map_err(|e| Error::new(ErrorKind::InvalidData, format!("PBC concat: {e}")))?;
		let mut row_key_ids = Vec::new();
		if row_identity_projection == RowIdentityProjection::Append {
			let mask = match pbc::survival_mask(program, &concatenated) {
				Ok(mask) => mask,
				Err(_) => {
					return Ok(query_plan_error(
						meta,
						ReadStatus::QUERY_INVALID_PROGRAM,
						local_node,
					));
				}
			};
			row_key_ids = filter_row_key_ids_by_mask(&group.row_key_ids, &mask);
		}
		let mut result = match pbc::execute_program(program, &concatenated) {
			Ok(result) => result,
			Err(_) => {
				return Ok(query_plan_error(
					meta,
					ReadStatus::QUERY_INVALID_PROGRAM,
					local_node,
				));
			}
		};
		if row_identity_projection == RowIdentityProjection::Append {
			result = append_query_row_identity_column(result, &row_key_ids)?;
		}
		if let Some(cancel) = cancellation_token {
			if cancel.is_cancelled() {
				return Ok(query_plan_error(
					meta,
					ReadStatus::QUERY_CANCELLED,
					local_node,
				));
			}
		}
		if result.num_rows() == 0 {
			continue;
		}
		if group.max_hlc > max_hlc {
			max_hlc = group.max_hlc;
		}
		let Some(result) = take_output_batch(result, &mut rows, row_limit) else {
			break;
		};
		clocks.progressed();
		if budget.caller_totals && rows > budget.max_rows {
			return Ok(query_plan_error(
				meta,
				ReadStatus::QUERY_BUDGET_EXCEEDED,
				local_node,
			));
		}
		if let Some(expired) = clocks.expired() {
			return Ok(query_plan_error(meta, expired, local_node));
		}
		// ADR 0088: coalesced rather than emitted per location.
		response_batches
			.push(result, stream_writer.as_deref_mut(), &mut batches)
			.await?;
	}

	// 4. Empty result is a legitimate answer. Clients map
	// KEY_NOT_FOUND to an empty batch on the receive side, so we
	// avoid the cost of encoding a zero-row stream.
	if rows == 0 && batches.is_empty() && aggregate_responses.is_empty() {
		let mut plan = query_plan_error(meta, ReadStatus::KEY_NOT_FOUND, local_node);
		// An empty result is a legitimate answer produced by real work: the live
		// clocks (and their progress marks) belong to it like any other exit.
		plan.clocks = clocks;
		plan.stats_explain = stats_prune_cache.explain;
		plan.row_group_fetch_audit = row_group_fetch_audit;
		plan.encoding_materialize_audit = encoding_materialize_audit;
		return Ok(plan);
	}

	// ADR 0088: the accumulator's tail must reach the caller — a bounded
	// read that stops short of the row target still owes its rows.
	response_batches
		.flush(stream_writer.as_deref_mut(), &mut batches)
		.await?;
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
		aggregate_responses,
		clocks,
		stats_explain: stats_prune_cache.explain,
		secondary_index_access_plan: None,
		row_group_fetch_audit,
		encoding_materialize_audit,
	})
}

fn table_record_matches_descriptor(
	meta: &QueryRequestMeta,
	descriptor: &TableSchemaDescriptor,
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
		aggregate_responses: Vec::new(),
		clocks: QueryClocks::new(Duration::from_millis(250), None),
		stats_explain: QueryStatsExplainCounters::default(),
		secondary_index_access_plan: None,
		row_group_fetch_audit: RowGroupFetchAuditCounters::default(),
		encoding_materialize_audit: Vec::new(),
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

fn query_ok_provisional_meta(meta: &QueryRequestMeta, leader_hint: NodeId) -> QueryResponseMeta {
	QueryResponseMeta {
		status: ReadStatus::OK,
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
	let mut pad = [0; 7];
	pad[..meta._pad.len()].copy_from_slice(&meta._pad);
	QueryRequestMeta {
		min_hlc: meta.min_hlc,
		expected_epoch: meta.expected_epoch,
		tenant_id: meta.tenant_id,
		consistency: meta.consistency,
		_pad: pad,
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
	descriptor: &TableSchemaDescriptor,
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
	// Aggregate budgets are caller-transmitted, so their totals genuinely cap the
	// query (ADR 541: only a caller budget may impose a total cap), and the caller's
	// wall budget is an absolute deadline — not a clamp on the stall bound.
	let max_locations = usize::try_from(budgets.max_locations.get()).unwrap_or(usize::MAX);
	let max_rows = budgets.max_rows.get();
	let caller_deadline = caller_deadline_from_budget_millis(budgets.max_wall_time_millis.get());
	QueryExecutionBudget {
		max_locations,
		max_rows,
		max_wall_time: default.max_wall_time,
		stats_pruning_enabled: true,
		caller_totals: true,
		caller_deadline,
	}
}

fn aggregate_reject_from_read_status(status: ReadStatus) -> AggRejectCode {
	match status {
		ReadStatus::QUERY_INVALID_PROGRAM => AggRejectCode::ProgramInvalid,
		ReadStatus::QUERY_BUDGET_EXCEEDED | ReadStatus::BACKPRESSURED => {
			AggRejectCode::BudgetExceeded
		}
		// ADR 541: expiry causes stay attributable through the aggregate protocol —
		// collapsing them into BudgetExceeded is the same defect one seam over.
		ReadStatus::QUERY_DEADLINE_EXCEEDED => AggRejectCode::QueryDeadlineExceeded,
		ReadStatus::QUERY_STALLED => AggRejectCode::Stalled,
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
		AggRejectCode::Stalled => ReadStatus::QUERY_STALLED,
		AggRejectCode::QueryDeadlineExceeded => ReadStatus::QUERY_DEADLINE_EXCEEDED,
		AggRejectCode::RetentionExpired => ReadStatus::RETENTION_EXPIRED,
	}
}

fn table_query_scan_ranges(
	tenant_id: TenantId,
	descriptor: &TableSchemaDescriptor,
	program: Option<&pbc::Program>,
) -> Result<Vec<QueryScanRange>, ReadStatus> {
	match descriptor.locality_policy {
		LocalityPolicy::TableSpreadProjection(spread) => {
			let ranges = table_spread_prefix_ranges(tenant_id, descriptor.table_object_id, spread)
				.map_err(|_| ReadStatus::QUERY_INVALID_PROGRAM)?;
			Ok(ranges
				.into_iter()
				.map(|(start, end)| QueryScanRange { start, end })
				.collect())
		}
		LocalityPolicy::IdentityProjection => Ok(vec![QueryScanRange {
			start: Locality::ZERO,
			end: Locality::MAX,
		}]),
		LocalityPolicy::ColumnProjection(ref projection) => {
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
		deadline_remaining_millis: meta.deadline_remaining_millis,
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
	.with_mutation_outcome(MutationCommitKnowledge::COMMITTED, MutationStopReason::NONE)
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
	.with_mutation_outcome(MutationCommitKnowledge::COMMITTED, MutationStopReason::NONE)
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
	descriptor: &TableSchemaDescriptor,
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
	descriptor: &TableSchemaDescriptor,
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
	descriptor: &TableSchemaDescriptor,
	writer_node_id: WriterNodeId,
	row_hlc: HybridTimestamp,
) -> Result<BifurcatedTableWrite, TableWriteValidationError> {
	bifurcate_table_write_with_row_group_config(
		meta,
		batch,
		descriptor,
		writer_node_id,
		row_hlc,
		&RowGroupWriterConfig::default(),
	)
}

fn bifurcate_table_write_with_row_group_config(
	meta: &TableWriteRequestMeta,
	batch: &RecordBatch,
	descriptor: &TableSchemaDescriptor,
	writer_node_id: WriterNodeId,
	row_hlc: HybridTimestamp,
	row_group_config: &RowGroupWriterConfig,
) -> Result<BifurcatedTableWrite, TableWriteValidationError> {
	bifurcate_table_write_with_row_group_encoding_and_chunk_layout_config(
		meta,
		batch,
		descriptor,
		writer_node_id,
		row_hlc,
		row_group_config,
		&EncodingWriterConfig::default(),
		&L2ChunkLayoutConfig::default(),
	)
}

fn bifurcate_table_write_with_row_group_encoding_and_chunk_layout_config(
	meta: &TableWriteRequestMeta,
	batch: &RecordBatch,
	descriptor: &TableSchemaDescriptor,
	writer_node_id: WriterNodeId,
	row_hlc: HybridTimestamp,
	row_group_config: &RowGroupWriterConfig,
	encoding_config: &EncodingWriterConfig,
	chunk_layout_config: &L2ChunkLayoutConfig,
) -> Result<BifurcatedTableWrite, TableWriteValidationError> {
	let observe_encode = matches!(
		&descriptor.locality_policy,
		LocalityPolicy::ColumnProjection(_)
	);
	let encode_started = Instant::now();
	let admission =
		validate_table_write_request_batch(meta, batch, descriptor, writer_node_id, row_hlc);
	if observe_encode {
		metrics().observe_pbc_locality_encode(encode_started.elapsed());
	}
	let admission = admission?;
	bifurcate_table_write_with_admission_and_row_group_encoding_and_chunk_layout_config(
		meta,
		batch,
		descriptor,
		&admission,
		row_hlc,
		row_group_config,
		encoding_config,
		chunk_layout_config,
	)
}

#[allow(dead_code)]
fn bifurcate_table_write_with_admission(
	meta: &TableWriteRequestMeta,
	batch: &RecordBatch,
	descriptor: &TableSchemaDescriptor,
	admission: &TableWriteAdmission,
	row_hlc: HybridTimestamp,
) -> Result<BifurcatedTableWrite, TableWriteValidationError> {
	bifurcate_table_write_with_admission_and_row_group_config(
		meta,
		batch,
		descriptor,
		admission,
		row_hlc,
		&RowGroupWriterConfig::default(),
	)
}

fn bifurcate_table_write_with_admission_and_row_group_config(
	meta: &TableWriteRequestMeta,
	batch: &RecordBatch,
	descriptor: &TableSchemaDescriptor,
	admission: &TableWriteAdmission,
	row_hlc: HybridTimestamp,
	row_group_config: &RowGroupWriterConfig,
) -> Result<BifurcatedTableWrite, TableWriteValidationError> {
	bifurcate_table_write_with_admission_and_row_group_encoding_and_chunk_layout_config(
		meta,
		batch,
		descriptor,
		admission,
		row_hlc,
		row_group_config,
		&EncodingWriterConfig::default(),
		&L2ChunkLayoutConfig::default(),
	)
}

#[cfg(test)]
fn bifurcate_table_write_with_admission_and_row_group_and_encoding_config(
	meta: &TableWriteRequestMeta,
	batch: &RecordBatch,
	descriptor: &TableSchemaDescriptor,
	admission: &TableWriteAdmission,
	row_hlc: HybridTimestamp,
	row_group_config: &RowGroupWriterConfig,
	encoding_config: &EncodingWriterConfig,
) -> Result<BifurcatedTableWrite, TableWriteValidationError> {
	bifurcate_table_write_with_admission_and_row_group_encoding_and_chunk_layout_config(
		meta,
		batch,
		descriptor,
		admission,
		row_hlc,
		row_group_config,
		encoding_config,
		&L2ChunkLayoutConfig::default(),
	)
}

fn bifurcate_table_write_with_admission_and_row_group_encoding_and_chunk_layout_config(
	meta: &TableWriteRequestMeta,
	batch: &RecordBatch,
	descriptor: &TableSchemaDescriptor,
	admission: &TableWriteAdmission,
	row_hlc: HybridTimestamp,
	row_group_config: &RowGroupWriterConfig,
	encoding_config: &EncodingWriterConfig,
	chunk_layout_config: &L2ChunkLayoutConfig,
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
				pack_admitted_record_batch_by_bucket_with_row_group_encoding_and_chunk_layout_config(
				meta,
				descriptor,
				batch,
				admission,
				row_hlc,
				row_group_config,
				encoding_config,
				chunk_layout_config,
			)?;
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
mod tests;
