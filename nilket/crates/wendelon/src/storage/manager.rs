//! Multi-slab storage front-end with size-triggered rotation.
//!
//! A single [`LocalSlabStore`] serves exactly one slab file, but
//! the system as a whole rotates through many — once an active
//! slab reaches its size cap it is sealed (read-only, eligible for
//! tiered migration) and a fresh slab becomes active. [`SlabManager`]
//! owns that lifecycle and exposes the composite as a single
//! [`StorageEngine`].
//!
//! ## Rotation protocol
//!
//! Appends check `active.write_cursor()` under a brief async lock.
//! When the cursor has crossed the configured cap, the locked
//! section atomically:
//!
//! 1. Moves the current active slab into the `sealed` map keyed by
//!    its `slab_id`.
//! 2. Opens a new slab file at `dir/slab_<id+1>.wnd`.
//! 3. Installs it as the new active slab and bumps `next_slab_id`.
//!
//! The actual disk append then runs **outside** the lock on the
//! `Arc<LocalSlabStore>` clone captured during the locked section.
//! Only the rotation decision is serialised; ordinary appends
//! remain lock-free at the slab level.
//!
//! ## Index model
//!
//! `SlabManager` owns the authoritative cross-slab `key_index`.
//! Per-slab indices still exist and get populated during recovery,
//! but the manager is the one layer callers talk to for
//! [`StorageEngine::lookup`]. On `open` / rotation, each slab's
//! recovered entries are drained into `key_index` once so there is
//! a single source of truth at steady state.
//!
//! ## Deferred
//!
//! * Background sealing: today sealing happens inline on the
//!   append path. Under sustained write pressure a dedicated
//!   rotation task would smooth the tail latency spike.
//! * S3 tier handoff: the `sealed` map is as far as records travel
//!   for now; remote upload is ADR 006's problem.

use crate::consensus::MigratedRangeTombstone;
use crate::crypto::{
	CryptoManager, DecryptedTieredSlab, EncryptedTieredSlab, FooterSummary,
	SLAB_ENCRYPTION_DEFAULT_CHUNK_SIZE, TieredColumnStatsBlock, TieredMultiRangeReadPlan,
	TieredOffsetIndexEntry, TieredPlaintextRange, TieredSlabObjectMetadata,
};
use crate::metrics::{LsmCompactionResult, LsmDigestInvariantKind, global as metrics};
use crate::schema::table::{
	RecordBodyCompressionPolicy, RecordLayoutPolicy, RowIdentityPolicy, TableObjectId,
	TableSchemaCatalogV1, TableSchemaDescriptorV1, TableWriteRequestMeta,
	validate_table_write_request_batch,
};
use crate::schema::{
	AggregatePartial, AggregateRequest, BufferValueCount, ByteCount, CompactionJobId,
	CompactionLevelIndex, EncodingPageByteOffset, HybridTimestamp,
	INDEX_REJECT_LOGICAL_FINGERPRINT_MISMATCH, Identity, IdentityPrefix, IndexObjectId,
	IpcBufferIndex, IpcColumnOrdinal, Locality, LogicalSchemaFingerprint, ManifestEpoch,
	RecordBodyByteLen, RecordBodyByteOffset, RecordRowIndex, RowCount, RowGroupIndex,
	SecondaryIndexCatalog, SecondaryIndexDescriptor, SecondaryIndexState,
	SelectivityFractionMicros, SimdPageValueCount, SlabId, SlabRecordHeader, SupersessionMetadata,
	TableLifecycleCatalog, TenantId, TenantKekId, TombstoneMetadata, WriterNodeId,
};
use crate::storage::columnar::{
	ColumnFetchRequirement, ColumnOrdinalMapping, ColumnarFallbackReason, L2ColumnFetchConfig,
	L2FetchStrategy, PlaintextByteRange, ProjectedRecordBatch, RecordBufferDirectory,
	StorageColumn, TieredRecordId, build_column_fetch_plan, decode_projected_record_batch,
	decode_projected_record_batch_from_plaintext_ranges, initial_metadata_probe_plan,
	next_metadata_probe_plan, observe_l2_fetch_strategy, parse_record_buffer_directory,
	pick_l2_fetch_strategy, selected_record_body_ranges, survival_mask_projected_record_batch,
};
use crate::storage::core::{
	GroupCommitConfig, LocalSlabStore, MultiRowIndexEntry, RecordLocation, RecoveredSlabEntry,
	StorageEngine, insert_version_sorted, newest_version_as_of, recover_slab_entries_from_blob,
	remove_version_exact, storage_multirow_index_entries_from_frame, validate_row_tombstone_chain,
};
use crate::storage::encoding::compute::{
	EncodedAggregateContext, EncodedPredicateNode, EncodedRowMask, EncodingComputeFallbackReason,
	EncodingComputeRejectCode, NativeEvaluationEligibility, PredicateAtom, aggregate_kind_for_tag,
	coerce_literal, decode_encoded_buffer, encoded_leaf_selectivity_allowed,
	encoded_predicate_node_from_tree, evaluate as evaluate_encoded_predicate,
	evaluate_validity_bitmap, evaluator_kind_for_cascade, native_aggregate_cascade_eligible,
	native_evaluation_eligibility, try_fold_encoded_aggregate_request,
};
use crate::storage::encoding::directory::page_stats_cascade_eligible;
use crate::storage::encoding::materialize::find_buffer_encoding;
use crate::storage::encoding::materialize::ipc_stream_contains_encoding_directory;
use crate::storage::encoding::page::expand_page_descriptors;
use crate::storage::encoding::page_materialize::{
	PageMaterializeBufferKey, PageMaterializeColumn, PageMaterializeFetch, PageMaterializeKey,
	PageValidityBitmap, decode_page_fetch, empty_projected_batch_for_requirement,
	encoded_page_body_range, extract_record_body_bytes as extract_encoded_page_bytes,
	fixed_width_encoding_indexed_value_column, projected_page_batch_from_decoded_buffers,
	rebase_validity_bitmap, required_projected_physical_ordinals,
};
use crate::storage::encoding::predicate_page_fetch::{
	PagePredicateClassification, PredicatePageFetchPlan, PredicatePageMaskSlice,
	classify_page_stats_for_atom, stitch_predicate_page_mask,
};
use crate::storage::encoding::sparse_projection_fetch::{
	SparseProjectionFetchDecision, SparseProjectionFetchInput, SparseProjectionFetchPlan,
	decide_sparse_projection_fetch,
};
use crate::storage::encoding::{
	BufferEncoding, ENCODING_DIRECTORY_METADATA_KEY, EncodedBufferByteRange, EncodingCascade,
	EncodingComputeConfig, EncodingDirectory, EncodingFallbackReason, EncodingRejectCode,
	EncodingWriterConfig, L2ChunkLayoutConfig, L2EncodingFetchConfig,
	choose_chunk_size_bytes_for_directories,
};
use crate::storage::index::{
	PersistentIndexUpdate, PersistentLocalIndex, PersistentRecordRow,
	PersistentSecondaryIndexSegmentKey, PersistentSecondaryIndexSegmentRow, persistent_index_path,
};
use crate::storage::io_uring::IoRingHandle;
use crate::storage::ipc::record_batch_compression_policy_from_ipc;
use crate::storage::l2::ObjectStore;
use crate::storage::multirow::{
	MultiRowSourceRow, StorageRowMetadata, decode_batch as decode_record_batch,
	encode_batch_with_policy, encode_multirow_from_survivors_with_policy,
	encode_multirow_from_survivors_with_row_group_encoding_and_chunk_layout_config,
	has_storage_synthetic_columns, materialize_record_location_batch,
	pack_multirow_source_rows_with_policy_row_group_encoding_and_chunk_layout_config,
};
use crate::storage::secondary_index::{
	SecondaryIndexCandidateRejectReason, SecondaryIndexCandidateValidation, SecondaryIndexEntry,
	SecondaryIndexIdentity, SecondaryIndexLookupReport, SecondaryIndexLookupRow,
	TableLookupByIndexRequest, encode_secondary_index_key, revalidate_secondary_index_candidate,
	secondary_index_entry_matches_query, secondary_index_key_range_intersects_query,
};
use crate::storage::secondary_index_backfill::{
	SecondaryIndexActiveBuffer, SecondaryIndexBackfillCandidate, SecondaryIndexBackfillError,
	SecondaryIndexBackfillErrorReason, SecondaryIndexBackfillFailure, SecondaryIndexBackfillPlan,
	SecondaryIndexBackfillSuccess, SecondaryIndexBackfillTickReport, SecondaryIndexCoverageKey,
	SecondaryIndexCoverageSnapshot, SecondaryIndexCoverageTracker,
};
use crate::storage::secondary_index_segment::{
	SecondaryIndexSegment, secondary_index_segment_storage_path,
};
use crate::storage::stats::{
	ColumnStatsDescriptorIdentity, ParsedColumnStatsBlock,
	build_column_stats_block_with_ciphertext_budget, locator_flags_for_parsed_column_stats_block,
	max_column_stats_ciphertext_bytes_for_slab_payload, parse_column_stats_block,
};
use crate::storage::{
	AsyncFileOps, BlockingFileOps, BlockingIoClass, CompactionCommit, CompactionDigestSummary,
	CompactionFailure, CompactionManifest, CompactionPlan, IoUringFileOps, LsmCompactionDriver,
	LsmCompactionJobRunner, LsmCompactionWorker, LsmGcDriver, LsmGcWorker, LsmPreparedOutputs,
	LsmRunSummary, LsmSourceFrame, LsmWrittenOutputRun, ManifestSnapshot, MvccRetentionPolicy,
	OpenFlags, SlabPinGuard, SlabPinTable, TierCommitter, VersionEntry, VersionLifecycle,
	VisibleDomainRow, apply_elision, cluster_retention_floor, compaction_visible_domain_summary,
	effective_table_retention, pick_compaction, required_output_run_count,
	storage_row_table_object_id_at, survivor_records, write_file,
};
use arrow::array::Array;
use arrow::compute::concat_batches;
use arrow::datatypes::DataType;
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use bytes::Bytes;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::io::{Cursor, Error, ErrorKind};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::sync::{Mutex, OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock as AsyncRwLock};
use tokio::task::JoinSet;
use tokio::time::Instant;
use zeroize::Zeroizing;

const STORAGE_BACKUP_FORMAT_VERSION: u16 = 1;

type TenantKey = (TenantId, Identity);
type TenantLocalityMembers = BTreeMap<TenantId, BTreeSet<(Locality, Identity)>>;

const DEFAULT_L2_TAIL_READ_WINDOW_BYTES: u64 = 64 * 1024;
const MIN_L2_TAIL_READ_WINDOW_BYTES: u64 = 256 + 16;
const IDENTITY_MAX: Identity = Identity::new([0xFF; 32]);

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

/// Construction parameters for [`SlabManager`].
pub struct SlabManagerConfig {
	/// Directory that will hold `slab_<id>.wnd` files.
	pub dir: PathBuf,
	/// Size threshold at which the active slab is sealed and a new
	/// one is opened. Measured against [`LocalSlabStore::write_cursor`].
	pub max_slab_bytes: u64,
	/// Submission handle for the dedicated slab-storage `io_uring`
	/// worker. Cloned into every slab the manager opens.
	pub io: IoRingHandle,
	/// Optional L2 object store used by the read path when a slab
	/// has been tiered out of L1. If `None`, the manager refuses
	/// reads against tiered slabs rather than silently returning
	/// `NotFound` — a missing L2 handle on a tiered slab is a
	/// configuration bug, not a miss.
	pub l2: Option<Arc<dyn ObjectStore>>,
	/// Policy for promoting hot tiered slabs back into local L1
	/// storage.
	pub rehydration: RehydrationPolicy,
	/// Node-local ADR 044 L2 columnar fetch strategy config.
	pub l2_column_fetch: L2ColumnFetchConfig,
	/// Deployment-wide ADR 056 row-group writer config.
	pub row_group_writer: crate::storage::row_group::RowGroupWriterConfig,
	/// Node-local ADR 056 L2 row-group fetch strategy config.
	pub l2_row_group_fetch: crate::storage::row_group::L2RowGroupFetchConfig,
	/// Deployment-wide ADR 057 Encoding writer config.
	pub encoding_writer: EncodingWriterConfig,
	/// Deployment-wide ADR 061 authenticated chunk layout writer config.
	pub l2_chunk_layout: L2ChunkLayoutConfig,
	/// Node-local ADR 057 L2 Encoding materialization config.
	pub l2_encoding_fetch: L2EncodingFetchConfig,
	/// Node-local ADR 058 compute-over-encoded-data executor config.
	pub encoding_compute: EncodingComputeConfig,
	/// ADR 016 data-path group-commit tuning.
	pub group_commit: GroupCommitConfig,
	/// ADR 022 slab-object encryption service used for L2 object bytes.
	pub crypto: Arc<CryptoManager>,
}

/// Policy that governs L2 -> L1 slab rehydration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RehydrationPolicy {
	/// Number of L2-served reads a slab must accumulate before the
	/// manager restores a local copy. `0` disables rehydration.
	pub promote_after_l2_reads: u32,
	/// Minimum amount of time a rehydrated local copy must remain
	/// resident before the sweeper may reclaim it again.
	pub min_local_residency: Duration,
	/// Tail window fetched before a cold L2 point read plans the
	/// ciphertext span. Must be large enough to contain the ADR 022
	/// fixed footer plus tail anchor; larger values reduce follow-up
	/// index range GETs for dense slabs.
	pub tail_read_window_bytes: u64,
}

impl Default for RehydrationPolicy {
	fn default() -> Self {
		Self {
			promote_after_l2_reads: 3,
			min_local_residency: Duration::from_secs(60),
			tail_read_window_bytes: DEFAULT_L2_TAIL_READ_WINDOW_BYTES,
		}
	}
}

impl RehydrationPolicy {
	pub fn validate(&self) -> std::io::Result<()> {
		if self.tail_read_window_bytes < MIN_L2_TAIL_READ_WINDOW_BYTES {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				format!(
					"rehydration.tail_read_window_bytes must be at least {MIN_L2_TAIL_READ_WINDOW_BYTES} bytes"
				),
			));
		}
		Ok(())
	}
}

/// Summary of a sealed slab, handed out by
/// [`SlabManager::list_sealed_slabs`] to the background sweeper.
/// `size_bytes` is the current write cursor — slabs only ever
/// shrink by being tiered out, so this is the upper bound on the
/// payload the sweeper will upload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SealedSlabInfo {
	pub slab_id: SlabId,
	pub size_bytes: u64,
}

/// Summary of a locally rehydrated slab. The authoritative tier
/// metadata still points at L2; this struct only describes the
/// temporary L1 cache copy and the cooldown that protects it from
/// immediate reclamation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RehydratedSlabInfo {
	pub slab_id: SlabId,
	pub size_bytes: u64,
	pub keep_local_until: Instant,
}

/// Outcome of a single [`SlabManager::append_migration`] call.
/// Lets the caller distinguish genuinely new ingests from
/// idempotent no-ops so it can keep honest progress counters
/// across racing sweeper and hot-read pulls.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MigrationIngestOutcome {
	/// A new retained version was appended and indexed.
	Inserted(RecordLocation),
	/// A matching `(key_identity, hlc)` with identical
	/// `payload_hash`
	/// already existed locally — the ingest was a no-op.
	Duplicate(RecordLocation),
}

/// A slab that has been uploaded to L2. The manager tracks this
/// so subsequent reads pivot to the object store instead of the
/// now-unlinked L1 file.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TieredSlabPointer {
	pub slab_id: SlabId,
	pub l2_uri: String,
	pub payload_hash: [u8; 32],
	/// `true` when the L2 object at `l2_uri` already carries an
	/// encrypted tiered-slab footer. Lets the plaintext-rewrite
	/// sweeper short-circuit without GET-ing the object on every pass
	/// once steady state is reached. Trailing field for postcard
	/// upgrade compat: older pointers default to `false`, which is the
	/// safe (and accurate) classification — the sweeper will GET them
	/// once and stamp the truth back through `commit_tier_slab`.
	#[serde(default)]
	pub is_encrypted: bool,
	/// Tenant that owns the slab payload when known.
	#[serde(default)]
	pub tenant_id: Option<TenantId>,
	/// Tenant KEK version referenced by the encrypted footer when known.
	#[serde(default)]
	pub tenant_kek_id: Option<TenantKekId>,
	/// ADR 039 advisory fields copied from the footer locator when
	/// known. The slab tail is still the source of truth.
	#[serde(default)]
	pub stats_format_version: Option<u16>,
	#[serde(default)]
	pub stats_block_len: Option<u32>,
	#[serde(default)]
	pub stats_block_hash128: Option<[u8; 16]>,
	/// ADR 040 visibility proof for CSB aggregate answers.
	/// `Untracked` is conservative and cannot short-circuit.
	#[serde(default)]
	pub tombstone_metadata: TombstoneMetadata,
	/// ADR 042 visibility proof for CSB aggregate answers.
	/// `Untracked` is conservative and cannot short-circuit.
	#[serde(default)]
	pub supersession_metadata: SupersessionMetadata,
}

impl TieredSlabPointer {
	pub fn record_tombstone_hlc(&mut self, tombstone_hlc: HybridTimestamp) {
		self.tombstone_metadata = crate::storage::aggregate::merge_earliest_tombstone(
			self.tombstone_metadata,
			tombstone_hlc,
		);
	}

	pub fn record_supersession_hlc(&mut self, supersession_hlc: HybridTimestamp) {
		self.supersession_metadata = crate::storage::aggregate::merge_earliest_supersession(
			self.supersession_metadata,
			supersession_hlc,
		);
	}
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalSlabBackup {
	pub slab_id: SlabId,
	#[serde(with = "serde_bytes")]
	pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageBackupManifest {
	pub format_version: u16,
	pub active_slab_id: SlabId,
	pub next_slab_id: SlabId,
	pub sealed_slab_ids: Vec<SlabId>,
	pub local_slab_bytes: u64,
	pub tiered_slabs: Vec<TieredSlabPointer>,
	pub omitted_rehydrated_slabs: Vec<SlabId>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageBackupImage {
	pub manifest: StorageBackupManifest,
	pub local_slabs: Vec<LocalSlabBackup>,
}

/// Outcome of one tombstone-aware reclaim pass.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TombstoneCompactionReport {
	pub compacted_slabs: usize,
	pub reclaimed_bytes: u64,
}

/// Outcome of one ADR 013 retention-pruning pass.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RetentionPruningReport {
	pub compacted_slabs: usize,
	pub pruned_versions: usize,
	pub pruned_data_rows: usize,
	pub pruned_tombstone_rows: usize,
	pub pair_drops: usize,
	pub ambiguous_rows_retained: usize,
	pub max_lag_seconds: u64,
	pub reclaimed_bytes: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct RetentionCompactionStats {
	pub pruned_data_rows: usize,
	pub pruned_tombstone_rows: usize,
	pub pair_drops: usize,
	pub ambiguous_rows_retained: usize,
	pub max_lag_seconds: u64,
}

/// Outcome of one explicit table layout recluster pass.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LayoutReclusterReport {
	pub rewritten_slabs: usize,
	pub rewritten_records: usize,
	pub relocated_rows: usize,
	pub reclaimed_bytes: u64,
}

pub(crate) enum PreparedTieredRetentionRewrite {
	Rewrite {
		slab_id: SlabId,
		old_l2_uri: String,
		encrypted_bytes: Vec<u8>,
		payload_hash: [u8; 32],
		tombstone_metadata: TombstoneMetadata,
		supersession_metadata: SupersessionMetadata,
		pruned_versions: usize,
		retention_stats: RetentionCompactionStats,
		reclaimed_bytes: u64,
	},
	Retire {
		slab_id: SlabId,
		old_l2_uri: String,
		pruned_versions: usize,
		retention_stats: RetentionCompactionStats,
		reclaimed_bytes: u64,
	},
	Observe {
		retention_stats: RetentionCompactionStats,
	},
}

/// Multi-slab composition of [`StorageEngine`].
pub struct SlabManager {
	config: SlabManagerConfig,
	file_ops: Arc<dyn AsyncFileOps>,
	state: Mutex<ManagerState>,
	compaction_manifest: RwLock<CompactionManifest>,
	slab_pins: Arc<SlabPinTable>,
	/// Authoritative key → version chain, aggregated across all
	/// active and sealed slabs. Per ADR 013 the chain stores every
	/// version sorted ascending by HLC; the tail is the current
	/// value and earlier entries back time-travel reads.
	key_index: DashMap<TenantKey, Vec<RecordLocation>>,
	/// Tenant-scoped locality membership index for range scans.
	///
	/// Query planning and migration scans use this ordered set to
	/// jump directly to the identities whose projected locality falls
	/// inside the requested window, instead of filtering every key in
	/// the tenant-wide `key_index`.
	locality_index: RwLock<TenantLocalityMembers>,
	/// Routing locality for each key. Legacy KV keys default to
	/// `Locality::from(key_identity)`; typed table rows can pin a
	/// table-scoped projection computed at ingest.
	key_localities: DashMap<TenantKey, Locality>,
	/// Best-effort compatibility map for in-flight readers that
	/// raced a compaction rewrite. New lookups pivot to the fresh
	/// location through `key_index`; a stale snapshot of the prior
	/// location follows this chain to the replacement slab.
	relocated_reads: DashMap<RecordLocation, RecordLocation>,
	/// Persistent restart index for the local slab set.
	///
	/// Ordinary record rows remain authoritative in slab bytes plus
	/// the in-memory `key_index`; if redb drifts, we rebuild it from
	/// those sources. Tiered-slab pointers come from the Control Ring
	/// state machine, and their redb transactions also stamp the
	/// highest applied control-log index we have mirrored so far.
	persistent_index: Option<Arc<PersistentLocalIndex>>,
	/// Retired tier pointers that still need local cleanup retried.
	///
	/// A tier-retirement apply may succeed in the state machine but
	/// fail locally before unlinking the slab file or before a caller
	/// deletes the old L2 object. The sweeper reconciles this set
	/// against the committed SM view on the next pass.
	retired_tiered_cleanup: DashMap<SlabId, TieredSlabPointer>,
	/// Local per-source-slab secondary-index sidecar metadata.
	/// The sidecar file bytes live under `config.dir`; this map is
	/// the runtime catalog recovered from redb on open and updated by
	/// backfill / compaction / drop-GC surfaces.
	secondary_index_segments:
		DashMap<PersistentSecondaryIndexSegmentKey, PersistentSecondaryIndexSegmentRow>,
	/// Runtime coverage state for secondary-index sidecars.
	///
	/// This is derived from persistent sidecar rows on open, then
	/// updated by the explicit backfill / active-buffer surfaces
	/// below. The replicated catalog remains authoritative for
	/// index lifecycle state; this map only tracks local coverage.
	secondary_index_coverage: DashMap<SecondaryIndexCoverageKey, SecondaryIndexCoverageTracker>,
	/// Per-source-slab lease used to serialize secondary-index
	/// backfill reads with compaction retirement of the same source
	/// slab. Backfill holds a read lease until its sidecar is
	/// persisted; compaction holds a write lease while removing or
	/// replacing the source slab.
	secondary_index_source_slab_leases: DashMap<SlabId, Arc<AsyncRwLock<()>>>,
	/// Parsed ADR 039 CSBs keyed by slab, descriptor, and footer
	/// locator identity. This avoids repeated L2 range reads and
	/// AEAD decrypts for WFRM query bursts over the same cold slab.
	tiered_stats_cache: DashMap<TieredStatsCacheKey, ParsedColumnStatsBlock>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecondaryIndexSegmentInfo {
	pub tenant_id: TenantId,
	pub table_object_id: TableObjectId,
	pub index_object_id: IndexObjectId,
	pub logical_schema_fingerprint: LogicalSchemaFingerprint,
	pub source_slab_id: SlabId,
	pub segment_path: String,
	pub l2_uri: Option<String>,
	pub payload_hash: [u8; 32],
	pub state_digest: [u8; 32],
	pub format_version: u16,
	pub entry_count: u64,
}

impl SecondaryIndexSegmentInfo {
	fn key(&self) -> PersistentSecondaryIndexSegmentKey {
		PersistentSecondaryIndexSegmentKey {
			tenant_id: self.tenant_id,
			table_object_id: self.table_object_id,
			index_object_id: self.index_object_id,
			logical_schema_fingerprint: self.logical_schema_fingerprint,
			source_slab_id: self.source_slab_id,
		}
	}

	pub fn coverage_key(&self) -> SecondaryIndexCoverageKey {
		SecondaryIndexCoverageKey {
			tenant_id: self.tenant_id,
			table_object_id: self.table_object_id,
			index_object_id: self.index_object_id,
			logical_schema_fingerprint: self.logical_schema_fingerprint,
		}
	}
}

impl From<PersistentSecondaryIndexSegmentRow> for SecondaryIndexSegmentInfo {
	fn from(row: PersistentSecondaryIndexSegmentRow) -> Self {
		Self {
			tenant_id: row.key.tenant_id,
			table_object_id: row.key.table_object_id,
			index_object_id: row.key.index_object_id,
			logical_schema_fingerprint: row.key.logical_schema_fingerprint,
			source_slab_id: row.key.source_slab_id,
			segment_path: row.segment_path,
			l2_uri: row.l2_uri,
			payload_hash: row.payload_hash,
			state_digest: row.state_digest,
			format_version: row.format_version,
			entry_count: row.entry_count,
		}
	}
}

impl From<SecondaryIndexSegmentInfo> for PersistentSecondaryIndexSegmentRow {
	fn from(info: SecondaryIndexSegmentInfo) -> Self {
		Self {
			key: info.key(),
			segment_path: info.segment_path,
			l2_uri: info.l2_uri,
			payload_hash: info.payload_hash,
			state_digest: info.state_digest,
			format_version: info.format_version,
			entry_count: info.entry_count,
		}
	}
}

fn secondary_index_coverage_key_from_persistent(
	key: &PersistentSecondaryIndexSegmentKey,
) -> SecondaryIndexCoverageKey {
	SecondaryIndexCoverageKey {
		tenant_id: key.tenant_id,
		table_object_id: key.table_object_id,
		index_object_id: key.index_object_id,
		logical_schema_fingerprint: key.logical_schema_fingerprint,
	}
}

fn secondary_index_backfill_reason_from_io(
	err: &std::io::Error,
) -> SecondaryIndexBackfillErrorReason {
	match err.kind() {
		ErrorKind::NotFound => SecondaryIndexBackfillErrorReason::MissingSegment,
		ErrorKind::InvalidData | ErrorKind::UnexpectedEof => {
			SecondaryIndexBackfillErrorReason::CorruptSegment
		}
		_ => SecondaryIndexBackfillErrorReason::EncodeFailure,
	}
}

fn lsm_run_summary_from_index(
	key_index: &DashMap<TenantKey, Vec<RecordLocation>>,
	slab_id: SlabId,
	size_bytes: u64,
	manifest_epoch: ManifestEpoch,
) -> LsmRunSummary {
	let mut min_row_hlc: Option<HybridTimestamp> = None;
	let mut max_row_hlc: Option<HybridTimestamp> = None;
	let mut row_count = 0_u64;

	for entry in key_index.iter() {
		for location in entry.value() {
			if location.slab_id != slab_id {
				continue;
			}
			min_row_hlc = Some(
				min_row_hlc
					.map(|current| current.min(location.hlc))
					.unwrap_or(location.hlc),
			);
			max_row_hlc = Some(
				max_row_hlc
					.map(|current| current.max(location.hlc))
					.unwrap_or(location.hlc),
			);
			row_count = row_count.saturating_add(1);
		}
	}

	LsmRunSummary::new(
		slab_id,
		CompactionLevelIndex::L0,
		min_row_hlc.unwrap_or_else(zero_hlc),
		max_row_hlc.unwrap_or_else(zero_hlc),
		ByteCount::new(size_bytes),
		RowCount::new(row_count),
		manifest_epoch,
	)
}

const fn zero_hlc() -> HybridTimestamp {
	HybridTimestamp {
		physical_ms: 0,
		logical: 0,
		node_id: 0,
	}
}

fn refresh_lsm_level_metrics(snapshot: &ManifestSnapshot) {
	for compaction_level in CompactionLevelIndex::ALL {
		metrics().set_lsm_level_summary(
			compaction_level,
			u64::from(snapshot.compaction_level_slab_count(compaction_level)),
			snapshot.compaction_level_total_bytes(compaction_level),
		);
	}
}

fn remove_non_live_lsm_rows_from_index(
	key_index: &DashMap<TenantKey, Vec<RecordLocation>>,
	runs: &[LsmRunSummary],
) {
	let non_live_slabs = runs
		.iter()
		.filter(|run| !run.state.is_live())
		.map(|run| run.slab_id)
		.collect::<HashSet<_>>();
	if non_live_slabs.is_empty() {
		return;
	}
	let keys = key_index
		.iter()
		.map(|entry| *entry.key())
		.collect::<Vec<_>>();
	for key in keys {
		let should_remove_key = if let Some(mut versions) = key_index.get_mut(&key) {
			versions.retain(|location| !non_live_slabs.contains(&location.slab_id));
			versions.is_empty()
		} else {
			false
		};
		if should_remove_key {
			key_index.remove(&key);
		}
	}
}

fn lsm_compaction_result_from_failure(err: &CompactionFailure) -> LsmCompactionResult {
	match err {
		CompactionFailure::ManifestRace => LsmCompactionResult::ManifestRace,
		CompactionFailure::SidecarDigestMismatch => LsmCompactionResult::SidecarDigestMismatch,
		CompactionFailure::VisibleDigestInvariance => LsmCompactionResult::VisibleDigestInvariance,
		CompactionFailure::BackfillRaceLost | CompactionFailure::IoError(_) => {
			LsmCompactionResult::Aborted
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct TieredStatsCacheKey {
	slab_id: SlabId,
	table_object_id: TableObjectId,
	schema_generation: u64,
	logical_schema_fingerprint: LogicalSchemaFingerprint,
	stats_format_version: u16,
	stats_block_len: u32,
	stats_block_hash128: [u8; 16],
}

fn tiered_stats_cache_key(
	pointer: &TieredSlabPointer,
	descriptor: &TableSchemaDescriptorV1,
) -> Option<TieredStatsCacheKey> {
	Some(TieredStatsCacheKey {
		slab_id: pointer.slab_id,
		table_object_id: descriptor.table_object_id,
		schema_generation: descriptor.schema_generation,
		logical_schema_fingerprint: descriptor.logical_schema_fingerprint,
		stats_format_version: pointer.stats_format_version?,
		stats_block_len: pointer.stats_block_len?,
		stats_block_hash128: pointer.stats_block_hash128?,
	})
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct TieredRowRefreshPlan {
	old_rows: Vec<PersistentRecordRow>,
	new_rows: Vec<PersistentRecordRow>,
	relocated_reads: Vec<(RecordLocation, RecordLocation)>,
}

struct LsmInstalledCompactionOutputs {
	installed: Vec<(LsmWrittenOutputRun, Arc<LocalSlabStore>)>,
	removed_rows: Vec<PersistentRecordRow>,
	inserted_rows: Vec<PersistentRecordRow>,
}

#[derive(Clone)]
struct RehydratedSlabState {
	slab: Arc<LocalSlabStore>,
	keep_local_until: Instant,
}

struct ManagerState {
	active: Arc<LocalSlabStore>,
	/// Sealed slabs keyed by `slab_id`. Read-only from the
	/// manager's perspective and eligible for tiering to L2 via
	/// the background sweeper. An entry leaves this map only when
	/// [`SlabManager::apply_tier_slab`] has moved the slab's
	/// backing from L1 to `tiered`.
	sealed: DashMap<SlabId, Arc<LocalSlabStore>>,
	/// Slabs whose payload now lives in L2. Keyed by `slab_id`;
	/// the value is the URI the read path hands to
	/// [`ObjectStore::get`]. Ordinary tiering removes the slab from
	/// `sealed` before inserting here; a later rehydration may add
	/// a separate local cache copy in `rehydrated` while L2 stays
	/// authoritative.
	tiered: DashMap<SlabId, TieredSlabPointer>,
	/// Local L1 cache copies of slabs whose authoritative bytes
	/// still live in L2. Kept outside `sealed` so compaction and
	/// ordinary L1 -> L2 demotion never mistake a cache copy for a
	/// new authoritative slab.
	rehydrated: DashMap<SlabId, RehydratedSlabState>,
	/// Consecutive reads that had to fetch a slab from L2.
	hot_read_counts: HashMap<SlabId, u32>,
	/// Slabs currently being restored from L2 to L1. Guards the
	/// hot-read path against duplicate local installs.
	rehydrating: HashSet<SlabId>,
	next_slab_id: SlabId,
}

impl SlabManager {
	async fn append_on_target(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
		hlc: HybridTimestamp,
		frame: &[u8],
	) -> std::io::Result<(Arc<LocalSlabStore>, RecordLocation)> {
		let slab = self.target_for_append().await?;
		let location = slab.append(tenant_id, key_identity, hlc, frame).await?;
		Ok((slab, location))
	}

	fn index_appended_location(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
		location: RecordLocation,
	) {
		self.index_appended_location_with_locality(
			tenant_id,
			key_identity,
			Locality::from(key_identity),
			location,
		);
	}

	fn index_appended_location_with_locality(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
		routing_locality: Locality,
		location: RecordLocation,
	) {
		self.key_index
			.entry((tenant_id, key_identity))
			.and_modify(|versions| insert_version_sorted(versions, location))
			.or_insert_with(|| vec![location]);
		self.insert_locality_membership_with_locality(tenant_id, key_identity, routing_locality);
	}

	fn latest_location_before(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
		hlc: HybridTimestamp,
	) -> Option<RecordLocation> {
		self.key_index
			.get(&(tenant_id, key_identity))
			.and_then(|versions| {
				versions
					.iter()
					.filter(|location| location.hlc < hlc)
					.max_by_key(|location| location.hlc)
					.copied()
			})
	}

	fn rollback_indexed_location(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
		location: RecordLocation,
	) {
		let key = (tenant_id, key_identity);
		let should_remove_key = if let Some(mut versions) = self.key_index.get_mut(&key) {
			let removed = remove_version_exact(&mut versions, location);
			removed && versions.is_empty()
		} else {
			false
		};
		if should_remove_key {
			self.key_index.remove(&key);
			self.remove_locality_membership(tenant_id, key_identity);
		}
	}

	fn routing_locality_for_key(&self, tenant_id: TenantId, key_identity: Identity) -> Locality {
		self.key_localities
			.get(&(tenant_id, key_identity))
			.map(|entry| *entry.value())
			.unwrap_or_else(|| Locality::from(key_identity))
	}

	/// Strict lookup for admission paths that must inherit an
	/// existing append-only row's stored routing locality.
	pub(crate) fn routing_locality_for_existing_key(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
	) -> Option<Locality> {
		self.key_localities
			.get(&(tenant_id, key_identity))
			.map(|entry| *entry.value())
	}

	#[cfg(test)]
	pub(crate) fn forget_routing_locality_for_test(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
	) {
		self.key_localities.remove(&(tenant_id, key_identity));
	}

	fn routing_locality_for_identity_any_tenant(&self, key_identity: Identity) -> Locality {
		// Migration cursors carry identity + HLC, while the scan
		// order is locality + identity + HLC. Resolve the current
		// routing locality for table-spread rows before resuming.
		self.key_localities
			.iter()
			.find_map(|entry| (entry.key().1 == key_identity).then_some(*entry.value()))
			.unwrap_or_else(|| Locality::from(key_identity))
	}

	fn recovered_entry_routing_locality(&self, entry: &RecoveredSlabEntry) -> Locality {
		if entry.routing_locality_trusted {
			entry.routing_locality
		} else {
			self.routing_locality_for_key(entry.tenant_id, entry.key_identity)
		}
	}

	fn persistent_record_row(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
		location: RecordLocation,
	) -> PersistentRecordRow {
		PersistentRecordRow {
			tenant_id,
			key_identity,
			routing_locality: self.routing_locality_for_key(tenant_id, key_identity),
			location,
		}
	}

	fn insert_locality_membership_with_locality(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
		routing_locality: Locality,
	) {
		let previous = self
			.key_localities
			.insert((tenant_id, key_identity), routing_locality);
		let mut locality_index = recover_rwlock_write(&self.locality_index, "locality_index");
		if let Some(previous_locality) = previous {
			if previous_locality != routing_locality {
				if let Some(members) = locality_index.get_mut(&tenant_id) {
					members.remove(&(previous_locality, key_identity));
				}
			}
		}
		locality_index
			.entry(tenant_id)
			.or_default()
			.insert((routing_locality, key_identity));
	}

	fn remove_locality_membership(&self, tenant_id: TenantId, key_identity: Identity) {
		let mut locality_index = recover_rwlock_write(&self.locality_index, "locality_index");
		let routing_locality = self.routing_locality_for_key(tenant_id, key_identity);
		let should_remove_tenant = if let Some(members) = locality_index.get_mut(&tenant_id) {
			members.remove(&(routing_locality, key_identity));
			members.is_empty()
		} else {
			false
		};
		if should_remove_tenant {
			locality_index.remove(&tenant_id);
		}
		self.key_localities.remove(&(tenant_id, key_identity));
	}

	/// Append a record and force the destination slab's durability
	/// barrier before returning. The manager index is published only
	/// after the coalesced `fdatasync` succeeds, so callers that
	/// require durable visibility do not leak
	/// append-complete-but-unflushed writes through the in-memory
	/// read path.
	pub async fn append_durable(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
		hlc: HybridTimestamp,
		frame: &[u8],
	) -> std::io::Result<RecordLocation> {
		self.append_durable_with_locality(
			tenant_id,
			key_identity,
			Locality::from(key_identity),
			hlc,
			frame,
		)
		.await
	}

	/// Append a record whose routing locality is not the identity
	/// projection. Typed table writes use this to place row-keyed
	/// records on table-scoped arcs while preserving `key_identity`
	/// for MVCC and point reads.
	pub async fn append_durable_with_locality(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
		routing_locality: Locality,
		hlc: HybridTimestamp,
		frame: &[u8],
	) -> std::io::Result<RecordLocation> {
		let superseded = self.latest_location_before(tenant_id, key_identity, hlc);
		let (slab, location) = self
			.append_on_target(tenant_id, key_identity, hlc, frame)
			.await?;
		if let Err(err) = slab.wait_durable_commit().await {
			slab.rollback_index_entry(tenant_id, key_identity, location);
			self.rollback_indexed_location(tenant_id, key_identity, location);
			return Err(err);
		}
		if let Err(err) = self
			.persist_record_row_strict(tenant_id, key_identity, routing_locality, location)
			.await
		{
			slab.rollback_index_entry(tenant_id, key_identity, location);
			self.rollback_indexed_location(tenant_id, key_identity, location);
			return Err(err);
		}
		self.index_appended_location_with_locality(
			tenant_id,
			key_identity,
			routing_locality,
			location,
		);
		if let Some(previous) = superseded {
			let _ = self
				.record_supersession_for_slab(previous.slab_id, hlc, None)
				.await?;
		}
		Ok(location)
	}

	pub(crate) async fn append_durable_multirow_with_locality(
		&self,
		tenant_id: TenantId,
		frame: &[u8],
		entries: &[MultiRowIndexEntry],
	) -> std::io::Result<RecordLocation> {
		let Some(primary) = entries.first().copied() else {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"multi-row append requires at least one index entry",
			));
		};
		let record_hlc = entries
			.iter()
			.map(|entry| entry.row_hlc)
			.max()
			.unwrap_or(primary.row_hlc);
		let mut superseded = Vec::new();
		let mut tombstoned = Vec::new();
		for entry in entries {
			if let Some(previous) =
				self.latest_location_before(tenant_id, entry.key_identity, entry.row_hlc)
			{
				if entry.is_tombstone {
					tombstoned.push((previous.slab_id, entry.row_hlc));
				} else {
					superseded.push((previous.slab_id, entry.row_hlc));
				}
			}
		}
		let (slab, base_location) = self
			.append_on_target(tenant_id, primary.key_identity, record_hlc, frame)
			.await?;
		if let Err(err) = slab.wait_durable_commit().await {
			slab.rollback_index_entry(tenant_id, primary.key_identity, base_location);
			self.rollback_indexed_location(tenant_id, primary.key_identity, base_location);
			return Err(err);
		}

		let rows = entries
			.iter()
			.map(|entry| PersistentRecordRow {
				tenant_id,
				key_identity: entry.key_identity,
				routing_locality: entry.routing_locality,
				location: RecordLocation {
					hlc: entry.row_hlc,
					record_row_index: entry.record_row_index,
					is_tombstone: entry.is_tombstone,
					..base_location
				},
			})
			.collect::<Vec<_>>();
		if let Err(err) = self.persist_record_rows_strict(rows.clone()).await {
			slab.rollback_index_entry(tenant_id, primary.key_identity, base_location);
			for row in &rows {
				self.rollback_indexed_location(row.tenant_id, row.key_identity, row.location);
			}
			return Err(err);
		}

		slab.rollback_index_entry(tenant_id, primary.key_identity, base_location);
		for row in &rows {
			slab.index_location(row.tenant_id, row.key_identity, row.location);
			self.index_appended_location_with_locality(
				row.tenant_id,
				row.key_identity,
				row.routing_locality,
				row.location,
			);
		}
		for (slab_id, supersession_hlc) in superseded {
			let _ = self
				.record_supersession_for_slab(slab_id, supersession_hlc, None)
				.await?;
		}
		for (slab_id, tombstone_hlc) in tombstoned {
			let _ = self
				.record_row_tombstone_for_slab(slab_id, tombstone_hlc, None)
				.await?;
		}
		Ok(base_location)
	}

	/// Durability barrier across every local L1 slab currently
	/// referenced by the manager.
	///
	/// Used by the migration sweeper before proposing
	/// `MigrationComplete`: the pull path may have appended records
	/// across the active slab and any newly sealed slabs created by
	/// rotation, so the sweeper needs a manager-wide barrier rather
	/// than a single-slab `fsync`.
	pub async fn fsync_all_local_slabs(&self) -> std::io::Result<()> {
		let (active, sealed, rehydrated) = {
			let state = self.state.lock().await;
			let active = state.active.clone();
			let sealed = state
				.sealed
				.iter()
				.map(|entry| entry.value().clone())
				.collect::<Vec<_>>();
			let rehydrated = state
				.rehydrated
				.iter()
				.map(|entry| entry.value().slab.clone())
				.collect::<Vec<_>>();
			(active, sealed, rehydrated)
		};

		active.fsync().await?;
		for slab in sealed {
			slab.fsync().await?;
		}
		for slab in rehydrated {
			slab.fsync().await?;
		}
		Ok(())
	}

	/// Open or create a manager rooted at `config.dir`. The
	/// directory must already exist; the manager will not create
	/// it to avoid racing with operator tooling that also manages
	/// permissions.
	///
	/// ## Boot behaviour
	///
	/// Scans `config.dir` for `slab_<hex>.wnd` files:
	///
	/// * **Empty directory**: starts fresh with slab `0` as active.
	/// * **One or more existing slabs**: opens all of them
	///   (recovering each slab's on-disk records into its local
	///   index), treats the highest-numbered slab as active, seals
	///   the rest, and sets `next_slab_id` to `max_id + 1`.
	///
	/// All recovered records are folded into the manager's
	/// aggregated `key_index`. Last-write-wins across slabs, ordered
	/// by slab id — so a rewrite that landed in a newer slab
	/// shadows the stale copy in an older one.
	pub async fn open(config: SlabManagerConfig) -> std::io::Result<Self> {
		config.rehydration.validate()?;
		config.l2_column_fetch.validate()?;
		config.row_group_writer.validate()?;
		config.l2_row_group_fetch.validate()?;
		config.encoding_writer.validate()?;
		config.l2_chunk_layout.validate()?;
		config.l2_encoding_fetch.validate()?;
		config.encoding_compute.validate()?;
		if !config.dir.is_dir() {
			return Err(Error::new(
				ErrorKind::NotFound,
				format!(
					"SlabManager directory does not exist: {}",
					config.dir.display()
				),
			));
		}

		let file_ops: Arc<dyn AsyncFileOps> = Arc::new(IoUringFileOps::with_isolated_blocking(
			config.io.clone(),
			"wendelon-fs-meta-slab",
			BlockingIoClass::SlabMetadata,
		)?);
		cleanup_retention_rewrite_tmp_files(file_ops.as_ref(), &config.dir).await?;
		cleanup_lsm_compaction_tmp_files(file_ops.as_ref(), &config.dir).await?;
		let mut existing_ids = discover_slab_ids(file_ops.as_ref(), &config.dir).await?;
		existing_ids.sort_unstable();

		let key_index: DashMap<TenantKey, Vec<RecordLocation>> = DashMap::new();
		let sealed: DashMap<SlabId, Arc<LocalSlabStore>> = DashMap::new();
		let tiered: DashMap<SlabId, TieredSlabPointer> = DashMap::new();
		let rehydrated: DashMap<SlabId, RehydratedSlabState> = DashMap::new();
		let secondary_index_segments: DashMap<
			PersistentSecondaryIndexSegmentKey,
			PersistentSecondaryIndexSegmentRow,
		> = DashMap::new();
		let secondary_index_coverage: DashMap<
			SecondaryIndexCoverageKey,
			SecondaryIndexCoverageTracker,
		> = DashMap::new();

		let mut recovered_key_localities: Vec<(TenantId, Identity, Locality)> = Vec::new();
		let (active, next_slab_id) = if existing_ids.is_empty() {
			let id = SlabId::ZERO;
			let slab = Arc::new(
				LocalSlabStore::open(
					slab_path(&config.dir, id),
					id,
					file_ops.clone(),
					config.group_commit.clone(),
				)
				.await?,
			);
			drain_slab_index(&slab, &key_index, &mut recovered_key_localities);
			(slab, id.saturating_add(1))
		} else {
			// Split: everything except the max becomes sealed, the
			// max becomes active. Drain into key_index in ascending
			// id order so later writes shadow earlier ones.
			let (&active_id, sealed_ids) = existing_ids.split_last().expect("non-empty");
			for &id in sealed_ids {
				let slab = Arc::new(
					LocalSlabStore::open(
						slab_path(&config.dir, id),
						id,
						file_ops.clone(),
						config.group_commit.clone(),
					)
					.await?,
				);
				drain_slab_index(&slab, &key_index, &mut recovered_key_localities);
				sealed.insert(id, slab);
			}
			let active = Arc::new(
				LocalSlabStore::open(
					slab_path(&config.dir, active_id),
					active_id,
					file_ops.clone(),
					config.group_commit.clone(),
				)
				.await?,
			);
			drain_slab_index(&active, &key_index, &mut recovered_key_localities);
			(active, active_id.saturating_add(1))
		};

		let mut persistent_key_localities: Vec<(TenantId, Identity, Locality)> = Vec::new();
		let mut recovered_lsm_runs: Vec<LsmRunSummary> = Vec::new();
		let persistent_index =
			match PersistentLocalIndex::open(persistent_index_path(&config.dir)).await {
				Ok(index) => Some(Arc::new(index)),
				Err(err) => {
					tracing::warn!(
						error = %err,
						dir = %config.dir.display(),
						"failed to open redb storage index; continuing with in-memory recovery"
					);
					None
				}
			};
		if let Some(index) = &persistent_index {
			match index.load_snapshot().await {
				Ok(snapshot) => {
					for pointer in snapshot.tiered_slabs {
						tiered.insert(pointer.slab_id, pointer);
					}
					for row in snapshot.tiered_records {
						key_index
							.entry((row.tenant_id, row.key_identity))
							.and_modify(|versions| insert_version_sorted(versions, row.location))
							.or_insert_with(|| vec![row.location]);
						persistent_key_localities.push((
							row.tenant_id,
							row.key_identity,
							row.routing_locality,
						));
					}
					for row in snapshot.secondary_index_segments {
						let coverage_key = secondary_index_coverage_key_from_persistent(&row.key);
						secondary_index_coverage
							.entry(coverage_key)
							.or_insert_with(|| SecondaryIndexCoverageTracker::new(coverage_key))
							.mark_covered(row.key.source_slab_id);
						secondary_index_segments.insert(row.key, row);
					}
					persistent_key_localities.extend(snapshot.record_localities);
					recovered_lsm_runs = snapshot.lsm_runs;
				}
				Err(err) => {
					tracing::warn!(
						error = %err,
						dir = %config.dir.display(),
						"failed to load tiered rows from redb; local slab scan remains authoritative"
					);
				}
			}
		}
		remove_non_live_lsm_rows_from_index(&key_index, &recovered_lsm_runs);
		let key_localities = build_key_localities(&key_index);
		for (tenant_id, key_identity, locality) in recovered_key_localities {
			if key_index.contains_key(&(tenant_id, key_identity)) {
				key_localities.insert((tenant_id, key_identity), locality);
			}
		}
		for (tenant_id, key_identity, locality) in persistent_key_localities {
			if key_index.contains_key(&(tenant_id, key_identity)) {
				key_localities.insert((tenant_id, key_identity), locality);
			}
		}
		for entry in key_index.iter() {
			validate_row_tombstone_chain(entry.value())?;
		}
		let locality_index = build_locality_index(&key_localities);
		let recovered_manifest_epoch = recovered_lsm_runs
			.iter()
			.map(|run| run.manifest_epoch)
			.max()
			.unwrap_or(ManifestEpoch::ZERO);
		let mut compaction_manifest = if recovered_lsm_runs.is_empty() {
			CompactionManifest::new(ManifestEpoch::ZERO)
		} else {
			CompactionManifest::from_recovered_runs(recovered_manifest_epoch, recovered_lsm_runs)
		};
		for entry in sealed.iter() {
			if compaction_manifest.get_run(*entry.key()).is_none() {
				compaction_manifest.insert_live_run(lsm_run_summary_from_index(
					&key_index,
					*entry.key(),
					entry.value().write_cursor(),
					compaction_manifest.manifest_epoch(),
				));
			}
		}

		let manager = Self {
			config,
			file_ops,
			state: Mutex::new(ManagerState {
				active,
				sealed,
				tiered,
				rehydrated,
				hot_read_counts: HashMap::new(),
				rehydrating: HashSet::new(),
				next_slab_id,
			}),
			compaction_manifest: RwLock::new(compaction_manifest),
			slab_pins: Arc::new(SlabPinTable::default()),
			key_index,
			locality_index: RwLock::new(locality_index),
			key_localities,
			relocated_reads: DashMap::new(),
			persistent_index,
			retired_tiered_cleanup: DashMap::new(),
			secondary_index_segments,
			secondary_index_coverage,
			secondary_index_source_slab_leases: DashMap::new(),
			tiered_stats_cache: DashMap::new(),
		};
		let snapshot = manager.compaction_manifest_snapshot();
		refresh_lsm_level_metrics(&snapshot);
		manager.rebuild_persistent_index().await;
		Ok(manager)
	}

	/// Test / diagnostic accessor: current active slab id.
	pub async fn active_slab_id(&self) -> SlabId {
		self.state.lock().await.active.slab_id()
	}

	#[cfg(test)]
	pub async fn force_seal_active_for_test(&self) -> SlabId {
		let mut state = self.state.lock().await;
		let sealed = state.active.clone();
		sealed
			.fsync()
			.await
			.expect("force_seal_active_for_test should fsync sealed slab");
		let sealed_id = sealed.slab_id();
		let sealed_size = sealed.write_cursor();
		let next_id = state.next_slab_id;
		let next = Arc::new(
			LocalSlabStore::open(
				slab_path(&self.config.dir, next_id),
				next_id,
				self.file_ops.clone(),
				self.config.group_commit.clone(),
			)
			.await
			.expect("force_seal_active_for_test should open next slab"),
		);
		state.active = next;
		state.sealed.insert(sealed_id, sealed);
		state.next_slab_id = next_id.saturating_add(1);
		self.record_l0_compaction_run(sealed_id, sealed_size);
		sealed_id
	}

	pub fn data_dir(&self) -> &Path {
		&self.config.dir
	}

	/// Test / diagnostic accessor: number of sealed slabs.
	pub async fn sealed_count(&self) -> usize {
		self.state.lock().await.sealed.len()
	}

	pub async fn reserve_lsm_output_slab_ids(&self, count: usize) -> std::io::Result<Vec<SlabId>> {
		let count = u64::try_from(count).map_err(|_| {
			Error::new(
				ErrorKind::InvalidInput,
				"LSM output slab reservation count does not fit in u64",
			)
		})?;
		let mut state = self.state.lock().await;
		let start = state.next_slab_id.get();
		let end = start.checked_add(count).ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidInput,
				"LSM output slab reservation would overflow SlabId",
			)
		})?;
		state.next_slab_id = SlabId::new(end);
		Ok((start..end).map(SlabId::new).collect())
	}

	fn record_l0_compaction_run(&self, slab_id: SlabId, size_bytes: u64) {
		let mut manifest = recover_rwlock_write(&self.compaction_manifest, "compaction_manifest");
		let summary = lsm_run_summary_from_index(
			&self.key_index,
			slab_id,
			size_bytes,
			manifest.manifest_epoch(),
		);
		if summary.row_count.get() == 0 {
			return;
		}
		manifest.insert_live_run(summary);
		let snapshot = manifest.snapshot();
		drop(manifest);
		refresh_lsm_level_metrics(&snapshot);
	}

	/// Test / diagnostic accessor: number of slabs currently
	/// pivoted to L2.
	pub async fn tiered_count(&self) -> usize {
		self.state.lock().await.tiered.len()
	}

	/// Test / diagnostic accessor: number of rehydrated local L1
	/// cache copies currently resident.
	pub async fn rehydrated_count(&self) -> usize {
		self.state.lock().await.rehydrated.len()
	}

	/// Snapshot of every sealed slab eligible for tiering. The
	/// sweeper uses this to decide what to ship to L2.
	/// The active slab is excluded by construction — only slabs
	/// that have been rotated out are here. Already-tiered slabs
	/// are excluded too, so calling this repeatedly is idempotent.
	pub async fn list_sealed_slabs(&self) -> Vec<SealedSlabInfo> {
		let state = self.state.lock().await;
		let mut out = Vec::with_capacity(state.sealed.len());
		for entry in state.sealed.iter() {
			out.push(SealedSlabInfo {
				slab_id: *entry.key(),
				size_bytes: entry.value().write_cursor(),
			});
		}
		out.sort_by_key(|s| s.slab_id);
		out
	}

	pub fn compaction_manifest_snapshot(&self) -> ManifestSnapshot {
		recover_rwlock_read(&self.compaction_manifest, "compaction_manifest").snapshot()
	}

	pub fn apply_lsm_manifest_compaction_commit(
		&self,
		commit: &CompactionCommit,
	) -> Result<ManifestEpoch, CompactionFailure> {
		let mut manifest = recover_rwlock_write(&self.compaction_manifest, "compaction_manifest");
		let post_epoch = manifest.apply_compaction_commit(commit)?;
		let snapshot = manifest.snapshot();
		drop(manifest);
		self.apply_lsm_pin_state_for_manifest_commit(commit);
		refresh_lsm_level_metrics(&snapshot);
		Ok(post_epoch)
	}

	fn apply_lsm_pin_state_for_manifest_commit(&self, commit: &CompactionCommit) {
		for slab_id in &commit.slabs_to_mark_delete_pending {
			self.slab_pins.force_delete_pending(*slab_id);
		}
		for slab_id in &commit.slabs_to_remove {
			self.slab_pins.clear_delete_pending(*slab_id);
		}
	}

	pub fn apply_lsm_manifest_planned_compaction_commit(
		&self,
		plan: &CompactionPlan,
		commit: &CompactionCommit,
	) -> Result<ManifestEpoch, CompactionFailure> {
		let mut manifest = recover_rwlock_write(&self.compaction_manifest, "compaction_manifest");
		let post_epoch = manifest.apply_planned_compaction_commit(plan, commit)?;
		let snapshot = manifest.snapshot();
		drop(manifest);
		self.apply_lsm_pin_state_for_manifest_commit(commit);
		refresh_lsm_level_metrics(&snapshot);
		Ok(post_epoch)
	}

	pub async fn install_lsm_compaction_temp_outputs(
		&self,
		plan: &CompactionPlan,
		prepared: &LsmPreparedOutputs,
		written: &[LsmWrittenOutputRun],
		commit: &CompactionCommit,
	) -> Result<ManifestEpoch, CompactionFailure> {
		let staged = self
			.stage_lsm_compaction_outputs(plan, prepared, written, commit, false)
			.await?;
		let post_epoch = match self.apply_lsm_manifest_planned_compaction_commit(plan, commit) {
			Ok(post_epoch) => post_epoch,
			Err(err) => {
				self.cleanup_lsm_installed_outputs(&staged.installed).await;
				return Err(err);
			}
		};
		self.complete_lsm_compaction_install(staged, post_epoch)
			.await
	}

	pub async fn install_lsm_compaction_temp_outputs_via_control_ring(
		&self,
		plan: &CompactionPlan,
		prepared: &LsmPreparedOutputs,
		written: &[LsmWrittenOutputRun],
		commit: &CompactionCommit,
		committer: &dyn TierCommitter,
	) -> Result<ManifestEpoch, CompactionFailure> {
		self.install_lsm_compaction_temp_outputs_via_control_ring_with_context(
			plan, prepared, written, commit, committer, None,
		)
		.await
	}

	async fn install_lsm_compaction_temp_outputs_via_control_ring_with_context(
		&self,
		plan: &CompactionPlan,
		prepared: &LsmPreparedOutputs,
		written: &[LsmWrittenOutputRun],
		commit: &CompactionCommit,
		committer: &dyn TierCommitter,
		secondary_index_context: Option<SecondaryIndexCompactionContext<'_>>,
	) -> Result<ManifestEpoch, CompactionFailure> {
		let staged = self
			.stage_lsm_compaction_outputs(
				plan,
				prepared,
				written,
				commit,
				secondary_index_context.is_some(),
			)
			.await?;
		let prepared_sidecars = match self
			.prepare_lsm_secondary_index_segments_for_compaction(
				plan,
				written,
				secondary_index_context,
			)
			.await
		{
			Ok(prepared_sidecars) => prepared_sidecars,
			Err(err) => {
				self.cleanup_lsm_installed_outputs(&staged.installed).await;
				return Err(err);
			}
		};
		let commit_result = committer.commit_lsm_compaction(commit.clone()).await;
		let Some(post_epoch) = self.lsm_committed_manifest_epoch(commit) else {
			self.cleanup_lsm_installed_outputs(&staged.installed).await;
			return match commit_result {
				Ok(()) => Err(CompactionFailure::ManifestRace),
				Err(err) => Err(err.into()),
			};
		};
		let post_epoch = self
			.complete_lsm_compaction_install(staged, post_epoch)
			.await?;
		self.publish_prepared_lsm_secondary_index_segments(prepared_sidecars)
			.await;
		Ok(post_epoch)
	}

	async fn stage_lsm_compaction_outputs(
		&self,
		plan: &CompactionPlan,
		prepared: &LsmPreparedOutputs,
		written: &[LsmWrittenOutputRun],
		commit: &CompactionCommit,
		allow_sidecar_covered_inputs: bool,
	) -> Result<LsmInstalledCompactionOutputs, CompactionFailure> {
		self.validate_lsm_written_outputs(plan, prepared, written, commit)?;
		self.validate_lsm_sidecar_swap_readiness(plan, commit, allow_sidecar_covered_inputs)?;
		{
			let manifest = recover_rwlock_read(&self.compaction_manifest, "compaction_manifest");
			manifest.validate_compaction_commit_against_plan(plan, commit)?;
		}

		let installed = self.install_lsm_output_files(written).await?;
		let max_output_slab_id = installed.iter().map(|(run, _)| run.slab_id).max();

		let removed_rows = self.lsm_removed_persistent_rows(prepared);
		let inserted_rows = self.lsm_inserted_persistent_rows(prepared, written);

		{
			let state = self.state.lock().await;
			for (run, _) in &installed {
				if state.sealed.contains_key(&run.slab_id)
					|| state.tiered.contains_key(&run.slab_id)
				{
					drop(state);
					self.cleanup_lsm_installed_outputs(&installed).await;
					return Err(CompactionFailure::ManifestRace);
				}
			}
		}
		if let Some(max_output_slab_id) = max_output_slab_id {
			let mut state = self.state.lock().await;
			if let Err(err) = self
				.advance_active_after_lsm_outputs(&mut state, max_output_slab_id)
				.await
			{
				drop(state);
				self.cleanup_lsm_installed_outputs(&installed).await;
				return Err(err.into());
			}
		}

		Ok(LsmInstalledCompactionOutputs {
			installed,
			removed_rows,
			inserted_rows,
		})
	}

	async fn complete_lsm_compaction_install(
		&self,
		staged: LsmInstalledCompactionOutputs,
		post_epoch: ManifestEpoch,
	) -> Result<ManifestEpoch, CompactionFailure> {
		let lsm_runs = self.compaction_manifest_snapshot().runs().to_vec();
		let LsmInstalledCompactionOutputs {
			installed,
			removed_rows,
			inserted_rows,
		} = staged;

		{
			let state = self.state.lock().await;
			for (run, slab) in installed {
				state.sealed.insert(run.slab_id, slab);
			}
			for row in &removed_rows {
				self.rollback_indexed_location(row.tenant_id, row.key_identity, row.location);
			}
			for row in &inserted_rows {
				self.index_appended_location_with_locality(
					row.tenant_id,
					row.key_identity,
					row.routing_locality,
					row.location,
				);
			}
		}

		self.mirror_persistent_update(
			PersistentIndexUpdate {
				delete_records: removed_rows,
				put_records: inserted_rows,
				put_lsm_runs: lsm_runs,
				..Default::default()
			},
			"lsm-compaction-install",
		)
		.await;
		Ok(post_epoch)
	}

	fn lsm_committed_manifest_epoch(&self, commit: &CompactionCommit) -> Option<ManifestEpoch> {
		let snapshot = self.compaction_manifest_snapshot();
		let successors = commit
			.slabs_to_add
			.iter()
			.map(|run| run.slab_id)
			.collect::<Vec<_>>();
		for slab_id in &commit.slabs_to_retire {
			let run = snapshot.run(*slab_id)?;
			if !matches!(&run.state, crate::storage::SlabState::Retired { successors: existing } if existing == &successors)
			{
				return None;
			}
		}
		for expected in &commit.slabs_to_add {
			let run = snapshot.run(expected.slab_id)?;
			if !run.state.is_live()
				|| run.compaction_level != expected.compaction_level
				|| run.min_row_hlc != expected.min_row_hlc
				|| run.max_row_hlc != expected.max_row_hlc
				|| run.size_bytes != expected.size_bytes
				|| run.row_count != expected.row_count
				|| run.sidecar_coverage != expected.sidecar_coverage
			{
				return None;
			}
		}
		Some(snapshot.manifest_epoch)
	}

	pub async fn run_lsm_compaction_once(
		&self,
		worker: &LsmCompactionWorker,
		plan: CompactionPlan,
	) -> Result<crate::storage::lsm::CompactionOutcome, CompactionFailure> {
		let started = Instant::now();
		let source_level = plan.source_level;
		let input_slabs = plan.touched_slabs().len() as u64;
		let result = self.run_lsm_compaction_once_inner(worker, plan).await;
		self.observe_lsm_compaction_result(source_level, input_slabs, started, &result);
		result
	}

	pub async fn run_lsm_compaction_once_via_control_ring(
		&self,
		worker: &LsmCompactionWorker,
		plan: CompactionPlan,
		committer: &dyn TierCommitter,
	) -> Result<crate::storage::lsm::CompactionOutcome, CompactionFailure> {
		self.run_lsm_compaction_once_via_control_ring_with_context(worker, plan, committer, None)
			.await
	}

	async fn run_lsm_compaction_once_via_control_ring_with_context(
		&self,
		worker: &LsmCompactionWorker,
		plan: CompactionPlan,
		committer: &dyn TierCommitter,
		secondary_index_context: Option<SecondaryIndexCompactionContext<'_>>,
	) -> Result<crate::storage::lsm::CompactionOutcome, CompactionFailure> {
		let started = Instant::now();
		let source_level = plan.source_level;
		let input_slabs = plan.touched_slabs().len() as u64;
		let result = self
			.run_lsm_compaction_once_inner_via_control_ring(
				worker,
				plan,
				committer,
				secondary_index_context,
			)
			.await;
		self.observe_lsm_compaction_result(source_level, input_slabs, started, &result);
		result
	}

	fn observe_lsm_compaction_result(
		&self,
		source_level: CompactionLevelIndex,
		input_slabs: u64,
		started: Instant,
		result: &Result<crate::storage::lsm::CompactionOutcome, CompactionFailure>,
	) {
		match result {
			Ok(outcome) => metrics().observe_lsm_compaction(
				source_level,
				LsmCompactionResult::Ok,
				input_slabs,
				outcome.bytes_out,
				started.elapsed(),
			),
			Err(err) => {
				let metric_result = lsm_compaction_result_from_failure(err);
				if matches!(metric_result, LsmCompactionResult::VisibleDigestInvariance) {
					metrics().observe_lsm_digest_invariance_violation(
						LsmDigestInvariantKind::VisibleDomain,
					);
				}
				metrics().observe_lsm_compaction(
					source_level,
					metric_result,
					input_slabs,
					0,
					started.elapsed(),
				);
			}
		}
	}

	pub fn pick_lsm_compaction_plan(
		&self,
		worker: &LsmCompactionWorker,
		now: HybridTimestamp,
	) -> Option<CompactionPlan> {
		let snapshot = self.compaction_manifest_snapshot();
		pick_compaction(&snapshot, &worker.config(), now)
	}

	pub async fn run_lsm_compaction_tick(
		&self,
		worker: &LsmCompactionWorker,
		now: HybridTimestamp,
	) -> Result<Option<crate::storage::lsm::CompactionOutcome>, CompactionFailure> {
		let Some(plan) = self.pick_lsm_compaction_plan(worker, now) else {
			return Ok(None);
		};
		self.run_lsm_compaction_once(worker, plan).await.map(Some)
	}

	pub async fn run_lsm_compaction_tick_via_control_ring(
		&self,
		worker: &LsmCompactionWorker,
		now: HybridTimestamp,
		committer: &dyn TierCommitter,
	) -> Result<Option<crate::storage::lsm::CompactionOutcome>, CompactionFailure> {
		let Some(plan) = self.pick_lsm_compaction_plan(worker, now) else {
			return Ok(None);
		};
		self.run_lsm_compaction_once_via_control_ring(worker, plan, committer)
			.await
			.map(Some)
	}

	pub async fn run_lsm_compaction_tick_via_control_ring_with_catalogs(
		&self,
		worker: &LsmCompactionWorker,
		now: HybridTimestamp,
		committer: &dyn TierCommitter,
		table_schemas: &TableSchemaCatalogV1,
		secondary_indexes: &SecondaryIndexCatalog,
	) -> Result<Option<crate::storage::lsm::CompactionOutcome>, CompactionFailure> {
		let Some(plan) = self.pick_lsm_compaction_plan(worker, now) else {
			return Ok(None);
		};
		self.run_lsm_compaction_once_via_control_ring_with_context(
			worker,
			plan,
			committer,
			Some(SecondaryIndexCompactionContext {
				table_schemas,
				secondary_indexes,
			}),
		)
		.await
		.map(Some)
	}

	async fn run_lsm_compaction_once_inner(
		&self,
		worker: &LsmCompactionWorker,
		plan: CompactionPlan,
	) -> Result<crate::storage::lsm::CompactionOutcome, CompactionFailure> {
		let _pins = worker.acquire_plan_pins(&plan)?;
		let prepared = self.prepare_lsm_compaction_outputs(worker, &plan).await?;
		let source_frames = self.read_lsm_single_row_source_frames(&plan).await?;
		let written = self
			.write_lsm_compaction_temp_outputs(&plan, &prepared, &source_frames)
			.await?;
		let digest_summary =
			match self.lsm_single_row_bridge_digest_summary(&prepared, &written, &source_frames) {
				Ok(digest_summary) => digest_summary,
				Err(err) => {
					self.cleanup_lsm_written_outputs(&written).await;
					return Err(err);
				}
			};
		let commit = worker.build_written_compaction_commit(&plan, &written, digest_summary);
		let post_epoch = match self
			.install_lsm_compaction_temp_outputs(&plan, &prepared, &written, &commit)
			.await
		{
			Ok(post_epoch) => post_epoch,
			Err(err) => {
				self.cleanup_lsm_written_outputs(&written).await;
				return Err(err);
			}
		};
		Ok(worker.compaction_outcome_from_commit(&plan, &prepared, &commit, post_epoch))
	}

	async fn run_lsm_compaction_once_inner_via_control_ring(
		&self,
		worker: &LsmCompactionWorker,
		plan: CompactionPlan,
		committer: &dyn TierCommitter,
		secondary_index_context: Option<SecondaryIndexCompactionContext<'_>>,
	) -> Result<crate::storage::lsm::CompactionOutcome, CompactionFailure> {
		let _pins = worker.acquire_plan_pins(&plan)?;
		let prepared = match secondary_index_context {
			Some(context) => {
				self.prepare_lsm_compaction_outputs_with_table_schemas(
					worker,
					&plan,
					context.table_schemas,
				)
				.await?
			}
			None => self.prepare_lsm_compaction_outputs(worker, &plan).await?,
		};
		let source_frames = self.read_lsm_single_row_source_frames(&plan).await?;
		let written = self
			.write_lsm_compaction_temp_outputs(&plan, &prepared, &source_frames)
			.await?;
		let digest_summary =
			match self.lsm_single_row_bridge_digest_summary(&prepared, &written, &source_frames) {
				Ok(digest_summary) => digest_summary,
				Err(err) => {
					self.cleanup_lsm_written_outputs(&written).await;
					return Err(err);
				}
			};
		let commit = worker.build_written_compaction_commit(&plan, &written, digest_summary);
		let post_epoch = match self
			.install_lsm_compaction_temp_outputs_via_control_ring_with_context(
				&plan,
				&prepared,
				&written,
				&commit,
				committer,
				secondary_index_context,
			)
			.await
		{
			Ok(post_epoch) => post_epoch,
			Err(err) => {
				self.cleanup_lsm_written_outputs(&written).await;
				return Err(err);
			}
		};
		Ok(worker.compaction_outcome_from_commit(&plan, &prepared, &commit, post_epoch))
	}

	fn lsm_single_row_bridge_digest_summary(
		&self,
		prepared: &LsmPreparedOutputs,
		written: &[LsmWrittenOutputRun],
		source_frames: &[LsmSourceFrame],
	) -> Result<CompactionDigestSummary, CompactionFailure> {
		let survivors = survivor_records(&prepared.decisions);
		let output_rows = prepared
			.output_runs
			.iter()
			.flat_map(|run| run.rows.iter().cloned())
			.collect::<Vec<_>>();
		let output_location_count = written.iter().map(|run| run.locations.len()).sum::<usize>();
		if survivors != output_rows || output_rows.len() != output_location_count {
			return Err(CompactionFailure::VisibleDigestInvariance);
		}
		let table_by_location = self.lsm_source_frame_table_map(source_frames)?;
		let mut pre_rows = Vec::with_capacity(prepared.decisions.len());
		let mut post_rows = Vec::with_capacity(survivors.len());
		for decision in &prepared.decisions {
			let record = match decision {
				crate::storage::SurvivorOrDrop::Survivor(record) => record,
				crate::storage::SurvivorOrDrop::Drop { record, .. } => record,
			};
			let visible_row = self.lsm_visible_domain_row_for_record(record, &table_by_location)?;
			pre_rows.push(visible_row);
			if matches!(decision, crate::storage::SurvivorOrDrop::Survivor(_)) {
				post_rows.push(visible_row);
			}
		}
		let digest_summary = compaction_visible_domain_summary(pre_rows, post_rows);
		if digest_summary.visible_pre != digest_summary.visible_post {
			return Err(CompactionFailure::VisibleDigestInvariance);
		}
		Ok(digest_summary)
	}

	fn lsm_source_frame_table_map(
		&self,
		source_frames: &[LsmSourceFrame],
	) -> Result<HashMap<RecordLocation, TableObjectId>, CompactionFailure> {
		let mut table_by_location = HashMap::with_capacity(source_frames.len());
		for source in source_frames {
			table_by_location.insert(
				source.entry.location,
				self.lsm_source_frame_table_object_id(source)?,
			);
		}
		Ok(table_by_location)
	}

	fn lsm_source_frame_table_object_id(
		&self,
		source: &LsmSourceFrame,
	) -> Result<TableObjectId, CompactionFailure> {
		let batch = match decode_record_batch(&source.frame) {
			Ok(batch) => batch,
			Err(_) => return Ok(TableObjectId::DEFAULT_KV),
		};
		match storage_row_table_object_id_at(&batch, source.entry.record_row_index)? {
			Some(table_object_id) => Ok(table_object_id),
			None => Ok(TableObjectId::DEFAULT_KV),
		}
	}

	fn lsm_visible_domain_row_for_record(
		&self,
		record: &VersionEntry,
		table_by_location: &HashMap<RecordLocation, TableObjectId>,
	) -> Result<VisibleDomainRow, CompactionFailure> {
		let Some(table_object_id) = table_by_location.get(&record.location).copied() else {
			return Err(CompactionFailure::VisibleDigestInvariance);
		};
		Ok(VisibleDomainRow {
			tenant_id: record.tenant_id,
			table_object_id,
			row_key_id: record.row_key_id,
			row_hlc: record.row_hlc,
			is_tombstone: record.location.is_tombstone,
		})
	}

	fn validate_lsm_written_outputs(
		&self,
		plan: &CompactionPlan,
		prepared: &LsmPreparedOutputs,
		written: &[LsmWrittenOutputRun],
		commit: &CompactionCommit,
	) -> Result<(), CompactionFailure> {
		if prepared.output_runs.len() != written.len()
			|| written.len() != commit.slabs_to_add.len()
			|| commit.job_id != plan.job_id
			|| commit.manifest_epoch_pre != plan.manifest_epoch_pre
			|| commit.has_lsm_gc_lifecycle_fields()
		{
			return Err(CompactionFailure::ManifestRace);
		}
		for ((planned, written), committed) in prepared
			.output_runs
			.iter()
			.zip(written)
			.zip(&commit.slabs_to_add)
		{
			if planned.slab_id != written.slab_id
				|| planned.slab_id != committed.slab_id
				|| planned.compaction_level != plan.target_level
				|| written.compaction_level != plan.target_level
				|| committed.compaction_level != plan.target_level
				|| planned.rows.len() != written.locations.len()
				|| committed.row_count != written.summary.row_count
				|| committed.size_bytes != written.summary.size_bytes
				|| committed.min_row_hlc != written.summary.min_row_hlc
				|| committed.max_row_hlc != written.summary.max_row_hlc
			{
				return Err(CompactionFailure::ManifestRace);
			}
		}
		Ok(())
	}

	fn validate_lsm_sidecar_swap_readiness(
		&self,
		plan: &CompactionPlan,
		commit: &CompactionCommit,
		allow_sidecar_covered_inputs: bool,
	) -> Result<(), CompactionFailure> {
		if !commit.sidecar_swaps.is_empty() {
			return Err(CompactionFailure::SidecarDigestMismatch);
		}
		let manifest = recover_rwlock_read(&self.compaction_manifest, "compaction_manifest");
		for slab_id in plan.touched_slabs() {
			let Some(run) = manifest.get_run(slab_id) else {
				return Err(CompactionFailure::ManifestRace);
			};
			let has_sidecar_segments = !self
				.list_secondary_index_segments_for_source_slab(slab_id)
				.is_empty();
			if !allow_sidecar_covered_inputs
				&& (run.sidecar_coverage != crate::storage::SidecarCoverage::None
					|| has_sidecar_segments)
			{
				return Err(CompactionFailure::SidecarDigestMismatch);
			}
		}
		Ok(())
	}

	async fn install_lsm_output_files(
		&self,
		written: &[LsmWrittenOutputRun],
	) -> std::io::Result<Vec<(LsmWrittenOutputRun, Arc<LocalSlabStore>)>> {
		let mut final_paths = Vec::with_capacity(written.len());
		for run in written {
			let final_path = slab_path(&self.config.dir, run.slab_id);
			if self.file_ops.file_len(&final_path).await?.is_some() {
				return Err(Error::new(
					ErrorKind::AlreadyExists,
					format!(
						"LSM output slab path already exists: {}",
						final_path.display()
					),
				));
			}
			final_paths.push(final_path);
		}

		let mut installed_paths = Vec::with_capacity(written.len());
		let install_result: std::io::Result<Vec<(LsmWrittenOutputRun, Arc<LocalSlabStore>)>> =
			async {
				let mut installed = Vec::with_capacity(written.len());
				for (run, final_path) in written.iter().zip(&final_paths) {
					self.file_ops.rename(&run.temp_path, final_path).await?;
					fsync_parent_dir(self.file_ops.as_ref(), final_path).await?;
					installed_paths.push(final_path.clone());
					let slab = Arc::new(
						LocalSlabStore::open(
							final_path,
							run.slab_id,
							self.file_ops.clone(),
							self.config.group_commit.clone(),
						)
						.await?,
					);
					installed.push((run.clone(), slab));
				}
				Ok(installed)
			}
			.await;

		match install_result {
			Ok(installed) => Ok(installed),
			Err(err) => {
				for path in installed_paths {
					let _ = unlink_local_file(self.file_ops.as_ref(), &path).await;
				}
				Err(err)
			}
		}
	}

	async fn cleanup_lsm_written_outputs(&self, written: &[LsmWrittenOutputRun]) {
		for run in written {
			let _ = unlink_local_file(self.file_ops.as_ref(), &run.temp_path).await;
		}
	}

	async fn cleanup_lsm_installed_outputs(
		&self,
		installed: &[(LsmWrittenOutputRun, Arc<LocalSlabStore>)],
	) {
		for (run, _) in installed {
			let final_path = slab_path(&self.config.dir, run.slab_id);
			let _ = unlink_local_file(self.file_ops.as_ref(), &final_path).await;
		}
	}

	fn lsm_removed_persistent_rows(
		&self,
		prepared: &LsmPreparedOutputs,
	) -> Vec<PersistentRecordRow> {
		let mut rows = prepared
			.decisions
			.iter()
			.map(|decision| match decision {
				crate::storage::SurvivorOrDrop::Survivor(record) => record,
				crate::storage::SurvivorOrDrop::Drop { record, .. } => record,
			})
			.map(|record| PersistentRecordRow {
				tenant_id: record.tenant_id,
				key_identity: record.row_key_id,
				routing_locality: self
					.routing_locality_for_key(record.tenant_id, record.row_key_id),
				location: record.location,
			})
			.collect::<Vec<_>>();
		rows.sort_by_key(|row| {
			(
				row.location.slab_id,
				row.tenant_id,
				row.key_identity,
				row.location.hlc,
				row.location.offset,
				row.location.record_row_index,
			)
		});
		rows
	}

	fn lsm_inserted_persistent_rows(
		&self,
		prepared: &LsmPreparedOutputs,
		written: &[LsmWrittenOutputRun],
	) -> Vec<PersistentRecordRow> {
		let mut rows = Vec::new();
		for (planned, written) in prepared.output_runs.iter().zip(written) {
			for (record, location) in planned.rows.iter().zip(&written.locations) {
				rows.push(PersistentRecordRow {
					tenant_id: record.tenant_id,
					key_identity: record.row_key_id,
					routing_locality: self
						.routing_locality_for_key(record.tenant_id, record.row_key_id),
					location: *location,
				});
			}
		}
		rows.sort_by_key(|row| {
			(
				row.location.slab_id,
				row.tenant_id,
				row.key_identity,
				row.location.hlc,
				row.location.offset,
				row.location.record_row_index,
			)
		});
		rows
	}

	async fn advance_active_after_lsm_outputs(
		&self,
		state: &mut ManagerState,
		max_output_slab_id: SlabId,
	) -> std::io::Result<()> {
		if state.active.slab_id() > max_output_slab_id {
			return Ok(());
		}
		let next_id = if state.next_slab_id <= max_output_slab_id {
			max_output_slab_id.saturating_add(1)
		} else {
			state.next_slab_id
		};
		let next_path = slab_path(&self.config.dir, next_id);
		let next_slab = Arc::new(
			LocalSlabStore::open(
				&next_path,
				next_id,
				self.file_ops.clone(),
				self.config.group_commit.clone(),
			)
			.await?,
		);
		let sealed_slab = std::mem::replace(&mut state.active, next_slab);
		let sealed_id = sealed_slab.slab_id();
		let sealed_size = sealed_slab.write_cursor();
		state.sealed.insert(sealed_id, sealed_slab);
		state.next_slab_id = next_id.saturating_add(1);
		self.record_l0_compaction_run(sealed_id, sealed_size);
		Ok(())
	}

	pub fn slab_pin_table(&self) -> Arc<SlabPinTable> {
		self.slab_pins.clone()
	}

	pub fn lsm_version_entries_for_plan(&self, plan: &CompactionPlan) -> Vec<VersionEntry> {
		let touched_slabs = plan.touched_slabs().into_iter().collect::<BTreeSet<_>>();
		let mut entries = Vec::new();
		for entry in self.key_index.iter() {
			let (tenant_id, row_key_id) = *entry.key();
			for location in entry.value().iter().copied() {
				if !touched_slabs.contains(&location.slab_id) {
					continue;
				}
				entries.push(VersionEntry {
					tenant_id,
					row_key_id,
					row_hlc: location.hlc,
					record_row_index: location.record_row_index,
					location,
					lifecycle: VersionLifecycle::AmbiguousTable,
				});
			}
		}
		entries.sort_by_key(VersionEntry::merge_key);
		entries
	}

	async fn lsm_version_entries_for_plan_with_table_schemas(
		&self,
		plan: &CompactionPlan,
		table_schemas: &TableSchemaCatalogV1,
	) -> Result<Vec<VersionEntry>, CompactionFailure> {
		let mut entries = self.lsm_version_entries_for_plan(plan);
		for entry in &mut entries {
			entry.lifecycle = self
				.lsm_version_lifecycle_for_entry(entry, table_schemas)
				.await?;
		}
		Ok(entries)
	}

	async fn lsm_version_lifecycle_for_entry(
		&self,
		entry: &VersionEntry,
		table_schemas: &TableSchemaCatalogV1,
	) -> Result<VersionLifecycle, CompactionFailure> {
		let frame = match self.read(entry.location).await {
			Ok(frame) => frame,
			Err(err) if err.kind() == ErrorKind::InvalidData => {
				return Ok(VersionLifecycle::AmbiguousTable);
			}
			Err(err) => return Err(err.into()),
		};
		let batch = match decode_record_batch(&frame) {
			Ok(batch) => batch,
			Err(_) => return Ok(VersionLifecycle::AmbiguousTable),
		};
		let Some(table_object_id) =
			storage_row_table_object_id_at(&batch, entry.location.record_row_index)?
		else {
			return Ok(VersionLifecycle::AmbiguousTable);
		};
		let Some(table) = table_schemas.latest_by_id(entry.tenant_id, table_object_id) else {
			return Ok(VersionLifecycle::AmbiguousTable);
		};
		let row_batch = materialize_record_location_batch(&batch, entry.location.record_row_index)?;
		let routing_locality = self.routing_locality_for_key(entry.tenant_id, entry.row_key_id);
		if !descriptor_matches_tiered_row(
			table,
			entry.tenant_id,
			entry.row_key_id,
			routing_locality,
			Some(table_object_id),
			&row_batch,
		)? {
			return Ok(VersionLifecycle::AmbiguousTable);
		}
		Ok(match table.row_identity_policy {
			RowIdentityPolicy::PrimaryKey(_) => VersionLifecycle::PrimaryKey,
			RowIdentityPolicy::AppendOnly => VersionLifecycle::AppendOnly,
			RowIdentityPolicy::AppendOnlyContentHash => VersionLifecycle::AppendOnlyContentHash,
		})
	}

	pub async fn prepare_lsm_compaction_outputs(
		&self,
		worker: &LsmCompactionWorker,
		plan: &CompactionPlan,
	) -> Result<LsmPreparedOutputs, CompactionFailure> {
		let entries = self.lsm_version_entries_for_plan(plan);
		let target_size = worker
			.config()
			.output_run_target_bytes(plan.target_level)
			.ok_or_else(|| {
				Error::new(
					ErrorKind::InvalidInput,
					"invalid compaction output target level",
				)
			})?;
		let decisions = apply_elision(entries.clone(), plan.retention_floor_snapshot);
		let output_run_count =
			required_output_run_count(survivor_records(&decisions), target_size)?;
		let output_slab_ids = self.reserve_lsm_output_slab_ids(output_run_count).await?;
		worker.plan_survivor_outputs(plan, entries, output_slab_ids)
	}

	async fn prepare_lsm_compaction_outputs_with_table_schemas(
		&self,
		worker: &LsmCompactionWorker,
		plan: &CompactionPlan,
		table_schemas: &TableSchemaCatalogV1,
	) -> Result<LsmPreparedOutputs, CompactionFailure> {
		let entries = self
			.lsm_version_entries_for_plan_with_table_schemas(plan, table_schemas)
			.await?;
		let target_size = worker
			.config()
			.output_run_target_bytes(plan.target_level)
			.ok_or_else(|| {
				Error::new(
					ErrorKind::InvalidInput,
					"invalid compaction output target level",
				)
			})?;
		let decisions = apply_elision(entries.clone(), plan.retention_floor_snapshot);
		let output_run_count =
			required_output_run_count(survivor_records(&decisions), target_size)?;
		let output_slab_ids = self.reserve_lsm_output_slab_ids(output_run_count).await?;
		worker.plan_survivor_outputs(plan, entries, output_slab_ids)
	}

	pub async fn read_lsm_single_row_source_frames(
		&self,
		plan: &CompactionPlan,
	) -> std::io::Result<Vec<LsmSourceFrame>> {
		let entries = self.lsm_version_entries_for_plan(plan);
		let mut frames = Vec::with_capacity(entries.len());
		for entry in entries {
			let frame = self.read(entry.location).await?;
			frames.push(LsmSourceFrame { entry, frame });
		}
		Ok(frames)
	}

	pub async fn write_lsm_compaction_temp_outputs(
		&self,
		plan: &CompactionPlan,
		prepared: &LsmPreparedOutputs,
		source_frames: &[LsmSourceFrame],
	) -> std::io::Result<Vec<LsmWrittenOutputRun>> {
		let mut source_frames_by_location = HashMap::with_capacity(source_frames.len());
		let mut multirow_source_frames_by_key = HashMap::new();
		for source in source_frames {
			if let Some(entries) = storage_multirow_index_entries_from_frame(&source.frame)? {
				let Some(matching_entry) =
					lsm_multirow_index_entry_for_version(&entries, &source.entry)
				else {
					return Err(Error::new(
						ErrorKind::InvalidData,
						"LSM source frame does not contain planned multi-row survivor",
					));
				};
				validate_lsm_multirow_source_entry(&source.entry, matching_entry)?;
				let key = LsmSourceFrameKey::from(source.entry.location);
				match multirow_source_frames_by_key.entry(key) {
					std::collections::hash_map::Entry::Vacant(entry) => {
						entry.insert(LsmMultiRowSourceFrame {
							frame: source.frame.clone(),
							entries,
						});
					}
					std::collections::hash_map::Entry::Occupied(entry) => {
						let existing = entry.get();
						if existing.frame != source.frame || existing.entries != entries {
							return Err(Error::new(
								ErrorKind::InvalidData,
								"inconsistent LSM source frames for the same physical record",
							));
						}
					}
				}
			} else if source_frames_by_location
				.insert(source.entry.location, source)
				.is_some()
			{
				return Err(Error::new(
					ErrorKind::InvalidInput,
					"duplicate LSM source frame location",
				));
			}
		}

		let mut written: Vec<LsmWrittenOutputRun> = Vec::with_capacity(prepared.output_runs.len());
		for run in &prepared.output_runs {
			if run.compaction_level != plan.target_level {
				return Err(Error::new(
					ErrorKind::InvalidInput,
					"planned LSM output run target level does not match compaction plan",
				));
			}

			let temp_path = lsm_compaction_tmp_path(&self.config.dir, plan.job_id, run.slab_id);
			let _ = unlink_local_file(self.file_ops.as_ref(), &temp_path).await;
			let output = match LocalSlabStore::open(
				&temp_path,
				run.slab_id,
				self.file_ops.clone(),
				self.config.group_commit.clone(),
			)
			.await
			{
				Ok(output) => Arc::new(output),
				Err(err) => {
					for previous in &written {
						let _ =
							unlink_local_file(self.file_ops.as_ref(), &previous.temp_path).await;
					}
					return Err(err);
				}
			};
			let write_result: std::io::Result<Vec<RecordLocation>> = async {
				let mut locations = vec![None; run.rows.len()];
				let mut written_multirow_keys = HashSet::new();
				for (row_index, row) in run.rows.iter().enumerate() {
					let multirow_key = LsmSourceFrameKey::from(row.location);
					if let Some(source) = multirow_source_frames_by_key.get(&multirow_key) {
						if !written_multirow_keys.insert(multirow_key) {
							continue;
						}
						let run_rows_for_source = run
							.rows
							.iter()
							.enumerate()
							.filter(|(_, candidate)| {
								LsmSourceFrameKey::from(candidate.location) == multirow_key
							})
							.collect::<Vec<_>>();
						if run_rows_for_source.len() != source.entries.len() {
							return Err(Error::new(
								ErrorKind::Unsupported,
								"LSM temp-output writer does not yet split multi-row frames",
							));
						}
						for entry in &source.entries {
							if !run_rows_for_source.iter().any(|(_, candidate)| {
								lsm_multirow_index_entry_matches_version(entry, candidate)
							}) {
								return Err(Error::new(
									ErrorKind::Unsupported,
									"LSM temp-output writer does not yet partially rewrite multi-row frames",
								));
							}
						}
						let Some(primary) = source.entries.first() else {
							return Err(Error::new(
								ErrorKind::InvalidData,
								"LSM multi-row source frame has no index entries",
							));
						};
						let record_hlc = source
							.entries
							.iter()
							.map(|entry| entry.row_hlc)
							.max()
							.unwrap_or(primary.row_hlc);
						let base_location = output
							.append(
								row.tenant_id,
								primary.key_identity,
								record_hlc,
								&source.frame,
							)
							.await?;
						output.rollback_index_entry(
							row.tenant_id,
							primary.key_identity,
							base_location,
						);
						let mut locations_by_row_index = HashMap::new();
						for entry in &source.entries {
							let new_location = RecordLocation {
								hlc: entry.row_hlc,
								record_row_index: entry.record_row_index,
								is_tombstone: entry.is_tombstone,
								..base_location
							};
							output.index_location(row.tenant_id, entry.key_identity, new_location);
							locations_by_row_index.insert(entry.record_row_index, new_location);
						}
						for (position, candidate) in run_rows_for_source {
							let Some(new_location) = locations_by_row_index
								.get(&candidate.record_row_index)
								.copied()
							else {
								return Err(Error::new(
									ErrorKind::InvalidData,
									"LSM multi-row output location missing planned row",
								));
							};
							locations[position] = Some(new_location);
						}
						continue;
					}

					let source = source_frames_by_location
						.get(&row.location)
						.ok_or_else(|| {
							Error::new(
								ErrorKind::InvalidInput,
								"missing LSM source frame for planned survivor",
							)
						})?;
					if source.entry != *row {
						return Err(Error::new(
							ErrorKind::InvalidInput,
							"LSM source frame entry does not match planned survivor",
						));
					}
					if storage_multirow_index_entries_from_frame(&source.frame)?.is_some() {
						return Err(Error::new(
							ErrorKind::Unsupported,
							"LSM temp-output writer does not yet split multi-row frames",
						));
					}
					let base_location = output
						.append(row.tenant_id, row.row_key_id, row.row_hlc, &source.frame)
						.await?;
					output.rollback_index_entry(row.tenant_id, row.row_key_id, base_location);
					let new_location = RecordLocation {
						hlc: row.row_hlc,
						record_row_index: row.record_row_index,
						is_tombstone: row.location.is_tombstone,
						..base_location
					};
					output.index_location(row.tenant_id, row.row_key_id, new_location);
					locations[row_index] = Some(new_location);
				}
				let locations = locations
					.into_iter()
					.collect::<Option<Vec<_>>>()
					.ok_or_else(|| {
						Error::new(
							ErrorKind::InvalidData,
							"LSM output writer did not materialize every planned survivor",
						)
					})?;
				output.fsync().await?;
				fsync_parent_dir(self.file_ops.as_ref(), &temp_path).await?;
				Ok(locations)
			}
			.await;

			let locations = match write_result {
				Ok(locations) => locations,
				Err(err) => {
					let _ = unlink_local_file(self.file_ops.as_ref(), &temp_path).await;
					for previous in &written {
						let _ =
							unlink_local_file(self.file_ops.as_ref(), &previous.temp_path).await;
					}
					return Err(err);
				}
			};
			let mut summary = run.summary(plan.manifest_epoch_pre);
			summary.size_bytes = ByteCount::new(output.write_cursor());
			written.push(LsmWrittenOutputRun {
				slab_id: run.slab_id,
				compaction_level: run.compaction_level,
				temp_path,
				summary,
				locations,
			});
		}
		Ok(written)
	}

	pub async fn sweep_lsm_delete_pending_once(
		&self,
		gc_worker: &LsmGcWorker,
	) -> std::io::Result<Vec<SlabId>> {
		let mut delete_pending_transition = None;
		{
			let mut manifest =
				recover_rwlock_write(&self.compaction_manifest, "compaction_manifest");
			let outcome = gc_worker
				.transition_retired_to_delete_pending_if(&mut manifest, |slab_id| {
					self.record_rows_for_slab(slab_id).is_empty()
				});
			if !outcome.marked_delete_pending.is_empty() {
				delete_pending_transition = Some(manifest.snapshot());
			}
		}
		if let Some(snapshot) = delete_pending_transition {
			self.mirror_persistent_update(
				PersistentIndexUpdate {
					put_lsm_runs: snapshot.runs().to_vec(),
					..Default::default()
				},
				"lsm-gc-mark-delete-pending",
			)
			.await;
		}

		let mut candidates = self
			.compaction_manifest_snapshot()
			.delete_pending_runs()
			.map(|run| run.slab_id)
			.collect::<Vec<_>>();
		candidates.sort_unstable();
		let mut deleted = Vec::new();
		for slab_id in candidates {
			if self.slab_pins.pin_count(slab_id) != 0 {
				continue;
			}
			for info in self.list_secondary_index_segments_for_source_slab(slab_id) {
				self.retire_secondary_index_segment_for_replaced_slab(&info)
					.await?;
			}
			{
				let mut state = self.state.lock().await;
				let _old = state.sealed.remove(&slab_id);
				let _old = state.rehydrated.remove(&slab_id);
				state.hot_read_counts.remove(&slab_id);
				state.rehydrating.remove(&slab_id);
			}
			let path = slab_path(&self.config.dir, slab_id);
			unlink_local_file(self.file_ops.as_ref(), &path).await?;
			let snapshot = {
				let mut manifest =
					recover_rwlock_write(&self.compaction_manifest, "compaction_manifest");
				gc_worker.complete_delete(&mut manifest, slab_id);
				manifest.snapshot()
			};
			refresh_lsm_level_metrics(&snapshot);
			self.mirror_persistent_update(
				PersistentIndexUpdate {
					delete_lsm_run_ids: vec![slab_id],
					..Default::default()
				},
				"lsm-gc-delete-run",
			)
			.await;
			deleted.push(slab_id);
		}
		Ok(deleted)
	}

	pub async fn sweep_lsm_delete_pending_once_via_control_ring(
		&self,
		gc_worker: &LsmGcWorker,
		committer: &dyn TierCommitter,
	) -> std::io::Result<Vec<SlabId>> {
		let snapshot = self.compaction_manifest_snapshot();
		let sweep_plan = gc_worker.plan_sweep(&snapshot);
		let mut to_mark_delete_pending = Vec::new();
		for slab_id in sweep_plan.retired_unpinned_slabs {
			if !self.record_rows_for_slab(slab_id).is_empty() {
				continue;
			}
			if self.slab_pins.mark_delete_pending(slab_id) {
				to_mark_delete_pending.push(slab_id);
			}
		}

		if !to_mark_delete_pending.is_empty() {
			let commit = CompactionCommit::gc_mark_delete_pending(
				Self::lsm_gc_job_id(snapshot.manifest_epoch, 0xD1, &to_mark_delete_pending),
				snapshot.manifest_epoch,
				to_mark_delete_pending,
			);
			committer.commit_lsm_compaction(commit).await?;
			let snapshot = self.compaction_manifest_snapshot();
			gc_worker.observe_manifest_snapshot(&snapshot);
			self.mirror_persistent_update(
				PersistentIndexUpdate {
					put_lsm_runs: snapshot.runs().to_vec(),
					..Default::default()
				},
				"lsm-gc-mark-delete-pending-control-ring",
			)
			.await;
		}

		let mut candidates = self
			.compaction_manifest_snapshot()
			.delete_pending_runs()
			.map(|run| run.slab_id)
			.collect::<Vec<_>>();
		candidates.sort_unstable();
		let mut deleted = Vec::new();
		for slab_id in candidates {
			if self.slab_pins.pin_count(slab_id) != 0 {
				continue;
			}
			for info in self.list_secondary_index_segments_for_source_slab(slab_id) {
				self.retire_secondary_index_segment_for_replaced_slab(&info)
					.await?;
			}
			{
				let mut state = self.state.lock().await;
				let _old = state.sealed.remove(&slab_id);
				let _old = state.rehydrated.remove(&slab_id);
				state.hot_read_counts.remove(&slab_id);
				state.rehydrating.remove(&slab_id);
			}
			let path = slab_path(&self.config.dir, slab_id);
			unlink_local_file(self.file_ops.as_ref(), &path).await?;
			deleted.push(slab_id);
		}

		if !deleted.is_empty() {
			let snapshot = self.compaction_manifest_snapshot();
			let commit = CompactionCommit::gc_remove(
				Self::lsm_gc_job_id(snapshot.manifest_epoch, 0xD2, &deleted),
				snapshot.manifest_epoch,
				deleted.clone(),
			);
			committer.commit_lsm_compaction(commit).await?;
			let snapshot = self.compaction_manifest_snapshot();
			gc_worker.observe_manifest_snapshot(&snapshot);
			self.mirror_persistent_update(
				PersistentIndexUpdate {
					delete_lsm_run_ids: deleted.clone(),
					..Default::default()
				},
				"lsm-gc-delete-run-control-ring",
			)
			.await;
		}
		Ok(deleted)
	}

	fn lsm_gc_job_id(
		manifest_epoch: ManifestEpoch,
		discriminator: u64,
		slab_ids: &[SlabId],
	) -> CompactionJobId {
		let mut value = 0xA55A_0550_0000_0000_u64 ^ discriminator ^ manifest_epoch.get();
		for slab_id in slab_ids {
			value = value.rotate_left(7) ^ slab_id.get();
		}
		CompactionJobId::new(value)
	}

	fn pin_lsm_read_location(&self, location: RecordLocation) -> std::io::Result<SlabPinGuard> {
		self.slab_pins.acquire(&[location.slab_id]).map_err(|_| {
			Error::new(
				ErrorKind::NotFound,
				format!("slab {} is delete-pending for LSM GC", location.slab_id),
			)
		})
	}

	/// Snapshot of every tiered slab that currently also has a
	/// local rehydrated L1 copy. Used by the sweeper to apply the
	/// residency cooldown before reclaiming those cache copies.
	pub async fn list_rehydrated_slabs(&self) -> Vec<RehydratedSlabInfo> {
		let state = self.state.lock().await;
		let mut out = Vec::with_capacity(state.rehydrated.len());
		for entry in state.rehydrated.iter() {
			out.push(RehydratedSlabInfo {
				slab_id: *entry.key(),
				size_bytes: entry.value().slab.write_cursor(),
				keep_local_until: entry.value().keep_local_until,
			});
		}
		out.sort_by_key(|s| s.slab_id);
		out
	}

	/// Total bytes currently retained in local L1 slabs.
	///
	/// Counts the active slab plus every sealed slab that still
	/// lives on local disk. Tiered slabs are excluded because their
	/// authoritative bytes already moved to L2.
	pub async fn local_l1_bytes(&self) -> u64 {
		let state = self.state.lock().await;
		let mut total = state.active.write_cursor();
		for entry in state.sealed.iter() {
			total = total.saturating_add(entry.value().write_cursor());
		}
		for entry in state.rehydrated.iter() {
			total = total.saturating_add(entry.value().slab.write_cursor());
		}
		total
	}

	/// Read every byte of a sealed L1 slab in one shot. Used by
	/// the sweeper to build the payload it ships to L2 and to
	/// compute the blake3 over that payload for the TierSlab
	/// Raft commit.
	///
	/// Returns `NotFound` if the slab is either absent, still
	/// active, or already tiered — all three are bugs on the
	/// caller side.
	pub async fn read_sealed_raw(&self, slab_id: SlabId) -> std::io::Result<Vec<u8>> {
		{
			let state = self.state.lock().await;
			if !state.sealed.contains_key(&slab_id) {
				return Err(Error::new(
					ErrorKind::NotFound,
					format!("slab {slab_id} is not in the sealed set"),
				));
			}
		}
		let path = slab_path(&self.config.dir, slab_id);
		self.file_ops.read_all(&path).await
	}

	pub async fn ensure_secondary_index_coverage(
		&self,
		tenant_id: TenantId,
		descriptor: &SecondaryIndexDescriptor,
		max_buffer_bytes: usize,
	) -> std::io::Result<SecondaryIndexBackfillPlan> {
		let identity = SecondaryIndexIdentity::from_descriptor(tenant_id, descriptor);
		let key = SecondaryIndexCoverageKey::from(identity);
		let (active_slab_id, historical_slabs) = self.secondary_index_source_slabs().await;
		let mut tracker = self
			.secondary_index_coverage
			.get(&key)
			.map(|entry| entry.value().clone())
			.unwrap_or_else(|| SecondaryIndexCoverageTracker::new(key));

		for row in self.secondary_index_segments.iter() {
			if secondary_index_coverage_key_from_persistent(&row.key) == key {
				tracker.mark_covered(row.key.source_slab_id);
			}
		}
		if tracker.active_buffer().is_none() {
			tracker.mark_active(SecondaryIndexActiveBuffer::new(
				identity,
				active_slab_id,
				max_buffer_bytes,
			))?;
		}

		let active_buffer_slab = tracker
			.active_buffer()
			.map(SecondaryIndexActiveBuffer::source_slab_id);
		let uncovered_slabs = tracker
			.uncovered_slabs()
			.map(|(slab_id, _error)| slab_id)
			.collect::<BTreeSet<_>>();
		let candidates = historical_slabs
			.into_iter()
			.filter(|slab_id| !tracker.is_covered(*slab_id))
			.filter(|slab_id| Some(*slab_id) != active_buffer_slab)
			.filter(|slab_id| !uncovered_slabs.contains(slab_id))
			.map(|source_slab_id| SecondaryIndexBackfillCandidate {
				key,
				source_slab_id,
			})
			.collect::<Vec<_>>();
		let snapshot = tracker.snapshot();
		self.secondary_index_coverage.insert(key, tracker);
		Ok(SecondaryIndexBackfillPlan {
			snapshot,
			candidates,
		})
	}

	pub fn secondary_index_coverage_snapshot(
		&self,
		key: SecondaryIndexCoverageKey,
	) -> Option<SecondaryIndexCoverageSnapshot> {
		self.secondary_index_coverage
			.get(&key)
			.map(|entry| entry.value().snapshot())
	}

	pub async fn secondary_index_tail_source_slabs(
		&self,
		tenant_id: TenantId,
		descriptor: &SecondaryIndexDescriptor,
	) -> BTreeSet<SlabId> {
		let identity = SecondaryIndexIdentity::from_descriptor(tenant_id, descriptor);
		let key = SecondaryIndexCoverageKey::from(identity);
		let (active_slab_id, historical_slabs) = self.secondary_index_source_slabs().await;
		let tracker = self
			.secondary_index_coverage
			.get(&key)
			.map(|entry| entry.value().clone());
		let covered_slabs = tracker
			.as_ref()
			.map(|tracker| tracker.covered_slabs().collect::<BTreeSet<_>>())
			.unwrap_or_default();
		let mut tail = BTreeSet::new();
		// The active slab can receive more writes after any buffered
		// entries are sealed, so it remains a scan tail until rotation.
		tail.insert(active_slab_id);
		for source_slab_id in historical_slabs {
			if !covered_slabs.contains(&source_slab_id) {
				tail.insert(source_slab_id);
			}
		}
		if let Some(tracker) = tracker {
			if let Some(buffer) = tracker.active_buffer() {
				tail.insert(buffer.source_slab_id());
			}
			tail.extend(
				tracker
					.uncovered_slabs()
					.map(|(source_slab_id, _)| source_slab_id),
			);
		}
		tail
	}

	pub fn list_secondary_index_coverage_snapshots(&self) -> Vec<SecondaryIndexCoverageSnapshot> {
		let mut out = self
			.secondary_index_coverage
			.iter()
			.map(|entry| entry.value().snapshot())
			.collect::<Vec<_>>();
		out.sort_by_key(|snapshot| {
			(
				snapshot.key.tenant_id,
				snapshot.key.table_object_id,
				snapshot.key.index_object_id,
			)
		});
		out
	}

	pub fn append_secondary_index_active_buffer_batch(
		&self,
		tenant_id: TenantId,
		descriptor: &SecondaryIndexDescriptor,
		batch: &RecordBatch,
		base_location: RecordLocation,
		max_buffer_bytes: usize,
	) -> std::io::Result<usize> {
		let identity = SecondaryIndexIdentity::from_descriptor(tenant_id, descriptor);
		let key = SecondaryIndexCoverageKey::from(identity);
		let mut tracker = self
			.secondary_index_coverage
			.entry(key)
			.or_insert_with(|| SecondaryIndexCoverageTracker::new(key));
		if tracker
			.active_buffer()
			.is_some_and(|buffer| buffer.source_slab_id() != base_location.slab_id)
		{
			let old_slab_id = tracker
				.active_buffer()
				.expect("checked Some above")
				.source_slab_id();
			tracker.mark_uncovered(
				old_slab_id,
				SecondaryIndexBackfillError::new(
					SecondaryIndexBackfillErrorReason::SealFailure,
					"active buffer moved to a different slab before seal",
				),
			);
		}
		if tracker.active_buffer().is_none() {
			tracker.mark_active(SecondaryIndexActiveBuffer::new(
				identity,
				base_location.slab_id,
				max_buffer_bytes,
			))?;
		}
		let result = tracker
			.active_buffer_mut()
			.expect("active buffer installed above")
			.append_storage_batch(descriptor, batch, base_location);
		if let Err(err) = result {
			let failure = tracker
				.active_buffer()
				.and_then(|buffer| buffer.failure().cloned())
				.unwrap_or_else(|| {
					SecondaryIndexBackfillError::new(
						SecondaryIndexBackfillErrorReason::EncodeFailure,
						err.to_string(),
					)
				});
			metrics().observe_storage_secondary_index_active_buffer_failure(
				tenant_id,
				descriptor.table_object_id,
				descriptor.index_object_id,
				failure.reason.label(),
			);
			tracker.mark_uncovered(base_location.slab_id, failure);
			return Err(err);
		}
		result
	}

	pub fn append_secondary_index_active_buffer_entries(
		&self,
		tenant_id: TenantId,
		descriptor: &SecondaryIndexDescriptor,
		entries: Vec<SecondaryIndexEntry>,
		source_slab_id: SlabId,
		max_buffer_bytes: usize,
	) -> std::io::Result<usize> {
		let identity = SecondaryIndexIdentity::from_descriptor(tenant_id, descriptor);
		let key = SecondaryIndexCoverageKey::from(identity);
		let mut tracker = self
			.secondary_index_coverage
			.entry(key)
			.or_insert_with(|| SecondaryIndexCoverageTracker::new(key));
		if tracker
			.active_buffer()
			.is_some_and(|buffer| buffer.source_slab_id() != source_slab_id)
		{
			let old_slab_id = tracker
				.active_buffer()
				.expect("checked Some above")
				.source_slab_id();
			tracker.mark_uncovered(
				old_slab_id,
				SecondaryIndexBackfillError::new(
					SecondaryIndexBackfillErrorReason::SealFailure,
					"active buffer moved to a different slab before seal",
				),
			);
		}
		if tracker.active_buffer().is_none() {
			tracker.mark_active(SecondaryIndexActiveBuffer::new(
				identity,
				source_slab_id,
				max_buffer_bytes,
			))?;
		}
		let result = tracker
			.active_buffer_mut()
			.expect("active buffer installed above")
			.append_entries(descriptor, entries, source_slab_id);
		if let Err(err) = result {
			let failure = tracker
				.active_buffer()
				.and_then(|buffer| buffer.failure().cloned())
				.unwrap_or_else(|| {
					SecondaryIndexBackfillError::new(
						SecondaryIndexBackfillErrorReason::EncodeFailure,
						err.to_string(),
					)
				});
			metrics().observe_storage_secondary_index_active_buffer_failure(
				tenant_id,
				descriptor.table_object_id,
				descriptor.index_object_id,
				failure.reason.label(),
			);
			tracker.mark_uncovered(source_slab_id, failure);
			return Err(err);
		}
		result
	}

	pub async fn seal_secondary_index_active_buffer(
		&self,
		tenant_id: TenantId,
		descriptor: &SecondaryIndexDescriptor,
	) -> std::io::Result<Option<SecondaryIndexSegmentInfo>> {
		let identity = SecondaryIndexIdentity::from_descriptor(tenant_id, descriptor);
		let key = SecondaryIndexCoverageKey::from(identity);
		let buffer = {
			let mut tracker = self
				.secondary_index_coverage
				.entry(key)
				.or_insert_with(|| SecondaryIndexCoverageTracker::new(key));
			tracker.take_active_buffer()
		};
		let Some(buffer) = buffer else {
			return Ok(None);
		};
		let source_slab_id = buffer.source_slab_id();
		let segment = match buffer.seal(descriptor) {
			Ok(segment) => segment,
			Err(err) => {
				self.mark_secondary_index_uncovered(
					key,
					source_slab_id,
					SecondaryIndexBackfillError::new(
						SecondaryIndexBackfillErrorReason::SealFailure,
						err.to_string(),
					),
				);
				metrics().observe_storage_secondary_index_active_buffer_failure(
					tenant_id,
					descriptor.table_object_id,
					descriptor.index_object_id,
					SecondaryIndexBackfillErrorReason::SealFailure.label(),
				);
				return Err(err);
			}
		};
		match self.persist_secondary_index_segment(&segment).await {
			Ok(info) => Ok(Some(info)),
			Err(err) => {
				self.mark_secondary_index_uncovered(
					key,
					source_slab_id,
					SecondaryIndexBackfillError::new(
						SecondaryIndexBackfillErrorReason::PersistFailure,
						err.to_string(),
					),
				);
				metrics().observe_storage_secondary_index_segment_failed(
					tenant_id,
					descriptor.table_object_id,
					descriptor.index_object_id,
					SecondaryIndexBackfillErrorReason::PersistFailure.label(),
				);
				Err(err)
			}
		}
	}

	pub async fn seal_secondary_index_active_buffer_if_source_changed(
		&self,
		tenant_id: TenantId,
		descriptor: &SecondaryIndexDescriptor,
		next_source_slab_id: SlabId,
	) -> std::io::Result<Option<SecondaryIndexSegmentInfo>> {
		let identity = SecondaryIndexIdentity::from_descriptor(tenant_id, descriptor);
		let key = SecondaryIndexCoverageKey::from(identity);
		let should_seal = self
			.secondary_index_coverage
			.get(&key)
			.and_then(|tracker| {
				tracker
					.value()
					.active_buffer()
					.map(|buffer| buffer.source_slab_id() != next_source_slab_id)
			})
			.unwrap_or(false);
		if should_seal {
			self.seal_secondary_index_active_buffer(tenant_id, descriptor)
				.await
		} else {
			Ok(None)
		}
	}

	pub async fn backfill_secondary_index_source_slab(
		&self,
		tenant_id: TenantId,
		table: &TableSchemaDescriptorV1,
		descriptor: &SecondaryIndexDescriptor,
		source_slab_id: SlabId,
	) -> std::io::Result<SecondaryIndexSegmentInfo> {
		self.backfill_secondary_index_source_slab_inner(
			tenant_id,
			table,
			descriptor,
			source_slab_id,
		)
		.await
	}

	pub async fn backfill_secondary_index_candidate(
		&self,
		tenant_id: TenantId,
		table: &TableSchemaDescriptorV1,
		descriptor: &SecondaryIndexDescriptor,
		candidate: &SecondaryIndexBackfillCandidate,
	) -> std::io::Result<SecondaryIndexSegmentInfo> {
		let expected_key = SecondaryIndexCoverageKey::from(
			SecondaryIndexIdentity::from_descriptor(tenant_id, descriptor),
		);
		if candidate.key != expected_key {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"secondary index backfill candidate does not match descriptor identity",
			));
		}
		self.backfill_secondary_index_source_slab_inner(
			tenant_id,
			table,
			descriptor,
			candidate.source_slab_id,
		)
		.await
	}

	pub async fn run_secondary_index_backfill_once(
		&self,
		tenant_id: TenantId,
		table: &TableSchemaDescriptorV1,
		descriptor: &SecondaryIndexDescriptor,
		max_slabs: usize,
		max_buffer_bytes: usize,
	) -> std::io::Result<SecondaryIndexBackfillTickReport> {
		let before = self
			.ensure_secondary_index_coverage(tenant_id, descriptor, max_buffer_bytes)
			.await?;
		let mut completed = Vec::new();
		let mut failures = Vec::new();

		for candidate in before.candidates.iter().take(max_slabs) {
			match self
				.backfill_secondary_index_candidate(tenant_id, table, descriptor, candidate)
				.await
			{
				Ok(info) => completed.push(SecondaryIndexBackfillSuccess {
					candidate: candidate.clone(),
					entry_count: info.entry_count,
				}),
				Err(err) => failures.push(SecondaryIndexBackfillFailure {
					candidate: candidate.clone(),
					error: SecondaryIndexBackfillError::new(
						secondary_index_backfill_reason_from_io(&err),
						err.to_string(),
					),
				}),
			}
		}

		let after = self
			.ensure_secondary_index_coverage(tenant_id, descriptor, max_buffer_bytes)
			.await?;
		let attempted = completed.len() + failures.len();
		let historical_complete =
			after.candidates.is_empty() && after.snapshot.uncovered_slabs.is_empty();

		Ok(SecondaryIndexBackfillTickReport {
			before,
			after,
			attempted,
			completed,
			failures,
			historical_complete,
		})
	}

	async fn backfill_secondary_index_source_slab_inner(
		&self,
		tenant_id: TenantId,
		table: &TableSchemaDescriptorV1,
		descriptor: &SecondaryIndexDescriptor,
		source_slab_id: SlabId,
	) -> std::io::Result<SecondaryIndexSegmentInfo> {
		let started_at = Instant::now();
		descriptor.validate_against_table(table)?;
		let key = SecondaryIndexCoverageKey::from(SecondaryIndexIdentity::from_descriptor(
			tenant_id, descriptor,
		));
		let source_slab_lease = self
			.acquire_secondary_index_source_slab_read_lease(source_slab_id)
			.await
			.map_err(|err| {
				self.mark_secondary_index_uncovered(
					key,
					source_slab_id,
					SecondaryIndexBackfillError::new(
						secondary_index_backfill_reason_from_io(&err),
						err.to_string(),
					),
				);
				err
			})?;
		let segment = match self
			.build_secondary_index_segment_for_source_slab(
				tenant_id,
				table,
				descriptor,
				source_slab_id,
			)
			.await
		{
			Ok(segment) => segment,
			Err(err) => {
				self.mark_secondary_index_uncovered(
					key,
					source_slab_id,
					SecondaryIndexBackfillError::new(
						secondary_index_backfill_reason_from_io(&err),
						err.to_string(),
					),
				);
				metrics().observe_storage_secondary_index_segment_failed(
					tenant_id,
					descriptor.table_object_id,
					descriptor.index_object_id,
					secondary_index_backfill_reason_from_io(&err).label(),
				);
				return Err(err);
			}
		};
		if let Err(err) = self
			.ensure_secondary_index_source_slab_available(source_slab_id)
			.await
		{
			self.mark_secondary_index_uncovered(
				key,
				source_slab_id,
				SecondaryIndexBackfillError::new(
					secondary_index_backfill_reason_from_io(&err),
					err.to_string(),
				),
			);
			metrics().observe_storage_secondary_index_segment_failed(
				tenant_id,
				descriptor.table_object_id,
				descriptor.index_object_id,
				secondary_index_backfill_reason_from_io(&err).label(),
			);
			return Err(err);
		}
		let result = match self.persist_secondary_index_segment(&segment).await {
			Ok(info) => {
				metrics().observe_storage_secondary_index_backfill_seconds(
					tenant_id,
					descriptor.table_object_id,
					descriptor.index_object_id,
					started_at.elapsed(),
				);
				Ok(info)
			}
			Err(err) => {
				self.mark_secondary_index_uncovered(
					key,
					source_slab_id,
					SecondaryIndexBackfillError::new(
						SecondaryIndexBackfillErrorReason::PersistFailure,
						err.to_string(),
					),
				);
				metrics().observe_storage_secondary_index_segment_failed(
					tenant_id,
					descriptor.table_object_id,
					descriptor.index_object_id,
					SecondaryIndexBackfillErrorReason::PersistFailure.label(),
				);
				Err(err)
			}
		};
		// Held until after sidecar persistence so compaction cannot
		// retire the source slab underneath a committed sidecar.
		// Explicitly name the guard for the ADR 052 read-lease grep.
		drop(source_slab_lease);
		result
	}

	pub fn mark_secondary_index_backfill_uncovered(
		&self,
		key: SecondaryIndexCoverageKey,
		source_slab_id: SlabId,
		error: SecondaryIndexBackfillError,
	) {
		self.mark_secondary_index_uncovered(key, source_slab_id, error);
	}

	pub async fn persist_secondary_index_segment(
		&self,
		segment: &SecondaryIndexSegment,
	) -> std::io::Result<SecondaryIndexSegmentInfo> {
		let identity = segment.identity();
		let segment_path = secondary_index_segment_storage_path(
			segment.header.tenant_id,
			segment.header.table_object_id,
			segment.header.index_object_id,
			segment.header.source_slab_id,
		);
		let info = SecondaryIndexSegmentInfo {
			tenant_id: segment.header.tenant_id,
			table_object_id: segment.header.table_object_id,
			index_object_id: segment.header.index_object_id,
			logical_schema_fingerprint: identity.logical_schema_fingerprint,
			source_slab_id: segment.header.source_slab_id,
			segment_path,
			l2_uri: None,
			payload_hash: segment.header.payload_hash,
			state_digest: *segment.header.state_digest.as_bytes(),
			format_version: segment.header.format_version,
			entry_count: segment.header.entry_count,
		};
		let row = PersistentSecondaryIndexSegmentRow::from(info.clone());
		let encoded = segment.encode()?;
		let path = secondary_index_sidecar_path(&self.config.dir, &info.segment_path)?;
		let tmp_path = secondary_index_sidecar_tmp_path(&path);
		if let Some(parent) = path.parent() {
			self.file_ops.create_dir_all(parent).await?;
		}
		let _ = unlink_local_file(self.file_ops.as_ref(), &tmp_path).await;
		self.file_ops.write_all(&tmp_path, &encoded).await?;
		let tmp = self
			.file_ops
			.open(
				&tmp_path,
				OpenFlags {
					read: true,
					write: true,
					create: false,
					create_new: false,
					truncate: false,
					direct: false,
				},
			)
			.await?;
		self.file_ops.fsync(&tmp).await?;
		self.file_ops.rename(&tmp_path, &path).await?;
		fsync_parent_dir(self.file_ops.as_ref(), &path).await?;
		if let Some(index) = &self.persistent_index {
			if let Err(err) = index.upsert_secondary_index_segment(row.clone()).await {
				let _ = unlink_local_file(self.file_ops.as_ref(), &path).await;
				return Err(err);
			}
		}
		self.secondary_index_segments.insert(row.key, row);
		self.mark_secondary_index_covered(info.coverage_key(), info.source_slab_id);
		metrics().observe_storage_secondary_index_segment_built(
			info.tenant_id,
			info.table_object_id,
			info.index_object_id,
			info.entry_count,
		);
		Ok(info)
	}

	pub async fn load_secondary_index_segment(
		&self,
		key: &SecondaryIndexSegmentInfo,
	) -> std::io::Result<Option<SecondaryIndexSegment>> {
		let Some(row) = self
			.secondary_index_segments
			.get(&key.key())
			.map(|entry| entry.value().clone())
		else {
			return Ok(None);
		};
		let path = secondary_index_sidecar_path(&self.config.dir, &row.segment_path)?;
		let bytes = match self.file_ops.read_all(&path).await {
			Ok(bytes) => bytes,
			Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
			Err(err) => return Err(err),
		};
		match SecondaryIndexSegment::decode(&bytes) {
			Ok(segment) => Ok(Some(segment)),
			Err(err) => {
				metrics().observe_storage_secondary_index_corruption_failure(
					row.key.tenant_id,
					row.key.table_object_id,
					row.key.index_object_id,
				);
				self.mark_secondary_index_uncovered(
					secondary_index_coverage_key_from_persistent(&row.key),
					row.key.source_slab_id,
					SecondaryIndexBackfillError::new(
						SecondaryIndexBackfillErrorReason::CorruptSegment,
						err.to_string(),
					),
				);
				Err(err)
			}
		}
	}

	pub async fn delete_secondary_index_segment(
		&self,
		key: &SecondaryIndexSegmentInfo,
	) -> std::io::Result<bool> {
		let Some(row) = self
			.secondary_index_segments
			.get(&key.key())
			.map(|entry| entry.value().clone())
		else {
			return Ok(false);
		};
		if let Some(index) = &self.persistent_index {
			index.delete_secondary_index_segment(row.key).await?;
		}
		let path = secondary_index_sidecar_path(&self.config.dir, &row.segment_path)?;
		unlink_local_file(self.file_ops.as_ref(), &path).await?;
		self.secondary_index_segments.remove(&row.key);
		self.mark_secondary_index_uncovered(
			secondary_index_coverage_key_from_persistent(&row.key),
			row.key.source_slab_id,
			SecondaryIndexBackfillError::new(
				SecondaryIndexBackfillErrorReason::MissingSegment,
				"secondary index sidecar segment deleted",
			),
		);
		Ok(true)
	}

	pub async fn gc_secondary_index_segments_for_dropping_index(
		&self,
		tenant_id: TenantId,
		descriptor: &SecondaryIndexDescriptor,
	) -> std::io::Result<usize> {
		let started_at = Instant::now();
		let segments = self.list_secondary_index_segments(
			tenant_id,
			descriptor.table_object_id,
			descriptor.index_object_id,
			descriptor.logical_schema_fingerprint,
		);
		let mut removed = 0usize;
		for info in segments {
			if self
				.retire_secondary_index_segment_for_replaced_slab(&info)
				.await?
			{
				removed = removed.saturating_add(1);
			}
		}
		let coverage_key = SecondaryIndexCoverageKey::from(
			SecondaryIndexIdentity::from_descriptor(tenant_id, descriptor),
		);
		self.secondary_index_coverage.remove(&coverage_key);
		metrics().set_storage_secondary_index_uncovered_slabs(
			tenant_id,
			descriptor.table_object_id,
			descriptor.index_object_id,
			0,
		);
		metrics().observe_storage_secondary_index_drop_gc_seconds(
			tenant_id,
			descriptor.table_object_id,
			descriptor.index_object_id,
			started_at.elapsed(),
		);
		Ok(removed)
	}

	pub fn list_secondary_index_segments(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		index_object_id: IndexObjectId,
		logical_schema_fingerprint: LogicalSchemaFingerprint,
	) -> Vec<SecondaryIndexSegmentInfo> {
		let mut out = self
			.secondary_index_segments
			.iter()
			.filter_map(|entry| {
				let key = entry.key();
				(key.tenant_id == tenant_id
					&& key.table_object_id == table_object_id
					&& key.index_object_id == index_object_id
					&& key.logical_schema_fingerprint == logical_schema_fingerprint)
					.then(|| SecondaryIndexSegmentInfo::from(entry.value().clone()))
			})
			.collect::<Vec<_>>();
		out.sort_by_key(|info| info.source_slab_id);
		out
	}

	fn list_secondary_index_segments_for_source_slab(
		&self,
		source_slab_id: SlabId,
	) -> Vec<SecondaryIndexSegmentInfo> {
		let mut out = self
			.secondary_index_segments
			.iter()
			.filter_map(|entry| {
				(entry.key().source_slab_id == source_slab_id)
					.then(|| SecondaryIndexSegmentInfo::from(entry.value().clone()))
			})
			.collect::<Vec<_>>();
		out.sort_by(|left, right| {
			(left.tenant_id, left.table_object_id, left.index_object_id)
				.cmp(&(
					right.tenant_id,
					right.table_object_id,
					right.index_object_id,
				))
				.then_with(|| {
					left.logical_schema_fingerprint
						.as_bytes()
						.cmp(right.logical_schema_fingerprint.as_bytes())
				})
		});
		out
	}

	fn forget_secondary_index_source_slab(
		&self,
		key: SecondaryIndexCoverageKey,
		source_slab_id: SlabId,
	) {
		if let Some(mut tracker) = self.secondary_index_coverage.get_mut(&key) {
			tracker.forget_slab(source_slab_id);
			let uncovered = tracker.uncovered_slabs().count() as u64;
			drop(tracker);
			self.observe_secondary_index_uncovered_slabs(key, uncovered);
		}
	}

	async fn retire_secondary_index_segment_for_replaced_slab(
		&self,
		info: &SecondaryIndexSegmentInfo,
	) -> std::io::Result<bool> {
		let Some(row) = self
			.secondary_index_segments
			.get(&info.key())
			.map(|entry| entry.value().clone())
		else {
			self.forget_secondary_index_source_slab(info.coverage_key(), info.source_slab_id);
			return Ok(false);
		};
		if let Some(index) = &self.persistent_index {
			index.delete_secondary_index_segment(row.key).await?;
		}
		let path = secondary_index_sidecar_path(&self.config.dir, &row.segment_path)?;
		unlink_local_file(self.file_ops.as_ref(), &path).await?;
		self.secondary_index_segments.remove(&row.key);
		self.forget_secondary_index_source_slab(info.coverage_key(), info.source_slab_id);
		Ok(true)
	}

	fn secondary_index_table_ids_from_plaintext(
		&self,
		source_slab_id: SlabId,
		plaintext: &[u8],
	) -> std::io::Result<BTreeSet<(TenantId, TableObjectId)>> {
		let mut table_ids = BTreeSet::new();
		for recovered in recover_slab_entries_from_blob(plaintext, source_slab_id)? {
			let frame = match extract_record_from_blob(plaintext, recovered.location) {
				Ok(frame) => frame,
				Err(_) => continue,
			};
			let storage_batch = match decode_record_batch(&frame) {
				Ok(batch) => batch,
				Err(_) => continue,
			};
			if let Some(table_object_id) =
				storage_row_table_object_id_at(&storage_batch, recovered.location.record_row_index)?
			{
				table_ids.insert((recovered.tenant_id, table_object_id));
			}
		}
		Ok(table_ids)
	}

	async fn secondary_index_compaction_descriptors_for_source_slab(
		&self,
		source_slab_id: SlabId,
		context: SecondaryIndexCompactionContext<'_>,
	) -> std::io::Result<Vec<(TenantId, TableSchemaDescriptorV1, SecondaryIndexDescriptor)>> {
		let plaintext = self
			.secondary_index_source_slab_plaintext(source_slab_id)
			.await?;
		self.secondary_index_compaction_descriptors_for_plaintext(
			source_slab_id,
			&plaintext,
			context,
		)
	}

	fn secondary_index_compaction_descriptors_for_plaintext(
		&self,
		source_slab_id: SlabId,
		plaintext: &[u8],
		context: SecondaryIndexCompactionContext<'_>,
	) -> std::io::Result<Vec<(TenantId, TableSchemaDescriptorV1, SecondaryIndexDescriptor)>> {
		let mut descriptors = Vec::new();
		for (tenant_id, table_object_id) in
			self.secondary_index_table_ids_from_plaintext(source_slab_id, plaintext)?
		{
			for row in context
				.secondary_indexes
				.for_table(tenant_id, table_object_id)
			{
				if !matches!(
					row.state,
					SecondaryIndexState::Building | SecondaryIndexState::Ready
				) {
					continue;
				}
				let Some(table) = context.table_schemas.get(
					tenant_id,
					row.descriptor.table_object_id,
					row.descriptor.table_schema_generation,
				) else {
					continue;
				};
				if row.descriptor.validate_against_table(table).is_ok() {
					descriptors.push((tenant_id, table.clone(), row.descriptor.clone()));
				}
			}
		}
		descriptors.sort_by_key(|(tenant_id, _table, descriptor)| {
			(
				*tenant_id,
				descriptor.table_object_id,
				descriptor.index_object_id,
			)
		});
		descriptors.dedup_by_key(|(tenant_id, _table, descriptor)| {
			(
				*tenant_id,
				descriptor.table_object_id,
				descriptor.index_object_id,
			)
		});
		Ok(descriptors)
	}

	async fn reconcile_secondary_index_segments_after_compaction(
		&self,
		old_slab_id: SlabId,
		new_slab_id: Option<SlabId>,
		context: Option<SecondaryIndexCompactionContext<'_>>,
		old_descriptors: Vec<(TenantId, TableSchemaDescriptorV1, SecondaryIndexDescriptor)>,
	) -> usize {
		let Some(context) = context else {
			return 0;
		};
		let mut retired_keys = HashSet::new();
		for old_info in self.list_secondary_index_segments_for_source_slab(old_slab_id) {
			let Some(row) = context
				.secondary_indexes
				.get(old_info.tenant_id, old_info.index_object_id)
			else {
				continue;
			};
			if !matches!(
				row.state,
				SecondaryIndexState::Building | SecondaryIndexState::Ready
			) {
				continue;
			}
			match self
				.retire_secondary_index_segment_for_replaced_slab(&old_info)
				.await
			{
				Ok(_) => {
					retired_keys.insert(old_info.coverage_key());
				}
				Err(err) => {
					tracing::warn!(
						old_slab_id = old_slab_id.get(),
						index_object_id = ?old_info.index_object_id,
						error = %err,
						"failed to retire old secondary index sidecar after compaction"
					);
				}
			}
		}
		for (tenant_id, _table, descriptor) in old_descriptors {
			let key = SecondaryIndexCoverageKey::from(SecondaryIndexIdentity::from_descriptor(
				tenant_id,
				&descriptor,
			));
			if retired_keys.insert(key) {
				self.forget_secondary_index_source_slab(key, old_slab_id);
			}
		}

		let Some(new_slab_id) = new_slab_id else {
			return 0;
		};
		let descriptors = match self
			.secondary_index_compaction_descriptors_for_source_slab(new_slab_id, context)
			.await
		{
			Ok(descriptors) => descriptors,
			Err(err) => {
				tracing::warn!(
					old_slab_id = old_slab_id.get(),
					new_slab_id = new_slab_id.get(),
					error = %err,
					"failed to enumerate secondary index descriptors for compacted slab"
				);
				return 0;
			}
		};

		let mut rebuilt = 0usize;
		for (tenant_id, table, descriptor) in descriptors {
			let key = SecondaryIndexCoverageKey::from(SecondaryIndexIdentity::from_descriptor(
				tenant_id,
				&descriptor,
			));
			if retired_keys.insert(key) {
				self.forget_secondary_index_source_slab(key, old_slab_id);
			}
			let old_info = self
				.list_secondary_index_segments(
					tenant_id,
					descriptor.table_object_id,
					descriptor.index_object_id,
					descriptor.logical_schema_fingerprint,
				)
				.into_iter()
				.find(|info| info.source_slab_id == old_slab_id);
			if let Some(old_info) = old_info
				&& let Err(err) = self
					.retire_secondary_index_segment_for_replaced_slab(&old_info)
					.await
			{
				tracing::warn!(
					old_slab_id = old_slab_id.get(),
					index_object_id = ?descriptor.index_object_id,
					error = %err,
					"failed to retire old secondary index sidecar before rebuild"
				);
			}
			let segment = match self
				.build_secondary_index_segment_for_source_slab(
					tenant_id,
					&table,
					&descriptor,
					new_slab_id,
				)
				.await
			{
				Ok(segment) => segment,
				Err(err) => {
					self.mark_secondary_index_uncovered(
						key,
						new_slab_id,
						SecondaryIndexBackfillError::new(
							secondary_index_backfill_reason_from_io(&err),
							err.to_string(),
						),
					);
					tracing::warn!(
						new_slab_id = new_slab_id.get(),
						index_object_id = ?descriptor.index_object_id,
						error = %err,
						"failed to rebuild secondary index sidecar after compaction"
					);
					continue;
				}
			};
			if let Err(err) = self.persist_secondary_index_segment(&segment).await {
				self.mark_secondary_index_uncovered(
					key,
					new_slab_id,
					SecondaryIndexBackfillError::new(
						SecondaryIndexBackfillErrorReason::PersistFailure,
						err.to_string(),
					),
				);
				tracing::warn!(
					new_slab_id = new_slab_id.get(),
					index_object_id = ?descriptor.index_object_id,
					error = %err,
					"failed to persist rebuilt secondary index sidecar after compaction"
				);
				continue;
			}
			rebuilt = rebuilt.saturating_add(1);
		}
		rebuilt
	}

	async fn prepare_lsm_secondary_index_segments_for_compaction(
		&self,
		plan: &CompactionPlan,
		written: &[LsmWrittenOutputRun],
		context: Option<SecondaryIndexCompactionContext<'_>>,
	) -> Result<Option<LsmPreparedSecondaryIndexSegments>, CompactionFailure> {
		let Some(context) = context else {
			return Ok(None);
		};

		let mut old_sidecars = Vec::new();
		let mut old_descriptors = Vec::new();
		for old_slab_id in plan.touched_slabs() {
			for old_info in self.list_secondary_index_segments_for_source_slab(old_slab_id) {
				old_sidecars.push(old_info);
			}
			for (tenant_id, _table, descriptor) in self
				.secondary_index_compaction_descriptors_for_source_slab(old_slab_id, context)
				.await?
			{
				old_descriptors.push((
					old_slab_id,
					SecondaryIndexCoverageKey::from(SecondaryIndexIdentity::from_descriptor(
						tenant_id,
						&descriptor,
					)),
				));
			}
		}

		let mut output_segments = Vec::new();
		for run in written {
			let path = slab_path(&self.config.dir, run.slab_id);
			let plaintext = self.file_ops.read_all(&path).await?;
			for (tenant_id, table, descriptor) in self
				.secondary_index_compaction_descriptors_for_plaintext(
					run.slab_id,
					&plaintext,
					context,
				)? {
				let key = SecondaryIndexCoverageKey::from(SecondaryIndexIdentity::from_descriptor(
					tenant_id,
					&descriptor,
				));
				let segment = self.build_secondary_index_segment_from_plaintext(
					tenant_id,
					&table,
					&descriptor,
					run.slab_id,
					&plaintext,
				)?;
				self.verify_lsm_secondary_index_segment_cold_rebuild(
					&segment,
					tenant_id,
					&table,
					&descriptor,
					run.slab_id,
					&plaintext,
				)?;
				output_segments.push(LsmPreparedSecondaryIndexSegment {
					key,
					output_slab_id: run.slab_id,
					index_object_id: descriptor.index_object_id,
					segment,
				});
			}
		}

		Ok(Some(LsmPreparedSecondaryIndexSegments {
			old_sidecars,
			old_descriptors,
			output_segments,
		}))
	}

	fn verify_lsm_secondary_index_segment_cold_rebuild(
		&self,
		segment: &SecondaryIndexSegment,
		tenant_id: TenantId,
		table: &TableSchemaDescriptorV1,
		descriptor: &SecondaryIndexDescriptor,
		source_slab_id: SlabId,
		plaintext: &[u8],
	) -> Result<(), CompactionFailure> {
		let cold_rebuild = self.build_secondary_index_segment_from_plaintext(
			tenant_id,
			table,
			descriptor,
			source_slab_id,
			plaintext,
		)?;
		if &cold_rebuild != segment {
			return Err(CompactionFailure::SidecarDigestMismatch);
		}
		Ok(())
	}

	async fn publish_prepared_lsm_secondary_index_segments(
		&self,
		prepared: Option<LsmPreparedSecondaryIndexSegments>,
	) -> usize {
		let Some(prepared) = prepared else {
			return 0;
		};
		let mut coverage_by_slab: BTreeMap<SlabId, (usize, usize)> = BTreeMap::new();

		for old_info in prepared.old_sidecars {
			if let Err(err) = self
				.retire_secondary_index_segment_for_replaced_slab(&old_info)
				.await
			{
				tracing::warn!(
					old_slab_id = old_info.source_slab_id.get(),
					index_object_id = ?old_info.index_object_id,
					error = %err,
					"failed to retire old secondary index sidecar after LSM compaction"
				);
			}
		}
		for (old_slab_id, key) in prepared.old_descriptors {
			self.forget_secondary_index_source_slab(key, old_slab_id);
		}

		let mut rebuilt = 0usize;
		for prepared_segment in prepared.output_segments {
			let coverage = coverage_by_slab
				.entry(prepared_segment.output_slab_id)
				.or_default();
			coverage.0 = coverage.0.saturating_add(1);
			if let Err(err) = self
				.persist_secondary_index_segment(&prepared_segment.segment)
				.await
			{
				self.mark_secondary_index_uncovered(
					prepared_segment.key,
					prepared_segment.output_slab_id,
					SecondaryIndexBackfillError::new(
						SecondaryIndexBackfillErrorReason::PersistFailure,
						err.to_string(),
					),
				);
				tracing::warn!(
					output_slab_id = prepared_segment.output_slab_id.get(),
					index_object_id = ?prepared_segment.index_object_id,
					error = %err,
					"failed to persist prepared secondary index sidecar after LSM compaction"
				);
				continue;
			}
			let coverage = coverage_by_slab
				.entry(prepared_segment.output_slab_id)
				.or_default();
			coverage.1 = coverage.1.saturating_add(1);
			rebuilt = rebuilt.saturating_add(1);
		}
		self.update_lsm_sidecar_coverage_after_publish(coverage_by_slab)
			.await;
		rebuilt
	}

	async fn update_lsm_sidecar_coverage_after_publish(
		&self,
		coverage_by_slab: BTreeMap<SlabId, (usize, usize)>,
	) {
		let updates = coverage_by_slab
			.into_iter()
			.filter_map(|(slab_id, (attempted, succeeded))| {
				if attempted == 0 {
					return None;
				}
				let coverage = if attempted == succeeded {
					crate::storage::SidecarCoverage::Complete
				} else {
					crate::storage::SidecarCoverage::Partial
				};
				Some((slab_id, coverage))
			})
			.collect::<Vec<_>>();
		if updates.is_empty() {
			return;
		}

		let lsm_runs = {
			let mut manifest =
				recover_rwlock_write(&self.compaction_manifest, "compaction_manifest");
			for (slab_id, coverage) in updates {
				let Some(run) = manifest.get_run(slab_id).cloned() else {
					continue;
				};
				if !run.state.is_live() || run.sidecar_coverage == coverage {
					continue;
				}
				let mut updated = run;
				updated.sidecar_coverage = coverage;
				manifest.insert_live_run(updated);
			}
			let snapshot = manifest.snapshot();
			drop(manifest);
			refresh_lsm_level_metrics(&snapshot);
			snapshot.runs().to_vec()
		};
		self.mirror_persistent_update(
			PersistentIndexUpdate {
				put_lsm_runs: lsm_runs,
				..Default::default()
			},
			"lsm-sidecar-coverage-update",
		)
		.await;
	}

	#[allow(dead_code)]
	async fn reconcile_lsm_secondary_index_segments_after_compaction(
		&self,
		plan: &CompactionPlan,
		written: &[LsmWrittenOutputRun],
		context: Option<SecondaryIndexCompactionContext<'_>>,
	) -> usize {
		match self
			.prepare_lsm_secondary_index_segments_for_compaction(plan, written, context)
			.await
		{
			Ok(prepared) => {
				self.publish_prepared_lsm_secondary_index_segments(prepared)
					.await
			}
			Err(err) => {
				tracing::warn!(
					error = %err,
					"failed to prepare secondary index sidecars after LSM compaction"
				);
				0
			}
		}
	}

	pub async fn table_lookup_by_index(
		&self,
		request: &TableLookupByIndexRequest,
		descriptor: &SecondaryIndexDescriptor,
		state: SecondaryIndexState,
	) -> std::io::Result<SecondaryIndexLookupReport> {
		if state != SecondaryIndexState::Ready
			|| descriptor.table_object_id != request.table_object_id
			|| descriptor.index_object_id != request.index_object_id
			|| descriptor.logical_schema_fingerprint != request.logical_schema_fingerprint
		{
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"{INDEX_REJECT_LOGICAL_FINGERPRINT_MISMATCH}: secondary index lookup requires a Ready descriptor matching the request",
				),
			));
		}

		let mut report = SecondaryIndexLookupReport::default();
		let mut seen = BTreeSet::new();
		for info in self.list_secondary_index_segments(
			request.tenant_id,
			request.table_object_id,
			request.index_object_id,
			request.logical_schema_fingerprint,
		) {
			let Some(segment) = self.load_secondary_index_segment(&info).await? else {
				return Err(Error::new(
					ErrorKind::NotFound,
					format!(
						"secondary index sidecar for source slab {} disappeared during lookup",
						info.source_slab_id
					),
				));
			};
			if !secondary_index_key_range_intersects_query(
				segment.header.entry_count,
				&segment.header.min_encoded_key,
				&segment.header.max_encoded_key,
				&request.query,
			) {
				continue;
			}
			report.stats.scanned_segments += 1;
			let segment_identity = segment.identity();
			for entry in segment
				.entries
				.iter()
				.filter(|entry| secondary_index_entry_matches_query(entry, &request.query))
			{
				report.stats.candidate_count += 1;
				let frame = self.read(entry.record_location).await?;
				let fetched_batch = decode_record_batch(&frame)?;
				let visible_as_of_location =
					self.lookup_as_of(request.tenant_id, entry.row_key_id, request.as_of_hlc);
				match revalidate_secondary_index_candidate(
					descriptor,
					segment_identity,
					entry,
					&fetched_batch,
					visible_as_of_location,
				)? {
					SecondaryIndexCandidateValidation::Accepted => {
						let key = (
							entry.row_key_id,
							entry.row_hlc,
							entry.record_location.slab_id,
							entry.record_location.offset,
							entry.record_location.length,
							entry.record_location.record_row_index,
						);
						if seen.insert(key) {
							report.stats.visible_count += 1;
							report.rows.push(SecondaryIndexLookupRow {
								row_key_id: entry.row_key_id,
								row_hlc: entry.row_hlc,
								record_location: entry.record_location,
							});
						}
					}
					SecondaryIndexCandidateValidation::Rejected(reason) => match reason {
						SecondaryIndexCandidateRejectReason::NotVisibleAsOf
						| SecondaryIndexCandidateRejectReason::Tombstone => {
							report.stats.superseded_count += 1;
						}
						SecondaryIndexCandidateRejectReason::NullIndexedValue
						| SecondaryIndexCandidateRejectReason::IndexedValueMismatch => {
							report.stats.stale_indexed_value_count += 1;
						}
						SecondaryIndexCandidateRejectReason::DescriptorMismatch
						| SecondaryIndexCandidateRejectReason::RowMetadataMismatch => {
							report.stats.rejected_count += 1;
						}
					},
				}
			}
		}
		metrics().observe_storage_secondary_index_lookup(
			request.tenant_id,
			request.table_object_id,
			request.index_object_id,
			report.stats.candidate_count,
			report.stats.visible_count,
			report.stats.superseded_count,
			report.stats.stale_indexed_value_count,
		);
		Ok(report)
	}

	async fn secondary_index_source_slabs(&self) -> (SlabId, Vec<SlabId>) {
		let state = self.state.lock().await;
		let active_slab_id = state.active.slab_id();
		let mut historical = state
			.sealed
			.iter()
			.map(|entry| *entry.key())
			.chain(state.tiered.iter().map(|entry| *entry.key()))
			.collect::<Vec<_>>();
		historical.sort_unstable();
		historical.dedup();
		(active_slab_id, historical)
	}

	fn secondary_index_source_slab_lease(&self, source_slab_id: SlabId) -> Arc<AsyncRwLock<()>> {
		self.secondary_index_source_slab_leases
			.entry(source_slab_id)
			.or_insert_with(|| Arc::new(AsyncRwLock::new(())))
			.clone()
	}

	async fn acquire_secondary_index_source_slab_read_lease(
		&self,
		source_slab_id: SlabId,
	) -> std::io::Result<OwnedRwLockReadGuard<()>> {
		let source_slab_lease = self.secondary_index_source_slab_lease(source_slab_id);
		let guard = source_slab_lease.read_owned().await;
		self.ensure_secondary_index_source_slab_available(source_slab_id)
			.await?;
		Ok(guard)
	}

	async fn acquire_secondary_index_source_slab_compaction_lease(
		&self,
		source_slab_id: SlabId,
	) -> OwnedRwLockWriteGuard<()> {
		self.secondary_index_source_slab_lease(source_slab_id)
			.write_owned()
			.await
	}

	async fn ensure_secondary_index_source_slab_available(
		&self,
		source_slab_id: SlabId,
	) -> std::io::Result<()> {
		let state = self.state.lock().await;
		if state.active.slab_id() == source_slab_id {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"secondary index backfill source slab is still active",
			));
		}
		if state.sealed.contains_key(&source_slab_id) || state.tiered.contains_key(&source_slab_id)
		{
			return Ok(());
		}
		Err(Error::new(
			ErrorKind::NotFound,
			format!("secondary index backfill source slab {source_slab_id} not found"),
		))
	}

	async fn build_secondary_index_segment_for_source_slab(
		&self,
		tenant_id: TenantId,
		table: &TableSchemaDescriptorV1,
		descriptor: &SecondaryIndexDescriptor,
		source_slab_id: SlabId,
	) -> std::io::Result<SecondaryIndexSegment> {
		let plaintext = self
			.secondary_index_source_slab_plaintext(source_slab_id)
			.await?;
		self.build_secondary_index_segment_from_plaintext(
			tenant_id,
			table,
			descriptor,
			source_slab_id,
			&plaintext,
		)
	}

	fn build_secondary_index_segment_from_plaintext(
		&self,
		tenant_id: TenantId,
		table: &TableSchemaDescriptorV1,
		descriptor: &SecondaryIndexDescriptor,
		source_slab_id: SlabId,
		plaintext: &[u8],
	) -> std::io::Result<SecondaryIndexSegment> {
		let identity = SecondaryIndexIdentity::from_descriptor(tenant_id, descriptor);
		let mut index_entries = Vec::new();
		for recovered in recover_slab_entries_from_blob(plaintext, source_slab_id)? {
			if recovered.tenant_id != tenant_id || recovered.location.is_tombstone {
				continue;
			}
			let frame = extract_record_from_blob(plaintext, recovered.location)?;
			let storage_batch = match decode_record_batch(&frame) {
				Ok(batch) => batch,
				Err(_) => continue,
			};
			let stored_table_object_id = storage_row_table_object_id_at(
				&storage_batch,
				recovered.location.record_row_index,
			)?;
			let materialized = match materialize_record_location_batch(
				&storage_batch,
				recovered.location.record_row_index,
			) {
				Ok(batch) if batch.num_rows() == 1 => batch,
				Ok(_) => continue,
				Err(err) => return Err(err),
			};
			let routing_locality = self.recovered_entry_routing_locality(&recovered);
			if !descriptor_matches_tiered_row(
				table,
				tenant_id,
				recovered.key_identity,
				routing_locality,
				stored_table_object_id,
				&materialized,
			)? {
				continue;
			}
			let Some(encoded_key) = encode_secondary_index_key(&materialized, 0, descriptor)?
			else {
				continue;
			};
			index_entries.push(SecondaryIndexEntry {
				encoded_key,
				row_key_id: recovered.key_identity,
				row_hlc: recovered.location.hlc,
				record_location: RecordLocation {
					is_tombstone: false,
					..recovered.location
				},
			});
		}
		SecondaryIndexSegment::build(
			identity,
			source_slab_id,
			descriptor.table_schema_generation,
			descriptor.key.arrow_type_tag,
			index_entries,
		)
	}

	async fn secondary_index_source_slab_plaintext(
		&self,
		source_slab_id: SlabId,
	) -> std::io::Result<Vec<u8>> {
		let tier_pointer = {
			let state = self.state.lock().await;
			if state.active.slab_id() == source_slab_id {
				return Err(Error::new(
					ErrorKind::InvalidInput,
					"secondary index backfill source slab is still active",
				));
			}
			if state.sealed.contains_key(&source_slab_id) {
				None
			} else if let Some(pointer) = state.tiered.get(&source_slab_id) {
				Some(pointer.clone())
			} else {
				return Err(Error::new(
					ErrorKind::NotFound,
					format!("secondary index backfill source slab {source_slab_id} not found"),
				));
			}
		};
		if let Some(pointer) = tier_pointer {
			self.tiered_plaintext_from_l2(&pointer).await
		} else {
			self.read_sealed_raw(source_slab_id).await
		}
	}

	fn mark_secondary_index_covered(&self, key: SecondaryIndexCoverageKey, source_slab_id: SlabId) {
		let mut tracker = self
			.secondary_index_coverage
			.entry(key)
			.or_insert_with(|| SecondaryIndexCoverageTracker::new(key));
		tracker.mark_covered(source_slab_id);
		let uncovered = tracker.uncovered_slabs().count() as u64;
		drop(tracker);
		self.observe_secondary_index_uncovered_slabs(key, uncovered);
	}

	fn mark_secondary_index_uncovered(
		&self,
		key: SecondaryIndexCoverageKey,
		source_slab_id: SlabId,
		error: SecondaryIndexBackfillError,
	) {
		let mut tracker = self
			.secondary_index_coverage
			.entry(key)
			.or_insert_with(|| SecondaryIndexCoverageTracker::new(key));
		tracker.mark_uncovered(source_slab_id, error);
		let uncovered = tracker.uncovered_slabs().count() as u64;
		drop(tracker);
		self.observe_secondary_index_uncovered_slabs(key, uncovered);
	}

	fn observe_secondary_index_uncovered_slabs(
		&self,
		key: SecondaryIndexCoverageKey,
		uncovered: u64,
	) {
		metrics().set_storage_secondary_index_uncovered_slabs(
			key.tenant_id,
			key.table_object_id,
			key.index_object_id,
			uncovered,
		);
	}

	/// Refresh the ADR 043 compression-convergence gauge from
	/// current slab contents and table descriptors.
	///
	/// The gauge counts uncompressed IPC frame bytes in sealed L1
	/// slabs and tiered slabs where the current table descriptor
	/// selects LZ4. Active slabs are excluded because their shape is
	/// still being written.
	///
	/// This is an expensive diagnostic refresh: tiered slabs are
	/// downloaded and decrypted to inspect their record bodies. Do
	/// not call it from a Prometheus scrape path. Prefer invoking it
	/// from explicit maintenance or compaction workflows.
	#[allow(dead_code)]
	pub(crate) async fn refresh_storage_compression_uncompressed_resident_bytes(
		&self,
		table_schemas: &TableSchemaCatalogV1,
	) -> std::io::Result<u64> {
		let (sealed_ids, tiered_pointers) = {
			let state = self.state.lock().await;
			let mut sealed_ids = state
				.sealed
				.iter()
				.map(|entry| *entry.key())
				.collect::<Vec<_>>();
			sealed_ids.sort_unstable();
			let mut tiered_pointers = state
				.tiered
				.iter()
				.map(|entry| entry.value().clone())
				.collect::<Vec<_>>();
			tiered_pointers.sort_by_key(|pointer| pointer.slab_id);
			(sealed_ids, tiered_pointers)
		};

		let mut bytes = 0_u64;
		for slab_id in sealed_ids {
			let blob = self.read_sealed_raw(slab_id).await?;
			bytes = bytes.saturating_add(uncompressed_resident_bytes_in_slab(
				&blob,
				slab_id,
				table_schemas,
			)?);
		}
		for pointer in tiered_pointers {
			let blob = self.tiered_plaintext_from_l2(&pointer).await?;
			bytes = bytes.saturating_add(uncompressed_resident_bytes_in_slab(
				&blob,
				pointer.slab_id,
				table_schemas,
			)?);
		}
		metrics().set_storage_compression_uncompressed_resident_bytes(bytes);
		Ok(bytes)
	}

	/// Encode a sealed slab into the ADR 022 L2 object layout.
	/// The current write path still assumes every sealed slab is
	/// single-tenant, so the footer and per-chunk AAD stay slab-scoped.
	pub fn encrypt_tiered_object_blob(
		&self,
		slab_id: SlabId,
		plaintext_blob: &[u8],
	) -> std::io::Result<EncryptedTieredSlab> {
		let metadata =
			build_tiered_object_metadata(plaintext_blob, slab_id, &self.config.l2_chunk_layout)?;
		self.config
			.crypto
			.encrypt_tiered_slab_object(slab_id, &metadata, plaintext_blob)
	}

	pub fn encrypt_tiered_object_blob_with_table_stats(
		&self,
		slab_id: SlabId,
		plaintext_blob: &[u8],
		table_schemas: &TableSchemaCatalogV1,
	) -> std::io::Result<EncryptedTieredSlab> {
		let mut metadata =
			build_tiered_object_metadata(plaintext_blob, slab_id, &self.config.l2_chunk_layout)?;
		metadata.column_stats_block = self.build_tiered_column_stats_block(
			plaintext_blob,
			slab_id,
			metadata.tenant_id,
			table_schemas,
		)?;
		self.config
			.crypto
			.encrypt_tiered_slab_object(slab_id, &metadata, plaintext_blob)
	}

	/// Detect whether a tiered object already carries an encrypted
	/// footer. Callers use this to distinguish legacy plaintext L2
	/// objects from ciphertext ones during migration.
	pub fn encrypted_tiered_footer_summary(
		&self,
		blob: &[u8],
	) -> std::io::Result<Option<FooterSummary>> {
		self.config.crypto.encrypted_footer_summary(blob)
	}

	pub(crate) fn decode_tiered_object_blob(
		&self,
		pointer: &TieredSlabPointer,
		blob: &[u8],
	) -> std::io::Result<Vec<u8>> {
		validate_tiered_blob(pointer, blob)?;
		decode_tiered_blob(self.config.crypto.as_ref(), pointer, blob)
	}

	#[allow(dead_code)]
	pub(crate) async fn load_tiered_column_stats_block(
		&self,
		pointer: &TieredSlabPointer,
		descriptor: &TableSchemaDescriptorV1,
	) -> std::io::Result<Option<ParsedColumnStatsBlock>> {
		metrics().pbc_slab_stats_consulted_total.inc();
		let Some(cache_key) = tiered_stats_cache_key(pointer, descriptor) else {
			metrics().pbc_slab_stats_missing_total.inc();
			return Ok(None);
		};
		if let Some(entry) = self.tiered_stats_cache.get(&cache_key) {
			metrics().pbc_slab_stats_cache_hits_total.inc();
			return Ok(Some(entry.value().clone()));
		}
		let Some(l2) = self.config.l2.as_ref() else {
			metrics().pbc_slab_stats_missing_total.inc();
			return Ok(None);
		};
		metrics().pbc_slab_stats_cache_misses_total.inc();
		let metadata = l2.head(&pointer.l2_uri).await?;
		let tail_start = metadata
			.size_bytes
			.saturating_sub(self.config.rehydration.tail_read_window_bytes);
		let tail_window = l2
			.get_range(&pointer.l2_uri, tail_start..metadata.size_bytes)
			.await?;
		let additional_metadata_range = match self.config.crypto.additional_tiered_metadata_range(
			pointer.slab_id,
			metadata.size_bytes,
			tail_window.as_ref(),
		) {
			Ok(range) => range,
			Err(error) => {
				metrics().pbc_slab_stats_decrypt_failed_total.inc();
				return Err(error);
			}
		};
		let additional_metadata = match additional_metadata_range {
			Some(range) => Some(l2.get_range(&pointer.l2_uri, range).await?),
			None => None,
		};
		let Some(plan) = self.config.crypto.plan_tiered_stats_block_read(
			pointer.slab_id,
			metadata.size_bytes,
			tail_window.as_ref(),
			additional_metadata.as_deref(),
		)?
		else {
			metrics().pbc_slab_stats_missing_total.inc();
			return Ok(None);
		};
		let locator = plan.locator();
		if pointer.stats_format_version != Some(locator.stats_format_version)
			|| pointer.stats_block_len != Some(locator.stats_block_len)
			|| pointer.stats_block_hash128 != Some(locator.stats_block_hash128)
		{
			metrics().pbc_slab_stats_pointer_drift_total.inc();
			return Ok(None);
		}
		let stats_ciphertext = l2
			.get_range(&pointer.l2_uri, plan.stats_range_in_object())
			.await?;
		let stats = plan.decrypt(stats_ciphertext.as_ref(), descriptor.schema_generation)?;
		let parsed = parse_column_stats_block(&stats.plaintext)?;
		let parsed_identity = ColumnStatsDescriptorIdentity::from_header(&parsed.header);
		let descriptor_identity = ColumnStatsDescriptorIdentity::from_descriptor(descriptor);
		if parsed_identity != descriptor_identity {
			if parsed.header.table_object_id == descriptor.table_object_id
				&& parsed.header.schema_generation == descriptor.schema_generation
				&& parsed.header.logical_schema_fingerprint != descriptor.logical_schema_fingerprint
			{
				if let Some(tenant_id) = pointer.tenant_id {
					metrics().observe_storage_csb_logical_fingerprint_mismatch(
						tenant_id,
						descriptor.table_object_id,
					);
					metrics().observe_storage_csb_pre_adr_051_cliff(
						tenant_id,
						descriptor.table_object_id,
					);
				}
			}
			metrics().pbc_slab_stats_pointer_drift_total.inc();
			return Ok(None);
		}
		self.tiered_stats_cache.insert(cache_key, parsed.clone());
		Ok(Some(parsed))
	}

	pub(crate) fn active_tenant_kek_id(&self, tenant_id: TenantId) -> std::io::Result<TenantKekId> {
		self.config.crypto.active_tenant_kek_id(tenant_id)
	}

	fn build_tiered_column_stats_block(
		&self,
		blob: &[u8],
		slab_id: SlabId,
		tenant_id: TenantId,
		table_schemas: &TableSchemaCatalogV1,
	) -> std::io::Result<Option<TieredColumnStatsBlock>> {
		let descriptors = table_schemas
			.descriptors_for_tenant(tenant_id)
			.into_iter()
			.filter(|descriptor| descriptor.table_object_id != TableObjectId::DEFAULT_KV)
			.collect::<Vec<_>>();
		if descriptors.is_empty() {
			return Ok(None);
		}
		let entries = recover_slab_entries_from_blob(blob, slab_id)?;
		let mut candidates: Option<Vec<TableSchemaDescriptorV1>> = None;
		let mut saw_unmatched_row = false;
		let mut row_batches = Vec::with_capacity(entries.len());

		for entry in entries {
			let record_tenant_id = entry.tenant_id;
			let key_identity = entry.key_identity;
			let routing_locality = self.recovered_entry_routing_locality(&entry);
			let location = entry.location;
			if record_tenant_id != tenant_id {
				return Err(Error::new(
					ErrorKind::InvalidInput,
					format!(
						"ADR 039 typed-table stats build saw tenant {} in slab {} after metadata chose tenant {}",
						record_tenant_id.get(),
						slab_id,
						tenant_id.get(),
					),
				));
			}
			let frame = extract_record_from_blob(blob, location)?;
			let (batch, stored_table_object_id) = match decode_record_batch(&frame) {
				Ok(decoded) => {
					// Phase 8.5: surface malformed
					// `__row_table_object_id__` as storage corruption
					// rather than silently treating it as PK. `Ok(None)`
					// means the column is absent (PK row); only fail
					// when the column is present but unreadable.
					let table_object_id = crate::storage::multirow::storage_row_table_object_id_at(
						&decoded,
						location.record_row_index,
					)?;
					match materialize_record_location_batch(&decoded, location.record_row_index) {
						Ok(batch) if batch.num_rows() == 1 => (batch, table_object_id),
						_ => {
							saw_unmatched_row = true;
							continue;
						}
					}
				}
				_ => {
					saw_unmatched_row = true;
					continue;
				}
			};
			let had_candidates = candidates.is_some();
			let matching = match candidates.take() {
				Some(existing) => existing,
				None => descriptors.clone(),
			}
			.into_iter()
			.filter_map(|descriptor| {
				match descriptor_matches_tiered_row(
					&descriptor,
					tenant_id,
					key_identity,
					routing_locality,
					stored_table_object_id,
					&batch,
				) {
					Ok(true) => Some(descriptor),
					Ok(false) | Err(_) => None,
				}
			})
			.collect::<Vec<_>>();
			if matching.is_empty() {
				if had_candidates {
					return Err(Error::new(
						ErrorKind::InvalidData,
						format!(
							"ADR 039 typed-table slab {slab_id} contains a row outside the matched table descriptor; stats are mandatory for typed-table v2 slabs"
						),
					));
				}
				saw_unmatched_row = true;
				continue;
			}
			candidates = Some(matching);
			row_batches.push(batch);
		}

		let Some(mut candidates) = candidates else {
			return Ok(None);
		};
		if saw_unmatched_row {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"ADR 039 typed-table slab {slab_id} also contains rows that cannot be represented by the matched table descriptor; stats are mandatory for typed-table v2 slabs"
				),
			));
		}
		if candidates.len() != 1 {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"ADR 039 typed-table slab {slab_id} matches {} table descriptors; cannot choose a mandatory CSB descriptor",
					candidates.len()
				),
			));
		}
		if row_batches.is_empty() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"ADR 039 typed-table slab {slab_id} matched a descriptor but no row batches"
				),
			));
		}
		let descriptor = candidates.remove(0);
		let schema = row_batches[0].schema();
		let batch = concat_batches(&schema, &row_batches).map_err(|err| {
			Error::new(
				ErrorKind::InvalidData,
				format!("ADR 039 CSB row concat failed: {err}"),
			)
		})?;
		let max_stats_ciphertext_bytes = max_column_stats_ciphertext_bytes_for_slab_payload(
			usize::try_from(self.config.max_slab_bytes).unwrap_or(usize::MAX),
		);
		let plaintext = build_column_stats_block_with_ciphertext_budget(
			&descriptor,
			&batch,
			crate::storage::stats::COLUMN_STATS_DEFAULT_PREFIX_BYTES,
			max_stats_ciphertext_bytes,
		)?;
		let parsed = parse_column_stats_block(&plaintext)?;
		if parsed.header.column_block_count == 0 {
			return Ok(None);
		}
		Ok(Some(TieredColumnStatsBlock::plaintext(
			plaintext,
			descriptor.schema_generation,
			parsed.header.column_block_count,
			locator_flags_for_parsed_column_stats_block(&parsed),
		)))
	}

	/// Export the authoritative local slab set as a backup image.
	///
	/// The image includes the active slab and every sealed slab.
	/// Rehydrated cache copies are intentionally excluded because
	/// L2 remains authoritative for those slabs; the manifest
	/// records them as omitted assumptions instead.
	pub async fn export_backup(&self) -> std::io::Result<StorageBackupImage> {
		self.fsync_all_local_slabs().await?;

		let (active_slab_id, next_slab_id, sealed_slab_ids, tiered_slabs, omitted) = {
			let state = self.state.lock().await;
			let mut sealed_slab_ids = state
				.sealed
				.iter()
				.map(|entry| *entry.key())
				.collect::<Vec<_>>();
			let mut tiered_slabs = state
				.tiered
				.iter()
				.map(|entry| entry.value().clone())
				.collect::<Vec<_>>();
			let mut omitted = state
				.rehydrated
				.iter()
				.map(|entry| *entry.key())
				.collect::<Vec<_>>();
			sealed_slab_ids.sort_unstable();
			tiered_slabs.sort_by_key(|pointer| pointer.slab_id);
			omitted.sort_unstable();
			(
				state.active.slab_id(),
				state.next_slab_id,
				sealed_slab_ids,
				tiered_slabs,
				omitted,
			)
		};

		let mut local_slab_ids = Vec::with_capacity(1 + sealed_slab_ids.len());
		local_slab_ids.push(active_slab_id);
		local_slab_ids.extend(sealed_slab_ids.iter().copied());
		local_slab_ids.sort_unstable();

		let mut local_slab_bytes = 0_u64;
		let mut local_slabs = Vec::with_capacity(local_slab_ids.len());
		for slab_id in local_slab_ids {
			let path = slab_path(&self.config.dir, slab_id);
			let bytes = self.file_ops.read_all(&path).await?;
			local_slab_bytes = local_slab_bytes.saturating_add(bytes.len() as u64);
			local_slabs.push(LocalSlabBackup { slab_id, bytes });
		}

		Ok(StorageBackupImage {
			manifest: StorageBackupManifest {
				format_version: STORAGE_BACKUP_FORMAT_VERSION,
				active_slab_id,
				next_slab_id,
				sealed_slab_ids,
				local_slab_bytes,
				tiered_slabs,
				omitted_rehydrated_slabs: omitted,
			},
			local_slabs,
		})
	}

	/// Restore a backup image into an empty slab directory.
	///
	/// The caller is responsible for opening a new [`SlabManager`]
	/// against the target after the files have been materialized.
	pub async fn restore_backup(
		backup: &StorageBackupImage,
		dir: impl AsRef<Path>,
	) -> std::io::Result<()> {
		let dir = dir.as_ref();
		if !dir.is_dir() {
			return Err(Error::new(
				ErrorKind::NotFound,
				format!("storage restore target does not exist: {}", dir.display()),
			));
		}
		let file_ops: Arc<dyn AsyncFileOps> = Arc::new(BlockingFileOps);
		if !discover_slab_ids(file_ops.as_ref(), dir).await?.is_empty() {
			return Err(Error::new(
				ErrorKind::AlreadyExists,
				format!("storage restore target is not empty: {}", dir.display()),
			));
		}

		let mut seen = HashSet::new();
		for slab in &backup.local_slabs {
			if !seen.insert(slab.slab_id) {
				return Err(Error::new(
					ErrorKind::InvalidInput,
					format!("backup contains duplicate slab {}", slab.slab_id),
				));
			}
			let path = slab_path(dir, slab.slab_id);
			write_file(
				file_ops.as_ref(),
				&path,
				OpenFlags {
					write: true,
					create_new: true,
					..Default::default()
				},
				&slab.bytes,
			)
			.await?;
			fsync_parent_dir(file_ops.as_ref(), &path).await?;
		}
		if !seen.contains(&backup.manifest.active_slab_id) {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				format!(
					"backup active_slab_id {} is missing from the local slab set",
					backup.manifest.active_slab_id
				),
			));
		}
		for pointer in &backup.manifest.tiered_slabs {
			if seen.contains(&pointer.slab_id) {
				return Err(Error::new(
					ErrorKind::InvalidInput,
					format!(
						"backup slab {} is marked tiered and local at the same time",
						pointer.slab_id
					),
				));
			}
		}
		Ok(())
	}

	/// Move a slab's backing from the sealed L1 set to the tiered
	/// set. On return:
	///
	/// * The slab is no longer in `sealed` — future
	///   [`StorageEngine::read`]s route to L2 via the stored URI.
	/// * The on-disk `slab_<id>.wnd` file has been unlinked.
	/// * Any read that grabbed an `Arc<LocalSlabStore>` *before*
	///   this call still holds its own reference, so its open fd
	///   survives the unlink (Linux semantics) — zero downtime
	///   for readers mid-flight.
	pub async fn apply_tier_slab(&self, pointer: TieredSlabPointer) -> std::io::Result<()> {
		self.apply_tier_slab_committed(pointer, None).await
	}

	pub async fn apply_tier_slab_committed(
		&self,
		pointer: TieredSlabPointer,
		last_control_log_index: Option<u64>,
	) -> std::io::Result<()> {
		// This path is intentionally idempotent for the same pointer. The
		// sweeper may fall back to calling it after Raft apply has already
		// fired the tier observer, so a second application of an identical
		// pointer must not rebuild row state or mutate relocated reads again.
		let old_pointer = {
			let state = self.state.lock().await;
			state
				.tiered
				.get(&pointer.slab_id)
				.map(|entry| entry.value().clone())
		};
		let refresh_plan = if old_pointer.as_ref().is_some_and(|old| old != &pointer)
			|| !self.has_rows_for_slab(pointer.slab_id)
		{
			Some(self.build_tiered_row_refresh_plan_from_l2(&pointer).await?)
		} else {
			None
		};
		#[cfg(debug_assertions)]
		if old_pointer.as_ref().is_some_and(|old| old == &pointer) {
			debug_assert!(
				refresh_plan.is_none(),
				"identical tier pointer replay must remain a no-op; the sweeper may reapply after the observer already committed local state"
			);
		}
		self.persist_tiered_pointer_strict(pointer.clone(), last_control_log_index)
			.await?;
		if let Some(plan) = &refresh_plan {
			for (old_location, new_location) in &plan.relocated_reads {
				self.relocated_reads.insert(*old_location, *new_location);
			}
		}
		if old_pointer
			.as_ref()
			.map(|old| old != &pointer)
			.unwrap_or(true)
		{
			self.clear_tiered_stats_cache_for_slab(pointer.slab_id);
		}
		{
			let mut state = self.state.lock().await;
			let _old = state.sealed.remove(&pointer.slab_id);
			let _old = state.rehydrated.remove(&pointer.slab_id);
			state.tiered.insert(pointer.slab_id, pointer.clone());
			state.hot_read_counts.remove(&pointer.slab_id);
			state.rehydrating.remove(&pointer.slab_id);
		}
		self.retired_tiered_cleanup.remove(&pointer.slab_id);
		if let Some(plan) = refresh_plan {
			self.apply_tiered_row_refresh_plan(plan).await;
		}

		// Unlink the L1 file. Open fds held by in-flight reads
		// continue to work; new reads find the entry in `tiered`
		// and go to L2.
		let path = slab_path(&self.config.dir, pointer.slab_id);
		unlink_local_file(self.file_ops.as_ref(), &path).await?;
		Ok(())
	}

	pub async fn retire_tiered_slab(&self, slab_id: SlabId) -> std::io::Result<bool> {
		self.retire_tiered_slab_committed(slab_id, None).await
	}

	pub async fn retire_tiered_slab_committed(
		&self,
		slab_id: SlabId,
		last_control_log_index: Option<u64>,
	) -> std::io::Result<bool> {
		let rows = self.record_rows_for_slab(slab_id);
		let retired_pointer = {
			let state = self.state.lock().await;
			state
				.tiered
				.get(&slab_id)
				.map(|entry| entry.value().clone())
		};
		let should_remove = {
			let state = self.state.lock().await;
			retired_pointer.is_some() || state.rehydrated.contains_key(&slab_id) || !rows.is_empty()
		};
		if !should_remove {
			return Ok(false);
		}
		if let Some(pointer) = retired_pointer.clone() {
			self.retired_tiered_cleanup.insert(slab_id, pointer);
		}
		self.delete_tiered_slab_strict(slab_id, last_control_log_index)
			.await?;
		{
			let mut state = self.state.lock().await;
			let _old = state.sealed.remove(&slab_id);
			let _old = state.rehydrated.remove(&slab_id);
			let _old = state.tiered.remove(&slab_id);
			state.hot_read_counts.remove(&slab_id);
			state.rehydrating.remove(&slab_id);
		}
		for row in &rows {
			self.rollback_indexed_location(row.tenant_id, row.key_identity, row.location);
		}
		self.relocated_reads.retain(|old_location, new_location| {
			old_location.slab_id != slab_id && new_location.slab_id != slab_id
		});
		self.clear_tiered_stats_cache_for_slab(slab_id);
		let path = slab_path(&self.config.dir, slab_id);
		unlink_local_file(self.file_ops.as_ref(), &path).await?;
		Ok(true)
	}

	/// Converge one recovered tiered slab into local state.
	///
	/// When the redb mirror is unavailable or missing rows, the
	/// manager rebuilds the per-record index directly from the
	/// authoritative L2 blob before installing the tier pointer.
	pub async fn reconcile_tiered_slab(
		&self,
		pointer: TieredSlabPointer,
		last_control_log_index: Option<u64>,
	) -> std::io::Result<()> {
		self.apply_tier_slab_committed(pointer, last_control_log_index)
			.await
	}

	/// Whether a slab has been pivoted to L2. Cheap — only hits
	/// the tiered DashMap.
	pub async fn is_tiered(&self, slab_id: SlabId) -> bool {
		self.state.lock().await.tiered.contains_key(&slab_id)
	}

	/// Whether a tiered slab currently also has a rehydrated local
	/// copy in L1.
	pub async fn is_rehydrated(&self, slab_id: SlabId) -> bool {
		self.state.lock().await.rehydrated.contains_key(&slab_id)
	}

	/// Lookup the L2 pointer for a tiered slab.
	pub async fn tier_pointer(&self, slab_id: SlabId) -> Option<TieredSlabPointer> {
		self.state
			.lock()
			.await
			.tiered
			.get(&slab_id)
			.map(|p| p.clone())
	}

	/// Enumerate the current tiered-slab pointers held in local
	/// runtime state. The returned list is sorted by slab id so
	/// background sweeps behave deterministically.
	pub async fn list_tiered_slabs(&self) -> Vec<TieredSlabPointer> {
		let mut pointers = self
			.state
			.lock()
			.await
			.tiered
			.iter()
			.map(|entry| entry.value().clone())
			.collect::<Vec<_>>();
		pointers.sort_by_key(|pointer| pointer.slab_id);
		pointers
	}

	/// Restore-time validation pass for replicated tier pointers.
	///
	/// Confirms that every committed tier reference is resolvable
	/// through the configured L2 store before the node advertises
	/// ready. This is intentionally lighter than a full object hash
	/// re-read: H2 only needs to prove the referenced objects exist
	/// and are reachable from the restore environment.
	pub async fn validate_tiered_references(&self) -> std::io::Result<()> {
		let pointers = self.list_tiered_slabs().await;
		if pointers.is_empty() {
			return Ok(());
		}
		let l2 = self.config.l2.as_ref().ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidInput,
				"restore validation requires an L2 object store when tiered slab references are present",
			)
		})?;
		for pointer in &pointers {
			let metadata = l2.head(&pointer.l2_uri).await.map_err(|err| {
				Error::other(format!(
					"restore validation failed for tiered slab {} at {}: {err}",
					pointer.slab_id, pointer.l2_uri
				))
			})?;
			if metadata.size_bytes == 0 {
				return Err(Error::new(
					ErrorKind::InvalidData,
					format!(
						"restore validation found zero-length tiered slab {} at {}",
						pointer.slab_id, pointer.l2_uri
					),
				));
			}
		}
		Ok(())
	}

	/// Tier retirements that committed in the SM but still need
	/// best-effort local cleanup retried.
	pub async fn list_retired_tiered_cleanup_candidates(&self) -> Vec<TieredSlabPointer> {
		let mut pointers = self
			.retired_tiered_cleanup
			.iter()
			.map(|entry| entry.value().clone())
			.collect::<Vec<_>>();
		pointers.sort_by_key(|pointer| pointer.slab_id);
		pointers
	}

	/// Retry only the local-file cleanup for a retired tiered slab.
	pub async fn retry_retired_tiered_local_cleanup(&self, slab_id: SlabId) -> std::io::Result<()> {
		let path = slab_path(&self.config.dir, slab_id);
		unlink_local_file(self.file_ops.as_ref(), &path).await
	}

	/// Mark one retired tiered slab cleanup candidate complete.
	pub fn clear_retired_tiered_cleanup(&self, slab_id: SlabId) {
		self.retired_tiered_cleanup.remove(&slab_id);
	}

	/// Best-effort reclaim of one rehydrated local cache copy. The
	/// authoritative L2 pointer remains intact; only the temporary
	/// L1 file is removed.
	pub async fn drop_rehydrated_slab(&self, slab_id: SlabId) -> std::io::Result<bool> {
		let removed = {
			let state = self.state.lock().await;
			if state.rehydrating.contains(&slab_id) {
				return Ok(false);
			}
			state.rehydrated.remove(&slab_id).map(|(_, state)| state)
		};
		let Some(_state) = removed else {
			return Ok(false);
		};
		let path = slab_path(&self.config.dir, slab_id);
		unlink_local_file(self.file_ops.as_ref(), &path).await?;
		Ok(true)
	}

	/// Enumerate every live `(key_identity, RecordLocation)` whose
	/// locality falls in the inclusive window
	/// `[start_locality, end_locality]`. Used
	/// by the Data Ring query path (ADR 005) to feed the PBC
	/// interpreter; the server then reads each location via the
	/// normal `StorageEngine::read` surface, which transparently
	/// pivots L1 vs. L2.
	///
	/// The returned pairs are sorted by `(locality, identity)` so
	/// the scan is deterministic even when multiple identities
	/// project into the same routing bucket. Every key yields only
	/// its latest version; use
	/// [`Self::scan_as_of`] for time-travel scans.
	pub fn scan(
		&self,
		tenant_id: TenantId,
		start_locality: impl Into<Locality>,
		end_locality: impl Into<Locality>,
	) -> Vec<(u64, RecordLocation)> {
		self.scan_as_of(
			tenant_id,
			start_locality,
			end_locality,
			HybridTimestamp {
				physical_ms: 0,
				logical: 0,
				node_id: 0,
			},
		)
	}

	/// Time-travel scan (ADR 013). Behaves like [`Self::scan`] but
	/// reports the newest version whose `hlc <= as_of_hlc`. A
	/// zero-valued `as_of_hlc` is the wire sentinel for "no upper
	/// bound" and falls through to the latest-version scan.
	pub fn scan_as_of(
		&self,
		tenant_id: TenantId,
		start_locality: impl Into<Locality>,
		end_locality: impl Into<Locality>,
		as_of_hlc: HybridTimestamp,
	) -> Vec<(u64, RecordLocation)> {
		self.scan_as_of_with_identity_and_locality(
			tenant_id,
			start_locality,
			end_locality,
			as_of_hlc,
		)
		.into_iter()
		.map(|(_key_identity, locality, loc)| (locality.get(), loc))
		.collect()
	}

	/// Variant of [`Self::scan_as_of`] that preserves the full
	/// authoritative [`Identity`] for each retained version. Query
	/// planning uses this when PBC needs to evaluate typed
	/// identity/locality predicates against record metadata rather
	/// than the decoded Arrow payload alone.
	pub fn scan_as_of_with_identity(
		&self,
		tenant_id: TenantId,
		start_locality: impl Into<Locality>,
		end_locality: impl Into<Locality>,
		as_of_hlc: HybridTimestamp,
	) -> Vec<(Identity, RecordLocation)> {
		self.scan_as_of_with_identity_and_locality(
			tenant_id,
			start_locality,
			end_locality,
			as_of_hlc,
		)
		.into_iter()
		.map(|(key_identity, _locality, location)| (key_identity, location))
		.collect()
	}

	/// Scan variant that also returns the routing locality used for
	/// range membership. This differs from `Locality::from(identity)`
	/// for typed table rows placed via `TableSpreadProjection`.
	pub fn scan_as_of_with_identity_and_locality(
		&self,
		tenant_id: TenantId,
		start_locality: impl Into<Locality>,
		end_locality: impl Into<Locality>,
		as_of_hlc: HybridTimestamp,
	) -> Vec<(Identity, Locality, RecordLocation)> {
		let start_locality = start_locality.into();
		let end_locality = end_locality.into();
		if start_locality > end_locality {
			return Vec::new();
		}
		let candidates =
			self.locality_candidates_for_tenant(tenant_id, start_locality, end_locality);
		let mut out = Vec::with_capacity(candidates.len());
		for (_locality, key_identity) in candidates {
			let Some(versions) = self.key_index.get(&(tenant_id, key_identity)) else {
				continue;
			};
			if let Some(location) = newest_version_as_of(versions.value(), as_of_hlc) {
				if location.is_tombstone {
					continue;
				}
				let locality = self
					.key_localities
					.get(&(tenant_id, key_identity))
					.map(|entry| *entry.value())
					.unwrap_or_else(|| Locality::from(key_identity));
				out.push((key_identity, locality, location));
			}
		}
		out
	}

	/// Every retained version whose identity projects into the
	/// inclusive locality window `[start_locality, end_locality]`
	/// and whose `hlc` is at or above `migration_hlc_floor`
	/// (Plan 019).
	///
	/// Unlike [`Self::scan`] / [`Self::scan_as_of`], this does not
	/// collapse per-key history — every version ≥ floor is
	/// returned. Output is sorted by `(locality, identity, hlc)` so
	/// the migration RPC can paginate with a stable cursor and the
	/// destination can append in the same order it observed on the
	/// source.
	pub fn scan_all_versions(
		&self,
		start_locality: impl Into<Locality>,
		end_locality: impl Into<Locality>,
		migration_hlc_floor: HybridTimestamp,
	) -> Vec<(TenantId, Identity, Locality, RecordLocation)> {
		self.scan_all_versions_from(
			start_locality,
			end_locality,
			migration_hlc_floor,
			Identity::ZERO,
			bytemuck::Zeroable::zeroed(),
		)
	}

	/// Version-aware paging variant of [`Self::scan_all_versions`].
	///
	/// Lets `handle_migrate` resume mid-key when a fat key has more
	/// retained versions than fit in a single page:
	///
	/// * When `(cursor_identity, cursor_hlc)` is `(zero, zero)` the
	///   scan
	///   behaves identically to `scan_all_versions`.
	/// * Otherwise we resume strictly after the last emitted
	///   `(locality, identity, hlc)` tuple.
	///
	/// Output is still sorted `(locality ASC, identity ASC, hlc ASC)`
	/// so the migration RPC can paginate deterministically.
	pub fn scan_all_versions_from(
		&self,
		start_locality: impl Into<Locality>,
		end_locality: impl Into<Locality>,
		migration_hlc_floor: HybridTimestamp,
		cursor_identity: Identity,
		cursor_hlc: HybridTimestamp,
	) -> Vec<(TenantId, Identity, Locality, RecordLocation)> {
		let start_locality = start_locality.into();
		let end_locality = end_locality.into();
		if start_locality > end_locality {
			return Vec::new();
		}
		let cursor_active =
			cursor_identity != Identity::ZERO || cursor_hlc != bytemuck::Zeroable::zeroed();
		let cursor_locality = if cursor_active {
			self.routing_locality_for_identity_any_tenant(cursor_identity)
		} else {
			start_locality
		};
		let mut out: Vec<(TenantId, Identity, Locality, RecordLocation)> = Vec::new();
		for (tenant_id, locality, key_identity) in
			self.locality_candidates_for_all_tenants(start_locality, end_locality)
		{
			let Some(versions) = self.key_index.get(&(tenant_id, key_identity)) else {
				continue;
			};
			for loc in versions.value().iter() {
				if loc.hlc < migration_hlc_floor {
					continue;
				}
				if cursor_active
					&& (locality, key_identity, loc.hlc)
						<= (cursor_locality, cursor_identity, cursor_hlc)
				{
					continue;
				}
				out.push((tenant_id, key_identity, locality, *loc));
			}
		}
		out.sort_by(
			|(tenant_a, ida, locality_a, la), (tenant_b, idb, locality_b, lb)| {
				(*locality_a, *tenant_a, *ida, la.hlc).cmp(&(*locality_b, *tenant_b, *idb, lb.hlc))
			},
		);
		out
	}

	fn migrated_tombstone_affects_slab(
		&self,
		slab_id: SlabId,
		tombstone: &MigratedRangeTombstone,
	) -> bool {
		self.locality_candidates_for_all_tenants(
			Locality::new(tombstone.range_start),
			Locality::new(tombstone.range_end),
		)
		.into_iter()
		.any(|(tenant_id, _locality, key_identity)| {
			self.key_index
				.get(&(tenant_id, key_identity))
				.is_some_and(|versions| {
					versions.iter().any(|loc| {
						loc.slab_id == slab_id && loc.hlc >= tombstone.migration_hlc_floor
					})
				})
		})
	}

	fn migrated_tombstone_affected_tiered_slabs(
		&self,
		tombstone: &MigratedRangeTombstone,
	) -> HashSet<SlabId> {
		self.locality_candidates_for_all_tenants(
			Locality::new(tombstone.range_start),
			Locality::new(tombstone.range_end),
		)
		.into_iter()
		.flat_map(|(tenant_id, _locality, key_identity)| {
			self.key_index
				.get(&(tenant_id, key_identity))
				.map(|versions| {
					versions
						.iter()
						.filter(|loc| loc.hlc >= tombstone.migration_hlc_floor)
						.map(|loc| loc.slab_id)
						.collect::<Vec<_>>()
				})
				.unwrap_or_default()
		})
		.collect()
	}

	pub fn slab_has_migrated_range_tombstone_at(
		&self,
		slab_id: SlabId,
		as_of_hlc: HybridTimestamp,
		tombstones: &[MigratedRangeTombstone],
	) -> bool {
		tombstones.iter().any(|tombstone| {
			tombstone.completed_at_hlc <= as_of_hlc
				&& self.migrated_tombstone_affects_slab(slab_id, tombstone)
		})
	}

	pub fn slab_has_supersession_at(&self, slab_id: SlabId, as_of_hlc: HybridTimestamp) -> bool {
		let unbounded_as_of =
			as_of_hlc.physical_ms == 0 && as_of_hlc.logical == 0 && as_of_hlc.node_id == 0;
		self.key_index.iter().any(|entry| {
			let versions = entry.value();
			versions.iter().enumerate().any(|(index, location)| {
				location.slab_id == slab_id
					&& versions[index + 1..].iter().any(|newer| {
						!newer.is_tombstone
							&& newer.hlc > location.hlc
							&& (unbounded_as_of || newer.hlc <= as_of_hlc)
					})
			})
		})
	}

	pub fn slab_hlc_bounds(
		&self,
		slab_id: SlabId,
	) -> Option<(HybridTimestamp, HybridTimestamp, u64)> {
		let mut bounds: Option<(HybridTimestamp, HybridTimestamp, u64)> = None;
		for entry in self.key_index.iter() {
			for location in entry.value().iter() {
				if location.slab_id != slab_id {
					continue;
				}
				bounds = Some(match bounds {
					Some((min_hlc, max_hlc, rows)) => (
						min_hlc.min(location.hlc),
						max_hlc.max(location.hlc),
						rows.saturating_add(1),
					),
					None => (location.hlc, location.hlc, 1),
				});
			}
		}
		bounds
	}

	pub fn slab_supersession_metadata(&self, slab_id: SlabId) -> SupersessionMetadata {
		let mut earliest: Option<HybridTimestamp> = None;
		for entry in self.key_index.iter() {
			let versions = entry.value();
			for (index, location) in versions.iter().enumerate() {
				if location.slab_id != slab_id {
					continue;
				}
				if let Some(newer) = versions[index + 1..]
					.iter()
					.filter(|newer| !newer.is_tombstone && newer.hlc > location.hlc)
					.min_by_key(|newer| newer.hlc)
				{
					earliest = Some(
						earliest
							.map(|current| current.min(newer.hlc))
							.unwrap_or(newer.hlc),
					);
				}
			}
		}
		earliest.map_or(SupersessionMetadata::None, SupersessionMetadata::Earliest)
	}

	pub fn slab_row_tombstone_metadata(&self, slab_id: SlabId) -> TombstoneMetadata {
		let mut earliest: Option<HybridTimestamp> = None;
		for entry in self.key_index.iter() {
			for location in entry.value().iter() {
				if location.slab_id == slab_id && location.is_tombstone {
					earliest = Some(
						earliest
							.map(|current| current.min(location.hlc))
							.unwrap_or(location.hlc),
					);
				}
			}
		}
		earliest.map_or(TombstoneMetadata::None, TombstoneMetadata::Earliest)
	}

	pub async fn record_supersession_for_slab(
		&self,
		slab_id: SlabId,
		supersession_hlc: HybridTimestamp,
		last_control_log_index: Option<u64>,
	) -> std::io::Result<bool> {
		let mut changed = None;
		{
			let state = self.state.lock().await;
			if let Some(mut pointer) = state.tiered.get_mut(&slab_id) {
				let before = pointer.supersession_metadata;
				pointer.record_supersession_hlc(supersession_hlc);
				if pointer.supersession_metadata != before {
					changed = Some((*pointer).clone());
				}
			}
		}
		let Some(pointer) = changed else {
			return Ok(false);
		};
		self.persist_tiered_pointer_strict(pointer.clone(), last_control_log_index)
			.await?;
		self.clear_tiered_stats_cache_for_slab(pointer.slab_id);
		Ok(true)
	}

	pub async fn record_row_tombstone_for_slab(
		&self,
		slab_id: SlabId,
		tombstone_hlc: HybridTimestamp,
		last_control_log_index: Option<u64>,
	) -> std::io::Result<bool> {
		let mut changed = None;
		{
			let state = self.state.lock().await;
			if let Some(mut pointer) = state.tiered.get_mut(&slab_id) {
				let before = pointer.tombstone_metadata;
				pointer.record_tombstone_hlc(tombstone_hlc);
				if pointer.tombstone_metadata != before {
					changed = Some((*pointer).clone());
				}
			}
		}
		let Some(pointer) = changed else {
			return Ok(false);
		};
		self.persist_tiered_pointer_strict(pointer.clone(), last_control_log_index)
			.await?;
		self.clear_tiered_stats_cache_for_slab(pointer.slab_id);
		Ok(true)
	}

	pub async fn record_migrated_range_tombstone(
		&self,
		tombstone: &MigratedRangeTombstone,
		last_control_log_index: Option<u64>,
	) -> std::io::Result<usize> {
		let affected_slab_ids = self.migrated_tombstone_affected_tiered_slabs(tombstone);
		if affected_slab_ids.is_empty() {
			return Ok(0);
		}

		let mut changed = Vec::new();
		{
			let state = self.state.lock().await;
			for slab_id in affected_slab_ids {
				let Some(mut pointer) = state.tiered.get_mut(&slab_id) else {
					continue;
				};
				let before = pointer.tombstone_metadata;
				pointer.record_tombstone_hlc(tombstone.completed_at_hlc);
				if pointer.tombstone_metadata != before {
					changed.push((*pointer).clone());
				}
			}
		}

		for pointer in &changed {
			self.persist_tiered_pointer_strict(pointer.clone(), last_control_log_index)
				.await?;
			self.clear_tiered_stats_cache_for_slab(pointer.slab_id);
		}

		Ok(changed.len())
	}

	/// Idempotent migration ingest (Plan 019).
	///
	/// Distinct from [`StorageEngine::append`] so the correctness
	/// boundary for lazy migration lives at a dedicated API rather
	/// than piggybacking on the hot write path:
	///
	/// * **Fast path** — if a version with the same
	///   `(key_identity, hlc)`
	///   already exists locally, read its frame, blake3 it, and
	///   compare to `payload_hash`. A match is a no-op and returns
	///   [`MigrationIngestOutcome::Duplicate`]. A mismatch is a
	///   fatal invariant violation: two nodes claim different bytes
	///   for the same MVCC slot, which would make MVCC history
	///   non-deterministic if silently accepted.
	/// * **Slow path** — otherwise append the frame through the
	///   same slab machinery the hot write path uses and return
	///   [`MigrationIngestOutcome::Inserted`].
	///
	/// The `migration_id` argument is currently only used for
	/// diagnostic framing in the mismatch error — retained so the
	/// signature stays stable once the migration registry and
	/// sweeper surfaces wire it through.
	pub async fn append_migration(
		&self,
		migration_id: u64,
		tenant_id: TenantId,
		key_identity: Identity,
		hlc: HybridTimestamp,
		payload_hash: [u8; 32],
		frame: &[u8],
	) -> std::io::Result<MigrationIngestOutcome> {
		self.append_migration_with_locality(
			migration_id,
			tenant_id,
			key_identity,
			Locality::from(key_identity),
			hlc,
			payload_hash,
			frame,
		)
		.await
	}

	/// Idempotent migration ingest for records whose routing
	/// locality is not the identity projection. Table-spread rows
	/// use this path so range migration preserves their table arc
	/// placement across replica-set movement.
	pub async fn append_migration_with_locality(
		&self,
		migration_id: u64,
		tenant_id: TenantId,
		key_identity: Identity,
		routing_locality: Locality,
		hlc: HybridTimestamp,
		payload_hash: [u8; 32],
		frame: &[u8],
	) -> std::io::Result<MigrationIngestOutcome> {
		// Fast-path collision check before touching disk: walk the
		// key's existing version chain under a short-lived read
		// guard and look for an exact HLC match. DashMap's read
		// guard is cheap and the chain is O(versions-per-key),
		// which is bounded by retention.
		let existing = self
			.key_index
			.get(&(tenant_id, key_identity))
			.and_then(|chain| chain.iter().find(|loc| loc.hlc == hlc).copied());

		if let Some(location) = existing {
			// Slow path: read the on-disk frame and hash it. If
			// the incoming payload really is a byte-identical copy
			// of what we already stored, the migration pull was
			// a duplicate and can be dropped.
			let stored = self.read(location).await?;
			let stored_hash = crate::hash::content::digest(&stored);
			if stored_hash == payload_hash {
				self.insert_locality_membership_with_locality(
					tenant_id,
					key_identity,
					routing_locality,
				);
				self.persist_record_row_strict(tenant_id, key_identity, routing_locality, location)
					.await?;
				return Ok(MigrationIngestOutcome::Duplicate(location));
			}
			// Different bytes claim the same MVCC slot. Refuse
			// loudly — accepting either side would make historical
			// reads depend on which replica answered the pull.
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"append_migration payload_hash mismatch: migration_id={migration_id} \
                     key_identity={key_identity} hlc={hlc:?} stored_hash={stored_hash:?} \
                     incoming_hash={payload_hash:?}"
				),
			));
		}

		if let Some(entries) = storage_multirow_index_entries_from_frame(frame)? {
			let Some(requested_entry) = entries
				.iter()
				.find(|entry| entry.key_identity == key_identity && entry.row_hlc == hlc)
				.copied()
			else {
				return Err(Error::new(
					ErrorKind::InvalidData,
					"multi-row migration frame does not contain the requested row identity/HLC",
				));
			};
			if requested_entry.routing_locality != routing_locality {
				return Err(Error::new(
					ErrorKind::InvalidData,
					"multi-row migration frame routing locality mismatch",
				));
			}
			let base_location = self
				.append_durable_multirow_with_locality(tenant_id, frame, &entries)
				.await?;
			return Ok(MigrationIngestOutcome::Inserted(RecordLocation {
				hlc: requested_entry.row_hlc,
				record_row_index: requested_entry.record_row_index,
				is_tombstone: requested_entry.is_tombstone,
				..base_location
			}));
		}

		// No prior version at this exact HLC — append to the same
		// slab machinery as hot writes, but publish the caller's
		// routing locality instead of deriving it from identity.
		let (slab, location) = self
			.append_on_target(tenant_id, key_identity, hlc, frame)
			.await?;
		if let Err(err) = self
			.persist_record_row_strict(tenant_id, key_identity, routing_locality, location)
			.await
		{
			slab.rollback_index_entry(tenant_id, key_identity, location);
			return Err(err);
		}
		self.index_appended_location_with_locality(
			tenant_id,
			key_identity,
			routing_locality,
			location,
		);
		Ok(MigrationIngestOutcome::Inserted(location))
	}

	/// Acquire the slab that should receive the next append,
	/// rotating if the current active slab has crossed the size cap.
	async fn target_for_append(&self) -> std::io::Result<Arc<LocalSlabStore>> {
		let mut state = self.state.lock().await;

		if state.active.write_cursor() < self.config.max_slab_bytes {
			return Ok(state.active.clone());
		}

		// Rotate. The check above and the mutation below happen
		// under the same lock, so concurrent overflow triggers
		// exactly one rotation.
		let next_id = state.next_slab_id;
		let next_path = slab_path(&self.config.dir, next_id);
		let next_slab = Arc::new(
			LocalSlabStore::open(
				&next_path,
				next_id,
				self.file_ops.clone(),
				self.config.group_commit.clone(),
			)
			.await?,
		);

		let sealed_slab = std::mem::replace(&mut state.active, next_slab.clone());
		let sealed_id = sealed_slab.slab_id();
		let sealed_size = sealed_slab.write_cursor();
		state.sealed.insert(sealed_id, sealed_slab);
		state.next_slab_id = next_id.saturating_add(1);
		self.record_l0_compaction_run(sealed_id, sealed_size);

		Ok(next_slab)
	}

	/// Resolve the authoritative backing for a slab, which may be
	/// either a local `Arc<LocalSlabStore>` (active / sealed) or
	/// an L2 pointer (tiered). Tiered wins over sealed so a
	/// post-pivot read never races back against the soon-to-be-
	/// unlinked L1 file.
	async fn resolve_backing(&self, slab_id: SlabId) -> SlabBacking {
		let state = self.state.lock().await;
		if state.active.slab_id() == slab_id {
			return SlabBacking::Local(state.active.clone());
		}
		if let Some(s) = state.sealed.get(&slab_id) {
			return SlabBacking::Local(s.clone());
		}
		if let Some(s) = state.rehydrated.get(&slab_id) {
			return SlabBacking::Local(s.slab.clone());
		}
		if let Some(p) = state.tiered.get(&slab_id) {
			return SlabBacking::L2(p.clone());
		}
		SlabBacking::Missing
	}

	fn relocate_read_location(&self, mut location: RecordLocation) -> RecordLocation {
		for _ in 0..8 {
			let Some(next) = self.relocated_reads.get(&location).map(|entry| *entry) else {
				return location;
			};
			if next == location {
				return location;
			}
			location = next;
		}
		location
	}

	pub async fn reclaim_tombstoned_ranges(
		&self,
		tombstones: &[MigratedRangeTombstone],
		max_slabs: usize,
	) -> std::io::Result<TombstoneCompactionReport> {
		if tombstones.is_empty() {
			return Ok(TombstoneCompactionReport::default());
		}
		for tombstone in tombstones {
			self.record_migrated_range_tombstone(tombstone, None)
				.await?;
		}
		if max_slabs == 0 {
			return Ok(TombstoneCompactionReport::default());
		}

		let sealed = {
			let state = self.state.lock().await;
			let mut slabs = state
				.sealed
				.iter()
				.map(|entry| (*entry.key(), entry.value().clone()))
				.collect::<Vec<_>>();
			slabs.sort_by_key(|(slab_id, _)| *slab_id);
			slabs
		};

		let mut report = TombstoneCompactionReport::default();
		for (slab_id, slab) in sealed {
			if report.compacted_slabs >= max_slabs {
				break;
			}
			if let Some(outcome) = self
				.compact_one_sealed_slab(slab_id, slab, tombstones)
				.await?
			{
				report.compacted_slabs += 1;
				report.reclaimed_bytes = report
					.reclaimed_bytes
					.saturating_add(outcome.reclaimed_bytes);
			}
		}
		Ok(report)
	}

	pub async fn prune_versions_older_than(
		&self,
		retention_floor: HybridTimestamp,
		max_slabs: usize,
	) -> std::io::Result<RetentionPruningReport> {
		if max_slabs == 0 {
			return Ok(RetentionPruningReport::default());
		}

		let sealed = {
			let state = self.state.lock().await;
			let mut slabs = state
				.sealed
				.iter()
				.map(|entry| (*entry.key(), entry.value().clone()))
				.collect::<Vec<_>>();
			slabs.sort_by_key(|(slab_id, _)| *slab_id);
			slabs
		};

		let mut report = RetentionPruningReport::default();
		for (slab_id, slab) in sealed {
			if report.compacted_slabs >= max_slabs {
				break;
			}
			if let Some(outcome) = self
				.prune_one_sealed_slab(slab_id, slab, retention_floor)
				.await?
			{
				report.compacted_slabs += 1;
				report.pruned_versions = report
					.pruned_versions
					.saturating_add(outcome.reclaimed_records);
				report.pruned_data_rows = report
					.pruned_data_rows
					.saturating_add(outcome.retention_stats.pruned_data_rows);
				report.pruned_tombstone_rows = report
					.pruned_tombstone_rows
					.saturating_add(outcome.retention_stats.pruned_tombstone_rows);
				report.pair_drops = report
					.pair_drops
					.saturating_add(outcome.retention_stats.pair_drops);
				report.ambiguous_rows_retained = report
					.ambiguous_rows_retained
					.saturating_add(outcome.retention_stats.ambiguous_rows_retained);
				report.max_lag_seconds = report
					.max_lag_seconds
					.max(outcome.retention_stats.max_lag_seconds);
				report.reclaimed_bytes = report
					.reclaimed_bytes
					.saturating_add(outcome.reclaimed_bytes);
			}
		}
		Ok(report)
	}

	pub async fn prune_versions_for_retention(
		&self,
		cluster_policy: MvccRetentionPolicy,
		table_lifecycle: &TableLifecycleCatalog,
		table_schemas: &TableSchemaCatalogV1,
		secondary_indexes: &SecondaryIndexCatalog,
		now_ms: u64,
		max_slabs: usize,
	) -> std::io::Result<RetentionPruningReport> {
		if max_slabs == 0 {
			return Ok(RetentionPruningReport::default());
		}

		let sealed = {
			let state = self.state.lock().await;
			let mut slabs = state
				.sealed
				.iter()
				.map(|entry| (*entry.key(), entry.value().clone()))
				.collect::<Vec<_>>();
			slabs.sort_by_key(|(slab_id, _)| *slab_id);
			slabs
		};

		let mut report = RetentionPruningReport::default();
		for (slab_id, slab) in sealed {
			if report.compacted_slabs >= max_slabs {
				break;
			}
			let scan = self
				.prune_one_sealed_slab_for_retention(
					slab_id,
					slab,
					cluster_policy,
					table_lifecycle,
					table_schemas,
					secondary_indexes,
					now_ms,
				)
				.await?;
			report.pruned_data_rows = report
				.pruned_data_rows
				.saturating_add(scan.retention_stats.pruned_data_rows);
			report.pruned_tombstone_rows = report
				.pruned_tombstone_rows
				.saturating_add(scan.retention_stats.pruned_tombstone_rows);
			report.pair_drops = report
				.pair_drops
				.saturating_add(scan.retention_stats.pair_drops);
			report.ambiguous_rows_retained = report
				.ambiguous_rows_retained
				.saturating_add(scan.retention_stats.ambiguous_rows_retained);
			report.max_lag_seconds = report
				.max_lag_seconds
				.max(scan.retention_stats.max_lag_seconds);
			if let Some(outcome) = scan.compaction {
				report.compacted_slabs += 1;
				report.pruned_versions = report
					.pruned_versions
					.saturating_add(outcome.reclaimed_records);
				report.reclaimed_bytes = report
					.reclaimed_bytes
					.saturating_add(outcome.reclaimed_bytes);
			}
		}
		Ok(report)
	}

	pub async fn recluster_sealed_table_layout(
		&self,
		tenant_id: TenantId,
		descriptor: &TableSchemaDescriptorV1,
		max_slabs: usize,
	) -> std::io::Result<LayoutReclusterReport> {
		descriptor.validate()?;
		if max_slabs == 0 {
			return Ok(LayoutReclusterReport::default());
		}

		let sealed = {
			let state = self.state.lock().await;
			let mut slabs = state
				.sealed
				.iter()
				.map(|entry| (*entry.key(), entry.value().clone()))
				.collect::<Vec<_>>();
			slabs.sort_by_key(|(slab_id, _)| *slab_id);
			slabs
		};

		let mut report = LayoutReclusterReport::default();
		for (slab_id, slab) in sealed {
			if report.rewritten_slabs >= max_slabs {
				break;
			}
			if let Some(outcome) = self
				.recluster_one_sealed_slab(slab_id, slab, tenant_id, descriptor)
				.await?
			{
				report.rewritten_slabs += 1;
				report.rewritten_records = report
					.rewritten_records
					.saturating_add(outcome.rewritten_records);
				report.relocated_rows =
					report.relocated_rows.saturating_add(outcome.relocated_rows);
				report.reclaimed_bytes = report
					.reclaimed_bytes
					.saturating_add(outcome.reclaimed_bytes);
			}
		}
		Ok(report)
	}
}

#[async_trait]
impl LsmCompactionJobRunner for SlabManager {
	async fn run_lsm_compaction_plan(
		&self,
		worker: &LsmCompactionWorker,
		plan: CompactionPlan,
	) -> Result<crate::storage::lsm::CompactionOutcome, CompactionFailure> {
		SlabManager::run_lsm_compaction_once(self, worker, plan).await
	}
}

#[async_trait]
impl LsmCompactionDriver for SlabManager {
	async fn run_lsm_compaction_tick(
		&self,
		worker: &LsmCompactionWorker,
		now: HybridTimestamp,
	) -> Result<Option<crate::storage::lsm::CompactionOutcome>, CompactionFailure> {
		SlabManager::run_lsm_compaction_tick(self, worker, now).await
	}
}

#[async_trait]
impl LsmGcDriver for SlabManager {
	async fn run_lsm_gc_tick(&self, worker: &LsmGcWorker) -> std::io::Result<Vec<SlabId>> {
		SlabManager::sweep_lsm_delete_pending_once(self, worker).await
	}
}

/// Authoritative location for a slab, as observed by the read
/// path. Keeping it an enum means the "is the file still here?"
/// question is answered with the same lock acquisition that
/// resolves the backing.
enum SlabBacking {
	Local(Arc<LocalSlabStore>),
	L2(TieredSlabPointer),
	Missing,
}

pub(crate) struct ProjectedL2RecordRead {
	pub(crate) projected: ProjectedRecordBatch,
	pub(crate) projected_first_record_row_index: RecordRowIndex,
	pub(crate) indexed_rows_materialized: bool,
	pub(crate) encoding_materialize_audit: Vec<EncodingMaterializeAuditSample>,
	#[allow(dead_code)]
	pub(crate) strategy: L2FetchStrategy,
}

pub(crate) struct EncodedAggregateL2RecordRead {
	pub(crate) partials: Vec<AggregatePartial>,
	pub(crate) rows_admitted: u64,
	#[allow(dead_code)]
	pub(crate) strategy: L2FetchStrategy,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct EncodingMaterializeAuditSample {
	pub(crate) cascade: &'static str,
	pub(crate) buffer_bytes_in: u64,
	pub(crate) buffer_bytes_out: u64,
}

#[derive(Clone, Copy, Debug)]
struct EncodingIndexedRowPage {
	page_key: PageMaterializeKey,
	row_offset_in_page: usize,
}

#[derive(Clone, Debug)]
struct EncodingIndexedPagePlan {
	selected_groups: Vec<RowGroupIndex>,
	row_pages: Vec<EncodingIndexedRowPage>,
	columns: Vec<PageMaterializeColumn>,
	fetches: Vec<PageMaterializeFetch>,
}

#[derive(Clone, Debug)]
struct EncodingComputePhaseBRead {
	outcome: crate::storage::row_group::PhaseBOutcome,
	phase_b_fetch: crate::storage::row_group::RowGroupRangeFetchPlan,
}

#[derive(Clone, Debug)]
struct EncodingComputePhaseBPlan {
	node: EncodingComputePhaseBNode,
	leaves: Vec<EncodingComputePhaseBLeaf>,
	fetches: Vec<EncodingComputePhaseBFetch>,
	record_body_ranges: Vec<crate::storage::row_group::RecordBodyByteRange>,
	body_ranges: Vec<TieredPlaintextRange>,
	record_ranges: Vec<TieredPlaintextRange>,
}

#[derive(Clone, Debug)]
struct EncodingPredicatePagePlan {
	node: EncodingComputePhaseBNode,
	leaves: Vec<EncodingComputePhaseBLeaf>,
	decisions: Vec<EncodingPredicatePageDecision>,
	fetches: Vec<EncodingPredicatePageFetch>,
	record_body_ranges: Vec<crate::storage::row_group::RecordBodyByteRange>,
	body_ranges: Vec<TieredPlaintextRange>,
	record_ranges: Vec<TieredPlaintextRange>,
}

#[derive(Clone, Debug)]
struct EncodingPredicatePageDecision {
	row_group_index: RowGroupIndex,
	leaf_index: usize,
	first_row: u32,
	row_count: u32,
	classification: EncodingPredicatePageDecisionKind,
}

#[derive(Clone, Debug)]
enum EncodingPredicatePageDecisionKind {
	Disjoint,
	ExactMatch,
	Maybe { fetch_index: usize },
}

#[derive(Clone, Debug)]
struct EncodingPredicatePageFetch {
	body_range: crate::storage::row_group::RecordBodyByteRange,
	encoding: BufferEncoding,
	cascade: EncodingCascade,
	data_type: DataType,
	atom: PredicateAtom,
}

#[derive(Clone, Debug)]
enum EncodingComputePhaseBNode {
	Leaf(usize),
	And(Vec<EncodingComputePhaseBNode>),
	Or(Vec<EncodingComputePhaseBNode>),
	Not(Box<EncodingComputePhaseBNode>),
}

#[derive(Clone, Debug)]
enum EncodingComputePhaseBLeaf {
	Encoded {
		column_index: usize,
		physical_ordinal: usize,
		atom: PredicateAtom,
		data_type: DataType,
	},
	Validity {
		physical_ordinal: usize,
		selected_when_no_nulls: bool,
	},
}

#[derive(Clone, Debug)]
struct EncodingComputePhaseBFetch {
	leaf_index: usize,
	row_group_index: RowGroupIndex,
	body_range: crate::storage::row_group::RecordBodyByteRange,
	kind: EncodingComputePhaseBFetchKind,
}

#[derive(Clone, Debug)]
enum EncodingComputePhaseBFetchKind {
	Encoded {
		encoding: BufferEncoding,
		cascade: EncodingCascade,
	},
	Validity {
		encoding: Option<BufferEncoding>,
		cascade: Option<EncodingCascade>,
	},
}

#[derive(Clone, Debug)]
struct EncodingComputeValidityFetch {
	body_range: crate::storage::row_group::RecordBodyByteRange,
	encoding: Option<BufferEncoding>,
	cascade: Option<EncodingCascade>,
}

#[derive(Clone, Debug)]
struct EncodingComputeAggregateFetch {
	column: u16,
	value_body_range: Option<crate::storage::row_group::RecordBodyByteRange>,
	validity_fetch: Option<EncodingComputeValidityFetch>,
	encoding: Option<BufferEncoding>,
	cascade: Option<EncodingCascade>,
	data_type: DataType,
}

#[derive(Clone, Debug)]
struct EncodingComputeAggregatePlan {
	fetches: Vec<EncodingComputeAggregateFetch>,
	record_body_ranges: Vec<crate::storage::row_group::RecordBodyByteRange>,
	body_ranges: Vec<TieredPlaintextRange>,
	record_ranges: Vec<TieredPlaintextRange>,
	pages_fetched: u64,
}

#[async_trait]
impl StorageEngine for SlabManager {
	async fn append(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
		hlc: HybridTimestamp,
		frame: &[u8],
	) -> std::io::Result<RecordLocation> {
		let (_slab, location) = self
			.append_on_target(tenant_id, key_identity, hlc, frame)
			.await?;
		// The per-slab `append` already pushed the new version onto
		// its local chain; mirror it into the manager's
		// authoritative multi-version index so `lookup` /
		// `lookup_as_of` stay O(1) without cross-slab scanning.
		self.index_appended_location(tenant_id, key_identity, location);
		self.persist_record_row(tenant_id, key_identity, location)
			.await;
		Ok(location)
	}

	async fn read(&self, location: RecordLocation) -> std::io::Result<Vec<u8>> {
		let location = self.relocate_read_location(location);
		let _pin = self.pin_lsm_read_location(location)?;
		match self.resolve_backing(location.slab_id).await {
			SlabBacking::Local(slab) => {
				let started = Instant::now();
				let result = slab.read(location).await;
				metrics()
					.l1_read_latency_seconds
					.observe_duration(started.elapsed());
				result
			}
			SlabBacking::L2(pointer) => {
				let started = Instant::now();
				let result = self.read_from_l2(&pointer, location).await;
				metrics()
					.l2_range_read_latency_seconds
					.observe_duration(started.elapsed());
				result
			}
			SlabBacking::Missing => Err(Error::new(
				ErrorKind::NotFound,
				format!("no slab with id {} under this manager", location.slab_id),
			)),
		}
	}

	fn lookup(&self, tenant_id: TenantId, key_identity: Identity) -> Option<RecordLocation> {
		self.key_index
			.get(&(tenant_id, key_identity))
			.and_then(|v| v.last().copied())
	}

	fn lookup_as_of(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
		as_of_hlc: HybridTimestamp,
	) -> Option<RecordLocation> {
		let versions = self.key_index.get(&(tenant_id, key_identity))?;
		newest_version_as_of(versions.value(), as_of_hlc)
	}
}

impl SlabManager {
	pub(crate) fn l2_column_fetch_enabled(&self) -> bool {
		self.config.l2_column_fetch.enabled || self.config.l2_row_group_fetch.enabled
	}

	pub(crate) fn row_group_writer_config(
		&self,
	) -> &crate::storage::row_group::RowGroupWriterConfig {
		&self.config.row_group_writer
	}

	pub(crate) fn encoding_writer_config(&self) -> &EncodingWriterConfig {
		&self.config.encoding_writer
	}

	pub(crate) fn l2_chunk_layout_config(&self) -> &L2ChunkLayoutConfig {
		&self.config.l2_chunk_layout
	}

	pub(crate) fn row_group_fetch_parallelism(&self) -> usize {
		self.config
			.l2_row_group_fetch
			.max_parallel_range_gets
			.get()
			.max(1) as usize
	}

	pub(crate) async fn read_projected_record(
		&self,
		location: RecordLocation,
		descriptor: &TableSchemaDescriptorV1,
		requirement: &ColumnFetchRequirement,
		predicate: Option<&crate::schema::pbc::Program>,
	) -> std::io::Result<Option<ProjectedL2RecordRead>> {
		if !self.config.l2_column_fetch.enabled && !self.config.l2_row_group_fetch.enabled {
			return Ok(None);
		}
		let location = self.relocate_read_location(location);
		let _pin = self.pin_lsm_read_location(location)?;
		match self.resolve_backing(location.slab_id).await {
			SlabBacking::Local(_) => Ok(None),
			SlabBacking::L2(pointer) => {
				let l2 = self.config.l2.as_ref().cloned().ok_or_else(|| {
					Error::new(
						ErrorKind::NotFound,
						format!(
							"slab {} is tiered but no L2 object store is configured",
							location.slab_id
						),
					)
				})?;
				let started = Instant::now();
				let result = self
					.read_projected_record_from_l2(
						l2,
						&pointer,
						location,
						descriptor,
						requirement,
						predicate,
						None,
					)
					.await;
				metrics()
					.l2_range_read_latency_seconds
					.observe_duration(started.elapsed());
				result.map(Some)
			}
			SlabBacking::Missing => Err(Error::new(
				ErrorKind::NotFound,
				format!("no slab with id {} under this manager", location.slab_id),
			)),
		}
	}

	pub(crate) async fn read_encoded_aggregate_record(
		&self,
		location: RecordLocation,
		descriptor: &TableSchemaDescriptorV1,
		request: &AggregateRequest,
		predicate: &crate::schema::pbc::Program,
	) -> std::io::Result<Option<EncodedAggregateL2RecordRead>> {
		if !self.config.encoding_compute.enabled
			|| !self.config.l2_encoding_fetch.enabled
			|| !self.config.l2_row_group_fetch.enabled
		{
			observe_encoding_compute_aggregate_request(
				request,
				"dense_fallback",
				EncodingComputeFallbackReason::ReaderDisabled.as_str(),
			);
			return Ok(None);
		}
		if !encoding_compute_aggregate_kernels_enabled(request, &self.config.encoding_compute) {
			observe_encoding_compute_aggregate_request(
				request,
				"dense_fallback",
				EncodingComputeFallbackReason::AggregateUnsupported.as_str(),
			);
			return Ok(None);
		}
		let location = self.relocate_read_location(location);
		let _pin = self.pin_lsm_read_location(location)?;
		match self.resolve_backing(location.slab_id).await {
			SlabBacking::Local(_) => Ok(None),
			SlabBacking::L2(pointer) => {
				let l2 = self.config.l2.as_ref().cloned().ok_or_else(|| {
					Error::new(
						ErrorKind::NotFound,
						format!(
							"slab {} is tiered but no L2 object store is configured",
							location.slab_id
						),
					)
				})?;
				let started = Instant::now();
				let result = self
					.read_encoded_aggregate_record_from_l2(
						l2, &pointer, location, descriptor, request, predicate,
					)
					.await;
				metrics()
					.l2_range_read_latency_seconds
					.observe_duration(started.elapsed());
				result
			}
			SlabBacking::Missing => Err(Error::new(
				ErrorKind::NotFound,
				format!("no slab with id {} under this manager", location.slab_id),
			)),
		}
	}

	pub(crate) async fn read_projected_indexed_rows(
		&self,
		locations: &[RecordLocation],
		descriptor: &TableSchemaDescriptorV1,
		requirement: &ColumnFetchRequirement,
	) -> std::io::Result<Option<ProjectedL2RecordRead>> {
		if locations.is_empty()
			|| (!self.config.l2_column_fetch.enabled && !self.config.l2_row_group_fetch.enabled)
		{
			return Ok(None);
		}
		let relocated = locations
			.iter()
			.map(|location| self.relocate_read_location(*location))
			.collect::<Vec<_>>();
		let first = relocated[0];
		for location in &relocated {
			if indexed_record_fetch_key(*location) != indexed_record_fetch_key(first) {
				return Err(Error::new(
					ErrorKind::InvalidInput,
					"projected indexed row fetch requires one physical record",
				));
			}
		}
		let _pin = self.pin_lsm_read_location(first)?;
		match self.resolve_backing(first.slab_id).await {
			SlabBacking::Local(_) => Ok(None),
			SlabBacking::L2(pointer) => {
				let l2 = self.config.l2.as_ref().cloned().ok_or_else(|| {
					Error::new(
						ErrorKind::NotFound,
						format!(
							"slab {} is tiered but no L2 object store is configured",
							first.slab_id
						),
					)
				})?;
				let started = Instant::now();
				let record_rows = relocated
					.iter()
					.map(|location| location.record_row_index)
					.collect::<Vec<_>>();
				let read = self
					.read_projected_record_from_l2(
						l2,
						&pointer,
						first,
						descriptor,
						requirement,
						None,
						Some(&record_rows),
					)
					.await?;
				metrics()
					.l2_range_read_latency_seconds
					.observe_duration(started.elapsed());
				let projected = if read.indexed_rows_materialized {
					read.projected.clone()
				} else {
					take_projected_indexed_rows(&read, &record_rows)?
				};
				Ok(Some(ProjectedL2RecordRead {
					projected,
					projected_first_record_row_index: RecordRowIndex::ZERO,
					indexed_rows_materialized: read.indexed_rows_materialized,
					encoding_materialize_audit: read.encoding_materialize_audit,
					strategy: read.strategy.clone(),
				}))
			}
			SlabBacking::Missing => Err(Error::new(
				ErrorKind::NotFound,
				format!("slab {} is not available for read", first.slab_id),
			)),
		}
	}

	fn locality_candidates_for_tenant(
		&self,
		tenant_id: TenantId,
		start_locality: Locality,
		end_locality: Locality,
	) -> Vec<(Locality, Identity)> {
		recover_rwlock_read(&self.locality_index, "locality_index")
			.get(&tenant_id)
			.map(|members| {
				members
					.range((start_locality, Identity::ZERO)..=(end_locality, IDENTITY_MAX))
					.copied()
					.collect()
			})
			.unwrap_or_default()
	}

	fn locality_candidates_for_all_tenants(
		&self,
		start_locality: Locality,
		end_locality: Locality,
	) -> Vec<(TenantId, Locality, Identity)> {
		let locality_index = recover_rwlock_read(&self.locality_index, "locality_index");
		let mut out = Vec::new();
		for (tenant_id, members) in locality_index.iter() {
			out.extend(
				members
					.range((start_locality, Identity::ZERO)..=(end_locality, IDENTITY_MAX))
					.map(|(locality, key_identity)| (*tenant_id, *locality, *key_identity)),
			);
		}
		out
	}

	fn persistent_record_rows_snapshot(&self) -> Vec<PersistentRecordRow> {
		let mut rows = Vec::new();
		for entry in self.key_index.iter() {
			let (tenant_id, key_identity) = *entry.key();
			for location in entry.value().iter() {
				rows.push(self.persistent_record_row(tenant_id, key_identity, *location));
			}
		}
		rows.sort_by_key(|row| {
			(
				row.location.slab_id,
				row.tenant_id,
				row.key_identity,
				row.location.hlc,
				row.location.offset,
				row.location.record_row_index,
			)
		});
		rows
	}

	fn has_rows_for_slab(&self, slab_id: SlabId) -> bool {
		self.key_index.iter().any(|entry| {
			entry
				.value()
				.iter()
				.any(|location| location.slab_id == slab_id)
		})
	}

	fn clear_tiered_stats_cache_for_slab(&self, slab_id: SlabId) {
		self.tiered_stats_cache
			.retain(|key, _stats| key.slab_id != slab_id);
	}

	async fn tiered_pointers_snapshot(&self) -> Vec<TieredSlabPointer> {
		let state = self.state.lock().await;
		let mut tiered = state
			.tiered
			.iter()
			.map(|entry| entry.value().clone())
			.collect::<Vec<_>>();
		tiered.sort_by_key(|pointer| pointer.slab_id);
		tiered
	}

	async fn rebuild_persistent_index(&self) {
		let Some(index) = &self.persistent_index else {
			return;
		};
		let rows = self.persistent_record_rows_snapshot();
		let tiered = self.tiered_pointers_snapshot().await;
		if let Err(err) = index.rebuild(rows, tiered, None).await {
			tracing::warn!(
				error = %err,
				dir = %self.config.dir.display(),
				"failed to rebuild redb storage index snapshot"
			);
		}
	}

	async fn mirror_persistent_update(
		&self,
		update: PersistentIndexUpdate,
		operation: &'static str,
	) {
		let Some(index) = &self.persistent_index else {
			return;
		};
		if let Err(err) = index.apply(update.clone()).await {
			tracing::warn!(
				error = %err,
				dir = %self.config.dir.display(),
				operation,
				"redb delta update failed; rebuilding from authoritative storage state"
			);
			let rows = self.persistent_record_rows_snapshot();
			let tiered = self.tiered_pointers_snapshot().await;
			if let Err(rebuild_err) = index
				.rebuild(rows, tiered, update.last_control_log_index)
				.await
			{
				tracing::warn!(
					error = %rebuild_err,
					dir = %self.config.dir.display(),
					operation,
					"failed to rebuild redb storage index snapshot"
				);
			}
		}
	}

	async fn persist_record_row(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
		location: RecordLocation,
	) {
		let Some(index) = &self.persistent_index else {
			return;
		};
		let row = self.persistent_record_row(tenant_id, key_identity, location);
		if let Err(err) = index.upsert_record(row).await {
			tracing::warn!(
				error = %err,
				slab_id = location.slab_id.get(),
				offset = location.offset,
				tenant_id = tenant_id.get(),
				"failed to persist slab record into redb"
			);
		}
	}

	async fn persist_record_row_strict(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
		routing_locality: Locality,
		location: RecordLocation,
	) -> std::io::Result<()> {
		let Some(index) = &self.persistent_index else {
			return Ok(());
		};
		let row = PersistentRecordRow {
			tenant_id,
			key_identity,
			routing_locality,
			location,
		};
		if let Err(err) = index.upsert_record(row).await {
			tracing::warn!(
				error = %err,
				slab_id = location.slab_id.get(),
				offset = location.offset,
				tenant_id = tenant_id.get(),
				"redb upsert failed on durable append; rebuilding snapshot before retry"
			);
			let mut rows = self.persistent_record_rows_snapshot();
			rows.push(row);
			rows.sort_by_key(|row| {
				(
					row.location.slab_id,
					row.tenant_id,
					row.key_identity,
					row.location.hlc,
					row.location.offset,
					row.location.record_row_index,
				)
			});
			let tiered = self.tiered_pointers_snapshot().await;
			index.rebuild(rows, tiered, None).await?;
		}
		Ok(())
	}

	async fn persist_record_rows_strict(
		&self,
		rows: Vec<PersistentRecordRow>,
	) -> std::io::Result<()> {
		let Some(index) = &self.persistent_index else {
			return Ok(());
		};
		if let Err(err) = index
			.apply(PersistentIndexUpdate {
				put_records: rows.clone(),
				..Default::default()
			})
			.await
		{
			tracing::warn!(
				error = %err,
				"redb batch upsert failed on durable multi-row append; rebuilding snapshot before retry"
			);
			let mut snapshot_rows = self.persistent_record_rows_snapshot();
			snapshot_rows.extend(rows);
			snapshot_rows.sort_by_key(|row| {
				(
					row.location.slab_id,
					row.tenant_id,
					row.key_identity,
					row.location.hlc,
					row.location.offset,
					row.location.record_row_index,
				)
			});
			let tiered = self.tiered_pointers_snapshot().await;
			index.rebuild(snapshot_rows, tiered, None).await?;
		}
		Ok(())
	}

	async fn delete_persistent_records(&self, records: &[CompactionRecord]) {
		let rows = records
			.iter()
			.map(|record| PersistentRecordRow {
				tenant_id: record.tenant_id,
				key_identity: record.key_identity,
				routing_locality: record.routing_locality,
				location: record.old_location,
			})
			.collect::<Vec<_>>();
		self.mirror_persistent_update(
			PersistentIndexUpdate {
				delete_records: rows,
				..Default::default()
			},
			"compaction-delete",
		)
		.await;
	}

	async fn replace_persistent_records(
		&self,
		removed: &[CompactionRecord],
		inserted: &[RelocatedRecord],
	) {
		let removed_rows = removed
			.iter()
			.map(|record| PersistentRecordRow {
				tenant_id: record.tenant_id,
				key_identity: record.key_identity,
				routing_locality: record.routing_locality,
				location: record.old_location,
			})
			.collect::<Vec<_>>();
		let inserted_rows = inserted
			.iter()
			.map(|record| PersistentRecordRow {
				tenant_id: record.tenant_id,
				key_identity: record.key_identity,
				routing_locality: record.routing_locality,
				location: record.new_location,
			})
			.collect::<Vec<_>>();
		self.mirror_persistent_update(
			PersistentIndexUpdate {
				delete_records: removed_rows,
				put_records: inserted_rows,
				..Default::default()
			},
			"compaction-replace",
		)
		.await;
	}

	async fn persist_tiered_pointer_strict(
		&self,
		pointer: TieredSlabPointer,
		last_control_log_index: Option<u64>,
	) -> std::io::Result<()> {
		let Some(index) = &self.persistent_index else {
			return Ok(());
		};
		if let Err(err) = index
			.upsert_tiered_slab(pointer.clone(), last_control_log_index)
			.await
		{
			tracing::warn!(
				error = %err,
				slab_id = pointer.slab_id.get(),
				"redb tier-pointer upsert failed; rebuilding snapshot before retry"
			);
			let rows = self.persistent_record_rows_snapshot();
			let mut tiered = self.tiered_pointers_snapshot().await;
			if let Some(existing) = tiered
				.iter_mut()
				.find(|entry| entry.slab_id == pointer.slab_id)
			{
				*existing = pointer.clone();
			} else {
				tiered.push(pointer.clone());
				tiered.sort_by_key(|entry| entry.slab_id);
			}
			index.rebuild(rows, tiered, last_control_log_index).await?;
		}
		Ok(())
	}

	async fn delete_tiered_slab_strict(
		&self,
		slab_id: SlabId,
		last_control_log_index: Option<u64>,
	) -> std::io::Result<()> {
		let Some(index) = &self.persistent_index else {
			return Ok(());
		};
		let mut rows = self.persistent_record_rows_snapshot();
		rows.retain(|row| row.location.slab_id != slab_id);
		let mut tiered = self.tiered_pointers_snapshot().await;
		tiered.retain(|entry| entry.slab_id != slab_id);
		index.rebuild(rows, tiered, last_control_log_index).await
	}

	fn record_rows_for_slab(&self, slab_id: SlabId) -> Vec<PersistentRecordRow> {
		let mut rows = Vec::new();
		for entry in self.key_index.iter() {
			let (tenant_id, key_identity) = *entry.key();
			for location in entry.value().iter() {
				if location.slab_id == slab_id {
					rows.push(self.persistent_record_row(tenant_id, key_identity, *location));
				}
			}
		}
		rows.sort_by_key(|row| {
			(
				row.location.slab_id,
				row.tenant_id,
				row.key_identity,
				row.location.hlc,
				row.location.offset,
			)
		});
		rows
	}

	async fn tiered_plaintext_from_l2(
		&self,
		pointer: &TieredSlabPointer,
	) -> std::io::Result<Vec<u8>> {
		let l2 = self.config.l2.clone().ok_or_else(|| {
			Error::new(
				ErrorKind::NotFound,
				format!(
					"cannot recover tiered slab {} without an L2 object store",
					pointer.slab_id
				),
			)
		})?;
		let blob = l2.get(&pointer.l2_uri).await?;
		validate_tiered_blob(pointer, &blob)?;
		decode_tiered_blob(self.config.crypto.as_ref(), pointer, &blob)
	}

	async fn load_tiered_rows_from_l2(
		&self,
		pointer: &TieredSlabPointer,
	) -> std::io::Result<Vec<PersistentRecordRow>> {
		let plaintext = self.tiered_plaintext_from_l2(pointer).await?;
		Ok(recover_slab_entries_from_blob(&plaintext, pointer.slab_id)?
			.into_iter()
			.map(|entry| PersistentRecordRow {
				tenant_id: entry.tenant_id,
				key_identity: entry.key_identity,
				routing_locality: self.recovered_entry_routing_locality(&entry),
				location: entry.location,
			})
			.collect::<Vec<_>>())
	}

	async fn build_tiered_row_refresh_plan_from_l2(
		&self,
		pointer: &TieredSlabPointer,
	) -> std::io::Result<TieredRowRefreshPlan> {
		let new_rows = self.load_tiered_rows_from_l2(pointer).await?;
		let old_rows = self.record_rows_for_slab(pointer.slab_id);
		let mut old_by_slot = HashMap::with_capacity(old_rows.len());
		for row in &old_rows {
			old_by_slot
				.entry((row.tenant_id, row.key_identity, row.location.hlc))
				.or_insert_with(Vec::new)
				.push(row.location);
		}
		let mut new_by_slot = HashMap::with_capacity(new_rows.len());
		for row in &new_rows {
			new_by_slot
				.entry((row.tenant_id, row.key_identity, row.location.hlc))
				.or_insert_with(Vec::new)
				.push(row.location);
		}
		let mut relocated_reads = Vec::new();
		for (slot, old_locations) in old_by_slot {
			let Some(new_locations) = new_by_slot.remove(&slot) else {
				continue;
			};
			// Retention rewrites preserve surviving rows in append/offset order but
			// may prune a prefix of older duplicates that share the same HLC. Align
			// from the tail so the newest duplicate survives the relocation map.
			let mut slot_relocations = old_locations
				.into_iter()
				.rev()
				.zip(new_locations.into_iter().rev())
				.filter_map(|(old_location, new_location)| {
					(old_location != new_location).then_some((old_location, new_location))
				})
				.collect::<Vec<_>>();
			slot_relocations.reverse();
			relocated_reads.extend(slot_relocations);
		}
		relocated_reads
			.sort_by_key(|(old_location, _)| (old_location.slab_id, old_location.offset));
		Ok(TieredRowRefreshPlan {
			old_rows,
			new_rows,
			relocated_reads,
		})
	}

	async fn apply_tiered_row_refresh_plan(&self, plan: TieredRowRefreshPlan) {
		if plan.old_rows == plan.new_rows {
			return;
		}
		for row in &plan.old_rows {
			self.rollback_indexed_location(row.tenant_id, row.key_identity, row.location);
		}
		for row in &plan.new_rows {
			self.key_index
				.entry((row.tenant_id, row.key_identity))
				.and_modify(|versions| {
					if !versions.contains(&row.location) {
						insert_version_sorted(versions, row.location);
					}
				})
				.or_insert_with(|| vec![row.location]);
			self.insert_locality_membership_with_locality(
				row.tenant_id,
				row.key_identity,
				row.routing_locality,
			);
		}
		self.mirror_persistent_update(
			PersistentIndexUpdate {
				delete_records: plan.old_rows,
				put_records: plan.new_rows,
				..Default::default()
			},
			"tiered-row-refresh",
		)
		.await;
	}

	#[cfg(test)]
	pub(crate) async fn prepare_tiered_retention_rewrite(
		&self,
		pointer: &TieredSlabPointer,
		retention_floor: HybridTimestamp,
		table_schemas: &TableSchemaCatalogV1,
	) -> std::io::Result<Option<PreparedTieredRetentionRewrite>> {
		let old_blob = self
			.config
			.l2
			.clone()
			.ok_or_else(|| {
				Error::new(
					ErrorKind::NotFound,
					format!(
						"cannot rewrite tiered slab {} without an L2 object store",
						pointer.slab_id
					),
				)
			})?
			.get(&pointer.l2_uri)
			.await?;
		validate_tiered_blob(pointer, &old_blob)?;
		let plaintext = Zeroizing::new(decode_tiered_blob(
			self.config.crypto.as_ref(),
			pointer,
			&old_blob,
		)?);
		let mut records = recover_slab_entries_from_blob(&plaintext, pointer.slab_id)?
			.into_iter()
			.map(|entry| CompactionRecord {
				tenant_id: entry.tenant_id,
				key_identity: entry.key_identity,
				routing_locality: self.recovered_entry_routing_locality(&entry),
				old_location: entry.location,
				reclaim: self.is_retention_prunable(
					entry.tenant_id,
					entry.key_identity,
					entry.location,
					retention_floor,
				),
				retention_ambiguous: false,
				retention_lag_seconds: 0,
			})
			.collect::<Vec<_>>();
		Self::mark_retention_prunable_row_tombstone_pairs(&mut records, retention_floor);
		records.sort_by_key(|record| record.old_location.offset);
		let physical_groups = physical_compaction_groups(&records);
		let pruned_versions = records.iter().filter(|record| record.reclaim).count();
		let retention_stats = Self::retention_compaction_stats(&records);
		let survivors = records.iter().filter(|record| !record.reclaim).count();
		if pruned_versions == 0 {
			return Ok(None);
		}
		if survivors == 0 {
			return Ok(Some(PreparedTieredRetentionRewrite::Retire {
				slab_id: pointer.slab_id,
				old_l2_uri: pointer.l2_uri.clone(),
				pruned_versions,
				retention_stats,
				reclaimed_bytes: old_blob.len() as u64,
			}));
		}

		let tmp_path = retention_rewrite_tmp_path(&self.config.dir, pointer.slab_id);
		let _ = unlink_local_file(self.file_ops.as_ref(), &tmp_path).await;
		let mut multirow_recompact_count = 0_u64;
		let rewrite_result: std::io::Result<Zeroizing<Vec<u8>>> = async {
			let rewritten = Arc::new(
				LocalSlabStore::open(
					&tmp_path,
					pointer.slab_id,
					self.file_ops.clone(),
					self.config.group_commit.clone(),
				)
				.await?,
			);
			for group in physical_groups.values() {
				let Some(first) = group.first().copied() else {
					continue;
				};
				let frame =
					Zeroizing::new(extract_record_from_blob(&plaintext, first.old_location)?);
				let Some(compacted) = compact_physical_record_frame(
					frame.as_ref(),
					group,
					Some(table_schemas),
					&self.config.row_group_writer,
					&self.config.encoding_writer,
					&self.config.l2_chunk_layout,
				)?
				else {
					continue;
				};
				if compacted.was_multirow {
					multirow_recompact_count = multirow_recompact_count.saturating_add(1);
				}
				rewritten
					.append(
						compacted.tenant_id,
						compacted.key_identity,
						compacted.hlc,
						&compacted.frame,
					)
					.await?;
			}
			rewritten.fsync().await?;
			Ok(Zeroizing::new(self.file_ops.read_all(&tmp_path).await?))
		}
		.await;
		let _ = unlink_local_file(self.file_ops.as_ref(), &tmp_path).await;
		let rewritten_plaintext = rewrite_result?;
		if multirow_recompact_count > 0 {
			metrics().observe_storage_multirow_recompact(multirow_recompact_count);
		}
		let encrypted = self.encrypt_tiered_object_blob_with_table_stats(
			pointer.slab_id,
			rewritten_plaintext.as_ref(),
			table_schemas,
		)?;
		let row_tombstone_metadata =
			row_tombstone_metadata_from_blob(rewritten_plaintext.as_ref(), pointer.slab_id)?;
		Ok(Some(PreparedTieredRetentionRewrite::Rewrite {
			slab_id: pointer.slab_id,
			old_l2_uri: pointer.l2_uri.clone(),
			payload_hash: crate::hash::content::digest(&encrypted.bytes),
			tombstone_metadata: merge_row_tombstone_metadata(
				pointer.tombstone_metadata,
				row_tombstone_metadata,
			),
			supersession_metadata: self.slab_supersession_metadata(pointer.slab_id),
			reclaimed_bytes: (old_blob.len() as u64).saturating_sub(encrypted.bytes.len() as u64),
			pruned_versions,
			retention_stats,
			encrypted_bytes: encrypted.bytes,
		}))
	}

	pub(crate) async fn prepare_tiered_table_retention_rewrite(
		&self,
		pointer: &TieredSlabPointer,
		cluster_policy: MvccRetentionPolicy,
		table_lifecycle: &TableLifecycleCatalog,
		table_schemas: &TableSchemaCatalogV1,
		now_ms: u64,
	) -> std::io::Result<Option<PreparedTieredRetentionRewrite>> {
		let old_blob = self
			.config
			.l2
			.clone()
			.ok_or_else(|| {
				Error::new(
					ErrorKind::NotFound,
					format!(
						"cannot rewrite tiered slab {} without an L2 object store",
						pointer.slab_id
					),
				)
			})?
			.get(&pointer.l2_uri)
			.await?;
		validate_tiered_blob(pointer, &old_blob)?;
		let plaintext = Zeroizing::new(decode_tiered_blob(
			self.config.crypto.as_ref(),
			pointer,
			&old_blob,
		)?);
		let mut records = recover_slab_entries_from_blob(&plaintext, pointer.slab_id)?
			.into_iter()
			.map(|entry| CompactionRecord {
				tenant_id: entry.tenant_id,
				key_identity: entry.key_identity,
				routing_locality: self.recovered_entry_routing_locality(&entry),
				old_location: entry.location,
				reclaim: false,
				retention_ambiguous: false,
				retention_lag_seconds: 0,
			})
			.collect::<Vec<_>>();
		self.mark_records_for_table_retention_from_blob(
			&plaintext,
			&mut records,
			cluster_policy,
			table_lifecycle,
			table_schemas,
			now_ms,
		)?;
		records.sort_by_key(|record| record.old_location.offset);
		let physical_groups = physical_compaction_groups(&records);
		let pruned_versions = records.iter().filter(|record| record.reclaim).count();
		let retention_stats = Self::retention_compaction_stats(&records);
		let survivors = records.iter().filter(|record| !record.reclaim).count();
		if pruned_versions == 0 {
			if retention_stats.max_lag_seconds > 0 || retention_stats.ambiguous_rows_retained > 0 {
				return Ok(Some(PreparedTieredRetentionRewrite::Observe {
					retention_stats,
				}));
			}
			return Ok(None);
		}
		if survivors == 0 {
			return Ok(Some(PreparedTieredRetentionRewrite::Retire {
				slab_id: pointer.slab_id,
				old_l2_uri: pointer.l2_uri.clone(),
				pruned_versions,
				retention_stats,
				reclaimed_bytes: old_blob.len() as u64,
			}));
		}

		let tmp_path = retention_rewrite_tmp_path(&self.config.dir, pointer.slab_id);
		let _ = unlink_local_file(self.file_ops.as_ref(), &tmp_path).await;
		let mut multirow_recompact_count = 0_u64;
		let rewrite_result: std::io::Result<Zeroizing<Vec<u8>>> = async {
			let rewritten = Arc::new(
				LocalSlabStore::open(
					&tmp_path,
					pointer.slab_id,
					self.file_ops.clone(),
					self.config.group_commit.clone(),
				)
				.await?,
			);
			for group in physical_groups.values() {
				let Some(first) = group.first().copied() else {
					continue;
				};
				let frame =
					Zeroizing::new(extract_record_from_blob(&plaintext, first.old_location)?);
				let Some(compacted) = compact_physical_record_frame(
					frame.as_ref(),
					group,
					Some(table_schemas),
					&self.config.row_group_writer,
					&self.config.encoding_writer,
					&self.config.l2_chunk_layout,
				)?
				else {
					continue;
				};
				if compacted.was_multirow {
					multirow_recompact_count = multirow_recompact_count.saturating_add(1);
				}
				rewritten
					.append(
						compacted.tenant_id,
						compacted.key_identity,
						compacted.hlc,
						&compacted.frame,
					)
					.await?;
			}
			rewritten.fsync().await?;
			Ok(Zeroizing::new(self.file_ops.read_all(&tmp_path).await?))
		}
		.await;
		let _ = unlink_local_file(self.file_ops.as_ref(), &tmp_path).await;
		let rewritten_plaintext = rewrite_result?;
		if multirow_recompact_count > 0 {
			metrics().observe_storage_multirow_recompact(multirow_recompact_count);
		}
		let encrypted = self.encrypt_tiered_object_blob_with_table_stats(
			pointer.slab_id,
			rewritten_plaintext.as_ref(),
			table_schemas,
		)?;
		let row_tombstone_metadata =
			row_tombstone_metadata_from_blob(rewritten_plaintext.as_ref(), pointer.slab_id)?;
		Ok(Some(PreparedTieredRetentionRewrite::Rewrite {
			slab_id: pointer.slab_id,
			old_l2_uri: pointer.l2_uri.clone(),
			payload_hash: crate::hash::content::digest(&encrypted.bytes),
			tombstone_metadata: merge_row_tombstone_metadata(
				pointer.tombstone_metadata,
				row_tombstone_metadata,
			),
			supersession_metadata: self.slab_supersession_metadata(pointer.slab_id),
			reclaimed_bytes: (old_blob.len() as u64).saturating_sub(encrypted.bytes.len() as u64),
			pruned_versions,
			retention_stats,
			encrypted_bytes: encrypted.bytes,
		}))
	}

	async fn reserve_slab_id(&self) -> SlabId {
		let mut state = self.state.lock().await;
		let next_id = state.next_slab_id;
		state.next_slab_id = state.next_slab_id.saturating_add(1);
		next_id
	}

	async fn compact_one_sealed_slab(
		&self,
		slab_id: SlabId,
		slab: Arc<LocalSlabStore>,
		tombstones: &[MigratedRangeTombstone],
	) -> std::io::Result<Option<CompactionOutcome>> {
		let mut records: Vec<CompactionRecord> = slab
			.index
			.iter()
			.flat_map(|entry| {
				let (tenant_id, key_identity) = *entry.key();
				let locality = self.routing_locality_for_key(tenant_id, key_identity);
				entry
					.value()
					.iter()
					.map(move |loc| CompactionRecord {
						tenant_id,
						key_identity,
						routing_locality: locality,
						old_location: *loc,
						reclaim: tombstones.iter().any(|tombstone| {
							locality.get() >= tombstone.range_start
								&& locality.get() <= tombstone.range_end
								&& loc.hlc >= tombstone.migration_hlc_floor
						}),
						retention_ambiguous: false,
						retention_lag_seconds: 0,
					})
					.collect::<Vec<_>>()
			})
			.collect();
		records.sort_by_key(|record| record.old_location.offset);

		self.compact_sealed_slab_records(slab_id, slab, &records, None)
			.await
	}

	async fn prune_one_sealed_slab(
		&self,
		slab_id: SlabId,
		slab: Arc<LocalSlabStore>,
		retention_floor: HybridTimestamp,
	) -> std::io::Result<Option<CompactionOutcome>> {
		let mut records: Vec<CompactionRecord> = slab
			.index
			.iter()
			.flat_map(|entry| {
				let (tenant_id, key_identity) = *entry.key();
				let routing_locality = self.routing_locality_for_key(tenant_id, key_identity);
				entry
					.value()
					.iter()
					.map(move |loc| CompactionRecord {
						tenant_id,
						key_identity,
						routing_locality,
						old_location: *loc,
						reclaim: self.is_retention_prunable(
							tenant_id,
							key_identity,
							*loc,
							retention_floor,
						),
						retention_ambiguous: false,
						retention_lag_seconds: 0,
					})
					.collect::<Vec<_>>()
			})
			.collect();
		Self::mark_retention_prunable_row_tombstone_pairs(&mut records, retention_floor);
		records.sort_by_key(|record| record.old_location.offset);

		self.compact_sealed_slab_records(slab_id, slab, &records, None)
			.await
	}

	async fn prune_one_sealed_slab_for_retention(
		&self,
		slab_id: SlabId,
		slab: Arc<LocalSlabStore>,
		cluster_policy: MvccRetentionPolicy,
		table_lifecycle: &TableLifecycleCatalog,
		table_schemas: &TableSchemaCatalogV1,
		secondary_indexes: &SecondaryIndexCatalog,
		now_ms: u64,
	) -> std::io::Result<RetentionSlabScanOutcome> {
		let mut records: Vec<CompactionRecord> = slab
			.index
			.iter()
			.flat_map(|entry| {
				let (tenant_id, key_identity) = *entry.key();
				let routing_locality = self.routing_locality_for_key(tenant_id, key_identity);
				entry
					.value()
					.iter()
					.map(move |loc| CompactionRecord {
						tenant_id,
						key_identity,
						routing_locality,
						old_location: *loc,
						reclaim: false,
						retention_ambiguous: false,
						retention_lag_seconds: 0,
					})
					.collect::<Vec<_>>()
			})
			.collect();
		self.mark_records_for_table_retention(
			&slab,
			&mut records,
			cluster_policy,
			table_lifecycle,
			table_schemas,
			now_ms,
		)
		.await?;
		records.sort_by_key(|record| record.old_location.offset);
		let retention_stats = Self::retention_compaction_stats(&records);
		let compaction = self
			.compact_sealed_slab_records(
				slab_id,
				slab,
				&records,
				Some(SecondaryIndexCompactionContext {
					table_schemas,
					secondary_indexes,
				}),
			)
			.await?;

		Ok(RetentionSlabScanOutcome {
			compaction,
			retention_stats,
		})
	}

	async fn recluster_one_sealed_slab(
		&self,
		slab_id: SlabId,
		slab: Arc<LocalSlabStore>,
		tenant_id: TenantId,
		descriptor: &TableSchemaDescriptorV1,
	) -> std::io::Result<Option<LayoutReclusterOutcome>> {
		let mut records: Vec<CompactionRecord> = slab
			.index
			.iter()
			.flat_map(|entry| {
				let (record_tenant_id, key_identity) = *entry.key();
				let routing_locality =
					self.routing_locality_for_key(record_tenant_id, key_identity);
				entry
					.value()
					.iter()
					.map(move |loc| CompactionRecord {
						tenant_id: record_tenant_id,
						key_identity,
						routing_locality,
						old_location: *loc,
						reclaim: false,
						retention_ambiguous: false,
						retention_lag_seconds: 0,
					})
					.collect::<Vec<_>>()
			})
			.collect();
		if records.is_empty() {
			return Ok(None);
		}
		records.sort_by_key(|record| {
			(
				record.old_location.offset,
				record.old_location.record_row_index,
			)
		});

		let target_is_multirow = descriptor.record_layout_policy.is_multi_row();
		let mut needs_recluster = false;
		let mut group_plans = Vec::new();
		for group in physical_compaction_groups(&records).values() {
			let Some(first) = group.first().copied() else {
				continue;
			};
			let frame = slab.read(first.old_location).await?;
			let source_is_multirow = storage_multirow_index_entries_from_frame(&frame)?.is_some();
			let decoded = match decode_record_batch(&frame) {
				Ok(batch) => Some(batch),
				Err(err) if source_is_multirow => return Err(err),
				Err(_) => None,
			};
			let source_compression_policy = match decoded.as_ref() {
				Some(_) => Some(record_batch_compression_policy_from_ipc(&frame)?),
				None => None,
			};
			let mut matching_rows = Vec::new();
			let mut nonmatching_records = Vec::new();
			for record in group {
				let matches_descriptor = match decoded.as_ref() {
					Some(_) if record.tenant_id != tenant_id => false,
					Some(batch) => {
						// Phase 8.5: malformed
						// `__row_table_object_id__` is storage
						// corruption, not "treat as PK".
						let stored_table_object_id =
							crate::storage::multirow::storage_row_table_object_id_at(
								batch,
								record.old_location.record_row_index,
							)?;
						let row_batch = materialize_record_location_batch(
							batch,
							record.old_location.record_row_index,
						)?;
						if descriptor_matches_tiered_row(
							descriptor,
							tenant_id,
							record.key_identity,
							record.routing_locality,
							stored_table_object_id,
							&row_batch,
						)? {
							matching_rows.push(ReclusterLogicalRow {
								tenant_id: record.tenant_id,
								key_identity: record.key_identity,
								routing_locality: record.routing_locality,
								old_location: record.old_location,
								row_hlc: record.old_location.hlc,
								batch: row_batch,
							});
							true
						} else {
							false
						}
					}
					None => false,
				};
				if !matches_descriptor {
					nonmatching_records.push(*record);
				}
			}
			if !matching_rows.is_empty() {
				let codec_mismatch = match source_compression_policy {
					Some(policy) => policy != descriptor.record_body_compression_policy,
					None => false,
				};
				if source_is_multirow != target_is_multirow || codec_mismatch {
					needs_recluster = true;
				}
			}
			group_plans.push(ReclusterGroupPlan {
				frame,
				records: group.clone(),
				matching_rows,
				nonmatching_records,
			});
		}

		if !needs_recluster {
			return Ok(None);
		}

		let old_bytes = slab.write_cursor();
		let replacement_id = self.reserve_slab_id().await;
		let replacement_path = slab_path(&self.config.dir, replacement_id);
		let replacement = Arc::new(
			LocalSlabStore::open(
				&replacement_path,
				replacement_id,
				self.file_ops.clone(),
				self.config.group_commit.clone(),
			)
			.await?,
		);
		let mut relocations = Vec::with_capacity(records.len());
		let mut rows_to_recluster = Vec::new();
		let mut rewritten_records = 0_usize;
		let mut multirow_recompact_count = 0_u64;

		for plan in group_plans {
			if plan.matching_rows.is_empty() {
				append_existing_frame_to_replacement(
					&replacement,
					&plan.frame,
					&plan.records,
					&mut relocations,
				)
				.await?;
				rewritten_records = rewritten_records.saturating_add(1);
				continue;
			}

			let matching_locations = plan
				.matching_rows
				.iter()
				.map(|row| row.old_location)
				.collect::<HashSet<_>>();
			rows_to_recluster.extend(plan.matching_rows);
			if plan.nonmatching_records.is_empty() {
				continue;
			}

			let partial = plan
				.records
				.iter()
				.map(|record| CompactionRecord {
					reclaim: matching_locations.contains(&record.old_location),
					..*record
				})
				.collect::<Vec<_>>();
			if let Some(compacted) = compact_physical_record_frame(
				&plan.frame,
				&partial,
				None,
				&self.config.row_group_writer,
				&self.config.encoding_writer,
				&self.config.l2_chunk_layout,
			)? {
				if compacted.was_multirow {
					multirow_recompact_count = multirow_recompact_count.saturating_add(1);
				}
				append_compacted_frame_to_replacement(&replacement, compacted, &mut relocations)
					.await?;
				rewritten_records = rewritten_records.saturating_add(1);
			}
		}

		match &descriptor.record_layout_policy {
			RecordLayoutPolicy::SingleRow => {
				rows_to_recluster.sort_by_key(|row| {
					(
						row.routing_locality,
						row.key_identity,
						row.row_hlc,
						row.old_location.offset,
						row.old_location.record_row_index,
					)
				});
				for row in &rows_to_recluster {
					let frame = encode_batch_with_policy(
						&row.batch,
						descriptor.record_body_compression_policy,
					)?
					.bytes;
					let new_location = replacement
						.append(row.tenant_id, row.key_identity, row.row_hlc, &frame)
						.await?;
					relocations.push(RelocatedRecord {
						tenant_id: row.tenant_id,
						key_identity: row.key_identity,
						routing_locality: row.routing_locality,
						old_location: row.old_location,
						new_location,
					});
					rewritten_records = rewritten_records.saturating_add(1);
				}
			}
			RecordLayoutPolicy::MultiRow(layout) => {
				let batches = rows_to_recluster
					.iter()
					.map(|row| row.batch.clone())
					.collect::<Vec<_>>();
				if !batches.is_empty() {
					let schema = batches[0].schema();
					let batch = concat_batches(&schema, &batches).map_err(|err| {
						Error::new(
							ErrorKind::InvalidData,
							format!("layout recluster row concat failed: {err}"),
						)
					})?;
					// ADR 045 Phase 8: reclustering re-stamps the
					// table-isolation column from the descriptor — the
					// descriptor is authoritative for the rows being
					// reclustered because reclustering operates
					// per-descriptor.
					let table_object_id_stamp = Some(descriptor.table_object_id);
					let source_rows = rows_to_recluster
						.iter()
						.enumerate()
						.map(|(source_row_index, row)| MultiRowSourceRow {
							source_row_index,
							key_identity: row.key_identity,
							routing_locality: row.routing_locality,
							row_hlc: row.row_hlc,
							is_tombstone: row.old_location.is_tombstone,
							table_object_id: table_object_id_stamp,
						})
						.collect::<Vec<_>>();
					for packed in
						pack_multirow_source_rows_with_policy_row_group_encoding_and_chunk_layout_config(
							*layout,
							&batch,
							&source_rows,
							Some(descriptor),
							descriptor.record_body_compression_policy,
							&self.config.row_group_writer,
							&self.config.encoding_writer,
							&self.config.l2_chunk_layout,
						)? {
						let Some(primary) = packed.encoded.rows.first().copied() else {
							return Err(Error::new(
								ErrorKind::InvalidData,
								"layout recluster produced an empty multi-row frame",
							));
						};
						let base_location = replacement
							.append(
								tenant_id,
								primary.key_identity,
								packed.encoded.max_row_hlc,
								&packed.encoded.frame,
							)
							.await?;
						replacement.rollback_index_entry(
							tenant_id,
							primary.key_identity,
							base_location,
						);
						for (source_row, encoded_row) in
							packed.source_rows.iter().zip(packed.encoded.rows.iter())
						{
							let old_row = rows_to_recluster
								.get(source_row.source_row_index)
								.ok_or_else(|| {
									Error::new(
										ErrorKind::InvalidData,
										"layout recluster source row index out of bounds",
									)
								})?;
							if old_row.key_identity != encoded_row.key_identity
								|| old_row.routing_locality != encoded_row.routing_locality
								|| old_row.row_hlc != encoded_row.row_hlc
								|| old_row.old_location.is_tombstone != encoded_row.is_tombstone
							{
								return Err(Error::new(
									ErrorKind::InvalidData,
									"layout recluster storage metadata does not match source row",
								));
							}
							let new_location = RecordLocation {
								hlc: encoded_row.row_hlc,
								record_row_index: encoded_row.record_row_index,
								is_tombstone: encoded_row.is_tombstone,
								..base_location
							};
							replacement.index_location(
								tenant_id,
								encoded_row.key_identity,
								new_location,
							);
							relocations.push(RelocatedRecord {
								tenant_id,
								key_identity: encoded_row.key_identity,
								routing_locality: encoded_row.routing_locality,
								old_location: old_row.old_location,
								new_location,
							});
						}
						rewritten_records = rewritten_records.saturating_add(1);
						multirow_recompact_count = multirow_recompact_count.saturating_add(1);
					}
				}
			}
		}

		if relocations.len() != records.len() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"layout recluster relocation count mismatch: old_rows={} new_rows={}",
					records.len(),
					relocations.len()
				),
			));
		}
		replacement.fsync().await?;
		let new_bytes = replacement.write_cursor();
		if !self
			.finish_compaction_with_replacement(
				slab_id,
				replacement_id,
				replacement.clone(),
				&records,
				&relocations,
			)
			.await?
		{
			let path = slab_path(&self.config.dir, replacement_id);
			let _ = unlink_local_file(self.file_ops.as_ref(), &path).await;
			return Ok(None);
		}

		let old_path = slab_path(&self.config.dir, slab_id);
		unlink_local_file(self.file_ops.as_ref(), &old_path).await?;
		if multirow_recompact_count > 0 {
			metrics().observe_storage_multirow_recompact(multirow_recompact_count);
		}

		Ok(Some(LayoutReclusterOutcome {
			reclaimed_bytes: old_bytes.saturating_sub(new_bytes),
			rewritten_records,
			relocated_rows: relocations.len(),
		}))
	}

	fn is_retention_prunable(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
		location: RecordLocation,
		retention_floor: HybridTimestamp,
	) -> bool {
		let Some(versions) = self.key_index.get(&(tenant_id, key_identity)) else {
			return false;
		};
		let older_versions = versions.partition_point(|stored| stored.hlc < retention_floor);
		let prune_prefix_len = older_versions.saturating_sub(1);
		prune_prefix_len > 0
			&& versions
				.iter()
				.take(prune_prefix_len)
				.any(|stored| *stored == location)
	}

	async fn mark_records_for_table_retention(
		&self,
		slab: &Arc<LocalSlabStore>,
		records: &mut [CompactionRecord],
		cluster_policy: MvccRetentionPolicy,
		table_lifecycle: &TableLifecycleCatalog,
		table_schemas: &TableSchemaCatalogV1,
		now_ms: u64,
	) -> std::io::Result<()> {
		if records.is_empty() {
			return Ok(());
		}
		let global_floor = cluster_retention_floor(cluster_policy, now_ms);
		let groups = physical_compaction_group_indices(records);
		for indices in groups.values() {
			let Some(first_idx) = indices.first().copied() else {
				continue;
			};
			let first = records[first_idx];
			let frame = slab.read(first.old_location).await?;
			self.mark_record_group_for_table_retention(
				&frame,
				indices,
				records,
				cluster_policy,
				table_lifecycle,
				table_schemas,
				now_ms,
				global_floor,
			)?;
		}
		Ok(())
	}

	fn mark_records_for_table_retention_from_blob(
		&self,
		plaintext: &[u8],
		records: &mut [CompactionRecord],
		cluster_policy: MvccRetentionPolicy,
		table_lifecycle: &TableLifecycleCatalog,
		table_schemas: &TableSchemaCatalogV1,
		now_ms: u64,
	) -> std::io::Result<()> {
		if records.is_empty() {
			return Ok(());
		}
		let global_floor = cluster_retention_floor(cluster_policy, now_ms);
		let groups = physical_compaction_group_indices(records);
		for indices in groups.values() {
			let Some(first_idx) = indices.first().copied() else {
				continue;
			};
			let first = records[first_idx];
			let frame = extract_record_from_blob(plaintext, first.old_location)?;
			self.mark_record_group_for_table_retention(
				&frame,
				indices,
				records,
				cluster_policy,
				table_lifecycle,
				table_schemas,
				now_ms,
				global_floor,
			)?;
		}
		Ok(())
	}

	fn mark_record_group_for_table_retention(
		&self,
		frame: &[u8],
		indices: &[usize],
		records: &mut [CompactionRecord],
		cluster_policy: MvccRetentionPolicy,
		table_lifecycle: &TableLifecycleCatalog,
		table_schemas: &TableSchemaCatalogV1,
		now_ms: u64,
		global_floor: Option<HybridTimestamp>,
	) -> std::io::Result<()> {
		if storage_multirow_index_entries_from_frame(frame)?.is_none() {
			if let Some(floor) = global_floor {
				for idx in indices {
					let record = records[*idx];
					records[*idx].reclaim = self.is_retention_prunable(
						record.tenant_id,
						record.key_identity,
						record.old_location,
						floor,
					);
					Self::record_retention_lag_if_retained(&mut records[*idx], floor, now_ms);
				}
			}
			return Ok(());
		}

		let batch = decode_record_batch(frame)?;
		if !has_storage_synthetic_columns(&batch) {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"multi-row retention record decoded without synthetic columns",
			));
		}
		for idx in indices {
			let record = records[*idx];
			let stored_table_object_id =
				storage_row_table_object_id_at(&batch, record.old_location.record_row_index)?;
			let Some(table_object_id) = stored_table_object_id else {
				records[*idx].retention_ambiguous = true;
				records[*idx].reclaim = false;
				continue;
			};
			let Some(descriptor) = table_schemas.latest_by_id(record.tenant_id, table_object_id)
			else {
				records[*idx].retention_ambiguous = true;
				records[*idx].reclaim = false;
				continue;
			};
			match descriptor.row_identity_policy {
				RowIdentityPolicy::PrimaryKey(_) => {
					if let Some(floor) = global_floor {
						records[*idx].reclaim = self.is_retention_prunable(
							record.tenant_id,
							record.key_identity,
							record.old_location,
							floor,
						);
						Self::record_retention_lag_if_retained(&mut records[*idx], floor, now_ms);
						if !records[*idx].reclaim
							&& record.old_location.is_tombstone
							&& record.old_location.hlc < floor
						{
							metrics()
								.observe_storage_pk_delete_tombstone_boundary_versions_retained(
									record.tenant_id,
									table_object_id,
									1,
								);
						}
					}
				}
				RowIdentityPolicy::AppendOnly | RowIdentityPolicy::AppendOnlyContentHash => {
					let effective = effective_table_retention(
						&cluster_policy,
						table_lifecycle.retention_policy(record.tenant_id, table_object_id),
						now_ms,
					);
					records[*idx].reclaim = effective.floor.is_some_and(|floor| {
						self.is_append_only_retention_prunable(
							record.tenant_id,
							record.key_identity,
							record.old_location,
							floor,
						)
					});
					if let Some(floor) = effective.floor {
						Self::record_retention_lag_if_retained(&mut records[*idx], floor, now_ms);
					}
				}
			}
		}
		Ok(())
	}

	fn record_retention_lag_if_retained(
		record: &mut CompactionRecord,
		floor: HybridTimestamp,
		now_ms: u64,
	) {
		if !record.reclaim && record.old_location.hlc < floor {
			record.retention_lag_seconds =
				now_ms.saturating_sub(record.old_location.hlc.physical_ms) / 1000;
		}
	}

	fn is_append_only_retention_prunable(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
		location: RecordLocation,
		table_floor: HybridTimestamp,
	) -> bool {
		if location.hlc >= table_floor {
			return false;
		}
		if location.is_tombstone {
			return true;
		}
		let Some(versions) = self.key_index.get(&(tenant_id, key_identity)) else {
			return false;
		};
		match versions
			.iter()
			.find(|stored| stored.hlc > location.hlc && stored.is_tombstone)
		{
			Some(tombstone) if tombstone.hlc >= table_floor => false,
			_ => true,
		}
	}

	fn mark_retention_prunable_row_tombstone_pairs(
		records: &mut [CompactionRecord],
		retention_floor: HybridTimestamp,
	) {
		if records.is_empty() {
			return;
		}
		let mut ordered = (0..records.len()).collect::<Vec<_>>();
		ordered.sort_by_key(|idx| {
			let record = records[*idx];
			(
				record.tenant_id,
				record.key_identity,
				record.old_location.hlc,
				record.old_location.offset,
				record.old_location.record_row_index,
			)
		});
		let mut prior_data_by_key: HashMap<TenantKey, usize> = HashMap::new();
		for idx in ordered {
			let record = records[idx];
			let key = (record.tenant_id, record.key_identity);
			if record.old_location.is_tombstone {
				if record.old_location.hlc < retention_floor
					&& let Some(data_idx) = prior_data_by_key.get(&key).copied()
				{
					records[data_idx].reclaim = true;
					records[idx].reclaim = true;
				}
				continue;
			}
			prior_data_by_key.insert(key, idx);
		}
	}

	fn retention_compaction_stats(records: &[CompactionRecord]) -> RetentionCompactionStats {
		let mut stats = RetentionCompactionStats::default();
		let mut reclaimed_data_by_key: HashSet<TenantKey> = HashSet::new();
		let mut reclaimed_tombstones_by_key: HashSet<TenantKey> = HashSet::new();
		for record in records {
			if record.retention_ambiguous && !record.reclaim {
				stats.ambiguous_rows_retained = stats.ambiguous_rows_retained.saturating_add(1);
			}
			if !record.reclaim {
				stats.max_lag_seconds = stats.max_lag_seconds.max(record.retention_lag_seconds);
				continue;
			}
			if record.old_location.is_tombstone {
				stats.pruned_tombstone_rows = stats.pruned_tombstone_rows.saturating_add(1);
				reclaimed_tombstones_by_key.insert((record.tenant_id, record.key_identity));
			} else {
				stats.pruned_data_rows = stats.pruned_data_rows.saturating_add(1);
				reclaimed_data_by_key.insert((record.tenant_id, record.key_identity));
			}
		}
		stats.pair_drops = reclaimed_tombstones_by_key
			.intersection(&reclaimed_data_by_key)
			.count();
		stats
	}

	async fn compact_sealed_slab_records(
		&self,
		slab_id: SlabId,
		slab: Arc<LocalSlabStore>,
		records: &[CompactionRecord],
		secondary_index_context: Option<SecondaryIndexCompactionContext<'_>>,
	) -> std::io::Result<Option<CompactionOutcome>> {
		if records.is_empty() || records.iter().all(|record| !record.reclaim) {
			return Ok(None);
		}

		let old_bytes = slab.write_cursor();
		let reclaimed_records = records.iter().filter(|record| record.reclaim).count();
		let retention_stats = Self::retention_compaction_stats(records);
		let survivors = records.iter().filter(|record| !record.reclaim).count();
		let old_secondary_index_descriptors = match secondary_index_context {
			Some(context) => match self
				.secondary_index_compaction_descriptors_for_source_slab(slab_id, context)
				.await
			{
				Ok(descriptors) => descriptors,
				Err(err) => {
					tracing::warn!(
						slab_id = slab_id.get(),
						error = %err,
						"failed to enumerate secondary index descriptors before compaction"
					);
					Vec::new()
				}
			},
			None => Vec::new(),
		};
		if survivors == 0 {
			self.finish_compaction_without_replacement(slab_id, &records)
				.await?;
			self.reconcile_secondary_index_segments_after_compaction(
				slab_id,
				None,
				secondary_index_context,
				old_secondary_index_descriptors,
			)
			.await;
			return Ok(Some(CompactionOutcome {
				reclaimed_bytes: old_bytes,
				reclaimed_records,
				retention_stats,
			}));
		}

		let replacement_id = self.reserve_slab_id().await;
		let replacement_path = slab_path(&self.config.dir, replacement_id);
		let replacement = Arc::new(
			LocalSlabStore::open(
				&replacement_path,
				replacement_id,
				self.file_ops.clone(),
				self.config.group_commit.clone(),
			)
			.await?,
		);
		let mut relocations = Vec::with_capacity(survivors);
		let physical_groups = physical_compaction_groups(records);
		let mut multirow_recompact_count = 0_u64;
		for group in physical_groups.values() {
			let Some(first) = group.first().copied() else {
				continue;
			};
			let frame = slab.read(first.old_location).await?;
			let Some(compacted) = compact_physical_record_frame(
				&frame,
				group,
				None,
				&self.config.row_group_writer,
				&self.config.encoding_writer,
				&self.config.l2_chunk_layout,
			)?
			else {
				continue;
			};
			if compacted.was_multirow {
				multirow_recompact_count = multirow_recompact_count.saturating_add(1);
			}
			let base_location = replacement
				.append(
					compacted.tenant_id,
					compacted.key_identity,
					compacted.hlc,
					&compacted.frame,
				)
				.await?;
			replacement.rollback_index_entry(
				compacted.tenant_id,
				compacted.key_identity,
				base_location,
			);
			for row in &compacted.rows {
				let new_location = RecordLocation {
					hlc: row.row_hlc,
					record_row_index: row.record_row_index,
					is_tombstone: row.old_location.is_tombstone,
					..base_location
				};
				replacement.index_location(row.tenant_id, row.key_identity, new_location);
				relocations.push(RelocatedRecord {
					tenant_id: row.tenant_id,
					key_identity: row.key_identity,
					routing_locality: row.routing_locality,
					old_location: row.old_location,
					new_location,
				});
			}
		}
		replacement.fsync().await?;
		let new_bytes = replacement.write_cursor();

		if !self
			.finish_compaction_with_replacement(
				slab_id,
				replacement_id,
				replacement.clone(),
				&records,
				&relocations,
			)
			.await?
		{
			let path = slab_path(&self.config.dir, replacement_id);
			let _ = unlink_local_file(self.file_ops.as_ref(), &path).await;
			return Ok(None);
		}

		self.reconcile_secondary_index_segments_after_compaction(
			slab_id,
			Some(replacement_id),
			secondary_index_context,
			old_secondary_index_descriptors,
		)
		.await;
		let old_path = slab_path(&self.config.dir, slab_id);
		unlink_local_file(self.file_ops.as_ref(), &old_path).await?;
		if multirow_recompact_count > 0 {
			metrics().observe_storage_multirow_recompact(multirow_recompact_count);
		}

		Ok(Some(CompactionOutcome {
			reclaimed_bytes: old_bytes.saturating_sub(new_bytes),
			reclaimed_records,
			retention_stats,
		}))
	}

	async fn finish_compaction_without_replacement(
		&self,
		slab_id: SlabId,
		records: &[CompactionRecord],
	) -> std::io::Result<()> {
		let source_slab_lease = self
			.acquire_secondary_index_source_slab_compaction_lease(slab_id)
			.await;
		let removed = {
			let state = self.state.lock().await;
			state.sealed.remove(&slab_id).is_some()
		};
		if !removed {
			return Ok(());
		}
		for record in records {
			self.rollback_indexed_location(
				record.tenant_id,
				record.key_identity,
				record.old_location,
			);
		}
		self.delete_persistent_records(records).await;
		let old_path = slab_path(&self.config.dir, slab_id);
		unlink_local_file(self.file_ops.as_ref(), &old_path).await?;
		self.secondary_index_source_slab_leases.remove(&slab_id);
		drop(source_slab_lease);
		Ok(())
	}

	async fn finish_compaction_with_replacement(
		&self,
		old_slab_id: SlabId,
		new_slab_id: SlabId,
		replacement: Arc<LocalSlabStore>,
		records: &[CompactionRecord],
		relocations: &[RelocatedRecord],
	) -> std::io::Result<bool> {
		let source_slab_lease = self
			.acquire_secondary_index_source_slab_compaction_lease(old_slab_id)
			.await;
		let state = self.state.lock().await;
		if !state.sealed.contains_key(&old_slab_id) || state.tiered.contains_key(&old_slab_id) {
			return Ok(false);
		}
		state.sealed.remove(&old_slab_id);
		state.sealed.insert(new_slab_id, replacement);
		for record in records {
			self.rollback_indexed_location(
				record.tenant_id,
				record.key_identity,
				record.old_location,
			);
		}
		for relocation in relocations {
			self.index_appended_location_with_locality(
				relocation.tenant_id,
				relocation.key_identity,
				relocation.routing_locality,
				relocation.new_location,
			);
			self.relocated_reads
				.insert(relocation.old_location, relocation.new_location);
		}
		drop(state);
		self.replace_persistent_records(records, relocations).await;
		self.secondary_index_source_slab_leases.remove(&old_slab_id);
		drop(source_slab_lease);
		Ok(true)
	}

	/// Fetch a single record from L2, then decide whether that L2
	/// read was hot enough to justify rehydrating the whole slab
	/// back into local L1 storage.
	async fn read_from_l2(
		&self,
		pointer: &TieredSlabPointer,
		location: RecordLocation,
	) -> std::io::Result<Vec<u8>> {
		let l2 = self.config.l2.as_ref().ok_or_else(|| {
			Error::new(
				ErrorKind::NotFound,
				format!(
					"slab {} is tiered but no ObjectStore configured on this manager",
					pointer.slab_id
				),
			)
		})?;
		let mut plaintext_for_rehydration = None;
		let frame = if pointer.is_encrypted {
			self.read_encrypted_record_from_l2(l2.as_ref(), pointer, location)
				.await?
		} else {
			let plaintext = self
				.fetch_full_tiered_plaintext(l2.as_ref(), pointer)
				.await?;
			let frame = extract_record_from_blob(&plaintext, location)?;
			plaintext_for_rehydration = Some(plaintext);
			frame
		};
		if self.note_l2_read(pointer.slab_id).await {
			let rehydrate_result = async {
				let plaintext = match plaintext_for_rehydration.take() {
					Some(plaintext) => plaintext,
					None => {
						self.fetch_full_tiered_plaintext(l2.as_ref(), pointer)
							.await?
					}
				};
				self.rehydrate_tiered_slab(pointer.clone(), bytes::Bytes::from(plaintext))
					.await
			}
			.await;
			if let Err(err) = rehydrate_result {
				tracing::warn!(
					slab_id = pointer.slab_id.get(),
					error = %err,
					"rehydration failed; continuing to serve slab from L2"
				);
			}
		}
		if let Ok(policy) = record_batch_compression_policy_from_ipc(&frame) {
			metrics().observe_storage_compression_l2_record_body_fetch(policy, frame.len() as u64);
		}
		Ok(frame)
	}

	async fn fetch_full_tiered_plaintext(
		&self,
		l2: &dyn ObjectStore,
		pointer: &TieredSlabPointer,
	) -> std::io::Result<Vec<u8>> {
		let blob = l2.get_bytes(&pointer.l2_uri).await?;
		validate_tiered_blob(pointer, blob.as_ref())?;
		decode_tiered_blob(self.config.crypto.as_ref(), pointer, blob.as_ref())
	}

	async fn read_encrypted_record_from_l2(
		&self,
		l2: &dyn ObjectStore,
		pointer: &TieredSlabPointer,
		location: RecordLocation,
	) -> std::io::Result<Vec<u8>> {
		let metadata = l2.head(&pointer.l2_uri).await?;
		let tail_start = metadata
			.size_bytes
			.saturating_sub(self.config.rehydration.tail_read_window_bytes);
		let tail_window = l2
			.get_range(&pointer.l2_uri, tail_start..metadata.size_bytes)
			.await?;
		let plaintext_start = usize::try_from(location.offset).map_err(|_| {
			Error::new(
				ErrorKind::InvalidData,
				format!("record offset {} does not fit into usize", location.offset),
			)
		})?;
		let plaintext_end_u64 = location
			.offset
			.checked_add(location.length as u64)
			.ok_or_else(|| {
				Error::new(
					ErrorKind::InvalidData,
					format!(
						"record range overflows u64 for slab {} at offset {}",
						pointer.slab_id, location.offset,
					),
				)
			})?;
		let plaintext_end = usize::try_from(plaintext_end_u64).map_err(|_| {
			Error::new(
				ErrorKind::InvalidData,
				format!("record end {} does not fit into usize", plaintext_end_u64),
			)
		})?;
		let additional_metadata_range = self.config.crypto.additional_tiered_metadata_range(
			pointer.slab_id,
			metadata.size_bytes,
			tail_window.as_ref(),
		)?;
		let additional_metadata = match additional_metadata_range {
			Some(range) => Some(l2.get_range(&pointer.l2_uri, range).await?),
			None => None,
		};
		let plan = self.config.crypto.plan_tiered_range_read(
			pointer.slab_id,
			metadata.size_bytes,
			tail_window.as_ref(),
			additional_metadata.as_deref(),
			plaintext_start..plaintext_end,
		)?;
		let footer = plan.footer();
		let ciphertext = l2
			.get_range(&pointer.l2_uri, plan.ciphertext_range_in_object())
			.await?;
		let record_bytes = plan.decrypt(ciphertext.as_ref())?;
		validate_tiered_record_header_tenant(&record_bytes, footer.tenant_id, pointer.slab_id)?;
		extract_record_from_blob(
			&record_bytes,
			RecordLocation {
				offset: 0,
				..location
			},
		)
	}

	async fn try_read_encoding_indexed_pages_from_l2(
		&self,
		l2: Arc<dyn ObjectStore>,
		pointer: &TieredSlabPointer,
		object_size_bytes: u64,
		tail_window: &[u8],
		additional_metadata: Option<&[u8]>,
		whole_record_range: std::ops::Range<usize>,
		body_location: &RecordLocation,
		header_size: u64,
		record_directory: &RecordBufferDirectory,
		page_directory: &crate::storage::row_group::RecordPageDirectory,
		descriptor: &TableSchemaDescriptorV1,
		requirement: &ColumnFetchRequirement,
		selected_record_rows: &[RecordRowIndex],
		selected_groups: &[RowGroupIndex],
		planning_started: Instant,
	) -> std::io::Result<Option<ProjectedL2RecordRead>> {
		let Some(encoding_directory) = record_directory.encoding_directory.as_ref() else {
			return Ok(None);
		};
		if selected_record_rows.is_empty() || !self.config.l2_encoding_fetch.enabled {
			return Ok(None);
		}
		let plan = match build_encoding_indexed_page_plan(
			record_directory,
			page_directory,
			encoding_directory,
			requirement,
			selected_record_rows,
			selected_groups,
		) {
			Ok(Some(plan)) => plan,
			Ok(None) => return Ok(None),
			Err(reason) => {
				metrics().observe_storage_l2_encoding_fallback(reason.as_str());
				return Ok(None);
			}
		};
		if plan.fetches.len()
			> self
				.config
				.l2_encoding_fetch
				.max_simd_pages_per_record
				.get() as usize
		{
			metrics().observe_storage_l2_encoding_fallback(
				EncodingFallbackReason::TooManyPages.as_str(),
			);
			return Ok(None);
		}

		let mut selected_body_byte_ranges = plan
			.fetches
			.iter()
			.map(|fetch| fetch.body_range)
			.collect::<Vec<_>>();
		coalesce_manager_record_body_ranges(&mut selected_body_byte_ranges)?;
		let selected_body_ranges = selected_body_byte_ranges
			.iter()
			.map(|range| TieredPlaintextRange::new(range.start, range.len))
			.collect::<Vec<_>>();
		let selected_record_ranges =
			record_body_ranges_to_record_ranges(header_size, &selected_body_ranges)?;
		let tiered_plan = self.config.crypto.plan_tiered_multi_range_read(
			pointer.slab_id,
			object_size_bytes,
			tail_window,
			additional_metadata,
			whole_record_range,
			&selected_record_ranges,
		)?;
		let phase_c_fetch = crate::storage::row_group::row_group_range_fetch_plan_from_tiered(
			&tiered_plan,
			&selected_body_byte_ranges,
		);
		let row_group_plan = crate::storage::row_group::RowGroupFetchPlan {
			record_id: page_directory.record_id,
			directory: page_directory.clone(),
			surviving_groups: plan.selected_groups.clone(),
			predicate_fetched_groups: Vec::new(),
			phase_b_fetch: None,
			phase_c_fetch,
			logical_schema_fingerprint: page_directory.logical_schema_fingerprint,
		};
		let strategy = pick_point_row_group_fetch_strategy(
			&self.config.l2_row_group_fetch,
			body_location,
			page_directory,
			row_group_plan,
		);
		let L2FetchStrategy::RowGroup(row_group_plan) = strategy else {
			metrics().observe_storage_l2_encoding_fallback(
				EncodingFallbackReason::DenseSelection.as_str(),
			);
			return Ok(None);
		};
		observe_l2_row_group_fetch_strategy(
			metrics(),
			&L2FetchStrategy::RowGroup(row_group_plan.clone()),
			planning_started.elapsed(),
		);

		let plaintext_extracts = if selected_record_ranges.is_empty() {
			Vec::new()
		} else {
			let ciphertext_runs = fetch_tiered_ciphertext_runs(
				l2,
				pointer,
				&tiered_plan,
				self.config.l2_row_group_fetch.max_parallel_range_gets.get() as usize,
			)
			.await?;
			tiered_plan.decrypt(&ciphertext_runs)?
		};
		let projected = materialize_encoding_indexed_pages(
			record_directory,
			requirement,
			&plan,
			&selected_body_ranges,
			&plaintext_extracts,
			selected_record_rows.len(),
			descriptor,
		)
		.map_err(arrow_io_error)?;
		Ok(Some(ProjectedL2RecordRead {
			projected,
			projected_first_record_row_index: RecordRowIndex::ZERO,
			indexed_rows_materialized: true,
			encoding_materialize_audit: encoding_materialize_audit_for_indexed_plan(&plan),
			strategy: L2FetchStrategy::RowGroup(row_group_plan),
		}))
	}

	async fn read_encoded_aggregate_record_from_l2(
		&self,
		l2: Arc<dyn ObjectStore>,
		pointer: &TieredSlabPointer,
		location: RecordLocation,
		descriptor: &TableSchemaDescriptorV1,
		request: &AggregateRequest,
		predicate: &crate::schema::pbc::Program,
	) -> std::io::Result<Option<EncodedAggregateL2RecordRead>> {
		let observe_fallback = |reason: EncodingComputeFallbackReason| {
			observe_encoding_compute_aggregate_request(request, "dense_fallback", reason.as_str());
		};
		if !pointer.is_encrypted {
			observe_fallback(EncodingComputeFallbackReason::DirectoryMissing);
			return Ok(None);
		}
		let body_location = record_body_location(location)?;
		let whole_record_range = record_plaintext_range_for_l2(location)?;
		let metadata = l2.head(&pointer.l2_uri).await?;
		let tail_start = metadata
			.size_bytes
			.saturating_sub(self.config.rehydration.tail_read_window_bytes);
		let tail_window = l2
			.get_range(&pointer.l2_uri, tail_start..metadata.size_bytes)
			.await?;
		let additional_metadata_range = self.config.crypto.additional_tiered_metadata_range(
			pointer.slab_id,
			metadata.size_bytes,
			tail_window.as_ref(),
		)?;
		let additional_metadata = match additional_metadata_range {
			Some(range) => Some(l2.get_range(&pointer.l2_uri, range).await?),
			None => None,
		};

		let mut probe = initial_metadata_probe_plan(&self.config.l2_column_fetch, &body_location)
			.map_err(columnar_io_error)?;
		let header_size = slab_record_header_size_u64()?;
		let directory = loop {
			let prefix_len = header_size
				.checked_add(probe.window_bytes.get())
				.ok_or_else(|| Error::new(ErrorKind::InvalidData, "metadata prefix overflow"))?;
			let prefix_range = TieredPlaintextRange::new(
				RecordBodyByteOffset::new(0),
				RecordBodyByteLen::try_new(prefix_len).map_err(|err| {
					Error::new(
						ErrorKind::InvalidData,
						format!("metadata prefix length rejected: {err}"),
					)
				})?,
			);
			let prefix_plan = self.config.crypto.plan_tiered_multi_range_read(
				pointer.slab_id,
				metadata.size_bytes,
				tail_window.as_ref(),
				additional_metadata.as_deref(),
				whole_record_range.clone(),
				&[prefix_range],
			)?;
			let prefix_ciphertext = fetch_tiered_ciphertext_runs(
				l2.clone(),
				pointer,
				&prefix_plan,
				self.config.l2_column_fetch.max_parallel_range_gets.get() as usize,
			)
			.await?;
			let prefix_plaintext = prefix_plan.decrypt(&prefix_ciphertext)?;
			let Some(prefix_plaintext) = prefix_plaintext.first() else {
				return Err(Error::new(
					ErrorKind::InvalidData,
					"metadata prefix decrypt returned no plaintext",
				));
			};
			let header_size_usize = usize::try_from(header_size).map_err(|_| {
				Error::new(
					ErrorKind::InvalidData,
					"slab record header size exceeds usize",
				)
			})?;
			if prefix_plaintext.len() < header_size_usize {
				return Err(Error::new(
					ErrorKind::UnexpectedEof,
					"metadata prefix shorter than slab record header",
				));
			}
			validate_tiered_record_header_tenant(
				&prefix_plaintext[..header_size_usize],
				prefix_plan.footer().tenant_id,
				pointer.slab_id,
			)?;
			let metadata_window_bytes = &prefix_plaintext[header_size_usize..];
			match parse_record_buffer_directory(
				TieredRecordId {
					slab_id: pointer.slab_id,
					record_offset: RecordBodyByteOffset::new(location.offset),
				},
				PlaintextByteRange {
					start: 0,
					len: u64::from(body_location.length),
				},
				metadata_window_bytes,
				descriptor,
			) {
				Ok(directory) => break directory,
				Err(crate::storage::columnar::ColumnFetchError::MetadataTruncated {
					needed,
					..
				}) => {
					let next = next_metadata_probe_plan(
						&self.config.l2_column_fetch,
						&body_location,
						probe.window_bytes,
						needed,
					)
					.map_err(columnar_io_error)?;
					match next {
						Some(next) => probe = next,
						None => {
							observe_fallback(EncodingComputeFallbackReason::DirectoryMissing);
							return Ok(None);
						}
					}
				}
				Err(crate::storage::columnar::ColumnFetchError::UnsupportedArrowLayout) => {
					observe_fallback(EncodingComputeFallbackReason::UnsupportedArrowLayout);
					return Ok(None);
				}
				Err(err) => return Err(columnar_io_error(err)),
			}
		};

		let Some(page_directory) = directory.page_directory.as_ref() else {
			observe_fallback(EncodingComputeFallbackReason::DirectoryMissing);
			return Ok(None);
		};
		let Some(encoding_directory) = directory.encoding_directory.as_ref() else {
			observe_fallback(EncodingComputeFallbackReason::DirectoryMissing);
			return Ok(None);
		};
		let Some(group) = crate::storage::row_group::row_group_for_record_row(
			page_directory,
			location.record_row_index,
		) else {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"record row index {} is not covered by row-group directory",
					location.record_row_index.get()
				),
			));
		};
		let row_count = RowCount::new(u64::from(group.row_count.get()));
		let mut selected_mask =
			encoding_compute_single_row_mask(row_count, group, location.record_row_index)
				.map_err(encoding_compute_invariant_error)?;
		let mut predicate_fetched_groups = Vec::new();
		let mut phase_b_fetch = None;
		let predicate_tree = predicate.predicate_tree();
		if !matches!(
			predicate_tree,
			crate::schema::pbc::PredicateTreeV1::AlwaysTrue
		) {
			let Some(_) = encoded_predicate_node_from_tree(&predicate_tree) else {
				observe_fallback(EncodingComputeFallbackReason::UnsupportedProgram);
				return Ok(None);
			};
			let selected_groups = [group.row_group_index];
			predicate_fetched_groups.push(group.row_group_index);
			let Some(encoded_phase_b) = self
				.try_read_encoding_compute_phase_b_from_l2(
					l2.clone(),
					pointer,
					metadata.size_bytes,
					tail_window.as_ref(),
					additional_metadata.as_deref(),
					whole_record_range.clone(),
					header_size,
					&directory,
					page_directory,
					predicate,
					&selected_groups,
					&predicate_fetched_groups,
				)
				.await?
			else {
				return Ok(None);
			};
			let mask = encoded_phase_b
				.outcome
				.survival_masks
				.get(&group.row_group_index)
				.ok_or_else(|| {
					Error::new(
						ErrorKind::InvalidData,
						format!(
							"Encoding compute aggregate mask missing for row group {}",
							group.row_group_index.get()
						),
					)
				})?;
			let predicate_mask = EncodedRowMask::from_arrow_boolean_array(mask);
			selected_mask = selected_mask
				.and(&predicate_mask)
				.map_err(encoding_compute_invariant_error)?;
			phase_b_fetch = Some(encoded_phase_b.phase_b_fetch);
		}

		let aggregate_plan = match build_encoding_compute_aggregate_plan(
			&directory,
			page_directory,
			encoding_directory,
			request,
			group,
			&self.config.encoding_compute,
			header_size,
		) {
			Ok(plan) => plan,
			Err(reason) => {
				observe_fallback(reason);
				return Ok(None);
			}
		};
		let aggregate_tiered_plan = self.config.crypto.plan_tiered_multi_range_read(
			pointer.slab_id,
			metadata.size_bytes,
			tail_window.as_ref(),
			additional_metadata.as_deref(),
			whole_record_range,
			&aggregate_plan.record_ranges,
		)?;
		let phase_c_fetch = crate::storage::row_group::row_group_range_fetch_plan_from_tiered(
			&aggregate_tiered_plan,
			&aggregate_plan.record_body_ranges,
		);
		let plaintext = if aggregate_plan.record_ranges.is_empty() {
			Vec::new()
		} else {
			let ciphertext = fetch_tiered_ciphertext_runs(
				l2,
				pointer,
				&aggregate_tiered_plan,
				self.config
					.l2_row_group_fetch
					.max_parallel_range_gets
					.get()
					.max(1) as usize,
			)
			.await?;
			aggregate_tiered_plan.decrypt(&ciphertext)?
		};
		let mut context = EncodedAggregateContext::new(row_count);
		populate_encoding_compute_non_null_counts(
			&mut context,
			&directory,
			page_directory,
			group,
			request,
		);
		for fetch in &aggregate_plan.fetches {
			if let Some(validity_fetch) = &fetch.validity_fetch {
				let validity = extract_record_body_bytes(
					validity_fetch.body_range,
					&aggregate_plan.body_ranges,
					&plaintext,
				)
				.map_err(arrow_io_error)?;
				let validity = if let Some(encoding) = &validity_fetch.encoding {
					let cascade = validity_fetch.cascade.as_ref().ok_or_else(|| {
						Error::new(
							ErrorKind::InvalidData,
							"Encoding compute aggregate validity fetch missing cascade",
						)
					})?;
					decode_encoded_buffer(&validity, &DataType::Boolean, cascade, encoding)
						.map_err(encoding_compute_decode_error)?
				} else {
					validity
				};
				let validity_mask = evaluate_validity_bitmap(Some(&validity), row_count, false)
					.map_err(encoding_compute_invariant_error)?;
				context = context
					.with_validity_mask(fetch.column, validity_mask)
					.map_err(encoding_compute_invariant_error)?;
			}
			if let Some(value_body_range) = fetch.value_body_range {
				let encoded = extract_record_body_bytes(
					value_body_range,
					&aggregate_plan.body_ranges,
					&plaintext,
				)
				.map_err(arrow_io_error)?;
				let encoding = fetch.encoding.as_ref().ok_or_else(|| {
					Error::new(
						ErrorKind::InvalidData,
						"Encoding compute aggregate value fetch missing encoding",
					)
				})?;
				let cascade = fetch.cascade.as_ref().ok_or_else(|| {
					Error::new(
						ErrorKind::InvalidData,
						"Encoding compute aggregate value fetch missing cascade",
					)
				})?;
				context = context
					.with_native_encoded_column(
						fetch.column,
						fetch.data_type.clone(),
						&encoded,
						encoding,
						cascade,
					)
					.map_err(encoding_compute_invariant_error)?;
			}
		}
		let Some(partials) =
			try_fold_encoded_aggregate_request(request, &context, Some(&selected_mask))
				.map_err(encoding_compute_invariant_error)?
		else {
			observe_fallback(EncodingComputeFallbackReason::AggregateUnsupported);
			return Ok(None);
		};
		let rows_admitted = selected_mask
			.popcount()
			.map_err(encoding_compute_invariant_error)?
			.get();
		observe_encoding_compute_aggregate_request(request, "exact", "none");
		if aggregate_plan.pages_fetched > 0 {
			metrics().observe_storage_l2_encoding_compute_pages_fetched(
				"selected",
				aggregate_plan.pages_fetched,
			);
		}
		let row_group_plan = crate::storage::row_group::RowGroupFetchPlan {
			record_id: page_directory.record_id,
			directory: page_directory.clone(),
			surviving_groups: vec![group.row_group_index],
			predicate_fetched_groups,
			phase_b_fetch,
			phase_c_fetch,
			logical_schema_fingerprint: page_directory.logical_schema_fingerprint,
		};
		Ok(Some(EncodedAggregateL2RecordRead {
			partials,
			rows_admitted,
			strategy: L2FetchStrategy::RowGroup(row_group_plan),
		}))
	}

	async fn try_read_encoding_compute_phase_b_from_l2(
		&self,
		l2: Arc<dyn ObjectStore>,
		pointer: &TieredSlabPointer,
		object_size_bytes: u64,
		tail_window: &[u8],
		additional_metadata: Option<&[u8]>,
		whole_record_range: std::ops::Range<usize>,
		header_size: u64,
		record_directory: &RecordBufferDirectory,
		page_directory: &crate::storage::row_group::RecordPageDirectory,
		predicate: &crate::schema::pbc::Program,
		selected_groups: &[RowGroupIndex],
		predicate_fetched_groups: &[RowGroupIndex],
	) -> std::io::Result<Option<EncodingComputePhaseBRead>> {
		if predicate_fetched_groups.is_empty() {
			return Ok(None);
		}
		let observe_fallback = |reason: EncodingComputeFallbackReason| {
			metrics().observe_storage_l2_encoding_compute_plan("dense_fallback", reason.as_str());
		};
		if !self.config.encoding_compute.enabled {
			observe_fallback(EncodingComputeFallbackReason::ReaderDisabled);
			return Ok(None);
		}
		let Some(encoding_directory) = record_directory.encoding_directory.as_ref() else {
			observe_fallback(EncodingComputeFallbackReason::DirectoryMissing);
			return Ok(None);
		};
		let Some(encoded_predicate) = encoded_predicate_node_from_tree(&predicate.predicate_tree())
		else {
			observe_fallback(EncodingComputeFallbackReason::UnsupportedProgram);
			return Ok(None);
		};
		let observe_page_fallback = |reason: EncodingComputeFallbackReason| {
			metrics().observe_storage_l2_encoding_compute_plan("page_fallback", reason.as_str());
		};
		match build_encoding_predicate_page_plan(
			record_directory,
			page_directory,
			encoding_directory,
			&encoded_predicate,
			selected_groups,
			predicate_fetched_groups,
			&self.config.encoding_compute,
			header_size,
		) {
			Ok(plan) => {
				let tiered_plan = self.config.crypto.plan_tiered_multi_range_read(
					pointer.slab_id,
					object_size_bytes,
					tail_window,
					additional_metadata,
					whole_record_range,
					&plan.record_ranges,
				)?;
				let phase_b_fetch =
					crate::storage::row_group::row_group_range_fetch_plan_from_tiered(
						&tiered_plan,
						&plan.record_body_ranges,
					);
				let plaintext = if plan.record_ranges.is_empty() {
					Vec::new()
				} else {
					let ciphertext = fetch_tiered_ciphertext_runs(
						l2.clone(),
						pointer,
						&tiered_plan,
						self.config
							.l2_row_group_fetch
							.max_parallel_range_gets
							.get()
							.max(1) as usize,
					)
					.await?;
					tiered_plan.decrypt(&ciphertext)?
				};
				let mut outcome = crate::storage::row_group::PhaseBOutcome::default();
				let mut total_mask_bytes = 0_u64;
				let mut rows_scanned = 0_u64;
				let mut rows_selected = 0_u64;
				let predicate_fetched = predicate_fetched_groups
					.iter()
					.copied()
					.collect::<BTreeSet<_>>();
				for group_index in selected_groups {
					if !predicate_fetched.contains(group_index) {
						continue;
					}
					let group = page_directory
						.row_groups
						.iter()
						.find(|group| group.row_group_index == *group_index)
						.ok_or_else(|| {
							Error::new(
								ErrorKind::InvalidData,
								format!(
									"Encoding predicate page selected row group {} missing",
									group_index.get()
								),
							)
						})?;
					let row_count = RowCount::new(u64::from(group.row_count.get()));
					let mut leaf_slices =
						vec![Vec::<PredicatePageMaskSlice>::new(); plan.leaves.len()];
					for decision in plan
						.decisions
						.iter()
						.filter(|decision| decision.row_group_index == *group_index)
					{
						let leaf = leaf_slices.get_mut(decision.leaf_index).ok_or_else(|| {
							Error::new(
								ErrorKind::InvalidData,
								"Encoding predicate page decision leaf out of range",
							)
						})?;
						match decision.classification {
							EncodingPredicatePageDecisionKind::Disjoint => {
								leaf.push(PredicatePageMaskSlice::Disjoint {
									first_row: RecordRowIndex::new(decision.first_row),
									row_count: RowCount::new(u64::from(decision.row_count)),
								});
							}
							EncodingPredicatePageDecisionKind::ExactMatch => {
								leaf.push(PredicatePageMaskSlice::ExactMatch {
									first_row: RecordRowIndex::new(decision.first_row),
									row_count: RowCount::new(u64::from(decision.row_count)),
								});
							}
							EncodingPredicatePageDecisionKind::Maybe { fetch_index } => {
								let fetch = plan.fetches.get(fetch_index).ok_or_else(|| {
									Error::new(
										ErrorKind::InvalidData,
										"Encoding predicate page fetch index out of range",
									)
								})?;
								let fetched = match extract_record_body_bytes(
									fetch.body_range,
									&plan.body_ranges,
									&plaintext,
								) {
									Ok(encoded) => encoded,
									Err(_) => {
										observe_page_fallback(
											EncodingComputeFallbackReason::UnsupportedArrowLayout,
										);
										return Ok(None);
									}
								};
								let Some(coercion) =
									coerce_literal(fetch.atom.literal.as_ref(), &fetch.data_type)
								else {
									observe_page_fallback(
										EncodingComputeFallbackReason::UnsupportedProgram,
									);
									return Ok(None);
								};
								let evaluator = evaluator_kind_for_cascade(&fetch.cascade)
									.ok_or_else(|| {
										Error::new(
											ErrorKind::InvalidData,
											"Encoding predicate page fetch carried unsupported cascade",
										)
									})?;
								let started = Instant::now();
								let mask = match evaluate_encoded_predicate(
									&fetched,
									&fetch.encoding,
									&fetch.cascade,
									&fetch.atom,
									&coercion,
								) {
									Ok(mask) => mask,
									Err(_) => {
										observe_page_fallback(
											EncodingComputeFallbackReason::UnsupportedCascade,
										);
										return Ok(None);
									}
								};
								metrics().observe_storage_l2_encoding_compute_kernel(
									evaluator.as_str(),
									fetch.cascade.kind().as_str(),
									started.elapsed(),
								);
								if mask.rows() != RowCount::new(u64::from(decision.row_count)) {
									return Err(encoding_compute_invariant_error(
										EncodingComputeRejectCode::MaskLengthMismatch,
									));
								}
								leaf.push(PredicatePageMaskSlice::Maybe {
									first_row: RecordRowIndex::new(decision.first_row),
									mask,
								});
							}
						}
					}
					let mut leaf_masks = Vec::with_capacity(plan.leaves.len());
					for slices in leaf_slices {
						let mask = stitch_predicate_page_mask(&PredicatePageFetchPlan {
							row_count,
							slices,
						})
						.map_err(encoding_compute_invariant_error)?;
						if !encoding_compute_mask_selectivity_allowed(
							&mask,
							&self.config.encoding_compute,
						)
						.map_err(encoding_compute_invariant_error)?
						{
							observe_page_fallback(EncodingComputeFallbackReason::DenseSelection);
							return Ok(None);
						}
						leaf_masks.push(mask);
					}
					let mask = encoding_compute_eval_node(&plan.node, &leaf_masks)
						.map_err(encoding_compute_invariant_error)?;
					let selected = mask
						.popcount()
						.map_err(encoding_compute_invariant_error)?
						.get();
					total_mask_bytes = total_mask_bytes.saturating_add(mask.byte_len() as u64);
					if total_mask_bytes
						> self.config.encoding_compute.max_mask_bytes_per_record.get()
					{
						observe_page_fallback(EncodingComputeFallbackReason::TooManyMaskBytes);
						return Ok(None);
					}
					rows_scanned = rows_scanned.saturating_add(row_count.get());
					rows_selected = rows_selected.saturating_add(selected);
					let boolean_mask = mask
						.to_arrow_boolean_array()
						.map_err(encoding_compute_invariant_error)?;
					outcome.survival_masks.insert(*group_index, boolean_mask);
				}
				metrics().observe_storage_l2_encoding_compute_plan("page_exact", "none");
				metrics().observe_storage_l2_encoding_compute_rows("scanned", rows_scanned);
				metrics().observe_storage_l2_encoding_compute_rows("selected", rows_selected);
				metrics().observe_storage_l2_encoding_compute_rows(
					"avoided",
					rows_scanned.saturating_sub(rows_selected),
				);
				metrics().observe_storage_l2_encoding_compute_mask_bytes(total_mask_bytes);
				metrics().observe_storage_l2_encoding_compute_pages_fetched(
					"selected",
					plan.fetches.len() as u64,
				);
				return Ok(Some(EncodingComputePhaseBRead {
					outcome,
					phase_b_fetch,
				}));
			}
			Err(reason) => observe_page_fallback(reason),
		}
		let plan = match build_encoding_compute_phase_b_plan(
			record_directory,
			page_directory,
			encoding_directory,
			&encoded_predicate,
			selected_groups,
			predicate_fetched_groups,
			&self.config.encoding_compute,
			header_size,
		) {
			Ok(plan) => plan,
			Err(reason) => {
				observe_fallback(reason);
				return Ok(None);
			}
		};
		let tiered_plan = self.config.crypto.plan_tiered_multi_range_read(
			pointer.slab_id,
			object_size_bytes,
			tail_window,
			additional_metadata,
			whole_record_range,
			&plan.record_ranges,
		)?;
		let phase_b_fetch = crate::storage::row_group::row_group_range_fetch_plan_from_tiered(
			&tiered_plan,
			&plan.record_body_ranges,
		);
		let plaintext = if plan.record_ranges.is_empty() {
			Vec::new()
		} else {
			let ciphertext = fetch_tiered_ciphertext_runs(
				l2,
				pointer,
				&tiered_plan,
				self.config
					.l2_row_group_fetch
					.max_parallel_range_gets
					.get()
					.max(1) as usize,
			)
			.await?;
			tiered_plan.decrypt(&ciphertext)?
		};
		let mut outcome = crate::storage::row_group::PhaseBOutcome::default();
		let mut total_mask_bytes = 0_u64;
		let mut rows_scanned = 0_u64;
		let mut rows_selected = 0_u64;
		let predicate_fetched = predicate_fetched_groups
			.iter()
			.copied()
			.collect::<BTreeSet<_>>();
		for group_index in selected_groups {
			if !predicate_fetched.contains(group_index) {
				continue;
			}
			let group = page_directory
				.row_groups
				.iter()
				.find(|group| group.row_group_index == *group_index)
				.ok_or_else(|| {
					Error::new(
						ErrorKind::InvalidData,
						format!(
							"Encoding compute selected row group {} missing",
							group_index.get()
						),
					)
				})?;
			let row_count = RowCount::new(u64::from(group.row_count.get()));
			let mut value_masks = vec![None::<EncodedRowMask>; plan.leaves.len()];
			let mut validity_masks = vec![None::<EncodedRowMask>; plan.leaves.len()];
			for fetch in plan
				.fetches
				.iter()
				.filter(|fetch| fetch.row_group_index == *group_index)
			{
				let fetched = match extract_record_body_bytes(
					fetch.body_range,
					&plan.body_ranges,
					&plaintext,
				) {
					Ok(encoded) => encoded,
					Err(_) => {
						observe_fallback(EncodingComputeFallbackReason::UnsupportedArrowLayout);
						return Ok(None);
					}
				};
				match &fetch.kind {
					EncodingComputePhaseBFetchKind::Encoded { encoding, cascade } => {
						let EncodingComputePhaseBLeaf::Encoded {
							atom, data_type, ..
						} = &plan.leaves[fetch.leaf_index]
						else {
							return Err(Error::new(
								ErrorKind::InvalidData,
								"Encoding compute value fetch targeted non-value leaf",
							));
						};
						let Some(coercion) = coerce_literal(atom.literal.as_ref(), data_type)
						else {
							observe_fallback(EncodingComputeFallbackReason::UnsupportedProgram);
							return Ok(None);
						};
						let evaluator = evaluator_kind_for_cascade(cascade).ok_or_else(|| {
							Error::new(
								ErrorKind::InvalidData,
								"Encoding compute fetch carried unsupported cascade",
							)
						})?;
						let started = Instant::now();
						let mask = match evaluate_encoded_predicate(
							&fetched, encoding, cascade, atom, &coercion,
						) {
							Ok(mask) => mask,
							Err(_) => {
								observe_fallback(EncodingComputeFallbackReason::UnsupportedCascade);
								return Ok(None);
							}
						};
						metrics().observe_storage_l2_encoding_compute_kernel(
							evaluator.as_str(),
							cascade.kind().as_str(),
							started.elapsed(),
						);
						value_masks[fetch.leaf_index] = Some(mask);
					}
					EncodingComputePhaseBFetchKind::Validity { encoding, cascade } => {
						let match_nulls = match &plan.leaves[fetch.leaf_index] {
							EncodingComputePhaseBLeaf::Encoded { .. } => false,
							EncodingComputePhaseBLeaf::Validity {
								selected_when_no_nulls,
								..
							} => !*selected_when_no_nulls,
						};
						let validity = if let Some(encoding) = encoding {
							let cascade = cascade.as_ref().ok_or_else(|| {
								Error::new(
									ErrorKind::InvalidData,
									"Encoding compute validity fetch missing cascade",
								)
							})?;
							decode_encoded_buffer(&fetched, &DataType::Boolean, cascade, encoding)
								.map_err(encoding_compute_decode_error)?
						} else {
							fetched
						};
						let mask =
							evaluate_validity_bitmap(Some(&validity), row_count, match_nulls)
								.map_err(encoding_compute_invariant_error)?;
						validity_masks[fetch.leaf_index] = Some(mask);
					}
				}
			}
			let mut leaf_masks = Vec::with_capacity(plan.leaves.len());
			for (leaf_index, leaf) in plan.leaves.iter().enumerate() {
				let mask = match leaf {
					EncodingComputePhaseBLeaf::Encoded { .. } => {
						let Some(value_mask) = value_masks[leaf_index].take() else {
							return Err(Error::new(
								ErrorKind::InvalidData,
								"Encoding compute encoded leaf missing value mask",
							));
						};
						if let Some(validity_mask) = validity_masks[leaf_index].take() {
							value_mask
								.and(&validity_mask)
								.map_err(encoding_compute_invariant_error)?
						} else {
							value_mask
						}
					}
					EncodingComputePhaseBLeaf::Validity {
						selected_when_no_nulls,
						..
					} => validity_masks[leaf_index].take().unwrap_or_else(|| {
						if *selected_when_no_nulls {
							EncodedRowMask::All { rows: row_count }
						} else {
							EncodedRowMask::None { rows: row_count }
						}
					}),
				};
				if !encoding_compute_mask_selectivity_allowed(&mask, &self.config.encoding_compute)
					.map_err(encoding_compute_invariant_error)?
				{
					observe_fallback(EncodingComputeFallbackReason::DenseSelection);
					return Ok(None);
				}
				leaf_masks.push(mask);
			}
			let mask = encoding_compute_eval_node(&plan.node, &leaf_masks)
				.map_err(encoding_compute_invariant_error)?;
			let selected = mask
				.popcount()
				.map_err(encoding_compute_invariant_error)?
				.get();
			total_mask_bytes = total_mask_bytes.saturating_add(mask.byte_len() as u64);
			if total_mask_bytes > self.config.encoding_compute.max_mask_bytes_per_record.get() {
				observe_fallback(EncodingComputeFallbackReason::TooManyMaskBytes);
				return Ok(None);
			}
			rows_scanned = rows_scanned.saturating_add(row_count.get());
			rows_selected = rows_selected.saturating_add(selected);
			let boolean_mask = mask
				.to_arrow_boolean_array()
				.map_err(encoding_compute_invariant_error)?;
			outcome.survival_masks.insert(*group_index, boolean_mask);
		}
		let pages_fetched = plan
			.fetches
			.iter()
			.map(|fetch| match &fetch.kind {
				EncodingComputePhaseBFetchKind::Encoded { encoding, .. } => {
					encoding.page_end_offsets.len().max(1) as u64
				}
				EncodingComputePhaseBFetchKind::Validity { .. } => 1,
			})
			.sum::<u64>();
		metrics().observe_storage_l2_encoding_compute_plan("exact", "none");
		metrics().observe_storage_l2_encoding_compute_rows("scanned", rows_scanned);
		metrics().observe_storage_l2_encoding_compute_rows("selected", rows_selected);
		metrics().observe_storage_l2_encoding_compute_rows(
			"avoided",
			rows_scanned.saturating_sub(rows_selected),
		);
		metrics().observe_storage_l2_encoding_compute_mask_bytes(total_mask_bytes);
		metrics().observe_storage_l2_encoding_compute_pages_fetched("selected", pages_fetched);
		Ok(Some(EncodingComputePhaseBRead {
			outcome,
			phase_b_fetch,
		}))
	}

	async fn read_projected_record_from_l2(
		&self,
		l2: Arc<dyn ObjectStore>,
		pointer: &TieredSlabPointer,
		location: RecordLocation,
		descriptor: &TableSchemaDescriptorV1,
		requirement: &ColumnFetchRequirement,
		predicate: Option<&crate::schema::pbc::Program>,
		selected_record_rows: Option<&[RecordRowIndex]>,
	) -> std::io::Result<ProjectedL2RecordRead> {
		if !pointer.is_encrypted {
			let plaintext = self
				.fetch_full_tiered_plaintext(l2.as_ref(), pointer)
				.await?;
			let frame = extract_record_from_blob(&plaintext, location)?;
			let projected = decode_full_projected_l2_record(
				pointer.slab_id,
				location,
				descriptor,
				requirement,
				&frame,
			)?;
			return Ok(ProjectedL2RecordRead {
				projected,
				projected_first_record_row_index: RecordRowIndex::ZERO,
				indexed_rows_materialized: false,
				encoding_materialize_audit: Vec::new(),
				strategy: L2FetchStrategy::WholeRecord,
			});
		}

		let planning_started = Instant::now();
		let body_location = record_body_location(location)?;
		let whole_record_range = record_plaintext_range_for_l2(location)?;
		let metadata = l2.head(&pointer.l2_uri).await?;
		let tail_start = metadata
			.size_bytes
			.saturating_sub(self.config.rehydration.tail_read_window_bytes);
		let tail_window = l2
			.get_range(&pointer.l2_uri, tail_start..metadata.size_bytes)
			.await?;
		let additional_metadata_range = self.config.crypto.additional_tiered_metadata_range(
			pointer.slab_id,
			metadata.size_bytes,
			tail_window.as_ref(),
		)?;
		let additional_metadata = match additional_metadata_range {
			Some(range) => Some(l2.get_range(&pointer.l2_uri, range).await?),
			None => None,
		};

		let pre_probe = pick_l2_fetch_strategy(
			&self.config.l2_column_fetch,
			descriptor,
			&body_location,
			requirement,
			None,
			None,
			false,
		);
		if !matches!(
			pre_probe,
			L2FetchStrategy::Fallback(ColumnarFallbackReason::MetadataProbeCap)
		) && !self.config.l2_row_group_fetch.enabled
		{
			observe_l2_fetch_strategy(metrics(), &pre_probe, planning_started.elapsed());
			let projected = self
				.read_full_projected_record_from_l2(
					l2.as_ref(),
					pointer,
					location,
					descriptor,
					requirement,
				)
				.await?;
			return Ok(ProjectedL2RecordRead {
				projected,
				projected_first_record_row_index: RecordRowIndex::ZERO,
				indexed_rows_materialized: false,
				encoding_materialize_audit: Vec::new(),
				strategy: pre_probe,
			});
		}

		let mut probe = initial_metadata_probe_plan(&self.config.l2_column_fetch, &body_location)
			.map_err(columnar_io_error)?;
		let header_size = slab_record_header_size_u64()?;
		let directory = loop {
			let prefix_len = header_size
				.checked_add(probe.window_bytes.get())
				.ok_or_else(|| Error::new(ErrorKind::InvalidData, "metadata prefix overflow"))?;
			let prefix_range = TieredPlaintextRange::new(
				RecordBodyByteOffset::new(0),
				RecordBodyByteLen::try_new(prefix_len).map_err(|err| {
					Error::new(
						ErrorKind::InvalidData,
						format!("metadata prefix length rejected: {err}"),
					)
				})?,
			);
			let prefix_plan = self.config.crypto.plan_tiered_multi_range_read(
				pointer.slab_id,
				metadata.size_bytes,
				tail_window.as_ref(),
				additional_metadata.as_deref(),
				whole_record_range.clone(),
				&[prefix_range],
			)?;
			let prefix_ciphertext = fetch_tiered_ciphertext_runs(
				l2.clone(),
				pointer,
				&prefix_plan,
				self.config.l2_column_fetch.max_parallel_range_gets.get() as usize,
			)
			.await?;
			let prefix_plaintext = prefix_plan.decrypt(&prefix_ciphertext)?;
			let Some(prefix_plaintext) = prefix_plaintext.first() else {
				return Err(Error::new(
					ErrorKind::InvalidData,
					"metadata prefix decrypt returned no plaintext",
				));
			};
			let header_size = usize::try_from(header_size).map_err(|_| {
				Error::new(
					ErrorKind::InvalidData,
					"slab record header size exceeds usize",
				)
			})?;
			if prefix_plaintext.len() < header_size {
				return Err(Error::new(
					ErrorKind::UnexpectedEof,
					"metadata prefix shorter than slab record header",
				));
			}
			validate_tiered_record_header_tenant(
				&prefix_plaintext[..header_size],
				prefix_plan.footer().tenant_id,
				pointer.slab_id,
			)?;
			let metadata_window_bytes = &prefix_plaintext[header_size..];
			match parse_record_buffer_directory(
				TieredRecordId {
					slab_id: pointer.slab_id,
					record_offset: RecordBodyByteOffset::new(location.offset),
				},
				PlaintextByteRange {
					start: 0,
					len: u64::from(body_location.length),
				},
				metadata_window_bytes,
				descriptor,
			) {
				Ok(directory) => break directory,
				Err(crate::storage::columnar::ColumnFetchError::MetadataTruncated {
					needed,
					..
				}) => {
					let next = next_metadata_probe_plan(
						&self.config.l2_column_fetch,
						&body_location,
						probe.window_bytes,
						needed,
					)
					.map_err(columnar_io_error)?;
					match next {
						Some(next) => probe = next,
						None => {
							let strategy =
								L2FetchStrategy::Fallback(ColumnarFallbackReason::MetadataProbeCap);
							observe_l2_fetch_strategy(
								metrics(),
								&strategy,
								planning_started.elapsed(),
							);
							let projected = self
								.read_full_projected_record_from_l2(
									l2.as_ref(),
									pointer,
									location,
									descriptor,
									requirement,
								)
								.await?;
							return Ok(ProjectedL2RecordRead {
								projected,
								projected_first_record_row_index: RecordRowIndex::ZERO,
								indexed_rows_materialized: false,
								encoding_materialize_audit: Vec::new(),
								strategy,
							});
						}
					}
				}
				Err(crate::storage::columnar::ColumnFetchError::UnsupportedArrowLayout) => {
					let strategy =
						L2FetchStrategy::Fallback(ColumnarFallbackReason::UnsupportedArrowLayout);
					observe_l2_fetch_strategy(metrics(), &strategy, planning_started.elapsed());
					let projected = self
						.read_full_projected_record_from_l2(
							l2.as_ref(),
							pointer,
							location,
							descriptor,
							requirement,
						)
						.await?;
					return Ok(ProjectedL2RecordRead {
						projected,
						projected_first_record_row_index: RecordRowIndex::ZERO,
						indexed_rows_materialized: false,
						encoding_materialize_audit: Vec::new(),
						strategy,
					});
				}
				Err(err) => return Err(columnar_io_error(err)),
			}
		};

		if let Some(page_directory) = directory.page_directory.clone() {
			if directory.encoding_directory.is_some() && !self.config.l2_encoding_fetch.enabled {
				metrics().observe_storage_l2_encoding_fallback("reader_disabled");
				let strategy = L2FetchStrategy::Fallback(ColumnarFallbackReason::Disabled);
				observe_l2_row_group_fetch_strategy(
					metrics(),
					&strategy,
					planning_started.elapsed(),
				);
				let projected = self
					.read_full_projected_record_from_l2(
						l2.as_ref(),
						pointer,
						location,
						descriptor,
						requirement,
					)
					.await?;
				return Ok(ProjectedL2RecordRead {
					projected,
					projected_first_record_row_index: RecordRowIndex::ZERO,
					indexed_rows_materialized: false,
					encoding_materialize_audit: Vec::new(),
					strategy,
				});
			}
			if !self.config.l2_row_group_fetch.enabled {
				let strategy = L2FetchStrategy::Fallback(ColumnarFallbackReason::Disabled);
				observe_l2_row_group_fetch_strategy(
					metrics(),
					&strategy,
					planning_started.elapsed(),
				);
				let projected = self
					.read_full_projected_record_from_l2(
						l2.as_ref(),
						pointer,
						location,
						descriptor,
						requirement,
					)
					.await?;
				return Ok(ProjectedL2RecordRead {
					projected,
					projected_first_record_row_index: RecordRowIndex::ZERO,
					indexed_rows_materialized: false,
					encoding_materialize_audit: Vec::new(),
					strategy,
				});
			}
			let Some(group) = crate::storage::row_group::row_group_for_record_row(
				&page_directory,
				location.record_row_index,
			) else {
				return Err(Error::new(
					ErrorKind::InvalidData,
					format!(
						"record row index {} is not covered by row-group directory",
						location.record_row_index.get()
					),
				));
			};
			let selected_groups = match selected_record_rows {
				Some(rows) if !rows.is_empty() => {
					crate::storage::row_group::row_groups_for_record_rows(&page_directory, rows)?
				}
				_ => vec![group.row_group_index],
			};
			if predicate.is_none() {
				if let Some(rows) = selected_record_rows {
					if let Some(read) = self
						.try_read_encoding_indexed_pages_from_l2(
							l2.clone(),
							pointer,
							metadata.size_bytes,
							tail_window.as_ref(),
							additional_metadata.as_deref(),
							whole_record_range.clone(),
							&body_location,
							header_size,
							&directory,
							&page_directory,
							descriptor,
							requirement,
							rows,
							selected_groups.as_slice(),
							planning_started,
						)
						.await?
					{
						return Ok(read);
					}
				}
			}
			let point_read = selected_record_rows.is_none();
			let mut predicate_fetched_groups = Vec::new();
			let mut phase_b_fetch = None;
			let mut phase_b_requirement_for_zip = ColumnFetchRequirement::default();
			let mut phase_b_projected_by_group = HashMap::new();
			let mut phase_b_outcome = crate::storage::row_group::PhaseBOutcome::default();
			let mut selected_groups = selected_groups;
			if let Some(predicate) = predicate {
				let predicate_kind = crate::storage::row_group::classify_pbc_for_row_group_pruning(
					predicate,
					&directory.ipc_schema,
				);
				let classifications = crate::storage::row_group::classify_row_groups(
					&page_directory,
					&predicate_kind,
					&self.config.l2_row_group_fetch,
				)
				.into_iter()
				.collect::<HashMap<_, _>>();
				selected_groups.retain(|group_index| {
					classifications
						.get(group_index)
						.copied()
						.unwrap_or(crate::storage::row_group::RowGroupClassification::Keep)
						!= crate::storage::row_group::RowGroupClassification::Skip
				});
				predicate_fetched_groups = selected_groups
					.iter()
					.copied()
					.filter(|group_index| {
						classifications
							.get(group_index)
							.copied()
							.unwrap_or(crate::storage::row_group::RowGroupClassification::Keep)
							== crate::storage::row_group::RowGroupClassification::NeedsPredicateFetch
					})
					.collect();
				if selected_groups.is_empty() {
					let row_group_plan = crate::storage::row_group::RowGroupFetchPlan {
						record_id: page_directory.record_id,
						directory: page_directory.clone(),
						surviving_groups: Vec::new(),
						predicate_fetched_groups: Vec::new(),
						phase_b_fetch: None,
						phase_c_fetch: crate::storage::row_group::RowGroupRangeFetchPlan::default(),
						logical_schema_fingerprint: page_directory.logical_schema_fingerprint,
					};
					let strategy = L2FetchStrategy::RowGroup(row_group_plan);
					observe_l2_row_group_fetch_strategy(
						metrics(),
						&strategy,
						planning_started.elapsed(),
					);
					let projected = crate::storage::row_group::materialize_row_groups_from_batches(
						&[],
						&page_directory,
						requirement,
						&[],
					)?;
					return Ok(ProjectedL2RecordRead {
						projected,
						projected_first_record_row_index: group.first_record_row_index,
						indexed_rows_materialized: false,
						encoding_materialize_audit: Vec::new(),
						strategy,
					});
				}
				if !predicate_fetched_groups.is_empty() {
					if let Some(encoded_phase_b) = self
						.try_read_encoding_compute_phase_b_from_l2(
							l2.clone(),
							pointer,
							metadata.size_bytes,
							tail_window.as_ref(),
							additional_metadata.as_deref(),
							whole_record_range.clone(),
							header_size,
							&directory,
							&page_directory,
							predicate,
							&selected_groups,
							&predicate_fetched_groups,
						)
						.await?
					{
						phase_b_fetch = Some(encoded_phase_b.phase_b_fetch);
						phase_b_outcome = encoded_phase_b.outcome;
						let predicate_fetched_set = predicate_fetched_groups
							.iter()
							.copied()
							.collect::<BTreeSet<_>>();
						if point_read
							&& predicate_fetched_set.contains(&group.row_group_index)
							&& !phase_b_point_read_survives(
								&phase_b_outcome,
								group,
								location.record_row_index,
							)? {
							let row_group_plan = crate::storage::row_group::RowGroupFetchPlan {
								record_id: page_directory.record_id,
								directory: page_directory.clone(),
								surviving_groups: Vec::new(),
								predicate_fetched_groups,
								phase_b_fetch,
								phase_c_fetch:
									crate::storage::row_group::RowGroupRangeFetchPlan::default(),
								logical_schema_fingerprint: page_directory
									.logical_schema_fingerprint,
							};
							let strategy = L2FetchStrategy::RowGroup(row_group_plan);
							observe_l2_row_group_fetch_strategy(
								metrics(),
								&strategy,
								planning_started.elapsed(),
							);
							let projected =
								crate::storage::row_group::materialize_row_groups_from_batches(
									&[],
									&page_directory,
									requirement,
									&[],
								)?;
							return Ok(ProjectedL2RecordRead {
								projected,
								projected_first_record_row_index: group.first_record_row_index,
								indexed_rows_materialized: false,
								encoding_materialize_audit: Vec::new(),
								strategy,
							});
						}
					}
					if phase_b_fetch.is_none() {
						let phase_b_requirement = ColumnFetchRequirement {
							predicate_columns: requirement.predicate_columns.clone(),
							storage_columns: requirement.storage_columns.clone(),
							..ColumnFetchRequirement::default()
						};
						if !phase_b_requirement.user_visible().is_empty()
							|| !phase_b_requirement.storage_columns.is_empty()
						{
							let phase_b_body_byte_ranges =
								crate::storage::row_group::selected_row_group_record_body_ranges(
									&directory,
									&page_directory,
									&phase_b_requirement,
									&selected_groups,
								)
								.map_err(columnar_io_error)?;
							let phase_b_body_ranges = phase_b_body_byte_ranges
								.iter()
								.map(|range| TieredPlaintextRange::new(range.start, range.len))
								.collect::<Vec<_>>();
							let phase_b_record_ranges = record_body_ranges_to_record_ranges(
								header_size,
								&phase_b_body_ranges,
							)?;
							let phase_b_tiered_plan =
								self.config.crypto.plan_tiered_multi_range_read(
									pointer.slab_id,
									metadata.size_bytes,
									tail_window.as_ref(),
									additional_metadata.as_deref(),
									whole_record_range.clone(),
									&phase_b_record_ranges,
								)?;
							phase_b_fetch = Some(
								crate::storage::row_group::row_group_range_fetch_plan_from_tiered(
									&phase_b_tiered_plan,
									&phase_b_body_byte_ranges,
								),
							);
							let phase_b_ciphertext = fetch_tiered_ciphertext_runs(
								l2.clone(),
								pointer,
								&phase_b_tiered_plan,
								self.config.l2_row_group_fetch.max_parallel_range_gets.get()
									as usize,
							)
							.await?;
							let phase_b_plaintext =
								phase_b_tiered_plan.decrypt(&phase_b_ciphertext)?;
							let user_column_count = descriptor.arrow_schema()?.fields().len();
							let predicate_fetched_set = predicate_fetched_groups
								.iter()
								.copied()
								.collect::<BTreeSet<_>>();
							for group_index in &selected_groups {
								let phase_b_projected =
							crate::storage::row_group::decode_projected_row_groups_from_plaintext_ranges(
								&directory,
								&page_directory,
								&phase_b_requirement,
								&phase_b_body_ranges,
								&phase_b_plaintext,
									&[*group_index],
							)
							.map_err(arrow_io_error)?;
								if predicate_fetched_set.contains(group_index) {
									let survival_mask = survival_mask_projected_record_batch(
										&phase_b_projected,
										predicate,
										user_column_count,
									)?;
									phase_b_outcome
										.decoded_predicate_batches
										.insert(*group_index, phase_b_projected.batch.clone());
									phase_b_outcome
										.survival_masks
										.insert(*group_index, survival_mask);
								}
								phase_b_projected_by_group.insert(*group_index, phase_b_projected);
							}
							if point_read && predicate_fetched_set.contains(&group.row_group_index)
							{
								let survival_mask = phase_b_outcome
									.survival_masks
									.get(&group.row_group_index)
									.ok_or_else(|| {
										Error::new(
											ErrorKind::InvalidData,
											format!(
												"Phase B survival mask missing for target row group {}",
												group.row_group_index.get()
											),
										)
									})?;
								let row_offset = location
									.record_row_index
									.get()
									.checked_sub(group.first_record_row_index.get())
									.ok_or_else(|| {
										Error::new(
											ErrorKind::InvalidData,
											format!(
												"record row index {} precedes row group {} base {}",
												location.record_row_index.get(),
												group.row_group_index.get(),
												group.first_record_row_index.get()
											),
										)
									})? as usize;
								if row_offset >= survival_mask.len() {
									return Err(Error::new(
										ErrorKind::InvalidData,
										format!(
											"record row index {} maps outside Phase B mask length {} for row group {}",
											location.record_row_index.get(),
											survival_mask.len(),
											group.row_group_index.get()
										),
									));
								}
								if !survival_mask.is_valid(row_offset)
									|| !survival_mask.value(row_offset)
								{
									let row_group_plan = crate::storage::row_group::RowGroupFetchPlan {
									record_id: page_directory.record_id,
									directory: page_directory.clone(),
									surviving_groups: Vec::new(),
									predicate_fetched_groups,
									phase_b_fetch,
									phase_c_fetch:
										crate::storage::row_group::RowGroupRangeFetchPlan::default(),
									logical_schema_fingerprint: page_directory
										.logical_schema_fingerprint,
								};
									let strategy = L2FetchStrategy::RowGroup(row_group_plan);
									observe_l2_row_group_fetch_strategy(
										metrics(),
										&strategy,
										planning_started.elapsed(),
									);
									let projected =
									crate::storage::row_group::materialize_row_groups_from_batches(
										&[],
										&page_directory,
										requirement,
										&[],
									)?;
									return Ok(ProjectedL2RecordRead {
										projected,
										projected_first_record_row_index: group
											.first_record_row_index,
										indexed_rows_materialized: false,
										encoding_materialize_audit: Vec::new(),
										strategy,
									});
								}
							}
							phase_b_requirement_for_zip = phase_b_requirement;
						}
					}
				}
			}
			let phase_c_requirement = if phase_b_fetch.is_some() {
				requirement.excluding(&phase_b_requirement_for_zip)
			} else {
				requirement.clone()
			};
			let phase_c_requirement_empty = phase_c_requirement.user_visible().is_empty()
				&& phase_c_requirement.storage_columns.is_empty();
			let dense_phase_c_body_byte_ranges = if phase_c_requirement_empty {
				Vec::new()
			} else {
				crate::storage::row_group::selected_row_group_record_body_ranges(
					&directory,
					&page_directory,
					&phase_c_requirement,
					&selected_groups,
				)
				.map_err(columnar_io_error)?
			};
			let mut sparse_projection_plan = None;
			let mut projection_groups = selected_groups.clone();
			let selected_body_byte_ranges = if !point_read
				&& !phase_c_requirement_empty
				&& phase_b_projected_by_group.is_empty()
				&& !phase_b_outcome.survival_masks.is_empty()
			{
				if let Some(encoding_directory) = directory.encoding_directory.as_ref() {
					let dense_predicate_batches_present = phase_b_outcome
						.decoded_predicate_batches
						.values()
						.any(|batch| batch.num_columns() > 0);
					match decide_sparse_projection_fetch(SparseProjectionFetchInput {
						record_directory: &directory,
						page_directory: &page_directory,
						encoding_directory,
						requirement: &phase_c_requirement,
						predicate_outcome: &phase_b_outcome,
						selected_groups: &selected_groups,
						dense_body_ranges: &dense_phase_c_body_byte_ranges,
						row_group_config: &self.config.l2_row_group_fetch,
						encoding_config: &self.config.l2_encoding_fetch,
						dense_predicate_batches_present,
					}) {
						SparseProjectionFetchDecision::Sparse(plan) => {
							projection_groups = plan.selected_groups.clone();
							let ranges = plan.selected_body_ranges.clone();
							sparse_projection_plan = Some(plan);
							ranges
						}
						SparseProjectionFetchDecision::Decline(reason) => {
							let _ = reason;
							dense_phase_c_body_byte_ranges.clone()
						}
					}
				} else {
					dense_phase_c_body_byte_ranges.clone()
				}
			} else {
				dense_phase_c_body_byte_ranges.clone()
			};
			let selected_body_ranges = selected_body_byte_ranges
				.iter()
				.map(|range| TieredPlaintextRange::new(range.start, range.len))
				.collect::<Vec<_>>();
			let selected_record_ranges =
				record_body_ranges_to_record_ranges(header_size, &selected_body_ranges)?;
			let tiered_plan = self.config.crypto.plan_tiered_multi_range_read(
				pointer.slab_id,
				metadata.size_bytes,
				tail_window.as_ref(),
				additional_metadata.as_deref(),
				whole_record_range.clone(),
				&selected_record_ranges,
			)?;
			let phase_c_fetch = crate::storage::row_group::row_group_range_fetch_plan_from_tiered(
				&tiered_plan,
				&selected_body_byte_ranges,
			);
			let row_group_plan = crate::storage::row_group::RowGroupFetchPlan {
				record_id: page_directory.record_id,
				directory: page_directory.clone(),
				surviving_groups: projection_groups.to_vec(),
				predicate_fetched_groups,
				phase_b_fetch,
				phase_c_fetch,
				logical_schema_fingerprint: page_directory.logical_schema_fingerprint,
			};
			let strategy = pick_point_row_group_fetch_strategy(
				&self.config.l2_row_group_fetch,
				&body_location,
				&page_directory,
				row_group_plan,
			);
			observe_l2_row_group_fetch_strategy(metrics(), &strategy, planning_started.elapsed());
			let L2FetchStrategy::RowGroup(row_group_plan) = strategy else {
				let projected = self
					.read_full_projected_record_from_l2(
						l2.as_ref(),
						pointer,
						location,
						descriptor,
						requirement,
					)
					.await?;
				return Ok(ProjectedL2RecordRead {
					projected,
					projected_first_record_row_index: RecordRowIndex::ZERO,
					indexed_rows_materialized: false,
					encoding_materialize_audit: Vec::new(),
					strategy,
				});
			};
			if let Some(sparse_projection_plan) = sparse_projection_plan.as_ref() {
				for fetch in &sparse_projection_plan.fetches {
					metrics().observe_storage_l2_encoding_simd_pages_fetched(
						fetch.cascade.kind().as_str(),
						1,
					);
				}
				let plaintext_extracts = if selected_record_ranges.is_empty() {
					Vec::new()
				} else {
					let ciphertext_runs = fetch_tiered_ciphertext_runs(
						l2,
						pointer,
						&tiered_plan,
						self.config.l2_row_group_fetch.max_parallel_range_gets.get() as usize,
					)
					.await?;
					tiered_plan.decrypt(&ciphertext_runs)?
				};
				let projected = materialize_sparse_projection_pages(
					&directory,
					&phase_c_requirement,
					sparse_projection_plan,
					&selected_body_ranges,
					&plaintext_extracts,
					&phase_b_outcome,
				)
				.map_err(arrow_io_error)?;
				return Ok(ProjectedL2RecordRead {
					projected,
					projected_first_record_row_index: group.first_record_row_index,
					indexed_rows_materialized: false,
					encoding_materialize_audit: Vec::new(),
					strategy: L2FetchStrategy::RowGroup(row_group_plan),
				});
			}
			let projected = if phase_b_projected_by_group.is_empty() {
				if phase_b_outcome.survival_masks.is_empty() || point_read {
					if phase_c_requirement_empty {
						crate::storage::row_group::empty_projected_row_group(
							group.row_count.get() as usize
						)?
					} else {
						let ciphertext_runs = fetch_tiered_ciphertext_runs(
							l2,
							pointer,
							&tiered_plan,
							self.config.l2_row_group_fetch.max_parallel_range_gets.get() as usize,
						)
						.await?;
						let plaintext_extracts = tiered_plan.decrypt(&ciphertext_runs)?;
						crate::storage::row_group::decode_projected_row_groups_from_plaintext_ranges(
							&directory,
							&page_directory,
							&phase_c_requirement,
							&selected_body_ranges,
							&plaintext_extracts,
							&selected_groups,
						)
						.map_err(arrow_io_error)?
					}
				} else {
					let plaintext_extracts = if phase_c_requirement_empty {
						Vec::new()
					} else {
						let ciphertext_runs = fetch_tiered_ciphertext_runs(
							l2,
							pointer,
							&tiered_plan,
							self.config.l2_row_group_fetch.max_parallel_range_gets.get() as usize,
						)
						.await?;
						tiered_plan.decrypt(&ciphertext_runs)?
					};
					let mut projected_groups = Vec::new();
					for group_index in &selected_groups {
						let group_entry = page_directory
							.row_groups
							.iter()
							.find(|candidate| candidate.row_group_index == *group_index)
							.ok_or_else(|| {
								Error::new(
									ErrorKind::InvalidData,
									format!(
										"selected row group {} missing from page directory",
										group_index.get()
									),
								)
							})?;
						let projected = if phase_c_requirement_empty {
							crate::storage::row_group::empty_projected_row_group(
								group_entry.row_count.get() as usize,
							)?
						} else {
							crate::storage::row_group::decode_projected_row_groups_from_plaintext_ranges(
								&directory,
								&page_directory,
								&phase_c_requirement,
								&selected_body_ranges,
								&plaintext_extracts,
								&[*group_index],
							)
							.map_err(arrow_io_error)?
						};
						projected_groups.push((*group_index, projected));
					}
					crate::storage::row_group::apply_phase_b_masks_to_projected_row_groups(
						projected_groups,
						&phase_b_outcome,
						row_group_plan.predicate_fetched_groups.as_slice(),
						&page_directory,
					)?
				}
			} else {
				let plaintext_extracts = if phase_c_requirement_empty {
					Vec::new()
				} else {
					let ciphertext_runs = fetch_tiered_ciphertext_runs(
						l2,
						pointer,
						&tiered_plan,
						self.config.l2_row_group_fetch.max_parallel_range_gets.get() as usize,
					)
					.await?;
					tiered_plan.decrypt(&ciphertext_runs)?
				};
				let mut projected_groups = Vec::new();
				for group_index in &selected_groups {
					let group_entry = page_directory
						.row_groups
						.iter()
						.find(|candidate| candidate.row_group_index == *group_index)
						.ok_or_else(|| {
							Error::new(
								ErrorKind::InvalidData,
								format!(
									"selected row group {} missing from page directory",
									group_index.get()
								),
							)
						})?;
					let phase_c_projected = if phase_c_requirement_empty {
						crate::storage::row_group::empty_projected_row_group(
							group_entry.row_count.get() as usize,
						)?
					} else {
						crate::storage::row_group::decode_projected_row_groups_from_plaintext_ranges(
							&directory,
							&page_directory,
							&phase_c_requirement,
							&selected_body_ranges,
							&plaintext_extracts,
							&[*group_index],
						)
						.map_err(arrow_io_error)?
					};
					let Some(phase_b_projected) = phase_b_projected_by_group.get(group_index)
					else {
						return Err(Error::new(
							ErrorKind::InvalidData,
							format!(
								"Phase B projected batch missing for selected row group {}",
								group_index.get()
							),
						));
					};
					let zipped =
						crate::storage::row_group::zip_phase_b_and_phase_c_projected_row_group(
							phase_b_projected,
							&phase_c_projected,
							requirement,
						)?;
					projected_groups.push((*group_index, zipped));
				}
				let mask_groups = if point_read {
					&[][..]
				} else {
					row_group_plan.predicate_fetched_groups.as_slice()
				};
				crate::storage::row_group::apply_phase_b_masks_to_projected_row_groups(
					projected_groups,
					&phase_b_outcome,
					mask_groups,
					&page_directory,
				)?
			};
			return Ok(ProjectedL2RecordRead {
				projected,
				projected_first_record_row_index: group.first_record_row_index,
				indexed_rows_materialized: false,
				encoding_materialize_audit: Vec::new(),
				strategy: L2FetchStrategy::RowGroup(row_group_plan),
			});
		}

		let selected_body_ranges =
			selected_record_body_ranges(&directory, requirement).map_err(columnar_io_error)?;
		let selected_record_ranges =
			record_body_ranges_to_record_ranges(header_size, &selected_body_ranges)?;
		let tiered_plan = self.config.crypto.plan_tiered_multi_range_read(
			pointer.slab_id,
			metadata.size_bytes,
			tail_window.as_ref(),
			additional_metadata.as_deref(),
			whole_record_range,
			&selected_record_ranges,
		)?;
		let candidate_plan = match build_column_fetch_plan(&directory, requirement, &tiered_plan) {
			Ok(plan) => Some(plan),
			Err(crate::storage::columnar::ColumnFetchError::UnsupportedArrowLayout) => None,
			Err(err) => return Err(columnar_io_error(err)),
		};
		let strategy = pick_l2_fetch_strategy(
			&self.config.l2_column_fetch,
			descriptor,
			&body_location,
			requirement,
			Some(&directory),
			candidate_plan,
			false,
		);
		observe_l2_fetch_strategy(metrics(), &strategy, planning_started.elapsed());
		if !matches!(strategy, L2FetchStrategy::Columnar(_)) {
			let projected = self
				.read_full_projected_record_from_l2(
					l2.as_ref(),
					pointer,
					location,
					descriptor,
					requirement,
				)
				.await?;
			return Ok(ProjectedL2RecordRead {
				projected,
				projected_first_record_row_index: RecordRowIndex::ZERO,
				indexed_rows_materialized: false,
				encoding_materialize_audit: Vec::new(),
				strategy,
			});
		}

		let ciphertext_runs = fetch_tiered_ciphertext_runs(
			l2,
			pointer,
			&tiered_plan,
			self.config.l2_column_fetch.max_parallel_range_gets.get() as usize,
		)
		.await?;
		let plaintext_extracts = tiered_plan.decrypt(&ciphertext_runs)?;
		let projected = decode_projected_record_batch_from_plaintext_ranges(
			&directory,
			requirement,
			&selected_body_ranges,
			&plaintext_extracts,
		)
		.map_err(arrow_io_error)?;
		Ok(ProjectedL2RecordRead {
			projected,
			projected_first_record_row_index: RecordRowIndex::ZERO,
			indexed_rows_materialized: false,
			encoding_materialize_audit: Vec::new(),
			strategy,
		})
	}

	async fn read_full_projected_record_from_l2(
		&self,
		l2: &dyn ObjectStore,
		pointer: &TieredSlabPointer,
		location: RecordLocation,
		descriptor: &TableSchemaDescriptorV1,
		requirement: &ColumnFetchRequirement,
	) -> std::io::Result<ProjectedRecordBatch> {
		let frame = if pointer.is_encrypted {
			self.read_encrypted_record_from_l2(l2, pointer, location)
				.await?
		} else {
			let plaintext = self.fetch_full_tiered_plaintext(l2, pointer).await?;
			extract_record_from_blob(&plaintext, location)?
		};
		decode_full_projected_l2_record(pointer.slab_id, location, descriptor, requirement, &frame)
	}

	async fn note_l2_read(&self, slab_id: SlabId) -> bool {
		let promote_after = self.config.rehydration.promote_after_l2_reads;
		if promote_after == 0 {
			return false;
		}
		let mut state = self.state.lock().await;
		if state.rehydrated.contains_key(&slab_id) || state.rehydrating.contains(&slab_id) {
			return false;
		}
		let hits = state.hot_read_counts.entry(slab_id).or_insert(0);
		*hits = hits.saturating_add(1);
		if *hits < promote_after {
			return false;
		}
		state.rehydrating.insert(slab_id);
		true
	}

	async fn rehydrate_tiered_slab(
		&self,
		pointer: TieredSlabPointer,
		blob: Bytes,
	) -> std::io::Result<()> {
		let result = self
			.install_rehydrated_local_copy(pointer.slab_id, blob.as_ref())
			.await;
		let mut state = self.state.lock().await;
		state.rehydrating.remove(&pointer.slab_id);
		match result {
			Ok(installed) => {
				state.hot_read_counts.remove(&pointer.slab_id);
				state.rehydrated.insert(pointer.slab_id, installed);
				Ok(())
			}
			Err(err) => {
				let retry_floor = self
					.config
					.rehydration
					.promote_after_l2_reads
					.saturating_sub(1);
				if retry_floor == 0 {
					state.hot_read_counts.remove(&pointer.slab_id);
				} else {
					state.hot_read_counts.insert(pointer.slab_id, retry_floor);
				}
				Err(err)
			}
		}
	}

	async fn install_rehydrated_local_copy(
		&self,
		slab_id: SlabId,
		blob: &[u8],
	) -> std::io::Result<RehydratedSlabState> {
		let path = slab_path(&self.config.dir, slab_id);
		let tmp_path = rehydrate_tmp_path(&self.config.dir, slab_id);
		let _ = unlink_local_file(self.file_ops.as_ref(), &tmp_path).await;
		self.file_ops.write_all(&tmp_path, blob).await?;
		let tmp = self
			.file_ops
			.open(
				&tmp_path,
				OpenFlags {
					read: true,
					write: true,
					create: false,
					create_new: false,
					truncate: false,
					direct: false,
				},
			)
			.await?;
		self.file_ops.fsync(&tmp).await?;
		self.file_ops.rename(&tmp_path, &path).await?;
		fsync_parent_dir(self.file_ops.as_ref(), &path).await?;
		let slab = Arc::new(
			LocalSlabStore::open(
				&path,
				slab_id,
				self.file_ops.clone(),
				self.config.group_commit.clone(),
			)
			.await?,
		);
		Ok(RehydratedSlabState {
			slab,
			keep_local_until: Instant::now() + self.config.rehydration.min_local_residency,
		})
	}
}

#[derive(Clone, Copy)]
struct CompactionRecord {
	tenant_id: TenantId,
	key_identity: Identity,
	routing_locality: Locality,
	old_location: RecordLocation,
	reclaim: bool,
	retention_ambiguous: bool,
	retention_lag_seconds: u64,
}

#[derive(Clone)]
struct ReclusterLogicalRow {
	tenant_id: TenantId,
	key_identity: Identity,
	routing_locality: Locality,
	old_location: RecordLocation,
	row_hlc: HybridTimestamp,
	batch: RecordBatch,
}

struct ReclusterGroupPlan {
	frame: Vec<u8>,
	records: Vec<CompactionRecord>,
	matching_rows: Vec<ReclusterLogicalRow>,
	nonmatching_records: Vec<CompactionRecord>,
}

#[derive(Clone, Copy)]
struct SecondaryIndexCompactionContext<'a> {
	table_schemas: &'a TableSchemaCatalogV1,
	secondary_indexes: &'a SecondaryIndexCatalog,
}

struct LsmPreparedSecondaryIndexSegments {
	old_sidecars: Vec<SecondaryIndexSegmentInfo>,
	old_descriptors: Vec<(SlabId, SecondaryIndexCoverageKey)>,
	output_segments: Vec<LsmPreparedSecondaryIndexSegment>,
}

struct LsmPreparedSecondaryIndexSegment {
	key: SecondaryIndexCoverageKey,
	output_slab_id: SlabId,
	index_object_id: IndexObjectId,
	segment: SecondaryIndexSegment,
}

struct CompactedFrame {
	tenant_id: TenantId,
	key_identity: Identity,
	hlc: HybridTimestamp,
	frame: Vec<u8>,
	was_multirow: bool,
	rows: Vec<CompactedFrameRow>,
}

#[derive(Clone, Copy)]
struct CompactedFrameRow {
	tenant_id: TenantId,
	key_identity: Identity,
	routing_locality: Locality,
	old_location: RecordLocation,
	row_hlc: HybridTimestamp,
	record_row_index: RecordRowIndex,
}

#[derive(Clone, Copy)]
struct RelocatedRecord {
	tenant_id: TenantId,
	key_identity: Identity,
	routing_locality: Locality,
	old_location: RecordLocation,
	new_location: RecordLocation,
}

#[derive(Clone, Copy)]
struct CompactionOutcome {
	reclaimed_bytes: u64,
	reclaimed_records: usize,
	retention_stats: RetentionCompactionStats,
}

#[derive(Clone, Copy)]
struct RetentionSlabScanOutcome {
	compaction: Option<CompactionOutcome>,
	retention_stats: RetentionCompactionStats,
}

#[derive(Clone, Copy)]
struct LayoutReclusterOutcome {
	reclaimed_bytes: u64,
	rewritten_records: usize,
	relocated_rows: usize,
}

// ============================================================
//  HELPERS
// ============================================================

fn validate_tiered_blob(pointer: &TieredSlabPointer, blob: &[u8]) -> std::io::Result<()> {
	let actual = crate::hash::content::digest(blob);
	if actual != pointer.payload_hash {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"L2 payload hash mismatch for slab {}: expected {:02x?}, got {:02x?}",
				pointer.slab_id,
				&pointer.payload_hash[..4],
				&actual[..4],
			),
		));
	}
	Ok(())
}

fn decode_tiered_blob(
	crypto: &CryptoManager,
	pointer: &TieredSlabPointer,
	blob: &[u8],
) -> std::io::Result<Vec<u8>> {
	match crypto.encrypted_footer_summary(blob)? {
		Some(FooterSummary {
			slab_id, tenant_id, ..
		}) => {
			if slab_id != pointer.slab_id {
				return Err(Error::new(
					ErrorKind::InvalidData,
					format!(
						"encrypted tiered slab footer references slab {} but metadata expects {}",
						slab_id, pointer.slab_id
					),
				));
			}
			let DecryptedTieredSlab { plaintext, .. } =
				crypto.decrypt_tiered_slab(pointer.slab_id, blob)?;
			let payload_tenant_id = single_tenant_id_from_plain_slab(&plaintext, pointer.slab_id)?;
			if payload_tenant_id != tenant_id {
				return Err(Error::new(
					ErrorKind::InvalidData,
					format!(
						"encrypted tiered slab footer tenant {} does not match payload tenant {} for slab {}",
						tenant_id.get(),
						payload_tenant_id.get(),
						pointer.slab_id,
					),
				));
			}
			Ok(plaintext)
		}
		None => Ok(blob.to_vec()),
	}
}

fn validate_tiered_record_header_tenant(
	blob: &[u8],
	expected_tenant_id: TenantId,
	slab_id: SlabId,
) -> std::io::Result<()> {
	let header_size = std::mem::size_of::<SlabRecordHeader>();
	if blob.len() < header_size {
		return Err(Error::new(
			ErrorKind::UnexpectedEof,
			"L2 record range shorter than record header",
		));
	}
	let header = bytemuck::pod_read_unaligned::<SlabRecordHeader>(&blob[..header_size]);
	if header.tenant_id != expected_tenant_id {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"encrypted tiered slab footer tenant {} does not match record header tenant {} for slab {}",
				expected_tenant_id.get(),
				header.tenant_id.get(),
				slab_id,
			),
		));
	}
	Ok(())
}

fn extract_record_from_blob(blob: &[u8], location: RecordLocation) -> std::io::Result<Vec<u8>> {
	let offset = location.offset as usize;
	let length = location.length as usize;
	let header_size = std::mem::size_of::<crate::schema::SlabRecordHeader>();
	if blob.len() < offset + header_size {
		return Err(Error::new(
			ErrorKind::UnexpectedEof,
			"L2 blob shorter than record header",
		));
	}
	let header = bytemuck::pod_read_unaligned::<crate::schema::SlabRecordHeader>(
		&blob[offset..offset + header_size],
	);
	let frame_len = header.frame_len as usize;
	let end = offset
		.checked_add(header_size)
		.and_then(|h| h.checked_add(frame_len))
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "frame_len overflows usize"))?;
	if end > blob.len() || header_size + frame_len > length {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"L2 record extends past downloaded blob",
		));
	}
	Ok(blob[offset + header_size..end].to_vec())
}

async fn fetch_tiered_ciphertext_runs(
	l2: Arc<dyn ObjectStore>,
	pointer: &TieredSlabPointer,
	plan: &TieredMultiRangeReadPlan,
	max_parallel_range_gets: usize,
) -> std::io::Result<Vec<Bytes>> {
	fetch_l2_ranges_bounded(
		l2,
		pointer.l2_uri.clone(),
		plan.ciphertext_ranges_in_object(),
		max_parallel_range_gets,
	)
	.await
}

async fn fetch_l2_ranges_bounded(
	l2: Arc<dyn ObjectStore>,
	uri: String,
	ranges: Vec<std::ops::Range<u64>>,
	max_parallel_range_gets: usize,
) -> std::io::Result<Vec<Bytes>> {
	if ranges.is_empty() {
		return Ok(Vec::new());
	}
	let max_parallel = max_parallel_range_gets.max(1).min(ranges.len());
	if max_parallel == 1 || ranges.len() == 1 {
		let mut out = Vec::with_capacity(ranges.len());
		for range in ranges {
			out.push(l2.get_range(&uri, range).await?);
		}
		return Ok(out);
	}

	let mut out = vec![None; ranges.len()];
	let mut next = 0usize;
	let mut join = JoinSet::new();
	while next < ranges.len() || !join.is_empty() {
		while next < ranges.len() && join.len() < max_parallel {
			let l2 = l2.clone();
			let uri = uri.clone();
			let range = ranges[next].clone();
			let index = next;
			join.spawn(async move {
				let bytes = l2.get_range(&uri, range).await?;
				Ok::<_, std::io::Error>((index, bytes))
			});
			next += 1;
		}
		let Some(joined) = join.join_next().await else {
			continue;
		};
		match joined {
			Ok(Ok((index, bytes))) => out[index] = Some(bytes),
			Ok(Err(err)) => return Err(err),
			Err(err) => {
				return Err(Error::other(format!(
					"L2 range fetch task failed before returning ciphertext: {err}"
				)));
			}
		}
	}
	out.into_iter()
		.enumerate()
		.map(|(index, bytes)| {
			bytes.ok_or_else(|| Error::other(format!("L2 range fetch {index} did not complete")))
		})
		.collect()
}

fn decode_full_projected_l2_record(
	slab_id: SlabId,
	location: RecordLocation,
	descriptor: &TableSchemaDescriptorV1,
	requirement: &ColumnFetchRequirement,
	frame: &[u8],
) -> std::io::Result<ProjectedRecordBatch> {
	let directory = parse_record_buffer_directory(
		TieredRecordId {
			slab_id,
			record_offset: RecordBodyByteOffset::new(location.offset),
		},
		PlaintextByteRange {
			start: 0,
			len: frame.len() as u64,
		},
		frame,
		descriptor,
	)
	.map_err(columnar_io_error)?;
	if directory.page_directory.is_some() {
		let batch = crate::storage::multirow::decode_batch(frame)?;
		return crate::storage::row_group::project_record_batch_for_requirement(
			&batch,
			requirement,
		);
	}
	decode_projected_record_batch(frame, &directory, requirement).map_err(arrow_io_error)
}

fn indexed_record_fetch_key(location: RecordLocation) -> (SlabId, u64, u32, bool) {
	(
		location.slab_id,
		location.offset,
		location.length,
		location.is_tombstone,
	)
}

fn take_projected_indexed_rows(
	read: &ProjectedL2RecordRead,
	record_rows: &[RecordRowIndex],
) -> std::io::Result<ProjectedRecordBatch> {
	if record_rows.is_empty() {
		return read.projected.take_rows(&[]);
	}
	if let L2FetchStrategy::RowGroup(plan) = &read.strategy {
		if plan.surviving_groups.is_empty() {
			return Ok(read.projected.slice(0, 0));
		}
		let mut group_offsets = HashMap::new();
		let mut projected_offset = 0usize;
		for group_index in &plan.surviving_groups {
			let group = plan
				.directory
				.row_groups
				.iter()
				.find(|group| group.row_group_index == *group_index)
				.copied()
				.ok_or_else(|| {
					Error::new(
						ErrorKind::InvalidData,
						format!(
							"row-group strategy selected missing group {}",
							group_index.get()
						),
					)
				})?;
			group_offsets.insert(*group_index, (projected_offset, group));
			projected_offset = projected_offset.saturating_add(group.row_count.get() as usize);
		}
		let mut offsets = Vec::with_capacity(record_rows.len());
		for row in record_rows {
			let group = crate::storage::row_group::row_group_for_record_row(&plan.directory, *row)
				.ok_or_else(|| {
					Error::new(
						ErrorKind::InvalidData,
						format!(
							"indexed record row {} is not covered by row-group directory",
							row.get()
						),
					)
				})?;
			let Some((base_offset, selected_group)) = group_offsets.get(&group.row_group_index)
			else {
				return Err(Error::new(
					ErrorKind::InvalidData,
					format!(
						"indexed record row {} maps to row group {} that was not fetched",
						row.get(),
						group.row_group_index.get()
					),
				));
			};
			let within_group = row
				.get()
				.checked_sub(selected_group.first_record_row_index.get())
				.ok_or_else(|| {
					Error::new(
						ErrorKind::InvalidData,
						format!(
							"indexed record row {} precedes row group {} base {}",
							row.get(),
							selected_group.row_group_index.get(),
							selected_group.first_record_row_index.get()
						),
					)
				})? as usize;
			offsets.push(base_offset.saturating_add(within_group));
		}
		return read.projected.take_rows(&offsets);
	}
	let base = read.projected_first_record_row_index.get();
	let offsets = record_rows
		.iter()
		.map(|row| {
			row.get().checked_sub(base).ok_or_else(|| {
				Error::new(
					ErrorKind::InvalidData,
					format!(
						"indexed record row {} precedes projected base {}",
						row.get(),
						base
					),
				)
			})
		})
		.map(|result| result.map(|offset| offset as usize))
		.collect::<std::io::Result<Vec<_>>>()?;
	read.projected.take_rows(&offsets)
}

fn build_encoding_compute_phase_b_plan(
	record_directory: &RecordBufferDirectory,
	page_directory: &crate::storage::row_group::RecordPageDirectory,
	encoding_directory: &EncodingDirectory,
	encoded_predicate: &EncodedPredicateNode,
	selected_groups: &[RowGroupIndex],
	predicate_fetched_groups: &[RowGroupIndex],
	config: &EncodingComputeConfig,
	header_size: u64,
) -> Result<EncodingComputePhaseBPlan, EncodingComputeFallbackReason> {
	let mut builder = EncodingComputePhaseBPlanBuilder {
		record_directory,
		leaves: Vec::new(),
	};
	let node = builder.build_node(encoded_predicate)?;
	let leaves = builder.leaves;
	let predicate_fetched = predicate_fetched_groups
		.iter()
		.copied()
		.collect::<BTreeSet<_>>();
	let mut fetches = Vec::new();
	let mut encoded_pages = 0_u64;
	for group_index in selected_groups {
		if !predicate_fetched.contains(group_index) {
			continue;
		}
		let group = page_directory
			.row_groups
			.iter()
			.find(|group| group.row_group_index == *group_index)
			.ok_or(EncodingComputeFallbackReason::UnsupportedArrowLayout)?;
		if group.row_count.get() < config.min_buffer_rows_for_encoded_eval.get() {
			return Err(EncodingComputeFallbackReason::DenseSelection);
		}
		for (leaf_index, leaf) in leaves.iter().enumerate() {
			match leaf {
				EncodingComputePhaseBLeaf::Encoded {
					atom,
					column_index,
					data_type,
					physical_ordinal,
					..
				} => {
					let (_, value_buffer_ordinal) =
						encoding_compute_value_buffer(record_directory, *column_index)?;
					let page = page_directory
						.buffer_pages
						.iter()
						.find(|page| {
							page.row_group_index == *group_index
								&& page.ipc_column_ordinal
									== IpcColumnOrdinal::new(*physical_ordinal as u32)
								&& page.ipc_buffer_ordinal == value_buffer_ordinal
								&& page.range.len.get() > 0
						})
						.ok_or(EncodingComputeFallbackReason::UnsupportedArrowLayout)?;
					let encoding = find_buffer_encoding(
						encoding_directory,
						*group_index,
						value_buffer_ordinal,
						page.range,
					)
					.ok_or(EncodingComputeFallbackReason::UnsupportedCascade)?;
					if encoding.decoded_value_count.get() != group.row_count.get() {
						return Err(EncodingComputeFallbackReason::UnsupportedArrowLayout);
					}
					let cascade = encoding_directory
						.cascades
						.get(encoding.cascade_index.get() as usize)
						.ok_or(EncodingComputeFallbackReason::UnsupportedCascade)?;
					let evaluator = evaluator_kind_for_cascade(cascade)
						.ok_or(EncodingComputeFallbackReason::UnsupportedCascade)?;
					if !config.predicate_evaluator_enabled(evaluator) {
						return Err(EncodingComputeFallbackReason::UnsupportedCascade);
					}
					let Some(coercion) = coerce_literal(atom.literal.as_ref(), data_type) else {
						return Err(EncodingComputeFallbackReason::UnsupportedProgram);
					};
					if matches!(
						native_evaluation_eligibility(cascade, atom, &coercion),
						NativeEvaluationEligibility::DecodeScalarFallback
					) {
						return Err(EncodingComputeFallbackReason::UnsupportedCascade);
					}
					encoded_pages =
						encoded_pages.saturating_add(encoding.page_end_offsets.len().max(1) as u64);
					if encoded_pages > u64::from(config.max_encoded_pages_per_record.get()) {
						return Err(EncodingComputeFallbackReason::TooManyPages);
					}
					fetches.push(EncodingComputePhaseBFetch {
						leaf_index,
						row_group_index: *group_index,
						body_range: encoding_compute_fetch_range_for_buffer_page(
							page.range, encoding,
						)?,
						kind: EncodingComputePhaseBFetchKind::Encoded {
							encoding: encoding.clone(),
							cascade: cascade.clone(),
						},
					});
					if let Some(validity_fetch) = encoding_compute_validity_page_for_row_group(
						record_directory,
						page_directory,
						encoding_directory,
						*group_index,
						*physical_ordinal,
					)? {
						fetches.push(EncodingComputePhaseBFetch {
							leaf_index,
							row_group_index: *group_index,
							body_range: validity_fetch.body_range,
							kind: EncodingComputePhaseBFetchKind::Validity {
								encoding: validity_fetch.encoding,
								cascade: validity_fetch.cascade,
							},
						});
					}
				}
				EncodingComputePhaseBLeaf::Validity {
					physical_ordinal, ..
				} => {
					if let Some(validity_fetch) = encoding_compute_validity_page_for_row_group(
						record_directory,
						page_directory,
						encoding_directory,
						*group_index,
						*physical_ordinal,
					)? {
						fetches.push(EncodingComputePhaseBFetch {
							leaf_index,
							row_group_index: *group_index,
							body_range: validity_fetch.body_range,
							kind: EncodingComputePhaseBFetchKind::Validity {
								encoding: validity_fetch.encoding,
								cascade: validity_fetch.cascade,
							},
						});
					}
				}
			}
		}
	}
	let mut record_body_ranges = fetches
		.iter()
		.map(|fetch| fetch.body_range)
		.collect::<Vec<_>>();
	coalesce_manager_record_body_ranges(&mut record_body_ranges)
		.map_err(|_| EncodingComputeFallbackReason::UnsupportedArrowLayout)?;
	let body_ranges = record_body_ranges
		.iter()
		.map(|range| TieredPlaintextRange::new(range.start, range.len))
		.collect::<Vec<_>>();
	let record_ranges = record_body_ranges_to_record_ranges(header_size, &body_ranges)
		.map_err(|_| EncodingComputeFallbackReason::UnsupportedArrowLayout)?;
	Ok(EncodingComputePhaseBPlan {
		node,
		leaves,
		fetches,
		record_body_ranges,
		body_ranges,
		record_ranges,
	})
}

fn build_encoding_predicate_page_plan(
	record_directory: &RecordBufferDirectory,
	page_directory: &crate::storage::row_group::RecordPageDirectory,
	encoding_directory: &EncodingDirectory,
	encoded_predicate: &EncodedPredicateNode,
	selected_groups: &[RowGroupIndex],
	predicate_fetched_groups: &[RowGroupIndex],
	config: &EncodingComputeConfig,
	header_size: u64,
) -> Result<EncodingPredicatePagePlan, EncodingComputeFallbackReason> {
	let mut builder = EncodingComputePhaseBPlanBuilder {
		record_directory,
		leaves: Vec::new(),
	};
	let node = builder.build_node(encoded_predicate)?;
	let leaves = builder.leaves;
	let predicate_fetched = predicate_fetched_groups
		.iter()
		.copied()
		.collect::<BTreeSet<_>>();
	let mut decisions = Vec::new();
	let mut fetches = Vec::new();
	let mut selected_value_bytes = 0_u64;
	let mut dense_value_bytes = 0_u64;
	let mut encoded_pages = 0_u64;
	for group_index in selected_groups {
		if !predicate_fetched.contains(group_index) {
			continue;
		}
		let group = page_directory
			.row_groups
			.iter()
			.find(|group| group.row_group_index == *group_index)
			.ok_or(EncodingComputeFallbackReason::UnsupportedArrowLayout)?;
		if group.row_count.get() < config.min_buffer_rows_for_encoded_eval.get() {
			return Err(EncodingComputeFallbackReason::DenseSelection);
		}
		for (leaf_index, leaf) in leaves.iter().enumerate() {
			match leaf {
				EncodingComputePhaseBLeaf::Encoded {
					atom,
					column_index,
					data_type,
					..
				} => {
					let (physical_ordinal, value_buffer_ordinal) =
						encoding_compute_value_buffer(record_directory, *column_index)?;
					build_encoding_predicate_page_leaf_decisions(
						page_directory,
						encoding_directory,
						group,
						leaf_index,
						physical_ordinal,
						value_buffer_ordinal,
						data_type,
						atom,
						config,
						&mut decisions,
						&mut fetches,
						&mut selected_value_bytes,
						&mut dense_value_bytes,
						&mut encoded_pages,
					)?;
				}
				EncodingComputePhaseBLeaf::Validity {
					physical_ordinal,
					selected_when_no_nulls,
				} => {
					let mapping = record_directory
						.column_to_buffers
						.get(*physical_ordinal)
						.ok_or(EncodingComputeFallbackReason::UnsupportedArrowLayout)?;
					if mapping.buffer_count != 2 {
						return Err(EncodingComputeFallbackReason::UnsupportedArrowLayout);
					}
					let value_buffer_ordinal = IpcBufferIndex::new(
						mapping
							.first_buffer_ordinal
							.get()
							.checked_add(1)
							.ok_or(EncodingComputeFallbackReason::UnsupportedArrowLayout)?,
					);
					let field = record_directory
						.ipc_schema
						.fields()
						.get(*physical_ordinal)
						.ok_or(EncodingComputeFallbackReason::UnsupportedArrowLayout)?;
					let atom = if *selected_when_no_nulls {
						PredicateAtom::is_not_null()
					} else {
						PredicateAtom::is_null()
					};
					build_encoding_predicate_page_leaf_decisions(
						page_directory,
						encoding_directory,
						group,
						leaf_index,
						*physical_ordinal,
						value_buffer_ordinal,
						field.data_type(),
						&atom,
						config,
						&mut decisions,
						&mut fetches,
						&mut selected_value_bytes,
						&mut dense_value_bytes,
						&mut encoded_pages,
					)?;
				}
			}
		}
	}
	if selected_value_bytes >= dense_value_bytes {
		return Err(EncodingComputeFallbackReason::DenseSelection);
	}
	let mut record_body_ranges = fetches
		.iter()
		.map(|fetch| fetch.body_range)
		.collect::<Vec<_>>();
	coalesce_manager_record_body_ranges(&mut record_body_ranges)
		.map_err(|_| EncodingComputeFallbackReason::UnsupportedArrowLayout)?;
	let body_ranges = record_body_ranges
		.iter()
		.map(|range| TieredPlaintextRange::new(range.start, range.len))
		.collect::<Vec<_>>();
	let record_ranges = record_body_ranges_to_record_ranges(header_size, &body_ranges)
		.map_err(|_| EncodingComputeFallbackReason::UnsupportedArrowLayout)?;
	Ok(EncodingPredicatePagePlan {
		node,
		leaves,
		decisions,
		fetches,
		record_body_ranges,
		body_ranges,
		record_ranges,
	})
}

#[allow(clippy::too_many_arguments)]
fn build_encoding_predicate_page_leaf_decisions(
	page_directory: &crate::storage::row_group::RecordPageDirectory,
	encoding_directory: &EncodingDirectory,
	group: &crate::storage::row_group::RowGroupDirectoryEntry,
	leaf_index: usize,
	physical_ordinal: usize,
	value_buffer_ordinal: IpcBufferIndex,
	data_type: &DataType,
	atom: &PredicateAtom,
	config: &EncodingComputeConfig,
	decisions: &mut Vec<EncodingPredicatePageDecision>,
	fetches: &mut Vec<EncodingPredicatePageFetch>,
	selected_value_bytes: &mut u64,
	dense_value_bytes: &mut u64,
	encoded_pages: &mut u64,
) -> Result<(), EncodingComputeFallbackReason> {
	let page = page_directory
		.buffer_pages
		.iter()
		.find(|page| {
			page.row_group_index == group.row_group_index
				&& page.ipc_column_ordinal == IpcColumnOrdinal::new(physical_ordinal as u32)
				&& page.ipc_buffer_ordinal == value_buffer_ordinal
				&& page.range.len.get() > 0
		})
		.ok_or(EncodingComputeFallbackReason::UnsupportedArrowLayout)?;
	let encoding = find_buffer_encoding(
		encoding_directory,
		group.row_group_index,
		value_buffer_ordinal,
		page.range,
	)
	.ok_or(EncodingComputeFallbackReason::UnsupportedCascade)?;
	if encoding.decoded_value_count.get() != group.row_count.get()
		|| encoding.page_stats.is_empty()
		|| encoding.page_stats.len() != encoding.page_end_offsets.len()
	{
		return Err(EncodingComputeFallbackReason::UnsupportedCascade);
	}
	let cascade = encoding_directory
		.cascades
		.get(encoding.cascade_index.get() as usize)
		.ok_or(EncodingComputeFallbackReason::UnsupportedCascade)?;
	if !page_stats_cascade_eligible(cascade) {
		return Err(EncodingComputeFallbackReason::UnsupportedCascade);
	}
	let evaluator = evaluator_kind_for_cascade(cascade)
		.ok_or(EncodingComputeFallbackReason::UnsupportedCascade)?;
	if !config.predicate_evaluator_enabled(evaluator) {
		return Err(EncodingComputeFallbackReason::UnsupportedCascade);
	}
	if !matches!(
		atom.op,
		crate::storage::encoding::compute::PredicateAtomOp::IsNull
			| crate::storage::encoding::compute::PredicateAtomOp::IsNotNull
	) {
		let Some(coercion) = coerce_literal(atom.literal.as_ref(), data_type) else {
			return Err(EncodingComputeFallbackReason::UnsupportedProgram);
		};
		if matches!(
			native_evaluation_eligibility(cascade, atom, &coercion),
			NativeEvaluationEligibility::DecodeScalarFallback
		) {
			return Err(EncodingComputeFallbackReason::UnsupportedCascade);
		}
	}
	*dense_value_bytes = dense_value_bytes.saturating_add(
		encoding_compute_fetch_range_for_buffer_page(page.range, encoding)?
			.len
			.get(),
	);
	let pages = expand_page_descriptors(encoding)
		.map_err(|_| EncodingComputeFallbackReason::UnsupportedArrowLayout)?;
	let bytes_per_value = decoded_bytes_per_encoded_value(encoding).ok();
	for (page_descriptor, stat) in pages.iter().zip(&encoding.page_stats) {
		let classification =
			classify_page_stats_for_atom(stat, page_descriptor.value_count.get(), data_type, atom)
				.ok_or(EncodingComputeFallbackReason::UnsupportedProgram)?;
		let first_row = page_descriptor.first_value_index.get();
		let row_count = page_descriptor.value_count.get();
		match classification {
			PagePredicateClassification::Disjoint => {
				decisions.push(EncodingPredicatePageDecision {
					row_group_index: group.row_group_index,
					leaf_index,
					first_row,
					row_count,
					classification: EncodingPredicatePageDecisionKind::Disjoint,
				});
			}
			PagePredicateClassification::ExactMatch => {
				decisions.push(EncodingPredicatePageDecision {
					row_group_index: group.row_group_index,
					leaf_index,
					first_row,
					row_count,
					classification: EncodingPredicatePageDecisionKind::ExactMatch,
				});
			}
			PagePredicateClassification::Maybe => {
				if stat.null_count.get() != 0
					|| matches!(
						atom.op,
						crate::storage::encoding::compute::PredicateAtomOp::IsNull
							| crate::storage::encoding::compute::PredicateAtomOp::IsNotNull
					) {
					return Err(EncodingComputeFallbackReason::UnsupportedCascade);
				}
				let Some(bytes_per_value) = bytes_per_value else {
					return Err(EncodingComputeFallbackReason::UnsupportedArrowLayout);
				};
				*encoded_pages = encoded_pages.saturating_add(1);
				if *encoded_pages > u64::from(config.max_encoded_pages_per_record.get()) {
					return Err(EncodingComputeFallbackReason::TooManyPages);
				}
				let body_range = encoding_compute_fetch_range_for_encoded_page(
					page.range,
					encoding,
					page_descriptor.byte_range,
				)?;
				*selected_value_bytes = selected_value_bytes.saturating_add(body_range.len.get());
				let fetch_index = fetches.len();
				fetches.push(EncodingPredicatePageFetch {
					body_range,
					encoding: encoding_for_predicate_page(
						encoding,
						row_count,
						bytes_per_value,
						page_descriptor.byte_range.len,
						stat.clone(),
					)?,
					cascade: cascade.clone(),
					data_type: data_type.clone(),
					atom: atom.clone(),
				});
				decisions.push(EncodingPredicatePageDecision {
					row_group_index: group.row_group_index,
					leaf_index,
					first_row,
					row_count,
					classification: EncodingPredicatePageDecisionKind::Maybe { fetch_index },
				});
			}
		}
	}
	Ok(())
}

fn decoded_bytes_per_encoded_value(
	encoding: &BufferEncoding,
) -> Result<usize, EncodingComputeFallbackReason> {
	let values = usize::try_from(encoding.decoded_value_count.get())
		.map_err(|_| EncodingComputeFallbackReason::UnsupportedArrowLayout)?;
	let bytes = usize::try_from(encoding.decoded_byte_len.get())
		.map_err(|_| EncodingComputeFallbackReason::UnsupportedArrowLayout)?;
	if values == 0 || bytes == 0 || bytes % values != 0 {
		return Err(EncodingComputeFallbackReason::UnsupportedArrowLayout);
	}
	Ok(bytes / values)
}

fn encoding_compute_fetch_range_for_encoded_page(
	buffer_range: crate::storage::row_group::RecordBodyByteRange,
	encoding: &BufferEncoding,
	page_range: EncodedBufferByteRange,
) -> Result<crate::storage::row_group::RecordBodyByteRange, EncodingComputeFallbackReason> {
	let relative_start = u64::from(encoding.encoded_range.start.get())
		.checked_add(u64::from(page_range.start.get()))
		.ok_or(EncodingComputeFallbackReason::UnsupportedArrowLayout)?;
	let relative_end = relative_start
		.checked_add(page_range.len.get())
		.ok_or(EncodingComputeFallbackReason::UnsupportedArrowLayout)?;
	let encoded_end = encoding
		.encoded_range
		.end()
		.map_err(|_| EncodingComputeFallbackReason::UnsupportedArrowLayout)?;
	if relative_end > encoded_end || encoded_end > buffer_range.len.get() {
		return Err(EncodingComputeFallbackReason::UnsupportedArrowLayout);
	}
	let start = buffer_range
		.start
		.get()
		.checked_add(relative_start)
		.ok_or(EncodingComputeFallbackReason::UnsupportedArrowLayout)?;
	Ok(crate::storage::row_group::RecordBodyByteRange {
		start: RecordBodyByteOffset::new(start),
		len: page_range.len,
	})
}

fn encoding_for_predicate_page(
	encoding: &BufferEncoding,
	row_count: u32,
	bytes_per_value: usize,
	encoded_len: RecordBodyByteLen,
	stat: crate::storage::encoding::EncodingPageStats,
) -> Result<BufferEncoding, EncodingComputeFallbackReason> {
	let decoded_byte_len = u64::from(row_count)
		.checked_mul(
			u64::try_from(bytes_per_value)
				.map_err(|_| EncodingComputeFallbackReason::UnsupportedArrowLayout)?,
		)
		.ok_or(EncodingComputeFallbackReason::UnsupportedArrowLayout)?;
	let encoded_len_u32 = u32::try_from(encoded_len.get())
		.map_err(|_| EncodingComputeFallbackReason::UnsupportedArrowLayout)?;
	Ok(BufferEncoding {
		row_group_index: encoding.row_group_index,
		ipc_column_ordinal: encoding.ipc_column_ordinal,
		ipc_buffer_ordinal: encoding.ipc_buffer_ordinal,
		cascade_index: encoding.cascade_index,
		decoded_byte_len: RecordBodyByteLen::new(decoded_byte_len),
		decoded_value_count: BufferValueCount::new(row_count),
		encoded_range: EncodedBufferByteRange {
			start: EncodingPageByteOffset::ZERO,
			len: encoded_len,
		},
		page_value_count: SimdPageValueCount::new(row_count),
		page_end_offsets: vec![EncodingPageByteOffset::new(encoded_len_u32)],
		page_stats: vec![stat],
	})
}

fn build_encoding_compute_aggregate_plan(
	record_directory: &RecordBufferDirectory,
	page_directory: &crate::storage::row_group::RecordPageDirectory,
	encoding_directory: &EncodingDirectory,
	request: &AggregateRequest,
	group: crate::storage::row_group::RowGroupDirectoryEntry,
	config: &EncodingComputeConfig,
	header_size: u64,
) -> Result<EncodingComputeAggregatePlan, EncodingComputeFallbackReason> {
	let mut fetches = Vec::new();
	let mut fetched_columns = BTreeSet::new();
	let mut pages_fetched = 0_u64;
	for expression in &request.aggregates {
		let Some(kind) = aggregate_kind_for_tag(expression.tag) else {
			return Err(EncodingComputeFallbackReason::AggregateUnsupported);
		};
		if !config.aggregate_kernel_enabled(kind) {
			return Err(EncodingComputeFallbackReason::AggregateUnsupported);
		}
		let function = expression
			.function()
			.map_err(|_| EncodingComputeFallbackReason::AggregateUnsupported)?;
		let Some(column) = function.column() else {
			continue;
		};
		if fetched_columns.contains(&column) {
			continue;
		}
		let physical_ordinal =
			encoding_compute_physical_user_column_ordinal(record_directory, column as usize)?;
		let validity_fetch = encoding_compute_validity_page_for_row_group(
			record_directory,
			page_directory,
			encoding_directory,
			group.row_group_index,
			physical_ordinal,
		)?;
		if validity_fetch.is_some() {
			pages_fetched = pages_fetched.saturating_add(1);
			if pages_fetched > u64::from(config.max_encoded_pages_per_record.get()) {
				return Err(EncodingComputeFallbackReason::TooManyPages);
			}
		}
		let field = record_directory
			.ipc_schema
			.fields()
			.get(physical_ordinal)
			.ok_or(EncodingComputeFallbackReason::UnsupportedArrowLayout)?;
		if matches!(
			function,
			crate::schema::AggregateFunction::CountNonNull { .. }
		) {
			fetches.push(EncodingComputeAggregateFetch {
				column,
				value_body_range: None,
				validity_fetch,
				encoding: None,
				cascade: None,
				data_type: field.data_type().clone(),
			});
			fetched_columns.insert(column);
			continue;
		}
		let (_, value_buffer_ordinal) =
			encoding_compute_value_buffer(record_directory, column as usize)?;
		let page = page_directory
			.buffer_pages
			.iter()
			.find(|page| {
				page.row_group_index == group.row_group_index
					&& page.ipc_column_ordinal == IpcColumnOrdinal::new(physical_ordinal as u32)
					&& page.ipc_buffer_ordinal == value_buffer_ordinal
					&& page.range.len.get() > 0
			})
			.ok_or(EncodingComputeFallbackReason::UnsupportedArrowLayout)?;
		let encoding = find_buffer_encoding(
			encoding_directory,
			group.row_group_index,
			value_buffer_ordinal,
			page.range,
		)
		.ok_or(EncodingComputeFallbackReason::UnsupportedCascade)?;
		if encoding.decoded_value_count.get() != group.row_count.get() {
			return Err(EncodingComputeFallbackReason::UnsupportedArrowLayout);
		}
		let cascade = encoding_directory
			.cascades
			.get(encoding.cascade_index.get() as usize)
			.ok_or(EncodingComputeFallbackReason::UnsupportedCascade)?;
		if evaluator_kind_for_cascade(cascade).is_none() {
			return Err(EncodingComputeFallbackReason::UnsupportedCascade);
		}
		if !native_aggregate_cascade_eligible(&function, field.data_type(), Some(cascade)) {
			return Err(EncodingComputeFallbackReason::AggregateUnsupported);
		}
		pages_fetched = pages_fetched.saturating_add(encoding.page_end_offsets.len().max(1) as u64);
		if pages_fetched > u64::from(config.max_encoded_pages_per_record.get()) {
			return Err(EncodingComputeFallbackReason::TooManyPages);
		}
		fetches.push(EncodingComputeAggregateFetch {
			column,
			value_body_range: Some(encoding_compute_fetch_range_for_buffer_page(
				page.range, encoding,
			)?),
			validity_fetch,
			encoding: Some(encoding.clone()),
			cascade: Some(cascade.clone()),
			data_type: field.data_type().clone(),
		});
		fetched_columns.insert(column);
	}
	let mut record_body_ranges = fetches
		.iter()
		.flat_map(|fetch| {
			[
				fetch.value_body_range,
				fetch
					.validity_fetch
					.as_ref()
					.map(|validity| validity.body_range),
			]
		})
		.flatten()
		.collect::<Vec<_>>();
	coalesce_manager_record_body_ranges(&mut record_body_ranges)
		.map_err(|_| EncodingComputeFallbackReason::UnsupportedArrowLayout)?;
	let body_ranges = record_body_ranges
		.iter()
		.map(|range| TieredPlaintextRange::new(range.start, range.len))
		.collect::<Vec<_>>();
	let record_ranges = record_body_ranges_to_record_ranges(header_size, &body_ranges)
		.map_err(|_| EncodingComputeFallbackReason::UnsupportedArrowLayout)?;
	Ok(EncodingComputeAggregatePlan {
		fetches,
		record_body_ranges,
		body_ranges,
		record_ranges,
		pages_fetched,
	})
}

fn populate_encoding_compute_non_null_counts(
	context: &mut EncodedAggregateContext,
	record_directory: &RecordBufferDirectory,
	page_directory: &crate::storage::row_group::RecordPageDirectory,
	group: crate::storage::row_group::RowGroupDirectoryEntry,
	request: &AggregateRequest,
) {
	for expression in &request.aggregates {
		let Ok(function) = expression.function() else {
			continue;
		};
		let Some(column) = function.column() else {
			continue;
		};
		let Ok(physical_ordinal) =
			encoding_compute_physical_user_column_ordinal(record_directory, column as usize)
		else {
			continue;
		};
		if let Some(null_count) = encoding_compute_row_group_column_null_count(
			record_directory,
			page_directory,
			group,
			physical_ordinal,
		) {
			let non_null = u64::from(group.row_count.get()).saturating_sub(null_count.get());
			context
				.column_non_null_counts
				.insert(column, RowCount::new(non_null));
		}
	}
}

fn encoding_compute_row_group_column_null_count(
	record_directory: &RecordBufferDirectory,
	page_directory: &crate::storage::row_group::RecordPageDirectory,
	group: crate::storage::row_group::RowGroupDirectoryEntry,
	physical_ordinal: usize,
) -> Option<RowCount> {
	let physical = IpcColumnOrdinal::new(physical_ordinal as u32);
	if let Some(stat) = page_directory.row_group_stats.iter().find(|stat| {
		stat.row_group_index == group.row_group_index && stat.ipc_column_ordinal == physical
	}) {
		return Some(stat.null_count);
	}
	let record_nulls = record_directory
		.column_null_counts
		.get(physical_ordinal)
		.copied()?;
	if record_nulls == 0 {
		return Some(RowCount::new(0));
	}
	let record_nulls = u64::try_from(record_nulls).ok()?;
	if record_nulls == page_directory.row_count.get() {
		return Some(RowCount::new(u64::from(group.row_count.get())));
	}
	None
}

fn encoding_compute_row_group_column_is_non_null(
	record_directory: &RecordBufferDirectory,
	page_directory: &crate::storage::row_group::RecordPageDirectory,
	group: crate::storage::row_group::RowGroupDirectoryEntry,
	physical_ordinal: usize,
) -> bool {
	encoding_compute_row_group_column_null_count(
		record_directory,
		page_directory,
		group,
		physical_ordinal,
	)
	.is_some_and(|count| count.get() == 0)
}

fn encoding_compute_validity_page_for_row_group(
	record_directory: &RecordBufferDirectory,
	page_directory: &crate::storage::row_group::RecordPageDirectory,
	encoding_directory: &EncodingDirectory,
	row_group_index: RowGroupIndex,
	physical_ordinal: usize,
) -> Result<Option<EncodingComputeValidityFetch>, EncodingComputeFallbackReason> {
	let mapping = record_directory
		.column_to_buffers
		.get(physical_ordinal)
		.ok_or(EncodingComputeFallbackReason::UnsupportedArrowLayout)?;
	if mapping.buffer_count != 2 {
		return Err(EncodingComputeFallbackReason::UnsupportedArrowLayout);
	}
	if page_directory
		.row_groups
		.iter()
		.find(|group| group.row_group_index == row_group_index)
		.is_some_and(|group| {
			encoding_compute_row_group_column_is_non_null(
				record_directory,
				page_directory,
				*group,
				physical_ordinal,
			)
		}) {
		return Ok(None);
	}
	let physical = IpcColumnOrdinal::new(physical_ordinal as u32);
	let validity_buffer_ordinal = mapping.first_buffer_ordinal;
	let Some(page) = page_directory.buffer_pages.iter().find(|page| {
		page.row_group_index == row_group_index
			&& page.ipc_column_ordinal == physical
			&& page.ipc_buffer_ordinal == validity_buffer_ordinal
	}) else {
		let known_non_null = page_directory
			.row_groups
			.iter()
			.find(|group| group.row_group_index == row_group_index)
			.is_some_and(|group| {
				encoding_compute_row_group_column_is_non_null(
					record_directory,
					page_directory,
					*group,
					physical_ordinal,
				)
			});
		return if known_non_null {
			Ok(None)
		} else {
			Err(EncodingComputeFallbackReason::UnsupportedArrowLayout)
		};
	};
	if page.range.len.get() == 0 {
		Ok(None)
	} else {
		let encoding = find_buffer_encoding(
			encoding_directory,
			row_group_index,
			validity_buffer_ordinal,
			page.range,
		);
		let (encoding, cascade) = match encoding {
			Some(encoding) => {
				let cascade = encoding_directory
					.cascades
					.get(encoding.cascade_index.get() as usize)
					.ok_or(EncodingComputeFallbackReason::UnsupportedCascade)?;
				(Some(encoding.clone()), Some(cascade.clone()))
			}
			None => (None, None),
		};
		let body_range = match encoding.as_ref() {
			Some(encoding) => encoding_compute_fetch_range_for_buffer_page(page.range, encoding)?,
			None => page.range,
		};
		Ok(Some(EncodingComputeValidityFetch {
			body_range,
			encoding,
			cascade,
		}))
	}
}

fn encoding_compute_single_row_mask(
	rows: RowCount,
	group: crate::storage::row_group::RowGroupDirectoryEntry,
	record_row_index: RecordRowIndex,
) -> Result<EncodedRowMask, EncodingComputeRejectCode> {
	let offset = record_row_index
		.get()
		.checked_sub(group.first_record_row_index.get())
		.ok_or(EncodingComputeRejectCode::SelectionRangeOverflow)?;
	if offset >= group.row_count.get() {
		return Err(EncodingComputeRejectCode::SelectionRangeOverflow);
	}
	let mut values = vec![
		false;
		usize::try_from(rows.get())
			.map_err(|_| EncodingComputeRejectCode::SelectionRangeOverflow)?
	];
	let offset =
		usize::try_from(offset).map_err(|_| EncodingComputeRejectCode::SelectionRangeOverflow)?;
	values[offset] = true;
	Ok(EncodedRowMask::from_bool_slice(&values))
}

fn encoding_compute_aggregate_kernels_enabled(
	request: &AggregateRequest,
	config: &EncodingComputeConfig,
) -> bool {
	request.aggregates.iter().all(|expression| {
		aggregate_kind_for_tag(expression.tag)
			.is_some_and(|kind| config.aggregate_kernel_enabled(kind))
	})
}

fn observe_encoding_compute_aggregate_request(
	request: &AggregateRequest,
	decision: &str,
	reason: &str,
) {
	for expression in &request.aggregates {
		if let Some(kind) = aggregate_kind_for_tag(expression.tag) {
			metrics().observe_storage_l2_encoding_compute_aggregate(
				kind.as_str(),
				decision,
				reason,
			);
		}
	}
}

struct EncodingComputePhaseBPlanBuilder<'a> {
	record_directory: &'a RecordBufferDirectory,
	leaves: Vec<EncodingComputePhaseBLeaf>,
}

impl EncodingComputePhaseBPlanBuilder<'_> {
	fn build_node(
		&mut self,
		node: &EncodedPredicateNode,
	) -> Result<EncodingComputePhaseBNode, EncodingComputeFallbackReason> {
		match node {
			EncodedPredicateNode::Comparison(comparison) => {
				self.push_encoded_leaf(comparison.column.column_index, comparison.atom.clone())
			}
			EncodedPredicateNode::InList(in_list) => {
				self.push_encoded_leaf(in_list.column.column_index, in_list.atom.clone())
			}
			EncodedPredicateNode::IsNull(column) => {
				self.push_trivial_null_leaf(column.column_index, false)
			}
			EncodedPredicateNode::IsNotNull(column) => {
				self.push_trivial_null_leaf(column.column_index, true)
			}
			EncodedPredicateNode::And(branches) => {
				if branches.is_empty() {
					return Err(EncodingComputeFallbackReason::UnsupportedProgram);
				}
				let nodes = branches
					.iter()
					.map(|branch| self.build_node(branch))
					.collect::<Result<Vec<_>, _>>()?;
				Ok(EncodingComputePhaseBNode::And(nodes))
			}
			EncodedPredicateNode::Or(branches) => {
				if branches.is_empty() {
					return Err(EncodingComputeFallbackReason::UnsupportedProgram);
				}
				let nodes = branches
					.iter()
					.map(|branch| self.build_node(branch))
					.collect::<Result<Vec<_>, _>>()?;
				Ok(EncodingComputePhaseBNode::Or(nodes))
			}
			EncodedPredicateNode::Not(branch) => Ok(EncodingComputePhaseBNode::Not(Box::new(
				self.build_node(branch)?,
			))),
		}
	}

	fn push_encoded_leaf(
		&mut self,
		column_index: usize,
		atom: PredicateAtom,
	) -> Result<EncodingComputePhaseBNode, EncodingComputeFallbackReason> {
		let (physical_ordinal, _) =
			encoding_compute_value_buffer(self.record_directory, column_index)?;
		let field = self
			.record_directory
			.ipc_schema
			.fields()
			.get(physical_ordinal)
			.ok_or(EncodingComputeFallbackReason::UnsupportedArrowLayout)?;
		if !encoding_compute_predicate_data_type_supported(field.data_type()) {
			return Err(EncodingComputeFallbackReason::UnsupportedArrowLayout);
		}
		if coerce_literal(atom.literal.as_ref(), field.data_type()).is_none() {
			return Err(EncodingComputeFallbackReason::UnsupportedProgram);
		}
		let leaf_index = self.leaves.len();
		self.leaves.push(EncodingComputePhaseBLeaf::Encoded {
			column_index,
			physical_ordinal,
			atom,
			data_type: field.data_type().clone(),
		});
		Ok(EncodingComputePhaseBNode::Leaf(leaf_index))
	}

	fn push_trivial_null_leaf(
		&mut self,
		column_index: usize,
		selected_when_no_nulls: bool,
	) -> Result<EncodingComputePhaseBNode, EncodingComputeFallbackReason> {
		let physical_ordinal =
			encoding_compute_physical_user_column_ordinal(self.record_directory, column_index)?;
		let leaf_index = self.leaves.len();
		self.leaves.push(EncodingComputePhaseBLeaf::Validity {
			physical_ordinal,
			selected_when_no_nulls,
		});
		Ok(EncodingComputePhaseBNode::Leaf(leaf_index))
	}
}

fn encoding_compute_physical_user_column_ordinal(
	directory: &RecordBufferDirectory,
	descriptor_ordinal: usize,
) -> Result<usize, EncodingComputeFallbackReason> {
	let user_physical_ordinals = directory
		.ipc_schema
		.fields()
		.iter()
		.enumerate()
		.filter_map(|(physical, field)| {
			StorageColumn::from_name(field.name())
				.is_none()
				.then_some(physical)
		})
		.collect::<Vec<_>>();
	user_physical_ordinals
		.get(descriptor_ordinal)
		.copied()
		.ok_or(EncodingComputeFallbackReason::UnsupportedArrowLayout)
}

fn encoding_compute_value_buffer(
	directory: &RecordBufferDirectory,
	descriptor_ordinal: usize,
) -> Result<(usize, IpcBufferIndex), EncodingComputeFallbackReason> {
	let physical_ordinal =
		encoding_compute_physical_user_column_ordinal(directory, descriptor_ordinal)?;
	let mapping = directory
		.column_to_buffers
		.get(physical_ordinal)
		.ok_or(EncodingComputeFallbackReason::UnsupportedArrowLayout)?;
	if mapping.buffer_count != 2 {
		return Err(EncodingComputeFallbackReason::UnsupportedArrowLayout);
	}
	let value_buffer_ordinal = IpcBufferIndex::new(
		mapping
			.first_buffer_ordinal
			.get()
			.checked_add(1)
			.ok_or(EncodingComputeFallbackReason::UnsupportedArrowLayout)?,
	);
	Ok((physical_ordinal, value_buffer_ordinal))
}

fn encoding_compute_predicate_data_type_supported(data_type: &DataType) -> bool {
	matches!(
		data_type,
		DataType::Boolean
			| DataType::Int8
			| DataType::Int16
			| DataType::Int32
			| DataType::Int64
			| DataType::UInt8
			| DataType::UInt16
			| DataType::UInt32
			| DataType::UInt64
			| DataType::Float32
			| DataType::Float64
			| DataType::Date32
			| DataType::Date64
			| DataType::Time32(_)
			| DataType::Time64(_)
			| DataType::Timestamp(_, _)
			| DataType::Duration(_)
			| DataType::FixedSizeBinary(_)
	)
}

fn encoding_compute_fetch_range_for_buffer_page(
	buffer_range: crate::storage::row_group::RecordBodyByteRange,
	encoding: &BufferEncoding,
) -> Result<crate::storage::row_group::RecordBodyByteRange, EncodingComputeFallbackReason> {
	let encoded_start = u64::from(encoding.encoded_range.start.get());
	let encoded_end = encoding
		.encoded_range
		.end()
		.map_err(|_| EncodingComputeFallbackReason::UnsupportedArrowLayout)?;
	if encoded_end > buffer_range.len.get() {
		return Err(EncodingComputeFallbackReason::UnsupportedArrowLayout);
	}
	let start = buffer_range
		.start
		.get()
		.checked_add(encoded_start)
		.ok_or(EncodingComputeFallbackReason::UnsupportedArrowLayout)?;
	Ok(crate::storage::row_group::RecordBodyByteRange {
		start: RecordBodyByteOffset::new(start),
		len: encoding.encoded_range.len,
	})
}

fn encoding_compute_mask_selectivity_allowed(
	mask: &EncodedRowMask,
	config: &EncodingComputeConfig,
) -> Result<bool, EncodingComputeRejectCode> {
	let rows = mask.rows().get();
	let selected = mask.popcount()?.get();
	let micros = if rows == 0 {
		0
	} else {
		selected
			.saturating_mul(1_000_000)
			.checked_div(rows)
			.unwrap_or(1_000_000)
			.min(1_000_000) as u32
	};
	Ok(encoded_leaf_selectivity_allowed(
		SelectivityFractionMicros::new(micros),
		config,
	))
}

fn encoding_compute_eval_node(
	node: &EncodingComputePhaseBNode,
	leaf_masks: &[EncodedRowMask],
) -> Result<EncodedRowMask, EncodingComputeRejectCode> {
	match node {
		EncodingComputePhaseBNode::Leaf(index) => leaf_masks
			.get(*index)
			.cloned()
			.ok_or(EncodingComputeRejectCode::MaskLengthMismatch),
		EncodingComputePhaseBNode::And(branches) => {
			let mut iter = branches.iter();
			let first = iter
				.next()
				.ok_or(EncodingComputeRejectCode::MaskLengthMismatch)?;
			let mut out = encoding_compute_eval_node(first, leaf_masks)?;
			for branch in iter {
				out = out.and(&encoding_compute_eval_node(branch, leaf_masks)?)?;
			}
			Ok(out)
		}
		EncodingComputePhaseBNode::Or(branches) => {
			let mut iter = branches.iter();
			let first = iter
				.next()
				.ok_or(EncodingComputeRejectCode::MaskLengthMismatch)?;
			let mut out = encoding_compute_eval_node(first, leaf_masks)?;
			for branch in iter {
				out = out.or(&encoding_compute_eval_node(branch, leaf_masks)?)?;
			}
			Ok(out)
		}
		EncodingComputePhaseBNode::Not(branch) => {
			encoding_compute_eval_node(branch, leaf_masks)?.not()
		}
	}
}

fn encoding_compute_invariant_error(code: EncodingComputeRejectCode) -> std::io::Error {
	metrics().observe_storage_l2_encoding_compute_invariant_violation(code.as_str());
	Error::new(
		ErrorKind::InvalidData,
		format!("Encoding compute invariant violation: {code:?}"),
	)
}

fn phase_b_point_read_survives(
	phase_b: &crate::storage::row_group::PhaseBOutcome,
	group: crate::storage::row_group::RowGroupDirectoryEntry,
	record_row_index: RecordRowIndex,
) -> std::io::Result<bool> {
	let survival_mask = phase_b
		.survival_masks
		.get(&group.row_group_index)
		.ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				format!(
					"Phase B survival mask missing for target row group {}",
					group.row_group_index.get()
				),
			)
		})?;
	let row_offset = record_row_index
		.get()
		.checked_sub(group.first_record_row_index.get())
		.ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				format!(
					"record row index {} precedes row group {} base {}",
					record_row_index.get(),
					group.row_group_index.get(),
					group.first_record_row_index.get()
				),
			)
		})? as usize;
	if row_offset >= survival_mask.len() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"record row index {} maps outside Phase B mask length {} for row group {}",
				record_row_index.get(),
				survival_mask.len(),
				group.row_group_index.get()
			),
		));
	}
	Ok(survival_mask.is_valid(row_offset) && survival_mask.value(row_offset))
}

fn build_encoding_indexed_page_plan(
	record_directory: &RecordBufferDirectory,
	page_directory: &crate::storage::row_group::RecordPageDirectory,
	encoding_directory: &EncodingDirectory,
	requirement: &ColumnFetchRequirement,
	selected_record_rows: &[RecordRowIndex],
	selected_groups: &[RowGroupIndex],
) -> Result<Option<EncodingIndexedPagePlan>, EncodingFallbackReason> {
	let physical_ordinals = required_projected_physical_ordinals(record_directory, requirement)
		.ok_or(EncodingFallbackReason::UnsupportedArrowLayout)?;
	if physical_ordinals.is_empty() {
		return Ok(Some(EncodingIndexedPagePlan {
			selected_groups: selected_groups.to_vec(),
			row_pages: selected_record_rows
				.iter()
				.map(|row| {
					let group =
						crate::storage::row_group::row_group_for_record_row(page_directory, *row)
							.ok_or(EncodingFallbackReason::UnsupportedArrowLayout)?;
					Ok(EncodingIndexedRowPage {
						page_key: PageMaterializeKey {
							row_group_index: group.row_group_index,
							page_index: 0,
						},
						row_offset_in_page: 0,
					})
				})
				.collect::<Result<Vec<_>, _>>()?,
			columns: Vec::new(),
			fetches: Vec::new(),
		}));
	}

	let mut columns = Vec::with_capacity(physical_ordinals.len());
	for physical_ordinal in physical_ordinals {
		let column = fixed_width_encoding_indexed_value_column(record_directory, physical_ordinal)
			.ok_or(EncodingFallbackReason::UnsupportedArrowLayout)?;
		columns.push(column);
	}

	let first_column = columns
		.first()
		.ok_or(EncodingFallbackReason::UnsupportedArrowLayout)?;
	let mut row_pages = Vec::with_capacity(selected_record_rows.len());
	let mut fetches_by_key = BTreeMap::<PageMaterializeBufferKey, PageMaterializeFetch>::new();
	for row in selected_record_rows {
		let group = crate::storage::row_group::row_group_for_record_row(page_directory, *row)
			.ok_or(EncodingFallbackReason::UnsupportedArrowLayout)?;
		let row_within_group = row
			.get()
			.checked_sub(group.first_record_row_index.get())
			.ok_or(EncodingFallbackReason::UnsupportedArrowLayout)?;
		let mut page_key = None;
		for column in &columns {
			let column_page = page_directory
				.buffer_pages
				.iter()
				.find(|page| {
					page.row_group_index == group.row_group_index
						&& page.ipc_column_ordinal
							== IpcColumnOrdinal::new(column.physical_ordinal as u32)
						&& page.ipc_buffer_ordinal == column.value_buffer_ordinal
						&& page.range.len.get() > 0
				})
				.ok_or(EncodingFallbackReason::UnsupportedArrowLayout)?;
			let encoding = encoding_directory
				.buffers
				.iter()
				.find(|encoding| {
					encoding.row_group_index == group.row_group_index
						&& encoding.ipc_column_ordinal == column_page.ipc_column_ordinal
						&& encoding.ipc_buffer_ordinal == column.value_buffer_ordinal
						&& !encoding.page_end_offsets.is_empty()
				})
				.ok_or(EncodingFallbackReason::UnsupportedArrowLayout)?;
			let pages = expand_page_descriptors(encoding)
				.map_err(|_| EncodingFallbackReason::UnsupportedArrowLayout)?;
			let page = pages
				.into_iter()
				.find(|page| {
					let start = page.first_value_index.get();
					let end = start.saturating_add(page.value_count.get());
					row_within_group >= start && row_within_group < end
				})
				.ok_or(EncodingFallbackReason::UnsupportedArrowLayout)?;
			let candidate_page_key = PageMaterializeKey {
				row_group_index: group.row_group_index,
				page_index: page.page_index.get(),
			};
			if column.physical_ordinal == first_column.physical_ordinal {
				let row_offset = row_within_group
					.checked_sub(page.first_value_index.get())
					.ok_or(EncodingFallbackReason::UnsupportedArrowLayout)?
					as usize;
				page_key = Some((candidate_page_key, row_offset));
			} else if Some(candidate_page_key) != page_key.map(|(key, _)| key) {
				return Err(EncodingFallbackReason::UnsupportedArrowLayout);
			}
			let cascade = encoding_directory
				.cascades
				.get(encoding.cascade_index.get() as usize)
				.ok_or(EncodingFallbackReason::UnsupportedArrowLayout)?
				.clone();
			let fetch_key = PageMaterializeBufferKey {
				row_group_index: group.row_group_index,
				ipc_column_ordinal: column_page.ipc_column_ordinal,
				ipc_buffer_ordinal: column.value_buffer_ordinal,
				page_index: page.page_index.get(),
			};
			fetches_by_key.entry(fetch_key).or_insert_with(|| {
				metrics()
					.observe_storage_l2_encoding_simd_pages_fetched(cascade.kind().as_str(), 1);
				PageMaterializeFetch {
					key: fetch_key,
					body_range: encoded_page_body_range(column_page.range, encoding, page),
					cascade,
					page,
					bytes_per_value: column.bytes_per_value,
					data_type: column.data_type.clone(),
				}
			});
		}
		let Some((page_key, row_offset_in_page)) = page_key else {
			return Err(EncodingFallbackReason::UnsupportedArrowLayout);
		};
		row_pages.push(EncodingIndexedRowPage {
			page_key,
			row_offset_in_page,
		});
	}

	let fetches = fetches_by_key.into_values().collect::<Vec<_>>();
	if fetches.is_empty() && !columns.is_empty() {
		return Ok(None);
	}
	Ok(Some(EncodingIndexedPagePlan {
		selected_groups: selected_groups.to_vec(),
		row_pages,
		columns,
		fetches,
	}))
}

fn encoding_materialize_audit_for_indexed_plan(
	plan: &EncodingIndexedPagePlan,
) -> Vec<EncodingMaterializeAuditSample> {
	let mut by_cascade = BTreeMap::<&'static str, (u64, u64)>::new();
	for fetch in &plan.fetches {
		let decoded =
			u64::from(fetch.page.value_count.get()).saturating_mul(fetch.bytes_per_value as u64);
		let entry = by_cascade
			.entry(fetch.cascade.kind().as_str())
			.or_insert((0, 0));
		entry.0 = entry.0.saturating_add(fetch.body_range.len.get());
		entry.1 = entry.1.saturating_add(decoded);
	}
	by_cascade
		.into_iter()
		.map(
			|(cascade, (buffer_bytes_in, buffer_bytes_out))| EncodingMaterializeAuditSample {
				cascade,
				buffer_bytes_in,
				buffer_bytes_out,
			},
		)
		.collect()
}

fn materialize_sparse_projection_pages<B: AsRef<[u8]>>(
	record_directory: &RecordBufferDirectory,
	requirement: &ColumnFetchRequirement,
	plan: &SparseProjectionFetchPlan,
	selected_ranges: &[TieredPlaintextRange],
	plaintext_extracts: &[B],
	predicate_outcome: &crate::storage::row_group::PhaseBOutcome,
) -> Result<ProjectedRecordBatch, arrow::error::ArrowError> {
	if plan.selected_groups.is_empty() {
		return empty_projected_batch_for_requirement(record_directory, requirement, 0);
	}
	let mut decoded_pages = BTreeMap::<PageMaterializeBufferKey, Vec<u8>>::new();
	for fetch in &plan.fetches {
		let encoded =
			extract_encoded_page_bytes(fetch.body_range, selected_ranges, plaintext_extracts)?;
		let decoded = decode_page_fetch(fetch, &encoded)?;
		decoded_pages.insert(fetch.key, decoded);
	}
	let mut decoded_validity_pages =
		BTreeMap::<PageMaterializeBufferKey, PageValidityBitmap>::new();
	for fetch in &plan.validity_fetches {
		if fetch.page.value_count.get() != fetch.page_row_count.get() {
			return Err(arrow::error::ArrowError::SchemaError(format!(
				"sparse projection validity fetch page row count {} disagrees with page descriptor {}",
				fetch.page_row_count.get(),
				fetch.page.value_count.get()
			)));
		}
		let fetched =
			extract_encoded_page_bytes(fetch.body_range, selected_ranges, plaintext_extracts)?;
		let validity = rebase_validity_bitmap(
			&fetched,
			fetch.bit_offset,
			fetch.page_row_count.get() as usize,
		)?;
		decoded_validity_pages.insert(fetch.key, validity);
	}

	let mut filtered = Vec::new();
	let mut template = None;
	for group in &plan.group_pages {
		let mask = predicate_outcome
			.survival_masks
			.get(&group.row_group_index)
			.ok_or_else(|| {
				arrow::error::ArrowError::SchemaError(format!(
					"Sparse projection mask missing for row group {}",
					group.row_group_index.get()
				))
			})?;
		for page in &group.selected_pages {
			let page_key = PageMaterializeKey {
				row_group_index: group.row_group_index,
				page_index: page.page_index.get(),
			};
			let page_batch = projected_page_batch_from_decoded_buffers(
				record_directory,
				requirement,
				&plan.columns,
				page_key,
				page.value_count.get() as usize,
				&decoded_pages,
				&decoded_validity_pages,
				&plan.all_valid_nullable_pages,
			)?;
			template.get_or_insert_with(|| page_batch.clone());
			let start = page.first_value_index.get() as usize;
			let len = page.value_count.get() as usize;
			if start.checked_add(len).is_none_or(|end| end > mask.len()) {
				return Err(arrow::error::ArrowError::SchemaError(format!(
					"Sparse projection page mask range {}..{} exceeds mask length {}",
					start,
					start.saturating_add(len),
					mask.len()
				)));
			}
			let local_mask = arrow::array::BooleanArray::from_iter(
				(start..start + len).map(|row| Some(mask.is_valid(row) && mask.value(row))),
			);
			if local_mask.true_count() == 0 {
				continue;
			}
			if local_mask.null_count() == 0 && local_mask.true_count() == local_mask.len() {
				filtered.push(page_batch);
				continue;
			}
			let batch = arrow::compute::filter_record_batch(&page_batch.batch, &local_mask)?;
			filtered.push(ProjectedRecordBatch {
				batch,
				ordinal_mapping: page_batch.ordinal_mapping,
				storage_mapping: page_batch.storage_mapping,
			});
		}
	}
	if filtered.is_empty() {
		if let Some(template) = template {
			return Ok(template.slice(0, 0));
		}
		return empty_projected_batch_for_requirement(record_directory, requirement, 0);
	}
	concat_projected_record_batches(filtered)
}

fn materialize_encoding_indexed_pages<B: AsRef<[u8]>>(
	record_directory: &RecordBufferDirectory,
	requirement: &ColumnFetchRequirement,
	plan: &EncodingIndexedPagePlan,
	selected_ranges: &[TieredPlaintextRange],
	plaintext_extracts: &[B],
	selected_row_count: usize,
	descriptor: &TableSchemaDescriptorV1,
) -> Result<ProjectedRecordBatch, arrow::error::ArrowError> {
	if plan.columns.is_empty() {
		return projected_batch_for_empty_indexed_rows(
			record_directory,
			selected_row_count,
			descriptor,
		);
	}
	let mut decoded_pages = BTreeMap::<PageMaterializeBufferKey, Vec<u8>>::new();
	for fetch in &plan.fetches {
		let encoded =
			extract_encoded_page_bytes(fetch.body_range, selected_ranges, plaintext_extracts)?;
		let decoded = decode_page_fetch(fetch, &encoded)?;
		decoded_pages.insert(fetch.key, decoded);
	}
	let decoded_validity_pages = BTreeMap::<PageMaterializeBufferKey, PageValidityBitmap>::new();
	let all_valid_nullable_pages = BTreeSet::<PageMaterializeBufferKey>::new();

	let mut page_batches = BTreeMap::<PageMaterializeKey, ProjectedRecordBatch>::new();
	for page_key in plan.row_pages.iter().map(|row| row.page_key) {
		if page_batches.contains_key(&page_key) {
			continue;
		}
		let page_fetch = plan
			.fetches
			.iter()
			.find(|fetch| {
				fetch.key.row_group_index == page_key.row_group_index
					&& fetch.key.page_index == page_key.page_index
			})
			.ok_or_else(|| {
				arrow::error::ArrowError::SchemaError(
					"Encoding indexed page batch missing fetch".into(),
				)
			})?;
		let page_batch = projected_page_batch_from_decoded_buffers(
			record_directory,
			requirement,
			&plan.columns,
			page_key,
			page_fetch.page.value_count.get() as usize,
			&decoded_pages,
			&decoded_validity_pages,
			&all_valid_nullable_pages,
		)?;
		page_batches.insert(page_key, page_batch);
	}

	let mut rows = Vec::with_capacity(plan.row_pages.len());
	for row_page in &plan.row_pages {
		let page_batch = page_batches.get(&row_page.page_key).ok_or_else(|| {
			arrow::error::ArrowError::SchemaError(
				"Encoding indexed page batch missing for selected row".into(),
			)
		})?;
		rows.push(
			page_batch
				.take_rows(&[row_page.row_offset_in_page])
				.map_err(|err| arrow::error::ArrowError::ExternalError(Box::new(err)))?,
		);
	}
	concat_projected_record_batches(rows)
}

fn projected_batch_for_empty_indexed_rows(
	record_directory: &RecordBufferDirectory,
	row_count: usize,
	descriptor: &TableSchemaDescriptorV1,
) -> Result<ProjectedRecordBatch, arrow::error::ArrowError> {
	let schema = std::sync::Arc::new(arrow::datatypes::Schema::empty());
	let batch = arrow::record_batch::RecordBatch::try_new_with_options(
		schema,
		Vec::new(),
		&arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(row_count)),
	)?;
	let descriptor_len = descriptor
		.arrow_schema()
		.map(|schema| schema.fields().len())
		.unwrap_or_else(|_| record_directory.ipc_schema.fields().len());
	Ok(ProjectedRecordBatch {
		batch,
		ordinal_mapping: ColumnOrdinalMapping::new(vec![None; descriptor_len]),
		storage_mapping: BTreeMap::new(),
	})
}

fn concat_projected_record_batches(
	batches: Vec<ProjectedRecordBatch>,
) -> Result<ProjectedRecordBatch, arrow::error::ArrowError> {
	let Some(first) = batches.first() else {
		return Err(arrow::error::ArrowError::InvalidArgumentError(
			"Encoding indexed page materialization produced no batches".into(),
		));
	};
	let schema = first.batch.schema();
	let raw = batches
		.iter()
		.map(|batch| batch.batch.clone())
		.collect::<Vec<_>>();
	let batch = arrow::compute::concat_batches(&schema, &raw)?;
	Ok(ProjectedRecordBatch {
		batch,
		ordinal_mapping: first.ordinal_mapping.clone(),
		storage_mapping: first.storage_mapping.clone(),
	})
}

fn extract_record_body_bytes<B: AsRef<[u8]>>(
	range: crate::storage::row_group::RecordBodyByteRange,
	selected_ranges: &[TieredPlaintextRange],
	plaintext_extracts: &[B],
) -> Result<Vec<u8>, arrow::error::ArrowError> {
	if selected_ranges.len() != plaintext_extracts.len() {
		return Err(arrow::error::ArrowError::InvalidArgumentError(format!(
			"Encoding indexed page expected {} plaintext extracts, got {}",
			selected_ranges.len(),
			plaintext_extracts.len()
		)));
	}
	let start = range.start.get();
	let end = range
		.end()
		.map_err(|err| arrow::error::ArrowError::SchemaError(err.to_string()))?;
	for (candidate, extract) in selected_ranges.iter().zip(plaintext_extracts.iter()) {
		let candidate_start = candidate.start.get();
		let candidate_end = candidate_start
			.checked_add(candidate.len.get())
			.ok_or_else(|| {
				arrow::error::ArrowError::SchemaError(
					"Encoding indexed plaintext range overflow".into(),
				)
			})?;
		if start >= candidate_start && end <= candidate_end {
			let relative_start = usize::try_from(start - candidate_start).map_err(|_| {
				arrow::error::ArrowError::SchemaError(
					"Encoding indexed relative page start exceeds usize".into(),
				)
			})?;
			let relative_end = usize::try_from(end - candidate_start).map_err(|_| {
				arrow::error::ArrowError::SchemaError(
					"Encoding indexed relative page end exceeds usize".into(),
				)
			})?;
			let extract = extract.as_ref();
			if relative_end > extract.len() {
				return Err(arrow::error::ArrowError::SchemaError(
					"Encoding indexed page range exceeds plaintext extract".into(),
				));
			}
			return Ok(extract[relative_start..relative_end].to_vec());
		}
	}
	Err(arrow::error::ArrowError::SchemaError(
		"Encoding indexed page range was not fetched".into(),
	))
}

fn coalesce_manager_record_body_ranges(
	ranges: &mut Vec<crate::storage::row_group::RecordBodyByteRange>,
) -> std::io::Result<()> {
	ranges.sort_by_key(|range| range.start.get());
	let mut coalesced = Vec::with_capacity(ranges.len());
	for range in ranges.drain(..) {
		if range.len.get() == 0 {
			continue;
		}
		let Some(last): Option<&mut crate::storage::row_group::RecordBodyByteRange> =
			coalesced.last_mut()
		else {
			coalesced.push(range);
			continue;
		};
		let last_end = last.end()?;
		let range_end = range.end()?;
		if range.start.get() <= last_end {
			last.len = RecordBodyByteLen::new(range_end.max(last_end) - last.start.get());
		} else {
			coalesced.push(range);
		}
	}
	*ranges = coalesced;
	Ok(())
}

fn encoding_compute_decode_error(code: EncodingRejectCode) -> std::io::Error {
	metrics().observe_storage_l2_encoding_invariant_violation(code.as_str());
	Error::new(
		ErrorKind::InvalidData,
		format!("Encoding compute validity decode rejected: {code:?}"),
	)
}

fn record_body_location(location: RecordLocation) -> std::io::Result<RecordLocation> {
	let header_size = slab_record_header_size_u64()?;
	let body_offset = location.offset.checked_add(header_size).ok_or_else(|| {
		Error::new(
			ErrorKind::InvalidData,
			format!(
				"record body offset overflows u64 for slab {} at offset {}",
				location.slab_id, location.offset
			),
		)
	})?;
	let body_len = u64::from(location.length)
		.checked_sub(header_size)
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "record is shorter than slab header"))?;
	let body_len = u32::try_from(body_len).map_err(|_| {
		Error::new(
			ErrorKind::InvalidData,
			format!("record body length {body_len} exceeds u32::MAX"),
		)
	})?;
	Ok(RecordLocation {
		offset: body_offset,
		length: body_len,
		..location
	})
}

fn record_plaintext_range_for_l2(
	location: RecordLocation,
) -> std::io::Result<std::ops::Range<usize>> {
	let start = usize::try_from(location.offset).map_err(|_| {
		Error::new(
			ErrorKind::InvalidData,
			format!("record offset {} does not fit into usize", location.offset),
		)
	})?;
	let end_u64 = location
		.offset
		.checked_add(u64::from(location.length))
		.ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				format!(
					"record range overflows u64 for slab {} at offset {}",
					location.slab_id, location.offset
				),
			)
		})?;
	let end = usize::try_from(end_u64).map_err(|_| {
		Error::new(
			ErrorKind::InvalidData,
			format!("record end {end_u64} does not fit into usize"),
		)
	})?;
	Ok(start..end)
}

fn record_body_ranges_to_record_ranges(
	header_size: u64,
	body_ranges: &[TieredPlaintextRange],
) -> std::io::Result<Vec<TieredPlaintextRange>> {
	body_ranges
		.iter()
		.map(|range| {
			let start = header_size.checked_add(range.start.get()).ok_or_else(|| {
				Error::new(ErrorKind::InvalidData, "record body range start overflow")
			})?;
			Ok(TieredPlaintextRange::new(
				RecordBodyByteOffset::new(start),
				range.len,
			))
		})
		.collect()
}

fn pick_point_row_group_fetch_strategy(
	config: &crate::storage::row_group::L2RowGroupFetchConfig,
	record_location: &RecordLocation,
	page_directory: &crate::storage::row_group::RecordPageDirectory,
	plan: crate::storage::row_group::RowGroupFetchPlan,
) -> L2FetchStrategy {
	if !config.enabled {
		return L2FetchStrategy::Fallback(ColumnarFallbackReason::Disabled);
	}
	if plan.surviving_groups.is_empty() {
		return L2FetchStrategy::RowGroup(plan);
	}
	let selected_group_scaled = (plan.surviving_groups.len() as u64).saturating_mul(u64::from(
		crate::storage::row_group::L2_ROW_GROUP_FETCH_FRACTION_DENOMINATOR,
	));
	let allowed_group_scaled = (page_directory.row_groups.len() as u64)
		.saturating_mul(u64::from(config.max_selected_row_group_fraction_micros));
	if selected_group_scaled > allowed_group_scaled {
		return L2FetchStrategy::Fallback(ColumnarFallbackReason::TooManyRowGroups);
	}
	let selected_ciphertext_bytes = plan
		.phase_b_fetch
		.as_ref()
		.map(|phase_b| phase_b.fetched_ciphertext_bytes)
		.unwrap_or(0)
		.saturating_add(plan.phase_c_fetch.fetched_ciphertext_bytes);
	let selected_byte_scaled = selected_ciphertext_bytes.saturating_mul(u64::from(
		crate::storage::row_group::L2_ROW_GROUP_FETCH_FRACTION_DENOMINATOR,
	));
	let allowed_byte_scaled = u64::from(record_location.length)
		.saturating_mul(u64::from(config.max_selected_byte_fraction_micros));
	if selected_byte_scaled > allowed_byte_scaled {
		return L2FetchStrategy::Fallback(ColumnarFallbackReason::TooManyBytes);
	}
	let selected_ranges = plan
		.phase_b_fetch
		.as_ref()
		.map(|phase_b| phase_b.chunk_runs.len())
		.unwrap_or(0)
		.saturating_add(plan.phase_c_fetch.chunk_runs.len());
	if selected_ranges > config.max_ranges_per_record.get() as usize {
		return L2FetchStrategy::Fallback(ColumnarFallbackReason::TooManyRanges);
	}
	L2FetchStrategy::RowGroup(plan)
}

fn observe_l2_row_group_fetch_strategy(
	metrics: &crate::metrics::MetricsRegistry,
	strategy: &L2FetchStrategy,
	planning_duration: Duration,
) {
	metrics.observe_storage_l2_row_group_fetch_plan(planning_duration);
	match strategy {
		L2FetchStrategy::RowGroup(plan) => {
			metrics.observe_storage_l2_row_group_fetch_request("row_group");
			let kept = plan.surviving_groups.len() as u64;
			let total = plan.directory.row_groups.len() as u64;
			if kept == 0 {
				metrics.observe_storage_l2_row_group_fetch_row_groups("no_survivors", total);
			} else {
				metrics.observe_storage_l2_row_group_fetch_row_groups("keep", kept);
				metrics.observe_storage_l2_row_group_fetch_row_groups(
					"predicate_fetch",
					plan.predicate_fetched_groups.len() as u64,
				);
				metrics.observe_storage_l2_row_group_fetch_row_groups(
					"skip",
					total.saturating_sub(kept),
				);
			}
			metrics.observe_storage_l2_row_group_fetch_bytes(
				"phase_b",
				plan.phase_b_fetch
					.as_ref()
					.map(|phase_b| phase_b.fetched_ciphertext_bytes)
					.unwrap_or(0),
			);
			metrics.observe_storage_l2_row_group_fetch_bytes(
				"phase_c",
				plan.phase_c_fetch.fetched_ciphertext_bytes,
			);
			metrics.observe_storage_l2_row_group_fetch_selected_ranges(
				plan.phase_c_fetch.chunk_runs.len().saturating_add(
					plan.phase_b_fetch
						.as_ref()
						.map(|phase_b| phase_b.chunk_runs.len())
						.unwrap_or(0),
				) as u64,
			);
			let selected_plaintext_bytes = plan
				.phase_c_fetch
				.selected_plaintext_ranges
				.iter()
				.map(|range| range.len.get())
				.sum::<u64>();
			let total_row_group_bytes = plan
				.directory
				.row_groups
				.iter()
				.map(|group| {
					group
						.metadata_range
						.len
						.get()
						.saturating_add(group.body_range.len.get())
				})
				.sum::<u64>();
			metrics.observe_storage_l2_row_group_fetch_bytes(
				"avoided",
				total_row_group_bytes.saturating_sub(selected_plaintext_bytes),
			);
		}
		L2FetchStrategy::Fallback(reason) => {
			metrics.observe_storage_l2_row_group_fetch_request("whole_record");
			metrics.observe_storage_l2_row_group_fetch_fallback(reason.as_str());
		}
		L2FetchStrategy::Columnar(_) => {
			metrics.observe_storage_l2_row_group_fetch_request("columnar_buffer");
		}
		L2FetchStrategy::WholeRecord => {
			metrics.observe_storage_l2_row_group_fetch_request("whole_record");
		}
	}
}

fn slab_record_header_size_u64() -> std::io::Result<u64> {
	u64::try_from(std::mem::size_of::<SlabRecordHeader>()).map_err(|_| {
		Error::new(
			ErrorKind::InvalidData,
			"slab record header size does not fit into u64",
		)
	})
}

fn columnar_io_error(error: crate::storage::columnar::ColumnFetchError) -> Error {
	Error::new(ErrorKind::InvalidData, error)
}

fn arrow_io_error(error: arrow::error::ArrowError) -> Error {
	Error::new(ErrorKind::InvalidData, error.to_string())
}

#[allow(dead_code)]
fn uncompressed_resident_bytes_in_slab(
	blob: &[u8],
	slab_id: SlabId,
	table_schemas: &TableSchemaCatalogV1,
) -> std::io::Result<u64> {
	let mut entries_by_record: BTreeMap<(u64, u32), Vec<RecoveredSlabEntry>> = BTreeMap::new();
	for entry in recover_slab_entries_from_blob(blob, slab_id)? {
		entries_by_record
			.entry((entry.location.offset, entry.location.length))
			.or_default()
			.push(entry);
	}

	let mut descriptors_by_tenant: HashMap<TenantId, Vec<TableSchemaDescriptorV1>> = HashMap::new();
	let mut bytes = 0_u64;
	for entries in entries_by_record.values() {
		let Some(first) = entries.first().copied() else {
			continue;
		};
		let frame = extract_record_from_blob(blob, first.location)?;
		let policy = match record_batch_compression_policy_from_ipc(&frame) {
			Ok(policy) => policy,
			Err(_) => continue,
		};
		if policy != RecordBodyCompressionPolicy::Uncompressed {
			continue;
		}
		let batch = match decode_record_batch(&frame) {
			Ok(batch) => batch,
			Err(_) => continue,
		};
		let mut matches_lz4_descriptor = false;
		for entry in entries {
			// Phase 8.5: malformed `__row_table_object_id__` is
			// storage corruption, not "treat as PK".
			let stored_table_object_id = crate::storage::multirow::storage_row_table_object_id_at(
				&batch,
				entry.location.record_row_index,
			)?;
			let row_batch =
				materialize_record_location_batch(&batch, entry.location.record_row_index)?;
			let descriptors = descriptors_by_tenant
				.entry(entry.tenant_id)
				.or_insert_with(|| {
					table_schemas
						.descriptors_for_tenant(entry.tenant_id)
						.into_iter()
						.filter(|descriptor| {
							descriptor.record_body_compression_policy
								== RecordBodyCompressionPolicy::Lz4Frame
						})
						.collect()
				});
			for descriptor in descriptors.iter() {
				if descriptor_matches_tiered_row(
					descriptor,
					entry.tenant_id,
					entry.key_identity,
					entry.routing_locality,
					stored_table_object_id,
					&row_batch,
				)? {
					matches_lz4_descriptor = true;
					break;
				}
			}
			if matches_lz4_descriptor {
				break;
			}
		}
		if matches_lz4_descriptor {
			bytes = bytes.saturating_add(frame.len() as u64);
		}
	}
	Ok(bytes)
}

fn physical_compaction_groups(
	records: &[CompactionRecord],
) -> BTreeMap<(u64, u32), Vec<CompactionRecord>> {
	let mut groups = BTreeMap::new();
	for record in records {
		groups
			.entry((record.old_location.offset, record.old_location.length))
			.or_insert_with(Vec::new)
			.push(*record);
	}
	groups
}

fn physical_compaction_group_indices(
	records: &[CompactionRecord],
) -> BTreeMap<(u64, u32), Vec<usize>> {
	let mut groups = BTreeMap::new();
	for (idx, record) in records.iter().enumerate() {
		groups
			.entry((record.old_location.offset, record.old_location.length))
			.or_insert_with(Vec::new)
			.push(idx);
	}
	groups
}

async fn append_existing_frame_to_replacement(
	replacement: &Arc<LocalSlabStore>,
	frame: &[u8],
	records: &[CompactionRecord],
	relocations: &mut Vec<RelocatedRecord>,
) -> std::io::Result<()> {
	let Some(first) = records.first().copied() else {
		return Ok(());
	};
	let hlc = records
		.iter()
		.map(|record| record.old_location.hlc)
		.max()
		.unwrap_or(first.old_location.hlc);
	let base_location = replacement
		.append(first.tenant_id, first.key_identity, hlc, frame)
		.await?;
	replacement.rollback_index_entry(first.tenant_id, first.key_identity, base_location);
	for record in records {
		let new_location = RecordLocation {
			hlc: record.old_location.hlc,
			record_row_index: record.old_location.record_row_index,
			is_tombstone: record.old_location.is_tombstone,
			..base_location
		};
		replacement.index_location(record.tenant_id, record.key_identity, new_location);
		relocations.push(RelocatedRecord {
			tenant_id: record.tenant_id,
			key_identity: record.key_identity,
			routing_locality: record.routing_locality,
			old_location: record.old_location,
			new_location,
		});
	}
	Ok(())
}

async fn append_compacted_frame_to_replacement(
	replacement: &Arc<LocalSlabStore>,
	compacted: CompactedFrame,
	relocations: &mut Vec<RelocatedRecord>,
) -> std::io::Result<()> {
	let base_location = replacement
		.append(
			compacted.tenant_id,
			compacted.key_identity,
			compacted.hlc,
			&compacted.frame,
		)
		.await?;
	replacement.rollback_index_entry(compacted.tenant_id, compacted.key_identity, base_location);
	for row in &compacted.rows {
		let new_location = RecordLocation {
			hlc: row.row_hlc,
			record_row_index: row.record_row_index,
			is_tombstone: row.old_location.is_tombstone,
			..base_location
		};
		replacement.index_location(row.tenant_id, row.key_identity, new_location);
		relocations.push(RelocatedRecord {
			tenant_id: row.tenant_id,
			key_identity: row.key_identity,
			routing_locality: row.routing_locality,
			old_location: row.old_location,
			new_location,
		});
	}
	Ok(())
}

fn compact_physical_record_frame(
	frame: &[u8],
	records: &[CompactionRecord],
	table_schemas: Option<&TableSchemaCatalogV1>,
	row_group_config: &crate::storage::row_group::RowGroupWriterConfig,
	encoding_config: &EncodingWriterConfig,
	chunk_layout_config: &L2ChunkLayoutConfig,
) -> std::io::Result<Option<CompactedFrame>> {
	let mut survivors = records
		.iter()
		.copied()
		.filter(|record| !record.reclaim)
		.collect::<Vec<_>>();
	if survivors.is_empty() {
		return Ok(None);
	}

	if storage_multirow_index_entries_from_frame(frame)?.is_none() {
		if records.len() != 1 {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"single-row storage record has multiple compaction index entries",
			));
		}
		let record = survivors[0];
		return Ok(Some(CompactedFrame {
			tenant_id: record.tenant_id,
			key_identity: record.key_identity,
			hlc: record.old_location.hlc,
			frame: frame.to_vec(),
			was_multirow: false,
			rows: vec![CompactedFrameRow {
				tenant_id: record.tenant_id,
				key_identity: record.key_identity,
				routing_locality: record.routing_locality,
				old_location: record.old_location,
				row_hlc: record.old_location.hlc,
				record_row_index: RecordRowIndex::ZERO,
			}],
		}));
	}

	let batch = decode_record_batch(frame)?;
	if !has_storage_synthetic_columns(&batch) {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"multi-row storage record decoded without synthetic columns",
		));
	}
	survivors.sort_by_key(|record| {
		(
			record.routing_locality,
			record.key_identity,
			record.old_location.hlc,
			record.old_location.record_row_index,
		)
	});
	// ADR 045 Phase 8: preserve the per-row `__row_table_object_id__`
	// stamp through compaction. AppendOnly rows must keep their
	// table-isolation binding; PK rows have `None`.
	let source_metadata = crate::storage::multirow::storage_rows_from_batch(&batch)?;
	let descriptor = compaction_row_group_descriptor(
		table_schemas,
		row_group_config,
		&survivors,
		&source_metadata,
	);
	let source_rows = survivors
		.iter()
		.map(|record| {
			let row_index = record.old_location.record_row_index.get() as usize;
			let table_object_id = source_metadata
				.get(row_index)
				.and_then(|meta| meta.table_object_id);
			MultiRowSourceRow {
				source_row_index: row_index,
				key_identity: record.key_identity,
				routing_locality: record.routing_locality,
				row_hlc: record.old_location.hlc,
				is_tombstone: record.old_location.is_tombstone,
				table_object_id,
			}
		})
		.collect::<Vec<_>>();
	let policy = crate::storage::ipc::record_batch_compression_policy_from_ipc(frame)?;
	let encoded = match descriptor {
		Some(descriptor) => {
			encode_multirow_from_survivors_with_row_group_encoding_and_chunk_layout_config(
				&batch,
				&source_rows,
				descriptor,
				policy,
				row_group_config,
				encoding_config,
				chunk_layout_config,
			)?
		}
		None => encode_multirow_from_survivors_with_policy(&batch, &source_rows, policy)?,
	};
	let entries = encoded.rows;
	if entries.len() != survivors.len() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"rewritten multi-row storage record entry count mismatch: expected={} actual={}",
				survivors.len(),
				entries.len()
			),
		));
	}

	let primary = entries[0];
	let hlc = encoded.max_row_hlc;
	let mut rows = Vec::with_capacity(entries.len());
	for (record, entry) in survivors.iter().zip(entries.iter()) {
		if record.key_identity != entry.key_identity
			|| record.routing_locality != entry.routing_locality
			|| record.old_location.hlc != entry.row_hlc
			|| record.old_location.is_tombstone != entry.is_tombstone
		{
			return Err(Error::new(
				ErrorKind::InvalidData,
				"rewritten multi-row storage metadata does not match surviving row",
			));
		}
		rows.push(CompactedFrameRow {
			tenant_id: record.tenant_id,
			key_identity: entry.key_identity,
			routing_locality: entry.routing_locality,
			old_location: record.old_location,
			row_hlc: entry.row_hlc,
			record_row_index: entry.record_row_index,
		});
	}
	Ok(Some(CompactedFrame {
		tenant_id: survivors[0].tenant_id,
		key_identity: primary.key_identity,
		hlc,
		frame: encoded.frame,
		was_multirow: true,
		rows,
	}))
}

fn compaction_row_group_descriptor<'a>(
	table_schemas: Option<&'a TableSchemaCatalogV1>,
	row_group_config: &crate::storage::row_group::RowGroupWriterConfig,
	survivors: &[CompactionRecord],
	source_metadata: &[StorageRowMetadata],
) -> Option<&'a TableSchemaDescriptorV1> {
	if !row_group_config.enabled {
		return None;
	}
	let table_schemas = table_schemas?;
	let first = survivors.first()?;
	let mut table_object_id = None;
	for record in survivors {
		if record.tenant_id != first.tenant_id {
			return None;
		}
		let metadata = source_metadata.get(record.old_location.record_row_index.get() as usize)?;
		match (table_object_id, metadata.table_object_id) {
			(None, Some(value)) => table_object_id = Some(value),
			(Some(expected), Some(value)) if expected == value => {}
			_ => return None,
		}
	}
	let descriptor = table_schemas.latest_by_id(first.tenant_id, table_object_id?)?;
	descriptor
		.record_layout_policy
		.is_multi_row()
		.then_some(descriptor)
}

fn descriptor_matches_tiered_row(
	descriptor: &TableSchemaDescriptorV1,
	tenant_id: TenantId,
	key_identity: Identity,
	routing_locality: Locality,
	stored_table_object_id: Option<TableObjectId>,
	batch: &RecordBatch,
) -> std::io::Result<bool> {
	if batch.num_rows() != 1 || batch.schema().as_ref() != &descriptor.arrow_schema()? {
		return Ok(false);
	}
	match &descriptor.row_identity_policy {
		// PK-keyed: re-derive identity / locality from the user
		// row and compare to the stored synthetic columns. Phase 8
		// also stamps `__row_table_object_id__` on multi-row PK
		// rows; when present, it gives us an early fail before
		// the (more expensive) PK re-derivation.
		RowIdentityPolicy::PrimaryKey(_) => {
			if let Some(stored) = stored_table_object_id
				&& stored != descriptor.table_object_id
			{
				return Ok(false);
			}
			let mut table_meta: TableWriteRequestMeta = bytemuck::Zeroable::zeroed();
			table_meta.tenant_id = tenant_id;
			table_meta.table_object_id = descriptor.table_object_id;
			table_meta.schema_generation = descriptor.schema_generation;
			table_meta.row_count = 1;
			let zero_hlc = HybridTimestamp {
				physical_ms: 0,
				logical: 0,
				node_id: 0,
			};
			let admission = match validate_table_write_request_batch(
				&table_meta,
				batch,
				descriptor,
				WriterNodeId::ZERO,
				zero_hlc,
			) {
				Ok(admission) => admission,
				Err(_) => return Ok(false),
			};
			Ok(admission.key_identities.as_slice() == [key_identity]
				&& admission.localities.as_slice() == [routing_locality])
		}
		// AppendOnly: identity is engine-synthesized at write
		// time from `(writer_node_id, commit_hlc,
		// intra_batch_index)` (not recoverable from the user
		// batch alone) and locality is request-level (depends
		// on `parent_write_id`). Phase 8 binds rows back to
		// their descriptor via `__row_table_object_id__` —
		// verify equality with `descriptor.table_object_id` so
		// table-spread prefix collisions across same-tenant
		// AppendOnly tables cannot leak rows.
		RowIdentityPolicy::AppendOnly => {
			let _ = (tenant_id, key_identity, routing_locality);
			Ok(stored_table_object_id == Some(descriptor.table_object_id))
		}
		RowIdentityPolicy::AppendOnlyContentHash => {
			if stored_table_object_id != Some(descriptor.table_object_id) {
				return Ok(false);
			}
			let mut table_meta: TableWriteRequestMeta = bytemuck::Zeroable::zeroed();
			table_meta.tenant_id = tenant_id;
			table_meta.table_object_id = descriptor.table_object_id;
			table_meta.schema_generation = descriptor.schema_generation;
			table_meta.row_count = 1;
			let zero_hlc = HybridTimestamp {
				physical_ms: 0,
				logical: 0,
				node_id: 0,
			};
			let admission = match validate_table_write_request_batch(
				&table_meta,
				batch,
				descriptor,
				WriterNodeId::ZERO,
				zero_hlc,
			) {
				Ok(admission) => admission,
				Err(_) => return Ok(false),
			};
			Ok(admission.key_identities.as_slice() == [key_identity]
				&& admission.localities.as_slice() == [routing_locality])
		}
	}
}

fn build_tiered_object_metadata(
	blob: &[u8],
	slab_id: SlabId,
	chunk_layout_config: &L2ChunkLayoutConfig,
) -> std::io::Result<TieredSlabObjectMetadata> {
	let entries = recover_slab_entries_from_blob(blob, slab_id)?;
	let Some(first_entry) = entries.first().copied() else {
		return Err(Error::new(
			ErrorKind::InvalidInput,
			format!(
				"sealed slab {} does not contain any committed records; tiered encryption requires non-empty single-tenant slabs",
				slab_id
			),
		));
	};
	let tenant_id = first_entry.tenant_id;
	let has_row_tombstone_records = entries.iter().any(|entry| entry.location.is_tombstone);
	let mut offset_index = Vec::with_capacity(entries.len());
	for entry in entries {
		let other_tenant_id = entry.tenant_id;
		let key_identity = entry.key_identity;
		let location = entry.location;
		if other_tenant_id != tenant_id {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				format!(
					"sealed slab {} contains multiple tenants ({} and {}); tiered encryption requires single-tenant slabs until W5 lands tenant-scoped active slabs",
					slab_id,
					tenant_id.get(),
					other_tenant_id.get(),
				),
			));
		}
		offset_index.push(TieredOffsetIndexEntry {
			key_locator: IdentityPrefix::from(key_identity),
			byte_offset: location.offset,
			byte_length: location.length,
			flags: 0,
		});
	}
	Ok(TieredSlabObjectMetadata {
		tenant_id,
		created_hlc: first_entry.location.hlc.physical_ms,
		chunk_size_bytes: choose_tiered_object_chunk_size(blob, slab_id, chunk_layout_config)?,
		offset_index,
		column_stats_block: None,
		has_row_tombstone_records,
	})
}

fn choose_tiered_object_chunk_size(
	blob: &[u8],
	slab_id: SlabId,
	chunk_layout_config: &L2ChunkLayoutConfig,
) -> std::io::Result<crate::schema::ChunkSizeBytes> {
	if !chunk_layout_config.enabled {
		return Ok(SLAB_ENCRYPTION_DEFAULT_CHUNK_SIZE);
	}
	let directories = encoding_directories_from_slab_blob(blob, slab_id)?;
	Ok(choose_chunk_size_bytes_for_directories(
		directories.iter(),
		chunk_layout_config,
	))
}

fn encoding_directories_from_slab_blob(
	blob: &[u8],
	slab_id: SlabId,
) -> std::io::Result<Vec<EncodingDirectory>> {
	let mut out = Vec::new();
	let mut seen_records = BTreeSet::new();
	for entry in recover_slab_entries_from_blob(blob, slab_id)? {
		if !seen_records.insert((entry.location.offset, entry.location.length)) {
			continue;
		}
		let frame = extract_record_from_blob(blob, entry.location)?;
		if !ipc_stream_contains_encoding_directory(&frame) {
			continue;
		}
		let reader = StreamReader::try_new(Cursor::new(frame), None).map_err(|err| {
			Error::new(
				ErrorKind::InvalidData,
				format!("arrow stream open for chunk-size selection: {err}"),
			)
		})?;
		let schema = reader.schema();
		let Some(value) = schema.metadata().get(ENCODING_DIRECTORY_METADATA_KEY) else {
			continue;
		};
		let directory =
			crate::storage::encoding::directory::deserialize_encoding_directory_value(value)
				.map_err(|code| {
					Error::new(
						ErrorKind::InvalidData,
						format!(
							"Encoding directory rejected during chunk-size selection: {code:?}"
						),
					)
				})?;
		out.push(directory);
	}
	Ok(out)
}

fn row_tombstone_metadata_from_blob(
	blob: &[u8],
	slab_id: SlabId,
) -> std::io::Result<TombstoneMetadata> {
	let entries = recover_slab_entries_from_blob(blob, slab_id)?;
	Ok(row_tombstone_metadata_from_entries(&entries))
}

fn row_tombstone_metadata_from_entries(entries: &[RecoveredSlabEntry]) -> TombstoneMetadata {
	let earliest = entries
		.iter()
		.filter(|entry| entry.location.is_tombstone)
		.map(|entry| entry.location.hlc)
		.min();
	earliest.map_or(TombstoneMetadata::None, TombstoneMetadata::Earliest)
}

fn merge_row_tombstone_metadata(
	current: TombstoneMetadata,
	rows: TombstoneMetadata,
) -> TombstoneMetadata {
	match (current, rows) {
		(TombstoneMetadata::Untracked, _) => TombstoneMetadata::Untracked,
		(current, TombstoneMetadata::None) => current,
		(TombstoneMetadata::None, TombstoneMetadata::Earliest(row_hlc)) => {
			TombstoneMetadata::Earliest(row_hlc)
		}
		(TombstoneMetadata::Earliest(existing), TombstoneMetadata::Earliest(row_hlc)) => {
			TombstoneMetadata::Earliest(existing.min(row_hlc))
		}
		(current, TombstoneMetadata::Untracked) => current,
	}
}

fn single_tenant_id_from_plain_slab(blob: &[u8], slab_id: SlabId) -> std::io::Result<TenantId> {
	Ok(build_tiered_object_metadata(blob, slab_id, &L2ChunkLayoutConfig::default())?.tenant_id)
}

fn slab_path(dir: &Path, slab_id: SlabId) -> PathBuf {
	dir.join(format!("slab_{:016x}.wnd", slab_id))
}

fn rehydrate_tmp_path(dir: &Path, slab_id: SlabId) -> PathBuf {
	dir.join(format!("slab_{:016x}.wnd.rehydrate.tmp", slab_id))
}

fn retention_rewrite_tmp_path(dir: &Path, slab_id: SlabId) -> PathBuf {
	dir.join(format!("slab_{:016x}.wnd.retention.tmp", slab_id))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct LsmSourceFrameKey {
	slab_id: SlabId,
	offset: u64,
	length: u32,
}

impl From<RecordLocation> for LsmSourceFrameKey {
	fn from(location: RecordLocation) -> Self {
		Self {
			slab_id: location.slab_id,
			offset: location.offset,
			length: location.length,
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct LsmMultiRowSourceFrame {
	frame: Vec<u8>,
	entries: Vec<MultiRowIndexEntry>,
}

fn lsm_multirow_index_entry_matches_version(
	entry: &MultiRowIndexEntry,
	version: &VersionEntry,
) -> bool {
	entry.key_identity == version.row_key_id
		&& entry.row_hlc == version.row_hlc
		&& entry.record_row_index == version.record_row_index
		&& entry.is_tombstone == version.location.is_tombstone
}

fn lsm_multirow_index_entry_for_version<'a>(
	entries: &'a [MultiRowIndexEntry],
	version: &VersionEntry,
) -> Option<&'a MultiRowIndexEntry> {
	entries
		.iter()
		.find(|entry| lsm_multirow_index_entry_matches_version(entry, version))
}

fn validate_lsm_multirow_source_entry(
	version: &VersionEntry,
	entry: &MultiRowIndexEntry,
) -> std::io::Result<()> {
	if lsm_multirow_index_entry_matches_version(entry, version) {
		return Ok(());
	}
	Err(Error::new(
		ErrorKind::InvalidData,
		"LSM multi-row source entry does not match planned survivor",
	))
}

fn lsm_compaction_tmp_path(dir: &Path, job_id: CompactionJobId, slab_id: SlabId) -> PathBuf {
	dir.join(format!(
		"compaction-temp-{job_id:016x}-slab_{slab_id:016x}.wnd"
	))
}

fn is_lsm_compaction_tmp_filename(name: &str) -> bool {
	name.starts_with("compaction-temp-")
}

fn secondary_index_sidecar_path(dir: &Path, relative: &str) -> std::io::Result<PathBuf> {
	let relative_path = Path::new(relative);
	if relative_path.is_absolute()
		|| relative_path.components().any(|component| {
			matches!(
				component,
				std::path::Component::ParentDir
					| std::path::Component::RootDir
					| std::path::Component::Prefix(_)
			)
		}) {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!("secondary index sidecar path is not relative-safe: {relative}"),
		));
	}
	Ok(dir.join(relative_path))
}

fn secondary_index_sidecar_tmp_path(path: &Path) -> PathBuf {
	let mut tmp = path.as_os_str().to_os_string();
	tmp.push(".tmp");
	PathBuf::from(tmp)
}

fn parse_mvcc_rewrite_tmp_filename(name: &str) -> Option<SlabId> {
	let rest = name
		.strip_prefix("slab_")?
		.strip_suffix(".wnd.retention.tmp")?;
	if rest.len() != 16 {
		return None;
	}
	u64::from_str_radix(rest, 16).ok().map(SlabId::new)
}

async fn cleanup_retention_rewrite_tmp_files(
	file_ops: &dyn AsyncFileOps,
	dir: &Path,
) -> std::io::Result<()> {
	let mut removed = 0usize;
	for entry in file_ops.read_dir(dir).await? {
		if !entry.is_file {
			continue;
		}
		let Some(name) = entry.file_name.to_str() else {
			continue;
		};
		if parse_mvcc_rewrite_tmp_filename(name).is_none() {
			continue;
		}
		unlink_local_file(file_ops, &dir.join(name)).await?;
		removed = removed.saturating_add(1);
	}
	if removed > 0 {
		tracing::warn!(
			dir = %dir.display(),
			removed_tmp_files = removed,
			"removed stale tiered retention rewrite temp files during open"
		);
	}
	Ok(())
}

async fn cleanup_lsm_compaction_tmp_files(
	file_ops: &dyn AsyncFileOps,
	dir: &Path,
) -> std::io::Result<()> {
	let mut removed = 0usize;
	for entry in file_ops.read_dir(dir).await? {
		if !entry.is_file {
			continue;
		}
		let Some(name) = entry.file_name.to_str() else {
			continue;
		};
		if !is_lsm_compaction_tmp_filename(name) {
			continue;
		}
		unlink_local_file(file_ops, &dir.join(name)).await?;
		removed = removed.saturating_add(1);
	}
	if removed > 0 {
		tracing::warn!(
			dir = %dir.display(),
			removed_tmp_files = removed,
			"removed stale LSM compaction temp files during open"
		);
	}
	Ok(())
}

async fn fsync_parent_dir(file_ops: &dyn AsyncFileOps, path: &Path) -> std::io::Result<()> {
	let parent = path.parent().ok_or_else(|| {
		Error::new(
			ErrorKind::InvalidInput,
			format!("path has no parent directory: {}", path.display()),
		)
	})?;
	file_ops.fsync_dir(parent).await
}

async fn unlink_local_file(file_ops: &dyn AsyncFileOps, path: &Path) -> std::io::Result<()> {
	match file_ops.unlink(path).await {
		Ok(()) => {
			fsync_parent_dir(file_ops, path).await?;
			Ok(())
		}
		Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
		Err(err) => Err(err),
	}
}

/// Parse a filename of the form `slab_<16-hex>.wnd` into a slab id.
/// Returns `None` for anything that does not match so the scan can
/// silently skip unrelated files — operators may reasonably drop
/// things like `.DS_Store` or log files into the directory.
fn parse_slab_filename(name: &str) -> Option<SlabId> {
	let rest = name.strip_prefix("slab_")?.strip_suffix(".wnd")?;
	if rest.len() != 16 {
		return None;
	}
	u64::from_str_radix(rest, 16).ok().map(SlabId::new)
}

/// Enumerate every `slab_<hex>.wnd` file in `dir` and return their
/// ids. Non-matching entries are ignored; I/O errors propagate.
async fn discover_slab_ids(
	file_ops: &dyn AsyncFileOps,
	dir: &Path,
) -> std::io::Result<Vec<SlabId>> {
	let mut ids = Vec::new();
	for entry in file_ops.read_dir(dir).await? {
		if !entry.is_file {
			continue;
		}
		if let Some(name) = entry.file_name.to_str() {
			if let Some(id) = parse_slab_filename(name) {
				ids.push(id);
			}
		}
	}
	Ok(ids)
}

/// Merge every version from a freshly opened slab's local index
/// into the manager's aggregated multi-version index. Called
/// exactly once per slab — at `open` for the first slab and at
/// rotation for every subsequent one (where the drain is trivially
/// empty for a fresh file).
///
/// Per-slab chains are already sorted; we walk them and insert
/// each version via [`insert_version_sorted`] so the aggregated
/// chain stays sorted globally even when older slabs contain
/// versions that post-date newer slabs (a legal outcome of leader
/// changes or cross-slab rewrites).
fn drain_slab_index(
	slab: &LocalSlabStore,
	key_index: &DashMap<TenantKey, Vec<RecordLocation>>,
	recovered_key_localities: &mut Vec<(TenantId, Identity, Locality)>,
) {
	for entry in slab.index.iter() {
		let tenant_key = *entry.key();
		for loc in entry.value().iter() {
			key_index
				.entry(tenant_key)
				.and_modify(|versions| insert_version_sorted(versions, *loc))
				.or_insert_with(|| vec![*loc]);
		}
	}
	recovered_key_localities.extend(
		slab.recovered_localities
			.iter()
			.map(|entry| (entry.key().0, entry.key().1, *entry.value())),
	);
}

fn build_key_localities(
	key_index: &DashMap<TenantKey, Vec<RecordLocation>>,
) -> DashMap<TenantKey, Locality> {
	let key_localities = DashMap::new();
	for entry in key_index.iter() {
		let (tenant_id, key_identity) = *entry.key();
		key_localities.insert((tenant_id, key_identity), Locality::from(key_identity));
	}
	key_localities
}

fn build_locality_index(key_localities: &DashMap<TenantKey, Locality>) -> TenantLocalityMembers {
	let mut locality_index = TenantLocalityMembers::new();
	for entry in key_localities.iter() {
		let (tenant_id, key_identity) = *entry.key();
		locality_index
			.entry(tenant_id)
			.or_default()
			.insert((*entry.value(), key_identity));
	}
	locality_index
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::crypto::CryptoManager;
	use crate::schema::table::{
		ColumnSmallSetSpecV1, ColumnStatsPolicyV1, LocalityPolicyV1, MultiRowLayoutDescriptor,
		PrimaryKeyDescriptorV1, RecordBodyCompressionPolicy, RecordLayoutPolicy, RowIdentityPolicy,
		STORAGE_ROW_HLC_COLUMN, STORAGE_ROW_KEY_ID_COLUMN, STORAGE_ROW_ROUTING_LOCALITY_COLUMN,
		STORAGE_ROW_TABLE_OBJECT_ID_COLUMN, STORAGE_ROW_TOMBSTONE_COLUMN, SchemaEvolutionPolicyV1,
		TableObjectId, TableSchemaCatalogV1, TableSpreadLocalityDescriptorV1,
		default_kv_table_descriptor_v1, encode_table_write_row_count,
	};
	use crate::schema::{
		IndexArrowTypeTag, IndexKeyEncoding, IndexObjectId, NodeId, RetentionWindowMillis,
		RowGroupRowCount, SecondaryIndexCatalog, SecondaryIndexKeyDescriptor,
		TableLifecycleCatalog, TableRetentionPolicy,
	};
	use crate::storage::io_uring::spawn_io_thread;
	use crate::storage::l2::{LocalFsObjectStore, ObjectStore};
	use crate::storage::multirow::pack_record_batch_by_bucket;
	use crate::storage::stats::{
		ARROW_STATS_TAG_INT64, decode_column_stats_payload, encode_signed_stat_value,
		parse_column_stats_block,
	};
	use crate::storage::{IndexQuery, SecondaryIndexEntry, SecondaryIndexIdentity};
	use arrow::array::{
		Array, BooleanArray, FixedSizeBinaryBuilder, Int64Array, LargeBinaryBuilder,
		LargeStringArray, UInt8Array, UInt64Array, UInt64Builder,
	};
	use arrow::datatypes::{DataType, Field, Schema};
	use arrow::record_batch::RecordBatch;

	fn fresh_dir(tag: &str) -> std::path::PathBuf {
		let dir = std::env::temp_dir().join(format!(
			"wendelon-manager-{tag}-{}-{}",
			std::process::id(),
			uuid::Uuid::new_v4()
		));
		std::fs::create_dir_all(&dir).unwrap();
		dir
	}

	fn hlc(ms: u64) -> HybridTimestamp {
		HybridTimestamp {
			physical_ms: ms,
			logical: 0,
			node_id: 1,
		}
	}

	fn id(n: u64) -> Identity {
		let mut bytes = [0u8; 32];
		bytes[..8].copy_from_slice(&n.to_le_bytes());
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

	struct CountingRangeStore {
		bytes: Bytes,
		in_flight: std::sync::atomic::AtomicUsize,
		max_seen: std::sync::atomic::AtomicUsize,
	}

	impl CountingRangeStore {
		fn new(bytes: Bytes) -> Self {
			Self {
				bytes,
				in_flight: std::sync::atomic::AtomicUsize::new(0),
				max_seen: std::sync::atomic::AtomicUsize::new(0),
			}
		}

		fn max_seen(&self) -> usize {
			self.max_seen.load(std::sync::atomic::Ordering::SeqCst)
		}
	}

	#[async_trait::async_trait]
	impl ObjectStore for CountingRangeStore {
		async fn put(&self, _key: &str, _bytes: &[u8]) -> std::io::Result<String> {
			Err(Error::other("put not implemented for CountingRangeStore"))
		}

		async fn head(&self, uri: &str) -> std::io::Result<crate::storage::l2::ObjectMetadata> {
			Ok(crate::storage::l2::ObjectMetadata {
				key: "counting".to_string(),
				uri: uri.to_string(),
				size_bytes: self.bytes.len() as u64,
			})
		}

		async fn get_range(
			&self,
			_uri: &str,
			range: std::ops::Range<u64>,
		) -> std::io::Result<Bytes> {
			let current = self
				.in_flight
				.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
				.saturating_add(1);
			let mut observed = self.max_seen.load(std::sync::atomic::Ordering::SeqCst);
			while current > observed {
				match self.max_seen.compare_exchange(
					observed,
					current,
					std::sync::atomic::Ordering::SeqCst,
					std::sync::atomic::Ordering::SeqCst,
				) {
					Ok(_) => break,
					Err(next) => observed = next,
				}
			}
			tokio::time::sleep(Duration::from_millis(25)).await;
			self.in_flight
				.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
			let start = usize::try_from(range.start)
				.map_err(|_| Error::new(ErrorKind::InvalidInput, "range start exceeds usize"))?;
			let end = usize::try_from(range.end)
				.map_err(|_| Error::new(ErrorKind::InvalidInput, "range end exceeds usize"))?;
			if start > end || end > self.bytes.len() {
				return Err(Error::new(ErrorKind::InvalidInput, "range out of bounds"));
			}
			Ok(self.bytes.slice(start..end))
		}

		async fn get_bytes(&self, _uri: &str) -> std::io::Result<Bytes> {
			Ok(self.bytes.clone())
		}

		async fn list(
			&self,
			_prefix: &str,
		) -> std::io::Result<Vec<crate::storage::l2::ObjectMetadata>> {
			Ok(Vec::new())
		}

		async fn delete(&self, _uri: &str) -> std::io::Result<()> {
			Ok(())
		}
	}

	#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
	struct ObjectStoreCallSnapshot {
		get_range_calls: usize,
		get_range_bytes: u64,
		get_bytes_calls: usize,
		get_bytes_bytes: u64,
	}

	struct CountingObjectStore {
		inner: Arc<dyn ObjectStore>,
		get_range_calls: std::sync::atomic::AtomicUsize,
		get_range_bytes: std::sync::atomic::AtomicU64,
		get_bytes_calls: std::sync::atomic::AtomicUsize,
		get_bytes_bytes: std::sync::atomic::AtomicU64,
	}

	impl CountingObjectStore {
		fn new(inner: Arc<dyn ObjectStore>) -> Self {
			Self {
				inner,
				get_range_calls: std::sync::atomic::AtomicUsize::new(0),
				get_range_bytes: std::sync::atomic::AtomicU64::new(0),
				get_bytes_calls: std::sync::atomic::AtomicUsize::new(0),
				get_bytes_bytes: std::sync::atomic::AtomicU64::new(0),
			}
		}

		fn snapshot(&self) -> ObjectStoreCallSnapshot {
			ObjectStoreCallSnapshot {
				get_range_calls: self
					.get_range_calls
					.load(std::sync::atomic::Ordering::SeqCst),
				get_range_bytes: self
					.get_range_bytes
					.load(std::sync::atomic::Ordering::SeqCst),
				get_bytes_calls: self
					.get_bytes_calls
					.load(std::sync::atomic::Ordering::SeqCst),
				get_bytes_bytes: self
					.get_bytes_bytes
					.load(std::sync::atomic::Ordering::SeqCst),
			}
		}
	}

	#[async_trait::async_trait]
	impl ObjectStore for CountingObjectStore {
		async fn put(&self, key: &str, bytes: &[u8]) -> std::io::Result<String> {
			self.inner.put(key, bytes).await
		}

		async fn head(&self, uri: &str) -> std::io::Result<crate::storage::l2::ObjectMetadata> {
			self.inner.head(uri).await
		}

		async fn get_range(
			&self,
			uri: &str,
			range: std::ops::Range<u64>,
		) -> std::io::Result<Bytes> {
			self.get_range_calls
				.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
			self.get_range_bytes.fetch_add(
				range.end.saturating_sub(range.start),
				std::sync::atomic::Ordering::SeqCst,
			);
			self.inner.get_range(uri, range).await
		}

		async fn get_bytes(&self, uri: &str) -> std::io::Result<Bytes> {
			let bytes = self.inner.get_bytes(uri).await?;
			self.get_bytes_calls
				.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
			self.get_bytes_bytes
				.fetch_add(bytes.len() as u64, std::sync::atomic::Ordering::SeqCst);
			Ok(bytes)
		}

		async fn list(
			&self,
			prefix: &str,
		) -> std::io::Result<Vec<crate::storage::l2::ObjectMetadata>> {
			self.inner.list(prefix).await
		}

		async fn delete(&self, uri: &str) -> std::io::Result<()> {
			self.inner.delete(uri).await
		}
	}

	#[tokio::test]
	async fn bounded_l2_range_fetch_preserves_order_and_parallel_cap() {
		let store = Arc::new(CountingRangeStore::new(Bytes::from_static(
			b"abcdefghijklmnopqrstuvwxyz",
		)));
		let ranges = vec![0..2, 2..5, 5..9, 9..14, 14..20, 20..26];

		let fetched =
			fetch_l2_ranges_bounded(store.clone(), "counting://object".to_string(), ranges, 2)
				.await
				.unwrap();

		assert_eq!(
			fetched.iter().map(Bytes::as_ref).collect::<Vec<_>>(),
			vec![
				b"ab".as_slice(),
				b"cde".as_slice(),
				b"fghi".as_slice(),
				b"jklmn".as_slice(),
				b"opqrst".as_slice(),
				b"uvwxyz".as_slice(),
			]
		);
		assert_eq!(store.max_seen(), 2);
	}

	fn encode_test_batch(batch: &RecordBatch) -> Vec<u8> {
		crate::storage::ipc::encode_record_batch_bytes(
			batch,
			RecordBodyCompressionPolicy::Uncompressed,
		)
		.unwrap()
	}

	fn test_hlc_storage_bytes(hlc: HybridTimestamp) -> [u8; 16] {
		let mut bytes = [0_u8; 16];
		bytes[..8].copy_from_slice(&hlc.physical_ms.to_le_bytes());
		bytes[8..12].copy_from_slice(&hlc.logical.to_le_bytes());
		bytes[12..16].copy_from_slice(&hlc.node_id.to_le_bytes());
		bytes
	}

	fn encode_test_multirow_storage_frame(
		rows: &[(Identity, Locality, HybridTimestamp, u64)],
	) -> Vec<u8> {
		let rows = rows
			.iter()
			.map(|(identity, locality, hlc, value)| (*identity, *locality, *hlc, *value, false))
			.collect::<Vec<_>>();
		encode_test_multirow_storage_frame_with_tombstones(&rows)
	}

	fn encode_test_multirow_storage_frame_with_tombstones(
		rows: &[(Identity, Locality, HybridTimestamp, u64, bool)],
	) -> Vec<u8> {
		let mut row_hlc = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
		let mut row_identity = FixedSizeBinaryBuilder::with_capacity(rows.len(), 32);
		let mut row_locality = UInt64Builder::with_capacity(rows.len());
		for (identity, locality, hlc, _, _) in rows {
			row_hlc.append_value(test_hlc_storage_bytes(*hlc)).unwrap();
			row_identity.append_value(identity.as_bytes()).unwrap();
			row_locality.append_value(locality.get());
		}
		let row_tombstone = BooleanArray::from(
			rows.iter()
				.map(|(_, _, _, _, value)| *value)
				.collect::<Vec<_>>(),
		);
		let schema = Arc::new(Schema::new(vec![
			Field::new("value", DataType::UInt64, false),
			Field::new(STORAGE_ROW_HLC_COLUMN, DataType::FixedSizeBinary(16), false),
			Field::new(
				STORAGE_ROW_KEY_ID_COLUMN,
				DataType::FixedSizeBinary(32),
				false,
			),
			Field::new(STORAGE_ROW_ROUTING_LOCALITY_COLUMN, DataType::UInt64, false),
			Field::new(STORAGE_ROW_TOMBSTONE_COLUMN, DataType::Boolean, false),
		]));
		let batch = RecordBatch::try_new(
			schema,
			vec![
				Arc::new(UInt64Array::from(
					rows.iter()
						.map(|(_, _, _, value, _)| *value)
						.collect::<Vec<_>>(),
				)) as Arc<dyn Array>,
				Arc::new(row_hlc.finish()) as Arc<dyn Array>,
				Arc::new(row_identity.finish()) as Arc<dyn Array>,
				Arc::new(row_locality.finish()) as Arc<dyn Array>,
				Arc::new(row_tombstone) as Arc<dyn Array>,
			],
		)
		.unwrap();
		encode_test_batch(&batch)
	}

	fn encode_test_append_only_storage_frame(
		table_object_id: TableObjectId,
		rows: &[(Identity, Locality, HybridTimestamp, u64, bool)],
	) -> Vec<u8> {
		let mut row_hlc = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
		let mut row_identity = FixedSizeBinaryBuilder::with_capacity(rows.len(), 32);
		let mut row_locality = UInt64Builder::with_capacity(rows.len());
		let mut row_table_object_id = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
		for (identity, locality, hlc, _, _) in rows {
			row_hlc.append_value(test_hlc_storage_bytes(*hlc)).unwrap();
			row_identity.append_value(identity.as_bytes()).unwrap();
			row_locality.append_value(locality.get());
			row_table_object_id
				.append_value(table_object_id.as_bytes())
				.unwrap();
		}
		let row_tombstone = BooleanArray::from(
			rows.iter()
				.map(|(_, _, _, _, value)| *value)
				.collect::<Vec<_>>(),
		);
		let schema = Arc::new(Schema::new(vec![
			Field::new("value", DataType::UInt64, false),
			Field::new(STORAGE_ROW_HLC_COLUMN, DataType::FixedSizeBinary(16), false),
			Field::new(
				STORAGE_ROW_KEY_ID_COLUMN,
				DataType::FixedSizeBinary(32),
				false,
			),
			Field::new(STORAGE_ROW_ROUTING_LOCALITY_COLUMN, DataType::UInt64, false),
			Field::new(
				STORAGE_ROW_TABLE_OBJECT_ID_COLUMN,
				DataType::FixedSizeBinary(16),
				false,
			),
			Field::new(STORAGE_ROW_TOMBSTONE_COLUMN, DataType::Boolean, false),
		]));
		let batch = RecordBatch::try_new(
			schema,
			vec![
				Arc::new(UInt64Array::from(
					rows.iter()
						.map(|(_, _, _, value, _)| *value)
						.collect::<Vec<_>>(),
				)) as Arc<dyn Array>,
				Arc::new(row_hlc.finish()) as Arc<dyn Array>,
				Arc::new(row_identity.finish()) as Arc<dyn Array>,
				Arc::new(row_locality.finish()) as Arc<dyn Array>,
				Arc::new(row_table_object_id.finish()) as Arc<dyn Array>,
				Arc::new(row_tombstone) as Arc<dyn Array>,
			],
		)
		.unwrap();
		encode_test_batch(&batch)
	}

	fn encode_test_pk_tombstone_marker_frame(
		descriptor: &TableSchemaDescriptorV1,
		key_identity: Identity,
		routing_locality: Locality,
		row_hlc: HybridTimestamp,
	) -> (Vec<u8>, Vec<MultiRowIndexEntry>) {
		let RecordLayoutPolicy::MultiRow(layout) = descriptor.record_layout_policy else {
			panic!("pk tombstone marker test requires multi-row layout")
		};
		let rows = vec![MultiRowSourceRow {
			source_row_index: 0,
			key_identity,
			routing_locality,
			row_hlc,
			is_tombstone: true,
			table_object_id: Some(descriptor.table_object_id),
		}];
		let mut packed = crate::storage::multirow::pack_tombstone_multirow_source_rows_with_policy(
			layout,
			&rows,
			descriptor.record_body_compression_policy,
		)
		.unwrap();
		assert_eq!(packed.len(), 1);
		let record = packed.remove(0);
		let entries = record
			.encoded
			.rows
			.iter()
			.copied()
			.map(Into::into)
			.collect::<Vec<_>>();
		(record.encoded.frame, entries)
	}

	fn encode_test_row_tombstone_marker_frame(
		table_object_id: TableObjectId,
		key_identity: Identity,
		routing_locality: Locality,
		row_hlc: HybridTimestamp,
	) -> (Vec<u8>, Vec<MultiRowIndexEntry>) {
		let layout = MultiRowLayoutDescriptor::new(4, 8, 1024 * 1024).unwrap();
		let rows = vec![MultiRowSourceRow {
			source_row_index: 0,
			key_identity,
			routing_locality,
			row_hlc,
			is_tombstone: true,
			table_object_id: Some(table_object_id),
		}];
		let mut packed = crate::storage::multirow::pack_tombstone_multirow_source_rows_with_policy(
			layout,
			&rows,
			RecordBodyCompressionPolicy::Uncompressed,
		)
		.unwrap();
		assert_eq!(packed.len(), 1);
		let record = packed.remove(0);
		let entries = record
			.encoded
			.rows
			.iter()
			.copied()
			.map(Into::into)
			.collect::<Vec<_>>();
		(record.encoded.frame, entries)
	}

	fn append_only_retention_descriptor(table_object_id: TableObjectId) -> TableSchemaDescriptorV1 {
		let schema = Schema::new(vec![Field::new("value", DataType::UInt64, false)]);
		TableSchemaDescriptorV1::new_with_layout(
			table_object_id,
			1,
			"analytics",
			"append_events",
			&schema,
			RowIdentityPolicy::AppendOnly,
			LocalityPolicyV1::default_table_spread(),
			RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::new(4, 8, 1024 * 1024).unwrap()),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.unwrap()
	}

	fn pk_retention_descriptor(table_object_id: TableObjectId) -> TableSchemaDescriptorV1 {
		let schema = Schema::new(vec![Field::new("value", DataType::UInt64, false)]);
		TableSchemaDescriptorV1::new_with_layout(
			table_object_id,
			1,
			"analytics",
			"pk_events",
			&schema,
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("value")),
			LocalityPolicyV1::default_table_spread(),
			RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::new(4, 8, 1024 * 1024).unwrap()),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.unwrap()
	}

	fn fixed_table_retention_catalog(
		descriptor: &TableSchemaDescriptorV1,
		window_ms: u64,
	) -> (TableSchemaCatalogV1, TableLifecycleCatalog) {
		let mut schemas = TableSchemaCatalogV1::default();
		schemas.upsert(tenant(), descriptor.clone()).unwrap();
		let mut lifecycle = TableLifecycleCatalog::default();
		lifecycle
			.set_next_generation(
				tenant(),
				descriptor.table_object_id,
				crate::schema::PolicyGeneration::ZERO,
				TableRetentionPolicy::FixedWindow(
					RetentionWindowMillis::try_new(window_ms).unwrap(),
				),
				hlc(1),
			)
			.unwrap();
		(schemas, lifecycle)
	}

	fn layout_recluster_descriptor(
		object_byte: u8,
		generation: u64,
		layout: RecordLayoutPolicy,
	) -> TableSchemaDescriptorV1 {
		let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
		let mut descriptor = TableSchemaDescriptorV1::new(
			TableObjectId::new([object_byte; 16]),
			generation,
			"analytics",
			"events",
			&schema,
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
			LocalityPolicyV1::TableSpreadProjection(TableSpreadLocalityDescriptorV1 {
				spread_factor: 1,
				prefix_bits: 1,
			}),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.unwrap();
		descriptor.record_layout_policy = layout;
		descriptor.record_body_compression_policy =
			RecordBodyCompressionPolicy::default_for_layout(&descriptor.record_layout_policy);
		descriptor.refresh_descriptor_fingerprints().unwrap();
		descriptor
	}

	fn layout_recluster_descriptor_with_codec(
		object_byte: u8,
		generation: u64,
		layout: RecordLayoutPolicy,
		compression_policy: RecordBodyCompressionPolicy,
	) -> TableSchemaDescriptorV1 {
		let mut descriptor = layout_recluster_descriptor(object_byte, generation, layout);
		descriptor.record_body_compression_policy = compression_policy;
		descriptor.refresh_descriptor_fingerprints().unwrap();
		descriptor
	}

	fn layout_recluster_batch(values: &[i64]) -> RecordBatch {
		RecordBatch::try_new(
			Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
			vec![Arc::new(Int64Array::from(
				values.iter().copied().map(Some).collect::<Vec<_>>(),
			)) as Arc<dyn Array>],
		)
		.unwrap()
	}

	fn layout_recluster_meta(
		descriptor: &TableSchemaDescriptorV1,
		row_count: usize,
	) -> TableWriteRequestMeta {
		let mut meta: TableWriteRequestMeta = bytemuck::Zeroable::zeroed();
		meta.tenant_id = tenant();
		meta.table_object_id = descriptor.table_object_id;
		meta.schema_generation = descriptor.schema_generation;
		meta.row_count = encode_table_write_row_count(row_count).unwrap();
		meta
	}

	fn first_i64(batch: &RecordBatch) -> i64 {
		batch
			.column(0)
			.as_any()
			.downcast_ref::<Int64Array>()
			.unwrap()
			.value(0)
	}

	fn storage_compression_l2_fetch_metric_value(codec: RecordBodyCompressionPolicy) -> u64 {
		let segment = "storage_compression_l2_record_body_bytes_fetched_total";
		let metric = format!("{}{}", "wendelon_", segment);
		let label = format!("{{codec=\"{}\"}}", codec.codec_label());
		metrics()
			.render_prometheus()
			.lines()
			.find_map(|line| {
				if line.starts_with(&metric) && line.contains(&label) {
					line.rsplit_once(' ')
						.and_then(|(_, value)| value.parse::<u64>().ok())
				} else {
					None
				}
			})
			.unwrap_or(0)
	}

	async fn fresh_manager() -> Arc<SlabManager> {
		fresh_manager_with_group_commit(GroupCommitConfig::default()).await
	}

	struct DirectLsmCommitter {
		manager: Arc<SlabManager>,
	}

	#[async_trait]
	impl TierCommitter for DirectLsmCommitter {
		async fn commit_tier_slab(
			&self,
			_tier: crate::consensus::TieredSlab,
		) -> std::io::Result<()> {
			Err(Error::new(
				ErrorKind::Unsupported,
				"tier commits are not used by this test committer",
			))
		}

		async fn retire_tiered_slab(&self, _slab_id: SlabId) -> std::io::Result<()> {
			Err(Error::new(
				ErrorKind::Unsupported,
				"tier retirements are not used by this test committer",
			))
		}

		async fn commit_lsm_compaction(&self, commit: CompactionCommit) -> std::io::Result<()> {
			self.manager
				.apply_lsm_manifest_compaction_commit(&commit)
				.map(|_| ())
				.map_err(Error::other)
		}
	}

	struct FailingLsmCommitter;

	#[async_trait]
	impl TierCommitter for FailingLsmCommitter {
		async fn commit_tier_slab(
			&self,
			_tier: crate::consensus::TieredSlab,
		) -> std::io::Result<()> {
			Err(Error::new(
				ErrorKind::Unsupported,
				"tier commits are not used by this test committer",
			))
		}

		async fn retire_tiered_slab(&self, _slab_id: SlabId) -> std::io::Result<()> {
			Err(Error::new(
				ErrorKind::Unsupported,
				"tier retirements are not used by this test committer",
			))
		}

		async fn commit_lsm_compaction(&self, _commit: CompactionCommit) -> std::io::Result<()> {
			Err(Error::other("injected LSM commit failure"))
		}
	}

	async fn fresh_manager_with_group_commit(group_commit: GroupCommitConfig) -> Arc<SlabManager> {
		let dir = fresh_dir("mv");
		let io = spawn_io_thread(64).unwrap();
		Arc::new(
			SlabManager::open(SlabManagerConfig {
				dir,
				// Large enough that no rotation fires during tests;
				// we want every version in the same active slab so
				// the test targets the multi-version index
				// semantics rather than cross-slab drain behaviour.
				max_slab_bytes: 1024 * 1024,
				io,
				l2: None,
				rehydration: RehydrationPolicy::default(),
				l2_column_fetch: Default::default(),
				row_group_writer: Default::default(),
				l2_row_group_fetch: Default::default(),
				encoding_writer: Default::default(),
				l2_chunk_layout: Default::default(),
				l2_encoding_fetch: Default::default(),
				encoding_compute: Default::default(),
				group_commit,
				crypto: test_crypto(),
			})
			.await
			.unwrap(),
		)
	}

	async fn open_manager_at_dir(
		dir: std::path::PathBuf,
		io: crate::storage::io_uring::IoRingHandle,
		crypto: Arc<CryptoManager>,
	) -> Arc<SlabManager> {
		Arc::new(
			SlabManager::open(SlabManagerConfig {
				dir,
				max_slab_bytes: 1024 * 1024,
				io,
				l2: None,
				rehydration: RehydrationPolicy::default(),
				l2_column_fetch: Default::default(),
				row_group_writer: Default::default(),
				l2_row_group_fetch: Default::default(),
				encoding_writer: Default::default(),
				l2_chunk_layout: Default::default(),
				l2_encoding_fetch: Default::default(),
				encoding_compute: Default::default(),
				group_commit: GroupCommitConfig::default(),
				crypto,
			})
			.await
			.unwrap(),
		)
	}

	async fn force_seal_active(manager: &Arc<SlabManager>) -> SlabId {
		let mut state = manager.state.lock().await;
		let sealed = state.active.clone();
		sealed.fsync().await.unwrap();
		let sealed_id = sealed.slab_id();
		let next_id = state.next_slab_id;
		let next = Arc::new(
			LocalSlabStore::open(
				slab_path(&manager.config.dir, next_id),
				next_id,
				manager.file_ops.clone(),
				manager.config.group_commit.clone(),
			)
			.await
			.unwrap(),
		);
		state.active = next;
		state.sealed.insert(sealed_id, sealed);
		state.next_slab_id = next_id.saturating_add(1);
		sealed_id
	}

	fn secondary_index_table_descriptor_for_test() -> TableSchemaDescriptorV1 {
		let table_object_id = TableObjectId::new([0x52; 16]);
		let schema = Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("age", DataType::Int64, true),
		]);
		TableSchemaDescriptorV1::new_with_layout(
			table_object_id,
			1,
			"analytics",
			"events",
			&schema,
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
			LocalityPolicyV1::default_table_spread(),
			RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::new(4, 8, 1024 * 1024).unwrap()),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.unwrap()
	}

	fn secondary_index_descriptor_for_test() -> SecondaryIndexDescriptor {
		let table = secondary_index_table_descriptor_for_test();
		SecondaryIndexDescriptor {
			index_object_id: IndexObjectId::new([0x9A; 16]),
			table_object_id: table.table_object_id,
			namespace: table.namespace.clone(),
			index_name: "events_age_idx".to_owned(),
			table_schema_generation: table.schema_generation,
			logical_schema_fingerprint: table.logical_schema_fingerprint,
			key: SecondaryIndexKeyDescriptor {
				column_name: "age".to_owned(),
				column_ordinal: 1,
				arrow_type_tag: IndexArrowTypeTag::Int64,
				encoding: IndexKeyEncoding::OrderedScalar,
			},
		}
	}

	fn secondary_index_encoded_i64_for_test(value: i64) -> Vec<u8> {
		((value as u64) ^ 0x8000_0000_0000_0000)
			.to_be_bytes()
			.to_vec()
	}

	fn secondary_index_storage_batch_for_test(
		ages: Vec<Option<i64>>,
		row_hlc: HybridTimestamp,
		identity_byte: u8,
	) -> RecordBatch {
		let ids = Int64Array::from_iter_values(0..ages.len() as i64);
		let ages = Int64Array::from(ages);
		let batch = RecordBatch::try_new(
			Arc::new(Schema::new(vec![
				Field::new("id", DataType::Int64, false),
				Field::new("age", DataType::Int64, true),
			])),
			vec![Arc::new(ids), Arc::new(ages)],
		)
		.unwrap();
		crate::storage::append_storage_synthetic_columns(
			&batch,
			&(0..batch.num_rows()).collect::<Vec<_>>(),
			&crate::schema::TableWriteAdmission {
				canonical_primary_keys: vec![vec![identity_byte]; batch.num_rows()],
				key_identities: vec![Identity::new([identity_byte; 32]); batch.num_rows()],
				localities: vec![Locality::new(11); batch.num_rows()],
			},
			row_hlc,
			Some(TableObjectId::new([0x52; 16])),
		)
		.unwrap()
	}

	fn secondary_index_user_batch_for_test(id_value: i64, age: Option<i64>) -> RecordBatch {
		RecordBatch::try_new(
			Arc::new(Schema::new(vec![
				Field::new("id", DataType::Int64, false),
				Field::new("age", DataType::Int64, true),
			])),
			vec![
				Arc::new(Int64Array::from(vec![id_value])) as Arc<dyn Array>,
				Arc::new(Int64Array::from(vec![age])) as Arc<dyn Array>,
			],
		)
		.unwrap()
	}

	async fn append_secondary_index_table_row_for_test(
		manager: &Arc<SlabManager>,
		table: &TableSchemaDescriptorV1,
		id_value: i64,
		age: Option<i64>,
		row_hlc: HybridTimestamp,
	) -> Identity {
		let batch = secondary_index_user_batch_for_test(id_value, age);
		let meta = layout_recluster_meta(table, batch.num_rows());
		let packed =
			pack_record_batch_by_bucket(&meta, table, &batch, WriterNodeId::ZERO, row_hlc).unwrap();
		assert_eq!(packed.len(), 1);
		let record = &packed[0];
		let entries = storage_multirow_index_entries_from_frame(&record.frame_bytes)
			.unwrap()
			.unwrap();
		let row_identity = entries[0].key_identity;
		manager
			.append_durable_multirow_with_locality(tenant(), &record.frame_bytes, &entries)
			.await
			.unwrap();
		row_identity
	}

	fn secondary_index_segment_for_test(source_slab_id: SlabId) -> SecondaryIndexSegment {
		let descriptor = secondary_index_descriptor_for_test();
		SecondaryIndexSegment::build(
			SecondaryIndexIdentity {
				tenant_id: tenant(),
				table_object_id: descriptor.table_object_id,
				index_object_id: descriptor.index_object_id,
				logical_schema_fingerprint: descriptor.logical_schema_fingerprint,
				column_ordinal: 1,
			},
			source_slab_id,
			descriptor.table_schema_generation,
			IndexArrowTypeTag::Int64,
			vec![SecondaryIndexEntry {
				encoded_key: ((42_i64 as u64) ^ 0x8000_0000_0000_0000)
					.to_be_bytes()
					.to_vec(),
				row_key_id: id(42),
				row_hlc: hlc(42),
				record_location: RecordLocation {
					slab_id: source_slab_id,
					offset: 128,
					length: 256,
					hlc: hlc(42),
					record_row_index: RecordRowIndex::ZERO,
					is_tombstone: false,
				},
			}],
		)
		.unwrap()
	}

	#[tokio::test]
	async fn secondary_index_segment_catalog_persists_loads_and_deletes() {
		let dir = fresh_dir("secondary-index-sidecar");
		let io = spawn_io_thread(64).unwrap();
		let crypto = test_crypto();
		let manager = open_manager_at_dir(dir.clone(), io, crypto.clone()).await;
		let segment = secondary_index_segment_for_test(SlabId::new(41));
		let info = manager
			.persist_secondary_index_segment(&segment)
			.await
			.unwrap();

		let listed = manager.list_secondary_index_segments(
			info.tenant_id,
			info.table_object_id,
			info.index_object_id,
			info.logical_schema_fingerprint,
		);
		assert_eq!(listed, vec![info.clone()]);
		let path = dir.join(&info.segment_path);
		assert!(path.exists());
		assert_eq!(
			manager
				.load_secondary_index_segment(&info)
				.await
				.unwrap()
				.unwrap(),
			segment
		);
		let coverage = manager
			.secondary_index_coverage_snapshot(info.coverage_key())
			.unwrap();
		assert_eq!(coverage.covered_slabs, vec![info.source_slab_id]);
		assert!(coverage.uncovered_slabs.is_empty());

		drop(manager);
		let reopened = open_manager_at_dir(dir.clone(), spawn_io_thread(64).unwrap(), crypto).await;
		assert_eq!(
			reopened.list_secondary_index_segments(
				info.tenant_id,
				info.table_object_id,
				info.index_object_id,
				info.logical_schema_fingerprint,
			),
			vec![info.clone()]
		);
		assert_eq!(
			reopened
				.load_secondary_index_segment(&info)
				.await
				.unwrap()
				.unwrap(),
			segment
		);
		let coverage = reopened
			.secondary_index_coverage_snapshot(info.coverage_key())
			.unwrap();
		assert_eq!(coverage.covered_slabs, vec![info.source_slab_id]);
		assert!(
			reopened
				.delete_secondary_index_segment(&info)
				.await
				.unwrap()
		);
		assert!(!path.exists());
		assert!(
			reopened
				.list_secondary_index_segments(
					info.tenant_id,
					info.table_object_id,
					info.index_object_id,
					info.logical_schema_fingerprint,
				)
				.is_empty()
		);
		let coverage = reopened
			.secondary_index_coverage_snapshot(info.coverage_key())
			.unwrap();
		assert!(coverage.covered_slabs.is_empty());
		assert_eq!(coverage.uncovered_slabs[0].0, info.source_slab_id);
		assert_eq!(
			coverage.uncovered_slabs[0].1.reason,
			SecondaryIndexBackfillErrorReason::MissingSegment
		);
	}

	#[tokio::test]
	async fn secondary_index_corrupt_sidecar_marks_uncovered_and_fails_closed() {
		let dir = fresh_dir("secondary-index-corrupt-sidecar");
		let io = spawn_io_thread(64).unwrap();
		let crypto = test_crypto();
		let manager = open_manager_at_dir(dir.clone(), io, crypto).await;
		let segment = secondary_index_segment_for_test(SlabId::new(41));
		let info = manager
			.persist_secondary_index_segment(&segment)
			.await
			.unwrap();
		std::fs::write(
			dir.join(&info.segment_path),
			b"not a secondary index segment",
		)
		.unwrap();

		let err = manager
			.load_secondary_index_segment(&info)
			.await
			.unwrap_err();
		assert!(err.to_string().contains("decode secondary index segment"));
		let coverage = manager
			.secondary_index_coverage_snapshot(info.coverage_key())
			.unwrap();
		assert!(coverage.covered_slabs.is_empty());
		assert_eq!(coverage.uncovered_slabs[0].0, info.source_slab_id);
		assert_eq!(
			coverage.uncovered_slabs[0].1.reason,
			SecondaryIndexBackfillErrorReason::CorruptSegment
		);
	}

	#[tokio::test]
	async fn secondary_index_drop_gc_deletes_sidecars_and_clears_local_coverage() {
		let dir = fresh_dir("secondary-index-drop-gc");
		let io = spawn_io_thread(64).unwrap();
		let crypto = test_crypto();
		let manager = open_manager_at_dir(dir.clone(), io, crypto).await;
		let descriptor = secondary_index_descriptor_for_test();
		let first = manager
			.persist_secondary_index_segment(&secondary_index_segment_for_test(SlabId::new(41)))
			.await
			.unwrap();
		let second = manager
			.persist_secondary_index_segment(&secondary_index_segment_for_test(SlabId::new(42)))
			.await
			.unwrap();

		let removed = manager
			.gc_secondary_index_segments_for_dropping_index(tenant(), &descriptor)
			.await
			.unwrap();

		assert_eq!(removed, 2);
		assert!(!dir.join(&first.segment_path).exists());
		assert!(!dir.join(&second.segment_path).exists());
		assert!(
			manager
				.list_secondary_index_segments(
					tenant(),
					descriptor.table_object_id,
					descriptor.index_object_id,
					descriptor.logical_schema_fingerprint,
				)
				.is_empty()
		);
		assert!(
			manager
				.secondary_index_coverage_snapshot(first.coverage_key())
				.is_none()
		);
	}

	#[tokio::test]
	async fn secondary_index_source_slab_read_lease_blocks_compaction_claim() {
		let manager = fresh_manager().await;
		let source_slab_id = force_seal_active(&manager).await;
		let source_slab_lease = manager
			.acquire_secondary_index_source_slab_read_lease(source_slab_id)
			.await
			.unwrap();

		let blocked = tokio::time::timeout(
			Duration::from_millis(20),
			manager.acquire_secondary_index_source_slab_compaction_lease(source_slab_id),
		)
		.await;
		assert!(
			blocked.is_err(),
			"compaction claim should wait while backfill holds the source_slab_lease"
		);

		drop(source_slab_lease);
		let compaction_lease = tokio::time::timeout(
			Duration::from_secs(1),
			manager.acquire_secondary_index_source_slab_compaction_lease(source_slab_id),
		)
		.await
		.expect("compaction lease should become available after backfill releases");
		drop(compaction_lease);
	}

	#[tokio::test]
	async fn secondary_index_coverage_plan_recovers_covered_and_enqueues_missing_sealed_slabs() {
		let manager = fresh_manager().await;
		let first_sealed = force_seal_active(&manager).await;
		let second_sealed = force_seal_active(&manager).await;
		let descriptor = secondary_index_descriptor_for_test();
		let info = manager
			.persist_secondary_index_segment(&secondary_index_segment_for_test(first_sealed))
			.await
			.unwrap();

		let plan = manager
			.ensure_secondary_index_coverage(tenant(), &descriptor, 4096)
			.await
			.unwrap();

		assert_eq!(plan.snapshot.covered_slabs, vec![info.source_slab_id]);
		assert_eq!(
			plan.snapshot.active_buffer.unwrap().source_slab_id,
			SlabId::new(2)
		);
		assert_eq!(
			plan.candidates,
			vec![SecondaryIndexBackfillCandidate {
				key: info.coverage_key(),
				source_slab_id: second_sealed,
			}]
		);
	}

	#[tokio::test]
	async fn secondary_index_active_buffer_seal_persists_sidecar_and_marks_covered() {
		let manager = fresh_manager().await;
		let descriptor = secondary_index_descriptor_for_test();
		let active_slab_id = manager.active_slab_id().await;
		let row_hlc = hlc(50);
		let base_location = RecordLocation {
			slab_id: active_slab_id,
			offset: 64,
			length: 512,
			hlc: row_hlc,
			record_row_index: RecordRowIndex::ZERO,
			is_tombstone: false,
		};

		let appended = manager
			.append_secondary_index_active_buffer_batch(
				tenant(),
				&descriptor,
				&secondary_index_storage_batch_for_test(vec![Some(42)], row_hlc, 7),
				base_location,
				4096,
			)
			.unwrap();
		assert_eq!(appended, 1);

		let info = manager
			.seal_secondary_index_active_buffer(tenant(), &descriptor)
			.await
			.unwrap()
			.unwrap();
		assert_eq!(info.source_slab_id, active_slab_id);
		assert_eq!(info.entry_count, 1);
		let loaded = manager
			.load_secondary_index_segment(&info)
			.await
			.unwrap()
			.unwrap();
		assert_eq!(loaded.entries.len(), 1);
		assert_eq!(loaded.entries[0].row_hlc, row_hlc);
		let coverage = manager
			.secondary_index_coverage_snapshot(info.coverage_key())
			.unwrap();
		assert_eq!(coverage.covered_slabs, vec![active_slab_id]);
		assert!(coverage.active_buffer.is_none());
	}

	#[tokio::test]
	async fn secondary_index_backfill_source_slab_emits_every_retained_data_version() {
		let manager = fresh_manager().await;
		let table = secondary_index_table_descriptor_for_test();
		let descriptor = secondary_index_descriptor_for_test();
		let row_identity =
			append_secondary_index_table_row_for_test(&manager, &table, 7, Some(42), hlc(10)).await;
		assert_eq!(
			append_secondary_index_table_row_for_test(&manager, &table, 7, Some(43), hlc(20)).await,
			row_identity
		);
		assert_eq!(
			append_secondary_index_table_row_for_test(&manager, &table, 7, None, hlc(30)).await,
			row_identity
		);
		let source_slab_id = force_seal_active(&manager).await;
		let plan = manager
			.ensure_secondary_index_coverage(tenant(), &descriptor, 4096)
			.await
			.unwrap();
		assert_eq!(
			plan.candidates,
			vec![SecondaryIndexBackfillCandidate {
				key: SecondaryIndexCoverageKey::from(SecondaryIndexIdentity::from_descriptor(
					tenant(),
					&descriptor,
				)),
				source_slab_id,
			}]
		);

		let info = manager
			.backfill_secondary_index_candidate(tenant(), &table, &descriptor, &plan.candidates[0])
			.await
			.unwrap();
		assert_eq!(info.source_slab_id, source_slab_id);
		assert_eq!(info.entry_count, 2);
		let segment = manager
			.load_secondary_index_segment(&info)
			.await
			.unwrap()
			.unwrap();
		assert_eq!(segment.entries.len(), 2);
		assert_eq!(segment.entries[0].row_key_id, row_identity);
		assert_eq!(segment.entries[1].row_key_id, row_identity);
		assert_eq!(segment.entries[0].row_hlc, hlc(10));
		assert_eq!(segment.entries[1].row_hlc, hlc(20));
		assert_eq!(
			segment.entries[0].encoded_key,
			((42_i64 as u64) ^ 0x8000_0000_0000_0000)
				.to_be_bytes()
				.to_vec()
		);
		assert_eq!(
			segment.entries[1].encoded_key,
			((43_i64 as u64) ^ 0x8000_0000_0000_0000)
				.to_be_bytes()
				.to_vec()
		);
		let coverage = manager
			.secondary_index_coverage_snapshot(info.coverage_key())
			.unwrap();
		assert_eq!(coverage.covered_slabs, vec![source_slab_id]);
	}

	#[tokio::test]
	async fn secondary_index_backfill_tick_respects_limit_and_reports_completion() {
		let manager = fresh_manager().await;
		let table = secondary_index_table_descriptor_for_test();
		let descriptor = secondary_index_descriptor_for_test();

		append_secondary_index_table_row_for_test(&manager, &table, 1, Some(10), hlc(10)).await;
		let first = force_seal_active(&manager).await;
		append_secondary_index_table_row_for_test(&manager, &table, 2, Some(20), hlc(20)).await;
		let second = force_seal_active(&manager).await;

		let first_tick = manager
			.run_secondary_index_backfill_once(tenant(), &table, &descriptor, 1, 4096)
			.await
			.unwrap();
		assert_eq!(first_tick.before.candidates.len(), 2);
		assert_eq!(first_tick.attempted, 1);
		assert_eq!(first_tick.completed.len(), 1);
		assert!(first_tick.failures.is_empty());
		assert!(!first_tick.historical_complete);
		assert_eq!(first_tick.completed[0].candidate.source_slab_id, first);

		let second_tick = manager
			.run_secondary_index_backfill_once(tenant(), &table, &descriptor, 8, 4096)
			.await
			.unwrap();
		assert_eq!(second_tick.before.candidates.len(), 1);
		assert_eq!(second_tick.attempted, 1);
		assert_eq!(second_tick.completed.len(), 1);
		assert!(second_tick.failures.is_empty());
		assert_eq!(second_tick.completed[0].candidate.source_slab_id, second);
		assert!(second_tick.historical_complete);

		let listed = manager.list_secondary_index_segments(
			tenant(),
			descriptor.table_object_id,
			descriptor.index_object_id,
			descriptor.logical_schema_fingerprint,
		);
		assert_eq!(listed.len(), 2);
	}

	#[tokio::test]
	async fn secondary_index_compaction_rebuilds_building_sidecar_and_heals_uncovered_slab() {
		let manager = fresh_manager().await;
		let table = secondary_index_table_descriptor_for_test();
		let descriptor = secondary_index_descriptor_for_test();
		let row_identity =
			append_secondary_index_table_row_for_test(&manager, &table, 7, Some(10), hlc(10)).await;
		assert_eq!(
			append_secondary_index_table_row_for_test(&manager, &table, 7, Some(20), hlc(20)).await,
			row_identity
		);
		let old_slab_id = force_seal_active(&manager).await;

		let mut schemas = TableSchemaCatalogV1::default();
		schemas.upsert(tenant(), table.clone()).unwrap();
		let lifecycle = TableLifecycleCatalog::default();
		let mut secondary_indexes = SecondaryIndexCatalog::default();
		secondary_indexes
			.create(tenant(), descriptor.clone(), &schemas)
			.unwrap();
		let coverage_key = SecondaryIndexCoverageKey::from(
			SecondaryIndexIdentity::from_descriptor(tenant(), &descriptor),
		);
		manager.mark_secondary_index_backfill_uncovered(
			coverage_key,
			old_slab_id,
			SecondaryIndexBackfillError::new(
				SecondaryIndexBackfillErrorReason::CompactionRace,
				"source slab compacted before backfill completed",
			),
		);

		let report = manager
			.prune_versions_for_retention(
				MvccRetentionPolicy::FixedWindow(Duration::from_millis(60_000)),
				&lifecycle,
				&schemas,
				&secondary_indexes,
				60_025,
				1,
			)
			.await
			.unwrap();

		assert_eq!(report.compacted_slabs, 1);
		assert_eq!(report.pruned_versions, 1);
		let listed = manager.list_secondary_index_segments(
			tenant(),
			descriptor.table_object_id,
			descriptor.index_object_id,
			descriptor.logical_schema_fingerprint,
		);
		assert_eq!(listed.len(), 1);
		let info = &listed[0];
		assert_ne!(info.source_slab_id, old_slab_id);
		assert_eq!(info.entry_count, 1);
		let segment = manager
			.load_secondary_index_segment(info)
			.await
			.unwrap()
			.unwrap();
		assert_eq!(segment.entries.len(), 1);
		assert_eq!(segment.entries[0].row_key_id, row_identity);
		assert_eq!(segment.entries[0].row_hlc, hlc(20));
		assert_eq!(
			segment.entries[0].encoded_key,
			secondary_index_encoded_i64_for_test(20)
		);
		let coverage = manager
			.secondary_index_coverage_snapshot(coverage_key)
			.unwrap();
		assert_eq!(coverage.covered_slabs, vec![info.source_slab_id]);
		assert!(coverage.uncovered_slabs.is_empty());
	}

	#[tokio::test]
	async fn lsm_compaction_rebuilds_secondary_index_sidecar_with_catalog_context() {
		let manager = fresh_manager().await;
		let table = secondary_index_table_descriptor_for_test();
		let descriptor = secondary_index_descriptor_for_test();
		let row_identity =
			append_secondary_index_table_row_for_test(&manager, &table, 7, Some(10), hlc(10)).await;
		let source_slab_id = manager.force_seal_active_for_test().await;

		let mut schemas = TableSchemaCatalogV1::default();
		schemas.upsert(tenant(), table.clone()).unwrap();
		let mut secondary_indexes = SecondaryIndexCatalog::default();
		secondary_indexes
			.create(tenant(), descriptor.clone(), &schemas)
			.unwrap();
		let tick = manager
			.run_secondary_index_backfill_once(tenant(), &table, &descriptor, 8, 4096)
			.await
			.unwrap();
		assert_eq!(tick.completed.len(), 1);
		let old_info = manager
			.list_secondary_index_segments(
				tenant(),
				descriptor.table_object_id,
				descriptor.index_object_id,
				descriptor.logical_schema_fingerprint,
			)
			.into_iter()
			.find(|info| info.source_slab_id == source_slab_id)
			.expect("old source sidecar should exist before LSM compaction");
		assert_eq!(old_info.source_slab_id, source_slab_id);

		let snapshot = manager.compaction_manifest_snapshot();
		let plan = CompactionPlan {
			job_id: CompactionJobId::new(0x5151),
			source_level: CompactionLevelIndex::L0,
			target_level: CompactionLevelIndex::L1,
			manifest_epoch_pre: snapshot.manifest_epoch,
			input_slabs: vec![source_slab_id],
			overlapping_slabs: Vec::new(),
			retention_floor_snapshot: hlc(100),
			compaction_start_hlc: hlc(101),
			dominant_reason: crate::storage::DominantReason::Operator,
		};
		let worker = LsmCompactionWorker::new(
			crate::storage::LsmConfig::default(),
			manager.slab_pin_table(),
		)
		.unwrap();
		let committer = DirectLsmCommitter {
			manager: manager.clone(),
		};

		let outcome = manager
			.run_lsm_compaction_once_via_control_ring_with_context(
				&worker,
				plan,
				&committer,
				Some(SecondaryIndexCompactionContext {
					table_schemas: &schemas,
					secondary_indexes: &secondary_indexes,
				}),
			)
			.await
			.unwrap();
		let output_slab_id = outcome.output_slabs[0].slab_id;

		assert!(
			manager
				.load_secondary_index_segment(&old_info)
				.await
				.unwrap()
				.is_none(),
			"old sidecar should be retired after LSM compaction"
		);
		let listed = manager.list_secondary_index_segments(
			tenant(),
			descriptor.table_object_id,
			descriptor.index_object_id,
			descriptor.logical_schema_fingerprint,
		);
		assert_eq!(listed.len(), 1);
		let info = &listed[0];
		assert_eq!(info.source_slab_id, output_slab_id);
		assert_eq!(info.entry_count, 1);
		let segment = manager
			.load_secondary_index_segment(info)
			.await
			.unwrap()
			.unwrap();
		assert_eq!(segment.entries.len(), 1);
		assert_eq!(segment.entries[0].row_key_id, row_identity);
		assert_eq!(
			segment.entries[0].encoded_key,
			secondary_index_encoded_i64_for_test(10)
		);
		let coverage = manager
			.secondary_index_coverage_snapshot(info.coverage_key())
			.unwrap();
		assert_eq!(coverage.covered_slabs, vec![output_slab_id]);
		assert!(coverage.uncovered_slabs.is_empty());
		assert_eq!(
			manager
				.compaction_manifest_snapshot()
				.run(output_slab_id)
				.unwrap()
				.sidecar_coverage,
			crate::storage::SidecarCoverage::Complete
		);
	}

	#[tokio::test]
	async fn lsm_compaction_commit_failure_does_not_publish_prepared_sidecar() {
		let manager = fresh_manager().await;
		let table = secondary_index_table_descriptor_for_test();
		let descriptor = secondary_index_descriptor_for_test();
		append_secondary_index_table_row_for_test(&manager, &table, 7, Some(10), hlc(10)).await;
		let source_slab_id = manager.force_seal_active_for_test().await;

		let mut schemas = TableSchemaCatalogV1::default();
		schemas.upsert(tenant(), table.clone()).unwrap();
		let mut secondary_indexes = SecondaryIndexCatalog::default();
		secondary_indexes
			.create(tenant(), descriptor.clone(), &schemas)
			.unwrap();
		manager
			.run_secondary_index_backfill_once(tenant(), &table, &descriptor, 8, 4096)
			.await
			.unwrap();

		let snapshot = manager.compaction_manifest_snapshot();
		let plan = CompactionPlan {
			job_id: CompactionJobId::new(0x5154),
			source_level: CompactionLevelIndex::L0,
			target_level: CompactionLevelIndex::L1,
			manifest_epoch_pre: snapshot.manifest_epoch,
			input_slabs: vec![source_slab_id],
			overlapping_slabs: Vec::new(),
			retention_floor_snapshot: hlc(100),
			compaction_start_hlc: hlc(101),
			dominant_reason: crate::storage::DominantReason::Operator,
		};
		let worker = LsmCompactionWorker::new(
			crate::storage::LsmConfig::default(),
			manager.slab_pin_table(),
		)
		.unwrap();

		let err = manager
			.run_lsm_compaction_once_via_control_ring_with_context(
				&worker,
				plan,
				&FailingLsmCommitter,
				Some(SecondaryIndexCompactionContext {
					table_schemas: &schemas,
					secondary_indexes: &secondary_indexes,
				}),
			)
			.await
			.unwrap_err();

		assert!(matches!(err, CompactionFailure::IoError(_)));
		let listed = manager.list_secondary_index_segments(
			tenant(),
			descriptor.table_object_id,
			descriptor.index_object_id,
			descriptor.logical_schema_fingerprint,
		);
		assert_eq!(listed.len(), 1);
		assert_eq!(listed[0].source_slab_id, source_slab_id);
		assert!(
			manager
				.compaction_manifest_snapshot()
				.run(source_slab_id)
				.unwrap()
				.state
				.is_live()
		);
	}

	#[tokio::test]
	async fn lsm_compaction_rejects_secondary_index_sidecar_without_catalog_context() {
		let manager = fresh_manager().await;
		let table = secondary_index_table_descriptor_for_test();
		let descriptor = secondary_index_descriptor_for_test();
		append_secondary_index_table_row_for_test(&manager, &table, 7, Some(10), hlc(10)).await;
		let source_slab_id = manager.force_seal_active_for_test().await;

		manager
			.run_secondary_index_backfill_once(tenant(), &table, &descriptor, 8, 4096)
			.await
			.unwrap();
		let old_info = manager
			.list_secondary_index_segments(
				tenant(),
				descriptor.table_object_id,
				descriptor.index_object_id,
				descriptor.logical_schema_fingerprint,
			)
			.into_iter()
			.find(|info| info.source_slab_id == source_slab_id)
			.expect("old source sidecar should exist before LSM compaction");

		let snapshot = manager.compaction_manifest_snapshot();
		let plan = CompactionPlan {
			job_id: CompactionJobId::new(0x5152),
			source_level: CompactionLevelIndex::L0,
			target_level: CompactionLevelIndex::L1,
			manifest_epoch_pre: snapshot.manifest_epoch,
			input_slabs: vec![source_slab_id],
			overlapping_slabs: Vec::new(),
			retention_floor_snapshot: hlc(100),
			compaction_start_hlc: hlc(101),
			dominant_reason: crate::storage::DominantReason::Operator,
		};
		let worker = LsmCompactionWorker::new(
			crate::storage::LsmConfig::default(),
			manager.slab_pin_table(),
		)
		.unwrap();
		let committer = DirectLsmCommitter {
			manager: manager.clone(),
		};

		let err = manager
			.run_lsm_compaction_once_via_control_ring(&worker, plan, &committer)
			.await
			.unwrap_err();

		assert!(matches!(err, CompactionFailure::SidecarDigestMismatch));
		assert!(
			manager
				.load_secondary_index_segment(&old_info)
				.await
				.unwrap()
				.is_some(),
			"sidecar should remain intact after rejected LSM compaction"
		);
		assert!(
			manager
				.compaction_manifest_snapshot()
				.run(source_slab_id)
				.unwrap()
				.state
				.is_live()
		);
	}

	#[tokio::test]
	async fn lsm_compaction_elides_non_visible_primary_key_versions_with_catalog_context() {
		let manager = fresh_manager().await;
		let table = secondary_index_table_descriptor_for_test();
		let row_identity =
			append_secondary_index_table_row_for_test(&manager, &table, 7, Some(10), hlc(10)).await;
		assert_eq!(
			append_secondary_index_table_row_for_test(&manager, &table, 7, Some(20), hlc(20)).await,
			row_identity
		);
		assert_eq!(
			append_secondary_index_table_row_for_test(&manager, &table, 7, Some(30), hlc(30)).await,
			row_identity
		);
		let source_slab_id = manager.force_seal_active_for_test().await;

		let mut schemas = TableSchemaCatalogV1::default();
		schemas.upsert(tenant(), table).unwrap();
		let secondary_indexes = SecondaryIndexCatalog::default();
		let snapshot = manager.compaction_manifest_snapshot();
		let plan = CompactionPlan {
			job_id: CompactionJobId::new(0x5153),
			source_level: CompactionLevelIndex::L0,
			target_level: CompactionLevelIndex::L1,
			manifest_epoch_pre: snapshot.manifest_epoch,
			input_slabs: vec![source_slab_id],
			overlapping_slabs: Vec::new(),
			retention_floor_snapshot: hlc(25),
			compaction_start_hlc: hlc(101),
			dominant_reason: crate::storage::DominantReason::Operator,
		};
		let worker = LsmCompactionWorker::new(
			crate::storage::LsmConfig::default(),
			manager.slab_pin_table(),
		)
		.unwrap();
		let committer = DirectLsmCommitter {
			manager: manager.clone(),
		};

		let outcome = manager
			.run_lsm_compaction_once_via_control_ring_with_context(
				&worker,
				plan,
				&committer,
				Some(SecondaryIndexCompactionContext {
					table_schemas: &schemas,
					secondary_indexes: &secondary_indexes,
				}),
			)
			.await
			.unwrap();

		assert_eq!(outcome.rows_in, 3);
		assert_eq!(outcome.rows_out, 2);
		assert_eq!(outcome.elided_versions, 1);
		assert!(
			manager
				.lookup_as_of(tenant(), row_identity, hlc(15))
				.is_none()
		);
		assert_eq!(
			manager
				.lookup_as_of(tenant(), row_identity, hlc(25))
				.unwrap()
				.hlc,
			hlc(20)
		);
		assert_eq!(manager.lookup(tenant(), row_identity).unwrap().hlc, hlc(30));
	}

	#[tokio::test]
	async fn secondary_index_lookup_point_equal_revalidates_sidecar_candidate() {
		let manager = fresh_manager().await;
		let table = secondary_index_table_descriptor_for_test();
		let descriptor = secondary_index_descriptor_for_test();
		let first_identity =
			append_secondary_index_table_row_for_test(&manager, &table, 1, Some(10), hlc(10)).await;
		append_secondary_index_table_row_for_test(&manager, &table, 2, Some(20), hlc(20)).await;
		force_seal_active(&manager).await;
		manager
			.run_secondary_index_backfill_once(tenant(), &table, &descriptor, 8, 4096)
			.await
			.unwrap();

		let request = TableLookupByIndexRequest::from_descriptor(
			tenant(),
			&descriptor,
			IndexQuery::PointEqual {
				encoded_key: secondary_index_encoded_i64_for_test(10),
			},
			hlc(99),
		);
		let report = manager
			.table_lookup_by_index(&request, &descriptor, SecondaryIndexState::Ready)
			.await
			.unwrap();
		assert_eq!(report.rows.len(), 1);
		assert_eq!(report.rows[0].row_key_id, first_identity);
		assert_eq!(report.rows[0].row_hlc, hlc(10));
		assert_eq!(report.stats.scanned_segments, 1);
		assert_eq!(report.stats.candidate_count, 1);
		assert_eq!(report.stats.visible_count, 1);
		assert_eq!(report.stats.superseded_count, 0);

		let err = manager
			.table_lookup_by_index(&request, &descriptor, SecondaryIndexState::Building)
			.await
			.unwrap_err();
		assert!(
			err.to_string()
				.contains(INDEX_REJECT_LOGICAL_FINGERPRINT_MISMATCH)
		);
	}

	#[tokio::test]
	async fn secondary_index_lookup_rejects_superseded_candidate_but_honors_as_of() {
		let manager = fresh_manager().await;
		let table = secondary_index_table_descriptor_for_test();
		let descriptor = secondary_index_descriptor_for_test();
		let row_identity =
			append_secondary_index_table_row_for_test(&manager, &table, 7, Some(10), hlc(10)).await;
		force_seal_active(&manager).await;
		manager
			.run_secondary_index_backfill_once(tenant(), &table, &descriptor, 8, 4096)
			.await
			.unwrap();
		assert_eq!(
			append_secondary_index_table_row_for_test(&manager, &table, 7, Some(20), hlc(20)).await,
			row_identity
		);

		let historical_request = TableLookupByIndexRequest::from_descriptor(
			tenant(),
			&descriptor,
			IndexQuery::PointEqual {
				encoded_key: secondary_index_encoded_i64_for_test(10),
			},
			hlc(15),
		);
		let historical = manager
			.table_lookup_by_index(&historical_request, &descriptor, SecondaryIndexState::Ready)
			.await
			.unwrap();
		assert_eq!(historical.rows.len(), 1);
		assert_eq!(historical.rows[0].row_key_id, row_identity);
		assert_eq!(historical.stats.visible_count, 1);

		let current_request = TableLookupByIndexRequest::from_descriptor(
			tenant(),
			&descriptor,
			IndexQuery::PointEqual {
				encoded_key: secondary_index_encoded_i64_for_test(10),
			},
			hlc(25),
		);
		let current = manager
			.table_lookup_by_index(&current_request, &descriptor, SecondaryIndexState::Ready)
			.await
			.unwrap();
		assert!(current.rows.is_empty());
		assert_eq!(current.stats.candidate_count, 1);
		assert_eq!(current.stats.visible_count, 0);
		assert_eq!(current.stats.superseded_count, 1);
	}

	#[tokio::test]
	async fn locality_candidates_recover_after_index_lock_poison() {
		let manager = fresh_manager().await;
		let key = id(0x4242);
		let locality = Locality::from(key);
		manager
			.append_durable(tenant(), key, hlc(10), b"v1")
			.await
			.unwrap();

		let locality_index = &manager.locality_index;
		let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
			let _guard = locality_index.write().unwrap();
			panic!("poison locality index");
		}));

		assert_eq!(
			manager.locality_candidates_for_tenant(tenant(), locality, locality),
			vec![(locality, key)]
		);
	}

	#[tokio::test]
	async fn lookup_returns_latest_version_after_multiple_appends() {
		let mgr = fresh_manager().await;
		let key = id(42);
		mgr.append(tenant(), key, hlc(10), b"v1").await.unwrap();
		mgr.append(tenant(), key, hlc(20), b"v2").await.unwrap();
		mgr.append(tenant(), key, hlc(30), b"v3").await.unwrap();

		let latest = mgr.lookup(tenant(), key).expect("key present");
		assert_eq!(latest.hlc.physical_ms, 30);
	}

	#[tokio::test]
	async fn encrypt_tiered_object_blob_uses_adr022_layout() {
		let mgr = fresh_manager().await;
		mgr.append(tenant(), id(0x1234), hlc(10), b"adr022-row")
			.await
			.unwrap();
		let slab_id = force_seal_active(&mgr).await;
		let plaintext = mgr.read_sealed_raw(slab_id).await.unwrap();
		let encrypted = mgr.encrypt_tiered_object_blob(slab_id, &plaintext).unwrap();
		assert!(encrypted.bytes.starts_with(b"WENDSLAB"));
		let footer = mgr
			.encrypted_tiered_footer_summary(&encrypted.bytes)
			.unwrap()
			.expect("encrypted footer expected");
		assert_eq!(footer.slab_id, slab_id);
		assert_eq!(footer.tenant_id, tenant());
	}

	#[tokio::test]
	async fn encrypt_tiered_object_blob_with_table_stats_builds_decryptable_csb() {
		let mgr = fresh_manager().await;
		let schema = Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("region", DataType::LargeUtf8, true),
		]);
		let mut descriptor = TableSchemaDescriptorV1::new(
			TableObjectId::new([0x39; 16]),
			3,
			"analytics",
			"events",
			&schema,
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
			LocalityPolicyV1::default_table_spread(),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.unwrap();
		descriptor.stats_policy = ColumnStatsPolicyV1 {
			small_sets: vec![ColumnSmallSetSpecV1::new("region")],
			..ColumnStatsPolicyV1::default()
		};
		descriptor.refresh_descriptor_fingerprints().unwrap();
		let batch = RecordBatch::try_new(
			Arc::new(schema),
			vec![
				Arc::new(Int64Array::from(vec![Some(9), Some(1)])) as Arc<dyn Array>,
				Arc::new(LargeStringArray::from(vec![Some("us"), Some("eu")])) as Arc<dyn Array>,
			],
		)
		.unwrap();
		let mut meta: TableWriteRequestMeta = bytemuck::Zeroable::zeroed();
		meta.tenant_id = tenant();
		meta.table_object_id = descriptor.table_object_id;
		meta.schema_generation = descriptor.schema_generation;
		meta.row_count = encode_table_write_row_count(batch.num_rows()).unwrap();
		let admission = validate_table_write_request_batch(
			&meta,
			&batch,
			&descriptor,
			WriterNodeId::ZERO,
			meta.client_hlc,
		)
		.unwrap();
		for row in 0..batch.num_rows() {
			let row_batch = batch.slice(row, 1);
			let frame = encode_test_batch(&row_batch);
			mgr.append_durable_with_locality(
				tenant(),
				admission.key_identities[row],
				admission.localities[row],
				hlc(10 + row as u64),
				&frame,
			)
			.await
			.unwrap();
		}
		let slab_id = force_seal_active(&mgr).await;
		let raw = mgr.read_sealed_raw(slab_id).await.unwrap();
		let mut catalog = TableSchemaCatalogV1::default();
		catalog.upsert(tenant(), descriptor.clone()).unwrap();

		let encrypted = mgr
			.encrypt_tiered_object_blob_with_table_stats(slab_id, &raw, &catalog)
			.unwrap();
		let locator = encrypted.stats_locator.expect("stats locator");
		assert_eq!(locator.stats_column_count, 2);
		assert!(locator.has_small_set());
		let stats = mgr
			.config
			.crypto
			.decrypt_tiered_stats_block(slab_id, &encrypted.bytes, descriptor.schema_generation)
			.unwrap()
			.expect("stats block");
		let parsed = parse_column_stats_block(&stats.plaintext).unwrap();
		assert_eq!(parsed.header.table_object_id, descriptor.table_object_id);
		assert_eq!(
			parsed.header.schema_generation,
			descriptor.schema_generation
		);
		assert_eq!(parsed.header.row_count, 2);
		assert_eq!(parsed.columns[0].arrow_type_tag, ARROW_STATS_TAG_INT64);
		let id_stats = decode_column_stats_payload(&parsed.columns[0]).unwrap();
		assert_eq!(id_stats.min, Some(encode_signed_stat_value(1, 8).unwrap()));
		assert_eq!(id_stats.max, Some(encode_signed_stat_value(9, 8).unwrap()));
		let region_stats = decode_column_stats_payload(&parsed.columns[1]).unwrap();
		assert_eq!(
			region_stats.small_set,
			Some(vec![b"eu".to_vec(), b"us".to_vec()])
		);
	}

	#[tokio::test]
	async fn table_stats_are_stable_across_record_body_compression() {
		let schema = Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("region", DataType::LargeUtf8, true),
		]));
		let mut descriptor = TableSchemaDescriptorV1::new(
			TableObjectId::new([0x43; 16]),
			7,
			"analytics",
			"events",
			schema.as_ref(),
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
			LocalityPolicyV1::default_table_spread(),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.unwrap();
		descriptor.record_body_compression_policy = RecordBodyCompressionPolicy::Lz4Frame;
		descriptor.stats_policy = ColumnStatsPolicyV1 {
			small_sets: vec![ColumnSmallSetSpecV1::new("region")],
			..ColumnStatsPolicyV1::default()
		};
		descriptor.refresh_descriptor_fingerprints().unwrap();
		descriptor.validate().unwrap();
		let batch = RecordBatch::try_new(
			schema,
			vec![
				Arc::new(Int64Array::from(vec![Some(9), Some(1), Some(6)])) as Arc<dyn Array>,
				Arc::new(LargeStringArray::from(vec![
					Some("us"),
					Some("eu"),
					Some("us"),
				])) as Arc<dyn Array>,
			],
		)
		.unwrap();
		let mut meta: TableWriteRequestMeta = bytemuck::Zeroable::zeroed();
		meta.tenant_id = tenant();
		meta.table_object_id = descriptor.table_object_id;
		meta.schema_generation = descriptor.schema_generation;
		meta.row_count = encode_table_write_row_count(batch.num_rows()).unwrap();
		let admission = validate_table_write_request_batch(
			&meta,
			&batch,
			&descriptor,
			WriterNodeId::ZERO,
			meta.client_hlc,
		)
		.unwrap();

		let mut plaintexts = Vec::new();
		for policy in [
			RecordBodyCompressionPolicy::Uncompressed,
			RecordBodyCompressionPolicy::Lz4Frame,
		] {
			let mgr = fresh_manager().await;
			for row in 0..batch.num_rows() {
				let row_batch = batch.slice(row, 1);
				let frame = encode_batch_with_policy(&row_batch, policy).unwrap().bytes;
				mgr.append_durable_with_locality(
					tenant(),
					admission.key_identities[row],
					admission.localities[row],
					hlc(30 + row as u64),
					&frame,
				)
				.await
				.unwrap();
			}
			let slab_id = force_seal_active(&mgr).await;
			let raw = mgr.read_sealed_raw(slab_id).await.unwrap();
			let mut catalog = TableSchemaCatalogV1::default();
			catalog.upsert(tenant(), descriptor.clone()).unwrap();
			let encrypted = mgr
				.encrypt_tiered_object_blob_with_table_stats(slab_id, &raw, &catalog)
				.unwrap();
			let stats = mgr
				.config
				.crypto
				.decrypt_tiered_stats_block(slab_id, &encrypted.bytes, descriptor.schema_generation)
				.unwrap()
				.expect("stats block");
			let parsed = parse_column_stats_block(&stats.plaintext).unwrap();
			assert_eq!(parsed.header.row_count, 3);
			assert_eq!(parsed.columns[0].arrow_type_tag, ARROW_STATS_TAG_INT64);
			let id_stats = decode_column_stats_payload(&parsed.columns[0]).unwrap();
			assert_eq!(id_stats.min, Some(encode_signed_stat_value(1, 8).unwrap()));
			assert_eq!(id_stats.max, Some(encode_signed_stat_value(9, 8).unwrap()));
			let region_stats = decode_column_stats_payload(&parsed.columns[1]).unwrap();
			assert_eq!(
				region_stats.small_set,
				Some(vec![b"eu".to_vec(), b"us".to_vec()])
			);
			plaintexts.push(stats.plaintext);
		}

		assert_eq!(
			plaintexts[0], plaintexts[1],
			"CSB plaintext should be a function of decoded rows and descriptor, not IPC body codec"
		);
	}

	#[tokio::test]
	async fn table_stats_ignore_default_kv_descriptor_for_legacy_slabs() {
		let mgr = fresh_manager().await;
		let descriptor = default_kv_table_descriptor_v1().unwrap();
		let schema = descriptor.arrow_schema().unwrap();
		let mut keys = LargeBinaryBuilder::new();
		keys.append_value(b"legacy-key");
		let mut values = LargeBinaryBuilder::new();
		values.append_value(b"legacy-value");
		let batch = RecordBatch::try_new(
			Arc::new(schema),
			vec![Arc::new(keys.finish()), Arc::new(values.finish())],
		)
		.unwrap();
		let mut meta: TableWriteRequestMeta = bytemuck::Zeroable::zeroed();
		meta.tenant_id = tenant();
		meta.table_object_id = descriptor.table_object_id;
		meta.schema_generation = descriptor.schema_generation;
		meta.row_count = encode_table_write_row_count(batch.num_rows()).unwrap();
		let admission = validate_table_write_request_batch(
			&meta,
			&batch,
			&descriptor,
			WriterNodeId::ZERO,
			meta.client_hlc,
		)
		.unwrap();
		let frame = encode_test_batch(&batch);
		mgr.append(tenant(), admission.key_identities[0], hlc(12), &frame)
			.await
			.unwrap();
		let slab_id = force_seal_active(&mgr).await;
		let raw = mgr.read_sealed_raw(slab_id).await.unwrap();
		let mut catalog = TableSchemaCatalogV1::default();
		catalog.upsert(tenant(), descriptor).unwrap();

		let encrypted = mgr
			.encrypt_tiered_object_blob_with_table_stats(slab_id, &raw, &catalog)
			.unwrap();

		assert!(
			encrypted.stats_locator.is_none(),
			"default KV descriptors must not opt legacy KV slabs into ADR 039 stats"
		);
	}

	#[tokio::test]
	async fn typed_table_stats_are_mandatory_when_any_row_matches_descriptor() {
		let mgr = fresh_manager().await;
		mgr.append(tenant(), id(0x44), hlc(9), b"legacy-row")
			.await
			.unwrap();

		let schema = Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("region", DataType::LargeUtf8, true),
		]);
		let descriptor = TableSchemaDescriptorV1::new(
			TableObjectId::new([0x41; 16]),
			1,
			"analytics",
			"events",
			&schema,
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
			LocalityPolicyV1::default_table_spread(),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.unwrap();
		let batch = RecordBatch::try_new(
			Arc::new(schema),
			vec![
				Arc::new(Int64Array::from(vec![Some(7)])) as Arc<dyn Array>,
				Arc::new(LargeStringArray::from(vec![Some("us")])) as Arc<dyn Array>,
			],
		)
		.unwrap();
		let mut meta: TableWriteRequestMeta = bytemuck::Zeroable::zeroed();
		meta.tenant_id = tenant();
		meta.table_object_id = descriptor.table_object_id;
		meta.schema_generation = descriptor.schema_generation;
		meta.row_count = encode_table_write_row_count(batch.num_rows()).unwrap();
		let admission = validate_table_write_request_batch(
			&meta,
			&batch,
			&descriptor,
			WriterNodeId::ZERO,
			meta.client_hlc,
		)
		.unwrap();
		let frame = encode_test_batch(&batch);
		mgr.append_durable_with_locality(
			tenant(),
			admission.key_identities[0],
			admission.localities[0],
			hlc(10),
			&frame,
		)
		.await
		.unwrap();
		let slab_id = force_seal_active(&mgr).await;
		let raw = mgr.read_sealed_raw(slab_id).await.unwrap();
		let mut catalog = TableSchemaCatalogV1::default();
		catalog.upsert(tenant(), descriptor).unwrap();

		let err = mgr
			.encrypt_tiered_object_blob_with_table_stats(slab_id, &raw, &catalog)
			.unwrap_err();

		assert_eq!(err.kind(), ErrorKind::InvalidData);
		assert!(
			err.to_string().contains("stats are mandatory"),
			"unexpected error: {err}"
		);
	}

	#[tokio::test]
	async fn stats_advisory_absence_skips_stats_fetch() {
		let mgr = fresh_manager().await;
		let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
		let descriptor = TableSchemaDescriptorV1::new(
			TableObjectId::new([0x42; 16]),
			1,
			"analytics",
			"events",
			&schema,
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
			LocalityPolicyV1::default_table_spread(),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.unwrap();
		let pointer = TieredSlabPointer {
			slab_id: SlabId::new(42),
			l2_uri: "memory://stats-should-not-be-read".to_owned(),
			payload_hash: [0xAB; 32],
			is_encrypted: true,
			tenant_id: Some(tenant()),
			tenant_kek_id: Some(TenantKekId::try_from(1).unwrap()),
			stats_format_version: None,
			stats_block_len: None,
			stats_block_hash128: None,
			tombstone_metadata: TombstoneMetadata::None,
			supersession_metadata: SupersessionMetadata::None,
		};

		let stats = mgr
			.load_tiered_column_stats_block(&pointer, &descriptor)
			.await
			.unwrap();

		assert!(stats.is_none());
	}

	#[tokio::test]
	async fn stats_advisory_drift_fails_closed_to_unavailable() {
		let l1_dir = fresh_dir("stats-drift-l1");
		let l2_root = fresh_dir("stats-drift-l2");
		let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&l2_root).unwrap());
		let io = spawn_io_thread(64).unwrap();
		let mgr = Arc::new(
			SlabManager::open(SlabManagerConfig {
				dir: l1_dir,
				max_slab_bytes: 1 << 20,
				io,
				l2: Some(l2.clone()),
				rehydration: RehydrationPolicy::default(),
				l2_column_fetch: Default::default(),
				row_group_writer: Default::default(),
				l2_row_group_fetch: Default::default(),
				encoding_writer: Default::default(),
				l2_chunk_layout: Default::default(),
				l2_encoding_fetch: Default::default(),
				encoding_compute: Default::default(),
				group_commit: GroupCommitConfig::default(),
				crypto: test_crypto(),
			})
			.await
			.unwrap(),
		);
		let schema = Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("region", DataType::LargeUtf8, true),
		]);
		let descriptor = TableSchemaDescriptorV1::new(
			TableObjectId::new([0x43; 16]),
			1,
			"analytics",
			"events",
			&schema,
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
			LocalityPolicyV1::default_table_spread(),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.unwrap();
		let batch = RecordBatch::try_new(
			Arc::new(schema),
			vec![
				Arc::new(Int64Array::from(vec![Some(1)])) as Arc<dyn Array>,
				Arc::new(LargeStringArray::from(vec![Some("eu")])) as Arc<dyn Array>,
			],
		)
		.unwrap();
		let mut meta: TableWriteRequestMeta = bytemuck::Zeroable::zeroed();
		meta.tenant_id = tenant();
		meta.table_object_id = descriptor.table_object_id;
		meta.schema_generation = descriptor.schema_generation;
		meta.row_count = encode_table_write_row_count(batch.num_rows()).unwrap();
		let admission = validate_table_write_request_batch(
			&meta,
			&batch,
			&descriptor,
			WriterNodeId::ZERO,
			meta.client_hlc,
		)
		.unwrap();
		let frame = encode_test_batch(&batch);
		mgr.append_durable_with_locality(
			tenant(),
			admission.key_identities[0],
			admission.localities[0],
			hlc(10),
			&frame,
		)
		.await
		.unwrap();
		let slab_id = force_seal_active(&mgr).await;
		let raw = mgr.read_sealed_raw(slab_id).await.unwrap();
		let mut catalog = TableSchemaCatalogV1::default();
		catalog.upsert(tenant(), descriptor.clone()).unwrap();
		let encrypted = mgr
			.encrypt_tiered_object_blob_with_table_stats(slab_id, &raw, &catalog)
			.unwrap();
		let locator = encrypted.stats_locator.expect("stats locator");
		let l2_uri = l2
			.put(
				&format!("stats-drift/slab_{slab_id:016x}.wnd"),
				&encrypted.bytes,
			)
			.await
			.unwrap();
		let mut drifted_hash = locator.stats_block_hash128;
		drifted_hash[0] ^= 0xFF;
		let pointer = TieredSlabPointer {
			slab_id,
			l2_uri,
			payload_hash: crate::hash::content::digest(&encrypted.bytes),
			is_encrypted: true,
			tenant_id: Some(encrypted.tenant_id),
			tenant_kek_id: Some(encrypted.tenant_kek_id),
			stats_format_version: Some(locator.stats_format_version),
			stats_block_len: Some(locator.stats_block_len),
			stats_block_hash128: Some(drifted_hash),
			tombstone_metadata: TombstoneMetadata::None,
			supersession_metadata: SupersessionMetadata::None,
		};
		let drift_before = metrics().pbc_slab_stats_pointer_drift_total.get();

		let stats = mgr
			.load_tiered_column_stats_block(&pointer, &descriptor)
			.await
			.unwrap();

		assert!(stats.is_none());
		assert!(
			metrics().pbc_slab_stats_pointer_drift_total.get() >= drift_before + 1,
			"pointer drift metric should increment when advisories disagree with footer locator"
		);
	}

	#[tokio::test]
	async fn column_stats_plaintext_does_not_encode_slab_lineage() {
		let mgr = fresh_manager().await;
		let schema = Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("region", DataType::LargeUtf8, true),
		]);
		let mut descriptor = TableSchemaDescriptorV1::new(
			TableObjectId::new([0x40; 16]),
			4,
			"analytics",
			"events",
			&schema,
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
			LocalityPolicyV1::default_table_spread(),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.unwrap();
		descriptor.stats_policy = ColumnStatsPolicyV1 {
			small_sets: vec![ColumnSmallSetSpecV1::new("region")],
			..ColumnStatsPolicyV1::default()
		};
		descriptor.refresh_descriptor_fingerprints().unwrap();
		let batch = RecordBatch::try_new(
			Arc::new(schema),
			vec![
				Arc::new(Int64Array::from(vec![Some(2), Some(8)])) as Arc<dyn Array>,
				Arc::new(LargeStringArray::from(vec![Some("eu"), Some("us")])) as Arc<dyn Array>,
			],
		)
		.unwrap();
		let mut meta: TableWriteRequestMeta = bytemuck::Zeroable::zeroed();
		meta.tenant_id = tenant();
		meta.table_object_id = descriptor.table_object_id;
		meta.schema_generation = descriptor.schema_generation;
		meta.row_count = encode_table_write_row_count(batch.num_rows()).unwrap();
		let admission = validate_table_write_request_batch(
			&meta,
			&batch,
			&descriptor,
			WriterNodeId::ZERO,
			meta.client_hlc,
		)
		.unwrap();
		for row in 0..batch.num_rows() {
			let row_batch = batch.slice(row, 1);
			let frame = encode_test_batch(&row_batch);
			mgr.append_durable_with_locality(
				tenant(),
				admission.key_identities[row],
				admission.localities[row],
				hlc(20 + row as u64),
				&frame,
			)
			.await
			.unwrap();
		}
		let slab_id = force_seal_active(&mgr).await;
		let raw = mgr.read_sealed_raw(slab_id).await.unwrap();
		let mut catalog = TableSchemaCatalogV1::default();
		catalog.upsert(tenant(), descriptor.clone()).unwrap();

		let encrypted = mgr
			.encrypt_tiered_object_blob_with_table_stats(slab_id, &raw, &catalog)
			.unwrap();
		let alternate_slab_id = slab_id.saturating_add(0x1000);
		let alternate_encrypted = mgr
			.encrypt_tiered_object_blob_with_table_stats(alternate_slab_id, &raw, &catalog)
			.unwrap();
		assert_ne!(
			encrypted
				.stats_locator
				.expect("stats locator")
				.stats_block_hash128,
			alternate_encrypted
				.stats_locator
				.expect("alternate stats locator")
				.stats_block_hash128,
			"CSB ciphertext is still slab-bound through ADR 039 AEAD metadata"
		);

		let stats = mgr
			.config
			.crypto
			.decrypt_tiered_stats_block(slab_id, &encrypted.bytes, descriptor.schema_generation)
			.unwrap()
			.expect("stats block");
		let alternate_stats = mgr
			.config
			.crypto
			.decrypt_tiered_stats_block(
				alternate_slab_id,
				&alternate_encrypted.bytes,
				descriptor.schema_generation,
			)
			.unwrap()
			.expect("alternate stats block");
		assert_eq!(
			stats.plaintext, alternate_stats.plaintext,
			"CSB plaintext is a function of output rows and descriptor only"
		);
		let parsed = parse_column_stats_block(&stats.plaintext).unwrap();
		assert_eq!(parsed.header.table_object_id, descriptor.table_object_id);
		assert_eq!(
			parsed.header.schema_generation,
			descriptor.schema_generation
		);
		assert_eq!(parsed.header.row_count, 2);
		assert_eq!(parsed.header.column_block_count, 2);
	}

	#[tokio::test]
	async fn tiered_retention_rewrite_recomputes_stats_from_survivors() {
		let l1_dir = fresh_dir("retention-stats-l1");
		let l2_root = fresh_dir("retention-stats-l2");
		let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&l2_root).unwrap());
		let io = spawn_io_thread(64).unwrap();
		let mgr = Arc::new(
			SlabManager::open(SlabManagerConfig {
				dir: l1_dir,
				max_slab_bytes: 1 << 20,
				io,
				l2: Some(l2.clone()),
				rehydration: RehydrationPolicy::default(),
				l2_column_fetch: Default::default(),
				row_group_writer: Default::default(),
				l2_row_group_fetch: Default::default(),
				encoding_writer: Default::default(),
				l2_chunk_layout: Default::default(),
				l2_encoding_fetch: Default::default(),
				encoding_compute: Default::default(),
				group_commit: GroupCommitConfig::default(),
				crypto: test_crypto(),
			})
			.await
			.unwrap(),
		);
		let schema = Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("region", DataType::LargeUtf8, true),
		]);
		let mut descriptor = TableSchemaDescriptorV1::new(
			TableObjectId::new([0x39; 16]),
			3,
			"analytics",
			"events",
			&schema,
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
			LocalityPolicyV1::default_table_spread(),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.unwrap();
		descriptor.stats_policy = ColumnStatsPolicyV1 {
			small_sets: vec![ColumnSmallSetSpecV1::new("region")],
			..ColumnStatsPolicyV1::default()
		};
		descriptor.refresh_descriptor_fingerprints().unwrap();
		let mut catalog = TableSchemaCatalogV1::default();
		catalog.upsert(tenant(), descriptor.clone()).unwrap();

		for (hlc_value, region) in [
			(hlc(10), "aa-old"),
			(hlc(20), "mm-anchor"),
			(hlc(30), "zz-latest"),
		] {
			let batch = RecordBatch::try_new(
				Arc::new(schema.clone()),
				vec![
					Arc::new(Int64Array::from(vec![Some(9)])) as Arc<dyn Array>,
					Arc::new(LargeStringArray::from(vec![Some(region)])) as Arc<dyn Array>,
				],
			)
			.unwrap();
			let mut meta: TableWriteRequestMeta = bytemuck::Zeroable::zeroed();
			meta.tenant_id = tenant();
			meta.table_object_id = descriptor.table_object_id;
			meta.schema_generation = descriptor.schema_generation;
			meta.row_count = encode_table_write_row_count(batch.num_rows()).unwrap();
			let admission = validate_table_write_request_batch(
				&meta,
				&batch,
				&descriptor,
				WriterNodeId::ZERO,
				meta.client_hlc,
			)
			.unwrap();
			let frame = encode_test_batch(&batch);
			mgr.append_durable_with_locality(
				tenant(),
				admission.key_identities[0],
				admission.localities[0],
				hlc_value,
				&frame,
			)
			.await
			.unwrap();
		}

		let slab_id = force_seal_active(&mgr).await;
		let raw = mgr.read_sealed_raw(slab_id).await.unwrap();
		let old_encrypted = mgr
			.encrypt_tiered_object_blob_with_table_stats(slab_id, &raw, &catalog)
			.unwrap();
		let old_stats = mgr
			.config
			.crypto
			.decrypt_tiered_stats_block(slab_id, &old_encrypted.bytes, descriptor.schema_generation)
			.unwrap()
			.expect("old stats block");
		let old_region = decode_column_stats_payload(
			&parse_column_stats_block(&old_stats.plaintext)
				.unwrap()
				.columns[1],
		)
		.unwrap();
		assert_eq!(old_region.min, Some(b"aa-old".to_vec()));
		let old_footer = mgr
			.encrypted_tiered_footer_summary(&old_encrypted.bytes)
			.unwrap()
			.expect("encrypted footer");
		let old_uri = l2
			.put(
				&format!("tier/slab_{slab_id:016x}.wnd"),
				&old_encrypted.bytes,
			)
			.await
			.unwrap();
		let old_pointer = TieredSlabPointer {
			slab_id,
			l2_uri: old_uri,
			payload_hash: crate::hash::content::digest(&old_encrypted.bytes),
			is_encrypted: true,
			tenant_id: Some(tenant()),
			tenant_kek_id: Some(old_footer.tenant_kek_id),
			stats_format_version: old_encrypted
				.stats_locator
				.map(|locator| locator.stats_format_version),
			stats_block_len: old_encrypted
				.stats_locator
				.map(|locator| locator.stats_block_len),
			stats_block_hash128: old_encrypted
				.stats_locator
				.map(|locator| locator.stats_block_hash128),
			tombstone_metadata: TombstoneMetadata::None,
			supersession_metadata: SupersessionMetadata::None,
		};
		mgr.apply_tier_slab(old_pointer.clone()).await.unwrap();

		let prepared = mgr
			.prepare_tiered_retention_rewrite(&old_pointer, hlc(25), &catalog)
			.await
			.unwrap()
			.expect("retention rewrite");
		let PreparedTieredRetentionRewrite::Rewrite {
			encrypted_bytes,
			pruned_versions,
			..
		} = prepared
		else {
			panic!("expected rewrite, not retire");
		};
		assert_eq!(pruned_versions, 1);
		let rewritten_stats = mgr
			.config
			.crypto
			.decrypt_tiered_stats_block(slab_id, &encrypted_bytes, descriptor.schema_generation)
			.unwrap()
			.expect("rewritten stats block");
		let parsed = parse_column_stats_block(&rewritten_stats.plaintext).unwrap();
		assert_eq!(parsed.header.row_count, 2);
		let region_stats = decode_column_stats_payload(&parsed.columns[1]).unwrap();
		assert_eq!(region_stats.min, Some(b"mm-anchor".to_vec()));
		assert_eq!(region_stats.max, Some(b"zz-latest".to_vec()));
		assert_eq!(
			region_stats.small_set,
			Some(vec![b"mm-anchor".to_vec(), b"zz-latest".to_vec()])
		);
	}

	#[tokio::test]
	async fn tiered_retention_rewrite_filters_partial_multirow_storage_records() {
		let l1_dir = fresh_dir("retention-multirow-l1");
		let l2_root = fresh_dir("retention-multirow-l2");
		let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&l2_root).unwrap());
		let io = spawn_io_thread(64).unwrap();
		let mgr = Arc::new(
			SlabManager::open(SlabManagerConfig {
				dir: l1_dir,
				max_slab_bytes: 1 << 20,
				io,
				l2: Some(l2.clone()),
				rehydration: RehydrationPolicy::default(),
				l2_column_fetch: Default::default(),
				row_group_writer: Default::default(),
				l2_row_group_fetch: Default::default(),
				encoding_writer: Default::default(),
				l2_chunk_layout: Default::default(),
				l2_encoding_fetch: Default::default(),
				encoding_compute: Default::default(),
				group_commit: GroupCommitConfig::default(),
				crypto: test_crypto(),
			})
			.await
			.unwrap(),
		);

		let key_a = id(0xA1);
		let key_b = id(0xB2);
		let key_c = id(0xC3);
		let loc_a = Locality::new(20);
		let loc_b = Locality::new(10);
		let loc_c = Locality::new(30);
		let frame = encode_test_multirow_storage_frame(&[
			(key_c, loc_c, hlc(30), 300),
			(key_a, loc_a, hlc(10), 100),
			(key_b, loc_b, hlc(20), 200),
		]);
		let entries = storage_multirow_index_entries_from_frame(&frame)
			.unwrap()
			.expect("multi-row entries");
		mgr.append_durable_multirow_with_locality(tenant(), &frame, &entries)
			.await
			.unwrap();
		mgr.append_durable_with_locality(tenant(), key_a, loc_a, hlc(40), b"new-a")
			.await
			.unwrap();

		let slab_id = force_seal_active(&mgr).await;
		let raw = mgr.read_sealed_raw(slab_id).await.unwrap();
		let old_encrypted = mgr.encrypt_tiered_object_blob(slab_id, &raw).unwrap();
		let old_uri = l2
			.put(
				&format!("tier/slab_{slab_id:016x}.wnd"),
				&old_encrypted.bytes,
			)
			.await
			.unwrap();
		let old_pointer = TieredSlabPointer {
			slab_id,
			l2_uri: old_uri,
			payload_hash: crate::hash::content::digest(&old_encrypted.bytes),
			is_encrypted: true,
			tenant_id: Some(tenant()),
			tenant_kek_id: Some(old_encrypted.tenant_kek_id),
			stats_format_version: None,
			stats_block_len: None,
			stats_block_hash128: None,
			tombstone_metadata: TombstoneMetadata::None,
			supersession_metadata: SupersessionMetadata::None,
		};
		mgr.apply_tier_slab(old_pointer.clone()).await.unwrap();

		let prepared = mgr
			.prepare_tiered_retention_rewrite(
				&old_pointer,
				hlc(50),
				&TableSchemaCatalogV1::default(),
			)
			.await
			.unwrap()
			.expect("retention rewrite");
		let PreparedTieredRetentionRewrite::Rewrite {
			encrypted_bytes,
			pruned_versions,
			..
		} = prepared
		else {
			panic!("expected rewrite, not retire");
		};
		assert_eq!(pruned_versions, 1);

		let rewritten_pointer = TieredSlabPointer {
			slab_id,
			l2_uri: "rewritten".to_string(),
			payload_hash: crate::hash::content::digest(&encrypted_bytes),
			is_encrypted: true,
			tenant_id: Some(tenant()),
			tenant_kek_id: Some(old_encrypted.tenant_kek_id),
			stats_format_version: None,
			stats_block_len: None,
			stats_block_hash128: None,
			tombstone_metadata: TombstoneMetadata::None,
			supersession_metadata: SupersessionMetadata::None,
		};
		let rewritten_plaintext = decode_tiered_blob(
			mgr.config.crypto.as_ref(),
			&rewritten_pointer,
			&encrypted_bytes,
		)
		.unwrap();
		let recovered = recover_slab_entries_from_blob(&rewritten_plaintext, slab_id).unwrap();
		assert!(
			!recovered
				.iter()
				.any(|entry| entry.key_identity == key_a && entry.location.hlc == hlc(10))
		);
		assert!(
			recovered
				.iter()
				.any(|entry| entry.key_identity == key_a && entry.location.hlc == hlc(40))
		);
		assert!(
			recovered
				.iter()
				.any(|entry| entry.key_identity == key_b && entry.location.hlc == hlc(20))
		);
		assert!(
			recovered
				.iter()
				.any(|entry| entry.key_identity == key_c && entry.location.hlc == hlc(30))
		);

		let mut groups: BTreeMap<(u64, u32), Vec<RecoveredSlabEntry>> = BTreeMap::new();
		for entry in recovered {
			groups
				.entry((entry.location.offset, entry.location.length))
				.or_default()
				.push(entry);
		}
		assert_eq!(groups.len(), 2);
		let multirow = groups
			.values()
			.find(|entries| entries.len() == 2)
			.expect("rewritten multi-row record");
		assert_eq!(multirow[0].key_identity, key_b);
		assert_eq!(multirow[0].location.record_row_index.get(), 0);
		assert_eq!(multirow[1].key_identity, key_c);
		assert_eq!(multirow[1].location.record_row_index.get(), 1);
	}

	#[tokio::test]
	async fn l1_retention_pruning_filters_partial_multirow_storage_records_once() {
		let mgr = fresh_manager().await;
		let key_a = id(0x1A);
		let key_b = id(0x1B);
		let key_c = id(0x1C);
		let loc_a = Locality::new(20);
		let loc_b = Locality::new(10);
		let loc_c = Locality::new(30);
		let frame = encode_test_multirow_storage_frame(&[
			(key_c, loc_c, hlc(30), 300),
			(key_a, loc_a, hlc(10), 100),
			(key_b, loc_b, hlc(20), 200),
		]);
		let entries = storage_multirow_index_entries_from_frame(&frame)
			.unwrap()
			.expect("multi-row entries");
		mgr.append_durable_multirow_with_locality(tenant(), &frame, &entries)
			.await
			.unwrap();
		mgr.append_durable_with_locality(tenant(), key_a, loc_a, hlc(40), b"new-a")
			.await
			.unwrap();

		let old_slab_id = force_seal_active(&mgr).await;
		let report = mgr.prune_versions_older_than(hlc(50), 1).await.unwrap();
		assert_eq!(report.compacted_slabs, 1);
		assert_eq!(report.pruned_versions, 1);
		assert!(
			mgr.lookup_as_of(tenant(), key_a, hlc(10)).is_none(),
			"pruned old row should leave the historical slot absent"
		);

		let loc_b_new = mgr.lookup(tenant(), key_b).expect("key_b survivor");
		let loc_c_new = mgr.lookup(tenant(), key_c).expect("key_c survivor");
		assert_ne!(loc_b_new.slab_id, old_slab_id);
		assert_eq!(loc_b_new.slab_id, loc_c_new.slab_id);
		assert_eq!(loc_b_new.offset, loc_c_new.offset);
		assert_eq!(loc_b_new.record_row_index.get(), 0);
		assert_eq!(loc_c_new.record_row_index.get(), 1);

		let rewritten_frame = mgr.read(loc_b_new).await.unwrap();
		let rewritten_entries = storage_multirow_index_entries_from_frame(&rewritten_frame)
			.unwrap()
			.expect("rewritten multi-row entries");
		assert_eq!(rewritten_entries.len(), 2);
		assert_eq!(rewritten_entries[0].key_identity, key_b);
		assert_eq!(rewritten_entries[1].key_identity, key_c);
		assert_eq!(
			mgr.read(mgr.lookup(tenant(), key_a).expect("new key_a"))
				.await
				.unwrap(),
			b"new-a"
		);
	}

	#[tokio::test]
	async fn l1_table_retention_prunes_append_only_rows_when_cluster_retention_disabled() {
		let mgr = fresh_manager().await;
		let descriptor = append_only_retention_descriptor(TableObjectId::new([0x48; 16]));
		let (schemas, lifecycle) = fixed_table_retention_catalog(&descriptor, 60_000);
		let old_key = id(0x4801);
		let live_key = id(0x4802);
		let frame = encode_test_append_only_storage_frame(
			descriptor.table_object_id,
			&[
				(old_key, Locality::new(11), hlc(30_000), 100, false),
				(live_key, Locality::new(12), hlc(50_000), 200, false),
			],
		);
		let entries = storage_multirow_index_entries_from_frame(&frame)
			.unwrap()
			.expect("append-only storage entries");
		mgr.append_durable_multirow_with_locality(tenant(), &frame, &entries)
			.await
			.unwrap();

		let old_slab_id = force_seal_active(&mgr).await;
		let report = mgr
			.prune_versions_for_retention(
				MvccRetentionPolicy::Disabled,
				&lifecycle,
				&schemas,
				&SecondaryIndexCatalog::default(),
				100_000,
				1,
			)
			.await
			.unwrap();

		assert_eq!(report.compacted_slabs, 1);
		assert_eq!(report.pruned_versions, 1);
		assert!(mgr.lookup(tenant(), old_key).is_none());
		let live_location = mgr.lookup(tenant(), live_key).expect("live row survives");
		assert_ne!(live_location.slab_id, old_slab_id);
		let live_frame = mgr.read(live_location).await.unwrap();
		let live_entries = storage_multirow_index_entries_from_frame(&live_frame)
			.unwrap()
			.expect("rewritten live entry");
		assert_eq!(live_entries.len(), 1);
		assert_eq!(live_entries[0].key_identity, live_key);
	}

	#[tokio::test]
	async fn l1_table_retention_reports_lag_for_retained_rows_below_floor() {
		let mgr = fresh_manager().await;
		let descriptor = append_only_retention_descriptor(TableObjectId::new([0x4A; 16]));
		let (schemas, lifecycle) = fixed_table_retention_catalog(&descriptor, 60_000);
		let pruned_key = id(0x4A01);
		let retained_key = id(0x4A02);
		let frame = encode_test_append_only_storage_frame(
			descriptor.table_object_id,
			&[
				(pruned_key, Locality::new(31), hlc(30_000), 100, false),
				(retained_key, Locality::new(32), hlc(30_000), 200, false),
			],
		);
		let entries = storage_multirow_index_entries_from_frame(&frame)
			.unwrap()
			.expect("append-only storage entries");
		mgr.append_durable_multirow_with_locality(tenant(), &frame, &entries)
			.await
			.unwrap();
		let (tombstone_frame, tombstone_entries) = encode_test_row_tombstone_marker_frame(
			descriptor.table_object_id,
			retained_key,
			Locality::new(32),
			hlc(50_000),
		);
		mgr.append_durable_multirow_with_locality(tenant(), &tombstone_frame, &tombstone_entries)
			.await
			.unwrap();

		force_seal_active(&mgr).await;
		let report = mgr
			.prune_versions_for_retention(
				MvccRetentionPolicy::Disabled,
				&lifecycle,
				&schemas,
				&SecondaryIndexCatalog::default(),
				100_000,
				1,
			)
			.await
			.unwrap();

		assert_eq!(report.compacted_slabs, 1);
		assert_eq!(report.pruned_versions, 1);
		assert_eq!(report.max_lag_seconds, 70);
		assert!(mgr.lookup(tenant(), pruned_key).is_none());
		assert!(
			!mgr.lookup_as_of(tenant(), retained_key, hlc(30_000))
				.expect("retained data row below floor")
				.is_tombstone
		);
		assert!(
			mgr.lookup_as_of(tenant(), retained_key, hlc(50_000))
				.expect("retained tombstone above floor")
				.is_tombstone
		);
	}

	#[tokio::test]
	async fn l1_table_retention_reports_lag_without_rewrite_for_pair_waiting_on_tombstone() {
		let mgr = fresh_manager().await;
		let descriptor = append_only_retention_descriptor(TableObjectId::new([0x4B; 16]));
		let (schemas, lifecycle) = fixed_table_retention_catalog(&descriptor, 60_000);
		let retained_key = id(0x4B01);
		let frame = encode_test_append_only_storage_frame(
			descriptor.table_object_id,
			&[(retained_key, Locality::new(33), hlc(30_000), 200, false)],
		);
		let entries = storage_multirow_index_entries_from_frame(&frame)
			.unwrap()
			.expect("append-only storage entries");
		mgr.append_durable_multirow_with_locality(tenant(), &frame, &entries)
			.await
			.unwrap();
		let (tombstone_frame, tombstone_entries) = encode_test_row_tombstone_marker_frame(
			descriptor.table_object_id,
			retained_key,
			Locality::new(33),
			hlc(50_000),
		);
		mgr.append_durable_multirow_with_locality(tenant(), &tombstone_frame, &tombstone_entries)
			.await
			.unwrap();

		force_seal_active(&mgr).await;
		let report = mgr
			.prune_versions_for_retention(
				MvccRetentionPolicy::Disabled,
				&lifecycle,
				&schemas,
				&SecondaryIndexCatalog::default(),
				100_000,
				1,
			)
			.await
			.unwrap();

		assert_eq!(report.compacted_slabs, 0);
		assert_eq!(report.pruned_versions, 0);
		assert_eq!(report.max_lag_seconds, 70);
		assert!(
			!mgr.lookup_as_of(tenant(), retained_key, hlc(30_000))
				.expect("retained data row below floor")
				.is_tombstone
		);
		assert!(
			mgr.lookup_as_of(tenant(), retained_key, hlc(50_000))
				.expect("retained tombstone above floor")
				.is_tombstone
		);
	}

	#[tokio::test]
	async fn l1_table_retention_keeps_pk_boundary_tombstone_under_cluster_floor() {
		let dir = fresh_dir("pk-boundary-tombstone-reopen");
		let io = spawn_io_thread(64).unwrap();
		let crypto = test_crypto();
		let mgr = open_manager_at_dir(dir.clone(), io.clone(), crypto.clone()).await;
		let descriptor = pk_retention_descriptor(TableObjectId::new([0x50; 16]));
		let mut schemas = TableSchemaCatalogV1::default();
		schemas.upsert(tenant(), descriptor.clone()).unwrap();
		let lifecycle = TableLifecycleCatalog::default();
		let key = id(0x5001);
		let locality = Locality::new(50);
		let data_frame = encode_test_append_only_storage_frame(
			descriptor.table_object_id,
			&[(key, locality, hlc(10_000), 100, false)],
		);
		let data_entries = storage_multirow_index_entries_from_frame(&data_frame)
			.unwrap()
			.expect("pk data entry");
		mgr.append_durable_multirow_with_locality(tenant(), &data_frame, &data_entries)
			.await
			.unwrap();
		let (tombstone_frame, tombstone_entries) =
			encode_test_pk_tombstone_marker_frame(&descriptor, key, locality, hlc(20_000));
		mgr.append_durable_multirow_with_locality(tenant(), &tombstone_frame, &tombstone_entries)
			.await
			.unwrap();

		force_seal_active(&mgr).await;
		let report = mgr
			.prune_versions_for_retention(
				MvccRetentionPolicy::FixedWindow(Duration::from_millis(60_000)),
				&lifecycle,
				&schemas,
				&SecondaryIndexCatalog::default(),
				100_000,
				1,
			)
			.await
			.unwrap();

		assert_eq!(report.compacted_slabs, 1);
		assert_eq!(report.pruned_versions, 1);
		assert_eq!(report.pair_drops, 0);
		assert!(mgr.lookup_as_of(tenant(), key, hlc(10_000)).is_none());
		assert!(
			mgr.lookup_as_of(tenant(), key, hlc(20_000))
				.expect("pk boundary tombstone survives")
				.is_tombstone
		);
		assert!(
			mgr.lookup(tenant(), key)
				.expect("latest pk boundary tombstone survives")
				.is_tombstone
		);
		mgr.fsync_all_local_slabs().await.unwrap();
		drop(mgr);

		let reopened = open_manager_at_dir(dir, io, crypto).await;
		assert!(
			reopened.lookup_as_of(tenant(), key, hlc(10_000)).is_none(),
			"pruned data row should stay absent after reopen"
		);
		assert!(
			reopened
				.lookup_as_of(tenant(), key, hlc(20_000))
				.expect("compacted pk boundary tombstone survives reopen")
				.is_tombstone
		);
		assert!(
			reopened
				.lookup(tenant(), key)
				.expect("latest compacted pk tombstone survives reopen")
				.is_tombstone
		);
	}

	#[tokio::test]
	async fn l1_retention_pruning_pair_gc_drops_data_and_row_tombstone() {
		let mgr = fresh_manager().await;
		let key = id(0x4701);
		let loc = Locality::new(47);
		let data_frame = encode_test_multirow_storage_frame(&[(key, loc, hlc(10), 100)]);
		let data_entries = storage_multirow_index_entries_from_frame(&data_frame)
			.unwrap()
			.expect("data entry");
		mgr.append_durable_multirow_with_locality(tenant(), &data_frame, &data_entries)
			.await
			.unwrap();
		let (tombstone_frame, tombstone_entries) = encode_test_row_tombstone_marker_frame(
			TableObjectId::new([0x47; 16]),
			key,
			loc,
			hlc(20),
		);
		mgr.append_durable_multirow_with_locality(tenant(), &tombstone_frame, &tombstone_entries)
			.await
			.unwrap();

		force_seal_active(&mgr).await;
		let report = mgr.prune_versions_older_than(hlc(30), 1).await.unwrap();

		assert_eq!(report.compacted_slabs, 1);
		assert_eq!(report.pruned_versions, 2);
		assert!(mgr.lookup(tenant(), key).is_none());
		assert!(mgr.lookup_as_of(tenant(), key, hlc(10)).is_none());
	}

	#[tokio::test]
	async fn l1_retention_pruning_keeps_pair_until_floor_passes_tombstone() {
		let mgr = fresh_manager().await;
		let key = id(0x4702);
		let loc = Locality::new(48);
		let data_frame = encode_test_multirow_storage_frame(&[(key, loc, hlc(10), 100)]);
		let data_entries = storage_multirow_index_entries_from_frame(&data_frame)
			.unwrap()
			.expect("data entry");
		mgr.append_durable_multirow_with_locality(tenant(), &data_frame, &data_entries)
			.await
			.unwrap();
		let (tombstone_frame, tombstone_entries) = encode_test_row_tombstone_marker_frame(
			TableObjectId::new([0x48; 16]),
			key,
			loc,
			hlc(20),
		);
		mgr.append_durable_multirow_with_locality(tenant(), &tombstone_frame, &tombstone_entries)
			.await
			.unwrap();

		force_seal_active(&mgr).await;
		let report = mgr.prune_versions_older_than(hlc(20), 1).await.unwrap();

		assert_eq!(report.compacted_slabs, 0);
		assert_eq!(report.pruned_versions, 0);
		assert!(
			!mgr.lookup_as_of(tenant(), key, hlc(10))
				.expect("data before delete")
				.is_tombstone
		);
		assert!(
			mgr.lookup_as_of(tenant(), key, hlc(20))
				.expect("tombstone at delete")
				.is_tombstone
		);
	}

	#[tokio::test]
	async fn tiered_retention_rewrite_pair_gc_drops_data_and_row_tombstone() {
		let l1_dir = fresh_dir("retention-tombstone-pair-l1");
		let l2_root = fresh_dir("retention-tombstone-pair-l2");
		let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&l2_root).unwrap());
		let io = spawn_io_thread(64).unwrap();
		let mgr = Arc::new(
			SlabManager::open(SlabManagerConfig {
				dir: l1_dir,
				max_slab_bytes: 1 << 20,
				io,
				l2: Some(l2.clone()),
				rehydration: RehydrationPolicy::default(),
				l2_column_fetch: Default::default(),
				row_group_writer: Default::default(),
				l2_row_group_fetch: Default::default(),
				encoding_writer: Default::default(),
				l2_chunk_layout: Default::default(),
				l2_encoding_fetch: Default::default(),
				encoding_compute: Default::default(),
				group_commit: GroupCommitConfig::default(),
				crypto: test_crypto(),
			})
			.await
			.unwrap(),
		);

		let key_a = id(0x4703);
		let key_b = id(0x4704);
		let loc_a = Locality::new(49);
		let loc_b = Locality::new(50);
		let data_a = encode_test_multirow_storage_frame(&[(key_a, loc_a, hlc(10), 100)]);
		let entries_a = storage_multirow_index_entries_from_frame(&data_a)
			.unwrap()
			.expect("data a entry");
		mgr.append_durable_multirow_with_locality(tenant(), &data_a, &entries_a)
			.await
			.unwrap();
		let (tombstone_a, tombstone_entries_a) = encode_test_row_tombstone_marker_frame(
			TableObjectId::new([0x49; 16]),
			key_a,
			loc_a,
			hlc(20),
		);
		mgr.append_durable_multirow_with_locality(tenant(), &tombstone_a, &tombstone_entries_a)
			.await
			.unwrap();
		let data_b = encode_test_multirow_storage_frame(&[(key_b, loc_b, hlc(12), 200)]);
		let entries_b = storage_multirow_index_entries_from_frame(&data_b)
			.unwrap()
			.expect("data b entry");
		mgr.append_durable_multirow_with_locality(tenant(), &data_b, &entries_b)
			.await
			.unwrap();

		let slab_id = force_seal_active(&mgr).await;
		let raw = mgr.read_sealed_raw(slab_id).await.unwrap();
		let old_encrypted = mgr.encrypt_tiered_object_blob(slab_id, &raw).unwrap();
		let old_uri = l2
			.put(
				&format!("tier/slab_{slab_id:016x}-pair.wnd"),
				&old_encrypted.bytes,
			)
			.await
			.unwrap();
		let old_pointer = TieredSlabPointer {
			slab_id,
			l2_uri: old_uri,
			payload_hash: crate::hash::content::digest(&old_encrypted.bytes),
			is_encrypted: true,
			tenant_id: Some(tenant()),
			tenant_kek_id: Some(old_encrypted.tenant_kek_id),
			stats_format_version: None,
			stats_block_len: None,
			stats_block_hash128: None,
			tombstone_metadata: TombstoneMetadata::None,
			supersession_metadata: SupersessionMetadata::None,
		};
		mgr.apply_tier_slab(old_pointer.clone()).await.unwrap();

		let prepared = mgr
			.prepare_tiered_retention_rewrite(
				&old_pointer,
				hlc(30),
				&TableSchemaCatalogV1::default(),
			)
			.await
			.unwrap()
			.expect("retention rewrite");
		let PreparedTieredRetentionRewrite::Rewrite {
			encrypted_bytes,
			pruned_versions,
			..
		} = prepared
		else {
			panic!("expected rewrite with key_b survivor");
		};
		assert_eq!(pruned_versions, 2);

		let rewritten_pointer = TieredSlabPointer {
			slab_id,
			l2_uri: "rewritten-pair".to_string(),
			payload_hash: crate::hash::content::digest(&encrypted_bytes),
			is_encrypted: true,
			tenant_id: Some(tenant()),
			tenant_kek_id: Some(old_encrypted.tenant_kek_id),
			stats_format_version: None,
			stats_block_len: None,
			stats_block_hash128: None,
			tombstone_metadata: TombstoneMetadata::None,
			supersession_metadata: SupersessionMetadata::None,
		};
		let rewritten_plaintext = decode_tiered_blob(
			mgr.config.crypto.as_ref(),
			&rewritten_pointer,
			&encrypted_bytes,
		)
		.unwrap();
		let recovered = recover_slab_entries_from_blob(&rewritten_plaintext, slab_id).unwrap();
		assert_eq!(recovered.len(), 1);
		assert_eq!(recovered[0].key_identity, key_b);
		assert!(!recovered[0].location.is_tombstone);
	}

	#[tokio::test]
	async fn tiered_table_retention_keeps_pk_boundary_tombstone_under_cluster_floor() {
		let l1_dir = fresh_dir("retention-pk-boundary-l1");
		let l2_root = fresh_dir("retention-pk-boundary-l2");
		let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&l2_root).unwrap());
		let io = spawn_io_thread(64).unwrap();
		let mgr = Arc::new(
			SlabManager::open(SlabManagerConfig {
				dir: l1_dir,
				max_slab_bytes: 1 << 20,
				io,
				l2: Some(l2.clone()),
				rehydration: RehydrationPolicy::default(),
				l2_column_fetch: Default::default(),
				row_group_writer: Default::default(),
				l2_row_group_fetch: Default::default(),
				encoding_writer: Default::default(),
				l2_chunk_layout: Default::default(),
				l2_encoding_fetch: Default::default(),
				encoding_compute: Default::default(),
				group_commit: GroupCommitConfig::default(),
				crypto: test_crypto(),
			})
			.await
			.unwrap(),
		);
		let descriptor = pk_retention_descriptor(TableObjectId::new([0x58; 16]));
		let mut schemas = TableSchemaCatalogV1::default();
		schemas.upsert(tenant(), descriptor.clone()).unwrap();
		let lifecycle = TableLifecycleCatalog::default();
		let key = id(0x5801);
		let locality = Locality::new(58);
		let data_frame = encode_test_append_only_storage_frame(
			descriptor.table_object_id,
			&[(key, locality, hlc(10_000), 100, false)],
		);
		let data_entries = storage_multirow_index_entries_from_frame(&data_frame)
			.unwrap()
			.expect("pk data entry");
		mgr.append_durable_multirow_with_locality(tenant(), &data_frame, &data_entries)
			.await
			.unwrap();
		let (tombstone_frame, tombstone_entries) =
			encode_test_pk_tombstone_marker_frame(&descriptor, key, locality, hlc(20_000));
		mgr.append_durable_multirow_with_locality(tenant(), &tombstone_frame, &tombstone_entries)
			.await
			.unwrap();

		let slab_id = force_seal_active(&mgr).await;
		let raw = mgr.read_sealed_raw(slab_id).await.unwrap();
		let old_encrypted = mgr
			.encrypt_tiered_object_blob_with_table_stats(slab_id, &raw, &schemas)
			.unwrap();
		let old_footer = mgr
			.encrypted_tiered_footer_summary(&old_encrypted.bytes)
			.unwrap()
			.expect("encrypted footer");
		let old_uri = l2
			.put(
				&format!("tier/slab_{slab_id:016x}-pk-boundary.wnd"),
				&old_encrypted.bytes,
			)
			.await
			.unwrap();
		let old_pointer = TieredSlabPointer {
			slab_id,
			l2_uri: old_uri,
			payload_hash: crate::hash::content::digest(&old_encrypted.bytes),
			is_encrypted: true,
			tenant_id: Some(tenant()),
			tenant_kek_id: Some(old_footer.tenant_kek_id),
			stats_format_version: old_encrypted
				.stats_locator
				.map(|locator| locator.stats_format_version),
			stats_block_len: old_encrypted
				.stats_locator
				.map(|locator| locator.stats_block_len),
			stats_block_hash128: old_encrypted
				.stats_locator
				.map(|locator| locator.stats_block_hash128),
			tombstone_metadata: TombstoneMetadata::None,
			supersession_metadata: SupersessionMetadata::None,
		};
		mgr.apply_tier_slab(old_pointer.clone()).await.unwrap();

		let prepared = mgr
			.prepare_tiered_table_retention_rewrite(
				&old_pointer,
				MvccRetentionPolicy::FixedWindow(Duration::from_millis(60_000)),
				&lifecycle,
				&schemas,
				100_000,
			)
			.await
			.unwrap()
			.expect("pk boundary retention rewrite");
		let PreparedTieredRetentionRewrite::Rewrite {
			encrypted_bytes,
			pruned_versions,
			retention_stats,
			tombstone_metadata,
			..
		} = prepared
		else {
			panic!("expected rewrite retaining pk boundary tombstone");
		};
		assert_eq!(pruned_versions, 1);
		assert_eq!(retention_stats.pruned_data_rows, 1);
		assert_eq!(retention_stats.pruned_tombstone_rows, 0);
		assert_eq!(tombstone_metadata, TombstoneMetadata::Earliest(hlc(20_000)));

		let rewritten_pointer = TieredSlabPointer {
			slab_id,
			l2_uri: "rewritten-pk-boundary".to_string(),
			payload_hash: crate::hash::content::digest(&encrypted_bytes),
			is_encrypted: true,
			tenant_id: Some(tenant()),
			tenant_kek_id: Some(old_footer.tenant_kek_id),
			stats_format_version: old_encrypted
				.stats_locator
				.map(|locator| locator.stats_format_version),
			stats_block_len: old_encrypted
				.stats_locator
				.map(|locator| locator.stats_block_len),
			stats_block_hash128: old_encrypted
				.stats_locator
				.map(|locator| locator.stats_block_hash128),
			tombstone_metadata: TombstoneMetadata::None,
			supersession_metadata: SupersessionMetadata::None,
		};
		let rewritten_plaintext = decode_tiered_blob(
			mgr.config.crypto.as_ref(),
			&rewritten_pointer,
			&encrypted_bytes,
		)
		.unwrap();
		let recovered = recover_slab_entries_from_blob(&rewritten_plaintext, slab_id).unwrap();
		assert_eq!(recovered.len(), 1);
		assert_eq!(recovered[0].key_identity, key);
		assert_eq!(recovered[0].location.hlc, hlc(20_000));
		assert!(recovered[0].location.is_tombstone);
	}

	#[tokio::test]
	async fn tiered_table_retention_rewrite_prunes_append_only_rows_when_cluster_retention_disabled()
	 {
		let l1_dir = fresh_dir("retention-table-l1");
		let l2_root = fresh_dir("retention-table-l2");
		let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&l2_root).unwrap());
		let io = spawn_io_thread(64).unwrap();
		let mgr = Arc::new(
			SlabManager::open(SlabManagerConfig {
				dir: l1_dir,
				max_slab_bytes: 1 << 20,
				io,
				l2: Some(l2.clone()),
				rehydration: RehydrationPolicy::default(),
				l2_column_fetch: Default::default(),
				row_group_writer: Default::default(),
				l2_row_group_fetch: Default::default(),
				encoding_writer: Default::default(),
				l2_chunk_layout: Default::default(),
				l2_encoding_fetch: Default::default(),
				encoding_compute: Default::default(),
				group_commit: GroupCommitConfig::default(),
				crypto: test_crypto(),
			})
			.await
			.unwrap(),
		);
		let descriptor = append_only_retention_descriptor(TableObjectId::new([0x49; 16]));
		let (schemas, lifecycle) = fixed_table_retention_catalog(&descriptor, 60_000);
		let old_key = id(0x4901);
		let live_key = id(0x4902);
		let frame = encode_test_append_only_storage_frame(
			descriptor.table_object_id,
			&[
				(old_key, Locality::new(21), hlc(30_000), 100, false),
				(live_key, Locality::new(22), hlc(50_000), 200, false),
			],
		);
		let entries = storage_multirow_index_entries_from_frame(&frame)
			.unwrap()
			.expect("append-only storage entries");
		mgr.append_durable_multirow_with_locality(tenant(), &frame, &entries)
			.await
			.unwrap();

		let slab_id = force_seal_active(&mgr).await;
		let raw = mgr.read_sealed_raw(slab_id).await.unwrap();
		let old_encrypted = mgr
			.encrypt_tiered_object_blob_with_table_stats(slab_id, &raw, &schemas)
			.unwrap();
		let old_footer = mgr
			.encrypted_tiered_footer_summary(&old_encrypted.bytes)
			.unwrap()
			.expect("encrypted footer");
		let old_uri = l2
			.put(
				&format!("tier/slab_{slab_id:016x}-table-retention.wnd"),
				&old_encrypted.bytes,
			)
			.await
			.unwrap();
		let old_pointer = TieredSlabPointer {
			slab_id,
			l2_uri: old_uri,
			payload_hash: crate::hash::content::digest(&old_encrypted.bytes),
			is_encrypted: true,
			tenant_id: Some(tenant()),
			tenant_kek_id: Some(old_footer.tenant_kek_id),
			stats_format_version: old_encrypted
				.stats_locator
				.map(|locator| locator.stats_format_version),
			stats_block_len: old_encrypted
				.stats_locator
				.map(|locator| locator.stats_block_len),
			stats_block_hash128: old_encrypted
				.stats_locator
				.map(|locator| locator.stats_block_hash128),
			tombstone_metadata: TombstoneMetadata::None,
			supersession_metadata: SupersessionMetadata::None,
		};
		mgr.apply_tier_slab(old_pointer.clone()).await.unwrap();

		let prepared = mgr
			.prepare_tiered_table_retention_rewrite(
				&old_pointer,
				MvccRetentionPolicy::Disabled,
				&lifecycle,
				&schemas,
				100_000,
			)
			.await
			.unwrap()
			.expect("table retention rewrite");
		let PreparedTieredRetentionRewrite::Rewrite {
			encrypted_bytes,
			pruned_versions,
			..
		} = prepared
		else {
			panic!("expected rewrite with live row survivor");
		};
		assert_eq!(pruned_versions, 1);

		let rewritten_pointer = TieredSlabPointer {
			slab_id,
			l2_uri: "rewritten-table-retention".to_string(),
			payload_hash: crate::hash::content::digest(&encrypted_bytes),
			is_encrypted: true,
			tenant_id: Some(tenant()),
			tenant_kek_id: Some(old_footer.tenant_kek_id),
			stats_format_version: old_encrypted
				.stats_locator
				.map(|locator| locator.stats_format_version),
			stats_block_len: old_encrypted
				.stats_locator
				.map(|locator| locator.stats_block_len),
			stats_block_hash128: old_encrypted
				.stats_locator
				.map(|locator| locator.stats_block_hash128),
			tombstone_metadata: TombstoneMetadata::None,
			supersession_metadata: SupersessionMetadata::None,
		};
		let rewritten_plaintext = decode_tiered_blob(
			mgr.config.crypto.as_ref(),
			&rewritten_pointer,
			&encrypted_bytes,
		)
		.unwrap();
		let recovered = recover_slab_entries_from_blob(&rewritten_plaintext, slab_id).unwrap();
		assert_eq!(recovered.len(), 1);
		assert_eq!(recovered[0].key_identity, live_key);
	}

	#[tokio::test]
	async fn tiered_table_retention_reports_lag_without_rewrite_for_pair_waiting_on_tombstone() {
		let l1_dir = fresh_dir("retention-table-lag-l1");
		let l2_root = fresh_dir("retention-table-lag-l2");
		let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&l2_root).unwrap());
		let io = spawn_io_thread(64).unwrap();
		let mgr = Arc::new(
			SlabManager::open(SlabManagerConfig {
				dir: l1_dir,
				max_slab_bytes: 1 << 20,
				io,
				l2: Some(l2.clone()),
				rehydration: RehydrationPolicy::default(),
				l2_column_fetch: Default::default(),
				row_group_writer: Default::default(),
				l2_row_group_fetch: Default::default(),
				encoding_writer: Default::default(),
				l2_chunk_layout: Default::default(),
				l2_encoding_fetch: Default::default(),
				encoding_compute: Default::default(),
				group_commit: GroupCommitConfig::default(),
				crypto: test_crypto(),
			})
			.await
			.unwrap(),
		);
		let descriptor = append_only_retention_descriptor(TableObjectId::new([0x4C; 16]));
		let (schemas, lifecycle) = fixed_table_retention_catalog(&descriptor, 60_000);
		let retained_key = id(0x4C01);
		let frame = encode_test_append_only_storage_frame(
			descriptor.table_object_id,
			&[(retained_key, Locality::new(34), hlc(30_000), 200, false)],
		);
		let entries = storage_multirow_index_entries_from_frame(&frame)
			.unwrap()
			.expect("append-only storage entries");
		mgr.append_durable_multirow_with_locality(tenant(), &frame, &entries)
			.await
			.unwrap();
		let (tombstone_frame, tombstone_entries) = encode_test_row_tombstone_marker_frame(
			descriptor.table_object_id,
			retained_key,
			Locality::new(34),
			hlc(50_000),
		);
		mgr.append_durable_multirow_with_locality(tenant(), &tombstone_frame, &tombstone_entries)
			.await
			.unwrap();

		let slab_id = force_seal_active(&mgr).await;
		let raw = mgr.read_sealed_raw(slab_id).await.unwrap();
		let old_encrypted = mgr.encrypt_tiered_object_blob(slab_id, &raw).unwrap();
		let old_uri = l2
			.put(
				&format!("tier/slab_{slab_id:016x}-table-retention-lag.wnd"),
				&old_encrypted.bytes,
			)
			.await
			.unwrap();
		let old_pointer = TieredSlabPointer {
			slab_id,
			l2_uri: old_uri,
			payload_hash: crate::hash::content::digest(&old_encrypted.bytes),
			is_encrypted: true,
			tenant_id: Some(tenant()),
			tenant_kek_id: Some(old_encrypted.tenant_kek_id),
			stats_format_version: None,
			stats_block_len: None,
			stats_block_hash128: None,
			tombstone_metadata: TombstoneMetadata::None,
			supersession_metadata: SupersessionMetadata::None,
		};
		mgr.apply_tier_slab(old_pointer.clone()).await.unwrap();

		let prepared = mgr
			.prepare_tiered_table_retention_rewrite(
				&old_pointer,
				MvccRetentionPolicy::Disabled,
				&lifecycle,
				&schemas,
				100_000,
			)
			.await
			.unwrap()
			.expect("retention observation");
		let PreparedTieredRetentionRewrite::Observe { retention_stats } = prepared else {
			panic!("expected lag-only observation");
		};
		assert_eq!(retention_stats.pruned_data_rows, 0);
		assert_eq!(retention_stats.pruned_tombstone_rows, 0);
		assert_eq!(retention_stats.max_lag_seconds, 70);
	}

	#[tokio::test]
	async fn sealed_layout_recluster_single_to_multi_preserves_reads() {
		let mgr = fresh_manager().await;
		let single_descriptor = layout_recluster_descriptor(0x62, 1, RecordLayoutPolicy::SingleRow);
		let multi_descriptor = layout_recluster_descriptor(
			0x62,
			2,
			RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::new(4, 4, 1024 * 1024).unwrap()),
		);
		let batch = layout_recluster_batch(&[1, 2, 3, 4]);
		let meta = layout_recluster_meta(&single_descriptor, batch.num_rows());
		let admission = validate_table_write_request_batch(
			&meta,
			&batch,
			&single_descriptor,
			WriterNodeId::ZERO,
			meta.client_hlc,
		)
		.unwrap();
		for row in 0..batch.num_rows() {
			let row_batch = batch.slice(row, 1);
			let frame = encode_test_batch(&row_batch);
			mgr.append_durable_with_locality(
				tenant(),
				admission.key_identities[row],
				admission.localities[row],
				hlc(10 + row as u64),
				&frame,
			)
			.await
			.unwrap();
		}
		let old_slab_id = force_seal_active(&mgr).await;

		let report = mgr
			.recluster_sealed_table_layout(tenant(), &multi_descriptor, 10)
			.await
			.unwrap();

		assert_eq!(report.rewritten_slabs, 1);
		assert_eq!(report.relocated_rows, batch.num_rows());
		let mut offsets = BTreeSet::new();
		for (row, expected) in [1, 2, 3, 4].into_iter().enumerate() {
			let location = mgr
				.lookup_as_of(tenant(), admission.key_identities[row], hlc(100))
				.expect("reclustered row");
			assert_ne!(location.slab_id, old_slab_id);
			offsets.insert(location.offset);
			let frame = mgr.read(location).await.unwrap();
			let storage_batch = decode_record_batch(&frame).unwrap();
			assert!(has_storage_synthetic_columns(&storage_batch));
			let user_batch =
				materialize_record_location_batch(&storage_batch, location.record_row_index)
					.unwrap();
			assert_eq!(first_i64(&user_batch), expected);
		}
		assert_eq!(
			offsets.len(),
			batch.num_rows(),
			"the test descriptor maps each primary key into a distinct routing bucket"
		);

		let second = mgr
			.recluster_sealed_table_layout(tenant(), &multi_descriptor, 10)
			.await
			.unwrap();
		assert_eq!(second, LayoutReclusterReport::default());
	}

	#[tokio::test]
	async fn sealed_layout_recluster_multi_to_single_preserves_reads() {
		let mgr = fresh_manager().await;
		let multi_descriptor = layout_recluster_descriptor(
			0x63,
			1,
			RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::new(4, 4, 1024 * 1024).unwrap()),
		);
		let single_descriptor = layout_recluster_descriptor(0x63, 2, RecordLayoutPolicy::SingleRow);
		let batch = layout_recluster_batch(&[1, 2, 3, 4]);
		let meta = layout_recluster_meta(&multi_descriptor, batch.num_rows());
		let admission = validate_table_write_request_batch(
			&meta,
			&batch,
			&multi_descriptor,
			WriterNodeId::ZERO,
			meta.client_hlc,
		)
		.unwrap();
		let packed = pack_record_batch_by_bucket(
			&meta,
			&multi_descriptor,
			&batch,
			WriterNodeId::ZERO,
			hlc(10),
		)
		.unwrap();
		for record in packed {
			let entries = storage_multirow_index_entries_from_frame(&record.frame_bytes)
				.unwrap()
				.expect("multi-row entries");
			mgr.append_durable_multirow_with_locality(tenant(), &record.frame_bytes, &entries)
				.await
				.unwrap();
		}
		let old_slab_id = force_seal_active(&mgr).await;

		let report = mgr
			.recluster_sealed_table_layout(tenant(), &single_descriptor, 10)
			.await
			.unwrap();

		assert_eq!(report.rewritten_slabs, 1);
		assert_eq!(report.relocated_rows, batch.num_rows());
		let mut offsets = BTreeSet::new();
		for (row, expected) in [1, 2, 3, 4].into_iter().enumerate() {
			let location = mgr
				.lookup_as_of(tenant(), admission.key_identities[row], hlc(100))
				.expect("reclustered row");
			assert_ne!(location.slab_id, old_slab_id);
			assert_eq!(location.record_row_index, RecordRowIndex::ZERO);
			offsets.insert(location.offset);
			let frame = mgr.read(location).await.unwrap();
			let user_batch = decode_record_batch(&frame).unwrap();
			assert!(!has_storage_synthetic_columns(&user_batch));
			assert_eq!(first_i64(&user_batch), expected);
		}
		assert_eq!(
			offsets.len(),
			batch.num_rows(),
			"multi-row rollback should materialize one physical record per row"
		);
	}

	async fn assert_sealed_layout_recluster_updates_multirow_codec(
		object_byte: u8,
		from_policy: RecordBodyCompressionPolicy,
		to_policy: RecordBodyCompressionPolicy,
	) {
		let mgr = fresh_manager().await;
		let layout = MultiRowLayoutDescriptor::new(4, 4, 1024 * 1024).unwrap();
		let source_descriptor = layout_recluster_descriptor_with_codec(
			object_byte,
			1,
			RecordLayoutPolicy::MultiRow(layout),
			from_policy,
		);
		let target_descriptor = layout_recluster_descriptor_with_codec(
			object_byte,
			2,
			RecordLayoutPolicy::MultiRow(layout),
			to_policy,
		);
		let batch = layout_recluster_batch(&[1, 2, 3, 4]);
		let meta = layout_recluster_meta(&source_descriptor, batch.num_rows());
		let admission = validate_table_write_request_batch(
			&meta,
			&batch,
			&source_descriptor,
			WriterNodeId::ZERO,
			meta.client_hlc,
		)
		.unwrap();
		let source_rows = admission
			.key_identities
			.iter()
			.zip(admission.localities.iter())
			.enumerate()
			.map(
				|(source_row_index, (key_identity, routing_locality))| MultiRowSourceRow {
					source_row_index,
					key_identity: *key_identity,
					routing_locality: *routing_locality,
					row_hlc: hlc(10 + source_row_index as u64),
					is_tombstone: false,
					table_object_id: None,
				},
			)
			.collect::<Vec<_>>();
		let encoded =
			encode_multirow_from_survivors_with_policy(&batch, &source_rows, from_policy).unwrap();
		assert_eq!(
			crate::storage::ipc::record_batch_compression_policy_from_ipc(&encoded.frame).unwrap(),
			from_policy
		);
		let encoded_entries = encoded
			.rows
			.iter()
			.copied()
			.map(Into::into)
			.collect::<Vec<_>>();
		mgr.append_durable_multirow_with_locality(tenant(), &encoded.frame, &encoded_entries)
			.await
			.unwrap();
		let old_slab_id = force_seal_active(&mgr).await;

		let report = mgr
			.recluster_sealed_table_layout(tenant(), &target_descriptor, 10)
			.await
			.unwrap();

		assert_eq!(report.rewritten_slabs, 1);
		assert_eq!(report.relocated_rows, batch.num_rows());
		for (row, expected) in [1, 2, 3, 4].into_iter().enumerate() {
			let location = mgr
				.lookup_as_of(tenant(), admission.key_identities[row], hlc(100))
				.expect("reclustered row");
			assert_ne!(location.slab_id, old_slab_id);
			let frame = mgr.read(location).await.unwrap();
			assert_eq!(
				crate::storage::ipc::record_batch_compression_policy_from_ipc(&frame).unwrap(),
				to_policy
			);
			let storage_batch = decode_record_batch(&frame).unwrap();
			assert!(has_storage_synthetic_columns(&storage_batch));
			let user_batch =
				materialize_record_location_batch(&storage_batch, location.record_row_index)
					.unwrap();
			assert_eq!(first_i64(&user_batch), expected);
		}

		let second = mgr
			.recluster_sealed_table_layout(tenant(), &target_descriptor, 10)
			.await
			.unwrap();
		assert_eq!(second, LayoutReclusterReport::default());
	}

	#[tokio::test]
	async fn sealed_layout_recluster_same_layout_updates_multirow_codec() {
		assert_sealed_layout_recluster_updates_multirow_codec(
			0x64,
			RecordBodyCompressionPolicy::Uncompressed,
			RecordBodyCompressionPolicy::Lz4Frame,
		)
		.await;
		assert_sealed_layout_recluster_updates_multirow_codec(
			0x65,
			RecordBodyCompressionPolicy::Lz4Frame,
			RecordBodyCompressionPolicy::Uncompressed,
		)
		.await;
	}

	#[tokio::test]
	async fn active_slab_reads_mixed_record_body_compression() {
		let mgr = fresh_manager().await;
		let uncompressed_descriptor = layout_recluster_descriptor_with_codec(
			0x66,
			1,
			RecordLayoutPolicy::SingleRow,
			RecordBodyCompressionPolicy::Uncompressed,
		);
		let lz4_descriptor = layout_recluster_descriptor_with_codec(
			0x66,
			2,
			RecordLayoutPolicy::SingleRow,
			RecordBodyCompressionPolicy::Lz4Frame,
		);

		let mut inserted = Vec::new();
		for (descriptor, value, ts) in [
			(&uncompressed_descriptor, 7_i64, hlc(10)),
			(&lz4_descriptor, 8_i64, hlc(20)),
		] {
			let batch = layout_recluster_batch(&[value]);
			let meta = layout_recluster_meta(descriptor, batch.num_rows());
			let admission = validate_table_write_request_batch(
				&meta,
				&batch,
				descriptor,
				WriterNodeId::ZERO,
				meta.client_hlc,
			)
			.unwrap();
			let frame = encode_batch_with_policy(&batch, descriptor.record_body_compression_policy)
				.unwrap()
				.bytes;
			let location = mgr
				.append_durable_with_locality(
					tenant(),
					admission.key_identities[0],
					admission.localities[0],
					ts,
					&frame,
				)
				.await
				.unwrap();
			inserted.push((
				admission.key_identities[0],
				location,
				value,
				descriptor.record_body_compression_policy,
			));
		}

		assert_eq!(inserted[0].1.slab_id, inserted[1].1.slab_id);
		for (key_identity, location, expected_value, expected_policy) in inserted {
			let found = mgr
				.lookup_as_of(tenant(), key_identity, hlc(100))
				.expect("mixed-codec row");
			assert_eq!(found, location);
			let frame = mgr.read(found).await.unwrap();
			assert_eq!(
				crate::storage::ipc::record_batch_compression_policy_from_ipc(&frame).unwrap(),
				expected_policy
			);
			let batch = decode_record_batch(&frame).unwrap();
			assert!(!has_storage_synthetic_columns(&batch));
			assert_eq!(first_i64(&batch), expected_value);
		}
	}

	#[tokio::test]
	async fn recovery_reopens_pk_delete_and_resurrection_chain() {
		let dir = fresh_dir("pk-delete-recovery");
		let io = spawn_io_thread(64).unwrap();
		let crypto = test_crypto();
		let descriptor = pk_retention_descriptor(TableObjectId::new([0x57; 16]));
		let key = id(0x5701);
		let locality = Locality::new(57);

		let manager = open_manager_at_dir(dir.clone(), io.clone(), crypto.clone()).await;
		let data_frame = encode_test_append_only_storage_frame(
			descriptor.table_object_id,
			&[(key, locality, hlc(10), 100, false)],
		);
		let data_entries = storage_multirow_index_entries_from_frame(&data_frame)
			.unwrap()
			.expect("pk data entry");
		manager
			.append_durable_multirow_with_locality(tenant(), &data_frame, &data_entries)
			.await
			.unwrap();
		let (tombstone_frame, tombstone_entries) =
			encode_test_pk_tombstone_marker_frame(&descriptor, key, locality, hlc(20));
		manager
			.append_durable_multirow_with_locality(tenant(), &tombstone_frame, &tombstone_entries)
			.await
			.unwrap();
		manager.fsync_all_local_slabs().await.unwrap();
		drop(manager);

		let reopened = open_manager_at_dir(dir.clone(), io.clone(), crypto.clone()).await;
		assert_eq!(
			reopened.routing_locality_for_existing_key(tenant(), key),
			Some(locality)
		);
		assert!(
			!reopened
				.lookup_as_of(tenant(), key, hlc(10))
				.expect("pre-delete data survives reopen")
				.is_tombstone
		);
		assert!(
			reopened
				.lookup_as_of(tenant(), key, hlc(20))
				.expect("delete tombstone survives reopen")
				.is_tombstone
		);
		assert!(
			reopened
				.lookup(tenant(), key)
				.expect("latest delete tombstone survives reopen")
				.is_tombstone
		);

		let resurrected_frame = encode_test_append_only_storage_frame(
			descriptor.table_object_id,
			&[(key, locality, hlc(30), 200, false)],
		);
		let resurrected_entries = storage_multirow_index_entries_from_frame(&resurrected_frame)
			.unwrap()
			.expect("pk resurrected data entry");
		reopened
			.append_durable_multirow_with_locality(
				tenant(),
				&resurrected_frame,
				&resurrected_entries,
			)
			.await
			.unwrap();
		reopened.fsync_all_local_slabs().await.unwrap();
		drop(reopened);

		let reopened = open_manager_at_dir(dir, io, crypto).await;
		assert!(
			reopened
				.lookup_as_of(tenant(), key, hlc(20))
				.expect("historical delete tombstone survives resurrection reopen")
				.is_tombstone
		);
		let latest = reopened
			.lookup_as_of(tenant(), key, hlc(30))
			.expect("resurrected data survives reopen");
		assert!(!latest.is_tombstone);
		let frame = reopened.read(latest).await.unwrap();
		let batch = decode_record_batch(&frame).unwrap();
		let values = batch
			.column(0)
			.as_any()
			.downcast_ref::<UInt64Array>()
			.unwrap();
		assert_eq!(values.value(0), 200);
	}

	#[tokio::test]
	async fn recovery_fails_closed_on_cross_slab_consecutive_pk_tombstones() {
		let dir = fresh_dir("pk-consecutive-tombstone-recovery");
		let io = spawn_io_thread(64).unwrap();
		let crypto = test_crypto();
		let descriptor = pk_retention_descriptor(TableObjectId::new([0x59; 16]));
		let key = id(0x5901);
		let locality = Locality::new(59);

		let manager = open_manager_at_dir(dir.clone(), io.clone(), crypto.clone()).await;
		let data_frame = encode_test_append_only_storage_frame(
			descriptor.table_object_id,
			&[(key, locality, hlc(10), 100, false)],
		);
		let data_entries = storage_multirow_index_entries_from_frame(&data_frame)
			.unwrap()
			.expect("pk data entry");
		manager
			.append_durable_multirow_with_locality(tenant(), &data_frame, &data_entries)
			.await
			.unwrap();
		let (first_tombstone_frame, first_tombstone_entries) =
			encode_test_pk_tombstone_marker_frame(&descriptor, key, locality, hlc(20));
		manager
			.append_durable_multirow_with_locality(
				tenant(),
				&first_tombstone_frame,
				&first_tombstone_entries,
			)
			.await
			.unwrap();
		force_seal_active(&manager).await;
		let (second_tombstone_frame, second_tombstone_entries) =
			encode_test_pk_tombstone_marker_frame(&descriptor, key, locality, hlc(30));
		manager
			.append_durable_multirow_with_locality(
				tenant(),
				&second_tombstone_frame,
				&second_tombstone_entries,
			)
			.await
			.unwrap();
		manager.fsync_all_local_slabs().await.unwrap();
		drop(manager);

		let reopened = SlabManager::open(SlabManagerConfig {
			dir,
			max_slab_bytes: 1024 * 1024,
			io,
			l2: None,
			rehydration: RehydrationPolicy::default(),
			l2_column_fetch: Default::default(),
			row_group_writer: Default::default(),
			l2_row_group_fetch: Default::default(),
			encoding_writer: Default::default(),
			l2_chunk_layout: Default::default(),
			l2_encoding_fetch: Default::default(),
			encoding_compute: Default::default(),
			group_commit: GroupCommitConfig::default(),
			crypto,
		})
		.await;
		let err = match reopened {
			Ok(_) => panic!("consecutive tombstones across slabs must fail recovery"),
			Err(err) => err,
		};
		assert_eq!(err.kind(), ErrorKind::InvalidData);
		assert!(err.to_string().contains("duplicate row tombstone"));
	}

	#[tokio::test]
	async fn recovery_reopens_mixed_record_body_compression_slab() {
		let dir = fresh_dir("mixed-compression-recovery");
		let mgr = Arc::new(
			SlabManager::open(SlabManagerConfig {
				dir: dir.clone(),
				max_slab_bytes: 1024 * 1024,
				io: spawn_io_thread(64).unwrap(),
				l2: None,
				rehydration: RehydrationPolicy::default(),
				l2_column_fetch: Default::default(),
				row_group_writer: Default::default(),
				l2_row_group_fetch: Default::default(),
				encoding_writer: Default::default(),
				l2_chunk_layout: Default::default(),
				l2_encoding_fetch: Default::default(),
				encoding_compute: Default::default(),
				group_commit: GroupCommitConfig::default(),
				crypto: test_crypto(),
			})
			.await
			.unwrap(),
		);
		let uncompressed_descriptor = layout_recluster_descriptor_with_codec(
			0x68,
			1,
			RecordLayoutPolicy::SingleRow,
			RecordBodyCompressionPolicy::Uncompressed,
		);
		let lz4_descriptor = layout_recluster_descriptor_with_codec(
			0x68,
			2,
			RecordLayoutPolicy::SingleRow,
			RecordBodyCompressionPolicy::Lz4Frame,
		);

		let mut inserted = Vec::new();
		for (descriptor, value, ts) in [
			(&uncompressed_descriptor, 17_i64, hlc(10)),
			(&lz4_descriptor, 18_i64, hlc(20)),
		] {
			let batch = layout_recluster_batch(&[value]);
			let meta = layout_recluster_meta(descriptor, batch.num_rows());
			let admission = validate_table_write_request_batch(
				&meta,
				&batch,
				descriptor,
				WriterNodeId::ZERO,
				meta.client_hlc,
			)
			.unwrap();
			let frame = encode_batch_with_policy(&batch, descriptor.record_body_compression_policy)
				.unwrap()
				.bytes;
			let location = mgr
				.append_durable_with_locality(
					tenant(),
					admission.key_identities[0],
					admission.localities[0],
					ts,
					&frame,
				)
				.await
				.unwrap();
			inserted.push((
				admission.key_identities[0],
				location,
				value,
				descriptor.record_body_compression_policy,
			));
		}
		assert_eq!(inserted[0].1.slab_id, inserted[1].1.slab_id);
		mgr.fsync_all_local_slabs().await.unwrap();
		drop(mgr);

		let reopened = SlabManager::open(SlabManagerConfig {
			dir,
			max_slab_bytes: 1024 * 1024,
			io: spawn_io_thread(64).unwrap(),
			l2: None,
			rehydration: RehydrationPolicy::default(),
			l2_column_fetch: Default::default(),
			row_group_writer: Default::default(),
			l2_row_group_fetch: Default::default(),
			encoding_writer: Default::default(),
			l2_chunk_layout: Default::default(),
			l2_encoding_fetch: Default::default(),
			encoding_compute: Default::default(),
			group_commit: GroupCommitConfig::default(),
			crypto: test_crypto(),
		})
		.await
		.unwrap();
		for (key_identity, location, expected_value, expected_policy) in inserted {
			let found = reopened
				.lookup_as_of(tenant(), key_identity, hlc(100))
				.expect("recovered mixed-codec row");
			assert_eq!(found, location);
			let frame = reopened.read(found).await.unwrap();
			assert_eq!(
				crate::storage::ipc::record_batch_compression_policy_from_ipc(&frame).unwrap(),
				expected_policy
			);
			let batch = decode_record_batch(&frame).unwrap();
			assert_eq!(first_i64(&batch), expected_value);
		}
	}

	#[tokio::test]
	async fn l2_reads_mixed_record_body_compression_slab() {
		let l1_dir = fresh_dir("mixed-compression-l2-l1");
		let l2_dir = fresh_dir("mixed-compression-l2-objects");
		let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&l2_dir).unwrap());
		let mgr = Arc::new(
			SlabManager::open(SlabManagerConfig {
				dir: l1_dir.clone(),
				max_slab_bytes: 1024 * 1024,
				io: spawn_io_thread(64).unwrap(),
				l2: Some(l2.clone()),
				rehydration: RehydrationPolicy::default(),
				l2_column_fetch: Default::default(),
				row_group_writer: Default::default(),
				l2_row_group_fetch: Default::default(),
				encoding_writer: Default::default(),
				l2_chunk_layout: Default::default(),
				l2_encoding_fetch: Default::default(),
				encoding_compute: Default::default(),
				group_commit: GroupCommitConfig::default(),
				crypto: test_crypto(),
			})
			.await
			.unwrap(),
		);
		let uncompressed_descriptor = layout_recluster_descriptor_with_codec(
			0x69,
			1,
			RecordLayoutPolicy::SingleRow,
			RecordBodyCompressionPolicy::Uncompressed,
		);
		let lz4_descriptor = layout_recluster_descriptor_with_codec(
			0x69,
			2,
			RecordLayoutPolicy::SingleRow,
			RecordBodyCompressionPolicy::Lz4Frame,
		);

		let mut inserted = Vec::new();
		for (descriptor, value, ts) in [
			(&uncompressed_descriptor, 27_i64, hlc(10)),
			(&lz4_descriptor, 28_i64, hlc(20)),
		] {
			let batch = layout_recluster_batch(&[value]);
			let meta = layout_recluster_meta(descriptor, batch.num_rows());
			let admission = validate_table_write_request_batch(
				&meta,
				&batch,
				descriptor,
				WriterNodeId::ZERO,
				meta.client_hlc,
			)
			.unwrap();
			let frame = encode_batch_with_policy(&batch, descriptor.record_body_compression_policy)
				.unwrap()
				.bytes;
			let location = mgr
				.append_durable_with_locality(
					tenant(),
					admission.key_identities[0],
					admission.localities[0],
					ts,
					&frame,
				)
				.await
				.unwrap();
			inserted.push((
				admission.key_identities[0],
				location,
				value,
				descriptor.record_body_compression_policy,
				frame.len() as u64,
			));
		}
		let sealed_id = force_seal_active(&mgr).await;
		let raw = mgr.read_sealed_raw(sealed_id).await.unwrap();
		let encrypted = mgr.encrypt_tiered_object_blob(sealed_id, &raw).unwrap();
		let payload_hash = crate::hash::content::digest(&encrypted.bytes);
		let uri = l2
			.put(
				&format!("mixed-compression/slab_{sealed_id:016x}.wnd"),
				&encrypted.bytes,
			)
			.await
			.unwrap();
		mgr.apply_tier_slab(TieredSlabPointer {
			slab_id: sealed_id,
			l2_uri: uri,
			payload_hash,
			is_encrypted: true,
			tenant_id: Some(tenant()),
			tenant_kek_id: Some(encrypted.tenant_kek_id),
			stats_format_version: encrypted
				.stats_locator
				.map(|locator| locator.stats_format_version),
			stats_block_len: encrypted
				.stats_locator
				.map(|locator| locator.stats_block_len),
			stats_block_hash128: encrypted
				.stats_locator
				.map(|locator| locator.stats_block_hash128),
			tombstone_metadata: TombstoneMetadata::None,
			supersession_metadata: SupersessionMetadata::None,
		})
		.await
		.unwrap();
		assert!(mgr.is_tiered(sealed_id).await);
		assert!(
			!slab_path(&l1_dir, sealed_id).exists(),
			"tiered slab should be unlinked from L1 before L2 reads"
		);

		let uncompressed_fetch_before =
			storage_compression_l2_fetch_metric_value(RecordBodyCompressionPolicy::Uncompressed);
		let lz4_fetch_before =
			storage_compression_l2_fetch_metric_value(RecordBodyCompressionPolicy::Lz4Frame);
		let mut expected_uncompressed_fetch_bytes = 0_u64;
		let mut expected_lz4_fetch_bytes = 0_u64;

		for (key_identity, location, expected_value, expected_policy, expected_frame_bytes) in
			inserted
		{
			let found = mgr
				.lookup_as_of(tenant(), key_identity, hlc(100))
				.expect("tiered mixed-codec row");
			assert_eq!(found, location);
			let frame = mgr.read(found).await.unwrap();
			assert_eq!(
				crate::storage::ipc::record_batch_compression_policy_from_ipc(&frame).unwrap(),
				expected_policy
			);
			let batch = decode_record_batch(&frame).unwrap();
			assert_eq!(first_i64(&batch), expected_value);
			match expected_policy {
				RecordBodyCompressionPolicy::Uncompressed => {
					expected_uncompressed_fetch_bytes =
						expected_uncompressed_fetch_bytes.saturating_add(expected_frame_bytes);
				}
				RecordBodyCompressionPolicy::Lz4Frame => {
					expected_lz4_fetch_bytes =
						expected_lz4_fetch_bytes.saturating_add(expected_frame_bytes);
				}
			}
		}
		assert!(
			storage_compression_l2_fetch_metric_value(RecordBodyCompressionPolicy::Uncompressed)
				>= uncompressed_fetch_before.saturating_add(expected_uncompressed_fetch_bytes)
		);
		assert!(
			storage_compression_l2_fetch_metric_value(RecordBodyCompressionPolicy::Lz4Frame)
				>= lz4_fetch_before.saturating_add(expected_lz4_fetch_bytes)
		);
	}

	#[tokio::test]
	async fn compression_resident_gauge_tracks_sealed_recluster_convergence() {
		let mgr = fresh_manager().await;
		let layout = MultiRowLayoutDescriptor::new(4, 4, 1024 * 1024).unwrap();
		let source_descriptor = layout_recluster_descriptor_with_codec(
			0x67,
			1,
			RecordLayoutPolicy::MultiRow(layout),
			RecordBodyCompressionPolicy::Uncompressed,
		);
		let target_descriptor = layout_recluster_descriptor_with_codec(
			0x67,
			2,
			RecordLayoutPolicy::MultiRow(layout),
			RecordBodyCompressionPolicy::Lz4Frame,
		);
		let batch = layout_recluster_batch(&[1, 2, 3, 4]);
		let meta = layout_recluster_meta(&source_descriptor, batch.num_rows());
		let admission = validate_table_write_request_batch(
			&meta,
			&batch,
			&source_descriptor,
			WriterNodeId::ZERO,
			meta.client_hlc,
		)
		.unwrap();
		let source_rows = admission
			.key_identities
			.iter()
			.zip(admission.localities.iter())
			.enumerate()
			.map(
				|(source_row_index, (key_identity, routing_locality))| MultiRowSourceRow {
					source_row_index,
					key_identity: *key_identity,
					routing_locality: *routing_locality,
					row_hlc: hlc(10 + source_row_index as u64),
					is_tombstone: false,
					table_object_id: None,
				},
			)
			.collect::<Vec<_>>();
		let encoded = encode_multirow_from_survivors_with_policy(
			&batch,
			&source_rows,
			RecordBodyCompressionPolicy::Uncompressed,
		)
		.unwrap();
		let encoded_entries = encoded
			.rows
			.iter()
			.copied()
			.map(Into::into)
			.collect::<Vec<_>>();
		mgr.append_durable_multirow_with_locality(tenant(), &encoded.frame, &encoded_entries)
			.await
			.unwrap();
		force_seal_active(&mgr).await;

		let mut catalog = TableSchemaCatalogV1::default();
		catalog.upsert(tenant(), target_descriptor.clone()).unwrap();
		let resident = mgr
			.refresh_storage_compression_uncompressed_resident_bytes(&catalog)
			.await
			.unwrap();
		assert_eq!(resident, encoded.frame.len() as u64);
		assert_eq!(
			metrics()
				.storage_compression_uncompressed_resident_bytes
				.get(),
			resident
		);

		let report = mgr
			.recluster_sealed_table_layout(tenant(), &target_descriptor, 10)
			.await
			.unwrap();
		assert_eq!(report.rewritten_slabs, 1);
		let resident = mgr
			.refresh_storage_compression_uncompressed_resident_bytes(&catalog)
			.await
			.unwrap();
		assert_eq!(resident, 0);
		assert_eq!(
			metrics()
				.storage_compression_uncompressed_resident_bytes
				.get(),
			0
		);
	}

	#[tokio::test]
	async fn compression_resident_gauge_counts_tiered_uncompressed_frames() {
		let l1_dir = fresh_dir("compression-gauge-tiered-l1");
		let l2_dir = fresh_dir("compression-gauge-tiered-l2");
		let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&l2_dir).unwrap());
		let mgr = Arc::new(
			SlabManager::open(SlabManagerConfig {
				dir: l1_dir.clone(),
				max_slab_bytes: 1024 * 1024,
				io: spawn_io_thread(64).unwrap(),
				l2: Some(l2.clone()),
				rehydration: RehydrationPolicy::default(),
				l2_column_fetch: Default::default(),
				row_group_writer: Default::default(),
				l2_row_group_fetch: Default::default(),
				encoding_writer: Default::default(),
				l2_chunk_layout: Default::default(),
				l2_encoding_fetch: Default::default(),
				encoding_compute: Default::default(),
				group_commit: GroupCommitConfig::default(),
				crypto: test_crypto(),
			})
			.await
			.unwrap(),
		);
		let layout = MultiRowLayoutDescriptor::new(4, 4, 1024 * 1024).unwrap();
		let source_descriptor = layout_recluster_descriptor_with_codec(
			0x6a,
			1,
			RecordLayoutPolicy::MultiRow(layout),
			RecordBodyCompressionPolicy::Uncompressed,
		);
		let target_descriptor = layout_recluster_descriptor_with_codec(
			0x6a,
			2,
			RecordLayoutPolicy::MultiRow(layout),
			RecordBodyCompressionPolicy::Lz4Frame,
		);
		let batch = layout_recluster_batch(&[31, 32, 33, 34]);
		let meta = layout_recluster_meta(&source_descriptor, batch.num_rows());
		let admission = validate_table_write_request_batch(
			&meta,
			&batch,
			&source_descriptor,
			WriterNodeId::ZERO,
			meta.client_hlc,
		)
		.unwrap();
		let source_rows = admission
			.key_identities
			.iter()
			.zip(admission.localities.iter())
			.enumerate()
			.map(
				|(source_row_index, (key_identity, routing_locality))| MultiRowSourceRow {
					source_row_index,
					key_identity: *key_identity,
					routing_locality: *routing_locality,
					row_hlc: hlc(50 + source_row_index as u64),
					is_tombstone: false,
					table_object_id: None,
				},
			)
			.collect::<Vec<_>>();
		let encoded = encode_multirow_from_survivors_with_policy(
			&batch,
			&source_rows,
			RecordBodyCompressionPolicy::Uncompressed,
		)
		.unwrap();
		let encoded_entries = encoded
			.rows
			.iter()
			.copied()
			.map(Into::into)
			.collect::<Vec<_>>();
		mgr.append_durable_multirow_with_locality(tenant(), &encoded.frame, &encoded_entries)
			.await
			.unwrap();
		let slab_id = force_seal_active(&mgr).await;
		let raw = mgr.read_sealed_raw(slab_id).await.unwrap();
		let encrypted = mgr.encrypt_tiered_object_blob(slab_id, &raw).unwrap();
		let payload_hash = crate::hash::content::digest(&encrypted.bytes);
		let uri = l2
			.put(
				&format!("compression-gauge/slab_{slab_id:016x}.wnd"),
				&encrypted.bytes,
			)
			.await
			.unwrap();
		mgr.apply_tier_slab(TieredSlabPointer {
			slab_id,
			l2_uri: uri,
			payload_hash,
			is_encrypted: true,
			tenant_id: Some(tenant()),
			tenant_kek_id: Some(encrypted.tenant_kek_id),
			stats_format_version: encrypted
				.stats_locator
				.map(|locator| locator.stats_format_version),
			stats_block_len: encrypted
				.stats_locator
				.map(|locator| locator.stats_block_len),
			stats_block_hash128: encrypted
				.stats_locator
				.map(|locator| locator.stats_block_hash128),
			tombstone_metadata: TombstoneMetadata::None,
			supersession_metadata: SupersessionMetadata::None,
		})
		.await
		.unwrap();
		assert!(
			!slab_path(&l1_dir, slab_id).exists(),
			"tiered gauge should be reading the L2 object, not the deleted L1 slab"
		);

		let mut catalog = TableSchemaCatalogV1::default();
		catalog.upsert(tenant(), target_descriptor).unwrap();
		let resident = mgr
			.refresh_storage_compression_uncompressed_resident_bytes(&catalog)
			.await
			.unwrap();
		assert_eq!(resident, encoded.frame.len() as u64);
		assert_eq!(
			metrics()
				.storage_compression_uncompressed_resident_bytes
				.get(),
			resident
		);
	}

	#[tokio::test]
	async fn projected_l2_read_materializes_sparse_multirow_record() {
		let l1_dir = fresh_dir("projected-l2-l1");
		let l2_dir = fresh_dir("projected-l2-l2");
		let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&l2_dir).unwrap());
		let mgr = Arc::new(
			SlabManager::open(SlabManagerConfig {
				dir: l1_dir.clone(),
				max_slab_bytes: 1024 * 1024 * 16,
				io: spawn_io_thread(64).unwrap(),
				l2: Some(l2.clone()),
				rehydration: RehydrationPolicy {
					promote_after_l2_reads: 0,
					..RehydrationPolicy::default()
				},
				l2_column_fetch: L2ColumnFetchConfig {
					enabled: true,
					min_record_bytes: RecordBodyByteLen::new(1),
					..L2ColumnFetchConfig::default()
				},
				row_group_writer: Default::default(),
				l2_row_group_fetch: Default::default(),
				encoding_writer: Default::default(),
				l2_chunk_layout: Default::default(),
				l2_encoding_fetch: Default::default(),
				encoding_compute: Default::default(),
				group_commit: GroupCommitConfig::default(),
				crypto: test_crypto(),
			})
			.await
			.unwrap(),
		);
		let mut fields = vec![Field::new("id", DataType::Int64, false)];
		for column in 1..16 {
			fields.push(Field::new(format!("c{column}"), DataType::Int64, false));
		}
		let schema = Schema::new(fields);
		let mut descriptor = TableSchemaDescriptorV1::new_with_layout(
			TableObjectId::new([0x72; 16]),
			1,
			"analytics",
			"projected_l2",
			&schema,
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
			LocalityPolicyV1::IdentityProjection,
			RecordLayoutPolicy::MultiRow(
				MultiRowLayoutDescriptor::new(20_000, 20_000, 8 * 1024 * 1024).unwrap(),
			),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.unwrap();
		descriptor.record_body_compression_policy = RecordBodyCompressionPolicy::Lz4Frame;
		descriptor.refresh_descriptor_fingerprints().unwrap();
		let row_count = 20_000_usize;
		let columns = (0..16)
			.map(|column| {
				Arc::new(Int64Array::from_iter_values((0..row_count).map(
					move |row| i64::try_from(column * 1_000_000 + row).unwrap(),
				))) as Arc<dyn Array>
			})
			.collect::<Vec<_>>();
		let batch = RecordBatch::try_new(Arc::new(schema), columns).unwrap();
		let meta = layout_recluster_meta(&descriptor, batch.num_rows());
		let admission = validate_table_write_request_batch(
			&meta,
			&batch,
			&descriptor,
			WriterNodeId::ZERO,
			meta.client_hlc,
		)
		.unwrap();
		let source_rows = admission
			.key_identities
			.iter()
			.zip(admission.localities.iter())
			.enumerate()
			.map(
				|(source_row_index, (key_identity, routing_locality))| MultiRowSourceRow {
					source_row_index,
					key_identity: *key_identity,
					routing_locality: *routing_locality,
					row_hlc: hlc(100 + source_row_index as u64),
					is_tombstone: false,
					table_object_id: None,
				},
			)
			.collect::<Vec<_>>();
		let encoded = encode_multirow_from_survivors_with_policy(
			&batch,
			&source_rows,
			RecordBodyCompressionPolicy::Lz4Frame,
		)
		.unwrap();
		let encoded_entries = encoded
			.rows
			.iter()
			.copied()
			.map(Into::into)
			.collect::<Vec<_>>();
		mgr.append_durable_multirow_with_locality(tenant(), &encoded.frame, &encoded_entries)
			.await
			.unwrap();
		let slab_id = force_seal_active(&mgr).await;
		let raw = mgr.read_sealed_raw(slab_id).await.unwrap();
		let encrypted = mgr.encrypt_tiered_object_blob(slab_id, &raw).unwrap();
		let payload_hash = crate::hash::content::digest(&encrypted.bytes);
		let uri = l2
			.put(
				&format!("projected-l2/slab_{slab_id:016x}.wnd"),
				&encrypted.bytes,
			)
			.await
			.unwrap();
		let pointer = TieredSlabPointer {
			slab_id,
			l2_uri: uri,
			payload_hash,
			is_encrypted: true,
			tenant_id: Some(tenant()),
			tenant_kek_id: Some(encrypted.tenant_kek_id),
			stats_format_version: encrypted
				.stats_locator
				.map(|locator| locator.stats_format_version),
			stats_block_len: encrypted
				.stats_locator
				.map(|locator| locator.stats_block_len),
			stats_block_hash128: encrypted
				.stats_locator
				.map(|locator| locator.stats_block_hash128),
			tombstone_metadata: TombstoneMetadata::None,
			supersession_metadata: SupersessionMetadata::None,
		};
		mgr.apply_tier_slab(pointer.clone()).await.unwrap();
		let location = mgr
			.lookup(tenant(), admission.key_identities[0])
			.expect("tiered row location");
		let requirement = ColumnFetchRequirement {
			output_columns: crate::storage::columnar::ColumnOrdinalSet::from_columns([15]),
			..ColumnFetchRequirement::default()
		};

		let projected = mgr
			.read_projected_record_from_l2(
				l2.clone(),
				&pointer,
				location,
				&descriptor,
				&requirement,
				None,
				None,
			)
			.await
			.unwrap();
		let full_frame = mgr
			.read_encrypted_record_from_l2(l2.as_ref(), &pointer, location)
			.await
			.unwrap();
		let whole = decode_record_batch(&full_frame).unwrap();

		assert!(matches!(projected.strategy, L2FetchStrategy::Columnar(_)));
		assert_eq!(projected.projected.batch, whole.project(&[15]).unwrap());
		assert_eq!(projected.projected.batch.num_columns(), 1);
		assert_eq!(projected.projected.batch.num_rows(), row_count);
	}

	#[tokio::test]
	async fn row_group_l2_point_read_materializes_projected_group() {
		let l1_dir = fresh_dir("row-group-l2-l1");
		let l2_dir = fresh_dir("row-group-l2-l2");
		let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&l2_dir).unwrap());
		let mgr = Arc::new(
			SlabManager::open(SlabManagerConfig {
				dir: l1_dir.clone(),
				max_slab_bytes: 1024 * 1024 * 16,
				io: spawn_io_thread(64).unwrap(),
				l2: Some(l2.clone()),
				rehydration: RehydrationPolicy {
					promote_after_l2_reads: 0,
					..RehydrationPolicy::default()
				},
				l2_column_fetch: L2ColumnFetchConfig {
					enabled: true,
					min_record_bytes: RecordBodyByteLen::new(1),
					..L2ColumnFetchConfig::default()
				},
				row_group_writer: Default::default(),
				l2_row_group_fetch: crate::storage::row_group::L2RowGroupFetchConfig {
					enabled: true,
					max_selected_row_group_fraction_micros:
						crate::storage::row_group::L2_ROW_GROUP_FETCH_FRACTION_DENOMINATOR - 1,
					..Default::default()
				},
				encoding_writer: Default::default(),
				l2_chunk_layout: Default::default(),
				l2_encoding_fetch: Default::default(),
				encoding_compute: Default::default(),
				group_commit: GroupCommitConfig::default(),
				crypto: test_crypto(),
			})
			.await
			.unwrap(),
		);
		let schema = Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("payload", DataType::Int64, false),
		]);
		let mut descriptor = TableSchemaDescriptorV1::new_with_layout(
			TableObjectId::new([0x73; 16]),
			1,
			"analytics",
			"row_group_l2",
			&schema,
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
			LocalityPolicyV1::IdentityProjection,
			RecordLayoutPolicy::MultiRow(
				MultiRowLayoutDescriptor::new(20_000, 20_000, 8 * 1024 * 1024).unwrap(),
			),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.unwrap();
		descriptor.record_body_compression_policy = RecordBodyCompressionPolicy::Lz4Frame;
		descriptor.refresh_descriptor_fingerprints().unwrap();
		let row_count = 20_000_usize;
		let batch = RecordBatch::try_new(
			Arc::new(schema),
			vec![
				Arc::new(Int64Array::from_iter_values(0..row_count as i64)) as Arc<dyn Array>,
				Arc::new(Int64Array::from_iter_values(
					(0..row_count).map(|row| 10_000_000_i64 + row as i64),
				)) as Arc<dyn Array>,
			],
		)
		.unwrap();
		let meta = layout_recluster_meta(&descriptor, batch.num_rows());
		let admission = validate_table_write_request_batch(
			&meta,
			&batch,
			&descriptor,
			WriterNodeId::ZERO,
			meta.client_hlc,
		)
		.unwrap();
		let source_rows = admission
			.key_identities
			.iter()
			.zip(admission.localities.iter())
			.enumerate()
			.map(
				|(source_row_index, (key_identity, routing_locality))| MultiRowSourceRow {
					source_row_index,
					key_identity: *key_identity,
					routing_locality: *routing_locality,
					row_hlc: hlc(200 + source_row_index as u64),
					is_tombstone: false,
					table_object_id: None,
				},
			)
			.collect::<Vec<_>>();
		let mut writer_config =
			crate::storage::row_group::RowGroupWriterConfig::point_read().with_enabled(true);
		writer_config.stats_supported_types = std::collections::BTreeSet::new();
		let encoded =
			crate::storage::multirow::encode_multirow_from_survivors_with_row_group_config(
				&batch,
				&source_rows,
				&descriptor,
				RecordBodyCompressionPolicy::Lz4Frame,
				&writer_config,
			)
			.unwrap();
		let encoded_entries = encoded
			.rows
			.iter()
			.copied()
			.map(Into::into)
			.collect::<Vec<_>>();
		mgr.append_durable_multirow_with_locality(tenant(), &encoded.frame, &encoded_entries)
			.await
			.unwrap();
		let slab_id = force_seal_active(&mgr).await;
		let raw = mgr.read_sealed_raw(slab_id).await.unwrap();
		let encrypted = mgr.encrypt_tiered_object_blob(slab_id, &raw).unwrap();
		let payload_hash = crate::hash::content::digest(&encrypted.bytes);
		let uri = l2
			.put(
				&format!("row-group-l2/slab_{slab_id:016x}.wnd"),
				&encrypted.bytes,
			)
			.await
			.unwrap();
		let pointer = TieredSlabPointer {
			slab_id,
			l2_uri: uri,
			payload_hash,
			is_encrypted: true,
			tenant_id: Some(tenant()),
			tenant_kek_id: Some(encrypted.tenant_kek_id),
			stats_format_version: encrypted
				.stats_locator
				.map(|locator| locator.stats_format_version),
			stats_block_len: encrypted
				.stats_locator
				.map(|locator| locator.stats_block_len),
			stats_block_hash128: encrypted
				.stats_locator
				.map(|locator| locator.stats_block_hash128),
			tombstone_metadata: TombstoneMetadata::None,
			supersession_metadata: SupersessionMetadata::None,
		};
		mgr.apply_tier_slab(pointer.clone()).await.unwrap();
		let target = encoded
			.rows
			.iter()
			.find(|row| row.record_row_index == RecordRowIndex::new(10_000))
			.expect("target storage row");
		let location = mgr
			.lookup(tenant(), target.key_identity)
			.expect("tiered row location");
		let requirement = ColumnFetchRequirement {
			output_columns: crate::storage::columnar::ColumnOrdinalSet::from_columns([1]),
			..ColumnFetchRequirement::default()
		};
		let predicate_requirement = ColumnFetchRequirement {
			predicate_columns: crate::storage::columnar::ColumnOrdinalSet::from_columns([0]),
			output_columns: crate::storage::columnar::ColumnOrdinalSet::from_columns([1]),
			..ColumnFetchRequirement::default()
		};
		let mut bytecode = Vec::new();
		bytecode.push(crate::schema::pbc::OP_PUSH_COL);
		bytecode.extend_from_slice(&0_u32.to_le_bytes());
		bytecode.push(crate::schema::pbc::OP_PUSH_INT);
		bytecode.extend_from_slice(&50_000_i64.to_le_bytes());
		bytecode.push(crate::schema::pbc::OP_GT);
		let disjoint_program = crate::schema::pbc::Program::compile(&bytecode).unwrap();

		let pruned = mgr
			.read_projected_record_from_l2(
				l2.clone(),
				&pointer,
				location,
				&descriptor,
				&predicate_requirement,
				Some(&disjoint_program),
				None,
			)
			.await
			.unwrap();
		assert_eq!(pruned.projected.batch.num_rows(), 0);
		let target_group = crate::schema::RowGroupIndex::new(2);
		match &pruned.strategy {
			L2FetchStrategy::RowGroup(plan) => {
				assert!(plan.surviving_groups.is_empty());
				assert_eq!(plan.predicate_fetched_groups, vec![target_group]);
				assert!(plan.phase_b_fetch.is_some());
				assert!(plan.phase_c_fetch.selected_plaintext_ranges.is_empty());
			}
			other => panic!("expected row-group no-survivors strategy, got {other:?}"),
		}

		let mut bytecode = Vec::new();
		bytecode.push(crate::schema::pbc::OP_PUSH_COL);
		bytecode.extend_from_slice(&0_u32.to_le_bytes());
		bytecode.push(crate::schema::pbc::OP_PUSH_INT);
		bytecode.extend_from_slice(&0_i64.to_le_bytes());
		bytecode.push(crate::schema::pbc::OP_GTE);
		let phase_b_pass_program = crate::schema::pbc::Program::compile(&bytecode).unwrap();
		let phase_b_pass = mgr
			.read_projected_record_from_l2(
				l2.clone(),
				&pointer,
				location,
				&descriptor,
				&predicate_requirement,
				Some(&phase_b_pass_program),
				None,
			)
			.await
			.unwrap();
		match &phase_b_pass.strategy {
			L2FetchStrategy::RowGroup(plan) => {
				assert_eq!(plan.surviving_groups, vec![target_group]);
				assert_eq!(plan.predicate_fetched_groups, vec![target_group]);
				let phase_b_fetch = plan.phase_b_fetch.as_ref().expect("Phase B fetch plan");
				for page in &plan.directory.buffer_pages {
					if page.row_group_index != target_group || page.range.len.get() == 0 {
						continue;
					}
					if page.ipc_column_ordinal == crate::schema::IpcColumnOrdinal::new(0) {
						assert!(body_ranges_cover(
							&phase_b_fetch.selected_plaintext_ranges,
							page.range
						));
						assert!(!body_ranges_cover(
							&plan.phase_c_fetch.selected_plaintext_ranges,
							page.range
						));
					}
					if page.ipc_column_ordinal == crate::schema::IpcColumnOrdinal::new(1) {
						assert!(body_ranges_cover(
							&plan.phase_c_fetch.selected_plaintext_ranges,
							page.range
						));
					}
				}
			}
			other => panic!("expected row-group Phase B strategy, got {other:?}"),
		}
		let predicate_only_requirement = ColumnFetchRequirement {
			predicate_columns: crate::storage::columnar::ColumnOrdinalSet::from_columns([0]),
			..ColumnFetchRequirement::default()
		};
		let predicate_only = mgr
			.read_projected_record_from_l2(
				l2.clone(),
				&pointer,
				location,
				&descriptor,
				&predicate_only_requirement,
				Some(&phase_b_pass_program),
				None,
			)
			.await
			.unwrap();
		match &predicate_only.strategy {
			L2FetchStrategy::RowGroup(plan) => {
				assert_eq!(plan.surviving_groups, vec![target_group]);
				assert_eq!(plan.predicate_fetched_groups, vec![target_group]);
				assert!(plan.phase_b_fetch.is_some());
				assert!(plan.phase_c_fetch.selected_plaintext_ranges.is_empty());
			}
			other => panic!("expected predicate-only row-group Phase B strategy, got {other:?}"),
		}
		assert_eq!(predicate_only.projected.batch.num_rows(), 4096);
		assert!(predicate_only.projected.physical_for(0).is_some());

		let full_frame = mgr
			.read_encrypted_record_from_l2(l2.as_ref(), &pointer, location)
			.await
			.unwrap();
		let whole = decode_record_batch(&full_frame).unwrap();
		let expected_id_payload = whole.project(&[0, 1]).unwrap().slice(8192, 4096);
		assert_eq!(phase_b_pass.projected.batch.num_rows(), 4096);
		let id_values = phase_b_pass
			.projected
			.batch
			.column(phase_b_pass.projected.physical_for(0).expect("id physical"))
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("id column");
		let payload_values = phase_b_pass
			.projected
			.batch
			.column(
				phase_b_pass
					.projected
					.physical_for(1)
					.expect("payload physical"),
			)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("payload column");
		let expected_ids = expected_id_payload.column(0);
		let expected_ids = expected_ids
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("expected id column");
		let expected_payloads = expected_id_payload.column(1);
		let expected_payloads = expected_payloads
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("expected payload column");
		assert_eq!(id_values.values(), expected_ids.values());
		assert_eq!(payload_values.values(), expected_payloads.values());

		let indexed_locations = [
			RecordRowIndex::new(10_000),
			RecordRowIndex::new(10_001),
			RecordRowIndex::new(13_000),
		]
		.into_iter()
		.map(|record_row_index| {
			let row = encoded
				.rows
				.iter()
				.find(|row| row.record_row_index == record_row_index)
				.expect("indexed source row");
			mgr.lookup(tenant(), row.key_identity)
				.expect("indexed row location")
		})
		.collect::<Vec<_>>();
		let indexed_projected = mgr
			.read_projected_indexed_rows(&indexed_locations, &descriptor, &requirement)
			.await
			.unwrap()
			.expect("projected indexed rows");
		match &indexed_projected.strategy {
			L2FetchStrategy::RowGroup(plan) => {
				assert_eq!(
					plan.surviving_groups,
					vec![
						crate::schema::RowGroupIndex::new(2),
						crate::schema::RowGroupIndex::new(3)
					]
				);
				assert!(plan.phase_b_fetch.is_none());
			}
			other => panic!("expected coalesced row-group indexed fetch, got {other:?}"),
		}
		let indexed_values = indexed_projected.projected.batch.column(0);
		let indexed_values = indexed_values
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("indexed payload column");
		let whole_payload = whole.column(1);
		let whole_payload = whole_payload
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("whole payload column");
		for (position, location) in indexed_locations.iter().enumerate() {
			assert_eq!(
				indexed_values.value(position),
				whole_payload.value(location.record_row_index.get() as usize)
			);
		}

		let mut bytecode = Vec::new();
		bytecode.push(crate::schema::pbc::OP_PUSH_COL);
		bytecode.extend_from_slice(&0_u32.to_le_bytes());
		bytecode.push(crate::schema::pbc::OP_PUSH_INT);
		bytecode.extend_from_slice(&12_000_i64.to_le_bytes());
		bytecode.push(crate::schema::pbc::OP_GT);
		let multi_group_program = crate::schema::pbc::Program::compile(&bytecode).unwrap();
		let indexed_record_rows = indexed_locations
			.iter()
			.map(|location| location.record_row_index)
			.collect::<Vec<_>>();
		let multi_group_masked = mgr
			.read_projected_record_from_l2(
				l2.clone(),
				&pointer,
				location,
				&descriptor,
				&predicate_requirement,
				Some(&multi_group_program),
				Some(&indexed_record_rows),
			)
			.await
			.unwrap();
		match &multi_group_masked.strategy {
			L2FetchStrategy::RowGroup(plan) => {
				assert_eq!(
					plan.surviving_groups,
					vec![
						crate::schema::RowGroupIndex::new(2),
						crate::schema::RowGroupIndex::new(3)
					]
				);
				assert_eq!(plan.predicate_fetched_groups, plan.surviving_groups);
				assert!(plan.phase_b_fetch.is_some());
			}
			other => panic!("expected multi-group Phase B row-group strategy, got {other:?}"),
		}
		let masked_ids = multi_group_masked
			.projected
			.batch
			.column(
				multi_group_masked
					.projected
					.physical_for(0)
					.expect("id physical"),
			)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("masked id column");
		let masked_payloads = multi_group_masked
			.projected
			.batch
			.column(
				multi_group_masked
					.projected
					.physical_for(1)
					.expect("payload physical"),
			)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("masked payload column");
		let selected_whole = arrow::compute::concat_batches(
			&whole.project(&[0, 1]).unwrap().schema(),
			&[
				whole.project(&[0, 1]).unwrap().slice(8192, 4096),
				whole.project(&[0, 1]).unwrap().slice(12288, 4096),
			],
		)
		.unwrap();
		let expected_masked =
			crate::schema::pbc::execute_program(&multi_group_program, &selected_whole).unwrap();
		let expected_ids = expected_masked
			.column(0)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("expected masked ids");
		let expected_payloads = expected_masked
			.column(1)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("expected masked payloads");
		assert_eq!(masked_ids.values(), expected_ids.values());
		assert_eq!(masked_payloads.values(), expected_payloads.values());

		let projected = mgr
			.read_projected_record_from_l2(
				l2.clone(),
				&pointer,
				location,
				&descriptor,
				&requirement,
				None,
				None,
			)
			.await
			.unwrap();
		let expected = whole.project(&[1]).unwrap().slice(8192, 4096);

		assert!(matches!(projected.strategy, L2FetchStrategy::RowGroup(_)));
		assert_eq!(
			projected.projected_first_record_row_index,
			RecordRowIndex::new(8192)
		);
		assert_eq!(projected.projected.batch.num_rows(), expected.num_rows());
		let actual_values = projected.projected.batch.column(0);
		let actual_values = actual_values
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("actual payload column");
		let expected_values = expected.column(0);
		let expected_values = expected_values
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("expected payload column");
		assert_eq!(actual_values.values(), expected_values.values());
	}

	#[test]
	fn encoding_predicate_page_plan_narrows_clustered_scan_pages() {
		let row_count = crate::storage::row_group::ROW_GROUP_SCAN_TARGET_ROWS as usize;
		let column_count = 17_usize;
		let schema = Schema::new(
			(0..column_count)
				.map(|column| Field::new(format!("c{column}"), DataType::UInt8, false))
				.collect::<Vec<_>>(),
		);
		let descriptor = TableSchemaDescriptorV1::new_with_layout(
			TableObjectId::new([0x63; 16]),
			1,
			"analytics",
			"adr063_page_plan",
			&schema,
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("c0")),
			LocalityPolicyV1::IdentityProjection,
			RecordLayoutPolicy::MultiRow(
				MultiRowLayoutDescriptor::new(
					crate::storage::row_group::ROW_GROUP_SCAN_TARGET_ROWS,
					crate::storage::row_group::ROW_GROUP_SCAN_TARGET_ROWS,
					8 * 1024 * 1024,
				)
				.unwrap(),
			),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.unwrap();
		let predicate_values = (0..row_count)
			.map(|row| {
				let page = row / 1_024;
				if page == 0 {
					0
				} else if page == 1 {
					1
				} else if page == 2 {
					if row % 2 == 0 { 1 } else { 3 }
				} else {
					3
				}
			})
			.collect::<Vec<_>>();
		let batch = RecordBatch::try_new(
			Arc::new(schema),
			(0..column_count)
				.map(|column| {
					if column == 0 {
						Arc::new(UInt8Array::from_iter_values(
							predicate_values.iter().copied(),
						)) as Arc<dyn Array>
					} else {
						Arc::new(UInt8Array::from_iter_values(
							(0..row_count).map(|row| ((row + column) % 4) as u8),
						)) as Arc<dyn Array>
					}
				})
				.collect::<Vec<_>>(),
		)
		.unwrap();
		let source_rows = (0..row_count)
			.map(|source_row_index| {
				let mut key = [0_u8; 32];
				key[..8].copy_from_slice(&(source_row_index as u64).to_be_bytes());
				MultiRowSourceRow {
					source_row_index,
					key_identity: Identity::new(key),
					routing_locality: Locality::ZERO,
					row_hlc: hlc(63),
					is_tombstone: false,
					table_object_id: Some(descriptor.table_object_id),
				}
			})
			.collect::<Vec<_>>();
		let row_group_config =
			crate::storage::row_group::RowGroupWriterConfig::scan().with_enabled(true);
		let encoding_config = EncodingWriterConfig {
			enabled: true,
			target_simd_page_values: crate::schema::SimdPageValueCount::new(1_024),
			min_buffer_bytes_for_encoding: RecordBodyByteLen::new(1),
			enabled_cascades: [
				crate::storage::EncodingCascadeKind::Plain,
				crate::storage::EncodingCascadeKind::Dictionary,
			]
			.into_iter()
			.collect(),
			..EncodingWriterConfig::default()
		};
		let encoded =
			crate::storage::multirow::encode_multirow_from_survivors_with_row_group_and_encoding_config(
				&batch,
				&source_rows,
				&descriptor,
				RecordBodyCompressionPolicy::Uncompressed,
				&row_group_config,
				&encoding_config,
			)
			.unwrap();
		let directory = parse_record_buffer_directory(
			TieredRecordId {
				slab_id: SlabId::new(63),
				record_offset: RecordBodyByteOffset::ZERO,
			},
			PlaintextByteRange {
				start: 0,
				len: encoded.frame.len() as u64,
			},
			&encoded.frame,
			&descriptor,
		)
		.unwrap();
		let page_directory = directory.page_directory.as_ref().unwrap();
		let encoding_directory = directory.encoding_directory.as_ref().unwrap();
		let mut bytecode = Vec::new();
		bytecode.push(crate::schema::pbc::OP_PUSH_COL);
		bytecode.extend_from_slice(&0_u32.to_le_bytes());
		bytecode.push(crate::schema::pbc::OP_PUSH_INT);
		bytecode.extend_from_slice(&2_i64.to_le_bytes());
		bytecode.push(crate::schema::pbc::OP_LT);
		let program = crate::schema::pbc::Program::compile(&bytecode).unwrap();
		let encoded_predicate = encoded_predicate_node_from_tree(&program.predicate_tree())
			.expect("clustered predicate should be page-plannable");
		let predicate_atom = PredicateAtom {
			op: crate::storage::encoding::compute::PredicateAtomOp::Lt,
			literal: Some(crate::schema::pbc::ColumnPredicateLiteralV1::Int(2)),
		};
		let (physical_ordinal, value_buffer_ordinal) =
			encoding_compute_value_buffer(&directory, 0).unwrap();
		let value_page = page_directory
			.buffer_pages
			.iter()
			.find(|page| {
				page.row_group_index == RowGroupIndex::ZERO
					&& page.ipc_column_ordinal == IpcColumnOrdinal::new(physical_ordinal as u32)
					&& page.ipc_buffer_ordinal == value_buffer_ordinal
					&& page.range.len.get() > 0
			})
			.expect("predicate value page");
		let value_encoding = find_buffer_encoding(
			encoding_directory,
			RowGroupIndex::ZERO,
			value_buffer_ordinal,
			value_page.range,
		)
		.expect("predicate value encoding");
		let cascade = encoding_directory
			.cascades
			.get(value_encoding.cascade_index.get() as usize)
			.expect("predicate cascade");
		assert_eq!(value_encoding.page_stats.len(), 64);
		assert!(page_stats_cascade_eligible(cascade), "{cascade:?}");
		let coercion = coerce_literal(predicate_atom.literal.as_ref(), &DataType::UInt8).unwrap();
		assert!(
			matches!(
				native_evaluation_eligibility(cascade, &predicate_atom, &coercion),
				NativeEvaluationEligibility::Eligible(_)
			),
			"{cascade:?}"
		);
		let selected_groups = page_directory
			.row_groups
			.iter()
			.map(|group| group.row_group_index)
			.collect::<Vec<_>>();
		let compute_config = EncodingComputeConfig::default();
		let page_plan = build_encoding_predicate_page_plan(
			&directory,
			page_directory,
			encoding_directory,
			&encoded_predicate,
			&selected_groups,
			&selected_groups,
			&compute_config,
			0,
		)
		.expect("clustered predicate should build a page-level plan");
		let dense_plan = build_encoding_compute_phase_b_plan(
			&directory,
			page_directory,
			encoding_directory,
			&encoded_predicate,
			&selected_groups,
			&selected_groups,
			&compute_config,
			0,
		)
		.expect("clustered predicate should also build a dense plan");
		let page_bytes = page_plan
			.record_body_ranges
			.iter()
			.map(|range| range.len.get())
			.sum::<u64>();
		let dense_bytes = dense_plan
			.record_body_ranges
			.iter()
			.map(|range| range.len.get())
			.sum::<u64>();
		let disjoint_pages = page_plan
			.decisions
			.iter()
			.filter(|decision| {
				matches!(
					decision.classification,
					EncodingPredicatePageDecisionKind::Disjoint
				)
			})
			.count();
		let exact_pages = page_plan
			.decisions
			.iter()
			.filter(|decision| {
				matches!(
					decision.classification,
					EncodingPredicatePageDecisionKind::ExactMatch
				)
			})
			.count();
		let maybe_pages = page_plan
			.decisions
			.iter()
			.filter(|decision| {
				matches!(
					decision.classification,
					EncodingPredicatePageDecisionKind::Maybe { .. }
				)
			})
			.count();

		assert_eq!(page_plan.fetches.len(), 1);
		assert_eq!((disjoint_pages, exact_pages, maybe_pages), (61, 2, 1));
		assert!(
			page_bytes * 10 < dense_bytes,
			"page plan bytes {page_bytes} should be materially smaller than dense bytes {dense_bytes}"
		);
	}

	#[tokio::test]
	async fn sparse_projection_fetch_materializes_exact_cocd_clustered_pages() {
		let l1_dir = fresh_dir("sparse-projection-l1");
		let l2_dir = fresh_dir("sparse-projection-l2");
		let l2_counter = Arc::new(CountingObjectStore::new(Arc::new(
			LocalFsObjectStore::new(&l2_dir).unwrap(),
		)));
		let l2: Arc<dyn ObjectStore> = l2_counter.clone();
		let mgr = Arc::new(
			SlabManager::open(SlabManagerConfig {
				dir: l1_dir.clone(),
				max_slab_bytes: 1024 * 1024 * 16,
				io: spawn_io_thread(64).unwrap(),
				l2: Some(l2.clone()),
				rehydration: RehydrationPolicy {
					promote_after_l2_reads: 0,
					..RehydrationPolicy::default()
				},
				l2_column_fetch: L2ColumnFetchConfig {
					enabled: true,
					min_record_bytes: RecordBodyByteLen::new(1),
					..L2ColumnFetchConfig::default()
				},
				row_group_writer: Default::default(),
				l2_row_group_fetch: crate::storage::row_group::L2RowGroupFetchConfig {
					enabled: true,
					max_selected_row_group_fraction_micros:
						crate::storage::row_group::L2_ROW_GROUP_FETCH_FRACTION_DENOMINATOR - 1,
					max_selected_byte_fraction_micros:
						crate::storage::row_group::L2_ROW_GROUP_FETCH_FRACTION_DENOMINATOR - 1,
					..Default::default()
				},
				encoding_writer: Default::default(),
				l2_chunk_layout: Default::default(),
				l2_encoding_fetch: crate::storage::encoding::L2EncodingFetchConfig {
					enabled: true,
					..Default::default()
				},
				encoding_compute: crate::storage::encoding::compute::EncodingComputeConfig {
					min_selectivity_for_encoded_eval_micros:
						crate::schema::SelectivityFractionMicros::new(1_000_000),
					..Default::default()
				},
				group_commit: GroupCommitConfig::default(),
				crypto: test_crypto(),
			})
			.await
			.unwrap(),
		);
		let schema = Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("selector", DataType::Int64, false),
			Field::new("payload", DataType::Int64, true),
		]);
		let mut descriptor = TableSchemaDescriptorV1::new_with_layout(
			TableObjectId::new([0x84; 16]),
			1,
			"analytics",
			"sparse_projection",
			&schema,
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
			LocalityPolicyV1::IdentityProjection,
			RecordLayoutPolicy::MultiRow(
				MultiRowLayoutDescriptor::new(4096, 4096, 8 * 1024 * 1024).unwrap(),
			),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.unwrap();
		descriptor.record_body_compression_policy = RecordBodyCompressionPolicy::Uncompressed;
		descriptor.refresh_descriptor_fingerprints().unwrap();
		let row_count = 4096_usize;
		let selector = (0..row_count).map(|row| row as i64).collect::<Vec<_>>();
		let batch = RecordBatch::try_new(
			Arc::new(schema),
			vec![
				Arc::new(Int64Array::from_iter_values(0..row_count as i64)) as Arc<dyn Array>,
				Arc::new(Int64Array::from_iter_values(selector.iter().copied())) as Arc<dyn Array>,
				Arc::new(Int64Array::from_iter(
					(0..row_count).map(|row| (row % 11 != 0).then_some((row % 128) as i64)),
				)) as Arc<dyn Array>,
			],
		)
		.unwrap();
		let source_rows = (0..row_count)
			.map(|source_row_index| {
				let mut key = [0_u8; 32];
				key[..8].copy_from_slice(&(source_row_index as u64).to_be_bytes());
				MultiRowSourceRow {
					source_row_index,
					key_identity: Identity::new(key),
					routing_locality: Locality::ZERO,
					row_hlc: hlc(600 + source_row_index as u64),
					is_tombstone: false,
					table_object_id: None,
				}
			})
			.collect::<Vec<_>>();
		let mut row_group_config =
			crate::storage::row_group::RowGroupWriterConfig::point_read_with_overrides(
				crate::schema::RowGroupRowCount::new(1024),
				crate::schema::RowGroupRowCount::new(1024),
				RowCount::new(1024),
				RecordBodyByteLen::new(1),
			)
			.with_enabled(true);
		row_group_config.stats_supported_types = std::collections::BTreeSet::new();
		let encoding_config = crate::storage::encoding::EncodingWriterConfig {
			enabled: true,
			target_simd_page_values: crate::schema::SimdPageValueCount::new(256),
			min_buffer_bytes_for_encoding: RecordBodyByteLen::new(1),
			..Default::default()
		};
		let encoded =
			crate::storage::multirow::encode_multirow_from_survivors_with_row_group_and_encoding_config(
				&batch,
				&source_rows,
				&descriptor,
				RecordBodyCompressionPolicy::Uncompressed,
				&row_group_config,
				&encoding_config,
			)
			.unwrap();
		let encoded_entries = encoded
			.rows
			.iter()
			.copied()
			.map(Into::into)
			.collect::<Vec<_>>();
		mgr.append_durable_multirow_with_locality(tenant(), &encoded.frame, &encoded_entries)
			.await
			.unwrap();
		let slab_id = force_seal_active(&mgr).await;
		let raw = mgr.read_sealed_raw(slab_id).await.unwrap();
		let encrypted = mgr.encrypt_tiered_object_blob(slab_id, &raw).unwrap();
		let payload_hash = crate::hash::content::digest(&encrypted.bytes);
		let uri = l2
			.put(
				&format!("sparse-projection/slab_{slab_id:016x}.wnd"),
				&encrypted.bytes,
			)
			.await
			.unwrap();
		let pointer = TieredSlabPointer {
			slab_id,
			l2_uri: uri,
			payload_hash,
			is_encrypted: true,
			tenant_id: Some(tenant()),
			tenant_kek_id: Some(encrypted.tenant_kek_id),
			stats_format_version: encrypted
				.stats_locator
				.map(|locator| locator.stats_format_version),
			stats_block_len: encrypted
				.stats_locator
				.map(|locator| locator.stats_block_len),
			stats_block_hash128: encrypted
				.stats_locator
				.map(|locator| locator.stats_block_hash128),
			tombstone_metadata: TombstoneMetadata::None,
			supersession_metadata: SupersessionMetadata::None,
		};
		mgr.apply_tier_slab(pointer.clone()).await.unwrap();
		let target = encoded
			.rows
			.iter()
			.find(|row| row.record_row_index == RecordRowIndex::new(300))
			.expect("target storage row");
		let location = mgr
			.lookup(tenant(), target.key_identity)
			.expect("tiered row location");
		// V1 sparse projection fetch consumes the exact COCD mask but
		// does not page-slice dense predicate columns.
		let requirement = ColumnFetchRequirement {
			output_columns: crate::storage::columnar::ColumnOrdinalSet::from_columns([2]),
			..ColumnFetchRequirement::default()
		};
		let mut bytecode = Vec::new();
		bytecode.push(crate::schema::pbc::OP_PUSH_COL);
		bytecode.extend_from_slice(&1_u32.to_le_bytes());
		bytecode.push(crate::schema::pbc::OP_PUSH_INT);
		bytecode.extend_from_slice(&256_i64.to_le_bytes());
		bytecode.push(crate::schema::pbc::OP_LT);
		let predicate = crate::schema::pbc::Program::compile(&bytecode).unwrap();
		let selected_rows = [RecordRowIndex::new(300)];

		let counter_before_sparse = l2_counter.snapshot();
		let sparse = mgr
			.read_projected_record_from_l2(
				l2.clone(),
				&pointer,
				location,
				&descriptor,
				&requirement,
				Some(&predicate),
				Some(&selected_rows),
			)
			.await
			.unwrap();
		let counter_after_sparse = l2_counter.snapshot();
		assert_eq!(counter_after_sparse.get_bytes_calls, 0);
		assert_eq!(counter_after_sparse.get_bytes_bytes, 0);
		assert!(
			counter_after_sparse.get_range_calls > counter_before_sparse.get_range_calls,
			"sparse projection read must use L2 range GETs"
		);
		let sparse_get_range_bytes = counter_after_sparse
			.get_range_bytes
			.saturating_sub(counter_before_sparse.get_range_bytes);
		let L2FetchStrategy::RowGroup(plan) = &sparse.strategy else {
			panic!(
				"expected row-group sparse projection strategy, got {:?}",
				sparse.strategy
			);
		};
		assert_eq!(
			plan.surviving_groups,
			vec![crate::schema::RowGroupIndex::ZERO]
		);
		assert_eq!(plan.predicate_fetched_groups, plan.surviving_groups);
		assert!(plan.phase_b_fetch.is_some());
		assert!(
			sparse_get_range_bytes >= plan.phase_c_fetch.fetched_ciphertext_bytes,
			"runtime range GET counter should cover sparse projection ciphertext bytes"
		);

		let full_frame = mgr
			.read_encrypted_record_from_l2(l2.as_ref(), &pointer, location)
			.await
			.unwrap();
		let directory = parse_record_buffer_directory(
			TieredRecordId {
				slab_id,
				record_offset: RecordBodyByteOffset::new(location.offset),
			},
			PlaintextByteRange {
				start: 0,
				len: full_frame.len() as u64,
			},
			&full_frame,
			&descriptor,
		)
		.unwrap();
		let page_directory = directory.page_directory.as_ref().unwrap();
		let encoding_directory = directory.encoding_directory.as_ref().unwrap();
		let payload_encoding = encoding_directory
			.buffers
			.iter()
			.find(|encoding| {
				encoding.row_group_index == crate::schema::RowGroupIndex::ZERO
					&& encoding.ipc_column_ordinal == crate::schema::IpcColumnOrdinal::new(2)
					&& encoding.page_end_offsets.len() > 1
					&& encoding.encoded_range.len < encoding.decoded_byte_len
			})
			.expect("payload value buffer should be page-encoded");
		let payload_null_ordinal = directory.column_to_buffers[2].first_buffer_ordinal;
		let payload_null_page = page_directory
			.buffer_pages
			.iter()
			.find(|page| {
				page.row_group_index == payload_encoding.row_group_index
					&& page.ipc_column_ordinal == payload_encoding.ipc_column_ordinal
					&& page.ipc_buffer_ordinal == payload_null_ordinal
					&& page.range.len.get() > 0
			})
			.expect("payload validity buffer page");
		assert!(
			body_ranges_overlap(
				&plan.phase_c_fetch.selected_plaintext_ranges,
				payload_null_page.range
			),
			"sparse projection should fetch the projected nullable column validity bitmap"
		);
		let payload_page = page_directory
			.buffer_pages
			.iter()
			.find(|page| {
				page.row_group_index == payload_encoding.row_group_index
					&& page.ipc_column_ordinal == payload_encoding.ipc_column_ordinal
					&& page.ipc_buffer_ordinal == payload_encoding.ipc_buffer_ordinal
			})
			.unwrap();
		let full_payload_range = crate::storage::row_group::RecordBodyByteRange {
			start: RecordBodyByteOffset::new(
				payload_page.range.start.get()
					+ u64::from(payload_encoding.encoded_range.start.get()),
			),
			len: payload_encoding.encoded_range.len,
		};
		assert!(
			!body_ranges_cover(
				&plan.phase_c_fetch.selected_plaintext_ranges,
				full_payload_range
			),
			"sparse projection should not fetch the whole projected payload buffer"
		);
		let selected_projection_bytes = plan
			.phase_c_fetch
			.selected_plaintext_ranges
			.iter()
			.map(|range| range.len.get())
			.sum::<u64>();
		assert!(
			selected_projection_bytes < full_payload_range.len.get(),
			"sparse projection bytes {selected_projection_bytes} should stay below full payload range {}",
			full_payload_range.len.get()
		);

		let expected = crate::schema::pbc::execute_program(&predicate, &batch)
			.unwrap()
			.project(&[2])
			.unwrap();
		assert_eq!(sparse.projected.batch, expected);

		let mut all_bytecode = Vec::new();
		all_bytecode.push(crate::schema::pbc::OP_PUSH_COL);
		all_bytecode.extend_from_slice(&1_u32.to_le_bytes());
		all_bytecode.push(crate::schema::pbc::OP_PUSH_INT);
		all_bytecode.extend_from_slice(&(row_count as i64).to_le_bytes());
		all_bytecode.push(crate::schema::pbc::OP_LT);
		let all_predicate = crate::schema::pbc::Program::compile(&all_bytecode).unwrap();
		let all = mgr
			.read_projected_record_from_l2(
				l2.clone(),
				&pointer,
				location,
				&descriptor,
				&requirement,
				Some(&all_predicate),
				Some(&selected_rows),
			)
			.await
			.unwrap();
		let L2FetchStrategy::RowGroup(all_plan) = &all.strategy else {
			panic!(
				"expected row-group dense projection strategy for all mask, got {:?}",
				all.strategy
			);
		};
		assert!(all_plan.phase_b_fetch.is_some());
		assert_eq!(
			all_plan.surviving_groups,
			vec![crate::schema::RowGroupIndex::ZERO]
		);
		assert!(
			body_ranges_cover(
				&all_plan.phase_c_fetch.selected_plaintext_ranges,
				full_payload_range
			),
			"all-mask sparse decline should route to the dense projection range"
		);
		let all_expected = batch.slice(0, row_group_config.target_row_group_rows().get() as usize);
		let all_expected = crate::schema::pbc::execute_program(&all_predicate, &all_expected)
			.unwrap()
			.project(&[2])
			.unwrap();
		assert_eq!(all.projected.batch, all_expected);

		let mut none_bytecode = Vec::new();
		none_bytecode.push(crate::schema::pbc::OP_PUSH_COL);
		none_bytecode.extend_from_slice(&1_u32.to_le_bytes());
		none_bytecode.push(crate::schema::pbc::OP_PUSH_INT);
		none_bytecode.extend_from_slice(&0_i64.to_le_bytes());
		none_bytecode.push(crate::schema::pbc::OP_LT);
		let none_predicate = crate::schema::pbc::Program::compile(&none_bytecode).unwrap();
		let none = mgr
			.read_projected_record_from_l2(
				l2,
				&pointer,
				location,
				&descriptor,
				&requirement,
				Some(&none_predicate),
				Some(&selected_rows),
			)
			.await
			.unwrap();
		let L2FetchStrategy::RowGroup(none_plan) = &none.strategy else {
			panic!(
				"expected row-group sparse projection strategy for none mask, got {:?}",
				none.strategy
			);
		};
		assert!(none_plan.phase_b_fetch.is_some());
		assert!(none_plan.surviving_groups.is_empty());
		assert!(none_plan.phase_c_fetch.selected_plaintext_ranges.is_empty());
		let none_expected = crate::schema::pbc::execute_program(&none_predicate, &batch)
			.unwrap()
			.project(&[2])
			.unwrap();
		assert_eq!(none.projected.batch, none_expected);
	}

	#[tokio::test]
	async fn adr061_enabled_chunk_layout_writes_16k_encrypted_slab() {
		let dir = fresh_dir("adr061-chunk-layout");
		let chunk_layout_config = crate::storage::encoding::L2ChunkLayoutConfig {
			enabled: true,
			..Default::default()
		};
		let mgr = Arc::new(
			SlabManager::open(SlabManagerConfig {
				dir,
				max_slab_bytes: 1024 * 1024 * 16,
				io: spawn_io_thread(64).unwrap(),
				l2: None,
				rehydration: RehydrationPolicy::default(),
				l2_column_fetch: L2ColumnFetchConfig::default(),
				row_group_writer: Default::default(),
				l2_row_group_fetch: Default::default(),
				encoding_writer: Default::default(),
				l2_chunk_layout: chunk_layout_config.clone(),
				l2_encoding_fetch: Default::default(),
				encoding_compute: Default::default(),
				group_commit: GroupCommitConfig::default(),
				crypto: test_crypto(),
			})
			.await
			.unwrap(),
		);
		let schema = Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("payload", DataType::UInt8, false),
		]);
		let mut descriptor = TableSchemaDescriptorV1::new_with_layout(
			TableObjectId::new([0x61; 16]),
			1,
			"analytics",
			"adr061_chunk_layout",
			&schema,
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
			LocalityPolicyV1::IdentityProjection,
			RecordLayoutPolicy::MultiRow(
				MultiRowLayoutDescriptor::new(4096, 4096, 8 * 1024 * 1024).unwrap(),
			),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.unwrap();
		descriptor.record_body_compression_policy = RecordBodyCompressionPolicy::Uncompressed;
		descriptor.refresh_descriptor_fingerprints().unwrap();
		let row_count = 4096_usize;
		let batch = RecordBatch::try_new(
			Arc::new(schema),
			vec![
				Arc::new(Int64Array::from_iter_values(0..row_count as i64)) as Arc<dyn Array>,
				Arc::new(UInt8Array::from_iter_values(
					(0..row_count).map(|row| row as u8),
				)) as Arc<dyn Array>,
			],
		)
		.unwrap();
		let source_rows = (0..row_count)
			.map(|source_row_index| {
				let mut key = [0_u8; 32];
				key[..8].copy_from_slice(&(source_row_index as u64).to_be_bytes());
				MultiRowSourceRow {
					source_row_index,
					key_identity: Identity::new(key),
					routing_locality: Locality::ZERO,
					row_hlc: hlc(610 + source_row_index as u64),
					is_tombstone: false,
					table_object_id: None,
				}
			})
			.collect::<Vec<_>>();
		let mut row_group_config =
			crate::storage::row_group::RowGroupWriterConfig::point_read_with_overrides(
				crate::schema::RowGroupRowCount::new(1024),
				crate::schema::RowGroupRowCount::new(1024),
				RowCount::new(1024),
				RecordBodyByteLen::new(1),
			)
			.with_enabled(true);
		row_group_config.stats_supported_types = std::collections::BTreeSet::new();
		let encoding_config = crate::storage::encoding::EncodingWriterConfig {
			enabled: true,
			target_simd_page_values: crate::schema::SimdPageValueCount::new(256),
			min_buffer_bytes_for_encoding: RecordBodyByteLen::new(1),
			..Default::default()
		};
		let encoded = crate::storage::multirow::encode_multirow_from_survivors_with_row_group_encoding_and_chunk_layout_config(
			&batch,
			&source_rows,
			&descriptor,
			RecordBodyCompressionPolicy::Uncompressed,
			&row_group_config,
			&encoding_config,
			&chunk_layout_config,
		)
		.unwrap();
		let parsed = parse_record_buffer_directory(
			TieredRecordId {
				slab_id: SlabId::ZERO,
				record_offset: RecordBodyByteOffset::ZERO,
			},
			PlaintextByteRange {
				start: 0,
				len: encoded.frame.len() as u64,
			},
			&encoded.frame,
			&descriptor,
		)
		.unwrap();
		let page_directory = parsed.page_directory.as_ref().unwrap();
		let encoding_directory = parsed.encoding_directory.as_ref().unwrap();
		for encoding in &encoding_directory.buffers {
			if !crate::storage::encoding::chunk_layout::default_sparse_engaged_key(
				encoding_directory,
				encoding.row_group_index,
				encoding.ipc_column_ordinal,
				encoding.ipc_buffer_ordinal,
			) {
				continue;
			}
			let page = page_directory
				.buffer_pages
				.iter()
				.find(|page| {
					page.row_group_index == encoding.row_group_index
						&& page.ipc_column_ordinal == encoding.ipc_column_ordinal
						&& page.ipc_buffer_ordinal == encoding.ipc_buffer_ordinal
				})
				.unwrap();
			let physical_start =
				page.range.start.get() + u64::from(encoding.encoded_range.start.get());
			assert_eq!(
				physical_start % u64::from(16 * 1024_u32),
				0,
				"default-engaged encoded buffer should start on the selected chunk boundary"
			);
		}
		let encoded_entries = encoded
			.rows
			.iter()
			.copied()
			.map(Into::into)
			.collect::<Vec<_>>();
		mgr.append_durable_multirow_with_locality(tenant(), &encoded.frame, &encoded_entries)
			.await
			.unwrap();
		let slab_id = force_seal_active(&mgr).await;
		let raw = mgr.read_sealed_raw(slab_id).await.unwrap();
		let encrypted = mgr.encrypt_tiered_object_blob(slab_id, &raw).unwrap();
		assert_eq!(
			encrypted.chunk_size_bytes,
			crate::schema::ChunkSizeBytes::new(16 * 1024)
		);
		let footer = mgr
			.encrypted_tiered_footer_summary(&encrypted.bytes)
			.unwrap()
			.expect("encrypted footer");
		assert_eq!(footer.chunk_size_bytes, encrypted.chunk_size_bytes);
		let decrypted = mgr
			.config
			.crypto
			.decrypt_tiered_slab(slab_id, &encrypted.bytes)
			.unwrap();
		assert_eq!(decrypted.plaintext, raw);
	}

	#[tokio::test]
	async fn encoding_secondary_index_fetches_only_selected_simd_pages() {
		let l1_dir = fresh_dir("encoding-index-l1");
		let l2_dir = fresh_dir("encoding-index-l2");
		let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&l2_dir).unwrap());
		let mgr = Arc::new(
			SlabManager::open(SlabManagerConfig {
				dir: l1_dir.clone(),
				max_slab_bytes: 1024 * 1024 * 16,
				io: spawn_io_thread(64).unwrap(),
				l2: Some(l2.clone()),
				rehydration: RehydrationPolicy {
					promote_after_l2_reads: 0,
					..RehydrationPolicy::default()
				},
				l2_column_fetch: L2ColumnFetchConfig {
					enabled: true,
					min_record_bytes: RecordBodyByteLen::new(1),
					..L2ColumnFetchConfig::default()
				},
				row_group_writer: Default::default(),
				l2_row_group_fetch: crate::storage::row_group::L2RowGroupFetchConfig {
					enabled: true,
					max_selected_row_group_fraction_micros:
						crate::storage::row_group::L2_ROW_GROUP_FETCH_FRACTION_DENOMINATOR - 1,
					..Default::default()
				},
				encoding_writer: Default::default(),
				l2_chunk_layout: Default::default(),
				l2_encoding_fetch: crate::storage::encoding::L2EncodingFetchConfig {
					enabled: true,
					..Default::default()
				},
				encoding_compute: Default::default(),
				group_commit: GroupCommitConfig::default(),
				crypto: test_crypto(),
			})
			.await
			.unwrap(),
		);
		let schema = Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("payload", DataType::UInt8, false),
		]);
		let mut descriptor = TableSchemaDescriptorV1::new_with_layout(
			TableObjectId::new([0x83; 16]),
			1,
			"analytics",
			"encoding_index",
			&schema,
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
			LocalityPolicyV1::IdentityProjection,
			RecordLayoutPolicy::MultiRow(
				MultiRowLayoutDescriptor::new(4096, 4096, 8 * 1024 * 1024).unwrap(),
			),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.unwrap();
		descriptor.record_body_compression_policy = RecordBodyCompressionPolicy::Uncompressed;
		descriptor.refresh_descriptor_fingerprints().unwrap();
		let row_count = 4096_usize;
		let batch = RecordBatch::try_new(
			Arc::new(schema),
			vec![
				Arc::new(Int64Array::from_iter_values(0..row_count as i64)) as Arc<dyn Array>,
				Arc::new(UInt8Array::from_iter_values((0..row_count).map(|_| 7_u8)))
					as Arc<dyn Array>,
			],
		)
		.unwrap();
		let meta = layout_recluster_meta(&descriptor, batch.num_rows());
		let admission = validate_table_write_request_batch(
			&meta,
			&batch,
			&descriptor,
			WriterNodeId::ZERO,
			meta.client_hlc,
		)
		.unwrap();
		let source_rows = admission
			.key_identities
			.iter()
			.zip(admission.localities.iter())
			.enumerate()
			.map(
				|(source_row_index, (key_identity, routing_locality))| MultiRowSourceRow {
					source_row_index,
					key_identity: *key_identity,
					routing_locality: *routing_locality,
					row_hlc: hlc(500 + source_row_index as u64),
					is_tombstone: false,
					table_object_id: None,
				},
			)
			.collect::<Vec<_>>();
		let mut row_group_config =
			crate::storage::row_group::RowGroupWriterConfig::point_read_with_overrides(
				crate::schema::RowGroupRowCount::new(1024),
				crate::schema::RowGroupRowCount::new(1024),
				RowCount::new(1024),
				RecordBodyByteLen::new(1),
			)
			.with_enabled(true);
		row_group_config.stats_supported_types = std::collections::BTreeSet::new();
		let encoding_config = crate::storage::encoding::EncodingWriterConfig {
			enabled: true,
			target_simd_page_values: crate::schema::SimdPageValueCount::new(256),
			min_buffer_bytes_for_encoding: RecordBodyByteLen::new(1),
			..Default::default()
		};
		let encoded =
				crate::storage::multirow::encode_multirow_from_survivors_with_row_group_and_encoding_config(
					&batch,
					&source_rows,
					&descriptor,
					RecordBodyCompressionPolicy::Uncompressed,
					&row_group_config,
					&encoding_config,
				)
				.unwrap();
		let encoded_entries = encoded
			.rows
			.iter()
			.copied()
			.map(Into::into)
			.collect::<Vec<_>>();
		mgr.append_durable_multirow_with_locality(tenant(), &encoded.frame, &encoded_entries)
			.await
			.unwrap();
		let slab_id = force_seal_active(&mgr).await;
		let raw = mgr.read_sealed_raw(slab_id).await.unwrap();
		let encrypted = mgr.encrypt_tiered_object_blob(slab_id, &raw).unwrap();
		let payload_hash = crate::hash::content::digest(&encrypted.bytes);
		let uri = l2
			.put(
				&format!("encoding-index/slab_{slab_id:016x}.wnd"),
				&encrypted.bytes,
			)
			.await
			.unwrap();
		let pointer = TieredSlabPointer {
			slab_id,
			l2_uri: uri,
			payload_hash,
			is_encrypted: true,
			tenant_id: Some(tenant()),
			tenant_kek_id: Some(encrypted.tenant_kek_id),
			stats_format_version: encrypted
				.stats_locator
				.map(|locator| locator.stats_format_version),
			stats_block_len: encrypted
				.stats_locator
				.map(|locator| locator.stats_block_len),
			stats_block_hash128: encrypted
				.stats_locator
				.map(|locator| locator.stats_block_hash128),
			tombstone_metadata: TombstoneMetadata::None,
			supersession_metadata: SupersessionMetadata::None,
		};
		mgr.apply_tier_slab(pointer.clone()).await.unwrap();
		let indexed_locations = [
			RecordRowIndex::new(300),
			RecordRowIndex::new(301),
			RecordRowIndex::new(900),
		]
		.into_iter()
		.map(|record_row_index| {
			let row = encoded
				.rows
				.iter()
				.find(|row| row.record_row_index == record_row_index)
				.expect("indexed source row");
			mgr.lookup(tenant(), row.key_identity)
				.expect("indexed row location")
		})
		.collect::<Vec<_>>();
		let requirement = ColumnFetchRequirement {
			output_columns: crate::storage::columnar::ColumnOrdinalSet::from_columns([1]),
			..ColumnFetchRequirement::default()
		};

		let indexed = mgr
			.read_projected_indexed_rows(&indexed_locations, &descriptor, &requirement)
			.await
			.unwrap()
			.expect("projected indexed rows");
		assert!(indexed.indexed_rows_materialized);
		assert_eq!(indexed.projected.batch.num_rows(), indexed_locations.len());
		let L2FetchStrategy::RowGroup(plan) = &indexed.strategy else {
			panic!(
				"expected encoding indexed row-group strategy, got {:?}",
				indexed.strategy
			);
		};
		assert_eq!(
			plan.surviving_groups,
			vec![crate::schema::RowGroupIndex::ZERO]
		);
		assert!(plan.phase_b_fetch.is_none());

		let full_frame = mgr
			.read_encrypted_record_from_l2(l2.as_ref(), &pointer, indexed_locations[0])
			.await
			.unwrap();
		let directory = parse_record_buffer_directory(
			TieredRecordId {
				slab_id,
				record_offset: RecordBodyByteOffset::new(indexed_locations[0].offset),
			},
			PlaintextByteRange {
				start: 0,
				len: full_frame.len() as u64,
			},
			&full_frame,
			&descriptor,
		)
		.unwrap();
		let page_directory = directory.page_directory.as_ref().unwrap();
		let encoding_directory = directory.encoding_directory.as_ref().unwrap();
		let payload_encoding = encoding_directory
			.buffers
			.iter()
			.find(|encoding| {
				encoding.row_group_index == crate::schema::RowGroupIndex::ZERO
					&& encoding.ipc_column_ordinal == crate::schema::IpcColumnOrdinal::new(1)
					&& encoding.page_end_offsets.len() > 1
					&& encoding.encoded_range.len < encoding.decoded_byte_len
			})
			.expect("payload value buffer should be page-encoded");
		let payload_page = page_directory
			.buffer_pages
			.iter()
			.find(|page| {
				page.row_group_index == payload_encoding.row_group_index
					&& page.ipc_column_ordinal == payload_encoding.ipc_column_ordinal
					&& page.ipc_buffer_ordinal == payload_encoding.ipc_buffer_ordinal
			})
			.unwrap();
		let full_encoded_range = crate::storage::row_group::RecordBodyByteRange {
			start: RecordBodyByteOffset::new(
				payload_page.range.start.get()
					+ u64::from(payload_encoding.encoded_range.start.get()),
			),
			len: payload_encoding.encoded_range.len,
		};
		assert!(
			!body_ranges_cover(
				&plan.phase_c_fetch.selected_plaintext_ranges,
				full_encoded_range
			),
			"SIMD-page fast path should not fetch the whole encoded buffer"
		);
		let selected_bytes = plan
			.phase_c_fetch
			.selected_plaintext_ranges
			.iter()
			.map(|range| range.len.get())
			.sum::<u64>();
		assert!(
			plan.phase_c_fetch.selected_plaintext_ranges.len() < indexed_locations.len(),
			"Encoding indexed L2 plan should deduplicate multiple selected rows on the same SIMD page"
		);
		assert!(selected_bytes < full_encoded_range.len.get());

		let values = indexed.projected.batch.column(0);
		let values = values
			.as_any()
			.downcast_ref::<UInt8Array>()
			.expect("indexed payload column");
		assert_eq!(values.values(), &[7, 7, 7]);
	}

	fn body_ranges_cover(
		ranges: &[crate::storage::row_group::RecordBodyByteRange],
		candidate: crate::storage::row_group::RecordBodyByteRange,
	) -> bool {
		let Ok(candidate_end) = candidate.end() else {
			return false;
		};
		ranges.iter().any(|range| {
			range.start.get() <= candidate.start.get()
				&& range
					.end()
					.is_ok_and(|range_end| range_end >= candidate_end)
		})
	}

	fn body_ranges_overlap(
		ranges: &[crate::storage::row_group::RecordBodyByteRange],
		candidate: crate::storage::row_group::RecordBodyByteRange,
	) -> bool {
		let Ok(candidate_end) = candidate.end() else {
			return false;
		};
		ranges.iter().any(|range| {
			let Ok(range_end) = range.end() else {
				return false;
			};
			range.start.get() < candidate_end && candidate.start.get() < range_end
		})
	}

	#[test]
	fn compaction_rewrites_table_bound_multirow_frame_with_row_group_directory() {
		let schema = Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("payload", DataType::Int64, false),
		]);
		let table_object_id = TableObjectId::new([0x88; 16]);
		let descriptor = TableSchemaDescriptorV1::new_with_layout(
			table_object_id,
			1,
			"analytics",
			"row_group_compaction",
			&schema,
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
			LocalityPolicyV1::IdentityProjection,
			RecordLayoutPolicy::MultiRow(
				MultiRowLayoutDescriptor::new(10, 10, 8 * 1024 * 1024).unwrap(),
			),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.unwrap();
		let mut catalog = TableSchemaCatalogV1::default();
		catalog.upsert(tenant(), descriptor.clone()).unwrap();
		let row_count = 10_usize;
		let batch = RecordBatch::try_new(
			Arc::new(schema),
			vec![
				Arc::new(Int64Array::from_iter_values(0..row_count as i64)) as Arc<dyn Array>,
				Arc::new(Int64Array::from_iter_values(
					(0..row_count).map(|row| 1_000_i64 + row as i64),
				)) as Arc<dyn Array>,
			],
		)
		.unwrap();
		let source_rows = (0..row_count)
			.map(|row| MultiRowSourceRow {
				source_row_index: row,
				key_identity: id(row as u64),
				routing_locality: Locality::new(7),
				row_hlc: hlc(300 + row as u64),
				is_tombstone: false,
				table_object_id: Some(table_object_id),
			})
			.collect::<Vec<_>>();
		let old = encode_multirow_from_survivors_with_policy(
			&batch,
			&source_rows,
			RecordBodyCompressionPolicy::Uncompressed,
		)
		.unwrap();
		let records = old
			.rows
			.iter()
			.map(|row| CompactionRecord {
				tenant_id: tenant(),
				key_identity: row.key_identity,
				routing_locality: row.routing_locality,
				old_location: RecordLocation {
					slab_id: SlabId::new(77),
					offset: 0,
					length: old.frame.len() as u32,
					hlc: row.row_hlc,
					record_row_index: row.record_row_index,
					is_tombstone: row.is_tombstone,
				},
				reclaim: false,
				retention_ambiguous: false,
				retention_lag_seconds: 0,
			})
			.collect::<Vec<_>>();
		let writer_config =
			crate::storage::row_group::RowGroupWriterConfig::point_read_with_overrides(
				RowGroupRowCount::new(4),
				RowGroupRowCount::new(4),
				RowCount::new(8),
				RecordBodyByteLen::new(1),
			)
			.with_enabled(true);
		let compacted = compact_physical_record_frame(
			&old.frame,
			&records,
			Some(&catalog),
			&writer_config,
			&EncodingWriterConfig::default(),
			&L2ChunkLayoutConfig::default(),
		)
		.unwrap()
		.expect("compacted frame");
		let compacted_again = compact_physical_record_frame(
			&old.frame,
			&records,
			Some(&catalog),
			&writer_config,
			&EncodingWriterConfig::default(),
			&L2ChunkLayoutConfig::default(),
		)
		.unwrap()
		.expect("compacted frame");
		let parsed = parse_record_buffer_directory(
			TieredRecordId {
				slab_id: SlabId::new(77),
				record_offset: RecordBodyByteOffset::ZERO,
			},
			PlaintextByteRange {
				start: 0,
				len: compacted.frame.len() as u64,
			},
			&compacted.frame,
			&descriptor,
		)
		.unwrap();
		let page_directory = parsed.page_directory.expect("row-group directory");

		assert_eq!(compacted.frame, compacted_again.frame);
		assert_eq!(page_directory.row_groups.len(), 3);
		assert_eq!(page_directory.row_count.get(), row_count as u64);
		assert_eq!(
			decode_record_batch(&compacted.frame).unwrap().num_rows(),
			row_count
		);
	}

	#[tokio::test]
	async fn encrypt_tiered_object_blob_rejects_empty_plain_slab() {
		let mgr = fresh_manager().await;
		let err = mgr
			.encrypt_tiered_object_blob(SlabId::new(77), &[])
			.unwrap_err();
		assert_eq!(err.kind(), ErrorKind::InvalidInput);
		assert!(
			err.to_string()
				.contains("does not contain any committed records"),
			"unexpected error: {err}"
		);
	}

	#[tokio::test]
	async fn decode_tiered_blob_rejects_footer_tenant_mismatch_with_payload_tenant() {
		let mgr = fresh_manager().await;
		let key = id(0xfeed);
		mgr.append(tenant(), key, hlc(10), b"tenant-bound payload")
			.await
			.unwrap();
		let slab_id = force_seal_active(&mgr).await;
		let plaintext = mgr.read_sealed_raw(slab_id).await.unwrap();

		let mismatched_tenant = TenantId::new(17);
		mgr.config
			.crypto
			.install_local_only_for_tests(mismatched_tenant)
			.unwrap();
		let encrypted = mgr
			.config
			.crypto
			.encrypt_tiered_slab_object(
				slab_id,
				&TieredSlabObjectMetadata {
					tenant_id: mismatched_tenant,
					created_hlc: 10,
					chunk_size_bytes: SLAB_ENCRYPTION_DEFAULT_CHUNK_SIZE,
					offset_index: vec![TieredOffsetIndexEntry {
						key_locator: IdentityPrefix::ZERO,
						byte_offset: 0,
						byte_length: plaintext.len().try_into().unwrap(),
						flags: 0,
					}],
					column_stats_block: None,
					has_row_tombstone_records: false,
				},
				&plaintext,
			)
			.unwrap();
		let pointer = TieredSlabPointer {
			slab_id,
			l2_uri: format!("memory://tiered/{slab_id:016x}"),
			payload_hash: crate::hash::content::digest(&encrypted.bytes),
			is_encrypted: true,
			tenant_id: Some(tenant()),
			tenant_kek_id: Some(encrypted.tenant_kek_id),
			stats_format_version: encrypted
				.stats_locator
				.map(|locator| locator.stats_format_version),
			stats_block_len: encrypted
				.stats_locator
				.map(|locator| locator.stats_block_len),
			stats_block_hash128: encrypted
				.stats_locator
				.map(|locator| locator.stats_block_hash128),
			tombstone_metadata: TombstoneMetadata::None,
			supersession_metadata: SupersessionMetadata::None,
		};

		let err =
			decode_tiered_blob(mgr.config.crypto.as_ref(), &pointer, &encrypted.bytes).unwrap_err();
		assert_eq!(err.kind(), ErrorKind::InvalidData);
		assert!(
			err.to_string().contains("footer tenant"),
			"unexpected error: {err}"
		);
	}

	#[tokio::test]
	async fn lookup_as_of_returns_historical_version() {
		let mgr = fresh_manager().await;
		let key = id(7);
		mgr.append(tenant(), key, hlc(10), b"v1").await.unwrap();
		mgr.append(tenant(), key, hlc(20), b"v2").await.unwrap();
		mgr.append(tenant(), key, hlc(30), b"v3").await.unwrap();

		assert_eq!(
			mgr.lookup_as_of(tenant(), key, hlc(15))
				.unwrap()
				.hlc
				.physical_ms,
			10
		);
		assert_eq!(
			mgr.lookup_as_of(tenant(), key, hlc(20))
				.unwrap()
				.hlc
				.physical_ms,
			20
		);
		assert_eq!(
			mgr.lookup_as_of(tenant(), key, hlc(25))
				.unwrap()
				.hlc
				.physical_ms,
			20
		);
		// Bound before the first write -> nothing visible yet.
		assert!(mgr.lookup_as_of(tenant(), key, hlc(5)).is_none());
	}

	#[tokio::test]
	async fn scan_all_versions_returns_every_retained_version_above_floor() {
		let mgr = fresh_manager().await;
		// Two keys, two versions each — one above floor, one below.
		let key_a = id(1);
		let key_b = id(2);
		mgr.append(tenant(), key_a, hlc(5), b"a@5").await.unwrap();
		mgr.append(tenant(), key_a, hlc(50), b"a@50").await.unwrap();
		mgr.append(tenant(), key_b, hlc(20), b"b@20").await.unwrap();
		mgr.append(tenant(), key_b, hlc(200), b"b@200")
			.await
			.unwrap();

		let hits = mgr.scan_all_versions(Locality::ZERO, Locality::MAX, hlc(10));
		// Floor at 10 drops (1, hlc=5); the remaining three survive.
		assert_eq!(hits.len(), 3);
		// Sorted first by locality/identity, then by hlc ascending.
		assert_eq!(hits[0].0, tenant());
		assert_eq!(hits[0].1, key_a);
		assert_eq!(hits[0].2, Locality::from(key_a));
		assert_eq!(hits[0].3.hlc.physical_ms, 50);
		assert_eq!(hits[1].0, tenant());
		assert_eq!(hits[1].1, key_b);
		assert_eq!(hits[1].2, Locality::from(key_b));
		assert_eq!(hits[1].3.hlc.physical_ms, 20);
		assert_eq!(hits[2].0, tenant());
		assert_eq!(hits[2].1, key_b);
		assert_eq!(hits[2].2, Locality::from(key_b));
		assert_eq!(hits[2].3.hlc.physical_ms, 200);

		// Inverted window → empty rather than panic.
		assert!(
			mgr.scan_all_versions(Locality::new(10), Locality::new(5), hlc(0))
				.is_empty()
		);
	}

	#[tokio::test]
	async fn append_migration_is_idempotent_on_identical_payload() {
		let mgr = fresh_manager().await;
		let key = id(42);
		let ts = hlc(100);
		let frame = b"payload-bytes";
		let hash = crate::hash::content::digest(frame);

		let first = mgr
			.append_migration(7, tenant(), key, ts, hash, frame)
			.await
			.unwrap();
		assert!(matches!(first, MigrationIngestOutcome::Inserted(_)));

		let second = mgr
			.append_migration(7, tenant(), key, ts, hash, frame)
			.await
			.unwrap();
		assert!(matches!(second, MigrationIngestOutcome::Duplicate(_)));

		// Only one version at this (key, hlc) should exist.
		let locality = Locality::from(key);
		let versions = mgr.scan_all_versions(locality, locality, hlc(0));
		assert_eq!(versions.len(), 1);
	}

	#[tokio::test]
	async fn append_migration_with_locality_preserves_custom_range_membership() {
		let source = fresh_manager().await;
		let destination = fresh_manager().await;
		let key = id(0xDADA);
		let identity_locality = Locality::from(key);
		let routing_locality = Locality::new(identity_locality.get().wrapping_add(1));
		assert_ne!(routing_locality, identity_locality);
		let ts = hlc(100);

		source
			.append_durable_with_locality(tenant(), key, routing_locality, ts, b"table-row")
			.await
			.unwrap();
		let source_versions = source.scan_all_versions(routing_locality, routing_locality, hlc(0));
		assert_eq!(source_versions.len(), 1);
		let (tenant_id, migrated_key, migrated_locality, source_loc) = source_versions[0];
		assert_eq!(migrated_key, key);
		assert_eq!(migrated_locality, routing_locality);

		let frame = source.read(source_loc).await.unwrap();
		let payload_hash = crate::hash::content::digest(&frame);
		destination
			.append_migration_with_locality(
				7,
				tenant_id,
				migrated_key,
				migrated_locality,
				source_loc.hlc,
				payload_hash,
				&frame,
			)
			.await
			.unwrap();

		let custom_hits = destination.scan_all_versions(routing_locality, routing_locality, hlc(0));
		assert_eq!(custom_hits.len(), 1);
		assert_eq!(custom_hits[0].1, key);
		assert_eq!(custom_hits[0].2, routing_locality);
		assert!(
			destination
				.scan_all_versions(identity_locality, identity_locality, hlc(0))
				.is_empty()
		);
	}

	#[tokio::test]
	async fn append_migration_rejects_conflicting_payload_for_same_slot() {
		let mgr = fresh_manager().await;
		let key = id(99);
		let ts = hlc(200);
		let frame_a = b"original";
		let hash_a = crate::hash::content::digest(frame_a);
		mgr.append_migration(1, tenant(), key, ts, hash_a, frame_a)
			.await
			.unwrap();

		// A peer claims different bytes for the exact same MVCC
		// slot — the mismatch must fail loudly, not silently
		// accept either side.
		let frame_b = b"tampered";
		let hash_b = crate::hash::content::digest(frame_b);
		let err = mgr
			.append_migration(1, tenant(), key, ts, hash_b, frame_b)
			.await
			.unwrap_err();
		assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
	}

	#[tokio::test]
	async fn scan_as_of_filters_versions_per_key() {
		let mgr = fresh_manager().await;
		// Two keys in the same range, each with two versions at
		// different HLCs.
		let key_a = id(1);
		let key_b = id(2);
		mgr.append(tenant(), key_a, hlc(10), b"a@10").await.unwrap();
		mgr.append(tenant(), key_a, hlc(30), b"a@30").await.unwrap();
		mgr.append(tenant(), key_b, hlc(20), b"b@20").await.unwrap();
		mgr.append(tenant(), key_b, hlc(40), b"b@40").await.unwrap();

		// AS OF 25 — should see key=1 at 10 (20 hasn't landed),
		// key=2 at 20.
		let at_25 = mgr.scan_as_of(tenant(), Locality::ZERO, Locality::MAX, hlc(25));
		assert_eq!(at_25.len(), 2);
		assert_eq!(at_25[0].0, Locality::from(key_a).get());
		assert_eq!(at_25[0].1.hlc.physical_ms, 10);
		assert_eq!(at_25[1].0, Locality::from(key_b).get());
		assert_eq!(at_25[1].1.hlc.physical_ms, 20);

		// AS OF 5 — nothing visible yet.
		assert!(
			mgr.scan_as_of(tenant(), Locality::ZERO, Locality::MAX, hlc(5))
				.is_empty()
		);

		// Zero sentinel -> latest versions.
		let latest = mgr.scan(tenant(), Locality::ZERO, Locality::MAX);
		assert_eq!(latest[0].1.hlc.physical_ms, 30);
		assert_eq!(latest[1].1.hlc.physical_ms, 40);
	}

	#[tokio::test]
	async fn scan_as_of_scopes_locality_membership_per_tenant() {
		let mgr = fresh_manager().await;
		let shared_identity = id(0x55);
		let locality = Locality::from(shared_identity);
		let other_tenant = TenantId::new(7);

		mgr.append(tenant(), shared_identity, hlc(10), b"tenant-a")
			.await
			.unwrap();
		mgr.append(other_tenant, shared_identity, hlc(20), b"tenant-b")
			.await
			.unwrap();

		let primary = mgr.scan_as_of(tenant(), locality, locality, hlc(30));
		assert_eq!(primary.len(), 1);
		assert_eq!(primary[0].0, locality.get());
		assert_eq!(primary[0].1.hlc.physical_ms, 10);

		let secondary = mgr.scan_as_of(other_tenant, locality, locality, hlc(30));
		assert_eq!(secondary.len(), 1);
		assert_eq!(secondary[0].0, locality.get());
		assert_eq!(secondary[0].1.hlc.physical_ms, 20);
	}

	#[tokio::test]
	async fn custom_routing_locality_survives_reopen() {
		let dir = fresh_dir("custom-locality-reopen");
		let manager = Arc::new(
			SlabManager::open(SlabManagerConfig {
				dir: dir.clone(),
				max_slab_bytes: 1024 * 1024,
				io: spawn_io_thread(64).unwrap(),
				l2: None,
				rehydration: RehydrationPolicy::default(),
				l2_column_fetch: Default::default(),
				row_group_writer: Default::default(),
				l2_row_group_fetch: Default::default(),
				encoding_writer: Default::default(),
				l2_chunk_layout: Default::default(),
				l2_encoding_fetch: Default::default(),
				encoding_compute: Default::default(),
				group_commit: GroupCommitConfig::default(),
				crypto: test_crypto(),
			})
			.await
			.unwrap(),
		);
		let key = id(0xCAFE);
		let routing_locality = Locality::new(42);
		assert_ne!(routing_locality, Locality::from(key));
		manager
			.append_durable_with_locality(tenant(), key, routing_locality, hlc(10), b"table-row")
			.await
			.unwrap();
		assert_eq!(
			manager
				.scan_as_of_with_identity_and_locality(
					tenant(),
					routing_locality,
					routing_locality,
					hlc(20),
				)
				.len(),
			1
		);
		drop(manager);

		let reopened = SlabManager::open(SlabManagerConfig {
			dir,
			max_slab_bytes: 1024 * 1024,
			io: spawn_io_thread(64).unwrap(),
			l2: None,
			rehydration: RehydrationPolicy::default(),
			l2_column_fetch: Default::default(),
			row_group_writer: Default::default(),
			l2_row_group_fetch: Default::default(),
			encoding_writer: Default::default(),
			l2_chunk_layout: Default::default(),
			l2_encoding_fetch: Default::default(),
			encoding_compute: Default::default(),
			group_commit: GroupCommitConfig::default(),
			crypto: test_crypto(),
		})
		.await
		.unwrap();
		let custom_hits = reopened.scan_as_of_with_identity_and_locality(
			tenant(),
			routing_locality,
			routing_locality,
			hlc(20),
		);
		assert_eq!(custom_hits.len(), 1);
		assert_eq!(custom_hits[0].0, key);
		assert_eq!(custom_hits[0].1, routing_locality);
		assert!(
			reopened
				.scan_as_of_with_identity_and_locality(
					tenant(),
					Locality::from(key),
					Locality::from(key),
					hlc(20),
				)
				.is_empty()
		);
	}

	#[tokio::test]
	async fn append_durable_coalesces_concurrent_fdatasyncs() {
		let mgr = fresh_manager_with_group_commit(GroupCommitConfig {
			floor: Duration::from_millis(20),
			ceiling: Duration::from_millis(20),
			per_write_cost_estimate: Duration::from_millis(20),
		})
		.await;

		let first = {
			let mgr = mgr.clone();
			tokio::spawn(async move {
				mgr.append_durable(tenant(), id(0x11), hlc(10), b"first")
					.await
					.unwrap();
			})
		};
		tokio::time::sleep(Duration::from_millis(1)).await;
		let second = {
			let mgr = mgr.clone();
			tokio::spawn(async move {
				mgr.append_durable(tenant(), id(0x22), hlc(20), b"second")
					.await
					.unwrap();
			})
		};
		first.await.unwrap();
		second.await.unwrap();

		let fdatasync_calls = {
			let state = mgr.state.lock().await;
			state.active.fdatasync_call_count()
		};
		assert_eq!(fdatasync_calls, 1);
	}

	#[tokio::test]
	async fn reopen_restores_tiered_rows_from_redb() {
		let l1_dir = fresh_dir("tiered-reopen-l1");
		let l2_dir = fresh_dir("tiered-reopen-l2");
		let io = spawn_io_thread(64).unwrap();
		let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&l2_dir).unwrap());
		let crypto = test_crypto();

		let manager = SlabManager::open(SlabManagerConfig {
			dir: l1_dir.clone(),
			max_slab_bytes: 1,
			io: io.clone(),
			l2: Some(l2.clone()),
			rehydration: RehydrationPolicy::default(),
			l2_column_fetch: Default::default(),
			row_group_writer: Default::default(),
			l2_row_group_fetch: Default::default(),
			encoding_writer: Default::default(),
			l2_chunk_layout: Default::default(),
			l2_encoding_fetch: Default::default(),
			encoding_compute: Default::default(),
			group_commit: GroupCommitConfig::default(),
			crypto: crypto.clone(),
		})
		.await
		.unwrap();

		let tiered_key = id(0x44);
		let other_key = id(0x99);
		let tiered_bytes = b"tiered-row".to_vec();
		manager
			.append(tenant(), tiered_key, hlc(10), &tiered_bytes)
			.await
			.unwrap();
		manager
			.append(tenant(), other_key, hlc(20), b"active-row")
			.await
			.unwrap();

		let sealed = manager.list_sealed_slabs().await;
		assert_eq!(sealed.len(), 1, "expected one sealed slab after rotation");
		let sealed_id = sealed[0].slab_id;
		let raw = manager.read_sealed_raw(sealed_id).await.unwrap();
		let encrypted = manager.encrypt_tiered_object_blob(sealed_id, &raw).unwrap();
		let payload_hash = crate::hash::content::digest(&encrypted.bytes);
		let uri = l2
			.put(
				&format!("sealed/slab_{sealed_id:016x}.wnd"),
				&encrypted.bytes,
			)
			.await
			.unwrap();
		manager
			.apply_tier_slab(TieredSlabPointer {
				slab_id: sealed_id,
				l2_uri: uri,
				payload_hash,
				is_encrypted: true,
				tenant_id: Some(tenant()),
				tenant_kek_id: Some(encrypted.tenant_kek_id),
				stats_format_version: encrypted
					.stats_locator
					.map(|locator| locator.stats_format_version),
				stats_block_len: encrypted
					.stats_locator
					.map(|locator| locator.stats_block_len),
				stats_block_hash128: encrypted
					.stats_locator
					.map(|locator| locator.stats_block_hash128),
				tombstone_metadata: TombstoneMetadata::None,
				supersession_metadata: SupersessionMetadata::None,
			})
			.await
			.unwrap();
		assert!(manager.is_tiered(sealed_id).await);
		assert!(
			!slab_path(&l1_dir, sealed_id).exists(),
			"tiered slab should be unlinked from L1"
		);

		drop(manager);

		let reopened = SlabManager::open(SlabManagerConfig {
			dir: l1_dir,
			max_slab_bytes: 1,
			io,
			l2: Some(l2),
			rehydration: RehydrationPolicy::default(),
			l2_column_fetch: Default::default(),
			row_group_writer: Default::default(),
			l2_row_group_fetch: Default::default(),
			encoding_writer: Default::default(),
			l2_chunk_layout: Default::default(),
			l2_encoding_fetch: Default::default(),
			encoding_compute: Default::default(),
			group_commit: GroupCommitConfig::default(),
			crypto: crypto.clone(),
		})
		.await
		.unwrap();
		assert!(reopened.is_tiered(sealed_id).await);

		let location = reopened
			.lookup(tenant(), tiered_key)
			.expect("tiered key should survive reopen");
		assert_eq!(location.slab_id, sealed_id);
		assert_eq!(reopened.read(location).await.unwrap(), tiered_bytes);
	}

	#[tokio::test]
	async fn cleanup_retention_rewrite_tmp_files_removes_stale_plaintext() {
		let dir = fresh_dir("retention-tmp-cleanup");
		let tmp_path = retention_rewrite_tmp_path(&dir, SlabId::new(0x77));
		std::fs::write(&tmp_path, b"plaintext-leak").unwrap();
		assert!(
			tmp_path.exists(),
			"fixture temp file should exist before open"
		);

		let file_ops = BlockingFileOps;
		cleanup_retention_rewrite_tmp_files(&file_ops, &dir)
			.await
			.unwrap();

		assert!(
			!tmp_path.exists(),
			"startup cleanup should remove stale plaintext retention temp files"
		);
	}

	#[tokio::test]
	async fn reopen_after_tombstone_compaction_preserves_survivor_lookup() {
		let dir = fresh_dir("compaction-redb");
		let io = spawn_io_thread(64).unwrap();
		let manager = Arc::new(
			SlabManager::open(SlabManagerConfig {
				dir: dir.clone(),
				max_slab_bytes: 1024 * 1024,
				io: io.clone(),
				l2: None,
				rehydration: RehydrationPolicy::default(),
				l2_column_fetch: Default::default(),
				row_group_writer: Default::default(),
				l2_row_group_fetch: Default::default(),
				encoding_writer: Default::default(),
				l2_chunk_layout: Default::default(),
				l2_encoding_fetch: Default::default(),
				encoding_compute: Default::default(),
				group_commit: GroupCommitConfig::default(),
				crypto: test_crypto(),
			})
			.await
			.unwrap(),
		);

		let removed_key = id(0x1010);
		let survivor_key = id(0x2020);
		manager
			.append_durable(tenant(), removed_key, hlc(10), b"drop-me")
			.await
			.unwrap();
		manager
			.append_durable(tenant(), survivor_key, hlc(20), b"keep-me")
			.await
			.unwrap();
		let sealed_id = force_seal_active(&manager).await;
		manager
			.append_durable(tenant(), id(0x3030), hlc(30), b"active-row")
			.await
			.unwrap();

		let target = Locality::from(removed_key).get();
		let report = manager
			.reclaim_tombstoned_ranges(
				&[MigratedRangeTombstone {
					migration_id: 1,
					range_start: target,
					range_end: target,
					to_epoch: 2,
					completed_by: NodeId {
						id: 7,
						_reserved: 0,
					},
					completed_at_hlc: hlc(1),
					migration_hlc_floor: hlc(0),
				}],
				1,
			)
			.await
			.unwrap();
		assert_eq!(report.compacted_slabs, 1);
		assert!(
			manager.lookup(tenant(), removed_key).is_none(),
			"reclaimed key should disappear from the in-memory index"
		);

		let survivor = manager
			.lookup(tenant(), survivor_key)
			.expect("survivor should remain indexed");
		assert_ne!(
			survivor.slab_id, sealed_id,
			"compaction should relocate the survivor into a replacement slab"
		);
		assert_eq!(manager.read(survivor).await.unwrap(), b"keep-me");

		drop(manager);

		let reopened = SlabManager::open(SlabManagerConfig {
			dir,
			max_slab_bytes: 1024 * 1024,
			io,
			l2: None,
			rehydration: RehydrationPolicy::default(),
			l2_column_fetch: Default::default(),
			row_group_writer: Default::default(),
			l2_row_group_fetch: Default::default(),
			encoding_writer: Default::default(),
			l2_chunk_layout: Default::default(),
			l2_encoding_fetch: Default::default(),
			encoding_compute: Default::default(),
			group_commit: GroupCommitConfig::default(),
			crypto: test_crypto(),
		})
		.await
		.unwrap();
		assert!(
			reopened.lookup(tenant(), removed_key).is_none(),
			"reclaimed key should stay absent after reopen"
		);
		let survivor = reopened
			.lookup(tenant(), survivor_key)
			.expect("survivor should still be indexed after reopen");
		assert_eq!(reopened.read(survivor).await.unwrap(), b"keep-me");
	}

	#[tokio::test]
	async fn lsm_compaction_rejects_sidecar_covered_source_runs_without_catalog_context() {
		let manager = fresh_manager().await;
		manager
			.append_durable(tenant(), id(0x55CA), hlc(10), b"sidecar-covered")
			.await
			.unwrap();
		let source_slab_id = manager.force_seal_active_for_test().await;

		{
			let mut manifest =
				recover_rwlock_write(&manager.compaction_manifest, "compaction_manifest");
			let mut run = manifest
				.get_run(source_slab_id)
				.expect("sealed slab should be present in the LSM manifest")
				.clone();
			run.sidecar_coverage = crate::storage::SidecarCoverage::Complete;
			manifest.insert_live_run(run);
		}

		let snapshot = manager.compaction_manifest_snapshot();
		let plan = CompactionPlan {
			job_id: CompactionJobId::new(0x55CA),
			source_level: CompactionLevelIndex::L0,
			target_level: CompactionLevelIndex::L1,
			manifest_epoch_pre: snapshot.manifest_epoch,
			input_slabs: vec![source_slab_id],
			overlapping_slabs: Vec::new(),
			retention_floor_snapshot: hlc(20),
			compaction_start_hlc: hlc(21),
			dominant_reason: crate::storage::DominantReason::Operator,
		};
		let commit = CompactionCommit {
			job_id: plan.job_id,
			manifest_epoch_pre: plan.manifest_epoch_pre,
			slabs_to_retire: vec![source_slab_id],
			slabs_to_add: Vec::new(),
			slabs_to_mark_delete_pending: Vec::new(),
			slabs_to_remove: Vec::new(),
			sidecar_swaps: Vec::new(),
			digest_summary: CompactionDigestSummary::zero(),
		};

		let err = manager
			.validate_lsm_sidecar_swap_readiness(&plan, &commit, false)
			.unwrap_err();
		assert!(matches!(err, CompactionFailure::SidecarDigestMismatch));
	}

	#[tokio::test]
	async fn lsm_compaction_copies_whole_multirow_frame_into_output_run() {
		let manager = fresh_manager().await;
		let first_key = id(0x6001);
		let second_key = id(0x6002);
		let frame = encode_test_multirow_storage_frame(&[
			(first_key, Locality::new(0x61), hlc(10), 11),
			(second_key, Locality::new(0x62), hlc(20), 22),
		]);
		let entries = storage_multirow_index_entries_from_frame(&frame)
			.unwrap()
			.expect("test frame should be a storage multi-row frame");
		let source_location = manager
			.append_durable_multirow_with_locality(tenant(), &frame, &entries)
			.await
			.unwrap();
		let source_slab_id = manager.force_seal_active_for_test().await;
		assert_eq!(source_location.slab_id, source_slab_id);

		let snapshot = manager.compaction_manifest_snapshot();
		let plan = CompactionPlan {
			job_id: CompactionJobId::new(0x6001),
			source_level: CompactionLevelIndex::L0,
			target_level: CompactionLevelIndex::L1,
			manifest_epoch_pre: snapshot.manifest_epoch,
			input_slabs: vec![source_slab_id],
			overlapping_slabs: Vec::new(),
			retention_floor_snapshot: hlc(100),
			compaction_start_hlc: hlc(101),
			dominant_reason: crate::storage::DominantReason::Operator,
		};
		let worker = LsmCompactionWorker::new(
			crate::storage::LsmConfig::default(),
			manager.slab_pin_table(),
		)
		.unwrap();

		let outcome = manager
			.run_lsm_compaction_once(&worker, plan)
			.await
			.unwrap();

		assert_eq!(outcome.rows_out, 2);
		assert_eq!(outcome.output_slabs.len(), 1);
		let output_slab_id = outcome.output_slabs[0].slab_id;
		let first = manager.lookup(tenant(), first_key).unwrap();
		let second = manager.lookup(tenant(), second_key).unwrap();
		assert_eq!(first.slab_id, output_slab_id);
		assert_eq!(second.slab_id, output_slab_id);
		assert_ne!(output_slab_id, source_slab_id);
		assert_eq!(first.offset, second.offset);
		assert_eq!(first.length, second.length);
		assert_eq!(first.record_row_index, RecordRowIndex::ZERO);
		assert_eq!(second.record_row_index, RecordRowIndex::new(1));
		assert!(!manager.read(first).await.unwrap().is_empty());
		assert!(!manager.read(second).await.unwrap().is_empty());
	}

	#[tokio::test]
	async fn migrated_range_tombstone_marks_tiered_slab_visibility_incomplete() {
		let manager = fresh_manager().await;
		let key = id(0x5151);
		manager
			.append_durable(tenant(), key, hlc(10), b"migrated")
			.await
			.unwrap();
		let sealed_id = force_seal_active(&manager).await;
		{
			let state = manager.state.lock().await;
			state.sealed.remove(&sealed_id);
			state.tiered.insert(
				sealed_id,
				TieredSlabPointer {
					slab_id: sealed_id,
					l2_uri: "memory://tiered".to_owned(),
					payload_hash: [0xAA; 32],
					is_encrypted: false,
					tenant_id: Some(tenant()),
					tenant_kek_id: None,
					stats_format_version: None,
					stats_block_len: None,
					stats_block_hash128: None,
					tombstone_metadata: TombstoneMetadata::None,
					supersession_metadata: SupersessionMetadata::None,
				},
			);
		}

		let locality = Locality::from(key).get();
		let tombstone = MigratedRangeTombstone {
			migration_id: 9,
			range_start: locality,
			range_end: locality,
			to_epoch: 2,
			completed_by: NodeId {
				id: 7,
				_reserved: 0,
			},
			completed_at_hlc: hlc(50),
			migration_hlc_floor: hlc(0),
		};
		assert!(!manager.slab_has_migrated_range_tombstone_at(
			sealed_id,
			hlc(49),
			std::slice::from_ref(&tombstone)
		));
		assert!(manager.slab_has_migrated_range_tombstone_at(
			sealed_id,
			hlc(50),
			std::slice::from_ref(&tombstone)
		));

		let changed = manager
			.record_migrated_range_tombstone(&tombstone, Some(99))
			.await
			.unwrap();
		assert_eq!(changed, 1);
		let pointer = manager.tier_pointer(sealed_id).await.unwrap();
		assert_eq!(
			pointer.tombstone_metadata,
			TombstoneMetadata::Earliest(hlc(50))
		);
		assert!(crate::storage::slab_visibility_complete(
			hlc(10),
			pointer.tombstone_metadata,
			SupersessionMetadata::None,
			hlc(49)
		));
		assert!(!crate::storage::slab_visibility_complete(
			hlc(10),
			pointer.tombstone_metadata,
			SupersessionMetadata::None,
			hlc(50)
		));
	}

	#[tokio::test]
	async fn post_tier_supersession_updates_tiered_pointer_visibility() {
		let manager = fresh_manager().await;
		let key = id(0x5151);
		manager
			.append_durable(tenant(), key, hlc(10), b"v10")
			.await
			.unwrap();
		let sealed_id = force_seal_active(&manager).await;
		manager
			.apply_tier_slab(TieredSlabPointer {
				slab_id: sealed_id,
				l2_uri: "memory://supersession".into(),
				payload_hash: [0x55; 32],
				is_encrypted: false,
				tenant_id: None,
				tenant_kek_id: None,
				stats_format_version: None,
				stats_block_len: None,
				stats_block_hash128: None,
				tombstone_metadata: TombstoneMetadata::None,
				supersession_metadata: SupersessionMetadata::None,
			})
			.await
			.unwrap();

		manager
			.append_durable(tenant(), key, hlc(20), b"v20")
			.await
			.unwrap();

		let pointer = manager.tier_pointer(sealed_id).await.unwrap();
		assert_eq!(
			pointer.supersession_metadata,
			SupersessionMetadata::Earliest(hlc(20))
		);
		assert!(!crate::storage::slab_visibility_complete(
			hlc(10),
			TombstoneMetadata::None,
			pointer.supersession_metadata,
			hlc(20)
		));
	}

	#[tokio::test]
	async fn post_tier_row_tombstone_updates_tiered_pointer_visibility() {
		let manager = fresh_manager().await;
		let key = id(0x5152);
		let locality = Locality::new(52);
		let data_frame = encode_test_multirow_storage_frame(&[(key, locality, hlc(10), 100)]);
		let data_entries = storage_multirow_index_entries_from_frame(&data_frame)
			.unwrap()
			.expect("data entry");
		manager
			.append_durable_multirow_with_locality(tenant(), &data_frame, &data_entries)
			.await
			.unwrap();
		let sealed_id = force_seal_active(&manager).await;
		manager
			.apply_tier_slab(TieredSlabPointer {
				slab_id: sealed_id,
				l2_uri: "memory://row-tombstone".into(),
				payload_hash: [0x56; 32],
				is_encrypted: false,
				tenant_id: None,
				tenant_kek_id: None,
				stats_format_version: None,
				stats_block_len: None,
				stats_block_hash128: None,
				tombstone_metadata: TombstoneMetadata::None,
				supersession_metadata: SupersessionMetadata::None,
			})
			.await
			.unwrap();

		let (tombstone_frame, tombstone_entries) = encode_test_row_tombstone_marker_frame(
			TableObjectId::new([0x51; 16]),
			key,
			locality,
			hlc(20),
		);
		manager
			.append_durable_multirow_with_locality(tenant(), &tombstone_frame, &tombstone_entries)
			.await
			.unwrap();

		let pointer = manager.tier_pointer(sealed_id).await.unwrap();
		assert_eq!(
			pointer.tombstone_metadata,
			TombstoneMetadata::Earliest(hlc(20))
		);
		assert!(!crate::storage::slab_visibility_complete(
			hlc(10),
			pointer.tombstone_metadata,
			SupersessionMetadata::None,
			hlc(20)
		));
	}

	#[tokio::test]
	async fn tiered_pk_tombstone_slab_records_row_tombstone_and_resurrection_metadata() {
		let manager = fresh_manager().await;
		let descriptor = pk_retention_descriptor(TableObjectId::new([0x5A; 16]));
		let key = id(0x5A01);
		let locality = Locality::new(0x5A);
		let data_frame = encode_test_append_only_storage_frame(
			descriptor.table_object_id,
			&[(key, locality, hlc(10), 100, false)],
		);
		let data_entries = storage_multirow_index_entries_from_frame(&data_frame)
			.unwrap()
			.expect("pk data entry");
		manager
			.append_durable_multirow_with_locality(tenant(), &data_frame, &data_entries)
			.await
			.unwrap();
		let (tombstone_frame, tombstone_entries) =
			encode_test_pk_tombstone_marker_frame(&descriptor, key, locality, hlc(20));
		manager
			.append_durable_multirow_with_locality(tenant(), &tombstone_frame, &tombstone_entries)
			.await
			.unwrap();
		let sealed_id = force_seal_active(&manager).await;
		assert_eq!(
			manager.slab_row_tombstone_metadata(sealed_id),
			TombstoneMetadata::Earliest(hlc(20))
		);
		manager
			.apply_tier_slab(TieredSlabPointer {
				slab_id: sealed_id,
				l2_uri: "memory://pk-row-tombstone".into(),
				payload_hash: [0x5A; 32],
				is_encrypted: false,
				tenant_id: None,
				tenant_kek_id: None,
				stats_format_version: None,
				stats_block_len: None,
				stats_block_hash128: None,
				tombstone_metadata: manager.slab_row_tombstone_metadata(sealed_id),
				supersession_metadata: SupersessionMetadata::None,
			})
			.await
			.unwrap();

		let resurrected_frame = encode_test_append_only_storage_frame(
			descriptor.table_object_id,
			&[(key, locality, hlc(30), 200, false)],
		);
		let resurrected_entries = storage_multirow_index_entries_from_frame(&resurrected_frame)
			.unwrap()
			.expect("pk resurrection entry");
		manager
			.append_durable_multirow_with_locality(
				tenant(),
				&resurrected_frame,
				&resurrected_entries,
			)
			.await
			.unwrap();

		let pointer = manager.tier_pointer(sealed_id).await.unwrap();
		assert_eq!(
			pointer.tombstone_metadata,
			TombstoneMetadata::Earliest(hlc(20))
		);
		assert_eq!(
			pointer.supersession_metadata,
			SupersessionMetadata::Earliest(hlc(30))
		);
		assert!(!crate::storage::slab_visibility_complete(
			hlc(20),
			pointer.tombstone_metadata,
			pointer.supersession_metadata,
			hlc(30)
		));
	}

	#[tokio::test]
	async fn retention_pruning_keeps_latest_version_before_floor() {
		let manager = fresh_manager().await;
		let key = id(0x4040);
		manager
			.append_durable(tenant(), key, hlc(10), b"v10")
			.await
			.unwrap();
		manager
			.append_durable(tenant(), key, hlc(20), b"v20")
			.await
			.unwrap();
		manager
			.append_durable(tenant(), key, hlc(30), b"v30")
			.await
			.unwrap();
		let sealed_id = force_seal_active(&manager).await;
		assert_eq!(manager.sealed_count().await, 1);

		let report = manager.prune_versions_older_than(hlc(25), 1).await.unwrap();
		assert_eq!(report.compacted_slabs, 1);
		assert_eq!(report.pruned_versions, 1);
		assert!(report.reclaimed_bytes > 0);

		let as_of_25 = manager
			.lookup_as_of(tenant(), key, hlc(25))
			.expect("anchor version retained");
		assert_ne!(as_of_25.slab_id, sealed_id);
		assert_eq!(as_of_25.hlc.physical_ms, 20);
		assert_eq!(manager.read(as_of_25).await.unwrap(), b"v20");

		let latest = manager.lookup(tenant(), key).expect("latest retained");
		assert_eq!(latest.hlc.physical_ms, 30);
		assert_eq!(manager.read(latest).await.unwrap(), b"v30");

		assert!(
			manager.lookup_as_of(tenant(), key, hlc(15)).is_none(),
			"versions older than the retained window should be pruned"
		);
	}

	#[tokio::test]
	async fn retention_pruning_keeps_latest_when_all_versions_precede_floor() {
		let manager = fresh_manager().await;
		let key = id(0x5050);
		manager
			.append_durable(tenant(), key, hlc(10), b"v10")
			.await
			.unwrap();
		manager
			.append_durable(tenant(), key, hlc(20), b"v20")
			.await
			.unwrap();
		force_seal_active(&manager).await;

		let report = manager
			.prune_versions_older_than(hlc(100), 1)
			.await
			.unwrap();
		assert_eq!(report.compacted_slabs, 1);
		assert_eq!(report.pruned_versions, 1);

		let latest = manager.lookup(tenant(), key).expect("latest retained");
		assert_eq!(latest.hlc.physical_ms, 20);
		assert_eq!(manager.read(latest).await.unwrap(), b"v20");
		assert!(
			manager.lookup_as_of(tenant(), key, hlc(15)).is_none(),
			"earlier history should be removed once the full chain is outside retention"
		);
	}
}
