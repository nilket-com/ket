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

use crate::consensus::{MigratedRangeTombstone, TieredSlab};
use crate::crypto::{
	CryptoManager, DecryptedTieredSlab, EncryptedTieredSlab, FooterSummary,
	SLAB_ENCRYPTION_DEFAULT_CHUNK_SIZE, TieredColumnStatsBlock, TieredMultiRangeReadPlan,
	TieredOffsetIndexEntry, TieredPlaintextRange, TieredSlabObjectMetadata,
};
use crate::metrics::{LsmCompactionResult, LsmDigestInvariantKind, global as metrics};
use crate::schema::table::PlacementSemantics;
use crate::schema::table::{
	RecordBodyCompressionPolicy, RecordLayoutPolicy, RowIdentityPolicy, TableObjectId,
	TableSchemaCatalog, TableSchemaDescriptor, TableWriteRequestMeta,
	validate_table_write_request_batch,
};
use crate::schema::{
	ARROW_STATS_TAG_FLOAT32, ARROW_STATS_TAG_FLOAT64, ARROW_STATS_TAG_INT32, ARROW_STATS_TAG_INT64,
	AggregatePartial, AggregateRequest, BufferValueCount, ByteCount, ColumnGroupNdvFact,
	ColumnGroupTopKEntry, ColumnGroupTopKFact, ColumnHeavyHitterEntry, ColumnHeavyHitterFact,
	ColumnNdvFact, ColumnRangeDistributionFact, ColumnRangeFact, CompactionJobId,
	CompactionLevelIndex, EncodingPageByteOffset, HllPrecision, HybridTimestamp,
	INDEX_REJECT_LOGICAL_FINGERPRINT_MISMATCH, Identity, IdentityPrefix, IndexObjectId,
	IpcBufferIndex, IpcColumnOrdinal, Locality, LogicalSchemaFingerprint, ManifestEpoch,
	MigrationCursor, NdvFact, NodeId, RecordBodyByteLen, RecordBodyByteOffset, RecordRowIndex,
	RowCount, RowGroupIndex, SecondaryIndexCatalog, SecondaryIndexDescriptor, SecondaryIndexState,
	SelectivityFractionMicros, SimdPageValueCount, SlabId, SlabRecordHeader, SupersessionMetadata,
	TableLifecycleCatalog, TableOptimizerFacts, TableReferenceIntegrityFailureMeta, TenantId,
	TenantKekId, TombstoneMetadata, WriterNodeId, range_fact_tag_supported,
};
use crate::storage::columnar::{
	ColumnFetchRequirement, ColumnOrdinalMapping, ColumnOrdinalSet, ColumnarFallbackReason,
	L2ColumnFetchConfig, L2FetchStrategy, PlaintextByteRange, ProjectedRecordBatch,
	RecordBufferDirectory, StorageColumn, TieredRecordId, build_column_fetch_plan,
	decode_projected_record_batch, decode_projected_record_batch_from_plaintext_ranges,
	initial_metadata_probe_plan, next_metadata_probe_plan, observe_l2_fetch_strategy,
	parse_record_buffer_directory, pick_l2_fetch_strategy, selected_record_body_ranges,
	survival_mask_projected_record_batch,
};
use crate::storage::core::{
	AppendInFlight, AppendReservation, GroupCommitConfig, LocalSlabStore, MultiRowIndexEntry,
	RecordLocation, RecoveredSlabEntry, StorageEngine, insert_version_sorted, newest_version_as_of,
	recover_slab_entries_from_blob, recover_slab_entries_from_blob_skipping_counters,
	remove_version_exact, storage_multirow_index_entries_from_frame, validate_row_tombstone_chain,
};
use crate::storage::encoding::compute::{
	EncodedAggregateContext, EncodedPredicateNode, EncodedRowMask, EncodingComputeFallbackReason,
	EncodingComputeRejectCode, NativeEvaluationEligibility, PredicateAtom, aggregate_kind_for_tag,
	coerce_literal, encoded_leaf_selectivity_allowed, encoded_predicate_node_from_tree,
	evaluate as evaluate_encoded_predicate, evaluate_fsst_predicate,
	evaluate_fsst_row_safe_predicate, evaluate_validity_bitmap, evaluator_kind_for_cascade,
	native_aggregate_cascade_eligible, native_evaluation_eligibility,
	try_fold_encoded_aggregate_request,
};
use crate::storage::encoding::directory::page_stats_cascade_eligible;
use crate::storage::encoding::materialize::{
	decode_encoded_validity_buffer, encoded_validity_cascade_supported, find_buffer_encoding,
	ipc_stream_contains_encoding_directory,
};
use crate::storage::encoding::page::expand_page_descriptors;
use crate::storage::encoding::page_materialize::{
	PageMaterializeBufferKey, PageMaterializeColumn, PageMaterializeFetch, PageMaterializeKey,
	PageMaterializeValiditySource, PageMaterializeValueSource, PageValidityBitmap,
	decode_page_fetch, decode_rle_validity_page, empty_projected_batch_for_requirement,
	encoded_page_body_range, extract_record_body_bytes as extract_encoded_page_bytes,
	filtered_predicate_page_batch, fixed_width_encoding_indexed_value_column,
	projected_page_batch_from_decoded_buffers, rebase_validity_bitmap,
	required_projected_physical_ordinals, synthesize_all_null_page_buffers,
};
use crate::storage::encoding::predicate_page_fetch::{
	PagePredicateClassification, PredicatePageFetchPlan, PredicatePageMaskSlice,
	classify_page_stats_for_atom, stitch_predicate_page_mask,
};
use crate::storage::encoding::sparse_projection_fetch::{
	SelectedPageDisposition, SparseProjectionFetchDecision, SparseProjectionFetchInput,
	SparseProjectionFetchPlan, decide_sparse_projection_fetch,
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
use crate::storage::maintenance_policy::{
	MaintenanceAttempt, MaintenanceOperation, MaintenancePolicyInner, SuccessDisposition,
	SystemMaintenanceClock, classify_compaction_failure, classify_io_error,
};
use crate::storage::multirow::{
	MultiRowSourceRow, StorageRowMetadata, decode_batch as decode_record_batch,
	encode_batch_with_policy, encode_multirow_from_survivors_with_policy,
	encode_multirow_from_survivors_with_row_group_encoding_and_chunk_layout_config,
	has_storage_synthetic_columns, materialize_record_location_batch,
	pack_multirow_source_rows_with_policy_row_group_encoding_and_chunk_layout_config,
};
use crate::storage::ownership::DataDirectoryOwner;
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
use crate::storage::sketch::HllRegisters;
use crate::storage::stats::{
	COLUMN_STAT_MAX_TRUNCATED, COLUMN_STAT_MIN_TRUNCATED, ColumnStatsDescriptorIdentity,
	ColumnStatsGroupTopKEntry, ColumnStatsGroupTopKPayload, ParsedColumnStatsBlock,
	arrow_stats_type_tag, build_column_stats_block_with_ciphertext_budget,
	decode_column_stats_payload, group_top_k_payload_encoded_len,
	locator_flags_for_parsed_column_stats_block,
	max_column_stats_ciphertext_bytes_for_slab_payload, parse_column_stats_block,
};
use crate::storage::{
	AsyncFileOps, BlockingFileOps, BlockingIoClass, CompactionCommit, CompactionDigestSummary,
	CompactionFailure, CompactionManifest, CompactionPlan, IoUringFileOps, LsmCompactionDriver,
	LsmCompactionJobRunner, LsmCompactionWorker, LsmGcDriver, LsmGcWorker, LsmPreparedOutputs,
	LsmRunSummary, LsmSourceFrame, LsmWrittenOutputRun, ManifestSnapshot, MvccRetentionPolicy,
	OpenFlags, SlabPinGuard, SlabPinTable, SlabState, TierCommitter, VersionEntry,
	VersionLifecycle, VisibleDomainRow, apply_elision, cluster_retention_floor,
	compaction_visible_domain_summary, effective_table_retention, required_output_run_count,
	storage_row_table_object_id_at, survivor_records,
};
use crate::storage::{core, counter};
use arrow::array::{Array, BooleanArray};
use arrow::compute::concat_batches;
use arrow::datatypes::DataType;
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use bytes::Bytes;
use dashmap::{DashMap, DashSet};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::io::{Cursor, Error, ErrorKind};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::sync::{Mutex, OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock as AsyncRwLock};
use tokio::task::JoinSet;
use tokio::time::Instant;
use zeroize::Zeroizing;

mod counter_visibility;
mod publication_accounting;
use publication_accounting::{CounterPendingGuards, RetainedSecondarySidecars};
mod encoding_compute;
mod metadata_observation;
mod observation;
mod open;
mod slab_ids;
pub(crate) use open::IndexRecoveryAuthority;
mod gc_disposal;
mod reservations;
pub use reservations::{ExposedSlab, OversizedReservation, ReservedSlab};
mod op_hooks;
mod op_recovery;
pub(crate) use slab_ids::{OwnTierView, OwnerEvidence, Ownership};
use slab_ids::{SLAB_ID_LEASE, slab_ids_exhausted};
mod retention_reclaim;
mod stats_facts;
use retention_reclaim::{
	cleanup_retention_rewrite_tmp_files, retention_rewrite_tmp_path, table_drop_rewrite_tmp_path,
};
mod backing_install;
mod disposal_evidence;
mod redirect_store;
mod relocation;
pub(crate) mod tier_authority;
mod tier_budget;
mod tier_deferral;
mod tier_generation;
#[cfg(any(test, feature = "test-support"))]
mod tier_measure;
mod tier_tokens;
pub use tier_tokens::{TierProposal, TierSubmission};
mod tier_plan;
mod version_identity;
use tier_generation::{ReadTarget, TierLayout};
pub use tier_generation::{
	StaleLocation, StaleLocationReason, stale_location, stale_location_error,
};
use version_identity::IncomingVersion;
mod slab_file_quarantine;
mod tiered_io;
use relocation::{
	KeyRelocation, RelocationInsertion, RelocationPair, RelocationScratch,
	decrement_reference_count,
};
pub use slab_file_quarantine::{
	DisagreeingSlabFileReason, QuarantinedSlabFile, QuarantinedSlabFilesReport,
};
#[cfg(test)]
mod pause_hooks;
use tiered_io::{
	RehydratedSlabState, SlabBacking, TierObjectEvidence, TieredPlaintext,
	build_tiered_object_metadata, decode_tiered_blob, descriptor_matches_tiered_row,
	extract_record_from_blob, fetch_tiered_ciphertext_runs, validate_tiered_blob,
	validate_tiered_record_header_tenant,
};
mod compaction;
use compaction::{
	CompactionOutcome, CompactionRecord, compact_physical_record_frame,
	physical_compaction_group_indices, physical_compaction_groups,
};
mod maintenance_publication;
use maintenance_publication::{
	maintenance_staged_path_of, maintenance_staged_slab_path,
	reconcile_staged_maintenance_publications,
};
mod index_publication;
use index_publication::{IndexPublication, IndexPublicationState, SingleAppendEvidence};
mod lsm_driver;
pub(crate) use lsm_driver::AcceptedControlLsmCompletion;
use lsm_driver::{
	cleanup_lsm_compaction_tmp_files, lsm_run_summary_from_index, refresh_lsm_level_metrics,
	remove_non_live_lsm_rows_from_index,
};
mod secondary_index_cleanup;
mod secondary_index_segments;
pub(crate) use encoding_compute::{
	EncodedAggregateL2RecordRead, EncodingMaterializeAuditSample, ProjectedL2RecordRead,
};
use encoding_compute::{
	encoding_compute_aggregate_kernels_enabled, encoding_compute_physical_user_column_ordinal,
	encoding_compute_row_group_column_null_count, encoding_directories_from_slab_blob,
	indexed_record_fetch_key, take_projected_indexed_rows,
};
pub use secondary_index_segments::SecondaryIndexSegmentInfo;
use secondary_index_segments::{
	SecondaryIndexCompactionContext, secondary_index_coverage_key_from_persistent,
};
use stats_facts::{TieredStatsCacheKey, tiered_stats_cache_key};

pub(crate) const STORAGE_BACKUP_FORMAT_VERSION: u16 = 2;

type TenantKey = (TenantId, Identity);
type PendingCounterFrame = (TenantId, Identity, u64, u32, u32, u32, SlabId, u64);

type TenantLocalityMembers = BTreeMap<TenantId, BTreeSet<(Locality, Identity)>>;

const DEFAULT_L2_TAIL_READ_WINDOW_BYTES: u64 = 64 * 1024;
const MIN_L2_TAIL_READ_WINDOW_BYTES: u64 = 256 + 16;
const IDENTITY_MAX: Identity = Identity::new([0xFF; 32]);
/// Maximum number of slab stores recovery will retain at once. Directory
/// metadata is scanned without descriptors first, so a large population of
/// empty historical slabs does not consume this budget.
const MAX_RECOVERY_OPEN_SLABS: usize = 4096;

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
	/// ADR 0692 §7: the node budget, in bytes, for tier history, publication
	/// scratch, proposer tokens and relocation redirects. A manager whose
	/// budget cannot hold its own fixed token registry refuses to open.
	pub tier_history_budget_bytes: u64,
}

/// The §7 default node budget for tier history: 256 MiB.
pub const DEFAULT_TIER_HISTORY_BUDGET_BYTES: u64 = 256 * 1024 * 1024;

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
	/// Immutable object generation for retirement. Advisory visibility
	/// summaries can change without reintroducing a retired object.
	pub(crate) fn same_object_generation(&self, other: &Self) -> bool {
		self.slab_id == other.slab_id
			&& crate::consensus::slab_ops::TierGeneration::from(self)
				== crate::consensus::slab_ops::TierGeneration::from(other)
	}

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
	/// The store these local slab ids and pointers belong to (ADR 0697).
	pub source: crate::consensus::OwnerRef,
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

pub use retention_reclaim::{TableDropReclaimReport, TableDropReclaimStatus};

/// Stable scan position for one ADR 0095 bounded reclaim phase. The upper
/// bound is captured on the first slice: after mutation drain closes, newer
/// slabs cannot contain target rows and must not make one debt chase an
/// unbounded moving tail.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableDropReclaimCursor {
	pub after_slab_id: Option<SlabId>,
	pub upper_bound_slab_id: Option<SlabId>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TableDropReclaimSliceReport {
	pub status: TableDropReclaimStatus,
	pub cursor_next: TableDropReclaimCursor,
	pub examined: usize,
	pub rewritten: usize,
	pub reclaimed_records: usize,
	pub reclaimed_bytes: u64,
	pub done: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TableDropReferenceCounts {
	pub in_memory_rows: usize,
	pub persistent_rows: usize,
	pub indexed_slabs: usize,
	pub secondary_indexed_slabs: usize,
}

impl TableDropReferenceCounts {
	pub fn is_empty(self) -> bool {
		self.in_memory_rows == 0
			&& self.persistent_rows == 0
			&& self.indexed_slabs == 0
			&& self.secondary_indexed_slabs == 0
	}
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
		old_generation: crate::consensus::slab_ops::TierGeneration,
		old_bytes: u64,
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
		old_generation: crate::consensus::slab_ops::TierGeneration,
		old_bytes: u64,
		pruned_versions: usize,
		retention_stats: RetentionCompactionStats,
		reclaimed_bytes: u64,
	},
	Observe {
		retention_stats: RetentionCompactionStats,
	},
}

pub(crate) enum PreparedTieredTableDropRewrite {
	Rewrite {
		slab_id: SlabId,
		old_l2_uri: String,
		old_generation: crate::consensus::slab_ops::TierGeneration,
		old_bytes: u64,
		encrypted_bytes: Vec<u8>,
		payload_hash: [u8; 32],
		tombstone_metadata: TombstoneMetadata,
		supersession_metadata: SupersessionMetadata,
		reclaimed_records: usize,
		reclaimed_bytes: u64,
	},
	Retire {
		slab_id: SlabId,
		old_l2_uri: String,
		old_generation: crate::consensus::slab_ops::TierGeneration,
		old_bytes: u64,
		reclaimed_records: usize,
		reclaimed_bytes: u64,
	},
}

/// Multi-slab composition of [`StorageEngine`].
pub struct SlabManager {
	_directory_owner: Option<DataDirectoryOwner>,
	finality_observation: Arc<crate::storage::finality_observation::FinalityObservationRegistry>,
	config: SlabManagerConfig,
	file_ops: Arc<dyn AsyncFileOps>,
	state: Mutex<ManagerState>,
	compaction_manifest: RwLock<CompactionManifest>,
	slab_pins: Arc<SlabPinTable>,
	/// Authoritative key → version chain, aggregated across all
	/// active and sealed slabs. Per ADR 013 the chain stores every
	/// version sorted ascending by HLC; the tail is the current
	/// value and earlier entries back time-travel reads.
	counter_eligible: Arc<super::counter::visibility::EligibleCells<(TenantId, Identity)>>,
	counter_unsupported: DashSet<(TenantId, Identity)>,
	/// V2 tentatives that have not reached a durable commit marker or rollback.
	/// This is volatile in-flight state; after restart unmarked tentatives are
	/// ordinary recovery orphans and can be copied safely.
	counter_pending_frames: DashSet<PendingCounterFrame>,
	counter_pending_guards: CounterPendingGuards,
	slab_publication_revisions: DashMap<SlabId, u64>,
	/// L0 sources whose class-preserving counter work made the current LSM plan
	/// ineligible. Excluding just these inputs lets ordinary L0 work proceed.
	counter_deferred_l0_slabs: DashSet<SlabId>,
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
	/// Durable typed-table attribution by published record location. This is
	/// separate from the key index because table identity cannot be recovered
	/// from an identity once its backing slab is missing.
	table_by_location: DashMap<RecordLocation, (TenantId, TableObjectId)>,
	/// Exact typed-table references by backing slab. Counts, rather than mere
	/// co-tenancy, define the affected table set for a missing slab.
	table_references_by_slab: DashMap<SlabId, BTreeMap<(TenantId, TableObjectId), usize>>,
	/// Exact typed-table secondary-index source references by backing slab.
	/// Keeping these distinct preserves the failure's actual reference kind.
	secondary_index_table_references_by_slab:
		DashMap<SlabId, BTreeMap<(TenantId, TableObjectId), usize>>,
	/// Fail-closed state derived from a published reference whose backing slab
	/// cannot be resolved. It is reconstructed from durable references on open;
	/// it is not a repair journal.
	integrity_failures: DashMap<(TenantId, TableObjectId), PublishedReferenceIntegrityFailure>,
	/// Compaction input slabs that have been proven to carry an unresolvable
	/// published reference. The picker skips only plans touching these slabs;
	/// transient compaction failures retain their retry behavior.
	quarantined_compaction_slabs: DashSet<SlabId>,
	/// Cross-slab redirects for in-flight readers that raced a maintenance
	/// rewrite (`redirect_store`). New lookups pivot to the fresh location
	/// through `key_index`; a stale holder of an old location follows a
	/// published redirect to the replacement slab.
	redirects: redirect_store::RedirectStores,
	/// Persistent restart index for the local slab set.
	///
	/// Ordinary record rows remain authoritative in slab bytes plus
	/// the in-memory `key_index`; if redb drifts, we rebuild it from
	/// those sources. Tiered-slab pointers come from the Control Ring
	/// state machine, and their redb transactions also stamp the
	/// highest applied control-log index we have mirrored so far.
	persistent_index: Option<Arc<PersistentLocalIndex>>,
	pending_metadata_tokens: DashMap<SlabId, crate::storage::index::PendingMetadataTokens>,
	completed_metadata_retries: DashMap<(SlabId, u64), crate::storage::index::MetadataKind>,
	persistent_index_gate: Arc<tokio::sync::RwLock<()>>,
	index_authority_unavailable: AtomicBool,
	/// ADR 0697: the control-ring node this store serves, bound once.
	control_node: std::sync::OnceLock<u32>,
	/// ADR 0697: set once when the ring shows this store is not (or no longer)
	/// its node's current incarnation; never cleared in this process.
	owner_authority_fence: std::sync::OnceLock<&'static str>,
	lsm_install_unresolved: DashMap<CompactionJobId, lsm_driver::UnresolvedLsmScope>,
	// Lock order: all required tier locks, then this scope gate, then state.
	// Never acquire a tier lock while holding either scope guard.
	lsm_intent_scope_gate: Arc<tokio::sync::RwLock<()>>,
	pending_index_publications: Arc<DashMap<(SlabId, u64), IndexPublicationState>>,
	single_append_publication_evidence: Arc<DashMap<(SlabId, u64), SingleAppendEvidence>>,
	index_publication_revision: std::sync::atomic::AtomicU64,
	orphaned_index_publications: Arc<std::sync::atomic::AtomicUsize>,
	/// ADR 0097 decision 4: the one admission truth every maintenance tick, the
	/// reservation guard and the operator accessor consult. Hydrated once at open
	/// from the persistent index and never replaced.
	maintenance_policy: Arc<MaintenancePolicyInner>,
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
	/// Serializes table admission against the one-time declaration cut. Table
	/// writes hold a read guard from catalog snapshot through local append;
	/// declaration holds the write guard while it seals the old active slab and
	/// installs the new active buffer.
	secondary_index_admission_gate: Arc<AsyncRwLock<()>>,
	/// Excludes LSM plan selection while a maintenance rewrite has durably
	/// published its replacement but has not yet installed and reflected that
	/// replacement in the live manager. Readers use the record index and slab
	/// pins, so this gate is planner-only.
	maintenance_publication_gate: Arc<AsyncRwLock<()>>,
	/// A D2 publication whose staged replacement could not be installed after
	/// the bounded S1 retry policy. Retaining the owned write guard keeps
	/// planners out of the committed-but-uninstalled world until startup
	/// reconciliation completes it.
	maintenance_publication_restart_required: AtomicBool,
	maintenance_publication_fail_stop: std::sync::Mutex<Option<OwnedRwLockWriteGuard<()>>>,
	/// Process-local acceleration for declaration cuts already established. A
	/// restart may repeat a cut before the durable completion receipt exists;
	/// that is safe and only creates another historical slab to backfill.
	secondary_index_declaration_cuts: DashSet<SecondaryIndexCoverageKey>,
	/// Serializes sidecar publication per path. An active-buffer seal and a
	/// lifecycle backfill can persist the same source slab's sidecar at once;
	/// unserialized, their temp files and catalog rows interleave.
	secondary_index_persist_stripes: Box<[tokio::sync::RwLock<()>]>,
	retained_secondary_sidecars: Arc<RetainedSecondarySidecars>,
	/// Per-slab rewrite claim shared by every reader that must retain a physical
	/// source and every operation that can replace or retire it. Secondary-index
	/// backfill holds a shared claim; compaction, retention, and ADR 0095 table
	/// reclaim hold the exclusive claim from their first physical read through
	/// publication/retirement.
	slab_rewrite_claims: DashMap<SlabId, Arc<AsyncRwLock<()>>>,
	/// Parsed ADR 039 CSBs keyed by slab, descriptor, and footer
	/// locator identity. This avoids repeated L2 range reads and
	/// AEAD decrypts for WFRM query bursts over the same cold slab.
	tiered_stats_cache: DashMap<TieredStatsCacheKey, ParsedColumnStatsBlock>,
	/// Per-slab tier publication locks (tier publication and retirement only).
	tier_publication_locks: DashMap<SlabId, Arc<tokio::sync::Mutex<()>>>,
	/// Serializes legacy cleanup with new unresolved records for a slab.
	cleanup_claims: DashMap<SlabId, Arc<tokio::sync::Mutex<()>>>,
	/// Every copy of a version whose identity is claimed by different payloads;
	/// rebuilt at each open. Reads of these refuse.
	conflicted_locations: DashSet<RecordLocation>,
	/// Tiered slabs not yet verified against their objects since open.
	unverified_tier_slabs: DashSet<SlabId>,
	tier_authority: Arc<tier_authority::TierAuthority>,
	installed_snapshot_index: std::sync::RwLock<Option<u64>>,
	snapshot_epoch: Arc<std::sync::atomic::AtomicU64>,
	snapshot_epoch_gate: Arc<std::sync::RwLock<()>>,
	awaiting_cluster: AtomicBool,
	/// Tiered slabs retired in this process; a later publication for one is stale.
	retired_tier_slabs: DashSet<SlabId>,
	/// The ADR 0692 §7 node budget for tier history and redirect memory.
	tier_budget: Arc<tier_budget::TierMemoryBudget>,
	/// Proposer history reservations awaiting their outcome (§7 tokens).
	tier_tokens: Arc<tier_tokens::TierTokenRegistry>,
	/// Committed tier pointers this node failed to publish, with the outcome
	/// of the apply that could not (`tier_deferral`).
	unapplied_tier_pointers: DashMap<SlabId, tier_deferral::TierDeferral>,
	/// The committed tier state this manager observes, once bound
	/// (`TierObserver::bind_tier_snapshot_source`); the replay revalidates
	/// against it.
	canonical_tier_source: std::sync::OnceLock<crate::consensus::TierSnapshotSource>,
	#[cfg(test)]
	replay_probe: tier_deferral::ReplayProbe,
	#[cfg(test)]
	counter_rollback_fail_once: std::sync::atomic::AtomicBool,
	#[cfg(test)]
	lsm_control_install_hold: lsm_driver::ControlLsmInstallHold,
	#[cfg(test)]
	lsm_after_rename_hold:
		std::sync::Mutex<Option<(Arc<tokio::sync::Semaphore>, Arc<tokio::sync::Semaphore>)>>,
	#[cfg(test)]
	lsm_fail_before_rename: std::sync::atomic::AtomicBool,
	#[cfg(test)]
	lsm_after_output_publish_hold:
		std::sync::Mutex<Option<(Arc<tokio::sync::Semaphore>, Arc<tokio::sync::Semaphore>)>>,
	#[cfg(test)]
	lsm_after_install_hold:
		std::sync::Mutex<Option<(Arc<tokio::sync::Semaphore>, Arc<tokio::sync::Semaphore>)>>,
	#[cfg(test)]
	lsm_after_intent_clear_hold:
		std::sync::Mutex<Option<(Arc<tokio::sync::Semaphore>, Arc<tokio::sync::Semaphore>)>>,
	#[cfg(test)]
	lsm_capture_hold:
		std::sync::Mutex<Option<(Arc<tokio::sync::Semaphore>, Arc<tokio::sync::Semaphore>)>>,
	#[cfg(test)]
	counter_rollback_hold:
		std::sync::Mutex<Option<(Arc<tokio::sync::Semaphore>, Arc<tokio::sync::Semaphore>)>>,
	#[cfg(test)]
	continuity_test_holds: pause_hooks::TestHolds,
	/// Durable cleanup intents that cannot be resolved from a stale local pointer.
	unresolved_tier_cleanup: retention_reclaim::UnresolvedTierCleanup,
	deferred_table_drop_l2: retention_reclaim::DeferredTableDropL2,
	cleanup_scan_schedule: retention_reclaim::CleanupScanSchedule,
	/// Local slab files open found disagreeing with the committed tier state;
	/// fixed at open, never served or unlinked.
	quarantined_slab_files: slab_file_quarantine::SlabFileQuarantine,
	/// Slab ids whose local file an operation of this process removed from
	/// `state` and has not yet unlinked: the only slab files it may unlink.
	released_slab_files: DashSet<SlabId>,
	live_reservations: Arc<DashSet<SlabId>>,
	/// Test-only pause points (manager/pause_hooks.rs).
	#[cfg(test)]
	test_pauses: pause_hooks::TestPauses,
}

/// Which durable reference was found to point at a missing backing slab.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PublishedReferenceKind {
	RecordLocation,
	SecondaryIndexSourceSlab,
}

/// The seam that first observed the broken published reference.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PublishedReferenceOrigin {
	Read,
	Recovery,
	Compaction,
}

/// Typed integrity state retained for every affected typed table.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PublishedReferenceIntegrityFailure {
	pub tenant_id: TenantId,
	pub table_object_id: TableObjectId,
	pub reference_kind: PublishedReferenceKind,
	pub missing_slab_id: SlabId,
	pub origin: PublishedReferenceOrigin,
}

impl PublishedReferenceIntegrityFailure {
	pub fn wire_meta(self) -> TableReferenceIntegrityFailureMeta {
		TableReferenceIntegrityFailureMeta {
			tenant_id: self.tenant_id,
			table_object_id: self.table_object_id.into(),
			missing_slab_id: self.missing_slab_id.0,
			reference_kind: match self.reference_kind {
				PublishedReferenceKind::RecordLocation => 1,
				PublishedReferenceKind::SecondaryIndexSourceSlab => 2,
			},
			origin: match self.origin {
				PublishedReferenceOrigin::Read => 1,
				PublishedReferenceOrigin::Recovery => 2,
				PublishedReferenceOrigin::Compaction => 3,
			},
			_pad: [0; 6],
		}
	}
}

const fn zero_hlc() -> HybridTimestamp {
	HybridTimestamp {
		physical_ms: 0,
		logical: 0,
		node_id: 0,
	}
}

fn table_integrity_error(failure: PublishedReferenceIntegrityFailure) -> std::io::Error {
	Error::new(
		ErrorKind::InvalidData,
		format!(
			"published reference integrity failure: tenant={} table={:?} reference={:?} missing_slab={} origin={:?}",
			failure.tenant_id.get(),
			failure.table_object_id,
			failure.reference_kind,
			failure.missing_slab_id,
			failure.origin,
		),
	)
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
	/// The committed slab id high-water: no id at or above it has been
	/// issued. `next_slab_id` never passes it.
	durable_slab_id_bound: SlabId,
	/// In memory only, never durable coverage: see `take_slab_range_before_its_commit`.
	reservation_in_flight_bound: SlabId,
	/// Each tiered slab's published generation history; absent means
	/// generation 0 with no history.
	tier_layouts: DashMap<SlabId, Arc<TierLayout>>,
	/// Slabs whose backing an authorized disposal removed
	/// (`disposal_evidence`).
	disposal_evidence: disposal_evidence::DisposalEvidence,
}

impl ManagerState {
	/// The only writer of `next_slab_id` after open. A released slab id is
	/// below it and is never allocated again, so the id-keyed release in
	/// `slab_file_quarantine.rs` can never unlink a newer file. Ids must
	/// already be covered by the durable bound (`SlabManager::cover_slab_ids_through`).
	fn advance_slab_allocator(&mut self, next: SlabId) -> std::io::Result<()> {
		debug_assert!(
			next >= self.next_slab_id,
			"the slab allocator only advances"
		);
		if next
			> self
				.durable_slab_id_bound
				.max(self.reservation_in_flight_bound)
		{
			return Err(Error::other(format!(
				"slab id {} is not covered by the durable high-water {}",
				next.get(),
				self.durable_slab_id_bound.get()
			)));
		}
		self.next_slab_id = self.next_slab_id.max(next);
		Ok(())
	}
}

impl SlabManager {
	async fn append_on_target(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
		hlc: HybridTimestamp,
		frame: &[u8],
	) -> std::io::Result<(
		Arc<LocalSlabStore>,
		RecordLocation,
		Option<Arc<AppendInFlight>>,
	)> {
		let (slab, reservation) = self.target_for_append(frame.len(), None).await?;
		let guard = reservation.quiescence_guard();
		let location = slab
			.append_reserved(reservation, tenant_id, key_identity, hlc, frame)
			.await?;
		Ok((slab, location, guard))
	}

	/// Caller must own the matching tier publication lock.
	fn bump_slab_publication_revision(&self, slab_id: SlabId) -> std::io::Result<()> {
		let mut revision = self.slab_publication_revisions.entry(slab_id).or_insert(0);
		*revision = revision
			.checked_add(1)
			.ok_or_else(|| Error::other("slab publication revision exhausted"))?;
		Ok(())
	}

	fn rollback_indexed_location(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
		location: RecordLocation,
	) {
		let key = (tenant_id, key_identity);
		let (removed, now_empty) = if let Some(mut versions) = self.key_index.get_mut(&key) {
			let removed = remove_version_exact(&mut versions, location);
			(removed, removed && versions.is_empty())
		} else {
			(false, false)
		};
		if removed {
			self.remove_published_table_reference(location);
		}
		if now_empty {
			#[cfg(test)]
			self.empty_key_removal_pause_point();
			self.remove_key_if_empty(tenant_id, key_identity);
		}
	}

	fn routing_locality_for_key(&self, tenant_id: TenantId, key_identity: Identity) -> Locality {
		self.key_localities
			.get(&(tenant_id, key_identity))
			.map(|entry| *entry.value())
			.unwrap_or_else(|| Locality::from(key_identity))
	}

	// ============================================================
	//  ADR 075 counter frames (counter-shaped, fenced)
	//
	//  These are deliberately NOT a general "append any record_class
	//  without indexing" API — each method fixes its own counter
	//  `record_class` internally and applies counter-specific index
	//  semantics, so ordinary append/index/LWW behaviour is untouched
	//  and no second generic append path exists.
	// ============================================================

	/// Append a tentative counter value to the active slab, durably commit that
	/// exact slab write, then index it live. The record class is fixed
	/// `COUNTER_TENTATIVE_V2`; recovery indexes it only if a matching commit marker
	/// is found, so an un-committed tentative does not survive restart.
	pub async fn counter_append_tentative(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
		hlc: HybridTimestamp,
		value: u64,
		origin: NodeId,
	) -> std::io::Result<RecordLocation> {
		self.require_tier_authority_ready()?;
		let frame = super::counter::encode_origin_frame(value, origin)?;
		let checked_same_hlc = self
			.refuse_conflicting_version(
				tenant_id,
				key_identity,
				hlc,
				IncomingVersion::Counter { value, origin },
			)
			.await?;
		let (slab, reservation) = self
			.target_for_append(frame.len(), Some((tenant_id, key_identity, hlc, origin)))
			.await?;
		let _append_guard = reservation.quiescence_guard();

		let location = match slab
			.write_reserved(
				reservation,
				tenant_id,
				key_identity,
				hlc,
				crate::schema::record_class::COUNTER_TENTATIVE_V2,
				&frame,
			)
			.await
		{
			Ok(location) => location,
			Err(error) => {
				// The write may have left a frame or detached work. Keep its hold.
				return Err(error);
			}
		};
		// The prepare acknowledgement must not outrun durability of this exact
		// tentative slab. A failed barrier leaves no live index entry or pending
		// replica state; the written frame remains an uncommitted recovery orphan.
		if let Err(error) = slab.wait_durable_commit().await {
			// A durable frame may remain; abandonment is not proven.
			return Err(error);
		}
		if let Err(error) = self.index_checked_version(
			tenant_id,
			key_identity,
			Locality::from(key_identity),
			location,
			checked_same_hlc,
		) {
			// A durable frame may remain; abandonment is not proven.
			return Err(error);
		}
		Ok(location)
	}

	/// Append a durable commit marker for `tentative_op_id` (the tentative's slab
	/// offset) to the active slab and flush. The record class is fixed
	/// `COUNTER_COMMIT_V2`; the marker is NOT a version, so it is not indexed. The
	/// tentative and its marker must be co-located in one slab — true within a
	/// single add (no rotation between the two writes); cross-slab pairing under
	/// rotation is a follow-on for the compaction path.
	pub async fn counter_append_commit_marker(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
		hlc: HybridTimestamp,
		tentative_op_id: u64,
		origin: NodeId,
	) -> std::io::Result<()> {
		self.require_tier_authority_ready()?;
		let marker = super::counter::encode_origin_frame(tentative_op_id, origin)?;
		let (slab, reservation) = self.target_for_append(marker.len(), None).await?;
		let _append_guard = reservation.quiescence_guard();
		slab.write_reserved(
			reservation,
			tenant_id,
			key_identity,
			hlc,
			crate::schema::record_class::COUNTER_COMMIT_V2,
			&marker,
		)
		.await?;
		slab.wait_durable_commit().await?;
		Ok(())
	}

	/// Remove a tentative counter version from the live `key_index` (rollback).
	/// No durable abort marker — recovery skips the orphan (it has no commit
	/// marker). Counter-shaped wrapper over the in-memory version removal.
	pub async fn counter_unindex_tentative(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
		location: RecordLocation,
	) -> std::io::Result<()> {
		let tier_lock = self.tier_publication_lock(location.slab_id);
		let _guard = tier_lock.lock().await;
		self.rollback_indexed_location(tenant_id, key_identity, location);
		#[cfg(test)]
		if self
			.counter_rollback_fail_once
			.swap(false, Ordering::AcqRel)
		{
			return Err(std::io::Error::other("injected counter rollback failure"));
		}
		self.bump_slab_publication_revision(location.slab_id)?;
		#[cfg(test)]
		{
			let hold = self.counter_rollback_hold.lock().unwrap().clone();
			if let Some((entered, release)) = hold {
				entered.add_permits(1);
				let _ = release.acquire().await;
			}
		}
		self.counter_pending_frames.retain(|pending| {
			!(pending.0 == tenant_id
				&& pending.1 == key_identity
				&& pending.6 == location.slab_id
				&& pending.7 == location.offset)
		});
		self.counter_pending_guards.retain(|pending, _| {
			!(pending.0 == tenant_id
				&& pending.1 == key_identity
				&& pending.6 == location.slab_id
				&& pending.7 == location.offset)
		});
		Ok(())
	}

	/// Mark a V2 tentative no longer pending only after the corresponding marker
	/// is durable and its live eligibility reservation has been published.
	pub(crate) fn counter_commit_published(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
		location: RecordLocation,
		origin: NodeId,
	) {
		let pending = Self::pending_counter_frame(
			tenant_id,
			key_identity,
			location.hlc,
			origin,
			location.slab_id,
			location.offset,
		);
		self.counter_pending_frames.remove(&pending);
		self.counter_pending_guards.remove(&pending);
	}

	pub(crate) fn counter_frame_is_pending(&self, frame: &core::ScannedCounterFrame) -> bool {
		if frame.record_class != crate::schema::record_class::COUNTER_TENTATIVE_V2 {
			return false;
		}
		let Ok((_, origin)) = super::counter::decode_origin_frame(&frame.payload) else {
			return false;
		};
		self.counter_pending_frames.contains(&(
			frame.tenant_id,
			frame.key_identity,
			frame.location.hlc.physical_ms,
			frame.location.hlc.logical,
			frame.location.hlc.node_id,
			origin.id,
			frame.location.slab_id,
			frame.location.offset,
		))
	}

	pub(crate) fn counter_slab_has_pending_frames(&self, slab_id: SlabId) -> bool {
		self.counter_pending_frames
			.iter()
			.any(|pending| pending.6 == slab_id)
	}

	pub(crate) fn counter_pending_frame_count(&self) -> usize {
		self.counter_pending_frames.len()
	}

	/// Strict lookup for admission paths that must inherit an
	/// existing append-only row's stored routing locality.
	pub(crate) fn routing_locality_for_existing_key(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
	) -> std::io::Result<Option<Locality>> {
		self.require_tier_authority_ready()?;
		Ok(self
			.key_localities
			.get(&(tenant_id, key_identity))
			.map(|entry| *entry.value()))
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
			table_object_id: self.published_table_object_id_for_recovery(location),
			// ADR 0090: rebuilt rows keep the location's own placement.
			placement: location.placement,
			partition: location.partition,
			location,
		}
	}

	fn insert_locality_membership_with_locality(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
		routing_locality: Locality,
	) {
		// `key_localities` changes only under the locality write lock, so a
		// concurrent empty-key removal cannot interleave with this publication.
		let mut locality_index = recover_rwlock_write(&self.locality_index, "locality_index");
		let previous = self
			.key_localities
			.insert((tenant_id, key_identity), routing_locality);
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
		self.require_append_admission()?;
		self.observe_publication(tenant_id, async {
			let metadata_epoch = self.capture_snapshot_epoch();
			let superseded = self.latest_location_before(tenant_id, key_identity, hlc);
			let checked_same_hlc = self
				.refuse_conflicting_version(
					tenant_id,
					key_identity,
					hlc,
					IncomingVersion::Record(frame),
				)
				.await?;
			let metadata_token = match superseded {
				Some(previous) => {
					self.begin_pending_metadata_operation(
						previous.slab_id,
						crate::storage::index::MetadataKind::Supersession,
					)
					.await?
				}
				None => None,
			};
			let (slab, location, _append_guard) = self
				.append_on_target(tenant_id, key_identity, hlc, frame)
				.await?;
			if let Err(err) = slab.wait_durable_commit().await {
				slab.rollback_index_entry(tenant_id, key_identity, location);
				self.rollback_indexed_location(tenant_id, key_identity, location);
				return Err(err);
			}
			let row = PersistentRecordRow {
				tenant_id,
				key_identity,
				routing_locality,
				table_object_id: None,
				placement: location.placement,
				partition: location.partition,
				location,
			};
			let publication = self.begin_single_append_publication(row, frame).await?;
			if let Err(err) = self
				.persist_record_row_strict(
					tenant_id,
					key_identity,
					routing_locality,
					location,
					&publication,
				)
				.await
			{
				slab.rollback_index_entry(tenant_id, key_identity, location);
				self.rollback_indexed_location(tenant_id, key_identity, location);
				return Err(err);
			}
			if let Err(err) = self.index_checked_version(
				tenant_id,
				key_identity,
				routing_locality,
				location,
				checked_same_hlc,
			) {
				slab.rollback_index_entry(tenant_id, key_identity, location);
				return Err(err);
			}
			publication.complete();
			#[cfg(test)]
			self.append_metadata_observed_pause_point().await;
			if let Some(previous) = superseded {
				// A durable row must not be reported failed if metadata publication fails.
				let _ = self
					.record_supersession_for_slab_at_epoch(
						previous.slab_id,
						hlc,
						None,
						metadata_epoch,
						metadata_token,
					)
					.await;
			}
			Ok(location)
		})
		.await
	}

	pub(crate) async fn append_durable_multirow_with_locality(
		&self,
		tenant_id: TenantId,
		frame: &[u8],
		entries: &[MultiRowIndexEntry],
	) -> std::io::Result<RecordLocation> {
		self.require_append_admission()?;
		self.observe_publication(tenant_id, async {
			let metadata_epoch = self.capture_snapshot_epoch();
			let Some(primary) = entries.first().copied() else {
				return Err(Error::new(
					ErrorKind::InvalidInput,
					"multi-row append requires at least one index entry",
				));
			};
			let batch = decode_record_batch(frame)?;
			let table_object_ids = entries
				.iter()
				.map(|entry| storage_row_table_object_id_at(&batch, entry.record_row_index))
				.collect::<std::io::Result<BTreeSet<_>>>()?;
			for table_object_id in table_object_ids.into_iter().flatten() {
				if let Some(failure) = self.table_integrity_failure(tenant_id, table_object_id) {
					return Err(table_integrity_error(failure));
				}
			}
			let record_hlc = entries
				.iter()
				.map(|entry| entry.row_hlc)
				.max()
				.unwrap_or(primary.row_hlc);
			let mut checked_same_hlc = Vec::with_capacity(entries.len());
			for entry in entries {
				checked_same_hlc.push(
					self.refuse_conflicting_version(
						tenant_id,
						entry.key_identity,
						entry.row_hlc,
						IncomingVersion::Row {
							batch: &batch,
							row: entry.record_row_index.get() as usize,
						},
					)
					.await?,
				);
			}
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
			let mut supersession_by_slab = BTreeMap::new();
			for (slab, event) in superseded {
				supersession_by_slab
					.entry(slab)
					.and_modify(|existing: &mut HybridTimestamp| *existing = (*existing).min(event))
					.or_insert(event);
			}
			let mut tombstone_by_slab = BTreeMap::new();
			for (slab, event) in tombstoned {
				tombstone_by_slab
					.entry(slab)
					.and_modify(|existing: &mut HybridTimestamp| *existing = (*existing).min(event))
					.or_insert(event);
			}
			let mut supersession_tokens = HashMap::new();
			for slab in supersession_by_slab.keys() {
				if let Some(token) = self
					.begin_pending_metadata_operation(
						*slab,
						crate::storage::index::MetadataKind::Supersession,
					)
					.await?
				{
					supersession_tokens.insert(*slab, token);
				}
			}
			let mut tombstone_tokens = HashMap::new();
			for slab in tombstone_by_slab.keys() {
				if let Some(token) = self
					.begin_pending_metadata_operation(
						*slab,
						crate::storage::index::MetadataKind::Tombstone,
					)
					.await?
				{
					tombstone_tokens.insert(*slab, token);
				}
			}
			let (slab, base_location, _append_guard) = self
				.append_on_target(tenant_id, primary.key_identity, record_hlc, frame)
				.await?;
			if let Err(err) = slab.wait_durable_commit().await {
				slab.rollback_index_entry(tenant_id, primary.key_identity, base_location);
				self.rollback_indexed_location(tenant_id, primary.key_identity, base_location);
				return Err(err);
			}

			let rows = entries
				.iter()
				.map(|entry| {
					// ADR 0090: the row's durable placement travels from the frame's
					// per-row entry into both the persistent index (V5) and the
					// in-memory location that route selection consults.
					let placement = if entry.partition.is_some() {
						PlacementSemantics::Partitioned
					} else {
						PlacementSemantics::RawLocality
					};
					Ok(PersistentRecordRow {
						tenant_id,
						key_identity: entry.key_identity,
						routing_locality: entry.routing_locality,
						table_object_id: storage_row_table_object_id_at(
							&batch,
							entry.record_row_index,
						)?,
						placement,
						partition: entry.partition,
						location: RecordLocation {
							hlc: entry.row_hlc,
							record_row_index: entry.record_row_index,
							is_tombstone: entry.is_tombstone,
							placement,
							partition: entry.partition,
							..base_location
						},
					})
				})
				.collect::<std::io::Result<Vec<_>>>()?;
			let publication = self
				.begin_index_publication(vec![base_location.slab_id])
				.await?;
			if let Err(err) = self
				.persist_record_rows_strict(rows.clone(), &publication)
				.await
			{
				slab.rollback_index_entry(tenant_id, primary.key_identity, base_location);
				for row in &rows {
					self.rollback_indexed_location(row.tenant_id, row.key_identity, row.location);
				}
				return Err(err);
			}

			slab.rollback_index_entry(tenant_id, primary.key_identity, base_location);
			for (row, checked_same_hlc) in rows.iter().zip(checked_same_hlc) {
				// The reference is counted before the location is indexed, so an
				// indexed attributed row is always counted.
				if let Some(table_object_id) = row.table_object_id {
					#[cfg(test)]
					self.append_reference_publication_pause_point();
					self.add_published_table_reference(tenant_id, table_object_id, row.location);
				}
				slab.index_location(row.tenant_id, row.key_identity, row.location);
				if let Err(err) = self.index_checked_version(
					row.tenant_id,
					row.key_identity,
					row.routing_locality,
					row.location,
					checked_same_hlc,
				) {
					slab.rollback_index_entry(row.tenant_id, row.key_identity, row.location);
					self.remove_published_table_reference(row.location);
					return Err(err);
				}
			}
			publication.complete();
			#[cfg(test)]
			self.append_metadata_observed_pause_point().await;
			for (slab_id, supersession_hlc) in supersession_by_slab {
				let _ = self
					.record_supersession_for_slab_at_epoch(
						slab_id,
						supersession_hlc,
						None,
						metadata_epoch,
						supersession_tokens.remove(&slab_id),
					)
					.await;
			}
			for (slab_id, tombstone_hlc) in tombstone_by_slab {
				let _ = self
					.record_row_tombstone_for_slab_at_epoch(
						slab_id,
						tombstone_hlc,
						None,
						metadata_epoch,
						tombstone_tokens.remove(&slab_id),
					)
					.await;
			}
			Ok(base_location)
		})
		.await
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
	/// * **One or more existing slabs**: treats the highest-numbered slab as
	///   active, skips zero-length sealed files, opens the non-empty sealed files
	///   (recovering each slab's on-disk records into its local index), and sets
	///   `next_slab_id` to `max_id + 1`.
	///
	/// All recovered records are folded into the manager's
	/// aggregated `key_index`. Last-write-wins across slabs, ordered
	/// by slab id — so a rewrite that landed in a newer slab
	/// shadows the stale copy in an older one.
	#[cfg(any(test, feature = "test-support"))]
	pub async fn open(config: SlabManagerConfig) -> std::io::Result<Arc<Self>> {
		Self::open_shared(
			config,
			None,
			crate::metrics::global(),
			open::StoreOpen::Existing,
			#[cfg(any(test, feature = "test-support"))]
			None,
		)
		.await
	}

	/// Open with the maintenance policy reporting to `maintenance_metrics`, so a
	/// test can assert exact attempt counts and gauges without racing every
	/// other concurrently running test on the process registry.
	#[cfg(test)]
	pub(crate) async fn open_with_maintenance_metrics(
		config: SlabManagerConfig,
		maintenance_metrics: &'static crate::metrics::MetricsRegistry,
	) -> std::io::Result<Arc<Self>> {
		Self::open_shared(
			config,
			None,
			maintenance_metrics,
			open::StoreOpen::Existing,
			#[cfg(any(test, feature = "test-support"))]
			None,
		)
		.await
	}

	/// Open a node-owned store while retaining the directory lease in the
	/// manager and its I/O handles.
	pub async fn open_with_owner(
		config: SlabManagerConfig,
		owner: DataDirectoryOwner,
	) -> std::io::Result<Arc<Self>> {
		Self::open_shared(
			config,
			Some(owner),
			crate::metrics::global(),
			open::StoreOpen::Existing,
			#[cfg(any(test, feature = "test-support"))]
			None,
		)
		.await
	}

	#[cfg(any(test, feature = "test-support"))]
	pub(crate) async fn open_with_file_ops_and_owner(
		config: SlabManagerConfig,
		file_ops: Arc<dyn AsyncFileOps>,
		owner: DataDirectoryOwner,
	) -> std::io::Result<Arc<Self>> {
		Self::open_shared(
			config,
			Some(owner),
			crate::metrics::global(),
			open::StoreOpen::Existing,
			Some(file_ops),
		)
		.await
	}

	/// Open with caller-supplied file operations for deterministic acceptance fixtures.
	#[cfg(any(test, feature = "test-support"))]
	pub async fn open_with_file_ops(
		config: SlabManagerConfig,
		file_ops: Arc<dyn AsyncFileOps>,
	) -> std::io::Result<Arc<Self>> {
		Self::open_shared(
			config,
			None,
			crate::metrics::global(),
			open::StoreOpen::Existing,
			Some(file_ops),
		)
		.await
	}

	/// Test / diagnostic accessor: current active slab id.
	pub async fn active_slab_id(&self) -> SlabId {
		self.state.lock().await.active.slab_id()
	}

	pub fn data_dir(&self) -> &Path {
		&self.config.dir
	}

	pub(crate) fn directory_owner(&self) -> Option<DataDirectoryOwner> {
		self._directory_owner.clone()
	}

	/// Durable typed-table attribution for a published record location.
	///
	/// This deliberately returns `None` for legacy/untyped rows. Callers that
	/// need an integrity fence must treat missing attribution as unknown, never
	/// infer a table from the irreversible row identity.
	pub(crate) fn published_table_object_id(
		&self,
		location: RecordLocation,
	) -> std::io::Result<Option<TableObjectId>> {
		self.require_tier_authority_ready()?;
		Ok(self.published_table_object_id_for_recovery(location))
	}
	fn published_table_object_id_for_recovery(
		&self,
		location: RecordLocation,
	) -> Option<TableObjectId> {
		self.table_by_location
			.get(&location)
			.map(|entry| entry.value().1)
	}

	/// Keyed ADR 0095 settlement check. Physical L1/L2 surfaces are traversed by
	/// the bounded phase cursors; this check independently proves that neither
	/// in-memory nor persistent lookup authority can still reach the object.
	pub fn table_drop_reference_counts(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
	) -> std::io::Result<TableDropReferenceCounts> {
		self.require_tier_authority_ready()?;
		let in_memory_rows = self
			.table_by_location
			.iter()
			.filter(|entry| *entry.value() == (tenant_id, table_object_id))
			.count();
		let persistent_rows = self
			.persistent_record_rows_snapshot()
			.into_iter()
			.filter(|row| {
				row.tenant_id == tenant_id && row.table_object_id == Some(table_object_id)
			})
			.count();
		let indexed_slabs = self
			.table_references_by_slab
			.iter()
			.filter(|entry| entry.value().contains_key(&(tenant_id, table_object_id)))
			.count();
		let secondary_indexed_slabs = self
			.secondary_index_table_references_by_slab
			.iter()
			.filter(|entry| entry.value().contains_key(&(tenant_id, table_object_id)))
			.count();
		Ok(TableDropReferenceCounts {
			in_memory_rows,
			persistent_rows,
			indexed_slabs,
			secondary_indexed_slabs,
		})
	}

	/// Attribute `location` to a typed table and count it on its slab.
	///
	/// The attribution and its count change together under the location's
	/// `table_by_location` entry guard; the count changes under one
	/// `table_references_by_slab` entry guard, so a reassignment replaces the
	/// old table's contribution with the new one atomically and a counts reader
	/// always sees the location attributed to one of them.
	fn add_published_table_reference(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		location: RecordLocation,
	) {
		let target = (tenant_id, table_object_id);
		match self.table_by_location.entry(location) {
			dashmap::mapref::entry::Entry::Occupied(mut occupied) => {
				let previous = *occupied.get();
				if previous == target {
					return;
				}
				{
					let mut references = self
						.table_references_by_slab
						.entry(location.slab_id)
						.or_default();
					decrement_reference_count(&mut references, previous);
					#[cfg(test)]
					self.reference_reassignment_pause_point();
					*references.entry(target).or_default() += 1;
				}
				occupied.insert(target);
			}
			dashmap::mapref::entry::Entry::Vacant(vacant) => {
				*self
					.table_references_by_slab
					.entry(location.slab_id)
					.or_default()
					.entry(target)
					.or_default() += 1;
				vacant.insert(target);
			}
		}
	}

	/// Remove `location`'s attribution and its count. The emptiness check and
	/// the slab entry's removal happen under one entry guard, so a concurrent
	/// add for another location on the slab is never erased.
	fn remove_published_table_reference(&self, location: RecordLocation) {
		if let dashmap::mapref::entry::Entry::Occupied(occupied) =
			self.table_by_location.entry(location)
		{
			let attribution = *occupied.get();
			if let dashmap::mapref::entry::Entry::Occupied(mut references) =
				self.table_references_by_slab.entry(location.slab_id)
			{
				decrement_reference_count(references.get_mut(), attribution);
				if references.get().is_empty() {
					#[cfg(test)]
					self.empty_reference_slab_removal_pause_point();
					references.remove();
				}
			}
			occupied.remove();
		}
	}

	pub(crate) fn add_secondary_index_source_reference(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		source_slab_id: SlabId,
	) {
		*self
			.secondary_index_table_references_by_slab
			.entry(source_slab_id)
			.or_default()
			.entry((tenant_id, table_object_id))
			.or_default() += 1;
	}

	pub(crate) fn remove_secondary_index_source_reference(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		source_slab_id: SlabId,
	) {
		if let dashmap::mapref::entry::Entry::Occupied(mut references) = self
			.secondary_index_table_references_by_slab
			.entry(source_slab_id)
		{
			decrement_reference_count(references.get_mut(), (tenant_id, table_object_id));
			if references.get().is_empty() {
				#[cfg(test)]
				self.empty_secondary_reference_slab_removal_pause_point();
				references.remove();
			}
		}
	}

	fn tables_referencing_slab_by_kind(
		&self,
		slab_id: SlabId,
		reference_kind: PublishedReferenceKind,
	) -> BTreeSet<(TenantId, TableObjectId)> {
		let references = match reference_kind {
			PublishedReferenceKind::RecordLocation => &self.table_references_by_slab,
			PublishedReferenceKind::SecondaryIndexSourceSlab => {
				&self.secondary_index_table_references_by_slab
			}
		};
		references
			.get(&slab_id)
			.map(|entries| entries.keys().copied().collect())
			.unwrap_or_default()
	}

	/// Returns the persistent fail-closed state for a typed table, if a
	/// published reference has already been found to lack a backing slab.
	pub(crate) fn table_integrity_failure(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
	) -> Option<PublishedReferenceIntegrityFailure> {
		self.integrity_failures
			.get(&(tenant_id, table_object_id))
			.map(|entry| *entry)
	}

	fn record_missing_published_reference(
		&self,
		missing_slab_id: SlabId,
		reference_kind: PublishedReferenceKind,
		origin: PublishedReferenceOrigin,
	) {
		let affected_tables = self.tables_referencing_slab_by_kind(missing_slab_id, reference_kind);
		if !affected_tables.is_empty() {
			self.quarantined_compaction_slabs.insert(missing_slab_id);
		}
		for (tenant_id, table_object_id) in affected_tables {
			self.integrity_failures
				.entry((tenant_id, table_object_id))
				.or_insert(PublishedReferenceIntegrityFailure {
					tenant_id,
					table_object_id,
					reference_kind,
					missing_slab_id,
					origin,
				});
		}
	}

	fn record_missing_published_references_for_slab(
		&self,
		missing_slab_id: SlabId,
		origin: PublishedReferenceOrigin,
	) {
		for reference_kind in [
			PublishedReferenceKind::RecordLocation,
			PublishedReferenceKind::SecondaryIndexSourceSlab,
		] {
			self.record_missing_published_reference(missing_slab_id, reference_kind, origin);
		}
	}

	/// ADR 0097 decision 4: the policy, for the operator accessor and the requeue.
	pub(crate) fn maintenance_policy(&self) -> &Arc<MaintenancePolicyInner> {
		&self.maintenance_policy
	}

	pub(crate) fn is_compaction_slab_quarantined(&self, slab_id: SlabId) -> bool {
		self.quarantined_compaction_slabs.contains(&slab_id)
	}

	/// Reconciliation failed for a retiring slab: quarantine its compaction work with
	/// one diagnostic, and record the integrity failure against every table whose
	/// published references name it. The slab file itself is preserved — containment,
	/// not repair.
	pub(super) fn quarantine_compaction_slab_for_reconciliation(
		&self,
		slab_id: SlabId,
		error: &std::io::Error,
	) {
		let newly_quarantined = self.quarantined_compaction_slabs.insert(slab_id);
		if newly_quarantined {
			tracing::error!(
				slab_id = slab_id.get(),
				error = %error,
				"quarantining slab retirement: secondary index reconciliation failed; \
				 slab file preserved"
			);
			self.record_missing_published_reference(
				slab_id,
				PublishedReferenceKind::SecondaryIndexSourceSlab,
				PublishedReferenceOrigin::Compaction,
			);
		}
	}

	async fn detect_missing_published_references_on_recovery(&self) {
		let referenced_slabs = self
			.table_references_by_slab
			.iter()
			.map(|entry| *entry.key())
			.chain(
				self.secondary_index_table_references_by_slab
					.iter()
					.map(|entry| *entry.key()),
			)
			.collect::<BTreeSet<_>>();
		for slab_id in referenced_slabs {
			let present = {
				let state = self.state.lock().await;
				state.active.slab_id() == slab_id
					|| state.sealed.contains_key(&slab_id)
					|| state.rehydrated.contains_key(&slab_id)
					|| state.tiered.contains_key(&slab_id)
			};
			if !present {
				self.record_missing_published_references_for_slab(
					slab_id,
					PublishedReferenceOrigin::Recovery,
				);
			}
		}
	}

	/// Test / diagnostic accessor: number of sealed slabs.
	pub async fn sealed_count(&self) -> usize {
		self.state.lock().await.sealed.len()
	}

	/// Reclaim the empty slab files a profile accumulated before ADR 84's
	/// producer guard landed. Runs against a **stopped** profile's directory.
	///
	/// Three conditions make a file safe to unlink, and all three are checked
	/// rather than assumed:
	///
	/// 1. it is zero length, so it cannot hold a record;
	/// 2. it is not the highest-numbered slab, which [`SlabManager::open`]
	///    promotes to active via `split_last` **regardless of length** — in an
	///    affected profile that file is very likely empty, and deleting it
	///    destroys the active slab;
	/// 3. the durable index does not name it.
	///
	/// A candidate failing (3) is reported, not removed. An empty file carrying
	/// a live reference is an inconsistency worth investigating, and deleting it
	/// would erase the evidence of whatever produced it.
	///
	/// Since bootstrap already skips empty slabs, this reclaims disk rather than
	/// restoring startup. It is idempotent: a second run removes nothing.
	pub async fn repair_empty_slabs(
		dir: &Path,
		io: IoRingHandle,
	) -> std::io::Result<EmptySlabRepairReport> {
		// The caller's ring is not adopted into this owner's lifetime (closing the
		// owner would close the caller's ring). Repair only needs metadata
		// operations, so it runs them through its own owner-gated executor.
		let _ = io;
		let owner = DataDirectoryOwner::acquire_existing(dir)?;
		let result = Self::repair_empty_slabs_owned(dir, owner.clone()).await;
		// Close like a node does: drain and join before releasing the lock, and
		// leave the clean-shutdown record exactly when the directory was clean.
		let closed = owner.close(std::time::Duration::from_secs(30)).await;
		let report = result?;
		closed?;
		Ok(report)
	}

	async fn repair_empty_slabs_owned(
		dir: &Path,
		owner: DataDirectoryOwner,
	) -> std::io::Result<EmptySlabRepairReport> {
		if !dir.is_dir() {
			return Err(Error::new(
				ErrorKind::NotFound,
				format!("slab directory does not exist: {}", dir.display()),
			));
		}
		let file_ops: Arc<dyn AsyncFileOps> = Arc::new(BlockingFileOps::with_owner(owner));
		let mut existing_ids = discover_slab_ids(file_ops.as_ref(), dir).await?;
		existing_ids.sort_unstable();
		let mut report = EmptySlabRepairReport {
			slab_files: existing_ids.len(),
			..Default::default()
		};
		let Some((&active_id, sealed_ids)) = existing_ids.split_last() else {
			return Ok(report);
		};
		// Recorded and then never acted on. The active slab is preserved
		// whatever its length, which is the whole point of naming it here.
		report.preserved_active = Some(active_id);
		if file_ops.file_len(&slab_path(dir, active_id)).await? == Some(0) {
			report.empty_slab_files += 1;
		}
		let mut candidates = Vec::new();
		for &id in sealed_ids {
			if file_ops.file_len(&slab_path(dir, id)).await? == Some(0) {
				report.empty_slab_files += 1;
				candidates.push(id);
			}
		}
		if candidates.is_empty() {
			return Ok(report);
		}
		let referenced = referenced_slab_ids(dir).await?;
		for id in candidates {
			if referenced.contains(&id) {
				report.referenced_refused.push(id);
				continue;
			}
			unlink_local_file(file_ops.as_ref(), &slab_path(dir, id)).await?;
			report.removed.push(id);
		}
		Ok(report)
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

	/// Compatibility bridge for a sealed slab recovered or constructed by an
	/// older path that predates manifest registration. Normal sealing already
	/// registers every non-empty slab; maintenance must nevertheless repair a
	/// missing Live row before it can publish that run as Retired.
	fn ensure_live_lsm_run_for_maintenance(&self, slab_id: SlabId, size_bytes: u64) {
		if self.compaction_manifest_snapshot().run(slab_id).is_none() {
			self.record_l0_compaction_run(slab_id, size_bytes);
			// Counter commit markers are physical maintenance frames, but are
			// intentionally absent from the ordinary row index. Preserve a live
			// zero-row run for a non-empty marker-only slab so one-source maintenance
			// can validate and rewrite it rather than treating it as already absent.
			if size_bytes > 0 {
				let mut manifest =
					recover_rwlock_write(&self.compaction_manifest, "compaction_manifest");
				if manifest.get_run(slab_id).is_none() {
					let manifest_epoch = manifest.manifest_epoch();
					manifest.insert_live_run(LsmRunSummary::new(
						slab_id,
						CompactionLevelIndex::L0,
						zero_hlc(),
						zero_hlc(),
						ByteCount::new(size_bytes),
						RowCount::new(0),
						manifest_epoch,
					));
					let snapshot = manifest.snapshot();
					drop(manifest);
					refresh_lsm_level_metrics(&snapshot);
				}
			}
		}
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
			if !entry.value().manager_appends_quiescent() {
				continue;
			}
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

	pub fn slab_pin_table(&self) -> Arc<SlabPinTable> {
		self.slab_pins.clone()
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
		self.require_tier_authority_ready()?;
		{
			let state = self.state.lock().await;
			let Some(slab) = state.sealed.get(&slab_id) else {
				return Err(Error::new(
					ErrorKind::NotFound,
					format!("slab {slab_id} is not in the sealed set"),
				));
			};
			if !slab.manager_appends_quiescent() {
				return Err(Error::new(
					ErrorKind::WouldBlock,
					"sealed slab has in-flight appends",
				));
			}
		}
		let path = slab_path(&self.config.dir, slab_id);
		self.file_ops.read_all(&path).await
	}

	/// Recheck local tier eligibility under the slab publication lock.
	pub(crate) async fn confirm_tier_candidate_quiescent(
		&self,
		slab_id: SlabId,
	) -> std::io::Result<()> {
		let lock = self.tier_publication_lock(slab_id);
		let _guard = lock.lock().await;
		let state = self.state.lock().await;
		if state.tiered.contains_key(&slab_id)
			|| !state
				.sealed
				.get(&slab_id)
				.is_some_and(|slab| slab.manager_appends_quiescent())
		{
			return Err(Error::new(
				ErrorKind::WouldBlock,
				"slab is not a quiescent sealed tier candidate",
			));
		}
		Ok(())
	}

	pub async fn table_lookup_by_index(
		&self,
		request: &TableLookupByIndexRequest,
		descriptor: &SecondaryIndexDescriptor,
		state: SecondaryIndexState,
	) -> std::io::Result<SecondaryIndexLookupReport> {
		self.require_tier_read_scope(&[])?;
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

		// The read-side gate is the INVARIANT — every historical source slab has a
		// durable segment — never the in-memory failure record. A gate keyed on "did
		// this process observe a failure" empties on restart and resurrects the
		// silent-wrong-answer this refusal exists to prevent; completeness derived
		// from durable state survives restart by construction. Answering from the
		// remaining segments over a gap would return success while silently omitting
		// rows — an integrity gap must not be reduced to a successful answer any more
		// than to a generic error. Unaffected indexes keep serving.
		let gaps = self
			.secondary_index_durable_coverage_gaps(request.tenant_id, descriptor)
			.await;
		if !gaps.is_empty() {
			let gap_ids: Vec<u64> = gaps.iter().map(|slab_id| slab_id.get()).collect();
			metrics().set_storage_secondary_index_coverage_gap_slabs(
				request.tenant_id,
				descriptor.table_object_id,
				descriptor.index_object_id,
				gaps.len() as u64,
			);
			metrics().observe_storage_secondary_index_lookup_refusal(
				request.tenant_id,
				descriptor.table_object_id,
				descriptor.index_object_id,
			);
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"secondary index lookup refused: source slab(s) {gap_ids:?} are \
					 uncovered and a successful answer would silently omit their rows"
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
			self.require_tier_read_scope(&[info.source_slab_id])?;
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
					self.lookup_as_of(request.tenant_id, entry.row_key_id, request.as_of_hlc)?;
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
		table_schemas: &TableSchemaCatalog,
	) -> std::io::Result<u64> {
		// A Retired slab remains physically readable until the pin-aware GC sweep
		// disposes it, but it is no longer resident data for convergence. Count
		// only Live manifest runs (and untracked legacy slabs, which predate the
		// manifest and therefore have no state to classify).
		let manifest = self.compaction_manifest_snapshot();
		let (sealed_ids, tiered_pointers) = {
			let state = self.state.lock().await;
			let mut sealed_ids = state
				.sealed
				.iter()
				.map(|entry| *entry.key())
				.filter(|slab_id| manifest.run(*slab_id).is_none_or(|run| run.state.is_live()))
				.collect::<Vec<_>>();
			sealed_ids.sort_unstable();
			let mut tiered_pointers = state
				.tiered
				.iter()
				.map(|entry| entry.value().clone())
				.filter(|pointer| {
					manifest
						.run(pointer.slab_id)
						.is_none_or(|run| run.state.is_live())
				})
				.collect::<Vec<_>>();
			tiered_pointers.sort_by_key(|pointer| pointer.slab_id);
			(sealed_ids, tiered_pointers)
		};

		let mut bytes = 0_u64;
		let acknowledged_by_slab = self.acknowledged_offsets_by_slab();
		let empty = BTreeSet::new();
		for slab_id in sealed_ids {
			let blob = self.read_sealed_raw(slab_id).await?;
			bytes = bytes.saturating_add(uncompressed_resident_bytes_in_slab(
				&blob,
				slab_id,
				table_schemas,
				acknowledged_by_slab.get(&slab_id).unwrap_or(&empty),
			)?);
		}
		for pointer in tiered_pointers {
			let blob = self.tiered_plaintext_from_l2(&pointer).await?;
			bytes = bytes.saturating_add(uncompressed_resident_bytes_in_slab(
				&blob,
				pointer.slab_id,
				table_schemas,
				acknowledged_by_slab.get(&pointer.slab_id).unwrap_or(&empty),
			)?);
		}
		metrics().set_storage_compression_uncompressed_resident_bytes(bytes);
		Ok(bytes)
	}

	pub(crate) fn decode_tiered_object_blob(
		&self,
		pointer: &TieredSlabPointer,
		blob: &[u8],
	) -> std::io::Result<Vec<u8>> {
		validate_tiered_blob(pointer, blob)?;
		// Callers decode the published generation (rewrap keeps its layout).
		let acknowledged = self.acknowledged_offsets_in_slab(pointer.slab_id);
		Ok(decode_tiered_blob(
			self.config.crypto.as_ref(),
			pointer,
			blob,
			TierObjectEvidence::Indexed(&acknowledged),
		)?
		.bytes)
	}

	#[allow(dead_code)]
	pub(crate) async fn load_tiered_column_stats_block(
		&self,
		pointer: &TieredSlabPointer,
		descriptor: &TableSchemaDescriptor,
	) -> std::io::Result<Option<ParsedColumnStatsBlock>> {
		self.require_tier_read_scope(&[pointer.slab_id])?;
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

	async fn has_uncovered_local_rows_for_descriptor(
		&self,
		tenant_id: TenantId,
		descriptor: &TableSchemaDescriptor,
	) -> std::io::Result<bool> {
		let (active_slab_id, sealed_slab_ids) = {
			let state = self.state.lock().await;
			let mut sealed = state
				.sealed
				.iter()
				.map(|entry| *entry.key())
				.collect::<Vec<_>>();
			sealed.sort_unstable();
			(state.active.slab_id(), sealed)
		};
		if self
			.record_rows_for_slab(active_slab_id)?
			.iter()
			.any(|row| row.tenant_id == tenant_id)
		{
			return Ok(true);
		}
		for slab_id in sealed_slab_ids {
			let tenant_rows = self
				.record_rows_for_slab(slab_id)?
				.into_iter()
				.filter(|row| row.tenant_id == tenant_id)
				.collect::<Vec<_>>();
			if tenant_rows.is_empty() {
				continue;
			}
			if matches!(
				descriptor.row_identity_policy,
				RowIdentityPolicy::PrimaryKey(_)
			) {
				return Ok(true);
			}
			let plaintext = self.read_sealed_raw(slab_id).await?;
			let table_ids = self.secondary_index_table_ids_from_plaintext(slab_id, &plaintext)?;
			if table_ids.contains(&(tenant_id, descriptor.table_object_id)) {
				return Ok(true);
			}
			if !table_ids
				.iter()
				.any(|(row_tenant, _)| *row_tenant == tenant_id)
			{
				return Ok(true);
			}
		}
		Ok(false)
	}

	pub(crate) fn active_tenant_kek_id(&self, tenant_id: TenantId) -> std::io::Result<TenantKekId> {
		self.config.crypto.active_tenant_kek_id(tenant_id)
	}

	/// Export the authoritative local slab set as a backup image.
	///
	/// The image includes the active slab and every sealed slab.
	/// Rehydrated cache copies are intentionally excluded because
	/// L2 remains authoritative for those slabs; the manifest
	/// records them as omitted assumptions instead.
	pub async fn export_backup(&self) -> std::io::Result<StorageBackupImage> {
		self.require_tier_authority_ready()?;
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
				source: self.owner_ref()?,
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

	/// Restore a backup image into an empty slab directory, or finish an
	/// identical pending restore after the caller validates its durable intent.
	///
	/// The caller is responsible for opening a new [`SlabManager`]
	/// against the target after the files have been materialized.
	pub(crate) async fn restore_backup(
		backup: &StorageBackupImage,
		dir: impl AsRef<Path>,
		owner: &DataDirectoryOwner,
		resume: bool,
	) -> std::io::Result<()> {
		backup.validate_format()?;
		let dir = dir.as_ref();
		owner.ensure_path(dir)?;
		if !dir.is_dir() {
			return Err(Error::new(
				ErrorKind::NotFound,
				format!("storage restore target does not exist: {}", dir.display()),
			));
		}
		let file_ops: Arc<dyn AsyncFileOps> = Arc::new(BlockingFileOps::with_owner(owner.clone()));
		let existing = discover_slab_ids(file_ops.as_ref(), dir).await?;
		if resume
			&& existing
				.iter()
				.any(|id| !backup.local_slabs.iter().any(|slab| slab.slab_id == *id))
		{
			return Err(Error::new(
				ErrorKind::InvalidData,
				"pending restore contains an unexpected local slab",
			));
		}
		if !resume && !existing.is_empty() {
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
			crate::storage::file_ops::write_restore_file(
				file_ops.as_ref(),
				&path,
				&slab.bytes,
				resume,
			)
			.await?;
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
	) -> std::io::Result<Vec<(u64, RecordLocation)>> {
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
	) -> std::io::Result<Vec<(u64, RecordLocation)>> {
		Ok(self
			.scan_as_of_with_identity_and_locality(
				tenant_id,
				start_locality,
				end_locality,
				as_of_hlc,
			)?
			.into_iter()
			.map(|(_key_identity, locality, loc)| (locality.get(), loc))
			.collect())
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
	) -> std::io::Result<Vec<(Identity, RecordLocation)>> {
		Ok(self
			.scan_as_of_with_identity_and_locality(
				tenant_id,
				start_locality,
				end_locality,
				as_of_hlc,
			)?
			.into_iter()
			.map(|(key_identity, _locality, location)| (key_identity, location))
			.collect())
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
	) -> std::io::Result<Vec<(Identity, Locality, RecordLocation)>> {
		self.require_tier_read_scope(&[])?;
		let start_locality = start_locality.into();
		let end_locality = end_locality.into();
		if start_locality > end_locality {
			return Ok(Vec::new());
		}
		let candidates =
			self.locality_candidates_for_tenant(tenant_id, start_locality, end_locality);
		let mut out = Vec::with_capacity(candidates.len());
		for (_locality, key_identity) in candidates {
			let Some(versions) = self.key_index.get(&(tenant_id, key_identity)) else {
				continue;
			};
			if let Some(location) = newest_version_as_of(versions.value(), as_of_hlc) {
				self.require_tier_read_scope(&[location.slab_id])?;
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
		Ok(out)
	}

	/// Every retained version whose identity projects into the
	/// inclusive locality window `[start_locality, end_locality]`
	/// and whose `hlc` is at or above `migration_hlc_floor`
	/// (Plan 019).
	///
	/// Unlike [`Self::scan`] / [`Self::scan_as_of`], this does not
	/// collapse per-key history — every version ≥ floor is
	/// returned. Output is sorted in [`MigrationCursor`] order so
	/// the migration RPC can paginate with a stable cursor and the
	/// destination can append in the same order it observed on the
	/// source.
	pub fn scan_all_versions(
		&self,
		start_locality: impl Into<Locality>,
		end_locality: impl Into<Locality>,
		migration_hlc_floor: HybridTimestamp,
	) -> std::io::Result<Vec<(TenantId, Identity, Locality, RecordLocation)>> {
		self.scan_all_versions_from(start_locality, end_locality, migration_hlc_floor, None)
	}

	/// Version-aware paging variant of [`Self::scan_all_versions`].
	///
	/// Resumes strictly after `cursor`, the last version a prior page
	/// emitted, mid-key for a fat key. The order, the resume comparison
	/// and the wire cursor are all [`MigrationCursor`]: locality, tenant,
	/// key, HLC, with each row's own routing locality.
	pub fn scan_all_versions_from(
		&self,
		start_locality: impl Into<Locality>,
		end_locality: impl Into<Locality>,
		migration_hlc_floor: HybridTimestamp,
		cursor: Option<MigrationCursor>,
	) -> std::io::Result<Vec<(TenantId, Identity, Locality, RecordLocation)>> {
		self.require_tier_read_scope(&[])?;
		let start_locality = start_locality.into();
		let end_locality = end_locality.into();
		if start_locality > end_locality {
			return Ok(Vec::new());
		}
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
				self.require_tier_read_scope(&[loc.slab_id])?;
				let position = MigrationCursor {
					locality,
					tenant_id,
					key_identity,
					hlc: loc.hlc,
				};
				if cursor.is_some_and(|cursor| position <= cursor) {
					continue;
				}
				out.push((tenant_id, key_identity, locality, *loc));
			}
		}
		out.sort_by_key(
			|&(tenant_id, key_identity, locality, location)| MigrationCursor {
				locality,
				tenant_id,
				key_identity,
				hlc: location.hlc,
			},
		);
		Ok(out)
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
	) -> std::io::Result<bool> {
		self.require_tier_read_scope(&[slab_id])?;
		Ok(tombstones.iter().any(|tombstone| {
			tombstone.completed_at_hlc <= as_of_hlc
				&& self.migrated_tombstone_affects_slab(slab_id, tombstone)
		}))
	}

	pub fn slab_has_supersession_at(
		&self,
		slab_id: SlabId,
		as_of_hlc: HybridTimestamp,
	) -> std::io::Result<bool> {
		self.require_tier_read_scope(&[slab_id])?;
		let unbounded_as_of =
			as_of_hlc.physical_ms == 0 && as_of_hlc.logical == 0 && as_of_hlc.node_id == 0;
		Ok(self.key_index.iter().any(|entry| {
			let versions = entry.value();
			versions.iter().enumerate().any(|(index, location)| {
				location.slab_id == slab_id
					&& versions[index + 1..].iter().any(|newer| {
						!newer.is_tombstone
							&& newer.hlc > location.hlc
							&& (unbounded_as_of || newer.hlc <= as_of_hlc)
					})
			})
		}))
	}

	pub fn slab_hlc_bounds(
		&self,
		slab_id: SlabId,
	) -> std::io::Result<Option<(HybridTimestamp, HybridTimestamp, u64)>> {
		self.require_tier_read_scope(&[slab_id])?;
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
		Ok(bounds)
	}

	pub fn slab_supersession_metadata(
		&self,
		slab_id: SlabId,
	) -> std::io::Result<SupersessionMetadata> {
		self.require_tier_read_scope(&[slab_id])?;
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
		Ok(earliest.map_or(SupersessionMetadata::None, SupersessionMetadata::Earliest))
	}

	pub fn slab_row_tombstone_metadata(
		&self,
		slab_id: SlabId,
	) -> std::io::Result<TombstoneMetadata> {
		self.require_tier_read_scope(&[slab_id])?;
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
		Ok(earliest.map_or(TombstoneMetadata::None, TombstoneMetadata::Earliest))
	}

	pub async fn record_supersession_for_slab(
		&self,
		slab_id: SlabId,
		supersession_hlc: HybridTimestamp,
		last_control_log_index: Option<u64>,
	) -> std::io::Result<bool> {
		self.update_tier_pointer_metadata(slab_id, last_control_log_index, None, |pointer| {
			pointer.record_supersession_hlc(supersession_hlc)
		})
		.await
	}

	pub async fn record_row_tombstone_for_slab(
		&self,
		slab_id: SlabId,
		tombstone_hlc: HybridTimestamp,
		last_control_log_index: Option<u64>,
	) -> std::io::Result<bool> {
		self.update_tier_pointer_metadata(slab_id, last_control_log_index, None, |pointer| {
			pointer.record_tombstone_hlc(tombstone_hlc)
		})
		.await
	}

	/// Serialize control-observed LSM manifest changes with snapshot installation.
	pub(crate) async fn observe_lsm_compaction_commit(
		&self,
		commit: &crate::storage::CompactionCommit,
		index: u64,
	) -> std::io::Result<()> {
		let _gate = self.persistent_index_gate.clone().read_owned().await;
		if self.observer_is_superseded(Some(index)) {
			return Ok(());
		}
		if self.snapshot_install_in_flight() {
			self.index_authority_unavailable
				.store(true, Ordering::Release);
			return Err(Error::new(
				ErrorKind::WouldBlock,
				"snapshot installation fences LSM observer",
			));
		}
		self.apply_lsm_manifest_compaction_commit(commit)
			.map(|_| ())
			.map_err(|err| Error::other(err.to_string()))
	}

	pub async fn record_migrated_range_tombstone(
		&self,
		tombstone: &MigratedRangeTombstone,
		last_control_log_index: Option<u64>,
	) -> std::io::Result<usize> {
		self.record_migrated_range_tombstone_impl(tombstone, last_control_log_index)
			.await
	}

	/// Admit a frame of `frame_len` bytes to the active slab and reserve its
	/// page-rounded record there, rotating first if the slab has crossed the
	/// size cap or the record would carry it across. Admission and reservation
	/// happen under one lock, so the cursor a later writer is judged against
	/// already includes this record. An empty slab admits one complete record of
	/// any size: the cap bounds overshoot to one padded record and never refuses
	/// a frame (ADR 0098).
	fn pending_counter_frame(
		tenant_id: TenantId,
		key_identity: Identity,
		hlc: HybridTimestamp,
		origin: NodeId,
		slab_id: SlabId,
		offset: u64,
	) -> PendingCounterFrame {
		(
			tenant_id,
			key_identity,
			hlc.physical_ms,
			hlc.logical,
			hlc.node_id,
			origin.id,
			slab_id,
			offset,
		)
	}

	async fn target_for_append(
		&self,
		frame_len: usize,
		pending_counter: Option<(TenantId, Identity, HybridTimestamp, NodeId)>,
	) -> std::io::Result<(Arc<LocalSlabStore>, AppendReservation)> {
		let (_, padded) = LocalSlabStore::padded_record_size(frame_len)?;
		let mut state = self.state.lock().await;
		let cursor = state.active.write_cursor();
		let fits =
			cursor == 0 || cursor.saturating_add(padded as u64) <= self.config.max_slab_bytes;
		if cursor < self.config.max_slab_bytes && fits {
			let mut reservation = state.active.reserve_append(frame_len)?;
			state.active.track_manager_reservation(&mut reservation)?;
			let slab = state.active.clone();
			if let Some((tenant_id, key_identity, hlc, origin)) = pending_counter {
				let pending = Self::pending_counter_frame(
					tenant_id,
					key_identity,
					hlc,
					origin,
					slab.slab_id(),
					reservation.offset(),
				);
				self.counter_pending_frames.insert(pending);
				self.counter_pending_guards.insert(
					pending,
					reservation
						.quiescence_guard()
						.expect("tracked manager reservation"),
				);
			}
			return Ok((slab, reservation));
		}

		// Rotate. The check above and the mutation below happen
		// under the same lock, so concurrent overflow triggers
		// exactly one rotation.
		let next_id = state.next_slab_id;
		let next_end = self.cover_slab_ids_through(&mut state, next_id).await?;
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
		state.install_sealed(&self.tier_budget, sealed_id, sealed_slab);
		state.advance_slab_allocator(next_end)?;
		self.record_l0_compaction_run(sealed_id, sealed_size);

		let mut reservation = next_slab.reserve_append(frame_len)?;
		next_slab.track_manager_reservation(&mut reservation)?;
		if let Some((tenant_id, key_identity, hlc, origin)) = pending_counter {
			let pending = Self::pending_counter_frame(
				tenant_id,
				key_identity,
				hlc,
				origin,
				next_slab.slab_id(),
				reservation.offset(),
			);
			self.counter_pending_frames.insert(pending);
			self.counter_pending_guards.insert(
				pending,
				reservation
					.quiescence_guard()
					.expect("tracked manager reservation"),
			);
		}
		Ok((next_slab, reservation))
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
			let Some(next) = self.redirects.resolve(&location) else {
				return location;
			};
			if next == location {
				return location;
			}
			location = next;
		}
		location
	}

	/// An L2 object that a committed pointer named, or that a commit with an
	/// ambiguous result may name, is kept rather than deleted. A snapshot
	/// installed later may still name it, and openraft installs snapshots on
	/// the apply worker, so a delete cannot be fenced against installs
	/// without stalling apply. Deletion returns with owner-scoped retirement
	/// authority. Infallible; it returns `Result` so call sites keep their
	/// cleanup sequencing unchanged.
	pub(crate) fn retain_superseded_l2_object(
		slab_id: SlabId,
		uri: &str,
		reason: &'static str,
	) -> std::io::Result<()> {
		crate::metrics::global()
			.storage_l2_superseded_objects_retained_total
			.inc();
		tracing::info!(
			slab_id = slab_id.get(),
			l2_uri = %uri,
			reason,
			"retained superseded L2 object"
		);
		Ok(())
	}
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
		self.require_tier_authority_ready()?;
		let checked_same_hlc = self
			.refuse_conflicting_version(
				tenant_id,
				key_identity,
				hlc,
				IncomingVersion::Record(frame),
			)
			.await?;
		let (slab, location, _append_guard) = self
			.append_on_target(tenant_id, key_identity, hlc, frame)
			.await?;
		let publication = self.begin_index_publication(vec![location.slab_id]).await?;
		// Commit the persistent row before readers can observe this version.
		// A failed/uncertain transaction keeps its publication fence and never
		// publishes the new manager location.
		if let Err(error) = self
			.persist_record_row(tenant_id, key_identity, location, &publication)
			.await
		{
			slab.rollback_index_entry(tenant_id, key_identity, location);
			return Err(error);
		}
		if let Err(error) = self.index_checked_version(
			tenant_id,
			key_identity,
			Locality::from(key_identity),
			location,
			checked_same_hlc,
		) {
			slab.rollback_index_entry(tenant_id, key_identity, location);
			return Err(error);
		}
		publication.complete();
		Ok(location)
	}

	async fn read(&self, location: RecordLocation) -> std::io::Result<Vec<u8>> {
		self.read_with_reference_origin(location, PublishedReferenceOrigin::Read)
			.await
	}

	fn lookup(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
	) -> std::io::Result<Option<RecordLocation>> {
		let location = self
			.key_index
			.get(&(tenant_id, key_identity))
			.and_then(|v| v.last().copied());
		let scope = location
			.iter()
			.map(|location| location.slab_id)
			.collect::<Vec<_>>();
		self.require_tier_read_scope(&scope)?;
		Ok(location)
	}

	fn lookup_as_of(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
		as_of_hlc: HybridTimestamp,
	) -> std::io::Result<Option<RecordLocation>> {
		let location = self
			.key_index
			.get(&(tenant_id, key_identity))
			.and_then(|versions| newest_version_as_of(versions.value(), as_of_hlc));
		let scope = location
			.iter()
			.map(|location| location.slab_id)
			.collect::<Vec<_>>();
		self.require_tier_read_scope(&scope)?;
		Ok(location)
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
		descriptor: &TableSchemaDescriptor,
		requirement: &ColumnFetchRequirement,
		predicate: Option<&crate::schema::pbc::Program>,
	) -> std::io::Result<Option<ProjectedL2RecordRead>> {
		if !self.config.l2_column_fetch.enabled && !self.config.l2_row_group_fetch.enabled {
			return Ok(None);
		}
		let ReadTarget {
			backing,
			location,
			_pin,
			..
		} = self.resolve_read_target(location, false).await?;
		match backing {
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
		descriptor: &TableSchemaDescriptor,
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
		let ReadTarget {
			backing,
			location,
			_pin,
			..
		} = self.resolve_read_target(location, false).await?;
		match backing {
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
		descriptor: &TableSchemaDescriptor,
		requirement: &ColumnFetchRequirement,
	) -> std::io::Result<Option<ProjectedL2RecordRead>> {
		if locations.is_empty()
			|| (!self.config.l2_column_fetch.enabled && !self.config.l2_row_group_fetch.enabled)
		{
			return Ok(None);
		}
		let (backing, relocated, _pins) = self.resolve_read_targets(locations).await?;
		let first = relocated[0];
		for location in &relocated {
			if indexed_record_fetch_key(*location) != indexed_record_fetch_key(first) {
				return Err(Error::new(
					ErrorKind::InvalidInput,
					"projected indexed row fetch requires one physical record",
				));
			}
		}
		match backing {
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

	fn record_rows_for_slab(&self, slab_id: SlabId) -> std::io::Result<Vec<PersistentRecordRow>> {
		self.require_tier_authority_ready()?;
		Ok(self.record_rows_for_slab_for_recovery(slab_id))
	}

	/// How many index rows name `slab_id`, counted without collecting them.
	fn count_record_rows_for_slab(&self, slab_id: SlabId) -> usize {
		self.key_index
			.iter()
			.map(|entry| {
				entry
					.value()
					.iter()
					.filter(|location| location.slab_id == slab_id)
					.count()
			})
			.sum()
	}

	fn record_rows_for_slab_for_recovery(&self, slab_id: SlabId) -> Vec<PersistentRecordRow> {
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

	pub(crate) async fn prepare_tiered_table_retention_rewrite(
		&self,
		pointer: &TieredSlabPointer,
		cluster_policy: MvccRetentionPolicy,
		table_lifecycle: &TableLifecycleCatalog,
		table_schemas: &TableSchemaCatalog,
		now_ms: u64,
	) -> std::io::Result<Option<PreparedTieredRetentionRewrite>> {
		self.require_tier_authority_ready()?;
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
		// The rewrite source is the published generation: the index's offsets
		// are the independent evidence for every acknowledged record in it.
		let plaintext = Zeroizing::new(
			decode_tiered_blob(
				self.config.crypto.as_ref(),
				pointer,
				&old_blob,
				TierObjectEvidence::Indexed(&self.acknowledged_offsets_in_slab(pointer.slab_id)),
			)?
			.bytes,
		);
		let mut records = recover_slab_entries_from_blob(
			&plaintext,
			pointer.slab_id,
			&self.acknowledged_offsets_in_slab(pointer.slab_id),
		)?
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
		self.keep_conflicting_versions(&mut records);
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
				old_generation: crate::consensus::slab_ops::TierGeneration::from(pointer),
				old_bytes: old_blob.len() as u64,
				pruned_versions,
				retention_stats,
				reclaimed_bytes: old_blob.len() as u64,
			}));
		}

		let tmp_path = retention_rewrite_tmp_path(&self.config.dir, pointer.slab_id);
		let _ = unlink_local_file(self.file_ops.as_ref(), &tmp_path).await;
		let mut multirow_recompact_count = 0_u64;
		// The replacement's own record offsets: the index still describes the
		// old layout, so every walk of the new bytes takes these instead.
		let mut replacement_offsets = BTreeSet::new();
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
				let location = rewritten
					.append(
						compacted.tenant_id,
						compacted.key_identity,
						compacted.hlc,
						&compacted.frame,
					)
					.await?;
				replacement_offsets.insert(location.offset);
			}
			rewritten.fsync().await?;
			Ok(Zeroizing::new(self.file_ops.read_all(&tmp_path).await?))
		}
		.await;
		let _ = unlink_local_file(self.file_ops.as_ref(), &tmp_path).await;
		let rewritten_plaintext = rewrite_result?;
		self.preflight_tier_publication(
			pointer.slab_id,
			&plaintext,
			&rewritten_plaintext,
			&replacement_offsets,
		)
		.await?;
		if multirow_recompact_count > 0 {
			metrics().observe_storage_multirow_recompact(multirow_recompact_count);
		}
		let encrypted = self.encrypt_tiered_blob_with_evidence(
			pointer.slab_id,
			rewritten_plaintext.as_ref(),
			&replacement_offsets,
			Some(table_schemas),
		)?;
		let row_tombstone_metadata = row_tombstone_metadata_from_blob(
			rewritten_plaintext.as_ref(),
			pointer.slab_id,
			&replacement_offsets,
		)?;
		Ok(Some(PreparedTieredRetentionRewrite::Rewrite {
			slab_id: pointer.slab_id,
			old_l2_uri: pointer.l2_uri.clone(),
			old_generation: crate::consensus::slab_ops::TierGeneration::from(pointer),
			old_bytes: old_blob.len() as u64,
			payload_hash: crate::hash::content::digest(&encrypted.bytes),
			tombstone_metadata: merge_row_tombstone_metadata(
				pointer.tombstone_metadata,
				row_tombstone_metadata,
			),
			supersession_metadata: self.slab_supersession_metadata(pointer.slab_id)?,
			reclaimed_bytes: (old_blob.len() as u64).saturating_sub(encrypted.bytes.len() as u64),
			pruned_versions,
			retention_stats,
			encrypted_bytes: encrypted.bytes,
		}))
	}

	pub(crate) async fn prepare_tiered_table_drop_rewrite(
		&self,
		pointer: &TieredSlabPointer,
		tenant_id: TenantId,
		descriptor: &TableSchemaDescriptor,
		table_schemas: &TableSchemaCatalog,
	) -> std::io::Result<Option<PreparedTieredTableDropRewrite>> {
		self.require_tier_authority_ready()?;
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
		// The rewrite source is the published generation: the index's offsets
		// are the independent evidence for every acknowledged record in it.
		let plaintext = Zeroizing::new(
			decode_tiered_blob(
				self.config.crypto.as_ref(),
				pointer,
				&old_blob,
				TierObjectEvidence::Indexed(&self.acknowledged_offsets_in_slab(pointer.slab_id)),
			)?
			.bytes,
		);
		let mut records = recover_slab_entries_from_blob(
			&plaintext,
			pointer.slab_id,
			&self.acknowledged_offsets_in_slab(pointer.slab_id),
		)?
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
		self.mark_records_for_table_drop_from_blob(
			&plaintext,
			&mut records,
			tenant_id,
			descriptor,
		)?;
		records.sort_by_key(|record| record.old_location.offset);
		let physical_groups = physical_compaction_groups(&records);
		let reclaimed_records = records.iter().filter(|record| record.reclaim).count();
		let survivors = records.iter().filter(|record| !record.reclaim).count();
		if reclaimed_records == 0 {
			return Ok(None);
		}
		if survivors == 0 {
			return Ok(Some(PreparedTieredTableDropRewrite::Retire {
				slab_id: pointer.slab_id,
				old_l2_uri: pointer.l2_uri.clone(),
				old_generation: crate::consensus::slab_ops::TierGeneration::from(pointer),
				old_bytes: old_blob.len() as u64,
				reclaimed_records,
				reclaimed_bytes: old_blob.len() as u64,
			}));
		}

		let tmp_path = table_drop_rewrite_tmp_path(&self.config.dir, pointer.slab_id);
		let _ = unlink_local_file(self.file_ops.as_ref(), &tmp_path).await;
		let mut multirow_recompact_count = 0_u64;
		// The replacement's own record offsets: the index still describes the
		// old layout, so every walk of the new bytes takes these instead.
		let mut replacement_offsets = BTreeSet::new();
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
				let location = rewritten
					.append(
						compacted.tenant_id,
						compacted.key_identity,
						compacted.hlc,
						&compacted.frame,
					)
					.await?;
				replacement_offsets.insert(location.offset);
			}
			rewritten.fsync().await?;
			Ok(Zeroizing::new(self.file_ops.read_all(&tmp_path).await?))
		}
		.await;
		let _ = unlink_local_file(self.file_ops.as_ref(), &tmp_path).await;
		let rewritten_plaintext = rewrite_result?;
		self.preflight_tier_publication(
			pointer.slab_id,
			&plaintext,
			&rewritten_plaintext,
			&replacement_offsets,
		)
		.await?;
		if multirow_recompact_count > 0 {
			metrics().observe_storage_multirow_recompact(multirow_recompact_count);
		}
		let encrypted = self.encrypt_tiered_blob_with_evidence(
			pointer.slab_id,
			rewritten_plaintext.as_ref(),
			&replacement_offsets,
			Some(table_schemas),
		)?;
		let row_tombstone_metadata = row_tombstone_metadata_from_blob(
			rewritten_plaintext.as_ref(),
			pointer.slab_id,
			&replacement_offsets,
		)?;
		Ok(Some(PreparedTieredTableDropRewrite::Rewrite {
			slab_id: pointer.slab_id,
			old_l2_uri: pointer.l2_uri.clone(),
			old_generation: crate::consensus::slab_ops::TierGeneration::from(pointer),
			old_bytes: old_blob.len() as u64,
			payload_hash: crate::hash::content::digest(&encrypted.bytes),
			tombstone_metadata: merge_row_tombstone_metadata(
				pointer.tombstone_metadata,
				row_tombstone_metadata,
			),
			supersession_metadata: self.slab_supersession_metadata(pointer.slab_id)?,
			reclaimed_bytes: (old_blob.len() as u64).saturating_sub(encrypted.bytes.len() as u64),
			reclaimed_records,
			encrypted_bytes: encrypted.bytes,
		}))
	}
}

// ============================================================
//  HELPERS
// ============================================================

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
	table_schemas: &TableSchemaCatalog,
	acknowledged: &BTreeSet<u64>,
) -> std::io::Result<u64> {
	let mut entries_by_record: BTreeMap<(u64, u32), Vec<RecoveredSlabEntry>> = BTreeMap::new();
	for entry in recover_slab_entries_from_blob(blob, slab_id, acknowledged)? {
		entries_by_record
			.entry((entry.location.offset, entry.location.length))
			.or_default()
			.push(entry);
	}

	let mut descriptors_by_tenant: HashMap<TenantId, Vec<TableSchemaDescriptor>> = HashMap::new();
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

fn row_tombstone_metadata_from_blob(
	blob: &[u8],
	slab_id: SlabId,
	acknowledged: &BTreeSet<u64>,
) -> std::io::Result<TombstoneMetadata> {
	let entries = recover_slab_entries_from_blob(blob, slab_id, acknowledged)?;
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

fn single_tenant_id_from_plain_slab(
	blob: &[u8],
	slab_id: SlabId,
	acknowledged: &BTreeSet<u64>,
) -> std::io::Result<TenantId> {
	Ok(
		build_tiered_object_metadata(blob, slab_id, &L2ChunkLayoutConfig::default(), acknowledged)?
			.tenant_id,
	)
}

fn slab_path(dir: &Path, slab_id: SlabId) -> PathBuf {
	dir.join(format!("slab_{:016x}.wnd", slab_id))
}

fn rehydrate_tmp_path(dir: &Path, slab_id: SlabId) -> PathBuf {
	dir.join(format!("slab_{:016x}.wnd.rehydrate.tmp", slab_id))
}

fn stats_advisory_fields(
	locator: Option<crate::crypto::TieredStatsBlockLocator>,
) -> (Option<u16>, Option<u32>, Option<[u8; 16]>) {
	match locator {
		Some(locator) => (
			Some(locator.stats_format_version),
			Some(locator.stats_block_len),
			Some(locator.stats_block_hash128),
		),
		None => (None, None, None),
	}
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
/// Outcome of an offline empty-slab repair.
///
/// Refusals are reported separately from removals on purpose: an operator
/// deciding whether a profile is healthy needs to see what the repair declined
/// to touch, and a single "cleaned N files" number would hide exactly the case
/// worth looking at.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EmptySlabRepairReport {
	/// Every `slab_<id>.wnd` file discovered.
	pub slab_files: usize,
	/// Files of zero length, counting the active slab if it is empty.
	pub empty_slab_files: usize,
	/// The highest-numbered slab, preserved unconditionally.
	pub preserved_active: Option<SlabId>,
	/// Empty slabs the durable index still names. These are kept.
	pub referenced_refused: Vec<SlabId>,
	/// Empty, non-active, unreferenced slabs that were unlinked.
	pub removed: Vec<SlabId>,
}

/// Every slab id the durable index still names, across every table that can
/// carry one.
///
/// Deliberately a superset of the three tables `can_retire_local_slab` consults.
/// That function decides retirement inside a running manager that knows its own
/// state; this one runs offline against an operator's profile with no such
/// context, where refusing one slab too many costs an empty file and deleting
/// one slab too few costs a reference.
async fn referenced_slab_ids(dir: &Path) -> std::io::Result<HashSet<SlabId>> {
	let index_path = persistent_index_path(dir);
	// An absent index cannot distinguish an unreferenced slab from unavailable
	// authority. Refuse explicitly; open itself also never creates the index.
	if !index_path.exists() {
		return Err(Error::new(
			ErrorKind::NotFound,
			format!(
				"empty-slab repair refuses to run without the durable index at {}: \
				 an absent index cannot distinguish 'nothing is referenced' from \
				 'nothing could be asked'",
				index_path.display()
			),
		));
	}
	let index = PersistentLocalIndex::open(index_path.clone())
		.await
		.map_err(|err| {
			Error::new(
				ErrorKind::InvalidData,
				format!(
					"empty-slab repair refuses to run without the durable index at {}: {err}",
					index_path.display()
				),
			)
		})?;
	let snapshot = index.load_snapshot().await.map_err(|err| {
		Error::new(
			ErrorKind::InvalidData,
			format!(
				"empty-slab repair refuses to run without a readable index snapshot at {}: {err}",
				index_path.display()
			),
		)
	})?;
	let mut referenced = HashSet::new();
	for row in snapshot
		.record_rows
		.iter()
		.chain(snapshot.tiered_records.iter())
	{
		referenced.insert(row.location.slab_id);
	}
	for pointer in &snapshot.tiered_slabs {
		referenced.insert(pointer.slab_id);
	}
	for row in &snapshot.secondary_index_segments {
		referenced.insert(row.key.source_slab_id);
	}
	for run in &snapshot.lsm_runs {
		referenced.insert(run.slab_id);
	}
	Ok(referenced)
}

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

pub(crate) const AWAITING_CLUSTER_MARKER: &str = "TIER_AUTHORITY_AWAITING_CLUSTER";

#[cfg(test)]
mod tests;

#[cfg(test)]
impl SlabManager {
	pub(crate) fn fail_next_retirement_transaction_for_test(&self) {
		self.persistent_index
			.as_ref()
			.unwrap()
			.fail_next_retirement
			.store(true, std::sync::atomic::Ordering::Release);
	}
}

/// Test support: read-only views of durable LSM state for acceptance tests.
#[cfg(feature = "test-support")]
pub mod durable_lsm_view {
	use super::SlabId;

	/// What protects one compaction source while its unlink is pending, read
	/// from the manager's own durable index without changing it.
	#[derive(Clone, Debug, PartialEq, Eq)]
	pub struct SourceProtection {
		/// The source's durable run state, if a run is recorded.
		pub run_state: Option<String>,
		/// Unresolved install intents that are a Control/Prepared pure GC
		/// removal of exactly this source, with exactly this source as input,
		/// whose commit digest matches the commit.
		pub exact_removal_intents: usize,
		/// Every other unresolved install intent naming this source.
		pub other_intents_naming_source: usize,
		/// Durable record rows (local and tiered) located in this source.
		pub durable_rows_in_source: usize,
	}

	pub async fn source_protection(
		manager: &super::SlabManager,
		source: SlabId,
	) -> std::io::Result<SourceProtection> {
		use crate::storage::index::{LsmInstallKind, LsmInstallPhase};
		let index = manager
			.persistent_index
			.as_ref()
			.ok_or_else(|| std::io::Error::new(std::io::ErrorKind::NotFound, "no durable index"))?;
		let snapshot = index.load_snapshot().await?;
		let run_state = snapshot
			.lsm_runs
			.iter()
			.find(|run| run.slab_id == source)
			.map(|run| format!("{:?}", run.state));
		let mut exact_removal_intents = 0;
		let mut other_intents_naming_source = 0;
		for intent in &snapshot.lsm_install_intents {
			let commit = &intent.commit;
			let names = commit.slabs_to_remove.contains(&source)
				|| commit.slabs_to_retire.contains(&source)
				|| commit.slabs_to_mark_delete_pending.contains(&source)
				|| intent.inputs.iter().any(|input| input.slab_id == source);
			if !names {
				continue;
			}
			let digest_matches = serde_json::to_vec(commit)
				.map(|bytes| *blake3::hash(&bytes).as_bytes() == intent.commit_digest)
				.unwrap_or(false);
			let exact = intent.kind == LsmInstallKind::Control
				&& intent.phase == LsmInstallPhase::Prepared
				&& commit.slabs_to_remove == [source]
				&& commit.slabs_to_add.is_empty()
				&& commit.slabs_to_retire.is_empty()
				&& commit.slabs_to_mark_delete_pending.is_empty()
				&& commit.sidecar_swaps.is_empty()
				&& intent.inputs.len() == 1
				&& intent.inputs[0].slab_id == source
				&& digest_matches;
			if exact {
				exact_removal_intents += 1;
			} else {
				other_intents_naming_source += 1;
			}
		}
		let durable_rows_in_source = snapshot
			.record_rows
			.iter()
			.chain(&snapshot.tiered_records)
			.filter(|row| row.location.slab_id == source)
			.count();
		Ok(SourceProtection {
			run_state,
			exact_removal_intents,
			other_intents_naming_source,
			durable_rows_in_source,
		})
	}
}
