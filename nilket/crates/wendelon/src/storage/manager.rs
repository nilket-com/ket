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
	CompactionLevelIndex, CounterCommitFrame, CounterTentativeFrame, EncodingPageByteOffset,
	HllPrecision, HybridTimestamp, INDEX_REJECT_LOGICAL_FINGERPRINT_MISMATCH, Identity,
	IdentityPrefix, IndexObjectId, IpcBufferIndex, IpcColumnOrdinal, Locality,
	LogicalSchemaFingerprint, ManifestEpoch, NdvFact, RecordBodyByteLen, RecordBodyByteOffset,
	RecordRowIndex, RowCount, RowGroupIndex, SecondaryIndexCatalog, SecondaryIndexDescriptor,
	SecondaryIndexState, SelectivityFractionMicros, SimdPageValueCount, SlabId, SlabRecordHeader,
	SupersessionMetadata, TableLifecycleCatalog, TableOptimizerFacts,
	TableReferenceIntegrityFailureMeta, TenantId, TenantKekId, TombstoneMetadata, WriterNodeId,
	range_fact_tag_supported,
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
	GroupCommitConfig, LocalSlabStore, MultiRowIndexEntry, RecordLocation, RecoveredSlabEntry,
	StorageEngine, insert_version_sorted, newest_version_as_of, recover_slab_entries_from_blob,
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
	OpenFlags, SlabPinGuard, SlabPinTable, TierCommitter, VersionEntry, VersionLifecycle,
	VisibleDomainRow, apply_elision, cluster_retention_floor, compaction_visible_domain_summary,
	effective_table_retention, pick_compaction, required_output_run_count,
	storage_row_table_object_id_at, survivor_records, write_file,
};
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
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::sync::{Mutex, OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock as AsyncRwLock};
use tokio::task::JoinSet;
use tokio::time::Instant;
use zeroize::Zeroizing;

mod encoding_compute;
mod retention_reclaim;
mod stats_facts;
use retention_reclaim::{
	cleanup_retention_rewrite_tmp_files, retention_rewrite_tmp_path, table_drop_rewrite_tmp_path,
};
mod tiered_io;
use tiered_io::{
	RehydratedSlabState, SlabBacking, build_tiered_object_metadata, decode_tiered_blob,
	descriptor_matches_tiered_row, extract_record_from_blob, fetch_tiered_ciphertext_runs,
	validate_tiered_blob, validate_tiered_record_header_tenant,
};
mod compaction;
use compaction::{
	CompactionOutcome, CompactionRecord, RelocatedRecord, compact_physical_record_frame,
	physical_compaction_group_indices, physical_compaction_groups,
};
mod lsm_driver;
use lsm_driver::{
	cleanup_lsm_compaction_tmp_files, lsm_run_summary_from_index, refresh_lsm_level_metrics,
	remove_non_live_lsm_rows_from_index,
};
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

const STORAGE_BACKUP_FORMAT_VERSION: u16 = 1;

type TenantKey = (TenantId, Identity);
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

/// Outcome of one explicit table-drop reclaim pass.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TableDropReclaimReport {
	pub compacted_slabs: usize,
	pub reclaimed_records: usize,
	pub reclaimed_bytes: u64,
}

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

pub(crate) enum PreparedTieredTableDropRewrite {
	Rewrite {
		slab_id: SlabId,
		old_l2_uri: String,
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
		reclaimed_records: usize,
		reclaimed_bytes: u64,
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
	/// Serializes table admission against the one-time declaration cut. Table
	/// writes hold a read guard from catalog snapshot through local append;
	/// declaration holds the write guard while it seals the old active slab and
	/// installs the new active buffer.
	secondary_index_admission_gate: Arc<AsyncRwLock<()>>,
	/// Process-local acceleration for declaration cuts already established. A
	/// restart may repeat a cut before the durable completion receipt exists;
	/// that is safe and only creates another historical slab to backfill.
	secondary_index_declaration_cuts: DashSet<SecondaryIndexCoverageKey>,
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
		let (removed, should_remove_key) = if let Some(mut versions) = self.key_index.get_mut(&key)
		{
			let removed = remove_version_exact(&mut versions, location);
			(removed, removed && versions.is_empty())
		} else {
			(false, false)
		};
		if removed {
			self.remove_published_table_reference(location);
		}
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

	// ============================================================
	//  ADR 075 counter frames (counter-shaped, fenced)
	//
	//  These are deliberately NOT a general "append any record_class
	//  without indexing" API — each method fixes its own counter
	//  `record_class` internally and applies counter-specific index
	//  semantics, so ordinary append/index/LWW behaviour is untouched
	//  and no second generic append path exists.
	// ============================================================

	/// Append a tentative counter value to the active slab and index it live in
	/// the authoritative `key_index`. The record class is fixed
	/// `COUNTER_TENTATIVE`; recovery indexes it only if a matching commit marker
	/// is found, so an un-committed tentative does not survive restart.
	pub async fn counter_append_tentative(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
		hlc: HybridTimestamp,
		value: u64,
	) -> std::io::Result<RecordLocation> {
		let slab = self.target_for_append().await?;
		let frame = CounterTentativeFrame { value };
		let location = slab
			.write_framed(
				tenant_id,
				key_identity,
				hlc,
				crate::schema::record_class::COUNTER_TENTATIVE,
				bytemuck::bytes_of(&frame),
			)
			.await?;
		self.index_appended_location(tenant_id, key_identity, location);
		Ok(location)
	}

	/// Append a durable commit marker for `tentative_op_id` (the tentative's slab
	/// offset) to the active slab and flush. The record class is fixed
	/// `COUNTER_COMMIT`; the marker is NOT a version, so it is not indexed. The
	/// tentative and its marker must be co-located in one slab — true within a
	/// single add (no rotation between the two writes); cross-slab pairing under
	/// rotation is a follow-on for the compaction path.
	pub async fn counter_append_commit_marker(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
		hlc: HybridTimestamp,
		tentative_op_id: u64,
	) -> std::io::Result<()> {
		let slab = self.target_for_append().await?;
		let marker = CounterCommitFrame { tentative_op_id };
		slab.write_framed(
			tenant_id,
			key_identity,
			hlc,
			crate::schema::record_class::COUNTER_COMMIT,
			bytemuck::bytes_of(&marker),
		)
		.await?;
		slab.wait_durable_commit().await?;
		Ok(())
	}

	/// Remove a tentative counter version from the live `key_index` (rollback).
	/// No durable abort marker — recovery skips the orphan (it has no commit
	/// marker). Counter-shaped wrapper over the in-memory version removal.
	pub fn counter_unindex_tentative(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
		location: RecordLocation,
	) {
		self.rollback_indexed_location(tenant_id, key_identity, location);
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
			table_object_id: self.published_table_object_id(location),
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
			if let Some(table_object_id) = row.table_object_id {
				self.add_published_table_reference(tenant_id, table_object_id, row.location);
			}
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
	/// * **One or more existing slabs**: treats the highest-numbered slab as
	///   active, skips zero-length sealed files, opens the non-empty sealed files
	///   (recovering each slab's on-disk records into its local index), and sets
	///   `next_slab_id` to `max_id + 1`.
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
		let table_by_location: DashMap<RecordLocation, (TenantId, TableObjectId)> = DashMap::new();
		let table_references_by_slab: DashMap<SlabId, BTreeMap<(TenantId, TableObjectId), usize>> =
			DashMap::new();
		let secondary_index_table_references_by_slab: DashMap<
			SlabId,
			BTreeMap<(TenantId, TableObjectId), usize>,
		> = DashMap::new();
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
			// max becomes active. Zero-length sealed files are not opened:
			// they cannot contain records, and retaining them would recreate
			// the descriptor exhaustion that this recovery path bounds.
			let (&active_id, sealed_ids) = existing_ids.split_last().expect("non-empty");
			let active_path = slab_path(&config.dir, active_id);
			let active_len = file_ops.file_len(&active_path).await?.ok_or_else(|| {
				Error::new(
					ErrorKind::NotFound,
					format!(
						"active slab disappeared during recovery: {}",
						active_path.display()
					),
				)
			})?;
			let mut non_empty_sealed_ids = Vec::with_capacity(sealed_ids.len());
			for &id in sealed_ids {
				let path = slab_path(&config.dir, id);
				match file_ops.file_len(&path).await? {
					Some(0) => continue,
					Some(_) => non_empty_sealed_ids.push(id),
					None => {
						return Err(Error::new(
							ErrorKind::NotFound,
							format!("slab disappeared during recovery: {}", path.display()),
						));
					}
				}
			}
			let non_empty_count = non_empty_sealed_ids.len() + usize::from(active_len > 0);
			let open_count = non_empty_sealed_ids.len() + 1;
			if open_count > MAX_RECOVERY_OPEN_SLABS {
				return Err(Error::new(
					ErrorKind::Other,
					format!(
						"bounded slab recovery refused: total slab files={}, non-empty slab files={}, slabs to open={}, bound={}; preserve the active slab and use the documented recovery path",
						existing_ids.len(),
						non_empty_count,
						open_count,
						MAX_RECOVERY_OPEN_SLABS,
					),
				));
			}
			// Drain into key_index in ascending id order so later writes
			// shadow earlier ones.
			for &id in &non_empty_sealed_ids {
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
					active_path,
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
					for row in snapshot.record_rows {
						if let Some(table_object_id) = row.table_object_id {
							table_by_location
								.insert(row.location, (row.tenant_id, table_object_id));
							*table_references_by_slab
								.entry(row.location.slab_id)
								.or_default()
								.entry((row.tenant_id, table_object_id))
								.or_default() += 1;
						}
					}
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
						*secondary_index_table_references_by_slab
							.entry(row.key.source_slab_id)
							.or_default()
							.entry((row.key.tenant_id, row.key.table_object_id))
							.or_default() += 1;
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
			table_by_location,
			table_references_by_slab,
			secondary_index_table_references_by_slab,
			integrity_failures: DashMap::new(),
			quarantined_compaction_slabs: DashSet::new(),
			relocated_reads: DashMap::new(),
			persistent_index,
			retired_tiered_cleanup: DashMap::new(),
			secondary_index_segments,
			secondary_index_coverage,
			secondary_index_admission_gate: Arc::new(AsyncRwLock::new(())),
			secondary_index_declaration_cuts: DashSet::new(),
			slab_rewrite_claims: DashMap::new(),
			tiered_stats_cache: DashMap::new(),
		};
		manager
			.detect_missing_published_references_on_recovery()
			.await;
		let snapshot = manager.compaction_manifest_snapshot();
		refresh_lsm_level_metrics(&snapshot);
		if manager.integrity_failures.is_empty() {
			manager.rebuild_persistent_index().await;
		} else {
			tracing::error!(
				fenced_table_count = manager.integrity_failures.len(),
				"published reference integrity failure detected during recovery; preserving durable index evidence"
			);
		}
		Ok(manager)
	}

	/// Test / diagnostic accessor: current active slab id.
	pub async fn active_slab_id(&self) -> SlabId {
		self.state.lock().await.active.slab_id()
	}

	pub fn data_dir(&self) -> &Path {
		&self.config.dir
	}

	/// Durable typed-table attribution for a published record location.
	///
	/// This deliberately returns `None` for legacy/untyped rows. Callers that
	/// need an integrity fence must treat missing attribution as unknown, never
	/// infer a table from the irreversible row identity.
	pub(crate) fn published_table_object_id(
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
	) -> TableDropReferenceCounts {
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
		TableDropReferenceCounts {
			in_memory_rows,
			persistent_rows,
			indexed_slabs,
			secondary_indexed_slabs,
		}
	}

	fn add_published_table_reference(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		location: RecordLocation,
	) {
		if let Some(previous) = self
			.table_by_location
			.insert(location, (tenant_id, table_object_id))
		{
			if previous == (tenant_id, table_object_id) {
				return;
			}
			self.remove_published_table_reference_count(previous.0, previous.1, location.slab_id);
		}
		*self
			.table_references_by_slab
			.entry(location.slab_id)
			.or_default()
			.entry((tenant_id, table_object_id))
			.or_default() += 1;
	}

	fn remove_published_table_reference_count(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		slab_id: SlabId,
	) {
		let remove_slab =
			if let Some(mut references) = self.table_references_by_slab.get_mut(&slab_id) {
				let key = (tenant_id, table_object_id);
				if let Some(count) = references.get_mut(&key) {
					*count = count.saturating_sub(1);
					if *count == 0 {
						references.remove(&key);
					}
				}
				references.is_empty()
			} else {
				false
			};
		if remove_slab {
			self.table_references_by_slab.remove(&slab_id);
		}
	}

	fn remove_published_table_reference(&self, location: RecordLocation) {
		if let Some((_, (tenant_id, table_object_id))) = self.table_by_location.remove(&location) {
			self.remove_published_table_reference_count(
				tenant_id,
				table_object_id,
				location.slab_id,
			);
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
		let remove_slab = if let Some(mut references) = self
			.secondary_index_table_references_by_slab
			.get_mut(&source_slab_id)
		{
			let key = (tenant_id, table_object_id);
			if let Some(count) = references.get_mut(&key) {
				*count = count.saturating_sub(1);
				if *count == 0 {
					references.remove(&key);
				}
			}
			references.is_empty()
		} else {
			false
		};
		if remove_slab {
			self.secondary_index_table_references_by_slab
				.remove(&source_slab_id);
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
		if !dir.is_dir() {
			return Err(Error::new(
				ErrorKind::NotFound,
				format!("slab directory does not exist: {}", dir.display()),
			));
		}
		let file_ops: Arc<dyn AsyncFileOps> = Arc::new(IoUringFileOps::with_isolated_blocking(
			io,
			"wendelon-fs-meta-repair",
			BlockingIoClass::SlabMetadata,
		)?);
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
		descriptor: &TableSchemaDescriptor,
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
			.record_rows_for_slab(active_slab_id)
			.iter()
			.any(|row| row.tenant_id == tenant_id)
		{
			return Ok(true);
		}
		for slab_id in sealed_slab_ids {
			let tenant_rows = self
				.record_rows_for_slab(slab_id)
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

	async fn delete_l2_object_if_present(
		object_store: &dyn ObjectStore,
		uri: &str,
	) -> std::io::Result<()> {
		match object_store.delete(uri).await {
			Ok(()) => Ok(()),
			Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
			Err(error) => Err(error),
		}
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
		self.read_with_reference_origin(location, PublishedReferenceOrigin::Read)
			.await
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
		descriptor: &TableSchemaDescriptor,
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
		descriptor: &TableSchemaDescriptor,
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

	/// A local slab may be unlinked only after the durable index agrees that
	/// no published record or secondary-index source still names it. Runtime
	/// `key_index` alone is insufficient: it can already have advanced past a
	/// source while a failed mirror leaves recovery pointing at that source.
	///
	/// A mismatch preserves the slab, quarantines further retirement work for
	/// it, and leaves repair to an explicit future operator decision.
	async fn can_retire_local_slab(&self, slab_id: SlabId) -> std::io::Result<bool> {
		let Some(index) = &self.persistent_index else {
			return Ok(true);
		};
		let snapshot = index.load_snapshot().await?;
		let record_references = snapshot
			.record_rows
			.iter()
			.chain(snapshot.tiered_records.iter())
			.filter(|row| row.location.slab_id == slab_id)
			.count();
		let secondary_references = snapshot
			.secondary_index_segments
			.iter()
			.filter(|row| row.key.source_slab_id == slab_id)
			.count();
		if record_references == 0 && secondary_references == 0 {
			return Ok(true);
		}

		self.quarantined_compaction_slabs.insert(slab_id);
		tracing::error!(
			slab_id = slab_id.get(),
			record_references,
			secondary_references,
			"refusing to retire slab with durable published references"
		);
		Ok(false)
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
			table_object_id: None,
			// ADR 0090: persist the location's own placement, never a default.
			placement: location.placement,
			partition: location.partition,
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
				table_object_id: None,
				placement: PlacementSemantics::RawLocality,
				partition: None,
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
		relocation_tables: &HashMap<RecordLocation, TableObjectId>,
	) {
		let removed_rows = removed
			.iter()
			.map(|record| PersistentRecordRow {
				tenant_id: record.tenant_id,
				key_identity: record.key_identity,
				routing_locality: record.routing_locality,
				table_object_id: relocation_tables.get(&record.old_location).copied(),
				placement: PlacementSemantics::RawLocality,
				partition: None,
				location: record.old_location,
			})
			.collect::<Vec<_>>();
		let inserted_rows = inserted
			.iter()
			.map(|record| PersistentRecordRow {
				tenant_id: record.tenant_id,
				key_identity: record.key_identity,
				routing_locality: record.routing_locality,
				table_object_id: relocation_tables.get(&record.old_location).copied(),
				placement: PlacementSemantics::RawLocality,
				partition: None,
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

	pub(crate) async fn prepare_tiered_table_retention_rewrite(
		&self,
		pointer: &TieredSlabPointer,
		cluster_policy: MvccRetentionPolicy,
		table_lifecycle: &TableLifecycleCatalog,
		table_schemas: &TableSchemaCatalog,
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

	pub(crate) async fn prepare_tiered_table_drop_rewrite(
		&self,
		pointer: &TieredSlabPointer,
		tenant_id: TenantId,
		descriptor: &TableSchemaDescriptor,
		table_schemas: &TableSchemaCatalog,
	) -> std::io::Result<Option<PreparedTieredTableDropRewrite>> {
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
				reclaimed_records,
				reclaimed_bytes: old_blob.len() as u64,
			}));
		}

		let tmp_path = table_drop_rewrite_tmp_path(&self.config.dir, pointer.slab_id);
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
		Ok(Some(PreparedTieredTableDropRewrite::Rewrite {
			slab_id: pointer.slab_id,
			old_l2_uri: pointer.l2_uri.clone(),
			payload_hash: crate::hash::content::digest(&encrypted.bytes),
			tombstone_metadata: merge_row_tombstone_metadata(
				pointer.tombstone_metadata,
				row_tombstone_metadata,
			),
			supersession_metadata: self.slab_supersession_metadata(pointer.slab_id),
			reclaimed_bytes: (old_blob.len() as u64).saturating_sub(encrypted.bytes.len() as u64),
			reclaimed_records,
			encrypted_bytes: encrypted.bytes,
		}))
	}

	async fn reserve_slab_id(&self) -> SlabId {
		let mut state = self.state.lock().await;
		let next_id = state.next_slab_id;
		state.next_slab_id = state.next_slab_id.saturating_add(1);
		next_id
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
) -> std::io::Result<u64> {
	let mut entries_by_record: BTreeMap<(u64, u32), Vec<RecoveredSlabEntry>> = BTreeMap::new();
	for entry in recover_slab_entries_from_blob(blob, slab_id)? {
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
	// `PersistentLocalIndex::open` goes through `Database::create`, which would
	// happily conjure an empty index here. That would report every candidate as
	// unreferenced and delete them all — not because nothing referenced them,
	// but because nothing could be asked. An absent index is a refusal, and it
	// must be caught before the open that would paper over it.
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

#[cfg(test)]
mod tests;

/// ADR 0090 route-gate hook: install an arbitrary version so route tests can
/// construct placement states the write path cannot produce yet. The location
/// may point at nothing — route selection must decide before any slab byte is
/// touched, which is exactly what the tests prove.
#[cfg(test)]
impl SlabManager {
	pub(crate) fn install_version_for_route_test(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
		location: RecordLocation,
	) {
		self.key_index
			.entry((tenant_id, key_identity))
			.or_default()
			.push(location);
	}
}

/// ADR 0090: placement resolution helpers that live outside the storage-engine
/// trait surface.
impl SlabManager {
	/// ADR 0090: the placement authority for one row version. Durable carriers
	/// that predate placement — the V1 secondary-index segment format — decode
	/// their locations as raw-locality, so a reader holding such a location
	/// must resolve the version's true placement from the key chain rather
	/// than trusting the segment's reconstruction.
	pub(crate) fn version_placement_at(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
		hlc: HybridTimestamp,
	) -> Option<(PlacementSemantics, Option<crate::schema::PartitionId>)> {
		let versions = self.key_index.get(&(tenant_id, key_identity))?;
		versions
			.value()
			.iter()
			.find(|location| location.hlc == hlc)
			.map(|location| (location.placement, location.partition))
	}
}
