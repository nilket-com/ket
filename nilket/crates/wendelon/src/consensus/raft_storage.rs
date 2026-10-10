//! openraft storage-v2 traits for Wendelon's Control Ring.
//!
//! Three concrete types live here:
//!
//!  * [`WendelonLogStore`] — durable Raft log + vote persistence.
//!    Log entries are held in an in-memory `BTreeMap<u64, Entry<…>>`
//!    for fast serve and are simultaneously streamed to an
//!    `O_DIRECT`-opened WAL file through
//!    [`AsyncFileOps`][crate::storage::AsyncFileOps]. The production
//!    implementation delegates data-path writes to the shared
//!    [`IoRingHandle`][crate::storage::io_uring::IoRingHandle], while
//!    namespace operations go through the same seam via blocking file
//!    APIs. An `fdatasync` is awaited before the openraft
//!    `LogFlushed` callback fires, so the log-io promise and the
//!    on-disk tail cannot disagree. The vote rides a separate
//!    single-page file (`vote.wnd`) so it can be rewritten in place
//!    without disturbing the append-only WAL.
//!
//!  * [`WendelonStateMachine`] — applies committed entries into an
//!    `Arc<RwLock<SmState>>`. The SM tracks
//!    `last_applied_log_id`, `last_membership`, per-holder lease
//!    table, and a per-epoch E_safe watermark map. The `Arc` is
//!    shared into [`WendelonSnapshotBuilder`] so snapshots are
//!    cheap consistent views rather than a deep clone.
//!
//!  * [`WendelonSnapshotBuilder`] — streams [`SmState`] into a
//!    file-backed snapshot artifact prefixed with
//!    [`SnapshotHeader`], matching the plan in
//!    `plans/consensus/raft-core.md` §3 while keeping snapshot
//!    install bounded-memory.
//!
//! ## Wire format (WAL record)
//!
//! ```text
//! [ WALRecordHeader (32B) ]
//! [ RaftLogEntryFixed (96B) ]
//! [ postcard-encoded openraft Entry<WendelonTypeConfig> ]
//! [ 4-byte profile-hash-prefix checksum ]
//! [ zero pad to next DIRECT_IO_ALIGN boundary ]
//! ```
//!
//! The tail-pad is required because `O_DIRECT` rejects non
//! page-aligned writes; the `body_len` field on the
//! [`WALRecordHeader`] is the source of truth for recovery and
//! readers ignore the pad. Concurrency is handled by an
//! `Arc<RwLock<LogStoreInner>>` so `get_log_reader` can hand out
//! a Self-typed reader that shares the same cache as the writer.

use crate::audit::{
	AdministrativeControlAction, AdministrativeControlSchema, AuditAuthority, AuditEvent,
	AuditMaterializationTracker, AuditOperation, AuditOutcome, AuditSchema, AuditSink,
	DataAccessKind, DataAccessPhase, DataAccessSchema,
};
use crate::consensus::retention::{
	self, HistoryCheck, RetentionGuard, RetentionStartError, RetentionViolation,
};
use crate::consensus::{
	AdvanceEpochRecord, CreateTenant, EpochConfig, MigratedRangeTombstone, MigrationComplete,
	MigrationLedgerEntry, MigrationState, RevokeActorKey, RevokedActorKeyId, SeedMigration,
	SetTableRetentionPolicy, TableDropFinalizeRequest, TableDropLogicalFinalizeRequest,
	TableDropPrepareRequest, TableDropSettlementRequest, TableReclaimCompletionRequest,
	TenantFinalization, TenantLifecycleState, TenantRecord, TieredSlab, UpdateTenantQuota,
	WendelonRequest, WendelonResponse, WendelonTypeConfig,
};
use crate::crypto::{InstallTenantKekVersion, RetireTenantKekVersion, TenantKeyset};
use crate::integration::compute::{
	LOGICAL_TYPE_INTERNAL_NAMESPACE, LogicalTypeCatalog, SchemaMutationKind, SchemaObjectKind,
	SchemaUpdateEnvelope, decode_logical_type_definition_create, decode_schema_update_envelope_v1,
};
use crate::metrics::{MetricsRegistry, global as metrics};
use crate::network::tls::ActorTrustDomain;
#[cfg(test)]
use crate::schema::{
	ClientWriteId, CtasIntentFingerprint, CtasOperationId, CtasTerminalCause, SupersessionMetadata,
	TableDropParticipantSet, TombstoneMetadata,
};
use crate::schema::{
	CtasExpectedBinding, CtasSagaCatalog, CtasSagaEntry, CtasSagaMutation, CtasSagaPhase,
	CtasSagaReject, CtasSagaResponse, FinalizationMarker, HybridTimestamp, IndexObjectId, Locality,
	LogEntryKind, NodeId as SchemaNodeId, PRE_VERSIONING_FORMAT, RaftLogEntryFixed,
	RowIdentityPolicy, SCHEMA_UPDATE_REJECT_INDEX_ALTER_UNSUPPORTED, SNAPSHOT_FORMAT_VERSION,
	SecondaryIndexCatalog, SecondaryIndexDescriptor, SecondaryIndexState, SlabId, SnapshotHeader,
	TableDropFenceCatalog, TableDropOutcome, TableDropOutcomeCatalog, TableDropReclaimDebt,
	TableDropTombstone, TableLifecycleCatalog, TableRetentionPolicy, TableSchemaCatalog,
	TableSchemaDescriptor, TenantId, TenantKekId, WAL_FORMAT_VERSION, WALRecordHeader,
	decode_secondary_index_descriptor, decode_secondary_index_drop_request,
	decode_secondary_index_lifecycle_complete, decode_table_schema_descriptor_v1,
	validate_table_schema_evolution,
};
use crate::storage::io_uring::{AlignedBuf, DIRECT_IO_ALIGN, IoRingHandle};
use crate::storage::mutation_drain::MutationDrainRegistry;
use crate::storage::{
	AsyncFileOps, BlockingFileOps, CompactionCommit, FileHandle, IoUringFileOps, OpenFlags,
	fsync_parent_dir, is_cdc_locality_heartbeat_finalization, write_file,
};
use bytemuck::{Pod, Zeroable};
#[cfg(test)]
use openraft::CommittedLeaderId;
use openraft::anyerror::AnyError;
use openraft::storage::{
	LogFlushed, LogState, RaftLogReader, RaftLogStorage, RaftSnapshotBuilder, RaftStateMachine,
	Snapshot,
};
use openraft::{
	BasicNode, Entry, EntryPayload, LogId, OptionalSend, SnapshotMeta, StorageError,
	StorageIOError, StoredMembership, Vote,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt::Debug;
use std::future::Future;
use std::io::{Error, ErrorKind, Read, Seek, SeekFrom, Write};
use std::ops::{Bound, RangeBounds};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncSeek, AsyncWrite, ReadBuf};
use tokio::sync::{Mutex, RwLock, broadcast, watch};
use tokio::task::JoinHandle;
use tokio::time::{Instant, MissedTickBehavior};

#[cfg(not(feature = "fips"))]
use crate::hash::blake3 as control_artifact_hash;
#[cfg(feature = "fips")]
use crate::hash::sha256 as control_artifact_hash;

/// WAL `record_type` discriminants — mirrored from
/// `plans/consensus/raft-core.md` §4.
const REC_LOG_ENTRY: u8 = 0;
#[allow(dead_code)]
const REC_SNAPSHOT_MARKER: u8 = 1;
#[allow(dead_code)]
const REC_SNAPSHOT_INSTALL: u8 = 2;
#[allow(dead_code)]
const REC_CHECKPOINT: u8 = 3;
const FINALIZATION_BROADCAST_CAPACITY: usize = 1024;
const CONTROL_RING_BACKUP_FORMAT_VERSION: u16 = 1;
const DEFAULT_WAL_SEGMENT_BYTES: u64 = 128 * 1024 * 1024;
const WAL_SEGMENT_SUFFIX: &str = ".seg";
const WAL_SEGMENT_STAGING_SUFFIX: &str = ".staging";
const CHECKPOINT_ENTRY_THRESHOLD: u64 = 1_000;
const CHECKPOINT_TIME_THRESHOLD: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RecoveryCheckpoint {
	pub commit_index: u64,
	pub e_safe_index: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DurableBootstrapMode {
	FreshBootstrap,
	Recovery,
}

impl DurableBootstrapMode {
	fn from_artifacts(found: bool) -> Self {
		if found {
			Self::Recovery
		} else {
			Self::FreshBootstrap
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlRingBackupManifest {
	pub format_version: u16,
	pub last_applied_log_id: Option<LogId<u32>>,
	pub voter_ids: Vec<u32>,
	pub lease_count: u64,
	pub tiered_slab_count: u64,
	pub migration_count: u64,
	pub wal_bytes: u64,
	pub vote_bytes: u64,
	pub snapshot_bytes: u64,
	pub state_hash: [u8; 32],
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlRingBackupImage {
	pub manifest: ControlRingBackupManifest,
	#[serde(with = "serde_bytes")]
	pub wal: Vec<u8>,
	#[serde(with = "serde_bytes")]
	pub vote: Vec<u8>,
	#[serde(with = "serde_bytes")]
	pub snapshot: Vec<u8>,
}

#[derive(Clone, Debug)]
struct ControlArtifactPaths {
	/// Base path used as the legacy monolithic WAL file location
	/// and as the filename prefix for segmented WAL artifacts.
	wal: PathBuf,
	vote: PathBuf,
	snapshot: PathBuf,
}

#[derive(Clone, Copy, Debug)]
struct WalOptions {
	segment_bytes: u64,
}

impl Default for WalOptions {
	fn default() -> Self {
		Self {
			segment_bytes: DEFAULT_WAL_SEGMENT_BYTES,
		}
	}
}

impl WalOptions {
	fn validate(self) -> std::io::Result<Self> {
		if self.segment_bytes < DIRECT_IO_ALIGN as u64 {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				format!(
					"wal segment size {} is smaller than O_DIRECT alignment {}",
					self.segment_bytes, DIRECT_IO_ALIGN
				),
			));
		}
		if self.segment_bytes % DIRECT_IO_ALIGN as u64 != 0 {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				format!(
					"wal segment size {} must be a multiple of {}",
					self.segment_bytes, DIRECT_IO_ALIGN
				),
			));
		}
		Ok(self)
	}
}

#[derive(Clone, Copy, Debug, Pod, Zeroable)]
#[repr(C)]
struct WALCheckpointBody {
	commit_index: u64,
	e_safe_index: u64,
}

#[derive(Clone, Debug)]
struct LoadedSnapshot {
	held: HeldSnapshot,
	last_included: Option<LogId<u32>>,
	state: SmState,
}

#[derive(Clone, Debug, Default)]
struct ArtifactPresence {
	wal: bool,
	vote: bool,
	snapshot: bool,
}

impl ArtifactPresence {
	fn any(&self) -> bool {
		self.wal || self.vote || self.snapshot
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct WalSegmentMeta {
	path: PathBuf,
	start_index: u64,
	end_log_index: Option<u64>,
	len_bytes: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct WalRecordPointer {
	segment_start_index: u64,
	offset: u64,
	len_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct DiscoveredWalSegment {
	path: PathBuf,
	start_index: u64,
	legacy: bool,
}

#[derive(Clone)]
struct RaftAuditMaterializer {
	local_node: SchemaNodeId,
	sink: Arc<dyn AuditSink>,
	tracker: Arc<AuditMaterializationTracker>,
}

impl RaftAuditMaterializer {
	fn emit(&self, event: AuditEvent) -> bool {
		// Raft-authoritative audit evidence lives in the committed log.
		// Emission to the local NDJSON chain is a required materialization
		// step for serious deployments, but a materialization failure does
		// not erase the already-committed authoritative record.
		if let Err(err) = self.sink.emit(event.clone()) {
			self.tracker.note_materialization_failed();
			tracing::warn!(
				event_type = %event.operation.as_str(),
				error = %err,
				"raft audit materialization failed; authoritative record remains in the committed log"
			);
			return false;
		}
		self.tracker.note_materialized();
		true
	}
}

#[derive(Clone)]
struct PendingWalWrite {
	entry: Entry<WendelonTypeConfig>,
	pointer: WalRecordPointer,
}

#[derive(Default)]
struct WALReplay {
	entries: BTreeMap<u64, Entry<WendelonTypeConfig>>,
	entry_positions: BTreeMap<u64, WalRecordPointer>,
	last_log_id: Option<LogId<u32>>,
	last_purged: Option<LogId<u32>>,
	checkpoint: Option<RecoveryCheckpoint>,
	wal_segments: Vec<WalSegmentMeta>,
	last_segment_needs_truncate: bool,
}

// ============================================================
//  LOG STORE
// ============================================================

/// Shared mutable guts of [`WendelonLogStore`]. Held behind an
/// `Arc<RwLock<_>>` so `get_log_reader` can clone the store handle
/// cheaply and keep the reader in sync with the writer.
struct LogStoreInner {
	/// In-memory cache of every log entry the SM might still ask
	/// for (index > `last_purged.index`). Populated on append and
	/// on WAL replay at open time.
	entries: BTreeMap<u64, Entry<WendelonTypeConfig>>,
	/// Highest log id durably present in the WAL. `None` on a
	/// fresh store.
	last_log_id: Option<LogId<u32>>,
	/// Highest log id compacted away by `purge`; never decreases.
	last_purged: Option<LogId<u32>>,
	/// Cached vote. Written to `vote_file` on `save_vote`; surfaced
	/// verbatim by `read_vote`.
	vote: Option<Vote<u32>>,
	/// Filesystem paths for the durable Control Ring artifacts.
	/// Absent for the in-memory test store.
	artifact_paths: Option<ControlArtifactPaths>,
	/// Snapshot loaded during durable recovery, if present.
	recovered_snapshot: Option<LoadedSnapshot>,
	/// Recovered checkpoint counters from the WAL.
	recovery_checkpoint: Option<RecoveryCheckpoint>,
	/// Durable committed pointer recovered from the WAL checkpoint
	/// stream. When absent, openraft falls back to `last_applied`
	/// and re-establishes commit on fresh leader traffic.
	recovery_committed: Option<LogId<u32>>,
	/// Whether startup was a true fresh bootstrap or durable recovery.
	bootstrap_mode: DurableBootstrapMode,
	/// File I/O seam over io_uring and namespace operations. `None`
	/// in the in-memory test mode.
	file_ops: Option<Arc<dyn AsyncFileOps>>,
	/// Ordered WAL segments, with the last element being the active
	/// writable segment when durable WAL backing is enabled.
	wal_segments: Vec<WalSegmentMeta>,
	/// Durable handle for the active WAL segment, if any.
	wal_file: Option<FileHandle>,
	/// Next byte offset in the active WAL segment. Page-aligned at
	/// all times.
	wal_cursor: u64,
	/// Precise on-disk position for every retained log entry.
	entry_positions: BTreeMap<u64, WalRecordPointer>,
	/// Fixed-size segment target for newly created WAL segments.
	wal_segment_bytes: u64,
	/// Durable vote artifact, if this is not the in-memory test store.
	vote_file: Option<FileHandle>,
}

/// Durable log + vote for the Control Ring.
///
/// Cloneable and shareable — all real state sits behind
/// `Arc<RwLock<LogStoreInner>>`, so `get_log_reader` just hands
/// out another handle pointing at the same cache.
#[derive(Clone)]
pub struct WendelonLogStore {
	inner: Arc<RwLock<LogStoreInner>>,
	/// Serialises WAL submissions so append bodies and their
	/// fsync barriers land in the intended order. `io_uring` CQE
	/// reordering is real for writes vs. fsyncs (see
	/// [`crate::storage::io_uring::IoOp::Fsync`]), and holding
	/// this across an append keeps the "await write → await
	/// fsync → fire callback" sequence coherent.
	wal_gate: Arc<Mutex<()>>,
	/// The node's Control retention guard; `None` is `Compacting`.
	retention: Option<Arc<RetentionGuard>>,
}

impl WendelonLogStore {
	/// In-memory store with no WAL backing. Intended for
	/// openraft unit tests and intentionally ephemeral harnesses.
	/// Live node bootstrap must use [`Self::open`] instead.
	pub fn in_memory() -> Self {
		Self {
			inner: Arc::new(RwLock::new(LogStoreInner {
				entries: BTreeMap::new(),
				last_log_id: None,
				last_purged: None,
				vote: None,
				artifact_paths: None,
				recovered_snapshot: None,
				recovery_checkpoint: None,
				recovery_committed: None,
				bootstrap_mode: DurableBootstrapMode::FreshBootstrap,
				file_ops: None,
				wal_segments: Vec::new(),
				wal_file: None,
				wal_cursor: 0,
				entry_positions: BTreeMap::new(),
				wal_segment_bytes: DEFAULT_WAL_SEGMENT_BYTES,
				vote_file: None,
			})),
			wal_gate: Arc::new(Mutex::new(())),
			retention: None,
		}
	}

	/// Preserved for callers that still use `::new()` — behaves
	/// identically to [`Self::in_memory`] until the WAL-backed
	/// constructor is wired at bootstrap time.
	pub fn new() -> Self {
		Self::in_memory()
	}

	/// Open (or create) the WAL and vote files, load any snapshot
	/// artifact, replay the WAL tail, and bind the resulting durable
	/// handles to the production [`AsyncFileOps`] implementation.
	pub async fn open(
		wal_path: impl AsRef<Path>,
		vote_path: impl AsRef<Path>,
		snapshot_path: impl AsRef<Path>,
		io: IoRingHandle,
	) -> std::io::Result<Self> {
		let file_ops = Arc::new(IoUringFileOps::with_isolated_blocking(
			io,
			"wendelon-fs-meta-wal",
			crate::storage::BlockingIoClass::WalMetadata,
		)?);
		Self::open_with_file_ops_and_options(
			wal_path,
			vote_path,
			snapshot_path,
			file_ops,
			WalOptions::default(),
		)
		.await
	}

	pub async fn open_with_file_ops(
		wal_path: impl AsRef<Path>,
		vote_path: impl AsRef<Path>,
		snapshot_path: impl AsRef<Path>,
		file_ops: Arc<dyn AsyncFileOps>,
	) -> std::io::Result<Self> {
		Self::open_with_file_ops_and_options(
			wal_path,
			vote_path,
			snapshot_path,
			file_ops,
			WalOptions::default(),
		)
		.await
	}

	/// [`Self::open`] under the node's retention guard. Under full retention
	/// the store refuses to open over a history that is not complete from
	/// index 0, before any byte of the data directory is modified.
	pub async fn open_with_retention(
		wal_path: impl AsRef<Path>,
		vote_path: impl AsRef<Path>,
		snapshot_path: impl AsRef<Path>,
		io: IoRingHandle,
		retention: Arc<RetentionGuard>,
	) -> std::io::Result<Self> {
		let file_ops = Arc::new(IoUringFileOps::with_isolated_blocking(
			io,
			"wendelon-fs-meta-wal",
			crate::storage::BlockingIoClass::WalMetadata,
		)?);
		Self::open_with_options_and_retention(
			wal_path,
			vote_path,
			snapshot_path,
			file_ops,
			WalOptions::default(),
			Some(retention),
		)
		.await
	}

	async fn open_with_file_ops_and_options(
		wal_path: impl AsRef<Path>,
		vote_path: impl AsRef<Path>,
		snapshot_path: impl AsRef<Path>,
		file_ops: Arc<dyn AsyncFileOps>,
		options: WalOptions,
	) -> std::io::Result<Self> {
		Self::open_with_options_and_retention(
			wal_path,
			vote_path,
			snapshot_path,
			file_ops,
			options,
			None,
		)
		.await
	}

	async fn open_with_options_and_retention(
		wal_path: impl AsRef<Path>,
		vote_path: impl AsRef<Path>,
		snapshot_path: impl AsRef<Path>,
		file_ops: Arc<dyn AsyncFileOps>,
		options: WalOptions,
		retention: Option<Arc<RetentionGuard>>,
	) -> std::io::Result<Self> {
		let options = options.validate()?;
		let wal_path = wal_path.as_ref();
		let vote_path = vote_path.as_ref();
		let snapshot_path = snapshot_path.as_ref();
		let artifact_paths = ControlArtifactPaths {
			wal: wal_path.to_path_buf(),
			vote: vote_path.to_path_buf(),
			snapshot: snapshot_path.to_path_buf(),
		};

		let artifacts =
			inspect_artifacts(file_ops.as_ref(), wal_path, vote_path, snapshot_path).await?;
		let bootstrap_mode = DurableBootstrapMode::from_artifacts(artifacts.any());
		let full = retention
			.as_ref()
			.is_some_and(|guard| guard.mode() == crate::consensus::ControlRetention::Full);
		if full {
			// Phase A: read-only, before replay, so these refusals are typed
			// even where replay would also have refused.
			let receive_tmp = SnapshotPaths::from_current(snapshot_path.to_path_buf()).receive_tmp;
			let segments = discover_all_wal_segments(file_ops.as_ref(), wal_path).await?;
			retention::check_before_replay(
				file_ops.file_len(snapshot_path).await?.is_some(),
				file_ops.file_len(&receive_tmp).await?.is_some(),
				segments
					.iter()
					.map(|segment| (segment.start_index, segment.legacy)),
			)
			.map_err(RetentionStartError::into_io)?;
		}
		let vote = load_vote_state(file_ops.as_ref(), vote_path).await?;
		let snapshot = load_snapshot_artifact(file_ops.as_ref(), snapshot_path).await?;
		let replay = replay_wal(file_ops.as_ref(), wal_path, snapshot.as_ref()).await?;
		let recovery_committed =
			resolve_recovered_committed(snapshot.as_ref(), &replay.entries, replay.checkpoint)?;
		let history = if full {
			// Phase B: read-only, after replay and the committed-pointer
			// resolution, and before the first mutation below.
			retention::check_after_replay(
				artifacts.any(),
				replay.last_purged.is_some(),
				replay.entries.keys().copied(),
				replay
					.checkpoint
					.map_or(0, |checkpoint| checkpoint.commit_index),
			)
			.map_err(RetentionStartError::into_io)?
		} else {
			HistoryCheck::NotChecked
		};
		if let Some(guard) = &retention {
			guard.record_history(history);
		}
		let wal_file = if replay.last_segment_needs_truncate {
			let last = replay.wal_segments.last().ok_or_else(|| {
				Error::new(
					ErrorKind::InvalidData,
					format!(
						"wal {} requested truncation without a discovered segment",
						wal_path.display()
					),
				)
			})?;
			let handle = open_existing_wal_segment(file_ops.as_ref(), &last.path).await?;
			file_ops.set_len(&handle, last.len_bytes).await?;
			file_ops.fsync(&handle).await?;
			Some(handle)
		} else if let Some(last) = replay.wal_segments.last() {
			Some(open_existing_wal_segment(file_ops.as_ref(), &last.path).await?)
		} else {
			None
		};
		let vote_file = open_control_artifact(file_ops.as_ref(), vote_path).await?;
		let wal_cursor = replay
			.wal_segments
			.last()
			.map(|segment| segment.len_bytes)
			.unwrap_or(0);

		Ok(Self {
			inner: Arc::new(RwLock::new(LogStoreInner {
				entries: replay.entries,
				entry_positions: replay.entry_positions,
				last_log_id: replay.last_log_id,
				last_purged: replay.last_purged,
				vote,
				artifact_paths: Some(artifact_paths),
				recovered_snapshot: snapshot,
				recovery_checkpoint: replay.checkpoint,
				recovery_committed,
				bootstrap_mode,
				file_ops: Some(file_ops),
				wal_segments: replay.wal_segments,
				wal_file,
				wal_cursor,
				wal_segment_bytes: options.segment_bytes,
				vote_file: Some(vote_file),
			})),
			wal_gate: Arc::new(Mutex::new(())),
			retention,
		})
	}

	/// Test seam for the retention suite: a store over `dir` with blocking
	/// file I/O and a small segment size, under `retention`.
	#[cfg(test)]
	pub(crate) async fn open_for_retention_test(
		dir: &Path,
		segment_bytes: u64,
		retention: Option<Arc<RetentionGuard>>,
	) -> std::io::Result<Self> {
		Self::open_with_options_and_retention(
			dir.join("control.wal"),
			dir.join("vote.wnd"),
			dir.join("snapshot.wnd"),
			Arc::new(crate::storage::BlockingFileOps),
			WalOptions { segment_bytes },
			retention,
		)
		.await
	}

	/// The file name a WAL segment starting at `start_index` has in `dir`.
	#[cfg(test)]
	pub(crate) fn segment_path_for_retention_test(dir: &Path, start_index: u64) -> PathBuf {
		wal_segment_path(&dir.join("control.wal"), start_index).unwrap()
	}

	pub async fn bootstrap_mode(&self) -> DurableBootstrapMode {
		self.inner.read().await.bootstrap_mode
	}

	pub async fn recovery_checkpoint(&self) -> Option<RecoveryCheckpoint> {
		self.inner.read().await.recovery_checkpoint
	}

	async fn write_recovery_checkpoint(
		&self,
		observation: CheckpointObservation,
	) -> std::io::Result<()> {
		let Some(last_log_id) = observation.last_log_id else {
			return Ok(());
		};
		let _gate = self.wal_gate.lock().await;
		let (
			file_ops,
			artifact_paths,
			mut wal_segments,
			mut wal_file,
			mut wal_cursor,
			segment_bytes,
			current_checkpoint,
		) = {
			let guard = self.inner.read().await;
			(
				guard.file_ops.clone(),
				guard.artifact_paths.clone(),
				guard.wal_segments.clone(),
				guard.wal_file.clone(),
				guard.wal_cursor,
				guard.wal_segment_bytes,
				guard.recovery_checkpoint,
			)
		};
		let current_checkpoint = current_checkpoint.unwrap_or_default();
		let next_checkpoint = RecoveryCheckpoint {
			commit_index: observation
				.checkpoint
				.commit_index
				.max(current_checkpoint.commit_index),
			e_safe_index: if observation.checkpoint.commit_index > current_checkpoint.commit_index {
				observation.checkpoint.e_safe_index
			} else {
				observation
					.checkpoint
					.e_safe_index
					.max(current_checkpoint.e_safe_index)
			},
		};
		if next_checkpoint == current_checkpoint {
			let mut guard = self.inner.write().await;
			guard.recovery_committed = Some(last_log_id);
			return Ok(());
		}
		let page = serialize_checkpoint_page(
			next_checkpoint.commit_index,
			next_checkpoint.e_safe_index,
			last_log_id.index,
			last_log_id.leader_id.term,
		);
		let file_ops = file_ops.ok_or_else(|| {
			Error::new(
				ErrorKind::Unsupported,
				"cannot write a recovery checkpoint without durable WAL backing",
			)
		})?;
		let artifact_paths = artifact_paths.ok_or_else(|| {
			Error::new(
				ErrorKind::Unsupported,
				"cannot write a recovery checkpoint without WAL artifact paths",
			)
		})?;
		if page.len() as u64 > segment_bytes {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				format!(
					"checkpoint record has {} bytes, exceeding segment size {}",
					page.len(),
					segment_bytes
				),
			));
		}
		if wal_file.is_none() || wal_cursor + page.len() as u64 > segment_bytes {
			let active_is_checkpoint_only = wal_segments
				.last()
				.map(|segment| segment.end_log_index.is_none())
				.unwrap_or(false);
			if active_is_checkpoint_only {
				let active_handle = wal_file
					.clone()
					.expect("checkpoint-only segment must have an active handle");
				file_ops.set_len(&active_handle, 0).await?;
				wal_cursor = 0;
				if let Some(active_segment) = wal_segments.last_mut() {
					active_segment.len_bytes = 0;
				}
			} else {
				let next_log_index = last_log_id.index.saturating_add(1);
				let (segment_path, segment_handle) =
					create_wal_segment(file_ops.as_ref(), &artifact_paths.wal, next_log_index)
						.await?;
				wal_segments.push(WalSegmentMeta {
					path: segment_path,
					start_index: next_log_index,
					end_log_index: None,
					len_bytes: 0,
				});
				wal_file = Some(segment_handle);
				wal_cursor = 0;
			}
		}
		let active_handle = wal_file.clone().expect("active wal segment handle");
		let active_segment = wal_segments
			.last_mut()
			.expect("active wal segment metadata");
		let expected_len = page.len();
		let (written, _page) = file_ops.write_at(&active_handle, wal_cursor, page).await?;
		if written != expected_len {
			return Err(Error::new(
				ErrorKind::WriteZero,
				format!("short checkpoint write: expected {expected_len} bytes, got {written}"),
			));
		}
		wal_cursor += expected_len as u64;
		active_segment.len_bytes = wal_cursor;
		file_ops.fdatasync(&active_handle).await?;

		let mut guard = self.inner.write().await;
		guard.recovery_checkpoint = Some(next_checkpoint);
		guard.recovery_committed = Some(last_log_id);
		guard.wal_segments = wal_segments;
		guard.wal_file = wal_file;
		guard.wal_cursor = wal_cursor;
		Ok(())
	}

	pub async fn recovered_state_machine(&self) -> std::io::Result<WendelonStateMachine> {
		let (snapshot, entries, checkpoint, snapshot_runtime) = {
			let guard = self.inner.read().await;
			let snapshot_runtime = match (&guard.file_ops, &guard.artifact_paths) {
				(Some(file_ops), Some(paths)) => SnapshotRuntime::new(
					file_ops.clone(),
					SnapshotPaths::from_current(paths.snapshot.clone()),
				),
				_ => SnapshotRuntime::ephemeral(),
			};
			(
				guard.recovered_snapshot.clone(),
				guard.entries.values().map(clone_entry).collect::<Vec<_>>(),
				guard.recovery_checkpoint,
				snapshot_runtime,
			)
		};
		let state_machine =
			WendelonStateMachine::from_recovery(snapshot, entries, checkpoint, snapshot_runtime)?;
		Ok(match &self.retention {
			Some(guard) => state_machine.with_retention_guard(guard.clone()),
			None => state_machine,
		})
	}

	pub async fn export_backup(
		&self,
		state_machine: &WendelonStateMachine,
	) -> std::io::Result<ControlRingBackupImage> {
		let (artifact_paths, file_ops, wal_segments) = {
			let guard = self.inner.read().await;
			(
				guard.artifact_paths.clone(),
				guard.file_ops.clone(),
				guard.wal_segments.clone(),
			)
		};
		let artifact_paths = artifact_paths.ok_or_else(|| {
			Error::new(
				ErrorKind::Unsupported,
				"cannot export a backup from the in-memory Control Ring store",
			)
		})?;
		let file_ops = file_ops.ok_or_else(|| {
			Error::new(
				ErrorKind::Unsupported,
				"cannot export a backup without file I/O backing",
			)
		})?;
		let wal =
			read_wal_backup_bytes(file_ops.as_ref(), &wal_segments, &artifact_paths.wal).await?;
		let vote = read_optional_artifact(file_ops.as_ref(), &artifact_paths.vote).await?;
		let snapshot = read_optional_artifact(file_ops.as_ref(), &artifact_paths.snapshot).await?;
		if wal.is_empty() && snapshot.is_empty() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"backup requires at least one Control Ring recovery artifact",
			));
		}

		let state = state_machine.state_snapshot().await;
		let encoded_state = encode_snapshot(&state)
			.map_err(|err| Error::other(format!("encode backup state snapshot: {err}")))?;
		let mut voter_ids: Vec<u32> = state.last_membership.voter_ids().collect();
		voter_ids.sort_unstable();

		Ok(ControlRingBackupImage {
			manifest: ControlRingBackupManifest {
				format_version: CONTROL_RING_BACKUP_FORMAT_VERSION,
				last_applied_log_id: state.last_applied_log_id,
				voter_ids,
				lease_count: state.leases.len() as u64,
				tiered_slab_count: state.tiered_slabs.len() as u64,
				migration_count: state.migrations.len() as u64,
				wal_bytes: wal.len() as u64,
				vote_bytes: vote.len() as u64,
				snapshot_bytes: snapshot.len() as u64,
				state_hash: control_artifact_digest_32(&encoded_state),
			},
			wal,
			vote,
			snapshot,
		})
	}

	pub async fn restore_backup(
		backup: &ControlRingBackupImage,
		target_dir: impl AsRef<Path>,
	) -> std::io::Result<()> {
		let target_dir = target_dir.as_ref();
		if !target_dir.is_dir() {
			return Err(Error::new(
				ErrorKind::NotFound,
				format!(
					"Control Ring restore target directory does not exist: {}",
					target_dir.display()
				),
			));
		}
		if backup.wal.is_empty() && backup.snapshot.is_empty() {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"backup image does not contain any Control Ring recovery artifact",
			));
		}

		let (wal_path, vote_path, snapshot_path) = control_artifact_paths(target_dir);
		let file_ops: Arc<dyn AsyncFileOps> = Arc::new(BlockingFileOps);
		ensure_wal_artifacts_absent(file_ops.as_ref(), &wal_path).await?;
		ensure_artifact_absent(&vote_path)?;
		ensure_artifact_absent(&snapshot_path)?;

		write_file(
			file_ops.as_ref(),
			&wal_path,
			OpenFlags {
				write: true,
				create_new: true,
				..Default::default()
			},
			&backup.wal,
		)
		.await?;
		fsync_parent_dir(file_ops.as_ref(), &wal_path).await?;
		write_file(
			file_ops.as_ref(),
			&vote_path,
			OpenFlags {
				write: true,
				create_new: true,
				..Default::default()
			},
			&backup.vote,
		)
		.await?;
		fsync_parent_dir(file_ops.as_ref(), &vote_path).await?;
		if !backup.snapshot.is_empty() {
			write_file(
				file_ops.as_ref(),
				&snapshot_path,
				OpenFlags {
					write: true,
					create_new: true,
					..Default::default()
				},
				&backup.snapshot,
			)
			.await?;
			fsync_parent_dir(file_ops.as_ref(), &snapshot_path).await?;
		}
		Ok(())
	}

	pub async fn finalization_markers_after(
		&self,
		start_log_index: u64,
		end_log_index: u64,
		epoch: u64,
	) -> std::io::Result<Vec<FinalizationMarker>> {
		if start_log_index >= end_log_index {
			return Ok(Vec::new());
		}

		let guard = self.inner.read().await;
		if let Some(last_purged) = guard.last_purged {
			if start_log_index < last_purged.index {
				return Err(Error::new(
					ErrorKind::InvalidInput,
					format!(
						"requested finalization floor {} predates retained log prefix {}",
						start_log_index, last_purged.index
					),
				));
			}
		}

		let markers = guard
			.entries
			.range((start_log_index.saturating_add(1))..=end_log_index)
			.filter_map(|(_, entry)| canonical_finalization_marker(entry))
			.filter(|marker| marker.epoch == epoch)
			.collect();
		Ok(markers)
	}

	pub async fn find_applied_finalization(
		&self,
		expected: &TenantFinalization,
		applied_upto: u64,
	) -> std::io::Result<Option<FinalizationMarker>> {
		if applied_upto == 0 {
			return Ok(None);
		}

		let guard = self.inner.read().await;
		let marker = guard
			.entries
			.range(..=applied_upto)
			.rev()
			.find_map(|(_, entry)| match &entry.payload {
				EntryPayload::Normal(WendelonRequest::Finalize(finalization))
					if finalization_matches(finalization, expected) =>
				{
					canonical_finalization_marker(entry)
				}
				_ => None,
			});
		Ok(marker)
	}
}

impl Default for WendelonLogStore {
	fn default() -> Self {
		Self::in_memory()
	}
}

impl RaftLogReader<WendelonTypeConfig> for WendelonLogStore {
	async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
		&mut self,
		range: RB,
	) -> Result<Vec<Entry<WendelonTypeConfig>>, StorageError<u32>> {
		let guard = self.inner.read().await;
		let out: Vec<Entry<WendelonTypeConfig>> = guard
			.entries
			.range((
				clone_bound(range.start_bound()),
				clone_bound(range.end_bound()),
			))
			.map(|(_, e)| clone_entry(e))
			.collect();
		Ok(out)
	}
}

impl RaftLogStorage<WendelonTypeConfig> for WendelonLogStore {
	type LogReader = Self;

	async fn get_log_state(&mut self) -> Result<LogState<WendelonTypeConfig>, StorageError<u32>> {
		let guard = self.inner.read().await;
		Ok(LogState {
			last_purged_log_id: guard.last_purged,
			last_log_id: guard.last_log_id.or(guard.last_purged),
		})
	}

	async fn get_log_reader(&mut self) -> Self::LogReader {
		self.clone()
	}

	async fn save_vote(&mut self, vote: &Vote<u32>) -> Result<(), StorageError<u32>> {
		let (file_ops, vote_file) = {
			let mut guard = self.inner.write().await;
			guard.vote = Some(*vote);
			(guard.file_ops.clone(), guard.vote_file.clone())
		};

		let (Some(file_ops), Some(vote_file)) = (file_ops, vote_file) else {
			// In-memory mode — caller accepted non-durable vote.
			return Ok(());
		};

		let mut buf = AlignedBuf::new(DIRECT_IO_ALIGN);
		let encoded_len = postcard::experimental::serialized_size(vote).map_err(|e| {
			StorageError::<u32>::from(StorageIOError::write_vote(AnyError::new(&e)))
		})?;
		if encoded_len + 4 > DIRECT_IO_ALIGN {
			return Err(StorageIOError::write_vote(AnyError::error(
				"encoded vote exceeds single O_DIRECT page",
			))
			.into());
		}
		buf.as_mut_slice()[..4].copy_from_slice(&(encoded_len as u32).to_le_bytes());
		let encoded = postcard::to_slice(vote, &mut buf.as_mut_slice()[4..4 + encoded_len])
			.map_err(|e| {
				StorageError::<u32>::from(StorageIOError::write_vote(AnyError::new(&e)))
			})?;
		if encoded.len() != encoded_len {
			return Err(StorageIOError::write_vote(AnyError::error(format!(
				"postcard serialized_size predicted {encoded_len} vote bytes but wrote {}",
				encoded.len()
			)))
			.into());
		}

		let (written, _buf) = file_ops
			.write_at(&vote_file, 0, buf)
			.await
			.map_err(|e| StorageIOError::write_vote(AnyError::new(&e)))?;
		if written != DIRECT_IO_ALIGN {
			return Err(StorageIOError::write_vote(AnyError::error(format!(
				"short vote write: expected {} bytes, got {}",
				DIRECT_IO_ALIGN, written
			)))
			.into());
		}

		file_ops
			.fdatasync(&vote_file)
			.await
			.map_err(|e| StorageIOError::write_vote(AnyError::new(&e)))?;
		Ok(())
	}

	async fn read_vote(&mut self) -> Result<Option<Vote<u32>>, StorageError<u32>> {
		Ok(self.inner.read().await.vote)
	}

	async fn save_committed(
		&mut self,
		committed: Option<LogId<u32>>,
	) -> Result<(), StorageError<u32>> {
		let Some(committed) = committed else {
			return Ok(());
		};

		let current = {
			let guard = self.inner.read().await;
			guard.recovery_checkpoint
		};
		let e_safe_index = current
			.map(|checkpoint| checkpoint.e_safe_index.min(committed.index))
			.unwrap_or(0);
		self.write_recovery_checkpoint(CheckpointObservation {
			last_log_id: Some(committed),
			checkpoint: RecoveryCheckpoint {
				commit_index: committed.index,
				e_safe_index,
			},
			force: false,
		})
		.await
		.map_err(|err| StorageIOError::write_logs(AnyError::new(&err)))?;
		Ok(())
	}

	async fn read_committed(&mut self) -> Result<Option<LogId<u32>>, StorageError<u32>> {
		Ok(self.inner.read().await.recovery_committed)
	}

	async fn append<I>(
		&mut self,
		entries: I,
		callback: LogFlushed<WendelonTypeConfig>,
	) -> Result<(), StorageError<u32>>
	where
		I: IntoIterator<Item = Entry<WendelonTypeConfig>> + OptionalSend,
		I::IntoIter: OptionalSend,
	{
		let batch: Vec<Entry<WendelonTypeConfig>> = entries.into_iter().collect();
		if batch.is_empty() {
			callback.log_io_completed(Ok(()));
			return Ok(());
		}

		// Serialize everything up front so a bad entry fails
		// without mutating either the cache or the WAL cursor.
		let mut pages: Vec<AlignedBuf> = Vec::with_capacity(batch.len());
		for entry in &batch {
			pages.push(serialize_entry_page(entry).map_err(StorageIOError::write_logs)?);
		}

		let _gate = self.wal_gate.lock().await;
		let (
			file_ops,
			artifact_paths,
			mut wal_segments,
			mut wal_file,
			mut wal_cursor,
			segment_bytes,
		) = {
			let guard = self.inner.read().await;
			(
				guard.file_ops.clone(),
				guard.artifact_paths.clone(),
				guard.wal_segments.clone(),
				guard.wal_file.clone(),
				guard.wal_cursor,
				guard.wal_segment_bytes,
			)
		};

		let Some(file_ops) = file_ops else {
			let mut guard = self.inner.write().await;
			for entry in &batch {
				guard.entries.insert(entry.log_id.index, clone_entry(entry));
				guard.last_log_id = Some(match guard.last_log_id {
					Some(prev) if prev > entry.log_id => prev,
					_ => entry.log_id,
				});
			}
			callback.log_io_completed(Ok(()));
			return Ok(());
		};
		let artifact_paths = artifact_paths.ok_or_else(|| {
			StorageIOError::write_logs(AnyError::error("durable wal append missing artifact paths"))
		})?;

		let mut touched_segments: Vec<(u64, FileHandle)> = Vec::new();
		let mut pending = Vec::with_capacity(batch.len());
		let wal_append_started = Instant::now();
		for (entry, page) in batch.iter().zip(pages) {
			if page.len() as u64 > segment_bytes {
				let err = Error::new(
					ErrorKind::InvalidInput,
					format!(
						"wal record {} has {} bytes, exceeding segment size {}",
						entry.log_id.index,
						page.len(),
						segment_bytes
					),
				);
				callback.log_io_completed(Err(Error::new(err.kind(), err.to_string())));
				return Err(StorageIOError::write_logs(AnyError::new(&err)).into());
			}
			if wal_file.is_none() || wal_cursor + page.len() as u64 > segment_bytes {
				let (segment_path, segment_handle) =
					create_wal_segment(file_ops.as_ref(), &artifact_paths.wal, entry.log_id.index)
						.await
						.map_err(|e| StorageIOError::write_logs(AnyError::new(&e)))?;
				wal_segments.push(WalSegmentMeta {
					path: segment_path,
					start_index: entry.log_id.index,
					end_log_index: None,
					len_bytes: 0,
				});
				wal_file = Some(segment_handle);
				wal_cursor = 0;
			}
			let active_handle = wal_file.clone().expect("active wal segment handle");
			let active_segment = wal_segments
				.last_mut()
				.expect("active wal segment metadata");
			let expected_len = page.len();
			let (written, _page) = match file_ops.write_at(&active_handle, wal_cursor, page).await {
				Ok(result) => result,
				Err(e) => {
					callback
						.log_io_completed(Err(std::io::Error::other(format!("wal write: {e}"))));
					return Err(StorageIOError::write_logs(AnyError::new(&e)).into());
				}
			};
			if written != expected_len {
				let err = std::io::Error::new(
					std::io::ErrorKind::WriteZero,
					format!("short wal write: expected {expected_len} bytes, got {written}"),
				);
				callback.log_io_completed(Err(std::io::Error::new(err.kind(), err.to_string())));
				return Err(StorageIOError::write_logs(AnyError::new(&err)).into());
			}
			if touched_segments
				.last()
				.map(|(start_index, _)| *start_index != active_segment.start_index)
				.unwrap_or(true)
			{
				touched_segments.push((active_segment.start_index, active_handle.clone()));
			}
			pending.push(PendingWalWrite {
				entry: clone_entry(entry),
				pointer: WalRecordPointer {
					segment_start_index: active_segment.start_index,
					offset: wal_cursor,
					len_bytes: expected_len as u64,
				},
			});
			wal_cursor += expected_len as u64;
			active_segment.len_bytes = wal_cursor;
			active_segment.end_log_index = Some(entry.log_id.index);
		}
		metrics().observe_wal_append_latency(wal_append_started.elapsed());

		let wal_fsync_started = Instant::now();
		for (_, handle) in &touched_segments {
			// Durability barrier. All write CQEs above have already
			// been awaited, so this fdatasync meaningfully covers them
			// despite `io_uring`'s unordered fsync-vs-write semantics.
			if let Err(e) = file_ops.fdatasync(handle).await {
				callback
					.log_io_completed(Err(std::io::Error::other(format!("wal fdatasync: {e}"))));
				return Err(StorageIOError::write_logs(AnyError::new(&e)).into());
			}
		}
		metrics().observe_wal_fsync_latency(wal_fsync_started.elapsed());

		{
			let mut guard = self.inner.write().await;
			guard.wal_segments = wal_segments;
			guard.wal_file = wal_file;
			guard.wal_cursor = wal_cursor;
			for pending in pending {
				let id = pending.entry.log_id;
				guard.entries.insert(id.index, pending.entry);
				guard.entry_positions.insert(id.index, pending.pointer);
				guard.last_log_id = Some(match guard.last_log_id {
					Some(prev) if prev > id => prev,
					_ => id,
				});
			}
		}

		callback.log_io_completed(Ok(()));
		Ok(())
	}

	async fn truncate(&mut self, log_id: LogId<u32>) -> Result<(), StorageError<u32>> {
		let _gate = self.wal_gate.lock().await;
		let (file_ops, last_purged, last_retained, mut wal_segments) = {
			let guard = self.inner.read().await;
			(
				guard.file_ops.clone(),
				guard.last_purged,
				guard
					.entry_positions
					.range(..log_id.index)
					.next_back()
					.map(|(&index, pointer)| (index, *pointer)),
				guard.wal_segments.clone(),
			)
		};

		if let Some(file_ops) = file_ops {
			if let Some((last_retained_index, pointer)) = last_retained {
				let keep_segment_index = wal_segments
					.iter()
					.position(|segment| segment.start_index == pointer.segment_start_index)
					.ok_or_else(|| {
						StorageIOError::write_logs(AnyError::error(
							"wal truncate lost retained segment metadata",
						))
					})?;
				let new_len = pointer.offset + pointer.len_bytes;
				let keep_path = wal_segments[keep_segment_index].path.clone();
				for segment in wal_segments.iter().skip(keep_segment_index + 1) {
					unlink_wal_artifact(file_ops.as_ref(), &segment.path)
						.await
						.map_err(|e| StorageIOError::write_logs(AnyError::new(&e)))?;
				}
				wal_segments.truncate(keep_segment_index + 1);
				wal_segments[keep_segment_index].len_bytes = new_len;
				wal_segments[keep_segment_index].end_log_index = Some(last_retained_index);
				let handle = open_existing_wal_segment(file_ops.as_ref(), &keep_path)
					.await
					.map_err(|e| StorageIOError::write_logs(AnyError::new(&e)))?;
				file_ops
					.set_len(&handle, new_len)
					.await
					.map_err(|e| StorageIOError::write_logs(AnyError::new(&e)))?;
				file_ops
					.fsync(&handle)
					.await
					.map_err(|e| StorageIOError::write_logs(AnyError::new(&e)))?;
				let mut guard = self.inner.write().await;
				guard.entries.retain(|&idx, _| idx < log_id.index);
				guard.entry_positions.retain(|&idx, _| idx < log_id.index);
				guard.wal_segments = wal_segments;
				guard.wal_file = Some(handle);
				guard.wal_cursor = new_len;
				guard.last_log_id = guard
					.entries
					.values()
					.last()
					.map(|e| e.log_id)
					.or(last_purged);
			} else {
				for segment in &wal_segments {
					unlink_wal_artifact(file_ops.as_ref(), &segment.path)
						.await
						.map_err(|e| StorageIOError::write_logs(AnyError::new(&e)))?;
				}
				let mut guard = self.inner.write().await;
				guard.entries.clear();
				guard.entry_positions.clear();
				guard.wal_segments.clear();
				guard.wal_file = None;
				guard.wal_cursor = 0;
				guard.last_log_id = guard.last_purged;
			}
		} else {
			let mut guard = self.inner.write().await;
			guard.entries.retain(|&idx, _| idx < log_id.index);
			guard.entry_positions.retain(|&idx, _| idx < log_id.index);
			guard.last_log_id = guard
				.entries
				.values()
				.last()
				.map(|e| e.log_id)
				.or(guard.last_purged);
		}
		Ok(())
	}

	async fn purge(&mut self, log_id: LogId<u32>) -> Result<(), StorageError<u32>> {
		if retention::refuses(&self.retention, RetentionViolation::Purge) {
			return Err(
				StorageIOError::write_logs(AnyError::new(&RetentionViolation::Purge)).into(),
			);
		}
		let _gate = self.wal_gate.lock().await;
		let (file_ops, mut wal_segments) = {
			let guard = self.inner.read().await;
			(guard.file_ops.clone(), guard.wal_segments.clone())
		};
		if let Some(file_ops) = file_ops.as_ref() {
			let delete_count = wal_segments
				.iter()
				.take_while(|segment| {
					segment
						.end_log_index
						.map(|index| index <= log_id.index)
						.unwrap_or(true)
				})
				.count();
			for segment in wal_segments.iter().take(delete_count) {
				unlink_wal_artifact(file_ops.as_ref(), &segment.path)
					.await
					.map_err(|e| StorageIOError::write_logs(AnyError::new(&e)))?;
			}
			if delete_count > 0 {
				wal_segments.drain(..delete_count);
			}
		}
		let reopened_file = if let Some(file_ops) = &file_ops {
			if let Some(last) = wal_segments.last() {
				Some(
					open_existing_wal_segment(file_ops.as_ref(), &last.path)
						.await
						.map_err(|e| StorageIOError::write_logs(AnyError::new(&e)))?,
				)
			} else {
				None
			}
		} else {
			None
		};

		let mut guard = self.inner.write().await;
		guard.entries.retain(|&idx, _| idx > log_id.index);
		guard.entry_positions.retain(|&idx, _| idx > log_id.index);
		guard.last_purged = Some(match guard.last_purged {
			Some(prev) if prev > log_id => prev,
			_ => log_id,
		});
		if guard.file_ops.is_some() {
			guard.wal_segments = wal_segments;
			if let Some(last) = guard.wal_segments.last() {
				guard.wal_cursor = last.len_bytes;
				guard.wal_file = reopened_file;
			} else {
				guard.wal_file = None;
				guard.wal_cursor = 0;
			}
		}
		guard.last_log_id = guard
			.entries
			.values()
			.last()
			.map(|e| e.log_id)
			.or(guard.last_purged);
		if guard.last_log_id.map(|id| id < log_id).unwrap_or(true) {
			guard.last_log_id = guard.last_purged;
		}
		Ok(())
	}
}

#[derive(Clone, Copy, Debug)]
struct CheckpointCoordinatorConfig {
	entry_threshold: u64,
	time_threshold: Duration,
}

impl Default for CheckpointCoordinatorConfig {
	fn default() -> Self {
		Self {
			entry_threshold: CHECKPOINT_ENTRY_THRESHOLD,
			time_threshold: CHECKPOINT_TIME_THRESHOLD,
		}
	}
}

#[derive(Clone, Copy, Debug, Default)]
struct CheckpointCoordinatorState {
	latest: Option<CheckpointObservation>,
	last_written: Option<RecoveryCheckpoint>,
}

pub struct CheckpointCoordinator {
	store: WendelonLogStore,
	config: CheckpointCoordinatorConfig,
	state: Mutex<CheckpointCoordinatorState>,
	write_gate: Mutex<()>,
	last_written_at: Mutex<Instant>,
}

impl CheckpointCoordinator {
	pub fn new(store: WendelonLogStore) -> Arc<Self> {
		Self::new_with_config(store, CheckpointCoordinatorConfig::default())
	}

	fn new_with_config(store: WendelonLogStore, config: CheckpointCoordinatorConfig) -> Arc<Self> {
		Arc::new(Self {
			store,
			config,
			state: Mutex::new(CheckpointCoordinatorState::default()),
			write_gate: Mutex::new(()),
			last_written_at: Mutex::new(Instant::now()),
		})
	}

	pub fn spawn(self: Arc<Self>) -> JoinHandle<()> {
		tokio::spawn(async move {
			let mut ticker = tokio::time::interval(self.config.time_threshold);
			ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
			loop {
				ticker.tick().await;
				if let Err(err) = self.flush_latest(false).await {
					tracing::warn!(error = %err, "periodic recovery checkpoint emission failed");
				}
			}
		})
	}

	async fn flush_latest(&self, force: bool) -> std::io::Result<()> {
		let persisted_checkpoint = self.store.recovery_checkpoint().await;
		let _write_guard = self.write_gate.lock().await;
		let observation = {
			let mut state = self.state.lock().await;
			if state.last_written.is_none() {
				state.last_written = persisted_checkpoint;
			}
			let Some(observation) = state.latest else {
				return Ok(());
			};
			let changed = state
				.last_written
				.map(|written| written != observation.checkpoint)
				.unwrap_or(true);
			if !changed {
				return Ok(());
			}
			if !force && !observation.force {
				let elapsed = self.last_written_at.lock().await.elapsed();
				if elapsed < self.config.time_threshold {
					return Ok(());
				}
			}
			observation
		};
		self.store.write_recovery_checkpoint(observation).await?;
		{
			let mut state = self.state.lock().await;
			state.last_written = Some(observation.checkpoint);
			if state.latest == Some(observation) {
				state.latest = None;
			}
		}
		*self.last_written_at.lock().await = Instant::now();
		Ok(())
	}
}

#[async_trait::async_trait]
impl CheckpointObserver for CheckpointCoordinator {
	async fn on_checkpoint_progress(
		&self,
		observation: CheckpointObservation,
	) -> std::io::Result<()> {
		let persisted_checkpoint = self.store.recovery_checkpoint().await;
		let should_flush_now = {
			let mut state = self.state.lock().await;
			if state.last_written.is_none() {
				state.last_written = persisted_checkpoint;
			}
			state.latest = Some(observation);
			let last_written_commit = state
				.last_written
				.map(|written| written.commit_index)
				.unwrap_or(0);
			observation.force
				|| observation
					.checkpoint
					.commit_index
					.saturating_sub(last_written_commit)
					>= self.config.entry_threshold
		};
		if should_flush_now {
			self.flush_latest(true).await?;
		}
		Ok(())
	}
}

// ============================================================
//  STATE MACHINE
// ============================================================

/// In-memory state machine content. `Serialize + Deserialize` so
/// the snapshot builder can postcard-encode it directly.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SmState {
	pub last_applied_log_id: Option<LogId<u32>>,
	pub last_membership: StoredMembership<u32, BasicNode>,
	/// Durable control-ring epoch history. The latest committed
	/// entry may be ahead of the externally visible watch epoch
	/// while auto-seeding is still reconciling the Migration
	/// Ledger after the config commit.
	#[serde(default)]
	pub epoch_history: BTreeMap<u64, EpochConfig>,
	/// Per-range active leases (Plan 018). Keyed by
	/// `LeaseDescriptor.range_start` so a holder renewing the same
	/// vnode replaces its prior lease in-place. Overlap detection
	/// scans every entry on grant — the cluster holds O(vnodes)
	/// leases, which is small enough that a linear pass is cheaper
	/// than maintaining a dedicated interval index here.
	pub leases: HashMap<u64, crate::schema::LeaseDescriptor>,
	/// Per-epoch E_safe watermark. Value is the `commit_hlc` of
	/// the highest FinalizationMarker observed in that epoch, so
	/// updates are monotonic within an epoch.
	pub e_safe_watermarks: HashMap<u64, HybridTimestamp>,
	/// Highest finalized Raft log index observed so far. This is the
	/// scalar checkpoint form of the read-safety boundary.
	#[serde(default)]
	pub last_e_safe_index: u64,
	/// Slabs that have been tiered to L2 by the sweeper (ADR 006).
	/// Keyed by `slab_id`; every replica must agree on the mapping
	/// so read-path pivots are deterministic. Observers — the
	/// local `SlabManager` — are fired out-of-band when an entry
	/// lands here so they can unlink the L1 file.
	#[serde(default)]
	pub tiered_slabs: HashMap<SlabId, TieredSlab>,
	/// Migration Ledger (Plan 019). Keyed by `migration_id` so
	/// apply-time idempotency and chained-migration reconciliation
	/// are both O(1). Entries never fall out of the map during
	/// normal operation — completed migrations are retained as the
	/// authoritative source-of-truth for future pulls; a dedicated
	/// GC pass (follow-up work) trims entries once every source
	/// replica has observed the corresponding tombstone.
	#[serde(default)]
	pub migrations: HashMap<u64, MigrationLedgerEntry>,
	/// Logical "this range moved away" markers (Plan 019). Appended
	/// on `MigrationComplete` so prior source replicas can fence
	/// their former range out of future migration-source scans.
	/// Physical reclaim is deferred to tombstone-aware compaction.
	#[serde(default)]
	pub tombstones: Vec<MigratedRangeTombstone>,
	/// Authoritative tenant metadata: lifecycle state, quota, and
	/// accounted bytes. This is the replicated source of truth the
	/// data-path cache periodically reconciles from.
	#[serde(default)]
	pub tenants: HashMap<crate::schema::TenantId, TenantRecord>,
	/// Wrapped Tenant KEKs keyed by tenant. Historical versions are
	/// retained so pre-rotation slabs keep decrypting while new slabs
	/// wrap under the current active KEK version.
	///
	/// Keep this durable field at the end of the struct so older
	/// snapshots can keep defaulting it cleanly; keeping new fields
	/// trailing avoids mid-struct postcard reorder hazards in future
	/// upgrades.
	#[serde(default)]
	pub tenant_keysets: HashMap<crate::schema::TenantId, TenantKeyset>,
	/// Replicated actor-certificate denylist keyed by trust domain and
	/// stable claim `key_id`. Transport and observability admission use
	/// this set directly so revocation does not depend on external CRL
	/// fetch timing.
	#[serde(default)]
	pub revoked_actor_keys: BTreeMap<RevokedActorKeyId, u64>,
	/// ADR 037 tenant table schema descriptors keyed by durable table
	/// object id and schema generation. Legacy KV traffic can still use
	/// the default descriptor without needing this map populated.
	#[serde(default)]
	pub table_schemas: TableSchemaCatalog,
	/// ADR 090 tenant logical type definitions. The ids in table descriptor
	/// field metadata are honest only if this durable registry travels with
	/// the descriptors that reference them.
	#[serde(default)]
	pub logical_types: LogicalTypeCatalog,
	/// ADR 052 secondary-index descriptors and lifecycle state. Index
	/// metadata is separate from table descriptors, so index create /
	/// drop does not mutate table schema generation.
	#[serde(default)]
	pub secondary_indexes: SecondaryIndexCatalog,
	/// ADR 048 table lifecycle policy keyed by durable table identity.
	/// Retention is current lifecycle authority and intentionally stays
	/// outside schema descriptors and schema generations.
	pub table_lifecycle: TableLifecycleCatalog,
	/// ADR 039 table DROP fence state keyed by durable table identity.
	/// This is distinct from retention policy and is consulted by
	/// Wendelon-owned DROP orchestration and data-path admission.
	#[serde(default)]
	pub table_drop_fences: TableDropFenceCatalog,
	/// ADR 0093 terminal DROP outcomes. Unlike a fence, an outcome survives
	/// finalize so a caller can recover after losing the success response.
	pub table_drop_outcomes: TableDropOutcomeCatalog,
	/// ADR 548 durable CTAS staged-publication saga ledger.
	#[serde(default)]
	pub ctas_sagas: CtasSagaCatalog,
	/// ADR 0087: one immutable birth certificate per materialized object,
	/// written inside the Publish apply, removed with the object.
	#[serde(default)]
	pub materialization_certificates: crate::schema::MaterializationCertificateCatalog,
	/// ADR 0095 admission authority after logical finalize and before physical
	/// settlement. This is distinct from migrated-range tombstones above.
	pub table_drop_tombstones: crate::schema::TableDropTombstoneCatalog,
	/// ADR 0095 replicated physical-erasure obligations.
	pub table_drop_reclaim_debts: crate::schema::TableDropReclaimDebtCatalog,
}

impl SmState {
	/// One lifecycle query serves every data-path admission site. A prepared
	/// fence and a finalized logical tombstone have identical authority.
	pub fn table_drop_blocks_admission(
		&self,
		tenant_id: crate::schema::TenantId,
		table_object_id: crate::schema::TableObjectId,
	) -> bool {
		self.table_drop_fences.is_fenced(tenant_id, table_object_id)
			|| self
				.table_drop_tombstones
				.contains(tenant_id, table_object_id)
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TenantKekRetirementVersionStatus {
	pub tenant_kek_id: TenantKekId,
	pub is_active: bool,
	pub tiered_slab_ref_count: u64,
	pub safe_to_retire: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TenantKekRetirementStatus {
	pub accounting_complete: bool,
	pub versions: Vec<TenantKekRetirementVersionStatus>,
}

pub(crate) fn tenant_kek_retirement_status(
	snapshot: &SmState,
	tenant_id: TenantId,
) -> TenantKekRetirementStatus {
	let Some(keyset) = snapshot.tenant_keysets.get(&tenant_id) else {
		return TenantKekRetirementStatus {
			accounting_complete: true,
			versions: Vec::new(),
		};
	};
	let active_tenant_kek_id = keyset.active_tenant_kek_id;
	let mut tiered_ref_counts: BTreeMap<TenantKekId, u64> = BTreeMap::new();
	let mut accounting_complete = !snapshot
		.tiered_slabs
		.values()
		.any(|tier| tier.is_encrypted && tier.tenant_id.is_none());
	for tier in snapshot.tiered_slabs.values() {
		if !tier.is_encrypted {
			continue;
		}
		match (tier.tenant_id, tier.tenant_kek_id) {
			(Some(tier_tenant_id), Some(tenant_kek_id)) if tier_tenant_id == tenant_id => {
				*tiered_ref_counts.entry(tenant_kek_id).or_default() += 1;
			}
			(Some(tier_tenant_id), None) if tier_tenant_id == tenant_id => {
				accounting_complete = false;
			}
			_ => {}
		}
	}
	let mut versions = keyset
		.keys
		.keys()
		.copied()
		.map(|tenant_kek_id| {
			let tiered_slab_ref_count = tiered_ref_counts.get(&tenant_kek_id).copied().unwrap_or(0);
			let is_active = Some(tenant_kek_id) == active_tenant_kek_id;
			TenantKekRetirementVersionStatus {
				tenant_kek_id,
				is_active,
				tiered_slab_ref_count,
				safe_to_retire: accounting_complete && !is_active && tiered_slab_ref_count == 0,
			}
		})
		.collect::<Vec<_>>();
	versions.sort_by_key(|version| version.tenant_kek_id);
	TenantKekRetirementStatus {
		accounting_complete,
		versions,
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdmissionSnapshot {
	pub current_epoch: u64,
	pub e_safe_watermarks: Arc<HashMap<u64, HybridTimestamp>>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RevocationSnapshot {
	pub revoked_actor_keys: Arc<BTreeMap<RevokedActorKeyId, u64>>,
}

impl RevocationSnapshot {
	pub fn contains(&self, trust_domain: ActorTrustDomain, key_id: &[u8]) -> bool {
		self.revoked_actor_keys.contains_key(&RevokedActorKeyId {
			trust_domain,
			key_id: key_id.to_vec(),
		})
	}
}

fn admission_snapshot_from_state(state: &SmState) -> Arc<AdmissionSnapshot> {
	Arc::new(AdmissionSnapshot {
		current_epoch: state.e_safe_watermarks.keys().copied().max().unwrap_or(0),
		e_safe_watermarks: Arc::new(state.e_safe_watermarks.clone()),
	})
}

fn revocation_snapshot_from_state(state: &SmState) -> Arc<RevocationSnapshot> {
	Arc::new(RevocationSnapshot {
		revoked_actor_keys: Arc::new(state.revoked_actor_keys.clone()),
	})
}

fn admission_snapshot_changed(
	current: &AdmissionSnapshot,
	state: &SmState,
	current_epoch: u64,
) -> bool {
	current.current_epoch != current_epoch
		|| current.e_safe_watermarks.as_ref() != &state.e_safe_watermarks
}

fn revocation_snapshot_changed(current: &RevocationSnapshot, state: &SmState) -> bool {
	current.revoked_actor_keys.as_ref() != &state.revoked_actor_keys
}

const REVOKED_ACTOR_KEY_RETENTION_MS: u64 = 24 * 60 * 60 * 1000;

fn prune_expired_revoked_actor_keys(state: &mut SmState, prune_before_ms: u64) {
	state
		.revoked_actor_keys
		.retain(|_, revoked_at_ms| *revoked_at_ms == 0 || *revoked_at_ms >= prune_before_ms);
}

/// Out-of-band observer for [`TieredSlab`] apply events.
///
/// Wendelon's `SlabManager` implements this so every node — not
/// just the sweeper that initiated the Raft commit — learns when
/// a slab has moved to L2. The observer runs inside the state
/// machine's apply loop and must therefore be non-blocking.
#[async_trait::async_trait]
pub trait TierObserver: Send + Sync {
	async fn on_slab_tiered(
		&self,
		tier: &TieredSlab,
		applied_log_index: u64,
	) -> std::io::Result<()>;

	async fn on_slab_retired(
		&self,
		slab_id: SlabId,
		applied_log_index: u64,
	) -> std::io::Result<()> {
		let _ = (slab_id, applied_log_index);
		Ok(())
	}

	async fn on_migrated_range_tombstone(
		&self,
		tombstone: &MigratedRangeTombstone,
		applied_log_index: u64,
	) -> std::io::Result<()> {
		let _ = (tombstone, applied_log_index);
		Ok(())
	}

	async fn on_lsm_compaction_committed(
		&self,
		commit: &CompactionCommit,
		applied_log_index: u64,
	) -> std::io::Result<()> {
		let _ = (commit, applied_log_index);
		Ok(())
	}
}

#[async_trait::async_trait]
pub trait TenantKekObserver: Send + Sync {
	async fn on_tenant_kek_rotated(
		&self,
		rotation: &InstallTenantKekVersion,
		applied_log_index: u64,
	) -> std::io::Result<()>;

	async fn sync_tenant_keysets(
		&self,
		snapshot: &HashMap<crate::schema::TenantId, TenantKeyset>,
	) -> std::io::Result<()>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CheckpointObservation {
	pub last_log_id: Option<LogId<u32>>,
	pub checkpoint: RecoveryCheckpoint,
	pub force: bool,
}

#[async_trait::async_trait]
pub trait CheckpointObserver: Send + Sync {
	async fn on_checkpoint_progress(
		&self,
		observation: CheckpointObservation,
	) -> std::io::Result<()>;
}

/// Snapshot currently held by the state machine.
#[derive(Clone, Debug)]
struct HeldSnapshot {
	meta: SnapshotMeta<u32, BasicNode>,
	path: PathBuf,
}

#[derive(Clone, Debug)]
struct SnapshotPaths {
	current: PathBuf,
	build_tmp: PathBuf,
	receive_tmp: PathBuf,
}

impl SnapshotPaths {
	fn from_current(current: PathBuf) -> Self {
		let parent = current
			.parent()
			.map(Path::to_path_buf)
			.unwrap_or_else(|| PathBuf::from("."));
		let file_name = current
			.file_name()
			.and_then(|name| name.to_str())
			.unwrap_or("snapshot.wnd")
			.to_string();
		Self {
			current,
			build_tmp: parent.join(format!(".{file_name}.build.tmp")),
			receive_tmp: parent.join(format!(".{file_name}.receive.tmp")),
		}
	}

	fn ephemeral() -> Self {
		let root = std::env::temp_dir().join(format!("wendelon-snapshot-{}", uuid::Uuid::new_v4()));
		Self::from_current(root.join("snapshot.wnd"))
	}
}

#[derive(Clone)]
struct SnapshotRuntime {
	file_ops: Arc<dyn AsyncFileOps>,
	paths: SnapshotPaths,
}

impl SnapshotRuntime {
	fn new(file_ops: Arc<dyn AsyncFileOps>, paths: SnapshotPaths) -> Self {
		Self { file_ops, paths }
	}

	fn ephemeral() -> Self {
		Self::new(Arc::new(BlockingFileOps), SnapshotPaths::ephemeral())
	}
}

/// Streaming snapshot artifact backed by [`AsyncFileOps`].
///
/// This path is intentionally bounded-memory and correctness-first,
/// not a hot `io_uring`/`O_DIRECT` fast path: under
/// [`crate::storage::IoUringFileOps`], the plain read/write methods
/// used here run through the file-op blocking executor over cloned
/// file handles rather than the submission queue.
pub struct SnapshotDataFile {
	path: PathBuf,
	file_ops: Arc<dyn AsyncFileOps>,
	handle: FileHandle,
	cursor: u64,
	logical_len: u64,
	pending: Option<PendingSnapshotIo>,
}

enum PendingSnapshotIo {
	Read(Pin<Box<dyn Future<Output = std::io::Result<Vec<u8>>> + Send>>),
	WriteInFlight {
		requested_len: usize,
		fut: Pin<Box<dyn Future<Output = std::io::Result<usize>> + Send>>,
	},
	WriteReady(usize),
}

impl SnapshotDataFile {
	async fn create(
		file_ops: Arc<dyn AsyncFileOps>,
		path: impl Into<PathBuf>,
	) -> std::io::Result<Self> {
		let path = path.into();
		let handle = file_ops
			.open(
				&path,
				OpenFlags {
					read: true,
					write: true,
					create: true,
					truncate: true,
					..Default::default()
				},
			)
			.await?;
		Ok(Self {
			path,
			file_ops,
			handle,
			cursor: 0,
			logical_len: 0,
			pending: None,
		})
	}

	async fn open(
		file_ops: Arc<dyn AsyncFileOps>,
		path: impl Into<PathBuf>,
	) -> std::io::Result<Self> {
		let path = path.into();
		let logical_len = file_ops.file_len(&path).await?.unwrap_or(0);
		let handle = file_ops
			.open(
				&path,
				OpenFlags {
					read: true,
					..Default::default()
				},
			)
			.await?;
		Ok(Self {
			path,
			file_ops,
			handle,
			cursor: 0,
			logical_len,
			pending: None,
		})
	}

	fn path(&self) -> &Path {
		&self.path
	}

	fn finish_write(
		&mut self,
		requested_len: usize,
		result: std::io::Result<usize>,
	) -> std::io::Result<usize> {
		let written = result?;
		let logical_written = written.min(requested_len);
		self.cursor = self
			.cursor
			.checked_add(logical_written as u64)
			.ok_or_else(|| Error::other("snapshot write cursor overflow"))?;
		self.logical_len = self.logical_len.max(self.cursor);
		Ok(logical_written)
	}
}

impl Unpin for SnapshotDataFile {}

impl AsyncRead for SnapshotDataFile {
	fn poll_read(
		mut self: Pin<&mut Self>,
		cx: &mut Context<'_>,
		buf: &mut ReadBuf<'_>,
	) -> Poll<std::io::Result<()>> {
		let this = self.as_mut().get_mut();
		if buf.remaining() == 0 {
			return Poll::Ready(Ok(()));
		}
		if this.pending.is_none() {
			let file_ops = this.file_ops.clone();
			let handle = this.handle.clone();
			let offset = this.cursor;
			let len = buf.remaining();
			this.pending = Some(PendingSnapshotIo::Read(Box::pin(async move {
				file_ops.read_plain_at(&handle, offset, len).await
			})));
		}
		let Some(PendingSnapshotIo::Read(fut)) = this.pending.as_mut() else {
			return Poll::Ready(Err(Error::other(
				"snapshot read attempted while a write was in flight",
			)));
		};
		match fut.as_mut().poll(cx) {
			Poll::Pending => Poll::Pending,
			Poll::Ready(result) => {
				this.pending = None;
				let bytes = result?;
				this.cursor = this
					.cursor
					.checked_add(bytes.len() as u64)
					.ok_or_else(|| Error::other("snapshot read cursor overflow"))?;
				buf.put_slice(&bytes);
				Poll::Ready(Ok(()))
			}
		}
	}
}

impl AsyncWrite for SnapshotDataFile {
	fn poll_write(
		mut self: Pin<&mut Self>,
		cx: &mut Context<'_>,
		buf: &[u8],
	) -> Poll<Result<usize, std::io::Error>> {
		let this = self.as_mut().get_mut();
		if this.pending.is_none() {
			let requested_len = buf.len();
			let file_ops = this.file_ops.clone();
			let handle = this.handle.clone();
			let offset = this.cursor;
			let bytes = buf.to_vec();
			this.pending = Some(PendingSnapshotIo::WriteInFlight {
				requested_len,
				fut: Box::pin(async move { file_ops.write_plain_at(&handle, offset, bytes).await }),
			});
		}
		match this.pending.as_mut() {
			Some(PendingSnapshotIo::Read(_)) => Poll::Ready(Err(Error::other(
				"snapshot write attempted while a read was in flight",
			))),
			Some(PendingSnapshotIo::WriteReady(written)) => {
				let result = Ok(*written);
				this.pending = None;
				Poll::Ready(result)
			}
			Some(PendingSnapshotIo::WriteInFlight { requested_len, fut }) => {
				match fut.as_mut().poll(cx) {
					Poll::Pending => Poll::Pending,
					Poll::Ready(result) => {
						let requested_len = *requested_len;
						this.pending = None;
						Poll::Ready(this.finish_write(requested_len, result))
					}
				}
			}
			None => Poll::Ready(Err(Error::other(
				"snapshot write future disappeared unexpectedly",
			))),
		}
	}

	fn poll_flush(
		mut self: Pin<&mut Self>,
		cx: &mut Context<'_>,
	) -> Poll<Result<(), std::io::Error>> {
		let this = self.as_mut().get_mut();
		match this.pending.as_mut() {
			None => Poll::Ready(Ok(())),
			Some(PendingSnapshotIo::Read(_)) => Poll::Ready(Err(Error::other(
				"snapshot flush attempted while a read was in flight",
			))),
			Some(PendingSnapshotIo::WriteReady(_)) => Poll::Ready(Ok(())),
			Some(PendingSnapshotIo::WriteInFlight { requested_len, fut }) => {
				let requested_len = *requested_len;
				let result = match fut.as_mut().poll(cx) {
					Poll::Pending => return Poll::Pending,
					Poll::Ready(result) => result,
				};
				match this.finish_write(requested_len, result) {
					Ok(written) => {
						this.pending = Some(PendingSnapshotIo::WriteReady(written));
						Poll::Ready(Ok(()))
					}
					Err(err) => {
						this.pending = None;
						Poll::Ready(Err(err))
					}
				}
			}
		}
	}

	fn poll_shutdown(
		self: Pin<&mut Self>,
		cx: &mut Context<'_>,
	) -> Poll<Result<(), std::io::Error>> {
		self.poll_flush(cx)
	}
}

impl AsyncSeek for SnapshotDataFile {
	fn start_seek(self: Pin<&mut Self>, position: SeekFrom) -> Result<(), std::io::Error> {
		let this = self.get_mut();
		if this.pending.is_some() {
			return Err(Error::other(
				"snapshot seek attempted while an I/O operation was in flight",
			));
		}
		let base = match position {
			SeekFrom::Start(offset) => {
				this.cursor = offset;
				return Ok(());
			}
			SeekFrom::Current(delta) => this.cursor as i128 + delta as i128,
			SeekFrom::End(delta) => this.logical_len as i128 + delta as i128,
		};
		if base < 0 || base > u64::MAX as i128 {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				format!("invalid snapshot seek target: {position:?}"),
			));
		}
		this.cursor = base as u64;
		Ok(())
	}

	fn poll_complete(
		self: Pin<&mut Self>,
		_cx: &mut Context<'_>,
	) -> Poll<Result<u64, std::io::Error>> {
		Poll::Ready(Ok(self.get_mut().cursor))
	}
}

struct HashingWriter<W> {
	inner: W,
	hasher: control_artifact_hash::Context,
}

impl<W> HashingWriter<W> {
	fn new(inner: W) -> Self {
		Self {
			inner,
			hasher: control_artifact_hash::Context::new(),
		}
	}

	fn finalize(self) -> (W, [u8; 32]) {
		(self.inner, self.hasher.finish())
	}
}

impl<W: Write> Write for HashingWriter<W> {
	fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
		let written = self.inner.write(buf)?;
		self.hasher.update(&buf[..written]);
		Ok(written)
	}

	fn flush(&mut self) -> std::io::Result<()> {
		self.inner.flush()
	}
}

#[derive(Default)]
struct SmInner {
	state: SmState,
	snapshot: Option<HeldSnapshot>,
	/// Monotonic id handed out in built snapshots so every meta
	/// has a unique `snapshot_id` even when the state hasn't
	/// changed between builds.
	snapshot_seq: u64,
}

/// Wendelon's Raft state machine. Lease / epoch / E_safe state
/// lives behind an `Arc<RwLock<_>>` so the snapshot builder can
/// take a read lock for a consistent view without blocking apply
/// traffic longer than the postcard encode takes.
#[derive(Clone)]
pub struct WendelonStateMachine {
	inner: Arc<RwLock<SmInner>>,
	/// ADR 0095 node-local admission/drain authority. Raft apply closes an
	/// object here when DROP prepare commits; data paths increment through the
	/// same mutex before beginning byte-mutating work.
	mutation_drain: Arc<MutationDrainRegistry>,
	snapshot_runtime: SnapshotRuntime,
	admission_snapshot: watch::Sender<Arc<AdmissionSnapshot>>,
	revocation_snapshot: watch::Sender<Arc<RevocationSnapshot>>,
	external_revocation_snapshot: Option<watch::Sender<Arc<RevocationSnapshot>>>,
	/// Optional callback fired every time a [`TieredSlab`] lands
	/// in the state. The local `SlabManager` installs one so it
	/// can unlink the L1 file once the cluster has agreed the
	/// payload lives in L2. `None` is fine for tests or for
	/// non-data nodes that never hold a `SlabManager`.
	tier_observer: Option<Arc<dyn TierObserver>>,
	/// Optional callback fired when a Tenant KEK rotation lands in
	/// the replicated state so local unwrap caches can converge.
	tenant_kek_observer: Option<Arc<dyn TenantKekObserver>>,
	/// Optional callback fired when committed apply progress should
	/// be checkpointed into the WAL.
	checkpoint_observer: Option<Arc<dyn CheckpointObserver>>,
	/// Optional materializer for Raft-authority audit events.
	audit_materializer: Option<RaftAuditMaterializer>,
	/// Node identity attached by production bootstrap for process-local
	/// observability events. Recovery remains independent of node identity.
	observability_node_id: Option<SchemaNodeId>,
	observability_metrics: &'static MetricsRegistry,
	/// Non-blocking live feed of committed finalization markers.
	finalization_events: broadcast::Sender<FinalizationMarker>,
	/// The node's Control retention guard; `None` is `Compacting`.
	retention: Option<Arc<RetentionGuard>>,
}

impl WendelonStateMachine {
	pub fn new() -> Self {
		let observability_metrics = metrics();
		observability_metrics.set_drop_reclaim_debts(0);
		let (admission_snapshot, _) =
			watch::channel(admission_snapshot_from_state(&SmState::default()));
		let (revocation_snapshot, _) =
			watch::channel(revocation_snapshot_from_state(&SmState::default()));
		let (finalization_events, _) = broadcast::channel(FINALIZATION_BROADCAST_CAPACITY);
		Self {
			inner: Arc::new(RwLock::new(SmInner::default())),
			mutation_drain: MutationDrainRegistry::fresh_reconciled(),
			snapshot_runtime: SnapshotRuntime::ephemeral(),
			admission_snapshot,
			revocation_snapshot,
			external_revocation_snapshot: None,
			tier_observer: None,
			tenant_kek_observer: None,
			checkpoint_observer: None,
			audit_materializer: None,
			observability_node_id: None,
			observability_metrics,
			finalization_events,
			retention: None,
		}
	}

	fn from_recovery(
		snapshot: Option<LoadedSnapshot>,
		entries: Vec<Entry<WendelonTypeConfig>>,
		checkpoint: Option<RecoveryCheckpoint>,
		snapshot_runtime: SnapshotRuntime,
	) -> std::io::Result<Self> {
		Self::from_recovery_with_metrics(snapshot, entries, checkpoint, snapshot_runtime, metrics())
	}

	fn from_recovery_with_metrics(
		snapshot: Option<LoadedSnapshot>,
		entries: Vec<Entry<WendelonTypeConfig>>,
		checkpoint: Option<RecoveryCheckpoint>,
		snapshot_runtime: SnapshotRuntime,
		observability_metrics: &'static MetricsRegistry,
	) -> std::io::Result<Self> {
		let (finalization_events, _) = broadcast::channel(FINALIZATION_BROADCAST_CAPACITY);
		let mut inner = SmInner::default();
		let snapshot_boundary = if let Some(snapshot) = snapshot.clone() {
			inner.state = snapshot.state;
			inner.state.last_applied_log_id = snapshot.held.meta.last_log_id;
			inner.state.last_membership = snapshot.held.meta.last_membership.clone();
			inner.snapshot = Some(snapshot.held.clone());
			inner.snapshot_seq = 1;
			snapshot.held.meta.last_log_id.map(|id| id.index)
		} else {
			None
		};

		let apply_upto = if let Some(cp) = checkpoint {
			if let Some(boundary) = snapshot_boundary {
				if cp.commit_index < boundary {
					return Err(Error::new(
						ErrorKind::InvalidData,
						format!(
							"recovery checkpoint commit_index {} is behind snapshot boundary {}",
							cp.commit_index, boundary
						),
					));
				}
			}
			cp.commit_index
		} else {
			snapshot_boundary.unwrap_or(0)
		};

		for entry in entries {
			if entry.log_id.index > apply_upto {
				break;
			}
			apply_entry_to_state(&mut inner.state, &entry);
			inner.state.last_applied_log_id = Some(match inner.state.last_applied_log_id {
				Some(prev) if prev > entry.log_id => prev,
				_ => entry.log_id,
			});
		}

		let admission_snapshot = watch::channel(admission_snapshot_from_state(&inner.state)).0;
		let revocation_snapshot = watch::channel(revocation_snapshot_from_state(&inner.state)).0;
		let mutation_drain = MutationDrainRegistry::recovering();
		for fence in inner.state.table_drop_fences.iter() {
			mutation_drain
				.close_committed(fence.operation_id)
				.map_err(|error| {
					Error::new(
						ErrorKind::InvalidData,
						format!("recovered DROP fence cannot seed mutation drain: {error}"),
					)
				})?;
		}
		for tombstone in inner.state.table_drop_tombstones.iter() {
			mutation_drain
				.close_committed(tombstone.operation_id)
				.map_err(|error| {
					Error::new(
						ErrorKind::InvalidData,
						format!("recovered DROP tombstone cannot seed mutation drain: {error}"),
					)
				})?;
		}
		observability_metrics.set_drop_reclaim_debts(inner.state.table_drop_reclaim_debts.len());
		Ok(Self {
			inner: Arc::new(RwLock::new(inner)),
			mutation_drain,
			snapshot_runtime,
			admission_snapshot,
			revocation_snapshot,
			external_revocation_snapshot: None,
			tier_observer: None,
			tenant_kek_observer: None,
			checkpoint_observer: None,
			audit_materializer: None,
			observability_node_id: None,
			observability_metrics,
			finalization_events,
			retention: None,
		})
	}

	/// Install a tier observer. Intended to be called once, at
	/// bootstrap, before the state machine is handed to openraft.
	/// Replacing an observer mid-run is not supported — callers
	/// should reconstruct the SM instead.
	pub fn with_tier_observer(mut self, observer: Arc<dyn TierObserver>) -> Self {
		self.tier_observer = Some(observer);
		self
	}

	pub fn with_tenant_kek_observer(mut self, observer: Arc<dyn TenantKekObserver>) -> Self {
		self.tenant_kek_observer = Some(observer);
		self
	}

	pub fn with_checkpoint_observer(mut self, observer: Arc<dyn CheckpointObserver>) -> Self {
		self.checkpoint_observer = Some(observer);
		self
	}

	pub fn with_external_revocation_snapshot(
		mut self,
		sender: watch::Sender<Arc<RevocationSnapshot>>,
	) -> Self {
		let _ = sender.send_replace(self.current_revocation_snapshot());
		self.external_revocation_snapshot = Some(sender);
		self
	}

	pub fn with_audit_materializer(
		mut self,
		local_node: SchemaNodeId,
		sink: Arc<dyn AuditSink>,
		tracker: Arc<AuditMaterializationTracker>,
	) -> Self {
		self.audit_materializer = Some(RaftAuditMaterializer {
			local_node,
			sink,
			tracker,
		});
		self
	}

	pub fn with_observability_node_id(mut self, local_node: SchemaNodeId) -> Self {
		self.observability_node_id = Some(local_node);
		self
	}

	/// Attaches the node's Control retention guard. Without one the state
	/// machine and its snapshot builder behave as `Compacting`.
	pub fn with_retention_guard(mut self, retention: Arc<RetentionGuard>) -> Self {
		self.retention = Some(retention);
		self
	}

	fn observability_metrics(&self) -> &MetricsRegistry {
		self.observability_metrics
	}

	#[cfg(test)]
	async fn with_observability_metrics_for_test(
		mut self,
		registry: &'static MetricsRegistry,
	) -> Self {
		self.observability_metrics = registry;
		registry
			.set_drop_reclaim_debts(self.inner.read().await.state.table_drop_reclaim_debts.len());
		self
	}

	pub fn subscribe_finalizations(&self) -> broadcast::Receiver<FinalizationMarker> {
		self.finalization_events.subscribe()
	}

	pub fn subscribe_admission_snapshot(&self) -> watch::Receiver<Arc<AdmissionSnapshot>> {
		self.admission_snapshot.subscribe()
	}

	pub fn current_admission_snapshot(&self) -> Arc<AdmissionSnapshot> {
		self.admission_snapshot.borrow().clone()
	}

	pub(crate) fn mutation_drain_registry(&self) -> Arc<MutationDrainRegistry> {
		self.mutation_drain.clone()
	}

	pub fn subscribe_revocation_snapshot(&self) -> watch::Receiver<Arc<RevocationSnapshot>> {
		self.revocation_snapshot.subscribe()
	}

	pub fn current_revocation_snapshot(&self) -> Arc<RevocationSnapshot> {
		self.revocation_snapshot.borrow().clone()
	}

	fn publish_admission_snapshot_if_changed(&self, state: &SmState) {
		let current_epoch = state.e_safe_watermarks.keys().copied().max().unwrap_or(0);
		let should_publish = {
			let current = self.admission_snapshot.borrow();
			admission_snapshot_changed(&current, state, current_epoch)
		};
		if should_publish {
			self.admission_snapshot
				.send_replace(admission_snapshot_from_state(state));
		}
	}

	fn publish_revocation_snapshot_if_changed(&self, state: &SmState) {
		let should_publish = {
			let current = self.revocation_snapshot.borrow();
			revocation_snapshot_changed(&current, state)
		};
		if should_publish {
			let snapshot = revocation_snapshot_from_state(state);
			self.revocation_snapshot.send_replace(snapshot.clone());
			if let Some(external) = &self.external_revocation_snapshot {
				external.send_replace(snapshot);
			}
		}
	}

	fn publish_live_snapshots_if_changed(&self, state: &SmState) {
		self.publish_admission_snapshot_if_changed(state);
		self.publish_revocation_snapshot_if_changed(state);
	}

	/// Test / caller inspection hook — return a cloned snapshot
	/// of the applied state without driving a full snapshot
	/// build round-trip.
	pub async fn state_snapshot(&self) -> SmState {
		self.inner.read().await.state.clone()
	}

	pub async fn tombstones_snapshot(&self) -> Vec<MigratedRangeTombstone> {
		self.inner.read().await.state.tombstones.clone()
	}

	pub async fn checkpoint_observation(&self, force: bool) -> Option<CheckpointObservation> {
		let guard = self.inner.read().await;
		checkpoint_observation_from_state(&guard.state, force)
	}

	pub async fn last_applied_log_index(&self) -> Option<u64> {
		self.inner
			.read()
			.await
			.state
			.last_applied_log_id
			.map(|id| id.index)
	}

	/// Test-only hook to seed the E_safe watermark for an epoch
	/// without routing a Finalize entry through openraft. Used by
	/// data-ring tests that want to exercise the LINEARIZABLE
	/// consistency gate in isolation; production code must never
	/// poke the SM state directly — all mutations belong to apply.
	#[cfg(test)]
	pub async fn seed_e_safe_for_test(&self, epoch: u64, hlc: HybridTimestamp) {
		let mut guard = self.inner.write().await;
		guard.state.e_safe_watermarks.insert(epoch, hlc);
		self.publish_live_snapshots_if_changed(&guard.state);
	}

	/// Test-only hook to seed a Migration Ledger entry without
	/// driving a `SeedMigration` through openraft. Lets data-ring
	/// tests exercise the lazy-migration merge path (Plan 019)
	/// against a controllable ledger shape.
	#[cfg(test)]
	pub async fn seed_migration_for_test(&self, entry: crate::consensus::MigrationLedgerEntry) {
		let mut guard = self.inner.write().await;
		guard.state.migrations.insert(entry.migration_id, entry);
	}

	#[cfg(test)]
	pub async fn seed_tiered_slab_for_test(&self, tier: crate::consensus::TieredSlab) {
		let mut guard = self.inner.write().await;
		guard.state.tiered_slabs.insert(tier.slab_id, tier);
	}

	#[cfg(test)]
	pub async fn seed_migrated_range_tombstone_for_test(
		&self,
		tombstone: crate::consensus::MigratedRangeTombstone,
	) {
		let mut guard = self.inner.write().await;
		guard.state.tombstones.push(tombstone);
	}

	#[cfg(test)]
	pub async fn retire_tiered_slab_for_test(&self, slab_id: SlabId) {
		let mut guard = self.inner.write().await;
		guard.state.tiered_slabs.remove(&slab_id);
	}
}

impl Default for WendelonStateMachine {
	fn default() -> Self {
		Self::new()
	}
}

impl RaftStateMachine<WendelonTypeConfig> for WendelonStateMachine {
	type SnapshotBuilder = WendelonSnapshotBuilder;

	async fn applied_state(
		&mut self,
	) -> Result<(Option<LogId<u32>>, StoredMembership<u32, BasicNode>), StorageError<u32>> {
		let guard = self.inner.read().await;
		Ok((
			guard.state.last_applied_log_id,
			guard.state.last_membership.clone(),
		))
	}

	async fn apply<I>(&mut self, entries: I) -> Result<Vec<WendelonResponse>, StorageError<u32>>
	where
		I: IntoIterator<Item = Entry<WendelonTypeConfig>> + OptionalSend,
		I::IntoIter: OptionalSend,
	{
		let mut out = Vec::new();
		// Tier observer callbacks run after the write lock is
		// released — they typically hit the filesystem (unlinking
		// the L1 slab file) and must not block apply progress for
		// other entries in the batch.
		let mut pending_audit_events: Vec<AuditEvent> = Vec::new();
		let mut pending_tier_events: Vec<(TierObserverEvent, u64)> = Vec::new();
		let mut pending_tenant_kek_events: Vec<(InstallTenantKekVersion, u64)> = Vec::new();
		let mut pending_tenant_kek_snapshot: Option<
			HashMap<crate::schema::TenantId, TenantKeyset>,
		> = None;
		let mut pending_finalizations: Vec<FinalizationMarker> = Vec::new();
		let mut pending_drop_settlements: Vec<TableDropSettlementRequest> = Vec::new();
		let pending_checkpoint = {
			let mut guard = self.inner.write().await;
			for entry in entries {
				let drop_prepare_operation = match &entry.payload {
					EntryPayload::Normal(WendelonRequest::TableDropPrepare(request)) => {
						Some(request.operation_id())
					}
					_ => None,
				};
				let tenant_kek_rotation = match &entry.payload {
					EntryPayload::Normal(WendelonRequest::InstallTenantKekVersion(rotation)) => {
						Some(rotation.clone())
					}
					_ => None,
				};
				let has_tenant_kek_rotation = tenant_kek_rotation.is_some();
				let mut requires_tenant_kek_snapshot_sync = matches!(
					&entry.payload,
					EntryPayload::Normal(WendelonRequest::CreateTenant(_))
				);
				if matches!(
					&entry.payload,
					EntryPayload::Normal(WendelonRequest::RetireTenantKekVersion(_))
				) {
					requires_tenant_kek_snapshot_sync = true;
				}
				let retention_audit_context = retention_audit_context(&guard.state, &entry);
				let schema_audit_context = schema_audit_context(&guard.state, &entry);
				let mut index_schema_audit_context =
					index_schema_audit_context(&guard.state, &entry);
				let (resp, tier) = apply_entry_to_state(&mut guard.state, &entry);
				// Live apply is the total Control Ring substrate numerator.
				// Recovery and snapshot installation use separate paths and
				// must not manufacture process-lifetime work (ADR 080).
				metrics().raft_entries_applied_total.inc();
				if let Some(context) = index_schema_audit_context.as_mut() {
					finalize_index_schema_audit_context(&guard.state, context);
				}
				let applied_successfully = matches!(&resp, WendelonResponse::Success);
				if applied_successfully
					&& let EntryPayload::Normal(WendelonRequest::TableDropSettlement(request)) =
						&entry.payload
				{
					pending_drop_settlements.push(request.clone());
				}
				if applied_successfully && let Some(operation_id) = drop_prepare_operation {
					self.mutation_drain
						.close_committed(operation_id)
						.expect("a committed DROP fence cannot conflict in the mutation registry");
				}
				guard.state.last_applied_log_id = Some(match guard.state.last_applied_log_id {
					Some(prev) if prev > entry.log_id => prev,
					_ => entry.log_id,
				});
				if let Some(t) = tier {
					pending_tier_events.push((t, entry.log_id.index));
				}
				if let Some(rotation) = tenant_kek_rotation {
					if applied_successfully {
						pending_tenant_kek_events.push((rotation, entry.log_id.index));
					}
				}
				// CreateTenant and destructive tenant-KEK retirement update
				// the authoritative keyset without an incremental observer
				// callback. Rotation also snapshots here so local crypto can
				// converge even if incremental apply missed a commit.
				if (requires_tenant_kek_snapshot_sync || has_tenant_kek_rotation)
					&& applied_successfully
				{
					pending_tenant_kek_snapshot = Some(guard.state.tenant_keysets.clone());
				}
				if let Some(materializer) = self.audit_materializer.as_ref() {
					if let Some(event) = raft_audit_event_with_context(
						materializer.local_node,
						&entry,
						&resp,
						retention_audit_context,
						schema_audit_context,
						index_schema_audit_context,
					) {
						materializer.tracker.note_authoritative_event();
						pending_audit_events.push(event);
					}
				}
				if let Some(marker) = canonical_finalization_marker(&entry) {
					pending_finalizations.push(marker);
				}
				out.push(resp);
			}
			// Publish under the apply lock so watch observers cannot
			// observe snapshots out of order relative to apply order.
			self.publish_live_snapshots_if_changed(&guard.state);
			self.observability_metrics()
				.set_drop_reclaim_debts(guard.state.table_drop_reclaim_debts.len());
			checkpoint_observation_from_state(&guard.state, false)
		};
		let pending_checkpoint = pending_checkpoint;
		if let Some(materializer) = self.audit_materializer.as_ref() {
			for event in pending_audit_events {
				let _ = materializer.emit(event);
			}
		}
		if let Some(observer) = self.tier_observer.as_ref() {
			for (event, applied_log_index) in &pending_tier_events {
				// An observer error is surfaced through logs but
				// must not tear the state machine down — the SM
				// has already committed the pivot and followers
				// must not diverge on a local filesystem hiccup.
				let result = match event {
					TierObserverEvent::Tiered(tier) => observer
						.on_slab_tiered(tier, *applied_log_index)
						.await
						.map_err(|error| (tier.slab_id, error)),
					TierObserverEvent::Retired(slab_id) => observer
						.on_slab_retired(*slab_id, *applied_log_index)
						.await
						.map_err(|error| (*slab_id, error)),
					TierObserverEvent::MigratedRangeTombstone(tombstone) => observer
						.on_migrated_range_tombstone(tombstone, *applied_log_index)
						.await
						.map_err(|error| (SlabId::new(0), error)),
					TierObserverEvent::LsmCompactionCommitted(commit) => observer
						.on_lsm_compaction_committed(commit, *applied_log_index)
						.await
						.map_err(|error| (SlabId::new(0), error)),
				};
				if let Err((slab_id, error)) = result {
					tracing::warn!(
						slab_id = slab_id.get(),
						error = %error,
						"tier observer failed; sweeper reconcile will retry local tier cleanup"
					);
				}
			}
		}
		if let Some(observer) = self.tenant_kek_observer.as_ref() {
			for (rotation, applied_log_index) in &pending_tenant_kek_events {
				if let Err(err) = observer
					.on_tenant_kek_rotated(rotation, *applied_log_index)
					.await
				{
					tracing::warn!(
						tenant_id = rotation.tenant_id.get(),
						tenant_kek_id = rotation.tenant_kek_id.get(),
						error = %err,
						"tenant KEK observer incremental apply failed; reconciling from committed keyset snapshot"
					);
				}
			}
			if let Some(snapshot) = pending_tenant_kek_snapshot.as_ref() {
				if let Err(err) = observer.sync_tenant_keysets(snapshot).await {
					tracing::warn!(
						tenants = snapshot.len(),
						error = %err,
						"tenant KEK observer snapshot sync failed; local crypto caches may remain stale until the next rotation or restart"
					);
				}
			}
		}
		for marker in pending_finalizations {
			let _ = self.finalization_events.send(marker);
		}
		for settlement in pending_drop_settlements {
			self.observability_metrics()
				.observe_drop_reclaim_settlement();
			tracing::info!(
				tenant_id = settlement.operation_id.tenant_id.get(),
				table_object_id = ?settlement.operation_id.table_object_id,
				schema_generation = settlement.operation_id.schema_generation.get(),
				finalize_log_index = settlement.finalize_log_index,
				participant_count = settlement.attested_participants.len(),
				ownership_epoch = settlement.ownership_epoch,
				local_node_id = ?self.observability_node_id.map(|node| node.id),
				"table drop physical reclaim settled"
			);
		}
		if let (Some(observer), Some(observation)) =
			(self.checkpoint_observer.as_ref(), pending_checkpoint)
		{
			if let Err(err) = observer.on_checkpoint_progress(observation).await {
				tracing::warn!(
					commit_index = observation.checkpoint.commit_index,
					e_safe_index = observation.checkpoint.e_safe_index,
					error = %err,
					"checkpoint observer failed; periodic emission will retry"
				);
			}
		}
		Ok(out)
	}

	async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
		WendelonSnapshotBuilder {
			inner: self.inner.clone(),
			snapshot_runtime: self.snapshot_runtime.clone(),
			checkpoint_observer: self.checkpoint_observer.clone(),
			retention: self.retention.clone(),
		}
	}

	async fn begin_receiving_snapshot(
		&mut self,
	) -> Result<Box<SnapshotDataFile>, StorageError<u32>> {
		if retention::refuses(&self.retention, RetentionViolation::SnapshotReceive) {
			return Err(StorageIOError::write_snapshot(
				None,
				AnyError::new(&RetentionViolation::SnapshotReceive),
			)
			.into());
		}
		ensure_snapshot_parent_dir(
			self.snapshot_runtime.file_ops.as_ref(),
			&self.snapshot_runtime.paths.receive_tmp,
		)
		.await
		.map_err(|err| StorageIOError::write_snapshot(None, AnyError::new(&err)))?;
		let snapshot = SnapshotDataFile::create(
			self.snapshot_runtime.file_ops.clone(),
			self.snapshot_runtime.paths.receive_tmp.clone(),
		)
		.await
		.map_err(|err| StorageIOError::write_snapshot(None, AnyError::new(&err)))?;
		Ok(Box::new(snapshot))
	}

	async fn install_snapshot(
		&mut self,
		meta: &SnapshotMeta<u32, BasicNode>,
		mut snapshot: Box<SnapshotDataFile>,
	) -> Result<(), StorageError<u32>> {
		if retention::refuses(&self.retention, RetentionViolation::SnapshotInstall) {
			return Err(StorageIOError::write_snapshot(
				Some(meta.signature()),
				AnyError::new(&RetentionViolation::SnapshotInstall),
			)
			.into());
		}
		let receive_path = if snapshot.path() == self.snapshot_runtime.paths.receive_tmp.as_path() {
			sync_snapshot_handle(
				snapshot.file_ops.clone(),
				snapshot.handle.clone(),
				snapshot.logical_len,
			)
			.await
			.map_err(|err| {
				StorageIOError::write_snapshot(Some(meta.signature()), AnyError::new(&err))
			})?;
			let receive_path = snapshot.path().to_path_buf();
			drop(snapshot);
			receive_path
		} else {
			ensure_snapshot_parent_dir(
				self.snapshot_runtime.file_ops.as_ref(),
				&self.snapshot_runtime.paths.receive_tmp,
			)
			.await
			.map_err(|err| {
				StorageIOError::write_snapshot(Some(meta.signature()), AnyError::new(&err))
			})?;
			let mut receive = SnapshotDataFile::create(
				self.snapshot_runtime.file_ops.clone(),
				self.snapshot_runtime.paths.receive_tmp.clone(),
			)
			.await
			.map_err(|err| {
				StorageIOError::write_snapshot(Some(meta.signature()), AnyError::new(&err))
			})?;
			tokio::io::copy(&mut *snapshot, &mut receive)
				.await
				.map_err(|err| {
					StorageIOError::write_snapshot(Some(meta.signature()), AnyError::new(&err))
				})?;
			sync_snapshot_handle(
				receive.file_ops.clone(),
				receive.handle.clone(),
				receive.logical_len,
			)
			.await
			.map_err(|err| {
				StorageIOError::write_snapshot(Some(meta.signature()), AnyError::new(&err))
			})?;
			let receive_path = receive.path().to_path_buf();
			drop(receive);
			drop(snapshot);
			receive_path
		};
		let (header, mut state) =
			decode_snapshot_file(self.snapshot_runtime.file_ops.as_ref(), &receive_path)
				.await
				.map_err(|err| {
					StorageIOError::read_snapshot(Some(meta.signature()), AnyError::new(&err))
				})?;
		validate_snapshot_header_log_coordinates(&header, state.last_applied_log_id).map_err(
			|err| {
				StorageIOError::read_snapshot(
					Some(meta.signature()),
					AnyError::error(format!("snapshot header/body identity mismatch: {err}")),
				)
			},
		)?;
		if state.last_applied_log_id != meta.last_log_id {
			return Err(StorageIOError::read_snapshot(
				Some(meta.signature()),
				AnyError::error(format!(
					"snapshot body last_applied {:?} disagrees with install meta {:?}",
					state.last_applied_log_id, meta.last_log_id
				)),
			)
			.into());
		}
		if state.last_membership != meta.last_membership {
			return Err(StorageIOError::read_snapshot(
				Some(meta.signature()),
				AnyError::error("snapshot membership disagrees with install meta"),
			)
			.into());
		}
		self.snapshot_runtime
			.file_ops
			.rename(&receive_path, &self.snapshot_runtime.paths.current)
			.await
			.map_err(|err| {
				StorageIOError::write_snapshot(Some(meta.signature()), AnyError::new(&err))
			})?;
		fsync_parent_dir(
			self.snapshot_runtime.file_ops.as_ref(),
			&self.snapshot_runtime.paths.current,
		)
		.await
		.map_err(|err| {
			StorageIOError::write_snapshot(Some(meta.signature()), AnyError::new(&err))
		})?;
		let pending_checkpoint = {
			let mut guard = self.inner.write().await;
			state.last_applied_log_id = meta.last_log_id;
			state.last_membership = meta.last_membership.clone();
			for fence in state.table_drop_fences.iter() {
				self.mutation_drain
					.close_committed(fence.operation_id)
					.map_err(|error| {
						StorageIOError::read_snapshot(
							Some(meta.signature()),
							AnyError::error(format!(
								"snapshot DROP fence conflicts with mutation drain state: {error}"
							)),
						)
					})?;
			}
			for tombstone in state.table_drop_tombstones.iter() {
				self.mutation_drain
					.close_committed(tombstone.operation_id)
					.map_err(|error| {
						StorageIOError::read_snapshot(
							Some(meta.signature()),
							AnyError::error(format!(
								"snapshot DROP tombstone conflicts with mutation drain state: {error}"
							)),
						)
					})?;
			}
			guard.state = state;
			guard.snapshot = Some(HeldSnapshot {
				meta: meta.clone(),
				path: self.snapshot_runtime.paths.current.clone(),
			});
			// Publish under the state lock so install-snapshot updates
			// stay monotonic for admission-snapshot observers.
			self.publish_live_snapshots_if_changed(&guard.state);
			self.observability_metrics()
				.set_drop_reclaim_debts(guard.state.table_drop_reclaim_debts.len());
			checkpoint_observation_from_state(&guard.state, true)
		};
		let pending_checkpoint = pending_checkpoint;
		if let (Some(observer), Some(observation)) =
			(self.checkpoint_observer.as_ref(), pending_checkpoint)
		{
			if let Err(err) = observer.on_checkpoint_progress(observation).await {
				tracing::warn!(
					commit_index = observation.checkpoint.commit_index,
					e_safe_index = observation.checkpoint.e_safe_index,
					error = %err,
					"checkpoint observer failed during snapshot install"
				);
			}
		}
		Ok(())
	}

	async fn get_current_snapshot(
		&mut self,
	) -> Result<Option<Snapshot<WendelonTypeConfig>>, StorageError<u32>> {
		let snapshot = {
			let guard = self.inner.read().await;
			guard.snapshot.clone()
		};
		let Some(snapshot) = snapshot else {
			return Ok(None);
		};
		let stream = SnapshotDataFile::open(
			self.snapshot_runtime.file_ops.clone(),
			snapshot.path.clone(),
		)
		.await
		.map_err(|err| {
			StorageIOError::read_snapshot(Some(snapshot.meta.signature()), AnyError::new(&err))
		})?;
		Ok(Some(Snapshot {
			meta: snapshot.meta,
			snapshot: Box::new(stream),
		}))
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum TierObserverEvent {
	Tiered(TieredSlab),
	Retired(SlabId),
	MigratedRangeTombstone(MigratedRangeTombstone),
	LsmCompactionCommitted(CompactionCommit),
}

/// Fold one committed [`WendelonRequest`] into the SM state.
///
/// Returns the [`WendelonResponse`] plus an optional tier-state
/// change so the caller can fire tier observers outside the write
/// lock. Tier observers do filesystem work (unlinking the L1 file);
/// keeping them off the lock prevents any cross-node staleness on a
/// busy apply batch.
fn apply_entry_to_state(
	state: &mut SmState,
	entry: &Entry<WendelonTypeConfig>,
) -> (WendelonResponse, Option<TierObserverEvent>) {
	match &entry.payload {
		EntryPayload::Blank => (WendelonResponse::Success, None),
		EntryPayload::Normal(req) => {
			apply_request_at_log_index(state, req.clone(), entry.log_id.index)
		}
		EntryPayload::Membership(membership) => {
			state.last_membership = StoredMembership::new(Some(entry.log_id), membership.clone());
			let voters = membership.voter_ids().collect::<BTreeSet<_>>();
			for (tenant_id, table_object_id, index_object_id) in state
				.secondary_indexes
				.reconcile_backfill_readiness(&voters)
			{
				metrics().observe_storage_secondary_index_lifecycle_transition(
					tenant_id,
					table_object_id,
					index_object_id,
					"building",
					"ready",
				);
			}
			(WendelonResponse::Success, None)
		}
	}
}

#[derive(Clone, Copy, Debug)]
struct RetentionAuditContext {
	previous_policy: Option<TableRetentionPolicy>,
}

#[derive(Clone, Debug)]
struct SchemaAuditContext {
	envelope: SchemaUpdateEnvelope,
	descriptor: TableSchemaDescriptor,
	previous_descriptor: Option<TableSchemaDescriptor>,
}

#[derive(Clone, Debug)]
struct IndexSchemaAuditContext {
	envelope: SchemaUpdateEnvelope,
	index_object_id: IndexObjectId,
	descriptor: Option<SecondaryIndexDescriptor>,
	previous_descriptor: Option<SecondaryIndexDescriptor>,
	previous_state: Option<SecondaryIndexState>,
	result_descriptor: Option<SecondaryIndexDescriptor>,
	result_state: Option<SecondaryIndexState>,
	replica_id: Option<SchemaNodeId>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DescriptorMutationAxis {
	LogicalOnly,
	PhysicalOnly,
	Combined,
}

impl DescriptorMutationAxis {
	fn as_str(self) -> &'static str {
		match self {
			Self::LogicalOnly => "logical_only",
			Self::PhysicalOnly => "physical_only",
			Self::Combined => "combined",
		}
	}
}

fn retention_audit_context(
	state: &SmState,
	entry: &Entry<WendelonTypeConfig>,
) -> Option<RetentionAuditContext> {
	let EntryPayload::Normal(WendelonRequest::SetTableRetentionPolicy(update)) = &entry.payload
	else {
		return None;
	};
	Some(RetentionAuditContext {
		previous_policy: state
			.table_lifecycle
			.retention_policy(update.tenant_id, update.table_object_id)
			.copied()
			.or_else(|| {
				state
					.table_schemas
					.latest_by_id(update.tenant_id, update.table_object_id)
					.map(|_| TableRetentionPolicy::InheritCluster)
			}),
	})
}

fn schema_audit_context(
	state: &SmState,
	entry: &Entry<WendelonTypeConfig>,
) -> Option<SchemaAuditContext> {
	let EntryPayload::Normal(WendelonRequest::SchemaUpdate(bytes)) = &entry.payload else {
		return None;
	};
	let envelope = decode_schema_update_envelope_v1(bytes).ok()?;
	if envelope.object.kind != SchemaObjectKind::Table
		|| !matches!(
			envelope.mutation,
			SchemaMutationKind::Create | SchemaMutationKind::Alter
		) {
		return None;
	}
	let descriptor = decode_table_schema_descriptor_v1(&envelope.payload).ok()?;
	let previous_descriptor = state
		.table_schemas
		.latest_by_name(
			envelope.tenant_id,
			envelope.object.namespace.as_str(),
			envelope.object.name.as_str(),
		)
		.cloned();
	Some(SchemaAuditContext {
		envelope,
		descriptor,
		previous_descriptor,
	})
}

fn index_schema_audit_context(
	state: &SmState,
	entry: &Entry<WendelonTypeConfig>,
) -> Option<IndexSchemaAuditContext> {
	let EntryPayload::Normal(WendelonRequest::SchemaUpdate(bytes)) = &entry.payload else {
		return None;
	};
	let envelope = decode_schema_update_envelope_v1(bytes).ok()?;
	if envelope.object.kind != SchemaObjectKind::Index {
		return None;
	}
	let (index_object_id, descriptor, replica_id) = match envelope.mutation {
		SchemaMutationKind::Create => {
			let descriptor = decode_secondary_index_descriptor(&envelope.payload).ok()?;
			(descriptor.index_object_id, Some(descriptor), None)
		}
		SchemaMutationKind::Drop => {
			let request = decode_secondary_index_drop_request(&envelope.payload).ok()?;
			(request.index_object_id, None, None)
		}
		SchemaMutationKind::IndexBackfillComplete | SchemaMutationKind::IndexDropGcComplete => {
			let complete = decode_secondary_index_lifecycle_complete(&envelope.payload).ok()?;
			(complete.index_object_id, None, Some(complete.replica_id))
		}
		SchemaMutationKind::Alter => return None,
	};
	let previous = state
		.secondary_indexes
		.get(envelope.tenant_id, index_object_id);
	let previous_descriptor = previous.map(|row| row.descriptor.clone());
	let previous_state = previous.map(|row| row.state);
	Some(IndexSchemaAuditContext {
		envelope,
		index_object_id,
		descriptor,
		previous_descriptor,
		previous_state,
		result_descriptor: None,
		result_state: None,
		replica_id,
	})
}

fn finalize_index_schema_audit_context(state: &SmState, context: &mut IndexSchemaAuditContext) {
	if let Some(row) = state
		.secondary_indexes
		.get(context.envelope.tenant_id, context.index_object_id)
	{
		context.result_descriptor = Some(row.descriptor.clone());
		context.result_state = Some(row.state);
	}
}

#[cfg(test)]
fn raft_audit_event(
	local_node: SchemaNodeId,
	entry: &Entry<WendelonTypeConfig>,
	resp: &WendelonResponse,
) -> Option<AuditEvent> {
	raft_audit_event_with_context(local_node, entry, resp, None, None, None)
}

fn raft_audit_event_with_context(
	local_node: SchemaNodeId,
	entry: &Entry<WendelonTypeConfig>,
	resp: &WendelonResponse,
	retention_context: Option<RetentionAuditContext>,
	schema_context: Option<SchemaAuditContext>,
	index_context: Option<IndexSchemaAuditContext>,
) -> Option<AuditEvent> {
	let EntryPayload::Normal(req) = &entry.payload else {
		return None;
	};
	let is_err = matches!(resp, WendelonResponse::Error(_));
	let outcome = audit_outcome(resp);
	match req {
		// Non-tenant-admin mutations emit on success only; the apply-layer
		// can reject for transient state-machine reasons and an error
		// record per replay would flood the audit log.
		WendelonRequest::Finalize(_) if is_err => None,
		WendelonRequest::Finalize(finalization)
			if is_cdc_locality_heartbeat_finalization(finalization) =>
		{
			None
		}
		WendelonRequest::AdvanceEpoch(_)
			| WendelonRequest::InstallTenantKekVersion(_)
			| WendelonRequest::RetireTenantKekVersion(_)
			| WendelonRequest::RevokeActorKey(_)
			if is_err =>
		{
			None
		}
		WendelonRequest::Finalize(finalization) => Some(
			AuditEvent::new(
				AuditAuthority::RaftLog,
				AuditOperation::DataWriteFinalized,
				local_node,
				format!(
					"raft_log_index={} raft_term={} tenant_id={} locality={} durability={} charged_bytes={} record_count={} commit_hlc={:?} payload_hash={}",
					entry.log_id.index,
					entry.log_id.leader_id.term,
					finalization.tenant_id.get(),
					Locality::from(finalization.key_identity).get(),
					finalization.marker.durability.0,
					finalization.charged_bytes,
					finalization.record_count,
					finalization.marker.commit_hlc,
					format_bytes_hex(&finalization.marker.payload_hash),
				),
			)
			.with_outcome(outcome)
			.with_raft_log_index(entry.log_id.index)
			.with_parameter("raft_term", entry.log_id.leader_id.term)
			.with_parameter("tenant_id", finalization.tenant_id.get())
			.with_parameter("locality", Locality::from(finalization.key_identity).get())
			.with_parameter("durability", finalization.marker.durability.0 as u64)
			.with_parameter("charged_bytes", finalization.charged_bytes)
			.with_parameter("record_count", finalization.record_count as u64)
			.with_parameter(
				"commit_hlc",
				format!("{:?}", finalization.marker.commit_hlc),
			)
			.with_parameter(
				"payload_hash",
				format_bytes_hex(&finalization.marker.payload_hash),
			)
			.with_schema(AuditSchema::DataAccess {
				access_kind: DataAccessKind::Write,
				phase: DataAccessPhase::Finalized,
				detail: DataAccessSchema {
					locality: Some(Locality::from(finalization.key_identity).get()),
					locality_end: None,
					durability: Some(finalization.marker.durability.0 as u64),
					charged_bytes: Some(finalization.charged_bytes),
					returned_bytes: None,
					result_count: Some(finalization.record_count as u64),
				},
			})
			.with_tenant(finalization.tenant_id)
			.with_record_identity(finalization.key_identity),
		),
		WendelonRequest::TierSlab(_) | WendelonRequest::RetireTieredSlab(_) => None,
		WendelonRequest::CompactionCommit(commit) => {
			let op = if is_err {
				AuditOperation::LsmInvariantViolation
			} else {
				AuditOperation::LsmCompactionCommitted
			};
			let output_level = commit
				.slabs_to_add
				.first()
				.map(|run| run.compaction_level.get() as u64)
				.unwrap_or(0);
			let bytes_out = commit
				.slabs_to_add
				.iter()
				.map(|run| run.size_bytes.get())
				.sum::<u64>();
			let rows_out = commit
				.slabs_to_add
				.iter()
				.map(|run| run.row_count.get())
				.sum::<u64>();
			Some(
				AuditEvent::new(
					AuditAuthority::RaftLog,
					op,
					local_node,
					format!(
						"raft_log_index={} raft_term={} compaction_job_id={} manifest_epoch_pre={} input_slab_count={} output_slab_count={} output_level={} bytes_out={} rows_out={}",
						entry.log_id.index,
						entry.log_id.leader_id.term,
						commit.job_id.get(),
						commit.manifest_epoch_pre.get(),
						commit.slabs_to_retire.len(),
						commit.slabs_to_add.len(),
						output_level,
						bytes_out,
						rows_out,
					),
				)
				.with_outcome(outcome)
				.with_raft_log_index(entry.log_id.index)
				.with_parameter("raft_term", entry.log_id.leader_id.term)
				.with_parameter("compaction_job_id", commit.job_id.get())
				.with_parameter("manifest_epoch_pre", commit.manifest_epoch_pre.get())
				.with_parameter("input_slab_count", commit.slabs_to_retire.len() as u64)
				.with_parameter("output_slab_count", commit.slabs_to_add.len() as u64)
				.with_parameter("output_level", output_level)
				.with_parameter("bytes_out", bytes_out)
				.with_parameter("rows_out", rows_out),
			)
		}
		WendelonRequest::AdvanceEpoch(record) => Some(
			AuditEvent::new(
				AuditAuthority::RaftLog,
				AuditOperation::ControlRingEpochAdvanced,
				local_node,
				format!(
					"raft_log_index={} raft_term={} from_epoch={} to_epoch={} from_peers=[{}] to_peers=[{}]",
					entry.log_id.index,
					entry.log_id.leader_id.term,
					record.from.epoch,
					record.to.epoch,
					format_epoch_peer_ids(&record.from),
					format_epoch_peer_ids(&record.to),
				),
			)
			.with_outcome(outcome)
			.with_raft_log_index(entry.log_id.index)
			.with_parameter("raft_term", entry.log_id.leader_id.term)
			.with_parameter("from_epoch", record.from.epoch)
			.with_parameter("to_epoch", record.to.epoch)
			.with_parameter("from_peers", format_epoch_peer_ids(&record.from))
			.with_parameter("to_peers", format_epoch_peer_ids(&record.to))
			.with_schema(AuditSchema::AdministrativeControl {
				action: AdministrativeControlAction::AdvanceEpoch,
				detail: AdministrativeControlSchema {
					from_epoch: Some(record.from.epoch),
					to_epoch: Some(record.to.epoch),
					..AdministrativeControlSchema::default()
				},
			}),
		),
		WendelonRequest::InstallTenantKekVersion(rotation) => Some(
			AuditEvent::new(
				AuditAuthority::RaftLog,
				AuditOperation::CryptoTenantKekRotated,
				local_node,
				format!(
					"raft_log_index={} raft_term={} tenant_id={} tenant_kek_id={}",
					entry.log_id.index,
					entry.log_id.leader_id.term,
					rotation.tenant_id.get(),
					rotation.tenant_kek_id.get(),
				),
			)
			.with_outcome(outcome)
			.with_raft_log_index(entry.log_id.index)
			.with_parameter("raft_term", entry.log_id.leader_id.term)
			.with_parameter("tenant_id", rotation.tenant_id.get())
			.with_parameter("tenant_kek_id", rotation.tenant_kek_id.get())
			.with_schema(AuditSchema::AdministrativeControl {
				action: AdministrativeControlAction::TenantKekRotate,
				detail: AdministrativeControlSchema {
					target_tenant_id: Some(rotation.tenant_id.get()),
					target_tenant_kek_id: Some(rotation.tenant_kek_id.get()),
					..AdministrativeControlSchema::default()
				},
			}),
		),
		WendelonRequest::RetireTenantKekVersion(retirement) => Some(
			AuditEvent::new(
				AuditAuthority::RaftLog,
				AuditOperation::CryptoTenantKekRetired,
				local_node,
				format!(
					"raft_log_index={} raft_term={} tenant_id={} tenant_kek_id={}",
					entry.log_id.index,
					entry.log_id.leader_id.term,
					retirement.tenant_id.get(),
					retirement.tenant_kek_id.get(),
				),
			)
			.with_outcome(outcome)
			.with_raft_log_index(entry.log_id.index)
			.with_parameter("raft_term", entry.log_id.leader_id.term)
			.with_parameter("tenant_id", retirement.tenant_id.get())
			.with_parameter("tenant_kek_id", retirement.tenant_kek_id.get())
			.with_schema(AuditSchema::AdministrativeControl {
				action: AdministrativeControlAction::TenantKekRetire,
				detail: AdministrativeControlSchema {
					target_tenant_id: Some(retirement.tenant_id.get()),
					target_tenant_kek_id: Some(retirement.tenant_kek_id.get()),
					..AdministrativeControlSchema::default()
				},
			}),
		),
		WendelonRequest::RevokeActorKey(revocation) => Some(
			AuditEvent::new(
				AuditAuthority::RaftLog,
				AuditOperation::IdentityActorKeyRevoked,
				local_node,
				format!(
					"raft_log_index={} raft_term={} trust_domain={} key_id={}",
					entry.log_id.index,
					entry.log_id.leader_id.term,
					revocation.trust_domain,
					format_bytes_hex(&revocation.key_id),
				),
			)
			.with_outcome(outcome)
			.with_raft_log_index(entry.log_id.index)
			.with_parameter("raft_term", entry.log_id.leader_id.term)
			.with_parameter("trust_domain", revocation.trust_domain.to_string())
			.with_parameter("key_id", format_bytes_hex(&revocation.key_id))
			.with_schema(AuditSchema::AdministrativeControl {
				action: AdministrativeControlAction::ActorKeyRevoke,
				detail: AdministrativeControlSchema {
					target_trust_domain: Some(revocation.trust_domain.to_string()),
					target_key_id: Some(format_bytes_hex(&revocation.key_id)),
					..AdministrativeControlSchema::default()
				},
			}),
		),
		// Tenant admin ops are attempt-audited per ADR 021 §5: every
		// mutation — including rejected ones — emits a record so the
		// audit stream can be reconciled against the tenant FSM.
		WendelonRequest::CreateTenant(create) => Some(
			AuditEvent::new(
				AuditAuthority::RaftLog,
				AuditOperation::TenantCreated,
				local_node,
				format!(
					"raft_log_index={} raft_term={} tenant_id={} quota_bytes={} replication_factor={} tenant_kek_id={} {}",
					entry.log_id.index,
					entry.log_id.leader_id.term,
					create.tenant_id.get(),
					create.quota_bytes,
					create.replication_factor,
					create
						.tenant_keyset
						.active_tenant_kek_id
						.map(|id| id.get())
						.unwrap_or(0),
					audit_outcome_message(resp),
				),
			)
			.with_outcome(outcome)
			.with_raft_log_index(entry.log_id.index)
			.with_parameter("raft_term", entry.log_id.leader_id.term)
			.with_parameter("tenant_id", create.tenant_id.get())
			.with_parameter("quota_bytes", create.quota_bytes)
			.with_parameter("replication_factor", create.replication_factor as u64)
			.with_parameter(
				"tenant_kek_id",
				create
					.tenant_keyset
					.active_tenant_kek_id
					.map(|id| id.get())
					.unwrap_or(0),
			)
			.with_schema(AuditSchema::AdministrativeControl {
				action: AdministrativeControlAction::TenantCreate,
				detail: AdministrativeControlSchema {
					target_tenant_id: Some(create.tenant_id.get()),
					target_tenant_kek_id: create
						.tenant_keyset
						.active_tenant_kek_id
						.map(|id| id.get()),
					..AdministrativeControlSchema::default()
				},
			})
			.with_parameters(audit_outcome_parameters(resp)),
		),
		WendelonRequest::UpdateTenantQuota(update) => Some(
			AuditEvent::new(
				AuditAuthority::RaftLog,
				AuditOperation::TenantQuotaUpdated,
				local_node,
				format!(
					"raft_log_index={} raft_term={} tenant_id={} quota_bytes={} {}",
					entry.log_id.index,
					entry.log_id.leader_id.term,
					update.tenant_id.get(),
					update.quota_bytes,
					audit_outcome_message(resp),
				),
			)
			.with_outcome(outcome)
			.with_raft_log_index(entry.log_id.index)
			.with_parameter("raft_term", entry.log_id.leader_id.term)
			.with_parameter("tenant_id", update.tenant_id.get())
			.with_parameter("quota_bytes", update.quota_bytes)
			.with_schema(AuditSchema::AdministrativeControl {
				action: AdministrativeControlAction::TenantQuotaUpdate,
				detail: AdministrativeControlSchema {
					target_tenant_id: Some(update.tenant_id.get()),
					..AdministrativeControlSchema::default()
				},
			})
			.with_parameters(audit_outcome_parameters(resp)),
		),
		WendelonRequest::SetTableRetentionPolicy(update) => {
			let table_object_id = format_bytes_hex(update.table_object_id.as_bytes());
			let retention_policy = table_retention_policy_label(update.new_policy);
			let retention_window_ms = table_retention_policy_window_ms(update.new_policy);
			let previous_policy = retention_context.and_then(|context| context.previous_policy);
			let previous_retention_policy = previous_policy.map(table_retention_policy_label);
			let previous_retention_window_ms =
				previous_policy.and_then(table_retention_policy_window_ms);
			let retention_extends_window = previous_policy.is_some_and(|previous| {
				retention_update_extends_window(previous, update.new_policy)
			});
			Some(
				AuditEvent::new(
					AuditAuthority::RaftLog,
					AuditOperation::TableRetentionPolicyUpdated,
					local_node,
					format!(
						"raft_log_index={} raft_term={} tenant_id={} table_object_id={} expected_policy_generation={} previous_retention_policy={} retention_policy={} retention_extends_window={} retention_extension_restores_history=false updated_at_hlc={:?} {}",
						entry.log_id.index,
						entry.log_id.leader_id.term,
						update.tenant_id.get(),
						table_object_id,
						update.expected_policy_generation.get(),
						previous_retention_policy.unwrap_or("unknown"),
						retention_policy,
						retention_extends_window,
						update.updated_at_hlc,
						audit_outcome_message(resp),
					),
				)
				.with_outcome(outcome)
				.with_raft_log_index(entry.log_id.index)
				.with_parameter("raft_term", entry.log_id.leader_id.term)
				.with_parameter("tenant_id", update.tenant_id.get())
				.with_parameter("table_object_id", table_object_id.clone())
				.with_parameter(
					"expected_policy_generation",
					update.expected_policy_generation.get(),
				)
				.with_parameter(
					"previous_retention_policy",
					previous_retention_policy.unwrap_or("unknown"),
				)
				.with_parameter("retention_policy", retention_policy)
				.with_parameter("updated_at_hlc", format!("{:?}", update.updated_at_hlc))
				.with_parameter("retention_extends_window", retention_extends_window)
				.with_parameter("retention_extension_restores_history", false)
				.with_schema(AuditSchema::AdministrativeControl {
					action: AdministrativeControlAction::TableRetentionPolicyUpdate,
					detail: AdministrativeControlSchema {
						target_tenant_id: Some(update.tenant_id.get()),
						target_table_object_id: Some(table_object_id),
						policy_generation: Some(update.expected_policy_generation.get()),
						retention_policy: Some(retention_policy.to_string()),
						retention_window_ms,
						previous_retention_policy: previous_retention_policy.map(str::to_string),
						previous_retention_window_ms,
						retention_extends_window: Some(retention_extends_window),
						retention_extension_restores_history: Some(false),
						..AdministrativeControlSchema::default()
					},
				})
				.with_parameters(audit_outcome_parameters(resp)),
			)
		},
		WendelonRequest::SchemaUpdate(_) => {
			if let Some(context) = schema_context {
				let table_object_id =
					format_bytes_hex(context.descriptor.table_object_id.as_bytes());
				let schema_mutation = schema_mutation_label(context.envelope.mutation);
				let descriptor_axis = descriptor_mutation_axis(
					context.previous_descriptor.as_ref(),
					&context.descriptor,
				)
				.map(DescriptorMutationAxis::as_str)
				.unwrap_or("unchanged");
				let logical_schema_fingerprint =
					format_bytes_hex(context.descriptor.logical_schema_fingerprint.as_bytes());
				let physical_descriptor_fingerprint =
					format_bytes_hex(context.descriptor.physical_descriptor_fingerprint.as_bytes());
				let previous_schema_generation = context
					.previous_descriptor
					.as_ref()
					.map(|descriptor| descriptor.schema_generation);
				let previous_logical_schema_fingerprint =
					context.previous_descriptor.as_ref().map(|descriptor| {
						format_bytes_hex(descriptor.logical_schema_fingerprint.as_bytes())
					});
				let previous_physical_descriptor_fingerprint =
					context.previous_descriptor.as_ref().map(|descriptor| {
						format_bytes_hex(descriptor.physical_descriptor_fingerprint.as_bytes())
					});
				Some(
					AuditEvent::new(
						AuditAuthority::RaftLog,
						AuditOperation::TableSchemaDescriptorUpdated,
						local_node,
						format!(
							"raft_log_index={} raft_term={} tenant_id={} table_object_id={} schema_mutation={} expected_generation={} requested_generation={} previous_schema_generation={} schema_generation={} descriptor_mutation_axis={} logical_schema_fingerprint={} physical_descriptor_fingerprint={} {}",
							entry.log_id.index,
							entry.log_id.leader_id.term,
							context.envelope.tenant_id.get(),
							table_object_id,
							schema_mutation,
							context.envelope.expected_generation.get(),
							context.envelope.requested_generation.get(),
							previous_schema_generation
								.map(|generation| generation.to_string())
								.unwrap_or_else(|| "none".to_string()),
							context.descriptor.schema_generation,
							descriptor_axis,
							logical_schema_fingerprint,
							physical_descriptor_fingerprint,
							audit_outcome_message(resp),
						),
					)
					.with_outcome(outcome)
					.with_raft_log_index(entry.log_id.index)
					.with_parameter("raft_term", entry.log_id.leader_id.term)
					.with_parameter("tenant_id", context.envelope.tenant_id.get())
					.with_parameter("table_object_id", table_object_id.clone())
					.with_parameter("schema_mutation", schema_mutation)
					.with_parameter(
						"expected_generation",
						context.envelope.expected_generation.get(),
					)
					.with_parameter(
						"requested_generation",
						context.envelope.requested_generation.get(),
					)
					.with_parameter(
						"previous_schema_generation",
						previous_schema_generation
							.map_or(serde_json::Value::Null, serde_json::Value::from),
					)
					.with_parameter("schema_generation", context.descriptor.schema_generation)
					.with_parameter("descriptor_mutation_axis", descriptor_axis)
					.with_parameter(
						"logical_schema_fingerprint",
						logical_schema_fingerprint.clone(),
					)
					.with_parameter(
						"previous_logical_schema_fingerprint",
						previous_logical_schema_fingerprint
							.clone()
							.map_or(serde_json::Value::Null, serde_json::Value::from),
					)
					.with_parameter(
						"physical_descriptor_fingerprint",
						physical_descriptor_fingerprint.clone(),
					)
					.with_parameter(
						"previous_physical_descriptor_fingerprint",
						previous_physical_descriptor_fingerprint
							.clone()
							.map_or(serde_json::Value::Null, serde_json::Value::from),
					)
					.with_schema(AuditSchema::AdministrativeControl {
						action: AdministrativeControlAction::TableSchemaDescriptorUpdate,
						detail: AdministrativeControlSchema {
							target_tenant_id: Some(context.envelope.tenant_id.get()),
							target_table_object_id: Some(table_object_id),
							schema_mutation: Some(schema_mutation.to_string()),
							schema_generation: Some(context.descriptor.schema_generation),
							previous_schema_generation,
							descriptor_mutation_axis: Some(descriptor_axis.to_string()),
							logical_schema_fingerprint: Some(logical_schema_fingerprint),
							previous_logical_schema_fingerprint,
							physical_descriptor_fingerprint: Some(
								physical_descriptor_fingerprint,
							),
							previous_physical_descriptor_fingerprint,
							..AdministrativeControlSchema::default()
						},
					})
					.with_parameters(audit_outcome_parameters(resp)),
				)
			} else {
				let context = index_context?;
				let descriptor = context
					.result_descriptor
					.as_ref()
					.or(context.descriptor.as_ref())
					.or(context.previous_descriptor.as_ref());
				let index_object_id = format_bytes_hex(context.index_object_id.as_bytes());
				let table_object_id =
					descriptor.map(|descriptor| format_bytes_hex(descriptor.table_object_id.as_bytes()));
				let logical_schema_fingerprint = descriptor.map(|descriptor| {
					format_bytes_hex(descriptor.logical_schema_fingerprint.as_bytes())
				});
				let schema_mutation = schema_mutation_label(context.envelope.mutation);
				let previous_index_lifecycle_state = context
					.previous_state
					.map(secondary_index_state_label)
					.unwrap_or("absent");
				let index_lifecycle_state = context
					.result_state
					.map(secondary_index_state_label)
					.unwrap_or("absent");
				let table_schema_generation =
					descriptor.map(|descriptor| descriptor.table_schema_generation);
				let indexed_column = descriptor.map(|descriptor| descriptor.key.column_name.clone());
				Some(
					AuditEvent::new(
						AuditAuthority::RaftLog,
						AuditOperation::SecondaryIndexCatalogUpdated,
						local_node,
						format!(
							"raft_log_index={} raft_term={} tenant_id={} table_object_id={} index_object_id={} schema_mutation={} previous_index_lifecycle_state={} index_lifecycle_state={} expected_generation={} requested_generation={} {}",
							entry.log_id.index,
							entry.log_id.leader_id.term,
							context.envelope.tenant_id.get(),
							table_object_id.as_deref().unwrap_or("unknown"),
							index_object_id,
							schema_mutation,
							previous_index_lifecycle_state,
							index_lifecycle_state,
							context.envelope.expected_generation.get(),
							context.envelope.requested_generation.get(),
							audit_outcome_message(resp),
						),
					)
					.with_outcome(outcome)
					.with_raft_log_index(entry.log_id.index)
					.with_parameter("raft_term", entry.log_id.leader_id.term)
					.with_parameter("tenant_id", context.envelope.tenant_id.get())
					.with_parameter(
						"table_object_id",
						table_object_id
							.clone()
							.map_or(serde_json::Value::Null, serde_json::Value::from),
					)
					.with_parameter("index_object_id", index_object_id.clone())
					.with_parameter("schema_mutation", schema_mutation)
					.with_parameter(
						"expected_generation",
						context.envelope.expected_generation.get(),
					)
					.with_parameter(
						"requested_generation",
						context.envelope.requested_generation.get(),
					)
					.with_parameter(
						"table_schema_generation",
						table_schema_generation
							.map_or(serde_json::Value::Null, serde_json::Value::from),
					)
					.with_parameter(
						"logical_schema_fingerprint",
						logical_schema_fingerprint
							.clone()
							.map_or(serde_json::Value::Null, serde_json::Value::from),
					)
					.with_parameter(
						"indexed_column",
						indexed_column
							.clone()
							.map_or(serde_json::Value::Null, serde_json::Value::from),
					)
					.with_parameter(
						"previous_index_lifecycle_state",
						previous_index_lifecycle_state,
					)
					.with_parameter("index_lifecycle_state", index_lifecycle_state)
					.with_parameter(
						"index_replica_id",
						context
							.replica_id
							.map(|node_id| node_id.id)
							.map_or(serde_json::Value::Null, serde_json::Value::from),
					)
					.with_schema(AuditSchema::AdministrativeControl {
						action: AdministrativeControlAction::SecondaryIndexCatalogUpdate,
						detail: AdministrativeControlSchema {
							target_tenant_id: Some(context.envelope.tenant_id.get()),
							target_table_object_id: table_object_id,
							target_index_object_id: Some(index_object_id),
							schema_mutation: Some(schema_mutation.to_string()),
							table_schema_generation,
							indexed_column,
							logical_schema_fingerprint,
							previous_index_lifecycle_state: Some(
								previous_index_lifecycle_state.to_string(),
							),
							index_lifecycle_state: Some(index_lifecycle_state.to_string()),
							index_replica_id: context.replica_id.map(|node_id| node_id.id),
							..AdministrativeControlSchema::default()
						},
					})
					.with_parameters(audit_outcome_parameters(resp)),
				)
			}
		},
		WendelonRequest::SuspendTenant(tenant_id) => Some(
			AuditEvent::new(
				AuditAuthority::RaftLog,
				AuditOperation::TenantSuspended,
				local_node,
				format!(
					"raft_log_index={} raft_term={} tenant_id={} {}",
					entry.log_id.index,
					entry.log_id.leader_id.term,
					tenant_id.get(),
					audit_outcome_message(resp),
				),
			)
			.with_outcome(outcome)
			.with_raft_log_index(entry.log_id.index)
			.with_parameter("raft_term", entry.log_id.leader_id.term)
			.with_parameter("tenant_id", tenant_id.get())
			.with_schema(AuditSchema::AdministrativeControl {
				action: AdministrativeControlAction::TenantSuspend,
				detail: AdministrativeControlSchema {
					target_tenant_id: Some(tenant_id.get()),
					..AdministrativeControlSchema::default()
				},
			})
			.with_parameters(audit_outcome_parameters(resp)),
		),
		WendelonRequest::ResumeTenant(tenant_id) => Some(
			AuditEvent::new(
				AuditAuthority::RaftLog,
				AuditOperation::TenantResumed,
				local_node,
				format!(
					"raft_log_index={} raft_term={} tenant_id={} {}",
					entry.log_id.index,
					entry.log_id.leader_id.term,
					tenant_id.get(),
					audit_outcome_message(resp),
				),
			)
			.with_outcome(outcome)
			.with_raft_log_index(entry.log_id.index)
			.with_parameter("raft_term", entry.log_id.leader_id.term)
			.with_parameter("tenant_id", tenant_id.get())
			.with_schema(AuditSchema::AdministrativeControl {
				action: AdministrativeControlAction::TenantResume,
				detail: AdministrativeControlSchema {
					target_tenant_id: Some(tenant_id.get()),
					..AdministrativeControlSchema::default()
				},
			})
			.with_parameters(audit_outcome_parameters(resp)),
		),
		WendelonRequest::DeleteTenant(tenant_id) => Some(
			AuditEvent::new(
				AuditAuthority::RaftLog,
				AuditOperation::TenantDeleteRequested,
				local_node,
				format!(
					"raft_log_index={} raft_term={} tenant_id={} {}",
					entry.log_id.index,
					entry.log_id.leader_id.term,
					tenant_id.get(),
					audit_outcome_message(resp),
				),
			)
			.with_outcome(outcome)
			.with_raft_log_index(entry.log_id.index)
			.with_parameter("raft_term", entry.log_id.leader_id.term)
			.with_parameter("tenant_id", tenant_id.get())
			.with_schema(AuditSchema::AdministrativeControl {
				action: AdministrativeControlAction::TenantDeleteRequest,
				detail: AdministrativeControlSchema {
					target_tenant_id: Some(tenant_id.get()),
					..AdministrativeControlSchema::default()
				},
			})
			.with_parameters(audit_outcome_parameters(resp)),
		),
		_ => None,
	}
}

fn audit_outcome(resp: &WendelonResponse) -> AuditOutcome {
	match resp {
		WendelonResponse::Error(_) => AuditOutcome::Error,
		_ => AuditOutcome::Allowed,
	}
}

fn audit_outcome_message(resp: &WendelonResponse) -> String {
	match resp {
		WendelonResponse::Error(message) => format!("outcome=error reason={message:?}"),
		_ => "outcome=success".to_string(),
	}
}

fn audit_outcome_parameters(resp: &WendelonResponse) -> Vec<(&'static str, serde_json::Value)> {
	match resp {
		WendelonResponse::Error(message) => {
			vec![("reason", serde_json::Value::String(format!("{message:?}")))]
		}
		_ => Vec::new(),
	}
}

fn schema_mutation_label(mutation: SchemaMutationKind) -> &'static str {
	match mutation {
		SchemaMutationKind::Create => "create",
		SchemaMutationKind::Alter => "alter",
		SchemaMutationKind::Drop => "drop",
		SchemaMutationKind::IndexBackfillComplete => "index_backfill_complete",
		SchemaMutationKind::IndexDropGcComplete => "index_drop_gc_complete",
	}
}

fn descriptor_mutation_axis(
	previous: Option<&TableSchemaDescriptor>,
	next: &TableSchemaDescriptor,
) -> Option<DescriptorMutationAxis> {
	let Some(previous) = previous else {
		return Some(DescriptorMutationAxis::Combined);
	};
	let logical_changed = previous.logical_schema_fingerprint != next.logical_schema_fingerprint;
	let physical_changed =
		previous.physical_descriptor_fingerprint != next.physical_descriptor_fingerprint;
	match (logical_changed, physical_changed) {
		(true, true) => Some(DescriptorMutationAxis::Combined),
		(true, false) => Some(DescriptorMutationAxis::LogicalOnly),
		(false, true) => Some(DescriptorMutationAxis::PhysicalOnly),
		(false, false) => None,
	}
}

fn table_retention_policy_label(policy: TableRetentionPolicy) -> &'static str {
	match policy {
		TableRetentionPolicy::InheritCluster => "inherit_cluster",
		TableRetentionPolicy::Disabled => "disabled",
		TableRetentionPolicy::FixedWindow(_) => "fixed_window",
	}
}

fn table_retention_policy_window_ms(policy: TableRetentionPolicy) -> Option<u64> {
	match policy {
		TableRetentionPolicy::FixedWindow(window) => Some(window.get()),
		TableRetentionPolicy::InheritCluster | TableRetentionPolicy::Disabled => None,
	}
}

fn retention_update_extends_window(
	previous: TableRetentionPolicy,
	next: TableRetentionPolicy,
) -> bool {
	match (previous, next) {
		(TableRetentionPolicy::FixedWindow(previous), TableRetentionPolicy::FixedWindow(next)) => {
			next.get() > previous.get()
		}
		(TableRetentionPolicy::Disabled, _) => false,
		(_, TableRetentionPolicy::Disabled) => true,
		(_, TableRetentionPolicy::InheritCluster) => false,
		(TableRetentionPolicy::InheritCluster, TableRetentionPolicy::FixedWindow(_)) => false,
	}
}

fn format_epoch_peer_ids(config: &EpochConfig) -> String {
	let mut out = String::new();
	for (idx, node_id) in config.peers.keys().enumerate() {
		if idx > 0 {
			out.push(',');
		}
		use std::fmt::Write as _;
		write!(&mut out, "{node_id}").expect("writing to string cannot fail");
	}
	out
}

fn format_bytes_hex(bytes: &[u8]) -> String {
	let mut out = String::with_capacity(bytes.len() * 2);
	for byte in bytes {
		use std::fmt::Write as _;
		write!(&mut out, "{byte:02x}").expect("writing to string cannot fail");
	}
	out
}

fn canonical_finalization_marker(entry: &Entry<WendelonTypeConfig>) -> Option<FinalizationMarker> {
	match &entry.payload {
		EntryPayload::Normal(WendelonRequest::Finalize(finalization)) => {
			let mut marker = finalization.marker;
			marker.log_index = entry.log_id.index;
			marker.term = entry.log_id.leader_id.term;
			marker.e_safe_index = entry.log_id.index;
			Some(marker)
		}
		_ => None,
	}
}

fn finalization_matches(left: &TenantFinalization, right: &TenantFinalization) -> bool {
	left.tenant_id == right.tenant_id
		&& left.key_identity == right.key_identity
		&& left.charged_bytes == right.charged_bytes
		&& left.record_count == right.record_count
		&& left.marker.epoch == right.marker.epoch
		&& left.marker.commit_hlc == right.marker.commit_hlc
		&& left.marker.payload_hash == right.marker.payload_hash
		&& left.marker.durability == right.marker.durability
}

fn checkpoint_observation_from_state(
	state: &SmState,
	force: bool,
) -> Option<CheckpointObservation> {
	let last_log_id = state.last_applied_log_id?;
	Some(CheckpointObservation {
		last_log_id: Some(last_log_id),
		checkpoint: RecoveryCheckpoint {
			commit_index: last_log_id.index,
			e_safe_index: state.last_e_safe_index.min(last_log_id.index),
		},
		force,
	})
}

#[cfg(test)]
fn apply_request(
	state: &mut SmState,
	req: WendelonRequest,
) -> (WendelonResponse, Option<TierObserverEvent>) {
	apply_request_at_log_index(state, req, 0)
}

fn apply_request_at_log_index(
	state: &mut SmState,
	req: WendelonRequest,
	entry_log_index: u64,
) -> (WendelonResponse, Option<TierObserverEvent>) {
	match req {
		WendelonRequest::Finalize(finalization) => {
			// E_safe advances monotonically within an epoch —
			// keep the later HLC.
			state
				.e_safe_watermarks
				.entry(finalization.marker.epoch)
				.and_modify(|hlc| {
					if finalization.marker.commit_hlc > *hlc {
						*hlc = finalization.marker.commit_hlc;
					}
				})
				.or_insert(finalization.marker.commit_hlc);
			if finalization.marker.e_safe_index > state.last_e_safe_index {
				state.last_e_safe_index = finalization.marker.e_safe_index;
			}
			if let Some(tenant) = state.tenants.get_mut(&finalization.tenant_id) {
				// Only charge accounting while the tenant is accepting
				// writes. A Finalize that lands after Suspend/Delete
				// should not inflate used_bytes further — the data-layer
				// already rejects new admissions in those states; this
				// is defense in depth for in-flight writes.
				if tenant.state.allows_writes() {
					tenant.used_bytes =
						tenant.used_bytes.saturating_add(finalization.charged_bytes);
				}
			}
			(WendelonResponse::Success, None)
		}
		WendelonRequest::AcquireLease(lease) => {
			// Range must be well-formed. A reversed window would
			// otherwise overlap nothing in the existing map, so a
			// nonsense grant would succeed silently.
			if lease.range_end < lease.range_start {
				return (WendelonResponse::LeaseDenied, None);
			}
			// Per-range overlap sweep. Two ranges [a,b] and [c,d]
			// overlap iff a <= d && c <= b. A live lease in the
			// *same or newer* epoch blocks the grant unless it
			// names the exact same window AND the same holder — in
			// that case the new lease is a renewal that extends the
			// expiry in place.
			for existing in state.leases.values() {
				if existing.lease_expiry <= lease.lease_start {
					continue; // already expired at requested start
				}
				if existing.epoch < lease.epoch {
					continue; // prior-epoch leases are invalid by
					// the epoch-bump rule (plan 018)
				}
				let overlaps = lease.range_start <= existing.range_end
					&& existing.range_start <= lease.range_end;
				if !overlaps {
					continue;
				}
				let same_range = existing.range_start == lease.range_start
					&& existing.range_end == lease.range_end;
				let same_holder = existing.holder.id == lease.holder.id;
				if same_range && same_holder {
					continue; // permissible renewal; falls through
					// to the insert below which overwrites
					// the prior entry with the new expiry.
				}
				return (WendelonResponse::LeaseDenied, None);
			}
			state.leases.insert(lease.range_start, lease);
			(WendelonResponse::Success, None)
		}
		WendelonRequest::ReleaseLease(release) => match state.leases.get(&release.range_start) {
			Some(existing)
				if existing.range_end == release.range_end
					&& existing.epoch == release.epoch
					&& existing.holder == release.holder =>
			{
				state.leases.remove(&release.range_start);
				(WendelonResponse::Success, None)
			}
			Some(_) => (WendelonResponse::LeaseDenied, None),
			None => (WendelonResponse::Success, None),
		},
		WendelonRequest::AdvanceEpoch(record) => {
			apply_advance_epoch(state, record);
			(WendelonResponse::Success, None)
		}
		WendelonRequest::SchemaUpdate(bytes) => match apply_schema_update(state, &bytes) {
			Ok(()) => (WendelonResponse::Success, None),
			Err(err) => (WendelonResponse::Error(err), None),
		},
		WendelonRequest::TableDropPrepare(request) => (
			apply_table_drop_prepare(state, request, entry_log_index),
			None,
		),
		WendelonRequest::TableReclaimCompletion(request) => (
			apply_table_reclaim_completion(state, request, entry_log_index),
			None,
		),
		WendelonRequest::TableDropFinalize(request) => (
			apply_table_drop_finalize(state, request, entry_log_index),
			None,
		),
		WendelonRequest::TableDropLogicalFinalize(request) => (
			apply_table_drop_logical_finalize(state, request, entry_log_index),
			None,
		),
		WendelonRequest::TableDropSettlement(request) => {
			(apply_table_drop_settlement(state, request), None)
		}
		WendelonRequest::CtasSaga(mutation) => {
			let response = match apply_ctas_saga_mutation(state, mutation) {
				CtasSagaResponse::Applied(_) => WendelonResponse::Success,
				CtasSagaResponse::Rejected(reject) => WendelonResponse::Error(format!(
					"{}{code}",
					crate::schema::CTAS_SAGA_REJECT_PREFIX,
					code = reject.code()
				)),
			};
			(response, None)
		}
		WendelonRequest::ReplayLegacyCtasRowDispatched(replay) => {
			let declared = apply_ctas_saga_mutation(
				state,
				CtasSagaMutation::DeclareChain {
					tenant_id: replay.tenant_id,
					operation_id: replay.operation_id,
					owner_epoch: replay.owner_epoch,
					row_count: replay.row_count,
					chunk_count: 1,
				},
			);
			let response = match declared {
				CtasSagaResponse::Applied(_) => apply_ctas_saga_mutation(
					state,
					CtasSagaMutation::MarkRowDispatched {
						tenant_id: replay.tenant_id,
						operation_id: replay.operation_id,
						owner_epoch: replay.owner_epoch,
						row_count: replay.row_count,
					},
				),
				other => other,
			};
			let response = match response {
				CtasSagaResponse::Applied(_) => WendelonResponse::Success,
				CtasSagaResponse::Rejected(reject) => WendelonResponse::Error(format!(
					"{}{code}",
					crate::schema::CTAS_SAGA_REJECT_PREFIX,
					code = reject.code()
				)),
			};
			(response, None)
		}
		WendelonRequest::TierSlab(tier) => {
			// Idempotent replays keep the existing mapping; pointer
			// refreshes (for re-encryption or URI/hash repair) update
			// the record in place and still notify the tier
			// observer so runtime readers pick up the new pointer.
			if let Some(existing) = state.tiered_slabs.get(&tier.slab_id) {
				if existing == &tier {
					return (WendelonResponse::Success, None);
				}
				state.tiered_slabs.insert(tier.slab_id, tier.clone());
				return (
					WendelonResponse::Success,
					Some(TierObserverEvent::Tiered(tier)),
				);
			}
			state.tiered_slabs.insert(tier.slab_id, tier.clone());
			(
				WendelonResponse::Success,
				Some(TierObserverEvent::Tiered(tier)),
			)
		}
		WendelonRequest::RetireTieredSlab(retire) => {
			if state.tiered_slabs.remove(&retire.slab_id).is_some() {
				return (
					WendelonResponse::Success,
					Some(TierObserverEvent::Retired(retire.slab_id)),
				);
			}
			(WendelonResponse::Success, None)
		}
		WendelonRequest::CompactionCommit(commit) => {
			if commit.digest_summary.visible_pre != commit.digest_summary.visible_post {
				return (
					WendelonResponse::Error(
						"LSM compaction visible-domain digest changed".to_owned(),
					),
					None,
				);
			}
			(
				WendelonResponse::Success,
				Some(TierObserverEvent::LsmCompactionCommitted(commit)),
			)
		}
		WendelonRequest::CreateTenant(create) => (apply_create_tenant(state, create), None),
		WendelonRequest::UpdateTenantQuota(update) => {
			(apply_update_tenant_quota(state, update), None)
		}
		WendelonRequest::SuspendTenant(tenant_id) => (
			apply_tenant_state_transition(state, tenant_id, TenantLifecycleState::Suspended),
			None,
		),
		WendelonRequest::ResumeTenant(tenant_id) => (
			apply_tenant_state_transition(state, tenant_id, TenantLifecycleState::Active),
			None,
		),
		WendelonRequest::DeleteTenant(tenant_id) => (apply_delete_tenant(state, tenant_id), None),
		WendelonRequest::InstallTenantKekVersion(rotation) => {
			let Some(tenant) = state.tenants.get(&rotation.tenant_id) else {
				return (
					WendelonResponse::Error(format!(
						"tenant {} does not exist",
						rotation.tenant_id.get()
					)),
					None,
				);
			};
			if tenant.state == TenantLifecycleState::Deleted {
				return (
					WendelonResponse::Error(format!(
						"tenant {} is deleted",
						rotation.tenant_id.get()
					)),
					None,
				);
			}
			let keyset = state.tenant_keysets.entry(rotation.tenant_id).or_default();
			match keyset.keys.entry(rotation.tenant_kek_id) {
				std::collections::btree_map::Entry::Occupied(existing) => {
					if existing.get().wrapped_key != rotation.wrapped_tenant_kek {
						return (
							WendelonResponse::Error(format!(
								"tenant {} tenant_kek_id {} already exists with different wrapped key bytes",
								rotation.tenant_id.get(),
								rotation.tenant_kek_id
							)),
							None,
						);
					}
				}
				std::collections::btree_map::Entry::Vacant(slot) => {
					slot.insert(crate::crypto::WrappedTenantKek {
						wrapped_key: rotation.wrapped_tenant_kek,
					});
				}
			}
			keyset.active_tenant_kek_id = Some(rotation.tenant_kek_id);
			(WendelonResponse::Success, None)
		}
		WendelonRequest::RetireTenantKekVersion(retirement) => {
			(apply_retire_tenant_kek_version(state, retirement), None)
		}
		WendelonRequest::RevokeActorKey(revocation) => {
			(apply_revoke_actor_key(state, revocation), None)
		}
		WendelonRequest::SetTableRetentionPolicy(update) => {
			(apply_set_table_retention_policy(state, update), None)
		}
		WendelonRequest::SeedMigration(seed) => {
			apply_seed_migration(state, seed);
			(WendelonResponse::Success, None)
		}
		WendelonRequest::MigrationComplete(done) => {
			let tombstone = apply_migration_complete(state, done);
			(
				WendelonResponse::Success,
				tombstone.map(TierObserverEvent::MigratedRangeTombstone),
			)
		}
	}
}

fn apply_create_tenant(state: &mut SmState, create: CreateTenant) -> WendelonResponse {
	if create.replication_factor == 0 {
		return WendelonResponse::Error(format!(
			"tenant {} replication_factor must be greater than zero",
			create.tenant_id.get()
		));
	}
	if state.tenants.contains_key(&create.tenant_id) {
		return WendelonResponse::Error(format!(
			"tenant {} already exists",
			create.tenant_id.get()
		));
	}

	let record = TenantRecord {
		tenant_id: create.tenant_id,
		state: TenantLifecycleState::Active,
		quota_bytes: create.quota_bytes,
		used_bytes: 0,
		replication_factor: create.replication_factor,
	};
	let Some(active_tenant_kek_id) = create.tenant_keyset.active_tenant_kek_id else {
		return WendelonResponse::Error(format!(
			"tenant {} create request is missing an active tenant_kek_id",
			create.tenant_id.get()
		));
	};
	if !create
		.tenant_keyset
		.keys
		.contains_key(&active_tenant_kek_id)
	{
		return WendelonResponse::Error(format!(
			"tenant {} active tenant_kek_id {} is missing wrapped key bytes",
			create.tenant_id.get(),
			active_tenant_kek_id.get()
		));
	}
	state.tenants.insert(create.tenant_id, record);
	let keyset = state.tenant_keysets.entry(create.tenant_id).or_default();
	for (tenant_kek_id, wrapped) in create.tenant_keyset.keys {
		if let Some(existing) = keyset.keys.get(&tenant_kek_id) {
			if existing != &wrapped {
				return WendelonResponse::Error(format!(
					"tenant {} tenant_kek_id {} has conflicting wrapped key bytes",
					create.tenant_id.get(),
					tenant_kek_id.get()
				));
			}
		}
		keyset.keys.insert(tenant_kek_id, wrapped);
	}
	keyset.active_tenant_kek_id = Some(active_tenant_kek_id);
	WendelonResponse::Success
}

fn apply_update_tenant_quota(state: &mut SmState, update: UpdateTenantQuota) -> WendelonResponse {
	let Some(record) = state.tenants.get_mut(&update.tenant_id) else {
		return WendelonResponse::Error(format!(
			"tenant {} does not exist",
			update.tenant_id.get()
		));
	};
	if record.state == TenantLifecycleState::Deleted {
		return WendelonResponse::Error(format!("tenant {} is deleted", update.tenant_id.get()));
	}
	record.quota_bytes = update.quota_bytes;
	WendelonResponse::Success
}

fn apply_schema_update(state: &mut SmState, bytes: &[u8]) -> Result<(), String> {
	let envelope = decode_schema_update_envelope_v1(bytes).map_err(|err| err.to_string())?;
	match envelope.object.kind {
		SchemaObjectKind::Table => apply_table_schema_update(state, envelope),
		SchemaObjectKind::Index => apply_index_schema_update(state, envelope),
		SchemaObjectKind::LogicalType => apply_logical_type_schema_update(state, envelope),
		other => Err(format!("schema object kind {other:?} is not implemented")),
	}
}

fn apply_logical_type_schema_update(
	state: &mut SmState,
	envelope: SchemaUpdateEnvelope,
) -> Result<(), String> {
	match envelope.mutation {
		SchemaMutationKind::Create => {
			if envelope.object.namespace != LOGICAL_TYPE_INTERNAL_NAMESPACE {
				return Err("logical type definitions must use the reserved namespace".to_owned());
			}
			if envelope.expected_generation != 0.into() || envelope.requested_generation != 1.into()
			{
				return Err(
					"logical type definitions use fixed create generation 0 -> 1".to_owned(),
				);
			}
			let create = decode_logical_type_definition_create(&envelope.payload)
				.map_err(|err| err.to_string())?;
			if create.name != envelope.object.name {
				return Err(
					"logical type definition name does not match schema envelope".to_owned(),
				);
			}
			state
				.logical_types
				.create(envelope.tenant_id, create)
				.map(|_| ())
				.map_err(|err| err.to_string())
		}
		SchemaMutationKind::Alter | SchemaMutationKind::Drop => {
			Err("logical type ALTER/DROP is not supported".to_owned())
		}
		SchemaMutationKind::IndexBackfillComplete | SchemaMutationKind::IndexDropGcComplete => {
			Err("logical types do not support secondary-index lifecycle mutations".to_owned())
		}
	}
}

fn apply_table_schema_update(
	state: &mut SmState,
	envelope: SchemaUpdateEnvelope,
) -> Result<(), String> {
	match envelope.mutation {
		SchemaMutationKind::Create | SchemaMutationKind::Alter => {
			let descriptor = decode_table_schema_descriptor_v1(&envelope.payload)
				.map_err(|err| err.to_string())?;
			let current_descriptor = state
				.table_schemas
				.latest_by_name(
					envelope.tenant_id,
					envelope.object.namespace.as_str(),
					envelope.object.name.as_str(),
				)
				.cloned();
			let current_generation = current_descriptor
				.as_ref()
				.map(|descriptor| descriptor.schema_generation)
				.unwrap_or(0);
			if let Some(descriptor) = current_descriptor.as_ref()
				&& state.table_drop_blocks_admission(envelope.tenant_id, descriptor.table_object_id)
			{
				return Err(
					"table schema update rejected because table drop is in progress".to_owned(),
				);
			}
			// Existence is reported before generation. A create always carries
			// expected_generation ZERO, so an existing table trips the generation
			// comparison too — and reporting that first tells the caller its
			// generation is stale when the actual fact is that the table is already
			// there. Order these so the specific cause wins over the generic one.
			if envelope.mutation == SchemaMutationKind::Create && current_generation != 0 {
				return Err("table schema create cannot replace an existing table".to_owned());
			}
			if current_generation != envelope.expected_generation.get() {
				return Err(format!(
					"schema expected_generation {} does not match current generation {}",
					envelope.expected_generation.get(),
					current_generation
				));
			}
			if envelope.mutation == SchemaMutationKind::Create
				&& state
					.table_schemas
					.latest_by_id(envelope.tenant_id, descriptor.table_object_id)
					.is_some()
			{
				return Err(
					"table schema create cannot reuse an existing table_object_id".to_owned(),
				);
			}
			if envelope.mutation == SchemaMutationKind::Alter && current_generation == 0 {
				return Err("table schema alter requires an existing table".to_owned());
			}
			if let (SchemaMutationKind::Alter, Some(previous)) =
				(envelope.mutation, current_descriptor.as_ref())
			{
				validate_table_schema_evolution(previous, &descriptor)
					.map_err(|err| err.to_string())?;
			}
			if descriptor.schema_generation != envelope.requested_generation.get() {
				return Err(format!(
					"table schema descriptor generation {} does not match requested generation {}",
					descriptor.schema_generation,
					envelope.requested_generation.get()
				));
			}
			if descriptor.namespace != envelope.object.namespace
				|| descriptor.table_name != envelope.object.name
			{
				return Err(
					"table schema descriptor object does not match schema envelope".to_owned(),
				);
			}
			let table_object_id = descriptor.table_object_id;
			state
				.table_schemas
				.upsert(envelope.tenant_id, descriptor)
				.map_err(|err| err.to_string())?;
			if envelope.mutation == SchemaMutationKind::Create {
				state.table_lifecycle.ensure_inherit_cluster(
					envelope.tenant_id,
					table_object_id,
					HybridTimestamp {
						physical_ms: 0,
						logical: 0,
						node_id: 0,
					},
				);
				observe_table_retention_active_tables(&state.table_lifecycle);
			}
			Ok(())
		}
		SchemaMutationKind::Drop => {
			Err("table schema drops require an explicit data-rewrite migration".to_owned())
		}
		SchemaMutationKind::IndexBackfillComplete | SchemaMutationKind::IndexDropGcComplete => {
			Err("table schema does not support secondary-index lifecycle mutations".to_owned())
		}
	}
}

fn ctas_expected_binding_matches(
	state: &SmState,
	tenant_id: crate::schema::TenantId,
	entry: &CtasSagaEntry,
) -> bool {
	let current = state.table_schemas.current_binding_key(
		tenant_id,
		&entry.descriptor.namespace,
		&entry.descriptor.table_name,
	);
	match entry.expected_binding {
		CtasExpectedBinding::Absent => current.is_none(),
		CtasExpectedBinding::Bound {
			table_object_id,
			schema_generation,
		} => current.is_some_and(|binding| binding == (table_object_id, schema_generation.get())),
	}
}

fn ctas_owned_entry_mut(
	state: &mut SmState,
	tenant_id: crate::schema::TenantId,
	operation_id: crate::schema::CtasOperationId,
	owner_epoch: u64,
) -> Result<&mut CtasSagaEntry, CtasSagaReject> {
	let entry = state
		.ctas_sagas
		.get_mut(tenant_id, operation_id)
		.ok_or(CtasSagaReject::NotFound)?;
	if entry.owner_epoch != owner_epoch {
		return Err(CtasSagaReject::StaleOwner);
	}
	Ok(entry)
}

fn apply_ctas_saga_mutation(state: &mut SmState, mutation: CtasSagaMutation) -> CtasSagaResponse {
	match mutation {
		CtasSagaMutation::Provision {
			tenant_id,
			operation_id,
			intent_fingerprint,
			descriptor,
			expected_binding,
			prior_descriptor,
			row_write_id,
			owner_epoch,
			observed_now_ms,
			owner_lease_expires_at_ms,
			certificate,
		} => {
			// ADR 0087: an oversized certificate refuses the materialization
			// whole, before any row is dispatched.
			if certificate.as_ref().is_some_and(|certificate| {
				certificate.len() > crate::schema::MATERIALIZATION_CERTIFICATE_MAX_BYTES
			}) {
				return CtasSagaResponse::Rejected(CtasSagaReject::CertificateTooLarge);
			}
			state.ctas_sagas.prune_terminal_detail(observed_now_ms);
			if let Some(existing) = state.ctas_sagas.get(tenant_id, operation_id) {
				return if existing.intent_fingerprint == intent_fingerprint {
					CtasSagaResponse::Applied(existing.clone())
				} else {
					CtasSagaResponse::Rejected(CtasSagaReject::IntentMismatch)
				};
			}
			if operation_id.issued_at_unix_millis().is_none() {
				return CtasSagaResponse::Rejected(CtasSagaReject::InvalidOperationId);
			}
			if state
				.ctas_sagas
				.is_expired(tenant_id, operation_id, observed_now_ms)
			{
				return CtasSagaResponse::Rejected(CtasSagaReject::Expired);
			}
			if state.ctas_sagas.len() >= crate::schema::CTAS_SAGA_CAPACITY {
				return CtasSagaResponse::Rejected(CtasSagaReject::CapacityExhausted);
			}
			let entry = CtasSagaEntry {
				tenant_id,
				operation_id,
				intent_fingerprint,
				descriptor: descriptor.clone(),
				expected_binding,
				prior_descriptor,
				row_write_id,
				row_count: None,
				chunk_count: None,
				chain_pins_released: true,
				phase: CtasSagaPhase::Provisioned,
				terminal_cause: None,
				owner_epoch,
				owner_lease_expires_at_ms,
				retry_debt: 0,
				terminal_at_ms: None,
				certificate,
			};
			let prior_matches = match (entry.expected_binding, entry.prior_descriptor.as_ref()) {
				(CtasExpectedBinding::Absent, None) => true,
				(
					CtasExpectedBinding::Bound {
						table_object_id,
						schema_generation,
					},
					Some(prior),
				) => {
					prior.table_object_id == table_object_id
						&& prior.schema_generation == schema_generation.get()
				}
				_ => false,
			};
			if !prior_matches {
				return CtasSagaResponse::Rejected(CtasSagaReject::BindingChanged);
			}
			if !ctas_expected_binding_matches(state, tenant_id, &entry) {
				return CtasSagaResponse::Rejected(CtasSagaReject::BindingChanged);
			}
			if state
				.ctas_sagas
				.operation_for_object(tenant_id, descriptor.table_object_id)
				.is_some()
			{
				return CtasSagaResponse::Rejected(CtasSagaReject::ForeignProvisionalObject);
			}
			if state
				.table_schemas
				.insert_provisional(tenant_id, descriptor)
				.is_err()
			{
				return CtasSagaResponse::Rejected(CtasSagaReject::ForeignProvisionalObject);
			}
			state.ctas_sagas.insert(entry.clone());
			CtasSagaResponse::Applied(entry)
		}
		CtasSagaMutation::DeclareChain {
			tenant_id,
			operation_id,
			owner_epoch,
			row_count,
			chunk_count,
		} => {
			if chunk_count == 0 || chunk_count > crate::schema::CTAS_MAX_WRITE_CHUNKS {
				return CtasSagaResponse::Rejected(CtasSagaReject::InvalidTransition);
			}
			let entry = match ctas_owned_entry_mut(state, tenant_id, operation_id, owner_epoch) {
				Ok(entry) => entry,
				Err(reject) => return CtasSagaResponse::Rejected(reject),
			};
			match entry.phase {
				CtasSagaPhase::Provisioned
					if entry.row_count.is_none() && entry.chunk_count.is_none() =>
				{
					entry.row_count = Some(row_count);
					entry.chunk_count = Some(chunk_count);
					entry.chain_pins_released = false;
				}
				CtasSagaPhase::Provisioned
					if entry.row_count == Some(row_count)
						&& entry.chunk_count == Some(chunk_count) => {}
				_ => return CtasSagaResponse::Rejected(CtasSagaReject::InvalidTransition),
			}
			CtasSagaResponse::Applied(entry.clone())
		}
		CtasSagaMutation::MarkRowDispatched {
			tenant_id,
			operation_id,
			owner_epoch,
			row_count,
		} => {
			let entry = match ctas_owned_entry_mut(state, tenant_id, operation_id, owner_epoch) {
				Ok(entry) => entry,
				Err(reject) => return CtasSagaResponse::Rejected(reject),
			};
			match entry.phase {
				CtasSagaPhase::Provisioned
					if entry.row_count == Some(row_count) && entry.chunk_count.is_some() =>
				{
					entry.phase = CtasSagaPhase::RowDispatched;
				}
				CtasSagaPhase::RowDispatched if entry.row_count == Some(row_count) => {}
				_ => return CtasSagaResponse::Rejected(CtasSagaReject::InvalidTransition),
			}
			CtasSagaResponse::Applied(entry.clone())
		}
		CtasSagaMutation::MarkChainPinsReleased {
			tenant_id,
			operation_id,
			owner_epoch,
		} => {
			let entry = match ctas_owned_entry_mut(state, tenant_id, operation_id, owner_epoch) {
				Ok(entry) => entry,
				Err(reject) => return CtasSagaResponse::Rejected(reject),
			};
			if !matches!(
				entry.phase,
				CtasSagaPhase::RowsCommitted { .. }
					| CtasSagaPhase::Provisioned
					| CtasSagaPhase::Reclaimed
					| CtasSagaPhase::Conflict
					| CtasSagaPhase::Published
					| CtasSagaPhase::RetiringOld
					| CtasSagaPhase::Complete
			) {
				return CtasSagaResponse::Rejected(CtasSagaReject::InvalidTransition);
			}
			entry.chain_pins_released = true;
			CtasSagaResponse::Applied(entry.clone())
		}
		CtasSagaMutation::AttestRowsCommitted {
			tenant_id,
			operation_id,
			owner_epoch,
			row_count,
		} => {
			let entry = match ctas_owned_entry_mut(state, tenant_id, operation_id, owner_epoch) {
				Ok(entry) => entry,
				Err(reject) => return CtasSagaResponse::Rejected(reject),
			};
			match entry.phase {
				CtasSagaPhase::RowDispatched if entry.row_count == Some(row_count) => {
					entry.phase = CtasSagaPhase::RowsCommitted { row_count };
				}
				CtasSagaPhase::RowsCommitted {
					row_count: existing,
				} if existing == row_count => {}
				_ => return CtasSagaResponse::Rejected(CtasSagaReject::InvalidTransition),
			}
			CtasSagaResponse::Applied(entry.clone())
		}
		CtasSagaMutation::Publish {
			tenant_id,
			operation_id,
			owner_epoch,
		} => {
			let Some(snapshot) = state.ctas_sagas.get(tenant_id, operation_id).cloned() else {
				return CtasSagaResponse::Rejected(CtasSagaReject::NotFound);
			};
			if snapshot.owner_epoch != owner_epoch {
				return CtasSagaResponse::Rejected(CtasSagaReject::StaleOwner);
			}
			if !matches!(snapshot.phase, CtasSagaPhase::RowsCommitted { .. }) {
				return CtasSagaResponse::Rejected(CtasSagaReject::RowsNotCommitted);
			}
			if !snapshot.chain_pins_released {
				return CtasSagaResponse::Rejected(CtasSagaReject::InvalidTransition);
			}
			if !ctas_expected_binding_matches(state, tenant_id, &snapshot) {
				return CtasSagaResponse::Rejected(CtasSagaReject::BindingChanged);
			}
			if !state
				.table_schemas
				.is_provisional(tenant_id, snapshot.descriptor.table_object_id)
				|| state
					.ctas_sagas
					.operation_for_object(tenant_id, snapshot.descriptor.table_object_id)
					.is_none_or(|owner| owner.operation_id != operation_id)
			{
				return CtasSagaResponse::Rejected(CtasSagaReject::ForeignProvisionalObject);
			}
			// ADR 0087: the attachment slot must be vacant BEFORE the binding
			// publishes, so no failure ordering can strand a visible table
			// without its certificate or a certificate without its table.
			if snapshot.certificate.is_some()
				&& state
					.materialization_certificates
					.get(tenant_id, snapshot.descriptor.table_object_id)
					.is_some()
			{
				return CtasSagaResponse::Rejected(CtasSagaReject::ForeignProvisionalObject);
			}
			if state
				.table_schemas
				.publish_provisional(
					tenant_id,
					snapshot.descriptor.table_object_id,
					snapshot.descriptor.schema_generation,
				)
				.is_err()
			{
				return CtasSagaResponse::Rejected(CtasSagaReject::ForeignProvisionalObject);
			}
			state.table_lifecycle.ensure_inherit_cluster(
				tenant_id,
				snapshot.descriptor.table_object_id,
				HybridTimestamp {
					physical_ms: 0,
					logical: 0,
					node_id: 0,
				},
			);
			// ADR 0087: the certificate transfers from its saga carrier into
			// its permanent home inside this same apply — table and
			// certificate become visible in one state transition or neither
			// does. Infallible here: vacancy was checked before the binding
			// published.
			if let Some(certificate) = snapshot.certificate.clone() {
				let admitted = state.materialization_certificates.admit(
					tenant_id,
					snapshot.descriptor.table_object_id,
					certificate,
				);
				debug_assert!(admitted.is_ok(), "vacancy was checked before publish");
			}
			let entry = state
				.ctas_sagas
				.get_mut(tenant_id, operation_id)
				.expect("CTAS snapshot came from the saga ledger");
			entry.phase = CtasSagaPhase::Published;
			CtasSagaResponse::Applied(entry.clone())
		}
		CtasSagaMutation::MarkRetiringOld {
			tenant_id,
			operation_id,
			owner_epoch,
		} => {
			let entry = match ctas_owned_entry_mut(state, tenant_id, operation_id, owner_epoch) {
				Ok(entry) => entry,
				Err(reject) => return CtasSagaResponse::Rejected(reject),
			};
			if entry.phase != CtasSagaPhase::Published
				|| !matches!(entry.expected_binding, CtasExpectedBinding::Bound { .. })
			{
				return CtasSagaResponse::Rejected(CtasSagaReject::InvalidTransition);
			}
			entry.phase = CtasSagaPhase::RetiringOld;
			CtasSagaResponse::Applied(entry.clone())
		}
		CtasSagaMutation::Complete {
			tenant_id,
			operation_id,
			owner_epoch,
			completed_at_ms,
		} => {
			let old_object = {
				let entry = match ctas_owned_entry_mut(state, tenant_id, operation_id, owner_epoch)
				{
					Ok(entry) => entry,
					Err(reject) => return CtasSagaResponse::Rejected(reject),
				};
				let allowed = match entry.expected_binding {
					CtasExpectedBinding::Absent => entry.phase == CtasSagaPhase::Published,
					CtasExpectedBinding::Bound { .. } => entry.phase == CtasSagaPhase::RetiringOld,
				};
				if !allowed {
					return CtasSagaResponse::Rejected(CtasSagaReject::InvalidTransition);
				}
				match entry.expected_binding {
					CtasExpectedBinding::Bound {
						table_object_id, ..
					} => Some(table_object_id),
					CtasExpectedBinding::Absent => None,
				}
			};
			if let Some(table_object_id) = old_object
				&& state
					.table_schemas
					.retire_unbound_object(tenant_id, table_object_id)
					.is_err()
			{
				return CtasSagaResponse::Rejected(CtasSagaReject::InvalidTransition);
			}
			let entry = state
				.ctas_sagas
				.get_mut(tenant_id, operation_id)
				.expect("owned CTAS saga entry remains present");
			entry.phase = CtasSagaPhase::Complete;
			entry.terminal_at_ms = Some(completed_at_ms);
			CtasSagaResponse::Applied(entry.clone())
		}
		CtasSagaMutation::Reclaim {
			tenant_id,
			operation_id,
			owner_epoch,
			cause,
			completed_at_ms,
		} => {
			if matches!(cause, crate::schema::CtasTerminalCause::PublicationConflict) {
				return CtasSagaResponse::Rejected(CtasSagaReject::InvalidTransition);
			}
			let table_object_id = {
				let entry = match ctas_owned_entry_mut(state, tenant_id, operation_id, owner_epoch)
				{
					Ok(entry) => entry,
					Err(reject) => return CtasSagaResponse::Rejected(reject),
				};
				let allowed = match cause {
					crate::schema::CtasTerminalCause::DeadlineBeforeRowDispatch
					| crate::schema::CtasTerminalCause::OwnerAbandonedBeforeRowDispatch => {
						entry.phase == CtasSagaPhase::Provisioned
					}
					crate::schema::CtasTerminalCause::RowKnownNotCommitted => {
						matches!(entry.phase, CtasSagaPhase::RowDispatched)
					}
					crate::schema::CtasTerminalCause::PublicationConflict => false,
				};
				if !allowed {
					return CtasSagaResponse::Rejected(CtasSagaReject::InvalidTransition);
				}
				entry.descriptor.table_object_id
			};
			if state
				.table_schemas
				.retire_unbound_object(tenant_id, table_object_id)
				.is_err()
			{
				return CtasSagaResponse::Rejected(CtasSagaReject::InvalidTransition);
			}
			let entry = state.ctas_sagas.get_mut(tenant_id, operation_id).unwrap();
			entry.phase = CtasSagaPhase::Reclaimed;
			entry.terminal_cause = Some(cause);
			entry.terminal_at_ms = Some(completed_at_ms);
			CtasSagaResponse::Applied(entry.clone())
		}
		CtasSagaMutation::RecordPublicationConflict {
			tenant_id,
			operation_id,
			owner_epoch,
			completed_at_ms,
		} => {
			let table_object_id = {
				let entry = match ctas_owned_entry_mut(state, tenant_id, operation_id, owner_epoch)
				{
					Ok(entry) => entry,
					Err(reject) => return CtasSagaResponse::Rejected(reject),
				};
				if !matches!(entry.phase, CtasSagaPhase::RowsCommitted { .. }) {
					return CtasSagaResponse::Rejected(CtasSagaReject::InvalidTransition);
				}
				entry.descriptor.table_object_id
			};
			if state
				.table_schemas
				.retire_unbound_object(tenant_id, table_object_id)
				.is_err()
			{
				return CtasSagaResponse::Rejected(CtasSagaReject::InvalidTransition);
			}
			let entry = state.ctas_sagas.get_mut(tenant_id, operation_id).unwrap();
			entry.phase = CtasSagaPhase::Conflict;
			entry.terminal_cause = Some(crate::schema::CtasTerminalCause::PublicationConflict);
			entry.terminal_at_ms = Some(completed_at_ms);
			CtasSagaResponse::Applied(entry.clone())
		}
		CtasSagaMutation::RecordRetryDebt {
			tenant_id,
			operation_id,
			owner_epoch,
		} => {
			let entry = match ctas_owned_entry_mut(state, tenant_id, operation_id, owner_epoch) {
				Ok(entry) => entry,
				Err(reject) => return CtasSagaResponse::Rejected(reject),
			};
			if entry.phase.is_terminal() {
				return CtasSagaResponse::Rejected(CtasSagaReject::InvalidTransition);
			}
			entry.retry_debt = entry.retry_debt.saturating_add(1);
			entry.owner_lease_expires_at_ms = 0;
			CtasSagaResponse::Applied(entry.clone())
		}
		CtasSagaMutation::Claim {
			tenant_id,
			operation_id,
			expected_owner_epoch,
			new_owner_epoch,
			observed_now_ms,
			lease_expires_at_ms,
		} => {
			let Some(entry) = state.ctas_sagas.get_mut(tenant_id, operation_id) else {
				return CtasSagaResponse::Rejected(CtasSagaReject::NotFound);
			};
			if entry.phase.is_terminal() {
				return CtasSagaResponse::Rejected(CtasSagaReject::InvalidTransition);
			}
			if entry.owner_epoch != expected_owner_epoch || new_owner_epoch <= expected_owner_epoch
			{
				return CtasSagaResponse::Rejected(CtasSagaReject::StaleOwner);
			}
			if observed_now_ms < entry.owner_lease_expires_at_ms {
				return CtasSagaResponse::Rejected(CtasSagaReject::OwnerLeaseStillLive);
			}
			entry.owner_epoch = new_owner_epoch;
			entry.owner_lease_expires_at_ms = lease_expires_at_ms;
			CtasSagaResponse::Applied(entry.clone())
		}
	}
}

fn apply_table_drop_prepare(
	state: &mut SmState,
	request: TableDropPrepareRequest,
	entry_log_index: u64,
) -> WendelonResponse {
	let operation_id = request.operation_id();
	if operation_id.schema_generation != request.expected_generation() {
		return WendelonResponse::Error(format!(
			"drop operation schema_generation {} does not match expected_generation {}",
			operation_id.schema_generation.get(),
			request.expected_generation().get()
		));
	}
	if state
		.table_drop_outcomes
		.get(operation_id.tenant_id, operation_id.table_object_id)
		.is_some()
	{
		return WendelonResponse::Error(format!(
			"table {:?} is already dropped",
			operation_id.table_object_id
		));
	}
	let Some(descriptor) = state
		.table_schemas
		.latest_by_id(operation_id.tenant_id, operation_id.table_object_id)
	else {
		return WendelonResponse::Error(format!(
			"table {:?} is missing",
			operation_id.table_object_id
		));
	};
	if descriptor.schema_generation != request.expected_generation().get() {
		return WendelonResponse::Error(format!(
			"schema expected_generation {} does not match current generation {}",
			request.expected_generation().get(),
			descriptor.schema_generation
		));
	}
	let name_is_bound = state.table_schemas.is_latest_name_bound(
		operation_id.tenant_id,
		&descriptor.namespace,
		&descriptor.table_name,
		operation_id.table_object_id,
		descriptor.schema_generation,
	);
	let is_ctas_retirement = state.ctas_sagas.retiring_old_binding(
		operation_id.tenant_id,
		operation_id.table_object_id,
		operation_id.schema_generation,
	);
	if !name_is_bound && !is_ctas_retirement {
		return WendelonResponse::Error(format!(
			"table {:?} is already dropped",
			operation_id.table_object_id
		));
	}
	let dependent_indexes = state
		.secondary_indexes
		.for_table(operation_id.tenant_id, operation_id.table_object_id);
	if !dependent_indexes.is_empty() {
		let indexes = dependent_indexes
			.iter()
			.map(|row| format!("{:?}", row.descriptor.index_object_id))
			.collect::<Vec<_>>()
			.join(", ");
		return WendelonResponse::Error(format!(
			"dependent secondary indexes present for table {:?}: {indexes}",
			operation_id.table_object_id
		));
	}
	match state
		.table_drop_fences
		.prepare(operation_id, entry_log_index)
	{
		Ok(_) => WendelonResponse::Success,
		Err(err) => WendelonResponse::Error(err.to_string()),
	}
}

fn apply_table_reclaim_completion(
	state: &mut SmState,
	request: TableReclaimCompletionRequest,
	entry_log_index: u64,
) -> WendelonResponse {
	match state.table_drop_fences.complete_reclaim(
		request.operation_id(),
		request.fence_log_index(),
		entry_log_index,
	) {
		Ok(_) => WendelonResponse::Success,
		Err(err) => WendelonResponse::Error(err.to_string()),
	}
}

fn apply_table_drop_finalize(
	state: &mut SmState,
	request: TableDropFinalizeRequest,
	entry_log_index: u64,
) -> WendelonResponse {
	let operation_id = request.operation_id();
	let outcome = TableDropOutcome {
		operation_id,
		finalize_log_index: entry_log_index,
	};
	if state
		.table_drop_outcomes
		.get(operation_id.tenant_id, operation_id.table_object_id)
		.is_some_and(|existing| *existing == outcome)
	{
		return WendelonResponse::Success;
	}
	if let Err(err) = state.table_drop_outcomes.validate_record(outcome) {
		return WendelonResponse::Error(err.to_string());
	}
	if let Err(err) = state.table_drop_fences.validate_finalize(
		operation_id,
		request.fence_log_index(),
		request.reclaim_completion_log_index(),
	) {
		return WendelonResponse::Error(err.to_string());
	}

	let expected_generation = operation_id.schema_generation.get();
	let already_unbound_for_ctas = state.ctas_sagas.retiring_old_binding(
		operation_id.tenant_id,
		operation_id.table_object_id,
		operation_id.schema_generation,
	);
	if !already_unbound_for_ctas
		&& let Err(err) = state.table_schemas.unbind_latest_name(
			operation_id.tenant_id,
			operation_id.table_object_id,
			expected_generation,
		) {
		return WendelonResponse::Error(err.to_string());
	}
	// Every fallible check is complete. The remaining mutations cannot reject,
	// so a failed finalize leaves the state byte-for-byte unchanged.
	state.table_drop_fences.remove_validated(operation_id);
	state.table_drop_outcomes.insert_validated(outcome);
	state
		.table_lifecycle
		.remove(operation_id.tenant_id, operation_id.table_object_id);
	// ADR 0087: destruction coupling — the certificate never survives its
	// object, removed in this same finalize apply.
	state
		.materialization_certificates
		.remove(operation_id.tenant_id, operation_id.table_object_id);
	observe_table_retention_active_tables(&state.table_lifecycle);
	WendelonResponse::Success
}

fn apply_table_drop_logical_finalize(
	state: &mut SmState,
	request: TableDropLogicalFinalizeRequest,
	entry_log_index: u64,
) -> WendelonResponse {
	let operation_id = request.operation_id;
	let outcome = TableDropOutcome {
		operation_id,
		finalize_log_index: entry_log_index,
	};
	let Some(descriptor) = state
		.table_schemas
		.latest_by_id(operation_id.tenant_id, operation_id.table_object_id)
		.cloned()
	else {
		return WendelonResponse::Error(format!(
			"table {:?} is missing during logical drop finalize",
			operation_id.table_object_id
		));
	};
	let stamp = match crate::consensus::drop_reclaim::derive_drop_participant_stamp(
		state,
		operation_id.tenant_id,
		&descriptor,
	) {
		Ok(stamp) => stamp,
		Err(error) => return WendelonResponse::Error(error.to_string()),
	};
	let tombstone = TableDropTombstone {
		operation_id,
		fence_log_index: request.fence_log_index,
		finalize_log_index: entry_log_index,
	};
	let debt = TableDropReclaimDebt {
		operation_id,
		fence_log_index: request.fence_log_index,
		finalize_log_index: entry_log_index,
		ownership_epoch: stamp.ownership_epoch,
		participants: stamp.participants,
	};

	if state
		.table_drop_outcomes
		.get(operation_id.tenant_id, operation_id.table_object_id)
		.is_some_and(|existing| *existing == outcome)
		&& state
			.table_drop_tombstones
			.get(operation_id.tenant_id, operation_id.table_object_id)
			.is_some_and(|existing| *existing == tombstone)
		&& state
			.table_drop_reclaim_debts
			.get(operation_id.tenant_id, operation_id.table_object_id)
			.is_some_and(|existing| existing == &debt)
	{
		return WendelonResponse::Success;
	}
	if let Err(error) = state.table_drop_outcomes.validate_record(outcome) {
		return WendelonResponse::Error(error.to_string());
	}
	if let Err(error) = state
		.table_drop_fences
		.validate_logical_finalize(operation_id, request.fence_log_index)
	{
		return WendelonResponse::Error(error.to_string());
	}
	if let Err(error) = state.table_drop_tombstones.validate_insert(tombstone) {
		return WendelonResponse::Error(error.to_string());
	}
	if let Err(error) = state.table_drop_reclaim_debts.validate_insert(&debt) {
		return WendelonResponse::Error(error.to_string());
	}

	let expected_generation = operation_id.schema_generation.get();
	let already_unbound_for_ctas = state.ctas_sagas.retiring_old_binding(
		operation_id.tenant_id,
		operation_id.table_object_id,
		operation_id.schema_generation,
	);
	if !already_unbound_for_ctas
		&& let Err(error) = state.table_schemas.unbind_latest_name(
			operation_id.tenant_id,
			operation_id.table_object_id,
			expected_generation,
		) {
		return WendelonResponse::Error(error.to_string());
	}

	// All fallible validation is complete. These mutations establish one
	// continuous admission authority: fence removal and tombstone insertion are
	// part of this exclusive state-machine apply.
	state.table_drop_fences.remove_validated(operation_id);
	state.table_drop_tombstones.insert_validated(tombstone);
	state.table_drop_reclaim_debts.insert_validated(debt);
	state.table_drop_outcomes.insert_validated(outcome);
	state
		.table_lifecycle
		.remove(operation_id.tenant_id, operation_id.table_object_id);
	state
		.materialization_certificates
		.remove(operation_id.tenant_id, operation_id.table_object_id);
	observe_table_retention_active_tables(&state.table_lifecycle);
	WendelonResponse::Success
}

fn apply_table_drop_settlement(
	state: &mut SmState,
	request: TableDropSettlementRequest,
) -> WendelonResponse {
	let operation_id = request.operation_id;
	let Some(tombstone) = state
		.table_drop_tombstones
		.get(operation_id.tenant_id, operation_id.table_object_id)
		.copied()
	else {
		return WendelonResponse::Error("table drop settlement tombstone is missing".to_string());
	};
	let Some(debt) = state
		.table_drop_reclaim_debts
		.get(operation_id.tenant_id, operation_id.table_object_id)
		.cloned()
	else {
		return WendelonResponse::Error("table drop settlement debt is missing".to_string());
	};
	if tombstone.operation_id != operation_id
		|| debt.operation_id != operation_id
		|| tombstone.finalize_log_index != request.finalize_log_index
		|| debt.finalize_log_index != request.finalize_log_index
		|| debt.ownership_epoch != request.ownership_epoch
	{
		return WendelonResponse::Error(
			"table drop settlement identity, finalize index, or ownership epoch mismatch"
				.to_string(),
		);
	}
	if let Err(error) = request.attested_participants.validate() {
		return WendelonResponse::Error(error.to_string());
	}
	if request.attested_participants != debt.participants {
		return WendelonResponse::Error(
			"table drop settlement does not carry the complete stamped participant set".to_string(),
		);
	}

	// Validate descriptor retirement on a clone so every fallible operation is
	// complete before the live state mutates.
	let mut retired_schemas = state.table_schemas.clone();
	if let Err(error) =
		retired_schemas.retire_unbound_object(operation_id.tenant_id, operation_id.table_object_id)
	{
		return WendelonResponse::Error(error.to_string());
	}
	state.table_schemas = retired_schemas;
	state
		.table_drop_reclaim_debts
		.remove_validated(operation_id);
	state.table_drop_tombstones.remove_validated(operation_id);
	WendelonResponse::Success
}

fn apply_index_schema_update(
	state: &mut SmState,
	envelope: SchemaUpdateEnvelope,
) -> Result<(), String> {
	match envelope.mutation {
		SchemaMutationKind::Create => {
			let descriptor = decode_secondary_index_descriptor(&envelope.payload)
				.map_err(|err| err.to_string())?;
			if state.table_drop_blocks_admission(envelope.tenant_id, descriptor.table_object_id) {
				return Err(
					"secondary index create rejected because table drop is in progress".to_owned(),
				);
			}
			if descriptor.namespace != envelope.object.namespace
				|| descriptor.index_name != envelope.object.name
			{
				return Err(
					"secondary index descriptor object does not match schema envelope".to_owned(),
				);
			}
			state
				.secondary_indexes
				.create(envelope.tenant_id, descriptor.clone(), &state.table_schemas)
				.map_err(|err| err.to_string())?;
			metrics().observe_storage_secondary_index_create(
				envelope.tenant_id,
				descriptor.table_object_id,
				descriptor.index_object_id,
			);
			Ok(())
		}
		SchemaMutationKind::Alter => Err(SCHEMA_UPDATE_REJECT_INDEX_ALTER_UNSUPPORTED.to_owned()),
		SchemaMutationKind::Drop => {
			let request = decode_secondary_index_drop_request(&envelope.payload)
				.map_err(|err| err.to_string())?;
			let existing = state
				.secondary_indexes
				.get(envelope.tenant_id, request.index_object_id)
				.cloned();
			if let Some(row) = existing.as_ref()
				&& (row.descriptor.namespace != envelope.object.namespace
					|| row.descriptor.index_name != envelope.object.name)
			{
				return Err("secondary index drop object does not match schema envelope".to_owned());
			}
			if let Some(row) = existing.as_ref()
				&& state
					.table_drop_blocks_admission(envelope.tenant_id, row.descriptor.table_object_id)
			{
				return Err(
					"secondary index drop rejected because table drop is in progress".to_owned(),
				);
			}
			state
				.secondary_indexes
				.mark_dropping(envelope.tenant_id, request.index_object_id)
				.map_err(|err| err.to_string())?;
			if let Some(row) = existing {
				metrics().observe_storage_secondary_index_drop(
					envelope.tenant_id,
					row.descriptor.table_object_id,
					row.descriptor.index_object_id,
					secondary_index_state_label(row.state),
				);
			}
			Ok(())
		}
		SchemaMutationKind::IndexBackfillComplete => {
			let complete = decode_secondary_index_lifecycle_complete(&envelope.payload)
				.map_err(|err| err.to_string())?;
			let voters = current_voter_ids(state, complete.replica_id.id);
			let before = state
				.secondary_indexes
				.get(envelope.tenant_id, complete.index_object_id)
				.cloned();
			if let Some(row) = before.as_ref()
				&& state
					.table_drop_blocks_admission(envelope.tenant_id, row.descriptor.table_object_id)
			{
				return Err(
					"secondary index backfill completion rejected because table drop is in progress"
						.to_owned(),
				);
			}
			state
				.secondary_indexes
				.mark_backfill_complete(
					envelope.tenant_id,
					complete.index_object_id,
					complete.replica_id,
					&voters,
				)
				.map_err(|err| err.to_string())?;
			let after = state
				.secondary_indexes
				.get(envelope.tenant_id, complete.index_object_id);
			if let Some(row) = before
				&& row.state == SecondaryIndexState::Building
				&& after.is_some_and(|row| row.state == SecondaryIndexState::Ready)
			{
				metrics().observe_storage_secondary_index_lifecycle_transition(
					envelope.tenant_id,
					row.descriptor.table_object_id,
					row.descriptor.index_object_id,
					"building",
					"ready",
				);
			}
			Ok(())
		}
		SchemaMutationKind::IndexDropGcComplete => {
			let complete = decode_secondary_index_lifecycle_complete(&envelope.payload)
				.map_err(|err| err.to_string())?;
			let voters = current_voter_ids(state, complete.replica_id.id);
			let before = state
				.secondary_indexes
				.get(envelope.tenant_id, complete.index_object_id)
				.cloned();
			if let Some(row) = before.as_ref()
				&& state
					.table_drop_blocks_admission(envelope.tenant_id, row.descriptor.table_object_id)
			{
				return Err(
					"secondary index drop GC completion rejected because table drop is in progress"
						.to_owned(),
				);
			}
			state
				.secondary_indexes
				.mark_drop_gc_complete(
					envelope.tenant_id,
					complete.index_object_id,
					complete.replica_id,
					&voters,
				)
				.map_err(|err| err.to_string())?;
			if let Some(row) = before
				&& state
					.secondary_indexes
					.get(envelope.tenant_id, complete.index_object_id)
					.is_none()
			{
				metrics().observe_storage_secondary_index_lifecycle_transition(
					envelope.tenant_id,
					row.descriptor.table_object_id,
					row.descriptor.index_object_id,
					"dropping",
					"absent",
				);
			}
			Ok(())
		}
	}
}

fn secondary_index_state_label(state: SecondaryIndexState) -> &'static str {
	match state {
		SecondaryIndexState::Building => "building",
		SecondaryIndexState::Ready => "ready",
		SecondaryIndexState::Dropping => "dropping",
	}
}

fn current_voter_ids(state: &SmState, fallback_replica_id: u32) -> BTreeSet<u32> {
	let mut voters: BTreeSet<u32> = state.last_membership.voter_ids().collect();
	if voters.is_empty() {
		voters.insert(fallback_replica_id);
	}
	voters
}

fn apply_set_table_retention_policy(
	state: &mut SmState,
	update: SetTableRetentionPolicy,
) -> WendelonResponse {
	match update.new_policy.validate() {
		Ok(()) => {}
		Err(err) => return WendelonResponse::Error(err.to_string()),
	}
	let Some(tenant) = state.tenants.get(&update.tenant_id) else {
		return WendelonResponse::Error(format!(
			"tenant {} does not exist",
			update.tenant_id.get()
		));
	};
	if tenant.state == TenantLifecycleState::Deleted {
		return WendelonResponse::Error(format!("tenant {} is deleted", update.tenant_id.get()));
	}
	let Some(descriptor) = state
		.table_schemas
		.latest_by_id(update.tenant_id, update.table_object_id)
	else {
		return WendelonResponse::Error(format!(
			"table {:?} does not exist for tenant {}",
			update.table_object_id,
			update.tenant_id.get()
		));
	};
	if state.table_drop_blocks_admission(update.tenant_id, update.table_object_id) {
		return WendelonResponse::Error(
			"table retention policy update rejected because table drop is in progress".to_string(),
		);
	}
	if matches!(
		descriptor.row_identity_policy,
		RowIdentityPolicy::PrimaryKey(_)
	) && update.new_policy.is_table_override_disallowed_for_pk()
	{
		return WendelonResponse::Error(format!(
			"{}: PK-keyed tables only accept InheritCluster retention in V1",
			crate::schema::RetentionRejectCode::RETENTION_REJECTED_PK_DISALLOWED
		));
	}
	match state.table_lifecycle.set_next_generation(
		update.tenant_id,
		update.table_object_id,
		update.expected_policy_generation,
		update.new_policy,
		update.updated_at_hlc,
	) {
		Ok(_) => {
			observe_table_retention_active_tables(&state.table_lifecycle);
			WendelonResponse::Success
		}
		Err(err) => WendelonResponse::Error(err.to_string()),
	}
}

fn observe_table_retention_active_tables(catalog: &TableLifecycleCatalog) {
	let counts = catalog.policy_counts();
	metrics().set_storage_table_retention_policy_active_tables(
		counts.inherit_cluster as u64,
		counts.disabled as u64,
		counts.fixed_window as u64,
	);
}

fn apply_tenant_state_transition(
	state: &mut SmState,
	tenant_id: crate::schema::TenantId,
	next: TenantLifecycleState,
) -> WendelonResponse {
	let Some(record) = state.tenants.get_mut(&tenant_id) else {
		return WendelonResponse::Error(format!("tenant {} does not exist", tenant_id.get()));
	};
	if record.state == next {
		return WendelonResponse::Success;
	}
	if !record.state.can_transition_to(next) {
		return WendelonResponse::Error(format!(
			"tenant {} cannot transition from {:?} to {:?}",
			tenant_id.get(),
			record.state,
			next
		));
	}
	record.state = next;
	WendelonResponse::Success
}

fn apply_delete_tenant(
	state: &mut SmState,
	tenant_id: crate::schema::TenantId,
) -> WendelonResponse {
	let Some(record) = state.tenants.get_mut(&tenant_id) else {
		return WendelonResponse::Error(format!("tenant {} does not exist", tenant_id.get()));
	};
	if record.state == TenantLifecycleState::Deleted {
		return WendelonResponse::Success;
	}
	let next = if record.used_bytes == 0 {
		TenantLifecycleState::Deleted
	} else {
		TenantLifecycleState::Deleting
	};
	if record.state != next && !record.state.can_transition_to(next) {
		return WendelonResponse::Error(format!(
			"tenant {} cannot transition from {:?} to {:?}",
			tenant_id.get(),
			record.state,
			next
		));
	}
	record.state = next;
	if next == TenantLifecycleState::Deleted {
		record.quota_bytes = 0;
		state.tenant_keysets.remove(&tenant_id);
	}
	WendelonResponse::Success
}

fn apply_advance_epoch(state: &mut SmState, record: AdvanceEpochRecord) {
	state
		.epoch_history
		.entry(record.from.epoch)
		.or_insert(record.from);
	state
		.epoch_history
		.entry(record.to.epoch)
		.or_insert(record.to);
}

fn apply_retire_tenant_kek_version(
	state: &mut SmState,
	retirement: RetireTenantKekVersion,
) -> WendelonResponse {
	let Some(tenant) = state.tenants.get(&retirement.tenant_id) else {
		return WendelonResponse::Error(format!(
			"tenant {} does not exist",
			retirement.tenant_id.get()
		));
	};
	if tenant.state == TenantLifecycleState::Deleted {
		return WendelonResponse::Error(format!(
			"tenant {} is deleted",
			retirement.tenant_id.get()
		));
	}
	let Some(keyset) = state.tenant_keysets.get(&retirement.tenant_id) else {
		return WendelonResponse::Error(format!(
			"tenant {} tenant_kek_id {} does not exist",
			retirement.tenant_id.get(),
			retirement.tenant_kek_id.get()
		));
	};
	if !keyset.keys.contains_key(&retirement.tenant_kek_id) {
		return WendelonResponse::Error(format!(
			"tenant {} tenant_kek_id {} does not exist",
			retirement.tenant_id.get(),
			retirement.tenant_kek_id.get()
		));
	}
	if keyset.active_tenant_kek_id == Some(retirement.tenant_kek_id) {
		return WendelonResponse::Error(format!(
			"tenant {} tenant_kek_id {} is active and cannot be retired",
			retirement.tenant_id.get(),
			retirement.tenant_kek_id.get()
		));
	}
	let status = tenant_kek_retirement_status(state, retirement.tenant_id);
	if !status.accounting_complete {
		return WendelonResponse::Error(format!(
			"tenant {} tenant KEK accounting is incomplete",
			retirement.tenant_id.get()
		));
	}
	let Some(version) = status
		.versions
		.iter()
		.find(|version| version.tenant_kek_id == retirement.tenant_kek_id)
	else {
		return WendelonResponse::Error(format!(
			"tenant {} tenant_kek_id {} does not exist",
			retirement.tenant_id.get(),
			retirement.tenant_kek_id.get()
		));
	};
	if version.tiered_slab_ref_count > 0 {
		return WendelonResponse::Error(format!(
			"tenant {} tenant_kek_id {} is still referenced by {} tiered slabs",
			retirement.tenant_id.get(),
			retirement.tenant_kek_id.get(),
			version.tiered_slab_ref_count,
		));
	}
	let Some(keyset) = state.tenant_keysets.get_mut(&retirement.tenant_id) else {
		return WendelonResponse::Error(format!(
			"tenant {} tenant_kek_id {} does not exist",
			retirement.tenant_id.get(),
			retirement.tenant_kek_id.get()
		));
	};
	keyset.keys.remove(&retirement.tenant_kek_id);
	WendelonResponse::Success
}

fn apply_revoke_actor_key(state: &mut SmState, revocation: RevokeActorKey) -> WendelonResponse {
	if revocation.key_id.is_empty() {
		return WendelonResponse::Error("actor certificate key_id must not be empty".to_string());
	}
	if revocation.revoked_at_ms == 0 {
		return WendelonResponse::Error(
			"actor certificate revocation timestamp must be non-zero".to_string(),
		);
	}
	let prune_before_ms = revocation
		.revoked_at_ms
		.saturating_sub(REVOKED_ACTOR_KEY_RETENTION_MS);
	prune_expired_revoked_actor_keys(state, prune_before_ms);
	state
		.revoked_actor_keys
		.insert(revocation.key(), revocation.revoked_at_ms);
	WendelonResponse::Success
}

/// Fold a [`SeedMigration`] into the ledger. Idempotent: a replay
/// with the same `migration_id` is a no-op rather than an error
/// so the Control Ring can safely retry seeding during an epoch
/// transition.
fn apply_seed_migration(state: &mut SmState, seed: SeedMigration) {
	if state.migrations.contains_key(&seed.entry.migration_id) {
		return;
	}
	state.migrations.insert(seed.entry.migration_id, seed.entry);
}

/// Apply a [`MigrationComplete`]. Validates the redundant range /
/// epoch / floor fields against the live ledger entry and — on
/// match — transitions the entry to `Completed`, installs a
/// tombstone, and advances any descendant entries that were
/// sourcing from the superseded physical holder.
///
/// A stale `migration_id` or a mismatch on any validated field
/// silently drops the request. Raft's happy path does not race
/// here (the leader is the one proposing), but dropping rather
/// than erroring keeps the state machine deterministic under
/// replay.
fn apply_migration_complete(
	state: &mut SmState,
	done: MigrationComplete,
) -> Option<MigratedRangeTombstone> {
	let matches = state
		.migrations
		.get(&done.migration_id)
		.map(|e| {
			e.range_start == done.range_start
				&& e.range_end == done.range_end
				&& e.to_epoch == done.to_epoch
				&& e.migration_hlc_floor == done.migration_hlc_floor
		})
		.unwrap_or(false);
	if !matches {
		return None;
	}

	{
		let entry = state
			.migrations
			.get_mut(&done.migration_id)
			.expect("matched above");
		if entry.state == MigrationState::Completed {
			return None; // idempotent replay
		}
		entry.state = MigrationState::Completed;
		entry.completed_by = Some(done.completed_by);
		entry.completed_at_hlc = Some(done.completed_at_hlc);
	}

	let tombstone = MigratedRangeTombstone {
		migration_id: done.migration_id,
		range_start: done.range_start,
		range_end: done.range_end,
		to_epoch: done.to_epoch,
		completed_by: done.completed_by,
		completed_at_hlc: done.completed_at_hlc,
		migration_hlc_floor: done.migration_hlc_floor,
	};
	state.tombstones.push(tombstone.clone());

	// Descendant reconciliation under the drain-all rule.
	//
	// `pull_migration_range` now requires *every* entry in
	// `source_replicas` to drain successfully — any failure
	// surfaces `MIGRATION_INCOMPLETE` to the client. That means
	// retaining a superseded physical holder in the candidate
	// list is not a performance hint: if the old holder is later
	// unreachable, the descendant pull fails even when the new
	// authoritative source has every byte the chain needs.
	//
	// So we split the two cases:
	//
	// * **Direct descendant** (`upstream_migration_id ==
	//   Some(completed_id)`): the predecessor has just published
	//   its authoritative drain. `completed_by` now owns every
	//   version the predecessor's source set collectively held,
	//   so we *replace* `source_replicas` with just
	//   `[completed_by]` and clear the upstream link. Old
	//   holders are dropped — the chain is now collapsed onto
	//   the completed intermediate.
	//
	// * **Mere range overlap** (no upstream link): we only have
	//   a hint that `completed_by` may hold useful bytes, not
	//   proof that it subsumes the other sources. Keep the
	//   existing candidates and prepend `completed_by` as a
	//   preference.
	let completed_id = done.migration_id;
	for entry in state.migrations.values_mut() {
		if entry.migration_id == completed_id {
			continue;
		}
		if entry.state == MigrationState::Completed {
			continue;
		}
		if entry.upstream_migration_id == Some(completed_id) {
			entry.source_replicas.clear();
			entry.source_replicas.push(done.completed_by);
			entry.upstream_migration_id = None;
			continue;
		}
		let overlaps = entry.range_start <= done.range_end && done.range_start <= entry.range_end;
		if !overlaps {
			continue;
		}
		entry
			.source_replicas
			.retain(|n| n.id != done.completed_by.id);
		entry.source_replicas.insert(0, done.completed_by);
	}

	Some(tombstone)
}

// ============================================================
//  SNAPSHOT BUILDER
// ============================================================

/// Produces snapshots of [`WendelonStateMachine`] by taking a
/// lock on the shared SM inner state and postcard-encoding it
/// behind a [`SnapshotHeader`] prefix.
#[derive(Clone)]
pub struct WendelonSnapshotBuilder {
	inner: Arc<RwLock<SmInner>>,
	snapshot_runtime: SnapshotRuntime,
	checkpoint_observer: Option<Arc<dyn CheckpointObserver>>,
	retention: Option<Arc<RetentionGuard>>,
}

impl Default for WendelonSnapshotBuilder {
	fn default() -> Self {
		Self {
			inner: Arc::new(RwLock::new(SmInner::default())),
			snapshot_runtime: SnapshotRuntime::ephemeral(),
			checkpoint_observer: None,
			retention: None,
		}
	}
}

impl WendelonSnapshotBuilder {
	pub fn new() -> Self {
		Self::default()
	}
}

impl RaftSnapshotBuilder<WendelonTypeConfig> for WendelonSnapshotBuilder {
	async fn build_snapshot(&mut self) -> Result<Snapshot<WendelonTypeConfig>, StorageError<u32>> {
		if retention::refuses(&self.retention, RetentionViolation::SnapshotBuild) {
			return Err(StorageIOError::write_snapshot(
				None,
				AnyError::new(&RetentionViolation::SnapshotBuild),
			)
			.into());
		}
		let (state, meta, pending_checkpoint) = {
			let mut guard = self.inner.write().await;
			guard.snapshot_seq += 1;
			let seq = guard.snapshot_seq;
			let state = guard.state.clone();
			let last_log_id = state.last_applied_log_id;
			let last_membership = state.last_membership.clone();
			let meta = SnapshotMeta {
				last_log_id,
				last_membership,
				snapshot_id: format!("snapshot-{seq}"),
			};
			(
				state,
				meta,
				checkpoint_observation_from_state(&guard.state, true),
			)
		};
		persist_snapshot_artifact(
			self.snapshot_runtime.file_ops.as_ref(),
			&self.snapshot_runtime.paths,
			&state,
		)
		.await
		.map_err(|err| {
			StorageIOError::write_snapshot(Some(meta.signature()), AnyError::new(&err))
		})?;
		{
			let mut guard = self.inner.write().await;
			guard.snapshot = Some(HeldSnapshot {
				meta: meta.clone(),
				path: self.snapshot_runtime.paths.current.clone(),
			});
		}
		if let (Some(observer), Some(observation)) =
			(self.checkpoint_observer.as_ref(), pending_checkpoint)
		{
			if let Err(err) = observer.on_checkpoint_progress(observation).await {
				tracing::warn!(
					commit_index = observation.checkpoint.commit_index,
					e_safe_index = observation.checkpoint.e_safe_index,
					error = %err,
					"checkpoint observer failed during snapshot build"
				);
			}
		}
		let snapshot = SnapshotDataFile::open(
			self.snapshot_runtime.file_ops.clone(),
			self.snapshot_runtime.paths.current.clone(),
		)
		.await
		.map_err(|err| {
			StorageIOError::read_snapshot(Some(meta.signature()), AnyError::new(&err))
		})?;
		Ok(Snapshot {
			meta,
			snapshot: Box::new(snapshot),
		})
	}
}

// ============================================================
//  WIRE / SERIALIZATION HELPERS
// ============================================================

async fn inspect_artifacts(
	file_ops: &dyn AsyncFileOps,
	wal_path: &Path,
	vote_path: &Path,
	snapshot_path: &Path,
) -> std::io::Result<ArtifactPresence> {
	Ok(ArtifactPresence {
		wal: wal_artifact_exists(file_ops, wal_path).await?,
		vote: artifact_exists(file_ops, vote_path).await?,
		snapshot: artifact_exists(file_ops, snapshot_path).await?,
	})
}

async fn wal_artifact_exists(
	file_ops: &dyn AsyncFileOps,
	wal_path: &Path,
) -> std::io::Result<bool> {
	if file_ops.file_len(wal_path).await?.is_some() {
		return Ok(true);
	}
	Ok(!discover_named_wal_segments(file_ops, wal_path)
		.await?
		.is_empty())
}

async fn artifact_exists(file_ops: &dyn AsyncFileOps, path: &Path) -> std::io::Result<bool> {
	Ok(file_ops
		.file_len(path)
		.await?
		.map(|len| len > 0)
		.unwrap_or(false))
}

async fn discover_all_wal_segments(
	file_ops: &dyn AsyncFileOps,
	wal_path: &Path,
) -> std::io::Result<Vec<DiscoveredWalSegment>> {
	let mut out = Vec::new();
	if file_ops.file_len(wal_path).await?.is_some() {
		out.push(DiscoveredWalSegment {
			path: wal_path.to_path_buf(),
			start_index: 0,
			legacy: true,
		});
	}
	out.extend(discover_named_wal_segments(file_ops, wal_path).await?);
	Ok(out)
}

async fn discover_named_wal_segments(
	file_ops: &dyn AsyncFileOps,
	wal_path: &Path,
) -> std::io::Result<Vec<DiscoveredWalSegment>> {
	let parent = wal_path.parent().ok_or_else(|| {
		Error::new(
			ErrorKind::InvalidInput,
			format!("wal path has no parent directory: {}", wal_path.display()),
		)
	})?;
	let mut out = Vec::new();
	for entry in file_ops.read_dir(parent).await? {
		if !entry.is_file {
			continue;
		}
		let Some(name) = entry.file_name.to_str() else {
			continue;
		};
		if let Some(start_index) = parse_wal_segment_filename(wal_path, name) {
			out.push(DiscoveredWalSegment {
				path: parent.join(name),
				start_index,
				legacy: false,
			});
		}
	}
	out.sort_by_key(|segment| segment.start_index);
	for window in out.windows(2) {
		if window[0].start_index == window[1].start_index {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"duplicate wal segment start index {} under {}",
					window[0].start_index,
					parent.display()
				),
			));
		}
	}
	Ok(out)
}

fn parse_wal_segment_filename(wal_path: &Path, name: &str) -> Option<u64> {
	let prefix = wal_segment_file_prefix(wal_path)?;
	let suffix = WAL_SEGMENT_SUFFIX;
	let rest = name.strip_prefix(&prefix)?.strip_suffix(suffix)?;
	rest.parse::<u64>().ok()
}

fn wal_segment_path(wal_path: &Path, start_index: u64) -> std::io::Result<PathBuf> {
	let prefix = wal_segment_file_prefix(wal_path).ok_or_else(|| {
		Error::new(
			ErrorKind::InvalidInput,
			format!("wal path has no filename prefix: {}", wal_path.display()),
		)
	})?;
	Ok(wal_path.with_file_name(format!("{prefix}{start_index:020}{WAL_SEGMENT_SUFFIX}")))
}

fn wal_segment_staging_path(wal_path: &Path, start_index: u64) -> std::io::Result<PathBuf> {
	let prefix = wal_segment_file_prefix(wal_path).ok_or_else(|| {
		Error::new(
			ErrorKind::InvalidInput,
			format!("wal path has no filename prefix: {}", wal_path.display()),
		)
	})?;
	Ok(wal_path.with_file_name(format!(
		"{prefix}{start_index:020}{WAL_SEGMENT_STAGING_SUFFIX}"
	)))
}

fn wal_segment_file_prefix(wal_path: &Path) -> Option<String> {
	Some(format!("{}.", wal_path.file_name()?.to_str()?))
}

async fn ensure_wal_artifacts_absent(
	file_ops: &dyn AsyncFileOps,
	wal_path: &Path,
) -> std::io::Result<()> {
	if file_ops.file_len(wal_path).await?.is_some() {
		return Err(Error::new(
			ErrorKind::AlreadyExists,
			format!("restore target already contains {}", wal_path.display()),
		));
	}
	let named = discover_named_wal_segments(file_ops, wal_path).await?;
	if let Some(segment) = named.first() {
		return Err(Error::new(
			ErrorKind::AlreadyExists,
			format!("restore target already contains {}", segment.path.display()),
		));
	}
	Ok(())
}

async fn create_wal_segment(
	file_ops: &dyn AsyncFileOps,
	wal_path: &Path,
	start_index: u64,
) -> std::io::Result<(PathBuf, FileHandle)> {
	let staging = wal_segment_staging_path(wal_path, start_index)?;
	let final_path = wal_segment_path(wal_path, start_index)?;
	match file_ops.file_len(&staging).await? {
		Some(_) => unlink_wal_artifact(file_ops, &staging).await?,
		None => {}
	}
	let handle = file_ops
		.open(
			&staging,
			OpenFlags {
				read: true,
				write: true,
				create_new: true,
				direct: true,
				..Default::default()
			},
		)
		.await?;
	file_ops.rename(&staging, &final_path).await?;
	fsync_parent_dir(file_ops, &final_path).await?;
	Ok((final_path, handle))
}

async fn open_existing_wal_segment(
	file_ops: &dyn AsyncFileOps,
	path: &Path,
) -> std::io::Result<FileHandle> {
	file_ops
		.open(
			path,
			OpenFlags {
				read: true,
				write: true,
				create: false,
				direct: true,
				..Default::default()
			},
		)
		.await
}

async fn unlink_wal_artifact(file_ops: &dyn AsyncFileOps, path: &Path) -> std::io::Result<()> {
	match file_ops.unlink(path).await {
		Ok(()) => fsync_parent_dir(file_ops, path).await,
		Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
		Err(err) => Err(err),
	}
}

async fn read_wal_backup_bytes(
	file_ops: &dyn AsyncFileOps,
	wal_segments: &[WalSegmentMeta],
	wal_path: &Path,
) -> std::io::Result<Vec<u8>> {
	if wal_segments.is_empty() {
		return read_optional_artifact(file_ops, wal_path).await;
	}
	let mut out = Vec::new();
	for segment in wal_segments {
		let mut bytes = read_optional_artifact(file_ops, &segment.path).await?;
		if bytes.len() > segment.len_bytes as usize {
			bytes.truncate(segment.len_bytes as usize);
		}
		out.extend_from_slice(&bytes);
	}
	Ok(out)
}

async fn load_vote_state(
	file_ops: &dyn AsyncFileOps,
	vote_path: &Path,
) -> std::io::Result<Option<Vote<u32>>> {
	let bytes = read_optional_artifact(file_ops, vote_path).await?;
	if bytes.is_empty() {
		return Ok(None);
	}
	if bytes.len() < 4 {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"vote artifact {} is shorter than its 4-byte length prefix",
				vote_path.display()
			),
		));
	}
	let encoded_len = u32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize;
	if encoded_len == 0 {
		return Ok(None);
	}
	if 4 + encoded_len > bytes.len() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"vote artifact {} declares {} bytes but file length is {}",
				vote_path.display(),
				encoded_len,
				bytes.len()
			),
		));
	}
	postcard::from_bytes(&bytes[4..4 + encoded_len])
		.map(Some)
		.map_err(|err| {
			Error::new(
				ErrorKind::InvalidData,
				format!("vote artifact {} decode failed: {err}", vote_path.display()),
			)
		})
}

async fn load_snapshot_artifact(
	file_ops: &dyn AsyncFileOps,
	snapshot_path: &Path,
) -> std::io::Result<Option<LoadedSnapshot>> {
	if !artifact_exists(file_ops, snapshot_path).await? {
		return Ok(None);
	}
	let (header, state) = decode_snapshot_file(file_ops, snapshot_path)
		.await
		.map_err(|err| {
			Error::new(
				ErrorKind::InvalidData,
				format!(
					"snapshot artifact {} decode failed: {err}",
					snapshot_path.display()
				),
			)
		})?;
	validate_snapshot_header_log_coordinates(&header, state.last_applied_log_id).map_err(
		|err| {
			Error::new(
				ErrorKind::InvalidData,
				format!(
					"snapshot artifact {} header/body identity mismatch: {err}",
					snapshot_path.display()
				),
			)
		},
	)?;
	let body_last = state.last_applied_log_id;
	let snapshot_id = format!("recovered-{}", header.last_included_index);
	Ok(Some(LoadedSnapshot {
		last_included: body_last,
		held: HeldSnapshot {
			meta: SnapshotMeta {
				last_log_id: body_last,
				last_membership: state.last_membership.clone(),
				snapshot_id,
			},
			path: snapshot_path.to_path_buf(),
		},
		state,
	}))
}

fn control_artifact_paths(data_dir: &Path) -> (PathBuf, PathBuf, PathBuf) {
	(
		data_dir.join("control.wal"),
		data_dir.join("vote.wnd"),
		data_dir.join("snapshot.wnd"),
	)
}

fn ensure_artifact_absent(path: &Path) -> std::io::Result<()> {
	if path.exists() {
		return Err(Error::new(
			ErrorKind::AlreadyExists,
			format!("restore target already contains {}", path.display()),
		));
	}
	Ok(())
}

async fn read_optional_artifact(
	file_ops: &dyn AsyncFileOps,
	path: &Path,
) -> std::io::Result<Vec<u8>> {
	match file_ops.read_all(path).await {
		Ok(bytes) => Ok(bytes),
		Err(err) if err.kind() == ErrorKind::NotFound => Ok(Vec::new()),
		Err(err) => Err(err),
	}
}

async fn replay_wal(
	file_ops: &dyn AsyncFileOps,
	wal_path: &Path,
	snapshot: Option<&LoadedSnapshot>,
) -> std::io::Result<WALReplay> {
	let mut replay = WALReplay {
		last_purged: snapshot.and_then(|s| s.last_included),
		..WALReplay::default()
	};
	let discovered = discover_all_wal_segments(file_ops, wal_path).await?;
	if snapshot.is_none()
		&& let Some(first) = discovered.first()
		&& !first.legacy
		&& first.start_index > 1
	{
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"WAL {} begins at retained log index {} but no snapshot provides the purged prefix",
				first.path.display(),
				first.start_index
			),
		));
	}
	for (index, segment) in discovered.iter().enumerate() {
		let allow_torn_tail = index + 1 == discovered.len();
		if let Some(meta) =
			replay_wal_segment(file_ops, segment, snapshot, allow_torn_tail, &mut replay).await?
		{
			replay.wal_segments.push(meta);
		}
	}
	Ok(replay)
}

fn resolve_recovered_committed(
	snapshot: Option<&LoadedSnapshot>,
	entries: &BTreeMap<u64, Entry<WendelonTypeConfig>>,
	checkpoint: Option<RecoveryCheckpoint>,
) -> std::io::Result<Option<LogId<u32>>> {
	let Some(checkpoint) = checkpoint else {
		return Ok(None);
	};
	if checkpoint.commit_index == 0 {
		return Ok(None);
	}
	if let Some(snapshot_log_id) = snapshot.and_then(|loaded| loaded.last_included) {
		if checkpoint.commit_index == snapshot_log_id.index {
			return Ok(Some(snapshot_log_id));
		}
	}
	if let Some(entry) = entries.get(&checkpoint.commit_index) {
		return Ok(Some(entry.log_id));
	}
	Err(Error::new(
		ErrorKind::InvalidData,
		format!(
			"recovery checkpoint commit_index {} is not present in the retained WAL tail or snapshot boundary",
			checkpoint.commit_index
		),
	))
}

async fn replay_wal_segment(
	file_ops: &dyn AsyncFileOps,
	segment: &DiscoveredWalSegment,
	snapshot: Option<&LoadedSnapshot>,
	allow_torn_tail: bool,
	replay: &mut WALReplay,
) -> std::io::Result<Option<WalSegmentMeta>> {
	let bytes = match file_ops.read_all(&segment.path).await {
		Ok(bytes) => bytes,
		Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
		Err(err) => return Err(err),
	};
	if bytes.is_empty() {
		if segment.legacy {
			return Ok(None);
		}
		return Ok(Some(WalSegmentMeta {
			path: segment.path.clone(),
			start_index: segment.start_index,
			end_log_index: None,
			len_bytes: 0,
		}));
	}

	let header_size = std::mem::size_of::<WALRecordHeader>();
	let fixed_size = std::mem::size_of::<RaftLogEntryFixed>();
	let boundary = snapshot.and_then(|s| s.last_included.map(|id| id.index));
	let mut offset: usize = 0;
	let mut last_good: usize = 0;
	let mut first_record_index: Option<u64> = None;
	let mut first_log_entry_index: Option<u64> = None;
	let mut end_log_index: Option<u64> = None;
	while offset < bytes.len() {
		let remaining = bytes.len() - offset;
		if remaining < header_size {
			if !allow_torn_tail {
				return Err(interior_corruption(
					&segment.path,
					offset,
					"sealed segment ends with a partial WAL header".to_string(),
				));
			}
			replay.last_segment_needs_truncate = true;
			break;
		}

		let header: WALRecordHeader = *bytemuck::from_bytes(&bytes[offset..offset + header_size]);
		// Before `body_len` is trusted for anything. A record shaped by a build
		// this one does not know is not a torn tail and not damage, and treating
		// it as either would be a guess presented as a diagnosis.
		if header.format_version > WAL_FORMAT_VERSION {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"wal {} record at offset {} declares format version {}, newer than this \
					 build accepts (supported: {}..={}); the segment is intact but written by \
					 a different build, not corrupt",
					segment.path.display(),
					offset,
					header.format_version,
					PRE_VERSIONING_FORMAT,
					WAL_FORMAT_VERSION
				),
			));
		}
		if first_record_index.is_none() && header.log_index != 0 {
			first_record_index = Some(header.log_index);
		}
		let total_len = header_size
			.checked_add(header.body_len as usize)
			.and_then(|n| n.checked_add(4))
			.ok_or_else(|| {
				Error::new(
					ErrorKind::InvalidData,
					format!(
						"wal {} record at offset {} overflows declared length",
						segment.path.display(),
						offset
					),
				)
			})?;
		let padded_len = page_round_up(total_len);
		if offset + total_len > bytes.len() || offset + padded_len > bytes.len() {
			if !allow_torn_tail {
				return Err(interior_corruption(
					&segment.path,
					offset,
					"sealed segment ends with a torn WAL tail".to_string(),
				));
			}
			replay.last_segment_needs_truncate = true;
			break;
		}

		let body_end = offset + header_size + header.body_len as usize;
		let record_end = body_end + 4;
		let actual_checksum = u32::from_le_bytes(bytes[body_end..record_end].try_into().unwrap());
		if !matches_artifact_checksum_u32(&bytes[offset..body_end], actual_checksum) {
			if allow_torn_tail && offset + padded_len == bytes.len() {
				replay.last_segment_needs_truncate = true;
				break;
			}
			return Err(interior_corruption(
				&segment.path,
				offset,
				"checksum mismatch".to_string(),
			));
		}

		let body = &bytes[offset + header_size..body_end];
		match header.record_type {
			REC_LOG_ENTRY => {
				let entry = decode_log_record(&segment.path, offset, &header, body, fixed_size)?;
				if first_log_entry_index.is_none() {
					first_log_entry_index = Some(entry.log_id.index);
					if segment.start_index != 0 && segment.start_index != entry.log_id.index {
						return Err(interior_corruption(
							&segment.path,
							offset,
							format!(
								"segment start index {} does not match first log entry {}",
								segment.start_index, entry.log_id.index
							),
						));
					}
				}
				end_log_index = Some(entry.log_id.index);
				if boundary
					.map(|idx| entry.log_id.index <= idx)
					.unwrap_or(false)
				{
					// Snapshot already subsumes this entry; keep scanning for
					// corruption and later tail records but do not cache it.
				} else {
					if replay
						.entries
						.insert(entry.log_id.index, clone_entry(&entry))
						.is_some()
					{
						return Err(Error::new(
							ErrorKind::InvalidData,
							format!(
								"wal {} contains duplicate log index {} during recovery",
								segment.path.display(),
								entry.log_id.index
							),
						));
					}
					let resolved_start = first_log_entry_index.unwrap_or(segment.start_index);
					replay.entry_positions.insert(
						entry.log_id.index,
						WalRecordPointer {
							segment_start_index: resolved_start,
							offset: offset as u64,
							len_bytes: padded_len as u64,
						},
					);
					replay.last_log_id = Some(match replay.last_log_id {
						Some(prev) if prev > entry.log_id => prev,
						_ => entry.log_id,
					});
				}
			}
			REC_CHECKPOINT => {
				if body.len() != std::mem::size_of::<WALCheckpointBody>() {
					return Err(interior_corruption(
						&segment.path,
						offset,
						format!("checkpoint body has {} bytes, expected 16", body.len()),
					));
				}
				let mut checkpoint = *bytemuck::from_bytes::<WALCheckpointBody>(body);
				if checkpoint.e_safe_index > checkpoint.commit_index {
					checkpoint.e_safe_index = checkpoint.commit_index;
				}
				let superseded = boundary
					.map(|snapshot_index| checkpoint.commit_index < snapshot_index)
					.unwrap_or(false);
				// A newer snapshot can overtake the last checkpoint in a retained
				// segment before the next rotation. The record remains checksum-
				// validated, but the snapshot supersedes its recovery authority.
				if !superseded {
					replay.checkpoint = Some(RecoveryCheckpoint {
						commit_index: checkpoint.commit_index,
						e_safe_index: checkpoint.e_safe_index,
					});
				}
			}
			other => {
				return Err(interior_corruption(
					&segment.path,
					offset,
					format!("unsupported WAL record_type={other}"),
				));
			}
		}

		last_good = offset + padded_len;
		offset = last_good;
	}

	let resolved_start = if segment.start_index != 0 {
		segment.start_index
	} else {
		first_log_entry_index.or(first_record_index).unwrap_or(0)
	};
	if last_good == 0 && segment.start_index == 0 {
		return Ok(None);
	}
	Ok(Some(WalSegmentMeta {
		path: segment.path.clone(),
		start_index: resolved_start,
		end_log_index,
		len_bytes: last_good as u64,
	}))
}

async fn open_control_artifact(
	file_ops: &dyn AsyncFileOps,
	path: &Path,
) -> std::io::Result<FileHandle> {
	let existed = file_ops.file_len(path).await?.is_some();
	let handle = file_ops
		.open(
			path,
			OpenFlags {
				read: true,
				write: true,
				create: true,
				create_new: false,
				truncate: false,
				direct: true,
			},
		)
		.await?;
	if !existed {
		let parent = path.parent().ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidInput,
				format!("artifact path has no parent directory: {}", path.display()),
			)
		})?;
		file_ops.fsync_dir(parent).await?;
	}
	Ok(handle)
}

fn decode_log_record(
	wal_path: &Path,
	offset: usize,
	header: &WALRecordHeader,
	body: &[u8],
	fixed_size: usize,
) -> std::io::Result<Entry<WendelonTypeConfig>> {
	if body.len() < fixed_size {
		return Err(interior_corruption(
			wal_path,
			offset,
			format!(
				"log record body is shorter than fixed prefix: {}",
				body.len()
			),
		));
	}
	let fixed: RaftLogEntryFixed = *bytemuck::from_bytes(&body[..fixed_size]);
	let payload = &body[fixed_size..];
	if payload.len() != fixed.payload_len as usize {
		return Err(interior_corruption(
			wal_path,
			offset,
			format!(
				"log record payload length mismatch: fixed={} actual={}",
				fixed.payload_len,
				payload.len()
			),
		));
	}
	if !matches_artifact_digest_32(payload, &fixed.payload_hash) {
		return Err(interior_corruption(
			wal_path,
			offset,
			"log record payload hash mismatch".to_string(),
		));
	}
	// The payload hash matched just above, so these bytes are exactly what
	// their writer produced. ADR 0092's dispatch and classification rule hold
	// from here: a superseded version is decoded by its own frozen shape and
	// upconverted, and a hash-matched decode failure at a recognized version
	// is a format mismatch named as such — never interior corruption, which
	// stays reserved for damage and framing contradictions.
	let entry: Entry<WendelonTypeConfig> = match header.format_version {
		1 => crate::consensus::wal_body::v1::decode_entry(payload)
			.map_err(|err| wal_body_format_mismatch(wal_path, offset, 1, &err.to_string()))?,
		2 => crate::consensus::wal_body::v2::decode_entry(payload)
			.map_err(|err| wal_body_format_mismatch(wal_path, offset, 2, &err.to_string()))?,
		3 => crate::consensus::wal_body::v3::decode_entry(payload)
			.map_err(|err| wal_body_format_mismatch(wal_path, offset, 3, &err.to_string()))?,
		_ => match postcard::take_from_bytes::<Entry<WendelonTypeConfig>>(payload) {
			Ok((entry, [])) => entry,
			Ok((_entry, rest)) => {
				return Err(wal_body_format_mismatch(
					wal_path,
					offset,
					header.format_version,
					&format!(
						"log entry decoded with {} unconsumed payload bytes",
						rest.len()
					),
				));
			}
			Err(err) => {
				return Err(wal_body_format_mismatch(
					wal_path,
					offset,
					header.format_version,
					&format!("log entry postcard decode failed: {err}"),
				));
			}
		},
	};
	if entry.log_id.index != fixed.index
		|| entry.log_id.leader_id.term != fixed.term
		|| header.log_index != fixed.index
		|| header.term != fixed.term
	{
		return Err(interior_corruption(
			wal_path,
			offset,
			"log record fixed/header coordinates do not match decoded entry".to_string(),
		));
	}
	Ok(entry)
}

/// ADR 0092: a hash-matched decode failure at a recognized format version.
/// The bytes are exactly what their writer produced, so the mismatch is
/// between shapes, not against damage — the diagnostic names the version the
/// record declares, the version this build writes, and stays out of the
/// corruption vocabulary so the operator reaches for the right remedy.
fn wal_body_format_mismatch(wal_path: &Path, offset: usize, version: u8, detail: &str) -> Error {
	let attribution = if version == PRE_VERSIONING_FORMAT {
		"it predates format versioning — it was very likely written by a build with a \
		 different log-entry shape"
			.to_string()
	} else {
		format!(
			"it declares format version {version} (this build writes {WAL_FORMAT_VERSION}) \
			 yet does not decode with that version's shape"
		)
	};
	Error::new(
		ErrorKind::InvalidData,
		format!(
			"wal {} record at offset {}: {}; the payload hash matched, so the record is \
			 intact and {}; this is a format mismatch, not corruption",
			wal_path.display(),
			offset,
			detail,
			attribution
		),
	)
}

fn interior_corruption(wal_path: &Path, offset: usize, detail: String) -> Error {
	Error::new(
		ErrorKind::InvalidData,
		format!(
			"wal {} interior corruption at offset {}: {}",
			wal_path.display(),
			offset,
			detail
		),
	)
}

fn validate_snapshot_header_log_coordinates(
	header: &SnapshotHeader,
	authoritative: Option<LogId<u32>>,
) -> Result<(), String> {
	let (term, index) = authoritative
		.map(|log_id| (log_id.leader_id.term, log_id.index))
		.unwrap_or((0, 0));
	if header.last_included_term == term && header.last_included_index == index {
		return Ok(());
	}
	Err(format!(
		"header coordinates (term={}, index={}) disagree with authoritative log identity {:?}",
		header.last_included_term, header.last_included_index, authoritative
	))
}

/// Build one page-padded WAL record for a single entry:
/// `[WALRecordHeader][RaftLogEntryFixed][postcard(entry)][profile-hash-prefix checksum]`
/// with the tail zero-padded to `DIRECT_IO_ALIGN`.
fn serialize_entry_page(entry: &Entry<WendelonTypeConfig>) -> Result<AlignedBuf, AnyError> {
	let payload_len =
		postcard::experimental::serialized_size(entry).map_err(|e| AnyError::new(&e))?;
	let header_size = std::mem::size_of::<WALRecordHeader>();
	let fixed_size = std::mem::size_of::<RaftLogEntryFixed>();
	let body_len = fixed_size + payload_len;
	let total = header_size + body_len + 4;
	let padded = page_round_up(total);

	let (kind, epoch) = match &entry.payload {
		EntryPayload::Normal(WendelonRequest::Finalize(m)) => {
			(LogEntryKind::APPEND, m.marker.epoch)
		}
		EntryPayload::Normal(WendelonRequest::AcquireLease(l)) => (LogEntryKind::APPEND, l.epoch),
		EntryPayload::Normal(WendelonRequest::ReleaseLease(l)) => (LogEntryKind::APPEND, l.epoch),
		EntryPayload::Normal(WendelonRequest::AdvanceEpoch(r)) => {
			(LogEntryKind::CONFIG, r.to.epoch)
		}
		EntryPayload::Normal(WendelonRequest::SchemaUpdate(_)) => (LogEntryKind::APPEND, 0),
		EntryPayload::Normal(WendelonRequest::CtasSaga(_))
		| EntryPayload::Normal(WendelonRequest::ReplayLegacyCtasRowDispatched(_)) => {
			(LogEntryKind::APPEND, 0)
		}
		EntryPayload::Normal(WendelonRequest::TierSlab(_)) => (LogEntryKind::APPEND, 0),
		EntryPayload::Normal(WendelonRequest::RetireTieredSlab(_)) => (LogEntryKind::APPEND, 0),
		EntryPayload::Normal(WendelonRequest::CompactionCommit(_)) => (LogEntryKind::APPEND, 0),
		EntryPayload::Normal(WendelonRequest::CreateTenant(_)) => (LogEntryKind::APPEND, 0),
		EntryPayload::Normal(WendelonRequest::UpdateTenantQuota(_)) => (LogEntryKind::APPEND, 0),
		EntryPayload::Normal(WendelonRequest::SuspendTenant(_)) => (LogEntryKind::APPEND, 0),
		EntryPayload::Normal(WendelonRequest::ResumeTenant(_)) => (LogEntryKind::APPEND, 0),
		EntryPayload::Normal(WendelonRequest::DeleteTenant(_)) => (LogEntryKind::APPEND, 0),
		EntryPayload::Normal(WendelonRequest::InstallTenantKekVersion(_)) => {
			(LogEntryKind::APPEND, 0)
		}
		EntryPayload::Normal(WendelonRequest::RetireTenantKekVersion(_)) => {
			(LogEntryKind::APPEND, 0)
		}
		EntryPayload::Normal(WendelonRequest::RevokeActorKey(_)) => (LogEntryKind::APPEND, 0),
		EntryPayload::Normal(WendelonRequest::SetTableRetentionPolicy(_)) => {
			(LogEntryKind::APPEND, 0)
		}
		EntryPayload::Normal(WendelonRequest::TableDropPrepare(_))
		| EntryPayload::Normal(WendelonRequest::TableReclaimCompletion(_))
		| EntryPayload::Normal(WendelonRequest::TableDropFinalize(_))
		| EntryPayload::Normal(WendelonRequest::TableDropLogicalFinalize(_))
		| EntryPayload::Normal(WendelonRequest::TableDropSettlement(_)) => (LogEntryKind::APPEND, 0),
		EntryPayload::Normal(WendelonRequest::SeedMigration(s)) => {
			(LogEntryKind::APPEND, s.entry.to_epoch)
		}
		EntryPayload::Normal(WendelonRequest::MigrationComplete(d)) => {
			(LogEntryKind::APPEND, d.to_epoch)
		}
		EntryPayload::Membership(_) => (LogEntryKind::CONFIG, 0),
		EntryPayload::Blank => (LogEntryKind::APPEND, 0),
	};

	let fixed = RaftLogEntryFixed {
		kind,
		_pad1: [0; 7],
		term: entry.log_id.leader_id.term,
		index: entry.log_id.index,
		epoch,
		leader: SchemaNodeId {
			id: entry.log_id.leader_id.node_id,
			_reserved: 0,
		},
		entry_hlc: HybridTimestamp {
			physical_ms: 0,
			logical: 0,
			node_id: entry.log_id.leader_id.node_id,
		},
		payload_len: payload_len as u32,
		_pad2: [0; 4],
		payload_hash: [0; 32],
	};

	let header = WALRecordHeader {
		record_type: REC_LOG_ENTRY,
		format_version: WAL_FORMAT_VERSION,
		_pad1: [0; 6],
		body_len: body_len as u64,
		log_index: entry.log_id.index,
		term: entry.log_id.leader_id.term,
	};

	let mut buf = AlignedBuf::new(padded);
	let dst = buf.as_mut_slice();
	let payload_start = header_size + fixed_size;
	let payload_end = payload_start + payload_len;
	let payload = postcard::to_slice(entry, &mut dst[payload_start..payload_end])
		.map_err(|e| AnyError::new(&e))?;
	if payload.len() != payload_len {
		return Err(AnyError::error(format!(
			"postcard serialized_size predicted {payload_len} bytes but wrote {}",
			payload.len()
		)));
	}
	let mut fixed = fixed;
	fixed.payload_hash = control_artifact_digest_32(payload);
	dst[..header_size].copy_from_slice(bytemuck::bytes_of(&header));
	dst[header_size..header_size + fixed_size].copy_from_slice(bytemuck::bytes_of(&fixed));
	let checksum = control_artifact_checksum_u32(&dst[..header_size + body_len]);
	dst[header_size + body_len..header_size + body_len + 4]
		.copy_from_slice(&checksum.to_le_bytes());
	// Remainder is already zero from AlignedBuf::new.
	Ok(buf)
}

fn serialize_checkpoint_page(
	commit_index: u64,
	e_safe_index: u64,
	log_index: u64,
	term: u64,
) -> AlignedBuf {
	let header_size = std::mem::size_of::<WALRecordHeader>();
	let body_size = std::mem::size_of::<WALCheckpointBody>();
	let total = header_size + body_size + 4;
	let padded = page_round_up(total);
	let body = WALCheckpointBody {
		commit_index,
		e_safe_index,
	};
	let header = WALRecordHeader {
		record_type: REC_CHECKPOINT,
		format_version: WAL_FORMAT_VERSION,
		_pad1: [0; 6],
		body_len: body_size as u64,
		log_index,
		term,
	};
	let mut buf = AlignedBuf::new(padded);
	let dst = buf.as_mut_slice();
	dst[..header_size].copy_from_slice(bytemuck::bytes_of(&header));
	dst[header_size..header_size + body_size].copy_from_slice(bytemuck::bytes_of(&body));
	let checksum = control_artifact_checksum_u32(&dst[..header_size + body_size]);
	dst[header_size + body_size..header_size + body_size + 4]
		.copy_from_slice(&checksum.to_le_bytes());
	buf
}

/// Encode a complete snapshot: `[SnapshotHeader][postcard(SmState)]`.
fn encode_snapshot(state: &SmState) -> Result<Vec<u8>, AnyError> {
	let body = postcard::to_allocvec(state).map_err(|e| AnyError::new(&e))?;
	let header = snapshot_header_template(state);
	let header = SnapshotHeader {
		state_sha256: control_artifact_digest_32(&body),
		..header
	};
	let mut out = Vec::with_capacity(std::mem::size_of::<SnapshotHeader>() + body.len());
	out.extend_from_slice(bytemuck::bytes_of(&header));
	out.extend_from_slice(&body);
	Ok(out)
}

fn snapshot_header_template(state: &SmState) -> SnapshotHeader {
	let (last_index, last_term) = match state.last_applied_log_id {
		Some(id) => (id.index, id.leader_id.term),
		None => (0, 0),
	};
	SnapshotHeader {
		last_included_index: last_index,
		last_included_term: last_term,
		last_included_epoch: 0,
		snapshot_hlc: HybridTimestamp {
			physical_ms: 0,
			logical: 0,
			node_id: 0,
		},
		state_sha256: [0; 32],
		cache_checkpoint_len: 0,
		format_version: SNAPSHOT_FORMAT_VERSION,
		_pad: [0; 3],
	}
}

async fn ensure_snapshot_parent_dir(
	file_ops: &dyn AsyncFileOps,
	path: &Path,
) -> std::io::Result<()> {
	let parent = path.parent().ok_or_else(|| {
		Error::new(
			ErrorKind::InvalidInput,
			format!("snapshot path has no parent directory: {}", path.display()),
		)
	})?;
	file_ops.create_dir_all(parent).await
}

async fn sync_snapshot_handle(
	file_ops: Arc<dyn AsyncFileOps>,
	handle: FileHandle,
	logical_len: u64,
) -> std::io::Result<()> {
	file_ops.set_len(&handle, logical_len).await?;
	file_ops.fsync(&handle).await
}

async fn persist_snapshot_artifact(
	file_ops: &dyn AsyncFileOps,
	paths: &SnapshotPaths,
	state: &SmState,
) -> std::io::Result<()> {
	ensure_snapshot_parent_dir(file_ops, &paths.current).await?;
	let state = state.clone();
	let handle = file_ops
		.open(
			&paths.build_tmp,
			OpenFlags {
				read: true,
				write: true,
				create: true,
				truncate: true,
				..Default::default()
			},
		)
		.await?;
	let write_handle = handle.clone();
	spawn_blocking_snapshot_io(move || {
		let mut file = write_handle.try_clone()?;
		let placeholder = snapshot_header_template(&state);
		file.write_all(bytemuck::bytes_of(&placeholder))?;
		let writer = HashingWriter::new(&mut file);
		let writer = postcard::to_io(&state, writer)
			.map_err(|err| Error::other(format!("snapshot postcard encode failed: {err}")))?;
		let (file, state_hash) = writer.finalize();
		file.flush()?;
		let header = SnapshotHeader {
			state_sha256: state_hash,
			..placeholder
		};
		file.seek(SeekFrom::Start(0))?;
		file.write_all(bytemuck::bytes_of(&header))?;
		file.flush()?;
		Ok(())
	})
	.await?;
	file_ops.fsync(&handle).await?;
	file_ops.rename(&paths.build_tmp, &paths.current).await?;
	fsync_parent_dir(file_ops, &paths.current).await
}

async fn decode_snapshot_file(
	file_ops: &dyn AsyncFileOps,
	path: &Path,
) -> std::io::Result<(SnapshotHeader, SmState)> {
	let handle = file_ops
		.open(
			path,
			OpenFlags {
				read: true,
				..Default::default()
			},
		)
		.await?;
	spawn_blocking_snapshot_io(move || {
		let file = handle.try_clone()?;
		decode_snapshot_reader(file).map_err(|err| Error::new(ErrorKind::InvalidData, err))
	})
	.await
}

fn decode_snapshot_reader<R: Read>(mut reader: R) -> Result<(SnapshotHeader, SmState), String> {
	let mut header_bytes = [0u8; std::mem::size_of::<SnapshotHeader>()];
	reader
		.read_exact(&mut header_bytes)
		.map_err(|err| format!("snapshot header read: {err}"))?;
	let header: SnapshotHeader = *bytemuck::from_bytes(&header_bytes);
	// Ahead of the digest check on purpose. A version this build does not know
	// describes a body whose shape it cannot reason about, and a digest computed
	// over bytes it cannot interpret would only dress up the confusion.
	if header.format_version > SNAPSHOT_FORMAT_VERSION {
		return Err(format!(
			"snapshot format version {} is newer than this build accepts (supported: {}..={}); \
			 the artifact is intact but written by a different build, not corrupt",
			header.format_version, PRE_VERSIONING_FORMAT, SNAPSHOT_FORMAT_VERSION
		));
	}
	if header.cache_checkpoint_len != 0 {
		return Err(format!(
			"snapshot cache checkpoint len {} is not supported yet",
			header.cache_checkpoint_len
		));
	}
	let mut body = Vec::new();
	reader
		.read_to_end(&mut body)
		.map_err(|err| format!("snapshot body read: {err}"))?;
	let blake3_hash = crate::hash::blake3::digest(&body);
	let sha256_hash = crate::hash::sha256::digest(&body);
	if header.state_sha256 != blake3_hash && header.state_sha256 != sha256_hash {
		return Err(format!(
			"snapshot body hash mismatch: expected {:02x?}, got blake3 {:02x?} or sha256 {:02x?}",
			header.state_sha256, blake3_hash, sha256_hash
		));
	}
	// The digest matched, so the body is exactly what its writer produced. A
	// decode failure here is therefore a shape mismatch, never damage — and for a
	// pre-versioning artifact that is the likeliest reading by far, since it is
	// the case ADR 85 exists to explain.
	// ADR 0092 dispatch: a superseded version with a registered replay
	// decoder is decoded by its own frozen shape and upconverted; anything
	// else decodes by the current shape, with the failure attributed to the
	// version the header declares.
	let state = match header.format_version {
		1 => crate::consensus::snapshot_body::v1::decode_state(&body).map_err(|err| {
			format!(
				"{err}; the body's digest matched, so this artifact is intact, and it \
				 declares snapshot format version 1, which this build replays — failing \
				 that decode means the artifact was not written at its declared version; \
				 this is a format mismatch, not corruption"
			)
		})?,
		2 => crate::consensus::snapshot_body::v2::decode_state(&body).map_err(|err| {
			format!(
				"{err}; the body's digest matched, so this artifact is intact, and it \
				 declares snapshot format version 2, which this build replays — failing \
				 that decode means the artifact was not written at its declared version; \
				 this is a format mismatch, not corruption"
			)
		})?,
		3 => crate::consensus::snapshot_body::v3::decode_state(&body).map_err(|err| {
			format!(
				"{err}; the body's digest matched, so this artifact is intact, and it \
				 declares snapshot format version 3, which this build replays — failing \
				 that decode means the artifact was not written at its declared version; \
				 this is a format mismatch, not corruption"
			)
		})?,
		4 => crate::consensus::snapshot_body::v4::decode_state(&body).map_err(|err| {
			format!(
				"{err}; the body's digest matched, so this artifact is intact, and it \
				 declares snapshot format version 4, which this build replays — failing \
				 that decode means the artifact was not written at its declared version; \
				 this is a format mismatch, not corruption"
			)
		})?,
		_ => decode_snapshot_state_bytes(&body).map_err(|err| {
			if header.format_version == PRE_VERSIONING_FORMAT {
				format!(
					"{err}; the body's digest matched, so this artifact is intact, and it \
					 predates format versioning — it was very likely written by a build with \
					 a different state-machine shape"
				)
			} else if header.format_version < SNAPSHOT_FORMAT_VERSION {
				// An older-but-versioned artifact with no registered replay
				// decoder. Unreachable until a third version exists; kept so
				// the future gap fails with attribution rather than a bare
				// decoder message.
				format!(
					"{err}; the body's digest matched, so this artifact is intact, and it was \
					 written at snapshot format version {} while this build writes {}; this \
					 build has no replay decoder for that version, so the older state cannot \
					 be read",
					header.format_version, SNAPSHOT_FORMAT_VERSION
				)
			} else {
				err
			}
		})?,
	};
	Ok((header, state))
}

fn decode_snapshot_state_bytes(body: &[u8]) -> Result<SmState, String> {
	match postcard::take_from_bytes::<SmState>(body) {
		Ok((state, [])) => Ok(state),
		Ok((_state, trailing)) => Err(format!(
			"snapshot has unexpected trailing bytes: {}",
			trailing.len()
		)),
		Err(err) => Err(format!("snapshot postcard decode failed: {err}")),
	}
}

async fn spawn_blocking_snapshot_io<T, F>(f: F) -> std::io::Result<T>
where
	T: Send + 'static,
	F: FnOnce() -> std::io::Result<T> + Send + 'static,
{
	tokio::task::spawn_blocking(f)
		.await
		.map_err(|err| Error::other(format!("snapshot I/O task failed: {err}")))?
}

#[inline]
fn page_round_up(n: usize) -> usize {
	(n + DIRECT_IO_ALIGN - 1) & !(DIRECT_IO_ALIGN - 1)
}

fn control_artifact_digest_32(bytes: &[u8]) -> [u8; 32] {
	control_artifact_hash::digest(bytes)
}

fn control_artifact_checksum_u32(bytes: &[u8]) -> u32 {
	control_artifact_hash::prefix_u32(bytes)
}

fn matches_artifact_digest_32(bytes: &[u8], expected: &[u8; 32]) -> bool {
	crate::hash::blake3::digest(bytes) == *expected
		|| crate::hash::sha256::digest(bytes) == *expected
}

fn matches_artifact_checksum_u32(bytes: &[u8], expected: u32) -> bool {
	crate::hash::blake3::prefix_u32(bytes) == expected
		|| crate::hash::sha256::prefix_u32(bytes) == expected
}

/// `BTreeMap::range` needs owned `Bound<u64>`s.
fn clone_bound(b: Bound<&u64>) -> Bound<u64> {
	match b {
		Bound::Included(v) => Bound::Included(*v),
		Bound::Excluded(v) => Bound::Excluded(*v),
		Bound::Unbounded => Bound::Unbounded,
	}
}

/// Deep-clone an `Entry<WendelonTypeConfig>`. Centralising the
/// match here gives a single migration target if openraft's
/// derive surface shifts.
fn clone_entry(e: &Entry<WendelonTypeConfig>) -> Entry<WendelonTypeConfig> {
	Entry {
		log_id: e.log_id,
		payload: match &e.payload {
			EntryPayload::Blank => EntryPayload::Blank,
			EntryPayload::Normal(d) => EntryPayload::Normal(d.clone()),
			EntryPayload::Membership(m) => EntryPayload::Membership(m.clone()),
		},
	}
}

#[allow(dead_code)]
fn _unused_request_type_witness(_r: WendelonRequest, _s: WendelonResponse) {}

#[cfg(test)]
mod schema_state_tests {
	use super::*;
	use crate::audit::InMemoryAuditSink;
	use crate::crypto::{TenantKeyset, WrappedTenantKek};
	use crate::integration::compute::{
		COMPUTE_STORAGE_CONTRACT_VERSION, DropOperationId, SchemaGeneration, SchemaIntentId,
		SchemaIntentOrigin, SchemaObjectKind, SchemaObjectRef, SchemaUpdateEnvelope,
		encode_schema_update_envelope_v1,
	};
	use crate::schema::{
		IndexArrowTypeTag, IndexKeyEncoding, IndexObjectId, LocalityPolicy,
		LogicalSchemaFingerprint, MultiRowLayoutDescriptor, PolicyGeneration, PrimaryKeyDescriptor,
		RecordBodyCompressionPolicy, RecordLayoutPolicy, RetentionRejectCode,
		RetentionWindowMillis, RowIdentityPolicy, SchemaEvolutionPolicy, SecondaryIndexDescriptor,
		SecondaryIndexDropRequest, SecondaryIndexKeyDescriptor, SecondaryIndexLifecycleComplete,
		SecondaryIndexState, TableObjectId, TableRetentionPolicy, TableSchemaDescriptor,
		TenantKekId, encode_secondary_index_descriptor, encode_secondary_index_drop_request,
		encode_secondary_index_lifecycle_complete, encode_table_schema_descriptor_v1,
	};
	use crate::storage::cdc_locality_heartbeat_finalization;
	use arrow::datatypes::{DataType, Field, Schema};
	use std::sync::Mutex as StdMutex;
	use tracing::field::{Field as TracingField, Visit};
	use tracing::{Event, Subscriber};
	use tracing_subscriber::layer::Context;
	use tracing_subscriber::prelude::*;
	use tracing_subscriber::registry::LookupSpan;
	use tracing_subscriber::{Layer, Registry};

	#[derive(Clone, Default)]
	struct CapturedEvents(Arc<StdMutex<Vec<Vec<(String, String)>>>>);

	#[derive(Default)]
	struct EventFieldVisitor(Vec<(String, String)>);

	impl EventFieldVisitor {
		fn push(&mut self, field: &TracingField, value: String) {
			self.0.push((field.name().to_owned(), value));
		}
	}

	impl Visit for EventFieldVisitor {
		fn record_bool(&mut self, field: &TracingField, value: bool) {
			self.push(field, value.to_string());
		}

		fn record_i64(&mut self, field: &TracingField, value: i64) {
			self.push(field, value.to_string());
		}

		fn record_u64(&mut self, field: &TracingField, value: u64) {
			self.push(field, value.to_string());
		}

		fn record_str(&mut self, field: &TracingField, value: &str) {
			self.push(field, value.to_owned());
		}

		fn record_debug(&mut self, field: &TracingField, value: &dyn Debug) {
			self.push(field, format!("{value:?}"));
		}
	}

	impl<S> Layer<S> for CapturedEvents
	where
		S: Subscriber + for<'lookup> LookupSpan<'lookup>,
	{
		fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
			let mut visitor = EventFieldVisitor::default();
			event.record(&mut visitor);
			self.0.lock().expect("captured event mutex").push(visitor.0);
		}
	}

	fn settlement_events(events: &CapturedEvents) -> Vec<Vec<(String, String)>> {
		events
			.0
			.lock()
			.expect("captured event mutex")
			.iter()
			.filter(|fields| {
				fields.iter().any(|(name, value)| {
					name == "message" && value.contains("table drop physical reclaim settled")
				})
			})
			.cloned()
			.collect()
	}

	fn finalization_entry(
		index: u64,
		finalization: TenantFinalization,
	) -> Entry<WendelonTypeConfig> {
		Entry {
			log_id: LogId::new(CommittedLeaderId::new(3, 1), index),
			payload: EntryPayload::Normal(WendelonRequest::Finalize(finalization)),
		}
	}

	#[tokio::test]
	async fn exact_cdc_heartbeat_is_not_materialized_as_a_data_write_audit() {
		let heartbeat = cdc_locality_heartbeat_finalization(
			Locality::new(42),
			HybridTimestamp {
				physical_ms: 100,
				logical: 2,
				node_id: 1,
			},
			7,
		);
		let entry = finalization_entry(11, heartbeat);
		assert!(
			raft_audit_event(
				SchemaNodeId {
					id: 1,
					_reserved: 0,
				},
				&entry,
				&WendelonResponse::Success,
			)
			.is_none()
		);
		assert!(
			raft_audit_event(
				SchemaNodeId {
					id: 1,
					_reserved: 0,
				},
				&entry,
				&WendelonResponse::Error("refused".to_owned()),
			)
			.is_none(),
			"failed finalizations retain their pre-ADR-079 no-audit behavior"
		);

		// Exercise the actual apply/materialization seam: the no-op remains
		// authoritative in state and the finalization stream, while the audit
		// tracker is never charged.
		let sink = Arc::new(InMemoryAuditSink::new());
		let tracker = Arc::new(AuditMaterializationTracker::new());
		let mut state_machine = WendelonStateMachine::new().with_audit_materializer(
			SchemaNodeId {
				id: 1,
				_reserved: 0,
			},
			sink.clone(),
			tracker.clone(),
		);
		let mut finalizations = state_machine.subscribe_finalizations();
		let responses = RaftStateMachine::apply(&mut state_machine, [entry])
			.await
			.unwrap();
		assert_eq!(responses, vec![WendelonResponse::Success]);
		assert!(sink.events().is_empty());
		assert_eq!(tracker.in_flight_entries(), 0);
		assert_eq!(tracker.lag_entries(), 0);
		assert_eq!(
			state_machine
				.current_admission_snapshot()
				.e_safe_watermarks
				.get(&7)
				.copied(),
			Some(HybridTimestamp {
				physical_ms: 100,
				logical: 2,
				node_id: 1,
			})
		);
		assert_eq!(finalizations.try_recv().unwrap().epoch, 7);
	}

	#[test]
	fn cdc_heartbeat_near_misses_remain_data_write_audits() {
		let canonical = cdc_locality_heartbeat_finalization(
			Locality::new(42),
			HybridTimestamp {
				physical_ms: 100,
				logical: 2,
				node_id: 1,
			},
			7,
		);
		let mut near_misses = Vec::new();
		let mut changed = canonical.clone();
		changed.tenant_id = TenantId::new(9);
		near_misses.push(changed);
		let mut changed = canonical.clone();
		changed.key_identity = crate::schema::Identity::ZERO;
		near_misses.push(changed);
		let mut changed = canonical.clone();
		changed.marker.payload_hash[0] ^= 1;
		near_misses.push(changed);
		let mut changed = canonical.clone();
		changed.marker.epoch += 1;
		near_misses.push(changed);
		let mut changed = canonical.clone();
		changed.marker.commit_hlc.logical += 1;
		near_misses.push(changed);
		let mut changed = canonical.clone();
		changed.charged_bytes = 1;
		near_misses.push(changed);
		let mut changed = canonical;
		changed.record_count = 1;
		near_misses.push(changed);

		for (offset, near_miss) in near_misses.into_iter().enumerate() {
			let entry = finalization_entry(20 + offset as u64, near_miss);
			let event = raft_audit_event(
				SchemaNodeId {
					id: 1,
					_reserved: 0,
				},
				&entry,
				&WendelonResponse::Success,
			)
			.expect("ADR 079 near-miss finalization must remain audited");
			assert_eq!(event.operation, AuditOperation::DataWriteFinalized);
			assert!(matches!(
				event.schema,
				Some(AuditSchema::DataAccess {
					access_kind: DataAccessKind::Write,
					phase: DataAccessPhase::Finalized,
					..
				})
			));
			let tracker = AuditMaterializationTracker::new();
			tracker.note_authoritative_event();
			assert_eq!(tracker.in_flight_entries(), 1);
			tracker.note_materialized();
			assert_eq!(tracker.in_flight_entries(), 0);
			assert_eq!(tracker.lag_entries(), 0);
		}
	}

	fn table_schema() -> Schema {
		Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("age", DataType::Int64, true),
		])
	}

	fn table_descriptor_for_schema(generation: u64, schema: &Schema) -> TableSchemaDescriptor {
		TableSchemaDescriptor::new(
			TableObjectId::new([4; 16]),
			generation,
			"analytics",
			"events",
			schema,
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
			LocalityPolicy::default_table_spread(),
			SchemaEvolutionPolicy::AdditiveOnly,
		)
		.unwrap()
	}

	fn table_descriptor(generation: u64) -> TableSchemaDescriptor {
		table_descriptor_for_schema(generation, &table_schema())
	}

	fn append_only_table_descriptor(generation: u64) -> TableSchemaDescriptor {
		append_only_table_descriptor_for(generation, TableObjectId::new([8; 16]))
	}

	fn append_only_table_descriptor_for(
		generation: u64,
		table_object_id: TableObjectId,
	) -> TableSchemaDescriptor {
		TableSchemaDescriptor::new_with_layout(
			table_object_id,
			generation,
			"analytics",
			"append_events",
			&table_schema(),
			RowIdentityPolicy::AppendOnly,
			LocalityPolicy::default_table_spread(),
			RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::new(4, 8, 1024 * 1024).unwrap()),
			SchemaEvolutionPolicy::AdditiveOnly,
		)
		.unwrap()
	}

	fn create_tenant() -> CreateTenant {
		let tenant_kek_id = TenantKekId::try_from(1).unwrap();
		let mut tenant_keyset = TenantKeyset::default();
		tenant_keyset.active_tenant_kek_id = Some(tenant_kek_id);
		tenant_keyset.keys.insert(
			tenant_kek_id,
			WrappedTenantKek {
				wrapped_key: vec![1; 40],
			},
		);
		CreateTenant {
			tenant_id: TenantId::new(7),
			quota_bytes: u64::MAX,
			replication_factor: 1,
			tenant_keyset,
		}
	}

	fn state_with_descriptor(descriptor: &TableSchemaDescriptor) -> SmState {
		let mut state = SmState::default();
		assert_eq!(
			apply_create_tenant(&mut state, create_tenant()),
			WendelonResponse::Success
		);
		apply_schema_update(
			&mut state,
			&schema_envelope(
				descriptor,
				0,
				descriptor.schema_generation,
				SchemaMutationKind::Create,
			),
		)
		.unwrap();
		state
	}

	fn state_with_descriptor_and_drop_epoch(descriptor: &TableSchemaDescriptor) -> SmState {
		let mut state = state_with_descriptor(descriptor);
		state.epoch_history.insert(
			7,
			EpochConfig {
				epoch: 7,
				peers: std::collections::BTreeMap::from([(
					1,
					crate::consensus::EpochPeer {
						node_id: SchemaNodeId {
							id: 1,
							_reserved: 0,
						},
						address: "127.0.0.1:7001".to_string(),
						tier_bits: 0,
						role_bits: 1,
						capability_bits: 0,
					},
				)]),
			},
		);
		state
	}

	fn ctas_write_id(byte: u8) -> ClientWriteId {
		let mut client_uuid = [byte; 16];
		client_uuid[6] = 0x70;
		client_uuid[8] = 0x80;
		ClientWriteId {
			client_uuid,
			sequence_no: 7,
			idempotency_ttl_ms: 300_000,
			_pad: 0,
		}
	}

	fn ctas_operation_id(byte: u8) -> CtasOperationId {
		let mut bytes = [byte; 16];
		bytes[..5].fill(0);
		bytes[5] = 1;
		bytes[6] = 0x70;
		bytes[8] = 0x80;
		CtasOperationId::new(bytes)
	}

	fn provision_ctas(
		state: &mut SmState,
		descriptor: TableSchemaDescriptor,
		expected_binding: CtasExpectedBinding,
	) -> CtasSagaEntry {
		let prior_descriptor = match expected_binding {
			CtasExpectedBinding::Absent => None,
			CtasExpectedBinding::Bound {
				table_object_id,
				schema_generation,
			} => state
				.table_schemas
				.get_including_provisional(
					TenantId::new(7),
					table_object_id,
					schema_generation.get(),
				)
				.cloned(),
		};
		let response = apply_ctas_saga_mutation(
			state,
			CtasSagaMutation::Provision {
				tenant_id: TenantId::new(7),
				operation_id: ctas_operation_id(0x54),
				intent_fingerprint: CtasIntentFingerprint::new([0x48; 32]),
				descriptor,
				expected_binding,
				prior_descriptor,
				row_write_id: ctas_write_id(0x54),
				owner_epoch: 1,
				observed_now_ms: 1,
				owner_lease_expires_at_ms: 1_000,
				certificate: None,
			},
		);
		let CtasSagaResponse::Applied(entry) = response else {
			panic!("provision CTAS: {response:?}");
		};
		entry
	}

	fn provision_ctas_with_certificate(
		state: &mut SmState,
		descriptor: TableSchemaDescriptor,
		certificate: Option<Vec<u8>>,
	) -> CtasSagaResponse {
		apply_ctas_saga_mutation(
			state,
			CtasSagaMutation::Provision {
				tenant_id: TenantId::new(7),
				operation_id: ctas_operation_id(0x54),
				intent_fingerprint: CtasIntentFingerprint::new([0x48; 32]),
				descriptor,
				expected_binding: CtasExpectedBinding::Absent,
				prior_descriptor: None,
				row_write_id: ctas_write_id(0x54),
				owner_epoch: 1,
				observed_now_ms: 1,
				owner_lease_expires_at_ms: 1_000,
				certificate,
			},
		)
	}

	fn drive_ctas_to_rows_committed(state: &mut SmState, row_count: u64) {
		let declared = apply_ctas_saga_mutation(
			state,
			CtasSagaMutation::DeclareChain {
				tenant_id: TenantId::new(7),
				operation_id: ctas_operation_id(0x54),
				owner_epoch: 1,
				row_count,
				chunk_count: 1,
			},
		);
		assert!(matches!(declared, CtasSagaResponse::Applied(_)));
		let dispatched = apply_ctas_saga_mutation(
			state,
			CtasSagaMutation::MarkRowDispatched {
				tenant_id: TenantId::new(7),
				operation_id: ctas_operation_id(0x54),
				owner_epoch: 1,
				row_count,
			},
		);
		assert!(matches!(dispatched, CtasSagaResponse::Applied(_)));
		let committed = apply_ctas_saga_mutation(
			state,
			CtasSagaMutation::AttestRowsCommitted {
				tenant_id: TenantId::new(7),
				operation_id: ctas_operation_id(0x54),
				owner_epoch: 1,
				row_count,
			},
		);
		assert!(matches!(committed, CtasSagaResponse::Applied(_)));
		let released = apply_ctas_saga_mutation(
			state,
			CtasSagaMutation::MarkChainPinsReleased {
				tenant_id: TenantId::new(7),
				operation_id: ctas_operation_id(0x54),
				owner_epoch: 1,
			},
		);
		assert!(matches!(released, CtasSagaResponse::Applied(_)));
	}

	#[test]
	fn adr0094_chain_must_be_declared_before_dispatch_and_cleanup_before_publish() {
		let mut state = SmState::default();
		let entry = provision_ctas(
			&mut state,
			append_only_table_descriptor(2),
			CtasExpectedBinding::Absent,
		);
		let dispatch_without_declaration = apply_ctas_saga_mutation(
			&mut state,
			CtasSagaMutation::MarkRowDispatched {
				tenant_id: entry.tenant_id,
				operation_id: entry.operation_id,
				owner_epoch: entry.owner_epoch,
				row_count: 9,
			},
		);
		assert_eq!(
			dispatch_without_declaration,
			CtasSagaResponse::Rejected(CtasSagaReject::InvalidTransition)
		);
		let declared = apply_ctas_saga_mutation(
			&mut state,
			CtasSagaMutation::DeclareChain {
				tenant_id: entry.tenant_id,
				operation_id: entry.operation_id,
				owner_epoch: entry.owner_epoch,
				row_count: 9,
				chunk_count: 3,
			},
		);
		let CtasSagaResponse::Applied(declared) = declared else {
			panic!("declare")
		};
		assert_eq!(declared.chunk_count, Some(3));
		assert!(!declared.chain_pins_released);
	}

	#[test]
	fn adr0087_oversized_certificate_refuses_whole_at_provision() {
		let mut state = SmState::default();
		let descriptor = append_only_table_descriptor(1);
		let object_id = descriptor.table_object_id;
		let response = provision_ctas_with_certificate(
			&mut state,
			descriptor,
			Some(vec![
				0;
				crate::schema::MATERIALIZATION_CERTIFICATE_MAX_BYTES + 1
			]),
		);
		assert_eq!(
			response,
			CtasSagaResponse::Rejected(CtasSagaReject::CertificateTooLarge)
		);
		assert!(
			state
				.ctas_sagas
				.get(TenantId::new(7), ctas_operation_id(0x54))
				.is_none(),
			"an oversized certificate must refuse whole: no saga entry"
		);
		assert!(
			state
				.table_schemas
				.get_including_provisional(TenantId::new(7), object_id, 1)
				.is_none(),
			"an oversized certificate must refuse whole: no provisional descriptor"
		);
	}

	#[test]
	fn adr0087_certificate_binds_with_the_name_in_one_publish_apply() {
		let mut state = SmState::default();
		let descriptor = append_only_table_descriptor(1);
		let object_id = descriptor.table_object_id;
		let certificate = vec![0xC5; crate::schema::MATERIALIZATION_CERTIFICATE_MAX_BYTES];
		let response =
			provision_ctas_with_certificate(&mut state, descriptor, Some(certificate.clone()));
		assert!(
			matches!(response, CtasSagaResponse::Applied(_)),
			"a certificate at exactly the bound is admitted: {response:?}"
		);
		drive_ctas_to_rows_committed(&mut state, 2);

		// The RowsCommitted -> Publish failure window: rows are durable, yet
		// neither the name nor the attachment is visible.
		assert!(
			state
				.table_schemas
				.latest_by_name(TenantId::new(7), "analytics", "append_events")
				.is_none()
		);
		assert!(
			state
				.materialization_certificates
				.get(TenantId::new(7), object_id)
				.is_none()
		);

		let published = apply_ctas_saga_mutation(
			&mut state,
			CtasSagaMutation::Publish {
				tenant_id: TenantId::new(7),
				operation_id: ctas_operation_id(0x54),
				owner_epoch: 1,
			},
		);
		assert!(matches!(published, CtasSagaResponse::Applied(_)));
		assert!(
			state
				.table_schemas
				.latest_by_name(TenantId::new(7), "analytics", "append_events")
				.is_some()
		);
		assert_eq!(
			state
				.materialization_certificates
				.get(TenantId::new(7), object_id),
			Some(certificate.as_slice()),
			"the attachment becomes visible in the same apply as the binding"
		);

		// The attachment outlives the saga entry's bounded detail retention.
		state.ctas_sagas.prune_terminal_detail(u64::MAX);
		assert_eq!(
			state
				.materialization_certificates
				.get(TenantId::new(7), object_id),
			Some(certificate.as_slice()),
			"pruning saga detail must never take the certificate with it"
		);
	}

	#[test]
	fn adr0087_occupied_attachment_slot_refuses_publish_before_the_binding() {
		let mut state = SmState::default();
		let descriptor = append_only_table_descriptor(1);
		let object_id = descriptor.table_object_id;
		state
			.materialization_certificates
			.admit(TenantId::new(7), object_id, vec![0xEE; 4])
			.expect("foreign occupancy for the control");
		let response = provision_ctas_with_certificate(&mut state, descriptor, Some(vec![0xC5; 8]));
		assert!(matches!(response, CtasSagaResponse::Applied(_)));
		drive_ctas_to_rows_committed(&mut state, 2);
		assert_eq!(
			apply_ctas_saga_mutation(
				&mut state,
				CtasSagaMutation::Publish {
					tenant_id: TenantId::new(7),
					operation_id: ctas_operation_id(0x54),
					owner_epoch: 1,
				},
			),
			CtasSagaResponse::Rejected(CtasSagaReject::ForeignProvisionalObject)
		);
		assert!(
			state
				.table_schemas
				.latest_by_name(TenantId::new(7), "analytics", "append_events")
				.is_none(),
			"a refused attachment must leave the binding unpublished: one transition or neither"
		);
		assert_eq!(
			state
				.materialization_certificates
				.get(TenantId::new(7), object_id),
			Some([0xEE; 4].as_slice()),
			"the occupant is immutable"
		);
	}

	#[test]
	fn adr0087_drop_finalize_destroys_the_attachment_with_its_object() {
		let mut state = SmState::default();
		let descriptor = append_only_table_descriptor(1);
		let object_id = descriptor.table_object_id;
		let generation = descriptor.schema_generation;
		let response = provision_ctas_with_certificate(&mut state, descriptor, Some(vec![0xC5; 8]));
		assert!(matches!(response, CtasSagaResponse::Applied(_)));
		drive_ctas_to_rows_committed(&mut state, 2);
		let published = apply_ctas_saga_mutation(
			&mut state,
			CtasSagaMutation::Publish {
				tenant_id: TenantId::new(7),
				operation_id: ctas_operation_id(0x54),
				owner_epoch: 1,
			},
		);
		assert!(matches!(published, CtasSagaResponse::Applied(_)));

		let drop_id = DropOperationId {
			tenant_id: TenantId::new(7),
			table_object_id: object_id,
			schema_generation: SchemaGeneration::new(generation),
		};
		assert_eq!(
			apply_table_drop_prepare(
				&mut state,
				TableDropPrepareRequest::current(drop_id, drop_id.schema_generation),
				10,
			),
			WendelonResponse::Success
		);
		assert_eq!(
			apply_table_reclaim_completion(
				&mut state,
				TableReclaimCompletionRequest::current(drop_id, 10),
				11,
			),
			WendelonResponse::Success
		);
		assert_eq!(
			apply_table_drop_finalize(
				&mut state,
				TableDropFinalizeRequest::current(drop_id, 10, 11),
				12,
			),
			WendelonResponse::Success
		);
		assert!(
			state
				.materialization_certificates
				.get(TenantId::new(7), object_id)
				.is_none(),
			"the certificate never survives its object"
		);
	}

	#[test]
	fn adr0087_plain_ctas_publishes_without_an_attachment() {
		let mut state = SmState::default();
		let descriptor = append_only_table_descriptor(1);
		let object_id = descriptor.table_object_id;
		provision_ctas(&mut state, descriptor, CtasExpectedBinding::Absent);
		drive_ctas_to_rows_committed(&mut state, 2);
		let published = apply_ctas_saga_mutation(
			&mut state,
			CtasSagaMutation::Publish {
				tenant_id: TenantId::new(7),
				operation_id: ctas_operation_id(0x54),
				owner_epoch: 1,
			},
		);
		assert!(matches!(published, CtasSagaResponse::Applied(_)));
		assert!(
			state
				.materialization_certificates
				.get(TenantId::new(7), object_id)
				.is_none()
		);
		assert!(state.materialization_certificates.is_empty());
	}

	#[test]
	fn adr548_provisional_descriptor_is_not_visible_to_by_id_consumers() {
		let descriptor = append_only_table_descriptor(1);
		let table_object_id = descriptor.table_object_id;
		let mut state = SmState::default();
		let entry = provision_ctas(&mut state, descriptor, CtasExpectedBinding::Absent);

		assert_eq!(entry.phase, CtasSagaPhase::Provisioned);
		assert!(
			state
				.table_schemas
				.get_including_provisional(TenantId::new(7), table_object_id, 1)
				.is_some()
		);
		assert!(
			state
				.table_schemas
				.latest_by_id(TenantId::new(7), table_object_id)
				.is_none()
		);
		assert!(
			state
				.table_schemas
				.descriptors_for_tenant(TenantId::new(7))
				.is_empty()
		);
		assert!(
			state
				.table_schemas
				.latest_by_name(TenantId::new(7), "analytics", "append_events")
				.is_none()
		);
	}

	#[test]
	fn adr548_reused_operation_id_with_different_intent_is_refused() {
		let mut state = SmState::default();
		let descriptor = append_only_table_descriptor(1);
		let original = provision_ctas(&mut state, descriptor.clone(), CtasExpectedBinding::Absent);
		let response = apply_ctas_saga_mutation(
			&mut state,
			CtasSagaMutation::Provision {
				tenant_id: TenantId::new(7),
				operation_id: original.operation_id,
				intent_fingerprint: CtasIntentFingerprint::new([0x99; 32]),
				descriptor,
				expected_binding: CtasExpectedBinding::Absent,
				prior_descriptor: None,
				row_write_id: original.row_write_id,
				owner_epoch: 1,
				observed_now_ms: 2,
				owner_lease_expires_at_ms: 1_001,
				certificate: None,
			},
		);
		assert_eq!(
			response,
			CtasSagaResponse::Rejected(CtasSagaReject::IntentMismatch)
		);
		assert_eq!(
			state
				.ctas_sagas
				.get(TenantId::new(7), original.operation_id),
			Some(&original)
		);
	}

	#[test]
	fn adr548_publication_requires_attestation_and_preserves_a_concurrent_winner() {
		let provisional = append_only_table_descriptor(1);
		let mut state = SmState::default();
		provision_ctas(&mut state, provisional, CtasExpectedBinding::Absent);

		assert_eq!(
			apply_ctas_saga_mutation(
				&mut state,
				CtasSagaMutation::Publish {
					tenant_id: TenantId::new(7),
					operation_id: ctas_operation_id(0x54),
					owner_epoch: 1,
				},
			),
			CtasSagaResponse::Rejected(CtasSagaReject::RowsNotCommitted)
		);
		drive_ctas_to_rows_committed(&mut state, 2);

		let winner = append_only_table_descriptor_for(1, TableObjectId::new([0x99; 16]));
		state
			.table_schemas
			.upsert(TenantId::new(7), winner.clone())
			.unwrap();
		assert_eq!(
			apply_ctas_saga_mutation(
				&mut state,
				CtasSagaMutation::Publish {
					tenant_id: TenantId::new(7),
					operation_id: ctas_operation_id(0x54),
					owner_epoch: 1,
				},
			),
			CtasSagaResponse::Rejected(CtasSagaReject::BindingChanged)
		);
		assert_eq!(
			state
				.table_schemas
				.latest_by_name(TenantId::new(7), "analytics", "append_events")
				.unwrap()
				.table_object_id,
			winner.table_object_id
		);
	}

	#[test]
	fn adr548_retiring_old_uses_the_fenced_drop_lifecycle_after_the_name_swap() {
		let old = append_only_table_descriptor_for(1, TableObjectId::new([0x21; 16]));
		let replacement = append_only_table_descriptor_for(1, TableObjectId::new([0x22; 16]));
		let mut state = SmState::default();
		state
			.table_schemas
			.upsert(TenantId::new(7), old.clone())
			.unwrap();
		provision_ctas(
			&mut state,
			replacement.clone(),
			CtasExpectedBinding::Bound {
				table_object_id: old.table_object_id,
				schema_generation: SchemaGeneration::new(old.schema_generation),
			},
		);
		drive_ctas_to_rows_committed(&mut state, 2);
		let _ = apply_ctas_saga_mutation(
			&mut state,
			CtasSagaMutation::Publish {
				tenant_id: TenantId::new(7),
				operation_id: ctas_operation_id(0x54),
				owner_epoch: 1,
			},
		);
		let _ = apply_ctas_saga_mutation(
			&mut state,
			CtasSagaMutation::MarkRetiringOld {
				tenant_id: TenantId::new(7),
				operation_id: ctas_operation_id(0x54),
				owner_epoch: 1,
			},
		);

		let drop_id = DropOperationId {
			tenant_id: TenantId::new(7),
			table_object_id: old.table_object_id,
			schema_generation: SchemaGeneration::new(old.schema_generation),
		};
		assert_eq!(
			apply_table_drop_prepare(
				&mut state,
				TableDropPrepareRequest::current(drop_id, drop_id.schema_generation),
				10,
			),
			WendelonResponse::Success
		);
		assert_eq!(
			apply_table_reclaim_completion(
				&mut state,
				TableReclaimCompletionRequest::current(drop_id, 10),
				11,
			),
			WendelonResponse::Success
		);
		assert_eq!(
			apply_table_drop_finalize(
				&mut state,
				TableDropFinalizeRequest::current(drop_id, 10, 11),
				12,
			),
			WendelonResponse::Success
		);
		assert_eq!(
			state
				.table_schemas
				.latest_by_name(TenantId::new(7), "analytics", "append_events")
				.unwrap()
				.table_object_id,
			replacement.table_object_id,
			"retirement finalization must never unbind the replacement"
		);
	}

	#[test]
	fn adr548_publication_rejects_a_missing_or_foreign_provisional_object() {
		let provisional = append_only_table_descriptor(1);
		let object_id = provisional.table_object_id;
		let mut state = SmState::default();
		provision_ctas(&mut state, provisional, CtasExpectedBinding::Absent);
		drive_ctas_to_rows_committed(&mut state, 1);
		state
			.table_schemas
			.retire_unbound_object(TenantId::new(7), object_id)
			.unwrap();
		assert_eq!(
			apply_ctas_saga_mutation(
				&mut state,
				CtasSagaMutation::Publish {
					tenant_id: TenantId::new(7),
					operation_id: ctas_operation_id(0x54),
					owner_epoch: 1,
				},
			),
			CtasSagaResponse::Rejected(CtasSagaReject::ForeignProvisionalObject)
		);
	}

	#[test]
	fn adr548_takeover_and_terminal_causes_are_typed() {
		let mut state = SmState::default();
		provision_ctas(
			&mut state,
			append_only_table_descriptor(1),
			CtasExpectedBinding::Absent,
		);
		assert_eq!(
			apply_ctas_saga_mutation(
				&mut state,
				CtasSagaMutation::Claim {
					tenant_id: TenantId::new(7),
					operation_id: ctas_operation_id(0x54),
					expected_owner_epoch: 1,
					new_owner_epoch: 2,
					observed_now_ms: 999,
					lease_expires_at_ms: 2_000,
				},
			),
			CtasSagaResponse::Rejected(CtasSagaReject::OwnerLeaseStillLive)
		);
		let CtasSagaResponse::Applied(claimed) = apply_ctas_saga_mutation(
			&mut state,
			CtasSagaMutation::Claim {
				tenant_id: TenantId::new(7),
				operation_id: ctas_operation_id(0x54),
				expected_owner_epoch: 1,
				new_owner_epoch: 2,
				observed_now_ms: 1_000,
				lease_expires_at_ms: 2_000,
			},
		) else {
			panic!("expired ownership must be claimable");
		};
		assert_eq!(claimed.owner_epoch, 2);
		let CtasSagaResponse::Applied(reclaimed) = apply_ctas_saga_mutation(
			&mut state,
			CtasSagaMutation::Reclaim {
				tenant_id: TenantId::new(7),
				operation_id: ctas_operation_id(0x54),
				owner_epoch: 2,
				cause: CtasTerminalCause::OwnerAbandonedBeforeRowDispatch,
				completed_at_ms: 1_001,
			},
		) else {
			panic!("current owner must reclaim an undispatched saga");
		};
		assert_eq!(reclaimed.phase, CtasSagaPhase::Reclaimed);
		assert_eq!(
			reclaimed.terminal_cause,
			Some(CtasTerminalCause::OwnerAbandonedBeforeRowDispatch)
		);
		let after_retention =
			1_001_u64.saturating_add(crate::schema::CTAS_SAGA_DETAIL_RETENTION_MS);
		assert_eq!(
			apply_ctas_saga_mutation(
				&mut state,
				CtasSagaMutation::Provision {
					tenant_id: TenantId::new(7),
					operation_id: ctas_operation_id(0x54),
					intent_fingerprint: CtasIntentFingerprint::new([0x48; 32]),
					descriptor: append_only_table_descriptor(2),
					expected_binding: CtasExpectedBinding::Absent,
					prior_descriptor: None,
					row_write_id: ctas_write_id(0x54),
					owner_epoch: 1,
					observed_now_ms: after_retention,
					owner_lease_expires_at_ms: after_retention.saturating_add(1_000),
					certificate: None,
				},
			),
			CtasSagaResponse::Rejected(CtasSagaReject::Expired)
		);
		assert!(
			state
				.ctas_sagas
				.get(TenantId::new(7), ctas_operation_id(0x54))
				.is_none(),
			"terminal detail must be reclaimed while durable expiry evidence remains"
		);
	}

	#[test]
	fn adr548_retry_debt_releases_nonterminal_ownership_for_a_later_activation() {
		let mut state = SmState::default();
		provision_ctas(
			&mut state,
			append_only_table_descriptor(1),
			CtasExpectedBinding::Absent,
		);
		let _ = apply_ctas_saga_mutation(
			&mut state,
			CtasSagaMutation::DeclareChain {
				tenant_id: TenantId::new(7),
				operation_id: ctas_operation_id(0x54),
				owner_epoch: 1,
				row_count: 3,
				chunk_count: 1,
			},
		);
		let _ = apply_ctas_saga_mutation(
			&mut state,
			CtasSagaMutation::MarkRowDispatched {
				tenant_id: TenantId::new(7),
				operation_id: ctas_operation_id(0x54),
				owner_epoch: 1,
				row_count: 3,
			},
		);
		let CtasSagaResponse::Applied(deferred) = apply_ctas_saga_mutation(
			&mut state,
			CtasSagaMutation::RecordRetryDebt {
				tenant_id: TenantId::new(7),
				operation_id: ctas_operation_id(0x54),
				owner_epoch: 1,
			},
		) else {
			panic!("current owner records retry debt");
		};
		assert_eq!(deferred.phase, CtasSagaPhase::RowDispatched);
		assert_eq!(deferred.retry_debt, 1);
		assert_eq!(deferred.owner_lease_expires_at_ms, 0);
		let CtasSagaResponse::Applied(claimed) = apply_ctas_saga_mutation(
			&mut state,
			CtasSagaMutation::Claim {
				tenant_id: TenantId::new(7),
				operation_id: ctas_operation_id(0x54),
				expected_owner_epoch: 1,
				new_owner_epoch: 2,
				observed_now_ms: 1,
				lease_expires_at_ms: 1_001,
			},
		) else {
			panic!("later activation claims released retry debt");
		};
		assert_eq!(claimed.owner_epoch, 2);
		assert!(!claimed.phase.is_terminal());
	}

	#[test]
	fn adr548_active_saga_capacity_refuses_without_evicting_live_work() {
		let mut state = SmState::default();
		let template = provision_ctas(
			&mut state,
			append_only_table_descriptor(1),
			CtasExpectedBinding::Absent,
		);
		let mut counter = 0_u16;
		while state.ctas_sagas.len() < crate::schema::CTAS_SAGA_CAPACITY {
			let mut operation_bytes = template.operation_id.into_bytes();
			operation_bytes[14..].copy_from_slice(&counter.to_be_bytes());
			let operation_id = CtasOperationId::new(operation_bytes);
			if state
				.ctas_sagas
				.get(TenantId::new(7), operation_id)
				.is_none()
			{
				let mut entry = template.clone();
				entry.operation_id = operation_id;
				state.ctas_sagas.insert(entry);
			}
			counter = counter.checked_add(1).expect("capacity fixture id space");
		}
		let mut refused_bytes = template.operation_id.into_bytes();
		refused_bytes[14..].copy_from_slice(&u16::MAX.to_be_bytes());
		let refused_id = CtasOperationId::new(refused_bytes);
		assert!(state.ctas_sagas.get(TenantId::new(7), refused_id).is_none());
		let before = state.ctas_sagas.len();
		let response = apply_ctas_saga_mutation(
			&mut state,
			CtasSagaMutation::Provision {
				tenant_id: TenantId::new(7),
				operation_id: refused_id,
				intent_fingerprint: CtasIntentFingerprint::new([0x49; 32]),
				descriptor: append_only_table_descriptor(2),
				expected_binding: CtasExpectedBinding::Absent,
				prior_descriptor: None,
				row_write_id: ctas_write_id(0x55),
				owner_epoch: 1,
				observed_now_ms: 1,
				owner_lease_expires_at_ms: 1_000,
				certificate: None,
			},
		);
		assert_eq!(
			response,
			CtasSagaResponse::Rejected(CtasSagaReject::CapacityExhausted)
		);
		assert_eq!(state.ctas_sagas.len(), before);
		assert_eq!(
			state
				.ctas_sagas
				.get(TenantId::new(7), template.operation_id),
			Some(&template),
			"capacity refusal must not evict active work"
		);
	}

	fn schema_envelope(
		descriptor: &TableSchemaDescriptor,
		expected_generation: u64,
		requested_generation: u64,
		mutation: SchemaMutationKind,
	) -> Vec<u8> {
		schema_envelope_for_kind(
			descriptor,
			expected_generation,
			requested_generation,
			mutation,
			SchemaObjectKind::Table,
		)
	}

	fn schema_envelope_for_kind(
		descriptor: &TableSchemaDescriptor,
		expected_generation: u64,
		requested_generation: u64,
		mutation: SchemaMutationKind,
		kind: SchemaObjectKind,
	) -> Vec<u8> {
		let payload = encode_table_schema_descriptor_v1(descriptor).unwrap();
		let envelope = SchemaUpdateEnvelope {
			contract_version: COMPUTE_STORAGE_CONTRACT_VERSION,
			origin: SchemaIntentOrigin::ComputeClient,
			tenant_id: TenantId::new(7),
			object: SchemaObjectRef {
				kind,
				namespace: descriptor.namespace.clone(),
				name: descriptor.table_name.clone(),
			},
			expected_generation: SchemaGeneration::new(expected_generation),
			requested_generation: SchemaGeneration::new(requested_generation),
			intent_id: SchemaIntentId::new([9; 16]),
			mutation,
			payload,
		};
		encode_schema_update_envelope_v1(&envelope).unwrap()
	}

	fn index_descriptor(table: &TableSchemaDescriptor) -> SecondaryIndexDescriptor {
		SecondaryIndexDescriptor {
			index_object_id: IndexObjectId::new([0x52; 16]),
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

	fn index_schema_envelope(
		descriptor: &SecondaryIndexDescriptor,
		expected_generation: u64,
		requested_generation: u64,
		mutation: SchemaMutationKind,
		payload: Vec<u8>,
	) -> Vec<u8> {
		let envelope = SchemaUpdateEnvelope {
			contract_version: COMPUTE_STORAGE_CONTRACT_VERSION,
			origin: SchemaIntentOrigin::WendelonOperator,
			tenant_id: TenantId::new(7),
			object: SchemaObjectRef {
				kind: SchemaObjectKind::Index,
				namespace: descriptor.namespace.clone(),
				name: descriptor.index_name.clone(),
			},
			expected_generation: SchemaGeneration::new(expected_generation),
			requested_generation: SchemaGeneration::new(requested_generation),
			intent_id: SchemaIntentId::new([0x52; 16]),
			mutation,
			payload,
		};
		encode_schema_update_envelope_v1(&envelope).unwrap()
	}

	#[test]
	fn schema_update_applies_table_descriptor_to_state() {
		let descriptor = table_descriptor(1);
		let bytes = schema_envelope(&descriptor, 0, 1, SchemaMutationKind::Create);
		let mut state = SmState::default();
		apply_schema_update(&mut state, &bytes).unwrap();
		assert_eq!(
			state
				.table_schemas
				.get(TenantId::new(7), descriptor.table_object_id, 1),
			Some(&descriptor)
		);
		assert_eq!(
			state
				.table_schemas
				.latest_by_name(TenantId::new(7), "analytics", "events"),
			Some(&descriptor)
		);
		let lifecycle = state
			.table_lifecycle
			.get(TenantId::new(7), descriptor.table_object_id)
			.expect("create should initialize table lifecycle");
		assert_eq!(lifecycle.policy_generation, PolicyGeneration::ZERO);
		assert_eq!(lifecycle.retention, TableRetentionPolicy::InheritCluster);
	}

	#[test]
	fn set_table_retention_policy_accepts_append_only_fixed_window() {
		let descriptor = append_only_table_descriptor(1);
		let mut state = state_with_descriptor(&descriptor);
		let update = SetTableRetentionPolicy {
			tenant_id: TenantId::new(7),
			table_object_id: descriptor.table_object_id,
			expected_policy_generation: PolicyGeneration::ZERO,
			new_policy: TableRetentionPolicy::FixedWindow(
				RetentionWindowMillis::try_new(90_000).unwrap(),
			),
			updated_at_hlc: HybridTimestamp {
				physical_ms: 42,
				logical: 1,
				node_id: 2,
			},
		};

		let (resp, event) =
			apply_request(&mut state, WendelonRequest::SetTableRetentionPolicy(update));

		assert_eq!(resp, WendelonResponse::Success);
		assert!(event.is_none());
		let lifecycle = state
			.table_lifecycle
			.get(TenantId::new(7), descriptor.table_object_id)
			.unwrap();
		assert_eq!(lifecycle.policy_generation, PolicyGeneration::new(1));
		assert_eq!(lifecycle.retention, update.new_policy);
		assert_eq!(lifecycle.updated_at_hlc, update.updated_at_hlc);
	}

	#[test]
	fn set_table_retention_policy_rejects_pk_override() {
		let descriptor = table_descriptor(1);
		let mut state = state_with_descriptor(&descriptor);
		let update = SetTableRetentionPolicy {
			tenant_id: TenantId::new(7),
			table_object_id: descriptor.table_object_id,
			expected_policy_generation: PolicyGeneration::ZERO,
			new_policy: TableRetentionPolicy::Disabled,
			updated_at_hlc: HybridTimestamp {
				physical_ms: 42,
				logical: 0,
				node_id: 2,
			},
		};

		let (resp, _) = apply_request(&mut state, WendelonRequest::SetTableRetentionPolicy(update));

		let WendelonResponse::Error(error) = resp else {
			panic!("expected PK override rejection");
		};
		assert!(error.contains(RetentionRejectCode::RETENTION_REJECTED_PK_DISALLOWED.name()));
		assert_eq!(
			state
				.table_lifecycle
				.current_generation(TenantId::new(7), descriptor.table_object_id),
			PolicyGeneration::ZERO
		);
	}

	#[test]
	fn set_table_retention_policy_rejects_stale_generation_generically() {
		let descriptor = append_only_table_descriptor(1);
		let mut state = state_with_descriptor(&descriptor);
		let first = SetTableRetentionPolicy {
			tenant_id: TenantId::new(7),
			table_object_id: descriptor.table_object_id,
			expected_policy_generation: PolicyGeneration::ZERO,
			new_policy: TableRetentionPolicy::FixedWindow(
				RetentionWindowMillis::try_new(90_000).unwrap(),
			),
			updated_at_hlc: HybridTimestamp {
				physical_ms: 42,
				logical: 0,
				node_id: 2,
			},
		};
		assert_eq!(
			apply_request(&mut state, WendelonRequest::SetTableRetentionPolicy(first)).0,
			WendelonResponse::Success
		);

		let stale = SetTableRetentionPolicy {
			expected_policy_generation: PolicyGeneration::ZERO,
			new_policy: TableRetentionPolicy::Disabled,
			updated_at_hlc: HybridTimestamp {
				physical_ms: 43,
				logical: 0,
				node_id: 2,
			},
			..first
		};
		let (resp, _) = apply_request(&mut state, WendelonRequest::SetTableRetentionPolicy(stale));

		let WendelonResponse::Error(error) = resp else {
			panic!("expected stale-generation rejection");
		};
		assert!(error.contains("stale policy_generation"));
		assert!(!error.contains("RETENTION_REJECTED"));
		assert_eq!(
			state
				.table_lifecycle
				.current_generation(TenantId::new(7), descriptor.table_object_id),
			PolicyGeneration::new(1)
		);
	}

	#[test]
	fn table_retention_policy_update_is_attempt_audited() {
		let update = SetTableRetentionPolicy {
			tenant_id: TenantId::new(7),
			table_object_id: TableObjectId::new([8; 16]),
			expected_policy_generation: PolicyGeneration::new(2),
			new_policy: TableRetentionPolicy::FixedWindow(
				RetentionWindowMillis::try_new(90_000).unwrap(),
			),
			updated_at_hlc: HybridTimestamp {
				physical_ms: 42,
				logical: 1,
				node_id: 2,
			},
		};
		let entry = Entry {
			log_id: LogId::new(CommittedLeaderId::new(3, 0), 11),
			payload: EntryPayload::Normal(WendelonRequest::SetTableRetentionPolicy(update)),
		};

		let event = raft_audit_event(
			SchemaNodeId {
				id: 1,
				_reserved: 0,
			},
			&entry,
			&WendelonResponse::Success,
		)
		.expect("retention policy update must emit audit");

		assert_eq!(event.operation, AuditOperation::TableRetentionPolicyUpdated);
		assert_eq!(
			event
				.parameters
				.get("retention_policy")
				.and_then(|value| value.as_str()),
			Some("fixed_window")
		);
		assert_eq!(
			event
				.parameters
				.get("expected_policy_generation")
				.and_then(|value| value.as_u64()),
			Some(2)
		);
		let Some(AuditSchema::AdministrativeControl { action, detail }) = event.schema else {
			panic!("expected administrative-control audit schema");
		};
		assert_eq!(
			action,
			AdministrativeControlAction::TableRetentionPolicyUpdate
		);
		assert_eq!(detail.target_tenant_id, Some(7));
		assert_eq!(detail.policy_generation, Some(2));
		assert_eq!(detail.retention_policy.as_deref(), Some("fixed_window"));
		assert_eq!(detail.retention_window_ms, Some(90_000));
	}

	#[test]
	fn schema_update_audit_records_descriptor_fingerprints() {
		let descriptor = table_descriptor(1);
		let entry = Entry {
			log_id: LogId::new(CommittedLeaderId::new(3, 0), 12),
			payload: EntryPayload::Normal(WendelonRequest::SchemaUpdate(schema_envelope(
				&descriptor,
				0,
				1,
				SchemaMutationKind::Create,
			))),
		};
		let state = SmState::default();
		let context = schema_audit_context(&state, &entry).expect("schema audit context");
		let event = raft_audit_event_with_context(
			SchemaNodeId {
				id: 1,
				_reserved: 0,
			},
			&entry,
			&WendelonResponse::Success,
			None,
			Some(context),
			None,
		)
		.expect("schema update must emit audit");

		assert_eq!(
			event.operation,
			AuditOperation::TableSchemaDescriptorUpdated
		);
		assert_eq!(
			event
				.parameters
				.get("schema_mutation")
				.and_then(|value| value.as_str()),
			Some("create")
		);
		assert_eq!(
			event
				.parameters
				.get("descriptor_mutation_axis")
				.and_then(|value| value.as_str()),
			Some("combined")
		);
		assert_eq!(
			event
				.parameters
				.get("logical_schema_fingerprint")
				.and_then(|value| value.as_str()),
			Some(format_bytes_hex(descriptor.logical_schema_fingerprint.as_bytes()).as_str())
		);
		assert!(
			event
				.parameters
				.get("previous_logical_schema_fingerprint")
				.is_some_and(serde_json::Value::is_null)
		);
		let Some(AuditSchema::AdministrativeControl { action, detail }) = event.schema else {
			panic!("expected administrative-control audit schema");
		};
		assert_eq!(
			action,
			AdministrativeControlAction::TableSchemaDescriptorUpdate
		);
		assert_eq!(detail.target_tenant_id, Some(7));
		assert_eq!(
			detail.target_table_object_id.as_deref(),
			Some(format_bytes_hex(descriptor.table_object_id.as_bytes()).as_str())
		);
		assert_eq!(detail.schema_mutation.as_deref(), Some("create"));
		assert_eq!(detail.schema_generation, Some(1));
		assert_eq!(detail.previous_schema_generation, None);
		assert_eq!(detail.descriptor_mutation_axis.as_deref(), Some("combined"));
		assert_eq!(
			detail.logical_schema_fingerprint.as_deref(),
			Some(format_bytes_hex(descriptor.logical_schema_fingerprint.as_bytes()).as_str())
		);
		assert_eq!(
			detail.physical_descriptor_fingerprint.as_deref(),
			Some(format_bytes_hex(descriptor.physical_descriptor_fingerprint.as_bytes()).as_str())
		);
	}

	#[test]
	fn schema_update_audit_marks_physical_only_descriptor_retune() {
		let descriptor = table_descriptor(1);
		let mut state = SmState::default();
		apply_schema_update(
			&mut state,
			&schema_envelope(&descriptor, 0, 1, SchemaMutationKind::Create),
		)
		.unwrap();
		let mut retuned = descriptor.clone();
		retuned.schema_generation = 2;
		retuned.record_body_compression_policy = RecordBodyCompressionPolicy::Lz4Frame;
		retuned.refresh_descriptor_fingerprints().unwrap();
		assert_eq!(
			retuned.logical_schema_fingerprint,
			descriptor.logical_schema_fingerprint
		);
		assert_ne!(
			retuned.physical_descriptor_fingerprint,
			descriptor.physical_descriptor_fingerprint
		);
		let entry = Entry {
			log_id: LogId::new(CommittedLeaderId::new(3, 0), 13),
			payload: EntryPayload::Normal(WendelonRequest::SchemaUpdate(schema_envelope(
				&retuned,
				1,
				2,
				SchemaMutationKind::Alter,
			))),
		};
		let context = schema_audit_context(&state, &entry).expect("schema audit context");
		let event = raft_audit_event_with_context(
			SchemaNodeId {
				id: 1,
				_reserved: 0,
			},
			&entry,
			&WendelonResponse::Success,
			None,
			Some(context),
			None,
		)
		.expect("schema update must emit audit");

		assert_eq!(
			event
				.parameters
				.get("schema_mutation")
				.and_then(|value| value.as_str()),
			Some("alter")
		);
		assert_eq!(
			event
				.parameters
				.get("descriptor_mutation_axis")
				.and_then(|value| value.as_str()),
			Some("physical_only")
		);
		assert_eq!(
			event
				.parameters
				.get("previous_schema_generation")
				.and_then(|value| value.as_u64()),
			Some(1)
		);
		assert_eq!(
			event
				.parameters
				.get("previous_physical_descriptor_fingerprint")
				.and_then(|value| value.as_str()),
			Some(format_bytes_hex(descriptor.physical_descriptor_fingerprint.as_bytes()).as_str())
		);
		let Some(AuditSchema::AdministrativeControl { detail, .. }) = event.schema else {
			panic!("expected administrative-control audit schema");
		};
		assert_eq!(detail.schema_generation, Some(2));
		assert_eq!(detail.previous_schema_generation, Some(1));
		assert_eq!(
			detail.descriptor_mutation_axis.as_deref(),
			Some("physical_only")
		);
		assert_eq!(
			detail.previous_logical_schema_fingerprint.as_deref(),
			Some(format_bytes_hex(descriptor.logical_schema_fingerprint.as_bytes()).as_str())
		);
		assert_eq!(
			detail.physical_descriptor_fingerprint.as_deref(),
			Some(format_bytes_hex(retuned.physical_descriptor_fingerprint.as_bytes()).as_str())
		);
	}

	#[test]
	fn secondary_index_schema_update_audit_records_lifecycle_state() {
		let table = table_descriptor(1);
		let mut state = state_with_descriptor(&table);
		let descriptor = index_descriptor(&table);
		let bytes = index_schema_envelope(
			&descriptor,
			0,
			1,
			SchemaMutationKind::Create,
			encode_secondary_index_descriptor(&descriptor).unwrap(),
		);
		let entry = Entry {
			log_id: LogId::new(CommittedLeaderId::new(3, 0), 14),
			payload: EntryPayload::Normal(WendelonRequest::SchemaUpdate(bytes.clone())),
		};
		let mut context =
			index_schema_audit_context(&state, &entry).expect("index schema audit context");
		apply_schema_update(&mut state, &bytes).unwrap();
		finalize_index_schema_audit_context(&state, &mut context);

		let event = raft_audit_event_with_context(
			SchemaNodeId {
				id: 1,
				_reserved: 0,
			},
			&entry,
			&WendelonResponse::Success,
			None,
			None,
			Some(context),
		)
		.expect("index schema update must emit audit");

		assert_eq!(
			event.operation,
			AuditOperation::SecondaryIndexCatalogUpdated
		);
		assert_eq!(
			event
				.parameters
				.get("schema_mutation")
				.and_then(|value| value.as_str()),
			Some("create")
		);
		assert_eq!(
			event
				.parameters
				.get("previous_index_lifecycle_state")
				.and_then(|value| value.as_str()),
			Some("absent")
		);
		assert_eq!(
			event
				.parameters
				.get("index_lifecycle_state")
				.and_then(|value| value.as_str()),
			Some("building")
		);
		assert_eq!(
			event
				.parameters
				.get("indexed_column")
				.and_then(|value| value.as_str()),
			Some("age")
		);
		let Some(AuditSchema::AdministrativeControl { action, detail }) = event.schema else {
			panic!("expected administrative-control audit schema");
		};
		assert_eq!(
			action,
			AdministrativeControlAction::SecondaryIndexCatalogUpdate
		);
		assert_eq!(
			detail.target_index_object_id.as_deref(),
			Some(format_bytes_hex(descriptor.index_object_id.as_bytes()).as_str())
		);
		assert_eq!(
			detail.target_table_object_id.as_deref(),
			Some(format_bytes_hex(descriptor.table_object_id.as_bytes()).as_str())
		);
		assert_eq!(detail.index_lifecycle_state.as_deref(), Some("building"));
		assert_eq!(
			detail.previous_index_lifecycle_state.as_deref(),
			Some("absent")
		);
		assert_eq!(detail.indexed_column.as_deref(), Some("age"));
	}

	#[test]
	fn table_retention_policy_update_audit_marks_irreversible_extension() {
		let update = SetTableRetentionPolicy {
			tenant_id: TenantId::new(7),
			table_object_id: TableObjectId::new([8; 16]),
			expected_policy_generation: PolicyGeneration::new(2),
			new_policy: TableRetentionPolicy::FixedWindow(
				RetentionWindowMillis::try_new(90_000).unwrap(),
			),
			updated_at_hlc: HybridTimestamp {
				physical_ms: 42,
				logical: 1,
				node_id: 2,
			},
		};
		let entry = Entry {
			log_id: LogId::new(CommittedLeaderId::new(3, 0), 11),
			payload: EntryPayload::Normal(WendelonRequest::SetTableRetentionPolicy(update)),
		};

		let event = raft_audit_event_with_context(
			SchemaNodeId {
				id: 1,
				_reserved: 0,
			},
			&entry,
			&WendelonResponse::Success,
			Some(RetentionAuditContext {
				previous_policy: Some(TableRetentionPolicy::FixedWindow(
					RetentionWindowMillis::try_new(60_000).unwrap(),
				)),
			}),
			None,
			None,
		)
		.expect("retention policy update must emit audit");

		assert_eq!(
			event
				.parameters
				.get("previous_retention_policy")
				.and_then(|value| value.as_str()),
			Some("fixed_window")
		);
		assert_eq!(
			event
				.parameters
				.get("retention_extends_window")
				.and_then(|value| value.as_bool()),
			Some(true)
		);
		assert_eq!(
			event
				.parameters
				.get("retention_extension_restores_history")
				.and_then(|value| value.as_bool()),
			Some(false)
		);
		let Some(AuditSchema::AdministrativeControl { detail, .. }) = event.schema else {
			panic!("expected administrative-control audit schema");
		};
		assert_eq!(
			detail.previous_retention_policy.as_deref(),
			Some("fixed_window")
		);
		assert_eq!(detail.previous_retention_window_ms, Some(60_000));
		assert_eq!(detail.retention_extends_window, Some(true));
		assert_eq!(detail.retention_extension_restores_history, Some(false));
		assert!(event.message.contains("retention_extends_window=true"));
		assert!(
			event
				.message
				.contains("retention_extension_restores_history=false")
		);
	}

	#[test]
	fn schema_update_rejects_descriptor_generation_mismatch() {
		let descriptor = table_descriptor(2);
		let bytes = schema_envelope(&descriptor, 0, 1, SchemaMutationKind::Create);
		let mut state = SmState::default();
		let err = apply_schema_update(&mut state, &bytes).unwrap_err();
		assert!(err.contains("does not match requested generation"));
		assert!(state.table_schemas.is_empty());
	}

	#[test]
	fn schema_update_rejects_stale_expected_generation() {
		let descriptor = table_descriptor(1);
		let mut state = SmState::default();
		apply_schema_update(
			&mut state,
			&schema_envelope(&descriptor, 0, 1, SchemaMutationKind::Create),
		)
		.unwrap();

		let next_descriptor = table_descriptor(2);
		let err = apply_schema_update(
			&mut state,
			&schema_envelope(&next_descriptor, 0, 2, SchemaMutationKind::Alter),
		)
		.unwrap_err();
		assert!(err.contains("expected_generation"));
		assert_eq!(
			state
				.table_schemas
				.latest_by_name(TenantId::new(7), "analytics", "events"),
			Some(&descriptor)
		);
	}

	#[test]
	fn schema_update_rejects_non_additive_alter() {
		let descriptor = table_descriptor(1);
		let mut state = SmState::default();
		apply_schema_update(
			&mut state,
			&schema_envelope(&descriptor, 0, 1, SchemaMutationKind::Create),
		)
		.unwrap();

		let changed_type_schema = Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("age", DataType::Utf8, true),
		]);
		let changed_type = table_descriptor_for_schema(2, &changed_type_schema);
		let err = apply_schema_update(
			&mut state,
			&schema_envelope(&changed_type, 1, 2, SchemaMutationKind::Alter),
		)
		.unwrap_err();
		assert!(err.contains("change type"));
		assert_eq!(
			state
				.table_schemas
				.latest_by_name(TenantId::new(7), "analytics", "events"),
			Some(&descriptor)
		);
		assert!(
			state
				.table_schemas
				.get(TenantId::new(7), changed_type.table_object_id, 2)
				.is_none()
		);
	}

	#[test]
	fn schema_update_reports_existence_before_generation_for_a_duplicate_create() {
		// A create always carries expected_generation ZERO, so an existing table
		// fails the generation comparison as well. The caller must be told the
		// table exists, not that its generation is stale: under the previous
		// ordering this assertion saw
		// "expected_generation 0 does not match current generation 1".
		let descriptor = table_descriptor(1);
		let mut state = SmState::default();
		apply_schema_update(
			&mut state,
			&schema_envelope(&descriptor, 0, 1, SchemaMutationKind::Create),
		)
		.unwrap();

		let err = apply_schema_update(
			&mut state,
			&schema_envelope(&descriptor, 0, 1, SchemaMutationKind::Create),
		)
		.unwrap_err();
		assert!(
			err.contains("cannot replace an existing table"),
			"duplicate create must report existence, got: {err}"
		);
		assert!(
			!err.contains("does not match current generation"),
			"existence must win over the generic generation mismatch, got: {err}"
		);

		// An alter with a genuinely stale generation still reports the generation.
		let stale = apply_schema_update(
			&mut state,
			&schema_envelope(&descriptor, 0, 2, SchemaMutationKind::Alter),
		)
		.unwrap_err();
		assert!(
			stale.contains("does not match current generation"),
			"alter must still surface a stale generation, got: {stale}"
		);
	}

	#[test]
	fn schema_update_rejects_create_reusing_table_object_id() {
		let descriptor = table_descriptor(1);
		let mut state = SmState::default();
		apply_schema_update(
			&mut state,
			&schema_envelope(&descriptor, 0, 1, SchemaMutationKind::Create),
		)
		.unwrap();

		let conflicting = TableSchemaDescriptor::new(
			descriptor.table_object_id,
			1,
			"analytics",
			"events_copy",
			&table_schema(),
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
			LocalityPolicy::default_table_spread(),
			SchemaEvolutionPolicy::AdditiveOnly,
		)
		.unwrap();
		let err = apply_schema_update(
			&mut state,
			&schema_envelope(&conflicting, 0, 1, SchemaMutationKind::Create),
		)
		.unwrap_err();
		assert!(err.contains("reuse an existing table_object_id"));
		assert_eq!(
			state
				.table_schemas
				.latest_by_name(TenantId::new(7), "analytics", "events"),
			Some(&descriptor)
		);
		assert!(
			state
				.table_schemas
				.latest_by_name(TenantId::new(7), "analytics", "events_copy")
				.is_none()
		);
	}

	#[test]
	fn schema_update_rejects_descriptor_fingerprint_corruption() {
		let mut descriptor = table_descriptor(1);
		let mut fingerprint = *descriptor.logical_schema_fingerprint.as_bytes();
		fingerprint[0] ^= 0xff;
		descriptor.logical_schema_fingerprint = LogicalSchemaFingerprint::from_bytes(fingerprint);
		let payload = postcard::to_allocvec(&descriptor).unwrap();
		let envelope = SchemaUpdateEnvelope {
			contract_version: COMPUTE_STORAGE_CONTRACT_VERSION,
			origin: SchemaIntentOrigin::ComputeClient,
			tenant_id: TenantId::new(7),
			object: SchemaObjectRef {
				kind: SchemaObjectKind::Table,
				namespace: descriptor.namespace.clone(),
				name: descriptor.table_name.clone(),
			},
			expected_generation: SchemaGeneration::ZERO,
			requested_generation: SchemaGeneration::new(descriptor.schema_generation),
			intent_id: SchemaIntentId::new([9; 16]),
			mutation: SchemaMutationKind::Create,
			payload,
		};
		let bytes = encode_schema_update_envelope_v1(&envelope).unwrap();
		let mut state = SmState::default();
		let err = apply_schema_update(&mut state, &bytes).unwrap_err();
		assert!(err.contains("fingerprint"));
		assert!(state.table_schemas.is_empty());
	}

	#[test]
	fn schema_update_rejects_descriptor_envelope_object_mismatch() {
		let descriptor = table_descriptor(1);
		let payload = encode_table_schema_descriptor_v1(&descriptor).unwrap();
		let envelope = SchemaUpdateEnvelope {
			contract_version: COMPUTE_STORAGE_CONTRACT_VERSION,
			origin: SchemaIntentOrigin::ComputeClient,
			tenant_id: TenantId::new(7),
			object: SchemaObjectRef {
				kind: SchemaObjectKind::Table,
				namespace: "analytics".to_owned(),
				name: "events_shadow".to_owned(),
			},
			expected_generation: SchemaGeneration::ZERO,
			requested_generation: SchemaGeneration::new(descriptor.schema_generation),
			intent_id: SchemaIntentId::new([9; 16]),
			mutation: SchemaMutationKind::Create,
			payload,
		};
		let bytes = encode_schema_update_envelope_v1(&envelope).unwrap();
		let mut state = SmState::default();
		let err = apply_schema_update(&mut state, &bytes).unwrap_err();
		assert!(err.contains("object does not match"));
		assert!(state.table_schemas.is_empty());
	}

	#[test]
	fn schema_update_rejects_drop() {
		let descriptor = table_descriptor(1);
		let bytes = schema_envelope(&descriptor, 1, 2, SchemaMutationKind::Drop);
		let mut state = SmState::default();
		let err = apply_schema_update(&mut state, &bytes).unwrap_err();
		assert!(err.contains("drops require"));
		assert!(state.table_schemas.is_empty());
	}

	#[test]
	fn table_drop_protocol_fences_completes_and_finalizes() {
		let descriptor = table_descriptor(1);
		let mut state = state_with_descriptor(&descriptor);
		let operation_id = DropOperationId {
			tenant_id: TenantId::new(7),
			table_object_id: descriptor.table_object_id,
			schema_generation: SchemaGeneration::new(descriptor.schema_generation),
		};

		assert_eq!(
			apply_table_drop_prepare(
				&mut state,
				TableDropPrepareRequest::current(
					operation_id,
					SchemaGeneration::new(descriptor.schema_generation),
				),
				10,
			),
			WendelonResponse::Success
		);
		let fence = state
			.table_drop_fences
			.get(TenantId::new(7), descriptor.table_object_id)
			.expect("drop fence");
		assert_eq!(fence.fence_log_index, 10);
		assert_eq!(fence.reclaim_completion_log_index, None);

		assert_eq!(
			apply_table_reclaim_completion(
				&mut state,
				TableReclaimCompletionRequest::current(operation_id, 10),
				11,
			),
			WendelonResponse::Success
		);
		assert_eq!(
			state
				.table_drop_fences
				.get(TenantId::new(7), descriptor.table_object_id)
				.and_then(|fence| fence.reclaim_completion_log_index),
			Some(11)
		);

		assert_eq!(
			apply_table_drop_finalize(
				&mut state,
				TableDropFinalizeRequest::current(operation_id, 10, 11),
				12,
			),
			WendelonResponse::Success
		);
		assert!(
			state
				.table_schemas
				.latest_by_name(TenantId::new(7), "analytics", "events")
				.is_none()
		);
		assert!(
			state
				.table_schemas
				.latest_by_id(TenantId::new(7), descriptor.table_object_id)
				.is_some()
		);
		assert!(
			state
				.table_drop_fences
				.get(TenantId::new(7), descriptor.table_object_id)
				.is_none()
		);
		assert!(
			state
				.table_lifecycle
				.get(TenantId::new(7), descriptor.table_object_id)
				.is_none()
		);
		assert_eq!(
			state
				.table_drop_outcomes
				.get(TenantId::new(7), descriptor.table_object_id),
			Some(&TableDropOutcome {
				operation_id,
				finalize_log_index: 12,
			})
		);

		let finalized_state = postcard::to_stdvec(&state).unwrap();
		assert_eq!(
			apply_table_drop_finalize(
				&mut state,
				TableDropFinalizeRequest::current(operation_id, 10, 11),
				12,
			),
			WendelonResponse::Success,
			"an identical finalize replay succeeds before consulting the consumed fence"
		);
		assert_eq!(
			postcard::to_stdvec(&state).unwrap(),
			finalized_state,
			"an identical finalize replay is state-idempotent"
		);
	}

	#[test]
	fn adr0093_conflicting_finalize_is_rejected_without_any_state_mutation() {
		let descriptor = table_descriptor(1);
		let mut state = state_with_descriptor(&descriptor);
		let operation_id = DropOperationId {
			tenant_id: TenantId::new(7),
			table_object_id: descriptor.table_object_id,
			schema_generation: SchemaGeneration::new(descriptor.schema_generation),
		};
		assert_eq!(
			apply_table_drop_prepare(
				&mut state,
				TableDropPrepareRequest::current(operation_id, operation_id.schema_generation),
				10,
			),
			WendelonResponse::Success
		);
		assert_eq!(
			apply_table_reclaim_completion(
				&mut state,
				TableReclaimCompletionRequest::current(operation_id, 10),
				11,
			),
			WendelonResponse::Success
		);
		assert_eq!(
			apply_table_drop_finalize(
				&mut state,
				TableDropFinalizeRequest::current(operation_id, 10, 11),
				12,
			),
			WendelonResponse::Success
		);

		let before = postcard::to_stdvec(&state).unwrap();
		let foreign_operation = DropOperationId {
			schema_generation: SchemaGeneration::new(operation_id.schema_generation.get() + 1),
			..operation_id
		};
		assert_ne!(foreign_operation, operation_id);
		let response = apply_table_drop_finalize(
			&mut state,
			TableDropFinalizeRequest::current(foreign_operation, 10, 11),
			12,
		);
		match response {
			WendelonResponse::Error(message) => {
				assert!(message.contains("outcome conflict"), "{message}");
			}
			other => panic!("expected conflicting finalize refusal, got {other:?}"),
		}
		assert_eq!(
			postcard::to_stdvec(&state).unwrap(),
			before,
			"a rejected conflicting finalize leaves the entire state byte-identical"
		);
	}

	#[test]
	fn adr0095_logical_finalize_is_atomic_and_settlement_requires_the_exact_stamp() {
		let descriptor = table_descriptor(1);
		let mut state = state_with_descriptor_and_drop_epoch(&descriptor);
		let operation_id = DropOperationId {
			tenant_id: TenantId::new(7),
			table_object_id: descriptor.table_object_id,
			schema_generation: SchemaGeneration::new(descriptor.schema_generation),
		};
		assert_eq!(
			apply_table_drop_prepare(
				&mut state,
				TableDropPrepareRequest::current(operation_id, operation_id.schema_generation),
				10,
			),
			WendelonResponse::Success
		);

		let fenced_state = postcard::to_stdvec(&state).unwrap();
		let rejected = apply_table_drop_logical_finalize(
			&mut state,
			TableDropLogicalFinalizeRequest {
				operation_id,
				fence_log_index: 9,
			},
			11,
		);
		assert!(matches!(rejected, WendelonResponse::Error(_)));
		assert_eq!(
			postcard::to_stdvec(&state).unwrap(),
			fenced_state,
			"a rejected logical finalize leaves the complete replicated state unchanged"
		);

		assert_eq!(
			apply_table_drop_logical_finalize(
				&mut state,
				TableDropLogicalFinalizeRequest {
					operation_id,
					fence_log_index: 10,
				},
				11,
			),
			WendelonResponse::Success
		);
		assert!(
			state.table_drop_blocks_admission(operation_id.tenant_id, operation_id.table_object_id)
		);
		assert!(
			state
				.table_drop_fences
				.get(operation_id.tenant_id, operation_id.table_object_id)
				.is_none()
		);
		let tombstone = *state
			.table_drop_tombstones
			.get(operation_id.tenant_id, operation_id.table_object_id)
			.expect("logical finalize installs an admission tombstone");
		let debt = state
			.table_drop_reclaim_debts
			.get(operation_id.tenant_id, operation_id.table_object_id)
			.cloned()
			.expect("logical finalize installs durable reclaim debt");
		assert_eq!(tombstone.finalize_log_index, 11);
		assert_eq!(debt.finalize_log_index, 11);
		assert_eq!(debt.ownership_epoch, 7);
		assert_eq!(
			state
				.table_drop_outcomes
				.get(operation_id.tenant_id, operation_id.table_object_id),
			Some(&TableDropOutcome {
				operation_id,
				finalize_log_index: 11,
			})
		);
		assert!(
			state
				.table_schemas
				.latest_by_name(operation_id.tenant_id, "analytics", "events")
				.is_none(),
			"name publication is removed in the same apply"
		);

		let finalized_state = postcard::to_stdvec(&state).unwrap();
		let rejected = apply_table_drop_settlement(
			&mut state,
			TableDropSettlementRequest {
				operation_id,
				finalize_log_index: 11,
				ownership_epoch: 8,
				attested_participants: debt.participants.clone(),
			},
		);
		assert!(matches!(rejected, WendelonResponse::Error(_)));
		assert_eq!(
			postcard::to_stdvec(&state).unwrap(),
			finalized_state,
			"a stale ownership epoch cannot mutate settlement state"
		);
		let mut incomplete = debt.participants.iter().collect::<Vec<_>>();
		incomplete.pop();
		let rejected = apply_table_drop_settlement(
			&mut state,
			TableDropSettlementRequest {
				operation_id,
				finalize_log_index: 11,
				ownership_epoch: 7,
				attested_participants: crate::schema::TableDropParticipantSet::new(incomplete),
			},
		);
		assert!(matches!(rejected, WendelonResponse::Error(_)));
		assert_eq!(
			postcard::to_stdvec(&state).unwrap(),
			finalized_state,
			"missing participant evidence cannot partially settle durable state"
		);
		let mut over_attested = debt.participants.iter().collect::<Vec<_>>();
		over_attested.push(crate::schema::NodeId {
			id: 99,
			_reserved: 0,
		});
		let rejected = apply_table_drop_settlement(
			&mut state,
			TableDropSettlementRequest {
				operation_id,
				finalize_log_index: 11,
				ownership_epoch: 7,
				attested_participants: crate::schema::TableDropParticipantSet::new(over_attested),
			},
		);
		assert!(matches!(rejected, WendelonResponse::Error(_)));
		assert_eq!(
			postcard::to_stdvec(&state).unwrap(),
			finalized_state,
			"an over-attested set cannot substitute for the exact participant stamp"
		);

		assert_eq!(
			apply_table_drop_settlement(
				&mut state,
				TableDropSettlementRequest {
					operation_id,
					finalize_log_index: 11,
					ownership_epoch: 7,
					attested_participants: debt.participants,
				},
			),
			WendelonResponse::Success
		);
		assert!(
			!state
				.table_drop_blocks_admission(operation_id.tenant_id, operation_id.table_object_id)
		);
		assert!(
			state
				.table_schemas
				.latest_by_id(operation_id.tenant_id, operation_id.table_object_id)
				.is_none(),
			"settlement retires the already-unbound descriptor"
		);
		assert!(
			state
				.table_drop_reclaim_debts
				.get(operation_id.tenant_id, operation_id.table_object_id)
				.is_none()
		);
		assert!(
			state
				.table_drop_tombstones
				.get(operation_id.tenant_id, operation_id.table_object_id)
				.is_none()
		);
	}

	#[tokio::test(flavor = "current_thread")]
	async fn adr0096_live_apply_owns_the_authoritative_gauge_and_single_settlement_signal() {
		let descriptor = table_descriptor(1);
		let operation_id = DropOperationId {
			tenant_id: TenantId::new(7),
			table_object_id: descriptor.table_object_id,
			schema_generation: SchemaGeneration::new(descriptor.schema_generation),
		};
		let registry = Box::leak(Box::new(MetricsRegistry::default()));
		let mut state_machine = WendelonStateMachine::new()
			.with_observability_node_id(SchemaNodeId {
				id: 17,
				_reserved: 0,
			})
			.with_observability_metrics_for_test(registry)
			.await;
		{
			let mut inner = state_machine.inner.write().await;
			inner.state = state_with_descriptor_and_drop_epoch(&descriptor);
		}
		registry.set_drop_reclaim_debts(0);

		let events = CapturedEvents::default();
		let subscriber = Registry::default().with(events.clone());
		let _subscriber_guard = tracing::subscriber::set_default(subscriber);
		let apply = |index, request| Entry {
			log_id: LogId::new(CommittedLeaderId::new(1, 17), index),
			payload: EntryPayload::Normal(request),
		};

		let prepared = RaftStateMachine::apply(
			&mut state_machine,
			[apply(
				10,
				WendelonRequest::TableDropPrepare(TableDropPrepareRequest::current(
					operation_id,
					operation_id.schema_generation,
				)),
			)],
		)
		.await
		.unwrap();
		assert_eq!(prepared, vec![WendelonResponse::Success]);
		assert_eq!(registry.drop_reclaim_debts_for_test(), 0);

		let finalized = RaftStateMachine::apply(
			&mut state_machine,
			[apply(
				11,
				WendelonRequest::TableDropLogicalFinalize(TableDropLogicalFinalizeRequest {
					operation_id,
					fence_log_index: 10,
				}),
			)],
		)
		.await
		.unwrap();
		assert_eq!(finalized, vec![WendelonResponse::Success]);
		assert_eq!(registry.drop_reclaim_debts_for_test(), 1);
		let debt = state_machine
			.state_snapshot()
			.await
			.table_drop_reclaim_debts
			.get(operation_id.tenant_id, operation_id.table_object_id)
			.cloned()
			.unwrap();

		let stale = RaftStateMachine::apply(
			&mut state_machine,
			[apply(
				12,
				WendelonRequest::TableDropSettlement(TableDropSettlementRequest {
					operation_id,
					finalize_log_index: debt.finalize_log_index,
					ownership_epoch: debt.ownership_epoch + 1,
					attested_participants: debt.participants.clone(),
				}),
			)],
		)
		.await
		.unwrap();
		assert!(matches!(stale.as_slice(), [WendelonResponse::Error(_)]));
		assert_eq!(registry.drop_reclaim_debts_for_test(), 1);
		assert_eq!(registry.drop_reclaim_settlements_for_test(), 0);

		let settlement = TableDropSettlementRequest {
			operation_id,
			finalize_log_index: debt.finalize_log_index,
			ownership_epoch: debt.ownership_epoch,
			attested_participants: debt.participants,
		};
		let settled = RaftStateMachine::apply(
			&mut state_machine,
			[apply(
				13,
				WendelonRequest::TableDropSettlement(settlement.clone()),
			)],
		)
		.await
		.unwrap();
		assert_eq!(settled, vec![WendelonResponse::Success]);
		assert_eq!(registry.drop_reclaim_debts_for_test(), 0);
		assert_eq!(registry.drop_reclaim_settlements_for_test(), 1);

		let duplicate = RaftStateMachine::apply(
			&mut state_machine,
			[apply(
				14,
				WendelonRequest::TableDropSettlement(settlement.clone()),
			)],
		)
		.await
		.unwrap();
		assert!(matches!(duplicate.as_slice(), [WendelonResponse::Error(_)]));
		assert_eq!(registry.drop_reclaim_settlements_for_test(), 1);

		let lines = settlement_events(&events);
		assert_eq!(
			lines.len(),
			1,
			"one live successful apply emits once: {lines:?}"
		);
		for required in [
			"tenant_id",
			"table_object_id",
			"schema_generation",
			"finalize_log_index",
			"participant_count",
			"ownership_epoch",
			"local_node_id",
		] {
			assert!(
				lines[0].iter().any(|(name, _)| name == required),
				"settlement line lacks {required}: {:?}",
				lines[0]
			);
		}

		// Recovery calls this pure function directly. Replaying the same shape on
		// a finalized state mutates consensus state but cannot emit process-lifetime
		// settlement telemetry.
		let mut replay_state = state_with_descriptor_and_drop_epoch(&descriptor);
		assert_eq!(
			apply_table_drop_prepare(
				&mut replay_state,
				TableDropPrepareRequest::current(operation_id, operation_id.schema_generation),
				10,
			),
			WendelonResponse::Success
		);
		assert_eq!(
			apply_table_drop_logical_finalize(
				&mut replay_state,
				TableDropLogicalFinalizeRequest {
					operation_id,
					fence_log_index: 10,
				},
				11,
			),
			WendelonResponse::Success
		);
		let replay_entry = apply(13, WendelonRequest::TableDropSettlement(settlement));
		assert_eq!(
			apply_entry_to_state(&mut replay_state, &replay_entry).0,
			WendelonResponse::Success
		);
		assert_eq!(registry.drop_reclaim_settlements_for_test(), 1);
		assert_eq!(settlement_events(&events).len(), 1);
	}

	#[tokio::test]
	async fn adr0095_committed_prepare_closes_mutation_admission_and_drains_prior_work() {
		let descriptor = table_descriptor(1);
		let operation_id = DropOperationId {
			tenant_id: TenantId::new(7),
			table_object_id: descriptor.table_object_id,
			schema_generation: SchemaGeneration::new(descriptor.schema_generation),
		};
		let mut state_machine = WendelonStateMachine::new();
		{
			let mut inner = state_machine.inner.write().await;
			inner.state = state_with_descriptor(&descriptor);
		}
		let registry = state_machine.mutation_drain_registry();
		let pre_fence = registry
			.admit(
				operation_id.tenant_id,
				operation_id.table_object_id,
				operation_id.schema_generation.get(),
			)
			.unwrap();
		let responses = RaftStateMachine::apply(
			&mut state_machine,
			[Entry {
				log_id: LogId::new(CommittedLeaderId::new(1, 1), 10),
				payload: EntryPayload::Normal(WendelonRequest::TableDropPrepare(
					TableDropPrepareRequest::current(operation_id, operation_id.schema_generation),
				)),
			}],
		)
		.await
		.unwrap();
		assert_eq!(responses, vec![WendelonResponse::Success]);
		assert!(
			registry
				.admit(
					operation_id.tenant_id,
					operation_id.table_object_id,
					operation_id.schema_generation.get(),
				)
				.is_err()
		);

		let waiting = {
			let registry = registry.clone();
			tokio::spawn(async move { registry.wait_until_drained(operation_id).await })
		};
		tokio::task::yield_now().await;
		assert!(!waiting.is_finished());
		drop(pre_fence);
		waiting.await.unwrap().unwrap();
	}

	#[test]
	fn table_drop_prepare_rejects_dependent_secondary_indexes() {
		let descriptor = table_descriptor(1);
		let mut state = state_with_descriptor(&descriptor);
		let index = index_descriptor(&descriptor);
		state
			.secondary_indexes
			.create(TenantId::new(7), index, &state.table_schemas)
			.expect("create secondary index");
		let operation_id = DropOperationId {
			tenant_id: TenantId::new(7),
			table_object_id: descriptor.table_object_id,
			schema_generation: SchemaGeneration::new(descriptor.schema_generation),
		};

		let response = apply_table_drop_prepare(
			&mut state,
			TableDropPrepareRequest::current(
				operation_id,
				SchemaGeneration::new(descriptor.schema_generation),
			),
			10,
		);

		assert!(
			matches!(response, WendelonResponse::Error(message) if message.contains("dependent secondary indexes"))
		);
		assert!(
			state
				.table_drop_fences
				.get(TenantId::new(7), descriptor.table_object_id)
				.is_none()
		);
	}

	#[test]
	fn table_drop_fence_rejects_concurrent_table_admin_mutations() {
		let descriptor = table_descriptor(1);
		let mut state = state_with_descriptor(&descriptor);
		let operation_id = DropOperationId {
			tenant_id: TenantId::new(7),
			table_object_id: descriptor.table_object_id,
			schema_generation: SchemaGeneration::new(descriptor.schema_generation),
		};
		assert_eq!(
			apply_table_drop_prepare(
				&mut state,
				TableDropPrepareRequest::current(
					operation_id,
					SchemaGeneration::new(descriptor.schema_generation),
				),
				10,
			),
			WendelonResponse::Success
		);

		let mut retuned = descriptor.clone();
		retuned.schema_generation = 2;
		retuned.record_body_compression_policy = RecordBodyCompressionPolicy::Lz4Frame;
		retuned.refresh_descriptor_fingerprints().unwrap();
		let err = apply_schema_update(
			&mut state,
			&schema_envelope(&retuned, 1, 2, SchemaMutationKind::Alter),
		)
		.unwrap_err();
		assert!(err.contains("drop is in progress"));
		assert_eq!(
			state
				.table_schemas
				.latest_by_id(TenantId::new(7), descriptor.table_object_id)
				.expect("descriptor")
				.schema_generation,
			1
		);

		let index = index_descriptor(&descriptor);
		let err = apply_schema_update(
			&mut state,
			&index_schema_envelope(
				&index,
				0,
				1,
				SchemaMutationKind::Create,
				encode_secondary_index_descriptor(&index).unwrap(),
			),
		)
		.unwrap_err();
		assert!(err.contains("drop is in progress"));
		assert!(
			state
				.secondary_indexes
				.get(TenantId::new(7), index.index_object_id)
				.is_none()
		);

		let update = SetTableRetentionPolicy {
			tenant_id: TenantId::new(7),
			table_object_id: descriptor.table_object_id,
			expected_policy_generation: PolicyGeneration::ZERO,
			new_policy: TableRetentionPolicy::InheritCluster,
			updated_at_hlc: HybridTimestamp {
				physical_ms: 42,
				logical: 1,
				node_id: 2,
			},
		};
		let response = apply_set_table_retention_policy(&mut state, update);
		assert!(
			matches!(response, WendelonResponse::Error(message) if message.contains("drop is in progress"))
		);
	}

	#[test]
	fn schema_update_rejects_unimplemented_object_kind() {
		let descriptor = table_descriptor(1);
		let bytes = schema_envelope_for_kind(
			&descriptor,
			0,
			1,
			SchemaMutationKind::Create,
			SchemaObjectKind::View,
		);
		let mut state = SmState::default();
		let err = apply_schema_update(&mut state, &bytes).unwrap_err();
		assert!(err.contains("schema object kind View is not implemented"));
		assert!(state.table_schemas.is_empty());
	}

	#[test]
	fn schema_update_creates_secondary_index_without_table_generation_bump() {
		let table = table_descriptor(1);
		let mut state = state_with_descriptor(&table);
		let descriptor = index_descriptor(&table);
		let bytes = index_schema_envelope(
			&descriptor,
			0,
			1,
			SchemaMutationKind::Create,
			encode_secondary_index_descriptor(&descriptor).unwrap(),
		);

		apply_schema_update(&mut state, &bytes).unwrap();
		let row = state
			.secondary_indexes
			.get(TenantId::new(7), descriptor.index_object_id)
			.unwrap();
		assert_eq!(row.state, SecondaryIndexState::Building);
		assert_eq!(
			state
				.table_schemas
				.latest_by_id(TenantId::new(7), table.table_object_id)
				.unwrap()
				.schema_generation,
			1
		);
	}

	#[test]
	fn secondary_index_lifecycle_mutations_are_linear_and_idempotent() {
		let table = table_descriptor(1);
		let mut state = state_with_descriptor(&table);
		state.last_membership = StoredMembership::new(
			None,
			openraft::Membership::new(vec![BTreeSet::from([1, 2])], ()),
		);
		let descriptor = index_descriptor(&table);
		apply_schema_update(
			&mut state,
			&index_schema_envelope(
				&descriptor,
				0,
				1,
				SchemaMutationKind::Create,
				encode_secondary_index_descriptor(&descriptor).unwrap(),
			),
		)
		.unwrap();
		let complete = SecondaryIndexLifecycleComplete {
			index_object_id: descriptor.index_object_id,
			replica_id: SchemaNodeId {
				id: 1,
				_reserved: 0,
			},
		};
		let complete_bytes = index_schema_envelope(
			&descriptor,
			1,
			2,
			SchemaMutationKind::IndexBackfillComplete,
			encode_secondary_index_lifecycle_complete(&complete).unwrap(),
		);
		apply_schema_update(&mut state, &complete_bytes).unwrap();
		apply_schema_update(&mut state, &complete_bytes).unwrap();
		assert_eq!(
			state
				.secondary_indexes
				.get(TenantId::new(7), descriptor.index_object_id)
				.unwrap()
				.state,
			SecondaryIndexState::Building
		);
		let complete_second = SecondaryIndexLifecycleComplete {
			index_object_id: descriptor.index_object_id,
			replica_id: SchemaNodeId {
				id: 2,
				_reserved: 0,
			},
		};
		let complete_second_bytes = index_schema_envelope(
			&descriptor,
			1,
			2,
			SchemaMutationKind::IndexBackfillComplete,
			encode_secondary_index_lifecycle_complete(&complete_second).unwrap(),
		);
		apply_schema_update(&mut state, &complete_second_bytes).unwrap();
		assert_eq!(
			state
				.secondary_indexes
				.get(TenantId::new(7), descriptor.index_object_id)
				.unwrap()
				.state,
			SecondaryIndexState::Ready
		);

		let drop = SecondaryIndexDropRequest {
			index_object_id: descriptor.index_object_id,
		};
		apply_schema_update(
			&mut state,
			&index_schema_envelope(
				&descriptor,
				2,
				3,
				SchemaMutationKind::Drop,
				encode_secondary_index_drop_request(&drop).unwrap(),
			),
		)
		.unwrap();
		assert_eq!(
			state
				.secondary_indexes
				.get(TenantId::new(7), descriptor.index_object_id)
				.unwrap()
				.state,
			SecondaryIndexState::Dropping
		);
		apply_schema_update(
			&mut state,
			&index_schema_envelope(
				&descriptor,
				3,
				4,
				SchemaMutationKind::IndexDropGcComplete,
				encode_secondary_index_lifecycle_complete(&complete).unwrap(),
			),
		)
		.unwrap();
		assert!(
			state
				.secondary_indexes
				.get(TenantId::new(7), descriptor.index_object_id)
				.is_some(),
			"drop GC completion from one voter cannot retire the global catalog row"
		);
		apply_schema_update(
			&mut state,
			&index_schema_envelope(
				&descriptor,
				3,
				4,
				SchemaMutationKind::IndexDropGcComplete,
				encode_secondary_index_lifecycle_complete(&complete_second).unwrap(),
			),
		)
		.unwrap();
		assert!(
			state
				.secondary_indexes
				.get(TenantId::new(7), descriptor.index_object_id)
				.is_none()
		);
	}

	#[test]
	fn secondary_index_alter_uses_schema_update_reject_namespace() {
		let table = table_descriptor(1);
		let mut state = state_with_descriptor(&table);
		let descriptor = index_descriptor(&table);
		let err = apply_schema_update(
			&mut state,
			&index_schema_envelope(
				&descriptor,
				0,
				1,
				SchemaMutationKind::Alter,
				encode_secondary_index_descriptor(&descriptor).unwrap(),
			),
		)
		.unwrap_err();
		assert_eq!(err, SCHEMA_UPDATE_REJECT_INDEX_ALTER_UNSUPPORTED);
	}
}

// ============================================================
//  RECOVERY TESTS
// ============================================================

#[cfg(test)]
mod recovery_tests {
	use super::*;
	use crate::integration::compute::{
		COMPUTE_STORAGE_CONTRACT_VERSION, DropOperationId, SchemaGeneration, SchemaIntentId,
		SchemaIntentOrigin, SchemaMutationKind, SchemaObjectKind, SchemaObjectRef,
		SchemaUpdateEnvelope, encode_schema_update_envelope_v1,
	};
	use crate::schema::{
		FinalizationMarker, LeaseDescriptor, LocalityPolicy, NodeId as SchemaNodeId,
		PrimaryKeyDescriptor, RowIdentityPolicy, SchemaEvolutionPolicy, TableObjectId,
		TableSchemaDescriptor, TenantId, encode_table_schema_descriptor_v1,
	};
	use crate::storage::{
		AlignedBuf, AsyncFileOps, BlockingFileOps, DirectoryEntry, FaultAction, FaultOp,
		FaultyFileOps, FileHandle, InjectedFault, OpenFlags,
	};
	use arrow::datatypes::{DataType, Field, Schema};
	use openraft::Vote;
	use openraft::storage::{RaftLogStorageExt, RaftSnapshotBuilder, RaftStateMachine};
	use std::collections::HashMap;
	use std::fs::OpenOptions;
	use std::io::Write;
	use std::path::PathBuf;
	use std::sync::{Arc, Mutex};
	use std::time::Duration;
	use tokio::sync::Notify;

	#[derive(Clone, Copy)]
	enum TestArtifactHashFormat {
		Blake3,
		Sha256,
	}

	impl TestArtifactHashFormat {
		fn digest(self, bytes: &[u8]) -> [u8; 32] {
			match self {
				Self::Blake3 => crate::hash::blake3::digest(bytes),
				Self::Sha256 => crate::hash::sha256::digest(bytes),
			}
		}

		fn checksum_u32(self, bytes: &[u8]) -> u32 {
			match self {
				Self::Blake3 => crate::hash::blake3::prefix_u32(bytes),
				Self::Sha256 => crate::hash::sha256::prefix_u32(bytes),
			}
		}
	}

	fn tempdir(tag: &str) -> PathBuf {
		let dir = std::env::temp_dir().join(format!(
			"wendelon-raft-recovery-{tag}-{}-{}",
			std::process::id(),
			uuid::Uuid::new_v4()
		));
		std::fs::create_dir_all(&dir).unwrap();
		dir
	}

	fn artifact_paths(dir: &Path) -> (PathBuf, PathBuf, PathBuf) {
		(
			dir.join("control.wal"),
			dir.join("vote.wnd"),
			dir.join("snapshot.wnd"),
		)
	}

	fn rewrite_snapshot_header(path: &Path, rewrite: impl FnOnce(&mut SnapshotHeader)) {
		let mut bytes = std::fs::read(path).unwrap();
		let header_len = std::mem::size_of::<SnapshotHeader>();
		let mut header = *bytemuck::from_bytes::<SnapshotHeader>(&bytes[..header_len]);
		rewrite(&mut header);
		bytes[..header_len].copy_from_slice(bytemuck::bytes_of(&header));
		std::fs::write(path, bytes).unwrap();
	}

	fn copy_flat_artifacts(source: &Path, target: &Path, omit_file_name: Option<&str>) {
		for entry in std::fs::read_dir(source).unwrap() {
			let entry = entry.unwrap();
			let file_type = entry.file_type().unwrap();
			assert!(file_type.is_file(), "unexpected nested test artifact");
			if entry.file_name().to_str() == omit_file_name {
				continue;
			}
			std::fs::copy(entry.path(), target.join(entry.file_name())).unwrap();
		}
	}

	fn log_id(term: u64, index: u64) -> LogId<u32> {
		log_id_for(1, term, index)
	}

	fn log_id_for(node_id: u32, term: u64, index: u64) -> LogId<u32> {
		LogId::new(CommittedLeaderId::new(term, node_id), index)
	}

	fn lease_entry(
		index: u64,
		range_start: u64,
		range_end: u64,
		holder: u32,
	) -> Entry<WendelonTypeConfig> {
		let lease = lease_descriptor(index, range_start, range_end, holder);
		Entry {
			log_id: log_id(1, index),
			payload: EntryPayload::Normal(WendelonRequest::AcquireLease(lease)),
		}
	}

	fn lease_descriptor(
		index: u64,
		range_start: u64,
		range_end: u64,
		holder: u32,
	) -> LeaseDescriptor {
		LeaseDescriptor {
			epoch: 1,
			holder: SchemaNodeId {
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
			issuer: SchemaNodeId {
				id: 1,
				_reserved: 0,
			},
			signature: [0; 64],
		}
	}

	fn finalize_entry(index: u64, physical_ms: u64) -> Entry<WendelonTypeConfig> {
		finalize_entry_for_epoch(index, 1, physical_ms)
	}

	fn recovery_table_schema() -> Schema {
		Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("region", DataType::Utf8, true),
		])
	}

	fn recovery_table_descriptor(generation: u64) -> TableSchemaDescriptor {
		TableSchemaDescriptor::new(
			TableObjectId::new([8; 16]),
			generation,
			"analytics",
			"events",
			&recovery_table_schema(),
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
			LocalityPolicy::default_table_spread(),
			SchemaEvolutionPolicy::AdditiveOnly,
		)
		.unwrap()
	}

	fn schema_update_entry(
		index: u64,
		descriptor: &TableSchemaDescriptor,
		expected_generation: u64,
		requested_generation: u64,
		mutation: SchemaMutationKind,
	) -> Entry<WendelonTypeConfig> {
		let payload = encode_table_schema_descriptor_v1(descriptor).unwrap();
		let envelope = SchemaUpdateEnvelope {
			contract_version: COMPUTE_STORAGE_CONTRACT_VERSION,
			origin: SchemaIntentOrigin::ComputeClient,
			tenant_id: TenantId::new(7),
			object: SchemaObjectRef {
				kind: SchemaObjectKind::Table,
				namespace: descriptor.namespace.clone(),
				name: descriptor.table_name.clone(),
			},
			expected_generation: SchemaGeneration::new(expected_generation),
			requested_generation: SchemaGeneration::new(requested_generation),
			intent_id: SchemaIntentId::new([index as u8; 16]),
			mutation,
			payload,
		};
		Entry {
			log_id: log_id(1, index),
			payload: EntryPayload::Normal(WendelonRequest::SchemaUpdate(
				encode_schema_update_envelope_v1(&envelope).unwrap(),
			)),
		}
	}

	fn finalize_entry_for_epoch(
		index: u64,
		epoch: u64,
		physical_ms: u64,
	) -> Entry<WendelonTypeConfig> {
		Entry {
			log_id: log_id(1, index),
			payload: EntryPayload::Normal(WendelonRequest::Finalize(
				crate::consensus::TenantFinalization {
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
						durability: crate::schema::DurabilityLevel::QUORUM,
						_pad: [0; 7],
					},
					tenant_id: TenantId::ZERO,
					key_identity: crate::schema::Identity::ZERO,
					charged_bytes: 0,
					record_count: 1,
				},
			)),
		}
	}

	fn write_vote_file(path: &Path, vote: Vote<u32>) {
		let encoded = postcard::to_allocvec(&vote).unwrap();
		let mut page = vec![0u8; DIRECT_IO_ALIGN];
		page[..4].copy_from_slice(&(encoded.len() as u32).to_le_bytes());
		page[4..4 + encoded.len()].copy_from_slice(&encoded);
		std::fs::write(path, page).unwrap();
	}

	fn write_snapshot_file(path: &Path, state: &SmState) {
		std::fs::write(path, encode_snapshot(state).unwrap()).unwrap();
	}

	fn write_snapshot_file_with_hash(path: &Path, state: &SmState, format: TestArtifactHashFormat) {
		let mut bytes = encode_snapshot(state).unwrap();
		let header_len = std::mem::size_of::<SnapshotHeader>();
		let mut header = *bytemuck::from_bytes::<SnapshotHeader>(&bytes[..header_len]);
		header.state_sha256 = format.digest(&bytes[header_len..]);
		bytes[..header_len].copy_from_slice(bytemuck::bytes_of(&header));
		std::fs::write(path, bytes).unwrap();
	}

	fn write_wal_pages(path: &Path, pages: &[AlignedBuf]) {
		let mut file = OpenOptions::new()
			.create(true)
			.write(true)
			.truncate(true)
			.open(path)
			.unwrap();
		for page in pages {
			file.write_all(page.as_slice()).unwrap();
		}
		file.flush().unwrap();
	}

	fn serialize_entry_page_with_hash(
		entry: &Entry<WendelonTypeConfig>,
		format: TestArtifactHashFormat,
	) -> Result<AlignedBuf, AnyError> {
		let mut page = serialize_entry_page(entry)?;
		let header_size = std::mem::size_of::<WALRecordHeader>();
		let fixed_size = std::mem::size_of::<RaftLogEntryFixed>();
		let header: WALRecordHeader =
			*bytemuck::from_bytes::<WALRecordHeader>(&page.as_slice()[..header_size]);
		let body_end = header_size + header.body_len as usize;
		let payload_start = header_size + fixed_size;
		let payload = &page.as_slice()[payload_start..body_end];
		let mut fixed = *bytemuck::from_bytes::<RaftLogEntryFixed>(
			&page.as_slice()[header_size..payload_start],
		);
		fixed.payload_hash = format.digest(payload);
		let dst = page.as_mut_slice();
		dst[header_size..payload_start].copy_from_slice(bytemuck::bytes_of(&fixed));
		let checksum = format.checksum_u32(&dst[..body_end]);
		dst[body_end..body_end + 4].copy_from_slice(&checksum.to_le_bytes());
		Ok(page)
	}

	fn serialize_checkpoint_page_with_hash(
		commit_index: u64,
		e_safe_index: u64,
		log_index: u64,
		term: u64,
		format: TestArtifactHashFormat,
	) -> AlignedBuf {
		let mut page = serialize_checkpoint_page(commit_index, e_safe_index, log_index, term);
		let header_size = std::mem::size_of::<WALRecordHeader>();
		let header: WALRecordHeader =
			*bytemuck::from_bytes::<WALRecordHeader>(&page.as_slice()[..header_size]);
		let body_end = header_size + header.body_len as usize;
		let checksum = format.checksum_u32(&page.as_slice()[..body_end]);
		page.as_mut_slice()[body_end..body_end + 4].copy_from_slice(&checksum.to_le_bytes());
		page
	}

	async fn open_store(dir: &Path) -> WendelonLogStore {
		let io = crate::storage::io_uring::spawn_io_thread(64).unwrap();
		let (wal, vote, snapshot) = artifact_paths(dir);
		WendelonLogStore::open(&wal, &vote, &snapshot, io)
			.await
			.unwrap()
	}

	async fn try_open_store_with_segment_bytes(
		dir: &Path,
		segment_bytes: u64,
	) -> std::io::Result<WendelonLogStore> {
		let (wal, vote, snapshot) = artifact_paths(dir);
		WendelonLogStore::open_with_file_ops_and_options(
			&wal,
			&vote,
			&snapshot,
			Arc::new(BlockingFileOps),
			WalOptions { segment_bytes },
		)
		.await
	}

	async fn open_store_with_segment_bytes(dir: &Path, segment_bytes: u64) -> WendelonLogStore {
		let io = crate::storage::io_uring::spawn_io_thread(64).unwrap();
		let (wal, vote, snapshot) = artifact_paths(dir);
		WendelonLogStore::open_with_file_ops_and_options(
			&wal,
			&vote,
			&snapshot,
			Arc::new(crate::storage::IoUringFileOps::new(io)),
			WalOptions { segment_bytes },
		)
		.await
		.unwrap()
	}

	async fn open_store_with_faults(
		dir: &Path,
		file_ops: Arc<FaultyFileOps>,
		segment_bytes: u64,
	) -> WendelonLogStore {
		open_store_with_async_file_ops(dir, file_ops, segment_bytes).await
	}

	async fn open_store_with_async_file_ops(
		dir: &Path,
		file_ops: Arc<dyn AsyncFileOps>,
		segment_bytes: u64,
	) -> WendelonLogStore {
		let (wal, vote, snapshot) = artifact_paths(dir);
		WendelonLogStore::open_with_file_ops_and_options(
			&wal,
			&vote,
			&snapshot,
			file_ops,
			WalOptions { segment_bytes },
		)
		.await
		.unwrap()
	}

	#[derive(Clone)]
	struct RecordingFileOps {
		inner: BlockingFileOps,
		events: Arc<Mutex<Vec<&'static str>>>,
		fdatasync_started: Arc<Notify>,
		release_fdatasync: Arc<Notify>,
	}

	impl RecordingFileOps {
		fn new() -> Self {
			Self {
				inner: BlockingFileOps,
				events: Arc::new(Mutex::new(Vec::new())),
				fdatasync_started: Arc::new(Notify::new()),
				release_fdatasync: Arc::new(Notify::new()),
			}
		}

		fn record(&self, event: &'static str) {
			self.events.lock().unwrap().push(event);
		}
	}

	#[async_trait::async_trait]
	impl AsyncFileOps for RecordingFileOps {
		async fn open(&self, path: &Path, flags: OpenFlags) -> std::io::Result<FileHandle> {
			self.inner.open(path, flags).await
		}

		async fn read_plain_at(
			&self,
			handle: &FileHandle,
			offset: u64,
			len: usize,
		) -> std::io::Result<Vec<u8>> {
			self.inner.read_plain_at(handle, offset, len).await
		}

		async fn write_plain_at(
			&self,
			handle: &FileHandle,
			offset: u64,
			bytes: Vec<u8>,
		) -> std::io::Result<usize> {
			self.inner.write_plain_at(handle, offset, bytes).await
		}

		async fn read_at(
			&self,
			handle: &FileHandle,
			offset: u64,
			buf: AlignedBuf,
		) -> std::io::Result<(usize, AlignedBuf)> {
			self.inner.read_at(handle, offset, buf).await
		}

		async fn write_at(
			&self,
			handle: &FileHandle,
			offset: u64,
			buf: AlignedBuf,
		) -> std::io::Result<(usize, AlignedBuf)> {
			let result = self.inner.write_at(handle, offset, buf).await;
			if result.is_ok() {
				self.record("write_complete");
			}
			result
		}

		async fn fsync(&self, handle: &FileHandle) -> std::io::Result<()> {
			self.inner.fsync(handle).await
		}

		async fn fdatasync(&self, handle: &FileHandle) -> std::io::Result<()> {
			self.record("fdatasync_start");
			self.fdatasync_started.notify_waiters();
			self.release_fdatasync.notified().await;
			let result = self.inner.fdatasync(handle).await;
			if result.is_ok() {
				self.record("fdatasync_end");
			}
			result
		}

		async fn read_all(&self, path: &Path) -> std::io::Result<Vec<u8>> {
			self.inner.read_all(path).await
		}

		async fn write_all(&self, path: &Path, bytes: &[u8]) -> std::io::Result<()> {
			self.inner.write_all(path, bytes).await
		}

		async fn file_len(&self, path: &Path) -> std::io::Result<Option<u64>> {
			self.inner.file_len(path).await
		}

		async fn set_len(&self, handle: &FileHandle, len: u64) -> std::io::Result<()> {
			self.inner.set_len(handle, len).await
		}

		async fn create_dir_all(&self, path: &Path) -> std::io::Result<()> {
			self.inner.create_dir_all(path).await
		}

		async fn read_dir(&self, path: &Path) -> std::io::Result<Vec<DirectoryEntry>> {
			self.inner.read_dir(path).await
		}

		async fn fsync_dir(&self, path: &Path) -> std::io::Result<()> {
			self.inner.fsync_dir(path).await
		}

		async fn rename(&self, from: &Path, to: &Path) -> std::io::Result<()> {
			self.inner.rename(from, to).await
		}

		async fn unlink(&self, path: &Path) -> std::io::Result<()> {
			self.inner.unlink(path).await
		}
	}

	async fn append_and_apply(
		store: &mut WendelonLogStore,
		sm: &mut WendelonStateMachine,
		entry: Entry<WendelonTypeConfig>,
	) {
		store
			.blocking_append(vec![clone_entry(&entry)])
			.await
			.unwrap();
		<WendelonStateMachine as RaftStateMachine<WendelonTypeConfig>>::apply(sm, vec![entry])
			.await
			.unwrap();
	}

	fn create_tenant_for_rotation(
		tenant_id: TenantId,
		rotation: &InstallTenantKekVersion,
	) -> CreateTenant {
		let mut tenant_keyset = crate::crypto::TenantKeyset::default();
		tenant_keyset.active_tenant_kek_id = Some(rotation.tenant_kek_id);
		tenant_keyset.keys.insert(
			rotation.tenant_kek_id,
			crate::crypto::WrappedTenantKek {
				wrapped_key: rotation.wrapped_tenant_kek.clone(),
			},
		);
		CreateTenant {
			tenant_id,
			quota_bytes: u64::MAX,
			replication_factor: 1,
			tenant_keyset,
		}
	}

	fn named_segment_paths(dir: &Path) -> Vec<PathBuf> {
		let mut out = std::fs::read_dir(dir)
			.unwrap()
			.filter_map(|entry| {
				let entry = entry.ok()?;
				let path = entry.path();
				if path
					.file_name()
					.and_then(|name| name.to_str())
					.map(|name| name.starts_with("control.wal.") && name.ends_with(".seg"))
					.unwrap_or(false)
				{
					Some(path)
				} else {
					None
				}
			})
			.collect::<Vec<_>>();
		out.sort();
		out
	}

	#[tokio::test]
	async fn normal_restart_recovers_vote_wal_and_state_machine() {
		let dir = tempdir("normal");
		let (wal, vote, _snapshot) = artifact_paths(&dir);
		write_vote_file(&vote, Vote::new_committed(3, 7));
		let entry = lease_entry(1, 0, 99, 7);
		write_wal_pages(
			&wal,
			&[
				serialize_entry_page(&entry).unwrap(),
				serialize_checkpoint_page(1, 0, 1, 1),
			],
		);

		let mut store = open_store(&dir).await;
		assert_eq!(store.bootstrap_mode().await, DurableBootstrapMode::Recovery);
		assert_eq!(
			store.read_vote().await.unwrap(),
			Some(Vote::new_committed(3, 7))
		);
		assert_eq!(store.read_committed().await.unwrap(), Some(log_id(1, 1)));

		let state = store.get_log_state().await.unwrap();
		assert_eq!(state.last_purged_log_id, None);
		assert_eq!(state.last_log_id, Some(log_id(1, 1)));

		let sm = store.recovered_state_machine().await.unwrap();
		let snapshot = sm.state_snapshot().await;
		assert_eq!(snapshot.last_applied_log_id, Some(log_id(1, 1)));
		assert!(snapshot.leases.contains_key(&0));
	}

	/// Restamp an encoded page's declared format version and repair its
	/// checksum, so the record stays byte-valid and only its declared shape
	/// changes — the condition the gate has to judge.
	fn wal_page_with_format_version(mut page: AlignedBuf, version: u8) -> AlignedBuf {
		let header_size = std::mem::size_of::<WALRecordHeader>();
		let mut header: WALRecordHeader = *bytemuck::from_bytes(&page.as_slice()[..header_size]);
		header.format_version = version;
		let body_end = header_size + header.body_len as usize;
		let dst = page.as_mut_slice();
		dst[..header_size].copy_from_slice(bytemuck::bytes_of(&header));
		let checksum = control_artifact_checksum_u32(&dst[..body_end]);
		dst[body_end..body_end + 4].copy_from_slice(&checksum.to_le_bytes());
		page
	}

	#[tokio::test]
	async fn adr0085_wal_record_from_a_newer_build_is_refused_by_name() {
		let dir = tempdir("wal-newer-format");
		let (wal, vote, snapshot) = artifact_paths(&dir);
		let page = wal_page_with_format_version(
			serialize_entry_page(&lease_entry(1, 0, 9, 1)).unwrap(),
			9,
		);
		write_wal_pages(&wal, &[page]);

		let io = crate::storage::io_uring::spawn_io_thread(64).unwrap();
		let error = match WendelonLogStore::open(&wal, &vote, &snapshot, io).await {
			Ok(_) => panic!("a record version this build does not know must be refused"),
			Err(error) => error.to_string(),
		};
		assert!(
			error.contains('9'),
			"the refusal names the version: {error}"
		);
		assert!(
			error.contains("not corrupt"),
			"an intact segment is not reported as damage: {error}"
		);
	}

	#[tokio::test]
	async fn adr0085_pre_versioning_wal_record_still_replays() {
		// The guarantee that keeps existing control rings starting: a record
		// whose version byte is zero is read exactly as before.
		let dir = tempdir("wal-legacy-format");
		let (wal, vote, _snapshot) = artifact_paths(&dir);
		write_vote_file(&vote, Vote::new_committed(3, 7));
		// Entry plus checkpoint, both stamped version zero: a checkpoint is what
		// lets replay reach the state machine, so this exercises the record all
		// the way through rather than stopping at a successful open.
		write_wal_pages(
			&wal,
			&[
				wal_page_with_format_version(
					serialize_entry_page(&lease_entry(1, 0, 99, 7)).unwrap(),
					PRE_VERSIONING_FORMAT,
				),
				wal_page_with_format_version(
					serialize_checkpoint_page(1, 0, 1, 1),
					PRE_VERSIONING_FORMAT,
				),
			],
		);

		let store = open_store(&dir).await;
		let recovered = store.recovered_state_machine().await.unwrap();
		let snapshot = recovered.state_snapshot().await;
		assert_eq!(snapshot.last_applied_log_id, Some(log_id(1, 1)));
		assert!(
			snapshot.leases.contains_key(&0),
			"a pre-versioning record is recognized and replayed, not refused"
		);
	}

	/// ADR 0092: the grounding probe, flipped to the contract. Immutable
	/// stored v1 bytes — never a current-encoder truncation — drive a real
	/// recovery, and the recovered state must be byte-equal to what the
	/// current certificate-less encoding of the same mutation produces. The
	/// pre-fix behavior this test replaced: the same bytes refused bootstrap
	/// as "interior corruption ... Hit the end of buffer".
	#[tokio::test]
	async fn adr0092_v1_ctas_provision_wal_record_replay_behavior() {
		let stored = manifest_bytes(
			include_str!("../../tests/fixtures/consensus_body/wal_v1_entries.txt"),
			"ctas_saga_provision",
		);

		// Arm one: the stored v1 payload, framed at format version 1.
		let dir = tempdir("wal-v1-provision");
		let (wal, vote, _snapshot) = artifact_paths(&dir);
		write_vote_file(&vote, Vote::new_committed(3, 7));
		write_wal_pages(
			&wal,
			&[
				wal_page_for_payload(&stored, 1),
				serialize_checkpoint_page(11, 0, 11, 3),
			],
		);
		let store = open_store(&dir).await;
		let recovered = store.recovered_state_machine().await.unwrap();
		let v1_state = recovered.state_snapshot().await;

		// Arm two: the same mutation written by this build with
		// `certificate: None`, replayed the ordinary way.
		let dir = tempdir("wal-v2-provision-none");
		let (wal, vote, _snapshot) = artifact_paths(&dir);
		write_vote_file(&vote, Vote::new_committed(3, 7));
		write_wal_pages(
			&wal,
			&[
				serialize_entry_page(&provision_none_entry()).unwrap(),
				serialize_checkpoint_page(11, 0, 11, 3),
			],
		);
		let store = open_store(&dir).await;
		let recovered = store.recovered_state_machine().await.unwrap();
		let v2_state = recovered.state_snapshot().await;

		let saga = v1_state
			.ctas_sagas
			.get(TenantId::new(7), provision_operation_id())
			.expect("the v1 provision must replay into the catalog");
		assert_eq!(
			saga.certificate, None,
			"upconversion adds certificate: None"
		);
		assert_eq!(
			postcard::to_stdvec(&v1_state).unwrap(),
			postcard::to_stdvec(&v2_state).unwrap(),
			"a v1 body and its current certificate-less twin must recover \
			 byte-identical states"
		);
	}

	/// ADR 0092: a profile holding a v1 snapshot AND a v1 WAL tail boots to
	/// the state a full replay produces — the steady upgrade state, where
	/// both artifacts predate the running build.
	#[tokio::test]
	async fn adr0092_v1_snapshot_and_v1_wal_tail_boot_to_full_replay_state() {
		let snapshot_body = manifest_bytes(
			include_str!("../../tests/fixtures/consensus_body/snapshot_v1.txt"),
			"smstate_active_saga",
		);
		let wal_payload = manifest_bytes(
			include_str!("../../tests/fixtures/consensus_body/wal_v1_entries.txt"),
			"ctas_saga_provision_tail_table",
		);

		let dir = tempdir("v1-snapshot-and-tail");
		let (wal, vote, snapshot) = artifact_paths(&dir);
		write_vote_file(&vote, Vote::new_committed(3, 7));
		let mut snapshot_bytes =
			Vec::with_capacity(std::mem::size_of::<SnapshotHeader>() + snapshot_body.len());
		let header = SnapshotHeader {
			last_included_index: 0,
			last_included_term: 0,
			last_included_epoch: 0,
			snapshot_hlc: HybridTimestamp {
				physical_ms: 0,
				logical: 0,
				node_id: 0,
			},
			state_sha256: control_artifact_digest_32(&snapshot_body),
			cache_checkpoint_len: 0,
			format_version: 1,
			_pad: [0; 3],
		};
		snapshot_bytes.extend_from_slice(bytemuck::bytes_of(&header));
		snapshot_bytes.extend_from_slice(&snapshot_body);
		std::fs::write(&snapshot, snapshot_bytes).unwrap();
		write_wal_pages(
			&wal,
			&[
				wal_page_for_payload(&wal_payload, 1),
				serialize_checkpoint_page(11, 0, 11, 3),
			],
		);

		let store = open_store(&dir).await;
		let recovered = store.recovered_state_machine().await.unwrap();
		let state = recovered.state_snapshot().await;

		// The snapshot's active saga (operation 0x55) and the tail's
		// provision (operation 0x54) must both be present, certificate-less.
		let mut op55 = [0x55u8; 16];
		op55[..5].fill(0);
		op55[5] = 1;
		op55[6] = 0x70;
		op55[8] = 0x80;
		let snapshot_saga = state
			.ctas_sagas
			.get(TenantId::new(7), CtasOperationId::new(op55))
			.expect("the snapshot's active saga survives the boot");
		assert_eq!(snapshot_saga.certificate, None);
		let tail_saga = state
			.ctas_sagas
			.get(TenantId::new(7), provision_operation_id())
			.expect("the tail's provision applies on top of the snapshot");
		assert_eq!(tail_saga.certificate, None);
	}

	fn manifest_bytes(manifest: &str, name: &str) -> Vec<u8> {
		let line = manifest
			.lines()
			.find(|line| line.starts_with(&format!("{name}=")))
			.unwrap_or_else(|| panic!("fixture manifest lacks `{name}`"));
		let hex = &line[name.len() + 1..];
		(0..hex.len())
			.step_by(2)
			.map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
			.collect()
	}

	fn provision_operation_id() -> CtasOperationId {
		let mut bytes = [0x54u8; 16];
		bytes[..5].fill(0);
		bytes[5] = 1;
		bytes[6] = 0x70;
		bytes[8] = 0x80;
		CtasOperationId::new(bytes)
	}

	fn provision_none_entry() -> Entry<WendelonTypeConfig> {
		let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
		let descriptor = TableSchemaDescriptor::new(
			TableObjectId::new([4; 16]),
			1,
			"analytics",
			"events",
			&schema,
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
			LocalityPolicy::default_table_spread(),
			SchemaEvolutionPolicy::AdditiveOnly,
		)
		.unwrap();
		let mut client_uuid = [0x54u8; 16];
		client_uuid[6] = 0x70;
		client_uuid[8] = 0x80;
		Entry {
			log_id: log_id(3, 11),
			payload: EntryPayload::Normal(WendelonRequest::CtasSaga(CtasSagaMutation::Provision {
				tenant_id: TenantId::new(7),
				operation_id: provision_operation_id(),
				intent_fingerprint: CtasIntentFingerprint::new([0x48; 32]),
				descriptor,
				expected_binding: CtasExpectedBinding::Absent,
				prior_descriptor: None,
				row_write_id: ClientWriteId {
					client_uuid,
					sequence_no: 7,
					idempotency_ttl_ms: 300_000,
					_pad: 0,
				},
				owner_epoch: 1,
				observed_now_ms: 1,
				owner_lease_expires_at_ms: 1_000,
				certificate: None,
			})),
		}
	}

	/// Frame an already-encoded entry payload at a declared format version.
	/// The framing (header, fixed prefix, hashes, checksum) is the current
	/// writer's — only the BODY bytes are the fixture's — because framing is
	/// not what the body format versions.
	fn wal_page_for_payload(payload: &[u8], format_version: u8) -> AlignedBuf {
		let (log_index, term) = (11u64, 3u64);
		let header_size = std::mem::size_of::<WALRecordHeader>();
		let fixed_size = std::mem::size_of::<RaftLogEntryFixed>();
		let body_len = fixed_size + payload.len();
		let mut page = AlignedBuf::new(page_round_up(header_size + body_len + 4));
		let header = WALRecordHeader {
			record_type: REC_LOG_ENTRY,
			format_version,
			_pad1: [0; 6],
			body_len: body_len as u64,
			log_index,
			term,
		};
		let fixed = RaftLogEntryFixed {
			kind: LogEntryKind::APPEND,
			_pad1: [0; 7],
			term,
			index: log_index,
			epoch: 0,
			leader: SchemaNodeId {
				id: 1,
				_reserved: 0,
			},
			entry_hlc: HybridTimestamp {
				physical_ms: 0,
				logical: 0,
				node_id: 1,
			},
			payload_len: payload.len() as u32,
			_pad2: [0; 4],
			payload_hash: control_artifact_digest_32(payload),
		};
		let dst = page.as_mut_slice();
		dst[..header_size].copy_from_slice(bytemuck::bytes_of(&header));
		dst[header_size..header_size + fixed_size].copy_from_slice(bytemuck::bytes_of(&fixed));
		dst[header_size + fixed_size..header_size + body_len].copy_from_slice(payload);
		let checksum = control_artifact_checksum_u32(&dst[..header_size + body_len]);
		dst[header_size + body_len..header_size + body_len + 4]
			.copy_from_slice(&checksum.to_le_bytes());
		page
	}

	#[tokio::test]
	async fn restart_without_checkpoint_defers_tail_state_machine_replay() {
		let dir = tempdir("no-checkpoint");
		let (wal, vote, _snapshot) = artifact_paths(&dir);
		write_vote_file(&vote, Vote::new_committed(3, 7));
		write_wal_pages(
			&wal,
			&[serialize_entry_page(&lease_entry(1, 0, 99, 7)).unwrap()],
		);

		let mut store = open_store(&dir).await;
		assert_eq!(store.read_committed().await.unwrap(), None);

		let sm = store.recovered_state_machine().await.unwrap();
		let snapshot = sm.state_snapshot().await;
		assert_eq!(snapshot.last_applied_log_id, None);
		assert!(!snapshot.leases.contains_key(&0));
	}

	#[tokio::test]
	async fn save_committed_persists_replay_boundary_for_restart() {
		let dir = tempdir("save-committed");
		let mut store = open_store(&dir).await;
		store
			.blocking_append(vec![lease_entry(1, 0, 9, 1), lease_entry(2, 10, 19, 2)])
			.await
			.unwrap();
		store.save_committed(Some(log_id(1, 2))).await.unwrap();

		let mut reopened = open_store(&dir).await;
		assert_eq!(reopened.read_committed().await.unwrap(), Some(log_id(1, 2)));
		let recovered = reopened.recovered_state_machine().await.unwrap();
		let snapshot = recovered.state_snapshot().await;
		assert_eq!(snapshot.last_applied_log_id, Some(log_id(1, 2)));
		assert!(snapshot.leases.contains_key(&0));
		assert!(snapshot.leases.contains_key(&10));
	}

	#[tokio::test]
	async fn torn_tail_at_eof_is_truncated_to_last_good_record() {
		let dir = tempdir("torn-tail");
		let (wal, _vote, _snapshot) = artifact_paths(&dir);
		let first = serialize_entry_page(&lease_entry(1, 0, 9, 1)).unwrap();
		let second = serialize_entry_page(&lease_entry(2, 10, 19, 2)).unwrap();

		let mut file = OpenOptions::new()
			.create(true)
			.write(true)
			.truncate(true)
			.open(&wal)
			.unwrap();
		file.write_all(first.as_slice()).unwrap();
		file.write_all(&second.as_slice()[..128]).unwrap();
		file.flush().unwrap();

		let mut store = open_store(&dir).await;
		let state = store.get_log_state().await.unwrap();
		assert_eq!(state.last_log_id, Some(log_id(1, 1)));
		assert_eq!(std::fs::metadata(&wal).unwrap().len(), first.len() as u64);
		assert_eq!(store.read_committed().await.unwrap(), None);

		let sm = store.recovered_state_machine().await.unwrap();
		let snapshot = sm.state_snapshot().await;
		assert_eq!(snapshot.last_applied_log_id, None);
		assert_eq!(snapshot.leases.len(), 0);
	}

	#[tokio::test]
	async fn interior_corruption_fails_startup_loudly() {
		let dir = tempdir("interior-corruption");
		let (wal, _vote, _snapshot) = artifact_paths(&dir);
		let mut first = serialize_entry_page(&lease_entry(1, 0, 9, 1)).unwrap();
		let second = serialize_entry_page(&lease_entry(2, 10, 19, 2)).unwrap();
		first.as_mut_slice()[64] ^= 0x5a;
		write_wal_pages(&wal, &[first, second]);

		let io = crate::storage::io_uring::spawn_io_thread(64).unwrap();
		let err =
			match WendelonLogStore::open(&wal, dir.join("vote.wnd"), dir.join("snapshot.wnd"), io)
				.await
			{
				Ok(_) => panic!("interior corruption must fail startup"),
				Err(err) => err,
			};
		assert_eq!(err.kind(), ErrorKind::InvalidData);
		assert!(
			err.to_string().contains("interior corruption"),
			"unexpected error: {err}"
		);
		assert!(
			err.to_string().contains("checksum mismatch"),
			"expected Control Ring checksum verification failure, got: {err}"
		);
	}

	#[tokio::test]
	async fn restart_recovers_blake3_and_sha256_wal_artifacts() {
		for (tag, format) in [
			("dual-read-wal-blake3", TestArtifactHashFormat::Blake3),
			("dual-read-wal-sha256", TestArtifactHashFormat::Sha256),
		] {
			let dir = tempdir(tag);
			let (wal, vote, _snapshot) = artifact_paths(&dir);
			write_vote_file(&vote, Vote::new_committed(3, 7));
			write_wal_pages(
				&wal,
				&[
					serialize_entry_page_with_hash(&lease_entry(1, 0, 9, 1), format).unwrap(),
					serialize_entry_page_with_hash(&lease_entry(2, 10, 19, 2), format).unwrap(),
					serialize_checkpoint_page_with_hash(2, 2, 2, 1, format),
				],
			);

			let mut store = open_store(&dir).await;
			let state = store.get_log_state().await.unwrap();
			assert_eq!(state.last_log_id, Some(log_id(1, 2)));
			let sm = store.recovered_state_machine().await.unwrap();
			let snapshot = sm.state_snapshot().await;
			assert_eq!(snapshot.last_applied_log_id, Some(log_id(1, 2)));
			assert!(snapshot.leases.contains_key(&0));
			assert!(snapshot.leases.contains_key(&10));
		}
	}

	#[tokio::test]
	async fn restart_recovers_complete_nonzero_identity_from_blake3_and_sha256_snapshots() {
		for (tag, format) in [
			("dual-read-snapshot-blake3", TestArtifactHashFormat::Blake3),
			("dual-read-snapshot-sha256", TestArtifactHashFormat::Sha256),
		] {
			let dir = tempdir(tag);
			let (_wal, _vote, snapshot_path) = artifact_paths(&dir);
			let mut state = SmState::default();
			let incident_log_id = log_id_for(1, 1, 134_999);
			state.last_applied_log_id = Some(incident_log_id);
			state.leases.insert(0, lease_descriptor(5, 0, 9, 1));
			write_snapshot_file_with_hash(&snapshot_path, &state, format);

			let store = open_store(&dir).await;
			{
				let guard = store.inner.read().await;
				let loaded = guard
					.recovered_snapshot
					.as_ref()
					.expect("snapshot must be recovered");
				assert_eq!(loaded.last_included, Some(incident_log_id));
				assert_eq!(loaded.held.meta.last_log_id, Some(incident_log_id));
			}
			let recovered = store.recovered_state_machine().await.unwrap();
			let snapshot = recovered.state_snapshot().await;
			assert_eq!(snapshot.last_applied_log_id, Some(incident_log_id));
			assert!(snapshot.leases.contains_key(&0));
		}
	}

	#[tokio::test]
	async fn snapshot_zero_coordinates_preserve_body_presence() {
		let none_dir = tempdir("snapshot-zero-none");
		let (_, _, none_snapshot) = artifact_paths(&none_dir);
		write_snapshot_file(&none_snapshot, &SmState::default());
		let none_store = open_store(&none_dir).await;
		let none_state = none_store
			.recovered_state_machine()
			.await
			.unwrap()
			.state_snapshot()
			.await;
		assert_eq!(none_state.last_applied_log_id, None);

		let some_dir = tempdir("snapshot-zero-some");
		let (_, _, some_snapshot) = artifact_paths(&some_dir);
		let mut state = SmState::default();
		state.last_applied_log_id = Some(log_id_for(0, 0, 0));
		write_snapshot_file(&some_snapshot, &state);
		let some_store = open_store(&some_dir).await;
		{
			let guard = some_store.inner.read().await;
			let loaded = guard
				.recovered_snapshot
				.as_ref()
				.expect("zero-coordinate snapshot must remain present");
			assert_eq!(loaded.last_included, Some(log_id_for(0, 0, 0)));
			assert_eq!(loaded.held.meta.last_log_id, Some(log_id_for(0, 0, 0)));
		}
		let some_state = some_store
			.recovered_state_machine()
			.await
			.unwrap()
			.state_snapshot()
			.await;
		assert_eq!(some_state.last_applied_log_id, Some(log_id_for(0, 0, 0)));
	}

	#[tokio::test]
	async fn snapshot_header_coordinates_and_body_digest_remain_independent_guards() {
		for (tag, corrupt) in [
			("snapshot-header-term", "term"),
			("snapshot-header-index", "index"),
			("snapshot-body-digest", "body"),
		] {
			let dir = tempdir(tag);
			let (_, _, snapshot_path) = artifact_paths(&dir);
			let mut state = SmState::default();
			state.last_applied_log_id = Some(log_id_for(9, 3, 17));
			write_snapshot_file(&snapshot_path, &state);

			match corrupt {
				"term" => rewrite_snapshot_header(&snapshot_path, |header| {
					header.last_included_term += 1;
				}),
				"index" => rewrite_snapshot_header(&snapshot_path, |header| {
					header.last_included_index += 1;
				}),
				"body" => {
					let mut bytes = std::fs::read(&snapshot_path).unwrap();
					let last = bytes.last_mut().expect("snapshot body byte");
					*last ^= 0x5a;
					std::fs::write(&snapshot_path, bytes).unwrap();
				}
				_ => unreachable!(),
			}

			let err = match try_open_store_with_segment_bytes(&dir, DEFAULT_WAL_SEGMENT_BYTES).await
			{
				Ok(_) => panic!("corrupt snapshot must fail recovery"),
				Err(err) => err,
			};
			assert_eq!(err.kind(), ErrorKind::InvalidData);
			if corrupt == "body" {
				assert!(err.to_string().contains("snapshot body hash mismatch"));
			} else {
				assert!(err.to_string().contains("header/body identity mismatch"));
			}
		}
	}

	#[tokio::test]
	async fn aged_restart_requires_snapshot_and_preserves_purged_state() {
		let dir = tempdir("aged-snapshot-restart");
		let mut store = open_store_with_segment_bytes(&dir, DIRECT_IO_ALIGN as u64).await;
		let mut sm = store.recovered_state_machine().await.unwrap();
		for index in 1..=5 {
			append_and_apply(
				&mut store,
				&mut sm,
				lease_entry(index, (index - 1) * 10, index * 10 - 1, index as u32),
			)
			.await;
		}
		let mut builder =
			<WendelonStateMachine as RaftStateMachine<WendelonTypeConfig>>::get_snapshot_builder(
				&mut sm,
			)
			.await;
		let built = builder.build_snapshot().await.unwrap();
		assert_eq!(built.meta.last_log_id, Some(log_id(1, 5)));
		drop(built);

		append_and_apply(&mut store, &mut sm, lease_entry(6, 50, 59, 6)).await;
		store.save_committed(Some(log_id(1, 6))).await.unwrap();
		store.purge(log_id(1, 5)).await.unwrap();
		drop(sm);
		drop(store);

		let missing_snapshot = tempdir("aged-snapshot-withheld");
		copy_flat_artifacts(&dir, &missing_snapshot, Some("snapshot.wnd"));
		let err = match try_open_store_with_segment_bytes(&missing_snapshot, DIRECT_IO_ALIGN as u64)
			.await
		{
			Ok(_) => panic!("purged WAL tail without its snapshot must fail closed"),
			Err(err) => err,
		};
		assert_eq!(err.kind(), ErrorKind::InvalidData);
		assert!(
			err.to_string()
				.contains("no snapshot provides the purged prefix")
		);

		let mut reopened = open_store_with_segment_bytes(&dir, DIRECT_IO_ALIGN as u64).await;
		let log_state = reopened.get_log_state().await.unwrap();
		assert_eq!(log_state.last_purged_log_id, Some(log_id(1, 5)));
		assert_eq!(log_state.last_log_id, Some(log_id(1, 6)));
		let recovered = reopened.recovered_state_machine().await.unwrap();
		let state = recovered.state_snapshot().await;
		assert_eq!(state.last_applied_log_id, Some(log_id(1, 6)));
		for range_start in [0, 10, 20, 30, 40, 50] {
			assert!(
				state.leases.contains_key(&range_start),
				"missing lease at range {range_start} after aged restart"
			);
		}
	}

	#[tokio::test]
	async fn checkpoint_restart_clamps_e_safe_and_replays_only_committed_prefix() {
		let dir = tempdir("checkpoint");
		let (wal, _vote, _snapshot) = artifact_paths(&dir);
		let first = serialize_entry_page(&lease_entry(1, 0, 9, 1)).unwrap();
		let second = serialize_entry_page(&lease_entry(2, 10, 19, 2)).unwrap();
		let third = serialize_entry_page(&lease_entry(3, 20, 29, 3)).unwrap();
		let checkpoint = serialize_checkpoint_page(2, 9, 3, 1);
		write_wal_pages(&wal, &[first, second, third, checkpoint]);

		let mut store = open_store(&dir).await;
		assert_eq!(
			store.recovery_checkpoint().await,
			Some(RecoveryCheckpoint {
				commit_index: 2,
				e_safe_index: 2,
			})
		);
		let state = store.get_log_state().await.unwrap();
		assert_eq!(state.last_log_id, Some(log_id(1, 3)));

		let sm = store.recovered_state_machine().await.unwrap();
		let snapshot = sm.state_snapshot().await;
		assert_eq!(snapshot.last_applied_log_id, Some(log_id(1, 2)));
		assert!(snapshot.leases.contains_key(&0));
		assert!(snapshot.leases.contains_key(&10));
		assert!(!snapshot.leases.contains_key(&20));
	}

	#[tokio::test]
	async fn snapshot_plus_tail_replay_rebuilds_purged_boundary_and_tail_state() {
		let dir = tempdir("snapshot-tail");
		let (wal, _vote, snapshot_path) = artifact_paths(&dir);
		let mut state = SmState::default();
		state.last_applied_log_id = Some(log_id(1, 5));
		state.leases.insert(0, lease_descriptor(5, 0, 9, 1));
		write_snapshot_file(&snapshot_path, &state);

		let older = serialize_entry_page(&lease_entry(4, 100, 109, 9)).unwrap();
		let tail_lease = serialize_entry_page(&lease_entry(6, 10, 19, 2)).unwrap();
		let tail_finalize = serialize_entry_page(&finalize_entry(7, 777)).unwrap();
		let checkpoint = serialize_checkpoint_page(7, 7, 7, 1);
		write_wal_pages(&wal, &[older, tail_lease, tail_finalize, checkpoint]);

		let mut store = open_store(&dir).await;
		let state = store.get_log_state().await.unwrap();
		assert_eq!(state.last_purged_log_id, Some(log_id(1, 5)));
		assert_eq!(state.last_log_id, Some(log_id(1, 7)));

		let entries = store.try_get_log_entries(0..=u64::MAX).await.unwrap();
		let indexes: Vec<u64> = entries.iter().map(|entry| entry.log_id.index).collect();
		assert_eq!(indexes, vec![6, 7]);

		let sm = store.recovered_state_machine().await.unwrap();
		let snapshot = sm.state_snapshot().await;
		assert_eq!(snapshot.last_applied_log_id, Some(log_id(1, 7)));
		assert!(snapshot.leases.contains_key(&0));
		assert!(snapshot.leases.contains_key(&10));
		assert_eq!(
			snapshot.e_safe_watermarks.get(&1).copied(),
			Some(HybridTimestamp {
				physical_ms: 777,
				logical: 0,
				node_id: 1,
			})
		);
	}

	#[tokio::test]
	async fn snapshot_supersedes_an_older_checkpoint_in_a_retained_segment() {
		let dir = tempdir("snapshot-supersedes-checkpoint");
		let (wal, _vote, snapshot_path) = artifact_paths(&dir);
		let mut state = SmState::default();
		state.last_applied_log_id = Some(log_id(1, 5));
		state.leases.insert(0, lease_descriptor(5, 0, 9, 1));
		write_snapshot_file(&snapshot_path, &state);

		let segment = wal_segment_path(&wal, 4).unwrap();
		write_wal_pages(
			&segment,
			&[
				serialize_entry_page(&lease_entry(4, 100, 109, 4)).unwrap(),
				serialize_checkpoint_page(4, 4, 4, 1),
				serialize_entry_page(&lease_entry(6, 10, 19, 6)).unwrap(),
			],
		);

		let mut store = open_store(&dir).await;
		let log_state = store.get_log_state().await.unwrap();
		assert_eq!(log_state.last_purged_log_id, Some(log_id(1, 5)));
		assert_eq!(log_state.last_log_id, Some(log_id(1, 6)));
		let recovered = store.recovered_state_machine().await.unwrap();
		let state = recovered.state_snapshot().await;
		assert_eq!(state.last_applied_log_id, Some(log_id(1, 5)));
		assert!(state.leases.contains_key(&0));
		assert!(!state.leases.contains_key(&10));
	}

	#[tokio::test]
	async fn built_snapshot_persists_artifact_and_recovers_on_reopen() {
		let dir = tempdir("snapshot-build-persist");
		let (_wal, _vote, snapshot_path) = artifact_paths(&dir);
		let mut store = open_store(&dir).await;
		let mut sm = store.recovered_state_machine().await.unwrap();

		append_and_apply(&mut store, &mut sm, lease_entry(1, 0, 9, 7)).await;

		let mut builder =
			<WendelonStateMachine as RaftStateMachine<WendelonTypeConfig>>::get_snapshot_builder(
				&mut sm,
			)
			.await;
		let built = builder.build_snapshot().await.unwrap();
		assert_eq!(built.meta.last_log_id, Some(log_id(1, 1)));
		assert!(
			std::fs::metadata(&snapshot_path).unwrap().len()
				> std::mem::size_of::<SnapshotHeader>() as u64
		);

		let reopened = open_store(&dir).await;
		let reopened_sm = reopened.recovered_state_machine().await.unwrap();
		let state = reopened_sm.state_snapshot().await;
		assert_eq!(state.last_applied_log_id, Some(log_id(1, 1)));
		assert!(state.leases.contains_key(&0));
	}

	#[tokio::test]
	async fn install_snapshot_stream_persists_artifact_and_recovers_on_reopen() {
		let source_dir = tempdir("snapshot-install-source");
		let mut source_store = open_store(&source_dir).await;
		let mut source_sm = source_store.recovered_state_machine().await.unwrap();
		append_and_apply(&mut source_store, &mut source_sm, lease_entry(1, 0, 9, 3)).await;

		let mut builder =
			<WendelonStateMachine as RaftStateMachine<WendelonTypeConfig>>::get_snapshot_builder(
				&mut source_sm,
			)
			.await;
		let built = builder.build_snapshot().await.unwrap();

		let target_dir = tempdir("snapshot-install-target");
		let (_wal, _vote, target_snapshot_path) = artifact_paths(&target_dir);
		let target_store = open_store(&target_dir).await;
		let mut target_sm = target_store.recovered_state_machine().await.unwrap();
		let meta = built.meta.clone();
		<WendelonStateMachine as RaftStateMachine<WendelonTypeConfig>>::install_snapshot(
			&mut target_sm,
			&meta,
			built.snapshot,
		)
		.await
		.unwrap();

		assert!(
			std::fs::metadata(&target_snapshot_path).unwrap().len()
				> std::mem::size_of::<SnapshotHeader>() as u64
		);
		let installed = target_sm.state_snapshot().await;
		assert_eq!(installed.last_applied_log_id, Some(log_id(1, 1)));
		assert!(installed.leases.contains_key(&0));
		assert_eq!(
			target_sm
				.inner
				.read()
				.await
				.snapshot
				.as_ref()
				.unwrap()
				.meta
				.last_log_id,
			Some(log_id(1, 1))
		);

		let reopened = open_store(&target_dir).await;
		let reopened_sm = reopened.recovered_state_machine().await.unwrap();
		let state = reopened_sm.state_snapshot().await;
		assert_eq!(state.last_applied_log_id, Some(log_id(1, 1)));
		assert!(state.leases.contains_key(&0));
	}

	#[tokio::test]
	async fn install_snapshot_rejects_full_identity_mismatch_before_publication() {
		let source_dir = tempdir("snapshot-install-identity-source");
		let mut source_store = open_store(&source_dir).await;
		let mut source_sm = source_store.recovered_state_machine().await.unwrap();
		append_and_apply(&mut source_store, &mut source_sm, lease_entry(1, 0, 9, 3)).await;
		let mut builder =
			<WendelonStateMachine as RaftStateMachine<WendelonTypeConfig>>::get_snapshot_builder(
				&mut source_sm,
			)
			.await;
		let built = builder.build_snapshot().await.unwrap();
		let mut mismatched_meta = built.meta.clone();
		mismatched_meta.last_log_id = Some(log_id_for(2, 1, 1));

		let target_dir = tempdir("snapshot-install-identity-target");
		let (_, _, target_snapshot_path) = artifact_paths(&target_dir);
		let target_store = open_store(&target_dir).await;
		let mut target_sm = target_store.recovered_state_machine().await.unwrap();
		let err = <WendelonStateMachine as RaftStateMachine<WendelonTypeConfig>>::install_snapshot(
			&mut target_sm,
			&mismatched_meta,
			built.snapshot,
		)
		.await
		.expect_err("install meta may not replace the body identity");
		assert!(err.to_string().contains("snapshot body last_applied"));
		assert!(!target_snapshot_path.exists());
		assert_eq!(target_sm.state_snapshot().await.last_applied_log_id, None);
	}

	#[tokio::test]
	async fn install_snapshot_rejects_header_coordinate_corruption_before_publication() {
		for coordinate in ["term", "index"] {
			let source_dir = tempdir(&format!("snapshot-install-header-{coordinate}-source"));
			let (_, _, source_snapshot_path) = artifact_paths(&source_dir);
			let mut source_store = open_store(&source_dir).await;
			let mut source_sm = source_store.recovered_state_machine().await.unwrap();
			append_and_apply(&mut source_store, &mut source_sm, lease_entry(1, 0, 9, 3)).await;
			let mut builder =
				<WendelonStateMachine as RaftStateMachine<WendelonTypeConfig>>::get_snapshot_builder(
					&mut source_sm,
				)
				.await;
			let built = builder.build_snapshot().await.unwrap();
			let meta = built.meta.clone();
			drop(built.snapshot);
			rewrite_snapshot_header(&source_snapshot_path, |header| match coordinate {
				"term" => header.last_included_term += 1,
				"index" => header.last_included_index += 1,
				_ => unreachable!(),
			});
			let corrupted =
				SnapshotDataFile::open(Arc::new(BlockingFileOps), source_snapshot_path.clone())
					.await
					.unwrap();

			let target_dir = tempdir(&format!("snapshot-install-header-{coordinate}-target"));
			let (_, _, target_snapshot_path) = artifact_paths(&target_dir);
			let target_store = open_store(&target_dir).await;
			let mut target_sm = target_store.recovered_state_machine().await.unwrap();
			let err =
				<WendelonStateMachine as RaftStateMachine<WendelonTypeConfig>>::install_snapshot(
					&mut target_sm,
					&meta,
					Box::new(corrupted),
				)
				.await
				.expect_err("header coordinate corruption must fail install");
			assert!(
				err.to_string()
					.contains("snapshot header/body identity mismatch")
			);
			assert!(!target_snapshot_path.exists());
		}
	}

	#[tokio::test]
	async fn install_snapshot_membership_mismatch_remains_rejected() {
		let source_dir = tempdir("snapshot-install-membership-source");
		let mut source_store = open_store(&source_dir).await;
		let mut source_sm = source_store.recovered_state_machine().await.unwrap();
		append_and_apply(&mut source_store, &mut source_sm, lease_entry(1, 0, 9, 3)).await;
		let mut builder =
			<WendelonStateMachine as RaftStateMachine<WendelonTypeConfig>>::get_snapshot_builder(
				&mut source_sm,
			)
			.await;
		let built = builder.build_snapshot().await.unwrap();
		let mut mismatched_meta = built.meta.clone();
		mismatched_meta.last_membership = StoredMembership::new(
			Some(log_id_for(2, 1, 1)),
			openraft::Membership::new(vec![std::collections::BTreeSet::from([2])], ()),
		);

		let target_dir = tempdir("snapshot-install-membership-target");
		let (_, _, target_snapshot_path) = artifact_paths(&target_dir);
		let target_store = open_store(&target_dir).await;
		let mut target_sm = target_store.recovered_state_machine().await.unwrap();
		let err = <WendelonStateMachine as RaftStateMachine<WendelonTypeConfig>>::install_snapshot(
			&mut target_sm,
			&mismatched_meta,
			built.snapshot,
		)
		.await
		.expect_err("membership mismatch must remain rejected");
		assert!(err.to_string().contains("snapshot membership disagrees"));
		assert!(!target_snapshot_path.exists());
	}

	#[tokio::test]
	async fn installed_snapshot_roundtrips_table_schema_catalog() {
		let source_dir = tempdir("snapshot-table-schema-source");
		let mut source_store = open_store(&source_dir).await;
		let mut source_sm = source_store.recovered_state_machine().await.unwrap();
		let descriptor = recovery_table_descriptor(1);
		append_and_apply(
			&mut source_store,
			&mut source_sm,
			schema_update_entry(1, &descriptor, 0, 1, SchemaMutationKind::Create),
		)
		.await;

		let mut builder =
			<WendelonStateMachine as RaftStateMachine<WendelonTypeConfig>>::get_snapshot_builder(
				&mut source_sm,
			)
			.await;
		let built = builder.build_snapshot().await.unwrap();

		let target_dir = tempdir("snapshot-table-schema-target");
		let target_store = open_store(&target_dir).await;
		let mut target_sm = target_store.recovered_state_machine().await.unwrap();
		let meta = built.meta.clone();
		<WendelonStateMachine as RaftStateMachine<WendelonTypeConfig>>::install_snapshot(
			&mut target_sm,
			&meta,
			built.snapshot,
		)
		.await
		.unwrap();

		let installed = target_sm.state_snapshot().await;
		assert_eq!(installed.last_applied_log_id, Some(log_id(1, 1)));
		assert_eq!(
			installed
				.table_schemas
				.get(TenantId::new(7), descriptor.table_object_id, 1),
			Some(&descriptor)
		);
		assert_eq!(
			installed
				.table_schemas
				.latest_by_name(TenantId::new(7), "analytics", "events"),
			Some(&descriptor)
		);

		let reopened = open_store(&target_dir).await;
		let reopened_sm = reopened.recovered_state_machine().await.unwrap();
		let recovered = reopened_sm.state_snapshot().await;
		assert_eq!(
			recovered
				.table_schemas
				.latest_by_id(TenantId::new(7), descriptor.table_object_id),
			Some(&descriptor)
		);
		assert_eq!(
			recovered
				.table_schemas
				.latest_by_name(TenantId::new(7), "analytics", "events"),
			Some(&descriptor)
		);
	}

	#[tokio::test]
	async fn adr0096_recovery_and_snapshot_install_refresh_debt_from_published_state() {
		let operation_id = DropOperationId {
			tenant_id: TenantId::new(7),
			table_object_id: TableObjectId::new([0x96; 16]),
			schema_generation: SchemaGeneration::new(1),
		};
		let debt = TableDropReclaimDebt {
			operation_id,
			fence_log_index: 8,
			finalize_log_index: 9,
			ownership_epoch: 3,
			participants: TableDropParticipantSet::new([SchemaNodeId {
				id: 1,
				_reserved: 0,
			}]),
		};
		let mut state = SmState::default();
		state.last_applied_log_id = Some(log_id(1, 9));
		state.table_drop_reclaim_debts.insert_validated(debt);

		let source_dir = tempdir("adr0096-recovery-source");
		let (_, _, source_snapshot_path) = artifact_paths(&source_dir);
		write_snapshot_file(&source_snapshot_path, &state);
		let source_store = open_store(&source_dir).await;
		let (loaded, entries, checkpoint) = {
			let guard = source_store.inner.read().await;
			(
				guard.recovered_snapshot.clone(),
				guard.entries.values().map(clone_entry).collect::<Vec<_>>(),
				guard.recovery_checkpoint,
			)
		};
		let recovery_metrics = Box::leak(Box::new(MetricsRegistry::default()));
		let mut source_sm = WendelonStateMachine::from_recovery_with_metrics(
			loaded,
			entries,
			checkpoint,
			SnapshotRuntime::ephemeral(),
			recovery_metrics,
		)
		.unwrap();
		assert_eq!(recovery_metrics.drop_reclaim_debts_for_test(), 1);
		assert_eq!(recovery_metrics.drop_reclaim_settlements_for_test(), 0);

		let mut builder =
			<WendelonStateMachine as RaftStateMachine<WendelonTypeConfig>>::get_snapshot_builder(
				&mut source_sm,
			)
			.await;
		let built = builder.build_snapshot().await.unwrap();
		let target_dir = tempdir("adr0096-install-target");
		let target_store = open_store(&target_dir).await;
		let install_metrics = Box::leak(Box::new(MetricsRegistry::default()));
		let mut target_sm = target_store
			.recovered_state_machine()
			.await
			.unwrap()
			.with_observability_metrics_for_test(install_metrics)
			.await;
		assert_eq!(install_metrics.drop_reclaim_debts_for_test(), 0);
		let meta = built.meta.clone();
		<WendelonStateMachine as RaftStateMachine<WendelonTypeConfig>>::install_snapshot(
			&mut target_sm,
			&meta,
			built.snapshot,
		)
		.await
		.unwrap();
		assert_eq!(install_metrics.drop_reclaim_debts_for_test(), 1);
		assert_eq!(
			target_sm
				.state_snapshot()
				.await
				.table_drop_reclaim_debts
				.len(),
			1
		);
	}

	#[tokio::test]
	async fn admission_snapshot_watch_remains_monotonic_across_apply_and_snapshot_install() {
		let target_dir = tempdir("admission-snapshot-monotonic-target");
		let mut target_store = open_store(&target_dir).await;
		let mut target_sm = target_store.recovered_state_machine().await.unwrap();
		let mut snapshots = target_sm.subscribe_admission_snapshot();

		append_and_apply(&mut target_store, &mut target_sm, finalize_entry(1, 111)).await;
		snapshots.changed().await.unwrap();
		let after_apply = snapshots.borrow().clone();
		assert_eq!(after_apply.current_epoch, 1);
		assert_eq!(
			after_apply.e_safe_watermarks.get(&1).copied(),
			Some(HybridTimestamp {
				physical_ms: 111,
				logical: 0,
				node_id: 1,
			})
		);

		let source_dir = tempdir("admission-snapshot-monotonic-source");
		let mut source_store = open_store(&source_dir).await;
		let mut source_sm = source_store.recovered_state_machine().await.unwrap();
		append_and_apply(&mut source_store, &mut source_sm, finalize_entry(1, 111)).await;
		append_and_apply(
			&mut source_store,
			&mut source_sm,
			finalize_entry_for_epoch(2, 2, 222),
		)
		.await;
		let mut builder =
			<WendelonStateMachine as RaftStateMachine<WendelonTypeConfig>>::get_snapshot_builder(
				&mut source_sm,
			)
			.await;
		let built = builder.build_snapshot().await.unwrap();
		let meta = built.meta.clone();
		<WendelonStateMachine as RaftStateMachine<WendelonTypeConfig>>::install_snapshot(
			&mut target_sm,
			&meta,
			built.snapshot,
		)
		.await
		.unwrap();
		snapshots.changed().await.unwrap();
		let after_install = snapshots.borrow().clone();
		assert!(after_install.current_epoch >= after_apply.current_epoch);
		for (epoch, watermark) in after_apply.e_safe_watermarks.iter() {
			if let Some(next) = after_install.e_safe_watermarks.get(epoch) {
				assert!(next >= watermark);
			}
		}
		assert_eq!(
			after_install.e_safe_watermarks.get(&2).copied(),
			Some(HybridTimestamp {
				physical_ms: 222,
				logical: 0,
				node_id: 1,
			})
		);
	}

	#[tokio::test]
	async fn admission_snapshot_watch_does_not_wake_for_unrelated_apply() {
		let dir = tempdir("admission-snapshot-noop");
		let mut store = open_store(&dir).await;
		let mut sm = store.recovered_state_machine().await.unwrap();
		let mut snapshots = sm.subscribe_admission_snapshot();

		append_and_apply(&mut store, &mut sm, lease_entry(1, 0, 9, 7)).await;

		let changed =
			tokio::time::timeout(std::time::Duration::from_millis(50), snapshots.changed()).await;
		assert!(
			changed.is_err(),
			"lease-only apply should not publish a new admission snapshot"
		);
	}

	#[tokio::test]
	async fn revocation_snapshot_watch_updates_on_revoke_actor_key() {
		let mut sm = WendelonStateMachine::new();
		let mut snapshots = sm.subscribe_revocation_snapshot();
		let revocation = RevokeActorKey {
			trust_domain: ActorTrustDomain::Ops,
			key_id: vec![1, 2, 3, 4],
			revoked_at_ms: 1,
		};
		assert!(
			!snapshots
				.borrow()
				.contains(revocation.trust_domain, &revocation.key_id,)
		);

		<WendelonStateMachine as RaftStateMachine<WendelonTypeConfig>>::apply(
			&mut sm,
			[Entry {
				log_id: log_id(1, 1),
				payload: EntryPayload::Normal(WendelonRequest::RevokeActorKey(revocation.clone())),
			}],
		)
		.await
		.unwrap();

		snapshots.changed().await.unwrap();
		let snapshot = snapshots.borrow().clone();
		assert!(snapshot.contains(revocation.trust_domain, &revocation.key_id));
		let state = sm.state_snapshot().await;
		assert_eq!(
			state.revoked_actor_keys.get(&revocation.key()).copied(),
			Some(revocation.revoked_at_ms)
		);
	}

	#[tokio::test]
	async fn revoke_actor_key_prunes_expired_entries_on_new_revoke() {
		let mut sm = WendelonStateMachine::new();
		let old = RevokeActorKey {
			trust_domain: ActorTrustDomain::Ops,
			key_id: vec![1, 2, 3, 4],
			revoked_at_ms: 1,
		};
		let fresh = RevokeActorKey {
			trust_domain: ActorTrustDomain::Ops,
			key_id: vec![5, 6, 7, 8],
			revoked_at_ms: REVOKED_ACTOR_KEY_RETENTION_MS + 2,
		};

		<WendelonStateMachine as RaftStateMachine<WendelonTypeConfig>>::apply(
			&mut sm,
			[
				Entry {
					log_id: log_id(1, 1),
					payload: EntryPayload::Normal(WendelonRequest::RevokeActorKey(old.clone())),
				},
				Entry {
					log_id: log_id(1, 2),
					payload: EntryPayload::Normal(WendelonRequest::RevokeActorKey(fresh.clone())),
				},
			],
		)
		.await
		.unwrap();

		let state = sm.state_snapshot().await;
		assert!(!state.revoked_actor_keys.contains_key(&old.key()));
		assert_eq!(
			state.revoked_actor_keys.get(&fresh.key()).copied(),
			Some(fresh.revoked_at_ms)
		);
	}

	#[tokio::test]
	async fn tenant_keysets_survive_snapshot_reopen_and_presnapshot_slabs_still_decrypt() {
		let dir = tempdir("snapshot-tenant-keysets");
		let mut store = open_store(&dir).await;
		let mut sm = store.recovered_state_machine().await.unwrap();
		let crypto = crate::crypto::test_support::persistent_dev_crypto_manager(&dir).unwrap();

		let first = crypto.prepare_rotation(TenantId::ZERO).unwrap();
		crypto.apply_rotation(&first).unwrap();
		let encrypted = crypto
			.encrypt_tiered_slab_object(
				SlabId::new(42),
				&crate::crypto::TieredSlabObjectMetadata {
					tenant_id: TenantId::ZERO,
					created_hlc: 0,
					chunk_size_bytes: crate::crypto::SLAB_ENCRYPTION_DEFAULT_CHUNK_SIZE,
					offset_index: vec![crate::crypto::TieredOffsetIndexEntry {
						key_locator: crate::schema::IdentityPrefix::ZERO,
						byte_offset: 0,
						byte_length: u32::try_from(b"before-snapshot".len()).unwrap(),
						flags: 0,
					}],
					column_stats_block: None,
					has_row_tombstone_records: false,
				},
				b"before-snapshot",
			)
			.unwrap();
		let second = crypto.prepare_rotation(TenantId::ZERO).unwrap();

		append_and_apply(
			&mut store,
			&mut sm,
			Entry {
				log_id: log_id(1, 1),
				payload: EntryPayload::Normal(WendelonRequest::CreateTenant(
					create_tenant_for_rotation(TenantId::ZERO, &first),
				)),
			},
		)
		.await;
		append_and_apply(
			&mut store,
			&mut sm,
			Entry {
				log_id: log_id(1, 2),
				payload: EntryPayload::Normal(WendelonRequest::InstallTenantKekVersion(
					second.clone(),
				)),
			},
		)
		.await;

		let mut builder =
			<WendelonStateMachine as RaftStateMachine<WendelonTypeConfig>>::get_snapshot_builder(
				&mut sm,
			)
			.await;
		builder.build_snapshot().await.unwrap();

		let reopened = open_store(&dir).await;
		let reopened_sm = reopened.recovered_state_machine().await.unwrap();
		let state = reopened_sm.state_snapshot().await;
		let keyset = state
			.tenant_keysets
			.get(&TenantId::ZERO)
			.expect("tenant keyset must survive snapshot reopen");
		assert_eq!(keyset.active_tenant_kek_id, Some(second.tenant_kek_id));
		assert_eq!(keyset.keys.len(), 2);
		assert!(keyset.keys.contains_key(&first.tenant_kek_id));
		assert!(keyset.keys.contains_key(&second.tenant_kek_id));

		let recovered_crypto =
			crate::crypto::test_support::persistent_dev_crypto_manager(&dir).unwrap();
		recovered_crypto
			.replace_keysets_from_authoritative_snapshot(&state.tenant_keysets)
			.unwrap();
		let decrypted = recovered_crypto
			.decrypt_tiered_slab(SlabId::new(42), &encrypted.bytes)
			.unwrap();
		assert_eq!(decrypted.plaintext, b"before-snapshot");
		assert_eq!(decrypted.tenant_kek_id, first.tenant_kek_id);
	}

	#[derive(Default)]
	struct FlakyTenantKekObserver {
		rotations: Mutex<Vec<(TenantId, crate::schema::TenantKekId)>>,
		synced: Mutex<Option<HashMap<TenantId, TenantKeyset>>>,
	}

	#[async_trait::async_trait]
	impl TenantKekObserver for FlakyTenantKekObserver {
		async fn on_tenant_kek_rotated(
			&self,
			rotation: &InstallTenantKekVersion,
			_applied_log_index: u64,
		) -> std::io::Result<()> {
			self.rotations
				.lock()
				.unwrap()
				.push((rotation.tenant_id, rotation.tenant_kek_id));
			Err(Error::other("injected tenant KEK observer failure"))
		}

		async fn sync_tenant_keysets(
			&self,
			snapshot: &HashMap<TenantId, TenantKeyset>,
		) -> std::io::Result<()> {
			*self.synced.lock().unwrap() = Some(snapshot.clone());
			Ok(())
		}
	}

	#[tokio::test]
	async fn tenant_kek_observer_failure_reconciles_from_snapshot_sync() {
		let dir = tempdir("tenant-kek-observer-sync");
		let observer = Arc::new(FlakyTenantKekObserver::default());
		let mut store = open_store(&dir).await;
		let mut sm = store
			.recovered_state_machine()
			.await
			.unwrap()
			.with_tenant_kek_observer(observer.clone());
		let crypto = crate::crypto::CryptoManager::new_dev();
		let seed = crypto.prepare_rotation(TenantId::ZERO).unwrap();
		let rotation = crypto.prepare_rotation(TenantId::ZERO).unwrap();

		append_and_apply(
			&mut store,
			&mut sm,
			Entry {
				log_id: log_id(1, 1),
				payload: EntryPayload::Normal(WendelonRequest::CreateTenant(
					create_tenant_for_rotation(TenantId::ZERO, &seed),
				)),
			},
		)
		.await;
		append_and_apply(
			&mut store,
			&mut sm,
			Entry {
				log_id: log_id(1, 2),
				payload: EntryPayload::Normal(WendelonRequest::InstallTenantKekVersion(
					rotation.clone(),
				)),
			},
		)
		.await;

		assert_eq!(
			observer.rotations.lock().unwrap().as_slice(),
			&[(TenantId::ZERO, rotation.tenant_kek_id)]
		);
		let synced = observer
			.synced
			.lock()
			.unwrap()
			.clone()
			.expect("snapshot sync must run after observer failure");
		assert_eq!(
			synced.get(&TenantId::ZERO).unwrap().active_tenant_kek_id,
			Some(rotation.tenant_kek_id)
		);
	}

	#[tokio::test]
	async fn fault_injected_torn_tail_truncates_to_last_valid_prefix() {
		let dir = tempdir("fault-torn-tail");
		let file_ops = Arc::new(FaultyFileOps::new(dir.clone()).unwrap());
		let mut store =
			open_store_with_faults(&dir, file_ops.clone(), (DIRECT_IO_ALIGN as u64) * 2).await;

		store
			.blocking_append(vec![lease_entry(1, 0, 9, 1)])
			.await
			.unwrap();
		file_ops.inject(InjectedFault::on(
			FaultOp::WriteAt,
			FaultAction::PartialWriteError {
				written_bytes: 128,
				persist_partial: true,
				kind: ErrorKind::Interrupted,
				message: "fault-injected torn wal tail".into(),
			},
		));
		let append_err = store.blocking_append(vec![lease_entry(2, 10, 19, 2)]).await;
		assert!(append_err.is_err(), "faulted append must fail");
		drop(store);

		file_ops.crash().unwrap();

		let mut reopened =
			open_store_with_faults(&dir, file_ops.clone(), (DIRECT_IO_ALIGN as u64) * 2).await;
		let state = reopened.get_log_state().await.unwrap();
		assert_eq!(state.last_log_id, Some(log_id(1, 1)));
		let indexes = reopened
			.try_get_log_entries(0..=u64::MAX)
			.await
			.unwrap()
			.into_iter()
			.map(|entry| entry.log_id.index)
			.collect::<Vec<_>>();
		assert_eq!(indexes, vec![1]);
		let segments = named_segment_paths(&dir);
		assert_eq!(segments.len(), 1);
		assert_eq!(
			std::fs::metadata(&segments[0]).unwrap().len(),
			DIRECT_IO_ALIGN as u64
		);
	}

	#[tokio::test]
	async fn fault_injected_partial_commit_recovers_only_crash_durable_batch_prefix() {
		let dir = tempdir("fault-partial-commit");
		let file_ops = Arc::new(FaultyFileOps::new(dir.clone()).unwrap());
		let mut store =
			open_store_with_faults(&dir, file_ops.clone(), (DIRECT_IO_ALIGN as u64) * 3).await;

		file_ops.inject(
			InjectedFault::on(
				FaultOp::WriteAt,
				FaultAction::PartialWriteError {
					written_bytes: 128,
					persist_partial: true,
					kind: ErrorKind::Interrupted,
					message: "fault-injected partial batch commit".into(),
				},
			)
			.after_matches(1),
		);
		let append_err = store
			.blocking_append(vec![lease_entry(1, 0, 9, 1), lease_entry(2, 10, 19, 2)])
			.await;
		assert!(append_err.is_err(), "faulted batch append must fail");
		drop(store);

		file_ops.crash().unwrap();

		let mut reopened =
			open_store_with_faults(&dir, file_ops.clone(), (DIRECT_IO_ALIGN as u64) * 3).await;
		let state = reopened.get_log_state().await.unwrap();
		assert_eq!(state.last_log_id, Some(log_id(1, 1)));
		let indexes = reopened
			.try_get_log_entries(0..=u64::MAX)
			.await
			.unwrap()
			.into_iter()
			.map(|entry| entry.log_id.index)
			.collect::<Vec<_>>();
		assert_eq!(indexes, vec![1]);
	}

	#[tokio::test]
	async fn fault_injected_missing_directory_fsync_loses_rotated_segment_on_crash() {
		let dir = tempdir("fault-missing-dir-fsync");
		let file_ops = Arc::new(FaultyFileOps::new(dir.clone()).unwrap());
		let mut store =
			open_store_with_faults(&dir, file_ops.clone(), DIRECT_IO_ALIGN as u64).await;

		file_ops.inject(InjectedFault::on(FaultOp::FsyncDir, FaultAction::Skip));
		store
			.blocking_append(vec![lease_entry(1, 0, 9, 1)])
			.await
			.unwrap();
		drop(store);

		file_ops.crash().unwrap();

		let mut reopened =
			open_store_with_faults(&dir, file_ops.clone(), DIRECT_IO_ALIGN as u64).await;
		let state = reopened.get_log_state().await.unwrap();
		assert_eq!(state.last_log_id, None);
		assert!(
			reopened
				.try_get_log_entries(0..=u64::MAX)
				.await
				.unwrap()
				.is_empty()
		);
		assert!(named_segment_paths(&dir).is_empty());
	}

	#[tokio::test]
	async fn fault_injected_mid_segment_corruption_fails_recovery_loudly() {
		let dir = tempdir("fault-mid-segment");
		let file_ops = Arc::new(FaultyFileOps::new(dir.clone()).unwrap());
		let mut store =
			open_store_with_faults(&dir, file_ops.clone(), DIRECT_IO_ALIGN as u64).await;
		store
			.blocking_append(vec![lease_entry(1, 0, 9, 1), lease_entry(2, 10, 19, 2)])
			.await
			.unwrap();
		drop(store);

		file_ops.crash().unwrap();
		file_ops.inject(
			InjectedFault::on(
				FaultOp::ReadAll,
				FaultAction::CorruptReadAll {
					offset: 64,
					xor_mask: 0x5a,
				},
			)
			.for_path("control.wal.00000000000000000001.seg"),
		);
		let (wal, vote, snapshot) = artifact_paths(&dir);
		let err = WendelonLogStore::open_with_file_ops_and_options(
			&wal,
			&vote,
			&snapshot,
			file_ops.clone(),
			WalOptions {
				segment_bytes: DIRECT_IO_ALIGN as u64,
			},
		)
		.await
		.err()
		.expect("corrupted sealed segment must fail startup");
		assert_eq!(err.kind(), ErrorKind::InvalidData);
		assert!(
			err.to_string().contains("interior corruption")
				|| err.to_string().contains("checksum mismatch"),
			"unexpected error: {err}"
		);
	}

	#[tokio::test]
	async fn blocking_file_ops_metadata_sync_trace_fixture() {
		let dir = tempdir("blocking-file-ops-strace");
		let mut store = open_store_with_async_file_ops(
			&dir,
			Arc::new(BlockingFileOps),
			(DIRECT_IO_ALIGN as u64) * 2,
		)
		.await;
		store
			.blocking_append(vec![
				lease_entry(1, 0, 9, 1),
				lease_entry(2, 10, 19, 2),
				lease_entry(3, 20, 29, 3),
			])
			.await
			.unwrap();
		store.truncate(log_id(1, 2)).await.unwrap();

		let state = store.get_log_state().await.unwrap();
		assert_eq!(state.last_log_id, Some(log_id(1, 1)));
		let indexes = store
			.try_get_log_entries(0..=u64::MAX)
			.await
			.unwrap()
			.into_iter()
			.map(|entry| entry.log_id.index)
			.collect::<Vec<_>>();
		assert_eq!(indexes, vec![1]);
	}

	#[tokio::test]
	async fn raft_append_returns_after_fdatasync_completes() {
		let dir = tempdir("raft-append-order");
		let file_ops = Arc::new(RecordingFileOps::new());
		let mut store =
			open_store_with_async_file_ops(&dir, file_ops.clone(), (DIRECT_IO_ALIGN as u64) * 2)
				.await;
		let events = file_ops.events.clone();
		let append = tokio::spawn(async move {
			store
				.blocking_append(vec![lease_entry(1, 0, 9, 1)])
				.await
				.unwrap();
			events.lock().unwrap().push("ack");
		});

		file_ops.fdatasync_started.notified().await;
		tokio::time::sleep(Duration::from_millis(5)).await;
		assert!(
			!append.is_finished(),
			"raft append returned before wal fdatasync completed"
		);
		file_ops.release_fdatasync.notify_waiters();
		append.await.unwrap();

		let events = file_ops.events.lock().unwrap().clone();
		let fdatasync_end = events
			.iter()
			.position(|event| *event == "fdatasync_end")
			.expect("fdatasync completion must be recorded");
		let ack = events
			.iter()
			.position(|event| *event == "ack")
			.expect("append completion must be recorded");
		assert!(
			fdatasync_end < ack,
			"raft append completion must follow fdatasync completion"
		);
	}

	#[tokio::test]
	async fn append_rotates_into_named_segments_and_recovers_them() {
		let dir = tempdir("segment-rotate");
		let mut store = open_store_with_segment_bytes(&dir, DIRECT_IO_ALIGN as u64).await;
		store
			.blocking_append(vec![
				lease_entry(1, 0, 9, 1),
				lease_entry(2, 10, 19, 2),
				lease_entry(3, 20, 29, 3),
			])
			.await
			.unwrap();

		let segments = named_segment_paths(&dir);
		assert_eq!(segments.len(), 3);
		assert!(
			segments[0]
				.file_name()
				.and_then(|name| name.to_str())
				.unwrap()
				.contains("00000000000000000001")
		);

		let mut reopened = open_store_with_segment_bytes(&dir, DIRECT_IO_ALIGN as u64).await;
		let state = reopened.get_log_state().await.unwrap();
		assert_eq!(state.last_log_id, Some(log_id(1, 3)));
		let indexes = reopened
			.try_get_log_entries(0..=u64::MAX)
			.await
			.unwrap()
			.into_iter()
			.map(|entry| entry.log_id.index)
			.collect::<Vec<_>>();
		assert_eq!(indexes, vec![1, 2, 3]);
	}

	#[tokio::test]
	async fn truncate_rewinds_segmented_tail_on_disk() {
		let dir = tempdir("segment-truncate");
		let mut store = open_store_with_segment_bytes(&dir, DIRECT_IO_ALIGN as u64).await;
		store
			.blocking_append(vec![
				lease_entry(1, 0, 9, 1),
				lease_entry(2, 10, 19, 2),
				lease_entry(3, 20, 29, 3),
			])
			.await
			.unwrap();

		store.truncate(log_id(1, 3)).await.unwrap();
		let segments = named_segment_paths(&dir);
		assert_eq!(segments.len(), 2);

		let mut reopened = open_store_with_segment_bytes(&dir, DIRECT_IO_ALIGN as u64).await;
		let indexes = reopened
			.try_get_log_entries(0..=u64::MAX)
			.await
			.unwrap()
			.into_iter()
			.map(|entry| entry.log_id.index)
			.collect::<Vec<_>>();
		assert_eq!(indexes, vec![1, 2]);
	}

	#[tokio::test]
	async fn purge_drops_fully_purged_segments() {
		let dir = tempdir("segment-purge");
		let (_, _, snapshot_path) = artifact_paths(&dir);
		let mut store = open_store_with_segment_bytes(&dir, DIRECT_IO_ALIGN as u64).await;
		store
			.blocking_append(vec![
				lease_entry(1, 0, 9, 1),
				lease_entry(2, 10, 19, 2),
				lease_entry(3, 20, 29, 3),
			])
			.await
			.unwrap();
		let mut snapshot_state = SmState::default();
		snapshot_state.last_applied_log_id = Some(log_id(1, 2));
		write_snapshot_file(&snapshot_path, &snapshot_state);

		store.purge(log_id(1, 2)).await.unwrap();
		let segments = named_segment_paths(&dir);
		assert_eq!(segments.len(), 1);
		assert!(
			segments[0]
				.file_name()
				.and_then(|name| name.to_str())
				.unwrap()
				.contains("00000000000000000003")
		);

		let mut reopened = open_store_with_segment_bytes(&dir, DIRECT_IO_ALIGN as u64).await;
		let indexes = reopened
			.try_get_log_entries(0..=u64::MAX)
			.await
			.unwrap()
			.into_iter()
			.map(|entry| entry.log_id.index)
			.collect::<Vec<_>>();
		assert_eq!(indexes, vec![3]);
	}

	#[tokio::test]
	async fn checkpoint_coordinator_respects_recovered_checkpoint_threshold() {
		let dir = tempdir("checkpoint-threshold");
		let mut store = open_store(&dir).await;
		let initial_coordinator = CheckpointCoordinator::new_with_config(
			store.clone(),
			CheckpointCoordinatorConfig {
				entry_threshold: 1,
				time_threshold: Duration::from_secs(60),
			},
		);
		let mut initial_sm =
			WendelonStateMachine::new().with_checkpoint_observer(initial_coordinator);
		append_and_apply(&mut store, &mut initial_sm, finalize_entry(1, 111)).await;
		assert_eq!(
			store.recovery_checkpoint().await,
			Some(RecoveryCheckpoint {
				commit_index: 1,
				e_safe_index: 1,
			})
		);

		let resumed_coordinator = CheckpointCoordinator::new_with_config(
			store.clone(),
			CheckpointCoordinatorConfig {
				entry_threshold: 2,
				time_threshold: Duration::from_secs(60),
			},
		);
		let mut resumed_sm = store
			.recovered_state_machine()
			.await
			.unwrap()
			.with_checkpoint_observer(resumed_coordinator);
		append_and_apply(&mut store, &mut resumed_sm, finalize_entry(2, 222)).await;
		assert_eq!(
			store.recovery_checkpoint().await,
			Some(RecoveryCheckpoint {
				commit_index: 1,
				e_safe_index: 1,
			})
		);

		append_and_apply(&mut store, &mut resumed_sm, finalize_entry(3, 333)).await;
		assert_eq!(
			store.recovery_checkpoint().await,
			Some(RecoveryCheckpoint {
				commit_index: 3,
				e_safe_index: 3,
			})
		);
	}

	#[tokio::test]
	async fn checkpoint_coordinator_flushes_on_timer() {
		let dir = tempdir("checkpoint-timer");
		let mut store = open_store(&dir).await;
		let coordinator = CheckpointCoordinator::new_with_config(
			store.clone(),
			CheckpointCoordinatorConfig {
				entry_threshold: u64::MAX,
				time_threshold: Duration::from_millis(40),
			},
		);
		let timer = coordinator.clone().spawn();
		let mut sm = WendelonStateMachine::new().with_checkpoint_observer(coordinator);

		append_and_apply(&mut store, &mut sm, finalize_entry(1, 111)).await;
		assert_eq!(store.recovery_checkpoint().await, None);

		tokio::time::sleep(Duration::from_millis(150)).await;
		assert_eq!(
			store.recovery_checkpoint().await,
			Some(RecoveryCheckpoint {
				commit_index: 1,
				e_safe_index: 1,
			})
		);
		timer.abort();
	}

	#[tokio::test]
	async fn checkpoint_coordinator_flushes_on_snapshot_boundary() {
		let dir = tempdir("checkpoint-snapshot");
		let mut store = open_store(&dir).await;
		let coordinator = CheckpointCoordinator::new_with_config(
			store.clone(),
			CheckpointCoordinatorConfig {
				entry_threshold: u64::MAX,
				time_threshold: Duration::from_secs(60),
			},
		);
		let mut sm = WendelonStateMachine::new().with_checkpoint_observer(coordinator);

		append_and_apply(&mut store, &mut sm, finalize_entry(1, 111)).await;
		assert_eq!(store.recovery_checkpoint().await, None);

		let mut builder =
			<WendelonStateMachine as RaftStateMachine<WendelonTypeConfig>>::get_snapshot_builder(
				&mut sm,
			)
			.await;
		let _snapshot = builder.build_snapshot().await.unwrap();

		assert_eq!(
			store.recovery_checkpoint().await,
			Some(RecoveryCheckpoint {
				commit_index: 1,
				e_safe_index: 1,
			})
		);
	}

	#[tokio::test]
	async fn checkpoint_can_recover_from_checkpoint_only_tail_segment() {
		let dir = tempdir("checkpoint-tail-segment");
		let mut store = open_store_with_segment_bytes(&dir, DIRECT_IO_ALIGN as u64).await;
		let coordinator = CheckpointCoordinator::new_with_config(
			store.clone(),
			CheckpointCoordinatorConfig {
				entry_threshold: 1,
				time_threshold: Duration::from_secs(60),
			},
		);
		let mut sm = WendelonStateMachine::new().with_checkpoint_observer(coordinator);

		append_and_apply(&mut store, &mut sm, finalize_entry(1, 111)).await;
		assert_eq!(
			store.recovery_checkpoint().await,
			Some(RecoveryCheckpoint {
				commit_index: 1,
				e_safe_index: 1,
			})
		);

		let segments = named_segment_paths(&dir);
		assert_eq!(segments.len(), 2);
		assert!(
			segments[1]
				.file_name()
				.and_then(|name| name.to_str())
				.unwrap()
				.contains("00000000000000000002")
		);

		let mut reopened = open_store_with_segment_bytes(&dir, DIRECT_IO_ALIGN as u64).await;
		assert_eq!(
			reopened.recovery_checkpoint().await,
			Some(RecoveryCheckpoint {
				commit_index: 1,
				e_safe_index: 1,
			})
		);
		assert_eq!(
			reopened.get_log_state().await.unwrap().last_log_id,
			Some(log_id(1, 1))
		);

		reopened
			.blocking_append(vec![finalize_entry(2, 222)])
			.await
			.unwrap();
		let mut reopened_again = open_store_with_segment_bytes(&dir, DIRECT_IO_ALIGN as u64).await;
		let indexes = reopened_again
			.try_get_log_entries(0..=u64::MAX)
			.await
			.unwrap()
			.into_iter()
			.map(|entry| entry.log_id.index)
			.collect::<Vec<_>>();
		assert_eq!(indexes, vec![1, 2]);
	}
}

// ============================================================
//  LEASE APPLY TESTS
// ============================================================

#[cfg(test)]
mod lease_tests {
	use super::*;
	use crate::schema::{
		HybridTimestamp, LeaseDescriptor, NodeId as SchemaNodeId, ReleaseLeaseRequestMeta,
	};

	fn hlc(physical_ms: u64) -> HybridTimestamp {
		HybridTimestamp {
			physical_ms,
			logical: 0,
			node_id: 0,
		}
	}

	fn lease(
		holder_id: u32,
		epoch: u64,
		range: (u64, u64),
		start_ms: u64,
		expiry_ms: u64,
	) -> LeaseDescriptor {
		LeaseDescriptor {
			epoch,
			holder: SchemaNodeId {
				id: holder_id,
				_reserved: 0,
			},
			range_start: range.0,
			range_end: range.1,
			lease_start: hlc(start_ms),
			lease_expiry: hlc(expiry_ms),
			min_commit_index: 0,
			issuer: SchemaNodeId {
				id: 1,
				_reserved: 0,
			},
			signature: [0; 64],
		}
	}

	fn release(holder_id: u32, epoch: u64, range: (u64, u64)) -> ReleaseLeaseRequestMeta {
		ReleaseLeaseRequestMeta {
			range_start: range.0,
			range_end: range.1,
			epoch,
			holder: SchemaNodeId {
				id: holder_id,
				_reserved: 0,
			},
		}
	}

	#[test]
	fn first_grant_succeeds_and_lands_in_state() {
		let mut state = SmState::default();
		let (resp, tier) = apply_request(
			&mut state,
			WendelonRequest::AcquireLease(lease(1, 1, (0, 99), 0, 100)),
		);
		assert_eq!(resp, WendelonResponse::Success);
		assert!(tier.is_none());
		assert_eq!(state.leases.len(), 1);
		let stored = state.leases.get(&0).expect("keyed by range_start=0");
		assert_eq!(stored.range_end, 99);
		assert_eq!(stored.holder.id, 1);
	}

	#[test]
	fn reversed_range_is_rejected() {
		let mut state = SmState::default();
		let (resp, _) = apply_request(
			&mut state,
			WendelonRequest::AcquireLease(lease(1, 1, (50, 10), 0, 100)),
		);
		assert_eq!(resp, WendelonResponse::LeaseDenied);
		assert!(state.leases.is_empty());
	}

	#[test]
	fn overlapping_valid_lease_from_other_holder_is_denied() {
		let mut state = SmState::default();
		apply_request(
			&mut state,
			WendelonRequest::AcquireLease(lease(1, 1, (0, 100), 0, 1000)),
		);
		let (resp, _) = apply_request(
			&mut state,
			// Different holder, overlapping range, same epoch, lease still live.
			WendelonRequest::AcquireLease(lease(2, 1, (50, 150), 100, 2000)),
		);
		assert_eq!(resp, WendelonResponse::LeaseDenied);
		// Original lease untouched.
		let stored = state.leases.get(&0).unwrap();
		assert_eq!(stored.holder.id, 1);
	}

	#[test]
	fn renewal_from_same_holder_same_range_extends_expiry() {
		let mut state = SmState::default();
		apply_request(
			&mut state,
			WendelonRequest::AcquireLease(lease(1, 1, (0, 100), 0, 500)),
		);
		let (resp, _) = apply_request(
			&mut state,
			WendelonRequest::AcquireLease(lease(1, 1, (0, 100), 400, 1500)),
		);
		assert_eq!(resp, WendelonResponse::Success);
		let stored = state.leases.get(&0).unwrap();
		assert_eq!(stored.lease_expiry.physical_ms, 1500);
	}

	#[test]
	fn expired_overlapping_lease_does_not_block_new_grant() {
		let mut state = SmState::default();
		// First lease expires at 100.
		apply_request(
			&mut state,
			WendelonRequest::AcquireLease(lease(1, 1, (0, 100), 0, 100)),
		);
		// Second grant asks to start at 150 — the old lease is
		// already expired, so even though it overlaps the range
		// and is in the same epoch the grant succeeds.
		let (resp, _) = apply_request(
			&mut state,
			WendelonRequest::AcquireLease(lease(2, 1, (0, 100), 150, 500)),
		);
		assert_eq!(resp, WendelonResponse::Success);
		assert_eq!(state.leases.get(&0).unwrap().holder.id, 2);
	}

	#[test]
	fn newer_epoch_invalidates_prior_overlap() {
		let mut state = SmState::default();
		apply_request(
			&mut state,
			WendelonRequest::AcquireLease(lease(1, 1, (0, 100), 0, 10_000)),
		);
		// New epoch — per plan 018 every prior-epoch lease is
		// invalid on the cluster transition. The grant must succeed
		// even though the old lease is temporally live.
		let (resp, _) = apply_request(
			&mut state,
			WendelonRequest::AcquireLease(lease(2, 2, (0, 100), 500, 5000)),
		);
		assert_eq!(resp, WendelonResponse::Success);
		assert_eq!(state.leases.get(&0).unwrap().epoch, 2);
	}

	#[test]
	fn non_overlapping_ranges_coexist() {
		let mut state = SmState::default();
		apply_request(
			&mut state,
			WendelonRequest::AcquireLease(lease(1, 1, (0, 99), 0, 1000)),
		);
		let (resp, _) = apply_request(
			&mut state,
			WendelonRequest::AcquireLease(lease(2, 1, (100, 199), 0, 1000)),
		);
		assert_eq!(resp, WendelonResponse::Success);
		assert_eq!(state.leases.len(), 2);
	}

	#[test]
	fn matching_release_removes_live_lease() {
		let mut state = SmState::default();
		apply_request(
			&mut state,
			WendelonRequest::AcquireLease(lease(1, 1, (0, 99), 0, 1_000)),
		);
		let (resp, _) = apply_request(
			&mut state,
			WendelonRequest::ReleaseLease(release(1, 1, (0, 99))),
		);
		assert_eq!(resp, WendelonResponse::Success);
		assert!(state.leases.is_empty());
	}

	#[test]
	fn release_is_idempotent_when_lease_already_absent() {
		let mut state = SmState::default();
		let (resp, _) = apply_request(
			&mut state,
			WendelonRequest::ReleaseLease(release(1, 1, (0, 99))),
		);
		assert_eq!(resp, WendelonResponse::Success);
		assert!(state.leases.is_empty());
	}

	#[test]
	fn release_for_other_holders_lease_is_denied() {
		let mut state = SmState::default();
		apply_request(
			&mut state,
			WendelonRequest::AcquireLease(lease(1, 1, (0, 99), 0, 1_000)),
		);
		let (resp, _) = apply_request(
			&mut state,
			WendelonRequest::ReleaseLease(release(2, 1, (0, 99))),
		);
		assert_eq!(resp, WendelonResponse::LeaseDenied);
		assert_eq!(state.leases.get(&0).unwrap().holder.id, 1);
	}
}

// ============================================================
//  MIGRATION LEDGER APPLY TESTS (Plan 019)
// ============================================================

#[cfg(test)]
mod migration_tests {
	use super::*;
	use crate::consensus::{
		MigrationComplete, MigrationLedgerEntry, MigrationState, SeedMigration,
	};
	use crate::schema::{HybridTimestamp, NodeId as SchemaNodeId};

	fn hlc(physical_ms: u64) -> HybridTimestamp {
		HybridTimestamp {
			physical_ms,
			logical: 0,
			node_id: 0,
		}
	}

	fn node(id: u32) -> SchemaNodeId {
		SchemaNodeId { id, _reserved: 0 }
	}

	fn entry(
		migration_id: u64,
		range: (u64, u64),
		to_epoch: u64,
		owner: u32,
		sources: Vec<u32>,
		upstream: Option<u64>,
	) -> MigrationLedgerEntry {
		MigrationLedgerEntry {
			migration_id,
			range_start: range.0,
			range_end: range.1,
			from_epoch: to_epoch - 1,
			to_epoch,
			logical_owner: node(owner),
			upstream_migration_id: upstream,
			source_replicas: sources.into_iter().map(node).collect(),
			migration_hlc_floor: hlc(1_000),
			state: MigrationState::Pending,
			completed_by: None,
			completed_at_hlc: None,
		}
	}

	#[test]
	fn seed_migration_inserts_ledger_entry() {
		let mut state = SmState::default();
		let (resp, _) = apply_request(
			&mut state,
			WendelonRequest::SeedMigration(SeedMigration {
				entry: entry(1, (0, 99), 2, 7, vec![1, 2], None),
			}),
		);
		assert_eq!(resp, WendelonResponse::Success);
		let stored = state.migrations.get(&1).unwrap();
		assert_eq!(stored.state, MigrationState::Pending);
		assert_eq!(stored.logical_owner.id, 7);
	}

	#[test]
	fn seed_migration_is_idempotent_on_replay() {
		let mut state = SmState::default();
		let mut e = entry(1, (0, 99), 2, 7, vec![1, 2], None);
		apply_request(
			&mut state,
			WendelonRequest::SeedMigration(SeedMigration { entry: e.clone() }),
		);
		// Simulate a racey replay with mutated source list; the
		// second apply must leave the original untouched so the
		// log is deterministic across replays.
		e.source_replicas = vec![node(99)];
		apply_request(
			&mut state,
			WendelonRequest::SeedMigration(SeedMigration { entry: e }),
		);
		assert_eq!(state.migrations.len(), 1);
		assert_eq!(state.migrations.get(&1).unwrap().source_replicas[0].id, 1);
	}

	#[test]
	fn migration_complete_transitions_state_and_installs_tombstone() {
		let mut state = SmState::default();
		apply_request(
			&mut state,
			WendelonRequest::SeedMigration(SeedMigration {
				entry: entry(1, (0, 99), 2, 7, vec![1, 2], None),
			}),
		);
		let done = MigrationComplete {
			migration_id: 1,
			range_start: 0,
			range_end: 99,
			to_epoch: 2,
			completed_by: node(7),
			completed_at_hlc: hlc(2_000),
			migration_hlc_floor: hlc(1_000),
		};
		let (resp, _) = apply_request(&mut state, WendelonRequest::MigrationComplete(done));
		assert_eq!(resp, WendelonResponse::Success);

		let stored = state.migrations.get(&1).unwrap();
		assert_eq!(stored.state, MigrationState::Completed);
		assert_eq!(stored.completed_by, Some(node(7)));
		assert_eq!(state.tombstones.len(), 1);
		assert_eq!(state.tombstones[0].migration_id, 1);
		assert_eq!(state.tombstones[0].completed_by.id, 7);
	}

	#[test]
	fn migration_complete_rejects_mismatched_floor() {
		let mut state = SmState::default();
		apply_request(
			&mut state,
			WendelonRequest::SeedMigration(SeedMigration {
				entry: entry(1, (0, 99), 2, 7, vec![1, 2], None),
			}),
		);
		let done = MigrationComplete {
			migration_id: 1,
			range_start: 0,
			range_end: 99,
			to_epoch: 2,
			completed_by: node(7),
			completed_at_hlc: hlc(2_000),
			migration_hlc_floor: hlc(9_999), // wrong
		};
		apply_request(&mut state, WendelonRequest::MigrationComplete(done));
		// Silently dropped — entry still pending, no tombstone.
		assert_eq!(
			state.migrations.get(&1).unwrap().state,
			MigrationState::Pending
		);
		assert!(state.tombstones.is_empty());
	}

	#[test]
	fn migration_complete_is_idempotent_on_replay() {
		let mut state = SmState::default();
		apply_request(
			&mut state,
			WendelonRequest::SeedMigration(SeedMigration {
				entry: entry(1, (0, 99), 2, 7, vec![1, 2], None),
			}),
		);
		let done = MigrationComplete {
			migration_id: 1,
			range_start: 0,
			range_end: 99,
			to_epoch: 2,
			completed_by: node(7),
			completed_at_hlc: hlc(2_000),
			migration_hlc_floor: hlc(1_000),
		};
		apply_request(&mut state, WendelonRequest::MigrationComplete(done.clone()));
		apply_request(&mut state, WendelonRequest::MigrationComplete(done));
		// Exactly one tombstone even after the replay.
		assert_eq!(state.tombstones.len(), 1);
	}

	#[test]
	fn migration_complete_advances_descendant_source_replicas() {
		// A -> B -> C chain: ledger holds two entries with
		// overlapping ranges; descendant (migration_id=2) points at
		// the original physical holders until predecessor commits.
		let mut state = SmState::default();
		apply_request(
			&mut state,
			WendelonRequest::SeedMigration(SeedMigration {
				entry: entry(1, (0, 99), 2, 7, vec![1, 2], None),
			}),
		);
		apply_request(
			&mut state,
			WendelonRequest::SeedMigration(SeedMigration {
				entry: entry(2, (0, 99), 3, 8, vec![1, 2], Some(1)),
			}),
		);

		let done = MigrationComplete {
			migration_id: 1,
			range_start: 0,
			range_end: 99,
			to_epoch: 2,
			completed_by: node(7),
			completed_at_hlc: hlc(2_000),
			migration_hlc_floor: hlc(1_000),
		};
		apply_request(&mut state, WendelonRequest::MigrationComplete(done));

		// Descendant now prefers the newly completed holder — the
		// completed_by node is hoisted to the front of source_replicas
		// and the upstream pointer clears.
		let descendant = state.migrations.get(&2).unwrap();
		assert_eq!(descendant.source_replicas[0].id, 7);
		assert_eq!(descendant.upstream_migration_id, None);
		assert_eq!(descendant.state, MigrationState::Pending);
	}

	/// Regression for Round 3 High #2: a direct descendant
	/// (`upstream_migration_id == Some(completed_id)`) must drop the
	/// predecessor's original physical holders entirely, not merely
	/// prepend `completed_by`. Under the drain-all rule any remaining
	/// holder still in the list would be required to ack — a down
	/// original holder would then fail the descendant's pull even
	/// after `completed_by` drained successfully. Once the predecessor
	/// publishes its authoritative drain, `completed_by` subsumes
	/// the predecessor's source set, so the descendant's list is
	/// collapsed to `[completed_by]`.
	#[test]
	fn chained_descendant_drops_superseded_holders_on_complete() {
		let mut state = SmState::default();
		apply_request(
			&mut state,
			WendelonRequest::SeedMigration(SeedMigration {
				entry: entry(1, (0, 99), 2, 7, vec![1, 2, 3], None),
			}),
		);
		apply_request(
			&mut state,
			WendelonRequest::SeedMigration(SeedMigration {
				entry: entry(2, (0, 99), 3, 8, vec![1, 2, 3], Some(1)),
			}),
		);

		let done = MigrationComplete {
			migration_id: 1,
			range_start: 0,
			range_end: 99,
			to_epoch: 2,
			completed_by: node(7),
			completed_at_hlc: hlc(2_000),
			migration_hlc_floor: hlc(1_000),
		};
		apply_request(&mut state, WendelonRequest::MigrationComplete(done));

		let descendant = state.migrations.get(&2).unwrap();
		let ids: Vec<u32> = descendant.source_replicas.iter().map(|n| n.id).collect();
		assert_eq!(
			ids,
			vec![7],
			"chained descendant must replace source list with [completed_by], \
             not retain superseded holders under the drain-all rule"
		);
		assert_eq!(descendant.upstream_migration_id, None);
	}

	/// Pure range-overlap (no `upstream_migration_id` link) should
	/// still prepend `completed_by` while preserving the existing
	/// holders — we only *hint* at a better source, we do not claim
	/// it subsumes the overlapping entry's full history.
	#[test]
	fn range_overlap_without_upstream_preserves_existing_sources() {
		let mut state = SmState::default();
		apply_request(
			&mut state,
			WendelonRequest::SeedMigration(SeedMigration {
				entry: entry(1, (0, 99), 2, 7, vec![1, 2], None),
			}),
		);
		// Overlapping range, but no upstream link — an independent
		// migration that happens to share bytes.
		apply_request(
			&mut state,
			WendelonRequest::SeedMigration(SeedMigration {
				entry: entry(2, (50, 150), 3, 8, vec![3, 4], None),
			}),
		);

		let done = MigrationComplete {
			migration_id: 1,
			range_start: 0,
			range_end: 99,
			to_epoch: 2,
			completed_by: node(7),
			completed_at_hlc: hlc(2_000),
			migration_hlc_floor: hlc(1_000),
		};
		apply_request(&mut state, WendelonRequest::MigrationComplete(done));

		let descendant = state.migrations.get(&2).unwrap();
		let ids: Vec<u32> = descendant.source_replicas.iter().map(|n| n.id).collect();
		assert_eq!(
			ids,
			vec![7, 3, 4],
			"overlap-only case must prepend completed_by, not replace"
		);
	}
}

#[cfg(test)]
mod tiered_slab_tests {
	use super::*;
	use crate::consensus::RetireTieredSlab;

	fn lsm_commit(visible_digest_byte: u8) -> CompactionCommit {
		let digest = crate::hash::state_digest::StateDigest::ZERO;
		let visible_post = if visible_digest_byte == 0 {
			digest
		} else {
			crate::hash::state_digest::StateDigest::new([visible_digest_byte; 32])
		};
		CompactionCommit {
			job_id: crate::schema::CompactionJobId::new(55),
			manifest_epoch_pre: crate::schema::ManifestEpoch::new(7),
			slabs_to_retire: vec![SlabId::new(1), SlabId::new(2)],
			slabs_to_add: vec![crate::storage::LsmRunSummary::new(
				SlabId::new(10),
				crate::schema::CompactionLevelIndex::L1,
				HybridTimestamp {
					physical_ms: 10,
					logical: 0,
					node_id: 1,
				},
				HybridTimestamp {
					physical_ms: 20,
					logical: 0,
					node_id: 1,
				},
				crate::schema::ByteCount::new(128),
				crate::schema::RowCount::new(2),
				crate::schema::ManifestEpoch::new(7),
			)],
			slabs_to_mark_delete_pending: Vec::new(),
			slabs_to_remove: Vec::new(),
			sidecar_swaps: Vec::new(),
			digest_summary: crate::storage::CompactionDigestSummary {
				visible_pre: digest,
				visible_post,
				version_entry_delta: digest,
			},
		}
	}

	#[test]
	fn retire_tiered_slab_removes_pointer_and_is_idempotent() {
		let mut state = SmState::default();
		let slab_id = SlabId::new(7);
		let tier = TieredSlab {
			slab_id,
			l2_uri: "memory://tier/slab_0000000000000007.wnd".into(),
			payload_hash: [0xAB; 32],
			is_encrypted: true,
			tenant_id: None,
			tenant_kek_id: None,
			stats_format_version: None,
			stats_block_len: None,
			stats_block_hash128: None,
			tombstone_metadata: TombstoneMetadata::None,
			supersession_metadata: SupersessionMetadata::None,
		};

		let (resp, event) = apply_request(&mut state, WendelonRequest::TierSlab(tier.clone()));
		assert_eq!(resp, WendelonResponse::Success);
		assert_eq!(event, Some(TierObserverEvent::Tiered(tier.clone())));
		assert_eq!(state.tiered_slabs.get(&slab_id), Some(&tier));

		let (resp, event) = apply_request(
			&mut state,
			WendelonRequest::RetireTieredSlab(RetireTieredSlab { slab_id }),
		);
		assert_eq!(resp, WendelonResponse::Success);
		assert_eq!(event, Some(TierObserverEvent::Retired(slab_id)));
		assert!(!state.tiered_slabs.contains_key(&slab_id));

		let (resp, event) = apply_request(
			&mut state,
			WendelonRequest::RetireTieredSlab(RetireTieredSlab { slab_id }),
		);
		assert_eq!(resp, WendelonResponse::Success);
		assert_eq!(event, None);
	}

	#[test]
	fn compaction_commit_request_emits_lsm_observer_event() {
		let mut state = SmState::default();
		let commit = lsm_commit(0);

		let (resp, event) = apply_request(
			&mut state,
			WendelonRequest::CompactionCommit(commit.clone()),
		);

		assert_eq!(resp, WendelonResponse::Success);
		assert_eq!(
			event,
			Some(TierObserverEvent::LsmCompactionCommitted(commit))
		);
	}

	#[test]
	fn compaction_commit_request_rejects_visible_digest_mismatch() {
		let mut state = SmState::default();

		let (resp, event) =
			apply_request(&mut state, WendelonRequest::CompactionCommit(lsm_commit(7)));

		assert!(
			matches!(resp, WendelonResponse::Error(error) if error.contains("visible-domain digest"))
		);
		assert_eq!(event, None);
	}
}

#[cfg(test)]
mod tenant_kek_tests {
	use super::*;
	use crate::crypto::{TenantKeyset, WrappedTenantKek};
	use crate::schema::{SlabId, TenantId, TenantKekId};

	fn seed_active_tenant(state: &mut SmState, tenant_id: TenantId) {
		state.tenants.insert(
			tenant_id,
			TenantRecord {
				tenant_id,
				state: TenantLifecycleState::Active,
				quota_bytes: u64::MAX,
				used_bytes: 0,
				replication_factor: 3,
			},
		);
	}

	fn seed_two_version_keyset(
		state: &mut SmState,
		tenant_id: TenantId,
		historical_id: TenantKekId,
		active_id: TenantKekId,
	) {
		let mut keyset = TenantKeyset::default();
		keyset.active_tenant_kek_id = Some(active_id);
		keyset.keys.insert(
			historical_id,
			WrappedTenantKek {
				wrapped_key: vec![1; 40],
			},
		);
		keyset.keys.insert(
			active_id,
			WrappedTenantKek {
				wrapped_key: vec![2; 40],
			},
		);
		state.tenant_keysets.insert(tenant_id, keyset);
	}

	#[test]
	fn rotate_tenant_kek_rejects_conflicting_duplicate_ids() {
		let mut state = SmState::default();
		seed_active_tenant(&mut state, TenantId::ZERO);
		let first = InstallTenantKekVersion {
			tenant_id: TenantId::ZERO,
			tenant_kek_id: TenantKekId::try_from(7).unwrap(),
			wrapped_tenant_kek: vec![1; 40],
		};
		let second = InstallTenantKekVersion {
			tenant_id: TenantId::ZERO,
			tenant_kek_id: TenantKekId::try_from(7).unwrap(),
			wrapped_tenant_kek: vec![2; 40],
		};

		let (resp, _) = apply_request(
			&mut state,
			WendelonRequest::InstallTenantKekVersion(first.clone()),
		);
		assert_eq!(resp, WendelonResponse::Success);

		let (resp, _) = apply_request(&mut state, WendelonRequest::InstallTenantKekVersion(second));
		match resp {
			WendelonResponse::Error(message) => {
				assert!(message.contains("tenant_kek_id 7"));
			}
			other => panic!("expected duplicate tenant KEK id rejection, got {other:?}"),
		}

		let keyset = state.tenant_keysets.get(&TenantId::ZERO).unwrap();
		assert_eq!(
			keyset.active_tenant_kek_id,
			Some(TenantKekId::try_from(7).unwrap())
		);
		assert_eq!(
			keyset
				.keys
				.get(&TenantKekId::try_from(7).unwrap())
				.unwrap()
				.wrapped_key,
			first.wrapped_tenant_kek
		);
	}

	#[test]
	fn create_tenant_preserves_preexisting_historical_keyset_versions() {
		let tenant_id = TenantId::ZERO;
		let historical_id = TenantKekId::try_from(6).unwrap();
		let active_id = TenantKekId::try_from(7).unwrap();
		let historical = WrappedTenantKek {
			wrapped_key: vec![1; 40],
		};
		let active = WrappedTenantKek {
			wrapped_key: vec![2; 40],
		};

		let mut state = SmState::default();
		let mut existing_keyset = TenantKeyset::default();
		existing_keyset.active_tenant_kek_id = Some(active_id);
		existing_keyset
			.keys
			.insert(historical_id, historical.clone());
		existing_keyset.keys.insert(active_id, active.clone());
		state.tenant_keysets.insert(tenant_id, existing_keyset);

		let mut request_keyset = TenantKeyset::default();
		request_keyset.active_tenant_kek_id = Some(active_id);
		request_keyset.keys.insert(active_id, active.clone());

		let (resp, _) = apply_request(
			&mut state,
			WendelonRequest::CreateTenant(CreateTenant {
				tenant_id,
				quota_bytes: 1024,
				replication_factor: 3,
				tenant_keyset: request_keyset,
			}),
		);
		assert_eq!(resp, WendelonResponse::Success);

		let keyset = state.tenant_keysets.get(&tenant_id).unwrap();
		assert_eq!(keyset.active_tenant_kek_id, Some(active_id));
		assert_eq!(keyset.keys.len(), 2);
		assert_eq!(keyset.keys.get(&historical_id), Some(&historical));
		assert_eq!(keyset.keys.get(&active_id), Some(&active));
	}

	#[test]
	fn retire_tenant_kek_rejects_active_version() {
		let tenant_id = TenantId::ZERO;
		let historical_id = TenantKekId::try_from(6).unwrap();
		let active_id = TenantKekId::try_from(7).unwrap();
		let mut state = SmState::default();
		seed_active_tenant(&mut state, tenant_id);
		seed_two_version_keyset(&mut state, tenant_id, historical_id, active_id);

		let (resp, _) = apply_request(
			&mut state,
			WendelonRequest::RetireTenantKekVersion(RetireTenantKekVersion {
				tenant_id,
				tenant_kek_id: active_id,
			}),
		);
		match resp {
			WendelonResponse::Error(message) => {
				assert!(message.contains("is active and cannot be retired"));
			}
			other => panic!("expected active tenant KEK rejection, got {other:?}"),
		}

		let keyset = state.tenant_keysets.get(&tenant_id).unwrap();
		assert!(keyset.keys.contains_key(&historical_id));
		assert!(keyset.keys.contains_key(&active_id));
	}

	#[test]
	fn retire_tenant_kek_rejects_referenced_historical_version() {
		let tenant_id = TenantId::ZERO;
		let historical_id = TenantKekId::try_from(6).unwrap();
		let active_id = TenantKekId::try_from(7).unwrap();
		let mut state = SmState::default();
		seed_active_tenant(&mut state, tenant_id);
		seed_two_version_keyset(&mut state, tenant_id, historical_id, active_id);
		state.tiered_slabs.insert(
			SlabId::new(41),
			TieredSlab {
				slab_id: SlabId::new(41),
				l2_uri: "s3://bucket/slab-41".to_string(),
				payload_hash: [9; 32],
				is_encrypted: true,
				tenant_id: Some(tenant_id),
				tenant_kek_id: Some(historical_id),
				stats_format_version: None,
				stats_block_len: None,
				stats_block_hash128: None,
				tombstone_metadata: TombstoneMetadata::None,
				supersession_metadata: SupersessionMetadata::None,
			},
		);

		let (resp, _) = apply_request(
			&mut state,
			WendelonRequest::RetireTenantKekVersion(RetireTenantKekVersion {
				tenant_id,
				tenant_kek_id: historical_id,
			}),
		);
		match resp {
			WendelonResponse::Error(message) => {
				assert!(message.contains("is still referenced by 1 tiered slabs"));
			}
			other => panic!("expected referenced tenant KEK rejection, got {other:?}"),
		}

		assert!(
			state
				.tenant_keysets
				.get(&tenant_id)
				.unwrap()
				.keys
				.contains_key(&historical_id)
		);
	}

	#[test]
	fn retire_tenant_kek_removes_unreferenced_historical_version() {
		let tenant_id = TenantId::ZERO;
		let historical_id = TenantKekId::try_from(6).unwrap();
		let active_id = TenantKekId::try_from(7).unwrap();
		let mut state = SmState::default();
		seed_active_tenant(&mut state, tenant_id);
		seed_two_version_keyset(&mut state, tenant_id, historical_id, active_id);

		let (resp, _) = apply_request(
			&mut state,
			WendelonRequest::RetireTenantKekVersion(RetireTenantKekVersion {
				tenant_id,
				tenant_kek_id: historical_id,
			}),
		);
		assert_eq!(resp, WendelonResponse::Success);

		let keyset = state.tenant_keysets.get(&tenant_id).unwrap();
		assert_eq!(keyset.active_tenant_kek_id, Some(active_id));
		assert!(!keyset.keys.contains_key(&historical_id));
		assert!(keyset.keys.contains_key(&active_id));
	}

	/// Rewrite an encoded snapshot's format version, leaving the body and its
	/// digest untouched. The artifact stays byte-valid; only its declared shape
	/// changes, which is exactly the condition the gate must judge.
	fn snapshot_bytes_with_format_version(state: &SmState, version: u8) -> Vec<u8> {
		let mut bytes = encode_snapshot(state).unwrap();
		let header_len = std::mem::size_of::<SnapshotHeader>();
		let mut header = *bytemuck::from_bytes::<SnapshotHeader>(&bytes[..header_len]);
		header.format_version = version;
		bytes[..header_len].copy_from_slice(bytemuck::bytes_of(&header));
		bytes
	}

	#[test]
	fn adr0085_wal_records_written_by_this_build_declare_their_format_version() {
		let checkpoint = serialize_checkpoint_page(7, 7, 7, 1);
		let header: WALRecordHeader =
			*bytemuck::from_bytes(&checkpoint.as_slice()[..std::mem::size_of::<WALRecordHeader>()]);
		assert_eq!(header.format_version, WAL_FORMAT_VERSION);
		assert_ne!(
			header.format_version, PRE_VERSIONING_FORMAT,
			"a build carrying the gate must not write records it would call unverifiable"
		);
	}

	#[test]
	fn adr0085_wal_header_keeps_its_pinned_size_after_carving_a_version() {
		// The version came out of reserved padding precisely so that every
		// reader's framing arithmetic stays put. If this ever moves, records
		// written either side of the change stop lining up.
		assert_eq!(std::mem::size_of::<WALRecordHeader>(), 32);
		assert_eq!(std::mem::size_of::<SnapshotHeader>(), 80);
	}

	#[test]
	fn adr0085_snapshot_written_by_this_build_declares_its_format_version() {
		let bytes = encode_snapshot(&SmState::default()).unwrap();
		let header: SnapshotHeader =
			*bytemuck::from_bytes(&bytes[..std::mem::size_of::<SnapshotHeader>()]);
		assert_eq!(header.format_version, SNAPSHOT_FORMAT_VERSION);
		assert_ne!(
			header.format_version, PRE_VERSIONING_FORMAT,
			"a build carrying the gate must not write artifacts it would call unverifiable"
		);
	}

	#[test]
	fn adr0085_snapshot_from_a_newer_build_is_refused_by_name() {
		let bytes = snapshot_bytes_with_format_version(&SmState::default(), 9);
		let error = decode_snapshot_reader(bytes.as_slice())
			.expect_err("a version this build does not know must be refused");
		assert!(
			error.contains('9'),
			"the refusal names the version: {error}"
		);
		assert!(
			error.contains("not corrupt"),
			"the refusal separates a shape mismatch from damage: {error}"
		);
		assert!(
			!error.contains("hash mismatch"),
			"an intact artifact must never be reported as damaged: {error}"
		);
	}

	#[test]
	fn adr0085_pre_versioning_snapshot_still_decodes_and_is_not_refused() {
		// The guarantee that keeps every existing profile starting: a
		// version-zero artifact whose shape happens to match is read as before.
		let state = SmState::default();
		let bytes = snapshot_bytes_with_format_version(&state, PRE_VERSIONING_FORMAT);
		let (header, decoded) = decode_snapshot_reader(bytes.as_slice())
			.expect("version zero is recognized, not refused");
		assert_eq!(header.format_version, PRE_VERSIONING_FORMAT);
		assert_eq!(decoded.last_applied_log_id, state.last_applied_log_id);
	}

	#[test]
	fn adr0092_v1_snapshot_of_the_exact_v1_shape_replays() {
		// This control's ancestor (adr0085_older_versioned_snapshot_shape_
		// mismatch_explains_itself_too) pinned the pre-replay behavior: an
		// intact v1 body was refused with "no migrations". ADR 0092 gave v1 a
		// replay decoder. The body is immutable stored evidence: after ADR 0093
		// inserted a field in the current spine, truncating the current encoder
		// could no longer construct any faithful older shape.
		assert!(
			SNAPSHOT_FORMAT_VERSION > 1,
			"this control needs a version below the current one to exist"
		);
		let manifest = include_str!("../../tests/fixtures/consensus_body/snapshot_v1.txt");
		let (_, encoded) = manifest.trim().split_once('=').unwrap();
		let body = (0..encoded.len())
			.step_by(2)
			.map(|index| u8::from_str_radix(&encoded[index..index + 2], 16).unwrap())
			.collect::<Vec<_>>();
		let header = SnapshotHeader {
			format_version: 1,
			state_sha256: control_artifact_digest_32(&body),
			..snapshot_header_template(&SmState::default())
		};
		let mut bytes = bytemuck::bytes_of(&header).to_vec();
		bytes.extend_from_slice(&body);

		let (header, decoded) = decode_snapshot_reader(bytes.as_slice())
			.expect("an exact v1 body must replay, not refuse");
		assert_eq!(header.format_version, 1);
		assert_eq!(
			decoded.ctas_sagas.len(),
			1,
			"the active v1 saga survives replay"
		);
		assert!(decoded.materialization_certificates.is_empty());
		assert!(decoded.table_drop_outcomes.is_empty());
	}

	#[test]
	fn adr0092_v1_stamped_snapshot_of_the_wrong_shape_is_refused_by_name() {
		// A CURRENT-shaped body stamped version 1: the v1 decode leaves the
		// certificate-catalog bytes unconsumed, so the artifact provably was
		// not written at its declared version. The refusal names the version
		// and stays out of the corruption vocabulary — the digest matched.
		let bytes = snapshot_bytes_with_format_version(&SmState::default(), 1);
		let error = decode_snapshot_reader(bytes.as_slice())
			.expect_err("a mis-stamped body must not decode");
		assert!(
			error.contains("snapshot format version 1"),
			"the refusal names the declared version: {error}"
		);
		assert!(
			error.contains("intact"),
			"the operator is told the bytes are sound: {error}"
		);
		assert!(
			error.contains("not corruption"),
			"a hash-matched recognized-version mismatch is never damage: {error}"
		);
		assert!(
			!error.contains("predates format versioning"),
			"a versioned artifact must not be described as pre-versioning: {error}"
		);
	}

	#[test]
	fn adr0085_pre_versioning_snapshot_shape_mismatch_names_the_format_not_damage() {
		// The incident's exact shape: digest matches, body is intact, and the
		// decoder runs off the end because the writer had different fields.
		let mut bytes =
			snapshot_bytes_with_format_version(&SmState::default(), PRE_VERSIONING_FORMAT);
		let header_len = std::mem::size_of::<SnapshotHeader>();
		bytes.truncate(bytes.len() - 1);
		let mut header = *bytemuck::from_bytes::<SnapshotHeader>(&bytes[..header_len]);
		header.state_sha256 = control_artifact_digest_32(&bytes[header_len..]);
		bytes[..header_len].copy_from_slice(bytemuck::bytes_of(&header));

		let error = decode_snapshot_reader(bytes.as_slice())
			.expect_err("a truncated-shape body must not decode");
		assert!(
			error.contains("predates format versioning"),
			"a pre-versioning shape failure explains itself: {error}"
		);
		assert!(
			error.contains("intact"),
			"the operator is told the bytes are sound: {error}"
		);
	}
}

/// ADR 0092: the consensus-body fixture corpus and its exact-set guards.
///
/// The WAL body is `postcard(Entry<WendelonTypeConfig>)` and the snapshot body
/// is `postcard(SmState)` — positional encodings that cannot describe
/// themselves. These controls pin the CURRENT snapshot (version 5) and WAL
/// (version 4) shapes two ways: a
/// stored golden encoding per request variant and per changed container, and
/// an exact-set guard over the variant lists, because an appended enum variant
/// moves no stored bytes and only the set check can see it. Any failure here
/// means the body shape moved: the re-pin travels with a
/// `WAL_FORMAT_VERSION`/`SNAPSHOT_FORMAT_VERSION` bump, a frozen fixture of
/// the outgoing version, and its replay control — never alone.
#[cfg(test)]
mod body_format_corpus {
	use super::*;
	use crate::consensus::{EpochPeer, RetireTieredSlab};
	use crate::crypto::WrappedTenantKek;
	use crate::hash::state_digest::StateDigest;
	use crate::integration::compute::{DropOperationId, SchemaGeneration};
	use crate::schema::{
		CompactionJobId, CtasExpectedBinding, CtasIntentFingerprint, CtasOperationId,
		CtasSagaMutation, DurabilityLevel, FinalizationMarker, Identity, LeaseDescriptor,
		LocalityPolicy, ManifestEpoch, PolicyGeneration, PrimaryKeyDescriptor,
		ReleaseLeaseRequestMeta, RowIdentityPolicy, SchemaEvolutionPolicy, TableObjectId,
		TableRetentionPolicy, TableSchemaDescriptor, TenantId,
	};
	use crate::storage::CompactionDigestSummary;
	use arrow::datatypes::{DataType, Field, Schema};
	use std::collections::BTreeMap;

	fn hts(physical_ms: u64) -> HybridTimestamp {
		HybridTimestamp {
			physical_ms,
			logical: 2,
			node_id: 1,
		}
	}

	fn node(id: u32) -> SchemaNodeId {
		SchemaNodeId { id, _reserved: 0 }
	}

	fn canonical_descriptor() -> TableSchemaDescriptor {
		let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
		TableSchemaDescriptor::new(
			TableObjectId::new([4; 16]),
			1,
			"analytics",
			"events",
			&schema,
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
			LocalityPolicy::default_table_spread(),
			SchemaEvolutionPolicy::AdditiveOnly,
		)
		.unwrap()
	}

	fn canonical_write_id() -> ClientWriteId {
		let mut client_uuid = [0x54u8; 16];
		client_uuid[6] = 0x70;
		client_uuid[8] = 0x80;
		ClientWriteId {
			client_uuid,
			sequence_no: 7,
			idempotency_ttl_ms: 300_000,
			_pad: 0,
		}
	}

	fn canonical_operation_id() -> CtasOperationId {
		let mut bytes = [0x54u8; 16];
		bytes[..5].fill(0);
		bytes[5] = 1;
		bytes[6] = 0x70;
		bytes[8] = 0x80;
		CtasOperationId::new(bytes)
	}

	fn canonical_epoch_config(epoch: u64) -> EpochConfig {
		let mut peers = BTreeMap::new();
		peers.insert(
			1,
			EpochPeer {
				node_id: node(1),
				address: "127.0.0.1:7001".to_string(),
				tier_bits: 1,
				role_bits: 1,
				capability_bits: 0,
			},
		);
		EpochConfig { epoch, peers }
	}

	fn canonical_ledger_entry() -> MigrationLedgerEntry {
		MigrationLedgerEntry {
			migration_id: 9,
			range_start: 0x1000,
			range_end: 0x1FFF,
			from_epoch: 1,
			to_epoch: 2,
			logical_owner: node(2),
			upstream_migration_id: None,
			source_replicas: vec![node(1)],
			migration_hlc_floor: hts(5),
			state: MigrationState::Pending,
			completed_by: None,
			completed_at_hlc: None,
		}
	}

	fn canonical_tenant_keyset() -> TenantKeyset {
		let tenant_kek_id = TenantKekId::try_from(1).unwrap();
		let mut keyset = TenantKeyset::default();
		keyset.active_tenant_kek_id = Some(tenant_kek_id);
		keyset.keys.insert(
			tenant_kek_id,
			WrappedTenantKek {
				wrapped_key: vec![0xAB; 40],
			},
		);
		keyset
	}

	fn canonical_mutations() -> Vec<(&'static str, CtasSagaMutation)> {
		let tenant_id = TenantId::new(7);
		let operation_id = canonical_operation_id();
		vec![
			(
				"provision",
				CtasSagaMutation::Provision {
					tenant_id,
					operation_id,
					intent_fingerprint: CtasIntentFingerprint::new([0x48; 32]),
					descriptor: canonical_descriptor(),
					expected_binding: CtasExpectedBinding::Absent,
					prior_descriptor: None,
					row_write_id: canonical_write_id(),
					owner_epoch: 1,
					observed_now_ms: 1,
					owner_lease_expires_at_ms: 1_000,
					certificate: Some(vec![0xC5; 4]),
				},
			),
			(
				"attest_rows_committed",
				CtasSagaMutation::AttestRowsCommitted {
					tenant_id,
					operation_id,
					owner_epoch: 1,
					row_count: 2,
				},
			),
			(
				"declare_chain",
				CtasSagaMutation::DeclareChain {
					tenant_id,
					operation_id,
					owner_epoch: 1,
					row_count: 2,
					chunk_count: 1,
				},
			),
			(
				"mark_row_dispatched",
				CtasSagaMutation::MarkRowDispatched {
					tenant_id,
					operation_id,
					owner_epoch: 1,
					row_count: 2,
				},
			),
			(
				"mark_chain_pins_released",
				CtasSagaMutation::MarkChainPinsReleased {
					tenant_id,
					operation_id,
					owner_epoch: 1,
				},
			),
			(
				"publish",
				CtasSagaMutation::Publish {
					tenant_id,
					operation_id,
					owner_epoch: 1,
				},
			),
			(
				"mark_retiring_old",
				CtasSagaMutation::MarkRetiringOld {
					tenant_id,
					operation_id,
					owner_epoch: 1,
				},
			),
			(
				"complete",
				CtasSagaMutation::Complete {
					tenant_id,
					operation_id,
					owner_epoch: 1,
					completed_at_ms: 9,
				},
			),
			(
				"reclaim",
				CtasSagaMutation::Reclaim {
					tenant_id,
					operation_id,
					owner_epoch: 1,
					cause: CtasTerminalCause::PublicationConflict,
					completed_at_ms: 9,
				},
			),
			(
				"record_publication_conflict",
				CtasSagaMutation::RecordPublicationConflict {
					tenant_id,
					operation_id,
					owner_epoch: 1,
					completed_at_ms: 9,
				},
			),
			(
				"record_retry_debt",
				CtasSagaMutation::RecordRetryDebt {
					tenant_id,
					operation_id,
					owner_epoch: 1,
				},
			),
			(
				"claim",
				CtasSagaMutation::Claim {
					tenant_id,
					operation_id,
					expected_owner_epoch: 1,
					new_owner_epoch: 2,
					observed_now_ms: 5,
					lease_expires_at_ms: 1_000,
				},
			),
		]
	}

	fn canonical_requests() -> Vec<(&'static str, WendelonRequest)> {
		let tenant_id = TenantId::new(7);
		vec![
			(
				"finalize",
				WendelonRequest::Finalize(TenantFinalization {
					marker: FinalizationMarker {
						log_index: 11,
						term: 3,
						epoch: 1,
						commit_hlc: hts(5),
						payload_hash: [0x11; 32],
						e_safe_index: 10,
						durability: DurabilityLevel(1),
						_pad: [0; 7],
					},
					tenant_id,
					key_identity: Identity::new([0x22; 32]),
					charged_bytes: 4_096,
					record_count: 2,
				}),
			),
			(
				"acquire_lease",
				WendelonRequest::AcquireLease(LeaseDescriptor {
					epoch: 1,
					holder: node(1),
					range_start: 0x1000,
					range_end: 0x1FFF,
					lease_start: hts(5),
					lease_expiry: hts(6),
					min_commit_index: 0,
					issuer: node(1),
					signature: [0x5A; 64],
				}),
			),
			(
				"release_lease",
				WendelonRequest::ReleaseLease(ReleaseLeaseRequestMeta {
					range_start: 0x1000,
					range_end: 0x1FFF,
					epoch: 1,
					holder: node(1),
				}),
			),
			(
				"advance_epoch",
				WendelonRequest::AdvanceEpoch(AdvanceEpochRecord {
					from: canonical_epoch_config(1),
					to: canonical_epoch_config(2),
				}),
			),
			(
				"schema_update",
				WendelonRequest::SchemaUpdate(vec![0x5C; 8]),
			),
			(
				"tier_slab",
				WendelonRequest::TierSlab(TieredSlab {
					slab_id: SlabId::new(3),
					l2_uri: "l2://slab/3".to_string(),
					payload_hash: [0x33; 32],
					is_encrypted: false,
					tenant_id: None,
					tenant_kek_id: None,
					stats_format_version: None,
					stats_block_len: None,
					stats_block_hash128: None,
					tombstone_metadata: TombstoneMetadata::default(),
					supersession_metadata: SupersessionMetadata::default(),
				}),
			),
			(
				"retire_tiered_slab",
				WendelonRequest::RetireTieredSlab(RetireTieredSlab {
					slab_id: SlabId::new(3),
				}),
			),
			(
				"compaction_commit",
				WendelonRequest::CompactionCommit(CompactionCommit {
					job_id: CompactionJobId::new(5),
					manifest_epoch_pre: ManifestEpoch::new(4),
					slabs_to_retire: vec![SlabId::new(3)],
					slabs_to_add: Vec::new(),
					slabs_to_mark_delete_pending: Vec::new(),
					slabs_to_remove: Vec::new(),
					sidecar_swaps: Vec::new(),
					digest_summary: CompactionDigestSummary {
						visible_pre: StateDigest::default(),
						visible_post: StateDigest::default(),
						version_entry_delta: StateDigest::default(),
					},
				}),
			),
			(
				"seed_migration",
				WendelonRequest::SeedMigration(SeedMigration {
					entry: canonical_ledger_entry(),
				}),
			),
			(
				"migration_complete",
				WendelonRequest::MigrationComplete(MigrationComplete {
					migration_id: 9,
					range_start: 0x1000,
					range_end: 0x1FFF,
					to_epoch: 2,
					completed_by: node(2),
					completed_at_hlc: hts(7),
					migration_hlc_floor: hts(5),
				}),
			),
			(
				"create_tenant",
				WendelonRequest::CreateTenant(CreateTenant {
					tenant_id,
					quota_bytes: u64::MAX,
					replication_factor: 1,
					tenant_keyset: canonical_tenant_keyset(),
				}),
			),
			(
				"update_tenant_quota",
				WendelonRequest::UpdateTenantQuota(UpdateTenantQuota {
					tenant_id,
					quota_bytes: 1 << 30,
				}),
			),
			("suspend_tenant", WendelonRequest::SuspendTenant(tenant_id)),
			("resume_tenant", WendelonRequest::ResumeTenant(tenant_id)),
			("delete_tenant", WendelonRequest::DeleteTenant(tenant_id)),
			(
				"install_tenant_kek_version",
				WendelonRequest::InstallTenantKekVersion(InstallTenantKekVersion {
					tenant_id,
					tenant_kek_id: TenantKekId::try_from(2).unwrap(),
					wrapped_tenant_kek: vec![0xAB; 40],
				}),
			),
			(
				"retire_tenant_kek_version",
				WendelonRequest::RetireTenantKekVersion(RetireTenantKekVersion {
					tenant_id,
					tenant_kek_id: TenantKekId::try_from(1).unwrap(),
				}),
			),
			(
				"revoke_actor_key",
				WendelonRequest::RevokeActorKey(RevokeActorKey {
					trust_domain: ActorTrustDomain::Data,
					key_id: vec![0x4B; 8],
					revoked_at_ms: 9,
				}),
			),
			(
				"set_table_retention_policy",
				WendelonRequest::SetTableRetentionPolicy(SetTableRetentionPolicy {
					tenant_id,
					table_object_id: TableObjectId::new([4; 16]),
					expected_policy_generation: PolicyGeneration::default(),
					new_policy: TableRetentionPolicy::default(),
					updated_at_hlc: hts(5),
				}),
			),
			(
				"table_drop_prepare",
				WendelonRequest::TableDropPrepare(TableDropPrepareRequest::V1 {
					operation_id: canonical_drop_operation_id(),
					expected_generation: SchemaGeneration::new(1),
				}),
			),
			(
				"table_reclaim_completion",
				WendelonRequest::TableReclaimCompletion(TableReclaimCompletionRequest::V1 {
					operation_id: canonical_drop_operation_id(),
					fence_log_index: 11,
				}),
			),
			(
				"table_drop_finalize",
				WendelonRequest::TableDropFinalize(TableDropFinalizeRequest::V1 {
					operation_id: canonical_drop_operation_id(),
					fence_log_index: 11,
					reclaim_completion_log_index: 12,
				}),
			),
			(
				"ctas_saga",
				WendelonRequest::CtasSaga(canonical_mutations().remove(0).1),
			),
			(
				"replay_legacy_ctas_row_dispatched",
				WendelonRequest::ReplayLegacyCtasRowDispatched(
					crate::schema::ReplayLegacyCtasRowDispatched {
						tenant_id,
						operation_id: canonical_operation_id(),
						owner_epoch: 1,
						row_count: 2,
					},
				),
			),
			(
				"table_drop_logical_finalize",
				WendelonRequest::TableDropLogicalFinalize(TableDropLogicalFinalizeRequest {
					operation_id: canonical_drop_operation_id(),
					fence_log_index: 11,
				}),
			),
			(
				"table_drop_settlement",
				WendelonRequest::TableDropSettlement(TableDropSettlementRequest {
					operation_id: canonical_drop_operation_id(),
					finalize_log_index: 12,
					ownership_epoch: 3,
					attested_participants: TableDropParticipantSet::new([node(1)]),
				}),
			),
		]
	}

	fn canonical_drop_operation_id() -> DropOperationId {
		DropOperationId {
			tenant_id: TenantId::new(7),
			table_object_id: TableObjectId::new([4; 16]),
			schema_generation: SchemaGeneration::new(1),
		}
	}

	fn entry_for(request: WendelonRequest) -> Entry<WendelonTypeConfig> {
		Entry {
			log_id: LogId::new(CommittedLeaderId::new(3, 1), 11),
			payload: EntryPayload::Normal(request),
		}
	}

	/// The canonical SmState carries an ACTIVE saga — a non-empty
	/// `CtasSagaCatalog` entry — because the certificate field this pin
	/// exists to watch sits per-entry, mid-stream inside the catalog map. An
	/// empty state would pin nothing that matters.
	fn canonical_sm_state() -> SmState {
		let mut state = SmState::default();
		state.last_e_safe_index = 10;
		let response = apply_ctas_saga_mutation(&mut state, canonical_mutations().remove(0).1);
		assert!(
			matches!(response, CtasSagaResponse::Applied(_)),
			"canonical provision must apply: {response:?}"
		);
		state
	}

	fn canonical_current_sm_state() -> SmState {
		let mut state = canonical_sm_state();
		let operation_id = DropOperationId {
			tenant_id: TenantId::new(7),
			table_object_id: TableObjectId::new([0xD3; 16]),
			schema_generation: SchemaGeneration::new(3),
		};
		state
			.table_drop_outcomes
			.insert_validated(TableDropOutcome {
				operation_id,
				finalize_log_index: 37,
			});
		state
			.table_drop_tombstones
			.insert_validated(TableDropTombstone {
				operation_id,
				fence_log_index: 36,
				finalize_log_index: 37,
			});
		state
			.table_drop_reclaim_debts
			.insert_validated(TableDropReclaimDebt {
				operation_id,
				fence_log_index: 36,
				finalize_log_index: 37,
				ownership_epoch: 3,
				participants: TableDropParticipantSet::new([node(1)]),
			});
		state
	}

	fn canonical_v4_sm_state() -> crate::consensus::snapshot_body::v4::SmState {
		crate::consensus::snapshot_body::v4::project(canonical_current_sm_state())
	}

	fn canonical_v3_sm_state() -> crate::consensus::snapshot_body::v3::SmState {
		let state = canonical_current_sm_state();
		crate::consensus::snapshot_body::v3::SmState {
			last_applied_log_id: state.last_applied_log_id,
			last_membership: state.last_membership,
			epoch_history: state.epoch_history,
			leases: state.leases,
			e_safe_watermarks: state.e_safe_watermarks,
			last_e_safe_index: state.last_e_safe_index,
			tiered_slabs: state.tiered_slabs,
			migrations: state.migrations,
			tombstones: state.tombstones,
			tenants: state.tenants,
			tenant_keysets: state.tenant_keysets,
			revoked_actor_keys: state.revoked_actor_keys,
			table_schemas: state.table_schemas,
			logical_types: state.logical_types,
			secondary_indexes: state.secondary_indexes,
			table_lifecycle: state.table_lifecycle,
			table_drop_fences: state.table_drop_fences,
			table_drop_outcomes: state.table_drop_outcomes,
			ctas_sagas: crate::consensus::snapshot_body::pre_v4_ctas::project(state.ctas_sagas),
			materialization_certificates: state.materialization_certificates,
		}
	}

	fn canonical_v2_sm_state() -> crate::consensus::snapshot_body::v2::SmState {
		let state = canonical_sm_state();
		crate::consensus::snapshot_body::v2::SmState {
			last_applied_log_id: state.last_applied_log_id,
			last_membership: state.last_membership,
			epoch_history: state.epoch_history,
			leases: state.leases,
			e_safe_watermarks: state.e_safe_watermarks,
			last_e_safe_index: state.last_e_safe_index,
			tiered_slabs: state.tiered_slabs,
			migrations: state.migrations,
			tombstones: state.tombstones,
			tenants: state.tenants,
			tenant_keysets: state.tenant_keysets,
			revoked_actor_keys: state.revoked_actor_keys,
			table_schemas: state.table_schemas,
			logical_types: state.logical_types,
			secondary_indexes: state.secondary_indexes,
			table_lifecycle: state.table_lifecycle,
			table_drop_fences: state.table_drop_fences,
			ctas_sagas: crate::consensus::snapshot_body::pre_v4_ctas::project(state.ctas_sagas),
			materialization_certificates: state.materialization_certificates,
		}
	}

	fn parse_manifest(raw: &str) -> BTreeMap<String, Vec<u8>> {
		raw.lines()
			.filter(|line| !line.trim().is_empty() && !line.starts_with('#'))
			.map(|line| {
				let (name, hex) = line
					.split_once('=')
					.unwrap_or_else(|| panic!("manifest line without '=': {line}"));
				let bytes = (0..hex.len())
					.step_by(2)
					.map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
					.collect();
				(name.to_string(), bytes)
			})
			.collect()
	}

	fn hex(bytes: &[u8]) -> String {
		bytes.iter().map(|b| format!("{b:02x}")).collect()
	}

	const WAL_MANIFEST: &str =
		include_str!("../../tests/fixtures/consensus_body/wal_v2_entries.txt");
	const MUTATION_MANIFEST: &str =
		include_str!("../../tests/fixtures/consensus_body/ctas_mutations_v2.txt");
	const SNAPSHOT_V2_MANIFEST: &str =
		include_str!("../../tests/fixtures/consensus_body/snapshot_v2.txt");
	const SNAPSHOT_V3_MANIFEST: &str =
		include_str!("../../tests/fixtures/consensus_body/snapshot_v3.txt");
	const WAL_V3_MANIFEST: &str =
		include_str!("../../tests/fixtures/consensus_body/wal_v3_entries.txt");
	const MUTATION_V3_MANIFEST: &str =
		include_str!("../../tests/fixtures/consensus_body/ctas_mutations_v3.txt");
	const SNAPSHOT_V4_MANIFEST: &str =
		include_str!("../../tests/fixtures/consensus_body/snapshot_v4.txt");
	const WAL_V4_MANIFEST: &str =
		include_str!("../../tests/fixtures/consensus_body/wal_v4_entries.txt");
	const SNAPSHOT_V5_MANIFEST: &str =
		include_str!("../../tests/fixtures/consensus_body/snapshot_v5.txt");

	fn wal_corpus() -> Vec<(&'static str, Vec<u8>)> {
		let mut corpus: Vec<(&'static str, Vec<u8>)> = canonical_requests()
			.into_iter()
			.map(|(name, request)| (name, postcard::to_stdvec(&entry_for(request)).unwrap()))
			.collect();
		corpus.push((
			"membership",
			postcard::to_stdvec(&Entry::<WendelonTypeConfig> {
				log_id: LogId::new(CommittedLeaderId::new(3, 1), 11),
				payload: EntryPayload::Membership(openraft::Membership::new(
					vec![std::collections::BTreeSet::from([1u32])],
					BTreeMap::from([(1u32, openraft::BasicNode::new("127.0.0.1:7001"))]),
				)),
			})
			.unwrap(),
		));
		corpus.push((
			"blank",
			postcard::to_stdvec(&Entry::<WendelonTypeConfig> {
				log_id: LogId::new(CommittedLeaderId::new(3, 1), 11),
				payload: EntryPayload::Blank,
			})
			.unwrap(),
		));
		corpus
	}

	#[test]
	fn adr0092_wal_corpus_covers_every_request_variant_exactly() {
		let names: Vec<&str> = canonical_requests().iter().map(|(name, _)| *name).collect();
		assert_eq!(
			names,
			WendelonRequest::ALL_VARIANT_NAMES.to_vec(),
			"the corpus and the variant list must agree, in order"
		);
		let by_variant_name: Vec<&str> = canonical_requests()
			.iter()
			.map(|(_, request)| request.variant_name())
			.collect();
		assert_eq!(
			by_variant_name, names,
			"each canonical body must actually be its named variant"
		);
	}

	#[test]
	fn adr0092_mutation_corpus_covers_every_variant_exactly() {
		let names: Vec<&str> = canonical_mutations()
			.iter()
			.map(|(name, _)| *name)
			.collect();
		assert_eq!(names, CtasSagaMutation::ALL_VARIANT_NAMES.to_vec());
		let by_variant_name: Vec<&str> = canonical_mutations()
			.iter()
			.map(|(_, mutation)| mutation.variant_name())
			.collect();
		assert_eq!(by_variant_name, names);
	}

	#[test]
	fn adr0092_wal_v2_encodings_match_stored_goldens() {
		let stored = parse_manifest(WAL_MANIFEST);
		let corpus = wal_corpus()
			.into_iter()
			.filter(|(name, _)| {
				!matches!(
					*name,
					"replay_legacy_ctas_row_dispatched"
						| "table_drop_logical_finalize"
						| "table_drop_settlement"
				)
			})
			.collect::<Vec<_>>();
		assert_eq!(
			stored.keys().cloned().collect::<Vec<_>>(),
			{
				let mut names: Vec<String> =
					corpus.iter().map(|(name, _)| name.to_string()).collect();
				names.sort();
				names
			},
			"stored manifest and corpus must hold exactly the same set"
		);
		for (name, bytes) in &corpus {
			let golden = &stored[*name];
			assert_eq!(
				&hex(bytes),
				&hex(golden),
				"WAL body encoding moved for `{name}`; a shape change must ride a \
				 WAL_FORMAT_VERSION bump with a frozen fixture of the outgoing version"
			);
			let decoded: Entry<WendelonTypeConfig> = postcard::from_bytes(golden).unwrap();
			assert_eq!(
				hex(&postcard::to_stdvec(&decoded).unwrap()),
				hex(golden),
				"stored golden must round-trip byte-stable for `{name}`"
			);
		}
	}

	#[test]
	fn adr0092_mutation_v2_encodings_match_stored_goldens() {
		let stored = parse_manifest(MUTATION_MANIFEST);
		let corpus: Vec<(&'static str, Vec<u8>)> = canonical_mutations()
			.into_iter()
			.filter_map(|(name, mutation)| {
				crate::consensus::wal_body::v2::project_mutation(mutation)
					.map(|mutation| (name, postcard::to_stdvec(&mutation).unwrap()))
			})
			.collect();
		assert_eq!(stored.keys().cloned().collect::<Vec<_>>(), {
			let mut names: Vec<String> = corpus.iter().map(|(name, _)| name.to_string()).collect();
			names.sort();
			names
		},);
		for (name, bytes) in &corpus {
			assert_eq!(
				&hex(bytes),
				&hex(&stored[*name]),
				"CTAS mutation encoding moved for `{name}`"
			);
		}
	}

	#[test]
	fn adr0094_frozen_wal_v3_and_mutation_corpora_match_goldens() {
		let stored_wal = parse_manifest(WAL_V3_MANIFEST);
		let corpus = wal_corpus()
			.into_iter()
			.filter(|(name, _)| {
				!matches!(
					*name,
					"table_drop_logical_finalize" | "table_drop_settlement"
				)
			})
			.collect::<Vec<_>>();
		assert_eq!(stored_wal.len(), corpus.len());
		for (name, bytes) in corpus {
			assert_eq!(
				hex(&bytes),
				hex(&stored_wal[name]),
				"WAL v3 body moved for {name}"
			);
		}
		let stored_mutations = parse_manifest(MUTATION_V3_MANIFEST);
		let mutations = canonical_mutations();
		assert_eq!(stored_mutations.len(), mutations.len());
		for (name, mutation) in mutations {
			assert_eq!(
				hex(&postcard::to_stdvec(&mutation).unwrap()),
				hex(&stored_mutations[name]),
				"CTAS mutation v3 body moved for {name}"
			);
		}
	}

	#[test]
	fn adr0094_frozen_snapshot_v4_matches_stored_golden() {
		let stored = parse_manifest(SNAPSHOT_V4_MANIFEST);
		let state = canonical_v4_sm_state();
		assert_eq!(
			hex(&postcard::to_stdvec(&state).unwrap()),
			hex(&stored["smstate_active_saga_and_drop_outcome"])
		);
		let entry = state
			.ctas_sagas
			.get(TenantId::new(7), canonical_operation_id())
			.unwrap();
		assert_eq!(
			hex(&postcard::to_stdvec(entry).unwrap()),
			hex(&stored["ctas_saga_entry"])
		);
	}

	#[test]
	fn adr0095_current_wal_v4_and_snapshot_v5_match_stored_goldens() {
		let stored_wal = parse_manifest(WAL_V4_MANIFEST);
		let corpus = wal_corpus();
		assert_eq!(stored_wal.len(), corpus.len());
		for (name, bytes) in corpus {
			assert_eq!(
				hex(&bytes),
				hex(&stored_wal[name]),
				"WAL v4 body moved for {name}; bump and freeze the outgoing spine"
			);
		}

		let stored_snapshot = parse_manifest(SNAPSHOT_V5_MANIFEST);
		let state = canonical_current_sm_state();
		assert_eq!(
			hex(&postcard::to_stdvec(&state).unwrap()),
			hex(&stored_snapshot["smstate_active_saga_drop_outcome_and_reclaim_debt"])
		);
		assert_eq!(state.table_drop_tombstones.len(), 1);
		assert_eq!(state.table_drop_reclaim_debts.len(), 1);
	}

	#[test]
	fn adr0093_snapshot_v2_fixture_is_frozen_and_upconverts_without_outcomes() {
		let stored = parse_manifest(SNAPSHOT_V2_MANIFEST);
		let state_bytes = postcard::to_stdvec(&canonical_v2_sm_state()).unwrap();
		assert_eq!(
			hex(&state_bytes),
			hex(&stored["smstate_active_saga"]),
			"the immutable v2 snapshot fixture must stay on the frozen v2 spine"
		);
		let converted =
			crate::consensus::snapshot_body::v2::decode_state(&stored["smstate_active_saga"])
				.unwrap();
		assert!(
			converted.table_drop_outcomes.is_empty(),
			"pre-v3 snapshots upconvert with no fabricated DROP outcomes"
		);
		assert_eq!(
			hex(&postcard::to_stdvec(&converted).unwrap()),
			hex(&postcard::to_stdvec(&canonical_sm_state()).unwrap()),
			"v2 upconversion changes only the addition of an empty outcome catalog"
		);
	}

	#[test]
	fn adr0093_snapshot_v3_active_saga_and_drop_outcome_match_stored_golden() {
		let stored = parse_manifest(SNAPSHOT_V3_MANIFEST);
		let state = canonical_v3_sm_state();
		let state_bytes = postcard::to_stdvec(&state).unwrap();
		assert_eq!(
			hex(&state_bytes),
			hex(&stored["smstate_active_saga_and_drop_outcome"]),
			"current SmState encoding moved; a shape change must ride a \
			 SNAPSHOT_FORMAT_VERSION bump with a frozen fixture of the outgoing version"
		);
		let entry = state
			.ctas_sagas
			.entries
			.get(&(TenantId::new(7), canonical_operation_id()))
			.expect("canonical state must hold its active saga")
			.clone();
		assert_eq!(
			hex(&postcard::to_stdvec(&entry).unwrap()),
			hex(&stored["ctas_saga_entry"]),
			"CtasSagaEntry encoding moved"
		);
		let decoded: crate::consensus::snapshot_body::v3::SmState =
			postcard::from_bytes(&stored["smstate_active_saga_and_drop_outcome"]).unwrap();
		assert_eq!(
			hex(&postcard::to_stdvec(&decoded).unwrap()),
			hex(&stored["smstate_active_saga_and_drop_outcome"]),
			"stored snapshot golden must round-trip byte-stable"
		);
		assert_eq!(decoded.table_drop_outcomes.len(), 1);
		let encoded_snapshot = encode_snapshot(&canonical_current_sm_state()).unwrap();
		let (header, round_tripped) = decode_snapshot_reader(encoded_snapshot.as_slice()).unwrap();
		assert_eq!(header.format_version, SNAPSHOT_FORMAT_VERSION);
		assert_eq!(
			round_tripped
				.table_drop_outcomes
				.get(TenantId::new(7), TableObjectId::new([0xD3; 16])),
			state
				.table_drop_outcomes
				.get(TenantId::new(7), TableObjectId::new([0xD3; 16])),
			"the full snapshot envelope carries retained DROP outcomes"
		);
	}

	// ---- ADR 0092: the frozen v1 fixtures and their replay controls ----

	const V1_WAL_MANIFEST: &str =
		include_str!("../../tests/fixtures/consensus_body/wal_v1_entries.txt");
	const V1_SNAPSHOT_MANIFEST: &str =
		include_str!("../../tests/fixtures/consensus_body/snapshot_v1.txt");

	/// The canonical v1 `Provision` request, built from the frozen shadow
	/// spine. Field values mirror `canonical_mutations()`'s provision so the
	/// upconversion equality below is byte-exact.
	fn v1_provision_request(operation_byte: u8) -> crate::consensus::wal_body::v1::WendelonRequest {
		v1_provision_request_for(operation_byte, canonical_descriptor())
	}

	fn v1_provision_request_for(
		operation_byte: u8,
		descriptor: TableSchemaDescriptor,
	) -> crate::consensus::wal_body::v1::WendelonRequest {
		let mut operation_bytes = [operation_byte; 16];
		operation_bytes[..5].fill(0);
		operation_bytes[5] = 1;
		operation_bytes[6] = 0x70;
		operation_bytes[8] = 0x80;
		crate::consensus::wal_body::v1::WendelonRequest::CtasSaga(
			crate::consensus::wal_body::v1::CtasSagaMutation::Provision {
				tenant_id: TenantId::new(7),
				operation_id: CtasOperationId::new(operation_bytes),
				intent_fingerprint: CtasIntentFingerprint::new([0x48; 32]),
				descriptor,
				expected_binding: CtasExpectedBinding::Absent,
				prior_descriptor: None,
				row_write_id: canonical_write_id(),
				owner_epoch: 1,
				observed_now_ms: 1,
				owner_lease_expires_at_ms: 1_000,
			},
		)
	}

	/// A second table identity, so the mixed snapshot+tail control's tail
	/// provision does not collide with the snapshot's saga binding.
	fn tail_descriptor() -> TableSchemaDescriptor {
		let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
		TableSchemaDescriptor::new(
			TableObjectId::new([6; 16]),
			1,
			"analytics",
			"events_tail",
			&schema,
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
			LocalityPolicy::default_table_spread(),
			SchemaEvolutionPolicy::AdditiveOnly,
		)
		.unwrap()
	}

	fn v1_entry_bytes(request: crate::consensus::wal_body::v1::WendelonRequest) -> Vec<u8> {
		postcard::to_stdvec(&openraft::Entry::<
			crate::consensus::wal_body::v1::WendelonWalV1TypeConfig,
		> {
			log_id: LogId::new(CommittedLeaderId::new(3, 1), 11),
			payload: EntryPayload::Normal(request),
		})
		.unwrap()
	}

	/// The current-shape provision with `certificate: None` — what the v1
	/// fixture must upconvert to, and whose encoding must be the v1 bytes
	/// plus exactly the trailing `None` discriminant.
	fn v2_provision_none_entry(operation_byte: u8) -> Entry<WendelonTypeConfig> {
		let mut operation_bytes = [operation_byte; 16];
		operation_bytes[..5].fill(0);
		operation_bytes[5] = 1;
		operation_bytes[6] = 0x70;
		operation_bytes[8] = 0x80;
		entry_for(WendelonRequest::CtasSaga(CtasSagaMutation::Provision {
			tenant_id: TenantId::new(7),
			operation_id: CtasOperationId::new(operation_bytes),
			intent_fingerprint: CtasIntentFingerprint::new([0x48; 32]),
			descriptor: canonical_descriptor(),
			expected_binding: CtasExpectedBinding::Absent,
			prior_descriptor: None,
			row_write_id: canonical_write_id(),
			owner_epoch: 1,
			observed_now_ms: 1,
			owner_lease_expires_at_ms: 1_000,
			certificate: None,
		}))
	}

	/// The canonical v1 snapshot, projected FROM the live apply path so it is
	/// faithful to what a real v1 writer held: the provision apply also
	/// registers the provisional descriptor in `table_schemas`, and a
	/// hand-built state missing that would pin a state no v1 build ever
	/// wrote. Certificates are `None` throughout, so dropping the field is
	/// the exact v1 projection.
	fn v1_snapshot_state() -> crate::consensus::snapshot_body::v1::SmState {
		let state = expected_converted_state();
		let (entries, expired_through_ms) = state.ctas_sagas.into_replay_parts();
		crate::consensus::snapshot_body::v1::SmState {
			last_applied_log_id: state.last_applied_log_id,
			last_membership: state.last_membership,
			epoch_history: state.epoch_history,
			leases: state.leases,
			e_safe_watermarks: state.e_safe_watermarks,
			last_e_safe_index: state.last_e_safe_index,
			tiered_slabs: state.tiered_slabs,
			migrations: state.migrations,
			tombstones: state.tombstones,
			tenants: state.tenants,
			tenant_keysets: state.tenant_keysets,
			revoked_actor_keys: state.revoked_actor_keys,
			table_schemas: state.table_schemas,
			logical_types: state.logical_types,
			secondary_indexes: state.secondary_indexes,
			table_lifecycle: state.table_lifecycle,
			table_drop_fences: state.table_drop_fences,
			ctas_sagas: crate::consensus::snapshot_body::v1::CtasSagaCatalog {
				entries: entries
					.into_iter()
					.map(|(key, entry)| {
						assert_eq!(
							entry.certificate, None,
							"the v1 projection only exists for certificate-less entries"
						);
						(
							key,
							crate::consensus::snapshot_body::v1::CtasSagaEntry {
								tenant_id: entry.tenant_id,
								operation_id: entry.operation_id,
								intent_fingerprint: entry.intent_fingerprint,
								descriptor: entry.descriptor,
								expected_binding: entry.expected_binding,
								prior_descriptor: entry.prior_descriptor,
								row_write_id: entry.row_write_id,
								row_count: entry.row_count,
								phase: entry.phase,
								terminal_cause: entry.terminal_cause,
								owner_epoch: entry.owner_epoch,
								owner_lease_expires_at_ms: entry.owner_lease_expires_at_ms,
								retry_debt: entry.retry_debt,
								terminal_at_ms: entry.terminal_at_ms,
							},
						)
					})
					.collect(),
				expired_through_ms,
			},
		}
	}

	/// What the v1 snapshot must convert to: the same state built through the
	/// live apply path with `certificate: None`.
	fn expected_converted_state() -> SmState {
		let mut operation_bytes = [0x55u8; 16];
		operation_bytes[..5].fill(0);
		operation_bytes[5] = 1;
		operation_bytes[6] = 0x70;
		operation_bytes[8] = 0x80;
		let mut state = SmState::default();
		state.last_e_safe_index = 10;
		let response = apply_ctas_saga_mutation(
			&mut state,
			CtasSagaMutation::Provision {
				tenant_id: TenantId::new(7),
				operation_id: CtasOperationId::new(operation_bytes),
				intent_fingerprint: CtasIntentFingerprint::new([0x48; 32]),
				descriptor: canonical_descriptor(),
				expected_binding: CtasExpectedBinding::Absent,
				prior_descriptor: None,
				row_write_id: canonical_write_id(),
				owner_epoch: 1,
				observed_now_ms: 1,
				owner_lease_expires_at_ms: 1_000,
				certificate: None,
			},
		);
		assert!(matches!(response, CtasSagaResponse::Applied(_)));
		state
	}

	#[test]
	fn adr0092_v1_wal_fixture_decodes_and_upconverts_from_stored_bytes() {
		let stored = parse_manifest(V1_WAL_MANIFEST);
		let provision = &stored["ctas_saga_provision"];
		// The frozen bytes tie back to the grounded reality: a v1 provision
		// body is the current certificate-less encoding minus exactly its
		// trailing None discriminant.
		let v2_none = postcard::to_stdvec(&v2_provision_none_entry(0x54)).unwrap();
		assert_eq!(v2_none.last(), Some(&0u8));
		assert_eq!(
			hex(provision),
			hex(&v2_none[..v2_none.len() - 1]),
			"the stored v1 provision must be the certificate-less current \
			 encoding minus its trailing None byte"
		);
		let decoded = crate::consensus::wal_body::v1::decode_entry(provision).unwrap();
		assert_eq!(
			hex(&postcard::to_stdvec(&decoded).unwrap()),
			hex(&v2_none),
			"upconversion must land exactly on the current certificate-less shape"
		);
		// An unchanged-shape variant must pass through the v1 decode
		// untouched: its v1 bytes are its v2 bytes.
		let lease = &stored["acquire_lease"];
		let v2_lease = wal_corpus()
			.into_iter()
			.find(|(name, _)| *name == "acquire_lease")
			.unwrap()
			.1;
		assert_eq!(hex(lease), hex(&v2_lease));
		let decoded = crate::consensus::wal_body::v1::decode_entry(lease).unwrap();
		assert_eq!(hex(&postcard::to_stdvec(&decoded).unwrap()), hex(&v2_lease));
	}

	#[test]
	fn adr0092_v1_wal_fixture_still_fails_the_current_shape_decode() {
		// The discriminating run: v1 readability comes from the dispatch, not
		// from a widened tolerance in the current decoder.
		let stored = parse_manifest(V1_WAL_MANIFEST);
		let provision = &stored["ctas_saga_provision"];
		let err = postcard::from_bytes::<Entry<WendelonTypeConfig>>(provision)
			.expect_err("the current shape must not decode a v1 provision body");
		assert!(err.to_string().contains("end of buffer"), "{err}");
	}

	#[test]
	fn adr0092_v1_snapshot_fixture_converts_the_active_saga() {
		let stored = parse_manifest(V1_SNAPSHOT_MANIFEST);
		let body = &stored["smstate_active_saga"];
		// Stored bytes match the frozen shadow encoding.
		assert_eq!(
			hex(body),
			hex(&postcard::to_stdvec(&v1_snapshot_state()).unwrap()),
			"the stored v1 snapshot must be the frozen shadow's encoding"
		);
		let converted = crate::consensus::snapshot_body::v1::decode_state(body).unwrap();
		let entry = converted
			.ctas_sagas
			.get(TenantId::new(7), {
				let mut operation_bytes = [0x55u8; 16];
				operation_bytes[..5].fill(0);
				operation_bytes[5] = 1;
				operation_bytes[6] = 0x70;
				operation_bytes[8] = 0x80;
				CtasOperationId::new(operation_bytes)
			})
			.expect("the active saga survives conversion");
		assert_eq!(entry.certificate, None, "conversion adds certificate: None");
		assert_eq!(
			hex(&postcard::to_stdvec(&converted).unwrap()),
			hex(&postcard::to_stdvec(&expected_converted_state()).unwrap()),
			"conversion must land exactly on the live apply path's state"
		);
	}

	/// Regeneration procedure for the v1 fixtures: unlike the current-version
	/// corpus these are IMMUTABLE — regenerate only to create them, never to
	/// repair a drift; a drift here means a referenced type moved under the
	/// frozen spine and the shadow must fork its own copy instead.
	#[test]
	#[ignore = "generator: one-time creation of the immutable v1 fixtures"]
	fn adr0092_generate_v1_fixtures_once() {
		let dir = std::env::temp_dir().join("wendelon_body_corpus");
		std::fs::create_dir_all(&dir).unwrap();
		let wal = format!(
			"ctas_saga_provision={}\nctas_saga_provision_tail_table={}\nacquire_lease={}\n",
			hex(&v1_entry_bytes(v1_provision_request(0x54))),
			hex(&v1_entry_bytes(v1_provision_request_for(
				0x54,
				tail_descriptor()
			))),
			hex(&v1_entry_bytes(
				crate::consensus::wal_body::v1::WendelonRequest::AcquireLease(LeaseDescriptor {
					epoch: 1,
					holder: node(1),
					range_start: 0x1000,
					range_end: 0x1FFF,
					lease_start: hts(5),
					lease_expiry: hts(6),
					min_commit_index: 0,
					issuer: node(1),
					signature: [0x5A; 64],
				})
			)),
		);
		std::fs::write(dir.join("wal_v1_entries.txt"), wal).unwrap();
		let snapshot = format!(
			"smstate_active_saga={}\n",
			hex(&postcard::to_stdvec(&v1_snapshot_state()).unwrap())
		);
		std::fs::write(dir.join("snapshot_v1.txt"), snapshot).unwrap();
		println!("v1 fixtures written to {}", dir.display());
	}

	/// Regeneration procedure, run only inside a format-version bump cut:
	/// `cargo test ... adr0092_regenerate_body_corpus_manifests -- --ignored
	/// --nocapture`, then copy the printed files into
	/// `tests/fixtures/consensus_body/` and re-run the corpus controls.
	#[test]
	#[ignore = "generator: writes fresh manifests for a format-version bump cut"]
	fn adr0092_regenerate_body_corpus_manifests() {
		let dir = std::env::temp_dir().join("wendelon_body_corpus");
		std::fs::create_dir_all(&dir).unwrap();
		let mut wal = String::new();
		for (name, bytes) in wal_corpus() {
			wal.push_str(&format!("{name}={}\n", hex(&bytes)));
		}
		std::fs::write(dir.join("wal_v4_entries.txt"), wal).unwrap();
		let mut mutations = String::new();
		for (name, mutation) in canonical_mutations() {
			mutations.push_str(&format!(
				"{name}={}\n",
				hex(&postcard::to_stdvec(&mutation).unwrap())
			));
		}
		std::fs::write(dir.join("ctas_mutations_v3.txt"), mutations).unwrap();
		let state = canonical_current_sm_state();
		let entry = state
			.ctas_sagas
			.get(TenantId::new(7), canonical_operation_id())
			.unwrap()
			.clone();
		let snapshot = format!(
			"smstate_active_saga_drop_outcome_and_reclaim_debt={}\nctas_saga_entry={}\n",
			hex(&postcard::to_stdvec(&state).unwrap()),
			hex(&postcard::to_stdvec(&entry).unwrap())
		);
		std::fs::write(dir.join("snapshot_v5.txt"), snapshot).unwrap();
		println!("manifests written to {}", dir.display());
	}
}
