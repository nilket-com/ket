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
use crate::storage::ownership::DataDirectoryOwner;
use crate::storage::{
	AsyncFileOps, BlockingFileOps, CompactionCommit, FileHandle, IoUringFileOps, OpenFlags,
	fsync_parent_dir, is_cdc_locality_heartbeat_finalization,
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

// Offline, read-only scan of a node's retained Control log over the same replay readers.
mod read_only;
#[cfg(test)]
pub(crate) use read_only::fixture as read_only_fixture;
pub(crate) use read_only::scan_control_log_read_only;

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

impl ControlRingBackupImage {
	/// Identity of the exact input bytes, independent of decoded map order.
	pub(crate) fn restore_artifact_hash(&self) -> [u8; 32] {
		restore_artifact_hash(
			control_artifact_digest_32(&self.wal),
			self.wal.len() as u64,
			control_artifact_digest_32(&self.snapshot),
			self.snapshot.len() as u64,
		)
	}
}

fn restore_artifact_hash(
	wal: [u8; 32],
	wal_len: u64,
	snapshot: [u8; 32],
	snapshot_len: u64,
) -> [u8; 32] {
	let mut hash = control_artifact_hash::Context::new();
	hash.update(b"wendelon-control-restore-v1\0");
	hash.update(&wal_len.to_le_bytes());
	hash.update(&wal);
	hash.update(&snapshot_len.to_le_bytes());
	hash.update(&snapshot);
	hash.finish()
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
	artifact_hash: [u8; 32],
	artifact_bytes: u64,
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
	artifact_hasher: Option<control_artifact_hash::Context>,
	artifact_bytes: u64,
	entries: BTreeMap<u64, Entry<WendelonTypeConfig>>,
	entry_positions: BTreeMap<u64, WalRecordPointer>,
	last_log_id: Option<LogId<u32>>,
	last_purged: Option<LogId<u32>>,
	checkpoint: Option<RecoveryCheckpoint>,
	wal_segments: Vec<WalSegmentMeta>,
	last_segment_needs_truncate: bool,
	/// The first retained entry was read from the legacy single-file WAL.
	legacy_start: bool,
	/// Each segment holding no log entry, by path. Read only by open-time completion.
	checkpoint_only_holders: BTreeMap<PathBuf, CheckpointOnlyHolder>,
}

/// A segment with no log entry: how many checkpoint records it holds, and the exact bytes of its
/// last complete record, validated by replay (header, version, checksum, body shape).
#[derive(Clone, Debug, PartialEq, Eq)]
struct CheckpointOnlyHolder {
	records: usize,
	last_record: Vec<u8>,
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
	/// Set after any WAL mutation fails once it may have had an on-disk effect. While set, every
	/// WAL mutator refuses without I/O; only a reopen, which rebuilds state from disk, clears it.
	poisoned: Option<WalPoison>,
	/// Physical positions of purged entries whose segments are still on disk because no durable
	/// snapshot covers them yet. Logical readers never see them; truncate uses them to keep
	/// those bytes, and the deferred purge drops them with their segments.
	deferred_positions: BTreeMap<u64, WalRecordPointer>,
}

/// Which WAL operation and step left the store with uncertain on-disk state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct WalPoison {
	op: &'static str,
	step: &'static str,
}

/// Deferred physical log: (segments wholly at or below the logical purge boundary, bytes of
/// purged records still on disk, straddling segments included), from tracked store state.
fn deferred_log(inner: &LogStoreInner) -> (usize, u64) {
	let segments = match inner.last_purged {
		Some(boundary) => inner
			.wal_segments
			.iter()
			.filter(|segment| {
				segment
					.end_log_index
					.is_some_and(|end| end <= boundary.index)
			})
			.count(),
		None => 0,
	};
	let bytes = inner
		.deferred_positions
		.values()
		.map(|pointer| pointer.len_bytes)
		.sum();
	(segments, bytes)
}

fn report_deferred(inner: &LogStoreInner) {
	let (segments, bytes) = deferred_log(inner);
	metrics().set_control_wal_deferred(segments, bytes);
}

/// The store's side of snapshot publication, called by the publisher after a snapshot is
/// durable, and at open.
#[async_trait::async_trait]
impl DeferredPurge for WendelonLogStore {
	async fn complete_deferred_purge(&self) -> std::io::Result<()> {
		// A failed publication refuses cleanup even when there is nothing to delete.
		self.publication.covered_index().await?;
		let last_purged = self.inner.read().await.last_purged;
		let Some(last_purged) = last_purged else {
			return Ok(());
		};
		let mut store = self.clone();
		store
			.purge(last_purged)
			.await
			.map_err(|err| Error::other(format!("deferred purge: {err}")))
	}
}

/// Truncate(k), resolved before any effect: r (the greatest retained entry below k) and its
/// record, the next index after the cut, the current tail's next index, and C′.
struct TruncatePlan {
	retained: Option<LogId<u32>>,
	retained_position: Option<(u64, WalRecordPointer)>,
	next: u64,
	tail_next: u64,
	checkpoint: Option<CheckpointPage>,
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
	/// Shared with the state machine: the durable snapshot that alone authorizes physical
	/// deletion of purged log.
	publication: Arc<SnapshotPublication>,
	_directory_owner: Option<DataDirectoryOwner>,
	/// Captured from the bytes parsed at open, before any recovery mutation.
	recovered_artifact_hash: Option<([u8; 32], Option<LogId<u32>>)>,
	#[cfg(test)]
	backup_after_wal_read_hold:
		Arc<std::sync::Mutex<Option<(Arc<tokio::sync::Semaphore>, Arc<tokio::sync::Semaphore>)>>>,
}

impl WendelonLogStore {
	#[cfg(test)]
	pub(crate) fn pause_backup_after_wal_read_for_test(
		&self,
	) -> (Arc<tokio::sync::Semaphore>, Arc<tokio::sync::Semaphore>) {
		let entered = Arc::new(tokio::sync::Semaphore::new(0));
		let release = Arc::new(tokio::sync::Semaphore::new(0));
		*self.backup_after_wal_read_hold.lock().unwrap() = Some((entered.clone(), release.clone()));
		(entered, release)
	}

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
				poisoned: None,
				deferred_positions: BTreeMap::new(),
			})),
			wal_gate: Arc::new(Mutex::new(())),
			retention: None,
			publication: Arc::new(SnapshotPublication::new(PublishedSnapshot::NoArtifact)),
			_directory_owner: None,
			#[cfg(test)]
			backup_after_wal_read_hold: Default::default(),
			recovered_artifact_hash: None,
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
	#[cfg(any(test, feature = "test-support"))]
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

	#[cfg(any(test, feature = "test-support"))]
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
	#[cfg(any(test, feature = "test-support"))]
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
			None,
		)
		.await
	}

	/// Open the node's WAL while retaining the same owner token as bootstrap.
	pub(crate) async fn open_with_retention_and_owner(
		wal_path: impl AsRef<Path>,
		vote_path: impl AsRef<Path>,
		snapshot_path: impl AsRef<Path>,
		io: IoRingHandle,
		retention: Arc<RetentionGuard>,
		owner: DataDirectoryOwner,
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
			Some(owner),
		)
		.await
	}

	/// Open durable control artifacts while retaining the directory owner.
	pub async fn open_with_file_ops_and_owner(
		wal_path: impl AsRef<Path>,
		vote_path: impl AsRef<Path>,
		snapshot_path: impl AsRef<Path>,
		file_ops: Arc<dyn AsyncFileOps>,
		owner: DataDirectoryOwner,
	) -> std::io::Result<Self> {
		let file_ops = file_ops.bind_directory_owner(owner.clone())?;
		Self::open_with_options_and_retention(
			wal_path,
			vote_path,
			snapshot_path,
			file_ops,
			WalOptions::default(),
			None,
			Some(owner),
		)
		.await
	}

	#[cfg(any(test, feature = "test-support"))]
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
		owner: Option<DataDirectoryOwner>,
	) -> std::io::Result<Self> {
		let options = options.validate()?;
		let wal_path = wal_path.as_ref();
		let vote_path = vote_path.as_ref();
		let snapshot_path = snapshot_path.as_ref();
		if let Some(owner) = &owner {
			owner.ensure_contains(wal_path)?;
			owner.ensure_contains(vote_path)?;
			owner.ensure_contains(snapshot_path)?;
		}
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
		let mut replay = replay;
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
		// Read-only, after the typed Full refusals and before any effect.
		check_retained_log_contiguous(
			wal_path,
			snapshot.as_ref().and_then(|loaded| loaded.last_included),
			replay.legacy_start,
			&replay.entries,
		)?;
		// Both digests come from the buffers actually consumed by the parsers,
		// including WAL padding. Never re-encode SmState to certify this input.
		let wal_hash = replay
			.artifact_hasher
			.take()
			.expect("replay input hasher")
			.finish();
		let (snapshot_hash, snapshot_bytes) = snapshot
			.as_ref()
			.map(|snapshot| (snapshot.artifact_hash, snapshot.artifact_bytes))
			.unwrap_or((control_artifact_digest_32(&[]), 0));
		let recovered_artifact_hash = restore_artifact_hash(
			wal_hash,
			replay.artifact_bytes,
			snapshot_hash,
			snapshot_bytes,
		);
		let input_applied =
			recovery_committed.or_else(|| snapshot.as_ref().and_then(|s| s.last_included));
		// The first effect of open, after every read-only refusal above.
		complete_checkpoint_holder_at_open(
			file_ops.as_ref(),
			wal_path,
			snapshot.as_ref(),
			&mut replay,
		)
		.await?;
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
		let published = match &snapshot {
			Some(loaded) => PublishedSnapshot::Artifact(loaded.last_included),
			None => PublishedSnapshot::NoArtifact,
		};

		let store = Self {
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
				poisoned: None,
				deferred_positions: BTreeMap::new(),
			})),
			wal_gate: Arc::new(Mutex::new(())),
			retention,
			publication: Arc::new(SnapshotPublication::new(published)),
			recovered_artifact_hash: owner
				.as_ref()
				.map(|_| (recovered_artifact_hash, input_applied)),
			_directory_owner: owner,
			#[cfg(test)]
			backup_after_wal_read_hold: Default::default(),
		};
		metrics().set_control_wal_store_poisoned(false);
		// Log the validated durable snapshot covers, kept by a purge that ran before it was
		// durable: removed now, after every read-only refusal and completion above, before any
		// append. A failure aborts open.
		if !full {
			store.complete_deferred_purge().await?;
		}
		Ok(store)
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
			None,
		)
		.await
	}

	/// The file name a WAL segment starting at `start_index` has in `dir`.
	#[cfg(test)]
	pub(crate) fn segment_path_for_retention_test(dir: &Path, start_index: u64) -> PathBuf {
		wal_segment_path(&dir.join("control.wal"), start_index).unwrap()
	}

	/// Deferred physical log, as the gauges report it.
	#[cfg(test)]
	pub(crate) async fn deferred_log_for_test(&self) -> (usize, u64) {
		deferred_log(&*self.inner.read().await)
	}

	/// Records a durable snapshot at `last_included`, as a publication would; for tests that
	/// place a snapshot artifact on disk by hand after open.
	#[cfg(test)]
	pub(crate) async fn note_published_snapshot_for_test(&self, last_included: LogId<u32>) {
		*self.publication.state.lock().await =
			PublicationState::Healthy(PublishedSnapshot::Artifact(Some(last_included)));
	}

	pub async fn bootstrap_mode(&self) -> DurableBootstrapMode {
		self.inner.read().await.bootstrap_mode
	}

	pub async fn recovery_checkpoint(&self) -> Option<RecoveryCheckpoint> {
		self.inner.read().await.recovery_checkpoint
	}

	/// True only when nothing has ever been made durable here: no vote, no
	/// retained or purged log position, no entry, no snapshot, no checkpoint
	/// and no committed pointer. Empty artifacts that a bootstrap created and
	/// then abandoned qualify; compaction or an uncommitted entry does not.
	pub(crate) async fn is_pristine(&self) -> bool {
		let inner = self.inner.read().await;
		inner.vote.is_none()
			&& inner.last_log_id.is_none()
			&& inner.last_purged.is_none()
			&& inner.entries.is_empty()
			&& inner.recovered_snapshot.is_none()
			&& inner.recovery_checkpoint.is_none()
			&& inner.recovery_committed.is_none()
	}

	/// `poison`, mapped to the storage error a Raft log mutator returns.
	async fn poisoned_storage(
		&self,
		op: &'static str,
		step: &'static str,
		error: Error,
	) -> StorageError<u32> {
		let error = self.poison(op, step, error).await;
		StorageIOError::write_logs(AnyError::new(&error)).into()
	}

	/// Truncate step 3: shrinks the retained segment R in place to `new_len`, the end of r's
	/// record, through a handle opened on its current path, then syncs it. Callers hold
	/// `wal_gate`; any failure poisons.
	async fn shrink_retained(
		&self,
		file_ops: &dyn AsyncFileOps,
		segment: &WalSegmentMeta,
		new_len: u64,
	) -> Result<FileHandle, StorageError<u32>> {
		let handle = match open_existing_wal_segment(file_ops, &segment.path).await {
			Ok(handle) => handle,
			Err(e) => return Err(self.poisoned_storage("truncate", "reopen", e).await),
		};
		if let Err(e) = file_ops.set_len(&handle, new_len).await {
			return Err(self.poisoned_storage("truncate", "set-len", e).await);
		}
		if let Err(e) = file_ops.fsync(&handle).await {
			return Err(self.poisoned_storage("truncate", "fsync", e).await);
		}
		Ok(handle)
	}

	/// Publishes a durable truncate at `k`: only retained entries and positions remain, and the
	/// tail is r, else the purge boundary.
	async fn publish_truncate(
		&self,
		k: u64,
		retained: Option<LogId<u32>>,
		wal_segments: Vec<WalSegmentMeta>,
		wal_file: Option<FileHandle>,
		wal_cursor: u64,
	) {
		let mut guard = self.inner.write().await;
		guard.entries.retain(|&idx, _| idx < k);
		guard.entry_positions.retain(|&idx, _| idx < k);
		guard.wal_segments = wal_segments;
		guard.wal_file = wal_file;
		guard.wal_cursor = wal_cursor;
		guard.last_log_id = retained.or(guard.last_purged);
	}

	/// Latches the poison (first cause wins) and returns `error`. Callers hold `wal_gate`.
	async fn poison(&self, op: &'static str, step: &'static str, error: Error) -> Error {
		self.inner
			.write()
			.await
			.poisoned
			.get_or_insert(WalPoison { op, step });
		metrics().set_control_wal_store_poisoned(true);
		error
	}

	async fn write_recovery_checkpoint(
		&self,
		observation: CheckpointObservation,
	) -> std::io::Result<()> {
		let Some(last_log_id) = observation.last_log_id else {
			return Ok(());
		};
		if observation.checkpoint.commit_index != last_log_id.index {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"checkpoint observation commits {} but names log index {}",
					observation.checkpoint.commit_index, last_log_id.index
				),
			));
		}
		let _gate = self.wal_gate.lock().await;
		let (
			file_ops,
			artifact_paths,
			mut wal_segments,
			mut wal_file,
			mut wal_cursor,
			segment_bytes,
			current_checkpoint,
			next_checkpoint,
			pointer,
			tail_next,
		) = {
			let guard = self.inner.read().await;
			if let Some(poison) = guard.poisoned {
				return Err(poisoned_error(poison));
			}
			let current = guard.recovery_checkpoint.unwrap_or_default();
			let next = RecoveryCheckpoint {
				commit_index: observation
					.checkpoint
					.commit_index
					.max(current.commit_index),
				e_safe_index: if observation.checkpoint.commit_index > current.commit_index {
					observation.checkpoint.e_safe_index
				} else {
					observation
						.checkpoint
						.e_safe_index
						.max(current.e_safe_index)
				},
			};
			if next.e_safe_index > next.commit_index {
				return Err(Error::new(
					ErrorKind::InvalidData,
					format!(
						"checkpoint e_safe {} exceeds its commit {}",
						next.e_safe_index, next.commit_index
					),
				));
			}
			let pointer = resolve_committed_pointer(
				next.commit_index,
				&[
					Some(last_log_id),
					guard.recovery_committed,
					guard
						.entries
						.get(&next.commit_index)
						.map(|entry| entry.log_id),
					guard.last_purged,
					guard
						.recovered_snapshot
						.as_ref()
						.and_then(|snapshot| snapshot.last_included),
				],
			)?;
			(
				guard.file_ops.clone(),
				guard.artifact_paths.clone(),
				guard.wal_segments.clone(),
				guard.wal_file.clone(),
				guard.wal_cursor,
				guard.wal_segment_bytes,
				current,
				next,
				pointer,
				physical_tail_next(guard.last_log_id, guard.last_purged),
			)
		};
		if next_checkpoint == current_checkpoint {
			let mut guard = self.inner.write().await;
			guard.recovery_committed = Some(pointer);
			return Ok(());
		}
		let page = serialize_checkpoint_page(
			next_checkpoint.commit_index,
			next_checkpoint.e_safe_index,
			pointer.index,
			pointer.leader_id.term,
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
		let tail_next = tail_next?;
		if let Err(e) = check_segment_identities(&wal_segments) {
			return Err(self.poison("checkpoint", "segment-invariant", e).await);
		}
		let page_len = page.len() as u64;
		let mut page = Some(page);
		let needs_rotation = wal_file.is_none()
			|| wal_cursor
				.checked_add(page_len)
				.is_none_or(|end| end > segment_bytes);
		let mut published = false;
		if needs_rotation {
			let reuse = match wal_segments.last() {
				Some(last) if last.end_log_index.is_none() => {
					if last.start_index != tail_next {
						let e = Error::new(
							ErrorKind::InvalidData,
							format!(
								"checkpoint-only active segment starts at {} but the WAL tail continues at {}",
								last.start_index, tail_next
							),
						);
						return Err(self.poison("checkpoint", "active-name", e).await);
					}
					Some(last.clone())
				}
				_ => None,
			};
			if let Some(last) = reuse {
				let (handle, len) = match replace_checkpoint_only_segment(
					file_ops.as_ref(),
					&artifact_paths.wal,
					&last,
					vec![page.take().expect("checkpoint page")],
				)
				.await
				{
					Ok(replaced) => replaced,
					Err(e) => return Err(self.poison("checkpoint", "replace", e).await),
				};
				let active = wal_segments.last_mut().expect("checkpoint-only segment");
				active.len_bytes = len;
				wal_file = Some(handle);
				wal_cursor = len;
			} else {
				// A complete holder: the page is synced before the name exists.
				let (segment_path, segment_handle, len) = match create_wal_holder(
					file_ops.as_ref(),
					&artifact_paths.wal,
					tail_next,
					vec![page.take().expect("checkpoint page")],
				)
				.await
				{
					Ok(created) => created,
					Err(e) => return Err(self.poison("checkpoint", "create-holder", e).await),
				};
				wal_segments.push(WalSegmentMeta {
					path: segment_path,
					start_index: tail_next,
					end_log_index: None,
					len_bytes: len,
				});
				wal_file = Some(segment_handle);
				wal_cursor = len;
			}
			published = true;
		}
		if let Some(page) = page.filter(|_| !published) {
			let active_handle = wal_file.clone().expect("active wal segment handle");
			let expected_len = page.len();
			let written = match file_ops.write_at(&active_handle, wal_cursor, page).await {
				Ok((written, _page)) => written,
				Err(e) => return Err(self.poison("checkpoint", "write", e).await),
			};
			if written != expected_len {
				let e = Error::new(
					ErrorKind::WriteZero,
					format!("short checkpoint write: expected {expected_len} bytes, got {written}"),
				);
				return Err(self.poison("checkpoint", "short-write", e).await);
			}
			wal_cursor += page_len;
			wal_segments
				.last_mut()
				.expect("active wal segment metadata")
				.len_bytes = wal_cursor;
			if let Err(e) = file_ops.fdatasync(&active_handle).await {
				return Err(self.poison("checkpoint", "fdatasync", e).await);
			}
		}
		if let Err(e) = check_segment_identities(&wal_segments) {
			return Err(self.poison("checkpoint", "segment-invariant", e).await);
		}

		let mut guard = self.inner.write().await;
		guard.recovery_checkpoint = Some(next_checkpoint);
		guard.recovery_committed = Some(pointer);
		guard.wal_segments = wal_segments;
		guard.wal_file = wal_file;
		guard.wal_cursor = wal_cursor;
		Ok(())
	}

	/// A replay proof needs a verified snapshot prefix or an uninterrupted log from 1.
	/// Unlike legacy recovery, it never accepts an unexplained retained prefix.
	/// It applies the rule reopen applies: a validated durable snapshot, recovered
	/// or published since, explains the prefix it covers. A failed publication
	/// refuses until reopen validates what is on disk.
	pub(crate) async fn replacement_replay_base_verified(&self) -> bool {
		let Ok(published) = self.publication.covered_log_id().await else {
			return false;
		};
		let guard = self.inner.read().await;
		let recovered = guard
			.recovered_snapshot
			.as_ref()
			.and_then(|s| s.last_included);
		let boundary = match (recovered, published) {
			(Some(recovered), Some(published)) if published.index > recovered.index => {
				Some(published)
			}
			(None, published) => published,
			(recovered, _) => recovered,
		};
		// A logical purge does not wait for snapshot coverage: every purged entry
		// must lie under the verified boundary, or the purge hides a gap.
		if guard
			.last_purged
			.is_some_and(|purged| boundary.is_none_or(|boundary| purged.index > boundary.index))
		{
			return false;
		}
		// Unlike open, the runtime log may still retain entries a published
		// snapshot covers; it must only leave no gap above the boundary.
		let Some(&first) = guard.entries.keys().next() else {
			return true;
		};
		let starts_covered = match boundary {
			Some(boundary) => boundary
				.index
				.checked_add(1)
				.is_some_and(|next| first <= next),
			None => first <= 1,
		};
		starts_covered
			&& guard
				.entries
				.keys()
				.zip(guard.entries.keys().skip(1))
				.all(|(previous, index)| previous.checked_add(1) == Some(*index))
	}

	pub async fn recovered_state_machine(&self) -> std::io::Result<WendelonStateMachine> {
		let (snapshot, entries, checkpoint, snapshot_runtime) = {
			let guard = self.inner.read().await;
			let snapshot_runtime = match (&guard.file_ops, &guard.artifact_paths) {
				(Some(file_ops), Some(paths)) => SnapshotRuntime::new(
					file_ops.clone(),
					SnapshotPaths::from_current(paths.snapshot.clone()),
					self.publication.clone(),
					Some(Arc::new(self.clone()) as Arc<dyn DeferredPurge>),
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
		let mut state_machine =
			WendelonStateMachine::from_recovery(snapshot, entries, checkpoint, snapshot_runtime)?;
		if let (Some((hash, input_applied)), Some(owner)) =
			(self.recovered_artifact_hash, &self._directory_owner)
		{
			let _permit = owner.write_permit()?;
			let applied = state_machine.inner.read().await.state.last_applied_log_id;
			if applied == input_applied {
				state_machine.restore_replay = Some(ControlRestoreReplay {
					owner: owner.clone(),
					hash,
					applied,
				});
			}
		}
		Ok(match &self.retention {
			Some(guard) => state_machine.with_retention_guard(guard.clone()),
			None => state_machine,
		})
	}

	pub async fn export_backup(
		&self,
		_state_machine: &WendelonStateMachine,
	) -> std::io::Result<ControlRingBackupImage> {
		self.export_backup_with_state()
			.await
			.map(|(image, _)| image)
	}

	/// Metadata and the node's consistent-cut record come from the copied bytes,
	/// never a later live SM observation. No restore authority is issued here.
	pub(crate) async fn export_backup_with_state(
		&self,
	) -> std::io::Result<(ControlRingBackupImage, SmState)> {
		let (wal, vote, snapshot) = {
			let _wal = self.wal_gate.lock().await;
			let (paths, file_ops, segments) = {
				let guard = self.inner.read().await;
				if let Some(poison) = guard.poisoned {
					return Err(poisoned_error(poison));
				}
				(
					guard.artifact_paths.clone(),
					guard.file_ops.clone(),
					guard.wal_segments.clone(),
				)
			};
			let paths = paths.ok_or_else(|| {
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
			// The publication lock is a leaf under the WAL gate; no SM lock is
			// acquired here, and both locks are released before pure replay.
			let publication = self.publication.state.lock().await;
			SnapshotPublication::healthy(&publication)?;
			let wal = read_wal_backup_bytes(file_ops.as_ref(), &segments, &paths.wal).await?;
			let vote = read_optional_artifact(file_ops.as_ref(), &paths.vote).await?;
			let snapshot = read_optional_artifact(file_ops.as_ref(), &paths.snapshot).await?;
			(wal, vote, snapshot)
		};
		#[cfg(test)]
		{
			let hold = self.backup_after_wal_read_hold.lock().unwrap().take();
			if let Some((entered, release)) = hold {
				entered.add_permits(1);
				release.acquire().await.unwrap().forget();
			}
		}
		if wal.is_empty() && snapshot.is_empty() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"backup requires at least one Control Ring recovery artifact",
			));
		}
		let state = replay_exported_control_state(&wal, &snapshot)?;
		let encoded_state = encode_snapshot(&state)
			.map_err(|err| Error::other(format!("encode backup state snapshot: {err}")))?;
		let mut voter_ids: Vec<u32> = state.last_membership.voter_ids().collect();
		voter_ids.sort_unstable();

		Ok((
			ControlRingBackupImage {
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
			},
			state,
		))
	}

	pub(crate) async fn restore_backup(
		backup: &ControlRingBackupImage,
		target_dir: impl AsRef<Path>,
		owner: &DataDirectoryOwner,
		resume: bool,
	) -> std::io::Result<()> {
		let target_dir = target_dir.as_ref();
		owner.ensure_path(target_dir)?;
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
		let file_ops: Arc<dyn AsyncFileOps> = Arc::new(BlockingFileOps::with_owner(owner.clone()));
		if resume {
			if !discover_named_wal_segments(file_ops.as_ref(), &wal_path)
				.await?
				.is_empty() || backup.snapshot.is_empty() && snapshot_path.exists()
			{
				return Err(Error::new(
					ErrorKind::InvalidData,
					"pending restore has unexpected control artifacts",
				));
			}
		} else {
			ensure_wal_artifacts_absent(file_ops.as_ref(), &wal_path).await?;
			ensure_artifact_absent(&vote_path)?;
			ensure_artifact_absent(&snapshot_path)?;
		}
		crate::storage::file_ops::write_restore_file(
			file_ops.as_ref(),
			&wal_path,
			&backup.wal,
			resume,
		)
		.await?;
		crate::storage::file_ops::write_restore_file(
			file_ops.as_ref(),
			&vote_path,
			&backup.vote,
			resume,
		)
		.await?;
		if !backup.snapshot.is_empty() {
			crate::storage::file_ops::write_restore_file(
				file_ops.as_ref(),
				&snapshot_path,
				&backup.snapshot,
				resume,
			)
			.await?;
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
			checkpoint,
			committed_pointer,
			superseded_at_open,
		) = {
			let guard = self.inner.read().await;
			if let Some(poison) = guard.poisoned {
				let err = poisoned_error(poison);
				callback.log_io_completed(Err(Error::new(err.kind(), err.to_string())));
				return Err(StorageIOError::write_logs(AnyError::new(&err)).into());
			}
			(
				guard.file_ops.clone(),
				guard.artifact_paths.clone(),
				guard.wal_segments.clone(),
				guard.wal_file.clone(),
				guard.wal_cursor,
				guard.wal_segment_bytes,
				guard.recovery_checkpoint,
				guard.recovery_committed,
				records_superseded_at_open(guard.recovered_snapshot.is_some()),
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
		if let Err(e) = check_segment_identities(&wal_segments) {
			let e = self.poison("append", "segment-invariant", e).await;
			callback.log_io_completed(Err(Error::new(e.kind(), e.to_string())));
			return Err(StorageIOError::write_logs(AnyError::new(&e)).into());
		}

		// Any failure after the first on-disk effect of this batch poisons the store.
		let mut effects = false;
		let mut touched_segments: Vec<(u64, FileHandle)> = Vec::new();
		let mut pending = Vec::with_capacity(batch.len());
		let wal_append_started = Instant::now();
		for (entry, page) in batch.iter().zip(pages) {
			let index = entry.log_id.index;
			if page.len() as u64 > segment_bytes {
				let mut err = Error::new(
					ErrorKind::InvalidInput,
					format!(
						"wal record {} has {} bytes, exceeding segment size {}",
						index,
						page.len(),
						segment_bytes
					),
				);
				if effects {
					err = self.poison("append", "oversized-after-writes", err).await;
				}
				callback.log_io_completed(Err(Error::new(err.kind(), err.to_string())));
				return Err(StorageIOError::write_logs(AnyError::new(&err)).into());
			}
			let expected_len = page.len();
			// I1: a checkpoint-only active segment is named for the next entry it will hold.
			let active_checkpoint_only = wal_segments
				.last()
				.filter(|last| last.end_log_index.is_none() && wal_file.is_some())
				.cloned();
			if let Some(active) = &active_checkpoint_only
				&& active.start_index != index
			{
				let err = Error::new(
					ErrorKind::InvalidData,
					format!(
						"entry {} would follow checkpoint-only segment {}",
						index, active.start_index
					),
				);
				let err = self.poison("append", "active-name", err).await;
				callback.log_io_completed(Err(Error::new(err.kind(), err.to_string())));
				return Err(StorageIOError::write_logs(AnyError::new(&err)).into());
			}
			let needs_rotation = wal_file.is_none()
				|| wal_cursor
					.checked_add(expected_len as u64)
					.is_none_or(|end| end > segment_bytes);
			if needs_rotation && let Some(active) = active_checkpoint_only {
				// The checkpoint-only segment named for this entry is full: replace it with the
				// latest checkpoint followed by the entry, so the checkpoint record survives.
				// Without an in-memory checkpoint, every record in it was superseded at open by
				// the validated snapshot, so the entry alone replaces them.
				let checkpoint = match CheckpointPage::latest(checkpoint, committed_pointer) {
					Ok(None) if !superseded_at_open => Err(Error::new(
						ErrorKind::InvalidData,
						"checkpoint-only segment without an in-memory checkpoint",
					)),
					other => other,
				};
				let checkpoint = match checkpoint {
					Ok(checkpoint) => checkpoint,
					Err(err) => {
						let err = self.poison("append", "checkpoint-state", err).await;
						callback.log_io_completed(Err(Error::new(err.kind(), err.to_string())));
						return Err(StorageIOError::write_logs(AnyError::new(&err)).into());
					}
				};
				let mut replacement = Vec::with_capacity(2);
				let checkpoint_len = match checkpoint {
					Some(checkpoint) => {
						let checkpoint_page = checkpoint.page();
						let len = checkpoint_page.len() as u64;
						replacement.push(checkpoint_page);
						len
					}
					None => 0,
				};
				replacement.push(page);
				effects = true;
				let (handle, len) = match replace_checkpoint_only_segment(
					file_ops.as_ref(),
					&artifact_paths.wal,
					&active,
					replacement,
				)
				.await
				{
					Ok(replaced) => replaced,
					Err(e) => {
						let e = self.poison("append", "replace", e).await;
						callback.log_io_completed(Err(Error::new(e.kind(), e.to_string())));
						return Err(StorageIOError::write_logs(AnyError::new(&e)).into());
					}
				};
				let last = wal_segments.last_mut().expect("checkpoint-only segment");
				last.len_bytes = len;
				last.end_log_index = Some(index);
				wal_file = Some(handle.clone());
				wal_cursor = len;
				touched_segments.push((last.start_index, handle));
				pending.push(PendingWalWrite {
					entry: clone_entry(entry),
					pointer: WalRecordPointer {
						segment_start_index: last.start_index,
						offset: checkpoint_len,
						len_bytes: expected_len as u64,
					},
				});
				continue;
			}
			if needs_rotation {
				let (segment_path, segment_handle) =
					match create_wal_segment(file_ops.as_ref(), &artifact_paths.wal, index).await {
						Ok(created) => created,
						Err(e) => {
							let e = self.poison("append", "create-segment", e).await;
							return Err(StorageIOError::write_logs(AnyError::new(&e)).into());
						}
					};
				wal_segments.push(WalSegmentMeta {
					path: segment_path,
					start_index: index,
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
			effects = true;
			let (written, _page) = match file_ops.write_at(&active_handle, wal_cursor, page).await {
				Ok(result) => result,
				Err(e) => {
					let e = self.poison("append", "write", e).await;
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
				let err = self.poison("append", "short-write", err).await;
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
			active_segment.end_log_index = Some(index);
		}
		metrics().observe_wal_append_latency(wal_append_started.elapsed());

		let wal_fsync_started = Instant::now();
		for (_, handle) in &touched_segments {
			// Durability barrier. All write CQEs above have already
			// been awaited, so this fdatasync meaningfully covers them
			// despite `io_uring`'s unordered fsync-vs-write semantics.
			if let Err(e) = file_ops.fdatasync(handle).await {
				let e = self.poison("append", "fdatasync", e).await;
				callback
					.log_io_completed(Err(std::io::Error::other(format!("wal fdatasync: {e}"))));
				return Err(StorageIOError::write_logs(AnyError::new(&e)).into());
			}
		}
		metrics().observe_wal_fsync_latency(wal_fsync_started.elapsed());
		if let Err(e) = check_segment_identities(&wal_segments) {
			let e = self.poison("append", "segment-invariant", e).await;
			callback.log_io_completed(Err(Error::new(e.kind(), e.to_string())));
			return Err(StorageIOError::write_logs(AnyError::new(&e)).into());
		}

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
		let k = log_id.index;
		let refuse = |e: Error| -> StorageError<u32> {
			StorageIOError::write_logs(AnyError::new(&e)).into()
		};
		let _gate = self.wal_gate.lock().await;
		let (file_ops, artifact_paths, wal_segments, wal_file, wal_cursor, segment_bytes, plan) = {
			let guard = self.inner.read().await;
			if let Some(poison) = guard.poisoned {
				return Err(refuse(poisoned_error(poison)));
			}
			check_segment_identities(&guard.wal_segments).map_err(refuse)?;
			// Nothing at or above k: no effect, as when nothing is removed.
			if guard.entries.range(k..).next().is_none() {
				return Ok(());
			}
			if let Some(committed) = guard.recovery_committed
				&& k <= committed.index
			{
				return Err(refuse(Error::new(
					ErrorKind::InvalidInput,
					format!(
						"truncate at {k} would remove committed entry {}",
						committed.index
					),
				)));
			}
			let retained = guard
				.entries
				.range(..k)
				.next_back()
				.map(|(_, entry)| entry.log_id);
			let plan = TruncatePlan {
				// R: the greatest retained entry below k, else the greatest purged record still on
				// disk, so deferred bytes are shrunk around, never unlinked.
				retained_position: match retained {
					Some(id) => guard
						.entry_positions
						.get(&id.index)
						.map(|pointer| (id.index, *pointer)),
					None => guard
						.deferred_positions
						.iter()
						.next_back()
						.map(|(&index, &pointer)| (index, pointer)),
				},
				retained,
				next: physical_tail_next(retained, guard.last_purged).map_err(refuse)?,
				tail_next: physical_tail_next(guard.last_log_id, guard.last_purged)
					.map_err(refuse)?,
				checkpoint: CheckpointPage::latest(
					guard.recovery_checkpoint,
					guard.recovery_committed,
				)
				.map_err(refuse)?,
			};
			(
				guard.file_ops.clone(),
				guard.artifact_paths.clone(),
				guard.wal_segments.clone(),
				guard.wal_file.clone(),
				guard.wal_cursor,
				guard.wal_segment_bytes,
				plan,
			)
		};

		let Some(file_ops) = file_ops else {
			let mut guard = self.inner.write().await;
			guard.entries.retain(|&idx, _| idx < k);
			guard.entry_positions.retain(|&idx, _| idx < k);
			guard.last_log_id = plan.retained.or(guard.last_purged);
			return Ok(());
		};
		let artifact_paths = artifact_paths.ok_or_else(|| {
			refuse(Error::new(
				ErrorKind::Unsupported,
				"durable wal truncate missing artifact paths",
			))
		})?;
		if plan.retained.is_some() && plan.retained_position.is_none() {
			return Err(refuse(Error::new(
				ErrorKind::InvalidData,
				"wal truncate lost the retained entry position",
			)));
		}
		// R: the segment holding r, and the byte boundary of r's record within it.
		let keep = match plan.retained_position {
			Some((_, pointer)) => {
				let at = wal_segments
					.iter()
					.position(|segment| segment.start_index == pointer.segment_start_index)
					.ok_or_else(|| {
						refuse(Error::new(
							ErrorKind::InvalidData,
							"wal truncate lost retained segment metadata",
						))
					})?;
				let new_len = pointer
					.offset
					.checked_add(pointer.len_bytes)
					.ok_or_else(|| {
						refuse(Error::new(
							ErrorKind::InvalidData,
							"wal record end overflows",
						))
					})?;
				if new_len > wal_segments[at].len_bytes {
					return Err(refuse(Error::new(
						ErrorKind::InvalidData,
						format!(
							"retained record ends at {new_len}, past segment {} length {}",
							wal_segments[at].path.display(),
							wal_segments[at].len_bytes
						),
					)));
				}
				Some((at, new_len))
			}
			None => None,
		};
		let suffix_from = keep.map_or(0, |(at, _)| at + 1);

		let Some(checkpoint) = plan.checkpoint else {
			// Without a checkpoint: suffix segments descending, then R shrinks in place.
			for segment in wal_segments[suffix_from..].iter().rev() {
				if let Err(e) = unlink_wal_artifact(file_ops.as_ref(), &segment.path).await {
					return Err(self.poisoned_storage("truncate", "unlink", e).await);
				}
			}
			let mut wal_segments = wal_segments;
			wal_segments.truncate(suffix_from);
			let (wal_file, wal_cursor) = match keep {
				Some((at, new_len)) => {
					let handle = self
						.shrink_retained(file_ops.as_ref(), &wal_segments[at], new_len)
						.await?;
					wal_segments[at].len_bytes = new_len;
					wal_segments[at].end_log_index = plan.retained_position.map(|(index, _)| index);
					(Some(handle), new_len)
				}
				None => (None, 0),
			};
			self.publish_truncate(k, plan.retained, wal_segments, wal_file, wal_cursor)
				.await;
			return Ok(());
		};

		// With a checkpoint, the last file in sorted order ends with C′ at every step.
		// Step 1: Z, a complete checkpoint holder that sorts last.
		let active = wal_segments.last().cloned().ok_or_else(|| {
			refuse(Error::new(
				ErrorKind::InvalidData,
				"wal truncate found entries but no segment",
			))
		})?;
		let z_from_active = active.end_log_index.is_none();
		if z_from_active && active.start_index != plan.tail_next {
			return Err(refuse(Error::new(
				ErrorKind::InvalidData,
				format!(
					"checkpoint-only active segment starts at {} but the WAL tail continues at {}",
					active.start_index, plan.tail_next
				),
			)));
		}
		if !z_from_active
			&& file_ops
				.file_len(&wal_segment_path(&artifact_paths.wal, plan.tail_next).map_err(refuse)?)
				.await
				.map_err(refuse)?
				.is_some()
		{
			return Err(refuse(Error::new(
				ErrorKind::AlreadyExists,
				format!("checkpoint holder name {} is occupied", plan.tail_next),
			)));
		}
		let z_path = if z_from_active {
			let handle = wal_file.clone().ok_or_else(|| {
				refuse(Error::new(
					ErrorKind::InvalidData,
					"checkpoint-only active segment has no handle",
				))
			})?;
			match append_checkpoint_to_active(
				file_ops.as_ref(),
				&artifact_paths.wal,
				&active,
				&handle,
				wal_cursor,
				segment_bytes,
				checkpoint,
			)
			.await
			{
				Ok(_) => active.path.clone(),
				Err(e) => return Err(self.poisoned_storage("truncate", "preserve", e).await),
			}
		} else {
			match create_wal_holder(
				file_ops.as_ref(),
				&artifact_paths.wal,
				plan.tail_next,
				vec![checkpoint.page()],
			)
			.await
			{
				Ok((path, _, _)) => path,
				Err(e) => return Err(self.poisoned_storage("truncate", "create-holder", e).await),
			}
		};
		// Step 2: the suffix, other than Z, descending; one unlink and directory sync each.
		let suffix_end = if z_from_active {
			wal_segments.len() - 1
		} else {
			wal_segments.len()
		};
		for segment in wal_segments[suffix_from..suffix_end].iter().rev() {
			if let Err(e) = unlink_wal_artifact(file_ops.as_ref(), &segment.path).await {
				return Err(self.poisoned_storage("truncate", "unlink", e).await);
			}
		}
		let mut wal_segments = wal_segments;
		wal_segments.truncate(suffix_from);
		// Step 3: R shrinks in place to the end of r's record.
		if let Some((at, new_len)) = keep {
			self.shrink_retained(file_ops.as_ref(), &wal_segments[at], new_len)
				.await?;
			wal_segments[at].len_bytes = new_len;
			wal_segments[at].end_log_index = plan.retained_position.map(|(index, _)| index);
		}
		// Step 4: the final checkpoint-only segment, named for the next entry.
		let (next_path, next_handle, next_len) = match create_wal_holder(
			file_ops.as_ref(),
			&artifact_paths.wal,
			plan.next,
			vec![checkpoint.page()],
		)
		.await
		{
			Ok(created) => created,
			Err(e) => return Err(self.poisoned_storage("truncate", "create-next", e).await),
		};
		// Step 5: Z goes; "next" is now last and ends with C′.
		if let Err(e) = unlink_wal_artifact(file_ops.as_ref(), &z_path).await {
			return Err(self.poisoned_storage("truncate", "unlink-holder", e).await);
		}
		wal_segments.push(WalSegmentMeta {
			path: next_path,
			start_index: plan.next,
			end_log_index: None,
			len_bytes: next_len,
		});
		if let Err(e) = check_segment_identities(&wal_segments) {
			return Err(self
				.poisoned_storage("truncate", "segment-invariant", e)
				.await);
		}
		// Step 6: publish.
		self.publish_truncate(k, plan.retained, wal_segments, Some(next_handle), next_len)
			.await;
		Ok(())
	}

	async fn purge(&mut self, log_id: LogId<u32>) -> Result<(), StorageError<u32>> {
		if retention::refuses(&self.retention, RetentionViolation::Purge) {
			return Err(
				StorageIOError::write_logs(AnyError::new(&RetentionViolation::Purge)).into(),
			);
		}
		let p = log_id.index;
		let refuse = |e: Error| -> StorageError<u32> {
			StorageIOError::write_logs(AnyError::new(&e)).into()
		};
		let _gate = self.wal_gate.lock().await;
		let (
			file_ops,
			artifact_paths,
			mut wal_segments,
			wal_file,
			wal_cursor,
			segment_bytes,
			checkpoint,
			last_purged,
			last_log_id,
			post_next,
			superseded_at_open,
		) = {
			let guard = self.inner.read().await;
			if let Some(poison) = guard.poisoned {
				return Err(refuse(poisoned_error(poison)));
			}
			check_segment_identities(&guard.wal_segments).map_err(refuse)?;
			// The post-purge boundary and tail: p may pass the local tail (a snapshot install).
			let last_purged = match guard.last_purged {
				Some(prev) if prev > log_id => prev,
				_ => log_id,
			};
			let last_log_id = guard
				.entries
				.range((std::ops::Bound::Excluded(p), std::ops::Bound::Unbounded))
				.next_back()
				.map(|(_, entry)| entry.log_id)
				.filter(|id| *id >= log_id)
				.unwrap_or(last_purged);
			(
				guard.file_ops.clone(),
				guard.artifact_paths.clone(),
				guard.wal_segments.clone(),
				guard.wal_file.clone(),
				guard.wal_cursor,
				guard.wal_segment_bytes,
				CheckpointPage::latest(guard.recovery_checkpoint, guard.recovery_committed)
					.map_err(refuse)?,
				last_purged,
				last_log_id,
				physical_tail_next(Some(last_log_id), None).map_err(refuse)?,
				// Read only with recovery_checkpoint None: see `records_superseded_at_open`.
				records_superseded_at_open(guard.recovered_snapshot.is_some()),
			)
		};
		let mut wal_file = wal_file;
		let mut wal_cursor = wal_cursor;
		let mut deleted_starts_all: BTreeSet<u64> = BTreeSet::new();
		// A failed snapshot publication refuses a durable purge before any effect or logical
		// change, until reopen.
		let covered = if file_ops.is_some() {
			self.publication.covered_index().await.map_err(refuse)?
		} else {
			None
		};
		if let Some(file_ops) = file_ops.as_ref() {
			let artifact_paths = artifact_paths.ok_or_else(|| {
				refuse(Error::new(
					ErrorKind::Unsupported,
					"durable wal purge missing artifact paths",
				))
			})?;
			// Physical deletion needs both the logical boundary and a durable snapshot covering
			// the segment; anything else is kept (deferred) until a publication covers it. A
			// checkpoint-only segment is never in the purged run.
			let delete_count = wal_segments
				.iter()
				.take_while(|segment| {
					segment
						.end_log_index
						.is_some_and(|end| end <= p && covered.is_some_and(|c| end <= c))
				})
				.count();
			deleted_starts_all = wal_segments
				.iter()
				.take(delete_count)
				.map(|segment| segment.start_index)
				.collect();
			// A surviving log segment wholly at or below the boundary is deferred: never the
			// segment new entries or C′ go into.
			let deferred_log = |segment: &WalSegmentMeta| {
				segment
					.end_log_index
					.is_some_and(|end| end <= last_purged.index)
			};
			// A checkpoint-only active segment the new boundary has passed: with C′ it is removed
			// after a holder at the post-purge name is durable; without, it is removed first.
			let below = wal_segments
				.last()
				.filter(|last| last.end_log_index.is_none() && last.start_index != post_next)
				.cloned();
			if let Some(below) = &below
				&& below.start_index > post_next
			{
				return Err(refuse(Error::new(
					ErrorKind::InvalidData,
					format!(
						"checkpoint-only active segment {} lies past the post-purge tail {post_next}",
						below.start_index
					),
				)));
			}
			// Without C′, the passed segment is removed first only on evidence that it carries
			// nothing to keep: it is exactly empty, or every record it holds was superseded at open
			// by the validated snapshot (`records_superseded_at_open`).
			if checkpoint.is_none()
				&& let Some(below) = &below
				&& !superseded_at_open
				&& !exactly_empty(file_ops.as_ref(), below)
					.await
					.map_err(refuse)?
			{
				return Err(refuse(Error::new(
					ErrorKind::InvalidData,
					format!(
						"checkpoint-only segment {} lies below the post-purge tail {post_next} with \
						 no checkpoint and no evidence it carries nothing",
						below.path.display()
					),
				)));
			}
			let survivor = wal_segments
				.last()
				.filter(|last| {
					delete_count < wal_segments.len() && below.is_none() && !deferred_log(last)
				})
				.cloned();
			if delete_count > 0 || below.is_some() {
				let holder_path =
					wal_segment_path(&artifact_paths.wal, post_next).map_err(refuse)?;
				// Step 1: C′ is preserved in a segment that survives and is last.
				let mut holder = None;
				let mut preserved_in_active = false;
				if let Some(checkpoint) = checkpoint {
					let appended = match (&survivor, &wal_file) {
						(Some(active), Some(handle)) => match append_checkpoint_to_active(
							file_ops.as_ref(),
							&artifact_paths.wal,
							active,
							handle,
							wal_cursor,
							segment_bytes,
							checkpoint,
						)
						.await
						{
							Ok(appended) => appended,
							Err(e) => {
								return Err(self.poisoned_storage("purge", "preserve", e).await);
							}
						},
						_ => None,
					};
					if let Some((handle, len)) = appended {
						wal_segments
							.last_mut()
							.expect("surviving segment")
							.len_bytes = len;
						wal_file = Some(handle);
						wal_cursor = len;
						preserved_in_active = true;
					} else {
						if file_ops
							.file_len(&holder_path)
							.await
							.map_err(refuse)?
							.is_some()
						{
							return Err(refuse(Error::new(
								ErrorKind::AlreadyExists,
								format!("checkpoint holder name {post_next} is occupied"),
							)));
						}
						holder = match create_wal_holder(
							file_ops.as_ref(),
							&artifact_paths.wal,
							post_next,
							vec![checkpoint.page()],
						)
						.await
						{
							Ok(created) => Some(created),
							Err(e) => {
								return Err(self
									.poisoned_storage("purge", "create-holder", e)
									.await);
							}
						};
					}
				}
				// Without C′ nothing is preserved: every checkpoint record on disk was superseded
				// at open and none was written since, so the passed segment goes first and no
				// stop can leave it (possibly empty, with no record to carry) last.
				if checkpoint.is_none()
					&& let Some(below) = &below
					&& let Err(e) = unlink_wal_artifact(file_ops.as_ref(), &below.path).await
				{
					return Err(self.poisoned_storage("purge", "unlink-holder", e).await);
				}
				// Step 2: the purged log segments, ascending.
				for segment in wal_segments.iter().take(delete_count) {
					if let Err(e) = unlink_wal_artifact(file_ops.as_ref(), &segment.path).await {
						return Err(self.poisoned_storage("purge", "unlink", e).await);
					}
				}
				// Step 3: the passed checkpoint-only segment, after the new holder is durable.
				if checkpoint.is_some()
					&& let Some(below) = &below
					&& let Err(e) = unlink_wal_artifact(file_ops.as_ref(), &below.path).await
				{
					return Err(self.poisoned_storage("purge", "unlink-holder", e).await);
				}
				wal_segments.drain(..delete_count);
				if below.is_some() {
					wal_segments.pop();
				}
				if let Some((path, handle, len)) = holder {
					wal_segments.push(WalSegmentMeta {
						path,
						start_index: post_next,
						end_log_index: None,
						len_bytes: len,
					});
					wal_file = Some(handle);
					wal_cursor = len;
				} else if preserved_in_active {
					// The live handle and cursor already name the surviving active segment.
				} else if let Some(last) = wal_segments.last() {
					wal_cursor = last.len_bytes;
					wal_file = match open_existing_wal_segment(file_ops.as_ref(), &last.path).await
					{
						Ok(handle) => Some(handle),
						Err(e) => return Err(self.poisoned_storage("purge", "reopen", e).await),
					};
				} else {
					wal_file = None;
					wal_cursor = 0;
				}
				if let Err(e) = check_segment_identities(&wal_segments) {
					return Err(self.poisoned_storage("purge", "segment-invariant", e).await);
				}
			}
			// New entries never go into a deferred segment: the next append names its own.
			if wal_segments.last().is_some_and(deferred_log) {
				wal_file = None;
				wal_cursor = 0;
			}
		}

		let mut guard = self.inner.write().await;
		guard.entries.retain(|&idx, _| idx > p);
		let purged_positions: Vec<(u64, WalRecordPointer)> = guard
			.entry_positions
			.range(..=p)
			.map(|(&index, &pointer)| (index, pointer))
			.collect();
		guard.entry_positions.retain(|&idx, _| idx > p);
		guard.last_purged = Some(last_purged);
		if guard.file_ops.is_some() {
			// Positions of purged records whose segments are kept stay known physically.
			for (index, pointer) in purged_positions {
				guard.deferred_positions.insert(index, pointer);
			}
			guard
				.deferred_positions
				.retain(|_, pointer| !deleted_starts_all.contains(&pointer.segment_start_index));
			guard.wal_segments = wal_segments;
			guard.wal_file = wal_file;
			guard.wal_cursor = wal_cursor;
			report_deferred(&guard);
		}
		guard.last_log_id = Some(last_log_id);
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

/// A weak view of the canonical state, bound when the observer is installed.
/// Reading it never keeps a state machine or data-directory owner alive between
/// attempts. The lease excludes canonical writes during comparison/publication.
#[derive(Clone)]
pub struct TierSnapshotSource(std::sync::Weak<RwLock<SmInner>>);

pub(crate) struct TierSnapshotLease(tokio::sync::OwnedRwLockReadGuard<SmInner>);

impl TierSnapshotSource {
	/// The committed tier record for `slab_id`, without waiting: `WouldBlock`
	/// when the state machine is busy, so a caller holding observer-side locks
	/// never waits on the apply loop.
	pub(crate) fn try_tiered_slab(&self, slab_id: SlabId) -> std::io::Result<Option<TieredSlab>> {
		let inner = self
			.0
			.upgrade()
			.ok_or_else(|| Error::new(ErrorKind::BrokenPipe, "canonical tier source closed"))?;
		let guard = inner
			.try_read()
			.map_err(|_| Error::new(ErrorKind::WouldBlock, "canonical tier state busy"))?;
		Ok(guard.state.tiered_slabs.get(&slab_id).cloned())
	}

	pub(crate) async fn read(&self) -> std::io::Result<TierSnapshotLease> {
		let inner = self
			.0
			.upgrade()
			.ok_or_else(|| Error::new(ErrorKind::BrokenPipe, "canonical tier source closed"))?;
		Ok(TierSnapshotLease(inner.read_owned().await))
	}
}
impl TierSnapshotLease {
	pub(crate) fn revision(&self) -> u64 {
		self.0.tier_revision
	}
	pub(crate) fn tiers(&self) -> Vec<TieredSlab> {
		self.0.state.tiered_slabs.values().cloned().collect()
	}
	pub(crate) fn has_migrated_ranges(&self) -> bool {
		!self.0.state.tombstones.is_empty()
	}
	pub(crate) fn index(&self) -> Option<u64> {
		self.0.state.last_applied_log_id.map(|id| id.index)
	}
}

/// Out-of-band observer for [`TieredSlab`] apply events.
///
/// Wendelon's `SlabManager` implements this so every node — not
/// just the sweeper that initiated the Raft commit — learns when
/// a slab has moved to L2. The observer runs inside the state
/// machine's apply loop and must therefore be non-blocking.
#[async_trait::async_trait]
pub trait TierObserver: Send + Sync {
	/// Bound once at SM construction, before the observer is used.
	fn bind_tier_snapshot_source(&self, _source: TierSnapshotSource) {}

	/// Fence new admissions before committed retirement changes control state.
	/// This hook is synchronous and must not wait on filesystem or network IO.
	fn before_slab_retired(&self, _slab_id: SlabId, _applied_log_index: u64) {}

	/// Called synchronously before publishing an incoming control snapshot.
	fn before_tier_snapshot_install(&self) -> std::io::Result<u64> {
		Ok(0)
	}

	/// Reconcile the complete committed tier set while admission stays fenced.
	async fn on_tier_snapshot_installed(
		&self,
		_tiers: &[TieredSlab],
		_index: Option<u64>,
		_snapshot_epoch: u64,
	) -> std::io::Result<()> {
		Ok(())
	}

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
	/// The one authority over which artifact `current` names; shared with the log store.
	publication: Arc<SnapshotPublication>,
	/// Completes physical log deletion a newly published snapshot now covers.
	deferred_purge: Option<Arc<dyn DeferredPurge>>,
	#[cfg(test)]
	install_after_publication_hold:
		Arc<std::sync::Mutex<Option<(Arc<tokio::sync::Semaphore>, Arc<tokio::sync::Semaphore>)>>>,
}

impl SnapshotRuntime {
	fn new(
		file_ops: Arc<dyn AsyncFileOps>,
		paths: SnapshotPaths,
		publication: Arc<SnapshotPublication>,
		deferred_purge: Option<Arc<dyn DeferredPurge>>,
	) -> Self {
		Self {
			file_ops,
			paths,
			publication,
			deferred_purge,
			#[cfg(test)]
			install_after_publication_hold: Arc::new(std::sync::Mutex::new(None)),
		}
	}

	fn ephemeral() -> Self {
		Self::new(
			Arc::new(BlockingFileOps),
			SnapshotPaths::ephemeral(),
			Arc::new(SnapshotPublication::new(PublishedSnapshot::NoArtifact)),
			None,
		)
	}

	/// After a successful publication: physical log deletion the new snapshot covers. Its
	/// failure fails the publishing call (the node stops); it never un-publishes.
	async fn complete_deferred_purge(&self) -> std::io::Result<()> {
		match &self.deferred_purge {
			Some(store) => store.complete_deferred_purge().await,
			None => Ok(()),
		}
	}
}

/// What `current` holds, as far as runtime publication knows it to be durable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PublishedSnapshot {
	/// No snapshot artifact.
	NoArtifact,
	/// An artifact whose state was applied through this log id (None: empty state).
	Artifact(Option<LogId<u32>>),
}

impl PublishedSnapshot {
	/// The log index a durable artifact covers: the only authority to delete log below it.
	fn covered_index(self) -> Option<u64> {
		self.covered_log_id().map(|id| id.index)
	}

	fn covered_log_id(self) -> Option<LogId<u32>> {
		match self {
			Self::Artifact(Some(id)) => Some(id),
			_ => None,
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PublicationState {
	Healthy(PublishedSnapshot),
	/// A step after a rename of `current` failed, leaving the artifact, this state or the
	/// state machine possibly inconsistent: every reader, publisher and cleanup refuses until
	/// reopen validates what is on disk. `durable` is what had been published before.
	Failed {
		durable: PublishedSnapshot,
		step: &'static str,
	},
}

/// How a candidate artifact compares with the published one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Admission {
	/// Strictly newer by index: it may replace `current`.
	Newer,
	/// The same full identity: the existing artifact stays authoritative.
	Same,
	/// Strictly older by index: it must never replace `current`.
	Older,
}

/// The publication authority for the current snapshot artifact. Every rename of `current`,
/// every update of the state machine's held snapshot, and every opening of a handle on
/// `current` happens under `state`'s lock, so metadata and the bytes behind it agree.
///
/// Lock order: this lock, then the state machine's `inner`; it is released before the
/// checkpoint observer or the log store's `wal_gate` are taken. The log store only reads it as
/// a leaf.
struct SnapshotPublication {
	state: tokio::sync::Mutex<PublicationState>,
}

impl SnapshotPublication {
	fn new(published: PublishedSnapshot) -> Self {
		Self {
			state: tokio::sync::Mutex::new(PublicationState::Healthy(published)),
		}
	}

	/// The published artifact if publication is healthy, else the refusal.
	fn healthy(state: &PublicationState) -> std::io::Result<PublishedSnapshot> {
		match *state {
			PublicationState::Healthy(published) => Ok(published),
			PublicationState::Failed { step, .. } => Err(Error::other(format!(
				"snapshot publication failed at {step}; reopen required"
			))),
		}
	}

	/// Index first; at an equal index the full identity must match.
	fn admit(
		published: PublishedSnapshot,
		candidate: Option<LogId<u32>>,
	) -> std::io::Result<Admission> {
		let current = match published {
			PublishedSnapshot::NoArtifact => return Ok(Admission::Newer),
			PublishedSnapshot::Artifact(current) => current,
		};
		match (current, candidate) {
			(None, None) => Ok(Admission::Same),
			(None, Some(_)) => Ok(Admission::Newer),
			(Some(_), None) => Ok(Admission::Older),
			(Some(current), Some(candidate)) => match candidate.index.cmp(&current.index) {
				std::cmp::Ordering::Greater => Ok(Admission::Newer),
				std::cmp::Ordering::Less => Ok(Admission::Older),
				std::cmp::Ordering::Equal if candidate == current => Ok(Admission::Same),
				std::cmp::Ordering::Equal => Err(Error::new(
					ErrorKind::InvalidData,
					format!(
						"snapshot at index {} conflicts with the published one: {candidate:?} vs {current:?}",
						candidate.index
					),
				)),
			},
		}
	}

	/// The index below which a durable artifact covers the log, read as a leaf. A failed
	/// publication refuses: no cleanup or purge acts until reopen. No artifact, or one of an
	/// empty state, is healthy with no coverage.
	async fn covered_index(&self) -> std::io::Result<Option<u64>> {
		Self::healthy(&*self.state.lock().await).map(PublishedSnapshot::covered_index)
	}

	async fn covered_log_id(&self) -> std::io::Result<Option<LogId<u32>>> {
		Self::healthy(&*self.state.lock().await).map(PublishedSnapshot::covered_log_id)
	}
}

/// The log store side of a publication: physical deletion of segments that the logical purge
/// boundary and the newly durable snapshot both cover.
#[async_trait::async_trait]
trait DeferredPurge: Send + Sync {
	async fn complete_deferred_purge(&self) -> std::io::Result<()>;
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
	/// Process-local freshness for tier/retirement/metadata changes only.
	/// Read and advanced under the canonical state lock; never serialized.
	tier_revision: u64,
	snapshot: Option<HeldSnapshot>,
	/// Monotonic id handed out in built snapshots so every meta
	/// has a unique `snapshot_id` even when the state hasn't
	/// changed between builds.
	snapshot_seq: u64,
}

fn recover_sm_inner(
	snapshot: Option<LoadedSnapshot>,
	entries: Vec<Entry<WendelonTypeConfig>>,
	checkpoint: Option<RecoveryCheckpoint>,
) -> std::io::Result<SmInner> {
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

	Ok(inner)
}

/// Wendelon's Raft state machine. Lease / epoch / E_safe state
/// lives behind an `Arc<RwLock<_>>` so the snapshot builder can
/// take a read lock for a consistent view without blocking apply
/// traffic longer than the postcard encode takes.
#[derive(Clone)]
struct ControlRestoreReplay {
	owner: DataDirectoryOwner,
	hash: [u8; 32],
	applied: Option<LogId<u32>>,
}

#[derive(Clone)]
pub struct WendelonStateMachine {
	/// Bootstrap-only receipt minted after owner-bound input parsing and replay.
	restore_replay: Option<ControlRestoreReplay>,
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
	#[cfg(test)]
	pub(crate) async fn seed_complete_snapshot_state_for_test(&self, state: SmState) {
		self.inner.write().await.state = state;
	}

	#[cfg(test)]
	pub(crate) fn pause_snapshot_after_publication_for_test(
		&self,
		entered: Arc<tokio::sync::Semaphore>,
		release: Arc<tokio::sync::Semaphore>,
	) {
		*self
			.snapshot_runtime
			.install_after_publication_hold
			.lock()
			.unwrap() = Some((entered, release));
	}

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
			restore_replay: None,
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
		let inner = recover_sm_inner(snapshot, entries, checkpoint)?;

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
			restore_replay: None,
		})
	}

	/// Install a tier observer. Intended to be called once, at
	/// bootstrap, before the state machine is handed to openraft.
	/// Replacing an observer mid-run is not supported — callers
	/// should reconstruct the SM instead.
	pub fn with_tier_observer(mut self, observer: Arc<dyn TierObserver>) -> Self {
		observer.bind_tier_snapshot_source(TierSnapshotSource(Arc::downgrade(&self.inner)));
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

	/// Extract only the applied facts needed by the completed-JOIN read proof.
	/// Both facts come from the same state-machine read; callers need not clone
	/// unrelated table, migration, audit or finalization state.
	pub(crate) async fn join_completion_snapshot(
		&self,
	) -> (StoredMembership<u32, BasicNode>, Option<(u64, EpochConfig)>) {
		let guard = self.inner.read().await;
		(
			guard.state.last_membership.clone(),
			guard
				.state
				.epoch_history
				.last_key_value()
				.map(|(key, epoch)| (*key, epoch.clone())),
		)
	}

	/// Test / caller inspection hook — return a cloned snapshot
	/// of the applied state without driving a full snapshot
	/// build round-trip.
	/// Identity of the exact snapshot/WAL buffers consumed by owner-bound replay.
	/// This receipt is used only during bootstrap, before live writers start.
	pub(crate) async fn restored_control_identity(
		&self,
		owner: &DataDirectoryOwner,
	) -> std::io::Result<([u8; 32], Option<LogId<u32>>)> {
		let _permit = owner.write_permit()?;
		let proof = self.restore_replay.as_ref().ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				"control replay lacks an owner-bound artifact receipt",
			)
		})?;
		if !proof.owner.same_owner(owner) {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"control replay belongs to another owner",
			));
		}
		let applied = self.inner.read().await.state.last_applied_log_id;
		if applied != proof.applied {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"control replay advanced beyond its restore receipt",
			));
		}
		Ok((proof.hash, applied))
	}

	/// One authoritative tier pointer, copied under the SM lock. Callers may
	/// hold a slab publication lock; no observer is awaited under this guard.
	/// A caller holding a slab publication lock cannot await this lock: a
	/// snapshot retry may hold the canonical read lease while draining that
	/// publication lock. Refusal leaves its reconciliation work for next pass.
	pub(crate) fn try_tiered_slab(&self, slab_id: SlabId) -> std::io::Result<Option<TieredSlab>> {
		let guard = self
			.inner
			.try_read()
			.map_err(|_| Error::new(ErrorKind::WouldBlock, "canonical tier state busy"))?;
		Ok(guard.state.tiered_slabs.get(&slab_id).cloned())
	}

	pub(crate) async fn tiered_slab(&self, slab_id: SlabId) -> Option<TieredSlab> {
		self.inner
			.read()
			.await
			.state
			.tiered_slabs
			.get(&slab_id)
			.cloned()
	}

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
				if guard.tier_revision == u64::MAX {
					return Err(StorageIOError::write_state_machine(AnyError::error(
						"canonical tier revision exhausted",
					))
					.into());
				}
				if let EntryPayload::Normal(WendelonRequest::RetireTieredSlab(retire)) =
					&entry.payload && guard.state.tiered_slabs.contains_key(&retire.slab_id)
					&& let Some(observer) = self.tier_observer.as_ref()
				{
					observer.before_slab_retired(retire.slab_id, entry.log_id.index);
				}
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
					guard.tier_revision = guard.tier_revision.checked_add(1).ok_or_else(|| {
						StorageIOError::write_state_machine(AnyError::error(
							"canonical tier revision exhausted",
						))
					})?;
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
		// Publication: compare, rename, directory sync and the held-snapshot update happen under
		// one lock, and any failure after the rename latches until reopen.
		let write_err = |err: Error| -> StorageError<u32> {
			StorageIOError::write_snapshot(Some(meta.signature()), AnyError::new(&err)).into()
		};
		let mut publication = self.snapshot_runtime.publication.state.lock().await;
		let published = SnapshotPublication::healthy(&publication).map_err(write_err)?;
		match SnapshotPublication::admit(published, meta.last_log_id).map_err(write_err)? {
			Admission::Newer => {}
			Admission::Same | Admission::Older => {
				return Err(write_err(Error::new(
					ErrorKind::InvalidInput,
					format!(
						"snapshot install at {:?} does not advance the published {published:?}",
						meta.last_log_id
					),
				)));
			}
		}
		let snapshot_epoch = if let Some(observer) = self.tier_observer.as_ref() {
			Some(observer.before_tier_snapshot_install().map_err(write_err)?)
		} else {
			None
		};
		self.snapshot_runtime
			.file_ops
			.rename(&receive_path, &self.snapshot_runtime.paths.current)
			.await
			.map_err(write_err)?;
		if let Err(err) = fsync_parent_dir(
			self.snapshot_runtime.file_ops.as_ref(),
			&self.snapshot_runtime.paths.current,
		)
		.await
		{
			*publication = PublicationState::Failed {
				durable: published,
				step: "install directory sync",
			};
			return Err(write_err(err));
		}
		let snapshot_tiers = state.tiered_slabs.values().cloned().collect::<Vec<_>>();
		#[cfg(test)]
		{
			let hold = self
				.snapshot_runtime
				.install_after_publication_hold
				.lock()
				.unwrap()
				.take();
			if let Some((entered, release)) = hold {
				entered.add_permits(1);
				if let Ok(permit) = release.acquire().await {
					permit.forget();
				}
			}
		}
		let pending_checkpoint = {
			let mut guard = self.inner.write().await;
			state.last_applied_log_id = meta.last_log_id;
			state.last_membership = meta.last_membership.clone();
			let drained: Result<(), StorageError<u32>> = (|| {
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
				Ok(())
			})();
			if let Err(err) = drained {
				// `current` is durable but the state machine does not hold it.
				*publication = PublicationState::Failed {
					durable: PublishedSnapshot::Artifact(meta.last_log_id),
					step: "install state update",
				};
				return Err(err);
			}
			let Some(next_revision) = guard.tier_revision.checked_add(1) else {
				*publication = PublicationState::Failed {
					durable: PublishedSnapshot::Artifact(meta.last_log_id),
					step: "install tier revision",
				};
				return Err(write_err(Error::other("canonical tier revision exhausted")));
			};
			guard.tier_revision = next_revision;
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
		if let Some(observer) = self.tier_observer.as_ref() {
			if let Err(err) = observer
				.on_tier_snapshot_installed(
					&snapshot_tiers,
					meta.last_log_id.map(|id| id.index),
					snapshot_epoch.expect("observer epoch"),
				)
				.await
			{
				*publication = PublicationState::Failed {
					durable: PublishedSnapshot::Artifact(meta.last_log_id),
					step: "tier snapshot reconciliation",
				};
				return Err(write_err(err));
			}
		}
		*publication = PublicationState::Healthy(PublishedSnapshot::Artifact(meta.last_log_id));
		drop(publication);
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
		// The new snapshot is durable: log it covers can now be deleted. A failure poisons the
		// log store and fails this install (the node stops); the snapshot stays published.
		self.snapshot_runtime
			.complete_deferred_purge()
			.await
			.map_err(write_err)?;
		Ok(())
	}

	async fn get_current_snapshot(
		&mut self,
	) -> Result<Option<Snapshot<WendelonTypeConfig>>, StorageError<u32>> {
		// The held metadata and the opened inode are read under the publication lock, so they
		// name the same artifact; a failed publication refuses until reopen.
		let publication = self.snapshot_runtime.publication.state.lock().await;
		SnapshotPublication::healthy(&publication)
			.map_err(|err| StorageIOError::read_snapshot(None, AnyError::new(&err)))?;
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
		drop(publication);
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
		// V-S1: held carriers emit no audit record.
		WendelonRequest::Root2Command(_) => None,
		_ => None,
	}
}

fn audit_outcome(resp: &WendelonResponse) -> AuditOutcome {
	match resp {
		WendelonResponse::Error(_) | WendelonResponse::Root2Dormant => AuditOutcome::Error,
		_ => AuditOutcome::Allowed,
	}
}

fn audit_outcome_message(resp: &WendelonResponse) -> String {
	match resp {
		WendelonResponse::Error(message) => format!("outcome=error reason={message:?}"),
		WendelonResponse::Root2Dormant => "outcome=held reason=Root2Dormant".to_string(),
		_ => "outcome=success".to_string(),
	}
}

fn audit_outcome_parameters(resp: &WendelonResponse) -> Vec<(&'static str, serde_json::Value)> {
	match resp {
		WendelonResponse::Error(message) => {
			vec![("reason", serde_json::Value::String(format!("{message:?}")))]
		}
		WendelonResponse::Root2Dormant => vec![(
			"reason",
			serde_json::Value::String("Root2Dormant".to_string()),
		)],
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
		WendelonRequest::Root2Command(_) => (WendelonResponse::Root2Dormant, None),
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
		let write_err = |err: Error| -> StorageError<u32> {
			StorageIOError::write_snapshot(Some(meta.signature()), AnyError::new(&err)).into()
		};
		// A failed publication refuses before any effect; it is checked again under the lock.
		SnapshotPublication::healthy(&*self.snapshot_runtime.publication.state.lock().await)
			.map_err(write_err)?;
		persist_snapshot_artifact(
			self.snapshot_runtime.file_ops.as_ref(),
			&self.snapshot_runtime.paths,
			&state,
		)
		.await
		.map_err(write_err)?;
		let file_ops = self.snapshot_runtime.file_ops.clone();
		let paths = &self.snapshot_runtime.paths;
		let mut publication = self.snapshot_runtime.publication.state.lock().await;
		let published_before = SnapshotPublication::healthy(&publication).map_err(write_err)?;
		let admission =
			SnapshotPublication::admit(published_before, meta.last_log_id).map_err(write_err)?;
		if admission != Admission::Newer {
			// A build no newer than `current` publishes nothing. It returns its own metadata over
			// its own bytes: a handle on the temporary inode, whose name is then removed so the
			// next build cannot rewrite those bytes.
			let own = SnapshotDataFile::open(file_ops.clone(), paths.build_tmp.clone())
				.await
				.map_err(write_err)?;
			unlink_wal_artifact(file_ops.as_ref(), &paths.build_tmp)
				.await
				.map_err(write_err)?;
			drop(publication);
			return Ok(Snapshot {
				meta,
				snapshot: Box::new(own),
			});
		}
		file_ops
			.rename(&paths.build_tmp, &paths.current)
			.await
			.map_err(write_err)?;
		if let Err(err) = fsync_parent_dir(file_ops.as_ref(), &paths.current).await {
			*publication = PublicationState::Failed {
				durable: published_before,
				step: "build directory sync",
			};
			return Err(write_err(err));
		}
		{
			let mut guard = self.inner.write().await;
			guard.snapshot = Some(HeldSnapshot {
				meta: meta.clone(),
				path: paths.current.clone(),
			});
		}
		let snapshot = match SnapshotDataFile::open(file_ops.clone(), paths.current.clone()).await {
			Ok(snapshot) => snapshot,
			Err(err) => {
				*publication = PublicationState::Failed {
					durable: PublishedSnapshot::Artifact(meta.last_log_id),
					step: "build handle open",
				};
				return Err(write_err(err));
			}
		};
		*publication = PublicationState::Healthy(PublishedSnapshot::Artifact(meta.last_log_id));
		drop(publication);
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
		self.snapshot_runtime
			.complete_deferred_purge()
			.await
			.map_err(write_err)?;
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

fn poisoned_error(poison: WalPoison) -> Error {
	Error::other(format!(
		"wal store refused: an earlier {} failed at {} with uncertain on-disk state; reopen required",
		poison.op, poison.step
	))
}

/// The next log index of the physical WAL: one past the last retained entry, else one past the
/// purge boundary, else genesis 0. Never derived from a committed pointer or an observation.
fn physical_tail_next(
	last_log_id: Option<LogId<u32>>,
	last_purged: Option<LogId<u32>>,
) -> std::io::Result<u64> {
	match last_log_id.or(last_purged) {
		Some(tail) => tail.index.checked_add(1).ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				"wal log index overflows the next segment name",
			)
		}),
		None => Ok(0),
	}
}

/// The exact committed log id at `index`, from every source that knows that index. Every such
/// source must agree; with none, the pointer is unknown and nothing is guessed.
fn resolve_committed_pointer(
	index: u64,
	sources: &[Option<LogId<u32>>],
) -> std::io::Result<LogId<u32>> {
	let mut found: Option<LogId<u32>> = None;
	for candidate in sources.iter().flatten().filter(|id| id.index == index) {
		match found {
			None => found = Some(*candidate),
			Some(known) if known == *candidate => {}
			Some(known) => {
				return Err(Error::new(
					ErrorKind::InvalidData,
					format!(
						"committed log id sources disagree at index {index}: {known:?} vs {candidate:?}"
					),
				));
			}
		}
	}
	found.ok_or_else(|| {
		Error::new(
			ErrorKind::InvalidData,
			format!("no retained or boundary log id for committed index {index}"),
		)
	})
}

/// I2: no two WAL segments share a start index or a path.
fn check_segment_identities(segments: &[WalSegmentMeta]) -> std::io::Result<()> {
	let mut starts = BTreeSet::new();
	let mut paths = BTreeSet::new();
	for segment in segments {
		if !starts.insert(segment.start_index) || !paths.insert(segment.path.clone()) {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"duplicate wal segment {} in store metadata",
					segment.path.display()
				),
			));
		}
	}
	Ok(())
}

/// Opens a fresh, empty staging file for `start_index`, clearing a stale one first.
async fn open_wal_staging(
	file_ops: &dyn AsyncFileOps,
	wal_path: &Path,
	start_index: u64,
) -> std::io::Result<(PathBuf, FileHandle)> {
	let staging = wal_segment_staging_path(wal_path, start_index)?;
	if file_ops.file_len(&staging).await?.is_some() {
		unlink_wal_artifact(file_ops, &staging).await?;
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
	Ok((staging, handle))
}

/// Creates segment `start_index` without ever replacing an existing file: the staging file is
/// published with `link(2)`, which refuses an existing name atomically.
async fn create_wal_segment(
	file_ops: &dyn AsyncFileOps,
	wal_path: &Path,
	start_index: u64,
) -> std::io::Result<(PathBuf, FileHandle)> {
	let final_path = wal_segment_path(wal_path, start_index)?;
	let (staging, handle) = open_wal_staging(file_ops, wal_path, start_index).await?;
	file_ops.link_no_replace(&staging, &final_path).await?;
	unlink_wal_artifact(file_ops, &staging).await?;
	Ok((final_path, handle))
}

/// The one sanctioned replacement: rewrites the known checkpoint-only `segment` (same name) with
/// `pages`, atomically, through a synced staging file. Returns the live handle and new length.
async fn replace_checkpoint_only_segment(
	file_ops: &dyn AsyncFileOps,
	wal_path: &Path,
	segment: &WalSegmentMeta,
	pages: Vec<AlignedBuf>,
) -> std::io::Result<(FileHandle, u64)> {
	let final_path = wal_segment_path(wal_path, segment.start_index)?;
	if segment.end_log_index.is_some() || final_path != segment.path {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"refusing to replace {}: not the checkpoint-only segment",
				segment.path.display()
			),
		));
	}
	let (staging, handle) = open_wal_staging(file_ops, wal_path, segment.start_index).await?;
	let len = write_pages_exact(file_ops, &handle, 0, pages).await?;
	file_ops.fdatasync(&handle).await?;
	file_ops.rename(&staging, &final_path).await?;
	fsync_parent_dir(file_ops, &final_path).await?;
	Ok((handle, len))
}

/// Writes `pages` back to back from `offset`, refusing a short write. Returns the end offset.
async fn write_pages_exact(
	file_ops: &dyn AsyncFileOps,
	handle: &FileHandle,
	offset: u64,
	pages: Vec<AlignedBuf>,
) -> std::io::Result<u64> {
	let mut end = offset;
	for page in pages {
		let expected = page.len();
		let (written, _page) = file_ops.write_at(handle, end, page).await?;
		if written != expected {
			return Err(Error::new(
				ErrorKind::WriteZero,
				format!("short wal write: expected {expected} bytes, got {written}"),
			));
		}
		end = end
			.checked_add(expected as u64)
			.ok_or_else(|| Error::new(ErrorKind::InvalidData, "wal length overflows"))?;
	}
	Ok(end)
}

/// Publishes segment `start_index` holding exactly `pages`, written and synced before its name
/// exists: staging write, `fdatasync`, `link(2)` (never replaces), then the staging name is
/// removed and the directory synced. A failure leaves at most a staging file, never a partial
/// segment under the final name. Returns the path, the live handle and the length.
async fn create_wal_holder(
	file_ops: &dyn AsyncFileOps,
	wal_path: &Path,
	start_index: u64,
	pages: Vec<AlignedBuf>,
) -> std::io::Result<(PathBuf, FileHandle, u64)> {
	let final_path = wal_segment_path(wal_path, start_index)?;
	let (staging, handle) = open_wal_staging(file_ops, wal_path, start_index).await?;
	let len = write_pages_exact(file_ops, &handle, 0, pages).await?;
	file_ops.fdatasync(&handle).await?;
	file_ops.link_no_replace(&staging, &final_path).await?;
	unlink_wal_artifact(file_ops, &staging).await?;
	Ok((final_path, handle, len))
}

/// C′: the in-memory latest checkpoint and the committed log id that heads its page.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct CheckpointPage {
	checkpoint: RecoveryCheckpoint,
	pointer: LogId<u32>,
}

impl CheckpointPage {
	/// None without a checkpoint. A checkpoint at index 0 is recovered without a pointer (open
	/// resolves none for it), so it is headed by the genesis log id, as when it was written.
	fn latest(
		checkpoint: Option<RecoveryCheckpoint>,
		pointer: Option<LogId<u32>>,
	) -> std::io::Result<Option<Self>> {
		let Some(checkpoint) = checkpoint else {
			return Ok(None);
		};
		let pointer = match pointer {
			Some(pointer) if pointer.index == checkpoint.commit_index => pointer,
			None if checkpoint.commit_index == 0 => LogId::default(),
			_ => {
				return Err(Error::new(
					ErrorKind::InvalidData,
					format!(
						"checkpoint commit {} has no matching committed log id {pointer:?}",
						checkpoint.commit_index
					),
				));
			}
		};
		Ok(Some(Self {
			checkpoint,
			pointer,
		}))
	}

	fn page(&self) -> AlignedBuf {
		serialize_checkpoint_page(
			self.checkpoint.commit_index,
			self.checkpoint.e_safe_index,
			self.pointer.index,
			self.pointer.leader_id.term,
		)
	}
}

/// Preserves C′ in the active segment `active` (the last one), whose live handle and cursor are
/// given: appended when it fits, else, for a checkpoint-only segment, by the constrained
/// replacement. Returns the new handle and length, or None when a log segment has no room.
async fn append_checkpoint_to_active(
	file_ops: &dyn AsyncFileOps,
	wal_path: &Path,
	active: &WalSegmentMeta,
	handle: &FileHandle,
	cursor: u64,
	segment_bytes: u64,
	page: CheckpointPage,
) -> std::io::Result<Option<(FileHandle, u64)>> {
	let page = page.page();
	let fits = cursor
		.checked_add(page.len() as u64)
		.is_some_and(|end| end <= segment_bytes);
	if fits {
		let end = write_pages_exact(file_ops, handle, cursor, vec![page]).await?;
		file_ops.fdatasync(handle).await?;
		return Ok(Some((handle.clone(), end)));
	}
	if active.end_log_index.is_none() {
		return replace_checkpoint_only_segment(file_ops, wal_path, active, vec![page])
			.await
			.map(Some);
	}
	Ok(None)
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
	let (header, state, artifact_hash, artifact_bytes) =
		decode_snapshot_file_with_identity(file_ops, snapshot_path)
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
		artifact_hash,
		artifact_bytes,
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

/// Decode the same immutable input pair that restore will consume. This is a
/// pure replay: no owner receipt, live observers, I/O or process metrics.
fn replay_exported_control_state(wal: &[u8], snapshot_bytes: &[u8]) -> std::io::Result<SmState> {
	let snapshot = if snapshot_bytes.is_empty() {
		None
	} else {
		let (header, state, artifact_hash, artifact_bytes) =
			decode_snapshot_reader_with_identity(snapshot_bytes)
				.map_err(|error| Error::new(ErrorKind::InvalidData, error))?;
		validate_snapshot_header_log_coordinates(&header, state.last_applied_log_id)
			.map_err(|error| Error::new(ErrorKind::InvalidData, error))?;
		let last_included = state.last_applied_log_id;
		Some(LoadedSnapshot {
			last_included,
			held: HeldSnapshot {
				meta: SnapshotMeta {
					last_log_id: last_included,
					last_membership: state.last_membership.clone(),
					snapshot_id: "exported-control-cut".into(),
				},
				path: PathBuf::from("exported.snapshot"),
			},
			state,
			artifact_hash,
			artifact_bytes,
		})
	};
	let segment = DiscoveredWalSegment {
		path: PathBuf::from("exported.wal"),
		start_index: 0,
		legacy: true,
	};
	let mut replay = WALReplay {
		last_purged: snapshot.as_ref().and_then(|s| s.last_included),
		..Default::default()
	};
	replay_wal_segment_bytes(wal, &segment, snapshot.as_ref(), false, &mut replay)?;
	check_retained_log_contiguous(
		&segment.path,
		snapshot.as_ref().and_then(|s| s.last_included),
		true,
		&replay.entries,
	)?;
	resolve_recovered_committed(snapshot.as_ref(), &replay.entries, replay.checkpoint)?;
	Ok(recover_sm_inner(
		snapshot,
		replay.entries.into_values().collect(),
		replay.checkpoint,
	)?
	.state)
}

async fn replay_wal(
	file_ops: &dyn AsyncFileOps,
	wal_path: &Path,
	snapshot: Option<&LoadedSnapshot>,
) -> std::io::Result<WALReplay> {
	let mut replay = WALReplay {
		artifact_hasher: Some(control_artifact_hash::Context::new()),
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
		let had_entries = !replay.entries.is_empty();
		if let Some(meta) =
			replay_wal_segment(file_ops, segment, snapshot, allow_torn_tail, &mut replay).await?
		{
			replay.wal_segments.push(meta);
		}
		// Where the first retained entry came from: only an entry the legacy file itself holds
		// keeps the legacy first-entry exemption.
		if !had_entries && !replay.entries.is_empty() {
			replay.legacy_start = segment.legacy;
		}
	}
	Ok(replay)
}

/// The retained log starts right after the validated snapshot boundary (with no snapshot: at 0
/// or 1), and every index follows its predecessor. Anything else is a missing first or interior
/// entry, refused before any open effect. This is a refusal only, never a recovery, and passing
/// it is not evidence of a complete genesis prefix.
///
/// Compatibility only: with no snapshot, a first retained entry read from the legacy
/// single-file WAL keeps the exemption replay already gives that file, so it may start at any
/// index. A validated snapshot always requires its boundary + 1, legacy or not, and the interior
/// rule applies to every WAL.
fn check_retained_log_contiguous(
	wal_path: &Path,
	boundary: Option<LogId<u32>>,
	legacy_start: bool,
	entries: &BTreeMap<u64, Entry<WendelonTypeConfig>>,
) -> std::io::Result<()> {
	let Some(&first) = entries.keys().next() else {
		return Ok(());
	};
	let first_ok = match boundary {
		Some(boundary) => boundary.index.checked_add(1) == Some(first),
		None => legacy_start || first <= 1,
	};
	if !first_ok {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"wal {} retains its first entry at {first}, not right after the snapshot boundary {:?}",
				wal_path.display(),
				boundary.map(|id| id.index)
			),
		));
	}
	let mut previous = first;
	for &index in entries.keys().skip(1) {
		if previous.checked_add(1) != Some(index) {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"wal {} is missing entries between {previous} and {index}",
					wal_path.display()
				),
			));
		}
		previous = index;
	}
	Ok(())
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

/// Open-time completion of an interrupted checkpoint-holder move. Runs after replay and the
/// `Full` checks, before any other effect. Z is a named checkpoint-only segment off the tail;
/// the target is the checkpoint-only segment named `tail_next`. Only these states qualify:
/// - Z is last and sorts past the tail: a truncate stopped between its steps 1 and 5. A present
///   target sorts just before Z and must be exactly Z's last validated record;
/// - Z is last and lies below the tail, explained by the durable snapshot boundary s (Z.start <=
///   s, no retained entry at or past Z.start, tail_next = s + 1): a snapshot install stopped before
///   its purge. Z's last validated record is published as the target; an exactly empty Z carries
///   no record and is only unlinked;
/// - the target is last and Z, so explained, is immediately before it: the same move stopped
///   after publishing the target. The target must be exactly one validated record; Z must be
///   exactly empty, or wholly validated checkpoint records ending in the target's record.
///
/// The target is made durable first (published byte for byte, or re-synced with its directory),
/// then Z is unlinked. Any other checkpoint-only segment off the tail in these shapes, any other
/// named checkpoint-only segment before them, or any differing target refuses before any effect.
async fn complete_checkpoint_holder_at_open(
	file_ops: &dyn AsyncFileOps,
	wal_path: &Path,
	snapshot: Option<&LoadedSnapshot>,
	replay: &mut WALReplay,
) -> std::io::Result<()> {
	let segments = &replay.wal_segments;
	let Some(last) = segments.last().cloned() else {
		return Ok(());
	};
	let named_checkpoint_only = |segment: &WalSegmentMeta| -> std::io::Result<bool> {
		Ok(segment.end_log_index.is_none()
			&& segment.path == wal_segment_path(wal_path, segment.start_index)?)
	};
	if !named_checkpoint_only(&last)? {
		return Ok(());
	}
	let tail_next = physical_tail_next(replay.last_log_id, replay.last_purged)?;
	let explained = |z: &WalSegmentMeta| {
		snapshot
			.and_then(|loaded| loaded.last_included)
			.is_some_and(|s| {
				z.start_index <= s.index
					&& replay.entries.range(z.start_index..).next().is_none()
					&& s.index.checked_add(1) == Some(tail_next)
			})
	};
	let refuse = |message: String| Error::new(ErrorKind::InvalidData, message);
	let below_tail = |z: &WalSegmentMeta| {
		refuse(format!(
			"checkpoint-only segment {} lies below the WAL tail {tail_next} and no durable \
			 snapshot boundary explains it",
			z.path.display()
		))
	};
	let not_exact = |target: &Path, z: &WalSegmentMeta| {
		refuse(format!(
			"{} is not exactly the checkpoint record of {}",
			target.display(),
			z.path.display()
		))
	};
	let held = |segment: &WalSegmentMeta| replay.checkpoint_only_holders.get(&segment.path);
	let target_path = wal_segment_path(wal_path, tail_next)?;
	// (Z's index, whether the target is to be published, and the record to publish.)
	let (z_at, publish): (usize, Option<Vec<u8>>) = if last.start_index == tail_next {
		let Some(z_at) = segments.len().checked_sub(2) else {
			return Ok(());
		};
		let z = &segments[z_at];
		if !named_checkpoint_only(z)? || z.start_index >= tail_next {
			return Ok(());
		}
		if !explained(z) {
			return Err(below_tail(z));
		}
		let target = held(&last)
			.filter(|target| {
				target.records == 1 && last.len_bytes == target.last_record.len() as u64
			})
			.ok_or_else(|| not_exact(&target_path, z))?;
		if !wholly_validated(file_ops, &last, target).await? {
			return Err(not_exact(&target_path, z));
		}
		let z_matches = match held(z) {
			Some(old) => {
				old.last_record == target.last_record && wholly_validated(file_ops, z, old).await?
			}
			None => exactly_empty(file_ops, z).await?,
		};
		if !z_matches {
			return Err(not_exact(&target_path, z));
		}
		(z_at, None)
	} else {
		let z_at = segments.len() - 1;
		let z = &last;
		if z.start_index < tail_next && !explained(z) {
			return Err(below_tail(z));
		}
		// Every byte must be a validated record: replay keeps a holder up to its last good record
		// even when a torn suffix follows, and that suffix is never carried or trimmed.
		let z_validated = match held(z) {
			Some(held) => wholly_validated(file_ops, z, held).await?,
			None => false,
		};
		let record = match held(z) {
			Some(held) if z_validated => Some(held.last_record.clone()),
			Some(_) => {
				return Err(refuse(format!(
					"checkpoint-only segment {} off the WAL tail {tail_next} holds bytes past its \
					 last validated checkpoint record",
					z.path.display()
				)));
			}
			None if z.start_index < tail_next && exactly_empty(file_ops, z).await? => None,
			None => {
				return Err(refuse(format!(
					"checkpoint-only segment {} off the WAL tail {tail_next} has no validated \
					 checkpoint record",
					z.path.display()
				)));
			}
		};
		let others: Vec<&WalSegmentMeta> = segments[..z_at]
			.iter()
			.filter(|segment| segment.start_index >= tail_next.min(z.start_index))
			.collect();
		match (others.as_slice(), &record) {
			([], _) => {
				if file_ops.file_len(&target_path).await?.is_some() {
					return Err(refuse(format!(
						"{} exists but replay did not account for it",
						target_path.display()
					)));
				}
				(z_at, record)
			}
			([target], Some(record)) if target.path == target_path => {
				let exact = match held(target) {
					Some(t) => {
						t.records == 1
							&& t.last_record == *record
							&& target.len_bytes == record.len() as u64
							&& wholly_validated(file_ops, target, t).await?
					}
					None => false,
				};
				if !exact {
					return Err(not_exact(&target_path, z));
				}
				(z_at, None)
			}
			_ => {
				return Err(refuse(format!(
					"unexpected segments between the WAL tail {tail_next} and {}",
					z.path.display()
				)));
			}
		}
	};
	let z = segments[z_at].clone();
	let target_at = if last.start_index == tail_next {
		Some(segments.len() - 1)
	} else {
		segments
			.iter()
			.position(|segment| segment.path == target_path)
	};
	// No other named checkpoint-only segment may precede these shapes.
	for (at, segment) in segments.iter().enumerate() {
		if at != z_at && Some(at) != target_at && named_checkpoint_only(segment)? {
			return Err(refuse(format!(
				"unexpected checkpoint-only segment {} beside {}",
				segment.path.display(),
				z.path.display()
			)));
		}
	}

	// Effects: the target is durable, contents and name, before Z goes.
	if let Some(record) = &publish {
		let mut page = AlignedBuf::new(record.len());
		page.as_mut_slice().copy_from_slice(record);
		create_wal_holder(file_ops, wal_path, tail_next, vec![page]).await?;
	} else if target_at.is_some() {
		let handle = open_existing_wal_segment(file_ops, &target_path).await?;
		file_ops.fdatasync(&handle).await?;
		fsync_parent_dir(file_ops, &target_path).await?;
	}
	unlink_wal_artifact(file_ops, &z.path).await?;
	if z_at + 1 == replay.wal_segments.len() {
		replay.last_segment_needs_truncate = false;
	}
	replay.wal_segments.remove(z_at);
	replay.checkpoint_only_holders.remove(&z.path);
	if let Some(record) = publish {
		replay.wal_segments.push(WalSegmentMeta {
			path: target_path.clone(),
			start_index: tail_next,
			end_log_index: None,
			len_bytes: record.len() as u64,
		});
		replay
			.wal_segments
			.sort_by_key(|segment| segment.start_index);
		replay.checkpoint_only_holders.insert(
			target_path,
			CheckpointOnlyHolder {
				records: 1,
				last_record: record,
			},
		);
	}
	Ok(())
}

/// Whether, given `recovery_checkpoint` is None, every checkpoint record on disk is known to be
/// superseded. Replay keeps the last record not superseded by the validated snapshot boundary
/// (strict: commit < boundary), so a None checkpoint after open means every record replay
/// validated had a commit below the recovered snapshot's boundary; a torn tail was cut to its
/// validated length at open. Every later checkpoint write sets the checkpoint, so it is still
/// None only if none was written. Without a recovered snapshot no record can be superseded, so a
/// None checkpoint then means there is no record at all, and a nonempty checkpoint-only segment
/// cannot exist; the answer is false so that such a segment is refused, not presumed empty.
fn records_superseded_at_open(recovered_snapshot: bool) -> bool {
	recovered_snapshot
}

/// Every byte of `segment` on disk is one of its replay-validated checkpoint records: the file is
/// exactly its validated length, and that length is its record count times the record size.
async fn wholly_validated(
	file_ops: &dyn AsyncFileOps,
	segment: &WalSegmentMeta,
	held: &CheckpointOnlyHolder,
) -> std::io::Result<bool> {
	let records_len = (held.records as u64).checked_mul(held.last_record.len() as u64);
	Ok(segment.end_log_index.is_none()
		&& records_len == Some(segment.len_bytes)
		&& file_ops.file_len(&segment.path).await? == Some(segment.len_bytes))
}

/// A segment of exactly zero length, in metadata and on disk: it carries no record at all.
async fn exactly_empty(
	file_ops: &dyn AsyncFileOps,
	segment: &WalSegmentMeta,
) -> std::io::Result<bool> {
	Ok(segment.len_bytes == 0 && file_ops.file_len(&segment.path).await? == Some(0))
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
	replay_wal_segment_bytes(&bytes, segment, snapshot, allow_torn_tail, replay)
}

fn replay_wal_segment_bytes(
	bytes: &[u8],
	segment: &DiscoveredWalSegment,
	snapshot: Option<&LoadedSnapshot>,
	allow_torn_tail: bool,
	replay: &mut WALReplay,
) -> std::io::Result<Option<WalSegmentMeta>> {
	if let Some(hash) = &mut replay.artifact_hasher {
		hash.update(&bytes);
		replay.artifact_bytes = replay
			.artifact_bytes
			.checked_add(bytes.len() as u64)
			.ok_or_else(|| Error::new(ErrorKind::InvalidData, "WAL input byte count overflow"))?;
	}

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
	let mut checkpoint_records: usize = 0;
	let mut last_checkpoint_record: Option<std::ops::Range<usize>> = None;
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
				checkpoint_records += 1;
				last_checkpoint_record = Some(offset..offset + padded_len);
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
	if first_log_entry_index.is_none()
		&& let Some(record) = last_checkpoint_record
	{
		replay.checkpoint_only_holders.insert(
			segment.path.clone(),
			CheckpointOnlyHolder {
				records: checkpoint_records,
				last_record: bytes[record].to_vec(),
			},
		);
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
		0 => crate::consensus::wal_body::v4::decode_entry(payload)
			.map_err(|err| wal_body_format_mismatch(wal_path, offset, 0, &err.to_string()))?,
		4 => crate::consensus::wal_body::v4::decode_entry(payload).map_err(|err| {
			wal_body_format_mismatch(wal_path, offset, header.format_version, &err.to_string())
		})?,
		5 => match postcard::take_from_bytes::<Entry<WendelonTypeConfig>>(payload) {
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
		version => {
			return Err(wal_body_format_mismatch(
				wal_path,
				offset,
				version,
				"unsupported WAL version",
			));
		}
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
		EntryPayload::Normal(WendelonRequest::Root2Command(_)) => (LogEntryKind::APPEND, 0),
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

/// Writes `state` to the build temporary file and syncs it. Publication (rename into `current`)
/// is the caller's, under the publication lock.
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
	file_ops.fsync(&handle).await
}

async fn decode_snapshot_file(
	file_ops: &dyn AsyncFileOps,
	path: &Path,
) -> std::io::Result<(SnapshotHeader, SmState)> {
	let (header, state, _, _) = decode_snapshot_file_with_identity(file_ops, path).await?;
	Ok((header, state))
}

async fn decode_snapshot_file_with_identity(
	file_ops: &dyn AsyncFileOps,
	path: &Path,
) -> std::io::Result<(SnapshotHeader, SmState, [u8; 32], u64)> {
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
		decode_snapshot_reader_with_identity(file)
			.map_err(|err| Error::new(ErrorKind::InvalidData, err))
	})
	.await
}

#[cfg(test)]
fn decode_snapshot_reader<R: Read>(reader: R) -> Result<(SnapshotHeader, SmState), String> {
	decode_snapshot_reader_with_identity(reader).map(|(header, state, _, _)| (header, state))
}

fn decode_snapshot_reader_with_identity<R: Read>(
	mut reader: R,
) -> Result<(SnapshotHeader, SmState, [u8; 32], u64), String> {
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
	let mut artifact = control_artifact_hash::Context::new();
	artifact.update(&header_bytes);
	artifact.update(&body);
	Ok((
		header,
		state,
		artifact.finish(),
		(header_bytes.len() + body.len()) as u64,
	))
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

	fn tempdir(tag: &str) -> crate::test_support::TestDir {
		crate::test_support::TestDir::new(&format!("wendelon-raft-recovery-{tag}"))
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

		async fn link_no_replace(&self, from: &Path, to: &Path) -> std::io::Result<()> {
			self.inner.link_no_replace(from, to).await
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

	/// Negative control for an injected torn write: the one named segment holds the first
	/// `torn_len` bytes of `entry`'s page at `offset`, and not the whole page. When the torn
	/// prefix went through the WAL's O_DIRECT handle, ext4 refused it and these bytes were absent.
	fn assert_torn_prefix_on_disk(
		dir: &Path,
		entry: &Entry<WendelonTypeConfig>,
		offset: usize,
		torn_len: usize,
	) {
		let page = serialize_entry_page(entry).unwrap();
		let page = page.as_slice();
		let prefix = &page[..torn_len];
		assert!(
			prefix.iter().any(|byte| *byte != 0),
			"the torn prefix must differ from a zero hole"
		);
		let segments = named_segment_paths(dir);
		assert_eq!(segments.len(), 1);
		let bytes = std::fs::read(&segments[0]).unwrap();
		assert!(
			bytes.len() >= offset + torn_len,
			"the segment must reach the torn prefix, has {} bytes",
			bytes.len()
		);
		assert_eq!(
			&bytes[offset..offset + torn_len],
			prefix,
			"the injected torn prefix must be on disk"
		);
		assert_ne!(
			bytes.get(offset..offset + page.len()),
			Some(page),
			"the injected write must be torn, not complete"
		);
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
		let file_ops = Arc::new(FaultyFileOps::new(dir.to_path_buf()).unwrap());
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
		assert_torn_prefix_on_disk(&dir, &lease_entry(2, 10, 19, 2), DIRECT_IO_ALIGN, 128);
		drop(store);

		file_ops.crash().unwrap();
		assert_torn_prefix_on_disk(&dir, &lease_entry(2, 10, 19, 2), DIRECT_IO_ALIGN, 128);

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
		let file_ops = Arc::new(FaultyFileOps::new(dir.to_path_buf()).unwrap());
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
		assert_torn_prefix_on_disk(&dir, &lease_entry(2, 10, 19, 2), DIRECT_IO_ALIGN, 128);
		drop(store);

		file_ops.crash().unwrap();
		assert_torn_prefix_on_disk(&dir, &lease_entry(2, 10, 19, 2), DIRECT_IO_ALIGN, 128);

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
		let file_ops = Arc::new(FaultyFileOps::new(dir.to_path_buf()).unwrap());
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
		let file_ops = Arc::new(FaultyFileOps::new(dir.to_path_buf()).unwrap());
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
		// The artifact is durable before the purge, as a publication would have recorded it.
		store.note_published_snapshot_for_test(log_id(1, 2)).await;

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

	/// A purge covered by a published snapshot keeps a replay proof that held
	/// before it. Without this, a replacement awaiting its proof stays fenced
	/// after its first runtime snapshot until a restart.
	#[tokio::test]
	async fn replay_proof_survives_a_purge_of_a_verified_log() {
		let dir = tempdir("replay-proof-purge");
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
		assert!(
			store.replacement_replay_base_verified().await,
			"premise: an uninterrupted log from 1 is verified"
		);
		let mut snapshot_state = SmState::default();
		snapshot_state.last_applied_log_id = Some(log_id(1, 2));
		write_snapshot_file(&snapshot_path, &snapshot_state);
		store.note_published_snapshot_for_test(log_id(1, 2)).await;
		store.purge(log_id(1, 2)).await.unwrap();
		assert!(
			store.replacement_replay_base_verified().await,
			"a purge of a verified log keeps its replay proof"
		);
		store
			.blocking_append(vec![lease_entry(4, 30, 39, 4)])
			.await
			.unwrap();
		assert!(store.replacement_replay_base_verified().await);
	}

	/// Runtime and reopen apply one rule: a retained prefix is explained only by
	/// a validated durable snapshot that covers it.
	#[tokio::test]
	async fn replay_proof_runtime_matches_reopen_for_a_covered_prefix() {
		let dir = tempdir("replay-proof-covered-prefix");
		let (_, _, snapshot_path) = artifact_paths(&dir);
		let mut store = open_store_with_segment_bytes(&dir, DIRECT_IO_ALIGN as u64).await;
		store
			.blocking_append(vec![lease_entry(5, 0, 9, 5), lease_entry(6, 10, 19, 6)])
			.await
			.unwrap();
		assert!(
			!store.replacement_replay_base_verified().await,
			"a prefix starting at 5 with no snapshot is unexplained"
		);
		let mut snapshot_state = SmState::default();
		snapshot_state.last_applied_log_id = Some(log_id(1, 5));
		write_snapshot_file(&snapshot_path, &snapshot_state);
		store.note_published_snapshot_for_test(log_id(1, 5)).await;
		store.purge(log_id(1, 5)).await.unwrap();
		let runtime = store.replacement_replay_base_verified().await;
		let reopened = open_store_with_segment_bytes(&dir, DIRECT_IO_ALIGN as u64).await;
		let after_reopen = reopened.replacement_replay_base_verified().await;
		assert!(
			after_reopen,
			"reopen treats the covering snapshot as the base"
		);
		assert_eq!(
			runtime, after_reopen,
			"a restart must not change the answer"
		);
	}

	/// Before a purge, the published snapshot covers entries the log still
	/// retains; that overlap is healthy and keeps the proof.
	#[tokio::test]
	async fn replay_proof_accepts_entries_a_published_snapshot_still_overlaps() {
		let dir = tempdir("replay-proof-overlap");
		let (_, _, snapshot_path) = artifact_paths(&dir);
		let mut store = open_store_with_segment_bytes(&dir, DIRECT_IO_ALIGN as u64).await;
		store
			.blocking_append(vec![
				lease_entry(1, 0, 9, 1),
				lease_entry(2, 10, 19, 2),
				lease_entry(3, 20, 29, 3),
				lease_entry(4, 30, 39, 4),
			])
			.await
			.unwrap();
		let mut snapshot_state = SmState::default();
		snapshot_state.last_applied_log_id = Some(log_id(1, 2));
		write_snapshot_file(&snapshot_path, &snapshot_state);
		store.note_published_snapshot_for_test(log_id(1, 2)).await;
		assert!(store.replacement_replay_base_verified().await);
	}

	/// Retained entries that start above the boundary leave an unexplained gap.
	#[tokio::test]
	async fn replay_proof_refuses_a_gap_above_the_published_boundary() {
		let dir = tempdir("replay-proof-gap");
		let (_, _, snapshot_path) = artifact_paths(&dir);
		let mut store = open_store_with_segment_bytes(&dir, DIRECT_IO_ALIGN as u64).await;
		store
			.blocking_append(vec![lease_entry(5, 0, 9, 5), lease_entry(6, 10, 19, 6)])
			.await
			.unwrap();
		let mut snapshot_state = SmState::default();
		snapshot_state.last_applied_log_id = Some(log_id(1, 2));
		write_snapshot_file(&snapshot_path, &snapshot_state);
		store.note_published_snapshot_for_test(log_id(1, 2)).await;
		assert!(!store.replacement_replay_base_verified().await);
		// A logical purge past the boundary empties the log; the gap stays unexplained.
		store.purge(log_id(1, 6)).await.unwrap();
		assert!(!store.replacement_replay_base_verified().await);
	}

	/// A failed publication refuses until reopen validates what is on disk.
	#[tokio::test]
	async fn replay_proof_refuses_while_publication_has_failed() {
		let dir = tempdir("replay-proof-failed-publication");
		let mut store = open_store_with_segment_bytes(&dir, DIRECT_IO_ALIGN as u64).await;
		store
			.blocking_append(vec![lease_entry(1, 0, 9, 1), lease_entry(2, 10, 19, 2)])
			.await
			.unwrap();
		assert!(
			store.replacement_replay_base_verified().await,
			"premise: an uninterrupted log from 1 is verified"
		);
		*store.publication.state.lock().await = PublicationState::Failed {
			durable: PublishedSnapshot::Artifact(None),
			step: "test failure",
		};
		assert!(!store.replacement_replay_base_verified().await);
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
		let mut sm = WendelonStateMachine::new().with_checkpoint_observer(coordinator.clone());

		// No timer yet: under the unreachable entry threshold, applying alone must
		// not checkpoint. With the timer already running, a slow apply would let
		// a tick flush first and this would be measuring the scheduler.
		append_and_apply(&mut store, &mut sm, finalize_entry(1, 111)).await;
		assert_eq!(store.recovery_checkpoint().await, None);

		// Only the timer can flush now. Wait for it rather than for a fixed
		// sleep a loaded host may not schedule within.
		let timer = coordinator.spawn();
		let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
		while store.recovery_checkpoint().await.is_none() {
			assert!(
				tokio::time::Instant::now() < deadline,
				"the timer never flushed the pending checkpoint"
			);
			tokio::time::sleep(Duration::from_millis(10)).await;
		}
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
		// The checkpoint-only segment was replaced by checkpoint + entry, not lost.
		assert_eq!(
			reopened_again.recovery_checkpoint().await,
			Some(RecoveryCheckpoint {
				commit_index: 1,
				e_safe_index: 1,
			})
		);
		check_segment_identities(&reopened_again.inner.read().await.wal_segments).unwrap();
	}

	// Checkpoint-rotation reproductions. Each asserts the invariant that must hold; before the
	// rotation fix they are expected to fail. Isolated temporary stores, small segments.

	fn entry_bytes(entries: &[Entry<WendelonTypeConfig>]) -> Vec<(u64, Vec<u8>)> {
		entries
			.iter()
			.map(|e| (e.log_id.index, postcard::to_allocvec(e).unwrap()))
			.collect()
	}

	/// R1: a checkpoint written while the committed index is behind the WAL tail must not leave a
	/// segment whose name disagrees with the next entry: every entry survives and the store reopens.
	#[tokio::test]
	async fn checkpoint_rotation_behind_tail_keeps_the_store_reopenable() {
		let dir = tempdir("checkpoint-rotation-behind-tail");
		let segment = 2 * DIRECT_IO_ALIGN as u64;
		let mut store = try_open_store_with_segment_bytes(&dir, segment)
			.await
			.unwrap();
		store
			.blocking_append(vec![finalize_entry(1, 111), finalize_entry(2, 222)])
			.await
			.unwrap();
		store.save_committed(Some(log_id(1, 1))).await.unwrap();
		store
			.blocking_append(vec![finalize_entry(3, 333)])
			.await
			.unwrap();
		let before = entry_bytes(&store.try_get_log_entries(0..=u64::MAX).await.unwrap());
		drop(store);
		let mut reopened = try_open_store_with_segment_bytes(&dir, segment)
			.await
			.expect("the store reopens after a checkpoint rotation behind the tail");
		let after = entry_bytes(&reopened.try_get_log_entries(0..=u64::MAX).await.unwrap());
		assert_eq!(after, before);
		assert_eq!(
			after.iter().map(|(i, _)| *i).collect::<Vec<_>>(),
			vec![1, 2, 3]
		);
	}

	/// R2: a checkpoint rotation must never replace a segment that already holds flushed entries.
	/// The evidence is durable-flush evidence (entry bytes and file inventory), not a claim about
	/// client acknowledgement or quorum commitment.
	#[tokio::test]
	async fn checkpoint_rotation_never_replaces_a_flushed_segment() {
		let dir = tempdir("checkpoint-rotation-clash");
		let segment = 2 * DIRECT_IO_ALIGN as u64;
		let mut store = try_open_store_with_segment_bytes(&dir, segment)
			.await
			.unwrap();
		store
			.blocking_append(
				(1..=4)
					.map(|i| finalize_entry(i, 100 * i))
					.collect::<Vec<_>>(),
			)
			.await
			.unwrap();
		let flushed = entry_bytes(&store.try_get_log_entries(0..=u64::MAX).await.unwrap());
		let segments_before = named_segment_paths(&dir);
		let files_before: Vec<Vec<u8>> = segments_before
			.iter()
			.map(|p| std::fs::read(p).unwrap())
			.collect();
		store.save_committed(Some(log_id(1, 2))).await.unwrap();
		for (path, bytes) in segments_before.iter().zip(&files_before) {
			let now = std::fs::read(path).unwrap();
			assert!(
				now.starts_with(bytes),
				"{} was replaced, not extended",
				path.display()
			);
		}
		drop(store);
		let mut reopened = try_open_store_with_segment_bytes(&dir, segment)
			.await
			.expect("the store reopens");
		let after = entry_bytes(&reopened.try_get_log_entries(0..=u64::MAX).await.unwrap());
		assert_eq!(
			after, flushed,
			"every flushed entry survives with identical bytes"
		);
	}

	/// R3: rolling over a full checkpoint-only segment must keep the recoverable commit boundary.
	#[tokio::test]
	async fn checkpoint_only_rollover_keeps_the_commit_boundary() {
		let dir = tempdir("checkpoint-only-rollover");
		let segment = DIRECT_IO_ALIGN as u64;
		let mut store = try_open_store_with_segment_bytes(&dir, segment)
			.await
			.unwrap();
		store
			.blocking_append(vec![finalize_entry(1, 111)])
			.await
			.unwrap();
		store.save_committed(Some(log_id(1, 1))).await.unwrap();
		store
			.blocking_append(vec![finalize_entry(2, 222)])
			.await
			.unwrap();
		drop(store);
		let mut reopened = try_open_store_with_segment_bytes(&dir, segment)
			.await
			.expect("the store reopens");
		assert_eq!(
			reopened
				.try_get_log_entries(0..=u64::MAX)
				.await
				.unwrap()
				.iter()
				.map(|e| e.log_id.index)
				.collect::<Vec<_>>(),
			vec![1, 2]
		);
		assert_eq!(
			reopened.recovery_checkpoint().await.map(|c| c.commit_index),
			Some(1),
			"the durable commit boundary survives the rollover"
		);
		assert_eq!(reopened.read_committed().await.unwrap(), Some(log_id(1, 1)));
	}

	/// R4: an apply observation older than an already-saved commit must not move the commit pointer
	/// backwards, in either checkpoint branch, and e_safe never exceeds the commit.
	#[tokio::test]
	async fn delayed_older_observation_does_not_regress_the_commit_pointer() {
		for advance_e_safe in [false, true] {
			let dir = tempdir("delayed-observation");
			let mut store = try_open_store_with_segment_bytes(&dir, 64 * DIRECT_IO_ALIGN as u64)
				.await
				.unwrap();
			store
				.blocking_append(vec![finalize_entry(1, 111), finalize_entry(2, 222)])
				.await
				.unwrap();
			store.save_committed(Some(log_id(1, 2))).await.unwrap();
			store
				.write_recovery_checkpoint(CheckpointObservation {
					last_log_id: Some(log_id(1, 1)),
					checkpoint: RecoveryCheckpoint {
						commit_index: 1,
						e_safe_index: u64::from(advance_e_safe),
					},
					force: true,
				})
				.await
				.unwrap();
			assert_eq!(
				store.read_committed().await.unwrap(),
				Some(log_id(1, 2)),
				"in-memory pointer, e_safe advanced: {advance_e_safe}"
			);
			let checkpoint = store.recovery_checkpoint().await.unwrap();
			assert_eq!(checkpoint.commit_index, 2);
			assert!(checkpoint.e_safe_index <= checkpoint.commit_index);
			drop(store);
			let mut reopened = try_open_store_with_segment_bytes(&dir, 64 * DIRECT_IO_ALIGN as u64)
				.await
				.unwrap();
			assert_eq!(reopened.read_committed().await.unwrap(), Some(log_id(1, 2)));
			let checkpoint = reopened.recovery_checkpoint().await.unwrap();
			assert_eq!(checkpoint.commit_index, 2);
			assert_eq!(checkpoint.e_safe_index, u64::from(advance_e_safe));
		}
	}

	// Rotation-fix regressions (X1-X4, I1/I2, poison). Isolated temporary stores.

	fn big_entry(index: u64, bytes: usize) -> Entry<WendelonTypeConfig> {
		Entry {
			log_id: log_id(1, index),
			payload: EntryPayload::Normal(WendelonRequest::SchemaUpdate(vec![0xAB; bytes])),
		}
	}

	async fn segment_metadata(store: &WendelonLogStore) -> Vec<(u64, Option<u64>, u64)> {
		let guard = store.inner.read().await;
		check_segment_identities(&guard.wal_segments).unwrap();
		guard
			.wal_segments
			.iter()
			.map(|s| (s.start_index, s.end_log_index, s.len_bytes))
			.collect()
	}

	/// N1/X2: appending into a full checkpoint-only segment keeps one metadata entry for that file,
	/// writes the checkpoint before the entry, and survives reopen with both.
	#[tokio::test]
	async fn full_checkpoint_only_segment_takes_checkpoint_then_entry() {
		let dir = tempdir("checkpoint-only-replacement");
		let page = DIRECT_IO_ALIGN as u64;
		let mut store = try_open_store_with_segment_bytes(&dir, page).await.unwrap();
		store
			.blocking_append(vec![finalize_entry(1, 111)])
			.await
			.unwrap();
		store.save_committed(Some(log_id(1, 1))).await.unwrap();
		assert_eq!(
			segment_metadata(&store).await,
			vec![(1, Some(1), page), (2, None, page)]
		);
		store
			.blocking_append(vec![finalize_entry(2, 222)])
			.await
			.unwrap();
		assert_eq!(
			segment_metadata(&store).await,
			vec![(1, Some(1), page), (2, Some(2), 2 * page)]
		);
		let pointer = store.inner.read().await.entry_positions[&2];
		assert_eq!(
			(
				pointer.segment_start_index,
				pointer.offset,
				pointer.len_bytes
			),
			(2, page, page)
		);
		assert_eq!(named_segment_paths(&dir).len(), 2);
		let before = entry_bytes(&store.try_get_log_entries(0..=u64::MAX).await.unwrap());
		drop(store);
		let mut reopened = try_open_store_with_segment_bytes(&dir, page).await.unwrap();
		assert_eq!(
			entry_bytes(&reopened.try_get_log_entries(0..=u64::MAX).await.unwrap()),
			before
		);
		assert_eq!(
			reopened.recovery_checkpoint().await.map(|c| c.commit_index),
			Some(1)
		);
		assert_eq!(reopened.read_committed().await.unwrap(), Some(log_id(1, 1)));
	}

	/// X2 with a full-size multi-page entry in a batch that then rotates: the replacement segment
	/// may exceed segment_bytes by the one checkpoint page, offsets are exact, and reopen agrees.
	#[tokio::test]
	async fn multi_page_entry_replaces_checkpoint_only_segment_within_a_batch() {
		let dir = tempdir("checkpoint-only-multi-page");
		let page = DIRECT_IO_ALIGN as u64;
		let segment = 2 * page;
		let mut store = try_open_store_with_segment_bytes(&dir, segment)
			.await
			.unwrap();
		store
			.blocking_append(vec![big_entry(1, 6000)])
			.await
			.unwrap();
		store.save_committed(Some(log_id(1, 1))).await.unwrap();
		assert_eq!(
			segment_metadata(&store).await,
			vec![(1, Some(1), segment), (2, None, page)]
		);
		store
			.blocking_append(vec![big_entry(2, 6000), finalize_entry(3, 333)])
			.await
			.unwrap();
		assert_eq!(
			segment_metadata(&store).await,
			vec![
				(1, Some(1), segment),
				(2, Some(2), page + segment),
				(3, Some(3), page)
			]
		);
		let positions = store.inner.read().await.entry_positions.clone();
		assert_eq!(
			(
				positions[&2].segment_start_index,
				positions[&2].offset,
				positions[&2].len_bytes
			),
			(2, page, segment)
		);
		assert_eq!(
			(positions[&3].segment_start_index, positions[&3].offset),
			(3, 0)
		);
		let before = entry_bytes(&store.try_get_log_entries(0..=u64::MAX).await.unwrap());
		drop(store);
		let mut reopened = try_open_store_with_segment_bytes(&dir, segment)
			.await
			.unwrap();
		assert_eq!(
			entry_bytes(&reopened.try_get_log_entries(0..=u64::MAX).await.unwrap()),
			before
		);
		assert_eq!(
			reopened.recovery_checkpoint().await.map(|c| c.commit_index),
			Some(1)
		);
	}

	/// X3: segment creation never replaces an existing file.
	#[tokio::test]
	async fn create_wal_segment_never_replaces_an_existing_name() {
		let dir = tempdir("segment-no-replace");
		let wal = dir.join("control.wal");
		let existing = wal_segment_path(&wal, 5).unwrap();
		std::fs::write(&existing, b"existing segment bytes").unwrap();
		let err = match create_wal_segment(&BlockingFileOps, &wal, 5).await {
			Ok(_) => panic!("an existing segment name must be refused"),
			Err(err) => err,
		};
		assert_eq!(err.kind(), ErrorKind::AlreadyExists);
		assert_eq!(std::fs::read(&existing).unwrap(), b"existing segment bytes");
	}

	/// X4: every source that knows the committed index must agree; none means unknown.
	#[tokio::test]
	async fn committed_pointer_sources_must_agree() {
		let a = log_id(1, 4);
		assert_eq!(
			resolve_committed_pointer(4, &[Some(a), None, Some(a)]).unwrap(),
			a
		);
		assert_eq!(
			resolve_committed_pointer(4, &[Some(a), Some(log_id(2, 4))])
				.unwrap_err()
				.kind(),
			ErrorKind::InvalidData
		);
		assert_eq!(
			resolve_committed_pointer(4, &[Some(log_id(1, 3)), None])
				.unwrap_err()
				.kind(),
			ErrorKind::InvalidData
		);
		// Genesis comes from a real source at index 0, never from the number alone.
		let genesis = LogId::default();
		assert_eq!(
			resolve_committed_pointer(0, &[Some(genesis)]).unwrap(),
			genesis
		);
		assert!(resolve_committed_pointer(0, &[None]).is_err());

		let dir = tempdir("checkpoint-observation-validation");
		let mut store = try_open_store_with_segment_bytes(&dir, 64 * DIRECT_IO_ALIGN as u64)
			.await
			.unwrap();
		store
			.blocking_append(vec![finalize_entry(1, 111), finalize_entry(2, 222)])
			.await
			.unwrap();
		let mismatched = store
			.write_recovery_checkpoint(CheckpointObservation {
				last_log_id: Some(log_id(1, 2)),
				checkpoint: RecoveryCheckpoint {
					commit_index: 1,
					e_safe_index: 0,
				},
				force: true,
			})
			.await
			.unwrap_err();
		assert_eq!(mismatched.kind(), ErrorKind::InvalidData);
		let unsafe_e = store
			.write_recovery_checkpoint(CheckpointObservation {
				last_log_id: Some(log_id(1, 2)),
				checkpoint: RecoveryCheckpoint {
					commit_index: 2,
					e_safe_index: 3,
				},
				force: true,
			})
			.await
			.unwrap_err();
		assert_eq!(unsafe_e.kind(), ErrorKind::InvalidData);
		// A same-index identity that disagrees with the retained entry is refused.
		let disagreeing = store
			.write_recovery_checkpoint(CheckpointObservation {
				last_log_id: Some(log_id(7, 2)),
				checkpoint: RecoveryCheckpoint {
					commit_index: 2,
					e_safe_index: 0,
				},
				force: true,
			})
			.await
			.unwrap_err();
		assert_eq!(disagreeing.kind(), ErrorKind::InvalidData);
		assert_eq!(
			store.recovery_checkpoint().await,
			None,
			"pre-effect refusals write nothing"
		);
	}

	/// The in-memory store keeps its pre-change behavior: a changed checkpoint is Unsupported,
	/// an unchanged one only moves the pointer.
	#[tokio::test]
	async fn in_memory_store_checkpoint_behavior_is_unchanged() {
		let store = WendelonLogStore::in_memory();
		let changed = store
			.write_recovery_checkpoint(CheckpointObservation {
				last_log_id: Some(log_id(1, 2)),
				checkpoint: RecoveryCheckpoint {
					commit_index: 2,
					e_safe_index: 0,
				},
				force: false,
			})
			.await
			.unwrap_err();
		assert_eq!(changed.kind(), ErrorKind::Unsupported);
		store
			.write_recovery_checkpoint(CheckpointObservation {
				last_log_id: Some(LogId::default()),
				checkpoint: RecoveryCheckpoint::default(),
				force: false,
			})
			.await
			.unwrap();
		assert_eq!(
			store.inner.read().await.recovery_committed,
			Some(LogId::default())
		);
	}

	/// D: after an uncertain failure every WAL mutator refuses without touching the disk, and a
	/// reopen rebuilds a contiguous store from what is on disk.
	#[tokio::test]
	async fn poison_after_uncertain_failure_refuses_every_mutator_without_io() {
		let dir = tempdir("wal-poison");
		let file_ops = Arc::new(FaultyFileOps::new(dir.to_path_buf()).unwrap());
		let mut store =
			open_store_with_faults(&dir, file_ops.clone(), DIRECT_IO_ALIGN as u64).await;
		store
			.blocking_append(vec![finalize_entry(1, 111)])
			.await
			.unwrap();
		file_ops.inject(InjectedFault::on(
			FaultOp::Fdatasync,
			FaultAction::Error {
				kind: ErrorKind::Other,
				message: "injected fdatasync failure".into(),
			},
		));
		assert!(
			store
				.blocking_append(vec![finalize_entry(2, 222)])
				.await
				.is_err()
		);
		assert!(store.inner.read().await.poisoned.is_some());
		let files = named_segment_paths(&dir)
			.iter()
			.map(|p| (p.clone(), std::fs::read(p).unwrap()))
			.collect::<Vec<_>>();
		assert!(
			store
				.blocking_append(vec![finalize_entry(3, 333)])
				.await
				.is_err()
		);
		assert!(store.save_committed(Some(log_id(1, 1))).await.is_err());
		assert!(store.truncate(log_id(1, 1)).await.is_err());
		assert!(store.purge(log_id(1, 1)).await.is_err());
		assert!(
			store
				.write_recovery_checkpoint(CheckpointObservation {
					last_log_id: Some(log_id(1, 1)),
					checkpoint: RecoveryCheckpoint {
						commit_index: 1,
						e_safe_index: 0,
					},
					force: true,
				})
				.await
				.is_err()
		);
		let after = named_segment_paths(&dir)
			.iter()
			.map(|p| (p.clone(), std::fs::read(p).unwrap()))
			.collect::<Vec<_>>();
		assert_eq!(after, files, "a poisoned store does no WAL I/O");
		drop(store);
		let mut reopened =
			open_store_with_faults(&dir, file_ops.clone(), DIRECT_IO_ALIGN as u64).await;
		let indexes = reopened
			.try_get_log_entries(0..=u64::MAX)
			.await
			.unwrap()
			.iter()
			.map(|e| e.log_id.index)
			.collect::<Vec<_>>();
		assert!(indexes == vec![1] || indexes == vec![1, 2], "{indexes:?}");
	}

	/// D/X2: a replacement whose rename succeeded but whose directory sync failed poisons the
	/// store; the published replacement is what a reopen finds, and nothing writes the old inode.
	#[tokio::test]
	async fn replacement_directory_sync_failure_poisons_the_store() {
		let dir = tempdir("replacement-dir-sync");
		let file_ops = Arc::new(FaultyFileOps::new(dir.to_path_buf()).unwrap());
		let mut store =
			open_store_with_faults(&dir, file_ops.clone(), DIRECT_IO_ALIGN as u64).await;
		store
			.blocking_append(vec![finalize_entry(1, 111)])
			.await
			.unwrap();
		store.save_committed(Some(log_id(1, 1))).await.unwrap();
		file_ops.inject(InjectedFault::on(
			FaultOp::FsyncDir,
			FaultAction::Error {
				kind: ErrorKind::Other,
				message: "injected directory sync failure".into(),
			},
		));
		assert!(
			store
				.blocking_append(vec![finalize_entry(2, 222)])
				.await
				.is_err()
		);
		assert_eq!(
			store.inner.read().await.poisoned.map(|p| (p.op, p.step)),
			Some(("append", "replace"))
		);
		assert!(store.save_committed(Some(log_id(1, 1))).await.is_err());
		assert!(
			store
				.blocking_append(vec![finalize_entry(2, 222)])
				.await
				.is_err()
		);
		drop(store);
		let mut reopened =
			open_store_with_faults(&dir, file_ops.clone(), DIRECT_IO_ALIGN as u64).await;
		assert_eq!(
			reopened
				.try_get_log_entries(0..=u64::MAX)
				.await
				.unwrap()
				.iter()
				.map(|e| e.log_id.index)
				.collect::<Vec<_>>(),
			vec![1, 2]
		);
		assert_eq!(
			reopened.recovery_checkpoint().await.map(|c| c.commit_index),
			Some(1)
		);
	}

	// B r3, P1, P2: crash-safe truncate and purge, and open-time completion. A stop is an injected
	// error at one occurrence of one syscall kind. After a stop the store is reopened from the same
	// page cache, and again after `crash` keeps only what was synced. This is same-page-cache
	// interruption evidence with a synced-filesystem model, not physical power-loss testing.

	const WAL_PAGE: u64 = DIRECT_IO_ALIGN as u64;
	const STOP_OPS: [FaultOp; 6] = [
		FaultOp::WriteAt,
		FaultOp::Fdatasync,
		FaultOp::Fsync,
		FaultOp::Link,
		FaultOp::Unlink,
		FaultOp::FsyncDir,
	];

	#[derive(Clone, Copy, Debug)]
	enum Mutation {
		Truncate(u64),
		Purge(u64),
	}

	async fn mutate(
		store: &mut WendelonLogStore,
		mutation: Mutation,
	) -> Result<(), StorageError<u32>> {
		match mutation {
			Mutation::Truncate(k) => store.truncate(log_id(1, k)).await,
			Mutation::Purge(p) => store.purge(log_id(1, p)).await,
		}
	}

	/// What a store recovers: its entries (index and bytes), its checkpoint, and every named
	/// segment file as (start, length).
	#[derive(Clone, Debug, PartialEq, Eq)]
	struct Recovered {
		entries: Vec<(u64, Vec<u8>)>,
		checkpoint: Option<RecoveryCheckpoint>,
		files: Vec<(u64, u64)>,
	}

	fn segment_files(dir: &Path) -> Vec<(u64, u64)> {
		let mut out: Vec<(u64, u64)> = named_segment_paths(dir)
			.iter()
			.map(|path| {
				let name = path.file_name().unwrap().to_str().unwrap();
				let start = name
					.trim_start_matches("control.wal.")
					.trim_end_matches(".seg")
					.parse()
					.unwrap();
				(start, std::fs::metadata(path).unwrap().len())
			})
			.collect();
		out.sort_unstable();
		out
	}

	fn artifact_bytes(dir: &Path) -> Vec<(String, Vec<u8>)> {
		let mut out: Vec<(String, Vec<u8>)> = std::fs::read_dir(dir)
			.unwrap()
			.map(|entry| {
				let entry = entry.unwrap();
				(
					entry.file_name().into_string().unwrap(),
					std::fs::read(entry.path()).unwrap(),
				)
			})
			.collect();
		out.sort();
		out
	}

	async fn recovered(store: &mut WendelonLogStore, dir: &Path) -> Recovered {
		Recovered {
			entries: entry_bytes(&store.try_get_log_entries(0..=u64::MAX).await.unwrap()),
			checkpoint: store.recovery_checkpoint().await,
			files: segment_files(dir),
		}
	}

	async fn reopen(dir: &Path, segment_bytes: u64, label: &str) -> (WendelonLogStore, Recovered) {
		let mut store = try_open_store_with_segment_bytes(dir, segment_bytes)
			.await
			.unwrap_or_else(|e| panic!("{label}: reopen: {e}"));
		let seen = recovered(&mut store, dir).await;
		(store, seen)
	}

	async fn try_open_faulty(
		dir: &Path,
		file_ops: Arc<FaultyFileOps>,
		segment_bytes: u64,
	) -> std::io::Result<WendelonLogStore> {
		let (wal, vote, snapshot) = artifact_paths(dir);
		WendelonLogStore::open_with_file_ops_and_options(
			&wal,
			&vote,
			&snapshot,
			file_ops,
			WalOptions { segment_bytes },
		)
		.await
	}

	fn stop_at(op: FaultOp, occurrence: usize) -> InjectedFault {
		InjectedFault::on(
			op,
			FaultAction::Error {
				kind: ErrorKind::Other,
				message: format!("stop at {op:?} #{occurrence}"),
			},
		)
		.after_matches(occurrence)
	}

	fn copy_store(seed: &Path) -> crate::test_support::TestDir {
		let dir = tempdir("stop");
		copy_flat_artifacts(seed, &dir, None);
		dir
	}

	async fn checkpoint(store: &WendelonLogStore, commit: u64, e_safe: u64) {
		store
			.write_recovery_checkpoint(CheckpointObservation {
				last_log_id: Some(log_id(1, commit)),
				checkpoint: RecoveryCheckpoint {
					commit_index: commit,
					e_safe_index: e_safe,
				},
				force: true,
			})
			.await
			.unwrap();
	}

	fn snapshot_at(dir: &Path, index: u64) {
		let mut state = SmState::default();
		state.last_applied_log_id = Some(log_id(1, index));
		write_snapshot_file(&artifact_paths(dir).2, &state);
	}

	fn segment_file(dir: &Path, start: u64) -> PathBuf {
		wal_segment_path(&artifact_paths(dir).0, start).unwrap()
	}

	fn assert_contiguous_prefix(
		seen: &[(u64, Vec<u8>)],
		original: &[(u64, Vec<u8>)],
		at_least: usize,
		label: &str,
	) {
		assert!(
			seen.len() >= at_least && seen.len() <= original.len(),
			"{label}: {} entries recovered",
			seen.len()
		);
		assert_eq!(
			seen,
			&original[..seen.len()],
			"{label}: a contiguous prefix with identical bytes"
		);
	}

	/// One stop: where, what each reopen recovered, and what the completed retry recovered.
	struct Stop {
		label: String,
		after_stops: Vec<Recovered>,
		after_retry: Recovered,
	}

	/// Runs `mutation` on a copy of `seed`, stopped at every occurrence of every syscall kind
	/// until an occurrence is never reached. `install`, when given, is a snapshot artifact placed
	/// and synced after open and before the mutation: an install the store has not seen. Each stop
	/// must fail the mutation and poison the store (a second attempt refuses with no file change).
	/// The copy is then reopened; for `rounds` > 1 the retry is stopped again at the same point and
	/// reopened, before a final retry that must complete.
	async fn stops(
		seed: &Path,
		segment_bytes: u64,
		mutation: Mutation,
		install: Option<(&Path, u64)>,
		rounds: usize,
	) -> Vec<Stop> {
		let mut out = Vec::new();
		for op in STOP_OPS {
			for occurrence in 0.. {
				let mut reached = false;
				for crash in [false, true] {
					let label = format!(
						"{mutation:?} stopped at {op:?} #{occurrence}{}",
						if crash { " then crash" } else { "" }
					);
					let dir = copy_store(seed);
					let mut after_stops = Vec::new();
					for round in 0..rounds {
						let file_ops = Arc::new(FaultyFileOps::new(dir.to_path_buf()).unwrap());
						let mut store = try_open_faulty(&dir, file_ops.clone(), segment_bytes)
							.await
							.unwrap_or_else(|e| panic!("{label}: open in round {round}: {e}"));
						if round == 0
							&& let Some((snapshot, last_included)) = install
						{
							std::fs::copy(snapshot, artifact_paths(&dir).2).unwrap();
							file_ops.fsync_dir(&dir).await.unwrap();
							store
								.note_published_snapshot_for_test(log_id(1, last_included))
								.await;
						}
						file_ops.inject(stop_at(op, occurrence));
						if mutate(&mut store, mutation).await.is_ok() {
							break;
						}
						let files = artifact_bytes(&dir);
						assert!(
							mutate(&mut store, mutation).await.is_err(),
							"{label}: the poisoned store refuses"
						);
						assert_eq!(
							artifact_bytes(&dir),
							files,
							"{label}: a refused mutation has no effect"
						);
						drop(store);
						if crash {
							file_ops.crash().unwrap();
						}
						after_stops.push(reopen(&dir, segment_bytes, &label).await.1);
					}
					if after_stops.is_empty() {
						break;
					}
					reached = true;
					let (mut store, _) = reopen(&dir, segment_bytes, &label).await;
					mutate(&mut store, mutation)
						.await
						.unwrap_or_else(|e| panic!("{label}: retry: {e}"));
					drop(store);
					let after_retry = reopen(&dir, segment_bytes, &label).await.1;
					out.push(Stop {
						label,
						after_stops,
						after_retry,
					});
				}
				if !reached {
					break;
				}
			}
		}
		out
	}

	/// Segments of three pages: [1,2,3] [C_old,4,5] [6,C,7], C_old = {1,0}, C = {2,1}.
	async fn seed_older_checkpoint_in_suffix() -> crate::test_support::TestDir {
		let dir = tempdir("truncate-older-checkpoint");
		let mut store = try_open_store_with_segment_bytes(&dir, 3 * WAL_PAGE)
			.await
			.unwrap();
		store
			.blocking_append(
				(1..=3)
					.map(|i| finalize_entry(i, 100 * i))
					.collect::<Vec<_>>(),
			)
			.await
			.unwrap();
		checkpoint(&store, 1, 0).await;
		store
			.blocking_append(vec![finalize_entry(4, 400), finalize_entry(5, 500)])
			.await
			.unwrap();
		store
			.blocking_append(vec![finalize_entry(6, 600)])
			.await
			.unwrap();
		checkpoint(&store, 2, 1).await;
		store
			.blocking_append(vec![finalize_entry(7, 700)])
			.await
			.unwrap();
		assert_eq!(
			segment_metadata(&store).await,
			vec![
				(1, Some(3), 3 * WAL_PAGE),
				(4, Some(5), 3 * WAL_PAGE),
				(6, Some(7), 3 * WAL_PAGE)
			]
		);
		dir
	}

	#[tokio::test]
	async fn truncate_with_older_checkpoint_in_surviving_suffix_keeps_latest() {
		let seed = seed_older_checkpoint_in_suffix().await;
		let original = reopen(&seed, 3 * WAL_PAGE, "seed").await.1;
		let c = Some(RecoveryCheckpoint {
			commit_index: 2,
			e_safe_index: 1,
		});
		assert_eq!(original.checkpoint, c);
		let all = stops(&seed, 3 * WAL_PAGE, Mutation::Truncate(3), None, 1).await;
		for op in [
			"Unlink #0",
			"Unlink #1",
			"Unlink #2",
			"FsyncDir #2",
			"FsyncDir #3",
		] {
			assert!(
				all.iter().any(|stop| stop.label.contains(op)),
				"a stop at {op}"
			);
		}
		for stop in &all {
			for seen in &stop.after_stops {
				assert_eq!(
					seen.checkpoint, c,
					"{}: the latest checkpoint wins",
					stop.label
				);
				assert_contiguous_prefix(&seen.entries, &original.entries, 2, &stop.label);
			}
			assert_eq!(stop.after_retry.checkpoint, c, "{}", stop.label);
			assert_eq!(
				stop.after_retry.entries,
				original.entries[..2].to_vec(),
				"{}",
				stop.label
			);
			assert_eq!(
				stop.after_retry.files,
				vec![(1, 2 * WAL_PAGE), (3, WAL_PAGE)],
				"{}",
				stop.label
			);
		}
	}

	#[tokio::test]
	async fn truncate_first_segment_holds_k_to_m_then_more() {
		let seed = tempdir("truncate-first-segment");
		let mut store = try_open_store_with_segment_bytes(&seed, 3 * WAL_PAGE)
			.await
			.unwrap();
		store
			.blocking_append(
				(1..=5)
					.map(|i| finalize_entry(i, 100 * i))
					.collect::<Vec<_>>(),
			)
			.await
			.unwrap();
		checkpoint(&store, 1, 1).await;
		assert_eq!(
			segment_metadata(&store).await,
			vec![(1, Some(3), 3 * WAL_PAGE), (4, Some(5), 3 * WAL_PAGE)]
		);
		drop(store);
		let original = reopen(&seed, 3 * WAL_PAGE, "seed").await.1;
		let c = Some(RecoveryCheckpoint {
			commit_index: 1,
			e_safe_index: 1,
		});
		for stop in stops(&seed, 3 * WAL_PAGE, Mutation::Truncate(2), None, 1).await {
			for seen in &stop.after_stops {
				assert_eq!(seen.checkpoint, c, "{}", stop.label);
				assert_contiguous_prefix(&seen.entries, &original.entries, 1, &stop.label);
			}
			assert_eq!(stop.after_retry.checkpoint, c, "{}", stop.label);
			assert_eq!(stop.after_retry.entries, original.entries[..1].to_vec());
			assert_eq!(
				stop.after_retry.files,
				vec![(1, WAL_PAGE), (2, WAL_PAGE)],
				"{}",
				stop.label
			);
		}
	}

	#[tokio::test]
	async fn truncate_without_checkpoint_obeys_poison_and_prefix() {
		let seed = tempdir("truncate-no-checkpoint");
		let mut store = try_open_store_with_segment_bytes(&seed, 2 * WAL_PAGE)
			.await
			.unwrap();
		store
			.blocking_append(
				(1..=5)
					.map(|i| finalize_entry(i, 100 * i))
					.collect::<Vec<_>>(),
			)
			.await
			.unwrap();
		assert_eq!(store.recovery_checkpoint().await, None);
		drop(store);
		let original = reopen(&seed, 2 * WAL_PAGE, "seed").await.1;
		let all = stops(&seed, 2 * WAL_PAGE, Mutation::Truncate(2), None, 1).await;
		assert!(all.iter().any(|stop| stop.label.contains("Unlink #1")));
		for stop in all {
			for seen in &stop.after_stops {
				assert_eq!(seen.checkpoint, None, "{}", stop.label);
				assert_contiguous_prefix(&seen.entries, &original.entries, 1, &stop.label);
			}
			assert_eq!(stop.after_retry.entries, original.entries[..1].to_vec());
			assert_eq!(
				stop.after_retry.files,
				vec![(1, WAL_PAGE)],
				"{}",
				stop.label
			);
		}
	}

	#[tokio::test]
	async fn truncate_noop_when_nothing_at_or_above_k() {
		let dir = tempdir("truncate-noop");
		let mut store = try_open_store_with_segment_bytes(&dir, 2 * WAL_PAGE)
			.await
			.unwrap();
		store
			.blocking_append(
				(1..=3)
					.map(|i| finalize_entry(i, 100 * i))
					.collect::<Vec<_>>(),
			)
			.await
			.unwrap();
		checkpoint(&store, 2, 2).await;
		let files = artifact_bytes(&dir);
		let before = recovered(&mut store, &dir).await;
		store.truncate(log_id(1, 4)).await.unwrap();
		assert_eq!(artifact_bytes(&dir), files, "no file effect");
		assert_eq!(recovered(&mut store, &dir).await, before);
		// A committed entry is never truncated: refused before any effect, without poison.
		assert!(store.truncate(log_id(1, 2)).await.is_err());
		assert_eq!(artifact_bytes(&dir), files);
		assert!(store.inner.read().await.poisoned.is_none());
	}

	#[tokio::test]
	async fn truncate_retry_after_each_stop_is_bounded() {
		let seed = tempdir("truncate-retry-bounded");
		let mut store = try_open_store_with_segment_bytes(&seed, WAL_PAGE)
			.await
			.unwrap();
		store
			.blocking_append(vec![finalize_entry(1, 100)])
			.await
			.unwrap();
		checkpoint(&store, 1, 1).await;
		store
			.blocking_append(vec![finalize_entry(2, 200)])
			.await
			.unwrap();
		store
			.blocking_append(vec![finalize_entry(3, 300)])
			.await
			.unwrap();
		store
			.blocking_append(vec![finalize_entry(4, 400)])
			.await
			.unwrap();
		checkpoint(&store, 2, 2).await;
		// Segment 2 is X2-extended (checkpoint then entry): one page past the segment size.
		assert_eq!(
			segment_metadata(&store).await,
			vec![
				(1, Some(1), WAL_PAGE),
				(2, Some(2), 2 * WAL_PAGE),
				(3, Some(3), WAL_PAGE),
				(4, Some(4), WAL_PAGE),
				(5, None, WAL_PAGE)
			]
		);
		drop(store);
		let original = reopen(&seed, WAL_PAGE, "seed").await.1;
		let c = Some(RecoveryCheckpoint {
			commit_index: 2,
			e_safe_index: 2,
		});
		for stop in stops(&seed, WAL_PAGE, Mutation::Truncate(3), None, 3).await {
			for seen in stop.after_stops.iter().chain([&stop.after_retry]) {
				assert_eq!(seen.checkpoint, c, "{}", stop.label);
				assert_contiguous_prefix(&seen.entries, &original.entries, 2, &stop.label);
				assert!(
					seen.files.iter().all(|(_, len)| *len <= 2 * WAL_PAGE),
					"{}: no file grows past the segment size plus a page: {:?}",
					stop.label,
					seen.files
				);
				let tail = seen.entries.last().map_or(0, |(index, _)| *index);
				assert!(
					seen.files.iter().filter(|(start, _)| *start > tail).count() <= 2,
					"{}: at most two checkpoint holders: {:?}",
					stop.label,
					seen.files
				);
			}
			assert_eq!(
				stop.after_retry.files,
				vec![(1, WAL_PAGE), (2, 2 * WAL_PAGE), (3, WAL_PAGE)],
				"{}",
				stop.label
			);
		}
	}

	/// The state a truncate leaves between its steps 2 and 5, built by hand: [1,2,3] and the
	/// holder Z = "8" holding C, past the tail.
	fn seed_misnamed_holder(target: Option<Vec<AlignedBuf>>) -> crate::test_support::TestDir {
		let dir = tempdir("misnamed-holder");
		let pages: Vec<AlignedBuf> = (1..=3)
			.map(|i| serialize_entry_page(&finalize_entry(i, 100 * i)).unwrap())
			.collect();
		write_wal_pages(&segment_file(&dir, 1), &pages);
		write_wal_pages(
			&segment_file(&dir, 8),
			&[serialize_checkpoint_page(2, 1, 2, 1)],
		);
		if let Some(target) = target {
			write_wal_pages(&segment_file(&dir, 4), &target);
		}
		dir
	}

	#[tokio::test]
	async fn open_completes_misnamed_trailing_checkpoint_segment() {
		let c = Some(RecoveryCheckpoint {
			commit_index: 2,
			e_safe_index: 1,
		});
		let page = serialize_checkpoint_page(2, 1, 2, 1).as_slice().to_vec();
		// Absent target: published byte for byte, then Z goes.
		// Present and equal: re-synced, then Z goes.
		for target in [None, Some(vec![serialize_checkpoint_page(2, 1, 2, 1)])] {
			let dir = seed_misnamed_holder(target);
			let (_, seen) = reopen(&dir, 3 * WAL_PAGE, "complete").await;
			assert_eq!(seen.files, vec![(1, 3 * WAL_PAGE), (4, WAL_PAGE)]);
			assert_eq!(seen.checkpoint, c);
			assert_eq!(seen.entries.len(), 3);
			assert_eq!(std::fs::read(segment_file(&dir, 4)).unwrap(), page);
		}
		// Unequal, or more than the one record: refused, with no effect.
		for target in [
			vec![serialize_checkpoint_page(1, 0, 1, 1)],
			vec![
				serialize_checkpoint_page(2, 1, 2, 1),
				serialize_checkpoint_page(2, 1, 2, 1),
			],
		] {
			let dir = seed_misnamed_holder(Some(target));
			let files = artifact_bytes(&dir);
			let err = try_open_store_with_segment_bytes(&dir, 3 * WAL_PAGE)
				.await
				.err()
				.expect("an unequal target refuses open");
			assert_eq!(err.kind(), ErrorKind::InvalidData, "{err}");
			assert_eq!(artifact_bytes(&dir), files);
		}
	}

	/// A checkpoint-only holder that open-time cleanup may extend by preserving C′: it must start
	/// with `prefix` and end with `last`, and every page must be an intact checkpoint record.
	struct ExpectedHolder {
		start: u64,
		prefix: Vec<u8>,
		last: Vec<u8>,
	}

	impl ExpectedHolder {
		fn single(start: u64, page: &AlignedBuf) -> Self {
			Self {
				start,
				prefix: page.as_slice().to_vec(),
				last: page.as_slice().to_vec(),
			}
		}
	}

	/// One page is a complete checkpoint record: header, body shape and checksum.
	fn is_checkpoint_page(page: &[u8]) -> bool {
		let header_size = std::mem::size_of::<WALRecordHeader>();
		let body_size = std::mem::size_of::<WALCheckpointBody>();
		let header: WALRecordHeader = *bytemuck::from_bytes(&page[..header_size]);
		if header.record_type != REC_CHECKPOINT || header.body_len != body_size as u64 {
			return false;
		}
		let body_end = header_size + body_size;
		let checksum = u32::from_le_bytes(page[body_end..body_end + 4].try_into().unwrap());
		matches_artifact_checksum_u32(&page[..body_end], checksum)
	}

	/// `seen` (the store at `dir`) recovers exactly `expect`'s entries, checkpoint and segment
	/// names. A segment named in `holders` may have grown by intact C′ pages, page-aligned and
	/// within the segment size; every other segment has exactly the expected length and, where
	/// `pristine` holds a file of that name, exactly its bytes.
	fn assert_recovered_within(
		seen: &Recovered,
		expect: &Recovered,
		dir: &Path,
		pristine: Option<&Path>,
		holders: &[ExpectedHolder],
		segment_bytes: u64,
		label: &str,
	) {
		assert_eq!(seen.entries, expect.entries, "{label}: entries");
		assert_eq!(seen.checkpoint, expect.checkpoint, "{label}: checkpoint");
		assert_eq!(
			seen.files
				.iter()
				.map(|(start, _)| *start)
				.collect::<Vec<_>>(),
			expect
				.files
				.iter()
				.map(|(start, _)| *start)
				.collect::<Vec<_>>(),
			"{label}: segment names"
		);
		for ((start, len), (_, want)) in seen.files.iter().zip(&expect.files) {
			let bytes = std::fs::read(segment_file(dir, *start)).unwrap();
			match holders.iter().find(|holder| holder.start == *start) {
				Some(holder) => {
					assert!(
						*len >= *want && *len <= segment_bytes.max(*want) && *len % WAL_PAGE == 0,
						"{label}: holder {start} is {len} bytes, expected {want} up to {segment_bytes}"
					);
					assert!(
						bytes.starts_with(&holder.prefix),
						"{label}: holder {start} prefix"
					);
					assert!(
						bytes.ends_with(&holder.last),
						"{label}: holder {start} last record"
					);
					assert!(
						bytes.chunks(WAL_PAGE as usize).all(is_checkpoint_page),
						"{label}: holder {start} holds only intact checkpoint records"
					);
				}
				None => {
					assert_eq!(len, want, "{label}: segment {start} length");
					if let Some(pristine) = pristine {
						let original = segment_file(pristine, *start);
						if original.exists() {
							assert_eq!(
								bytes,
								std::fs::read(original).unwrap(),
								"{label}: segment {start} bytes"
							);
						}
					}
				}
			}
		}
	}

	/// Stops at every syscall of open-time completion, each followed by repeated stopped opens and
	/// a final open that completes.
	async fn completion_stops(
		seed: &Path,
		segment_bytes: u64,
		expect: &Recovered,
		holders: &[ExpectedHolder],
	) {
		let mut reached_any = false;
		for op in STOP_OPS {
			for occurrence in 0.. {
				let mut reached = false;
				for crash in [false, true] {
					let label = format!(
						"open stopped at {op:?} #{occurrence}{}",
						if crash { " then crash" } else { "" }
					);
					let dir = copy_store(seed);
					for round in 0..2 {
						let file_ops = Arc::new(FaultyFileOps::new(dir.to_path_buf()).unwrap());
						file_ops.inject(stop_at(op, occurrence));
						match try_open_faulty(&dir, file_ops.clone(), segment_bytes).await {
							Ok(_) => break,
							Err(_) => {
								reached = true;
								reached_any = true;
								if crash {
									file_ops.crash().unwrap();
								}
							}
						}
						let _ = round;
					}
					let (_, seen) = reopen(&dir, segment_bytes, &label).await;
					assert_recovered_within(
						&seen,
						expect,
						&dir,
						Some(seed),
						holders,
						segment_bytes,
						&label,
					);
				}
				if !reached {
					break;
				}
			}
		}
		assert!(reached_any);
	}

	#[tokio::test]
	async fn open_completion_stops_are_retry_safe() {
		let page = serialize_checkpoint_page(2, 1, 2, 1).as_slice().to_vec();
		let seed = seed_misnamed_holder(None);
		let expect = Recovered {
			entries: entry_bytes(
				&(1..=3)
					.map(|i| finalize_entry(i, 100 * i))
					.collect::<Vec<_>>(),
			),
			checkpoint: Some(RecoveryCheckpoint {
				commit_index: 2,
				e_safe_index: 1,
			}),
			files: vec![(1, 3 * WAL_PAGE), (4, WAL_PAGE)],
		};
		let c = serialize_checkpoint_page(2, 1, 2, 1);
		completion_stops(
			&seed,
			3 * WAL_PAGE,
			&expect,
			&[ExpectedHolder::single(4, &c)],
		)
		.await;
		// The snapshot-explained shape: [1,2,3], Z = "3" holding C, snapshot at 3.
		let below = tempdir("below-tail-explained");
		let pages: Vec<AlignedBuf> = (1..=3)
			.map(|i| serialize_entry_page(&finalize_entry(i, 100 * i)).unwrap())
			.collect();
		write_wal_pages(&segment_file(&below, 1), &pages);
		write_wal_pages(
			&segment_file(&below, 3),
			&[serialize_checkpoint_page(3, 3, 3, 1)],
		);
		snapshot_at(&below, 3);
		let expect = Recovered {
			entries: Vec::new(),
			checkpoint: Some(RecoveryCheckpoint {
				commit_index: 3,
				e_safe_index: 3,
			}),
			files: vec![(4, WAL_PAGE)],
		};
		let c3 = serialize_checkpoint_page(3, 3, 3, 1);
		completion_stops(
			&below,
			3 * WAL_PAGE,
			&expect,
			&[ExpectedHolder::single(4, &c3)],
		)
		.await;
		assert_eq!(page, c.as_slice());
	}

	#[tokio::test]
	async fn open_refuses_unexplained_below_tail_holder_without_effect() {
		let build = |snapshot: Option<u64>| {
			let dir = tempdir("below-tail");
			let pages: Vec<AlignedBuf> = (1..=3)
				.map(|i| serialize_entry_page(&finalize_entry(i, 100 * i)).unwrap())
				.collect();
			write_wal_pages(&segment_file(&dir, 1), &pages);
			write_wal_pages(
				&segment_file(&dir, 3),
				&[serialize_checkpoint_page(2, 2, 2, 1)],
			);
			if let Some(index) = snapshot {
				snapshot_at(&dir, index);
			}
			dir
		};
		// No snapshot (the pre-fix R1 shape), and a snapshot that leaves entry 3 retained past Z.
		for snapshot in [None, Some(2)] {
			let dir = build(snapshot);
			let files = artifact_bytes(&dir);
			let err = try_open_store_with_segment_bytes(&dir, 3 * WAL_PAGE)
				.await
				.err()
				.expect("an unexplained below-tail holder refuses open");
			assert_eq!(err.kind(), ErrorKind::InvalidData, "{snapshot:?}: {err}");
			assert_eq!(artifact_bytes(&dir), files, "{snapshot:?}");
		}
		// A corrupt snapshot refuses before completion.
		let dir = build(Some(3));
		let snapshot_path = artifact_paths(&dir).2;
		let mut bytes = std::fs::read(&snapshot_path).unwrap();
		let last = bytes.len() - 1;
		bytes[last] ^= 0xFF;
		std::fs::write(&snapshot_path, bytes).unwrap();
		let files = artifact_bytes(&dir);
		assert!(
			try_open_store_with_segment_bytes(&dir, 3 * WAL_PAGE)
				.await
				.is_err()
		);
		assert_eq!(artifact_bytes(&dir), files);
		// Full retention refuses a snapshot before replay: no completion effect.
		let dir = build(Some(3));
		let files = artifact_bytes(&dir);
		let guard = retention::RetentionGuard::new(crate::consensus::ControlRetention::Full);
		assert!(
			WendelonLogStore::open_for_retention_test(&dir, 3 * WAL_PAGE, Some(guard))
				.await
				.is_err()
		);
		assert_eq!(artifact_bytes(&dir), files);
		// Explained by a valid snapshot at 3: completes.
		let dir = build(Some(3));
		let (_, seen) = reopen(&dir, 3 * WAL_PAGE, "explained").await;
		assert_eq!(seen.files, vec![(4, WAL_PAGE)]);
		assert!(seen.entries.is_empty());
		assert_eq!(
			seen.checkpoint, None,
			"commit 2 < snapshot 3 stays superseded"
		);
	}

	#[tokio::test]
	async fn purge_all_log_segments_with_spare_tail_room() {
		let seed = tempdir("purge-spare-room");
		let mut store = try_open_store_with_segment_bytes(&seed, 3 * WAL_PAGE)
			.await
			.unwrap();
		store
			.blocking_append(
				(1..=4)
					.map(|i| finalize_entry(i, 100 * i))
					.collect::<Vec<_>>(),
			)
			.await
			.unwrap();
		checkpoint(&store, 4, 4).await;
		assert_eq!(
			segment_metadata(&store).await,
			vec![(1, Some(3), 3 * WAL_PAGE), (4, Some(4), 2 * WAL_PAGE)]
		);
		drop(store);
		snapshot_at(&seed, 4);
		let c = Some(RecoveryCheckpoint {
			commit_index: 4,
			e_safe_index: 4,
		});
		assert_eq!(reopen(&seed, 3 * WAL_PAGE, "seed").await.1.checkpoint, c);
		for stop in stops(&seed, 3 * WAL_PAGE, Mutation::Purge(4), None, 1).await {
			for seen in &stop.after_stops {
				assert_eq!(seen.checkpoint, c, "{}", stop.label);
				assert!(seen.entries.is_empty());
			}
			assert_eq!(stop.after_retry.checkpoint, c, "{}", stop.label);
			// Preservation went to a new holder named 5, not into the purged tail segment 4; a
			// retry may append C′ to it once more, within the segment.
			assert!(
				matches!(stop.after_retry.files.as_slice(), [(5, len)] if *len <= 3 * WAL_PAGE),
				"{}: {:?}",
				stop.label,
				stop.after_retry.files
			);
		}
	}

	#[tokio::test]
	async fn purge_full_checkpoint_only_tail_replaces_not_collides() {
		let seed = tempdir("purge-full-holder");
		let mut store = try_open_store_with_segment_bytes(&seed, WAL_PAGE)
			.await
			.unwrap();
		store
			.blocking_append(vec![finalize_entry(1, 100)])
			.await
			.unwrap();
		store
			.blocking_append(vec![finalize_entry(2, 200)])
			.await
			.unwrap();
		checkpoint(&store, 2, 2).await;
		assert_eq!(
			segment_metadata(&store).await,
			vec![
				(1, Some(1), WAL_PAGE),
				(2, Some(2), WAL_PAGE),
				(3, None, WAL_PAGE)
			]
		);
		drop(store);
		snapshot_at(&seed, 2);
		let c = Some(RecoveryCheckpoint {
			commit_index: 2,
			e_safe_index: 2,
		});
		for stop in stops(&seed, WAL_PAGE, Mutation::Purge(2), None, 1).await {
			for seen in &stop.after_stops {
				assert_eq!(seen.checkpoint, c, "{}", stop.label);
			}
			assert_eq!(stop.after_retry.checkpoint, c, "{}", stop.label);
			assert_eq!(
				stop.after_retry.files,
				vec![(3, WAL_PAGE)],
				"{}",
				stop.label
			);
		}
	}

	#[tokio::test]
	async fn purge_never_deletes_checkpoint_only() {
		let dir = tempdir("purge-keeps-holder");
		let mut store = try_open_store_with_segment_bytes(&dir, WAL_PAGE)
			.await
			.unwrap();
		store
			.blocking_append(vec![finalize_entry(1, 100)])
			.await
			.unwrap();
		store
			.blocking_append(vec![finalize_entry(2, 200)])
			.await
			.unwrap();
		checkpoint(&store, 2, 2).await;
		snapshot_at(&dir, 2);
		store.note_published_snapshot_for_test(log_id(1, 2)).await;
		store.purge(log_id(1, 2)).await.unwrap();
		assert_eq!(segment_metadata(&store).await, vec![(3, None, WAL_PAGE)]);
		// N1: the converted holder has one metadata entry and later purge/truncate keep it.
		store
			.blocking_append(vec![finalize_entry(3, 300)])
			.await
			.unwrap();
		assert_eq!(
			segment_metadata(&store).await,
			vec![(3, Some(3), 2 * WAL_PAGE)]
		);
		store.purge(log_id(1, 2)).await.unwrap();
		store.truncate(log_id(1, 4)).await.unwrap();
		assert!(segment_file(&dir, 3).exists());
		drop(store);
		let (_, seen) = reopen(&dir, WAL_PAGE, "holder").await;
		assert_eq!(
			seen.entries.iter().map(|(i, _)| *i).collect::<Vec<_>>(),
			vec![3]
		);
		assert_eq!(seen.files, vec![(3, 2 * WAL_PAGE)]);
	}

	#[tokio::test]
	async fn snapshot_strict_rule_unchanged() {
		for (snapshot, expect) in [
			(
				2,
				Some(RecoveryCheckpoint {
					commit_index: 2,
					e_safe_index: 2,
				}),
			),
			(3, None),
		] {
			let dir = tempdir("strict-rule");
			let mut store = try_open_store_with_segment_bytes(&dir, 4 * WAL_PAGE)
				.await
				.unwrap();
			store
				.blocking_append(
					(1..=3)
						.map(|i| finalize_entry(i, 100 * i))
						.collect::<Vec<_>>(),
				)
				.await
				.unwrap();
			checkpoint(&store, 2, 2).await;
			drop(store);
			snapshot_at(&dir, snapshot);
			let (_, seen) = reopen(&dir, 4 * WAL_PAGE, "strict").await;
			assert_eq!(seen.checkpoint, expect, "snapshot at {snapshot}");
		}
	}

	/// A follower with entry 1 and a checkpoint-only active segment "2" installs a real snapshot
	/// at 3 (s > n). Either it stops before the purge and reopens (P2), or it purges (P1); with and
	/// without a coordinator checkpoint emitted between the install and the purge.
	#[tokio::test]
	async fn snapshot_install_past_the_tail_reopens_and_purges() {
		let source_dir = tempdir("install-source");
		let mut source_store = open_store(&source_dir).await;
		let mut source_sm = source_store.recovered_state_machine().await.unwrap();
		for (i, start) in [(1, 0), (2, 10), (3, 20)] {
			append_and_apply(
				&mut source_store,
				&mut source_sm,
				lease_entry(i, start, start + 9, 3),
			)
			.await;
		}
		for emit in [false, true] {
			for stop_before_purge in [true, false] {
				let label = format!("emit {emit}, stop before purge {stop_before_purge}");
				let mut builder = <WendelonStateMachine as RaftStateMachine<
					WendelonTypeConfig,
				>>::get_snapshot_builder(&mut source_sm)
				.await;
				let built = builder.build_snapshot().await.unwrap();
				assert_eq!(built.meta.last_log_id, Some(log_id(1, 3)));
				let dir = tempdir("install-target");
				let mut store = try_open_store_with_segment_bytes(&dir, WAL_PAGE)
					.await
					.unwrap();
				store
					.blocking_append(vec![finalize_entry(1, 100)])
					.await
					.unwrap();
				checkpoint(&store, 1, 1).await;
				assert_eq!(
					segment_metadata(&store).await,
					vec![(1, Some(1), WAL_PAGE), (2, None, WAL_PAGE)]
				);
				let mut sm = store.recovered_state_machine().await.unwrap();
				<WendelonStateMachine as RaftStateMachine<WendelonTypeConfig>>::install_snapshot(
					&mut sm,
					&built.meta,
					built.snapshot,
				)
				.await
				.unwrap();
				if emit {
					checkpoint(&store, 3, 3).await;
				}
				let expect = emit.then_some(RecoveryCheckpoint {
					commit_index: 3,
					e_safe_index: 3,
				});
				if stop_before_purge {
					drop(sm);
					drop(store);
					let (mut reopened, seen) = reopen(&dir, WAL_PAGE, &label).await;
					assert_eq!(seen.files, vec![(4, WAL_PAGE)], "{label}");
					assert_eq!(seen.checkpoint, expect, "{label}");
					assert!(seen.entries.is_empty());
					reopened.purge(log_id(1, 3)).await.unwrap();
					store = reopened;
				} else {
					store.purge(log_id(1, 3)).await.unwrap();
				}
				assert_eq!(segment_metadata(&store).await, vec![(4, None, WAL_PAGE)]);
				store
					.blocking_append(vec![finalize_entry(4, 400)])
					.await
					.unwrap();
				drop(store);
				let (_, seen) = reopen(&dir, WAL_PAGE, &label).await;
				// A checkpoint superseded at open carries no authority: the full holder is then
				// replaced by the entry alone. Otherwise the checkpoint precedes the entry.
				let pages = if emit || !stop_before_purge { 2 } else { 1 };
				assert_eq!(seen.files, vec![(4, pages * WAL_PAGE)], "{label}");
				assert_eq!(seen.checkpoint, expect, "{label}");
				assert_eq!(
					seen.entries.iter().map(|(i, _)| *i).collect::<Vec<_>>(),
					vec![4]
				);
			}
		}
	}

	/// P1 under every stop: the in-memory store has not seen the install; purge(3) must move the
	/// checkpoint-only active segment "2" to "4", holder first and "2" last.
	#[tokio::test]
	async fn purge_past_local_tail_renames_checkpoint_only_holder() {
		let seed = tempdir("purge-past-tail");
		let mut store = try_open_store_with_segment_bytes(&seed, WAL_PAGE)
			.await
			.unwrap();
		store
			.blocking_append(vec![finalize_entry(1, 100)])
			.await
			.unwrap();
		checkpoint(&store, 1, 1).await;
		drop(store);
		let snapshot_dir = tempdir("purge-past-tail-snapshot");
		snapshot_at(&snapshot_dir, 3);
		let snapshot = artifact_paths(&snapshot_dir).2;
		for stop in stops(&seed, WAL_PAGE, Mutation::Purge(3), Some((&snapshot, 3)), 1).await {
			for seen in &stop.after_stops {
				assert!(seen.entries.is_empty(), "{}", stop.label);
				assert_eq!(seen.checkpoint, None, "{}: commit 1 < 3", stop.label);
			}
			assert_eq!(
				stop.after_retry.files,
				vec![(4, WAL_PAGE)],
				"{}",
				stop.label
			);
		}
	}

	/// P1 with no checkpoint: an empty checkpoint-only "2" (an append-rotation residue) is removed
	/// first, the log after, and no holder is made.
	#[tokio::test]
	async fn purge_past_local_tail_without_checkpoint_omits_the_holder() {
		let seed = tempdir("purge-past-tail-empty");
		let mut store = try_open_store_with_segment_bytes(&seed, WAL_PAGE)
			.await
			.unwrap();
		store
			.blocking_append(vec![finalize_entry(1, 100)])
			.await
			.unwrap();
		drop(store);
		std::fs::File::create(segment_file(&seed, 2)).unwrap();
		let (store, seen) = reopen(&seed, WAL_PAGE, "seed").await;
		assert_eq!(seen.checkpoint, None);
		assert_eq!(
			segment_metadata(&store).await,
			vec![(1, Some(1), WAL_PAGE), (2, None, 0)]
		);
		drop(store);
		let snapshot_dir = tempdir("purge-past-tail-empty-snapshot");
		snapshot_at(&snapshot_dir, 3);
		let snapshot = artifact_paths(&snapshot_dir).2;
		for stop in stops(&seed, WAL_PAGE, Mutation::Purge(3), Some((&snapshot, 3)), 1).await {
			for seen in &stop.after_stops {
				assert!(seen.entries.is_empty(), "{}", stop.label);
				assert_eq!(seen.checkpoint, None, "{}", stop.label);
			}
			assert_eq!(stop.after_retry.files, Vec::new(), "{}", stop.label);
		}
	}

	#[tokio::test]
	async fn purge_refuses_an_occupied_holder_name_before_effects() {
		let dir = tempdir("purge-occupied");
		let mut store = try_open_store_with_segment_bytes(&dir, WAL_PAGE)
			.await
			.unwrap();
		store
			.blocking_append(vec![finalize_entry(1, 100)])
			.await
			.unwrap();
		checkpoint(&store, 1, 1).await;
		snapshot_at(&dir, 3);
		store.note_published_snapshot_for_test(log_id(1, 3)).await;
		// A stray file already holds the post-purge name.
		write_wal_pages(
			&segment_file(&dir, 4),
			&[serialize_checkpoint_page(1, 0, 1, 1)],
		);
		let files = artifact_bytes(&dir);
		let err = store.purge(log_id(1, 3)).await.err().expect("refused");
		assert!(err.to_string().contains("occupied"), "{err}");
		assert_eq!(artifact_bytes(&dir), files);
		assert!(store.inner.read().await.poisoned.is_none());
	}

	#[tokio::test]
	async fn purge_log_bearing_active_and_no_active_file() {
		// A surviving log tail with room takes C′ at its cursor.
		let dir = tempdir("purge-log-active");
		let mut store = try_open_store_with_segment_bytes(&dir, 3 * WAL_PAGE)
			.await
			.unwrap();
		store
			.blocking_append(
				(1..=4)
					.map(|i| finalize_entry(i, 100 * i))
					.collect::<Vec<_>>(),
			)
			.await
			.unwrap();
		checkpoint(&store, 2, 2).await;
		snapshot_at(&dir, 3);
		store.note_published_snapshot_for_test(log_id(1, 3)).await;
		store.purge(log_id(1, 3)).await.unwrap();
		assert_eq!(
			segment_metadata(&store).await,
			vec![(4, Some(4), 3 * WAL_PAGE)]
		);
		drop(store);
		let (_, seen) = reopen(&dir, 3 * WAL_PAGE, "log active").await;
		assert_eq!(
			seen.entries.iter().map(|(i, _)| *i).collect::<Vec<_>>(),
			vec![4]
		);
		// No active file at all: purge has nothing to remove and no effect.
		let dir = tempdir("purge-no-file");
		let mut store = try_open_store_with_segment_bytes(&dir, WAL_PAGE)
			.await
			.unwrap();
		let files = artifact_bytes(&dir);
		store.purge(log_id(1, 3)).await.unwrap();
		assert_eq!(artifact_bytes(&dir), files);
		assert_eq!(segment_metadata(&store).await, Vec::new());
	}

	/// [1,2,3] in segment 1, a snapshot at 3, and the given checkpoint-only files.
	fn seed_snapshot_explained(holders: &[(u64, Vec<AlignedBuf>)]) -> crate::test_support::TestDir {
		let dir = tempdir("snapshot-explained");
		let pages: Vec<AlignedBuf> = (1..=3)
			.map(|i| serialize_entry_page(&finalize_entry(i, 100 * i)).unwrap())
			.collect();
		write_wal_pages(&segment_file(&dir, 1), &pages);
		for (start, pages) in holders {
			if pages.is_empty() {
				std::fs::File::create(segment_file(&dir, *start)).unwrap();
			} else {
				write_wal_pages(&segment_file(&dir, *start), pages);
			}
		}
		snapshot_at(&dir, 3);
		dir
	}

	#[tokio::test]
	async fn open_completes_the_adjacent_target_shape() {
		let c3 = || serialize_checkpoint_page(3, 3, 3, 1);
		let c_old = || serialize_checkpoint_page(2, 2, 2, 1);
		let expect = Recovered {
			entries: Vec::new(),
			checkpoint: Some(RecoveryCheckpoint {
				commit_index: 3,
				e_safe_index: 3,
			}),
			files: vec![(4, WAL_PAGE)],
		};
		// Z exactly equal, Z exactly empty, Z wholly validated records ending in the target's.
		for z in [vec![c3()], Vec::new(), vec![c_old(), c3()]] {
			let shape = format!("Z of {} pages", z.len());
			let seed = seed_snapshot_explained(&[(3, z), (4, vec![c3()])]);
			let holders = [ExpectedHolder::single(4, &c3())];
			completion_stops(&seed, 3 * WAL_PAGE, &expect, &holders).await;
			let pristine = copy_store(&seed);
			let (_, seen) = reopen(&seed, 3 * WAL_PAGE, &shape).await;
			assert_recovered_within(
				&seen,
				&expect,
				&seed,
				Some(&pristine),
				&holders,
				3 * WAL_PAGE,
				&shape,
			);
		}
		// A trailing, exactly empty Z below the tail carries nothing: it is only unlinked.
		let seed = seed_snapshot_explained(&[(3, Vec::new())]);
		let (_, seen) = reopen(&seed, 3 * WAL_PAGE, "empty trailing").await;
		assert_eq!(seen.files, Vec::<(u64, u64)>::new());
		assert_eq!(seen.checkpoint, None);
	}

	#[tokio::test]
	async fn open_refuses_adjacent_shapes_outside_the_bounds_without_effect() {
		let c3 = || serialize_checkpoint_page(3, 3, 3, 1);
		let c_old = || serialize_checkpoint_page(2, 2, 2, 1);
		let zero_page = || AlignedBuf::new(DIRECT_IO_ALIGN);
		let cases: Vec<(&str, Vec<(u64, Vec<AlignedBuf>)>)> = vec![
			("unequal Z", vec![(3, vec![c_old()]), (4, vec![c3()])]),
			(
				"target of two records",
				vec![(3, vec![c3()]), (4, vec![c3(), c3()])],
			),
			(
				"zero-filled Z",
				vec![(3, vec![zero_page()]), (4, vec![c3()])],
			),
			(
				"non-adjacent holder",
				vec![(2, vec![c3()]), (3, vec![c3()]), (4, vec![c3()])],
			),
			(
				"unexplained trailing holder past nothing",
				vec![(2, vec![c3()]), (3, vec![c3()])],
			),
		];
		for (name, holders) in cases {
			let dir = seed_snapshot_explained(&holders);
			let files = artifact_bytes(&dir);
			assert!(
				try_open_store_with_segment_bytes(&dir, 3 * WAL_PAGE)
					.await
					.is_err(),
				"{name} refuses open"
			);
			assert_eq!(artifact_bytes(&dir), files, "{name}: no effect");
		}
	}

	/// An append-rotation residue (an empty segment published before its entry), then a real
	/// install past the tail, then a crash before the purge: open unlinks the exactly empty
	/// trailing segment, publishing nothing, and the purge and next append proceed.
	#[tokio::test]
	async fn open_unlinks_an_empty_residue_explained_by_an_installed_snapshot() {
		let source_dir = tempdir("residue-source");
		let mut source_store = open_store(&source_dir).await;
		let mut source_sm = source_store.recovered_state_machine().await.unwrap();
		for (i, start) in [(1, 0), (2, 10), (3, 20)] {
			append_and_apply(
				&mut source_store,
				&mut source_sm,
				lease_entry(i, start, start + 9, 3),
			)
			.await;
		}
		let mut builder =
			<WendelonStateMachine as RaftStateMachine<WendelonTypeConfig>>::get_snapshot_builder(
				&mut source_sm,
			)
			.await;
		let built = builder.build_snapshot().await.unwrap();
		let dir = tempdir("residue-target");
		let mut store = try_open_store_with_segment_bytes(&dir, WAL_PAGE)
			.await
			.unwrap();
		store
			.blocking_append(vec![finalize_entry(1, 100)])
			.await
			.unwrap();
		drop(store);
		std::fs::File::create(segment_file(&dir, 2)).unwrap();
		let (store, seen) = reopen(&dir, WAL_PAGE, "residue").await;
		assert_eq!(seen.files, vec![(1, WAL_PAGE), (2, 0)]);
		let mut sm = store.recovered_state_machine().await.unwrap();
		<WendelonStateMachine as RaftStateMachine<WendelonTypeConfig>>::install_snapshot(
			&mut sm,
			&built.meta,
			built.snapshot,
		)
		.await
		.unwrap();
		drop(sm);
		drop(store);
		let (mut store, seen) = reopen(&dir, WAL_PAGE, "installed").await;
		assert_eq!(seen.files, Vec::<(u64, u64)>::new());
		assert_eq!(seen.checkpoint, None);
		assert!(seen.entries.is_empty());
		store.purge(log_id(1, 3)).await.unwrap();
		store
			.blocking_append(vec![finalize_entry(4, 400)])
			.await
			.unwrap();
		drop(store);
		let (_, seen) = reopen(&dir, WAL_PAGE, "appended").await;
		assert_eq!(seen.files, vec![(4, WAL_PAGE)]);
		assert_eq!(
			seen.entries.iter().map(|(i, _)| *i).collect::<Vec<_>>(),
			vec![4]
		);
	}

	#[tokio::test]
	async fn open_refuses_empty_or_torn_holders_outside_the_bounds_without_effect() {
		let entries = |dir: &Path, last: u64| {
			let pages: Vec<AlignedBuf> = (1..=last)
				.map(|i| serialize_entry_page(&finalize_entry(i, 100 * i)).unwrap())
				.collect();
			write_wal_pages(&segment_file(dir, 1), &pages);
		};
		// An empty segment below the tail with no snapshot to explain it.
		let no_snapshot = tempdir("empty-no-snapshot");
		entries(&no_snapshot, 2);
		std::fs::File::create(segment_file(&no_snapshot, 2)).unwrap();
		// A zero-filled, nonempty trailing segment: torn, not empty.
		let zero_filled = tempdir("zero-filled-trailing");
		entries(&zero_filled, 3);
		write_wal_pages(
			&segment_file(&zero_filled, 3),
			&[AlignedBuf::new(DIRECT_IO_ALIGN)],
		);
		snapshot_at(&zero_filled, 3);
		// Two empty holders: ambiguous.
		let ambiguous = tempdir("two-empty-holders");
		entries(&ambiguous, 3);
		std::fs::File::create(segment_file(&ambiguous, 2)).unwrap();
		std::fs::File::create(segment_file(&ambiguous, 3)).unwrap();
		snapshot_at(&ambiguous, 3);
		for dir in [no_snapshot, zero_filled, ambiguous] {
			let files = artifact_bytes(&dir);
			assert!(
				try_open_store_with_segment_bytes(&dir, 3 * WAL_PAGE)
					.await
					.is_err(),
				"{} refuses open",
				dir.display()
			);
			assert_eq!(artifact_bytes(&dir), files, "{}: no effect", dir.display());
		}
	}

	/// P1 with no checkpoint and a nonempty passed holder whose records were all superseded at
	/// open by the recovered snapshot: it is removed first, then the log, under every stop.
	#[tokio::test]
	async fn purge_past_local_tail_removes_a_superseded_holder_first() {
		let seed = tempdir("purge-superseded-holder");
		let mut store = try_open_store_with_segment_bytes(&seed, 2 * WAL_PAGE)
			.await
			.unwrap();
		store
			.blocking_append(vec![finalize_entry(1, 100), finalize_entry(2, 200)])
			.await
			.unwrap();
		checkpoint(&store, 1, 1).await;
		assert_eq!(
			segment_metadata(&store).await,
			vec![(1, Some(2), 2 * WAL_PAGE), (3, None, WAL_PAGE)]
		);
		drop(store);
		snapshot_at(&seed, 2);
		let seen = reopen(&seed, 2 * WAL_PAGE, "seed").await.1;
		assert_eq!(seen.checkpoint, None, "commit 1 < snapshot 2");
		assert_eq!(seen.files, vec![(3, WAL_PAGE)]);
		let snapshot_dir = tempdir("purge-superseded-holder-snapshot");
		snapshot_at(&snapshot_dir, 5);
		let snapshot = artifact_paths(&snapshot_dir).2;
		let all = stops(
			&seed,
			2 * WAL_PAGE,
			Mutation::Purge(5),
			Some((&snapshot, 5)),
			1,
		)
		.await;
		// Open already removed the log the recovered snapshot covers; only the holder remains.
		for stop in all {
			for seen in &stop.after_stops {
				assert!(seen.entries.is_empty(), "{}", stop.label);
				assert_eq!(seen.checkpoint, None, "{}", stop.label);
			}
			// A stop before the holder is removed leaves it below the new tail; reopen then moves
			// its superseded record to the holder named 6 (open-time completion), which the retry
			// keeps. Either way no checkpoint is recovered.
			assert_eq!(stop.after_retry.checkpoint, None, "{}", stop.label);
			assert!(
				matches!(stop.after_retry.files.as_slice(), [] | [(6, WAL_PAGE)]),
				"{}: {:?}",
				stop.label,
				stop.after_retry.files
			);
		}
	}

	/// The trailing checkpoint-only shapes open-time completion acts on: past the tail (Z = 8 over
	/// [1,2,3]) or below it, explained by a snapshot at 3 (Z = 3 over [1,2,3]).
	fn seed_trailing_holder(
		below_tail: bool,
		z: &[AlignedBuf],
		suffix: &[u8],
	) -> crate::test_support::TestDir {
		let dir = tempdir("trailing-holder");
		let pages: Vec<AlignedBuf> = (1..=3)
			.map(|i| serialize_entry_page(&finalize_entry(i, 100 * i)).unwrap())
			.collect();
		write_wal_pages(&segment_file(&dir, 1), &pages);
		let z_path = segment_file(&dir, if below_tail { 3 } else { 8 });
		write_wal_pages(&z_path, z);
		if !suffix.is_empty() {
			OpenOptions::new()
				.append(true)
				.open(&z_path)
				.unwrap()
				.write_all(suffix)
				.unwrap();
		}
		if below_tail {
			snapshot_at(&dir, 3);
		}
		dir
	}

	/// F1: a trailing holder with a valid checkpoint record followed by bytes replay did not
	/// validate (a partial header, a zero-filled page, a final record with a bad checksum) is not
	/// wholly validated: open refuses before any effect, in both off-tail shapes.
	#[tokio::test]
	async fn open_refuses_a_trailing_holder_with_an_unvalidated_suffix_without_effect() {
		let page = serialize_checkpoint_page(2, 1, 2, 1);
		let mut bad_crc = serialize_checkpoint_page(2, 1, 2, 1);
		let crc_at =
			std::mem::size_of::<WALRecordHeader>() + std::mem::size_of::<WALCheckpointBody>();
		bad_crc.as_mut_slice()[crc_at] ^= 0xFF;
		let suffixes: [(&str, Vec<u8>); 3] = [
			("partial header", page.as_slice()[..16].to_vec()),
			("zero-filled page", vec![0; DIRECT_IO_ALIGN]),
			("final bad checksum", bad_crc.as_slice().to_vec()),
		];
		for below_tail in [false, true] {
			for (name, suffix) in &suffixes {
				let label = format!("{name}, below tail {below_tail}");
				let dir = seed_trailing_holder(
					below_tail,
					&[serialize_checkpoint_page(2, 1, 2, 1)],
					suffix,
				);
				let files = artifact_bytes(&dir);
				let err = try_open_store_with_segment_bytes(&dir, 3 * WAL_PAGE)
					.await
					.err()
					.unwrap_or_else(|| panic!("{label}: an unvalidated suffix refuses open"));
				assert_eq!(err.kind(), ErrorKind::InvalidData, "{label}: {err}");
				assert_eq!(artifact_bytes(&dir), files, "{label}: no effect");
			}
		}
	}

	/// A wholly validated trailing holder of several records completes from its last record, in
	/// both off-tail shapes.
	#[tokio::test]
	async fn open_completes_a_wholly_valid_multi_record_trailing_holder() {
		for below_tail in [false, true] {
			let (older, latest, expect) = if below_tail {
				(
					serialize_checkpoint_page(2, 2, 2, 1),
					serialize_checkpoint_page(3, 3, 3, 1),
					RecoveryCheckpoint {
						commit_index: 3,
						e_safe_index: 3,
					},
				)
			} else {
				(
					serialize_checkpoint_page(1, 0, 1, 1),
					serialize_checkpoint_page(2, 1, 2, 1),
					RecoveryCheckpoint {
						commit_index: 2,
						e_safe_index: 1,
					},
				)
			};
			let latest_bytes = latest.as_slice().to_vec();
			let holders = [ExpectedHolder::single(4, &latest)];
			let dir = seed_trailing_holder(below_tail, &[older, latest], &[]);
			let pristine = copy_store(&dir);
			let (_, seen) = reopen(&dir, 3 * WAL_PAGE, "multi-record").await;
			let want = Recovered {
				entries: if below_tail {
					Vec::new()
				} else {
					entry_bytes(
						&(1..=3)
							.map(|i| finalize_entry(i, 100 * i))
							.collect::<Vec<_>>(),
					)
				},
				checkpoint: Some(expect),
				files: if below_tail {
					vec![(4, WAL_PAGE)]
				} else {
					vec![(1, 3 * WAL_PAGE), (4, WAL_PAGE)]
				},
			};
			assert_recovered_within(
				&seen,
				&want,
				&dir,
				Some(&pristine),
				&holders,
				3 * WAL_PAGE,
				&format!("below tail {below_tail}"),
			);
			assert!(
				std::fs::read(segment_file(&dir, 4))
					.unwrap()
					.ends_with(&latest_bytes)
			);
		}
	}

	// Snapshot handoff reproductions. A direct `purge` stands in for openraft's unconditioned
	// `PurgeLog` overtaking the state-machine worker's install (a log-store ordering surrogate
	// supported by the source trace, not a measured Raft scheduling failure). No snapshot covering
	// the purge is made durable. Logs starting at index 1 are legacy test fixtures, not
	// authenticated genesis. Each test asserts the recovery property the handoff must preserve.

	/// T1: with no durable snapshot, a purge must not cost the store its reopenability or any
	/// entry: the needed log is recovered whole, with identical bytes.
	#[tokio::test]
	async fn purge_before_durable_snapshot_keeps_the_store_reopenable() {
		let dir = tempdir("handoff-t1");
		let mut store = try_open_store_with_segment_bytes(&dir, 2 * WAL_PAGE)
			.await
			.unwrap();
		store
			.blocking_append(
				(1..=5)
					.map(|i| finalize_entry(i, 100 * i))
					.collect::<Vec<_>>(),
			)
			.await
			.unwrap();
		assert_eq!(
			segment_metadata(&store).await,
			vec![
				(1, Some(2), 2 * WAL_PAGE),
				(3, Some(4), 2 * WAL_PAGE),
				(5, Some(5), WAL_PAGE)
			]
		);
		let original = recovered(&mut store, &dir).await;
		store.purge(log_id(1, 2)).await.unwrap();
		drop(store);
		let reopened = try_open_store_with_segment_bytes(&dir, 2 * WAL_PAGE).await;
		let mut reopened =
			reopened.expect("the store reopens: no durable snapshot covers the purge");
		let seen = recovered(&mut reopened, &dir).await;
		assert_eq!(
			seen.entries, original.entries,
			"every entry recovered, identical bytes"
		);
	}

	/// T1b: the install-past-the-tail shape (purge(3) over entry 1 with a checkpoint-only active
	/// segment "2"), again with no durable snapshot: entry 1 and its checkpoint are recovered.
	#[tokio::test]
	async fn purge_past_the_tail_before_durable_snapshot_keeps_the_store_reopenable() {
		let dir = tempdir("handoff-t1b");
		let mut store = try_open_store_with_segment_bytes(&dir, WAL_PAGE)
			.await
			.unwrap();
		store
			.blocking_append(vec![finalize_entry(1, 100)])
			.await
			.unwrap();
		checkpoint(&store, 1, 1).await;
		assert_eq!(
			segment_metadata(&store).await,
			vec![(1, Some(1), WAL_PAGE), (2, None, WAL_PAGE)]
		);
		let original = recovered(&mut store, &dir).await;
		store.purge(log_id(1, 3)).await.unwrap();
		drop(store);
		let reopened = try_open_store_with_segment_bytes(&dir, WAL_PAGE).await;
		let mut reopened =
			reopened.expect("the store reopens: no durable snapshot covers the purge");
		let seen = recovered(&mut reopened, &dir).await;
		assert_eq!(
			seen.entries, original.entries,
			"entry 1 recovered, identical bytes"
		);
		assert_eq!(
			seen.checkpoint, original.checkpoint,
			"the checkpoint is recovered"
		);
	}

	/// T2: with an older durable snapshot at j = 2, purge(4) before the newer snapshot is durable
	/// must leave a recoverable contiguous log: the first retained entry is j + 1 and every entry
	/// past j is recovered with identical bytes, at the snapshot boundary.
	#[tokio::test]
	async fn purge_before_durable_snapshot_with_an_older_snapshot_keeps_the_log_contiguous() {
		let dir = tempdir("handoff-t2");
		let mut store = try_open_store_with_segment_bytes(&dir, 2 * WAL_PAGE)
			.await
			.unwrap();
		store
			.blocking_append(
				(1..=6)
					.map(|i| finalize_entry(i, 100 * i))
					.collect::<Vec<_>>(),
			)
			.await
			.unwrap();
		drop(store);
		snapshot_at(&dir, 2);
		let mut store = try_open_store_with_segment_bytes(&dir, 2 * WAL_PAGE)
			.await
			.unwrap();
		let original = recovered(&mut store, &dir).await;
		assert_eq!(
			original.entries.iter().map(|(i, _)| *i).collect::<Vec<_>>(),
			vec![3, 4, 5, 6]
		);
		store.purge(log_id(1, 4)).await.unwrap();
		drop(store);
		let reopened = try_open_store_with_segment_bytes(&dir, 2 * WAL_PAGE).await;
		let mut reopened =
			reopened.expect("the store reopens: the older snapshot is still durable");
		let state = reopened.get_log_state().await.unwrap();
		assert_eq!(
			state.last_purged_log_id,
			Some(log_id(1, 2)),
			"the durable boundary is j"
		);
		let seen = recovered(&mut reopened, &dir).await;
		assert_eq!(
			seen.entries.first().map(|(i, _)| *i),
			Some(3),
			"the first retained entry is j + 1: no hole past the snapshot"
		);
		assert_eq!(
			seen.entries, original.entries,
			"every entry past j recovered, identical bytes"
		);
	}

	/// Test-only file ops that hold the sync of one temporary snapshot artifact (named by its
	/// suffix) at a barrier: the candidate is written and synced up to that point, before any
	/// publication step, and proceeds only when released.
	#[derive(Clone)]
	struct GatedSnapshotSyncFileOps {
		inner: BlockingFileOps,
		suffix: &'static str,
		gated_fds: Arc<Mutex<std::collections::HashSet<i32>>>,
		reached: Arc<Notify>,
		release: Arc<Notify>,
	}

	impl GatedSnapshotSyncFileOps {
		fn new(suffix: &'static str) -> Self {
			Self {
				inner: BlockingFileOps,
				suffix,
				gated_fds: Arc::new(Mutex::new(std::collections::HashSet::new())),
				reached: Arc::new(Notify::new()),
				release: Arc::new(Notify::new()),
			}
		}
	}

	#[async_trait::async_trait]
	impl AsyncFileOps for GatedSnapshotSyncFileOps {
		async fn open(&self, path: &Path, flags: OpenFlags) -> std::io::Result<FileHandle> {
			let handle = self.inner.open(path, flags).await?;
			if path.to_string_lossy().ends_with(self.suffix) && flags.write {
				self.gated_fds.lock().unwrap().insert(handle.raw_fd());
			}
			Ok(handle)
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
			self.inner.write_at(handle, offset, buf).await
		}

		async fn fsync(&self, handle: &FileHandle) -> std::io::Result<()> {
			let result = self.inner.fsync(handle).await;
			let gated = self.gated_fds.lock().unwrap().remove(&handle.raw_fd());
			if gated {
				self.reached.notify_one();
				self.release.notified().await;
			}
			result
		}

		async fn fdatasync(&self, handle: &FileHandle) -> std::io::Result<()> {
			self.inner.fdatasync(handle).await
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

		async fn link_no_replace(&self, from: &Path, to: &Path) -> std::io::Result<()> {
			self.inner.link_no_replace(from, to).await
		}

		async fn unlink(&self, path: &Path) -> std::io::Result<()> {
			self.inner.unlink(path).await
		}
	}

	async fn bounded<F: std::future::Future>(future: F) -> F::Output {
		tokio::time::timeout(Duration::from_secs(30), future)
			.await
			.expect("a bounded wait elapsed")
	}

	/// A source state machine with leases 1..=n applied, and its snapshot.
	async fn source_snapshot(
		tag: &str,
		n: u64,
	) -> (crate::test_support::TestDir, Snapshot<WendelonTypeConfig>) {
		let dir = tempdir(tag);
		let mut store = open_store(&dir).await;
		let mut sm = store.recovered_state_machine().await.unwrap();
		for i in 1..=n {
			append_and_apply(&mut store, &mut sm, lease_entry(i, 10 * i, 10 * i + 9, 3)).await;
		}
		let mut builder =
			<WendelonStateMachine as RaftStateMachine<WendelonTypeConfig>>::get_snapshot_builder(
				&mut sm,
			)
			.await;
		let snapshot = builder.build_snapshot().await.unwrap();
		(dir, snapshot)
	}

	/// The last-included index in a snapshot artifact's header, read from a returned stream.
	async fn header_index(mut stream: Box<SnapshotDataFile>) -> u64 {
		let mut bytes = Vec::new();
		tokio::io::AsyncReadExt::read_to_end(&mut stream, &mut bytes)
			.await
			.unwrap();
		let header_len = std::mem::size_of::<SnapshotHeader>();
		bytemuck::from_bytes::<SnapshotHeader>(&bytes[..header_len]).last_included_index
	}

	/// The current snapshot's metadata and its bytes both name `index`.
	async fn assert_current(sm: &mut WendelonStateMachine, index: u64, label: &str) {
		let current =
			<WendelonStateMachine as RaftStateMachine<WendelonTypeConfig>>::get_current_snapshot(
				sm,
			)
			.await
			.unwrap()
			.expect("a current snapshot");
		assert_eq!(
			current.meta.last_log_id.map(|id| id.index),
			Some(index),
			"{label}: current metadata"
		);
		assert_eq!(
			header_index(current.snapshot).await,
			index,
			"{label}: current bytes"
		);
	}

	/// F-R: a local build of older state (applied 1) is held after writing and syncing its
	/// candidate, before publication; a real newer install (3) publishes; the build resumes. The
	/// published metadata, the current bytes and the durable artifact stay at 3; the stale build
	/// returns its own metadata over its own bytes; an equal-identity build changes nothing.
	#[tokio::test]
	async fn stale_local_build_never_replaces_a_newer_installed_snapshot() {
		let (_newer_dir, newer) = source_snapshot("fr-source", 3).await;
		assert_eq!(newer.meta.last_log_id, Some(log_id(1, 3)));
		let dir = tempdir("fr-target");
		let gate = GatedSnapshotSyncFileOps::new(".build.tmp");
		let mut store =
			open_store_with_async_file_ops(&dir, Arc::new(gate.clone()), 64 * WAL_PAGE).await;
		let mut sm = store.recovered_state_machine().await.unwrap();
		append_and_apply(&mut store, &mut sm, lease_entry(1, 10, 19, 3)).await;
		let mut builder =
			<WendelonStateMachine as RaftStateMachine<WendelonTypeConfig>>::get_snapshot_builder(
				&mut sm,
			)
			.await;
		let build = tokio::spawn(async move { builder.build_snapshot().await });
		bounded(gate.reached.notified()).await;
		<WendelonStateMachine as RaftStateMachine<WendelonTypeConfig>>::install_snapshot(
			&mut sm,
			&newer.meta,
			newer.snapshot,
		)
		.await
		.unwrap();
		gate.release.notify_one();
		let stale = bounded(build)
			.await
			.unwrap()
			.expect("a stale build returns its own snapshot");
		assert_eq!(stale.meta.last_log_id, Some(log_id(1, 1)));
		assert_eq!(
			header_index(stale.snapshot).await,
			1,
			"the stale build's bytes are its own"
		);
		assert_current(&mut sm, 3, "after the stale build").await;

		// An equal-identity build (applied 3) publishes nothing and returns its own bytes.
		let current_path = artifact_paths(&dir).2;
		let before = std::fs::read(&current_path).unwrap();
		gate.release.notify_one();
		let mut builder =
			<WendelonStateMachine as RaftStateMachine<WendelonTypeConfig>>::get_snapshot_builder(
				&mut sm,
			)
			.await;
		let same = bounded(builder.build_snapshot()).await.unwrap();
		assert_eq!(same.meta.last_log_id, Some(log_id(1, 3)));
		assert_eq!(header_index(same.snapshot).await, 3);
		assert_eq!(
			std::fs::read(&current_path).unwrap(),
			before,
			"an equal build does not replace current"
		);
		assert_current(&mut sm, 3, "after the equal build").await;
		drop(sm);
		drop(store);
		let reopened = open_store(&dir).await;
		assert_eq!(
			reopened
				.recovered_state_machine()
				.await
				.unwrap()
				.state_snapshot()
				.await
				.last_applied_log_id,
			Some(log_id(1, 3)),
			"the durable artifact stays at 3"
		);
	}

	/// While builds and an install publish, every current snapshot a reader obtains has
	/// metadata and bytes naming the same artifact.
	#[tokio::test]
	async fn current_snapshot_metadata_and_bytes_agree_under_contention() {
		let (_newer_dir, newer) = source_snapshot("contention-source", 3).await;
		let dir = tempdir("contention-target");
		let mut store = open_store_with_segment_bytes(&dir, 64 * WAL_PAGE).await;
		let mut sm = store.recovered_state_machine().await.unwrap();
		append_and_apply(&mut store, &mut sm, lease_entry(1, 10, 19, 3)).await;
		let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
		let mut reader_sm = sm.clone();
		let reader_stop = stop.clone();
		let reader = tokio::spawn(async move {
			let mut observed = 0usize;
			while !reader_stop.load(std::sync::atomic::Ordering::SeqCst) {
				if let Some(current) = <WendelonStateMachine as RaftStateMachine<
					WendelonTypeConfig,
				>>::get_current_snapshot(&mut reader_sm)
				.await
				.unwrap()
				{
					let index = current.meta.last_log_id.map_or(0, |id| id.index);
					assert_eq!(header_index(current.snapshot).await, index);
					observed += 1;
				}
				tokio::task::yield_now().await;
			}
			observed
		});
		for _ in 0..3 {
			let mut builder =
				<WendelonStateMachine as RaftStateMachine<WendelonTypeConfig>>::get_snapshot_builder(
					&mut sm,
				)
				.await;
			builder.build_snapshot().await.unwrap();
		}
		<WendelonStateMachine as RaftStateMachine<WendelonTypeConfig>>::install_snapshot(
			&mut sm,
			&newer.meta,
			newer.snapshot,
		)
		.await
		.unwrap();
		let mut builder =
			<WendelonStateMachine as RaftStateMachine<WendelonTypeConfig>>::get_snapshot_builder(
				&mut sm,
			)
			.await;
		builder.build_snapshot().await.unwrap();
		stop.store(true, std::sync::atomic::Ordering::SeqCst);
		assert!(bounded(reader).await.unwrap() > 0);
		assert_current(&mut sm, 3, "final").await;
	}

	/// A real install held after syncing its received artifact, before publication: openraft's
	/// ungated purge runs meanwhile and must keep the log (a crash then recovers it whole). On
	/// release the install publishes and its own cleanup deletes the covered log before it
	/// returns; the next entry then appends.
	#[tokio::test]
	async fn install_held_before_publication_keeps_the_purged_log() {
		let (_newer_dir, newer) = source_snapshot("held-source", 3).await;
		let dir = tempdir("held-target");
		let gate = GatedSnapshotSyncFileOps::new(".receive.tmp");
		let mut store =
			open_store_with_async_file_ops(&dir, Arc::new(gate.clone()), WAL_PAGE).await;
		store
			.blocking_append(vec![finalize_entry(1, 100), finalize_entry(2, 200)])
			.await
			.unwrap();
		let original = recovered(&mut store, &dir).await;
		let sm = store.recovered_state_machine().await.unwrap();
		let mut installer = sm.clone();
		let meta = newer.meta.clone();
		let install = tokio::spawn(async move {
			<WendelonStateMachine as RaftStateMachine<WendelonTypeConfig>>::install_snapshot(
				&mut installer,
				&meta,
				newer.snapshot,
			)
			.await
		});
		bounded(gate.reached.notified()).await;
		store.purge(log_id(1, 3)).await.unwrap();
		assert_eq!(
			store.deferred_log_for_test().await,
			(2, 2 * WAL_PAGE),
			"both purged segments are kept and counted"
		);
		let crashed = copy_store(&dir);
		let (_, seen) = reopen(&crashed, WAL_PAGE, "crash while held").await;
		assert_eq!(
			seen.entries, original.entries,
			"the old log is recovered whole"
		);
		gate.release.notify_one();
		bounded(install).await.unwrap().unwrap();
		assert_eq!(
			store.deferred_log_for_test().await,
			(0, 0),
			"cleanup ran in the install"
		);
		assert!(segment_files(&dir).is_empty(), "{:?}", segment_files(&dir));
		store
			.blocking_append(vec![finalize_entry(4, 400)])
			.await
			.unwrap();
		drop(sm);
		drop(store);
		let (_, seen) = reopen(&dir, WAL_PAGE, "after").await;
		assert_eq!(
			seen.entries.iter().map(|(i, _)| *i).collect::<Vec<_>>(),
			vec![4]
		);
	}

	/// A failure after a rename of `current` latches: readers, publishers and export refuse
	/// until reopen, which validates what is on disk.
	#[tokio::test]
	async fn failed_publication_refuses_readers_and_publishers_until_reopen() {
		let dir = tempdir("publication-failed");
		let file_ops = Arc::new(FaultyFileOps::new(dir.to_path_buf()).unwrap());
		let mut store = open_store_with_faults(&dir, file_ops.clone(), WAL_PAGE).await;
		let mut sm = store.recovered_state_machine().await.unwrap();
		append_and_apply(&mut store, &mut sm, lease_entry(1, 10, 19, 3)).await;
		file_ops.inject(
			InjectedFault::on(
				FaultOp::FsyncDir,
				FaultAction::Error {
					kind: ErrorKind::Other,
					message: "injected snapshot directory sync failure".into(),
				},
			)
			.for_path(dir.to_path_buf()),
		);
		let mut builder =
			<WendelonStateMachine as RaftStateMachine<WendelonTypeConfig>>::get_snapshot_builder(
				&mut sm,
			)
			.await;
		assert!(builder.build_snapshot().await.is_err());
		let files = artifact_bytes(&dir);
		assert!(
			<WendelonStateMachine as RaftStateMachine<WendelonTypeConfig>>::get_current_snapshot(
				&mut sm
			)
			.await
			.is_err(),
			"a reader refuses"
		);
		assert!(
			builder.build_snapshot().await.is_err(),
			"a publisher refuses"
		);
		assert!(store.export_backup(&sm).await.is_err(), "export refuses");
		assert_eq!(artifact_bytes(&dir), files, "refusals have no effect");
		drop(builder);
		drop(sm);
		drop(store);
		let reopened = open_store(&dir).await;
		let mut recovered_sm = reopened.recovered_state_machine().await.unwrap();
		assert_current(&mut recovered_sm, 1, "reopen").await;
	}

	/// A snapshot at the published index with a different full log id is a conflict: refused
	/// before any effect.
	#[tokio::test]
	async fn same_index_conflicting_snapshot_is_refused_before_effects() {
		let dir = tempdir("conflict-source");
		let mut source_store = open_store(&dir).await;
		let mut source_sm = source_store.recovered_state_machine().await.unwrap();
		let mut entry = lease_entry(1, 10, 19, 3);
		entry.log_id = log_id(2, 1);
		append_and_apply(&mut source_store, &mut source_sm, entry).await;
		let mut builder =
			<WendelonStateMachine as RaftStateMachine<WendelonTypeConfig>>::get_snapshot_builder(
				&mut source_sm,
			)
			.await;
		let conflicting = builder.build_snapshot().await.unwrap();
		assert_eq!(conflicting.meta.last_log_id, Some(log_id(2, 1)));

		let target = tempdir("conflict-target");
		let mut store = open_store(&target).await;
		let mut sm = store.recovered_state_machine().await.unwrap();
		append_and_apply(&mut store, &mut sm, lease_entry(1, 10, 19, 3)).await;
		let mut builder =
			<WendelonStateMachine as RaftStateMachine<WendelonTypeConfig>>::get_snapshot_builder(
				&mut sm,
			)
			.await;
		builder.build_snapshot().await.unwrap();
		let current = std::fs::read(artifact_paths(&target).2).unwrap();
		assert!(
			<WendelonStateMachine as RaftStateMachine<WendelonTypeConfig>>::install_snapshot(
				&mut sm,
				&conflicting.meta,
				conflicting.snapshot,
			)
			.await
			.is_err()
		);
		assert_eq!(std::fs::read(artifact_paths(&target).2).unwrap(), current);
		assert_current(&mut sm, 1, "after the refused conflict").await;
	}

	/// Purge before any durable snapshot keeps the purged bytes; truncate with no logical entry
	/// below k shrinks the straddling segment around them, and repeated purge, truncate and
	/// append keep a contiguous recoverable log with exact bytes.
	#[tokio::test]
	async fn truncate_keeps_the_deferred_prefix() {
		let dir = tempdir("deferred-truncate");
		let mut store = try_open_store_with_segment_bytes(&dir, 2 * WAL_PAGE)
			.await
			.unwrap();
		store
			.blocking_append(
				(1..=5)
					.map(|i| finalize_entry(i, 100 * i))
					.collect::<Vec<_>>(),
			)
			.await
			.unwrap();
		let original = recovered(&mut store, &dir).await;
		store.purge(log_id(1, 3)).await.unwrap();
		assert_eq!(store.deferred_log_for_test().await, (1, 3 * WAL_PAGE));
		store.truncate(log_id(1, 4)).await.unwrap();
		assert_eq!(
			segment_metadata(&store).await,
			vec![(1, Some(2), 2 * WAL_PAGE), (3, Some(3), WAL_PAGE)],
			"the straddler shrinks around entry 3; the suffix goes"
		);
		let four = finalize_entry(4, 999);
		store
			.blocking_append(vec![clone_entry(&four)])
			.await
			.unwrap();
		store.purge(log_id(1, 4)).await.unwrap();
		let five = finalize_entry(5, 555);
		store
			.blocking_append(vec![clone_entry(&five)])
			.await
			.unwrap();
		assert_eq!(
			segment_metadata(&store)
				.await
				.last()
				.map(|(start, _, _)| *start),
			Some(5),
			"after purge, the next entry starts its own segment"
		);
		drop(store);
		let (_, seen) = reopen(&dir, 2 * WAL_PAGE, "deferred truncate").await;
		let mut expect = original.entries[..3].to_vec();
		expect.extend(entry_bytes(&[four, five]));
		assert_eq!(
			seen.entries, expect,
			"1..3 original, 4 and 5 replaced, contiguous"
		);
	}

	/// A purge past the local tail followed by publication of the covering snapshot: cleanup
	/// removes the covered log and the next entry starts the segment named for it.
	#[tokio::test]
	async fn append_after_purge_past_the_tail_starts_its_own_segment() {
		let dir = tempdir("purge-past-tail-append");
		let mut store = try_open_store_with_segment_bytes(&dir, 2 * WAL_PAGE)
			.await
			.unwrap();
		store
			.blocking_append(vec![finalize_entry(1, 100), finalize_entry(2, 200)])
			.await
			.unwrap();
		store.purge(log_id(1, 5)).await.unwrap();
		assert_eq!(
			segment_files(&dir),
			vec![(1, 2 * WAL_PAGE)],
			"kept until durable"
		);
		snapshot_at(&dir, 5);
		store.note_published_snapshot_for_test(log_id(1, 5)).await;
		store.complete_deferred_purge().await.unwrap();
		assert!(segment_files(&dir).is_empty());
		store
			.blocking_append(vec![finalize_entry(6, 600)])
			.await
			.unwrap();
		assert_eq!(segment_metadata(&store).await, vec![(6, Some(6), WAL_PAGE)]);
		drop(store);
		let (_, seen) = reopen(&dir, 2 * WAL_PAGE, "past tail").await;
		assert_eq!(
			seen.entries.iter().map(|(i, _)| *i).collect::<Vec<_>>(),
			vec![6]
		);
	}

	/// A crash after a snapshot became durable but before its cleanup: open removes exactly the
	/// log the validated snapshot covers.
	#[tokio::test]
	async fn open_cleans_the_log_a_durable_snapshot_covers() {
		let dir = tempdir("open-cleanup");
		let mut store = try_open_store_with_segment_bytes(&dir, 2 * WAL_PAGE)
			.await
			.unwrap();
		store
			.blocking_append(
				(1..=5)
					.map(|i| finalize_entry(i, 100 * i))
					.collect::<Vec<_>>(),
			)
			.await
			.unwrap();
		let original = recovered(&mut store, &dir).await;
		store.purge(log_id(1, 4)).await.unwrap();
		assert_eq!(
			segment_files(&dir).len(),
			3,
			"nothing removed before durability"
		);
		drop(store);
		snapshot_at(&dir, 4);
		let (_, seen) = reopen(&dir, 2 * WAL_PAGE, "open cleanup").await;
		assert_eq!(seen.files, vec![(5, WAL_PAGE)]);
		assert_eq!(seen.entries, original.entries[4..].to_vec());
	}

	/// A cleanup failure after the snapshot is durable poisons the store and fails the install;
	/// reopen completes the cleanup.
	#[tokio::test]
	async fn cleanup_failure_fails_the_install_and_reopen_completes_it() {
		let (_newer_dir, newer) = source_snapshot("cleanup-fail-source", 3).await;
		let dir = tempdir("cleanup-fail");
		let file_ops = Arc::new(FaultyFileOps::new(dir.to_path_buf()).unwrap());
		let mut store = open_store_with_faults(&dir, file_ops.clone(), WAL_PAGE).await;
		store
			.blocking_append(vec![finalize_entry(1, 100), finalize_entry(2, 200)])
			.await
			.unwrap();
		store.purge(log_id(1, 3)).await.unwrap();
		let mut sm = store.recovered_state_machine().await.unwrap();
		file_ops.inject(InjectedFault::on(
			FaultOp::Unlink,
			FaultAction::Error {
				kind: ErrorKind::Other,
				message: "injected cleanup unlink failure".into(),
			},
		));
		assert!(
			<WendelonStateMachine as RaftStateMachine<WendelonTypeConfig>>::install_snapshot(
				&mut sm,
				&newer.meta,
				newer.snapshot,
			)
			.await
			.is_err(),
			"the install fails"
		);
		assert!(
			store.inner.read().await.poisoned.is_some(),
			"the store is poisoned"
		);
		assert!(
			store
				.blocking_append(vec![finalize_entry(4, 400)])
				.await
				.is_err()
		);
		drop(sm);
		drop(store);
		let (_, seen) = reopen(&dir, WAL_PAGE, "after cleanup failure").await;
		assert!(seen.files.is_empty(), "{:?}", seen.files);
		assert!(seen.entries.is_empty());
	}

	/// The replay guard refuses a missing first entry or an interior hole before any effect.
	#[tokio::test]
	async fn replay_refuses_a_missing_first_or_interior_entry_without_effect() {
		let page = |i: u64| serialize_entry_page(&finalize_entry(i, 100 * i)).unwrap();
		let across = tempdir("guard-across");
		write_wal_pages(&segment_file(&across, 1), &[page(1), page(2)]);
		write_wal_pages(&segment_file(&across, 4), &[page(4)]);
		let within = tempdir("guard-within");
		write_wal_pages(&segment_file(&within, 1), &[page(1), page(2), page(4)]);
		let first = tempdir("guard-first");
		write_wal_pages(&segment_file(&first, 3), &[page(3), page(4)]);
		snapshot_at(&first, 1);
		for dir in [across, within, first] {
			let files = artifact_bytes(&dir);
			let err = try_open_store_with_segment_bytes(&dir, 4 * WAL_PAGE)
				.await
				.err()
				.unwrap_or_else(|| panic!("{} refuses open", dir.display()));
			assert_eq!(err.kind(), ErrorKind::InvalidData, "{err}");
			assert_eq!(artifact_bytes(&dir), files, "{}: no effect", dir.display());
		}
	}

	/// A local build publishes before openraft schedules its purge, so the purge after it
	/// deletes immediately, as before.
	#[tokio::test]
	async fn purge_after_a_local_build_deletes_immediately() {
		let dir = tempdir("build-parity");
		let mut store = open_store_with_segment_bytes(&dir, WAL_PAGE).await;
		let mut sm = store.recovered_state_machine().await.unwrap();
		for i in 1..=3 {
			append_and_apply(&mut store, &mut sm, lease_entry(i, 10 * i, 10 * i + 9, 3)).await;
		}
		append_and_apply(&mut store, &mut sm, lease_entry(4, 50, 59, 3)).await;
		let mut builder =
			<WendelonStateMachine as RaftStateMachine<WendelonTypeConfig>>::get_snapshot_builder(
				&mut sm,
			)
			.await;
		builder.build_snapshot().await.unwrap();
		store.purge(log_id(1, 3)).await.unwrap();
		assert_eq!(store.deferred_log_for_test().await, (0, 0));
		assert_eq!(segment_files(&dir), vec![(4, WAL_PAGE)]);
	}

	/// The legacy first-entry exemption belongs to an entry the legacy file holds: an empty or
	/// checkpoint-only legacy file followed by segments starting at 33 is refused before effect.
	#[tokio::test]
	async fn legacy_exemption_needs_the_first_entry_from_the_legacy_file() {
		let page = |i: u64| serialize_entry_page(&finalize_entry(i, 100 * i)).unwrap();
		for checkpoint_only in [false, true] {
			let dir = tempdir("legacy-origin");
			let (wal, _, _) = artifact_paths(&dir);
			if checkpoint_only {
				write_wal_pages(&wal, &[serialize_checkpoint_page(0, 0, 0, 0)]);
			} else {
				std::fs::File::create(&wal).unwrap();
			}
			write_wal_pages(&segment_file(&dir, 33), &[page(33), page(34)]);
			let files = artifact_bytes(&dir);
			let err = try_open_store_with_segment_bytes(&dir, 4 * WAL_PAGE)
				.await
				.err()
				.unwrap_or_else(|| panic!("checkpoint-only {checkpoint_only}: refuses open"));
			assert_eq!(err.kind(), ErrorKind::InvalidData, "{err}");
			assert_eq!(
				artifact_bytes(&dir),
				files,
				"checkpoint-only {checkpoint_only}: no effect"
			);
		}
	}

	/// Open-time cleanup preserves C′ in the holder before each deletion attempt; interrupted
	/// again and again, the holder stays within the segment size (full, it is replaced), holds
	/// only intact checkpoint records ending in C′, and a final open completes.
	#[tokio::test]
	async fn interrupted_open_cleanup_never_grows_a_holder_past_the_segment() {
		let c3 = serialize_checkpoint_page(3, 3, 3, 1);
		let dir = tempdir("cleanup-bound");
		let pages: Vec<AlignedBuf> = (1..=3)
			.map(|i| serialize_entry_page(&finalize_entry(i, 100 * i)).unwrap())
			.collect();
		write_wal_pages(&segment_file(&dir, 1), &pages);
		write_wal_pages(
			&segment_file(&dir, 4),
			&[serialize_checkpoint_page(3, 3, 3, 1)],
		);
		snapshot_at(&dir, 3);
		let mut lengths = Vec::new();
		for round in 0..6 {
			let file_ops = Arc::new(FaultyFileOps::new(dir.to_path_buf()).unwrap());
			file_ops.inject(
				InjectedFault::on(
					FaultOp::Unlink,
					FaultAction::Error {
						kind: ErrorKind::Other,
						message: "injected cleanup unlink failure".into(),
					},
				)
				.for_path(segment_file(&dir, 1)),
			);
			assert!(
				try_open_faulty(&dir, file_ops, 3 * WAL_PAGE).await.is_err(),
				"round {round}: the interrupted cleanup aborts open"
			);
			let holder = std::fs::read(segment_file(&dir, 4)).unwrap();
			assert!(
				holder.len() as u64 <= 3 * WAL_PAGE,
				"round {round}: {}",
				holder.len()
			);
			assert!(holder.chunks(WAL_PAGE as usize).all(is_checkpoint_page));
			assert!(holder.ends_with(c3.as_slice()));
			assert!(
				segment_file(&dir, 1).exists(),
				"round {round}: the log is not yet removed"
			);
			lengths.push(holder.len() as u64 / WAL_PAGE);
		}
		assert!(
			lengths.contains(&3) && lengths.contains(&1),
			"{lengths:?}: full, then replaced"
		);
		let (_, seen) = reopen(&dir, 3 * WAL_PAGE, "completed").await;
		assert_eq!(
			seen.files
				.iter()
				.map(|(start, _)| *start)
				.collect::<Vec<_>>(),
			vec![4]
		);
		assert_eq!(
			seen.checkpoint,
			Some(RecoveryCheckpoint {
				commit_index: 3,
				e_safe_index: 3,
			})
		);
	}

	/// The pending-cleanup shape for F-P: entry 1 applied, a checkpoint-only holder "2" (C =
	/// {1,1}), then a real build whose post-rename directory sync fails, so publication is failed.
	/// With `purged`, a purge(1) ran first (logical boundary set, the log deferred).
	async fn failed_publication_with_pending_holder(
		tag: &str,
		purged: bool,
	) -> (
		crate::test_support::TestDir,
		WendelonLogStore,
		WendelonStateMachine,
	) {
		let dir = tempdir(tag);
		let file_ops = Arc::new(FaultyFileOps::new(dir.to_path_buf()).unwrap());
		let mut store = open_store_with_faults(&dir, file_ops.clone(), WAL_PAGE).await;
		let mut sm = store.recovered_state_machine().await.unwrap();
		append_and_apply(&mut store, &mut sm, lease_entry(1, 10, 19, 3)).await;
		checkpoint(&store, 1, 1).await;
		if purged {
			store.purge(log_id(1, 1)).await.unwrap();
		}
		assert_eq!(
			segment_metadata(&store).await,
			vec![(1, Some(1), WAL_PAGE), (2, None, WAL_PAGE)]
		);
		file_ops.inject(
			InjectedFault::on(
				FaultOp::FsyncDir,
				FaultAction::Error {
					kind: ErrorKind::Other,
					message: "injected snapshot directory sync failure".into(),
				},
			)
			.for_path(dir.to_path_buf()),
		);
		let mut builder =
			<WendelonStateMachine as RaftStateMachine<WendelonTypeConfig>>::get_snapshot_builder(
				&mut sm,
			)
			.await;
		assert!(
			builder.build_snapshot().await.is_err(),
			"publication fails after its rename"
		);
		(dir, store, sm)
	}

	/// Everything the log store holds, logically and physically, for no-change assertions.
	#[derive(Debug, PartialEq)]
	struct StoreState {
		last_log_id: Option<LogId<u32>>,
		last_purged: Option<LogId<u32>>,
		entries: Vec<(u64, Vec<u8>)>,
		entry_positions: BTreeMap<u64, WalRecordPointer>,
		deferred_positions: BTreeMap<u64, WalRecordPointer>,
		wal_segments: Vec<WalSegmentMeta>,
		wal_cursor: u64,
	}

	async fn store_state(store: &WendelonLogStore) -> StoreState {
		let guard = store.inner.read().await;
		StoreState {
			last_log_id: guard.last_log_id,
			last_purged: guard.last_purged,
			entries: entry_bytes(&guard.entries.values().map(clone_entry).collect::<Vec<_>>()),
			entry_positions: guard.entry_positions.clone(),
			deferred_positions: guard.deferred_positions.clone(),
			wal_segments: guard.wal_segments.clone(),
			wal_cursor: guard.wal_cursor,
		}
	}

	#[derive(Clone, Copy, Debug)]
	enum AfterFailedPublication {
		/// purge(3): past the checkpoint-only holder "2", which a purge would move to "4".
		Purge,
		/// The deferred cleanup a publication triggers.
		Cleanup,
	}

	/// F-P: once publication has failed, `op` refuses before any effect or logical change; the
	/// store, its files and the refusal of readers are unchanged; reopen resolves the artifact.
	async fn failed_publication_refuses(op: AfterFailedPublication, purged: bool) {
		let label = format!("{op:?}, purged first {purged}");
		let (dir, mut store, mut sm) =
			failed_publication_with_pending_holder("failed-publication", purged).await;
		let files = artifact_bytes(&dir);
		let segments_before = segment_files(&dir);
		let state = store_state(&store).await;
		let result = match op {
			AfterFailedPublication::Purge => store
				.purge(log_id(1, 3))
				.await
				.map_err(|err| err.to_string()),
			AfterFailedPublication::Cleanup => store
				.complete_deferred_purge()
				.await
				.map_err(|err| err.to_string()),
		};
		let files_after = artifact_bytes(&dir);
		let state_after = store_state(&store).await;
		eprintln!(
			"{label}: result {result:?}; segments before {segments_before:?} after {:?}; last_purged before {:?} after {:?}",
			segment_files(&dir),
			state.last_purged,
			state_after.last_purged
		);
		assert!(result.is_err(), "{label}: refuses, got {result:?}");
		assert_eq!(files_after, files, "{label}: no file effect");
		assert_eq!(state_after, state, "{label}: no logical or physical change");
		assert!(
			<WendelonStateMachine as RaftStateMachine<WendelonTypeConfig>>::get_current_snapshot(
				&mut sm
			)
			.await
			.is_err(),
			"{label}: readers still refuse"
		);
		drop(sm);
		drop(store);
		let reopened = open_store(&dir).await;
		let mut recovered = reopened.recovered_state_machine().await.unwrap();
		assert_current(&mut recovered, 1, &label).await;
	}

	#[tokio::test]
	async fn failed_publication_refuses_purge_past_a_holder_before_effects() {
		failed_publication_refuses(AfterFailedPublication::Purge, true).await;
	}

	#[tokio::test]
	async fn failed_publication_refuses_purge_with_no_prior_purge_before_effects() {
		failed_publication_refuses(AfterFailedPublication::Purge, false).await;
	}

	#[tokio::test]
	async fn failed_publication_refuses_cleanup_before_effects() {
		failed_publication_refuses(AfterFailedPublication::Cleanup, true).await;
	}

	#[tokio::test]
	async fn failed_publication_refuses_cleanup_with_no_prior_purge() {
		failed_publication_refuses(AfterFailedPublication::Cleanup, false).await;
	}

	/// Test-only file ops that fail opening one path while armed: the builder's open of `current`
	/// for its returned handle, after a successful rename and directory sync.
	#[derive(Clone)]
	struct FailCurrentOpenFileOps {
		inner: BlockingFileOps,
		current: PathBuf,
		armed: Arc<std::sync::atomic::AtomicBool>,
	}

	#[async_trait::async_trait]
	impl AsyncFileOps for FailCurrentOpenFileOps {
		async fn open(&self, path: &Path, flags: OpenFlags) -> std::io::Result<FileHandle> {
			if path == self.current && self.armed.load(std::sync::atomic::Ordering::SeqCst) {
				return Err(Error::other("injected open failure of current"));
			}
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
			self.inner.write_at(handle, offset, buf).await
		}

		async fn fsync(&self, handle: &FileHandle) -> std::io::Result<()> {
			self.inner.fsync(handle).await
		}

		async fn fdatasync(&self, handle: &FileHandle) -> std::io::Result<()> {
			self.inner.fdatasync(handle).await
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

		async fn link_no_replace(&self, from: &Path, to: &Path) -> std::io::Result<()> {
			self.inner.link_no_replace(from, to).await
		}

		async fn unlink(&self, path: &Path) -> std::io::Result<()> {
			self.inner.unlink(path).await
		}
	}

	async fn publication_state(store: &WendelonLogStore) -> PublicationState {
		*store.publication.state.lock().await
	}

	/// After a terminal publication failure: every reader, publisher, purge, cleanup and export
	/// refuses, and none of them changes a file.
	async fn assert_terminal_refusals(
		store: &mut WendelonLogStore,
		sm: &mut WendelonStateMachine,
		dir: &Path,
		label: &str,
	) {
		let files = artifact_bytes(dir);
		assert!(
			<WendelonStateMachine as RaftStateMachine<WendelonTypeConfig>>::get_current_snapshot(
				sm
			)
			.await
			.is_err(),
			"{label}: a reader refuses"
		);
		let mut builder =
			<WendelonStateMachine as RaftStateMachine<WendelonTypeConfig>>::get_snapshot_builder(
				sm,
			)
			.await;
		assert!(
			builder.build_snapshot().await.is_err(),
			"{label}: a publisher refuses"
		);
		assert!(
			store.purge(log_id(1, 1)).await.is_err(),
			"{label}: purge refuses"
		);
		assert!(
			store.complete_deferred_purge().await.is_err(),
			"{label}: cleanup refuses"
		);
		assert!(
			store.export_backup(sm).await.is_err(),
			"{label}: export refuses"
		);
		assert_eq!(
			artifact_bytes(dir),
			files,
			"{label}: no refusal changes a file"
		);
	}

	/// Binding note B: the builder's open of the `current` handle fails after a successful rename
	/// and directory sync. Publication latches terminal failure carrying the durable candidate;
	/// everything refuses until reopen, which validates and serves the candidate.
	#[tokio::test]
	async fn build_handle_open_failure_after_durable_publication_latches() {
		let dir = tempdir("build-handle-open-failure");
		let armed = Arc::new(std::sync::atomic::AtomicBool::new(false));
		let file_ops = FailCurrentOpenFileOps {
			inner: BlockingFileOps,
			current: artifact_paths(&dir).2,
			armed: armed.clone(),
		};
		let mut store = open_store_with_async_file_ops(&dir, Arc::new(file_ops), WAL_PAGE).await;
		let mut sm = store.recovered_state_machine().await.unwrap();
		append_and_apply(&mut store, &mut sm, lease_entry(1, 10, 19, 3)).await;
		armed.store(true, std::sync::atomic::Ordering::SeqCst);
		let mut builder =
			<WendelonStateMachine as RaftStateMachine<WendelonTypeConfig>>::get_snapshot_builder(
				&mut sm,
			)
			.await;
		assert!(builder.build_snapshot().await.is_err(), "the build fails");
		assert_eq!(
			publication_state(&store).await,
			PublicationState::Failed {
				durable: PublishedSnapshot::Artifact(Some(log_id(1, 1))),
				step: "build handle open",
			},
			"terminal failure carries the durable candidate"
		);
		assert_terminal_refusals(&mut store, &mut sm, &dir, "build handle open").await;
		drop(builder);
		drop(sm);
		drop(store);
		let reopened = open_store(&dir).await;
		let mut recovered = reopened.recovered_state_machine().await.unwrap();
		assert_current(&mut recovered, 1, "reopen after build handle open failure").await;
	}

	/// Binding note B: an install whose state update fails after publication, here a real
	/// conflict between the snapshot's DROP fence and the receiving drain registry. The candidate
	/// is durable; publication latches terminal failure carrying it; nothing exposes old metadata
	/// over its bytes; reopen validates and serves it.
	#[tokio::test]
	async fn install_state_update_failure_after_durable_publication_latches() {
		let tenant_id = TenantId::new(7);
		let table_object_id = TableObjectId::new([5; 16]);
		let fenced = DropOperationId {
			tenant_id,
			table_object_id,
			schema_generation: SchemaGeneration::new(2),
		};
		let other = DropOperationId {
			schema_generation: SchemaGeneration::new(1),
			..fenced
		};
		let source = tempdir("install-drain-conflict-source");
		let mut state = SmState::default();
		state.last_applied_log_id = Some(log_id(1, 3));
		state.table_drop_fences.prepare(fenced, 3).unwrap();
		let source_path = source.join("snapshot.wnd");
		write_snapshot_file(&source_path, &state);
		let meta = SnapshotMeta {
			last_log_id: state.last_applied_log_id,
			last_membership: state.last_membership.clone(),
			snapshot_id: "drain-conflict".to_string(),
		};
		let data = SnapshotDataFile::open(Arc::new(BlockingFileOps), source_path)
			.await
			.unwrap();

		let dir = tempdir("install-drain-conflict");
		let mut store = open_store(&dir).await;
		let mut sm = store.recovered_state_machine().await.unwrap();
		sm.mutation_drain.close_committed(other).unwrap();
		assert!(
			<WendelonStateMachine as RaftStateMachine<WendelonTypeConfig>>::install_snapshot(
				&mut sm,
				&meta,
				Box::new(data),
			)
			.await
			.is_err(),
			"the install fails in its state update"
		);
		assert_eq!(
			publication_state(&store).await,
			PublicationState::Failed {
				durable: PublishedSnapshot::Artifact(Some(log_id(1, 3))),
				step: "install state update",
			},
			"terminal failure carries the durable candidate"
		);
		assert!(
			sm.inner.read().await.snapshot.is_none(),
			"the held snapshot was never set to the candidate"
		);
		assert_terminal_refusals(&mut store, &mut sm, &dir, "install state update").await;
		drop(sm);
		drop(store);
		let reopened = open_store(&dir).await;
		let mut recovered = reopened.recovered_state_machine().await.unwrap();
		assert_current(
			&mut recovered,
			3,
			"reopen after install state update failure",
		)
		.await;
		assert!(
			recovered
				.state_snapshot()
				.await
				.table_drop_fences
				.is_fenced(tenant_id, table_object_id),
			"the candidate's state is what reopen serves"
		);
	}

	/// Binding note C: an artifact of empty state (last applied None) is distinct from no
	/// artifact. It publishes, is served with matching metadata and bytes, survives an equal
	/// build unchanged, reopens as itself, and gives no authority to delete log.
	#[tokio::test]
	async fn empty_state_artifact_is_published_but_authorizes_no_deletion() {
		let dir = tempdir("empty-state-artifact");
		let mut store = try_open_store_with_segment_bytes(&dir, WAL_PAGE)
			.await
			.unwrap();
		let mut sm = store.recovered_state_machine().await.unwrap();
		assert_eq!(
			publication_state(&store).await,
			PublicationState::Healthy(PublishedSnapshot::NoArtifact)
		);
		let mut builder =
			<WendelonStateMachine as RaftStateMachine<WendelonTypeConfig>>::get_snapshot_builder(
				&mut sm,
			)
			.await;
		let built = builder.build_snapshot().await.unwrap();
		assert_eq!(built.meta.last_log_id, None);
		assert_eq!(
			publication_state(&store).await,
			PublicationState::Healthy(PublishedSnapshot::Artifact(None))
		);
		let current_path = artifact_paths(&dir).2;
		let published = std::fs::read(&current_path).unwrap();
		let current =
			<WendelonStateMachine as RaftStateMachine<WendelonTypeConfig>>::get_current_snapshot(
				&mut sm,
			)
			.await
			.unwrap()
			.expect("an empty-state artifact is current");
		assert_eq!(current.meta.last_log_id, None);
		let mut served = Vec::new();
		let mut stream = current.snapshot;
		tokio::io::AsyncReadExt::read_to_end(&mut stream, &mut served)
			.await
			.unwrap();
		assert_eq!(
			served, published,
			"metadata and bytes name the same artifact"
		);

		let same = builder.build_snapshot().await.unwrap();
		assert_eq!(same.meta.last_log_id, None);
		assert_eq!(
			std::fs::read(&current_path).unwrap(),
			published,
			"an equal empty build keeps the authoritative bytes"
		);

		store
			.blocking_append(vec![finalize_entry(1, 100), finalize_entry(2, 200)])
			.await
			.unwrap();
		let segments = segment_files(&dir);
		store.purge(log_id(1, 1)).await.unwrap();
		assert_eq!(
			segment_files(&dir),
			segments,
			"an empty-state artifact deletes no log"
		);
		assert_eq!(store.deferred_log_for_test().await.0, 1);
		drop(builder);
		drop(sm);
		drop(store);
		let mut reopened = try_open_store_with_segment_bytes(&dir, WAL_PAGE)
			.await
			.unwrap();
		assert_eq!(
			publication_state(&reopened).await,
			PublicationState::Healthy(PublishedSnapshot::Artifact(None)),
			"reopen recovers the empty-state artifact, not no artifact"
		);
		let mut recovered_sm = reopened.recovered_state_machine().await.unwrap();
		let current =
			<WendelonStateMachine as RaftStateMachine<WendelonTypeConfig>>::get_current_snapshot(
				&mut recovered_sm,
			)
			.await
			.unwrap()
			.expect("current after reopen");
		assert_eq!(current.meta.last_log_id, None);
		assert_eq!(
			recovered(&mut reopened, &dir)
				.await
				.entries
				.iter()
				.map(|(i, _)| *i)
				.collect::<Vec<_>>(),
			vec![1, 2],
			"no prefix was deleted on the empty artifact's authority"
		);
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
/// (version 5) shapes two ways: a
/// stored golden encoding per request variant and per changed container, and
/// an exact-set guard over the variant lists, because an appended enum variant
/// moves no stored bytes and only the set check can see it. Any failure here
/// means the body shape moved: the re-pin travels with a
/// `WAL_FORMAT_VERSION`/`SNAPSHOT_FORMAT_VERSION` bump, a frozen fixture of
/// the outgoing version, and its replay control — never alone.
#[cfg(test)]
pub(crate) mod body_format_corpus {
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

	pub(crate) fn canonical_requests() -> Vec<(&'static str, WendelonRequest)> {
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
			(
				"root2_command",
				WendelonRequest::Root2Command({
					let v: serde_json::Value = serde_json::from_str(include_str!(
						"../storage/mutation_visibility/root2/tests/goldens.json"
					))
					.unwrap();
					let hex = v["fixtures"]
						.as_array()
						.unwrap()
						.iter()
						.find(|f| f["name"] == "command_bootstrap")
						.unwrap()["hex"]
						.as_str()
						.unwrap();
					let bytes = (0..hex.len())
						.step_by(2)
						.map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
						.collect::<Vec<_>>();
					crate::consensus::Root2Carrier1::new(&bytes, Some(&[0x67; 296])).unwrap()
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

	// Frozen before adding conditional promoted-JOIN commands. Never regenerate
	// these bytes to repair drift; fork any payload leaf that subsequently moves.
	const WAL_V5_OUTGOING_MANIFEST: &str =
		include_str!("../../tests/fixtures/consensus_body/wal_v5_entries.txt");
	const WAL_V5_OUTGOING_NAMES: [&str; 29] = [
		"finalize",
		"acquire_lease",
		"release_lease",
		"advance_epoch",
		"schema_update",
		"tier_slab",
		"retire_tiered_slab",
		"compaction_commit",
		"seed_migration",
		"migration_complete",
		"create_tenant",
		"update_tenant_quota",
		"suspend_tenant",
		"resume_tenant",
		"delete_tenant",
		"install_tenant_kek_version",
		"retire_tenant_kek_version",
		"revoke_actor_key",
		"set_table_retention_policy",
		"table_drop_prepare",
		"table_reclaim_completion",
		"table_drop_finalize",
		"ctas_saga",
		"replay_legacy_ctas_row_dispatched",
		"table_drop_logical_finalize",
		"table_drop_settlement",
		"root2_command",
		"membership",
		"blank",
	];

	#[test]
	fn pd11_frozen_outgoing_v5_spine_and_corpus_are_byte_exact() {
		let stored = parse_manifest(WAL_V5_OUTGOING_MANIFEST);
		let mut names = WAL_V5_OUTGOING_NAMES.map(str::to_owned).to_vec();
		names.sort();
		assert_eq!(stored.keys().cloned().collect::<Vec<_>>(), names);
		let live: BTreeMap<_, _> = wal_corpus().into_iter().collect();
		for (name, payload) in stored {
			assert_eq!(
				&live[name.as_str()],
				&payload,
				"outgoing v5 leaf drift for {name}"
			);
			let (frozen, rest) = postcard::take_from_bytes::<
				Entry<crate::consensus::wal_body::v5::WendelonWalV5TypeConfig>,
			>(&payload)
			.unwrap();
			assert!(rest.is_empty(), "frozen v5 decoder left bytes for {name}");
			assert_eq!(postcard::to_stdvec(&frozen).unwrap(), payload);
			let converted = crate::consensus::wal_body::v5::decode_entry(&payload).unwrap();
			assert_eq!(postcard::to_stdvec(&converted).unwrap(), payload);
		}
	}

	#[test]
	fn pd11_outgoing_v5_production_replay_matches_frozen_spine_and_rejects_suffix() {
		for (name, payload) in parse_manifest(WAL_V5_OUTGOING_MANIFEST) {
			let frozen = crate::consensus::wal_body::v5::decode_entry(&payload).unwrap();
			let production = super::root2_carrier_tests::record(&payload, 5).unwrap();
			assert_eq!(
				postcard::to_stdvec(&production).unwrap(),
				postcard::to_stdvec(&frozen).unwrap(),
				"production replay drift for {name}"
			);
			let mut suffix = payload;
			suffix.push(0);
			assert!(
				crate::consensus::wal_body::v5::decode_entry(&suffix)
					.unwrap_err()
					.to_string()
					.contains("unconsumed payload bytes")
			);
			assert!(
				super::root2_carrier_tests::record(&suffix, 5)
					.unwrap_err()
					.to_string()
					.contains("format mismatch")
			);
		}
	}

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
						| "root2_command"
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
					"table_drop_logical_finalize" | "table_drop_settlement" | "root2_command"
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
		let corpus = wal_corpus()
			.into_iter()
			.filter(|(name, _)| *name != "root2_command")
			.collect::<Vec<_>>();
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
		std::fs::write(dir.join("wal_v5_entries.txt"), wal).unwrap();
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

#[cfg(test)]
#[path = "root2_carrier/wal_tests.rs"]
mod root2_carrier_tests;

// Narrow fixture access for the carrier compatibility controls. Shadow shapes remain
// confined to this replay/format-control file; no arbitrary body decoder is exported.
#[cfg(test)]
pub(crate) fn carrier_legacy_fixture_entry(
	version: u8,
	name: &str,
) -> std::io::Result<Entry<WendelonTypeConfig>> {
	let manifest = match version {
		0 | 4 => include_str!("../../tests/fixtures/consensus_body/wal_v4_entries.txt"),
		1 => include_str!("../../tests/fixtures/consensus_body/wal_v1_entries.txt"),
		2 => include_str!("../../tests/fixtures/consensus_body/wal_v2_entries.txt"),
		3 => include_str!("../../tests/fixtures/consensus_body/wal_v3_entries.txt"),
		_ => {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"unsupported legacy carrier fixture version",
			));
		}
	};
	let hex = manifest
		.lines()
		.filter_map(|line| line.split_once('='))
		.find_map(|(n, hex)| (n == name).then_some(hex))
		.ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidInput,
				"unknown legacy carrier fixture name",
			)
		})?;
	let bytes = (0..hex.len())
		.step_by(2)
		.map(|i| u8::from_str_radix(&hex[i..i + 2], 16))
		.collect::<Result<Vec<_>, _>>()
		.map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
	match version {
		0 | 4 => crate::consensus::wal_body::v4::decode_entry(&bytes),
		1 => crate::consensus::wal_body::v1::decode_entry(&bytes),
		2 => crate::consensus::wal_body::v2::decode_entry(&bytes),
		3 => crate::consensus::wal_body::v3::decode_entry(&bytes),
		_ => unreachable!(),
	}
}
#[cfg(test)]
fn assert_carrier_v4_fixture_round_trip(payload: &[u8]) {
	let frozen = postcard::from_bytes::<
		Entry<crate::consensus::wal_body::v4::WendelonWalV4TypeConfig>,
	>(payload)
	.unwrap();
	assert_eq!(postcard::to_stdvec(&frozen).unwrap(), payload);
}

#[cfg(test)]
impl ControlRingBackupImage {
	/// Force two map entry orders in the encoded body, without relying on
	/// RandomState seeds or insertion order to happen to produce a collision.
	pub(crate) fn equivalent_restore_inputs_for_test() -> (SmState, [Self; 2]) {
		let mut state = SmState::default();
		state.last_applied_log_id = Some(LogId::new(openraft::CommittedLeaderId::new(3, 1), 42));
		let first = (
			0x8899_u64,
			HybridTimestamp {
				physical_ms: 11,
				logical: 0,
				node_id: 1,
			},
		);
		let second = (
			0xaabb_u64,
			HybridTimestamp {
				physical_ms: 22,
				logical: 0,
				node_id: 1,
			},
		);
		state.e_safe_watermarks.extend([first, second]);
		let body = postcard::to_allocvec(&state).unwrap();
		let map = postcard::to_allocvec(&state.e_safe_watermarks).unwrap();
		let offsets = body
			.windows(map.len())
			.enumerate()
			.filter_map(|(offset, bytes)| (bytes == map).then_some(offset))
			.collect::<Vec<_>>();
		assert_eq!(offsets.len(), 1, "unique map byte range in fixture");
		let images = [[first, second], [second, first]].map(|entries| {
			let mut ordered_map = postcard::to_allocvec(&2usize).unwrap();
			for entry in entries {
				ordered_map.extend(postcard::to_allocvec(&entry).unwrap());
			}
			assert_eq!(ordered_map.len(), map.len());
			let mut reordered = body.clone();
			reordered[offsets[0]..offsets[0] + map.len()].copy_from_slice(&ordered_map);
			let decoded: SmState = postcard::from_bytes(&reordered).unwrap();
			assert_eq!(decoded.e_safe_watermarks, state.e_safe_watermarks);
			let header = SnapshotHeader {
				state_sha256: control_artifact_digest_32(&reordered),
				..snapshot_header_template(&state)
			};
			let mut snapshot = bytemuck::bytes_of(&header).to_vec();
			snapshot.extend(reordered);
			Self {
				manifest: ControlRingBackupManifest {
					format_version: CONTROL_RING_BACKUP_FORMAT_VERSION,
					last_applied_log_id: state.last_applied_log_id,
					voter_ids: Vec::new(),
					lease_count: 0,
					tiered_slab_count: 0,
					migration_count: 0,
					wal_bytes: 0,
					vote_bytes: 0,
					snapshot_bytes: snapshot.len() as u64,
					state_hash: control_artifact_digest_32(&snapshot),
				},
				wal: Vec::new(),
				vote: Vec::new(),
				snapshot,
			}
		});
		assert_ne!(images[0].snapshot, images[1].snapshot);
		(state, images)
	}
}
