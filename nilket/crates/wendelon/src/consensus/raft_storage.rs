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
//! [ 4-byte blake3-prefix checksum ]
//! [ zero pad to next DIRECT_IO_ALIGN boundary ]
//! ```
//!
//! The tail-pad is required because `O_DIRECT` rejects non
//! page-aligned writes; the `body_len` field on the
//! [`WALRecordHeader`] is the source of truth for recovery and
//! readers ignore the pad. Concurrency is handled by an
//! `Arc<RwLock<LogStoreInner>>` so `get_log_reader` can hand out
//! a Self-typed reader that shares the same cache as the writer.

use crate::audit::{AuditAuthority, AuditEvent, AuditSink};
use crate::consensus::{
	AdvanceEpochRecord, CreateTenant, EpochConfig, MigratedRangeTombstone, MigrationComplete,
	MigrationLedgerEntry, MigrationState, SeedMigration, TenantFinalization, TenantLifecycleState,
	TenantRecord, TieredSlab, UpdateTenantQuota, WendelonRequest, WendelonResponse,
	WendelonTypeConfig,
};
use crate::crypto::{InstallTenantKekVersion, TenantKeyset};
use crate::schema::{
	FinalizationMarker, HybridTimestamp, Locality, LogEntryKind, NodeId as SchemaNodeId,
	RaftLogEntryFixed, SlabId, SnapshotHeader, WALRecordHeader,
};
use crate::storage::io_uring::{AlignedBuf, DIRECT_IO_ALIGN, IoRingHandle};
use crate::storage::{
	AsyncFileOps, BlockingFileOps, FileHandle, IoUringFileOps, OpenFlags, fsync_parent_dir,
	write_file,
};
use bytemuck::{Pod, Zeroable};
use openraft::anyerror::AnyError;
use openraft::storage::{
	LogFlushed, LogState, RaftLogReader, RaftLogStorage, RaftSnapshotBuilder, RaftStateMachine,
	Snapshot,
};
use openraft::{
	BasicNode, CommittedLeaderId, Entry, EntryPayload, LogId, OptionalSend, SnapshotMeta,
	StorageError, StorageIOError, StoredMembership, Vote,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
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
use tokio::sync::{Mutex, RwLock, broadcast};
use tokio::task::JoinHandle;
use tokio::time::{Instant, MissedTickBehavior};

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
}

impl RaftAuditMaterializer {
	fn emit(&self, event: AuditEvent) {
		if let Err(err) = self.sink.emit(event.clone()) {
			tracing::warn!(
				event_type = %event.event_type,
				error = %err,
				"raft audit materialization failed; authoritative record remains in the committed log"
			);
		}
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
		Self::open_with_file_ops_and_options(
			wal_path,
			vote_path,
			snapshot_path,
			Arc::new(IoUringFileOps::new(io)),
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

	async fn open_with_file_ops_and_options(
		wal_path: impl AsRef<Path>,
		vote_path: impl AsRef<Path>,
		snapshot_path: impl AsRef<Path>,
		file_ops: Arc<dyn AsyncFileOps>,
		options: WalOptions,
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
		let vote = load_vote_state(file_ops.as_ref(), vote_path).await?;
		let snapshot = load_snapshot_artifact(file_ops.as_ref(), snapshot_path).await?;
		let replay = replay_wal(file_ops.as_ref(), wal_path, snapshot.as_ref()).await?;
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
				bootstrap_mode,
				file_ops: Some(file_ops),
				wal_segments: replay.wal_segments,
				wal_file,
				wal_cursor,
				wal_segment_bytes: options.segment_bytes,
				vote_file: Some(vote_file),
			})),
			wal_gate: Arc::new(Mutex::new(())),
		})
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
		let page = serialize_checkpoint_page(
			observation.checkpoint.commit_index,
			observation.checkpoint.e_safe_index,
			last_log_id.index,
			last_log_id.leader_id.term,
		);
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
		guard.recovery_checkpoint = Some(observation.checkpoint);
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
		WendelonStateMachine::from_recovery(snapshot, entries, checkpoint, snapshot_runtime)
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
				state_hash: blake3_32(&encoded_state),
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
	/// Keep this durable field at the end of the struct. Pre-W4
	/// snapshots are decoded through an explicit legacy fallback;
	/// keeping new fields trailing avoids mid-struct postcard
	/// reorder hazards in future upgrades.
	#[serde(default)]
	pub tenant_keysets: HashMap<crate::schema::TenantId, TenantKeyset>,
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
/// used here run through `spawn_blocking_io` over cloned file
/// handles rather than the submission queue.
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
	hasher: blake3::Hasher,
}

impl<W> HashingWriter<W> {
	fn new(inner: W) -> Self {
		Self {
			inner,
			hasher: blake3::Hasher::new(),
		}
	}

	fn finalize(self) -> (W, [u8; 32]) {
		(self.inner, *self.hasher.finalize().as_bytes())
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
	snapshot_runtime: SnapshotRuntime,
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
	/// Non-blocking live feed of committed finalization markers.
	finalization_events: broadcast::Sender<FinalizationMarker>,
}

impl WendelonStateMachine {
	pub fn new() -> Self {
		let (finalization_events, _) = broadcast::channel(FINALIZATION_BROADCAST_CAPACITY);
		Self {
			inner: Arc::new(RwLock::new(SmInner::default())),
			snapshot_runtime: SnapshotRuntime::ephemeral(),
			tier_observer: None,
			tenant_kek_observer: None,
			checkpoint_observer: None,
			audit_materializer: None,
			finalization_events,
		}
	}

	fn from_recovery(
		snapshot: Option<LoadedSnapshot>,
		entries: Vec<Entry<WendelonTypeConfig>>,
		checkpoint: Option<RecoveryCheckpoint>,
		snapshot_runtime: SnapshotRuntime,
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
			entries.last().map(|entry| entry.log_id.index).unwrap_or(0)
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

		Ok(Self {
			inner: Arc::new(RwLock::new(inner)),
			snapshot_runtime,
			tier_observer: None,
			tenant_kek_observer: None,
			checkpoint_observer: None,
			audit_materializer: None,
			finalization_events,
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

	pub fn with_audit_materializer(
		mut self,
		local_node: SchemaNodeId,
		sink: Arc<dyn AuditSink>,
	) -> Self {
		self.audit_materializer = Some(RaftAuditMaterializer { local_node, sink });
		self
	}

	pub fn subscribe_finalizations(&self) -> broadcast::Receiver<FinalizationMarker> {
		self.finalization_events.subscribe()
	}

	/// Test / caller inspection hook — return a cloned snapshot
	/// of the applied state without driving a full snapshot
	/// build round-trip.
	pub async fn state_snapshot(&self) -> SmState {
		self.inner.read().await.state.clone()
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
		let mut pending_tier_events: Vec<(TieredSlab, u64)> = Vec::new();
		let mut pending_tenant_kek_events: Vec<(InstallTenantKekVersion, u64)> = Vec::new();
		let mut pending_tenant_kek_snapshot: Option<
			HashMap<crate::schema::TenantId, TenantKeyset>,
		> = None;
		let mut pending_finalizations: Vec<FinalizationMarker> = Vec::new();
		let pending_checkpoint = {
			let mut guard = self.inner.write().await;
			for entry in entries {
				let tenant_kek_rotation = match &entry.payload {
					EntryPayload::Normal(WendelonRequest::InstallTenantKekVersion(rotation)) => {
						Some(rotation.clone())
					}
					_ => None,
				};
				let is_create_tenant = matches!(
					&entry.payload,
					EntryPayload::Normal(WendelonRequest::CreateTenant(_))
				);
				let (resp, tier) = apply_entry_to_state(&mut guard.state, &entry);
				let applied_successfully = matches!(&resp, WendelonResponse::Success);
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
						pending_tenant_kek_snapshot = Some(guard.state.tenant_keysets.clone());
					}
				}
				// CreateTenant installs the initial tenant keyset directly in
				// the state machine without a separate InstallTenantKekVersion
				// entry. The local CryptoManager still needs to learn about
				// the new keys, so reconcile from the committed snapshot.
				if is_create_tenant && applied_successfully {
					pending_tenant_kek_snapshot = Some(guard.state.tenant_keysets.clone());
				}
				if let Some(materializer) = self.audit_materializer.as_ref() {
					if let Some(event) = raft_audit_event(materializer.local_node, &entry, &resp) {
						pending_audit_events.push(event);
					}
				}
				if let Some(marker) = canonical_finalization_marker(&entry) {
					pending_finalizations.push(marker);
				}
				out.push(resp);
			}
			checkpoint_observation_from_state(&guard.state, false)
		};
		if let Some(materializer) = self.audit_materializer.as_ref() {
			for event in pending_audit_events {
				materializer.emit(event);
			}
		}
		if let Some(observer) = self.tier_observer.as_ref() {
			for (tier, applied_log_index) in &pending_tier_events {
				// An observer error is surfaced through logs but
				// must not tear the state machine down — the SM
				// has already committed the pivot and followers
				// must not diverge on a local filesystem hiccup.
				if let Err(e) = observer.on_slab_tiered(tier, *applied_log_index).await {
					tracing::warn!(
						slab_id = tier.slab_id.get(),
						error = %e,
						"tier observer failed; local L1 cleanup will retry on next sweep"
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
		}
	}

	async fn begin_receiving_snapshot(
		&mut self,
	) -> Result<Box<SnapshotDataFile>, StorageError<u32>> {
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
		let header_last = log_id_from_snapshot_header(&header);
		if header_last != meta.last_log_id {
			return Err(StorageIOError::read_snapshot(
				Some(meta.signature()),
				AnyError::error(format!(
					"snapshot header {:?} disagrees with install meta {:?}",
					header_last, meta.last_log_id
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
			guard.state = state;
			guard.snapshot = Some(HeldSnapshot {
				meta: meta.clone(),
				path: self.snapshot_runtime.paths.current.clone(),
			});
			checkpoint_observation_from_state(&guard.state, true)
		};
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

/// Fold one committed [`WendelonRequest`] into the SM state.
///
/// Returns the [`WendelonResponse`] plus an optional `TieredSlab`
/// so the caller can fire tier observers outside the write lock.
/// Tier observers do filesystem work (unlinking the L1 file);
/// keeping them off the lock prevents any cross-node staleness
/// on a busy apply batch.
fn apply_entry_to_state(
	state: &mut SmState,
	entry: &Entry<WendelonTypeConfig>,
) -> (WendelonResponse, Option<TieredSlab>) {
	match &entry.payload {
		EntryPayload::Blank => (WendelonResponse::Success, None),
		EntryPayload::Normal(req) => apply_request(state, req.clone()),
		EntryPayload::Membership(membership) => {
			state.last_membership = StoredMembership::new(Some(entry.log_id), membership.clone());
			(WendelonResponse::Success, None)
		}
	}
}

fn raft_audit_event(
	local_node: SchemaNodeId,
	entry: &Entry<WendelonTypeConfig>,
	resp: &WendelonResponse,
) -> Option<AuditEvent> {
	let EntryPayload::Normal(req) = &entry.payload else {
		return None;
	};
	let is_err = matches!(resp, WendelonResponse::Error(_));
	let outcome = audit_outcome_field(resp);
	match req {
		// Non-tenant-admin mutations emit on success only; the apply-layer
		// can reject for transient state-machine reasons and an error
		// record per replay would flood the audit log.
		WendelonRequest::Finalize(_) if is_err => None,
		WendelonRequest::AdvanceEpoch(_) | WendelonRequest::InstallTenantKekVersion(_)
			if is_err =>
		{
			None
		}
		WendelonRequest::Finalize(finalization) => Some(
			AuditEvent::new(
				AuditAuthority::RaftLog,
				"data.write_finalized",
				local_node,
				format!(
					"raft_log_index={} raft_term={} tenant_id={} locality={} durability={} charged_bytes={} commit_hlc={:?} payload_hash={}",
					entry.log_id.index,
					entry.log_id.leader_id.term,
					finalization.tenant_id.get(),
					Locality::from(finalization.key_identity).get(),
					finalization.marker.durability.0,
					finalization.charged_bytes,
					finalization.marker.commit_hlc,
					format_bytes_hex(&finalization.marker.payload_hash),
				),
			)
			.with_tenant(finalization.tenant_id)
			.with_record_identity(finalization.key_identity),
		),
		WendelonRequest::AdvanceEpoch(record) => Some(AuditEvent::new(
			AuditAuthority::RaftLog,
			"control_ring.epoch_advanced",
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
		)),
		WendelonRequest::InstallTenantKekVersion(rotation) => Some(AuditEvent::new(
			AuditAuthority::RaftLog,
			"crypto.tenant_kek_rotated",
			local_node,
			format!(
				"raft_log_index={} raft_term={} tenant_id={} tenant_kek_id={}",
				entry.log_id.index,
				entry.log_id.leader_id.term,
				rotation.tenant_id.get(),
				rotation.tenant_kek_id,
			),
		)),
		// Tenant admin ops are attempt-audited per ADR 021 §5: every
		// mutation — including rejected ones — emits a record so the
		// audit stream can be reconciled against the tenant FSM.
		WendelonRequest::CreateTenant(create) => Some(AuditEvent::new(
			AuditAuthority::RaftLog,
			"tenant.created",
			local_node,
			format!(
				"raft_log_index={} raft_term={} tenant_id={} quota_bytes={} replication_factor={} tenant_kek_id={} {outcome}",
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
			),
		)),
		WendelonRequest::UpdateTenantQuota(update) => Some(AuditEvent::new(
			AuditAuthority::RaftLog,
			"tenant.quota_updated",
			local_node,
			format!(
				"raft_log_index={} raft_term={} tenant_id={} quota_bytes={} {outcome}",
				entry.log_id.index,
				entry.log_id.leader_id.term,
				update.tenant_id.get(),
				update.quota_bytes,
			),
		)),
		WendelonRequest::SuspendTenant(tenant_id) => Some(AuditEvent::new(
			AuditAuthority::RaftLog,
			"tenant.suspended",
			local_node,
			format!(
				"raft_log_index={} raft_term={} tenant_id={} {outcome}",
				entry.log_id.index,
				entry.log_id.leader_id.term,
				tenant_id.get(),
			),
		)),
		WendelonRequest::ResumeTenant(tenant_id) => Some(AuditEvent::new(
			AuditAuthority::RaftLog,
			"tenant.resumed",
			local_node,
			format!(
				"raft_log_index={} raft_term={} tenant_id={} {outcome}",
				entry.log_id.index,
				entry.log_id.leader_id.term,
				tenant_id.get(),
			),
		)),
		WendelonRequest::DeleteTenant(tenant_id) => Some(AuditEvent::new(
			AuditAuthority::RaftLog,
			"tenant.delete_requested",
			local_node,
			format!(
				"raft_log_index={} raft_term={} tenant_id={} {outcome}",
				entry.log_id.index,
				entry.log_id.leader_id.term,
				tenant_id.get(),
			),
		)),
		_ => None,
	}
}

fn audit_outcome_field(resp: &WendelonResponse) -> String {
	match resp {
		WendelonResponse::Error(message) => {
			format!("outcome=error reason={:?}", message)
		}
		_ => "outcome=success".to_string(),
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

fn apply_request(
	state: &mut SmState,
	req: WendelonRequest,
) -> (WendelonResponse, Option<TieredSlab>) {
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
		WendelonRequest::SchemaUpdate(_bytes) => {
			// Schema registry is not modelled in SmState yet;
			// accepting the write keeps the log replayable and
			// lets the control-ring feature land incrementally.
			(WendelonResponse::Success, None)
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
				return (WendelonResponse::Success, Some(tier));
			}
			state.tiered_slabs.insert(tier.slab_id, tier.clone());
			(WendelonResponse::Success, Some(tier))
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
		WendelonRequest::SeedMigration(seed) => {
			apply_seed_migration(state, seed);
			(WendelonResponse::Success, None)
		}
		WendelonRequest::MigrationComplete(done) => {
			apply_migration_complete(state, done);
			(WendelonResponse::Success, None)
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
fn apply_migration_complete(state: &mut SmState, done: MigrationComplete) {
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
		return;
	}

	{
		let entry = state
			.migrations
			.get_mut(&done.migration_id)
			.expect("matched above");
		if entry.state == MigrationState::Completed {
			return; // idempotent replay
		}
		entry.state = MigrationState::Completed;
		entry.completed_by = Some(done.completed_by);
		entry.completed_at_hlc = Some(done.completed_at_hlc);
	}

	state.tombstones.push(MigratedRangeTombstone {
		migration_id: done.migration_id,
		range_start: done.range_start,
		range_end: done.range_end,
		to_epoch: done.to_epoch,
		completed_by: done.completed_by,
		migration_hlc_floor: done.migration_hlc_floor,
	});

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
}

impl Default for WendelonSnapshotBuilder {
	fn default() -> Self {
		Self {
			inner: Arc::new(RwLock::new(SmInner::default())),
			snapshot_runtime: SnapshotRuntime::ephemeral(),
			checkpoint_observer: None,
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
	let header_last = log_id_from_snapshot_header(&header);
	if state.last_applied_log_id != header_last {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"snapshot artifact {} last_applied {:?} disagrees with header {:?}",
				snapshot_path.display(),
				state.last_applied_log_id,
				header_last
			),
		));
	}
	let snapshot_id = format!("recovered-{}", header.last_included_index);
	Ok(Some(LoadedSnapshot {
		last_included: header_last,
		held: HeldSnapshot {
			meta: SnapshotMeta {
				last_log_id: header_last,
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
		let expected_checksum = blake3_prefix_u32(&bytes[offset..body_end]);
		let actual_checksum = u32::from_le_bytes(bytes[body_end..record_end].try_into().unwrap());
		if expected_checksum != actual_checksum {
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
				if let Some(boundary) = boundary {
					if checkpoint.commit_index < boundary {
						return Err(interior_corruption(
							&segment.path,
							offset,
							format!(
								"checkpoint commit_index {} is behind snapshot boundary {}",
								checkpoint.commit_index, boundary
							),
						));
					}
				}
				replay.checkpoint = Some(RecoveryCheckpoint {
					commit_index: checkpoint.commit_index,
					e_safe_index: checkpoint.e_safe_index,
				});
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
	let payload_hash = blake3_32(payload);
	if payload_hash != fixed.payload_hash {
		return Err(interior_corruption(
			wal_path,
			offset,
			"log record payload hash mismatch".to_string(),
		));
	}
	let entry: Entry<WendelonTypeConfig> = postcard::from_bytes(payload).map_err(|err| {
		interior_corruption(
			wal_path,
			offset,
			format!("log entry postcard decode failed: {err}"),
		)
	})?;
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

fn log_id_from_snapshot_header(header: &SnapshotHeader) -> Option<LogId<u32>> {
	if header.last_included_index == 0 && header.last_included_term == 0 {
		None
	} else {
		Some(LogId::new(
			CommittedLeaderId::new(header.last_included_term, 0),
			header.last_included_index,
		))
	}
}

/// Build one page-padded WAL record for a single entry:
/// `[WALRecordHeader][RaftLogEntryFixed][postcard(entry)][blake3-prefix checksum]`
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
		EntryPayload::Normal(WendelonRequest::TierSlab(_)) => (LogEntryKind::APPEND, 0),
		EntryPayload::Normal(WendelonRequest::CreateTenant(_)) => (LogEntryKind::APPEND, 0),
		EntryPayload::Normal(WendelonRequest::UpdateTenantQuota(_)) => (LogEntryKind::APPEND, 0),
		EntryPayload::Normal(WendelonRequest::SuspendTenant(_)) => (LogEntryKind::APPEND, 0),
		EntryPayload::Normal(WendelonRequest::ResumeTenant(_)) => (LogEntryKind::APPEND, 0),
		EntryPayload::Normal(WendelonRequest::DeleteTenant(_)) => (LogEntryKind::APPEND, 0),
		EntryPayload::Normal(WendelonRequest::InstallTenantKekVersion(_)) => {
			(LogEntryKind::APPEND, 0)
		}
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
		_pad1: [0; 7],
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
	fixed.payload_hash = blake3_32(payload);
	dst[..header_size].copy_from_slice(bytemuck::bytes_of(&header));
	dst[header_size..header_size + fixed_size].copy_from_slice(bytemuck::bytes_of(&fixed));
	let checksum = blake3_prefix_u32(&dst[..header_size + body_len]);
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
		_pad1: [0; 7],
		body_len: body_size as u64,
		log_index,
		term,
	};
	let mut buf = AlignedBuf::new(padded);
	let dst = buf.as_mut_slice();
	dst[..header_size].copy_from_slice(bytemuck::bytes_of(&header));
	dst[header_size..header_size + body_size].copy_from_slice(bytemuck::bytes_of(&body));
	let checksum = blake3_prefix_u32(&dst[..header_size + body_size]);
	dst[header_size + body_size..header_size + body_size + 4]
		.copy_from_slice(&checksum.to_le_bytes());
	buf
}

/// Encode a complete snapshot: `[SnapshotHeader][postcard(SmState)]`.
fn encode_snapshot(state: &SmState) -> Result<Vec<u8>, AnyError> {
	let body = postcard::to_allocvec(state).map_err(|e| AnyError::new(&e))?;
	let header = snapshot_header_template(state);
	let header = SnapshotHeader {
		state_sha256: blake3_32(&body),
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
		_pad: [0; 4],
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
	let actual_hash = blake3_32(&body);
	if actual_hash != header.state_sha256 {
		return Err(format!(
			"snapshot body hash mismatch: expected {:02x?}, got {:02x?}",
			header.state_sha256, actual_hash
		));
	}
	let state = decode_snapshot_state_bytes(&body)?;
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

fn blake3_32(bytes: &[u8]) -> [u8; 32] {
	*blake3::hash(bytes).as_bytes()
}

fn blake3_prefix_u32(bytes: &[u8]) -> u32 {
	let hash = blake3::hash(bytes);
	let prefix: [u8; 4] = hash.as_bytes()[..4]
		.try_into()
		.expect("blake3 output is 32 bytes");
	u32::from_le_bytes(prefix)
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

// ============================================================
//  RECOVERY TESTS
// ============================================================

#[cfg(test)]
mod recovery_tests {
	use super::*;
	use crate::schema::{FinalizationMarker, LeaseDescriptor, NodeId as SchemaNodeId, TenantId};
	use crate::storage::{
		AlignedBuf, AsyncFileOps, BlockingFileOps, DirectoryEntry, FaultAction, FaultOp,
		FaultyFileOps, FileHandle, InjectedFault, OpenFlags,
	};
	use openraft::Vote;
	use openraft::storage::{RaftLogStorageExt, RaftSnapshotBuilder, RaftStateMachine};
	use std::collections::HashMap;
	use std::fs::OpenOptions;
	use std::io::Write;
	use std::path::PathBuf;
	use std::sync::{Arc, Mutex};
	use std::time::Duration;
	use tokio::sync::Notify;

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

	fn log_id(term: u64, index: u64) -> LogId<u32> {
		LogId::new(CommittedLeaderId::new(term, 0), index)
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
		Entry {
			log_id: log_id(1, index),
			payload: EntryPayload::Normal(WendelonRequest::Finalize(
				crate::consensus::TenantFinalization {
					marker: FinalizationMarker {
						log_index: index,
						term: 1,
						epoch: 1,
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

	async fn open_store(dir: &Path) -> WendelonLogStore {
		let io = crate::storage::io_uring::spawn_io_thread(64).unwrap();
		let (wal, vote, snapshot) = artifact_paths(dir);
		WendelonLogStore::open(&wal, &vote, &snapshot, io)
			.await
			.unwrap()
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
		write_wal_pages(&wal, &[serialize_entry_page(&entry).unwrap()]);

		let mut store = open_store(&dir).await;
		assert_eq!(store.bootstrap_mode().await, DurableBootstrapMode::Recovery);
		assert_eq!(
			store.read_vote().await.unwrap(),
			Some(Vote::new_committed(3, 7))
		);

		let state = store.get_log_state().await.unwrap();
		assert_eq!(state.last_purged_log_id, None);
		assert_eq!(state.last_log_id, Some(log_id(1, 1)));

		let sm = store.recovered_state_machine().await.unwrap();
		let snapshot = sm.state_snapshot().await;
		assert_eq!(snapshot.last_applied_log_id, Some(log_id(1, 1)));
		assert!(snapshot.leases.contains_key(&0));
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

		let sm = store.recovered_state_machine().await.unwrap();
		let snapshot = sm.state_snapshot().await;
		assert_eq!(snapshot.last_applied_log_id, Some(log_id(1, 1)));
		assert_eq!(snapshot.leases.len(), 1);
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
			"expected blake3-prefix checksum verification failure, got: {err}"
		);
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

		let reopened = open_store(&target_dir).await;
		let reopened_sm = reopened.recovered_state_machine().await.unwrap();
		let state = reopened_sm.state_snapshot().await;
		assert_eq!(state.last_applied_log_id, Some(log_id(1, 1)));
		assert!(state.leases.contains_key(&0));
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
			.encrypt_tiered_slab(SlabId::new(42), TenantId::ZERO, b"before-snapshot")
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
		let mut store = open_store_with_segment_bytes(&dir, DIRECT_IO_ALIGN as u64).await;
		store
			.blocking_append(vec![
				lease_entry(1, 0, 9, 1),
				lease_entry(2, 10, 19, 2),
				lease_entry(3, 20, 29, 3),
			])
			.await
			.unwrap();

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
mod tenant_kek_tests {
	use super::*;
	use crate::crypto::{TenantKeyset, WrappedTenantKek};
	use crate::schema::{TenantId, TenantKekId};

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
}
