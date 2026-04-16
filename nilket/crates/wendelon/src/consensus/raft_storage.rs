//! openraft storage-v2 traits for Wendelon's Control Ring.
//!
//! Three concrete types live here:
//!
//!  * [`WendelonLogStore`] — durable Raft log + vote persistence.
//!    Log entries are held in an in-memory `BTreeMap<u64, Entry<…>>`
//!    for fast serve and are simultaneously streamed to an
//!    `O_DIRECT`-opened WAL file via the shared
//!    [`IoRingHandle`][crate::storage::io_uring::IoRingHandle]. A
//!    `submit_fsync` is awaited *before* the openraft `LogFlushed`
//!    callback fires, so the log-io promise and the on-disk tail
//!    cannot disagree. The vote rides a separate single-page file
//!    (`vote.wnd`) so it can be rewritten in place without
//!    disturbing the append-only WAL.
//!
//!  * [`WendelonStateMachine`] — applies committed entries into an
//!    `Arc<RwLock<SmState>>`. The SM tracks
//!    `last_applied_log_id`, `last_membership`, per-holder lease
//!    table, and a per-epoch E_safe watermark map. The `Arc` is
//!    shared into [`WendelonSnapshotBuilder`] so snapshots are
//!    cheap consistent views rather than a deep clone.
//!
//!  * [`WendelonSnapshotBuilder`] — postcard-serialises
//!    [`SmState`] into a `Cursor<Vec<u8>>` prefixed with
//!    [`SnapshotHeader`], matching the plan in
//!    `plans/consensus/raft-core.md` §3.
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

use crate::consensus::{
	AdvanceEpochRecord, EpochConfig, MigratedRangeTombstone, MigrationComplete,
	MigrationLedgerEntry, MigrationState, SeedMigration, TieredSlab, WendelonRequest,
	WendelonResponse, WendelonTypeConfig,
};
use crate::schema::{
	HybridTimestamp, LogEntryKind, NodeId as SchemaNodeId, RaftLogEntryFixed, SnapshotHeader,
	WALRecordHeader,
};
use crate::storage::io_uring::{AlignedBuf, DIRECT_IO_ALIGN, IoOp, IoRingHandle};
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
use std::fs::{File, OpenOptions};
use std::io::{Cursor, Error, ErrorKind};
use std::ops::{Bound, RangeBounds};
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::io::{AsRawFd, RawFd};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

/// Linux `O_DIRECT`. Repeated here rather than pulling `libc` in
/// for a single constant — same value / same justification as in
/// [`crate::storage::core`].
const O_DIRECT: i32 = 0o0040000;

/// WAL `record_type` discriminants — mirrored from
/// `plans/consensus/raft-core.md` §4.
const REC_LOG_ENTRY: u8 = 0;
#[allow(dead_code)]
const REC_SNAPSHOT_MARKER: u8 = 1;
#[allow(dead_code)]
const REC_SNAPSHOT_INSTALL: u8 = 2;
#[allow(dead_code)]
const REC_CHECKPOINT: u8 = 3;

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

#[derive(Default)]
struct WALReplay {
	entries: BTreeMap<u64, Entry<WendelonTypeConfig>>,
	last_log_id: Option<LogId<u32>>,
	last_purged: Option<LogId<u32>>,
	checkpoint: Option<RecoveryCheckpoint>,
	wal_cursor: u64,
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
	/// Cached vote. Written to `vote_fd` on `save_vote`; surfaced
	/// verbatim by `read_vote`.
	vote: Option<Vote<u32>>,
	/// Snapshot loaded during durable recovery, if present.
	recovered_snapshot: Option<HeldSnapshot>,
	/// Recovered checkpoint counters from the WAL.
	recovery_checkpoint: Option<RecoveryCheckpoint>,
	/// Whether startup was a true fresh bootstrap or durable recovery.
	bootstrap_mode: DurableBootstrapMode,

	/// io_uring handle for WAL and vote persistence. `None` in
	/// the in-memory test mode — append skips the disk path.
	io: Option<IoRingHandle>,
	/// Owning WAL file handle (keeps `wal_fd` valid).
	_wal_file: Option<File>,
	wal_fd: Option<RawFd>,
	/// Next byte offset in the WAL. Page-aligned at all times.
	wal_cursor: u64,
	/// Owning vote file handle.
	_vote_file: Option<File>,
	vote_fd: Option<RawFd>,
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
				recovered_snapshot: None,
				recovery_checkpoint: None,
				bootstrap_mode: DurableBootstrapMode::FreshBootstrap,
				io: None,
				_wal_file: None,
				wal_fd: None,
				wal_cursor: 0,
				_vote_file: None,
				vote_fd: None,
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
	/// handles to the supplied `io_uring` submission handle.
	pub fn open(
		wal_path: impl AsRef<Path>,
		vote_path: impl AsRef<Path>,
		snapshot_path: impl AsRef<Path>,
		io: IoRingHandle,
	) -> std::io::Result<Self> {
		let wal_path = wal_path.as_ref();
		let vote_path = vote_path.as_ref();
		let snapshot_path = snapshot_path.as_ref();

		let artifacts = inspect_artifacts(wal_path, vote_path, snapshot_path)?;
		let bootstrap_mode = DurableBootstrapMode::from_artifacts(artifacts.any());
		let vote = load_vote_state(vote_path)?;
		let snapshot = load_snapshot_artifact(snapshot_path)?;
		let replay = replay_wal(wal_path, snapshot.as_ref())?;

		let wal_file = OpenOptions::new()
			.read(true)
			.write(true)
			.create(true)
			.truncate(false)
			.custom_flags(O_DIRECT)
			.open(wal_path)?;
		let wal_fd = wal_file.as_raw_fd();

		let vote_file = OpenOptions::new()
			.read(true)
			.write(true)
			.create(true)
			.truncate(false)
			.custom_flags(O_DIRECT)
			.open(vote_path)?;
		let vote_fd = vote_file.as_raw_fd();

		Ok(Self {
			inner: Arc::new(RwLock::new(LogStoreInner {
				entries: replay.entries,
				last_log_id: replay.last_log_id,
				last_purged: replay.last_purged,
				vote,
				recovered_snapshot: snapshot.map(|s| s.held),
				recovery_checkpoint: replay.checkpoint,
				bootstrap_mode,
				io: Some(io),
				_wal_file: Some(wal_file),
				wal_fd: Some(wal_fd),
				wal_cursor: replay.wal_cursor,
				_vote_file: Some(vote_file),
				vote_fd: Some(vote_fd),
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

	pub async fn recovered_state_machine(&self) -> std::io::Result<WendelonStateMachine> {
		let (snapshot, entries, checkpoint) = {
			let guard = self.inner.read().await;
			(
				guard.recovered_snapshot.clone(),
				guard.entries.values().map(clone_entry).collect::<Vec<_>>(),
				guard.recovery_checkpoint,
			)
		};
		WendelonStateMachine::from_recovery(snapshot, entries, checkpoint)
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
		let (io, fd) = {
			let mut guard = self.inner.write().await;
			guard.vote = Some(*vote);
			(guard.io.clone(), guard.vote_fd)
		};

		let (Some(io), Some(fd)) = (io, fd) else {
			// In-memory mode — caller accepted non-durable vote.
			return Ok(());
		};

		let mut buf = AlignedBuf::new(DIRECT_IO_ALIGN);
		let encoded = postcard::to_allocvec(vote).map_err(|e| {
			StorageError::<u32>::from(StorageIOError::write_vote(AnyError::new(&e)))
		})?;
		if encoded.len() + 4 > DIRECT_IO_ALIGN {
			return Err(StorageIOError::write_vote(AnyError::error(
				"encoded vote exceeds single O_DIRECT page",
			))
			.into());
		}
		buf.as_mut_slice()[..4].copy_from_slice(&(encoded.len() as u32).to_le_bytes());
		buf.as_mut_slice()[4..4 + encoded.len()].copy_from_slice(&encoded);

		let reply = io
			.submit(IoOp::Write, fd, buf, 0)
			.await
			.map_err(|e| StorageIOError::write_vote(AnyError::new(&e)))?;
		let completion = reply.await.map_err(|_| {
			StorageIOError::write_vote(AnyError::error("vote reply channel closed"))
		})?;
		if completion.result < 0 {
			return Err(StorageIOError::write_vote(io_errno(completion.result)).into());
		}

		let reply = io
			.submit_fsync(fd)
			.await
			.map_err(|e| StorageIOError::write_vote(AnyError::new(&e)))?;
		let completion = reply.await.map_err(|_| {
			StorageIOError::write_vote(AnyError::error("vote fsync reply channel closed"))
		})?;
		if completion.result < 0 {
			return Err(StorageIOError::write_vote(io_errno(completion.result)).into());
		}
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
		let (io, fd, offsets) = {
			let mut guard = self.inner.write().await;
			let io = guard.io.clone();
			let fd = guard.wal_fd;
			let mut offsets = Vec::with_capacity(pages.len());
			for (entry, page) in batch.iter().zip(&pages) {
				offsets.push(guard.wal_cursor);
				guard.wal_cursor += page.len() as u64;
				let id = entry.log_id;
				guard.last_log_id = Some(match guard.last_log_id {
					Some(prev) if prev > id => prev,
					_ => id,
				});
				guard.entries.insert(id.index, clone_entry(entry));
			}
			(io, fd, offsets)
		};

		let (Some(io), Some(fd)) = (io, fd) else {
			callback.log_io_completed(Ok(()));
			return Ok(());
		};

		let mut replies = Vec::with_capacity(pages.len());
		for (page, offset) in pages.into_iter().zip(offsets) {
			let reply = match io.submit(IoOp::Write, fd, page, offset).await {
				Ok(r) => r,
				Err(e) => {
					callback
						.log_io_completed(Err(std::io::Error::other(format!("wal submit: {e}"))));
					return Err(StorageIOError::write_logs(AnyError::new(&e)).into());
				}
			};
			replies.push(reply);
		}
		for reply in replies {
			let completion = match reply.await {
				Ok(c) => c,
				Err(_) => {
					let msg = "wal write reply channel closed";
					callback.log_io_completed(Err(std::io::Error::new(
						std::io::ErrorKind::BrokenPipe,
						msg,
					)));
					return Err(StorageIOError::write_logs(AnyError::error(msg)).into());
				}
			};
			if completion.result < 0 {
				callback
					.log_io_completed(Err(std::io::Error::from_raw_os_error(-completion.result)));
				return Err(StorageIOError::write_logs(io_errno(completion.result)).into());
			}
		}

		// Durability barrier. All write CQEs above have already
		// been awaited, so this fsync meaningfully covers them
		// despite `io_uring`'s unordered fsync-vs-write semantics.
		let reply = match io.submit_fsync(fd).await {
			Ok(r) => r,
			Err(e) => {
				callback
					.log_io_completed(Err(std::io::Error::other(format!("wal fsync submit: {e}"))));
				return Err(StorageIOError::write_logs(AnyError::new(&e)).into());
			}
		};
		let completion = match reply.await {
			Ok(c) => c,
			Err(_) => {
				let msg = "wal fsync reply channel closed";
				callback.log_io_completed(Err(std::io::Error::new(
					std::io::ErrorKind::BrokenPipe,
					msg,
				)));
				return Err(StorageIOError::write_logs(AnyError::error(msg)).into());
			}
		};
		if completion.result < 0 {
			callback.log_io_completed(Err(std::io::Error::from_raw_os_error(-completion.result)));
			return Err(StorageIOError::write_logs(io_errno(completion.result)).into());
		}

		callback.log_io_completed(Ok(()));
		Ok(())
	}

	async fn truncate(&mut self, log_id: LogId<u32>) -> Result<(), StorageError<u32>> {
		// Drop cached entries with index >= log_id.index. WAL
		// rewinds / torn-suffix compaction are a follow-up; for
		// correctness the cache-level truncate is what openraft
		// observes.
		let mut guard = self.inner.write().await;
		guard.entries.retain(|&idx, _| idx < log_id.index);
		if let Some(prev) = guard.last_log_id {
			if prev >= log_id {
				guard.last_log_id = guard
					.entries
					.values()
					.last()
					.map(|e| e.log_id)
					.or(guard.last_purged);
			}
		}
		Ok(())
	}

	async fn purge(&mut self, log_id: LogId<u32>) -> Result<(), StorageError<u32>> {
		let mut guard = self.inner.write().await;
		guard.entries.retain(|&idx, _| idx > log_id.index);
		guard.last_purged = Some(match guard.last_purged {
			Some(prev) if prev > log_id => prev,
			_ => log_id,
		});
		if guard.last_log_id.map(|id| id < log_id).unwrap_or(true) {
			guard.last_log_id = guard.last_purged;
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
	/// Durable control-plane epoch history. The latest committed
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
	/// Slabs that have been tiered to L2 by the sweeper (ADR 006).
	/// Keyed by `slab_id`; every replica must agree on the mapping
	/// so read-path pivots are deterministic. Observers — the
	/// local `SlabManager` — are fired out-of-band when an entry
	/// lands here so they can unlink the L1 file.
	#[serde(default)]
	pub tiered_slabs: HashMap<u64, TieredSlab>,
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
}

/// Out-of-band observer for [`TieredSlab`] apply events.
///
/// Wendelon's `SlabManager` implements this so every node — not
/// just the sweeper that initiated the Raft commit — learns when
/// a slab has moved to L2. The observer runs inside the state
/// machine's apply loop and must therefore be non-blocking.
#[async_trait::async_trait]
pub trait TierObserver: Send + Sync {
	async fn on_slab_tiered(&self, tier: &TieredSlab) -> std::io::Result<()>;
}

/// Snapshot currently held by the state machine.
#[derive(Clone, Debug)]
struct HeldSnapshot {
	meta: SnapshotMeta<u32, BasicNode>,
	data: Vec<u8>,
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
#[derive(Clone, Default)]
pub struct WendelonStateMachine {
	inner: Arc<RwLock<SmInner>>,
	/// Optional callback fired every time a [`TieredSlab`] lands
	/// in the state. The local `SlabManager` installs one so it
	/// can unlink the L1 file once the cluster has agreed the
	/// payload lives in L2. `None` is fine for tests or for
	/// non-data nodes that never hold a `SlabManager`.
	tier_observer: Option<Arc<dyn TierObserver>>,
}

impl WendelonStateMachine {
	pub fn new() -> Self {
		Self {
			inner: Arc::new(RwLock::new(SmInner::default())),
			tier_observer: None,
		}
	}

	fn from_recovery(
		snapshot: Option<HeldSnapshot>,
		entries: Vec<Entry<WendelonTypeConfig>>,
		checkpoint: Option<RecoveryCheckpoint>,
	) -> std::io::Result<Self> {
		let mut inner = SmInner::default();
		let snapshot_boundary = if let Some(snapshot) = snapshot.clone() {
			let (_, state) = decode_snapshot(&snapshot.data).map_err(|e| {
				Error::new(
					ErrorKind::InvalidData,
					format!("recovery snapshot decode failed: {e}"),
				)
			})?;
			inner.state = state;
			inner.state.last_applied_log_id = snapshot.meta.last_log_id;
			inner.state.last_membership = snapshot.meta.last_membership.clone();
			inner.snapshot = Some(snapshot.clone());
			inner.snapshot_seq = 1;
			snapshot.meta.last_log_id.map(|id| id.index)
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
			tier_observer: None,
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

	/// Test / caller inspection hook — return a cloned snapshot
	/// of the applied state without driving a full snapshot
	/// build round-trip.
	pub async fn state_snapshot(&self) -> SmState {
		self.inner.read().await.state.clone()
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
		let mut pending_tier_events: Vec<TieredSlab> = Vec::new();
		{
			let mut guard = self.inner.write().await;
			for entry in entries {
				let (resp, tier) = apply_entry_to_state(&mut guard.state, &entry);
				guard.state.last_applied_log_id = Some(match guard.state.last_applied_log_id {
					Some(prev) if prev > entry.log_id => prev,
					_ => entry.log_id,
				});
				if let Some(t) = tier {
					pending_tier_events.push(t);
				}
				out.push(resp);
			}
		}
		if let Some(observer) = self.tier_observer.as_ref() {
			for tier in &pending_tier_events {
				// An observer error is surfaced through logs but
				// must not tear the state machine down — the SM
				// has already committed the pivot and followers
				// must not diverge on a local filesystem hiccup.
				if let Err(e) = observer.on_slab_tiered(tier).await {
					tracing::warn!(
						slab_id = tier.slab_id,
						error = %e,
						"tier observer failed; local L1 cleanup will retry on next sweep"
					);
				}
			}
		}
		Ok(out)
	}

	async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
		WendelonSnapshotBuilder {
			inner: self.inner.clone(),
		}
	}

	async fn begin_receiving_snapshot(
		&mut self,
	) -> Result<Box<Cursor<Vec<u8>>>, StorageError<u32>> {
		Ok(Box::new(Cursor::new(Vec::new())))
	}

	async fn install_snapshot(
		&mut self,
		meta: &SnapshotMeta<u32, BasicNode>,
		snapshot: Box<Cursor<Vec<u8>>>,
	) -> Result<(), StorageError<u32>> {
		let bytes = snapshot.into_inner();
		let (_, state) = decode_snapshot(&bytes).map_err(|e| {
			StorageIOError::read_snapshot(Some(meta.signature()), AnyError::error(e))
		})?;
		let mut guard = self.inner.write().await;
		guard.state = state;
		guard.state.last_applied_log_id = meta.last_log_id;
		guard.state.last_membership = meta.last_membership.clone();
		guard.snapshot = Some(HeldSnapshot {
			meta: meta.clone(),
			data: bytes,
		});
		Ok(())
	}

	async fn get_current_snapshot(
		&mut self,
	) -> Result<Option<Snapshot<WendelonTypeConfig>>, StorageError<u32>> {
		let guard = self.inner.read().await;
		Ok(guard.snapshot.as_ref().map(|s| Snapshot {
			meta: s.meta.clone(),
			snapshot: Box::new(Cursor::new(s.data.clone())),
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

fn apply_request(
	state: &mut SmState,
	req: WendelonRequest,
) -> (WendelonResponse, Option<TieredSlab>) {
	match req {
		WendelonRequest::Finalize(marker) => {
			// E_safe advances monotonically within an epoch —
			// keep the later HLC.
			state
				.e_safe_watermarks
				.entry(marker.epoch)
				.and_modify(|hlc| {
					if marker.commit_hlc > *hlc {
						*hlc = marker.commit_hlc;
					}
				})
				.or_insert(marker.commit_hlc);
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
			// Idempotent: a replay must not fire the observer
			// twice. The first insert wins; later apply passes
			// return Success without a pending event.
			if state.tiered_slabs.contains_key(&tier.slab_id) {
				return (WendelonResponse::Success, None);
			}
			state.tiered_slabs.insert(tier.slab_id, tier.clone());
			(WendelonResponse::Success, Some(tier))
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
}

impl Default for WendelonSnapshotBuilder {
	fn default() -> Self {
		Self {
			inner: Arc::new(RwLock::new(SmInner::default())),
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
		let (bytes, meta) = {
			let mut guard = self.inner.write().await;
			guard.snapshot_seq += 1;
			let seq = guard.snapshot_seq;
			let state = guard.state.clone();
			let last_log_id = state.last_applied_log_id;
			let last_membership = state.last_membership.clone();
			let bytes = encode_snapshot(&state)
				.map_err(|e| StorageIOError::read_snapshot(None, AnyError::error(e)))?;
			let meta = SnapshotMeta {
				last_log_id,
				last_membership,
				snapshot_id: format!("snapshot-{seq}"),
			};
			guard.snapshot = Some(HeldSnapshot {
				meta: meta.clone(),
				data: bytes.clone(),
			});
			(bytes, meta)
		};
		Ok(Snapshot {
			meta,
			snapshot: Box::new(Cursor::new(bytes)),
		})
	}
}

// ============================================================
//  WIRE / SERIALIZATION HELPERS
// ============================================================

fn inspect_artifacts(
	wal_path: &Path,
	vote_path: &Path,
	snapshot_path: &Path,
) -> std::io::Result<ArtifactPresence> {
	Ok(ArtifactPresence {
		wal: artifact_exists(wal_path)?,
		vote: artifact_exists(vote_path)?,
		snapshot: artifact_exists(snapshot_path)?,
	})
}

fn artifact_exists(path: &Path) -> std::io::Result<bool> {
	match std::fs::metadata(path) {
		Ok(meta) => Ok(meta.len() > 0),
		Err(err) if err.kind() == ErrorKind::NotFound => Ok(false),
		Err(err) => Err(err),
	}
}

fn load_vote_state(vote_path: &Path) -> std::io::Result<Option<Vote<u32>>> {
	let bytes = match std::fs::read(vote_path) {
		Ok(bytes) => bytes,
		Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
		Err(err) => return Err(err),
	};
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

fn load_snapshot_artifact(snapshot_path: &Path) -> std::io::Result<Option<LoadedSnapshot>> {
	let bytes = match std::fs::read(snapshot_path) {
		Ok(bytes) => bytes,
		Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
		Err(err) => return Err(err),
	};
	if bytes.is_empty() {
		return Ok(None);
	}
	let (header, state) = decode_snapshot(&bytes).map_err(|err| {
		Error::new(
			ErrorKind::InvalidData,
			format!(
				"snapshot artifact {} decode failed: {err}",
				snapshot_path.display()
			),
		)
	})?;
	let hsize = std::mem::size_of::<SnapshotHeader>();
	let body = &bytes[hsize..];
	let actual_hash = blake3_32(body);
	if actual_hash != header.state_sha256 {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"snapshot artifact {} hash mismatch: expected {:02x?}, got {:02x?}",
				snapshot_path.display(),
				header.state_sha256,
				actual_hash
			),
		));
	}
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
			data: bytes,
		},
	}))
}

fn replay_wal(wal_path: &Path, snapshot: Option<&LoadedSnapshot>) -> std::io::Result<WALReplay> {
	let bytes = match std::fs::read(wal_path) {
		Ok(bytes) => bytes,
		Err(err) if err.kind() == ErrorKind::NotFound => return Ok(WALReplay::default()),
		Err(err) => return Err(err),
	};
	if bytes.is_empty() {
		return Ok(WALReplay {
			last_purged: snapshot.and_then(|s| s.last_included),
			..WALReplay::default()
		});
	}

	let header_size = std::mem::size_of::<WALRecordHeader>();
	let fixed_size = std::mem::size_of::<RaftLogEntryFixed>();
	let boundary = snapshot.and_then(|s| s.last_included.map(|id| id.index));
	let mut replay = WALReplay {
		last_purged: snapshot.and_then(|s| s.last_included),
		..WALReplay::default()
	};

	let mut offset: usize = 0;
	let mut last_good: usize = 0;
	while offset < bytes.len() {
		let remaining = bytes.len() - offset;
		if remaining < header_size {
			truncate_wal_tail(wal_path, last_good)?;
			replay.wal_cursor = last_good as u64;
			return Ok(replay);
		}

		let header: WALRecordHeader = *bytemuck::from_bytes(&bytes[offset..offset + header_size]);
		let total_len = header_size
			.checked_add(header.body_len as usize)
			.and_then(|n| n.checked_add(4))
			.ok_or_else(|| {
				Error::new(
					ErrorKind::InvalidData,
					format!(
						"wal {} record at offset {} overflows declared length",
						wal_path.display(),
						offset
					),
				)
			})?;
		let padded_len = page_round_up(total_len);
		if offset + total_len > bytes.len() || offset + padded_len > bytes.len() {
			truncate_wal_tail(wal_path, last_good)?;
			replay.wal_cursor = last_good as u64;
			return Ok(replay);
		}

		let body_end = offset + header_size + header.body_len as usize;
		let record_end = body_end + 4;
		let expected_crc = blake3_prefix_u32(&bytes[offset..body_end]);
		let actual_crc = u32::from_le_bytes(bytes[body_end..record_end].try_into().unwrap());
		if expected_crc != actual_crc {
			if offset + padded_len == bytes.len() {
				truncate_wal_tail(wal_path, last_good)?;
				replay.wal_cursor = last_good as u64;
				return Ok(replay);
			}
			return Err(interior_corruption(
				wal_path,
				offset,
				"checksum mismatch".to_string(),
			));
		}

		let body = &bytes[offset + header_size..body_end];
		match header.record_type {
			REC_LOG_ENTRY => {
				let entry = decode_log_record(wal_path, offset, &header, body, fixed_size)?;
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
								wal_path.display(),
								entry.log_id.index
							),
						));
					}
					replay.last_log_id = Some(match replay.last_log_id {
						Some(prev) if prev > entry.log_id => prev,
						_ => entry.log_id,
					});
				}
			}
			REC_CHECKPOINT => {
				if body.len() != std::mem::size_of::<WALCheckpointBody>() {
					return Err(interior_corruption(
						wal_path,
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
							wal_path,
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
					wal_path,
					offset,
					format!("unsupported WAL record_type={other}"),
				));
			}
		}

		last_good = offset + padded_len;
		offset = last_good;
	}

	replay.wal_cursor = last_good as u64;
	Ok(replay)
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

fn truncate_wal_tail(wal_path: &Path, last_good: usize) -> std::io::Result<()> {
	let file = OpenOptions::new().write(true).open(wal_path)?;
	file.set_len(last_good as u64)
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
/// `[WALRecordHeader][RaftLogEntryFixed][postcard(entry)][crc32]`
/// with the tail zero-padded to `DIRECT_IO_ALIGN`.
fn serialize_entry_page(entry: &Entry<WendelonTypeConfig>) -> Result<AlignedBuf, AnyError> {
	let encoded = postcard::to_allocvec(entry).map_err(|e| AnyError::new(&e))?;
	let payload_len = encoded.len();
	let header_size = std::mem::size_of::<WALRecordHeader>();
	let fixed_size = std::mem::size_of::<RaftLogEntryFixed>();
	let body_len = fixed_size + payload_len;
	let total = header_size + body_len + 4;
	let padded = page_round_up(total);

	let (kind, epoch) = match &entry.payload {
		EntryPayload::Normal(WendelonRequest::Finalize(m)) => (LogEntryKind::APPEND, m.epoch),
		EntryPayload::Normal(WendelonRequest::AcquireLease(l)) => (LogEntryKind::APPEND, l.epoch),
		EntryPayload::Normal(WendelonRequest::AdvanceEpoch(r)) => {
			(LogEntryKind::CONFIG, r.to.epoch)
		}
		EntryPayload::Normal(WendelonRequest::SchemaUpdate(_)) => (LogEntryKind::APPEND, 0),
		EntryPayload::Normal(WendelonRequest::TierSlab(_)) => (LogEntryKind::APPEND, 0),
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
		payload_hash: blake3_32(&encoded),
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
	dst[..header_size].copy_from_slice(bytemuck::bytes_of(&header));
	dst[header_size..header_size + fixed_size].copy_from_slice(bytemuck::bytes_of(&fixed));
	dst[header_size + fixed_size..header_size + fixed_size + payload_len].copy_from_slice(&encoded);
	let crc = blake3_prefix_u32(&dst[..header_size + body_len]);
	dst[header_size + body_len..header_size + body_len + 4].copy_from_slice(&crc.to_le_bytes());
	// Remainder is already zero from AlignedBuf::new.
	Ok(buf)
}

#[cfg(test)]
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
	let crc = blake3_prefix_u32(&dst[..header_size + body_size]);
	dst[header_size + body_size..header_size + body_size + 4].copy_from_slice(&crc.to_le_bytes());
	buf
}

/// Encode a complete snapshot: `[SnapshotHeader][postcard(SmState)]`.
fn encode_snapshot(state: &SmState) -> Result<Vec<u8>, AnyError> {
	let body = postcard::to_allocvec(state).map_err(|e| AnyError::new(&e))?;
	let (last_index, last_term) = match state.last_applied_log_id {
		Some(id) => (id.index, id.leader_id.term),
		None => (0, 0),
	};
	let header = SnapshotHeader {
		last_included_index: last_index,
		last_included_term: last_term,
		last_included_epoch: 0,
		snapshot_hlc: HybridTimestamp {
			physical_ms: 0,
			logical: 0,
			node_id: 0,
		},
		state_sha256: blake3_32(&body),
		cache_checkpoint_len: 0,
		_pad: [0; 4],
	};
	let mut out = Vec::with_capacity(std::mem::size_of::<SnapshotHeader>() + body.len());
	out.extend_from_slice(bytemuck::bytes_of(&header));
	out.extend_from_slice(&body);
	Ok(out)
}

/// Inverse of [`encode_snapshot`]. Returns the decoded header
/// alongside the state for future hash-validation wiring.
fn decode_snapshot(bytes: &[u8]) -> Result<(SnapshotHeader, SmState), String> {
	let hsize = std::mem::size_of::<SnapshotHeader>();
	if bytes.len() < hsize {
		return Err("snapshot shorter than header".into());
	}
	let header: SnapshotHeader = *bytemuck::from_bytes(&bytes[..hsize]);
	let state: SmState = postcard::from_bytes(&bytes[hsize..])
		.map_err(|e| format!("snapshot postcard decode: {e}"))?;
	Ok((header, state))
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

fn io_errno(result: i32) -> AnyError {
	AnyError::error(format!("io_uring errno {}", -result))
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
	use crate::schema::{FinalizationMarker, LeaseDescriptor, NodeId as SchemaNodeId};
	use openraft::Vote;
	use std::io::Write;
	use std::path::PathBuf;

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
			signature: [0; 64],
			_pad: [0; 8],
		}
	}

	fn finalize_entry(index: u64, physical_ms: u64) -> Entry<WendelonTypeConfig> {
		Entry {
			log_id: log_id(1, index),
			payload: EntryPayload::Normal(WendelonRequest::Finalize(FinalizationMarker {
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
				_reserved: 0,
			})),
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

	fn open_store(dir: &Path) -> WendelonLogStore {
		let io = crate::storage::io_uring::spawn_io_thread(64).unwrap();
		let (wal, vote, snapshot) = artifact_paths(dir);
		WendelonLogStore::open(&wal, &vote, &snapshot, io).unwrap()
	}

	#[tokio::test]
	async fn normal_restart_recovers_vote_wal_and_state_machine() {
		let dir = tempdir("normal");
		let (wal, vote, _snapshot) = artifact_paths(&dir);
		write_vote_file(&vote, Vote::new_committed(3, 7));
		let entry = lease_entry(1, 0, 99, 7);
		write_wal_pages(&wal, &[serialize_entry_page(&entry).unwrap()]);

		let mut store = open_store(&dir);
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

		let mut store = open_store(&dir);
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
		let err = match WendelonLogStore::open(
			&wal,
			dir.join("vote.wnd"),
			dir.join("snapshot.wnd"),
			io,
		) {
			Ok(_) => panic!("interior corruption must fail startup"),
			Err(err) => err,
		};
		assert_eq!(err.kind(), ErrorKind::InvalidData);
		assert!(
			err.to_string().contains("interior corruption"),
			"unexpected error: {err}"
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

		let mut store = open_store(&dir);
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

		let mut store = open_store(&dir);
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
}

// ============================================================
//  LEASE APPLY TESTS
// ============================================================

#[cfg(test)]
mod lease_tests {
	use super::*;
	use crate::schema::{HybridTimestamp, LeaseDescriptor, NodeId as SchemaNodeId};

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
			signature: [0; 64],
			_pad: [0; 8],
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
