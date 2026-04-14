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

use crate::consensus::{TieredSlab, WendelonRequest, WendelonResponse, WendelonTypeConfig};
use crate::schema::{
    HybridTimestamp, LogEntryKind, NodeId as SchemaNodeId, RaftLogEntryFixed, SnapshotHeader,
    WALRecordHeader,
};
use crate::storage::io_uring::{AlignedBuf, IoOp, IoRingHandle, DIRECT_IO_ALIGN};
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
use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::fs::{File, OpenOptions};
use std::io::Cursor;
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
    /// openraft unit tests and for the current scaffold wiring
    /// where the disk path has not yet been plumbed at bootstrap.
    pub fn in_memory() -> Self {
        Self {
            inner: Arc::new(RwLock::new(LogStoreInner {
                entries: BTreeMap::new(),
                last_log_id: None,
                last_purged: None,
                vote: None,
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

    /// Open (or create) the WAL and vote files and bind them to
    /// the supplied `io_uring` submission handle. Does not replay
    /// the WAL yet — recovery is tracked as a follow-up.
    pub fn open(
        wal_path: impl AsRef<Path>,
        vote_path: impl AsRef<Path>,
        io: IoRingHandle,
    ) -> std::io::Result<Self> {
        let wal_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .custom_flags(O_DIRECT)
            .open(wal_path.as_ref())?;
        let wal_fd = wal_file.as_raw_fd();

        let vote_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .custom_flags(O_DIRECT)
            .open(vote_path.as_ref())?;
        let vote_fd = vote_file.as_raw_fd();

        Ok(Self {
            inner: Arc::new(RwLock::new(LogStoreInner {
                entries: BTreeMap::new(),
                last_log_id: None,
                last_purged: None,
                vote: None,
                io: Some(io),
                _wal_file: Some(wal_file),
                wal_fd: Some(wal_fd),
                wal_cursor: 0,
                _vote_file: Some(vote_file),
                vote_fd: Some(vote_fd),
            })),
            wal_gate: Arc::new(Mutex::new(())),
        })
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
            .range((clone_bound(range.start_bound()), clone_bound(range.end_bound())))
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
        let encoded = postcard::to_allocvec(vote)
            .map_err(|e| StorageError::<u32>::from(StorageIOError::write_vote(AnyError::new(&e))))?;
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
        let completion = reply
            .await
            .map_err(|_| StorageIOError::write_vote(AnyError::error("vote reply channel closed")))?;
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
                    callback.log_io_completed(Err(std::io::Error::other(format!(
                        "wal submit: {e}"
                    ))));
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
                callback.log_io_completed(Err(std::io::Error::from_raw_os_error(
                    -completion.result,
                )));
                return Err(StorageIOError::write_logs(io_errno(completion.result)).into());
            }
        }

        // Durability barrier. All write CQEs above have already
        // been awaited, so this fsync meaningfully covers them
        // despite `io_uring`'s unordered fsync-vs-write semantics.
        let reply = match io.submit_fsync(fd).await {
            Ok(r) => r,
            Err(e) => {
                callback.log_io_completed(Err(std::io::Error::other(format!(
                    "wal fsync submit: {e}"
                ))));
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
            callback.log_io_completed(Err(std::io::Error::from_raw_os_error(
                -completion.result,
            )));
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
    /// Per-holder active lease. Keyed by `LeaseDescriptor.holder.id`
    /// — our current leases are per-leader; once per-range lease
    /// semantics land the key will switch to a range identifier
    /// without touching the openraft surface.
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
#[derive(Clone)]
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
                let log_id = entry.log_id;
                let resp = match entry.payload {
                    EntryPayload::Blank => WendelonResponse::Success,
                    EntryPayload::Normal(req) => {
                        let (resp, tier) = apply_request(&mut guard.state, req);
                        if let Some(t) = tier {
                            pending_tier_events.push(t);
                        }
                        resp
                    }
                    EntryPayload::Membership(m) => {
                        guard.state.last_membership = StoredMembership::new(Some(log_id), m);
                        WendelonResponse::Success
                    }
                };
                guard.state.last_applied_log_id = Some(match guard.state.last_applied_log_id {
                    Some(prev) if prev > log_id => prev,
                    _ => log_id,
                });
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
            let key = lease.holder.id as u64;
            // Deny if an existing lease for this holder is still
            // valid at the requested lease_start and no epoch bump
            // has occurred — a new epoch invalidates all leases.
            if let Some(existing) = state.leases.get(&key) {
                if existing.lease_expiry > lease.lease_start && existing.epoch >= lease.epoch {
                    return (WendelonResponse::LeaseDenied, None);
                }
            }
            state.leases.insert(key, lease);
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
        EntryPayload::Normal(WendelonRequest::SchemaUpdate(_)) => (LogEntryKind::APPEND, 0),
        EntryPayload::Normal(WendelonRequest::TierSlab(_)) => (LogEntryKind::APPEND, 0),
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
    dst[header_size + fixed_size..header_size + fixed_size + payload_len]
        .copy_from_slice(&encoded);
    let crc = blake3_prefix_u32(&dst[..header_size + body_len]);
    dst[header_size + body_len..header_size + body_len + 4].copy_from_slice(&crc.to_le_bytes());
    // Remainder is already zero from AlignedBuf::new.
    Ok(buf)
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
