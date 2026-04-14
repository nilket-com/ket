//! openraft storage-v2 traits for Wendelon's Control Ring.
//!
//! This file ties [`WendelonTypeConfig`] to the three openraft v2 surfaces:
//!
//!  * [`RaftLogStorage`] — durable Raft log plus `vote` persistence.
//!  * [`RaftStateMachine`] — applies committed entries and owns snapshot IO.
//!  * [`RaftSnapshotBuilder`] — materialises a snapshot view of the SM.
//!
//! Status: compilable skeleton. Every method returns a `StorageError`
//! that wraps a [`NotWired`] sentinel so the Raft core fails fast and
//! loudly — instead of silently succeeding — while the real backends
//! are bolted on. The eventual wiring is:
//!
//!  * Log entries land in an `O_DIRECT` append-only slab on the
//!    Control Ring disk via the [`crate::storage::IoRingHandle`].
//!    The per-entry size is bounded by the Stellarator Invariant
//!    (ADR 003) — no bulk rows live here.
//!  * The state machine is an in-memory map of lease/epoch/E_safe
//!    state, snapshotted to disk as a serialized blob.
//!  * Snapshot data rides [`std::io::Cursor<Vec<u8>>`] exactly as
//!    declared in [`WendelonTypeConfig`].
//!
//! Keeping all three impls in one module (rather than splitting the
//! log store, state machine, and snapshot builder across files)
//! matches openraft's own reference store and makes it easy to share
//! the `Arc<Inner>` once real state is introduced.

use crate::consensus::{WendelonRequest, WendelonResponse, WendelonTypeConfig};
use openraft::anyerror::AnyError;
use openraft::storage::{LogFlushed, LogState, RaftLogReader, RaftLogStorage, RaftSnapshotBuilder, RaftStateMachine, Snapshot};
use openraft::{BasicNode, Entry, LogId, OptionalSend, SnapshotMeta, StorageError, StorageIOError, StoredMembership, Vote};
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;

// ============================================================
//  LOG STORAGE
// ============================================================

/// Durable log for Wendelon's Control Ring Raft group.
///
/// Will eventually delegate writes to a dedicated `io_uring` slab on
/// the Control Ring disk; reads are served from the same slab plus an
/// in-memory index. Today every method stubs out to `NotWired`.
#[derive(Default)]
pub struct WendelonLogStore {
    // TODO: Arc<Mutex<LogSlab>> + IoRingHandle once the slab layout
    // for Raft entries lands (see plans/consensus/raft-core.md).
}

impl WendelonLogStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl RaftLogReader<WendelonTypeConfig> for WendelonLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        _range: RB,
    ) -> Result<Vec<Entry<WendelonTypeConfig>>, StorageError<u32>> {
        Err(StorageIOError::read_logs(not_wired("try_get_log_entries")).into())
    }
}

impl RaftLogStorage<WendelonTypeConfig> for WendelonLogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<WendelonTypeConfig>, StorageError<u32>> {
        // An uninitialised store has no purged or present log id.
        Ok(LogState::default())
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        WendelonLogStore::new()
    }

    async fn save_vote(&mut self, _vote: &Vote<u32>) -> Result<(), StorageError<u32>> {
        Err(StorageIOError::write_vote(not_wired("save_vote")).into())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u32>>, StorageError<u32>> {
        // Returning None is valid for a fresh store — the node has
        // never voted. Once on-disk vote persistence lands this will
        // decode from the dedicated vote slot.
        Ok(None)
    }

    async fn append<I>(&mut self, _entries: I, callback: LogFlushed<WendelonTypeConfig>) -> Result<(), StorageError<u32>>
    where
        I: IntoIterator<Item = Entry<WendelonTypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        // Fire the callback with an IO error so openraft propagates
        // the failure through its flush-tracking path instead of
        // assuming the write succeeded.
        callback.log_io_completed(Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "wendelon raft log append not yet wired",
        )));
        Err(StorageIOError::write_logs(not_wired("append")).into())
    }

    async fn truncate(&mut self, _log_id: LogId<u32>) -> Result<(), StorageError<u32>> {
        Err(StorageIOError::write_logs(not_wired("truncate")).into())
    }

    async fn purge(&mut self, _log_id: LogId<u32>) -> Result<(), StorageError<u32>> {
        Err(StorageIOError::write_logs(not_wired("purge")).into())
    }
}

// ============================================================
//  STATE MACHINE
// ============================================================

/// Wendelon's Raft state machine. Holds lease/epoch/E_safe state and
/// owns the current snapshot. For now everything is stubbed; the
/// real implementation will be an `Arc<RwLock<SmInner>>` shared with
/// the [`WendelonSnapshotBuilder`] so snapshots are cheap views.
#[derive(Default)]
pub struct WendelonStateMachine {
    // TODO: Arc<RwLock<SmInner>> with the applied log id, stored
    // membership, lease table, E_safe watermark, and current snapshot.
}

impl WendelonStateMachine {
    pub fn new() -> Self {
        Self::default()
    }
}

impl RaftStateMachine<WendelonTypeConfig> for WendelonStateMachine {
    type SnapshotBuilder = WendelonSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u32>>, StoredMembership<u32, BasicNode>), StorageError<u32>> {
        // A fresh SM reports no applied entries and empty membership.
        Ok((None, StoredMembership::default()))
    }

    async fn apply<I>(&mut self, _entries: I) -> Result<Vec<WendelonResponse>, StorageError<u32>>
    where
        I: IntoIterator<Item = Entry<WendelonTypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        // Until the SM is wired up every apply fails loudly; this
        // prevents the Raft core from advancing commit index against
        // a silent no-op state machine.
        //
        // The `LogId` attached here is a sentinel — the real impl
        // will thread each entry's `log_id` through so the subject of
        // any apply failure points to the exact offending entry.
        Err(StorageIOError::apply(
            LogId::default(),
            not_wired("apply"),
        )
        .into())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        WendelonSnapshotBuilder::new()
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Box<Cursor<Vec<u8>>>, StorageError<u32>> {
        // A fresh in-memory buffer is the right receive target for
        // `SnapshotData = Cursor<Vec<u8>>`. Install happens in
        // `install_snapshot` below.
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        _meta: &SnapshotMeta<u32, BasicNode>,
        _snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<u32>> {
        Err(StorageIOError::write_snapshot(None, not_wired("install_snapshot")).into())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<WendelonTypeConfig>>, StorageError<u32>> {
        // No snapshot yet; this is a valid, non-error outcome for a
        // freshly booted node that has never built or received one.
        Ok(None)
    }
}

// ============================================================
//  SNAPSHOT BUILDER
// ============================================================

/// Produces snapshots of [`WendelonStateMachine`]. Once the SM owns
/// real state, this type will hold an `Arc` clone of the SM inner
/// and serialise it on demand.
#[derive(Default)]
pub struct WendelonSnapshotBuilder {
    // TODO: Arc<RwLock<SmInner>> — shared read handle into the SM.
}

impl WendelonSnapshotBuilder {
    pub fn new() -> Self {
        Self::default()
    }
}

impl RaftSnapshotBuilder<WendelonTypeConfig> for WendelonSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<WendelonTypeConfig>, StorageError<u32>> {
        Err(StorageIOError::read_snapshot(None, not_wired("build_snapshot")).into())
    }
}

// ============================================================
//  STUB ERROR
//
//  Used while the storage engines are unwired. Returning a real
//  `StorageError` backed by this sentinel means any attempt to
//  drive Raft against a bare skeleton surfaces immediately with a
//  pointed message, rather than looking like generic IO failure.
// ============================================================

#[derive(Debug, thiserror::Error)]
#[error("wendelon raft storage not yet wired: {method}")]
struct NotWired {
    method: &'static str,
}

fn not_wired(method: &'static str) -> AnyError {
    AnyError::new(&NotWired { method })
}

// Silence dead-code lint on the dummy field bindings used by the
// stub types before real state lands. The `#[allow(dead_code)]` on
// individual fields would be noisier than this single blanket
// suppression, and the moment those fields exist this attribute can
// be removed with no other churn.
#[allow(dead_code)]
fn _unused_request_type_witness(_r: WendelonRequest) {}
