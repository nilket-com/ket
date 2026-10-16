//! ADR 075: idempotent atomic counter fetch-add — the local storage-API-boundary
//! correctness core, now carrying the **tentative lifecycle** the cut-2 distributed
//! protocol sits on.
//!
//! Still **no network dispatch, no idempotency, and no witness machinery** — those
//! wrap this core in the data-server cut; until then `COUNTER_ADD_REQ` stays
//! non-callable. What this module owns:
//!
//! - the **per-key read-modify-write critical section** (D2) held across the whole
//!   tentative → quorum-decision → commit/rollback sequence, so concurrent adders
//!   cannot share a `previous` and a failed add never poisons the next one;
//! - **witness-after-quorum** (the cut-2 correction): a tentative append indexes the
//!   counter version but emits **no** pending-finalization witness — the witness only
//!   appears on `commit`, so a quorum-failed add has nothing the reconciler could later
//!   finalize. (`handle_replicate` witnesses ordinary legs immediately, which is why
//!   the counter cannot reuse `REPLICATE_REQ`.)
//! - **rollback-on-quorum-failure** that removes the tentative indexed version, so
//!   `read_committed` (the next add's base) is never poisoned;
//! - fail-closed deltas (D5), absent-creates-from-0 (D6), and structural counter/blob
//!   domain separation (D7).
//!
//! The `quorum` decision is supplied by the caller (the data server fans the tentative
//! out to peers over the cut-2 counter-replicate frame and awaits quorum); this core
//! holds the per-key lock across that await and commits or rolls back accordingly.

pub(crate) mod visibility;
pub use visibility::{CounterVersion, CounterVisibilityError};
use visibility::{EligibleCells, winner};

use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::Mutex;

/// A counter record's caller key, in the reserved counter domain. A distinct type so a
/// counter key can never be confused with a blob key at the storage API boundary —
/// domain separation is structural (the type/keyspace), not caller discipline (D7).
#[derive(Clone, PartialEq, Eq, Hash, Debug, serde::Serialize, serde::Deserialize)]
pub struct CounterKey(pub Vec<u8>);

/// Why a `fetch_add` was rejected. Every rejection is **without a committed mutation**.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CounterAddError {
	/// `delta == 0` — a `fetch_add` is a mutation, not a read (D5).
	ZeroDelta,
	/// `previous + delta` would exceed `u64::MAX`; no wrap, no saturate (D5).
	Overflow,
	/// The key holds a non-counter (blob) record — cross-kind add rejected (D7).
	BlobKeyConflict,
	/// The tentative append did not reach data quorum; it was rolled back, leaving no
	/// visible version (so the next add's `previous` is not poisoned).
	QuorumFailed,
	/// Quorum was reached but the durable commit marker could not be written; the live
	/// index was rolled back and the tentative left as an unreachable orphan (recovery
	/// skips it). The add did not commit.
	CommitMarkerFailed,
}

/// A counter-op identity. Stamped into the durable tentative frame and referenced by the
/// commit marker so recovery can pair them — counter-specific, not a generic record id.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct CounterOpId(pub u64);

/// A durable counter frame. The model is deliberately **counter-specific** — it does not
/// add MVCC / finalization-aware reads to ordinary records. Only two frame kinds exist:
/// a tentative value carrying its `op_id`, and a commit marker referencing that `op_id`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CounterFrame {
	/// A tentative counter value — durable bytes carry the `op_id` to match a commit
	/// marker. Indexed for the live view, but NOT counted on recovery until committed.
	Tentative {
		op_id: CounterOpId,
		key: CounterKey,
		value: u64,
	},
	/// A commit marker for `op_id` — makes the paired tentative recoverable.
	Commit { op_id: CounterOpId },
}

/// The **recovery rule** (counter-specific): replay the durable counter frames and index
/// a tentative value **only if a matching commit marker exists** — orphan tentatives
/// (rolled back, or crashed mid-commit) are skipped, never resurrected. Later committed
/// values for a key supersede earlier ones (frame order is append/HLC order). This is the
/// production rule the real `recover_slab` applies to counter frames; here it is a pure
/// function so the restart-survival proofs exercise it directly.
pub fn recover_counter_index(
	frames: &[CounterFrame],
) -> std::collections::HashMap<CounterKey, u64> {
	use std::collections::HashSet;
	let committed: HashSet<CounterOpId> = frames
		.iter()
		.filter_map(|f| match f {
			CounterFrame::Commit { op_id } => Some(*op_id),
			CounterFrame::Tentative { .. } => None,
		})
		.collect();
	let mut index = std::collections::HashMap::new();
	for frame in frames {
		if let CounterFrame::Tentative { op_id, key, value } = frame {
			if committed.contains(op_id) {
				index.insert(key.clone(), *value);
			}
			// else: orphan tentative — skipped (the non-negotiable: no resurrection).
		}
	}
	index
}

/// The pre/post values of a successful (committed) add.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CounterAddOutcome {
	pub previous: u64,
	pub current: u64,
}

/// The canonical public idempotency key for one counter add — the 24-byte
/// `ClientWriteId::canonical_idempotency_bytes()`. The leader keys its D4
/// dedup record by THIS (never a hashed/narrowed handle — collisions would
/// break exactly-once), so the public idempotency contract lives in the
/// mutation layer, durable with the witness, not in a server-side cache that
/// dies with the process.
pub type CounterWriteKey = [u8; 24];

/// How a leader `fetch_add` resolved: a fresh apply, or an idempotent replay
/// of the recorded outcome for an already-applied `CounterWriteKey` (D4). The
/// caller needs the distinction to answer `OK` vs `DUPLICATE` faithfully —
/// including for a retry that lands after a node restart.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CounterAddResult {
	/// The delta was applied by THIS call.
	Applied(CounterAddOutcome),
	/// The write-key was already applied — the original outcome, nothing re-added.
	Replayed(CounterAddOutcome),
}

impl CounterAddResult {
	pub fn outcome(self) -> CounterAddOutcome {
		match self {
			Self::Applied(outcome) | Self::Replayed(outcome) => outcome,
		}
	}
	pub fn is_replay(self) -> bool {
		matches!(self, Self::Replayed(_))
	}
}

/// The outcome of the caller-supplied data-quorum decision over the tentative append.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QuorumOutcome {
	/// Data quorum reached — promote the tentative version to committed.
	Reached,
	/// Data quorum not reached — roll the tentative version back everywhere.
	Failed,
}

/// Opaque handle to an in-flight tentative version, returned by `tentative_append` and
/// consumed by `commit_tentative` / `rollback_tentative`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TentativeId(pub u64);

/// The local store the counter core reads and appends through, exposing the tentative
/// lifecycle. The real implementation is the storage manager (index without a witness,
/// commit emits the witness, rollback un-indexes); the in-memory test implementation
/// proves the core. Ops are async to model the real storage path (so the per-key lock
/// is load-bearing across await points).
pub trait CounterBackend {
	/// Latest **committed** counter value for `key`. `Ok(None)` = absent;
	/// `Err(BlobKeyConflict)` = the key holds a blob record (domain violation).
	fn read_committed(
		&self,
		key: &CounterKey,
	) -> impl std::future::Future<Output = Result<Option<u64>, CounterAddError>> + Send;
	/// Index a **tentative** version (visible to recovery/peers, but with **no**
	/// pending-finalization witness). Returns its handle.
	fn tentative_append(
		&self,
		key: &CounterKey,
		value: u64,
	) -> impl std::future::Future<Output = TentativeId> + Send;
	/// Promote a tentative version to committed: write the durable commit marker, THEN
	/// treat the value as committed for future reads. `Err(())` if the durable marker
	/// write fails — the caller must then roll back the live index (the tentative is
	/// left as an unreachable orphan that recovery skips).
	fn commit_tentative(
		&self,
		key: &CounterKey,
		id: TentativeId,
	) -> impl std::future::Future<Output = Result<(), ()>> + Send;
	/// Remove a tentative version — un-index it, leaving no visible value.
	fn rollback_tentative(
		&self,
		key: &CounterKey,
		id: TentativeId,
	) -> impl std::future::Future<Output = ()> + Send;
}

/// The per-key read-modify-write critical section over a [`CounterBackend`].
pub struct CounterCore<B> {
	backend: B,
	/// One async lock per counter key — the D2 critical section, held across the whole
	/// tentative → quorum → commit/rollback sequence. Acquired from a cloned `Arc`,
	/// never while holding the `DashMap` shard guard.
	locks: DashMap<CounterKey, Arc<Mutex<()>>>,
}

impl<B: CounterBackend> CounterCore<B> {
	pub fn new(backend: B) -> Self {
		Self {
			backend,
			locks: DashMap::new(),
		}
	}

	/// Atomic `+delta`, gated on the caller's data-quorum decision. Under the per-key
	/// lock: `read_committed` (base) → `checked_add` → `tentative_append` (no witness) →
	/// await the `quorum` decision → `commit` on `Reached`, `rollback` on `Failed`. The
	/// lock is held across the quorum await, so a concurrent adder cannot read a
	/// tentative-or-failed value, and a failed add leaves nothing behind (D6 no-poison).
	/// `delta == 0` and overflow fail closed *before* any append (D5). An absent counter
	/// bases at `0` (D6).
	pub async fn fetch_add_with_quorum<Q, Fut>(
		&self,
		key: &CounterKey,
		delta: u64,
		quorum: Q,
	) -> Result<CounterAddOutcome, CounterAddError>
	where
		Q: FnOnce(u64) -> Fut,
		Fut: std::future::Future<Output = QuorumOutcome>,
	{
		if delta == 0 {
			return Err(CounterAddError::ZeroDelta);
		}
		let lock = self.locks.entry(key.clone()).or_default().clone();
		let _guard = lock.lock().await;
		let previous = self.backend.read_committed(key).await?.unwrap_or(0);
		let current = previous
			.checked_add(delta)
			.ok_or(CounterAddError::Overflow)?;
		let id = self.backend.tentative_append(key, current).await;
		match quorum(current).await {
			QuorumOutcome::Reached => match self.backend.commit_tentative(key, id).await {
				Ok(()) => Ok(CounterAddOutcome { previous, current }),
				// Commit marker write failed: roll back the live index, leave the orphan
				// tentative (recovery skips it). The add did not commit.
				Err(()) => {
					self.backend.rollback_tentative(key, id).await;
					Err(CounterAddError::CommitMarkerFailed)
				}
			},
			QuorumOutcome::Failed => {
				self.backend.rollback_tentative(key, id).await;
				Err(CounterAddError::QuorumFailed)
			}
		}
	}

	/// Point read of the committed counter. `Ok(None)` = absent (the API tells the truth
	/// about existence; the consumer defaults `None -> 0`, D6). Never observes a
	/// tentative version.
	pub async fn read(&self, key: &CounterKey) -> Result<Option<u64>, CounterAddError> {
		self.backend.read_committed(key).await
	}
}

// ============================================================
//  ADR 075 cut-2 substrate: the REAL local counter store
// ============================================================
//
// `CounterCore` above proves the lifecycle logic against an in-memory backend.
// `SlabCounterStore` is the REAL local substrate: durable counter frames on a
// [`LocalSlabStore`], recovered through the production `recover_slab` rule
// (`record_class` discriminant + committed-only indexing). This section stays
// local-only: a committed value survives restart, a rolled-back or marker-failed
// tentative does not, and ordinary blob recovery is untouched.
//
// Lifecycle (the operator's model): a tentative append **indexes** the counter
// version for the live view (the per-key lock in the consumer serializes the
// window) but writes **no** commit marker; the durable commit marker is what
// makes the value survive restart ("committed for future reads"); a live
// rollback removes the in-memory index entry only, leaving the durable tentative
// as an orphan that recovery skips — no durable abort marker needed.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::schema::{
	CounterReplicaPhase, CounterTentativeFrame, HybridTimestamp, Identity, NodeId, TenantId,
	record_class,
};
use crate::storage::StorageEngine;
use crate::storage::core::{LocalSlabStore, RecordLocation};

/// Fixed 32-byte domain key separating counter identities from blob identities
/// (D7, structural). A counter key `k` derives identity
/// `Identity::blake3_keyed(COUNTER_DOMAIN, k)`, which cannot collide with a
/// blob's tenant-scoped identity for the same caller bytes — the physical
/// collision is unrepresentable, not merely discouraged.
const COUNTER_DOMAIN: &[u8; 32] = b"wendelon::adr075::counter-domain";

/// Domain-separated counter identity for `key` (D7 structural separation): a
/// counter key derives a `blake3_keyed` identity in the reserved counter domain,
/// which cannot collide with a blob's tenant-scoped identity for the same bytes.
pub fn counter_identity(key: &CounterKey) -> Identity {
	Identity::blake3_keyed(COUNTER_DOMAIN, &key.0)
}

/// A handle to a durable tentative counter version: the counter's identity and
/// the tentative frame's on-disk location. The op-id is the location's offset
/// (globally unique, never reused — the write cursor resumes past it on restart).
#[derive(Clone, Copy, Debug)]
pub struct SlabTentative {
	key_identity: Identity,
	location: RecordLocation,
}

impl SlabTentative {
	fn op_id(&self) -> u64 {
		self.location.offset
	}
}

/// The counter-frame storage primitives [`SlabCounterStore`] runs on, so the
/// same tentative/commit/rollback logic serves both [`LocalSlabStore`] (the
/// unit substrate) and [`SlabManager`] (production). Deliberately
/// **counter-shaped**: there is no generic "write an arbitrary `record_class`
/// without indexing" method, so ordinary blob append/index/LWW semantics stay
/// untouched and no second generic append path exists.
pub trait CounterSlab {
	/// Append a tentative counter value at `hlc` and index it live (no marker).
	fn counter_tentative(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
		hlc: HybridTimestamp,
		value: u64,
		origin: NodeId,
	) -> impl std::future::Future<Output = std::io::Result<RecordLocation>> + Send;
	/// Append a durable commit marker for `tentative_op_id` (not a version, not
	/// indexed) and make it durable.
	fn counter_commit_marker(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
		hlc: HybridTimestamp,
		tentative_op_id: u64,
		origin: NodeId,
	) -> impl std::future::Future<Output = std::io::Result<()>> + Send;
	/// Remove a tentative version from the live index (rollback; recovery skips
	/// the orphan).
	fn counter_unindex(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
		location: RecordLocation,
	);
	/// Latest indexed version for the counter key.
	fn counter_lookup(&self, tenant_id: TenantId, key_identity: Identity)
	-> Option<RecordLocation>;
	/// Read a counter value frame and parse its little-endian `u64`.
	fn counter_read_value(
		&self,
		location: RecordLocation,
	) -> impl std::future::Future<Output = std::io::Result<u64>> + Send;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CounterFormatError {
	InvalidOriginFrame,
	MaintenanceUnsupported,
}
impl std::fmt::Display for CounterFormatError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "counter format {self:?}")
	}
}
impl std::error::Error for CounterFormatError {}
impl CounterFormatError {
	pub(crate) fn io(self) -> std::io::Error {
		std::io::Error::new(std::io::ErrorKind::InvalidData, self)
	}
}
pub(crate) fn encode_origin_frame(value: u64, origin: NodeId) -> std::io::Result<[u8; 16]> {
	if origin.id == 0 || origin._reserved != 0 {
		return Err(CounterFormatError::InvalidOriginFrame.io());
	}
	let mut bytes = [0; 16];
	bytes[..8].copy_from_slice(&value.to_le_bytes());
	bytes[8..12].copy_from_slice(&origin.id.to_le_bytes());
	Ok(bytes)
}
pub(crate) fn decode_origin_frame(bytes: &[u8]) -> std::io::Result<(u64, NodeId)> {
	if bytes.len() != 16 || bytes[12..16] != [0; 4] {
		return Err(CounterFormatError::InvalidOriginFrame.io());
	}
	let origin = u32::from_le_bytes(bytes[8..12].try_into().expect("fixed origin"));
	if origin == 0 {
		return Err(CounterFormatError::InvalidOriginFrame.io());
	}
	Ok((
		u64::from_le_bytes(bytes[..8].try_into().expect("fixed value")),
		NodeId {
			id: origin,
			_reserved: 0,
		},
	))
}

fn parse_counter_value(frame: &[u8]) -> std::io::Result<u64> {
	if frame.len() < std::mem::size_of::<CounterTentativeFrame>() {
		return Err(std::io::Error::new(
			std::io::ErrorKind::InvalidData,
			"counter version frame shorter than its fixed value",
		));
	}
	Ok(u64::from_le_bytes(frame[..8].try_into().unwrap()))
}

impl CounterSlab for LocalSlabStore {
	async fn counter_tentative(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
		hlc: HybridTimestamp,
		value: u64,
		origin: NodeId,
	) -> std::io::Result<RecordLocation> {
		let frame = encode_origin_frame(value, origin)?;
		let location = self
			.write_framed(
				tenant_id,
				key_identity,
				hlc,
				record_class::COUNTER_TENTATIVE_V2,
				&frame,
			)
			.await?;
		self.index_location(tenant_id, key_identity, location);
		Ok(location)
	}
	async fn counter_commit_marker(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
		hlc: HybridTimestamp,
		tentative_op_id: u64,
		origin: NodeId,
	) -> std::io::Result<()> {
		let marker = encode_origin_frame(tentative_op_id, origin)?;
		self.write_framed(
			tenant_id,
			key_identity,
			hlc,
			record_class::COUNTER_COMMIT_V2,
			&marker,
		)
		.await?;
		self.wait_durable_commit().await?;
		Ok(())
	}
	fn counter_unindex(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
		location: RecordLocation,
	) {
		self.rollback_index_entry(tenant_id, key_identity, location);
	}
	fn counter_lookup(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
	) -> Option<RecordLocation> {
		self.lookup(tenant_id, key_identity)
	}
	async fn counter_read_value(&self, location: RecordLocation) -> std::io::Result<u64> {
		parse_counter_value(&self.read(location).await?)
	}
}

impl CounterSlab for crate::storage::manager::SlabManager {
	async fn counter_tentative(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
		hlc: HybridTimestamp,
		value: u64,
		origin: NodeId,
	) -> std::io::Result<RecordLocation> {
		self.counter_append_tentative(tenant_id, key_identity, hlc, value, origin)
			.await
	}
	async fn counter_commit_marker(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
		hlc: HybridTimestamp,
		tentative_op_id: u64,
		origin: NodeId,
	) -> std::io::Result<()> {
		self.counter_append_commit_marker(tenant_id, key_identity, hlc, tentative_op_id, origin)
			.await
	}
	fn counter_unindex(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
		location: RecordLocation,
	) {
		self.counter_unindex_tentative(tenant_id, key_identity, location);
	}
	fn counter_lookup(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
	) -> Option<RecordLocation> {
		self.lookup(tenant_id, key_identity)
	}
	async fn counter_read_value(&self, location: RecordLocation) -> std::io::Result<u64> {
		parse_counter_value(&self.read(location).await?)
	}
}

/// The real counter store over a [`CounterSlab`] (a `LocalSlabStore` in unit
/// tests, a `SlabManager` in production). The tentative/commit/rollback logic is
/// identical for both — only the slab primitives differ.
pub struct SlabCounterStore<S> {
	slab: Arc<S>,
	origin: NodeId,
	tenant_id: TenantId,
	/// Test seam: when armed, the next `commit_tentative` models a failed durable
	/// marker write (no marker frame reaches the slab — the on-disk state is
	/// exactly that of a real failed marker write).
	#[cfg(test)]
	fail_next_commit_marker: std::sync::atomic::AtomicBool,
}

impl<S: CounterSlab> SlabCounterStore<S> {
	pub fn new(slab: Arc<S>, tenant_id: TenantId, origin: NodeId) -> Self {
		Self {
			slab,
			origin,
			tenant_id,
			#[cfg(test)]
			fail_next_commit_marker: std::sync::atomic::AtomicBool::new(false),
		}
	}

	/// Latest **committed** counter value for `key`, or `None` if absent.
	pub async fn read_committed(&self, key: &CounterKey) -> std::io::Result<Option<u64>> {
		let identity = counter_identity(key);
		let Some(location) = self.slab.counter_lookup(self.tenant_id, identity) else {
			return Ok(None);
		};
		Ok(Some(self.slab.counter_read_value(location).await?))
	}

	/// Append a durable tentative value frame **at the caller-supplied `hlc`** and
	/// index it for the live view — **no** commit marker yet (so recovery would
	/// skip it until committed). The HLC is caller-owned (the leader's per-op
	/// `commit_hlc`) so a re-driven tentative lands at its ORIGINAL timestamp and
	/// a stale reconcile cannot supersede a newer committed value. The returned
	/// handle's op-id is the frame's durable offset.
	pub async fn tentative_append(
		&self,
		key: &CounterKey,
		value: u64,
		hlc: HybridTimestamp,
	) -> std::io::Result<SlabTentative> {
		let identity = counter_identity(key);
		let location = self
			.slab
			.counter_tentative(self.tenant_id, identity, hlc, value, self.origin)
			.await?;
		Ok(SlabTentative {
			key_identity: identity,
			location,
		})
	}

	/// Promote a tentative to committed: write the durable commit marker. The
	/// value is already live-indexed (by `tentative_append`), so commit only adds
	/// the marker that makes it survive restart. On marker-write failure it
	/// surfaces `Err` **without** mutating the live index — the CALLER decides: a
	/// terminal single-shot caller rolls back (orphan skipped by recovery); a
	/// post-quorum caller leaves it for the reconciler to retry. This
	/// caller-decides contract is what makes commit re-drivable (cut-2b
	/// reconciliation) and mirrors the abstract `CounterCore` commit/rollback.
	pub async fn commit_tentative(&self, t: SlabTentative) -> std::io::Result<()> {
		#[cfg(test)]
		if self.fail_next_commit_marker.swap(false, Ordering::Relaxed) {
			return Err(std::io::Error::new(
				std::io::ErrorKind::Other,
				"injected commit-marker write failure",
			));
		}
		// The marker only pairs an op-id; reuse the tentative's own HLC so the
		// slab needs no clock of its own (HLC is caller-owned). `counter_commit_marker`
		// makes it durable.
		self.slab
			.counter_commit_marker(
				self.tenant_id,
				t.key_identity,
				t.location.hlc,
				t.op_id(),
				self.origin,
			)
			.await
	}

	/// Roll back a tentative: remove the live index entry only. No durable abort
	/// marker is written — the durable tentative frame is left an orphan that
	/// `recover_slab` skips (it has no commit marker).
	pub fn rollback_tentative(&self, t: SlabTentative) {
		self.slab
			.counter_unindex(self.tenant_id, t.key_identity, t.location);
	}

	#[cfg(test)]
	pub(crate) fn arm_commit_marker_failure(&self) {
		self.fail_next_commit_marker.store(true, Ordering::Relaxed);
	}
}

// ============================================================
//  ADR 075 cut-2 (replica protocol, DARK): the replica side
// ============================================================
//
// `CounterReplica` is the leader→replica handler logic over the proven
// [`SlabCounterStore`] substrate. A counter add's primary fans the tentative
// out (`replicate_tentative`), gathers the EXACT set of participants that acked
// success, then on quorum drives that set with `commit` (write the durable
// marker) or `rollback` (drop the live tentative). This is the
// `COUNTER_REPLICATE_REQ` / `COUNTER_ROLLBACK_REQ` handler logic; the leader
// orchestration and the public `COUNTER_ADD_REQ` dispatch are the NEXT cut, so
// nothing here is reachable from a client yet.
//
// Two properties this layer makes structural:
//   - **No witness on append.** Unlike `handle_replicate` (which calls
//     `pending_finalizations.upsert` right after the durable append), this path
//     never touches the finalization machinery — a counter tentative carries no
//     pending-finalization witness, so the reconciler can never finalize a
//     quorum-failed add. The observable proof at this layer: a replicate with no
//     subsequent commit leaves NOTHING recoverable (recovery skips the orphan),
//     which is exactly what "no witness" buys.
//   - **Commit marker only on the explicit commit step.** `replicate_tentative`
//     indexes the value live but writes no marker; only `commit` writes it.
//
// `op_id` is the leader-assigned, cross-node stable handle for one add; each
// replica maps it to its own local tentative (whose durable tentative/commit
// pairing uses that replica's local record offset).

/// The replica side of the counter-replication protocol over one
/// [`SlabCounterStore`].
pub struct CounterReplica<S> {
	store: SlabCounterStore<S>,
	/// In-flight tentatives by leader-assigned `op_id` → local tentative handle.
	/// This is the replica's live tentative index state: `replicate_tentative`
	/// inserts, `commit` / `rollback` remove. Not durable — recovery rebuilds
	/// committed state from frames and skips orphan (uncommitted) tentatives.
	pending: DashMap<u64, SlabTentative>,
	/// Per-`op_id` gate making `replicate_tentative` idempotent under concurrency.
	/// The `contains_key → append → insert` body is check-then-act: two concurrent
	/// retransmits of one `op_id` could both miss `pending` and append two live
	/// tentatives, while `commit`/`rollback` only track the last-inserted handle —
	/// leaking the first. Holding this lock across the append serializes same-op_id
	/// retransmits (distinct op_ids never contend). Removed on the terminal
	/// `commit`/`rollback` to bound the map; mirrors the leader's per-key `locks`.
	op_locks: DashMap<u64, Arc<Mutex<()>>>,
}

impl<S: CounterSlab> CounterReplica<S> {
	pub fn new(store: SlabCounterStore<S>) -> Self {
		Self {
			store,
			pending: DashMap::new(),
			op_locks: DashMap::new(),
		}
	}

	/// `COUNTER_REPLICATE_REQ` (phase `TENTATIVE`): durably append the tentative
	/// value and index it live, with **no** commit marker and **no**
	/// pending-finalization witness. Idempotent on `op_id` — a duplicate
	/// tentative is a no-op success (it must not append a second durable frame),
	/// including under concurrent retransmits (the per-op_id gate below).
	pub async fn replicate_tentative(
		&self,
		op_id: u64,
		key: &CounterKey,
		value: u64,
		commit_hlc: HybridTimestamp,
	) -> std::io::Result<()> {
		let gate = self.op_locks.entry(op_id).or_default().clone();
		let _guard = gate.lock().await;
		if self.pending.contains_key(&op_id) {
			return Ok(());
		}
		let tentative = self.store.tentative_append(key, value, commit_hlc).await?;
		self.pending.insert(op_id, tentative);
		Ok(())
	}

	/// `COUNTER_REPLICATE_REQ` (phase `COMMIT`): write the durable commit marker
	/// for a previously-replicated tentative — the ONLY path that makes this
	/// replica's value survive restart. No-op if `op_id` is unknown (already
	/// committed or rolled back). Retryable: on marker-write failure the pending
	/// entry is KEPT so the leader's reconciler can re-drive the commit.
	pub async fn commit(&self, op_id: u64) -> std::io::Result<()> {
		let tentative = match self.pending.get(&op_id) {
			Some(entry) => *entry.value(),
			None => return Ok(()),
		};
		self.store.commit_tentative(tentative).await?;
		self.pending.remove(&op_id);
		self.op_locks.remove(&op_id);
		Ok(())
	}

	/// `COUNTER_ROLLBACK_REQ`: remove the tentative's live index state. No
	/// durable abort marker — the orphan tentative frame is skipped by recovery.
	/// No-op if `op_id` is unknown.
	pub fn rollback(&self, op_id: u64) {
		if let Some((_, tentative)) = self.pending.remove(&op_id) {
			self.store.rollback_tentative(tentative);
		}
		self.op_locks.remove(&op_id);
	}

	/// Point read of the committed counter on this replica.
	pub async fn read_committed(&self, key: &CounterKey) -> std::io::Result<Option<u64>> {
		self.store.read_committed(key).await
	}

	/// Number of in-flight (replicated-but-not-yet-committed/rolled) tentatives.
	#[cfg(test)]
	fn pending_count(&self) -> usize {
		self.pending.len()
	}

	/// Arm the next `commit` on this replica to fail its durable marker write.
	#[cfg(test)]
	fn arm_commit_marker_failure(&self) {
		self.store.arm_commit_marker_failure();
	}
}

// ============================================================
//  ADR 075 cut-3b: the server-side replica (identity-based)
// ============================================================
//
// `CounterReplicaService` is the production replica the data-server's
// `COUNTER_REPLICATE_REQ` / `COUNTER_ROLLBACK_REQ` handlers drive. Unlike the
// harness `CounterReplica<S>` it is **identity-based** — the leader has already
// derived and server-verified the counter `key_identity`, so the replica never
// re-derives from a `CounterKey`. It holds the leader→local pending map and
// drives the counter-shaped `SlabManager` primitives directly. It NEVER touches
// the pending-finalization / `TenantFinalization` machinery (witness-after-quorum
// lives in the leader), so a replicated tentative carries no witness.

/// Live operation scope: allocator identity is checked by ingress, never request metadata.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct ReplicaOpKey2 {
	tenant: TenantId,
	origin_node: u32,
	op_id: u64,
}
impl ReplicaOpKey2 {
	fn new(origin: NodeId, tenant: TenantId, op_id: u64) -> std::io::Result<Self> {
		if origin.id == 0 || origin._reserved != 0 {
			return Err(std::io::Error::new(
				std::io::ErrorKind::InvalidInput,
				"counter origin has reserved bits",
			));
		}
		Ok(Self {
			tenant,
			origin_node: origin.id,
			op_id,
		})
	}
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CounterReplicaRejection {
	ZeroExpectedKey,
	TentativeConflict,
}
impl std::fmt::Display for CounterReplicaRejection {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.write_str(match self {
			Self::ZeroExpectedKey => "counter expected key is zero",
			Self::TentativeConflict => "counter tentative conflicts",
		})
	}
}
impl std::error::Error for CounterReplicaRejection {}
impl CounterReplicaRejection {
	fn into_error(self) -> std::io::Error {
		let kind = match self {
			Self::ZeroExpectedKey => std::io::ErrorKind::InvalidInput,
			Self::TentativeConflict => std::io::ErrorKind::AlreadyExists,
		};
		std::io::Error::new(kind, self)
	}
}
#[cfg(test)]
fn counter_replica_rejection2(error: &std::io::Error) -> Option<&CounterReplicaRejection> {
	error.get_ref()?.downcast_ref::<CounterReplicaRejection>()
}
fn check_expected_key2(expected_key: Identity) -> std::io::Result<()> {
	if expected_key == Identity::new([0; 32]) {
		return Err(CounterReplicaRejection::ZeroExpectedKey.into_error());
	}
	Ok(())
}
/// Pending content and its local marker offset. This is a live map, not a terminal ledger.
#[derive(Clone, Copy)]
struct PendingReplicaTentative {
	tenant_id: TenantId,
	key_identity: Identity,
	value: u64,
	location: RecordLocation,
}
/// The server-side counter replica over a [`SlabManager`].
pub struct CounterReplicaService {
	slab: Arc<crate::storage::manager::SlabManager>,
	marked: Arc<EligibleCells<(TenantId, Identity)>>,
	pending: DashMap<ReplicaOpKey2, PendingReplicaTentative>,
	op_locks: DashMap<ReplicaOpKey2, Arc<Mutex<()>>>,
}
impl CounterReplicaService {
	pub fn new(slab: Arc<crate::storage::manager::SlabManager>) -> Self {
		Self {
			marked: slab.counter_eligible_cells(),
			slab,
			pending: DashMap::new(),
			op_locks: DashMap::new(),
		}
	}
	// Called with the borrowed gate guard held and no DashMap refs retained. Under the shard
	// write lock, two Arc owners mean exactly this caller and the map, with no old waiter.
	fn reclaim_gate2(&self, key: ReplicaOpKey2, gate: &Arc<Mutex<()>>) {
		if self.pending.contains_key(&key) {
			return;
		}
		self.op_locks.remove_if(&key, |_, stored| {
			Arc::ptr_eq(stored, gate) && Arc::strong_count(gate) == 2
		});
	}
	/// Append one tentative; matching retries are idempotent only while pending exists.
	pub async fn replicate_tentative(
		&self,
		origin: NodeId,
		op_id: u64,
		tenant_id: TenantId,
		key_identity: Identity,
		value: u64,
		commit_hlc: HybridTimestamp,
	) -> std::io::Result<()> {
		let key = ReplicaOpKey2::new(origin, tenant_id, op_id)?;
		// Scope validation precedes the production-only canonical producer binding.
		if commit_hlc
			!= (HybridTimestamp {
				physical_ms: op_id,
				logical: 0,
				node_id: 0,
			}) {
			return Err(CounterReplicaRejection::TentativeConflict.into_error());
		}
		let gate = self.op_locks.entry(key).or_default().clone();
		let _guard = gate.lock().await;
		if let Some(pending) = self.pending.get(&key).map(|entry| *entry.value()) {
			return if (pending.key_identity, pending.value, pending.location.hlc)
				== (key_identity, value, commit_hlc)
			{
				Ok(())
			} else {
				Err(CounterReplicaRejection::TentativeConflict.into_error())
			};
		}
		// Reserve known-origin collision metadata only; this never publishes a value.
		let _reservation = self
			.marked
			.prepare(
				&(tenant_id, key_identity),
				CounterVersion {
					value,
					hlc: commit_hlc,
					origin: Some(origin),
				},
			)
			.map_err(CounterVisibilityError::io)?;
		let result = match self
			.slab
			.counter_append_tentative(tenant_id, key_identity, commit_hlc, value, origin)
			.await
		{
			Ok(location) => {
				self.pending.insert(
					key,
					PendingReplicaTentative {
						tenant_id,
						key_identity,
						value,
						location,
					},
				);
				Ok(())
			}
			Err(error) => Err(error),
		};
		self.reclaim_gate2(key, &gate);
		result
	}
	/// Marker failures retain pending state for re-drive. Zero refuses before missing lookup.
	pub async fn commit(
		&self,
		origin: NodeId,
		op_id: u64,
		tenant_id: TenantId,
		expected_key: Identity,
	) -> std::io::Result<()> {
		let key = ReplicaOpKey2::new(origin, tenant_id, op_id)?;
		check_expected_key2(expected_key)?;
		let gate = self.op_locks.entry(key).or_default().clone();
		let _guard = gate.lock().await;
		let Some(pending) = self.pending.get(&key).map(|entry| *entry.value()) else {
			self.reclaim_gate2(key, &gate);
			return Ok(());
		};
		if pending.tenant_id != tenant_id || pending.key_identity != expected_key {
			return Err(CounterReplicaRejection::TentativeConflict.into_error());
		}
		let reservation = self
			.marked
			.prepare(
				&(tenant_id, expected_key),
				CounterVersion {
					value: pending.value,
					hlc: pending.location.hlc,
					origin: Some(origin),
				},
			)
			.map_err(CounterVisibilityError::io)?;
		self.slab
			.counter_append_commit_marker(
				pending.tenant_id,
				pending.key_identity,
				pending.location.hlc,
				pending.location.offset,
				origin,
			)
			.await?;
		reservation.publish().map_err(CounterVisibilityError::io)?;
		self.pending.remove(&key);
		self.reclaim_gate2(key, &gate);
		Ok(())
	}
	/// Remove only this operation's matching tentative; a missing nonzero key is a no-op.
	pub async fn rollback(
		&self,
		origin: NodeId,
		op_id: u64,
		tenant_id: TenantId,
		expected_key: Identity,
	) -> std::io::Result<()> {
		let key = ReplicaOpKey2::new(origin, tenant_id, op_id)?;
		check_expected_key2(expected_key)?;
		let gate = self.op_locks.entry(key).or_default().clone();
		let _guard = gate.lock().await;
		let Some(pending) = self.pending.get(&key).map(|entry| *entry.value()) else {
			self.reclaim_gate2(key, &gate);
			return Ok(());
		};
		if pending.tenant_id != tenant_id || pending.key_identity != expected_key {
			return Err(CounterReplicaRejection::TentativeConflict.into_error());
		}
		self.pending.remove(&key);
		self.slab.counter_unindex_tentative(
			pending.tenant_id,
			pending.key_identity,
			pending.location,
		);
		self.reclaim_gate2(key, &gate);
		Ok(())
	}
	/// Only marker-eligible values; raw tentative lookup remains a distinct storage API.
	pub async fn read_committed(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
	) -> std::io::Result<Option<u64>> {
		Ok(self
			.read_eligible(tenant_id, key_identity)
			.await?
			.map(|v| v.value))
	}
	pub(crate) async fn read_eligible(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
	) -> std::io::Result<Option<CounterVersion>> {
		if !self
			.slab
			.counter_eligible_supported(tenant_id, key_identity)
			.await?
		{
			return Err(CounterVisibilityError::Unsupported.io());
		}
		self.marked
			.read(&(tenant_id, key_identity))
			.map_err(CounterVisibilityError::io)
	}
	/// Test-only deterministic remote pause; no production request or authority seam.
	#[cfg(test)]
	pub(crate) fn ctr4_pause_operation(
		&self,
		origin: NodeId,
		tenant: TenantId,
		op: u64,
	) -> Arc<Mutex<()>> {
		let key = ReplicaOpKey2::new(origin, tenant, op).expect("fixture scope");
		self.op_locks.entry(key).or_default().clone()
	}
	#[cfg(test)]
	pub(crate) fn pending_count(&self) -> usize {
		self.pending.len()
	}
}

/// The leader's own (local) participant: a [`CounterParticipant`] over the
/// in-process [`CounterReplicaService`], pinned to one tenant. The leader speaks
/// `CounterKey`; this adapter derives the domain-separated identity ([D7]) and
/// forwards to the identity-based service. The remote participant (transport) is
/// the data-server's enum variant; together they are the leader's participant set.
#[derive(Clone)]
pub struct LocalCounterParticipant {
	service: Arc<CounterReplicaService>,
	tenant_id: TenantId,
	origin: NodeId,
}

impl LocalCounterParticipant {
	pub fn new(service: Arc<CounterReplicaService>, tenant_id: TenantId, origin: NodeId) -> Self {
		Self {
			service,
			tenant_id,
			origin,
		}
	}
}

impl CounterParticipant for LocalCounterParticipant {
	async fn replicate_tentative(
		&self,
		op_id: u64,
		key: &CounterKey,
		value: u64,
		commit_hlc: HybridTimestamp,
	) -> Result<(), ParticipantError> {
		self.service
			.replicate_tentative(
				self.origin,
				op_id,
				self.tenant_id,
				counter_identity(key),
				value,
				commit_hlc,
			)
			.await
			.map_err(ParticipantError::from_io)
	}
	async fn commit(&self, op_id: u64, key: &CounterKey) -> Result<(), ParticipantError> {
		self.service
			.commit(self.origin, op_id, self.tenant_id, counter_identity(key))
			.await
			.map_err(ParticipantError::from_io)
	}
	async fn rollback(&self, op_id: u64, key: &CounterKey) {
		let _ = self
			.service
			.rollback(self.origin, op_id, self.tenant_id, counter_identity(key))
			.await;
	}
	fn eligibility_status(&self, identity: Identity) -> Result<(), CounterVisibilityError> {
		self.service
			.marked
			.read(&(self.tenant_id, identity))
			.map(|_| ())
	}
	async fn read_eligible(
		&self,
		identity: Identity,
	) -> Result<Option<CounterVersion>, ParticipantError> {
		self.service
			.read_eligible(self.tenant_id, identity)
			.await
			.map_err(ParticipantError::from_io)
	}
	async fn read_committed(&self, key: &CounterKey) -> Result<Option<u64>, ParticipantError> {
		self.service
			.read_committed(self.tenant_id, counter_identity(key))
			.await
			.map_err(ParticipantError::from_io)
	}
}

/// The wire side a [`RemoteCounterParticipant`] drives, implemented by the network
/// layer (which owns the counter replicate/rollback transport — cut-3b). Boxed
/// futures keep this trait **dyn-compatible**, so `RemoteCounterParticipant` — and
/// thus [`CounterParticipantKind`] — stays a concrete (non-generic) type while the
/// storage layer never depends on the network layer.
pub trait CounterReplicaTransport: Send + Sync {
	/// Ship one replicate (`TENTATIVE` or `COMMIT`) to `peer`. `Ok(())` ⇒ the peer
	/// acked OK (the leader counts it accepted); `Err(())` ⇒ unavailable/rejected.
	fn replicate(
		&self,
		peer: NodeId,
		tenant_id: TenantId,
		key_identity: Identity,
		op_id: u64,
		value: u64,
		commit_hlc: HybridTimestamp,
		epoch: u64,
		phase: CounterReplicaPhase,
	) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), ()>> + Send + '_>>;
	/// Ship a rollback of `op_id` to `peer` (best-effort; recovery skips the orphan).
	fn rollback(
		&self,
		peer: NodeId,
		tenant_id: TenantId,
		key_identity: Identity,
		op_id: u64,
		epoch: u64,
	) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>>;
}

/// The leader's participant for a **remote** replica: a [`CounterParticipant`] that
/// drives the replicate/commit/rollback wire to one peer via a
/// [`CounterReplicaTransport`]. Pinned to a tenant + epoch; derives the
/// domain-separated identity from the leader's `CounterKey` like the local arm.
#[derive(Clone)]
pub struct RemoteCounterParticipant {
	transport: Arc<dyn CounterReplicaTransport>,
	peer: NodeId,
	tenant_id: TenantId,
	epoch: u64,
}

impl RemoteCounterParticipant {
	pub fn new(
		transport: Arc<dyn CounterReplicaTransport>,
		peer: NodeId,
		tenant_id: TenantId,
		epoch: u64,
	) -> Self {
		Self {
			transport,
			peer,
			tenant_id,
			epoch,
		}
	}

	fn zero_hlc() -> HybridTimestamp {
		HybridTimestamp {
			physical_ms: 0,
			logical: 0,
			node_id: 0,
		}
	}
}

impl CounterParticipant for RemoteCounterParticipant {
	async fn replicate_tentative(
		&self,
		op_id: u64,
		key: &CounterKey,
		value: u64,
		commit_hlc: HybridTimestamp,
	) -> Result<(), ParticipantError> {
		self.transport
			.replicate(
				self.peer,
				self.tenant_id,
				counter_identity(key),
				op_id,
				value,
				commit_hlc,
				self.epoch,
				CounterReplicaPhase::TENTATIVE,
			)
			.await
			.map_err(|()| ParticipantError::Unavailable)
	}
	async fn commit(&self, op_id: u64, key: &CounterKey) -> Result<(), ParticipantError> {
		// The existing key field binds commit to the original counter; value/HLC stay unused.
		self.transport
			.replicate(
				self.peer,
				self.tenant_id,
				counter_identity(key),
				op_id,
				0,
				Self::zero_hlc(),
				self.epoch,
				CounterReplicaPhase::COMMIT,
			)
			.await
			.map_err(|()| ParticipantError::Unavailable)
	}
	async fn rollback(&self, op_id: u64, key: &CounterKey) {
		self.transport
			.rollback(
				self.peer,
				self.tenant_id,
				counter_identity(key),
				op_id,
				self.epoch,
			)
			.await;
	}
	fn eligibility_status(&self, _identity: Identity) -> Result<(), CounterVisibilityError> {
		Err(CounterVisibilityError::Unsupported)
	}
	async fn read_eligible(
		&self,
		_identity: Identity,
	) -> Result<Option<CounterVersion>, ParticipantError> {
		Err(ParticipantError::Unavailable)
	}
	async fn read_committed(&self, _key: &CounterKey) -> Result<Option<u64>, ParticipantError> {
		// The leader reads the committed base ONLY from its explicit local arm; a
		// remote replica is never asked for it.
		Err(ParticipantError::Unavailable)
	}
}

/// The production participant type a [`CounterLeader`] fans an add out across: the
/// closed family of participant arms — the leader's own in-process
/// [`LocalCounterParticipant`] and, for quorum > 1, one [`RemoteCounterParticipant`]
/// per peer replica. The enum keeps the leader monomorphic over one concrete `P`
/// while letting the participant set mix arms — no `dyn` on the hot
/// `CounterParticipant` path (its `impl Future` returns are not dyn-compatible).
#[derive(Clone)]
pub enum CounterParticipantKind {
	/// The leader's own in-process replica over the local [`CounterReplicaService`].
	Local(LocalCounterParticipant),
	/// A remote replica driven over the counter replicate/rollback wire.
	Remote(RemoteCounterParticipant),
}

impl CounterParticipant for CounterParticipantKind {
	async fn replicate_tentative(
		&self,
		op_id: u64,
		key: &CounterKey,
		value: u64,
		commit_hlc: HybridTimestamp,
	) -> Result<(), ParticipantError> {
		match self {
			Self::Local(p) => p.replicate_tentative(op_id, key, value, commit_hlc).await,
			Self::Remote(p) => p.replicate_tentative(op_id, key, value, commit_hlc).await,
		}
	}
	async fn commit(&self, op_id: u64, key: &CounterKey) -> Result<(), ParticipantError> {
		match self {
			Self::Local(p) => p.commit(op_id, key).await,
			Self::Remote(p) => p.commit(op_id, key).await,
		}
	}
	async fn rollback(&self, op_id: u64, key: &CounterKey) {
		match self {
			Self::Local(p) => p.rollback(op_id, key).await,
			Self::Remote(p) => p.rollback(op_id, key).await,
		}
	}
	fn eligibility_status(&self, identity: Identity) -> Result<(), CounterVisibilityError> {
		match self {
			Self::Local(p) => p.eligibility_status(identity),
			Self::Remote(p) => p.eligibility_status(identity),
		}
	}
	async fn read_eligible(
		&self,
		identity: Identity,
	) -> Result<Option<CounterVersion>, ParticipantError> {
		match self {
			Self::Local(p) => p.read_eligible(identity).await,
			Self::Remote(p) => p.read_eligible(identity).await,
		}
	}
	async fn read_committed(&self, key: &CounterKey) -> Result<Option<u64>, ParticipantError> {
		match self {
			Self::Local(p) => p.read_committed(key).await,
			Self::Remote(p) => p.read_committed(key).await,
		}
	}
}

// ============================================================
//  ADR 075 cut-2b: leader orchestration + counter finalization
// ============================================================
//
// `CounterLeader` is the primary-side orchestration over a per-add
// [`CounterParticipantPlan`]. The data-server derives that plan from the key's
// current replica set; recovery uses recorded peer identities from the durable
// witness. The five boundaries it pins:
//
//  1. **Accepted set is authoritative.** Commit and rollback iterate ONLY the
//     exact set of participants that acked tentative-append success (local
//     included iff local accepted). Never the full participant set, never a
//     "quorum set". A failed-quorum add must roll back EVERY accepted
//     participant so no failed add stays visible on any future primary.
//  2. **No quorum-no-commit window.** After quorum success the leader persists a
//     counter-finalization witness BEFORE the commit fan-out, then drives the
//     commits. Success is only returned once the witness exists (a recoverable
//     pending state that WILL finish the commits) — a value that reached quorum
//     but lacks markers is not abandoned.
//  3. **Duplicate replay before lock/mutation.** Primary-only check first (a
//     non-primary cannot replay authoritatively — it never accepted the
//     original — so it redirects, it does not replay), then the idempotency
//     lookup, then the per-key lock.
//  4. **Failure after partial commit reconciles to completion, never rolls
//     back.** Once any accepted participant has its commit marker, rollback is
//     unsafe. The witness lets the reconciler FINISH the remaining commit
//     markers. (This is why counter finalization is distinct from generic write
//     finalization: it must finish commit-marker fan-out, not just record a
//     `TenantFinalization`.)
//  5. **No double-add / no loss on reconcile.** Re-driving commit only writes
//     markers for already-appended tentatives (commit is idempotent; the value
//     is the absolute `current`, not a re-applied delta), so reconciliation
//     completes the counter without double-adding or losing it.

use std::collections::HashSet;
use std::path::PathBuf;

/// A participant step failed (I/O, epoch, transport). A participant that errors
/// on `replicate_tentative` is simply excluded from the authoritative accepted
/// set — it is never committed or rolled back.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParticipantError {
	Unavailable,
	Visibility(CounterVisibilityError),
}

impl ParticipantError {
	fn from_io(error: std::io::Error) -> Self {
		match error
			.get_ref()
			.and_then(|e| e.downcast_ref::<CounterVisibilityError>())
		{
			Some(error) => Self::Visibility(*error),
			None => Self::Unavailable,
		}
	}
	fn leader(self) -> CounterLeaderError {
		match self {
			Self::Visibility(error) => CounterLeaderError::Visibility(error),
			Self::Unavailable => CounterLeaderError::Internal,
		}
	}
}

/// One participant in a counter add's fan-out: the local primary store or a
/// remote replica. The leader drives exactly these steps.
pub trait CounterParticipant {
	/// Append+index the tentative at the leader's `commit_hlc` (no marker, no
	/// witness). `Ok` ⇒ this participant is in the accepted set. The HLC is
	/// carried so a re-driven tentative lands at its original timestamp.
	fn replicate_tentative(
		&self,
		op_id: u64,
		key: &CounterKey,
		value: u64,
		commit_hlc: HybridTimestamp,
	) -> impl std::future::Future<Output = Result<(), ParticipantError>> + Send;
	/// Write the durable commit marker for `op_id`. Idempotent.
	fn commit(
		&self,
		op_id: u64,
		key: &CounterKey,
	) -> impl std::future::Future<Output = Result<(), ParticipantError>> + Send;
	/// Drop the tentative's live index state for `op_id` (recovery skips the orphan).
	fn rollback(
		&self,
		op_id: u64,
		key: &CounterKey,
	) -> impl std::future::Future<Output = ()> + Send;
	fn eligibility_status(&self, identity: Identity) -> Result<(), CounterVisibilityError>;
	fn read_eligible(
		&self,
		identity: Identity,
	) -> impl std::future::Future<Output = Result<Option<CounterVersion>, ParticipantError>> + Send;
	/// Latest committed value (used by the leader to read the local base).
	fn read_committed(
		&self,
		key: &CounterKey,
	) -> impl std::future::Future<Output = Result<Option<u64>, ParticipantError>> + Send;
}

/// The fresh participant set for one counter add. It is derived from the
/// current ring by the data-server and passed into the leader for this single
/// operation. Durable witnesses never store positions into this vector; they
/// store the peer identities below.
pub struct CounterParticipantPlan<P> {
	participants: Vec<(NodeId, P)>,
	/// Acks required for quorum (counts local + remotes).
	quorum: usize,
}

impl<P> CounterParticipantPlan<P> {
	pub fn new(participants: Vec<(NodeId, P)>, quorum: usize) -> Self {
		Self {
			participants,
			quorum,
		}
	}

	pub fn quorum(&self) -> usize {
		self.quorum
	}

	pub fn len(&self) -> usize {
		self.participants.len()
	}

	fn get(&self, id: NodeId) -> Option<&P> {
		self.participants
			.iter()
			.find_map(|(candidate, participant)| (*candidate == id).then_some(participant))
	}

	fn iter(&self) -> impl Iterator<Item = (NodeId, &P)> {
		self.participants
			.iter()
			.map(|(id, participant)| (*id, participant))
	}
}

/// Resolves participants recorded in a durable witness. This is deliberately
/// separate from fresh ring routing: reconciliation finishes the peers named by
/// the witness, not whatever the current ring would choose for a new op.
pub trait CounterParticipantResolver<P: CounterParticipant> {
	fn resolve_recorded(&self, id: NodeId) -> Option<P>;
}

/// A resolver over a static vector, used by unit tests and same-call commit
/// completion where the accepted peers are already known.
pub struct StaticCounterParticipantResolver<P> {
	participants: Vec<(NodeId, P)>,
}

impl<P> StaticCounterParticipantResolver<P> {
	pub fn new(participants: Vec<(NodeId, P)>) -> Self {
		Self { participants }
	}
}

impl<P: CounterParticipant + Clone> CounterParticipantResolver<P>
	for StaticCounterParticipantResolver<P>
{
	fn resolve_recorded(&self, id: NodeId) -> Option<P> {
		self.participants
			.iter()
			.find_map(|(candidate, participant)| (*candidate == id).then(|| participant.clone()))
	}
}

/// Why a leader `fetch_add` was rejected. A successful return (including a
/// recoverable-pending partial commit) carries the [`CounterAddOutcome`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CounterLeaderError {
	Visibility(CounterVisibilityError),
	/// This node is not the key's primary — the caller must redirect.
	NotLeader,
	/// `delta == 0` (D5).
	ZeroDelta,
	/// `previous + delta` overflows `u64` (D5) — no mutation.
	Overflow,
	/// The tentative fan-out did not reach quorum; every accepted participant
	/// was rolled back, so no failed add remains visible (D6 no-poison).
	QuorumFailed,
	/// A participant read/step failed unrecoverably before any durable mutation.
	Internal,
}

/// The counter-finalization witness: the leader's durable record that an add
/// reached quorum and which accepted participants still owe a commit marker.
/// DISTINCT from a generic `TenantFinalization` — reconciliation finishes the
/// per-participant commit-marker fan-out it names. Holds exactly what re-drive
/// needs: the key (identity/domain), the absolute value, the leader-chosen
/// `commit_hlc` (so a re-driven tentative keeps its original ordering), the
/// exact accepted-participant set, and which of them are confirmed committed.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct CounterFinalizationWitness {
	op_id: u64,
	/// The public canonical write-key (D4). The witness is durable BEFORE the
	/// commit fan-out, so carrying the dedup identity + outcome here is what
	/// makes idempotent replay survive a crash at ANY point: pending → the
	/// witness itself replays; resolved → the applied record (written before
	/// the witness is pruned) replays. No window re-adds.
	write_key: CounterWriteKey,
	key: CounterKey,
	value: u64,
	/// The pre-add base — with `value` (the post-add absolute), the full
	/// replayable outcome.
	previous: u64,
	commit_hlc: HybridTimestamp,
	/// The exact accepted set (peer identities).
	accepted: Vec<NodeId>,
	/// Accepted participants whose commit marker is confirmed written.
	committed: HashSet<NodeId>,
}

mod witness;
use witness::StoredWitness;

/// The durable D4 dedup record a resolved witness compacts into: enough to
/// replay the original outcome for a duplicate `CounterWriteKey` — plus the
/// op's id, so recovery seeds the op-id allocator past EVERY resolved op, not
/// just the pending ones. Without it, a clean restart (no pending witnesses)
/// resets `op_seq` to 1 and the next add re-mints an already-used
/// `commit_hlc_for(op_id)`, breaking the "later ops always order after earlier
/// ones" contract the stale-reconcile protection relies on.
#[derive(Clone, Copy, serde::Serialize, serde::Deserialize)]
struct AppliedCounterAdd {
	op_id: u64,
	write_key: CounterWriteKey,
	previous: u64,
	current: u64,
}

/// Durable backing for counter-finalization witnesses: one small file per
/// pending op-id under a directory, written atomically (temp + rename),
/// rewritten as participants confirm their markers, and removed on resolve. On
/// restart `load_all` rebuilds the pending set so the reconciler can finish the
/// commit-marker fan-out. (This is the counter-specific recoverable-pending
/// state — it never routes through the generic `TenantFinalization` path.)
pub struct DurableCounterWitnessStore {
	dir: PathBuf,
}

impl DurableCounterWitnessStore {
	pub fn open(dir: impl Into<PathBuf>) -> std::io::Result<Self> {
		let dir = dir.into();
		std::fs::create_dir_all(&dir)?;
		Ok(Self { dir })
	}

	fn path_for(&self, op_id: u64) -> PathBuf {
		self.dir.join(format!("counter-witness-{op_id:016x}.pw"))
	}

	fn origin_path_for(&self, op_id: u64) -> PathBuf {
		self.dir.join(format!("counter-witness-{op_id:016x}.pw2"))
	}
	fn write(&self, witness: &StoredWitness) -> std::io::Result<()> {
		let (path, other, bytes) = match witness.origin {
			Some(_) => (
				self.origin_path_for(witness.op_id),
				self.path_for(witness.op_id),
				witness::encode(witness)?,
			),
			None => (
				self.path_for(witness.op_id),
				self.origin_path_for(witness.op_id),
				postcard::to_allocvec(&witness.body)
					.map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?,
			),
		};
		if other.try_exists()? {
			return Err(CounterVisibilityError::Conflict.io());
		}
		let tmp = self.dir.join(format!(".tmp-{:016x}", witness.op_id));
		std::fs::write(&tmp, &bytes)?;
		std::fs::rename(&tmp, path)?;
		Ok(())
	}

	fn remove(&self, op_id: u64) -> std::io::Result<()> {
		let old = self.path_for(op_id);
		let new = self.origin_path_for(op_id);
		if old.try_exists()? && new.try_exists()? {
			return Err(CounterVisibilityError::Conflict.io());
		}
		for path in [old, new] {
			match std::fs::remove_file(path) {
				Ok(()) => (),
				Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
				Err(e) => return Err(e),
			}
		}
		Ok(())
	}

	fn load_all(&self) -> std::io::Result<Vec<StoredWitness>> {
		let mut out = Vec::new();
		let mut seen = HashSet::new();
		for entry in std::fs::read_dir(&self.dir)? {
			let path = entry?.path();
			let extension = path.extension().and_then(|s| s.to_str());
			if !matches!(extension, Some("pw" | "pw2")) {
				continue;
			}
			let bytes = std::fs::read(&path)?;
			let w = if extension == Some("pw2") {
				witness::decode(&bytes)?
			} else {
				StoredWitness {
					origin: None,
					body: postcard::from_bytes::<CounterFinalizationWitness>(&bytes)
						.map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?,
				}
			};
			let expected = if w.origin.is_some() {
				self.origin_path_for(w.op_id)
			} else {
				self.path_for(w.op_id)
			};
			if path != expected || !seen.insert(w.op_id) {
				return Err(CounterVisibilityError::Conflict.io());
			}
			out.push(w);
		}
		Ok(out)
	}

	fn applied_log_path(&self) -> PathBuf {
		self.dir.join("counter-applied.log")
	}

	/// Append the durable D4 dedup record for a RESOLVED witness. Called by the
	/// commit driver BEFORE the witness file is pruned, so the dedup identity is
	/// never windowless: pending ops replay from the witness, resolved ops from
	/// this log. Length-prefixed postcard frames; a torn tail (crash mid-append)
	/// is tolerated by `load_applied` — the same op's witness is still on disk
	/// in that case (pruning happens after), so nothing is lost.
	/// Unbounded growth note: one ~44-byte frame per resolved add, never
	/// compacted — TTL-based pruning (the wire's `idempotency_ttl_ms`) is an
	/// explicit follow-on.
	fn append_applied(&self, applied: &AppliedCounterAdd) -> std::io::Result<()> {
		let bytes = postcard::to_allocvec(applied)
			.map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
		use std::io::Write as _;
		let mut file = std::fs::OpenOptions::new()
			.create(true)
			.append(true)
			.open(self.applied_log_path())?;
		file.write_all(&(bytes.len() as u32).to_le_bytes())?;
		file.write_all(&bytes)?;
		Ok(())
	}

	/// Load every applied dedup record (restart path). Stops cleanly at a torn
	/// tail frame (crash mid-append) — see `append_applied` for why that loses
	/// nothing.
	fn load_applied(&self) -> std::io::Result<Vec<AppliedCounterAdd>> {
		let bytes = match std::fs::read(self.applied_log_path()) {
			Ok(bytes) => bytes,
			Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
			Err(e) => return Err(e),
		};
		let mut out = Vec::new();
		let mut at = 0usize;
		while at + 4 <= bytes.len() {
			let len = u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap()) as usize;
			at += 4;
			if at + len > bytes.len() {
				break; // torn tail
			}
			match postcard::from_bytes::<AppliedCounterAdd>(&bytes[at..at + len]) {
				Ok(applied) => out.push(applied),
				Err(_) => break, // torn/corrupt tail
			}
			at += len;
		}
		Ok(out)
	}
}

/// The primary-side counter orchestrator. Durable state is per tenant; the
/// participant set for a fresh add is supplied per operation from the current
/// ring, while reconciliation resolves the identities recorded in each witness.
pub struct CounterLeader<P> {
	witnessed: EligibleCells<Identity>,
	local_id: NodeId,
	local: P,
	/// Primary-only gate. A non-primary redirects rather than replaying.
	is_primary: bool,
	/// Per-key D2 critical section, held across the whole fan-out.
	locks: DashMap<CounterKey, Arc<Mutex<()>>>,
	/// Typed idempotency outcomes by canonical public write-key (D4): a
	/// duplicate replays the original `previous`/`current` without re-applying.
	/// The in-memory view of the durable record (witness while pending, applied
	/// log once resolved) — `recover` rebuilds it, so replay survives restart.
	idempotency: DashMap<CounterWriteKey, CounterAddOutcome>,
	/// In-memory mirror of the pending witnesses (durable in `witness_store`).
	witnesses: DashMap<u64, StoredWitness>,
	/// Durable backing for the witnesses (survives leader restart).
	witness_store: DurableCounterWitnessStore,
	/// Leader-assigned op-id allocator (cross-node stable handles). `commit_hlc`
	/// is derived from it, so op-ids and HLCs advance together.
	op_seq: AtomicU64,
}

impl<P: CounterParticipant + Clone> CounterLeader<P> {
	pub fn new(
		local_id: NodeId,
		local: P,
		is_primary: bool,
		witness_store: DurableCounterWitnessStore,
	) -> Self {
		Self {
			witnessed: EligibleCells::default(),
			local_id,
			local,
			is_primary,
			locks: DashMap::new(),
			idempotency: DashMap::new(),
			witnesses: DashMap::new(),
			witness_store,
			op_seq: AtomicU64::new(1),
		}
	}

	/// Open a leader and LOAD its durable state (restart path): the applied D4
	/// dedup log and the pending witnesses — BOTH feed the idempotency map, so a
	/// duplicate retry after restart replays instead of re-adding (a pending
	/// witness is an add that WILL complete, so its outcome already replays).
	/// The op-id allocator is seeded past the highest op-id across BOTH the
	/// applied records and the pending witnesses — `commit_hlc_for` derives the
	/// HLC from the op-id, so reusing one would mint a duplicate HLC and break
	/// the "later ops always order after earlier ones" contract (the basis of
	/// the stale-reconcile protection and any as-of/version semantics). Run
	/// [`reconcile`](Self::reconcile) after to finish any partial commits.
	pub fn recover(
		local_id: NodeId,
		local: P,
		is_primary: bool,
		witness_store: DurableCounterWitnessStore,
	) -> std::io::Result<Self> {
		let applied = witness_store.load_applied()?;
		let pending = witness_store.load_all()?;
		let leader = Self::new(local_id, local, is_primary, witness_store);
		let mut max_op = 0;
		for record in applied {
			max_op = max_op.max(record.op_id);
			leader.idempotency.insert(
				record.write_key,
				CounterAddOutcome {
					previous: record.previous,
					current: record.current,
				},
			);
		}
		for witness in pending {
			if witness.origin.is_some_and(|origin| origin != local_id) {
				return Err(CounterVisibilityError::Conflict.io());
			}
			leader.witnessed.recover(
				&counter_identity(&witness.key),
				CounterVersion {
					value: witness.value,
					hlc: witness.commit_hlc,
					origin: witness.origin,
				},
			);
			max_op = max_op.max(witness.op_id);
			leader.idempotency.insert(
				witness.write_key,
				CounterAddOutcome {
					previous: witness.previous,
					current: witness.value,
				},
			);
			leader.witnesses.insert(witness.op_id, witness);
		}
		leader.op_seq.store(max_op + 1, Ordering::Relaxed);
		Ok(leader)
	}

	/// The leader-chosen HLC for an op, derived from its (monotonic) op-id so
	/// later ops always order after earlier ones on every participant's chain.
	fn commit_hlc_for(op_id: u64) -> HybridTimestamp {
		HybridTimestamp {
			physical_ms: op_id,
			logical: 0,
			node_id: 0,
		}
	}

	/// Atomic idempotent `+delta` on `key`, gated by the five boundaries above.
	/// Keyed by the canonical public `write_key` (D4) — concurrent duplicates
	/// serialize on the per-key lock and replay under it, and the dedup record
	/// is durable with the witness/applied log, so replay survives restart.
	pub async fn fetch_add(
		&self,
		plan: CounterParticipantPlan<P>,
		write_key: CounterWriteKey,
		key: &CounterKey,
		delta: u64,
	) -> Result<CounterAddResult, CounterLeaderError> {
		// (3) Primary-only FIRST. A non-primary has no authoritative idempotency
		// record (it never accepted the original), so replaying from it could
		// return a wrong/absent result — it must redirect to the real primary,
		// which holds the authoritative cache and replays correctly.
		if !self.is_primary {
			return Err(CounterLeaderError::NotLeader);
		}
		self.local
			.eligibility_status(counter_identity(key))
			.map_err(CounterLeaderError::Visibility)?;
		self.witnessed
			.read(&counter_identity(key))
			.map_err(CounterLeaderError::Visibility)?;
		// (3) Duplicate replay BEFORE lock/mutation (mandatory).
		if let Some(cached) = self.idempotency.get(&write_key) {
			return Ok(CounterAddResult::Replayed(*cached.value()));
		}
		if delta == 0 {
			return Err(CounterLeaderError::ZeroDelta);
		}
		// (1/2) Per-key lock across read → fan-out → quorum → commit/rollback.
		let lock = self.locks.entry(key.clone()).or_default().clone();
		let _guard = lock.lock().await;
		// Re-check under the lock: a concurrent duplicate may have committed.
		if let Some(cached) = self.idempotency.get(&write_key) {
			return Ok(CounterAddResult::Replayed(*cached.value()));
		}

		if plan.len() == 0 || plan.quorum() == 0 || plan.get(self.local_id).is_none() {
			return Err(CounterLeaderError::Internal);
		}

		let previous = self
			.read_identity(counter_identity(key))
			.await?
			.unwrap_or(0);
		let current = previous
			.checked_add(delta)
			.ok_or(CounterLeaderError::Overflow)?;

		// Fan out the tentative to ALL participants at the op's `commit_hlc`; the
		// accepted set is EXACTLY those peer identities that ack success.
		let op_id = self.op_seq.fetch_add(1, Ordering::Relaxed);
		let commit_hlc = Self::commit_hlc_for(op_id);
		let mut accepted = Vec::new();
		for (id, participant) in plan.iter() {
			if participant
				.replicate_tentative(op_id, key, current, commit_hlc)
				.await
				.is_ok()
			{
				accepted.push(id);
			}
		}

		// (1) Quorum over the EXACT accepted set.
		if accepted.len() < plan.quorum() {
			// Roll back EVERY accepted participant — not the full set, not a
			// quorum subset — so no failed add stays visible on any future primary.
			for &id in &accepted {
				if let Some(participant) = plan.get(id) {
					participant.rollback(op_id, key).await;
				}
			}
			return Err(CounterLeaderError::QuorumFailed);
		}

		// (2) Persist the counter-finalization witness DURABLY before the commit
		// fan-out — from here the add WILL complete (no quorum-no-commit window).
		// A persist failure aborts: roll the accepted set back, no value committed.
		let witness = CounterFinalizationWitness {
			op_id,
			write_key,
			key: key.clone(),
			value: current,
			previous,
			commit_hlc,
			accepted: accepted.clone(),
			committed: HashSet::new(),
		};
		let witness = StoredWitness {
			body: witness,
			origin: Some(self.local_id),
		};
		let reserved = self.witnessed.prepare(
			&counter_identity(key),
			CounterVersion {
				value: current,
				hlc: commit_hlc,
				origin: Some(self.local_id),
			},
		);
		let reservation = match reserved {
			Ok(reservation) => reservation,
			Err(error) => {
				for &id in &accepted {
					if let Some(p) = plan.get(id) {
						p.rollback(op_id, key).await;
					}
				}
				return Err(CounterLeaderError::Visibility(error));
			}
		};
		if self.witness_store.write(&witness).is_err() {
			for &id in &accepted {
				if let Some(participant) = plan.get(id) {
					participant.rollback(op_id, key).await;
				}
			}
			return Err(CounterLeaderError::Internal);
		}
		self.witnesses.insert(op_id, witness);
		// The entry is preallocated and same-key leader updates remain under the add mutex.
		// Preserve the witness on any residual poison; never roll back a witnessed operation.
		reservation
			.publish()
			.map_err(CounterLeaderError::Visibility)?;
		// (4/5) Drive commit markers across the accepted set; partial failures
		// stay pending in the durable witness for the reconciler, never rolled back.
		let resolver = StaticCounterParticipantResolver::new(
			plan.iter()
				.map(|(id, participant)| (id, participant.clone()))
				.collect(),
		);
		self.drive_commits(op_id, &resolver).await;
		self.witnessed
			.read(&counter_identity(key))
			.map_err(CounterLeaderError::Visibility)?;

		// (3/4) Record the typed idempotency outcome. The value reached quorum
		// and is either fully committed or recoverable-pending (the durable
		// witness will finish it) — never abandoned. The DURABLE dedup record
		// already exists (the witness above, compacted to the applied log by
		// `drive_commits`); this is its in-memory fast path.
		let outcome = CounterAddOutcome { previous, current };
		self.idempotency.insert(write_key, outcome);
		Ok(CounterAddResult::Applied(outcome))
	}

	/// Drive commit markers for the accepted participants that still owe one,
	/// marking each committed (and re-persisting the witness) on success.
	/// Resolves the witness — in memory AND on disk — only once EVERY accepted
	/// participant is committed. Idempotent and re-runnable (the reconciler).
	async fn drive_commits<R>(&self, op_id: u64, resolver: &R)
	where
		R: CounterParticipantResolver<P>,
	{
		let (key, value, commit_hlc, accepted) = match self.witnesses.get(&op_id) {
			Some(witness) => (
				witness.key.clone(),
				witness.value,
				witness.commit_hlc,
				witness.accepted.clone(),
			),
			None => return,
		};
		for i in accepted {
			let already = self
				.witnesses
				.get(&op_id)
				.map(|witness| witness.committed.contains(&i))
				.unwrap_or(false);
			if already {
				continue;
			}
			// Re-establish the tentative (idempotent on op_id, at the ORIGINAL
			// commit_hlc so a stale reconcile cannot supersede a newer value)
			// before committing, so reconciliation survives a participant that
			// lost its in-flight pending entry. `commit` is idempotent and the
			// value is the absolute `current`, so re-driving never double-adds.
			let Some(participant) = resolver.resolve_recorded(i) else {
				continue;
			};
			let outcome = match participant
				.replicate_tentative(op_id, &key, value, commit_hlc)
				.await
			{
				Ok(()) => participant.commit(op_id, &key).await,
				Err(error) => Err(error),
			};
			if matches!(outcome, Err(ParticipantError::Visibility(_))) {
				self.witnessed
					.refuse(&counter_identity(&key), CounterVisibilityError::Poisoned);
				return;
			}
			if outcome.is_ok() {
				if let Some(mut witness) = self.witnesses.get_mut(&op_id) {
					witness.committed.insert(i);
					let _ = self.witness_store.write(&witness);
				}
			}
		}
		let resolved = self.witnesses.get(&op_id).and_then(|witness| {
			let complete = witness
				.accepted
				.iter()
				.all(|i| witness.committed.contains(i));
			complete.then(|| AppliedCounterAdd {
				op_id: witness.op_id,
				write_key: witness.write_key,
				previous: witness.previous,
				current: witness.value,
			})
		});
		if let Some(applied) = resolved {
			// Compact the resolved witness into the durable applied log BEFORE
			// pruning it, so the D4 dedup record is never windowless: a crash
			// here leaves witness AND log entry (recover dedups by write-key);
			// a crash before leaves the witness. An append failure keeps the
			// witness pending — the reconciler retries the whole resolution.
			if self.witness_store.append_applied(&applied).is_err() {
				return;
			}
			// Remove only after every accepted participant has a commit marker.
			self.witnesses.remove(&op_id);
			let _ = self.witness_store.remove(op_id);
		}
	}

	/// The counter-specific reconciler: finish commit-marker fan-out for every
	/// pending witness. Run on recovery (and as a retry). DISTINCT from generic
	/// write finalization — it completes per-participant commit markers, it does
	/// not merely record a marker. Idempotent: already-committed participants are
	/// skipped and `commit` is itself idempotent, so no double-add.
	pub async fn reconcile<R>(&self, resolver: &R)
	where
		R: CounterParticipantResolver<P>,
	{
		let pending: Vec<u64> = self.witnesses.iter().map(|entry| *entry.key()).collect();
		for op_id in pending {
			self.drive_commits(op_id, resolver).await;
		}
	}

	/// Point read of the committed counter from the local primary store.
	pub async fn read(&self, key: &CounterKey) -> Result<Option<u64>, CounterLeaderError> {
		self.read_identity(counter_identity(key)).await
	}

	/// The caller selects current-key ownership before exposing this leader view publicly.
	pub async fn read_identity(
		&self,
		identity: Identity,
	) -> Result<Option<u64>, CounterLeaderError> {
		let marked = self
			.local
			.read_eligible(identity)
			.await
			.map_err(ParticipantError::leader)?;
		let witnessed = self
			.witnessed
			.read(&identity)
			.map_err(CounterLeaderError::Visibility)?;
		match winner(marked, witnessed) {
			Ok(value) => Ok(value.map(|v| v.value)),
			Err(error) => {
				self.witnessed.refuse(&identity, error);
				Err(CounterLeaderError::Visibility(error))
			}
		}
	}

	/// Number of unresolved counter-finalization witnesses (pending commits).
	#[cfg(test)]
	fn pending_witnesses(&self) -> usize {
		self.witnesses.len()
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::collections::HashMap;
	use std::sync::Mutex as StdMutex;

	/// A committed record at a physical key — proves counter/blob domain separation.
	enum Record {
		Counter(u64),
		Blob,
	}

	#[derive(Default)]
	struct State {
		/// Live committed index (counters + blobs).
		committed: HashMap<CounterKey, Record>,
		/// In-flight tentative versions: id -> (key, value). NOT visible to
		/// `read_committed` and carrying no witness (witness-after-quorum).
		tentative: HashMap<u64, (CounterKey, u64)>,
		/// The DURABLE counter frame log — what survives a restart. `tentative_append`
		/// records a `Tentative` frame; `commit_tentative` records a `Commit` marker.
		frames: Vec<CounterFrame>,
		/// Emitted pending-finalization witnesses (modelled). A tentative append emits
		/// none; a commit emits one. Used to prove witness-after-quorum.
		witnesses: u64,
		next_id: u64,
	}

	#[derive(Default)]
	struct InMemoryBackend {
		state: StdMutex<State>,
		/// If set, `read_committed` yields, widening the read→append window so an
		/// un-serialized core would deterministically lose increments.
		yield_in_read: bool,
		/// If set, the durable commit-marker write fails (the value never commits).
		fail_commit: bool,
	}

	impl InMemoryBackend {
		fn with_yield() -> Self {
			Self {
				yield_in_read: true,
				..Default::default()
			}
		}
		fn with_failing_commit() -> Self {
			Self {
				fail_commit: true,
				..Default::default()
			}
		}
		fn blob_write(&self, key: &CounterKey) -> Result<(), CounterAddError> {
			let mut s = self.state.lock().unwrap();
			if matches!(s.committed.get(key), Some(Record::Counter(_))) {
				return Err(CounterAddError::BlobKeyConflict);
			}
			s.committed.insert(key.clone(), Record::Blob);
			Ok(())
		}
		fn witness_count(&self) -> u64 {
			self.state.lock().unwrap().witnesses
		}
		fn tentative_count(&self) -> usize {
			self.state.lock().unwrap().tentative.len()
		}
		/// Simulate a process restart: rebuild the committed COUNTER index from the
		/// durable frame log via the production recovery rule, dropping in-flight
		/// tentatives. Blob records (recovered separately in the real engine) are kept.
		fn simulate_restart(&self) {
			let mut s = self.state.lock().unwrap();
			let recovered = recover_counter_index(&s.frames);
			s.committed.retain(|_, r| matches!(r, Record::Blob));
			for (key, value) in recovered {
				s.committed.insert(key, Record::Counter(value));
			}
			s.tentative.clear();
		}
	}

	impl CounterBackend for InMemoryBackend {
		async fn read_committed(&self, key: &CounterKey) -> Result<Option<u64>, CounterAddError> {
			let snapshot = {
				let s = self.state.lock().unwrap();
				match s.committed.get(key) {
					None => Ok(None),
					Some(Record::Counter(v)) => Ok(Some(*v)),
					Some(Record::Blob) => Err(CounterAddError::BlobKeyConflict),
				}
			};
			if self.yield_in_read {
				tokio::task::yield_now().await;
			}
			snapshot
		}
		async fn tentative_append(&self, key: &CounterKey, value: u64) -> TentativeId {
			let mut s = self.state.lock().unwrap();
			let id = s.next_id;
			s.next_id += 1;
			s.tentative.insert(id, (key.clone(), value));
			// Durable tentative frame, carrying the op id. NO witness, NO commit marker.
			s.frames.push(CounterFrame::Tentative {
				op_id: CounterOpId(id),
				key: key.clone(),
				value,
			});
			TentativeId(id)
		}
		async fn commit_tentative(&self, _key: &CounterKey, id: TentativeId) -> Result<(), ()> {
			let mut s = self.state.lock().unwrap();
			if self.fail_commit {
				// Durable commit-marker write fails: no Commit frame, value not committed.
				// The tentative frame stays as an orphan (recovery will skip it).
				return Err(());
			}
			if let Some((key, value)) = s.tentative.remove(&id.0) {
				// Durable commit marker BEFORE treating the value as committed for reads.
				s.frames.push(CounterFrame::Commit {
					op_id: CounterOpId(id.0),
				});
				s.committed.insert(key, Record::Counter(value));
				s.witnesses += 1; // the witness appears only on commit.
			}
			Ok(())
		}
		async fn rollback_tentative(&self, _key: &CounterKey, id: TentativeId) {
			// Live-only un-index; the durable tentative frame is left as an orphan with no
			// commit marker, which recovery skips. No durable abort marker needed.
			self.state.lock().unwrap().tentative.remove(&id.0);
		}
	}

	fn key(b: &[u8]) -> CounterKey {
		CounterKey(b.to_vec())
	}
	async fn reached(_v: u64) -> QuorumOutcome {
		QuorumOutcome::Reached
	}
	async fn failed(_v: u64) -> QuorumOutcome {
		QuorumOutcome::Failed
	}

	#[tokio::test]
	async fn commit_path_creates_from_zero_and_emits_witness_only_on_commit() {
		let core = CounterCore::new(InMemoryBackend::default());
		let k = key(b"c");
		assert_eq!(core.backend.witness_count(), 0);
		let out = core.fetch_add_with_quorum(&k, 5, reached).await.unwrap();
		assert_eq!(
			out,
			CounterAddOutcome {
				previous: 0,
				current: 5
			}
		);
		assert_eq!(core.read(&k).await.unwrap(), Some(5));
		assert_eq!(core.backend.witness_count(), 1, "witness appears on commit");
		assert_eq!(
			core.backend.tentative_count(),
			0,
			"no tentative left behind"
		);
	}

	#[tokio::test]
	async fn quorum_failure_rolls_back_and_does_not_poison_next_previous() {
		let core = CounterCore::new(InMemoryBackend::default());
		let k = key(b"c");
		// A first add that fails quorum: rolled back, no committed value, no witness.
		assert_eq!(
			core.fetch_add_with_quorum(&k, 7, failed).await,
			Err(CounterAddError::QuorumFailed)
		);
		assert_eq!(
			core.read(&k).await.unwrap(),
			None,
			"failed add left nothing"
		);
		assert_eq!(
			core.backend.witness_count(),
			0,
			"no witness for a failed add"
		);
		assert_eq!(core.backend.tentative_count(), 0, "tentative rolled back");
		// The next add must base at 0 (the failed add's 7 is not visible / not counted).
		let out = core.fetch_add_with_quorum(&k, 1, reached).await.unwrap();
		assert_eq!(
			out,
			CounterAddOutcome {
				previous: 0,
				current: 1
			},
			"next previous is not poisoned by the failed add"
		);
	}

	#[tokio::test]
	async fn zero_delta_and_overflow_reject_before_any_append() {
		let core = CounterCore::new(InMemoryBackend::default());
		let k = key(b"c");
		core.fetch_add_with_quorum(&k, u64::MAX, reached)
			.await
			.unwrap();
		assert_eq!(
			core.fetch_add_with_quorum(&k, 1, reached).await,
			Err(CounterAddError::Overflow)
		);
		assert_eq!(
			core.fetch_add_with_quorum(&k, 0, reached).await,
			Err(CounterAddError::ZeroDelta)
		);
		assert_eq!(core.read(&k).await.unwrap(), Some(u64::MAX), "no mutation");
		// Overflow/zero never reached the tentative phase → no extra witnesses.
		assert_eq!(core.backend.witness_count(), 1);
	}

	#[tokio::test]
	async fn counter_add_to_blob_key_is_rejected() {
		let backend = InMemoryBackend::default();
		let k = key(b"b");
		backend.blob_write(&k).unwrap();
		let core = CounterCore::new(backend);
		assert_eq!(
			core.fetch_add_with_quorum(&k, 1, reached).await,
			Err(CounterAddError::BlobKeyConflict)
		);
	}

	// THE non-negotiable: a rolled-back tentative must NOT resurrect on restart. The
	// durable tentative frame stays an orphan (no commit marker), so the recovery rule
	// skips it. A prior committed value must survive.
	#[tokio::test]
	async fn rollback_tentative_survives_restart() {
		let core = CounterCore::new(InMemoryBackend::default());
		let k = key(b"c");
		// Establish a prior committed value of 3.
		core.fetch_add_with_quorum(&k, 3, reached).await.unwrap();
		// A second add fails quorum → rolled back (orphan tentative frame remains durable).
		assert_eq!(
			core.fetch_add_with_quorum(&k, 99, failed).await,
			Err(CounterAddError::QuorumFailed)
		);
		assert_eq!(
			core.read(&k).await.unwrap(),
			Some(3),
			"live: rollback did not commit"
		);
		// Restart: rebuild from the durable frame log via the recovery rule.
		core.backend.simulate_restart();
		assert_eq!(
			core.read(&k).await.unwrap(),
			Some(3),
			"the rolled-back tentative did not resurrect; the prior committed value survived"
		);
	}

	// The positive twin: a legitimately committed add MUST survive restart — recovery
	// must not skip a committed counter value.
	#[tokio::test]
	async fn committed_tentative_survives_restart() {
		let core = CounterCore::new(InMemoryBackend::default());
		let k = key(b"c");
		core.fetch_add_with_quorum(&k, 5, reached).await.unwrap();
		core.fetch_add_with_quorum(&k, 4, reached).await.unwrap(); // committed -> 9
		core.backend.simulate_restart();
		assert_eq!(
			core.read(&k).await.unwrap(),
			Some(9),
			"the committed counter value survived restart"
		);
	}

	// Commit-marker write failure: the live index rolls back, the value does not commit,
	// and the orphan tentative is skipped on restart.
	#[tokio::test]
	async fn commit_marker_failure_rolls_back_and_orphan_skipped_on_restart() {
		let core = CounterCore::new(InMemoryBackend::with_failing_commit());
		let k = key(b"c");
		assert_eq!(
			core.fetch_add_with_quorum(&k, 5, reached).await,
			Err(CounterAddError::CommitMarkerFailed)
		);
		assert_eq!(core.read(&k).await.unwrap(), None, "live: not committed");
		assert_eq!(
			core.backend.witness_count(),
			0,
			"no witness on a failed commit"
		);
		core.backend.simulate_restart();
		assert_eq!(
			core.read(&k).await.unwrap(),
			None,
			"the orphan tentative did not resurrect"
		);
	}

	#[tokio::test]
	async fn blob_write_to_counter_key_is_rejected() {
		let core = CounterCore::new(InMemoryBackend::default());
		let k = key(b"c");
		core.fetch_add_with_quorum(&k, 1, reached).await.unwrap();
		assert_eq!(
			core.backend.blob_write(&k),
			Err(CounterAddError::BlobKeyConflict)
		);
		assert_eq!(core.read(&k).await.unwrap(), Some(1));
	}

	// D2 headline / racing proof: N adders, true parallelism, a yield in the backend
	// read, all committing on quorum. The per-key lock serializes the whole
	// read→tentative→commit so every adder observes a unique `previous` and the sum is
	// exact. Revert-verify: dropping the held lock loses increments.
	#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
	async fn racing_adders_do_not_share_previous_and_sum_exactly() {
		const N: u64 = 64;
		let core = Arc::new(CounterCore::new(InMemoryBackend::with_yield()));
		let k = key(b"c");
		let mut handles = Vec::new();
		for _ in 0..N {
			let core = core.clone();
			let k = k.clone();
			handles.push(tokio::spawn(async move {
				core.fetch_add_with_quorum(&k, 1, reached).await.unwrap()
			}));
		}
		let mut previouses = Vec::new();
		for h in handles {
			previouses.push(h.await.unwrap().previous);
		}
		previouses.sort_unstable();
		let expected: Vec<u64> = (0..N).collect();
		assert_eq!(
			previouses, expected,
			"every adder observed a unique previous"
		);
		assert_eq!(
			core.read(&k).await.unwrap(),
			Some(N),
			"sum is exact (no lost increments)"
		);
	}
}

#[cfg(test)]
mod slab_store_tests {
	//! ADR 075 cut-2 substrate proofs: the REAL `SlabCounterStore` over a real
	//! `LocalSlabStore`, exercising the production `recover_slab` rule across an
	//! actual close/reopen. `COUNTER_ADD_REQ` dispatch is not exercised here.

	use super::*;
	use crate::schema::{HybridTimestamp, SlabId, SlabRecordHeader, TenantId};

	/// An HLC keyed off the value, so a test's later (larger) append supersedes
	/// the earlier one on the version chain.
	fn h(ms: u64) -> HybridTimestamp {
		HybridTimestamp {
			physical_ms: ms,
			logical: 0,
			node_id: 0,
		}
	}

	use crate::storage::core::{GroupCommitConfig, LocalSlabStore};
	use crate::storage::{BlockingFileOps, StorageEngine};
	use std::io::{Read, Seek, SeekFrom};
	use std::path::{Path, PathBuf};
	use std::time::Duration;

	fn slab_dir() -> PathBuf {
		let dir = std::env::temp_dir().join(format!(
			"wendelon-adr075-{}-{}",
			std::process::id(),
			uuid::Uuid::new_v4()
		));
		std::fs::create_dir_all(&dir).unwrap();
		dir
	}

	async fn open_store(path: &Path) -> Arc<LocalSlabStore> {
		Arc::new(
			LocalSlabStore::open(
				path,
				SlabId::ZERO,
				Arc::new(BlockingFileOps),
				GroupCommitConfig {
					floor: Duration::ZERO,
					ceiling: Duration::ZERO,
					per_write_cost_estimate: Duration::ZERO,
				},
			)
			.await
			.unwrap(),
		)
	}

	fn tenant() -> TenantId {
		TenantId::new(7)
	}

	fn ckey(b: &[u8]) -> CounterKey {
		CounterKey(b.to_vec())
	}

	// Gate: a committed counter value survives restart through the REAL recovery
	// path (committed tentatives are re-indexed; latest-by-HLC is the value).
	#[tokio::test]
	async fn committed_counter_value_survives_restart_through_real_recovery() {
		let dir = slab_dir();
		let path = dir.join("slab_0000000000000000.wnd");
		let k = ckey(b"evidence");
		{
			let store = open_store(&path).await;
			let counter = SlabCounterStore::new(
				store.clone(),
				tenant(),
				NodeId {
					id: 1,
					_reserved: 0,
				},
			);
			let t1 = counter.tentative_append(&k, 5, h(5)).await.unwrap();
			counter.commit_tentative(t1).await.unwrap();
			let t2 = counter.tentative_append(&k, 9, h(9)).await.unwrap(); // +4 -> 9
			counter.commit_tentative(t2).await.unwrap();
			assert_eq!(counter.read_committed(&k).await.unwrap(), Some(9));
			store.fsync().await.unwrap();
		}
		// Reopen the same on-disk slab: the production recover_slab rebuilds the
		// committed counter index.
		let store = open_store(&path).await;
		let counter = SlabCounterStore::new(
			store,
			tenant(),
			NodeId {
				id: 1,
				_reserved: 0,
			},
		);
		assert_eq!(
			counter.read_committed(&k).await.unwrap(),
			Some(9),
			"committed counter value survived restart via real recovery"
		);
	}

	// Gate: a rolled-back tentative is removed from the live index immediately
	// AND does not resurrect through the REAL recovery path (orphan, no marker).
	#[tokio::test]
	async fn rolled_back_tentative_does_not_resurrect_through_real_recovery() {
		let dir = slab_dir();
		let path = dir.join("slab_0000000000000000.wnd");
		let k = ckey(b"evidence");
		{
			let store = open_store(&path).await;
			let counter = SlabCounterStore::new(
				store.clone(),
				tenant(),
				NodeId {
					id: 1,
					_reserved: 0,
				},
			);
			let t1 = counter.tentative_append(&k, 3, h(3)).await.unwrap();
			counter.commit_tentative(t1).await.unwrap();
			assert_eq!(counter.read_committed(&k).await.unwrap(), Some(3));

			// A second tentative is indexed live...
			let t2 = counter.tentative_append(&k, 99, h(99)).await.unwrap();
			assert_eq!(
				counter.read_committed(&k).await.unwrap(),
				Some(99),
				"tentative is visible in the live index"
			);
			// ...then rolled back: live index state removed only, committed 3 intact.
			counter.rollback_tentative(t2);
			assert_eq!(
				counter.read_committed(&k).await.unwrap(),
				Some(3),
				"live rollback removed the tentative index entry, not the committed value"
			);
			store.fsync().await.unwrap();
		}
		let store = open_store(&path).await;
		let counter = SlabCounterStore::new(
			store,
			tenant(),
			NodeId {
				id: 1,
				_reserved: 0,
			},
		);
		assert_eq!(
			counter.read_committed(&k).await.unwrap(),
			Some(3),
			"the orphan tentative (99) did not resurrect; the committed value (3) survived"
		);
	}

	// Gate: a commit-marker write failure rolls back live state AND recovery
	// skips the orphan — exactly the durable/live state of a real failed marker.
	#[tokio::test]
	async fn commit_marker_failure_rolls_back_live_and_orphan_skipped_on_real_recovery() {
		let dir = slab_dir();
		let path = dir.join("slab_0000000000000000.wnd");
		let k = ckey(b"evidence");
		{
			let store = open_store(&path).await;
			let counter = SlabCounterStore::new(
				store.clone(),
				tenant(),
				NodeId {
					id: 1,
					_reserved: 0,
				},
			);
			let t1 = counter.tentative_append(&k, 3, h(3)).await.unwrap();
			counter.commit_tentative(t1).await.unwrap();

			let t2 = counter.tentative_append(&k, 50, h(50)).await.unwrap();
			assert_eq!(counter.read_committed(&k).await.unwrap(), Some(50));
			counter.arm_commit_marker_failure();
			assert!(
				counter.commit_tentative(t2).await.is_err(),
				"marker write failure surfaces as Err"
			);
			// Commit is caller-decides: a terminal single-shot caller aborts by
			// rolling the tentative back (no marker was written, so recovery skips
			// the orphan). (A post-quorum caller would instead leave it for the
			// reconciler — see the leader's drive_commits.)
			counter.rollback_tentative(t2);
			assert_eq!(
				counter.read_committed(&k).await.unwrap(),
				Some(3),
				"caller rolled the marker-failed tentative back"
			);
			store.fsync().await.unwrap();
		}
		let store = open_store(&path).await;
		let counter = SlabCounterStore::new(
			store,
			tenant(),
			NodeId {
				id: 1,
				_reserved: 0,
			},
		);
		assert_eq!(
			counter.read_committed(&k).await.unwrap(),
			Some(3),
			"the marker-failed orphan tentative (50) did not resurrect"
		);
	}

	// Gate: ordinary blob recovery stays byte-identical even when counter frames
	// share the slab — the blob round-trips and its on-disk record_class is 0.
	#[tokio::test]
	async fn ordinary_blob_recovery_stays_byte_identical() {
		let dir = slab_dir();
		let path = dir.join("slab_0000000000000000.wnd");
		let blob_id = Identity::new([0xAB; 32]);
		let blob_bytes = b"ordinary-blob-value".to_vec();
		let k = ckey(b"evidence");
		let blob_offset;
		{
			let store = open_store(&path).await;
			// A blob write (STANDARD record) and a committed counter in one slab.
			let loc = store
				.append(
					tenant(),
					blob_id,
					HybridTimestamp {
						physical_ms: 1_000,
						logical: 0,
						node_id: 0,
					},
					&blob_bytes,
				)
				.await
				.unwrap();
			blob_offset = loc.offset;
			let counter = SlabCounterStore::new(
				store.clone(),
				tenant(),
				NodeId {
					id: 1,
					_reserved: 0,
				},
			);
			let t = counter.tentative_append(&k, 42, h(42)).await.unwrap();
			counter.commit_tentative(t).await.unwrap();
			store.fsync().await.unwrap();

			// The blob's on-disk header carries record_class == STANDARD (0): the
			// byte-for-byte same slot the pre-075 `_pad_align` zero-fill held.
			let mut file = std::fs::File::open(&path).unwrap();
			file.seek(SeekFrom::Start(blob_offset)).unwrap();
			let mut header_bytes = vec![0_u8; std::mem::size_of::<SlabRecordHeader>()];
			file.read_exact(&mut header_bytes).unwrap();
			let header = bytemuck::pod_read_unaligned::<SlabRecordHeader>(&header_bytes);
			assert_eq!(header.record_class, record_class::STANDARD);
			assert_eq!(header.magic, 0x534C4142);
		}
		// Reopen: the blob recovers and reads back byte-identical; the counter
		// recovers too (counter frames in the slab do not perturb blob recovery).
		let store = open_store(&path).await;
		let recovered = store
			.lookup(tenant(), blob_id)
			.expect("blob must be re-indexed");
		assert_eq!(store.read(recovered).await.unwrap(), blob_bytes);
		let counter = SlabCounterStore::new(
			store,
			tenant(),
			NodeId {
				id: 1,
				_reserved: 0,
			},
		);
		assert_eq!(counter.read_committed(&k).await.unwrap(), Some(42));
	}
}

#[cfg(test)]
mod replica_tests {
	//! ADR 075 cut-2 (replica protocol, DARK): the `CounterReplica` handlers
	//! exercised directly over a real `LocalSlabStore`, across a real
	//! close/reopen. No leader, no `COUNTER_ADD_REQ` dispatch.

	use super::*;
	use crate::schema::{HybridTimestamp, SlabId, TenantId};
	use crate::storage::BlockingFileOps;
	use crate::storage::core::{GroupCommitConfig, LocalSlabStore};
	use std::path::{Path, PathBuf};
	use std::time::Duration;

	fn h(ms: u64) -> HybridTimestamp {
		HybridTimestamp {
			physical_ms: ms,
			logical: 0,
			node_id: 0,
		}
	}

	fn slab_dir() -> PathBuf {
		let dir = std::env::temp_dir().join(format!(
			"wendelon-adr075-replica-{}-{}",
			std::process::id(),
			uuid::Uuid::new_v4()
		));
		std::fs::create_dir_all(&dir).unwrap();
		dir
	}

	async fn open_store(path: &Path) -> Arc<LocalSlabStore> {
		Arc::new(
			LocalSlabStore::open(
				path,
				SlabId::ZERO,
				Arc::new(BlockingFileOps),
				GroupCommitConfig {
					floor: Duration::ZERO,
					ceiling: Duration::ZERO,
					per_write_cost_estimate: Duration::ZERO,
				},
			)
			.await
			.unwrap(),
		)
	}

	fn replica(store: Arc<LocalSlabStore>) -> CounterReplica<LocalSlabStore> {
		CounterReplica::new(SlabCounterStore::new(
			store,
			TenantId::new(7),
			NodeId {
				id: 1,
				_reserved: 0,
			},
		))
	}

	fn ckey(b: &[u8]) -> CounterKey {
		CounterKey(b.to_vec())
	}

	/// A `CounterSlab` that counts durable tentative appends and yields inside
	/// `counter_tentative` to widen the replicate race window. The `op_id`
	/// idempotency contract is "no second durable frame", but the last-wins live
	/// index masks a double-append from value reads — so the append count is the
	/// only direct observable that discriminates the gated path from the racy one.
	struct CountingSlab {
		inner: Arc<LocalSlabStore>,
		tentative_appends: std::sync::atomic::AtomicUsize,
	}

	impl CountingSlab {
		fn new(inner: Arc<LocalSlabStore>) -> Self {
			Self {
				inner,
				tentative_appends: std::sync::atomic::AtomicUsize::new(0),
			}
		}
		fn append_count(&self) -> usize {
			self.tentative_appends.load(Ordering::Relaxed)
		}
	}

	impl CounterSlab for CountingSlab {
		async fn counter_tentative(
			&self,
			tenant_id: TenantId,
			key_identity: Identity,
			hlc: HybridTimestamp,
			value: u64,
			origin: NodeId,
		) -> std::io::Result<RecordLocation> {
			// Yield so a concurrent racer reaches the `pending` check before this
			// append completes — without the per-op_id gate both would append.
			tokio::task::yield_now().await;
			self.tentative_appends.fetch_add(1, Ordering::Relaxed);
			self.inner
				.counter_tentative(tenant_id, key_identity, hlc, value, origin)
				.await
		}
		async fn counter_commit_marker(
			&self,
			tenant_id: TenantId,
			key_identity: Identity,
			hlc: HybridTimestamp,
			tentative_op_id: u64,
			origin: NodeId,
		) -> std::io::Result<()> {
			self.inner
				.counter_commit_marker(tenant_id, key_identity, hlc, tentative_op_id, origin)
				.await
		}
		fn counter_unindex(
			&self,
			tenant_id: TenantId,
			key_identity: Identity,
			location: RecordLocation,
		) {
			self.inner
				.counter_unindex(tenant_id, key_identity, location)
		}
		fn counter_lookup(
			&self,
			tenant_id: TenantId,
			key_identity: Identity,
		) -> Option<RecordLocation> {
			self.inner.counter_lookup(tenant_id, key_identity)
		}
		async fn counter_read_value(&self, location: RecordLocation) -> std::io::Result<u64> {
			self.inner.counter_read_value(location).await
		}
	}

	// Proof: two CONCURRENT retransmits of one `op_id` append EXACTLY ONE durable
	// tentative frame. The body is check (`pending`) → append → insert; without the
	// per-op_id gate both racers miss `pending` and append twice, leaking the first
	// frame (the live index is last-wins, so value reads hide it — the append count
	// is the discriminator). With the gate the second racer observes `pending` and
	// no-ops. (The sequential dedup is already covered above; this locks the
	// concurrent case the gate exists for.)
	#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
	async fn concurrent_duplicate_replicate_appends_one_tentative() {
		let dir = slab_dir();
		let path = dir.join("slab_0000000000000000.wnd");
		let k = ckey(b"dup");
		let store = open_store(&path).await;
		let counting = Arc::new(CountingSlab::new(store));
		let r = Arc::new(CounterReplica::new(SlabCounterStore::new(
			counting.clone(),
			TenantId::new(7),
			NodeId {
				id: 1,
				_reserved: 0,
			},
		)));

		let (ra, rb) = (r.clone(), r.clone());
		let (ka, kb) = (k.clone(), k.clone());
		let a = tokio::spawn(async move { ra.replicate_tentative(1, &ka, 5, h(5)).await });
		let b = tokio::spawn(async move { rb.replicate_tentative(1, &kb, 5, h(5)).await });
		a.await.unwrap().unwrap();
		b.await.unwrap().unwrap();

		assert_eq!(
			counting.append_count(),
			1,
			"concurrent same-op_id retransmits must append exactly one durable tentative"
		);
		assert_eq!(r.pending_count(), 1);
		assert_eq!(r.read_committed(&k).await.unwrap(), Some(5));
	}

	// Proof 1: replicate appends a tentative (live-indexed) and writes NO commit
	// marker / NO witness — so without the explicit commit step, NOTHING is
	// recoverable. The "reopen -> None" is exactly what no-witness buys: a
	// witnessed write would be finalized by the reconciler; a tentative is not.
	#[tokio::test]
	async fn replicate_appends_tentative_and_writes_no_marker_or_witness() {
		let dir = slab_dir();
		let path = dir.join("slab_0000000000000000.wnd");
		let k = ckey(b"evidence");
		{
			let store = open_store(&path).await;
			let r = replica(store.clone());
			r.replicate_tentative(1, &k, 5, h(5)).await.unwrap();
			assert_eq!(
				r.read_committed(&k).await.unwrap(),
				Some(5),
				"tentative is live-indexed on the replica"
			);
			assert_eq!(r.pending_count(), 1);
			// Duplicate replicate of the same op_id is a no-op (no 2nd frame).
			r.replicate_tentative(1, &k, 5, h(5)).await.unwrap();
			assert_eq!(r.pending_count(), 1);
			store.fsync().await.unwrap();
		}
		// No commit step ran: recovery finds an orphan tentative and skips it.
		let store = open_store(&path).await;
		let r = replica(store);
		assert_eq!(
			r.read_committed(&k).await.unwrap(),
			None,
			"a replicated-but-uncommitted tentative leaves nothing recoverable (no witness)"
		);
	}

	// Proof 2: rollback removes the tentative's live index state.
	#[tokio::test]
	async fn rollback_removes_tentative_live_index_state() {
		let dir = slab_dir();
		let path = dir.join("slab_0000000000000000.wnd");
		let k = ckey(b"evidence");
		let store = open_store(&path).await;
		let r = replica(store);
		r.replicate_tentative(1, &k, 5, h(5)).await.unwrap();
		assert_eq!(r.read_committed(&k).await.unwrap(), Some(5));
		r.rollback(1);
		assert_eq!(
			r.read_committed(&k).await.unwrap(),
			None,
			"rollback removed the live tentative index entry"
		);
		assert_eq!(r.pending_count(), 0);
	}

	// Proof 3: a rollback is harmless across restart — recovery skips the orphan
	// tentative, and a prior committed value survives.
	#[tokio::test]
	async fn rollback_after_restart_is_harmless_recovery_skips_orphan() {
		let dir = slab_dir();
		let path = dir.join("slab_0000000000000000.wnd");
		let k = ckey(b"evidence");
		{
			let store = open_store(&path).await;
			let r = replica(store.clone());
			r.replicate_tentative(1, &k, 3, h(3)).await.unwrap();
			r.commit(1).await.unwrap(); // committed -> 3
			r.replicate_tentative(2, &k, 99, h(99)).await.unwrap();
			assert_eq!(r.read_committed(&k).await.unwrap(), Some(99));
			r.rollback(2);
			assert_eq!(r.read_committed(&k).await.unwrap(), Some(3));
			store.fsync().await.unwrap();
		}
		let store = open_store(&path).await;
		let r = replica(store);
		assert_eq!(
			r.read_committed(&k).await.unwrap(),
			Some(3),
			"the rolled-back tentative (99) did not resurrect; the committed value (3) survived"
		);
	}

	// Proof 4: the commit marker is written ONLY by the explicit commit step,
	// and that marker is what makes the replica's value survive restart.
	#[tokio::test]
	async fn commit_marker_written_only_on_explicit_commit_and_survives_restart() {
		let dir = slab_dir();
		let path = dir.join("slab_0000000000000000.wnd");
		let k = ckey(b"evidence");
		{
			let store = open_store(&path).await;
			let r = replica(store.clone());
			r.replicate_tentative(7, &k, 42, h(42)).await.unwrap();
			// Explicit leader-side commit step writes the durable marker.
			r.commit(7).await.unwrap();
			assert_eq!(r.read_committed(&k).await.unwrap(), Some(42));
			assert_eq!(r.pending_count(), 0, "committed op left the pending set");
			store.fsync().await.unwrap();
		}
		let store = open_store(&path).await;
		let r = replica(store);
		assert_eq!(
			r.read_committed(&k).await.unwrap(),
			Some(42),
			"the explicitly-committed value survived restart via its marker"
		);
	}
}

#[cfg(test)]
mod leader_tests {
	//! ADR 075 cut-2b: `CounterLeader` orchestration over a deterministic
	//! participant harness (each participant a real `CounterReplica` over a real
	//! `LocalSlabStore`). The two failure-injection fixtures the leader boundary
	//! turns on — "remote accepts, leader fails before quorum" and "commit
	//! succeeds on some, fails on others" — are expressible here precisely
	//! because the harness is deterministic. Data-server `COUNTER_ADD_REQ`
	//! dispatch is covered in the network tests.

	use super::*;
	use crate::schema::{HybridTimestamp, SlabId, TenantId};
	use crate::storage::BlockingFileOps;
	use crate::storage::core::{GroupCommitConfig, LocalSlabStore};
	use std::path::{Path, PathBuf};
	use std::sync::atomic::AtomicBool;
	use std::time::Duration;

	fn slab_dir() -> PathBuf {
		let dir = std::env::temp_dir().join(format!(
			"wendelon-adr075-leader-{}-{}",
			std::process::id(),
			uuid::Uuid::new_v4()
		));
		std::fs::create_dir_all(&dir).unwrap();
		dir
	}

	fn h(ms: u64) -> HybridTimestamp {
		HybridTimestamp {
			physical_ms: ms,
			logical: 0,
			node_id: 0,
		}
	}

	fn node_id(index: usize) -> NodeId {
		NodeId {
			id: (index as u32) + 1,
			_reserved: 0,
		}
	}

	/// A fresh durable witness store under a unique temp dir (most fixtures
	/// don't restart; the restart fixture builds its own shared-dir store).
	fn wstore() -> DurableCounterWitnessStore {
		DurableCounterWitnessStore::open(slab_dir().join("witnesses")).unwrap()
	}

	async fn open_store(path: &Path) -> Arc<LocalSlabStore> {
		Arc::new(
			LocalSlabStore::open(
				path,
				SlabId::ZERO,
				Arc::new(BlockingFileOps),
				GroupCommitConfig {
					floor: Duration::ZERO,
					ceiling: Duration::ZERO,
					per_write_cost_estimate: Duration::ZERO,
				},
			)
			.await
			.unwrap(),
		)
	}

	fn ckey(b: &[u8]) -> CounterKey {
		CounterKey(b.to_vec())
	}

	/// Canonical write-key fixture: the test's small id, LE-embedded in 24 bytes.
	fn wk(n: u64) -> CounterWriteKey {
		let mut key = [0u8; 24];
		key[..8].copy_from_slice(&n.to_le_bytes());
		key
	}

	/// A participant over a real store, with deterministic fault injection.
	#[derive(Clone)]
	struct TestParticipant {
		replica: Arc<CounterReplica<LocalSlabStore>>,
		fail_tentative: Arc<AtomicBool>,
	}

	impl TestParticipant {
		fn over(store: Arc<LocalSlabStore>) -> Self {
			Self {
				replica: Arc::new(CounterReplica::new(SlabCounterStore::new(
					store,
					TenantId::new(7),
					NodeId {
						id: 1,
						_reserved: 0,
					},
				))),
				fail_tentative: Arc::new(AtomicBool::new(false)),
			}
		}
		/// The next `replicate_tentative` fails (excluded from the accepted set).
		fn arm_tentative_failure(&self) {
			self.fail_tentative.store(true, Ordering::Relaxed);
		}
		/// The next `commit` fails its durable marker write (partial commit).
		fn arm_commit_failure(&self) {
			self.replica.arm_commit_marker_failure();
		}
	}

	impl CounterParticipant for TestParticipant {
		async fn replicate_tentative(
			&self,
			op_id: u64,
			key: &CounterKey,
			value: u64,
			commit_hlc: HybridTimestamp,
		) -> Result<(), ParticipantError> {
			if self.fail_tentative.swap(false, Ordering::Relaxed) {
				return Err(ParticipantError::Unavailable);
			}
			self.replica
				.replicate_tentative(op_id, key, value, commit_hlc)
				.await
				.map_err(|_| ParticipantError::Unavailable)
		}
		async fn commit(&self, op_id: u64, _key: &CounterKey) -> Result<(), ParticipantError> {
			self.replica
				.commit(op_id)
				.await
				.map_err(|_| ParticipantError::Unavailable)
		}
		async fn rollback(&self, op_id: u64, _key: &CounterKey) {
			self.replica.rollback(op_id);
		}
		fn eligibility_status(&self, _identity: Identity) -> Result<(), CounterVisibilityError> {
			Ok(())
		}
		async fn read_eligible(
			&self,
			identity: Identity,
		) -> Result<Option<CounterVersion>, ParticipantError> {
			// Historical local-slab harness: raw substrate visibility remains unchanged.
			let slab = &self.replica.store.slab;
			let Some(location) = slab.counter_lookup(TenantId::new(7), identity) else {
				return Ok(None);
			};
			let value = slab
				.counter_read_value(location)
				.await
				.map_err(ParticipantError::from_io)?;
			Ok(Some(CounterVersion {
				value,
				hlc: location.hlc,
				origin: None,
			}))
		}
		async fn read_committed(&self, key: &CounterKey) -> Result<Option<u64>, ParticipantError> {
			self.replica
				.read_committed(key)
				.await
				.map_err(|_| ParticipantError::Unavailable)
		}
	}

	struct LeaderHarness {
		leader: CounterLeader<TestParticipant>,
		participants: Vec<(NodeId, TestParticipant)>,
		quorum: usize,
	}

	impl LeaderHarness {
		fn new(
			participants: Vec<TestParticipant>,
			quorum: usize,
			is_primary: bool,
			witness_store: DurableCounterWitnessStore,
		) -> Self {
			let participants: Vec<_> = participants
				.into_iter()
				.enumerate()
				.map(|(i, participant)| (node_id(i), participant))
				.collect();
			let local = participants
				.first()
				.expect("test leader needs a local participant")
				.1
				.clone();
			let leader = CounterLeader::new(node_id(0), local, is_primary, witness_store);
			Self {
				leader,
				participants,
				quorum,
			}
		}

		fn recover(
			participants: Vec<TestParticipant>,
			quorum: usize,
			is_primary: bool,
			witness_store: DurableCounterWitnessStore,
		) -> std::io::Result<Self> {
			let participants: Vec<_> = participants
				.into_iter()
				.enumerate()
				.map(|(i, participant)| (node_id(i), participant))
				.collect();
			let local = participants
				.first()
				.expect("test leader needs a local participant")
				.1
				.clone();
			let leader = CounterLeader::recover(node_id(0), local, is_primary, witness_store)?;
			Ok(Self {
				leader,
				participants,
				quorum,
			})
		}

		fn plan(&self) -> CounterParticipantPlan<TestParticipant> {
			CounterParticipantPlan::new(self.participants.clone(), self.quorum)
		}

		fn resolver(&self) -> StaticCounterParticipantResolver<TestParticipant> {
			StaticCounterParticipantResolver::new(self.participants.clone())
		}

		async fn fetch_add(
			&self,
			write_key: CounterWriteKey,
			key: &CounterKey,
			delta: u64,
		) -> Result<CounterAddResult, CounterLeaderError> {
			self.leader
				.fetch_add(self.plan(), write_key, key, delta)
				.await
		}

		async fn reconcile(&self) {
			self.leader.reconcile(&self.resolver()).await;
		}

		async fn read(&self, key: &CounterKey) -> Result<Option<u64>, CounterLeaderError> {
			self.leader.read(key).await
		}

		fn pending_witnesses(&self) -> usize {
			self.leader.pending_witnesses()
		}

		/// Read a specific participant's committed value (test inspection).
		async fn participant_read(&self, i: usize, key: &CounterKey) -> Option<u64> {
			self.participants[i].1.read_committed(key).await.unwrap()
		}
		/// Arm a participant's next commit to fail.
		fn arm_participant_commit_failure(&self, i: usize) {
			self.participants[i].1.arm_commit_failure();
		}
		/// Arm a participant's next tentative to fail.
		fn arm_participant_tentative_failure(&self, i: usize) {
			self.participants[i].1.arm_tentative_failure();
		}
	}

	/// Re-open a participant's slab and read its committed value (durability).
	async fn read_after_restart(path: &Path, key: &CounterKey) -> Option<u64> {
		let store = open_store(path).await;
		SlabCounterStore::new(
			store,
			TenantId::new(7),
			NodeId {
				id: 1,
				_reserved: 0,
			},
		)
		.read_committed(key)
		.await
		.unwrap()
	}

	/// Build `n` participants over fresh slabs; returns the leader-ready vec and
	/// the per-participant paths (for restart-durability checks).
	async fn participants(dir: &Path, n: usize) -> (Vec<TestParticipant>, Vec<PathBuf>) {
		let mut ps = Vec::new();
		let mut paths = Vec::new();
		for i in 0..n {
			let path = dir.join(format!("slab_p{i}.wnd"));
			ps.push(TestParticipant::over(open_store(&path).await));
			paths.push(path);
		}
		(ps, paths)
	}

	/// A `CounterReplicaTransport` that records the calls a `RemoteCounterParticipant`
	/// makes, so the arm's wire translation can be asserted without a real peer.
	#[derive(Default)]
	struct MockCounterTransport {
		replicates: std::sync::Mutex<Vec<(u64, u64, CounterReplicaPhase, Identity)>>,
		rollbacks: std::sync::Mutex<Vec<u64>>,
	}

	impl CounterReplicaTransport for MockCounterTransport {
		fn replicate(
			&self,
			_peer: NodeId,
			_tenant_id: TenantId,
			key_identity: Identity,
			op_id: u64,
			value: u64,
			_commit_hlc: HybridTimestamp,
			_epoch: u64,
			phase: CounterReplicaPhase,
		) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), ()>> + Send + '_>> {
			self.replicates
				.lock()
				.unwrap()
				.push((op_id, value, phase, key_identity));
			Box::pin(async { Ok(()) })
		}
		fn rollback(
			&self,
			_peer: NodeId,
			_tenant_id: TenantId,
			_key_identity: Identity,
			op_id: u64,
			_epoch: u64,
		) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
			self.rollbacks.lock().unwrap().push(op_id);
			Box::pin(async {})
		}
	}

	// cut-8 (Fork-1): the remote participant translates `CounterParticipant` calls
	// into the counter replicate/rollback wire — TENTATIVE carries the value + the
	// domain-separated identity, COMMIT carries the op_id, ROLLBACK carries the op_id.
	// The leader reads its base only locally, so a remote `read_committed` is never
	// invoked (it returns `Unavailable`).
	#[tokio::test]
	async fn remote_participant_translates_calls_to_transport() {
		let mock = Arc::new(MockCounterTransport::default());
		let peer = NodeId {
			id: 2,
			_reserved: 0,
		};
		let participant = RemoteCounterParticipant::new(mock.clone(), peer, TenantId::new(7), 1);
		let key = ckey(b"verdict");

		participant
			.replicate_tentative(7, &key, 5, h(7))
			.await
			.unwrap();
		participant.commit(7, &key).await.unwrap();
		participant.rollback(8, &key).await;

		let reps = mock.replicates.lock().unwrap();
		assert_eq!(reps.len(), 2);
		// TENTATIVE carries op_id + value + the domain-separated identity (not raw key).
		assert_eq!(
			reps[0],
			(7, 5, CounterReplicaPhase::TENTATIVE, counter_identity(&key))
		);
		assert_eq!(reps[1].0, 7, "commit carries the op_id");
		assert_eq!(reps[1].2, CounterReplicaPhase::COMMIT);
		assert_eq!(*mock.rollbacks.lock().unwrap(), vec![8]);
		assert!(
			participant.read_committed(&key).await.is_err(),
			"the leader never reads a remote replica's base"
		);
	}

	// Happy path: quorum reached, every accepted participant committed, the local
	// read reflects `current`, and the witness is fully resolved.
	#[tokio::test]
	async fn fetch_add_commits_all_accepted_and_resolves_witness() {
		let dir = slab_dir();
		let (ps, _paths) = participants(&dir, 3).await;
		let leader = LeaderHarness::new(ps, 2, true, wstore());
		let k = ckey(b"verdict");
		let out = leader.fetch_add(wk(1), &k, 5).await.unwrap();
		assert_eq!(
			out,
			CounterAddResult::Applied(CounterAddOutcome {
				previous: 0,
				current: 5
			})
		);
		assert_eq!(leader.read(&k).await.unwrap(), Some(5));
		assert_eq!(
			leader.pending_witnesses(),
			0,
			"witness resolved on full commit"
		);
		for i in 0..3 {
			assert_eq!(leader.participant_read(i, &k).await, Some(5));
		}
	}

	// Boundary 3: a duplicate write_id replays the original outcome WITHOUT
	// re-adding; a distinct write_id adds again.
	#[tokio::test]
	async fn duplicate_write_id_replays_without_readding() {
		let dir = slab_dir();
		let (ps, _paths) = participants(&dir, 3).await;
		let leader = LeaderHarness::new(ps, 2, true, wstore());
		let k = ckey(b"verdict");
		let first = leader.fetch_add(wk(1), &k, 5).await.unwrap();
		let replay = leader.fetch_add(wk(1), &k, 5).await.unwrap();
		assert_eq!(
			replay,
			CounterAddResult::Replayed(first.outcome()),
			"duplicate replays the original previous/current, marked a replay"
		);
		assert_eq!(
			leader.read(&k).await.unwrap(),
			Some(5),
			"duplicate did not re-add"
		);
		let second = leader.fetch_add(wk(2), &k, 5).await.unwrap();
		assert_eq!(
			second,
			CounterAddResult::Applied(CounterAddOutcome {
				previous: 5,
				current: 10
			})
		);
	}

	// Boundary 3: a non-primary REDIRECTS (NotLeader) rather than replaying — it
	// holds no authoritative idempotency record.
	#[tokio::test]
	async fn non_primary_redirects_not_replays() {
		let dir = slab_dir();
		let (ps, _paths) = participants(&dir, 1).await;
		let leader = LeaderHarness::new(ps, 1, false, wstore());
		let k = ckey(b"verdict");
		assert_eq!(
			leader.fetch_add(wk(1), &k, 5).await,
			Err(CounterLeaderError::NotLeader)
		);
	}

	// Boundary 1 (rollback): on quorum failure EVERY accepted participant is
	// rolled back. Discriminating: a rollback inferred from quorum state would
	// leave the accepted-but-sub-quorum participants poisoned.
	#[tokio::test]
	async fn quorum_failure_rolls_back_every_accepted_participant() {
		let dir = slab_dir();
		let (ps, _paths) = participants(&dir, 3).await;
		let leader = LeaderHarness::new(ps, 3, true, wstore()); // need all 3
		let k = ckey(b"verdict");
		leader.arm_participant_tentative_failure(2); // participant 2 will NOT accept
		assert_eq!(
			leader.fetch_add(wk(1), &k, 5).await,
			Err(CounterLeaderError::QuorumFailed)
		);
		// The accepted set was {0, 1}; both must be rolled back (not poisoned),
		// and participant 2 (never accepted) has nothing.
		assert_eq!(
			leader.participant_read(0, &k).await,
			None,
			"accepted local rolled back"
		);
		assert_eq!(
			leader.participant_read(1, &k).await,
			None,
			"accepted remote rolled back"
		);
		assert_eq!(leader.participant_read(2, &k).await, None);
		assert_eq!(
			leader.pending_witnesses(),
			0,
			"no witness on quorum failure"
		);
		// And the next add is not poisoned by the failed one.
		let out = leader.fetch_add(wk(2), &k, 1).await.unwrap().outcome();
		assert_eq!(
			out.previous, 0,
			"failed add did not poison the next previous"
		);
	}

	// Boundary 1 (commit): commit reaches EVERY accepted participant, including
	// one beyond the quorum count. Discriminating: a commit inferred from the
	// quorum set would skip participant 2, losing it on its failover.
	#[tokio::test]
	async fn commit_reaches_every_accepted_participant_beyond_quorum() {
		let dir = slab_dir();
		let (ps, paths) = participants(&dir, 3).await;
		let leader = LeaderHarness::new(ps, 2, true, wstore()); // quorum 2 of 3, all accept
		let k = ckey(b"verdict");
		leader.fetch_add(wk(1), &k, 5).await.unwrap();
		assert_eq!(leader.pending_witnesses(), 0);
		// Participant 2 is beyond the quorum count but still in the accepted set:
		// its marker must be durable.
		assert_eq!(
			read_after_restart(&paths[2], &k).await,
			Some(5),
			"the accepted-beyond-quorum participant has a durable marker"
		);
	}

	// Operator fixture 1: a remote accepts the tentative, the leader fails before
	// quorum (no commit, no rollback), the remote becomes primary, and the next
	// add sees `previous` UNPOISONED — the orphan tentative is skipped on recovery.
	#[tokio::test]
	async fn remote_accept_then_leader_failure_before_quorum_does_not_poison_failover() {
		let dir = slab_dir();
		let path_a = dir.join("slab_remote_a.wnd");
		let k = ckey(b"verdict");
		{
			// The leader sent a tentative; remote A accepted it; the leader then
			// died before reaching quorum (so neither commit nor rollback ran).
			let store_a = open_store(&path_a).await;
			let a = CounterReplica::new(SlabCounterStore::new(
				store_a.clone(),
				TenantId::new(7),
				NodeId {
					id: 1,
					_reserved: 0,
				},
			));
			a.replicate_tentative(1, &k, 5, h(5)).await.unwrap();
			assert_eq!(
				a.read_committed(&k).await.unwrap(),
				Some(5),
				"A holds it live"
			);
			store_a.fsync().await.unwrap();
		}
		// A becomes the primary after restart and takes the next add.
		let store_a2 = open_store(&path_a).await;
		let leader = LeaderHarness::new(vec![TestParticipant::over(store_a2)], 1, true, wstore());
		let out = leader.fetch_add(wk(2), &k, 1).await.unwrap().outcome();
		assert_eq!(
			out.previous, 0,
			"the pre-quorum tentative did not poison the failover primary's base"
		);
		assert_eq!(out.current, 1);
	}

	// Operator fixture 2: commit succeeds on some accepted participants and fails
	// on one. The leader does NOT roll back (the witness stays pending), then the
	// reconciler completes the remaining commit — no double-add, no loss.
	#[tokio::test]
	async fn partial_commit_reconciles_to_completion_no_double_add_no_loss() {
		let dir = slab_dir();
		let (ps, paths) = participants(&dir, 3).await;
		let leader = LeaderHarness::new(ps, 2, true, wstore());
		let k = ckey(b"verdict");
		leader.arm_participant_commit_failure(2); // participant 2's commit marker fails once
		let out = leader.fetch_add(wk(1), &k, 5).await.unwrap();
		assert_eq!(
			out,
			CounterAddResult::Applied(CounterAddOutcome {
				previous: 0,
				current: 5
			})
		);
		// Quorum reached; 0 and 1 committed; 2 is owed — recoverable-pending, NOT
		// rolled back. Committed participants keep the value.
		assert_eq!(
			leader.pending_witnesses(),
			1,
			"2 still owes a commit marker"
		);
		assert_eq!(
			leader.participant_read(0, &k).await,
			Some(5),
			"committed, not rolled back"
		);
		assert_eq!(
			leader.participant_read(1, &k).await,
			Some(5),
			"committed, not rolled back"
		);

		// The reconciler finishes the remaining commit marker fan-out.
		leader.reconcile().await;
		assert_eq!(
			leader.pending_witnesses(),
			0,
			"reconcile completed the witness"
		);

		// No loss and no double-add: every accepted participant is durably at 5.
		for path in &paths {
			assert_eq!(
				read_after_restart(path, &k).await,
				Some(5),
				"reconciled value is durable on every accepted participant (5, not lost, not doubled)"
			);
		}
	}

	// ADR 076 D3 discriminator: pending witnesses re-drive the recorded peer
	// identities, while a later fresh add can use a different current-ring
	// participant set. A positional re-drive would commit the old witness on the
	// new peer (2) instead of the recorded peer (1).
	#[tokio::test]
	async fn recorded_redrive_and_fresh_routing_are_distinct_after_ring_shift() {
		let dir = slab_dir();
		let (ps, paths) = participants(&dir, 3).await;
		let leader = LeaderHarness::new(ps, 2, true, wstore());
		let k = ckey(b"verdict");
		let p0 = leader.participants.first().unwrap().clone();
		let p1 = leader.participants.get(1).unwrap().clone();
		let p2 = leader.participants.get(2).unwrap().clone();

		let old_recorded_plan = CounterParticipantPlan::new(vec![p0.clone(), p1.clone()], 2);
		leader.arm_participant_commit_failure(1);
		assert_eq!(
			leader
				.leader
				.fetch_add(old_recorded_plan, wk(1), &k, 5)
				.await
				.unwrap(),
			CounterAddResult::Applied(CounterAddOutcome {
				previous: 0,
				current: 5,
			})
		);
		assert_eq!(
			leader.pending_witnesses(),
			1,
			"recorded peer 1 still owes the old commit marker"
		);

		let recorded_resolver = StaticCounterParticipantResolver::new(vec![p0.clone(), p1.clone()]);
		leader.leader.reconcile(&recorded_resolver).await;
		assert_eq!(
			leader.pending_witnesses(),
			0,
			"old witness resolved through the recorded resolver"
		);
		assert_eq!(
			read_after_restart(&paths[1], &k).await,
			Some(5),
			"recorded peer 1 received the owed commit marker"
		);
		assert_eq!(
			read_after_restart(&paths[2], &k).await,
			None,
			"new-ring peer 2 was not guessed from the old recorded witness"
		);

		let new_ring_plan = CounterParticipantPlan::new(vec![p0, p2], 2);
		assert_eq!(
			leader
				.leader
				.fetch_add(new_ring_plan, wk(2), &k, 3)
				.await
				.unwrap(),
			CounterAddResult::Applied(CounterAddOutcome {
				previous: 5,
				current: 8,
			})
		);
		assert_eq!(
			read_after_restart(&paths[2], &k).await,
			Some(8),
			"fresh add used the new current-ring peer 2"
		);
		assert_eq!(
			read_after_restart(&paths[1], &k).await,
			Some(5),
			"fresh add did not keep targeting the old recorded-only peer"
		);
	}

	// Acceptance: a LEADER restart with a partial commit loads the durable
	// witness and reconciles markers to every accepted participant. The witness
	// is removed only after all are committed.
	#[tokio::test]
	async fn restart_with_partial_commit_loads_witness_and_reconciles() {
		let dir = slab_dir();
		let wdir = dir.join("witnesses");
		let paths: Vec<PathBuf> = (0..3).map(|i| dir.join(format!("slab_p{i}.wnd"))).collect();
		let k = ckey(b"verdict");
		// Leader 1: partial commit (participant 2's marker fails), then "crash".
		{
			let mut ps = Vec::new();
			let mut stores = Vec::new();
			for path in &paths {
				let s = open_store(path).await;
				ps.push(TestParticipant::over(s.clone()));
				stores.push(s);
			}
			let leader = LeaderHarness::new(
				ps,
				2,
				true,
				DurableCounterWitnessStore::open(&wdir).unwrap(),
			);
			leader.arm_participant_commit_failure(2);
			let out = leader.fetch_add(wk(1), &k, 5).await.unwrap();
			assert_eq!(
				out,
				CounterAddResult::Applied(CounterAddOutcome {
					previous: 0,
					current: 5
				})
			);
			assert_eq!(
				leader.pending_witnesses(),
				1,
				"2 owes a marker before crash"
			);
			for s in &stores {
				s.fsync().await.unwrap();
			}
			// leader1 dropped — the process "crashes" with the witness durable.
		}
		// Leader 2: recover from the SAME witness dir + reopened (fresh) stores.
		let mut ps2 = Vec::new();
		for path in &paths {
			ps2.push(TestParticipant::over(open_store(path).await));
		}
		let leader2 = LeaderHarness::recover(
			ps2,
			2,
			true,
			DurableCounterWitnessStore::open(&wdir).unwrap(),
		)
		.unwrap();
		assert_eq!(
			leader2.pending_witnesses(),
			1,
			"the partial witness was loaded from disk on restart"
		);
		// F2: the pending witness ALSO restores the D4 dedup record — a duplicate
		// retry that lands after restart but BEFORE reconcile replays, not re-adds.
		assert_eq!(
			leader2.fetch_add(wk(1), &k, 5).await.unwrap(),
			CounterAddResult::Replayed(CounterAddOutcome {
				previous: 0,
				current: 5
			}),
			"a post-restart duplicate replays from the recovered witness"
		);
		leader2.reconcile().await;
		assert_eq!(
			leader2.pending_witnesses(),
			0,
			"reconcile completed the commit fan-out after restart"
		);
		// Every accepted participant is durably at 5 (no loss, no double-add).
		for path in &paths {
			assert_eq!(read_after_restart(path, &k).await, Some(5));
		}
		// The durable witness is removed only after every marker is written.
		assert!(
			DurableCounterWitnessStore::open(&wdir)
				.unwrap()
				.load_all()
				.unwrap()
				.is_empty(),
			"witness removed only after all accepted participants committed"
		);
	}

	// Acceptance: a duplicate retry DURING the pending window replays the
	// original outcome and does not re-add (no second op, value unchanged).
	#[tokio::test]
	async fn duplicate_retry_during_pending_replays_and_does_not_readd() {
		let dir = slab_dir();
		let (ps, _paths) = participants(&dir, 3).await;
		let leader = LeaderHarness::new(ps, 2, true, wstore());
		let k = ckey(b"verdict");
		leader.arm_participant_commit_failure(2); // leave the witness pending
		let first = leader.fetch_add(wk(1), &k, 5).await.unwrap();
		assert_eq!(leader.pending_witnesses(), 1, "still pending");
		// Same write-key while pending → replays the eventual result, no re-add.
		let replay = leader.fetch_add(wk(1), &k, 5).await.unwrap();
		assert_eq!(
			replay,
			CounterAddResult::Replayed(first.outcome()),
			"duplicate during pending replays the outcome"
		);
		assert_eq!(leader.pending_witnesses(), 1, "no second op was created");
		assert_eq!(
			leader.participant_read(0, &k).await,
			Some(5),
			"value unchanged (not re-added to 10)"
		);
	}

	// Acceptance: repeated reconcile is idempotent — a second pass over a
	// resolved witness is a no-op and does not double-add.
	#[tokio::test]
	async fn repeated_reconcile_is_idempotent() {
		let dir = slab_dir();
		let (ps, paths) = participants(&dir, 3).await;
		let leader = LeaderHarness::new(ps, 2, true, wstore());
		let k = ckey(b"verdict");
		leader.arm_participant_commit_failure(2);
		leader.fetch_add(wk(1), &k, 5).await.unwrap();
		leader.reconcile().await;
		leader.reconcile().await; // second pass: no witness left, no-op
		leader.reconcile().await;
		assert_eq!(leader.pending_witnesses(), 0);
		assert_eq!(
			leader.read(&k).await.unwrap(),
			Some(5),
			"value stable across reconciles"
		);
		for path in &paths {
			assert_eq!(read_after_restart(path, &k).await, Some(5));
		}
	}

	// Acceptance (commit_hlc / locations needed for re-drive): a stale reconcile
	// after restart must NOT supersede a newer committed value. op N is partial
	// on a participant; op N+1 commits a newer value there; op N reconciles after
	// restart re-driving at its ORIGINAL commit_hlc, so the newer value stands.
	#[tokio::test]
	async fn stale_reconcile_does_not_supersede_newer_value_after_restart() {
		let dir = slab_dir();
		let wdir = dir.join("witnesses");
		let paths: Vec<PathBuf> = (0..2).map(|i| dir.join(format!("slab_p{i}.wnd"))).collect();
		let k = ckey(b"verdict");
		{
			let mut ps = Vec::new();
			let mut stores = Vec::new();
			for path in &paths {
				let s = open_store(path).await;
				ps.push(TestParticipant::over(s.clone()));
				stores.push(s);
			}
			let leader = LeaderHarness::new(
				ps,
				1,
				true,
				DurableCounterWitnessStore::open(&wdir).unwrap(),
			);
			// op N (+5): participant 1's marker fails -> opN partial, witness pending.
			leader.arm_participant_commit_failure(1);
			assert_eq!(
				leader
					.fetch_add(wk(1), &k, 5)
					.await
					.unwrap()
					.outcome()
					.current,
				5
			);
			assert_eq!(leader.pending_witnesses(), 1);
			// op N+1 (+3 -> 8): fully commits on BOTH participants, newer commit_hlc.
			assert_eq!(
				leader
					.fetch_add(wk(2), &k, 3)
					.await
					.unwrap()
					.outcome()
					.current,
				8
			);
			assert_eq!(
				leader.participant_read(1, &k).await,
				Some(8),
				"p1 now holds the newer 8"
			);
			for s in &stores {
				s.fsync().await.unwrap();
			}
		}
		// Restart: p1's in-flight pending is gone, so opN reconcile must RE-APPEND
		// its tentative — at opN's original commit_hlc, below opN+1's.
		let mut ps2 = Vec::new();
		for path in &paths {
			ps2.push(TestParticipant::over(open_store(path).await));
		}
		let leader2 = LeaderHarness::recover(
			ps2,
			1,
			true,
			DurableCounterWitnessStore::open(&wdir).unwrap(),
		)
		.unwrap();
		leader2.reconcile().await;
		assert_eq!(leader2.pending_witnesses(), 0, "opN reconciled");
		// The stale opN (value 5) did NOT supersede opN+1 (value 8).
		assert_eq!(
			read_after_restart(&paths[1], &k).await,
			Some(8),
			"stale reconcile at the original commit_hlc did not poison the newer value"
		);
	}

	// F2 (D4 across restart): a duplicate retry AFTER a leader restart — when the
	// witness is long RESOLVED — replays from the durable applied log instead of
	// re-adding against the recovered committed value. Discriminating: without
	// the applied log (witness pruned on resolve, dedup in-memory only), recover
	// has no record of write-key 1 and the retry would re-add 0→5 on top of the
	// committed 5 (→ 10).
	#[tokio::test]
	async fn duplicate_retry_after_restart_replays_from_applied_log() {
		let dir = slab_dir();
		let wdir = dir.join("witnesses");
		let path = dir.join("slab_p0.wnd");
		let k = ckey(b"verdict");
		{
			let store = open_store(&path).await;
			let leader = LeaderHarness::new(
				vec![TestParticipant::over(store.clone())],
				1,
				true,
				DurableCounterWitnessStore::open(&wdir).unwrap(),
			);
			let first = leader.fetch_add(wk(1), &k, 5).await.unwrap();
			assert_eq!(
				first,
				CounterAddResult::Applied(CounterAddOutcome {
					previous: 0,
					current: 5
				})
			);
			assert_eq!(
				leader.pending_witnesses(),
				0,
				"fully resolved — the dedup record now lives ONLY in the applied log"
			);
			store.fsync().await.unwrap();
		}
		// Restart: recover from the same dirs.
		let leader2 = LeaderHarness::recover(
			vec![TestParticipant::over(open_store(&path).await)],
			1,
			true,
			DurableCounterWitnessStore::open(&wdir).unwrap(),
		)
		.unwrap();
		leader2.reconcile().await;
		// The duplicate retry replays the ORIGINAL outcome — no re-add.
		assert_eq!(
			leader2.fetch_add(wk(1), &k, 5).await.unwrap(),
			CounterAddResult::Replayed(CounterAddOutcome {
				previous: 0,
				current: 5
			}),
			"post-restart duplicate replays from the applied log"
		);
		assert_eq!(
			leader2.read(&k).await.unwrap(),
			Some(5),
			"the retry did not re-add (5, not 10)"
		);
		// A FRESH add still bases on the recovered committed value.
		assert_eq!(
			leader2.fetch_add(wk(2), &k, 1).await.unwrap(),
			CounterAddResult::Applied(CounterAddOutcome {
				previous: 5,
				current: 6
			})
		);
	}

	// F5: recovery seeds the op-id allocator from the APPLIED records, not just
	// pending witnesses — `commit_hlc_for` derives the HLC from the op-id, so a
	// clean restart (no pending witnesses) must NOT reset `op_seq` and re-mint an
	// already-used commit HLC for the next add. Discriminating: without applied
	// op-id seeding, the post-restart add reuses op 1 and its committed version
	// lands at the SAME hlc as the pre-restart one (equal, not strictly newer),
	// silently breaking the "later ops always order after earlier ones" contract
	// (the basis of stale-reconcile protection and as-of/version semantics).
	#[tokio::test]
	async fn fresh_add_after_restart_gets_strictly_newer_commit_hlc() {
		let dir = slab_dir();
		let wdir = dir.join("witnesses");
		let path = dir.join("slab_p0.wnd");
		let k = ckey(b"verdict");
		let identity = counter_identity(&k);
		let hlc_before;
		{
			let store = open_store(&path).await;
			let leader = LeaderHarness::new(
				vec![TestParticipant::over(store.clone())],
				1,
				true,
				DurableCounterWitnessStore::open(&wdir).unwrap(),
			);
			leader.fetch_add(wk(1), &k, 5).await.unwrap();
			assert_eq!(leader.pending_witnesses(), 0, "clean restart: no pending");
			hlc_before = store
				.counter_lookup(TenantId::new(7), identity)
				.expect("committed version is indexed")
				.hlc;
			store.fsync().await.unwrap();
		}
		// Clean restart — recovery must seed op_seq past the applied op-ids.
		let store2 = open_store(&path).await;
		let leader2 = LeaderHarness::recover(
			vec![TestParticipant::over(store2.clone())],
			1,
			true,
			DurableCounterWitnessStore::open(&wdir).unwrap(),
		)
		.unwrap();
		leader2.reconcile().await;
		assert_eq!(
			leader2.fetch_add(wk(2), &k, 3).await.unwrap(),
			CounterAddResult::Applied(CounterAddOutcome {
				previous: 5,
				current: 8
			})
		);
		let hlc_after = store2
			.counter_lookup(TenantId::new(7), identity)
			.expect("the fresh add's version is indexed")
			.hlc;
		assert!(
			hlc_after.physical_ms > hlc_before.physical_ms,
			"the post-restart add's commit HLC must be STRICTLY newer (got {} after {} — an op-id was reused)",
			hlc_after.physical_ms,
			hlc_before.physical_ms
		);
	}
}

#[cfg(test)]
#[path = "counter/replica_scope_tests.rs"]
mod replica_scope_tests;
