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

use std::io::{Error, ErrorKind};
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex as StdMutex};

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
	/// The write-key already names this same add — the original outcome, nothing re-added.
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

use crate::consensus::monitor::{PhysicalClock, SystemClock};
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
	/// Complete the manager's in-flight tentative admission after the marker is
	/// durable and any live eligibility reservation has been published.
	fn counter_commit_published(
		&self,
		_tenant_id: TenantId,
		_key_identity: Identity,
		_location: RecordLocation,
		_origin: NodeId,
	) {
	}
	/// Remove a tentative version from the live index (rollback; recovery skips
	/// the orphan).
	fn counter_unindex(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
		location: RecordLocation,
	) -> impl std::future::Future<Output = std::io::Result<()>> + Send;
	/// Latest indexed version for the counter key.
	fn counter_lookup(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
	) -> std::io::Result<Option<RecordLocation>>;
	/// A standalone slab has no manager-wide join to resolve leftover markers.
	/// Manager-backed stores resolve these during recovery and use the default.
	fn counter_recovery_error(
		&self,
		_tenant_id: TenantId,
		_key_identity: Identity,
	) -> Option<std::io::Error> {
		None
	}
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
		// Keep the LocalSlabStore test adapter's prepare semantics aligned with
		// SlabManager: an acknowledged tentative has crossed its durability edge.
		self.wait_durable_commit().await?;
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
	async fn counter_unindex(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
		location: RecordLocation,
	) -> std::io::Result<()> {
		self.rollback_index_entry(tenant_id, key_identity, location);
		Ok(())
	}
	fn counter_lookup(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
	) -> std::io::Result<Option<RecordLocation>> {
		self.lookup(tenant_id, key_identity)
	}
	fn counter_recovery_error(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
	) -> Option<std::io::Error> {
		if let Some((_, _, error)) = self
			.recovered_counter_errors
			.iter()
			.find(|(tenant, key, _)| *tenant == tenant_id && *key == key_identity)
		{
			return Some(error.io());
		}
		self.recovered_counter_markers_v2
			.iter()
			.any(|marker| marker.tenant_id == tenant_id && marker.key_identity == key_identity)
			.then(|| CounterVisibilityError::Conflict.io())
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
	fn counter_commit_published(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
		location: RecordLocation,
		origin: NodeId,
	) {
		crate::storage::manager::SlabManager::counter_commit_published(
			self,
			tenant_id,
			key_identity,
			location,
			origin,
		);
	}
	async fn counter_unindex(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
		location: RecordLocation,
	) -> std::io::Result<()> {
		self.counter_unindex_tentative(tenant_id, key_identity, location)
			.await
	}
	fn counter_lookup(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
	) -> std::io::Result<Option<RecordLocation>> {
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
		if let Some(error) = self.slab.counter_recovery_error(self.tenant_id, identity) {
			return Err(error);
		}
		let Some(location) = self.slab.counter_lookup(self.tenant_id, identity)? else {
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
			.await?;
		self.slab
			.counter_commit_published(self.tenant_id, t.key_identity, t.location, self.origin);
		Ok(())
	}

	/// Roll back a tentative: remove the live index entry only. No durable abort
	/// marker is written — the durable tentative frame is left an orphan that
	/// `recover_slab` skips (it has no commit marker).
	pub async fn rollback_tentative(&self, t: SlabTentative) -> std::io::Result<()> {
		self.slab
			.counter_unindex(self.tenant_id, t.key_identity, t.location)
			.await
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
		let gate = self.op_locks.entry(op_id).or_default().clone();
		let _guard = gate.lock().await;
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
	pub async fn rollback(&self, op_id: u64) -> std::io::Result<()> {
		let gate = self.op_locks.entry(op_id).or_default().clone();
		let _guard = gate.lock().await;
		if let Some(tentative) = self.pending.get(&op_id).map(|entry| *entry.value()) {
			self.store.rollback_tentative(tentative).await?;
			self.pending.remove(&op_id);
		}
		self.op_locks.remove(&op_id);
		Ok(())
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
/// Logical admissions for one replica operation, including queued futures.
struct ReplicaGateEntry {
	mutex: Arc<Mutex<()>>,
	participants: usize,
}

pub(crate) struct ReplicaGate {
	service: CounterReplicaService,
	key: ReplicaOpKey2,
	identity: std::sync::Weak<Mutex<()>>,
	guard: Option<tokio::sync::OwnedMutexGuard<()>>,
}
impl ReplicaGate {
	/// Count actual reserved operations without retaining a shard across a yield.
	#[cfg(test)]
	pub(crate) fn participant_count(&self) -> usize {
		let entry = self
			.service
			.op_locks
			.get(&self.key)
			.expect("live pause gate");
		assert!(std::sync::Weak::ptr_eq(
			&self.identity,
			&Arc::downgrade(&entry.mutex)
		));
		entry.participants
	}

	async fn admit(service: &CounterReplicaService, key: ReplicaOpKey2) -> Self {
		let mutex = {
			let mut entry = service
				.op_locks
				.entry(key)
				.or_insert_with(|| ReplicaGateEntry {
					mutex: Arc::new(Mutex::new(())),
					participants: 0,
				});
			entry.participants = entry
				.participants
				.checked_add(1)
				.expect("replica admissions overflow");
			entry.mutex.clone()
		};
		// The ticket exists before awaiting: cancellation retires its reservation.
		let mut ticket = Self {
			service: service.clone(),
			key,
			identity: Arc::downgrade(&mutex),
			guard: None,
		};
		ticket.guard = Some(mutex.lock_owned().await);
		ticket
	}
}
impl Drop for ReplicaGate {
	fn drop(&mut self) {
		// Keep this admission counted while unlock can synchronously finish a waiter.
		// Neither map may be borrowed across this reentrant callback.
		drop(self.guard.take());
		if let dashmap::mapref::entry::Entry::Occupied(mut entry) =
			self.service.op_locks.entry(self.key)
		{
			assert!(
				std::sync::Weak::ptr_eq(&self.identity, &Arc::downgrade(&entry.get().mutex)),
				"live replica gate identity changed"
			);
			let count = &mut entry.get_mut().participants;
			*count = count
				.checked_sub(1)
				.expect("replica admission retired twice");
			// A successor may have inserted pending during unlock. Read it freshly
			// at zero admissions, under the shard that also serializes new arrivals.
			if *count == 0 && !self.service.pending.contains_key(&self.key) {
				drop(entry.remove());
			}
		} else {
			panic!("live replica gate disappeared");
		}
	}
}

/// The server-side counter replica over a [`SlabManager`].
#[derive(Clone)]
pub struct CounterReplicaService {
	slab: Arc<crate::storage::manager::SlabManager>,
	marked: Arc<EligibleCells<(TenantId, Identity)>>,
	runtime: tokio::runtime::Handle,
	pending: Arc<DashMap<ReplicaOpKey2, PendingReplicaTentative>>,
	op_locks: Arc<DashMap<ReplicaOpKey2, ReplicaGateEntry>>,
	rollback_in_progress: Arc<DashMap<ReplicaOpKey2, ()>>,
}
impl CounterReplicaService {
	pub fn new(slab: Arc<crate::storage::manager::SlabManager>) -> Self {
		Self {
			marked: slab.counter_eligible_cells(),
			runtime: tokio::runtime::Handle::current(),
			slab,
			pending: Arc::new(DashMap::new()),
			op_locks: Arc::new(DashMap::new()),
			rollback_in_progress: Arc::new(DashMap::new()),
		}
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
		let _gate = ReplicaGate::admit(self, key).await;
		if self.rollback_in_progress.contains_key(&key) {
			return Err(std::io::Error::new(
				std::io::ErrorKind::WouldBlock,
				"counter rollback requires retry",
			));
		}
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
		let _gate = ReplicaGate::admit(self, key).await;
		if self.rollback_in_progress.contains_key(&key) {
			return Err(std::io::Error::new(
				std::io::ErrorKind::WouldBlock,
				"counter rollback requires retry",
			));
		}
		let Some(pending) = self.pending.get(&key).map(|entry| *entry.value()) else {
			let commit_hlc = HybridTimestamp {
				physical_ms: op_id,
				logical: 0,
				node_id: 0,
			};
			let committed = self
				.marked
				.committed_identity(&(tenant_id, expected_key), commit_hlc, origin)
				.map_err(CounterVisibilityError::io)?;
			return if committed.is_some() {
				Ok(())
			} else {
				Err(CounterVisibilityError::TentativeMissing.io())
			};
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
		self.slab.counter_commit_published(
			pending.tenant_id,
			pending.key_identity,
			pending.location,
			origin,
		);
		self.pending.remove(&key);
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
		let gate = ReplicaGate::admit(self, key).await;
		let Some(pending) = self.pending.get(&key).map(|entry| *entry.value()) else {
			self.rollback_in_progress.remove(&key);
			return Ok(());
		};
		if pending.tenant_id != tenant_id || pending.key_identity != expected_key {
			return Err(CounterReplicaRejection::TentativeConflict.into_error());
		}
		self.rollback_in_progress.insert(key, ());
		let service = self.clone();
		self.runtime
			.spawn(async move {
				let _gate = gate;
				service
					.slab
					.counter_unindex_tentative(
						pending.tenant_id,
						pending.key_identity,
						pending.location,
					)
					.await?;
				service.pending.remove(&key);
				service.rollback_in_progress.remove(&key);
				Ok(())
			})
			.await
			.map_err(|error| {
				std::io::Error::other(format!("counter rollback job failed: {error}"))
			})?
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
	pub(crate) async fn ctr4_pause_operation(
		&self,
		origin: NodeId,
		tenant: TenantId,
		op: u64,
	) -> ReplicaGate {
		let key = ReplicaOpKey2::new(origin, tenant, op).expect("fixture scope");
		ReplicaGate::admit(self, key).await
	}
	#[cfg(test)]
	pub(crate) fn ctr4_operation_state(
		&self,
		origin: NodeId,
		tenant: TenantId,
		op: u64,
	) -> (usize, bool) {
		let key = ReplicaOpKey2::new(origin, tenant, op).unwrap();
		self.op_locks
			.get(&key)
			.map(|entry| (entry.participants, entry.mutex.try_lock().is_err()))
			.unwrap_or((0, false))
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
	/// The write key already names a different add (another counter key, another
	/// delta, or an unkeyed record that cannot show which add it names). Nothing
	/// was applied and the recorded outcome is withheld. Not retryable as-is.
	WriteKeyConflict,
	/// E0: the write key is not a UUIDv7 identity, or was issued further ahead
	/// of this node's clock than the skew allowance. Nothing was applied.
	InvalidWriteIdentity,
	/// E1: the write key was issued at or before the dedup horizon. Nothing was
	/// applied by this request; whether an earlier presentation committed is
	/// indeterminate (its record may have been compacted away).
	OperationExpired,
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

mod applied_log;
mod compaction;
mod file_ops;
mod retention;
use applied_log::{
	AppliedLogFault, AppliedLogImage, validate_witness_set, witness_matches_applied,
};
use file_ops::{CounterDurableFileOps, StdCounterDurableFileOps};
pub use retention::{CounterRetentionConfig, DEFAULT_COUNTER_CLOCK_SKEW_MS};

mod write_key;
use write_key::{RecordedAdd, WriteKeyGate, WriteKeyGates};

/// The durable D4 dedup record a resolved witness compacts into: enough to
/// replay the original outcome for a duplicate `CounterWriteKey` — plus the
/// op's id, so recovery seeds the op-id allocator past EVERY resolved op, not
/// just the pending ones. Without it, a clean restart (no pending witnesses)
/// resets `op_seq` to 1 and the next add re-mints an already-used
/// `commit_hlc_for(op_id)`, breaking the "later ops always order after earlier
/// ones" contract the stale-reconcile protection relies on.
///
/// `key` is the counter the add resolved. Every record this build appends
/// carries it (the keyed frame), so recovery binds a record to its witness's
/// counter as well as its identity and outcome. It is `None` only for an
/// unkeyed record written before the keyed frame existed; for those, recovery
/// can compare only the operation id, write key and outcome.
#[derive(Clone, Debug, PartialEq, Eq)]
struct AppliedCounterAdd {
	op_id: u64,
	write_key: CounterWriteKey,
	key: Option<CounterKey>,
	previous: u64,
	current: u64,
}

/// Durable backing for counter-finalization witnesses: one small file per
/// pending op-id under a directory, published with a synced temp file, an
/// atomic rename and a directory sync, rewritten the same way as participants
/// confirm their markers, and removed (then the directory synced) only after
/// the op's applied record is durable. On restart `load_all` rebuilds the
/// pending set so the reconciler can finish the commit-marker fan-out. (This is
/// the counter-specific recoverable-pending state — it never routes through the
/// generic `TenantFinalization` path.)
///
/// The store is the tenant leader's only dedup authority. When it is owned (the
/// production path), every mutation holds a write permit of the node's
/// data-directory owner, whose exclusive lock makes this store the only writer
/// of its directory for the node's lifetime.
pub struct DurableCounterWitnessStore {
	dir: PathBuf,
	/// The tenant whose leader owns this store, for diagnostics.
	tenant: Option<TenantId>,
	owner: Option<crate::storage::ownership::DataDirectoryOwner>,
	ops: Arc<dyn CounterDurableFileOps>,
	/// Serializes witness publication, rewrites and removal, and the applied
	/// append's check that the operation's witness is still published.
	witness_lock: StdMutex<()>,
	/// The applied log as this store last wrote it. Holding the lock serializes
	/// appends, so frames from concurrent resolutions never interleave.
	applied: StdMutex<AppliedLogState>,
	/// Latched when the durable state can no longer be described: an applied
	/// append or barrier failed, a failed prepare could not withdraw its
	/// witness, or a compaction failed at or after its `CURRENT` rename. Every
	/// later publish, rewrite, append and compaction then fails closed with
	/// `AppliedLogFault::Unsettled` until recovery reopens the store.
	unsettled: std::sync::atomic::AtomicBool,
	/// `Q_mem`: the durable issuance cutoff of the current generation. Stored
	/// with Release ordering once a compacted generation is durable and before
	/// any record it deleted is evicted; read with Acquire ordering by E1.
	cutoff: AtomicU64,
	/// The highest generation number a compaction has tried in this process.
	attempted_generation: AtomicU64,
	#[cfg(test)]
	switches: compaction::CompactionSwitches,
}

enum AppliedLogState {
	/// Not read yet. A store used without `CounterLeader::recover` loads the log
	/// once, on its first append.
	Unloaded,
	/// Authoritative for the store's lifetime: the owner lock admits no other
	/// writer, so a length this image does not expect fails closed.
	Loaded(AppliedLogImage),
	/// An append or barrier failed, leaving the durable length unknown. Every
	/// further append fails closed until recovery reopens the store.
	Unsettled,
}

impl DurableCounterWitnessStore {
	pub fn open(dir: impl Into<PathBuf>) -> std::io::Result<Self> {
		Self::open_with_file_ops(dir, Arc::new(StdCounterDurableFileOps))
	}

	pub(crate) fn open_with_owner(
		dir: PathBuf,
		owner: crate::storage::ownership::DataDirectoryOwner,
	) -> std::io::Result<Self> {
		owner.ensure_contains(&dir)?;
		Self::open_inner(dir, Some(owner), Arc::new(StdCounterDurableFileOps))
	}

	fn open_with_file_ops(
		dir: impl Into<PathBuf>,
		ops: Arc<dyn CounterDurableFileOps>,
	) -> std::io::Result<Self> {
		Self::open_inner(dir.into(), None, ops)
	}

	fn open_inner(
		dir: PathBuf,
		owner: Option<crate::storage::ownership::DataDirectoryOwner>,
		ops: Arc<dyn CounterDurableFileOps>,
	) -> std::io::Result<Self> {
		let _permit =
			crate::storage::ownership::DataDirectoryOwner::optional_permit(owner.as_ref())?;
		// A durable namespace baseline before any operation is written.
		file_ops::create_dir_durably(ops.as_ref(), &dir)?;
		Ok(Self::unopened(dir, owner, ops))
	}

	/// Open over an existing directory WITHOUT its open-time directory sync
	/// (test-only), so a test can show what recovery's own barrier provides.
	#[cfg(test)]
	fn open_without_open_sync(
		dir: impl Into<PathBuf>,
		ops: Arc<dyn CounterDurableFileOps>,
	) -> Self {
		Self::unopened(dir.into(), None, ops)
	}

	fn unopened(
		dir: PathBuf,
		owner: Option<crate::storage::ownership::DataDirectoryOwner>,
		ops: Arc<dyn CounterDurableFileOps>,
	) -> Self {
		Self {
			dir,
			tenant: None,
			owner,
			ops,
			witness_lock: StdMutex::new(()),
			applied: StdMutex::new(AppliedLogState::Unloaded),
			unsettled: std::sync::atomic::AtomicBool::new(false),
			cutoff: AtomicU64::new(0),
			attempted_generation: AtomicU64::new(0),
			#[cfg(test)]
			switches: compaction::CompactionSwitches::default(),
		}
	}

	/// Name the tenant this store serves (diagnostics only).
	pub(crate) fn with_tenant(mut self, tenant: TenantId) -> Self {
		self.tenant = Some(tenant);
		self
	}

	/// Whether the store has latched fail-closed (see `unsettled`). The data
	/// server polls this to trip its local-persistence distrust fence.
	pub(crate) fn is_unsettled(&self) -> bool {
		self.unsettled.load(Ordering::Acquire)
	}

	fn check_settled(&self) -> std::io::Result<()> {
		if self.is_unsettled() {
			Err(AppliedLogFault::Unsettled.io())
		} else {
			Ok(())
		}
	}

	fn unsettle(&self, path: &Path, cause: &Error) {
		if !self.unsettled.swap(true, Ordering::AcqRel) {
			tracing::warn!(
				tenant_id = self.tenant.map(|tenant| tenant.get()),
				path = %path.display(),
				error = %cause,
				"counter witness store unsettled; failing closed until recovery"
			);
		}
	}

	fn permit(&self) -> std::io::Result<Option<crate::storage::ownership::WritePermit>> {
		crate::storage::ownership::DataDirectoryOwner::optional_permit(self.owner.as_ref())
	}

	fn lock_witnesses(&self) -> std::io::Result<std::sync::MutexGuard<'_, ()>> {
		self.witness_lock
			.lock()
			.map_err(|_| Error::other("counter witness lock poisoned"))
	}

	fn lock_applied(&self) -> std::io::Result<std::sync::MutexGuard<'_, AppliedLogState>> {
		self.applied
			.lock()
			.map_err(|_| Error::other("counter applied-log lock poisoned"))
	}

	fn path_for(&self, op_id: u64) -> PathBuf {
		self.dir.join(format!("counter-witness-{op_id:016x}.pw"))
	}

	fn origin_path_for(&self, op_id: u64) -> PathBuf {
		self.dir.join(format!("counter-witness-{op_id:016x}.pw2"))
	}

	fn witness_path(&self, witness: &StoredWitness) -> PathBuf {
		match witness.origin {
			Some(_) => self.origin_path_for(witness.op_id),
			None => self.path_for(witness.op_id),
		}
	}

	/// Publish a NEW witness before any participant commit: temp write, file
	/// sync, atomic rename, directory sync. On failure the operation is rolled
	/// back, so the partly published name is withdrawn (unlinked, then the
	/// directory synced) rather than left for a later directory sync to make
	/// durable. If the withdrawal itself fails, the witness may be durable: the
	/// store latches unsettled so nothing else is published or resolved before
	/// recovery decides that operation.
	fn write(&self, witness: &StoredWitness) -> std::io::Result<()> {
		let _permit = self.permit()?;
		self.check_settled()?;
		let _witness = self.lock_witnesses()?;
		let published = self.replace_locked(witness);
		if published.is_err() {
			if let Err(cleanup) = self.withdraw_locked(witness) {
				self.unsettle(&self.witness_path(witness), &cleanup);
			}
		}
		published
	}

	/// Unlink a failed publication's witness and temp names, then sync the
	/// directory. Every step is attempted; the first failure is returned.
	fn withdraw_locked(&self, witness: &StoredWitness) -> std::io::Result<()> {
		let mut first = None;
		for path in [
			self.witness_path(witness),
			self.temp_path_for(witness.op_id),
		] {
			match self.ops.unlink(&path) {
				Ok(()) => (),
				Err(error) if error.kind() == ErrorKind::NotFound => (),
				Err(error) => {
					first.get_or_insert(error);
				}
			}
		}
		if let Err(error) = self.ops.sync_dir(&self.dir) {
			first.get_or_insert(error);
		}
		first.map_or(Ok(()), Err)
	}

	/// Durably replace a published witness with its updated progress. A failure
	/// leaves the previous durable witness (or the new one) in place.
	fn rewrite(&self, witness: &StoredWitness) -> std::io::Result<()> {
		let _permit = self.permit()?;
		self.check_settled()?;
		let _witness = self.lock_witnesses()?;
		self.replace_locked(witness)
	}

	fn temp_path_for(&self, op_id: u64) -> PathBuf {
		self.dir.join(format!(".tmp-{op_id:016x}"))
	}

	fn replace_locked(&self, witness: &StoredWitness) -> std::io::Result<()> {
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
		if self.ops.exists(&other)? {
			return Err(CounterVisibilityError::Conflict.io());
		}
		let tmp = self.temp_path_for(witness.op_id);
		self.ops.write_file(&tmp, &bytes)?;
		self.ops.sync_all(&tmp)?;
		self.ops.rename(&tmp, &path)?;
		self.ops.sync_dir(&self.dir)
	}

	/// Unlink a resolved witness, then sync the directory. Only called after
	/// the op's applied record is durable; an error leaves the caller to retry.
	fn remove(&self, op_id: u64) -> std::io::Result<()> {
		let _permit = self.permit()?;
		let _witness = self.lock_witnesses()?;
		let old = self.path_for(op_id);
		let new = self.origin_path_for(op_id);
		if self.ops.exists(&old)? && self.ops.exists(&new)? {
			return Err(CounterVisibilityError::Conflict.io());
		}
		for path in [old, new] {
			match self.ops.unlink(&path) {
				Ok(()) => (),
				Err(error) if error.kind() == ErrorKind::NotFound => (),
				Err(error) => return Err(error),
			}
		}
		self.ops.sync_dir(&self.dir)
	}

	fn load_all(&self) -> std::io::Result<Vec<StoredWitness>> {
		let mut out = Vec::new();
		let mut seen = HashSet::new();
		for path in self.ops.read_dir(&self.dir)? {
			let extension = path.extension().and_then(|s| s.to_str());
			if !matches!(extension, Some("pw" | "pw2")) {
				continue;
			}
			let bytes = self.ops.read_file(&path)?;
			let w = if extension == Some("pw2") {
				witness::decode(&bytes)?
			} else {
				StoredWitness {
					origin: None,
					body: postcard::from_bytes::<CounterFinalizationWitness>(&bytes)
						.map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?,
				}
			};
			if path != self.witness_path(&w) || !seen.insert(w.op_id) {
				return Err(CounterVisibilityError::Conflict.io());
			}
			out.push(w);
		}
		Ok(out)
	}

	/// Append the durable D4 dedup record for a RESOLVED witness. Called by the
	/// commit driver after every accepted participant's marker is durable and
	/// BEFORE the witness is pruned, so the dedup identity is never windowless:
	/// pending ops replay from the witness, resolved ops from this log. The
	/// frame is appended, `sync_data`'d and, when this append created the log,
	/// its directory entry synced; only then may the witness be unlinked. A
	/// record already present (a retried resolution) is re-synced, not appended.
	/// Frames go to the current generation; compaction bounds the log (E3).
	fn append_applied(
		&self,
		applied: &AppliedCounterAdd,
		witness: &StoredWitness,
	) -> std::io::Result<()> {
		let _permit = self.permit()?;
		self.check_settled()?;
		let mut state = self.lock_applied()?;
		if !witness_matches_applied(witness, applied) || applied.key.as_ref() != Some(&witness.key)
		{
			return Err(CounterVisibilityError::Conflict.io());
		}
		if matches!(*state, AppliedLogState::Unloaded) {
			*state = AppliedLogState::Loaded(self.read_applied_image()?);
		}
		let AppliedLogState::Loaded(image) = &mut *state else {
			return Err(AppliedLogFault::Unsettled.io());
		};
		let path = self.generation_path(image.generation);
		match self.append_to_image(image, applied, witness) {
			Ok(()) => Ok(()),
			Err(AppendFailure::Retained(error)) => Err(error),
			Err(AppendFailure::Unsettled(error)) => {
				*state = AppliedLogState::Unsettled;
				self.unsettle(&path, &error);
				Err(error)
			}
		}
	}

	fn append_to_image(
		&self,
		image: &mut AppliedLogImage,
		applied: &AppliedCounterAdd,
		witness: &StoredWitness,
	) -> Result<(), AppendFailure> {
		let path = self.generation_path(image.generation);
		let observed_len = match self.ops.file_len(&path) {
			Ok(len) => Some(len),
			Err(error) if error.kind() == ErrorKind::NotFound => None,
			Err(error) => return Err(AppendFailure::Retained(error)),
		};
		if observed_len.unwrap_or(0) != image.file_len {
			tracing::warn!(
				tenant_id = self.tenant.map(|tenant| tenant.get()),
				path = %path.display(),
				expected_len = image.file_len,
				observed_len,
				"counter applied log changed outside its owner; failing closed"
			);
			return Err(AppendFailure::Retained(
				AppliedLogFault::Diverged {
					expected_len: image.file_len,
					observed_len,
				}
				.io(),
			));
		}
		if let Some(tail) = image.torn_tail.as_ref() {
			// Only a lazily loaded image can still hold a torn tail; recovery
			// repairs it first. It must be a prefix of this operation's own frame.
			if image.by_op(applied.op_id).is_some()
				|| !applied_log::tail_is_prefix_of(tail, applied)
			{
				return Err(AppendFailure::Retained(
					CounterVisibilityError::Conflict.io(),
				));
			}
			let valid_len = tail.valid_len;
			if valid_len < image.base_len {
				return Err(AppendFailure::Retained(
					CounterVisibilityError::Conflict.io(),
				));
			}
			self.ops
				.truncate(&path, valid_len)
				.and_then(|()| self.ops.sync_data(&path))
				.map_err(AppendFailure::Unsettled)?;
			image.torn_tail = None;
			image.file_len = valid_len;
		}
		if let Some(existing) = image.by_op(applied.op_id) {
			if !applied_log::same_applied(existing, applied) {
				return Err(AppendFailure::Retained(
					CounterVisibilityError::Conflict.io(),
				));
			}
			// A retried resolution: the frame exists, but it may have been read
			// back from a crashed process's unsynced page cache. Repeat the
			// barriers before the caller may unlink the witness.
			self.ops
				.sync_data(&path)
				.map_err(AppendFailure::Unsettled)?;
			return self.ensure_applied_name_durable(image);
		}
		if let Some(existing) = image.by_key(&applied.write_key) {
			return Err(AppendFailure::Retained(
				AppliedLogFault::IdentityConflict {
					write_key: applied.write_key,
					recorded_op_id: existing.op_id,
					conflicting_op_id: applied.op_id,
				}
				.io(),
			));
		}
		// A new record needs its witness still published: never append for an
		// operation whose prepare witness is gone. Only this check excludes a
		// concurrent witness publish or removal.
		let published = {
			let _witness = self.lock_witnesses().map_err(AppendFailure::Retained)?;
			self.ops.exists(&self.witness_path(witness))
		};
		match published {
			Ok(true) => (),
			Ok(false) => {
				return Err(AppendFailure::Retained(
					CounterVisibilityError::Conflict.io(),
				));
			}
			Err(error) => return Err(AppendFailure::Retained(error)),
		}
		let frame = applied_log::encode_frame(applied).map_err(AppendFailure::Retained)?;
		let created = self
			.ops
			.append_file(&path, &frame)
			.map_err(AppendFailure::Unsettled)?;
		if created {
			image.name_durable = false;
		}
		self.ops
			.sync_data(&path)
			.map_err(AppendFailure::Unsettled)?;
		image.push(applied.clone(), frame.len());
		self.ensure_applied_name_durable(image)
	}

	/// Sync the directory once after this store created the log. A failure
	/// keeps the frame (it is in the file and the image) and leaves the witness
	/// in place; a retried resolution repeats the barrier.
	fn ensure_applied_name_durable(
		&self,
		image: &mut AppliedLogImage,
	) -> Result<(), AppendFailure> {
		if !image.name_durable {
			self.ops
				.sync_dir(&self.dir)
				.map_err(AppendFailure::Retained)?;
			image.name_durable = true;
		}
		Ok(())
	}

	/// The current generation as it stands, without recovery's cleanup: the
	/// one `CURRENT` names, else generation 0 (§3.6).
	fn read_applied_image(&self) -> std::io::Result<AppliedLogImage> {
		let image = self.resolve_image(false)?;
		self.cutoff.fetch_max(image.cutoff, Ordering::Release);
		Ok(image)
	}

	/// Load the current generation for recovery (deleting stale generations
	/// and temp files), bind it to the pending witnesses, repair a torn final
	/// frame that a durable witness still represents (never below the
	/// generation's base), and make every byte recovery derives state from
	/// durable before it is served. The resulting image becomes the store's
	/// authoritative cache, and its cutoff `Q_mem`, before any add is served.
	fn recover_applied(&self, pending: &[StoredWitness]) -> std::io::Result<RecoveredApplied> {
		let _permit = self.permit()?;
		let mut state = self.lock_applied()?;
		let mut image = self.resolve_image(true)?;
		let path = self.generation_path(image.generation);
		applied_log::validate_pairs(&image, pending)?;
		if let Some(tail) = image.torn_tail.take() {
			if tail.valid_len < image.base_len {
				return Err(CounterVisibilityError::Conflict.io());
			}
			applied_log::check_torn_tail_owner(&image, &tail, pending)?;
			self.ops.truncate(&path, tail.valid_len)?;
			image.file_len = tail.valid_len;
		}
		if self.ops.exists(&path)? {
			self.ops.sync_data(&path)?;
		}
		let recovered = RecoveredApplied {
			records: image.records.clone(),
			high_water: image.high_water,
		};
		self.cutoff.store(image.cutoff, Ordering::Release);
		self.attempted_generation
			.fetch_max(image.generation, Ordering::Relaxed);
		let current = image.generation >= 1;
		*state = AppliedLogState::Loaded(image);
		if current {
			self.repoison_on_recovery(&mut state);
		}
		Ok(recovered)
	}

	#[cfg(test)]
	fn load_applied(&self) -> std::io::Result<Vec<AppliedCounterAdd>> {
		Ok(self.read_applied_image()?.records)
	}
}

/// What recovery read from the current generation: its records and the
/// operation-id high-water its header carries (0 for generation 0).
struct RecoveredApplied {
	records: Vec<AppliedCounterAdd>,
	high_water: u64,
}

/// How an append attempt failed: `Retained` leaves the file exactly as the
/// image describes it; `Unsettled` may have changed it, so the image is no
/// longer authoritative.
enum AppendFailure {
	Retained(Error),
	Unsettled(Error),
}

/// The leader's handle on its witness store. Every call runs the store's
/// blocking file I/O and barriers on the blocking pool, never on an async
/// worker. The store takes its owner write permit inside that blocking task,
/// so a cancelled caller cannot release admission before the I/O finishes.
struct OffloadedWitnessStore(Arc<DurableCounterWitnessStore>);

impl OffloadedWitnessStore {
	async fn run<T: Send + 'static>(
		&self,
		operation: impl FnOnce(&DurableCounterWitnessStore) -> std::io::Result<T> + Send + 'static,
	) -> std::io::Result<T> {
		let store = self.0.clone();
		tokio::task::spawn_blocking(move || operation(&store))
			.await
			.map_err(|error| Error::other(format!("counter witness store task failed: {error}")))?
	}

	async fn write(&self, witness: &StoredWitness) -> std::io::Result<()> {
		let witness = witness.clone();
		self.run(move |store| store.write(&witness)).await
	}

	async fn rewrite(&self, witness: StoredWitness) -> std::io::Result<()> {
		self.run(move |store| store.rewrite(&witness)).await
	}

	async fn append_applied(
		&self,
		applied: &AppliedCounterAdd,
		witness: &StoredWitness,
	) -> std::io::Result<()> {
		let (applied, witness) = (applied.clone(), witness.clone());
		self.run(move |store| store.append_applied(&applied, &witness))
			.await
	}

	async fn remove(&self, op_id: u64) -> std::io::Result<()> {
		self.run(move |store| store.remove(op_id)).await
	}
}

/// Test-only switches that disable one write-key mechanism, so a law test can
/// show the failure the mechanism prevents.
#[cfg(test)]
#[derive(Default)]
struct WriteKeySwitches {
	/// Admit every add without its write-key gate.
	bypass_gate: std::sync::atomic::AtomicBool,
	/// Never reclaim a released gate's map entry.
	keep_gate_entries: std::sync::atomic::AtomicBool,
	/// Run the add's body in the caller's future instead of its own task.
	inline_body: std::sync::atomic::AtomicBool,
}

/// Test-only switches that disable one dedup-horizon rule (E1), so a law test
/// can show the failure the rule prevents.
#[cfg(test)]
#[derive(Default)]
struct RetentionSwitches {
	/// Evaluate E1 before the record lookup instead of after it.
	expiry_before_lookup: std::sync::atomic::AtomicBool,
	/// E1 ignores `Q_mem` and refuses by `now - E` alone.
	ignore_cutoff: std::sync::atomic::AtomicBool,
	/// Parks the next add between its write-key gate and its record lookup.
	lookup_park: StdMutex<Option<Arc<LookupPark>>>,
}

/// A parked add: `reached` fires when it parks; it resumes once `release` is
/// notified.
#[cfg(test)]
#[derive(Default)]
struct LookupPark {
	reached: tokio::sync::Notify,
	release: tokio::sync::Notify,
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
	/// The add each canonical public write-key names, with its outcome (D4). A
	/// retry of that add replays the original `previous`/`current` without
	/// re-applying; any other add under the key is refused (see `write_key`).
	/// The in-memory view of the durable record (witness while pending, applied
	/// log once resolved) — `recover` rebuilds it, so both survive restart.
	idempotency: DashMap<CounterWriteKey, RecordedAdd>,
	/// One add in flight per write key, by the exact 24 bytes (never striped or
	/// hashed). Lock order: write-key gate, then the counter-key lock, then the
	/// participant op gates, then the store's file mutexes; reconcile and the
	/// commit driver take neither leader gate.
	write_gates: WriteKeyGates,
	/// In-memory mirror of the pending witnesses (durable in `witness_store`).
	witnesses: DashMap<u64, StoredWitness>,
	/// Durable backing for the witnesses (survives leader restart).
	witness_store: OffloadedWitnessStore,
	/// Leader-assigned op-id allocator (cross-node stable handles). `commit_hlc`
	/// is derived from it, so op-ids and HLCs advance together.
	op_seq: AtomicU64,
	/// The dedup horizon (H, S, E) E0, E1 and compaction apply.
	retention: CounterRetentionConfig,
	/// The node's wall clock, read by E0, E1 and compaction.
	clock: Arc<dyn PhysicalClock>,
	/// The latest issuance over the records loaded at recovery and every add
	/// accepted since: compaction's clock never runs past it plus E.
	max_issued_accepted: AtomicU64,
	compaction: compaction::CompactionSchedule,
	#[cfg(test)]
	write_key_switches: WriteKeySwitches,
	#[cfg(test)]
	retention_switches: RetentionSwitches,
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
			write_gates: DashMap::new(),
			witnesses: DashMap::new(),
			witness_store: OffloadedWitnessStore(Arc::new(witness_store)),
			op_seq: AtomicU64::new(1),
			retention: CounterRetentionConfig::default(),
			clock: default_leader_clock(),
			max_issued_accepted: AtomicU64::new(0),
			compaction: compaction::CompactionSchedule::default(),
			#[cfg(test)]
			write_key_switches: WriteKeySwitches::default(),
			#[cfg(test)]
			retention_switches: RetentionSwitches::default(),
		}
	}

	/// The dedup horizon and the wall clock this leader applies. A leader built
	/// without it uses the default horizon and the system clock.
	pub fn with_retention(
		mut self,
		retention: CounterRetentionConfig,
		clock: Arc<dyn PhysicalClock>,
	) -> Self {
		self.retention = retention;
		self.clock = clock;
		self
	}

	#[cfg(test)]
	pub(crate) fn set_next_op_id_for_test(&self, next: u64) {
		self.op_seq.store(next, Ordering::Relaxed);
	}

	/// Open a leader and LOAD its durable state (restart path): the applied D4
	/// dedup log and the pending witnesses — BOTH feed the idempotency map, so a
	/// duplicate retry after restart replays instead of re-adding (a pending
	/// witness is an add that WILL complete, so its outcome already replays),
	/// and a different add under a recorded write key is refused. A witness
	/// names its counter key, so it also keys an unkeyed record of its own
	/// operation. Two operations under one write key, in any mix of records and
	/// witnesses, fail recovery closed with a typed identity conflict.
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
		let pending = witness_store.load_all()?;
		validate_witness_set(&pending)?;
		if pending
			.iter()
			.any(|witness| witness.origin.is_some_and(|origin| origin != local_id))
		{
			return Err(CounterVisibilityError::Conflict.io());
		}
		let applied = witness_store.recover_applied(&pending)?;
		let leader = Self::new(local_id, local, is_primary, witness_store);
		// The generation header carries the high-water of every operation it
		// compacted away; frames and witnesses carry the rest.
		let mut max_op = applied.high_water;
		let mut max_issued = 0;
		for record in applied.records {
			max_op = max_op.max(record.op_id);
			max_issued = max_issued.max(retention::issued_ms(&record.write_key).unwrap_or(0));
			let recorded = RecordedAdd::from_durable(record.key, record.previous, record.current)?;
			leader.idempotency.insert(record.write_key, recorded);
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
			max_issued = max_issued.max(retention::issued_ms(&witness.write_key).unwrap_or(0));
			let recorded = RecordedAdd::from_durable(
				Some(witness.key.clone()),
				witness.previous,
				witness.value,
			)?;
			leader.idempotency.insert(witness.write_key, recorded);
			leader.witnesses.insert(witness.op_id, witness);
		}
		let next_op = max_op.checked_add(1).ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				"counter operation id high-water exhausted",
			)
		})?;
		leader.op_seq.store(next_op, Ordering::Relaxed);
		leader
			.max_issued_accepted
			.store(max_issued, Ordering::Relaxed);
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
}

/// The add itself runs on a spawned task, so its participants must be `'static`.
impl<P: CounterParticipant + Clone + Send + Sync + 'static> CounterLeader<P> {
	/// Atomic idempotent `+delta` on `key`, gated by the five boundaries above.
	/// Keyed by the canonical public `write_key` (D4): a write key names one add,
	/// a retry of that add replays it, and any other add under the key is refused
	/// (`WriteKeyConflict`). Requests under one write key are admitted one at a
	/// time, and the dedup record is durable with the witness/applied log, so
	/// replay and refusal survive restart.
	pub async fn fetch_add(
		self: &Arc<Self>,
		plan: CounterParticipantPlan<P>,
		write_key: CounterWriteKey,
		key: &CounterKey,
		delta: u64,
	) -> Result<CounterAddResult, CounterLeaderError> {
		self.fetch_add_fenced(plan, write_key, key, delta, || async {})
			.await
	}

	/// [`fetch_add`](Self::fetch_add), running `fence` on the add's own task when
	/// the add leaves the dedup store latched unsettled. Everything after the
	/// primary and eligibility checks runs on that task, which owns the
	/// write-key gate and the counter-key lock: a dropped caller cancels no step
	/// and releases no guard (K5), so a durable witness is never left without its
	/// record, its pending entry and its published value, and the fence is never
	/// skipped.
	pub async fn fetch_add_fenced<F, Fence>(
		self: &Arc<Self>,
		plan: CounterParticipantPlan<P>,
		write_key: CounterWriteKey,
		key: &CounterKey,
		delta: u64,
		fence: F,
	) -> Result<CounterAddResult, CounterLeaderError>
	where
		F: FnOnce() -> Fence + Send + 'static,
		Fence: std::future::Future<Output = ()> + Send,
	{
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
		// E0, before the write-key gate: only a v7 identity issued no further
		// ahead than the skew allowance may name an add.
		self.refuse_unpresentable(&write_key)?;
		let leader = self.clone();
		let key = key.clone();
		let body = async move {
			let result = leader.admitted_add(plan, write_key, &key, delta).await;
			if leader.durability_unsettled() {
				fence().await;
			}
			result
		};
		#[cfg(test)]
		if self.write_key_switches.inline_body.load(Ordering::Relaxed) {
			return body.await;
		}
		match tokio::spawn(body).await {
			Ok(result) => result,
			Err(error) => {
				let payload = if error.is_panic() {
					let panic = error.into_panic();
					panic
						.downcast_ref::<&str>()
						.map(|message| (*message).to_owned())
						.or_else(|| panic.downcast_ref::<String>().cloned())
						.unwrap_or_else(|| "non-string panic payload".to_owned())
				} else {
					error.to_string()
				};
				tracing::error!(
					write_key = ?write_key,
					error = %payload,
					"counter add task failed; answering internal error"
				);
				Err(CounterLeaderError::Internal)
			}
		}
	}

	/// The add's body, on its own task. Lock order: write-key gate, then the
	/// counter-key lock, both owned by this task until it ends.
	async fn admitted_add(
		&self,
		plan: CounterParticipantPlan<P>,
		write_key: CounterWriteKey,
		key: &CounterKey,
		delta: u64,
	) -> Result<CounterAddResult, CounterLeaderError> {
		// One add per write key in flight, admitted before the record lookup so no
		// concurrent add under this key can install a record after the lookup
		// misses. Taken before, never while holding, the counter-key lock.
		let _write_gate = self.admit_write_key(write_key).await;
		#[cfg(test)]
		self.before_record_lookup(&write_key).await?;
		// (3) Duplicate replay BEFORE lock/mutation (mandatory). The record
		// lookup, then E1, then the record's answer: an expired write key is
		// refused whether or not its record survives; a known one replays only
		// the add it names; any other add under it, a zero delta included, is
		// refused with nothing applied.
		if let Some(outcome) = self.recorded_answer(&write_key, key, delta)? {
			return Ok(CounterAddResult::Replayed(outcome));
		}
		let Some(recorded_delta) = NonZeroU64::new(delta) else {
			return Err(CounterLeaderError::ZeroDelta);
		};
		// (1/2) Per-key lock across read → fan-out → quorum → commit/rollback.
		let lock = self.locks.entry(key.clone()).or_default().clone();
		let _key_guard = lock.lock_owned().await;

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
		// A failed publish answers `Internal`, but that one answer is in doubt when
		// the store could not withdraw the half-published witness (it latches
		// unsettled): recovery may then find the witness and complete the add, and
		// a same-key retry after restart replays it rather than applying it twice.
		if self.witness_store.write(&witness).await.is_err() {
			for &id in &accepted {
				if let Some(participant) = plan.get(id) {
					participant.rollback(op_id, key).await;
				}
			}
			return Err(CounterLeaderError::Internal);
		}
		// (3/4) Install point: the witness is durable, so record the add it names
		// before anything can fail. Every later exit leaves the write key recorded;
		// no record is ever removed. The DURABLE record is the witness itself,
		// compacted to the applied log by `drive_commits`.
		self.witnesses.insert(op_id, witness);
		if let Some(issued) = retention::issued_ms(&write_key) {
			self.max_issued_accepted
				.fetch_max(issued, Ordering::Relaxed);
		}
		let outcome = CounterAddOutcome { previous, current };
		self.idempotency.insert(
			write_key,
			RecordedAdd {
				key: Some(key.clone()),
				delta: recorded_delta,
				outcome,
			},
		);
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
		// The value reached quorum and is either fully committed or
		// recoverable-pending (the durable witness will finish it) — never abandoned.
		Ok(CounterAddResult::Applied(outcome))
	}
}

impl<P: CounterParticipant + Clone> CounterLeader<P> {
	/// Admit one request under `write_key`, waiting while another holds it.
	async fn admit_write_key(&self, write_key: CounterWriteKey) -> WriteKeyGate<'_> {
		#[cfg(test)]
		if self.write_key_switches.bypass_gate.load(Ordering::Relaxed) {
			return WriteKeyGate::bypassed(&self.write_gates, write_key);
		}
		let gate = WriteKeyGate::admit(&self.write_gates, write_key).await;
		#[cfg(test)]
		if self
			.write_key_switches
			.keep_gate_entries
			.load(Ordering::Relaxed)
		{
			return gate.keep_entry();
		}
		gate
	}

	/// The recorded answer for `write_key`: the record lookup, then E1 against
	/// the cutoff and clock read after it, then the record's own answer (its
	/// outcome for a retry of that add, a conflict for any other). Compaction
	/// publishes `Q_mem` before it evicts, so a lookup that misses an evicted
	/// record reads a cutoff that covers it. The map reference ends here.
	fn recorded_answer(
		&self,
		write_key: &CounterWriteKey,
		key: &CounterKey,
		delta: u64,
	) -> Result<Option<CounterAddOutcome>, CounterLeaderError> {
		let recorded = self
			.idempotency
			.get(write_key)
			.map(|recorded| recorded.clone());
		self.refuse_expired(write_key)?;
		recorded
			.map(|recorded| recorded.answer(key, delta))
			.transpose()
	}

	/// E0: refuse a write key that is not a v7 identity issued no more than
	/// the skew allowance ahead of this node's clock.
	fn refuse_unpresentable(&self, write_key: &CounterWriteKey) -> Result<(), CounterLeaderError> {
		if retention::presentable(write_key, self.clock.now_ms(), &self.retention) {
			Ok(())
		} else {
			Err(CounterLeaderError::InvalidWriteIdentity)
		}
	}

	/// E1, after the record lookup.
	fn refuse_expired(&self, write_key: &CounterWriteKey) -> Result<(), CounterLeaderError> {
		#[cfg(test)]
		if self
			.retention_switches
			.expiry_before_lookup
			.load(Ordering::Relaxed)
		{
			return Ok(());
		}
		self.check_unexpired(write_key)
	}

	/// Refuse a write key issued at or before `max(Q_mem, now - E)`. `Q_mem` is
	/// read first (Acquire), then the clock.
	fn check_unexpired(&self, write_key: &CounterWriteKey) -> Result<(), CounterLeaderError> {
		let issued =
			retention::issued_ms(write_key).ok_or(CounterLeaderError::InvalidWriteIdentity)?;
		let cutoff = self.witness_store.0.cutoff();
		#[cfg(test)]
		let cutoff = if self
			.retention_switches
			.ignore_cutoff
			.load(Ordering::Relaxed)
		{
			0
		} else {
			cutoff
		};
		if issued <= retention::expiry_cutoff(cutoff, self.clock.now_ms(), &self.retention) {
			Err(CounterLeaderError::OperationExpired)
		} else {
			Ok(())
		}
	}

	/// Test hooks between the write-key gate and the record lookup: E1 moved
	/// ahead of the lookup (a negative control), then an armed park.
	#[cfg(test)]
	async fn before_record_lookup(
		&self,
		write_key: &CounterWriteKey,
	) -> Result<(), CounterLeaderError> {
		if self
			.retention_switches
			.expiry_before_lookup
			.load(Ordering::Relaxed)
		{
			self.check_unexpired(write_key)?;
		}
		let park = self.retention_switches.lookup_park.lock().unwrap().take();
		if let Some(park) = park {
			park.reached.notify_one();
			park.release.notified().await;
		}
		Ok(())
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
					let progress = witness.clone();
					// Release the shard lock before the durable rewrite. A failed or
					// reordered progress rewrite keeps an older durable witness;
					// re-driving an already-committed participant is idempotent.
					drop(witness);
					let _ = self.witness_store.rewrite(progress).await;
				}
			}
		}
		let resolved = self.witnesses.get(&op_id).and_then(|witness| {
			let complete = witness
				.accepted
				.iter()
				.all(|i| witness.committed.contains(i));
			complete.then(|| {
				(
					AppliedCounterAdd {
						op_id: witness.op_id,
						write_key: witness.write_key,
						key: Some(witness.key.clone()),
						previous: witness.previous,
						current: witness.value,
					},
					witness.clone(),
				)
			})
		});
		if let Some((applied, witness)) = resolved {
			// Compact the resolved witness into the durable applied log BEFORE
			// pruning it, so the D4 dedup record is never windowless: a crash
			// here leaves witness AND log entry (recover dedups by write-key);
			// a crash before leaves the witness. An append failure keeps the
			// witness pending — the reconciler retries the whole resolution.
			if self
				.witness_store
				.append_applied(&applied, &witness)
				.await
				.is_err()
			{
				return;
			}
			// Keep the volatile pending witness until the unlink and containing
			// directory barrier both succeed. If either fails, the applied record
			// remains authoritative and a retry safely repeats cleanup.
			if self.witness_store.remove(op_id).await.is_ok() {
				self.witnesses.remove(&op_id);
			}
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

	/// Whether this leader's dedup store has latched fail-closed. The data
	/// server trips its local-persistence distrust fence when it has.
	pub(crate) fn durability_unsettled(&self) -> bool {
		self.witness_store.0.is_unsettled()
	}

	/// Make the next compaction check due.
	#[cfg(test)]
	pub(crate) fn force_compaction_due_for_test(&self) {
		self.compaction.force_due.store(true, Ordering::Relaxed);
	}

	/// The applied-log generation the store serves (0 before any compaction).
	#[cfg(test)]
	pub(crate) fn served_generation_for_test(&self) -> Option<u64> {
		match &*self.witness_store.0.lock_applied().ok()? {
			AppliedLogState::Loaded(image) => Some(image.generation),
			_ => None,
		}
	}

	/// Latch the dedup store as if a persistence barrier had failed.
	#[cfg(test)]
	pub(crate) fn latch_unsettled_for_test(&self) {
		let store = &self.witness_store.0;
		store.unsettle(&store.dir, &Error::other("test-only persistence failure"));
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
	use std::path::Path;
	use std::time::Duration;

	fn slab_dir() -> crate::test_support::TestDir {
		crate::test_support::TestDir::new("wendelon-adr075")
	}

	async fn open_store(path: &Path) -> Arc<LocalSlabStore> {
		open_store_with_ops(path, Arc::new(BlockingFileOps)).await
	}

	async fn open_store_with_ops(
		path: &Path,
		file_ops: Arc<dyn crate::storage::AsyncFileOps>,
	) -> Arc<LocalSlabStore> {
		Arc::new(
			LocalSlabStore::open(
				path,
				SlabId::ZERO,
				file_ops,
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

	#[tokio::test]
	async fn ctr6_failed_prepare_sync_leaves_no_index_pending_or_recovered_value() {
		use crate::storage::{FaultAction, FaultOp, FaultyFileOps, InjectedFault};
		for replica_arm in [false, true] {
			let dir = slab_dir();
			let path = dir.join("slab_0000000000000000.wnd");
			let ops = Arc::new(FaultyFileOps::new(dir.to_path_buf()).unwrap());
			let store = open_store_with_ops(&path, ops.clone()).await;
			let tenant = TenantId::new(7);
			let origin = NodeId {
				id: if replica_arm { 21 } else { 20 },
				_reserved: 0,
			};
			let key = ckey(if replica_arm {
				b"ctr6-replica-sync"
			} else {
				b"ctr6-local-sync"
			});
			let identity = counter_identity(&key);
			ops.inject(InjectedFault::on(
				FaultOp::Fdatasync,
				FaultAction::Error {
					kind: std::io::ErrorKind::Other,
					message: "CTR-6 injected tentative fdatasync failure".into(),
				},
			));
			if replica_arm {
				let replica =
					CounterReplica::new(SlabCounterStore::new(store.clone(), tenant, origin));
				assert!(
					replica
						.replicate_tentative(901, &key, 42, h(901))
						.await
						.is_err()
				);
				assert_eq!(
					replica.pending_count(),
					0,
					"failed prepare created pending state"
				);
			} else {
				let local = SlabCounterStore::new(store.clone(), tenant, origin);
				assert!(local.tentative_append(&key, 42, h(902)).await.is_err());
			}
			assert_eq!(store.counter_lookup(tenant, identity).unwrap(), None);
			drop(store);
			ops.crash().unwrap();
			let reopened = open_store_with_ops(&path, ops).await;
			assert_eq!(reopened.counter_lookup(tenant, identity).unwrap(), None);
			assert!(reopened.recovered_counter_tentatives_v2.is_empty());
		}
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
			counter.rollback_tentative(t2).await.unwrap();
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
			counter.rollback_tentative(t2).await.unwrap();
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
			.unwrap()
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
	use std::path::Path;
	use std::time::Duration;

	fn h(ms: u64) -> HybridTimestamp {
		HybridTimestamp {
			physical_ms: ms,
			logical: 0,
			node_id: 0,
		}
	}

	fn slab_dir() -> crate::test_support::TestDir {
		crate::test_support::TestDir::new("wendelon-adr075-replica")
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
		async fn counter_unindex(
			&self,
			tenant_id: TenantId,
			key_identity: Identity,
			location: RecordLocation,
		) -> std::io::Result<()> {
			self.inner
				.counter_unindex(tenant_id, key_identity, location)
				.await
		}
		fn counter_lookup(
			&self,
			tenant_id: TenantId,
			key_identity: Identity,
		) -> std::io::Result<Option<RecordLocation>> {
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
		r.rollback(1).await.unwrap();
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
			r.rollback(2).await.unwrap();
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

/// The wall clock of a leader built without `with_retention`.
#[cfg(not(test))]
fn default_leader_clock() -> Arc<dyn PhysicalClock> {
	Arc::new(SystemClock)
}

/// In unit tests, a leader built without `with_retention` runs on a clock
/// frozen at the test identities' issuance, so a [`test_write_key`] never
/// ages past E however slow the lane. Tests of the horizon itself set their
/// own clock.
#[cfg(test)]
fn default_leader_clock() -> Arc<dyn PhysicalClock> {
	struct TestIdentityClock;
	impl PhysicalClock for TestIdentityClock {
		fn now_ms(&self) -> u64 {
			test_identity_issued_ms()
		}
	}
	Arc::new(TestIdentityClock)
}

#[cfg(test)]
fn test_identity_issued_ms() -> u64 {
	static ISSUED: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
	*ISSUED.get_or_init(|| SystemClock.now_ms())
}

/// A test write key: a UUIDv7 client uuid issued at the test identities'
/// issuance (so a key presented again, after a restart say, is the same key),
/// with `n` as the sequence number. Leaders on the default test clock never
/// see it age.
#[cfg(test)]
pub(crate) fn test_write_key(n: u64) -> CounterWriteKey {
	test_write_key_at(test_identity_issued_ms(), n)
}

/// A test write key whose UUIDv7 uuid was issued at `issued_ms`, sequence `n`.
#[cfg(test)]
pub(crate) fn test_write_key_at(issued_ms: u64, n: u64) -> CounterWriteKey {
	let mut key = [0u8; 24];
	key[..6].copy_from_slice(&issued_ms.to_be_bytes()[2..]);
	key[6] = 0x70;
	key[8] = 0x80;
	key[16..].copy_from_slice(&n.to_le_bytes());
	key
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

	fn slab_dir() -> crate::test_support::TestDir {
		crate::test_support::TestDir::new("wendelon-adr075-leader")
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

	/// A fresh durable witness store inside the test's scratch dir (most fixtures
	/// don't restart; the restart fixture builds its own shared-dir store).
	fn wstore(dir: &Path) -> DurableCounterWitnessStore {
		DurableCounterWitnessStore::open(dir.join("wstore")).unwrap()
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

	/// Canonical write-key fixture: a UUIDv7 client uuid issued at this
	/// process's first use, with the test's small id as the sequence number.
	fn wk(n: u64) -> CounterWriteKey {
		test_write_key(n)
	}

	fn wk_at(issued_ms: u64, n: u64) -> CounterWriteKey {
		test_write_key_at(issued_ms, n)
	}

	/// A participant over a real store, with deterministic fault injection.
	#[derive(Clone)]
	struct TestParticipant {
		replica: Arc<CounterReplica<LocalSlabStore>>,
		fail_tentative: Arc<AtomicBool>,
		/// The next `commit` fails with a visibility error (the key is poisoned).
		fail_commit_visibility: Arc<AtomicBool>,
		/// Every `replicate_tentative` call that reached this participant.
		tentatives: Arc<StdMutex<Vec<(u64, CounterKey)>>>,
		/// Parks the next `replicate_tentative` until the test releases it.
		park: Arc<StdMutex<Option<Arc<CallPark>>>>,
		/// Parks the next `commit` until the test releases it.
		commit_park: Arc<StdMutex<Option<Arc<CallPark>>>>,
		observer: Option<Arc<dyn ParticipantObserver>>,
	}

	/// A parked participant call: `reached` fires when the call parks, and the
	/// call resumes once `release` is notified.
	#[derive(Default)]
	struct CallPark {
		reached: tokio::sync::Notify,
		release: tokio::sync::Notify,
	}

	/// Sees each participant step, so a crash model can order participant
	/// markers against the leader's persistence and stop the world at a cut.
	trait ParticipantObserver: Send + Sync {
		/// The leader process crashed: it reaches no participant any more.
		fn crashed(&self) -> bool;
		fn commit_started(&self);
		/// The participant's commit marker is durable and acknowledged.
		fn marker_durable(&self);
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
				fail_commit_visibility: Arc::new(AtomicBool::new(false)),
				tentatives: Arc::default(),
				park: Arc::default(),
				commit_park: Arc::default(),
				observer: None,
			}
		}

		fn over_observed(
			store: Arc<LocalSlabStore>,
			observer: Arc<dyn ParticipantObserver>,
		) -> Self {
			let mut participant = Self::over(store);
			participant.observer = Some(observer);
			participant
		}

		fn leader_crashed(&self) -> bool {
			self.observer
				.as_ref()
				.is_some_and(|observer| observer.crashed())
		}
		/// The next `replicate_tentative` fails (excluded from the accepted set).
		fn arm_tentative_failure(&self) {
			self.fail_tentative.store(true, Ordering::Relaxed);
		}
		/// The next `commit` fails its durable marker write (partial commit).
		fn arm_commit_failure(&self) {
			self.replica.arm_commit_marker_failure();
		}
		fn lose_pending_for_test(&self, op_id: u64) {
			self.replica.pending.remove(&op_id);
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
			if self.leader_crashed() || self.fail_tentative.swap(false, Ordering::Relaxed) {
				return Err(ParticipantError::Unavailable);
			}
			self.tentatives.lock().unwrap().push((op_id, key.clone()));
			let park = self.park.lock().unwrap().take();
			if let Some(park) = park {
				park.reached.notify_one();
				park.release.notified().await;
			}
			self.replica
				.replicate_tentative(op_id, key, value, commit_hlc)
				.await
				.map_err(|_| ParticipantError::Unavailable)
		}
		async fn commit(&self, op_id: u64, _key: &CounterKey) -> Result<(), ParticipantError> {
			if self.leader_crashed() {
				return Err(ParticipantError::Unavailable);
			}
			let park = self.commit_park.lock().unwrap().take();
			if let Some(park) = park {
				park.reached.notify_one();
				park.release.notified().await;
			}
			if self.fail_commit_visibility.swap(false, Ordering::Relaxed) {
				return Err(ParticipantError::Visibility(
					CounterVisibilityError::Poisoned,
				));
			}
			if let Some(observer) = &self.observer {
				observer.commit_started();
			}
			self.replica
				.commit(op_id)
				.await
				.map_err(|_| ParticipantError::Unavailable)?;
			if let Some(observer) = &self.observer {
				observer.marker_durable();
			}
			Ok(())
		}
		async fn rollback(&self, op_id: u64, _key: &CounterKey) {
			if self.leader_crashed() {
				return;
			}
			let _ = self.replica.rollback(op_id).await;
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
			let Some(location) = slab.counter_lookup(TenantId::new(7), identity).unwrap() else {
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
		leader: Arc<CounterLeader<TestParticipant>>,
		participants: Vec<(NodeId, TestParticipant)>,
		quorum: usize,
	}

	/// A wall clock a test sets and steps (E0, E1 and compaction read it).
	#[derive(Default)]
	struct TestClock {
		now_ms: AtomicU64,
	}

	impl TestClock {
		fn at(now_ms: u64) -> Arc<Self> {
			Arc::new(Self {
				now_ms: AtomicU64::new(now_ms),
			})
		}

		fn set(&self, now_ms: u64) {
			self.now_ms.store(now_ms, Ordering::SeqCst);
		}
	}

	impl PhysicalClock for TestClock {
		fn now_ms(&self) -> u64 {
			self.now_ms.load(Ordering::SeqCst)
		}
	}

	impl LeaderHarness {
		fn new(
			participants: Vec<TestParticipant>,
			quorum: usize,
			is_primary: bool,
			witness_store: DurableCounterWitnessStore,
		) -> Self {
			Self::new_on(participants, quorum, is_primary, witness_store, None)
		}

		/// A leader on `clock` (the system clock when `None`) with the default
		/// dedup horizon.
		fn new_on(
			participants: Vec<TestParticipant>,
			quorum: usize,
			is_primary: bool,
			witness_store: DurableCounterWitnessStore,
			clock: Option<Arc<TestClock>>,
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
				leader: Arc::new(Self::on_clock(leader, clock)),
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
			Self::recover_on(participants, quorum, is_primary, witness_store, None)
		}

		fn recover_on(
			participants: Vec<TestParticipant>,
			quorum: usize,
			is_primary: bool,
			witness_store: DurableCounterWitnessStore,
			clock: Option<Arc<TestClock>>,
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
				leader: Arc::new(Self::on_clock(leader, clock)),
				participants,
				quorum,
			})
		}

		fn on_clock(
			leader: CounterLeader<TestParticipant>,
			clock: Option<Arc<TestClock>>,
		) -> CounterLeader<TestParticipant> {
			match clock {
				Some(clock) => leader.with_retention(CounterRetentionConfig::default(), clock),
				None => leader,
			}
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
		/// Arm a participant's next commit to fail with a visibility error.
		fn arm_participant_commit_visibility_failure(&self, i: usize) {
			self.participants[i]
				.1
				.fail_commit_visibility
				.store(true, Ordering::Relaxed);
		}
		/// Park a participant's next `replicate_tentative`.
		fn park_participant_tentative(&self, i: usize) -> Arc<CallPark> {
			let park = Arc::new(CallPark::default());
			*self.participants[i].1.park.lock().unwrap() = Some(park.clone());
			park
		}
		/// Park a participant's next `commit`.
		fn park_participant_commit(&self, i: usize) -> Arc<CallPark> {
			let park = Arc::new(CallPark::default());
			*self.participants[i].1.commit_park.lock().unwrap() = Some(park.clone());
			park
		}
		/// The `(op_id, key)` of every `replicate_tentative` a participant saw.
		fn participant_tentatives(&self, i: usize) -> Vec<(u64, CounterKey)> {
			self.participants[i].1.tentatives.lock().unwrap().clone()
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
		let leader = LeaderHarness::new(ps, 2, true, wstore(&dir));
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
		let leader = LeaderHarness::new(ps, 2, true, wstore(&dir));
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
		let leader = LeaderHarness::new(ps, 1, false, wstore(&dir));
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
		let leader = LeaderHarness::new(ps, 3, true, wstore(&dir)); // need all 3
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
		let leader = LeaderHarness::new(ps, 2, true, wstore(&dir)); // quorum 2 of 3, all accept
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
		let leader =
			LeaderHarness::new(vec![TestParticipant::over(store_a2)], 1, true, wstore(&dir));
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
		let leader = LeaderHarness::new(ps, 2, true, wstore(&dir));
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

	#[tokio::test]
	async fn ctr6_redrive_reprepares_lost_replica_pending_before_commit_and_keeps_witness() {
		let dir = slab_dir();
		let wdir = dir.join("witnesses");
		let (ps, paths) = participants(&dir, 2).await;
		let store = DurableCounterWitnessStore::open(&wdir).unwrap();
		let leader = LeaderHarness::new(ps, 2, true, store);
		let key = ckey(b"ctr6-redrive");
		leader.arm_participant_commit_failure(1);
		let applied = leader.fetch_add(wk(1), &key, 5).await.unwrap();
		assert_eq!(applied.outcome().current, 5);
		assert_eq!(leader.pending_witnesses(), 1);
		assert_eq!(
			DurableCounterWitnessStore::open(&wdir)
				.unwrap()
				.load_all()
				.unwrap()
				.len(),
			1,
			"the accepted but uncommitted participant keeps the witness"
		);

		// Model loss of the replica's volatile pending entry while its tentative
		// bytes remain. The re-drive must restore the matching prepare first.
		leader.participants[1].1.lose_pending_for_test(1);
		leader.reconcile().await;
		assert_eq!(leader.pending_witnesses(), 0);
		for path in &paths {
			assert_eq!(read_after_restart(path, &key).await, Some(5));
		}
		assert!(
			DurableCounterWitnessStore::open(&wdir)
				.unwrap()
				.load_all()
				.unwrap()
				.is_empty(),
			"the witness is pruned only after the re-prepared replica commits"
		);
	}

	// ADR 076 D3 discriminator: pending witnesses re-drive the recorded peer
	// identities, while a later fresh add can use a different current-ring
	// participant set. A positional re-drive would commit the old witness on the
	// new peer (2) instead of the recorded peer (1).
	#[tokio::test]
	async fn recorded_redrive_and_fresh_routing_are_distinct_after_ring_shift() {
		let dir = slab_dir();
		let (ps, paths) = participants(&dir, 3).await;
		let leader = LeaderHarness::new(ps, 2, true, wstore(&dir));
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
		let leader = LeaderHarness::new(ps, 2, true, wstore(&dir));
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
		let leader = LeaderHarness::new(ps, 2, true, wstore(&dir));
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
				.unwrap()
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
			.unwrap()
			.expect("the fresh add's version is indexed")
			.hlc;
		assert!(
			hlc_after.physical_ms > hlc_before.physical_ms,
			"the post-restart add's commit HLC must be STRICTLY newer (got {} after {} — an op-id was reused)",
			hlc_after.physical_ms,
			hlc_before.physical_ms
		);
	}

	mod crash_cut_tests;
	mod write_key_reuse_tests;

	mod throughput_measurement;
}

#[cfg(test)]
#[path = "counter/replica_scope_tests.rs"]
mod replica_scope_tests;
