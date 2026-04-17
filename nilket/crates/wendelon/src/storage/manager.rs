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

use crate::consensus::MigratedRangeTombstone;
use crate::schema::HybridTimestamp;
use crate::storage::core::{
	LocalSlabStore, RecordLocation, StorageEngine, insert_version_sorted, newest_version_as_of,
	remove_version_exact,
};
use crate::storage::io_uring::IoRingHandle;
use crate::storage::l2::ObjectStore;
use async_trait::async_trait;
use bytes::Bytes;
use dashmap::DashMap;
use std::collections::{HashMap, HashSet};
use std::io::{Error, ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::Instant;

/// Construction parameters for [`SlabManager`].
pub struct SlabManagerConfig {
	/// Directory that will hold `slab_<id>.wnd` files.
	pub dir: PathBuf,
	/// Size threshold at which the active slab is sealed and a new
	/// one is opened. Measured against [`LocalSlabStore::write_cursor`].
	pub max_slab_bytes: u64,
	/// Submission handle for the shared `io_uring` thread. Cloned
	/// into every slab the manager opens.
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
}

impl Default for RehydrationPolicy {
	fn default() -> Self {
		Self {
			promote_after_l2_reads: 3,
			min_local_residency: Duration::from_secs(60),
		}
	}
}

/// Summary of a sealed slab, handed out by
/// [`SlabManager::list_sealed_slabs`] to the background sweeper.
/// `size_bytes` is the current write cursor — slabs only ever
/// shrink by being tiered out, so this is the upper bound on the
/// payload the sweeper will upload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SealedSlabInfo {
	pub slab_id: u64,
	pub size_bytes: u64,
}

/// Summary of a locally rehydrated slab. The authoritative tier
/// metadata still points at L2; this struct only describes the
/// temporary L1 cache copy and the cooldown that protects it from
/// immediate reclamation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RehydratedSlabInfo {
	pub slab_id: u64,
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
	/// A matching `(key_hash, hlc)` with identical `payload_hash`
	/// already existed locally — the ingest was a no-op.
	Duplicate(RecordLocation),
}

/// A slab that has been uploaded to L2. The manager tracks this
/// so subsequent reads pivot to the object store instead of the
/// now-unlinked L1 file.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TieredSlabPointer {
	pub slab_id: u64,
	pub l2_uri: String,
	pub payload_hash: [u8; 32],
}

/// Outcome of one tombstone-aware reclaim pass.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TombstoneCompactionReport {
	pub compacted_slabs: usize,
	pub reclaimed_bytes: u64,
}

/// Multi-slab composition of [`StorageEngine`].
pub struct SlabManager {
	config: SlabManagerConfig,
	state: Mutex<ManagerState>,
	/// Authoritative key → version chain, aggregated across all
	/// active and sealed slabs. Per ADR 013 the chain stores every
	/// version sorted ascending by HLC; the tail is the current
	/// value and earlier entries back time-travel reads.
	key_index: DashMap<u64, Vec<RecordLocation>>,
	/// Best-effort compatibility map for in-flight readers that
	/// raced a compaction rewrite. New lookups pivot to the fresh
	/// location through `key_index`; a stale snapshot of the prior
	/// location follows this chain to the replacement slab.
	relocated_reads: DashMap<RecordLocation, RecordLocation>,
}

#[derive(Clone)]
struct RehydratedSlabState {
	slab: Arc<LocalSlabStore>,
	keep_local_until: Instant,
}

struct ManagerState {
	active: Arc<LocalSlabStore>,
	/// Sealed slabs keyed by `slab_id`. Read-only from the
	/// manager's perspective and eligible for tiering to L2 via
	/// the background sweeper. An entry leaves this map only when
	/// [`SlabManager::apply_tier_slab`] has moved the slab's
	/// backing from L1 to `tiered`.
	sealed: DashMap<u64, Arc<LocalSlabStore>>,
	/// Slabs whose payload now lives in L2. Keyed by `slab_id`;
	/// the value is the URI the read path hands to
	/// [`ObjectStore::get`]. Ordinary tiering removes the slab from
	/// `sealed` before inserting here; a later rehydration may add
	/// a separate local cache copy in `rehydrated` while L2 stays
	/// authoritative.
	tiered: DashMap<u64, TieredSlabPointer>,
	/// Local L1 cache copies of slabs whose authoritative bytes
	/// still live in L2. Kept outside `sealed` so compaction and
	/// ordinary L1 -> L2 demotion never mistake a cache copy for a
	/// new authoritative slab.
	rehydrated: DashMap<u64, RehydratedSlabState>,
	/// Consecutive reads that had to fetch a slab from L2.
	hot_read_counts: HashMap<u64, u32>,
	/// Slabs currently being restored from L2 to L1. Guards the
	/// hot-read path against duplicate local installs.
	rehydrating: HashSet<u64>,
	next_slab_id: u64,
}

impl SlabManager {
	async fn append_on_target(
		&self,
		key_hash: u64,
		hlc: HybridTimestamp,
		frame: &[u8],
	) -> std::io::Result<(Arc<LocalSlabStore>, RecordLocation)> {
		let slab = self.target_for_append().await?;
		let location = slab.append(key_hash, hlc, frame).await?;
		Ok((slab, location))
	}

	fn index_appended_location(&self, key_hash: u64, location: RecordLocation) {
		self.key_index
			.entry(key_hash)
			.and_modify(|versions| insert_version_sorted(versions, location))
			.or_insert_with(|| vec![location]);
	}

	fn rollback_indexed_location(&self, key_hash: u64, location: RecordLocation) {
		let should_remove_key = if let Some(mut versions) = self.key_index.get_mut(&key_hash) {
			let removed = remove_version_exact(&mut versions, location);
			removed && versions.is_empty()
		} else {
			false
		};
		if should_remove_key {
			self.key_index.remove(&key_hash);
		}
	}

	/// Append a record and force the destination slab's durability
	/// barrier before returning. The manager index is published only
	/// after the `fsync` succeeds, so callers that require durable
	/// visibility do not leak append-complete-but-unflushed writes
	/// through the in-memory read path.
	pub async fn append_durable(
		&self,
		key_hash: u64,
		hlc: HybridTimestamp,
		frame: &[u8],
	) -> std::io::Result<RecordLocation> {
		let (slab, location) = self.append_on_target(key_hash, hlc, frame).await?;
		if let Err(err) = slab.fsync().await {
			slab.rollback_index_entry(key_hash, location);
			self.rollback_indexed_location(key_hash, location);
			return Err(err);
		}
		self.index_appended_location(key_hash, location);
		Ok(location)
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
	/// * **One or more existing slabs**: opens all of them
	///   (recovering each slab's on-disk records into its local
	///   index), treats the highest-numbered slab as active, seals
	///   the rest, and sets `next_slab_id` to `max_id + 1`.
	///
	/// All recovered records are folded into the manager's
	/// aggregated `key_index`. Last-write-wins across slabs, ordered
	/// by slab id — so a rewrite that landed in a newer slab
	/// shadows the stale copy in an older one.
	pub fn open(config: SlabManagerConfig) -> std::io::Result<Self> {
		if !config.dir.is_dir() {
			return Err(Error::new(
				ErrorKind::NotFound,
				format!(
					"SlabManager directory does not exist: {}",
					config.dir.display()
				),
			));
		}

		let mut existing_ids = discover_slab_ids(&config.dir)?;
		existing_ids.sort_unstable();

		let key_index: DashMap<u64, Vec<RecordLocation>> = DashMap::new();
		let sealed: DashMap<u64, Arc<LocalSlabStore>> = DashMap::new();
		let tiered: DashMap<u64, TieredSlabPointer> = DashMap::new();
		let rehydrated: DashMap<u64, RehydratedSlabState> = DashMap::new();

		let (active, next_slab_id) = if existing_ids.is_empty() {
			let id: u64 = 0;
			let slab = Arc::new(LocalSlabStore::open(
				slab_path(&config.dir, id),
				id,
				config.io.clone(),
			)?);
			drain_slab_index(&slab, &key_index);
			(slab, id + 1)
		} else {
			// Split: everything except the max becomes sealed, the
			// max becomes active. Drain into key_index in ascending
			// id order so later writes shadow earlier ones.
			let (&active_id, sealed_ids) = existing_ids.split_last().expect("non-empty");
			for &id in sealed_ids {
				let slab = Arc::new(LocalSlabStore::open(
					slab_path(&config.dir, id),
					id,
					config.io.clone(),
				)?);
				drain_slab_index(&slab, &key_index);
				sealed.insert(id, slab);
			}
			let active = Arc::new(LocalSlabStore::open(
				slab_path(&config.dir, active_id),
				active_id,
				config.io.clone(),
			)?);
			drain_slab_index(&active, &key_index);
			(active, active_id + 1)
		};

		Ok(Self {
			config,
			state: Mutex::new(ManagerState {
				active,
				sealed,
				tiered,
				rehydrated,
				hot_read_counts: HashMap::new(),
				rehydrating: HashSet::new(),
				next_slab_id,
			}),
			key_index,
			relocated_reads: DashMap::new(),
		})
	}

	/// Test / diagnostic accessor: current active slab id.
	pub async fn active_slab_id(&self) -> u64 {
		self.state.lock().await.active.slab_id()
	}

	/// Test / diagnostic accessor: number of sealed slabs.
	pub async fn sealed_count(&self) -> usize {
		self.state.lock().await.sealed.len()
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
	pub async fn read_sealed_raw(&self, slab_id: u64) -> std::io::Result<Vec<u8>> {
		{
			let state = self.state.lock().await;
			if !state.sealed.contains_key(&slab_id) {
				return Err(Error::new(
					ErrorKind::NotFound,
					format!("slab {slab_id} is not in the sealed set"),
				));
			}
		}
		// The L1 file lives at the canonical path; read via the
		// normal filesystem surface. O_DIRECT alignment is only
		// relevant for the `io_uring` append / lookup path.
		let path = slab_path(&self.config.dir, slab_id);
		std::fs::read(path)
	}

	/// Move a slab's backing from the sealed L1 set to the tiered
	/// set. On return:
	///
	/// * The slab is no longer in `sealed` — future
	///   [`StorageEngine::read`]s route to L2 via the stored URI.
	/// * The on-disk `slab_<id>.wnd` file has been unlinked.
	/// * Any read that grabbed an `Arc<LocalSlabStore>` *before*
	///   this call still holds its own reference, so its open fd
	///   survives the unlink (Linux semantics) — zero downtime
	///   for readers mid-flight.
	pub async fn apply_tier_slab(&self, pointer: TieredSlabPointer) -> std::io::Result<()> {
		let mut state = self.state.lock().await;
		// Idempotent: a replay of the Raft TierSlab entry must not
		// fail. If we're already tiered, accept the no-op.
		if state.tiered.contains_key(&pointer.slab_id) {
			return Ok(());
		}
		let _old = state.sealed.remove(&pointer.slab_id);
		let _old = state.rehydrated.remove(&pointer.slab_id);
		state.tiered.insert(pointer.slab_id, pointer.clone());
		state.hot_read_counts.remove(&pointer.slab_id);
		state.rehydrating.remove(&pointer.slab_id);
		// Unlink the L1 file. Open fds held by in-flight reads
		// continue to work; new reads find the entry in `tiered`
		// and go to L2.
		let path = slab_path(&self.config.dir, pointer.slab_id);
		match std::fs::remove_file(&path) {
			Ok(()) => {}
			Err(e) if e.kind() == ErrorKind::NotFound => {}
			Err(e) => return Err(e),
		}
		Ok(())
	}

	/// Whether a slab has been pivoted to L2. Cheap — only hits
	/// the tiered DashMap.
	pub async fn is_tiered(&self, slab_id: u64) -> bool {
		self.state.lock().await.tiered.contains_key(&slab_id)
	}

	/// Whether a tiered slab currently also has a rehydrated local
	/// copy in L1.
	pub async fn is_rehydrated(&self, slab_id: u64) -> bool {
		self.state.lock().await.rehydrated.contains_key(&slab_id)
	}

	/// Lookup the L2 pointer for a tiered slab.
	pub async fn tier_pointer(&self, slab_id: u64) -> Option<TieredSlabPointer> {
		self.state
			.lock()
			.await
			.tiered
			.get(&slab_id)
			.map(|p| p.clone())
	}

	/// Best-effort reclaim of one rehydrated local cache copy. The
	/// authoritative L2 pointer remains intact; only the temporary
	/// L1 file is removed.
	pub async fn drop_rehydrated_slab(&self, slab_id: u64) -> std::io::Result<bool> {
		let removed = {
			let state = self.state.lock().await;
			if state.rehydrating.contains(&slab_id) {
				return Ok(false);
			}
			state.rehydrated.remove(&slab_id).map(|(_, state)| state)
		};
		let Some(_state) = removed else {
			return Ok(false);
		};
		let path = slab_path(&self.config.dir, slab_id);
		match std::fs::remove_file(&path) {
			Ok(()) => {}
			Err(err) if err.kind() == ErrorKind::NotFound => {}
			Err(err) => return Err(err),
		}
		Ok(true)
	}

	/// Enumerate every live `(key_hash, RecordLocation)` whose hash
	/// falls in the inclusive window `[start_hash, end_hash]`. Used
	/// by the Data Ring query path (ADR 005) to feed the WPBC
	/// interpreter; the server then reads each location via the
	/// normal `StorageEngine::read` surface, which transparently
	/// pivots L1 vs. L2.
	///
	/// The returned pairs are sorted by `key_hash` so the scan is
	/// deterministic — useful for debugging and for the client's
	/// pagination cursor, which is keyed on the highest observed
	/// hash. Every key yields only its latest version; use
	/// [`Self::scan_as_of`] for time-travel scans.
	pub fn scan(&self, start_hash: u64, end_hash: u64) -> Vec<(u64, RecordLocation)> {
		self.scan_as_of(
			start_hash,
			end_hash,
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
		start_hash: u64,
		end_hash: u64,
		as_of_hlc: HybridTimestamp,
	) -> Vec<(u64, RecordLocation)> {
		if start_hash > end_hash {
			return Vec::new();
		}
		let mut out: Vec<(u64, RecordLocation)> = self
			.key_index
			.iter()
			.filter_map(|entry| {
				let h = *entry.key();
				if h < start_hash || h > end_hash {
					return None;
				}
				newest_version_as_of(entry.value(), as_of_hlc).map(|loc| (h, loc))
			})
			.collect();
		out.sort_by_key(|(h, _)| *h);
		out
	}

	/// Every retained version whose `key_hash` falls in the
	/// inclusive window `[start_hash, end_hash]` and whose `hlc`
	/// is at or above `migration_hlc_floor` (Plan 019).
	///
	/// Unlike [`Self::scan`] / [`Self::scan_as_of`], this does not
	/// collapse per-key history — every version ≥ floor is
	/// returned. Output is sorted first by `key_hash` then by
	/// ascending `hlc` so the migration RPC can paginate with a
	/// stable cursor and the destination can append in the same
	/// order it observed on the source.
	pub fn scan_all_versions(
		&self,
		start_hash: u64,
		end_hash: u64,
		migration_hlc_floor: HybridTimestamp,
	) -> Vec<(u64, RecordLocation)> {
		self.scan_all_versions_from(
			start_hash,
			end_hash,
			migration_hlc_floor,
			0,
			bytemuck::Zeroable::zeroed(),
		)
	}

	/// Version-aware paging variant of [`Self::scan_all_versions`].
	///
	/// Lets `handle_migrate` resume mid-key when a fat key has more
	/// retained versions than fit in a single page:
	///
	/// * When `(cursor_hash, cursor_hlc)` is `(0, zero)` the scan
	///   behaves identically to `scan_all_versions`.
	/// * Otherwise, for `key_hash == cursor_hash` we emit only
	///   versions strictly later than `cursor_hlc` — the prior page
	///   already emitted everything at or below it.
	/// * For `key_hash > cursor_hash` we emit every version at or
	///   above the pinned floor, exactly like the non-paged scan.
	/// * Keys strictly less than `cursor_hash` are skipped: the
	///   prior page already drained them.
	///
	/// Output is still sorted `(key_hash ASC, hlc ASC)` so the
	/// migration RPC can paginate deterministically.
	pub fn scan_all_versions_from(
		&self,
		start_hash: u64,
		end_hash: u64,
		migration_hlc_floor: HybridTimestamp,
		cursor_hash: u64,
		cursor_hlc: HybridTimestamp,
	) -> Vec<(u64, RecordLocation)> {
		if start_hash > end_hash {
			return Vec::new();
		}
		let cursor_active = cursor_hash != 0 || cursor_hlc != bytemuck::Zeroable::zeroed();
		let mut out: Vec<(u64, RecordLocation)> = Vec::new();
		for entry in self.key_index.iter() {
			let h = *entry.key();
			if h < start_hash || h > end_hash {
				continue;
			}
			if cursor_active && h < cursor_hash {
				continue;
			}
			for loc in entry.value().iter() {
				if loc.hlc < migration_hlc_floor {
					continue;
				}
				if cursor_active && h == cursor_hash && loc.hlc <= cursor_hlc {
					continue;
				}
				out.push((h, *loc));
			}
		}
		out.sort_by(|(ha, la), (hb, lb)| ha.cmp(hb).then(la.hlc.cmp(&lb.hlc)));
		out
	}

	/// Idempotent migration ingest (Plan 019).
	///
	/// Distinct from [`StorageEngine::append`] so the correctness
	/// boundary for lazy migration lives at a dedicated API rather
	/// than piggybacking on the hot write path:
	///
	/// * **Fast path** — if a version with the same `(key_hash, hlc)`
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
		key_hash: u64,
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
			.get(&key_hash)
			.and_then(|chain| chain.iter().find(|loc| loc.hlc == hlc).copied());

		if let Some(location) = existing {
			// Slow path: read the on-disk frame and hash it. If
			// the incoming payload really is a byte-identical copy
			// of what we already stored, the migration pull was
			// a duplicate and can be dropped.
			let stored = self.read(location).await?;
			let stored_hash: [u8; 32] = *blake3::hash(&stored).as_bytes();
			if stored_hash == payload_hash {
				return Ok(MigrationIngestOutcome::Duplicate(location));
			}
			// Different bytes claim the same MVCC slot. Refuse
			// loudly — accepting either side would make historical
			// reads depend on which replica answered the pull.
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"append_migration payload_hash mismatch: migration_id={migration_id} \
                     key_hash={key_hash:#x} hlc={hlc:?} stored_hash={stored_hash:?} \
                     incoming_hash={payload_hash:?}"
				),
			));
		}

		// No prior version at this exact HLC — take the normal
		// append path. `StorageEngine::append` handles rotation,
		// disk ack, and folding the new version into the
		// multi-version index.
		let location = self.append(key_hash, hlc, frame).await?;
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
		let next_slab = Arc::new(LocalSlabStore::open(
			&next_path,
			next_id,
			self.config.io.clone(),
		)?);

		let sealed_slab = std::mem::replace(&mut state.active, next_slab.clone());
		state.sealed.insert(sealed_slab.slab_id(), sealed_slab);
		state.next_slab_id = next_id + 1;

		Ok(next_slab)
	}

	/// Resolve the authoritative backing for a slab, which may be
	/// either a local `Arc<LocalSlabStore>` (active / sealed) or
	/// an L2 pointer (tiered). Tiered wins over sealed so a
	/// post-pivot read never races back against the soon-to-be-
	/// unlinked L1 file.
	async fn resolve_backing(&self, slab_id: u64) -> SlabBacking {
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

	pub async fn reclaim_tombstoned_ranges(
		&self,
		tombstones: &[MigratedRangeTombstone],
		max_slabs: usize,
	) -> std::io::Result<TombstoneCompactionReport> {
		if tombstones.is_empty() || max_slabs == 0 {
			return Ok(TombstoneCompactionReport::default());
		}

		let sealed = {
			let state = self.state.lock().await;
			let mut slabs = state
				.sealed
				.iter()
				.map(|entry| (*entry.key(), entry.value().clone()))
				.collect::<Vec<_>>();
			slabs.sort_by_key(|(slab_id, _)| *slab_id);
			slabs
		};

		let mut report = TombstoneCompactionReport::default();
		for (slab_id, slab) in sealed {
			if report.compacted_slabs >= max_slabs {
				break;
			}
			if let Some(outcome) = self
				.compact_one_sealed_slab(slab_id, slab, tombstones)
				.await?
			{
				report.compacted_slabs += 1;
				report.reclaimed_bytes = report
					.reclaimed_bytes
					.saturating_add(outcome.reclaimed_bytes);
			}
		}
		Ok(report)
	}
}

/// Authoritative location for a slab, as observed by the read
/// path. Keeping it an enum means the "is the file still here?"
/// question is answered with the same lock acquisition that
/// resolves the backing.
enum SlabBacking {
	Local(Arc<LocalSlabStore>),
	L2(TieredSlabPointer),
	Missing,
}

#[async_trait]
impl StorageEngine for SlabManager {
	async fn append(
		&self,
		key_hash: u64,
		hlc: HybridTimestamp,
		frame: &[u8],
	) -> std::io::Result<RecordLocation> {
		let (_slab, location) = self.append_on_target(key_hash, hlc, frame).await?;
		// The per-slab `append` already pushed the new version onto
		// its local chain; mirror it into the manager's
		// authoritative multi-version index so `lookup` /
		// `lookup_as_of` stay O(1) without cross-slab scanning.
		self.index_appended_location(key_hash, location);
		Ok(location)
	}

	async fn read(&self, location: RecordLocation) -> std::io::Result<Vec<u8>> {
		let location = self.relocate_read_location(location);
		match self.resolve_backing(location.slab_id).await {
			SlabBacking::Local(slab) => slab.read(location).await,
			SlabBacking::L2(pointer) => self.read_from_l2(&pointer, location).await,
			SlabBacking::Missing => Err(Error::new(
				ErrorKind::NotFound,
				format!("no slab with id {} under this manager", location.slab_id),
			)),
		}
	}

	fn lookup(&self, key_hash: u64) -> Option<RecordLocation> {
		self.key_index
			.get(&key_hash)
			.and_then(|v| v.last().copied())
	}

	fn lookup_as_of(&self, key_hash: u64, as_of_hlc: HybridTimestamp) -> Option<RecordLocation> {
		let versions = self.key_index.get(&key_hash)?;
		newest_version_as_of(versions.value(), as_of_hlc)
	}
}

impl SlabManager {
	async fn reserve_slab_id(&self) -> u64 {
		let mut state = self.state.lock().await;
		let next_id = state.next_slab_id;
		state.next_slab_id = state.next_slab_id.saturating_add(1);
		next_id
	}

	async fn compact_one_sealed_slab(
		&self,
		slab_id: u64,
		slab: Arc<LocalSlabStore>,
		tombstones: &[MigratedRangeTombstone],
	) -> std::io::Result<Option<CompactionOutcome>> {
		let mut records: Vec<CompactionRecord> = slab
			.index
			.iter()
			.flat_map(|entry| {
				let key_hash = *entry.key();
				entry
					.value()
					.iter()
					.map(move |loc| CompactionRecord {
						key_hash,
						old_location: *loc,
						reclaim: tombstones.iter().any(|tombstone| {
							key_hash >= tombstone.range_start
								&& key_hash <= tombstone.range_end
								&& loc.hlc >= tombstone.migration_hlc_floor
						}),
					})
					.collect::<Vec<_>>()
			})
			.collect();
		records.sort_by_key(|record| record.old_location.offset);

		if records.is_empty() || records.iter().all(|record| !record.reclaim) {
			return Ok(None);
		}

		let old_bytes = slab.write_cursor();
		let survivors = records.iter().filter(|record| !record.reclaim).count();
		if survivors == 0 {
			self.finish_compaction_without_replacement(slab_id, &records)
				.await?;
			return Ok(Some(CompactionOutcome {
				reclaimed_bytes: old_bytes,
			}));
		}

		let replacement_id = self.reserve_slab_id().await;
		let replacement_path = slab_path(&self.config.dir, replacement_id);
		let replacement = Arc::new(LocalSlabStore::open(
			&replacement_path,
			replacement_id,
			self.config.io.clone(),
		)?);
		let mut relocations = Vec::with_capacity(survivors);
		for record in records.iter().filter(|record| !record.reclaim) {
			let frame = slab.read(record.old_location).await?;
			let new_location = replacement
				.append(record.key_hash, record.old_location.hlc, &frame)
				.await?;
			relocations.push(RelocatedRecord {
				key_hash: record.key_hash,
				old_location: record.old_location,
				new_location,
			});
		}
		replacement.fsync().await?;
		let new_bytes = replacement.write_cursor();

		if !self
			.finish_compaction_with_replacement(
				slab_id,
				replacement_id,
				replacement.clone(),
				&records,
				&relocations,
			)
			.await?
		{
			let path = slab_path(&self.config.dir, replacement_id);
			let _ = std::fs::remove_file(path);
			return Ok(None);
		}

		let old_path = slab_path(&self.config.dir, slab_id);
		match std::fs::remove_file(&old_path) {
			Ok(()) => {}
			Err(err) if err.kind() == ErrorKind::NotFound => {}
			Err(err) => return Err(err),
		}

		Ok(Some(CompactionOutcome {
			reclaimed_bytes: old_bytes.saturating_sub(new_bytes),
		}))
	}

	async fn finish_compaction_without_replacement(
		&self,
		slab_id: u64,
		records: &[CompactionRecord],
	) -> std::io::Result<()> {
		let state = self.state.lock().await;
		if state.sealed.remove(&slab_id).is_none() {
			return Ok(());
		}
		for record in records {
			self.rollback_indexed_location(record.key_hash, record.old_location);
		}
		let old_path = slab_path(&self.config.dir, slab_id);
		match std::fs::remove_file(&old_path) {
			Ok(()) => {}
			Err(err) if err.kind() == ErrorKind::NotFound => {}
			Err(err) => return Err(err),
		}
		Ok(())
	}

	async fn finish_compaction_with_replacement(
		&self,
		old_slab_id: u64,
		new_slab_id: u64,
		replacement: Arc<LocalSlabStore>,
		records: &[CompactionRecord],
		relocations: &[RelocatedRecord],
	) -> std::io::Result<bool> {
		let state = self.state.lock().await;
		if !state.sealed.contains_key(&old_slab_id) || state.tiered.contains_key(&old_slab_id) {
			return Ok(false);
		}
		state.sealed.remove(&old_slab_id);
		state.sealed.insert(new_slab_id, replacement);
		for record in records {
			self.rollback_indexed_location(record.key_hash, record.old_location);
		}
		for relocation in relocations {
			self.index_appended_location(relocation.key_hash, relocation.new_location);
			self.relocated_reads
				.insert(relocation.old_location, relocation.new_location);
		}
		Ok(true)
	}

	/// Fetch a single record from L2, then decide whether that L2
	/// read was hot enough to justify rehydrating the whole slab
	/// back into local L1 storage.
	async fn read_from_l2(
		&self,
		pointer: &TieredSlabPointer,
		location: RecordLocation,
	) -> std::io::Result<Vec<u8>> {
		let l2 = self.config.l2.as_ref().ok_or_else(|| {
			Error::new(
				ErrorKind::NotFound,
				format!(
					"slab {} is tiered but no ObjectStore configured on this manager",
					pointer.slab_id
				),
			)
		})?;
		let blob = l2.get_bytes(&pointer.l2_uri).await?;
		validate_tiered_blob(pointer, blob.as_ref())?;
		let frame = extract_record_from_blob(blob.as_ref(), location)?;
		if self.note_l2_read(pointer.slab_id).await {
			if let Err(err) = self
				.rehydrate_tiered_slab(pointer.clone(), blob.clone())
				.await
			{
				tracing::warn!(
					slab_id = pointer.slab_id,
					error = %err,
					"rehydration failed; continuing to serve slab from L2"
				);
			}
		}
		Ok(frame)
	}

	async fn note_l2_read(&self, slab_id: u64) -> bool {
		let promote_after = self.config.rehydration.promote_after_l2_reads;
		if promote_after == 0 {
			return false;
		}
		let mut state = self.state.lock().await;
		if state.rehydrated.contains_key(&slab_id) || state.rehydrating.contains(&slab_id) {
			return false;
		}
		let hits = state.hot_read_counts.entry(slab_id).or_insert(0);
		*hits = hits.saturating_add(1);
		if *hits < promote_after {
			return false;
		}
		state.rehydrating.insert(slab_id);
		true
	}

	async fn rehydrate_tiered_slab(
		&self,
		pointer: TieredSlabPointer,
		blob: Bytes,
	) -> std::io::Result<()> {
		let result = self.install_rehydrated_local_copy(pointer.slab_id, blob.as_ref());
		let mut state = self.state.lock().await;
		state.rehydrating.remove(&pointer.slab_id);
		match result {
			Ok(installed) => {
				state.hot_read_counts.remove(&pointer.slab_id);
				state.rehydrated.insert(pointer.slab_id, installed);
				Ok(())
			}
			Err(err) => {
				let retry_floor = self
					.config
					.rehydration
					.promote_after_l2_reads
					.saturating_sub(1);
				if retry_floor == 0 {
					state.hot_read_counts.remove(&pointer.slab_id);
				} else {
					state.hot_read_counts.insert(pointer.slab_id, retry_floor);
				}
				Err(err)
			}
		}
	}

	fn install_rehydrated_local_copy(
		&self,
		slab_id: u64,
		blob: &[u8],
	) -> std::io::Result<RehydratedSlabState> {
		let path = slab_path(&self.config.dir, slab_id);
		let tmp_path = rehydrate_tmp_path(&self.config.dir, slab_id);
		match std::fs::remove_file(&tmp_path) {
			Ok(()) => {}
			Err(err) if err.kind() == ErrorKind::NotFound => {}
			Err(err) => return Err(err),
		}
		let mut tmp = std::fs::File::create(&tmp_path)?;
		tmp.write_all(blob)?;
		tmp.sync_all()?;
		std::fs::rename(&tmp_path, &path)?;
		let slab = Arc::new(LocalSlabStore::open(
			&path,
			slab_id,
			self.config.io.clone(),
		)?);
		Ok(RehydratedSlabState {
			slab,
			keep_local_until: Instant::now() + self.config.rehydration.min_local_residency,
		})
	}
}

#[derive(Clone, Copy)]
struct CompactionRecord {
	key_hash: u64,
	old_location: RecordLocation,
	reclaim: bool,
}

#[derive(Clone, Copy)]
struct RelocatedRecord {
	key_hash: u64,
	old_location: RecordLocation,
	new_location: RecordLocation,
}

#[derive(Clone, Copy)]
struct CompactionOutcome {
	reclaimed_bytes: u64,
}

// ============================================================
//  HELPERS
// ============================================================

fn validate_tiered_blob(pointer: &TieredSlabPointer, blob: &[u8]) -> std::io::Result<()> {
	let actual = *blake3::hash(blob).as_bytes();
	if actual != pointer.payload_hash {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"L2 payload hash mismatch for slab {}: expected {:02x?}, got {:02x?}",
				pointer.slab_id,
				&pointer.payload_hash[..4],
				&actual[..4],
			),
		));
	}
	Ok(())
}

fn extract_record_from_blob(blob: &[u8], location: RecordLocation) -> std::io::Result<Vec<u8>> {
	let offset = location.offset as usize;
	let length = location.length as usize;
	let header_size = std::mem::size_of::<crate::schema::SlabRecordHeader>();
	if blob.len() < offset + header_size {
		return Err(Error::new(
			ErrorKind::UnexpectedEof,
			"L2 blob shorter than record header",
		));
	}
	let header: &crate::schema::SlabRecordHeader =
		bytemuck::from_bytes(&blob[offset..offset + header_size]);
	let frame_len = header.frame_len as usize;
	let end = offset
		.checked_add(header_size)
		.and_then(|h| h.checked_add(frame_len))
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "frame_len overflows usize"))?;
	if end > blob.len() || header_size + frame_len > length {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"L2 record extends past downloaded blob",
		));
	}
	Ok(blob[offset + header_size..end].to_vec())
}

fn slab_path(dir: &Path, slab_id: u64) -> PathBuf {
	dir.join(format!("slab_{:016x}.wnd", slab_id))
}

fn rehydrate_tmp_path(dir: &Path, slab_id: u64) -> PathBuf {
	dir.join(format!("slab_{:016x}.wnd.rehydrate.tmp", slab_id))
}

/// Parse a filename of the form `slab_<16-hex>.wnd` into a slab id.
/// Returns `None` for anything that does not match so the scan can
/// silently skip unrelated files — operators may reasonably drop
/// things like `.DS_Store` or log files into the directory.
fn parse_slab_filename(name: &str) -> Option<u64> {
	let rest = name.strip_prefix("slab_")?.strip_suffix(".wnd")?;
	if rest.len() != 16 {
		return None;
	}
	u64::from_str_radix(rest, 16).ok()
}

/// Enumerate every `slab_<hex>.wnd` file in `dir` and return their
/// ids. Non-matching entries are ignored; I/O errors propagate.
fn discover_slab_ids(dir: &Path) -> std::io::Result<Vec<u64>> {
	let mut ids = Vec::new();
	for entry in std::fs::read_dir(dir)? {
		let entry = entry?;
		if !entry.file_type()?.is_file() {
			continue;
		}
		if let Some(name) = entry.file_name().to_str() {
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
fn drain_slab_index(slab: &LocalSlabStore, key_index: &DashMap<u64, Vec<RecordLocation>>) {
	for entry in slab.index.iter() {
		let key = *entry.key();
		for loc in entry.value().iter() {
			key_index
				.entry(key)
				.and_modify(|versions| insert_version_sorted(versions, *loc))
				.or_insert_with(|| vec![*loc]);
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::storage::io_uring::spawn_io_thread;

	fn fresh_dir(tag: &str) -> std::path::PathBuf {
		let dir = std::env::temp_dir().join(format!(
			"wendelon-manager-{tag}-{}-{}",
			std::process::id(),
			uuid::Uuid::new_v4()
		));
		std::fs::create_dir_all(&dir).unwrap();
		dir
	}

	fn hlc(ms: u64) -> HybridTimestamp {
		HybridTimestamp {
			physical_ms: ms,
			logical: 0,
			node_id: 1,
		}
	}

	async fn fresh_manager() -> Arc<SlabManager> {
		let dir = fresh_dir("mv");
		let io = spawn_io_thread(64).unwrap();
		Arc::new(
			SlabManager::open(SlabManagerConfig {
				dir,
				// Large enough that no rotation fires during tests;
				// we want every version in the same active slab so
				// the test targets the multi-version index
				// semantics rather than cross-slab drain behaviour.
				max_slab_bytes: 1024 * 1024,
				io,
				l2: None,
				rehydration: RehydrationPolicy::default(),
			})
			.unwrap(),
		)
	}

	#[tokio::test]
	async fn lookup_returns_latest_version_after_multiple_appends() {
		let mgr = fresh_manager().await;
		let key: u64 = 42;
		mgr.append(key, hlc(10), b"v1").await.unwrap();
		mgr.append(key, hlc(20), b"v2").await.unwrap();
		mgr.append(key, hlc(30), b"v3").await.unwrap();

		let latest = mgr.lookup(key).expect("key present");
		assert_eq!(latest.hlc.physical_ms, 30);
	}

	#[tokio::test]
	async fn lookup_as_of_returns_historical_version() {
		let mgr = fresh_manager().await;
		let key: u64 = 7;
		mgr.append(key, hlc(10), b"v1").await.unwrap();
		mgr.append(key, hlc(20), b"v2").await.unwrap();
		mgr.append(key, hlc(30), b"v3").await.unwrap();

		assert_eq!(mgr.lookup_as_of(key, hlc(15)).unwrap().hlc.physical_ms, 10);
		assert_eq!(mgr.lookup_as_of(key, hlc(20)).unwrap().hlc.physical_ms, 20);
		assert_eq!(mgr.lookup_as_of(key, hlc(25)).unwrap().hlc.physical_ms, 20);
		// Bound before the first write -> nothing visible yet.
		assert!(mgr.lookup_as_of(key, hlc(5)).is_none());
	}

	#[tokio::test]
	async fn scan_all_versions_returns_every_retained_version_above_floor() {
		let mgr = fresh_manager().await;
		// Two keys, two versions each — one above floor, one below.
		mgr.append(1, hlc(5), b"a@5").await.unwrap();
		mgr.append(1, hlc(50), b"a@50").await.unwrap();
		mgr.append(2, hlc(20), b"b@20").await.unwrap();
		mgr.append(2, hlc(200), b"b@200").await.unwrap();

		let hits = mgr.scan_all_versions(0, u64::MAX, hlc(10));
		// Floor at 10 drops (1, hlc=5); the remaining three survive.
		assert_eq!(hits.len(), 3);
		// Sorted first by key_hash, then by hlc ascending.
		assert_eq!(hits[0].0, 1);
		assert_eq!(hits[0].1.hlc.physical_ms, 50);
		assert_eq!(hits[1].0, 2);
		assert_eq!(hits[1].1.hlc.physical_ms, 20);
		assert_eq!(hits[2].0, 2);
		assert_eq!(hits[2].1.hlc.physical_ms, 200);

		// Inverted window → empty rather than panic.
		assert!(mgr.scan_all_versions(10, 5, hlc(0)).is_empty());
	}

	#[tokio::test]
	async fn append_migration_is_idempotent_on_identical_payload() {
		let mgr = fresh_manager().await;
		let key: u64 = 42;
		let ts = hlc(100);
		let frame = b"payload-bytes";
		let hash: [u8; 32] = *blake3::hash(frame).as_bytes();

		let first = mgr.append_migration(7, key, ts, hash, frame).await.unwrap();
		assert!(matches!(first, MigrationIngestOutcome::Inserted(_)));

		let second = mgr.append_migration(7, key, ts, hash, frame).await.unwrap();
		assert!(matches!(second, MigrationIngestOutcome::Duplicate(_)));

		// Only one version at this (key, hlc) should exist.
		let versions = mgr.scan_all_versions(key, key, hlc(0));
		assert_eq!(versions.len(), 1);
	}

	#[tokio::test]
	async fn append_migration_rejects_conflicting_payload_for_same_slot() {
		let mgr = fresh_manager().await;
		let key: u64 = 99;
		let ts = hlc(200);
		let frame_a = b"original";
		let hash_a: [u8; 32] = *blake3::hash(frame_a).as_bytes();
		mgr.append_migration(1, key, ts, hash_a, frame_a)
			.await
			.unwrap();

		// A peer claims different bytes for the exact same MVCC
		// slot — the mismatch must fail loudly, not silently
		// accept either side.
		let frame_b = b"tampered";
		let hash_b: [u8; 32] = *blake3::hash(frame_b).as_bytes();
		let err = mgr
			.append_migration(1, key, ts, hash_b, frame_b)
			.await
			.unwrap_err();
		assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
	}

	#[tokio::test]
	async fn scan_as_of_filters_versions_per_key() {
		let mgr = fresh_manager().await;
		// Two keys in the same range, each with two versions at
		// different HLCs.
		mgr.append(1, hlc(10), b"a@10").await.unwrap();
		mgr.append(1, hlc(30), b"a@30").await.unwrap();
		mgr.append(2, hlc(20), b"b@20").await.unwrap();
		mgr.append(2, hlc(40), b"b@40").await.unwrap();

		// AS OF 25 — should see key=1 at 10 (20 hasn't landed),
		// key=2 at 20.
		let at_25 = mgr.scan_as_of(0, u64::MAX, hlc(25));
		assert_eq!(at_25.len(), 2);
		assert_eq!(at_25[0].0, 1);
		assert_eq!(at_25[0].1.hlc.physical_ms, 10);
		assert_eq!(at_25[1].0, 2);
		assert_eq!(at_25[1].1.hlc.physical_ms, 20);

		// AS OF 5 — nothing visible yet.
		assert!(mgr.scan_as_of(0, u64::MAX, hlc(5)).is_empty());

		// Zero sentinel -> latest versions.
		let latest = mgr.scan(0, u64::MAX);
		assert_eq!(latest[0].1.hlc.physical_ms, 30);
		assert_eq!(latest[1].1.hlc.physical_ms, 40);
	}
}
