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
use crate::crypto::{
	CryptoManager, DecryptedTieredSlab, EncryptedTieredSlab, FooterSummary, TieredOffsetIndexEntry,
	TieredSlabObjectMetadata,
};
use crate::metrics::global as metrics;
use crate::schema::{
	HybridTimestamp, Identity, IdentityPrefix, Locality, SlabId, SlabRecordHeader, TenantId,
};
use crate::storage::core::{
	GroupCommitConfig, LocalSlabStore, RecordLocation, StorageEngine, insert_version_sorted,
	newest_version_as_of, recover_slab_entries_from_blob, remove_version_exact,
};
use crate::storage::index::{
	PersistentIndexUpdate, PersistentLocalIndex, PersistentRecordRow, persistent_index_path,
};
use crate::storage::io_uring::IoRingHandle;
use crate::storage::l2::ObjectStore;
use crate::storage::{AsyncFileOps, BlockingFileOps, IoUringFileOps, OpenFlags, write_file};
use async_trait::async_trait;
use bytes::Bytes;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::io::{Error, ErrorKind};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::Instant;

const STORAGE_BACKUP_FORMAT_VERSION: u16 = 1;

type TenantKey = (TenantId, Identity);
type TenantLocalityMembers = BTreeMap<TenantId, BTreeSet<(Locality, Identity)>>;

const DEFAULT_L2_TAIL_READ_WINDOW_BYTES: u64 = 64 * 1024;
const MIN_L2_TAIL_READ_WINDOW_BYTES: u64 = 256 + 16;
const IDENTITY_MAX: Identity = Identity::new([0xFF; 32]);

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
	pub reclaimed_bytes: u64,
}

pub(crate) enum PreparedTieredRetentionRewrite {
	Rewrite {
		slab_id: SlabId,
		old_l2_uri: String,
		encrypted_bytes: Vec<u8>,
		payload_hash: [u8; 32],
		pruned_versions: usize,
		reclaimed_bytes: u64,
	},
	Retire {
		slab_id: SlabId,
		old_l2_uri: String,
		pruned_versions: usize,
		reclaimed_bytes: u64,
	},
}

/// Multi-slab composition of [`StorageEngine`].
pub struct SlabManager {
	config: SlabManagerConfig,
	file_ops: Arc<dyn AsyncFileOps>,
	state: Mutex<ManagerState>,
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
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct TieredRowRefreshPlan {
	old_rows: Vec<PersistentRecordRow>,
	new_rows: Vec<PersistentRecordRow>,
	relocated_reads: Vec<(RecordLocation, RecordLocation)>,
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
		self.key_index
			.entry((tenant_id, key_identity))
			.and_modify(|versions| insert_version_sorted(versions, location))
			.or_insert_with(|| vec![location]);
		self.insert_locality_membership(tenant_id, key_identity);
	}

	fn rollback_indexed_location(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
		location: RecordLocation,
	) {
		let key = (tenant_id, key_identity);
		let should_remove_key = if let Some(mut versions) = self.key_index.get_mut(&key) {
			let removed = remove_version_exact(&mut versions, location);
			removed && versions.is_empty()
		} else {
			false
		};
		if should_remove_key {
			self.key_index.remove(&key);
			self.remove_locality_membership(tenant_id, key_identity);
		}
	}

	fn insert_locality_membership(&self, tenant_id: TenantId, key_identity: Identity) {
		self.locality_index
			.write()
			.expect("locality index lock poisoned")
			.entry(tenant_id)
			.or_default()
			.insert((Locality::from(key_identity), key_identity));
	}

	fn remove_locality_membership(&self, tenant_id: TenantId, key_identity: Identity) {
		let mut locality_index = self
			.locality_index
			.write()
			.expect("locality index lock poisoned");
		let should_remove_tenant = if let Some(members) = locality_index.get_mut(&tenant_id) {
			members.remove(&(Locality::from(key_identity), key_identity));
			members.is_empty()
		} else {
			false
		};
		if should_remove_tenant {
			locality_index.remove(&tenant_id);
		}
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
		let (slab, location) = self
			.append_on_target(tenant_id, key_identity, hlc, frame)
			.await?;
		if let Err(err) = slab.wait_durable_commit().await {
			slab.rollback_index_entry(tenant_id, key_identity, location);
			self.rollback_indexed_location(tenant_id, key_identity, location);
			return Err(err);
		}
		if let Err(err) = self
			.persist_record_row_strict(tenant_id, key_identity, location)
			.await
		{
			slab.rollback_index_entry(tenant_id, key_identity, location);
			self.rollback_indexed_location(tenant_id, key_identity, location);
			return Err(err);
		}
		self.index_appended_location(tenant_id, key_identity, location);
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
	pub async fn open(config: SlabManagerConfig) -> std::io::Result<Self> {
		config.rehydration.validate()?;
		if !config.dir.is_dir() {
			return Err(Error::new(
				ErrorKind::NotFound,
				format!(
					"SlabManager directory does not exist: {}",
					config.dir.display()
				),
			));
		}

		let file_ops: Arc<dyn AsyncFileOps> = Arc::new(IoUringFileOps::new(config.io.clone()));
		cleanup_retention_rewrite_tmp_files(file_ops.as_ref(), &config.dir).await?;
		let mut existing_ids = discover_slab_ids(file_ops.as_ref(), &config.dir).await?;
		existing_ids.sort_unstable();

		let key_index: DashMap<TenantKey, Vec<RecordLocation>> = DashMap::new();
		let sealed: DashMap<SlabId, Arc<LocalSlabStore>> = DashMap::new();
		let tiered: DashMap<SlabId, TieredSlabPointer> = DashMap::new();
		let rehydrated: DashMap<SlabId, RehydratedSlabState> = DashMap::new();

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
			drain_slab_index(&slab, &key_index);
			(slab, id.saturating_add(1))
		} else {
			// Split: everything except the max becomes sealed, the
			// max becomes active. Drain into key_index in ascending
			// id order so later writes shadow earlier ones.
			let (&active_id, sealed_ids) = existing_ids.split_last().expect("non-empty");
			for &id in sealed_ids {
				let slab = Arc::new(
					LocalSlabStore::open(
						slab_path(&config.dir, id),
						id,
						file_ops.clone(),
						config.group_commit.clone(),
					)
					.await?,
				);
				drain_slab_index(&slab, &key_index);
				sealed.insert(id, slab);
			}
			let active = Arc::new(
				LocalSlabStore::open(
					slab_path(&config.dir, active_id),
					active_id,
					file_ops.clone(),
					config.group_commit.clone(),
				)
				.await?,
			);
			drain_slab_index(&active, &key_index);
			(active, active_id.saturating_add(1))
		};

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
					for pointer in snapshot.tiered_slabs {
						tiered.insert(pointer.slab_id, pointer);
					}
					for row in snapshot.tiered_records {
						key_index
							.entry((row.tenant_id, row.key_identity))
							.and_modify(|versions| insert_version_sorted(versions, row.location))
							.or_insert_with(|| vec![row.location]);
					}
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
		let locality_index = build_locality_index(&key_index);

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
			key_index,
			locality_index: RwLock::new(locality_index),
			relocated_reads: DashMap::new(),
			persistent_index,
			retired_tiered_cleanup: DashMap::new(),
		};
		manager.rebuild_persistent_index().await;
		Ok(manager)
	}

	/// Test / diagnostic accessor: current active slab id.
	pub async fn active_slab_id(&self) -> SlabId {
		self.state.lock().await.active.slab_id()
	}

	#[cfg(test)]
	pub async fn force_seal_active_for_test(&self) -> SlabId {
		let mut state = self.state.lock().await;
		let sealed = state.active.clone();
		sealed
			.fsync()
			.await
			.expect("force_seal_active_for_test should fsync sealed slab");
		let sealed_id = sealed.slab_id();
		let next_id = state.next_slab_id;
		let next = Arc::new(
			LocalSlabStore::open(
				slab_path(&self.config.dir, next_id),
				next_id,
				self.file_ops.clone(),
				self.config.group_commit.clone(),
			)
			.await
			.expect("force_seal_active_for_test should open next slab"),
		);
		state.active = next;
		state.sealed.insert(sealed_id, sealed);
		state.next_slab_id = next_id.saturating_add(1);
		sealed_id
	}

	pub fn data_dir(&self) -> &Path {
		&self.config.dir
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

	/// Encode a sealed slab into the ADR 022 L2 object layout.
	/// The current write path still assumes every sealed slab is
	/// single-tenant, so the footer and per-chunk AAD stay slab-scoped.
	pub fn encrypt_tiered_object_blob(
		&self,
		slab_id: SlabId,
		plaintext_blob: &[u8],
	) -> std::io::Result<EncryptedTieredSlab> {
		let metadata = build_tiered_object_metadata(plaintext_blob, slab_id)?;
		self.config
			.crypto
			.encrypt_tiered_slab_object(slab_id, &metadata, plaintext_blob)
	}

	/// Detect whether a tiered object already carries an encrypted
	/// footer. Callers use this to distinguish legacy plaintext L2
	/// objects from ciphertext ones during migration.
	pub fn encrypted_tiered_footer_summary(
		&self,
		blob: &[u8],
	) -> std::io::Result<Option<FooterSummary>> {
		self.config.crypto.encrypted_footer_summary(blob)
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
		self.apply_tier_slab_committed(pointer, None).await
	}

	pub async fn apply_tier_slab_committed(
		&self,
		pointer: TieredSlabPointer,
		last_control_log_index: Option<u64>,
	) -> std::io::Result<()> {
		// This path is intentionally idempotent for the same pointer. The
		// sweeper may fall back to calling it after Raft apply has already
		// fired the tier observer, so a second application of an identical
		// pointer must not rebuild row state or mutate relocated reads again.
		let old_pointer = {
			let state = self.state.lock().await;
			state
				.tiered
				.get(&pointer.slab_id)
				.map(|entry| entry.value().clone())
		};
		let refresh_plan = if old_pointer.as_ref().is_some_and(|old| old != &pointer)
			|| !self.has_rows_for_slab(pointer.slab_id)
		{
			Some(self.build_tiered_row_refresh_plan_from_l2(&pointer).await?)
		} else {
			None
		};
		#[cfg(debug_assertions)]
		if old_pointer.as_ref().is_some_and(|old| old == &pointer) {
			debug_assert!(
				refresh_plan.is_none(),
				"identical tier pointer replay must remain a no-op; the sweeper may reapply after the observer already committed local state"
			);
		}
		self.persist_tiered_pointer_strict(pointer.clone(), last_control_log_index)
			.await?;
		if let Some(plan) = &refresh_plan {
			for (old_location, new_location) in &plan.relocated_reads {
				self.relocated_reads.insert(*old_location, *new_location);
			}
		}
		{
			let mut state = self.state.lock().await;
			let _old = state.sealed.remove(&pointer.slab_id);
			let _old = state.rehydrated.remove(&pointer.slab_id);
			state.tiered.insert(pointer.slab_id, pointer.clone());
			state.hot_read_counts.remove(&pointer.slab_id);
			state.rehydrating.remove(&pointer.slab_id);
		}
		self.retired_tiered_cleanup.remove(&pointer.slab_id);
		if let Some(plan) = refresh_plan {
			self.apply_tiered_row_refresh_plan(plan).await;
		}

		// Unlink the L1 file. Open fds held by in-flight reads
		// continue to work; new reads find the entry in `tiered`
		// and go to L2.
		let path = slab_path(&self.config.dir, pointer.slab_id);
		unlink_local_file(self.file_ops.as_ref(), &path).await?;
		Ok(())
	}

	pub async fn retire_tiered_slab(&self, slab_id: SlabId) -> std::io::Result<bool> {
		self.retire_tiered_slab_committed(slab_id, None).await
	}

	pub async fn retire_tiered_slab_committed(
		&self,
		slab_id: SlabId,
		last_control_log_index: Option<u64>,
	) -> std::io::Result<bool> {
		let rows = self.record_rows_for_slab(slab_id);
		let retired_pointer = {
			let state = self.state.lock().await;
			state
				.tiered
				.get(&slab_id)
				.map(|entry| entry.value().clone())
		};
		let should_remove = {
			let state = self.state.lock().await;
			retired_pointer.is_some() || state.rehydrated.contains_key(&slab_id) || !rows.is_empty()
		};
		if !should_remove {
			return Ok(false);
		}
		if let Some(pointer) = retired_pointer.clone() {
			self.retired_tiered_cleanup.insert(slab_id, pointer);
		}
		self.delete_tiered_slab_strict(slab_id, last_control_log_index)
			.await?;
		{
			let mut state = self.state.lock().await;
			let _old = state.sealed.remove(&slab_id);
			let _old = state.rehydrated.remove(&slab_id);
			let _old = state.tiered.remove(&slab_id);
			state.hot_read_counts.remove(&slab_id);
			state.rehydrating.remove(&slab_id);
		}
		for row in &rows {
			self.rollback_indexed_location(row.tenant_id, row.key_identity, row.location);
		}
		self.relocated_reads.retain(|old_location, new_location| {
			old_location.slab_id != slab_id && new_location.slab_id != slab_id
		});
		let path = slab_path(&self.config.dir, slab_id);
		unlink_local_file(self.file_ops.as_ref(), &path).await?;
		Ok(true)
	}

	/// Converge one recovered tiered slab into local state.
	///
	/// When the redb mirror is unavailable or missing rows, the
	/// manager rebuilds the per-record index directly from the
	/// authoritative L2 blob before installing the tier pointer.
	pub async fn reconcile_tiered_slab(
		&self,
		pointer: TieredSlabPointer,
		last_control_log_index: Option<u64>,
	) -> std::io::Result<()> {
		self.apply_tier_slab_committed(pointer, last_control_log_index)
			.await
	}

	/// Whether a slab has been pivoted to L2. Cheap — only hits
	/// the tiered DashMap.
	pub async fn is_tiered(&self, slab_id: SlabId) -> bool {
		self.state.lock().await.tiered.contains_key(&slab_id)
	}

	/// Whether a tiered slab currently also has a rehydrated local
	/// copy in L1.
	pub async fn is_rehydrated(&self, slab_id: SlabId) -> bool {
		self.state.lock().await.rehydrated.contains_key(&slab_id)
	}

	/// Lookup the L2 pointer for a tiered slab.
	pub async fn tier_pointer(&self, slab_id: SlabId) -> Option<TieredSlabPointer> {
		self.state
			.lock()
			.await
			.tiered
			.get(&slab_id)
			.map(|p| p.clone())
	}

	/// Enumerate the current tiered-slab pointers held in local
	/// runtime state. The returned list is sorted by slab id so
	/// background sweeps behave deterministically.
	pub async fn list_tiered_slabs(&self) -> Vec<TieredSlabPointer> {
		let mut pointers = self
			.state
			.lock()
			.await
			.tiered
			.iter()
			.map(|entry| entry.value().clone())
			.collect::<Vec<_>>();
		pointers.sort_by_key(|pointer| pointer.slab_id);
		pointers
	}

	/// Tier retirements that committed in the SM but still need
	/// best-effort local cleanup retried.
	pub async fn list_retired_tiered_cleanup_candidates(&self) -> Vec<TieredSlabPointer> {
		let mut pointers = self
			.retired_tiered_cleanup
			.iter()
			.map(|entry| entry.value().clone())
			.collect::<Vec<_>>();
		pointers.sort_by_key(|pointer| pointer.slab_id);
		pointers
	}

	/// Retry only the local-file cleanup for a retired tiered slab.
	pub async fn retry_retired_tiered_local_cleanup(&self, slab_id: SlabId) -> std::io::Result<()> {
		let path = slab_path(&self.config.dir, slab_id);
		unlink_local_file(self.file_ops.as_ref(), &path).await
	}

	/// Mark one retired tiered slab cleanup candidate complete.
	pub fn clear_retired_tiered_cleanup(&self, slab_id: SlabId) {
		self.retired_tiered_cleanup.remove(&slab_id);
	}

	/// Best-effort reclaim of one rehydrated local cache copy. The
	/// authoritative L2 pointer remains intact; only the temporary
	/// L1 file is removed.
	pub async fn drop_rehydrated_slab(&self, slab_id: SlabId) -> std::io::Result<bool> {
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
		unlink_local_file(self.file_ops.as_ref(), &path).await?;
		Ok(true)
	}

	/// Enumerate every live `(key_identity, RecordLocation)` whose
	/// locality falls in the inclusive window
	/// `[start_locality, end_locality]`. Used
	/// by the Data Ring query path (ADR 005) to feed the WPBC
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
		self.scan_as_of_with_identity(tenant_id, start_locality, end_locality, as_of_hlc)
			.into_iter()
			.map(|(key_identity, loc)| (Locality::from(key_identity).get(), loc))
			.collect()
	}

	/// Variant of [`Self::scan_as_of`] that preserves the full
	/// authoritative [`Identity`] for each retained version. Query
	/// planning uses this when WPBC needs to evaluate typed
	/// identity/locality predicates against record metadata rather
	/// than the decoded Arrow payload alone.
	pub fn scan_as_of_with_identity(
		&self,
		tenant_id: TenantId,
		start_locality: impl Into<Locality>,
		end_locality: impl Into<Locality>,
		as_of_hlc: HybridTimestamp,
	) -> Vec<(Identity, RecordLocation)> {
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
				out.push((key_identity, location));
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
	) -> Vec<(TenantId, Identity, RecordLocation)> {
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
	) -> Vec<(TenantId, Identity, RecordLocation)> {
		let start_locality = start_locality.into();
		let end_locality = end_locality.into();
		if start_locality > end_locality {
			return Vec::new();
		}
		let cursor_active =
			cursor_identity != Identity::ZERO || cursor_hlc != bytemuck::Zeroable::zeroed();
		let cursor_locality = Locality::from(cursor_identity);
		let mut out: Vec<(TenantId, Identity, RecordLocation)> = Vec::new();
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
				out.push((tenant_id, key_identity, *loc));
			}
		}
		out.sort_by(|(tenant_a, ida, la), (tenant_b, idb, lb)| {
			(Locality::from(*ida), *tenant_a, *ida, la.hlc).cmp(&(
				Locality::from(*idb),
				*tenant_b,
				*idb,
				lb.hlc,
			))
		});
		out
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

		// No prior version at this exact HLC — take the normal
		// append path. `StorageEngine::append` handles rotation,
		// disk ack, and folding the new version into the
		// multi-version index.
		let location = self.append(tenant_id, key_identity, hlc, frame).await?;
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
		state.sealed.insert(sealed_slab.slab_id(), sealed_slab);
		state.next_slab_id = next_id.saturating_add(1);

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

	pub async fn prune_versions_older_than(
		&self,
		retention_floor: HybridTimestamp,
		max_slabs: usize,
	) -> std::io::Result<RetentionPruningReport> {
		if max_slabs == 0 {
			return Ok(RetentionPruningReport::default());
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

		let mut report = RetentionPruningReport::default();
		for (slab_id, slab) in sealed {
			if report.compacted_slabs >= max_slabs {
				break;
			}
			if let Some(outcome) = self
				.prune_one_sealed_slab(slab_id, slab, retention_floor)
				.await?
			{
				report.compacted_slabs += 1;
				report.pruned_versions = report
					.pruned_versions
					.saturating_add(outcome.reclaimed_records);
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
		let location = self.relocate_read_location(location);
		match self.resolve_backing(location.slab_id).await {
			SlabBacking::Local(slab) => {
				let started = Instant::now();
				let result = slab.read(location).await;
				metrics()
					.l1_read_latency_seconds
					.observe_duration(started.elapsed());
				result
			}
			SlabBacking::L2(pointer) => {
				let started = Instant::now();
				let result = self.read_from_l2(&pointer, location).await;
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
	fn locality_candidates_for_tenant(
		&self,
		tenant_id: TenantId,
		start_locality: Locality,
		end_locality: Locality,
	) -> Vec<(Locality, Identity)> {
		self.locality_index
			.read()
			.expect("locality index lock poisoned")
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
		let locality_index = self
			.locality_index
			.read()
			.expect("locality index lock poisoned");
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
				rows.push(PersistentRecordRow {
					tenant_id,
					key_identity,
					location: *location,
				});
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

	async fn persist_record_row(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
		location: RecordLocation,
	) {
		let Some(index) = &self.persistent_index else {
			return;
		};
		let row = PersistentRecordRow {
			tenant_id,
			key_identity,
			location,
		};
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
		location: RecordLocation,
	) -> std::io::Result<()> {
		let Some(index) = &self.persistent_index else {
			return Ok(());
		};
		let row = PersistentRecordRow {
			tenant_id,
			key_identity,
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
				)
			});
			let tiered = self.tiered_pointers_snapshot().await;
			index.rebuild(rows, tiered, None).await?;
		}
		Ok(())
	}

	async fn delete_persistent_records(&self, records: &[CompactionRecord]) {
		let rows = records
			.iter()
			.map(|record| PersistentRecordRow {
				tenant_id: record.tenant_id,
				key_identity: record.key_identity,
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
	) {
		let removed_rows = removed
			.iter()
			.map(|record| PersistentRecordRow {
				tenant_id: record.tenant_id,
				key_identity: record.key_identity,
				location: record.old_location,
			})
			.collect::<Vec<_>>();
		let inserted_rows = inserted
			.iter()
			.map(|record| PersistentRecordRow {
				tenant_id: record.tenant_id,
				key_identity: record.key_identity,
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
					rows.push(PersistentRecordRow {
						tenant_id,
						key_identity,
						location: *location,
					});
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

	async fn tiered_plaintext_from_l2(
		&self,
		pointer: &TieredSlabPointer,
	) -> std::io::Result<Vec<u8>> {
		let l2 = self.config.l2.clone().ok_or_else(|| {
			Error::new(
				ErrorKind::NotFound,
				format!(
					"cannot recover tiered slab {} without an L2 object store",
					pointer.slab_id
				),
			)
		})?;
		let blob = l2.get(&pointer.l2_uri).await?;
		validate_tiered_blob(pointer, &blob)?;
		decode_tiered_blob(self.config.crypto.as_ref(), pointer, &blob)
	}

	async fn load_tiered_rows_from_l2(
		&self,
		pointer: &TieredSlabPointer,
	) -> std::io::Result<Vec<PersistentRecordRow>> {
		let plaintext = self.tiered_plaintext_from_l2(pointer).await?;
		Ok(recover_slab_entries_from_blob(&plaintext, pointer.slab_id)?
			.into_iter()
			.map(|(tenant_id, key_identity, location)| PersistentRecordRow {
				tenant_id,
				key_identity,
				location,
			})
			.collect::<Vec<_>>())
	}

	async fn build_tiered_row_refresh_plan_from_l2(
		&self,
		pointer: &TieredSlabPointer,
	) -> std::io::Result<TieredRowRefreshPlan> {
		let new_rows = self.load_tiered_rows_from_l2(pointer).await?;
		let old_rows = self.record_rows_for_slab(pointer.slab_id);
		let mut old_by_slot = HashMap::with_capacity(old_rows.len());
		for row in &old_rows {
			old_by_slot
				.entry((row.tenant_id, row.key_identity, row.location.hlc))
				.or_insert_with(Vec::new)
				.push(row.location);
		}
		let mut new_by_slot = HashMap::with_capacity(new_rows.len());
		for row in &new_rows {
			new_by_slot
				.entry((row.tenant_id, row.key_identity, row.location.hlc))
				.or_insert_with(Vec::new)
				.push(row.location);
		}
		let mut relocated_reads = Vec::new();
		for (slot, old_locations) in old_by_slot {
			let Some(new_locations) = new_by_slot.remove(&slot) else {
				continue;
			};
			// Retention rewrites preserve surviving rows in append/offset order but
			// may prune a prefix of older duplicates that share the same HLC. Align
			// from the tail so the newest duplicate survives the relocation map.
			let mut slot_relocations = old_locations
				.into_iter()
				.rev()
				.zip(new_locations.into_iter().rev())
				.filter_map(|(old_location, new_location)| {
					(old_location != new_location).then_some((old_location, new_location))
				})
				.collect::<Vec<_>>();
			slot_relocations.reverse();
			relocated_reads.extend(slot_relocations);
		}
		relocated_reads
			.sort_by_key(|(old_location, _)| (old_location.slab_id, old_location.offset));
		Ok(TieredRowRefreshPlan {
			old_rows,
			new_rows,
			relocated_reads,
		})
	}

	async fn apply_tiered_row_refresh_plan(&self, plan: TieredRowRefreshPlan) {
		if plan.old_rows == plan.new_rows {
			return;
		}
		for row in &plan.old_rows {
			self.rollback_indexed_location(row.tenant_id, row.key_identity, row.location);
		}
		for row in &plan.new_rows {
			self.key_index
				.entry((row.tenant_id, row.key_identity))
				.and_modify(|versions| {
					if !versions.contains(&row.location) {
						insert_version_sorted(versions, row.location);
					}
				})
				.or_insert_with(|| vec![row.location]);
			self.insert_locality_membership(row.tenant_id, row.key_identity);
		}
		self.mirror_persistent_update(
			PersistentIndexUpdate {
				delete_records: plan.old_rows,
				put_records: plan.new_rows,
				..Default::default()
			},
			"tiered-row-refresh",
		)
		.await;
	}

	pub(crate) async fn prepare_tiered_retention_rewrite(
		&self,
		pointer: &TieredSlabPointer,
		retention_floor: HybridTimestamp,
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
		let plaintext = decode_tiered_blob(self.config.crypto.as_ref(), pointer, &old_blob)?;
		let mut records = recover_slab_entries_from_blob(&plaintext, pointer.slab_id)?
			.into_iter()
			.map(|(tenant_id, key_identity, old_location)| CompactionRecord {
				tenant_id,
				key_identity,
				old_location,
				reclaim: self.is_retention_prunable(
					tenant_id,
					key_identity,
					old_location,
					retention_floor,
				),
			})
			.collect::<Vec<_>>();
		records.sort_by_key(|record| record.old_location.offset);
		let pruned_versions = records.iter().filter(|record| record.reclaim).count();
		let survivors = records.iter().filter(|record| !record.reclaim).count();
		if pruned_versions == 0 {
			return Ok(None);
		}
		if survivors == 0 {
			return Ok(Some(PreparedTieredRetentionRewrite::Retire {
				slab_id: pointer.slab_id,
				old_l2_uri: pointer.l2_uri.clone(),
				pruned_versions,
				reclaimed_bytes: old_blob.len() as u64,
			}));
		}

		let tmp_path = retention_rewrite_tmp_path(&self.config.dir, pointer.slab_id);
		let _ = unlink_local_file(self.file_ops.as_ref(), &tmp_path).await;
		let rewrite_result = async {
			let rewritten = Arc::new(
				LocalSlabStore::open(
					&tmp_path,
					pointer.slab_id,
					self.file_ops.clone(),
					self.config.group_commit.clone(),
				)
				.await?,
			);
			for record in records.iter().filter(|record| !record.reclaim) {
				let frame = extract_record_from_blob(&plaintext, record.old_location)?;
				rewritten
					.append(
						record.tenant_id,
						record.key_identity,
						record.old_location.hlc,
						&frame,
					)
					.await?;
			}
			rewritten.fsync().await?;
			self.file_ops.read_all(&tmp_path).await
		}
		.await;
		let _ = unlink_local_file(self.file_ops.as_ref(), &tmp_path).await;
		let rewritten_plaintext = rewrite_result?;
		let encrypted = self.encrypt_tiered_object_blob(pointer.slab_id, &rewritten_plaintext)?;
		Ok(Some(PreparedTieredRetentionRewrite::Rewrite {
			slab_id: pointer.slab_id,
			old_l2_uri: pointer.l2_uri.clone(),
			payload_hash: crate::hash::content::digest(&encrypted.bytes),
			reclaimed_bytes: (old_blob.len() as u64).saturating_sub(encrypted.bytes.len() as u64),
			pruned_versions,
			encrypted_bytes: encrypted.bytes,
		}))
	}

	async fn reserve_slab_id(&self) -> SlabId {
		let mut state = self.state.lock().await;
		let next_id = state.next_slab_id;
		state.next_slab_id = state.next_slab_id.saturating_add(1);
		next_id
	}

	async fn compact_one_sealed_slab(
		&self,
		slab_id: SlabId,
		slab: Arc<LocalSlabStore>,
		tombstones: &[MigratedRangeTombstone],
	) -> std::io::Result<Option<CompactionOutcome>> {
		let mut records: Vec<CompactionRecord> = slab
			.index
			.iter()
			.flat_map(|entry| {
				let (tenant_id, key_identity) = *entry.key();
				let locality = Locality::from(key_identity);
				entry
					.value()
					.iter()
					.map(move |loc| CompactionRecord {
						tenant_id,
						key_identity,
						old_location: *loc,
						reclaim: tombstones.iter().any(|tombstone| {
							locality.get() >= tombstone.range_start
								&& locality.get() <= tombstone.range_end
								&& loc.hlc >= tombstone.migration_hlc_floor
						}),
					})
					.collect::<Vec<_>>()
			})
			.collect();
		records.sort_by_key(|record| record.old_location.offset);

		self.compact_sealed_slab_records(slab_id, slab, &records)
			.await
	}

	async fn prune_one_sealed_slab(
		&self,
		slab_id: SlabId,
		slab: Arc<LocalSlabStore>,
		retention_floor: HybridTimestamp,
	) -> std::io::Result<Option<CompactionOutcome>> {
		let mut records: Vec<CompactionRecord> = slab
			.index
			.iter()
			.flat_map(|entry| {
				let (tenant_id, key_identity) = *entry.key();
				entry
					.value()
					.iter()
					.map(move |loc| CompactionRecord {
						tenant_id,
						key_identity,
						old_location: *loc,
						reclaim: self.is_retention_prunable(
							tenant_id,
							key_identity,
							*loc,
							retention_floor,
						),
					})
					.collect::<Vec<_>>()
			})
			.collect();
		records.sort_by_key(|record| record.old_location.offset);

		self.compact_sealed_slab_records(slab_id, slab, &records)
			.await
	}

	fn is_retention_prunable(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
		location: RecordLocation,
		retention_floor: HybridTimestamp,
	) -> bool {
		let Some(versions) = self.key_index.get(&(tenant_id, key_identity)) else {
			return false;
		};
		let older_versions = versions.partition_point(|stored| stored.hlc < retention_floor);
		let prune_prefix_len = older_versions.saturating_sub(1);
		prune_prefix_len > 0
			&& versions
				.iter()
				.take(prune_prefix_len)
				.any(|stored| *stored == location)
	}

	async fn compact_sealed_slab_records(
		&self,
		slab_id: SlabId,
		slab: Arc<LocalSlabStore>,
		records: &[CompactionRecord],
	) -> std::io::Result<Option<CompactionOutcome>> {
		if records.is_empty() || records.iter().all(|record| !record.reclaim) {
			return Ok(None);
		}

		let old_bytes = slab.write_cursor();
		let reclaimed_records = records.iter().filter(|record| record.reclaim).count();
		let survivors = records.iter().filter(|record| !record.reclaim).count();
		if survivors == 0 {
			self.finish_compaction_without_replacement(slab_id, &records)
				.await?;
			return Ok(Some(CompactionOutcome {
				reclaimed_bytes: old_bytes,
				reclaimed_records,
			}));
		}

		let replacement_id = self.reserve_slab_id().await;
		let replacement_path = slab_path(&self.config.dir, replacement_id);
		let replacement = Arc::new(
			LocalSlabStore::open(
				&replacement_path,
				replacement_id,
				self.file_ops.clone(),
				self.config.group_commit.clone(),
			)
			.await?,
		);
		let mut relocations = Vec::with_capacity(survivors);
		for record in records.iter().filter(|record| !record.reclaim) {
			let frame = slab.read(record.old_location).await?;
			let new_location = replacement
				.append(
					record.tenant_id,
					record.key_identity,
					record.old_location.hlc,
					&frame,
				)
				.await?;
			relocations.push(RelocatedRecord {
				tenant_id: record.tenant_id,
				key_identity: record.key_identity,
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
			let _ = unlink_local_file(self.file_ops.as_ref(), &path).await;
			return Ok(None);
		}

		let old_path = slab_path(&self.config.dir, slab_id);
		unlink_local_file(self.file_ops.as_ref(), &old_path).await?;

		Ok(Some(CompactionOutcome {
			reclaimed_bytes: old_bytes.saturating_sub(new_bytes),
			reclaimed_records,
		}))
	}

	async fn finish_compaction_without_replacement(
		&self,
		slab_id: SlabId,
		records: &[CompactionRecord],
	) -> std::io::Result<()> {
		let removed = {
			let state = self.state.lock().await;
			state.sealed.remove(&slab_id).is_some()
		};
		if !removed {
			return Ok(());
		}
		for record in records {
			self.rollback_indexed_location(
				record.tenant_id,
				record.key_identity,
				record.old_location,
			);
		}
		self.delete_persistent_records(records).await;
		let old_path = slab_path(&self.config.dir, slab_id);
		unlink_local_file(self.file_ops.as_ref(), &old_path).await?;
		Ok(())
	}

	async fn finish_compaction_with_replacement(
		&self,
		old_slab_id: SlabId,
		new_slab_id: SlabId,
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
			self.rollback_indexed_location(
				record.tenant_id,
				record.key_identity,
				record.old_location,
			);
		}
		for relocation in relocations {
			self.index_appended_location(
				relocation.tenant_id,
				relocation.key_identity,
				relocation.new_location,
			);
			self.relocated_reads
				.insert(relocation.old_location, relocation.new_location);
		}
		drop(state);
		self.replace_persistent_records(records, relocations).await;
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
		let mut plaintext_for_rehydration = None;
		let frame = if pointer.is_encrypted {
			self.read_encrypted_record_from_l2(l2.as_ref(), pointer, location)
				.await?
		} else {
			let plaintext = self
				.fetch_full_tiered_plaintext(l2.as_ref(), pointer)
				.await?;
			let frame = extract_record_from_blob(&plaintext, location)?;
			plaintext_for_rehydration = Some(plaintext);
			frame
		};
		if self.note_l2_read(pointer.slab_id).await {
			let rehydrate_result = async {
				let plaintext = match plaintext_for_rehydration.take() {
					Some(plaintext) => plaintext,
					None => {
						self.fetch_full_tiered_plaintext(l2.as_ref(), pointer)
							.await?
					}
				};
				self.rehydrate_tiered_slab(pointer.clone(), bytes::Bytes::from(plaintext))
					.await
			}
			.await;
			if let Err(err) = rehydrate_result {
				tracing::warn!(
					slab_id = pointer.slab_id.get(),
					error = %err,
					"rehydration failed; continuing to serve slab from L2"
				);
			}
		}
		Ok(frame)
	}

	async fn fetch_full_tiered_plaintext(
		&self,
		l2: &dyn ObjectStore,
		pointer: &TieredSlabPointer,
	) -> std::io::Result<Vec<u8>> {
		let blob = l2.get_bytes(&pointer.l2_uri).await?;
		validate_tiered_blob(pointer, blob.as_ref())?;
		decode_tiered_blob(self.config.crypto.as_ref(), pointer, blob.as_ref())
	}

	async fn read_encrypted_record_from_l2(
		&self,
		l2: &dyn ObjectStore,
		pointer: &TieredSlabPointer,
		location: RecordLocation,
	) -> std::io::Result<Vec<u8>> {
		let metadata = l2.head(&pointer.l2_uri).await?;
		let tail_start = metadata
			.size_bytes
			.saturating_sub(self.config.rehydration.tail_read_window_bytes);
		let tail_window = l2
			.get_range(&pointer.l2_uri, tail_start..metadata.size_bytes)
			.await?;
		let plaintext_start = usize::try_from(location.offset).map_err(|_| {
			Error::new(
				ErrorKind::InvalidData,
				format!("record offset {} does not fit into usize", location.offset),
			)
		})?;
		let plaintext_end_u64 = location
			.offset
			.checked_add(location.length as u64)
			.ok_or_else(|| {
				Error::new(
					ErrorKind::InvalidData,
					format!(
						"record range overflows u64 for slab {} at offset {}",
						pointer.slab_id, location.offset,
					),
				)
			})?;
		let plaintext_end = usize::try_from(plaintext_end_u64).map_err(|_| {
			Error::new(
				ErrorKind::InvalidData,
				format!("record end {} does not fit into usize", plaintext_end_u64),
			)
		})?;
		let additional_metadata_range = self.config.crypto.additional_tiered_metadata_range(
			pointer.slab_id,
			metadata.size_bytes,
			tail_window.as_ref(),
		)?;
		let additional_metadata = match additional_metadata_range {
			Some(range) => Some(l2.get_range(&pointer.l2_uri, range).await?),
			None => None,
		};
		let plan = self.config.crypto.plan_tiered_range_read(
			pointer.slab_id,
			metadata.size_bytes,
			tail_window.as_ref(),
			additional_metadata.as_deref(),
			plaintext_start..plaintext_end,
		)?;
		let footer = plan.footer();
		let ciphertext = l2
			.get_range(&pointer.l2_uri, plan.ciphertext_range_in_object())
			.await?;
		let record_bytes = plan.decrypt(ciphertext.as_ref())?;
		validate_tiered_record_header_tenant(&record_bytes, footer.tenant_id, pointer.slab_id)?;
		extract_record_from_blob(
			&record_bytes,
			RecordLocation {
				offset: 0,
				..location
			},
		)
	}

	async fn note_l2_read(&self, slab_id: SlabId) -> bool {
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
		let result = self
			.install_rehydrated_local_copy(pointer.slab_id, blob.as_ref())
			.await;
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

	async fn install_rehydrated_local_copy(
		&self,
		slab_id: SlabId,
		blob: &[u8],
	) -> std::io::Result<RehydratedSlabState> {
		let path = slab_path(&self.config.dir, slab_id);
		let tmp_path = rehydrate_tmp_path(&self.config.dir, slab_id);
		let _ = unlink_local_file(self.file_ops.as_ref(), &tmp_path).await;
		self.file_ops.write_all(&tmp_path, blob).await?;
		let tmp = self
			.file_ops
			.open(
				&tmp_path,
				OpenFlags {
					read: true,
					write: true,
					create: false,
					create_new: false,
					truncate: false,
					direct: false,
				},
			)
			.await?;
		self.file_ops.fsync(&tmp).await?;
		self.file_ops.rename(&tmp_path, &path).await?;
		fsync_parent_dir(self.file_ops.as_ref(), &path).await?;
		let slab = Arc::new(
			LocalSlabStore::open(
				&path,
				slab_id,
				self.file_ops.clone(),
				self.config.group_commit.clone(),
			)
			.await?,
		);
		Ok(RehydratedSlabState {
			slab,
			keep_local_until: Instant::now() + self.config.rehydration.min_local_residency,
		})
	}
}

#[derive(Clone, Copy)]
struct CompactionRecord {
	tenant_id: TenantId,
	key_identity: Identity,
	old_location: RecordLocation,
	reclaim: bool,
}

#[derive(Clone, Copy)]
struct RelocatedRecord {
	tenant_id: TenantId,
	key_identity: Identity,
	old_location: RecordLocation,
	new_location: RecordLocation,
}

#[derive(Clone, Copy)]
struct CompactionOutcome {
	reclaimed_bytes: u64,
	reclaimed_records: usize,
}

// ============================================================
//  HELPERS
// ============================================================

fn validate_tiered_blob(pointer: &TieredSlabPointer, blob: &[u8]) -> std::io::Result<()> {
	let actual = crate::hash::content::digest(blob);
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

fn decode_tiered_blob(
	crypto: &CryptoManager,
	pointer: &TieredSlabPointer,
	blob: &[u8],
) -> std::io::Result<Vec<u8>> {
	match crypto.encrypted_footer_summary(blob)? {
		Some(FooterSummary {
			slab_id, tenant_id, ..
		}) => {
			if slab_id != pointer.slab_id {
				return Err(Error::new(
					ErrorKind::InvalidData,
					format!(
						"encrypted tiered slab footer references slab {} but metadata expects {}",
						slab_id, pointer.slab_id
					),
				));
			}
			let DecryptedTieredSlab { plaintext, .. } =
				crypto.decrypt_tiered_slab(pointer.slab_id, blob)?;
			let payload_tenant_id = single_tenant_id_from_plain_slab(&plaintext, pointer.slab_id)?;
			if payload_tenant_id != tenant_id {
				return Err(Error::new(
					ErrorKind::InvalidData,
					format!(
						"encrypted tiered slab footer tenant {} does not match payload tenant {} for slab {}",
						tenant_id.get(),
						payload_tenant_id.get(),
						pointer.slab_id,
					),
				));
			}
			Ok(plaintext)
		}
		None => Ok(blob.to_vec()),
	}
}

fn validate_tiered_record_header_tenant(
	blob: &[u8],
	expected_tenant_id: TenantId,
	slab_id: SlabId,
) -> std::io::Result<()> {
	let header_size = std::mem::size_of::<SlabRecordHeader>();
	if blob.len() < header_size {
		return Err(Error::new(
			ErrorKind::UnexpectedEof,
			"L2 record range shorter than record header",
		));
	}
	let header = bytemuck::pod_read_unaligned::<SlabRecordHeader>(&blob[..header_size]);
	if header.tenant_id != expected_tenant_id {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"encrypted tiered slab footer tenant {} does not match record header tenant {} for slab {}",
				expected_tenant_id.get(),
				header.tenant_id.get(),
				slab_id,
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
	let header = bytemuck::pod_read_unaligned::<crate::schema::SlabRecordHeader>(
		&blob[offset..offset + header_size],
	);
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

fn build_tiered_object_metadata(
	blob: &[u8],
	slab_id: SlabId,
) -> std::io::Result<TieredSlabObjectMetadata> {
	let entries = recover_slab_entries_from_blob(blob, slab_id)?;
	let Some((tenant_id, _, first_location)) = entries.first().copied() else {
		return Err(Error::new(
			ErrorKind::InvalidInput,
			format!(
				"sealed slab {} does not contain any committed records; tiered encryption requires non-empty single-tenant slabs",
				slab_id
			),
		));
	};
	let mut offset_index = Vec::with_capacity(entries.len());
	for (other_tenant_id, key_identity, location) in entries {
		if other_tenant_id != tenant_id {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				format!(
					"sealed slab {} contains multiple tenants ({} and {}); tiered encryption requires single-tenant slabs until W5 lands tenant-scoped active slabs",
					slab_id,
					tenant_id.get(),
					other_tenant_id.get(),
				),
			));
		}
		offset_index.push(TieredOffsetIndexEntry {
			key_locator: IdentityPrefix::from(key_identity),
			byte_offset: location.offset,
			byte_length: location.length,
			flags: 0,
		});
	}
	Ok(TieredSlabObjectMetadata {
		tenant_id,
		created_hlc: first_location.hlc.physical_ms,
		offset_index,
	})
}

fn single_tenant_id_from_plain_slab(blob: &[u8], slab_id: SlabId) -> std::io::Result<TenantId> {
	Ok(build_tiered_object_metadata(blob, slab_id)?.tenant_id)
}

fn slab_path(dir: &Path, slab_id: SlabId) -> PathBuf {
	dir.join(format!("slab_{:016x}.wnd", slab_id))
}

fn rehydrate_tmp_path(dir: &Path, slab_id: SlabId) -> PathBuf {
	dir.join(format!("slab_{:016x}.wnd.rehydrate.tmp", slab_id))
}

fn retention_rewrite_tmp_path(dir: &Path, slab_id: SlabId) -> PathBuf {
	dir.join(format!("slab_{:016x}.wnd.retention.tmp", slab_id))
}

fn parse_retention_rewrite_tmp_filename(name: &str) -> Option<SlabId> {
	let rest = name
		.strip_prefix("slab_")?
		.strip_suffix(".wnd.retention.tmp")?;
	if rest.len() != 16 {
		return None;
	}
	u64::from_str_radix(rest, 16).ok().map(SlabId::new)
}

async fn cleanup_retention_rewrite_tmp_files(
	file_ops: &dyn AsyncFileOps,
	dir: &Path,
) -> std::io::Result<()> {
	let mut removed = 0usize;
	for entry in file_ops.read_dir(dir).await? {
		if !entry.is_file {
			continue;
		}
		let Some(name) = entry.file_name.to_str() else {
			continue;
		};
		if parse_retention_rewrite_tmp_filename(name).is_none() {
			continue;
		}
		unlink_local_file(file_ops, &dir.join(name)).await?;
		removed = removed.saturating_add(1);
	}
	if removed > 0 {
		tracing::warn!(
			dir = %dir.display(),
			removed_tmp_files = removed,
			"removed stale tiered retention rewrite temp files during open"
		);
	}
	Ok(())
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
fn drain_slab_index(slab: &LocalSlabStore, key_index: &DashMap<TenantKey, Vec<RecordLocation>>) {
	for entry in slab.index.iter() {
		let tenant_key = *entry.key();
		for loc in entry.value().iter() {
			key_index
				.entry(tenant_key)
				.and_modify(|versions| insert_version_sorted(versions, *loc))
				.or_insert_with(|| vec![*loc]);
		}
	}
}

fn build_locality_index(
	key_index: &DashMap<TenantKey, Vec<RecordLocation>>,
) -> TenantLocalityMembers {
	let mut locality_index = TenantLocalityMembers::new();
	for entry in key_index.iter() {
		let (tenant_id, key_identity) = *entry.key();
		locality_index
			.entry(tenant_id)
			.or_default()
			.insert((Locality::from(key_identity), key_identity));
	}
	locality_index
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::crypto::CryptoManager;
	use crate::schema::NodeId;
	use crate::storage::io_uring::spawn_io_thread;
	use crate::storage::l2::{LocalFsObjectStore, ObjectStore};

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

	fn id(n: u64) -> Identity {
		let mut bytes = [0u8; 32];
		bytes[..8].copy_from_slice(&n.to_le_bytes());
		Identity::new(bytes)
	}

	fn tenant() -> TenantId {
		TenantId::ZERO
	}

	fn test_crypto() -> Arc<CryptoManager> {
		let crypto = Arc::new(CryptoManager::new_dev());
		crypto.install_local_only_for_tests(tenant()).unwrap();
		crypto
	}

	async fn fresh_manager() -> Arc<SlabManager> {
		fresh_manager_with_group_commit(GroupCommitConfig::default()).await
	}

	async fn fresh_manager_with_group_commit(group_commit: GroupCommitConfig) -> Arc<SlabManager> {
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
				group_commit,
				crypto: test_crypto(),
			})
			.await
			.unwrap(),
		)
	}

	async fn force_seal_active(manager: &Arc<SlabManager>) -> SlabId {
		let mut state = manager.state.lock().await;
		let sealed = state.active.clone();
		sealed.fsync().await.unwrap();
		let sealed_id = sealed.slab_id();
		let next_id = state.next_slab_id;
		let next = Arc::new(
			LocalSlabStore::open(
				slab_path(&manager.config.dir, next_id),
				next_id,
				manager.file_ops.clone(),
				manager.config.group_commit.clone(),
			)
			.await
			.unwrap(),
		);
		state.active = next;
		state.sealed.insert(sealed_id, sealed);
		state.next_slab_id = next_id.saturating_add(1);
		sealed_id
	}

	#[tokio::test]
	async fn lookup_returns_latest_version_after_multiple_appends() {
		let mgr = fresh_manager().await;
		let key = id(42);
		mgr.append(tenant(), key, hlc(10), b"v1").await.unwrap();
		mgr.append(tenant(), key, hlc(20), b"v2").await.unwrap();
		mgr.append(tenant(), key, hlc(30), b"v3").await.unwrap();

		let latest = mgr.lookup(tenant(), key).expect("key present");
		assert_eq!(latest.hlc.physical_ms, 30);
	}

	#[tokio::test]
	async fn encrypt_tiered_object_blob_uses_adr022_layout() {
		let mgr = fresh_manager().await;
		mgr.append(tenant(), id(0x1234), hlc(10), b"adr022-row")
			.await
			.unwrap();
		let slab_id = force_seal_active(&mgr).await;
		let plaintext = mgr.read_sealed_raw(slab_id).await.unwrap();
		let encrypted = mgr.encrypt_tiered_object_blob(slab_id, &plaintext).unwrap();
		assert!(encrypted.bytes.starts_with(b"WENDSLAB"));
		let footer = mgr
			.encrypted_tiered_footer_summary(&encrypted.bytes)
			.unwrap()
			.expect("encrypted footer expected");
		assert_eq!(footer.slab_id, slab_id);
		assert_eq!(footer.tenant_id, tenant());
	}

	#[tokio::test]
	async fn encrypt_tiered_object_blob_rejects_empty_plain_slab() {
		let mgr = fresh_manager().await;
		let err = mgr
			.encrypt_tiered_object_blob(SlabId::new(77), &[])
			.unwrap_err();
		assert_eq!(err.kind(), ErrorKind::InvalidInput);
		assert!(
			err.to_string()
				.contains("does not contain any committed records"),
			"unexpected error: {err}"
		);
	}

	#[tokio::test]
	async fn decode_tiered_blob_rejects_footer_tenant_mismatch_with_payload_tenant() {
		let mgr = fresh_manager().await;
		let key = id(0xfeed);
		mgr.append(tenant(), key, hlc(10), b"tenant-bound payload")
			.await
			.unwrap();
		let slab_id = force_seal_active(&mgr).await;
		let plaintext = mgr.read_sealed_raw(slab_id).await.unwrap();

		let mismatched_tenant = TenantId::new(17);
		mgr.config
			.crypto
			.install_local_only_for_tests(mismatched_tenant)
			.unwrap();
		let encrypted = mgr
			.config
			.crypto
			.encrypt_tiered_slab_object(
				slab_id,
				&TieredSlabObjectMetadata {
					tenant_id: mismatched_tenant,
					created_hlc: 10,
					offset_index: vec![TieredOffsetIndexEntry {
						key_locator: IdentityPrefix::ZERO,
						byte_offset: 0,
						byte_length: plaintext.len().try_into().unwrap(),
						flags: 0,
					}],
				},
				&plaintext,
			)
			.unwrap();
		let pointer = TieredSlabPointer {
			slab_id,
			l2_uri: format!("memory://tiered/{slab_id:016x}"),
			payload_hash: crate::hash::content::digest(&encrypted.bytes),
			is_encrypted: true,
		};

		let err =
			decode_tiered_blob(mgr.config.crypto.as_ref(), &pointer, &encrypted.bytes).unwrap_err();
		assert_eq!(err.kind(), ErrorKind::InvalidData);
		assert!(
			err.to_string().contains("footer tenant"),
			"unexpected error: {err}"
		);
	}

	#[tokio::test]
	async fn lookup_as_of_returns_historical_version() {
		let mgr = fresh_manager().await;
		let key = id(7);
		mgr.append(tenant(), key, hlc(10), b"v1").await.unwrap();
		mgr.append(tenant(), key, hlc(20), b"v2").await.unwrap();
		mgr.append(tenant(), key, hlc(30), b"v3").await.unwrap();

		assert_eq!(
			mgr.lookup_as_of(tenant(), key, hlc(15))
				.unwrap()
				.hlc
				.physical_ms,
			10
		);
		assert_eq!(
			mgr.lookup_as_of(tenant(), key, hlc(20))
				.unwrap()
				.hlc
				.physical_ms,
			20
		);
		assert_eq!(
			mgr.lookup_as_of(tenant(), key, hlc(25))
				.unwrap()
				.hlc
				.physical_ms,
			20
		);
		// Bound before the first write -> nothing visible yet.
		assert!(mgr.lookup_as_of(tenant(), key, hlc(5)).is_none());
	}

	#[tokio::test]
	async fn scan_all_versions_returns_every_retained_version_above_floor() {
		let mgr = fresh_manager().await;
		// Two keys, two versions each — one above floor, one below.
		let key_a = id(1);
		let key_b = id(2);
		mgr.append(tenant(), key_a, hlc(5), b"a@5").await.unwrap();
		mgr.append(tenant(), key_a, hlc(50), b"a@50").await.unwrap();
		mgr.append(tenant(), key_b, hlc(20), b"b@20").await.unwrap();
		mgr.append(tenant(), key_b, hlc(200), b"b@200")
			.await
			.unwrap();

		let hits = mgr.scan_all_versions(Locality::ZERO, Locality::MAX, hlc(10));
		// Floor at 10 drops (1, hlc=5); the remaining three survive.
		assert_eq!(hits.len(), 3);
		// Sorted first by locality/identity, then by hlc ascending.
		assert_eq!(hits[0].0, tenant());
		assert_eq!(hits[0].1, key_a);
		assert_eq!(hits[0].2.hlc.physical_ms, 50);
		assert_eq!(hits[1].0, tenant());
		assert_eq!(hits[1].1, key_b);
		assert_eq!(hits[1].2.hlc.physical_ms, 20);
		assert_eq!(hits[2].0, tenant());
		assert_eq!(hits[2].1, key_b);
		assert_eq!(hits[2].2.hlc.physical_ms, 200);

		// Inverted window → empty rather than panic.
		assert!(
			mgr.scan_all_versions(Locality::new(10), Locality::new(5), hlc(0))
				.is_empty()
		);
	}

	#[tokio::test]
	async fn append_migration_is_idempotent_on_identical_payload() {
		let mgr = fresh_manager().await;
		let key = id(42);
		let ts = hlc(100);
		let frame = b"payload-bytes";
		let hash = crate::hash::content::digest(frame);

		let first = mgr
			.append_migration(7, tenant(), key, ts, hash, frame)
			.await
			.unwrap();
		assert!(matches!(first, MigrationIngestOutcome::Inserted(_)));

		let second = mgr
			.append_migration(7, tenant(), key, ts, hash, frame)
			.await
			.unwrap();
		assert!(matches!(second, MigrationIngestOutcome::Duplicate(_)));

		// Only one version at this (key, hlc) should exist.
		let locality = Locality::from(key);
		let versions = mgr.scan_all_versions(locality, locality, hlc(0));
		assert_eq!(versions.len(), 1);
	}

	#[tokio::test]
	async fn append_migration_rejects_conflicting_payload_for_same_slot() {
		let mgr = fresh_manager().await;
		let key = id(99);
		let ts = hlc(200);
		let frame_a = b"original";
		let hash_a = crate::hash::content::digest(frame_a);
		mgr.append_migration(1, tenant(), key, ts, hash_a, frame_a)
			.await
			.unwrap();

		// A peer claims different bytes for the exact same MVCC
		// slot — the mismatch must fail loudly, not silently
		// accept either side.
		let frame_b = b"tampered";
		let hash_b = crate::hash::content::digest(frame_b);
		let err = mgr
			.append_migration(1, tenant(), key, ts, hash_b, frame_b)
			.await
			.unwrap_err();
		assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
	}

	#[tokio::test]
	async fn scan_as_of_filters_versions_per_key() {
		let mgr = fresh_manager().await;
		// Two keys in the same range, each with two versions at
		// different HLCs.
		let key_a = id(1);
		let key_b = id(2);
		mgr.append(tenant(), key_a, hlc(10), b"a@10").await.unwrap();
		mgr.append(tenant(), key_a, hlc(30), b"a@30").await.unwrap();
		mgr.append(tenant(), key_b, hlc(20), b"b@20").await.unwrap();
		mgr.append(tenant(), key_b, hlc(40), b"b@40").await.unwrap();

		// AS OF 25 — should see key=1 at 10 (20 hasn't landed),
		// key=2 at 20.
		let at_25 = mgr.scan_as_of(tenant(), Locality::ZERO, Locality::MAX, hlc(25));
		assert_eq!(at_25.len(), 2);
		assert_eq!(at_25[0].0, Locality::from(key_a).get());
		assert_eq!(at_25[0].1.hlc.physical_ms, 10);
		assert_eq!(at_25[1].0, Locality::from(key_b).get());
		assert_eq!(at_25[1].1.hlc.physical_ms, 20);

		// AS OF 5 — nothing visible yet.
		assert!(
			mgr.scan_as_of(tenant(), Locality::ZERO, Locality::MAX, hlc(5))
				.is_empty()
		);

		// Zero sentinel -> latest versions.
		let latest = mgr.scan(tenant(), Locality::ZERO, Locality::MAX);
		assert_eq!(latest[0].1.hlc.physical_ms, 30);
		assert_eq!(latest[1].1.hlc.physical_ms, 40);
	}

	#[tokio::test]
	async fn scan_as_of_scopes_locality_membership_per_tenant() {
		let mgr = fresh_manager().await;
		let shared_identity = id(0x55);
		let locality = Locality::from(shared_identity);
		let other_tenant = TenantId::new(7);

		mgr.append(tenant(), shared_identity, hlc(10), b"tenant-a")
			.await
			.unwrap();
		mgr.append(other_tenant, shared_identity, hlc(20), b"tenant-b")
			.await
			.unwrap();

		let primary = mgr.scan_as_of(tenant(), locality, locality, hlc(30));
		assert_eq!(primary.len(), 1);
		assert_eq!(primary[0].0, locality.get());
		assert_eq!(primary[0].1.hlc.physical_ms, 10);

		let secondary = mgr.scan_as_of(other_tenant, locality, locality, hlc(30));
		assert_eq!(secondary.len(), 1);
		assert_eq!(secondary[0].0, locality.get());
		assert_eq!(secondary[0].1.hlc.physical_ms, 20);
	}

	#[tokio::test]
	async fn append_durable_coalesces_concurrent_fdatasyncs() {
		let mgr = fresh_manager_with_group_commit(GroupCommitConfig {
			floor: Duration::from_millis(20),
			ceiling: Duration::from_millis(20),
			per_write_cost_estimate: Duration::from_millis(20),
		})
		.await;

		let first = {
			let mgr = mgr.clone();
			tokio::spawn(async move {
				mgr.append_durable(tenant(), id(0x11), hlc(10), b"first")
					.await
					.unwrap();
			})
		};
		tokio::time::sleep(Duration::from_millis(1)).await;
		let second = {
			let mgr = mgr.clone();
			tokio::spawn(async move {
				mgr.append_durable(tenant(), id(0x22), hlc(20), b"second")
					.await
					.unwrap();
			})
		};
		first.await.unwrap();
		second.await.unwrap();

		let fdatasync_calls = {
			let state = mgr.state.lock().await;
			state.active.fdatasync_call_count()
		};
		assert_eq!(fdatasync_calls, 1);
	}

	#[tokio::test]
	async fn reopen_restores_tiered_rows_from_redb() {
		let l1_dir = fresh_dir("tiered-reopen-l1");
		let l2_dir = fresh_dir("tiered-reopen-l2");
		let io = spawn_io_thread(64).unwrap();
		let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&l2_dir).unwrap());
		let crypto = test_crypto();

		let manager = SlabManager::open(SlabManagerConfig {
			dir: l1_dir.clone(),
			max_slab_bytes: 1,
			io: io.clone(),
			l2: Some(l2.clone()),
			rehydration: RehydrationPolicy::default(),
			group_commit: GroupCommitConfig::default(),
			crypto: crypto.clone(),
		})
		.await
		.unwrap();

		let tiered_key = id(0x44);
		let other_key = id(0x99);
		let tiered_bytes = b"tiered-row".to_vec();
		manager
			.append(tenant(), tiered_key, hlc(10), &tiered_bytes)
			.await
			.unwrap();
		manager
			.append(tenant(), other_key, hlc(20), b"active-row")
			.await
			.unwrap();

		let sealed = manager.list_sealed_slabs().await;
		assert_eq!(sealed.len(), 1, "expected one sealed slab after rotation");
		let sealed_id = sealed[0].slab_id;
		let raw = manager.read_sealed_raw(sealed_id).await.unwrap();
		let encrypted = manager.encrypt_tiered_object_blob(sealed_id, &raw).unwrap();
		let payload_hash = crate::hash::content::digest(&encrypted.bytes);
		let uri = l2
			.put(
				&format!("sealed/slab_{sealed_id:016x}.wnd"),
				&encrypted.bytes,
			)
			.await
			.unwrap();
		manager
			.apply_tier_slab(TieredSlabPointer {
				slab_id: sealed_id,
				l2_uri: uri,
				payload_hash,
				is_encrypted: true,
			})
			.await
			.unwrap();
		assert!(manager.is_tiered(sealed_id).await);
		assert!(
			!slab_path(&l1_dir, sealed_id).exists(),
			"tiered slab should be unlinked from L1"
		);

		drop(manager);

		let reopened = SlabManager::open(SlabManagerConfig {
			dir: l1_dir,
			max_slab_bytes: 1,
			io,
			l2: Some(l2),
			rehydration: RehydrationPolicy::default(),
			group_commit: GroupCommitConfig::default(),
			crypto: crypto.clone(),
		})
		.await
		.unwrap();
		assert!(reopened.is_tiered(sealed_id).await);

		let location = reopened
			.lookup(tenant(), tiered_key)
			.expect("tiered key should survive reopen");
		assert_eq!(location.slab_id, sealed_id);
		assert_eq!(reopened.read(location).await.unwrap(), tiered_bytes);
	}

	#[tokio::test]
	async fn cleanup_retention_rewrite_tmp_files_removes_stale_plaintext() {
		let dir = fresh_dir("retention-tmp-cleanup");
		let tmp_path = retention_rewrite_tmp_path(&dir, SlabId::new(0x77));
		std::fs::write(&tmp_path, b"plaintext-leak").unwrap();
		assert!(
			tmp_path.exists(),
			"fixture temp file should exist before open"
		);

		let file_ops = BlockingFileOps;
		cleanup_retention_rewrite_tmp_files(&file_ops, &dir)
			.await
			.unwrap();

		assert!(
			!tmp_path.exists(),
			"startup cleanup should remove stale plaintext retention temp files"
		);
	}

	#[tokio::test]
	async fn reopen_after_tombstone_compaction_preserves_survivor_lookup() {
		let dir = fresh_dir("compaction-redb");
		let io = spawn_io_thread(64).unwrap();
		let manager = Arc::new(
			SlabManager::open(SlabManagerConfig {
				dir: dir.clone(),
				max_slab_bytes: 1024 * 1024,
				io: io.clone(),
				l2: None,
				rehydration: RehydrationPolicy::default(),
				group_commit: GroupCommitConfig::default(),
				crypto: test_crypto(),
			})
			.await
			.unwrap(),
		);

		let removed_key = id(0x1010);
		let survivor_key = id(0x2020);
		manager
			.append_durable(tenant(), removed_key, hlc(10), b"drop-me")
			.await
			.unwrap();
		manager
			.append_durable(tenant(), survivor_key, hlc(20), b"keep-me")
			.await
			.unwrap();
		let sealed_id = force_seal_active(&manager).await;
		manager
			.append_durable(tenant(), id(0x3030), hlc(30), b"active-row")
			.await
			.unwrap();

		let target = Locality::from(removed_key).get();
		let report = manager
			.reclaim_tombstoned_ranges(
				&[MigratedRangeTombstone {
					migration_id: 1,
					range_start: target,
					range_end: target,
					to_epoch: 2,
					completed_by: NodeId {
						id: 7,
						_reserved: 0,
					},
					migration_hlc_floor: hlc(0),
				}],
				1,
			)
			.await
			.unwrap();
		assert_eq!(report.compacted_slabs, 1);
		assert!(
			manager.lookup(tenant(), removed_key).is_none(),
			"reclaimed key should disappear from the in-memory index"
		);

		let survivor = manager
			.lookup(tenant(), survivor_key)
			.expect("survivor should remain indexed");
		assert_ne!(
			survivor.slab_id, sealed_id,
			"compaction should relocate the survivor into a replacement slab"
		);
		assert_eq!(manager.read(survivor).await.unwrap(), b"keep-me");

		drop(manager);

		let reopened = SlabManager::open(SlabManagerConfig {
			dir,
			max_slab_bytes: 1024 * 1024,
			io,
			l2: None,
			rehydration: RehydrationPolicy::default(),
			group_commit: GroupCommitConfig::default(),
			crypto: test_crypto(),
		})
		.await
		.unwrap();
		assert!(
			reopened.lookup(tenant(), removed_key).is_none(),
			"reclaimed key should stay absent after reopen"
		);
		let survivor = reopened
			.lookup(tenant(), survivor_key)
			.expect("survivor should still be indexed after reopen");
		assert_eq!(reopened.read(survivor).await.unwrap(), b"keep-me");
	}

	#[tokio::test]
	async fn retention_pruning_keeps_latest_version_before_floor() {
		let manager = fresh_manager().await;
		let key = id(0x4040);
		manager
			.append_durable(tenant(), key, hlc(10), b"v10")
			.await
			.unwrap();
		manager
			.append_durable(tenant(), key, hlc(20), b"v20")
			.await
			.unwrap();
		manager
			.append_durable(tenant(), key, hlc(30), b"v30")
			.await
			.unwrap();
		let sealed_id = force_seal_active(&manager).await;
		assert_eq!(manager.sealed_count().await, 1);

		let report = manager.prune_versions_older_than(hlc(25), 1).await.unwrap();
		assert_eq!(report.compacted_slabs, 1);
		assert_eq!(report.pruned_versions, 1);
		assert!(report.reclaimed_bytes > 0);

		let as_of_25 = manager
			.lookup_as_of(tenant(), key, hlc(25))
			.expect("anchor version retained");
		assert_ne!(as_of_25.slab_id, sealed_id);
		assert_eq!(as_of_25.hlc.physical_ms, 20);
		assert_eq!(manager.read(as_of_25).await.unwrap(), b"v20");

		let latest = manager.lookup(tenant(), key).expect("latest retained");
		assert_eq!(latest.hlc.physical_ms, 30);
		assert_eq!(manager.read(latest).await.unwrap(), b"v30");

		assert!(
			manager.lookup_as_of(tenant(), key, hlc(15)).is_none(),
			"versions older than the retained window should be pruned"
		);
	}

	#[tokio::test]
	async fn retention_pruning_keeps_latest_when_all_versions_precede_floor() {
		let manager = fresh_manager().await;
		let key = id(0x5050);
		manager
			.append_durable(tenant(), key, hlc(10), b"v10")
			.await
			.unwrap();
		manager
			.append_durable(tenant(), key, hlc(20), b"v20")
			.await
			.unwrap();
		force_seal_active(&manager).await;

		let report = manager
			.prune_versions_older_than(hlc(100), 1)
			.await
			.unwrap();
		assert_eq!(report.compacted_slabs, 1);
		assert_eq!(report.pruned_versions, 1);

		let latest = manager.lookup(tenant(), key).expect("latest retained");
		assert_eq!(latest.hlc.physical_ms, 20);
		assert_eq!(manager.read(latest).await.unwrap(), b"v20");
		assert!(
			manager.lookup_as_of(tenant(), key, hlc(15)).is_none(),
			"earlier history should be removed once the full chain is outside retention"
		);
	}
}
