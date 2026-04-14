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

use crate::schema::HybridTimestamp;
use crate::storage::core::{LocalSlabStore, RecordLocation, StorageEngine};
use crate::storage::io_uring::IoRingHandle;
use crate::storage::l2::ObjectStore;
use async_trait::async_trait;
use dashmap::DashMap;
use std::io::{Error, ErrorKind};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;

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

/// A slab that has been uploaded to L2. The manager tracks this
/// so subsequent reads pivot to the object store instead of the
/// now-unlinked L1 file.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TieredSlabPointer {
    pub slab_id: u64,
    pub l2_uri: String,
    pub payload_hash: [u8; 32],
}

/// Multi-slab composition of [`StorageEngine`].
pub struct SlabManager {
    config: SlabManagerConfig,
    state: Mutex<ManagerState>,
    /// Authoritative key → location index, aggregated across all
    /// active and sealed slabs.
    key_index: DashMap<u64, RecordLocation>,
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
    /// [`ObjectStore::get`]. Entries here shadow any same-id entry
    /// in `sealed` — the tier pivot removes from `sealed` before
    /// inserting here.
    tiered: DashMap<u64, TieredSlabPointer>,
    next_slab_id: u64,
}

impl SlabManager {
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

        let key_index = DashMap::new();
        let sealed: DashMap<u64, Arc<LocalSlabStore>> = DashMap::new();
        let tiered: DashMap<u64, TieredSlabPointer> = DashMap::new();

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
                next_slab_id,
            }),
            key_index,
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
    pub async fn apply_tier_slab(
        &self,
        pointer: TieredSlabPointer,
    ) -> std::io::Result<()> {
        let state = self.state.lock().await;
        // Idempotent: a replay of the Raft TierSlab entry must not
        // fail. If we're already tiered, accept the no-op.
        if state.tiered.contains_key(&pointer.slab_id) {
            return Ok(());
        }
        let _old = state.sealed.remove(&pointer.slab_id);
        state.tiered.insert(pointer.slab_id, pointer.clone());
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

    /// Lookup the L2 pointer for a tiered slab.
    pub async fn tier_pointer(&self, slab_id: u64) -> Option<TieredSlabPointer> {
        self.state
            .lock()
            .await
            .tiered
            .get(&slab_id)
            .map(|p| p.clone())
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
        if let Some(p) = state.tiered.get(&slab_id) {
            return SlabBacking::L2(p.clone());
        }
        if state.active.slab_id() == slab_id {
            return SlabBacking::Local(state.active.clone());
        }
        if let Some(s) = state.sealed.get(&slab_id) {
            return SlabBacking::Local(s.clone());
        }
        SlabBacking::Missing
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
        let slab = self.target_for_append().await?;
        let location = slab.append(key_hash, hlc, frame).await?;
        // The per-slab `append` already inserted into its local
        // index; mirror into the manager's authoritative index so
        // `lookup` stays O(1) without cross-slab scanning.
        self.key_index.insert(key_hash, location);
        Ok(location)
    }

    async fn read(&self, location: RecordLocation) -> std::io::Result<Vec<u8>> {
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
        self.key_index.get(&key_hash).map(|r| *r)
    }
}

impl SlabManager {
    /// Fetch a single record from L2. V1 downloads the whole slab
    /// blob and carves out the record range; a follow-up will
    /// teach [`ObjectStore`] about ranged GETs so only the record
    /// window crosses the wire.
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
        let blob = l2.get(&pointer.l2_uri).await?;
        let offset = location.offset as usize;
        let length = location.length as usize;
        // The record layout in L2 matches L1 exactly — the sweeper
        // uploads the raw slab file verbatim. Re-materialise via
        // the same wire header parsing that `LocalSlabStore::read`
        // uses.
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
        if end > blob.len() || location.length as usize > length.max(header_size + frame_len) {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "L2 record extends past downloaded blob",
            ));
        }
        Ok(blob[offset + header_size..end].to_vec())
    }
}

// ============================================================
//  HELPERS
// ============================================================

fn slab_path(dir: &Path, slab_id: u64) -> PathBuf {
    dir.join(format!("slab_{:016x}.wnd", slab_id))
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

/// Copy every entry from a freshly opened slab's local index into
/// the manager's aggregated index. Called exactly once per slab —
/// at `open` for the first slab and at rotation for every
/// subsequent one (where the drain is trivially empty for a fresh
/// file).
fn drain_slab_index(slab: &LocalSlabStore, key_index: &DashMap<u64, RecordLocation>) {
    for entry in slab.index.iter() {
        key_index.insert(*entry.key(), *entry.value());
    }
}
