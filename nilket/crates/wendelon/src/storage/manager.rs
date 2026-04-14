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
    /// manager's perspective; eligible for tiering once that path
    /// is implemented.
    sealed: DashMap<u64, Arc<LocalSlabStore>>,
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

    /// Find the slab that backs `location`, whether active or sealed.
    async fn slab_for(&self, slab_id: u64) -> Option<Arc<LocalSlabStore>> {
        let state = self.state.lock().await;
        if state.active.slab_id() == slab_id {
            return Some(state.active.clone());
        }
        state.sealed.get(&slab_id).map(|s| s.clone())
    }
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
        let slab = self.slab_for(location.slab_id).await.ok_or_else(|| {
            Error::new(
                ErrorKind::NotFound,
                format!("no slab with id {} under this manager", location.slab_id),
            )
        })?;
        slab.read(location).await
    }

    fn lookup(&self, key_hash: u64) -> Option<RecordLocation> {
        self.key_index.get(&key_hash).map(|r| *r)
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
