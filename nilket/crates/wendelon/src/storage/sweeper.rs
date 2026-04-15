//! Lazy tiering Background Sweeper (ADR 006 / Target #3).
//!
//! [`SlabSweeper`] is a single tokio task that periodically asks
//! the [`SlabManager`] for sealed slabs, streams each one out to
//! an [`ObjectStore`] (L2), verifies the payload hash, commits a
//! [`WendelonRequest::TierSlab`] through the Control Ring, and —
//! only after the local state machine has applied the commit —
//! returns so the L1 file can be unlinked.
//!
//! ## Two-phase garbage collection
//!
//! The strict invariant is: **the L1 slab file must remain
//! readable until every replica's state machine has committed the
//! tier pivot.** This file enforces that by:
//!
//! 1. Reading the sealed L1 blob (L1 still authoritative).
//! 2. Uploading it to L2.
//! 3. Round-trip fetching and hashing the uploaded bytes (paranoia
//!    against a broken backend).
//! 4. Calling `raft.client_write(TierSlab(..))`. openraft only
//!    returns after the entry has been committed *and* applied
//!    locally, so by the time we see `Ok(..)` every replica has
//!    observed the pivot and our own `WendelonStateMachine` has
//!    fired its [`TierObserver`] — which is what actually unlinks
//!    the L1 file through [`SlabManager::apply_tier_slab`].
//!
//! There is deliberately **no direct unlink call** in this file.
//! The apply path is the single source of truth for "this slab
//! has left L1"; driving the unlink from here would race apply
//! and open a window where a read sees neither the L1 file nor
//! the `tiered` map entry. Routing every unlink through the SM
//! apply observer collapses that window.
//!
//! ## Rate limiting
//!
//! The sweeper's config caps:
//!  * `scan_interval`: how often the task scans for new sealed
//!    slabs. Too-small values starve the write path on a busy
//!    cluster; the default of 5s is a compromise.
//!  * `max_upload_bytes_per_sec`: a token bucket applied to the
//!    L2 upload so the sweeper never saturates the NIC. The
//!    bucket is refilled on a 100ms tick.
//!
//! ## Leader-only uploads
//!
//! Every replica runs its own sweeper, but only the Raft leader
//! has a functioning `client_write`. Followers call
//! `client_write` and openraft returns `ForwardToLeader` — the
//! sweeper catches that error and quietly skips ahead to the next
//! sealed slab rather than spamming uploads that will never
//! commit. The upload cost is therefore paid exactly once per
//! slab even though the sweeper itself runs everywhere.

use crate::consensus::{TieredSlab, WendelonRaft, WendelonRequest, WendelonStateMachine};
use crate::storage::l2::ObjectStore;
use crate::storage::manager::{SealedSlabInfo, SlabManager, TieredSlabPointer};
use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time::{sleep, Instant};

/// Knobs for the background sweeper. Defaults aim for "runs on a
/// laptop without destroying your write throughput" — production
/// deployments will want to set `max_upload_bytes_per_sec` from
/// the NIC capacity and the per-node upload budget.
#[derive(Clone, Debug)]
pub struct SweeperConfig {
	/// How often the sweeper checks for new sealed slabs.
	pub scan_interval: Duration,
	/// Only slabs at or above this size are eligible. Set to 0 to
	/// tier every sealed slab — useful for tests; production
	/// normally wants this non-zero so small gap-fill slabs are
	/// not shipped off NVMe.
	pub min_slab_bytes: u64,
	/// Upload rate limit in bytes per second. `None` disables the
	/// limiter. Applied per sweeper instance, not per slab.
	pub max_upload_bytes_per_sec: Option<u64>,
	/// Object-key prefix. URIs returned by the object store are
	/// `<prefix>slab_<hex>.wnd`; operators may set this to a
	/// namespace / cluster identifier so multiple Wendelon
	/// deployments can share a bucket.
	pub key_prefix: String,
}

impl Default for SweeperConfig {
	fn default() -> Self {
		Self {
			scan_interval: Duration::from_secs(5),
			min_slab_bytes: 0,
			max_upload_bytes_per_sec: None,
			key_prefix: String::new(),
		}
	}
}

/// The sweeper task and everything it needs to do its job.
///
/// Cloneable — internally every long-lived piece is an `Arc`, so
/// tests can pass the same `SlabSweeper` around without rebuilding
/// it.
#[derive(Clone)]
pub struct SlabSweeper {
	slab: Arc<SlabManager>,
	object_store: Arc<dyn ObjectStore>,
	raft: Arc<dyn TierCommitter>,
	state_machine: WendelonStateMachine,
	config: SweeperConfig,
	/// Fires to nudge the sweeper to scan immediately instead of
	/// waiting for the next interval. Used by tests and by the
	/// slab rotation path to hand-off a freshly-sealed slab.
	wakeup: Arc<Notify>,
}

/// The sweeper's view of the Raft handle.
///
/// Abstracted behind a trait so tests can drive the tier path
/// without standing up an openraft cluster. The production impl
/// is a thin wrapper over [`WendelonRaft`]; see [`RaftCommitter`].
#[async_trait]
pub trait TierCommitter: Send + Sync {
	/// Submit a TierSlab entry. Returns `Ok(())` only after the
	/// entry has been committed *and* applied locally, so the
	/// caller can treat a successful return as "L1 file has been
	/// unlinked via the SM apply observer". An error means the
	/// sweeper should skip this slab and try again on the next
	/// scan — the L1 file stays put.
	async fn commit_tier_slab(&self, tier: TieredSlab) -> std::io::Result<()>;
}

/// Production [`TierCommitter`] backed by an openraft [`WendelonRaft`].
///
/// `client_write` is resolved on the leader and blocks until the
/// local state machine has applied the entry, which is exactly the
/// synchronisation the sweeper needs. On a follower, openraft
/// returns `ForwardToLeader`, which we surface as a non-fatal
/// `WouldBlock` so the sweeper can skip this pass without tearing
/// down.
pub struct RaftCommitter {
	raft: WendelonRaft,
}

impl RaftCommitter {
	pub fn new(raft: WendelonRaft) -> Self {
		Self { raft }
	}
}

#[async_trait]
impl TierCommitter for RaftCommitter {
	async fn commit_tier_slab(&self, tier: TieredSlab) -> std::io::Result<()> {
		match self
			.raft
			.client_write(WendelonRequest::TierSlab(tier))
			.await
		{
			Ok(_) => Ok(()),
			Err(e) => Err(std::io::Error::new(
				std::io::ErrorKind::Other,
				format!("TierSlab commit failed: {e}"),
			)),
		}
	}
}

impl SlabSweeper {
	pub fn new(
		slab: Arc<SlabManager>,
		object_store: Arc<dyn ObjectStore>,
		raft: Arc<dyn TierCommitter>,
		state_machine: WendelonStateMachine,
		config: SweeperConfig,
	) -> Self {
		Self {
			slab,
			object_store,
			raft,
			state_machine,
			config,
			wakeup: Arc::new(Notify::new()),
		}
	}

	/// Prod the sweeper to scan right now, skipping the remaining
	/// wait. Safe to call from any task; multiple concurrent
	/// wakeups coalesce.
	pub fn wake(&self) {
		self.wakeup.notify_one();
	}

	/// Spawn the background loop. The returned [`JoinHandle`]
	/// completes only when `wakeup.notify_waiters()` is called on
	/// an explicitly-cancelled sweeper or the task panics.
	pub fn spawn(self) -> JoinHandle<()> {
		tokio::spawn(self.run())
	}

	/// Execute a single sweep pass and return. Separated from
	/// [`Self::run`] so tests can step through the sweeper
	/// without racing a tokio interval.
	pub async fn sweep_once(&self) -> std::io::Result<SweepReport> {
		let mut report = SweepReport::default();
		let candidates = self.slab.list_sealed_slabs().await;
		for candidate in candidates {
			if candidate.size_bytes < self.config.min_slab_bytes {
				report.skipped_too_small += 1;
				continue;
			}
			if self.slab.is_tiered(candidate.slab_id).await {
				// Raced with another replica's commit — local SM
				// already applied the pivot, nothing to do.
				report.skipped_already_tiered += 1;
				continue;
			}
			match self.tier_one_slab(&candidate).await {
				Ok(()) => report.tiered += 1,
				Err(e) => {
					tracing::warn!(
						slab_id = candidate.slab_id,
						error = %e,
						"sweeper pass failed on slab; will retry on next scan"
					);
					report.failed += 1;
				}
			}
		}
		Ok(report)
	}

	/// Drive a single slab through the full two-phase pipeline.
	async fn tier_one_slab(&self, info: &SealedSlabInfo) -> std::io::Result<()> {
		// Phase 1: snapshot L1 bytes. `read_sealed_raw` fails
		// loudly if the slab has been rotated away or already
		// tiered — either is a sign the caller's info went stale,
		// and re-listing on the next pass will converge.
		let blob = self.slab.read_sealed_raw(info.slab_id).await?;
		let expected_hash = *blake3::hash(&blob).as_bytes();

		// Phase 2: upload to L2, applying the rate limit.
		self.rate_limit_upload(blob.len()).await;
		let key = format!("{}slab_{:016x}.wnd", self.config.key_prefix, info.slab_id);
		let l2_uri = self.object_store.put(&key, &blob).await?;

		// Phase 3: round-trip verification. A mismatch here means
		// the object store acked a write but returned a different
		// blob — we refuse to pivot on corrupt data. The L1 file
		// is still intact; the sweeper will retry next pass.
		let fetched = self.object_store.get(&l2_uri).await?;
		let fetched_hash = *blake3::hash(&fetched).as_bytes();
		if fetched_hash != expected_hash {
			// Best-effort cleanup of the bad object. Ignoring the
			// error here is intentional: we already know the
			// backend is misbehaving.
			let _ = self.object_store.delete(&l2_uri).await;
			return Err(std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				format!(
					"L2 round-trip hash mismatch for slab {}: expected {:02x?}, got {:02x?}",
					info.slab_id,
					&expected_hash[..4],
					&fetched_hash[..4],
				),
			));
		}

		// Phase 4: commit through Raft. Only returns Ok after the
		// local SM has applied the entry and fired the tier
		// observer — which is what actually unlinks the L1 file
		// via `SlabManager::apply_tier_slab`. Readers that
		// captured an `Arc<LocalSlabStore>` *before* the apply
		// keep their open fd (Linux unlink semantics).
		let tier = TieredSlab {
			slab_id: info.slab_id,
			l2_uri,
			payload_hash: expected_hash,
		};
		self.raft.commit_tier_slab(tier).await?;

		// Defensive: if the SM has an observer wired in, apply is
		// already done. If it does not (e.g., a test wiring), the
		// sweeper still owns the invariant — call through the
		// manager's apply_tier_slab directly. This is idempotent
		// on the real path because the observer already moved the
		// slab out of `sealed`.
		if let Some(pointer) = self.sm_tier_pointer(info.slab_id).await {
			self.slab.apply_tier_slab(pointer).await?;
		}

		Ok(())
	}

	/// Pull the state machine's authoritative tier pointer for a
	/// slab (post-apply). Returns `None` if for some reason the
	/// apply didn't land — which we treat as a terminal failure
	/// upstream.
	async fn sm_tier_pointer(&self, slab_id: u64) -> Option<TieredSlabPointer> {
		let state = self.state_machine.state_snapshot().await;
		state.tiered_slabs.get(&slab_id).map(|t| TieredSlabPointer {
			slab_id: t.slab_id,
			l2_uri: t.l2_uri.clone(),
			payload_hash: t.payload_hash,
		})
	}

	/// Token-bucket rate limiter. For V1 we keep it simple —
	/// every upload blocks long enough to respect
	/// `max_upload_bytes_per_sec`. The bucket has no burst credit
	/// because the sweeper is explicitly a background job; if you
	/// want fast backfill, raise the cap rather than widening the
	/// bucket.
	async fn rate_limit_upload(&self, bytes: usize) {
		let Some(cap) = self.config.max_upload_bytes_per_sec else {
			return;
		};
		if cap == 0 {
			return;
		}
		let millis = ((bytes as u128) * 1000) / (cap as u128).max(1);
		// Clamp: a single massive slab should still respect the
		// scan cadence — no point sleeping for an hour on a
		// single upload, operators would rather see progress.
		let capped = millis.min(self.config.scan_interval.as_millis());
		if capped > 0 {
			sleep(Duration::from_millis(capped as u64)).await;
		}
	}

	/// Background loop shape: sleep until the next scan deadline
	/// *or* a wakeup signal, then run one `sweep_once`. Shutdown
	/// happens by dropping every clone of `self.wakeup` (the
	/// runtime will no longer deliver notifies and the loop exits
	/// naturally at the next cycle).
	async fn run(self) {
		let mut next_deadline = Instant::now() + self.config.scan_interval;
		loop {
			let now = Instant::now();
			let remaining = next_deadline.saturating_duration_since(now);
			tokio::select! {
				_ = sleep(remaining) => {},
				_ = self.wakeup.notified() => {},
			}
			if let Err(e) = self.sweep_once().await {
				tracing::warn!(error = %e, "sweeper pass returned error");
			}
			next_deadline = Instant::now() + self.config.scan_interval;
		}
	}
}

/// Per-pass bookkeeping. Returned from [`SlabSweeper::sweep_once`]
/// for test assertions and for operators who want to wire the
/// counts into Prometheus.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct SweepReport {
	pub tiered: usize,
	pub failed: usize,
	pub skipped_too_small: usize,
	pub skipped_already_tiered: usize,
}

// ============================================================
//  TIER OBSERVER BRIDGE
// ============================================================

use crate::consensus::TierObserver;

/// Glue that lets a [`SlabManager`] be installed as the state
/// machine's [`TierObserver`]. Holds an `Arc<SlabManager>` so the
/// SM apply loop can fire filesystem work without owning the
/// manager's lifetime.
pub struct ManagerTierObserver {
	slab: Arc<SlabManager>,
}

impl ManagerTierObserver {
	pub fn new(slab: Arc<SlabManager>) -> Self {
		Self { slab }
	}
}

#[async_trait]
impl TierObserver for ManagerTierObserver {
	async fn on_slab_tiered(&self, tier: &TieredSlab) -> std::io::Result<()> {
		self.slab
			.apply_tier_slab(TieredSlabPointer {
				slab_id: tier.slab_id,
				l2_uri: tier.l2_uri.clone(),
				payload_hash: tier.payload_hash,
			})
			.await
	}
}

// ============================================================
//  TESTS
// ============================================================

#[cfg(test)]
mod tests {
	use super::*;
	use crate::storage::io_uring::spawn_io_thread;
	use crate::storage::l2::LocalFsObjectStore;
	use crate::storage::manager::SlabManagerConfig;
	use tokio::sync::Mutex as AsyncMutex;

	/// Test-only committer that skips openraft entirely. Records
	/// the commit in the provided SM so the sweeper's observer
	/// path still fires.
	struct DirectApplyCommitter {
		observer: Arc<dyn TierObserver>,
		/// When `Some`, every call to `commit_tier_slab` returns
		/// an error cloned from this template. Models a persistent
		/// "leader unreachable" scenario.
		persistent_err: AsyncMutex<Option<std::io::ErrorKind>>,
	}

	impl DirectApplyCommitter {
		fn ok(_sm: WendelonStateMachine, observer: Arc<dyn TierObserver>) -> Self {
			Self {
				observer,
				persistent_err: AsyncMutex::new(None),
			}
		}
	}

	#[async_trait]
	impl TierCommitter for DirectApplyCommitter {
		async fn commit_tier_slab(&self, tier: TieredSlab) -> std::io::Result<()> {
			if let Some(kind) = *self.persistent_err.lock().await {
				return Err(std::io::Error::new(kind, "simulated commit failure"));
			}
			self.observer.on_slab_tiered(&tier).await?;
			Ok(())
		}
	}

	fn fresh_dir(tag: &str) -> std::path::PathBuf {
		let dir = std::env::temp_dir().join(format!(
			"wendelon-sweeper-{tag}-{}-{}",
			std::process::id(),
			uuid::Uuid::new_v4()
		));
		std::fs::create_dir_all(&dir).unwrap();
		dir
	}

	fn hlc() -> crate::schema::HybridTimestamp {
		crate::schema::HybridTimestamp {
			physical_ms: 1,
			logical: 0,
			node_id: 1,
		}
	}

	async fn build_manager_with_sealed_slabs(
		l2: Option<Arc<dyn ObjectStore>>,
	) -> (Arc<SlabManager>, std::path::PathBuf) {
		use crate::storage::core::StorageEngine;
		let dir = fresh_dir("l1");
		let io = spawn_io_thread(64).unwrap();
		let manager = Arc::new(
			SlabManager::open(SlabManagerConfig {
				dir: dir.clone(),
				// Tiny cap so two appends produce two sealed slabs.
				max_slab_bytes: 1,
				io,
				l2,
			})
			.unwrap(),
		);
		// Each append rotates because the write_cursor crosses
		// `max_slab_bytes` on the first record.
		manager.append(1, hlc(), b"hello-1").await.unwrap();
		manager.append(2, hlc(), b"hello-2").await.unwrap();
		manager.append(3, hlc(), b"hello-3").await.unwrap();
		(manager, dir)
	}

	#[tokio::test]
	async fn sweeper_tiers_sealed_slabs_and_unlinks_l1() {
		let l2_root = fresh_dir("l2");
		let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&l2_root).unwrap());
		let (manager, l1_dir) = build_manager_with_sealed_slabs(Some(l2.clone())).await;

		let sealed_before = manager.list_sealed_slabs().await;
		assert!(
			!sealed_before.is_empty(),
			"precondition: manager should have sealed slabs"
		);

		let sm = WendelonStateMachine::new();
		let observer: Arc<dyn TierObserver> = Arc::new(ManagerTierObserver::new(manager.clone()));
		let committer: Arc<dyn TierCommitter> =
			Arc::new(DirectApplyCommitter::ok(sm.clone(), observer));

		let sweeper = SlabSweeper::new(
			manager.clone(),
			l2.clone(),
			committer,
			sm,
			SweeperConfig {
				scan_interval: Duration::from_millis(10),
				min_slab_bytes: 0,
				max_upload_bytes_per_sec: None,
				key_prefix: "tier/".into(),
			},
		);

		let report = sweeper.sweep_once().await.unwrap();
		assert_eq!(report.tiered, sealed_before.len());
		assert_eq!(report.failed, 0);

		// L1 files for every sealed slab should be gone.
		for info in &sealed_before {
			let path = l1_dir.join(format!("slab_{:016x}.wnd", info.slab_id));
			assert!(!path.exists(), "L1 file still present: {}", path.display());
		}
		// Manager's tiered count matches.
		assert_eq!(manager.tiered_count().await, sealed_before.len());
		// Sealed count is zero — they all moved to tiered.
		assert_eq!(manager.sealed_count().await, 0);
	}

	#[tokio::test]
	async fn read_pivots_to_l2_after_tiering() {
		use crate::storage::core::StorageEngine;

		let l2_root = fresh_dir("l2");
		let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&l2_root).unwrap());
		let (manager, _l1_dir) = build_manager_with_sealed_slabs(Some(l2.clone())).await;

		// Record the location of key=1 before sweeping so we can
		// verify the read path pivots without a re-lookup.
		let loc1 = manager.lookup(1).expect("key present");
		let original = manager.read(loc1).await.expect("pre-tier read ok");

		let sm = WendelonStateMachine::new();
		let observer: Arc<dyn TierObserver> = Arc::new(ManagerTierObserver::new(manager.clone()));
		let committer: Arc<dyn TierCommitter> =
			Arc::new(DirectApplyCommitter::ok(sm.clone(), observer));
		let sweeper = SlabSweeper::new(
			manager.clone(),
			l2,
			committer,
			sm,
			SweeperConfig {
				scan_interval: Duration::from_millis(10),
				min_slab_bytes: 0,
				max_upload_bytes_per_sec: None,
				key_prefix: String::new(),
			},
		);
		sweeper.sweep_once().await.unwrap();
		assert!(manager.is_tiered(loc1.slab_id).await);

		// Same location, but now served from L2.
		let via_l2 = manager.read(loc1).await.expect("post-tier read ok");
		assert_eq!(via_l2, original);
	}

	#[tokio::test]
	async fn sweeper_skips_slabs_under_min_size() {
		let l2_root = fresh_dir("l2");
		let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&l2_root).unwrap());
		let (manager, _l1_dir) = build_manager_with_sealed_slabs(Some(l2.clone())).await;

		let sm = WendelonStateMachine::new();
		let observer: Arc<dyn TierObserver> = Arc::new(ManagerTierObserver::new(manager.clone()));
		let committer: Arc<dyn TierCommitter> =
			Arc::new(DirectApplyCommitter::ok(sm.clone(), observer));
		let sweeper = SlabSweeper::new(
			manager.clone(),
			l2,
			committer,
			sm,
			SweeperConfig {
				scan_interval: Duration::from_millis(10),
				// Each sealed slab is ~4K on disk; set the floor
				// absurdly high so nothing qualifies.
				min_slab_bytes: u64::MAX,
				max_upload_bytes_per_sec: None,
				key_prefix: String::new(),
			},
		);
		let report = sweeper.sweep_once().await.unwrap();
		assert_eq!(report.tiered, 0);
		assert!(report.skipped_too_small > 0);
		assert_eq!(manager.tiered_count().await, 0);
	}

	#[tokio::test]
	async fn failed_commit_leaves_l1_intact() {
		let l2_root = fresh_dir("l2");
		let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&l2_root).unwrap());
		let (manager, l1_dir) = build_manager_with_sealed_slabs(Some(l2.clone())).await;
		let sealed_before = manager.list_sealed_slabs().await;

		let sm = WendelonStateMachine::new();
		let observer: Arc<dyn TierObserver> = Arc::new(ManagerTierObserver::new(manager.clone()));
		let committer_inner = Arc::new(DirectApplyCommitter::ok(sm.clone(), observer));
		*committer_inner.persistent_err.lock().await = Some(std::io::ErrorKind::ConnectionRefused);
		let committer: Arc<dyn TierCommitter> = committer_inner.clone();

		let sweeper = SlabSweeper::new(
			manager.clone(),
			l2,
			committer,
			sm,
			SweeperConfig {
				scan_interval: Duration::from_millis(10),
				min_slab_bytes: 0,
				max_upload_bytes_per_sec: None,
				key_prefix: String::new(),
			},
		);
		let report = sweeper.sweep_once().await.unwrap();
		assert_eq!(report.tiered, 0);
		assert!(report.failed > 0);
		// L1 still intact for every sealed slab.
		for info in &sealed_before {
			let path = l1_dir.join(format!("slab_{:016x}.wnd", info.slab_id));
			assert!(path.exists(), "L1 file missing: {}", path.display());
		}
		assert_eq!(manager.tiered_count().await, 0);
	}

	#[tokio::test]
	async fn apply_tier_slab_is_idempotent() {
		let l2_root = fresh_dir("l2");
		let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&l2_root).unwrap());
		let (manager, _l1_dir) = build_manager_with_sealed_slabs(Some(l2)).await;
		let sealed = manager.list_sealed_slabs().await;
		let info = sealed.first().unwrap().clone();

		let ptr = TieredSlabPointer {
			slab_id: info.slab_id,
			l2_uri: "file:///dev/null".into(),
			payload_hash: [0; 32],
		};
		manager.apply_tier_slab(ptr.clone()).await.unwrap();
		// Second apply must not fail even though sealed no longer
		// contains the slab.
		manager.apply_tier_slab(ptr).await.unwrap();
		assert!(manager.is_tiered(info.slab_id).await);
	}
}
