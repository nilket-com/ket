#[path = "../src/test_support/test_dir.rs"]
mod test_dir;
use async_trait::async_trait;

async fn initialize_test_manager(
	config: SlabManagerConfig,
) -> std::io::Result<std::sync::Arc<SlabManager>> {
	let owner = wendelon::storage::ownership::DataDirectoryOwner::acquire_existing(&config.dir)?;
	SlabManager::initialize_new_store_with_owner(config, owner).await
}
use openraft::storage::RaftStateMachine;
use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::Mutex as AsyncMutex;
use wendelon::consensus::{
	RetireTieredSlab, TierObserver, WendelonRequest, WendelonStateMachine, WendelonTypeConfig,
};
use wendelon::crypto::{CryptoManager, test_support};
use wendelon::schema::{HybridTimestamp, Identity, TenantId};
use wendelon::storage::{
	LocalFsObjectStore, ManagerTierObserver, ObjectStore, RehydrationPolicy, SlabManager,
	SlabManagerConfig, SlabSweeper, StorageEngine, SweeperConfig, TierCommitter, spawn_io_thread,
};

fn fresh_dir(tag: &str) -> test_dir::TestDir {
	test_dir::TestDir::new(&format!("wendelon-l2-rehydration-{tag}"))
}

fn hlc() -> HybridTimestamp {
	HybridTimestamp {
		physical_ms: 1,
		logical: 0,
		node_id: 1,
	}
}

fn id(locality: u64) -> Identity {
	let mut bytes = [0u8; 32];
	bytes[..8].copy_from_slice(&locality.to_le_bytes());
	Identity::new(bytes)
}

fn tenant() -> TenantId {
	TenantId::ZERO
}

fn test_crypto() -> Arc<CryptoManager> {
	Arc::new(test_support::seeded_dev_crypto_manager(tenant()).unwrap())
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct ObjectStoreCallCounts {
	head: usize,
	get_range: usize,
	get_bytes: usize,
}

struct CountingObjectStore {
	inner: Arc<dyn ObjectStore>,
	head_calls: AtomicUsize,
	get_range_calls: AtomicUsize,
	get_bytes_calls: AtomicUsize,
}

impl CountingObjectStore {
	fn new(inner: Arc<dyn ObjectStore>) -> Self {
		Self {
			inner,
			head_calls: AtomicUsize::new(0),
			get_range_calls: AtomicUsize::new(0),
			get_bytes_calls: AtomicUsize::new(0),
		}
	}

	fn snapshot(&self) -> ObjectStoreCallCounts {
		ObjectStoreCallCounts {
			head: self.head_calls.load(Ordering::Relaxed),
			get_range: self.get_range_calls.load(Ordering::Relaxed),
			get_bytes: self.get_bytes_calls.load(Ordering::Relaxed),
		}
	}
}

#[async_trait]
impl ObjectStore for CountingObjectStore {
	fn planned_upload_uri(&self, key: &str) -> std::io::Result<String> {
		self.inner.planned_upload_uri(key)
	}

	async fn put(&self, key: &str, bytes: &[u8]) -> std::io::Result<String> {
		self.inner.put(key, bytes).await
	}

	async fn head(&self, uri: &str) -> std::io::Result<wendelon::storage::ObjectMetadata> {
		self.head_calls.fetch_add(1, Ordering::Relaxed);
		self.inner.head(uri).await
	}

	async fn get_range(
		&self,
		uri: &str,
		range: std::ops::Range<u64>,
	) -> std::io::Result<bytes::Bytes> {
		self.get_range_calls.fetch_add(1, Ordering::Relaxed);
		self.inner.get_range(uri, range).await
	}

	async fn get_bytes(&self, uri: &str) -> std::io::Result<bytes::Bytes> {
		self.get_bytes_calls.fetch_add(1, Ordering::Relaxed);
		self.inner.get_bytes(uri).await
	}

	async fn list(&self, prefix: &str) -> std::io::Result<Vec<wendelon::storage::ObjectMetadata>> {
		self.inner.list(prefix).await
	}

	async fn delete(&self, uri: &str) -> std::io::Result<()> {
		self.inner.delete(uri).await
	}
}

struct DirectApplyCommitter {
	observer: Arc<dyn TierObserver>,
	sm: Arc<AsyncMutex<WendelonStateMachine>>,
	next_index: AtomicU64,
}

impl DirectApplyCommitter {
	fn new(observer: Arc<dyn TierObserver>, state_machine: WendelonStateMachine) -> Self {
		Self {
			observer,
			sm: Arc::new(AsyncMutex::new(state_machine)),
			next_index: AtomicU64::new(1),
		}
	}
}

#[async_trait]
impl TierCommitter for DirectApplyCommitter {
	async fn commit_tier_slab(
		&self,
		proposal: wendelon::storage::TierProposal,
	) -> std::io::Result<()> {
		let tier = proposal.tier().clone();
		let index = self.next_index.fetch_add(1, Ordering::Relaxed);
		let entry = Entry::<WendelonTypeConfig> {
			log_id: LogId::new(CommittedLeaderId::new(1, 0), index),
			payload: EntryPayload::Normal(WendelonRequest::TierSlab(tier.clone())),
		};
		let mut sm = self.sm.lock().await;
		<WendelonStateMachine as RaftStateMachine<WendelonTypeConfig>>::apply(
			&mut *sm,
			vec![entry],
		)
		.await
		.map_err(|err| std::io::Error::other(format!("test tier commit failed: {err}")))?;
		self.observer.on_slab_tiered(&tier, 0).await
	}

	async fn retire_tiered_slab(&self, slab_id: wendelon::schema::SlabId) -> std::io::Result<()> {
		let index = self.next_index.fetch_add(1, Ordering::Relaxed);
		let entry = Entry::<WendelonTypeConfig> {
			log_id: LogId::new(CommittedLeaderId::new(1, 0), index),
			payload: EntryPayload::Normal(WendelonRequest::RetireTieredSlab(RetireTieredSlab {
				slab_id,
			})),
		};
		let mut sm = self.sm.lock().await;
		<WendelonStateMachine as RaftStateMachine<WendelonTypeConfig>>::apply(
			&mut *sm,
			vec![entry],
		)
		.await
		.map_err(|err| std::io::Error::other(format!("test retire commit failed: {err}")))?;
		self.observer.on_slab_retired(slab_id, 0).await
	}
}

async fn build_tiered_manager(
	rehydration: RehydrationPolicy,
) -> (
	test_dir::TestDir,
	test_dir::TestDir,
	Arc<SlabManager>,
	Arc<CountingObjectStore>,
	SlabSweeper,
	wendelon::storage::RecordLocation,
	Vec<u8>,
) {
	let l1_dir = fresh_dir("l1");
	let l2_dir = fresh_dir("l2");
	let io = spawn_io_thread(64).unwrap();
	let l2_inner: Arc<dyn ObjectStore> =
		Arc::new(LocalFsObjectStore::new(l2_dir.to_path_buf()).unwrap());
	let l2 = Arc::new(CountingObjectStore::new(l2_inner));
	let l2_store: Arc<dyn ObjectStore> = l2.clone();
	let manager = initialize_test_manager(SlabManagerConfig {
		tier_history_budget_bytes: wendelon::storage::DEFAULT_TIER_HISTORY_BUDGET_BYTES,
		dir: l1_dir.to_path_buf(),
		max_slab_bytes: 1,
		io,
		l2: Some(l2_store.clone()),
		rehydration,
		l2_column_fetch: Default::default(),
		row_group_writer: Default::default(),
		l2_row_group_fetch: Default::default(),
		encoding_writer: Default::default(),
		l2_chunk_layout: Default::default(),
		l2_encoding_fetch: Default::default(),
		encoding_compute: Default::default(),
		group_commit: wendelon::storage::GroupCommitConfig::default(),
		crypto: test_crypto(),
	})
	.await
	.unwrap();

	manager
		.append(tenant(), id(1), hlc(), b"hello-1")
		.await
		.unwrap();
	manager
		.append(tenant(), id(2), hlc(), b"hello-2")
		.await
		.unwrap();
	manager
		.append(tenant(), id(3), hlc(), b"hello-3")
		.await
		.unwrap();

	let location = manager
		.lookup(tenant(), id(1))
		.unwrap()
		.expect("key present");
	let original = manager.read(location).await.expect("pre-tier read ok");

	let observer: Arc<dyn TierObserver> = Arc::new(ManagerTierObserver::new(manager.clone()));
	let state_machine = WendelonStateMachine::new();
	let committer: Arc<dyn TierCommitter> =
		Arc::new(DirectApplyCommitter::new(observer, state_machine.clone()));
	let sweeper = SlabSweeper::new(
		manager.clone(),
		l2_store.clone(),
		committer,
		state_machine,
		SweeperConfig {
			scan_interval: Duration::from_millis(10),
			min_slab_bytes: 0,
			max_upload_bytes_per_sec: None,
			key_prefix: String::new(),
			max_plaintext_rewrites_per_pass: 1,
			max_dek_rewraps_per_pass: 1,
		},
	);
	let report = sweeper.sweep_once().await.unwrap();
	assert!(
		report.tiered > 0,
		"precondition: expected at least one tiered slab"
	);
	assert!(manager.is_tiered(location.slab_id).await);

	(l2_dir, l1_dir, manager, l2, sweeper, location, original)
}

#[tokio::test]
async fn cold_l2_read_uses_head_and_range_fetches_before_rehydration() {
	let policy = RehydrationPolicy {
		promote_after_l2_reads: 2,
		min_local_residency: Duration::from_secs(60),
		tail_read_window_bytes: 280,
	};
	let (_l2_dir, _l1_dir, manager, l2, _sweeper, location, original) =
		build_tiered_manager(policy).await;

	let before = l2.snapshot();
	let first = manager.read(location).await.unwrap();
	let after = l2.snapshot();

	assert_eq!(first, original);
	assert_eq!(after.head - before.head, 1);
	assert_eq!(after.get_range - before.get_range, 3);
	assert_eq!(after.get_bytes - before.get_bytes, 0);
	assert!(!manager.is_rehydrated(location.slab_id).await);
}

#[tokio::test]
async fn multiple_l2_reads_trigger_promotion_to_l1() {
	let policy = RehydrationPolicy {
		promote_after_l2_reads: 2,
		min_local_residency: Duration::from_secs(60),
		tail_read_window_bytes: RehydrationPolicy::default().tail_read_window_bytes,
	};
	let (_l2_dir, l1_dir, manager, _l2, _sweeper, location, original) =
		build_tiered_manager(policy).await;

	assert!(!manager.is_rehydrated(location.slab_id).await);

	let first = manager.read(location).await.unwrap();
	assert_eq!(first, original);
	assert!(!manager.is_rehydrated(location.slab_id).await);

	let second = manager.read(location).await.unwrap();
	assert_eq!(second, original);
	assert!(manager.is_rehydrated(location.slab_id).await);
	assert_eq!(manager.rehydrated_count().await, 1);

	let local_path = l1_dir.join(format!("slab_{:016x}.wnd", location.slab_id));
	assert!(local_path.exists(), "rehydrated slab file missing");
}

#[tokio::test]
async fn promoted_slab_is_served_from_local_and_metadata_stays_tiered() {
	let policy = RehydrationPolicy {
		promote_after_l2_reads: 2,
		min_local_residency: Duration::from_secs(60),
		tail_read_window_bytes: RehydrationPolicy::default().tail_read_window_bytes,
	};
	let (_l2_dir, _l1_dir, manager, l2, _sweeper, location, original) =
		build_tiered_manager(policy).await;

	manager.read(location).await.unwrap();
	manager.read(location).await.unwrap();

	assert!(manager.is_rehydrated(location.slab_id).await);
	assert!(manager.is_tiered(location.slab_id).await);

	let pointer = manager
		.tier_pointer(location.slab_id)
		.await
		.expect("tier pointer retained");
	l2.delete(&pointer.l2_uri).await.unwrap();

	let local = manager.read(location).await.unwrap();
	assert_eq!(local, original);
}

#[tokio::test]
async fn thrash_guard_blocks_immediate_re_demote() {
	let policy = RehydrationPolicy {
		promote_after_l2_reads: 2,
		min_local_residency: Duration::from_millis(50),
		tail_read_window_bytes: RehydrationPolicy::default().tail_read_window_bytes,
	};
	let (_l2_dir, l1_dir, manager, _l2, sweeper, location, original) =
		build_tiered_manager(policy).await;

	manager.read(location).await.unwrap();
	manager.read(location).await.unwrap();
	assert!(manager.is_rehydrated(location.slab_id).await);

	let guarded = sweeper.sweep_once().await.unwrap();
	assert_eq!(guarded.rehydrated_reclaimed, 0);
	assert_eq!(guarded.skipped_rehydration_guard, 1);
	assert!(manager.is_rehydrated(location.slab_id).await);

	tokio::time::sleep(Duration::from_millis(60)).await;

	let reclaimed = sweeper.sweep_once().await.unwrap();
	assert_eq!(reclaimed.rehydrated_reclaimed, 1);
	assert!(!manager.is_rehydrated(location.slab_id).await);

	let local_path = l1_dir.join(format!("slab_{:016x}.wnd", location.slab_id));
	assert!(
		!local_path.exists(),
		"cooled rehydrated slab should be reclaimed"
	);
	assert_eq!(manager.read(location).await.unwrap(), original);
}
