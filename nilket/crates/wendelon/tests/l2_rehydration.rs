use async_trait::async_trait;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use wendelon::consensus::{TierObserver, TieredSlab, WendelonStateMachine};
use wendelon::crypto::{CryptoManager, test_support};
use wendelon::schema::{HybridTimestamp, Identity, TenantId};
use wendelon::storage::{
	LocalFsObjectStore, ManagerTierObserver, ObjectStore, RehydrationPolicy, SlabManager,
	SlabManagerConfig, SlabSweeper, StorageEngine, SweeperConfig, TierCommitter, spawn_io_thread,
};

fn fresh_dir(tag: &str) -> PathBuf {
	let dir = std::env::temp_dir().join(format!(
		"wendelon-l2-rehydration-{tag}-{}-{}",
		std::process::id(),
		uuid::Uuid::new_v4()
	));
	std::fs::create_dir_all(&dir).unwrap();
	dir
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

struct DirectApplyCommitter {
	observer: Arc<dyn TierObserver>,
}

#[async_trait]
impl TierCommitter for DirectApplyCommitter {
	async fn commit_tier_slab(&self, tier: TieredSlab) -> std::io::Result<()> {
		self.observer.on_slab_tiered(&tier, 0).await
	}
}

async fn build_tiered_manager(
	rehydration: RehydrationPolicy,
) -> (
	Arc<SlabManager>,
	Arc<dyn ObjectStore>,
	SlabSweeper,
	PathBuf,
	wendelon::storage::RecordLocation,
	Vec<u8>,
) {
	let l1_dir = fresh_dir("l1");
	let l2_dir = fresh_dir("l2");
	let io = spawn_io_thread(64).unwrap();
	let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&l2_dir).unwrap());
	let manager = Arc::new(
		SlabManager::open(SlabManagerConfig {
			dir: l1_dir.clone(),
			max_slab_bytes: 1,
			io,
			l2: Some(l2.clone()),
			rehydration,
			group_commit: wendelon::storage::GroupCommitConfig::default(),
			crypto: test_crypto(),
		})
		.await
		.unwrap(),
	);

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

	let location = manager.lookup(tenant(), id(1)).expect("key present");
	let original = manager.read(location).await.expect("pre-tier read ok");

	let observer: Arc<dyn TierObserver> = Arc::new(ManagerTierObserver::new(manager.clone()));
	let committer: Arc<dyn TierCommitter> = Arc::new(DirectApplyCommitter { observer });
	let sweeper = SlabSweeper::new(
		manager.clone(),
		l2.clone(),
		committer,
		WendelonStateMachine::new(),
		SweeperConfig {
			scan_interval: Duration::from_millis(10),
			min_slab_bytes: 0,
			max_upload_bytes_per_sec: None,
			key_prefix: String::new(),
			max_plaintext_rewrites_per_pass: 1,
		},
	);
	let report = sweeper.sweep_once().await.unwrap();
	assert!(
		report.tiered > 0,
		"precondition: expected at least one tiered slab"
	);
	assert!(manager.is_tiered(location.slab_id).await);

	(manager, l2, sweeper, l1_dir, location, original)
}

#[tokio::test]
async fn multiple_l2_reads_trigger_promotion_to_l1() {
	let policy = RehydrationPolicy {
		promote_after_l2_reads: 2,
		min_local_residency: Duration::from_secs(60),
	};
	let (manager, _l2, _sweeper, l1_dir, location, original) = build_tiered_manager(policy).await;

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
	};
	let (manager, l2, _sweeper, _l1_dir, location, original) = build_tiered_manager(policy).await;

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
	};
	let (manager, _l2, sweeper, l1_dir, location, original) = build_tiered_manager(policy).await;

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
