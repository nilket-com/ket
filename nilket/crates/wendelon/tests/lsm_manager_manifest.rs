use async_trait::async_trait;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use wendelon::crypto::{CryptoManager, test_support};
use wendelon::hash::state_digest::StateDigest;
use wendelon::schema::{
	CompactionJobId, CompactionLevelIndex, HybridTimestamp, Identity, SlabId, TenantId,
};
use wendelon::storage::core::StorageEngine;
use wendelon::storage::io_uring::spawn_io_thread;
use wendelon::storage::manager::{RehydrationPolicy, SlabManager, SlabManagerConfig};
use wendelon::storage::{
	CompactionCommit, CompactionDigestSummary, CompactionPlan, DominantReason, GroupCommitConfig,
	IoUringFileOps, LocalSlabStore, LsmCompactionWorker, LsmConfig, LsmGcWorker, LsmRunSummary,
	SidecarSwap, SlabState, TierCommitter, VersionLifecycle,
};

fn tenant() -> TenantId {
	TenantId::ZERO
}

fn id(locality: u64) -> Identity {
	let mut bytes = [0u8; 32];
	bytes[..8].copy_from_slice(&locality.to_le_bytes());
	Identity::new(bytes)
}

fn hlc(ms: u64) -> HybridTimestamp {
	HybridTimestamp {
		physical_ms: ms,
		logical: 0,
		node_id: 1,
	}
}

fn test_crypto() -> Arc<CryptoManager> {
	Arc::new(test_support::seeded_dev_crypto_manager(tenant()).unwrap())
}

fn fresh_dir(tag: &str) -> std::path::PathBuf {
	let dir = std::env::temp_dir().join(format!(
		"wendelon-lsm-manager-{tag}-{}-{}",
		std::process::id(),
		uuid::Uuid::new_v4()
	));
	std::fs::create_dir_all(&dir).unwrap();
	dir
}

fn digest_summary() -> CompactionDigestSummary {
	CompactionDigestSummary {
		visible_pre: StateDigest::ZERO,
		visible_post: StateDigest::ZERO,
		version_entry_delta: StateDigest::ZERO,
	}
}

struct ManifestApplyingCommitter {
	manager: Arc<SlabManager>,
	applied: AtomicUsize,
	last_commit: Mutex<Option<CompactionCommit>>,
}

#[async_trait]
impl TierCommitter for ManifestApplyingCommitter {
	async fn commit_tier_slab(
		&self,
		_tier: wendelon::consensus::TieredSlab,
	) -> std::io::Result<()> {
		Err(std::io::Error::new(
			std::io::ErrorKind::Unsupported,
			"tier commits are not used by this test committer",
		))
	}

	async fn retire_tiered_slab(&self, _slab_id: SlabId) -> std::io::Result<()> {
		Err(std::io::Error::new(
			std::io::ErrorKind::Unsupported,
			"tier retirements are not used by this test committer",
		))
	}

	async fn commit_lsm_compaction(&self, commit: CompactionCommit) -> std::io::Result<()> {
		*self.last_commit.lock().unwrap() = Some(commit.clone());
		self.manager
			.apply_lsm_manifest_compaction_commit(&commit)
			.map(|_| {
				self.applied.fetch_add(1, Ordering::SeqCst);
			})
			.map_err(std::io::Error::other)
	}
}

#[tokio::test]
async fn sealed_slabs_enter_compaction_manifest_as_l0_live() {
	let manager = SlabManager::open(SlabManagerConfig {
		dir: fresh_dir("l1"),
		max_slab_bytes: 1,
		io: spawn_io_thread(16).unwrap(),
		l2: None,
		rehydration: RehydrationPolicy::default(),
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
		.append(tenant(), id(1), hlc(1), b"first-record")
		.await
		.unwrap();
	manager
		.append(tenant(), id(2), hlc(2), b"second-record")
		.await
		.unwrap();

	let snapshot = manager.compaction_manifest_snapshot();
	let l0 = snapshot.slabs_at_compaction_level(CompactionLevelIndex::L0);

	assert_eq!(l0.len(), 1);
	assert_eq!(l0[0].slab_id, SlabId::ZERO);
	assert!(l0[0].state.is_live());
	assert_eq!(l0[0].min_row_hlc, hlc(1));
	assert_eq!(l0[0].max_row_hlc, hlc(1));
	assert_eq!(l0[0].row_count.get(), 1);
	assert!(l0[0].size_bytes.get() > 0);
}

#[tokio::test]
async fn open_removes_stale_lsm_compaction_temp_files() {
	let dir = fresh_dir("tmp-cleanup");
	let temp = dir.join("compaction-temp-77-output-0001.wnd");
	let unrelated = dir.join("not-compaction-temp-77-output-0001.wnd");
	std::fs::write(&temp, b"orphan").unwrap();
	std::fs::write(&unrelated, b"keep").unwrap();

	let _manager = SlabManager::open(SlabManagerConfig {
		dir: dir.clone(),
		max_slab_bytes: 1,
		io: spawn_io_thread(16).unwrap(),
		l2: None,
		rehydration: RehydrationPolicy::default(),
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

	assert!(!temp.exists());
	assert!(unrelated.exists());
}

#[tokio::test]
async fn lsm_output_slab_id_reservation_advances_manager_sequence() {
	let manager = SlabManager::open(SlabManagerConfig {
		dir: fresh_dir("reserve-outputs"),
		max_slab_bytes: 1,
		io: spawn_io_thread(16).unwrap(),
		l2: None,
		rehydration: RehydrationPolicy::default(),
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

	let reserved = manager.reserve_lsm_output_slab_ids(3).await.unwrap();

	assert_eq!(
		reserved,
		vec![SlabId::new(1), SlabId::new(2), SlabId::new(3)]
	);
	manager
		.append(tenant(), id(8), hlc(8), b"first-record")
		.await
		.unwrap();
	manager
		.append(tenant(), id(9), hlc(9), b"second-record")
		.await
		.unwrap();
	assert_eq!(manager.active_slab_id().await, SlabId::new(4));
}

#[tokio::test]
async fn lsm_version_entries_for_plan_come_from_manager_key_index() {
	let manager = SlabManager::open(SlabManagerConfig {
		dir: fresh_dir("extract-entries"),
		max_slab_bytes: 1,
		io: spawn_io_thread(16).unwrap(),
		l2: None,
		rehydration: RehydrationPolicy::default(),
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

	let first = manager
		.append(tenant(), id(41), hlc(41), b"first-record")
		.await
		.unwrap();
	manager
		.append(tenant(), id(42), hlc(42), b"second-record")
		.await
		.unwrap();
	let snapshot = manager.compaction_manifest_snapshot();
	let input = snapshot.slabs_at_compaction_level(CompactionLevelIndex::L0)[0].clone();
	let plan = CompactionPlan {
		job_id: CompactionJobId::new(4100),
		source_level: CompactionLevelIndex::L0,
		target_level: CompactionLevelIndex::L1,
		manifest_epoch_pre: snapshot.manifest_epoch,
		input_slabs: vec![input.slab_id],
		overlapping_slabs: Vec::new(),
		retention_floor_snapshot: hlc(100),
		compaction_start_hlc: hlc(101),
		dominant_reason: DominantReason::LevelSize,
	};

	let entries = manager.lsm_version_entries_for_plan(&plan);

	assert_eq!(entries.len(), 1);
	assert_eq!(entries[0].tenant_id, tenant());
	assert_eq!(entries[0].row_key_id, id(41));
	assert_eq!(entries[0].row_hlc, hlc(41));
	assert_eq!(entries[0].location, first);
	assert_eq!(entries[0].lifecycle, VersionLifecycle::AmbiguousTable);
}

#[tokio::test]
async fn manager_prepares_lsm_outputs_from_extracted_entries_and_reserved_ids() {
	let manager = SlabManager::open(SlabManagerConfig {
		dir: fresh_dir("prepare-outputs"),
		max_slab_bytes: 1,
		io: spawn_io_thread(16).unwrap(),
		l2: None,
		rehydration: RehydrationPolicy::default(),
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
		.append(tenant(), id(51), hlc(51), b"first-record")
		.await
		.unwrap();
	manager
		.append(tenant(), id(52), hlc(52), b"second-record")
		.await
		.unwrap();
	let snapshot = manager.compaction_manifest_snapshot();
	let input = snapshot.slabs_at_compaction_level(CompactionLevelIndex::L0)[0].clone();
	let plan = CompactionPlan {
		job_id: CompactionJobId::new(5100),
		source_level: CompactionLevelIndex::L0,
		target_level: CompactionLevelIndex::L1,
		manifest_epoch_pre: snapshot.manifest_epoch,
		input_slabs: vec![input.slab_id],
		overlapping_slabs: Vec::new(),
		retention_floor_snapshot: hlc(100),
		compaction_start_hlc: hlc(101),
		dominant_reason: DominantReason::LevelSize,
	};
	let worker = LsmCompactionWorker::new(LsmConfig::default(), manager.slab_pin_table()).unwrap();

	let prepared = manager
		.prepare_lsm_compaction_outputs(&worker, &plan)
		.await
		.unwrap();

	assert_eq!(prepared.decisions.len(), 1);
	assert_eq!(prepared.output_runs.len(), 1);
	assert_eq!(prepared.output_runs[0].slab_id, SlabId::new(2));
	assert_eq!(prepared.output_runs[0].rows[0].row_key_id, id(51));
	assert_eq!(manager.active_slab_id().await, SlabId::new(1));
	let reserved = manager.reserve_lsm_output_slab_ids(1).await.unwrap();
	assert_eq!(reserved, vec![SlabId::new(3)]);
}

#[tokio::test]
async fn manager_reads_lsm_single_row_source_frames_for_plan() {
	let manager = SlabManager::open(SlabManagerConfig {
		dir: fresh_dir("source-frames"),
		max_slab_bytes: 1,
		io: spawn_io_thread(16).unwrap(),
		l2: None,
		rehydration: RehydrationPolicy::default(),
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

	let first = manager
		.append(tenant(), id(61), hlc(61), b"first-record")
		.await
		.unwrap();
	manager
		.append(tenant(), id(62), hlc(62), b"second-record")
		.await
		.unwrap();
	let snapshot = manager.compaction_manifest_snapshot();
	let input = snapshot.slabs_at_compaction_level(CompactionLevelIndex::L0)[0].clone();
	let plan = CompactionPlan {
		job_id: CompactionJobId::new(6100),
		source_level: CompactionLevelIndex::L0,
		target_level: CompactionLevelIndex::L1,
		manifest_epoch_pre: snapshot.manifest_epoch,
		input_slabs: vec![input.slab_id],
		overlapping_slabs: Vec::new(),
		retention_floor_snapshot: hlc(100),
		compaction_start_hlc: hlc(101),
		dominant_reason: DominantReason::LevelSize,
	};

	let frames = manager
		.read_lsm_single_row_source_frames(&plan)
		.await
		.unwrap();

	assert_eq!(frames.len(), 1);
	assert_eq!(frames[0].entry.location, first);
	assert_eq!(frames[0].entry.row_key_id, id(61));
	assert_eq!(frames[0].frame.as_slice(), b"first-record");
}

#[tokio::test]
async fn manager_writes_lsm_temp_output_slabs_without_installing_them() {
	let dir = fresh_dir("temp-outputs");
	let manager = SlabManager::open(SlabManagerConfig {
		dir: dir.clone(),
		max_slab_bytes: 1,
		io: spawn_io_thread(16).unwrap(),
		l2: None,
		rehydration: RehydrationPolicy::default(),
		l2_column_fetch: Default::default(),
		row_group_writer: Default::default(),
		l2_row_group_fetch: Default::default(),
		encoding_writer: Default::default(),
		l2_chunk_layout: Default::default(),
		l2_encoding_fetch: Default::default(),
		encoding_compute: Default::default(),
		group_commit: GroupCommitConfig::default(),
		crypto: test_crypto(),
	})
	.await
	.unwrap();

	let first = manager
		.append(tenant(), id(71), hlc(71), b"first-record")
		.await
		.unwrap();
	manager
		.append(tenant(), id(72), hlc(72), b"second-record")
		.await
		.unwrap();
	let snapshot = manager.compaction_manifest_snapshot();
	let input = snapshot.slabs_at_compaction_level(CompactionLevelIndex::L0)[0].clone();
	let plan = CompactionPlan {
		job_id: CompactionJobId::new(7100),
		source_level: CompactionLevelIndex::L0,
		target_level: CompactionLevelIndex::L1,
		manifest_epoch_pre: snapshot.manifest_epoch,
		input_slabs: vec![input.slab_id],
		overlapping_slabs: Vec::new(),
		retention_floor_snapshot: hlc(100),
		compaction_start_hlc: hlc(101),
		dominant_reason: DominantReason::LevelSize,
	};
	let worker = LsmCompactionWorker::new(LsmConfig::default(), manager.slab_pin_table()).unwrap();
	let prepared = manager
		.prepare_lsm_compaction_outputs(&worker, &plan)
		.await
		.unwrap();
	let frames = manager
		.read_lsm_single_row_source_frames(&plan)
		.await
		.unwrap();

	let written = manager
		.write_lsm_compaction_temp_outputs(&plan, &prepared, &frames)
		.await
		.unwrap();

	assert_eq!(written.len(), 1);
	assert_eq!(written[0].slab_id, prepared.output_runs[0].slab_id);
	assert_eq!(written[0].compaction_level, CompactionLevelIndex::L1);
	assert_eq!(written[0].summary.row_count.get(), 1);
	assert!(written[0].summary.size_bytes.get() >= u64::from(first.length));
	assert!(written[0].temp_path.exists());
	assert!(
		written[0]
			.temp_path
			.file_name()
			.unwrap()
			.to_str()
			.unwrap()
			.starts_with("compaction-temp-")
	);
	assert!(
		!dir.join(format!("slab_{:016x}.wnd", written[0].slab_id))
			.exists()
	);
	assert_eq!(manager.lookup(tenant(), id(71)).unwrap(), first);

	let temp_store = LocalSlabStore::open(
		&written[0].temp_path,
		written[0].slab_id,
		Arc::new(IoUringFileOps::new(spawn_io_thread(16).unwrap())),
		GroupCommitConfig::default(),
	)
	.await
	.unwrap();
	assert_eq!(
		temp_store.read(written[0].locations[0]).await.unwrap(),
		b"first-record"
	);
	drop(temp_store);

	let temp_path = written[0].temp_path.clone();
	drop(manager);
	let _reopened = SlabManager::open(SlabManagerConfig {
		dir: dir.clone(),
		max_slab_bytes: 1,
		io: spawn_io_thread(16).unwrap(),
		l2: None,
		rehydration: RehydrationPolicy::default(),
		l2_column_fetch: Default::default(),
		row_group_writer: Default::default(),
		l2_row_group_fetch: Default::default(),
		encoding_writer: Default::default(),
		l2_chunk_layout: Default::default(),
		l2_encoding_fetch: Default::default(),
		encoding_compute: Default::default(),
		group_commit: GroupCommitConfig::default(),
		crypto: test_crypto(),
	})
	.await
	.unwrap();
	assert!(!temp_path.exists());
}

#[tokio::test]
async fn manager_installs_lsm_temp_outputs_into_manifest_and_lookup_index() {
	let dir = fresh_dir("install-outputs");
	let manager = SlabManager::open(SlabManagerConfig {
		dir: dir.clone(),
		max_slab_bytes: 1,
		io: spawn_io_thread(16).unwrap(),
		l2: None,
		rehydration: RehydrationPolicy::default(),
		l2_column_fetch: Default::default(),
		row_group_writer: Default::default(),
		l2_row_group_fetch: Default::default(),
		encoding_writer: Default::default(),
		l2_chunk_layout: Default::default(),
		l2_encoding_fetch: Default::default(),
		encoding_compute: Default::default(),
		group_commit: GroupCommitConfig::default(),
		crypto: test_crypto(),
	})
	.await
	.unwrap();

	let first = manager
		.append(tenant(), id(91), hlc(91), b"first-record")
		.await
		.unwrap();
	manager
		.append(tenant(), id(92), hlc(92), b"second-record")
		.await
		.unwrap();
	let snapshot = manager.compaction_manifest_snapshot();
	let input = snapshot.slabs_at_compaction_level(CompactionLevelIndex::L0)[0].clone();
	let plan = CompactionPlan {
		job_id: CompactionJobId::new(9100),
		source_level: CompactionLevelIndex::L0,
		target_level: CompactionLevelIndex::L1,
		manifest_epoch_pre: snapshot.manifest_epoch,
		input_slabs: vec![input.slab_id],
		overlapping_slabs: Vec::new(),
		retention_floor_snapshot: hlc(100),
		compaction_start_hlc: hlc(101),
		dominant_reason: DominantReason::LevelSize,
	};
	let worker = LsmCompactionWorker::new(LsmConfig::default(), manager.slab_pin_table()).unwrap();
	let prepared = manager
		.prepare_lsm_compaction_outputs(&worker, &plan)
		.await
		.unwrap();
	let frames = manager
		.read_lsm_single_row_source_frames(&plan)
		.await
		.unwrap();
	let written = manager
		.write_lsm_compaction_temp_outputs(&plan, &prepared, &frames)
		.await
		.unwrap();
	let temp_path = written[0].temp_path.clone();
	let final_path = dir.join(format!("slab_{:016x}.wnd", written[0].slab_id));
	let commit = CompactionCommit {
		job_id: plan.job_id,
		manifest_epoch_pre: plan.manifest_epoch_pre,
		slabs_to_retire: plan.touched_slabs(),
		slabs_to_add: written.iter().map(|run| run.summary.clone()).collect(),
		slabs_to_mark_delete_pending: Vec::new(),
		slabs_to_remove: Vec::new(),
		sidecar_swaps: Vec::new(),
		digest_summary: digest_summary(),
	};

	let post_epoch = manager
		.install_lsm_compaction_temp_outputs(&plan, &prepared, &written, &commit)
		.await
		.unwrap();
	let after = manager.compaction_manifest_snapshot();
	let output_location = written[0].locations[0];

	assert_eq!(post_epoch, snapshot.manifest_epoch.saturating_add(1));
	assert!(!temp_path.exists());
	assert!(final_path.exists());
	assert_eq!(manager.lookup(tenant(), id(91)).unwrap(), output_location);
	assert_eq!(
		manager.read(output_location).await.unwrap(),
		b"first-record"
	);
	assert_eq!(manager.read(first).await.unwrap(), b"first-record");
	assert!(matches!(
		after.run(first.slab_id).map(|run| &run.state),
		Some(SlabState::Retired { successors }) if successors == &vec![written[0].slab_id]
	));
	assert_eq!(
		after
			.slabs_at_compaction_level(CompactionLevelIndex::L1)
			.iter()
			.map(|run| run.slab_id)
			.collect::<Vec<_>>(),
		vec![written[0].slab_id]
	);
	assert!(manager.active_slab_id().await > written[0].slab_id);
}

#[tokio::test]
async fn manager_runs_single_lsm_compaction_job_end_to_end() {
	let manager = SlabManager::open(SlabManagerConfig {
		dir: fresh_dir("run-once"),
		max_slab_bytes: 1,
		io: spawn_io_thread(16).unwrap(),
		l2: None,
		rehydration: RehydrationPolicy::default(),
		l2_column_fetch: Default::default(),
		row_group_writer: Default::default(),
		l2_row_group_fetch: Default::default(),
		encoding_writer: Default::default(),
		l2_chunk_layout: Default::default(),
		l2_encoding_fetch: Default::default(),
		encoding_compute: Default::default(),
		group_commit: GroupCommitConfig::default(),
		crypto: test_crypto(),
	})
	.await
	.unwrap();

	let first = manager
		.append(tenant(), id(101), hlc(101), b"first-record")
		.await
		.unwrap();
	manager
		.append(tenant(), id(102), hlc(102), b"second-record")
		.await
		.unwrap();
	let snapshot = manager.compaction_manifest_snapshot();
	let input = snapshot.slabs_at_compaction_level(CompactionLevelIndex::L0)[0].clone();
	let plan = CompactionPlan {
		job_id: CompactionJobId::new(10100),
		source_level: CompactionLevelIndex::L0,
		target_level: CompactionLevelIndex::L1,
		manifest_epoch_pre: snapshot.manifest_epoch,
		input_slabs: vec![input.slab_id],
		overlapping_slabs: Vec::new(),
		retention_floor_snapshot: hlc(200),
		compaction_start_hlc: hlc(201),
		dominant_reason: DominantReason::LevelSize,
	};
	let worker = LsmCompactionWorker::new(LsmConfig::default(), manager.slab_pin_table()).unwrap();

	let outcome = worker.run_one(&manager, plan).await.unwrap();
	let output_slab = outcome.output_slabs[0].slab_id;
	let output_location = manager.lookup(tenant(), id(101)).unwrap();

	assert_eq!(outcome.manifest_epoch_pre, snapshot.manifest_epoch);
	assert_eq!(
		outcome.manifest_epoch_post,
		snapshot.manifest_epoch.saturating_add(1)
	);
	assert_eq!(outcome.rows_in, 1);
	assert_eq!(outcome.rows_out, 1);
	assert_eq!(outcome.elided_versions, 0);
	assert_eq!(output_location.slab_id, output_slab);
	assert_eq!(
		manager.read(output_location).await.unwrap(),
		b"first-record"
	);
	assert_eq!(manager.read(first).await.unwrap(), b"first-record");
	assert!(matches!(
		manager
			.compaction_manifest_snapshot()
			.run(first.slab_id)
			.map(|run| &run.state),
		Some(SlabState::Retired { successors }) if successors == &vec![output_slab]
	));
}

#[tokio::test]
async fn manager_runs_lsm_compaction_job_through_control_ring_committer() {
	let manager = Arc::new(
		SlabManager::open(SlabManagerConfig {
			dir: fresh_dir("run-once-control-ring"),
			max_slab_bytes: 1,
			io: spawn_io_thread(16).unwrap(),
			l2: None,
			rehydration: RehydrationPolicy::default(),
			l2_column_fetch: Default::default(),
			row_group_writer: Default::default(),
			l2_row_group_fetch: Default::default(),
			encoding_writer: Default::default(),
			l2_chunk_layout: Default::default(),
			l2_encoding_fetch: Default::default(),
			encoding_compute: Default::default(),
			group_commit: GroupCommitConfig::default(),
			crypto: test_crypto(),
		})
		.await
		.unwrap(),
	);

	let first = manager
		.append(tenant(), id(111), hlc(111), b"first-record")
		.await
		.unwrap();
	manager
		.append(tenant(), id(112), hlc(112), b"second-record")
		.await
		.unwrap();
	let snapshot = manager.compaction_manifest_snapshot();
	let input = snapshot.slabs_at_compaction_level(CompactionLevelIndex::L0)[0].clone();
	let plan = CompactionPlan {
		job_id: CompactionJobId::new(11100),
		source_level: CompactionLevelIndex::L0,
		target_level: CompactionLevelIndex::L1,
		manifest_epoch_pre: snapshot.manifest_epoch,
		input_slabs: vec![input.slab_id],
		overlapping_slabs: Vec::new(),
		retention_floor_snapshot: hlc(200),
		compaction_start_hlc: hlc(201),
		dominant_reason: DominantReason::LevelSize,
	};
	let worker = LsmCompactionWorker::new(LsmConfig::default(), manager.slab_pin_table()).unwrap();
	let committer = ManifestApplyingCommitter {
		manager: manager.clone(),
		applied: AtomicUsize::new(0),
		last_commit: Mutex::new(None),
	};

	let outcome = manager
		.run_lsm_compaction_once_via_control_ring(&worker, plan, &committer)
		.await
		.unwrap();
	let output_slab = outcome.output_slabs[0].slab_id;
	let output_location = manager.lookup(tenant(), id(111)).unwrap();

	assert_eq!(committer.applied.load(Ordering::SeqCst), 1);
	let committed = committer.last_commit.lock().unwrap().clone().unwrap();
	assert_eq!(
		committed.digest_summary.visible_pre,
		committed.digest_summary.visible_post
	);
	assert_ne!(committed.digest_summary.visible_pre, StateDigest::ZERO);
	assert_eq!(outcome.manifest_epoch_pre, snapshot.manifest_epoch);
	assert_eq!(
		outcome.manifest_epoch_post,
		snapshot.manifest_epoch.saturating_add(1)
	);
	assert_eq!(output_location.slab_id, output_slab);
	assert_eq!(
		manager.read(output_location).await.unwrap(),
		b"first-record"
	);
	assert_eq!(manager.read(first).await.unwrap(), b"first-record");
	assert!(matches!(
		manager
			.compaction_manifest_snapshot()
			.run(first.slab_id)
			.map(|run| &run.state),
		Some(SlabState::Retired { successors }) if successors == &vec![output_slab]
	));
}

#[tokio::test]
async fn manager_picker_tick_runs_l0_compaction_when_threshold_met() {
	let manager = SlabManager::open(SlabManagerConfig {
		dir: fresh_dir("picker-tick"),
		max_slab_bytes: 1,
		io: spawn_io_thread(16).unwrap(),
		l2: None,
		rehydration: RehydrationPolicy::default(),
		l2_column_fetch: Default::default(),
		row_group_writer: Default::default(),
		l2_row_group_fetch: Default::default(),
		encoding_writer: Default::default(),
		l2_chunk_layout: Default::default(),
		l2_encoding_fetch: Default::default(),
		encoding_compute: Default::default(),
		group_commit: GroupCommitConfig::default(),
		crypto: test_crypto(),
	})
	.await
	.unwrap();

	let first = manager
		.append(tenant(), id(121), hlc(1), b"first-record")
		.await
		.unwrap();
	manager
		.append(tenant(), id(122), hlc(2), b"second-record")
		.await
		.unwrap();
	manager
		.append(tenant(), id(123), hlc(3), b"third-record")
		.await
		.unwrap();
	manager
		.append(tenant(), id(124), hlc(4), b"fourth-record")
		.await
		.unwrap();
	let active = manager
		.append(tenant(), id(125), hlc(5), b"fifth-record")
		.await
		.unwrap();
	let worker = LsmCompactionWorker::new(LsmConfig::default(), manager.slab_pin_table()).unwrap();

	let plan = manager
		.pick_lsm_compaction_plan(&worker, hlc(20_000))
		.expect("aged four-slab L0 set should be picked");
	assert_eq!(plan.source_level, CompactionLevelIndex::L0);
	assert_eq!(plan.input_slabs.len(), 4);

	let outcome = manager
		.run_lsm_compaction_tick(&worker, hlc(20_000))
		.await
		.unwrap()
		.expect("picker tick should run a job");
	let after = manager.compaction_manifest_snapshot();
	let output_location = manager.lookup(tenant(), id(121)).unwrap();

	assert_eq!(outcome.rows_in, 4);
	assert_eq!(outcome.rows_out, 4);
	assert_eq!(output_location.slab_id, outcome.output_slabs[0].slab_id);
	assert_eq!(
		manager.read(output_location).await.unwrap(),
		b"first-record"
	);
	assert_eq!(manager.read(first).await.unwrap(), b"first-record");
	assert_eq!(manager.read(active).await.unwrap(), b"fifth-record");
	assert!(matches!(
		after.run(first.slab_id).map(|run| &run.state),
		Some(SlabState::Retired { successors }) if successors == &vec![outcome.output_slabs[0].slab_id]
	));
	assert!(
		after
			.slabs_at_compaction_level(CompactionLevelIndex::L1)
			.iter()
			.any(|run| run.slab_id == outcome.output_slabs[0].slab_id)
	);
}

#[tokio::test]
async fn manager_reopens_with_persisted_lsm_manifest_filtering_retired_inputs() {
	let dir = fresh_dir("reopen-lsm-manifest");
	let manager = SlabManager::open(SlabManagerConfig {
		dir: dir.clone(),
		max_slab_bytes: 1,
		io: spawn_io_thread(16).unwrap(),
		l2: None,
		rehydration: RehydrationPolicy::default(),
		l2_column_fetch: Default::default(),
		row_group_writer: Default::default(),
		l2_row_group_fetch: Default::default(),
		encoding_writer: Default::default(),
		l2_chunk_layout: Default::default(),
		l2_encoding_fetch: Default::default(),
		encoding_compute: Default::default(),
		group_commit: GroupCommitConfig::default(),
		crypto: test_crypto(),
	})
	.await
	.unwrap();

	let first = manager
		.append(tenant(), id(131), hlc(131), b"first-record")
		.await
		.unwrap();
	manager
		.append(tenant(), id(132), hlc(132), b"second-record")
		.await
		.unwrap();
	let snapshot = manager.compaction_manifest_snapshot();
	let input = snapshot.slabs_at_compaction_level(CompactionLevelIndex::L0)[0].clone();
	let plan = CompactionPlan {
		job_id: CompactionJobId::new(13100),
		source_level: CompactionLevelIndex::L0,
		target_level: CompactionLevelIndex::L1,
		manifest_epoch_pre: snapshot.manifest_epoch,
		input_slabs: vec![input.slab_id],
		overlapping_slabs: Vec::new(),
		retention_floor_snapshot: hlc(200),
		compaction_start_hlc: hlc(201),
		dominant_reason: DominantReason::LevelSize,
	};
	let worker = LsmCompactionWorker::new(LsmConfig::default(), manager.slab_pin_table()).unwrap();
	let outcome = manager
		.run_lsm_compaction_once(&worker, plan)
		.await
		.unwrap();
	let output_slab = outcome.output_slabs[0].slab_id;
	drop(manager);

	let reopened = SlabManager::open(SlabManagerConfig {
		dir: dir.clone(),
		max_slab_bytes: 1,
		io: spawn_io_thread(16).unwrap(),
		l2: None,
		rehydration: RehydrationPolicy::default(),
		l2_column_fetch: Default::default(),
		row_group_writer: Default::default(),
		l2_row_group_fetch: Default::default(),
		encoding_writer: Default::default(),
		l2_chunk_layout: Default::default(),
		l2_encoding_fetch: Default::default(),
		encoding_compute: Default::default(),
		group_commit: GroupCommitConfig::default(),
		crypto: test_crypto(),
	})
	.await
	.unwrap();
	let reopened_location = reopened.lookup(tenant(), id(131)).unwrap();
	let reopened_manifest = reopened.compaction_manifest_snapshot();

	assert_eq!(reopened_location.slab_id, output_slab);
	assert_eq!(
		reopened.read(reopened_location).await.unwrap(),
		b"first-record"
	);
	assert_eq!(reopened.read(first).await.unwrap(), b"first-record");
	assert!(matches!(
		reopened_manifest.run(first.slab_id).map(|run| &run.state),
		Some(SlabState::Retired { successors }) if successors == &vec![output_slab]
	));
	assert!(
		reopened_manifest
			.slabs_at_compaction_level(CompactionLevelIndex::L1)
			.iter()
			.any(|run| run.slab_id == output_slab)
	);
}

#[tokio::test]
async fn manager_install_rejects_bad_digest_before_manifest_apply() {
	let dir = fresh_dir("run-once-digest-reject");
	let manager = SlabManager::open(SlabManagerConfig {
		dir: dir.clone(),
		max_slab_bytes: 1,
		io: spawn_io_thread(16).unwrap(),
		l2: None,
		rehydration: RehydrationPolicy::default(),
		l2_column_fetch: Default::default(),
		row_group_writer: Default::default(),
		l2_row_group_fetch: Default::default(),
		encoding_writer: Default::default(),
		l2_chunk_layout: Default::default(),
		l2_encoding_fetch: Default::default(),
		encoding_compute: Default::default(),
		group_commit: GroupCommitConfig::default(),
		crypto: test_crypto(),
	})
	.await
	.unwrap();

	let first = manager
		.append(tenant(), id(111), hlc(111), b"first-record")
		.await
		.unwrap();
	manager
		.append(tenant(), id(112), hlc(112), b"second-record")
		.await
		.unwrap();
	let snapshot = manager.compaction_manifest_snapshot();
	let input = snapshot.slabs_at_compaction_level(CompactionLevelIndex::L0)[0].clone();
	let plan = CompactionPlan {
		job_id: CompactionJobId::new(11100),
		source_level: CompactionLevelIndex::L0,
		target_level: CompactionLevelIndex::L1,
		manifest_epoch_pre: snapshot.manifest_epoch,
		input_slabs: vec![input.slab_id],
		overlapping_slabs: Vec::new(),
		retention_floor_snapshot: hlc(200),
		compaction_start_hlc: hlc(201),
		dominant_reason: DominantReason::LevelSize,
	};
	let worker = LsmCompactionWorker::new(LsmConfig::default(), manager.slab_pin_table()).unwrap();
	let prepared = manager
		.prepare_lsm_compaction_outputs(&worker, &plan)
		.await
		.unwrap();
	let frames = manager
		.read_lsm_single_row_source_frames(&plan)
		.await
		.unwrap();
	let written = manager
		.write_lsm_compaction_temp_outputs(&plan, &prepared, &frames)
		.await
		.unwrap();
	let mut bad_digest = digest_summary();
	bad_digest.visible_post = StateDigest::new([7; 32]);
	let commit = worker.build_written_compaction_commit(&plan, &written, bad_digest);

	let err = manager
		.install_lsm_compaction_temp_outputs(&plan, &prepared, &written, &commit)
		.await
		.unwrap_err();

	assert!(matches!(
		err,
		wendelon::storage::CompactionFailure::VisibleDigestInvariance
	));
	assert_eq!(manager.lookup(tenant(), id(111)).unwrap(), first);
	assert!(
		std::fs::read_dir(&dir)
			.unwrap()
			.flatten()
			.filter_map(|entry| entry.file_name().into_string().ok())
			.any(|name| name.starts_with("compaction-temp-"))
	);
	assert!(
		manager
			.compaction_manifest_snapshot()
			.run(first.slab_id)
			.unwrap()
			.state
			.is_live()
	);
}

#[tokio::test]
async fn manager_install_rejects_explicit_lsm_sidecar_swaps() {
	let dir = fresh_dir("sidecar-swap-reject");
	let manager = SlabManager::open(SlabManagerConfig {
		dir: dir.clone(),
		max_slab_bytes: 1,
		io: spawn_io_thread(16).unwrap(),
		l2: None,
		rehydration: RehydrationPolicy::default(),
		l2_column_fetch: Default::default(),
		row_group_writer: Default::default(),
		l2_row_group_fetch: Default::default(),
		encoding_writer: Default::default(),
		l2_chunk_layout: Default::default(),
		l2_encoding_fetch: Default::default(),
		encoding_compute: Default::default(),
		group_commit: GroupCommitConfig::default(),
		crypto: test_crypto(),
	})
	.await
	.unwrap();

	let first = manager
		.append(tenant(), id(141), hlc(141), b"first-record")
		.await
		.unwrap();
	manager
		.append(tenant(), id(142), hlc(142), b"second-record")
		.await
		.unwrap();
	let snapshot = manager.compaction_manifest_snapshot();
	let input = snapshot.slabs_at_compaction_level(CompactionLevelIndex::L0)[0].clone();
	let plan = CompactionPlan {
		job_id: CompactionJobId::new(14100),
		source_level: CompactionLevelIndex::L0,
		target_level: CompactionLevelIndex::L1,
		manifest_epoch_pre: snapshot.manifest_epoch,
		input_slabs: vec![input.slab_id],
		overlapping_slabs: Vec::new(),
		retention_floor_snapshot: hlc(200),
		compaction_start_hlc: hlc(201),
		dominant_reason: DominantReason::LevelSize,
	};
	let worker = LsmCompactionWorker::new(LsmConfig::default(), manager.slab_pin_table()).unwrap();
	let prepared = manager
		.prepare_lsm_compaction_outputs(&worker, &plan)
		.await
		.unwrap();
	let frames = manager
		.read_lsm_single_row_source_frames(&plan)
		.await
		.unwrap();
	let written = manager
		.write_lsm_compaction_temp_outputs(&plan, &prepared, &frames)
		.await
		.unwrap();
	let mut commit = worker.build_written_compaction_commit(&plan, &written, digest_summary());
	commit.sidecar_swaps.push(SidecarSwap {
		source_slab_id: first.slab_id,
		output_slab_id: written[0].slab_id,
		old_sidecar_digest: [1; 32],
		new_sidecar_digest: [2; 32],
	});

	let err = manager
		.install_lsm_compaction_temp_outputs(&plan, &prepared, &written, &commit)
		.await
		.unwrap_err();

	assert!(matches!(
		err,
		wendelon::storage::CompactionFailure::SidecarDigestMismatch
	));
	assert_eq!(manager.lookup(tenant(), id(141)).unwrap(), first);
	assert!(
		!dir.join(format!("slab_{:016x}.wnd", written[0].slab_id))
			.exists()
	);
}

#[tokio::test]
async fn manager_removes_lsm_temp_output_on_missing_source_frame() {
	let dir = fresh_dir("temp-output-missing-frame");
	let manager = SlabManager::open(SlabManagerConfig {
		dir: dir.clone(),
		max_slab_bytes: 1,
		io: spawn_io_thread(16).unwrap(),
		l2: None,
		rehydration: RehydrationPolicy::default(),
		l2_column_fetch: Default::default(),
		row_group_writer: Default::default(),
		l2_row_group_fetch: Default::default(),
		encoding_writer: Default::default(),
		l2_chunk_layout: Default::default(),
		l2_encoding_fetch: Default::default(),
		encoding_compute: Default::default(),
		group_commit: GroupCommitConfig::default(),
		crypto: test_crypto(),
	})
	.await
	.unwrap();

	manager
		.append(tenant(), id(81), hlc(81), b"first-record")
		.await
		.unwrap();
	manager
		.append(tenant(), id(82), hlc(82), b"second-record")
		.await
		.unwrap();
	let snapshot = manager.compaction_manifest_snapshot();
	let input = snapshot.slabs_at_compaction_level(CompactionLevelIndex::L0)[0].clone();
	let plan = CompactionPlan {
		job_id: CompactionJobId::new(8100),
		source_level: CompactionLevelIndex::L0,
		target_level: CompactionLevelIndex::L1,
		manifest_epoch_pre: snapshot.manifest_epoch,
		input_slabs: vec![input.slab_id],
		overlapping_slabs: Vec::new(),
		retention_floor_snapshot: hlc(100),
		compaction_start_hlc: hlc(101),
		dominant_reason: DominantReason::LevelSize,
	};
	let worker = LsmCompactionWorker::new(LsmConfig::default(), manager.slab_pin_table()).unwrap();
	let prepared = manager
		.prepare_lsm_compaction_outputs(&worker, &plan)
		.await
		.unwrap();

	let err = manager
		.write_lsm_compaction_temp_outputs(&plan, &prepared, &[])
		.await
		.unwrap_err();

	assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
	assert!(
		std::fs::read_dir(&dir)
			.unwrap()
			.flatten()
			.filter_map(|entry| entry.file_name().into_string().ok())
			.all(|name| !name.starts_with("compaction-temp-"))
	);
}

#[tokio::test]
async fn point_read_refuses_lsm_delete_pending_slab() {
	let manager = SlabManager::open(SlabManagerConfig {
		dir: fresh_dir("read-pin"),
		max_slab_bytes: 1,
		io: spawn_io_thread(16).unwrap(),
		l2: None,
		rehydration: RehydrationPolicy::default(),
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

	let first = manager
		.append(tenant(), id(11), hlc(11), b"first-record")
		.await
		.unwrap();
	manager
		.append(tenant(), id(12), hlc(12), b"second-record")
		.await
		.unwrap();
	let pins = manager.slab_pin_table();
	assert!(pins.mark_delete_pending(first.slab_id));

	let err = manager.read(first).await.unwrap_err();

	assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
	assert_eq!(pins.pin_count(first.slab_id), 0);
	pins.clear_delete_pending(first.slab_id);
	assert_eq!(manager.read(first).await.unwrap(), b"first-record");
}

#[tokio::test]
async fn manager_applies_lsm_manifest_compaction_commit() {
	let manager = SlabManager::open(SlabManagerConfig {
		dir: fresh_dir("manifest-commit"),
		max_slab_bytes: 1,
		io: spawn_io_thread(16).unwrap(),
		l2: None,
		rehydration: RehydrationPolicy::default(),
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
		.append(tenant(), id(21), hlc(21), b"first-record")
		.await
		.unwrap();
	manager
		.append(tenant(), id(22), hlc(22), b"second-record")
		.await
		.unwrap();
	let before = manager.compaction_manifest_snapshot();
	let input = before.slabs_at_compaction_level(CompactionLevelIndex::L0)[0].clone();
	let output = LsmRunSummary::new(
		SlabId::new(100),
		CompactionLevelIndex::L1,
		input.min_row_hlc,
		input.max_row_hlc,
		input.size_bytes,
		input.row_count,
		before.manifest_epoch,
	);
	let commit = CompactionCommit {
		job_id: CompactionJobId::new(1000),
		manifest_epoch_pre: before.manifest_epoch,
		slabs_to_retire: vec![input.slab_id],
		slabs_to_add: vec![output],
		slabs_to_mark_delete_pending: Vec::new(),
		slabs_to_remove: Vec::new(),
		sidecar_swaps: Vec::new(),
		digest_summary: digest_summary(),
	};

	let post_epoch = manager
		.apply_lsm_manifest_compaction_commit(&commit)
		.unwrap();
	let after = manager.compaction_manifest_snapshot();

	assert_eq!(post_epoch, before.manifest_epoch.saturating_add(1));
	assert!(
		after
			.slabs_at_compaction_level(CompactionLevelIndex::L0)
			.is_empty()
	);
	assert_eq!(
		after
			.slabs_at_compaction_level(CompactionLevelIndex::L1)
			.iter()
			.map(|run| run.slab_id)
			.collect::<Vec<_>>(),
		vec![SlabId::new(100)]
	);
	assert!(matches!(
		after.run(input.slab_id).map(|run| &run.state),
		Some(SlabState::Retired { successors }) if successors == &vec![SlabId::new(100)]
	));
}

#[tokio::test]
async fn manager_lsm_gc_waits_for_authoritative_index_rewrite() {
	let manager = SlabManager::open(SlabManagerConfig {
		dir: fresh_dir("manifest-gc"),
		max_slab_bytes: 1,
		io: spawn_io_thread(16).unwrap(),
		l2: None,
		rehydration: RehydrationPolicy::default(),
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

	let first = manager
		.append(tenant(), id(31), hlc(31), b"first-record")
		.await
		.unwrap();
	manager
		.append(tenant(), id(32), hlc(32), b"second-record")
		.await
		.unwrap();
	let before = manager.compaction_manifest_snapshot();
	let input = before.slabs_at_compaction_level(CompactionLevelIndex::L0)[0].clone();
	assert_eq!(first.slab_id, input.slab_id);
	let output = LsmRunSummary::new(
		SlabId::new(101),
		CompactionLevelIndex::L1,
		input.min_row_hlc,
		input.max_row_hlc,
		input.size_bytes,
		input.row_count,
		before.manifest_epoch,
	);
	let commit = CompactionCommit {
		job_id: CompactionJobId::new(1001),
		manifest_epoch_pre: before.manifest_epoch,
		slabs_to_retire: vec![input.slab_id],
		slabs_to_add: vec![output],
		slabs_to_mark_delete_pending: Vec::new(),
		slabs_to_remove: Vec::new(),
		sidecar_swaps: Vec::new(),
		digest_summary: digest_summary(),
	};
	manager
		.apply_lsm_manifest_compaction_commit(&commit)
		.unwrap();
	let pins = manager.slab_pin_table();
	let gc = LsmGcWorker::new(LsmConfig::default(), pins.clone()).unwrap();
	let guard = pins.acquire(&[input.slab_id]).unwrap();

	assert!(
		manager
			.sweep_lsm_delete_pending_once(&gc)
			.await
			.unwrap()
			.is_empty()
	);
	assert!(matches!(
		manager
			.compaction_manifest_snapshot()
			.run(input.slab_id)
			.map(|run| &run.state),
		Some(SlabState::Retired { .. })
	));
	drop(guard);

	let deleted = manager.sweep_lsm_delete_pending_once(&gc).await.unwrap();

	assert!(deleted.is_empty());
	assert!(matches!(
		manager
			.compaction_manifest_snapshot()
			.run(input.slab_id)
			.map(|run| &run.state),
		Some(SlabState::Retired { .. })
	));
	assert_eq!(manager.read(first).await.unwrap(), b"first-record");
}

#[tokio::test]
async fn manager_lsm_gc_lifecycle_commits_through_control_ring() {
	let dir = fresh_dir("gc-control-ring");
	let manager = Arc::new(
		SlabManager::open(SlabManagerConfig {
			dir: dir.clone(),
			max_slab_bytes: 1,
			io: spawn_io_thread(16).unwrap(),
			l2: None,
			rehydration: RehydrationPolicy::default(),
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
		.unwrap(),
	);

	manager
		.append(tenant(), id(41), hlc(41), b"first-record")
		.await
		.unwrap();
	manager
		.append(tenant(), id(42), hlc(42), b"second-record")
		.await
		.unwrap();
	let snapshot = manager.compaction_manifest_snapshot();
	let input = snapshot.slabs_at_compaction_level(CompactionLevelIndex::L0)[0].clone();
	let plan = CompactionPlan {
		job_id: CompactionJobId::new(4100),
		source_level: CompactionLevelIndex::L0,
		target_level: CompactionLevelIndex::L1,
		manifest_epoch_pre: snapshot.manifest_epoch,
		input_slabs: vec![input.slab_id],
		overlapping_slabs: Vec::new(),
		retention_floor_snapshot: hlc(100),
		compaction_start_hlc: hlc(101),
		dominant_reason: DominantReason::LevelSize,
	};
	let worker = LsmCompactionWorker::new(LsmConfig::default(), manager.slab_pin_table()).unwrap();
	let committer = ManifestApplyingCommitter {
		manager: manager.clone(),
		applied: AtomicUsize::new(0),
		last_commit: Mutex::new(None),
	};
	manager
		.run_lsm_compaction_once_via_control_ring(&worker, plan, &committer)
		.await
		.unwrap();
	assert!(matches!(
		manager
			.compaction_manifest_snapshot()
			.run(input.slab_id)
			.map(|run| &run.state),
		Some(SlabState::Retired { .. })
	));

	let gc = LsmGcWorker::new(LsmConfig::default(), manager.slab_pin_table()).unwrap();
	let deleted = manager
		.sweep_lsm_delete_pending_once_via_control_ring(&gc, &committer)
		.await
		.unwrap();

	assert_eq!(deleted, vec![input.slab_id]);
	assert!(
		manager
			.compaction_manifest_snapshot()
			.run(input.slab_id)
			.is_none()
	);
	assert!(
		!dir.join(format!("slab_{:016x}.wnd", input.slab_id))
			.exists()
	);
	let last_commit = committer.last_commit.lock().unwrap().clone().unwrap();
	assert_eq!(last_commit.slabs_to_remove, vec![input.slab_id]);
	assert_eq!(committer.applied.load(Ordering::SeqCst), 3);
}
