use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use wendelon::hash::state_digest::StateDigest;
use wendelon::schema::{
	CompactionJobId, CompactionLevelIndex, HybridTimestamp, Identity, ManifestEpoch,
	RecordRowIndex, SlabId, TenantId,
};
use wendelon::storage::{
	CompactionDigestSummary, CompactionFailure, CompactionManifest, CompactionPlan, DominantReason,
	DropReason, LsmCompactionDriver, LsmCompactionService, LsmCompactionWorker, LsmConfig,
	LsmGcDriver, LsmGcService, LsmGcWorker, LsmRunSummary, RecordLocation, SlabPinTable,
	SurvivorOrDrop, VersionEntry, VersionLifecycle,
};

fn hlc(ms: u64) -> HybridTimestamp {
	HybridTimestamp {
		physical_ms: ms,
		logical: 0,
		node_id: 0,
	}
}

fn plan() -> CompactionPlan {
	CompactionPlan {
		job_id: CompactionJobId::new(77),
		source_level: CompactionLevelIndex::L0,
		target_level: CompactionLevelIndex::L1,
		manifest_epoch_pre: ManifestEpoch::new(5),
		input_slabs: vec![SlabId::new(3), SlabId::new(1)],
		overlapping_slabs: vec![SlabId::new(2), SlabId::new(3)],
		retention_floor_snapshot: hlc(0),
		compaction_start_hlc: hlc(10),
		dominant_reason: DominantReason::LevelSize,
	}
}

fn valid_plan() -> CompactionPlan {
	CompactionPlan {
		job_id: CompactionJobId::new(78),
		source_level: CompactionLevelIndex::L0,
		target_level: CompactionLevelIndex::L1,
		manifest_epoch_pre: ManifestEpoch::new(5),
		input_slabs: vec![SlabId::new(1)],
		overlapping_slabs: vec![SlabId::new(2)],
		retention_floor_snapshot: hlc(25),
		compaction_start_hlc: hlc(50),
		dominant_reason: DominantReason::LevelSize,
	}
}

fn digest_summary() -> CompactionDigestSummary {
	CompactionDigestSummary {
		visible_pre: StateDigest::ZERO,
		visible_post: StateDigest::ZERO,
		version_entry_delta: StateDigest::ZERO,
	}
}

fn key(byte: u8) -> Identity {
	Identity::new([byte; 32])
}

fn version(key_byte: u8, ms: u64, length: u32, lifecycle: VersionLifecycle) -> VersionEntry {
	VersionEntry {
		tenant_id: TenantId::ZERO,
		table_object_id: None,
		row_key_id: key(key_byte),
		row_hlc: hlc(ms),
		record_row_index: RecordRowIndex::ZERO,
		location: RecordLocation {
			slab_id: SlabId::new(1),
			offset: ms,
			length,
			hlc: hlc(ms),
			record_row_index: RecordRowIndex::ZERO,
			is_tombstone: false,
		},
		lifecycle,
	}
}

#[derive(Default)]
struct CountingDriver {
	ticks: AtomicUsize,
}

#[derive(Default)]
struct CountingGcDriver {
	ticks: AtomicUsize,
}

#[async_trait::async_trait]
impl LsmCompactionDriver for CountingDriver {
	async fn run_lsm_compaction_tick(
		&self,
		_worker: &LsmCompactionWorker,
		_now: HybridTimestamp,
	) -> Result<Option<wendelon::storage::CompactionOutcome>, CompactionFailure> {
		self.ticks.fetch_add(1, Ordering::SeqCst);
		Ok(None)
	}
}

#[async_trait::async_trait]
impl LsmGcDriver for CountingGcDriver {
	async fn run_lsm_gc_tick(&self, _worker: &LsmGcWorker) -> std::io::Result<Vec<SlabId>> {
		self.ticks.fetch_add(1, Ordering::SeqCst);
		Ok(vec![SlabId::new(9)])
	}
}

#[test]
fn compaction_plan_touched_slabs_are_unique_and_sorted() {
	assert_eq!(
		plan().touched_slabs(),
		vec![SlabId::new(1), SlabId::new(2), SlabId::new(3)]
	);
}

#[test]
fn compaction_worker_acquires_bulk_pins_for_touched_slabs() {
	let pins = Arc::new(SlabPinTable::default());
	let worker = LsmCompactionWorker::new(LsmConfig::default(), pins.clone()).unwrap();

	let guard = worker.acquire_plan_pins(&plan()).unwrap();

	assert_eq!(
		guard.slabs(),
		&[SlabId::new(1), SlabId::new(2), SlabId::new(3)]
	);
	assert_eq!(pins.pin_count(SlabId::new(1)), 1);
	assert_eq!(pins.pin_count(SlabId::new(2)), 1);
	assert_eq!(pins.pin_count(SlabId::new(3)), 1);
	drop(guard);
	assert_eq!(pins.pin_count(SlabId::new(1)), 0);
	assert_eq!(pins.pin_count(SlabId::new(2)), 0);
	assert_eq!(pins.pin_count(SlabId::new(3)), 0);
}

#[test]
fn compaction_worker_treats_delete_pending_pin_as_manifest_race() {
	let pins = Arc::new(SlabPinTable::default());
	assert!(pins.mark_delete_pending(SlabId::new(2)));
	let worker = LsmCompactionWorker::new(LsmConfig::default(), pins.clone()).unwrap();

	let err = worker.acquire_plan_pins(&plan()).unwrap_err();

	assert!(matches!(err, CompactionFailure::ManifestRace));
	assert_eq!(pins.pin_count(SlabId::new(1)), 0);
	assert_eq!(pins.pin_count(SlabId::new(3)), 0);
}

#[tokio::test]
async fn compaction_service_pause_stops_new_ticks() {
	let driver = Arc::new(CountingDriver::default());
	let worker =
		LsmCompactionWorker::new(LsmConfig::default(), Arc::new(SlabPinTable::default())).unwrap();
	let service = LsmCompactionService::new(driver.clone(), worker);
	let control = service.control();
	control.pause();

	assert!(service.tick_once(hlc(100)).await.unwrap().is_none());
	assert_eq!(driver.ticks.load(Ordering::SeqCst), 0);

	control.resume();
	assert!(service.tick_once(hlc(101)).await.unwrap().is_none());
	assert_eq!(driver.ticks.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn gc_service_tick_delegates_to_driver() {
	let driver = Arc::new(CountingGcDriver::default());
	let worker = LsmGcWorker::new(LsmConfig::default(), Arc::new(SlabPinTable::default())).unwrap();
	let service = LsmGcService::new(driver.clone(), worker);

	assert_eq!(service.tick_once().await.unwrap(), vec![SlabId::new(9)]);
	assert_eq!(driver.ticks.load(Ordering::SeqCst), 1);
}

#[test]
fn compaction_worker_plans_survivor_outputs_with_configured_target_size() {
	let pins = Arc::new(SlabPinTable::default());
	let worker = LsmCompactionWorker::new(
		LsmConfig {
			lsm_l1_slab_size_target_bytes: 15,
			..LsmConfig::default()
		},
		pins,
	)
	.unwrap();
	let mut plan = plan();
	plan.retention_floor_snapshot = hlc(25);

	let prepared = worker
		.plan_survivor_outputs(
			&plan,
			[
				version(1, 10, 10, VersionLifecycle::AppendOnly),
				version(1, 30, 10, VersionLifecycle::AppendOnly),
				version(1, 40, 10, VersionLifecycle::AppendOnly),
			],
			[SlabId::new(10), SlabId::new(11)],
		)
		.unwrap();

	assert!(matches!(
		&prepared.decisions[0],
		SurvivorOrDrop::Drop {
			reason: DropReason::Retention,
			..
		}
	));
	assert_eq!(prepared.output_runs.len(), 2);
	assert_eq!(prepared.output_runs[0].slab_id, SlabId::new(10));
	assert_eq!(prepared.output_runs[0].rows[0].row_hlc, hlc(30));
	assert_eq!(prepared.output_runs[1].slab_id, SlabId::new(11));
	assert_eq!(prepared.output_runs[1].rows[0].row_hlc, hlc(40));
}

#[test]
fn compaction_worker_builds_commit_and_outcome_from_prepared_outputs() {
	let pins = Arc::new(SlabPinTable::default());
	let worker = LsmCompactionWorker::new(
		LsmConfig {
			lsm_l1_slab_size_target_bytes: 15,
			..LsmConfig::default()
		},
		pins,
	)
	.unwrap();
	let plan = valid_plan();
	let prepared = worker
		.plan_survivor_outputs(
			&plan,
			[
				version(1, 10, 10, VersionLifecycle::AppendOnly),
				version(1, 30, 10, VersionLifecycle::AppendOnly),
				version(1, 40, 10, VersionLifecycle::AppendOnly),
			],
			[SlabId::new(10), SlabId::new(11)],
		)
		.unwrap();

	let commit = worker.build_compaction_commit(&plan, &prepared, digest_summary());

	assert_eq!(commit.job_id, plan.job_id);
	assert_eq!(commit.manifest_epoch_pre, ManifestEpoch::new(5));
	assert_eq!(commit.slabs_to_retire, vec![SlabId::new(1), SlabId::new(2)]);
	assert_eq!(
		commit
			.slabs_to_add
			.iter()
			.map(|run| (run.slab_id, run.compaction_level, run.row_count.get()))
			.collect::<Vec<_>>(),
		vec![
			(SlabId::new(10), CompactionLevelIndex::L1, 1),
			(SlabId::new(11), CompactionLevelIndex::L1, 1),
		]
	);

	let outcome =
		worker.compaction_outcome_from_commit(&plan, &prepared, &commit, ManifestEpoch::new(6));

	assert_eq!(outcome.job_id, plan.job_id);
	assert_eq!(outcome.manifest_epoch_pre, ManifestEpoch::new(5));
	assert_eq!(outcome.manifest_epoch_post, ManifestEpoch::new(6));
	assert_eq!(outcome.rows_in, 3);
	assert_eq!(outcome.rows_out, 2);
	assert_eq!(outcome.elided_versions, 1);
	assert_eq!(outcome.bytes_in, 30);
	assert_eq!(outcome.bytes_out, 20);
	assert_eq!(
		outcome
			.output_slabs
			.iter()
			.map(|run| run.manifest_epoch)
			.collect::<Vec<_>>(),
		vec![ManifestEpoch::new(6), ManifestEpoch::new(6)]
	);
}

#[test]
fn worker_commit_artifact_passes_manifest_plan_validation() {
	let pins = Arc::new(SlabPinTable::default());
	let worker = LsmCompactionWorker::new(LsmConfig::default(), pins).unwrap();
	let plan = valid_plan();
	let prepared = worker
		.plan_survivor_outputs(
			&plan,
			[version(1, 30, 10, VersionLifecycle::AppendOnly)],
			[SlabId::new(10)],
		)
		.unwrap();
	let commit = worker.build_compaction_commit(&plan, &prepared, digest_summary());
	let mut manifest = CompactionManifest::new(plan.manifest_epoch_pre);
	manifest.insert_live_run(LsmRunSummary::new(
		SlabId::new(1),
		CompactionLevelIndex::L0,
		hlc(10),
		hlc(20),
		wendelon::schema::ByteCount::new(10),
		wendelon::schema::RowCount::new(1),
		plan.manifest_epoch_pre,
	));
	manifest.insert_live_run(LsmRunSummary::new(
		SlabId::new(2),
		CompactionLevelIndex::L1,
		hlc(15),
		hlc(30),
		wendelon::schema::ByteCount::new(10),
		wendelon::schema::RowCount::new(1),
		plan.manifest_epoch_pre,
	));

	let post_epoch = manifest
		.apply_planned_compaction_commit(&plan, &commit)
		.unwrap();

	assert_eq!(post_epoch, ManifestEpoch::new(6));
	assert!(manifest.get_run(SlabId::new(10)).unwrap().state.is_live());
}
