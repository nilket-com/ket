use wendelon::schema::{
	ByteCount, CompactionLevelIndex, HybridTimestamp, ManifestEpoch, RowCount, SlabId,
};
use wendelon::storage::{
	DominantReason, LsmConfig, LsmRunSummary, ManifestSnapshot, SlabState, pick_compaction,
};

fn hlc(ms: u64) -> HybridTimestamp {
	HybridTimestamp {
		physical_ms: ms,
		logical: 0,
		node_id: 0,
	}
}

fn run(
	slab_id: u64,
	compaction_level: CompactionLevelIndex,
	min_ms: u64,
	max_ms: u64,
	size_bytes: u64,
) -> LsmRunSummary {
	LsmRunSummary::new(
		SlabId::new(slab_id),
		compaction_level,
		hlc(min_ms),
		hlc(max_ms),
		ByteCount::new(size_bytes),
		RowCount::new(10),
		ManifestEpoch::new(11),
	)
}

fn config() -> LsmConfig {
	LsmConfig {
		lsm_l1_slab_size_target_bytes: 100,
		..LsmConfig::default()
	}
}

#[test]
fn empty_manifest_returns_none() {
	let snapshot = ManifestSnapshot::new(ManifestEpoch::new(1), Vec::new());
	assert!(pick_compaction(&snapshot, &config(), hlc(10_000)).is_none());
}

#[test]
fn three_l0_runs_are_below_threshold() {
	let snapshot = ManifestSnapshot::new(
		ManifestEpoch::new(1),
		vec![
			run(1, CompactionLevelIndex::L0, 1, 2, 10),
			run(2, CompactionLevelIndex::L0, 3, 4, 10),
			run(3, CompactionLevelIndex::L0, 5, 6, 10),
		],
	);
	assert!(pick_compaction(&snapshot, &config(), hlc(10_000)).is_none());
}

#[test]
fn four_aged_l0_runs_pick_l0_to_l1_with_l1_overlaps() {
	let snapshot = ManifestSnapshot::new(
		ManifestEpoch::new(2),
		vec![
			run(1, CompactionLevelIndex::L0, 1, 10, 10),
			run(2, CompactionLevelIndex::L0, 11, 20, 10),
			run(3, CompactionLevelIndex::L0, 21, 30, 10),
			run(4, CompactionLevelIndex::L0, 31, 40, 10),
			run(5, CompactionLevelIndex::L1, 15, 25, 10),
			run(6, CompactionLevelIndex::L1, 100, 120, 10),
		],
	);

	let plan = pick_compaction(&snapshot, &config(), hlc(10_000)).unwrap();

	assert_eq!(plan.source_level, CompactionLevelIndex::L0);
	assert_eq!(plan.target_level, CompactionLevelIndex::L1);
	assert_eq!(
		plan.input_slabs,
		vec![
			SlabId::new(1),
			SlabId::new(2),
			SlabId::new(3),
			SlabId::new(4)
		]
	);
	assert_eq!(plan.overlapping_slabs, vec![SlabId::new(5)]);
	assert_eq!(plan.dominant_reason, DominantReason::LevelSize);
}

#[test]
fn four_l0_runs_with_young_oldest_run_wait() {
	let snapshot = ManifestSnapshot::new(
		ManifestEpoch::new(2),
		vec![
			run(1, CompactionLevelIndex::L0, 9_900, 9_901, 10),
			run(2, CompactionLevelIndex::L0, 9_902, 9_903, 10),
			run(3, CompactionLevelIndex::L0, 9_904, 9_905, 10),
			run(4, CompactionLevelIndex::L0, 9_906, 9_907, 10),
		],
	);

	assert!(pick_compaction(&snapshot, &config(), hlc(10_000)).is_none());
}

#[test]
fn l1_byte_pressure_picks_largest_l1_run_and_l2_overlaps() {
	let snapshot = ManifestSnapshot::new(
		ManifestEpoch::new(3),
		vec![
			run(1, CompactionLevelIndex::L1, 10, 20, 150),
			run(2, CompactionLevelIndex::L1, 30, 40, 500),
			run(3, CompactionLevelIndex::L2, 35, 45, 10),
		],
	);

	let plan = pick_compaction(&snapshot, &config(), hlc(10_000)).unwrap();

	assert_eq!(plan.source_level, CompactionLevelIndex::L1);
	assert_eq!(plan.target_level, CompactionLevelIndex::L2);
	assert_eq!(plan.input_slabs, vec![SlabId::new(2)]);
	assert_eq!(plan.overlapping_slabs, vec![SlabId::new(3)]);
}

#[test]
fn l2_byte_pressure_picks_l2_to_l3() {
	let snapshot = ManifestSnapshot::new(
		ManifestEpoch::new(4),
		vec![run(1, CompactionLevelIndex::L2, 10, 20, 5_000)],
	);

	let plan = pick_compaction(&snapshot, &config(), hlc(10_000)).unwrap();

	assert_eq!(plan.source_level, CompactionLevelIndex::L2);
	assert_eq!(plan.target_level, CompactionLevelIndex::L3);
}

#[test]
fn non_live_slabs_are_never_selected() {
	let snapshot = ManifestSnapshot::new(
		ManifestEpoch::new(5),
		vec![
			run(1, CompactionLevelIndex::L1, 10, 20, 5_000).with_state(SlabState::Retired {
				successors: vec![SlabId::new(9)],
			}),
			run(2, CompactionLevelIndex::L1, 21, 30, 5_000).with_state(SlabState::DeletePending),
		],
	);

	assert!(pick_compaction(&snapshot, &config(), hlc(10_000)).is_none());
}

#[test]
fn debt_scores_refine_dominant_reason_after_level_pressure() {
	let snapshot = ManifestSnapshot::new(
		ManifestEpoch::new(6),
		vec![
			run(1, CompactionLevelIndex::L1, 10, 20, 500).with_debt_scores(0, 9, 0, 0),
			run(2, CompactionLevelIndex::L1, 30, 40, 500),
		],
	);

	let plan = pick_compaction(&snapshot, &config(), hlc(10_000)).unwrap();

	assert_eq!(plan.input_slabs, vec![SlabId::new(1)]);
	assert_eq!(plan.dominant_reason, DominantReason::TombstoneDebt);
}
