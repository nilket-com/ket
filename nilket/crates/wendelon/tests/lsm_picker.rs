use wendelon::schema::{
	ByteCount, CompactionLevelIndex, HybridTimestamp, ManifestEpoch, RowCount, SlabId,
};
use wendelon::storage::{
	DominantReason, LsmConfig, LsmRunSummary, ManifestSnapshot, SlabState, pick_compaction,
	pick_compaction_where,
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

/// ADR 0097 decision 4: a quarantined run must not starve its LEVEL.
///
/// The ranking is size-dominant, so the largest run is offered first - and a large
/// run is exactly the kind that hits a shape-dependent rewrite failure and stays
/// quarantined. Offering only that one turned a bounded quarantine on one piece of
/// work into an unbounded stall of every other run at the level.
///
/// The assertion is that ANOTHER L1 SOURCE is selected. Falling through to L2 would
/// also avoid the blocked run while proving nothing about same-level progress, so
/// this fixture deliberately has no L2 pressure to fall through to.
#[test]
fn a_blocked_l1_source_does_not_starve_the_rest_of_l1() {
	let blocked = SlabId::new(90);
	let snapshot = ManifestSnapshot::new(
		ManifestEpoch::new(1),
		vec![
			// Largest, so ranked first, and rejected by the predicate.
			run(90, CompactionLevelIndex::L1, 10, 20, 900),
			run(91, CompactionLevelIndex::L1, 30, 40, 300),
			run(92, CompactionLevelIndex::L1, 50, 60, 200),
		],
	);

	// Unfiltered, the picker offers the blocked run: the fixture reproduces the
	// starvation shape rather than assuming it.
	let top = pick_compaction(&snapshot, &config(), hlc(10_000)).expect("L1 is over target");
	assert_eq!(top.input_slabs, vec![blocked]);

	let plan = pick_compaction_where(&snapshot, &config(), hlc(10_000), |plan| {
		!plan.touched_slabs().contains(&blocked)
	})
	.expect("another L1 source is eligible");
	assert_eq!(
		plan.source_level,
		CompactionLevelIndex::L1,
		"progress must be made at the SAME level, not by falling through"
	);
	assert_eq!(plan.input_slabs, vec![SlabId::new(91)]);
	assert!(!plan.touched_slabs().contains(&blocked));
}

/// With every L1 source blocked there is genuinely nothing eligible at that level,
/// and the picker says so rather than offering one anyway.
#[test]
fn a_fully_blocked_level_yields_no_plan() {
	let snapshot = ManifestSnapshot::new(
		ManifestEpoch::new(1),
		vec![
			run(90, CompactionLevelIndex::L1, 10, 20, 900),
			run(91, CompactionLevelIndex::L1, 30, 40, 300),
		],
	);
	assert!(
		pick_compaction_where(&snapshot, &config(), hlc(10_000), |_| false).is_none(),
		"nothing eligible means no plan, not an ineligible one"
	);
}

/// The discriminator for WHERE eligibility is applied.
///
/// The sibling test blocks an L1 *source*, which an implementation that only
/// consulted `input_slabs` would also pass. Here the blocked slab is an
/// OVERLAPPING TARGET of the top-ranked L1 source and appears nowhere in its
/// `input_slabs`, so the top candidate is rejectable only if eligibility is applied
/// to the whole plan.
#[test]
fn a_blocked_overlap_rejects_its_candidate_and_another_l1_source_is_selected() {
	let blocked_overlap = SlabId::new(80);
	let snapshot = ManifestSnapshot::new(
		ManifestEpoch::new(1),
		vec![
			// Largest L1 source, ranked first; its HLC range meets the L2 run below.
			run(90, CompactionLevelIndex::L1, 10, 20, 900),
			// Second L1 source, disjoint in HLC, so it pulls in no overlap.
			run(91, CompactionLevelIndex::L1, 30, 40, 300),
			run(80, CompactionLevelIndex::L2, 10, 20, 50),
		],
	);

	// The fixture must actually produce the shape it guards: the top candidate
	// touches the blocked slab only through its overlaps.
	let top = pick_compaction(&snapshot, &config(), hlc(10_000)).expect("L1 is over target");
	assert_eq!(top.input_slabs, vec![SlabId::new(90)]);
	assert!(
		!top.input_slabs.contains(&blocked_overlap),
		"the blocked slab must NOT be an input, or this proves nothing"
	);
	assert!(
		top.overlapping_slabs.contains(&blocked_overlap),
		"the blocked slab must be an overlap of the top candidate"
	);

	let plan = pick_compaction_where(&snapshot, &config(), hlc(10_000), |plan| {
		!plan.touched_slabs().contains(&blocked_overlap)
	})
	.expect("the second L1 source pulls in no blocked overlap");
	assert_eq!(
		plan.source_level,
		CompactionLevelIndex::L1,
		"progress at the same level"
	);
	assert_eq!(plan.input_slabs, vec![SlabId::new(91)]);
	assert!(!plan.touched_slabs().contains(&blocked_overlap));
}
