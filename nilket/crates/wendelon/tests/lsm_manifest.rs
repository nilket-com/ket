use wendelon::schema::{
	ByteCount, CompactionLevelIndex, HybridTimestamp, ManifestEpoch, RowCount, SlabId,
};
use wendelon::storage::{CompactionManifest, LsmRunSummary, SidecarCoverage};

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
) -> LsmRunSummary {
	LsmRunSummary::new(
		SlabId::new(slab_id),
		compaction_level,
		hlc(min_ms),
		hlc(max_ms),
		ByteCount::new(1024),
		RowCount::new(10),
		ManifestEpoch::new(7),
	)
}

#[test]
fn new_admissions_enter_l0_live() {
	let mut manifest = CompactionManifest::new(ManifestEpoch::new(7));
	manifest.insert_live_run(run(1, CompactionLevelIndex::L0, 10, 20));
	let snapshot = manifest.snapshot();
	let l0 = snapshot.slabs_at_compaction_level(CompactionLevelIndex::L0);

	assert_eq!(l0.len(), 1);
	assert_eq!(l0[0].slab_id, SlabId::new(1));
	assert_eq!(l0[0].compaction_level, CompactionLevelIndex::L0);
	assert!(l0[0].state.is_live());
}

#[test]
fn overlap_queries_are_hlc_based_and_live_only() {
	let mut manifest = CompactionManifest::new(ManifestEpoch::new(1));
	manifest.insert_live_run(run(1, CompactionLevelIndex::L1, 10, 20));
	manifest.insert_live_run(run(2, CompactionLevelIndex::L1, 30, 40));
	manifest.insert_live_run(run(3, CompactionLevelIndex::L1, 15, 18));
	manifest.retire_run(SlabId::new(3), vec![SlabId::new(9)]);
	manifest.insert_live_run(run(4, CompactionLevelIndex::L2, 15, 18));

	let overlaps =
		manifest
			.snapshot()
			.overlapping_slabs(CompactionLevelIndex::L1, hlc(18), hlc(35));

	assert_eq!(
		overlaps.iter().map(|run| run.slab_id).collect::<Vec<_>>(),
		vec![SlabId::new(1), SlabId::new(2)]
	);
}

#[test]
fn manifest_snapshot_is_immutable_after_later_mutation() {
	let mut manifest = CompactionManifest::new(ManifestEpoch::new(3));
	manifest.insert_live_run(run(1, CompactionLevelIndex::L0, 1, 2));
	let snapshot = manifest.snapshot();

	manifest.advance_epoch();
	manifest.insert_live_run(run(2, CompactionLevelIndex::L0, 3, 4));
	manifest.retire_run(SlabId::new(1), vec![SlabId::new(2)]);

	assert_eq!(snapshot.manifest_epoch, ManifestEpoch::new(3));
	assert_eq!(
		snapshot
			.slabs_at_compaction_level(CompactionLevelIndex::L0)
			.iter()
			.map(|run| run.slab_id)
			.collect::<Vec<_>>(),
		vec![SlabId::new(1)]
	);
}

#[test]
fn manifest_tracks_level_totals_and_sidecar_coverage() {
	let mut manifest = CompactionManifest::new(ManifestEpoch::new(5));
	let mut first = run(1, CompactionLevelIndex::L2, 10, 20);
	first.size_bytes = ByteCount::new(10);
	first.sidecar_coverage = SidecarCoverage::Complete;
	let mut second = run(2, CompactionLevelIndex::L2, 21, 30);
	second.size_bytes = ByteCount::new(15);
	manifest.insert_live_run(first);
	manifest.insert_live_run(second);

	let snapshot = manifest.snapshot();

	assert_eq!(
		snapshot.compaction_level_slab_count(CompactionLevelIndex::L2),
		2
	);
	assert_eq!(
		snapshot.compaction_level_total_bytes(CompactionLevelIndex::L2),
		25
	);
	assert_eq!(
		snapshot.slabs_at_compaction_level(CompactionLevelIndex::L2)[0].sidecar_coverage,
		SidecarCoverage::Complete
	);
}
