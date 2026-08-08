use wendelon::schema::{
	CompactionLevelIndex, HybridTimestamp, Identity, ManifestEpoch, RecordRowIndex, SlabId,
	TenantId,
};
use wendelon::storage::{
	RecordLocation, SurvivorOrDrop, VersionEntry, VersionLifecycle, plan_output_runs,
	required_output_run_count, survivor_records,
};

fn hlc(ms: u64) -> HybridTimestamp {
	HybridTimestamp {
		physical_ms: ms,
		logical: 0,
		node_id: 0,
	}
}

fn key(byte: u8) -> Identity {
	Identity::new([byte; 32])
}

fn version(key_byte: u8, ms: u64, row_idx: u32, length: u32) -> VersionEntry {
	VersionEntry {
		tenant_id: TenantId::ZERO,
		table_object_id: None,
		row_key_id: key(key_byte),
		row_hlc: hlc(ms),
		record_row_index: RecordRowIndex::new(row_idx),
		location: RecordLocation {
			slab_id: SlabId::new(1),
			offset: ms,
			length,
			hlc: hlc(ms),
			record_row_index: RecordRowIndex::new(row_idx),
			is_tombstone: false,
		},
		lifecycle: VersionLifecycle::PrimaryKey,
	}
}

#[test]
fn output_planner_splits_survivors_at_target_size() {
	let runs = plan_output_runs(
		[
			version(1, 10, 0, 40),
			version(1, 20, 0, 40),
			version(1, 30, 0, 40),
		],
		CompactionLevelIndex::L1,
		80,
		[SlabId::new(10), SlabId::new(11)],
	)
	.unwrap();

	assert_eq!(runs.len(), 2);
	assert_eq!(runs[0].slab_id, SlabId::new(10));
	assert_eq!(runs[0].estimated_size_bytes.get(), 80);
	assert_eq!(runs[0].rows.len(), 2);
	assert_eq!(runs[1].slab_id, SlabId::new(11));
	assert_eq!(runs[1].estimated_size_bytes.get(), 40);
	assert_eq!(runs[1].rows.len(), 1);
}

#[test]
fn output_planner_keeps_oversized_single_row_in_one_run() {
	let runs = plan_output_runs(
		[version(1, 10, 0, 120), version(1, 20, 0, 20)],
		CompactionLevelIndex::L1,
		64,
		[SlabId::new(10), SlabId::new(11)],
	)
	.unwrap();

	assert_eq!(runs.len(), 2);
	assert_eq!(runs[0].estimated_size_bytes.get(), 120);
	assert_eq!(runs[0].rows.len(), 1);
	assert_eq!(runs[1].estimated_size_bytes.get(), 20);
}

#[test]
fn output_planner_sorts_rows_by_merge_key_before_splitting() {
	let runs = plan_output_runs(
		[
			version(2, 30, 0, 10),
			version(1, 10, 2, 10),
			version(1, 10, 1, 10),
		],
		CompactionLevelIndex::L2,
		100,
		[SlabId::new(20)],
	)
	.unwrap();

	let rows = runs[0]
		.rows
		.iter()
		.map(|row| {
			(
				row.row_hlc.physical_ms,
				row.row_key_id,
				row.record_row_index,
			)
		})
		.collect::<Vec<_>>();
	assert_eq!(
		rows,
		vec![
			(10, key(1), RecordRowIndex::new(1)),
			(10, key(1), RecordRowIndex::new(2)),
			(30, key(2), RecordRowIndex::new(0)),
		]
	);
}

#[test]
fn output_run_summary_uses_target_level_and_row_hlc_bounds() {
	let runs = plan_output_runs(
		[version(1, 10, 0, 12), version(1, 30, 0, 18)],
		CompactionLevelIndex::L2,
		100,
		[SlabId::new(20)],
	)
	.unwrap();

	let summary = runs[0].summary(ManifestEpoch::new(7));

	assert_eq!(summary.slab_id, SlabId::new(20));
	assert_eq!(summary.compaction_level, CompactionLevelIndex::L2);
	assert_eq!(summary.min_row_hlc, hlc(10));
	assert_eq!(summary.max_row_hlc, hlc(30));
	assert_eq!(summary.row_count.get(), 2);
	assert_eq!(summary.size_bytes.get(), 30);
	assert_eq!(summary.manifest_epoch, ManifestEpoch::new(7));
}

#[test]
fn output_planner_reports_missing_reserved_slab_ids() {
	let err = plan_output_runs(
		[version(1, 10, 0, 40), version(1, 20, 0, 40)],
		CompactionLevelIndex::L1,
		40,
		[SlabId::new(10)],
	)
	.unwrap_err();

	assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
}

#[test]
fn required_output_run_count_matches_split_planner() {
	let records = [
		version(1, 30, 0, 40),
		version(1, 10, 0, 40),
		version(1, 20, 0, 40),
	];

	let count = required_output_run_count(records.clone(), 80).unwrap();
	let runs = plan_output_runs(
		records,
		CompactionLevelIndex::L1,
		80,
		[SlabId::new(10), SlabId::new(11)],
	)
	.unwrap();

	assert_eq!(count, runs.len());
}

#[test]
fn survivor_records_filters_drops_before_output_planning() {
	let survivor = version(1, 30, 0, 10);
	let dropped = version(1, 10, 0, 10);

	let records = survivor_records(&[
		SurvivorOrDrop::Survivor(survivor.clone()),
		SurvivorOrDrop::Drop {
			record: dropped,
			reason: wendelon::storage::DropReason::Retention,
		},
	]);

	assert_eq!(records, vec![survivor]);
}
