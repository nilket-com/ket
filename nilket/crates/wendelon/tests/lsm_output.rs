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
			placement: wendelon::schema::table::PlacementSemantics::RawLocality,
			partition: None,
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

/// Rows of one physical frame, differing only by row index within it.
fn frame_row(key_byte: u8, ms: u64, row_idx: u32, offset: u64, length: u32) -> VersionEntry {
	let mut entry = version(key_byte, ms, row_idx, length);
	entry.location.offset = offset;
	entry
}

/// ADR 0097: `location.length` is the CONTAINING FRAME's length, so charging it
/// once per logical row multiplied a multi-row frame by the rows it holds. The
/// measured slab planned 2.20 GiB of physical bytes as 18.81 GiB that way.
///
/// Under the per-row accounting these three rows are charged 270 bytes and split
/// into three runs against a 100-byte target; frame-atomic accounting charges the
/// frame once and needs one run.
#[test]
fn output_planner_charges_a_multi_row_frame_once() {
	let runs = plan_output_runs(
		[
			frame_row(1, 10, 0, 4096, 90),
			frame_row(1, 11, 1, 4096, 90),
			frame_row(1, 12, 2, 4096, 90),
		],
		CompactionLevelIndex::L1,
		100,
		[SlabId::new(10), SlabId::new(11), SlabId::new(12)],
	)
	.unwrap();

	assert_eq!(runs.len(), 1, "one frame is one unit of work");
	assert_eq!(
		runs[0].estimated_size_bytes.get(),
		90,
		"the frame's bytes are charged once, not once per row"
	);
	assert_eq!(runs[0].rows.len(), 3, "all three rows are still planned");
}

/// A run may not take some rows of a frame and leave the rest: that is the split
/// the writer cannot perform, and reaching it costs a whole working set before it
/// reports `does not yet split multi-row frames`. The target here falls INSIDE the
/// second frame, which is the boundary that used to produce a partial frame.
#[test]
fn output_planner_never_splits_one_frame_across_runs() {
	let runs = plan_output_runs(
		[
			frame_row(1, 10, 0, 4096, 60),
			frame_row(1, 11, 1, 4096, 60),
			frame_row(2, 20, 0, 8192, 60),
			frame_row(2, 21, 1, 8192, 60),
		],
		CompactionLevelIndex::L1,
		100,
		[SlabId::new(10), SlabId::new(11)],
	)
	.unwrap();

	assert_eq!(runs.len(), 2);
	for run in &runs {
		let offsets = run
			.rows
			.iter()
			.map(|row| row.location.offset)
			.collect::<std::collections::HashSet<_>>();
		assert_eq!(
			offsets.len(),
			1,
			"a run holds whole frames: {:?}",
			run.rows
				.iter()
				.map(|row| (row.location.offset, row.record_row_index))
				.collect::<Vec<_>>()
		);
		assert_eq!(run.rows.len(), 2, "every row of the frame travels with it");
		assert_eq!(run.estimated_size_bytes.get(), 60);
	}
}
