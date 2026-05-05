use wendelon::schema::{HybridTimestamp, Identity, RecordRowIndex, SlabId, TenantId};
use wendelon::storage::{
	DropReason, RecordLocation, SurvivorOrDrop, VersionEntry, VersionLifecycle, apply_elision,
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

fn version(
	key_byte: u8,
	ms: u64,
	row_idx: u32,
	is_tombstone: bool,
	lifecycle: VersionLifecycle,
) -> VersionEntry {
	VersionEntry {
		tenant_id: TenantId::ZERO,
		row_key_id: key(key_byte),
		row_hlc: hlc(ms),
		record_row_index: RecordRowIndex::new(row_idx),
		location: RecordLocation {
			slab_id: SlabId::new(1),
			offset: ms,
			length: 10,
			hlc: hlc(ms),
			record_row_index: RecordRowIndex::new(row_idx),
			is_tombstone,
		},
		lifecycle,
	}
}

fn survivors(outcome: &[SurvivorOrDrop]) -> Vec<u64> {
	outcome
		.iter()
		.filter_map(|entry| match entry {
			SurvivorOrDrop::Survivor(record) => Some(record.row_hlc.physical_ms),
			SurvivorOrDrop::Drop { .. } => None,
		})
		.collect()
}

fn drops(outcome: &[SurvivorOrDrop]) -> Vec<(u64, DropReason)> {
	outcome
		.iter()
		.filter_map(|entry| match entry {
			SurvivorOrDrop::Survivor(_) => None,
			SurvivorOrDrop::Drop { record, reason } => Some((record.row_hlc.physical_ms, *reason)),
		})
		.collect()
}

#[test]
fn append_only_row_below_floor_drops() {
	let outcome = apply_elision(
		[version(1, 10, 0, false, VersionLifecycle::AppendOnly)],
		hlc(20),
	);

	assert_eq!(survivors(&outcome), Vec::<u64>::new());
	assert_eq!(drops(&outcome), vec![(10, DropReason::Retention)]);
}

#[test]
fn append_only_data_tombstone_pair_drops_only_when_delete_is_below_floor() {
	let below_delete = apply_elision(
		[
			version(1, 10, 0, false, VersionLifecycle::AppendOnly),
			version(1, 20, 0, true, VersionLifecycle::AppendOnly),
		],
		hlc(30),
	);
	assert_eq!(
		drops(&below_delete),
		vec![
			(10, DropReason::TombstonePair),
			(20, DropReason::TombstonePair)
		]
	);

	let above_delete = apply_elision(
		[
			version(1, 10, 0, false, VersionLifecycle::AppendOnly),
			version(1, 40, 0, true, VersionLifecycle::AppendOnly),
		],
		hlc(30),
	);
	assert_eq!(survivors(&above_delete), vec![10, 40]);
	assert!(drops(&above_delete).is_empty());
}

#[test]
fn primary_key_chain_retains_above_floor_and_newest_below_floor_boundary() {
	let outcome = apply_elision(
		[
			version(1, 10, 0, false, VersionLifecycle::PrimaryKey),
			version(1, 20, 0, false, VersionLifecycle::PrimaryKey),
			version(1, 30, 0, false, VersionLifecycle::PrimaryKey),
			version(1, 40, 0, false, VersionLifecycle::PrimaryKey),
		],
		hlc(35),
	);

	assert_eq!(survivors(&outcome), vec![30, 40]);
	assert_eq!(
		drops(&outcome),
		vec![(10, DropReason::PkPrefix), (20, DropReason::PkPrefix)]
	);
}

#[test]
fn primary_key_boundary_tombstone_is_retained() {
	let outcome = apply_elision(
		[
			version(1, 10, 0, false, VersionLifecycle::PrimaryKey),
			version(1, 20, 0, true, VersionLifecycle::PrimaryKey),
			version(1, 40, 0, false, VersionLifecycle::PrimaryKey),
		],
		hlc(30),
	);

	assert_eq!(survivors(&outcome), vec![20, 40]);
	assert_eq!(drops(&outcome), vec![(10, DropReason::PkPrefix)]);
}

#[test]
fn ambiguous_table_ownership_rows_are_retained_fail_closed() {
	let outcome = apply_elision(
		[version(1, 10, 0, false, VersionLifecycle::AmbiguousTable)],
		hlc(30),
	);

	assert_eq!(survivors(&outcome), vec![10]);
	assert!(drops(&outcome).is_empty());
}

#[test]
fn migrated_range_tombstone_below_floor_drops() {
	let outcome = apply_elision(
		[
			version(1, 10, 0, true, VersionLifecycle::MigratedRangeTombstone),
			version(1, 40, 0, true, VersionLifecycle::MigratedRangeTombstone),
		],
		hlc(30),
	);

	assert_eq!(survivors(&outcome), vec![40]);
	assert_eq!(drops(&outcome), vec![(10, DropReason::MigratedRange)]);
}

#[test]
fn survivor_output_is_sorted_by_merge_key() {
	let outcome = apply_elision(
		[
			version(2, 20, 0, false, VersionLifecycle::PrimaryKey),
			version(1, 10, 2, false, VersionLifecycle::PrimaryKey),
			version(1, 10, 1, false, VersionLifecycle::PrimaryKey),
		],
		hlc(0),
	);

	let merge_order = outcome
		.iter()
		.map(|entry| match entry {
			SurvivorOrDrop::Survivor(record) => (
				record.row_hlc.physical_ms,
				record.row_key_id,
				record.record_row_index,
			),
			SurvivorOrDrop::Drop { record, .. } => (
				record.row_hlc.physical_ms,
				record.row_key_id,
				record.record_row_index,
			),
		})
		.collect::<Vec<_>>();

	assert_eq!(
		merge_order,
		vec![
			(10, key(1), RecordRowIndex::new(1)),
			(10, key(1), RecordRowIndex::new(2)),
			(20, key(2), RecordRowIndex::new(0)),
		]
	);
}

#[test]
fn content_hash_duplicate_versions_are_not_recollapsed() {
	let outcome = apply_elision(
		[
			version(1, 40, 0, false, VersionLifecycle::AppendOnlyContentHash),
			version(1, 40, 1, false, VersionLifecycle::AppendOnlyContentHash),
		],
		hlc(30),
	);

	assert_eq!(survivors(&outcome), vec![40, 40]);
	assert!(drops(&outcome).is_empty());
}
