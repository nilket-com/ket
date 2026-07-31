use std::sync::Arc;
use wendelon::schema::{
	ByteCount, CompactionLevelIndex, HybridTimestamp, ManifestEpoch, RowCount, SlabId,
};
use wendelon::storage::{
	CompactionManifest, LsmConfig, LsmGcWorker, LsmRunSummary, SlabPinTable, SlabState,
};

fn hlc(ms: u64) -> HybridTimestamp {
	HybridTimestamp {
		physical_ms: ms,
		logical: 0,
		node_id: 0,
	}
}

fn run(slab_id: u64, state: SlabState) -> LsmRunSummary {
	LsmRunSummary::new(
		SlabId::new(slab_id),
		CompactionLevelIndex::L0,
		hlc(slab_id * 10),
		hlc(slab_id * 10 + 5),
		ByteCount::new(1024),
		RowCount::new(10),
		ManifestEpoch::new(1),
	)
	.with_state(state)
}

#[test]
fn retired_slab_with_active_pin_is_not_delete_pending() {
	let pins = Arc::new(SlabPinTable::default());
	let worker = LsmGcWorker::new(LsmConfig::default(), pins.clone()).unwrap();
	let mut manifest = CompactionManifest::new(ManifestEpoch::new(1));
	let slab = SlabId::new(1);
	manifest.insert_live_run(run(1, SlabState::Live));
	manifest.retire_run(slab, vec![SlabId::new(10)]);
	let _guard = pins.acquire(&[slab]).unwrap();

	let outcome = worker.transition_retired_to_delete_pending(&mut manifest);

	assert!(outcome.marked_delete_pending.is_empty());
	assert_eq!(outcome.retired_slab_pinned_bytes, 1024);
	assert!(!pins.is_delete_pending(slab));
	assert!(matches!(
		manifest.get_run(slab).map(|run| &run.state),
		Some(SlabState::Retired { .. })
	));
}

#[test]
fn retired_slab_transitions_after_last_pin_drops() {
	let pins = Arc::new(SlabPinTable::default());
	let worker = LsmGcWorker::new(LsmConfig::default(), pins.clone()).unwrap();
	let mut manifest = CompactionManifest::new(ManifestEpoch::new(1));
	let slab = SlabId::new(2);
	manifest.insert_live_run(run(2, SlabState::Live));
	manifest.retire_run(slab, vec![SlabId::new(11)]);
	let guard = pins.acquire(&[slab]).unwrap();
	drop(guard);

	let outcome = worker.transition_retired_to_delete_pending(&mut manifest);

	assert_eq!(outcome.marked_delete_pending, vec![slab]);
	assert_eq!(outcome.pending_delete_slabs, 1);
	assert!(pins.is_delete_pending(slab));
	assert!(matches!(
		manifest.get_run(slab).map(|run| &run.state),
		Some(SlabState::DeletePending)
	));
}

#[test]
fn delete_pending_slab_cannot_be_newly_pinned() {
	let pins = Arc::new(SlabPinTable::default());
	let worker = LsmGcWorker::new(LsmConfig::default(), pins.clone()).unwrap();
	let mut manifest = CompactionManifest::new(ManifestEpoch::new(1));
	let slab = SlabId::new(3);
	manifest.insert_live_run(run(3, SlabState::Live));
	manifest.retire_run(slab, vec![SlabId::new(12)]);

	worker.transition_retired_to_delete_pending(&mut manifest);

	assert!(pins.acquire(&[slab]).is_err());
}

#[test]
fn delete_pending_transition_can_be_delayed_by_manager_predicate() {
	let pins = Arc::new(SlabPinTable::default());
	let worker = LsmGcWorker::new(LsmConfig::default(), pins.clone()).unwrap();
	let mut manifest = CompactionManifest::new(ManifestEpoch::new(1));
	let slab = SlabId::new(14);
	manifest.insert_live_run(run(14, SlabState::Live));
	manifest.retire_run(slab, vec![SlabId::new(15)]);

	let delayed = worker
		.transition_retired_to_delete_pending_if(&mut manifest, |candidate| candidate != slab);

	assert!(delayed.marked_delete_pending.is_empty());
	assert!(!pins.is_delete_pending(slab));
	assert!(matches!(
		manifest.get_run(slab).map(|run| &run.state),
		Some(SlabState::Retired { .. })
	));

	let ready = worker.transition_retired_to_delete_pending_if(&mut manifest, |_| true);

	assert_eq!(ready.marked_delete_pending, vec![slab]);
	assert!(pins.is_delete_pending(slab));
	assert!(matches!(
		manifest.get_run(slab).map(|run| &run.state),
		Some(SlabState::DeletePending)
	));
}

#[test]
fn delete_completion_removes_manifest_row_and_updates_pressure() {
	let pins = Arc::new(SlabPinTable::default());
	let worker = LsmGcWorker::new(LsmConfig::default(), pins.clone()).unwrap();
	let mut manifest = CompactionManifest::new(ManifestEpoch::new(1));
	let slab = SlabId::new(4);
	manifest.insert_live_run(run(4, SlabState::Live));
	manifest.retire_run(slab, vec![SlabId::new(13)]);
	worker.transition_retired_to_delete_pending(&mut manifest);

	let removed = worker.complete_delete(&mut manifest, slab);

	assert_eq!(removed, Some(slab));
	assert!(manifest.get_run(slab).is_none());
	assert!(!pins.is_delete_pending(slab));
}
