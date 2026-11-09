use arrow::array::Int32Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use wendelon::hash::state_digest::StateDigest;
use wendelon::schema::{
	ByteCount, CdcCursor, CdcRejectCode, CompactionJobId, CompactionLevelIndex, HybridTimestamp,
	Identity, Locality, ManifestEpoch, RecordRowIndex, RowCount, SlabId, TableObjectId,
	TableRetentionPolicy, TenantId, default_kv_table_descriptor_v1,
};
use wendelon::storage::{
	CatchupSnapshot, CdcLiveEntry, CdcLiveTailRing, CompactionCommit, CompactionDigestSummary,
	CompactionManifest, DropReason, EffectiveRetention, LsmConfig, LsmGcWorker, LsmRunSummary,
	RecordLocation, SlabPinTable, SlabState, SurvivorOrDrop, VersionEntry, VersionLifecycle,
	apply_elision, check_read_against_retention,
};

/// A fixed ring owner for fixtures without a store, minted the way the control
/// ring itself decodes one (ADR 0697: only a store mints its own owner).
fn test_owner() -> wendelon::consensus::OwnerRef {
	serde_json::from_str(r#"{"node":1,"incarnation":1}"#).unwrap()
}

fn hlc(ms: u64) -> HybridTimestamp {
	HybridTimestamp {
		physical_ms: ms,
		logical: 0,
		node_id: 1,
	}
}

fn cursor(ms: u64, key: u8) -> CdcCursor {
	CdcCursor::new(hlc(ms), Identity::new([key; 32]), RecordRowIndex::ZERO)
}

fn batch(value: i32) -> Arc<RecordBatch> {
	let schema = Arc::new(Schema::new(vec![Field::new(
		"value",
		DataType::Int32,
		false,
	)]));
	Arc::new(RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![value]))]).unwrap())
}

fn live_entry(ms: u64, key: u8) -> CdcLiveEntry {
	CdcLiveEntry::new(
		cursor(ms, key),
		TenantId::new(7),
		TableObjectId::new([3; 16]),
		default_kv_table_descriptor_v1()
			.unwrap()
			.logical_schema_fingerprint,
		batch(i32::from(key)),
	)
}

fn run(slab_id: u64, level: CompactionLevelIndex, min_ms: u64, max_ms: u64) -> LsmRunSummary {
	LsmRunSummary::new(
		SlabId::new(slab_id),
		level,
		hlc(min_ms),
		hlc(max_ms),
		ByteCount::new(1024),
		RowCount::new(10),
		ManifestEpoch::new(10),
	)
}

fn digest_summary() -> CompactionDigestSummary {
	CompactionDigestSummary {
		visible_pre: StateDigest::ZERO,
		visible_post: StateDigest::ZERO,
		version_entry_delta: StateDigest::ZERO,
	}
}

fn version(ms: u64, lifecycle: VersionLifecycle) -> VersionEntry {
	VersionEntry {
		tenant_id: TenantId::new(7),
		table_object_id: None,
		row_key_id: Identity::new([9; 32]),
		row_hlc: hlc(ms),
		record_row_index: RecordRowIndex::ZERO,
		location: RecordLocation {
			slab_id: SlabId::new(ms),
			offset: ms,
			length: 4,
			hlc: hlc(ms),
			record_row_index: RecordRowIndex::ZERO,
			is_tombstone: false,
			placement: wendelon::schema::table::PlacementSemantics::RawLocality,
			partition: None,
		},
		lifecycle,
	}
}

#[tokio::test]
async fn cdc_catchup_live_tail_handoff_is_monotonic_across_compaction_commit() {
	let mut manifest = CompactionManifest::new(ManifestEpoch::new(10));
	manifest.insert_live_run(run(1, CompactionLevelIndex::L0, 10, 20));

	let commit = CompactionCommit {
		owner: test_owner(),
		job_id: CompactionJobId::new(800),
		manifest_epoch_pre: ManifestEpoch::new(10),
		slabs_to_retire: vec![SlabId::new(1)],
		slabs_to_add: vec![run(10, CompactionLevelIndex::L1, 10, 20)],
		slabs_to_mark_delete_pending: Vec::new(),
		slabs_to_remove: Vec::new(),
		sidecar_swaps: Vec::new(),
		digest_summary: digest_summary(),
	};
	manifest.apply_compaction_commit(&commit).unwrap();
	assert!(matches!(
		manifest.get_run(SlabId::new(1)).map(|run| &run.state),
		Some(SlabState::Retired { .. })
	));

	let ring = CdcLiveTailRing::new(Locality::new(1), u64::MAX);
	ring.push(live_entry(20, 2).with_finalized(true)).await;
	ring.push(live_entry(30, 3).with_finalized(true)).await;

	let catchup_high_water = cursor(20, 2);
	let live = ring
		.eligible_entries_after(
			TenantId::new(7),
			TableObjectId::new([3; 16]),
			catchup_high_water,
			hlc(30),
			16,
		)
		.await;

	assert_eq!(
		live.iter().map(|entry| entry.cursor).collect::<Vec<_>>(),
		vec![cursor(30, 3)]
	);
}

#[test]
fn retention_floor_rejects_cdc_cursor_while_lsm_elides_below_floor_rows() {
	let effective = EffectiveRetention {
		policy: TableRetentionPolicy::InheritCluster,
		floor: Some(hlc(50)),
	};

	let rejected = check_read_against_retention(cursor(40, 4).row_hlc, &effective);
	assert!(rejected.is_err());
	assert_eq!(
		CdcRejectCode::BelowRetentionFloor.as_str(),
		"below_retention_floor"
	);
	assert!(check_read_against_retention(cursor(60, 6).row_hlc, &effective).is_ok());

	let decisions = apply_elision(
		[
			version(40, VersionLifecycle::AppendOnly),
			version(60, VersionLifecycle::AppendOnly),
		],
		&wendelon::storage::PlanElision::RetentionFloor(hlc(50)),
	);
	assert!(matches!(
		&decisions[0],
		SurvivorOrDrop::Drop {
			reason: DropReason::Retention,
			..
		}
	));
	assert!(matches!(&decisions[1], SurvivorOrDrop::Survivor(_)));
}

#[tokio::test]
async fn ring_overflow_continuity_reject_is_not_masked_by_compaction_state() {
	let ring = CdcLiveTailRing::new(Locality::new(1), u64::MAX);
	ring.push(live_entry(90, 9).with_finalized(true)).await;
	let snapshot = CatchupSnapshot::new(hlc(50));

	assert_eq!(
		ring.open_time_ring_continuity_check(&snapshot).await,
		Err(CdcRejectCode::RingOverflowDuringCatchup)
	);
}

#[test]
fn long_running_cdc_pin_blocks_lsm_gc_until_subscription_catchup_drops() {
	let slab_pins = Arc::new(SlabPinTable::default());
	let subscription_pin = slab_pins.acquire(&[SlabId::new(1)]).unwrap();
	let gc = LsmGcWorker::new(LsmConfig::default(), slab_pins.clone()).unwrap();
	let mut manifest = CompactionManifest::new(ManifestEpoch::new(10));
	manifest.insert_live_run(run(1, CompactionLevelIndex::L0, 10, 20));
	manifest.insert_live_run(run(10, CompactionLevelIndex::L1, 10, 20));
	manifest.retire_run(SlabId::new(1), vec![SlabId::new(10)]);

	let pinned = gc.plan_sweep(&manifest.snapshot());
	assert!(pinned.retired_unpinned_slabs.is_empty());
	assert_eq!(pinned.retired_slab_pinned_bytes, 1024);

	drop(subscription_pin);
	let unpinned = gc.plan_sweep(&manifest.snapshot());
	assert_eq!(unpinned.retired_unpinned_slabs, vec![SlabId::new(1)]);
	assert!(slab_pins.mark_delete_pending(SlabId::new(1)));
	assert!(slab_pins.acquire(&[SlabId::new(1)]).is_err());
}

#[tokio::test]
async fn catchup_window_and_live_tail_rescan_do_not_duplicate_boundary_cursor() {
	let ring = CdcLiveTailRing::new(Locality::new(1), u64::MAX);
	for key in [1, 2, 3] {
		ring.push(live_entry(10 + u64::from(key), key).with_finalized(true))
			.await;
	}

	let start = CdcCursor::MIN;
	let upper = cursor(12, 2);
	let catchup = [cursor(11, 1), cursor(12, 2), cursor(13, 3)]
		.into_iter()
		.filter(|cursor| wendelon::schema::cdc_cursor_in_catchup_window(*cursor, start, upper))
		.collect::<Vec<_>>();
	assert_eq!(catchup, vec![cursor(11, 1), cursor(12, 2)]);

	let live = ring
		.eligible_entries_after(
			TenantId::new(7),
			TableObjectId::new([3; 16]),
			upper,
			hlc(13),
			16,
		)
		.await;
	assert_eq!(
		live.iter().map(|entry| entry.cursor).collect::<Vec<_>>(),
		vec![cursor(13, 3)]
	);
}
