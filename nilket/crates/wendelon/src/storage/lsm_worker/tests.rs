//! The commit a prepared plan builds: only a plan bound to its outputs' capabilities
//! can reach it (ADR 0697 §4 T2), so these live inside the crate.

use super::*;
use crate::schema::{
	CompactionJobId, CompactionLevelIndex, HybridTimestamp, Identity, ManifestEpoch,
	RecordRowIndex, SlabId,
};
use crate::storage::{
	CompactionDigestSummary, CompactionManifest, CompactionPlan, DominantReason,
	LsmCompactionWorker, LsmConfig, LsmRunSummary, RecordLocation, SlabPinTable, VersionEntry,
	VersionLifecycle,
};

/// A plan bound to capabilities minted for the test, on no store.
fn bound(planned: LsmPlannedOutputs) -> LsmPreparedOutputs {
	let exposed = planned
		.output_runs
		.iter()
		.map(|run| crate::storage::ExposedSlab::unbound_for_test(run.slab_id))
		.collect();
	LsmPreparedOutputs::bind(planned, exposed).unwrap()
}

fn test_owner() -> crate::consensus::OwnerRef {
	serde_json::from_str(r#"{"node":1,"incarnation":1}"#).unwrap()
}

fn hlc(ms: u64) -> HybridTimestamp {
	HybridTimestamp {
		physical_ms: ms,
		logical: 0,
		node_id: 0,
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
	CompactionDigestSummary::zero()
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
			placement: crate::schema::table::PlacementSemantics::RawLocality,
			partition: None,
		},
		lifecycle,
	}
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
	let prepared = bound(prepared);

	let commit = worker.build_compaction_commit(test_owner(), &plan, &prepared, digest_summary());

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
	let prepared = bound(prepared);
	let commit = worker.build_compaction_commit(test_owner(), &plan, &prepared, digest_summary());
	let mut manifest = CompactionManifest::new(plan.manifest_epoch_pre);
	manifest.insert_live_run(LsmRunSummary::new(
		SlabId::new(1),
		CompactionLevelIndex::L0,
		hlc(10),
		hlc(20),
		crate::schema::ByteCount::new(10),
		crate::schema::RowCount::new(1),
		plan.manifest_epoch_pre,
	));
	manifest.insert_live_run(LsmRunSummary::new(
		SlabId::new(2),
		CompactionLevelIndex::L1,
		hlc(15),
		hlc(30),
		crate::schema::ByteCount::new(10),
		crate::schema::RowCount::new(1),
		plan.manifest_epoch_pre,
	));

	let post_epoch = manifest
		.apply_planned_compaction_commit(&plan, &commit)
		.unwrap();

	assert_eq!(post_epoch, ManifestEpoch::new(6));
	assert!(manifest.get_run(SlabId::new(10)).unwrap().state.is_live());
}
