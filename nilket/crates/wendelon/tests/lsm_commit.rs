use wendelon::hash::state_digest::StateDigest;
use wendelon::schema::{
	ByteCount, CompactionJobId, CompactionLevelIndex, HybridTimestamp, ManifestEpoch, RowCount,
	SlabId,
};
use wendelon::storage::{
	CompactionCommit, CompactionDigestSummary, CompactionFailure, CompactionManifest,
	CompactionPlan, DominantReason, LsmRunSummary, SlabState,
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
		node_id: 0,
	}
}

fn run(slab_id: u64, compaction_level: CompactionLevelIndex) -> LsmRunSummary {
	LsmRunSummary::new(
		SlabId::new(slab_id),
		compaction_level,
		hlc(slab_id * 10),
		hlc(slab_id * 10 + 5),
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

fn plan() -> CompactionPlan {
	CompactionPlan {
		job_id: CompactionJobId::new(46),
		source_level: CompactionLevelIndex::L0,
		target_level: CompactionLevelIndex::L1,
		manifest_epoch_pre: ManifestEpoch::new(10),
		input_slabs: vec![SlabId::new(1)],
		overlapping_slabs: vec![SlabId::new(2)],
		elision: wendelon::storage::PlanElision::RetentionFloor(hlc(0)),
		compaction_start_hlc: hlc(100),
		dominant_reason: DominantReason::LevelSize,
	}
}

#[test]
fn compaction_commit_swaps_inputs_for_outputs_at_epoch_boundary() {
	let mut manifest = CompactionManifest::new(ManifestEpoch::new(10));
	manifest.insert_live_run(run(1, CompactionLevelIndex::L0));
	manifest.insert_live_run(run(2, CompactionLevelIndex::L0));
	let output = run(10, CompactionLevelIndex::L1);
	let commit = CompactionCommit {
		owner: test_owner(),
		job_id: CompactionJobId::new(42),
		manifest_epoch_pre: ManifestEpoch::new(10),
		slabs_to_retire: vec![SlabId::new(1), SlabId::new(2)],
		slabs_to_add: vec![output],
		slabs_to_mark_delete_pending: Vec::new(),
		slabs_to_remove: Vec::new(),
		sidecar_swaps: Vec::new(),
		digest_summary: digest_summary(),
	};

	let post_epoch = manifest.apply_compaction_commit(&commit).unwrap();

	assert_eq!(post_epoch, ManifestEpoch::new(11));
	assert_eq!(manifest.manifest_epoch(), ManifestEpoch::new(11));
	assert!(matches!(
		manifest.get_run(SlabId::new(1)).map(|run| &run.state),
		Some(SlabState::Retired { successors }) if successors == &vec![SlabId::new(10)]
	));
	assert!(matches!(
		manifest.get_run(SlabId::new(2)).map(|run| &run.state),
		Some(SlabState::Retired { successors }) if successors == &vec![SlabId::new(10)]
	));
	let output = manifest.get_run(SlabId::new(10)).unwrap();
	assert_eq!(output.compaction_level, CompactionLevelIndex::L1);
	assert_eq!(output.manifest_epoch, ManifestEpoch::new(11));
	assert!(output.state.is_live());
}

#[test]
fn compaction_commit_replay_is_idempotent_after_epoch_advance() {
	let mut manifest = CompactionManifest::new(ManifestEpoch::new(10));
	manifest.insert_live_run(run(1, CompactionLevelIndex::L0));
	manifest.insert_live_run(run(2, CompactionLevelIndex::L0));
	let commit = CompactionCommit {
		owner: test_owner(),
		job_id: CompactionJobId::new(42),
		manifest_epoch_pre: ManifestEpoch::new(10),
		slabs_to_retire: vec![SlabId::new(1), SlabId::new(2)],
		slabs_to_add: vec![run(10, CompactionLevelIndex::L1)],
		slabs_to_mark_delete_pending: Vec::new(),
		slabs_to_remove: Vec::new(),
		sidecar_swaps: Vec::new(),
		digest_summary: digest_summary(),
	};
	assert_eq!(
		manifest.apply_compaction_commit(&commit).unwrap(),
		ManifestEpoch::new(11)
	);

	let replay_epoch = manifest.apply_compaction_commit(&commit).unwrap();

	assert_eq!(replay_epoch, ManifestEpoch::new(11));
	assert_eq!(manifest.manifest_epoch(), ManifestEpoch::new(11));
	assert!(matches!(
		manifest.get_run(SlabId::new(1)).map(|run| &run.state),
		Some(SlabState::Retired { successors }) if successors == &vec![SlabId::new(10)]
	));
	assert!(manifest.get_run(SlabId::new(10)).unwrap().state.is_live());
}

#[test]
fn gc_mark_delete_pending_commit_transitions_retired_runs() {
	let mut manifest = CompactionManifest::new(ManifestEpoch::new(10));
	manifest.insert_live_run(run(1, CompactionLevelIndex::L0));
	manifest.retire_run(SlabId::new(1), vec![SlabId::new(10)]);
	let commit = CompactionCommit::gc_mark_delete_pending(
		test_owner(),
		CompactionJobId::new(70),
		ManifestEpoch::new(10),
		vec![SlabId::new(1)],
	);

	let post_epoch = manifest.apply_compaction_commit(&commit).unwrap();

	assert_eq!(post_epoch, ManifestEpoch::new(11));
	assert!(matches!(
		manifest.get_run(SlabId::new(1)).map(|run| &run.state),
		Some(SlabState::DeletePending)
	));
	assert_eq!(
		manifest.apply_compaction_commit(&commit).unwrap(),
		post_epoch
	);
}

#[test]
fn gc_remove_commit_deletes_delete_pending_runs() {
	let mut manifest = CompactionManifest::new(ManifestEpoch::new(10));
	manifest.insert_live_run(run(1, CompactionLevelIndex::L0));
	manifest.retire_run(SlabId::new(1), vec![SlabId::new(10)]);
	manifest.advance_epoch();
	manifest.mark_delete_pending(SlabId::new(1));
	let commit = CompactionCommit::gc_remove(
		test_owner(),
		CompactionJobId::new(71),
		ManifestEpoch::new(11),
		vec![SlabId::new(1)],
	);

	let post_epoch = manifest.apply_compaction_commit(&commit).unwrap();

	assert_eq!(post_epoch, ManifestEpoch::new(12));
	assert!(manifest.get_run(SlabId::new(1)).is_none());
	assert_eq!(
		manifest.apply_compaction_commit(&commit).unwrap(),
		post_epoch
	);
}

#[test]
fn manifest_race_leaves_inputs_live() {
	let mut manifest = CompactionManifest::new(ManifestEpoch::new(10));
	manifest.insert_live_run(run(1, CompactionLevelIndex::L0));
	let commit = CompactionCommit {
		owner: test_owner(),
		job_id: CompactionJobId::new(43),
		manifest_epoch_pre: ManifestEpoch::new(9),
		slabs_to_retire: vec![SlabId::new(1)],
		slabs_to_add: vec![run(10, CompactionLevelIndex::L1)],
		slabs_to_mark_delete_pending: Vec::new(),
		slabs_to_remove: Vec::new(),
		sidecar_swaps: Vec::new(),
		digest_summary: digest_summary(),
	};

	let err = manifest.apply_compaction_commit(&commit).unwrap_err();

	assert!(matches!(err, CompactionFailure::ManifestRace));
	assert_eq!(manifest.manifest_epoch(), ManifestEpoch::new(10));
	assert!(manifest.get_run(SlabId::new(1)).unwrap().state.is_live());
	assert!(manifest.get_run(SlabId::new(10)).is_none());
}

#[test]
fn visible_digest_mismatch_rejects_before_retiring_inputs() {
	let mut manifest = CompactionManifest::new(ManifestEpoch::new(10));
	manifest.insert_live_run(run(1, CompactionLevelIndex::L0));
	let commit = CompactionCommit {
		owner: test_owner(),
		job_id: CompactionJobId::new(44),
		manifest_epoch_pre: ManifestEpoch::new(10),
		slabs_to_retire: vec![SlabId::new(1)],
		slabs_to_add: vec![run(10, CompactionLevelIndex::L1)],
		slabs_to_mark_delete_pending: Vec::new(),
		slabs_to_remove: Vec::new(),
		sidecar_swaps: Vec::new(),
		digest_summary: CompactionDigestSummary {
			visible_pre: StateDigest::ZERO,
			visible_post: StateDigest::new([1; 32]),
			version_entry_delta: StateDigest::ZERO,
		},
	};

	let err = manifest.apply_compaction_commit(&commit).unwrap_err();

	assert!(matches!(err, CompactionFailure::VisibleDigestInvariance));
	assert!(manifest.get_run(SlabId::new(1)).unwrap().state.is_live());
	assert!(manifest.get_run(SlabId::new(10)).is_none());
}

#[test]
fn stale_or_duplicate_output_id_rejects_as_manifest_race() {
	let mut manifest = CompactionManifest::new(ManifestEpoch::new(10));
	manifest.insert_live_run(run(1, CompactionLevelIndex::L0));
	manifest.insert_live_run(run(10, CompactionLevelIndex::L1));
	let commit = CompactionCommit {
		owner: test_owner(),
		job_id: CompactionJobId::new(45),
		manifest_epoch_pre: ManifestEpoch::new(10),
		slabs_to_retire: vec![SlabId::new(1)],
		slabs_to_add: vec![run(10, CompactionLevelIndex::L1)],
		slabs_to_mark_delete_pending: Vec::new(),
		slabs_to_remove: Vec::new(),
		sidecar_swaps: Vec::new(),
		digest_summary: digest_summary(),
	};

	let err = manifest.apply_compaction_commit(&commit).unwrap_err();

	assert!(matches!(err, CompactionFailure::ManifestRace));
	assert!(manifest.get_run(SlabId::new(1)).unwrap().state.is_live());
}

#[test]
fn planned_commit_validates_source_target_levels_and_retire_set() {
	let mut manifest = CompactionManifest::new(ManifestEpoch::new(10));
	manifest.insert_live_run(run(1, CompactionLevelIndex::L0));
	manifest.insert_live_run(run(2, CompactionLevelIndex::L1));
	let plan = plan();
	let commit = CompactionCommit {
		owner: test_owner(),
		job_id: plan.job_id,
		manifest_epoch_pre: plan.manifest_epoch_pre,
		slabs_to_retire: vec![SlabId::new(2), SlabId::new(1)],
		slabs_to_add: vec![run(10, CompactionLevelIndex::L1)],
		slabs_to_mark_delete_pending: Vec::new(),
		slabs_to_remove: Vec::new(),
		sidecar_swaps: Vec::new(),
		digest_summary: digest_summary(),
	};

	let post_epoch = manifest
		.apply_planned_compaction_commit(&plan, &commit)
		.unwrap();

	assert_eq!(post_epoch, ManifestEpoch::new(11));
	assert!(matches!(
		manifest.get_run(SlabId::new(1)).map(|run| &run.state),
		Some(SlabState::Retired { successors }) if successors == &vec![SlabId::new(10)]
	));
	assert!(matches!(
		manifest.get_run(SlabId::new(2)).map(|run| &run.state),
		Some(SlabState::Retired { successors }) if successors == &vec![SlabId::new(10)]
	));
}

#[test]
fn planned_commit_rejects_wrong_source_level_before_mutation() {
	let mut manifest = CompactionManifest::new(ManifestEpoch::new(10));
	manifest.insert_live_run(run(1, CompactionLevelIndex::L1));
	manifest.insert_live_run(run(2, CompactionLevelIndex::L1));
	let plan = plan();
	let commit = CompactionCommit {
		owner: test_owner(),
		job_id: plan.job_id,
		manifest_epoch_pre: plan.manifest_epoch_pre,
		slabs_to_retire: vec![SlabId::new(1), SlabId::new(2)],
		slabs_to_add: vec![run(10, CompactionLevelIndex::L1)],
		slabs_to_mark_delete_pending: Vec::new(),
		slabs_to_remove: Vec::new(),
		sidecar_swaps: Vec::new(),
		digest_summary: digest_summary(),
	};

	let err = manifest
		.apply_planned_compaction_commit(&plan, &commit)
		.unwrap_err();

	assert!(matches!(err, CompactionFailure::ManifestRace));
	assert!(manifest.get_run(SlabId::new(1)).unwrap().state.is_live());
	assert!(manifest.get_run(SlabId::new(10)).is_none());
}

#[test]
fn planned_commit_rejects_missing_overlap_retirement() {
	let mut manifest = CompactionManifest::new(ManifestEpoch::new(10));
	manifest.insert_live_run(run(1, CompactionLevelIndex::L0));
	manifest.insert_live_run(run(2, CompactionLevelIndex::L1));
	let plan = plan();
	let commit = CompactionCommit {
		owner: test_owner(),
		job_id: plan.job_id,
		manifest_epoch_pre: plan.manifest_epoch_pre,
		slabs_to_retire: vec![SlabId::new(1)],
		slabs_to_add: vec![run(10, CompactionLevelIndex::L1)],
		slabs_to_mark_delete_pending: Vec::new(),
		slabs_to_remove: Vec::new(),
		sidecar_swaps: Vec::new(),
		digest_summary: digest_summary(),
	};

	let err = manifest
		.apply_planned_compaction_commit(&plan, &commit)
		.unwrap_err();

	assert!(matches!(err, CompactionFailure::ManifestRace));
	assert!(manifest.get_run(SlabId::new(1)).unwrap().state.is_live());
	assert!(manifest.get_run(SlabId::new(2)).unwrap().state.is_live());
}
