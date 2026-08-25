use crate::schema::{CompactionLevelIndex, HybridTimestamp, ManifestEpoch, SlabId};
use crate::storage::lsm::{
	CompactionCommit, CompactionFailure, CompactionPlan, LsmRunSummary, SlabState,
};
use std::collections::{BTreeMap, BTreeSet};
use std::io::{Error, ErrorKind};
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ManifestSnapshot {
	pub manifest_epoch: ManifestEpoch,
	runs: Arc<Vec<LsmRunSummary>>,
}

impl ManifestSnapshot {
	pub fn new(manifest_epoch: ManifestEpoch, mut runs: Vec<LsmRunSummary>) -> Self {
		runs.sort_by_key(|run| (run.compaction_level, run.min_row_hlc, run.slab_id));
		Self {
			manifest_epoch,
			runs: Arc::new(runs),
		}
	}

	pub fn runs(&self) -> &[LsmRunSummary] {
		self.runs.as_ref().as_slice()
	}

	pub fn run(&self, slab_id: SlabId) -> Option<&LsmRunSummary> {
		self.runs.iter().find(|run| run.slab_id == slab_id)
	}

	pub fn live_runs(&self) -> impl Iterator<Item = &LsmRunSummary> {
		self.runs.iter().filter(|run| run.state.is_live())
	}

	pub fn retired_runs(&self) -> impl Iterator<Item = &LsmRunSummary> {
		self.runs
			.iter()
			.filter(|run| matches!(run.state, SlabState::Retired { .. }))
	}

	pub fn delete_pending_runs(&self) -> impl Iterator<Item = &LsmRunSummary> {
		self.runs.iter().filter(|run| run.state.is_delete_pending())
	}

	pub fn slabs_at_compaction_level(
		&self,
		compaction_level: CompactionLevelIndex,
	) -> Vec<LsmRunSummary> {
		self.live_runs()
			.filter(|run| run.compaction_level == compaction_level)
			.cloned()
			.collect()
	}

	pub fn overlapping_slabs(
		&self,
		compaction_level: CompactionLevelIndex,
		min_hlc: HybridTimestamp,
		max_hlc: HybridTimestamp,
	) -> Vec<LsmRunSummary> {
		self.live_runs()
			.filter(|run| {
				run.compaction_level == compaction_level
					&& run.min_row_hlc <= max_hlc
					&& run.max_row_hlc >= min_hlc
			})
			.cloned()
			.collect()
	}

	pub fn oldest_l0_slab_age_millis(&self, now: HybridTimestamp) -> Option<u64> {
		self.live_runs()
			.filter(|run| run.compaction_level == CompactionLevelIndex::L0)
			.map(|run| now.physical_ms.saturating_sub(run.min_row_hlc.physical_ms))
			.max()
	}

	pub fn compaction_level_total_bytes(&self, compaction_level: CompactionLevelIndex) -> u64 {
		self.live_runs()
			.filter(|run| run.compaction_level == compaction_level)
			.map(|run| run.size_bytes.get())
			.sum()
	}

	pub fn compaction_level_slab_count(&self, compaction_level: CompactionLevelIndex) -> u32 {
		self.live_runs()
			.filter(|run| run.compaction_level == compaction_level)
			.count()
			.min(u32::MAX as usize) as u32
	}
}

#[derive(Clone, Debug, Default)]
pub struct CompactionManifest {
	manifest_epoch: ManifestEpoch,
	runs: BTreeMap<SlabId, LsmRunSummary>,
}

impl CompactionManifest {
	pub fn new(manifest_epoch: ManifestEpoch) -> Self {
		Self {
			manifest_epoch,
			runs: BTreeMap::new(),
		}
	}

	pub fn from_recovered_runs(manifest_epoch: ManifestEpoch, runs: Vec<LsmRunSummary>) -> Self {
		Self {
			manifest_epoch,
			runs: runs.into_iter().map(|run| (run.slab_id, run)).collect(),
		}
	}

	pub fn manifest_epoch(&self) -> ManifestEpoch {
		self.manifest_epoch
	}

	pub fn get_run(&self, slab_id: SlabId) -> Option<&LsmRunSummary> {
		self.runs.get(&slab_id)
	}

	pub fn insert_live_run(&mut self, mut run: LsmRunSummary) -> Option<LsmRunSummary> {
		run.state = SlabState::Live;
		run.manifest_epoch = self.manifest_epoch;
		self.runs.insert(run.slab_id, run)
	}

	pub fn retire_run(
		&mut self,
		slab_id: SlabId,
		successors: Vec<SlabId>,
	) -> Option<LsmRunSummary> {
		let run = self.runs.get_mut(&slab_id)?;
		run.state = SlabState::Retired { successors };
		run.manifest_epoch = self.manifest_epoch;
		Some(run.clone())
	}

	pub fn mark_delete_pending(&mut self, slab_id: SlabId) -> Option<LsmRunSummary> {
		let run = self.runs.get_mut(&slab_id)?;
		run.state = SlabState::DeletePending;
		run.manifest_epoch = self.manifest_epoch;
		Some(run.clone())
	}

	pub fn remove_run(&mut self, slab_id: SlabId) -> Option<LsmRunSummary> {
		self.runs.remove(&slab_id)
	}

	/// Publish a maintenance rewrite that intentionally removes a declared
	/// subset of the old run's visible rows. This is deliberately separate from
	/// a normal compaction commit: normal compaction structurally requires
	/// visible-digest invariance, while retention, table-drop reclaim, and range
	/// reclaim change visibility by design.
	///
	/// The operation is idempotent by `(old slab, replacement slab)` shape. A
	/// caller persists its resulting run rows atomically with the record-index
	/// relocation before installing the replacement file or changing the
	/// in-memory manager state.
	pub fn apply_maintenance_rewrite(
		&mut self,
		old_slab_id: SlabId,
		replacement: Option<LsmRunSummary>,
	) -> Result<ManifestEpoch, Error> {
		let successors = replacement
			.as_ref()
			.map(|run| vec![run.slab_id])
			.unwrap_or_default();

		if let Some(old) = self.runs.get(&old_slab_id) {
			if matches!(&old.state, SlabState::Retired { successors: existing } if existing == &successors)
				&& replacement.as_ref().is_none_or(|expected| {
					self.runs.get(&expected.slab_id).is_some_and(|actual| {
						actual.state.is_live()
							&& actual.compaction_level == expected.compaction_level
							&& actual.min_row_hlc == expected.min_row_hlc
							&& actual.max_row_hlc == expected.max_row_hlc
							&& actual.size_bytes == expected.size_bytes
							&& actual.row_count == expected.row_count
					})
				}) {
				return Ok(self.manifest_epoch);
			}
		}

		let Some(old) = self.runs.get(&old_slab_id) else {
			return Err(Error::new(
				ErrorKind::NotFound,
				format!("maintenance rewrite source run {old_slab_id} is absent"),
			));
		};
		if !old.state.is_live() {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				format!("maintenance rewrite source run {old_slab_id} is not Live"),
			));
		}
		if let Some(replacement) = &replacement {
			if replacement.slab_id == old_slab_id || self.runs.contains_key(&replacement.slab_id) {
				return Err(Error::new(
					ErrorKind::AlreadyExists,
					format!(
						"maintenance rewrite replacement run {} conflicts with the manifest",
						replacement.slab_id
					),
				));
			}
		}

		let post_epoch = self.advance_epoch();
		let old = self
			.runs
			.get_mut(&old_slab_id)
			.expect("validated maintenance source run disappeared");
		old.state = SlabState::Retired {
			successors: successors.clone(),
		};
		old.manifest_epoch = post_epoch;
		if let Some(mut replacement) = replacement {
			replacement.state = SlabState::Live;
			replacement.manifest_epoch = post_epoch;
			self.runs.insert(replacement.slab_id, replacement);
		}
		Ok(post_epoch)
	}

	pub fn apply_compaction_commit(
		&mut self,
		commit: &CompactionCommit,
	) -> Result<ManifestEpoch, CompactionFailure> {
		if commit.is_lsm_gc_lifecycle_commit() {
			return self.apply_lsm_gc_commit(commit);
		}
		if commit.has_lsm_gc_lifecycle_fields() {
			return Err(CompactionFailure::ManifestRace);
		}
		if self.compaction_commit_already_applied(commit) {
			return Ok(self.manifest_epoch);
		}
		if self.manifest_epoch != commit.manifest_epoch_pre {
			return Err(CompactionFailure::ManifestRace);
		}
		if commit.digest_summary.visible_pre != commit.digest_summary.visible_post {
			return Err(CompactionFailure::VisibleDigestInvariance);
		}

		let mut input_ids = BTreeSet::new();
		for slab_id in &commit.slabs_to_retire {
			if !input_ids.insert(*slab_id) {
				return Err(CompactionFailure::ManifestRace);
			}
			let Some(run) = self.runs.get(slab_id) else {
				return Err(CompactionFailure::ManifestRace);
			};
			if !run.state.is_live() {
				return Err(CompactionFailure::ManifestRace);
			}
		}

		let mut output_ids = BTreeSet::new();
		for run in &commit.slabs_to_add {
			if !output_ids.insert(run.slab_id)
				|| input_ids.contains(&run.slab_id)
				|| self.runs.contains_key(&run.slab_id)
			{
				return Err(CompactionFailure::ManifestRace);
			}
		}

		let post_epoch = self.advance_epoch();
		let successors = commit
			.slabs_to_add
			.iter()
			.map(|run| run.slab_id)
			.collect::<Vec<_>>();
		for slab_id in &commit.slabs_to_retire {
			let run = self
				.runs
				.get_mut(slab_id)
				.expect("validated compaction input slab disappeared");
			run.state = SlabState::Retired {
				successors: successors.clone(),
			};
			run.manifest_epoch = post_epoch;
		}
		for run in &commit.slabs_to_add {
			let mut output = run.clone();
			output.state = SlabState::Live;
			output.manifest_epoch = post_epoch;
			self.runs.insert(output.slab_id, output);
		}
		Ok(post_epoch)
	}

	fn apply_lsm_gc_commit(
		&mut self,
		commit: &CompactionCommit,
	) -> Result<ManifestEpoch, CompactionFailure> {
		if self.lsm_gc_commit_already_applied(commit) {
			return Ok(self.manifest_epoch);
		}
		if self.manifest_epoch != commit.manifest_epoch_pre {
			return Err(CompactionFailure::ManifestRace);
		}
		if commit.digest_summary.visible_pre != commit.digest_summary.visible_post {
			return Err(CompactionFailure::VisibleDigestInvariance);
		}
		if !commit.slabs_to_mark_delete_pending.is_empty() && !commit.slabs_to_remove.is_empty() {
			return Err(CompactionFailure::ManifestRace);
		}

		if !commit.slabs_to_mark_delete_pending.is_empty() {
			let mut ids = BTreeSet::new();
			for slab_id in &commit.slabs_to_mark_delete_pending {
				if !ids.insert(*slab_id) {
					return Err(CompactionFailure::ManifestRace);
				}
				let Some(run) = self.runs.get(slab_id) else {
					return Err(CompactionFailure::ManifestRace);
				};
				if !matches!(run.state, SlabState::Retired { .. }) {
					return Err(CompactionFailure::ManifestRace);
				}
			}
			let post_epoch = self.advance_epoch();
			for slab_id in &commit.slabs_to_mark_delete_pending {
				let run = self
					.runs
					.get_mut(slab_id)
					.expect("validated delete-pending slab disappeared");
				run.state = SlabState::DeletePending;
				run.manifest_epoch = post_epoch;
			}
			return Ok(post_epoch);
		}

		let mut ids = BTreeSet::new();
		for slab_id in &commit.slabs_to_remove {
			if !ids.insert(*slab_id) {
				return Err(CompactionFailure::ManifestRace);
			}
			let Some(run) = self.runs.get(slab_id) else {
				return Err(CompactionFailure::ManifestRace);
			};
			if !matches!(run.state, SlabState::DeletePending) {
				return Err(CompactionFailure::ManifestRace);
			}
		}
		let post_epoch = self.advance_epoch();
		for slab_id in &commit.slabs_to_remove {
			self.runs.remove(slab_id);
		}
		Ok(post_epoch)
	}

	fn lsm_gc_commit_already_applied(&self, commit: &CompactionCommit) -> bool {
		if !commit.slabs_to_mark_delete_pending.is_empty() && !commit.slabs_to_remove.is_empty() {
			return false;
		}
		if !commit.slabs_to_mark_delete_pending.is_empty() {
			return commit.slabs_to_mark_delete_pending.iter().all(|slab_id| {
				matches!(
					self.runs.get(slab_id).map(|run| &run.state),
					Some(SlabState::DeletePending)
				)
			});
		}
		!commit.slabs_to_remove.is_empty()
			&& commit
				.slabs_to_remove
				.iter()
				.all(|slab_id| !self.runs.contains_key(slab_id))
	}

	fn compaction_commit_already_applied(&self, commit: &CompactionCommit) -> bool {
		let successors = commit
			.slabs_to_add
			.iter()
			.map(|run| run.slab_id)
			.collect::<Vec<_>>();
		for slab_id in &commit.slabs_to_retire {
			let Some(run) = self.runs.get(slab_id) else {
				return false;
			};
			if !matches!(&run.state, SlabState::Retired { successors: existing } if existing == &successors)
			{
				return false;
			}
		}
		for expected in &commit.slabs_to_add {
			let Some(run) = self.runs.get(&expected.slab_id) else {
				return false;
			};
			if !run.state.is_live()
				|| run.compaction_level != expected.compaction_level
				|| run.min_row_hlc != expected.min_row_hlc
				|| run.max_row_hlc != expected.max_row_hlc
				|| run.size_bytes != expected.size_bytes
				|| run.row_count != expected.row_count
				|| run.sidecar_coverage != expected.sidecar_coverage
			{
				return false;
			}
		}
		true
	}

	pub fn validate_compaction_commit_against_plan(
		&self,
		plan: &CompactionPlan,
		commit: &CompactionCommit,
	) -> Result<(), CompactionFailure> {
		if commit.has_lsm_gc_lifecycle_fields() {
			return Err(CompactionFailure::ManifestRace);
		}
		if commit.job_id != plan.job_id || commit.manifest_epoch_pre != plan.manifest_epoch_pre {
			return Err(CompactionFailure::ManifestRace);
		}
		if self.manifest_epoch != plan.manifest_epoch_pre {
			return Err(CompactionFailure::ManifestRace);
		}
		let planned_retire = plan.touched_slabs();
		let mut committed_retire = commit.slabs_to_retire.clone();
		committed_retire.sort_unstable();
		committed_retire.dedup();
		if committed_retire != planned_retire
			|| committed_retire.len() != commit.slabs_to_retire.len()
		{
			return Err(CompactionFailure::ManifestRace);
		}

		for slab_id in &plan.input_slabs {
			let Some(run) = self.runs.get(slab_id) else {
				return Err(CompactionFailure::ManifestRace);
			};
			if !run.state.is_live() || run.compaction_level != plan.source_level {
				return Err(CompactionFailure::ManifestRace);
			}
		}
		for slab_id in &plan.overlapping_slabs {
			let Some(run) = self.runs.get(slab_id) else {
				return Err(CompactionFailure::ManifestRace);
			};
			if !run.state.is_live() || run.compaction_level != plan.target_level {
				return Err(CompactionFailure::ManifestRace);
			}
		}
		for run in &commit.slabs_to_add {
			if run.compaction_level != plan.target_level || self.runs.contains_key(&run.slab_id) {
				return Err(CompactionFailure::ManifestRace);
			}
		}
		if commit.digest_summary.visible_pre != commit.digest_summary.visible_post {
			return Err(CompactionFailure::VisibleDigestInvariance);
		}
		Ok(())
	}

	pub fn apply_planned_compaction_commit(
		&mut self,
		plan: &CompactionPlan,
		commit: &CompactionCommit,
	) -> Result<ManifestEpoch, CompactionFailure> {
		self.validate_compaction_commit_against_plan(plan, commit)?;
		self.apply_compaction_commit(commit)
	}

	pub fn advance_epoch(&mut self) -> ManifestEpoch {
		self.manifest_epoch = self.manifest_epoch.saturating_add(1);
		self.manifest_epoch
	}

	pub fn snapshot(&self) -> ManifestSnapshot {
		ManifestSnapshot::new(
			self.manifest_epoch,
			self.runs.values().cloned().collect::<Vec<_>>(),
		)
	}
}
