use super::*;

pub(super) fn lsm_run_summary_from_index(
	key_index: &DashMap<TenantKey, Vec<RecordLocation>>,
	slab_id: SlabId,
	size_bytes: u64,
	manifest_epoch: ManifestEpoch,
) -> LsmRunSummary {
	let mut min_row_hlc: Option<HybridTimestamp> = None;
	let mut max_row_hlc: Option<HybridTimestamp> = None;
	let mut row_count = 0_u64;

	for entry in key_index.iter() {
		for location in entry.value() {
			if location.slab_id != slab_id {
				continue;
			}
			min_row_hlc = Some(
				min_row_hlc
					.map(|current| current.min(location.hlc))
					.unwrap_or(location.hlc),
			);
			max_row_hlc = Some(
				max_row_hlc
					.map(|current| current.max(location.hlc))
					.unwrap_or(location.hlc),
			);
			row_count = row_count.saturating_add(1);
		}
	}

	LsmRunSummary::new(
		slab_id,
		CompactionLevelIndex::L0,
		min_row_hlc.unwrap_or_else(zero_hlc),
		max_row_hlc.unwrap_or_else(zero_hlc),
		ByteCount::new(size_bytes),
		RowCount::new(row_count),
		manifest_epoch,
	)
}

pub(super) fn refresh_lsm_level_metrics(snapshot: &ManifestSnapshot) {
	for compaction_level in CompactionLevelIndex::ALL {
		metrics().set_lsm_level_summary(
			compaction_level,
			u64::from(snapshot.compaction_level_slab_count(compaction_level)),
			snapshot.compaction_level_total_bytes(compaction_level),
		);
	}
}

pub(super) fn remove_non_live_lsm_rows_from_index(
	key_index: &DashMap<TenantKey, Vec<RecordLocation>>,
	runs: &[LsmRunSummary],
) {
	let non_live_slabs = runs
		.iter()
		.filter(|run| !run.state.is_live())
		.map(|run| run.slab_id)
		.collect::<HashSet<_>>();
	if non_live_slabs.is_empty() {
		return;
	}
	let keys = key_index
		.iter()
		.map(|entry| *entry.key())
		.collect::<Vec<_>>();
	for key in keys {
		let should_remove_key = if let Some(mut versions) = key_index.get_mut(&key) {
			versions.retain(|location| !non_live_slabs.contains(&location.slab_id));
			versions.is_empty()
		} else {
			false
		};
		if should_remove_key {
			key_index.remove(&key);
		}
	}
}

pub(super) fn lsm_compaction_result_from_failure(err: &CompactionFailure) -> LsmCompactionResult {
	match err {
		CompactionFailure::ManifestRace => LsmCompactionResult::ManifestRace,
		CompactionFailure::SidecarDigestMismatch => LsmCompactionResult::SidecarDigestMismatch,
		CompactionFailure::VisibleDigestInvariance => LsmCompactionResult::VisibleDigestInvariance,
		CompactionFailure::BackfillRaceLost | CompactionFailure::IoError(_) => {
			LsmCompactionResult::Aborted
		}
	}
}

pub(super) struct LsmInstalledCompactionOutputs {
	pub(super) installed: Vec<(LsmWrittenOutputRun, Arc<LocalSlabStore>)>,
	pub(super) removed_rows: Vec<PersistentRecordRow>,
	pub(super) inserted_rows: Vec<PersistentRecordRow>,
}

#[async_trait]
impl LsmCompactionJobRunner for SlabManager {
	async fn run_lsm_compaction_plan(
		&self,
		worker: &LsmCompactionWorker,
		plan: CompactionPlan,
	) -> Result<crate::storage::lsm::CompactionOutcome, CompactionFailure> {
		SlabManager::run_lsm_compaction_once(self, worker, plan).await
	}
}

#[async_trait]
impl LsmCompactionDriver for SlabManager {
	async fn run_lsm_compaction_tick(
		&self,
		worker: &LsmCompactionWorker,
		now: HybridTimestamp,
	) -> Result<Option<crate::storage::lsm::CompactionOutcome>, CompactionFailure> {
		SlabManager::run_lsm_compaction_tick(self, worker, now).await
	}
}

#[async_trait]
impl LsmGcDriver for SlabManager {
	async fn run_lsm_gc_tick(&self, worker: &LsmGcWorker) -> std::io::Result<Vec<SlabId>> {
		SlabManager::sweep_lsm_delete_pending_once(self, worker).await
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(super) struct LsmSourceFrameKey {
	pub(super) slab_id: SlabId,
	pub(super) offset: u64,
	pub(super) length: u32,
}

impl From<RecordLocation> for LsmSourceFrameKey {
	fn from(location: RecordLocation) -> Self {
		Self {
			slab_id: location.slab_id,
			offset: location.offset,
			length: location.length,
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct LsmMultiRowSourceFrame {
	pub(super) frame: Vec<u8>,
	pub(super) entries: Vec<MultiRowIndexEntry>,
}

pub(super) fn lsm_multirow_index_entry_matches_version(
	entry: &MultiRowIndexEntry,
	version: &VersionEntry,
) -> bool {
	entry.key_identity == version.row_key_id
		&& entry.row_hlc == version.row_hlc
		&& entry.record_row_index == version.record_row_index
		&& entry.is_tombstone == version.location.is_tombstone
}

pub(super) fn lsm_multirow_index_entry_for_version<'a>(
	entries: &'a [MultiRowIndexEntry],
	version: &VersionEntry,
) -> Option<&'a MultiRowIndexEntry> {
	entries
		.iter()
		.find(|entry| lsm_multirow_index_entry_matches_version(entry, version))
}

pub(super) fn validate_lsm_multirow_source_entry(
	version: &VersionEntry,
	entry: &MultiRowIndexEntry,
) -> std::io::Result<()> {
	if lsm_multirow_index_entry_matches_version(entry, version) {
		return Ok(());
	}
	Err(Error::new(
		ErrorKind::InvalidData,
		"LSM multi-row source entry does not match planned survivor",
	))
}

pub(super) fn lsm_compaction_tmp_path(
	dir: &Path,
	job_id: CompactionJobId,
	slab_id: SlabId,
) -> PathBuf {
	dir.join(format!(
		"compaction-temp-{job_id:016x}-slab_{slab_id:016x}.wnd"
	))
}

pub(super) fn is_lsm_compaction_tmp_filename(name: &str) -> bool {
	name.starts_with("compaction-temp-")
}

pub(super) async fn cleanup_lsm_compaction_tmp_files(
	file_ops: &dyn AsyncFileOps,
	dir: &Path,
) -> std::io::Result<()> {
	let mut removed = 0usize;
	for entry in file_ops.read_dir(dir).await? {
		if !entry.is_file {
			continue;
		}
		let Some(name) = entry.file_name.to_str() else {
			continue;
		};
		if !is_lsm_compaction_tmp_filename(name) {
			continue;
		}
		unlink_local_file(file_ops, &dir.join(name)).await?;
		removed = removed.saturating_add(1);
	}
	if removed > 0 {
		tracing::warn!(
			dir = %dir.display(),
			removed_tmp_files = removed,
			"removed stale LSM compaction temp files during open"
		);
	}
	Ok(())
}

impl SlabManager {
	pub async fn reserve_lsm_output_slab_ids(&self, count: usize) -> std::io::Result<Vec<SlabId>> {
		let count = u64::try_from(count).map_err(|_| {
			Error::new(
				ErrorKind::InvalidInput,
				"LSM output slab reservation count does not fit in u64",
			)
		})?;
		let mut state = self.state.lock().await;
		let start = state.next_slab_id.get();
		let end = start.checked_add(count).ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidInput,
				"LSM output slab reservation would overflow SlabId",
			)
		})?;
		state.next_slab_id = SlabId::new(end);
		Ok((start..end).map(SlabId::new).collect())
	}

	pub fn apply_lsm_manifest_compaction_commit(
		&self,
		commit: &CompactionCommit,
	) -> Result<ManifestEpoch, CompactionFailure> {
		let mut manifest = recover_rwlock_write(&self.compaction_manifest, "compaction_manifest");
		let post_epoch = manifest.apply_compaction_commit(commit)?;
		let snapshot = manifest.snapshot();
		drop(manifest);
		self.apply_lsm_pin_state_for_manifest_commit(commit);
		refresh_lsm_level_metrics(&snapshot);
		Ok(post_epoch)
	}

	pub(super) fn apply_lsm_pin_state_for_manifest_commit(&self, commit: &CompactionCommit) {
		for slab_id in &commit.slabs_to_mark_delete_pending {
			self.slab_pins.force_delete_pending(*slab_id);
		}
		for slab_id in &commit.slabs_to_remove {
			self.slab_pins.clear_delete_pending(*slab_id);
		}
	}

	pub fn apply_lsm_manifest_planned_compaction_commit(
		&self,
		plan: &CompactionPlan,
		commit: &CompactionCommit,
	) -> Result<ManifestEpoch, CompactionFailure> {
		let mut manifest = recover_rwlock_write(&self.compaction_manifest, "compaction_manifest");
		let post_epoch = manifest.apply_planned_compaction_commit(plan, commit)?;
		let snapshot = manifest.snapshot();
		drop(manifest);
		self.apply_lsm_pin_state_for_manifest_commit(commit);
		refresh_lsm_level_metrics(&snapshot);
		Ok(post_epoch)
	}

	pub async fn install_lsm_compaction_temp_outputs(
		&self,
		plan: &CompactionPlan,
		prepared: &LsmPreparedOutputs,
		written: &[LsmWrittenOutputRun],
		commit: &CompactionCommit,
	) -> Result<ManifestEpoch, CompactionFailure> {
		let staged = self
			.stage_lsm_compaction_outputs(plan, prepared, written, commit, false)
			.await?;
		let post_epoch = match self.apply_lsm_manifest_planned_compaction_commit(plan, commit) {
			Ok(post_epoch) => post_epoch,
			Err(err) => {
				self.cleanup_lsm_installed_outputs(&staged.installed).await;
				return Err(err);
			}
		};
		self.complete_lsm_compaction_install(staged, post_epoch)
			.await
	}

	pub async fn install_lsm_compaction_temp_outputs_via_control_ring(
		&self,
		plan: &CompactionPlan,
		prepared: &LsmPreparedOutputs,
		written: &[LsmWrittenOutputRun],
		commit: &CompactionCommit,
		committer: &dyn TierCommitter,
	) -> Result<ManifestEpoch, CompactionFailure> {
		self.install_lsm_compaction_temp_outputs_via_control_ring_with_context(
			plan, prepared, written, commit, committer, None,
		)
		.await
	}

	pub(super) async fn install_lsm_compaction_temp_outputs_via_control_ring_with_context(
		&self,
		plan: &CompactionPlan,
		prepared: &LsmPreparedOutputs,
		written: &[LsmWrittenOutputRun],
		commit: &CompactionCommit,
		committer: &dyn TierCommitter,
		secondary_index_context: Option<SecondaryIndexCompactionContext<'_>>,
	) -> Result<ManifestEpoch, CompactionFailure> {
		let staged = self
			.stage_lsm_compaction_outputs(
				plan,
				prepared,
				written,
				commit,
				secondary_index_context.is_some(),
			)
			.await?;
		let prepared_sidecars = match self
			.prepare_lsm_secondary_index_segments_for_compaction(
				plan,
				written,
				secondary_index_context,
			)
			.await
		{
			Ok(prepared_sidecars) => prepared_sidecars,
			Err(err) => {
				self.cleanup_lsm_installed_outputs(&staged.installed).await;
				return Err(err);
			}
		};
		let commit_result = committer.commit_lsm_compaction(commit.clone()).await;
		let Some(post_epoch) = self.lsm_committed_manifest_epoch(commit) else {
			self.cleanup_lsm_installed_outputs(&staged.installed).await;
			return match commit_result {
				Ok(()) => Err(CompactionFailure::ManifestRace),
				Err(err) => Err(err.into()),
			};
		};
		let post_epoch = self
			.complete_lsm_compaction_install(staged, post_epoch)
			.await?;
		self.publish_prepared_lsm_secondary_index_segments(prepared_sidecars)
			.await;
		Ok(post_epoch)
	}

	pub(super) async fn stage_lsm_compaction_outputs(
		&self,
		plan: &CompactionPlan,
		prepared: &LsmPreparedOutputs,
		written: &[LsmWrittenOutputRun],
		commit: &CompactionCommit,
		allow_sidecar_covered_inputs: bool,
	) -> Result<LsmInstalledCompactionOutputs, CompactionFailure> {
		self.validate_lsm_written_outputs(plan, prepared, written, commit)?;
		self.validate_lsm_sidecar_swap_readiness(plan, commit, allow_sidecar_covered_inputs)?;
		{
			let manifest = recover_rwlock_read(&self.compaction_manifest, "compaction_manifest");
			manifest.validate_compaction_commit_against_plan(plan, commit)?;
		}

		let installed = self.install_lsm_output_files(written).await?;
		let max_output_slab_id = installed.iter().map(|(run, _)| run.slab_id).max();

		let removed_rows = self.lsm_removed_persistent_rows(prepared);
		let inserted_rows = self.lsm_inserted_persistent_rows(prepared, written);

		{
			let state = self.state.lock().await;
			for (run, _) in &installed {
				if state.sealed.contains_key(&run.slab_id)
					|| state.tiered.contains_key(&run.slab_id)
				{
					drop(state);
					self.cleanup_lsm_installed_outputs(&installed).await;
					return Err(CompactionFailure::ManifestRace);
				}
			}
		}
		if let Some(max_output_slab_id) = max_output_slab_id {
			let mut state = self.state.lock().await;
			if let Err(err) = self
				.advance_active_after_lsm_outputs(&mut state, max_output_slab_id)
				.await
			{
				drop(state);
				self.cleanup_lsm_installed_outputs(&installed).await;
				return Err(err.into());
			}
		}

		Ok(LsmInstalledCompactionOutputs {
			installed,
			removed_rows,
			inserted_rows,
		})
	}

	pub(super) async fn complete_lsm_compaction_install(
		&self,
		staged: LsmInstalledCompactionOutputs,
		post_epoch: ManifestEpoch,
	) -> Result<ManifestEpoch, CompactionFailure> {
		let lsm_runs = self.compaction_manifest_snapshot().runs().to_vec();
		let LsmInstalledCompactionOutputs {
			installed,
			removed_rows,
			inserted_rows,
		} = staged;

		{
			let state = self.state.lock().await;
			for (run, slab) in installed {
				state.sealed.insert(run.slab_id, slab);
			}
			for row in &removed_rows {
				self.rollback_indexed_location(row.tenant_id, row.key_identity, row.location);
			}
			for row in &inserted_rows {
				self.index_appended_location_with_locality(
					row.tenant_id,
					row.key_identity,
					row.routing_locality,
					row.location,
				);
				if let Some(table_object_id) = row.table_object_id {
					self.add_published_table_reference(
						row.tenant_id,
						table_object_id,
						row.location,
					);
				}
			}
		}

		self.mirror_persistent_update(
			PersistentIndexUpdate {
				delete_records: removed_rows,
				put_records: inserted_rows,
				put_lsm_runs: lsm_runs,
				..Default::default()
			},
			"lsm-compaction-install",
		)
		.await;
		Ok(post_epoch)
	}

	pub(super) fn lsm_committed_manifest_epoch(
		&self,
		commit: &CompactionCommit,
	) -> Option<ManifestEpoch> {
		let snapshot = self.compaction_manifest_snapshot();
		let successors = commit
			.slabs_to_add
			.iter()
			.map(|run| run.slab_id)
			.collect::<Vec<_>>();
		for slab_id in &commit.slabs_to_retire {
			let run = snapshot.run(*slab_id)?;
			if !matches!(&run.state, crate::storage::SlabState::Retired { successors: existing } if existing == &successors)
			{
				return None;
			}
		}
		for expected in &commit.slabs_to_add {
			let run = snapshot.run(expected.slab_id)?;
			if !run.state.is_live()
				|| run.compaction_level != expected.compaction_level
				|| run.min_row_hlc != expected.min_row_hlc
				|| run.max_row_hlc != expected.max_row_hlc
				|| run.size_bytes != expected.size_bytes
				|| run.row_count != expected.row_count
				|| run.sidecar_coverage != expected.sidecar_coverage
			{
				return None;
			}
		}
		Some(snapshot.manifest_epoch)
	}

	pub async fn run_lsm_compaction_once(
		&self,
		worker: &LsmCompactionWorker,
		plan: CompactionPlan,
	) -> Result<crate::storage::lsm::CompactionOutcome, CompactionFailure> {
		let started = Instant::now();
		let source_level = plan.source_level;
		let input_slabs = plan.touched_slabs().len() as u64;
		let result = self.run_lsm_compaction_once_inner(worker, plan).await;
		self.observe_lsm_compaction_result(source_level, input_slabs, started, &result);
		result
	}

	pub async fn run_lsm_compaction_once_via_control_ring(
		&self,
		worker: &LsmCompactionWorker,
		plan: CompactionPlan,
		committer: &dyn TierCommitter,
	) -> Result<crate::storage::lsm::CompactionOutcome, CompactionFailure> {
		self.run_lsm_compaction_once_via_control_ring_with_context(worker, plan, committer, None)
			.await
	}

	pub(super) async fn run_lsm_compaction_once_via_control_ring_with_context(
		&self,
		worker: &LsmCompactionWorker,
		plan: CompactionPlan,
		committer: &dyn TierCommitter,
		secondary_index_context: Option<SecondaryIndexCompactionContext<'_>>,
	) -> Result<crate::storage::lsm::CompactionOutcome, CompactionFailure> {
		let started = Instant::now();
		let source_level = plan.source_level;
		let input_slabs = plan.touched_slabs().len() as u64;
		let result = self
			.run_lsm_compaction_once_inner_via_control_ring(
				worker,
				plan,
				committer,
				secondary_index_context,
			)
			.await;
		self.observe_lsm_compaction_result(source_level, input_slabs, started, &result);
		result
	}

	pub(super) fn observe_lsm_compaction_result(
		&self,
		source_level: CompactionLevelIndex,
		input_slabs: u64,
		started: Instant,
		result: &Result<crate::storage::lsm::CompactionOutcome, CompactionFailure>,
	) {
		match result {
			Ok(outcome) => metrics().observe_lsm_compaction(
				source_level,
				LsmCompactionResult::Ok,
				input_slabs,
				outcome.bytes_out,
				started.elapsed(),
			),
			Err(err) => {
				let metric_result = lsm_compaction_result_from_failure(err);
				if matches!(metric_result, LsmCompactionResult::VisibleDigestInvariance) {
					metrics().observe_lsm_digest_invariance_violation(
						LsmDigestInvariantKind::VisibleDomain,
					);
				}
				metrics().observe_lsm_compaction(
					source_level,
					metric_result,
					input_slabs,
					0,
					started.elapsed(),
				);
			}
		}
	}

	pub fn pick_lsm_compaction_plan(
		&self,
		worker: &LsmCompactionWorker,
		now: HybridTimestamp,
	) -> Option<CompactionPlan> {
		let snapshot = self.compaction_manifest_snapshot();
		pick_compaction(&snapshot, &worker.config(), now).filter(|plan| {
			!plan
				.touched_slabs()
				.into_iter()
				.any(|slab_id| self.is_compaction_slab_quarantined(slab_id))
		})
	}

	pub async fn run_lsm_compaction_tick(
		&self,
		worker: &LsmCompactionWorker,
		now: HybridTimestamp,
	) -> Result<Option<crate::storage::lsm::CompactionOutcome>, CompactionFailure> {
		let Some(plan) = self.pick_lsm_compaction_plan(worker, now) else {
			return Ok(None);
		};
		self.run_lsm_compaction_once(worker, plan).await.map(Some)
	}

	pub async fn run_lsm_compaction_tick_via_control_ring(
		&self,
		worker: &LsmCompactionWorker,
		now: HybridTimestamp,
		committer: &dyn TierCommitter,
	) -> Result<Option<crate::storage::lsm::CompactionOutcome>, CompactionFailure> {
		let Some(plan) = self.pick_lsm_compaction_plan(worker, now) else {
			return Ok(None);
		};
		self.run_lsm_compaction_once_via_control_ring(worker, plan, committer)
			.await
			.map(Some)
	}

	pub async fn run_lsm_compaction_tick_via_control_ring_with_catalogs(
		&self,
		worker: &LsmCompactionWorker,
		now: HybridTimestamp,
		committer: &dyn TierCommitter,
		table_schemas: &TableSchemaCatalog,
		secondary_indexes: &SecondaryIndexCatalog,
	) -> Result<Option<crate::storage::lsm::CompactionOutcome>, CompactionFailure> {
		let Some(plan) = self.pick_lsm_compaction_plan(worker, now) else {
			return Ok(None);
		};
		self.run_lsm_compaction_once_via_control_ring_with_context(
			worker,
			plan,
			committer,
			Some(SecondaryIndexCompactionContext {
				table_schemas,
				secondary_indexes,
			}),
		)
		.await
		.map(Some)
	}

	pub(super) async fn run_lsm_compaction_once_inner(
		&self,
		worker: &LsmCompactionWorker,
		plan: CompactionPlan,
	) -> Result<crate::storage::lsm::CompactionOutcome, CompactionFailure> {
		let _pins = worker.acquire_plan_pins(&plan)?;
		let prepared = self.prepare_lsm_compaction_outputs(worker, &plan).await?;
		let source_frames = self.read_lsm_single_row_source_frames(&plan).await?;
		let written = self
			.write_lsm_compaction_temp_outputs(&plan, &prepared, &source_frames)
			.await?;
		let digest_summary =
			match self.lsm_single_row_bridge_digest_summary(&prepared, &written, &source_frames) {
				Ok(digest_summary) => digest_summary,
				Err(err) => {
					self.cleanup_lsm_written_outputs(&written).await;
					return Err(err);
				}
			};
		let commit = worker.build_written_compaction_commit(&plan, &written, digest_summary);
		let post_epoch = match self
			.install_lsm_compaction_temp_outputs(&plan, &prepared, &written, &commit)
			.await
		{
			Ok(post_epoch) => post_epoch,
			Err(err) => {
				self.cleanup_lsm_written_outputs(&written).await;
				return Err(err);
			}
		};
		Ok(worker.compaction_outcome_from_commit(&plan, &prepared, &commit, post_epoch))
	}

	pub(super) async fn run_lsm_compaction_once_inner_via_control_ring(
		&self,
		worker: &LsmCompactionWorker,
		plan: CompactionPlan,
		committer: &dyn TierCommitter,
		secondary_index_context: Option<SecondaryIndexCompactionContext<'_>>,
	) -> Result<crate::storage::lsm::CompactionOutcome, CompactionFailure> {
		let _pins = worker.acquire_plan_pins(&plan)?;
		let prepared = match secondary_index_context {
			Some(context) => {
				self.prepare_lsm_compaction_outputs_with_table_schemas(
					worker,
					&plan,
					context.table_schemas,
				)
				.await?
			}
			None => self.prepare_lsm_compaction_outputs(worker, &plan).await?,
		};
		let source_frames = self.read_lsm_single_row_source_frames(&plan).await?;
		let written = self
			.write_lsm_compaction_temp_outputs(&plan, &prepared, &source_frames)
			.await?;
		let digest_summary =
			match self.lsm_single_row_bridge_digest_summary(&prepared, &written, &source_frames) {
				Ok(digest_summary) => digest_summary,
				Err(err) => {
					self.cleanup_lsm_written_outputs(&written).await;
					return Err(err);
				}
			};
		let commit = worker.build_written_compaction_commit(&plan, &written, digest_summary);
		let post_epoch = match self
			.install_lsm_compaction_temp_outputs_via_control_ring_with_context(
				&plan,
				&prepared,
				&written,
				&commit,
				committer,
				secondary_index_context,
			)
			.await
		{
			Ok(post_epoch) => post_epoch,
			Err(err) => {
				self.cleanup_lsm_written_outputs(&written).await;
				return Err(err);
			}
		};
		Ok(worker.compaction_outcome_from_commit(&plan, &prepared, &commit, post_epoch))
	}

	pub(super) fn lsm_single_row_bridge_digest_summary(
		&self,
		prepared: &LsmPreparedOutputs,
		written: &[LsmWrittenOutputRun],
		source_frames: &[LsmSourceFrame],
	) -> Result<CompactionDigestSummary, CompactionFailure> {
		let survivors = survivor_records(&prepared.decisions);
		let output_rows = prepared
			.output_runs
			.iter()
			.flat_map(|run| run.rows.iter().cloned())
			.collect::<Vec<_>>();
		let output_location_count = written.iter().map(|run| run.locations.len()).sum::<usize>();
		if survivors != output_rows || output_rows.len() != output_location_count {
			return Err(CompactionFailure::VisibleDigestInvariance);
		}
		let table_by_location = self.lsm_source_frame_table_map(source_frames)?;
		let mut pre_rows = Vec::with_capacity(prepared.decisions.len());
		let mut post_rows = Vec::with_capacity(survivors.len());
		for decision in &prepared.decisions {
			let record = match decision {
				crate::storage::SurvivorOrDrop::Survivor(record) => record,
				crate::storage::SurvivorOrDrop::Drop { record, .. } => record,
			};
			let visible_row = self.lsm_visible_domain_row_for_record(record, &table_by_location)?;
			pre_rows.push(visible_row);
			if matches!(decision, crate::storage::SurvivorOrDrop::Survivor(_)) {
				post_rows.push(visible_row);
			}
		}
		let digest_summary = compaction_visible_domain_summary(pre_rows, post_rows);
		if digest_summary.visible_pre != digest_summary.visible_post {
			return Err(CompactionFailure::VisibleDigestInvariance);
		}
		Ok(digest_summary)
	}

	pub(super) fn lsm_source_frame_table_map(
		&self,
		source_frames: &[LsmSourceFrame],
	) -> Result<HashMap<RecordLocation, TableObjectId>, CompactionFailure> {
		let mut table_by_location = HashMap::with_capacity(source_frames.len());
		for source in source_frames {
			table_by_location.insert(
				source.entry.location,
				self.lsm_source_frame_table_object_id(source)?,
			);
		}
		Ok(table_by_location)
	}

	pub(super) fn lsm_source_frame_table_object_id(
		&self,
		source: &LsmSourceFrame,
	) -> Result<TableObjectId, CompactionFailure> {
		let batch = match decode_record_batch(&source.frame) {
			Ok(batch) => batch,
			Err(_) => return Ok(TableObjectId::DEFAULT_KV),
		};
		match storage_row_table_object_id_at(&batch, source.entry.record_row_index)? {
			Some(table_object_id) => Ok(table_object_id),
			None => Ok(TableObjectId::DEFAULT_KV),
		}
	}

	pub(super) fn lsm_visible_domain_row_for_record(
		&self,
		record: &VersionEntry,
		table_by_location: &HashMap<RecordLocation, TableObjectId>,
	) -> Result<VisibleDomainRow, CompactionFailure> {
		let Some(table_object_id) = table_by_location.get(&record.location).copied() else {
			return Err(CompactionFailure::VisibleDigestInvariance);
		};
		Ok(VisibleDomainRow {
			tenant_id: record.tenant_id,
			table_object_id,
			row_key_id: record.row_key_id,
			row_hlc: record.row_hlc,
			is_tombstone: record.location.is_tombstone,
		})
	}

	pub(super) fn validate_lsm_written_outputs(
		&self,
		plan: &CompactionPlan,
		prepared: &LsmPreparedOutputs,
		written: &[LsmWrittenOutputRun],
		commit: &CompactionCommit,
	) -> Result<(), CompactionFailure> {
		if prepared.output_runs.len() != written.len()
			|| written.len() != commit.slabs_to_add.len()
			|| commit.job_id != plan.job_id
			|| commit.manifest_epoch_pre != plan.manifest_epoch_pre
			|| commit.has_lsm_gc_lifecycle_fields()
		{
			return Err(CompactionFailure::ManifestRace);
		}
		for ((planned, written), committed) in prepared
			.output_runs
			.iter()
			.zip(written)
			.zip(&commit.slabs_to_add)
		{
			if planned.slab_id != written.slab_id
				|| planned.slab_id != committed.slab_id
				|| planned.compaction_level != plan.target_level
				|| written.compaction_level != plan.target_level
				|| committed.compaction_level != plan.target_level
				|| planned.rows.len() != written.locations.len()
				|| committed.row_count != written.summary.row_count
				|| committed.size_bytes != written.summary.size_bytes
				|| committed.min_row_hlc != written.summary.min_row_hlc
				|| committed.max_row_hlc != written.summary.max_row_hlc
			{
				return Err(CompactionFailure::ManifestRace);
			}
		}
		Ok(())
	}

	pub(super) fn validate_lsm_sidecar_swap_readiness(
		&self,
		plan: &CompactionPlan,
		commit: &CompactionCommit,
		allow_sidecar_covered_inputs: bool,
	) -> Result<(), CompactionFailure> {
		if !commit.sidecar_swaps.is_empty() {
			return Err(CompactionFailure::SidecarDigestMismatch);
		}
		let manifest = recover_rwlock_read(&self.compaction_manifest, "compaction_manifest");
		for slab_id in plan.touched_slabs() {
			let Some(run) = manifest.get_run(slab_id) else {
				return Err(CompactionFailure::ManifestRace);
			};
			let has_sidecar_segments = !self
				.list_secondary_index_segments_for_source_slab(slab_id)
				.is_empty();
			if !allow_sidecar_covered_inputs
				&& (run.sidecar_coverage != crate::storage::SidecarCoverage::None
					|| has_sidecar_segments)
			{
				return Err(CompactionFailure::SidecarDigestMismatch);
			}
		}
		Ok(())
	}

	pub(super) async fn install_lsm_output_files(
		&self,
		written: &[LsmWrittenOutputRun],
	) -> std::io::Result<Vec<(LsmWrittenOutputRun, Arc<LocalSlabStore>)>> {
		let mut final_paths = Vec::with_capacity(written.len());
		for run in written {
			let final_path = slab_path(&self.config.dir, run.slab_id);
			if self.file_ops.file_len(&final_path).await?.is_some() {
				return Err(Error::new(
					ErrorKind::AlreadyExists,
					format!(
						"LSM output slab path already exists: {}",
						final_path.display()
					),
				));
			}
			final_paths.push(final_path);
		}

		let mut installed_paths = Vec::with_capacity(written.len());
		let install_result: std::io::Result<Vec<(LsmWrittenOutputRun, Arc<LocalSlabStore>)>> =
			async {
				let mut installed = Vec::with_capacity(written.len());
				for (run, final_path) in written.iter().zip(&final_paths) {
					self.file_ops.rename(&run.temp_path, final_path).await?;
					fsync_parent_dir(self.file_ops.as_ref(), final_path).await?;
					installed_paths.push(final_path.clone());
					let slab = Arc::new(
						LocalSlabStore::open(
							final_path,
							run.slab_id,
							self.file_ops.clone(),
							self.config.group_commit.clone(),
						)
						.await?,
					);
					installed.push((run.clone(), slab));
				}
				Ok(installed)
			}
			.await;

		match install_result {
			Ok(installed) => Ok(installed),
			Err(err) => {
				for path in installed_paths {
					let _ = unlink_local_file(self.file_ops.as_ref(), &path).await;
				}
				Err(err)
			}
		}
	}

	pub(super) async fn cleanup_lsm_written_outputs(&self, written: &[LsmWrittenOutputRun]) {
		for run in written {
			let _ = unlink_local_file(self.file_ops.as_ref(), &run.temp_path).await;
		}
	}

	pub(super) async fn cleanup_lsm_installed_outputs(
		&self,
		installed: &[(LsmWrittenOutputRun, Arc<LocalSlabStore>)],
	) {
		for (run, _) in installed {
			let final_path = slab_path(&self.config.dir, run.slab_id);
			let _ = unlink_local_file(self.file_ops.as_ref(), &final_path).await;
		}
	}

	pub(super) fn lsm_removed_persistent_rows(
		&self,
		prepared: &LsmPreparedOutputs,
	) -> Vec<PersistentRecordRow> {
		let mut rows = prepared
			.decisions
			.iter()
			.map(|decision| match decision {
				crate::storage::SurvivorOrDrop::Survivor(record) => record,
				crate::storage::SurvivorOrDrop::Drop { record, .. } => record,
			})
			.map(|record| PersistentRecordRow {
				tenant_id: record.tenant_id,
				key_identity: record.row_key_id,
				routing_locality: self
					.routing_locality_for_key(record.tenant_id, record.row_key_id),
				table_object_id: record.table_object_id,
				// ADR 0090: the row's durable placement travels with its
				// location; a maintenance rewrite must never downgrade it.
				placement: record.location.placement,
				partition: record.location.partition,
				location: record.location,
			})
			.collect::<Vec<_>>();
		rows.sort_by_key(|row| {
			(
				row.location.slab_id,
				row.tenant_id,
				row.key_identity,
				row.location.hlc,
				row.location.offset,
				row.location.record_row_index,
			)
		});
		rows
	}

	pub(super) fn lsm_inserted_persistent_rows(
		&self,
		prepared: &LsmPreparedOutputs,
		written: &[LsmWrittenOutputRun],
	) -> Vec<PersistentRecordRow> {
		let mut rows = Vec::new();
		for (planned, written) in prepared.output_runs.iter().zip(written) {
			for (record, location) in planned.rows.iter().zip(&written.locations) {
				rows.push(PersistentRecordRow {
					tenant_id: record.tenant_id,
					key_identity: record.row_key_id,
					routing_locality: self
						.routing_locality_for_key(record.tenant_id, record.row_key_id),
					table_object_id: record.table_object_id,
					// ADR 0090: the LSM output re-homes the bytes, not the
					// placement; carry it from the planned row into the new
					// location so the rewrite cannot downgrade it.
					placement: record.location.placement,
					partition: record.location.partition,
					location: RecordLocation {
						placement: record.location.placement,
						partition: record.location.partition,
						..*location
					},
				});
			}
		}
		rows.sort_by_key(|row| {
			(
				row.location.slab_id,
				row.tenant_id,
				row.key_identity,
				row.location.hlc,
				row.location.offset,
				row.location.record_row_index,
			)
		});
		rows
	}

	pub(super) async fn advance_active_after_lsm_outputs(
		&self,
		state: &mut ManagerState,
		max_output_slab_id: SlabId,
	) -> std::io::Result<()> {
		if state.active.slab_id() > max_output_slab_id {
			return Ok(());
		}
		let next_id = if state.next_slab_id <= max_output_slab_id {
			max_output_slab_id.saturating_add(1)
		} else {
			state.next_slab_id
		};
		let next_path = slab_path(&self.config.dir, next_id);
		let next_slab = Arc::new(
			LocalSlabStore::open(
				&next_path,
				next_id,
				self.file_ops.clone(),
				self.config.group_commit.clone(),
			)
			.await?,
		);
		let sealed_slab = std::mem::replace(&mut state.active, next_slab);
		let sealed_id = sealed_slab.slab_id();
		let sealed_size = sealed_slab.write_cursor();
		if sealed_size == 0 {
			// LSM output installation can advance past an active slab that has
			// never received a write. It is not a manifest run and therefore
			// cannot be retired by LSM GC; remove the file at the producer so
			// empty maintenance rounds do not accumulate forever.
			if let Err(error) = unlink_local_file(
				self.file_ops.as_ref(),
				&slab_path(&self.config.dir, sealed_id),
			)
			.await
			{
				// Keep the state coherent after the active replacement if the
				// producer cannot remove the orphan. The caller sees the error and
				// cleans up newly installed output files, but does not retry this
				// unlink; retaining the slab keeps it visible to this process's
				// recovery state and leaves the old one-file leak explicit.
				state.sealed.insert(sealed_id, sealed_slab);
				state.next_slab_id = next_id.saturating_add(1);
				return Err(error);
			}
		} else {
			state.sealed.insert(sealed_id, sealed_slab);
			self.record_l0_compaction_run(sealed_id, sealed_size);
		}
		state.next_slab_id = next_id.saturating_add(1);
		Ok(())
	}

	pub fn lsm_version_entries_for_plan(&self, plan: &CompactionPlan) -> Vec<VersionEntry> {
		let touched_slabs = plan.touched_slabs().into_iter().collect::<BTreeSet<_>>();
		let mut entries = Vec::new();
		for entry in self.key_index.iter() {
			let (tenant_id, row_key_id) = *entry.key();
			for location in entry.value().iter().copied() {
				if !touched_slabs.contains(&location.slab_id) {
					continue;
				}
				entries.push(VersionEntry {
					tenant_id,
					table_object_id: self.published_table_object_id(location),
					row_key_id,
					row_hlc: location.hlc,
					record_row_index: location.record_row_index,
					location,
					lifecycle: VersionLifecycle::AmbiguousTable,
				});
			}
		}
		entries.sort_by_key(VersionEntry::merge_key);
		entries
	}

	pub(super) async fn lsm_version_entries_for_plan_with_table_schemas(
		&self,
		plan: &CompactionPlan,
		table_schemas: &TableSchemaCatalog,
	) -> Result<Vec<VersionEntry>, CompactionFailure> {
		let mut entries = self.lsm_version_entries_for_plan(plan);
		for entry in &mut entries {
			entry.lifecycle = self
				.lsm_version_lifecycle_for_entry(entry, table_schemas)
				.await?;
		}
		Ok(entries)
	}

	pub(super) async fn lsm_version_lifecycle_for_entry(
		&self,
		entry: &VersionEntry,
		table_schemas: &TableSchemaCatalog,
	) -> Result<VersionLifecycle, CompactionFailure> {
		let frame = match self.read(entry.location).await {
			Ok(frame) => frame,
			Err(err) if err.kind() == ErrorKind::InvalidData => {
				return Ok(VersionLifecycle::AmbiguousTable);
			}
			Err(err) => return Err(err.into()),
		};
		let batch = match decode_record_batch(&frame) {
			Ok(batch) => batch,
			Err(_) => return Ok(VersionLifecycle::AmbiguousTable),
		};
		let Some(table_object_id) =
			storage_row_table_object_id_at(&batch, entry.location.record_row_index)?
		else {
			return Ok(VersionLifecycle::AmbiguousTable);
		};
		let Some(table) = table_schemas.latest_by_id(entry.tenant_id, table_object_id) else {
			return Ok(VersionLifecycle::AmbiguousTable);
		};
		let row_batch = materialize_record_location_batch(&batch, entry.location.record_row_index)?;
		let routing_locality = self.routing_locality_for_key(entry.tenant_id, entry.row_key_id);
		if !descriptor_matches_tiered_row(
			table,
			entry.tenant_id,
			entry.row_key_id,
			routing_locality,
			Some(table_object_id),
			&row_batch,
		)? {
			return Ok(VersionLifecycle::AmbiguousTable);
		}
		Ok(match table.row_identity_policy {
			RowIdentityPolicy::PrimaryKey(_) => VersionLifecycle::PrimaryKey,
			RowIdentityPolicy::AppendOnly => VersionLifecycle::AppendOnly,
			RowIdentityPolicy::AppendOnlyContentHash => VersionLifecycle::AppendOnlyContentHash,
		})
	}

	pub async fn prepare_lsm_compaction_outputs(
		&self,
		worker: &LsmCompactionWorker,
		plan: &CompactionPlan,
	) -> Result<LsmPreparedOutputs, CompactionFailure> {
		let entries = self.lsm_version_entries_for_plan(plan);
		let target_size = worker
			.config()
			.output_run_target_bytes(plan.target_level)
			.ok_or_else(|| {
				Error::new(
					ErrorKind::InvalidInput,
					"invalid compaction output target level",
				)
			})?;
		let decisions = apply_elision(entries.clone(), plan.retention_floor_snapshot);
		let output_run_count =
			required_output_run_count(survivor_records(&decisions), target_size)?;
		let output_slab_ids = self.reserve_lsm_output_slab_ids(output_run_count).await?;
		worker.plan_survivor_outputs(plan, entries, output_slab_ids)
	}

	pub(super) async fn prepare_lsm_compaction_outputs_with_table_schemas(
		&self,
		worker: &LsmCompactionWorker,
		plan: &CompactionPlan,
		table_schemas: &TableSchemaCatalog,
	) -> Result<LsmPreparedOutputs, CompactionFailure> {
		let entries = self
			.lsm_version_entries_for_plan_with_table_schemas(plan, table_schemas)
			.await?;
		let target_size = worker
			.config()
			.output_run_target_bytes(plan.target_level)
			.ok_or_else(|| {
				Error::new(
					ErrorKind::InvalidInput,
					"invalid compaction output target level",
				)
			})?;
		let decisions = apply_elision(entries.clone(), plan.retention_floor_snapshot);
		let output_run_count =
			required_output_run_count(survivor_records(&decisions), target_size)?;
		let output_slab_ids = self.reserve_lsm_output_slab_ids(output_run_count).await?;
		worker.plan_survivor_outputs(plan, entries, output_slab_ids)
	}

	pub async fn read_lsm_single_row_source_frames(
		&self,
		plan: &CompactionPlan,
	) -> std::io::Result<Vec<LsmSourceFrame>> {
		let entries = self.lsm_version_entries_for_plan(plan);
		let mut frames = Vec::with_capacity(entries.len());
		for entry in entries {
			let frame = self.read(entry.location).await?;
			frames.push(LsmSourceFrame { entry, frame });
		}
		Ok(frames)
	}

	pub async fn write_lsm_compaction_temp_outputs(
		&self,
		plan: &CompactionPlan,
		prepared: &LsmPreparedOutputs,
		source_frames: &[LsmSourceFrame],
	) -> std::io::Result<Vec<LsmWrittenOutputRun>> {
		let mut source_frames_by_location = HashMap::with_capacity(source_frames.len());
		let mut multirow_source_frames_by_key = HashMap::new();
		for source in source_frames {
			if let Some(entries) = storage_multirow_index_entries_from_frame(&source.frame)? {
				let Some(matching_entry) =
					lsm_multirow_index_entry_for_version(&entries, &source.entry)
				else {
					return Err(Error::new(
						ErrorKind::InvalidData,
						"LSM source frame does not contain planned multi-row survivor",
					));
				};
				validate_lsm_multirow_source_entry(&source.entry, matching_entry)?;
				let key = LsmSourceFrameKey::from(source.entry.location);
				match multirow_source_frames_by_key.entry(key) {
					std::collections::hash_map::Entry::Vacant(entry) => {
						entry.insert(LsmMultiRowSourceFrame {
							frame: source.frame.clone(),
							entries,
						});
					}
					std::collections::hash_map::Entry::Occupied(entry) => {
						let existing = entry.get();
						if existing.frame != source.frame || existing.entries != entries {
							return Err(Error::new(
								ErrorKind::InvalidData,
								"inconsistent LSM source frames for the same physical record",
							));
						}
					}
				}
			} else if source_frames_by_location
				.insert(source.entry.location, source)
				.is_some()
			{
				return Err(Error::new(
					ErrorKind::InvalidInput,
					"duplicate LSM source frame location",
				));
			}
		}

		let mut written: Vec<LsmWrittenOutputRun> = Vec::with_capacity(prepared.output_runs.len());
		for run in &prepared.output_runs {
			if run.compaction_level != plan.target_level {
				return Err(Error::new(
					ErrorKind::InvalidInput,
					"planned LSM output run target level does not match compaction plan",
				));
			}

			let temp_path = lsm_compaction_tmp_path(&self.config.dir, plan.job_id, run.slab_id);
			let _ = unlink_local_file(self.file_ops.as_ref(), &temp_path).await;
			let output = match LocalSlabStore::open(
				&temp_path,
				run.slab_id,
				self.file_ops.clone(),
				self.config.group_commit.clone(),
			)
			.await
			{
				Ok(output) => Arc::new(output),
				Err(err) => {
					for previous in &written {
						let _ =
							unlink_local_file(self.file_ops.as_ref(), &previous.temp_path).await;
					}
					return Err(err);
				}
			};
			let write_result: std::io::Result<Vec<RecordLocation>> = async {
				let mut locations = vec![None; run.rows.len()];
				let mut written_multirow_keys = HashSet::new();
				for (row_index, row) in run.rows.iter().enumerate() {
					let multirow_key = LsmSourceFrameKey::from(row.location);
					if let Some(source) = multirow_source_frames_by_key.get(&multirow_key) {
						if !written_multirow_keys.insert(multirow_key) {
							continue;
						}
						let run_rows_for_source = run
							.rows
							.iter()
							.enumerate()
							.filter(|(_, candidate)| {
								LsmSourceFrameKey::from(candidate.location) == multirow_key
							})
							.collect::<Vec<_>>();
						if run_rows_for_source.len() != source.entries.len() {
							return Err(Error::new(
								ErrorKind::Unsupported,
								"LSM temp-output writer does not yet split multi-row frames",
							));
						}
						for entry in &source.entries {
							if !run_rows_for_source.iter().any(|(_, candidate)| {
								lsm_multirow_index_entry_matches_version(entry, candidate)
							}) {
								return Err(Error::new(
									ErrorKind::Unsupported,
									"LSM temp-output writer does not yet partially rewrite multi-row frames",
								));
							}
						}
						let Some(primary) = source.entries.first() else {
							return Err(Error::new(
								ErrorKind::InvalidData,
								"LSM multi-row source frame has no index entries",
							));
						};
						let record_hlc = source
							.entries
							.iter()
							.map(|entry| entry.row_hlc)
							.max()
							.unwrap_or(primary.row_hlc);
						let base_location = output
							.append(
								row.tenant_id,
								primary.key_identity,
								record_hlc,
								&source.frame,
							)
							.await?;
						output.rollback_index_entry(
							row.tenant_id,
							primary.key_identity,
							base_location,
						);
						let mut locations_by_row_index = HashMap::new();
						for entry in &source.entries {
							let new_location = RecordLocation {
								hlc: entry.row_hlc,
								record_row_index: entry.record_row_index,
								is_tombstone: entry.is_tombstone,
								..base_location
							};
							output.index_location(row.tenant_id, entry.key_identity, new_location);
							locations_by_row_index.insert(entry.record_row_index, new_location);
						}
						for (position, candidate) in run_rows_for_source {
							let Some(new_location) = locations_by_row_index
								.get(&candidate.record_row_index)
								.copied()
							else {
								return Err(Error::new(
									ErrorKind::InvalidData,
									"LSM multi-row output location missing planned row",
								));
							};
							locations[position] = Some(new_location);
						}
						continue;
					}

					let source = source_frames_by_location
						.get(&row.location)
						.ok_or_else(|| {
							Error::new(
								ErrorKind::InvalidInput,
								"missing LSM source frame for planned survivor",
							)
						})?;
					if source.entry != *row {
						return Err(Error::new(
							ErrorKind::InvalidInput,
							"LSM source frame entry does not match planned survivor",
						));
					}
					if storage_multirow_index_entries_from_frame(&source.frame)?.is_some() {
						return Err(Error::new(
							ErrorKind::Unsupported,
							"LSM temp-output writer does not yet split multi-row frames",
						));
					}
					let base_location = output
						.append(row.tenant_id, row.row_key_id, row.row_hlc, &source.frame)
						.await?;
					output.rollback_index_entry(row.tenant_id, row.row_key_id, base_location);
					let new_location = RecordLocation {
						hlc: row.row_hlc,
						record_row_index: row.record_row_index,
						is_tombstone: row.location.is_tombstone,
						..base_location
					};
					output.index_location(row.tenant_id, row.row_key_id, new_location);
					locations[row_index] = Some(new_location);
				}
				let locations = locations
					.into_iter()
					.collect::<Option<Vec<_>>>()
					.ok_or_else(|| {
						Error::new(
							ErrorKind::InvalidData,
							"LSM output writer did not materialize every planned survivor",
						)
					})?;
				output.fsync().await?;
				fsync_parent_dir(self.file_ops.as_ref(), &temp_path).await?;
				Ok(locations)
			}
			.await;

			let locations = match write_result {
				Ok(locations) => locations,
				Err(err) => {
					let _ = unlink_local_file(self.file_ops.as_ref(), &temp_path).await;
					for previous in &written {
						let _ =
							unlink_local_file(self.file_ops.as_ref(), &previous.temp_path).await;
					}
					return Err(err);
				}
			};
			let mut summary = run.summary(plan.manifest_epoch_pre);
			summary.size_bytes = ByteCount::new(output.write_cursor());
			written.push(LsmWrittenOutputRun {
				slab_id: run.slab_id,
				compaction_level: run.compaction_level,
				temp_path,
				summary,
				locations,
			});
		}
		Ok(written)
	}

	pub async fn sweep_lsm_delete_pending_once(
		&self,
		gc_worker: &LsmGcWorker,
	) -> std::io::Result<Vec<SlabId>> {
		let mut delete_pending_transition = None;
		{
			let mut manifest =
				recover_rwlock_write(&self.compaction_manifest, "compaction_manifest");
			let outcome = gc_worker
				.transition_retired_to_delete_pending_if(&mut manifest, |slab_id| {
					self.record_rows_for_slab(slab_id).is_empty()
				});
			if !outcome.marked_delete_pending.is_empty() {
				delete_pending_transition = Some(manifest.snapshot());
			}
		}
		if let Some(snapshot) = delete_pending_transition {
			self.mirror_persistent_update(
				PersistentIndexUpdate {
					put_lsm_runs: snapshot.runs().to_vec(),
					..Default::default()
				},
				"lsm-gc-mark-delete-pending",
			)
			.await;
		}

		let mut candidates = self
			.compaction_manifest_snapshot()
			.delete_pending_runs()
			.map(|run| run.slab_id)
			.collect::<Vec<_>>();
		candidates.sort_unstable();
		let mut deleted = Vec::new();
		for slab_id in candidates {
			if self.is_compaction_slab_quarantined(slab_id) {
				continue;
			}
			if self.slab_pins.pin_count(slab_id) != 0 {
				continue;
			}
			if !self.can_retire_local_slab(slab_id).await? {
				continue;
			}
			for info in self.list_secondary_index_segments_for_source_slab(slab_id) {
				self.retire_secondary_index_segment_for_replaced_slab(&info)
					.await?;
			}
			{
				let mut state = self.state.lock().await;
				let _old = state.sealed.remove(&slab_id);
				let _old = state.rehydrated.remove(&slab_id);
				state.hot_read_counts.remove(&slab_id);
				state.rehydrating.remove(&slab_id);
			}
			let path = slab_path(&self.config.dir, slab_id);
			unlink_local_file(self.file_ops.as_ref(), &path).await?;
			let snapshot = {
				let mut manifest =
					recover_rwlock_write(&self.compaction_manifest, "compaction_manifest");
				gc_worker.complete_delete(&mut manifest, slab_id);
				manifest.snapshot()
			};
			refresh_lsm_level_metrics(&snapshot);
			self.mirror_persistent_update(
				PersistentIndexUpdate {
					delete_lsm_run_ids: vec![slab_id],
					..Default::default()
				},
				"lsm-gc-delete-run",
			)
			.await;
			deleted.push(slab_id);
		}
		Ok(deleted)
	}

	pub async fn sweep_lsm_delete_pending_once_via_control_ring(
		&self,
		gc_worker: &LsmGcWorker,
		committer: &dyn TierCommitter,
	) -> std::io::Result<Vec<SlabId>> {
		let snapshot = self.compaction_manifest_snapshot();
		let sweep_plan = gc_worker.plan_sweep(&snapshot);
		let mut to_mark_delete_pending = Vec::new();
		for slab_id in sweep_plan.retired_unpinned_slabs {
			if !self.record_rows_for_slab(slab_id).is_empty() {
				continue;
			}
			if self.slab_pins.mark_delete_pending(slab_id) {
				to_mark_delete_pending.push(slab_id);
			}
		}

		if !to_mark_delete_pending.is_empty() {
			let commit = CompactionCommit::gc_mark_delete_pending(
				Self::lsm_gc_job_id(snapshot.manifest_epoch, 0xD1, &to_mark_delete_pending),
				snapshot.manifest_epoch,
				to_mark_delete_pending,
			);
			committer.commit_lsm_compaction(commit).await?;
			let snapshot = self.compaction_manifest_snapshot();
			gc_worker.observe_manifest_snapshot(&snapshot);
			self.mirror_persistent_update(
				PersistentIndexUpdate {
					put_lsm_runs: snapshot.runs().to_vec(),
					..Default::default()
				},
				"lsm-gc-mark-delete-pending-control-ring",
			)
			.await;
		}

		let mut candidates = self
			.compaction_manifest_snapshot()
			.delete_pending_runs()
			.map(|run| run.slab_id)
			.collect::<Vec<_>>();
		candidates.sort_unstable();
		let mut deleted = Vec::new();
		for slab_id in candidates {
			if self.is_compaction_slab_quarantined(slab_id) {
				continue;
			}
			if self.slab_pins.pin_count(slab_id) != 0 {
				continue;
			}
			if !self.can_retire_local_slab(slab_id).await? {
				continue;
			}
			for info in self.list_secondary_index_segments_for_source_slab(slab_id) {
				self.retire_secondary_index_segment_for_replaced_slab(&info)
					.await?;
			}
			{
				let mut state = self.state.lock().await;
				let _old = state.sealed.remove(&slab_id);
				let _old = state.rehydrated.remove(&slab_id);
				state.hot_read_counts.remove(&slab_id);
				state.rehydrating.remove(&slab_id);
			}
			let path = slab_path(&self.config.dir, slab_id);
			unlink_local_file(self.file_ops.as_ref(), &path).await?;
			deleted.push(slab_id);
		}

		if !deleted.is_empty() {
			let snapshot = self.compaction_manifest_snapshot();
			let commit = CompactionCommit::gc_remove(
				Self::lsm_gc_job_id(snapshot.manifest_epoch, 0xD2, &deleted),
				snapshot.manifest_epoch,
				deleted.clone(),
			);
			committer.commit_lsm_compaction(commit).await?;
			let snapshot = self.compaction_manifest_snapshot();
			gc_worker.observe_manifest_snapshot(&snapshot);
			self.mirror_persistent_update(
				PersistentIndexUpdate {
					delete_lsm_run_ids: deleted.clone(),
					..Default::default()
				},
				"lsm-gc-delete-run-control-ring",
			)
			.await;
		}
		Ok(deleted)
	}

	pub(super) fn lsm_gc_job_id(
		manifest_epoch: ManifestEpoch,
		discriminator: u64,
		slab_ids: &[SlabId],
	) -> CompactionJobId {
		let mut value = 0xA55A_0550_0000_0000_u64 ^ discriminator ^ manifest_epoch.get();
		for slab_id in slab_ids {
			value = value.rotate_left(7) ^ slab_id.get();
		}
		CompactionJobId::new(value)
	}

	pub(super) fn pin_lsm_read_location(
		&self,
		location: RecordLocation,
	) -> std::io::Result<SlabPinGuard> {
		self.slab_pins.acquire(&[location.slab_id]).map_err(|_| {
			Error::new(
				ErrorKind::NotFound,
				format!("slab {} is delete-pending for LSM GC", location.slab_id),
			)
		})
	}
}
