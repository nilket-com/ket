use super::*;

const MAINTENANCE_STAGE_INSTALL_ATTEMPTS: usize = 3;

impl SlabManager {
	pub(super) fn enter_maintenance_publication_fail_stop(
		&self,
		publication_gate: OwnedRwLockWriteGuard<()>,
	) {
		self.maintenance_publication_restart_required
			.store(true, Ordering::Release);
		let mut held = self
			.maintenance_publication_fail_stop
			.lock()
			.expect("maintenance publication fail-stop mutex poisoned");
		if held.is_none() {
			*held = Some(publication_gate);
		}
	}

	/// Strict D2 persistence for a maintenance rewrite. Unlike ordinary mirror
	/// updates, this cannot fall back to rebuilding from the live maps: the
	/// replacement is still staged and therefore deliberately invisible to that
	/// rebuild. The caller must install the staged file and perform D3 only after
	/// this transaction succeeds.
	pub(super) async fn persist_maintenance_publication(
		&self,
		update: PersistentIndexUpdate,
		publication: &IndexPublication,
	) -> std::io::Result<()> {
		let index = self.persistent_index.as_ref().ok_or_else(|| {
			Error::new(
				ErrorKind::Other,
				"maintenance publication requires the local persistent index",
			)
		})?;
		index
			.apply_with_publication_guard(update, Some(publication.gate.clone()))
			.await
	}

	pub(super) fn maintenance_manifest_preview(
		&self,
		old_slab_id: SlabId,
		replacement: Option<LsmRunSummary>,
	) -> std::io::Result<Vec<LsmRunSummary>> {
		let manifest = recover_rwlock_read(&self.compaction_manifest, "compaction_manifest");
		let mut preview = manifest.clone();
		drop(manifest);
		preview.apply_maintenance_rewrite(old_slab_id, replacement)?;
		Ok(preview.snapshot().runs().to_vec())
	}

	pub(super) fn apply_maintenance_manifest_publication(
		&self,
		old_slab_id: SlabId,
		replacement: Option<LsmRunSummary>,
	) -> std::io::Result<ManifestSnapshot> {
		let mut manifest = recover_rwlock_write(&self.compaction_manifest, "compaction_manifest");
		manifest.apply_maintenance_rewrite(old_slab_id, replacement)?;
		let snapshot = manifest.snapshot();
		drop(manifest);
		refresh_lsm_level_metrics(&snapshot);
		Ok(snapshot)
	}
}

/// A replacement produced by maintenance remains outside normal slab
/// discovery until its record and manifest publication is durable. The two
/// ids make the publication identity explicit and keep unrelated staged
/// outputs from sharing a name.
pub(super) fn maintenance_staged_slab_path(
	dir: &Path,
	old_slab_id: SlabId,
	replacement_slab_id: SlabId,
) -> PathBuf {
	dir.join(format!(
		"maintenance-staged-{:016x}-{:016x}.wnd",
		old_slab_id, replacement_slab_id
	))
}

fn parse_maintenance_staged_slab_filename(name: &str) -> Option<(SlabId, SlabId)> {
	let body = name
		.strip_prefix("maintenance-staged-")?
		.strip_suffix(".wnd")?;
	let (old, replacement) = body.split_once('-')?;
	if old.len() != 16 || replacement.len() != 16 {
		return None;
	}
	Some((
		SlabId::new(u64::from_str_radix(old, 16).ok()?),
		SlabId::new(u64::from_str_radix(replacement, 16).ok()?),
	))
}

fn staged_maintenance_publication_is_committed(
	runs: &[LsmRunSummary],
	old_slab_id: SlabId,
	replacement_slab_id: SlabId,
) -> bool {
	let old_committed = runs.iter().any(|run| {
		run.slab_id == old_slab_id
			&& matches!(
				&run.state,
				SlabState::Retired { successors }
					if successors.as_slice() == [replacement_slab_id]
			)
	});
	let replacement_live = runs
		.iter()
		.any(|run| run.slab_id == replacement_slab_id && run.state.is_live());
	old_committed && replacement_live
}

/// The only S1 installer. Both startup recovery and a live D1′ failure call
/// this helper, so they share the same retry, idempotence, and directory-fsync
/// semantics.
async fn install_committed_staged_maintenance_publication(
	file_ops: &dyn AsyncFileOps,
	staged_path: &Path,
	final_path: &Path,
	old_slab_id: SlabId,
	replacement_slab_id: SlabId,
) -> std::io::Result<()> {
	let mut last_error = None;
	for attempt in 0..MAINTENANCE_STAGE_INSTALL_ATTEMPTS {
		match file_ops.rename(staged_path, final_path).await {
			Ok(()) => return fsync_parent_dir(file_ops, final_path).await,
			Err(err) => {
				// A successful rename followed by an interrupted fsync can be observed
				// as a retry whose source is gone and destination is present. Treat it
				// as S2 and finish the directory barrier.
				if file_ops.file_len(staged_path).await?.is_none()
					&& file_ops.file_len(final_path).await?.is_some()
				{
					return fsync_parent_dir(file_ops, final_path).await;
				}
				last_error = Some(err);
				if attempt + 1 < MAINTENANCE_STAGE_INSTALL_ATTEMPTS {
					tokio::time::sleep(Duration::from_millis(10)).await;
				}
			}
		}
	}
	let err = last_error.expect("non-zero maintenance install attempts leave an error");
	Err(Error::new(
		err.kind(),
		format!(
			"maintenance publication {old_slab_id}->{replacement_slab_id} is committed but staged installation exhausted its retry policy; restart-required recovery must reconcile S1: {err}"
		),
	))
}

/// Resolve staged maintenance files before normal discovery can drain a final
/// slab or allocate a fresh id. A matching committed run pair is S1 and rolls
/// forward by rename; every other staged file is S0 and only that staged file
/// is reaped. The old backing remains untouched in both cases.
pub(super) async fn reconcile_staged_maintenance_publications(
	file_ops: &dyn AsyncFileOps,
	dir: &Path,
	runs: Option<&[LsmRunSummary]>,
	only_publication: Option<(SlabId, SlabId)>,
) -> std::io::Result<()> {
	for entry in file_ops.read_dir(dir).await? {
		if !entry.is_file {
			continue;
		}
		let Some(name) = entry.file_name.to_str() else {
			continue;
		};
		let Some((old_slab_id, replacement_slab_id)) = parse_maintenance_staged_slab_filename(name)
		else {
			continue;
		};
		if only_publication
			.is_some_and(|publication| publication != (old_slab_id, replacement_slab_id))
		{
			continue;
		}
		let staged_path = dir.join(name);
		let final_path = slab_path(dir, replacement_slab_id);
		let committed = runs.is_some_and(|runs| {
			staged_maintenance_publication_is_committed(runs, old_slab_id, replacement_slab_id)
		});
		if committed {
			if file_ops.file_len(&final_path).await?.is_none() {
				install_committed_staged_maintenance_publication(
					file_ops,
					&staged_path,
					&final_path,
					old_slab_id,
					replacement_slab_id,
				)
				.await?;
			} else {
				// A rename may have completed before its directory fsync or before a
				// crash. The final file is the committed S2 object; discard only its
				// now-redundant staged twin.
				unlink_local_file(file_ops, &staged_path).await?;
			}
		} else {
			// Never infer data absence from this classification. D0 has retained
			// the old backing, and a redb rebuild has deliberately removed D2 from
			// the reconstructed world, so only this invisible staged file is safe
			// to discard before the reclaim debt re-drives the rewrite.
			unlink_local_file(file_ops, &staged_path).await?;
		}
	}
	Ok(())
}
