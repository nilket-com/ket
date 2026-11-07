//! The local half of T1 for a GC removal (ADR 0697 §6.1): the claim authorizes
//! the disposal, Complete acknowledges it, and only Complete's Applied authorizes
//! the purge. Each stage runs as its own task, so whoever asked for it can go away
//! without releasing the exclusion and authority its filesystem work depends on.

use super::*;
use crate::consensus::slab_ops::{GcRemove, OpKey, SlabRejection};
use crate::storage::index::{
	GcFileIdentity, LoadedOp, LsmInputAuthority, LsmInstallIntent, OpOwnership, OpProgress,
	OpProgressUpdate, OpTicket, RejectedClaim,
};
use crate::storage::sweeper::VerifiedReceipt;

pub(super) fn gc_quarantine_path(dir: &Path, slab_id: SlabId, job_id: CompactionJobId) -> PathBuf {
	dir.join(format!(
		"gc-quarantine-{:016x}-{:016x}.wnd",
		slab_id.get(),
		job_id.get()
	))
}

/// This entry still names the inode and length this workflow recorded.
pub(super) fn gc_require_recorded_inode(path: &Path, file: &GcFileIdentity) -> std::io::Result<()> {
	use std::os::unix::fs::MetadataExt;
	let same = std::fs::symlink_metadata(path).is_ok_and(|metadata| {
		metadata.file_type().is_file()
			&& (metadata.dev(), metadata.ino()) == (file.dev, file.ino)
			&& metadata.len() == file.length
	});
	if !same {
		return Err(Error::new(
			ErrorKind::WouldBlock,
			"a GC file is missing or is not the recorded file; it remains fenced",
		));
	}
	Ok(())
}

/// This entry's bytes are the ones recorded, read from the inode the recheck under
/// the lease will compare.
pub(super) async fn gc_require_recorded_content(
	path: &Path,
	file: &GcFileIdentity,
) -> std::io::Result<()> {
	use std::os::unix::fs::MetadataExt;
	let (path, file) = (path.to_path_buf(), file.clone());
	tokio::task::spawn_blocking(move || {
		let mismatch = || {
			Error::new(
				ErrorKind::WouldBlock,
				"a GC file is missing or is not the recorded file; it remains fenced",
			)
		};
		let mut opened = std::fs::File::open(&path).map_err(|_| mismatch())?;
		let metadata = opened.metadata()?;
		if (metadata.dev(), metadata.ino()) != (file.dev, file.ino) {
			return Err(mismatch());
		}
		let mut hasher = blake3::Hasher::new();
		let length = std::io::copy(&mut opened, &mut hasher)?;
		if length != file.length || *hasher.finalize().as_bytes() != file.digest {
			return Err(mismatch());
		}
		Ok(())
	})
	.await
	.map_err(Error::other)?
}

/// Whether anything is at `path`. Only NotFound is absence: a lookup that failed says
/// nothing about what is there.
fn gc_path_present(path: &Path) -> std::io::Result<bool> {
	match std::fs::symlink_metadata(path) {
		Ok(_) => Ok(true),
		Err(error) if error.kind() == ErrorKind::NotFound => Ok(false),
		Err(error) => Err(error),
	}
}

/// Whether `path` holds the recorded file. Anything else there is not this workflow's.
fn gc_recorded_file_present(path: &Path, file: &GcFileIdentity) -> std::io::Result<bool> {
	if gc_path_present(path)? {
		gc_require_recorded_inode(path, file)?;
		return Ok(true);
	}
	Ok(false)
}

impl SlabManager {
	/// Content, revision and inode of the file a removal will dispose of. Read
	/// without the write gate: the later exclusive check ties this to the same
	/// inode and revision, so the bytes cannot have changed in between. A store
	/// in a ring must find its file; outside one, an earlier local release may
	/// have taken it.
	pub(super) async fn gc_read_original_identity(
		&self,
		slab_id: SlabId,
	) -> std::io::Result<Option<GcFileIdentity>> {
		use std::os::unix::fs::MetadataExt;
		let handle = {
			let state = self.state.lock().await;
			state
				.sealed
				.get(&slab_id)
				.map(|slab| slab.value().clone())
				.or_else(|| {
					state
						.rehydrated
						.get(&slab_id)
						.map(|entry| entry.slab.clone())
				})
		};
		let Some(handle) = handle else {
			if self.control_node_bound() {
				return Err(Error::new(
					ErrorKind::WouldBlock,
					"a claimed slab has no local backing; it remains fenced",
				));
			}
			return Ok(None);
		};
		let (dev, ino) = handle.opened_file_identity()?;
		let revision = self
			.slab_publication_revisions
			.get(&slab_id)
			.map_or(0, |revision| *revision);
		let path = slab_path(&self.config.dir, slab_id);
		let (length, digest) = tokio::task::spawn_blocking(move || {
			let mut file = std::fs::File::open(&path)?;
			let metadata = file.metadata()?;
			if (metadata.dev(), metadata.ino()) != (dev, ino) {
				return Err(Error::new(
					ErrorKind::InvalidData,
					"GC slab path does not name its owned file",
				));
			}
			let mut hasher = blake3::Hasher::new();
			let length = std::io::copy(&mut file, &mut hasher)?;
			Ok::<_, Error>((length, *hasher.finalize().as_bytes()))
		})
		.await
		.map_err(Error::other)??;
		Ok(Some(GcFileIdentity {
			length,
			digest,
			revision,
			dev,
			ino,
		}))
	}

	/// A mark decided Applied: P0, the exact durable DeletePending postimage, built
	/// from the commit and its recorded run rather than from observer delivery, then the
	/// mark's handoff. Run as its own task, it owns its exclusion and authority through
	/// every write. Pins are rebuilt here too: a reopened store has none, and a pinned
	/// run refuses. A failed step retries from `MarkPublished`, its own postimage.
	pub(super) async fn gc_complete_mark(
		self: Arc<Self>,
		mark: OpTicket,
		removal: OpKey,
		intent: LsmInstallIntent,
		pre_run: crate::storage::LsmRunSummary,
		ownership: OpOwnership,
	) -> std::io::Result<(crate::storage::manifest::ManifestSnapshot, OpOwnership)> {
		use crate::storage::index::LsmInputAuthority;
		let index = self.require_op_index()?;
		let slab_id = pre_run.slab_id;
		let crate::consensus::slab_ops::SlabCommand::Compaction(commit) = mark.command() else {
			return Err(Error::other("a GC mark is a compaction command"));
		};
		let _claim = self.acquire_slab_rewrite_exclusive_claim(slab_id).await;
		let lease = match self.canonical_tier_source.get() {
			Some(source) => Some(source.read().await?),
			None => None,
		};
		if lease.as_ref().is_some_and(|lease| {
			!lease.op_applied_for_current_owner(mark.key(), mark.digest())
				|| !lease.claim_held_by_current_owner(commit.owner.slab(slab_id), commit.job_id)
		}) {
			return Err(Error::new(
				ErrorKind::WouldBlock,
				"a mark is not applied and claimed for the current owner; it remains fenced",
			));
		}
		let _maintenance = self
			.maintenance_publication_gate
			.clone()
			.write_owned()
			.await;
		let tier_lock = self.tier_publication_lock(slab_id);
		let _tier_guard = tier_lock.lock().await;
		self.gc_require_row_stands(
			mark.key(),
			mark.digest(),
			|at| {
				matches!(
					at,
					Some(OpProgress::MarkPrepared { .. } | OpProgress::MarkPublished { .. })
				)
			},
			Some(&intent),
		)
		.await?;
		if self.compaction_manifest_snapshot().run(slab_id) == Some(&pre_run) {
			self.apply_lsm_manifest_compaction_commit(commit)
				.map_err(|error| Error::other(error.to_string()))?;
		}
		let snapshot = self.compaction_manifest_snapshot();
		let actual = snapshot.run(slab_id).cloned();
		let exact = actual.as_ref().is_some_and(|run| {
			let mut expected_after = pre_run.clone();
			expected_after.state = SlabState::DeletePending;
			expected_after.manifest_epoch = commit.manifest_epoch_pre.saturating_add(1);
			*run == expected_after
		});
		let current = self.lsm_input_authority_locked(&[slab_id]).await.ok();
		let pins = self.slab_pins.pin_count(slab_id);
		let frames = self.counter_slab_has_pending_frames(slab_id);
		// The durable rows, as disposal reads them: a reopen rebuilds the memory index
		// from slab files, so a retired slab's old record is back there and means nothing.
		let durable = index.load_snapshot().await?;
		let rows = durable
			.record_rows
			.iter()
			.chain(&durable.tiered_records)
			.filter(|row| row.location.slab_id == slab_id)
			.count();
		if !exact
			|| current.is_none()
			|| pins != 0
			|| !self.slab_pins.mark_delete_pending(slab_id)
			|| frames || rows != 0
		{
			return Err(Error::new(
				ErrorKind::WouldBlock,
				format!(
					"committed LSM GC mark changed; intent remains fenced (exact postimage {exact}, input authority {}, pins {pins}, counter frames {frames}, live rows {rows})",
					current.is_some()
				),
			));
		}
		let current = current.unwrap_or_default();
		let changed = || {
			Error::new(
				ErrorKind::WouldBlock,
				"committed LSM GC mark changed; intent remains fenced",
			)
		};
		let left = match index.op_progress(mark.key().op_seq).await? {
			Some(OpProgress::MarkPublished { left }) => {
				// Its postimage is durable. Only this workflow's own bump may separate
				// the authority it recorded from what is here now.
				if LsmInputAuthority::all_unchanged_since(&current, &left) {
					if left.first().map(|input| input.opened)
						== current.first().map(|input| input.opened)
					{
						left
					} else {
						current
					}
				} else if current.len() == 1
					&& left.len() == 1
					&& current[0].opened == left[0].opened
					&& current[0].tiered == left[0].tiered
					&& current[0].revision.checked_add(1) == Some(left[0].revision)
				{
					// The commit landed and its answer was lost: the bump is still owed.
					self.bump_slab_publication_revision(slab_id)?;
					left
				} else {
					return Err(changed());
				}
			}
			_ => {
				if !LsmInputAuthority::all_unchanged_since(&current, &intent.inputs) {
					return Err(changed());
				}
				let mut left = current;
				for input in &mut left {
					input.revision = input
						.revision
						.checked_add(1)
						.ok_or_else(|| Error::other("slab publication revision exhausted"))?;
				}
				let publication = self.begin_index_publication(vec![slab_id]).await?;
				index
					.apply_with_publication_guard(
						PersistentIndexUpdate {
							put_lsm_runs: vec![actual.unwrap_or_else(|| pre_run.clone())],
							op_progress: Some(OpProgressUpdate {
								key: mark.key(),
								digest: mark.digest(),
								progress: OpProgress::MarkPublished { left: left.clone() },
							}),
							..Default::default()
						},
						Some(publication.gate.clone()),
					)
					.await?;
				self.bump_slab_publication_revision(slab_id)?;
				publication.complete();
				left
			}
		};
		index.hand_off_gc_mark(&mark, intent, removal, left).await?;
		drop(lease);
		Ok((snapshot, ownership))
	}

	/// Claim-stage work, P0 to P3: everything below is authorized by the live
	/// qualified claim, which the canonical lease keeps from changing until the
	/// run is durably gone. Nothing here awaits the ring.
	pub(super) async fn gc_dispose_run(
		self: Arc<Self>,
		remove: GcRemove,
		key: OpKey,
		digest: [u8; 32],
		expected: Vec<LsmInputAuthority>,
		expected_run: crate::storage::LsmRunSummary,
		intent: LsmInstallIntent,
		file: Option<GcFileIdentity>,
		ownership: OpOwnership,
	) -> std::io::Result<(crate::storage::manifest::ManifestSnapshot, OpOwnership)> {
		let slab_id = remove.slab.local_id();
		let index = self.require_op_index()?;
		#[cfg(test)]
		self.pause_gc_disposal_entry_for_test().await;
		let job_id = remove.job_id;
		let snapshot = {
			let _claim = self.acquire_slab_rewrite_exclusive_claim(slab_id).await;
			let lease = match self.canonical_tier_source.get() {
				Some(source) => Some(source.read().await?),
				None => None,
			};
			if lease
				.as_ref()
				.is_some_and(|lease| !lease.claim_held_by_current_owner(remove.slab, remove.job_id))
			{
				return Err(Error::new(
					ErrorKind::WouldBlock,
					"this job does not hold the claim for the current owner; it remains fenced",
				));
			}
			let _maintenance = self
				.maintenance_publication_gate
				.clone()
				.write_owned()
				.await;
			let tier_lock = self.tier_publication_lock(slab_id);
			let _tier_guard = tier_lock.lock().await;
			self.gc_require_row_stands(key, digest, |at| at.is_none(), Some(&intent))
				.await?;
			if self
				.lsm_input_authority_locked(&[slab_id])
				.await
				.ok()
				.as_ref() != Some(&expected)
				|| self.compaction_manifest_snapshot().run(slab_id) != Some(&expected_run)
				|| crate::consensus::slab_ops::RunIdentity::of(&expected_run) != remove.expected_run
				|| self.slab_pins.pin_count(slab_id) != 0
				|| self.counter_slab_has_pending_frames(slab_id)
				|| !self.slab_pins.mark_delete_pending(slab_id)
			{
				return Err(Error::new(
					ErrorKind::WouldBlock,
					"committed LSM GC authority changed; intent remains fenced",
				));
			}
			self.gc_require_no_durable_references_locked(slab_id)
				.await?;
			if let Some(file) = &file {
				self.gc_require_same_file(slab_id, file).await?;
				index
					.advance_op_progress(
						OpProgressUpdate {
							key,
							digest,
							progress: OpProgress::QuarantineRecorded(file.clone()),
						},
						None,
					)
					.await?;
				let original = slab_path(&self.config.dir, slab_id);
				let quarantine = gc_quarantine_path(&self.config.dir, slab_id, remove.job_id);
				self.file_ops
					.link_no_replace(&original, &quarantine)
					.await?;
				fsync_parent_dir(self.file_ops.as_ref(), &quarantine).await?;
			}
			self.gc_drop_run_locked(&remove, key, digest, intent, file, true)
				.await?;
			drop(lease);
			self.compaction_manifest_snapshot()
		};
		// The transaction above is durable: the scope this workflow held is done.
		// Released outside the gates, so no scope waiter can invert them.
		self.clear_registered_lsm_intent(job_id).await;
		self.tier_authority.notify_publication_change();
		Ok((snapshot, ownership))
	}

	/// Whether live rows name this run. A reopen under an unresolved intent can leave memory
	/// naming it after its rows moved: memory only says when to ask, and the durable rows
	/// answer.
	pub(super) async fn gc_rows_still_name(&self, slab_id: SlabId) -> std::io::Result<bool> {
		if self.record_rows_for_slab(slab_id)?.is_empty() {
			return Ok(false);
		}
		let durable = self.require_op_index()?.load_snapshot().await?;
		Ok(durable
			.record_rows
			.iter()
			.chain(&durable.tiered_records)
			.any(|row| row.location.slab_id == slab_id))
	}

	/// Holding its ownership and exclusions: this stage's row and linked intent are exactly
	/// what it was given, before it does anything a commit could not take back. The commit
	/// compares them again; that protects the row, not the files before it.
	pub(super) async fn gc_require_row_stands(
		&self,
		key: OpKey,
		digest: [u8; 32],
		phase: impl Fn(Option<&OpProgress>) -> bool,
		intent: Option<&LsmInstallIntent>,
	) -> std::io::Result<()> {
		let index = self.require_op_index()?;
		let stands = index
			.load_op_intents()
			.await?
			.iter()
			.find(|op| op.key() == key)
			.is_some_and(|op| op.digest() == digest && phase(op.progress()));
		let linked = match intent {
			Some(intent) => index
				.load_snapshot()
				.await?
				.lsm_install_intents
				.iter()
				.any(|durable| durable == intent),
			None => true,
		};
		if !stands || !linked {
			return Err(Error::new(
				ErrorKind::WouldBlock,
				format!(
					"op {} is not at the phase or linked intent this stage was given (row {stands}, intent {linked}); it remains fenced",
					key.op_seq
				),
			));
		}
		Ok(())
	}

	/// Under exclusion: no durable row or sidecar still names this run. A reopen rebuilds
	/// the memory index from slab files, so memory says nothing about a retired slab.
	async fn gc_require_no_durable_references_locked(
		&self,
		slab_id: SlabId,
	) -> std::io::Result<()> {
		let index = self.require_op_index()?;
		let durable = index.load_snapshot().await?;
		if durable
			.record_rows
			.iter()
			.chain(&durable.tiered_records)
			.any(|row| row.location.slab_id == slab_id)
		{
			return Err(Error::new(
				ErrorKind::WouldBlock,
				"committed LSM GC has live record references",
			));
		}
		for info in self.list_secondary_index_segments_for_source_slab(slab_id) {
			self.retire_secondary_index_segment_for_replaced_slab_locked(&info)
				.await?;
		}
		let durable = index.load_snapshot().await?;
		if durable
			.secondary_index_segments
			.iter()
			.any(|row| row.key.source_slab_id == slab_id)
		{
			return Err(Error::new(
				ErrorKind::WouldBlock,
				"committed LSM GC has sidecar references",
			));
		}
		Ok(())
	}

	/// Under exclusion and the claim: P2 and P3. The original goes, if it is still here,
	/// and one transaction drops the run and records that.
	async fn gc_drop_run_locked(
		&self,
		remove: &GcRemove,
		key: OpKey,
		digest: [u8; 32],
		intent: LsmInstallIntent,
		file: Option<GcFileIdentity>,
		original_present: bool,
	) -> std::io::Result<()> {
		let slab_id = remove.slab.local_id();
		let index = self.require_op_index()?;
		self.bump_slab_publication_revision(slab_id)?;
		if self.slab_pins.pin_count(slab_id) != 0 || self.counter_slab_has_pending_frames(slab_id) {
			return Err(Error::new(
				ErrorKind::WouldBlock,
				"LSM GC eligibility changed before unlink",
			));
		}
		if original_present {
			self.gc_unlink_owned_slab_file(slab_id).await?;
		}
		self.redirects.remove_touching(&self.tier_budget, slab_id);
		self.record_disposal_evidence(slab_id).await;
		let publication = self.begin_index_publication(vec![slab_id]).await?;
		// Removing the run advances the manifest epoch, and no row will carry that: the floor
		// does, in the transaction that drops the run, so memory never shows an epoch a reopen
		// would not recover. A resume after it finds `RunDropped` and never comes here again; one
		// before it finds the run and the progress as they were. The maintenance gate this
		// workflow holds excludes every other writer of the epoch but an observed commit.
		let transition = self.lsm_manifest_transition_gate.write().await;
		let pre = self.compaction_manifest_snapshot().manifest_epoch;
		index
			.apply_with_publication_guard(
				PersistentIndexUpdate {
					delete_lsm_run_ids: vec![slab_id],
					raise_lsm_manifest_epoch_floor: Some(
						CompactionManifest::epoch_after_transition(pre),
					),
					op_progress: Some(OpProgressUpdate {
						key,
						digest,
						progress: OpProgress::RunDropped(file),
					}),
					complete_lsm_intent: Some(intent),
					..Default::default()
				},
				Some(publication.gate.clone()),
			)
			.await?;
		#[cfg(test)]
		self.pause_gc_after_run_dropped_for_test().await;
		// Memory follows durability: a commit that fails or is uncertain leaves
		// the run in the manifest, and the intent still fencing its scope.
		self.apply_lsm_manifest_compaction_commit(&CompactionCommit::gc_remove(
			remove.slab.owner(),
			remove.job_id,
			pre,
			vec![slab_id],
		))
		.map_err(|error| Error::other(error.to_string()))?;
		drop(transition);
		publication.complete();
		Ok(())
	}

	/// §6.1, the row at `QuarantineRecorded`: the disposal it began, from whichever of its
	/// files is left. Both exact: unlink, then drop the run. Original only: link first.
	/// Entry only: drop the run. Anything else, or a claim no longer held, fences. Its
	/// revision was this open's or another's, so the recorded inode and content decide.
	pub(super) async fn gc_resume_disposal(
		self: Arc<Self>,
		remove: GcRemove,
		key: OpKey,
		digest: [u8; 32],
		intent: LsmInstallIntent,
		file: GcFileIdentity,
		ownership: OpOwnership,
	) -> std::io::Result<OpOwnership> {
		let slab_id = remove.slab.local_id();
		let original = slab_path(&self.config.dir, slab_id);
		let entry = gc_quarantine_path(&self.config.dir, slab_id, remove.job_id);
		// Content first and outside the lease; the same inode is rechecked under it.
		let readable = if gc_recorded_file_present(&entry, &file)? {
			&entry
		} else {
			&original
		};
		gc_require_recorded_content(readable, &file).await?;
		{
			let _claim = self.acquire_slab_rewrite_exclusive_claim(slab_id).await;
			let lease = match self.canonical_tier_source.get() {
				Some(source) => Some(source.read().await?),
				None => None,
			};
			if lease
				.as_ref()
				.is_some_and(|lease| !lease.claim_held_by_current_owner(remove.slab, remove.job_id))
			{
				return Err(Error::new(
					ErrorKind::WouldBlock,
					"this job does not hold the claim for the current owner; it remains fenced",
				));
			}
			let _maintenance = self
				.maintenance_publication_gate
				.clone()
				.write_owned()
				.await;
			let tier_lock = self.tier_publication_lock(slab_id);
			let _tier_guard = tier_lock.lock().await;
			let recorded = OpProgress::QuarantineRecorded(file.clone());
			self.gc_require_row_stands(key, digest, |at| at == Some(&recorded), Some(&intent))
				.await?;
			let snapshot = self.compaction_manifest_snapshot();
			if !snapshot.run(slab_id).is_some_and(|run| {
				run.state == SlabState::DeletePending
					&& crate::consensus::slab_ops::RunIdentity::of(run) == remove.expected_run
			}) || self.slab_pins.pin_count(slab_id) != 0
				|| self.counter_slab_has_pending_frames(slab_id)
				|| !self.slab_pins.mark_delete_pending(slab_id)
			{
				return Err(Error::new(
					ErrorKind::WouldBlock,
					"a recorded GC disposal's run changed; it remains fenced",
				));
			}
			self.gc_require_no_durable_references_locked(slab_id)
				.await?;
			let original_present = gc_recorded_file_present(&original, &file)?;
			if !gc_recorded_file_present(&entry, &file)? {
				self.file_ops.link_no_replace(&original, &entry).await?;
			}
			fsync_parent_dir(self.file_ops.as_ref(), &entry).await?;
			self.gc_drop_run_locked(&remove, key, digest, intent, Some(file), original_present)
				.await?;
			drop(lease);
		}
		self.clear_registered_lsm_intent(remove.job_id).await;
		self.tier_authority.notify_publication_change();
		Ok(ownership)
	}

	/// Under exclusion: the path still names the inode that was hashed, at the
	/// revision it was hashed at, and is still that length.
	async fn gc_require_same_file(
		&self,
		slab_id: SlabId,
		file: &GcFileIdentity,
	) -> std::io::Result<()> {
		use std::os::unix::fs::MetadataExt;
		let revision = self
			.slab_publication_revisions
			.get(&slab_id)
			.map_or(0, |revision| *revision);
		let metadata = std::fs::symlink_metadata(slab_path(&self.config.dir, slab_id));
		let same = metadata.is_ok_and(|metadata| {
			metadata.file_type().is_file()
				&& (metadata.dev(), metadata.ino()) == (file.dev, file.ino)
				&& metadata.len() == file.length
		});
		if !same || revision != file.revision {
			return Err(Error::new(
				ErrorKind::WouldBlock,
				"the file a GC removal recorded changed or is missing; it remains fenced",
			));
		}
		Ok(())
	}

	/// Post-Complete work, P3c and P4, from the phase the row stands at: authorized by
	/// Complete's exact Applied and the removed set, read together and held through the
	/// handoff. From `PurgeAuthorized` an absent entry is an unlink that already happened
	/// and its sync is still owed; from `Purged` an entry here is not this workflow's. A
	/// phase before this one is never completed by an Applied.
	pub(super) async fn gc_finish_removal(
		self: Arc<Self>,
		remove: GcRemove,
		ticket: OpTicket,
		phase: OpProgress,
		_ownership: OpOwnership,
	) -> std::io::Result<()> {
		let index = self.require_op_index()?;
		let slab_id = remove.slab.local_id();
		let quarantine = gc_quarantine_path(&self.config.dir, slab_id, remove.job_id);
		// Content first and outside the lease; the same inode is rechecked under it.
		match &phase {
			OpProgress::RunDropped(Some(file)) => {
				gc_require_recorded_content(&quarantine, file).await?;
			}
			OpProgress::PurgeAuthorized(file) if gc_recorded_file_present(&quarantine, file)? => {
				gc_require_recorded_content(&quarantine, file).await?;
			}
			_ => {}
		}
		let lease = match self.canonical_tier_source.get() {
			Some(source) => Some(source.read().await?),
			None => None,
		};
		if lease.as_ref().is_some_and(|lease| {
			!lease.op_applied_for_current_owner(ticket.key(), ticket.digest())
				|| !lease.removed_for_current_owner(remove.slab)
		}) {
			return Err(Error::new(
				ErrorKind::WouldBlock,
				"a completed removal is not removed for the current owner; it remains fenced",
			));
		}
		self.gc_require_row_stands(ticket.key(), ticket.digest(), |at| at == Some(&phase), None)
			.await?;
		if self.compaction_manifest_snapshot().run(slab_id).is_some()
			|| gc_path_present(&slab_path(&self.config.dir, slab_id))?
		{
			return Err(Error::new(
				ErrorKind::WouldBlock,
				"a completed removal's run or original is still here; it remains fenced",
			));
		}
		let update = |progress| OpProgressUpdate {
			key: ticket.key(),
			digest: ticket.digest(),
			progress,
		};
		let purge = match phase {
			OpProgress::RunDropped(None) => None,
			OpProgress::RunDropped(Some(file)) => {
				gc_require_recorded_inode(&quarantine, &file)?;
				index
					.advance_op_progress(update(OpProgress::PurgeAuthorized(file.clone())), None)
					.await?;
				Some((file, true))
			}
			OpProgress::PurgeAuthorized(file) => {
				let present = gc_recorded_file_present(&quarantine, &file)?;
				Some((file, present))
			}
			OpProgress::Purged(_) => {
				if gc_path_present(&quarantine)? {
					return Err(Error::new(
						ErrorKind::WouldBlock,
						"a purged removal's entry is back; it remains fenced",
					));
				}
				None
			}
			other => {
				return Err(Error::new(
					ErrorKind::WouldBlock,
					format!("a removal at {other:?} cannot finish; it remains fenced"),
				));
			}
		};
		if let Some((file, present)) = purge {
			if present {
				self.file_ops.unlink(&quarantine).await?;
			}
			fsync_parent_dir(self.file_ops.as_ref(), &quarantine).await?;
			index
				.advance_op_progress(update(OpProgress::Purged(file)), None)
				.await?;
		}
		#[cfg(test)]
		self.pause_gc_before_clear_for_test().await;
		index.hand_off_finished_removal(&ticket).await?;
		drop(lease);
		Ok(())
	}

	/// The mark lost: put back what it changed, and record that, before anything
	/// is asked of the ring. The row and the linked intent both stay, so this
	/// survives a crash and the scope stays fenced until the Cancel is definite. Run as
	/// its own task: the rejection and the current owner are read first, and held.
	pub(super) async fn undo_rejected_claim(
		self: Arc<Self>,
		mark: OpTicket,
		removal: OpKey,
		reason: SlabRejection,
		pre_run: crate::storage::LsmRunSummary,
		pre_delete_pending: bool,
		ownership: OpOwnership,
	) -> std::io::Result<OpOwnership> {
		let slab_id = pre_run.slab_id;
		let lease = match self.canonical_tier_source.get() {
			Some(source) => Some(source.read().await?),
			None => None,
		};
		if lease
			.as_ref()
			.is_some_and(|lease| !lease.op_rejected_for_current_owner(mark.key(), mark.digest()))
		{
			return Err(Error::new(
				ErrorKind::WouldBlock,
				"a mark is not rejected for the current owner; it remains fenced",
			));
		}
		let _maintenance = self.maintenance_publication_gate.clone().read_owned().await;
		let tier_lock = self.tier_publication_lock(slab_id);
		let _tier_guard = tier_lock.lock().await;
		if self.compaction_manifest_snapshot().run(slab_id) != Some(&pre_run) {
			return Err(Error::new(
				ErrorKind::WouldBlock,
				"a rejected mark's run changed; it remains fenced",
			));
		}
		if !pre_delete_pending {
			self.slab_pins.clear_delete_pending(slab_id);
		}
		self.require_op_index()?
			.advance_op_progress(
				OpProgressUpdate {
					key: mark.key(),
					digest: mark.digest(),
					progress: OpProgress::ClaimRejected {
						reason,
						pre_run,
						pre_delete_pending,
						removal,
					},
				},
				None,
			)
			.await?;
		drop(lease);
		Ok(ownership)
	}

	/// Cancel a rejected mark's reserved removal with its original digest, and
	/// only on that exact definite answer hand both off. A transport failure is
	/// retried by the driver. Anything else is an anomaly and clears nothing.
	pub(crate) async fn resolve_rejected_claim(
		&self,
		committer: &dyn crate::storage::TierCommitter,
		claim: RejectedClaim,
	) -> std::io::Result<()> {
		let index = self.require_op_index()?;
		committer.preflight(claim.mark.key().owner).await?;
		let removal = match claim.removal {
			LoadedOp::Reserved(reserved) => index.reserved_for_cancel(reserved).await?,
			LoadedOp::Submittable(ticket) => ticket,
		};
		match committer.cancel_op(&removal).await? {
			VerifiedReceipt::Rejected {
				reason: SlabRejection::Cancelled,
				..
			} => {}
			other => {
				return Err(Error::new(
					ErrorKind::WouldBlock,
					format!("a rejected claim's removal answered {other:?}; it remains fenced"),
				));
			}
		}
		let crate::consensus::slab_ops::SlabCommand::Compaction(commit) = claim.mark.command()
		else {
			return Err(Error::other("a rejected claim's mark is not a mark"));
		};
		let linked = index
			.load_snapshot()
			.await?
			.lsm_install_intents
			.into_iter()
			.find(|intent| intent.commit.job_id == commit.job_id)
			.ok_or_else(|| Error::other("a rejected claim lost its linked intent"))?;
		// That answer is history. What authorizes this handoff is the current owner
		// and both decisions read together, held until the transaction has ended.
		self.require_cleanup_authority()?;
		let lease = match self.canonical_tier_source.get() {
			Some(source) => Some(source.read().await?),
			None => None,
		};
		if lease.as_ref().is_some_and(|lease| {
			!lease.rejected_claim_decided_for_current_owner(
				(claim.mark.key(), claim.mark.digest()),
				(removal.key(), removal.digest()),
			)
		}) {
			return Err(Error::new(
				ErrorKind::WouldBlock,
				"a rejected claim is not decided for the current owner; it remains fenced",
			));
		}
		index
			.hand_off_rejected_claim(&claim.mark, &removal, linked, Some(Box::new(lease)))
			.await?;
		self.clear_registered_lsm_intent(commit.job_id).await;
		self.tier_authority.notify_publication_change();
		Ok(())
	}
}
