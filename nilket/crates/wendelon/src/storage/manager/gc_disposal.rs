//! The local half of T1 for a GC removal (ADR 0697 §6.1): the claim authorizes
//! the disposal, Complete acknowledges it, and only Complete's Applied authorizes
//! the purge. Each stage runs as its own task, so whoever asked for it can go away
//! without releasing the exclusion and authority its filesystem work depends on.

use super::*;
use crate::consensus::slab_ops::{GcRemove, OpKey, SlabRejection};
use crate::storage::index::{
	GcFileIdentity, LoadedOp, LsmInputAuthority, LsmInstallIntent, OpProgress, OpProgressUpdate,
	OpTicket, RejectedClaim,
};
use crate::storage::sweeper::VerifiedReceipt;

pub(super) fn gc_quarantine_path(dir: &Path, slab_id: SlabId, job_id: CompactionJobId) -> PathBuf {
	dir.join(format!(
		"gc-quarantine-{:016x}-{:016x}.wnd",
		slab_id.get(),
		job_id.get()
	))
}

/// The quarantine entry still names the inode and length this workflow recorded.
pub(super) fn gc_require_quarantine_inode(
	path: &Path,
	file: &GcFileIdentity,
) -> std::io::Result<()> {
	use std::os::unix::fs::MetadataExt;
	let same = std::fs::symlink_metadata(path).is_ok_and(|metadata| {
		metadata.file_type().is_file()
			&& (metadata.dev(), metadata.ino()) == (file.dev, file.ino)
			&& metadata.len() == file.length
	});
	if !same {
		return Err(Error::new(
			ErrorKind::WouldBlock,
			"a quarantine entry is missing or is not the recorded file; it remains fenced",
		));
	}
	Ok(())
}

/// The quarantine entry's bytes are the ones recorded, read from the inode the
/// recheck under the lease will compare.
pub(super) async fn gc_require_quarantine_content(
	path: &Path,
	file: &GcFileIdentity,
) -> std::io::Result<()> {
	use std::os::unix::fs::MetadataExt;
	let (path, file) = (path.to_path_buf(), file.clone());
	tokio::task::spawn_blocking(move || {
		let mismatch = || {
			Error::new(
				ErrorKind::WouldBlock,
				"a quarantine entry is missing or is not the recorded file; it remains fenced",
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
	) -> std::io::Result<crate::storage::manifest::ManifestSnapshot> {
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
			self.bump_slab_publication_revision(slab_id)?;
			if self.slab_pins.pin_count(slab_id) != 0
				|| self.counter_slab_has_pending_frames(slab_id)
			{
				return Err(Error::new(
					ErrorKind::WouldBlock,
					"LSM GC eligibility changed before unlink",
				));
			}
			self.gc_unlink_owned_slab_file(slab_id).await?;
			self.redirects.remove_touching(&self.tier_budget, slab_id);
			self.record_disposal_evidence(slab_id).await;
			let publication = self.begin_index_publication(vec![slab_id]).await?;
			index
				.apply_with_publication_guard(
					PersistentIndexUpdate {
						delete_lsm_run_ids: vec![slab_id],
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
			// Memory follows durability: a commit that fails or is uncertain leaves
			// the run in the manifest, and the intent still fencing its scope.
			let snapshot = self.compaction_manifest_snapshot();
			self.apply_lsm_manifest_compaction_commit(&CompactionCommit::gc_remove(
				remove.slab.owner(),
				remove.job_id,
				snapshot.manifest_epoch,
				vec![slab_id],
			))
			.map_err(|error| Error::other(error.to_string()))?;
			publication.complete();
			drop(lease);
			self.compaction_manifest_snapshot()
		};
		// The transaction above is durable: the scope this workflow held is done.
		// Released outside the gates, so no scope waiter can invert them.
		self.clear_registered_lsm_intent(job_id).await;
		self.tier_authority.notify_publication_change();
		Ok(snapshot)
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

	/// Post-Complete work, P3c and P4: authorized by Complete's exact Applied
	/// receipt and the removed set, read together and held through the handoff.
	pub(super) async fn gc_finish_removal(
		self: Arc<Self>,
		remove: GcRemove,
		ticket: OpTicket,
		file: Option<GcFileIdentity>,
	) -> std::io::Result<()> {
		let index = self.require_op_index()?;
		let quarantine =
			gc_quarantine_path(&self.config.dir, remove.slab.local_id(), remove.job_id);
		// Content first and outside the lease; the same inode is rechecked under it.
		if let Some(file) = &file {
			gc_require_quarantine_content(&quarantine, file).await?;
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
		if let Some(file) = file {
			gc_require_quarantine_inode(&quarantine, &file)?;
			let update = |progress| OpProgressUpdate {
				key: ticket.key(),
				digest: ticket.digest(),
				progress,
			};
			index
				.advance_op_progress(update(OpProgress::PurgeAuthorized(file.clone())), None)
				.await?;
			self.file_ops.unlink(&quarantine).await?;
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
	/// survives a crash and the scope stays fenced until the Cancel is definite.
	pub(super) async fn undo_rejected_claim(
		&self,
		mark: &OpTicket,
		removal: OpKey,
		reason: SlabRejection,
		pre_run: &crate::storage::LsmRunSummary,
		pre_delete_pending: bool,
	) -> std::io::Result<()> {
		let slab_id = pre_run.slab_id;
		let _maintenance = self.maintenance_publication_gate.clone().read_owned().await;
		let tier_lock = self.tier_publication_lock(slab_id);
		let _tier_guard = tier_lock.lock().await;
		if self.compaction_manifest_snapshot().run(slab_id) != Some(pre_run) {
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
						pre_run: pre_run.clone(),
						pre_delete_pending,
						removal,
					},
				},
				None,
			)
			.await
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
