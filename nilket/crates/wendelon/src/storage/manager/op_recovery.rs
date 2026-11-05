//! ADR 0697 §6: resolving an operation a live workflow no longer holds. The driver's
//! pass acquires a row's ownership, rereads it, reads its decision together with what
//! it names, and only then acts. A view is evidence; each phase revalidates authority.
//! Recovery here is leader-only, and a store that is not its node's current incarnation
//! recovers nothing: history answers what happened, and grants no authority.

use super::*;
use crate::consensus::slab_ops::{
	RecordedOutcome, RunIdentity, SlabCommand, SlabStanding, ViewOutcome,
};
use crate::storage::index::{LoadedOp, LsmInstallIntent, OpProgress, OpTicket};

fn fenced(reason: String) -> Error {
	Error::new(ErrorKind::WouldBlock, reason)
}

impl SlabManager {
	/// §6 step 4's typed status, from the driver's role check.
	pub(crate) fn note_awaiting_leadership(&self, unresolved: bool) {
		self.tier_authority
			.awaiting_leadership
			.store(unresolved, Ordering::Release);
	}

	/// Every abandoned GC mark. A row in the hands of a live workflow is skipped.
	/// One row's failure stays visible and never starves the others.
	pub(crate) async fn recover_abandoned_marks(
		self: &Arc<Self>,
		committer: &dyn crate::storage::TierCommitter,
	) -> std::io::Result<()> {
		let index = self.require_op_index()?;
		let mut first_error = None;
		self.clear_scopes_of_handed_off_marks().await?;
		for loaded in index.load_op_intents().await? {
			let LoadedOp::Submittable(mark) = &loaded else {
				continue;
			};
			let SlabCommand::Compaction(commit) = mark.command() else {
				continue;
			};
			if commit.slabs_to_mark_delete_pending.is_empty()
				|| !matches!(
					loaded.progress(),
					Some(OpProgress::MarkPrepared { .. } | OpProgress::MarkPublished { .. })
				) {
				continue;
			}
			if let Err(error) = self.recover_abandoned_mark(mark, committer).await {
				tracing::warn!(op_seq = mark.key().op_seq, %error, "a GC mark is unresolved");
				first_error.get_or_insert(error);
			}
		}
		first_error.map_or(Ok(()), Err)
	}

	/// A mark's handoff can commit with its answer lost, leaving this process's scope
	/// behind. Its evidence is exact: the job's durable intent is gone, nothing live holds
	/// the scope, and its reserved removal carries the authority only that handoff writes.
	async fn clear_scopes_of_handed_off_marks(&self) -> std::io::Result<()> {
		let index = self.require_op_index()?;
		let orphaned = self
			.lsm_install_unresolved
			.iter()
			.filter(|scope| !scope.value().in_flight)
			.map(|scope| *scope.key())
			.collect::<Vec<_>>();
		if orphaned.is_empty() {
			return Ok(());
		}
		let durable = index.load_snapshot().await?;
		let rows = index.load_op_intents().await?;
		for job_id in orphaned {
			let handed_off = rows.iter().any(|op| {
				let LoadedOp::Reserved(reserved) = op else {
					return false;
				};
				op.reserved_inputs().is_some()
					&& matches!(reserved.command(), SlabCommand::GcRemove(remove) if remove.job_id == job_id)
			});
			if handed_off
				&& !durable
					.lsm_install_intents
					.iter()
					.any(|intent| intent.commit.job_id == job_id)
			{
				self.clear_registered_lsm_intent(job_id).await;
				self.tier_authority.notify_publication_change();
			}
		}
		Ok(())
	}

	async fn recover_abandoned_mark(
		self: &Arc<Self>,
		mark: &OpTicket,
		committer: &dyn crate::storage::TierCommitter,
	) -> std::io::Result<()> {
		let index = self.require_op_index()?;
		let SlabCommand::Compaction(commit) = mark.command() else {
			return Ok(());
		};
		// The linked intent names the whole workflow, and its live scope says a
		// producer still owns it.
		let Some(intent) = index
			.load_snapshot()
			.await?
			.lsm_install_intents
			.into_iter()
			.find(|intent: &LsmInstallIntent| intent.commit.job_id == commit.job_id)
		else {
			return Err(fenced(format!(
				"mark {} has lost its linked intent; it remains fenced",
				mark.key().op_seq
			)));
		};
		let Some(removal) = intent.ops.iter().find(|key| **key != mark.key()).copied() else {
			return Err(fenced(format!(
				"mark {} reserves no removal; it remains fenced",
				mark.key().op_seq
			)));
		};
		if self
			.lsm_install_unresolved
			.get(&commit.job_id)
			.is_some_and(|scope| scope.in_flight)
		{
			return Ok(());
		}
		let Some(ownership) = index.try_acquire_ops(vec![mark.key().op_seq, removal.op_seq]) else {
			return Ok(());
		};
		// Holding it now: the row as it stands, not as the scan saw it.
		let Some(LoadedOp::Submittable(mark)) =
			index.load_op_intents().await?.into_iter().find(
				|op| matches!(op, LoadedOp::Submittable(ticket) if ticket.key() == mark.key()),
			)
		else {
			return Ok(());
		};
		// §6 step 1. A committer with no ring reports this row unresolved, with no
		// local answer; a failed barrier is not a decision either.
		let view = committer
			.op_view(mark.key(), mark.digest(), mark.command().slabs())
			.await?;
		if !view.owner_current {
			return Ok(());
		}
		if mark.key().op_seq <= view.acked_through {
			return Err(fenced(format!(
				"mark {} is an intent at or below the acknowledged prefix {}; it remains fenced",
				mark.key().op_seq,
				view.acked_through
			)));
		}
		let progress = index.op_progress(mark.key().op_seq).await?;
		let pre_run_of = |progress: Option<OpProgress>| match progress {
			Some(OpProgress::MarkPrepared {
				pre_run,
				pre_delete_pending,
			}) => Some((pre_run, pre_delete_pending)),
			_ => None,
		};
		let published = matches!(progress, Some(OpProgress::MarkPublished { .. }));
		let Some((pre_run, pre_delete_pending)) = pre_run_of(progress).or_else(|| {
			// Past its publication only Applied can follow; the linked intent records
			// the run it started from.
			published
				.then(|| intent.local_input_runs.first().cloned())
				.flatten()
				.map(|run| (run, false))
		}) else {
			return Ok(());
		};
		let outcome = match view.outcome {
			// Abandoned before any decision: close it by its original digest, then read
			// what was decided: possibly that it applied after all. The receipt itself
			// grants nothing.
			ViewOutcome::None => {
				committer.cancel_op(&mark).await?;
				committer
					.op_view(mark.key(), mark.digest(), mark.command().slabs())
					.await?
					.outcome
			}
			decided => decided,
		};
		match outcome {
			ViewOutcome::Recorded(RecordedOutcome::Applied { .. }) => {
				tokio::spawn(self.clone().gc_complete_mark(
					mark.clone(),
					removal,
					intent,
					pre_run,
					ownership,
				))
				.await
				.map_err(Error::other)??;
				self.clear_registered_lsm_intent(commit.job_id).await;
				self.tier_authority.notify_publication_change();
				Ok(())
			}
			ViewOutcome::Recorded(RecordedOutcome::Rejected { reason, .. }) => {
				let _ownership = tokio::spawn(self.clone().undo_rejected_claim(
					mark.clone(),
					removal,
					reason,
					pre_run,
					pre_delete_pending,
					ownership,
				))
				.await
				.map_err(Error::other)??;
				let claim = index
					.load_rejected_claims()
					.await?
					.into_iter()
					.find(|claim| claim.mark.key() == mark.key())
					.ok_or_else(|| Error::other("a rejected claim lost its row"))?;
				self.resolve_rejected_claim(committer, claim).await
			}
			ViewOutcome::Conflict => Err(fenced(format!(
				"mark {} was decided under another digest; it remains fenced",
				mark.key().op_seq
			))),
			ViewOutcome::None => Err(fenced(format!(
				"mark {} has no decision after its Cancel; it remains fenced",
				mark.key().op_seq
			))),
		}
	}

	/// Every GC removal left behind by a workflow that is gone: §6.1's continuation, one
	/// step of it per pass, each from a fresh view. A Complete is never cancelled here.
	pub(crate) async fn recover_abandoned_removals(
		self: &Arc<Self>,
		committer: &dyn crate::storage::TierCommitter,
	) -> std::io::Result<()> {
		let index = self.require_op_index()?;
		let mut first_error = None;
		for loaded in index.load_op_intents().await? {
			// Not begun and no authority left for it: its mark is still deciding it, with a
			// rejected claim's Cancel among the ways, and this pass's mark recovery has that.
			if !matches!(loaded.command(), SlabCommand::GcRemove(_))
				|| (loaded.progress().is_none() && loaded.reserved_inputs().is_none())
			{
				continue;
			}
			if let Err(error) = self.recover_abandoned_removal(&loaded, committer).await {
				tracing::warn!(op_seq = loaded.key().op_seq, %error, "a GC removal is unresolved");
				first_error.get_or_insert(error);
			}
		}
		first_error.map_or(Ok(()), Err)
	}

	async fn recover_abandoned_removal(
		self: &Arc<Self>,
		loaded: &LoadedOp,
		committer: &dyn crate::storage::TierCommitter,
	) -> std::io::Result<()> {
		let index = self.require_op_index()?;
		let key = loaded.key();
		let Some(ownership) = index.try_acquire_ops(vec![key.op_seq]) else {
			return Ok(());
		};
		// Holding it now: the row as it stands, not as the scan saw it.
		let Some(loaded) = index
			.load_op_intents()
			.await?
			.into_iter()
			.find(|op| op.key() == key)
		else {
			return Ok(());
		};
		let SlabCommand::GcRemove(remove) = loaded.command().clone() else {
			return Ok(());
		};
		let slab_id = remove.slab.local_id();
		if self
			.lsm_install_unresolved
			.get(&remove.job_id)
			.is_some_and(|scope| scope.in_flight)
		{
			return Ok(());
		}
		let linked = index
			.load_snapshot()
			.await?
			.lsm_install_intents
			.into_iter()
			.find(|intent: &LsmInstallIntent| {
				intent.commit.job_id == remove.job_id && intent.ops.contains(&key)
			});
		let progress = loaded.progress().cloned();
		// Handed off by its mark and not begun: the sweep's local stage starts it.
		if progress.is_none() && linked.is_none() {
			return Ok(());
		}
		let view = committer
			.op_view(key, loaded.digest(), loaded.command().slabs())
			.await?;
		if !view.owner_current {
			return Ok(());
		}
		if key.op_seq <= view.acked_through {
			return Err(fenced(format!(
				"removal {} is an intent at or below the acknowledged prefix {}; it remains fenced",
				key.op_seq, view.acked_through
			)));
		}
		let claimed = view.slabs.iter().any(|(slab, standing)| {
			*slab == remove.slab && *standing == SlabStanding::Claimed(remove.job_id)
		});
		let phase = progress.clone();
		match (view.outcome, progress) {
			(ViewOutcome::Conflict, _) => Err(fenced(format!(
				"removal {} was decided under another digest; it remains fenced",
				key.op_seq
			))),
			(ViewOutcome::Recorded(RecordedOutcome::Rejected { reason, .. }), _) => {
				Err(fenced(format!(
					"the Complete of an accepted claim was rejected ({reason:?}); removal {} remains fenced",
					key.op_seq
				)))
			}
			(
				ViewOutcome::Recorded(RecordedOutcome::Applied { .. }),
				Some(
					phase @ (OpProgress::RunDropped(_)
					| OpProgress::PurgeAuthorized(_)
					| OpProgress::Purged(_)),
				),
			) => {
				let LoadedOp::Submittable(ticket) = loaded else {
					return Err(fenced(format!(
						"removal {} applied without its submission; it remains fenced",
						key.op_seq
					)));
				};
				tokio::spawn(
					self.clone()
						.gc_finish_removal(remove, ticket, phase, ownership),
				)
				.await
				.map_err(Error::other)??;
				self.slab_pins.clear_delete_pending(slab_id);
				self.tier_authority.notify_publication_change();
				Ok(())
			}
			// Applied never justifies disposal not yet performed.
			(ViewOutcome::Recorded(RecordedOutcome::Applied { .. }), _) => Err(fenced(format!(
				"removal {} applied at {phase:?}; it remains fenced",
				key.op_seq
			))),
			(ViewOutcome::None, _) if !claimed => Err(fenced(format!(
				"removal {} has no decision and its claim is gone; it remains fenced",
				key.op_seq
			))),
			(ViewOutcome::None, None) => {
				self.restart_removal_disposal(loaded, remove, linked, ownership)
					.await
			}
			(ViewOutcome::None, Some(OpProgress::QuarantineRecorded(file))) => {
				let Some(intent) = linked else {
					return Err(fenced(format!(
						"removal {} has lost its linked intent; it remains fenced",
						key.op_seq
					)));
				};
				tokio::spawn(self.clone().gc_resume_disposal(
					remove,
					key,
					loaded.digest(),
					intent,
					file,
					ownership,
				))
				.await
				.map_err(Error::other)??;
				Ok(())
			}
			// The same key and digest, whichever side of its first submission it is on.
			(ViewOutcome::None, Some(OpProgress::RunDropped(file))) => {
				if let Some(file) = &file {
					super::gc_disposal::gc_require_recorded_inode(
						&super::gc_disposal::gc_quarantine_path(
							&self.config.dir,
							slab_id,
							remove.job_id,
						),
						file,
					)?;
				}
				committer.preflight(key.owner).await?;
				let ticket = match loaded {
					LoadedOp::Submittable(ticket) => ticket,
					LoadedOp::Reserved(reserved) => index.mark_may_be_submitted(reserved).await?,
				};
				Self::require_gc_applied(committer.submit_op(&ticket).await?)
			}
			(ViewOutcome::None, _) => Err(fenced(format!(
				"removal {} has no decision at {phase:?}; it remains fenced",
				key.op_seq
			))),
		}
	}

	/// The table's first row: a workflow that left its intent and recorded nothing yet.
	/// The local stage from its start, against the authority its mark left.
	async fn restart_removal_disposal(
		self: &Arc<Self>,
		loaded: LoadedOp,
		remove: crate::consensus::slab_ops::GcRemove,
		linked: Option<LsmInstallIntent>,
		ownership: crate::storage::index::OpOwnership,
	) -> std::io::Result<()> {
		let slab_id = remove.slab.local_id();
		let key = loaded.key();
		let (Some(intent), Some(inputs), LoadedOp::Reserved(_)) =
			(linked, loaded.reserved_inputs(), &loaded)
		else {
			return Err(fenced(format!(
				"removal {} has no phase and no authority to begin from; it remains fenced",
				key.op_seq
			)));
		};
		let Some(run) = self
			.compaction_manifest_snapshot()
			.run(slab_id)
			.filter(|run| {
				run.state == SlabState::DeletePending && RunIdentity::of(run) == remove.expected_run
			})
			.cloned()
		else {
			return Err(fenced(format!(
				"removal {} no longer finds its run; it remains fenced",
				key.op_seq
			)));
		};
		let expected = {
			let _maintenance = self.maintenance_publication_gate.clone().read_owned().await;
			let tier_lock = self.tier_publication_lock(slab_id);
			let _tier_guard = tier_lock.lock().await;
			self.lsm_input_authority_locked(&[slab_id])
				.await
				.map_err(|error| fenced(format!("removal {}: {error}", key.op_seq)))?
		};
		if !crate::storage::index::LsmInputAuthority::all_unchanged_since(&expected, inputs) {
			return Err(fenced(format!(
				"removal {}'s input authority changed; it remains fenced",
				key.op_seq
			)));
		}
		let file = self.gc_read_original_identity(slab_id).await?;
		tokio::spawn(self.clone().gc_dispose_run(
			remove,
			key,
			loaded.digest(),
			expected,
			run,
			intent,
			file,
			ownership,
		))
		.await
		.map_err(Error::other)??;
		Ok(())
	}

	/// Every install whose workflow is gone. Each row reads its decision and acts on it
	/// from what the store durably holds; the view authorizes nothing by itself.
	pub(crate) async fn recover_abandoned_installs(
		self: &Arc<Self>,
		committer: &dyn crate::storage::TierCommitter,
	) -> std::io::Result<()> {
		let index = self.require_op_index()?;
		let mut first_error = None;
		for loaded in index.load_op_intents().await? {
			let LoadedOp::Submittable(ticket) = &loaded else {
				continue;
			};
			let SlabCommand::Compaction(commit) = ticket.command() else {
				continue;
			};
			if !commit.slabs_to_mark_delete_pending.is_empty() {
				continue;
			}
			if let Err(error) = self.recover_abandoned_install(ticket, committer).await {
				tracing::warn!(op_seq = ticket.key().op_seq, %error, "an LSM install is unresolved");
				first_error.get_or_insert(error);
			}
		}
		first_error.map_or(Ok(()), Err)
	}

	async fn recover_abandoned_install(
		self: &Arc<Self>,
		ticket: &OpTicket,
		committer: &dyn crate::storage::TierCommitter,
	) -> std::io::Result<()> {
		let index = self.require_op_index()?;
		let key = ticket.key();
		let Some(ownership) = index.try_acquire_ops(vec![key.op_seq]) else {
			return Ok(());
		};
		// Holding it now: the row as it stands, not as the scan saw it.
		let Some(LoadedOp::Submittable(ticket)) = index
			.load_op_intents()
			.await?
			.into_iter()
			.find(|op| op.key() == key)
		else {
			return Ok(());
		};
		let SlabCommand::Compaction(commit) = ticket.command() else {
			return Ok(());
		};
		if self
			.lsm_install_unresolved
			.get(&commit.job_id)
			.is_some_and(|scope| scope.in_flight)
		{
			return Ok(());
		}
		let durable = index.load_snapshot().await?;
		let Some(intent) = durable
			.lsm_install_intents
			.iter()
			.find(|intent: &&LsmInstallIntent| {
				intent.commit.job_id == commit.job_id && intent.ops.contains(&key)
			})
			.cloned()
		else {
			return Err(fenced(format!(
				"install {} has lost its linked intent; it remains fenced",
				key.op_seq
			)));
		};
		let read = || committer.op_view(key, ticket.digest(), ticket.command().slabs());
		let view = read().await?;
		if !view.owner_current {
			return Ok(());
		}
		if key.op_seq <= view.acked_through {
			return Err(fenced(format!(
				"install {} is an intent at or below the acknowledged prefix {}; it remains fenced",
				key.op_seq, view.acked_through
			)));
		}
		let outcome = match view.outcome {
			// Abandoned before any decision: close it by its original digest, then read
			// what was decided: possibly that it applied after all.
			ViewOutcome::None => {
				committer.cancel_op(&ticket).await?;
				read().await?.outcome
			}
			decided => decided,
		};
		let published = Self::install_left_a_trace(&intent, &durable);
		match outcome {
			ViewOutcome::Recorded(RecordedOutcome::Rejected { .. }) if published => {
				Err(fenced(format!(
					"install {} was rejected and has publication or conflicting input evidence; it remains fenced",
					key.op_seq
				)))
			}
			ViewOutcome::Recorded(RecordedOutcome::Rejected { .. }) => {
				tokio::spawn(
					self.clone()
						.undo_abandoned_install(ticket, intent, ownership),
				)
				.await
				.map_err(Error::other)??;
				Ok(())
			}
			ViewOutcome::Recorded(RecordedOutcome::Applied { .. }) => {
				let named = |id: SlabId| {
					intent.inputs.iter().any(|input| input.slab_id == id)
						|| intent
							.commit
							.slabs_to_add
							.iter()
							.any(|add| add.slab_id == id)
				};
				let covered = intent
					.local_input_runs
					.iter()
					.chain(&intent.commit.slabs_to_add)
					.any(|run| run.sidecar_coverage != crate::storage::SidecarCoverage::None)
					|| durable
						.secondary_index_segments
						.iter()
						.any(|row| named(row.key.source_slab_id));
				let catalogs = if covered {
					Some(committer.secondary_index_catalogs().await?)
				} else {
					None
				};
				tokio::spawn(
					self.clone()
						.finish_applied_install(ticket, intent, published, catalogs, ownership),
				)
				.await
				.map_err(Error::other)??;
				Ok(())
			}
			ViewOutcome::Conflict => Err(fenced(format!(
				"install {} was decided under another digest; it remains fenced",
				key.op_seq
			))),
			ViewOutcome::None => Err(fenced(format!(
				"install {} has no decision after its Cancel; it remains fenced",
				key.op_seq
			))),
		}
	}

	/// A definitely rejected install, per output, from whichever of its names is left: a
	/// temp file is a private artifact and goes, a final path moves out of the discovery
	/// namespace, and an abandoned name stays. One transaction then transfers every
	/// identity to its reservation, whose claim disposes of it. Run as its own task, it
	/// holds its authority and exclusion through every write.
	pub(super) async fn undo_abandoned_install(
		self: Arc<Self>,
		ticket: OpTicket,
		intent: LsmInstallIntent,
		_ownership: crate::storage::index::OpOwnership,
	) -> std::io::Result<()> {
		let index = self.require_op_index()?;
		let lease = match self.canonical_tier_source.get() {
			Some(source) => Some(source.read().await?),
			None => None,
		};
		if lease.as_ref().is_some_and(|lease| {
			!lease.op_rejected_for_current_owner(ticket.key(), ticket.digest())
		}) {
			return Err(fenced(format!(
				"install {} is not rejected for the current owner; it remains fenced",
				ticket.key().op_seq
			)));
		}
		let _maintenance = self
			.maintenance_publication_gate
			.clone()
			.write_owned()
			.await;
		// Before any file moves: this row and this intent, exactly, and no postimage.
		self.gc_require_row_stands(
			ticket.key(),
			ticket.digest(),
			|at| at.is_none(),
			Some(&intent),
		)
		.await?;
		let durable = index.load_snapshot().await?;
		if Self::install_left_a_trace(&intent, &durable) {
			return Err(fenced(format!(
				"install {} has publication or conflicting input evidence; it remains fenced",
				ticket.key().op_seq
			)));
		}
		let mut outputs = Vec::with_capacity(intent.local_output_files.len());
		for expected in &intent.local_output_files {
			outputs.push((
				expected.slab_id,
				self.abandon_install_output(&intent, expected).await?,
			));
		}
		self.hand_off_install_exactly(
			&ticket,
			&intent,
			index
				.hand_off_rejected_install(&ticket, intent.clone(), outputs)
				.await,
		)
		.await?;
		drop(lease);
		self.clear_registered_lsm_intent(intent.commit.job_id).await;
		self.tier_authority.notify_publication_change();
		Ok(())
	}

	/// A handoff whose commit landed and whose answer did not is finished. Its row and intent
	/// being gone is the only proof taken of that, read under this stage's ownership.
	async fn hand_off_install_exactly(
		&self,
		ticket: &OpTicket,
		intent: &LsmInstallIntent,
		answer: std::io::Result<()>,
	) -> std::io::Result<()> {
		let Err(error) = answer else {
			return Ok(());
		};
		let index = self.require_op_index()?;
		let row_gone = !index
			.load_op_intents()
			.await?
			.iter()
			.any(|op| op.key() == ticket.key());
		let intent_gone = !index
			.load_snapshot()
			.await?
			.lsm_install_intents
			.iter()
			.any(|durable| durable.commit.job_id == intent.commit.job_id);
		if row_gone && intent_gone {
			return Ok(());
		}
		Err(error)
	}

	/// One output of a rejected install, whichever step it had reached. Every name found
	/// must be the recorded file. Both final and abandoned exact is a relocation that
	/// linked and did not unlink. Anything else fences, with nothing moved.
	async fn abandon_install_output(
		&self,
		intent: &LsmInstallIntent,
		expected: &crate::storage::index::LsmFileIdentity,
	) -> std::io::Result<crate::storage::index::AbandonedOutput> {
		let id = expected.slab_id;
		let dir = &self.config.dir;
		let final_path = slab_path(dir, id);
		let temp = lsm_driver::lsm_compaction_tmp_path(dir, intent.commit.job_id, id);
		let moved = super::reservations::abandoned_output_path(dir, id);
		let ops = self.file_ops.as_ref();
		let mut found = [false; 3];
		for (slot, path) in found.iter_mut().zip([&final_path, &temp, &moved]) {
			*slot = ops.file_len(path).await?.is_some();
			if *slot && lsm_driver::lsm_file_identity(ops, path, id).await? != *expected {
				return Err(fenced(format!(
					"output {id:?} at {} is not the recorded file; it remains fenced",
					path.display()
				)));
			}
		}
		// Nothing is unlinked before the identity is durable somewhere: a temp file moves
		// to the abandoned name like a final one, so a crash anywhere here leaves a name that
		// proves what this retry finds. Finding none of the three is never taken as done.
		match found {
			[false, true, false] => self.relocate_rejected_output_from(&temp, id).await,
			[true, false, false] => self.relocate_rejected_output(id).await,
			[true, false, true] => {
				ops.unlink(&final_path).await?;
				fsync_parent_dir(ops, &final_path).await?;
				self.abandoned_output_evidence(&moved).await
			}
			[false, true, true] => {
				ops.unlink(&temp).await?;
				fsync_parent_dir(ops, &temp).await?;
				self.abandoned_output_evidence(&moved).await
			}
			[false, false, true] => self.abandoned_output_evidence(&moved).await,
			other => Err(fenced(format!(
				"output {id:?} stands at final, temp, abandoned {other:?}; it remains fenced"
			))),
		}
	}

	/// An install the ring applied. Not yet published: every output's identity and its
	/// reservation, then its rows from the output itself against the digest recorded before
	/// it was proposed, in one publication. Already published: the exact postimage, with no
	/// reservation recreated and nothing applied twice. Either way the handoff ends the
	/// intent, and memory is loaded with the rest of what this open deferred.
	pub(super) async fn finish_applied_install(
		self: Arc<Self>,
		ticket: OpTicket,
		intent: LsmInstallIntent,
		published: bool,
		catalogs: Option<(TableSchemaCatalog, SecondaryIndexCatalog)>,
		_ownership: crate::storage::index::OpOwnership,
	) -> std::io::Result<()> {
		let index = self.require_op_index()?;
		let key = ticket.key();
		let refused = |why: &str| {
			fenced(format!(
				"applied install {}: {why}; it remains fenced",
				key.op_seq
			))
		};
		// A sidecar swap cannot be proposed by any producer: preflight refuses it before
		// its intent. A persisted one is evidence of something else, and is not dropped.
		if !intent.commit.sidecar_swaps.is_empty() {
			return Err(refused(
				"its intent names sidecar swaps, which no recovery supports",
			));
		}
		// Covered inputs had their outputs' segments prepared by the live workflow, and a
		// crash took that with it. They are prepared again below, only against the catalogs
		// the existing sidecars were built for: never an incompatible replacement.
		let covered = intent
			.local_input_runs
			.iter()
			.chain(&intent.commit.slabs_to_add)
			.any(|run| run.sidecar_coverage != crate::storage::SidecarCoverage::None);
		if covered && catalogs.is_none() {
			return Err(refused(
				"it is sidecar-covered, with no catalogs to rebuild from",
			));
		}
		if let Some((_, indexes)) = &catalogs {
			// Publication retires the inputs' sidecars, so after a crash past it the outputs'
			// are what shows which schema this install was built for.
			let named = intent
				.inputs
				.iter()
				.map(|input| input.slab_id)
				.chain(intent.commit.slabs_to_add.iter().map(|run| run.slab_id));
			for slab_id in named {
				for info in self.list_secondary_index_segments_for_source_slab(slab_id) {
					let current = indexes
						.for_table(info.tenant_id, info.table_object_id)
						.into_iter()
						.any(|row| {
							row.descriptor.index_object_id == info.index_object_id
								&& row.descriptor.logical_schema_fingerprint
									== info.logical_schema_fingerprint
								&& matches!(
									row.state,
									crate::schema::SecondaryIndexState::Building
										| crate::schema::SecondaryIndexState::Ready
								)
						});
					if !current {
						return Err(refused(&format!(
							"a sidecar of {slab_id:?}, {:?}, has no matching index in the catalog",
							info.index_object_id
						)));
					}
				}
			}
		}
		let commit_digest =
			*blake3::hash(&serde_json::to_vec(&intent.commit).map_err(Error::other)?).as_bytes();
		if commit_digest != intent.commit_digest {
			return Err(refused("its intent's digest does not match its commit"));
		}
		// Sidecars are built first, with no authority held: an input can be tiered, and
		// object storage can stall. Nothing here changes anything.
		let prepared_sidecars = match &catalogs {
			Some((table_schemas, secondary_indexes)) => {
				self.prepare_install_sidecars(&intent, table_schemas, secondary_indexes)
					.await?
			}
			None => None,
		};
		// Capability first, before anything moves, held through the publication.
		let mut _exposed = Vec::new();
		if !published {
			for output in &intent.local_output_files {
				_exposed.push(self.reacquire_exposed(output.slab_id).await?);
			}
		}
		let lease = match self.canonical_tier_source.get() {
			Some(source) => Some(source.read().await?),
			None => None,
		};
		if lease
			.as_ref()
			.is_some_and(|lease| !lease.op_applied_for_current_owner(key, ticket.digest()))
		{
			return Err(refused("it is not applied for the current owner"));
		}
		let _maintenance = self
			.maintenance_publication_gate
			.clone()
			.write_owned()
			.await;
		let mut touched = intent
			.inputs
			.iter()
			.map(|input| input.slab_id)
			.chain(intent.local_output_files.iter().map(|file| file.slab_id))
			.collect::<Vec<_>>();
		touched.sort_unstable();
		touched.dedup();
		let locks = touched
			.iter()
			.map(|id| self.tier_publication_lock(*id))
			.collect::<Vec<_>>();
		let mut _guards = Vec::with_capacity(locks.len());
		for lock in &locks {
			_guards.push(lock.lock().await);
		}
		self.gc_require_row_stands(key, ticket.digest(), |at| at.is_none(), Some(&intent))
			.await?;
		self.reconcile_held_install_publication(&intent).await?;
		for file in &intent.local_output_files {
			let path = slab_path(&self.config.dir, file.slab_id);
			if lsm_driver::lsm_file_identity(self.file_ops.as_ref(), &path, file.slab_id).await?
				!= *file
			{
				return Err(refused("an output is not the recorded file"));
			}
		}
		let durable = index.load_snapshot().await?;
		// An earlier attempt's outcome, just decided, may be what this finds.
		let published = published || Self::install_left_a_trace(&intent, &durable);
		let inputs = intent
			.inputs
			.iter()
			.map(|input| input.slab_id)
			.collect::<std::collections::HashSet<_>>();
		if published {
			let exact = Self::install_postimage_is_exact(&intent, &durable);
			if !exact {
				return Err(refused(
					"its durable postimage is partial or not the recorded one",
				));
			}
		} else {
			self.publish_applied_install(&intent, &durable, &inputs)
				.await?;
		}
		if catalogs.is_some() {
			self.republish_install_sidecars(&intent, prepared_sidecars)
				.await?;
		}
		self.hand_off_install_exactly(
			&ticket,
			&intent,
			index
				.hand_off_op(key, ticket.digest(), Some(intent.clone()))
				.await,
		)
		.await?;
		drop(_guards);
		drop(lease);
		self.clear_registered_lsm_intent(intent.commit.job_id).await;
		self.tier_authority.notify_publication_change();
		Ok(())
	}

	/// Whether any part of this install's postimage is durable. It need not have outputs, so
	/// this reads the whole transition: its outputs, its inputs' retirement, and their rows.
	fn install_left_a_trace(
		intent: &LsmInstallIntent,
		durable: &crate::storage::index::PersistentIndexSnapshot,
	) -> bool {
		let output = |id: SlabId| {
			intent
				.commit
				.slabs_to_add
				.iter()
				.any(|add| add.slab_id == id)
		};
		let inputs = intent
			.inputs
			.iter()
			.map(|input| input.slab_id)
			.collect::<std::collections::HashSet<_>>();
		durable.lsm_runs.iter().any(|run| {
			output(run.slab_id)
				|| (intent.commit.slabs_to_retire.contains(&run.slab_id) && !run.state.is_live())
		}) || durable
			.record_rows
			.iter()
			.any(|row| output(row.location.slab_id))
			|| crate::storage::index::lsm_input_rows_digest(durable, &inputs)
				!= intent.local_input_rows_digest
	}

	/// Whether this install's exact postimage is what is durable. Its coverage is excluded:
	/// sidecar publication moves that afterward, and the sidecar check below answers for it.
	fn install_postimage_is_exact(
		intent: &LsmInstallIntent,
		durable: &crate::storage::index::PersistentIndexSnapshot,
	) -> bool {
		let outputs = intent
			.local_output_files
			.iter()
			.map(|file| file.slab_id)
			.collect::<std::collections::HashSet<_>>();
		let inputs = intent
			.inputs
			.iter()
			.map(|input| input.slab_id)
			.collect::<std::collections::HashSet<_>>();
		let successors = intent
			.commit
			.slabs_to_add
			.iter()
			.map(|add| add.slab_id)
			.collect::<Vec<_>>();
		intent.commit.slabs_to_add.iter().all(|add| {
			durable.lsm_runs.iter().any(|run| {
				run.slab_id == add.slab_id
					&& run.state.is_live()
					&& run.compaction_level == add.compaction_level
					&& run.min_row_hlc == add.min_row_hlc
					&& run.max_row_hlc == add.max_row_hlc
					&& run.size_bytes == add.size_bytes
					&& run.row_count == add.row_count
			})
		}) && intent.commit.slabs_to_retire.iter().all(|id| {
			durable.lsm_runs.iter().any(|run| {
				run.slab_id == *id
					&& matches!(&run.state, SlabState::Retired { successors: found } if *found == successors)
			})
		}) && crate::storage::index::lsm_input_rows_digest(durable, &outputs)
			== intent.local_output_rows_digest
			&& !durable
				.record_rows
				.iter()
				.any(|row| inputs.contains(&row.location.slab_id))
	}

	/// A publication an earlier attempt could not read the outcome of, now decided from
	/// durable state: exact, memory follows it; no trace of it, this retry starts over; and
	/// anything between stays fenced, still held. Only this job's own publication is touched.
	async fn reconcile_held_install_publication(
		&self,
		intent: &LsmInstallIntent,
	) -> std::io::Result<()> {
		let job = intent.commit.job_id;
		let held = self
			.tier_authority
			.held_install_publications
			.lock()
			.unwrap_or_else(|e| e.into_inner())
			.remove(&job);
		let Some(publication) = held else {
			return Ok(());
		};
		let hold_again = |publication| {
			self.tier_authority
				.held_install_publications
				.lock()
				.unwrap_or_else(|e| e.into_inner())
				.insert(job, publication);
		};
		let durable = match self.read_published_install_outcome().await {
			Ok(durable) => durable,
			Err(error) => {
				hold_again(publication);
				return Err(error);
			}
		};
		if Self::install_postimage_is_exact(intent, &durable) {
			// Memory follows exactly once: the manifest recognizes its own commit. Not
			// following is not an outcome anyone can guess at, so it stays held.
			if let Err(error) = self.apply_lsm_manifest_compaction_commit(&intent.commit) {
				hold_again(publication);
				return Err(fenced(format!(
					"job {job:?}: memory cannot follow its publication ({error}); it remains fenced"
				)));
			}
			publication.complete();
			return Ok(());
		}
		if !Self::install_left_a_trace(intent, &durable) {
			publication.complete();
			return Ok(());
		}
		hold_again(publication);
		Err(fenced(format!(
			"job {job:?}: its publication left a partial postimage; it remains fenced"
		)))
	}

	async fn read_published_install_outcome(
		&self,
	) -> std::io::Result<crate::storage::index::PersistentIndexSnapshot> {
		#[cfg(test)]
		if self
			.continuity_test_holds
			.fail_publication_readback
			.swap(false, Ordering::AcqRel)
		{
			return Err(Error::other(
				"injected: a publication's outcome could not be read",
			));
		}
		self.require_op_index()?.load_snapshot().await
	}

	/// The install's sidecars, through the path the live workflow takes, prepared from its
	/// outputs at their final paths.
	async fn prepare_install_sidecars(
		&self,
		intent: &LsmInstallIntent,
		table_schemas: &TableSchemaCatalog,
		secondary_indexes: &SecondaryIndexCatalog,
	) -> std::io::Result<Option<super::secondary_index_segments::LsmPreparedSecondaryIndexSegments>>
	{
		let commit = &intent.commit;
		// Neither time is read: preparing sidecars asks a plan only for its slabs.
		let any_hlc = <HybridTimestamp as bytemuck::Zeroable>::zeroed();
		let plan = CompactionPlan {
			job_id: commit.job_id,
			source_level: CompactionLevelIndex::L0,
			target_level: CompactionLevelIndex::L0,
			manifest_epoch_pre: commit.manifest_epoch_pre,
			input_slabs: intent.inputs.iter().map(|input| input.slab_id).collect(),
			overlapping_slabs: Vec::new(),
			retention_floor_snapshot: any_hlc,
			compaction_start_hlc: any_hlc,
			dominant_reason: crate::storage::DominantReason::Operator,
		};
		let written = commit
			.slabs_to_add
			.iter()
			.map(|run| crate::storage::LsmWrittenOutputRun {
				slab_id: run.slab_id,
				compaction_level: run.compaction_level,
				temp_path: slab_path(&self.config.dir, run.slab_id),
				summary: run.clone(),
				locations: Vec::new(),
			})
			.collect::<Vec<_>>();
		self.prepare_lsm_secondary_index_segments_for_compaction(
			&plan,
			&written,
			Some(SecondaryIndexCompactionContext {
				table_schemas,
				secondary_indexes,
			}),
			false,
		)
		.await
		.map_err(|error| fenced(format!("the install's sidecars: {error}")))
	}

	/// With the input sidecars retired and the new ones persisted under this stage's
	/// exclusions; a repeat replaces what an earlier attempt published. A segment that could
	/// not be persisted is only acceptable durably recorded as such, so that a query knows to
	/// fall back and a reopen still knows: that is checked here, not assumed from a count.
	async fn republish_install_sidecars(
		&self,
		intent: &LsmInstallIntent,
		prepared: Option<super::secondary_index_segments::LsmPreparedSecondaryIndexSegments>,
	) -> std::io::Result<()> {
		let expected = prepared
			.as_ref()
			.map(|prepared| {
				prepared
					.output_segments
					.iter()
					.map(|segment| (segment.output_slab_id, segment.index_object_id))
					.collect::<Vec<_>>()
			})
			.unwrap_or_default();
		self.publish_prepared_lsm_secondary_index_segments_locked(prepared)
			.await;
		if self.has_orphaned_index_publications() {
			return Err(fenced(
				"the install's sidecar publication is unresolved; it remains fenced".into(),
			));
		}
		let durable = self.require_op_index()?.load_snapshot().await?;
		for (slab_id, index_object_id) in expected {
			let persisted = durable.secondary_index_segments.iter().any(|row| {
				row.key.source_slab_id == slab_id && row.key.index_object_id == index_object_id
			});
			let recorded_partial = durable.lsm_runs.iter().any(|run| {
				run.slab_id == slab_id
					&& run.sidecar_coverage == crate::storage::SidecarCoverage::Partial
			});
			if !persisted && !recorded_partial {
				return Err(fenced(format!(
					"job {:?}: output {slab_id:?}'s sidecar is neither durable nor durably recorded 					 as partial; it remains fenced",
					intent.commit.job_id
				)));
			}
		}
		Ok(())
	}

	/// The publication the workflow did not reach, from durable evidence alone: its rows
	/// are read back from the outputs, carried from the input rows they replace, and must
	/// be exactly what its intent recorded before it was proposed.
	async fn publish_applied_install(
		&self,
		intent: &LsmInstallIntent,
		durable: &crate::storage::index::PersistentIndexSnapshot,
		inputs: &std::collections::HashSet<SlabId>,
	) -> std::io::Result<()> {
		let job = intent.commit.job_id;
		let refused = |why: &str| {
			fenced(format!(
				"applied install of job {job:?}: {why}; it remains fenced"
			))
		};
		// As `open` built them: durable runs, and a summary of any slab never yet in one.
		let manifest = self.compaction_manifest_snapshot();
		if let Some(run) = intent
			.local_input_runs
			.iter()
			.find(|run| manifest.run(run.slab_id) != Some(run))
		{
			return Err(refused(&format!(
				"its input run {:?} is not the recorded one: {:?} against {:?}",
				run.slab_id,
				run,
				manifest.run(run.slab_id)
			)));
		}
		if crate::storage::index::lsm_input_rows_digest(durable, inputs)
			!= intent.local_input_rows_digest
		{
			return Err(refused("its input rows are not the recorded ones"));
		}
		for file in &intent.local_input_files {
			let path = slab_path(&self.config.dir, file.slab_id);
			if lsm_driver::lsm_file_identity(self.file_ops.as_ref(), &path, file.slab_id).await?
				!= *file
			{
				return Err(refused("an input file is not the recorded one"));
			}
		}
		let removed = durable
			.record_rows
			.iter()
			.filter(|row| inputs.contains(&row.location.slab_id))
			.copied()
			.collect::<Vec<_>>();
		let mut inserted = Vec::new();
		for file in &intent.local_output_files {
			let slab = LocalSlabStore::open_recovered(
				slab_path(&self.config.dir, file.slab_id),
				file.slab_id,
				self.file_ops.clone(),
				self.config.group_commit.clone(),
				Some(&BTreeSet::new()),
				true,
			)
			.await?;
			for entry in slab.index.iter() {
				let (tenant_id, key_identity) = *entry.key();
				for location in entry.value().iter() {
					let Some(source) = removed.iter().find(|row| {
						row.tenant_id == tenant_id
							&& row.key_identity == key_identity
							&& row.location.hlc == location.hlc
					}) else {
						return Err(refused("an output row has no input row"));
					};
					inserted.push(PersistentRecordRow {
						location: RecordLocation {
							placement: source.placement,
							partition: source.partition,
							..*location
						},
						..*source
					});
				}
			}
		}
		if crate::storage::index::lsm_rows_digest(inserted.iter().copied())
			!= intent.local_output_rows_digest
		{
			return Err(refused("its outputs' rows are not the recorded ones"));
		}
		// The postimage is prepared aside. Memory moves only once the durable outcome is
		// known, so a transaction that never landed leaves this retry its own preimage.
		let runs = {
			let mut prepared =
				recover_rwlock_read(&self.compaction_manifest, "compaction_manifest").clone();
			prepared
				.apply_compaction_commit(&intent.commit)
				.map_err(|error| refused(&error.to_string()))?;
			prepared.snapshot().runs().to_vec()
		};
		let mut slabs = inputs.iter().copied().collect::<Vec<_>>();
		slabs.extend(intent.local_output_files.iter().map(|file| file.slab_id));
		let publication = self.begin_index_publication(slabs).await?;
		let index = self.require_op_index()?;
		let applied = index
			.apply_with_publication_guard(
				PersistentIndexUpdate {
					delete_records: removed,
					put_records: inserted,
					put_lsm_runs: runs,
					promote_reservations: intent
						.local_output_files
						.iter()
						.map(|file| file.slab_id)
						.collect(),
					..Default::default()
				},
				Some(publication.gate.clone()),
			)
			.await;
		// Its worker has returned, and this stage holds every exclusion, so the durable state
		// now answers. If it cannot be read, this stage's publication stays held for the next
		// pass to decide: an unknown outcome is fenced, never completed on a guess.
		let durable = match self.read_published_install_outcome().await {
			Ok(durable) => durable,
			Err(error) => {
				self.tier_authority
					.held_install_publications
					.lock()
					.unwrap_or_else(|e| e.into_inner())
					.insert(intent.commit.job_id, publication);
				return Err(error);
			}
		};
		if Self::install_postimage_is_exact(intent, &durable) {
			if let Err(error) = self.apply_lsm_manifest_compaction_commit(&intent.commit) {
				self.tier_authority
					.held_install_publications
					.lock()
					.unwrap_or_else(|e| e.into_inner())
					.insert(job, publication);
				return Err(refused(&error.to_string()));
			}
			publication.complete();
			return Ok(());
		}
		publication.complete();
		applied?;
		Err(refused("its publication did not land"))
	}

	/// A slab's unpaired counter tentatives, each held pending, as `open` does. Local
	/// recovery has consumed physically paired copies; this remaining copy's resolution is
	/// never inferred from another sharing its logical identity.
	pub(super) fn note_recovered_counter_tentatives(
		slab: &Arc<LocalSlabStore>,
		frames: &DashSet<PendingCounterFrame>,
		guards: &CounterPendingGuards,
	) -> std::io::Result<()> {
		for candidate in &slab.recovered_counter_tentatives_v2 {
			let pending = Self::pending_counter_frame(
				candidate.tenant_id,
				candidate.key_identity,
				candidate.location.hlc,
				candidate.origin,
				candidate.location.slab_id,
				candidate.location.offset,
			);
			frames.insert(pending);
			guards.insert(pending, slab.track_recovered_counter()?);
		}
		Ok(())
	}

	/// §6, Codex 01a1aab5: what `open` left undone for an intent, once none remains. The
	/// store stays fenced until this has finished, and a failure is retried by the next
	/// pass. Everything it reads is durable, so a death part way is a clean next open.
	pub(crate) async fn complete_deferred_open(self: &Arc<Self>) -> std::io::Result<()> {
		if !self.tier_authority.deferred_open.load(Ordering::Acquire) {
			return Ok(());
		}
		let index = self.require_op_index()?;
		if !self.lsm_install_unresolved.is_empty()
			|| !index.load_snapshot().await?.lsm_install_intents.is_empty()
		{
			return Ok(());
		}
		tokio::spawn(self.clone().finish_deferred_open())
			.await
			.map_err(Error::other)?
	}

	async fn finish_deferred_open(self: Arc<Self>) -> std::io::Result<()> {
		#[cfg(test)]
		self.pause_deferred_open_for_test().await?;
		let _maintenance = self
			.maintenance_publication_gate
			.clone()
			.write_owned()
			.await;
		if !self.tier_authority.deferred_open.load(Ordering::Acquire) {
			return Ok(());
		}
		// `open`'s own conditions for rebuilding, rechecked under this gate. Each keeps its
		// own readiness reason; this is about the durable evidence a rebuild would replace.
		let durable_intents = self
			.require_op_index()?
			.load_snapshot()
			.await?
			.lsm_install_intents
			.len();
		let standing = [
			("an integrity failure", !self.integrity_failures.is_empty()),
			(
				"unavailable index authority",
				self.index_authority_unavailable.load(Ordering::Acquire),
			),
			(
				"awaiting the cluster",
				self.awaiting_cluster.load(Ordering::Acquire),
			),
			("tier verification", !self.tier_authority.pending.is_empty()),
			(
				"an unresolved scope",
				!self.lsm_install_unresolved.is_empty(),
			),
			(
				"a pending publication",
				!self.pending_index_publications.is_empty(),
			),
			("a durable intent", durable_intents != 0),
		];
		if let Some((name, _)) = standing.iter().find(|(_, stands)| *stands) {
			return Err(fenced(format!(
				"{name} stands; this open's work stays deferred"
			)));
		}
		// A secondary-index temp file's producer is only excluded by readiness, which is
		// not what `open`'s cleanup of them relies on: those stay for the next open.
		maintenance_publication::reconcile_staged_maintenance_publications(
			self.file_ops.as_ref(),
			&self.config.dir,
			Some(self.compaction_manifest_snapshot().runs()),
			None,
		)
		.await?;
		retention_reclaim::cleanup_retention_rewrite_tmp_files(
			self.file_ops.as_ref(),
			&self.config.dir,
		)
		.await?;
		lsm_driver::cleanup_lsm_compaction_tmp_files(self.file_ops.as_ref(), &self.config.dir)
			.await?;
		let durable = self.require_op_index()?.load_snapshot().await?;
		#[cfg(test)]
		self.pause_deferred_open_stage_for_test(1).await;
		self.load_unopened_live_runs(&durable).await?;
		#[cfg(test)]
		self.pause_deferred_open_stage_for_test(2).await;
		self.remove_rows_of_slabs_no_longer_here(&durable).await;
		// `open` builds this last, from the final localities of the keys that remain.
		self.key_localities
			.retain(|key, _| self.key_index.contains_key(key));
		*recover_rwlock_write(&self.locality_index, "locality_index") =
			build_locality_index(&self.key_localities);
		#[cfg(test)]
		self.pause_deferred_open_stage_for_test(3).await;
		{
			// The mirror, as `open` rebuilds it, from one checked snapshot, and only once
			// memory holds every version it names. Disposal evidence stays: unlike at
			// `open`, this process holds readers that can still be told what it disposed of.
			let _gate = self.persistent_index_gate.clone().write_owned().await;
			let durable = self.require_op_index()?.load_snapshot().await?;
			open::refuse_unaccounted_durable_versions(
				&self,
				durable
					.record_rows
					.iter()
					.chain(durable.tiered_records.iter())
					.map(|row| {
						(
							(row.tenant_id, row.key_identity, row.location),
							row.table_object_id,
						)
					})
					.collect(),
			)?;
			let rows = self.persistent_record_rows_snapshot();
			let tiered = self.tiered_pointers_snapshot().await;
			self.require_op_index()?
				.rebuild_with_secondary_index_segments_and_lsm_runs(
					rows,
					tiered,
					durable.secondary_index_segments,
					durable.lsm_runs,
					None,
				)
				.await?;
		}
		#[cfg(test)]
		self.pause_deferred_open_stage_for_test(4).await;
		if self.tier_authority.pending.is_empty() {
			self.recover_version_conflicts().await?;
		}
		self.tier_authority
			.deferred_open
			.store(false, Ordering::Release);
		self.tier_authority.notify_publication_change();
		Ok(())
	}

	/// Every live durable run this open held back as evidence: its slab, its rows in
	/// memory, and what they reference, as `open` loads any other.
	async fn load_unopened_live_runs(
		&self,
		durable: &crate::storage::index::PersistentIndexSnapshot,
	) -> std::io::Result<()> {
		for run in durable.lsm_runs.iter().filter(|run| run.state.is_live()) {
			let id = run.slab_id;
			{
				let state = self.state.lock().await;
				if state.active.slab_id() == id
					|| state.sealed.contains_key(&id)
					|| state.tiered.contains_key(&id)
					|| state.rehydrated.contains_key(&id)
				{
					continue;
				}
			}
			let path = slab_path(&self.config.dir, id);
			// A live durable run with no file is lost storage, never absence: rebuilding
			// over it would erase the only evidence of what it held.
			if self.file_ops.file_len(&path).await?.is_none() {
				return Err(fenced(format!(
					"live run {id:?} has no file; this open's work stays deferred"
				)));
			}
			let acknowledged = durable
				.record_rows
				.iter()
				.filter(|row| row.location.slab_id == id)
				.map(|row| row.location.offset)
				.collect::<BTreeSet<_>>();
			let slab = Arc::new(
				LocalSlabStore::open_recovered(
					path,
					id,
					self.file_ops.clone(),
					self.config.group_commit.clone(),
					Some(&acknowledged),
					true,
				)
				.await?,
			);
			Self::note_recovered_counter_tentatives(
				&slab,
				&self.counter_pending_frames,
				&self.counter_pending_guards,
			)?;
			// Nothing fallible from here to the install, and this runs detached under the
			// maintenance gate, so no one sees it half done, and a retry finds it installed
			// and skips it: this drain, which does not dedup, runs once per slab.
			let mut localities = Vec::new();
			drain_slab_index(&slab, &self.key_index, &mut localities);
			// As `open` orders them: what the slab recovered, then what is durable.
			let durable_localities = durable
				.record_rows
				.iter()
				.filter(|row| row.location.slab_id == id)
				.map(|row| (row.tenant_id, row.key_identity, row.routing_locality));
			for (tenant_id, key_identity, locality) in
				localities.into_iter().chain(durable_localities)
			{
				self.key_localities
					.insert((tenant_id, key_identity), locality);
			}
			for row in durable
				.record_rows
				.iter()
				.filter(|row| row.location.slab_id == id)
			{
				if let Some(table) = row.table_object_id
					&& self
						.table_by_location
						.insert(row.location, (row.tenant_id, table))
						.is_none()
				{
					*self
						.table_references_by_slab
						.entry(id)
						.or_default()
						.entry((row.tenant_id, table))
						.or_default() += 1;
				}
			}
			// Exactly what is durable: a sidecar published before or by this recovery alike.
			let mut sidecar_references = BTreeMap::new();
			for row in durable
				.secondary_index_segments
				.iter()
				.filter(|row| row.key.source_slab_id == id)
			{
				*sidecar_references
					.entry((row.key.tenant_id, row.key.table_object_id))
					.or_default() += 1;
			}
			self.secondary_index_table_references_by_slab
				.insert(id, sidecar_references);
			Self::recover_counter_cells(
				&self.counter_eligible,
				std::slice::from_ref(&slab),
				&self.counter_unsupported,
				&self.key_index,
			);
			self.state
				.lock()
				.await
				.install_sealed(&self.tier_budget, id, slab);
		}
		Ok(())
	}

	/// A clean open drops rows of non-live runs. Under an intent it did not, and GC since
	/// may have ended a run altogether, so its slab is in no list. A location is dropped for
	/// that only with no backing, no run and no durable row naming it: absence from the
	/// manifest alone says nothing.
	async fn remove_rows_of_slabs_no_longer_here(
		&self,
		durable: &crate::storage::index::PersistentIndexSnapshot,
	) {
		let manifest = self.compaction_manifest_snapshot();
		let state = self.state.lock().await;
		let stale = |slab_id: SlabId| match manifest.run(slab_id) {
			Some(run) => !run.state.is_live(),
			None => {
				state.active.slab_id() != slab_id
					&& !state.sealed.contains_key(&slab_id)
					&& !state.tiered.contains_key(&slab_id)
					&& !state.rehydrated.contains_key(&slab_id)
					&& !durable
						.record_rows
						.iter()
						.chain(&durable.tiered_records)
						.any(|row| row.location.slab_id == slab_id)
			}
		};
		let mut by_key = BTreeMap::new();
		for entry in self.key_index.iter() {
			let removals = entry
				.value()
				.iter()
				.filter(|location| stale(location.slab_id))
				.copied()
				.collect::<Vec<_>>();
			if !removals.is_empty() {
				by_key.insert(*entry.key(), removals);
			}
		}
		for ((tenant_id, key_identity), removals) in by_key {
			let mut relocation =
				KeyRelocation::new(self.routing_locality_for_key(tenant_id, key_identity), None);
			relocation.removals = removals;
			let mut scratch = RelocationScratch::for_relocation(&relocation);
			self.relocate_indexed_locations(tenant_id, key_identity, &relocation, &mut scratch);
		}
	}

	/// Every Tier and Retire whose workflow is gone. Ownership before action, as anywhere:
	/// a live proposal holds its sequence through its submission.
	pub(crate) async fn recover_abandoned_tier_ops(
		self: &Arc<Self>,
		committer: &dyn crate::storage::TierCommitter,
	) -> std::io::Result<()> {
		let index = self.require_op_index()?;
		let mut first_error = None;
		for loaded in index.load_op_intents().await? {
			let LoadedOp::Submittable(ticket) = &loaded else {
				continue;
			};
			if !matches!(
				ticket.command(),
				SlabCommand::Tier(_) | SlabCommand::Retire(_)
			) {
				continue;
			}
			if let Err(error) = self.recover_abandoned_tier_op(ticket, committer).await {
				tracing::warn!(op_seq = ticket.key().op_seq, %error, "a tier operation is unresolved");
				first_error.get_or_insert(error);
			}
		}
		first_error.map_or(Ok(()), Err)
	}

	async fn recover_abandoned_tier_op(
		self: &Arc<Self>,
		ticket: &OpTicket,
		committer: &dyn crate::storage::TierCommitter,
	) -> std::io::Result<()> {
		let index = self.require_op_index()?;
		let key = ticket.key();
		let Some(ownership) = index.try_acquire_ops(vec![key.op_seq]) else {
			return Ok(());
		};
		// Holding it now: the row as it stands, not as the scan saw it.
		let Some(LoadedOp::Submittable(ticket)) = index
			.load_op_intents()
			.await?
			.into_iter()
			.find(|op| op.key() == key)
		else {
			return Ok(());
		};
		let read = || committer.op_view(key, ticket.digest(), ticket.command().slabs());
		let view = read().await?;
		if !view.owner_current {
			return Ok(());
		}
		if key.op_seq <= view.acked_through {
			return Err(fenced(format!(
				"tier operation {} is an intent at or below the acknowledged prefix {}; it remains fenced",
				key.op_seq, view.acked_through
			)));
		}
		let outcome = match view.outcome {
			ViewOutcome::None => {
				committer.cancel_op(&ticket).await?;
				read().await?.outcome
			}
			decided => decided,
		};
		// Its apply is deferred in this process: that path replays it, and hands it off.
		let slab_id = match ticket.command() {
			SlabCommand::Tier(op) => op.tier.slab.local_id(),
			SlabCommand::Retire(op) => op.slab.local_id(),
			_ => return Ok(()),
		};
		if self.unapplied_tier_pointers.contains_key(&slab_id) {
			return Ok(());
		}
		match outcome {
			ViewOutcome::Recorded(RecordedOutcome::Applied { index: applied }) => tokio::spawn(
				self.clone()
					.deliver_applied_tier_op(ticket, applied, ownership),
			)
			.await
			.map_err(Error::other)?,
			ViewOutcome::Recorded(RecordedOutcome::Rejected { .. }) => {
				tokio::spawn(self.clone().hand_off_rejected_tier_op(ticket, ownership))
					.await
					.map_err(Error::other)?
			}
			ViewOutcome::Conflict => Err(fenced(format!(
				"tier operation {} was decided under another digest; it remains fenced",
				key.op_seq
			))),
			ViewOutcome::None => Err(fenced(format!(
				"tier operation {} has no decision after its Cancel; it remains fenced",
				key.op_seq
			))),
		}
	}

	/// §6.1's adapter: what the ring's observer would have delivered, had this store been
	/// here, then the handoff once it is durable. A failed apply keeps its row.
	async fn deliver_applied_tier_op(
		self: Arc<Self>,
		ticket: OpTicket,
		applied: u64,
		_ownership: crate::storage::index::OpOwnership,
	) -> std::io::Result<()> {
		// Object storage first, with no authority held: it can stall, and a state machine
		// writer must not wait on it. What it found is only used if nothing moved since.
		let prepared = match ticket.command() {
			SlabCommand::Tier(op) => Some((
				TieredSlabPointer::from_ring(&op.tier),
				self.prepare_tier_delivery(&TieredSlabPointer::from_ring(&op.tier))
					.await?,
			)),
			_ => None,
		};
		#[cfg(test)]
		{
			let hold = self
				.continuity_test_holds
				.tier_delivery_hold
				.lock()
				.unwrap()
				.clone();
			if let Some((entered, release)) = hold {
				entered.add_permits(1);
				let _ = release.acquire().await;
			}
		}
		let lease = match self.canonical_tier_source.get() {
			Some(source) => Some(source.read().await?),
			None => None,
		};
		if lease
			.as_ref()
			.is_some_and(|lease| !lease.op_applied_for_current_owner(ticket.key(), ticket.digest()))
		{
			return Err(fenced(format!(
				"tier operation {} is not applied for the current owner; it remains fenced",
				ticket.key().op_seq
			)));
		}
		match ticket.command() {
			SlabCommand::Tier(_) => {
				let Some((pointer, prepared)) = prepared else {
					return Err(Error::other("a tier operation prepared nothing"));
				};
				self.apply_prepared_tier_delivery(pointer, Some(applied), prepared)
					.await?;
			}
			SlabCommand::Retire(op) => {
				self.retire_tiered_slab_committed(op.slab.local_id(), Some(applied))
					.await?;
			}
			other => {
				return Err(Error::other(format!("not a tier operation: {other:?}")));
			}
		}
		drop(lease);
		self.hand_off_op_if_complete(&ticket).await
	}

	/// Rejected, by the ring's word. A tier's candidate becomes durable in the handoff, and
	/// nothing is deleted from L2 here: its disposal is the candidate policy's.
	async fn hand_off_rejected_tier_op(
		self: Arc<Self>,
		ticket: OpTicket,
		_ownership: crate::storage::index::OpOwnership,
	) -> std::io::Result<()> {
		let index = self.require_op_index()?;
		let lease = match self.canonical_tier_source.get() {
			Some(source) => Some(source.read().await?),
			None => None,
		};
		if lease.as_ref().is_some_and(|lease| {
			!lease.op_rejected_for_current_owner(ticket.key(), ticket.digest())
		}) {
			return Err(fenced(format!(
				"tier operation {} is not rejected for the current owner; it remains fenced",
				ticket.key().op_seq
			)));
		}
		match ticket.command() {
			SlabCommand::Tier(_) => index.hand_off_rejected_tier(&ticket).await?,
			_ => {
				index
					.hand_off_op(ticket.key(), ticket.digest(), None)
					.await?;
			}
		}
		drop(lease);
		self.release_resolved_op_fence().await
	}
}
