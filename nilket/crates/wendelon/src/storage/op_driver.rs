//! Drives finished operations to their acknowledgement (ADR 0697 §5.6). It runs
//! from startup, retries after a lost response, and needs no later handoff or
//! election to make progress; it keeps working while new work is refused.

use crate::consensus::slab_ops::OPS_WINDOW_W;
use crate::storage::TierCommitter;
use crate::storage::index::LoadedOp;
use crate::storage::manager::SlabManager;
use std::sync::Arc;

pub struct OpDriver {
	slab: Arc<SlabManager>,
	committer: Arc<dyn TierCommitter>,
}

impl OpDriver {
	pub fn new(slab: Arc<SlabManager>, committer: Arc<dyn TierCommitter>) -> Self {
		Self { slab, committer }
	}

	/// One pass. Only the owner's node as leader can append an Ack, so a
	/// follower waits; a superseded owner resolves nothing, and its Ack is refused.
	pub async fn drive_once(&self) -> std::io::Result<()> {
		let index = self.slab.require_op_index()?;
		// §6 step 4: a follower resolves nothing. With unresolved rows it says so; a
		// clean follower is not made unready by being a follower.
		if !self.committer.should_propose() {
			let unresolved = !index.load_op_intents().await?.is_empty();
			self.slab.note_awaiting_leadership(unresolved);
			return Ok(());
		}
		self.slab.note_awaiting_leadership(false);
		let owner = self.slab.owner_ref()?;
		let mut unresolved = None;
		if let Err(error) = self
			.slab
			.recover_abandoned_marks(self.committer.as_ref())
			.await
		{
			unresolved.get_or_insert(error);
		}
		if let Err(error) = self
			.slab
			.recover_abandoned_removals(self.committer.as_ref())
			.await
		{
			unresolved.get_or_insert(error);
		}
		if let Err(error) = self
			.slab
			.recover_abandoned_installs(self.committer.as_ref())
			.await
		{
			unresolved.get_or_insert(error);
		}
		// Installs have had their pass, so what they held comes next: a snapshot's mark before the
		// tier operations, whose log index may be too old to resolve that mark, which would strand it.
		if let Err(error) = self.slab.retry_deferred_retirements().await {
			unresolved.get_or_insert(error);
		}
		if let Err(error) = self
			.slab
			.recover_abandoned_tier_ops(self.committer.as_ref())
			.await
		{
			unresolved.get_or_insert(error);
		}
		if let Err(error) = self.slab.complete_deferred_open().await {
			unresolved.get_or_insert(error);
		}
		// A role failure stops the pass. One claim's failure does not: it stays
		// fenced and visible while independent work keeps draining, and is reported
		// after. The prefix still stops at its sequence.
		let claims = index.load_rejected_claims().await?;
		if !claims.is_empty() {
			self.committer.preflight(owner).await?;
		}
		for claim in claims {
			// A workflow still resolving this claim owns it.
			let Some(_ownership) = index.try_acquire_ops(claim.op_seqs()) else {
				continue;
			};
			if let Err(error) = self
				.slab
				.resolve_rejected_claim(self.committer.as_ref(), claim)
				.await
			{
				tracing::warn!(%error, "a rejected claim is unresolved");
				unresolved.get_or_insert(error);
			}
		}
		for loaded in index.load_op_intents().await? {
			if let LoadedOp::Submittable(ticket) = loaded {
				self.slab
					.hand_off_unowned_op_if_complete(ticket.key())
					.await?;
			}
		}
		if let Err(error) = self.slab.drive_reserved_ids(self.committer.as_ref()).await {
			unresolved.get_or_insert(error);
		}
		self.slab.release_resolved_op_fence().await?;
		let pending = index.load_ack_pending().await?;
		let unacked: Vec<u64> = pending
			.iter()
			.filter(|(_, acked)| !acked)
			.map(|(seq, _)| *seq)
			.take(OPS_WINDOW_W as usize)
			.collect();
		if !unacked.is_empty() {
			let receipt = self.committer.ack_outcomes(owner, unacked.clone()).await?;
			if !crate::storage::sweeper::ack_receipt_answers(&receipt, owner, &unacked) {
				return Err(std::io::Error::other(format!(
					"an Ack receipt does not answer {owner:?}'s batch: {receipt:?}"
				)));
			}
			// Only what this response accepted; the rest stays and is retried.
			index.mark_ops_acked(receipt.accepted).await?;
		}
		if !pending.is_empty()
			&& let Some(prefix) = self.committer.op_prefix(owner).await?
		{
			index.advance_known_acked_through(prefix).await?;
		}
		if let Some(holder) = index.oldest_op_holder().await? {
			tracing::debug!(
				op_seq = holder.op_seq,
				transition = holder.transition.unwrap_or("awaiting_ack"),
				state = ?holder.state,
				"oldest operation holding the window"
			);
		}
		unresolved.map_or(Ok(()), Err)
	}

	/// Run until the task is aborted. Errors are expected while leadership moves
	/// and are retried.
	pub async fn run(self: Arc<Self>, interval: std::time::Duration) {
		loop {
			tokio::time::sleep(interval).await;
			if let Err(error) = self.drive_once().await {
				tracing::debug!(%error, "operation driver pass did not finish; retrying");
			}
		}
	}
}
