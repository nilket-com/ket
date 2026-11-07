//! The manager's half of operation evidence (ADR 0697 §5): which generation a
//! proposal names, and when an operation's local effects are durable enough to
//! hand it off. An Ack never precedes that.

use super::*;
use crate::consensus::slab_ops::{GcRemove, RunIdentity, SlabCommand, TierGeneration};
use crate::storage::index::{LoadedOp, OpTicket};
use crate::storage::sweeper::{SlabOpRejected, VerifiedReceipt};

/// A delete-pending run in a ring with no removal reserved. It has no claim behind
/// it, so it stays fenced, and it never keeps a sweep from valid reserved work.
#[derive(Debug)]
pub(super) struct GcRemoveUnreserved;

impl std::fmt::Display for GcRemoveUnreserved {
	fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		formatter.write_str("a delete-pending run has no reserved removal; it remains fenced")
	}
}

impl std::error::Error for GcRemoveUnreserved {}

pub(super) fn is_gc_remove_unreserved(error: &Error) -> bool {
	error
		.get_ref()
		.is_some_and(|cause| cause.is::<GcRemoveUnreserved>())
}

/// A GC removal reserved with its mark, in the phase a reopen found it.
pub(super) struct GcRemoveReservation {
	loaded: LoadedOp,
	pub(super) job_id: crate::schema::CompactionJobId,
	/// The exact input authority its mark left, which it must still find. `None`
	/// only for a removal allocated on its own, with no mark behind it.
	pub(super) expected_inputs: Option<Vec<crate::storage::index::LsmInputAuthority>>,
}

impl GcRemoveReservation {
	pub(super) fn awaits_its_workflow(&self) -> bool {
		self.loaded.awaits_its_workflow()
	}

	pub(super) fn key(&self) -> crate::consensus::slab_ops::OpKey {
		match &self.loaded {
			LoadedOp::Submittable(ticket) => ticket.key(),
			LoadedOp::Reserved(reserved) => reserved.key(),
		}
	}

	pub(super) fn digest(&self) -> [u8; 32] {
		match &self.loaded {
			LoadedOp::Submittable(ticket) => ticket.digest(),
			LoadedOp::Reserved(reserved) => reserved.digest(),
		}
	}

	/// The removal command, as it was built when its mark was allocated.
	pub(super) fn remove_command(&self) -> std::io::Result<GcRemove> {
		let command = match &self.loaded {
			LoadedOp::Submittable(ticket) => ticket.command(),
			LoadedOp::Reserved(reserved) => reserved.command(),
		};
		match command {
			SlabCommand::GcRemove(remove) => Ok(remove.clone()),
			other => Err(Error::other(format!("not a GC removal: {other:?}"))),
		}
	}

	/// The durable transition before a reserved removal's first submission.
	pub(super) async fn into_ticket(
		self,
		index: &PersistentLocalIndex,
	) -> std::io::Result<OpTicket> {
		match self.loaded {
			LoadedOp::Submittable(ticket) => Ok(ticket),
			LoadedOp::Reserved(reserved) => index.mark_may_be_submitted(reserved).await,
		}
	}
}

/// A submitted tier or retire operation, or a removal past its local disposal,
/// whose linked intent went with the run: a restart has not re-established that
/// phase's authority, reserved or not. Any other LSM operation has its linked
/// intent fencing its own scope, and any other reserved one was never sent.
fn op_needs_receipt(op: &LoadedOp) -> bool {
	use crate::storage::index::OpProgress;
	if matches!(
		op.progress(),
		Some(OpProgress::RunDropped(_) | OpProgress::PurgeAuthorized(_) | OpProgress::Purged(_))
	) {
		return true;
	}
	matches!(op, LoadedOp::Submittable(ticket) if matches!(
		ticket.command(),
		SlabCommand::Tier(_) | SlabCommand::Retire(_)
	))
}

impl SlabManager {
	pub(crate) fn require_op_index(&self) -> std::io::Result<&PersistentLocalIndex> {
		self.persistent_index.as_deref().ok_or_else(|| {
			Error::new(
				ErrorKind::WouldBlock,
				"operations need the durable storage index",
			)
		})
	}

	/// A reopen with undecided tier or retire operations fences until each has a
	/// verified Applied receipt and a durable effect. An intermediate state:
	/// recovery of the rest is ADR 0697 §6.
	pub(super) async fn fence_unresolved_ops_at_open(&self) -> std::io::Result<()> {
		if let Some(index) = self.persistent_index.as_deref() {
			let unresolved = index.load_op_intents().await?.iter().any(op_needs_receipt);
			self.tier_authority
				.unresolved_op_fence
				.store(unresolved, Ordering::Release);
			let retained = index.load_rejected_tier_candidates().await?;
			if !retained.is_empty() {
				tracing::warn!(
					count = retained.len(),
					oldest_op = retained[0].op_seq,
					oldest_uri = %retained[0].l2_uri,
					"rejected tier candidates remain uploaded and unreferenced"
				);
			}
		}
		Ok(())
	}

	/// Lift the reopen fence once no such operation remains.
	pub(crate) async fn release_resolved_op_fence(&self) -> std::io::Result<()> {
		if self
			.tier_authority
			.unresolved_op_fence
			.load(Ordering::Acquire)
			&& !self
				.require_op_index()?
				.load_op_intents()
				.await?
				.iter()
				.any(op_needs_receipt)
		{
			self.tier_authority
				.unresolved_op_fence
				.store(false, Ordering::Release);
			self.tier_authority.notify_publication_change();
		}
		Ok(())
	}

	/// Whether an Applied operation's local effects are durable (ADR 0697 §6.1,
	/// Addendum A1). A fenced or deferred apply does not qualify. Only the tier
	/// and retire transitions resolve here; each LSM site owns its own handoff.
	pub(crate) async fn op_effect_is_durable(&self, command: &SlabCommand) -> bool {
		match command {
			SlabCommand::Tier(op) => {
				let id = op.tier.slab.local_id();
				self.unapplied_tier_pointers.get(&id).is_none()
					&& self.tier_pointer(id).await.is_some_and(|pointer| {
						TierGeneration::from(&pointer) == TierGeneration::from(&op.tier)
					})
			}
			SlabCommand::Retire(op) => {
				let id = op.slab.local_id();
				self.unapplied_tier_pointers.get(&id).is_none()
					&& self.tier_pointer(id).await.is_none()
			}
			SlabCommand::Compaction(_)
			| SlabCommand::GcRemove(_)
			| SlabCommand::Claim(_)
			| SlabCommand::Complete(_)
			| SlabCommand::BurnReserved(_) => false,
		}
	}

	/// Hand an operation off if it is decided Applied for a store that is still
	/// its node's current incarnation and its effects are durable (ADR 0697 §5.5).
	/// A historical receipt alone is not authority. The canonical lease holds the
	/// entry and the registry in one read and keeps supersession from applying
	/// until this handoff's transaction has ended, whatever happens to its caller,
	/// so an Intent is never erased under obsolete authority. If not, its intent
	/// stays and the driver resolves it.
	pub(crate) async fn hand_off_op_if_complete(&self, ticket: &OpTicket) -> std::io::Result<()> {
		let index = self.require_op_index()?;
		// Current-owner evidence does not restore missing cleanup evidence. This
		// also covers the sticky owner fence.
		if self.require_cleanup_authority().is_err() {
			return Ok(());
		}
		// A manager outside a state machine has only its committer's word.
		let lease = match self.canonical_tier_source.get() {
			Some(source) => Some(source.read().await?),
			None => None,
		};
		if let Some(lease) = &lease
			&& !lease.op_applied_for_current_owner(ticket.key(), ticket.digest())
		{
			return Ok(());
		}
		if self.op_effect_is_durable(ticket.command()).await {
			#[cfg(test)]
			self.pause_op_handoff_for_test().await;
			// The transaction owns the lease: an aborted caller cannot release
			// it while the blocking commit is still to come.
			index
				.hand_off_op_under(ticket.key(), ticket.digest(), Box::new(lease))
				.await?;
		}
		Ok(())
	}

	/// `hand_off_op_if_complete` for a pass over the intents, which owns none of them. A workflow
	/// still working an operation hands it off itself, and one that finished after the scan
	/// already has, so holding it this reads the row as it stands.
	pub(crate) async fn hand_off_unowned_op_if_complete(
		&self,
		key: crate::consensus::slab_ops::OpKey,
	) -> std::io::Result<()> {
		let index = self.require_op_index()?;
		let Some(_ownership) = index.try_acquire_ops(vec![key.op_seq]) else {
			return Ok(());
		};
		let Some(LoadedOp::Submittable(ticket)) = index
			.load_op_intents()
			.await?
			.into_iter()
			.find(|op| op.key() == key)
		else {
			return Ok(());
		};
		self.hand_off_op_if_complete(&ticket).await
	}

	/// GC has no undo for a definite rejection yet, so its intent stays and
	/// fences until exact pre-state restoration exists (Addendum A1).
	pub(super) fn require_gc_applied(receipt: VerifiedReceipt) -> std::io::Result<()> {
		match receipt {
			VerifiedReceipt::Applied { .. } => Ok(()),
			VerifiedReceipt::Rejected { reason, .. } => {
				Err(Error::new(ErrorKind::WouldBlock, SlabOpRejected(reason)))
			}
		}
	}

	/// The removal reserved with this run's mark, or a new one for a run marked
	/// by another path.
	pub(super) async fn gc_remove_reservation_or_allocate(
		&self,
		index: &PersistentLocalIndex,
		run: &crate::storage::LsmRunSummary,
	) -> std::io::Result<GcRemoveReservation> {
		if let Some(reservation) = self.gc_remove_reservation(index, run.slab_id).await? {
			return Ok(reservation);
		}
		// In a ring, every delete-pending run came through a mark, which reserved
		// its removal; one with no reservation has no claim behind it.
		if self.control_node_bound() {
			return Err(Error::new(ErrorKind::WouldBlock, GcRemoveUnreserved));
		}
		let job_id = Self::lsm_gc_job_id(run.manifest_epoch, 0xD2, &[run.slab_id]);
		let allocation = index
			.allocate_ops(
				vec![SlabCommand::GcRemove(GcRemove {
					job_id,
					slab: self.owner_ref()?.slab(run.slab_id),
					expected_run: RunIdentity::of(run),
				})],
				None,
			)
			.await?;
		Ok(GcRemoveReservation {
			loaded: LoadedOp::Submittable(allocation.first),
			job_id,
			expected_inputs: None,
		})
	}

	/// Find the removal reserved with `slab_id`'s mark.
	async fn gc_remove_reservation(
		&self,
		index: &PersistentLocalIndex,
		slab_id: SlabId,
	) -> std::io::Result<Option<GcRemoveReservation>> {
		for loaded in index.load_op_intents().await? {
			let command = match &loaded {
				LoadedOp::Submittable(ticket) => ticket.command(),
				LoadedOp::Reserved(reserved) => reserved.command(),
			};
			if let SlabCommand::GcRemove(GcRemove { slab, job_id, .. }) = command
				&& slab.local_id() == slab_id
			{
				let job_id = *job_id;
				let expected_inputs = loaded.reserved_inputs().map(<[_]>::to_vec);
				return Ok(Some(GcRemoveReservation {
					loaded,
					job_id,
					expected_inputs,
				}));
			}
		}
		Ok(None)
	}

	/// An LSM operation failed before it was ever sent. Close its sequence
	/// through the ring by its own digest, then clear its intent with the
	/// handoff. Any other answer leaves both in place, fenced.
	pub(super) async fn cancel_unsubmitted_lsm_op(
		&self,
		index: &PersistentLocalIndex,
		committer: &dyn crate::storage::TierCommitter,
		ticket: &OpTicket,
		intent: &crate::storage::index::LsmInstallIntent,
	) -> std::io::Result<()> {
		use crate::consensus::slab_ops::SlabRejection;
		match committer.cancel_op(ticket).await? {
			VerifiedReceipt::Rejected {
				reason: SlabRejection::Cancelled,
				..
			} => {
				index
					.hand_off_op(ticket.key(), ticket.digest(), Some(intent.clone()))
					.await?;
				self.clear_registered_lsm_intent(intent.commit.job_id).await;
				Ok(())
			}
			other => Err(Error::new(
				ErrorKind::WouldBlock,
				crate::storage::sweeper::OpReceiptAnomaly(format!(
					"a never-sent op {:?} did not cancel: {other:?}",
					ticket.key()
				)),
			)),
		}
	}

	/// Apply a decided GC removal to this store's manifest, by the exact run it
	/// was reserved against. The reservation carries no epoch, so unrelated
	/// manifest changes do not invalidate it; a different run fences.
	pub(crate) async fn observe_lsm_gc_removed(
		&self,
		remove: &GcRemove,
		op_seq: u64,
		index: u64,
	) -> std::io::Result<()> {
		let _gate = self.persistent_index_gate.clone().read_owned().await;
		if self.observer_is_superseded(Some(index)) {
			return Ok(());
		}
		if self.snapshot_install_in_flight() {
			self.index_authority_unavailable
				.store(true, Ordering::Release);
			return Err(Error::new(
				ErrorKind::WouldBlock,
				"snapshot installation fences LSM observer",
			));
		}
		// §6.1: the local work precedes Complete, so the run is already gone and
		// nothing is left to do here. Only this operation's own durable evidence
		// explains an absent run, and a present one is unexplained.
		let store = self.require_op_index()?;
		let evidence = store
			.op_evidence(op_seq, SlabCommand::GcRemove(remove.clone()).digest())
			.await?;
		use crate::storage::index::{OpEvidence, OpProgress};
		let absent = self
			.compaction_manifest_snapshot()
			.run(remove.slab.local_id())
			.is_none();
		// An obsolete duplicate: this sequence is below the acknowledged prefix, so
		// no row says which operation it was. Ignored, and it proves nothing: this
		// branch authorizes no effect and clears no fence. A present run falls
		// through and is unexplained.
		if absent && evidence == OpEvidence::BelowPrefix {
			return Ok(());
		}
		// This exact operation's own durable state, past its local disposal.
		let explained = match evidence {
			OpEvidence::Intent(Some(progress)) => matches!(
				progress,
				OpProgress::RunDropped(_) | OpProgress::PurgeAuthorized(_) | OpProgress::Purged(_)
			),
			OpEvidence::Finished => true,
			_ => false,
		};
		if !(explained && absent) {
			self.index_authority_unavailable
				.store(true, Ordering::Release);
			return Err(Error::new(
				ErrorKind::InvalidData,
				"a decided GC removal does not match this store's local progress",
			));
		}
		Ok(())
	}
}

/// Fixtures that drive a ring directly still allocate through the store, so a
/// later production allocation never collides with their sequences, and hand
/// off at once, so they never hold its window. Their entries are real
/// operations; the local bookkeeping around them is what is shortened.
#[cfg(test)]
impl SlabManager {
	/// The generation a fixture's entry replaces: committed state where it can be
	/// read now, else this store's mirror.
	async fn tier_generation_to_replace(&self, slab_id: SlabId) -> Option<TierGeneration> {
		let pointer = match self.try_canonical_tier_pointer(slab_id) {
			Some(committed) => committed,
			None => self.tier_pointer(slab_id).await,
		};
		pointer.as_ref().map(TierGeneration::from)
	}

	async fn op_request_for_test(&self, command: SlabCommand) -> crate::consensus::WendelonRequest {
		let index = self.require_op_index().unwrap();
		let ticket = index
			.allocate_ops(vec![command.clone()], None)
			.await
			.unwrap()
			.first;
		index
			.hand_off_op(ticket.key(), ticket.digest(), None)
			.await
			.unwrap();
		crate::consensus::WendelonRequest::SlabOp(crate::consensus::slab_ops::SlabOp {
			op_seq: ticket.key().op_seq,
			command,
		})
	}

	/// A tier operation replacing whatever generation this store can see.
	pub(crate) async fn tier_request_for_test(
		&self,
		tier: crate::consensus::TieredSlab,
	) -> crate::consensus::WendelonRequest {
		let replaces = self.tier_generation_to_replace(tier.slab.local_id()).await;
		self.op_request_for_test(SlabCommand::Tier(crate::consensus::slab_ops::TierOp {
			tier,
			replaces,
		}))
		.await
	}

	/// A retirement of the generation this store can see.
	pub(crate) async fn retire_request_for_test(
		&self,
		slab: crate::consensus::SlabRef,
	) -> crate::consensus::WendelonRequest {
		let expected = self
			.tier_generation_to_replace(slab.local_id())
			.await
			.unwrap_or_else(|| TierGeneration {
				l2_uri: String::new(),
				payload_hash: [0; 32],
				is_encrypted: false,
				tenant_id: None,
				tenant_kek_id: None,
				stats_format_version: None,
				stats_block_len: None,
				stats_block_hash128: None,
			});
		self.op_request_for_test(SlabCommand::Retire(crate::consensus::slab_ops::RetireOp {
			slab,
			expected,
		}))
		.await
	}
}
