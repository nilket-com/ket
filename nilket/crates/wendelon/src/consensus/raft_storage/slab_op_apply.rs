//! The state machine's half of ADR 0697 §5.3: admission, decisions and
//! acknowledgement. Admission looks up history first; current-owner admission
//! applies only to a new effect, so a superseded owner's retry still learns what
//! happened, and learns nothing else.

use super::*;
use crate::consensus::slab_ops::{
	AckOutcomes, AckReceipt, BurnOp, CancelOp, ClaimOp, CompleteOp, GcRemove, OPS_WINDOW_W,
	OpEntry, OpKey, OpOutcome, OpReceipt, OpResponse, OwnerTerminal, RecordedOutcome, RetireOp,
	SlabCommand, SlabOp, SlabRejection, TierGeneration, TierOp,
};
use crate::schema::CompactionJobId;

enum Admission {
	/// Rules 1–5: answered with no state change.
	Answer(WendelonResponse),
	/// Rule 6: a new effect by the current owner.
	New(IncarnationRegistry),
}

fn receipt(key: OpKey, digest: [u8; 32], outcome: OpOutcome) -> WendelonResponse {
	WendelonResponse::Op(OpResponse::Receipt(OpReceipt {
		key,
		digest,
		outcome,
	}))
}

/// §5.3 rules 1–5, in order, for an original or a Cancel.
fn admit_op(state: &SmState, key: OpKey, digest: [u8; 32], leader_node: u32) -> Admission {
	let table = state.op_tables.get(&key.owner);
	let acked_through = table.map_or(0, |table| table.acked_through);
	if key.op_seq <= acked_through {
		return Admission::Answer(receipt(key, digest, OpOutcome::Unavailable));
	}
	if let Some(entry) = table.and_then(|table| table.outcomes.get(&key.op_seq)) {
		let outcome = if entry.digest == digest {
			OpOutcome::Recorded(entry.outcome.clone())
		} else {
			OpOutcome::Conflict
		};
		return Admission::Answer(receipt(key, digest, outcome));
	}
	if key.op_seq > acked_through.saturating_add(OPS_WINDOW_W) {
		return Admission::Answer(WendelonResponse::Refused(SlabRefusal::OutsideWindow {
			key,
			acked_through,
		}));
	}
	match admit_slab_owner(state, key.owner, leader_node) {
		Ok(registry) => Admission::New(registry),
		Err(refusal) => Admission::Answer(WendelonResponse::Refused(refusal)),
	}
}

/// Whether `slab`'s identity can still carry a pointer or a new run.
fn require_not_terminal(state: &SmState, slab: SlabRef) -> Result<(), Box<SlabRejection>> {
	let Some(terminal) = state.slab_terminal.get(&slab.owner()) else {
		return Ok(());
	};
	if let Some(job_id) = terminal.claimed.get(&slab.local_id()) {
		return Err(Box::new(SlabRejection::ClaimedBy { job_id: *job_id }));
	}
	if terminal.removed.contains(slab.local_id()) {
		return Err(Box::new(SlabRejection::Removed));
	}
	Ok(())
}

/// Whether `job_id` may start a terminal transition of `slab`. A claim it
/// already holds stands: a mark naming a slab twice is not a conflict with itself.
fn require_claimable(
	state: &SmState,
	slab: SlabRef,
	job_id: CompactionJobId,
) -> Result<(), Box<SlabRejection>> {
	if state.tiered_slabs.contains_key(&slab) {
		return Err(Box::new(SlabRejection::Tiered));
	}
	match require_not_terminal(state, slab) {
		Err(rejection) if *rejection == (SlabRejection::ClaimedBy { job_id }) => Ok(()),
		other => other,
	}
}

fn require_claim_held(
	state: &SmState,
	slab: SlabRef,
	job_id: CompactionJobId,
) -> Result<(), Box<SlabRejection>> {
	let terminal = state.slab_terminal.get(&slab.owner());
	let current = terminal.and_then(|terminal| terminal.claimed.get(&slab.local_id()).copied());
	if current == Some(job_id) {
		return Ok(());
	}
	if terminal.is_some_and(|terminal| terminal.removed.contains(slab.local_id())) {
		return Err(Box::new(SlabRejection::Removed));
	}
	Err(Box::new(SlabRejection::NotClaimedBy { current }))
}

/// The reachable validators, before any side effect and any mutation.
fn validate_command(state: &SmState, command: &SlabCommand) -> Result<(), Box<SlabRejection>> {
	match command {
		SlabCommand::Tier(TierOp { tier, replaces }) => {
			require_not_terminal(state, tier.slab)?;
			let current = state.tiered_slabs.get(&tier.slab).map(TierGeneration::from);
			if current != *replaces {
				return Err(Box::new(SlabRejection::GenerationMismatch {
					expected: replaces.clone(),
					current,
				}));
			}
			Ok(())
		}
		SlabCommand::Retire(RetireOp { slab, expected }) => {
			let Some(current) = state.tiered_slabs.get(slab).map(TierGeneration::from) else {
				return Err(Box::new(SlabRejection::NotTiered));
			};
			if current != *expected {
				return Err(Box::new(SlabRejection::GenerationMismatch {
					expected: Some(expected.clone()),
					current: Some(current),
				}));
			}
			Ok(())
		}
		SlabCommand::Compaction(commit) => {
			if commit.digest_summary.visible_pre != commit.digest_summary.visible_post {
				return Err(Box::new(SlabRejection::VisibleDomainMismatch));
			}
			for slab in &commit.slabs_to_mark_delete_pending {
				require_claimable(state, commit.owner.slab(*slab), commit.job_id)?;
			}
			// A new run names a live identity, never an ended one.
			for run in &commit.slabs_to_add {
				let slab = commit.owner.slab(run.slab_id);
				require_not_terminal(state, slab)?;
				if state.tiered_slabs.contains_key(&slab) {
					return Err(Box::new(SlabRejection::Tiered));
				}
			}
			Ok(())
		}
		SlabCommand::GcRemove(GcRemove { slab, job_id, .. })
		| SlabCommand::Complete(CompleteOp { slab, job_id }) => require_claim_held(state, *slab, *job_id),
		SlabCommand::Claim(ClaimOp { slab, job_id }) => require_claimable(state, *slab, *job_id),
		SlabCommand::BurnReserved(BurnOp { slab }) => {
			if state.tiered_slabs.contains_key(slab) {
				return Err(Box::new(SlabRejection::Tiered));
			}
			require_not_terminal(state, *slab)
		}
	}
}

fn claim(state: &mut SmState, slab: SlabRef, job_id: CompactionJobId) {
	state
		.slab_terminal
		.entry(slab.owner())
		.or_default()
		.claimed
		.insert(slab.local_id(), job_id);
}

/// `slab`'s identity ends. Whatever claim it held ends with it.
fn mark_removed(state: &mut SmState, slab: SlabRef) {
	let terminal: &mut OwnerTerminal = state.slab_terminal.entry(slab.owner()).or_default();
	terminal.claimed.remove(&slab.local_id());
	terminal.removed.insert(slab.local_id());
}

fn apply_valid_command(
	state: &mut SmState,
	op_seq: u64,
	command: SlabCommand,
	registry: IncarnationRegistry,
) -> Option<TierObserverEvent> {
	match command {
		SlabCommand::Tier(TierOp { tier, .. }) => {
			// An identical pointer changes nothing and notifies no one; a
			// metadata refresh of the same generation does both.
			if state.tiered_slabs.get(&tier.slab) == Some(&tier) {
				return None;
			}
			state.tiered_slabs.insert(tier.slab, tier.clone());
			Some(TierObserverEvent::Tiered(tier, registry))
		}
		SlabCommand::Retire(RetireOp { slab, .. }) => {
			state.tiered_slabs.remove(&slab);
			mark_removed(state, slab);
			Some(TierObserverEvent::Retired(slab, registry))
		}
		SlabCommand::Compaction(commit) => {
			for slab in &commit.slabs_to_mark_delete_pending {
				claim(state, commit.owner.slab(*slab), commit.job_id);
			}
			Some(TierObserverEvent::LsmCompactionCommitted(commit, registry))
		}
		SlabCommand::GcRemove(remove) => {
			mark_removed(state, remove.slab);
			Some(TierObserverEvent::GcRemoved(remove, op_seq, registry))
		}
		SlabCommand::Claim(ClaimOp { slab, job_id }) => {
			claim(state, slab, job_id);
			None
		}
		SlabCommand::Complete(CompleteOp { slab, .. })
		| SlabCommand::BurnReserved(BurnOp { slab }) => {
			mark_removed(state, slab);
			None
		}
	}
}

pub(super) fn apply_slab_op(
	state: &mut SmState,
	op: SlabOp,
	index: u64,
	leader_node: u32,
) -> (WendelonResponse, Option<TierObserverEvent>) {
	let key = OpKey {
		owner: op.command.owner(),
		op_seq: op.op_seq,
	};
	let digest = op.command.digest();
	let registry = match admit_op(state, key, digest, leader_node) {
		Admission::Answer(response) => return (response, None),
		Admission::New(registry) => registry,
	};
	let (outcome, event) = match validate_command(state, &op.command) {
		Err(reason) => (
			RecordedOutcome::Rejected {
				reason: *reason,
				index,
			},
			None,
		),
		Ok(()) => (
			RecordedOutcome::Applied { index },
			apply_valid_command(state, op.op_seq, op.command, registry),
		),
	};
	record_outcome(state, key, digest, outcome.clone());
	(receipt(key, digest, OpOutcome::Recorded(outcome)), event)
}

/// The only insertion into a table: rule 6, inside the window.
fn record_outcome(state: &mut SmState, key: OpKey, digest: [u8; 32], outcome: RecordedOutcome) {
	state
		.op_tables
		.entry(key.owner)
		.or_default()
		.outcomes
		.insert(
			key.op_seq,
			OpEntry {
				digest,
				outcome,
				acked: false,
			},
		);
}

pub(super) fn apply_cancel_op(
	state: &mut SmState,
	cancel: CancelOp,
	index: u64,
	leader_node: u32,
) -> WendelonResponse {
	let CancelOp { key, target_digest } = cancel;
	match admit_op(state, key, target_digest, leader_node) {
		Admission::Answer(response) => response,
		Admission::New(_) => {
			let outcome = RecordedOutcome::Rejected {
				reason: SlabRejection::Cancelled,
				index,
			};
			record_outcome(state, key, target_digest, outcome.clone());
			receipt(key, target_digest, OpOutcome::Recorded(outcome))
		}
	}
}

pub(super) fn apply_ack_outcomes(
	state: &mut SmState,
	ack: AckOutcomes,
	leader_node: u32,
) -> WendelonResponse {
	let AckOutcomes { owner, seqs } = ack;
	if seqs.len() as u64 > OPS_WINDOW_W {
		return WendelonResponse::Refused(SlabRefusal::AckBatchTooLarge {
			len: seqs.len() as u64,
		});
	}
	// Ack mutates, so owner admission comes first.
	if let Err(refusal) = admit_slab_owner(state, owner, leader_node) {
		return WendelonResponse::Refused(refusal);
	}
	let mut empty = crate::consensus::slab_ops::OpTable::default();
	let table = state.op_tables.get_mut(&owner).unwrap_or(&mut empty);
	let (accepted, not_decided, outside_window) = table.acknowledge(&seqs);
	WendelonResponse::Op(OpResponse::Ack(AckReceipt {
		owner,
		accepted,
		not_decided,
		outside_window,
		acked_through: table.acked_through,
	}))
}

/// Whether applying `op` would retire a pointer, for the pre-apply hook: a
/// command refused, replayed or rejected never reaches it.
pub(super) fn op_retirement(
	state: &SmState,
	op: &SlabOp,
	leader_node: u32,
) -> Option<(SlabRef, IncarnationRegistry)> {
	let SlabCommand::Retire(RetireOp { slab, .. }) = &op.command else {
		return None;
	};
	let key = OpKey {
		owner: op.command.owner(),
		op_seq: op.op_seq,
	};
	match admit_op(state, key, op.command.digest(), leader_node) {
		Admission::New(registry) if validate_command(state, &op.command).is_ok() => {
			Some((*slab, registry))
		}
		_ => None,
	}
}

/// Snapshot validation for the decision windows (ADR 0697 §5.3).
/// ADR 0697 §4: per owner, tiered, claimed and removed are pairwise disjoint, and
/// `removed` is in its one canonical encoding. Checked for every snapshot state
/// written, decoded or installed.
pub(super) fn validate_slab_terminal(state: &SmState) -> Result<(), String> {
	for (owner, terminal) in &state.slab_terminal {
		let registered = state
			.incarnations
			.get(&owner.node())
			.is_some_and(|registry| {
				registry.current == Some(owner.incarnation())
					|| registry.retired.contains(&owner.incarnation())
			});
		if !registered {
			return Err(format!(
				"snapshot terminal state for {owner:?} names an owner its incarnation registry does not"
			));
		}
		if !terminal.removed.is_canonical() {
			return Err(format!(
				"snapshot removed intervals for {owner:?} are not canonical: {:?}",
				terminal.removed.intervals()
			));
		}
		for slab in terminal.claimed.keys() {
			if terminal.removed.contains(*slab) {
				return Err(format!(
					"snapshot slab {slab} of {owner:?} is both claimed and removed"
				));
			}
			if state.tiered_slabs.contains_key(&owner.slab(*slab)) {
				return Err(format!(
					"snapshot slab {slab} of {owner:?} is both claimed and tiered"
				));
			}
		}
	}
	for slab in state.tiered_slabs.keys() {
		if state
			.slab_terminal
			.get(&slab.owner())
			.is_some_and(|terminal| terminal.removed.contains(slab.local_id()))
		{
			return Err(format!("snapshot slab {slab:?} is both tiered and removed"));
		}
	}
	Ok(())
}

pub(super) fn validate_op_tables(state: &SmState) -> Result<(), String> {
	for (owner, table) in &state.op_tables {
		let registered = state
			.incarnations
			.get(&owner.node())
			.is_some_and(|registry| {
				registry.current == Some(owner.incarnation())
					|| registry.retired.contains(&owner.incarnation())
			});
		if !registered {
			return Err(format!(
				"snapshot op table for {owner:?} names an owner its incarnation registry does not"
			));
		}
		for seq in table.outcomes.keys() {
			if *seq <= table.acked_through || table.outside_window(*seq) {
				return Err(format!(
					"snapshot op table for {owner:?} holds seq {seq} outside its window from {}",
					table.acked_through
				));
			}
		}
	}
	Ok(())
}

#[cfg(test)]
#[path = "slab_op_apply/tests.rs"]
mod tests;
