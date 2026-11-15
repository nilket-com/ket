//! ADR 0104 (wendelon): Raft instances and logical slots.
//!
//! A Raft instance id is one durable Raft store's identity: OpenRaft counts
//! votes and progress by it, and it is never issued twice. A slot is the
//! logical member (placement, epoch peers, ring, signer, SAN, address). Below
//! [`ALLOCATED_BASE`] an instance's slot is the instance id itself; from it up,
//! an instance is allocated for a replacement and its slot is recorded here.
//!
//! Every first identity is granted against the birth of the Raft store that
//! asked for it, so a blank store can never take back an identity that was
//! issued to a store which has since been lost.

use super::{DecisionRefusal, DecisionResult, DecisionState};
use crate::consensus::raft_storage::SmState;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// The first allocated instance id. ParentDecisions slots lie in
/// `1..ALLOCATED_BASE`.
pub const ALLOCATED_BASE: u32 = 1 << 31;

/// A Raft store's birth: 128 random bits written by its first page write.
pub type Birth = [u8; 16];

/// How a Raft store's identity was first issued.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Origin {
	Formation,
	Join,
	Replacement,
}

/// What a store's process may run. `ControlOnly` holds until a later
/// explicitly authorized activation: it votes and catches up, and runs no
/// data, signing or reclaim duty.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Role {
	Full,
	ControlOnly,
}

/// A Raft store's durable identity, kept in the same page as its vote.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RaftIdentity {
	pub instance: u32,
	pub origin: Origin,
	pub role: Role,
}

/// Whether `slot` is a valid ParentDecisions slot id.
pub fn is_pd_slot(slot: u32) -> bool {
	slot != 0 && slot < ALLOCATED_BASE
}

/// The replicated allocator of replacement instance ids: exactly
/// `2^31` ids are issued, then it is exhausted; it never wraps.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Allocator {
	Next(u32),
	Exhausted,
}

impl Default for Allocator {
	fn default() -> Self {
		Self::Next(ALLOCATED_BASE)
	}
}

impl Allocator {
	fn issue(&mut self) -> Option<u32> {
		match *self {
			Self::Next(next) => {
				*self = next.checked_add(1).map_or(Self::Exhausted, Self::Next);
				Some(next)
			}
			Self::Exhausted => None,
		}
	}
}

/// What a grant was for.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum GrantKind {
	/// An ordinary join: the instance id is the slot.
	Join,
	/// A replacement of the slot's current instance, under its token.
	Replacement { token: u128 },
}

/// A granted instance. Never pruned.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstanceRecord {
	pub slot: u32,
	pub birth: Birth,
	pub kind: GrantKind,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplacementPhase {
	Begun,
	Reserved { r_new: u32 },
	Done { r_new: u32 },
}

/// One replacement, by its token. Never pruned: a replay after Done answers
/// this record.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Replacement {
	pub slot: u32,
	pub r_old: u32,
	/// The applied uniform voter set at Begin; the swap's target is derived
	/// from it, never from the current configuration.
	pub pre_voters: BTreeSet<u32>,
	pub phase: ReplacementPhase,
}

impl Replacement {
	/// The voters after the swap.
	pub fn target(&self, r_new: u32) -> BTreeSet<u32> {
		let mut target = self.pre_voters.clone();
		target.remove(&self.r_old);
		target.insert(r_new);
		target
	}
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstanceState {
	instances: BTreeMap<u32, InstanceRecord>,
	allocator: Allocator,
	retired: BTreeSet<u32>,
	replacements: BTreeMap<u128, Replacement>,
}

impl InstanceState {
	pub fn record(&self, instance: u32) -> Option<&InstanceRecord> {
		self.instances.get(&instance)
	}
	pub fn replacement(&self, token: u128) -> Option<&Replacement> {
		self.replacements.get(&token)
	}
	pub fn replacements(&self) -> &BTreeMap<u128, Replacement> {
		&self.replacements
	}
	pub fn is_retired(&self, instance: u32) -> bool {
		self.retired.contains(&instance)
	}
	pub fn allocator(&self) -> Allocator {
		self.allocator
	}
	/// The open replacement, if any, with its token.
	pub fn open_replacement(&self) -> Option<(u128, &Replacement)> {
		self.replacements
			.iter()
			.find(|(_, replacement)| !matches!(replacement.phase, ReplacementPhase::Done { .. }))
			.map(|(token, replacement)| (*token, replacement))
	}
	/// The instance reserved by an open replacement, if any.
	pub fn reserved_instance(&self) -> Option<u32> {
		match self.open_replacement()?.1.phase {
			ReplacementPhase::Reserved { r_new } => Some(r_new),
			_ => None,
		}
	}

	/// Validation of a decoded snapshot: every replacement's instances agree
	/// with the instance records and the allocator.
	pub(crate) fn validate(&self) -> Result<(), String> {
		if self
			.replacements
			.values()
			.filter(|replacement| !matches!(replacement.phase, ReplacementPhase::Done { .. }))
			.count() > 1
		{
			return Err("more than one open replacement".into());
		}
		for (instance, record) in &self.instances {
			if !is_pd_slot(record.slot) {
				return Err(format!("instance {instance} records slot {}", record.slot));
			}
			let allocated = *instance >= ALLOCATED_BASE;
			match record.kind {
				GrantKind::Join if allocated || *instance != record.slot => {
					return Err(format!("join grant {instance} is not its slot"));
				}
				GrantKind::Replacement { .. } if !allocated => {
					return Err(format!("replacement grant {instance} is not allocated"));
				}
				GrantKind::Replacement { .. } => match self.allocator {
					Allocator::Next(next) if *instance >= next => {
						return Err(format!("instance {instance} is not yet issued"));
					}
					_ => {}
				},
				GrantKind::Join => {}
			}
		}
		Ok(())
	}
}

/// An allocated instance with no record: a log that cannot be interpreted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UnknownInstance(pub u32);

impl std::fmt::Display for UnknownInstance {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "Raft instance {} has no recorded slot", self.0)
	}
}

/// The one crossing from a Raft instance id to its slot. Today has no
/// allocated instances: every id is its own slot, high bits included.
pub fn slot_of(state: &SmState, instance: u32) -> Result<u32, UnknownInstance> {
	match &state.decisions {
		None => Ok(instance),
		Some(decisions) => slot_in(decisions, instance),
	}
}

pub(crate) fn slot_in(decisions: &DecisionState, instance: u32) -> Result<u32, UnknownInstance> {
	if instance < ALLOCATED_BASE {
		return Ok(instance);
	}
	decisions
		.instances
		.record(instance)
		.map(|record| record.slot)
		.ok_or(UnknownInstance(instance))
}

/// The slots of every current voter and learner. An unmappable instance makes
/// the answer `None`: no slot question is answered from a partial view.
fn served_slots(state: &SmState, decisions: &DecisionState) -> Option<BTreeSet<u32>> {
	state
		.last_membership
		.membership()
		.nodes()
		.map(|(instance, _)| slot_in(decisions, *instance).ok())
		.collect()
}

pub(super) fn grant(
	state: &SmState,
	decisions: &mut DecisionState,
	birth: Birth,
	slot: u32,
	kind: GrantKind,
) -> Result<DecisionResult, DecisionRefusal> {
	let instances = &decisions.instances;
	// Idempotent by birth AND intent: the same store asking for the same thing.
	if let Some((instance, record)) = instances
		.instances
		.iter()
		.find(|(_, record)| record.birth == birth)
	{
		if record.slot == slot && record.kind == kind {
			return Ok(DecisionResult::InstanceGranted {
				instance: *instance,
			});
		}
		return Err(DecisionRefusal::GrantIntentMismatch);
	}
	if !is_pd_slot(slot) {
		return Err(DecisionRefusal::SlotOutOfRange);
	}
	match kind {
		GrantKind::Join => {
			if instances.is_retired(slot) {
				return Err(DecisionRefusal::InstanceRetired);
			}
			if instances
				.open_replacement()
				.is_some_and(|(_, replacement)| replacement.slot == slot)
			{
				return Err(DecisionRefusal::ReplacementOpen);
			}
			if instances
				.instances
				.values()
				.any(|record| record.slot == slot)
			{
				return Err(DecisionRefusal::SlotGranted);
			}
			let served = served_slots(state, decisions).ok_or(DecisionRefusal::SlotServed)?;
			if served.contains(&slot) {
				return Err(DecisionRefusal::SlotServed);
			}
			decisions
				.instances
				.instances
				.insert(slot, InstanceRecord { slot, birth, kind });
			Ok(DecisionResult::InstanceGranted { instance: slot })
		}
		GrantKind::Replacement { token } => {
			let replacement = decisions
				.instances
				.replacements
				.get(&token)
				.ok_or(DecisionRefusal::UnknownReplacement)?;
			if replacement.slot != slot {
				return Err(DecisionRefusal::GrantIntentMismatch);
			}
			match replacement.phase {
				ReplacementPhase::Begun => {}
				// Issued to another birth: never handed out again.
				ReplacementPhase::Reserved { .. } | ReplacementPhase::Done { .. } => {
					return Err(DecisionRefusal::GrantedToAnotherBirth);
				}
			}
			let instance = decisions
				.instances
				.allocator
				.issue()
				.ok_or(DecisionRefusal::InstanceSpaceExhausted)?;
			decisions
				.instances
				.instances
				.insert(instance, InstanceRecord { slot, birth, kind });
			decisions
				.instances
				.replacements
				.get_mut(&token)
				.expect("checked above")
				.phase = ReplacementPhase::Reserved { r_new: instance };
			Ok(DecisionResult::InstanceGranted { instance })
		}
	}
}

pub(super) fn begin_replacement(
	state: &SmState,
	decisions: &mut DecisionState,
	token: u128,
	slot: u32,
) -> Result<DecisionResult, DecisionRefusal> {
	if let Some(existing) = decisions.instances.replacements.get(&token) {
		if existing.slot != slot {
			return Err(DecisionRefusal::GrantIntentMismatch);
		}
		return Ok(recorded(existing));
	}
	if !is_pd_slot(slot) {
		return Err(DecisionRefusal::SlotOutOfRange);
	}
	if decisions.instances.open_replacement().is_some() {
		return Err(DecisionRefusal::ReplacementOpen);
	}
	let membership = state.last_membership.membership();
	if membership.get_joint_config().len() != 1 {
		return Err(DecisionRefusal::MembershipNotUniform);
	}
	let pre_voters: BTreeSet<u32> = membership.voter_ids().collect();
	let mut serving = Vec::new();
	for voter in &pre_voters {
		let voter_slot = slot_in(decisions, *voter).map_err(|_| DecisionRefusal::SlotNotVoter)?;
		if voter_slot == slot {
			serving.push(*voter);
		}
	}
	let [r_old] = serving[..] else {
		return Err(DecisionRefusal::SlotNotVoter);
	};
	if membership.nodes().count() != pre_voters.len() {
		// A learner is present: some other change is in progress.
		return Err(DecisionRefusal::MembershipNotUniform);
	}
	let replacement = Replacement {
		slot,
		r_old,
		pre_voters,
		phase: ReplacementPhase::Begun,
	};
	let result = recorded(&replacement);
	decisions.instances.replacements.insert(token, replacement);
	Ok(result)
}

fn recorded(replacement: &Replacement) -> DecisionResult {
	DecisionResult::ReplacementRecorded {
		slot: replacement.slot,
		r_old: replacement.r_old,
		phase: replacement.phase,
	}
}

/// Done is derived from the applied membership: uniform, exactly the recorded
/// target, and the old instance no longer a node at all.
pub(super) fn complete_replacement(
	state: &SmState,
	decisions: &mut DecisionState,
	token: u128,
) -> Result<DecisionResult, DecisionRefusal> {
	let replacement = decisions
		.instances
		.replacements
		.get(&token)
		.ok_or(DecisionRefusal::UnknownReplacement)?;
	let r_new = match replacement.phase {
		ReplacementPhase::Done { r_new } => {
			return Ok(DecisionResult::ReplacementCompleted { r_new });
		}
		ReplacementPhase::Begun => return Err(DecisionRefusal::ReplacementIncomplete),
		ReplacementPhase::Reserved { r_new } => r_new,
	};
	let membership = state.last_membership.membership();
	let voters: BTreeSet<u32> = membership.voter_ids().collect();
	let nodes: BTreeSet<u32> = membership.nodes().map(|(id, _)| *id).collect();
	if membership.get_joint_config().len() != 1
		|| voters != replacement.target(r_new)
		|| nodes != voters
	{
		return Err(DecisionRefusal::ReplacementIncomplete);
	}
	let r_old = replacement.r_old;
	decisions.instances.retired.insert(r_old);
	decisions
		.instances
		.replacements
		.get_mut(&token)
		.expect("checked above")
		.phase = ReplacementPhase::Done { r_new };
	Ok(DecisionResult::ReplacementCompleted { r_new })
}

#[cfg(test)]
pub(crate) fn exhaust_allocator_for_test(decisions: &mut DecisionState, next: u32) {
	decisions.instances.allocator = Allocator::Next(next);
}

/// The current Raft leader as a dialable slot. This process's own slot is
/// answered only when the leader is this very instance; another instance of
/// this slot, or an allocated instance with no record yet, is no known leader.
/// Today every instance is its own slot, so this is the leader id.
pub(crate) async fn leader_slot(
	raft: &crate::consensus::WendelonRaft,
	state: &crate::consensus::raft_storage::WendelonStateMachine,
	local_slot: u32,
) -> Option<u32> {
	let leader = raft.current_leader().await?;
	let local_instance = raft.metrics().borrow().id;
	// Below the allocated range an instance is its own slot: no state read, so
	// a leader lookup never waits on the state machine's lock.
	let slot = if leader < ALLOCATED_BASE {
		leader
	} else {
		state
			.read_state(|state| slot_of(state, leader).ok())
			.await?
	};
	// Another instance of this very slot is not this process.
	(slot != local_slot || leader == local_instance).then_some(slot)
}
