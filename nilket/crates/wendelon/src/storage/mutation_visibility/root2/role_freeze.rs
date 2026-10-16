//! Dormant control freeze. Existing grants, leases and write proofs remain effective.
use super::{
	command::{ApplyResult2, Command2},
	role_epochs::{RoleEpochs3, Seed3},
	state::Root2,
	types::*,
	validation::ROOT_BASE2,
};

pub(super) fn blocked(state: &State2, command: &Command2) -> bool {
	let class = match command {
		Command2::EnrollSelector(_)
		| Command2::EnrollAuthority { .. }
		| Command2::RevokeAuthority(_)
		| Command2::EnrollLeaseIssuer { .. }
		| Command2::RevokeLeaseIssuer { .. }
		| Command2::Grant { .. }
		| Command2::Release { .. }
		| Command2::Cut { .. }
		| Command2::Resolve { .. }
		| Command2::AdmissionPermission(_)
		| Command2::EpochAdmissionPermission(_) => true,
		Command2::Bootstrap { .. }
		| Command2::Cover { .. }
		| Command2::Reserve(_)
		| Command2::Finalize { .. }
		| Command2::RotateAdmin(_)
		| Command2::RotateGovernance(_)
		| Command2::RotateRecovery(_)
		| Command2::Freeze(_)
		| Command2::Unfreeze(_) => false,
	};
	class
		&& state
			.role_epochs
			.as_ref()
			.is_some_and(|epochs| epochs.frozen)
}

pub(super) fn transition(
	root: &mut Root2,
	command: &Command2,
	at: Log2,
	seed: Option<Seed3>,
) -> Result2<ApplyResult2> {
	let (original, covered, fingerprint, digest) = seed.ok_or(RootError2::TrustMismatch)?;
	let state = &root.state;
	if (covered, fingerprint, digest) != (state.covered, state.fingerprint, state.command_digest)
		|| state.control_len != super::admin::manifest::ADMIN_MANIFEST_P3_BYTES3 as u64
		|| state
			.role_epochs
			.as_ref()
			.is_some_and(|epochs| epochs.original != original)
	{
		return Err(RootError2::TrustMismatch);
	}
	let zero = Log2 {
		term: 0,
		node: 0,
		index: 0,
	};
	let (frozen, previous) = state
		.role_epochs
		.as_ref()
		.map_or((false, zero), |e| (e.frozen, e.transition));
	let next_frozen = match command {
		Command2::Freeze(binding) if !frozen && *binding == previous => true,
		Command2::Unfreeze(binding) if frozen && *binding == previous => false,
		Command2::Freeze(_) | Command2::Unfreeze(_) => {
			return Ok(ApplyResult2::Denied {
				reason: 24,
				reference: None,
			});
		}
		_ => return Err(RootError2::TrustMismatch),
	};
	let delta = if state.role_epochs.is_none() { 120 } else { 0 };
	let history = state
		.derived
		.usage
		.history
		.checked_add(delta)
		.ok_or(RootError2::Overflow)?;
	let total = ROOT_BASE2
		.checked_add(state.derived.usage.held)
		.and_then(|n| n.checked_add(history))
		.and_then(|n| n.checked_add(state.derived.usage.registry))
		.ok_or(RootError2::Overflow)?;
	if history > root.limits.history || total > ROOT_BYTES2 {
		return Ok(ApplyResult2::Denied {
			reason: 11,
			reference: None,
		});
	}
	let epochs = root
		.state
		.role_epochs
		.get_or_insert_with(|| Box::new(RoleEpochs3::empty(original)));
	epochs.frozen = next_frozen;
	epochs.transition = at;
	Ok(ApplyResult2::Registered(at))
}
