//! Dormant, monotone retirement. This consumer never creates an authorization token.
use super::{
	codec::*,
	command::*,
	role_epochs::{Retirement3, RoleEpochs3, Seed3},
	state::Root2,
	trust::TrustedEntry2,
	types::*,
	validation::{ROOT_BASE2, nonzero},
};
pub(super) fn is_retired(s: &State2) -> bool {
	s.role_epochs.as_ref().is_some_and(|e| e.retired.is_some())
}
pub(super) fn retire(
	root: &mut Root2,
	at: Log2,
	fingerprint: Digest2,
	command_digest: Digest2,
	seed: Option<Seed3>,
) -> Result2<ApplyResult2> {
	let (original, covered, fp, digest) = seed.ok_or(RootError2::TrustMismatch)?;
	let s = &root.state;
	if (covered, fp, digest) != (s.covered, s.fingerprint, s.command_digest)
		|| s.control_len != super::admin::manifest::ADMIN_MANIFEST_P3_BYTES3 as u64
		|| s.role_epochs
			.as_ref()
			.is_some_and(|e| e.original != original || e.retired.is_some())
	{
		return Err(RootError2::TrustMismatch);
	}
	let delta = if s.role_epochs.is_none() { 204 } else { 84 };
	let history = s
		.derived
		.usage
		.history
		.checked_add(delta)
		.ok_or(RootError2::Overflow)?;
	let total = ROOT_BASE2
		.checked_add(s.derived.usage.held)
		.and_then(|n| n.checked_add(history))
		.and_then(|n| n.checked_add(s.derived.usage.registry))
		.ok_or(RootError2::Overflow)?;
	if history > root.limits.history || total > ROOT_BYTES2 {
		return Ok(ApplyResult2::Denied {
			reason: 11,
			reference: None,
		});
	}
	nonzero(fingerprint)?;
	nonzero(command_digest)?;
	let epochs = root
		.state
		.role_epochs
		.get_or_insert_with(|| Box::new(RoleEpochs3::empty(original)));
	epochs.retired = Some(Retirement3 {
		at,
		fingerprint,
		digest: command_digest,
	});
	Ok(ApplyResult2::Registered(at))
}
/// Exactly one tip-only terminal transition, shared by both reducer paths.
pub(super) fn consume(
	old: &Root2,
	entry: &TrustedEntry2,
	command: &Command2,
) -> Result2<(Root2, ApplyResult2)> {
	let s = &old.state;
	let (g, c, l, fp, d) = entry.fields();
	valid_log(l)?;
	if !is_retired(s)
		|| (g, c) != (s.generation, s.cluster)
		|| command.digest(g, c)? != d
		|| matches!(command, Command2::Bootstrap { .. } | Command2::Cover { .. })
	{
		return Err(RootError2::TrustMismatch);
	}
	if l.index == s.covered.index {
		if (l, fp, d) != (s.covered, s.fingerprint, s.command_digest) {
			return Err(RootError2::TrustMismatch);
		}
		return Ok((old.clone(), ApplyResult2::AlreadyProcessed));
	}
	if s.covered.index.checked_add(1) != Some(l.index) {
		return Err(RootError2::LineageGap);
	}
	let mut next = old.clone();
	next.state.covered = l;
	next.state.fingerprint = fp;
	next.state.command_digest = d;
	super::incremental::cached_capacity(&next.state, next.limits)?;
	Ok((
		next,
		ApplyResult2::Denied {
			reason: 25,
			reference: None,
		},
	))
}

/// Terminal state is independent of the replica's later covered tip.
pub(super) fn terminal_state_digest3(root: &Root2) -> Result2<Digest2> {
	let retired = root
		.state
		.role_epochs
		.as_ref()
		.and_then(|e| e.retired.as_ref())
		.ok_or(RootError2::TrustMismatch)?;
	let mut normalized = root.clone();
	normalized.state.covered = retired.at;
	normalized.state.fingerprint = retired.fingerprint;
	normalized.state.command_digest = retired.digest;
	Ok(hash(b"VISROOT2-TERMINAL-STATE3\0", &normalized.encode()?))
}
