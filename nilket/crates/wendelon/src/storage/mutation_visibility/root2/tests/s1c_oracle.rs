//! Frozen S1 legacy oracle from 8c649102: module/import adaptation only.
#![allow(dead_code)]
use crate::storage::mutation_visibility::root2::{codec, command, trust};
mod reducer;
pub(super) mod state;
mod types;
mod validation;

pub(super) fn install(
	bytes: &[u8],
) -> crate::storage::mutation_visibility::root2::Result2<state::Root2> {
	let u = state::UnverifiedRoot2::decode(bytes)?;
	let (g, c, l, fp) = u.fixture_fields();
	u.fixture_install(
		g,
		c,
		l,
		fp,
		crate::storage::mutation_visibility::root2::codec::hash(b"", bytes),
	)
}
pub(super) fn reduce(
	bytes: &[u8],
	limits: crate::storage::mutation_visibility::root2::types::Limits2,
	entry: &trust::TrustedEntry2,
	verified: &trust::VerifiedCommand2,
	command: &command::Command2,
) -> crate::storage::mutation_visibility::root2::Result2<(Vec<u8>, command::ApplyResult2)> {
	let root = install(bytes)?.lower_limits(limits);
	let (next, outcome) = state::Root2::reduce(Some(&root), entry, verified, command)?;
	Ok((next.encode()?, outcome))
}
pub(super) fn bootstrap(
	e: &trust::TrustedEntry2,
	v: &trust::VerifiedCommand2,
	c: &command::Command2,
) -> crate::storage::mutation_visibility::root2::Result2<(Vec<u8>, command::ApplyResult2)> {
	let (n, o) = state::Root2::reduce(None, e, v, c)?;
	Ok((n.encode()?, o))
}
