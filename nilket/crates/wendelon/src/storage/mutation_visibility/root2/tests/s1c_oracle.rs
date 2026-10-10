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
// Test-only profile projection at the frozen-oracle boundary; never a production migration. The
// walk is structural and bounded, refuses a non-empty permission section, and otherwise keeps every
// body byte (including any mutation under test); only the count word, magic and version change, and
// the envelope checksum is recomputed for that deliberate transform alone.
fn permission_count_at(body: &[u8]) -> crate::storage::mutation_visibility::root2::Result2<usize> {
	use crate::storage::mutation_visibility::root2::types::ROOT_BYTES2;
	let mut r = codec::Read2::new(body);
	r.take(212)?;
	for _ in 0..2 {
		let n = r.count(4096, 4)?;
		for _ in 0..n {
			r.blob(ROOT_BYTES2)?;
		}
	}
	Ok(body.len() - r.remaining().len())
}
pub(super) fn legacy_profile2(
	bytes: &[u8],
) -> crate::storage::mutation_visibility::root2::Result2<Vec<u8>> {
	use crate::storage::mutation_visibility::root2::types::{ROOT_BYTES2, RootError2};
	let body = codec::open_versioned(bytes, b"VISAUT03", 3, ROOT_BYTES2)?;
	let at = permission_count_at(body)?;
	if body.get(at..at + 4) != Some(&[0u8; 4][..]) {
		return Err(RootError2::Codec);
	}
	codec::envelope(
		b"VISAUT02",
		&[&body[..at], &body[at + 4..]].concat(),
		ROOT_BYTES2,
	)
}
pub(super) fn profile3(
	bytes: &[u8],
) -> crate::storage::mutation_visibility::root2::Result2<Vec<u8>> {
	use crate::storage::mutation_visibility::root2::types::ROOT_BYTES2;
	let body = codec::open(bytes, b"VISAUT02", ROOT_BYTES2)?;
	let at = permission_count_at(body)?;
	codec::envelope_versioned(
		b"VISAUT03",
		3,
		&[&body[..at], &[0u8; 4][..], &body[at..]].concat(),
		ROOT_BYTES2,
	)
}
