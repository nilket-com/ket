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
// walk is structural and bounded, refuses non-empty issuer and permission sections, and otherwise keeps every
// body byte (including any mutation under test); only the two count words, magic and version change, and
// the envelope checksum is recomputed for that deliberate transform alone.
fn registry_extension_at(
	body: &[u8],
) -> crate::storage::mutation_visibility::root2::Result2<usize> {
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
	let body = codec::open_versioned(bytes, b"VISAUT04", 4, ROOT_BYTES2)?;
	let at = registry_extension_at(body)?;
	if body.get(at..at + 8) != Some(&[0u8; 8][..]) {
		return Err(RootError2::Codec);
	}
	codec::envelope(
		b"VISAUT02",
		&[&body[..at], &body[at + 8..]].concat(),
		ROOT_BYTES2,
	)
}
/// D-G2, test-only: the S1c comparison projection of a profile-3 root. The frozen oracle has no
/// permission section, so the section and its registry charge (117 bytes per record) are stripped;
/// `control_len` passes through as an opaque scalar. The rest is the strict `legacy_profile2`.
/// Callers compare only steps with no `Denied(18)` or `Denied(19)`, the two permission refusals.
pub(super) fn s1c_projection(
	bytes: &[u8],
) -> crate::storage::mutation_visibility::root2::Result2<Vec<u8>> {
	use crate::storage::mutation_visibility::root2::types::{ROOT_BYTES2, RootError2};
	let body = codec::open_versioned(bytes, b"VISAUT04", 4, ROOT_BYTES2)?;
	let at = registry_extension_at(body)?;
	let mut r = codec::Read2::new(&body[at..]);
	let issuers = r.take(4)?;
	let n = r.count(4096, 117)?;
	for _ in 0..n {
		if r.blob(113)?.len() != 113 {
			return Err(RootError2::Codec);
		}
	}
	let mut stripped = [&body[..at], issuers, &[0u8; 4][..], r.remaining()].concat();
	let charge = 117 * n as u64;
	let registry = u64::from_le_bytes(stripped[204..212].try_into().unwrap())
		.checked_sub(charge)
		.ok_or(RootError2::RootInvariant)?;
	stripped[204..212].copy_from_slice(&registry.to_le_bytes());
	legacy_profile2(&codec::envelope_versioned(
		b"VISAUT04",
		4,
		&stripped,
		ROOT_BYTES2,
	)?)
}
pub(super) fn profile3(
	bytes: &[u8],
) -> crate::storage::mutation_visibility::root2::Result2<Vec<u8>> {
	use crate::storage::mutation_visibility::root2::types::ROOT_BYTES2;
	let body = codec::open(bytes, b"VISAUT02", ROOT_BYTES2)?;
	let at = registry_extension_at(body)?;
	codec::envelope_versioned(
		b"VISAUT04",
		4,
		&[&body[..at], &[0u8; 8][..], &body[at..]].concat(),
		ROOT_BYTES2,
	)
}
