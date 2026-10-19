//! Compact, canonical role proofs. No signing, authority factory or persistent active-key cache.
use super::{admin_install::*, codec::hash, types::Digest2};
use ed25519_dalek::{Signature, VerifyingKey};
pub(super) const ROLE_PROOF_BYTES3: usize = 224;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RoleProofError3 {
	Length,
	Magic,
	Version,
	Reserved,
	Role,
	Presence,
	Domain,
}
fn array<const N: usize>(bytes: &[u8], at: usize) -> [u8; N] {
	bytes[at..at + N]
		.try_into()
		.expect("fixed checked role proof")
}
pub(super) fn parse(bytes: &[u8]) -> Result<(), RoleProofError3> {
	use RoleProofError3 as E;
	if bytes.len() != ROLE_PROOF_BYTES3 {
		return Err(E::Length);
	}
	if &bytes[..8] != b"VISROL03" {
		return Err(E::Magic);
	}
	if u16::from_le_bytes(array(bytes, 8)) != 1 {
		return Err(E::Version);
	}
	if bytes[10..12] != [0, 0] {
		return Err(E::Reserved);
	}
	for at in [76, 150] {
		if bytes[at] > 3 {
			return Err(E::Role);
		}
		if bytes[at + 9] > 1 {
			return Err(E::Presence);
		}
		if bytes[at] == 0
			&& (bytes[at + 1..at + 10] != [0; 9] || bytes[at + 10..at + 74] != [0; 64])
		{
			return Err(E::Presence);
		}
		if bytes[at + 9] == 0 && bytes[at + 10..at + 74] != [0; 64] {
			return Err(E::Presence);
		}
	}
	Ok(())
}
/// The bound administrator context's canonical width: generation, cluster, the install LogId
/// (term u64, node u32, index u64), three digests, the initial epoch and the manifest pin.
pub(super) const CONTEXT_BYTES3: usize = 188;
/// The one encoding of an `AdminContext2`: what the role-proof context digest hashes and what the
/// offline context export carries, so an exported context re-derives the exact digest.
pub(super) fn context_bytes(c: &AdminContext2) -> [u8; CONTEXT_BYTES3] {
	let mut b = Vec::with_capacity(CONTEXT_BYTES3);
	b.extend(c.generation);
	b.extend(c.cluster);
	b.extend(c.installed_at.term.to_le_bytes());
	b.extend(c.installed_at.node.to_le_bytes());
	b.extend(c.installed_at.index.to_le_bytes());
	b.extend(c.bootstrap_fingerprint);
	b.extend(c.bootstrap_command);
	b.extend(c.genesis);
	b.extend(c.initial_epoch.to_le_bytes());
	b.extend(c.manifest_pin);
	b.try_into().expect("fixed administrator context width")
}
pub(super) fn context_digest(a: &InstalledAdmin2) -> Digest2 {
	hash(
		b"VISROOT2-ROLE3-CONTEXT\0",
		&context_bytes(&admin_context2(a)),
	)
}
#[derive(Clone, Copy)]
pub(super) enum RoleForm3 {
	Legacy,
	Freeze,
	Unfreeze,
	Retire,
}
fn domain(
	form: RoleForm3,
	expected: [u8; 2],
	slot: usize,
) -> Result<&'static [u8], RoleProofError3> {
	use RoleForm3::*;
	Ok(match (form, expected, slot) {
		(Legacy, [2, 0], 0) => b"VISROOT2-ROLE3-SINGLE-GOVERNANCE\0",
		(Legacy, [3, 0], 0) => b"VISROOT2-ROLE3-SINGLE-RECOVERY\0",
		(Legacy, [1, 2], 0) => b"VISROOT2-ROLE3-JOINT-ADMIN-GOVERNANCE-ADMIN\0",
		(Legacy, [1, 2], 1) => b"VISROOT2-ROLE3-JOINT-ADMIN-GOVERNANCE-GOVERNANCE\0",
		(Freeze, [3, 0], 0) => b"VISROOT2-ROLE3-FREEZE-RECOVERY\0",
		(Unfreeze, [1, 3], 0) => b"VISROOT2-ROLE3-UNFREEZE-ADMIN\0",
		(Unfreeze, [1, 3], 1) => b"VISROOT2-ROLE3-UNFREEZE-RECOVERY-WITH-ADMIN\0",
		(Unfreeze, [2, 3], 0) => b"VISROOT2-ROLE3-UNFREEZE-GOVERNANCE\0",
		(Unfreeze, [2, 3], 1) => b"VISROOT2-ROLE3-UNFREEZE-RECOVERY-WITH-GOVERNANCE\0",
		(Retire, [3, 0], 0) => b"VISROOT2-ROLE3-RETIRE-RECOVERY\0",
		(Retire, [1, 2], 0) => b"VISROOT2-ROLE3-RETIRE-ADMIN-WITH-GOVERNANCE\0",
		(Retire, [1, 2], 1) => b"VISROOT2-ROLE3-RETIRE-GOVERNANCE-WITH-ADMIN\0",
		_ => return Err(RoleProofError3::Domain),
	})
}
pub(super) fn preimage(
	bytes: &[u8],
	expected: [u8; 2],
	slot: usize,
) -> Result<Vec<u8>, RoleProofError3> {
	preimage_for(bytes, expected, slot, RoleForm3::Legacy)
}
pub(super) fn preimage_for(
	bytes: &[u8],
	expected: [u8; 2],
	slot: usize,
	form: RoleForm3,
) -> Result<Vec<u8>, RoleProofError3> {
	parse(bytes)?;
	let mut b = Vec::with_capacity(160);
	b.extend_from_slice(domain(form, expected, slot)?);
	b.extend_from_slice(&bytes[..86]);
	b.extend_from_slice(&bytes[150..160]);
	Ok(b)
}
pub(super) fn verify(
	a: &InstalledAdmin2,
	bytes: &[u8],
	command: Digest2,
	expected: [u8; 2],
) -> Result<bool, RoleProofError3> {
	verify_for(a, bytes, command, expected, RoleForm3::Legacy)
}
pub(super) fn verify_for(
	a: &InstalledAdmin2,
	bytes: &[u8],
	command: Digest2,
	expected: [u8; 2],
	form: RoleForm3,
) -> Result<bool, RoleProofError3> {
	parse(bytes)?;
	if admin_profile2(a) != super::admin::manifest::ManifestProfile2::Three
		|| bytes[12..44] != context_digest(a)
		|| bytes[44..76] != command
	{
		return Ok(false);
	}
	for (i, at) in [76, 150].into_iter().enumerate() {
		if bytes[at] != expected[i] {
			return Ok(false);
		}
		if expected[i] == 0 {
			continue;
		}
		if bytes[at + 9] != 1 {
			return Ok(false);
		}
		let Some((epoch, key)) = active_role3(a, usize::from(expected[i] - 1)) else {
			return Ok(false);
		};
		if u64::from_le_bytes(array(bytes, at + 1)) != epoch {
			return Ok(false);
		}
		if !slot_signed(bytes, expected, i, form, &key)? {
			return Ok(false);
		}
	}
	Ok(true)
}
/// Slot `slot`'s strict signature under `key` over the form's canonical preimage: the one role
/// proof signature check, shared by `verify_for` and the offline compromise audit (which supplies
/// a historical epoch's key). It checks no role, epoch, presence or binding.
pub(super) fn slot_signed(
	bytes: &[u8],
	expected: [u8; 2],
	slot: usize,
	form: RoleForm3,
	key: &Digest2,
) -> Result<bool, RoleProofError3> {
	let Ok(key) = VerifyingKey::from_bytes(key) else {
		return Ok(false);
	};
	let at = if slot == 0 { 76 } else { 150 };
	Ok(key
		.verify_strict(
			&preimage_for(bytes, expected, slot, form)?,
			&Signature::from_bytes(&array(bytes, at + 10)),
		)
		.is_ok())
}
