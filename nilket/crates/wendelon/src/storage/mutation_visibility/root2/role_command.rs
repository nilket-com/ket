//! Compact, canonical role proofs. No signing, authority factory or persistent active-key cache.
use super::admin::manifest::ManifestProfile2;
use super::command::Command2;
use super::role_view::RoleView3;
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
/// The inverse of `context_bytes`: every field is fixed width, so any 188 bytes are one context.
pub(super) fn context_from_bytes(b: &[u8; CONTEXT_BYTES3]) -> AdminContext2 {
	AdminContext2 {
		generation: array(b, 0),
		cluster: array(b, 16),
		installed_at: super::types::Log2 {
			term: u64::from_le_bytes(array(b, 32)),
			node: u32::from_le_bytes(array(b, 40)),
			index: u64::from_le_bytes(array(b, 44)),
		},
		bootstrap_fingerprint: array(b, 52),
		bootstrap_command: array(b, 84),
		genesis: array(b, 116),
		initial_epoch: u64::from_le_bytes(array(b, 148)),
		manifest_pin: array(b, 156),
	}
}
pub(super) fn context_digest(a: &InstalledAdmin2) -> Digest2 {
	context_digest3(&admin_context2(a))
}
/// The one role-proof context digest: the domain over exactly `context_bytes`.
pub(super) fn context_digest3(c: &AdminContext2) -> Digest2 {
	hash(b"VISROOT2-ROLE3-CONTEXT\0", &context_bytes(c))
}
/// What a containment proof binds in place of a command digest (activation §8.2, D-21): one root
/// (`RootKey`), one node, one boot of it (its fresh 16-byte nonce) and the requested state. BLAKE3
/// over the domain and the fields, through the crate's BLAKE3 boundary. Shared by the offline
/// facade and, later, the node's containment check.
pub(super) fn containment_digest3(
	root_key: &Digest2,
	node_id: u32,
	boot_nonce: &[u8; 16],
	on: bool,
) -> Digest2 {
	let mut h = crate::hash::blake3::Context::new();
	h.update(b"wendelon/root2/containment-elect/v1\0");
	h.update(root_key);
	h.update(&node_id.to_le_bytes());
	h.update(boot_nonce);
	h.update(&[u8::from(on)]);
	h.finish()
}
#[derive(Clone, Copy)]
pub(super) enum RoleForm3 {
	Legacy,
	Freeze,
	Unfreeze,
	Retire,
	/// The recovery role's process-local containment override (D-21): never an on-log form.
	/// `role_requirement` never yields it; only the offline facade and the node's containment
	/// check name it.
	Containment,
}
/// The role pair and form a proof for `command` must carry, given the proof's slot-0 role, which
/// alone separates RetireRoot `[3,0]` from `[1,2]` and Unfreeze `[1,3]` from `[2,3]`. `None` for
/// a command no role proof authorizes. Shared by `verify_command2` and the offline facade.
pub(super) fn role_requirement(command: &Command2, slot0_role: u8) -> Option<([u8; 2], RoleForm3)> {
	let expected = match command {
		Command2::RetireRoot if slot0_role == 3 => [3, 0],
		Command2::RetireRoot => [1, 2],
		Command2::RotateRecovery(_) => [1, 2],
		Command2::Unfreeze(_) if slot0_role == 1 => [1, 3],
		Command2::Unfreeze(_) => [2, 3],
		Command2::RotateAdmin(_) | Command2::RotateGovernance(_) | Command2::Freeze(_) => [3, 0],
		_ => return None,
	};
	let form = match command {
		Command2::RetireRoot => RoleForm3::Retire,
		Command2::Freeze(_) => RoleForm3::Freeze,
		Command2::Unfreeze(_) => RoleForm3::Unfreeze,
		_ => RoleForm3::Legacy,
	};
	Some((expected, form))
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
		(Containment, [3, 0], 0) => b"VISROOT2-ROLE3-CONTAINMENT-RECOVERY\0",
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
/// The installed administrator's check: a thin wrapper over `verify_core` on its view.
pub(super) fn verify_for(
	a: &InstalledAdmin2,
	bytes: &[u8],
	command: Digest2,
	expected: [u8; 2],
	form: RoleForm3,
) -> Result<bool, RoleProofError3> {
	verify_core(&a.view(), bytes, command, expected, form)
}
/// The slot loop: profile 3, the view's context and this command bound, then each expected slot
/// present at its role's active epoch and signed by that epoch's key.
pub(super) fn verify_core(
	view: &RoleView3,
	bytes: &[u8],
	command: Digest2,
	expected: [u8; 2],
	form: RoleForm3,
) -> Result<bool, RoleProofError3> {
	parse(bytes)?;
	if view.profile() != ManifestProfile2::Three
		|| bytes[12..44] != context_digest3(&view.context())
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
		let Some((epoch, key)) = view.active(usize::from(expected[i] - 1)) else {
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
/// One slot as written: role (0 empty), epoch, presence, signature.
pub(super) type RoleSlot3 = (u8, u64, bool, [u8; 64]);
/// The canonical `VISROL03` bytes for these bindings and slots; refuses exactly what `parse`
/// refuses. The one production layout of a role proof.
pub(super) fn encode_role_proof3(
	context: Digest2,
	command: Digest2,
	slots: [RoleSlot3; 2],
) -> Result<[u8; ROLE_PROOF_BYTES3], RoleProofError3> {
	let mut b = [0; ROLE_PROOF_BYTES3];
	b[..8].copy_from_slice(b"VISROL03");
	b[8..10].copy_from_slice(&1u16.to_le_bytes());
	b[12..44].copy_from_slice(&context);
	b[44..76].copy_from_slice(&command);
	for ((role, epoch, present, signature), at) in slots.into_iter().zip([76, 150]) {
		b[at] = role;
		b[at + 1..at + 9].copy_from_slice(&epoch.to_le_bytes());
		b[at + 9] = u8::from(present);
		b[at + 10..at + 74].copy_from_slice(&signature);
	}
	parse(&b)?;
	Ok(b)
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
