//! Signature findings, independent of the reducer's path. Each signer slot of an entry's proof
//! yields one record of its claimed role and epoch (untrusted bytes), whether its signature
//! verifies under the key the replayed history holds for that claim, and whether that epoch was
//! the active one in the pre-entry state. Verification uses each form's production preimage and
//! the same strict Ed25519 check; the role-proof check is the production function itself. A
//! signature counts only when its embedded context and command digest bind this root and this
//! entry: a proof replayed from elsewhere is never attributed here. Neither the reducer's effect
//! nor `active_at_entry` ever decides `crypto_valid`.
use super::trace::History2;
use crate::storage::mutation_visibility::root2::admin_install::{InstalledAdmin2, admin_context2};
use crate::storage::mutation_visibility::root2::admin_statement::{
	ADMIN_STATEMENT_BYTES2, EPOCH_STATEMENT_BYTES3, StatementFacts2, admin_statement_preimage2,
	encode_admin_statement2, epoch_statement_preimage3, inspect_admin_statement2,
	inspect_epoch_statement3,
};
use crate::storage::mutation_visibility::root2::command::Command2;
use crate::storage::mutation_visibility::root2::committed::GOVERNANCE_SIDECAR_BYTES2;
use crate::storage::mutation_visibility::root2::governance_command::{
	governance_command_preimage2, parse_governance_command2,
};
use crate::storage::mutation_visibility::root2::role_command::{
	ROLE_PROOF_BYTES3, RoleForm3, context_digest, parse, slot_signed,
};
use crate::storage::mutation_visibility::root2::types::Digest2;
use ed25519_dalek::{Signature, VerifyingKey};

/// The proof form a claim came from.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Form2 {
	/// `VISROL03`: explicit role and epoch per slot.
	RoleProof,
	/// `VISADG01`: governance at epoch 0 by its verifier rule.
	Governance,
	/// `VISADS02`: the legacy administrator statement, admin at epoch 0.
	Statement,
	/// `VISADS03`: an explicit administrator epoch.
	EpochStatement,
}
/// Why a present claim is not attributed.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Invalid2 {
	/// The slot names a role but carries no signature.
	Unsigned,
	/// The embedded context or command digest is not this root's or this entry's.
	Binding,
	/// The history holds no key for the claimed role and epoch.
	UnknownEpoch,
	/// No signing domain exists for the claimed role pair under this command's form.
	Domain,
	/// Strict verification under the historical key failed.
	Signature,
}
/// Why an entry yields no claim at all.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Unattributable2 {
	NoSidecar,
	Unparseable,
	NoRole,
	/// The form is not signed by an administrator role (Bootstrap is pin-authorized; Reserve and
	/// Finalize are signed by enrolled authorities).
	NoRoleForm,
}
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) struct Claim2 {
	pub(super) role: u8,
	pub(super) epoch: u64,
	pub(super) form: Form2,
	pub(super) slot: u8,
	pub(super) crypto_valid: bool,
	pub(super) active_at_entry: bool,
	pub(super) invalid: Option<Invalid2>,
}
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Record2 {
	Claim(Claim2),
	Unattributable(Unattributable2),
}

/// Every record of one entry. `admin` is the installed administrator view (the root's context),
/// absent only before the install entry; `history` is the pre-entry key history.
pub(super) fn records2(
	command: &Command2,
	sidecar: Option<&[u8]>,
	admin: Option<&InstalledAdmin2>,
	history: &History2,
	digest: Digest2,
) -> Vec<Record2> {
	use Unattributable2 as U;
	let one = |u| vec![Record2::Unattributable(u)];
	let Some(admin) = admin else {
		return one(U::NoRoleForm);
	};
	match command {
		Command2::Bootstrap { .. }
		| Command2::Cover { .. }
		| Command2::Reserve(_)
		| Command2::Finalize { .. } => one(U::NoRoleForm),
		Command2::AdmissionPermission(bytes) => statement(bytes, admin, history),
		Command2::EpochAdmissionPermission(bytes) => epoch_statement(bytes, admin, history),
		Command2::RotateAdmin(_)
		| Command2::RotateGovernance(_)
		| Command2::RotateRecovery(_)
		| Command2::Freeze(_)
		| Command2::Unfreeze(_)
		| Command2::RetireRoot => match sidecar {
			None => one(U::NoSidecar),
			Some(bytes) => role_proof(bytes, form_of(command), admin, history, digest),
		},
		Command2::EnrollSelector(_)
		| Command2::EnrollAuthority { .. }
		| Command2::RevokeAuthority(_)
		| Command2::Grant { .. }
		| Command2::Release { .. }
		| Command2::Cut { .. }
		| Command2::Resolve { .. }
		| Command2::EnrollLeaseIssuer { .. }
		| Command2::RevokeLeaseIssuer { .. } => match sidecar {
			None => one(U::NoSidecar),
			Some(bytes) if bytes.len() == ROLE_PROOF_BYTES3 => {
				role_proof(bytes, RoleForm3::Legacy, admin, history, digest)
			}
			Some(bytes) if bytes.len() == GOVERNANCE_SIDECAR_BYTES2 => {
				governance(bytes, admin, history, digest)
			}
			Some(_) => one(U::Unparseable),
		},
	}
}
/// The signing-domain family production verifies this command's role proof under.
fn form_of(command: &Command2) -> RoleForm3 {
	match command {
		Command2::RetireRoot => RoleForm3::Retire,
		Command2::Freeze(_) => RoleForm3::Freeze,
		Command2::Unfreeze(_) => RoleForm3::Unfreeze,
		_ => RoleForm3::Legacy,
	}
}
fn u64_at(bytes: &[u8], at: usize) -> u64 {
	let mut b = [0u8; 8];
	b.copy_from_slice(&bytes[at..at + 8]);
	u64::from_le_bytes(b)
}
fn claim(
	role: u8,
	epoch: u64,
	form: Form2,
	slot: u8,
	history: &History2,
	invalid: Option<Invalid2>,
) -> Record2 {
	Record2::Claim(Claim2 {
		role,
		epoch,
		form,
		slot,
		crypto_valid: invalid.is_none(),
		active_at_entry: history.active(role) == Some(epoch),
		invalid,
	})
}
/// Strict Ed25519 under the historical key, exactly as the production verifiers call it.
fn strict(key: &Digest2, preimage: &[u8], signature: &[u8]) -> bool {
	let Ok(signature) = <[u8; 64]>::try_from(signature) else {
		return false;
	};
	VerifyingKey::from_bytes(key).is_ok_and(|key| {
		key.verify_strict(preimage, &Signature::from_bytes(&signature))
			.is_ok()
	})
}
fn role_proof(
	bytes: &[u8],
	form: RoleForm3,
	admin: &InstalledAdmin2,
	history: &History2,
	digest: Digest2,
) -> Vec<Record2> {
	if parse(bytes).is_err() {
		return vec![Record2::Unattributable(Unattributable2::Unparseable)];
	}
	let claimed = [bytes[76], bytes[150]];
	let bound = bytes[12..44] == context_digest(admin) && bytes[44..76] == digest;
	let mut out = Vec::new();
	for (slot, at) in [(0u8, 76usize), (1, 150)] {
		let role = bytes[at];
		if role == 0 {
			continue;
		}
		let epoch = u64_at(bytes, at + 1);
		let invalid = if bytes[at + 9] != 1 {
			Some(Invalid2::Unsigned)
		} else if !bound {
			Some(Invalid2::Binding)
		} else {
			match history.key(role, epoch) {
				None => Some(Invalid2::UnknownEpoch),
				Some(key) => match slot_signed(bytes, claimed, usize::from(slot), form, &key) {
					Ok(true) => None,
					Ok(false) => Some(Invalid2::Signature),
					Err(_) => Some(Invalid2::Domain),
				},
			}
		};
		out.push(claim(role, epoch, Form2::RoleProof, slot, history, invalid));
	}
	if out.is_empty() {
		out.push(Record2::Unattributable(Unattributable2::NoRole));
	}
	out
}
fn governance(
	bytes: &[u8],
	admin: &InstalledAdmin2,
	history: &History2,
	digest: Digest2,
) -> Vec<Record2> {
	let (Ok((context, embedded, signature)), Ok(fixed)) = (
		parse_governance_command2(bytes),
		<&[u8; GOVERNANCE_SIDECAR_BYTES2]>::try_from(bytes),
	) else {
		return vec![Record2::Unattributable(Unattributable2::Unparseable)];
	};
	let invalid = if context != admin_context2(admin) || embedded != digest {
		Some(Invalid2::Binding)
	} else {
		match (history.key(2, 0), governance_command_preimage2(fixed)) {
			(Some(key), Ok(preimage)) if strict(&key, &preimage, &signature) => None,
			_ => Some(Invalid2::Signature),
		}
	};
	vec![claim(2, 0, Form2::Governance, 0, history, invalid)]
}
/// The statement's context and permission key, re-encoded under this root's context, equal its
/// own bytes: the region both statement forms share, before their epoch and signature tails.
fn statement_bound(bytes: &[u8], facts: &StatementFacts2, admin: &InstalledAdmin2) -> bool {
	let signed = ADMIN_STATEMENT_BYTES2 - 64;
	encode_admin_statement2(facts.action, &facts.key, &admin_context2(admin), &[0; 64])
		.is_ok_and(|expected| expected[8..signed] == bytes[8..signed])
}
fn statement(
	bytes: &[u8; ADMIN_STATEMENT_BYTES2],
	admin: &InstalledAdmin2,
	history: &History2,
) -> Vec<Record2> {
	let Ok(facts) = inspect_admin_statement2(bytes) else {
		return vec![Record2::Unattributable(Unattributable2::Unparseable)];
	};
	let invalid = if !statement_bound(bytes, &facts, admin) {
		Some(Invalid2::Binding)
	} else {
		match (history.key(1, 0), admin_statement_preimage2(bytes)) {
			(Some(key), Ok(preimage))
				if strict(&key, &preimage, &bytes[ADMIN_STATEMENT_BYTES2 - 64..]) =>
			{
				None
			}
			_ => Some(Invalid2::Signature),
		}
	};
	vec![claim(1, 0, Form2::Statement, 0, history, invalid)]
}
fn epoch_statement(
	bytes: &[u8; EPOCH_STATEMENT_BYTES3],
	admin: &InstalledAdmin2,
	history: &History2,
) -> Vec<Record2> {
	let Ok(facts) = inspect_epoch_statement3(bytes) else {
		return vec![Record2::Unattributable(Unattributable2::Unparseable)];
	};
	let epoch = u64_at(bytes, ADMIN_STATEMENT_BYTES2 - 64);
	let invalid = if !statement_bound(bytes, &facts, admin) {
		Some(Invalid2::Binding)
	} else {
		match (history.key(1, epoch), epoch_statement_preimage3(bytes)) {
			(None, _) => Some(Invalid2::UnknownEpoch),
			(Some(key), Ok(preimage))
				if strict(&key, &preimage, &bytes[EPOCH_STATEMENT_BYTES3 - 64..]) =>
			{
				None
			}
			_ => Some(Invalid2::Signature),
		}
	};
	vec![claim(1, epoch, Form2::EpochStatement, 0, history, invalid)]
}
