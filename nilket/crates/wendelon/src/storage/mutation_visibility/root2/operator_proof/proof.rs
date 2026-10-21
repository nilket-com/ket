//! Unsigned layouts and preimages per proof form, signature splicing, the self-check and the
//! joint-proof merge. Every layout comes from a production encoder, every preimage from the
//! production preimage function, and every check from the production checkers under the view a
//! context-export reader built. Nothing here signs: a signature arrives as 64 bytes.
use super::context::VerifiedContext;
use crate::storage::mutation_visibility::root2::admin::key::check_enrollable_admin_key2;
use crate::storage::mutation_visibility::root2::admin_statement::{
	AdminAction2, AdmissionNamespace2, PermissionKey2, admin_statement_preimage2,
	check_admin_statement2, check_epoch_statement3, encode_admin_statement2,
	encode_epoch_statement3, epoch_statement_preimage3,
};
use crate::storage::mutation_visibility::root2::command::Command2;
use crate::storage::mutation_visibility::root2::governance_command::{
	GovernanceAction2, check_governance_command2, encode_governance_command2,
	governance_command_preimage2,
};
use crate::storage::mutation_visibility::root2::role_command::{
	ROLE_PROOF_BYTES3, RoleForm3, RoleSlot3, containment_digest3, context_digest3,
	context_from_bytes, encode_role_proof3, parse, preimage_for, role_requirement, slot_signed,
	verify_core,
};
use crate::storage::mutation_visibility::root2::role_epochs::SUCCESSORS3;
use crate::storage::mutation_visibility::root2::role_view::RoleView3;
use crate::storage::mutation_visibility::root2::types::{AuthorityRef2, Digest2};

/// Why the facade refuses; nothing is produced with any of these.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProofRefusal {
	/// Not a canonical `Command2` envelope.
	Command,
	/// The command's (generation, cluster) is not the context's.
	Deployment,
	/// Bootstrap (the pin), Reserve and Finalize (authority keys) and Cover: no role key signs.
	NotRoleSigned,
	/// An admission statement: built by `prepare_statement`, never as a sidecar.
	StatementCommand,
	/// `VISADC01` (no production caller) or an unknown form.
	NoProductionForm,
	/// A governance command needs exactly one sidecar form; no other command takes a choice.
	SidecarChoice,
	/// The partner role is missing, not needed, or not the requirement's other role.
	WithChoice,
	KeyNotInHistory,
	StaleEpoch,
	/// The requirement does not name the key's role.
	RoleNotNamed,
	/// The governance role's action mask lacks the command's action.
	ActionNotPermitted,
	/// `VISADG01` after governance has rotated (a consumed denial).
	GovernanceRotated,
	/// `VISADS02` after the administrator has rotated (a consumed denial).
	AdminRotated,
	/// The context shows the root retired.
	Retired,
	/// The context shows the root frozen, which consumes a statement or governance command.
	Frozen,
	/// Statement fields the encoder refuses.
	Statement,
	/// A partial or merge input is not the layout rebuilt from (command, context).
	Layout,
	/// Two merge inputs differ outside the two signature areas.
	Mismatch,
	/// A partial or merge input fills no slot, or both.
	SlotFill,
	/// A slot is filled twice: two inputs fill the same slot, or a partial fills the signer's.
	Overwritten,
	/// A filled slot does not verify under the history key of its claimed (role, epoch).
	SlotSignature,
	/// A merge or partial of a one-slot form.
	NotJoint,
	/// The assembled bytes do not pass the production checkers.
	SelfCheck,
	/// A new role key that enrollment refuses (a consumed `Denied(20)` as a rotation).
	NotEnrollable,
	/// A new role key already in the root's history (a consumed `Denied(21)` as a rotation).
	KeyReused,
	/// A role already at its successor limit (a consumed `Denied(22)` as a rotation).
	SuccessorLimit,
}
use ProofRefusal as P;

enum Output {
	Sidecar,
	Statement([u8; 16], [u8; 16]),
	EpochStatement([u8; 16], [u8; 16]),
}
/// What one offline key is asked to sign: the unsigned output layout and the exact preimage.
pub struct Unsigned {
	layout: Vec<u8>,
	preimage: Vec<u8>,
	signature_at: usize,
	output: Output,
}
impl Unsigned {
	/// The output bytes with this signer's signature still zero.
	pub fn layout(&self) -> &[u8] {
		&self.layout
	}
	/// The exact bytes the key signs.
	pub fn preimage(&self) -> &[u8] {
		&self.preimage
	}
}

/// The signature area of each role-proof slot.
const SLOT_SIGNATURE: [std::ops::Range<usize>; 2] = [86..150, 160..224];

fn decoded(context: &VerifiedContext, command: &[u8]) -> Result<(Command2, Digest2), P> {
	let (g, c, decoded) = Command2::decode(command).map_err(|_| P::Command)?;
	let bound = context_from_bytes(context.context());
	if (g, c) != (bound.generation, bound.cluster) {
		return Err(P::Deployment);
	}
	let digest = decoded.digest(g, c).map_err(|_| P::Command)?;
	Ok((decoded, digest))
}
/// The governance action of the nine governance kinds; `None` for every other kind.
fn governance_action(command: &Command2) -> Option<GovernanceAction2> {
	Some(match command {
		Command2::EnrollSelector(_) => GovernanceAction2::EnrollSelector,
		Command2::EnrollAuthority { .. } => GovernanceAction2::EnrollAuthority,
		Command2::RevokeAuthority(_) => GovernanceAction2::RevokeAuthority,
		Command2::Grant { .. } => GovernanceAction2::Grant,
		Command2::Release { .. } => GovernanceAction2::Release,
		Command2::Cut { .. } => GovernanceAction2::Cut,
		Command2::Resolve { .. } => GovernanceAction2::Resolve,
		Command2::EnrollLeaseIssuer { .. } => GovernanceAction2::EnrollLeaseIssuer,
		Command2::RevokeLeaseIssuer { .. } => GovernanceAction2::RevokeLeaseIssuer,
		_ => return None,
	})
}
/// The signer's role, which must be at its active epoch, and a refusal once retired.
fn signer_role(context: &VerifiedContext, signer: &[u8; 32]) -> Result<u8, P> {
	let (role, epoch) = context.identity(signer).ok_or(P::KeyNotInHistory)?;
	if context.active()[usize::from(role - 1)].0 != epoch {
		return Err(P::StaleEpoch);
	}
	if context.retired().refuses_as_retired() {
		return Err(P::Retired);
	}
	Ok(role)
}
/// The unsigned `VISROL03` layout for `expected`: each named role at its active epoch, present,
/// with a zero signature.
fn role_layout(
	context: &VerifiedContext,
	digest: Digest2,
	expected: [u8; 2],
) -> [u8; ROLE_PROOF_BYTES3] {
	let active = context.active();
	let slot = |role: u8| -> RoleSlot3 {
		match role {
			0 => (0, 0, false, [0; 64]),
			r => (r, active[usize::from(r - 1)].0, true, [0; 64]),
		}
	};
	encode_role_proof3(
		context_digest3(&context_from_bytes(context.context())),
		digest,
		[slot(expected[0]), slot(expected[1])],
	)
	.expect("a canonical unsigned role proof")
}
fn filled(bytes: &[u8], slot: usize) -> bool {
	bytes[SLOT_SIGNATURE[slot].clone()] != [0; 64]
}
/// Equal outside both signature areas.
fn same_unsigned(a: &[u8], b: &[u8]) -> bool {
	a.len() == b.len()
		&& (0..a.len())
			.filter(|i| !SLOT_SIGNATURE.iter().any(|r| r.contains(i)))
			.all(|i| a[i] == b[i])
}
/// A proof input that fills exactly one slot of `layout`, whose signature verifies with the one
/// production check under the history key of the slot's claimed (role, epoch). Returns the slot.
fn one_signed_slot(
	context: &VerifiedContext,
	layout: &[u8],
	input: &[u8],
	expected: [u8; 2],
	form: RoleForm3,
) -> Result<usize, P> {
	if input.len() != ROLE_PROOF_BYTES3 || parse(input).is_err() || !same_unsigned(layout, input) {
		return Err(P::Layout);
	}
	let slot = match [filled(input, 0), filled(input, 1)] {
		[true, false] => 0,
		[false, true] => 1,
		_ => return Err(P::SlotFill),
	};
	let at = [76, 150][slot];
	let epoch = u64::from_le_bytes(input[at + 1..at + 9].try_into().expect("epoch width"));
	let key = context.key_at(input[at], epoch).ok_or(P::SlotSignature)?;
	if slot_signed(input, expected, slot, form, &key) != Ok(true) {
		return Err(P::SlotSignature);
	}
	Ok(slot)
}

/// The unsigned output for `signer` (a key of the verified history) on `command`:
/// - a role command (rotations, Freeze, Unfreeze, RetireRoot) takes a `VISROL03` slot chosen by
///   the key's role; `with` names the partner role for RetireRoot `[1,2]` and Unfreeze, and a
///   second signer passes the first signer's `partial`, checked against the rebuilt layout;
/// - a governance command takes `sidecar` `VISADG01` (governance epoch 0) or `VISROL03` `[2,0]`.
pub fn prepare(
	context: &VerifiedContext,
	command: &[u8],
	signer: &[u8; 32],
	with: Option<u8>,
	sidecar: Option<&[u8; 8]>,
	partial: Option<&[u8]>,
) -> Result<Unsigned, P> {
	let (decoded, digest) = decoded(context, command)?;
	let role = signer_role(context, signer)?;
	let (layout, expected, form, slot) = if role_requirement(&decoded, 0).is_some() {
		if sidecar.is_some_and(|s| s != b"VISROL03") {
			return Err(P::SidecarChoice);
		}
		let (expected, form) = role_requirement(&decoded, with.map_or(role, |w| w.min(role)))
			.ok_or(P::NotRoleSigned)?;
		let choice =
			matches!(decoded, Command2::RetireRoot | Command2::Unfreeze(_)) && expected[1] != 0;
		if choice != with.is_some() || with.is_some_and(|w| w == role || !expected.contains(&w)) {
			return Err(P::WithChoice);
		}
		let slot = expected
			.iter()
			.position(|r| *r == role)
			.ok_or(P::RoleNotNamed)?;
		(
			role_layout(context, digest, expected).to_vec(),
			expected,
			form,
			slot,
		)
	} else if let Some(action) = governance_action(&decoded) {
		if context.frozen().refuses_while_frozen() {
			return Err(P::Frozen);
		}
		if with.is_some() {
			return Err(P::WithChoice);
		}
		if role != 2 {
			return Err(P::RoleNotNamed);
		}
		match sidecar {
			Some(b"VISROL03") => {
				if !context.view().governs(action) {
					return Err(P::ActionNotPermitted);
				}
				let layout = role_layout(context, digest, [2, 0]).to_vec();
				(layout, [2, 0], RoleForm3::Legacy, 0)
			}
			Some(b"VISADG01") => {
				if partial.is_some() {
					return Err(P::NotJoint);
				}
				if context.active()[1].0 != 0 {
					return Err(P::GovernanceRotated);
				}
				let bound = context_from_bytes(context.context());
				let layout = encode_governance_command2(&bound, digest, &[0; 64])
					.map_err(|_| P::Deployment)?;
				let preimage = governance_command_preimage2(&layout).map_err(|_| P::Deployment)?;
				return Ok(Unsigned {
					layout: layout.to_vec(),
					preimage,
					signature_at: 232,
					output: Output::Sidecar,
				});
			}
			Some(b"VISADC01") => return Err(P::NoProductionForm),
			_ => return Err(P::SidecarChoice),
		}
	} else if matches!(
		decoded,
		Command2::AdmissionPermission(_) | Command2::EpochAdmissionPermission(_)
	) {
		return Err(P::StatementCommand);
	} else {
		return Err(P::NotRoleSigned);
	};
	let layout = match partial {
		None => layout,
		Some(_) if expected[1] == 0 => return Err(P::NotJoint),
		Some(partial) => {
			if one_signed_slot(context, &layout, partial, expected, form)? == slot {
				return Err(P::Overwritten);
			}
			partial.to_vec()
		}
	};
	let preimage = preimage_for(&layout, expected, slot, form).map_err(|_| P::NotRoleSigned)?;
	Ok(Unsigned {
		layout,
		preimage,
		signature_at: SLOT_SIGNATURE[slot].start,
		output: Output::Sidecar,
	})
}

/// The unsigned administrator statement for `signer` (the active administrator key): `form`
/// `VISADS02` (administrator epoch 0 only) or `VISADS03` (any epoch, carried explicitly), granting
/// (`action` 1) or revoking (2) the authority's permission in the administrator's namespace.
pub fn prepare_statement(
	context: &VerifiedContext,
	signer: &[u8; 32],
	form: &[u8; 8],
	action: u8,
	authority: &[u8; 16],
	revision: u64,
) -> Result<Unsigned, P> {
	if signer_role(context, signer)? != 1 {
		return Err(P::RoleNotNamed);
	}
	if context.frozen().refuses_while_frozen() {
		return Err(P::Frozen);
	}
	let action = match action {
		1 => AdminAction2::Grant,
		2 => AdminAction2::Revoke,
		_ => return Err(P::Statement),
	};
	let bound = context_from_bytes(context.context());
	let ns = context.view().namespace();
	let key = PermissionKey2 {
		authority: AuthorityRef2 {
			id: *authority,
			revision,
		},
		namespace: AdmissionNamespace2 {
			generation: bound.generation,
			tenant: ns.tenant,
			scope: ns.scope,
			table: ns.table,
			schema: ns.schema,
		},
	};
	let (g, c) = (bound.generation, bound.cluster);
	let epoch = context.active()[0].0;
	match form {
		b"VISADS02" => {
			if epoch != 0 {
				return Err(P::AdminRotated);
			}
			let layout = encode_admin_statement2(action, &key, &bound, &[0; 64])
				.map_err(|_| P::Statement)?;
			Ok(Unsigned {
				preimage: admin_statement_preimage2(&layout).map_err(|_| P::Statement)?,
				layout: layout.to_vec(),
				signature_at: 273,
				output: Output::Statement(g, c),
			})
		}
		b"VISADS03" => {
			let layout = encode_epoch_statement3(action, &key, &bound, epoch, &[0; 64])
				.map_err(|_| P::Statement)?;
			Ok(Unsigned {
				preimage: epoch_statement_preimage3(&layout).map_err(|_| P::Statement)?,
				layout: layout.to_vec(),
				signature_at: 281,
				output: Output::EpochStatement(g, c),
			})
		}
		_ => Err(P::NoProductionForm),
	}
}

/// The output with `signature` spliced in: the sidecar, or for a statement the whole canonical
/// `Command2` envelope with the signature embedded.
pub fn splice(unsigned: &Unsigned, signature: &[u8; 64]) -> Result<Vec<u8>, P> {
	let mut b = unsigned.layout.clone();
	b[unsigned.signature_at..unsigned.signature_at + 64].copy_from_slice(signature);
	match unsigned.output {
		Output::Sidecar => Ok(b),
		Output::Statement(g, c) => {
			Command2::AdmissionPermission(b.try_into().map_err(|_| P::Statement)?)
				.encode(g, c)
				.map_err(|_| P::Statement)
		}
		Output::EpochStatement(g, c) => {
			Command2::EpochAdmissionPermission(b.try_into().map_err(|_| P::Statement)?)
				.encode(g, c)
				.map_err(|_| P::Statement)
		}
	}
}

/// Runs the production checkers on `command` and its `sidecar` under the reader's view, exactly as
/// the node's verifier would on the pre-entry state the context describes.
pub fn self_check(
	context: &VerifiedContext,
	command: &[u8],
	sidecar: Option<&[u8]>,
) -> Result<(), P> {
	let (decoded, digest) = decoded(context, command)?;
	check_under(context.view(), &decoded, digest, sidecar)
}
/// The self-check under one view; the facade passes only the reader's.
pub(in crate::storage::mutation_visibility::root2) fn check_under(
	view: &RoleView3,
	command: &Command2,
	digest: Digest2,
	sidecar: Option<&[u8]>,
) -> Result<(), P> {
	let accepted = match (command, sidecar) {
		(Command2::AdmissionPermission(s), None) => check_admin_statement2(view, s).is_ok(),
		(Command2::EpochAdmissionPermission(s), None) => check_epoch_statement3(view, s).is_ok(),
		(_, Some(s)) if role_requirement(command, 0).is_some() => {
			s.len() == ROLE_PROOF_BYTES3
				&& role_requirement(command, s[76]).is_some_and(|(expected, form)| {
					verify_core(view, s, digest, expected, form) == Ok(true)
				})
		}
		(_, Some(s)) => match governance_action(command) {
			Some(action) if s.len() == ROLE_PROOF_BYTES3 => {
				parse(s).is_ok()
					&& view.governs(action)
					&& verify_core(view, s, digest, [2, 0], RoleForm3::Legacy) == Ok(true)
			}
			Some(action) => check_governance_command2(view, action, s, digest).is_ok(),
			None => return Err(P::NotRoleSigned),
		},
		_ => false,
	};
	if accepted { Ok(()) } else { Err(P::SelfCheck) }
}

/// Two partial `VISROL03` proofs of one joint command, one per host: refused unless both are the
/// layout rebuilt from (command, context) outside the two signature areas, each fills exactly one
/// slot (different ones), and each slot verifies under the history key of its claimed (role,
/// epoch). The merged proof passes `self_check` before it is returned.
pub fn merge(context: &VerifiedContext, command: &[u8], a: &[u8], b: &[u8]) -> Result<Vec<u8>, P> {
	let (decoded, digest) = decoded(context, command)?;
	if a.len() != ROLE_PROOF_BYTES3 || parse(a).is_err() {
		return Err(P::Layout);
	}
	if !same_unsigned(a, b) {
		return Err(P::Mismatch);
	}
	let (expected, form) = role_requirement(&decoded, a[76]).ok_or(P::NotRoleSigned)?;
	if expected[1] == 0 {
		return Err(P::NotJoint);
	}
	let layout = role_layout(context, digest, expected);
	let first = one_signed_slot(context, &layout, a, expected, form)?;
	let second = one_signed_slot(context, &layout, b, expected, form)?;
	if first == second {
		return Err(P::Overwritten);
	}
	let mut merged = a.to_vec();
	merged[SLOT_SIGNATURE[second].clone()].copy_from_slice(&b[SLOT_SIGNATURE[second].clone()]);
	self_check(context, command, Some(&merged))?;
	Ok(merged)
}

/// Whether `key` may become a new key of `role` (1 admin, 2 governance, 3 recovery): enrollable,
/// and, under a verified `context`, neither anywhere in that root's history nor beyond the role's
/// successor limit; the rotation's own rules, checked before the key is ever proposed.
pub fn check_new_role_key(
	context: Option<&VerifiedContext>,
	role: u8,
	key: &[u8; 32],
) -> Result<(), P> {
	if !(1..=3).contains(&role) {
		return Err(P::RoleNotNamed);
	}
	check_enrollable_admin_key2(key).map_err(|_| P::NotEnrollable)?;
	let Some(context) = context else {
		return Ok(());
	};
	if context.identity(key).is_some() {
		return Err(P::KeyReused);
	}
	if context
		.history()
		.iter()
		.filter(|(r, ..)| *r == role)
		.count()
		>= SUCCESSORS3
	{
		return Err(P::SuccessorLimit);
	}
	Ok(())
}

/// The unsigned containment proof for `signer`, the active recovery key (D-21): a `VISROL03`
/// layout binding the context digest and, in place of a command digest, the containment digest
/// of (`root_key`, `node_id`, `boot_nonce`, `on`), with recovery present in slot 0 at its active
/// epoch. Never an on-log proof: no command selects this form. A frozen root does not refuse it
/// (containment is incident response); a retired root does.
pub fn prepare_containment(
	context: &VerifiedContext,
	signer: &[u8; 32],
	root_key: &[u8; 32],
	node_id: u32,
	boot_nonce: &[u8; 16],
	on: bool,
) -> Result<Unsigned, P> {
	if signer_role(context, signer)? != 3 {
		return Err(P::RoleNotNamed);
	}
	let digest = containment_digest3(root_key, node_id, boot_nonce, on);
	let layout = role_layout(context, digest, [3, 0]).to_vec();
	let preimage =
		preimage_for(&layout, [3, 0], 0, RoleForm3::Containment).map_err(|_| P::NotRoleSigned)?;
	Ok(Unsigned {
		layout,
		preimage,
		signature_at: SLOT_SIGNATURE[0].start,
		output: Output::Sidecar,
	})
}

/// The node's containment check under the reader's view: `proof` is a recovery proof in the
/// containment domain over the context digest and the containment digest of exactly these inputs.
pub fn self_check_containment(
	context: &VerifiedContext,
	root_key: &[u8; 32],
	node_id: u32,
	boot_nonce: &[u8; 16],
	on: bool,
	proof: &[u8],
) -> Result<(), P> {
	let digest = containment_digest3(root_key, node_id, boot_nonce, on);
	match verify_core(
		context.view(),
		proof,
		digest,
		[3, 0],
		RoleForm3::Containment,
	) {
		Ok(true) => Ok(()),
		_ => Err(P::SelfCheck),
	}
}
