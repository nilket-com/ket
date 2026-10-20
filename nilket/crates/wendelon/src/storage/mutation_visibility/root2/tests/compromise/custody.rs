//! Key custody T-K7 (the operator proof facade's cell matrix) and T-MERGE (merge validation).
//! Every cell starts from the context export the CLI writes for a real profile-3 history and the
//! production reader's `VerifiedContext`. The facade builds the unsigned output, the test signs its
//! preimage with the seed of the key's (role, epoch), and the spliced output must pass
//! `self_check`, then the production verifier and reducer: the carrier is applied through the sole
//! production verification producer, and authorization is never the consumed denial.
use super::export_chain::exported;
use super::reader::verify;
use super::support::*;
use super::wire;
use crate::storage::mutation_visibility::root2::command::{ApplyResult2, Command2};
use crate::storage::mutation_visibility::root2::governance_command::GovernanceAction2;
use crate::storage::mutation_visibility::root2::governance_verification::verify_governance2;
use crate::storage::mutation_visibility::root2::operator_proof::{
	self as facade, ProofRefusal as P, Unsigned, VerifiedContext, check_under,
};
use crate::storage::mutation_visibility::root2::types::Log2;
use ed25519_dalek::{Signer, SigningKey};

fn context(r: &Root) -> VerifiedContext {
	verify(&exported(r, 7), r.manifest()).unwrap()
}
fn bytes(r: &Root, c: &Command2) -> Vec<u8> {
	c.encode(r.f.dep.generation, r.f.dep.cluster).unwrap()
}
fn seed(r: &Root, role: u8) -> u8 {
	r.seeds[usize::from(role - 1)]
}
fn signed(seed: u8, u: &Unsigned) -> Vec<u8> {
	let signature = SigningKey::from_bytes(&[seed; 32])
		.sign(u.preimage())
		.to_bytes();
	facade::splice(u, &signature).unwrap()
}
/// The authorization was accepted: the reducer did not consume the entry as an authorization
/// (8), frozen (23), binding (24) or retired-root (25) denial.
fn authorized(result: ApplyResult2) {
	assert!(
		!matches!(
			result,
			ApplyResult2::Denied {
				reason: 8 | 23 | 24 | 25,
				..
			}
		),
		"{result:?}"
	);
}
/// The Freeze/Unfreeze binding as the tool builds it: the context's audit-derived transition.
fn binding(ctx: &VerifiedContext) -> Log2 {
	let b = ctx.transition().freeze_binding();
	Log2 {
		term: u64::from_le_bytes(b[..8].try_into().unwrap()),
		node: u32::from_le_bytes(b[8..12].try_into().unwrap()),
		index: u64::from_le_bytes(b[12..].try_into().unwrap()),
	}
}
/// A rotation the facade produced has been applied: the fixture's signer bookkeeping follows.
fn rotated(r: &mut Root, role: u8, next: u8) {
	let i = usize::from(role - 1);
	r.seeds[i] = next;
	r.epochs[i] += 1;
}
/// One signer, one sidecar: prepare, sign, splice, self-check, then the node accepts it.
fn single(r: &mut Root, c: Command2, role: u8, with: Option<u8>, form: Option<&[u8; 8]>) {
	let ctx = context(r);
	let command = bytes(r, &c);
	let s = seed(r, role);
	let u = facade::prepare(&ctx, &command, &key(s), with, form, None).unwrap();
	let sidecar = signed(s, &u);
	facade::self_check(&ctx, &command, Some(&sidecar)).unwrap();
	authorized(r.apply(c, Some(sidecar)));
}
/// Two hosts each sign their own slot from (command, context); the partials merge, and a second
/// host signing over the first's partial yields the same bytes.
fn joint(r: &mut Root, c: Command2, signers: [(u8, Option<u8>); 2]) -> Vec<u8> {
	let ctx = context(r);
	let command = bytes(r, &c);
	let [(a, with_a), (b, with_b)] = signers;
	let ua = facade::prepare(&ctx, &command, &key(seed(r, a)), with_a, None, None).unwrap();
	let ub = facade::prepare(&ctx, &command, &key(seed(r, b)), with_b, None, None).unwrap();
	assert_eq!(ua.layout(), ub.layout(), "both hosts rebuild one layout");
	let (pa, pb) = (signed(seed(r, a), &ua), signed(seed(r, b), &ub));
	// A partial is not yet a proof.
	assert_eq!(
		facade::self_check(&ctx, &command, Some(&pa)),
		Err(P::SelfCheck)
	);
	let merged = facade::merge(&ctx, &command, &pa, &pb).unwrap();
	assert_eq!(merged, facade::merge(&ctx, &command, &pb, &pa).unwrap());
	let over = facade::prepare(&ctx, &command, &key(seed(r, b)), with_b, None, Some(&pa)).unwrap();
	assert_eq!(signed(seed(r, b), &over), merged);
	facade::self_check(&ctx, &command, Some(&merged)).unwrap();
	authorized(r.apply(c, Some(merged.clone())));
	merged
}

#[test]
fn t_k7_single_recovery_slot_cells() {
	// RotateAdmin and RotateGovernance (Legacy), Freeze (Freeze form), RetireRoot (Retire form).
	let mut r = Root::new();
	single(&mut r, Command2::RotateAdmin(key(21)), RECOVERY, None, None);
	rotated(&mut r, ADMIN, 21);
	single(
		&mut r,
		Command2::RotateGovernance(key(31)),
		RECOVERY,
		None,
		None,
	);
	rotated(&mut r, GOVERNANCE, 31);
	let ctx = context(&r);
	assert_eq!(ctx.active(), [(1, key(21)), (1, key(31)), (0, key(13))]);
	single(
		&mut r,
		Command2::Freeze(binding(&ctx)),
		RECOVERY,
		None,
		None,
	);
	assert!(context(&r).frozen().refuses_while_frozen());
	let mut r = Root::new();
	single(&mut r, Command2::RetireRoot, RECOVERY, None, None);
	assert!(context(&r).retired().refuses_as_retired());
}
#[test]
fn t_k7_two_slot_cells_merge_across_hosts() {
	// RotateRecovery [1,2] (Legacy).
	let mut r = Root::new();
	joint(
		&mut r,
		Command2::RotateRecovery(key(41)),
		[(ADMIN, None), (GOVERNANCE, None)],
	);
	rotated(&mut r, RECOVERY, 41);
	// Unfreeze [1,3] and [2,3], each after a facade-built Freeze.
	for partner in [ADMIN, GOVERNANCE] {
		let ctx = context(&r);
		single(
			&mut r,
			Command2::Freeze(binding(&ctx)),
			RECOVERY,
			None,
			None,
		);
		let ctx = context(&r);
		joint(
			&mut r,
			Command2::Unfreeze(binding(&ctx)),
			[(partner, Some(RECOVERY)), (RECOVERY, Some(partner))],
		);
		assert!(!context(&r).frozen().refuses_while_frozen());
	}
	// RetireRoot [1,2] (Retire form).
	joint(
		&mut r,
		Command2::RetireRoot,
		[(ADMIN, Some(GOVERNANCE)), (GOVERNANCE, Some(ADMIN))],
	);
	assert!(context(&r).retired().refuses_as_retired());
}
/// The nine governance kinds, the issuer enrolled first so Grant's issuer exists.
fn governance_cells(r: &mut Root, form: &[u8; 8]) {
	let issuer = Command2::EnrollLeaseIssuer {
		node: 7,
		key: wire::key(wire::ISSUER_SEED),
	};
	single(r, issuer, GOVERNANCE, None, Some(form));
	for c in wire::nine(&r.f) {
		let ctx = context(r);
		let command = bytes(r, &c);
		let s = seed(r, GOVERNANCE);
		let u = facade::prepare(&ctx, &command, &key(s), None, Some(form), None).unwrap();
		let sidecar = signed(s, &u);
		facade::self_check(&ctx, &command, Some(&sidecar)).unwrap();
		let digest = c.digest(r.f.dep.generation, r.f.dep.cluster).unwrap();
		assert_eq!(
			verify_governance2(&r.f.state, &r.f.pin, &c, Some(&sidecar), digest),
			Ok(true),
			"{c:?}"
		);
		authorized(r.apply(c, Some(sidecar)));
	}
}
#[test]
fn t_k7_governance_family_under_both_sidecars_and_epochs() {
	// VISADG01 and VISROL03 [2,0] at governance epoch 0.
	for form in [b"VISADG01", b"VISROL03"] {
		let mut r = Root::new();
		governance_cells(&mut r, form);
	}
	// VISROL03 [2,0] at governance epoch 1.
	let mut r = Root::new();
	single(
		&mut r,
		Command2::RotateGovernance(key(31)),
		RECOVERY,
		None,
		None,
	);
	rotated(&mut r, GOVERNANCE, 31);
	governance_cells(&mut r, b"VISROL03");
}
/// An administrator statement through the facade, then the node: an admission effect.
fn statement(r: &mut Root, form: &[u8; 8]) {
	let ctx = context(r);
	let s = seed(r, ADMIN);
	let authority = wire::authority();
	let u = facade::prepare_statement(&ctx, &key(s), form, 1, &authority.id, authority.revision)
		.unwrap();
	let command = signed(s, &u);
	facade::self_check(&ctx, &command, None).unwrap();
	let (_, _, c) = Command2::decode(&command).unwrap();
	let result = r.apply(c, None);
	assert!(
		matches!(result, ApplyResult2::Permission { outcome: 1, .. }),
		"{result:?}"
	);
}
#[test]
fn t_k7_statement_cells() {
	// VISADS02 at admin epoch 0; VISADS03 at admin epochs 0 and 1.
	for form in [b"VISADS02", b"VISADS03"] {
		let mut r = Root::new();
		r.enrol_authority();
		statement(&mut r, form);
	}
	let mut r = Root::new();
	r.enrol_authority();
	single(&mut r, Command2::RotateAdmin(key(21)), RECOVERY, None, None);
	rotated(&mut r, ADMIN, 21);
	statement(&mut r, b"VISADS03");
}
#[test]
fn t_k7_refused_cells_are_typed_and_produce_nothing() {
	let mut r = Root::new();
	let ctx = context(&r);
	let prepare =
		|r: &Root, ctx: &VerifiedContext, c: &Command2, role: u8, form: Option<&[u8; 8]>| {
			facade::prepare(ctx, &bytes(r, c), &key(seed(r, role)), None, form, None).err()
		};
	let fixtures = super::super::support::command;
	// Bootstrap (the pin), Reserve and Finalize (authority keys), Cover.
	for c in [
		fixtures("command_bootstrap"),
		Command2::Reserve(super::super::support::claim()),
		fixtures("command_finalize"),
		Command2::Cover {
			kind: 0,
			external: [5; 32],
		},
	] {
		let Ok(command) = c.encode(r.f.dep.generation, r.f.dep.cluster) else {
			panic!("{c:?} does not encode under the fixture deployment");
		};
		for role in [ADMIN, GOVERNANCE, RECOVERY] {
			assert_eq!(
				facade::prepare(&ctx, &command, &key(seed(&r, role)), None, None, None).err(),
				Some(P::NotRoleSigned),
				"{c:?}"
			);
		}
	}
	// VISADC01: no production caller, no facade form.
	let enroll = wire::enroll_authority();
	assert_eq!(
		prepare(&r, &ctx, &enroll, GOVERNANCE, Some(b"VISADC01")),
		Some(P::NoProductionForm)
	);
	// A key whose role the requirement does not name.
	assert_eq!(
		prepare(&r, &ctx, &Command2::RotateAdmin(key(21)), ADMIN, None),
		Some(P::RoleNotNamed)
	);
	assert_eq!(
		prepare(&r, &ctx, &enroll, ADMIN, Some(b"VISROL03")),
		Some(P::RoleNotNamed)
	);
	assert_eq!(
		facade::prepare_statement(
			&ctx,
			&key(seed(&r, GOVERNANCE)),
			b"VISADS03",
			1,
			&[0x41; 16],
			1
		)
		.err(),
		Some(P::RoleNotNamed)
	);
	// A key that is not in this root's history at all.
	assert_eq!(
		facade::prepare(&ctx, &bytes(&r, &enroll), &key(0x99), None, None, None).err(),
		Some(P::KeyNotInHistory)
	);
	// After both rotations: VISADG01 and VISADS02 are refused, and the superseded keys are stale.
	single(&mut r, Command2::RotateAdmin(key(21)), RECOVERY, None, None);
	rotated(&mut r, ADMIN, 21);
	single(
		&mut r,
		Command2::RotateGovernance(key(31)),
		RECOVERY,
		None,
		None,
	);
	rotated(&mut r, GOVERNANCE, 31);
	let ctx = context(&r);
	assert_eq!(
		prepare(&r, &ctx, &enroll, GOVERNANCE, Some(b"VISADG01")),
		Some(P::GovernanceRotated)
	);
	assert_eq!(
		facade::prepare_statement(&ctx, &key(21), b"VISADS02", 1, &[0x41; 16], 1).err(),
		Some(P::AdminRotated)
	);
	assert_eq!(
		facade::prepare(
			&ctx,
			&bytes(&r, &enroll),
			&key(12),
			None,
			Some(b"VISROL03"),
			None
		)
		.err(),
		Some(P::StaleEpoch)
	);
	assert_eq!(
		facade::prepare_statement(&ctx, &key(11), b"VISADS03", 1, &[0x41; 16], 1).err(),
		Some(P::StaleEpoch)
	);
	// Controls: the same requests by the current keys are prepared.
	assert!(prepare(&r, &ctx, &enroll, GOVERNANCE, Some(b"VISROL03")).is_none());
	assert!(facade::prepare_statement(&ctx, &key(21), b"VISADS03", 1, &[0x41; 16], 1).is_ok());
	// Frozen: a statement or governance command would be consumed; retired: everything.
	single(
		&mut r,
		Command2::Freeze(binding(&ctx)),
		RECOVERY,
		None,
		None,
	);
	let frozen = context(&r);
	assert_eq!(
		prepare(&r, &frozen, &enroll, GOVERNANCE, Some(b"VISROL03")),
		Some(P::Frozen)
	);
	assert_eq!(
		facade::prepare_statement(&frozen, &key(21), b"VISADS03", 1, &[0x41; 16], 1).err(),
		Some(P::Frozen)
	);
	let mut r = Root::new();
	single(&mut r, Command2::RetireRoot, RECOVERY, None, None);
	let retired = context(&r);
	assert_eq!(
		prepare(
			&r,
			&retired,
			&Command2::RotateAdmin(key(21)),
			RECOVERY,
			None
		),
		Some(P::Retired)
	);
}
#[test]
fn t_k7_with_choice_is_exact() {
	let r = Root::new();
	let ctx = context(&r);
	let retire = bytes(&r, &Command2::RetireRoot);
	let unfreeze = bytes(&r, &Command2::Unfreeze(binding(&ctx)));
	let rotate = bytes(&r, &Command2::RotateAdmin(key(21)));
	let prepare = |command: &[u8], role: u8, with| {
		facade::prepare(&ctx, command, &key(seed(&r, role)), with, None, None).err()
	};
	// Recovery alone retires; the admin/governance form needs the partner named.
	assert_eq!(prepare(&retire, RECOVERY, None), None);
	assert_eq!(
		prepare(&retire, RECOVERY, Some(ADMIN)),
		Some(P::RoleNotNamed)
	);
	assert_eq!(prepare(&retire, ADMIN, None), Some(P::WithChoice));
	assert_eq!(prepare(&retire, ADMIN, Some(GOVERNANCE)), None);
	// Unfreeze always names the partner, and only the requirement's other role.
	assert_eq!(prepare(&unfreeze, RECOVERY, None), Some(P::WithChoice));
	assert_eq!(
		prepare(&unfreeze, ADMIN, Some(GOVERNANCE)),
		Some(P::WithChoice)
	);
	assert_eq!(prepare(&unfreeze, ADMIN, Some(ADMIN)), Some(P::WithChoice));
	assert_eq!(prepare(&unfreeze, RECOVERY, Some(GOVERNANCE)), None);
	// No other command takes a choice.
	assert_eq!(prepare(&rotate, RECOVERY, Some(ADMIN)), Some(P::WithChoice));
	assert_eq!(prepare(&rotate, RECOVERY, None), None);
}
#[test]
fn t_k7_self_check_refuses_flipped_preimage_wrong_alternative_and_altered_digest() {
	let mut r = Root::new();
	let ctx = context(&r);
	let c = Command2::RotateAdmin(key(21));
	let command = bytes(&r, &c);
	let s = seed(&r, RECOVERY);
	let u = facade::prepare(&ctx, &command, &key(s), None, None, None).unwrap();
	let good = signed(s, &u);
	assert_eq!(facade::self_check(&ctx, &command, Some(&good)), Ok(()));
	// A flipped preimage byte, signed and spliced.
	for at in [0, 40, u.preimage().len() - 1] {
		let mut preimage = u.preimage().to_vec();
		preimage[at] ^= 1;
		let signature = SigningKey::from_bytes(&[s; 32]).sign(&preimage).to_bytes();
		let bad = facade::splice(&u, &signature).unwrap();
		assert_eq!(
			facade::self_check(&ctx, &command, Some(&bad)),
			Err(P::SelfCheck),
			"preimage byte {at}"
		);
	}
	// The command digest altered after signing: another command, or the bound digest edited.
	let other = bytes(&r, &Command2::RotateAdmin(key(22)));
	assert_eq!(
		facade::self_check(&ctx, &other, Some(&good)),
		Err(P::SelfCheck)
	);
	let mut edited = good.clone();
	edited[44] ^= 1;
	assert_eq!(
		facade::self_check(&ctx, &command, Some(&edited)),
		Err(P::SelfCheck)
	);
	// The wrong role alternative: a valid Unfreeze [1,3] proof read as [2,3].
	let ctx = context(&r);
	single(
		&mut r,
		Command2::Freeze(binding(&ctx)),
		RECOVERY,
		None,
		None,
	);
	let ctx = context(&r);
	let unfreeze = Command2::Unfreeze(binding(&ctx));
	let command = bytes(&r, &unfreeze);
	let parts = [(ADMIN, Some(RECOVERY)), (RECOVERY, Some(ADMIN))].map(|(role, with)| {
		let u = facade::prepare(&ctx, &command, &key(seed(&r, role)), with, None, None).unwrap();
		signed(seed(&r, role), &u)
	});
	let merged = facade::merge(&ctx, &command, &parts[0], &parts[1]).unwrap();
	assert_eq!(facade::self_check(&ctx, &command, Some(&merged)), Ok(()));
	let mut wrong = merged.clone();
	wrong[76] = GOVERNANCE;
	assert_eq!(
		facade::self_check(&ctx, &command, Some(&wrong)),
		Err(P::SelfCheck)
	);
	// The node agrees with every refusal above: the wrong alternative is a consumed denial.
	deny(r.apply(unfreeze, Some(wrong)), 8);
}
#[test]
fn t_k7_governance_action_bit_is_part_of_the_role_proof_self_check() {
	// A profile-3 manifest fixes the mask, so the out-of-mask view is test-only.
	let r = Root::new();
	let ctx = context(&r);
	let c = wire::enroll_authority();
	let command = bytes(&r, &c);
	let s = seed(&r, GOVERNANCE);
	let u = facade::prepare(&ctx, &command, &key(s), None, Some(b"VISROL03"), None).unwrap();
	let sidecar = signed(s, &u);
	let digest = c.digest(r.f.dep.generation, r.f.dep.cluster).unwrap();
	let bit = GovernanceAction2::EnrollAuthority.bit();
	assert_eq!(check_under(ctx.view(), &c, digest, Some(&sidecar)), Ok(()));
	assert_eq!(
		check_under(&ctx.view().with_mask(0x1FF), &c, digest, Some(&sidecar)),
		Ok(())
	);
	assert_eq!(
		check_under(
			&ctx.view().with_mask(0x1FF & !bit),
			&c,
			digest,
			Some(&sidecar)
		),
		Err(P::SelfCheck)
	);
	// The same mask on another action's bit leaves this command authorized.
	assert_eq!(
		check_under(
			&ctx.view()
				.with_mask(0x1FF & !GovernanceAction2::Grant.bit()),
			&c,
			digest,
			Some(&sidecar)
		),
		Ok(())
	);
}
#[test]
fn t_merge_refuses_each_inconsistency() {
	let r = Root::new();
	let ctx = context(&r);
	let c = Command2::RotateRecovery(key(41));
	let command = bytes(&r, &c);
	let part = |c: &Command2, role: u8| {
		let u =
			facade::prepare(&ctx, &bytes(&r, c), &key(seed(&r, role)), None, None, None).unwrap();
		signed(seed(&r, role), &u)
	};
	let (a, b) = (part(&c, ADMIN), part(&c, GOVERNANCE));
	// Control.
	assert!(facade::merge(&ctx, &command, &a, &b).is_ok());
	// Mismatched commands, or contexts.
	let other = part(&Command2::RotateRecovery(key(43)), GOVERNANCE);
	assert_eq!(
		facade::merge(&ctx, &command, &a, &other).err(),
		Some(P::Mismatch)
	);
	let mut foreign = b.clone();
	foreign[20] ^= 1;
	assert_eq!(
		facade::merge(&ctx, &command, &a, &foreign).err(),
		Some(P::Mismatch)
	);
	// A slot overwritten: both inputs fill the same slot.
	let mut twice = a.clone();
	twice[86..150].copy_from_slice(&part(&c, ADMIN)[86..150]);
	assert_eq!(
		facade::merge(&ctx, &command, &a, &twice).err(),
		Some(P::Overwritten)
	);
	// An invalid first slot.
	let mut invalid = a.clone();
	invalid[100] ^= 1;
	assert_eq!(
		facade::merge(&ctx, &command, &invalid, &b).err(),
		Some(P::SlotSignature)
	);
	// A layout that is not the one rebuilt from (command, context): both partials agree with each
	// other, but bind a command other than the one being merged.
	let wrong = Command2::RotateRecovery(key(43));
	let (wa, wb) = (part(&wrong, ADMIN), part(&wrong, GOVERNANCE));
	assert!(facade::merge(&ctx, &bytes(&r, &wrong), &wa, &wb).is_ok());
	assert_eq!(
		facade::merge(&ctx, &command, &wa, &wb).err(),
		Some(P::Layout)
	);
	// An input filling both slots, or none.
	let both = facade::merge(&ctx, &command, &a, &b).unwrap();
	assert_eq!(
		facade::merge(&ctx, &command, &a, &both).err(),
		Some(P::SlotFill)
	);
	let mut none = a.clone();
	none[86..150].fill(0);
	assert_eq!(
		facade::merge(&ctx, &command, &none, &b).err(),
		Some(P::SlotFill)
	);
	// A one-slot form is never merged.
	let single = Command2::RotateAdmin(key(21));
	let p = part(&single, RECOVERY);
	assert_eq!(
		facade::merge(&ctx, &bytes(&r, &single), &p, &p).err(),
		Some(P::NotJoint)
	);
	// The second signer's own check of the first partial: rebuilt layout, the other slot, valid.
	let gov = key(seed(&r, GOVERNANCE));
	let over = |partial: &[u8]| facade::prepare(&ctx, &command, &gov, None, None, Some(partial));
	assert!(over(&a).is_ok());
	assert_eq!(over(&wa).err(), Some(P::Layout));
	assert_eq!(over(&b).err(), Some(P::Overwritten));
	assert_eq!(over(&invalid).err(), Some(P::SlotSignature));
}
