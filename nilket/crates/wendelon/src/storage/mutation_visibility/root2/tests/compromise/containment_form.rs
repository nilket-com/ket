//! Containment signing (D-21) at the facade: the vectors, cross-use refusals against the
//! production verifier and reducer, the binding to one (RootKey, node, boot, state), and the
//! facade's signer refusals. The node-side check (`live.rs`) is activation slice 6; here
//! `self_check_containment` runs the same `verify_core` call under the reader's view.
use super::export_chain::exported;
use super::reader::verify;
use super::support::*;
use crate::storage::mutation_visibility::root2::command::{ApplyResult2, Command2};
use crate::storage::mutation_visibility::root2::operator_proof::{
	self as facade, ProofRefusal as P, Unsigned, VerifiedContext,
};
use crate::storage::mutation_visibility::root2::role_command::{RoleForm3, preimage_for};
use crate::storage::mutation_visibility::root2::types::Log2;
use ed25519_dalek::{Signer, SigningKey};
use serde_json::{Value, json};

const ROOT_KEY: [u8; 32] = [0x52; 32];
const NODE: u32 = 4;
const NONCE: [u8; 16] = [0x6e; 16];

fn context(r: &Root) -> VerifiedContext {
	verify(&exported(r, 7), r.manifest()).unwrap()
}
fn hex(b: &[u8]) -> String {
	b.iter().map(|x| format!("{x:02x}")).collect()
}
fn sign(seed: u8, preimage: &[u8]) -> [u8; 64] {
	SigningKey::from_bytes(&[seed; 32])
		.sign(preimage)
		.to_bytes()
}
fn signed(seed: u8, u: &Unsigned) -> Vec<u8> {
	facade::splice(u, &sign(seed, u.preimage())).unwrap()
}
/// `layout` with slot 0 signed by `seed` over the `form` preimage of that same layout.
fn signed_as(seed: u8, layout: &[u8], form: RoleForm3) -> Vec<u8> {
	let mut b = layout.to_vec();
	let preimage = preimage_for(&b, [3, 0], 0, form).unwrap();
	b[86..150].copy_from_slice(&sign(seed, &preimage));
	b
}
fn containment(
	r: &Root,
	ctx: &VerifiedContext,
	rk: [u8; 32],
	node: u32,
	nonce: [u8; 16],
	on: bool,
) -> Vec<u8> {
	let s = r.seeds[2];
	signed(
		s,
		&facade::prepare_containment(ctx, &key(s), &rk, node, &nonce, on).unwrap(),
	)
}
fn check(
	ctx: &VerifiedContext,
	rk: [u8; 32],
	node: u32,
	nonce: [u8; 16],
	on: bool,
	proof: &[u8],
) -> bool {
	facade::self_check_containment(ctx, &rk, node, &nonce, on, proof).is_ok()
}
fn binding(ctx: &VerifiedContext) -> Log2 {
	let b = ctx.transition().freeze_binding();
	Log2 {
		term: u64::from_le_bytes(b[..8].try_into().unwrap()),
		node: u32::from_le_bytes(b[8..12].try_into().unwrap()),
		index: u64::from_le_bytes(b[12..].try_into().unwrap()),
	}
}

/// The vectors: three (RootKey, node, nonce, state) inputs on one root, each with its digest,
/// preimage and signed proof, plus the export and manifest a custody tool replays them on.
fn vectors() -> Value {
	let r = Root::new();
	let export = exported(&r, 7);
	let ctx = verify(&export, r.manifest()).unwrap();
	let rows: Vec<Value> = [
		(ROOT_KEY, NODE, NONCE, true),
		(ROOT_KEY, NODE, NONCE, false),
		([0x53; 32], 9, [0x01; 16], true),
	]
	.into_iter()
	.map(|(rk, node, nonce, on)| {
		let u = facade::prepare_containment(&ctx, &key(r.seeds[2]), &rk, node, &nonce, on).unwrap();
		let proof = signed(r.seeds[2], &u);
		json!({
			"root_key": hex(&rk),
			"node_id": node,
			"boot_nonce": hex(&nonce),
			"on": on,
			"digest": hex(&u.layout()[44..76]),
			"preimage": hex(u.preimage()),
			"proof": hex(&proof),
		})
	})
	.collect();
	json!({
		"recovery_seed": r.seeds[2],
		"export": hex(&export),
		"manifest": hex(r.manifest()),
		"vectors": rows,
	})
}

#[test]
fn t_cv_the_vectors_are_reproduced_and_the_digest_is_the_documented_blake3() {
	let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
		.join("src/storage/mutation_visibility/root2/tests/compromise/containment_vectors.json");
	let generated = vectors();
	if std::env::var_os("WENDELON_WRITE_VECTORS").is_some_and(|v| v == "1") {
		std::fs::write(
			&path,
			serde_json::to_string_pretty(&generated).unwrap() + "\n",
		)
		.unwrap();
	}
	let pinned: Value = serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
	assert_eq!(
		generated, pinned,
		"regenerate with WENDELON_WRITE_VECTORS=1 and review the diff"
	);
	let r = Root::new();
	let ctx = context(&r);
	for v in pinned["vectors"].as_array().unwrap() {
		let unhex = |s: &Value| -> Vec<u8> {
			let s = s.as_str().unwrap();
			(0..s.len())
				.step_by(2)
				.map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
				.collect()
		};
		let (rk, nonce): ([u8; 32], [u8; 16]) = (
			unhex(&v["root_key"]).try_into().unwrap(),
			unhex(&v["boot_nonce"]).try_into().unwrap(),
		);
		let (node, on) = (
			v["node_id"].as_u64().unwrap() as u32,
			v["on"].as_bool().unwrap(),
		);
		// The digest, recomputed from the documented concatenation with a one-shot BLAKE3.
		let mut message = b"wendelon/root2/containment-elect/v1\0".to_vec();
		message.extend(rk);
		message.extend(node.to_le_bytes());
		message.extend(nonce);
		message.push(u8::from(on));
		assert_eq!(hex(&crate::hash::blake3::digest(&message)), v["digest"]);
		let proof = unhex(&v["proof"]);
		assert!(check(&ctx, rk, node, nonce, on, &proof));
		// Control: one input byte changed gives another digest and fails the check.
		let mut other = rk;
		other[0] ^= 1;
		assert!(!check(&ctx, other, node, nonce, on, &proof));
	}
}

#[test]
fn t_cx_a_containment_proof_never_authorizes_an_on_log_command() {
	let mut r = Root::new();
	let ctx = context(&r);
	let proof = containment(&r, &ctx, ROOT_KEY, NODE, NONCE, true);
	let s = r.seeds[2];
	let denied = |result: ApplyResult2| {
		assert!(
			matches!(result, ApplyResult2::Denied { reason: 8, .. }),
			"{result:?}"
		);
	};
	// The containment proof itself, as an on-log sidecar.
	denied(r.apply(Command2::RetireRoot, Some(proof.clone())));
	denied(r.apply(Command2::Freeze(binding(&ctx)), Some(proof.clone())));
	// The Legacy [3,0] commands too: the recovery-authorized rotations.
	denied(r.apply(Command2::RotateAdmin(key(42)), Some(proof.clone())));
	denied(r.apply(Command2::RotateGovernance(key(43)), Some(proof.clone())));
	let issuer = Command2::EnrollLeaseIssuer {
		node: 7,
		key: key(0x39),
	};
	let governance = r.apply(issuer.clone(), Some(proof.clone()));
	assert!(
		matches!(governance, ApplyResult2::Denied { .. }),
		"{governance:?}"
	);
	// The domain alone separates them: the on-log layout, carrying the command digest, signed in
	// the containment domain, is refused; signed in its own domain it is accepted (control).
	for (c, form) in [
		(Command2::RetireRoot, RoleForm3::Retire),
		(Command2::Freeze(binding(&ctx)), RoleForm3::Freeze),
		(Command2::RotateAdmin(key(42)), RoleForm3::Legacy),
		(Command2::RotateGovernance(key(43)), RoleForm3::Legacy),
	] {
		let command = c.encode(r.f.dep.generation, r.f.dep.cluster).unwrap();
		let u = facade::prepare(&ctx, &command, &key(s), None, None, None).unwrap();
		let wrong = signed_as(s, u.layout(), RoleForm3::Containment);
		assert_eq!(
			facade::self_check(&ctx, &command, Some(&wrong)),
			Err(P::SelfCheck)
		);
		let right = signed_as(s, u.layout(), form);
		facade::self_check(&ctx, &command, Some(&right)).unwrap();
	}
	let mut fresh = Root::new();
	let command = Command2::RetireRoot
		.encode(fresh.f.dep.generation, fresh.f.dep.cluster)
		.unwrap();
	let fctx = context(&fresh);
	let u = facade::prepare(&fctx, &command, &key(fresh.seeds[2]), None, None, None).unwrap();
	denied(fresh.apply(
		Command2::RetireRoot,
		Some(signed_as(
			fresh.seeds[2],
			u.layout(),
			RoleForm3::Containment,
		)),
	));
	let accepted = fresh.apply(
		Command2::RetireRoot,
		Some(signed_as(fresh.seeds[2], u.layout(), RoleForm3::Retire)),
	);
	assert!(
		!matches!(accepted, ApplyResult2::Denied { reason: 8, .. }),
		"{accepted:?}"
	);
	// The same through the reducer for a Legacy [3,0] rotation: the containment domain over the
	// RotateAdmin layout is denied; the Legacy domain over it registers (control).
	let mut legacy = Root::new();
	let rotate = Command2::RotateAdmin(key(44));
	let command = rotate
		.encode(legacy.f.dep.generation, legacy.f.dep.cluster)
		.unwrap();
	let lctx = context(&legacy);
	let u = facade::prepare(&lctx, &command, &key(legacy.seeds[2]), None, None, None).unwrap();
	denied(legacy.apply(
		rotate.clone(),
		Some(signed_as(
			legacy.seeds[2],
			u.layout(),
			RoleForm3::Containment,
		)),
	));
	let registered = legacy.apply(
		rotate,
		Some(signed_as(legacy.seeds[2], u.layout(), RoleForm3::Legacy)),
	);
	assert!(
		matches!(registered, ApplyResult2::Registered(_)),
		"{registered:?}"
	);
}

#[test]
fn t_cx_an_on_log_recovery_proof_is_not_a_containment_proof() {
	let r = Root::new();
	let ctx = context(&r);
	let s = r.seeds[2];
	let rotate = Command2::RotateAdmin(key(40))
		.encode(r.f.dep.generation, r.f.dep.cluster)
		.unwrap();
	let legacy = signed(
		s,
		&facade::prepare(&ctx, &rotate, &key(s), None, None, None).unwrap(),
	);
	facade::self_check(&ctx, &rotate, Some(&legacy)).unwrap();
	assert!(!check(&ctx, ROOT_KEY, NODE, NONCE, true, &legacy));
	// The containment layout signed in the Legacy (or Retire, Freeze) domain is refused; in the
	// containment domain it is accepted (control).
	let u = facade::prepare_containment(&ctx, &key(s), &ROOT_KEY, NODE, &NONCE, true).unwrap();
	for form in [RoleForm3::Legacy, RoleForm3::Retire, RoleForm3::Freeze] {
		assert!(!check(
			&ctx,
			ROOT_KEY,
			NODE,
			NONCE,
			true,
			&signed_as(s, u.layout(), form)
		));
	}
	assert!(check(
		&ctx,
		ROOT_KEY,
		NODE,
		NONCE,
		true,
		&signed_as(s, u.layout(), RoleForm3::Containment)
	));
}

#[test]
fn t_cb_a_containment_proof_binds_one_root_node_boot_and_state() {
	let r = Root::new();
	let ctx = context(&r);
	let proof = containment(&r, &ctx, ROOT_KEY, NODE, NONCE, true);
	assert!(check(&ctx, ROOT_KEY, NODE, NONCE, true, &proof));
	let mut nonce = NONCE;
	nonce[15] ^= 1;
	for (what, rk, node, nonce, on) in [
		("boot nonce", ROOT_KEY, NODE, nonce, true),
		("node", ROOT_KEY, NODE + 1, NONCE, true),
		("RootKey", [0x53; 32], NODE, NONCE, true),
		("state", ROOT_KEY, NODE, NONCE, false),
	] {
		assert!(!check(&ctx, rk, node, nonce, on, &proof), "{what}");
	}
}

#[test]
fn t_cf_only_the_active_recovery_key_of_a_live_root_prepares() {
	let mut r = Root::new();
	let ctx = context(&r);
	for signer in [r.seeds[0], r.seeds[1]] {
		assert!(matches!(
			facade::prepare_containment(&ctx, &key(signer), &ROOT_KEY, NODE, &NONCE, true),
			Err(P::RoleNotNamed)
		));
	}
	facade::prepare_containment(&ctx, &key(r.seeds[2]), &ROOT_KEY, NODE, &NONCE, true).unwrap();
	// A frozen root still prepares (containment is incident response).
	r.freeze();
	let frozen = context(&r);
	assert!(frozen.frozen().refuses_while_frozen());
	facade::prepare_containment(&frozen, &key(r.seeds[2]), &ROOT_KEY, NODE, &NONCE, true).unwrap();
	// A stale recovery epoch is refused; the new one prepares.
	let old = r.seeds[2];
	r.unfreeze();
	r.rotate(RECOVERY, 45);
	let rotated = context(&r);
	assert!(matches!(
		facade::prepare_containment(&rotated, &key(old), &ROOT_KEY, NODE, &NONCE, true),
		Err(P::StaleEpoch)
	));
	let proof = containment(&r, &rotated, ROOT_KEY, NODE, NONCE, true);
	assert!(check(&rotated, ROOT_KEY, NODE, NONCE, true, &proof));
	// A retired root refuses.
	r.retire();
	let retired = context(&r);
	assert!(matches!(
		facade::prepare_containment(&retired, &key(r.seeds[2]), &ROOT_KEY, NODE, &NONCE, true),
		Err(P::Retired)
	));
}
