//! Attribution and match classes: A1, A2, A10, A17.
use super::support::*;
use crate::storage::mutation_visibility::root2::command::Command2;
use serde_json::Value;

/// The signature record for `role` in `row`.
fn claim<'a>(row: &'a Value, role: &str) -> &'a Value {
	row["signatures"]
		.as_array()
		.unwrap()
		.iter()
		.find(|s| s["claimed_role"] == role)
		.unwrap_or_else(|| panic!("no {role} claim in {row}"))
}

/// A1 history: 3 governance enrols an authority, 4 admin statement (applied), 5 an idle Unfreeze
/// co-signed by admin (Denied(24)), 6 governance alone, 7 explicit-epoch admin statement.
fn admin_history() -> Root {
	let mut r = Root::new();
	r.enrol_authority();
	applied(r.statement_use(11));
	deny(r.idle_unfreeze(), 24);
	applied(r.governance_use(r.current(GOVERNANCE)));
	applied(r.epoch_statement_use(11, 0));
	r
}

#[test]
fn a1_single_role_suspect_rows_in_order_with_applied_and_denied_effects() {
	let r = admin_history();
	let dirs = r.write(None);
	let out = audit(&dirs, &suspects(&[(ADMIN, 0)]));
	check_outcome(&out);
	assert_eq!(out.code, 0, "{}", out.stderr);
	// Every Root2 entry of the root, in ascending index: the install entry and five commands.
	let all: Vec<u64> = out
		.rows
		.iter()
		.map(|r| r["log_id"]["index"].as_u64().unwrap())
		.collect();
	assert_eq!(all, [2, 3, 4, 5, 6, 7]);
	assert_eq!(out.indices("suspect_use"), [4, 5, 7]);
	assert_eq!(out.indices("none"), [2, 3, 6]);
	assert_eq!(out.row(4)["effect"]["outcome"], "applied");
	assert_eq!(out.row(5)["effect"]["outcome"], "denied");
	assert_eq!(out.row(5)["effect"]["reason"], 24);
	for i in [4, 5, 7] {
		let admin = claim(out.row(i), "admin");
		assert_eq!(admin["crypto_valid"], true);
		assert_eq!(admin["active_at_entry"], true);
		assert_eq!(admin["claimed_epoch"], 0);
	}
	assert_eq!(claim(out.row(4), "admin")["form"], "VISADS02");
	assert_eq!(claim(out.row(5), "admin")["form"], "VISROL03");
	assert_eq!(claim(out.row(7), "admin")["form"], "VISADS03");
	assert_eq!(
		out.row(2)["signatures"][0]["unattributable"],
		"no_role_form",
		"Bootstrap names no signer role"
	);
	// The root key is the full bound administrator context on every row.
	let root = &out.rows[0]["root"];
	assert!(out.rows.iter().all(|r| r["root"] == *root));
	assert_eq!(root["installed_at"]["index"], 2);
	// --suspect-only keeps exactly the suspect classes plus the final record.
	let only = audit(
		&dirs,
		&[suspects(&[(ADMIN, 0)]), vec!["--suspect-only".into()]].concat(),
	);
	assert_eq!(only.code, 0);
	assert_eq!(only.indices("suspect_use"), [4, 5, 7]);
	assert_eq!(only.rows.len(), 3);
	// Negative control: an epoch the history never held matches nothing.
	let none = audit(&dirs, &suspects(&[(ADMIN, 1)]));
	assert_eq!(none.code, 0);
	assert!(none.indices("suspect_use").is_empty());
	assert!(none.indices("suspect_claim_unverified").is_empty());
}

#[test]
fn a2_joint_proof_with_one_suspect_signer_is_suspect_use() {
	let mut r = Root::new();
	r.rotate(RECOVERY, 33);
	let dirs = r.write(None);
	let out = audit(&dirs, &suspects(&[(ADMIN, 0)]));
	check_outcome(&out);
	assert_eq!(out.code, 0, "{}", out.stderr);
	let row = out.row(3);
	assert_eq!(row["command"]["kind"], "rotate_recovery");
	assert_eq!(row["match"], "suspect_use");
	// The honest co-signer's valid signature is shown and is not the suspect.
	assert_eq!(claim(row, "governance")["crypto_valid"], true);
	let positions = row["positions"].as_array().unwrap();
	assert_eq!(positions.len(), 1);
	assert_eq!(positions[0]["identity"]["role"], "admin");
	// Negative control: the rotated-away recovery key did not sign this entry.
	let other = audit(&dirs, &suspects(&[(RECOVERY, 0)]));
	assert_eq!(other.class(3), "none");
}

#[test]
fn a10_forged_sidecar_claiming_a_suspect_is_never_suspect_use() {
	let mut r = Root::new();
	let c = Command2::Freeze(crate::storage::mutation_visibility::root2::types::Log2 {
		term: 0,
		node: 0,
		index: 0,
	});
	// Claims recovery epoch 0 but is signed by an unrelated key.
	let forged = r.proof(&c, [(RECOVERY, 0, 99), (0, 0, 0)]);
	deny(r.apply(c, Some(forged)), 8);
	// Control: the genuine recovery key freezes next.
	r.freeze();
	let dirs = r.write(None);
	let out = audit(&dirs, &suspects(&[(RECOVERY, 0)]));
	check_outcome(&out);
	assert_eq!(out.code, 0, "{}", out.stderr);
	assert_eq!(out.class(3), "suspect_claim_unverified");
	let forged = claim(out.row(3), "recovery");
	assert_eq!(forged["crypto_valid"], false);
	assert_eq!(forged["reason"], "signature");
	assert!(out.row(3)["positions"].as_array().unwrap().is_empty());
	assert_eq!(out.class(4), "suspect_use");
	assert_eq!(claim(out.row(4), "recovery")["crypto_valid"], true);
}

#[test]
fn a17_unknown_claimed_epoch_is_unverified_and_classed_by_its_claim() {
	let mut r = Root::new();
	// A VISROL03 claim of recovery epoch 5, which never existed, signed by the epoch-0 key.
	let c = Command2::RotateAdmin(key(44));
	let p = r.proof(&c, [(RECOVERY, 5, 13), (0, 0, 0)]);
	deny(r.apply(c, Some(p)), 8);
	// And a VISADS03 claim of admin epoch 3.
	deny(r.epoch_statement_use(11, 3), 8);
	let dirs = r.write(None);
	let out = audit(&dirs, &suspects(&[(RECOVERY, 5), (ADMIN, 3)]));
	check_outcome(&out);
	assert_eq!(out.code, 0, "{}", out.stderr);
	for (i, role) in [(3, "recovery"), (4, "admin")] {
		let c = claim(out.row(i), role);
		assert_eq!(c["crypto_valid"], false);
		assert_eq!(c["reason"], "unknown_epoch");
		assert_eq!(c["active_at_entry"], false);
		assert_eq!(out.class(i), "suspect_claim_unverified");
	}
	// Negative control: the claim names epoch 5, not the real epoch 0.
	let real = audit(&dirs, &suspects(&[(RECOVERY, 0), (ADMIN, 0)]));
	assert_eq!(real.class(3), "none");
	assert_eq!(real.class(4), "none");
}

#[test]
fn a10_a_genuine_signature_replayed_onto_another_entry_is_not_attributed() {
	let mut r = Root::new();
	// A valid recovery signature over another command's digest, attached to RotateAdmin.
	let other = Command2::RotateGovernance(key(45));
	let replayed = r.proof(&other, [r.current(RECOVERY), (0, 0, 0)]);
	deny(r.apply(Command2::RotateAdmin(key(44)), Some(replayed)), 8);
	let dirs = r.write(None);
	let out = audit(&dirs, &suspects(&[(RECOVERY, 0)]));
	check_outcome(&out);
	assert_eq!(out.code, 0, "{}", out.stderr);
	let c = claim(out.row(3), "recovery");
	assert_eq!(c["crypto_valid"], false);
	assert_eq!(c["reason"], "binding_mismatch");
	assert_eq!(out.class(3), "suspect_claim_unverified");
}
