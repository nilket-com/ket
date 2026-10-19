//! Scripted dry runs of `plans/ops/root2_compromise.md`, one per section, with test keys only.
//! Each builds the section's history (the compromise, then its containment entries in the order
//! the runbook gives), runs the section's exact audit invocation, and asserts the outcome the
//! runbook records for it.
use super::support::*;
use crate::storage::mutation_visibility::root2::command::Command2;
use serde_json::Value;

fn flags<'a>(row: &'a Value, role: &str) -> &'a Value {
	row["positions"]
		.as_array()
		.unwrap()
		.iter()
		.find(|p| p["identity"]["role"] == role)
		.unwrap()
}
fn superseded(out: &Out, role: &str, epoch: u64) -> Option<u64> {
	out.containment(role, epoch)["superseded_at"]["index"].as_u64()
}
/// The runbook's exit criteria for a contained identity: complete, no finding, and every use
/// after its containment entry denied.
fn exit_criteria(out: &Out, role: &str, epoch: u64) {
	check_outcome(out);
	assert_eq!(out.code, 0, "{}", out.stderr);
	assert!(
		!out.containment(role, epoch)["activated_at"].is_null(),
		"the suspect identity exists in this root"
	);
	assert!(out.rows.iter().all(|r| r["finding"] == false));
	let at = superseded(out, role, epoch).expect("the identity is contained");
	for i in out.indices("suspect_use") {
		if i > at {
			assert_eq!(out.row(i)["effect"]["outcome"], "denied", "use at {i}");
		}
	}
}

#[test]
fn section1_admin_key_compromised() {
	let mut r = Root::new();
	r.enrol_authority(); // 3: governance enrols an Admission authority
	applied(r.statement_use(11)); // 4: the attacker's grant
	r.freeze(); // 5: Freeze, recovery
	r.rotate(ADMIN, 40); // 6: RotateAdmin, recovery
	deny(r.statement_use(11), 23); // 7: a later attempt while frozen
	let c = Command2::Unfreeze(super::wire::log(5));
	let p = r.proof(&c, [r.current(GOVERNANCE), r.current(RECOVERY)]);
	applied(r.apply(c, Some(p))); // 8: Unfreeze, recovery + governance
	deny(r.statement_use(11), 8); // 9: stale after unfreeze
	let dirs = r.write(None);
	let out = audit(
		&dirs,
		&[suspects(&[(ADMIN, 0)]), vec!["--suspect-only".into()]].concat(),
	);
	exit_criteria(&out, "admin", 0);
	assert_eq!(out.indices("suspect_use"), [4, 7, 9]);
	assert_eq!(superseded(&out, "admin", 0), Some(6));
	assert_eq!(
		out.row(4)["effect"]["outcome"],
		"applied",
		"pre-detection: review"
	);
	assert_eq!(flags(out.row(7), "admin")["in_freeze_interval"], true);
}

#[test]
fn section2_governance_key_compromised() {
	let mut r = Root::new();
	applied(r.legacy_governance_use(12)); // 3: VISADG01 use by the attacker
	r.freeze(); // 4
	r.rotate(GOVERNANCE, 41); // 5
	r.unfreeze(); // 6: recovery + admin
	deny(r.governance_use((GOVERNANCE, 0, 12)), 8); // 7
	let dirs = r.write(None);
	let out = audit(&dirs, &suspects(&[(GOVERNANCE, 0)]));
	exit_criteria(&out, "governance", 0);
	assert_eq!(out.indices("suspect_use"), [3, 7]);
	assert_eq!(superseded(&out, "governance", 0), Some(5));
}

#[test]
fn section3_admin_and_governance_compromised() {
	let mut r = Root::new();
	r.statement_use(11); // 3
	applied(r.governance_use(r.current(GOVERNANCE))); // 4
	r.freeze(); // 5
	r.rotate(ADMIN, 40); // 6
	r.rotate(GOVERNANCE, 41); // 7
	r.unfreeze(); // 8: recovery + the new admin
	let dirs = r.write(None);
	let out = audit(&dirs, &suspects(&[(ADMIN, 0), (GOVERNANCE, 0)]));
	exit_criteria(&out, "admin", 0);
	exit_criteria(&out, "governance", 0);
	assert_eq!(out.indices("suspect_use"), [3, 4]);
	assert_eq!(
		(
			superseded(&out, "admin", 0),
			superseded(&out, "governance", 0)
		),
		(Some(6), Some(7))
	);
}

#[test]
fn section4_recovery_key_compromised_race_won_and_lost() {
	// Won: the attacker freezes; admin + governance rotate recovery first.
	let mut r = Root::new();
	r.freeze(); // 3: by the compromised recovery key
	r.rotate(RECOVERY, 33); // 4: RotateRecovery, admin + governance (allowed while frozen)
	r.unfreeze(); // 5: the new recovery key + admin
	let dirs = r.write(None);
	let out = audit(&dirs, &suspects(&[(RECOVERY, 0)]));
	exit_criteria(&out, "recovery", 0);
	assert_eq!(out.indices("suspect_use"), [3]);
	assert_eq!(superseded(&out, "recovery", 0), Some(4));
	// Lost: the attacker rotated admin first; the audit shows an applied suspect rotation, which
	// is the runbook's signal to go to M2.
	let mut r = Root::new();
	r.rotate(ADMIN, 66); // 3: by the compromised recovery key
	let dirs = r.write(None);
	let out = audit(&dirs, &suspects(&[(RECOVERY, 0)]));
	check_outcome(&out);
	assert_eq!(out.code, 0);
	let row = out.row(3);
	assert_eq!(row["command"]["kind"], "rotate_admin");
	assert_eq!(row["match"], "suspect_use");
	assert_eq!(row["effect"]["outcome"], "applied");
	assert!(superseded(&out, "recovery", 0).is_none());
}

#[test]
fn section5_recovery_key_lost() {
	let mut r = Root::new();
	r.rotate(RECOVERY, 33); // 3: admin + governance
	let dirs = r.write(None);
	let lost = dirs.key_file(&key(13));
	let out = audit(&dirs, &["--key".into(), lost.display().to_string()]);
	exit_criteria(&out, "recovery", 0);
	assert!(
		out.indices("suspect_use").is_empty(),
		"the lost key was never used"
	);
	assert_eq!(superseded(&out, "recovery", 0), Some(3));
}

#[test]
fn section6_m2_old_root_retired_and_audited() {
	let mut r = Root::new();
	r.statement_use(11); // 3: a use under review
	r.retire(); // 4: RetireRoot, recovery
	deny(r.statement_use(11), 25); // 5: every later command is a consumed Denied(25)
	let dirs = r.write(None);
	let out = audit(
		&dirs,
		&suspects(&[(ADMIN, 0), (GOVERNANCE, 0), (RECOVERY, 0)]),
	);
	check_outcome(&out);
	assert_eq!(out.code, 0, "{}", out.stderr);
	assert_eq!(
		out.containment("admin", 0)["retired_at"]["index"].as_u64(),
		Some(4)
	);
	assert_eq!(out.row(5)["effect"]["reason"], 25);
	assert_eq!(flags(out.row(5), "admin")["after_retired"], true);
	assert!(out.rows.iter().all(|r| r["finding"] == false));
}
