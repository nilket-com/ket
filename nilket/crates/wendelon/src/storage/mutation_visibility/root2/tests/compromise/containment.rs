//! Containment per suspect identity: A3, A8, A9, A13, A14, A15, A16.
use super::support::*;
use serde_json::Value;

fn position<'a>(row: &'a Value, role: &str) -> &'a Value {
	row["positions"]
		.as_array()
		.unwrap()
		.iter()
		.find(|p| p["identity"]["role"] == role)
		.unwrap_or_else(|| panic!("no {role} position in {row}"))
}
fn claim<'a>(row: &'a Value, role: &str) -> &'a Value {
	row["signatures"]
		.as_array()
		.unwrap()
		.iter()
		.find(|s| s["claimed_role"] == role)
		.unwrap()
}
fn at(v: &Value) -> Option<u64> {
	v["index"].as_u64()
}

#[test]
fn a3_rotated_suspect_is_used_only_before_its_rotation() {
	let mut r = Root::new();
	applied(r.governance_use(r.current(GOVERNANCE))); // 3
	applied(r.governance_use(r.current(GOVERNANCE))); // 4
	r.rotate(GOVERNANCE, 41); // 5
	applied(r.governance_use(r.current(GOVERNANCE))); // 6, the new epoch-1 key
	let dirs = r.write(None);
	let out = audit(&dirs, &suspects(&[(GOVERNANCE, 0)]));
	check_outcome(&out);
	assert_eq!(out.code, 0, "{}", out.stderr);
	assert_eq!(out.indices("suspect_use"), [3, 4]);
	assert_eq!(out.class(6), "none");
	let c = out.containment("governance", 0);
	assert_eq!(at(&c["superseded_at"]), Some(5));
	assert_eq!(at(&c["activated_at"]), Some(2));
	assert!(c["retired_at"].is_null());
	assert_eq!(
		position(out.row(4), "governance")["after_superseded"],
		false
	);
	// Control: the successor identity is used at 6 and was never superseded.
	let next = audit(&dirs, &suspects(&[(GOVERNANCE, 1)]));
	assert_eq!(next.indices("suspect_use"), [6]);
	let c = next.containment("governance", 1);
	assert_eq!(at(&c["activated_at"]), Some(5));
	assert!(c["superseded_at"].is_null());
}

#[test]
fn a8_retired_root_lists_rows_past_retirement_and_verifies_where_the_reducer_did_not() {
	let mut r = Root::new();
	r.statement_use(11); // 3
	r.retire(); // 4
	deny(r.statement_use(11), 25); // 5: no authorization is evaluated after retirement
	deny(r.governance_use(r.current(GOVERNANCE)), 25); // 6
	let dirs = r.write(None);
	let out = audit(&dirs, &suspects(&[(ADMIN, 0)]));
	check_outcome(&out);
	assert_eq!(out.code, 0, "{}", out.stderr);
	let all: Vec<u64> = out
		.rows
		.iter()
		.map(|r| r["log_id"]["index"].as_u64().unwrap())
		.collect();
	assert_eq!(all, [2, 3, 4, 5, 6]);
	let row = out.row(5);
	assert_eq!(row["match"], "suspect_use");
	assert_eq!(row["effect"]["reason"], 25);
	assert_eq!(claim(row, "admin")["crypto_valid"], true);
	let p = position(row, "admin");
	assert_eq!(p["after_retired"], true);
	assert_eq!(
		row["finding"], false,
		"a denial after retirement is no finding"
	);
	assert_eq!(position(out.row(3), "admin")["after_retired"], false);
	assert_eq!(at(&out.containment("admin", 0)["retired_at"]), Some(4));
}

#[test]
fn a9_suspect_use_during_freeze_is_denied23_and_verified_inside_the_interval() {
	let mut r = Root::new();
	applied(r.governance_use(r.current(GOVERNANCE))); // 3, control: before the freeze
	r.freeze(); // 4
	deny(r.governance_use(r.current(GOVERNANCE)), 23); // 5
	let dirs = r.write(None);
	let out = audit(&dirs, &suspects(&[(GOVERNANCE, 0)]));
	check_outcome(&out);
	assert_eq!(out.code, 0, "{}", out.stderr);
	let before = out.row(3);
	assert_eq!(before["match"], "suspect_use");
	assert_eq!(before["effect"]["outcome"], "applied");
	assert_eq!(position(before, "governance")["in_freeze_interval"], false);
	let row = out.row(5);
	assert_eq!(row["match"], "suspect_use");
	assert_eq!(row["effect"]["reason"], 23);
	assert_eq!(claim(row, "governance")["crypto_valid"], true);
	assert_eq!(position(row, "governance")["in_freeze_interval"], true);
	let intervals = &out.containment("governance", 0)["freeze_intervals"];
	assert_eq!(at(&intervals[0][0]), Some(4));
	assert!(intervals[0][1].is_null());
}

#[test]
fn a13_stale_key_after_rotation_is_suspect_use_inactive_and_denied() {
	let mut r = Root::new();
	r.statement_use(11); // 3, control: before the rotation
	r.rotate(ADMIN, 40); // 4
	deny(r.statement_use(11), 8); // 5, the epoch-0 key after its supersession
	let dirs = r.write(None);
	let out = audit(&dirs, &suspects(&[(ADMIN, 0)]));
	check_outcome(&out);
	assert_eq!(out.code, 0, "{}", out.stderr);
	let row = out.row(5);
	assert_eq!(row["match"], "suspect_use");
	let c = claim(row, "admin");
	assert_eq!(c["crypto_valid"], true);
	assert_eq!(c["active_at_entry"], false);
	assert_eq!(row["effect"]["outcome"], "denied");
	assert_eq!(row["effect"]["reason"], 8);
	assert_eq!(position(row, "admin")["after_superseded"], true);
	assert_eq!(row["finding"], false);
	// Control: the same key before the rotation was active and not past containment.
	let before = out.row(3);
	assert_eq!(claim(before, "admin")["active_at_entry"], true);
	assert_eq!(position(before, "admin")["after_superseded"], false);
	assert_eq!(at(&out.containment("admin", 0)["superseded_at"]), Some(4));
}

#[test]
fn a14_containment_before_first_use_names_the_earlier_rotation() {
	let mut r = Root::new();
	r.rotate(ADMIN, 40); // 3
	applied(r.governance_use(r.current(GOVERNANCE))); // 4
	deny(r.statement_use(11), 8); // 5, the first suspect use is after containment
	// Control: later containment entries do not move the identity's supersession.
	r.rotate(ADMIN, 42); // 6, epoch 1 superseded; epoch 0 stays at 3
	r.freeze(); // 7
	let dirs = r.write(None);
	let out = audit(&dirs, &suspects(&[(ADMIN, 0)]));
	check_outcome(&out);
	assert_eq!(out.code, 0, "{}", out.stderr);
	assert_eq!(out.indices("suspect_use"), [5]);
	assert_eq!(at(&out.containment("admin", 0)["superseded_at"]), Some(3));
	assert_eq!(position(out.row(5), "admin")["after_superseded"], true);
	let next = audit(&dirs, &suspects(&[(ADMIN, 1)]));
	assert_eq!(at(&next.containment("admin", 1)["superseded_at"]), Some(6));
}

#[test]
fn a15_multiple_suspects_have_distinct_containment_and_per_identity_flags() {
	let mut r = Root::new();
	r.rotate(RECOVERY, 33); // 3, joint admin + governance, before either rotation
	r.rotate(ADMIN, 40); // 4
	deny(r.statement_use(11), 8); // 5, stale admin
	applied(r.governance_use((GOVERNANCE, 0, 12))); // 6, governance still current
	r.rotate(GOVERNANCE, 41); // 7
	deny(r.governance_use((GOVERNANCE, 0, 12)), 8); // 8, stale governance
	let dirs = r.write(None);
	let out = audit(&dirs, &suspects(&[(ADMIN, 0), (GOVERNANCE, 0)]));
	check_outcome(&out);
	assert_eq!(out.code, 0, "{}", out.stderr);
	let admin = out.containment("admin", 0);
	let governance = out.containment("governance", 0);
	assert_eq!(at(&admin["superseded_at"]), Some(4));
	assert_eq!(at(&governance["superseded_at"]), Some(7));
	assert_eq!(out.indices("suspect_use"), [3, 5, 6, 8]);
	// One row, two identities, each evaluated on its own.
	let joint = out.row(3);
	assert_eq!(joint["positions"].as_array().unwrap().len(), 2);
	assert_eq!(position(joint, "admin")["after_superseded"], false);
	assert_eq!(position(joint, "governance")["after_superseded"], false);
	assert_eq!(position(out.row(5), "admin")["after_superseded"], true);
	assert_eq!(
		position(out.row(6), "governance")["after_superseded"],
		false
	);
	assert_eq!(position(out.row(8), "governance")["after_superseded"], true);
	assert!(out.rows.iter().all(|r| r["finding"] == false));
}

#[test]
fn a16_freeze_intervals_are_reversible_and_flag_by_interval() {
	let mut r = Root::new();
	r.freeze(); // 3 = f1
	r.unfreeze(); // 4 = u1
	applied(r.governance_use(r.current(GOVERNANCE))); // 5, between u1 and f2
	r.freeze(); // 6 = f2
	deny(r.governance_use(r.current(GOVERNANCE)), 23); // 7, after f2
	let dirs = r.write(None);
	let out = audit(&dirs, &suspects(&[(GOVERNANCE, 0)]));
	check_outcome(&out);
	assert_eq!(out.code, 0, "{}", out.stderr);
	let intervals = out.containment("governance", 0)["freeze_intervals"]
		.as_array()
		.unwrap()
		.clone();
	assert_eq!(intervals.len(), 2);
	assert_eq!(
		(at(&intervals[0][0]), at(&intervals[0][1])),
		(Some(3), Some(4))
	);
	assert_eq!(at(&intervals[1][0]), Some(6));
	assert!(intervals[1][1].is_null());
	assert_eq!(
		position(out.row(5), "governance")["in_freeze_interval"],
		false
	);
	let after = out.row(7);
	assert_eq!(position(after, "governance")["in_freeze_interval"], true);
	assert_eq!(after["effect"]["reason"], 23);
	// The intervals are shared and repeated in every identity's object.
	let both = audit(&dirs, &suspects(&[(GOVERNANCE, 0), (ADMIN, 0)]));
	assert_eq!(
		both.containment("admin", 0)["freeze_intervals"],
		both.containment("governance", 0)["freeze_intervals"]
	);
	// The admin co-signed u1 while frozen: inside the interval it ends.
	assert_eq!(position(both.row(4), "admin")["in_freeze_interval"], true);
}
