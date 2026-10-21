//! T-K7 through the tool: every accepted cell signs through `sign` (and `merge` for joint
//! proofs) and passes the facade's own `self_check`; every refused cell is typed and writes
//! nothing. Plus `build`.
use super::support::*;
use wendelon::storage::mutation_visibility::root2::Command2;
use wendelon::storage::mutation_visibility::root2::operator_proof as facade;

use super::signing::{Root, accepted, code, log, nine};

#[test]
fn t_k7_single_recovery_slot_cells_sign_through_the_tool() {
	let mut r = Root::new();
	for (n, c) in [
		Command2::RotateAdmin(public(&[0x51; 32])),
		Command2::RotateGovernance(public(&[0x52; 32])),
		Command2::Freeze(log(9)),
		Command2::RetireRoot,
	]
	.into_iter()
	.enumerate()
	{
		let (bytes, path) = r.command(&c);
		let out = format!("single{n}");
		let (ok, shown, err) = r.sign("rec0", &path, &code(&c), &out, &[]);
		assert!(ok, "{err}");
		assert!(shown.contains("VISROL03 [3,0] slot 0"), "{shown}");
		if matches!(c, Command2::RetireRoot) {
			assert!(shown.contains("TERMINAL"), "{shown}");
		}
		accepted(&r, &bytes, &out);
	}
}

#[test]
fn build_writes_the_canonical_command_and_prints_its_code() {
	let mut r = Root::new();
	let (d, m, ctx) = (r.d(), r.fx.manifest(), r.ctx());
	let key = hex(&public(&[0x51; 32]));
	let out = r.io.ok(&[
		"build",
		"rotate-admin",
		"--dir",
		&d,
		"--context",
		&ctx,
		"--trusted-manifest",
		&m,
		"--key",
		&key,
		"--out",
		"rot",
	]);
	let expected = Command2::RotateAdmin(public(&[0x51; 32]));
	assert_eq!(
		r.read("rot.command"),
		expected.encode(GENERATION, CLUSTER).unwrap()
	);
	assert!(out.contains(&format!("code {}", code(&expected))), "{out}");
	let out = r.io.ok(&[
		"build",
		"retire-root",
		"--dir",
		&d,
		"--context",
		&ctx,
		"--trusted-manifest",
		&m,
		"--out",
		"ret",
	]);
	assert!(
		out.contains(&format!("code {}", code(&Command2::RetireRoot))),
		"{out}"
	);
	// A key already in the history is refused before anything is written.
	r.io.refused(
		&[
			"build",
			"rotate-admin",
			"--dir",
			&d,
			"--context",
			&ctx,
			"--trusted-manifest",
			&m,
			"--key",
			&hex(&public(&ADMIN_SEED)),
			"--out",
			"dup",
		],
		"KeyReused",
	);
	assert!(!r.fx.exists("dup.command"));
}

/// Two hosts sign their own slot of `c`; the partials merge; a second signer over the first's
/// partial yields the same proof.
fn joint(
	r: &mut Root,
	c: &Command2,
	first: (&str, Option<&str>),
	second: (&str, Option<&str>),
	tag: &str,
) {
	let (bytes, path) = r.command(c);
	let code = code(c);
	fn with(w: Option<&str>) -> Vec<&str> {
		w.map(|w| vec!["--with", w]).unwrap_or_default()
	}
	let (a, b) = (format!("{tag}-a"), format!("{tag}-b"));
	let (ok, _, err) = r.sign(first.0, &path, &code, &a, &with(first.1));
	assert!(ok, "{tag}: {err}");
	let (ok, _, err) = r.sign(second.0, &path, &code, &b, &with(second.1));
	assert!(ok, "{tag}: {err}");
	let (d, m, ctx) = (r.d(), r.fx.manifest(), r.ctx());
	let (pa, pb) = (
		r.fx.keys.join(format!("{a}.sidecar")),
		r.fx.keys.join(format!("{b}.sidecar")),
	);
	let merged = format!("{tag}-merged");
	r.io.ok(&[
		"merge",
		"--dir",
		&d,
		"--context",
		&ctx,
		"--trusted-manifest",
		&m,
		"--command",
		&path,
		"--partial",
		pa.to_str().unwrap(),
		"--partial",
		pb.to_str().unwrap(),
		"--out",
		&merged,
	]);
	let proof = r.read(&format!("{merged}.sidecar"));
	facade::self_check(&r.verified(), &bytes, Some(&proof)).unwrap();
	let over = format!("{tag}-over");
	let mut extra = with(second.1);
	extra.extend(["--partial", pa.to_str().unwrap()]);
	let (ok, _, err) = r.sign(second.0, &path, &code, &over, &extra);
	assert!(ok, "{tag}: {err}");
	assert_eq!(r.read(&format!("{over}.sidecar")), proof, "{tag}");
}

#[test]
fn t_k7_two_slot_cells_merge_across_hosts() {
	let mut r = Root::new();
	joint(
		&mut r,
		&Command2::RetireRoot,
		("admin0", Some("governance")),
		("gov0", Some("admin")),
		"retire",
	);
	joint(
		&mut r,
		&Command2::RotateRecovery(public(&[0x53; 32])),
		("admin0", None),
		("gov0", None),
		"rotrec",
	);
	joint(
		&mut r,
		&Command2::Unfreeze(log(9)),
		("admin0", Some("recovery")),
		("rec0", Some("admin")),
		"unf13",
	);
	joint(
		&mut r,
		&Command2::Unfreeze(log(9)),
		("gov0", Some("recovery")),
		("rec0", Some("governance")),
		"unf23",
	);
}

#[test]
fn t_k7_governance_family_and_statements_at_both_epochs() {
	let mut r = Root::new();
	for (n, c) in nine().iter().enumerate() {
		let (bytes, path) = r.command(c);
		for form in ["visadg01", "visrol03"] {
			let out = format!("g{n}-{form}");
			let (ok, _, err) = r.sign("gov0", &path, &code(c), &out, &["--sidecar", form]);
			assert!(ok, "{n} {form}: {err}");
			accepted(&r, &bytes, &out);
		}
	}
	for form in ["visads02", "visads03"] {
		let (ok, _, err) = r.statement("admin0", form, &format!("s0-{form}"));
		assert!(ok, "{form}: {err}");
		facade::self_check(&r.verified(), &r.read(&format!("s0-{form}.command")), None).unwrap();
	}
	// Governance and the administrator at epoch 1.
	r.rotate_and_bind(2, [0x52; 32], "gov1");
	r.rotate_and_bind(1, [0x51; 32], "admin1");
	for (n, c) in nine().iter().enumerate() {
		let (bytes, path) = r.command(c);
		let out = format!("g{n}-e1");
		let (ok, _, err) = r.sign("gov1", &path, &code(c), &out, &["--sidecar", "visrol03"]);
		assert!(ok, "{n}: {err}");
		accepted(&r, &bytes, &out);
		// Refused cell: VISADG01 after governance has rotated.
		let (ok, _, err) = r.sign("gov1", &path, &code(c), "late", &["--sidecar", "visadg01"]);
		assert!(!ok && err.contains("GovernanceRotated"), "{err}");
		assert!(!r.written("late"));
	}
	let (ok, _, err) = r.statement("admin1", "visads03", "s1");
	assert!(ok, "{err}");
	facade::self_check(&r.verified(), &r.read("s1.command"), None).unwrap();
	// Refused cell: VISADS02 after the administrator has rotated.
	let ctx = r.ctx();
	let code = r.statement_code(&ctx, "visads03");
	r.io.say(&[PRIMARY, &code]);
	let (d, m) = (r.d(), r.fx.manifest());
	r.io.refused(
		&[
			"sign-statement",
			"--dir",
			&d,
			"--key",
			"admin1",
			"--context",
			&ctx,
			"--trusted-manifest",
			&m,
			"--action",
			"grant",
			"--permission-key",
			&format!("{}:3", "4a".repeat(16)),
			"--sidecar",
			"visads02",
			"--out",
			"s2",
		],
		"AdminRotated",
	);
	assert!(!r.written("s2"));
}

#[test]
fn t_k7_refused_cells_are_typed_and_write_nothing() {
	let mut r = Root::new();
	for (what, c, key, extra, needle) in [
		(
			"Bootstrap",
			Command2::Bootstrap {
				epoch: 1,
				control: [1; 32],
				length: 241,
			},
			"rec0",
			vec![],
			"NotRoleSigned",
		),
		(
			"Cover",
			Command2::Cover {
				kind: 1,
				external: [2; 32],
			},
			"rec0",
			vec![],
			"NotRoleSigned",
		),
		(
			"VISADC01",
			nine().remove(7),
			"gov0",
			vec!["--sidecar", "visadc01"],
			"NoProductionForm",
		),
		(
			"wrong --with",
			Command2::RetireRoot,
			"admin0",
			vec!["--with", "recovery"],
			"WithChoice",
		),
		(
			"a role the requirement does not name",
			Command2::RotateAdmin(public(&[0x51; 32])),
			"gov0",
			vec![],
			"RoleNotNamed",
		),
	] {
		let (_, path) = r.command(&c);
		let (ok, _, err) = r.sign(key, &path, &code(&c), "refused", &extra);
		assert!(!ok && err.contains(needle), "{what}: {err}");
		assert!(!r.written("refused"), "{what}");
	}
}
