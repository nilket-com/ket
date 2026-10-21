//! T-SIGN: each K3 `sign` refusal writes nothing, and each is paired with a run that differs only
//! in that condition and signs. Plus the 5c rows of T-HOST.
use super::signing::{Root, code, nine};
use super::support::*;
use crate::host::HostFacts;
use crate::log::verify_chain;
use wendelon::storage::mutation_visibility::root2::Command2;

/// Signs `c` with `key` under the current context: refused with `needle`, or `None` to sign.
fn expect(r: &mut Root, key: &str, c: &Command2, extra: &[&str], needle: Option<&str>, what: &str) {
	let (_, path) = r.command(c);
	let (ok, _, err) = r.sign(key, &path, &code(c), "out", extra);
	match needle {
		Some(needle) => {
			assert!(!ok && err.contains(needle), "{what}: {err}");
			assert!(!r.written("out"), "{what}: something was written");
		}
		None => {
			assert!(ok, "{what} (control): {err}");
			for f in ["out.command", "out.sidecar"] {
				std::fs::remove_file(r.fx.keys.join(f)).ok();
			}
		}
	}
}

#[test]
fn each_sign_refusal_is_refused_and_its_control_signs() {
	let mut r = Root::new();
	let rotate = Command2::RotateGovernance(public(&[0x52; 32]));
	// Pending: a Pending file of the same recovery key.
	r.generate("rec.again", "recovery", "0", RECOVERY_SEED);
	expect(
		&mut r,
		"rec.again",
		&rotate,
		&[],
		Some("only a Bound"),
		"Pending",
	);
	expect(&mut r, "rec0", &rotate, &[], None, "Pending");
	// The header's context is not the verified one.
	let foreign = r.file(&World::other_root().export(1));
	let (_, path) = r.command(&rotate);
	let (ok, _, err) = r.sign_in(&foreign, "rec0", &path, &code(&rotate), "out", &[]);
	assert!(
		!ok && err.contains("trust anchor") && !r.written("out"),
		"{err}"
	);
	expect(&mut r, "rec0", &rotate, &[], None, "context");
	// A key the history does not hold, and a header epoch the history does not give it.
	r.plant("stray", [0x99; 32], 3, 0, context_bytes(0xa3));
	expect(
		&mut r,
		"stray",
		&rotate,
		&[],
		Some("does not hold this key"),
		"not in history",
	);
	r.plant("skewed", RECOVERY_SEED, 3, 1, context_bytes(0xa3));
	expect(
		&mut r,
		"skewed",
		&rotate,
		&[],
		Some("role 3 epoch 0, not role 3 epoch 1"),
		"epoch",
	);
	r.plant("faithful", RECOVERY_SEED, 3, 0, context_bytes(0xa3));
	expect(&mut r, "faithful", &rotate, &[], None, "planted control");
	// A role the requirement does not name.
	let rotate_admin = Command2::RotateAdmin(public(&[0x51; 32]));
	expect(
		&mut r,
		"gov0",
		&rotate_admin,
		&[],
		Some("RoleNotNamed"),
		"role",
	);
	expect(&mut r, "rec0", &rotate_admin, &[], None, "role");
	// A retired root.
	r.world.retired = true;
	expect(
		&mut r,
		"rec0",
		&rotate,
		&[],
		Some("Retired"),
		"retired root",
	);
	r.world.retired = false;
	expect(&mut r, "rec0", &rotate, &[], None, "retired root");
	// A frozen root: a governance command, and a statement.
	let issuer = nine().remove(7);
	let unfrozen = r.ctx();
	let code = r.statement_code(&unfrozen, "visads03");
	r.world.frozen = true;
	expect(
		&mut r,
		"gov0",
		&issuer,
		&["--sidecar", "visadg01"],
		Some("Frozen"),
		"frozen governance",
	);
	let (ok, _, err) = r.statement_with("admin0", "visads03", "st", &code);
	assert!(!ok && err.contains("Frozen") && !r.written("st"), "{err}");
	r.world.frozen = false;
	expect(
		&mut r,
		"gov0",
		&issuer,
		&["--sidecar", "visadg01"],
		None,
		"frozen governance",
	);
	let (ok, _, err) = r.statement("admin0", "visads03", "st");
	assert!(ok, "{err}");
	// A stale (retired) epoch: after the administrator rotates, epoch 0 no longer signs.
	r.rotate_and_bind(1, [0x51; 32], "admin1");
	let (ok, _, err) = r.statement("admin0", "visads03", "stale");
	assert!(
		!ok && err.contains("StaleEpoch") && !r.written("stale"),
		"{err}"
	);
	let (ok, _, err) = r.statement("admin1", "visads03", "fresh");
	assert!(ok, "{err}");
}

#[test]
fn signing_a_recovery_proof_refuses_an_online_host_unless_overridden() {
	let mut r = Root::new();
	let c = Command2::RotateAdmin(public(&[0x51; 32]));
	r.io.host = HostFacts {
		interfaces: vec!["eth0".into()],
		..HostFacts::default()
	};
	expect(
		&mut r,
		"rec0",
		&c,
		&[],
		Some("fails the recovery-role checks"),
		"dirty host",
	);
	expect(
		&mut r,
		"rec0",
		&c,
		&["--override-host-checks"],
		None,
		"overridden",
	);
	let (lines, _) = verify_chain(&r.fx.log()).unwrap();
	assert_eq!(
		lines.last().unwrap().overrides,
		["host-checks: non-loopback interface eth0"]
	);
	// The recovery-role checks do not apply to an administrator statement.
	let (ok, _, err) = r.statement("admin0", "visads03", "st");
	assert!(ok, "{err}");
	// Control: a clean host signs without an override.
	r.io.host = HostFacts::default();
	expect(&mut r, "rec0", &c, &[], None, "clean host");
}
