//! `sign containment` (D-21) through the tool: the facade's vectors replay byte for byte, the 5c
//! signing authority holds, the 32-hex code is the containment digest's prefix, and the
//! recovery-role host checks apply.
use super::signing::Root;
use super::support::*;
use crate::host::HostFacts;
use crate::log::verify_chain;
use serde_json::Value;
use wendelon::storage::mutation_visibility::root2::operator_proof::{
	self as facade, verify_context_export,
};

const VECTORS: &str = include_str!(
	"../../../wendelon/src/storage/mutation_visibility/root2/tests/compromise/containment_vectors.json"
);

const ROOT_KEY: [u8; 32] = [0x52; 32];
const NONCE: [u8; 16] = [0x6e; 16];

/// Runs `sign containment` with `key`, typing `code`.
fn contain(
	r: &mut Root,
	ctx: &str,
	key: &str,
	rk: &[u8],
	node: u32,
	nonce: &[u8],
	on: bool,
	code: &str,
	out: &str,
	extra: &[&str],
) -> (bool, String, String) {
	r.io.say(&[PRIMARY, code]);
	let (d, m) = (r.d(), r.fx.manifest());
	let (rk, node, nonce) = (hex(rk), node.to_string(), hex(nonce));
	let mut argv = vec![
		"sign",
		"containment",
		"--dir",
		&d,
		"--key",
		key,
		"--context",
		ctx,
		"--trusted-manifest",
		&m,
		"--root-key",
		&rk,
		"--node-id",
		&node,
		"--boot-nonce",
		&nonce,
		"--state",
		if on { "on" } else { "off" },
		"--out",
		out,
	];
	argv.extend(extra);
	let res = r.io.run(&argv);
	r.io.secrets.clear();
	res
}

/// The containment digest the facade lays out for these inputs (the code's source).
fn digest(r: &Root, rk: [u8; 32], node: u32, nonce: [u8; 16], on: bool) -> String {
	let u = facade::prepare_containment(
		&r.verified(),
		&public(&RECOVERY_SEED),
		&rk,
		node,
		&nonce,
		on,
	)
	.unwrap();
	hex(&u.layout()[44..76])
}

#[test]
fn the_facade_vectors_replay_byte_for_byte_through_the_tool() {
	let v: Value = serde_json::from_str(VECTORS).unwrap();
	let mut r = Root::new();
	let export = unhex(v["export"].as_str().unwrap());
	let manifest = unhex(v["manifest"].as_str().unwrap());
	let ctx = r.file(&export);
	let m = r.file(&manifest);
	let seed = [v["recovery_seed"].as_u64().unwrap() as u8; 32];
	let context: [u8; 188] = export[12..200].try_into().unwrap();
	r.plant("vector-recovery", seed, 3, 0, context);
	let verified = verify_context_export(&export, &manifest).unwrap();
	for (n, row) in v["vectors"].as_array().unwrap().iter().enumerate() {
		let rk = unhex(row["root_key"].as_str().unwrap());
		let nonce = unhex(row["boot_nonce"].as_str().unwrap());
		let (node, on) = (
			row["node_id"].as_u64().unwrap() as u32,
			row["on"].as_bool().unwrap(),
		);
		let code = &row["digest"].as_str().unwrap()[..32];
		let out = format!("vector{n}");
		r.io.say(&[PRIMARY, code]);
		let d = r.d();
		let (rk_hex, node_s, nonce_hex) = (hex(&rk), node.to_string(), hex(&nonce));
		let (ok, shown, err) = r.io.run(&[
			"sign",
			"containment",
			"--dir",
			&d,
			"--key",
			"vector-recovery",
			"--context",
			&ctx,
			"--trusted-manifest",
			&m,
			"--root-key",
			&rk_hex,
			"--node-id",
			&node_s,
			"--boot-nonce",
			&nonce_hex,
			"--state",
			if on { "on" } else { "off" },
			"--out",
			&out,
		]);
		r.io.secrets.clear();
		assert!(ok, "{n}: {err}");
		assert!(
			shown.contains(&format!(
				"containment digest {}",
				row["digest"].as_str().unwrap()
			)),
			"{shown}"
		);
		let proof = r.read(&format!("{out}.containment"));
		assert_eq!(hex(&proof), row["proof"].as_str().unwrap(), "vector {n}");
		facade::self_check_containment(
			&verified,
			&rk.try_into().unwrap(),
			node,
			&nonce.try_into().unwrap(),
			on,
			&proof,
		)
		.unwrap();
	}
}

#[test]
fn containment_needs_the_active_bound_recovery_key_and_the_full_code() {
	let mut r = Root::new();
	let ctx = r.ctx();
	let code = digest(&r, ROOT_KEY, 4, NONCE, true)[..32].to_string();
	// Not the recovery role.
	for key in ["admin0", "gov0"] {
		let (ok, _, err) = contain(
			&mut r,
			&ctx,
			key,
			&ROOT_KEY,
			4,
			&NONCE,
			true,
			&code,
			"c",
			&[],
		);
		assert!(
			!ok && err.contains("RoleNotNamed") && !r.fx.exists("c.containment"),
			"{key}: {err}"
		);
	}
	// A Pending file, and a context that is not the file's.
	r.generate("rec.again", "recovery", "0", RECOVERY_SEED);
	let (ok, _, err) = contain(
		&mut r,
		&ctx,
		"rec.again",
		&ROOT_KEY,
		4,
		&NONCE,
		true,
		&code,
		"c",
		&[],
	);
	assert!(
		!ok && err.contains("only a Bound") && !r.fx.exists("c.containment"),
		"{err}"
	);
	let foreign = r.file(&World::other_root().export(1));
	let (ok, _, err) = contain(
		&mut r,
		&foreign,
		"rec0",
		&ROOT_KEY,
		4,
		&NONCE,
		true,
		&code,
		"c",
		&[],
	);
	assert!(
		!ok && err.contains("trust anchor") && !r.fx.exists("c.containment"),
		"{err}"
	);
	// A wrong code, and one matching only the first 16 hex digits, write nothing.
	let half = format!("{}{}", &code[..16], "0".repeat(16));
	for wrong in [hex(&[0x11; 16]), half] {
		let (ok, shown, err) = contain(
			&mut r,
			&ctx,
			"rec0",
			&ROOT_KEY,
			4,
			&NONCE,
			true,
			&wrong,
			"c",
			&[],
		);
		assert!(
			!ok && err.contains("confirmation") && !r.fx.exists("c.containment"),
			"{err}"
		);
		assert!(
			!shown.contains(&digest(&r, ROOT_KEY, 4, NONCE, true)),
			"the digest is withheld: {shown}"
		);
	}
	// Control: the active recovery key with the full code writes a proof the facade accepts.
	let (ok, shown, err) = contain(
		&mut r,
		&ctx,
		"rec0",
		&ROOT_KEY,
		4,
		&NONCE,
		true,
		&code,
		"c",
		&[],
	);
	assert!(ok, "{err}");
	assert!(shown.contains("containment override ON"), "{shown}");
	let proof = r.read("c.containment");
	facade::self_check_containment(&r.verified(), &ROOT_KEY, 4, &NONCE, true, &proof).unwrap();
	let (lines, _) = verify_chain(&r.fx.log()).unwrap();
	let last = lines.last().unwrap();
	assert_eq!(
		last.command_digest.as_deref(),
		Some(digest(&r, ROOT_KEY, 4, NONCE, true).as_str())
	);
	assert_eq!(
		last.form_slot.as_deref(),
		Some("VISROL03 [3,0] slot 0 containment")
	);
	// A stale recovery epoch: after a recovery rotation, epoch 0 no longer signs; epoch 1 does.
	r.rotate_and_bind(3, [0x71; 32], "rec1");
	let ctx = r.ctx();
	let (ok, _, err) = contain(
		&mut r,
		&ctx,
		"rec0",
		&ROOT_KEY,
		4,
		&NONCE,
		false,
		&code,
		"stale",
		&[],
	);
	assert!(
		!ok && err.contains("StaleEpoch") && !r.fx.exists("stale.containment"),
		"{err}"
	);
	let u = facade::prepare_containment(
		&r.verified(),
		&public(&[0x71; 32]),
		&ROOT_KEY,
		4,
		&NONCE,
		false,
	)
	.unwrap();
	let fresh = hex(&u.layout()[44..76])[..32].to_string();
	let (ok, _, err) = contain(
		&mut r,
		&ctx,
		"rec1",
		&ROOT_KEY,
		4,
		&NONCE,
		false,
		&fresh,
		"fresh",
		&[],
	);
	assert!(ok, "{err}");
}

#[test]
fn containment_signing_refuses_an_online_host_unless_overridden() {
	let mut r = Root::new();
	let ctx = r.ctx();
	let code = digest(&r, ROOT_KEY, 4, NONCE, true)[..32].to_string();
	r.io.host = HostFacts {
		interfaces: vec!["eth0".into()],
		..HostFacts::default()
	};
	let (ok, _, err) = contain(
		&mut r,
		&ctx,
		"rec0",
		&ROOT_KEY,
		4,
		&NONCE,
		true,
		&code,
		"h",
		&[],
	);
	assert!(
		!ok && err.contains("fails the recovery-role checks") && !r.fx.exists("h.containment"),
		"{err}"
	);
	let (ok, _, err) = contain(
		&mut r,
		&ctx,
		"rec0",
		&ROOT_KEY,
		4,
		&NONCE,
		true,
		&code,
		"h",
		&["--override-host-checks"],
	);
	assert!(ok, "{err}");
	let (lines, _) = verify_chain(&r.fx.log()).unwrap();
	assert_eq!(
		lines.last().unwrap().overrides,
		["host-checks: non-loopback interface eth0"]
	);
	// Control: a clean host signs without an override.
	r.io.host = HostFacts::default();
	let (ok, _, err) = contain(
		&mut r,
		&ctx,
		"rec0",
		&ROOT_KEY,
		4,
		&NONCE,
		true,
		&code,
		"h2",
		&[],
	);
	assert!(ok, "{err}");
}

#[test]
fn containment_is_refused_for_a_retired_root() {
	let mut r = Root::new();
	let code = digest(&r, ROOT_KEY, 4, NONCE, true)[..32].to_string();
	r.world.retired = true;
	let retired = r.ctx();
	let (ok, _, err) = contain(
		&mut r,
		&retired,
		"rec0",
		&ROOT_KEY,
		4,
		&NONCE,
		true,
		&code,
		"c",
		&[],
	);
	assert!(
		!ok && err.contains("Retired") && !r.fx.exists("c.containment"),
		"{err}"
	);
	// Control: the same root, not retired, signs.
	r.world.retired = false;
	let live = r.ctx();
	let (ok, _, err) = contain(
		&mut r,
		&live,
		"rec0",
		&ROOT_KEY,
		4,
		&NONCE,
		true,
		&code,
		"c",
		&[],
	);
	assert!(ok, "{err}");
}
