//! T-WYS (the bytes signed are the bytes shown; a wrong code writes nothing), T-MERGE (any
//! difference outside the slot ranges is refused) and the 5c leg of T-K5.
use super::support::*;
use ed25519_dalek::{Signature, VerifyingKey};
use wendelon::storage::mutation_visibility::root2::Command2;
use wendelon::storage::mutation_visibility::root2::operator_proof as facade;

use super::signing::{Root, accepted, code};

#[test]
fn what_is_signed_is_what_was_shown_and_a_wrong_code_writes_nothing() {
	let mut r = Root::new();
	let c = Command2::RotateAdmin(public(&[0x51; 32]));
	let (bytes, path) = r.command(&c);
	// The digest prefix is not a rotation's code: refused, nothing written, and logged.
	let digest = hex(&c.digest(GENERATION, CLUSTER).unwrap());
	let (ok, shown, err) = r.sign("rec0", &path, &digest[..32], "wys", &[]);
	assert!(!ok && err.contains("confirmation"), "{err}");
	assert!(!r.written("wys"));
	assert!(
		!shown.contains(&hex(&public(&[0x51; 32]))),
		"the new key is withheld before the code: {shown}"
	);
	assert!(
		!shown.contains(&digest),
		"the digest is withheld before the code: {shown}"
	);
	let log = String::from_utf8(r.fx.log()).unwrap();
	assert!(
		log.lines()
			.last()
			.unwrap()
			.contains("the confirmation does not match")
	);
	// A code agreeing with the right one only in its first 16 hex digits (64 bits) is refused.
	let half = format!("{}{}", &code(&c)[..16], "0".repeat(16));
	assert_ne!(half, code(&c));
	let (ok, _, err) = r.sign("rec0", &path, &half, "wys", &[]);
	assert!(!ok && err.contains("confirmation"), "{err}");
	assert!(!r.written("wys"));
	// The correct code: the displayed digest, preimage hash and key are those of the written files.
	let (ok, shown, err) = r.sign("rec0", &path, &code(&c), "wys", &[]);
	assert!(ok, "{err}");
	let field = |prefix: &str| {
		shown
			.lines()
			.find_map(|l| l.strip_prefix(prefix))
			.unwrap_or_else(|| panic!("{prefix} in {shown}"))
			.split_whitespace()
			.next()
			.unwrap()
			.to_string()
	};
	let written = r.read("wys.command");
	assert_eq!(written, bytes);
	let (g, cl, decoded) = Command2::decode(&written).unwrap();
	assert_eq!(
		field("command digest "),
		hex(&decoded.digest(g, cl).unwrap())
	);
	assert_eq!(field("new administrator key "), hex(&public(&[0x51; 32])));
	let rebuilt = facade::prepare(
		&r.verified(),
		&written,
		&public(&RECOVERY_SEED),
		None,
		None,
		None,
	)
	.unwrap();
	let preimage_hash = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, rebuilt.preimage());
	assert_eq!(field("preimage sha256 "), hex(preimage_hash.as_ref()));
	let sidecar = r.read("wys.sidecar");
	let signature_hex = hex(&sidecar[86..150]);
	assert!(
		!shown.contains(&signature_hex),
		"the signature is only in the written file"
	);
	assert!(
		!String::from_utf8(r.fx.log())
			.unwrap()
			.contains(&signature_hex)
	);
	let signature = Signature::from_bytes(&sidecar[86..150].try_into().unwrap());
	let recovery = VerifyingKey::from_bytes(&public(&RECOVERY_SEED)).unwrap();
	recovery
		.verify_strict(rebuilt.preimage(), &signature)
		.unwrap();
	// Control: the same signature is not over another command's preimage, and that command
	// shows another digest.
	let other = Command2::RotateAdmin(public(&[0x52; 32]))
		.encode(GENERATION, CLUSTER)
		.unwrap();
	let rebuilt = facade::prepare(
		&r.verified(),
		&other,
		&public(&RECOVERY_SEED),
		None,
		None,
		None,
	)
	.unwrap();
	assert!(
		recovery
			.verify_strict(rebuilt.preimage(), &signature)
			.is_err()
	);
	let (_, _, other_decoded) = Command2::decode(&other).unwrap();
	assert_ne!(
		hex(&other_decoded.digest(g, cl).unwrap()),
		field("command digest ")
	);
	// A statement's wrong code writes nothing either.
	let ctx = r.ctx();
	let (d, m) = (r.d(), r.fx.manifest());
	r.io.say(&[PRIMARY, "0123456789abcdef0123456789abcdef"]);
	r.io.refused(
		&[
			"sign-statement",
			"--dir",
			&d,
			"--key",
			"admin0",
			"--context",
			&ctx,
			"--trusted-manifest",
			&m,
			"--action",
			"grant",
			"--permission-key",
			&format!("{}:3", "4a".repeat(16)),
			"--out",
			"st",
		],
		"confirmation",
	);
	assert!(!r.written("st"));
}

#[test]
fn a_merge_refuses_every_inconsistency_and_any_byte_outside_the_slots() {
	let mut r = Root::new();
	let c = Command2::RotateRecovery(public(&[0x53; 32]));
	let (bytes, path) = r.command(&c);
	assert!(r.sign("admin0", &path, &code(&c), "pa", &[]).0);
	assert!(r.sign("gov0", &path, &code(&c), "pg", &[]).0);
	let c2 = Command2::RotateRecovery(public(&[0x54; 32]));
	let (_, path2) = r.command(&c2);
	assert!(r.sign("gov0", &path2, &code(&c2), "pg2", &[]).0);
	let pa = r.read("pa.sidecar");
	let pg = r.read("pg.sidecar");
	let pg2 = r.read("pg2.sidecar");
	let ctx = r.ctx();
	let foreign = r.file(&World::other_root().export(1));
	fn merge(r: &mut Root, path: &str, ctx: &str, a: &[u8], b: &[u8], out: &str) -> (bool, String) {
		let (d, m) = (r.d(), r.fx.manifest());
		let (fa, fb) = (r.file(a), r.file(b));
		let res = r.io.run(&[
			"merge",
			"--dir",
			&d,
			"--context",
			ctx,
			"--trusted-manifest",
			&m,
			"--command",
			&path,
			"--partial",
			&fa,
			"--partial",
			&fb,
			"--out",
			out,
		]);
		(res.0, res.2)
	}
	let flipped = |p: &[u8], at: usize| {
		let mut p = p.to_vec();
		p[at] ^= 1;
		p
	};
	for (what, ctx, a, b, needle) in [
		(
			"mismatched commands",
			ctx.as_str(),
			pa.clone(),
			pg2,
			"Mismatch",
		),
		(
			"another root's context",
			foreign.as_str(),
			pa.clone(),
			pg.clone(),
			"Layout",
		),
		(
			"a slot filled twice",
			ctx.as_str(),
			pa.clone(),
			pa.clone(),
			"Overwritten",
		),
		(
			"an invalid first slot",
			ctx.as_str(),
			flipped(&pa, 100),
			pg.clone(),
			"SlotSignature",
		),
		(
			"a slot header byte",
			ctx.as_str(),
			pa.clone(),
			flipped(&pg, 80),
			"Mismatch",
		),
		(
			"the version byte",
			ctx.as_str(),
			pa.clone(),
			flipped(&pg, 9),
			"Mismatch",
		),
		(
			"the command digest",
			ctx.as_str(),
			pa.clone(),
			flipped(&pg, 50),
			"Mismatch",
		),
	] {
		let (ok, err) = merge(&mut r, &path, ctx, &a, &b, "m");
		assert!(!ok && err.contains(needle), "{what}: {err}");
		assert!(!r.fx.exists("m.sidecar"), "{what}");
	}
	// Control: partials differing only inside the slot ranges merge, and the result is checked.
	let (ok, err) = merge(&mut r, &path, &ctx, &pa, &pg, "m");
	assert!(ok, "{err}");
	facade::self_check(&r.verified(), &bytes, Some(&r.read("m.sidecar"))).unwrap();
}

#[test]
fn a_restored_key_signs_and_a_wrong_passphrase_does_not() {
	let mut r = Root::new();
	let (d, b) = (r.d(), r.fx.backups.display().to_string());
	r.io.say(&[PRIMARY, SECONDARY, SECONDARY]);
	r.io.ok(&[
		"backup", "--dir", &d, "--key", "rec0", "--to", &b, "--out", "rec0.bak",
	]);
	r.io.say(&[SECONDARY, NEWER, NEWER]);
	r.io.ok(&[
		"restore", "--dir", &d, "--from", &b, "--backup", "rec0.bak", "--out", "rec0.r",
	]);
	let c = Command2::RotateAdmin(public(&[0x51; 32]));
	let (bytes, path) = r.command(&c);
	let ctx = r.ctx();
	r.io.say(&[NEWER, &code(&c)]);
	let m = r.fx.manifest();
	r.io.ok(&[
		"sign",
		"--dir",
		&d,
		"--key",
		"rec0.r",
		"--context",
		&ctx,
		"--trusted-manifest",
		&m,
		"--command",
		&path,
		"--out",
		"rs",
	]);
	accepted(&r, &bytes, "rs");
	r.io.say(&[PRIMARY, &code(&c)]);
	r.io.refused(
		&[
			"sign",
			"--dir",
			&d,
			"--key",
			"rec0.r",
			"--context",
			&ctx,
			"--trusted-manifest",
			&m,
			"--command",
			&path,
			"--out",
			"rs2",
		],
		"wrong passphrase",
	);
	assert!(!r.written("rs2"));
}
