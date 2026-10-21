//! T-GEN-BIND, T-BIND, T-CTX, T-K5 and T-K6 through the command line the binary runs.
use super::support::*;
use crate::envelope::fingerprint;

/// A Pending key `name` of `role` for `epoch` from `seed`, generated without a context.
fn generate(fx: &Fixture, io: &mut TestIo, name: &str, role: &str, epoch: &str, seed: [u8; 32]) {
	io.say(&[PRIMARY, PRIMARY]).seed(seed);
	let out = io.ok(&[
		"generate",
		"--dir",
		&fx.dir(),
		"--role",
		role,
		"--intended-epoch",
		epoch,
		"--out",
		name,
	]);
	assert!(out.starts_with(&hex(&public(&seed))), "{out}");
}

/// Binds `name` to `out` with the given export files.
fn bind(fx: &Fixture, io: &mut TestIo, name: &str, out: &str, contexts: &[&str]) -> (bool, String) {
	io.say(&[PRIMARY]);
	let mut argv = vec![
		"bind",
		"--dir",
		fx.keys.to_str().unwrap(),
		"--key",
		name,
		"--out",
		out,
	];
	let manifest = fx.manifest();
	for c in contexts {
		argv.extend(["--context", c]);
	}
	argv.extend(["--trusted-manifest", &manifest]);
	let (ok, _, err) = io.run(&argv);
	io.secrets.clear();
	(ok, err)
}

fn show(fx: &Fixture, io: &mut TestIo, name: &str, passphrase: &str) -> String {
	io.say(&[passphrase]);
	io.ok(&["show-public", "--dir", &fx.dir(), "--key", name])
}

/// The genesis ceremony: three Pending role keys, bound after two node-distinct exports.
fn genesis(fx: &Fixture, io: &mut TestIo) -> World {
	let world = World::genesis();
	generate(fx, io, "admin.p", "admin", "0", ADMIN_SEED);
	generate(fx, io, "recovery.p", "recovery", "0", RECOVERY_SEED);
	let a = fx.file("genesis-a", &world.export(1));
	let b = fx.file("genesis-b", &world.export(2));
	for (p, out) in [("admin.p", "admin0"), ("recovery.p", "recovery0")] {
		let (ok, err) = bind(fx, io, p, out, &[&a, &b]);
		assert!(ok, "{err}");
	}
	world
}

#[test]
fn genesis_binds_only_with_two_exports_from_different_nodes() {
	let fx = Fixture::new();
	let mut io = fx.io();
	let world = World::genesis();
	generate(&fx, &mut io, "recovery.p", "recovery", "0", RECOVERY_SEED);
	let a = fx.file("a", &world.export(1));
	let a_again = fx.file("a-again", &world.export(1));
	let b = fx.file("b", &world.export(2));
	let (ok, err) = bind(&fx, &mut io, "recovery.p", "recovery0", &[&a]);
	assert!(!ok && err.contains("genesis"), "{err}");
	let (ok, err) = bind(&fx, &mut io, "recovery.p", "recovery0", &[&a, &a_again]);
	assert!(!ok && err.contains("same node"), "{err}");
	let other = fx.file("other", &World::other_root().export(2));
	let (ok, err) = bind(&fx, &mut io, "recovery.p", "recovery0", &[&a, &other]);
	assert!(!ok && err.contains("different contexts"), "{err}");
	assert!(fx.exists("recovery.p") && !fx.exists("recovery0"));
	// Control: two byte-identical exports from different nodes bind it.
	let (ok, err) = bind(&fx, &mut io, "recovery.p", "recovery0", &[&a, &b]);
	assert!(ok, "{err}");
	assert!(fx.exists("recovery0") && fx.exists("pending/recovery.p") && !fx.exists("recovery.p"));
	assert_eq!(
		show(&fx, &mut io, "recovery0", PRIMARY).trim(),
		hex(&public(&RECOVERY_SEED))
	);
	// The Pending original still decrypts in pending/, and can be backed up and restored.
	io.say(&[PRIMARY, SECONDARY, SECONDARY]);
	io.ok(&[
		"backup",
		"--dir",
		&fx.dir(),
		"--pending",
		"--key",
		"recovery.p",
		"--to",
		&fx.backups.display().to_string(),
		"--out",
		"recovery.p.bak",
	]);
	io.say(&[SECONDARY, NEWER, NEWER]);
	io.ok(&[
		"restore",
		"--dir",
		&fx.dir(),
		"--from",
		&fx.backups.display().to_string(),
		"--backup",
		"recovery.p.bak",
		"--out",
		"recovery.p.restored",
	]);
	let listing = io.ok(&["show-public", "--dir", &fx.dir(), "--unauthenticated"]);
	assert!(
		listing.contains("UNAUTHENTICATED recovery.p.restored state=Pending role=3 epoch=0"),
		"{listing}"
	);
	assert!(
		listing.lines().all(|l| l.starts_with("UNAUTHENTICATED ")),
		"{listing}"
	);
}

#[test]
fn a_successor_binds_at_the_epoch_its_rotation_received() {
	let fx = Fixture::new();
	let mut io = fx.io();
	let mut world = genesis(&fx, &mut io);
	let genesis_export = fx.file("g", &world.export(1));
	// Pending successors generated against the genesis context: one that will be rotated in, one
	// that never is, and one that asked for the wrong epoch.
	for (name, epoch, seed) in [("admin1.p", "1", [0x51; 32]), ("stray.p", "1", [0x52; 32])] {
		io.say(&[PRIMARY, PRIMARY]).seed(seed);
		io.ok(&[
			"generate",
			"--dir",
			&fx.dir(),
			"--role",
			"admin",
			"--intended-epoch",
			epoch,
			"--out",
			name,
			"--context",
			&genesis_export,
			"--trusted-manifest",
			&fx.manifest(),
		]);
	}
	generate(&fx, &mut io, "late.p", "admin", "2", [0x51; 32]);
	world.rotate(1, [0x51; 32]);
	let rotated = fx.file("r", &world.export(1));
	let (ok, err) = bind(&fx, &mut io, "stray.p", "stray", &[&rotated]);
	assert!(!ok && err.contains("does not hold this key"), "{err}");
	let (ok, err) = bind(&fx, &mut io, "late.p", "late", &[&rotated]);
	assert!(
		!ok && err.contains("role 1 epoch 1, not role 1 epoch 2"),
		"{err}"
	);
	// A context the Pending file was not generated for is refused.
	let mut other = World::other_root();
	other.rotate(1, [0x51; 32]);
	let foreign = fx.file("foreign", &other.export(1));
	let (ok, err) = bind(&fx, &mut io, "admin1.p", "admin1", &[&foreign]);
	assert!(!ok && err.contains("another context"), "{err}");
	// Control: the rotated history holds it at epoch 1; one authenticated export suffices.
	let (ok, err) = bind(&fx, &mut io, "admin1.p", "admin1", &[&rotated]);
	assert!(ok, "{err}");
	// The epoch it actually received can be named explicitly.
	io.say(&[PRIMARY]);
	io.ok(&[
		"bind",
		"--dir",
		&fx.dir(),
		"--key",
		"late.p",
		"--out",
		"late",
		"--epoch",
		"1",
		"--context",
		&rotated,
		"--trusted-manifest",
		&fx.manifest(),
	]);
}

#[test]
fn a_tampered_or_partial_export_and_a_wrong_manifest_are_refused() {
	let fx = Fixture::new();
	let mut io = fx.io();
	let mut world = genesis(&fx, &mut io);
	generate(&fx, &mut io, "admin1.p", "admin", "1", [0x51; 32]);
	world.rotate(1, [0x51; 32]);
	let mut forged = world.clone();
	forged.successors[0].sidecar[100] ^= 1;
	let mut partial = world.clone();
	partial.coverage = 0;
	for (what, export, needle) in [
		("tampered", forged.export(1), "Proof"),
		("partial", partial.export(1), "Coverage"),
	] {
		let f = fx.file(what, &export);
		let (ok, err) = bind(&fx, &mut io, "admin1.p", "admin1", &[&f]);
		assert!(!ok && err.contains(needle), "{what}: {err}");
	}
	let good = fx.file("good", &world.export(1));
	let mut digest_broken = world.export(1);
	*digest_broken.last_mut().unwrap() ^= 1;
	let broken = fx.file("broken", &digest_broken);
	let (ok, err) = bind(&fx, &mut io, "admin1.p", "admin1", &[&broken]);
	assert!(!ok && err.contains("Digest"), "{err}");
	io.say(&[PRIMARY]);
	io.refused(
		&[
			"bind",
			"--dir",
			&fx.dir(),
			"--key",
			"admin1.p",
			"--out",
			"admin1",
			"--context",
			&good,
			"--trusted-manifest",
			&fx.file("not-the-manifest", &[0; 241]),
		],
		"Manifest",
	);
	// Control: the verified chain binds.
	let (ok, err) = bind(&fx, &mut io, "admin1.p", "admin1", &[&good]);
	assert!(ok, "{err}");
}

#[test]
fn backup_restore_and_restore_verify_round_trip_and_refuse_mismatches() {
	let fx = Fixture::new();
	let mut io = fx.io();
	let world = genesis(&fx, &mut io);
	let ctx = fx.file("g", &world.export(1));
	let backups = fx.backups.display().to_string();
	// The backup passphrase must differ from the primary.
	io.say(&[PRIMARY, PRIMARY, PRIMARY]);
	io.refused(
		&[
			"backup",
			"--dir",
			&fx.dir(),
			"--key",
			"admin0",
			"--to",
			&backups,
			"--out",
			"admin0.bak",
		],
		"must differ",
	);
	io.say(&[PRIMARY, SECONDARY, SECONDARY]);
	io.ok(&[
		"backup",
		"--dir",
		&fx.dir(),
		"--key",
		"admin0",
		"--to",
		&backups,
		"--out",
		"admin0.bak",
	]);
	io.say(&[PRIMARY, SECONDARY, SECONDARY]);
	io.ok(&[
		"backup",
		"--dir",
		&fx.dir(),
		"--key",
		"recovery0",
		"--to",
		&backups,
		"--out",
		"recovery0.bak",
	]);
	let verify = |io: &mut TestIo, key: &str, backup: &str, ctx: &str, pass: &str| {
		io.say(&[pass]);
		io.run(&[
			"restore-verify",
			"--dir",
			&fx.dir(),
			"--key",
			key,
			"--from",
			&backups,
			"--backup",
			backup,
			"--context",
			ctx,
			"--trusted-manifest",
			&fx.manifest(),
		])
	};
	let (ok, out, err) = verify(&mut io, "admin0", "admin0.bak", &ctx, SECONDARY);
	assert!(ok && out.contains("backup verified"), "{err}");
	let (ok, _, err) = verify(&mut io, "admin0", "admin0.bak", &ctx, PRIMARY);
	assert!(!ok && err.contains("wrong passphrase"), "{err}");
	let (ok, _, err) = verify(&mut io, "recovery0", "admin0.bak", &ctx, SECONDARY);
	assert!(
		!ok && err.contains("not the Bound file's identity"),
		"{err}"
	);
	let foreign = fx.file("foreign", &World::other_root().export(1));
	let (ok, _, err) = verify(&mut io, "admin0", "admin0.bak", &foreign, SECONDARY);
	assert!(!ok && err.contains("not the backup's context"), "{err}");
	io.secrets.clear();
	// Restore under a new primary passphrase; the restored file shows the same key under it only.
	io.say(&[SECONDARY, NEWER, NEWER]);
	io.ok(&[
		"restore",
		"--dir",
		&fx.dir(),
		"--from",
		&backups,
		"--backup",
		"admin0.bak",
		"--out",
		"admin0.r",
	]);
	assert_eq!(
		show(&fx, &mut io, "admin0.r", NEWER).trim(),
		hex(&public(&ADMIN_SEED))
	);
	io.say(&[PRIMARY]);
	io.refused(
		&["show-public", "--dir", &fx.dir(), "--key", "admin0.r"],
		"wrong passphrase",
	);
	// A backup is not a primary: it cannot be bound or backed up again.
	io.say(&[PRIMARY]);
	io.refused(
		&[
			"restore",
			"--dir",
			&fx.dir(),
			"--from",
			&fx.dir(),
			"--backup",
			"admin0",
			"--out",
			"x",
		],
		"not a backup",
	);
}

#[test]
fn retire_needs_a_verified_later_epoch_and_destroy_a_second_source() {
	let fx = Fixture::new();
	let mut io = fx.io();
	let mut world = genesis(&fx, &mut io);
	let unrotated = fx.file("g", &world.export(1));
	let retire = |io: &mut TestIo, ctx: &str| {
		io.run(&[
			"retire-epoch",
			"--dir",
			&fx.dir(),
			"--key",
			"admin0",
			"--context",
			ctx,
			"--trusted-manifest",
			&fx.manifest(),
		])
	};
	let (ok, _, err) = retire(&mut io, &unrotated);
	assert!(!ok && err.contains("not retired"), "{err}");
	generate(&fx, &mut io, "admin1.p", "admin", "1", [0x51; 32]);
	world.rotate(1, [0x51; 32]);
	let mut forged = world.clone();
	forged.successors[0].sidecar[90] ^= 0x10;
	let (ok, _, err) = retire(&mut io, &fx.file("forged", &forged.export(1)));
	assert!(!ok && err.contains("Proof"), "{err}");
	let a = fx.file("a", &world.export(1));
	let b = fx.file("b", &world.export(2));
	let (ok, err) = bind(&fx, &mut io, "admin1.p", "admin1", &[&a]);
	assert!(ok, "{err}");
	// The operator accounts for the recorded backups: a wrong count changes nothing.
	io.say(&["1"]);
	let (ok, _, err) = retire(&mut io, &a);
	assert!(!ok && err.contains("confirmation"), "{err}");
	assert!(fx.exists("admin0"));
	io.say(&["0"]);
	let (ok, _, err) = retire(&mut io, &a);
	assert!(ok, "{err}");
	assert!(fx.exists("retired/admin0") && !fx.exists("admin0"));
	let destroy = |io: &mut TestIo, extra: &[&str]| {
		let mut argv = vec![
			"destroy",
			"--dir",
			fx.keys.to_str().unwrap(),
			"--key",
			"admin0",
			"--retired",
			"--context",
			&a,
		];
		let manifest = fx.manifest();
		argv.extend(["--trusted-manifest", &manifest]);
		argv.extend(extra);
		io.run(&argv)
	};
	io.say(&[PRIMARY]);
	let (ok, _, err) = destroy(&mut io, &[]);
	assert!(!ok && err.contains("second-source evidence"), "{err}");
	let fpr = fingerprint(&public(&ADMIN_SEED));
	io.say(&[PRIMARY, "0000000000000000"]);
	let (ok, _, err) = destroy(&mut io, &["--context", &b]);
	assert!(!ok && err.contains("confirmation"), "{err}");
	io.say(&[PRIMARY, PRIMARY, &fpr]);
	let (ok, _, err) = destroy(&mut io, &["--successor", "admin1"]);
	assert!(ok, "{err}");
	assert!(!fx.exists("retired/admin0"));
	io.secrets.clear();
	// The other second-source form, on the recovery key's Pending original: refused without the
	// evidence flags or a context showing its epoch active, accepted with both.
	let pending = |io: &mut TestIo, ctx: &str, evidence: &[&str]| {
		let mut argv = vec![
			"destroy",
			"--dir",
			fx.keys.to_str().unwrap(),
			"--pending",
			"--key",
			"admin.p",
			"--context",
			ctx,
		];
		let manifest = fx.manifest();
		argv.extend(["--trusted-manifest", &manifest]);
		argv.extend(evidence);
		io.run(&argv)
	};
	let digest = "ab".repeat(32);
	let evidence = [
		"--evidence",
		"accepted-proof",
		"--evidence-digest",
		digest.as_str(),
	];
	io.say(&[PRIMARY]);
	let (ok, _, err) = pending(&mut io, &a, &evidence);
	assert!(!ok && err.contains("not active"), "{err}");
	io.say(&[PRIMARY]);
	let (ok, _, err) = pending(&mut io, &unrotated, &[]);
	assert!(!ok && err.contains("--evidence is required"), "{err}");
	io.say(&[PRIMARY, &fpr]);
	let (ok, _, err) = pending(&mut io, &unrotated, &evidence);
	assert!(ok, "{err}");
	assert!(!fx.exists("pending/admin.p"));
	io.secrets.clear();
	io.refused(
		&[
			"destroy",
			"--dir",
			&fx.dir(),
			"--pending",
			"--key",
			"recovery.p",
			"--trusted-manifest",
			&fx.manifest(),
		],
		"--context is required",
	);
	assert!(fx.exists("pending/recovery.p"));
}

#[test]
fn destroy_pending_refuses_a_context_other_than_the_pending_files_own() {
	let fx = Fixture::new();
	let mut io = fx.io();
	let mut world = genesis(&fx, &mut io);
	let g = fx.file("g", &world.export(1));
	io.say(&[PRIMARY, PRIMARY]).seed([0x51; 32]);
	io.ok(&[
		"generate",
		"--dir",
		&fx.dir(),
		"--role",
		"admin",
		"--intended-epoch",
		"1",
		"--out",
		"admin1.p",
		"--context",
		&g,
		"--trusted-manifest",
		&fx.manifest(),
	]);
	world.rotate(1, [0x51; 32]);
	let r = fx.file("r", &world.export(1));
	let (ok, err) = bind(&fx, &mut io, "admin1.p", "admin1", &[&r]);
	assert!(ok, "{err}");
	// Another root over the same manifest, rotated to the same key: the key is active there too.
	let mut other = World::other_root();
	other.rotate(1, [0x51; 32]);
	let foreign = fx.file("foreign", &other.export(1));
	let fpr = fingerprint(&public(&[0x51; 32]));
	let digest = "ef".repeat(32);
	let destroy = |io: &mut TestIo, ctx: &str| {
		io.say(&[PRIMARY, &fpr]);
		let (ok, _, err) = io.run(&[
			"destroy",
			"--dir",
			&fx.dir(),
			"--pending",
			"--key",
			"admin1.p",
			"--context",
			ctx,
			"--trusted-manifest",
			&fx.manifest(),
			"--evidence",
			"accepted-proof",
			"--evidence-digest",
			&digest,
		]);
		io.secrets.clear();
		(ok, err)
	};
	let (ok, err) = destroy(&mut io, &foreign);
	assert!(
		!ok && err.contains("not the Pending file's context"),
		"{err}"
	);
	assert!(fx.exists("pending/admin1.p"));
	// Control: its own root's context.
	let (ok, err) = destroy(&mut io, &r);
	assert!(ok, "{err}");
	assert!(!fx.exists("pending/admin1.p"));
}

/// Holds `<keys>/<sub>`'s lock through an independently opened handle, creating it if absent.
fn hold(fx: &Fixture, sub: &str) -> key_guard::custody::CustodyDir {
	let path = fx.keys.join(sub);
	if !path.exists() {
		std::fs::create_dir(&path).unwrap();
		std::fs::set_permissions(&path, std::os::unix::fs::PermissionsExt::from_mode(0o700))
			.unwrap();
	}
	key_guard::custody::open_custody_dir_anchored(&path, &fx.root).unwrap()
}

#[test]
fn a_busy_destination_child_refuses_before_anything_is_published() {
	let fx = Fixture::new();
	let mut io = fx.io();
	let world = World::genesis();
	generate(&fx, &mut io, "recovery.p", "recovery", "0", RECOVERY_SEED);
	let a = fx.file("a", &world.export(1));
	let b = fx.file("b", &world.export(2));
	let pending = hold(&fx, "pending");
	let held = pending.lock().unwrap();
	let (ok, err) = bind(&fx, &mut io, "recovery.p", "recovery0", &[&a, &b]);
	assert!(!ok && err.contains("locked by another operation"), "{err}");
	assert!(fx.exists("recovery.p") && !fx.exists("recovery0") && !fx.exists("pending/recovery.p"));
	// Control: with the child free, the same bind publishes the Bound file and moves the original.
	drop(held);
	let (ok, err) = bind(&fx, &mut io, "recovery.p", "recovery0", &[&a, &b]);
	assert!(ok, "{err}");
	assert!(fx.exists("recovery0") && fx.exists("pending/recovery.p"));
}

#[test]
fn audit_lines_mark_identities_that_were_never_authenticated() {
	let fx = Fixture::new();
	let mut io = fx.io();
	let mut world = genesis(&fx, &mut io);
	let line = |fx: &Fixture| {
		crate::log::verify_chain(&fx.log())
			.unwrap()
			.0
			.pop()
			.unwrap()
	};
	// restore-verify: a refused comparison leaves the Bound file's identity unauthenticated; the
	// accepted one records the authenticated backup's identity.
	let backups = fx.backups.display().to_string();
	let ctx = fx.file("g", &world.export(1));
	io.say(&[PRIMARY, SECONDARY, SECONDARY]);
	io.ok(&[
		"backup",
		"--dir",
		&fx.dir(),
		"--key",
		"admin0",
		"--to",
		&backups,
		"--out",
		"admin0.bak",
	]);
	let verify = |io: &mut TestIo, key: &str| {
		io.say(&[SECONDARY]);
		let r = io.run(&[
			"restore-verify",
			"--dir",
			&fx.dir(),
			"--key",
			key,
			"--from",
			&backups,
			"--backup",
			"admin0.bak",
			"--context",
			&ctx,
			"--trusted-manifest",
			&fx.manifest(),
		]);
		io.secrets.clear();
		r.0
	};
	assert!(!verify(&mut io, "recovery0"));
	let refused = line(&fx);
	assert_eq!(refused.identity, None);
	assert!(refused.unauthenticated_identity.is_some());
	assert!(verify(&mut io, "admin0"));
	let accepted = line(&fx);
	assert_eq!(
		accepted.identity, accepted.unauthenticated_identity,
		"the claim, now authenticated"
	);
	assert!(accepted.identity.is_some());
	// retire-epoch decrypts nothing: its identity is logged as unauthenticated only.
	generate(&fx, &mut io, "admin1.p", "admin", "1", [0x51; 32]);
	world.rotate(1, [0x51; 32]);
	let rotated = fx.file("r", &world.export(1));
	io.say(&["1"]);
	io.ok(&[
		"retire-epoch",
		"--dir",
		&fx.dir(),
		"--key",
		"admin0",
		"--context",
		&rotated,
		"--trusted-manifest",
		&fx.manifest(),
	]);
	let retired = line(&fx);
	assert_eq!(retired.identity, None);
	assert_eq!(retired.unauthenticated_identity, accepted.identity);
	// Control: an operation that decrypts logs the authenticated identity.
	io.say(&[PRIMARY]);
	io.ok(&["show-public", "--dir", &fx.dir(), "--key", "recovery0"]);
	let shown = line(&fx);
	assert!(shown.identity.is_some() && shown.unauthenticated_identity.is_none());
}

#[test]
fn a_busy_retired_directory_refuses_retire_and_changes_nothing() {
	let fx = Fixture::new();
	let mut io = fx.io();
	let mut world = genesis(&fx, &mut io);
	world.rotate(1, [0x51; 32]);
	let rotated = fx.file("r", &world.export(1));
	let retired = hold(&fx, "retired");
	let held = retired.lock().unwrap();
	let retire = |io: &mut TestIo| {
		io.say(&["0"]);
		let r = io.run(&[
			"retire-epoch",
			"--dir",
			&fx.dir(),
			"--key",
			"admin0",
			"--context",
			&rotated,
			"--trusted-manifest",
			&fx.manifest(),
		]);
		io.secrets.clear();
		r
	};
	let (ok, _, err) = retire(&mut io);
	assert!(!ok && err.contains("locked by another operation"), "{err}");
	assert!(fx.exists("admin0") && !fx.exists("retired/admin0"));
	// Control: with retired/ free, the same retire moves the file.
	drop(held);
	let (ok, _, err) = retire(&mut io);
	assert!(ok, "{err}");
	assert!(!fx.exists("admin0") && fx.exists("retired/admin0"));
}
