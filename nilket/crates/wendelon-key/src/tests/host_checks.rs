//! T-HOST and T-K4: every recovery-role operation K4 lists refuses on a host with a node data
//! directory or an up, non-loopback interface; the override is explicit and logged; a clean host
//! proceeds.
use super::support::*;
use crate::host::HostFacts;
use crate::log::verify_chain;

fn dirty(kind: u8) -> HostFacts {
	HostFacts {
		data_dirs: if kind == 0 {
			vec!["/home/op/.local/share/nilket/local".into()]
		} else {
			vec![]
		},
		interfaces: if kind == 1 {
			vec!["eth0".into()]
		} else {
			vec![]
		},
		swap: false,
		hostname: "online-host".into(),
	}
}

/// The recovery-role operations of K4 with this slice's subcommands, as (secrets, argv).
fn operations(fx: &Fixture, ctx: &str) -> Vec<(Vec<&'static str>, Vec<String>)> {
	let d = fx.dir();
	let b = fx.backups.display().to_string();
	let m = fx.manifest();
	let s = |v: &[&str]| v.iter().map(|x| x.to_string()).collect::<Vec<_>>();
	vec![
		(
			vec![PRIMARY, PRIMARY],
			s(&[
				"generate",
				"--dir",
				&d,
				"--role",
				"recovery",
				"--intended-epoch",
				"0",
				"--out",
				"r2.p",
			]),
		),
		(
			vec![PRIMARY],
			s(&["show-public", "--dir", &d, "--key", "recovery0"]),
		),
		(
			vec![PRIMARY, SECONDARY, SECONDARY],
			s(&[
				"backup",
				"--dir",
				&d,
				"--key",
				"recovery0",
				"--to",
				&b,
				"--out",
				"r.bak",
			]),
		),
		(
			vec![SECONDARY],
			s(&[
				"restore-verify",
				"--dir",
				&d,
				"--key",
				"recovery0",
				"--from",
				&b,
				"--backup",
				"r.bak",
				"--context",
				ctx,
				"--trusted-manifest",
				&m,
			]),
		),
		(
			vec![SECONDARY, NEWER, NEWER],
			s(&[
				"restore",
				"--dir",
				&d,
				"--from",
				&b,
				"--backup",
				"r.bak",
				"--out",
				"r.restored",
			]),
		),
		(
			vec![PRIMARY],
			s(&[
				"bind",
				"--dir",
				&d,
				"--key",
				"r2.p",
				"--out",
				"r2",
				"--context",
				ctx,
				"--trusted-manifest",
				&m,
			]),
		),
	]
}

#[test]
fn every_recovery_operation_refuses_an_online_or_node_host_unless_overridden() {
	let fx = Fixture::new();
	let mut io = fx.io();
	let world = World::genesis();
	io.say(&[PRIMARY, PRIMARY]).seed(RECOVERY_SEED);
	io.ok(&[
		"generate",
		"--dir",
		&fx.dir(),
		"--role",
		"recovery",
		"--intended-epoch",
		"0",
		"--out",
		"recovery.p",
	]);
	let a = fx.file("a", &world.export(1));
	let b = fx.file("b", &world.export(2));
	io.say(&[PRIMARY]);
	io.ok(&[
		"bind",
		"--dir",
		&fx.dir(),
		"--key",
		"recovery.p",
		"--out",
		"recovery0",
		"--context",
		&a,
		"--context",
		&b,
		"--trusted-manifest",
		&fx.manifest(),
	]);
	for (step, (secrets, argv)) in operations(&fx, &a).into_iter().enumerate() {
		let argv: Vec<&str> = argv.iter().map(String::as_str).collect();
		for kind in [0, 1] {
			io.host = dirty(kind);
			io.say(&secrets);
			if step == 0 {
				io.seed(RECOVERY_SEED);
			}
			let err = io.refused(&argv, "fails the recovery-role checks");
			assert!(
				err.contains(if kind == 0 {
					"node data directory"
				} else {
					"eth0"
				}),
				"{err}"
			);
		}
		// Overridden on a dirty host, the operation proceeds and the override is logged.
		io.host = dirty(1);
		io.say(&secrets);
		if step == 0 {
			io.seed(RECOVERY_SEED);
		}
		let mut overridden = argv.clone();
		overridden.push("--override-host-checks");
		if step == 5 {
			// The genesis bind of the second Pending key needs both exports.
			overridden.extend(["--context", &b]);
		}
		io.ok(&overridden);
		let (lines, _) = verify_chain(&fx.log()).unwrap();
		let last = lines.last().unwrap();
		assert_eq!(last.outcome, "ok");
		assert_eq!(
			last.overrides,
			["host-checks: non-loopback interface eth0"],
			"{argv:?}"
		);
	}
	// Control: on a clean host the same operations need no override.
	io.host = HostFacts::default();
	io.say(&[PRIMARY]);
	io.ok(&["show-public", "--dir", &fx.dir(), "--key", "recovery0"]);
	// A non-recovery key is not subject to the recovery-role checks.
	io.host = dirty(1);
	io.say(&[PRIMARY, PRIMARY]).seed(ADMIN_SEED);
	io.ok(&[
		"generate",
		"--dir",
		&fx.dir(),
		"--role",
		"admin",
		"--intended-epoch",
		"0",
		"--out",
		"admin.p",
	]);
}

/// `destroy` of a recovery key (with its successor's file as the second source) and
/// `destroy --pending` of a recovery Pending original, each with every file decrypted checked.
#[test]
fn both_destroy_forms_refuse_an_online_or_node_host_unless_overridden() {
	let fx = Fixture::new();
	let mut io = fx.io();
	let mut world = World::genesis();
	let d = fx.dir();
	let m = fx.manifest();
	let fpr = crate::envelope::fingerprint(&public(&RECOVERY_SEED));
	io.say(&[PRIMARY, PRIMARY]).seed(RECOVERY_SEED);
	io.ok(&[
		"generate",
		"--dir",
		&d,
		"--role",
		"recovery",
		"--intended-epoch",
		"0",
		"--out",
		"recovery.p",
	]);
	let g1 = fx.file("g1", &world.export(1));
	let g2 = fx.file("g2", &world.export(2));
	io.say(&[PRIMARY]);
	io.ok(&[
		"bind",
		"--dir",
		&d,
		"--key",
		"recovery.p",
		"--out",
		"recovery0",
		"--context",
		&g1,
		"--context",
		&g2,
		"--trusted-manifest",
		&m,
	]);
	let evidence_digest = "cd".repeat(32);
	let pending = |ctx: &str| -> Vec<String> {
		[
			"destroy",
			"--dir",
			&d,
			"--pending",
			"--key",
			"recovery.p",
			"--context",
			ctx,
			"--trusted-manifest",
			&m,
			"--evidence",
			"crypto-valid-row",
			"--evidence-digest",
			&evidence_digest,
		]
		.iter()
		.map(|s| s.to_string())
		.collect()
	};
	let argv = pending(&g1);
	let argv: Vec<&str> = argv.iter().map(String::as_str).collect();
	for kind in [0, 1] {
		io.host = dirty(kind);
		io.say(&[PRIMARY, &fpr]);
		io.refused(&argv, "fails the recovery-role checks");
		assert!(fx.exists("pending/recovery.p"));
	}
	// Control: the same destroy on a clean host succeeds.
	io.host = HostFacts::default();
	io.say(&[PRIMARY, &fpr]);
	io.ok(&argv);
	assert!(!fx.exists("pending/recovery.p"));
	// A recovery rotation, its successor bound, the old epoch retired.
	world.rotate_recovery([0x71; 32]);
	io.say(&[PRIMARY, PRIMARY]).seed([0x71; 32]);
	io.ok(&[
		"generate",
		"--dir",
		&d,
		"--role",
		"recovery",
		"--intended-epoch",
		"1",
		"--out",
		"recovery1.p",
	]);
	let r1 = fx.file("r1", &world.export(1));
	io.say(&[PRIMARY]);
	io.ok(&[
		"bind",
		"--dir",
		&d,
		"--key",
		"recovery1.p",
		"--out",
		"recovery1",
		"--context",
		&r1,
		"--trusted-manifest",
		&m,
	]);
	io.say(&["0"]);
	io.ok(&[
		"retire-epoch",
		"--dir",
		&d,
		"--key",
		"recovery0",
		"--context",
		&r1,
		"--trusted-manifest",
		&m,
	]);
	let destroy = [
		"destroy",
		"--dir",
		&d,
		"--retired",
		"--key",
		"recovery0",
		"--context",
		&r1,
		"--trusted-manifest",
		&m,
		"--successor",
		"recovery1",
	];
	for kind in [0, 1] {
		io.host = dirty(kind);
		io.say(&[PRIMARY, PRIMARY, &fpr]);
		io.refused(&destroy, "fails the recovery-role checks");
		assert!(fx.exists("retired/recovery0"));
	}
	// Overridden on a dirty host it proceeds; both decrypted files (the target and the successor)
	// were checked, so both overrides are logged.
	io.host = dirty(1);
	io.say(&[PRIMARY, PRIMARY, &fpr]);
	let mut overridden = destroy.to_vec();
	overridden.push("--override-host-checks");
	io.ok(&overridden);
	assert!(!fx.exists("retired/recovery0"));
	let (lines, _) = verify_chain(&fx.log()).unwrap();
	let last = lines.last().unwrap();
	assert_eq!(
		(last.operation.as_str(), last.outcome.as_str()),
		("destroy", "ok")
	);
	assert_eq!(last.overrides.len(), 2, "{:?}", last.overrides);
	// Control: the next epoch's destroy on a clean host needs no override.
	io.host = HostFacts::default();
	world.rotate_recovery([0x72; 32]);
	io.say(&[PRIMARY, PRIMARY]).seed([0x72; 32]);
	io.ok(&[
		"generate",
		"--dir",
		&d,
		"--role",
		"recovery",
		"--intended-epoch",
		"2",
		"--out",
		"recovery2.p",
	]);
	let r2 = fx.file("r2", &world.export(1));
	io.say(&[PRIMARY]);
	io.ok(&[
		"bind",
		"--dir",
		&d,
		"--key",
		"recovery2.p",
		"--out",
		"recovery2",
		"--context",
		&r2,
		"--trusted-manifest",
		&m,
	]);
	io.say(&["0"]);
	io.ok(&[
		"retire-epoch",
		"--dir",
		&d,
		"--key",
		"recovery1",
		"--context",
		&r2,
		"--trusted-manifest",
		&m,
	]);
	let fpr1 = crate::envelope::fingerprint(&public(&[0x71; 32]));
	io.say(&[PRIMARY, PRIMARY, &fpr1]);
	io.ok(&[
		"destroy",
		"--dir",
		&d,
		"--retired",
		"--key",
		"recovery1",
		"--context",
		&r2,
		"--trusted-manifest",
		&m,
		"--successor",
		"recovery2",
	]);
	assert!(!fx.exists("retired/recovery1"));
	let (lines, _) = verify_chain(&fx.log()).unwrap();
	assert!(lines.last().unwrap().overrides.is_empty());
	// The newest Pending original: refused on a dirty host, then overridden there and logged.
	let fpr2 = crate::envelope::fingerprint(&public(&[0x72; 32]));
	let argv = pending(&r2);
	let mut argv: Vec<&str> = argv.iter().map(String::as_str).collect();
	argv[5] = "recovery2.p";
	io.host = dirty(0);
	io.say(&[PRIMARY, &fpr2]);
	io.refused(&argv, "fails the recovery-role checks");
	io.say(&[PRIMARY, &fpr2]);
	argv.push("--override-host-checks");
	io.ok(&argv);
	assert!(!fx.exists("pending/recovery2.p"));
	let (lines, _) = verify_chain(&fx.log()).unwrap();
	let last = lines.last().unwrap();
	assert_eq!(
		(last.operation.as_str(), last.outcome.as_str()),
		("destroy-pending", "ok")
	);
	assert_eq!(
		last.overrides,
		["host-checks: node data directory /home/op/.local/share/nilket/local"]
	);
}
