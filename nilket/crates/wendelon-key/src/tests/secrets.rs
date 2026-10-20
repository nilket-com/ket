//! T-SECRET: across a full ceremony (generate, bind, show, backup, restore, retire, destroy and
//! refusals), no passphrase, seed byte string or seed hex appears in stdout, stderr, the log, or
//! any error's `Display` or `Debug`. T-K3z for the tool: seeds and passphrases are only ever held
//! in key-guard's `SecretBuf`, whose wipe key-guard's own T-K3z proves.
use super::support::*;
use crate::error::ToolError;
use std::path::Path;

/// Every `.rs` file under `src`, by its path relative to `src`, read from disk now.
pub(crate) fn sources() -> Vec<(String, String)> {
	fn visit(root: &Path, dir: &Path, out: &mut Vec<(String, String)>) {
		for e in std::fs::read_dir(dir).unwrap() {
			let p = e.unwrap().path();
			if p.is_dir() {
				visit(root, &p, out);
			} else if p.extension().is_some_and(|x| x == "rs") {
				let rel = p.strip_prefix(root).unwrap().to_string_lossy().into_owned();
				out.push((rel, std::fs::read_to_string(&p).unwrap()));
			}
		}
	}
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let mut out = Vec::new();
	visit(&root, &root, &mut out);
	out.sort();
	out
}

fn needles() -> Vec<String> {
	let mut n: Vec<String> = [PRIMARY, SECONDARY, NEWER]
		.iter()
		.map(|s| s.to_string())
		.collect();
	for seed in [ADMIN_SEED, RECOVERY_SEED, [0x51; 32]] {
		n.push(hex(&seed));
		n.push(format!("{seed:?}").replace(['[', ']'], ""));
	}
	n
}

fn leaks(text: &str) -> Option<String> {
	needles().into_iter().find(|n| text.contains(n.as_str()))
}

#[test]
fn no_secret_reaches_any_output_log_or_error() {
	let fx = Fixture::new();
	let mut io = fx.io();
	let mut seen = String::new();
	let mut world = World::genesis();
	let mut step = |io: &mut TestIo, secrets: &[&str], seed: Option<[u8; 32]>, argv: &[&str]| {
		io.say(secrets);
		if let Some(s) = seed {
			io.seed(s);
		}
		let (_, out, err) = io.run(argv);
		io.secrets.clear();
		io.seeds.clear();
		seen.push_str(&out);
		seen.push_str(&err);
	};
	let d = fx.dir();
	let m = fx.manifest();
	let b = fx.backups.display().to_string();
	let g1 = fx.file("g1", &world.export(1));
	let g2 = fx.file("g2", &world.export(2));
	step(
		&mut io,
		&[PRIMARY, PRIMARY],
		Some(ADMIN_SEED),
		&[
			"generate",
			"--dir",
			&d,
			"--role",
			"admin",
			"--intended-epoch",
			"0",
			"--out",
			"a.p",
		],
	);
	step(
		&mut io,
		&[SECONDARY],
		None,
		&["show-public", "--dir", &d, "--key", "a.p"],
	);
	step(
		&mut io,
		&[PRIMARY],
		None,
		&[
			"bind",
			"--dir",
			&d,
			"--key",
			"a.p",
			"--out",
			"a",
			"--context",
			&g1,
			"--trusted-manifest",
			&m,
		],
	);
	step(
		&mut io,
		&[PRIMARY],
		None,
		&[
			"bind",
			"--dir",
			&d,
			"--key",
			"a.p",
			"--out",
			"a",
			"--context",
			&g1,
			"--context",
			&g2,
			"--trusted-manifest",
			&m,
		],
	);
	step(
		&mut io,
		&[PRIMARY],
		None,
		&["show-public", "--dir", &d, "--key", "a"],
	);
	step(
		&mut io,
		&[PRIMARY, PRIMARY, PRIMARY],
		None,
		&[
			"backup", "--dir", &d, "--key", "a", "--to", &b, "--out", "a.bak",
		],
	);
	step(
		&mut io,
		&[PRIMARY, SECONDARY, SECONDARY],
		None,
		&[
			"backup", "--dir", &d, "--key", "a", "--to", &b, "--out", "a.bak",
		],
	);
	step(
		&mut io,
		&[SECONDARY, NEWER, NEWER],
		None,
		&[
			"restore", "--dir", &d, "--from", &b, "--backup", "a.bak", "--out", "a.r",
		],
	);
	step(
		&mut io,
		&[],
		None,
		&["show-public", "--dir", &d, "--unauthenticated"],
	);
	step(
		&mut io,
		&[PRIMARY, PRIMARY],
		Some([0x51; 32]),
		&[
			"generate",
			"--dir",
			&d,
			"--role",
			"admin",
			"--intended-epoch",
			"1",
			"--out",
			"a1.p",
		],
	);
	world.rotate(1, [0x51; 32]);
	let r = fx.file("r", &world.export(1));
	let r2 = fx.file("r2", &world.export(2));
	step(
		&mut io,
		&[PRIMARY],
		None,
		&[
			"bind",
			"--dir",
			&d,
			"--key",
			"a1.p",
			"--out",
			"a1",
			"--context",
			&r,
			"--trusted-manifest",
			&m,
		],
	);
	step(
		&mut io,
		&["1"],
		None,
		&[
			"retire-epoch",
			"--dir",
			&d,
			"--key",
			"a",
			"--context",
			&r,
			"--trusted-manifest",
			&m,
		],
	);
	step(
		&mut io,
		&[PRIMARY, "wrong"],
		None,
		&[
			"destroy",
			"--dir",
			&d,
			"--key",
			"a.r",
			"--context",
			&r,
			"--context",
			&r2,
			"--trusted-manifest",
			&m,
		],
	);
	let log = String::from_utf8(fx.log()).unwrap();
	assert!(log.lines().count() >= 12, "{log}");
	assert!(
		log.contains("wrong passphrase"),
		"wrong-passphrase failures are logged"
	);
	assert_eq!(leaks(&seen), None, "output");
	assert_eq!(leaks(&log), None, "log");
	for e in [
		ToolError::Decrypt { name: "k".into() },
		ToolError::KeyMismatch { name: "k".into() },
		ToolError::PassphraseTooShort { chars: 3 },
		ToolError::PassphraseMismatch,
		ToolError::BackupPassphraseReused,
		ToolError::Confirmation,
		ToolError::Kdf,
		ToolError::Seal,
	] {
		assert_eq!(leaks(&format!("{e} {e:?}")), None);
	}
	// Control: the probe finds each secret in plainly formatted text.
	assert!(leaks(&format!("{PRIMARY:?}")).is_some());
	assert!(leaks(&hex(&ADMIN_SEED)).is_some());
	assert!(leaks(&format!("{:?}", ADMIN_SEED)).is_some());
}

#[test]
fn seeds_and_passphrases_live_only_in_secret_buffers() {
	for (name, text) in sources() {
		if name.starts_with("tests/") {
			continue;
		}
		for line in text.lines().filter(|l| !l.trim_start().starts_with("//")) {
			// No owned copy of a seed or passphrase: no `[u8; 32]` seed array, no String of one.
			assert!(!line.contains("let seed: [u8"), "{name}: {line}");
			assert!(
				!line.contains("String::from_utf8(passphrase"),
				"{name}: {line}"
			);
		}
	}
	let io = &sources().into_iter().find(|(n, _)| n == "io.rs").unwrap().1;
	assert!(io.contains("fn new_seed(&mut self) -> Result<SecretBuf, ToolError>"));
	assert!(io.contains(
		"fn secret(&mut self, prompt: &str, max: usize) -> Result<SecretBuf, ToolError>"
	));
}
