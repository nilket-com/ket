//! T-PASS (policy): UTF-8, NFC, at least 16 characters, two equal entries for a new passphrase,
//! and no passphrase source outside the terminal seam.
use super::support::*;
use crate::error::ToolError;
use crate::passphrase::{MIN_CHARS, normalize};
use key_guard::custody::SecretBuf;

fn buf(bytes: &[u8]) -> SecretBuf {
	let mut b = SecretBuf::with_capacity(bytes.len());
	for x in bytes {
		b.push(*x).unwrap();
	}
	b
}

#[test]
fn a_passphrase_is_utf8_nfc_and_at_least_sixteen_characters() {
	assert!(matches!(
		normalize(&buf(&[b'a'; 15])),
		Err(ToolError::PassphraseTooShort { chars: 15 })
	));
	normalize(&buf(&[b'a'; MIN_CHARS])).unwrap();
	// Characters, not bytes: sixteen two-byte characters pass, fifteen do not.
	normalize(&buf("é".repeat(16).as_bytes())).unwrap();
	assert!(normalize(&buf("é".repeat(15).as_bytes())).is_err());
	assert!(matches!(
		normalize(&buf(&[0xff; 20])),
		Err(ToolError::PassphraseNotUtf8)
	));
	// NFC: a decomposed and a precomposed spelling normalise to the same bytes.
	let decomposed = format!("{}e\u{301}", "x".repeat(15));
	let precomposed = format!("{}\u{e9}", "x".repeat(15));
	assert_eq!(
		normalize(&buf(decomposed.as_bytes())).unwrap().as_slice(),
		normalize(&buf(precomposed.as_bytes())).unwrap().as_slice()
	);
}

#[test]
fn a_new_passphrase_needs_two_equal_entries_and_none_comes_from_argv_or_the_environment() {
	let fx = Fixture::new();
	let mut io = fx.io();
	let generate = [
		"generate",
		"--dir",
		&fx.dir(),
		"--role",
		"admin",
		"--intended-epoch",
		"0",
		"--out",
		"k",
	];
	io.say(&[PRIMARY, SECONDARY]).seed(ADMIN_SEED);
	io.refused(&generate, "differ");
	io.say(&["fifteen chars!!", "fifteen chars!!"])
		.seed(ADMIN_SEED);
	io.refused(&generate, "at least 16");
	// The same text in two normal forms is one passphrase.
	let decomposed = format!("{}e\u{301}", "x".repeat(15));
	let precomposed = format!("{}\u{e9}", "x".repeat(15));
	io.say(&[&decomposed, &precomposed]).seed(ADMIN_SEED);
	io.ok(&generate);
	// There is no passphrase flag, and the environment is never read for one.
	let mut with_flag = generate.to_vec();
	with_flag.extend(["--passphrase", PRIMARY]);
	io.refused(&with_flag, "unknown argument \"--passphrase\"");
	let sources = crate::tests::secrets::sources();
	for (name, text) in sources.iter().filter(|(n, _)| !n.starts_with("tests/")) {
		for line in text.lines().filter(|l| !l.trim_start().starts_with("//")) {
			if name != "host.rs" {
				assert!(
					!line.contains("env::var"),
					"{name} reads the environment: {line}"
				);
			}
			assert!(!line.contains("stdin"), "{name} reads stdin: {line}");
		}
	}
}

#[test]
fn the_only_environment_reads_are_the_host_probes_home_directories() {
	let host = &crate::tests::secrets::sources()
		.into_iter()
		.find(|(n, _)| n == "host.rs")
		.unwrap()
		.1;
	let reads: Vec<&str> = host.matches("env_dir(\"").collect();
	assert_eq!(reads.len(), 2);
	assert!(host.contains("env_dir(\"HOME\")") && host.contains("env_dir(\"XDG_DATA_HOME\")"));
	assert_eq!(
		host.matches("env::var").count(),
		1,
		"one helper reads the environment"
	);
}
