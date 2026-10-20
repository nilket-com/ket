//! Source pins for the custody modules, T-K3z, the secret-leak checks and the interface rule.
use crate::custody::memory::probe::{self, Stage};
use crate::custody::process::counts;
use crate::custody::{CustodyError, Refusal, SecretBuf, sys};
use std::path::{Path, PathBuf};

fn custody_dir() -> PathBuf {
	Path::new(env!("CARGO_MANIFEST_DIR")).join("src/custody")
}

/// Every `.rs` file under `src/custody`, by its path relative to it, read from disk now.
fn all_sources() -> Vec<(String, String)> {
	fn visit(root: &Path, dir: &Path, out: &mut Vec<(String, String)>) {
		for entry in std::fs::read_dir(dir).unwrap() {
			let path = entry.unwrap().path();
			if path.is_dir() {
				visit(root, &path, out);
			} else if path.extension().is_some_and(|e| e == "rs") {
				let name = path
					.strip_prefix(root)
					.unwrap()
					.to_string_lossy()
					.into_owned();
				out.push((name, std::fs::read_to_string(&path).unwrap()));
			}
		}
	}
	let root = custody_dir();
	let mut out = Vec::new();
	visit(&root, &root, &mut out);
	out.sort();
	out
}

fn source(name: &str) -> String {
	all_sources()
		.into_iter()
		.find(|(n, _)| n == name)
		.unwrap_or_else(|| panic!("{name} is not under src/custody"))
		.1
}

/// The tokens, spelled so this file does not contain them.
const U: &str = concat!("un", "safe");
const STOP: &str = concat!("Stop", "(");

/// Code with `//` comments removed, so prose about UNSAFE code is not a token.
fn code(line: &str) -> &str {
	line.split("//").next().unwrap_or("")
}

fn has_unsafe_token(line: &str) -> bool {
	code(line)
		.split(|c: char| !(c.is_alphanumeric() || c == '_'))
		.any(|word| word == U)
}

/// The pin: no file but `sys.rs` has an `unsafe` token, and every `unsafe` line in `sys.rs`
/// directly follows a comment block holding `SAFETY:`.
fn unsafe_pin(files: &[(String, String)]) -> Result<(), String> {
	for (name, text) in files {
		let lines: Vec<&str> = text.lines().collect();
		for (index, line) in lines.iter().enumerate() {
			if !has_unsafe_token(line) {
				continue;
			}
			if name != "sys.rs" {
				return Err(format!("{name}:{}: {U} outside sys.rs", index + 1));
			}
			let documented = lines[..index]
				.iter()
				.rev()
				.take_while(|l| l.trim_start().starts_with("//"))
				.any(|l| l.contains("SAFETY:"));
			if !documented {
				return Err(format!("{name}:{}: {U} without SAFETY", index + 1));
			}
		}
	}
	Ok(())
}

#[test]
fn the_pins_see_every_custody_source_file() {
	let names: Vec<String> = all_sources().into_iter().map(|(n, _)| n).collect();
	for expected in [
		"dir.rs",
		"dir/tests.rs",
		"mod.rs",
		"sys.rs",
		"tests.rs",
		"tty.rs",
	] {
		assert!(
			names.iter().any(|n| n == expected),
			"{expected} in {names:?}"
		);
	}
}

#[test]
fn every_unsafe_block_lives_in_sys_and_is_documented() {
	unsafe_pin(&all_sources()).unwrap();
}

/// `extra` appended to `name`, or added as a new file `name` when there is none.
fn with(name: &str, extra: &str) -> Vec<(String, String)> {
	let extra = extra.replace("UNSAFE", U).replace("STOP(", STOP);
	let mut files = all_sources();
	match files.iter_mut().find(|(n, _)| n == name) {
		Some(file) => file.1.push_str(&extra),
		None => files.push((name.to_string(), extra)),
	}
	files
}

#[test]
fn the_unsafe_pin_catches_a_stray_or_undocumented_block() {
	let stray = "\nfn f() { UNSAFE { libc::sync() } }\n";
	assert!(unsafe_pin(&with("dir.rs", stray)).is_err());
	assert!(unsafe_pin(&with("new_module.rs", stray)).is_err());
	assert!(unsafe_pin(&with("dir/new_tests.rs", stray)).is_err());
	let wrong_file = "\n// SAFETY: still the wrong file.\nUNSAFE fn g() {}\n";
	assert!(unsafe_pin(&with("tty.rs", wrong_file)).is_err());
	let undocumented = "\nfn f() {\n\tUNSAFE { libc::sync() }\n}\n";
	assert!(unsafe_pin(&with("sys.rs", undocumented)).is_err());
	let documented = "\nfn f() {\n\t// SAFETY: no arguments.\n\tUNSAFE { libc::sync() }\n}\n";
	assert!(unsafe_pin(&with("sys.rs", documented)).is_ok());
	assert!(
		unsafe_pin(&with(
			"dir.rs",
			"\n// prose about UNSAFE code is not a token\n"
		))
		.is_ok()
	);
}

/// The pin: in every custody file, the stop type is constructed only as `Stop::ROOT`'s `/` and,
/// with an anchor, only inside the private `#[cfg(test)] fn at`; no non-test file has a `pub`
/// item naming it.
fn anchor_pin(files: &[(String, String)]) -> Result<(), String> {
	let root_line = format!("const ROOT: Stop = {STOP}None);");
	let at_line = "fn at(anchor: (u64, u64)) -> Stop {";
	for (name, text) in files {
		let lines: Vec<&str> = text.lines().collect();
		for (index, line) in lines.iter().enumerate() {
			let line = code(line).trim();
			let test_file = name.ends_with("tests.rs") || name == "test_dir.rs";
			if !test_file && line.contains("pub") && line.contains("Stop") {
				return Err(format!("{name}:{}: a public item names Stop", index + 1));
			}
			if !line.contains(STOP) || line.starts_with("struct Stop") {
				continue;
			}
			if name == "dir.rs" && line == root_line {
				continue;
			}
			let in_at = name == "dir.rs"
				&& line == format!("{STOP}Some(anchor))")
				&& index >= 2
				&& code(lines[index - 1]).trim() == at_line
				&& lines[index - 2].trim() == "#[cfg(test)]";
			if !in_at {
				return Err(format!("{name}:{}: Stop is built outside fn at", index + 1));
			}
		}
	}
	Ok(())
}

#[test]
fn the_walk_anchor_is_test_only_and_unreachable_from_the_public_api() {
	anchor_pin(&all_sources()).unwrap();
}

#[test]
fn the_anchor_pin_catches_a_leak() {
	for (name, leak) in [
		("dir.rs", "\nfn leak() -> Stop { STOP(Some((0, 0))) }\n"),
		("dir.rs", "\nfn leak() -> Stop { STOP(None) }\n"),
		("dir.rs", "\nfn leak() -> Stop {\n\tSTOP(Some((0, 0)))\n}\n"),
		("tty.rs", "\nfn leak() -> Stop { STOP(None) }\n"),
		(
			"dir/tests.rs",
			"\nfn leak() -> Stop { STOP(Some((0, 0))) }\n",
		),
		("dir.rs", "\npub(crate) fn leak(stop: Stop) {}\n"),
	] {
		assert!(anchor_pin(&with(name, leak)).is_err(), "{name}: {leak}");
	}
	let dir = source("dir.rs");
	let ungated = dir.replacen("\t#[cfg(test)]\n\tfn at(", "\tfn at(", 1);
	assert_ne!(ungated, dir);
	let mut files = all_sources();
	files.iter_mut().find(|(n, _)| n == "dir.rs").unwrap().1 = ungated;
	assert!(anchor_pin(&files).is_err());
}

#[test]
fn a_dropped_secret_is_zero_after_the_wipe_and_was_not_before() {
	let seen = probe::record(|| {
		let mut seed = SecretBuf::zeroed(32);
		seed.as_mut_slice().fill(0xA5);
		drop(seed);
	});
	assert_eq!(seen.len(), 2, "{seen:?}");
	let (before, after) = (&seen[0], &seen[1]);
	assert_eq!(before.0, Stage::BeforeWipe);
	assert!(
		before.1.iter().all(|&b| b == 0xA5),
		"the probe saw the secret"
	);
	assert_eq!(after.0, Stage::AfterWipe);
	assert_eq!(after.1.len(), 32);
	assert!(after.1.iter().all(|&b| b == 0));
}

#[test]
fn the_buffer_never_grows_and_edits_zero_what_they_remove() {
	let mut buf = SecretBuf::with_capacity(4);
	for b in *b"ab\xc3\xa9" {
		buf.push(b).unwrap();
	}
	assert!(buf.push(b'x').is_err());
	assert_eq!(buf.capacity(), 4);
	buf.pop_char();
	assert_eq!(buf.as_slice(), b"ab");
	let seen = probe::record(|| drop(buf));
	assert_eq!(
		seen[0].1, b"ab\0\0",
		"popped bytes were zeroed before the drop"
	);
}

const SECRET: &str = "hunter2-correct-horse";

fn leaks(text: &str) -> bool {
	text.contains(SECRET)
		|| text.contains(&format!("{:?}", SECRET.as_bytes()).replace(['[', ']'], ""))
}

#[test]
fn debug_and_errors_never_show_secret_bytes() {
	let mut buf = SecretBuf::with_capacity(64);
	for b in SECRET.bytes() {
		buf.push(b).unwrap();
	}
	let debug = format!("{buf:?}");
	assert!(!leaks(&debug), "{debug}");
	assert!(debug.contains("len: 21"), "{debug}");
	for error in [
		CustodyError::TooLong { max: 8 },
		CustodyError::Interrupted,
		CustodyError::EndOfInput,
		CustodyError::NotATerminal,
		CustodyError::Refused {
			what: "keys/key".into(),
			reason: Refusal::Size { len: 3 },
		},
	] {
		assert!(!leaks(&error.to_string()) && !leaks(&format!("{error:?}")));
	}
	// Control: the probe finds the secret in plainly formatted text and bytes.
	assert!(leaks(&format!(
		"{:?}",
		String::from_utf8_lossy(buf.as_slice())
	)));
	assert!(leaks(&format!("{:?}", buf.as_slice())));
}

#[test]
fn mlock_is_reported_not_fatal() {
	let buf = SecretBuf::with_capacity(64);
	let direct = crate::custody::lock_memory(&[0_u8; 64]).is_ok();
	// Both succeed or both fail under the same RLIMIT_MEMLOCK; neither panics.
	assert_eq!(buf.is_locked(), direct);
}

#[test]
fn only_up_non_loopback_interfaces_count() {
	let up = libc::IFF_UP as u32;
	let loopback = libc::IFF_LOOPBACK as u32;
	assert!(counts(up));
	assert!(counts(up | libc::IFF_RUNNING as u32));
	assert!(!counts(up | loopback));
	assert!(!counts(loopback));
	assert!(!counts(0));
	let entries = sys::interfaces().unwrap();
	let listed = crate::custody::non_loopback_interfaces_up().unwrap();
	for (name, flags) in &entries {
		if counts(*flags) {
			assert!(listed.contains(name), "{name}");
		}
	}
	for name in &listed {
		assert!(
			entries.iter().any(|(n, f)| n == name && counts(*f)),
			"{name}"
		);
	}
	assert!(!listed.iter().any(|n| n == "lo"));
}
