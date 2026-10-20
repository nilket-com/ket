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

/// The one attribute that may gate the anchor: test builds and the `fixture-anchor` feature.
const GATE: &str = "#[cfg(any(test, feature = \"fixture-anchor\"))]";
const ANCHORED: &str = "open_custody_dir_anchored";

fn is_test_file(name: &str) -> bool {
	name.ends_with("tests.rs") || name == "test_dir.rs"
}

/// The pin: in every custody file, the stop type is constructed only as `Stop::ROOT`'s `/` and,
/// with an anchor, only inside the private `fn at`, which carries exactly `GATE`; no non-test file
/// has a `pub` item naming it.
fn anchor_pin(files: &[(String, String)]) -> Result<(), String> {
	let root_line = format!("const ROOT: Stop = {STOP}None);");
	let at_line = "fn at(anchor: (u64, u64)) -> Stop {";
	for (name, text) in files {
		let lines: Vec<&str> = text.lines().collect();
		for (index, line) in lines.iter().enumerate() {
			let line = code(line).trim();
			if !is_test_file(name) && line.contains("pub") && line.contains("Stop") {
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
				&& lines[index - 2].trim() == GATE;
			if !in_at {
				return Err(format!("{name}:{}: Stop is built outside fn at", index + 1));
			}
		}
	}
	Ok(())
}

/// The pin on the anchored open, over every non-test custody file:
/// - the feature appears only as exactly `GATE`, on three lines: `fn at`, the anchored open and
///   its one re-export;
/// - the anchored open is defined once, in `dir.rs`, directly under `GATE`, and re-exported once,
///   in `mod.rs`, directly under `GATE`; it is named nowhere else;
/// - `Stop::at(` is called only inside the anchored open's body.
fn anchored_pin(files: &[(String, String)]) -> Result<(), String> {
	let define = format!("pub fn {ANCHORED}(");
	let export = format!("pub use crate::custody::dir::{ANCHORED};");
	let at_call = concat!("Stop::", "at(");
	let (mut gates, mut defines, mut exports) = (0, 0, 0);
	for (name, text) in files.iter().filter(|(n, _)| !is_test_file(n)) {
		let lines: Vec<&str> = text.lines().collect();
		let mut body = None;
		for (index, raw) in lines.iter().enumerate() {
			let line = code(raw).trim();
			let at = || format!("{name}:{}", index + 1);
			let gated = index >= 1 && lines[index - 1].trim() == GATE;
			if line.contains("fixture-anchor") {
				if line != GATE {
					return Err(format!("{}: the feature gates something else", at()));
				}
				gates += 1;
			}
			if line.starts_with(&define) && name == "dir.rs" && gated {
				defines += 1;
				body = Some(index);
			} else if line == export && name == "mod.rs" && gated {
				exports += 1;
			} else if line.contains(ANCHORED) {
				return Err(format!(
					"{}: the anchored open is named outside its gate",
					at()
				));
			}
			if body.is_some() && *raw == "}" {
				body = None;
			}
			if line.contains(at_call) && (body.is_none() || name != "dir.rs") {
				return Err(format!(
					"{}: the anchor is built outside the anchored open",
					at()
				));
			}
		}
	}
	if (gates, defines, exports) != (3, 1, 1) {
		return Err(format!(
			"gates {gates}, definitions {defines}, re-exports {exports}; expected 3, 1, 1"
		));
	}
	Ok(())
}

#[test]
fn the_walk_anchor_is_test_or_feature_only_and_unreachable_from_the_shipped_api() {
	anchor_pin(&all_sources()).unwrap();
	anchored_pin(&all_sources()).unwrap();
}

/// `name` with its first `from` replaced by `to`; the replacement must happen.
fn replaced(name: &str, from: &str, to: &str) -> Vec<(String, String)> {
	let mut files = all_sources();
	let file = &mut files.iter_mut().find(|(n, _)| n == name).unwrap().1;
	let edited = file.replacen(from, to, 1);
	assert_ne!(&edited, file, "{name}: {from:?} not found");
	*file = edited;
	files
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
	let gated_at = format!("\t{GATE}\n\tfn at(");
	assert!(anchor_pin(&replaced("dir.rs", &gated_at, "\tfn at(")).is_err());
	// The gate is exact: neither the test gate alone nor the feature alone.
	let test_only = "\t#[cfg(test)]\n\tfn at(";
	assert!(anchor_pin(&replaced("dir.rs", &gated_at, test_only)).is_err());
	let shipped = "\t#[cfg(feature = \"fixture-anchor\")]\n\tfn at(";
	assert!(anchor_pin(&replaced("dir.rs", &gated_at, shipped)).is_err());
}

#[test]
fn the_anchored_pin_catches_an_ungated_open_a_stray_anchor_and_a_reused_feature() {
	let define = format!("{GATE}\npub fn {ANCHORED}(");
	let export = format!("{GATE}\npub use crate::custody::dir::{ANCHORED};");
	for (what, files) in [
		(
			"ungated open",
			replaced("dir.rs", &define, &format!("pub fn {ANCHORED}(")),
		),
		(
			"ungated re-export",
			replaced(
				"mod.rs",
				&export,
				&format!("pub use crate::custody::dir::{ANCHORED};"),
			),
		),
		(
			"open gated by the feature alone",
			replaced(
				"dir.rs",
				&define,
				&format!("#[cfg(feature = \"fixture-anchor\")]\npub fn {ANCHORED}("),
			),
		),
		(
			"anchor built outside the open",
			with("dir.rs", "\nfn stray() -> Stop {\n\tStop::at((0, 0))\n}\n"),
		),
		(
			"anchor built in another file",
			with("tty.rs", "\nfn stray() {\n\tlet _ = Stop::at((0, 0));\n}\n"),
		),
		(
			"a second gated item",
			with(
				"tty.rs",
				&format!("\n{GATE}\npub fn also_for_fixtures() {{}}\n"),
			),
		),
		(
			"the open named elsewhere",
			with(
				"tty.rs",
				&format!("\nfn f() {{ let _ = super::{ANCHORED}; }}\n"),
			),
		),
		(
			"a glob re-export",
			replaced(
				"mod.rs",
				&export,
				&format!("{GATE}\npub use crate::custody::dir::*;"),
			),
		),
	] {
		assert!(anchored_pin(&files).is_err(), "{what}");
	}
	// Control: the test files may name and build the anchor freely.
	let test_use = format!("\nfn f() {{ let _ = super::{ANCHORED}; let _ = Stop::at((0, 0)); }}\n");
	assert!(anchored_pin(&with("dir/tests.rs", &test_use)).is_ok());
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
