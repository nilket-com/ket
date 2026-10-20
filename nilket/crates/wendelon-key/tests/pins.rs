//! Source and build pins of the custody tool, each probed:
//! - T-NET: no network token in the tool's sources;
//! - `#![forbid(unsafe_code)]` heads the library and the binary;
//! - T-ANCHOR-USE: key-guard's fixture open is named only in the `cfg(test)` tests module;
//! - T-K1a: nothing but `wendelon-key` depends on `wendelon-key`; the FIPS build lines name only
//!   `--bin wendelon`; wendelon's CLI has no `key` branch;
//! - T-FIPS (D-b): the FIPS daemon graph has no `wendelon-key` and neither `non-fips` feature.
use serde_json::{Value, json};
use std::path::{Path, PathBuf};
use std::process::Command;

fn crate_dir() -> PathBuf {
	PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}
fn workspace() -> PathBuf {
	crate_dir().join("../..")
}

/// Every `.rs` file under `src`, relative to it.
fn sources() -> Vec<(String, String)> {
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
	let root = crate_dir().join("src");
	let mut out = Vec::new();
	visit(&root, &root, &mut out);
	out.sort();
	out
}

fn code(line: &str) -> &str {
	line.split("//").next().unwrap_or("")
}
fn words(line: &str) -> impl Iterator<Item = &str> {
	code(line).split(|c: char| !(c.is_alphanumeric() || c == '_'))
}

const NETWORK: [&str; 6] = ["net", "socket", "reqwest", "quinn", "tokio", "opendal"];

fn network_tokens(files: &[(String, String)]) -> Vec<String> {
	let mut found = Vec::new();
	for (name, text) in files {
		for (i, line) in text.lines().enumerate() {
			if let Some(w) = words(line).find(|w| NETWORK.contains(w)) {
				found.push(format!("{name}:{}: {w}", i + 1));
			}
		}
	}
	found
}

#[test]
fn the_tool_has_no_network_token() {
	let files = sources();
	assert!(files.iter().any(|(n, _)| n == "lib.rs") && files.len() > 10);
	assert_eq!(network_tokens(&files), Vec::<String>::new());
	for probe in [
		"use std::net::TcpStream;",
		"let s = socket::open();",
		"tokio::spawn(f);",
	] {
		let mut probed = files.clone();
		probed[0].1.push_str(&format!("\n{probe}\n"));
		assert!(!network_tokens(&probed).is_empty(), "{probe}");
	}
	// Control: prose about the network in a comment is not a token.
	let mut prose = files.clone();
	prose[0].1.push_str("\n// no net access, no socket\n");
	assert!(network_tokens(&prose).is_empty());
}

const FORBID: &str = "#![forbid(unsafe_code)]";

fn forbids(files: &[(String, String)]) -> bool {
	["lib.rs", "main.rs"].iter().all(|n| {
		files
			.iter()
			.any(|(f, t)| f == n && t.lines().any(|l| l.trim() == FORBID))
	})
}

#[test]
fn the_library_and_the_binary_forbid_unsafe_code() {
	let files = sources();
	assert!(forbids(&files));
	for target in ["lib.rs", "main.rs"] {
		let mut probed = files.clone();
		let f = probed.iter_mut().find(|(n, _)| n == target).unwrap();
		f.1 = f.1.replace(FORBID, "");
		assert!(!forbids(&probed), "{target}");
	}
}

const ANCHORED: &str = "open_custody_dir_anchored";

/// The fixture open is named only under `tests/`, and `lib.rs` declares that module test-only.
fn anchor_confined(files: &[(String, String)]) -> Result<(), String> {
	for (name, text) in files {
		if !name.starts_with("tests/") && text.lines().any(|l| code(l).contains(ANCHORED)) {
			return Err(format!("{name} names the fixture open"));
		}
	}
	let lib = &files.iter().find(|(n, _)| n == "lib.rs").unwrap().1;
	if !lib.contains("#[cfg(test)]\nmod tests;") {
		return Err("lib.rs: the tests module is not cfg(test)".into());
	}
	Ok(())
}

#[test]
fn the_fixture_open_is_used_only_by_the_test_module() {
	let files = sources();
	assert!(
		files
			.iter()
			.any(|(n, t)| n.starts_with("tests/") && t.contains(ANCHORED))
	);
	anchor_confined(&files).unwrap();
	let mut probed = files.clone();
	let io = probed.iter_mut().find(|(n, _)| n == "io.rs").unwrap();
	io.1.push_str(&format!(
		"\nfn f() {{ let _ = key_guard::custody::{ANCHORED}; }}\n"
	));
	assert!(anchor_confined(&probed).is_err());
	let mut ungated = files.clone();
	let lib = ungated.iter_mut().find(|(n, _)| n == "lib.rs").unwrap();
	lib.1 = lib.1.replace("#[cfg(test)]\nmod tests;", "mod tests;");
	assert!(anchor_confined(&ungated).is_err());
}

fn cargo(args: &[&str]) -> String {
	let output = Command::new(env!("CARGO"))
		.args(args)
		.arg("--manifest-path")
		.arg(workspace().join("Cargo.toml"))
		.arg("--offline")
		.output()
		.expect("cargo runs");
	assert!(
		output.status.success(),
		"{}",
		String::from_utf8_lossy(&output.stderr)
	);
	String::from_utf8(output.stdout).unwrap()
}

/// Packages other than `wendelon-key` that depend on it, on any edge.
fn dependents(metadata: &Value) -> Vec<String> {
	metadata["packages"]
		.as_array()
		.into_iter()
		.flatten()
		.filter(|p| p["name"] != "wendelon-key")
		.filter(|p| {
			p["dependencies"]
				.as_array()
				.into_iter()
				.flatten()
				.any(|d| d["name"] == "wendelon-key")
		})
		.filter_map(|p| p["name"].as_str().map(String::from))
		.collect()
}

#[test]
fn nothing_depends_on_the_tool() {
	let metadata: Value =
		serde_json::from_str(&cargo(&["metadata", "--format-version", "1", "--no-deps"])).unwrap();
	let names: Vec<&str> = metadata["packages"]
		.as_array()
		.unwrap()
		.iter()
		.filter_map(|p| p["name"].as_str())
		.collect();
	assert!(
		names.contains(&"wendelon-key") && names.contains(&"nilket") && names.contains(&"wendelon")
	);
	assert_eq!(dependents(&metadata), Vec::<String>::new());
	let synthetic = json!({ "packages": [
		{ "name": "wendelon-key", "dependencies": [{ "name": "wendelon" }] },
		{ "name": "nilket", "dependencies": [{ "name": "wendelon-key", "kind": null }] },
	]});
	assert_eq!(dependents(&synthetic), ["nilket"]);
}

/// The `cargo build ... --features fips` lines of a script, and whether each names exactly
/// `--bin wendelon`.
fn fips_lines_name_only_the_daemon(text: &str) -> Result<usize, String> {
	let mut n = 0;
	for line in text
		.lines()
		.filter(|l| l.contains("cargo build") && l.contains("--features fips"))
	{
		let bins: Vec<&str> = line
			.split_whitespace()
			.collect::<Vec<_>>()
			.windows(2)
			.filter(|w| w[0] == "--bin")
			.map(|w| w[1])
			.collect();
		if bins != ["wendelon"] || line.contains("--bins") || line.contains("--workspace") {
			return Err(line.trim().to_string());
		}
		n += 1;
	}
	Ok(n)
}

#[test]
fn the_fips_build_names_only_the_daemon_and_the_cli_has_no_key_branch() {
	let w = workspace().join("crates/wendelon");
	for script in [
		"ci/check_fips_symbol_sweep.sh",
		"ci/generate_regulated_release_evidence.sh",
	] {
		let text = std::fs::read_to_string(w.join(script)).unwrap();
		assert_eq!(fips_lines_name_only_the_daemon(&text), Ok(1), "{script}");
	}
	let probe = "cargo build --locked --profile fips --no-default-features --features fips --bin wendelon-key";
	assert!(fips_lines_name_only_the_daemon(probe).is_err());
	let probe = "cargo build --profile fips --no-default-features --features fips --workspace --bin wendelon";
	assert!(fips_lines_name_only_the_daemon(probe).is_err());
	let cli = std::fs::read_to_string(w.join("src/cli.rs")).unwrap();
	let key_branch = |t: &str| {
		t.lines()
			.any(|l| code(l).contains("== \"key\"") || code(l).contains("\"key\" =>"))
	};
	assert!(cli.contains("== \"audit\"") && !key_branch(&cli));
	assert!(key_branch(&format!(
		"{cli}\n\t\t[cmd, rest @ ..] if cmd == \"key\" => key::run(rest),\n"
	)));
}

/// D-b over three `cargo tree` texts of one graph: the dependency tree (no `wendelon-key` node)
/// and the features enabled on aws-lc-rs and on wendelon (neither `non-fips`).
fn fips_graph_violations(tree: &str, aws_lc: &str, wendelon: &str) -> Vec<String> {
	let lines = |t: &str| {
		t.lines()
			.map(str::trim)
			.map(String::from)
			.collect::<Vec<_>>()
	};
	let mut found: Vec<String> = lines(tree)
		.into_iter()
		.filter(|l| l.starts_with("wendelon-key "))
		.collect();
	found.extend(
		lines(aws_lc)
			.into_iter()
			.filter(|l| l.starts_with("aws-lc-rs feature \"non-fips\"")),
	);
	found.extend(
		lines(wendelon)
			.into_iter()
			.filter(|l| l.starts_with("wendelon feature \"non-fips\"")),
	);
	found
}

/// The three texts for `-p wendelon` with `features` (none for the default graph).
fn graph(features: &[&str]) -> (String, String, String) {
	let run = |extra: &[&str]| {
		let mut args = vec!["tree", "-p", "wendelon", "--prefix", "none"];
		args.extend(features);
		args.extend(extra);
		cargo(&args)
	};
	(
		run(&["-e", "normal,build"]),
		run(&["-e", "features", "-i", "aws-lc-rs"]),
		run(&["-e", "features", "-i", "wendelon"]),
	)
}

#[test]
fn the_fips_daemon_graph_has_neither_the_tool_nor_a_non_fips_feature() {
	let (tree, aws_lc, wendelon) = graph(&["--no-default-features", "--features", "fips"]);
	assert!(
		tree.lines().count() > 100 && tree.contains("key-guard v"),
		"the graph was read"
	);
	assert!(aws_lc.contains("aws-lc-rs feature \"fips\""), "{aws_lc}");
	assert!(wendelon.contains("wendelon feature \"fips\""), "{wendelon}");
	assert_eq!(
		fips_graph_violations(&tree, &aws_lc, &wendelon),
		Vec::<String>::new()
	);
	// Control: the default (non-FIPS) graph does enable wendelon's `non-fips` feature, so the
	// matcher sees real output; and each banned line is caught.
	let (d_tree, d_aws, d_wendelon) = graph(&[]);
	assert!(!fips_graph_violations(&d_tree, &d_aws, &d_wendelon).is_empty());
	let add = |t: &str, l: &str| format!("{t}\n{l}\n");
	assert_eq!(
		fips_graph_violations(&add(&tree, "wendelon-key v0.1.0 (/x)"), &aws_lc, &wendelon).len(),
		1
	);
	assert_eq!(
		fips_graph_violations(
			&tree,
			&add(&aws_lc, "aws-lc-rs feature \"non-fips\""),
			&wendelon
		)
		.len(),
		1
	);
	assert_eq!(
		fips_graph_violations(
			&tree,
			&aws_lc,
			&add(&wendelon, "wendelon feature \"non-fips\"")
		)
		.len(),
		1
	);
}
