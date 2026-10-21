//! Repo-wide pins for `nilket/plans/ops/test_parallelism.md`.
//!
//! Tests run concurrently by default (cargo-cgroup exports 1024 test threads;
//! the cpuset bounds CPU). Serial execution is narrow, reasoned and listed in
//! `nilket/.config/serial-allowlist.txt`. One phrase for an execution model is
//! banned outright because it hides what is meant. Every pin below has a probe
//! or negative control, so a matcher that silently matches nothing fails.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;

const ALLOWLIST: &str = "nilket/.config/serial-allowlist.txt";
const LADDER_TEMPLATE: &str = "nilket/scripts/ladder/run-ladder.sh.template";
const SUPPLEMENT_TEMPLATE: &str = "nilket/scripts/ladder/run-supplement.sh.template";
const CARGO_CGROUP: &str = "nilket/scripts/cargo-cgroup";
const WRAPPER_TEST: &str = "nilket/scripts/tests/test-cargo-cgroup-systemd-env.sh";
/// This file holds the matcher probes, so it cannot be scanned by them.
const THIS_FILE: &str = "nilket/crates/nilket/tests/test_parallelism_policy.rs";

/// Files that may contain the banned phrase, with the reason. Expected empty.
const TERMINOLOGY_ALLOWLIST: &[(&str, &str)] = &[];

/// Tracked binary formats: not text, so not scanned for the phrase. Any other
/// file that is not UTF-8 fails the scan instead of being skipped silently.
const BINARY_EXTENSIONS: &[&str] = &["arrow", "jpeg", "parquet", "png", "sqlite", "ttf"];

fn ket_root() -> PathBuf {
	Path::new(env!("CARGO_MANIFEST_DIR"))
		.ancestors()
		.nth(3)
		.expect("ket root above nilket/crates/nilket")
		.to_path_buf()
}

struct Tracked {
	path: String,
	executable: bool,
}

fn tracked_files(root: &Path) -> Vec<Tracked> {
	let output = Command::new("git")
		.arg("-C")
		.arg(root)
		.args(["ls-files", "-s", "-z"])
		.output()
		.expect("these pins enumerate tracked files with git; run them from a git checkout");
	assert!(
		output.status.success(),
		"git ls-files failed: {}",
		String::from_utf8_lossy(&output.stderr)
	);
	let files: Vec<Tracked> = output
		.stdout
		.split(|byte| *byte == 0)
		.filter(|entry| !entry.is_empty())
		.map(|entry| {
			let entry = String::from_utf8_lossy(entry);
			let (meta, path) = entry.split_once('\t').expect("ls-files -s entry");
			Tracked {
				path: path.to_string(),
				executable: meta.starts_with("100755"),
			}
		})
		.collect();
	// Control: an empty or truncated listing would make every scan vacuous.
	assert!(
		files.iter().any(|f| f.path == CARGO_CGROUP && f.executable) && files.len() > 1000,
		"tracked-file listing is implausible ({} files)",
		files.len()
	);
	files
}

enum Content {
	Missing,
	Text(String),
	NotUtf8,
}

fn read_content(root: &Path, file: &str) -> Content {
	match std::fs::read(root.join(file)) {
		Err(_) => Content::Missing,
		Ok(bytes) => match String::from_utf8(bytes) {
			Ok(text) => Content::Text(text),
			Err(_) => Content::NotUtf8,
		},
	}
}

fn read_text(root: &Path, file: &str) -> Option<String> {
	match read_content(root, file) {
		Content::Text(text) => Some(text),
		_ => None,
	}
}

fn extension(file: &str) -> Option<&str> {
	let name = file.rsplit('/').next()?;
	name.rsplit_once('.').map(|(_, ext)| ext)
}

// ---------------------------------------------------------------- terminology

/// The banned word pair, assembled so this file does not contain it.
fn banned_head() -> String {
	["sin", "gle"].concat()
}

/// Char offsets of the banned phrase: the head word, then any run of
/// non-alphanumeric characters (none, spaces, NBSP, dashes, underscores, `/`,
/// `.`, newlines), then "thread", in any ASCII case and with any suffix.
fn banned_phrase_offsets(text: &str) -> Vec<usize> {
	let chars: Vec<char> = text.chars().map(|c| c.to_ascii_lowercase()).collect();
	let head: Vec<char> = banned_head().chars().collect();
	let tail: Vec<char> = "thread".chars().collect();
	let mut hits = Vec::new();
	let mut i = 0;
	while i + head.len() <= chars.len() {
		if chars[i..i + head.len()] == head[..] {
			let mut at = i + head.len();
			while at < chars.len() && !chars[at].is_alphanumeric() {
				at += 1;
			}
			if at + tail.len() <= chars.len() && chars[at..at + tail.len()] == tail[..] {
				hits.push(i);
			}
			i += head.len();
		} else {
			i += 1;
		}
	}
	hits
}

#[test]
fn banned_phrase_matcher_probe() {
	let head = banned_head();
	let title = format!("{}{}", head[..1].to_ascii_uppercase(), &head[1..]);
	for positive in [
		format!("{head} threaded"),
		format!("{head}-threaded"),
		format!("{head}threaded"),
		format!("{head}_thread_duckdb"),
		format!("{} THREADED", head.to_ascii_uppercase()),
		format!("{title}-thread reference"),
		format!("a {head}\nthreaded run"),
		format!("{head}\u{a0}threaded"),
		format!("{head}/thread"),
		format!("{head}.threaded"),
		format!("{head} \u{2014} threaded"),
	] {
		assert_eq!(
			banned_phrase_offsets(&positive).len(),
			1,
			"missed: {positive:?}"
		);
	}
	for negative in [
		format!("{head}-process scheduler"),
		format!("{head} core"),
		"one thread".to_string(),
		"a `current_thread` runtime".to_string(),
		format!("{head} node; each thread"),
	] {
		assert!(
			banned_phrase_offsets(&negative).is_empty(),
			"false hit: {negative:?}"
		);
	}
}

/// Why a file that is not UTF-8 is a problem, if it is one.
fn non_text_problem(file: &str) -> Option<String> {
	match extension(file) {
		Some(ext) if BINARY_EXTENSIONS.contains(&ext) => None,
		_ => Some(format!(
			"{file}: not UTF-8 and not an allowlisted binary format; the phrase scan cannot read it"
		)),
	}
}

#[test]
fn non_text_files_fail_unless_their_format_is_allowlisted() {
	assert!(non_text_problem("crates/x/res/a.png").is_none());
	assert!(non_text_problem("crates/x/notes.md").is_some());
	assert!(non_text_problem("crates/x/blob").is_some());
}

#[test]
fn banned_phrase_appears_nowhere_in_the_repo() {
	let root = ket_root();
	let mut violations = Vec::new();
	let mut scanned = 0usize;
	for file in tracked_files(&root) {
		let file = file.path;
		if TERMINOLOGY_ALLOWLIST.iter().any(|(path, _)| *path == file) {
			continue;
		}
		let text = match read_content(&root, &file) {
			Content::Missing => continue,
			Content::NotUtf8 => {
				violations.extend(non_text_problem(&file));
				continue;
			}
			Content::Text(text) => text,
		};
		scanned += 1;
		for offset in banned_phrase_offsets(&text) {
			let line = text.chars().take(offset).filter(|c| *c == '\n').count() + 1;
			violations.push(format!("{file}:{line}"));
		}
	}
	assert!(scanned > 1000, "scanned only {scanned} text files");
	for (path, reason) in TERMINOLOGY_ALLOWLIST {
		assert!(
			!reason.trim().is_empty(),
			"{path}: allowlist entry needs a reason"
		);
	}
	assert!(
		violations.is_empty(),
		"banned execution-model phrase found {} time(s); say what is meant instead \
		 (one worker thread, a `current_thread` runtime, serial (reason: ...), one core); \
		 see nilket/plans/ops/test_parallelism.md:\n{}",
		violations.len(),
		violations.join("\n")
	);
}

// ------------------------------------------------------------ serial settings

/// (needle, separators that may sit between the needle and the count).
/// Quotes and commas cover two-element forms such as `"--test-threads", "1"`
/// and `.env("RUST_TEST_THREADS", "1")`; a brace and `value` cover Cargo's
/// `[env]` table form `RUST_TEST_THREADS = { value = "1" }`.
const SERIAL_NEEDLES: &[(&str, &str)] = &[
	("test-threads", "= \"',{"),
	("RUST_TEST_THREADS", "=:- \"',{"),
	("NEXTEST_TEST_THREADS", "=:- \"',{"),
	("test_threads", "= \"',{"),
	("max-threads", "= \"',{"),
	("CARGO_BUILD_JOBS", "=:- \"',{"),
	("CARGO_CGROUP_JOBS", "=:- \"',{"),
	("--jobs", "= \"',{"),
];

/// Shell and PowerShell variables in one file whose every assignment is the
/// literal 1, so a count passed through them resolves statically to 1.
fn one_valued_variables(text: &str) -> BTreeMap<String, bool> {
	let mut vars: BTreeMap<String, bool> = BTreeMap::new();
	for line in text.lines() {
		let mut rest = line.trim();
		for prefix in ["export ", "local ", "readonly ", "declare "] {
			rest = rest.strip_prefix(prefix).unwrap_or(rest).trim_start();
		}
		let rest = rest.strip_prefix('$').unwrap_or(rest);
		let Some((name, value)) = rest.split_once('=') else {
			continue;
		};
		let name = name.trim_end();
		let valid = name
			.chars()
			.next()
			.is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
			&& name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
		if !valid || value.starts_with('=') {
			continue;
		}
		let value = value
			.trim()
			.trim_end_matches(';')
			.trim_matches(|c| c == '"' || c == '\'');
		let is_one = value == "1";
		vars.entry(name.to_string())
			.and_modify(|all_one| *all_one &= is_one)
			.or_insert(is_one);
	}
	vars
}

fn count_is_one(rest: &str, separators: &str, vars: &BTreeMap<String, bool>) -> bool {
	let mut trimmed = rest.trim_start_matches(|c| separators.contains(c));
	if let Some(after) = trimmed.strip_prefix("value") {
		trimmed = after.trim_start_matches(|c| separators.contains(c));
	}
	let trimmed = trimmed.trim_start_matches(|c| c == '"' || c == '\'');
	if let Some(var) = trimmed.strip_prefix('$') {
		let var = var.strip_prefix('{').unwrap_or(var);
		let name: String = var
			.chars()
			.take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
			.collect();
		return !name.is_empty() && vars.get(&name) == Some(&true);
	}
	let mut chars = trimmed.chars();
	chars.next() == Some('1') && !chars.next().is_some_and(|c| c.is_ascii_digit())
}

/// Whether a line carries a one-thread or one-job setting, or a nextest
/// `threads-required` (which can make one test hold the whole pool).
fn is_serial_setting(line: &str, vars: &BTreeMap<String, bool>) -> bool {
	if line.contains("threads-required") {
		return true;
	}
	for (needle, separators) in SERIAL_NEEDLES {
		let mut from = 0;
		while let Some(rel) = line[from..].find(needle) {
			let end = from + rel + needle.len();
			if count_is_one(&line[end..], separators, vars) {
				return true;
			}
			from = end;
		}
	}
	// `-j1` / `-j 1` as a standalone flag.
	let mut from = 0;
	while let Some(rel) = line[from..].find("-j") {
		let start = from + rel;
		let standalone = start == 0
			|| line[..start].ends_with(|c: char| c.is_whitespace() || c == '\'' || c == '"');
		if standalone && count_is_one(&line[start + 2..], " ", vars) {
			return true;
		}
		from = start + 2;
	}
	false
}

/// Scripts, CI and gate config, build files, test-runner config and Rust
/// sources (which may build cargo command lines).
fn in_serial_scope(file: &str, executable: bool) -> bool {
	let name = file.rsplit('/').next().unwrap_or(file);
	file != ALLOWLIST
		&& file != THIS_FILE
		&& (file.starts_with("ci/")
			|| file.starts_with("scripts/")
			|| file.starts_with("nilket/scripts/")
			|| file.starts_with("nilket/.config/")
			|| file.contains("/ci/")
			|| file.starts_with(".cargo/config")
			|| file.contains("/.cargo/config")
			|| (file.contains(".github/workflows/")
				&& (name.ends_with(".yml") || name.ends_with(".yaml")))
			|| matches!(
				name,
				"Makefile" | "makefile" | "GNUmakefile" | "justfile" | "Justfile" | ".justfile"
			) || name.starts_with("Containerfile")
			|| name.starts_with("Dockerfile")
			|| name.ends_with(".py")
			|| name.ends_with("nextest.toml")
			|| name.ends_with(".sh")
			|| name.ends_with(".ps1")
			|| name.ends_with(".rs")
			|| (executable && !name.contains('.')))
}

#[test]
fn serial_setting_matcher_probe() {
	let none = BTreeMap::new();
	for positive in [
		"cargo test -- --test-threads=1",
		"cargo test -- --test-threads 1 --nocapture",
		"RUST_TEST_THREADS=1 run cargo test",
		"NEXTEST_TEST_THREADS=1 cargo nextest run",
		"test_threads=\"${CARGO_CGROUP_RUST_TEST_THREADS:-1}\"",
		"test_threads = 1",
		"distributed-fixtures = { max-threads = 1 }",
		"test-threads = 1",
		"threads-required = \"num-test-threads\"",
		"cargo build -j1",
		"cargo build -j 1 --release",
		"cargo build --jobs=1",
		"CARGO_BUILD_JOBS=1 cargo test",
		"'--', '--test-threads=1')",
		"cmd.args([\"--test-threads\", \"1\"]);",
		"@('--', '--test-threads', '1')",
		".env(\"RUST_TEST_THREADS\", \"1\")",
		"RUST_TEST_THREADS = { value = \"1\" }",
		"RUST_TEST_THREADS = { value = \"1\", force = true }",
	] {
		assert!(is_serial_setting(positive, &none), "missed: {positive:?}");
	}
	for negative in [
		"cargo test -- --test-threads=12",
		"cargo test -- --test-threads=\"$main_threads\"",
		"test_threads=\"${CARGO_CGROUP_RUST_TEST_THREADS:-1024}\"",
		"RUST_TEST_THREADS=16",
		"max-threads = 4",
		"cargo build -j16",
		"cargo build --jobs 10",
		"make foo-j1",
		"CARGO_CGROUP_JOBS=6",
		"cmd.args([\"--test-threads\", \"16\"]);",
		"RUST_TEST_THREADS = { value = \"12\" }",
	] {
		assert!(
			!is_serial_setting(negative, &none),
			"false hit: {negative:?}"
		);
	}
	// A count passed through a variable resolves when every assignment is 1.
	let script = "export T=1\nlocal N=1\n$P = 1\nM=4\nM=1\n";
	let vars = one_valued_variables(script);
	assert!(is_serial_setting("cargo test -- --test-threads=$T", &vars));
	assert!(is_serial_setting(
		"cargo test -- --test-threads \"${N}\"",
		&vars
	));
	assert!(is_serial_setting("'--test-threads=$P'", &vars));
	assert!(!is_serial_setting("cargo test -- --test-threads=$M", &vars));
	assert!(!is_serial_setting(
		"cargo test -- --test-threads=$UNSET",
		&vars
	));

	assert!(in_serial_scope("nilket/scripts/cargo-cgroup", true));
	assert!(in_serial_scope(LADDER_TEMPLATE, false));
	assert!(in_serial_scope("nilket/crates/wendelon/ci/check.sh", false));
	assert!(in_serial_scope("ci/check-windows.ps1", false));
	assert!(in_serial_scope("nilket/.config/nextest.toml", false));
	assert!(in_serial_scope("crates/x/.config/nextest.toml", false));
	assert!(in_serial_scope(
		"nilket/crates/wendelon/.github/workflows/rust-ci.yml",
		false
	));
	assert!(in_serial_scope("crates/x/Makefile", false));
	assert!(in_serial_scope("justfile", false));
	assert!(in_serial_scope("packaging/podman/image/build", true));
	assert!(in_serial_scope("crates/x/src/runner.rs", false));
	assert!(in_serial_scope("tools/bench.py", false));
	assert!(in_serial_scope(
		"packaging/podman/image/Containerfile",
		false
	));
	assert!(in_serial_scope(
		"packaging/podman/policy/Containerfile.probe",
		false
	));
	assert!(in_serial_scope(SUPPLEMENT_TEMPLATE, false));
	assert!(!in_serial_scope("packaging/podman/image/README", false));
	assert!(!in_serial_scope(ALLOWLIST, false));
	assert!(!in_serial_scope(THIS_FILE, false));
	assert!(!in_serial_scope(
		"nilket/crates/toron/plans/impl/0054_x.md",
		false
	));
}

/// Where an allowlisted serial setting may hold CPU.
#[derive(Clone, Debug, PartialEq)]
enum SerialScope {
	/// A dedicated lane of at most this many CPUs (1 or 2), declared on the
	/// same line as the setting, so one thread never holds a large lane.
	Lane(usize),
	/// A nextest test-group limit or `threads-required` in a nextest.toml: it
	/// limits concurrency inside the group only, while other tests keep running.
	TestGroup,
	/// A comment or `#[ignore]` reason in a Rust source that documents how to
	/// run one measurement; it executes nothing.
	Doc,
}

#[derive(Debug)]
struct AllowEntry {
	scope: SerialScope,
	count: usize,
}

/// Each allowlist line: `path<TAB>exact line, trimmed<TAB>scope<TAB>reason`,
/// where scope is `lane:1`, `lane:2`, `test-group` or `doc`.
fn parse_allowlist(text: &str) -> BTreeMap<(String, String), AllowEntry> {
	let mut entries: BTreeMap<(String, String), AllowEntry> = BTreeMap::new();
	for (number, raw) in text.lines().enumerate() {
		if raw.trim().is_empty() || raw.starts_with('#') {
			continue;
		}
		let at = number + 1;
		let fields: Vec<&str> = raw.split('\t').collect();
		assert_eq!(
			fields.len(),
			4,
			"{ALLOWLIST}:{at}: want path<TAB>line<TAB>scope<TAB>reason"
		);
		let scope = match fields[2].trim() {
			"lane:1" => SerialScope::Lane(1),
			"lane:2" => SerialScope::Lane(2),
			"test-group" => SerialScope::TestGroup,
			"doc" => SerialScope::Doc,
			other => panic!(
				"{ALLOWLIST}:{at}: scope `{other}` is not lane:1, lane:2, test-group or doc; \
				 a serial setting never holds a lane of more than 2 CPUs"
			),
		};
		assert!(
			fields[3].trim().len() >= 20,
			"{ALLOWLIST}:{at}: every entry needs a written reason"
		);
		let entry = entries
			.entry((fields[0].to_string(), fields[1].trim().to_string()))
			.or_insert(AllowEntry {
				scope: scope.clone(),
				count: 0,
			});
		assert_eq!(entry.scope, scope, "{ALLOWLIST}:{at}: one line, two scopes");
		entry.count += 1;
	}
	entries
}

/// CPUs in a cpuset list such as `0-7,16-23` or `22,23`.
fn cpu_list_size(list: &str) -> Option<usize> {
	let mut cpus = std::collections::BTreeSet::new();
	for item in list.split(',') {
		let (lo, hi) = match item.split_once('-') {
			Some((lo, hi)) => (lo.parse::<usize>().ok()?, hi.parse::<usize>().ok()?),
			None => {
				let cpu = item.parse::<usize>().ok()?;
				(cpu, cpu)
			}
		};
		if lo > hi {
			return None;
		}
		cpus.extend(lo..=hi);
	}
	(!cpus.is_empty()).then_some(cpus.len())
}

/// The CPU count of a cpuset declared on this line, if any: `taskset -c L`,
/// `taskset --cpu-list L`, `CARGO_CGROUP_CPUS=L`, or a Windows `/affinity HEX`.
fn declared_cpuset_size(line: &str) -> Option<usize> {
	let words: Vec<&str> = line.split_whitespace().collect();
	for (i, word) in words.iter().enumerate() {
		let word = word.trim_matches(|c| c == '"' || c == '\'');
		if let Some(list) = word.strip_prefix("CARGO_CGROUP_CPUS=") {
			return cpu_list_size(list);
		}
		let next = words
			.get(i + 1)
			.map(|w| w.trim_matches(|c| c == '"' || c == '\''));
		match (word, next) {
			("-c" | "--cpu-list", Some(list)) if i > 0 && words[i - 1].ends_with("taskset") => {
				return cpu_list_size(list);
			}
			("/affinity", Some(mask)) => {
				return u64::from_str_radix(mask, 16)
					.ok()
					.map(|m| m.count_ones() as usize);
			}
			_ => {}
		}
	}
	None
}

/// Trimmed lines of a Rust source inside a fenced code block of a doc comment
/// (`///` or `//!`): rustdoc compiles and runs those as doctests, so they are
/// code, never `doc`.
fn rust_doc_code_lines(text: &str) -> std::collections::BTreeSet<String> {
	let mut lines = std::collections::BTreeSet::new();
	let mut in_block = false;
	for raw in text.lines() {
		let line = raw.trim();
		let Some(body) = line
			.strip_prefix("///")
			.or_else(|| line.strip_prefix("//!"))
		else {
			in_block = false;
			continue;
		};
		if body.trim_start().starts_with("```") {
			in_block = !in_block;
			continue;
		}
		if in_block {
			lines.insert(line.to_string());
		}
	}
	lines
}

/// Problems with one allowlisted occurrence, given where it sits; `doc_code`
/// says whether it sits in a doctest code block.
fn scope_problems(file: &str, line: &str, scope: &SerialScope, doc_code: bool) -> Vec<String> {
	match scope {
		SerialScope::TestGroup => {
			if file.ends_with("nextest.toml")
				&& (line.contains("max-threads") || line.contains("threads-required"))
			{
				Vec::new()
			} else {
				vec![format!(
					"{file}: `{line}` is not a nextest test-group limit"
				)]
			}
		}
		SerialScope::Doc if doc_code => vec![format!(
			"{file}: `{line}` is in a doctest code block, which runs; it is code, not doc"
		)],
		SerialScope::Doc => {
			if file.ends_with(".rs") && (line.starts_with("//") || line.starts_with("#[ignore")) {
				Vec::new()
			} else {
				vec![format!(
					"{file}: `{line}` is not a Rust comment or #[ignore] reason"
				)]
			}
		}
		SerialScope::Lane(allowed) => match declared_cpuset_size(line) {
			Some(size) if size <= *allowed => Vec::new(),
			Some(size) => vec![format!(
				"{file}: `{line}` holds a {size}-CPU lane; a serial setting gets at most {allowed}"
			)],
			None => vec![format!(
				"{file}: `{line}` declares no cpuset on its own line; a serial setting must sit in \
				 a dedicated lane of at most {allowed} CPU(s)"
			)],
		},
	}
}

#[test]
fn allowlist_parser_and_lane_rule_probe() {
	let parsed = parse_allowlist(
		"# c\nci/x.sh\ttaskset -c 23 cargo test -j1\tlane:1\ttimes one syscall path on a quiet core\n",
	);
	assert_eq!(parsed.len(), 1);
	for refused in [
		"ci/x.sh\tcargo test -j1\tlane:1\t\n",
		"ci/x.sh\tcargo test -j1\tlane:8\tan eight-core lane for one thread is refused\n",
		"ci/x.sh\tcargo test -j1\ttaskset\tunknown scopes are refused outright here\n",
	] {
		assert!(
			std::panic::catch_unwind(|| parse_allowlist(refused)).is_err(),
			"must refuse {refused:?}"
		);
	}
	assert_eq!(cpu_list_size("0-7,16-23"), Some(16));
	assert_eq!(cpu_list_size("22,23"), Some(2));
	assert_eq!(cpu_list_size("7-2"), None);
	assert_eq!(
		declared_cpuset_size("CARGO_CGROUP_CPUS=22-23 step D x"),
		Some(2)
	);
	assert_eq!(declared_cpuset_size("taskset -c 0-7 cargo test"), Some(8));
	assert_eq!(
		declared_cpuset_size("start \"\" /affinity 3 /wait /b cargo"),
		Some(2)
	);
	assert_eq!(declared_cpuset_size("cargo test -- --test-threads=1"), None);
	// Negative controls: one thread in an 8-CPU lane, or in no declared lane.
	let line = "taskset -c 0-7 cargo test -- --test-threads=1";
	assert!(!scope_problems("ci/x.sh", line, &SerialScope::Lane(2), false).is_empty());
	assert!(!scope_problems("ci/x.sh", "cargo test -j1", &SerialScope::Lane(1), false).is_empty());
	assert!(
		!scope_problems("ci/x.sh", "max-threads = 1", &SerialScope::TestGroup, false).is_empty()
	);
	assert!(
		!scope_problems("ci/x.sh", "/// --test-threads 1", &SerialScope::Doc, false).is_empty()
	);
	assert!(
		!scope_problems(
			"src/a.rs",
			"cmd.arg(\"--test-threads=1\")",
			&SerialScope::Doc,
			false
		)
		.is_empty()
	);
	assert!(
		scope_problems(
			"src/a.rs",
			"/// --test-threads 1`.",
			&SerialScope::Doc,
			false
		)
		.is_empty()
	);
	let line = "taskset -c 22,23 cargo test -- --test-threads=1";
	assert!(scope_problems("ci/x.sh", line, &SerialScope::Lane(2), false).is_empty());
	// A doctest line is code even though it starts with `///`.
	let source = "/// Run it:\n/// ```\n/// run(\"--test-threads=1\");\n/// ```\n/// --test-threads 1`.\nfn f() {}\n";
	let code = rust_doc_code_lines(source);
	assert!(code.contains("/// run(\"--test-threads=1\");"));
	assert!(!code.contains("/// --test-threads 1`."));
	let line = "/// run(\"--test-threads=1\");";
	assert!(!scope_problems("src/a.rs", line, &SerialScope::Doc, true).is_empty());
}

#[test]
fn serial_settings_are_exactly_the_allowlist_and_sit_in_small_lanes() {
	let root = ket_root();
	let allowlist = parse_allowlist(
		&read_text(&root, ALLOWLIST).unwrap_or_else(|| panic!("missing {ALLOWLIST}")),
	);
	let mut found: BTreeMap<(String, String), usize> = BTreeMap::new();
	let mut doctest_lines = std::collections::BTreeSet::new();
	let mut scoped = 0usize;
	for file in tracked_files(&root) {
		if !in_serial_scope(&file.path, file.executable) {
			continue;
		}
		let Some(text) = read_text(&root, &file.path) else {
			continue;
		};
		scoped += 1;
		let vars = one_valued_variables(&text);
		let doc_code = rust_doc_code_lines(&text);
		for line in text.lines() {
			if is_serial_setting(line, &vars) {
				if doc_code.contains(line.trim()) {
					doctest_lines.insert((file.path.clone(), line.trim().to_string()));
				}
				*found
					.entry((file.path.clone(), line.trim().to_string()))
					.or_default() += 1;
			}
		}
	}
	assert!(scoped > 1000, "serial scope matched only {scoped} files");
	let mut problems = Vec::new();
	for ((file, line), count) in &found {
		match allowlist.get(&(file.clone(), line.clone())) {
			Some(entry) if entry.count == *count => {
				let in_doctest = doctest_lines.contains(&(file.clone(), line.clone()));
				problems.extend(scope_problems(file, line, &entry.scope, in_doctest));
			}
			Some(entry) => problems.push(format!(
				"{file}: `{line}` occurs {count} time(s), allowlisted {}",
				entry.count
			)),
			None => problems.push(format!("{file}: unlisted serial setting `{line}`")),
		}
	}
	for (file, line) in allowlist.keys() {
		if !found.contains_key(&(file.clone(), line.clone())) {
			problems.push(format!("{ALLOWLIST}: stale entry {file}: `{line}`"));
		}
	}
	assert!(
		problems.is_empty(),
		"serial settings must be narrow, reasoned, allowlisted and confined to lanes of at \
		 most 2 CPUs (nilket/plans/ops/test_parallelism.md):\n{}",
		problems.join("\n")
	);
}

// ---------------------------------------------------------- cargo-cgroup limits

#[test]
fn cargo_cgroup_default_expressions_are_pinned() {
	let script = read_text(&ket_root(), CARGO_CGROUP).expect("read cargo-cgroup");
	for expected in [
		"test_threads=\"${CARGO_CGROUP_RUST_TEST_THREADS:-1024}\"",
		"memory_high_cfg=\"${CARGO_CGROUP_MEMORY_HIGH:-28G}\"",
		"memory_max_cfg=\"${CARGO_CGROUP_MEMORY_MAX:-32G}\"",
		"slice=\"${CARGO_CGROUP_SLICE:-cargo-cgroup.slice}\"",
		"slice_memory_max_cfg=\"${CARGO_CGROUP_SLICE_MEMORY_MAX:-96G}\"",
		"slice_memory_percent=\"${CARGO_CGROUP_SLICE_MEMORY_PERCENT:-60}\"",
		"tasks_max=\"${CARGO_CGROUP_TASKS_MAX:-16384}\"",
		"nofile=\"${CARGO_CGROUP_NOFILE:-$(nofile_limit)}\"",
		"--unit=\"$unit\" --slice=\"${slice}\"",
		"set-property --runtime \"${slice}\" \"MemoryMax=${slice_memory_max}\"",
		"--property=\"LimitNOFILE=${nofile}\"",
		"--property=\"TasksMax=${tasks_max}\"",
		"runtime_max_sec=\"${CARGO_CGROUP_RUNTIME_MAX_SEC:-86400}\"",
		"runtime_dir=\"${CARGO_CGROUP_RUNTIME_DIR:-/run/user/$(id -u)}\"",
		"local lock=\"${runtime_dir}/cargo-cgroup-slice.lock\"",
		"was stopped by RuntimeMaxSec=",
		"--property=\"RuntimeMaxSec=${runtime_max_sec}\"",
		"\"${wrapped_cmd[@]}\" <&0 {lockfd}>&- &",
	] {
		assert!(script.contains(expected), "cargo-cgroup lost `{expected}`");
	}
	// Both env lists use the same value: wrapped_cmd (systemd, affinity) and nerdctl.
	assert!(script.contains("\t\"RUST_TEST_THREADS=${test_threads}\""));
	assert!(script.contains("--env \"RUST_TEST_THREADS=${test_threads}\""));
	assert_eq!(script.matches("RUST_TEST_THREADS=${").count(), 2);
	// The lock never lives in TMPDIR (per-lane), and the unit is not collected
	// before its Result (a RuntimeMaxSec stop) is read.
	assert!(!script.contains("${TMPDIR:-/tmp}}/cargo-cgroup-slice.lock"));
	assert!(!script.contains("--wait --collect"));
}

/// The resource values the systemd backend actually passes (slice and
/// invocation memory on several host sizes, tasks, open files, signal
/// handling) are asserted by the wrapper's shell test against fake systemd
/// tools; running it here makes those values part of the gate.
#[test]
fn cargo_cgroup_systemd_resource_test_passes() {
	let root = ket_root();
	let output = Command::new("bash")
		.arg(root.join(WRAPPER_TEST))
		.current_dir(&root)
		.output()
		.expect("run the cargo-cgroup shell test");
	let stdout = String::from_utf8_lossy(&output.stdout);
	assert!(
		output.status.success(),
		"{WRAPPER_TEST} failed\nstdout: {stdout}\nstderr: {}",
		String::from_utf8_lossy(&output.stderr)
	);
	for pass in [
		"PASS: host-aware slice and invocation memory, nofile minimum, signal stops the unit",
		"PASS: the slice limit is never lowered under a running session",
		"PASS: a RuntimeMaxSec stop is reported with rc 124",
		"PASS: default test threads are 1024 and the override is validated",
		"PASS: systemd environment allowlist",
	] {
		assert!(stdout.contains(pass), "missing `{pass}` in: {stdout}");
	}
}

/// Runs the real wrapper through its affinity backend, with `cargo-envprobe`
/// as the child, and reads back what the child received.
fn probe_test_threads(overrides: &[(&str, &str)]) -> (Option<i32>, String) {
	let root = ket_root().join("nilket");
	let scripts = root.join("scripts");
	let path = format!(
		"{}:{}",
		scripts.display(),
		std::env::var("PATH").unwrap_or_default()
	);
	let mut cmd = Command::new(scripts.join("cargo-cgroup"));
	cmd.arg("envprobe")
		.current_dir(&root)
		.env("PATH", path)
		.env("CARGO_CGROUP_BACKEND", "affinity")
		.env("CARGO_CGROUP_ALLOW_AFFINITY_ONLY", "1")
		.env("CARGO_CGROUP_CPUS", "0")
		.env("RUST_TEST_THREADS", "99")
		.env_remove("CARGO_CGROUP_RUST_TEST_THREADS");
	for (name, value) in overrides {
		cmd.env(name, value);
	}
	let output = cmd.output().expect("run cargo-cgroup envprobe");
	let threads = String::from_utf8_lossy(&output.stdout)
		.lines()
		.find_map(|line| line.strip_prefix("RUST_TEST_THREADS=").map(str::to_string))
		.unwrap_or_default();
	(output.status.code(), threads)
}

#[test]
fn cargo_cgroup_delivers_1024_test_threads_and_honours_the_override() {
	assert_eq!(probe_test_threads(&[]), (Some(0), "1024".to_string()));
	assert_eq!(
		probe_test_threads(&[("CARGO_CGROUP_RUST_TEST_THREADS", "7")]),
		(Some(0), "7".to_string())
	);
	// Negative control: a malformed override is refused, not passed through.
	let (code, threads) = probe_test_threads(&[("CARGO_CGROUP_RUST_TEST_THREADS", "all")]);
	assert_eq!(code, Some(64));
	assert!(threads.is_empty());
}

// --------------------------------------------------------------- ladder driver

/// `step <lane> <name> <budget> <cargo args...>`: the budget is a timeout(1)
/// duration such as `45m`.
fn step_budget_problem(line: &str) -> Option<String> {
	let words: Vec<&str> = line.split_whitespace().collect();
	let budget = words.get(3).copied().unwrap_or("");
	let digits = budget.trim_end_matches(['s', 'm', 'h']);
	let ok = budget.len() > digits.len()
		&& !digits.is_empty()
		&& digits.chars().all(|c| c.is_ascii_digit());
	(!ok).then(|| format!("step without a timeout budget: `{line}`"))
}

/// Shared pins for a checked-in driver template: no serial step outside a
/// lane of at most 2 CPUs, and every step bounded by timeout(1).
fn check_driver_template(path: &str, invocation: &str, min_steps: usize) -> String {
	let template = read_text(&ket_root(), path).unwrap_or_else(|| {
		panic!("missing {path}: the serial-settings pin covers the driver through it")
	});
	assert!(in_serial_scope(path, false));
	// A noise-sensitive step may run alone only in a dedicated lane of at most
	// 2 CPUs declared on its own line; the full scan also requires an allowlist
	// entry for it.
	let vars = one_valued_variables(&template);
	let oversized: Vec<String> = template
		.lines()
		.filter(|line| is_serial_setting(line, &vars))
		.flat_map(|line| scope_problems(path, line.trim(), &SerialScope::Lane(2), false))
		.collect();
	assert!(oversized.is_empty(), "{path}: {oversized:?}");
	// A hang must end: every step runs under timeout(1) with a budget.
	assert!(
		template.contains(&format!("timeout --kill-after=60 \"$budget\" {invocation}")),
		"{path}: step() no longer wraps {invocation} in timeout"
	);
	let steps: Vec<&str> = template
		.lines()
		.map(str::trim)
		.filter(|line| line.starts_with("step "))
		.collect();
	assert!(
		steps.len() >= min_steps,
		"{path}: only {} steps found",
		steps.len()
	);
	let unbounded: Vec<String> = steps
		.iter()
		.filter_map(|l| step_budget_problem(l))
		.collect();
	assert!(unbounded.is_empty(), "{path}: {unbounded:?}");
	template
}

#[test]
fn checked_in_ladder_template_is_scanned_and_every_step_is_bounded() {
	let template = check_driver_template(LADDER_TEMPLATE, "scripts/cargo-cgroup", 15);
	assert!(template.contains("(cd \"$SRC/nilket\" && CARGO_CGROUP_RUNTIME_MAX_SEC="));
	// Negative control for the budget parser.
	assert!(step_budget_problem("step A wendelon-test test --locked").is_some());
	assert!(step_budget_problem("step A wendelon-test 45m test --locked").is_none());
}

/// The supplement runs plan-commit steps with the audited commit's wrapper,
/// copied aside before any plan commit is checked out, never the plan
/// commit's own (older) wrapper.
#[test]
fn checked_in_supplement_template_uses_the_commit_wrapper() {
	let template = check_driver_template(SUPPLEMENT_TEMPLATE, "\"$WRAPPER\"", 4);
	let copy = template
		.find("cp \"$SRC/nilket/scripts/cargo-cgroup\" \"$WRAPPER\"")
		.expect("supplement copies the commit's wrapper aside");
	let plan_loop = template
		.find("for pc in $PLAN_COMMITS")
		.expect("supplement has a plan-commit loop");
	assert!(
		copy < plan_loop,
		"the wrapper must be copied before any plan checkout"
	);
	assert!(
		!template.contains("&& scripts/cargo-cgroup"),
		"a step uses the checked-out wrapper"
	);
}
