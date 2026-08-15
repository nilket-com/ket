// ADR 490 follow-up: stamp the gauge binary with the git commit it was built
// from, so a perf row can never silently come from a stale build again. Best
// effort — "unknown" when git is unavailable — and re-stamped whenever HEAD or
// the index moves.
use std::process::Command;

fn git(args: &[&str]) -> Option<String> {
	let output = Command::new("git").args(args).output().ok()?;
	if !output.status.success() {
		return None;
	}
	Some(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn main() {
	if let Some(git_dir) = git(&["rev-parse", "--git-dir"]) {
		println!("cargo:rerun-if-changed={git_dir}/HEAD");
		println!("cargo:rerun-if-changed={git_dir}/index");
	}
	let commit = git(&["rev-parse", "--short=12", "HEAD"]).unwrap_or_else(|| "unknown".to_string());
	// Tracked modifications only. The stamp answers "can this binary be attributed
	// to a commit someone can check out?", and an untracked file cannot change what
	// was compiled — a scratch log or a draft plan sitting in the tree does not make
	// the build unattributable.
	//
	// Counting untracked files here cost a complete 35-minute timing-atlas run:
	// every cell measured cleanly, replay violations were empty, and authority was
	// still withheld as `build_not_clean` because a log file and two draft notes
	// were present. A guard that voids good evidence over repo hygiene is charging
	// the wrong thing.
	let dirty = git(&["status", "--porcelain", "--untracked-files=no"]).map(|s| !s.is_empty());
	let stamp = match dirty {
		Some(true) => format!("{commit}-dirty"),
		Some(false) => commit,
		None => commit,
	};
	println!("cargo:rustc-env=TORON_TPCH_BUILD_COMMIT={stamp}");
}
