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
	let dirty = git(&["status", "--porcelain"]).map(|s| !s.is_empty());
	let stamp = match dirty {
		Some(true) => format!("{commit}-dirty"),
		Some(false) => commit,
		None => commit,
	};
	println!("cargo:rustc-env=TORON_TPCH_BUILD_COMMIT={stamp}");
}
