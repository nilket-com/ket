//! Guards for `scripts/cargo-cgroup`, the canonical test invocation.

use super::support::manifest_dir;

fn repo_root() -> std::path::PathBuf {
	manifest_dir()
		.parent()
		.and_then(|crates| crates.parent())
		.expect("workspace root above crates/toron")
		.to_path_buf()
}

/// The child process must actually receive `RUST_MIN_STACK`.
///
/// Deep-recursion targets overflow the default 8 MiB test-thread stack and
/// abort. An aborted target prints no `test result:` line, so a receipt that
/// sums reported failures reads clean while that target's remaining tests
/// vanish — `explain_analyze_test` did exactly this.
///
/// This runs the real wrapper end to end: `cargo <name>` dispatches to
/// `cargo-<name>` on PATH, so `cargo-envprobe` lands as the wrapper's child and
/// prints what it actually received. Asserting the intent — that the script
/// mentions the variable — would not catch the failure this exists for, which
/// is the value being lost on the hop into the execution backend.
#[test]
pub(crate) fn cargo_cgroup_delivers_the_required_stack_to_its_child() {
	let root = repo_root();
	let scripts = root.join("scripts");
	let wrapper = scripts.join("cargo-cgroup");
	assert!(wrapper.is_file(), "missing {}", wrapper.display());

	let path = format!(
		"{}:{}",
		scripts.display(),
		std::env::var("PATH").unwrap_or_default()
	);
	// The affinity backend runs the shared `wrapped_cmd` array directly. The
	// systemd backend passes that same array to `systemd-run`, which does not
	// inherit the caller's PATH — so the probe can only be *delivered* through
	// affinity, while the environment plumbing under test is identical.
	let output = std::process::Command::new(&wrapper)
		.arg("envprobe")
		.current_dir(&root)
		.env("PATH", path)
		.env("CARGO_CGROUP_BACKEND", "affinity")
		.env("CARGO_CGROUP_ALLOW_AFFINITY_ONLY", "1")
		.output()
		.expect("run cargo-cgroup envprobe");

	let stdout = String::from_utf8_lossy(&output.stdout);
	assert!(
		stdout.contains("RUST_MIN_STACK=67108864"),
		"child did not receive the declared stack; stdout: {stdout}\nstderr: {}",
		String::from_utf8_lossy(&output.stderr)
	);
	assert!(
		!stdout.contains("RUST_MIN_STACK=<unset>"),
		"stack reached the child unset: {stdout}"
	);
	// The stack must not have arrived by inheriting ambient user-manager state.
	assert!(
		std::process::Command::new("systemctl")
			.args(["--user", "show-environment"])
			.output()
			.map(|out| !String::from_utf8_lossy(&out.stdout).contains("RUST_MIN_STACK"))
			.unwrap_or(true),
		"RUST_MIN_STACK is set in the user manager; this control cannot \
		 distinguish the wrapper's declaration from ambient inheritance"
	);
}

/// Every backend declares the stack, including the one this host cannot run.
#[test]
pub(crate) fn cargo_cgroup_declares_the_stack_for_every_backend() {
	let script = std::fs::read_to_string(repo_root().join("scripts/cargo-cgroup"))
		.expect("read cargo-cgroup");
	assert!(script.contains("min_stack=\"${CARGO_CGROUP_RUST_MIN_STACK:-67108864}\""));
	// `wrapped_cmd` serves systemd and affinity; nerdctl builds its own env list
	// and is not exercisable here, so it is pinned structurally.
	assert!(script.contains("\"RUST_MIN_STACK=${min_stack}\""));
	assert!(script.contains("--env \"RUST_MIN_STACK=${min_stack}\""));
}
