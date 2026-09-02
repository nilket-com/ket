use std::process::Command;

fn git(args: &[&str]) -> Option<String> {
	let output = Command::new("git").args(args).output().ok()?;
	output
		.status
		.success()
		.then(|| String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn main() {
	// ADR 581 cut 1 keeps the legacy stamp solely as the deciding gate while content identity
	// runs in shadow. Cut 2 deletes this build script and the dirty authority input.
	println!("cargo:rerun-if-changed=.git/HEAD");
	println!("cargo:rerun-if-changed=.git/index");
	let commit = git(&["rev-parse", "--short=12", "HEAD"])
		.filter(|value| !value.is_empty())
		.unwrap_or_else(|| "unknown".to_string());
	let dirty = git(&["status", "--porcelain", "--untracked-files=no"])
		.map(|value| !value.is_empty())
		.unwrap_or(false);
	let stamp = if dirty {
		format!("{commit}-dirty")
	} else {
		commit
	};
	println!("cargo:rustc-env=TORON_TPCH_BUILD_COMMIT={stamp}");
}
