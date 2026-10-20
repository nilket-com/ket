//! T-PASS on the built binary: passphrases come only from the controlling terminal. Without one
//! (`setsid`), the binary refuses before anything else, whatever the environment says; under a
//! pseudo-terminal (`script`) it gets past that check; and the process hardens itself at startup.
use std::process::{Command, Stdio};

const BIN: &str = env!("CARGO_BIN_EXE_wendelon-key");

fn missing_dir() -> String {
	std::env::temp_dir()
		.join(format!("wendelon-key-absent-{}", std::process::id()))
		.display()
		.to_string()
}

fn generate_args() -> Vec<String> {
	[
		"generate",
		"--dir",
		&missing_dir(),
		"--role",
		"admin",
		"--intended-epoch",
		"0",
		"--out",
		"k",
	]
	.iter()
	.map(|s| s.to_string())
	.collect()
}

#[test]
fn without_a_terminal_the_binary_refuses_whatever_the_environment_holds() {
	let output = Command::new("setsid")
		.arg("--wait")
		.arg(BIN)
		.args(generate_args())
		.env(
			"WENDELON_KEY_PASSPHRASE",
			"an environment passphrase that is ignored",
		)
		.env("PASSPHRASE", "another ignored passphrase value")
		.stdin(Stdio::null())
		.output()
		.unwrap();
	let err = String::from_utf8_lossy(&output.stderr);
	assert_eq!(output.status.code(), Some(2), "{err}");
	assert!(
		err.contains("the passphrase source is not a terminal"),
		"{err}"
	);
}

#[test]
fn under_a_pseudo_terminal_the_binary_passes_the_terminal_check() {
	let command = std::iter::once(BIN.to_string())
		.chain(generate_args())
		.map(|a| format!("'{a}'"))
		.collect::<Vec<_>>()
		.join(" ");
	let output = Command::new("script")
		.args(["-q", "-e", "-c", &command, "/dev/null"])
		.stdin(Stdio::null())
		.output()
		.unwrap();
	let shown = String::from_utf8_lossy(&output.stdout);
	assert!(!shown.contains("not a terminal"), "{shown}");
	// It went on to the custody directory, which does not exist.
	assert!(shown.contains(&missing_dir()), "{shown}");
	assert_eq!(output.status.code(), Some(2), "{shown}");
}

#[test]
fn an_unknown_flag_is_refused_and_there_is_no_passphrase_flag() {
	let output = Command::new(BIN)
		.args(generate_args())
		.args(["--passphrase", "x"])
		.stdin(Stdio::null())
		.output()
		.unwrap();
	let err = String::from_utf8_lossy(&output.stderr);
	assert!(err.contains("unknown argument \"--passphrase\""), "{err}");
}
