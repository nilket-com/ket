#![cfg(unix)]

use libaryon::core::{OwnerId, SessionStore};
use std::ffi::OsString;
use std::fs;
use std::os::unix::ffi::OsStringExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

struct TempState(PathBuf);

impl TempState {
	fn new(label: &str) -> Self {
		let nonce = SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.unwrap()
			.as_nanos();
		Self(std::env::temp_dir().join(format!(
			"polariton-product-0073-{}-{label}-{nonce}",
			std::process::id()
		)))
	}

	fn session_root(&self) -> PathBuf {
		self.0.join("libaryon").join("sessions")
	}
}

impl Drop for TempState {
	fn drop(&mut self) {
		let _ = fs::remove_dir_all(&self.0);
	}
}

fn run_list(state: &Path) -> std::process::Output {
	Command::new(env!("CARGO_BIN_EXE_polariton"))
		.args(["--session", "list"])
		.env("XDG_STATE_HOME", state)
		.output()
		.expect("run terminal discovery")
}

#[test]
fn product_0073_terminal_list_is_non_mutating_discriminating_and_head_neutral() {
	let absent = TempState::new("absent");
	let output = run_list(&absent.0);
	assert!(
		output.status.success(),
		"{}",
		String::from_utf8_lossy(&output.stderr)
	);
	assert_eq!(output.stdout, b"No saved sessions.\n");
	assert!(
		!absent.0.exists(),
		"listing must not create the platform root"
	);

	let state = TempState::new("report");
	unsafe { std::env::set_var("XDG_STATE_HOME", &state.0) };
	let store = SessionStore::platform_default().unwrap();

	let mut active = store.create().unwrap();
	let active_id = active.snapshot().session_id();
	active.mint_document(None).unwrap();
	active.commit().unwrap();

	let mut committed = store.create().unwrap();
	let committed_id = committed.snapshot().session_id();
	let file_id = committed
		.mint_document(Some(PathBuf::from("/work/demo.rn")))
		.unwrap();
	let scratch_id = committed.mint_document(None).unwrap();
	let scratch_spelling_id = committed
		.mint_document(Some(PathBuf::from("<scratch>")))
		.unwrap();
	let non_utf8_id = committed
		.mint_document(Some(PathBuf::from(OsString::from_vec(vec![
			b'/', b't', b'm', b'p', b'/', 0xff,
		]))))
		.unwrap();
	committed.set_owner_payload(
		OwnerId::new("polariton_session").unwrap(),
		2,
		b"not valid Polariton JSON".to_vec(),
	);
	committed.commit().unwrap();
	drop(committed);

	let damaged = store.create().unwrap();
	let damaged_id = damaged.snapshot().session_id();
	drop(damaged);
	fs::write(
		state
			.session_root()
			.join(damaged_id.to_string())
			.join("CURRENT"),
		b"broken\n",
	)
	.unwrap();
	fs::write(state.session_root().join("notes"), b"foreign").unwrap();

	let output = run_list(&state.0);
	assert!(
		output.status.success(),
		"{}",
		String::from_utf8_lossy(&output.stderr)
	);
	let stdout = String::from_utf8(output.stdout).unwrap();
	assert!(stdout.starts_with("STATE      UPDATED_UTC"));
	assert!(stdout.contains(&format!(
		"active     -                       -     ?  {active_id}"
	)));
	assert!(stdout.contains(&format!("{committed_id}\n")));
	let committed_row = stdout
		.lines()
		.find(|line| line.ends_with(&committed_id.to_string()))
		.expect("committed summary row");
	let fields = committed_row.split_whitespace().collect::<Vec<_>>();
	assert_eq!(fields[0], "committed");
	chrono::DateTime::parse_from_rfc3339(fields[1]).expect("RFC 3339 committed UTC time");
	assert!(fields[1].ends_with('Z'));
	assert_eq!(fields[2], "2", "current committed generation");
	assert_eq!(fields[3], "4", "all file and scratch documents counted");
	assert_eq!(fields[4], committed_id.to_string());
	assert!(stdout.contains(&format!("document {file_id}  /work/demo.rn")));
	assert!(stdout.contains(&format!("document {scratch_id}  <scratch>")));
	assert!(stdout.contains(&format!(
		"document {scratch_spelling_id}  hex:3c736372617463683e"
	)));
	assert!(stdout.contains(&format!("document {non_utf8_id}  hex:2f746d702fff")));
	assert!(stdout.contains(&format!(
		"damaged    -                       -     ?  {damaged_id}\n  corrupt saved session: CURRENT must be 16 lowercase hexadecimal digits plus newline"
	)));
	assert!(stdout.contains("foreign    -                       -     ?  \"notes\""));
	assert!(
		stdout.find(&active_id.to_string()).unwrap()
			< stdout.find(&committed_id.to_string()).unwrap(),
		"active sessions sort before committed sessions"
	);
	assert!(
		stdout.find(&committed_id.to_string()).unwrap()
			< stdout.find(&damaged_id.to_string()).unwrap(),
		"committed sessions sort before damaged sessions"
	);

	assert_eq!(
		active.snapshot().documents().count(),
		1,
		"active holder remains alive while listing completes"
	);
	drop(active);
}
