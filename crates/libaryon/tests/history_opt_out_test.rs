use libaryon::engine::{EngineConfig, EngineHandle};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_home(label: &str) -> PathBuf {
	let nonce = SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.expect("clock after epoch")
		.as_nanos();
	std::env::temp_dir().join(format!(
		"baryon-history-{label}-{}-{nonce}",
		std::process::id()
	))
}

/// Every variable any platform consults for the engine state directory.
const STATE_VARIABLES: [&str; 5] = [
	"XDG_STATE_HOME",
	"HOME",
	"LOCALAPPDATA",
	"APPDATA",
	"USERPROFILE",
];

fn history_file(home: &Path) -> PathBuf {
	// macOS roots engine state under HOME and Windows under LOCALAPPDATA,
	// not XDG_STATE_HOME.
	if cfg!(windows) {
		home.join("baryon/command_history")
	} else if cfg!(target_os = "macos") {
		home.join("Library/Application Support/baryon/state/command_history")
	} else {
		home.join("baryon/command_history")
	}
}

/// Opens a startup file, which history records as `e <path>`, in an engine
/// whose state directories all point into `home`.
fn open_startup_file(home: &Path, persistent_history: bool) -> String {
	for variable in STATE_VARIABLES {
		unsafe { std::env::set_var(variable, home) };
	}
	fs::create_dir_all(home).expect("create temp home");
	let file = home.join("notes.md");
	fs::write(&file, "notes\n").expect("write startup file");
	let file = file.to_string_lossy().into_owned();
	let handle = EngineHandle::spawn(EngineConfig {
		persistent_history,
		initial_file: Some(file.clone()),
		..EngineConfig::default()
	});
	handle.shutdown().expect("join engine");
	format!("e {file}")
}

#[test]
fn disabled_persistent_history_never_writes_the_history_file() {
	// This integration-test process owns the environment for the duration of
	// its sole test, so no concurrently running Rust code can observe the swap.
	let saved = STATE_VARIABLES.map(|variable| (variable, std::env::var_os(variable)));

	let recorded = temp_home("recorded");
	let entry = open_startup_file(&recorded, true);
	let history = fs::read_to_string(history_file(&recorded)).unwrap_or_default();
	assert!(
		history.lines().any(|line| line == entry),
		"control: persistent history must record {entry:?}, got {history:?}"
	);

	let disabled = temp_home("disabled");
	open_startup_file(&disabled, false);
	assert!(
		!history_file(&disabled).exists(),
		"a disabled persistent history must not create the history file"
	);

	for (variable, value) in saved {
		match value {
			Some(value) => unsafe { std::env::set_var(variable, value) },
			None => unsafe { std::env::remove_var(variable) },
		}
	}
	let _ = fs::remove_dir_all(recorded);
	let _ = fs::remove_dir_all(disabled);
}
