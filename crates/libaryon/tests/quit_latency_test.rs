use libaryon::engine::{EngineConfig, EngineHandle, InputEvent, KeyCode, Modifiers};
use std::fs;
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const QUIT_LATENCY_LIMIT: Duration = Duration::from_millis(100);

fn temp_state_home() -> PathBuf {
	let nonce = SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.expect("clock after epoch")
		.as_nanos();
	std::env::temp_dir().join(format!(
		"baryon-quit-latency-{}-{nonce}",
		std::process::id()
	))
}

fn type_quit(handle: &EngineHandle) {
	let input = handle.input_sender();
	for code in [KeyCode::Char(':'), KeyCode::Char('q'), KeyCode::Enter] {
		input
			.send(InputEvent::Key {
				code,
				modifiers: Modifiers::NONE,
			})
			.expect("submit :q");
	}
}

#[test]
fn typed_quit_stops_a_production_configured_engine_promptly() {
	let state_home = temp_state_home();
	let history_dir = state_home.join("baryon");
	fs::create_dir_all(&history_dir).expect("create state directory");
	let history = (0..1_000)
		.map(|index| format!("command-{index:04}\n"))
		.collect::<String>();
	fs::write(history_dir.join("command_history"), history).expect("seed command history");

	// This integration-test process owns the environment for the duration of
	// its sole test, so no concurrently running Rust code can observe the swap.
	unsafe { std::env::set_var("XDG_STATE_HOME", &state_home) };

	let mut samples = Vec::new();
	for _ in 0..7 {
		let handle = EngineHandle::spawn(EngineConfig::default());
		let started = Instant::now();
		type_quit(&handle);
		handle.shutdown().expect("join engine after :q");
		samples.push(started.elapsed());
	}
	samples.sort_unstable();
	let median = samples[samples.len() / 2];
	eprintln!("typed :q samples: {samples:?}; median={median:?}");
	assert!(
		median < QUIT_LATENCY_LIMIT,
		"median typed :q latency {median:?} exceeded {QUIT_LATENCY_LIMIT:?}"
	);

	unsafe { std::env::remove_var("XDG_STATE_HOME") };
	let _ = fs::remove_dir_all(state_home);
}
