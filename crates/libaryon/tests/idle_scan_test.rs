//! A huge line on screen goes quiet (crates/libaryon/plans/adr/0011-scanning-on-demand.md): with no
//! input, the engine stops publishing frames once the view settles, and never blanks a window it had
//! drawn while it gets there.

use libaryon::engine::settings::EngineSettings;
use libaryon::engine::{EditorCommand, EngineConfig, EngineHandle};
use libaryon::uast::Viewport;
use std::fs;
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

fn scratch_root() -> PathBuf {
	let nonce = SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.expect("clock after epoch")
		.as_nanos();
	let root =
		std::env::temp_dir().join(format!("baryon-idle-scan-{}-{nonce}", std::process::id()));
	fs::create_dir_all(&root).expect("create scratch root");
	root
}

#[test]
fn an_idle_huge_line_stops_publishing_frames() {
	let root = scratch_root();
	// This integration-test process owns the environment for its sole test.
	unsafe {
		std::env::set_var("HOME", root.join("state"));
		std::env::set_var("XDG_STATE_HOME", root.join("state"));
		std::env::set_var("XDG_CONFIG_HOME", root.join("config"));
		std::env::set_var("XDG_CACHE_HOME", root.join("cache"));
		std::env::remove_var("DISPLAY");
		std::env::remove_var("WAYLAND_DISPLAY");
	}
	// One line of words, four extents long: long enough for the wrap index, and to take frames to
	// scan to its end.
	let words = "alpha beta\tgamma delta ".repeat((4 << 20) / 23);
	let file = root.join("long-line.txt");
	fs::write(&file, &words).expect("write fixture");
	let mut engine = EngineHandle::spawn(EngineConfig {
		initial_file: Some(file.to_string_lossy().into_owned()),
		persistent_history: false,
		..EngineConfig::from(EngineSettings::default())
	});
	let viewports = engine.take_viewports().expect("viewport receiver");
	let send = |command| engine.command_sender().send(command).expect("send");
	send(EditorCommand::Resize(80, 24));
	let has_text = |viewport: &Viewport| {
		viewport
			.focused_pane()
			.is_some_and(|pane| pane.tokens.iter().any(|token| !token.text.is_empty()))
	};
	let deadline = Instant::now() + Duration::from_secs(60);
	loop {
		let viewport = viewports
			.recv_timeout(Duration::from_secs(10))
			.expect("a first frame");
		if has_text(&viewport) {
			break;
		}
		assert!(Instant::now() < deadline, "the line never drew");
	}

	// Scroll one row, into the indexed line, then watch the frames that follow.
	send(EditorCommand::ScrollViewport(1));
	let mut frames = 0;
	let mut drawn = false;
	let settle = Instant::now() + Duration::from_secs(5);
	while let Some(left) = settle.checked_duration_since(Instant::now()) {
		let Ok(viewport) = viewports.recv_timeout(left) else {
			break;
		};
		frames += 1;
		let text = has_text(&viewport);
		assert!(
			!(drawn && !text),
			"frame {frames} blanked a window it had drawn"
		);
		drawn |= text;
	}
	assert!(drawn, "the scrolled view drew its rows");
	assert!(frames <= 8, "{frames} frames for a one-row scroll");
	let idle = viewports.recv_timeout(Duration::from_secs(3));
	assert!(
		idle.is_err(),
		"the engine kept publishing frames with no input"
	);

	engine.shutdown().expect("join engine");
	let _ = fs::remove_dir_all(root);
}
