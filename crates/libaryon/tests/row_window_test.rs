//! Row windows (crates/libaryon/plans/adr/0009-wrapped-long-lines.md): a wrapped view that starts
//! inside a line longer than one extent publishes that line from its first visible row's byte.

use libaryon::engine::settings::EngineSettings;
use libaryon::engine::{EditorCommand, EngineConfig, EngineHandle};
use libaryon::uast::{PaneViewport, Viewport};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const WIDTH: u16 = 40;
const HEIGHT: u16 = 10;
const DEADLINE: Duration = Duration::from_secs(60);
/// One cold extent.
const EXTENT: u64 = 1 << 20;

fn scratch_root() -> PathBuf {
	let nonce = SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.expect("clock after epoch")
		.as_nanos();
	let root =
		std::env::temp_dir().join(format!("baryon-row-window-{}-{nonce}", std::process::id()));
	fs::create_dir_all(&root).expect("create scratch root");
	root
}

fn spawn(
	file: &Path,
) -> (
	EngineHandle,
	libaryon::engine::viewport_channel::ViewportReceiver,
) {
	let mut engine = EngineHandle::spawn(EngineConfig {
		initial_file: Some(file.to_string_lossy().into_owned()),
		persistent_history: false,
		..EngineConfig::from(EngineSettings::default())
	});
	let viewports = engine.take_viewports().expect("viewport receiver");
	engine
		.command_sender()
		.send(EditorCommand::Resize(WIDTH, HEIGHT))
		.expect("resize engine");
	(engine, viewports)
}

fn focused(viewport: &Viewport) -> &PaneViewport {
	viewport.focused_pane().expect("focused pane")
}

/// Scroll forward until a viewport satisfies `done`. A scroll stops at the
/// index's scanned prefix, so it is repeated while the engine scans further.
fn scroll_until(
	engine: &EngineHandle,
	viewports: &libaryon::engine::viewport_channel::ViewportReceiver,
	rows: i32,
	done: impl Fn(&PaneViewport) -> bool,
) -> Viewport {
	let deadline = Instant::now() + DEADLINE;
	loop {
		engine
			.command_sender()
			.send(EditorCommand::ScrollViewport(rows))
			.expect("scroll");
		let viewport = viewports
			.recv_timeout(Duration::from_secs(5))
			.expect("a viewport after scrolling");
		if done(focused(&viewport)) {
			return viewport;
		}
		assert!(Instant::now() < deadline, "the scroll never got there");
	}
}

#[test]
fn row_windows() {
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

	// A newline-free sparse file: every cell is one NUL byte and one column, so
	// a row's start byte equals its display offset.
	let sparse = root.join("sparse.bin");
	fs::File::create(&sparse)
		.and_then(|file| file.set_len(16 * EXTENT))
		.expect("create sparse fixture");
	let (engine, viewports) = spawn(&sparse);
	viewports.recv_timeout(DEADLINE).expect("first viewport");
	let viewport = scroll_until(&engine, &viewports, 20_000, |pane| {
		pane.row_window
			.is_some_and(|window| window.start_byte.get() > EXTENT)
	});
	let pane = focused(&viewport);
	let window = pane.row_window.expect("a row window");
	let first = pane.visible_rows.first().expect("visible rows");
	assert_eq!(first.start_byte, Some(window.start_byte));
	assert_eq!(first.start_offset, Some(window.start_offset));
	assert_eq!(window.start_byte.get(), window.start_offset.get());
	assert_eq!(u64::from(window.tab_phase), window.start_offset.get() % 4);
	assert_eq!(pane.render_rows, pane.visible_rows);
	assert_eq!(pane.viewport_row_offset.get(), 0);
	assert_eq!(
		pane.tokens.first().map(|token| token.absolute_start_byte),
		Some(window.start_byte)
	);
	assert!(
		pane.tokens
			.iter()
			.all(|token| token.absolute_start_byte >= window.start_byte),
		"no token starts before the window"
	);
	engine.shutdown().expect("stop engine");

	// Highlighting starts at the window: a window inside a long string keeps
	// absolute spans at or after its start (ADR 0009 consequence).
	let source = root.join("long_string.rs");
	let mut text = b"fn main() { let s = \"".to_vec();
	text.extend(std::iter::repeat_n(b'a', (EXTENT + EXTENT / 2) as usize));
	text.extend_from_slice(b"\"; }\n");
	fs::write(&source, &text).expect("write long string");
	let (engine, viewports) = spawn(&source);
	viewports.recv_timeout(DEADLINE).expect("first viewport");
	let viewport = scroll_until(&engine, &viewports, 2_000, |pane| pane.row_window.is_some());
	let pane = focused(&viewport);
	let window = pane.row_window.expect("a row window");
	assert!(
		pane.highlights
			.iter()
			.all(|span| span.start >= window.start_byte),
		"highlight spans are absolute and inside the window"
	);
	engine.shutdown().expect("stop engine");

	// A window that starts in trailing whitespace starts at the run: its origin
	// is the run's byte and offset, while each row keeps its own offset and no
	// whitespace row claims a byte.
	let trailing = root.join("trailing.txt");
	let word_bytes = (EXTENT + EXTENT / 2) as usize;
	let mut text = vec![b'a'; word_bytes];
	text.extend(std::iter::repeat_n(b' ', 3_000));
	fs::write(&trailing, &text).expect("write trailing whitespace");
	let (engine, viewports) = spawn(&trailing);
	viewports.recv_timeout(DEADLINE).expect("first viewport");
	let viewport = scroll_until(&engine, &viewports, 20_000, |pane| {
		pane.row_window.is_some()
			&& pane
				.visible_rows
				.first()
				.is_some_and(|row| row.start_byte.is_none())
	});
	let pane = focused(&viewport);
	let window = pane.row_window.expect("a row window");
	// One byte and one column per cell: the run starts after the word.
	assert_eq!(window.start_byte.get(), word_bytes as u64);
	assert_eq!(window.start_offset.get(), word_bytes as u64);
	assert_eq!(
		pane.tokens.first().map(|token| token.absolute_start_byte),
		Some(window.start_byte)
	);
	let mut previous = window.start_offset;
	for row in &pane.visible_rows {
		assert_eq!(
			row.start_byte, None,
			"a whitespace row claims no byte: {row:?}"
		);
		let offset = row.start_offset.expect("an indexed row has an offset");
		assert!(offset > previous, "rows keep their own, increasing offsets");
		previous = offset;
	}
	engine.shutdown().expect("stop engine");

	// Short lines publish no window, so their fields keep their meaning.
	let short = root.join("short.txt");
	fs::write(&short, "first line\nsecond line\nthird line\n".repeat(50)).expect("write short");
	let (engine, viewports) = spawn(&short);
	viewports.recv_timeout(DEADLINE).expect("first viewport");
	engine
		.command_sender()
		.send(EditorCommand::ScrollViewport(5))
		.expect("scroll");
	let viewport = viewports
		.recv_timeout(Duration::from_secs(5))
		.expect("a viewport after scrolling");
	assert_eq!(focused(&viewport).row_window, None);
	engine.shutdown().expect("stop engine");
	let _ = fs::remove_dir_all(&root);
}
