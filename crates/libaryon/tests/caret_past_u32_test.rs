//! The caret past `u32::MAX` columns (crates/libaryon/plans/adr/0010-caret-on-the-wrap-index.md):
//! scrolling deep into a newline-free sparse file places the caret at the first visible row, and it
//! moves from there. Scanning that far takes minutes in a debug build, so the test is ignored by
//! default and runs in release: `cargo test --release -p libaryon --test caret_past_u32_test --
//! --ignored`.

use libaryon::engine::settings::EngineSettings;
use libaryon::engine::{EditorCommand, EngineConfig, EngineHandle, MoveDirection};
use libaryon::uast::{PaneViewport, Viewport};
use std::fs;
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const WIDTH: u16 = 100;
const HEIGHT: u16 = 30;
/// Past `u32::MAX` columns, and so bytes: every cell of the file is one NUL byte and one column.
const FILE_BYTES: u64 = 5 << 30;
const TARGET: u64 = u32::MAX as u64 + (64 << 20);

fn scratch_root() -> PathBuf {
	let nonce = SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.expect("clock after epoch")
		.as_nanos();
	let root =
		std::env::temp_dir().join(format!("baryon-caret-u32-{}-{nonce}", std::process::id()));
	fs::create_dir_all(&root).expect("create scratch root");
	root
}

fn focused(viewport: &Viewport) -> &PaneViewport {
	viewport.focused_pane().expect("focused pane")
}

fn send(engine: &EngineHandle, command: EditorCommand) {
	engine.command_sender().send(command).expect("send command");
}

/// The last viewport once none has arrived for a while: a scroll that reached past the scanned
/// prefix keeps moving the view as the scan continues, and the caret with it.
fn settled(
	viewports: &libaryon::engine::viewport_channel::ViewportReceiver,
	mut latest: Viewport,
) -> Viewport {
	while let Ok(viewport) = viewports.recv_timeout(Duration::from_secs(3)) {
		latest = viewport;
	}
	latest
}

#[test]
#[ignore = "scans 4 GiB; run in release with --ignored"]
fn the_caret_is_placed_and_moves_past_u32_columns() {
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
	let sparse = root.join("sparse.bin");
	fs::File::create(&sparse)
		.and_then(|file| file.set_len(FILE_BYTES))
		.expect("create sparse fixture");
	let mut engine = EngineHandle::spawn(EngineConfig {
		initial_file: Some(sparse.to_string_lossy().into_owned()),
		persistent_history: false,
		..EngineConfig::from(EngineSettings::default())
	});
	let viewports = engine.take_viewports().expect("viewport receiver");
	send(&engine, EditorCommand::Resize(WIDTH, HEIGHT));

	// Scroll until the view starts past the target. Each scroll stops at the scanned prefix.
	let deadline = Instant::now() + Duration::from_secs(900);
	let viewport = loop {
		while viewports.try_recv().is_ok() {}
		send(&engine, EditorCommand::ScrollViewport(100_000));
		let viewport = viewports
			.recv_timeout(Duration::from_secs(10))
			.expect("a viewport after scrolling");
		let deep = focused(&viewport)
			.row_window
			.is_some_and(|window| window.start_offset.get() > TARGET);
		if deep {
			break viewport;
		}
		assert!(
			Instant::now() < deadline,
			"the scroll never got past u32 columns"
		);
	};
	let viewport = settled(&viewports, viewport);
	let pane = focused(&viewport);
	let first = pane.visible_rows[0];
	let origin = first.start_offset.expect("an indexed row has an offset");
	assert!(origin.get() > u64::from(u32::MAX));
	assert_eq!(first.start_col, origin, "start_col is exact past u32");
	assert_eq!(
		pane.cursor_abs_pos.col, origin,
		"scrolling places the caret at the first visible row"
	);
	assert_eq!(
		pane.cursor_abs_byte.get(),
		origin.get(),
		"one byte per column"
	);

	// Right moves one cell from there, without reading the line from its start.
	send(&engine, EditorCommand::MoveCursor(MoveDirection::Right));
	let deadline = Instant::now() + Duration::from_secs(60);
	let moved = loop {
		let viewport = viewports
			.recv_timeout(Duration::from_secs(10))
			.expect("a viewport after moving");
		if focused(&viewport).cursor_abs_pos.col != origin {
			break viewport;
		}
		assert!(Instant::now() < deadline, "the caret never moved");
	};
	let moved = settled(&viewports, moved);
	let pane = focused(&moved);
	assert_eq!(pane.cursor_abs_pos.col.get(), origin.get() + 1);
	assert_eq!(pane.cursor_abs_byte.get(), origin.get() + 1);

	// Left comes back one cell, a word motion moves within one window, and Home goes to the line's
	// start: none of them reads the line from its start.
	let step =
		|engine: &EngineHandle, command: EditorCommand, moved: &dyn Fn(&PaneViewport) -> bool| {
			send(engine, command);
			let deadline = Instant::now() + Duration::from_secs(60);
			loop {
				let viewport = viewports
					.recv_timeout(Duration::from_secs(10))
					.expect("a viewport after a motion");
				if moved(focused(&viewport)) {
					break settled(&viewports, viewport);
				}
				assert!(Instant::now() < deadline, "the caret never moved");
			}
		};
	let left = step(
		&engine,
		EditorCommand::MoveCursor(MoveDirection::Left),
		&|pane| pane.cursor_abs_pos.col == origin,
	);
	assert_eq!(focused(&left).cursor_abs_byte.get(), origin.get());
	let word = step(
		&engine,
		EditorCommand::MoveCursor(MoveDirection::NextWord),
		&|pane| pane.cursor_abs_byte.get() != origin.get(),
	);
	let moved = focused(&word).cursor_abs_byte.get() - origin.get();
	assert!(
		moved > 0 && moved <= 1 << 20,
		"a word motion reads one window: moved {moved}"
	);
	assert_eq!(
		focused(&word).cursor_abs_pos.col.get(),
		focused(&word).cursor_abs_byte.get()
	);
	let home = step(&engine, EditorCommand::LineStart, &|pane| {
		pane.cursor_abs_byte.get() == 0
	});
	assert_eq!(focused(&home).cursor_abs_pos.col.get(), 0);

	engine.shutdown().expect("join engine");
	let _ = fs::remove_dir_all(root);
}
