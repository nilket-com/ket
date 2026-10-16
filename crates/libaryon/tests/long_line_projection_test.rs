//! Long-line projection (crates/libaryon/plans/adr/0007-long-line-projection.md): an unwrapped pane
//! publishes each line only up to its right edge plus one cell, at its true absolute bytes.

use libaryon::engine::settings::EngineSettings;
use libaryon::engine::{EditorCommand, EngineConfig, EngineHandle};
use libaryon::uast::{PaneViewport, Viewport};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const WIDTH: u16 = 40;
const HEIGHT: u16 = 10;
const DEADLINE: Duration = Duration::from_secs(30);
/// One cold extent (`svp::ingest::COLD_INGEST_EXTENT_SIZE`).
const EXTENT_BYTES: u64 = 1 << 20;
/// Longer than one extent, so the line spans leaves.
const LONG_LINE_BYTES: usize = (1 << 20) + (1 << 19);
/// A newline-free sparse file of 1,024 extents.
const SPARSE_BYTES: u64 = 1 << 30;

fn scratch_root() -> PathBuf {
	let nonce = SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.expect("clock after epoch")
		.as_nanos();
	let root =
		std::env::temp_dir().join(format!("baryon-long-line-{}-{nonce}", std::process::id()));
	fs::create_dir_all(&root).expect("create scratch root");
	root
}

/// Open `file`, resize, and return the first viewport whose focused pane's text satisfies `ready`.
fn first_ready(file: &Path, wrap: bool, ready: impl Fn(&[u8]) -> bool) -> Viewport {
	let mut settings = EngineSettings::default();
	settings.wrap_enabled = wrap;
	let mut engine = EngineHandle::spawn(EngineConfig {
		initial_file: Some(file.to_string_lossy().into_owned()),
		persistent_history: false,
		..EngineConfig::from(settings)
	});
	let viewports = engine.take_viewports().expect("viewport receiver");
	engine
		.command_sender()
		.send(EditorCommand::Resize(WIDTH, HEIGHT))
		.expect("resize engine");
	let deadline = Instant::now() + DEADLINE;
	let ready_viewport = loop {
		let viewport = viewports
			.recv_timeout(deadline.saturating_duration_since(Instant::now()))
			.unwrap_or_else(|_| panic!("{}: no ready viewport in {DEADLINE:?}", file.display()));
		if viewport
			.focused_pane()
			.is_some_and(|pane| ready(&published_text(pane)))
		{
			break viewport;
		}
	};
	engine.shutdown().expect("stop engine");
	ready_viewport
}

fn published_text(pane: &PaneViewport) -> Vec<u8> {
	pane.tokens
		.iter()
		.flat_map(|token| token.text.iter().copied())
		.collect()
}

#[test]
fn unwrapped_long_lines_are_published_to_the_pane_edge() {
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
	let edge = usize::from(WIDTH) + 1;

	// A newline-free file longer than one extent.
	let single = root.join("single.txt");
	let mut text = b"HEADMARK".to_vec();
	text.resize(LONG_LINE_BYTES, b'x');
	fs::write(&single, &text).expect("write single-line fixture");
	let viewport = first_ready(&single, false, |text| text.starts_with(b"HEADMARK"));
	let pane = viewport.focused_pane().expect("focused pane");
	let published = published_text(pane);
	assert!(
		published.len() <= edge,
		"unwrapped pane published {} bytes of a {LONG_LINE_BYTES}-byte line",
		published.len()
	);
	assert!(published.len() > 20, "pane should still show its text");
	assert_eq!(pane.tokens[0].absolute_start_byte.get(), 0);

	// A long line between short ones: the lines after it keep their absolute bytes.
	let mixed = root.join("mixed.txt");
	let mut text = b"first line\n".to_vec();
	let long_start = text.len();
	text.extend(std::iter::repeat_n(b'y', LONG_LINE_BYTES));
	text.extend_from_slice(b"\nTAILMARK after\nlast\n");
	let tail_byte = (long_start + LONG_LINE_BYTES + 1) as u64;
	fs::write(&mixed, &text).expect("write mixed fixture");
	let viewport = first_ready(&mixed, false, |text| {
		text.windows(8).any(|window| window == b"TAILMARK")
	});
	let pane = viewport.focused_pane().expect("focused pane");
	let published = published_text(pane);
	assert!(
		published.len() <= 3 * edge + 16,
		"mixed pane published {} bytes",
		published.len()
	);
	let mut tail_found = false;
	for token in &pane.tokens {
		if let Some(at) = token
			.text
			.windows(8)
			.position(|window| window == b"TAILMARK")
		{
			assert_eq!(token.absolute_start_byte.get() + at as u64, tail_byte);
			tail_found = true;
		}
	}
	assert!(tail_found, "TAILMARK must be published");

	// Wrap on is outside this change: its output is not clipped to one row.
	let viewport = first_ready(&single, true, |text| text.starts_with(b"HEADMARK"));
	let pane = viewport.focused_pane().expect("focused pane");
	assert!(
		published_text(pane).len() > edge,
		"wrap-on output must be left to the wrap path"
	);

	// A newline-free sparse file far larger than one extent: a frame arrives in both wrap
	// modes, and nothing past the line's first extent is discovered.
	let sparse = root.join("sparse.bin");
	let file = fs::File::create(&sparse).expect("create sparse fixture");
	file.set_len(SPARSE_BYTES).expect("size sparse fixture");
	{
		use std::io::Write as _;
		(&file).write_all(b"HEADMARK").expect("write sparse head");
	}
	drop(file);
	for wrap in [false, true] {
		let viewport = first_ready(&sparse, wrap, |text| text.starts_with(b"HEADMARK"));
		let pane = viewport.focused_pane().expect("focused pane");
		assert!(
			pane.tokens
				.iter()
				.all(|token| token.absolute_start_byte.get() < EXTENT_BYTES),
			"wrap={wrap}: a token past the first extent was discovered"
		);
	}
	let _ = fs::remove_dir_all(&root);
}
