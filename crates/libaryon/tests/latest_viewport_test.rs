//! Latest-viewport delivery (crates/libaryon/plans/adr/0008-latest-viewport-delivery.md): a reader
//! that stops reading while the engine renders finds one frame waiting, not every frame since.

use libaryon::engine::settings::EngineSettings;
use libaryon::engine::{EditorCommand, EngineConfig, EngineHandle};
use std::fs;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Newline-free and sparse: the background line scan drives frames for its whole length.
const SPARSE_BYTES: u64 = 64 << 20;

#[test]
fn a_stalled_reader_finds_only_the_latest_viewport() {
	let nonce = SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.expect("clock after epoch")
		.as_nanos();
	let root = std::env::temp_dir().join(format!(
		"baryon-latest-viewport-{}-{nonce}",
		std::process::id()
	));
	fs::create_dir_all(&root).expect("create scratch root");
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
	let file = fs::File::create(&sparse).expect("create sparse fixture");
	file.set_len(SPARSE_BYTES).expect("size sparse fixture");
	drop(file);

	let mut engine = EngineHandle::spawn(EngineConfig {
		initial_file: Some(sparse.to_string_lossy().into_owned()),
		persistent_history: false,
		..EngineConfig::from(EngineSettings::default())
	});
	let viewports = engine.take_viewports().expect("viewport receiver");
	engine
		.command_sender()
		.send(EditorCommand::Resize(100, 30))
		.expect("resize engine");
	viewports
		.recv_timeout(Duration::from_secs(30))
		.expect("first viewport");

	// Stall while the scan renders: one frame is waiting, not every frame since.
	std::thread::sleep(Duration::from_secs(2));
	let waiting = viewports.try_iter().count();
	assert_eq!(
		waiting, 1,
		"a stalled reader holds only the latest viewport"
	);

	// A reply queued behind newer snapshots is still delivered.
	engine
		.command_sender()
		.send(EditorCommand::QueryDocumentText)
		.expect("query document text");
	std::thread::sleep(Duration::from_secs(1));
	let frames: Vec<_> = viewports.try_iter().collect();
	assert!(
		frames.iter().any(|frame| frame.document_text.is_some()),
		"the document-text reply survives the stall"
	);
	assert!(
		frames.len() <= 2,
		"the reply and at most one newer snapshot, found {} frames",
		frames.len()
	);

	engine.shutdown().expect("stop engine");
	let _ = fs::remove_dir_all(&root);
}
