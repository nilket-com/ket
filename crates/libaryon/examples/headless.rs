use libaryon::engine::{EditorCommand, EngineConfig, EngineHandle};
use libaryon::uast::Viewport;
use std::sync::mpsc::Receiver;
use std::time::{Duration, Instant};

fn projected_text(viewport: &Viewport) -> String {
	let bytes: Vec<u8> = viewport
		.focused_pane()
		.into_iter()
		.flat_map(|pane| &pane.tokens)
		.flat_map(|token| token.text.iter().copied())
		.collect();
	String::from_utf8_lossy(&bytes).into_owned()
}

fn receive_until(
	viewports: &Receiver<Viewport>,
	mut predicate: impl FnMut(&Viewport) -> bool,
) -> Viewport {
	let deadline = Instant::now() + Duration::from_secs(10);
	loop {
		let viewport = viewports
			.recv_timeout(deadline.saturating_duration_since(Instant::now()))
			.expect("engine stopped before producing the requested viewport");
		if predicate(&viewport) {
			return viewport;
		}
	}
}

fn main() {
	let initial_file = std::env::args()
		.nth(1)
		.unwrap_or_else(|| "README.md".to_string());
	let mut engine = EngineHandle::spawn(EngineConfig {
		initial_file: Some(initial_file),
		..EngineConfig::default()
	});
	let commands = engine.command_sender();
	let viewports = engine.take_viewports().expect("viewport receiver");

	commands
		.send(EditorCommand::Resize(100, 30))
		.expect("resize engine");
	receive_until(&viewports, |viewport| !projected_text(viewport).is_empty());

	let inserted = "[headless edit] ";
	commands
		.send(EditorCommand::InsertText(inserted.to_string()))
		.expect("edit document");
	receive_until(&viewports, |viewport| {
		projected_text(viewport).contains(inserted)
	});

	commands
		.send(EditorCommand::SearchStart("headless".to_string()))
		.expect("search document");
	let searched = receive_until(&viewports, |viewport| {
		viewport.search_pattern.as_deref() == Some("headless")
	});
	print!("{}", projected_text(&searched));

	engine.shutdown().expect("join engine");
}
