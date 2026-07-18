//! Thin Iced host for the headless libaryon engine.
//!
//! This deliberately owns no document state. The engine is authoritative; this
//! module only forwards portable input, coalesces projected viewports, and
//! renders the focused pane's currently visible tokens. Rich visual layers are
//! added after this minimal bridge is proven in Polariton.

use iced::keyboard::{self, Key};
use iced::widget::{container, scrollable, text};
use iced::{Color, Element, Event, Length, Subscription, event};
use libaryon::engine::{
	EditorCommand, EngineConfig, EngineHandle, InputEvent, KeyCode, Modifiers,
};
use libaryon::uast::Viewport;
use std::sync::mpsc::{Receiver, TryRecvError};

const INITIAL_COLUMNS: u16 = 120;
const INITIAL_ROWS: u16 = 40;

#[derive(Debug, Clone)]
pub enum EditorMsg {
	Input(InputEvent),
}

/// Long-lived owner of Polariton's libaryon session.
pub struct BaryonEditor {
	engine: Option<EngineHandle>,
	commands: libaryon::engine::CommandSender,
	input: libaryon::engine::InputSender,
	viewports: Receiver<Viewport>,
	viewport: Option<Viewport>,
	document_text: String,
	text_query_pending: bool,
	/// Input was forwarded after the pending text query was issued, so the
	/// in-flight reply is already stale and another query must follow it.
	content_dirty: bool,
}

impl BaryonEditor {
	pub fn new(text: &str) -> Self {
		let mut engine = EngineHandle::spawn(EngineConfig::default());
		let commands = engine.command_sender();
		let input = engine.input_sender();
		let viewports = engine.take_viewports().expect("new engine owns viewport receiver");
		let mut editor = Self {
			engine: Some(engine),
			commands,
			input,
			viewports,
			viewport: None,
			document_text: String::new(),
			text_query_pending: false,
			content_dirty: false,
		};
		editor.send(EditorCommand::Resize(INITIAL_COLUMNS, INITIAL_ROWS));
		editor.send(EditorCommand::ReplaceDocument {
			text: text.to_owned(),
			language: Some("sql".to_owned()),
		});
		editor.request_content();
		editor
	}

	/// Last complete document text received from the engine.
	pub fn cached_content(&self) -> &str {
		&self.document_text
	}

	/// Request a fresh document snapshot. The next projected `document_text`
	/// updates [`Self::cached_content`]. Calls coalesce while a request is
	/// live, but a request issued while one is pending marks the reply stale so
	/// [`Self::poll`] immediately re-queries — the cache always converges on
	/// the state after the last forwarded input.
	pub fn request_content(&mut self) {
		if self.text_query_pending {
			self.content_dirty = true;
		} else if self.send(EditorCommand::QueryDocumentText) {
			self.text_query_pending = true;
			self.content_dirty = false;
		} else {
			// The query never left; retry from the next request or reply.
			self.content_dirty = true;
		}
	}

	pub fn replace_content(&mut self, text: &str) {
		// Optimistic: the engine confirms via the queued query, but Run/Save
		// must never observe a cleared document in the interim.
		self.document_text = text.to_owned();
		self.send(EditorCommand::ReplaceDocument {
			text: text.to_owned(),
			language: Some("sql".to_owned()),
		});
		self.request_content();
	}

	/// Translate the available Iced window size into the engine's cell grid.
	/// Pane-accurate sizing follows with the custom renderer in the next layer.
	pub fn resize(&self, width: f32, height: f32) {
		let columns = (width / 9.6).floor().clamp(1.0, u16::MAX as f32) as u16;
		let rows = (height / 22.0).floor().clamp(1.0, u16::MAX as f32) as u16;
		self.send(EditorCommand::Resize(columns, rows));
	}

	pub fn subscription(&self) -> Subscription<EditorMsg> {
		event::listen_with(|event, status, _window| match (event, status) {
			// Only keys no other widget consumed: a captured key was typed
			// into a text_input (connection name, statement title, …) and must
			// not also edit the SQL document.
			(
				Event::Keyboard(keyboard::Event::KeyPressed { key, modifiers, .. }),
				event::Status::Ignored,
			) => key_code(&key).map(|code| {
				EditorMsg::Input(InputEvent::Key {
					code,
					modifiers: map_modifiers(modifiers),
				})
			}),
			_ => None,
		})
	}

	/// Drain every available projection and retain only the latest one.
	/// This is deliberately called from Iced's update path, never blocking it.
	pub fn poll(&mut self) -> bool {
		let mut changed = false;
		loop {
			match self.viewports.try_recv() {
				Ok(viewport) => {
					if let Some(document_text) = viewport.document_text.as_ref() {
						self.document_text = document_text.clone();
						self.text_query_pending = false;
						if self.content_dirty {
							// Input landed after this query was issued; chase
							// it so the cache converges on the latest state.
							self.request_content();
						}
					}
					self.viewport = Some(viewport);
					changed = true;
				}
				Err(TryRecvError::Empty | TryRecvError::Disconnected) => return changed,
			}
		}
	}

	pub fn update(&mut self, message: EditorMsg) {
		match message {
			EditorMsg::Input(input) => {
				if let Err(error) = self.input.try_send(input) {
					eprintln!("Editor engine rejected input: {error}");
					return;
				}
				self.request_content();
			}
		}
	}

	pub fn view(&self) -> Element<'_, EditorMsg> {
		let content = self
			.viewport
			.as_ref()
			.and_then(Viewport::focused_pane)
			.map(projected_text)
			.unwrap_or_else(|| "Starting editor…".to_owned());
		scrollable(
			container(
				text(content)
					.font(iced::Font::MONOSPACE)
					.size(15)
					.color(Color::from_rgb8(220, 220, 220)),
			)
			.padding(12)
			.width(Length::Fill),
		)
		.width(Length::Fill)
		.height(Length::Fill)
		.into()
	}

	pub fn shutdown(&mut self) {
		if let Some(engine) = self.engine.take() {
			let _ = engine.shutdown();
		}
	}

	fn send(&self, command: EditorCommand) -> bool {
		match self.commands.try_send(command) {
			Ok(()) => true,
			Err(error) => {
				eprintln!("Editor engine rejected command: {error}");
				false
			}
		}
	}
}

impl Drop for BaryonEditor {
	fn drop(&mut self) {
		self.shutdown();
	}
}

fn projected_text(pane: &libaryon::uast::projection::PaneViewport) -> String {
	String::from_utf8_lossy(
		&pane
			.tokens
			.iter()
			.flat_map(|token| token.text.iter().copied())
			.collect::<Vec<_>>(),
	)
	.into_owned()
}

fn map_modifiers(modifiers: keyboard::Modifiers) -> Modifiers {
	let mut mapped = Modifiers::NONE;
	if modifiers.shift() { mapped |= Modifiers::SHIFT; }
	if modifiers.control() { mapped |= Modifiers::CONTROL; }
	if modifiers.alt() { mapped |= Modifiers::ALT; }
	if modifiers.logo() { mapped |= Modifiers::SUPER; }
	mapped
}

fn key_code(key: &Key) -> Option<KeyCode> {
	use iced::keyboard::key::Named;
	Some(match key.as_ref() {
		Key::Character(value) => KeyCode::Char(value.chars().next()?),
		Key::Named(Named::Backspace) => KeyCode::Backspace,
		Key::Named(Named::Enter) => KeyCode::Enter,
		Key::Named(Named::ArrowLeft) => KeyCode::Left,
		Key::Named(Named::ArrowRight) => KeyCode::Right,
		Key::Named(Named::ArrowUp) => KeyCode::Up,
		Key::Named(Named::ArrowDown) => KeyCode::Down,
		Key::Named(Named::Home) => KeyCode::Home,
		Key::Named(Named::End) => KeyCode::End,
		Key::Named(Named::PageUp) => KeyCode::PageUp,
		Key::Named(Named::PageDown) => KeyCode::PageDown,
		Key::Named(Named::Tab) => KeyCode::Tab,
		Key::Named(Named::Delete) => KeyCode::Delete,
		Key::Named(Named::Insert) => KeyCode::Insert,
		Key::Named(Named::Escape) => KeyCode::Esc,
		_ => return None,
	})
}

#[cfg(test)]
mod tests {
	use super::BaryonEditor;
	use std::thread;
	use std::time::{Duration, Instant};

	fn wait_for_content(editor: &mut BaryonEditor, expected: &str) {
		let deadline = Instant::now() + Duration::from_secs(10);
		while editor.cached_content() != expected && Instant::now() < deadline {
			editor.poll();
			thread::sleep(Duration::from_millis(5));
		}
		assert_eq!(editor.cached_content(), expected);
	}

	#[test]
	fn in_memory_content_round_trips_through_the_engine() {
		let mut editor = BaryonEditor::new("select 1;");
		wait_for_content(&mut editor, "select 1;");
		editor.replace_content("select 2;");
		wait_for_content(&mut editor, "select 2;");
	}

	#[test]
	fn rapid_input_still_converges_the_cached_content() {
		use super::EditorMsg;
		use libaryon::engine::{InputEvent, KeyCode, Modifiers};

		let mut editor = BaryonEditor::new("select 1;");
		wait_for_content(&mut editor, "select 1;");
		// Enter insert mode, then type without polling in between: the second
		// key lands while the first key's text query is in flight, so the
		// cache only converges if the stale reply is chased with a re-query.
		for ch in ['i', 'x'] {
			editor.update(EditorMsg::Input(InputEvent::Key {
				code: KeyCode::Char(ch),
				modifiers: Modifiers::NONE,
			}));
		}
		wait_for_content(&mut editor, "xselect 1;");
	}
}
