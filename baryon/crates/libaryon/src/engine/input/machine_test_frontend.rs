use super::keymap::InputState;
use super::machine::{InputContext, InputRuntime};
use crate::core::{DocByte, DocLine};
use crate::engine::{EditorCommand, EditorMode, KeyCode, Modifiers};
use crate::uast::projection::Viewport;
use std::ops::{Deref, DerefMut};
use std::sync::mpsc;

pub(super) struct TestFrontend {
	runtime: InputRuntime,
	tx_cmd: mpsc::Sender<EditorCommand>,
	pub(super) current_viewport: Option<Viewport>,
}

impl TestFrontend {
	pub(super) fn new() -> (Self, mpsc::Receiver<EditorCommand>) {
		let (tx_cmd, rx_cmd) = mpsc::channel();
		(
			Self {
				runtime: InputRuntime::new(),
				tx_cmd,
				current_viewport: None,
			},
			rx_cmd,
		)
	}

	pub(super) fn editor_mode(&self) -> EditorMode {
		self.runtime.editor_mode()
	}

	pub(super) fn set_editor_mode_for_test(&mut self, mode: EditorMode) {
		self.runtime.state = InputState::from_editor_mode(mode);
	}

	pub(super) fn apply_mode_override(&mut self, mode: EditorMode) {
		self.runtime.apply_mode_override(mode);
	}

	pub(super) fn dispatch_key(
		&mut self,
		code: KeyCode,
		modifiers: Modifiers,
		should_quit: &mut bool,
	) -> bool {
		let context = self.context();
		let dispatch = self.runtime.dispatch_key(context, code, modifiers);
		if dispatch.clear_status {
			self.runtime.status_message = None;
		}
		if dispatch.status_message.is_some() {
			self.runtime.status_message = dispatch.status_message;
		}
		let mut quit = false;
		for command in dispatch.commands {
			quit |= matches!(command, EditorCommand::Quit);
			self.tx_cmd.send(command).expect("test command receiver");
		}
		*should_quit |= quit;
		quit
	}

	fn context(&self) -> InputContext {
		let Some(viewport) = self.current_viewport.as_ref() else {
			return InputContext::default();
		};
		let Some(pane) = viewport.pane(viewport.focused_pane_id) else {
			return InputContext::default();
		};
		InputContext {
			is_file_browser: pane.is_file_browser,
			cursor_available: true,
			viewport_line_count: pane.viewport_line_count,
			viewport_start_line: pane.viewport_start_line,
			cursor_abs_byte: pane.cursor_abs_byte,
			cursor_line_start_byte: pane.cursor_line_start_byte,
			cursor_line: pane.cursor_abs_pos.line,
			cursor_count: pane.cursor_count,
			active_search: viewport.search_pattern.is_some(),
		}
	}
}

impl Deref for TestFrontend {
	type Target = InputRuntime;

	fn deref(&self) -> &Self::Target {
		&self.runtime
	}
}

impl DerefMut for TestFrontend {
	fn deref_mut(&mut self) -> &mut Self::Target {
		&mut self.runtime
	}
}

#[test]
fn default_context_is_stable_for_headless_resolution() {
	let (frontend, _commands) = TestFrontend::new();
	let context = frontend.context();
	assert!(!context.cursor_available);
	assert_eq!(context.cursor_abs_byte, DocByte::ZERO);
	assert_eq!(context.cursor_line, DocLine::ZERO);
}
