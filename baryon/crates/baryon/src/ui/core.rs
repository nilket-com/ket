use super::markdown_preview::MarkdownPreviewController;
use super::minimap::MinimapController;
use crossterm::cursor::SetCursorStyle;
use crossterm::event::{self, Event, KeyEventKind};
use crossterm::execute;
use libaryon::core::{DocLine, PaneId};
use libaryon::engine::{EditorCommand, EditorMode, InputEvent, InputSender, KeyCode, Modifiers};
use libaryon::uast::projection::Viewport;
use ratatui::{Terminal, backend::Backend};
use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::io;
use std::sync::mpsc;
use std::time::{Duration, Instant};

const BUSY_SPINNER_INTERVAL: Duration = Duration::from_millis(80);

fn poll_engine_item<T>(receiver: &mpsc::Receiver<T>, disconnected: &Cell<bool>) -> Option<T> {
	match receiver.try_recv() {
		Ok(item) => Some(item),
		Err(mpsc::TryRecvError::Empty) => None,
		Err(mpsc::TryRecvError::Disconnected) => {
			disconnected.set(true);
			None
		}
	}
}

pub trait CommandSink: Send {
	fn try_send_command(
		&self,
		command: EditorCommand,
	) -> Result<(), mpsc::TrySendError<EditorCommand>>;
}

impl CommandSink for mpsc::Sender<EditorCommand> {
	fn try_send_command(
		&self,
		command: EditorCommand,
	) -> Result<(), mpsc::TrySendError<EditorCommand>> {
		self.send(command)
			.map_err(|mpsc::SendError(command)| mpsc::TrySendError::Disconnected(command))
	}
}

impl CommandSink for libaryon::engine::CommandSender {
	fn try_send_command(
		&self,
		command: EditorCommand,
	) -> Result<(), mpsc::TrySendError<EditorCommand>> {
		self.try_send(command)
	}
}

enum PendingEngineRequest {
	Command(EditorCommand),
	Input(InputEvent),
}

pub struct Frontend<B: Backend + io::Write> {
	pub(super) terminal: Terminal<B>,
	pub(super) tx_cmd: Box<dyn CommandSink>,
	pub(super) tx_input: InputSender,
	pending_requests: RefCell<VecDeque<PendingEngineRequest>>,
	engine_disconnected: Cell<bool>,
	pub(super) rx_view: mpsc::Receiver<Viewport>,
	pub(super) current_viewport: Option<Viewport>,
	pub(super) status_message: Option<String>,
	pub(super) needs_redraw: bool,
	pub(super) busy_spinner_frame: usize,
	pub(super) busy_spinner_last_tick: Instant,
	pub(super) last_browser_click: Option<(PaneId, DocLine, Instant)>,
	pub(super) pane_resize_drag: Option<super::mouse::PaneResizeDrag>,
	pub(super) minimap: MinimapController,
	pub(super) markdown_preview: MarkdownPreviewController,
	pub(super) diff_bridge: super::diff::DiffBridgeController,
	last_git_refresh: Instant,
}

impl<B: Backend + io::Write> Frontend<B> {
	pub fn new(
		terminal: Terminal<B>,
		tx_cmd: impl CommandSink + 'static,
		tx_input: InputSender,
		rx_view: mpsc::Receiver<Viewport>,
	) -> Self {
		Self {
			terminal,
			tx_cmd: Box::new(tx_cmd),
			tx_input,
			pending_requests: RefCell::new(VecDeque::new()),
			engine_disconnected: Cell::new(false),
			rx_view,
			current_viewport: None,
			status_message: None,
			needs_redraw: false,
			busy_spinner_frame: 0,
			busy_spinner_last_tick: Instant::now(),
			last_browser_click: None,
			pane_resize_drag: None,
			minimap: MinimapController::new(),
			markdown_preview: MarkdownPreviewController::new(),
			diff_bridge: super::diff::DiffBridgeController::new(),
			last_git_refresh: Instant::now(),
		}
	}

	pub fn run(&mut self) -> Result<(), io::Error> {
		self.terminal
			.clear()
			.map_err(|e| io::Error::other(e.to_string()))?;
		let size = self
			.terminal
			.size()
			.map_err(|e| io::Error::other(e.to_string()))?;
		self.send_command(EditorCommand::Resize(size.width, size.height));
		self.apply_cursor_style();
		let mut initial_draw = true;
		let mut last_reload_check = Instant::now();
		loop {
			let mut got_new_view = initial_draw;
			initial_draw = false;
			let mut should_quit = self.engine_disconnected.get();
			got_new_view |= self.minimap.poll();
			got_new_view |= self.markdown_preview.poll();
			got_new_view |= self.diff_bridge.poll();

			while let Some(view) = poll_engine_item(&self.rx_view, &self.engine_disconnected) {
				let completion_finished = self.current_viewport.as_ref().is_some_and(|current| {
					current.input.completion == libaryon::engine::InputCompletionStatus::Pending
						&& view.input.completion == libaryon::engine::InputCompletionStatus::Idle
				});
				let was_busy = self
					.current_viewport
					.as_ref()
					.is_some_and(|current| current.busy);
				if view.should_quit {
					should_quit = true;
				}
				if view.busy && !was_busy {
					self.status_message = None;
					self.busy_spinner_frame = 0;
					self.busy_spinner_last_tick = Instant::now();
				}
				if let Some(msg) = view.status_message.clone() {
					self.status_message = Some(msg);
				} else if completion_finished {
					self.status_message = None;
				}
				let mode_changed = self
					.current_viewport
					.as_ref()
					.is_none_or(|current| current.input.mode != view.input.mode);
				self.current_viewport = Some(view);
				if mode_changed {
					self.apply_cursor_style();
				}
				got_new_view = true;
			}
			if should_quit || self.engine_disconnected.get() {
				break;
			}

			if self.current_viewport.as_ref().is_some_and(|view| view.busy)
				&& self.busy_spinner_last_tick.elapsed() >= BUSY_SPINNER_INTERVAL
			{
				let elapsed = self.busy_spinner_last_tick.elapsed();
				let steps = (elapsed.as_millis() / BUSY_SPINNER_INTERVAL.as_millis()) as usize;
				self.busy_spinner_frame = self.busy_spinner_frame.wrapping_add(steps);
				self.busy_spinner_last_tick = Instant::now();
				got_new_view = true;
			}

			if got_new_view || self.needs_redraw {
				self.needs_redraw = false;
				self.draw().map_err(|e| io::Error::other(e.to_string()))?;
			}
			self.flush_pending_requests();
			if !self.has_pending_requests()
				&& last_reload_check.elapsed() >= Duration::from_millis(1500)
			{
				self.send_command(EditorCommand::CheckFileReload);
				last_reload_check = Instant::now();
			}
			if !self.has_pending_requests()
				&& self
					.current_viewport
					.as_ref()
					.is_some_and(|view| view.panes.iter().any(|pane| pane.is_git_status_panel))
				&& self.current_viewport.as_ref().is_some_and(|view| {
					view.git_refresh_seconds > 0
						&& self.last_git_refresh.elapsed()
							>= Duration::from_secs(view.git_refresh_seconds)
				}) {
				self.send_command(EditorCommand::GitStatus);
				self.last_git_refresh = Instant::now();
			}
			if !self.has_pending_requests() && event::poll(Duration::from_millis(16))? {
				loop {
					match event::read()? {
						Event::Key(key) if key.kind == KeyEventKind::Press => {
							self.status_message = None;
							self.needs_redraw = true;
							let (code, modifiers) = portable_key(key.code, key.modifiers);
							self.queue_input(InputEvent::Key { code, modifiers });
						}
						Event::Mouse(mouse) => {
							self.handle_mouse(mouse);
						}
						Event::Resize(w, h) => {
							self.send_command(EditorCommand::Resize(w, h));
							self.needs_redraw = true;
						}
						Event::Paste(data) => {
							self.queue_input(InputEvent::Paste(data));
						}
						_ => {}
					}
					if should_quit
						|| self.has_pending_requests()
						|| !event::poll(Duration::from_millis(0))?
					{
						break;
					}
				}
			}

			if should_quit || self.engine_disconnected.get() {
				break;
			}
		}
		Ok(())
	}

	pub(super) fn editor_mode(&self) -> EditorMode {
		self.current_viewport
			.as_ref()
			.map(|view| view.input.mode.clone())
			.unwrap_or(EditorMode::Normal)
	}

	pub(super) fn send_command(&self, command: EditorCommand) {
		self.queue_request(PendingEngineRequest::Command(command));
	}

	fn queue_input(&self, input: InputEvent) {
		self.queue_request(PendingEngineRequest::Input(input));
	}

	fn queue_request(&self, request: PendingEngineRequest) {
		if !self.has_pending_requests() {
			match self.try_send_request(request) {
				Ok(()) => return,
				Err(mpsc::TrySendError::Full(request)) => {
					self.pending_requests.borrow_mut().push_back(request);
					return;
				}
				Err(mpsc::TrySendError::Disconnected(_)) => {
					self.engine_disconnected.set(true);
					return;
				}
			}
		}
		self.pending_requests.borrow_mut().push_back(request);
	}

	fn flush_pending_requests(&self) {
		loop {
			let Some(request) = self.pending_requests.borrow_mut().pop_front() else {
				break;
			};
			match self.try_send_request(request) {
				Ok(()) => {}
				Err(mpsc::TrySendError::Full(request)) => {
					self.pending_requests.borrow_mut().push_front(request);
					break;
				}
				Err(mpsc::TrySendError::Disconnected(_)) => {
					self.pending_requests.borrow_mut().clear();
					self.engine_disconnected.set(true);
					break;
				}
			}
		}
	}

	fn has_pending_requests(&self) -> bool {
		!self.pending_requests.borrow().is_empty()
	}

	fn try_send_request(
		&self,
		request: PendingEngineRequest,
	) -> Result<(), mpsc::TrySendError<PendingEngineRequest>> {
		match request {
			PendingEngineRequest::Command(command) => self
				.tx_cmd
				.try_send_command(command)
				.map_err(|error| match error {
					mpsc::TrySendError::Full(command) => {
						mpsc::TrySendError::Full(PendingEngineRequest::Command(command))
					}
					mpsc::TrySendError::Disconnected(command) => {
						mpsc::TrySendError::Disconnected(PendingEngineRequest::Command(command))
					}
				}),
			PendingEngineRequest::Input(input) => {
				self.tx_input.try_send(input).map_err(|error| match error {
					mpsc::TrySendError::Full(input) => {
						mpsc::TrySendError::Full(PendingEngineRequest::Input(input))
					}
					mpsc::TrySendError::Disconnected(input) => {
						mpsc::TrySendError::Disconnected(PendingEngineRequest::Input(input))
					}
				})
			}
		}
	}

	pub(super) fn apply_cursor_style(&mut self) {
		let style = match self.editor_mode() {
			EditorMode::Normal
			| EditorMode::Command
			| EditorMode::Search
			| EditorMode::Password
			| EditorMode::Confirm
			| EditorMode::Visual { .. } => SetCursorStyle::SteadyBlock,
			EditorMode::Insert => SetCursorStyle::SteadyBar,
		};
		let _ = execute!(self.terminal.backend_mut(), style);
	}

	pub fn release_terminal(self) -> Terminal<B> {
		self.terminal
	}
}

fn portable_key(code: event::KeyCode, modifiers: event::KeyModifiers) -> (KeyCode, Modifiers) {
	let mut portable_modifiers = Modifiers::NONE;
	for (source, target) in [
		(event::KeyModifiers::SHIFT, Modifiers::SHIFT),
		(event::KeyModifiers::CONTROL, Modifiers::CONTROL),
		(event::KeyModifiers::ALT, Modifiers::ALT),
		(event::KeyModifiers::SUPER, Modifiers::SUPER),
		(event::KeyModifiers::HYPER, Modifiers::HYPER),
		(event::KeyModifiers::META, Modifiers::META),
	] {
		if modifiers.contains(source) {
			portable_modifiers.insert(target);
		}
	}
	let code = match code {
		event::KeyCode::Backspace => KeyCode::Backspace,
		event::KeyCode::Enter => KeyCode::Enter,
		event::KeyCode::Left => KeyCode::Left,
		event::KeyCode::Right => KeyCode::Right,
		event::KeyCode::Up => KeyCode::Up,
		event::KeyCode::Down => KeyCode::Down,
		event::KeyCode::Home => KeyCode::Home,
		event::KeyCode::End => KeyCode::End,
		event::KeyCode::PageUp => KeyCode::PageUp,
		event::KeyCode::PageDown => KeyCode::PageDown,
		event::KeyCode::Tab => KeyCode::Tab,
		event::KeyCode::BackTab => {
			portable_modifiers.insert(Modifiers::SHIFT);
			KeyCode::Tab
		}
		event::KeyCode::Delete => KeyCode::Delete,
		event::KeyCode::Insert => KeyCode::Insert,
		event::KeyCode::F(number) => KeyCode::Function(number),
		event::KeyCode::Char(ch) => {
			portable_modifiers.remove(Modifiers::SHIFT);
			KeyCode::Char(ch)
		}
		event::KeyCode::Null => KeyCode::Null,
		event::KeyCode::Esc => KeyCode::Esc,
		_ => KeyCode::Unsupported,
	};
	(code, portable_modifiers)
}

#[cfg(test)]
mod receiver_tests {
	use super::poll_engine_item;
	use std::cell::Cell;
	use std::sync::mpsc;

	#[test]
	fn viewport_channel_disconnect_is_observable_without_an_outgoing_request() {
		let (tx, rx) = mpsc::channel();
		let disconnected = Cell::new(false);
		assert_eq!(poll_engine_item(&rx, &disconnected), None);
		assert!(
			!disconnected.get(),
			"an empty live channel remains connected"
		);
		tx.send(7_u8).expect("queue viewport");
		assert_eq!(poll_engine_item(&rx, &disconnected), Some(7));
		drop(tx);
		assert_eq!(poll_engine_item(&rx, &disconnected), None);
		assert!(disconnected.get(), "channel closure must stop the TUI loop");
	}
}

#[cfg(test)]
mod portable_key_tests {
	use super::*;

	#[test]
	fn character_shift_is_normalized_into_the_character() {
		let (code, modifiers) = portable_key(
			event::KeyCode::Char('A'),
			event::KeyModifiers::SHIFT | event::KeyModifiers::CONTROL,
		);
		assert_eq!(code, KeyCode::Char('A'));
		assert_eq!(modifiers, Modifiers::CONTROL);
	}

	#[test]
	fn backtab_normalizes_to_shift_tab() {
		let (code, modifiers) = portable_key(event::KeyCode::BackTab, event::KeyModifiers::NONE);
		assert_eq!(code, KeyCode::Tab);
		assert_eq!(modifiers, Modifiers::SHIFT);
	}

	#[test]
	fn special_key_modifier_combinations_are_preserved_exactly() {
		let (code, modifiers) = portable_key(
			event::KeyCode::PageDown,
			event::KeyModifiers::CONTROL | event::KeyModifiers::ALT,
		);
		assert_eq!(code, KeyCode::PageDown);
		assert_eq!(modifiers, Modifiers::CONTROL | Modifiers::ALT);
	}
}
