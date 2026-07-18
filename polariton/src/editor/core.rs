use iced::keyboard::{self, Key};
use iced::widget::{column, container, row, text};
use iced::{Element, Length, Subscription, Task, Theme, event};

use crate::gui::statusbar::{Segment, StatusBar, StatusBarStyle, Tone};

use super::analysis::{self, AnalysisSnapshot};
use super::buffer::Buffer;
use super::command::EditorCommand;
use super::coords::{CharIdx, CursorPos, LineIdx, line};
use super::highlight::SyntaxLanguage;
use super::theme::EditorTheme;
use super::undo::UndoConfig;
use super::vim::{VimHandler, VimMode};
use super::widget;
use super::widget::{EditorAction, EditorWidget};

// ─── Public message type ──────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum EditorMsg {
	Action(EditorAction),
	#[allow(dead_code)]
	Command(EditorCommand),
	Key(Key, keyboard::Modifiers, Option<String>),
	Scroll(f32, f32),
	MouseMove(iced::Point),
	MouseUp,
	/// Paste system-clipboard text at the current cursor position.
	Paste(String),
	/// Paste system-clipboard text after the current cursor (vim `p` semantics).
	PasteAfter(String),
	/// Replace the current visual selection with system-clipboard text (vim visual `p`).
	VisualPaste(String),
	AnalysisReady(AnalysisSnapshot),
	/// Do nothing (used for Task completion).
	Noop,
}

// ─── CodeEditor ───────────────────────────────────────────────────────────────

/// Self-contained code editor state. Embed in your app's state, drive with
/// `update` / `view` / `subscription`, and map messages to your own type.
pub struct CodeEditor {
	pub buffer: Buffer,
	pub theme: EditorTheme,
	pub view: EditorViewState,
	pub vim: VimHandler,
	pointer: PointerState,
}

pub struct EditorViewState {
	pub scroll_y: f32,
	pub scroll_x: f32,
	pub show_minimap: bool,
	pub show_whitespace: bool,
	pub viewport_w: f32,
	pub viewport_h: f32,
}

struct PointerState {
	is_dragging: bool,
	click_count: u32,
}

fn default_undo_config() -> UndoConfig {
	UndoConfig {
		max_history: 1000,
		group_timeout_ms: 600,
	}
}

#[allow(dead_code)] // public API — used by the consuming application, not the demo
impl CodeEditor {
	/// Create a new editor with the given initial content and syntax language.
	pub fn new(content: &str, language: SyntaxLanguage) -> Self {
		let undo_cfg = default_undo_config();
		let buffer = Buffer::with_undo_config(content, language, undo_cfg);
		let mut ed = Self {
			buffer,
			theme: EditorTheme::dark(),
			view: EditorViewState {
				scroll_y: 0.0,
				scroll_x: 0.0,
				show_minimap: true,
				show_whitespace: true,
				viewport_w: 0.0,
				viewport_h: 0.0,
			},
			vim: VimHandler::new(),
			pointer: PointerState {
				is_dragging: false,
				click_count: 0,
			},
		};
		ed.update_status();
		ed
	}

	/// The current text content of the buffer.
	pub fn content(&self) -> String {
		self.buffer.document.rope.to_string()
	}

	/// Replace the buffer content (resets scroll and undo history).
	pub fn set_content(&mut self, content: &str) {
		let lang = self.buffer.language();
		self.buffer = Buffer::with_undo_config(content, lang, default_undo_config());
		self.view.scroll_y = 0.0;
		self.view.scroll_x = 0.0;
		self.update_status();
	}

	/// Replace content and switch language in one call.
	pub fn set_content_with_language(&mut self, content: &str, language: SyntaxLanguage) {
		self.buffer = Buffer::with_undo_config(content, language, default_undo_config());
		self.view.scroll_y = 0.0;
		self.view.scroll_x = 0.0;
		self.update_status();
	}

	/// Switch the syntax highlighting language (preserves content).
	pub fn set_language(&mut self, lang: SyntaxLanguage) {
		let content = self.content();
		self.set_content_with_language(&content, lang);
	}

	/// Enable or disable vim modal editing. When disabled the editor behaves
	/// like a conventional text editor (always in "insert" mode).
	pub fn set_vim_enabled(&mut self, enabled: bool) {
		self.vim.mode = if enabled {
			VimMode::Normal
		} else {
			VimMode::Off
		};
		self.update_status();
	}

	/// Returns `true` when vim modal editing is active.
	pub fn vim_enabled(&self) -> bool {
		self.vim.mode != VimMode::Off
	}

	/// Swap the active color theme.
	pub fn set_theme(&mut self, theme: EditorTheme) {
		self.theme = theme;
	}

	/// Notify the editor of its viewport size (pixels). Call whenever the
	/// containing pane is resized so cursor-scroll math stays accurate.
	pub fn set_viewport(&mut self, w: f32, h: f32) {
		self.view.viewport_w = w;
		self.view.viewport_h = h;
		if self.buffer.document.wrap_config.enabled {
			self.update_wrap_col();
		}
	}

	/// Enable or disable word wrap, computing the column from the current viewport.
	pub fn set_wrap_enabled(&mut self, enabled: bool) {
		self.buffer.set_wrap(enabled);
		if enabled {
			self.update_wrap_col();
		}
		if !enabled {
			// Horizontal scroll is meaningful again when wrap is off.
		}
	}

	/// Recompute the wrap column from the current viewport width and apply it.
	fn update_wrap_col(&mut self) {
		if self.view.viewport_w < 1.0 {
			return;
		}
		let gw = widget::gutter_width(*self.buffer.line_count());
		let mm = if self.view.show_minimap {
			widget::minimap_width()
		} else {
			0.0
		};
		let usable =
			self.view.viewport_w - gw - widget::scrollbar_width() - mm - widget::left_pad();
		let col = ((usable / widget::char_width()) as usize).max(20);
		self.buffer.set_wrap_col(CharIdx(col));
		self.view.scroll_x = 0.0;
	}

	// ─── iced integration ─────────────────────────────────────────────────────

	pub fn subscription(&self) -> Subscription<EditorMsg> {
		let events = event::listen_with(|event, _status, _id| match event {
			iced::Event::Keyboard(keyboard::Event::KeyPressed {
				key,
				modifiers,
				text,
				..
			}) => Some(EditorMsg::Key(key, modifiers, text.map(|t| t.to_string()))),
			iced::Event::Mouse(iced::mouse::Event::WheelScrolled { delta }) => {
				let (dx, dy) = match delta {
					iced::mouse::ScrollDelta::Lines { x, y } => (-x * 40.0, -y * 40.0),
					iced::mouse::ScrollDelta::Pixels { x, y } => (-x, -y),
				};
				Some(EditorMsg::Scroll(dx, dy))
			}
			iced::Event::Mouse(iced::mouse::Event::CursorMoved { position }) => {
				Some(EditorMsg::MouseMove(position))
			}
			iced::Event::Mouse(iced::mouse::Event::ButtonReleased(iced::mouse::Button::Left)) => {
				Some(EditorMsg::MouseUp)
			}
			_ => None,
		});

		let analysis = if self.buffer.analysis_is_current() {
			Subscription::none()
		} else {
			let data = (
				self.buffer.document_version(),
				self.buffer.language(),
				self.buffer.full_text(),
			);
			Subscription::run_with(data, analysis_subscription)
		};

		Subscription::batch([events, analysis])
	}

	pub fn update(&mut self, msg: EditorMsg) -> Task<EditorMsg> {
		match msg {
			EditorMsg::Command(cmd) => {
				return self.execute_command(cmd);
			}
			EditorMsg::AnalysisReady(snapshot) => {
				if self.buffer.apply_analysis(snapshot) {
					self.update_status();
				}
				return Task::none();
			}
			EditorMsg::Action(EditorAction::Resize(w, h)) => {
				return self.execute_command(EditorCommand::SetViewport(w, h));
			}
			EditorMsg::Action(EditorAction::ToggleFold(line)) => {
				return self.execute_command(EditorCommand::ToggleFold(LineIdx(line)));
			}
			EditorMsg::Action(EditorAction::MouseDown(pos)) => {
				let cursor_pos = self.pos_from_pixel(pos);
				self.pointer.is_dragging = true;
				self.pointer.click_count = 1;
				return self.execute_command(EditorCommand::SetCursor(cursor_pos, false));
			}
			EditorMsg::Action(EditorAction::AddCaret(pos)) => {
				let cursor_pos = self.pos_from_pixel(pos);
				return self.execute_command(EditorCommand::AddCursor(cursor_pos));
			}
			EditorMsg::Action(EditorAction::DoubleClick(pos)) => {
				let cursor_pos = self.pos_from_pixel(pos);
				self.pointer.is_dragging = true;
				self.pointer.click_count = 2;
				return self.execute_command(EditorCommand::SelectWordAt(cursor_pos));
			}
			EditorMsg::Action(_) => {}

			EditorMsg::MouseMove(pos) => {
				if self.pointer.is_dragging && self.pointer.click_count == 1 {
					let target = self.pos_from_pixel(pos);
					return self.execute_command(EditorCommand::SetCursor(target, true));
				}
			}
			EditorMsg::MouseUp => {
				self.pointer.is_dragging = false;
			}

			EditorMsg::Paste(text) => {
				if !text.is_empty() {
					self.buffer.session.clipboard = text.clone();
					self.buffer.session.clipboard_is_line = false;
					return self.execute_command(EditorCommand::Paste(text));
				}
			}

			EditorMsg::PasteAfter(text) => {
				if !text.is_empty() {
					self.buffer.session.clipboard = text.clone();
					self.buffer.session.clipboard_is_line = false;
					return self.execute_command(EditorCommand::PasteAfter(text));
				}
			}

			EditorMsg::VisualPaste(yank) => {
				if !self.buffer.session.selection.is_caret() {
					let (s, e) = self.buffer.session.selection.ordered();
					let is_line = self.vim.mode == VimMode::VisualLine;
					let lcount = *e.line - *s.line + 1;
					let replaced = if is_line {
						let t = self.buffer.yank_lines(s.line, lcount);
						self.buffer.delete_lines(s.line, lcount);
						t
					} else {
						self.buffer.cut()
					};
					if !yank.is_empty() {
						self.buffer.paste(&yank);
					}
					self.buffer.session.clipboard = yank;
					self.buffer.session.clipboard_is_line = false;
					self.vim.mode = VimMode::Normal;
					self.update_status();
					self.ensure_cursor_visible();
					if !replaced.is_empty() {
						return iced::clipboard::write::<EditorMsg>(replaced).map(|_| EditorMsg::Noop);
					}
				}
			}

			EditorMsg::Key(key, mods, text) => {
				// Ctrl+\ toggles vim on/off from any mode
				if mods.command() {
					if let Key::Character(ref ch) = key {
						if ch.as_str() == "\\" {
							self.set_vim_enabled(self.vim.mode == VimMode::Off);
							return Task::none();
						}
					}
				}

				if self.vim.mode != VimMode::Off {
					let mut vim = std::mem::replace(&mut self.vim, VimHandler::new());
					let task = vim.handle_key(self, key.clone(), mods, text.clone());
					self.vim = vim;
					if !matches!(self.vim.mode, VimMode::Insert | VimMode::Off) {
						return task;
					}
				}

				let shift = mods.shift();
				let ctrl = mods.command();
				let alt = mods.alt();

				if self.buffer.session.search.is_open {
					match key {
						Key::Named(keyboard::key::Named::Escape) => {
							return self.execute_command(EditorCommand::SearchClose);
						}
						Key::Named(keyboard::key::Named::Enter) if ctrl && shift => {
							return self.execute_command(EditorCommand::SearchReplaceAll);
						}
						Key::Named(keyboard::key::Named::Enter) if shift => {
							return self.execute_command(EditorCommand::SearchPrev);
						}
						Key::Named(keyboard::key::Named::Enter) => {
							return self.execute_command(EditorCommand::SearchNext);
						}
						_ => {}
					}
				}

				match key {
					Key::Character(ref ch) if ctrl && alt && ch.eq_ignore_ascii_case("k") => {
						self.buffer.add_caret_above();
					}
					Key::Character(ref ch) if ctrl && alt && ch.eq_ignore_ascii_case("j") => {
						self.buffer.add_caret_below();
					}
					Key::Character(ref ch) if ctrl && ch.as_str() == "f" => {
						return self.execute_command(EditorCommand::SearchOpen);
					}
					Key::Character(ref ch) if ctrl && shift && ch.as_str() == "h" => {
						return self.execute_command(EditorCommand::SearchReplaceCurrent);
					}
					Key::Character(ref ch) if ctrl && shift && ch.as_str() == "[" => {
						let l = self.buffer.session.selection.head.line;
						return self.execute_command(EditorCommand::ToggleFold(l));
					}
					Key::Character(ref ch) if ctrl && shift && ch.as_str() == "]" => {
						let l = self.buffer.session.selection.head.line;
						return self.execute_command(EditorCommand::ToggleFold(l));
					}
					Key::Character(ref ch) if ctrl && ch.as_str() == "w" => {
						let enabled = !self.buffer.document.wrap_config.enabled;
						return self.execute_command(EditorCommand::SetWrap(enabled));
					}
					Key::Character(ref ch) if ctrl && ch.as_str() == "m" => {
						self.view.show_minimap = !self.view.show_minimap;
					}
					Key::Character(ref ch) if ctrl && ch.as_str() == "l" => {
						self.view.show_whitespace = !self.view.show_whitespace;
					}
					Key::Named(keyboard::key::Named::ArrowLeft) if ctrl => {
						return self.execute_command(EditorCommand::MoveWordBackward(1, shift));
					}
					Key::Named(keyboard::key::Named::ArrowRight) if ctrl => {
						return self.execute_command(EditorCommand::MoveWordForward(1, shift));
					}
					Key::Named(keyboard::key::Named::ArrowLeft) => {
						return self.execute_command(EditorCommand::MoveLeft(1, shift));
					}
					Key::Named(keyboard::key::Named::ArrowRight) => {
						return self.execute_command(EditorCommand::MoveRight(1, shift));
					}
					Key::Named(keyboard::key::Named::ArrowUp) => {
						return self.execute_command(EditorCommand::MoveUp(1, shift));
					}
					Key::Named(keyboard::key::Named::ArrowDown) => {
						return self.execute_command(EditorCommand::MoveDown(1, shift));
					}
					Key::Named(keyboard::key::Named::Home) if ctrl => {
						return self.execute_command(EditorCommand::MoveToDocStart(shift));
					}
					Key::Named(keyboard::key::Named::End) if ctrl => {
						return self.execute_command(EditorCommand::MoveToDocEnd(shift));
					}
					Key::Named(keyboard::key::Named::Home) => {
						return self.execute_command(EditorCommand::MoveToLineStart(shift));
					}
					Key::Named(keyboard::key::Named::End) => {
						return self.execute_command(EditorCommand::MoveToLineEnd(shift));
					}
					Key::Named(keyboard::key::Named::PageUp) => {
						let v = widget::visible_line_count(self.view.viewport_h);
						self.buffer.page_up(v, shift);
					}
					Key::Named(keyboard::key::Named::PageDown) => {
						let v = widget::visible_line_count(self.view.viewport_h);
						self.buffer.page_down(v, shift);
					}
					Key::Named(keyboard::key::Named::Backspace) => {
						return self.execute_command(EditorCommand::DeleteBack);
					}
					Key::Named(keyboard::key::Named::Delete) => {
						return self.execute_command(EditorCommand::DeleteForward);
					}
					Key::Named(keyboard::key::Named::Enter) => {
						return self.execute_command(EditorCommand::InsertNewline);
					}
					Key::Named(keyboard::key::Named::Tab) if shift => {
						return self.execute_command(EditorCommand::Outdent);
					}
					Key::Named(keyboard::key::Named::Tab) => {
						return self.execute_command(EditorCommand::Indent);
					}
					Key::Character(ref ch) => {
						let s = ch.as_str();
						if ctrl {
							match s {
								"a" => return self.execute_command(EditorCommand::SelectAll),
								"z" if shift => return self.execute_command(EditorCommand::Redo),
								"z" => return self.execute_command(EditorCommand::Undo),
								"y" => return self.execute_command(EditorCommand::Redo),
								"d" => self.buffer.duplicate_line(),
								"c" => return self.execute_command(EditorCommand::Copy),
								"x" => return self.execute_command(EditorCommand::Cut),
								"v" => {
									return iced::clipboard::read()
										.map(|t| EditorMsg::Paste(t.unwrap_or_default()));
								}
								_ => {}
							}
						} else if !ctrl && !alt {
							let insert = text.as_deref().unwrap_or(s);
							return self.execute_command(EditorCommand::Insert(insert.to_string()));
						}
					}
					Key::Named(keyboard::key::Named::Space) if !mods.command() => {
						return self.execute_command(EditorCommand::Insert(" ".to_string()));
					}
					_ => {}
				}
				self.update_status();
				if self.buffer.document.wrap_config.enabled {
					self.update_wrap_col();
				}
				self.ensure_cursor_visible();
			}

			EditorMsg::Scroll(dx, dy) => {
				let sp = if self.buffer.session.search.is_open {
					widget::search_panel_height()
				} else {
					0.0
				};
				let eh = self.view.viewport_h - sp;
				let vl_count = self.buffer.document.visual_lines.len();
				let max_y = (vl_count as f32 * widget::line_height() + widget::top_pad() * 2.0
					- eh)
					.max(0.0);
				self.view.scroll_y = (self.view.scroll_y + dy).clamp(0.0, max_y);
				if !self.buffer.document.wrap_config.enabled {
					self.view.scroll_x = (self.view.scroll_x + dx).max(0.0);
				}
			}
			EditorMsg::Noop => {}
		}
		Task::none()
	}

	pub fn view(&self) -> Element<'_, EditorMsg> {
		let visual_block =
			if self.vim.mode == VimMode::VisualBlock && !self.buffer.session.selection.is_caret() {
				let (s, e) = self.buffer.session.selection.ordered();
				let left_col = self
					.buffer
					.session
					.selection
					.anchor
					.col
					.min(self.buffer.session.selection.head.col);
				let right_col = self
					.buffer
					.session
					.selection
					.anchor
					.col
					.max(self.buffer.session.selection.head.col);
				Some((s.line, e.line, left_col, right_col))
			} else {
				None
			};
		let editor = EditorWidget::new(&self.buffer, &self.theme, EditorMsg::Action)
			.scroll_y(self.view.scroll_y)
			.scroll_x(self.view.scroll_x)
			.show_minimap(self.view.show_minimap)
			.show_whitespace(self.view.show_whitespace)
			.block_cursor(self.vim.mode != VimMode::Insert && self.vim.mode != VimMode::Off)
			.visual_block(visual_block);

		let status_bar = self.status_bar().view();

		let cmd_bar_color = self.theme.cmdbar_text;
		let cmd_bar = container(
			row![
				text(":").size(14).color(cmd_bar_color),
				text(&self.vim.command).size(14).color(cmd_bar_color),
				text("█").size(14).color(iced::Color {
					a: 0.7,
					..cmd_bar_color
				}),
			]
			.padding(iced::Padding {
				top: 4.0,
				bottom: 4.0,
				left: 8.0,
				right: 8.0,
			})
			.spacing(0),
		)
		.style({
			let bg = self.theme.cmdbar_bg;
			move |_: &Theme| container::Style {
				background: Some(iced::Background::Color(bg)),
				..Default::default()
			}
		})
		.width(Length::Fill);

		if self.vim.mode == VimMode::Command {
			column![
				container(Element::from(editor))
					.width(Length::Fill)
					.height(Length::Fill),
				cmd_bar,
				status_bar,
			]
			.into()
		} else {
			column![
				container(Element::from(editor))
					.width(Length::Fill)
					.height(Length::Fill),
				status_bar,
			]
			.into()
		}
	}

	pub fn execute_command(&mut self, cmd: EditorCommand) -> Task<EditorMsg> {
		match cmd {
			EditorCommand::Insert(text) => {
				for ch in text.chars() {
					self.buffer.insert_char_auto_pair(ch);
				}
				self.update_status();
				self.ensure_cursor_visible();
			}
			EditorCommand::DeleteBack => {
				self.buffer.backspace();
				self.update_status();
				self.ensure_cursor_visible();
			}
			EditorCommand::DeleteForward => {
				self.buffer.delete();
				self.update_status();
				self.ensure_cursor_visible();
			}
			EditorCommand::InsertNewline => {
				self.buffer.insert_newline();
				self.update_status();
				self.ensure_cursor_visible();
			}
			EditorCommand::Indent => {
				self.buffer.indent_lines();
				self.update_status();
				self.ensure_cursor_visible();
			}
			EditorCommand::Outdent => {
				self.buffer.dedent_lines();
				self.update_status();
				self.ensure_cursor_visible();
			}
			EditorCommand::ReplaceChar(ch) => {
				self.buffer.replace_char(ch);
				self.update_status();
				self.ensure_cursor_visible();
			}

			EditorCommand::MoveUp(n, extend) => {
				for _ in 0..n {
					self.buffer.move_up(extend);
				}
				self.update_status();
				self.ensure_cursor_visible();
			}
			EditorCommand::MoveDown(n, extend) => {
				for _ in 0..n {
					self.buffer.move_down(extend);
				}
				self.update_status();
				self.ensure_cursor_visible();
			}
			EditorCommand::MoveLeft(n, extend) => {
				for _ in 0..n {
					self.buffer.move_left(extend);
				}
				self.update_status();
				self.ensure_cursor_visible();
			}
			EditorCommand::MoveRight(n, extend) => {
				for _ in 0..n {
					self.buffer.move_right(extend);
				}
				self.update_status();
				self.ensure_cursor_visible();
			}
			EditorCommand::MoveWordForward(n, extend) => {
				for _ in 0..n {
					self.buffer.move_word_right(extend);
				}
				self.update_status();
				self.ensure_cursor_visible();
			}
			EditorCommand::MoveWordBackward(n, extend) => {
				for _ in 0..n {
					self.buffer.move_word_left(extend);
				}
				self.update_status();
				self.ensure_cursor_visible();
			}
			EditorCommand::MoveToLineStart(extend) => {
				self.buffer.move_home(extend);
				self.update_status();
				self.ensure_cursor_visible();
			}
			EditorCommand::MoveToLineEnd(extend) => {
				self.buffer.move_end(extend);
				self.update_status();
				self.ensure_cursor_visible();
			}
			EditorCommand::MoveToDocStart(extend) => {
				self.buffer.move_to_start(extend);
				self.update_status();
				self.ensure_cursor_visible();
			}
			EditorCommand::MoveToDocEnd(extend) => {
				self.buffer.move_to_end(extend);
				self.update_status();
				self.ensure_cursor_visible();
			}
			EditorCommand::SetCursor(pos, extend) => {
				self.buffer.set_head(pos, extend);
				self.update_status();
				self.ensure_cursor_visible();
			}
			EditorCommand::AddCursor(pos) => {
				self.buffer.add_cursor(pos);
				self.update_status();
			}
			EditorCommand::ClearSecondarySelections => {
				self.buffer.clear_secondary_selections();
				self.update_status();
			}
			EditorCommand::SelectWordAt(pos) => {
				self.buffer.select_word_at(pos);
				self.update_status();
			}
			EditorCommand::SelectAll => {
				self.buffer.select_all();
				self.update_status();
			}

			EditorCommand::Cut => {
				let text = self.buffer.cut();
				if !text.is_empty() {
					return iced::clipboard::write::<EditorMsg>(text).map(|_| EditorMsg::Noop);
				}
			}
			EditorCommand::Copy => {
				let text = self.buffer.copy();
				if !text.is_empty() {
					return iced::clipboard::write::<EditorMsg>(text).map(|_| EditorMsg::Noop);
				}
			}
			EditorCommand::Paste(text) => {
				self.buffer.paste(&text);
				self.update_status();
				self.ensure_cursor_visible();
			}
			EditorCommand::PasteAfter(text) => {
				let p = self.buffer.session.selection.head;
				let ll = self.buffer.line_len(p.line);
				if p.col < ll {
					self.buffer.move_right(false);
				}
				self.buffer.paste(&text);
				self.update_status();
				self.ensure_cursor_visible();
			}

			EditorCommand::Undo => {
				self.buffer.undo();
				self.update_status();
				self.ensure_cursor_visible();
			}
			EditorCommand::Redo => {
				self.buffer.redo();
				self.update_status();
				self.ensure_cursor_visible();
			}

			EditorCommand::SetLanguage(lang) => {
				self.set_language(lang);
			}
			EditorCommand::ToggleFold(line) => {
				self.buffer.toggle_fold(line);
				if self.buffer.document.wrap_config.enabled {
					self.update_wrap_col();
				}
				self.update_status();
				self.ensure_cursor_visible();
			}
			EditorCommand::SetWrap(enabled) => {
				self.set_wrap_enabled(enabled);
			}
			EditorCommand::Scroll(dx, dy) => {
				self.view.scroll_x = (self.view.scroll_x + dx).max(0.0);
				self.view.scroll_y = (self.view.scroll_y + dy).max(0.0);
			}
			EditorCommand::SetViewport(w, h) => {
				self.set_viewport(w, h);
			}

			EditorCommand::SearchOpen => {
				self.buffer.search_open();
				self.update_status();
			}
			EditorCommand::SearchClose => {
				self.buffer.search_close();
				self.update_status();
			}
			EditorCommand::SearchNext => {
				self.buffer.search_next();
				self.update_status();
				self.ensure_cursor_visible();
			}
			EditorCommand::SearchPrev => {
				self.buffer.search_prev();
				self.update_status();
				self.ensure_cursor_visible();
			}
			EditorCommand::SearchReplaceCurrent => {
				self.buffer.search_replace_current();
				self.update_status();
			}
			EditorCommand::SearchReplaceAll => {
				self.buffer.search_replace_all();
				self.update_status();
			}

			EditorCommand::VimSetMode(mode) => {
				self.vim.mode = mode;
				self.update_status();
			}
		}
		Task::none()
	}

	// ─── Internal helpers ──────────────────────────────────────────────────────

	fn pos_from_pixel(&self, pixel: iced::Point) -> CursorPos {
		let gw = widget::gutter_width(*self.buffer.line_count());
		let bounds = iced::Rectangle {
			x: 0.0,
			y: 0.0,
			width: self.view.viewport_w,
			height: self.view.viewport_h,
		};
		widget::pixel_to_pos(
			&self.buffer,
			&bounds,
			gw,
			self.view.scroll_x,
			self.view.scroll_y,
			pixel.x,
			pixel.y,
		)
	}

	pub(in crate::editor) fn take_count(&mut self) -> usize {
		self.vim.take_count()
	}

	pub(in crate::editor) fn update_status(&mut self) {
		// Status is derived directly in `status_bar()`. Keep this hook in place so
		// existing editor actions do not need to change in the same cleanup pass.
	}

	fn status_bar(&self) -> StatusBar {
		let p = self.buffer.session.selection.head;
		let diagnostics = self.buffer.document.diagnostics.len();
		let selection = if !self.buffer.session.selection.is_caret() {
			let (s, e) = self.buffer.session.selection.ordered();
			let cs = self.buffer.document.rope.line_to_char(*s.line) + *s.col;
			let ce = self.buffer.document.rope.line_to_char(*e.line) + *e.col;
			Some(format!(
				"{} chars / {} lines",
				ce.saturating_sub(cs),
				*e.line - *s.line + 1
			))
		} else {
			None
		};
		let search = if self.buffer.session.search.is_open {
			Some(format!(
				"{}/{}",
				self.buffer.session.search.current_match + 1,
				self.buffer.session.search.match_count()
			))
		} else {
			None
		};
		let carets = if self.buffer.has_secondary_selections() {
			Some(self.buffer.selection_count().to_string())
		} else {
			None
		};
		let wrap_status = if self.buffer.document.wrap_config.enabled {
			"wrap on"
		} else {
			"wrap off"
		};
		let (mode_label, mode_tone) = match self.vim.mode {
			VimMode::Off => ("vim off", Tone::Warning),
			VimMode::Normal => ("vim normal", Tone::Accent),
			VimMode::Insert => ("vim insert", Tone::Success),
			VimMode::Visual => ("vim visual", Tone::Accent),
			VimMode::VisualLine => ("vim visual-line", Tone::Accent),
			VimMode::VisualBlock => ("vim visual-block", Tone::Accent),
			VimMode::Command => ("vim command", Tone::Accent),
		};

		let mut bar = StatusBar::new()
			.style(self.status_bar_style())
			.inset(0.0, 0.0)
			.lane_split(3, 4)
			.left(Segment::toned_text(mode_label, mode_tone))
			.left(Segment::text(wrap_status))
			.left(Segment::text("UTF-8"))
			.left(Segment::text(self.buffer.language().display_name()))
			.right(
				Segment::text("C-l=ws  C-m=map  C-w=wrap  C-A-j/k=carets  MMB=caret  C-\\=vim")
					.max_chars(40)
					.fill_portion(1),
			)
			.right(
				Segment::label_value(
					"Ln",
					format!("{}, Col {}", *p.line + 1, *p.col + 1),
					Tone::Normal,
				)
				.reserve_chars(14),
			)
			.right(
				Segment::label_value(
					"diag",
					diagnostics.to_string(),
					if diagnostics > 0 {
						Tone::Warning
					} else {
						Tone::Success
					},
				)
				.reserve_chars(8),
			);
		if let Some(selection) = selection {
			bar = bar.right(
				Segment::label_value("sel", selection, Tone::Normal)
					.max_chars(22)
					.reserve_chars(22),
			);
		}
		if let Some(search) = search {
			bar = bar.right(Segment::label_value("search", search, Tone::Accent).reserve_chars(12));
		}
		if let Some(carets) = carets {
			bar = bar.right(Segment::label_value("carets", carets, Tone::Accent).reserve_chars(10));
		}
		bar
	}

	fn status_bar_style(&self) -> StatusBarStyle {
		StatusBarStyle {
			rail_background: self.theme.statusbar_bg,
			rail_separator: self.theme.statusbar_sep,
			segment_background: self.theme.statusbar_bg,
			segment_border: self.theme.statusbar_sep,
			progress_background: self.theme.background,
			progress_bar: self.theme.cursor,
			text_normal: self.theme.statusbar_text,
			text_accent: self.theme.statusbar_text,
			text_success: self.theme.function,
			text_warning: self.theme.number,
			text_danger: self.theme.error_underline,
		}
	}

	pub(in crate::editor) fn cursor_visual_line_idx(&self) -> usize {
		let head = self.buffer.session.selection.head;
		self.buffer
			.document
			.visual_lines
			.iter()
			.position(|vl| {
				vl.doc_line == head.line && vl.col_start <= head.col && head.col <= vl.col_end
			})
			.or_else(|| {
				self.buffer
					.document
					.visual_lines
					.iter()
					.position(|vl| vl.doc_line == head.line)
			})
			.unwrap_or(*head.line)
	}

	pub(in crate::editor) fn ensure_cursor_visible(&mut self) {
		let sp = if self.buffer.session.search.is_open {
			widget::search_panel_height()
		} else {
			0.0
		};
		let vh = self.view.viewport_h - widget::top_pad() * 2.0 - sp;
		if vh < 1.0 {
			return;
		}
		let vl_idx = self.cursor_visual_line_idx();
		let cy = vl_idx as f32 * widget::line_height();
		if cy < self.view.scroll_y {
			self.view.scroll_y = cy;
		} else if cy + widget::line_height() > self.view.scroll_y + vh {
			self.view.scroll_y = cy + widget::line_height() - vh;
		}
		if self.buffer.document.wrap_config.enabled {
			self.view.scroll_x = 0.0;
			return;
		}
		let head = self.buffer.session.selection.head;
		let hlt = self.buffer.line_text(head.line);
		let vcol = line::visual_col_of(&hlt, head.col);
		let char_w = widget::char_width();
		let cx = *vcol as f32 * char_w;
		let gw = widget::gutter_width(*self.buffer.line_count());
		let mm = if self.view.show_minimap {
			widget::minimap_width()
		} else {
			0.0
		};
		let vw = self.view.viewport_w - gw - widget::scrollbar_width() - mm;
		if cx < self.view.scroll_x {
			self.view.scroll_x = cx;
		} else if cx + char_w > self.view.scroll_x + vw {
			self.view.scroll_x = cx + char_w - vw;
		}
	}
}

fn analysis_subscription(
	data: &(u64, SyntaxLanguage, String),
) -> iced::futures::stream::BoxStream<'static, EditorMsg> {
	use iced::futures::StreamExt;
	use iced::futures::stream;

	let (version, language, text) = data.clone();
	stream::once(
		async move { EditorMsg::AnalysisReady(analysis::analyze(version, language, text)) },
	)
	.boxed()
}
