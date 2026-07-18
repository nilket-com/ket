pub mod command;
pub mod core;
pub mod normal;
pub mod operator;
pub mod visual;

use self::command::handle_command_key;
use self::normal::handle_normal_key;
use self::visual::{handle_visual_key, handle_visual_block_key};

use super::coords::{CharIdx, LineIdx};
use super::core::{CodeEditor, EditorMsg};
use iced::Task;
use iced::keyboard::{self, Key};

pub use self::core::{NormalEdit, VimMode, parse_substitute};

pub struct VimHandler {
	pub mode: VimMode,
	pub(in crate::editor) command: String,
	pub(in crate::editor) count: String,
	pub(in crate::editor) pending_g: bool,
	pub(in crate::editor) pending_op: Option<char>,
	/// Pending block insert: (insert_col, top_line, bottom_line)
	pub(in crate::editor) block_insert: Option<(CharIdx, LineIdx, LineIdx)>,
	/// Pending f/F/t/T: stores which variant is waiting for the target char
	pub(in crate::editor) pending_find: Option<char>,
	/// Last f/F/t/T find, for ; and , repeat
	pub(in crate::editor) last_find: Option<(char, char)>,
	/// Pending z-prefix (zz/zt/zb)
	pub(in crate::editor) pending_z: bool,
	/// Pending i/a text-object prefix inside an operator motion
	pub(in crate::editor) pending_obj_prefix: Option<char>,
	/// Last repeatable normal-mode edit (for `.`)
	pub(in crate::editor) last_edit: Option<NormalEdit>,
	/// Text inserted during the last Insert session (for dot-repeat of change ops)
	pub(in crate::editor) last_insert_text: String,
	/// Cursor col when Insert mode was entered (for last_insert_text capture)
	pub(in crate::editor) insert_enter_col: CharIdx,
	/// Cursor line when Insert mode was entered
	pub(in crate::editor) insert_enter_line: LineIdx,
}

impl VimHandler {
	pub fn new() -> Self {
		Self {
			mode: VimMode::Normal,
			command: String::new(),
			count: String::new(),
			pending_g: false,
			pending_op: None,
			block_insert: None,
			pending_find: None,
			last_find: None,
			pending_z: false,
			pending_obj_prefix: None,
			last_edit: None,
			last_insert_text: String::new(),
			insert_enter_col: CharIdx(0),
			insert_enter_line: LineIdx(0),
		}
	}

	pub fn take_count(&mut self) -> usize {
		let n = self.count.parse::<usize>().unwrap_or(1).max(1);
		self.count.clear();
		n
	}

	pub fn handle_key(
		&mut self,
		ed: &mut CodeEditor,
		key: Key,
		mods: keyboard::Modifiers,
		text: Option<String>,
	) -> Task<EditorMsg> {
		match self.mode {
			VimMode::Command => {
				return handle_command_key(self, ed, key, text);
			}
			VimMode::Normal => {
				return handle_normal_key(self, ed, key, mods, text);
			}
			VimMode::Visual | VimMode::VisualLine => {
				return handle_visual_key(self, ed, key, mods, text);
			}
			VimMode::VisualBlock => {
				return handle_visual_block_key(self, ed, key, mods, text);
			}
			VimMode::Insert | VimMode::Off => {}
		}

		// Insert mode (vim enabled): Escape → Normal
		if matches!(&key, Key::Named(keyboard::key::Named::Escape))
			&& self.mode == VimMode::Insert
			&& !ed.buffer.session.search.is_open
		{
			let col_before = ed.buffer.session.selection.head.col;
			let line_before = ed.buffer.session.selection.head.line;
			// Capture inserted text for dot-repeat
			if line_before == self.insert_enter_line && col_before > self.insert_enter_col {
				self.last_insert_text = ed
					.buffer
					.line_text(line_before)
					.chars()
					.skip(*self.insert_enter_col)
					.take(*col_before - *self.insert_enter_col)
					.collect();
			}
			self.mode = VimMode::Normal;
			if *col_before > 0 {
				ed.buffer.move_left(false);
			}
			if let Some((insert_col, top_line, bottom_line)) = self.block_insert.take() {
				if col_before > insert_col && line_before == top_line {
					let inserted: String = ed
						.buffer
						.line_text(top_line)
						.chars()
						.skip(*insert_col)
						.take(*col_before - *insert_col)
						.collect();
					if !inserted.is_empty() {
						ed.buffer.block_insert_text(
							top_line,
							bottom_line,
							insert_col,
							&inserted,
						);
					}
				}
				ed.buffer.session.selection =
					super::coords::Selection::caret(super::coords::CursorPos::new(top_line, insert_col));
			}
			ed.update_status();
			return Task::none();
		}

		Task::none()
	}

	pub(in crate::editor) fn enter_insert_mode(&mut self, ed: &mut CodeEditor) {
		self.mode = VimMode::Insert;
		self.insert_enter_col = ed.buffer.session.selection.head.col;
		self.insert_enter_line = ed.buffer.session.selection.head.line;
		self.last_insert_text.clear();
		ed.update_status();
	}

	pub(in crate::editor) fn do_find(
		&mut self,
		ed: &mut CodeEditor,
		kind: char,
		target: char,
		count: usize,
		extend: bool,
	) {
		for _ in 0..count {
			match kind {
				'f' => ed.buffer.move_to_char(target, false, extend),
				'F' => ed.buffer.move_to_char_back(target, false, extend),
				't' => ed.buffer.move_to_char(target, true, extend),
				'T' => ed.buffer.move_to_char_back(target, true, extend),
				_ => {}
			}
		}
	}

	pub(in crate::editor) fn scroll_cursor_z(&mut self, ed: &mut CodeEditor, mode: char) {
		let vl_idx = ed.cursor_visual_line_idx();
		let cy = vl_idx as f32 * super::widget::line_height();
		let lh = super::widget::line_height();
		ed.view.scroll_y = match mode {
			'z' => (cy - ed.view.viewport_h / 2.0 + lh / 2.0).max(0.0),
			't' => cy,
			'b' => (cy - ed.view.viewport_h + lh).max(0.0),
			_ => ed.view.scroll_y,
		};
	}

	pub(in crate::editor) fn select_text_object(
		&self,
		ed: &mut CodeEditor,
		obj: &str,
		_count: usize,
	) -> bool {
		let origin = ed.buffer.session.selection.head;
		match obj {
			"iw" => {
				let lt = ed.buffer.line_text(origin.line);
				let chars: Vec<char> = lt.chars().collect();
				if chars.is_empty() {
					return false;
				}
				let is_w = |c: char| c.is_alphanumeric() || c == '_';
				let col = (*origin.col).min(chars.len().saturating_sub(1));
				let mut s = col;
				while s > 0 && is_w(chars[s - 1]) {
					s -= 1;
				}
				let mut e = col;
				if e < chars.len() && is_w(chars[e]) {
					while e < chars.len() && is_w(chars[e]) {
						e += 1;
					}
				} else {
					e += 1; // fallback
				}
				ed.buffer.session.selection = super::coords::Selection {
					anchor: super::coords::CursorPos::new(origin.line, CharIdx(s)),
					head: super::coords::CursorPos::new(origin.line, CharIdx(e)),
				};
				true
			}
			"aw" => {
				let lt = ed.buffer.line_text(origin.line);
				let chars: Vec<char> = lt.chars().collect();
				if chars.is_empty() {
					return false;
				}
				let is_w = |c: char| c.is_alphanumeric() || c == '_';
				let col = (*origin.col).min(chars.len().saturating_sub(1));
				let mut s = col;
				while s > 0 && is_w(chars[s - 1]) {
					s -= 1;
				}
				let mut e = col;
				if e < chars.len() && is_w(chars[e]) {
					while e < chars.len() && is_w(chars[e]) {
						e += 1;
					}
				} else {
					e += 1;
				}
				let pre_ws = e;
				while e < chars.len() && chars[e].is_whitespace() {
					e += 1;
				}
				if e == pre_ws && s > 0 {
					while s > 0 && chars[s - 1].is_whitespace() {
						s -= 1;
					}
				}
				ed.buffer.session.selection = super::coords::Selection {
					anchor: super::coords::CursorPos::new(origin.line, CharIdx(s)),
					head: super::coords::CursorPos::new(origin.line, CharIdx(e)),
				};
				true
			}
			_ => false,
		}
	}

	pub(in crate::editor) fn exec_operator_motion(
		&mut self,
		ed: &mut CodeEditor,
		op: char,
		motion: &str,
		count: usize,
	) -> Task<EditorMsg> {
		self::operator::exec_operator_motion(self, ed, op, motion, count)
	}

	pub(in crate::editor) fn replay_edit(
		&mut self,
		ed: &mut CodeEditor,
		edit: NormalEdit,
	) -> Task<EditorMsg> {
		self::operator::replay_edit(self, ed, edit)
	}
}
