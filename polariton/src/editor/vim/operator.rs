use iced::Task;

use super::{NormalEdit, VimHandler, VimMode};
use super::super::coords::{CharIdx, CursorPos, Selection};
use super::super::core::{CodeEditor, EditorMsg};

// ─── Operator + motion engine ──────────────────────────────────────────────

pub(in crate::editor) fn exec_operator_motion(
	vim: &mut VimHandler,
	ed: &mut CodeEditor,
	op: char,
	motion: &str,
	count: usize,
) -> Task<EditorMsg> {
	let origin = ed.buffer.session.selection.head;
	ed.buffer.session.selection.anchor = origin;

	match motion {
		"h" => {
			for _ in 0..count {
				ed.buffer.move_left(true);
			}
		}
		"j" => {
			for _ in 0..count {
				ed.buffer.move_down(true);
			}
		}
		"k" => {
			for _ in 0..count {
				ed.buffer.move_up(true);
			}
		}
		"l" => {
			for _ in 0..count {
				ed.buffer.move_right(true);
			}
		}
		"w" | "e" => {
			for _ in 0..count {
				ed.buffer.move_word_right(true);
			}
		}
		"b" => {
			for _ in 0..count {
				ed.buffer.move_word_left(true);
			}
		}
		"0" | "^" => ed.buffer.move_home(true),
		"$" => ed.buffer.move_end(true),
		"G" => ed.buffer.move_to_end(true),
		"gg" => ed.buffer.move_to_start(true),
		// ── text objects ────────────────────────────────────────────
		"iw" | "aw" => {
			if !vim.select_text_object(ed, motion, count) {
				return Task::none();
			}
		}
		_ => {
			ed.buffer.session.selection = Selection::caret(origin);
			ed.update_status();
			return Task::none();
		}
	}

	match op {
		'd' => {
			let yanked = ed.buffer.cut();
			ed.buffer.session.selection =
				Selection::caret(ed.buffer.session.selection.head);
			ed.buffer.session.clipboard_is_line = false;
			vim.last_edit = Some(NormalEdit::OperatorMotion {
				op: 'd',
				motion: motion.to_string(),
				count,
			});
			ed.update_status();
			ed.ensure_cursor_visible();
			if !yanked.is_empty() {
				return iced::clipboard::write::<EditorMsg>(yanked).map(|_| EditorMsg::Noop);
			}
		}
		'y' => {
			let yanked = ed.buffer.copy();
			let start = origin.min(ed.buffer.session.selection.head);
			ed.buffer.session.selection = Selection::caret(start);
			ed.buffer.session.clipboard_is_line = false;
			ed.update_status();
			ed.ensure_cursor_visible();
			if !yanked.is_empty() {
				return iced::clipboard::write::<EditorMsg>(yanked).map(|_| EditorMsg::Noop);
			}
		}
		'c' => {
			let _ = ed.buffer.cut();
			ed.buffer.session.selection =
				Selection::caret(ed.buffer.session.selection.head);
			vim.last_edit = Some(NormalEdit::ChangeMotion {
				motion: motion.to_string(),
				count,
			});
			vim.enter_insert_mode(ed);
			ed.update_status();
			ed.ensure_cursor_visible();
		}
		_ => {}
	}
	Task::none()
}

// ─── Dot-repeat ───────────────────────────────────────────────────────────

pub(in crate::editor) fn replay_edit(
	vim: &mut VimHandler,
	ed: &mut CodeEditor,
	edit: NormalEdit,
) -> Task<EditorMsg> {
	match edit {
		NormalEdit::OperatorMotion { op, motion, count } => {
			exec_operator_motion(vim, ed, op, &motion, count)
		}
		NormalEdit::ChangeMotion { motion, count } => {
			let _ = exec_operator_motion(vim, ed, 'c', &motion, count);
			// exec_operator_motion for 'c' leaves us in Insert mode;
			// directly insert the saved text and return to Normal.
			let text = vim.last_insert_text.clone();
			for c in text.chars() {
				ed.buffer.insert_char(c);
			}
			vim.mode = VimMode::Normal;
			Task::none()
		}
		NormalEdit::LineOp { op: 'd', count } => {
			let line = ed.buffer.session.selection.head.line;
			let yanked = ed.buffer.yank_lines(line, count);
			ed.buffer.delete_lines(line, count);
			iced::clipboard::write::<EditorMsg>(yanked).map(|_| EditorMsg::Noop)
		}
		NormalEdit::LineOp { op: 'c', count: _ } => {
			let line = ed.buffer.session.selection.head.line;
			let len = ed.buffer.line_len(line);
			ed.buffer.session.selection = Selection {
				anchor: CursorPos::new(line, CharIdx(0)),
				head: CursorPos::new(line, len),
			};
			let _ = ed.buffer.cut();
			ed.buffer.session.selection = Selection::caret(CursorPos::new(line, CharIdx(0)));
			let text = vim.last_insert_text.clone();
			for c in text.chars() {
				ed.buffer.insert_char(c);
			}
			Task::none()
		}
		NormalEdit::LineOp { .. } => Task::none(),
		NormalEdit::DeleteChar { count } => {
			for _ in 0..count {
				ed.buffer.delete();
			}
			Task::none()
		}
		NormalEdit::BackspaceChar { count } => {
			for _ in 0..count {
				ed.buffer.backspace();
			}
			Task::none()
		}
		NormalEdit::ToggleCase { count } => {
			for _ in 0..count {
				let pos = ed.buffer.session.selection.head;
				let lt = ed.buffer.line_text(pos.line);
				if let Some(c) = lt.chars().nth(*pos.col) {
					let toggled = if c.is_uppercase() {
						c.to_lowercase().next().unwrap_or(c)
					} else {
						c.to_uppercase().next().unwrap_or(c)
					};
					ed.buffer.replace_char(toggled);
					ed.buffer.move_right(false);
				}
			}
			Task::none()
		}
		NormalEdit::ReplaceChar { ch, count } => {
			for _ in 0..count {
				ed.buffer.replace_char(ch);
			}
			Task::none()
		}
	}
}
