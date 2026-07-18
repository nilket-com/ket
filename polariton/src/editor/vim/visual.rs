use iced::Task;
use iced::keyboard::{self, Key};

use super::super::coords::{CharIdx, CursorPos, Selection};
use super::super::core::{CodeEditor, EditorMsg};
use super::{VimHandler, VimMode};

// ─── Visual mode ──────────────────────────────────────────────────────────

pub(in crate::editor) fn handle_visual_key(
	vim: &mut VimHandler,
	ed: &mut CodeEditor,
	key: Key,
	mods: keyboard::Modifiers,
	text: Option<String>,
) -> Task<EditorMsg> {
	use keyboard::key::Named;
	let ctrl = mods.command();

	if let Key::Character(_) = &key {
		let ch = text.as_deref().unwrap_or("");
		let is_count_digit = ch.len() == 1
			&& ch.chars().next().map_or(false, |c| c.is_ascii_digit())
			&& (ch != "0" || !vim.count.is_empty());
		if is_count_digit {
			vim.count.push_str(ch);
			return Task::none();
		}
	}
	let count = vim.take_count();
	let was_g = vim.pending_g;
	vim.pending_g = false;

	// Text object handling
	if let Some(prefix) = vim.pending_obj_prefix.take() {
		if let Key::Character(ref kc) = key {
			let obj = format!("{}{}", prefix, kc.as_str());
			vim.select_text_object(ed, &obj, count);
			ed.update_status();
			ed.ensure_cursor_visible();
		}
		return Task::none();
	}

	match key {
		Key::Named(Named::Escape) => {
			ed.buffer.session.selection.anchor = ed.buffer.session.selection.head;
			vim.mode = VimMode::Normal;
		}
		Key::Named(Named::ArrowLeft) => ed.buffer.move_left(true),
		Key::Named(Named::ArrowRight) => ed.buffer.move_right(true),
		Key::Named(Named::ArrowUp) => ed.buffer.move_up(true),
		Key::Named(Named::ArrowDown) => ed.buffer.move_down(true),
		Key::Character(ref kc) => {
			let key_ch = kc.as_str();
			let ch = if ctrl {
				key_ch
			} else {
				text.as_deref().unwrap_or(key_ch)
			};
			if ctrl {
				match ch {
					"f" | "F" => ed.buffer.search_open(),
					_ => {}
				}
			} else {
				match ch {
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
					"g" => {
						if was_g {
							ed.buffer.move_to_start(true);
						} else {
							vim.pending_g = true;
						}
					}
					"i" | "a" => {
						vim.pending_obj_prefix = ch.chars().next();
						return Task::none();
					}
					"d" | "x" => {
						let yanked = ed.buffer.cut();
						ed.buffer.session.selection =
							Selection::caret(ed.buffer.session.selection.head);
						vim.mode = VimMode::Normal;
						ed.update_status();
						ed.ensure_cursor_visible();
						if !yanked.is_empty() {
							return iced::clipboard::write::<EditorMsg>(yanked)
								.map(|_| EditorMsg::Noop);
						}
					}
					"y" => {
						let yanked = ed.buffer.copy();
						let (s, _) = ed.buffer.session.selection.ordered();
						ed.buffer.session.selection = Selection::caret(s);
						vim.mode = VimMode::Normal;
						ed.update_status();
						ed.ensure_cursor_visible();
						if !yanked.is_empty() {
							return iced::clipboard::write::<EditorMsg>(yanked)
								.map(|_| EditorMsg::Noop);
						}
					}
					"p" => {
						return iced::clipboard::read()
							.map(|t| EditorMsg::VisualPaste(t.unwrap_or_default()));
					}
					"c" => {
						let _ = ed.buffer.cut();
						ed.buffer.session.selection =
							Selection::caret(ed.buffer.session.selection.head);
						vim.enter_insert_mode(ed);
						ed.update_status();
						ed.ensure_cursor_visible();
						return Task::none();
					}
					"v" => {
						if vim.mode == VimMode::Visual {
							vim.mode = VimMode::Normal;
							ed.buffer.session.selection.anchor =
								ed.buffer.session.selection.head;
						} else {
							vim.mode = VimMode::Visual;
							let (s, e) = ed.buffer.session.selection.ordered();
							ed.buffer.session.selection.anchor = s;
							ed.buffer.session.selection.head = e;
						}
					}
					"V" => {
						if vim.mode == VimMode::VisualLine {
							vim.mode = VimMode::Normal;
							ed.buffer.session.selection.anchor =
								ed.buffer.session.selection.head;
						} else {
							vim.mode = VimMode::VisualLine;
							let (s, e) = ed.buffer.session.selection.ordered();
							ed.buffer.select_lines(*e.line - *s.line + 1);
						}
					}
					"u" => {
						ed.buffer.transform_case(false);
						vim.mode = VimMode::Normal;
						ed.update_status();
						ed.ensure_cursor_visible();
						return Task::none();
					}
					"U" => {
						ed.buffer.transform_case(true);
						vim.mode = VimMode::Normal;
						ed.update_status();
						ed.ensure_cursor_visible();
						return Task::none();
					}
					"<" => {
						ed.buffer.dedent_lines();
						vim.mode = VimMode::Normal;
						ed.update_status();
						ed.ensure_cursor_visible();
						return Task::none();
					}
					">" => {
						ed.buffer.indent_lines();
						vim.mode = VimMode::Normal;
						ed.update_status();
						ed.ensure_cursor_visible();
						return Task::none();
					}
					_ => {}
				}
			}
		}
		_ => {}
	}

	// V-LINE: snap selection to whole lines
	if vim.mode == VimMode::VisualLine {
		let (s, e) = ed.buffer.session.selection.ordered();
		if ed.buffer.session.selection.head >= ed.buffer.session.selection.anchor {
			ed.buffer.session.selection.anchor = CursorPos::new(s.line, CharIdx(0));
			ed.buffer.session.selection.head =
				CursorPos::new(e.line, ed.buffer.line_len(e.line));
		} else {
			ed.buffer.session.selection.head = CursorPos::new(s.line, CharIdx(0));
			ed.buffer.session.selection.anchor =
				CursorPos::new(e.line, ed.buffer.line_len(e.line));
		}
	}

	ed.update_status();
	ed.ensure_cursor_visible();
	Task::none()
}

// ─── Visual block mode ────────────────────────────────────────────────────

pub(in crate::editor) fn handle_visual_block_key(
	vim: &mut VimHandler,
	ed: &mut CodeEditor,
	key: Key,
	mods: keyboard::Modifiers,
	text: Option<String>,
) -> Task<EditorMsg> {
	use keyboard::key::Named;
	let ctrl = mods.command();

	if let Key::Character(_) = &key {
		let ch = text.as_deref().unwrap_or("");
		let is_count_digit = ch.len() == 1
			&& ch.chars().next().map_or(false, |c| c.is_ascii_digit())
			&& (ch != "0" || !vim.count.is_empty());
		if is_count_digit {
			vim.count.push_str(ch);
			return Task::none();
		}
	}
	let count = vim.take_count();
	vim.pending_g = false;

	// Text object handling
	if let Some(prefix) = vim.pending_obj_prefix.take() {
		if let Key::Character(ref kc) = key {
			let obj = format!("{}{}", prefix, kc.as_str());
			vim.select_text_object(ed, &obj, count);
			ed.update_status();
			ed.ensure_cursor_visible();
		}
		return Task::none();
	}

	match key {
		Key::Named(Named::Escape) => {
			ed.buffer.session.selection.anchor = ed.buffer.session.selection.head;
			vim.mode = VimMode::Normal;
		}
		Key::Named(Named::ArrowLeft) => ed.buffer.move_left(true),
		Key::Named(Named::ArrowRight) => ed.buffer.move_right(true),
		Key::Named(Named::ArrowUp) => ed.buffer.move_up(true),
		Key::Named(Named::ArrowDown) => ed.buffer.move_down(true),
		Key::Character(ref kc) => {
			let key_ch = kc.as_str();
			let ch = if ctrl {
				key_ch
			} else {
				text.as_deref().unwrap_or(key_ch)
			};
			if ctrl {
				match ch {
					"v" | "V" => {
						// Ctrl+V again collapses back to Normal
						ed.buffer.session.selection.anchor =
							ed.buffer.session.selection.head;
						vim.mode = VimMode::Normal;
					}
					_ => {}
				}
			} else {
				match ch {
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
					"i" | "a" => {
						vim.pending_obj_prefix = ch.chars().next();
						return Task::none();
					}
					"I" => {
						let (s, e) = ed.buffer.session.selection.ordered();
						vim.block_insert = Some((s.col, s.line, e.line));
						ed.buffer.session.selection = Selection::caret(s);
						vim.mode = VimMode::Insert;
						ed.update_status();
						return Task::none();
					}
					"A" => {
						let (s, e) = ed.buffer.session.selection.ordered();
						vim.block_insert = Some((e.col, s.line, e.line));
						ed.buffer.session.selection = Selection::caret(e);
						vim.mode = VimMode::Insert;
						ed.update_status();
						return Task::none();
					}
					"d" | "x" => {
						let (s, e) = ed.buffer.session.selection.ordered();
						ed.buffer.block_delete(s.line, e.line, s.col, e.col);
						ed.buffer.session.selection = Selection::caret(s);
						vim.mode = VimMode::Normal;
					}
					"<" => {
						ed.buffer.dedent_lines();
						vim.mode = VimMode::Normal;
					}
					">" => {
						ed.buffer.indent_lines();
						vim.mode = VimMode::Normal;
					}
					_ => {}
				}
			}
		}
		_ => {}
	}

	ed.update_status();
	ed.ensure_cursor_visible();
	Task::none()
}
