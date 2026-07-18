use iced::Task;
use iced::keyboard::{self, Key};

use super::{NormalEdit, VimHandler, VimMode};
use super::super::coords::{CharIdx, CursorPos, LineIdx, Selection};
use super::super::core::{CodeEditor, EditorMsg};
use super::super::widget;

// ─── Normal mode ──────────────────────────────────────────────────────────────

pub(in crate::editor) fn handle_normal_key(
	vim: &mut VimHandler,
	ed: &mut CodeEditor,
	key: Key,
	mods: keyboard::Modifiers,
	text: Option<String>,
) -> Task<EditorMsg> {
	use keyboard::key::Named;
	let shift = mods.shift();
	let ctrl = mods.command();
	let was_g = vim.pending_g;
	vim.pending_g = false;
	let was_z = vim.pending_z;
	vim.pending_z = false;

	// f/F/t/T pending: next key is the target char
	if let Some(find_kind) = vim.pending_find.take() {
		let target = match &key {
			Key::Named(Named::Space) => Some(' '),
			Key::Character(kc) => {
				let s = if ctrl {
					kc.as_str()
				} else {
					text.as_deref().unwrap_or(kc.as_str())
				};
				s.chars().next()
			}
			_ => None,
		};
		if let Some(tc) = target {
			let count = vim.take_count();
			vim.last_find = Some((find_kind, tc));
			vim.do_find(ed, find_kind, tc, count, false);
			ed.update_status();
			ed.ensure_cursor_visible();
		}
		return Task::none();
	}

	// `r` (replace char) consumes the very next key as the replacement
	if vim.pending_op == Some('r') {
		vim.pending_op = None;
		let ch = match &key {
			Key::Named(Named::Space) => Some(' '),
			Key::Named(Named::Tab) => Some('\t'),
			Key::Named(Named::Enter) => Some('\n'),
			Key::Named(Named::Escape) => None,
			Key::Character(_) => text.as_deref().and_then(|t| t.chars().next()),
			_ => None,
		};
		if let Some(c) = ch {
			let count = vim.take_count();
			for _ in 0..count {
				ed.buffer.replace_char(c);
			}
			vim.last_edit = Some(NormalEdit::ReplaceChar { ch: c, count });
		} else {
			vim.count.clear();
		}
		ed.update_status();
		ed.ensure_cursor_visible();
		return Task::none();
	}

	match key {
		Key::Named(Named::Escape) => {
			if ed.buffer.session.search.is_open {
				ed.buffer.search_close();
			}
			ed.buffer.session.selection.anchor = ed.buffer.session.selection.head;
			vim.count.clear();
			vim.pending_op = None;
		}
		Key::Named(Named::ArrowLeft) if ctrl => ed.buffer.move_word_left(shift),
		Key::Named(Named::ArrowRight) if ctrl => ed.buffer.move_word_right(shift),
		Key::Named(Named::ArrowLeft) => ed.buffer.move_left(shift),
		Key::Named(Named::ArrowRight) => ed.buffer.move_right(shift),
		Key::Named(Named::ArrowUp) => ed.buffer.move_up(shift),
		Key::Named(Named::ArrowDown) => ed.buffer.move_down(shift),
		Key::Named(Named::Home) if ctrl => ed.buffer.move_to_start(shift),
		Key::Named(Named::End) if ctrl => ed.buffer.move_to_end(shift),
		Key::Named(Named::Home) => ed.buffer.move_home(shift),
		Key::Named(Named::End) => ed.buffer.move_end(shift),
		Key::Named(Named::PageUp) => {
			let v = widget::visible_line_count(ed.view.viewport_h);
			ed.buffer.page_up(v, false);
		}
		Key::Named(Named::PageDown) => {
			let v = widget::visible_line_count(ed.view.viewport_h);
			ed.buffer.page_down(v, false);
		}

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
					"w" | "W" => {
						let e = !ed.buffer.document.wrap_config.enabled;
						ed.set_wrap_enabled(e);
					}
					"m" | "M" => ed.view.show_minimap = !ed.view.show_minimap,
					"l" | "L" => ed.view.show_whitespace = !ed.view.show_whitespace,
					"r" | "R" => ed.buffer.redo(),
					"v" | "V" => {
						vim.mode = VimMode::VisualBlock;
						ed.buffer.session.selection.anchor =
							ed.buffer.session.selection.head;
					}
					_ => {}
				}
			} else {
				// z-prefix commands: zz / zt / zb
				if was_z {
					match ch {
						"z" => vim.scroll_cursor_z(ed, 'z'),
						"t" => vim.scroll_cursor_z(ed, 't'),
						"b" => vim.scroll_cursor_z(ed, 'b'),
						_ => {}
					}
					return Task::none();
				}

				// Count prefix digits
				let is_count_digit = ch.len() == 1
					&& ch.chars().next().map_or(false, |c| c.is_ascii_digit())
					&& (ch != "0" || !vim.count.is_empty());
				if is_count_digit {
					vim.count.push_str(ch);
					ed.update_status();
					return Task::none();
				}

				let count = vim.take_count();

				// Pending operator + motion/doubling
				if let Some(op) = vim.pending_op.take() {
					if (op == '>' && ch == ">") || (op == '<' && ch == "<") {
						let line = ed.buffer.session.selection.head.line;
						let line_count = *ed.buffer.line_count();
						let last =
							(line + count - 1).min(LineIdx(line_count.saturating_sub(1usize)));
						ed.buffer.session.selection = Selection {
							anchor: CursorPos::new(line, CharIdx(0)),
							head: CursorPos::new(last, ed.buffer.line_len(last)),
						};
						if op == '>' {
							ed.buffer.indent_lines();
						} else {
							ed.buffer.dedent_lines();
						}
						ed.buffer.session.selection =
							Selection::caret(CursorPos::new(line, CharIdx(0)));
						ed.update_status();
						ed.ensure_cursor_visible();
						return Task::none();
					}
					// Text object prefix: 'i'/'a' followed by object key (w, s, …)
					if let Some(obj) = vim.pending_obj_prefix.take() {
						let motion = format!("{}{}", obj, ch);
						return vim.exec_operator_motion(ed, op, &motion, count);
					}
					// Wait for text-object key
					if (ch == "i" || ch == "a") && !was_g {
						vim.pending_op = Some(op);
						vim.pending_obj_prefix = ch.chars().next();
						return Task::none();
					}
					// `g` inside dg/yg/cg — wait for second `g`
					if ch == "g" && !was_g {
						vim.pending_op = Some(op);
						vim.pending_g = true;
						return Task::none();
					}
					let task = match (op, ch) {
						('d', "d") => {
							let line = ed.buffer.session.selection.head.line;
							let yanked = ed.buffer.yank_lines(line, count);
							ed.buffer.delete_lines(line, count);
							vim.last_edit = Some(NormalEdit::LineOp { op: 'd', count });
							ed.update_status();
							ed.ensure_cursor_visible();
							iced::clipboard::write::<EditorMsg>(yanked).map(|_| EditorMsg::Noop)
						}
						('y', "y") => {
							let line = ed.buffer.session.selection.head.line;
							let yanked = ed.buffer.yank_lines(line, count);
							ed.update_status();
							iced::clipboard::write::<EditorMsg>(yanked).map(|_| EditorMsg::Noop)
						}
						('c', "c") => {
							let line = ed.buffer.session.selection.head.line;
							let len = ed.buffer.line_len(line);
							ed.buffer.session.selection = Selection {
								anchor: CursorPos::new(line, CharIdx(0)),
								head: CursorPos::new(line, len),
							};
							let _ = ed.buffer.cut();
							ed.buffer.session.selection =
								Selection::caret(CursorPos::new(line, CharIdx(0)));
							vim.last_edit = Some(NormalEdit::LineOp { op: 'c', count });
							vim.enter_insert_mode(ed);
							ed.update_status();
							ed.ensure_cursor_visible();
							Task::none()
						}
						(op, motion) => {
							let motion_str = if was_g { "gg" } else { motion };
							vim.exec_operator_motion(ed, op, motion_str, count)
						}
					};
					return task;
				}

				match ch {
					"i" => vim.enter_insert_mode(ed),
					"I" => {
						ed.buffer.move_home(false);
						vim.enter_insert_mode(ed);
					}
					"a" => {
						ed.buffer.move_right(false);
						vim.enter_insert_mode(ed);
					}
					"A" => {
						ed.buffer.move_end(false);
						vim.enter_insert_mode(ed);
					}
					"o" => {
						ed.buffer.move_end(false);
						ed.buffer.insert_newline();
						vim.enter_insert_mode(ed);
					}
					"O" => {
						ed.buffer.move_home(false);
						ed.buffer.insert_newline();
						ed.buffer.move_up(false);
						vim.enter_insert_mode(ed);
					}
					"v" => {
						vim.mode = VimMode::Visual;
						ed.buffer.session.selection.anchor =
							ed.buffer.session.selection.head;
					}
					"V" => {
						vim.mode = VimMode::VisualLine;
						ed.buffer.select_lines(count);
					}
					"d" => vim.pending_op = Some('d'),
					"y" => vim.pending_op = Some('y'),
					"c" => vim.pending_op = Some('c'),
					"r" => vim.pending_op = Some('r'),
					">" => vim.pending_op = Some('>'),
					"<" => vim.pending_op = Some('<'),
					"C" => {
						return vim.exec_operator_motion(ed, 'c', "$", 1);
					}
					"p" => {
						return iced::clipboard::read()
							.map(|t| EditorMsg::PasteAfter(t.unwrap_or_default()));
					}
					"P" => {
						return iced::clipboard::read()
							.map(|t| EditorMsg::Paste(t.unwrap_or_default()));
					}
					"~" => {
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
						vim.last_edit = Some(NormalEdit::ToggleCase { count });
					}
					"*" => {
						if let Some(word) = ed.buffer.word_under_cursor() {
							ed.buffer.search_star(&word, true);
							ed.ensure_cursor_visible();
						}
					}
					"#" => {
						if let Some(word) = ed.buffer.word_under_cursor() {
							ed.buffer.search_star(&word, false);
							ed.ensure_cursor_visible();
						}
					}
					":" => vim.mode = VimMode::Command,
					"h" => {
						for _ in 0..count {
							ed.buffer.move_left(false);
						}
					}
					"j" => {
						for _ in 0..count {
							ed.buffer.move_down(false);
						}
					}
					"k" => {
						for _ in 0..count {
							ed.buffer.move_up(false);
						}
					}
					"l" => {
						for _ in 0..count {
							ed.buffer.move_right(false);
						}
					}
					"w" => {
						for _ in 0..count {
							ed.buffer.move_word_right(false);
						}
					}
					"b" => {
						for _ in 0..count {
							ed.buffer.move_word_left(false);
						}
					}
					"e" => {
						for _ in 0..count {
							ed.buffer.move_word_right(false);
						}
					}
					"0" => ed.buffer.move_home(false),
					"$" => ed.buffer.move_end(false),
					"^" => ed.buffer.move_home(false),
					"g" if was_g => ed.buffer.move_to_start(false),
					"g" => vim.pending_g = true,
					"G" => ed.buffer.move_to_end(false),
					"x" => {
						for _ in 0..count {
							ed.buffer.delete();
						}
						vim.last_edit = Some(NormalEdit::DeleteChar { count });
					}
					"X" => {
						for _ in 0..count {
							ed.buffer.backspace();
						}
						vim.last_edit = Some(NormalEdit::BackspaceChar { count });
					}
					"u" => ed.buffer.undo(),
					"n" => ed.buffer.search_next(),
					"N" => ed.buffer.search_prev(),
					// ── find-char motions ───────────────────────────────
					"f" => {
						vim.pending_find = Some('f');
						return Task::none();
					}
					"F" => {
						vim.pending_find = Some('F');
						return Task::none();
					}
					"t" => {
						vim.pending_find = Some('t');
						return Task::none();
					}
					"T" => {
						vim.pending_find = Some('T');
						return Task::none();
					}
					";" => {
						if let Some((kind, target)) = vim.last_find {
							vim.do_find(ed, kind, target, count, false);
						}
					}
					"," => {
						if let Some((kind, target)) = vim.last_find {
							let rev = match kind {
								'f' => 'F',
								'F' => 'f',
								't' => 'T',
								'T' => 't',
								c => c,
							};
							vim.do_find(ed, rev, target, count, false);
						}
					}
					// ── scroll centering ────────────────────────────────
					"z" => {
						vim.pending_z = true;
						return Task::none();
					}
					// ── dot repeat ──────────────────────────────────────
					"." => {
						if let Some(edit) = vim.last_edit.clone() {
							let task = vim.replay_edit(ed, edit);
							ed.update_status();
							ed.ensure_cursor_visible();
							return task;
						}
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
