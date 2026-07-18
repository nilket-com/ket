use iced::Task;
use iced::keyboard::{self, Key};

use super::{VimHandler, VimMode, parse_substitute};
use super::super::coords::{CharIdx, CursorPos, LineIdx};
use super::super::core::{CodeEditor, EditorMsg};

// ─── Command bar ──────────────────────────────────────────────────────────

pub(in crate::editor) fn handle_command_key(
	vim: &mut VimHandler,
	ed: &mut CodeEditor,
	key: Key,
	text: Option<String>,
) -> Task<EditorMsg> {
	use keyboard::key::Named;
	match key {
		Key::Named(Named::Escape) => {
			vim.mode = VimMode::Normal;
			vim.command.clear();
		}
		Key::Named(Named::Enter) => {
			execute_vim_command(vim, ed);
			vim.mode = VimMode::Normal;
			vim.command.clear();
		}
		Key::Named(Named::Backspace) => {
			if vim.command.pop().is_none() {
				vim.mode = VimMode::Normal;
			}
		}
		Key::Named(Named::Space) => {
			vim.command.push(' ');
		}
		Key::Character(_) => {
			if let Some(t) = text {
				vim.command.push_str(&t);
			}
		}
		_ => {}
	}
	ed.update_status();
	Task::none()
}

fn execute_vim_command(vim: &mut VimHandler, ed: &mut CodeEditor) {
	let cmd = vim.command.trim().to_string();

	if let Ok(n) = cmd.parse::<usize>() {
		let line_count = *ed.buffer.line_count();
		let line = n
			.saturating_sub(1usize)
			.min(line_count.saturating_sub(1usize));
		let target = LineIdx(line);
		ed.buffer.session.selection.anchor = CursorPos::new(target, CharIdx(0));
		ed.buffer.session.selection.head = CursorPos::new(target, CharIdx(0));
		ed.ensure_cursor_visible();
		return;
	}

	if let Some((first, last, pat, rep, global, icase)) = parse_substitute(
		&cmd,
		*ed.buffer.session.selection.head.line,
		(*ed.buffer.line_count()).saturating_sub(1usize),
	) {
		let changed = ed
			.buffer
			.substitute(LineIdx(first), LineIdx(last), &pat, &rep, global, icase);
		if changed > 0 {
			let line_count = *ed.buffer.line_count();
			let line = first.min(line_count.saturating_sub(1usize));
			let target = LineIdx(line);
			ed.buffer.session.selection.anchor = CursorPos::new(target, CharIdx(0));
			ed.buffer.session.selection.head = CursorPos::new(target, CharIdx(0));
			ed.ensure_cursor_visible();
		}
		ed.update_status();
		return;
	}

	match cmd.as_str() {
		"noh" | "nohl" | "nohlsearch" => ed.buffer.search_close(),
		"q" | "q!" | "wq" | "w" => {}
		_ => {}
	}
}
