use crate::editor::analysis::AnalysisSnapshot;
use crate::editor::coords::{
	ByteIdx, CharIdx, CursorPos, LineIdx, Selection, TAB_WIDTH, document, line,
};
use crate::editor::folding::FoldState;
use crate::editor::highlight::{SyntaxLanguage, SyntaxToken, TokenKind};
use crate::editor::search::SearchState;
use crate::editor::undo::{EditKind, UndoConfig, UndoStack};
use crate::editor::wrap::{self, WrapConfig};
use regex::{Captures, RegexBuilder};
use ropey::Rope;

use super::state::{DocumentState, SessionState};

#[derive(Debug, Clone, Copy)]
pub struct TokenSpan {
	pub col_start: CharIdx,
	pub col_end: CharIdx,
	pub kind: TokenKind,
}

// ─── Bracket matching ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BracketPair {
	pub open_line: LineIdx,
	pub open_col: CharIdx,
	pub close_line: LineIdx,
	pub close_col: CharIdx,
}

#[derive(Clone, Copy)]
struct RefreshFlags {
	brackets: bool,
	visual_lines: bool,
	search: bool,
}

impl RefreshFlags {
	const TEXT_EDIT: Self = Self {
		brackets: true,
		visual_lines: true,
		search: true,
	};

	const VISUAL_LINES_ONLY: Self = Self {
		brackets: false,
		visual_lines: true,
		search: false,
	};

	const BRACKETS_ONLY: Self = Self {
		brackets: true,
		visual_lines: false,
		search: false,
	};
}

// ─── Auto-pairs ───────────────────────────────────────────────────────────────

fn matching_close(c: char) -> Option<char> {
	match c {
		'(' => Some(')'),
		'[' => Some(']'),
		'{' => Some('}'),
		'\'' => Some('\''),
		'"' => Some('"'),
		_ => None,
	}
}
fn is_open_bracket(c: char) -> bool {
	matches!(c, '(' | '[' | '{')
}
fn is_close_bracket(c: char) -> bool {
	matches!(c, ')' | ']' | '}')
}
fn matching_open(c: char) -> Option<char> {
	match c {
		')' => Some('('),
		']' => Some('['),
		'}' => Some('{'),
		_ => None,
	}
}

// ─── Buffer ───────────────────────────────────────────────────────────────────

pub struct Buffer {
	pub document: DocumentState,
	pub session: SessionState,
	undo_stack: UndoStack,
}

impl Buffer {
	pub fn new(text: &str, language: SyntaxLanguage) -> Self {
		Self::with_undo_config(text, language, UndoConfig::default())
	}

	pub fn with_undo_config(text: &str, language: SyntaxLanguage, undo_config: UndoConfig) -> Self {
		let rope = Rope::from_str(text);
		let sel = Selection::caret(CursorPos::zero());
		let undo = UndoStack::new(undo_config);

		let mut buf = Self {
			document: DocumentState {
				rope,
				diagnostics: Vec::new(),
				folds: FoldState::new(),
				wrap_config: WrapConfig::default(),
				visual_lines: Vec::new(),
				language,
				tokens: Vec::new(),
				document_version: 0,
				analyzed_version: 0,
			},
			session: SessionState {
				selection: sel,
				secondary_selections: Vec::new(),
				matched_bracket: None,
				search: SearchState::new(),
				desired_col: None,
				clipboard: String::new(),
				clipboard_is_line: false,
			},
			undo_stack: undo,
		};
		buf.post_edit();
		buf
	}

	// ── Accessors ─────────────────────────────────────────────────────────

	pub fn tokens(&self) -> &[SyntaxToken] {
		&self.document.tokens
	}
	pub fn language(&self) -> SyntaxLanguage {
		self.document.language
	}

	pub fn set_language(&mut self, lang: SyntaxLanguage) {
		self.document.language = lang;
		self.document.tokens.clear();
		self.document.diagnostics.clear();
		self.document.folds.apply_regions(Default::default());
		self.document.analyzed_version = 0;
		self.post_edit();
	}

	pub fn document_version(&self) -> u64 {
		self.document.document_version
	}

	pub fn analysis_is_current(&self) -> bool {
		self.document.analyzed_version == self.document.document_version
	}

	pub fn apply_analysis(&mut self, snapshot: AnalysisSnapshot) -> bool {
		if snapshot.version != self.document.document_version
			|| snapshot.language != self.document.language
		{
			return false;
		}
		self.document.tokens = snapshot.tokens;
		self.document.diagnostics = snapshot.diagnostics;
		self.document.folds.apply_regions(snapshot.fold_regions);
		self.document.analyzed_version = snapshot.version;
		self.refresh_visual_lines();
		true
	}

	pub fn line_count(&self) -> LineIdx {
		LineIdx(self.document.rope.len_lines().max(1))
	}

	pub fn line_len(&self, line: LineIdx) -> CharIdx {
		document::line_len(&self.document.rope, line)
	}

	pub fn clamp_pos(&self, p: CursorPos) -> CursorPos {
		document::clamp_pos(&self.document.rope, p)
	}

	fn pos_to_char(&self, p: CursorPos) -> usize {
		document::pos_to_char(&self.document.rope, p)
	}

	pub fn line_text(&self, line: LineIdx) -> String {
		if *line >= self.document.rope.len_lines() {
			return String::new();
		}
		let s: String = self.document.rope.line(*line).chars().collect();
		s.trim_end_matches('\n').trim_end_matches('\r').to_string()
	}

	pub fn line_slice(&self, line: LineIdx, start_col: CharIdx, end_col: CharIdx) -> String {
		let text = self.line_text(line);
		line::slice_chars(&text, start_col, end_col)
	}

	pub fn token_spans_for_line(
		&self,
		line: LineIdx,
		start_col: CharIdx,
		end_col: CharIdx,
	) -> Vec<TokenSpan> {
		let text = self.line_text(line);
		let line_len = CharIdx(text.chars().count());
		let clipped_start = start_col.min(line_len);
		let clipped_end = end_col.min(line_len);
		if clipped_start >= clipped_end {
			return Vec::new();
		}

		let line_byte_start = self.document.rope.line_to_byte(*line);
		let line_byte_end = line_byte_start + text.len();
		let clip_byte_start = line_byte_start + *line::char_to_byte_idx(&text, clipped_start);
		let clip_byte_end = line_byte_start + *line::char_to_byte_idx(&text, clipped_end);

		let mut spans = Vec::new();
		for tok in &self.document.tokens {
			if tok.byte_range.end <= line_byte_start || tok.byte_range.start >= line_byte_end {
				continue;
			}
			let byte_start = tok.byte_range.start.max(clip_byte_start);
			let byte_end = tok.byte_range.end.min(clip_byte_end);
			if byte_start >= byte_end {
				continue;
			}
			let col_start = line::byte_to_char_idx(&text, ByteIdx(byte_start - line_byte_start));
			let col_end = line::byte_to_char_idx(&text, ByteIdx(byte_end - line_byte_start));
			if col_start < col_end {
				spans.push(TokenSpan {
					col_start,
					col_end,
					kind: tok.kind,
				});
			}
		}
		spans.sort_by_key(|span| span.col_start);
		spans
	}

	pub fn full_text(&self) -> String {
		self.document.rope.to_string()
	}

	pub fn selected_text(&self) -> String {
		if self.session.selection.is_caret() {
			return String::new();
		}
		let (s, e) = self.session.selection.ordered();
		self.document
			.rope
			.slice(self.pos_to_char(s)..self.pos_to_char(e))
			.to_string()
	}

	pub fn has_multiple_carets(&self) -> bool {
		self.session.selection.is_caret()
			&& !self.session.secondary_selections.is_empty()
			&& self
				.session
				.secondary_selections
				.iter()
				.all(Selection::is_caret)
	}

	pub fn clear_secondary_selections(&mut self) {
		self.session.secondary_selections.clear();
	}

	pub fn secondary_selections(&self) -> &[Selection] {
		&self.session.secondary_selections
	}

	pub fn selection_count(&self) -> usize {
		1 + self.session.secondary_selections.len()
	}

	pub fn has_secondary_selections(&self) -> bool {
		!self.session.secondary_selections.is_empty()
	}

	pub fn add_cursor(&mut self, pos: CursorPos) {
		if !self.session.selection.is_caret() {
			return;
		}
		let pos = self.clamp_pos(pos);
		if pos == self.session.selection.head {
			return;
		}
		self.session
			.secondary_selections
			.push(Selection::caret(pos));
		self.normalize_secondary_selections();
		self.refresh_bracket_match();
	}

	fn line_indent(&self, line: LineIdx) -> String {
		self.line_text(line)
			.chars()
			.take_while(|c| c.is_whitespace())
			.collect()
	}

	fn char_at(&self, p: CursorPos) -> Option<char> {
		self.line_text(self.clamp_pos(p).line)
			.chars()
			.nth(*self.clamp_pos(p).col)
	}

	fn char_before(&self, p: CursorPos) -> Option<char> {
		if *p.col == 0 {
			None
		} else {
			self.line_text(p.line).chars().nth(*p.col - 1)
		}
	}

	// ── Post-edit refresh ─────────────────────────────────────────────────

	fn post_edit(&mut self) {
		self.finish_undo();
		self.document.document_version = self.document.document_version.saturating_add(1);
		self.refresh(RefreshFlags::TEXT_EDIT);
	}

	fn refresh(&mut self, flags: RefreshFlags) {
		if flags.brackets {
			self.update_bracket_match();
		}
		if flags.visual_lines {
			self.recompute_visual_lines();
		}
		if flags.search && self.session.search.is_open {
			self.session.search.find_all(&self.document.rope);
		}
	}

	fn refresh_visual_lines(&mut self) {
		self.refresh(RefreshFlags::VISUAL_LINES_ONLY);
	}

	fn refresh_bracket_match(&mut self) {
		self.refresh(RefreshFlags::BRACKETS_ONLY);
	}

	fn recompute_visual_lines(&mut self) {
		self.document.visual_lines = wrap::compute_visual_lines(
			self.line_count(),
			&|l| self.line_text(l),
			&|l| self.document.folds.is_hidden(l),
			&self.document.wrap_config,
		);
	}

	fn normalize_secondary_selections(&mut self) {
		let primary = self.clamp_pos(self.session.selection.head);
		self.session.secondary_selections = self
			.session
			.secondary_selections
			.iter()
			.copied()
			.map(|sel| Selection {
				anchor: self.clamp_pos(sel.anchor),
				head: self.clamp_pos(sel.head),
			})
			.filter(|sel| sel.head != primary)
			.collect();
		self.session
			.secondary_selections
			.sort_by(|a, b| a.head.cmp(&b.head));
		self.session
			.secondary_selections
			.dedup_by(|a, b| a.anchor == b.anchor && a.head == b.head);
	}

	fn all_cursor_heads(&self) -> Vec<CursorPos> {
		let mut carets = Vec::with_capacity(self.session.secondary_selections.len() + 1);
		carets.push(self.session.selection.head);
		carets.extend(self.session.secondary_selections.iter().map(|s| s.head));
		carets.sort();
		carets.dedup();
		carets
	}

	fn set_cursor_heads(&mut self, primary: CursorPos, secondary: Vec<CursorPos>) {
		self.session.selection = Selection::caret(self.clamp_pos(primary));
		self.session.secondary_selections = secondary.into_iter().map(Selection::caret).collect();
		self.normalize_secondary_selections();
		self.refresh_bracket_match();
	}

	fn map_secondary_cursor_heads(&mut self, mut map: impl FnMut(&Self, CursorPos) -> CursorPos) {
		let primary = map(self, self.session.selection.head);
		let secondary = self
			.session
			.secondary_selections
			.iter()
			.map(|sel| map(self, sel.head))
			.collect();
		self.set_cursor_heads(primary, secondary);
	}

	fn map_all_selection_heads(&mut self, mut map: impl FnMut(&Self, Selection) -> CursorPos) {
		let primary = self.clamp_pos(map(self, self.session.selection));
		let next_secondary: Vec<_> = self
			.session
			.secondary_selections
			.iter()
			.copied()
			.map(|sel| Selection {
				head: self.clamp_pos(map(self, sel)),
				..sel
			})
			.collect();
		self.session.selection.head = primary;
		self.session.secondary_selections = next_secondary;
		self.normalize_secondary_selections();
		self.refresh_bracket_match();
	}

	fn selected_texts(&self) -> Vec<String> {
		let mut texts = Vec::new();
		for sel in
			std::iter::once(&self.session.selection).chain(self.session.secondary_selections.iter())
		{
			if sel.is_caret() {
				continue;
			}
			let (s, e) = sel.ordered();
			texts.push(
				self.document
					.rope
					.slice(self.pos_to_char(s)..self.pos_to_char(e))
					.to_string(),
			);
		}
		texts
	}

	// ── Undo / Redo ───────────────────────────────────────────────────────

	fn save_undo(&mut self, kind: EditKind) {
		self.undo_stack.begin_edit(self.session.selection, kind);
	}

	fn save_undo_boundary(&mut self) {
		self.undo_stack.force_boundary(self.session.selection);
	}

	fn finish_undo(&mut self) {
		self.undo_stack.end_edit(self.session.selection);
	}

	fn replace_range(&mut self, start: usize, end: usize, insert: &str) {
		let start_byte = self.document.rope.char_to_byte(start);
		let end_byte = self.document.rope.char_to_byte(end);
		let insert_byte_len = insert.len();
		let deleted_byte_len = end_byte - start_byte;

		let deleted = self.document.rope.slice(start..end).to_string();
		self.undo_stack
			.record_change(CharIdx(start), deleted, insert.to_string());
		self.document.rope.remove(start..end);
		if !insert.is_empty() {
			self.document.rope.insert(start, insert);
		}

		// Incrementally shift tokens to maintain alignment until the next async analysis returns.
		let diff = insert_byte_len as isize - deleted_byte_len as isize;
		if diff != 0 {
			self.document.tokens.retain_mut(|tok| {
				if tok.byte_range.start >= end_byte {
					// Entirely after the edit: shift both ends.
					tok.byte_range.start = (tok.byte_range.start as isize + diff) as usize;
					tok.byte_range.end = (tok.byte_range.end as isize + diff) as usize;
					true
				} else if tok.byte_range.end <= start_byte {
					// Entirely before the edit: no change.
					true
				} else {
					// Overlaps with the edit: clip or shift as best as possible.
					if tok.byte_range.start < start_byte {
						tok.byte_range.end = (tok.byte_range.end as isize + diff)
							.max(start_byte as isize) as usize;
						tok.byte_range.start < tok.byte_range.end
					} else {
						let new_start = (tok.byte_range.start as isize + diff)
							.max(start_byte as isize) as usize;
						let new_end = (tok.byte_range.end as isize + diff)
							.max(new_start as isize) as usize;
						tok.byte_range.start = new_start;
						tok.byte_range.end = new_end;
						tok.byte_range.start < tok.byte_range.end
					}
				}
			});
		}
	}

	fn insert_char_at(&mut self, start: CharIdx, insert: &str) {
		self.replace_range(*start, *start, insert);
	}

	fn remove_range(&mut self, start: CharIdx, end: CharIdx) {
		self.replace_range(*start, *end, "");
	}

	pub fn undo(&mut self) {
		if self
			.undo_stack
			.undo(&mut self.document.rope, &mut self.session.selection)
		{
			self.clear_secondary_selections();
			self.post_edit();
		}
	}

	pub fn redo(&mut self) {
		if self
			.undo_stack
			.redo(&mut self.document.rope, &mut self.session.selection)
		{
			self.clear_secondary_selections();
			self.post_edit();
		}
	}

	// ── Clipboard ─────────────────────────────────────────────────────────

	pub fn copy(&mut self) -> String {
		let text = if self.has_secondary_selections() {
			self.selected_texts().join("\n")
		} else {
			self.selected_text()
		};
		if !text.is_empty() {
			self.session.clipboard = text.clone();
			self.session.clipboard_is_line = false;
		}
		text
	}

	/// Delete a rectangular block from `top..=bottom` lines, columns `left_col..right_col_excl`.
	pub fn block_delete(
		&mut self,
		top: LineIdx,
		bottom: LineIdx,
		left_col: CharIdx,
		right_col_excl: CharIdx,
	) {
		if left_col >= right_col_excl {
			return;
		}
		self.save_undo_boundary();
		let bottom = bottom.min(self.line_count().saturating_sub(1usize));
		for li_raw in (*top..=*bottom).rev() {
			let li = LineIdx(li_raw);
			let line_len = self.line_len(li);
			if left_col >= line_len {
				continue;
			}
			let ci_start = self.document.rope.line_to_char(*li) + *left_col;
			let ci_end = self.document.rope.line_to_char(*li) + (*right_col_excl).min(*line_len);
			if ci_start < ci_end {
				self.remove_range(CharIdx(ci_start), CharIdx(ci_end));
			}
		}
		self.session.selection = Selection::caret(CursorPos::new(top, left_col));
		self.post_edit();
	}

	/// Insert `text` at `col` on every line from `top+1..=bottom`, replicating a block insert.
	/// The top line already has the text from normal insert-mode editing.
	pub fn block_insert_text(&mut self, top: LineIdx, bottom: LineIdx, col: CharIdx, text: &str) {
		if text.is_empty() {
			return;
		}
		let bottom = bottom.min(self.line_count().saturating_sub(1usize));
		if bottom <= top {
			return;
		}
		for li_raw in (*top + 1..=*bottom).rev() {
			let li = LineIdx(li_raw);
			let line_len = self.line_len(li);
			if col <= line_len {
				let ci = self.document.rope.line_to_char(*li) + *col;
				self.insert_char_at(CharIdx(ci), text);
			} else {
				// Pad with spaces to reach col, then insert
				let pad: String = " ".repeat(*col - *line_len);
				let ci = self.document.rope.line_to_char(*li) + *line_len;
				self.insert_char_at(CharIdx(ci), &format!("{}{}", pad, text));
			}
		}
		self.post_edit();
	}

	pub fn transform_case(&mut self, uppercase: bool) {
		if self.session.selection.is_caret() {
			return;
		}
		let text = self.selected_text();
		let transformed: String = if uppercase {
			text.chars().flat_map(|c| c.to_uppercase()).collect()
		} else {
			text.chars().flat_map(|c| c.to_lowercase()).collect()
		};
		let (s, e) = self.session.selection.ordered();
		self.save_undo_boundary();
		let ci_start = self.pos_to_char(s);
		let ci_end = self.pos_to_char(e);
		self.replace_range(ci_start, ci_end, &transformed);
		self.session.selection = Selection::caret(s);
		self.post_edit();
	}

	pub fn cut(&mut self) -> String {
		let text = self.copy();
		if self.has_secondary_selections() {
			let mut ranges: Vec<_> = std::iter::once(self.session.selection)
				.chain(self.session.secondary_selections.iter().copied())
				.filter(|sel| !sel.is_caret())
				.map(|sel| {
					let (s, e) = sel.ordered();
					(self.pos_to_char(s), self.pos_to_char(e), s)
				})
				.collect();
			if !ranges.is_empty() {
				self.save_undo_boundary();
				ranges.sort_by(|a, b| b.0.cmp(&a.0));
				let primary_start = self.session.selection.ordered().0;
				let secondary = ranges
					.iter()
					.map(|(_, _, start)| *start)
					.filter(|&start| start != primary_start)
					.collect();
				for (start, end, _) in &ranges {
					self.remove_range(CharIdx(*start), CharIdx(*end));
				}

				self.set_cursor_heads(primary_start, secondary);
				self.post_edit();
			}
		} else if !text.is_empty() {
			self.save_undo_boundary();
			self.delete_selection_inner();
			self.post_edit();
		}
		text
	}

	pub fn paste(&mut self, text: &str) {
		if text.is_empty() {
			return;
		}
		if self.has_secondary_selections() {
			let mut selections: Vec<_> = std::iter::once(self.session.selection)
				.chain(self.session.secondary_selections.iter().copied())
				.collect();
			let distributed: Vec<String> = if text.matches('\n').count() + 1 == selections.len() {
				text.split('\n').map(|s| s.to_string()).collect()
			} else {
				vec![text.to_string(); selections.len()]
			};
			self.save_undo(EditKind::Paste);
			let primary = self.session.selection.head;
			let mut edits: Vec<_> = selections
				.drain(..)
				.zip(distributed)
				.map(|(sel, insert)| {
					let (start, end) = if sel.is_caret() {
						let ci = self.pos_to_char(sel.head);
						(ci, ci)
					} else {
						let (s, e) = sel.ordered();
						(self.pos_to_char(s), self.pos_to_char(e))
					};
					(start, end, sel, insert)
				})
				.collect();
			edits.sort_by(|a, b| b.0.cmp(&a.0));
			for (start, end, _, insert) in &edits {
				self.replace_range(*start, *end, insert);
			}
			let mut new_primary = primary;
			let mut secondary = Vec::new();
			for (_, _, sel, insert) in edits {
				let base = if sel.is_caret() {
					sel.head
				} else {
					sel.ordered().0
				};
				let caret = if insert.contains('\n') {
					let count = insert.chars().filter(|&c| c == '\n').count();
					let after = insert.rsplit('\n').next().unwrap_or("");
					CursorPos::new(base.line + count, CharIdx(after.chars().count()))
				} else {
					CursorPos::new(base.line, base.col + insert.chars().count())
				};
				if sel.head == primary {
					new_primary = caret;
				} else {
					secondary.push(caret);
				}
			}
			self.set_cursor_heads(new_primary, secondary);
			self.post_edit();
			return;
		}
		if self.has_multiple_carets() {
			self.insert_text(text);
			return;
		}
		self.save_undo(EditKind::Paste);
		self.delete_selection_inner();
		self.session.desired_col = None;
		let pos = self.session.selection.head;
		let ci = self.pos_to_char(pos);
		self.insert_char_at(CharIdx(ci), text);

		let newlines = text.chars().filter(|c| *c == '\n').count();
		let new_pos = if newlines > 0 {
			let after = text.rsplit('\n').next().unwrap_or("");
			CursorPos::new(pos.line + newlines, CharIdx(after.chars().count()))
		} else {
			CursorPos::new(pos.line, pos.col + text.chars().count())
		};
		self.session.selection = Selection::caret(new_pos);
		self.post_edit();
	}

	/// Yank `count` whole lines starting at `line` into the internal clipboard.
	/// Returns the yanked text so callers can also write to the system clipboard.
	pub fn yank_lines(&mut self, line: LineIdx, count: usize) -> String {
		let last = (*line + count - 1).min(*self.line_count() - 1);
		let start_ci = self.document.rope.line_to_char(*line);
		let end_ci = if last + 1 < self.document.rope.len_lines() {
			self.document.rope.line_to_char(last + 1)
		} else {
			self.document.rope.len_chars()
		};
		let mut text: String = self.document.rope.slice(start_ci..end_ci).to_string();
		// Ensure the yanked text always ends with a newline so paste works correctly.
		if !text.ends_with('\n') {
			text.push('\n');
		}
		self.session.clipboard = text.clone();
		self.session.clipboard_is_line = true;
		text
	}

	/// Delete `count` whole lines starting at `line`.
	pub fn delete_lines(&mut self, line: LineIdx, count: usize) {
		let last = (*line + count - 1).min(*self.line_count() - 1);
		self.save_undo(EditKind::Delete);
		let start_ci = self.document.rope.line_to_char(*line);
		let end_ci = if last + 1 < self.document.rope.len_lines() {
			self.document.rope.line_to_char(last + 1)
		} else if *line > 0 {
			// Last line with no trailing newline: delete preceding newline too
			let prev_end = self.document.rope.line_to_char(*line);
			let prev_line_start = self.document.rope.line_to_char(*line - 1);
			let prev_text: String = self
				.document
				.rope
				.slice(prev_line_start..prev_end)
				.to_string();
			let trim = prev_text
				.trim_end_matches('\n')
				.trim_end_matches('\r')
				.chars()
				.count();
			prev_line_start + trim
		} else {
			self.document.rope.len_chars()
		};
		let real_start = start_ci.min(end_ci);
		let real_end = start_ci.max(end_ci);
		self.remove_range(CharIdx(real_start), CharIdx(real_end));
		let new_line = line.min(self.line_count().saturating_sub(1usize));
		self.session.selection = Selection::caret(CursorPos::new(new_line, 0));
		self.post_edit();
	}

	/// Paste linewise clipboard content as new line(s) below the current line.
	pub fn paste_line_below(&mut self) {
		if self.session.clipboard.is_empty() {
			return;
		}
		self.save_undo(EditKind::Paste);
		let line = self.session.selection.head.line;
		// Insert after the newline at end of current line
		let insert_ci = if *line + 1 < self.document.rope.len_lines() {
			self.document.rope.line_to_char(*line + 1)
		} else {
			// No trailing newline on last line — add one first
			let end = self.document.rope.len_chars();
			self.insert_char_at(CharIdx(end), "\n");
			end + 1
		};
		let text = self.session.clipboard.clone();
		self.insert_char_at(CharIdx(insert_ci), &text);
		self.session.selection = Selection::caret(CursorPos::new(line + 1, 0));
		self.post_edit();
	}

	/// Paste linewise clipboard content as new line(s) above the current line.
	pub fn paste_line_above(&mut self) {
		if self.session.clipboard.is_empty() {
			return;
		}
		self.save_undo(EditKind::Paste);
		let line = self.session.selection.head.line;
		let insert_ci = self.document.rope.line_to_char(*line);
		let text = self.session.clipboard.clone();
		self.insert_char_at(CharIdx(insert_ci), &text);
		self.session.selection = Selection::caret(CursorPos::new(line, 0));
		self.post_edit();
	}

	/// Select the full extent of `count` lines starting at the cursor's line.
	/// Sets anchor to line start and head to end of last line (exclusive of newline).
	pub fn select_lines(&mut self, count: usize) {
		let line = self.session.selection.head.line;
		let last = (*line + count - 1).min(*self.line_count() - 1);
		let last_line = LineIdx(last);
		self.session.selection.anchor = CursorPos::new(line, 0);
		self.session.selection.head = CursorPos::new(last_line, self.line_len(last_line));
	}

	// ── Indent / Dedent ───────────────────────────────────────────────────

	/// Indent selected lines (or current line) by one tab character.
	pub fn indent_lines(&mut self) {
		if self.has_multiple_carets() {
			self.save_undo(EditKind::Insert);
			let mut lines: Vec<LineIdx> = self
				.all_cursor_heads()
				.into_iter()
				.map(|p| p.line)
				.collect();
			lines.sort_unstable();
			lines.dedup();
			for &line in lines.iter().rev() {
				let ci = self.document.rope.line_to_char(*line);
				self.insert_char_at(CharIdx(ci), "\t");
			}
			self.map_secondary_cursor_heads(|_, p| CursorPos::new(p.line, p.col + 1));
			self.post_edit();
			return;
		}
		let (first, last) = if self.session.selection.is_caret() {
			let l = self.session.selection.head.line;
			(l, l)
		} else {
			let (s, e) = self.session.selection.ordered();
			(s.line, e.line)
		};
		self.save_undo(EditKind::Insert);
		for line_raw in (*first..=*last).rev() {
			let ci = self.document.rope.line_to_char(line_raw);
			self.insert_char_at(CharIdx(ci), "\t");
		}
		let shift = |p: CursorPos| CursorPos::new(p.line, p.col + 1);
		self.session.selection.anchor = shift(self.session.selection.anchor);
		self.session.selection.head = shift(self.session.selection.head);
		self.post_edit();
	}

	/// Dedent selected lines (or current line) by one tab stop.
	/// Removes a leading tab first; if none, removes up to 4 leading spaces.
	pub fn dedent_lines(&mut self) {
		if self.has_multiple_carets() {
			self.save_undo(EditKind::Delete);
			let mut lines: Vec<LineIdx> = self
				.all_cursor_heads()
				.into_iter()
				.map(|p| p.line)
				.collect();
			lines.sort_unstable();
			lines.dedup();
			let mut removed = Vec::with_capacity(lines.len());
			for &line in lines.iter().rev() {
				let text = self.line_text(line);
				let ci = self.document.rope.line_to_char(*line);
				let count = if text.starts_with('\t') {
					self.remove_range(CharIdx(ci), CharIdx(ci + 1));
					1
				} else {
					let spaces = text
						.chars()
						.take_while(|c| *c == ' ')
						.count()
						.min(TAB_WIDTH);
					if spaces > 0 {
						self.remove_range(CharIdx(ci), CharIdx(ci + spaces));
					}
					spaces
				};
				removed.push((line, count));
			}
			removed.sort_unstable_by_key(|(line, _)| *line);
			self.map_secondary_cursor_heads(|_, p| {
				let dec = removed
					.iter()
					.find(|(line, _)| *line == p.line)
					.map(|(_, count)| *count)
					.unwrap_or(0);
				CursorPos::new(p.line, p.col.saturating_sub(dec))
			});
			self.post_edit();
			return;
		}
		let (first, last) = if self.session.selection.is_caret() {
			let l = self.session.selection.head.line;
			(l, l)
		} else {
			let (s, e) = self.session.selection.ordered();
			(s.line, e.line)
		};
		self.save_undo(EditKind::Delete);
		let mut removed = vec![0usize; *last - *first + 1];
		for (i, line_raw) in (*first..=*last).rev().enumerate() {
			let line = LineIdx(line_raw);
			let text = self.line_text(line);
			let ci = self.document.rope.line_to_char(*line);
			let count = if text.starts_with('\t') {
				self.remove_range(CharIdx(ci), CharIdx(ci + 1));
				1
			} else {
				let spaces = text
					.chars()
					.take_while(|c| *c == ' ')
					.count()
					.min(TAB_WIDTH);
				if spaces > 0 {
					self.remove_range(CharIdx(ci), CharIdx(ci + spaces));
				}
				spaces
				};
				removed[*last - *first - i] = count;
				}

		let clamp = |p: CursorPos| {
			let rm = removed
				.get(*p.line.saturating_sub(*first))
				.copied()
				.unwrap_or(0);
			CursorPos::new(p.line, p.col.saturating_sub(rm))
		};
		self.session.selection.anchor = clamp(self.session.selection.anchor);
		self.session.selection.head = clamp(self.session.selection.head);
		self.post_edit();
	}

	// ── Editing ───────────────────────────────────────────────────────────

	pub fn insert_char(&mut self, ch: char) {
		self.insert_text(&ch.to_string());
	}

	pub fn insert_text(&mut self, text: &str) {
		if text.is_empty() {
			return;
		}
		if self.has_multiple_carets() {
			self.save_undo(EditKind::Insert);
			self.session.desired_col = None;
			let primary = self.session.selection.head;
			let mut edits: Vec<_> = self
				.all_cursor_heads()
				.into_iter()
				.map(|caret| (CharIdx(self.pos_to_char(caret)), caret))
				.collect();
			edits.sort_by(|a, b| b.0.cmp(&a.0));
			for &(ci, _) in &edits {
				self.insert_char_at(ci, text);
			}
			let newlines = text.chars().filter(|c| *c == '\n').count();
			let secondary = edits
				.iter()
				.map(|(_, caret)| {
					if newlines > 0 {
						let after = text.rsplit('\n').next().unwrap_or("");
						CursorPos::new(caret.line + newlines, CharIdx(after.chars().count()))
					} else {
						CursorPos::new(caret.line, caret.col + text.chars().count())
					}
				})
				.filter(|&caret| caret != primary)
				.collect();
			let new_primary = if newlines > 0 {
				let after = text.rsplit('\n').next().unwrap_or("");
				CursorPos::new(primary.line + newlines, CharIdx(after.chars().count()))
			} else {
				CursorPos::new(primary.line, primary.col + text.chars().count())
			};
			self.set_cursor_heads(new_primary, secondary);
			self.post_edit();
			return;
		}
		self.save_undo(EditKind::Insert);
		self.delete_selection_inner();
		self.session.desired_col = None;
		let pos = self.session.selection.head;
		let ci = self.pos_to_char(pos);
		self.insert_char_at(CharIdx(ci), text);
		let newlines = text.chars().filter(|c| *c == '\n').count();
		let new = if newlines > 0 {
			let after = text.rsplit('\n').next().unwrap_or("");
			CursorPos::new(pos.line + newlines, CharIdx(after.chars().count()))
		} else {
			CursorPos::new(pos.line, pos.col + text.chars().count())
		};
		self.session.selection = Selection::caret(new);
		self.post_edit();
	}

	/// Syntax-aware Enter: auto-indent + extra indent after openers.
	pub fn insert_newline(&mut self) {
		if self.has_multiple_carets() {
			self.save_undo(EditKind::Newline);
			self.session.desired_col = None;
			let primary = self.session.selection.head;
			let carets = self.all_cursor_heads();
			let mut edits: Vec<_> = carets
				.iter()
				.copied()
				.map(|caret| {
					let indent = self.line_indent(caret.line);
					let before = self.line_text(caret.line);
					let before_cursor = &before[..(*caret.col).min(before.len())];
					let trimmed = before_cursor.trim_end();
					let extra =
						match self.document.language {
							SyntaxLanguage::Txt => "",
							SyntaxLanguage::Sql => {
								if trimmed.ends_with('(')
									|| trimmed.ends_with('{') || trimmed.ends_with('[')
									|| trimmed.to_uppercase().ends_with(" AS")
									|| trimmed.to_uppercase().ends_with(" BEGIN")
									|| trimmed.to_uppercase().ends_with(" THEN")
								{
									"    "
								} else {
									""
								}
							}
							SyntaxLanguage::Rust => {
								if trimmed.ends_with('{')
									|| trimmed.ends_with('(') || trimmed.ends_with('[')
									|| trimmed.ends_with("=>")
								{
									"    "
								} else {
									""
								}
							}
						};
					(
						CharIdx(self.pos_to_char(caret)),
						caret,
						format!("\n{}{}", indent, extra),
						CharIdx(indent.chars().count() + extra.chars().count()),
					)
				})
				.collect();
			edits.sort_by(|a, b| b.0.cmp(&a.0));
			for (ci, _, text, _) in &edits {
				self.insert_char_at(*ci, text);
			}
			let secondary = edits
				.iter()
				.map(|(_, caret, _, col)| CursorPos::new(caret.line + 1, *col))
				.filter(|&caret| caret != primary)
				.collect();
			let primary_col = edits
				.iter()
				.find(|(_, caret, _, _)| *caret == primary)
				.map(|(_, _, _, col)| *col)
				.unwrap_or(CharIdx(0));
			self.set_cursor_heads(CursorPos::new(primary.line + 1, primary_col), secondary);
			self.post_edit();
			return;
		}
		self.save_undo(EditKind::Newline);
		self.delete_selection_inner();
		self.session.desired_col = None;
		let pos = self.session.selection.head;
		let indent = self.line_indent(pos.line);
		let before = self.line_text(pos.line);
		let before_cursor = &before[..(*pos.col).min(before.len())];
		let trimmed = before_cursor.trim_end();

		let extra = match self.document.language {
			SyntaxLanguage::Txt => "",
			SyntaxLanguage::Sql => {
				if trimmed.ends_with('(')
					|| trimmed.ends_with('{')
					|| trimmed.ends_with('[')
					|| trimmed.to_uppercase().ends_with(" AS")
					|| trimmed.to_uppercase().ends_with(" BEGIN")
					|| trimmed.to_uppercase().ends_with(" THEN")
				{
					"    "
				} else {
					""
				}
			}
			SyntaxLanguage::Rust => {
				if trimmed.ends_with('{')
					|| trimmed.ends_with('(')
					|| trimmed.ends_with('[')
					|| trimmed.ends_with("=>")
				{
					"    "
				} else {
					""
				}
			}
		};

		let ins = format!("\n{}{}", indent, extra);
		let ci = self.pos_to_char(pos);
		self.insert_char_at(CharIdx(ci), &ins);
		self.session.selection =
			Selection::caret(CursorPos::new(pos.line + 1, CharIdx(indent.chars().count() + extra.chars().count())));
		self.post_edit();
	}

	pub fn insert_char_auto_pair(&mut self, ch: char) {
		if self.has_multiple_carets() {
			if let Some(close) = matching_close(ch) {
				self.save_undo(EditKind::Insert);
				self.session.desired_col = None;
				let primary = self.session.selection.head;
				let mut edits: Vec<_> = self
					.all_cursor_heads()
					.into_iter()
					.map(|caret| (CharIdx(self.pos_to_char(caret)), caret))
					.collect();
				edits.sort_by(|a, b| b.0.cmp(&a.0));
				let pair = format!("{}{}", ch, close);
				for &(ci, _) in &edits {
					self.insert_char_at(ci, &pair);
				}
				let secondary = edits
					.iter()
					.map(|(_, caret)| CursorPos::new(caret.line, caret.col + 1))
					.filter(|&caret| caret != primary)
					.collect();
				self.set_cursor_heads(CursorPos::new(primary.line, primary.col + 1), secondary);
				self.post_edit();
				return;
			}
			self.insert_char(ch);
			return;
		}
		// Skip over matching close
		if is_close_bracket(ch) || ch == '\'' || ch == '"' {
			if self.char_at(self.session.selection.head) == Some(ch) {
				let p = self.session.selection.head;
				self.session.selection = Selection::caret(CursorPos::new(p.line, p.col + 1));
				self.session.desired_col = None;
				self.refresh_bracket_match();
				return;
			}
		}
		if let Some(close) = matching_close(ch) {
			if ch == '\'' || ch == '"' {
				if let Some(prev) = self.char_before(self.session.selection.head) {
					if prev.is_alphanumeric() || prev == '_' {
						self.insert_char(ch);
						return;
					}
				}
			}
			self.save_undo(EditKind::Insert);
			self.delete_selection_inner();
			self.session.desired_col = None;
			let p = self.session.selection.head;
			let ci = self.pos_to_char(p);
			self.insert_char_at(CharIdx(ci), &format!("{}{}", ch, close));
			self.session.selection = Selection::caret(CursorPos::new(p.line, p.col + 1));
			self.post_edit();
		} else {
			self.insert_char(ch);
		}
	}

	pub fn backspace(&mut self) {
		if self.has_multiple_carets() {
			self.session.desired_col = None;
			self.save_undo(EditKind::Delete);
			let primary = self.session.selection.head;
			let mut edits: Vec<_> = self
				.all_cursor_heads()
				.into_iter()
				.filter_map(|caret| {
					if *caret.line == 0 && *caret.col == 0 {
						return None;
					}
					let (new_pos, start, end) = if *caret.col == 0 {
						let prev_line = caret.line.saturating_sub(1usize);
						let new_pos = CursorPos::new(prev_line, self.line_len(prev_line));
						(new_pos, self.pos_to_char(new_pos), self.pos_to_char(caret))
					} else {
						let new_pos = CursorPos::new(caret.line, caret.col.saturating_sub(1usize));
						(new_pos, self.pos_to_char(new_pos), self.pos_to_char(caret))
					};
					Some((start, end, caret, new_pos))
				})
				.collect();
			edits.sort_by(|a, b| b.0.cmp(&a.0));
			for (start, end, _, _) in &edits {
				self.remove_range(CharIdx(*start), CharIdx(*end));
			}

			let secondary = edits
				.iter()
				.map(|(_, _, caret, new_pos)| if *caret == primary { primary } else { *new_pos })
				.filter(|&caret| caret != primary)
				.collect();
			let new_primary = edits
				.iter()
				.find(|(_, _, caret, _)| *caret == primary)
				.map(|(_, _, _, new_pos)| *new_pos)
				.unwrap_or(primary);
			self.set_cursor_heads(new_primary, secondary);
			self.post_edit();
			return;
		}
		self.session.desired_col = None;
		if !self.session.selection.is_caret() {
			self.save_undo(EditKind::Delete);
			self.delete_selection_inner();
			self.post_edit();
			return;
		}
		let p = self.session.selection.head;
		if *p.line == 0 && *p.col == 0 {
			return;
		}
		self.save_undo(EditKind::Delete);

		// Auto-pair removal
		if *p.col > 0 {
			if let Some(prev) = self.char_before(p) {
				if let Some(exp) = matching_close(prev) {
					if self.char_at(p) == Some(exp) {
						let cs = self.pos_to_char(CursorPos::new(p.line, p.col.saturating_sub(1usize)));
						self.remove_range(CharIdx(cs), CharIdx(cs + 2));
						self.session.selection =
							Selection::caret(CursorPos::new(p.line, p.col.saturating_sub(1usize)));
						self.post_edit();
						return;
					}
				}
			}
		}

		let (new_pos, ds, de);
		if *p.col == 0 {
			let pl = p.line.saturating_sub(1usize);
			new_pos = CursorPos::new(pl, self.line_len(pl));
			ds = self.pos_to_char(new_pos);
			de = self.pos_to_char(p);
		} else {
			new_pos = CursorPos::new(p.line, p.col.saturating_sub(1usize));
			ds = self.pos_to_char(new_pos);
			de = self.pos_to_char(p);
		}
		self.remove_range(CharIdx(ds), CharIdx(de));
		self.session.selection = Selection::caret(new_pos);
		self.post_edit();
	}

	pub fn delete(&mut self) {
		if self.has_multiple_carets() {
			self.session.desired_col = None;
			self.save_undo(EditKind::Delete);
			let primary = self.session.selection.head;
			let mut edits: Vec<_> = self
				.all_cursor_heads()
				.into_iter()
				.filter_map(|caret| {
					let ci = self.pos_to_char(caret);
					(ci < self.document.rope.len_chars()).then_some((ci, caret))
				})
				.collect();
			edits.sort_by(|a, b| b.0.cmp(&a.0));
			for (ci, _) in &edits {
				self.remove_range(CharIdx(*ci), CharIdx(*ci + 1));
			}
			let secondary = edits
				.iter()
				.map(|(_, caret)| *caret)
				.filter(|&caret| caret != primary)
				.collect();
			self.set_cursor_heads(primary, secondary);
			self.post_edit();
			return;
		}
		self.session.desired_col = None;
		if !self.session.selection.is_caret() {
			self.save_undo(EditKind::Delete);
			self.delete_selection_inner();
			self.post_edit();
			return;
		}
		let ci = self.pos_to_char(self.session.selection.head);
		if ci >= self.document.rope.len_chars() {
			return;
		}
		self.save_undo(EditKind::Delete);
		self.remove_range(CharIdx(ci), CharIdx(ci + 1));
		self.post_edit();
	}

	pub fn delete_word_back(&mut self) {
		if self.has_multiple_carets() {
			self.session.desired_col = None;
			self.save_undo(EditKind::Delete);
			let primary = self.session.selection.head;
			let mut edits: Vec<_> = self
				.all_cursor_heads()
				.into_iter()
				.filter_map(|caret| {
					if *caret.line == 0 && *caret.col == 0 {
						return None;
					}
					let target = self.word_boundary_left(caret);
					Some((
						self.pos_to_char(target),
						self.pos_to_char(caret),
						caret,
						target,
					))
				})
				.collect();
			edits.sort_by(|a, b| b.0.cmp(&a.0));
			for (start, end, _, _) in &edits {
				self.remove_range(CharIdx(*start), CharIdx(*end));
			}

			let secondary = edits
				.iter()
				.map(|(_, _, caret, target)| if *caret == primary { primary } else { *target })
				.filter(|&caret| caret != primary)
				.collect();
			let new_primary = edits
				.iter()
				.find(|(_, _, caret, _)| *caret == primary)
				.map(|(_, _, _, target)| *target)
				.unwrap_or(primary);
			self.set_cursor_heads(new_primary, secondary);
			self.post_edit();
			return;
		}
		self.session.desired_col = None;
		if !self.session.selection.is_caret() {
			self.save_undo(EditKind::Delete);
			self.delete_selection_inner();
			self.post_edit();
			return;
		}
		let p = self.session.selection.head;
		if *p.line == 0 && *p.col == 0 {
			return;
		}
		self.save_undo(EditKind::Delete);
		let t = self.word_boundary_left(p);
		self.remove_range(CharIdx(self.pos_to_char(t)), CharIdx(self.pos_to_char(p)));
		self.session.selection = Selection::caret(t);
		self.post_edit();
	}

	pub fn delete_word_forward(&mut self) {
		if self.has_multiple_carets() {
			self.session.desired_col = None;
			self.save_undo(EditKind::Delete);
			let primary = self.session.selection.head;
			let mut edits: Vec<_> = self
				.all_cursor_heads()
				.into_iter()
				.filter_map(|caret| {
					if self.pos_to_char(caret) >= self.document.rope.len_chars() {
						return None;
					}
					let target = self.word_boundary_right(caret);
					Some((self.pos_to_char(caret), self.pos_to_char(target), caret))
				})
				.collect();
			edits.sort_by(|a, b| b.0.cmp(&a.0));
			for (start, end, _) in &edits {
				self.remove_range(CharIdx(*start), CharIdx(*end));
			}

			let secondary = edits
				.iter()
				.map(|(_, _, caret)| *caret)
				.filter(|&caret| caret != primary)
				.collect();
			self.set_cursor_heads(primary, secondary);
			self.post_edit();
			return;
		}
		self.session.desired_col = None;
		if !self.session.selection.is_caret() {
			self.save_undo(EditKind::Delete);
			self.delete_selection_inner();
			self.post_edit();
			return;
		}
		let p = self.session.selection.head;
		if self.pos_to_char(p) >= self.document.rope.len_chars() {
			return;
		}
		self.save_undo(EditKind::Delete);
		let t = self.word_boundary_right(p);
		self.remove_range(CharIdx(self.pos_to_char(p)), CharIdx(self.pos_to_char(t)));
		self.post_edit();
	}

	pub fn duplicate_line(&mut self) {
		if self.has_multiple_carets() {
			self.save_undo_boundary();
			let primary = self.session.selection.head;
			let mut lines: Vec<LineIdx> = self
				.all_cursor_heads()
				.into_iter()
				.map(|p| p.line)
				.collect();
			lines.sort_unstable();
			lines.dedup();
			for &line in lines.iter().rev() {
				let text = self.line_text(line);
				let line_start = self.document.rope.line_to_char(*line);
				let line_chars = self.document.rope.line(*line).len_chars();
				let insert_at = line_start + line_chars;
				let insert = if insert_at >= self.document.rope.len_chars() {
					format!("\n{}", text)
				} else {
					format!("{}\n", text)
				};
				self.insert_char_at(CharIdx(insert_at), &insert);
			}
			let shifted = |caret: CursorPos| {
				let offset = lines.iter().filter(|&&line| line <= caret.line).count();
				CursorPos::new(caret.line + offset, caret.col)
			};
			let secondary = self
				.session
				.secondary_selections
				.iter()
				.map(|sel| shifted(sel.head))
				.collect();
			self.set_cursor_heads(shifted(primary), secondary);
			self.post_edit();
			return;
		}
		self.save_undo_boundary();
		let l = self.session.selection.head.line;
		let t = self.line_text(l);
		let ls = self.document.rope.line_to_char(*l);
		let lc = self.document.rope.line(*l).len_chars();
		let at = ls + lc;
		let ins = if at >= self.document.rope.len_chars() {
			format!("\n{}", t)
		} else {
			format!("{}\n", t)
		};
		self.insert_char_at(CharIdx(at), &ins);
		self.session.selection =
			Selection::caret(CursorPos::new(l + 1, self.session.selection.head.col));
		self.post_edit();
	}

	fn delete_selection_inner(&mut self) {
		if self.session.selection.is_caret() {
			return;
		}
		let (s, e) = self.session.selection.ordered();
		self.remove_range(CharIdx(self.pos_to_char(s)), CharIdx(self.pos_to_char(e)));
		self.session.selection = Selection::caret(s);
	}

	// ── Search ────────────────────────────────────────────────────────────

	pub fn search_open(&mut self) {
		self.session.search.is_open = true;
		// Pre-fill with selected text
		let sel = self.selected_text();
		if !sel.is_empty() && !sel.contains('\n') {
			self.session.search.query = sel;
		}
		self.session.search.find_all(&self.document.rope);
	}

	pub fn search_close(&mut self) {
		self.session.search.is_open = false;
		self.session.search.matches.clear();
	}

	pub fn search_update_query(&mut self, query: &str) {
		self.session.search.query = query.to_string();
		self.session.search.find_all(&self.document.rope);
	}

	pub fn search_next(&mut self) {
		self.session.search.next_match();
		self.jump_to_current_match();
	}

	pub fn search_update_replacement(&mut self, replacement: &str) {
		self.session.search.replacement = replacement.to_string();
	}

	pub fn search_prev(&mut self) {
		self.session.search.prev_match();
		self.jump_to_current_match();
	}

	pub fn search_replace_current(&mut self) {
		self.save_undo_boundary();
		if let Some(m) = self.session.search.current().cloned() {
			let replacement = self.session.search.replacement.clone();
			self.replace_range(m.char_start, m.char_end, &replacement);
			self.post_edit();
		}
	}

	pub fn search_replace_all(&mut self) {
		self.save_undo_boundary();
		let count = self.session.search.replace_all(&mut self.document.rope);
		if count > 0 {
			self.post_edit();
		}
	}

	fn jump_to_current_match(&mut self) {
		if let Some(m) = self.session.search.current() {
			self.session.selection = Selection {
				anchor: CursorPos::new(m.line, m.col_start),
				head: CursorPos::new(m.line, m.col_end),
			};
		}
	}

	/// Search for `word` without opening the panel (used by `*` / `#`).
	/// Jumps to the nearest match at or after the cursor.
	pub fn search_star(&mut self, word: &str, forward: bool) {
		self.session.search.query = word.to_string();
		self.session.search.find_all(&self.document.rope);
		let ci = self.pos_to_char(self.session.selection.head);
		if forward {
			self.session.search.jump_to_nearest(ci + 1);
		} else {
			// Jump to the match just before current pos
			if !self.session.search.matches.is_empty() {
				let n = self.session.search.matches.len();
				self.session.search.current_match = (0..n)
					.rev()
					.find(|&i| self.session.search.matches[i].char_start < ci)
					.unwrap_or(n - 1);
			}
		}
		self.jump_to_current_match();
	}

	/// Replace the character under the cursor with `ch`, leaving the cursor on it.
	pub fn replace_char(&mut self, ch: char) {
		let pos = self.session.selection.head;
		if pos.col >= self.line_len(pos.line) {
			return;
		}
		self.save_undo(EditKind::Delete);
		let ci = self.pos_to_char(pos);
		self.replace_range(ci, ci + 1, &ch.to_string());
		let new_pos = if ch == '\n' {
			CursorPos::new(pos.line + 1, 0)
		} else {
			pos
		};
		self.session.selection = Selection::caret(new_pos);
		self.post_edit();
	}

	/// Return the word (alphanumeric + `_`) under the cursor, or `None`.
	pub fn word_under_cursor(&self) -> Option<String> {
		let pos = self.session.selection.head;
		let text = self.line_text(pos.line);
		let chars: Vec<char> = text.chars().collect();
		if *pos.col >= chars.len() {
			return None;
		}
		let is_word = |c: char| c.is_alphanumeric() || c == '_';
		if !is_word(chars[*pos.col]) {
			return None;
		}
		let mut start = *pos.col;
		while start > 0 && is_word(chars[start - 1]) {
			start -= 1;
		}
		let mut end = *pos.col + 1;
		while end < chars.len() && is_word(chars[end]) {
			end += 1;
		}
		Some(chars[start..end].iter().collect())
	}

	// ── Folding ───────────────────────────────────────────────────────────

	pub fn toggle_fold(&mut self, line: LineIdx) {
		self.document.folds.toggle(line);
		self.refresh_visual_lines();
	}

	// ── Wrapping ──────────────────────────────────────────────────────────

	pub fn set_wrap(&mut self, enabled: bool) {
		self.document.wrap_config.enabled = enabled;
		self.refresh_visual_lines();
	}

	pub fn set_wrap_col(&mut self, col: CharIdx) {
		self.document.wrap_config.wrap_col = col;
		if self.document.wrap_config.enabled {
			self.refresh_visual_lines();
		}
	}

	fn move_caret_left(&self, p: CursorPos) -> CursorPos {
		if *p.col > 0 {
			CursorPos::new(p.line, p.col.saturating_sub(1usize))
		} else if *p.line > 0 {
			let pl = p.line.saturating_sub(1usize);
			CursorPos::new(pl, self.line_len(pl))
		} else {
			p
		}
	}

	fn move_caret_right(&self, p: CursorPos) -> CursorPos {
		let ll = self.line_len(p.line);
		if p.col < ll {
			CursorPos::new(p.line, p.col + 1)
		} else if *p.line < *self.line_count() - 1 {
			CursorPos::new(p.line + 1, 0)
		} else {
			p
		}
	}

	fn move_caret_up(&self, p: CursorPos, target_col: CharIdx) -> CursorPos {
		if *p.line == 0 {
			return p;
		}
		let mut line = p.line.saturating_sub(1usize);
		while *line > 0 && self.document.folds.is_hidden(line) {
			line -= 1;
		}
		CursorPos::new(line, target_col.min(self.line_len(line)))
	}

	fn move_caret_down(&self, p: CursorPos, target_col: CharIdx) -> CursorPos {
		if *p.line >= *self.line_count() - 1 {
			return p;
		}
		let mut line = p.line + 1;
		let max = self.line_count().saturating_sub(1usize);
		while line < max && self.document.folds.is_hidden(line) {
			line += 1;
		}
		CursorPos::new(line, target_col.min(self.line_len(line)))
	}

	pub fn add_caret_above(&mut self) {
		if !self.session.selection.is_caret() {
			return;
		}
		let mut secondary: Vec<_> = self
			.session
			.secondary_selections
			.iter()
			.map(|sel| sel.head)
			.collect();
		for caret in self.all_cursor_heads() {
			let next = self.move_caret_up(caret, caret.col);
			if next != caret && next != self.session.selection.head {
				secondary.push(next);
			}
		}
		self.set_cursor_heads(self.session.selection.head, secondary);
	}

	pub fn add_caret_below(&mut self) {
		if !self.session.selection.is_caret() {
			return;
		}
		let mut secondary: Vec<_> = self
			.session
			.secondary_selections
			.iter()
			.map(|sel| sel.head)
			.collect();
		for caret in self.all_cursor_heads() {
			let next = self.move_caret_down(caret, caret.col);
			if next != caret && next != self.session.selection.head {
				secondary.push(next);
			}
		}
		self.set_cursor_heads(self.session.selection.head, secondary);
	}

	// ── Navigation ────────────────────────────────────────────────────────

	pub fn move_left(&mut self, extend: bool) {
		if self.has_multiple_carets() && extend {
			self.session.desired_col = None;
			self.map_all_selection_heads(|buf, sel| buf.move_caret_left(sel.head));
			return;
		}
		if self.has_multiple_carets() && !extend {
			self.session.desired_col = None;
			self.map_secondary_cursor_heads(|buf, p| buf.move_caret_left(p));
			return;
		}
		self.session.desired_col = None;
		if !extend && !self.session.selection.is_caret() {
			let (s, _) = self.session.selection.ordered();
			self.session.selection = Selection::caret(s);
			self.refresh_bracket_match();
			return;
		}
		let p = self.session.selection.head;
		let n = if *p.col > 0 {
			CursorPos::new(p.line, p.col.saturating_sub(1usize))
		} else if *p.line > 0 {
			let pl = p.line.saturating_sub(1usize);
			CursorPos::new(pl, self.line_len(pl))
		} else {
			p
		};
		self.set_head(n, extend);
	}

	pub fn move_right(&mut self, extend: bool) {
		if self.has_multiple_carets() && extend {
			self.session.desired_col = None;
			self.map_all_selection_heads(|buf, sel| buf.move_caret_right(sel.head));
			return;
		}
		if self.has_multiple_carets() && !extend {
			self.session.desired_col = None;
			self.map_secondary_cursor_heads(|buf, p| buf.move_caret_right(p));
			return;
		}
		self.session.desired_col = None;
		if !extend && !self.session.selection.is_caret() {
			let (_, e) = self.session.selection.ordered();
			self.session.selection = Selection::caret(e);
			self.refresh_bracket_match();
			return;
		}
		let p = self.session.selection.head;
		let ll = self.line_len(p.line);
		let n = if p.col < ll {
			CursorPos::new(p.line, p.col + 1)
		} else if *p.line < *self.line_count() - 1 {
			CursorPos::new(p.line + 1, 0)
		} else {
			p
		};
		self.set_head(n, extend);
	}

	pub fn move_up(&mut self, extend: bool) {
		if self.has_multiple_carets() && extend {
			let target_col = self
				.session
				.desired_col
				.unwrap_or(self.session.selection.head.col);
			self.map_all_selection_heads(|buf, sel| buf.move_caret_up(sel.head, target_col));
			self.session.desired_col = Some(target_col);
			return;
		}
		if self.has_multiple_carets() && !extend {
			let target_col = self
				.session
				.desired_col
				.unwrap_or(self.session.selection.head.col);
			self.map_secondary_cursor_heads(|buf, p| buf.move_caret_up(p, target_col));
			self.session.desired_col = Some(target_col);
			return;
		}
		let p = self.session.selection.head;
		if *p.line == 0 {
			return;
		}
		let tc = self.session.desired_col.unwrap_or(p.col);
		// Skip folded lines
		let mut nl = p.line.saturating_sub(1usize);
		while *nl > 0 && self.document.folds.is_hidden(nl) {
			nl -= 1;
		}
		let nc = tc.min(self.line_len(nl));
		self.set_head(CursorPos::new(nl, nc), extend);
		self.session.desired_col = Some(tc);
	}

	pub fn move_down(&mut self, extend: bool) {
		if self.has_multiple_carets() && extend {
			let target_col = self
				.session
				.desired_col
				.unwrap_or(self.session.selection.head.col);
			self.map_all_selection_heads(|buf, sel| buf.move_caret_down(sel.head, target_col));
			self.session.desired_col = Some(target_col);
			return;
		}
		if self.has_multiple_carets() && !extend {
			let target_col = self
				.session
				.desired_col
				.unwrap_or(self.session.selection.head.col);
			self.map_secondary_cursor_heads(|buf, p| buf.move_caret_down(p, target_col));
			self.session.desired_col = Some(target_col);
			return;
		}
		let p = self.session.selection.head;
		if *p.line >= *self.line_count() - 1 {
			return;
		}
		let tc = self.session.desired_col.unwrap_or(p.col);
		let mut nl = p.line + 1;
		let max = self.line_count().saturating_sub(1usize);
		while nl < max && self.document.folds.is_hidden(nl) {
			nl += 1;
		}
		let nc = tc.min(self.line_len(nl));
		self.set_head(CursorPos::new(nl, nc), extend);
		self.session.desired_col = Some(tc);
	}

	pub fn move_home(&mut self, extend: bool) {
		if self.has_multiple_carets() && extend {
			self.session.desired_col = None;
			self.map_all_selection_heads(|buf, sel| {
				let first = buf
					.line_text(sel.head.line)
					.chars()
					.position(|c| !c.is_whitespace())
					.unwrap_or(0);
				let col = if *sel.head.col <= first && *sel.head.col != 0 {
					CharIdx(0)
				} else {
					CharIdx(first)
				};
				CursorPos::new(sel.head.line, col)
			});
			return;
		}
		if self.has_multiple_carets() && !extend {
			self.session.desired_col = None;
			self.map_secondary_cursor_heads(|buf, p| {
				let first = buf
					.line_text(p.line)
					.chars()
					.position(|c| !c.is_whitespace())
					.unwrap_or(0);
				let col = if *p.col <= first && *p.col != 0 {
					CharIdx(0)
				} else {
					CharIdx(first)
				};
				CursorPos::new(p.line, col)
			});
			return;
		}
		self.session.desired_col = None;
		let p = self.session.selection.head;
		let first = self
			.line_text(p.line)
			.chars()
			.position(|c| !c.is_whitespace())
			.unwrap_or(0);
		let col = if *p.col <= first && *p.col != 0 {
			CharIdx(0)
		} else {
			CharIdx(first)
		};
		self.set_head(CursorPos::new(p.line, col), extend);
	}

	pub fn move_end(&mut self, extend: bool) {
		if self.has_multiple_carets() && extend {
			self.session.desired_col = None;
			self.map_all_selection_heads(|buf, sel| {
				CursorPos::new(sel.head.line, buf.line_len(sel.head.line))
			});
			return;
		}
		if self.has_multiple_carets() && !extend {
			self.session.desired_col = None;
			self.map_secondary_cursor_heads(|buf, p| CursorPos::new(p.line, buf.line_len(p.line)));
			return;
		}
		self.session.desired_col = None;
		let p = self.session.selection.head;
		self.set_head(CursorPos::new(p.line, self.line_len(p.line)), extend);
	}

	pub fn move_to_start(&mut self, extend: bool) {
		self.session.desired_col = None;
		self.set_head(CursorPos::zero(), extend);
	}

	pub fn move_to_end(&mut self, extend: bool) {
		self.session.desired_col = None;
		let l = self.line_count().saturating_sub(1usize);
		self.set_head(CursorPos::new(l, self.line_len(l)), extend);
	}

	pub fn move_word_left(&mut self, extend: bool) {
		if self.has_multiple_carets() {
			self.session.desired_col = None;
			self.map_all_selection_heads(|buf, sel| buf.word_boundary_left(sel.head));
			return;
		}
		self.session.desired_col = None;
		let p = self.session.selection.head;
		self.set_head(self.word_boundary_left(p), extend);
	}

	pub fn move_word_right(&mut self, extend: bool) {
		if self.has_multiple_carets() {
			self.session.desired_col = None;
			self.map_all_selection_heads(|buf, sel| buf.word_boundary_right(sel.head));
			return;
		}
		self.session.desired_col = None;
		let p = self.session.selection.head;
		self.set_head(self.word_boundary_right(p), extend);
	}

	pub fn page_up(&mut self, vis: usize, extend: bool) {
		let p = self.session.selection.head;
		let tc = self.session.desired_col.unwrap_or(p.col);
		let nl = p.line.saturating_sub(vis);
		self.set_head(CursorPos::new(nl, tc.min(self.line_len(nl))), extend);
		self.session.desired_col = Some(tc);
	}

	pub fn page_down(&mut self, vis: usize, extend: bool) {
		let p = self.session.selection.head;
		let tc = self.session.desired_col.unwrap_or(p.col);
		let nl = (p.line + vis).min(self.line_count().saturating_sub(1usize));
		self.set_head(CursorPos::new(nl, tc.min(self.line_len(nl))), extend);
		self.session.desired_col = Some(tc);
	}

	pub fn select_all(&mut self) {
		self.clear_secondary_selections();
		let l = self.line_count().saturating_sub(1usize);
		self.session.selection = Selection {
			anchor: CursorPos::zero(),
			head: CursorPos::new(l, self.line_len(l)),
		};
	}

	pub fn select_word_at(&mut self, p: CursorPos) {
		let text = self.line_text(p.line);
		let chars: Vec<char> = text.chars().collect();
		if *p.col >= chars.len() {
			self.session.selection = Selection::caret(p);
			return;
		}
		let is_word = |c: char| c.is_alphanumeric() || c == '_';
		if !is_word(chars[*p.col]) {
			self.session.selection = Selection::caret(p);
			return;
		}
		let mut start = *p.col;
		while start > 0 && is_word(chars[start - 1]) {
			start -= 1;
		}
		let mut end = *p.col + 1;
		while end < chars.len() && is_word(chars[end]) {
			end += 1;
		}
		self.session.selection = Selection {
			anchor: CursorPos::new(p.line, CharIdx(start)),
			head: CursorPos::new(p.line, CharIdx(end)),
		};
	}

	pub fn select_line(&mut self, line: LineIdx) {
		self.clear_secondary_selections();
		let l = line.min(self.line_count().saturating_sub(1usize));
		self.session.selection = Selection {
			anchor: CursorPos::new(l, 0),
			head: CursorPos::new(l, self.line_len(l)),
		};
	}

	pub fn move_to_char(&mut self, target: char, before: bool, extend: bool) {
		let line = self.session.selection.head.line;
		let col = self.session.selection.head.col;
		let lt = self.line_text(line);
		let chars: Vec<char> = lt.chars().collect();

		let mut result = None;
		for i in (*col + 1)..chars.len() {
			if chars[i] == target {
				result = Some(if before { i.saturating_sub(1usize) } else { i });
				break;
			}
		}

		if let Some(d) = result {
			self.set_head(CursorPos::new(line, CharIdx(d)), extend);
		}
	}

	pub fn move_to_char_back(&mut self, target: char, before: bool, extend: bool) {
		let line = self.session.selection.head.line;
		let col = self.session.selection.head.col;
		let lt = self.line_text(line);
		let chars: Vec<char> = lt.chars().collect();

		let mut result = None;
		for i in (0..*col).rev() {
			if chars[i] == target {
				result = Some(if before { i + 1 } else { i });
				break;
			}
		}

		if let Some(d) = result {
			self.set_head(CursorPos::new(line, CharIdx(d)), extend);
		}
	}

	pub fn set_head(&mut self, p: CursorPos, extend: bool) {
		if !extend {
			self.clear_secondary_selections();
		}
		if extend {
			self.session.selection.head = p;
		} else {
			self.session.selection = Selection::caret(p);
		}
		self.refresh_bracket_match();
	}

	pub fn click_to_pos(&self, line: LineIdx, col: CharIdx) -> CursorPos {
		self.clamp_pos(CursorPos::new(line, col))
	}

	// ── Word boundaries ───────────────────────────────────────────────────

	fn word_boundary_left(&self, p: CursorPos) -> CursorPos {
		if *p.col == 0 {
			if *p.line == 0 {
				return p;
			}
			let pl = p.line.saturating_sub(1usize);
			return CursorPos::new(pl, self.line_len(pl));
		}
		let chars: Vec<char> = self.line_text(p.line).chars().collect();
		let mut c = (*p.col).min(chars.len());
		let is_w = |ch: char| ch.is_alphanumeric() || ch == '_';
		while c > 0 && chars[c - 1].is_whitespace() {
			c -= 1;
		}
		if c > 0 && is_w(chars[c - 1]) {
			while c > 0 && is_w(chars[c - 1]) {
				c -= 1;
			}
		} else if c > 0 {
			c -= 1;
		}
		CursorPos::new(p.line, CharIdx(c))
	}

	fn word_boundary_right(&self, p: CursorPos) -> CursorPos {
		let ll = self.line_len(p.line);
		if p.col >= ll {
			if *p.line >= *self.line_count() - 1 {
				return p;
			}
			return CursorPos::new(p.line + 1, 0);
		}
		let chars: Vec<char> = self.line_text(p.line).chars().collect();
		let mut c = *p.col;
		let is_w = |ch: char| ch.is_alphanumeric() || ch == '_';
		if c < chars.len() && is_w(chars[c]) {
			while c < chars.len() && is_w(chars[c]) {
				c += 1;
			}
		} else if c < chars.len() && !chars[c].is_whitespace() {
			c += 1;
		}
		while c < chars.len() && chars[c].is_whitespace() {
			c += 1;
		}
		CursorPos::new(p.line, CharIdx(c))
	}

	// ── Bracket matching ──────────────────────────────────────────────────

	fn update_bracket_match(&mut self) {
		self.session.matched_bracket = None;
		let p = self.session.selection.head;
		let text = self.line_text(p.line);
		let chars: Vec<char> = text.chars().collect();
		for &col in &[*p.col, (*p.col).wrapping_sub(1)] {
			if col < chars.len() {
				let ch = chars[col];
				if is_open_bracket(ch) {
					if let Some((ml, mc)) = self.find_close(p.line, CharIdx(col), ch) {
						self.session.matched_bracket = Some(BracketPair {
							open_line: p.line,
							open_col: CharIdx(col),
							close_line: ml,
							close_col: mc,
						});
						return;
					}
				} else if is_close_bracket(ch) {
					if let Some((ml, mc)) = self.find_open(p.line, CharIdx(col), ch) {
						self.session.matched_bracket = Some(BracketPair {
							open_line: ml,
							open_col: mc,
							close_line: p.line,
							close_col: CharIdx(col),
						});
						return;
					}
				}
			}
		}
	}

	fn find_close(&self, sl: LineIdx, sc: CharIdx, open: char) -> Option<(LineIdx, CharIdx)> {
		let close = matching_close(open)?;
		let mut d = 0i32;
		for l_raw in *sl..*self.line_count() {
			let l = LineIdx(l_raw);
			let cs: Vec<char> = self.line_text(l).chars().collect();
			for c in (if l == sl { *sc } else { 0 })..cs.len() {
				if cs[c] == open {
					d += 1;
				} else if cs[c] == close {
					d -= 1;
					if d == 0 {
						return Some((l, CharIdx(c)));
					}
				}
			}
		}
		None
	}

	fn find_open(&self, sl: LineIdx, sc: CharIdx, close: char) -> Option<(LineIdx, CharIdx)> {
		let open = matching_open(close)?;
		let mut d = 0i32;
		for l_raw in (0..=*sl).rev() {
			let l = LineIdx(l_raw);
			let cs: Vec<char> = self.line_text(l).chars().collect();
			let end = if l == sl {
				*sc
			} else {
				cs.len().saturating_sub(1usize)
			};
			for c in (0..=end).rev() {
				if c >= cs.len() {
					continue;
				}
				if cs[c] == close {
					d += 1;
				} else if cs[c] == open {
					d -= 1;
					if d == 0 {
						return Some((l, CharIdx(c)));
					}
				}
			}
		}
		None
	}

	// ── Indent guides ─────────────────────────────────────────────────────

	/// Returns visual column positions of indent guides for this line.
	pub fn indent_guides(&self, line: LineIdx) -> Vec<usize> {
		let text = self.line_text(line);
		// Count leading whitespace in visual columns (tabs = TAB_WIDTH, spaces = 1).
		let mut vcol = 0usize;
		for ch in text.chars() {
			match ch {
				'\t' => vcol = (vcol / TAB_WIDTH + 1) * TAB_WIDTH,
				' ' => vcol += 1,
				_ => break,
			}
		}
		let mut g = Vec::new();
		let mut c = TAB_WIDTH;
		while c <= vcol {
			g.push(c);
			c += TAB_WIDTH;
		}
		g
	}

	// ── Vim :substitute ───────────────────────────────────────────────────

	/// Apply a vim-style substitution to lines `first..=last`.
	/// `pattern` is a Rust regex. `replacement` supports vim escapes:
	/// `&` = whole match, `\1`–`\9` = capture groups, `\t` = tab, `\n` = newline, `\\` = backslash.
	/// Returns the number of lines changed.
	pub fn substitute(
		&mut self,
		first: LineIdx,
		last: LineIdx,
		pattern: &str,
		replacement: &str,
		global: bool,
		case_insensitive: bool,
	) -> usize {
		let re = match RegexBuilder::new(pattern)
			.case_insensitive(case_insensitive)
			.build()
		{
			Ok(r) => r,
			Err(_) => return 0,
		};

		let rep = replacement.to_string();
		let last = last.min(self.line_count().saturating_sub(1usize));

		self.save_undo(EditKind::Other);

		let mut changed = 0usize;
		// Process bottom-to-top so rope char indices above stay valid.
		for line_raw in (*first..=*last).rev() {
			let line = LineIdx(line_raw);
			let text = self.line_text(line);
			let new_text = if global {
				re.replace_all(&text, |caps: &Captures| apply_vim_replacement(&rep, caps))
					.into_owned()
			} else {
				re.replace(&text, |caps: &Captures| apply_vim_replacement(&rep, caps))
					.into_owned()
			};
			if new_text == text {
				continue;
			}

			// Splice just the content portion of the line (leave the newline).
			let line_start = self.document.rope.line_to_char(*line);
			let content_end = line_start + self.line_text(line).chars().count();
			self.replace_range(line_start, content_end, &new_text);
			changed += 1;
		}

		if changed > 0 {
			self.post_edit();
		}
		changed
	}
}

fn apply_vim_replacement(rep: &str, caps: &Captures) -> String {
	let mut out = String::new();
	let mut chars = rep.chars().peekable();
	while let Some(ch) = chars.next() {
		match ch {
			'\\' => match chars.next() {
				Some('t') => out.push('\t'),
				Some('n') => out.push('\n'),
				Some(c @ '0'..='9') => {
					let n = c.to_digit(10).unwrap() as usize;
					out.push_str(caps.get(n).map_or("", |m| m.as_str()));
				}
				Some(c) => {
					out.push('\\');
					out.push(c);
				}
				None => out.push('\\'),
			},
			'&' => out.push_str(caps.get(0).map_or("", |m| m.as_str())),
			c => out.push(c),
		}
	}
	out
}
