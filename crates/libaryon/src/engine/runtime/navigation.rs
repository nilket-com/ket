use super::super::core::refresh_sum_tree;
use super::super::document::cursor::{
	next_word_end, next_word_start, page_motion_step, place_cursor_at_line_col, prev_word_start,
	smart_home_visual_col, step_left_visual_col, step_right_visual_col,
};
use super::super::document::io::read_loaded_document;
use super::super::document::parse::{find_matching_delimiter_byte, word_object_delta_at_cursor};
use super::super::folding::{
	FoldSet, clamp_cursor_line_to_viewport, doc_line_for_visual_index, max_visual_line_index,
	scroll_viewport, visual_line_index_for_doc_line,
};
use super::super::layout::{
	ViewportAnchor, clamp_cursor_to_viewport_anchor_with_sum_tree,
	scroll_viewport_anchor_with_sum_tree, viewport_geometry_for_viewport_with_numbering,
};
use super::super::protocol::{
	EditorCommand, EditorMode, MoveDirection, VerticalDirection, VisualKind,
};
use super::super::session::{
	AddCaretResult, Affinity, CachedSumTree, Caret, FileBrowserEditorMut, TextEditorMut, UiState,
};
use super::super::support::{
	first_non_whitespace_visual_col, line_end_byte_sparse, line_end_visual_col,
	read_line_bytes_sparse,
};
use super::super::uast_query::{UastQuery, is_function_scope};
use super::super::undo::line_col_from_byte_offset;
use super::CommandEffect;
use crate::core::{
	DisplayRow, DocByte, DocDisplayCol, DocLine, DocVisualRow, EDITOR_CHROME_ROWS, StateId,
	TAB_SIZE, TopologyRev,
};
use crate::ecs::{NodeId, UastRegistry};
use crate::uast::{LineLookupState, UastProjection};

/// Outcome of a single-line character search (`f`/`F`/`t`/`T`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FindCharOutcome {
	/// `ch` was found on the line at a position that would actually move
	/// the cursor. The new target visual column is attached.
	Moved(DocDisplayCol),
	/// `ch` was found on the line, but every matching position would leave
	/// the cursor where it already is (the classic `tb` on `"ab"` from the
	/// `a` case for the exclusive motions). The caller should silently do
	/// nothing — reporting "not found" would be a lie.
	NoMove,
	/// `ch` does not occur on the line in the requested direction at all.
	NotFound,
}

/// Search the current line's content for `ch` and return the outcome that
/// the caller should apply to the cursor, respecting direction and
/// inclusivity.
///
/// - `forward` true = search right (f/t), false = search left (F/T).
/// - `inclusive` true = land on the matched char (f/F), false = land one
///   position short of it (t/T).
/// - The search is line-local: it never crosses a newline.
///
/// The column model matches the rest of the editor (see
/// [`crate::engine::support::advance_visual_col_only`] and
/// [`crate::engine::undo::line_col_from_byte_offset`]): every byte of a
/// non-tab character advances the visual column by one, so multi-byte
/// UTF-8 scalars span multiple visual columns. Iterating with
/// `.chars()` and adding `c.len_utf8()` per step keeps this helper in
/// agreement with the byte-oriented stack while still letting us match
/// complete Unicode scalars against the target `ch`.
pub(crate) fn find_char_target_col(
	line_bytes: &[u8],
	current_col: DocDisplayCol,
	ch: char,
	forward: bool,
	inclusive: bool,
) -> FindCharOutcome {
	let Ok(line_str) = std::str::from_utf8(line_bytes) else {
		return FindCharOutcome::NotFound;
	};
	let content = line_str.strip_suffix('\n').unwrap_or(line_str);

	// Build per-character entries: (visual-col at the char's start, the char).
	let mut col = DocDisplayCol::ZERO;
	let mut chars: Vec<(DocDisplayCol, char)> = Vec::new();
	for c in content.chars() {
		let start_col = col;
		if c == '\t' {
			col = col
				.checked_add(TAB_SIZE - (col.get() % TAB_SIZE))
				.expect("DocDisplayCol addition overflow");
		} else {
			col = col
				.checked_add(c.len_utf8() as u32)
				.expect("DocDisplayCol addition overflow");
		}
		chars.push((start_col, c));
	}

	if chars.is_empty() {
		return FindCharOutcome::NotFound;
	}

	// Locate the current character. When `current_col` lands between columns
	// (inside a tab expansion or mid-codepoint), pick the character whose
	// start-col is the largest value <= current_col, so `cursor_abs_col`
	// always resolves.
	let current_idx = chars
		.iter()
		.rposition(|(c, _)| *c <= current_col)
		.unwrap_or(0);

	// Track whether we saw any match at all so the caller can distinguish
	// "nothing to find" from "found but every candidate was a no-op".
	let mut any_match = false;
	if forward {
		let start = current_idx + 1;
		for (i, (_, c)) in chars.iter().enumerate().skip(start) {
			if *c == ch {
				any_match = true;
				let target_idx = if inclusive {
					i
				} else if i == 0 {
					continue;
				} else {
					i - 1
				};
				let target_col = chars[target_idx].0;
				if target_col == current_col {
					// This match exists but would be a no-op — look past
					// it for a truly actionable match before giving up.
					continue;
				}
				return FindCharOutcome::Moved(target_col);
			}
		}
	} else if current_idx > 0 {
		for i in (0..current_idx).rev() {
			if chars[i].1 == ch {
				any_match = true;
				let target_idx = if inclusive { i } else { i + 1 };
				if target_idx >= chars.len() {
					continue;
				}
				let target_col = chars[target_idx].0;
				if target_col == current_col {
					continue;
				}
				return FindCharOutcome::Moved(target_col);
			}
		}
	}

	if any_match {
		FindCharOutcome::NoMove
	} else {
		FindCharOutcome::NotFound
	}
}

pub(super) fn handle_navigation_doctab_command(
	registry: &UastRegistry,
	tab: &mut TextEditorMut<'_>,
	ui: &mut UiState,
	command: EditorCommand,
) -> CommandEffect {
	{
		let root = tab.document.root_id;
		let topology_rev = tab.document.topology_rev;
		let state_id = tab.document.ledger.current_state_id;
		let mut pending = false;
		let folds = &mut tab.view.folds;
		tab.cursor.visit_carets_mut(|caret| {
			pending |= !caret.resolve(registry, root, folds, topology_rev, state_id);
		});
		let can_replace_pending_target = matches!(
			command,
			EditorCommand::AddCursorAt(_)
				| EditorCommand::AddCursorAdjacent(_)
				| EditorCommand::ClickCursor(_)
				| EditorCommand::GotoLine(_)
				| EditorCommand::CollapseCursors
				| EditorCommand::Resize(_, _)
				| EditorCommand::ScrollViewport(_)
		);
		if pending && !can_replace_pending_target {
			ui.status_message = Some("Loading cursor extent".to_string());
			return CommandEffect::render();
		}
	}
	if tab.cursor.len() > 1 && multi_cursor_navigation_supported(&command) {
		return move_all_cursors(registry, tab, ui, command);
	}
	match command {
		EditorCommand::CollapseCursors => {
			tab.cursor.collapse();
			return CommandEffect::render();
		}
		EditorCommand::AddCursorAt(target) => {
			return add_cursor_at(registry, tab, ui, target);
		}
		EditorCommand::AddCursorAdjacent(direction) => {
			return add_cursor_adjacent(registry, tab, ui, direction);
		}
		EditorCommand::ClickCursor(_) => tab.cursor.collapse(),
		EditorCommand::GotoLine(_)
		| EditorCommand::MatchDelimiter
		| EditorCommand::SelectInnerWord
		| EditorCommand::SelectAroundFunction
		| EditorCommand::MoveCursor(MoveDirection::Top | MoveDirection::Bottom) => tab.cursor.collapse(),
		_ => {}
	}
	let reset_preferred_col = matches!(
		command,
		EditorCommand::ClickCursor(_)
			| EditorCommand::GotoLine(_)
			| EditorCommand::LineStart
			| EditorCommand::FirstNonWhitespace
			| EditorCommand::LineEnd
			| EditorCommand::SmartHome
			| EditorCommand::AppendAtLineEnd
			| EditorCommand::MoveCursor(
				MoveDirection::Left
					| MoveDirection::Right
					| MoveDirection::NextWord
					| MoveDirection::PrevWord
					| MoveDirection::NextWordEnd
					| MoveDirection::Top
					| MoveDirection::Bottom
					| MoveDirection::FindChar { .. }
			)
	);
	let mut active_visual = tab.cursor.active_visual();
	let gutter_numbering = tab.gutter_numbering();
	let root = tab.document.root_id;
	let root_id = Some(root);
	let topology_rev = tab.document.topology_rev;
	let state_id = tab.document.ledger.current_state_id;
	if !tab.cursor.primary_mut().resolve(
		registry,
		root,
		&mut tab.view.folds,
		topology_rev,
		state_id,
	) {
		return CommandEffect::render();
	}
	let effect = {
		let super::super::session::TextViewState { folds, common } = &mut *tab.view;
		handle_navigation_command(
			registry,
			root_id,
			folds,
			tab.cursor.primary_mut(),
			&mut active_visual,
			&mut common.scroll_y,
			&mut common.scroll_row_offset,
			&mut common.viewport_width,
			&mut common.viewport_lines,
			common.wrap_enabled,
			common.minimap_enabled,
			gutter_numbering,
			topology_rev,
			tab.document.ledger.current_state_id,
			&mut tab.document.semantic.cached_sum_tree,
			&mut ui.status_message,
			&mut ui.mode_override,
			command,
		)
	};
	tab.cursor.primary_mut().resolve(
		registry,
		root,
		&mut tab.view.folds,
		topology_rev,
		tab.document.ledger.current_state_id,
	);
	tab.cursor.set_active_visual(active_visual);
	if reset_preferred_col && let Some(cursors) = tab.cursor.points_mut() {
		let abs_col = cursors.primary().abs_col();
		cursors.primary_mut().preferred_col = abs_col;
	}
	if let Some(cursors) = tab.cursor.points_mut() {
		let root = tab.document.root_id;
		cursors.normalize(registry, root);
	}
	effect
}

/// File-browser rows are backed by a synthetic UAST, but use the same cursor
/// and viewport machinery as text documents. Keep their navigation on that
/// shared path instead of silently dropping it at the text-buffer boundary.
pub(super) fn handle_file_browser_navigation_command(
	registry: &UastRegistry,
	tab: &mut FileBrowserEditorMut<'_>,
	ui: &mut UiState,
	command: EditorCommand,
) -> CommandEffect {
	let root = tab.document.root_id;
	let topology_rev = tab.document.topology_rev;
	let state_id = StateId::ZERO;
	if !tab.cursor.primary_mut().resolve(
		registry,
		root,
		&mut tab.view.folds,
		topology_rev,
		state_id,
	) {
		return CommandEffect::render();
	}

	let reset_preferred_col = matches!(
		command,
		EditorCommand::ClickCursor(_)
			| EditorCommand::GotoLine(_)
			| EditorCommand::LineStart
			| EditorCommand::FirstNonWhitespace
			| EditorCommand::LineEnd
			| EditorCommand::SmartHome
			| EditorCommand::AppendAtLineEnd
			| EditorCommand::MoveCursor(
				MoveDirection::Left
					| MoveDirection::Right
					| MoveDirection::NextWord
					| MoveDirection::PrevWord
					| MoveDirection::NextWordEnd
					| MoveDirection::Top
					| MoveDirection::Bottom
					| MoveDirection::FindChar { .. }
			)
	);
	let mut active_visual = tab.cursor.active_visual();
	let gutter_numbering = tab.view.gutter_numbering();
	let effect = handle_navigation_command(
		registry,
		Some(root),
		&mut tab.view.folds,
		tab.cursor.primary_mut(),
		&mut active_visual,
		&mut tab.view.common.scroll_y,
		&mut tab.view.common.scroll_row_offset,
		&mut tab.view.common.viewport_width,
		&mut tab.view.common.viewport_lines,
		tab.view.common.wrap_enabled,
		tab.view.common.minimap_enabled,
		gutter_numbering,
		topology_rev,
		state_id,
		&mut tab.document.cached_sum_tree,
		&mut ui.status_message,
		&mut ui.mode_override,
		command,
	);
	tab.cursor
		.primary_mut()
		.resolve(registry, root, &mut tab.view.folds, topology_rev, state_id);
	tab.cursor.set_active_visual(active_visual);
	if reset_preferred_col {
		tab.cursor.sync_preferred_col();
	}
	effect
}

fn multi_cursor_navigation_supported(command: &EditorCommand) -> bool {
	matches!(
		command,
		EditorCommand::MoveCursor(
			MoveDirection::Up
				| MoveDirection::Down
				| MoveDirection::Left
				| MoveDirection::Right
				| MoveDirection::NextWord
				| MoveDirection::PrevWord
				| MoveDirection::NextWordEnd
				| MoveDirection::FindChar { .. }
		) | EditorCommand::LineStart
			| EditorCommand::FirstNonWhitespace
			| EditorCommand::LineEnd
			| EditorCommand::SmartHome
			| EditorCommand::AppendAtLineEnd
			| EditorCommand::PageUp
			| EditorCommand::PageDown
	)
}

fn move_all_cursors(
	registry: &UastRegistry,
	tab: &mut TextEditorMut<'_>,
	ui: &mut UiState,
	command: EditorCommand,
) -> CommandEffect {
	let root = tab.document.root_id;
	let vertical = matches!(
		command,
		EditorCommand::MoveCursor(MoveDirection::Up | MoveDirection::Down)
			| EditorCommand::PageUp
			| EditorCommand::PageDown
	);
	let base_scroll_y = tab.view.scroll_y;
	let base_scroll_row_offset = tab.view.scroll_row_offset;
	let base_width = tab.view.viewport_width;
	let base_lines = tab.view.viewport_lines;
	let wrap_enabled = tab.view.wrap_enabled;
	let minimap_enabled = tab.view.minimap_enabled;
	let gutter_numbering = tab.gutter_numbering();
	let topology_rev = tab.document.topology_rev;
	let current_state_id = tab.document.ledger.current_state_id;
	let mut cached_sum_tree = tab.document.semantic.cached_sum_tree.take();
	let mut primary_result = None;

	{
		let folds = &mut tab.view.folds;
		let Some(cursors) = tab.cursor.points_mut() else {
			return CommandEffect::default();
		};
		for (index, caret) in cursors.all_mut().enumerate() {
			if !caret.resolve(registry, root, folds, topology_rev, current_state_id) {
				continue;
			}
			let mut active_visual = None;
			let mut scroll_y = base_scroll_y;
			let mut scroll_row_offset = base_scroll_row_offset;
			let mut viewport_width = base_width;
			let mut viewport_lines = base_lines;
			let mut status_message = None;
			let mut mode_override = None;
			handle_navigation_command(
				registry,
				Some(root),
				folds,
				caret,
				&mut active_visual,
				&mut scroll_y,
				&mut scroll_row_offset,
				&mut viewport_width,
				&mut viewport_lines,
				wrap_enabled,
				minimap_enabled,
				gutter_numbering,
				topology_rev,
				current_state_id,
				&mut cached_sum_tree,
				&mut status_message,
				&mut mode_override,
				command.clone(),
			);
			caret.resolve(registry, root, folds, topology_rev, current_state_id);
			if !vertical {
				caret.preferred_col = caret.abs_col();
			}
			if index == 0 {
				primary_result = Some((
					scroll_y,
					scroll_row_offset,
					viewport_width,
					viewport_lines,
					status_message,
					mode_override,
				));
			}
		}
		cursors.normalize(registry, root);
	}

	tab.document.semantic.cached_sum_tree = cached_sum_tree;
	if let Some((
		scroll_y,
		scroll_row_offset,
		viewport_width,
		viewport_lines,
		status_message,
		mode_override,
	)) = primary_result
	{
		tab.view.scroll_y = scroll_y;
		tab.view.scroll_row_offset = scroll_row_offset;
		tab.view.viewport_width = viewport_width;
		tab.view.viewport_lines = viewport_lines;
		if status_message.is_some() {
			ui.status_message = status_message;
		}
		if mode_override.is_some() {
			ui.mode_override = mode_override;
		}
	}
	CommandEffect::render()
}

fn add_cursor_at(
	registry: &UastRegistry,
	tab: &mut TextEditorMut<'_>,
	ui: &mut UiState,
	target: crate::core::CursorPosition,
) -> CommandEffect {
	let root = tab.document.root_id;
	let topology_rev = tab.document.topology_rev;
	let state_id = tab.document.ledger.current_state_id;
	let Some(cursors) = tab.cursor.points_mut() else {
		return CommandEffect::default();
	};
	let mut caret = *cursors.primary();
	let placed = caret.place_at_line_col(
		registry,
		root,
		&mut tab.view.folds,
		topology_rev,
		state_id,
		target.line,
		target.col,
		true,
		super::super::session::Affinity::After,
	);
	if !placed {
		ui.status_message = Some("Line index is still building".to_string());
		let required = registry
			.find_node_at_line_col(root, &tab.view.folds, target.line, target.col)
			.required_node();
		caret.set_pending_target(target.line, target.col, Affinity::After, true, required);
		caret.preferred_col = target.col;
		return finish_add_cursor_result(cursors.add_pending(caret), ui);
	}
	caret.preferred_col = target.col;
	finish_add_cursor(registry, root, cursors, caret, ui)
}

fn add_cursor_adjacent(
	registry: &UastRegistry,
	tab: &mut TextEditorMut<'_>,
	ui: &mut UiState,
	direction: VerticalDirection,
) -> CommandEffect {
	let root = tab.document.root_id;
	let topology_rev = tab.document.topology_rev;
	let state_id = tab.document.ledger.current_state_id;
	let Some(cursors) = tab.cursor.points_mut() else {
		return CommandEffect::default();
	};
	let primary = *cursors.primary();
	if registry.exact_line_count(root).is_none() {
		let target_line = match direction {
			VerticalDirection::Up => primary.abs_line().saturating_sub(1),
			VerticalDirection::Down => primary.abs_line().saturating_add(1),
		};
		if target_line == primary.abs_line() {
			return CommandEffect::default();
		}
		let mut caret = primary;
		let placed = caret.place_at_line_col(
			registry,
			root,
			&mut tab.view.folds,
			topology_rev,
			state_id,
			target_line,
			primary.preferred_col,
			false,
			super::super::session::Affinity::After,
		);
		if !placed {
			ui.status_message = Some("Line index is still building".to_string());
			let required = registry
				.find_node_at_line_col(root, &tab.view.folds, target_line, primary.preferred_col)
				.required_node();
			caret.set_pending_target(
				target_line,
				primary.preferred_col,
				Affinity::After,
				false,
				required,
			);
			caret.preferred_col = primary.preferred_col;
			return finish_add_cursor_result(cursors.add_pending(caret), ui);
		}
		caret.preferred_col = primary.preferred_col;
		return finish_add_cursor(registry, root, cursors, caret, ui);
	}
	let max_line = DocLine::new(registry.get_total_newlines(root));
	let current_visual =
		visual_line_index_for_doc_line(registry, root, &tab.view.folds, primary.abs_line());
	let max_visual = max_visual_line_index(registry, root, &tab.view.folds, max_line);
	let target_visual = match direction {
		VerticalDirection::Up => current_visual.saturating_sub(1),
		VerticalDirection::Down => current_visual.saturating_add(1).min(max_visual),
	};
	if target_visual == current_visual {
		return CommandEffect::default();
	}
	let target_line =
		doc_line_for_visual_index(registry, root, &tab.view.folds, target_visual, max_line);
	let mut caret = primary;
	let placed = caret.place_at_line_col(
		registry,
		root,
		&mut tab.view.folds,
		topology_rev,
		state_id,
		target_line,
		primary.preferred_col,
		false,
		super::super::session::Affinity::After,
	);
	if !placed {
		ui.status_message = Some("Line index is still building".to_string());
		let required = registry
			.find_node_at_line_col(root, &tab.view.folds, target_line, primary.preferred_col)
			.required_node();
		caret.set_pending_target(
			target_line,
			primary.preferred_col,
			Affinity::After,
			false,
			required,
		);
		caret.preferred_col = primary.preferred_col;
		return finish_add_cursor_result(cursors.add_pending(caret), ui);
	}
	caret.preferred_col = primary.preferred_col;
	finish_add_cursor(registry, root, cursors, caret, ui)
}

fn finish_add_cursor(
	registry: &UastRegistry,
	root: NodeId,
	cursors: &mut super::super::session::CursorSet,
	caret: Caret,
	ui: &mut UiState,
) -> CommandEffect {
	finish_add_cursor_result(cursors.add_or_promote(registry, root, caret), ui)
}

fn finish_add_cursor_result(result: AddCaretResult, ui: &mut UiState) -> CommandEffect {
	match result {
		AddCaretResult::Added | AddCaretResult::Promoted => CommandEffect::render(),
		AddCaretResult::Unchanged => CommandEffect::default(),
		AddCaretResult::AtLimit => {
			ui.status_message = Some("Maximum of 1024 cursors reached".to_string());
			CommandEffect::render()
		}
	}
}

pub(super) fn handle_navigation_command(
	registry: &UastRegistry,
	root_id: Option<NodeId>,
	folds: &mut FoldSet,
	caret: &mut Caret,
	active_visual: &mut Option<(DocByte, VisualKind)>,
	scroll_y: &mut u64,
	scroll_row_offset: &mut DisplayRow,
	viewport_width: &mut u16,
	viewport_lines: &mut u32,
	wrap_enabled: bool,
	minimap_enabled: bool,
	gutter_numbering: Option<super::super::layout::GutterNumbering>,
	topology_rev: TopologyRev,
	current_state_id: StateId,
	cached_sum_tree: &mut Option<CachedSumTree>,
	status_message: &mut Option<String>,
	mode_override: &mut Option<EditorMode>,
	command: EditorCommand,
) -> CommandEffect {
	let mut target_line = caret.abs_line();
	let mut target_col = if matches!(
		&command,
		EditorCommand::MoveCursor(MoveDirection::Up | MoveDirection::Down)
			| EditorCommand::PageUp
			| EditorCommand::PageDown
	) {
		caret.preferred_col
	} else {
		caret.abs_col()
	};
	match command {
		EditorCommand::Resize(w, h) => {
			*viewport_width = w;
			*viewport_lines = h.saturating_sub(EDITOR_CHROME_ROWS) as u32;
		}
		EditorCommand::MoveCursor(dir) => {
			if let Some(rid) = root_id {
				match dir {
					MoveDirection::Up => {
						let max_doc_line = DocLine::new(registry.get_total_newlines(rid));
						let current_visual =
							visual_line_index_for_doc_line(registry, rid, folds, target_line);
						target_line = doc_line_for_visual_index(
							registry,
							rid,
							folds,
							current_visual.saturating_sub(1),
							max_doc_line,
						);
					}
					MoveDirection::Down => {
						let max_doc_line = DocLine::new(registry.get_total_newlines(rid));
						let current_visual =
							visual_line_index_for_doc_line(registry, rid, folds, target_line);
						let max_visual = max_visual_line_index(registry, rid, folds, max_doc_line);
						target_line = doc_line_for_visual_index(
							registry,
							rid,
							folds,
							current_visual.saturating_add(1).min(max_visual),
							max_doc_line,
						);
					}
					MoveDirection::Left => {
						if let Ok(line_bytes) =
							read_line_bytes_sparse(registry, rid, target_line, false)
						{
							target_col =
								step_left_visual_col(&line_bytes, DocLine::ZERO, target_col);
						} else {
							target_col = target_col.saturating_sub(1);
						}
					}
					MoveDirection::Right => {
						if let Ok(line_bytes) =
							read_line_bytes_sparse(registry, rid, target_line, false)
						{
							target_col =
								step_right_visual_col(&line_bytes, DocLine::ZERO, target_col);
						} else {
							target_col = caret
								.abs_col()
								.checked_add(1)
								.expect("DocDisplayCol addition overflow");
						}
					}
					MoveDirection::NextWord
					| MoveDirection::PrevWord
					| MoveDirection::NextWordEnd => {
						if let Ok(bytes) = read_loaded_document(registry, rid)
							&& let Ok(doc) = std::str::from_utf8(&bytes)
						{
							let cursor_abs_byte = caret.byte().get() as usize;
							let target_byte = match dir {
								MoveDirection::NextWord => next_word_start(doc, cursor_abs_byte),
								MoveDirection::PrevWord => prev_word_start(doc, cursor_abs_byte),
								MoveDirection::NextWordEnd => next_word_end(doc, cursor_abs_byte),
								_ => unreachable!(),
							};
							let (line, col) =
								line_col_from_byte_offset(&bytes, DocByte::new(target_byte as u64));
							target_line = line;
							target_col = col;
						}
					}
					MoveDirection::Top => {
						target_line = doc_line_for_visual_index(
							registry,
							rid,
							folds,
							DocVisualRow::ZERO,
							DocLine::new(registry.get_total_newlines(rid)),
						);
						target_col = DocDisplayCol::ZERO;
					}
					MoveDirection::Bottom => {
						let max_doc_line = DocLine::new(registry.get_total_newlines(rid));
						target_line = doc_line_for_visual_index(
							registry,
							rid,
							folds,
							max_visual_line_index(registry, rid, folds, max_doc_line),
							max_doc_line,
						);
						target_col = DocDisplayCol::ZERO;
						let bottom_margin = u64::from(viewport_lines.saturating_sub(1));
						*scroll_y = target_line.get().saturating_sub(bottom_margin);
						*scroll_row_offset = DisplayRow::ZERO;
					}
					MoveDirection::FindChar {
						ch,
						forward,
						inclusive,
					} => {
						if let Ok(line_bytes) =
							read_line_bytes_sparse(registry, rid, target_line, false)
						{
							match find_char_target_col(
								&line_bytes,
								target_col,
								ch,
								forward,
								inclusive,
							) {
								FindCharOutcome::Moved(col) => {
									target_col = col;
								}
								FindCharOutcome::NoMove => {
									// Character is on the line but every
									// candidate would leave the cursor
									// exactly where it is — silently
									// no-op, matching Vim's visual-bell
									// behavior.
								}
								FindCharOutcome::NotFound => {
									*status_message = Some(format!("'{}' not found on line", ch));
								}
							}
						}
					}
				}
				place_cursor_at_line_col(
					registry,
					rid,
					folds,
					target_line,
					target_col,
					false,
					(topology_rev, current_state_id),
					caret,
				);
			}
		}
		EditorCommand::GotoLine(target) => {
			if let Some(rid) = root_id {
				if let LineLookupState::Pending {
					indexed_through_line,
					..
				} = registry.line_lookup_state(rid, target)
				{
					let required = registry
						.find_node_at_line_col_raw(rid, target, DocDisplayCol::ZERO)
						.required_node();
					caret.set_pending_target(
						target,
						DocDisplayCol::ZERO,
						Affinity::After,
						true,
						required,
					);
					*status_message = Some(format!(
						"Line index still building (indexed through line {})",
						indexed_through_line.get().saturating_add(1)
					));
					return CommandEffect::render();
				}
				let max_doc_line = DocLine::new(registry.get_total_newlines(rid));
				target_line = target.min(max_doc_line);
				target_col = DocDisplayCol::ZERO;
				place_cursor_at_line_col(
					registry,
					rid,
					folds,
					target_line,
					target_col,
					true,
					(topology_rev, current_state_id),
					caret,
				);
			}
		}
		EditorCommand::AppendAtLineEnd => {
			if let Some(rid) = root_id {
				// Position the cursor at the line-end byte (the '\n' or EOF)
				// so that Insert mode inserts after the last visible character.
				// Using byte-level positioning avoids visual-col clamping that
				// would land the cursor ON the last character instead of past it.
				if let Ok(line_end) = line_end_byte_sparse(registry, rid, target_line, false) {
					caret.set_byte(line_end, super::super::session::Affinity::After);
				}
				*mode_override = Some(EditorMode::Insert);
			}
		}
		line_motion @ (EditorCommand::LineStart
		| EditorCommand::FirstNonWhitespace
		| EditorCommand::LineEnd
		| EditorCommand::SmartHome) => {
			if let Some(rid) = root_id
				&& let Ok(line_bytes) = read_line_bytes_sparse(registry, rid, target_line, false)
			{
				let target_col = match line_motion {
					EditorCommand::LineStart => DocDisplayCol::ZERO,
					EditorCommand::FirstNonWhitespace => {
						first_non_whitespace_visual_col(&line_bytes, DocLine::ZERO)
					}
					EditorCommand::LineEnd => line_end_visual_col(&line_bytes, DocLine::ZERO),
					EditorCommand::SmartHome => {
						smart_home_visual_col(&line_bytes, DocLine::ZERO, target_col)
					}
					_ => unreachable!(),
				};
				place_cursor_at_line_col(
					registry,
					rid,
					folds,
					target_line,
					target_col,
					false,
					(topology_rev, current_state_id),
					caret,
				);
			}
		}
		EditorCommand::PageUp => {
			if let Some(rid) = root_id {
				let max_doc_line = DocLine::new(registry.get_total_newlines(rid));
				let current_visual =
					visual_line_index_for_doc_line(registry, rid, folds, target_line);
				target_line = doc_line_for_visual_index(
					registry,
					rid,
					folds,
					current_visual.saturating_sub(page_motion_step(*viewport_lines)),
					max_doc_line,
				);
				place_cursor_at_line_col(
					registry,
					rid,
					folds,
					target_line,
					target_col,
					false,
					(topology_rev, current_state_id),
					caret,
				);
			}
		}
		EditorCommand::PageDown => {
			if let Some(rid) = root_id {
				let max_doc_line = DocLine::new(registry.get_total_newlines(rid));
				let current_visual =
					visual_line_index_for_doc_line(registry, rid, folds, target_line);
				let max_visual = max_visual_line_index(registry, rid, folds, max_doc_line);
				target_line = doc_line_for_visual_index(
					registry,
					rid,
					folds,
					current_visual
						.saturating_add(page_motion_step(*viewport_lines))
						.min(max_visual),
					max_doc_line,
				);
				place_cursor_at_line_col(
					registry,
					rid,
					folds,
					target_line,
					target_col,
					false,
					(topology_rev, current_state_id),
					caret,
				);
			}
		}
		EditorCommand::SelectInnerWord => {
			if let Some(rid) = root_id {
				let cursor_abs_byte = caret.byte();
				match word_object_delta_at_cursor(registry, rid, cursor_abs_byte) {
					Ok(Some(delta)) => {
						let start = delta.global_byte_offset;
						let last_byte = start
							.saturating_add(delta.deleted_bytes.len() as u64)
							.saturating_sub(1);
						caret.set_byte(last_byte, super::super::session::Affinity::After);
						*active_visual = Some((start, VisualKind::Char));
						*mode_override = Some(EditorMode::Visual {
							anchor: start,
							kind: VisualKind::Char,
						});
					}
					Ok(None) => {
						*status_message = Some("No structural word at cursor".to_string());
					}
					Err(msg) => *status_message = Some(msg),
				}
			}
		}
		EditorCommand::SelectAroundFunction => {
			if let Some(rid) = root_id {
				let cursor_abs_byte = caret.byte();
				let q = UastQuery::new(registry, rid);
				if let Some(leaf) = q.node_at_offset(cursor_abs_byte) {
					if let Some(func) = q.find_ancestor(leaf, is_function_scope) {
						let (fn_start, fn_end) = q.node_bounds(func);
						let last_byte = fn_end.saturating_sub(1);
						caret.set_byte(last_byte, super::super::session::Affinity::After);

						*active_visual = Some((fn_start, VisualKind::Char));
						*mode_override = Some(EditorMode::Visual {
							anchor: fn_start,
							kind: VisualKind::Char,
						});
					} else {
						*status_message = Some("No enclosing function scope".to_string());
					}
				}
			}
		}
		EditorCommand::NextFunction => {
			if let Some(rid) = root_id {
				let cursor_abs_byte = caret.byte();
				let q = UastQuery::new(registry, rid);
				if let Some(next_func) = q.next_of_kind(cursor_abs_byte, is_function_scope) {
					let (fn_start, _) = q.node_bounds(next_func);
					caret.set_byte(fn_start, super::super::session::Affinity::After);
				} else {
					*status_message = Some("No more functions".to_string());
				}
			}
		}
		EditorCommand::MatchDelimiter => {
			if let Some(rid) = root_id {
				let cursor_abs_byte = caret.byte();
				match read_loaded_document(registry, rid) {
					Ok(bytes) => match std::str::from_utf8(&bytes) {
						Ok(doc) => {
							let cursor_byte = cursor_abs_byte.get() as usize;
							match find_matching_delimiter_byte(doc, cursor_byte) {
								Some(target_byte) => {
									let (target_line, target_col) = line_col_from_byte_offset(
										&bytes,
										DocByte::new(target_byte as u64),
									);
									place_cursor_at_line_col(
										registry,
										rid,
										folds,
										target_line,
										target_col,
										true,
										(topology_rev, current_state_id),
										caret,
									);
								}
								None => {
									*status_message =
										Some("No matching delimiter at cursor".to_string());
								}
							}
						}
						Err(_) => {
							*status_message =
								Some("Matching delimiter requires valid UTF-8 text".to_string());
						}
					},
					Err(msg) => *status_message = Some(msg),
				}
			}
		}
		EditorCommand::Scroll(delta) => {
			if let Some(rid) = root_id {
				let max_doc_line = DocLine::new(registry.get_total_newlines(rid));
				let current_visual =
					visual_line_index_for_doc_line(registry, rid, folds, target_line);
				let max_visual = max_visual_line_index(registry, rid, folds, max_doc_line);
				let target_visual = DocVisualRow::new(
					(i128::from(current_visual.get()) + i128::from(delta))
						.clamp(0, i128::from(max_visual.get())) as u64,
				);

				if wrap_enabled {
					let geometry = viewport_geometry_for_viewport_with_numbering(
						DocLine::new(*scroll_y),
						*viewport_lines,
						max_doc_line,
						*viewport_width,
						minimap_enabled,
						gutter_numbering,
					);
					let sum_tree =
						refresh_sum_tree(cached_sum_tree, registry, rid, current_state_id);
					let anchor = scroll_viewport_anchor_with_sum_tree(
						registry,
						rid,
						folds,
						ViewportAnchor {
							line: DocLine::new(*scroll_y),
							row_offset: *scroll_row_offset,
						},
						delta,
						max_doc_line,
						true,
						geometry.text_width,
						sum_tree,
					);
					*scroll_y = anchor.line.get();
					*scroll_row_offset = anchor.row_offset;
				} else {
					*scroll_y = scroll_viewport(
						registry,
						rid,
						folds,
						*scroll_y,
						delta,
						max_doc_line,
						*viewport_lines,
					);
					*scroll_row_offset = DisplayRow::ZERO;
				}

				target_line =
					doc_line_for_visual_index(registry, rid, folds, target_visual, max_doc_line);
				place_cursor_at_line_col(
					registry,
					rid,
					folds,
					target_line,
					target_col,
					false,
					(topology_rev, current_state_id),
					caret,
				);
			}
		}
		EditorCommand::ScrollViewportToLine(requested_line) => {
			if let Some(rid) = root_id {
				let max_doc_line = DocLine::new(registry.get_total_newlines(rid));
				let target = requested_line.min(max_doc_line);
				if wrap_enabled {
					let geometry = viewport_geometry_for_viewport_with_numbering(
						DocLine::new(*scroll_y),
						*viewport_lines,
						max_doc_line,
						*viewport_width,
						minimap_enabled,
						gutter_numbering,
					);
					let sum_tree =
						refresh_sum_tree(cached_sum_tree, registry, rid, current_state_id);
					// Anchor at the top of the target line, then clamp by moving forward
					// (viewport_lines - 1) rows and back — hitting EOF during the forward
					// step naturally clamps the anchor to a valid top.
					let raw_anchor = ViewportAnchor {
						line: target,
						row_offset: DisplayRow::ZERO,
					};
					let vp_step = (*viewport_lines).max(1).saturating_sub(1) as i32;
					let forward = scroll_viewport_anchor_with_sum_tree(
						registry,
						rid,
						folds,
						raw_anchor,
						vp_step,
						max_doc_line,
						true,
						geometry.text_width,
						sum_tree,
					);
					let sum_tree =
						refresh_sum_tree(cached_sum_tree, registry, rid, current_state_id);
					let anchor = scroll_viewport_anchor_with_sum_tree(
						registry,
						rid,
						folds,
						forward,
						-vp_step,
						max_doc_line,
						true,
						geometry.text_width,
						sum_tree,
					);
					*scroll_y = anchor.line.get();
					*scroll_row_offset = anchor.row_offset;
					let sum_tree =
						refresh_sum_tree(cached_sum_tree, registry, rid, current_state_id);
					let (clamped_cursor_line, clamped_cursor_col) =
						clamp_cursor_to_viewport_anchor_with_sum_tree(
							registry,
							rid,
							folds,
							anchor,
							target_line,
							target_col,
							max_doc_line,
							*viewport_lines,
							true,
							geometry.text_width,
							sum_tree,
						);
					if clamped_cursor_line != target_line || clamped_cursor_col != target_col {
						place_cursor_at_line_col(
							registry,
							rid,
							folds,
							clamped_cursor_line,
							clamped_cursor_col,
							false,
							(topology_rev, current_state_id),
							caret,
						);
					}
				} else {
					let target_visual =
						visual_line_index_for_doc_line(registry, rid, folds, target);
					let max_visual = max_visual_line_index(registry, rid, folds, max_doc_line);
					let max_top_visual = max_visual
						.saturating_sub(u64::from((*viewport_lines).max(1).saturating_sub(1)));
					let clamped_visual = target_visual.min(max_top_visual);
					*scroll_y = doc_line_for_visual_index(
						registry,
						rid,
						folds,
						clamped_visual,
						max_doc_line,
					)
					.get();
					*scroll_row_offset = DisplayRow::ZERO;
					let clamped_cursor_line = clamp_cursor_line_to_viewport(
						registry,
						rid,
						folds,
						target_line,
						*scroll_y,
						max_doc_line,
						*viewport_lines,
					);
					if clamped_cursor_line != target_line {
						place_cursor_at_line_col(
							registry,
							rid,
							folds,
							clamped_cursor_line,
							target_col,
							false,
							(topology_rev, current_state_id),
							caret,
						);
					}
				}
			}
		}
		EditorCommand::ScrollViewport(delta) => {
			if let Some(rid) = root_id {
				let max_doc_line = DocLine::new(registry.get_total_newlines(rid));
				if wrap_enabled {
					let geometry = viewport_geometry_for_viewport_with_numbering(
						DocLine::new(*scroll_y),
						*viewport_lines,
						max_doc_line,
						*viewport_width,
						minimap_enabled,
						gutter_numbering,
					);
					let sum_tree =
						refresh_sum_tree(cached_sum_tree, registry, rid, current_state_id);
					let anchor = scroll_viewport_anchor_with_sum_tree(
						registry,
						rid,
						folds,
						ViewportAnchor {
							line: DocLine::new(*scroll_y),
							row_offset: *scroll_row_offset,
						},
						delta,
						max_doc_line,
						true,
						geometry.text_width,
						sum_tree,
					);
					*scroll_y = anchor.line.get();
					*scroll_row_offset = anchor.row_offset;
					let sum_tree =
						refresh_sum_tree(cached_sum_tree, registry, rid, current_state_id);
					let (clamped_cursor_line, clamped_cursor_col) =
						clamp_cursor_to_viewport_anchor_with_sum_tree(
							registry,
							rid,
							folds,
							anchor,
							target_line,
							target_col,
							max_doc_line,
							*viewport_lines,
							true,
							geometry.text_width,
							sum_tree,
						);
					if clamped_cursor_line != target_line || clamped_cursor_col != target_col {
						place_cursor_at_line_col(
							registry,
							rid,
							folds,
							clamped_cursor_line,
							clamped_cursor_col,
							false,
							(topology_rev, current_state_id),
							caret,
						);
					}
				} else {
					*scroll_y = scroll_viewport(
						registry,
						rid,
						folds,
						*scroll_y,
						delta,
						max_doc_line,
						*viewport_lines,
					);
					let clamped_cursor_line = clamp_cursor_line_to_viewport(
						registry,
						rid,
						folds,
						target_line,
						*scroll_y,
						max_doc_line,
						*viewport_lines,
					);
					if clamped_cursor_line != target_line {
						place_cursor_at_line_col(
							registry,
							rid,
							folds,
							clamped_cursor_line,
							target_col,
							false,
							(topology_rev, current_state_id),
							caret,
						);
					}
				}
			}
		}
		EditorCommand::ClickCursor(target_pos) => {
			if let Some(rid) = root_id {
				let max_doc_line = DocLine::new(registry.get_total_newlines(rid));
				target_line = target_pos.line.min(max_doc_line);
				target_col = target_pos.col;
				place_cursor_at_line_col(
					registry,
					rid,
					folds,
					target_line,
					target_col,
					true,
					(topology_rev, current_state_id),
					caret,
				);
			}
		}
		_ => unreachable!(),
	}
	if let Some(root) = root_id {
		caret.resolve(registry, root, folds, topology_rev, current_state_id);
	}

	CommandEffect::render()
}

#[cfg(test)]
mod tests {
	use super::{FindCharOutcome, find_char_target_col};
	use crate::core::{DisplayRow, DocByte, DocDisplayCol, DocLine, StateId, TopologyRev};
	use crate::engine::folding::FoldSet;
	use crate::engine::session::Caret;
	use crate::engine::test_support::build_exact_metric_physical_document;
	use crate::uast::UastProjection;

	fn resolved_caret(
		registry: &crate::ecs::UastRegistry,
		root: crate::ecs::NodeId,
		byte: DocByte,
	) -> Caret {
		let mut caret = Caret::at_start();
		let mut folds = FoldSet::default();
		caret.set_byte(byte, crate::engine::session::Affinity::After);
		assert!(caret.resolve(registry, root, &mut folds, TopologyRev::ZERO, StateId::ZERO,));
		caret.preferred_col = caret.abs_col();
		caret
	}

	#[test]
	fn unresolved_navigation_target_stays_pending_without_moving_the_anchor() {
		let (registry, root, path) =
			build_exact_metric_physical_document(&["first\ninside\n", "second\n"]);
		let first = registry.get_first_child(root).expect("first leaf");
		let mut folds = FoldSet::default();
		let mut caret = Caret::at_start();
		caret.reset_to_start(first, TopologyRev::ZERO, StateId::ZERO);
		assert!(!caret.place_at_line_col(
			&registry,
			root,
			&mut folds,
			TopologyRev::ZERO,
			StateId::ZERO,
			DocLine::new(1),
			DocDisplayCol::ZERO,
			false,
			crate::engine::session::Affinity::After,
		));

		assert_eq!(caret.byte(), DocByte::ZERO);
		assert_eq!(caret.abs_line(), DocLine::new(1));
		assert!(caret.pending_target().is_some());
		registry.unregister_device(78);
		let _ = std::fs::remove_file(path);
	}

	#[test]
	fn find_char_forward_inclusive_lands_on_match() {
		let line = b"hello world\n";
		assert_eq!(
			find_char_target_col(line, DocDisplayCol::ZERO, 'o', true, true),
			FindCharOutcome::Moved(DocDisplayCol::new(4)),
		);
	}

	#[test]
	fn find_char_forward_exclusive_stops_one_before_match() {
		let line = b"hello world\n";
		assert_eq!(
			find_char_target_col(line, DocDisplayCol::ZERO, 'o', true, false),
			FindCharOutcome::Moved(DocDisplayCol::new(3)),
		);
	}

	#[test]
	fn find_char_forward_skips_current_position() {
		let line = b"hello world\n";
		// Starting on the first 'o' at col 4 and searching forward for 'o'
		// must not re-match the current character — it should find the 'o' in
		// "world" at column 7.
		assert_eq!(
			find_char_target_col(line, DocDisplayCol::new(4), 'o', true, true),
			FindCharOutcome::Moved(DocDisplayCol::new(7)),
		);
	}

	#[test]
	fn find_char_backward_inclusive_lands_on_match() {
		let line = b"hello world\n";
		assert_eq!(
			find_char_target_col(line, DocDisplayCol::new(10), 'h', false, true),
			FindCharOutcome::Moved(DocDisplayCol::ZERO),
		);
	}

	#[test]
	fn find_char_backward_exclusive_stops_one_after_match() {
		let line = b"hello world\n";
		assert_eq!(
			find_char_target_col(line, DocDisplayCol::new(10), 'h', false, false),
			FindCharOutcome::Moved(DocDisplayCol::new(1)),
		);
	}

	#[test]
	fn find_char_not_found_returns_notfound() {
		let line = b"hello world\n";
		assert_eq!(
			find_char_target_col(line, DocDisplayCol::ZERO, 'z', true, true),
			FindCharOutcome::NotFound,
		);
		assert_eq!(
			find_char_target_col(line, DocDisplayCol::new(10), 'z', false, true),
			FindCharOutcome::NotFound,
		);
	}

	#[test]
	fn find_char_does_not_cross_newline() {
		// Content is intentionally a single line; newline boundaries are
		// enforced by the caller reading one DocLine at a time, but the
		// helper strips trailing '\n' defensively so a stray newline never
		// counts as a match target and the search stays within the line.
		let line = b"abc\n";
		assert_eq!(
			find_char_target_col(line, DocDisplayCol::ZERO, '\n', true, true),
			FindCharOutcome::NotFound,
		);
	}

	#[test]
	fn find_char_empty_line_is_graceful() {
		let line = b"\n";
		assert_eq!(
			find_char_target_col(line, DocDisplayCol::ZERO, 'a', true, true),
			FindCharOutcome::NotFound,
		);
	}

	#[test]
	fn find_char_backward_from_start_is_notfound() {
		let line = b"hello\n";
		assert_eq!(
			find_char_target_col(line, DocDisplayCol::ZERO, 'h', false, true),
			FindCharOutcome::NotFound,
		);
	}

	#[test]
	fn find_char_forward_exclusive_no_duplicate_move() {
		// If the cursor is already at the "one before" position relative to
		// an 'o' (col 3, with 'o' at col 4), searching forward with t should
		// report NoMove: the character *is* on the line, but landing there
		// would leave the cursor exactly where it started. The caller must
		// silently no-op instead of lying with a "not found" status.
		let line = b"hello\n";
		assert_eq!(
			find_char_target_col(line, DocDisplayCol::new(3), 'o', true, false),
			FindCharOutcome::NoMove,
		);
	}

	#[test]
	fn find_char_tb_on_ab_reports_no_move() {
		// Classic Vim edge case: on the `a` of `"ab"`, `tb` cannot move the
		// cursor (the exclusive-forward target is column 0, which is already
		// where we are). This must surface as NoMove — not NotFound —
		// because 'b' really is on the line; a "not found" status would be a
		// lie and is what the first version of this helper did.
		let line = b"ab\n";
		assert_eq!(
			find_char_target_col(line, DocDisplayCol::ZERO, 'b', true, false),
			FindCharOutcome::NoMove,
		);
	}

	#[test]
	fn find_char_respects_tab_expansion() {
		// Tab at col 0 expands to col 4. The 'x' lives at col 4.
		let line = b"\tx\n";
		assert_eq!(
			find_char_target_col(line, DocDisplayCol::ZERO, 'x', true, true),
			FindCharOutcome::Moved(DocDisplayCol::new(4)),
		);
	}

	#[test]
	fn find_char_counts_multi_byte_chars_as_byte_columns() {
		// `é` is two UTF-8 bytes and must occupy two visual columns so the
		// helper stays in lock-step with `advance_visual_col_only` /
		// `line_col_from_byte_offset`, which also count by bytes. If this
		// ever drifts back to counting scalars, every caller that resolves
		// a cursor by byte offset would end up one column off on any line
		// containing non-ASCII text.
		//
		// `"éx\n"` → chars at [(0,'é'), (2,'x')]. Forward-inclusive for 'x'
		// from the start lands on column 2, not column 1.
		let line = "éx\n".as_bytes();
		assert_eq!(
			find_char_target_col(line, DocDisplayCol::ZERO, 'x', true, true),
			FindCharOutcome::Moved(DocDisplayCol::new(2)),
		);

		// Exercise the exclusive path with a multi-byte scalar between the
		// cursor and the target so the "one before the match" arithmetic
		// has to respect byte widths too.
		// `"aébc\n"` → chars at [(0,'a'), (1,'é'), (3,'b'), (4,'c')].
		// Forward-exclusive 'c' from col 0 lands on 'b' at col 3.
		let line_three = "aébc\n".as_bytes();
		assert_eq!(
			find_char_target_col(line_three, DocDisplayCol::ZERO, 'c', true, false),
			FindCharOutcome::Moved(DocDisplayCol::new(3)),
		);
		// And backward-inclusive 'é' from the end lands at col 1, the start
		// byte column of the two-byte scalar.
		assert_eq!(
			find_char_target_col(line_three, DocDisplayCol::new(4), 'é', false, true),
			FindCharOutcome::Moved(DocDisplayCol::new(1)),
		);
	}

	#[test]
	fn append_at_line_end_moves_cursor_to_eol_and_sets_insert_mode() {
		use super::handle_navigation_command;
		use crate::core::{DocByte, DocDisplayCol, DocLine, StateId};
		use crate::engine::protocol::{EditorCommand, EditorMode, VisualKind};
		use crate::engine::session::CachedSumTree;
		use crate::engine::test_support::build_document;

		let (registry, root) = build_document("hello world\n");
		// Place cursor at byte 0 (the 'h'), col 0.
		let mut caret = resolved_caret(&registry, root, DocByte::ZERO);
		let mut folds = FoldSet::default();
		let mut active_visual: Option<(DocByte, VisualKind)> = None;
		let mut scroll_y = 0u64;
		let mut scroll_row_offset = DisplayRow::ZERO;
		let mut viewport_width = 80u16;
		let mut viewport_lines = 24u32;
		let mut cached_sum_tree: Option<CachedSumTree> = None;
		let mut status_message: Option<String> = None;
		let mut mode_override: Option<EditorMode> = None;

		handle_navigation_command(
			&registry,
			Some(root),
			&mut folds,
			&mut caret,
			&mut active_visual,
			&mut scroll_y,
			&mut scroll_row_offset,
			&mut viewport_width,
			&mut viewport_lines,
			false,
			false,
			Some(crate::engine::GutterNumbering::Absolute),
			TopologyRev::ZERO,
			StateId::ZERO,
			&mut cached_sum_tree,
			&mut status_message,
			&mut mode_override,
			EditorCommand::AppendAtLineEnd,
		);

		// Cursor should have moved to end of "hello world" = col 11.
		assert_eq!(
			caret.abs_col(),
			DocDisplayCol::new(11),
			"AppendAtLineEnd must move cursor to end of line"
		);
		assert_eq!(caret.abs_line(), DocLine::ZERO);
		// Must request Insert mode.
		assert_eq!(
			mode_override,
			Some(EditorMode::Insert),
			"AppendAtLineEnd must set mode_override to Insert"
		);
	}

	#[test]
	fn select_inner_word_grafted_rust_document_selects_structural_token_bounds() {
		use super::handle_navigation_command;
		use crate::core::{DocByte, DocDisplayCol, DocLine, StateId};
		use crate::engine::document::parse::word_object_delta_at_cursor;
		use crate::engine::document::selection::resolve_visual_ranges_sparse;
		use crate::engine::protocol::{EditorCommand, EditorMode, VisualKind};
		use crate::engine::session::CachedSumTree;
		use crate::engine::test_support::build_grafted_rust_document;

		let source = include_str!("../clipboard.rs");
		let (registry, root) = build_grafted_rust_document(source);
		let word_pos = source
			.find("ClipboardHandle")
			.expect("ClipboardHandle exists in clipboard.rs");
		let expected_delta =
			word_object_delta_at_cursor(&registry, root, DocByte::new(word_pos as u64))
				.expect("word lookup should succeed")
				.expect("expected structural word delta");
		let expected_start = expected_delta.global_byte_offset;
		let expected_end = expected_start
			.saturating_add(expected_delta.deleted_bytes.len() as u64)
			.saturating_sub(1);

		let mut caret = resolved_caret(&registry, root, DocByte::new(word_pos as u64));
		let mut folds = FoldSet::default();

		let mut active_visual: Option<(DocByte, VisualKind)> = None;
		let mut scroll_y = 0u64;
		let mut scroll_row_offset = DisplayRow::ZERO;
		let mut viewport_width = 80u16;
		let mut viewport_lines = 24u32;
		let mut cached_sum_tree: Option<CachedSumTree> = None;
		let mut status_message: Option<String> = None;
		let mut mode_override: Option<EditorMode> = None;

		handle_navigation_command(
			&registry,
			Some(root),
			&mut folds,
			&mut caret,
			&mut active_visual,
			&mut scroll_y,
			&mut scroll_row_offset,
			&mut viewport_width,
			&mut viewport_lines,
			false,
			false,
			Some(crate::engine::GutterNumbering::Absolute),
			TopologyRev::ZERO,
			StateId::ZERO,
			&mut cached_sum_tree,
			&mut status_message,
			&mut mode_override,
			EditorCommand::SelectInnerWord,
		);

		assert_eq!(status_message, None);
		assert_eq!(active_visual, Some((expected_start, VisualKind::Char)));
		assert_eq!(
			mode_override,
			Some(EditorMode::Visual {
				anchor: expected_start,
				kind: VisualKind::Char,
			}),
		);

		let cursor_abs_byte = caret.byte();
		assert_eq!(cursor_abs_byte, expected_end);

		let ranges = resolve_visual_ranges_sparse(
			&registry,
			root,
			&folds,
			expected_start,
			cursor_abs_byte,
			VisualKind::Char,
		)
		.expect("resolve visual ranges");
		assert_eq!(ranges, vec![(expected_start, expected_end)]);

		let selected_text = String::from_utf8(
			registry
				.read_loaded_slice(root, expected_start, expected_end.saturating_add(1))
				.expect("read selected bytes"),
		)
		.expect("selected text utf8");
		assert_eq!(selected_text.as_bytes(), expected_delta.deleted_bytes);
		assert_eq!(caret.abs_line(), DocLine::new(2));
		assert_eq!(caret.abs_col(), DocDisplayCol::new(25));
	}

	/// Exercises AppendAtLineEnd against a structurally-grafted Rust
	/// document — the same tree shape the real editor produces after
	/// semantic parsing.  A flat `build_document` hides the bug because
	/// its tree has only one leaf; in a grafted tree, `line_end_byte_sparse`
	/// walks siblings that may be branch nodes (no virtual_data), causing
	/// it to fail silently and leave the cursor unmoved.
	#[test]
	fn append_at_line_end_grafted_rust_document_moves_cursor_to_eol() {
		use super::handle_navigation_command;
		use crate::core::{DocByte, DocDisplayCol, DocLine, StateId};
		use crate::engine::protocol::{EditorCommand, EditorMode, VisualKind};
		use crate::engine::session::CachedSumTree;
		use crate::engine::test_support::build_grafted_rust_document;

		let source = include_str!("../clipboard.rs");
		let (registry, root) = build_grafted_rust_document(source);

		// Position cursor at "inner:" on line 3 (0-indexed), mid-line.
		let inner_pos = source
			.find("inner:")
			.expect("inner: exists in clipboard.rs");
		let mut caret = resolved_caret(&registry, root, DocByte::new(inner_pos as u64));
		let mut folds = FoldSet::default();

		assert_eq!(
			caret.abs_line(),
			DocLine::new(3),
			"inner: should be on line 3"
		);
		let start_col = caret.abs_col();

		// Compute expected end-of-line visual column from source,
		// expanding tabs to TAB_SIZE (4) columns.
		let line_text = source.lines().nth(3).expect("line 3 exists");
		let mut visual_len = 0u32;
		for &b in line_text.as_bytes() {
			if b == b'\t' {
				visual_len += 4 - (visual_len % 4);
			} else {
				visual_len += 1;
			}
		}
		let expected_eol_col = DocDisplayCol::new(visual_len);
		assert!(
			start_col < expected_eol_col,
			"cursor must start before EOL for this test to be meaningful"
		);

		let mut active_visual: Option<(DocByte, VisualKind)> = None;
		let mut scroll_y = 0u64;
		let mut scroll_row_offset = DisplayRow::ZERO;
		let mut viewport_width = 80u16;
		let mut viewport_lines = 24u32;
		let mut cached_sum_tree: Option<CachedSumTree> = None;
		let mut status_message: Option<String> = None;
		let mut mode_override: Option<EditorMode> = None;

		handle_navigation_command(
			&registry,
			Some(root),
			&mut folds,
			&mut caret,
			&mut active_visual,
			&mut scroll_y,
			&mut scroll_row_offset,
			&mut viewport_width,
			&mut viewport_lines,
			false,
			false,
			Some(crate::engine::GutterNumbering::Absolute),
			TopologyRev::ZERO,
			StateId::ZERO,
			&mut cached_sum_tree,
			&mut status_message,
			&mut mode_override,
			EditorCommand::AppendAtLineEnd,
		);

		assert_eq!(
			status_message, None,
			"AppendAtLineEnd must not produce an error on a grafted document"
		);
		assert_eq!(
			caret.abs_col(),
			expected_eol_col,
			"AppendAtLineEnd must move cursor from col {} to end of line (col {})",
			start_col.get(),
			expected_eol_col.get(),
		);
		assert_eq!(caret.abs_line(), DocLine::new(3));
		assert_eq!(
			mode_override,
			Some(EditorMode::Insert),
			"AppendAtLineEnd must set Insert mode"
		);
	}
}
