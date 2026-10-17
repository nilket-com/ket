use super::super::core::refresh_sum_tree;
use super::super::document::cursor::{
	WordMotion, WordTarget, line_col_from_doc_byte_sparse, page_motion_step,
	place_cursor_at_line_col_indexed, smart_home_visual_col, step_left_visual_col,
	step_right_visual_col, word_motion_target,
};
use super::super::document::io::read_loaded_document;
use super::super::document::parse::{find_matching_delimiter_byte, word_object_delta_at_cursor};
use super::super::folding::{
	FoldSet, clamp_cursor_line_to_viewport, doc_line_for_visual_index, max_visual_line_index,
	scroll_viewport, visual_line_index_for_doc_line,
};
use super::super::layout::{
	CaretIndex, CaretLookup, LayoutDemand, ViewportAnchor, WrapLayout,
	clamp_cursor_to_viewport_anchor_with_sum_tree, scroll_viewport_anchor_with_sum_tree,
	viewport_geometry_for_viewport_with_numbering,
};
use super::super::protocol::{
	EditorCommand, EditorMode, MoveDirection, VerticalDirection, VisualKind,
};
use super::super::session::{
	AddCaretResult, Affinity, CachedSumTree, Caret, CommonViewState, FileBrowserEditorMut,
	TextEditorMut, UiState,
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
use crate::svp::ingest::COLD_INGEST_EXTENT_SIZE;
use crate::uast::LineLookupState;

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
				.checked_add(u64::from(TAB_SIZE) - (col.get() % u64::from(TAB_SIZE)))
				.expect("DocDisplayCol addition overflow");
		} else {
			col = col
				.checked_add(c.len_utf8() as u64)
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

/// The wrap index for caret lookups in `view` (ADR 0010), keyed at the text
/// width render uses. Navigation drops `demand`; the next render resolves a
/// pending caret again and acts on it.
pub(super) fn caret_index<'a>(
	registry: &UastRegistry,
	root: NodeId,
	view: &'a CommonViewState,
	gutter_numbering: Option<super::super::layout::GutterNumbering>,
	demand: &'a std::cell::RefCell<LayoutDemand>,
	state_id: StateId,
	topology_rev: TopologyRev,
) -> CaretIndex<'a> {
	let text_width = viewport_geometry_for_viewport_with_numbering(
		DocLine::new(view.scroll_y),
		view.viewport_lines,
		DocLine::new(registry.get_total_newlines(root)),
		view.viewport_width,
		view.minimap_enabled,
		gutter_numbering,
	)
	.text_width;
	CaretIndex::new(
		WrapLayout::new(&view.wrap_index, state_id, topology_rev, demand),
		view.wrap_enabled,
		text_width,
	)
}

/// LineStart, LineEnd, FirstNonWhitespace and SmartHome on an indexed line,
/// reading near the caret rather than the whole line (ADR 0010).
#[allow(clippy::too_many_arguments)]
fn indexed_line_motion(
	registry: &UastRegistry,
	root: NodeId,
	folds: &mut FoldSet,
	caret: &mut Caret,
	motion: &EditorCommand,
	line: DocLine,
	current_col: DocDisplayCol,
	(start, end): (DocByte, DocByte),
	index: CaretIndex<'_>,
	carried: &std::cell::RefCell<super::super::layout::CarriedWork>,
	(topology_rev, state_id): (TopologyRev, StateId),
) {
	match motion {
		EditorCommand::LineStart => {
			caret.place_at_known(
				registry,
				root,
				topology_rev,
				state_id,
				start,
				line,
				DocDisplayCol::ZERO,
			);
		}
		// The end is known once the line's scan has finished. Until then the
		// caret stays; render scans on, and a later End moves.
		EditorCommand::LineEnd => match index.line_end(registry, root, line) {
			Some(CaretLookup::Known((byte, col))) => {
				caret.place_at_known(registry, root, topology_rev, state_id, byte, line, col);
			}
			Some(CaretLookup::Pending) => {
				carried.borrow_mut().scan_to_end(index.work_key(root), line)
			}
			None => {}
		},
		_ => {
			// The first non-whitespace cell within one extent of the line's
			// start; past that, the line's start.
			let window = index.reserve_read(COLD_INGEST_EXTENT_SIZE) as u64;
			if window == 0 {
				let mut demand = LayoutDemand::default();
				demand.note(None);
				carried.borrow_mut().carry(index.work_key(root), demand);
				return;
			}
			let limit = DocByte::new(end.get().min(start.get().saturating_add(window)));

			let mut bytes = Vec::new();
			if registry
				.for_each_loaded_slice_fragment(root, start, limit, |fragment| {
					bytes.extend_from_slice(fragment)
				})
				.is_err()
			{
				let at = DocByte::new(start.get().saturating_add(bytes.len() as u64));
				let node = registry.find_node_at_doc_byte_raw(root, at).node_id;
				let mut demand = LayoutDemand::default();
				demand.note(Some(node));
				carried.borrow_mut().carry(index.work_key(root), demand);
				return;
			}
			let first = first_non_whitespace_visual_col(&bytes, DocLine::ZERO);
			let col = match motion {
				EditorCommand::SmartHome if current_col == first => DocDisplayCol::ZERO,
				_ => first,
			};
			place_cursor_at_line_col_indexed(
				registry,
				root,
				folds,
				line,
				col,
				false,
				(topology_rev, state_id),
				caret,
				Some(index),
			);
		}
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
		let gutter_numbering = tab.gutter_numbering();
		let demand = std::cell::RefCell::new(LayoutDemand::default());
		let view = &mut *tab.view;
		let indexed = caret_index(
			registry,
			root,
			&view.common,
			gutter_numbering,
			&demand,
			state_id,
			topology_rev,
		);
		let folds = &mut view.folds;
		tab.cursor.visit_carets_mut(|caret| {
			pending |= !caret.resolve_indexed(
				registry,
				root,
				folds,
				topology_rev,
				state_id,
				Some(indexed),
			);
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
	let demand = std::cell::RefCell::new(LayoutDemand::default());
	let view = &mut *tab.view;
	if !tab.cursor.primary_mut().resolve_indexed(
		registry,
		root,
		&mut view.folds,
		topology_rev,
		state_id,
		Some(caret_index(
			registry,
			root,
			&view.common,
			gutter_numbering,
			&demand,
			state_id,
			topology_rev,
		)),
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
			&common.wrap_index,
			&common.carried,
			true,
			&mut ui.status_message,
			&mut ui.mode_override,
			command,
		)
	};
	let state_id = tab.document.ledger.current_state_id;
	let view = &mut *tab.view;
	tab.cursor.primary_mut().resolve_indexed(
		registry,
		root,
		&mut view.folds,
		topology_rev,
		state_id,
		Some(caret_index(
			registry,
			root,
			&view.common,
			gutter_numbering,
			&demand,
			state_id,
			topology_rev,
		)),
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
		&tab.view.common.wrap_index,
		&tab.view.common.carried,
		true,
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
		let demand = std::cell::RefCell::new(LayoutDemand::default());
		let view = &mut *tab.view;
		let indexed = caret_index(
			registry,
			root,
			&view.common,
			gutter_numbering,
			&demand,
			current_state_id,
			topology_rev,
		);
		let folds = &mut view.folds;
		let Some(cursors) = tab.cursor.points_mut() else {
			return CommandEffect::default();
		};
		// Every caret's motion shares one frame's read budget.
		view.common.wrap_index.borrow_mut().begin_frame();
		for (index, caret) in cursors.all_mut().enumerate() {
			if !caret.resolve_indexed(
				registry,
				root,
				folds,
				topology_rev,
				current_state_id,
				Some(indexed),
			) {
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
				&view.common.wrap_index,
				&view.common.carried,
				false,
				&mut status_message,
				&mut mode_override,
				command.clone(),
			);
			caret.resolve_indexed(
				registry,
				root,
				folds,
				topology_rev,
				current_state_id,
				Some(indexed),
			);
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
	let gutter_numbering = tab.gutter_numbering();
	let demand = std::cell::RefCell::new(LayoutDemand::default());
	let Some(cursors) = tab.cursor.points_mut() else {
		return CommandEffect::default();
	};
	let mut caret = *cursors.primary();
	let view = &mut *tab.view;
	let indexed = caret_index(
		registry,
		root,
		&view.common,
		gutter_numbering,
		&demand,
		state_id,
		topology_rev,
	);
	let placed = caret.place_at_line_col_indexed(
		registry,
		root,
		&mut view.folds,
		topology_rev,
		state_id,
		target.line,
		target.col,
		true,
		super::super::session::Affinity::After,
		Some(indexed),
	);
	if !placed {
		// Placement left the target pending, naming any node it needs.
		ui.status_message = Some("Line index is still building".to_string());
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
	let gutter_numbering = tab.gutter_numbering();
	let demand = std::cell::RefCell::new(LayoutDemand::default());
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
		let view = &mut *tab.view;
		let placed = caret.place_at_line_col_indexed(
			registry,
			root,
			&mut view.folds,
			topology_rev,
			state_id,
			target_line,
			primary.preferred_col,
			false,
			super::super::session::Affinity::After,
			Some(caret_index(
				registry,
				root,
				&view.common,
				gutter_numbering,
				&demand,
				state_id,
				topology_rev,
			)),
		);
		if !placed {
			// Placement left the target pending, naming any node it needs.
			ui.status_message = Some("Line index is still building".to_string());
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
	let view = &mut *tab.view;
	let placed = caret.place_at_line_col_indexed(
		registry,
		root,
		&mut view.folds,
		topology_rev,
		state_id,
		target_line,
		primary.preferred_col,
		false,
		super::super::session::Affinity::After,
		Some(caret_index(
			registry,
			root,
			&view.common,
			gutter_numbering,
			&demand,
			state_id,
			topology_rev,
		)),
	);
	if !placed {
		// Placement left the target pending, naming any node it needs.
		ui.status_message = Some("Line index is still building".to_string());
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
	wrap_index: &std::cell::RefCell<super::super::wrap_index::WrapIndex>,
	carried: &std::cell::RefCell<super::super::layout::CarriedWork>,
	new_frame: bool,
	status_message: &mut Option<String>,
	mode_override: &mut Option<EditorMode>,
	command: EditorCommand,
) -> CommandEffect {
	// A command is its own frame for the wrap index: it may read one budget,
	// so clamping the cursor into a scrolled view can place it (ADR 0009). A
	// command over several carets is one frame, begun by its caller.
	if new_frame {
		wrap_index.borrow_mut().begin_frame();
	}
	let wrap_demand = std::cell::RefCell::new(super::super::layout::LayoutDemand::default());
	let wrap = || {
		Some(super::super::layout::WrapLayout::new(
			wrap_index,
			current_state_id,
			topology_rev,
			&wrap_demand,
		))
	};
	// Caret placement takes indexed lines through the index (ADR 0010), at
	// the render's text width so they share its checkpoints.
	let caret_text_width = root_id.map_or(0, |rid| {
		viewport_geometry_for_viewport_with_numbering(
			DocLine::new(*scroll_y),
			*viewport_lines,
			DocLine::new(registry.get_total_newlines(rid)),
			*viewport_width,
			minimap_enabled,
			gutter_numbering,
		)
		.text_width
	});
	let indexed = || {
		wrap()
			.map(|wrap| super::super::layout::CaretIndex::new(wrap, wrap_enabled, caret_text_width))
	};
	// The caret index and the line's range, when the line is on its path.
	let on_index = |rid: NodeId, line: DocLine| {
		indexed().and_then(|index| {
			index
				.indexed_line(registry, rid, line)
				.map(|range| (index, range))
		})
	};
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
					MoveDirection::Left => match on_index(rid, target_line) {
						// One cell back: the start of the cell holding the byte
						// before the caret (ADR 0010).
						Some((index, CaretLookup::Known((start, _)))) => {
							target_col = if caret.byte() > start {
								match index.cell_at_byte(
									registry,
									rid,
									caret.byte().saturating_sub(1),
								) {
									Some(CaretLookup::Known((_, _, col))) => col,
									_ => target_col,
								}
							} else {
								DocDisplayCol::ZERO
							};
						}
						Some((_, CaretLookup::Pending)) => {}
						None => {
							if let Ok(line_bytes) =
								read_line_bytes_sparse(registry, rid, target_line, false)
							{
								target_col =
									step_left_visual_col(&line_bytes, DocLine::ZERO, target_col);
							} else {
								target_col = target_col.saturating_sub(1);
							}
						}
					},
					// One cell on: placement resolves a column to the first cell
					// boundary at or after it.
					MoveDirection::Right if on_index(rid, target_line).is_some() => {
						target_col = caret.abs_col().saturating_add(1);
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
						let motion = match dir {
							MoveDirection::NextWord => WordMotion::NextStart,
							MoveDirection::PrevWord => WordMotion::PrevStart,
							_ => WordMotion::NextEnd,
						};
						// A window of one extent beside the caret, not the whole
						// document (ADR 0010).
						let window = indexed().map_or(COLD_INGEST_EXTENT_SIZE, |index| {
							index.reserve_read(COLD_INGEST_EXTENT_SIZE)
						});
						match word_motion_target(registry, rid, caret.byte(), motion, window as u64)
						{
							WordTarget::Byte(byte) => {
								let found = match indexed()
									.and_then(|index| index.line_col_for_byte(registry, rid, byte))
								{
									Some(CaretLookup::Known(found)) => Some(found),
									Some(CaretLookup::Pending) => None,
									None => line_col_from_doc_byte_sparse(registry, rid, byte).ok(),
								};
								if let Some((line, col)) = found {
									target_line = line;
									target_col = col;
								}
							}
							WordTarget::Pending(node) => wrap_demand.borrow_mut().note(Some(node)),
							WordTarget::Wait => wrap_demand.borrow_mut().note(None),
							WordTarget::Stay => {}
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
				place_cursor_at_line_col_indexed(
					registry,
					rid,
					folds,
					target_line,
					target_col,
					false,
					(topology_rev, current_state_id),
					caret,
					indexed(),
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
				place_cursor_at_line_col_indexed(
					registry,
					rid,
					folds,
					target_line,
					target_col,
					true,
					(topology_rev, current_state_id),
					caret,
					indexed(),
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
				&& let Some((index, range)) = on_index(rid, target_line)
			{
				if let CaretLookup::Known((start, end)) = range {
					indexed_line_motion(
						registry,
						rid,
						folds,
						caret,
						&line_motion,
						target_line,
						target_col,
						(start, end),
						index,
						carried,
						(topology_rev, current_state_id),
					);
				}
			} else if let Some(rid) = root_id
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
				place_cursor_at_line_col_indexed(
					registry,
					rid,
					folds,
					target_line,
					target_col,
					false,
					(topology_rev, current_state_id),
					caret,
					indexed(),
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
				place_cursor_at_line_col_indexed(
					registry,
					rid,
					folds,
					target_line,
					target_col,
					false,
					(topology_rev, current_state_id),
					caret,
					indexed(),
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
				place_cursor_at_line_col_indexed(
					registry,
					rid,
					folds,
					target_line,
					target_col,
					false,
					(topology_rev, current_state_id),
					caret,
					indexed(),
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
									place_cursor_at_line_col_indexed(
										registry,
										rid,
										folds,
										target_line,
										target_col,
										true,
										(topology_rev, current_state_id),
										caret,
										indexed(),
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
						wrap(),
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
				place_cursor_at_line_col_indexed(
					registry,
					rid,
					folds,
					target_line,
					target_col,
					false,
					(topology_rev, current_state_id),
					caret,
					indexed(),
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
						wrap(),
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
						wrap(),
					);
					let previous_view = (*scroll_y, *scroll_row_offset);
					*scroll_y = anchor.line.get();
					*scroll_row_offset = anchor.row_offset;
					let sum_tree =
						refresh_sum_tree(cached_sum_tree, registry, rid, current_state_id);
					let clamped = clamp_cursor_to_viewport_anchor_with_sum_tree(
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
						wrap(),
					);
					match clamped {
						Some(target) if target.line != target_line || target.col != target_col => {
							match target.byte {
								// A row of an indexed line: place the caret from the
								// index, without reading the line from its start.
								Some(byte) => caret.place_at_known(
									registry,
									rid,
									topology_rev,
									current_state_id,
									byte,
									target.line,
									target.col,
								),
								None => {
									place_cursor_at_line_col_indexed(
										registry,
										rid,
										folds,
										target.line,
										target.col,
										false,
										(topology_rev, current_state_id),
										caret,
										indexed(),
									);
								}
							}
						}
						Some(_) => {}
						// The cursor's new place is out of reach this command: keep
						// the view where it was rather than move the cursor somewhere
						// guessed (ADR 0009). The next command continues.
						None => (*scroll_y, *scroll_row_offset) = previous_view,
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
						place_cursor_at_line_col_indexed(
							registry,
							rid,
							folds,
							clamped_cursor_line,
							target_col,
							false,
							(topology_rev, current_state_id),
							caret,
							indexed(),
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
						wrap(),
					);
					let previous_view = (*scroll_y, *scroll_row_offset);
					*scroll_y = anchor.line.get();
					*scroll_row_offset = anchor.row_offset;
					let sum_tree =
						refresh_sum_tree(cached_sum_tree, registry, rid, current_state_id);
					let clamped = clamp_cursor_to_viewport_anchor_with_sum_tree(
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
						wrap(),
					);
					match clamped {
						Some(target) if target.line != target_line || target.col != target_col => {
							match target.byte {
								// A row of an indexed line: place the caret from the
								// index, without reading the line from its start.
								Some(byte) => caret.place_at_known(
									registry,
									rid,
									topology_rev,
									current_state_id,
									byte,
									target.line,
									target.col,
								),
								None => {
									place_cursor_at_line_col_indexed(
										registry,
										rid,
										folds,
										target.line,
										target.col,
										false,
										(topology_rev, current_state_id),
										caret,
										indexed(),
									);
								}
							}
						}
						Some(_) => {}
						// The cursor's new place is out of reach this command: keep
						// the view where it was rather than move the cursor somewhere
						// guessed (ADR 0009). The next command continues.
						None => (*scroll_y, *scroll_row_offset) = previous_view,
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
						place_cursor_at_line_col_indexed(
							registry,
							rid,
							folds,
							clamped_cursor_line,
							target_col,
							false,
							(topology_rev, current_state_id),
							caret,
							indexed(),
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
				place_cursor_at_line_col_indexed(
					registry,
					rid,
					folds,
					target_line,
					target_col,
					true,
					(topology_rev, current_state_id),
					caret,
					indexed(),
				);
			}
		}
		_ => unreachable!(),
	}
	if let Some(root) = root_id {
		caret.resolve_indexed(
			registry,
			root,
			folds,
			topology_rev,
			current_state_id,
			indexed(),
		);
		// What the command's lookups could not reach, render carries on.
		if let Some(index) = indexed() {
			let key = index.work_key(root);
			carried.borrow_mut().carry(key, wrap_demand.take());
		}
	}

	CommandEffect::render()
}

#[cfg(test)]
mod tests {
	use super::{FindCharOutcome, find_char_target_col};
	use crate::core::{DisplayRow, DocByte, DocDisplayCol, DocLine, StateId, TopologyRev};
	use crate::engine::document::cursor::{
		next_word_end, next_word_start, prev_word_start, step_left_visual_col,
		step_right_visual_col,
	};
	use crate::engine::folding::FoldSet;
	use crate::engine::protocol::MoveDirection;
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

	/// One line in two leaves, `head` then `tail`, whose bytes the test can
	/// make cold by clearing them.
	fn two_leaf_document(
		head: &[u8],
		tail: &[u8],
	) -> (
		crate::ecs::UastRegistry,
		crate::ecs::NodeId,
		crate::ecs::NodeId,
		crate::ecs::NodeId,
	) {
		use crate::uast::kind::SemanticKind;
		use crate::uast::metrics::SpanMetrics;
		let registry = crate::ecs::UastRegistry::new(8);
		let mut chunk = registry.reserve_chunk(3).expect("OOM");
		let metrics = |bytes: usize| SpanMetrics {
			byte_length: bytes as u64,
			newlines: 0,
		};
		let root = chunk.spawn_node(
			SemanticKind::RelationalTable,
			None,
			metrics(head.len() + tail.len()),
		);
		let head_node = chunk.spawn_node(SemanticKind::Token, None, metrics(head.len()));
		let tail_node = chunk.spawn_node(SemanticKind::Token, None, metrics(tail.len()));
		chunk.append_local_child(root, head_node);
		chunk.append_local_child(root, tail_node);
		unsafe {
			*registry.virtual_data[head_node.index()].get() = Some(head.to_vec());
			*registry.virtual_data[tail_node.index()].get() = Some(tail.to_vec());
		}
		drop(chunk);
		(registry, root, head_node, tail_node)
	}

	fn set_resident(
		registry: &crate::ecs::UastRegistry,
		node: crate::ecs::NodeId,
		bytes: Option<&[u8]>,
	) {
		unsafe {
			*registry.virtual_data[node.index()].get() = bytes.map(<[u8]>::to_vec);
		}
	}

	/// A text view's navigation state, driven command by command, with render's
	/// carried-work step between commands.
	struct Nav<'a> {
		registry: &'a crate::ecs::UastRegistry,
		root: crate::ecs::NodeId,
		folds: FoldSet,
		caret: Caret,
		active_visual: Option<(DocByte, crate::engine::protocol::VisualKind)>,
		scroll_y: u64,
		scroll_row_offset: DisplayRow,
		viewport_width: u16,
		viewport_lines: u32,
		cached_sum_tree: Option<crate::engine::session::CachedSumTree>,
		wrap_index: std::cell::RefCell<crate::engine::wrap_index::WrapIndex>,
		carried: std::cell::RefCell<crate::engine::layout::CarriedWork>,
	}

	impl<'a> Nav<'a> {
		fn new(registry: &'a crate::ecs::UastRegistry, root: crate::ecs::NodeId) -> Self {
			Self {
				registry,
				root,
				folds: FoldSet::default(),
				caret: resolved_caret(registry, root, DocByte::ZERO),
				active_visual: None,
				scroll_y: 0,
				scroll_row_offset: DisplayRow::ZERO,
				viewport_width: 80,
				viewport_lines: 24,
				cached_sum_tree: None,
				wrap_index: std::cell::RefCell::default(),
				carried: std::cell::RefCell::default(),
			}
		}

		fn with_limits(
			registry: &'a crate::ecs::UastRegistry,
			root: crate::ecs::NodeId,
			limits: crate::engine::wrap_index::WrapIndexLimits,
		) -> Self {
			let nav = Self::new(registry, root);
			*nav.wrap_index.borrow_mut() =
				crate::engine::wrap_index::WrapIndex::with_limits(limits);
			nav
		}

		fn run(&mut self, command: crate::engine::protocol::EditorCommand) {
			self.run_in_frame(command, true);
		}

		/// `new_frame: false` continues the frame, as later carets of one
		/// multi-cursor command do.
		fn run_in_frame(
			&mut self,
			command: crate::engine::protocol::EditorCommand,
			new_frame: bool,
		) {
			let mut status_message = None;
			let mut mode_override = None;
			super::handle_navigation_command(
				self.registry,
				Some(self.root),
				&mut self.folds,
				&mut self.caret,
				&mut self.active_visual,
				&mut self.scroll_y,
				&mut self.scroll_row_offset,
				&mut self.viewport_width,
				&mut self.viewport_lines,
				true,
				false,
				None,
				TopologyRev::ZERO,
				StateId::ZERO,
				&mut self.cached_sum_tree,
				&self.wrap_index,
				&self.carried,
				new_frame,
				&mut status_message,
				&mut mode_override,
				command,
			);
		}

		/// Scroll until the view stops: the index has scanned the line.
		fn scan(&mut self) {
			let mut last = None;
			for _ in 0..1_000 {
				self.run(crate::engine::protocol::EditorCommand::ScrollViewport(
					1_000_000,
				));
				if last == Some((self.scroll_y, self.scroll_row_offset)) {
					break;
				}
				last = Some((self.scroll_y, self.scroll_row_offset));
			}
		}

		/// Render's frames until a pending caret target settles, as the engine
		/// renders between commands.
		fn settle(&mut self) {
			for _ in 0..1_000 {
				if self.caret.pending_target().is_none() {
					return;
				}
				self.render_frame();
			}
			panic!("the caret target never settled");
		}

		/// Render's step for carried work and caret resolution, at navigation's
		/// key.
		fn render_frame(&mut self) -> crate::engine::layout::LayoutDemand {
			self.wrap_index.borrow_mut().begin_frame();
			let demand = std::cell::RefCell::new(crate::engine::layout::LayoutDemand::default());
			let text_width =
				super::super::super::layout::viewport_geometry_for_viewport_with_numbering(
					DocLine::new(self.scroll_y),
					self.viewport_lines,
					DocLine::new(self.registry.get_total_newlines(self.root)),
					self.viewport_width,
					false,
					None,
				)
				.text_width;
			let index = crate::engine::layout::CaretIndex::new(
				crate::engine::layout::WrapLayout::new(
					&self.wrap_index,
					StateId::ZERO,
					TopologyRev::ZERO,
					&demand,
				),
				true,
				text_width,
			);
			let (caret, folds) = (&mut self.caret, &mut self.folds);
			crate::engine::layout::before_layout(
				&self.carried,
				index,
				self.registry,
				self.root,
				|| {
					caret.resolve_indexed(
						self.registry,
						self.root,
						folds,
						TopologyRev::ZERO,
						StateId::ZERO,
						Some(index),
					);
				},
			)
		}

		fn at(&self) -> (u64, u64) {
			(self.caret.byte().get(), self.caret.abs_col().get())
		}
	}

	/// Where a column of `line` resolves, as the whole-line helper puts it.
	fn whole_at_col(line: &[u8], col: DocDisplayCol) -> (u64, u64) {
		let (byte, col) = crate::core::text::byte_offset_for_display_col(
			line,
			col.get(),
			0,
			crate::core::TAB_SIZE,
		);
		(byte as u64, col)
	}

	#[test]
	fn motions_on_an_indexed_line_match_the_whole_line() {
		use crate::engine::protocol::EditorCommand;
		let mut head = b"  \t".to_vec();
		head.extend(std::iter::repeat_n(b'a', 1 << 20));
		// Longer than the index's checkpoint spacing, so the positions below have
		// a checkpoint inside the tail: a walk reads from the nearest checkpoint
		// before its target, and none of them is in the cold head.
		let tail = "x 日本\tb  c\té d foo_bar ".repeat(4_000);
		let line = [head.as_slice(), tail.as_bytes()].concat();
		let text = std::str::from_utf8(&line).expect("utf-8");
		let (registry, root, head_node, _) = two_leaf_document(&head, tail.as_bytes());
		let mut nav = Nav::new(&registry, root);
		nav.scan();
		let width = crate::core::text::display_width(&line, 0, crate::core::TAB_SIZE);

		// Words, with the line resident: the window gives the whole line's answer.
		nav.run(EditorCommand::ClickCursor(
			crate::core::CursorPosition::new(DocLine::ZERO, DocDisplayCol::new(width - 400)),
		));
		for (dir, rule) in [
			(
				MoveDirection::NextWord,
				next_word_start as fn(&str, usize) -> usize,
			),
			(MoveDirection::NextWord, next_word_start),
			(MoveDirection::NextWordEnd, next_word_end),
			(MoveDirection::NextWordEnd, next_word_end),
			(MoveDirection::PrevWord, prev_word_start),
			(MoveDirection::PrevWord, prev_word_start),
			(MoveDirection::PrevWord, prev_word_start),
		] {
			let byte = rule(text, nav.caret.byte().get() as usize);
			let col = crate::core::text::display_col_for_byte_offset(
				&line,
				byte,
				0,
				crate::core::TAB_SIZE,
			);
			nav.run(EditorCommand::MoveCursor(dir.clone()));
			assert_eq!(nav.at(), (byte as u64, col), "{dir:?}");
		}

		// The rest with the line's head cold: nothing reads the line from its start.
		set_resident(&registry, head_node, None);
		// The tail's cell boundaries, from the head's width: the whole line's
		// answer for every step below without walking a mebibyte each time.
		let mut boundaries = vec![(
			head.len() as u64,
			crate::core::text::display_width(&head, 0, crate::core::TAB_SIZE),
		)];
		crate::core::text::visit_display_cells(tail.as_bytes(), |cell| {
			let (_, col) = *boundaries.last().expect("a start");
			let next = crate::core::text::advance_display_col(col, cell, crate::core::TAB_SIZE);
			boundaries.push(((head.len() + cell.byte_end) as u64, next));
			true
		});
		// Right, Right, Left: a net cell per round, so over more than one
		// repeat of the tail Left starts after every kind of cell.
		for step in 0..90 {
			let col = nav.caret.abs_col().get();
			let (dir, expected) = if step % 3 == 2 {
				let at = boundaries.partition_point(|&(_, start)| start < col);
				(MoveDirection::Left, boundaries[at.saturating_sub(1)])
			} else {
				let at = boundaries.partition_point(|&(_, start)| start <= col);
				(
					MoveDirection::Right,
					boundaries[at.min(boundaries.len() - 1)],
				)
			};
			nav.run(EditorCommand::MoveCursor(dir.clone()));
			assert_eq!(nav.at(), expected, "{dir:?} from {col}");
		}
		// A step each way agrees with the whole-line helpers themselves.
		let col = nav.caret.abs_col();
		nav.run(EditorCommand::MoveCursor(MoveDirection::Right));
		assert_eq!(
			nav.at(),
			whole_at_col(&line, step_right_visual_col(&line, DocLine::ZERO, col))
		);
		let col = nav.caret.abs_col();
		nav.run(EditorCommand::MoveCursor(MoveDirection::Left));
		assert_eq!(
			nav.at(),
			whole_at_col(&line, step_left_visual_col(&line, DocLine::ZERO, col))
		);
		nav.run(EditorCommand::LineEnd);
		assert_eq!(nav.at(), (line.len() as u64, width), "End");
		nav.run(EditorCommand::LineStart);
		assert_eq!(nav.at(), (0, 0), "Home");
	}

	#[test]
	fn end_on_an_unscanned_line_waits_while_render_scans_then_moves() {
		use crate::engine::protocol::EditorCommand;
		let head = vec![b'a'; 3 << 20];
		let tail = b" end\tof the line".to_vec();
		let line = [head.as_slice(), tail.as_slice()].concat();
		let (registry, root, _, _) = two_leaf_document(&head, &tail);
		let mut nav = Nav::new(&registry, root);
		nav.run(EditorCommand::LineEnd);
		assert_eq!(nav.at(), (0, 0), "End waits in place");
		assert_eq!(nav.carried.borrow().pending_scan(), Some(DocLine::ZERO));
		let mut frames = 0;
		while nav.carried.borrow().pending_scan().is_some() {
			let demand = nav.render_frame();
			assert!(demand.pending.is_empty(), "the line is resident");
			frames += 1;
			assert!(frames < 100, "the scan never finished");
			assert_eq!(nav.at(), (0, 0), "the caret stays while render scans");
		}
		assert!(frames > 1, "the scan took several frames");
		nav.run(EditorCommand::LineEnd);
		let width = crate::core::text::display_width(&line, 0, crate::core::TAB_SIZE);
		assert_eq!(nav.at(), (line.len() as u64, width), "a second End moves");
	}

	#[test]
	fn a_cold_window_requests_its_node_and_a_later_command_moves() {
		use crate::engine::protocol::EditorCommand;
		// A word motion whose window reaches a cold leaf.
		let (registry, root, _, tail_node) = two_leaf_document(b"hello ", b"world foo");
		let mut nav = Nav::new(&registry, root);
		set_resident(&registry, tail_node, None);
		nav.run(EditorCommand::MoveCursor(MoveDirection::NextWord));
		assert_eq!(nav.at(), (0, 0), "unavailable bytes are not a boundary");
		assert_eq!(
			nav.render_frame().pending,
			vec![tail_node],
			"render hydrates the node"
		);
		set_resident(&registry, tail_node, Some(b"world foo"));
		nav.run(EditorCommand::MoveCursor(MoveDirection::NextWord));
		assert_eq!(nav.at(), (6, 6));

		// A line-head window on an indexed line whose head is cold.
		let mut head = b"  \t".to_vec();
		head.extend(std::iter::repeat_n(b'a', 1 << 20));
		let (registry, root, head_node, _) = two_leaf_document(&head, b" tail");
		let mut nav = Nav::new(&registry, root);
		nav.scan();
		nav.run(EditorCommand::LineEnd);
		let end = nav.at();
		set_resident(&registry, head_node, None);
		nav.run(EditorCommand::FirstNonWhitespace);
		assert_eq!(nav.at(), end, "the caret stays");
		assert_eq!(nav.render_frame().pending, vec![head_node]);
		set_resident(&registry, head_node, Some(&head));
		nav.run(EditorCommand::FirstNonWhitespace);
		assert_eq!(nav.at(), (3, 4), "after the leading spaces and tab");
	}

	#[test]
	fn motion_windows_and_their_lookups_share_one_frame_budget() {
		use crate::engine::protocol::EditorCommand;
		const BUDGET: usize = 8 * 1024;
		let head = vec![b'a'; 1 << 20];
		let (registry, root, _, _) = two_leaf_document(&head, b" end");
		let limits = crate::engine::wrap_index::WrapIndexLimits {
			read_budget: BUDGET,
			checkpoint_bytes: 1024,
			line_bytes: 4 << 20,
			max_lines: 4,
		};
		let mut nav = Nav::with_limits(&registry, root, limits);
		nav.scan();
		nav.caret = resolved_caret(&registry, root, DocByte::new(1_000));

		// The next word boundary is a mebibyte away: each press reads at most
		// half the frame's budget for its window, and still makes progress.
		let mut progressed = false;
		for _ in 0..8 {
			let before = nav.caret.byte().get();
			nav.run(EditorCommand::MoveCursor(MoveDirection::NextWord));
			nav.settle();
			let moved = nav.caret.byte().get() - before;
			assert!(moved <= (BUDGET / 2) as u64, "a window of {moved} bytes");
			assert_eq!(nav.caret.abs_col().get(), nav.caret.byte().get());
			progressed |= moved > 0;
		}
		assert!(progressed, "repeated presses move");

		// Two carets in one frame share its budget: the second's window is at
		// most half of what the first left.
		let start = resolved_caret(&registry, root, DocByte::new(50_000));
		// Each window is fixed when its command runs; settling afterwards only
		// places the caret at the target already chosen.
		nav.caret = start;
		nav.run(EditorCommand::MoveCursor(MoveDirection::NextWord));
		let left = nav.wrap_index.borrow().budget_left();
		let first_caret = nav.caret;
		nav.caret = start;
		nav.run_in_frame(EditorCommand::MoveCursor(MoveDirection::NextWord), false);
		nav.settle();
		let second = nav.caret.byte().get() - 50_000;
		nav.caret = first_caret;
		nav.settle();
		let first = nav.caret.byte().get() - 50_000;
		assert!(
			second <= (left / 2) as u64,
			"the second caret read {second} of {left} left"
		);
		assert!(second < first.max(1), "the frame's budget was shared");
	}

	#[test]
	fn clicks_cached_carets_and_selections_on_a_cold_line_head_use_the_index() {
		use super::handle_navigation_command;
		use crate::core::{CursorPosition, DocByte, DocLine, StateId, TAB_SIZE};
		use crate::ecs::UastRegistry;
		use crate::engine::document::selection::{
			resolve_visual_ranges, resolve_visual_ranges_sparse,
		};
		use crate::engine::layout::{CaretIndex, LayoutDemand, WrapLayout};
		use crate::engine::protocol::{EditorCommand, EditorMode, VisualKind};
		use crate::engine::session::CachedSumTree;
		use crate::engine::wrap_index::WrapIndex;
		use crate::uast::kind::SemanticKind;
		use crate::uast::metrics::SpanMetrics;
		use std::cell::RefCell;

		// One line: a word long enough for the wrap index, then a run of
		// words, spaces and tabs, in two leaves.
		let word = vec![b'a'; 1 << 20];
		let run = b"ab\tc  \td\t\te ".repeat(300);
		let line = [word.as_slice(), run.as_slice()].concat();
		let registry = UastRegistry::new(8);
		let mut chunk = registry.reserve_chunk(3).expect("OOM");
		let metrics = |bytes: usize| SpanMetrics {
			byte_length: bytes as u64,
			newlines: 0,
		};
		let root = chunk.spawn_node(SemanticKind::RelationalTable, None, metrics(line.len()));
		let head = chunk.spawn_node(SemanticKind::Token, None, metrics(word.len()));
		let tail = chunk.spawn_node(SemanticKind::Token, None, metrics(run.len()));
		chunk.append_local_child(root, head);
		chunk.append_local_child(root, tail);
		unsafe {
			*registry.virtual_data[head.index()].get() = Some(word.clone());
			*registry.virtual_data[tail.index()].get() = Some(run.clone());
		}
		drop(chunk);

		let wrap_index = RefCell::new(WrapIndex::default());
		let mut folds = FoldSet::default();
		let mut active_visual: Option<(DocByte, VisualKind)> = None;
		let mut scroll_y = 0u64;
		let mut scroll_row_offset = DisplayRow::ZERO;
		let mut viewport_width = 80u16;
		let mut viewport_lines = 24u32;
		let mut cached_sum_tree: Option<CachedSumTree> = None;
		let mut status_message: Option<String> = None;
		let mut mode_override: Option<EditorMode> = None;
		let mut command = |caret: &mut Caret,
		                   folds: &mut FoldSet,
		                   scroll_y: &mut u64,
		                   scroll_row_offset: &mut DisplayRow,
		                   command: EditorCommand| {
			handle_navigation_command(
				&registry,
				Some(root),
				folds,
				caret,
				&mut active_visual,
				scroll_y,
				scroll_row_offset,
				&mut viewport_width,
				&mut viewport_lines,
				true,
				false,
				None,
				TopologyRev::ZERO,
				StateId::ZERO,
				&mut cached_sum_tree,
				&wrap_index,
				&Default::default(),
				true,
				&mut status_message,
				&mut mode_override,
				command,
			);
		};

		// Scan the line while it is resident, then make its head cold.
		let mut caret = resolved_caret(&registry, root, DocByte::ZERO);
		let mut last = None;
		for _ in 0..100 {
			command(
				&mut caret,
				&mut folds,
				&mut scroll_y,
				&mut scroll_row_offset,
				EditorCommand::ScrollViewport(1_000_000),
			);
			if last == Some((scroll_y, scroll_row_offset)) {
				break;
			}
			last = Some((scroll_y, scroll_row_offset));
		}
		unsafe {
			*registry.virtual_data[head.index()].get() = None;
		}
		let width = crate::core::text::display_width(&line, 0, TAB_SIZE);

		// A click deep in the line lands where the whole line puts it.
		for target in [width - 3, width - 40, width - 777] {
			command(
				&mut caret,
				&mut folds,
				&mut scroll_y,
				&mut scroll_row_offset,
				EditorCommand::ClickCursor(CursorPosition::new(
					DocLine::ZERO,
					DocDisplayCol::new(target),
				)),
			);
			let (byte, col) =
				crate::core::text::byte_offset_for_display_col(&line, target, 0, TAB_SIZE);
			assert!(
				caret.pending_target().is_none(),
				"click at {target} was placed"
			);
			assert_eq!(caret.byte().get(), byte as u64, "click at {target}");
			assert_eq!(
				caret.abs_col(),
				DocDisplayCol::new(col),
				"click at {target}"
			);
		}

		let demand = RefCell::new(LayoutDemand::default());
		let indexed = || {
			Some(CaretIndex::new(
				WrapLayout::new(&wrap_index, StateId::ZERO, TopologyRev::ZERO, &demand),
				true,
				super::super::super::layout::viewport_geometry_for_viewport_with_numbering(
					DocLine::ZERO,
					viewport_lines,
					DocLine::ZERO,
					viewport_width,
					false,
					None,
				)
				.text_width,
			))
		};
		// A caret entering with no cached position resolves it from the index.
		let deep = word.len() + run.len() - 9;
		let mut stale = Caret::at_start();
		stale.set_byte(
			DocByte::new(deep as u64),
			crate::engine::session::Affinity::After,
		);
		assert!(stale.resolve_indexed(
			&registry,
			root,
			&mut folds,
			TopologyRev::ZERO,
			StateId::ZERO,
			indexed(),
		));
		let col = crate::core::text::display_col_for_byte_offset(&line, deep, 0, TAB_SIZE);
		assert_eq!(
			(stale.abs_line(), stale.abs_col()),
			(DocLine::ZERO, DocDisplayCol::new(col))
		);

		// Selections deep in the line match the whole-buffer resolution.
		let anchor = DocByte::new((word.len() + run.len() - 300) as u64);
		let cursor = DocByte::new((word.len() + run.len() - 7) as u64);
		for kind in [VisualKind::Char, VisualKind::Line, VisualKind::Block] {
			let sparse = resolve_visual_ranges_sparse(
				&registry,
				root,
				&folds,
				anchor,
				cursor,
				kind,
				indexed(),
			)
			.unwrap_or_else(|error| panic!("{kind:?}: {error}"));
			assert_eq!(
				sparse,
				resolve_visual_ranges(anchor, cursor, kind, &line),
				"{kind:?}"
			);
		}
	}

	#[test]
	fn scrolling_into_trailing_whitespace_places_the_caret_without_the_line_start() {
		use super::handle_navigation_command;
		use crate::core::{DocByte, DocLine, StateId};
		use crate::ecs::UastRegistry;
		use crate::engine::protocol::{EditorCommand, EditorMode, VisualKind};
		use crate::engine::session::CachedSumTree;
		use crate::engine::wrap_index::WrapIndex;
		use crate::uast::kind::SemanticKind;
		use crate::uast::metrics::SpanMetrics;
		use std::cell::RefCell;

		// One line: a word long enough for the wrap index, then a trailing run
		// of spaces and tabs, in two leaves.
		let word = vec![b'a'; 1 << 20];
		let run = b"  \t \t\t   ".repeat(300);
		let registry = UastRegistry::new(8);
		let mut chunk = registry.reserve_chunk(3).expect("OOM");
		let metrics = |bytes: usize| SpanMetrics {
			byte_length: bytes as u64,
			newlines: 0,
		};
		let root = chunk.spawn_node(
			SemanticKind::RelationalTable,
			None,
			metrics(word.len() + run.len()),
		);
		let head = chunk.spawn_node(SemanticKind::Token, None, metrics(word.len()));
		let tail = chunk.spawn_node(SemanticKind::Token, None, metrics(run.len()));
		chunk.append_local_child(root, head);
		chunk.append_local_child(root, tail);
		unsafe {
			*registry.virtual_data[head.index()].get() = Some(word.clone());
			*registry.virtual_data[tail.index()].get() = Some(run.clone());
		}
		drop(chunk);

		let wrap_index = RefCell::new(WrapIndex::default());
		let mut folds = FoldSet::default();
		let mut active_visual: Option<(DocByte, VisualKind)> = None;
		let mut scroll_y = 0u64;
		let mut scroll_row_offset = DisplayRow::ZERO;
		let mut viewport_width = 80u16;
		let mut viewport_lines = 24u32;
		let mut cached_sum_tree: Option<CachedSumTree> = None;
		let mut status_message: Option<String> = None;
		let mut mode_override: Option<EditorMode> = None;
		let mut scroll =
			|caret: &mut Caret, scroll_y: &mut u64, scroll_row_offset: &mut DisplayRow| {
				handle_navigation_command(
					&registry,
					Some(root),
					&mut folds,
					caret,
					&mut active_visual,
					scroll_y,
					scroll_row_offset,
					&mut viewport_width,
					&mut viewport_lines,
					true,
					false,
					None,
					TopologyRev::ZERO,
					StateId::ZERO,
					&mut cached_sum_tree,
					&wrap_index,
					&Default::default(),
					true,
					&mut status_message,
					&mut mode_override,
					EditorCommand::ScrollViewport(1_000_000),
				);
			};

		// Scan the whole line: scroll until the view stops moving.
		let mut caret = resolved_caret(&registry, root, DocByte::ZERO);
		let mut last = None;
		for _ in 0..100 {
			scroll(&mut caret, &mut scroll_y, &mut scroll_row_offset);
			if last == Some((scroll_y, scroll_row_offset)) {
				break;
			}
			last = Some((scroll_y, scroll_row_offset));
		}

		// From the top again, with the word's leaf no longer resident: nothing
		// can read the line from its start.
		let mut caret = resolved_caret(&registry, root, DocByte::ZERO);
		scroll_y = 0;
		scroll_row_offset = DisplayRow::ZERO;
		unsafe {
			*registry.virtual_data[head.index()].get() = None;
		}
		for _ in 0..100 {
			scroll(&mut caret, &mut scroll_y, &mut scroll_row_offset);
			if caret.byte().get() >= word.len() as u64 {
				break;
			}
		}

		let byte = caret.byte().get() as usize;
		let col = caret.abs_col().get();
		assert!(
			byte >= word.len(),
			"the caret is in the trailing run: {byte}"
		);
		assert_eq!(caret.abs_line(), DocLine::ZERO);
		assert!(
			caret.pending_target().is_none(),
			"the caret was placed, not left pending"
		);
		// The byte names the cell holding the caret's column in the full line.
		let line = [word.as_slice(), run.as_slice()].concat();
		let at =
			crate::core::text::display_col_for_byte_offset(&line, byte, 0, crate::core::TAB_SIZE);
		let (_, after) = crate::core::text::byte_offset_for_display_col(
			&line[byte..],
			1,
			at,
			crate::core::TAB_SIZE,
		);
		assert!(
			at <= col && (at == col || col < after),
			"byte {byte} spans {at}..{after}, caret at {col}"
		);
		assert!(matches!(line[byte], b' ' | b'\t'));
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
			&Default::default(),
			&Default::default(),
			true,
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
			&Default::default(),
			&Default::default(),
			true,
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
			None,
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
		let mut visual_len = 0u64;
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
			&Default::default(),
			&Default::default(),
			true,
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
