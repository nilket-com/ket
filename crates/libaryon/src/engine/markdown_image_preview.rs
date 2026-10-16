use super::core::refresh_sum_tree;
use super::document::io::read_document_snapshot;
use super::folding::pan_scroll_y_to_keep_cursor_visible;
use super::layout::{
	ViewportAnchor, pan_viewport_anchor_to_keep_cursor_visible_with_sum_tree,
	viewport_geometry_for_viewport_with_numbering,
};
use super::protocol::{EditorCommand, MoveDirection, SplitDirection};
use super::session::{
	BufferContent, EditorPane, EditorSession, MarkdownImagePreviewState, MarkdownPreviewBuffer,
	MarkdownPreviewEditorMut, View, text_editor, with_markdown_preview_buffer_mut,
	with_text_buffer_mut,
};
use crate::core::{BufferId, DisplayRow, DocByte, DocLine, PaneId, Rgb, StateId, ViewId};
use crate::ecs::UastRegistry;
use crate::svp::markdown_provider::fenced_code_blocks;
use crate::svp::rust_highlight::{CATEGORY_COUNT, HighlightSpan};
use crate::uast::projection::{
	MarkdownCellIdentity, MarkdownCellSnapshot, MarkdownPreviewSnapshot,
};
use std::path::Path;

struct MarkdownSourceSnapshot {
	buffer_id: BufferId,
	view_id: ViewId,
	path: Option<String>,
	state_id: StateId,
	text: String,
	highlights: Vec<HighlightSpan>,
	theme_colors: [Option<Rgb>; CATEGORY_COUNT],
	semantic_revision: u64,
	cells: Vec<MarkdownCellSnapshot>,
	scroll_line: DocLine,
	viewport_line_count: u32,
}

#[derive(Clone, Copy)]
struct SourceViewport {
	scroll_line: DocLine,
	line_count: u32,
}

pub(crate) fn open_markdown_image_preview(
	registry: &UastRegistry,
	session: &mut EditorSession,
	serif: bool,
) -> Result<(), String> {
	let source_view_id = markdown_image_preview_state(session, session.focused_pane_id)
		.map(|state| state.source_view_id)
		.unwrap_or_else(|| {
			session
				.layout
				.find_pane(session.focused_pane_id)
				.expect("focused pane must exist")
				.active_tab()
				.id
		});
	let source_pane_id = session
		.layout
		.pane_id_for_view(source_view_id)
		.ok_or_else(|| "Markdown preview source view is unavailable".to_string())?;
	if let Some(existing_preview_id) = preview_for_source(session, source_view_id) {
		if let Some(view_id) = session
			.layout
			.find_pane(existing_preview_id)
			.map(|pane| pane.active_tab().id)
		{
			let _ = with_markdown_preview_buffer_mut(
				&mut session.buffers,
				&mut session.layout,
				view_id,
				|preview| {
					preview.document.state.serif = serif;
				},
			);
		}
		let activated = session
			.layout
			.find_pane_mut(source_pane_id)
			.expect("source pane must exist")
			.activate_view(source_view_id);
		if !activated {
			return Err("Markdown preview source view is unavailable".to_string());
		}
		session.focused_pane_id = source_pane_id;
		sync_markdown_image_preview_scroll(registry, session);
		return Ok(());
	}

	let source = markdown_source_snapshot(registry, session, source_view_id)?;
	let preview_view = create_markdown_preview_view(session, &source, serif, false);
	let preview_buffer_id = preview_view.buffer_id;
	let preview_pane = EditorPane::from_view(preview_view);

	if session
		.split_pane_with_new_pane_order(
			source_pane_id,
			SplitDirection::Vertical,
			preview_pane,
			false,
		)
		.is_none()
	{
		session.buffers.remove(preview_buffer_id);
		return Err("Unable to open Markdown preview".to_string());
	}
	session.focused_pane_id = source_pane_id;
	reset_global_preview_transients(session);
	Ok(())
}

/// Open a pinned preview pane rendering an in-memory markdown document (used
/// for :help). A pinned pane with the same title is reused: its content is
/// replaced, its scroll reset, and it is focused.
pub(crate) fn open_static_markdown_preview(
	session: &mut EditorSession,
	title: String,
	markdown: String,
) -> Result<(), String> {
	if let Some(existing_id) = pinned_preview_with_title(session, &title) {
		if let Some(view_id) = session
			.layout
			.find_pane(existing_id)
			.map(|pane| pane.active_tab().id)
		{
			let _ = with_markdown_preview_buffer_mut(
				&mut session.buffers,
				&mut session.layout,
				view_id,
				|preview| {
					preview.document.state.source_cells = markdown_cells(&markdown);
					preview.document.state.source_text = markdown;
					preview.document.state.source_scroll_line = DocLine::ZERO;
					preview.view.scroll_y = 0;
					preview.view.scroll_row_offset = DisplayRow::ZERO;
				},
			);
		}
		session.focused_pane_id = existing_id;
		reset_global_preview_transients(session);
		return Ok(());
	}

	let focused_pane_id = session.focused_pane_id;
	let (source_buffer_id, source_view_id, state_id, viewport_line_count) = {
		let view = session
			.layout
			.find_pane(focused_pane_id)
			.ok_or_else(|| "Focused pane is unavailable".to_string())?
			.active_tab();
		let state_id = session
			.buffers
			.get(view.buffer_id)
			.and_then(|buffer| buffer.text())
			.map_or(StateId::ZERO, |buffer| buffer.ledger.current_state_id);
		(
			view.buffer_id,
			view.id,
			state_id,
			view.common().viewport_lines,
		)
	};
	let cells = markdown_cells(&markdown);
	let source = MarkdownSourceSnapshot {
		buffer_id: source_buffer_id,
		view_id: source_view_id,
		path: Some(title.clone()),
		state_id,
		text: markdown,
		highlights: Vec::new(),
		theme_colors: [None; CATEGORY_COUNT],
		semantic_revision: 0,
		cells,
		scroll_line: DocLine::ZERO,
		viewport_line_count,
	};
	let preview_view = create_markdown_preview_view(session, &source, false, true);
	let preview_buffer_id = preview_view.buffer_id;
	let preview_pane = EditorPane::from_view(preview_view);

	if session
		.split_pane_with_new_pane_order(
			focused_pane_id,
			SplitDirection::Vertical,
			preview_pane,
			false,
		)
		.is_none()
	{
		session.buffers.remove(preview_buffer_id);
		return Err("Unable to open document preview".to_string());
	}
	reset_global_preview_transients(session);
	Ok(())
}

/// Handle scroll-like commands when the focused pane is a pinned preview.
/// Pinned panes have no source pane to redirect to, so scrolling adjusts the
/// preview's own anchor line, clamped to the document. Returns true when the
/// command was consumed.
pub(crate) fn scroll_pinned_preview(session: &mut EditorSession, cmd: &EditorCommand) -> bool {
	let focused_pane_id = session.focused_pane_id;
	let Some(state) = markdown_image_preview_state(session, focused_pane_id) else {
		return false;
	};
	if !state.pinned {
		return false;
	}
	let max_line = (state.source_text.lines().count() as u64).saturating_sub(1);
	let current = state.source_scroll_line.get();
	let page = u64::from(state.source_viewport_line_count.max(1));
	let target = match cmd {
		EditorCommand::Scroll(delta) | EditorCommand::ScrollViewport(delta) => {
			saturating_offset(current, *delta)
		}
		EditorCommand::ScrollViewportToLine(line) | EditorCommand::GotoLine(line) => line.get(),
		EditorCommand::PageUp => current.saturating_sub(page),
		EditorCommand::PageDown => current.saturating_add(page),
		EditorCommand::MoveCursor(MoveDirection::Up) => current.saturating_sub(1),
		EditorCommand::MoveCursor(MoveDirection::Down) => current.saturating_add(1),
		EditorCommand::MoveCursor(MoveDirection::Top) => 0,
		EditorCommand::MoveCursor(MoveDirection::Bottom) => max_line,
		_ => return false,
	};
	let clamped = target.min(max_line);
	let Some(view_id) = session
		.layout
		.find_pane(focused_pane_id)
		.map(|pane| pane.active_tab().id)
	else {
		return false;
	};
	let _ = with_markdown_preview_buffer_mut(
		&mut session.buffers,
		&mut session.layout,
		view_id,
		|preview| {
			preview.document.state.source_scroll_line = DocLine::new(clamped);
			preview.view.scroll_y = clamped;
			preview.view.scroll_row_offset = DisplayRow::ZERO;
		},
	);
	true
}

fn saturating_offset(current: u64, delta: i32) -> u64 {
	if delta < 0 {
		current.saturating_sub(u64::from(delta.unsigned_abs()))
	} else {
		current.saturating_add(delta as u64)
	}
}

pub(crate) fn refresh_markdown_image_previews(
	registry: &UastRegistry,
	session: &mut EditorSession,
) {
	let preview_ids = markdown_image_preview_pane_ids(session);
	for preview_id in preview_ids {
		let Some(state) = markdown_image_preview_state(session, preview_id) else {
			continue;
		};
		if state.pinned {
			continue;
		}

		let source = match markdown_source_snapshot(registry, session, state.source_view_id) {
			Ok(source) => source,
			Err(err) => {
				if let Some(view_id) = session
					.layout
					.find_pane(preview_id)
					.map(|pane| pane.active_tab().id)
				{
					let _ = with_markdown_preview_buffer_mut(
						&mut session.buffers,
						&mut session.layout,
						view_id,
						|preview| rebuild_markdown_image_preview_error(preview, &err),
					);
				}
				continue;
			}
		};

		if state.source_buffer_id == source.buffer_id
			&& state.source_path == source.path
			&& state.source_state_id == source.state_id
			&& state.source_semantic_revision == source.semantic_revision
			&& state.source_scroll_line == source.scroll_line
			&& state.source_viewport_line_count == source.viewport_line_count
		{
			continue;
		}

		if let Some(view_id) = session
			.layout
			.find_pane(preview_id)
			.map(|pane| pane.active_tab().id)
		{
			let _ = with_markdown_preview_buffer_mut(
				&mut session.buffers,
				&mut session.layout,
				view_id,
				|preview| update_markdown_image_preview(preview, &source),
			);
		}
	}

	sync_markdown_image_preview_scroll(registry, session);
}

pub(crate) fn sync_markdown_image_preview_scroll(
	registry: &UastRegistry,
	session: &mut EditorSession,
) -> bool {
	let previews = session
		.pane_ids()
		.into_iter()
		.filter_map(|pane_id| {
			let state = markdown_image_preview_state(session, pane_id)?;
			if state.pinned {
				return None;
			}
			Some((pane_id, state.source_view_id))
		})
		.collect::<Vec<_>>();

	// Apply cursor-visibility auto-pan to source panes now so the sync reads
	// the same scroll_y that the upcoming render will use. Without this,
	// commands that only move the cursor (PageUp/PageDown/GotoLine) leave the
	// preview one render behind.
	let source_ids: std::collections::HashSet<ViewId> =
		previews.iter().map(|(_, source_id)| *source_id).collect();
	for source_id in source_ids {
		let _ = with_text_buffer_mut(
			registry,
			&mut session.buffers,
			&mut session.layout,
			&mut session.events,
			source_id,
			|tab| {
				let rid = tab.document.root_id;
				let max_doc_line = DocLine::new(registry.get_total_newlines(rid));
				if tab.view.wrap_enabled {
					let state_id = tab.document.ledger.current_state_id;
					let geometry = viewport_geometry_for_viewport_with_numbering(
						DocLine::new(tab.view.scroll_y),
						tab.view.viewport_lines,
						max_doc_line,
						tab.view.viewport_width,
						tab.view.minimap_enabled,
						tab.gutter_numbering(),
					);
					let sum_tree = refresh_sum_tree(
						&mut tab.document.semantic.cached_sum_tree,
						registry,
						rid,
						state_id,
					);
					let anchor = pan_viewport_anchor_to_keep_cursor_visible_with_sum_tree(
						registry,
						rid,
						&tab.view.folds,
						ViewportAnchor {
							line: DocLine::new(tab.view.scroll_y),
							row_offset: tab.view.scroll_row_offset,
						},
						tab.cursor.abs_line(),
						tab.cursor.abs_col(),
						max_doc_line,
						tab.view.viewport_lines,
						true,
						geometry.text_width,
						sum_tree,
						Some(super::layout::WrapLayout::new(
							&tab.view.common.wrap_index,
							state_id,
							tab.document.topology_rev,
							&std::cell::RefCell::default(),
						)),
					);
					tab.view.scroll_y = anchor.line.get();
					tab.view.scroll_row_offset = anchor.row_offset;
				} else {
					tab.view.scroll_y = pan_scroll_y_to_keep_cursor_visible(
						registry,
						rid,
						&tab.view.folds,
						tab.view.scroll_y,
						tab.cursor.abs_line(),
						max_doc_line,
						tab.view.viewport_lines,
					);
				}
			},
		);
	}

	let mut changed = false;
	for (preview_id, source_id) in previews {
		let Some(source_viewport) = source_viewport(session, source_id) else {
			continue;
		};
		let Some(view_id) = session
			.layout
			.find_pane(preview_id)
			.map(|pane| pane.active_tab().id)
		else {
			continue;
		};
		if with_markdown_preview_buffer_mut(
			&mut session.buffers,
			&mut session.layout,
			view_id,
			|preview| {
				let changed = preview.document.state.source_scroll_line
					!= source_viewport.scroll_line
					|| preview.document.state.source_viewport_line_count
						!= source_viewport.line_count;
				if changed {
					preview.document.state.source_scroll_line = source_viewport.scroll_line;
					preview.document.state.source_viewport_line_count = source_viewport.line_count;
					preview.view.scroll_y = source_viewport.scroll_line.get();
					preview.view.scroll_row_offset = DisplayRow::ZERO;
				}
				changed
			},
		) == Some(true)
		{
			changed = true;
		}
	}
	changed
}

/// If the focused pane is a Markdown preview, returns the identity of its
/// source view so scroll-like commands can be routed to the exact tab.
pub(crate) fn scroll_target_view(session: &EditorSession) -> Option<ViewId> {
	let state = markdown_image_preview_state(session, session.focused_pane_id)?;
	if state.pinned {
		return None;
	}
	session.layout.find_view(state.source_view_id)?;
	Some(state.source_view_id)
}

pub(crate) fn markdown_image_preview_snapshot(
	buffer: &MarkdownPreviewBuffer,
) -> MarkdownPreviewSnapshot {
	let state = &buffer.state;
	MarkdownPreviewSnapshot {
		source_buffer_id: state.source_buffer_id,
		source_view_id: state.source_view_id,
		serif: state.serif,
		source_path: state.source_path.clone(),
		source_text: state.source_text.clone(),
		code_highlights: state.source_highlights.clone(),
		theme_colors: state.source_theme_colors,
		cells: state.source_cells.clone(),
		source_scroll_line: state.source_scroll_line,
		source_viewport_line_count: state.source_viewport_line_count,
	}
}

fn create_markdown_preview_view(
	session: &mut EditorSession,
	source: &MarkdownSourceSnapshot,
	serif: bool,
	pinned: bool,
) -> View {
	let title = if pinned {
		source.path.clone().unwrap_or_else(|| "_".to_string())
	} else {
		preview_title(source.path.as_deref())
	};
	let buffer_id = session
		.buffers
		.insert(BufferContent::MarkdownPreview(MarkdownPreviewBuffer {
			title,
			state: MarkdownImagePreviewState {
				source_buffer_id: source.buffer_id,
				source_view_id: source.view_id,
				serif,
				source_path: source.path.clone(),
				source_state_id: source.state_id,
				source_text: source.text.clone(),
				source_highlights: source.highlights.clone(),
				source_theme_colors: source.theme_colors,
				source_semantic_revision: source.semantic_revision,
				source_cells: source.cells.clone(),
				source_scroll_line: source.scroll_line,
				source_viewport_line_count: source.viewport_line_count,
				pinned,
			},
		}));
	let view_id = session.allocate_view_id();
	let mut view = View::new_markdown_preview(
		view_id,
		buffer_id,
		&session.persistent_settings,
		crate::core::DEFAULT_VIEWPORT_WIDTH,
		source.viewport_line_count,
	);
	view.common_mut().scroll_y = source.scroll_line.get();
	view
}

fn markdown_source_snapshot(
	registry: &UastRegistry,
	session: &EditorSession,
	source_view_id: ViewId,
) -> Result<MarkdownSourceSnapshot, String> {
	let view = session
		.layout
		.find_view(source_view_id)
		.ok_or_else(|| "Markdown preview source view is unavailable".to_string())?;
	let buffer = session
		.buffers
		.get(view.buffer_id)
		.ok_or_else(|| "Markdown preview source buffer is unavailable".to_string())?;
	if buffer.is_markdown_preview() {
		return Err("Focused pane is already a Markdown preview".to_string());
	}
	if buffer.is_file_browser() {
		return Err("File browser cannot be previewed as Markdown".to_string());
	}

	let tab = text_editor(buffer, view)
		.ok_or_else(|| "Markdown preview source is not a text buffer".to_string())?;
	let root = tab.document.root_id;
	let bytes = read_document_snapshot(registry, root, tab.document.backing_path.as_deref())?;
	let text = String::from_utf8_lossy(&bytes).into_owned();
	let cells = markdown_cells(&text);
	Ok(MarkdownSourceSnapshot {
		buffer_id: buffer.id,
		view_id: source_view_id,
		path: tab
			.document
			.file_path
			.as_ref()
			.map(|path| path.to_string_lossy().into_owned()),
		state_id: tab.document.ledger.current_state_id,
		text,
		highlights: tab.document.semantic.highlights.clone(),
		theme_colors: tab.view.current_theme.syntax_colors,
		semantic_revision: tab.document.semantic.view_revision,
		cells,
		scroll_line: DocLine::new(tab.view.scroll_y),
		viewport_line_count: tab.view.viewport_lines,
	})
}

fn update_markdown_image_preview(
	tab: &mut MarkdownPreviewEditorMut<'_>,
	source: &MarkdownSourceSnapshot,
) {
	let state = &mut tab.document.state;
	state.source_buffer_id = source.buffer_id;
	state.source_path = source.path.clone();
	state.source_state_id = source.state_id;
	state.source_text = source.text.clone();
	state.source_highlights = source.highlights.clone();
	state.source_theme_colors = source.theme_colors;
	state.source_semantic_revision = source.semantic_revision;
	state.source_cells = source.cells.clone();
	state.source_scroll_line = source.scroll_line;
	state.source_viewport_line_count = source.viewport_line_count;
	tab.document.title = preview_title(source.path.as_deref());
	tab.view.scroll_y = source.scroll_line.get();
	tab.view.scroll_row_offset = DisplayRow::ZERO;
}

pub fn markdown_cells(source: &str) -> Vec<MarkdownCellSnapshot> {
	fenced_code_blocks(source)
		.into_iter()
		.map(|block| {
			let body_range = DocByte::new(block.body_range.start as u64)
				..DocByte::new(block.body_range.end as u64);
			let identity = match block.explicit_id {
				Some(id) => MarkdownCellIdentity::Explicit { id },
				None => MarkdownCellIdentity::Implicit {
					occurrence: block.occurrence,
					body_range: body_range.clone(),
				},
			};
			MarkdownCellSnapshot {
				language_id: block.language_id,
				body_range,
				identity,
				presentation: block.presentation,
			}
		})
		.collect()
}

fn rebuild_markdown_image_preview_error(tab: &mut MarkdownPreviewEditorMut<'_>, error: &str) {
	tab.document.state.source_path = None;
	tab.document.state.source_text = format!("# Markdown preview unavailable\n\n{}\n", error);
	tab.view.scroll_y = 0;
	tab.view.scroll_row_offset = DisplayRow::ZERO;
}

fn preview_for_source(session: &EditorSession, source_view_id: ViewId) -> Option<PaneId> {
	session.pane_ids().into_iter().find(|pane_id| {
		markdown_image_preview_state(session, *pane_id)
			.is_some_and(|state| !state.pinned && state.source_view_id == source_view_id)
	})
}

fn pinned_preview_with_title(session: &EditorSession, title: &str) -> Option<PaneId> {
	session.pane_ids().into_iter().find(|pane_id| {
		markdown_image_preview_state(session, *pane_id)
			.is_some_and(|state| state.pinned && state.source_path.as_deref() == Some(title))
	})
}

fn markdown_image_preview_pane_ids(session: &EditorSession) -> Vec<PaneId> {
	session
		.pane_ids()
		.into_iter()
		.filter(|pane_id| markdown_image_preview_state(session, *pane_id).is_some())
		.collect()
}

fn markdown_image_preview_state(
	session: &EditorSession,
	pane_id: PaneId,
) -> Option<MarkdownImagePreviewState> {
	session
		.buffers
		.get(session.layout.find_pane(pane_id)?.active_tab().buffer_id)?
		.markdown_preview()
		.map(|buffer| buffer.state.clone())
}

fn source_viewport(session: &EditorSession, source_view_id: ViewId) -> Option<SourceViewport> {
	let view = session.layout.find_view(source_view_id)?;
	let buffer = session.buffers.get(view.buffer_id)?;
	let tab = text_editor(buffer, view)?;
	Some(SourceViewport {
		scroll_line: DocLine::new(tab.view.scroll_y),
		line_count: tab.view.viewport_lines,
	})
}

fn reset_global_preview_transients(session: &mut EditorSession) {
	session.search_confirm = None;
	session.ui.mode_override = None;
	session.ui.yank_flash = None;
}

fn preview_title(source_path: Option<&str>) -> String {
	let source_name = source_path
		.map(|path| {
			Path::new(path)
				.file_name()
				.and_then(|name| name.to_str())
				.unwrap_or(path)
				.to_string()
		})
		.unwrap_or_else(|| "_".to_string());
	format!("{} Preview", source_name)
}

#[cfg(test)]
mod tests {
	use super::*;

	fn test_session(registry: &UastRegistry) -> EditorSession {
		EditorSession::new(
			registry,
			"onedark".to_string(),
			true,
			true,
			true,
			false,
			true,
			true,
			crate::engine::settings::SaveDurability::default(),
			crate::engine::UiColorOverrides::default(),
			std::collections::BTreeMap::new(),
			None,
		)
	}

	fn open_help(session: &mut EditorSession, markdown: &str) {
		open_static_markdown_preview(session, "Baryon Help".to_string(), markdown.to_string())
			.expect("open static preview");
	}

	fn focused_state(session: &EditorSession) -> MarkdownImagePreviewState {
		markdown_image_preview_state(session, session.focused_pane_id).expect("pinned state")
	}

	#[test]
	fn preview_title_uses_source_file_name() {
		assert_eq!(
			preview_title(Some("/tmp/project/README.md")),
			"README.md Preview"
		);
		assert_eq!(preview_title(None), "_ Preview");
	}

	#[test]
	fn static_markdown_preview_opens_focused_pinned_pane() {
		let registry = UastRegistry::new(32);
		let mut session = test_session(&registry);
		open_help(&mut session, "# Help\n\nbody\n");

		let state = focused_state(&session);
		assert!(state.pinned);
		assert_eq!(state.source_text, "# Help\n\nbody\n");
		assert_eq!(state.source_path.as_deref(), Some("Baryon Help"));
		assert_eq!(state.source_scroll_line, DocLine::ZERO);
		assert_eq!(session.pane_ids().len(), 2);
	}

	#[test]
	fn reopening_a_pinned_title_reuses_the_pane() {
		let registry = UastRegistry::new(32);
		let mut session = test_session(&registry);
		open_help(&mut session, "first");
		scroll_pinned_preview(&mut session, &EditorCommand::PageDown);
		open_help(&mut session, "second\ncontent");

		assert_eq!(session.pane_ids().len(), 2, "should reuse the pinned pane");
		let state = focused_state(&session);
		assert_eq!(state.source_text, "second\ncontent");
		assert_eq!(state.source_scroll_line, DocLine::ZERO);
	}

	#[test]
	fn refresh_and_sync_leave_pinned_previews_alone() {
		let registry = UastRegistry::new(32);
		let mut session = test_session(&registry);
		let markdown = "l1\nl2\nl3\nl4\nl5\nl6\nl7\nl8\nl9\nl10\n";
		open_help(&mut session, markdown);
		scroll_pinned_preview(
			&mut session,
			&EditorCommand::MoveCursor(MoveDirection::Down),
		);
		assert_eq!(focused_state(&session).source_scroll_line, DocLine::new(1));

		refresh_markdown_image_previews(&registry, &mut session);
		sync_markdown_image_preview_scroll(&registry, &mut session);

		let state = focused_state(&session);
		assert_eq!(state.source_text, markdown, "refresh must not clobber text");
		assert_eq!(
			state.source_scroll_line,
			DocLine::new(1),
			"sync must not reset pinned scroll"
		);
	}

	#[test]
	fn preview_refresh_distinguishes_buffers_at_the_same_state() {
		let registry = UastRegistry::new(64);
		let mut session = test_session(&registry);
		open_markdown_image_preview(&registry, &mut session, false).expect("open preview");

		let source_view_id = session
			.layout
			.find_pane(session.focused_pane_id)
			.expect("source pane")
			.active_tab()
			.id;
		let original_buffer_id = session
			.layout
			.find_view(source_view_id)
			.expect("source view")
			.buffer_id;
		let (replacement, _) =
			super::super::session::TextBuffer::new_unnamed(&registry).expect("replacement buffer");
		let replacement_buffer_id = session.buffers.insert(BufferContent::Text(replacement));
		assert_ne!(original_buffer_id, replacement_buffer_id);
		assert_eq!(
			session
				.buffers
				.get(original_buffer_id)
				.and_then(|buffer| buffer.text())
				.expect("original text buffer")
				.ledger
				.current_state_id,
			session
				.buffers
				.get(replacement_buffer_id)
				.and_then(|buffer| buffer.text())
				.expect("replacement text buffer")
				.ledger
				.current_state_id,
			"fixture must exercise equal StateIds"
		);
		session
			.layout
			.find_view_mut(source_view_id)
			.expect("source view")
			.buffer_id = replacement_buffer_id;

		refresh_markdown_image_previews(&registry, &mut session);

		let preview_state = session
			.pane_ids()
			.into_iter()
			.filter_map(|pane_id| markdown_image_preview_state(&session, pane_id))
			.find(|state| !state.pinned)
			.expect("live preview");
		assert_eq!(preview_state.source_view_id, source_view_id);
		assert_eq!(preview_state.source_buffer_id, replacement_buffer_id);
	}

	#[test]
	fn pinned_preview_scrolling_clamps_to_the_document() {
		let registry = UastRegistry::new(32);
		let mut session = test_session(&registry);
		open_help(&mut session, "l1\nl2\nl3\nl4\nl5\n");

		assert!(scroll_pinned_preview(
			&mut session,
			&EditorCommand::MoveCursor(MoveDirection::Down)
		));
		assert_eq!(focused_state(&session).source_scroll_line, DocLine::new(1));

		assert!(scroll_pinned_preview(
			&mut session,
			&EditorCommand::PageDown
		));
		assert_eq!(
			focused_state(&session).source_scroll_line,
			DocLine::new(4),
			"PageDown clamps to the last line"
		);

		assert!(scroll_pinned_preview(
			&mut session,
			&EditorCommand::Scroll(-100)
		));
		assert_eq!(focused_state(&session).source_scroll_line, DocLine::ZERO);

		assert!(scroll_pinned_preview(
			&mut session,
			&EditorCommand::MoveCursor(MoveDirection::Bottom)
		));
		assert_eq!(focused_state(&session).source_scroll_line, DocLine::new(4));
	}

	#[test]
	fn pinned_scrolling_ignores_normal_panes_and_scroll_targets_skip_pinned() {
		let registry = UastRegistry::new(32);
		let mut session = test_session(&registry);
		assert!(!scroll_pinned_preview(
			&mut session,
			&EditorCommand::PageDown
		));

		open_help(&mut session, "body");
		assert!(
			scroll_target_view(&session).is_none(),
			"pinned previews must not redirect scrolling to a source pane"
		);
	}
}
