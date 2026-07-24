use super::core::{MAX_MINIMAP_TEXT_BYTES, refresh_sum_tree};
use super::document::io::read_document_snapshot;
use super::document::selection::resolve_visual_ranges_sparse;
use super::folding::{pan_scroll_y_to_keep_cursor_visible, visual_line_index_for_doc_line};
use super::language::{LanguageProvider, LanguageRegistry};
use super::layout::{
	ViewportAnchor, collect_visible_rows_with_sum_tree, cursor_view_metrics_with_sum_tree,
	pan_viewport_anchor_to_keep_cursor_visible_with_sum_tree,
	viewport_geometry_for_viewport_with_numbering,
};
use super::minimap::{
	MinimapContentRevision, MinimapOverlay, build_byte_fallback_minimap_snapshot,
	build_search_minimap_bands, build_text_minimap_snapshot,
};
use super::semantic::{
	SemanticPipelineParams, dispatch_semantic_responses_in_layout, drive_semantic_pipeline,
};
use super::session::{
	BufferContent, BufferStore, CachedSumTree, CursorState, DiffPair, EditorPane, EditorSession,
	PaneLayout, SemanticState, TextViewState, ViewContent,
};
use super::support::line_start_byte_sparse;
use crate::core::{
	BufferId, CursorPosition, DOCTAB_BAR_ROWS, DisplayRow, DocByte, DocLine, DocLineCount, PaneCol,
	PaneId, PaneLocalCol, PaneLocalRow, PaneRect, PaneRow, Rgb, ScreenCol, ScreenRow,
};
use crate::ecs::{NodeId, UastRegistry};
use crate::engine::SplitDirection;
use crate::svp::SvpResolver;
use crate::svp::rust_highlight::{HighlightSpan, HighlightSpanIndex, TokenCategory};
use crate::svp::semantic::SemanticReactor;
use crate::uast::projection::{
	GutterLineNumber, HostPanelViewport, PaneDividerViewport, ProjectedCaret,
};
use crate::uast::{PaneViewport, RenderToken, UastProjection, Viewport};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::path::Path;
use std::sync::{Arc, mpsc};

pub(crate) struct RenderParams<'a> {
	pub(crate) registry: &'a Arc<UastRegistry>,
	pub(crate) resolver: &'a Arc<SvpResolver>,
	pub(crate) tx_view: &'a mpsc::Sender<Viewport>,
	pub(crate) reactor: &'a SemanticReactor,
	pub(crate) lang_registry: &'a Arc<LanguageRegistry>,
	pub(crate) session: &'a mut EditorSession,
}

struct RenderLedgerState {
	current_state_id: crate::core::StateId,
	dirty: bool,
}

impl RenderLedgerState {
	fn is_dirty(&self) -> bool {
		self.dirty
	}
}

struct RenderDocumentState {
	buffer_id: BufferId,
	root_id: Option<NodeId>,
	topology_rev: crate::core::TopologyRev,
	file_path: Option<String>,
	virtual_path: Option<String>,
	backing_path: Option<String>,
	active_language_id: Option<super::language::LanguageId>,
	ledger: RenderLedgerState,
}

struct RenderTab<'a> {
	document: RenderDocumentState,
	cursor: &'a mut CursorState,
	view: &'a mut TextViewState,
	semantic: Option<&'a mut SemanticState>,
	browser_sum_tree: Option<&'a mut Option<CachedSumTree>>,
	search_matches: &'a [super::protocol::SearchMatch],
	match_index: Option<usize>,
	match_info: Option<String>,
}

impl RenderTab<'_> {
	fn gutter_numbering(&self) -> Option<super::layout::GutterNumbering> {
		self.view.gutter_numbering()
	}
}

fn viewport_is_busy(io_busy: bool, semantic_busy: bool) -> bool {
	io_busy || semantic_busy
}

fn project_gutter_line_numbers(
	registry: &UastRegistry,
	root: Option<NodeId>,
	folds: &super::folding::FoldSet,
	visible_lines: &[DocLine],
	viewport_start_line: DocLine,
	cursor_line: DocLine,
	numbering: super::layout::GutterNumbering,
) -> Vec<GutterLineNumber> {
	let mut lines = visible_lines.to_vec();
	lines.push(viewport_start_line);
	lines.sort_unstable();
	lines.dedup();

	// `visible_lines` comes from the fold-aware layout projection. After wrapped
	// duplicates are removed, adjacent entries are adjacent logical visual rows,
	// even when their document-line indices jump across a closed fold. Anchor the
	// sequence once instead of rescanning the document topology for every row.
	let viewport_start_visual = root.map_or(viewport_start_line.get(), |root| {
		visual_line_index_for_doc_line(registry, root, folds, viewport_start_line).get()
	});
	let cursor_visual = lines
		.binary_search(&cursor_line)
		.map(|index| viewport_start_visual.saturating_add(index as u64))
		.unwrap_or_else(|_| {
			root.map_or(cursor_line.get(), |root| {
				visual_line_index_for_doc_line(registry, root, folds, cursor_line).get()
			})
		});
	lines
		.into_iter()
		.enumerate()
		.map(|(index, line)| {
			let line_visual = viewport_start_visual.saturating_add(index as u64);
			let relative = line_visual.abs_diff(cursor_visual);
			let number = match numbering {
				super::layout::GutterNumbering::Absolute => line.get().saturating_add(1),
				super::layout::GutterNumbering::Relative => relative,
				super::layout::GutterNumbering::Hybrid if line == cursor_line => {
					line.get().saturating_add(1)
				}
				super::layout::GutterNumbering::Hybrid => relative,
			};
			GutterLineNumber { line, number }
		})
		.collect()
}

pub(crate) fn render_frame(params: RenderParams<'_>) -> bool {
	let RenderParams {
		registry,
		resolver,
		tx_view,
		reactor,
		lang_registry,
		session,
	} = params;
	for root in registry.drain_metric_inflations() {
		crate::svp::ingest::persist_line_index(registry, root);
	}
	let pane_layouts = session.pane_layouts();
	if pane_layouts
		.iter()
		.all(|pane_layout| pane_layout.pane_id != session.focused_pane_id)
		&& let Some(first_pane_id) = pane_layouts.first().map(|pane_layout| pane_layout.pane_id)
	{
		session.focused_pane_id = first_pane_id;
	}

	let grafted = dispatch_semantic_responses_in_layout(
		registry,
		reactor,
		&mut session.buffers,
		&mut session.layout,
		&mut session.events,
	);
	if grafted && !session.diff_pairs.is_empty() {
		super::diff::recompute_all_diffs(registry, session);
	}
	super::markdown_image_preview::refresh_markdown_image_previews(registry, session);
	let ui_palette = session.persistent_settings.ui_colors.palette();
	let git_colors = [
		session
			.persistent_settings
			.ui_colors
			.resolve(super::ui_colors::UiColorKey::GitStaged)
			.color,
		session
			.persistent_settings
			.ui_colors
			.resolve(super::ui_colors::UiColorKey::GitUnstaged)
			.color,
		session
			.persistent_settings
			.ui_colors
			.resolve(super::ui_colors::UiColorKey::GitUntracked)
			.color,
		session
			.persistent_settings
			.ui_colors
			.resolve(super::ui_colors::UiColorKey::GitRename)
			.color,
	];
	let git_branch = {
		let EditorSession {
			buffers,
			layout,
			focused_pane_id,
			git_branch_cache,
			..
		} = session;
		let document = layout
			.find_pane(*focused_pane_id)
			.and_then(|pane| buffers.get(pane.active_tab().buffer_id));
		match document.map(|document| &document.content) {
			Some(BufferContent::Text(document)) => {
				if let Some(path) = document
					.backing_path
					.as_deref()
					.or(document.file_path.as_deref())
				{
					git_branch_cache.cached_branch_for_path(Path::new(path))
				} else {
					git_branch_cache.cached_branch_for_current_dir()
				}
			}
			Some(BufferContent::FileBrowser(document)) => {
				git_branch_cache.cached_branch_for_path(&document.state.root_path)
			}
			Some(BufferContent::MarkdownPreview(document)) if !document.state.pinned => document
				.state
				.source_path
				.as_deref()
				.and_then(|path| git_branch_cache.cached_branch_for_path(Path::new(path))),
			// A host panel has no path of its own, so it contributes no branch.
			Some(BufferContent::MarkdownPreview(_) | BufferContent::HostPanel(_)) | None => None,
		}
	};

	let EditorSession {
		input,
		viewport_id,
		hit_test_fingerprint,
		buffers,
		layout,
		focused_pane_id,
		search_pattern,
		search_case_insensitive,
		search_pattern_revision,
		search_confirm,
		pending_reload,
		ui,
		next_semantic_request_id,
		output_panels,
		git_status_branch,
		..
	} = session;
	let git_status_buffer_id = output_panels.get("git-status").copied();
	let git_status_branch = git_status_branch.as_deref();

	let confirm_prompt = if pending_reload.is_some() {
		Some("Reload and discard changes? [y/n]".to_string())
	} else {
		search_confirm
			.as_ref()
			.map(|_| "Replace? [y/n/a/q]".to_string())
	};
	let status_message = ui.status_message.take();
	let document_text = ui.document_text_response.take();
	let document_text_buffer_id = ui.document_text_response_buffer_id.take();
	debug_assert_eq!(
		document_text.is_some(),
		document_text_buffer_id.is_some(),
		"document text replies must carry their source buffer"
	);
	let io_busy = ui.busy;
	let should_quit = ui.pending_quit;
	let yank_flash = ui.yank_flash;

	let mut panes = Vec::with_capacity(pane_layouts.len());
	let mut semantic_busy = false;
	registry.retain_viewport_pin_owners(
		&pane_layouts
			.iter()
			.map(|layout| layout.pane_id.get())
			.collect(),
	);
	for pane_layout in pane_layouts {
		let is_focused = pane_layout.pane_id == *focused_pane_id;
		let Some(pane) = layout.find_pane_mut(pane_layout.pane_id) else {
			continue;
		};
		panes.push(render_pane_viewport(
			registry,
			resolver,
			reactor,
			lang_registry,
			next_semantic_request_id,
			pane_layout,
			pane,
			buffers,
			*search_pattern_revision,
			if is_focused { yank_flash } else { None },
			&session.diff_pairs,
			session
				.output_panels
				.get("git-status")
				.is_some_and(|buffer_id| *buffer_id == pane.active_tab().buffer_id),
			git_status_buffer_id,
			git_status_branch,
			git_colors,
			&mut semantic_busy,
		));
	}
	let next_hit_test_fingerprint = hit_test_fingerprint_for(&panes);
	if *hit_test_fingerprint != Some(next_hit_test_fingerprint) {
		if hit_test_fingerprint.is_some() {
			*viewport_id = viewport_id.next();
		}
		*hit_test_fingerprint = Some(next_hit_test_fingerprint);
	}

	let dividers = dividers_for(&panes);
	let _ = tx_view.send(Viewport {
		viewport_id: *viewport_id,
		panes,
		focused_pane_id: *focused_pane_id,
		git_branch,
		status_message,
		dividers,
		document_text,
		document_text_buffer_id,
		busy: viewport_is_busy(io_busy, semantic_busy),
		should_quit,
		search_pattern: search_pattern.clone(),
		search_case_insensitive: *search_case_insensitive,
		ui_palette,
		git_refresh_seconds: session.git_refresh_seconds,
		confirm_prompt,
		input: input.projection(),
		language_completion_candidates: lang_registry.completion_candidates(),
	});

	// Eviction frees buffers that engine-thread readers borrow into, so it runs
	// here rather than on the resolver's completion path. Visible nodes were
	// pinned above and survive.
	registry.enforce_resident_budget(None);

	should_quit
}

/// Widen a divider's grab area beyond its painted size. A 1-row horizontal
/// divider is a poor pointer target, so the engine publishes a usable hit
/// rectangle rather than leaving every frontend to invent one.
const DIVIDER_HIT_PADDING: u32 = 1;

/// Derive draggable boundaries from the projected pane rectangles. Adjacency
/// is decided here, once, so no frontend has to know how wide a split gap is.
fn dividers_for(panes: &[PaneViewport]) -> Vec<PaneDividerViewport> {
	let mut dividers = Vec::new();
	for before in panes {
		for after in panes {
			if before.pane_id == after.pane_id {
				continue;
			}
			// Vertical divider: `after` begins one gap to the right of
			// `before`, and the two share vertical extent.
			let shares_rows = before.rect.screen_row.get() < after.rect.bottom().get()
				&& after.rect.screen_row.get() < before.rect.bottom().get();
			if shares_rows
				&& after.rect.screen_col.get()
					== before.rect.right().get() + crate::core::SPLIT_DIVIDER_COLS
			{
				let top = before
					.rect
					.screen_row
					.get()
					.max(after.rect.screen_row.get());
				let bottom = before.rect.bottom().get().min(after.rect.bottom().get());
				let visual = PaneRect::new(
					ScreenCol::new(before.rect.right().get()),
					ScreenRow::new(top),
					PaneCol::new(crate::core::SPLIT_DIVIDER_COLS),
					PaneRow::new(bottom.saturating_sub(top)),
				);
				dividers.push(PaneDividerViewport {
					before: before.pane_id,
					after: after.pane_id,
					axis: SplitDirection::Vertical,
					visual_rect: visual,
					hit_rect: visual,
				});
			}

			let shares_columns = before.rect.screen_col.get() < after.rect.right().get()
				&& after.rect.screen_col.get() < before.rect.right().get();
			if shares_columns
				&& after.rect.screen_row.get()
					== before.rect.bottom().get() + crate::core::SPLIT_DIVIDER_ROWS
			{
				let left = before
					.rect
					.screen_col
					.get()
					.max(after.rect.screen_col.get());
				let right = before.rect.right().get().min(after.rect.right().get());
				let visual = PaneRect::new(
					ScreenCol::new(left),
					ScreenRow::new(before.rect.bottom().get()),
					PaneCol::new(right.saturating_sub(left)),
					PaneRow::new(crate::core::SPLIT_DIVIDER_ROWS),
				);
				// Horizontal gaps are a single row. Grow the grab area into the
				// pane above, but never into the lower pane's header: that header is
				// the pane-reordering handle and must own its entire hit area.
				let hit = PaneRect::new(
					ScreenCol::new(left),
					ScreenRow::new(
						before
							.rect
							.bottom()
							.get()
							.saturating_sub(DIVIDER_HIT_PADDING),
					),
					PaneCol::new(right.saturating_sub(left)),
					PaneRow::new(crate::core::SPLIT_DIVIDER_ROWS + DIVIDER_HIT_PADDING),
				);
				dividers.push(PaneDividerViewport {
					before: before.pane_id,
					after: after.pane_id,
					axis: SplitDirection::Horizontal,
					visual_rect: visual,
					hit_rect: hit,
				});
			}
		}
	}
	dividers
}

fn hit_test_fingerprint_for(panes: &[PaneViewport]) -> u64 {
	let mut hasher = std::collections::hash_map::DefaultHasher::new();
	for pane in panes {
		pane.pane_id.hash(&mut hasher);
		pane.view_id.hash(&mut hasher);
		pane.rect.screen_col.hash(&mut hasher);
		pane.rect.screen_row.hash(&mut hasher);
		pane.rect.width.hash(&mut hasher);
		pane.rect.height.hash(&mut hasher);
		pane.viewport_start_line.hash(&mut hasher);
		pane.viewport_row_offset.hash(&mut hasher);
		pane.viewport_line_count.hash(&mut hasher);
		pane.max_doc_line.hash(&mut hasher);
		pane.wrap_enabled.hash(&mut hasher);
		pane.gutter_numbering
			.map(|numbering| std::mem::discriminant(&numbering))
			.hash(&mut hasher);
		match pane.minimap.as_ref() {
			None => 0_u8.hash(&mut hasher),
			Some(crate::engine::MinimapSnapshot::Overview(_)) => 1_u8.hash(&mut hasher),
			Some(crate::engine::MinimapSnapshot::ByteFallback(_)) => 2_u8.hash(&mut hasher),
			Some(crate::engine::MinimapSnapshot::Preview(preview)) => {
				3_u8.hash(&mut hasher);
				preview.rows.len().hash(&mut hasher);
			}
		}
		pane.is_file_browser.hash(&mut hasher);
		pane.is_output_panel.hash(&mut hasher);
		pane.markdown_preview.is_some().hash(&mut hasher);
		for row in &pane.visible_rows {
			row.line.hash(&mut hasher);
			row.start_col.hash(&mut hasher);
		}
		for token in &pane.tokens {
			token.absolute_start_line.hash(&mut hasher);
			token.absolute_start_byte.hash(&mut hasher);
			token.text.hash(&mut hasher);
		}
	}
	hasher.finish()
}

fn render_pane_viewport(
	registry: &Arc<UastRegistry>,
	resolver: &Arc<SvpResolver>,
	reactor: &SemanticReactor,
	lang_registry: &Arc<LanguageRegistry>,
	next_semantic_request_id: &mut crate::core::RequestId,
	pane_layout: PaneLayout,
	pane: &mut EditorPane,
	buffers: &mut BufferStore,
	search_pattern_revision: u64,
	yank_flash: Option<(DocByte, DocByte)>,
	diff_pairs: &HashMap<PaneId, DiffPair>,
	is_git_status_panel: bool,
	git_status_buffer_id: Option<BufferId>,
	_git_status_branch: Option<&str>,
	git_colors: [Rgb; 4],
	semantic_busy: &mut bool,
) -> PaneViewport {
	let pane_width = pane_layout.rect.width.get().min(u16::MAX as u32) as u16;
	let pane_text_lines = pane_layout
		.rect
		.height
		.get()
		.saturating_sub(DOCTAB_BAR_ROWS as u32);
	let active_doctab_index = pane.active_tab_index;
	let active_buffer_id = pane.active_tab().buffer_id;
	let active_view_id = pane.active_tab().id;
	// Read before the mutable render borrow below: a host panel projects only
	// presentation identity, so nothing here depends on render state.
	let host_panel = buffers
		.get(pane.active_tab().buffer_id)
		.and_then(|buffer| buffer.host_panel())
		.map(|panel| HostPanelViewport {
			panel_id: panel.panel_id,
			kind: panel.descriptor.kind.clone(),
			title: panel.descriptor.title.clone(),
			revision: panel.revision,
		});

	let (
		tokens,
		scroll_y,
		viewport_start_line,
		viewport_row_offset,
		viewport_line_count,
		wrap_enabled,
		visible_whitespace,
		gutter_numbering,
		gutter_line_numbers,
		cursor_lines,
		cursor_pane_row,
		cursor_abs_pos,
		cursor_pane_col,
		cursor_abs_byte,
		cursor_line_start_byte,
		secondary_carets,
		cursor_count,
		max_doc_line_index,
		file_name,
		file_size,
		line_count,
		is_dirty,
		search_match_info,
		global_start_byte,
		highlights,
		diagnostics,
		inline_annotations,
		selection_ranges,
		minimap,
		markdown_cells,
		markdown_preview,
		theme_colors,
		render_rows,
		visible_rows,
		visible_line_starts,
		dma_pending,
		language_id,
		language_label,
		is_file_browser,
		is_output_panel,
		background_override,
		semantic_pending,
	) = 'render_active_view: {
		let view = pane.active_tab_mut();
		view.common_mut().viewport_width = pane_width;
		view.common_mut().viewport_lines = pane_text_lines;
		let buffer_id = view.buffer_id;
		let buffer = buffers
			.get_mut(buffer_id)
			.expect("every projected view must reference a live buffer");
		if let (
			BufferContent::MarkdownPreview(document),
			ViewContent::MarkdownPreview(preview_view),
		) = (&buffer.content, &mut view.content)
		{
			let state = &document.state;
			let line_count = state.source_text.lines().count().max(1) as u64;
			break 'render_active_view (
				Vec::new(),
				preview_view.state.scroll_y,
				state.source_scroll_line,
				preview_view.state.scroll_row_offset,
				preview_view.state.viewport_lines,
				preview_view.state.wrap_enabled,
				false,
				None,
				Vec::new(),
				Vec::new(),
				PaneLocalRow::ZERO,
				CursorPosition::new(state.source_scroll_line, crate::core::DocDisplayCol::ZERO),
				PaneLocalCol::ZERO,
				DocByte::ZERO,
				DocByte::ZERO,
				Vec::new(),
				0,
				line_count.saturating_sub(1),
				Some(document.title.clone()),
				state.source_text.len() as u64,
				Some(line_count),
				false,
				None,
				DocByte::ZERO,
				Vec::new(),
				Vec::new(),
				Vec::new(),
				Vec::new(),
				None,
				Vec::new(),
				Some(super::markdown_image_preview::markdown_image_preview_snapshot(document)),
				preview_view.state.current_theme.syntax_colors,
				Vec::new(),
				Vec::new(),
				Vec::new(),
				false,
				lang_registry.resolve_language_id("markdown"),
				"Markdown Preview",
				false,
				false,
				preview_view
					.state
					.background_hex
					.as_deref()
					.and_then(Rgb::from_hex),
				false,
			);
		}
		if let (BufferContent::HostPanel(document), ViewContent::HostPanel(panel_view)) =
			(&buffer.content, &mut view.content)
		{
			// A host panel projects geometry and identity only: no tokens, no
			// caret, nothing text-shaped. The frontend paints the rect.
			break 'render_active_view (
				Vec::new(),
				panel_view.state.scroll_y,
				DocLine::ZERO,
				panel_view.state.scroll_row_offset,
				panel_view.state.viewport_lines,
				false,
				false,
				None,
				Vec::new(),
				Vec::new(),
				PaneLocalRow::ZERO,
				CursorPosition::new(DocLine::ZERO, crate::core::DocDisplayCol::ZERO),
				PaneLocalCol::ZERO,
				DocByte::ZERO,
				DocByte::ZERO,
				Vec::new(),
				0,
				0,
				Some(document.descriptor.title.clone()),
				0,
				Some(1),
				false,
				None,
				DocByte::ZERO,
				Vec::new(),
				Vec::new(),
				Vec::new(),
				Vec::new(),
				None,
				Vec::new(),
				None,
				panel_view.state.current_theme.syntax_colors,
				Vec::new(),
				Vec::new(),
				Vec::new(),
				false,
				None,
				"Host Panel",
				false,
				false,
				panel_view
					.state
					.background_hex
					.as_deref()
					.and_then(Rgb::from_hex),
				false,
			);
		}
		let (
			document,
			semantic,
			cursor,
			text_view,
			browser_sum_tree,
			search_matches,
			match_index,
			match_info,
			is_file_browser,
			is_output_panel,
		) = match (&mut buffer.content, &mut view.content) {
			(BufferContent::Text(document), ViewContent::Text(text_view)) => {
				let search_matches_current =
					document.search_matches_are_current(search_pattern_revision);
				let search_matches = if search_matches_current {
					document.search_matches.as_slice()
				} else {
					&[]
				};
				(
					RenderDocumentState {
						buffer_id,
						root_id: Some(document.root_id),
						topology_rev: document.topology_rev,
						file_path: document.file_path.clone(),
						virtual_path: document.virtual_path.clone(),
						backing_path: document.backing_path.clone(),
						active_language_id: document.active_language_id,
						ledger: RenderLedgerState {
							current_state_id: document.ledger.current_state_id,
							dirty: document.ledger.is_dirty(),
						},
					},
					Some(document.semantic.as_mut()),
					&mut text_view.cursor,
					&mut text_view.state,
					None,
					search_matches,
					search_matches_current
						.then_some(text_view.match_index)
						.flatten(),
					search_matches_current
						.then(|| text_view.match_info.clone())
						.flatten(),
					false,
					buffer.role == crate::engine::session::BufferRole::OutputPanel,
				)
			}
			(BufferContent::FileBrowser(document), ViewContent::FileBrowser(browser_view)) => (
				RenderDocumentState {
					buffer_id,
					root_id: Some(document.root_id),
					topology_rev: document.topology_rev,
					file_path: Some(document.file_path.clone()),
					virtual_path: None,
					backing_path: None,
					active_language_id: lang_registry.resolve_language_id("fs"),
					ledger: RenderLedgerState {
						current_state_id: crate::core::StateId::ZERO,
						dirty: false,
					},
				},
				None,
				&mut browser_view.cursor,
				&mut browser_view.state,
				Some(&mut document.cached_sum_tree),
				&[] as &[super::protocol::SearchMatch],
				None,
				None,
				true,
				false,
			),
			_ => panic!("view content must match its buffer content"),
		};
		let mut tab = RenderTab {
			document,
			cursor,
			view: text_view,
			semantic,
			browser_sum_tree,
			search_matches,
			match_index,
			match_info,
		};
		let gutter_numbering = if is_file_browser || is_git_status_panel {
			None
		} else {
			tab.gutter_numbering()
		};
		// File trees and Git status are navigation chrome, not editable source
		// documents. Keep their full width available for paths regardless of
		// the session's normal editor minimap setting.
		let minimap_enabled = tab.view.minimap_enabled && !is_file_browser && !is_git_status_panel;
		let visible_whitespace = tab.view.visible_whitespace && !is_file_browser;

		let (
			virtual_tokens,
			tokens,
			total_lines,
			viewport_anchor,
			render_rows,
			visible_rows,
			cursor_pane_row,
			cursor_pane_col,
			secondary_carets,
		) = if let Some(rid) = tab.document.root_id {
			let total_lines = registry.get_total_newlines(rid);
			let mut projection_misses = Vec::new();
			let mut projection_pending = false;
			let topology_rev = tab.document.topology_rev;
			let state_id = tab.document.ledger.current_state_id;
			let folds = &mut tab.view.folds;
			tab.cursor.visit_carets_mut(|caret| {
				if !caret.resolve(registry, rid, folds, topology_rev, state_id) {
					projection_pending = true;
					if let Some(node) = caret
						.pending_target()
						.and_then(|target| target.required_node)
						.or_else(|| caret.node_cache().map(|cache| cache.node))
					{
						projection_misses.push(node);
					}
				}
			});
			if !projection_pending && let Some(cursors) = tab.cursor.points_mut() {
				cursors.normalize(registry, rid);
			}
			let max_doc_line = DocLine::new(total_lines);
			let current_state_id = tab.document.ledger.current_state_id;
			let cached_sum_tree = if let Some(semantic) = tab.semantic.as_deref_mut() {
				&mut semantic.cached_sum_tree
			} else {
				tab.browser_sum_tree
					.as_deref_mut()
					.expect("non-text render must have a content cache")
			};
			let sum_tree = refresh_sum_tree(cached_sum_tree, registry, rid, current_state_id);
			let mut geometry = viewport_geometry_for_viewport_with_numbering(
				DocLine::new(tab.view.scroll_y),
				tab.view.viewport_lines,
				max_doc_line,
				tab.view.viewport_width,
				minimap_enabled,
				gutter_numbering,
			);
			let viewport_anchor = if tab.view.wrap_enabled {
				let mut anchor = ViewportAnchor {
					line: DocLine::new(tab.view.scroll_y),
					row_offset: tab.view.scroll_row_offset,
				};
				for _ in 0..3 {
					anchor = pan_viewport_anchor_to_keep_cursor_visible_with_sum_tree(
						registry,
						rid,
						&tab.view.folds,
						anchor,
						tab.cursor.abs_line(),
						tab.cursor.abs_col(),
						max_doc_line,
						tab.view.viewport_lines,
						true,
						geometry.text_width,
						sum_tree,
					);
					let adjusted_geometry = viewport_geometry_for_viewport_with_numbering(
						anchor.line,
						tab.view.viewport_lines,
						max_doc_line,
						tab.view.viewport_width,
						minimap_enabled,
						gutter_numbering,
					);
					if adjusted_geometry.gutter_width == geometry.gutter_width {
						break;
					}
					geometry = adjusted_geometry;
				}
				anchor
			} else {
				let anchor = ViewportAnchor {
					line: DocLine::new(pan_scroll_y_to_keep_cursor_visible(
						registry,
						rid,
						&tab.view.folds,
						tab.view.scroll_y,
						tab.cursor.abs_line(),
						max_doc_line,
						tab.view.viewport_lines,
					)),
					row_offset: DisplayRow::ZERO,
				};
				geometry = viewport_geometry_for_viewport_with_numbering(
					anchor.line,
					tab.view.viewport_lines,
					max_doc_line,
					tab.view.viewport_width,
					minimap_enabled,
					gutter_numbering,
				);
				anchor
			};
			tab.view.scroll_row_offset = viewport_anchor.row_offset;
			let scroll_line = viewport_anchor.line;

			let virtual_start_line = scroll_line.saturating_sub(60);
			let virtual_line_count = tab.view.viewport_lines + 120;
			let virtual_tokens = registry.query_viewport(
				rid,
				&tab.view.folds,
				virtual_start_line,
				virtual_line_count,
			);
			let visible_tokens =
				registry.query_viewport(rid, &tab.view.folds, scroll_line, tab.view.viewport_lines);

			let render_rows = collect_visible_rows_with_sum_tree(
				registry,
				rid,
				&tab.view.folds,
				ViewportAnchor {
					line: viewport_anchor.line,
					row_offset: DisplayRow::ZERO,
				},
				max_doc_line,
				tab.view
					.viewport_lines
					.saturating_add(viewport_anchor.row_offset.get()),
				tab.view.wrap_enabled,
				geometry.text_width,
				sum_tree,
			);
			let visible_rows: Vec<_> = render_rows
				.iter()
				.skip(viewport_anchor.row_offset.get() as usize)
				.take(tab.view.viewport_lines as usize)
				.copied()
				.collect();
			let (cursor_pane_row, cursor_pane_col) = cursor_view_metrics_with_sum_tree(
				registry,
				rid,
				&tab.view.folds,
				viewport_anchor,
				tab.cursor.abs_line(),
				tab.cursor.abs_col(),
				max_doc_line,
				tab.view.wrap_enabled,
				geometry.text_width,
				sum_tree,
			);
			let secondary_carets = tab
				.cursor
				.points()
				.into_iter()
				.flat_map(|cursors| cursors.secondary())
				.filter_map(|caret| {
					let (pane_row, pane_col) = cursor_view_metrics_with_sum_tree(
						registry,
						rid,
						&tab.view.folds,
						viewport_anchor,
						caret.abs_line(),
						caret.abs_col(),
						max_doc_line,
						tab.view.wrap_enabled,
						geometry.text_width,
						sum_tree,
					);
					visible_rows
						.get(pane_row.get() as usize)
						.is_some_and(|row| {
							row.line == caret.abs_line() && caret.abs_col() >= row.start_col
						})
						.then(|| ProjectedCaret {
							pane_row,
							pane_col,
							abs_pos: CursorPosition::new(caret.abs_line(), caret.abs_col()),
							abs_byte: caret.byte(),
						})
				})
				.collect();

			for token in &visible_tokens {
				if !token.is_virtual && token.text.is_empty() {
					let _ = if is_file_browser {
						resolver.hydrate_whole_leaf(token.node_id)
					} else {
						resolver.hydrate_whole_leaf_for_buffer(token.node_id, buffer_id)
					};
				}
			}
			for node in projection_misses {
				let _ = if is_file_browser {
					resolver.hydrate_whole_leaf(node)
				} else {
					resolver.hydrate_whole_leaf_for_buffer(node, buffer_id)
				};
			}
			registry.set_viewport_resident_pins(
				pane_layout.pane_id.get(),
				visible_tokens.iter().map(|token| token.node_id),
			);

			(
				virtual_tokens,
				visible_tokens,
				total_lines,
				viewport_anchor,
				render_rows,
				visible_rows,
				cursor_pane_row,
				cursor_pane_col,
				secondary_carets,
			)
		} else {
			tab.view.scroll_row_offset = DisplayRow::ZERO;
			(
				Vec::new(),
				Vec::new(),
				0,
				ViewportAnchor {
					line: DocLine::ZERO,
					row_offset: DisplayRow::ZERO,
				},
				Vec::new(),
				Vec::new(),
				PaneLocalRow::ZERO,
				PaneLocalCol::ZERO,
				Vec::new(),
			)
		};

		let mut global_start_byte = DocByte::ZERO;
		let mut highlights = Vec::new();
		if tab.document.file_path.is_some() || tab.document.virtual_path.is_some() {
			let active_provider = tab
				.document
				.active_language_id
				.and_then(|id| lang_registry.get_provider(id));
			if let Some(provider) = active_provider
				&& !virtual_tokens.is_empty()
			{
				highlights = lexical_highlights_for_view(provider, &virtual_tokens, &tokens);
			}
		}

		if !is_output_panel && let Some(semantic) = tab.semantic.as_deref_mut() {
			drive_semantic_pipeline(SemanticPipelineParams {
				registry,
				reactor,
				lang_registry,
				next_request_id: next_semantic_request_id,
				buffer_id: tab.document.buffer_id,
				root_id: tab.document.root_id.expect("text buffer has a root"),
				// In-memory documents carry a virtual identity so language
				// services still receive a file-path-shaped context.
				file_path: tab
					.document
					.file_path
					.as_deref()
					.or(tab.document.virtual_path.as_deref()),
				backing_path: tab.document.backing_path.as_deref(),
				active_language_id: tab.document.active_language_id,
				state_id: tab.document.ledger.current_state_id,
				semantic,
				has_virtual_tokens: !virtual_tokens.is_empty(),
			});
		}
		let semantic_pending = tab
			.semantic
			.as_deref()
			.is_some_and(|semantic| semantic.pending_semantic_request_id.is_some());

		if let Some(semantic) = tab.semantic.as_deref()
			&& !semantic.highlights.is_empty()
		{
			highlights = merge_highlights(&highlights, &semantic.highlights);
		}

		if let Some(first_visible) = tokens.first() {
			global_start_byte = first_visible.absolute_start_byte;
		}

		let (cursor_abs_byte, cursor_line_start_byte, file_size, line_count) =
			if let Some(rid) = tab.document.root_id {
				let cursor_abs_byte = tab.cursor.byte();
				let cursor_line_start_byte =
					line_start_byte_sparse(registry, rid, tab.cursor.abs_line())
						.unwrap_or(cursor_abs_byte);
				(
					cursor_abs_byte,
					cursor_line_start_byte,
					registry.get_total_bytes(rid),
					registry.exact_line_count(rid),
				)
			} else {
				(DocByte::ZERO, DocByte::ZERO, 0, Some(1))
			};

		let selection_ranges = match (tab.cursor.active_visual(), tab.document.root_id) {
			(Some((anchor, kind)), Some(rid)) => resolve_visual_ranges_sparse(
				registry,
				rid,
				&tab.view.folds,
				anchor,
				cursor_abs_byte,
				kind,
			)
			.unwrap_or_default(),
			_ => Vec::new(),
		};
		let viewport_start_line = viewport_anchor.line;
		tab.view.scroll_y = viewport_start_line.get();
		let mut visible_line_starts = Vec::new();
		for row in &visible_rows {
			if visible_line_starts.last().copied() != Some(row.line) {
				visible_line_starts.push(row.line);
			}
		}
		let gutter_line_numbers = gutter_numbering.map_or_else(Vec::new, |numbering| {
			project_gutter_line_numbers(
				registry,
				tab.document.root_id,
				&tab.view.folds,
				&visible_line_starts,
				viewport_start_line,
				tab.cursor.abs_line(),
				numbering,
			)
		});
		let viewport_end_line = visible_rows
			.last()
			.map(|row| row.line)
			.unwrap_or(viewport_start_line)
			.boundary_after();
		let minimap = if minimap_enabled {
			if let Some(rid) = tab.document.root_id {
				let overlay = MinimapOverlay::new(
					viewport_start_line,
					tab.view.viewport_lines,
					tab.cursor.abs_line(),
				);
				let path_changed =
					tab.view.last_minimap_path.as_deref() != tab.document.file_path.as_deref();
				let state_changed =
					tab.view.last_minimap_state != Some(tab.document.ledger.current_state_id);
				let semantic_revision = tab
					.semantic
					.as_deref()
					.map_or(0, |semantic| semantic.view_revision);
				let semantic_changed = tab.view.last_minimap_semantic_revision != semantic_revision;
				let total_lines_changed = tab.view.last_minimap_total_lines != total_lines;
				let content_revision = MinimapContentRevision::new(
					tab.document.buffer_id,
					tab.document.ledger.current_state_id,
					semantic_revision,
				);
				if tab.view.cached_minimap.is_none()
					|| path_changed || state_changed
					|| semantic_changed
					|| total_lines_changed
				{
					tab.view.cached_minimap =
						Some(if file_size > 0 && file_size <= MAX_MINIMAP_TEXT_BYTES {
							match read_document_snapshot(
								registry,
								rid,
								tab.document.backing_path.as_deref().map(Path::new),
							) {
								Ok(bytes) => build_text_minimap_snapshot(
									&bytes,
									tab.semantic
										.as_deref()
										.map_or(&[], |semantic| semantic.highlights.as_slice()),
									overlay,
									tab.document
										.active_language_id
										.and_then(|id| lang_registry.get_provider(id)),
									content_revision,
								),
								Err(_) => build_byte_fallback_minimap_snapshot(
									registry,
									rid,
									overlay,
									content_revision,
								),
							}
						} else {
							build_byte_fallback_minimap_snapshot(
								registry,
								rid,
								overlay,
								content_revision,
							)
						});
					tab.view.last_minimap_state = Some(tab.document.ledger.current_state_id);
					tab.view.last_minimap_semantic_revision = semantic_revision;
					tab.view.last_minimap_total_lines = total_lines;
					tab.view.last_minimap_path = tab.document.file_path.clone();
				}

				tab.view.cached_minimap.as_ref().map(|snapshot| {
					let mut snapshot = snapshot.clone();
					let (search_bands, active_search_band) = build_search_minimap_bands(
						tab.search_matches,
						tab.match_index,
						DocLineCount::new(total_lines.saturating_add(1)),
					);
					let overlay = snapshot.overlay_mut();
					overlay.viewport_start_line = viewport_start_line;
					overlay.viewport_end_line = viewport_end_line;
					overlay.viewport_line_count = tab.view.viewport_lines;
					overlay.cursor_line = tab.cursor.abs_line();
					overlay.search_bands = search_bands;
					overlay.active_search_band = active_search_band;
					snapshot
				})
			} else {
				tab.view.common.reset_minimap_cache();
				None
			}
		} else {
			tab.view.common.reset_minimap_cache();
			None
		};
		let markdown_preview = None;
		// Fenced execution cells belong to the Markdown source, not to an
		// optional preview pane. Keep them projected when the preview is closed
		// so notebook commands never fall back to dispatching raw Markdown.
		let markdown_cells = tab
			.document
			.active_language_id
			.and_then(|id| lang_registry.get_provider(id))
			.filter(|provider| provider.label() == "Markdown")
			.and(tab.document.root_id)
			.and_then(|root_id| {
				read_document_snapshot(
					registry,
					root_id,
					tab.document.backing_path.as_deref().map(Path::new),
				)
				.ok()
			})
			.map(|bytes| {
				crate::engine::markdown_image_preview::markdown_cells(
					String::from_utf8_lossy(&bytes).as_ref(),
				)
			})
			.unwrap_or_default();
		let dma_pending = tokens
			.iter()
			.any(|token| token.text.is_empty() && !token.is_folded);

		let mut cursor_lines: Vec<_> = tab
			.cursor
			.points()
			.into_iter()
			.flat_map(|cursors| cursors.all())
			.map(|caret| caret.abs_line())
			.collect();
		if cursor_lines.is_empty() {
			cursor_lines.push(tab.cursor.abs_line());
		}
		cursor_lines.sort_unstable();
		cursor_lines.dedup();
		let cursor_count = tab.cursor.len();

		(
			tokens,
			tab.view.scroll_y,
			viewport_start_line,
			viewport_anchor.row_offset,
			tab.view.viewport_lines,
			tab.view.wrap_enabled,
			visible_whitespace,
			gutter_numbering,
			gutter_line_numbers,
			cursor_lines,
			cursor_pane_row,
			CursorPosition::new(tab.cursor.abs_line(), tab.cursor.abs_col()),
			cursor_pane_col,
			cursor_abs_byte,
			cursor_line_start_byte,
			secondary_carets,
			cursor_count,
			total_lines,
			tab.document.file_path.clone(),
			file_size,
			line_count,
			tab.document.ledger.is_dirty(),
			tab.match_info.clone(),
			global_start_byte,
			highlights,
			tab.semantic
				.as_deref()
				.map_or_else(Vec::new, |semantic| semantic.diagnostics.clone()),
			tab.semantic
				.as_deref()
				.map_or_else(Vec::new, |semantic| semantic.inline_annotations.clone()),
			selection_ranges,
			minimap,
			markdown_cells,
			markdown_preview,
			{
				let mut theme_colors = tab.view.current_theme.syntax_colors;
				if is_git_status_panel {
					theme_colors[TokenCategory::GitStaged as usize] = Some(git_colors[0]);
					theme_colors[TokenCategory::GitUnstaged as usize] = Some(git_colors[1]);
					theme_colors[TokenCategory::GitUntracked as usize] = Some(git_colors[2]);
					theme_colors[TokenCategory::GitRename as usize] = Some(git_colors[3]);
				}
				theme_colors
			},
			render_rows,
			visible_rows,
			visible_line_starts,
			dma_pending,
			tab.document.active_language_id,
			tab.document
				.active_language_id
				.and_then(|id| lang_registry.get_provider(id))
				.map(|provider| provider.label())
				.unwrap_or("Plaintext"),
			is_file_browser,
			is_output_panel,
			tab.view.background_hex.as_deref().and_then(Rgb::from_hex),
			semantic_pending,
		)
	};
	*semantic_busy |= semantic_pending;

	let diff_pair =
		diff_pairs
			.get(&pane_layout.pane_id)
			.map(|pair| crate::uast::projection::DiffPair {
				partner_pane_id: pair.partner_pane_id,
				blocks: pair
					.blocks
					.iter()
					.map(|block| crate::uast::projection::DiffBlock {
						range: block.range,
						kind: match block.kind {
							crate::engine::session::DiffKind::Added => {
								crate::uast::projection::DiffKind::Added
							}
							crate::engine::session::DiffKind::Removed => {
								crate::uast::projection::DiffKind::Removed
							}
							crate::engine::session::DiffKind::Modified => {
								crate::uast::projection::DiffKind::Modified
							}
							crate::engine::session::DiffKind::Moved => {
								crate::uast::projection::DiffKind::Moved
							}
						},
						partner_range: block.partner_range,
						intra_line_diffs: block.intra_line_diffs.as_ref().map(|diffs| {
							diffs
								.iter()
								.map(|diff| crate::uast::projection::IntraLineDiff {
									line: diff.line,
									col_start: diff.col_start,
									col_end: diff.col_end,
									partner_line: diff.partner_line,
									partner_col_start: diff.partner_col_start,
									partner_col_end: diff.partner_col_end,
									kind: match diff.kind {
										crate::engine::session::IntraLineDiffKind::Added => {
											crate::uast::projection::IntraLineDiffKind::Added
										}
										crate::engine::session::IntraLineDiffKind::Removed => {
											crate::uast::projection::IntraLineDiffKind::Removed
										}
										crate::engine::session::IntraLineDiffKind::Modified => {
											crate::uast::projection::IntraLineDiffKind::Modified
										}
									},
								})
								.collect()
						}),
					})
					.collect(),
			});

	PaneViewport {
		pane_id: pane_layout.pane_id,
		buffer_id: active_buffer_id,
		view_id: active_view_id,
		host_panel,
		rect: pane_layout.rect,
		tokens,
		scroll_y,
		viewport_start_line,
		viewport_row_offset,
		viewport_line_count,
		wrap_enabled,
		visible_whitespace,
		gutter_numbering,
		gutter_line_numbers,
		cursor_lines,
		cursor_pane_row,
		cursor_abs_pos,
		cursor_pane_col,
		cursor_abs_byte,
		cursor_line_start_byte,
		secondary_carets,
		cursor_count,
		max_doc_line: DocLine::new(max_doc_line_index),
		file_name,
		file_size,
		line_count,
		is_dirty,
		search_match_info,
		global_start_byte,
		highlights,
		diagnostics,
		inline_annotations,
		selection_ranges,
		yank_flash,
		minimap,
		markdown_cells,
		markdown_preview,
		theme_colors,
		render_rows,
		visible_rows,
		visible_line_starts,
		dma_pending,
		language_id,
		language_label,
		is_file_browser,
		is_output_panel,
		is_git_status_panel,
		background_override,
		tabs: pane
			.tabs
			.iter()
			.map(|view| {
				let buffer = buffers
					.get(view.buffer_id)
					.expect("tab summary view must reference a live buffer");
				crate::uast::projection::DocTabSummary {
					buffer_id: view.buffer_id,
					view_id: view.id,
					title: if git_status_buffer_id == Some(view.buffer_id) {
						"git".to_string()
					} else {
						buffer
							.file_path()
							.map(|path| {
								Path::new(path)
									.file_name()
									.and_then(|name| name.to_str())
									.unwrap_or(path)
									.to_string()
							})
							.unwrap_or_else(|| "[No Name]".to_string())
					},
					is_dirty: buffer.is_dirty(),
				}
			})
			.collect(),
		active_doctab_index,
		diff_pair,
	}
}

fn lexical_highlights_for_view(
	provider: &dyn LanguageProvider,
	virtual_tokens: &[RenderToken],
	visible_tokens: &[RenderToken],
) -> Vec<HighlightSpan> {
	let mut highlights = lexical_highlights_for_tokens(provider, virtual_tokens);
	if let Some((visible_start, visible_end)) = loaded_token_byte_range(visible_tokens) {
		// The wider context window can start in an unloaded DMA chunk on large files.
		// If that window produces no spans covering the actually visible bytes, rerun
		// the lexer on the loaded visible tokens so the viewport never falls back to white.
		if !highlights_overlap_range(&highlights, visible_start, visible_end) {
			highlights = lexical_highlights_for_tokens(provider, visible_tokens);
		}
	}
	highlights
}

fn lexical_highlights_for_tokens(
	provider: &dyn LanguageProvider,
	tokens: &[RenderToken],
) -> Vec<HighlightSpan> {
	let mut buffer = Vec::new();
	let mut offset_map: Vec<(usize, DocByte)> = Vec::new();
	for token in tokens {
		offset_map.push((buffer.len(), token.absolute_start_byte));
		buffer.extend_from_slice(&token.text);
	}
	let mut highlights = provider.lexical_baseline(&buffer, &offset_map);
	highlights.sort_unstable_by_key(|span| span.start);
	highlights
}

fn loaded_token_byte_range(tokens: &[RenderToken]) -> Option<(DocByte, DocByte)> {
	let mut start = None;
	let mut end = DocByte::ZERO;
	for token in tokens {
		if token.text.is_empty() {
			continue;
		}
		if start.is_none() {
			start = Some(token.absolute_start_byte);
		}
		end = token
			.absolute_start_byte
			.saturating_add(token.text.len() as u64);
	}
	start.map(|start| (start, end))
}

fn highlights_overlap_range(
	highlights: &[HighlightSpan],
	range_start: DocByte,
	range_end: DocByte,
) -> bool {
	if range_start >= range_end {
		return false;
	}
	let search = highlights.partition_point(|span| span.end <= range_start);
	highlights[search..]
		.first()
		.is_some_and(|span| span.start < range_end)
}

pub(crate) fn merge_highlights(
	lexical: &[HighlightSpan],
	semantic: &[HighlightSpan],
) -> Vec<HighlightSpan> {
	HighlightSpanIndex::merged(lexical, semantic).into_spans()
}

#[cfg(test)]
mod tests {
	use super::{merge_highlights, project_gutter_line_numbers, viewport_is_busy};
	use crate::core::{DocByte, DocLine};
	use crate::engine::GutterNumbering;
	use crate::engine::folding::FoldSet;
	use crate::engine::test_support::build_foldable_document;
	use crate::svp::rust_highlight::{HighlightSpan, TokenCategory};

	fn number_for(numbers: &[crate::uast::projection::GutterLineNumber], line: u64) -> u64 {
		numbers
			.iter()
			.find(|entry| entry.line == DocLine::new(line))
			.expect("line should have a projected gutter number")
			.number
	}

	#[test]
	fn relative_gutter_numbers_count_folded_subtrees_as_one_move() {
		let (registry, root, folded) = build_foldable_document();
		let mut folds = FoldSet::default();
		folds.insert(folded);
		let visible_lines = [DocLine::ZERO, DocLine::new(1), DocLine::new(3)];

		let numbers = project_gutter_line_numbers(
			&registry,
			Some(root),
			&folds,
			&visible_lines,
			DocLine::ZERO,
			DocLine::ZERO,
			GutterNumbering::Relative,
		);

		assert_eq!(number_for(&numbers, 0), 0);
		assert_eq!(number_for(&numbers, 1), 1);
		assert_eq!(number_for(&numbers, 3), 2);
	}

	#[test]
	fn hybrid_gutter_keeps_only_cursor_line_absolute() {
		let (registry, root, folded) = build_foldable_document();
		let mut folds = FoldSet::default();
		folds.insert(folded);
		let visible_lines = [DocLine::ZERO, DocLine::new(1), DocLine::new(3)];

		let numbers = project_gutter_line_numbers(
			&registry,
			Some(root),
			&folds,
			&visible_lines,
			DocLine::ZERO,
			DocLine::ZERO,
			GutterNumbering::Hybrid,
		);

		assert_eq!(number_for(&numbers, 0), 1);
		assert_eq!(number_for(&numbers, 1), 1);
		assert_eq!(number_for(&numbers, 3), 2);
	}

	#[test]
	fn pending_semantic_analysis_keeps_the_viewport_busy() {
		assert!(viewport_is_busy(false, true));
		assert!(viewport_is_busy(true, false));
		assert!(!viewport_is_busy(false, false));
	}

	#[test]
	fn injected_highlights_consistently_override_parent_markdown_spans() {
		let span = |start, end, category| {
			HighlightSpan::new(DocByte::new(start), DocByte::new(end), category)
		};
		let lexical = [span(0, 30, TokenCategory::String)];
		// The Markdown provider emits its parent span first. Injected SQL spans
		// are appended afterward, including one tied at the parent's start.
		let semantic = [
			span(0, 30, TokenCategory::String),
			span(0, 6, TokenCategory::Keyword),
			span(10, 16, TokenCategory::Keyword),
			span(20, 26, TokenCategory::Keyword),
		];
		let merged = merge_highlights(&lexical, &semantic);

		for byte in [0, 10, 20] {
			let category = merged
				.iter()
				.rev()
				.find(|span| span.start.get() <= byte && byte < span.end.get())
				.map(|span| span.category);
			assert_eq!(category, Some(TokenCategory::Keyword));
		}
	}

	#[test]
	fn nested_semantic_parent_overwrites_lexical_after_inner_span_ends() {
		let span = |start, end, category| {
			HighlightSpan::new(DocByte::new(start), DocByte::new(end), category)
		};
		let lexical = [
			span(15, 18, TokenCategory::Variable),
			span(50, 60, TokenCategory::Variable),
		];
		let semantic = [
			span(0, 100, TokenCategory::String),
			span(10, 20, TokenCategory::Keyword),
		];

		let merged = merge_highlights(&lexical, &semantic);
		assert_eq!(merged, semantic);
	}
}
