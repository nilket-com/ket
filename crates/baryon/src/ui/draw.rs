use super::Frontend;
use super::markdown_preview::render_markdown_preview_fallback;
use super::minimap::render_minimap_snapshot;
use crate::ui::*;
use libaryon::core::{DOCTAB_BAR_ROWS, PaneLocalRow, PaneRect, Rgb, STATUS_BAR_ROWS, TAB_SIZE};
use libaryon::engine::{
	EditorMode, InlineAnnotationKind, MinimapSnapshot, UiPalette, VisualKind,
	viewport_geometry_for_viewport_with_numbering,
};
use libaryon::svp::diagnostic::DiagnosticSeverity;
use libaryon::svp::projector::{DiagnosticProjector, HighlightProjector};
use libaryon::svp::rust_highlight::default_text_color;
use libaryon::uast::projection::{DocTabSummary, PaneViewport, ProjectedCaret};
use ratatui::{
	backend::Backend,
	buffer::Buffer,
	layout::Rect,
	style::{Color, Modifier, Style},
};
use regex_automata::meta::Regex;
use regex_automata::util::syntax;
use std::io;

/// Three dot-columns rendered across two Braille cells.
///
/// A Unicode Braille cell is only two dot-columns wide. The left cell's right
/// column plus both columns of the right cell form a stable 3×4 canvas; three
/// adjacent dots chase around its ten-dot perimeter.
const BUSY_SPINNER_FRAMES: [[char; 2]; 10] = [
	['⠸', '⠀'],
	['⠘', '⠁'],
	['⠈', '⠉'],
	['⠀', '⠙'],
	['⠀', '⠸'],
	['⠀', '⢰'],
	['⠀', '⣠'],
	['⢀', '⣀'],
	['⢠', '⡀'],
	['⢰', '⠀'],
];

fn braille_spinner_frame(frame: usize) -> [char; 2] {
	BUSY_SPINNER_FRAMES[frame % BUSY_SPINNER_FRAMES.len()]
}

fn paint_busy_spinner(
	buf: &mut Buffer,
	x: &mut usize,
	width: usize,
	y: u16,
	frame: usize,
	bar_style: Style,
	accent: Rgb,
) {
	if *x < width {
		*x += 1;
	}
	let frame = braille_spinner_frame(frame);
	for glyph in frame {
		if *x >= width {
			break;
		}
		if let Some(cell) = buf.cell_mut((*x as u16, y)) {
			cell.set_char(glyph)
				.set_style(bar_style.fg(ratatui_color(accent)));
		}
		*x += 1;
	}
}

fn paint_git_branch(
	buf: &mut Buffer,
	x: &mut usize,
	width: usize,
	y: u16,
	branch: &str,
	bar_style: Style,
	accent: Rgb,
) {
	let branch_style = bar_style.fg(ratatui_color(accent));
	for c in [' ', '|', ' '].into_iter().chain(branch.chars()) {
		if *x >= width {
			break;
		}
		if let Some(cell) = buf.cell_mut((*x as u16, y)) {
			cell.set_char(c).set_style(branch_style);
		}
		*x += 1;
	}
}

fn draw_gutter_line_number(
	buf: &mut Buffer,
	x_offset: u16,
	gutter_width: u16,
	y: usize,
	line_index: u64,
	total_newlines: u64,
	number: u64,
	number_style: Style,
) {
	if y > u16::MAX as usize || line_index > total_newlines {
		return;
	}

	let line_str = number.to_string();
	if line_str.len() >= gutter_width as usize {
		return;
	}

	let start_x = x_offset + (gutter_width - 1).saturating_sub(line_str.len() as u16);
	for (i, c) in line_str.chars().enumerate() {
		if let Some(cell) = buf.cell_mut((start_x + i as u16, y as u16)) {
			cell.set_char(c).set_style(number_style);
		}
	}
}

fn gutter_number_for_line(pane: &PaneViewport, line_index: u64) -> Option<u64> {
	let line = libaryon::core::DocLine::new(line_index);
	pane.gutter_line_numbers
		.binary_search_by_key(&line, |entry| entry.line)
		.ok()
		.map(|index| pane.gutter_line_numbers[index].number)
}

fn paint_text_segment(
	buf: &mut Buffer,
	x: &mut usize,
	visual_y: &mut usize,
	current_doc_line: u64,
	current_display_col: &mut u32,
	next_render_row: &mut usize,
	render_rows: &[libaryon::engine::VisibleRow],
	text_left: usize,
	text_right: usize,
	render_height: usize,
	skip_rows: usize,
	screen_y_offset: usize,
	text: &str,
	style: Style,
	wrap_enabled: bool,
) -> bool {
	if text_right <= text_left {
		return *visual_y < skip_rows.saturating_add(render_height);
	}

	for c in text.chars() {
		if wrap_enabled {
			while let Some(row) = render_rows.get(*next_render_row) {
				if row.line.get() < current_doc_line
					|| (row.line.get() == current_doc_line
						&& row.start_col.get() < *current_display_col)
				{
					*next_render_row += 1;
					continue;
				}
				if row.line.get() == current_doc_line && row.start_col.get() == *current_display_col
				{
					if row.start_col != libaryon::core::DocDisplayCol::ZERO {
						*visual_y += 1;
						*x = text_left;
					}
					*next_render_row += 1;
				}
				break;
			}
		}
		if *visual_y >= skip_rows.saturating_add(render_height) {
			return false;
		}
		if *x >= text_right {
			if wrap_enabled && render_rows.get(*next_render_row).is_none() {
				return false;
			}
			*x += 1;
			*current_display_col = current_display_col.saturating_add(1);
			continue;
		}
		if *visual_y >= skip_rows {
			let screen_y = screen_y_offset + (*visual_y - skip_rows);
			if screen_y >= screen_y_offset.saturating_add(render_height) {
				return false;
			}
			if let Some(cell) = buf.cell_mut((*x as u16, screen_y as u16)) {
				cell.set_char(c).set_style(style);
			}
		}
		*x += 1;
		*current_display_col = current_display_col.saturating_add(1);
	}

	*visual_y < skip_rows.saturating_add(render_height)
}

fn next_utf8_chunk(bytes: &[u8]) -> Option<(char, usize)> {
	let max_len = bytes.len().min(4);
	for len in 1..=max_len {
		let prefix = &bytes[..len];
		if let Ok(text) = std::str::from_utf8(prefix)
			&& let Some(ch) = text.chars().next()
			&& ch.len_utf8() == len
		{
			return Some((ch, len));
		}
	}
	None
}

fn segment_overlaps(ranges: &[(usize, usize)], start: usize, len: usize) -> bool {
	let end = start.saturating_add(len);
	ranges
		.iter()
		.any(|&(range_start, range_end)| range_start < end && range_end > start)
}

fn apply_diagnostic_style(style: Style, severity: Option<DiagnosticSeverity>) -> Style {
	match severity {
		Some(DiagnosticSeverity::Error) => style
			.add_modifier(Modifier::UNDERLINED)
			.underline_color(DIAGNOSTIC_ERROR_UNDERLINE),
		Some(_) | None => style,
	}
}

fn folded_placeholder_style(accent: Rgb) -> Style {
	Style::default()
		.fg(ratatui_color(accent))
		.bg(FOLDED_PLACEHOLDER_BG)
		.add_modifier(Modifier::BOLD)
}

fn gutter_fill_style(palette: UiPalette) -> Style {
	Style::default()
		.bg(ratatui_color(palette.gutter_surface))
		.fg(ratatui_color(palette.gutter_text))
}

fn cursor_gutter_line_number_style(palette: UiPalette) -> Style {
	Style::default()
		.bg(ratatui_color(palette.gutter_surface))
		.fg(ratatui_color(palette.gutter_accent))
		.add_modifier(Modifier::BOLD)
}

fn gutter_line_number_style(
	line_index: u64,
	cursor_lines: &[libaryon::core::DocLine],
	palette: UiPalette,
) -> Style {
	if cursor_lines.iter().any(|line| line.get() == line_index) {
		cursor_gutter_line_number_style(palette)
	} else {
		gutter_fill_style(palette)
	}
}

fn status_right_text(
	search_info: Option<&str>,
	wrap_enabled: bool,
	file_size: u64,
	line_count: Option<u64>,
	cursor_line: u64,
	cursor_col: u32,
	cursor_count: usize,
	language_label: &str,
) -> String {
	let size_str = format_file_size(file_size);
	let lines_str = line_count
		.map(format_line_count)
		.unwrap_or_else(|| "counting lines".to_string());
	let wrap_label = if wrap_enabled { "WRAP" } else { "NOWRAP" };
	let lang_label = language_label;
	let cursor_label = (cursor_count > 1).then(|| format!("{cursor_count} cursors | "));
	let cursor_label = cursor_label.as_deref().unwrap_or("");
	match search_info {
		Some(info) => format!(
			"{} | {}{} | {} | {} | {} | UTF-8 | {}:{} ",
			info,
			cursor_label,
			wrap_label,
			size_str,
			lines_str,
			lang_label,
			cursor_line,
			cursor_col
		),
		None => format!(
			"{}{} | {} | {} | {} | UTF-8 | {}:{} ",
			cursor_label, wrap_label, size_str, lines_str, lang_label, cursor_line, cursor_col
		),
	}
}

fn prompt_window(buffer: &str, cursor_byte: usize, width: usize) -> (String, usize) {
	if width == 0 {
		return (String::new(), 0);
	}

	let mut cursor_byte = cursor_byte.min(buffer.len());
	while !buffer.is_char_boundary(cursor_byte) {
		cursor_byte -= 1;
	}
	let cursor_char = buffer[..cursor_byte].chars().count();
	let start = cursor_char.saturating_sub(width.saturating_sub(1));
	let visible = buffer.chars().skip(start).take(width).collect();
	let cursor_col = cursor_char.saturating_sub(start).min(width - 1);
	(visible, cursor_col)
}

fn should_render_preview_snapshot_fallback(rendered_preview: bool, preview_pending: bool) -> bool {
	!rendered_preview && !preview_pending
}

fn paint_secondary_carets(
	buf: &mut Buffer,
	carets: &[ProjectedCaret],
	pane_x: u16,
	gutter_width: u16,
	text_right: u16,
	text_top: u16,
	text_height: u16,
) {
	for caret in carets {
		let screen_x = pane_x
			.saturating_add(gutter_width)
			.saturating_add(caret.pane_col.get().min(u32::from(u16::MAX)) as u16);
		let screen_y =
			text_top.saturating_add(caret.pane_row.get().min(u32::from(u16::MAX)) as u16);
		if screen_x < text_right
			&& caret.pane_row.get() < u32::from(text_height)
			&& let Some(cell) = buf.cell_mut((screen_x, screen_y))
		{
			cell.set_style(Style::default().add_modifier(Modifier::REVERSED));
		}
	}
}

fn pane_rect_to_area(rect: PaneRect) -> Rect {
	Rect {
		x: rect.screen_col.get() as u16,
		y: rect.screen_row.get() as u16,
		width: rect.width.get() as u16,
		height: rect.height.get() as u16,
	}
}

fn effective_editor_background(background_override: Option<Rgb>, palette: UiPalette) -> Rgb {
	background_override.unwrap_or(palette.editor_background)
}

fn draw_tab_bar(
	buf: &mut Buffer,
	area: Rect,
	tabs: &[DocTabSummary],
	active_index: usize,
	is_focused: bool,
	palette: UiPalette,
) {
	if area.width == 0 || area.height == 0 {
		return;
	}

	let (base_bg, base_fg, active_bg, active_fg) = if is_focused {
		(
			ratatui_color(palette.tab_surface),
			ratatui_color(palette.tab_text),
			ratatui_color(palette.tab_active_surface),
			ratatui_color(palette.tab_accent),
		)
	} else {
		(
			ratatui_color(palette.tab_active_surface),
			ratatui_color(palette.tab_text),
			ratatui_color(palette.tab_surface),
			ratatui_color(palette.tab_accent),
		)
	};
	let base_style = Style::default().bg(base_bg).fg(base_fg);
	let active_style = Style::default()
		.bg(active_bg)
		.fg(active_fg)
		.add_modifier(Modifier::BOLD);
	for x in 0..area.width {
		if let Some(cell) = buf.cell_mut((area.x + x, area.y)) {
			cell.set_char(' ').set_style(base_style);
		}
	}

	let mut x = area.x as usize;
	let max_x = area.x as usize + area.width as usize;
	for (idx, tab) in tabs.iter().enumerate() {
		if x >= max_x {
			break;
		}
		let style = if idx == active_index {
			active_style
		} else {
			base_style
		};
		let mut label = format!(" {}", tab.title);
		if tab.is_dirty {
			label.push_str(" +");
		}
		label.push_str(" x ");
		for ch in label.chars() {
			if x >= max_x {
				break;
			}
			if let Some(cell) = buf.cell_mut((x as u16, area.y)) {
				cell.set_char(ch).set_style(style);
			}
			x += 1;
		}
		if x < max_x {
			if let Some(cell) = buf.cell_mut((x as u16, area.y)) {
				cell.set_char('|').set_style(base_style);
			}
			x += 1;
		}
	}
}

#[derive(Default, Clone, Copy)]
struct PaneDrawArtifacts {
	cursor_to_set: Option<(u16, u16)>,
	minimap_area: Option<Rect>,
	markdown_preview_area: Option<Rect>,
}

fn draw_pane(
	buf: &mut Buffer,
	pane: &PaneViewport,
	is_focused: bool,
	search_pattern: Option<&str>,
	search_case_insensitive: bool,
	palette: UiPalette,
) -> PaneDrawArtifacts {
	let pane_area = pane_rect_to_area(pane.rect);
	if pane_area.width == 0 || pane_area.height == 0 {
		return PaneDrawArtifacts::default();
	}

	let tab_bar_area = Rect {
		x: pane_area.x,
		y: pane_area.y,
		width: pane_area.width,
		height: DOCTAB_BAR_ROWS.min(pane_area.height),
	};
	draw_tab_bar(
		buf,
		tab_bar_area,
		pane.tabs.as_slice(),
		pane.active_doctab_index,
		is_focused,
		palette,
	);

	let text_area_top = pane.rect.screen_row.saturating_add(DOCTAB_BAR_ROWS.into());
	let text_area_height = pane.rect.height.saturating_sub(DOCTAB_BAR_ROWS.into());
	if text_area_height.get() == 0 {
		return PaneDrawArtifacts::default();
	}

	if let Some(panel) = pane.host_panel.as_ref() {
		// Baryon cannot draw a host panel's contents — by design, that belongs
		// to the embedding frontend. Render an honest placeholder so the pane
		// is visible and identifiable standalone rather than blank.
		let area = Rect {
			x: pane_area.x,
			y: text_area_top.get() as u16,
			width: pane_area.width,
			height: text_area_height.get() as u16,
		};
		let surface = ratatui_color(palette.preview_surface);
		let fill = Style::default().bg(surface).fg(surface);
		for row in 0..area.height {
			for col in 0..area.width {
				if let Some(cell) = buf.cell_mut((area.x + col, area.y + row)) {
					cell.set_char(' ').set_style(fill);
				}
			}
		}
		let label = format!("[{}: {}]", panel.kind, panel.title);
		let label_style = Style::default()
			.bg(surface)
			.fg(ratatui_color(palette.preview_text));
		let label_row = area.y + area.height / 2;
		let label_col = area
			.x
			.saturating_add(area.width.saturating_sub(label.chars().count() as u16) / 2);
		for (offset, character) in label.chars().enumerate() {
			let x = label_col.saturating_add(offset as u16);
			if x >= area.x.saturating_add(area.width) {
				break;
			}
			if let Some(cell) = buf.cell_mut((x, label_row)) {
				cell.set_char(character).set_style(label_style);
			}
		}
		return PaneDrawArtifacts::default();
	}

	if pane.markdown_preview.is_some() {
		let preview_area = Rect {
			x: pane_area.x,
			y: text_area_top.get() as u16,
			width: pane_area.width,
			height: text_area_height.get() as u16,
		};
		let preview_surface = ratatui_color(palette.preview_surface);
		let bg_style = Style::default().bg(preview_surface).fg(preview_surface);
		for row in 0..preview_area.height {
			for col in 0..preview_area.width {
				if let Some(cell) = buf.cell_mut((preview_area.x + col, preview_area.y + row)) {
					cell.set_char(' ').set_style(bg_style);
				}
			}
		}
		return PaneDrawArtifacts {
			cursor_to_set: None,
			minimap_area: None,
			markdown_preview_area: Some(preview_area),
		};
	}

	let geometry = viewport_geometry_for_viewport_with_numbering(
		pane.viewport_start_line,
		pane.viewport_line_count,
		pane.max_doc_line,
		pane_area.width,
		pane.minimap.is_some(),
		pane.gutter_numbering,
	);
	let minimap_width = geometry.minimap_width;
	let separator_width = geometry.separator_width;
	let gutter_width = geometry.gutter_width;
	let text_left = pane_area.x as usize + gutter_width as usize;
	let text_right = text_left + geometry.text_width as usize;
	let minimap_area = Rect {
		x: pane_area.x + pane_area.width.saturating_sub(minimap_width),
		y: text_area_top.get() as u16,
		width: minimap_width,
		height: text_area_height.get() as u16,
	};

	let effective_bg = ratatui_color(effective_editor_background(
		pane.background_override,
		palette,
	));
	let render_height = text_area_height.get() as usize;
	let skip_rows = if pane.wrap_enabled {
		pane.viewport_row_offset.get() as usize
	} else {
		0
	};
	let max_visual_rows = skip_rows.saturating_add(render_height);

	let bg_style = Style::default().bg(effective_bg).fg(effective_bg);
	for gy in 0..render_height as u32 {
		let screen_y = text_area_top.saturating_add(gy);
		for gx in (pane_area.x + gutter_width)..(text_right as u16) {
			if let Some(cell) = buf.cell_mut((gx, screen_y.get() as u16)) {
				cell.set_char(' ').set_style(bg_style);
			}
		}
	}

	for gy in 0..render_height as u32 {
		let screen_y = text_area_top.saturating_add(gy);
		for gx in pane_area.x..pane_area.x.saturating_add(gutter_width) {
			if let Some(cell) = buf.cell_mut((gx, screen_y.get() as u16)) {
				cell.set_char(' ').set_style(gutter_fill_style(palette));
			}
		}
	}

	if minimap_width > 0 {
		let minimap_surface = ratatui_color(palette.minimap_surface);
		let separator_x = pane_area.x.saturating_add(
			pane_area
				.width
				.saturating_sub(minimap_width + separator_width),
		);
		for gy_idx in 0..render_height as u32 {
			let gy = text_area_top.saturating_add(gy_idx);
			if let Some(cell) = buf.cell_mut((separator_x, gy.get() as u16)) {
				cell.set_char(' ')
					.set_style(Style::default().bg(minimap_surface).fg(minimap_surface));
			}
			for gx in minimap_area.x..(minimap_area.x + minimap_area.width) {
				if let Some(cell) = buf.cell_mut((gx, gy.get() as u16)) {
					cell.set_char(' ')
						.set_style(Style::default().bg(minimap_surface).fg(minimap_surface));
				}
			}
		}
	}

	if let Some(diff_pair) = &pane.diff_pair {
		for block in &diff_pair.blocks {
			let color = match block.kind {
				libaryon::uast::projection::DiffKind::Added => Color::Rgb(0, 48, 0),
				libaryon::uast::projection::DiffKind::Removed => Color::Rgb(48, 0, 0),
				libaryon::uast::projection::DiffKind::Modified => Color::Rgb(0, 0, 48),
				libaryon::uast::projection::DiffKind::Moved => Color::Rgb(48, 0, 48),
			};

			for (pane_row_idx, row) in pane.visible_rows.iter().enumerate() {
				if row.line >= block.range.0 && row.line < block.range.1 {
					let pane_row = PaneLocalRow::new(pane_row_idx as u32);
					let screen_y = text_area_top.saturating_add(pane_row.get());
					if screen_y < text_area_top.saturating_add(text_area_height.get()) {
						for gx in pane_area.x..pane_area.x + pane_area.width {
							if let Some(cell) = buf.cell_mut((gx, screen_y.get() as u16)) {
								cell.set_bg(color);
							}
						}
					}
				}
			}

			// Layer 3 overlay: keep the soft-blue block background but
			// intensify the specific token columns that actually changed so
			// reviewers can see what moved inside the modified line.
			if let Some(intra_line_diffs) = block.intra_line_diffs.as_ref() {
				for intra in intra_line_diffs {
					let intense = match intra.kind {
						libaryon::uast::projection::IntraLineDiffKind::Added => {
							Color::Rgb(16, 120, 16)
						}
						libaryon::uast::projection::IntraLineDiffKind::Removed => {
							Color::Rgb(120, 16, 16)
						}
						libaryon::uast::projection::IntraLineDiffKind::Modified => {
							Color::Rgb(32, 64, 180)
						}
					};

					for (pane_row_idx, row) in pane.visible_rows.iter().enumerate() {
						if row.line != intra.line {
							continue;
						}

						let pane_row = PaneLocalRow::new(pane_row_idx as u32);
						let screen_y = text_area_top.saturating_add(pane_row.get());
						if screen_y >= text_area_top.saturating_add(text_area_height.get()) {
							continue;
						}

						// A line that exceeds the viewport width is split into
						// multiple visible rows; each row owns a col window
						// [start_col, start_col + visible_width). Clip the
						// intra-line range to this row's window.
						let row_start_col = row.start_col.get();
						let visible_width = geometry.text_width as u32;
						let row_end_col = row_start_col.saturating_add(visible_width);

						let clip_start = intra.col_start.max(row_start_col);
						let mut clip_end = intra.col_end.min(row_end_col);
						// Zero-length ranges (pure insertions/deletions) still
						// deserve a single-column marker so the user can see
						// the insertion point on the opposing pane.
						if intra.col_start == intra.col_end
							&& intra.col_start >= row_start_col
							&& intra.col_start < row_end_col
						{
							clip_end = clip_start.saturating_add(1).min(row_end_col);
						}
						if clip_end <= clip_start {
							continue;
						}

						let gx_start = (text_left as u32)
							.saturating_add(clip_start.saturating_sub(row_start_col));
						let gx_end = (text_left as u32)
							.saturating_add(clip_end.saturating_sub(row_start_col));
						let pane_right = pane_area.x as u32 + pane_area.width as u32;
						let gx_end = gx_end.min(text_right as u32).min(pane_right);
						let gx_start = gx_start.min(gx_end);

						for gx in (gx_start as u16)..(gx_end as u16) {
							if let Some(cell) = buf.cell_mut((gx, screen_y.get() as u16)) {
								cell.set_bg(intense);
							}
						}
					}
				}
			}
		}
	}

	let mut x = text_left;
	let mut visual_y: usize = 0;
	let mut current_doc_line = pane.viewport_start_line.get();
	let mut current_display_col = 0u32;
	let mut next_render_row = 0usize;
	let mut line_has_content = false;
	if pane.gutter_numbering.is_some()
		&& render_height > 0
		&& skip_rows == 0
		&& let Some(number) = gutter_number_for_line(pane, current_doc_line)
	{
		draw_gutter_line_number(
			buf,
			pane_area.x,
			gutter_width,
			text_area_top.get() as usize,
			current_doc_line,
			pane.max_doc_line.get(),
			number,
			gutter_line_number_style(current_doc_line, &pane.cursor_lines, palette),
		);
	}

	let search_pat = search_pattern.unwrap_or("");
	let projector = HighlightProjector::new(pane.highlights.clone(), pane.theme_colors);
	let diagnostic_projector = DiagnosticProjector::new(pane.diagnostics.clone());
	let search_highlight_style = Style::default()
		.bg(ratatui_color(palette.search_highlight))
		.fg(contrasting_text_color(palette.search_highlight));
	let mut current_global_byte = pane.global_start_byte;

	for token in &pane.tokens {
		if x != text_left
			&& token.absolute_start_line.get() > current_doc_line
			&& visual_y < max_visual_rows
		{
			visual_y += 1;
			x = text_left;
			current_display_col = 0;
		}
		if x == text_left && token.absolute_start_line.get() != current_doc_line {
			current_doc_line = token.absolute_start_line.get();
			current_display_col = 0;
			if pane.gutter_numbering.is_some()
				&& visual_y >= skip_rows
				&& visual_y.saturating_sub(skip_rows) < render_height
				&& let Some(number) = gutter_number_for_line(pane, current_doc_line)
			{
				draw_gutter_line_number(
					buf,
					pane_area.x,
					gutter_width,
					(visual_y - skip_rows) + text_area_top.get() as usize,
					current_doc_line,
					pane.max_doc_line.get(),
					number,
					gutter_line_number_style(current_doc_line, &pane.cursor_lines, palette),
				);
			}
		}

		// Text no highlighter claimed is `BodyText`, the same value the Markdown
		// preview paints prose with, so a themed body color reaches both panes.
		let base_style = Style::default().fg(ratatui_color(default_text_color(
			&pane.theme_colors,
			palette.editor_text,
		)));
		let virtual_style = base_style;
		let has_physical_bytes = !token.text.is_empty() && !token.is_folded;
		if !token.is_folded && !has_physical_bytes {
			continue;
		}
		let text = token.text.as_slice();

		if token.is_folded {
			let mut style = folded_placeholder_style(palette.accent);
			let folded_start = token.absolute_start_byte;
			let folded_end_exclusive = folded_start.saturating_add(token.physical_byte_len as u64);
			let folded_end = folded_end_exclusive.saturating_sub(1);
			let in_selection = pane
				.selection_ranges
				.iter()
				.any(|(start, end)| *start <= folded_end && *end >= folded_start);
			if in_selection {
				style = style.bg(SELECTION_BG);
			}
			if let Some((flash_start, flash_end)) = pane.yank_flash
				&& flash_start <= folded_end
				&& flash_end >= folded_start
			{
				style = Style::default().bg(MODE_NORMAL_BG).fg(MODE_TEXT_FG);
			}
			style = apply_diagnostic_style(
				style,
				diagnostic_projector.severity_for_range(folded_start, folded_end_exclusive),
			);

			let display = std::str::from_utf8(text).unwrap_or("❯❯❯");
			if !paint_text_segment(
				buf,
				&mut x,
				&mut visual_y,
				current_doc_line,
				&mut current_display_col,
				&mut next_render_row,
				&pane.render_rows,
				text_left,
				text_right,
				render_height,
				skip_rows,
				text_area_top.get() as usize,
				display,
				style,
				pane.wrap_enabled,
			) {
				visual_y = max_visual_rows;
			}
			current_global_byte = token
				.absolute_start_byte
				.saturating_add(token.physical_byte_len as u64);
			continue;
		}

		let mut highlight_ranges: Vec<(usize, usize)> = Vec::new();
		if !search_pat.is_empty()
			&& has_physical_bytes
			&& let Ok(re) = Regex::builder()
				.syntax(syntax::Config::new().case_insensitive(search_case_insensitive))
				.build(search_pat)
		{
			for m in re.find_iter(text) {
				highlight_ranges.push((m.start(), m.end()));
			}
		}

		let mut byte_idx = 0usize;
		while byte_idx < text.len() {
			if visual_y >= max_visual_rows {
				break;
			}

			let remaining = &text[byte_idx..];
			let (chunk_kind, chunk_len) = if let Some((ch, ch_len)) = next_utf8_chunk(remaining) {
				if ch == '\n' {
					(0u8, 1usize)
				} else if ch == '\t' {
					(1u8, 1usize)
				} else if ch.is_control() {
					(2u8, ch_len)
				} else if ch == ' ' {
					(4u8, 1usize)
				} else {
					(3u8, ch_len)
				}
			} else {
				(2u8, 1usize)
			};

			let in_highlight = segment_overlaps(&highlight_ranges, byte_idx, chunk_len);
			let mut style = if in_highlight {
				search_highlight_style
			} else {
				virtual_style
			};

			if !in_highlight
				&& has_physical_bytes
				&& let Some(fg) = projector.style_for_byte(current_global_byte)
			{
				style = style.fg(ratatui_color(fg));
			}

			if has_physical_bytes {
				let chunk_start_byte = current_global_byte;
				let chunk_end_exclusive = current_global_byte.saturating_add(chunk_len as u64);
				let chunk_end_byte = chunk_end_exclusive.saturating_sub(1);
				let in_selection = pane
					.selection_ranges
					.iter()
					.any(|(start, end)| *start <= chunk_end_byte && *end >= chunk_start_byte);
				if in_selection {
					style = style.bg(SELECTION_BG);
				}

				if let Some((flash_start, flash_end)) = pane.yank_flash
					&& flash_start <= chunk_end_byte
					&& flash_end >= chunk_start_byte
				{
					style = Style::default().bg(MODE_NORMAL_BG).fg(MODE_TEXT_FG);
				}
				style = apply_diagnostic_style(
					style,
					diagnostic_projector.severity_for_range(chunk_start_byte, chunk_end_exclusive),
				);
			}

			match chunk_kind {
				0 => {
					if pane.visible_whitespace {
						let ws_style = style.fg(SYNTAX_WHITESPACE);
						paint_text_segment(
							buf,
							&mut x,
							&mut visual_y,
							current_doc_line,
							&mut current_display_col,
							&mut next_render_row,
							&pane.render_rows,
							text_left,
							text_right,
							render_height,
							skip_rows,
							text_area_top.get() as usize,
							"\u{00AC}",
							ws_style,
							pane.wrap_enabled,
						);
					}
					byte_idx += 1;
					if has_physical_bytes {
						current_global_byte = current_global_byte.saturating_add(1);
					}
					visual_y += 1;
					x = text_left;
					current_doc_line = current_doc_line.saturating_add(1);
					current_display_col = 0;
					line_has_content = false;
					if pane.gutter_numbering.is_some()
						&& visual_y >= skip_rows
						&& visual_y.saturating_sub(skip_rows) < render_height
						&& let Some(number) = gutter_number_for_line(pane, current_doc_line)
					{
						draw_gutter_line_number(
							buf,
							pane_area.x,
							gutter_width,
							(visual_y - skip_rows) + text_area_top.get() as usize,
							current_doc_line,
							pane.max_doc_line.get(),
							number,
							gutter_line_number_style(current_doc_line, &pane.cursor_lines, palette),
						);
					}
				}
				1 => {
					let spaces_to_add =
						TAB_SIZE as usize - (current_display_col as usize % TAB_SIZE as usize);
					if pane.visible_whitespace {
						let ws_style = style.fg(SYNTAX_WHITESPACE);
						let after = &remaining[1..];
						let has_newline_in_token = after.contains(&b'\n');
						let intra_token_only_ws = has_newline_in_token
							&& after
								.iter()
								.take_while(|&&b| b != b'\n')
								.all(|&b| b == b' ' || b == b'\t');
						let is_trailing = line_has_content && intra_token_only_ws;
						let glyph = if is_trailing { "~" } else { "\u{25B8}" };
						if !paint_text_segment(
							buf,
							&mut x,
							&mut visual_y,
							current_doc_line,
							&mut current_display_col,
							&mut next_render_row,
							&pane.render_rows,
							text_left,
							text_right,
							render_height,
							skip_rows,
							text_area_top.get() as usize,
							glyph,
							ws_style,
							pane.wrap_enabled,
						) {
							visual_y = max_visual_rows;
						}
						for _ in 1..spaces_to_add {
							if !paint_text_segment(
								buf,
								&mut x,
								&mut visual_y,
								current_doc_line,
								&mut current_display_col,
								&mut next_render_row,
								&pane.render_rows,
								text_left,
								text_right,
								render_height,
								skip_rows,
								text_area_top.get() as usize,
								" ",
								ws_style,
								pane.wrap_enabled,
							) {
								visual_y = max_visual_rows;
								break;
							}
						}
					} else {
						for _ in 0..spaces_to_add {
							if !paint_text_segment(
								buf,
								&mut x,
								&mut visual_y,
								current_doc_line,
								&mut current_display_col,
								&mut next_render_row,
								&pane.render_rows,
								text_left,
								text_right,
								render_height,
								skip_rows,
								text_area_top.get() as usize,
								" ",
								style,
								pane.wrap_enabled,
							) {
								visual_y = max_visual_rows;
								break;
							}
						}
					}
					byte_idx += 1;
					if has_physical_bytes {
						current_global_byte = current_global_byte.saturating_add(1);
					}
				}
				2 => {
					let ws_style = style.fg(SYNTAX_WHITESPACE);
					line_has_content = true;
					for &byte in &remaining[..chunk_len] {
						let hex = format!("<{:02X}>", byte);
						if !paint_text_segment(
							buf,
							&mut x,
							&mut visual_y,
							current_doc_line,
							&mut current_display_col,
							&mut next_render_row,
							&pane.render_rows,
							text_left,
							text_right,
							render_height,
							skip_rows,
							text_area_top.get() as usize,
							&hex,
							ws_style,
							pane.wrap_enabled,
						) {
							visual_y = max_visual_rows;
							break;
						}
					}
					byte_idx += chunk_len;
					if has_physical_bytes {
						current_global_byte = current_global_byte.saturating_add(chunk_len as u64);
					}
				}
				4 => {
					if pane.visible_whitespace {
						let after = &remaining[1..];
						let has_newline_in_token = after.contains(&b'\n');
						let intra_token_only_ws = has_newline_in_token
							&& after
								.iter()
								.take_while(|&&b| b != b'\n')
								.all(|&b| b == b' ' || b == b'\t');
						let is_trailing = line_has_content && intra_token_only_ws;
						let ws_style = style.fg(SYNTAX_WHITESPACE);
						let glyph = if is_trailing { "~" } else { "\u{2423}" };
						if !paint_text_segment(
							buf,
							&mut x,
							&mut visual_y,
							current_doc_line,
							&mut current_display_col,
							&mut next_render_row,
							&pane.render_rows,
							text_left,
							text_right,
							render_height,
							skip_rows,
							text_area_top.get() as usize,
							glyph,
							ws_style,
							pane.wrap_enabled,
						) {
							visual_y = max_visual_rows;
						}
					} else if !paint_text_segment(
						buf,
						&mut x,
						&mut visual_y,
						current_doc_line,
						&mut current_display_col,
						&mut next_render_row,
						&pane.render_rows,
						text_left,
						text_right,
						render_height,
						skip_rows,
						text_area_top.get() as usize,
						" ",
						style,
						pane.wrap_enabled,
					) {
						visual_y = max_visual_rows;
					}
					byte_idx += 1;
					if has_physical_bytes {
						current_global_byte = current_global_byte.saturating_add(1);
					}
				}
				_ => {
					let (ch, ch_len) =
						next_utf8_chunk(remaining).expect("printable chunk must decode");
					let mut encoded = [0u8; 4];
					let display = ch.encode_utf8(&mut encoded);
					if !paint_text_segment(
						buf,
						&mut x,
						&mut visual_y,
						current_doc_line,
						&mut current_display_col,
						&mut next_render_row,
						&pane.render_rows,
						text_left,
						text_right,
						render_height,
						skip_rows,
						text_area_top.get() as usize,
						display,
						style,
						pane.wrap_enabled,
					) {
						visual_y = max_visual_rows;
					}
					byte_idx += ch_len;
					if has_physical_bytes {
						current_global_byte = current_global_byte.saturating_add(ch_len as u64);
					}
					line_has_content = true;
				}
			}
		}
		if visual_y >= max_visual_rows {
			break;
		}
	}

	paint_secondary_carets(
		buf,
		&pane.secondary_carets,
		pane_area.x,
		gutter_width,
		text_right as u16,
		text_area_top.get() as u16,
		text_area_height.get() as u16,
	);
	paint_inline_annotations(
		buf,
		pane,
		text_left,
		text_right,
		text_area_top.get() as usize,
		render_height,
		skip_rows,
	);

	let cursor_to_set = if is_focused {
		let visual_cursor_y = pane.cursor_pane_row.get();
		let visual_cursor_x = (pane.cursor_pane_col.get() as u16)
			.checked_add(gutter_width)
			.and_then(|value| value.checked_add(pane_area.x))
			.unwrap_or(text_right as u16);
		let max_cursor_y = text_area_top.saturating_add(text_area_height.saturating_sub(1).into());
		let max_cursor_x = text_right.saturating_sub(1) as u16;
		Some((
			visual_cursor_x.min(max_cursor_x),
			(text_area_top.get() + visual_cursor_y).min(max_cursor_y.get()) as u16,
		))
	} else {
		None
	};

	PaneDrawArtifacts {
		cursor_to_set,
		minimap_area: if pane.minimap.is_some() && minimap_area.width > 0 && minimap_area.height > 0
		{
			Some(minimap_area)
		} else {
			None
		},
		markdown_preview_area: None,
	}
}

fn paint_inline_annotations(
	buf: &mut Buffer,
	pane: &PaneViewport,
	text_left: usize,
	text_right: usize,
	text_top: usize,
	render_height: usize,
	skip_rows: usize,
) {
	for annotation in &pane.inline_annotations {
		let Some(row) = pane
			.render_rows
			.iter()
			.enumerate()
			.rev()
			.find_map(|(row, visible)| (visible.line == annotation.line).then_some(row))
		else {
			continue;
		};
		let Some(screen_row) = row.checked_sub(skip_rows) else {
			continue;
		};
		if screen_row >= render_height {
			continue;
		}
		let width = annotation.text.chars().count();
		if width == 0 || width.saturating_add(2) > text_right.saturating_sub(text_left) {
			continue;
		}
		let Some(end_column) = annotation_end_column(pane, annotation.line) else {
			continue;
		};
		// Keep the hint visually associated with its dependency rather than with
		// the far edge of a wide monitor. Two tab stops leave a readable gap.
		let preferred_x = text_left.saturating_add(next_tab_stop(next_tab_stop(end_column)));
		let x = preferred_x.min(text_right.saturating_sub(width));
		let y = text_top.saturating_add(screen_row);
		let color = match annotation.kind {
			InlineAnnotationKind::Current => Color::Green,
			InlineAnnotationKind::Upgrade => Color::Yellow,
			InlineAnnotationKind::Info => Color::DarkGray,
		};
		for (offset, ch) in annotation.text.chars().enumerate() {
			if let Some(cell) = buf.cell_mut(((x + offset) as u16, y as u16)) {
				cell.set_char(ch);
				cell.set_fg(color);
			}
		}
	}
}

fn next_tab_stop(column: usize) -> usize {
	let tab = TAB_SIZE as usize;
	column.saturating_add(tab - column % tab)
}

fn annotation_end_column(
	pane: &PaneViewport,
	annotation_line: libaryon::core::DocLine,
) -> Option<usize> {
	let mut current_line = pane.tokens.first()?.absolute_start_line;
	let mut column = 0usize;
	let mut seen_line = false;

	for token in &pane.tokens {
		if token.absolute_start_line > current_line {
			current_line = token.absolute_start_line;
			column = 0;
		}
		for &byte in &token.text {
			if current_line == annotation_line {
				seen_line = true;
			}
			if byte == b'\n' {
				if current_line == annotation_line {
					return Some(column);
				}
				current_line = current_line.saturating_add(1);
				column = 0;
			} else if byte == b'\t' {
				column = next_tab_stop(column);
			} else if current_line == annotation_line {
				column = column.saturating_add(1);
			}
		}
	}

	seen_line.then_some(column)
}

impl<B: Backend + io::Write> Frontend<B> {
	pub(super) fn draw(&mut self) -> Result<(), B::Error> {
		let current_viewport = &self.current_viewport;
		let current_mode = self.editor_mode();
		let input_projection = current_viewport.as_ref().map(|view| &view.input);
		let status_message = &self.status_message;
		let git_branch = current_viewport
			.as_ref()
			.and_then(|view| view.git_branch.as_deref());
		let busy_spinner_frame = self.busy_spinner_frame;
		let prompt_buffer = input_projection
			.map(|input| input.prompt_buffer.as_str())
			.unwrap_or("");
		let prompt_cursor = input_projection.map_or(0, |input| input.prompt_cursor);
		let minimap = &mut self.minimap;
		let markdown_preview = &mut self.markdown_preview;
		let diff_bridge = &mut self.diff_bridge;
		let uast_graph = &mut self.uast_graph;
		let ui_palette = current_viewport
			.as_ref()
			.map(|view| view.ui_palette)
			.unwrap_or_default();
		// Spinner frames redraw independently of engine output. Keeping the terminal
		// cursor visible avoids a hide/show flash on every animation frame.
		let keep_cursor_visible = current_viewport.as_ref().is_some_and(|view| view.busy);

		if !keep_cursor_visible {
			let backend = self.terminal.backend_mut();
			backend.hide_cursor()?;
			Backend::flush(backend)?;
		}

		let draw_result = self
			.terminal
			.draw(|f| {
				let mut cursor_to_set = None;
				let mut pending_minimap_areas: Vec<(usize, Rect)> = Vec::new();
				let mut pending_markdown_preview_areas: Vec<(usize, Rect)> = Vec::new();
				let mut pending_uast_graph_areas: Vec<(usize, Rect)> = Vec::new();
				let max_width = f.area().width;
				let max_height = f.area().height;
				let status_bar_height = STATUS_BAR_ROWS;
				let editor_area_height = max_height.saturating_sub(status_bar_height);

				{
					let buf = f.buffer_mut();
					let divider_surface = ratatui_color(ui_palette.divider_surface);
					let divider_style = Style::default().bg(divider_surface).fg(divider_surface);
					for gy in 0..editor_area_height {
						for gx in 0..max_width {
							if let Some(cell) = buf.cell_mut((gx, gy)) {
								cell.set_char(' ').set_style(divider_style);
							}
						}
					}
					if let Some(view) = current_viewport {
						for (index, pane) in view.panes.iter().enumerate() {
							let artifacts = draw_pane(
								buf,
								pane,
								pane.pane_id == view.focused_pane_id,
								view.search_pattern.as_deref(),
								view.search_case_insensitive,
								view.ui_palette,
							);
							if artifacts.cursor_to_set.is_some() {
								cursor_to_set = artifacts.cursor_to_set;
							}
							if let Some(minimap_area) = artifacts.minimap_area {
								pending_minimap_areas.push((index, minimap_area));
							}
							if let Some(markdown_preview_area) = artifacts.markdown_preview_area {
								pending_markdown_preview_areas.push((index, markdown_preview_area));
							}
							if pane.uast_graph.is_some() {
								pending_uast_graph_areas.push((
									index,
									Rect {
										x: pane.rect.screen_col.get() as u16,
										y: pane
											.rect
											.screen_row
											.saturating_add(DOCTAB_BAR_ROWS.into())
											.get() as u16,
										width: pane.rect.width.get().min(u16::MAX as u32) as u16,
										height: pane
											.rect
											.height
											.saturating_sub(DOCTAB_BAR_ROWS.into())
											.get()
											.min(u16::MAX as u32)
											as u16,
									},
								));
							}
						}

						// Draw diff bridges
						for i in 0..view.panes.len() {
							let pane = &view.panes[i];
							if let Some(diff_pair) = &pane.diff_pair {
								// Find the partner pane
								if let Some(partner_idx) = view
									.panes
									.iter()
									.position(|p| p.pane_id == diff_pair.partner_pane_id)
								{
									let partner = &view.panes[partner_idx];

									// If this is the "left" pane of the pair (to avoid double drawing)
									if pane.rect.screen_col.get() < partner.rect.screen_col.get() {
										let bridge_top = pane
											.rect
											.screen_row
											.saturating_add(DOCTAB_BAR_ROWS.into());
										let bridge_height =
											pane.rect.height.saturating_sub(DOCTAB_BAR_ROWS.into());

										let bridge_area = Rect {
											x: pane.rect.right().get() as u16,
											y: bridge_top.get() as u16,
											width: partner
												.rect
												.screen_col
												.get()
												.saturating_sub(pane.rect.right().get())
												as u16,
											height: bridge_height.get() as u16,
										};

										if bridge_area.width > 0 && bridge_area.height > 0 {
											diff_bridge.request_bridge(
												pane,
												partner,
												bridge_area,
												bridge_top,
											);
											if let Some(protocol) = diff_bridge
												.get_protocol(pane.pane_id, partner.pane_id)
											{
												f.render_widget(
													ratatui_image::Image::new(protocol),
													bridge_area,
												);
											}
										}
									}
								}
							}
						}
					}
				}

				if let Some(view) = current_viewport {
					let live_view_ids: std::collections::HashSet<_> =
						view.panes.iter().map(|p| p.view_id).collect();
					minimap.retain_views(|view_id| live_view_ids.contains(&view_id));
					markdown_preview.retain_views(|view_id| live_view_ids.contains(&view_id));
					uast_graph.retain_views(|view_id| live_view_ids.contains(&view_id));

					for (pane_index, graph_area) in pending_uast_graph_areas {
						let Some(pane) = view.panes.get(pane_index) else {
							continue;
						};
						let Some(snapshot) = pane.uast_graph.as_ref() else {
							continue;
						};
						uast_graph.request(pane.view_id, snapshot, graph_area);
						uast_graph.render(pane.view_id, f, graph_area);
					}

					for (pane_index, preview_area) in pending_markdown_preview_areas {
						let Some(pane) = view.panes.get(pane_index) else {
							continue;
						};
						let Some(snapshot) = pane.markdown_preview.as_ref() else {
							continue;
						};

						let preview_theme_colors = view
							.view(snapshot.source_view_id)
							.map(|source| source.theme_colors)
							.unwrap_or(pane.theme_colors);
						markdown_preview.request_preview(
							pane.view_id,
							snapshot,
							preview_area,
							preview_theme_colors,
							view.search_pattern.as_deref(),
							view.search_case_insensitive,
							view.ui_palette,
						);
						let rendered_preview =
							markdown_preview.render(pane.view_id, f, preview_area);
						if !rendered_preview {
							let pending = markdown_preview.has_pending_preview(pane.view_id);
							let buf = f.buffer_mut();
							render_markdown_preview_fallback(
								buf,
								preview_area,
								pending,
								view.ui_palette,
							);
						}
					}

					for (pane_index, minimap_area) in pending_minimap_areas {
						let Some(pane) = view.panes.get(pane_index) else {
							continue;
						};
						let Some(snapshot) = pane.minimap.as_ref() else {
							continue;
						};

						match snapshot {
							MinimapSnapshot::Preview(preview) => {
								minimap.request_preview(
									pane.view_id,
									preview,
									minimap_area,
									pane.theme_colors,
									view.ui_palette,
								);
								let rendered_preview =
									minimap.render(pane.view_id, f, minimap_area);
								if should_render_preview_snapshot_fallback(
									rendered_preview,
									minimap.has_pending_preview(pane.view_id),
								) {
									let buf = f.buffer_mut();
									render_minimap_snapshot(
										buf,
										minimap_area,
										snapshot,
										&pane.theme_colors,
										view.ui_palette,
									);
								}
							}
							_ => {
								let buf = f.buffer_mut();
								render_minimap_snapshot(
									buf,
									minimap_area,
									snapshot,
									&pane.theme_colors,
									view.ui_palette,
								);
							}
						}
					}
				}

				let bar_y = max_height.saturating_sub(status_bar_height);
				let bar_bg = Style::default()
					.bg(ratatui_color(ui_palette.status_surface))
					.fg(ratatui_color(ui_palette.status_text));
				let w = max_width as usize;
				let buf = f.buffer_mut();

				for sx in 0..max_width {
					if let Some(cell) = buf.cell_mut((sx, bar_y)) {
						cell.set_char(' ').set_style(bar_bg);
					}
				}

				if let Some(msg) = status_message {
					for (i, c) in msg.chars().enumerate() {
						if i >= w {
							break;
						}
						if let Some(cell) = buf.cell_mut((i as u16, bar_y)) {
							cell.set_char(c);
						}
					}
				} else {
					let mode_str = match &current_mode {
						EditorMode::Normal => input_projection
							.and_then(|input| input.pending_sequence.as_deref())
							.map_or_else(|| "NOR".to_string(), |pending| format!("NOR {pending}")),
						EditorMode::Insert => "INS".to_string(),
						EditorMode::Command => "CMD".to_string(),
						EditorMode::Search => "FIND".to_string(),
						EditorMode::Password => "PASS".to_string(),
						EditorMode::Confirm => "Y/N".to_string(),
						EditorMode::Visual { kind, .. } => match kind {
							VisualKind::Char => "VIS".to_string(),
							VisualKind::Line => "VIS-LN".to_string(),
							VisualKind::Block => "VIS-BL".to_string(),
						},
					};
					let mode_style = bar_bg.fg(MODE_TEXT_FG).bg(match &current_mode {
						EditorMode::Normal => MODE_NORMAL_BG,
						EditorMode::Insert => MODE_INSERT_BG,
						EditorMode::Command => MODE_COMMAND_BG,
						EditorMode::Search => MODE_SEARCH_BG,
						EditorMode::Password => MODE_COMMAND_BG,
						EditorMode::Confirm => MODE_CONFIRM_BG,
						EditorMode::Visual { .. } => MODE_VISUAL_BG,
					});

					let mut x = 0usize;
					let mode_text = format!(" {mode_str} ");
					for c in mode_text.chars() {
						if x >= w {
							break;
						}
						if let Some(cell) = buf.cell_mut((x as u16, bar_y)) {
							cell.set_char(c).set_style(mode_style);
						}
						x += 1;
					}

					if x < w {
						if let Some(cell) = buf.cell_mut((x as u16, bar_y)) {
							cell.set_char(' ').set_style(bar_bg);
						}
						x += 1;
					}

					let (file_name, file_sz, dirty) = current_viewport
						.as_ref()
						.map(|v| (v.file_name.as_deref(), v.file_size, v.is_dirty))
						.unwrap_or((None, 0, false));

					if matches!(&current_mode, EditorMode::Command) {
						let prompt_start = x;
						if x < w {
							if let Some(cell) = buf.cell_mut((x as u16, bar_y)) {
								cell.set_char(':').set_style(bar_bg);
							}
							x += 1;
						}
						let available = w.saturating_sub(x);
						let (visible, prompt_cursor_col) =
							prompt_window(prompt_buffer, prompt_cursor, available);
						for c in visible.chars() {
							if x >= w {
								break;
							}
							if let Some(cell) = buf.cell_mut((x as u16, bar_y)) {
								cell.set_char(c).set_style(bar_bg);
							}
							x += 1;
						}
						let prompt_cursor_x = if available > 0 {
							prompt_start
								.saturating_add(1)
								.saturating_add(prompt_cursor_col)
						} else {
							prompt_start
						};
						cursor_to_set =
							Some((prompt_cursor_x.min(w.saturating_sub(1)) as u16, bar_y));
					} else if matches!(&current_mode, EditorMode::Search) {
						let prompt_start = x;
						if x < w {
							if let Some(cell) = buf.cell_mut((x as u16, bar_y)) {
								cell.set_char('/').set_style(bar_bg);
							}
							x += 1;
						}
						let available = w.saturating_sub(x);
						let (visible, prompt_cursor_col) =
							prompt_window(prompt_buffer, prompt_cursor, available);
						for c in visible.chars() {
							if x >= w {
								break;
							}
							if let Some(cell) = buf.cell_mut((x as u16, bar_y)) {
								cell.set_char(c).set_style(bar_bg);
							}
							x += 1;
						}
						let prompt_cursor_x = if available > 0 {
							prompt_start
								.saturating_add(1)
								.saturating_add(prompt_cursor_col)
						} else {
							prompt_start
						};
						cursor_to_set =
							Some((prompt_cursor_x.min(w.saturating_sub(1)) as u16, bar_y));
					} else if matches!(&current_mode, EditorMode::Confirm) {
						let prompt = current_viewport
							.as_ref()
							.and_then(|v| v.confirm_prompt.as_deref())
							.unwrap_or("Replace? [y/n/a/q]");
						for c in prompt.chars() {
							if x >= w {
								break;
							}
							if let Some(cell) = buf.cell_mut((x as u16, bar_y)) {
								cell.set_char(c).set_style(bar_bg);
							}
							x += 1;
						}
					} else {
						let display_name = file_name
							.map(|p| {
								std::path::Path::new(p)
									.file_name()
									.and_then(|n| n.to_str())
									.unwrap_or(p)
							})
							.unwrap_or("[No File]");
						let name_style = if dirty { bar_bg.fg(DIRTY_FG) } else { bar_bg };
						for c in display_name.chars() {
							if x >= w {
								break;
							}
							if let Some(cell) = buf.cell_mut((x as u16, bar_y)) {
								cell.set_char(c).set_style(name_style);
							}
							x += 1;
						}
						if dirty {
							if x < w {
								if let Some(cell) = buf.cell_mut((x as u16, bar_y)) {
									cell.set_char(' ').set_style(bar_bg);
								}
								x += 1;
							}
							if x < w {
								if let Some(cell) = buf.cell_mut((x as u16, bar_y)) {
									cell.set_char('\u{25CF}')
										.set_style(bar_bg.fg(DIRTY_BULLET_FG));
								}
								x += 1;
							}
						}
						if let Some(branch) = git_branch {
							paint_git_branch(
								buf,
								&mut x,
								w,
								bar_y,
								branch,
								bar_bg,
								ui_palette.accent,
							);
						}
						let busy = current_viewport.as_ref().is_some_and(|view| view.busy);
						if busy {
							paint_busy_spinner(
								buf,
								&mut x,
								w,
								bar_y,
								busy_spinner_frame,
								bar_bg,
								ui_palette.accent,
							);
						}
						let dma_pending = current_viewport
							.as_ref()
							.map(|v| v.dma_pending)
							.unwrap_or(false);
						if dma_pending {
							let dma_style = bar_bg.fg(STATUS_BAR_INFO);
							for c in " \u{276E}\u{276E}\u{276E}DMA\u{276F}\u{276F}\u{276F}".chars()
							{
								if x >= w {
									break;
								}
								if let Some(cell) = buf.cell_mut((x as u16, bar_y)) {
									cell.set_char(c).set_style(dma_style);
								}
								x += 1;
							}
						}
					}

					let (cursor_line, cursor_col) = current_viewport
						.as_ref()
						.map(|v| {
							(
								v.cursor_abs_pos.line.get() + 1,
								v.cursor_abs_pos.col.get() + 1,
							)
						})
						.unwrap_or((1, 1));

					let search_info = current_viewport
						.as_ref()
						.and_then(|v| v.search_match_info.as_deref());
					let view_language = current_viewport
						.as_ref()
						.map(|v| v.language_label)
						.unwrap_or("Plaintext");
					let right_text = status_right_text(
						search_info,
						current_viewport
							.as_ref()
							.map(|v| v.wrap_enabled)
							.unwrap_or(true),
						file_sz,
						current_viewport.as_ref().and_then(|v| v.line_count),
						cursor_line,
						cursor_col,
						current_viewport
							.as_ref()
							.map(|v| v.cursor_count)
							.unwrap_or(1),
						view_language,
					);

					let right_start = w.saturating_sub(right_text.len());
					if right_start > x {
						let dim_style = bar_bg.fg(SEARCH_DIM_FG);
						let search_style = bar_bg.fg(SEARCH_INFO_FG);
						for (i, c) in right_text.chars().enumerate() {
							let rx = right_start + i;
							if rx >= w {
								break;
							}
							let style = if c == '|' {
								dim_style
							} else if search_info.is_some()
								&& rx < right_start + search_info.unwrap().len()
							{
								search_style
							} else {
								bar_bg
							};
							if let Some(cell) = buf.cell_mut((rx as u16, bar_y)) {
								cell.set_char(c).set_style(style);
							}
						}
					}
				}

				f.set_cursor_position(cursor_to_set.unwrap_or((0, 0)));
			})
			.map(|_| ());

		let show_result = {
			let backend = self.terminal.backend_mut();
			match backend.show_cursor() {
				Ok(()) => Backend::flush(backend),
				Err(err) => Err(err),
			}
		};

		match (draw_result, show_result) {
			(Err(err), _) => Err(err),
			(Ok(_), Err(err)) => Err(err),
			(Ok(_), Ok(())) => Ok(()),
		}
	}
}

fn format_file_size(bytes: u64) -> String {
	const KB: u64 = 1024;
	const MB: u64 = 1024 * KB;
	const GB: u64 = 1024 * MB;
	if bytes >= GB {
		format!("{:.1} GB", bytes as f64 / GB as f64)
	} else if bytes >= MB {
		format!("{:.1} MB", bytes as f64 / MB as f64)
	} else if bytes >= KB {
		format!("{:.1} KB", bytes as f64 / KB as f64)
	} else {
		format!("{} B", bytes)
	}
}

fn format_line_count(lines: u64) -> String {
	let noun = if lines == 1 { "line" } else { "lines" };
	const MILLION: u64 = 1_000_000;
	const BILLION: u64 = 1_000 * MILLION;
	const TRILLION: u64 = 1_000 * BILLION;
	let count = if lines >= TRILLION {
		format!("{:.1}T", lines as f64 / TRILLION as f64)
	} else if lines >= BILLION {
		format!("{:.1}B", lines as f64 / BILLION as f64)
	} else if lines >= MILLION {
		format!("{:.1}M", lines as f64 / MILLION as f64)
	} else {
		lines.to_string()
	};
	format!("{count} {noun}")
}

#[cfg(test)]
mod tests {
	use super::{
		BUSY_SPINNER_FRAMES, apply_diagnostic_style, braille_spinner_frame,
		cursor_gutter_line_number_style, draw_tab_bar, effective_editor_background,
		folded_placeholder_style, format_line_count, gutter_fill_style, gutter_line_number_style,
		paint_busy_spinner, paint_git_branch, paint_secondary_carets, paint_text_segment,
		prompt_window, should_render_preview_snapshot_fallback, status_right_text,
	};
	use crate::ui::{DIAGNOSTIC_ERROR_UNDERLINE, FOLDED_PLACEHOLDER_BG, ratatui_color};
	use libaryon::core::{
		CursorPosition, DocByte, DocDisplayCol, DocLine, PaneLocalCol, PaneLocalRow,
	};
	use libaryon::engine::{UiPalette, VisibleRow};
	use libaryon::svp::diagnostic::DiagnosticSeverity;
	use libaryon::uast::projection::{DocTabSummary, ProjectedCaret};
	use ratatui::{
		buffer::Buffer,
		layout::Rect,
		style::{Modifier, Style},
	};

	#[test]
	fn text_painting_uses_layout_word_boundaries() {
		let mut buffer = Buffer::empty(Rect::new(0, 0, 5, 2));
		let mut x = 0usize;
		let mut y = 0usize;
		let mut col = 0u32;
		let mut next_row = 0usize;
		let rows = [
			VisibleRow {
				line: DocLine::ZERO,
				start_col: DocDisplayCol::ZERO,
			},
			VisibleRow {
				line: DocLine::ZERO,
				start_col: DocDisplayCol::new(6),
			},
		];

		assert!(paint_text_segment(
			&mut buffer,
			&mut x,
			&mut y,
			0,
			&mut col,
			&mut next_row,
			&rows,
			0,
			5,
			2,
			0,
			0,
			"hello world",
			Style::default(),
			true,
		));

		let first: String = (0..5)
			.map(|x| buffer[(x, 0)].symbol().chars().next().unwrap())
			.collect();
		let second: String = (0..5)
			.map(|x| buffer[(x, 1)].symbol().chars().next().unwrap())
			.collect();
		assert_eq!(first, "hello");
		assert_eq!(second, "world");
	}

	#[test]
	fn error_diagnostics_apply_a_red_underline_overlay() {
		let style = apply_diagnostic_style(Style::default(), Some(DiagnosticSeverity::Error));
		assert!(style.add_modifier.contains(Modifier::UNDERLINED));
		assert_eq!(style.underline_color, Some(DIAGNOSTIC_ERROR_UNDERLINE));
	}

	#[test]
	fn folded_placeholder_uses_dedicated_palette() {
		let palette = UiPalette::default();
		let style = folded_placeholder_style(palette.accent);
		assert_eq!(style.fg, Some(ratatui_color(palette.accent)));
		assert_eq!(style.bg, Some(FOLDED_PLACEHOLDER_BG));
		assert!(style.add_modifier.contains(Modifier::BOLD));
	}

	#[test]
	fn cursor_gutter_line_numbers_use_dedicated_palette() {
		let palette = UiPalette::default();
		let style = cursor_gutter_line_number_style(palette);
		assert_eq!(style.fg, Some(ratatui_color(palette.gutter_accent)));
		assert_eq!(style.bg, Some(ratatui_color(palette.gutter_surface)));
		assert!(style.add_modifier.contains(Modifier::BOLD));
	}

	#[test]
	fn pane_local_background_overrides_the_session_palette() {
		let palette = UiPalette::default();
		let local = libaryon::core::Rgb::new(1, 2, 3);
		assert_eq!(
			effective_editor_background(None, palette),
			palette.editor_background
		);
		assert_eq!(effective_editor_background(Some(local), palette), local);
	}

	#[test]
	fn tab_bar_uses_both_configurable_surfaces_in_focused_and_unfocused_panes() {
		let palette = UiPalette {
			tab_surface: libaryon::core::Rgb::new(1, 2, 3),
			tab_active_surface: libaryon::core::Rgb::new(4, 5, 6),
			..UiPalette::default()
		};
		let tabs = [
			DocTabSummary {
				buffer_id: libaryon::core::BufferId::new(1),
				view_id: libaryon::core::ViewId::new(1),
				title: "a".to_string(),
				is_dirty: false,
				snapshot: None,
			},
			DocTabSummary {
				buffer_id: libaryon::core::BufferId::new(2),
				view_id: libaryon::core::ViewId::new(2),
				title: "b".to_string(),
				is_dirty: false,
				snapshot: None,
			},
		];
		let area = Rect::new(0, 0, 10, 1);

		let mut focused = Buffer::empty(area);
		draw_tab_bar(&mut focused, area, &tabs, 0, true, palette);
		assert_eq!(
			focused[(0, 0)].bg,
			ratatui_color(palette.tab_active_surface)
		);
		assert_eq!(focused[(9, 0)].bg, ratatui_color(palette.tab_surface));

		let mut unfocused = Buffer::empty(area);
		draw_tab_bar(&mut unfocused, area, &tabs, 0, false, palette);
		assert_eq!(unfocused[(0, 0)].bg, ratatui_color(palette.tab_surface));
		assert_eq!(
			unfocused[(9, 0)].bg,
			ratatui_color(palette.tab_active_surface)
		);
	}

	#[test]
	fn gutter_line_number_style_highlights_all_cursor_lines() {
		let palette = UiPalette::default();
		let active_style =
			gutter_line_number_style(12, &[DocLine::new(12), DocLine::new(42)], palette);
		let inactive_style =
			gutter_line_number_style(11, &[DocLine::new(12), DocLine::new(42)], palette);

		assert_eq!(active_style.fg, Some(ratatui_color(palette.gutter_accent)));
		assert_eq!(inactive_style.fg, Some(ratatui_color(palette.gutter_text)));
		assert_eq!(
			inactive_style.bg,
			Some(ratatui_color(palette.gutter_surface))
		);
		assert_eq!(inactive_style, gutter_fill_style(palette));
	}

	#[test]
	fn status_bar_reports_wrap_state_before_file_size() {
		assert_eq!(
			status_right_text(None, true, 1024, Some(27), 4, 8, 1, "Rust"),
			"WRAP | 1.0 KB | 27 lines | Rust | UTF-8 | 4:8 "
		);
		assert_eq!(
			status_right_text(Some("3/9"), false, 1024, Some(1), 4, 8, 1, "Python"),
			"3/9 | NOWRAP | 1.0 KB | 1 line | Python | UTF-8 | 4:8 "
		);
		assert_eq!(
			status_right_text(None, true, 1024, None, 4, 8, 3, "Rust"),
			"3 cursors | WRAP | 1.0 KB | counting lines | Rust | UTF-8 | 4:8 "
		);
		assert_eq!(format_line_count(12_345_678_901), "12.3B lines");
	}

	#[test]
	fn busy_status_uses_a_looping_braille_spinner() {
		let first_cycle: String = (0..10).flat_map(braille_spinner_frame).collect();
		assert_eq!(first_cycle, "⠸⠀⠘⠁⠈⠉⠀⠙⠀⠸⠀⢰⠀⣠⢀⣀⢠⡀⢰⠀");
		assert_eq!(braille_spinner_frame(10), braille_spinner_frame(0));
		for frame in BUSY_SPINNER_FRAMES {
			let active_dots: u32 = frame
				.into_iter()
				.map(|glyph| (u32::from(glyph) - 0x2800).count_ones())
				.sum();
			assert_eq!(active_dots, 3);
		}
	}

	#[test]
	fn busy_spinner_uses_the_palette_accent() {
		let mut buffer = Buffer::empty(Rect::new(0, 0, 12, 1));
		let mut x = 5;
		let accent = UiPalette::default().accent;
		paint_busy_spinner(&mut buffer, &mut x, 12, 0, 0, Style::default(), accent);

		assert_eq!(buffer[(5, 0)].symbol(), " ");
		assert_eq!(buffer[(6, 0)].symbol(), "⠸");
		assert_eq!(buffer[(7, 0)].symbol(), "⠀");
		assert_eq!(buffer[(6, 0)].fg, ratatui_color(accent));
		assert_eq!(buffer[(7, 0)].fg, ratatui_color(accent));
		assert_eq!(x, 8);
	}

	#[test]
	fn git_branch_is_painted_beside_the_file_name_in_the_accent_color() {
		let mut buffer = Buffer::empty(Rect::new(0, 0, 12, 1));
		let mut x = 2;
		let accent = UiPalette::default().accent;
		paint_git_branch(&mut buffer, &mut x, 12, 0, "main", Style::default(), accent);

		let painted: String = (2..9).map(|x| buffer[(x, 0)].symbol()).collect();
		assert_eq!(painted, " | main");
		assert_eq!(buffer[(8, 0)].fg, ratatui_color(accent));
		assert_eq!(x, 9);
	}

	#[test]
	fn prompt_window_keeps_prompt_cursor_visible() {
		assert_eq!(
			prompt_window("edit very/long/path.rs", 0, 8),
			("edit ver".into(), 0)
		);
		assert_eq!(
			prompt_window("edit very/long/path.rs", "edit very/long/path.rs".len(), 8),
			("path.rs".into(), 7),
		);
		assert_eq!(prompt_window("e café.rs", 6, 6), ("e café".into(), 5));
	}

	#[test]
	fn secondary_caret_overlay_reverses_the_projected_cell() {
		let mut buffer = Buffer::empty(Rect::new(0, 0, 12, 5));
		paint_secondary_carets(
			&mut buffer,
			&[ProjectedCaret {
				pane_row: PaneLocalRow::new(1),
				pane_col: PaneLocalCol::new(2),
				abs_pos: CursorPosition::new(DocLine::new(8), DocDisplayCol::new(2)),
				abs_byte: DocByte::new(20),
			}],
			1,
			2,
			10,
			1,
			3,
		);
		assert!(buffer[(5, 2)].modifier.contains(Modifier::REVERSED));
	}

	#[test]
	fn preview_snapshot_fallback_waits_for_async_preview() {
		assert!(!should_render_preview_snapshot_fallback(false, true));
		assert!(should_render_preview_snapshot_fallback(false, false));
		assert!(!should_render_preview_snapshot_fallback(true, true));
	}
}
