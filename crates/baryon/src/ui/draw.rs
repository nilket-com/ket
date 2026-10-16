use super::Frontend;
use super::diff::{DiffHoverEmphasis, HoveredDiffBand};
use super::markdown_preview::render_markdown_preview_fallback;
use super::minimap::render_minimap_snapshot;
use super::mouse::TAB_CONTROL_LABEL;
use crate::ui::*;
use libaryon::core::{DOCTAB_BAR_ROWS, PaneLocalRow, PaneRect, Rgb, STATUS_BAR_ROWS, TAB_SIZE};
use libaryon::engine::{
	EditorMode, InlineAnnotationKind, MinimapSnapshot, UiPalette, VisualKind,
	viewport_geometry_for_viewport_with_numbering,
};
use libaryon::svp::diagnostic::DiagnosticSeverity;
use libaryon::svp::projector::{DiagnosticProjector, HighlightProjector};
use libaryon::svp::rust_highlight::default_text_color;
use libaryon::uast::projection::{DocTabSummary, PaneViewport, PickerViewport, ProjectedCaret};
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

fn diff_block_fill(base: Rgb, accent: Rgb) -> Rgb {
	let glass = blend_rgb(base, accent, libaryon::core::BlendRatio::new(2, 5));
	cap_dark_glass_luma(base, glass, 46)
}

fn diff_intra_line_fill(base: Rgb, accent: Rgb) -> Rgb {
	let glass = blend_rgb(base, accent, libaryon::core::BlendRatio::new(3, 5));
	cap_dark_glass_luma(base, glass, 66)
}

fn focused_diff_block_fill(base: Rgb, accent: Rgb, emphasis: DiffHoverEmphasis) -> Rgb {
	match emphasis {
		DiffHoverEmphasis::Normal | DiffHoverEmphasis::Selected => diff_block_fill(base, accent),
		DiffHoverEmphasis::Dimmed => blend_rgb(base, accent, libaryon::core::BlendRatio::new(1, 8)),
	}
}

fn focused_diff_intra_line_fill(base: Rgb, accent: Rgb, emphasis: DiffHoverEmphasis) -> Rgb {
	match emphasis {
		DiffHoverEmphasis::Normal | DiffHoverEmphasis::Selected => {
			diff_intra_line_fill(base, accent)
		}
		DiffHoverEmphasis::Dimmed => blend_rgb(base, accent, libaryon::core::BlendRatio::new(1, 5)),
	}
}

fn cap_dark_glass_luma(base: Rgb, glass: Rgb, lift: u32) -> Rgb {
	let luma = |color: Rgb| {
		(u32::from(color.r) * 299 + u32::from(color.g) * 587 + u32::from(color.b) * 114) / 1_000
	};
	let base_luma = luma(base);
	if base_luma >= 128 {
		return glass;
	}
	let glass_luma = luma(glass);
	let cap = base_luma.saturating_add(lift).min(255);
	if glass_luma <= cap || glass_luma == 0 {
		return glass;
	}
	let scale = |channel: u8| {
		u8::try_from(u32::from(channel) * cap / glass_luma)
			.expect("luminance cap never increases a channel")
	};
	Rgb::new(scale(glass.r), scale(glass.g), scale(glass.b))
}

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

fn row_window_starts_in_trailing_whitespace(pane: &PaneViewport, line: u64) -> bool {
	pane.row_window
		.is_some_and(|window| window.line.get() == line)
		&& pane
			.visible_rows
			.first()
			.is_some_and(|row| row.line.get() == line && row.start_byte.is_none())
}

fn paint_text_segment(
	buf: &mut Buffer,
	x: &mut usize,
	visual_y: &mut usize,
	current_doc_line: u64,
	current_display_col: &mut u64,
	next_render_row: &mut usize,
	render_rows: &[libaryon::engine::VisibleRow],
	row_window: Option<libaryon::engine::RowWindow>,
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

	for cell in libaryon::core::text::display_cells(text.as_bytes()) {
		let grapheme = &text[cell.byte_start..cell.byte_end];
		let cell_width = cell.width.max(1) as usize;
		if wrap_enabled {
			while let Some(row) = render_rows.get(*next_render_row) {
				let row_start = row
					.start_offset
					.map_or(u64::from(row.start_col.get()), |offset| offset.get());
				if row.line.get() < current_doc_line
					|| (row.line.get() == current_doc_line && row_start < *current_display_col)
				{
					*next_render_row += 1;
					continue;
				}
				if row.line.get() == current_doc_line && row_start == *current_display_col {
					let first_window_row = *next_render_row == 0
						&& row_window.is_some_and(|window| window.line.get() == current_doc_line);
					if row_start != 0 && !first_window_row {
						*visual_y += 1;
						*x = text_left;
					}
					*next_render_row += 1;
				}
				break;
			}
			// The engine may start a trailing-whitespace window at the run,
			// before the first visible row. Consume that prefix to its absolute
			// row start without drawing it above the viewport.
			if *next_render_row == 0
				&& row_window.is_some_and(|window| window.line.get() == current_doc_line)
				&& render_rows.first().is_some_and(|row| {
					row.line.get() == current_doc_line
						&& row
							.start_offset
							.is_some_and(|start| start.get() > *current_display_col)
				}) {
				*current_display_col = current_display_col.saturating_add(cell_width as u64);
				continue;
			}
		}
		if *visual_y >= skip_rows.saturating_add(render_height) {
			return false;
		}
		if x.saturating_add(cell_width) > text_right {
			if wrap_enabled && render_rows.get(*next_render_row).is_none() {
				return false;
			}
			*x = x.saturating_add(cell_width);
			*current_display_col = current_display_col.saturating_add(cell_width as u64);
			continue;
		}
		if *visual_y >= skip_rows {
			let screen_y = screen_y_offset + (*visual_y - skip_rows);
			if screen_y >= screen_y_offset.saturating_add(render_height) {
				return false;
			}
			// The terminal can resize between receiving an engine viewport and
			// drawing it. The viewport's pane rectangle then briefly describes the
			// old terminal, so clip the one direct Buffer write to the current
			// buffer rather than indexing just past its bottom or right edge.
			let area = buf.area;
			if let (Ok(x), Ok(y)) = (u16::try_from(*x), u16::try_from(screen_y))
				&& x >= area.x
				&& x < area.right()
				&& y >= area.y
				&& y < area.bottom()
			{
				let width = cell_width.min(usize::from(area.right() - x));
				buf.set_stringn(x, y, grapheme, width, style);
			}
		}
		*x = x.saturating_add(cell_width);
		*current_display_col = current_display_col.saturating_add(cell_width as u64);
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

fn status_file_name(file_path: Option<&str>) -> &str {
	file_path
		.map(|path| {
			std::path::Path::new(path)
				.file_name()
				.and_then(|name| name.to_str())
				.unwrap_or(path)
		})
		.unwrap_or("_")
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

fn paint_picker_text(buf: &mut Buffer, x: u16, y: u16, width: u16, text: &str, style: Style) {
	for (offset, ch) in text.chars().take(width as usize).enumerate() {
		if let Some(cell) = buf.cell_mut((x.saturating_add(offset as u16), y)) {
			cell.set_char(ch).set_style(style);
		}
	}
}

fn draw_picker_overlay(
	buf: &mut Buffer,
	screen: Rect,
	picker: &PickerViewport,
	palette: UiPalette,
) -> Option<(u16, u16)> {
	if screen.width < 12 || screen.height < 5 {
		return None;
	}
	let width = screen.width.saturating_sub(4).clamp(12, 112);
	let requested_height = u16::try_from(picker.results.len())
		.unwrap_or(u16::MAX)
		.saturating_add(4);
	let height = requested_height.min(screen.height).max(5);
	let x = screen
		.x
		.saturating_add(screen.width.saturating_sub(width) / 2);
	let y = screen
		.y
		.saturating_add(screen.height.saturating_sub(height) / 3);
	let right = x.saturating_add(width).saturating_sub(1);
	let bottom = y.saturating_add(height).saturating_sub(1);
	let surface = Style::default()
		.bg(ratatui_color(palette.tab_surface))
		.fg(ratatui_color(palette.tab_text));
	let border = surface.fg(ratatui_color(palette.tab_accent));
	let selected = Style::default()
		.bg(ratatui_color(palette.tab_active_surface))
		.fg(ratatui_color(palette.tab_accent))
		.add_modifier(Modifier::BOLD);

	for row in y..=bottom {
		for col in x..=right {
			if let Some(cell) = buf.cell_mut((col, row)) {
				cell.set_char(' ').set_style(surface);
			}
		}
	}
	if let Some(cell) = buf.cell_mut((x, y)) {
		cell.set_char('╭').set_style(border);
	}
	if let Some(cell) = buf.cell_mut((right, y)) {
		cell.set_char('╮').set_style(border);
	}
	if let Some(cell) = buf.cell_mut((x, bottom)) {
		cell.set_char('╰').set_style(border);
	}
	if let Some(cell) = buf.cell_mut((right, bottom)) {
		cell.set_char('╯').set_style(border);
	}
	for col in x.saturating_add(1)..right {
		if let Some(top) = buf.cell_mut((col, y)) {
			top.set_char('─').set_style(border);
		}
		if let Some(bottom_cell) = buf.cell_mut((col, bottom)) {
			bottom_cell.set_char('─').set_style(border);
		}
	}
	for row in y.saturating_add(1)..bottom {
		if let Some(left) = buf.cell_mut((x, row)) {
			left.set_char('│').set_style(border);
		}
		if let Some(right_cell) = buf.cell_mut((right, row)) {
			right_cell.set_char('│').set_style(border);
		}
	}
	paint_picker_text(
		buf,
		x.saturating_add(2),
		y,
		width.saturating_sub(4),
		&format!(" {} repo search ", picker.mode.label()),
		border.add_modifier(Modifier::BOLD),
	);

	let inner_x = x.saturating_add(2);
	let inner_width = width.saturating_sub(4);
	let query_width = inner_width.saturating_sub(2) as usize;
	let (visible_query, query_cursor_col) =
		prompt_window(&picker.query, picker.query_cursor, query_width);
	paint_picker_text(buf, inner_x, y.saturating_add(1), 2, "> ", border);
	paint_picker_text(
		buf,
		inner_x.saturating_add(2),
		y.saturating_add(1),
		query_width as u16,
		&visible_query,
		surface,
	);

	let result_rows = height.saturating_sub(4) as usize;
	if let Some(error) = picker.error.as_deref() {
		paint_picker_text(
			buf,
			inner_x,
			y.saturating_add(2),
			inner_width,
			error,
			surface
				.fg(ratatui_color(palette.diff_removed))
				.add_modifier(Modifier::BOLD),
		);
	}
	for (local_index, result) in picker.results.iter().take(result_rows).enumerate() {
		let absolute_index = picker.first_result.saturating_add(local_index);
		let style = if absolute_index == picker.selected {
			selected
		} else {
			surface
		};
		let row = y.saturating_add(2).saturating_add(local_index as u16);
		for col in inner_x..inner_x.saturating_add(inner_width) {
			if let Some(cell) = buf.cell_mut((col, row)) {
				cell.set_char(' ').set_style(style);
			}
		}
		let label = format!(
			"{}:{}:{}  {}",
			result.path,
			result.line.get().saturating_add(1),
			result.col.get().saturating_add(1),
			result.line_text
		);
		paint_picker_text(buf, inner_x, row, inner_width, &label, style);
	}

	let qualifier = if picker.truncated { "≥" } else { "" };
	let activity = if picker.searching {
		" · searching…"
	} else {
		""
	};
	let footer = format!(
		" {}/{}{qualifier} · {} files{} · ↑↓ select · Enter open · Esc close ",
		picker.shown, picker.matched, picker.files_walked, activity
	);
	paint_picker_text(
		buf,
		x.saturating_add(1),
		bottom,
		width.saturating_sub(2),
		&footer,
		border,
	);

	Some((
		inner_x
			.saturating_add(2)
			.saturating_add(query_cursor_col as u16)
			.min(right.saturating_sub(1)),
		y.saturating_add(1),
	))
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
		label.push_str(TAB_CONTROL_LABEL);
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
	diff_hover: Option<HoveredDiffBand>,
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

	let effective_bg_rgb = effective_editor_background(pane.background_override, palette);
	let effective_bg = ratatui_color(effective_bg_rgb);
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
			let hover_emphasis = diff_hover.map_or(DiffHoverEmphasis::Normal, |hover| {
				hover.emphasis_for(pane.pane_id, diff_pair.partner_pane_id, block)
			});
			let accent = match block.kind {
				libaryon::uast::projection::DiffKind::Added => palette.diff_added,
				libaryon::uast::projection::DiffKind::Removed => palette.diff_removed,
				libaryon::uast::projection::DiffKind::Modified => palette.diff_modified,
				libaryon::uast::projection::DiffKind::Moved => palette.diff_moved,
			};
			let color = ratatui_color(focused_diff_block_fill(
				effective_bg_rgb,
				accent,
				hover_emphasis,
			));

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

			// Layer 3 overlay: keep the saturated glass block background while
			// intensifying the specific token columns that actually changed.
			if let Some(intra_line_diffs) = block.intra_line_diffs.as_ref() {
				for intra in intra_line_diffs {
					let intense = match intra.kind {
						libaryon::uast::projection::IntraLineDiffKind::Added => palette.diff_added,
						libaryon::uast::projection::IntraLineDiffKind::Removed => {
							palette.diff_removed
						}
						libaryon::uast::projection::IntraLineDiffKind::Modified => {
							palette.diff_modified
						}
					};
					let intense = ratatui_color(focused_diff_intra_line_fill(
						effective_bg_rgb,
						intense,
						hover_emphasis,
					));

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
	let mut current_display_col = pane
		.row_window
		.filter(|window| window.line.get() == current_doc_line)
		.map_or(0, |window| window.start_offset.get());
	let mut next_render_row = 0usize;
	let mut line_has_content = row_window_starts_in_trailing_whitespace(pane, current_doc_line);
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
			line_has_content = row_window_starts_in_trailing_whitespace(pane, current_doc_line);
			current_display_col = pane
				.row_window
				.filter(|window| window.line.get() == current_doc_line)
				.map_or(0, |window| window.start_offset.get());
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
				pane.row_window,
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
			continue;
		}
		// Each token carries its own absolute start: tokens need not be
		// byte-contiguous, since an unwrapped long line is clipped at the pane
		// edge and resumes at its newline.
		let mut current_global_byte = token.absolute_start_byte;

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
					let grapheme_len =
						libaryon::core::text::display_cell_span_for_byte_offset(remaining, 0)
							.map_or(ch_len, |(_, end)| end);
					(3u8, grapheme_len)
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
							pane.row_window,
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
							pane.row_window,
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
								pane.row_window,
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
								pane.row_window,
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
							pane.row_window,
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
							pane.row_window,
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
						pane.row_window,
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
					let display = std::str::from_utf8(&remaining[..chunk_len])
						.expect("printable grapheme must decode");
					if !paint_text_segment(
						buf,
						&mut x,
						&mut visual_y,
						current_doc_line,
						&mut current_display_col,
						&mut next_render_row,
						&pane.render_rows,
						pane.row_window,
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
					byte_idx += chunk_len;
					if has_physical_bytes {
						current_global_byte = current_global_byte.saturating_add(chunk_len as u64);
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
		let diff_hover = self.diff_bridge.hovered_band();
		let diff_bridge = &mut self.diff_bridge;
		let pane_resize_active = self.pane_resize_drag.is_some();
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
								diff_hover,
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
											.min(u16::MAX as u32) as u16,
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
										if !super::diff::should_render_diff_bridge(
											&diff_pair.blocks,
											pane_resize_active,
										) {
											diff_bridge.clear_bridge(pane.pane_id, partner.pane_id);
											continue;
										}

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
												ui_palette,
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
					if let Some(picker) = view.picker.as_ref() {
						let overlay_area = Rect {
							x: 0,
							y: 0,
							width: max_width,
							height: editor_area_height,
						};
						cursor_to_set = draw_picker_overlay(
							f.buffer_mut(),
							overlay_area,
							picker,
							view.ui_palette,
						);
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
						let display_name = status_file_name(file_name);
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
		cursor_gutter_line_number_style, diff_block_fill, diff_intra_line_fill,
		draw_picker_overlay, draw_tab_bar, effective_editor_background, focused_diff_block_fill,
		folded_placeholder_style, format_line_count, gutter_fill_style, gutter_line_number_style,
		paint_busy_spinner, paint_git_branch, paint_secondary_carets, paint_text_segment,
		prompt_window, should_render_preview_snapshot_fallback, status_file_name,
		status_right_text,
	};
	use crate::ui::{
		DIAGNOSTIC_ERROR_UNDERLINE, FOLDED_PLACEHOLDER_BG, blend_rgb, diff::DiffHoverEmphasis,
		ratatui_color,
	};
	use libaryon::core::{
		CursorPosition, DocByte, DocDisplayCol, DocLine, PaneLocalCol, PaneLocalRow,
	};
	use libaryon::engine::{UiPalette, VisibleRow};
	use libaryon::svp::diagnostic::DiagnosticSeverity;
	use libaryon::uast::projection::{
		DocTabSummary, PickerResultViewport, PickerViewport, ProjectedCaret,
	};
	use ratatui::{
		buffer::Buffer,
		layout::Rect,
		style::{Modifier, Style},
	};

	#[test]
	fn text_paint_clips_a_stale_pane_below_a_resized_terminal() {
		let mut buffer = Buffer::empty(Rect::new(0, 0, 180, 54));
		let mut x = 24;
		let mut visual_y = 0;
		let mut display_col = 0;
		let mut next_render_row = 0;

		assert!(paint_text_segment(
			&mut buffer,
			&mut x,
			&mut visual_y,
			0,
			&mut display_col,
			&mut next_render_row,
			&[],
			None,
			0,
			180,
			1,
			0,
			54,
			"x",
			Style::default(),
			false,
		));
		assert_eq!(buffer[(24, 53)].symbol(), " ");
	}

	#[test]
	fn picker_overlay_marks_the_selected_row_and_places_the_query_cursor() {
		let palette = UiPalette::default();
		let mut buffer = Buffer::empty(Rect::new(0, 0, 80, 20));
		let picker = PickerViewport {
			mode: libaryon::engine::RepoSearchMode::Fuzzy,
			query: "render_frame".to_string(),
			query_cursor: "render".len(),
			selected: 1,
			first_result: 0,
			results: vec![
				PickerResultViewport {
					path: "src/a.rs".to_string(),
					line: DocLine::new(3),
					col: DocDisplayCol::new(2),
					line_text: "render_frame();".to_string(),
					score: 3_000_000,
				},
				PickerResultViewport {
					path: "src/b.rs".to_string(),
					line: DocLine::new(7),
					col: DocDisplayCol::new(4),
					line_text: "fn render_frame()".to_string(),
					score: 2_999_996,
				},
			],
			shown: 2,
			matched: 2,
			truncated: false,
			searching: false,
			files_walked: 9,
			error: None,
		};
		let cursor = draw_picker_overlay(&mut buffer, Rect::new(0, 0, 80, 19), &picker, palette)
			.expect("overlay");
		assert_eq!(buffer[cursor].symbol(), "_");
		assert_eq!(buffer[(cursor.0.saturating_sub(1), cursor.1)].symbol(), "r");
		let selected_row_y = cursor.1.saturating_add(2);
		assert_eq!(
			buffer[(4, selected_row_y)].bg,
			ratatui_color(palette.tab_active_surface)
		);
	}

	#[test]
	fn regex_picker_labels_its_mode_and_renders_compile_errors_inside_the_modal() {
		let palette = UiPalette::default();
		let mut buffer = Buffer::empty(Rect::new(0, 0, 80, 20));
		let picker = PickerViewport {
			mode: libaryon::engine::RepoSearchMode::Regex,
			query: "[".to_string(),
			query_cursor: 1,
			selected: 0,
			first_result: 0,
			results: Vec::new(),
			shown: 0,
			matched: 0,
			truncated: false,
			searching: false,
			files_walked: 0,
			error: Some("Invalid regular expression: unclosed character class".to_string()),
		};
		draw_picker_overlay(&mut buffer, Rect::new(0, 0, 80, 19), &picker, palette)
			.expect("overlay");
		let screen = (0..20)
			.map(|y| (0..80).map(|x| buffer[(x, y)].symbol()).collect::<String>())
			.collect::<Vec<_>>()
			.join("\n");
		assert!(screen.contains("regex repo search"));
		assert!(screen.contains("Invalid regular expression"));
	}

	#[test]
	fn dark_diff_glass_stays_saturated_while_capping_perceptual_brightness() {
		let palette = UiPalette::default();
		assert_eq!(
			diff_block_fill(palette.editor_background, palette.diff_added),
			libaryon::core::Rgb::new(37, 80, 51)
		);
		assert_eq!(
			diff_block_fill(palette.editor_background, palette.diff_removed),
			libaryon::core::Rgb::new(104, 46, 46),
			"the already-dark red glass should remain unchanged"
		);
		assert_eq!(
			diff_block_fill(palette.editor_background, palette.diff_modified),
			libaryon::core::Rgb::new(45, 67, 95)
		);
		assert_eq!(
			diff_intra_line_fill(palette.editor_background, palette.diff_moved),
			libaryon::core::Rgb::new(102, 67, 125)
		);
		assert_eq!(
			diff_block_fill(libaryon::core::Rgb::new(230, 230, 230), palette.diff_added),
			libaryon::core::Rgb::new(170, 218, 186),
			"the black-glass luminance cap is dark-theme-specific"
		);
	}

	#[test]
	fn hover_focus_preserves_the_selected_glass_and_dims_unrelated_blocks() {
		let palette = UiPalette::default();
		let selected = focused_diff_block_fill(
			palette.editor_background,
			palette.diff_added,
			DiffHoverEmphasis::Selected,
		);
		let dimmed = focused_diff_block_fill(
			palette.editor_background,
			palette.diff_added,
			DiffHoverEmphasis::Dimmed,
		);
		assert_eq!(
			selected,
			diff_block_fill(palette.editor_background, palette.diff_added)
		);
		assert_eq!(
			dimmed,
			blend_rgb(
				palette.editor_background,
				palette.diff_added,
				libaryon::core::BlendRatio::new(1, 8),
			)
		);
		assert_ne!(selected, dimmed);
	}

	#[test]
	fn text_painting_uses_layout_word_boundaries() {
		let mut buffer = Buffer::empty(Rect::new(0, 0, 5, 2));
		let mut x = 0usize;
		let mut y = 0usize;
		let mut col = 0u64;
		let mut next_row = 0usize;
		let rows = [
			VisibleRow::whole(DocLine::ZERO, DocDisplayCol::ZERO),
			VisibleRow::whole(DocLine::ZERO, DocDisplayCol::new(6)),
		];

		assert!(paint_text_segment(
			&mut buffer,
			&mut x,
			&mut y,
			0,
			&mut col,
			&mut next_row,
			&rows,
			None,
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
	fn text_painting_starts_a_row_window_at_its_absolute_origin() {
		let mut buffer = Buffer::empty(Rect::new(0, 0, 4, 2));
		let mut x = 0usize;
		let mut y = 0usize;
		let mut col = 100u64;
		let mut next_row = 0usize;
		let rows = [
			VisibleRow {
				line: DocLine::ZERO,
				start_col: DocDisplayCol::new(102),
				start_byte: Some(DocByte::new(102)),
				start_offset: Some(libaryon::core::DisplayOffset::new(102)),
			},
			VisibleRow {
				line: DocLine::ZERO,
				start_col: DocDisplayCol::new(106),
				start_byte: Some(DocByte::new(106)),
				start_offset: Some(libaryon::core::DisplayOffset::new(106)),
			},
		];
		let window = libaryon::engine::RowWindow {
			line: DocLine::ZERO,
			start_byte: DocByte::new(100),
			start_offset: libaryon::core::DisplayOffset::new(100),
			tab_phase: 0,
		};

		assert!(paint_text_segment(
			&mut buffer,
			&mut x,
			&mut y,
			0,
			&mut col,
			&mut next_row,
			&rows,
			Some(window),
			0,
			4,
			2,
			0,
			0,
			"  abcdef",
			Style::default(),
			true,
		));
		let first: String = (0..4).map(|x| buffer[(x, 0)].symbol().to_owned()).collect();
		let second: String = (0..4).map(|x| buffer[(x, 1)].symbol().to_owned()).collect();
		assert_eq!(first, "abcd");
		assert_eq!(second, "ef  ");
	}

	#[test]
	fn text_painting_keeps_emoji_sequences_in_one_wide_cell() {
		let mut buffer = Buffer::empty(Rect::new(0, 0, 4, 1));
		let mut x = 0usize;
		let mut y = 0usize;
		let mut col = 0u64;
		let mut next_row = 0usize;

		assert!(paint_text_segment(
			&mut buffer,
			&mut x,
			&mut y,
			0,
			&mut col,
			&mut next_row,
			&[],
			None,
			0,
			4,
			1,
			0,
			0,
			"☠️",
			Style::default(),
			false,
		));

		assert_eq!(buffer[(0, 0)].symbol(), "☠️");
		assert_eq!(x, 2);
		assert_eq!(col, 2);
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
				source_path: None,
				buffer_id: libaryon::core::BufferId::new(1),
				document_id: None,
				view_id: libaryon::core::ViewId::new(1),
				title: "a".to_string(),
				is_dirty: false,
				snapshot: None,
			},
			DocTabSummary {
				source_path: None,
				buffer_id: libaryon::core::BufferId::new(2),
				document_id: None,
				view_id: libaryon::core::ViewId::new(2),
				title: "b".to_string(),
				is_dirty: false,
				snapshot: None,
			},
		];
		let area = Rect::new(0, 0, 40, 1);

		let mut focused = Buffer::empty(area);
		draw_tab_bar(&mut focused, area, &tabs, 0, true, palette);
		assert_eq!(
			focused[(0, 0)].bg,
			ratatui_color(palette.tab_active_surface)
		);
		assert_eq!(focused[(15, 0)].bg, ratatui_color(palette.tab_surface));
		for (column, symbol) in [(3, "❮"), (5, "—"), (7, "|"), (9, "+"), (11, "✕"), (13, "❯")]
		{
			assert_eq!(focused[(column, 0)].symbol(), symbol);
		}

		let mut unfocused = Buffer::empty(area);
		draw_tab_bar(&mut unfocused, area, &tabs, 0, false, palette);
		assert_eq!(unfocused[(0, 0)].bg, ratatui_color(palette.tab_surface));
		assert_eq!(
			unfocused[(15, 0)].bg,
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
		assert_eq!(status_file_name(None), "_");
		assert_eq!(status_file_name(Some("/tmp/project/main.rs")), "main.rs");
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
