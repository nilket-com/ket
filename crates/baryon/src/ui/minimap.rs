use image::{DynamicImage, Rgba, RgbaImage};
use libaryon::core::{
	BlendRatio, CellHeight, CellRow, MinimapFingerprint, PixelHeight, PixelWidth, PixelX, PixelY,
	Rgb, TokenBin, ViewId,
};
use libaryon::engine::{
	EMPTY_PREVIEW_BIN, MinimapOverlay, MinimapSnapshot, OverviewSnapshot, PreviewRow,
	PreviewSnapshot, UiPalette, dominant_preview_bin_for_rows, sample_preview_bin_range,
};
use libaryon::svp::rust_highlight::CATEGORY_COUNT;
#[cfg(test)]
use libaryon::svp::rust_highlight::TokenCategory;
use ratatui::{
	Frame,
	buffer::Buffer,
	layout::Rect,
	style::{Color, Style},
};
use ratatui_image::{Image, Resize, picker::Picker, protocol::Protocol};
use std::{
	collections::HashMap,
	collections::hash_map::DefaultHasher,
	hash::{Hash, Hasher},
	sync::mpsc,
	thread,
};

#[derive(Clone)]
struct PreviewRequest {
	view_id: ViewId,
	fingerprint: MinimapFingerprint,
	snapshot: PreviewSnapshot,
	area: Rect,
	theme_colors: [Option<Rgb>; CATEGORY_COUNT],
	ui_palette: UiPalette,
}

struct PreviewResponse {
	view_id: ViewId,
	fingerprint: MinimapFingerprint,
	area: Rect,
	protocol: Option<Protocol>,
}

struct ViewEntry {
	requested_fingerprint: Option<MinimapFingerprint>,
	pending_fingerprint: Option<MinimapFingerprint>,
	current_fingerprint: Option<MinimapFingerprint>,
	current_area: Option<Rect>,
	current_protocol: Option<Protocol>,
}

impl ViewEntry {
	fn new() -> Self {
		Self {
			requested_fingerprint: None,
			pending_fingerprint: None,
			current_fingerprint: None,
			current_area: None,
			current_protocol: None,
		}
	}
}

#[cfg(test)]
fn token_bin(category: TokenCategory) -> TokenBin {
	TokenBin::new(category as u8)
}

pub(super) struct MinimapController {
	tx_request: mpsc::Sender<PreviewRequest>,
	rx_ready: mpsc::Receiver<PreviewResponse>,
	views: HashMap<ViewId, ViewEntry>,
	font_size: (u16, u16),
}

impl MinimapController {
	pub fn new() -> Self {
		let picker = Picker::from_query_stdio().unwrap_or_else(|_| Picker::halfblocks());
		let font_size = picker.font_size();
		let font_size = (font_size.width, font_size.height);
		let (tx_request, rx_request) = mpsc::channel::<PreviewRequest>();
		let (tx_ready, rx_ready) = mpsc::channel::<PreviewResponse>();

		thread::spawn(move || {
			while let Ok(mut request) = rx_request.recv() {
				while let Ok(next_request) = rx_request.try_recv() {
					request = next_request;
				}

				let protocol = render_preview_protocol(
					&picker,
					&request.snapshot,
					request.area,
					&request.theme_colors,
					request.ui_palette,
				);
				if tx_ready
					.send(PreviewResponse {
						view_id: request.view_id,
						fingerprint: request.fingerprint,
						area: request.area,
						protocol,
					})
					.is_err()
				{
					break;
				}
			}
		});

		Self {
			tx_request,
			rx_ready,
			views: HashMap::new(),
			font_size,
		}
	}

	pub fn request_preview(
		&mut self,
		view_id: ViewId,
		preview: &PreviewSnapshot,
		area: Rect,
		theme_colors: [Option<Rgb>; CATEGORY_COUNT],
		ui_palette: UiPalette,
	) {
		if area.width == 0 || area.height == 0 {
			return;
		}

		let fingerprint = preview_request_fingerprint(preview, area, &theme_colors, ui_palette);
		let entry = self.views.entry(view_id).or_insert_with(ViewEntry::new);
		if entry.requested_fingerprint == Some(fingerprint)
			|| entry.pending_fingerprint == Some(fingerprint)
			|| entry.current_fingerprint == Some(fingerprint)
		{
			entry.requested_fingerprint = Some(fingerprint);
			return;
		}

		entry.requested_fingerprint = Some(fingerprint);
		entry.pending_fingerprint = Some(fingerprint);
		let _ = self.tx_request.send(PreviewRequest {
			view_id,
			fingerprint,
			snapshot: preview.clone(),
			area,
			theme_colors,
			ui_palette,
		});
	}

	pub fn poll(&mut self) -> bool {
		let mut updated = false;
		while let Ok(response) = self.rx_ready.try_recv() {
			let Some(entry) = self.views.get_mut(&response.view_id) else {
				continue;
			};
			if entry.requested_fingerprint == Some(response.fingerprint) {
				entry.current_fingerprint = Some(response.fingerprint);
				entry.current_area = Some(response.area);
				entry.current_protocol = response.protocol;
				entry.pending_fingerprint = None;
				updated = true;
			}
		}
		updated
	}

	pub fn render(&self, view_id: ViewId, frame: &mut Frame<'_>, area: Rect) -> bool {
		if area.width == 0 || area.height == 0 {
			return false;
		}
		let Some(entry) = self.views.get(&view_id) else {
			return false;
		};
		if entry.current_area != Some(area) {
			return false;
		}
		let Some(protocol) = &entry.current_protocol else {
			return false;
		};
		frame.render_widget(Image::new(protocol), area);
		true
	}

	pub fn has_pending_preview(&self, view_id: ViewId) -> bool {
		self.views
			.get(&view_id)
			.is_some_and(|entry| entry.pending_fingerprint.is_some())
	}

	pub fn current_preview_content_height(
		&self,
		view_id: ViewId,
		area: Rect,
		total_rows: usize,
	) -> Option<u32> {
		let entry = self.views.get(&view_id)?;
		if entry.current_area != Some(area) || entry.current_protocol.is_none() {
			return None;
		}
		Some(preview_image_content_height_rows(
			total_rows,
			area.height,
			self.font_size.1,
		))
	}

	pub fn retain_views<F: Fn(ViewId) -> bool>(&mut self, keep: F) {
		self.views.retain(|view_id, _| keep(*view_id));
	}
}

fn render_preview_protocol(
	picker: &Picker,
	snapshot: &PreviewSnapshot,
	area: Rect,
	theme_colors: &[Option<Rgb>; CATEGORY_COUNT],
	ui_palette: UiPalette,
) -> Option<Protocol> {
	let font_size = picker.font_size();
	let image = render_preview_image(
		snapshot,
		area,
		theme_colors,
		ui_palette,
		(font_size.width, font_size.height),
	);
	picker
		.new_protocol(image, area.into(), Resize::Fit(None))
		.ok()
}

fn preview_request_fingerprint(
	snapshot: &PreviewSnapshot,
	area: Rect,
	theme_colors: &[Option<Rgb>; CATEGORY_COUNT],
	ui_palette: UiPalette,
) -> MinimapFingerprint {
	let mut hasher = DefaultHasher::new();
	area.x.hash(&mut hasher);
	area.y.hash(&mut hasher);
	area.width.hash(&mut hasher);
	area.height.hash(&mut hasher);
	snapshot.overlay.doc_line_count.hash(&mut hasher);
	snapshot.overlay.viewport_start_line.get().hash(&mut hasher);
	snapshot.overlay.viewport_end_line.get().hash(&mut hasher);
	snapshot.overlay.viewport_line_count.hash(&mut hasher);
	snapshot.overlay.cursor_line.get().hash(&mut hasher);
	snapshot.overlay.search_bands.hash(&mut hasher);
	snapshot.overlay.active_search_band.hash(&mut hasher);
	snapshot.max_columns.hash(&mut hasher);
	snapshot.rows.len().hash(&mut hasher);
	for row in &snapshot.rows {
		row.logical_width.hash(&mut hasher);
		hasher.write(TokenBin::as_bytes(&row.bins));
	}
	for color in theme_colors {
		match color {
			Some(color) => {
				1u8.hash(&mut hasher);
				let (r, g, b) = rgb_components(*color);
				r.hash(&mut hasher);
				g.hash(&mut hasher);
				b.hash(&mut hasher);
			}
			None => 0u8.hash(&mut hasher),
		}
	}
	for color in [
		ui_palette.search_highlight,
		ui_palette.minimap_surface,
		ui_palette.minimap_accent,
	] {
		let (r, g, b) = rgb_components(color);
		r.hash(&mut hasher);
		g.hash(&mut hasher);
		b.hash(&mut hasher);
	}
	MinimapFingerprint::new(hasher.finish())
}

fn preview_sparse_line_px(total_rows: usize) -> PixelHeight {
	PixelHeight::new(if total_rows <= 64 { 2 } else { 1 })
}

fn preview_sparse_height_px(total_rows: usize) -> PixelHeight {
	let rows = total_rows.min(u32::MAX as usize) as u32;
	preview_sparse_line_px(total_rows) * rows
}

fn preview_image_used_height_px(
	total_rows: usize,
	area_height: u16,
	font_height: u16,
) -> PixelHeight {
	if total_rows == 0 || area_height == 0 {
		return PixelHeight::ZERO;
	}
	let font_h = u32::from(font_height.max(1));
	let height_px = PixelHeight::new(u32::from(area_height).saturating_mul(font_h));
	let sparse_height_px = preview_sparse_height_px(total_rows);
	if sparse_height_px <= height_px {
		sparse_height_px
	} else {
		height_px
	}
}

pub(super) fn preview_image_content_height_rows(
	total_rows: usize,
	area_height: u16,
	font_height: u16,
) -> u32 {
	let used_height_px = preview_image_used_height_px(total_rows, area_height, font_height);
	if used_height_px == PixelHeight::ZERO {
		return 0;
	}
	let font_h = u32::from(font_height.max(1));
	used_height_px
		.get()
		.div_ceil(font_h)
		.min(u32::from(area_height))
		.max(1)
}

fn rgb_rgba(color: Rgb) -> Rgba<u8> {
	Rgba([color.r, color.g, color.b, 255])
}

fn rgb_components(color: Rgb) -> (u8, u8, u8) {
	(color.r, color.g, color.b)
}

fn terminal_color_components(color: Color) -> (u8, u8, u8) {
	match color {
		Color::Rgb(r, g, b) => (r, g, b),
		Color::Black => (0, 0, 0),
		Color::White => (255, 255, 255),
		Color::DarkGray => (96, 96, 96),
		Color::Gray => (160, 160, 160),
		Color::LightRed => (255, 128, 128),
		Color::LightGreen => (128, 255, 128),
		Color::LightYellow => (255, 255, 128),
		Color::LightBlue => (128, 128, 255),
		Color::LightMagenta => (255, 128, 255),
		Color::LightCyan => (128, 255, 255),
		Color::Red => (224, 92, 92),
		Color::Green => (92, 224, 92),
		Color::Yellow => (224, 224, 92),
		Color::Blue => (92, 92, 224),
		Color::Magenta => (224, 92, 224),
		Color::Cyan => (92, 224, 224),
		Color::Indexed(value) => (value, value, value),
		Color::Reset => (18, 18, 18),
	}
}

fn blend_channel(base: u8, accent: u8, ratio: BlendRatio) -> u8 {
	let base_weight = u32::from(ratio.base());
	let accent_weight = u32::from(ratio.accent());
	let total_weight = u32::from(ratio.total());
	((u32::from(base) * base_weight + u32::from(accent) * accent_weight) / total_weight) as u8
}

fn blend_colors(base: Color, accent: Color, ratio: BlendRatio) -> Color {
	let (br, bg, bb) = terminal_color_components(base);
	let (ar, ag, ab) = terminal_color_components(accent);
	Color::Rgb(
		blend_channel(br, ar, ratio),
		blend_channel(bg, ag, ratio),
		blend_channel(bb, ab, ratio),
	)
}

fn blend_rgba(base: Rgba<u8>, accent: Rgba<u8>, ratio: BlendRatio) -> Rgba<u8> {
	Rgba([
		blend_channel(base[0], accent[0], ratio),
		blend_channel(base[1], accent[1], ratio),
		blend_channel(base[2], accent[2], ratio),
		255,
	])
}

fn set_minimap_cell(buf: &mut Buffer, x: u16, y: u16, color: Color) {
	if let Some(cell) = buf.cell_mut((x, y)) {
		cell.set_char(' ')
			.set_style(Style::default().bg(color).fg(color));
	}
}

fn set_minimap_glyph(buf: &mut Buffer, x: u16, y: u16, glyph: char, fg: Color, bg: Color) {
	if let Some(cell) = buf.cell_mut((x, y)) {
		cell.set_char(glyph)
			.set_style(Style::default().fg(fg).bg(bg));
	}
}

fn tint_minimap_cell_bg(buf: &mut Buffer, x: u16, y: u16, color: Color) {
	if let Some(cell) = buf.cell_mut((x, y)) {
		cell.bg = color;
	}
}

fn fill_minimap_area(buf: &mut Buffer, area: Rect, color: Color) {
	for row in 0..area.height {
		for col in 0..area.width {
			set_minimap_cell(buf, area.x + col, area.y + row, color);
		}
	}
}

fn pixel_x_start(x: PixelX, width: PixelWidth) -> u32 {
	x.get().min(width.get())
}

fn pixel_y_start(y: PixelY, height: PixelHeight) -> u32 {
	y.get().min(height.get())
}

fn fill_image_rect(
	image: &mut RgbaImage,
	x0: PixelX,
	y0: PixelY,
	x1: PixelWidth,
	y1: PixelHeight,
	color: Rgba<u8>,
) {
	let max_x = PixelWidth::new(image.width());
	let max_y = PixelHeight::new(image.height());
	for y in pixel_y_start(y0, max_y)..y1.min(max_y).get() {
		for x in pixel_x_start(x0, max_x)..x1.min(max_x).get() {
			image.put_pixel(x, y, color);
		}
	}
}

fn tint_image_rect(
	image: &mut RgbaImage,
	x0: PixelX,
	y0: PixelY,
	x1: PixelWidth,
	y1: PixelHeight,
	tint: Rgba<u8>,
	ratio: BlendRatio,
) {
	let max_x = PixelWidth::new(image.width());
	let max_y = PixelHeight::new(image.height());
	for y in pixel_y_start(y0, max_y)..y1.min(max_y).get() {
		for x in pixel_x_start(x0, max_x)..x1.min(max_x).get() {
			let current = *image.get_pixel(x, y);
			image.put_pixel(x, y, blend_rgba(current, tint, ratio));
		}
	}
}

fn marker_gutter_width(area: Rect) -> u16 {
	if area.width > 2 { 1 } else { 0 }
}

fn render_search_markers(
	buf: &mut Buffer,
	area: Rect,
	content_height: u16,
	overlay: &MinimapOverlay,
	ui_palette: UiPalette,
) {
	let marker_gutter = marker_gutter_width(area);
	if marker_gutter == 0 || content_height == 0 {
		return;
	}

	let search_active_bg = crate::ui::ratatui_color(ui_palette.search_highlight);
	let search_inactive_bg = blend_colors(
		crate::ui::ratatui_color(ui_palette.minimap_surface),
		search_active_bg,
		BlendRatio::new(3, 5),
	);
	let marker_width = marker_gutter.min(area.width);
	let marker_x = area.x + area.width.saturating_sub(marker_width);
	for (band_idx, intensity) in overlay.search_bands.iter().copied().enumerate() {
		if intensity == 0 {
			continue;
		}
		let y0 =
			((band_idx as u32) * content_height as u32) / overlay.search_bands.len().max(1) as u32;
		let y1 = (((band_idx as u32) + 1) * content_height as u32)
			/ overlay.search_bands.len().max(1) as u32;
		let color = if overlay.active_search_band == Some(band_idx) {
			search_active_bg
		} else {
			search_inactive_bg
		};
		for row in y0.min(content_height.saturating_sub(1) as u32)
			..y1.max(y0 + 1).min(content_height as u32)
		{
			for x in 0..marker_width {
				set_minimap_cell(buf, marker_x + x, area.y + row as u16, color);
			}
		}
	}
}

fn render_search_markers_image(
	image: &mut RgbaImage,
	content_width_px: PixelWidth,
	content_height_px: PixelHeight,
	overlay: &MinimapOverlay,
	ui_palette: UiPalette,
) {
	if content_height_px == PixelHeight::ZERO || content_width_px.get() >= image.width() {
		return;
	}

	let marker_x_px = content_width_px.start_position();
	let (r, g, b) = rgb_components(ui_palette.search_highlight);
	let search_active_bg = Rgba([r, g, b, 255]);
	let search_inactive_bg = blend_rgba(
		rgb_rgba(ui_palette.minimap_surface),
		search_active_bg,
		BlendRatio::new(3, 5),
	);
	for (band_idx, intensity) in overlay.search_bands.iter().copied().enumerate() {
		if intensity == 0 {
			continue;
		}
		let y0 = PixelY::new(
			(band_idx as u32 * content_height_px.get()) / overlay.search_bands.len().max(1) as u32,
		);
		let y1 = PixelHeight::new(
			((band_idx as u32 + 1) * content_height_px.get())
				/ overlay.search_bands.len().max(1) as u32,
		);
		let color = if overlay.active_search_band == Some(band_idx) {
			search_active_bg
		} else {
			search_inactive_bg
		};
		fill_image_rect(
			image,
			marker_x_px,
			y0.min(content_height_px.saturating_sub(1).start_position()),
			PixelWidth::new(image.width()),
			y1.max(y0.boundary_after()).min(content_height_px),
			color,
		);
	}
}

fn overlay_bounds(
	overlay: &MinimapOverlay,
	content_height: u16,
) -> Option<(CellRow, CellHeight, CellRow)> {
	if content_height == 0 {
		return None;
	}

	let doc_line_count = overlay.doc_line_count;
	let cell_height = CellHeight::new(content_height);
	let last_doc_line = overlay.doc_line_count.last_index()?;
	let last_cell_row = cell_height.last_index()?;
	let viewport_start = overlay.viewport_start_line.min(last_doc_line);
	let viewport_end = overlay
		.viewport_end_line
		.max(viewport_start.boundary_after())
		.min(doc_line_count);
	let viewport_top = viewport_start.scale_into(doc_line_count, cell_height);
	let viewport_bottom = viewport_end
		.scale_into(doc_line_count, cell_height)
		.max(viewport_top.boundary_after())
		.min(cell_height);
	let cursor_row = overlay
		.cursor_line
		.min(last_doc_line)
		.scale_into(doc_line_count, cell_height);
	Some((viewport_top, viewport_bottom, cursor_row.min(last_cell_row)))
}

fn render_overview_overlay(
	buf: &mut Buffer,
	area: Rect,
	content_width: u16,
	base_color: Color,
	overlay: &MinimapOverlay,
	ui_palette: UiPalette,
) {
	use crate::ui::*;

	let Some((viewport_top, viewport_bottom, cursor_row)) = overlay_bounds(overlay, area.height)
	else {
		return;
	};
	let viewport_top = viewport_top.get();
	let viewport_bottom = viewport_bottom.get();
	let cursor_row = cursor_row.get();
	let viewport_bg = Color::Rgb(30, 30, 30);
	let viewport_content_bg = blend_colors(base_color, viewport_bg, BlendRatio::new(2, 3));
	let frame_bg = ratatui_color(ui_palette.minimap_accent);
	let frame_side_bg = frame_bg;
	let cursor_bg = frame_bg;
	let frame_right = area.x + content_width.saturating_sub(1);

	for row in viewport_top.min(area.height.saturating_sub(1))..viewport_bottom {
		for col in 0..content_width {
			set_minimap_cell(buf, area.x + col, area.y + row, viewport_content_bg);
		}
	}

	let frame_top = viewport_top.min(area.height.saturating_sub(1));
	let frame_bottom = viewport_bottom
		.saturating_sub(1)
		.min(area.height.saturating_sub(1));
	for col in 0..content_width {
		let x = area.x + col;
		set_minimap_cell(buf, x, area.y + frame_top, frame_bg);
		set_minimap_cell(buf, x, area.y + frame_bottom, frame_bg);
	}
	for row in frame_top..=frame_bottom {
		set_minimap_cell(buf, area.x, area.y + row, frame_side_bg);
		set_minimap_cell(buf, frame_right, area.y + row, frame_side_bg);
	}

	for col in 0..content_width {
		set_minimap_cell(buf, area.x + col, area.y + cursor_row, cursor_bg);
	}
}

fn render_preview_overlay(
	buf: &mut Buffer,
	area: Rect,
	content_width: u16,
	content_height: u16,
	overlay: &MinimapOverlay,
	ui_palette: UiPalette,
) {
	use crate::ui::*;

	let Some((viewport_top, viewport_bottom, cursor_row)) = overlay_bounds(overlay, content_height)
	else {
		return;
	};
	let frame_bg = blend_colors(
		ratatui_color(ui_palette.minimap_surface),
		ratatui_color(ui_palette.minimap_accent),
		BlendRatio::new(3, 4),
	);
	let frame_side_bg = frame_bg;
	let cursor_bg = ratatui_color(ui_palette.minimap_accent);
	let frame_right = area.x + content_width.saturating_sub(1);
	let frame_top = viewport_top.get().min(content_height.saturating_sub(1));
	let frame_bottom = viewport_bottom
		.get()
		.saturating_sub(1)
		.min(content_height.saturating_sub(1));

	for col in 0..content_width {
		let x = area.x + col;
		tint_minimap_cell_bg(buf, x, area.y + frame_top, frame_bg);
		tint_minimap_cell_bg(buf, x, area.y + frame_bottom, frame_bg);
	}
	for row in frame_top..=frame_bottom {
		tint_minimap_cell_bg(buf, area.x, area.y + row, frame_side_bg);
		tint_minimap_cell_bg(buf, frame_right, area.y + row, frame_side_bg);
	}
	for col in 0..content_width {
		tint_minimap_cell_bg(buf, area.x + col, area.y + cursor_row.get(), cursor_bg);
	}
}

fn render_preview_overlay_image(
	image: &mut RgbaImage,
	content_width_px: PixelWidth,
	content_height_px: PixelHeight,
	overlay: &MinimapOverlay,
	ui_palette: UiPalette,
) {
	if content_width_px == PixelWidth::ZERO || content_height_px == PixelHeight::ZERO {
		return;
	}
	let frame_bg = rgb_rgba(ui_palette.minimap_accent);
	let cursor_bg = frame_bg;
	let doc_line_count = overlay.doc_line_count;
	let pixel_height = content_height_px;
	let Some(last_doc_line) = overlay.doc_line_count.last_index() else {
		return;
	};
	let viewport_start = overlay.viewport_start_line.min(last_doc_line);
	let viewport_end = overlay
		.viewport_end_line
		.max(viewport_start.boundary_after())
		.min(doc_line_count);
	let cursor_line = overlay.cursor_line.min(last_doc_line);
	let cursor_line_end = cursor_line.boundary_after();
	let frame_top_px = viewport_start.scale_into(doc_line_count, pixel_height);
	let frame_bottom_px = viewport_end
		.scale_into(doc_line_count, pixel_height)
		.max(frame_top_px.boundary_after());
	let cursor_top_px = cursor_line.scale_into(doc_line_count, pixel_height);
	let cursor_bottom_px = cursor_line_end
		.min(doc_line_count)
		.scale_into(doc_line_count, pixel_height)
		.max(cursor_top_px.boundary_after());
	let horizontal_thickness = PixelHeight::new(if content_height_px.get() >= 180 { 2 } else { 1 });
	let vertical_thickness = PixelWidth::new(if content_width_px.get() >= 80 { 2 } else { 1 });
	let cursor_span_px = cursor_bottom_px.saturating_sub(cursor_top_px.boundary_before().get());
	let cursor_line_top =
		(cursor_top_px + (cursor_span_px.saturating_sub(horizontal_thickness.get()) / 2)).min(
			content_height_px
				.saturating_sub(horizontal_thickness.get())
				.start_position(),
		);
	let cursor_line_bottom = cursor_line_top
		.boundary_before()
		.saturating_add(horizontal_thickness.get())
		.min(content_height_px);
	tint_image_rect(
		image,
		PixelX::ZERO,
		frame_top_px,
		content_width_px,
		(frame_top_px + horizontal_thickness)
			.boundary_before()
			.min(content_height_px),
		frame_bg,
		BlendRatio::new(1, 1),
	);
	tint_image_rect(
		image,
		PixelX::ZERO,
		frame_bottom_px
			.saturating_sub(horizontal_thickness.get())
			.min(content_height_px.saturating_sub(1))
			.start_position(),
		content_width_px,
		frame_bottom_px.min(content_height_px),
		frame_bg,
		BlendRatio::new(1, 1),
	);
	tint_image_rect(
		image,
		PixelX::ZERO,
		frame_top_px,
		vertical_thickness,
		frame_bottom_px.min(content_height_px),
		frame_bg,
		BlendRatio::new(1, 1),
	);
	tint_image_rect(
		image,
		content_width_px
			.saturating_sub(vertical_thickness.get())
			.start_position(),
		frame_top_px,
		content_width_px,
		frame_bottom_px.min(content_height_px),
		frame_bg,
		BlendRatio::new(1, 1),
	);
	fill_image_rect(
		image,
		PixelX::ZERO,
		cursor_line_top,
		content_width_px,
		cursor_line_bottom,
		cursor_bg,
	);
}

fn sample_preview_bin(row: &PreviewRow, cell_col: u16, content_width: u16) -> TokenBin {
	sample_preview_bin_range(
		row,
		cell_col as usize,
		(cell_col as usize).saturating_add(1),
		content_width.max(1) as usize,
	)
}

fn sample_preview_half_bin(
	row: &PreviewRow,
	cell_col: u16,
	half: usize,
	content_width: u16,
) -> TokenBin {
	if half > 1 {
		return EMPTY_PREVIEW_BIN;
	}

	let total_half_columns = content_width.max(1) as usize * 2;
	let start_half = (cell_col as usize).saturating_mul(2).saturating_add(half);
	sample_preview_bin_range(
		row,
		start_half,
		start_half.saturating_add(1),
		total_half_columns,
	)
}

fn preview_braille_glyph(left_active: bool, right_active: bool) -> Option<char> {
	let mut mask = 0u8;
	if left_active {
		mask |= 0x02;
	}
	if right_active {
		mask |= 0x10;
	}
	(mask != 0).then(|| char::from_u32(0x2800 + mask as u32).unwrap_or(' '))
}

fn preview_bin_color(
	bin: TokenBin,
	theme_colors: &[Option<Rgb>; CATEGORY_COUNT],
	base_bg: Color,
) -> Option<Color> {
	if bin == EMPTY_PREVIEW_BIN {
		return None;
	}

	let accent = theme_colors
		.get(bin.index())
		.and_then(|color| *color)
		.map(crate::ui::ratatui_color)
		.unwrap_or(crate::ui::TEXT_FG);
	Some(blend_colors(base_bg, accent, BlendRatio::new(5, 6)))
}

fn preview_bin_rgba(
	bin: TokenBin,
	theme_colors: &[Option<Rgb>; CATEGORY_COUNT],
	base_bg: Rgba<u8>,
) -> Option<Rgba<u8>> {
	if bin == EMPTY_PREVIEW_BIN {
		return None;
	}

	let accent = theme_colors
		.get(bin.index())
		.and_then(|color| *color)
		.unwrap_or(Rgb::new(253, 253, 253));
	let (r, g, b) = rgb_components(accent);
	Some(blend_rgba(
		base_bg,
		Rgba([r, g, b, 255]),
		BlendRatio::new(5, 6),
	))
}

fn render_preview_image(
	snapshot: &PreviewSnapshot,
	area: Rect,
	theme_colors: &[Option<Rgb>; CATEGORY_COUNT],
	ui_palette: UiPalette,
	font_size: (u16, u16),
) -> DynamicImage {
	let font_w = PixelWidth::new(font_size.0.max(1) as u32);
	let font_h = PixelHeight::new(font_size.1.max(1) as u32);
	let width_px = font_w * area.width.max(1) as u32;
	let height_px = font_h * area.height.max(1) as u32;
	let base_bg = rgb_rgba(ui_palette.minimap_surface);
	let marker_gutter = marker_gutter_width(area);
	let content_width = area.width.saturating_sub(marker_gutter).max(1);
	let mut image = RgbaImage::from_pixel(width_px.get(), height_px.get(), base_bg);
	let total_rows = snapshot.rows.len();
	if total_rows == 0 {
		return DynamicImage::ImageRgba8(image);
	}

	let content_width_px = font_w * u32::from(content_width);
	let sparse_line_px = preview_sparse_line_px(total_rows);
	let sparse_height_px = preview_sparse_height_px(total_rows);
	let used_height_px = preview_image_used_height_px(total_rows, area.height, font_size.1);
	let use_sparse_preview = sparse_height_px <= height_px;
	if use_sparse_preview {
		let sparse_line_px = sparse_line_px.get();
		let ink_top_pad = if sparse_line_px > 1 { 1 } else { 0 };
		let ink_bottom_pad = if sparse_line_px > 2 { 1 } else { 0 };
		let max_columns = snapshot.max_columns.max(1) as u32;

		for (row_idx, row) in snapshot.rows.iter().enumerate() {
			if row.logical_width == 0 {
				continue;
			}

			let row_width_px = PixelWidth::new(
				((row.logical_width as u32).saturating_mul(content_width_px.get()))
					.div_ceil(max_columns)
					.max(1)
					.min(content_width_px.get()),
			);

			let row_top = PixelY::new(row_idx as u32 * sparse_line_px);
			let row_bottom = PixelHeight::new(
				row_top
					.get()
					.saturating_add(sparse_line_px)
					.min(height_px.get()),
			);
			let ink_top = PixelY::new(
				row_top
					.get()
					.saturating_add(ink_top_pad)
					.min(row_bottom.saturating_sub(1).get()),
			);
			let ink_bottom = row_bottom
				.saturating_sub(ink_bottom_pad)
				.max(ink_top.boundary_after());
			let core_top = if (ink_bottom.start_position() - ink_top).get() > 2 {
				ink_top + PixelHeight::new(1)
			} else {
				ink_top
			};
			let core_bottom = if ink_bottom
				.saturating_sub(core_top.boundary_before().get())
				.get() > 1
			{
				ink_bottom.saturating_sub(1)
			} else {
				ink_bottom
			};

			for x in 0..row_width_px.get() {
				let bin = sample_preview_bin_range(
					row,
					x.saturating_sub(1) as usize,
					(x + 2).min(row_width_px.get()) as usize,
					row_width_px.get() as usize,
				);
				let Some(color) = preview_bin_rgba(bin, theme_colors, base_bg) else {
					continue;
				};
				let edge = blend_rgba(base_bg, color, BlendRatio::new(3, 8));
				let core = blend_rgba(base_bg, color, BlendRatio::new(7, 8));
				let x = PixelX::new(x);
				fill_image_rect(
					&mut image,
					x,
					ink_top,
					(x + PixelWidth::new(1)).boundary_before(),
					ink_bottom,
					edge,
				);
				fill_image_rect(
					&mut image,
					x,
					core_top,
					(x + PixelWidth::new(1)).boundary_before(),
					core_bottom,
					core,
				);
			}
		}
	} else {
		let total_columns = content_width_px.get() as usize;
		let dense_x_pad = 0u32;
		for y in 0..used_height_px.get() {
			let row_start = ((y as usize) * total_rows) / used_height_px.get().max(1) as usize;
			let row_end = ((((y as usize) + 1) * total_rows) + used_height_px.get() as usize - 1)
				/ used_height_px.get().max(1) as usize;
			let row_end = row_end.max(row_start.saturating_add(1)).min(total_rows);
			let row_slice = &snapshot.rows[row_start.min(total_rows - 1)..row_end];
			for x in 0..content_width_px.get() {
				let within_cell = x % font_w.get();
				if within_cell < dense_x_pad
					|| within_cell >= font_w.get().saturating_sub(dense_x_pad)
				{
					continue;
				}

				let Some((bin, non_empty, sampled_rows)) = dominant_preview_bin_for_rows(
					row_slice,
					x as usize,
					(x as usize).saturating_add(1),
					total_columns,
				) else {
					continue;
				};
				let Some(base_color) = preview_bin_rgba(bin, theme_colors, base_bg) else {
					continue;
				};
				let coverage_ratio = BlendRatio::new(
					(2 + ((non_empty as u32 * 6) / sampled_rows.max(1) as u32) as u16).min(8),
					8,
				);
				let color = blend_rgba(base_bg, base_color, coverage_ratio);
				image.put_pixel(x, y, color);
			}
		}
	}

	render_search_markers_image(
		&mut image,
		content_width_px,
		used_height_px,
		&snapshot.overlay,
		ui_palette,
	);
	render_preview_overlay_image(
		&mut image,
		content_width_px,
		used_height_px,
		&snapshot.overlay,
		ui_palette,
	);
	DynamicImage::ImageRgba8(image)
}

pub(super) fn render_minimap_snapshot(
	buf: &mut Buffer,
	area: Rect,
	snapshot: &MinimapSnapshot,
	theme_colors: &[Option<Rgb>; CATEGORY_COUNT],
	ui_palette: UiPalette,
) {
	match snapshot {
		MinimapSnapshot::Preview(snapshot) => {
			render_preview_snapshot(buf, area, snapshot, theme_colors, ui_palette)
		}
		MinimapSnapshot::Overview(snapshot) => render_overview_snapshot(
			buf,
			area,
			snapshot,
			Color::Rgb(
				crate::ui::MINIMAP_RGBA_DENSITY[0],
				crate::ui::MINIMAP_RGBA_DENSITY[1],
				crate::ui::MINIMAP_RGBA_DENSITY[2],
			),
			ui_palette,
		),
		MinimapSnapshot::ByteFallback(snapshot) => render_overview_snapshot(
			buf,
			area,
			snapshot,
			Color::Rgb(
				crate::ui::MINIMAP_RGBA_FALLBACK[0],
				crate::ui::MINIMAP_RGBA_FALLBACK[1],
				crate::ui::MINIMAP_RGBA_FALLBACK[2],
			),
			ui_palette,
		),
	}
}

fn render_overview_snapshot(
	buf: &mut Buffer,
	area: Rect,
	snapshot: &OverviewSnapshot,
	band_color: Color,
	ui_palette: UiPalette,
) {
	use crate::ui::*;

	if area.width == 0 || area.height == 0 || snapshot.bands.is_empty() {
		return;
	}

	let base_bg = ratatui_color(ui_palette.minimap_surface);
	let marker_gutter = marker_gutter_width(area);
	let content_width = area.width.saturating_sub(marker_gutter).max(1);
	fill_minimap_area(buf, area, base_bg);

	for row in 0..area.height {
		let band_idx = ((row as usize) * snapshot.bands.len()) / (area.height.max(1) as usize);
		let density = snapshot.bands[band_idx.min(snapshot.bands.len().saturating_sub(1))];
		let line_width =
			((density as u32).saturating_mul(content_width as u32) / 255).max(1) as u16;
		for col in 0..line_width.min(content_width) {
			set_minimap_cell(buf, area.x + col, area.y + row, band_color);
		}
	}

	render_search_markers(buf, area, area.height, &snapshot.overlay, ui_palette);
	render_overview_overlay(
		buf,
		area,
		content_width,
		band_color,
		&snapshot.overlay,
		ui_palette,
	);
}

fn render_preview_snapshot(
	buf: &mut Buffer,
	area: Rect,
	snapshot: &PreviewSnapshot,
	theme_colors: &[Option<Rgb>; CATEGORY_COUNT],
	ui_palette: UiPalette,
) {
	use crate::ui::*;

	if area.width == 0 || area.height == 0 {
		return;
	}

	let base_bg = ratatui_color(ui_palette.minimap_surface);
	let marker_gutter = marker_gutter_width(area);
	let content_width = area.width.saturating_sub(marker_gutter).max(1);
	fill_minimap_area(buf, area, base_bg);

	let total_rows = snapshot.rows.len();
	if total_rows == 0 {
		render_search_markers(buf, area, area.height, &snapshot.overlay, ui_palette);
		return;
	}

	let content_height = area.height.min(total_rows as u16).max(1);
	for screen_row in 0..content_height {
		let row_idx = if total_rows <= area.height as usize {
			screen_row as usize
		} else {
			((screen_row as usize) * total_rows) / area.height.max(1) as usize
		};
		let row = &snapshot.rows[row_idx.min(total_rows - 1)];
		for col in 0..content_width {
			let left_bin = sample_preview_half_bin(row, col, 0, content_width);
			let right_bin = sample_preview_half_bin(row, col, 1, content_width);
			let cell_bin = sample_preview_bin(row, col, content_width);
			let left_active = left_bin != EMPTY_PREVIEW_BIN;
			let right_active = right_bin != EMPTY_PREVIEW_BIN;
			if let (Some(glyph), Some(color)) = (
				preview_braille_glyph(left_active, right_active),
				preview_bin_color(cell_bin, theme_colors, base_bg),
			) {
				set_minimap_glyph(
					buf,
					area.x + col,
					area.y + screen_row,
					glyph,
					color,
					base_bg,
				);
			}
		}
	}

	render_search_markers(buf, area, content_height, &snapshot.overlay, ui_palette);
	render_preview_overlay(
		buf,
		area,
		content_width,
		content_height,
		&snapshot.overlay,
		ui_palette,
	);
}

#[cfg(test)]
mod tests {
	use super::{
		preview_image_content_height_rows, preview_request_fingerprint, render_minimap_snapshot,
		render_preview_image, rgb_rgba, token_bin,
	};
	use libaryon::core::{DocLine, DocLineCount, PixelHeight, Rgb};
	use libaryon::engine::PREVIEW_BIN_COLUMNS;
	use libaryon::engine::UiPalette;
	use libaryon::engine::{
		EMPTY_PREVIEW_BIN, MinimapOverlay, MinimapSnapshot, OverviewSnapshot, PreviewRow,
		PreviewSnapshot,
	};
	use libaryon::svp::rust_highlight::CATEGORY_COUNT;
	#[cfg(test)]
	use libaryon::svp::rust_highlight::TokenCategory;
	use ratatui::{buffer::Buffer, layout::Rect, style::Color};

	fn palette_with_search(search_highlight: Rgb) -> UiPalette {
		UiPalette {
			search_highlight,
			..UiPalette::default()
		}
	}

	#[test]
	fn overview_renderer_keeps_uniform_width_for_uniform_bands() {
		let area = Rect::new(0, 0, 14, 20);
		let mut buf = Buffer::empty(area);
		let snapshot = MinimapSnapshot::ByteFallback(OverviewSnapshot {
			overlay: MinimapOverlay {
				doc_line_count: DocLineCount::new(1_000),
				viewport_start_line: DocLine::new(500),
				viewport_end_line: DocLineCount::new(501),
				viewport_line_count: 1,
				cursor_line: DocLine::new(500),
				search_bands: vec![0; 256],
				active_search_band: None,
			},
			bands: vec![127; 256],
		});

		render_minimap_snapshot(
			&mut buf,
			area,
			&snapshot,
			&[None; CATEGORY_COUNT],
			UiPalette::default(),
		);

		let fallback_bg = Color::Rgb(76, 76, 76);
		let expected_width = 6usize;
		for row in 0..area.height {
			if row == 10 {
				continue;
			}
			let width = (0..area.width)
				.take_while(|&col| buf[(col, row)].bg == fallback_bg)
				.count();
			assert_eq!(width, expected_width, "row {row} rendered uneven width");
		}
	}

	#[test]
	fn overview_renderer_uses_custom_search_highlight() {
		let area = Rect::new(0, 0, 14, 20);
		let mut buf = Buffer::empty(area);
		let mut search_bands = vec![0; 256];
		search_bands[128] = 255;
		let snapshot = MinimapSnapshot::Overview(OverviewSnapshot {
			overlay: MinimapOverlay {
				doc_line_count: DocLineCount::new(1_000),
				viewport_start_line: DocLine::ZERO,
				viewport_end_line: DocLineCount::new(20),
				viewport_line_count: 20,
				cursor_line: DocLine::ZERO,
				search_bands,
				active_search_band: Some(128),
			},
			bands: vec![127; 256],
		});
		let custom = Rgb::new(12, 200, 240);

		render_minimap_snapshot(
			&mut buf,
			area,
			&snapshot,
			&[None; CATEGORY_COUNT],
			palette_with_search(custom),
		);

		assert_eq!(
			buf[(area.width - 1, area.height / 2)].bg,
			crate::ui::ratatui_color(custom)
		);
	}

	#[test]
	fn preview_image_uses_custom_search_highlight() {
		let area = Rect::new(0, 0, 12, 10);
		let mut search_bands = vec![0; 256];
		search_bands[128] = 255;
		let snapshot = PreviewSnapshot {
			overlay: MinimapOverlay {
				doc_line_count: DocLineCount::new(100),
				viewport_start_line: DocLine::ZERO,
				viewport_end_line: DocLineCount::new(20),
				viewport_line_count: 20,
				cursor_line: DocLine::ZERO,
				search_bands,
				active_search_band: Some(128),
			},
			max_columns: 8,
			rows: (0..100)
				.map(|_| PreviewRow {
					logical_width: 8,
					bins: vec![token_bin(TokenCategory::Keyword); PREVIEW_BIN_COLUMNS]
						.into_boxed_slice(),
				})
				.collect(),
		};
		let custom = Rgb::new(12, 200, 240);
		let image = render_preview_image(
			&snapshot,
			area,
			&[None; CATEGORY_COUNT],
			palette_with_search(custom),
			(10, 20),
		)
		.to_rgba8();
		let marker_x = image.width() - 1;
		let marker_y = 50;

		assert_eq!(
			image[(marker_x, marker_y)],
			image::Rgba([12, 200, 240, 255])
		);
	}

	#[test]
	fn preview_image_and_fingerprint_use_custom_surface_and_accent() {
		let area = Rect::new(0, 0, 4, 3);
		let snapshot = PreviewSnapshot {
			overlay: MinimapOverlay {
				doc_line_count: DocLineCount::ZERO,
				viewport_start_line: DocLine::ZERO,
				viewport_end_line: DocLineCount::ZERO,
				viewport_line_count: 0,
				cursor_line: DocLine::ZERO,
				search_bands: vec![0; 256],
				active_search_band: None,
			},
			max_columns: 0,
			rows: Vec::new(),
		};
		let default = UiPalette::default();
		let custom = UiPalette {
			minimap_surface: Rgb::new(1, 2, 3),
			minimap_accent: Rgb::new(4, 5, 6),
			..default
		};
		let image = render_preview_image(&snapshot, area, &[None; CATEGORY_COUNT], custom, (8, 16))
			.to_rgba8();
		assert!(
			image
				.pixels()
				.all(|pixel| *pixel == image::Rgba([1, 2, 3, 255]))
		);
		assert_ne!(
			preview_request_fingerprint(&snapshot, area, &[None; CATEGORY_COUNT], default,),
			preview_request_fingerprint(&snapshot, area, &[None; CATEGORY_COUNT], custom,)
		);
	}

	#[test]
	fn preview_image_content_height_matches_sparse_renderer() {
		assert_eq!(preview_image_content_height_rows(0, 40, 20), 0);
		assert_eq!(preview_image_content_height_rows(20, 40, 20), 2);
		assert_eq!(preview_image_content_height_rows(100, 40, 20), 5);
		assert_eq!(preview_image_content_height_rows(1_000, 40, 20), 40);
	}

	#[test]
	fn preview_renderer_top_aligns_short_files() {
		let area = Rect::new(0, 0, 12, 10);
		let mut buf = Buffer::empty(area);
		let rows = vec![
			PreviewRow {
				logical_width: 8,
				bins: {
					let mut bins = vec![EMPTY_PREVIEW_BIN; PREVIEW_BIN_COLUMNS];
					bins[0] = token_bin(TokenCategory::Keyword);
					bins.into_boxed_slice()
				},
			},
			PreviewRow {
				logical_width: 8,
				bins: {
					let mut bins = vec![EMPTY_PREVIEW_BIN; PREVIEW_BIN_COLUMNS];
					bins[PREVIEW_BIN_COLUMNS / 3] = token_bin(TokenCategory::Function);
					bins.into_boxed_slice()
				},
			},
		];
		let snapshot = MinimapSnapshot::Preview(PreviewSnapshot {
			overlay: MinimapOverlay {
				doc_line_count: DocLineCount::new(2),
				viewport_start_line: DocLine::ZERO,
				viewport_end_line: DocLineCount::new(2),
				viewport_line_count: 2,
				cursor_line: DocLine::ZERO,
				search_bands: vec![0; 256],
				active_search_band: None,
			},
			max_columns: 8,
			rows,
		});
		let mut theme_colors = [None; CATEGORY_COUNT];
		theme_colors[TokenCategory::Keyword as usize] = Some(Rgb::new(255, 0, 0));
		theme_colors[TokenCategory::Function as usize] = Some(Rgb::new(0, 255, 0));

		render_minimap_snapshot(
			&mut buf,
			area,
			&snapshot,
			&theme_colors,
			UiPalette::default(),
		);

		assert!((0..area.width).any(|x| buf[(x, 0)].symbol() != " "));
		assert!((0..area.width).any(|x| buf[(x, 1)].symbol() != " "));
		assert_eq!(buf[(0, 5)].symbol(), " ");
	}

	#[test]
	fn preview_image_top_aligns_short_files() {
		let area = Rect::new(0, 0, 12, 10);
		let snapshot = PreviewSnapshot {
			overlay: MinimapOverlay {
				doc_line_count: DocLineCount::new(2),
				viewport_start_line: DocLine::ZERO,
				viewport_end_line: DocLineCount::new(2),
				viewport_line_count: 2,
				cursor_line: DocLine::ZERO,
				search_bands: vec![0; 256],
				active_search_band: None,
			},
			max_columns: 8,
			rows: vec![
				PreviewRow {
					logical_width: 8,
					bins: {
						let mut bins = vec![EMPTY_PREVIEW_BIN; PREVIEW_BIN_COLUMNS];
						bins[0] = token_bin(TokenCategory::Keyword);
						bins.into_boxed_slice()
					},
				},
				PreviewRow {
					logical_width: 8,
					bins: {
						let mut bins = vec![EMPTY_PREVIEW_BIN; PREVIEW_BIN_COLUMNS];
						bins[PREVIEW_BIN_COLUMNS / 2] = token_bin(TokenCategory::Function);
						bins.into_boxed_slice()
					},
				},
			],
		};
		let mut theme_colors = [None; CATEGORY_COUNT];
		theme_colors[TokenCategory::Keyword as usize] = Some(Rgb::new(255, 0, 0));
		theme_colors[TokenCategory::Function as usize] = Some(Rgb::new(0, 255, 0));

		let image = render_preview_image(
			&snapshot,
			area,
			&theme_colors,
			UiPalette::default(),
			(10, 20),
		)
		.to_rgba8();
		let base_bg = image[(0, image.height() - 1)];
		let top_has_ink = (0..image.width()).any(|x| image[(x, 1)] != base_bg);
		let lower_empty = (0..image.width()).all(|x| image[(x, image.height() - 5)] == base_bg);

		assert!(top_has_ink);
		assert!(lower_empty);
	}

	#[test]
	fn preview_image_dense_files_use_full_vertical_extent() {
		let area = Rect::new(0, 0, 12, 10);
		let mut rows = Vec::new();
		for row_idx in 0..400 {
			rows.push(PreviewRow {
				logical_width: 8,
				bins: {
					let mut bins = vec![EMPTY_PREVIEW_BIN; PREVIEW_BIN_COLUMNS];
					for preview_bin in bins.iter_mut().skip(row_idx % 3).step_by(3) {
						*preview_bin = token_bin(TokenCategory::Keyword);
					}
					bins.into_boxed_slice()
				},
			});
		}
		let snapshot = PreviewSnapshot {
			overlay: MinimapOverlay {
				doc_line_count: DocLineCount::new(rows.len() as u64),
				viewport_start_line: DocLine::ZERO,
				viewport_end_line: DocLineCount::new(20),
				viewport_line_count: 20,
				cursor_line: DocLine::new(10),
				search_bands: vec![0; 256],
				active_search_band: None,
			},
			max_columns: 8,
			rows,
		};
		let mut theme_colors = [None; CATEGORY_COUNT];
		theme_colors[TokenCategory::Keyword as usize] = Some(Rgb::new(255, 0, 0));

		let image = render_preview_image(
			&snapshot,
			area,
			&theme_colors,
			UiPalette::default(),
			(10, 20),
		)
		.to_rgba8();
		let base_bg = image[(0, image.height() - 1)];
		let near_bottom_has_ink =
			(0..image.width()).any(|x| image[(x, image.height().saturating_sub(8))] != base_bg);

		assert!(near_bottom_has_ink);
	}

	#[test]
	fn preview_image_dense_files_do_not_leave_periodic_empty_vertical_stripes() {
		let area = Rect::new(0, 0, 12, 10);
		let rows = (0..400)
			.map(|_| PreviewRow {
				logical_width: 8,
				bins: vec![token_bin(TokenCategory::Keyword); PREVIEW_BIN_COLUMNS]
					.into_boxed_slice(),
			})
			.collect();
		let snapshot = PreviewSnapshot {
			overlay: MinimapOverlay {
				doc_line_count: DocLineCount::new(400),
				viewport_start_line: DocLine::ZERO,
				viewport_end_line: DocLineCount::new(20),
				viewport_line_count: 20,
				cursor_line: DocLine::new(10),
				search_bands: vec![0; 256],
				active_search_band: None,
			},
			max_columns: 8,
			rows,
		};
		let mut theme_colors = [None; CATEGORY_COUNT];
		theme_colors[TokenCategory::Keyword as usize] = Some(Rgb::new(255, 0, 0));

		let image = render_preview_image(
			&snapshot,
			area,
			&theme_colors,
			UiPalette::default(),
			(10, 20),
		)
		.to_rgba8();
		let base_bg = image[(0, image.height() - 1)];
		let content_width_px = (area.width.saturating_sub(1).max(1) as u32) * 10;

		for x in 1..content_width_px.saturating_sub(1) {
			let has_ink = (0..image.height()).any(|y| image[(x, y)] != base_bg);
			assert!(has_ink, "column {x} rendered as an empty vertical stripe");
		}
	}

	#[test]
	fn preview_image_sparse_files_do_not_leave_periodic_empty_vertical_stripes() {
		let area = Rect::new(0, 0, 12, 10);
		let rows = (0..100)
			.map(|_| PreviewRow {
				logical_width: 8,
				bins: vec![token_bin(TokenCategory::Keyword); PREVIEW_BIN_COLUMNS]
					.into_boxed_slice(),
			})
			.collect();
		let snapshot = PreviewSnapshot {
			overlay: MinimapOverlay {
				doc_line_count: DocLineCount::new(100),
				viewport_start_line: DocLine::ZERO,
				viewport_end_line: DocLineCount::new(20),
				viewport_line_count: 20,
				cursor_line: DocLine::new(10),
				search_bands: vec![0; 256],
				active_search_band: None,
			},
			max_columns: 8,
			rows,
		};
		let mut theme_colors = [None; CATEGORY_COUNT];
		theme_colors[TokenCategory::Keyword as usize] = Some(Rgb::new(255, 0, 0));

		let image = render_preview_image(
			&snapshot,
			area,
			&theme_colors,
			UiPalette::default(),
			(10, 20),
		)
		.to_rgba8();
		let base_bg = image[(0, image.height() - 1)];
		let content_width_px = (area.width.saturating_sub(1).max(1) as u32) * 10;

		for x in 1..content_width_px.saturating_sub(1) {
			let has_ink = (0..100u32).any(|y| image[(x, y)] != base_bg);
			assert!(
				has_ink,
				"column {x} rendered as an empty sparse vertical stripe"
			);
		}
	}

	#[test]
	fn preview_image_tiny_files_do_not_leave_periodic_empty_vertical_stripes() {
		// Regression: for small files (total_rows <= 64) sparse_line_px is 2,
		// which previously triggered a per-font-cell 1-px horizontal pad at
		// x % font_w in {0, font_w - 1}. After Resize::Fit scales the image
		// down to terminal cells those tiny gaps aliased into visible vertical
		// bands — see the lib.rs screenshot regression tracked in the ticket
		// thread for this bug.
		let area = Rect::new(0, 0, 12, 10);
		let rows = (0..8)
			.map(|_| PreviewRow {
				logical_width: 8,
				bins: vec![token_bin(TokenCategory::Keyword); PREVIEW_BIN_COLUMNS]
					.into_boxed_slice(),
			})
			.collect();
		let snapshot = PreviewSnapshot {
			overlay: MinimapOverlay {
				doc_line_count: DocLineCount::new(8),
				viewport_start_line: DocLine::ZERO,
				viewport_end_line: DocLineCount::new(8),
				viewport_line_count: 8,
				cursor_line: DocLine::ZERO,
				search_bands: vec![0; 256],
				active_search_band: None,
			},
			max_columns: 8,
			rows,
		};
		let mut theme_colors = [None; CATEGORY_COUNT];
		theme_colors[TokenCategory::Keyword as usize] = Some(Rgb::new(255, 0, 0));

		let image = render_preview_image(
			&snapshot,
			area,
			&theme_colors,
			UiPalette::default(),
			(10, 20),
		)
		.to_rgba8();
		// Probe base_bg from below used_height_px (8 rows * sparse_line_px=2 = 16)
		// so the sample is guaranteed to be unpainted by either the bin path
		// or any overlay (frame/cursor).
		let base_bg = image[(area.width as u32 * 10 / 2, image.height() - 1)];
		let content_width_px = (area.width.saturating_sub(1).max(1) as u32) * 10;

		// Skip x=0..1 and the last two columns (vertical viewport frame tints
		// those unconditionally). Check every non-frame column at at least one
		// ink row — since every row's bins are fully keyword-filled, no x
		// inside row_width_px should render as an empty stripe.
		let ink_rows: [u32; 7] = [1, 3, 5, 7, 9, 11, 13];
		for x in 2..content_width_px.saturating_sub(2) {
			let has_ink = ink_rows.iter().any(|&y| image[(x, y)] != base_bg);
			assert!(
				has_ink,
				"column {x} rendered as an empty vertical stripe for a tiny file"
			);
		}
	}

	#[test]
	fn preview_image_sparse_files_respect_relative_line_widths() {
		let area = Rect::new(0, 0, 12, 10);
		let snapshot = PreviewSnapshot {
			overlay: MinimapOverlay {
				doc_line_count: DocLineCount::new(100),
				viewport_start_line: DocLine::new(90),
				viewport_end_line: DocLineCount::new(95),
				viewport_line_count: 5,
				cursor_line: DocLine::new(92),
				search_bands: vec![0; 256],
				active_search_band: None,
			},
			max_columns: 32,
			rows: vec![
				PreviewRow {
					logical_width: 8,
					bins: vec![token_bin(TokenCategory::Keyword); PREVIEW_BIN_COLUMNS]
						.into_boxed_slice(),
				},
				PreviewRow {
					logical_width: 32,
					bins: vec![token_bin(TokenCategory::Keyword); PREVIEW_BIN_COLUMNS]
						.into_boxed_slice(),
				},
			],
		};
		let mut theme_colors = [None; CATEGORY_COUNT];
		theme_colors[TokenCategory::Keyword as usize] = Some(Rgb::new(255, 0, 0));

		let image = render_preview_image(
			&snapshot,
			area,
			&theme_colors,
			UiPalette::default(),
			(10, 20),
		)
		.to_rgba8();
		let base_bg = image[(0, image.height() - 1)];
		let content_width_px = (area.width.saturating_sub(1).max(1) as u32) * 10;
		let probe_x = content_width_px.saturating_sub(6);
		let short_row_y = 1u32;
		let long_row_y = 3u32;

		assert_eq!(image[(probe_x, short_row_y)], base_bg);
		assert_ne!(image[(probe_x, long_row_y)], base_bg);
	}

	#[test]
	fn preview_image_uses_the_minimap_accent_for_the_viewport_frame() {
		let area = Rect::new(0, 0, 12, 10);
		let snapshot = PreviewSnapshot {
			overlay: MinimapOverlay {
				doc_line_count: DocLineCount::new(20),
				viewport_start_line: DocLine::new(4),
				viewport_end_line: DocLineCount::new(10),
				viewport_line_count: 6,
				cursor_line: DocLine::new(7),
				search_bands: vec![0; 256],
				active_search_band: None,
			},
			max_columns: 8,
			rows: (0..20)
				.map(|_| PreviewRow {
					logical_width: 8,
					bins: vec![token_bin(TokenCategory::Keyword); PREVIEW_BIN_COLUMNS]
						.into_boxed_slice(),
				})
				.collect(),
		};
		let mut theme_colors = [None; CATEGORY_COUNT];
		theme_colors[TokenCategory::Keyword as usize] = Some(Rgb::new(255, 0, 0));

		let image = render_preview_image(
			&snapshot,
			area,
			&theme_colors,
			UiPalette::default(),
			(10, 20),
		)
		.to_rgba8();
		let expected = rgb_rgba(UiPalette::default().minimap_accent);
		let sparse_line_px = (20u32 / 6).clamp(2, 4);
		let used_height_px = snapshot.rows.len() as u32 * sparse_line_px;
		let frame_y = snapshot
			.overlay
			.viewport_start_line
			.scale_into(
				snapshot.overlay.doc_line_count,
				PixelHeight::new(used_height_px),
			)
			.get();

		assert_eq!(
			image[(0, frame_y.min(image.height().saturating_sub(1)))],
			expected
		);
	}

	#[test]
	fn preview_image_uses_the_minimap_accent_for_the_cursor_line() {
		let area = Rect::new(0, 0, 12, 10);
		let snapshot = PreviewSnapshot {
			overlay: MinimapOverlay {
				doc_line_count: DocLineCount::new(20),
				viewport_start_line: DocLine::new(4),
				viewport_end_line: DocLineCount::new(10),
				viewport_line_count: 6,
				cursor_line: DocLine::new(7),
				search_bands: vec![0; 256],
				active_search_band: None,
			},
			max_columns: 8,
			rows: (0..20)
				.map(|_| PreviewRow {
					logical_width: 8,
					bins: vec![token_bin(TokenCategory::Keyword); PREVIEW_BIN_COLUMNS]
						.into_boxed_slice(),
				})
				.collect(),
		};
		let mut theme_colors = [None; CATEGORY_COUNT];
		theme_colors[TokenCategory::Keyword as usize] = Some(Rgb::new(255, 0, 0));

		let image = render_preview_image(
			&snapshot,
			area,
			&theme_colors,
			UiPalette::default(),
			(10, 20),
		)
		.to_rgba8();
		let expected = rgb_rgba(UiPalette::default().minimap_accent);
		let sparse_line_px = if snapshot.rows.len() <= 64 { 2 } else { 1 };
		let used_height_px = snapshot.rows.len() as u32 * sparse_line_px;
		let cursor_top = snapshot
			.overlay
			.cursor_line
			.scale_into(
				snapshot.overlay.doc_line_count,
				PixelHeight::new(used_height_px),
			)
			.get();
		let cursor_bottom = snapshot.overlay.cursor_line.boundary_after().scale_into(
			snapshot.overlay.doc_line_count,
			PixelHeight::new(used_height_px),
		);
		let cursor_bottom = cursor_bottom.get();
		let horizontal_thickness = if image.height() >= 180 { 2 } else { 1 };
		let cursor_y = cursor_top
			.saturating_add(
				cursor_bottom
					.saturating_sub(cursor_top)
					.saturating_sub(horizontal_thickness)
					/ 2,
			)
			.min(image.height().saturating_sub(horizontal_thickness));

		assert_eq!(
			image[(
				image.width() / 2,
				cursor_y.min(image.height().saturating_sub(1))
			)],
			expected
		);
	}
}
