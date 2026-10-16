use image::Rgba;
use libaryon::core::{
	DiffFingerprint, DocLine, PaneId, PaneLocalRow, PixelDelta, PixelHeight, PixelWidth, PixelY,
	Rgb, ScreenRow,
};
use libaryon::engine::UiPalette;
use libaryon::uast::projection::{DiffBlock, DiffKind, PaneViewport, Viewport};
use ratatui::layout::Rect;
use ratatui_image::{Resize, picker::Picker, protocol::Protocol};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::mpsc;
use std::thread;

pub(super) struct DiffBridgeController {
	tx_request: mpsc::Sender<BridgeRequest>,
	rx_ready: mpsc::Receiver<BridgeResponse>,
	current_protocols: std::collections::HashMap<(PaneId, PaneId), Protocol>,
	requested_fingerprints: std::collections::HashMap<(PaneId, PaneId), DiffFingerprint>,
	hovered_band: Option<HoveredDiffBand>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct HoveredDiffBand {
	left_pane_id: PaneId,
	right_pane_id: PaneId,
	left_range: (DocLine, DocLine),
	right_range: (DocLine, DocLine),
	kind: DiffKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DiffHoverEmphasis {
	Normal,
	Selected,
	Dimmed,
}

impl HoveredDiffBand {
	pub(super) fn emphasis_for(
		self,
		pane_id: PaneId,
		partner_pane_id: PaneId,
		block: &DiffBlock,
	) -> DiffHoverEmphasis {
		let selected = if pane_id == self.left_pane_id && partner_pane_id == self.right_pane_id {
			block.range == self.left_range
				&& block.partner_range == self.right_range
				&& block.kind == self.kind
		} else if pane_id == self.right_pane_id && partner_pane_id == self.left_pane_id {
			block.range == self.right_range
				&& block.partner_range == self.left_range
				&& block.kind == self.kind
		} else {
			return DiffHoverEmphasis::Normal;
		};

		if selected {
			DiffHoverEmphasis::Selected
		} else {
			DiffHoverEmphasis::Dimmed
		}
	}

	fn matches_left_block(self, block: &DiffBlock) -> bool {
		block.range == self.left_range
			&& block.partner_range == self.right_range
			&& block.kind == self.kind
	}

	fn remains_valid(self, view: &Viewport) -> bool {
		view.pane(self.left_pane_id)
			.and_then(|pane| pane.diff_pair.as_ref())
			.is_some_and(|pair| {
				pair.partner_pane_id == self.right_pane_id
					&& pair
						.blocks
						.iter()
						.any(|block| self.matches_left_block(block))
			})
	}
}

struct BridgeRequest {
	left_pane_id: PaneId,
	right_pane_id: PaneId,
	fingerprint: DiffFingerprint,
	area: Rect,
	left_visible_rows: Vec<libaryon::engine::VisibleRow>,
	right_visible_rows: Vec<libaryon::engine::VisibleRow>,
	blocks: Vec<DiffBlock>,
	text_area_top: ScreenRow,
	colors: DiffColors,
	hovered_band: Option<HoveredDiffBand>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct DiffColors {
	added: Rgb,
	removed: Rgb,
	modified: Rgb,
	moved: Rgb,
}

const DIFF_BRIDGE_ALPHA: u8 = 192;
const DIFF_BRIDGE_FOCUSED_ALPHA: u8 = 240;
const DIFF_BRIDGE_DIMMED_ALPHA: u8 = 40;

impl From<UiPalette> for DiffColors {
	fn from(palette: UiPalette) -> Self {
		Self {
			added: palette.diff_added,
			removed: palette.diff_removed,
			modified: palette.diff_modified,
			moved: palette.diff_moved,
		}
	}
}

fn bridge_tint(color: Rgb, emphasis: DiffHoverEmphasis) -> Rgba<u8> {
	let alpha = match emphasis {
		DiffHoverEmphasis::Normal => DIFF_BRIDGE_ALPHA,
		DiffHoverEmphasis::Selected => DIFF_BRIDGE_FOCUSED_ALPHA,
		DiffHoverEmphasis::Dimmed => DIFF_BRIDGE_DIMMED_ALPHA,
	};
	Rgba([color.r, color.g, color.b, alpha])
}

struct BridgeResponse {
	left_pane_id: PaneId,
	right_pane_id: PaneId,
	fingerprint: DiffFingerprint,
	protocol: Option<Protocol>,
}

impl DiffBridgeController {
	pub fn new(picker: Picker) -> Self {
		let (tx_request, rx_request) = mpsc::channel::<BridgeRequest>();
		let (tx_ready, rx_ready) = mpsc::channel::<BridgeResponse>();

		thread::spawn(move || {
			while let Ok(request) = rx_request.recv() {
				let mut pending = std::collections::HashMap::new();
				pending.insert((request.left_pane_id, request.right_pane_id), request);
				while let Ok(next) = rx_request.try_recv() {
					pending.insert((next.left_pane_id, next.right_pane_id), next);
				}
				for request in pending.into_values() {
					let protocol = render_bridge_protocol(&picker, &request);
					if tx_ready
						.send(BridgeResponse {
							left_pane_id: request.left_pane_id,
							right_pane_id: request.right_pane_id,
							fingerprint: request.fingerprint,
							protocol,
						})
						.is_err()
					{
						return;
					}
				}
			}
		});

		Self {
			tx_request,
			rx_ready,
			current_protocols: std::collections::HashMap::new(),
			requested_fingerprints: std::collections::HashMap::new(),
			hovered_band: None,
		}
	}

	pub fn hovered_band(&self) -> Option<HoveredDiffBand> {
		self.hovered_band
	}

	pub fn set_hovered_band(&mut self, hovered_band: Option<HoveredDiffBand>) -> bool {
		if self.hovered_band == hovered_band {
			return false;
		}
		self.hovered_band = hovered_band;
		true
	}

	pub fn retain_valid_hover(&mut self, view: &Viewport) {
		if self
			.hovered_band
			.is_some_and(|hover| !hover.remains_valid(view))
		{
			self.hovered_band = None;
		}
	}

	pub fn update_hover(&mut self, view: &Viewport, column: u16, row: u16) -> bool {
		let invalidated = self
			.hovered_band
			.is_some_and(|hover| !hover.remains_valid(view));
		if invalidated {
			self.hovered_band = None;
		}
		let next = hovered_diff_band_at(view, column, row);
		let crossing_selected_bridge = next.is_none()
			&& self
				.hovered_band
				.is_some_and(|hover| hovered_bridge_contains(view, hover, column, row));
		if crossing_selected_bridge {
			invalidated
		} else {
			self.set_hovered_band(next) || invalidated
		}
	}

	pub fn request_bridge(
		&mut self,
		left_pane: &PaneViewport,
		right_pane: &PaneViewport,
		area: Rect,
		text_area_top: ScreenRow,
		palette: UiPalette,
	) {
		let pair_key = (left_pane.pane_id, right_pane.pane_id);
		let blocks = left_pane.diff_pair.as_ref().map(|p| &p.blocks).unwrap();
		if blocks.is_empty() {
			self.clear_bridge(left_pane.pane_id, right_pane.pane_id);
			return;
		}
		let colors = DiffColors::from(palette);
		let hovered_band = self.hovered_band.filter(|hover| {
			hover.left_pane_id == left_pane.pane_id
				&& hover.right_pane_id == right_pane.pane_id
				&& blocks.iter().any(|block| hover.matches_left_block(block))
		});

		let fingerprint =
			bridge_request_fingerprint(left_pane, right_pane, area, blocks, colors, hovered_band);
		if self.requested_fingerprints.get(&pair_key) == Some(&fingerprint) {
			return;
		}

		self.requested_fingerprints.insert(pair_key, fingerprint);

		if self
			.tx_request
			.send(BridgeRequest {
				left_pane_id: left_pane.pane_id,
				right_pane_id: right_pane.pane_id,
				fingerprint,
				area,
				left_visible_rows: left_pane
					.visible_rows
					.iter()
					.map(|r| libaryon::engine::VisibleRow {
						line: r.line,
						start_col: r.start_col,
					})
					.collect(),
				right_visible_rows: right_pane
					.visible_rows
					.iter()
					.map(|r| libaryon::engine::VisibleRow {
						line: r.line,
						start_col: r.start_col,
					})
					.collect(),
				blocks: blocks.clone(),
				text_area_top,
				colors,
				hovered_band,
			})
			.is_err()
		{
			self.requested_fingerprints.remove(&pair_key);
		}
	}

	pub fn clear_bridge(&mut self, left_id: PaneId, right_id: PaneId) {
		for pair_key in [(left_id, right_id), (right_id, left_id)] {
			self.requested_fingerprints.remove(&pair_key);
			self.current_protocols.remove(&pair_key);
		}
	}

	pub fn poll(&mut self) -> bool {
		let mut updated = false;
		while let Ok(response) = self.rx_ready.try_recv() {
			let pair_key = (response.left_pane_id, response.right_pane_id);
			if self.requested_fingerprints.get(&pair_key) == Some(&response.fingerprint) {
				if let Some(protocol) = response.protocol {
					self.current_protocols.insert(pair_key, protocol);
					updated = true;
				} else {
					self.requested_fingerprints.remove(&pair_key);
					updated = true;
				}
			}
		}
		updated
	}

	pub fn get_protocol(&self, left_id: PaneId, right_id: PaneId) -> Option<&Protocol> {
		self.current_protocols.get(&(left_id, right_id))
	}
}

pub(super) fn should_render_diff_bridge(blocks: &[DiffBlock], pane_resize_active: bool) -> bool {
	!pane_resize_active && !blocks.is_empty()
}

fn bridge_request_fingerprint(
	left: &PaneViewport,
	right: &PaneViewport,
	area: Rect,
	blocks: &[DiffBlock],
	colors: DiffColors,
	hovered_band: Option<HoveredDiffBand>,
) -> DiffFingerprint {
	let mut hasher = DefaultHasher::new();
	area.x.hash(&mut hasher);
	area.y.hash(&mut hasher);
	area.width.hash(&mut hasher);
	area.height.hash(&mut hasher);
	left.viewport_start_line.get().hash(&mut hasher);
	left.viewport_row_offset.get().hash(&mut hasher);
	right.viewport_start_line.get().hash(&mut hasher);
	right.viewport_row_offset.get().hash(&mut hasher);
	blocks.hash(&mut hasher);
	colors.hash(&mut hasher);
	hovered_band.hash(&mut hasher);
	DiffFingerprint::new(hasher.finish())
}

fn render_bridge_protocol(picker: &Picker, request: &BridgeRequest) -> Option<Protocol> {
	let font_size = picker.font_size();
	let font_w = PixelWidth::new(font_size.width.max(1) as u32);
	let font_h = PixelHeight::new(font_size.height.max(1) as u32);
	let width_px = font_w * u32::from(request.area.width);
	let height_px = font_h * u32::from(request.area.height);

	let mut image =
		image::RgbaImage::from_pixel(width_px.get(), height_px.get(), Rgba([18, 18, 18, 0]));

	for selected_pass in [false, true] {
		for block in &request.blocks {
			let emphasis = request
				.hovered_band
				.map_or(DiffHoverEmphasis::Normal, |hover| {
					hover.emphasis_for(request.left_pane_id, request.right_pane_id, block)
				});
			if (emphasis == DiffHoverEmphasis::Selected) != selected_pass {
				continue;
			}
			let color = match block.kind {
				libaryon::uast::projection::DiffKind::Added => request.colors.added,
				libaryon::uast::projection::DiffKind::Removed => request.colors.removed,
				libaryon::uast::projection::DiffKind::Modified => request.colors.modified,
				libaryon::uast::projection::DiffKind::Moved => request.colors.moved,
			};
			let color = bridge_tint(color, emphasis);

			let left_y_range = find_line_range_y(
				block.range,
				&request.left_visible_rows,
				request.text_area_top,
				ScreenRow::new(request.area.y as u32),
				font_h,
			);
			let right_y_range = find_line_range_y(
				block.partner_range,
				&request.right_visible_rows,
				request.text_area_top,
				ScreenRow::new(request.area.y as u32),
				font_h,
			);

			if let (Some((l0, l1)), Some((r0, r1))) = (left_y_range, right_y_range) {
				draw_bezier_bridge(&mut image, l0, l1, r0, r1, color);
			}
		}
	}

	picker
		.new_protocol(
			image::DynamicImage::ImageRgba8(image),
			request.area.into(),
			Resize::Fit(None),
		)
		.ok()
}

pub(super) fn hovered_diff_band_at(
	view: &Viewport,
	column: u16,
	row: u16,
) -> Option<HoveredDiffBand> {
	let pane = view.panes.iter().find(|pane| {
		pane.rect.contains(
			libaryon::core::ScreenCol::new(u32::from(column)),
			ScreenRow::new(u32::from(row)),
		)
	})?;
	let pair = pane.diff_pair.as_ref()?;
	let text_area_top = pane
		.rect
		.screen_row
		.saturating_add(libaryon::core::DOCTAB_BAR_ROWS.into());
	let local_row = u32::from(row).checked_sub(text_area_top.get())? as usize;
	let line = pane.visible_rows.get(local_row)?.line;
	let block = pair
		.blocks
		.iter()
		.find(|block| line >= block.range.0 && line < block.range.1)?;
	let partner = view.pane(pair.partner_pane_id)?;

	if pane.rect.screen_col < partner.rect.screen_col {
		Some(HoveredDiffBand {
			left_pane_id: pane.pane_id,
			right_pane_id: partner.pane_id,
			left_range: block.range,
			right_range: block.partner_range,
			kind: block.kind,
		})
	} else if partner.rect.screen_col < pane.rect.screen_col {
		Some(HoveredDiffBand {
			left_pane_id: partner.pane_id,
			right_pane_id: pane.pane_id,
			left_range: block.partner_range,
			right_range: block.range,
			kind: block.kind,
		})
	} else {
		None
	}
}

pub(super) fn hovered_bridge_contains(
	view: &Viewport,
	hover: HoveredDiffBand,
	column: u16,
	row: u16,
) -> bool {
	let Some(left) = view.pane(hover.left_pane_id) else {
		return false;
	};
	let Some(right) = view.pane(hover.right_pane_id) else {
		return false;
	};
	let column = u32::from(column);
	let row = u32::from(row);
	let text_top = left
		.rect
		.screen_row
		.saturating_add(libaryon::core::DOCTAB_BAR_ROWS.into())
		.get();
	column >= left.rect.right().get()
		&& column < right.rect.screen_col.get()
		&& row >= text_top
		&& row < left.rect.bottom().get()
}

fn find_line_range_y(
	range: (DocLine, DocLine),
	visible_rows: &[libaryon::engine::VisibleRow],
	text_area_top: ScreenRow,
	bridge_y: ScreenRow,
	font_h: PixelHeight,
) -> Option<(f32, f32)> {
	let y_for_line = |target_line: DocLine| -> f32 {
		let mut last_y = 0.0;
		for (i, row) in visible_rows.iter().enumerate() {
			let pane_row = PaneLocalRow::new(i as u32);
			let screen_y = text_area_top.saturating_add(pane_row.get());
			let relative_y = screen_y.saturating_sub(bridge_y.get());
			let y_px = PixelY::new(relative_y.get().saturating_mul(font_h.get()));
			if row.line >= target_line {
				return y_px.get() as f32;
			}
			last_y = (y_px + font_h).get() as f32;
		}
		// Extrapolate if past the end or before start
		if let (Some(first), Some(last)) = (visible_rows.first(), visible_rows.last()) {
			if target_line < first.line {
				let lines_above = first.line.get() - target_line.get();
				let first_screen_y = text_area_top.saturating_sub(bridge_y.get());
				let y_px = PixelDelta::new(
					first_screen_y.get() as i32 * font_h.get() as i32
						- lines_above as i32 * font_h.get() as i32,
				);
				return y_px.get() as f32;
			}
			if target_line > last.line {
				let lines_below = target_line.get() - last.line.get();
				return last_y + (lines_below as f32 * font_h.get() as f32);
			}
		}
		0.0
	};

	if range.0 == range.1 {
		let y = y_for_line(range.0);
		Some((y, y))
	} else {
		Some((y_for_line(range.0), y_for_line(range.1)))
	}
}

fn draw_bezier_bridge(
	image: &mut image::RgbaImage,
	l0: f32,
	l1: f32,
	r0: f32,
	r1: f32,
	color: Rgba<u8>,
) {
	let width = image.width() as f32;

	for x in 0..image.width() {
		let t = x as f32 / width;
		// Cubic Bezier: (1-t)^3*P0 + 3(1-t)^2*t*P1 + 3(1-t)*t^2*P2 + t^3*P3
		// We use two curves, one for the top edge and one for the bottom edge.
		let y_top = cubic_bezier(t, l0, l0, r0, r0);
		let y_bottom = cubic_bezier(t, l1, l1, r1, r1);

		let start_y = (y_top as i32).max(0) as u32;
		let end_y = (y_bottom as i32).max(0) as u32;
		let end_y = end_y.max(start_y + 1);

		for y in start_y..end_y {
			if y < image.height() {
				// Simple alpha blending
				let pixel = image.get_pixel_mut(x, y);
				*pixel = blend_rgba(*pixel, color);
			}
		}
	}
}

fn cubic_bezier(t: f32, p0: f32, p1: f32, p2: f32, p3: f32) -> f32 {
	let mt = 1.0 - t;
	mt * mt * mt * p0 + 3.0 * mt * mt * t * p1 + 3.0 * mt * t * t * p2 + t * t * t * p3
}

// Actually, for a simple smooth transition, we can use a S-curve (clamped cosine or smoothstep)
fn blend_rgba(base: Rgba<u8>, overlay: Rgba<u8>) -> Rgba<u8> {
	let alpha = overlay[3] as f32 / 255.0;
	let inv_alpha = 1.0 - alpha;
	Rgba([
		(base[0] as f32 * inv_alpha + overlay[0] as f32 * alpha) as u8,
		(base[1] as f32 * inv_alpha + overlay[1] as f32 * alpha) as u8,
		(base[2] as f32 * inv_alpha + overlay[2] as f32 * alpha) as u8,
		255,
	])
}

#[cfg(test)]
mod tests {
	use super::*;
	use ratatui::layout::Rect;
	use std::hash::{Hash, Hasher};

	fn color_fingerprint(colors: DiffColors) -> u64 {
		let mut hasher = DefaultHasher::new();
		colors.hash(&mut hasher);
		hasher.finish()
	}

	#[test]
	fn bridge_palette_participates_in_the_cache_identity() {
		let first = DiffColors::from(UiPalette::default());
		let second = DiffColors {
			added: Rgb::new(1, 2, 3),
			..first
		};
		assert_ne!(color_fingerprint(first), color_fingerprint(second));
	}

	#[test]
	fn bridge_uses_a_vivid_glass_alpha() {
		let tint = bridge_tint(Rgb::new(10, 20, 30), DiffHoverEmphasis::Normal);
		assert_eq!(tint, Rgba([10, 20, 30, 192]));
	}

	#[test]
	fn hover_focus_makes_one_band_dominant_without_changing_its_color() {
		let color = Rgb::new(10, 20, 30);
		assert_eq!(
			bridge_tint(color, DiffHoverEmphasis::Selected),
			Rgba([10, 20, 30, 240])
		);
		assert_eq!(
			bridge_tint(color, DiffHoverEmphasis::Dimmed),
			Rgba([10, 20, 30, 40])
		);
	}

	#[test]
	fn bridge_render_policy_skips_empty_pairs_and_active_resize() {
		let block = DiffBlock {
			range: (DocLine::new(0), DocLine::new(1)),
			partner_range: (DocLine::new(0), DocLine::new(1)),
			kind: DiffKind::Modified,
			intra_line_diffs: None,
		};

		assert!(!should_render_diff_bridge(&[], false));
		assert!(!should_render_diff_bridge(
			std::slice::from_ref(&block),
			true
		));
		assert!(should_render_diff_bridge(
			std::slice::from_ref(&block),
			false
		));
	}

	#[test]
	fn hovered_band_recognizes_both_mirrored_endpoint_blocks() {
		let hover = HoveredDiffBand {
			left_pane_id: PaneId::new(1),
			right_pane_id: PaneId::new(2),
			left_range: (DocLine::new(3), DocLine::new(5)),
			right_range: (DocLine::new(8), DocLine::new(11)),
			kind: DiffKind::Modified,
		};
		let left = DiffBlock {
			range: hover.left_range,
			partner_range: hover.right_range,
			kind: hover.kind,
			intra_line_diffs: None,
		};
		let right = DiffBlock {
			range: hover.right_range,
			partner_range: hover.left_range,
			kind: hover.kind,
			intra_line_diffs: None,
		};
		let unrelated = DiffBlock {
			range: (DocLine::new(12), DocLine::new(13)),
			partner_range: (DocLine::new(12), DocLine::new(13)),
			kind: DiffKind::Added,
			intra_line_diffs: None,
		};

		assert_eq!(
			hover.emphasis_for(PaneId::new(1), PaneId::new(2), &left),
			DiffHoverEmphasis::Selected
		);
		assert_eq!(
			hover.emphasis_for(PaneId::new(2), PaneId::new(1), &right),
			DiffHoverEmphasis::Selected
		);
		assert_eq!(
			hover.emphasis_for(PaneId::new(1), PaneId::new(2), &unrelated),
			DiffHoverEmphasis::Dimmed
		);
		assert_eq!(
			hover.emphasis_for(PaneId::new(9), PaneId::new(10), &unrelated),
			DiffHoverEmphasis::Normal
		);
	}

	#[test]
	fn test_render_bridge_protocol() {
		// A unit test has no terminal to query; stdio queries block without one.
		let picker = Picker::halfblocks();

		let request = BridgeRequest {
			left_pane_id: PaneId::new(1),
			right_pane_id: PaneId::new(2),
			fingerprint: DiffFingerprint::new(123),
			area: Rect {
				x: 40,
				y: 5,
				width: 1,
				height: 20,
			},
			left_visible_rows: vec![],
			right_visible_rows: vec![],
			blocks: vec![DiffBlock {
				range: (DocLine::new(0), DocLine::new(10)),
				kind: libaryon::uast::projection::DiffKind::Modified,
				partner_range: (DocLine::new(0), DocLine::new(10)),
				intra_line_diffs: None,
			}],
			text_area_top: ScreenRow::new(0),
			colors: DiffColors::from(UiPalette::default()),
			hovered_band: None,
		};

		let protocol = render_bridge_protocol(&picker, &request);
		assert!(protocol.is_some(), "Protocol should not be None");
	}
}
