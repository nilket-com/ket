use image::Rgba;
use libaryon::core::{
	DiffFingerprint, DocLine, PaneId, PaneLocalRow, PixelDelta, PixelHeight, PixelWidth, PixelY,
	ScreenRow,
};
use libaryon::uast::projection::{DiffBlock, PaneViewport};
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
}

struct BridgeResponse {
	left_pane_id: PaneId,
	right_pane_id: PaneId,
	fingerprint: DiffFingerprint,
	protocol: Option<Protocol>,
}

impl DiffBridgeController {
	pub fn new() -> Self {
		let picker = Picker::from_query_stdio().unwrap_or_else(|_| Picker::halfblocks());
		let (tx_request, rx_request) = mpsc::channel::<BridgeRequest>();
		let (tx_ready, rx_ready) = mpsc::channel::<BridgeResponse>();

		thread::spawn(move || {
			while let Ok(request) = rx_request.recv() {
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
					break;
				}
			}
		});

		Self {
			tx_request,
			rx_ready,
			current_protocols: std::collections::HashMap::new(),
			requested_fingerprints: std::collections::HashMap::new(),
		}
	}

	pub fn request_bridge(
		&mut self,
		left_pane: &PaneViewport,
		right_pane: &PaneViewport,
		area: Rect,
		text_area_top: ScreenRow,
	) {
		let pair_key = (left_pane.pane_id, right_pane.pane_id);
		let blocks = left_pane.diff_pair.as_ref().map(|p| &p.blocks).unwrap();

		let fingerprint = bridge_request_fingerprint(left_pane, right_pane, area, blocks);
		if self.requested_fingerprints.get(&pair_key) == Some(&fingerprint) {
			return;
		}

		self.requested_fingerprints.insert(pair_key, fingerprint);

		let _ = self.tx_request.send(BridgeRequest {
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
		});
	}

	pub fn poll(&mut self) -> bool {
		let mut updated = false;
		while let Ok(response) = self.rx_ready.try_recv() {
			let pair_key = (response.left_pane_id, response.right_pane_id);
			if self.requested_fingerprints.get(&pair_key) == Some(&response.fingerprint)
				&& let Some(protocol) = response.protocol
			{
				self.current_protocols.insert(pair_key, protocol);
				updated = true;
			}
		}
		updated
	}

	pub fn get_protocol(&self, left_id: PaneId, right_id: PaneId) -> Option<&Protocol> {
		self.current_protocols.get(&(left_id, right_id))
	}
}

fn bridge_request_fingerprint(
	left: &PaneViewport,
	right: &PaneViewport,
	area: Rect,
	blocks: &[DiffBlock],
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
	DiffFingerprint::new(hasher.finish())
}

fn render_bridge_protocol(picker: &Picker, request: &BridgeRequest) -> Option<Protocol> {
	let font_size = picker.font_size();
	let font_w = PixelWidth::new(font_size.0.max(1) as u32);
	let font_h = PixelHeight::new(font_size.1.max(1) as u32);
	let width_px = font_w * u32::from(request.area.width);
	let height_px = font_h * u32::from(request.area.height);

	let mut image =
		image::RgbaImage::from_pixel(width_px.get(), height_px.get(), Rgba([18, 18, 18, 0]));

	for block in &request.blocks {
		let color = match block.kind {
			libaryon::uast::projection::DiffKind::Added => Rgba([0, 128, 0, 128]),
			libaryon::uast::projection::DiffKind::Removed => Rgba([128, 0, 0, 128]),
			libaryon::uast::projection::DiffKind::Modified => Rgba([0, 0, 128, 128]),
			libaryon::uast::projection::DiffKind::Moved => Rgba([128, 0, 128, 128]),
		};

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

	picker
		.new_protocol(
			image::DynamicImage::ImageRgba8(image),
			request.area,
			Resize::Fit(None),
		)
		.ok()
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

	#[test]
	fn test_render_bridge_protocol() {
		let picker = Picker::from_query_stdio().unwrap_or_else(|_| Picker::halfblocks());

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
		};

		let protocol = render_bridge_protocol(&picker, &request);
		assert!(protocol.is_some(), "Protocol should not be None");
	}
}
