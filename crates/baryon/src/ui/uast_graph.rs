use font8x8::{BASIC_FONTS, UnicodeFonts};
use image::{Rgba, RgbaImage};
use libaryon::core::{PixelHeight, PixelWidth, ViewId};
use libaryon::uast::{SemanticKind, UastGraphSnapshot};
use ratatui::{Frame, layout::Rect};
use ratatui_image::{Image, Resize, picker::Picker, protocol::Protocol};
use std::collections::{HashMap, hash_map::DefaultHasher};
use std::hash::{Hash, Hasher};
use std::sync::mpsc;
use std::thread;

pub(super) struct UastGraphController {
	tx_request: mpsc::Sender<GraphRequest>,
	rx_ready: mpsc::Receiver<GraphResponse>,
	protocols: HashMap<ViewId, Protocol>,
	requested: HashMap<ViewId, u64>,
}

struct GraphRequest {
	view_id: ViewId,
	fingerprint: u64,
	area: Rect,
	snapshot: UastGraphSnapshot,
}

struct GraphResponse {
	view_id: ViewId,
	fingerprint: u64,
	protocol: Option<Protocol>,
}

impl UastGraphController {
	pub fn new() -> Self {
		let picker = Picker::from_query_stdio().unwrap_or_else(|_| Picker::halfblocks());
		let (tx_request, rx_request) = mpsc::channel::<GraphRequest>();
		let (tx_ready, rx_ready) = mpsc::channel::<GraphResponse>();
		thread::spawn(move || {
			'worker: while let Ok(request) = rx_request.recv() {
				// Coalesce per view: superseded requests for the same pane are
				// dropped, while other panes' latest requests are kept.
				let mut latest = vec![request];
				while let Ok(next) = rx_request.try_recv() {
					latest.retain(|queued| queued.view_id != next.view_id);
					latest.push(next);
				}
				for request in latest {
					let protocol = render_protocol(&picker, &request);
					if tx_ready
						.send(GraphResponse {
							view_id: request.view_id,
							fingerprint: request.fingerprint,
							protocol,
						})
						.is_err()
					{
						break 'worker;
					}
				}
			}
		});
		Self {
			tx_request,
			rx_ready,
			protocols: HashMap::new(),
			requested: HashMap::new(),
		}
	}

	pub fn request(&mut self, view_id: ViewId, snapshot: &UastGraphSnapshot, area: Rect) {
		let fingerprint = fingerprint(snapshot, area);
		if self.requested.get(&view_id) == Some(&fingerprint) {
			return;
		}
		self.requested.insert(view_id, fingerprint);
		if self
			.tx_request
			.send(GraphRequest {
				view_id,
				fingerprint,
				area,
				snapshot: snapshot.clone(),
			})
			.is_err()
		{
			self.requested.remove(&view_id);
		}
	}

	pub fn poll(&mut self) -> bool {
		let mut updated = false;
		while let Ok(response) = self.rx_ready.try_recv() {
			if self.requested.get(&response.view_id) != Some(&response.fingerprint) {
				continue;
			}
			match response.protocol {
				Some(protocol) => {
					self.protocols.insert(response.view_id, protocol);
					updated = true;
				}
				// A failed render must not stand as satisfied: forgetting the
				// fingerprint lets a later frame retry, while responses for
				// newer requests are left untouched by the guard above.
				None => {
					self.requested.remove(&response.view_id);
				}
			}
		}
		updated
	}

	pub fn render(&self, view_id: ViewId, frame: &mut Frame<'_>, area: Rect) -> bool {
		let Some(protocol) = self.protocols.get(&view_id) else {
			return false;
		};
		frame.render_widget(Image::new(protocol), area);
		true
	}

	pub fn retain_views(&mut self, retain: impl Fn(ViewId) -> bool) {
		self.protocols.retain(|view_id, _| retain(*view_id));
		self.requested.retain(|view_id, _| retain(*view_id));
	}
}

fn fingerprint(snapshot: &UastGraphSnapshot, area: Rect) -> u64 {
	let mut hasher = DefaultHasher::new();
	snapshot.hash(&mut hasher);
	area.x.hash(&mut hasher);
	area.y.hash(&mut hasher);
	area.width.hash(&mut hasher);
	area.height.hash(&mut hasher);
	hasher.finish()
}

fn render_protocol(picker: &Picker, request: &GraphRequest) -> Option<Protocol> {
	if request.area.width == 0 || request.area.height == 0 {
		return None;
	}
	let font = picker.font_size();
	let width =
		PixelWidth::new(u32::from(request.area.width).saturating_mul(u32::from(font.width.max(1))));
	let height = PixelHeight::new(
		u32::from(request.area.height).saturating_mul(u32::from(font.height.max(1))),
	);
	let mut image = RgbaImage::from_pixel(width.get(), height.get(), Rgba([16, 18, 24, 255]));
	draw_graph(&mut image, &request.snapshot);
	picker
		.new_protocol(
			image::DynamicImage::ImageRgba8(image),
			request.area.into(),
			Resize::Fit(None),
		)
		.ok()
}

#[derive(Clone, Copy)]
struct NodeBox {
	x: u32,
	y: u32,
	width: u32,
	height: u32,
}

fn draw_graph(image: &mut RgbaImage, snapshot: &UastGraphSnapshot) {
	if snapshot.nodes.is_empty() {
		return;
	}
	let min_depth = snapshot
		.nodes
		.iter()
		.map(|node| node.depth)
		.min()
		.unwrap_or(0);
	let max_depth = snapshot
		.nodes
		.iter()
		.map(|node| node.depth)
		.max()
		.unwrap_or(min_depth);
	let level_count = u32::from(max_depth.saturating_sub(min_depth)) + 1;
	let mut levels = HashMap::<u16, Vec<u32>>::new();
	for node in &snapshot.nodes {
		levels.entry(node.depth).or_default().push(node.id);
	}
	let box_height = 30u32.min(
		(image.height() / level_count.max(1))
			.saturating_sub(6)
			.max(14),
	);
	let mut boxes = HashMap::<u32, NodeBox>::new();
	for (depth, ids) in &levels {
		let count = ids.len() as u32;
		let slot_width = image.width() / count.max(1);
		let box_width = slot_width.saturating_sub(8).clamp(24, 144);
		let level = u32::from(depth.saturating_sub(min_depth));
		let y_slot = image.height() / level_count.max(1);
		let y = level
			.saturating_mul(y_slot)
			.saturating_add(y_slot.saturating_sub(box_height) / 2);
		for (index, id) in ids.iter().enumerate() {
			let x = (index as u32)
				.saturating_mul(slot_width)
				.saturating_add(slot_width.saturating_sub(box_width) / 2);
			boxes.insert(
				*id,
				NodeBox {
					x,
					y,
					width: box_width.min(image.width().saturating_sub(x)),
					height: box_height.min(image.height().saturating_sub(y)),
				},
			);
		}
	}

	for node in &snapshot.nodes {
		let (Some(parent), Some(child)) = (
			node.parent.and_then(|id| boxes.get(&id)),
			boxes.get(&node.id),
		) else {
			continue;
		};
		draw_line(
			image,
			parent.x + parent.width / 2,
			parent.y + parent.height,
			child.x + child.width / 2,
			child.y,
			Rgba([103, 116, 142, 255]),
		);
	}

	for node in &snapshot.nodes {
		let Some(rect) = boxes.get(&node.id).copied() else {
			continue;
		};
		let fill = match node.semantic_kind {
			SemanticKind::RelationalTable => Rgba([40, 66, 96, 255]),
			SemanticKind::RelationalRow => Rgba([48, 82, 72, 255]),
			SemanticKind::Group => Rgba([65, 59, 83, 255]),
			SemanticKind::Token => Rgba([55, 59, 68, 255]),
		};
		fill_rect(image, rect, fill);
		let border = if node.selected {
			Rgba([255, 199, 92, 255])
		} else {
			Rgba([151, 166, 196, 255])
		};
		stroke_rect(image, rect, border);
		let max_chars = rect.width.saturating_sub(6) as usize / 8;
		let label = node
			.label
			.chars()
			.take(max_chars.max(1))
			.collect::<String>();
		draw_text(
			image,
			rect.x.saturating_add(4),
			rect.y.saturating_add(4),
			&label,
			Rgba([238, 241, 247, 255]),
		);
		if rect.height >= 24 {
			draw_text(
				image,
				rect.x.saturating_add(4),
				rect.y.saturating_add(15),
				&format!("#{}", node.id),
				Rgba([177, 187, 207, 255]),
			);
		}
	}
}

fn fill_rect(image: &mut RgbaImage, rect: NodeBox, color: Rgba<u8>) {
	for y in rect.y..rect.y.saturating_add(rect.height).min(image.height()) {
		for x in rect.x..rect.x.saturating_add(rect.width).min(image.width()) {
			image.put_pixel(x, y, color);
		}
	}
}

fn stroke_rect(image: &mut RgbaImage, rect: NodeBox, color: Rgba<u8>) {
	if rect.width == 0 || rect.height == 0 {
		return;
	}
	let right = rect.x.saturating_add(rect.width - 1).min(image.width() - 1);
	let bottom = rect
		.y
		.saturating_add(rect.height - 1)
		.min(image.height() - 1);
	for x in rect.x..=right {
		image.put_pixel(x, rect.y.min(image.height() - 1), color);
		image.put_pixel(x, bottom, color);
	}
	for y in rect.y..=bottom {
		image.put_pixel(rect.x.min(image.width() - 1), y, color);
		image.put_pixel(right, y, color);
	}
}

fn draw_line(image: &mut RgbaImage, x0: u32, y0: u32, x1: u32, y1: u32, color: Rgba<u8>) {
	let (mut x0, mut y0, x1, y1) = (x0 as i32, y0 as i32, x1 as i32, y1 as i32);
	let dx = (x1 - x0).abs();
	let sx = if x0 < x1 { 1 } else { -1 };
	let dy = -(y1 - y0).abs();
	let sy = if y0 < y1 { 1 } else { -1 };
	let mut error = dx + dy;
	loop {
		if x0 >= 0 && y0 >= 0 && (x0 as u32) < image.width() && (y0 as u32) < image.height() {
			image.put_pixel(x0 as u32, y0 as u32, color);
		}
		if x0 == x1 && y0 == y1 {
			break;
		}
		let twice = 2 * error;
		if twice >= dy {
			error += dy;
			x0 += sx;
		}
		if twice <= dx {
			error += dx;
			y0 += sy;
		}
	}
}

fn draw_text(image: &mut RgbaImage, x: u32, y: u32, text: &str, color: Rgba<u8>) {
	let mut cursor = x;
	for ch in text.chars() {
		let glyph = BASIC_FONTS
			.get(ch)
			.or_else(|| BASIC_FONTS.get('?'))
			.unwrap_or([0; 8]);
		for (row, bits) in glyph.iter().enumerate() {
			for column in 0..8 {
				if bits & (1 << column) == 0 {
					continue;
				}
				let px = cursor.saturating_add(column);
				let py = y.saturating_add(row as u32);
				if px < image.width() && py < image.height() {
					image.put_pixel(px, py, color);
				}
			}
		}
		cursor = cursor.saturating_add(8);
		if cursor >= image.width() {
			break;
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use libaryon::core::{BufferId, StateId, TopologyRev};
	use libaryon::uast::{ProviderKindId, UastGraphNode, UastGraphStamp, UastStorageKind};

	#[test]
	fn graph_renderer_draws_nodes_and_edges() {
		let snapshot = UastGraphSnapshot {
			stamp: UastGraphStamp {
				source_buffer_id: BufferId::new(1),
				root_id: 1,
				state_id: StateId::ZERO,
				topology_rev: TopologyRev::ZERO,
			},
			nodes: vec![
				UastGraphNode {
					id: 1,
					parent: None,
					depth: 0,
					label: "SOURCE_FILE".to_string(),
					semantic_kind: SemanticKind::RelationalTable,
					storage: UastStorageKind::Branch,
					start_byte: 0,
					end_byte: 10,
					provider_kind: ProviderKindId::new(1),
					ancestry: vec![ProviderKindId::new(1)],
					selected: true,
				},
				UastGraphNode {
					id: 2,
					parent: Some(1),
					depth: 1,
					label: "FN".to_string(),
					semantic_kind: SemanticKind::RelationalTable,
					storage: UastStorageKind::Branch,
					start_byte: 0,
					end_byte: 10,
					provider_kind: ProviderKindId::new(2),
					ancestry: vec![ProviderKindId::new(1), ProviderKindId::new(2)],
					selected: false,
				},
			],
			source_truncated: false,
		};
		let background = Rgba([16, 18, 24, 255]);
		let mut image = RgbaImage::from_pixel(320, 180, background);
		draw_graph(&mut image, &snapshot);
		assert!(image.pixels().any(|pixel| *pixel != background));
	}
}
