use iced::advanced::renderer;
use iced::advanced::text;
use iced::{Color, Font, Point, Rectangle, Size};

use crate::gui::pane_grid::state::{Axis, CollapseState, PaneId, SplitId};
use crate::gui::pane_grid::style::HandleStyle;

const HANDLE_FONT: Font = Font::with_name("DejaVu Sans Mono");

const DOT_TOP_RADIUS: f32 = 1.5;
const DOT_BOTTOM_RADIUS: f32 = 3.0;
const DOT_SPACING: f32 = 5.0;
const ARROW_FONT_SIZE: f32 = 14.0;
const ARROW_ZONE_SIZE: f32 = 16.0;
const ARROW_GAP: f32 = -8.0;
const DOT_GROUP_OFFSET: f32 = 16.0;
const COLLAPSED_STRIP_THICKNESS: f32 = 6.0;
pub const STRIP_THICKNESS: f32 = 7.0;
pub const PANE_DRAG_HANDLE_THICKNESS: f32 = 18.0;
const GRIP_DOT_SPACING: f32 = 4.0;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandleAction {
	CollapseFirst(SplitId),
	CollapseSecond(SplitId),
	Expand(SplitId),
}

#[derive(Debug, Clone, Copy)]
pub struct HandleZone {
	pub split_id: SplitId,
	pub bounds: Rectangle,
	pub axis: Axis,
	pub collapse: CollapseState,
	pub first_arrow: Rectangle,
	pub second_arrow: Rectangle,
	/// The full region this split divides (needed for accurate ratio computation).
	pub parent_region: Rectangle,
}

impl HandleZone {
	pub fn new(
		split_id: SplitId,
		bounds: Rectangle,
		axis: Axis,
		collapse: CollapseState,
		parent_region: Rectangle,
	) -> Self {
		let (first_arrow, second_arrow) = arrow_zones(bounds, axis);
		Self {
			split_id,
			bounds,
			axis,
			collapse,
			first_arrow,
			second_arrow,
			parent_region,
		}
	}

	pub fn hit_test(&self, point: Point) -> Option<HandleAction> {
		if self.first_arrow.contains(point) {
			return Some(match self.collapse {
				CollapseState::FirstCollapsed => HandleAction::Expand(self.split_id),
				_ => HandleAction::CollapseFirst(self.split_id),
			});
		}
		if self.second_arrow.contains(point) {
			return Some(match self.collapse {
				CollapseState::SecondCollapsed => HandleAction::Expand(self.split_id),
				_ => HandleAction::CollapseSecond(self.split_id),
			});
		}
		None
	}

	pub fn contains(&self, point: Point) -> bool {
		self.bounds.contains(point)
	}
}

fn arrow_zones(bounds: Rectangle, axis: Axis) -> (Rectangle, Rectangle) {
	let cx = bounds.x + bounds.width / 2.0;
	let cy = bounds.y + bounds.height / 2.0;
	let half_gap = ARROW_GAP / 2.0;

	match axis {
		Axis::Horizontal => {
			let first = Rectangle {
				x: bounds.x,
				y: cy - half_gap - ARROW_ZONE_SIZE,
				width: bounds.width,
				height: ARROW_ZONE_SIZE,
			};
			let second = Rectangle {
				x: bounds.x,
				y: cy + half_gap,
				width: bounds.width,
				height: ARROW_ZONE_SIZE,
			};
			(first, second)
		}
		Axis::Vertical => {
			let first = Rectangle {
				x: cx - half_gap - ARROW_ZONE_SIZE,
				y: bounds.y,
				width: ARROW_ZONE_SIZE,
				height: bounds.height,
			};
			let second = Rectangle {
				x: cx + half_gap,
				y: bounds.y,
				width: ARROW_ZONE_SIZE,
				height: bounds.height,
			};
			(first, second)
		}
	}
}

pub fn draw_handle<Renderer>(
	renderer: &mut Renderer,
	zone: &HandleZone,
	style: &HandleStyle,
	hovered_arrow: Option<bool>,
) where
	Renderer: renderer::Renderer + text::Renderer<Font = iced::Font>,
{
	let bounds = zone.bounds;

	renderer.fill_quad(
		renderer::Quad {
			bounds,
			border: iced::Border {
				color: style.border_color,
				width: 0.5,
				radius: 0.0.into(),
			},
			..renderer::Quad::default()
		},
		style.background,
	);

	draw_dots(renderer, bounds, zone.axis, style);

	let first_color = if hovered_arrow == Some(true) || zone.collapse == CollapseState::FirstCollapsed {
		style.arrow_hover_color
	} else {
		style.arrow_color
	};
	let second_color = if hovered_arrow == Some(false) || zone.collapse == CollapseState::SecondCollapsed {
		style.arrow_hover_color
	} else {
		style.arrow_color
	};

	let (first_glyph, second_glyph) = arrow_glyphs(zone.axis, zone.collapse);

	draw_arrow_text(renderer, zone.first_arrow, first_glyph, first_color, ARROW_FONT_SIZE);
	draw_arrow_text(renderer, zone.second_arrow, second_glyph, second_color, ARROW_FONT_SIZE);
}

fn arrow_glyphs(axis: Axis, collapse: CollapseState) -> (&'static str, &'static str) {
	match axis {
		Axis::Horizontal => {
			let first = match collapse {
				CollapseState::FirstCollapsed => "\u{25b6}",
				_ => "\u{25c0}",
			};
			let second = match collapse {
				CollapseState::SecondCollapsed => "\u{25c0}",
				_ => "\u{25b6}",
			};
			(first, second)
		}
		Axis::Vertical => {
			let first = match collapse {
				CollapseState::FirstCollapsed => "\u{25bc}",
				_ => "\u{25b2}",
			};
			let second = match collapse {
				CollapseState::SecondCollapsed => "\u{25b2}",
				_ => "\u{25bc}",
			};
			(first, second)
		}
	}
}

fn draw_arrow_text<Renderer>(
	renderer: &mut Renderer,
	zone: Rectangle,
	glyph: &str,
	color: Color,
	font_size: f32,
) where
	Renderer: renderer::Renderer + text::Renderer<Font = iced::Font>,
{
	renderer.fill_text(
		text::Text {
			content: glyph.to_owned(),
			bounds: Size::new(zone.width, zone.height),
			size: font_size.into(),
			line_height: text::LineHeight::default(),
			font: HANDLE_FONT,
			align_x: text::Alignment::Center,
			align_y: iced::alignment::Vertical::Center,
			shaping: text::Shaping::Basic,
			wrapping: text::Wrapping::None,
		},
		Point::new(zone.center_x(), zone.center_y()),
		color,
		zone,
	);
}

fn draw_dots<Renderer>(renderer: &mut Renderer, bounds: Rectangle, axis: Axis, style: &HandleStyle)
where
	Renderer: renderer::Renderer,
{
	let cx = bounds.x + bounds.width / 2.0;
	let cy = bounds.y + bounds.height / 2.0;

	let dot_offsets: [f32; 3] = [0.0, DOT_SPACING, DOT_SPACING * 2.0];

	let mut centers = Vec::with_capacity(6);
	for side in [-1.0_f32, 1.0] {
		let base = side * DOT_GROUP_OFFSET;
		for &d in &dot_offsets {
			let offset = base + side * d;
			let (x, y) = match axis {
				Axis::Horizontal => (cx, cy + offset),
				Axis::Vertical => (cx + offset, cy),
			};
			centers.push((x, y));
		}
	}

	for &(x, y) in &centers {
		renderer.fill_quad(
			renderer::Quad {
				bounds: Rectangle {
					x: x - DOT_BOTTOM_RADIUS,
					y: y - DOT_BOTTOM_RADIUS,
					width: DOT_BOTTOM_RADIUS * 2.0,
					height: DOT_BOTTOM_RADIUS * 2.0,
				},
				border: iced::Border {
					color: Color::TRANSPARENT,
					width: 0.0,
					radius: DOT_BOTTOM_RADIUS.into(),
				},
				..renderer::Quad::default()
			},
			style.dot_bottom_color,
		);
	}

	for &(x, y) in &centers {
		renderer.fill_quad(
			renderer::Quad {
				bounds: Rectangle {
					x: x - DOT_TOP_RADIUS,
					y: y - DOT_TOP_RADIUS,
					width: DOT_TOP_RADIUS * 2.0,
					height: DOT_TOP_RADIUS * 2.0,
				},
				border: iced::Border {
					color: Color::TRANSPARENT,
					width: 0.0,
					radius: DOT_TOP_RADIUS.into(),
				},
				..renderer::Quad::default()
			},
			style.dot_top_color,
		);
	}
}

#[derive(Debug, Clone, Copy)]
pub struct DragHandleZone {
	pub pane: PaneId,
	pub bounds: Rectangle,
}

impl DragHandleZone {
	pub fn new(pane: PaneId, pane_bounds: Rectangle) -> Self {
		Self {
			pane,
			bounds: Rectangle {
				x: pane_bounds.x,
				y: pane_bounds.y,
				width: pane_bounds.width,
				height: PANE_DRAG_HANDLE_THICKNESS,
			},
		}
	}

	pub fn contains(&self, point: Point) -> bool {
		self.bounds.contains(point)
	}
}

pub fn draw_drag_handle<Renderer>(
	renderer: &mut Renderer,
	zone: &DragHandleZone,
	style: &HandleStyle,
) where
	Renderer: renderer::Renderer,
{
	let bounds = zone.bounds;

	renderer.fill_quad(
		renderer::Quad {
			bounds,
			border: iced::Border {
				color: style.border_color,
				width: 0.5,
				radius: 0.0.into(),
			},
			..renderer::Quad::default()
		},
		style.background,
	);

	let cx = bounds.x + bounds.width / 2.0;
	let cy = bounds.y + bounds.height / 2.0;

	let offsets: [f32; 3] = [
		-GRIP_DOT_SPACING,
		0.0,
		GRIP_DOT_SPACING,
	];

	for &dx in &offsets {
		let x = cx + dx;
		let y = cy;

		renderer.fill_quad(
			renderer::Quad {
				bounds: Rectangle {
					x: x - DOT_BOTTOM_RADIUS,
					y: y - DOT_BOTTOM_RADIUS,
					width: DOT_BOTTOM_RADIUS * 2.0,
					height: DOT_BOTTOM_RADIUS * 2.0,
				},
				border: iced::Border {
					color: Color::TRANSPARENT,
					width: 0.0,
					radius: DOT_BOTTOM_RADIUS.into(),
				},
				..renderer::Quad::default()
			},
			style.dot_bottom_color,
		);

		renderer.fill_quad(
			renderer::Quad {
				bounds: Rectangle {
					x: x - DOT_TOP_RADIUS,
					y: y - DOT_TOP_RADIUS,
					width: DOT_TOP_RADIUS * 2.0,
					height: DOT_TOP_RADIUS * 2.0,
				},
				border: iced::Border {
					color: Color::TRANSPARENT,
					width: 0.0,
					radius: DOT_TOP_RADIUS.into(),
				},
				..renderer::Quad::default()
			},
			style.dot_top_color,
		);
	}
}

pub fn collapsed_strip_thickness() -> f32 {
	COLLAPSED_STRIP_THICKNESS
}

pub fn handle_thickness(style: &HandleStyle) -> f32 {
	style.thickness
}
