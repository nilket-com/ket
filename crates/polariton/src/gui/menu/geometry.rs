use iced::advanced::mouse;
use iced::advanced::text::{self, Paragraph};
use iced::{Pixels, Point, Rectangle, Size};

use super::{MenuItem, MenuRoot, MenuState};

pub const BAR_HEIGHT: f32 = 32.0;
pub const BAR_ITEM_PADDING_X: f32 = 12.0;
pub const BAR_ITEM_GAP: f32 = 4.0;
pub const PANEL_GAP: f32 = 2.0;
pub const PANEL_PADDING: f32 = 6.0;
pub const PANEL_ITEM_HEIGHT: f32 = 28.0;
pub const PANEL_SEPARATOR_HEIGHT: f32 = 8.0;
pub const PANEL_MIN_WIDTH: f32 = 180.0;
pub const LABEL_SIZE: Pixels = Pixels(16.0);
pub const PANEL_TEXT_OFFSET: f32 = 10.0;
pub const ARROW_GUTTER: f32 = 6.0;
pub const ARROW_TEXT_GAP: f32 = 12.0;

#[derive(Debug, Clone, Copy)]
pub(crate) struct RootGeometry<'a> {
	pub id: &'a str,
	pub label: &'a str,
	pub bounds: Rectangle,
}

#[derive(Debug)]
pub(crate) struct PanelGeometry<'a> {
	pub bounds: Rectangle,
	pub items: Vec<ItemGeometry<'a>>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ItemGeometry<'a> {
	pub depth: usize,
	pub bounds: Rectangle,
	pub kind: ItemKind<'a>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum ItemKind<'a> {
	Action { id: &'a str, label: &'a str },
	Submenu { id: &'a str, label: &'a str },
	Separator,
}

#[derive(Debug)]
pub(crate) struct MenuGeometry<'a> {
	pub roots: Vec<RootGeometry<'a>>,
	pub panels: Vec<PanelGeometry<'a>>,
	pub bar_bounds: Rectangle,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum Hit<'a> {
	Root(RootGeometry<'a>),
	Panel,
	PanelItem(ItemGeometry<'a>),
}

impl<'a> MenuGeometry<'a> {
	pub fn new<Renderer: text::Renderer<Font = iced::Font>>(
		roots: &'a [MenuRoot],
		state: &'a MenuState,
		_renderer: &Renderer,
		label_font: iced::Font,
		symbol_font: iced::Font,
		width: f32,
	) -> Self {
		let font = label_font;
		let line_height = text::LineHeight::default();

		let mut x = 0.0;
		let mut root_geometries = Vec::with_capacity(roots.len());

		for root in roots {
			let label_width = measure_label(root.label.as_str(), font, line_height);
			let item_width = label_width + BAR_ITEM_PADDING_X * 2.0;

			root_geometries.push(RootGeometry {
				id: root.id.as_str(),
				label: root.label.as_str(),
				bounds: Rectangle {
					x,
					y: 0.0,
					width: item_width,
					height: BAR_HEIGHT,
				},
			});

			x += item_width + BAR_ITEM_GAP;
		}

		let bar_bounds = Rectangle {
			x: 0.0,
			y: 0.0,
			width,
			height: BAR_HEIGHT,
		};

		let mut panels = Vec::new();

		if let Some(root_id) = state.open_root()
			&& let Some((root_index, root)) = roots
				.iter()
				.enumerate()
				.find(|(_, root)| root.id == root_id)
		{
			let anchor = root_geometries[root_index].bounds;
			let mut current_items = root.items.as_slice();
			let mut panel_x = anchor.x;
			let mut panel_y = BAR_HEIGHT + PANEL_GAP;

			for depth in 0..=state.open_path().len() {
				let panel = layout_panel(
					current_items,
					depth,
					font,
					symbol_font,
					line_height,
					Point::new(panel_x, panel_y),
				);

				let next_items = state.open_path().get(depth).and_then(|submenu_id| {
					panel.items.iter().find_map(|item| match item.kind {
						ItemKind::Submenu { id, .. } if id == *submenu_id => {
							let child = submenu_items(current_items, id)?;
							panel_x = item.bounds.x + item.bounds.width + PANEL_GAP;
							panel_y = item.bounds.y;
							Some(child)
						}
						_ => None,
					})
				});

				panels.push(panel);

				let Some(items) = next_items else {
					break;
				};

				current_items = items;
			}
		}

		Self {
			roots: root_geometries,
			panels,
			bar_bounds,
		}
	}

	pub fn with_origin(mut self, origin: Point) -> Self {
		self.bar_bounds.x += origin.x;
		self.bar_bounds.y += origin.y;

		for root in &mut self.roots {
			root.bounds.x += origin.x;
			root.bounds.y += origin.y;
		}

		for panel in &mut self.panels {
			panel.bounds.x += origin.x;
			panel.bounds.y += origin.y;

			for item in &mut panel.items {
				item.bounds.x += origin.x;
				item.bounds.y += origin.y;
			}
		}

		self
	}

	pub fn hit_test(&self, cursor: mouse::Cursor) -> Option<Hit<'a>> {
		for root in &self.roots {
			if cursor.is_over(root.bounds) {
				return Some(Hit::Root(*root));
			}
		}

		for panel in &self.panels {
			if cursor.is_over(panel.bounds) {
				for item in &panel.items {
					if cursor.is_over(item.bounds) {
						return Some(Hit::PanelItem(*item));
					}
				}

				return Some(Hit::Panel);
			}
		}

		None
	}

	pub fn contains(&self, cursor: mouse::Cursor) -> bool {
		cursor.is_over(self.bar_bounds)
			|| self.panels.iter().any(|panel| cursor.is_over(panel.bounds))
	}
}

pub(crate) fn root_by_id<'a>(roots: &'a [MenuRoot], id: &str) -> Option<&'a MenuRoot> {
	roots.iter().find(|root| root.id == id)
}

pub(crate) fn submenu_items<'a>(items: &'a [MenuItem], id: &str) -> Option<&'a [MenuItem]> {
	items.iter().find_map(|item| match item {
		MenuItem::Submenu {
			id: submenu_id,
			items,
			..
		} if submenu_id == id => Some(items.as_slice()),
		_ => None,
	})
}

fn layout_panel<'a>(
	items: &'a [MenuItem],
	depth: usize,
	font: iced::Font,
	symbol_font: iced::Font,
	line_height: text::LineHeight,
	origin: Point,
) -> PanelGeometry<'a> {
	let mut width = PANEL_MIN_WIDTH;

	for item in items {
		width = width.max(match item {
			MenuItem::Action { label, .. } => {
				measure_label(label.as_str(), font, line_height) + PANEL_TEXT_OFFSET * 2.0
			}
			MenuItem::Submenu { label, .. } => {
				measure_label(label.as_str(), font, line_height)
					+ PANEL_TEXT_OFFSET * 2.0
					+ ARROW_TEXT_GAP
					+ measure_label("▷", symbol_font, line_height)
					+ ARROW_GUTTER
			}
			MenuItem::Separator => continue,
		});
	}

	let height = items.iter().fold(PANEL_PADDING * 2.0, |height, item| {
		height
			+ match item {
				MenuItem::Separator => PANEL_SEPARATOR_HEIGHT,
				_ => PANEL_ITEM_HEIGHT,
			}
	});

	let mut y = origin.y + PANEL_PADDING;
	let mut geometries = Vec::with_capacity(items.len());

	for item in items {
		let item_height = match item {
			MenuItem::Separator => PANEL_SEPARATOR_HEIGHT,
			_ => PANEL_ITEM_HEIGHT,
		};

		let bounds = Rectangle {
			x: origin.x + PANEL_PADDING,
			y,
			width: width - PANEL_PADDING * 2.0,
			height: item_height,
		};

		let kind = match item {
			MenuItem::Action { id, label } => ItemKind::Action {
				id: id.as_str(),
				label: label.as_str(),
			},
			MenuItem::Submenu { id, label, .. } => ItemKind::Submenu {
				id: id.as_str(),
				label: label.as_str(),
			},
			MenuItem::Separator => ItemKind::Separator,
		};

		geometries.push(ItemGeometry {
			depth,
			bounds,
			kind,
		});

		y += item_height;
	}

	PanelGeometry {
		bounds: Rectangle {
			x: origin.x,
			y: origin.y,
			width,
			height,
		},
		items: geometries,
	}
}

fn measure_label(label: &str, font: iced::Font, line_height: text::LineHeight) -> f32 {
	let paragraph = <iced::Renderer as text::Renderer>::Paragraph::with_text(text::Text {
		content: label,
		bounds: Size::new(
			f32::INFINITY,
			f32::from(line_height.to_absolute(LABEL_SIZE)),
		),
		size: LABEL_SIZE,
		line_height,
		font,
		align_x: text::Alignment::Left,
		align_y: iced::alignment::Vertical::Center,
		shaping: text::Shaping::Basic,
		wrapping: text::Wrapping::None,
	});

	paragraph.min_width().ceil()
}
