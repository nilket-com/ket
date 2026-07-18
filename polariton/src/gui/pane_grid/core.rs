use iced::advanced::layout::{self, Layout, Node};
use iced::advanced::renderer;
use iced::advanced::text;
use iced::advanced::widget::{self, Tree, Widget};
use iced::advanced::{Clipboard, Shell};
use iced::mouse;
use iced::{Element, Event, Length, Point, Rectangle, Size, Vector};

use crate::gui::pane_grid::handle::{
	self, DragHandleZone, HandleAction, HandleZone,
	collapsed_strip_thickness, handle_thickness, PANE_DRAG_HANDLE_THICKNESS,
};
use crate::gui::pane_grid::state::{
	Action, Axis, CollapseState, DropEdge, DropTarget, MaximizeState, NodeId,
	PaneId, SplitId, State,
};
use crate::gui::pane_grid::style::{ChromeVisibility, Style};

const PANE_DRAG_DEADBAND: f32 = 10.0;

pub struct PaneGrid<'a, Message, Theme = iced::Theme, Renderer = iced::Renderer> {
	state: &'a State,
	content: Vec<(PaneId, Element<'a, Message, Theme, Renderer>)>,
	style: Style,
	on_action: Box<dyn Fn(Action) -> Message + 'a>,
}

impl<'a, Message, Theme, Renderer> PaneGrid<'a, Message, Theme, Renderer>
where
	Renderer: renderer::Renderer + text::Renderer<Font = iced::Font>,
{
	pub fn new(
		state: &'a State,
		content: Vec<(PaneId, Element<'a, Message, Theme, Renderer>)>,
		on_action: impl Fn(Action) -> Message + 'a,
	) -> Self {
		Self {
			state,
			content,
			style: Style::default(),
			on_action: Box::new(on_action),
		}
	}

	pub fn style(mut self, style: Style) -> Self {
		self.style = style;
		self
	}

	fn find_content(&self, pane: PaneId) -> Option<usize> {
		self.content.iter().position(|(id, _)| *id == pane)
	}
}

#[derive(Default)]
struct WidgetState {
	handle_zones: Vec<HandleZone>,
	drag_handle_zones: Vec<DragHandleZone>,
	pane_bounds: Vec<(PaneId, Rectangle)>,
	child_order: Vec<usize>,
	hovered_arrow: Option<(SplitId, bool)>,
	drop_target: Option<DropTarget>,
	cursor_inside: bool,
}

impl<Message, Theme, Renderer> Widget<Message, Theme, Renderer>
	for PaneGrid<'_, Message, Theme, Renderer>
where
	Renderer: renderer::Renderer + text::Renderer<Font = iced::Font>,
{
	fn size(&self) -> Size<Length> {
		Size::new(Length::Fill, Length::Fill)
	}

	fn tag(&self) -> widget::tree::Tag {
		widget::tree::Tag::of::<WidgetState>()
	}

	fn state(&self) -> widget::tree::State {
		widget::tree::State::new(WidgetState::default())
	}

	fn children(&self) -> Vec<widget::Tree> {
		self.content.iter().map(|(_, elem)| Tree::new(elem)).collect()
	}

	fn diff(&self, tree: &mut Tree) {
		tree.diff_children(
			&self.content.iter().map(|(_, e)| e).collect::<Vec<_>>(),
		);
	}

	fn layout(
		&mut self,
		tree: &mut Tree,
		renderer: &Renderer,
		limits: &layout::Limits,
	) -> Node {
		let size = limits.max();

		let mut handle_zones = Vec::new();
		let mut drag_handle_zones = Vec::new();
		let mut pane_bounds = Vec::new();
		let mut child_order: Vec<usize> = Vec::new();

		let children = match self.state.maximize() {
			MaximizeState::Maximized(pane) => {
				let mut children = Vec::new();
				if let Some(idx) = self.find_content(pane) {
					let full_bounds = Rectangle::new(Point::ORIGIN, size);
					pane_bounds.push((pane, full_bounds));
					drag_handle_zones.push(DragHandleZone::new(pane, full_bounds));

					let content_bounds = Size::new(
						size.width,
						size.height - PANE_DRAG_HANDLE_THICKNESS,
					);
					let child_node = self.content[idx].1.as_widget_mut().layout(
						&mut tree.children[idx],
						renderer,
						&layout::Limits::new(Size::ZERO, content_bounds),
					);
					let child_node = child_node.move_to(Point::new(
						0.0,
						PANE_DRAG_HANDLE_THICKNESS,
					));
					children.push(child_node);
					child_order.push(idx);
				}
				children
			}
			MaximizeState::None => {
				let mut children = Vec::new();
				if let Some(root) = self.state.root() {
					layout_node(
						&mut self.content,
						self.state,
						&self.style,
						&mut tree.children,
						renderer,
						root,
						Rectangle::new(Point::ORIGIN, size),
						&mut handle_zones,
						&mut drag_handle_zones,
						&mut pane_bounds,
						&mut child_order,
						&mut children,
					);
				}
				children
			}
		};

		let widget_state = tree.state.downcast_mut::<WidgetState>();
		widget_state.handle_zones = handle_zones;
		widget_state.drag_handle_zones = drag_handle_zones;
		widget_state.pane_bounds = pane_bounds;
		widget_state.child_order = child_order;

		Node::with_children(size, children)
	}

	fn draw(
		&self,
		tree: &Tree,
		renderer: &mut Renderer,
		theme: &Theme,
		style: &renderer::Style,
		layout: Layout<'_>,
		cursor: mouse::Cursor,
		viewport: &Rectangle,
	) {
		let widget_state = tree.state.downcast_ref::<WidgetState>();
		let origin = layout.bounds().position();
		let offset = Vector::new(origin.x, origin.y);

		let show_chrome = match self.style.chrome {
			ChromeVisibility::Always => true,
			ChromeVisibility::OnHover => cursor
				.position()
				.is_some_and(|pos| layout.bounds().contains(pos)),
		};

		if show_chrome {
			for &(_, bounds) in &widget_state.pane_bounds {
				renderer.fill_quad(
					renderer::Quad {
						bounds: bounds + offset,
						border: iced::Border {
							color: self.style.pane.border_color,
							width: self.style.pane.border_width,
							radius: 0.0.into(),
						},
						..renderer::Quad::default()
					},
					self.style.pane.background,
				);
			}
		}

		for (child_idx, child_layout) in layout.children().enumerate() {
			if let Some(&content_idx) = widget_state.child_order.get(child_idx) {
				self.content[content_idx].1.as_widget().draw(
					&tree.children[content_idx],
					renderer,
					theme,
					style,
					child_layout,
					cursor,
					viewport,
				);
			}
		}

		if show_chrome {
			for zone in &widget_state.drag_handle_zones {
				let offset_zone = DragHandleZone {
					bounds: zone.bounds + offset,
					..*zone
				};
				handle::draw_drag_handle(renderer, &offset_zone, &self.style.handle);
			}

			for zone in &widget_state.handle_zones {
				let offset_zone = HandleZone {
					bounds: zone.bounds + offset,
					first_arrow: zone.first_arrow + offset,
					second_arrow: zone.second_arrow + offset,
					..*zone
				};
				let hovered = widget_state.hovered_arrow
					.filter(|(sid, _)| *sid == zone.split_id)
					.map(|(_, is_first)| is_first);
				handle::draw_handle(renderer, &offset_zone, &self.style.handle, hovered);
			}
		}

		if let Some(drag) = self.state.pane_dragging() {
			let dist = ((drag.current.x - drag.origin.x).powi(2)
				+ (drag.current.y - drag.origin.y).powi(2))
			.sqrt();
			if dist > PANE_DRAG_DEADBAND {
				if let Some(target) = &widget_state.drop_target {
					if let Some(&(_, bounds)) = widget_state.pane_bounds
						.iter()
						.find(|(id, _)| *id == target.pane)
					{
						let overlay_bounds = drop_overlay_bounds(bounds, target.edge) + offset;
						renderer.fill_quad(
							renderer::Quad {
								bounds: overlay_bounds,
								border: iced::Border {
									color: self.style.handle.border_color,
									width: 1.0,
									radius: 0.0.into(),
								},
								..renderer::Quad::default()
							},
							self.style.drop_overlay_color,
						);
					}
				}
			}
		}
	}

	fn update(
		&mut self,
		tree: &mut Tree,
		event: &Event,
		layout: Layout<'_>,
		cursor: mouse::Cursor,
		renderer: &Renderer,
		clipboard: &mut dyn Clipboard,
		shell: &mut Shell<'_, Message>,
		viewport: &Rectangle,
	) {
		let widget_state = tree.state.downcast_mut::<WidgetState>();

		let child_order = widget_state.child_order.clone();
		for (child_idx, child_layout) in layout.children().enumerate() {
			if let Some(&content_idx) = child_order.get(child_idx) {
				self.content[content_idx].1.as_widget_mut().update(
					&mut tree.children[content_idx],
					event,
					child_layout,
					cursor,
					renderer,
					clipboard,
					shell,
					viewport,
				);
			}
		}

		let origin = layout.bounds().position();
		let widget_bounds = layout.bounds();

		if let Event::Mouse(mouse::Event::CursorMoved { .. }) = event {
			if self.style.chrome == ChromeVisibility::OnHover {
				let inside = cursor
					.position()
					.is_some_and(|pos| widget_bounds.contains(pos));
				if inside != widget_state.cursor_inside {
					widget_state.cursor_inside = inside;
					shell.request_redraw();
				}
			}
		}

		match event {
			Event::Mouse(mouse::Event::ButtonPressed(mouse::Button::Left)) => {
				if let Some(pos) = cursor.position() {
					let local = Point::new(pos.x - origin.x, pos.y - origin.y);
					for zone in &widget_state.handle_zones {
						if let Some(action) = zone.hit_test(local) {
							let msg = match action {
								HandleAction::CollapseFirst(id) => {
									Action::CollapseFirst(id)
								}
								HandleAction::CollapseSecond(id) => {
									Action::CollapseSecond(id)
								}
								HandleAction::Expand(id) => {
									Action::Expand(id)
								}
							};
							shell.publish((self.on_action)(msg));
							widget_state.hovered_arrow = None;
							return;
						}
					}
					for zone in &widget_state.drag_handle_zones {
						if zone.contains(local) {
							shell.publish((self.on_action)(
								Action::PaneDragStarted(zone.pane, pos),
							));
							return;
						}
					}
					for zone in &widget_state.handle_zones {
						if zone.contains(local) {
							shell.publish((self.on_action)(
								Action::DragStarted(zone.split_id),
							));
							return;
						}
					}
				}
			}
			Event::Mouse(mouse::Event::CursorMoved { .. }) => {
				if let Some(pos) = cursor.position() {
					let local = Point::new(pos.x - origin.x, pos.y - origin.y);
					if self.state.pane_dragging().is_some() {
						shell.publish((self.on_action)(
							Action::PaneDragMoved(pos),
						));
						let source = self.state.pane_dragging().unwrap().pane;
						let mut new_target = None;
						for &(pane, bounds) in &widget_state.pane_bounds {
							if pane == source {
								continue;
							}
							if bounds.contains(local) {
								let edge = compute_drop_edge(bounds, local);
								new_target = Some(DropTarget { pane, edge });
								break;
							}
						}
						widget_state.drop_target = new_target;
						return;
					}
					if let Some(split_id) = self.state.dragging() {
						if let Some(zone) = widget_state.handle_zones
							.iter()
							.find(|z| z.split_id == split_id)
						{
							let ht = handle_thickness(&self.style.handle);
							let ratio = match zone.axis {
								Axis::Horizontal => {
									let usable = zone.parent_region.width - ht;
									if usable > 0.0 {
										(local.x - zone.parent_region.x - ht / 2.0) / usable
									} else {
										0.5
									}
								}
								Axis::Vertical => {
									let usable = zone.parent_region.height - ht;
									if usable > 0.0 {
										(local.y - zone.parent_region.y - ht / 2.0) / usable
									} else {
										0.5
									}
								}
							};
							let ratio = ratio.clamp(0.05, 0.95);
							shell.publish((self.on_action)(
								Action::DragMoved(split_id, ratio),
							));
						}
						return;
					}
					let mut new_hover = None;
					for zone in &widget_state.handle_zones {
						if zone.first_arrow.contains(local) {
							new_hover = Some((zone.split_id, true));
							break;
						}
						if zone.second_arrow.contains(local) {
							new_hover = Some((zone.split_id, false));
							break;
						}
					}
					widget_state.hovered_arrow = new_hover;
				}
			}
			Event::Mouse(mouse::Event::ButtonReleased(mouse::Button::Left)) => {
				if self.state.pane_dragging().is_some() {
					if let Some(target) = widget_state.drop_target.take() {
						shell.publish((self.on_action)(
							Action::PaneDragDropped(target),
						));
					} else {
						shell.publish((self.on_action)(
							Action::PaneDragCancelled,
						));
					}
					return;
				}
				if self.state.dragging().is_some() {
					shell.publish((self.on_action)(Action::DragEnded));
				}
			}
			_ => {}
		}
	}

	fn mouse_interaction(
		&self,
		tree: &Tree,
		layout: Layout<'_>,
		cursor: mouse::Cursor,
		viewport: &Rectangle,
		renderer: &Renderer,
	) -> mouse::Interaction {
		let widget_state = tree.state.downcast_ref::<WidgetState>();

		if self.state.pane_dragging().is_some() {
			return mouse::Interaction::Grabbing;
		}

		if self.state.dragging().is_some() {
			return mouse::Interaction::Grabbing;
		}

		if let Some(pos) = cursor.position() {
			let origin = layout.bounds().position();
			let local = Point::new(pos.x - origin.x, pos.y - origin.y);
			for zone in &widget_state.drag_handle_zones {
				if zone.contains(local) {
					return mouse::Interaction::Grab;
				}
			}
			for zone in &widget_state.handle_zones {
				if zone.first_arrow.contains(local) || zone.second_arrow.contains(local) {
					return mouse::Interaction::Pointer;
				}
				if zone.contains(local) {
					return match zone.axis {
						Axis::Horizontal => mouse::Interaction::ResizingHorizontally,
						Axis::Vertical => mouse::Interaction::ResizingVertically,
					};
				}
			}
		}

		for (child_idx, child_layout) in layout.children().enumerate() {
			if let Some(&content_idx) = widget_state.child_order.get(child_idx) {
				let interaction = self.content[content_idx].1.as_widget().mouse_interaction(
					&tree.children[content_idx],
					child_layout,
					cursor,
					viewport,
					renderer,
				);
				if interaction != mouse::Interaction::None {
					return interaction;
				}
			}
		}

		mouse::Interaction::None
	}
}

fn layout_node<Message, Theme, Renderer>(
	content: &mut [(PaneId, Element<'_, Message, Theme, Renderer>)],
	state: &State,
	style: &Style,
	tree_children: &mut [Tree],
	renderer: &Renderer,
	node: NodeId,
	region: Rectangle,
	handle_zones: &mut Vec<HandleZone>,
	drag_handle_zones: &mut Vec<DragHandleZone>,
	pane_bounds: &mut Vec<(PaneId, Rectangle)>,
	child_order: &mut Vec<usize>,
	children: &mut Vec<Node>,
) where
	Renderer: renderer::Renderer + text::Renderer<Font = iced::Font>,
{
	match node {
		NodeId::Pane(pane) => {
			let idx = content.iter().position(|(id, _)| *id == pane);
			if let Some(idx) = idx {
				pane_bounds.push((pane, region));
				drag_handle_zones.push(DragHandleZone::new(pane, region));

				let content_height = (region.height - PANE_DRAG_HANDLE_THICKNESS).max(0.0);
				let limits = layout::Limits::new(
					Size::ZERO,
					Size::new(region.width, content_height),
				);
				let child_node = content[idx].1.as_widget_mut().layout(
					&mut tree_children[idx],
					renderer,
					&limits,
				);
				let child_node = child_node.move_to(Point::new(
					region.x,
					region.y + PANE_DRAG_HANDLE_THICKNESS,
				));
				child_order.push(idx);
				children.push(child_node);
			}
		}
		NodeId::Split(split_id) => {
			if let Some(split) = state.get_split(split_id) {
				let axis = split.axis;
				let ratio = split.ratio;
				let collapse = split.collapse;
				let ht = handle_thickness(&style.handle);

				let (first_region, handle_region, second_region) =
					split_regions(region, axis, ratio, collapse, ht);

				let first = split.first;
				let second = split.second;

				if collapse != CollapseState::FirstCollapsed {
					layout_node(
						content, state, style, tree_children, renderer,
						first, first_region, handle_zones, drag_handle_zones,
						pane_bounds, child_order, children,
					);
				}

				if collapse != CollapseState::SecondCollapsed {
					layout_node(
						content, state, style, tree_children, renderer,
						second, second_region, handle_zones, drag_handle_zones,
						pane_bounds, child_order, children,
					);
				}

				handle_zones.push(HandleZone::new(
					split_id,
					handle_region,
					axis,
					collapse,
					region,
				));
			}
		}
	}
}

fn split_regions(
	region: Rectangle,
	axis: Axis,
	ratio: f32,
	collapse: CollapseState,
	handle_thickness: f32,
) -> (Rectangle, Rectangle, Rectangle) {
	let strip = collapsed_strip_thickness();

	match collapse {
		CollapseState::FirstCollapsed => {
			let handle = match axis {
				Axis::Horizontal => Rectangle {
					x: region.x,
					y: region.y,
					width: strip,
					height: region.height,
				},
				Axis::Vertical => Rectangle {
					x: region.x,
					y: region.y,
					width: region.width,
					height: strip,
				},
			};
			let first = Rectangle {
				x: region.x,
				y: region.y,
				width: 0.0,
				height: 0.0,
			};
			let second = match axis {
				Axis::Horizontal => Rectangle {
					x: region.x + strip,
					y: region.y,
					width: region.width - strip,
					height: region.height,
				},
				Axis::Vertical => Rectangle {
					x: region.x,
					y: region.y + strip,
					width: region.width,
					height: region.height - strip,
				},
			};
			(first, handle, second)
		}
		CollapseState::SecondCollapsed => {
			let second = Rectangle {
				x: region.x,
				y: region.y,
				width: 0.0,
				height: 0.0,
			};
			let (handle, first) = match axis {
				Axis::Horizontal => (
					Rectangle {
						x: region.x + region.width - strip,
						y: region.y,
						width: strip,
						height: region.height,
					},
					Rectangle {
						x: region.x,
						y: region.y,
						width: region.width - strip,
						height: region.height,
					},
				),
				Axis::Vertical => (
					Rectangle {
						x: region.x,
						y: region.y + region.height - strip,
						width: region.width,
						height: strip,
					},
					Rectangle {
						x: region.x,
						y: region.y,
						width: region.width,
						height: region.height - strip,
					},
				),
			};
			(first, handle, second)
		}
		CollapseState::Expanded => {
			match axis {
				Axis::Horizontal => {
					let first_w = (region.width - handle_thickness) * ratio;
					let first = Rectangle {
						x: region.x,
						y: region.y,
						width: first_w,
						height: region.height,
					};
					let handle = Rectangle {
						x: region.x + first_w,
						y: region.y,
						width: handle_thickness,
						height: region.height,
					};
					let second = Rectangle {
						x: region.x + first_w + handle_thickness,
						y: region.y,
						width: region.width - first_w - handle_thickness,
						height: region.height,
					};
					(first, handle, second)
				}
				Axis::Vertical => {
					let first_h = (region.height - handle_thickness) * ratio;
					let first = Rectangle {
						x: region.x,
						y: region.y,
						width: region.width,
						height: first_h,
					};
					let handle = Rectangle {
						x: region.x,
						y: region.y + first_h,
						width: region.width,
						height: handle_thickness,
					};
					let second = Rectangle {
						x: region.x,
						y: region.y + first_h + handle_thickness,
						width: region.width,
						height: region.height - first_h - handle_thickness,
					};
					(first, handle, second)
				}
			}
		}
	}
}

impl<'a, Message, Theme, Renderer> From<PaneGrid<'a, Message, Theme, Renderer>>
	for Element<'a, Message, Theme, Renderer>
where
	Message: 'a,
	Theme: 'a,
	Renderer: renderer::Renderer + text::Renderer<Font = iced::Font> + 'a,
{
	fn from(pane_grid: PaneGrid<'a, Message, Theme, Renderer>) -> Self {
		Element::new(pane_grid)
	}
}


fn compute_drop_edge(bounds: Rectangle, pos: Point) -> DropEdge {
	let rel_x = (pos.x - bounds.x) / bounds.width;
	let rel_y = (pos.y - bounds.y) / bounds.height;

	if rel_x > 0.3 && rel_x < 0.7 && rel_y > 0.3 && rel_y < 0.7 {
		return DropEdge::Center;
	}

	let dist_left = rel_x;
	let dist_right = 1.0 - rel_x;
	let dist_top = rel_y;
	let dist_bottom = 1.0 - rel_y;

	let min = dist_left.min(dist_right).min(dist_top).min(dist_bottom);
	if min == dist_left {
		DropEdge::Left
	} else if min == dist_right {
		DropEdge::Right
	} else if min == dist_top {
		DropEdge::Top
	} else {
		DropEdge::Bottom
	}
}

fn drop_overlay_bounds(bounds: Rectangle, edge: DropEdge) -> Rectangle {
	match edge {
		DropEdge::Center => bounds,
		DropEdge::Left => Rectangle {
			x: bounds.x,
			y: bounds.y,
			width: bounds.width / 2.0,
			height: bounds.height,
		},
		DropEdge::Right => Rectangle {
			x: bounds.x + bounds.width / 2.0,
			y: bounds.y,
			width: bounds.width / 2.0,
			height: bounds.height,
		},
		DropEdge::Top => Rectangle {
			x: bounds.x,
			y: bounds.y,
			width: bounds.width,
			height: bounds.height / 2.0,
		},
		DropEdge::Bottom => Rectangle {
			x: bounds.x,
			y: bounds.y + bounds.height / 2.0,
			width: bounds.width,
			height: bounds.height / 2.0,
		},
	}
}
