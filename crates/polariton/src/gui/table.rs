use crate::coordinate::{Content, Pt, Rect, Widget as WidgetSpace, Window};
use crate::gui::table_skin::{TableRgba, TableSkin};
use iced::{
	Color, Element, Event,
	Length::{self, Fill},
	Pixels, Point, Rectangle, Size,
	advanced::{
		Clipboard, Layout, Shell, Widget,
		layout::{Limits, Node},
		mouse::{self, Cursor, Interaction, ScrollDelta},
		renderer::{self, Style},
		text::{self, Renderer as TextRenderer, Text},
		widget::{Tree, tree},
	},
	alignment::{Horizontal, Vertical},
	border, keyboard,
};
use polars::{
	datatypes::AnyValue,
	frame::{DataFrame, column::Column},
};

const ROW_HEIGHT: f32 = 28.0;
const HEADER_HEIGHT: f32 = 32.0;
const HEADER_HEIGHT_WITH_TYPES: f32 = 48.0;
const TYPE_LABEL_FONT_SIZE: f32 = 13.0;
const CELL_PADDING_X: f32 = 8.0;
const FONT_SIZE: f32 = 14.0;
/// Estimated glyph advance as a fraction of the font size. DejaVu Sans Mono's
/// true advance is 0.602 em and headers render bold, so the previous 0.6
/// estimate came out a fraction of a pixel narrow whenever the header was the
/// column's widest string — which wrapped it ("margin" → "margi\nn"). The
/// slack covers the advance plus shaping differences between this estimate
/// and the rendered text.
const CHAR_ADVANCE: f32 = 0.65;
const MIN_COL_WIDTH: f32 = 28.0;
const MAX_COL_WIDTH: f32 = 320.0;
const V_SCROLLBAR_WIDTH: f32 = 12.0;
const H_SCROLLBAR_HEIGHT: f32 = 12.0;
const COL_RESIZE_GRAB_ZONE: f32 = 4.0;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ColumnAlignment {
	Left,
	Right,
}

impl ColumnAlignment {
	fn horizontal(self) -> Horizontal {
		match self {
			Self::Left => Horizontal::Left,
			Self::Right => Horizontal::Right,
		}
	}
}

fn text_bounds(cell_x: f32, cell_y: f32, cell_width: f32, cell_height: f32) -> Rectangle {
	Rectangle {
		x: cell_x + CELL_PADDING_X,
		y: cell_y,
		width: (cell_width - CELL_PADDING_X * 2.0).max(0.0),
		height: cell_height,
	}
}

fn clamp_horizontal_scroll(offset: f64, total_width: f32, viewport_width: f32) -> f64 {
	let max_scroll = (total_width - viewport_width).max(0.0) as f64;
	offset.clamp(0.0, max_scroll)
}

/// A scroll action requested from outside the widget — currently keyboard
/// input routed from a focused host panel.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum TableScroll {
	LineUp,
	LineDown,
	PageUp,
	PageDown,
	Top,
	Bottom,
}

pub struct Table<'a> {
	data_frame: &'a DataFrame,
	row_offset: usize,
	skin: TableSkin,
	/// `(sequence, action)`. The widget applies an action only when the
	/// sequence advances, so a value that stays set across frames does not
	/// override the user's own scrolling.
	scroll_request: Option<(u64, TableScroll)>,
	col_width: Option<f32>,
	show_column_types: bool,
	/// Caller-owned column widths that outlive this widget's tree state. The
	/// tree state dies whenever the panel content swaps variants (a rerun
	/// passes through `Running`), so user resizes written only into
	/// `TableState` were lost on every execution. Widths are seeded from the
	/// store when the state is (re)built and written back when a resize drag
	/// ends. A length mismatch (schema change) ignores the store and
	/// re-measures.
	width_store: Option<&'a std::cell::RefCell<Option<Vec<f32>>>>,
}

impl<'a> Table<'a> {
	pub fn new(data_frame: &'a DataFrame, row_offset: usize, skin: TableSkin) -> Self {
		Self {
			data_frame,
			row_offset,
			skin,
			scroll_request: None,
			col_width: None,
			show_column_types: false,
			width_store: None,
		}
	}

	pub fn width_store(mut self, store: Option<&'a std::cell::RefCell<Option<Vec<f32>>>>) -> Self {
		self.width_store = store;
		self
	}

	pub fn col_width(mut self, width: f32) -> Self {
		self.col_width = Some(width);
		self
	}

	pub fn show_column_types(mut self, show: bool) -> Self {
		self.show_column_types = show;
		self
	}

	/// Drive scrolling from outside the widget. Keyboard input reaches a host
	/// panel only after Baryon has declined it, so this never competes with
	/// workspace bindings.
	pub fn scroll_request(mut self, request: Option<(u64, TableScroll)>) -> Self {
		self.scroll_request = request;
		self
	}

	fn apply_scroll_request(&self, state: &mut TableState, bounds: Rect<Window>) {
		let Some((sequence, action)) = self.scroll_request else {
			return;
		};
		if sequence == state.last_scroll_sequence {
			return;
		}
		state.last_scroll_sequence = sequence;
		let viewport_height = (bounds.height() - self.header_height()).max(0.0) as f64;
		let max_scroll = (self.total_content_height() as f64 - viewport_height).max(0.0);
		let page = viewport_height.max(ROW_HEIGHT as f64);
		let target = match action {
			TableScroll::LineUp => state.v_scroll_offset - ROW_HEIGHT as f64,
			TableScroll::LineDown => state.v_scroll_offset + ROW_HEIGHT as f64,
			TableScroll::PageUp => state.v_scroll_offset - page,
			TableScroll::PageDown => state.v_scroll_offset + page,
			TableScroll::Top => 0.0,
			TableScroll::Bottom => max_scroll,
		};
		state.v_scroll_offset = target.clamp(0.0, max_scroll);
	}

	fn header_height(&self) -> f32 {
		if self.show_column_types {
			HEADER_HEIGHT_WITH_TYPES
		} else {
			HEADER_HEIGHT
		}
	}

	fn col_count(&self) -> usize {
		self.data_frame.width()
	}

	fn total_row_count(&self) -> usize {
		self.data_frame.height()
	}

	fn measure_col_width(&self, col_idx: usize) -> f32 {
		if let Some(width) = self.col_width {
			return width.clamp(MIN_COL_WIDTH, MAX_COL_WIDTH);
		}
		let col_name = self
			.data_frame
			.get_column_names()
			.get(col_idx)
			.map(|s| s.as_str())
			.unwrap_or("");
		let sample_rows = self.data_frame.height().min(100);
		let max_content_chars = (0..sample_rows)
			.map(|row| self.cell_str(col_idx, row).len())
			.max()
			.unwrap_or(0);
		let type_label_chars = self
			.show_column_types
			.then(|| {
				self.data_frame
					.columns()
					.get(col_idx)
					.map(|column| column.dtype().to_string().len())
					.unwrap_or(0)
			})
			.unwrap_or(0);
		let max_chars = max_content_chars.max(col_name.len()).max(type_label_chars);
		let text_width = max_chars as f32 * FONT_SIZE * CHAR_ADVANCE;
		(text_width + CELL_PADDING_X * 2.0).clamp(MIN_COL_WIDTH, MAX_COL_WIDTH)
	}

	fn col_widths<'s>(&self, state: &'s mut TableState, _viewport_width: f32) -> &'s [f32] {
		let col_count = self.col_count();
		if state.col_widths.len() != col_count {
			let seeded = self.width_store.and_then(|store| {
				store
					.borrow()
					.as_ref()
					.filter(|widths| widths.len() == col_count)
					.cloned()
			});
			state.col_widths = seeded
				.unwrap_or_else(|| (0..col_count).map(|i| self.measure_col_width(i)).collect());
		}
		&state.col_widths
	}

	fn column_alignment(&self, col_idx: usize) -> ColumnAlignment {
		if self
			.data_frame
			.columns()
			.get(col_idx)
			.is_some_and(|column| column.dtype().is_numeric())
		{
			ColumnAlignment::Right
		} else {
			ColumnAlignment::Left
		}
	}

	fn col_widths_ref<'s>(&self, state: &'s TableState) -> &'s [f32] {
		&state.col_widths
	}

	fn total_content_width(&self, state: &TableState) -> f32 {
		state.col_widths.iter().sum()
	}

	fn total_content_height(&self) -> f32 {
		self.header_height() + self.total_row_count() as f32 * ROW_HEIGHT
	}

	fn loaded_row_count(&self) -> usize {
		self.data_frame.height()
	}

	fn cell_str(&self, col_idx: usize, row_idx: usize) -> String {
		let series: &Column = match self.data_frame.columns().get(col_idx) {
			Some(s) => s,
			None => return String::new(),
		};
		if row_idx >= series.len() {
			return String::new();
		}
		match series.get(row_idx) {
			Ok(AnyValue::Null) | Err(_) => String::new(),
			Ok(AnyValue::String(s)) => s.to_string(),
			Ok(AnyValue::StringOwned(s)) => s.to_string(),
			Ok(v) => format!("{v}"),
		}
	}

	fn col_left_edges(&self, state: &TableState) -> Vec<f32> {
		let mut edges = Vec::with_capacity(state.col_widths.len());
		let mut x = 0.0f32;
		for &w in &state.col_widths {
			edges.push(x);
			x += w;
		}
		edges
	}

	fn row_num_width(&self, bounds: Rect<Window>, v_scroll: f64) -> f32 {
		let first_visible = (v_scroll / ROW_HEIGHT as f64).floor() as usize;
		let visible_count =
			((bounds.height() - self.header_height()) / ROW_HEIGHT).ceil() as usize + 1;
		let max_idx = self.row_offset + first_visible + visible_count + 1;
		let digits = if max_idx > 0 {
			(max_idx as f64).log10().floor() as f32 + 1.0
		} else {
			1.0
		};
		(digits * FONT_SIZE * 0.6 + CELL_PADDING_X * 2.0).max(MIN_COL_WIDTH)
	}

	fn divider_at_cursor(
		&self,
		state: &TableState,
		bounds: Rect<Window>,
		cursor: Pt<Window>,
		row_num_w: f32,
	) -> Option<usize> {
		if cursor.y() < bounds.y() || cursor.y() > bounds.y() + self.header_height() {
			return None;
		}
		if cursor.x() < bounds.x() + row_num_w {
			return None;
		}
		let content_x = cursor.x() - (bounds.x() + row_num_w) + state.h_scroll_offset as f32;
		let edges = self.col_left_edges(state);
		for (i, &left) in edges.iter().enumerate() {
			let divider_x = left + state.col_widths[i];
			if (content_x - divider_x).abs() <= COL_RESIZE_GRAB_ZONE {
				return Some(i);
			}
		}
		None
	}

	fn h_scrollbar_thumb_rect(
		&self,
		bounds: Rect<Window>,
		h_scroll_offset: f64,
		state: &TableState,
		row_num_w: f32,
	) -> Rect<Window> {
		let total_w = self.total_content_width(state);
		let track_w = bounds.width() - V_SCROLLBAR_WIDTH - row_num_w;
		let thumb_w = (track_w * (track_w / total_w.max(1.0))).max(20.0);
		let max_scroll = (total_w - track_w).max(0.0);
		let thumb_x = bounds.x()
			+ row_num_w
			+ if max_scroll > 0.0 {
				h_scroll_offset as f32 / max_scroll * (track_w - thumb_w)
			} else {
				0.0
			};
		bounds.region(
			thumb_x,
			bounds.y() + bounds.height() - H_SCROLLBAR_HEIGHT + 2.0,
			thumb_w,
			H_SCROLLBAR_HEIGHT - 4.0,
		)
	}

	fn hit_test_cell(
		&self,
		state: &TableState,
		bounds: Rect<Window>,
		pos: Pt<Window>,
		row_num_w: f32,
	) -> Option<(usize, usize)> {
		let header_h = self.header_height();
		let data_top = bounds.y() + header_h;
		let data_bottom = bounds.y() + bounds.height() - H_SCROLLBAR_HEIGHT;
		let data_left = bounds.x() + row_num_w;
		let data_right = bounds.x() + bounds.width() - V_SCROLLBAR_WIDTH;
		if pos.y() < data_top
			|| pos.y() >= data_bottom
			|| pos.x() < data_left
			|| pos.x() >= data_right
		{
			return None;
		}
		let widget_bounds = Rect::<WidgetSpace>::from_iced_frame(bounds.into_iced());
		let widget_pos = Pt::<WidgetSpace>::from_window(pos, bounds);
		let data_origin = widget_bounds.origin().offset(row_num_w, header_h);
		let content = Pt::<Content>::from_widget(
			widget_pos,
			data_origin,
			state.h_scroll_offset as f32,
			state.v_scroll_offset as f32,
		);
		let row_idx = (content.y() / ROW_HEIGHT).floor() as usize;
		if row_idx >= self.loaded_row_count() {
			return None;
		}
		let mut x_acc = 0.0f32;
		for (col_idx, &w) in state.col_widths.iter().enumerate() {
			if content.x() >= x_acc && content.x() < x_acc + w {
				return Some((col_idx, row_idx));
			}
			x_acc += w;
		}
		None
	}

	fn v_scrollbar_thumb_rect(&self, bounds: Rect<Window>, v_scroll_offset: f64) -> Rect<Window> {
		let total_h = self.total_content_height();
		let track_h = bounds.height() - self.header_height() - H_SCROLLBAR_HEIGHT;
		let thumb_h = (track_h * (track_h / (total_h - self.header_height()).max(1.0))).max(20.0);
		let max_scroll = (total_h - bounds.height() + H_SCROLLBAR_HEIGHT).max(0.0);
		let thumb_y = bounds.y()
			+ self.header_height()
			+ if max_scroll > 0.0 {
				v_scroll_offset as f32 / max_scroll * (track_h - thumb_h)
			} else {
				0.0
			};
		bounds.region(
			bounds.x() + bounds.width() - V_SCROLLBAR_WIDTH - 2.0,
			thumb_y,
			V_SCROLLBAR_WIDTH - 4.0,
			thumb_h,
		)
	}
}

fn skin_color(color: TableRgba) -> Color {
	Color::from_rgba8(color.red(), color.green(), color.blue(), color.alpha())
}

struct TableSelection {
	anchor: (usize, usize),
	active: (usize, usize),
}

impl TableSelection {
	fn range(&self) -> (usize, usize, usize, usize) {
		let min_col = self.anchor.0.min(self.active.0);
		let max_col = self.anchor.0.max(self.active.0);
		let min_row = self.anchor.1.min(self.active.1);
		let max_row = self.anchor.1.max(self.active.1);
		(min_col, max_col, min_row, max_row)
	}

	fn contains(&self, col: usize, row: usize) -> bool {
		let (min_col, max_col, min_row, max_row) = self.range();
		col >= min_col && col <= max_col && row >= min_row && row <= max_row
	}
}

#[derive(Default)]
pub struct TableState {
	col_widths: Vec<f32>,
	resizing_col: Option<usize>,
	resize_drag_start_x: f32,
	resize_drag_start_width: f32,
	h_drag_start_offset: f64,
	h_drag_start_x: f32,
	h_dragging_scrollbar: bool,
	h_scroll_offset: f64,
	v_drag_start_offset: f64,
	v_drag_start_y: f32,
	v_dragging_scrollbar: bool,
	v_scroll_offset: f64,
	last_scroll_sequence: u64,
	selection: Option<TableSelection>,
	is_selecting: bool,
	modifiers: keyboard::Modifiers,
}

impl<Message, Theme, Renderer> Widget<Message, Theme, Renderer> for Table<'_>
where
	Renderer: renderer::Renderer + TextRenderer<Font = iced::Font>,
{
	fn tag(&self) -> tree::Tag {
		tree::Tag::of::<TableState>()
	}

	fn state(&self) -> tree::State {
		tree::State::new(TableState::default())
	}

	fn size(&self) -> Size<Length> {
		Size {
			width: Fill,
			height: Fill,
		}
	}

	fn layout(&mut self, _tree: &mut Tree, _renderer: &Renderer, limits: &Limits) -> Node {
		Node::new(limits.max())
	}

	fn mouse_interaction(
		&self,
		tree: &Tree,
		layout: Layout<'_>,
		cursor: Cursor,
		_viewport: &Rectangle,
		_renderer: &Renderer,
	) -> Interaction {
		let state = tree.state.downcast_ref::<TableState>();
		let bounds = Rect::<Window>::from_iced_layout(layout.bounds());
		let row_num_w = self.row_num_width(bounds, state.v_scroll_offset);
		if state.resizing_col.is_some() {
			return Interaction::ResizingHorizontally;
		}
		if let Some(pos) = cursor.position().map(Pt::<Window>::from_iced_event)
			&& self
				.divider_at_cursor(state, bounds, pos, row_num_w)
				.is_some()
		{
			return Interaction::ResizingHorizontally;
		}
		Interaction::default()
	}

	fn update(
		&mut self,
		tree: &mut Tree,
		event: &Event,
		layout: Layout<'_>,
		cursor: Cursor,
		_renderer: &Renderer,
		_clipboard: &mut dyn Clipboard,
		shell: &mut Shell<'_, Message>,
		_viewport: &Rectangle,
	) {
		let bounds = Rect::<Window>::from_iced_layout(layout.bounds());
		let state = tree.state.downcast_mut::<TableState>();
		self.apply_scroll_request(state, bounds);
		let row_num_w = self.row_num_width(bounds, state.v_scroll_offset);
		let viewport_w = bounds.width() - V_SCROLLBAR_WIDTH - row_num_w;
		self.col_widths(state, viewport_w);
		let total_h = self.total_content_height();
		let total_w = self.total_content_width(state);
		let viewport_h = bounds.height() - H_SCROLLBAR_HEIGHT;
		let max_v_scroll = (total_h - viewport_h).max(0.0) as f64;
		let max_h_scroll = (total_w - viewport_w).max(0.0) as f64;
		state.h_scroll_offset = clamp_horizontal_scroll(state.h_scroll_offset, total_w, viewport_w);
		let v_thumb = self.v_scrollbar_thumb_rect(bounds, state.v_scroll_offset);
		let h_thumb = self.h_scrollbar_thumb_rect(bounds, state.h_scroll_offset, state, row_num_w);
		match event {
			Event::Mouse(mouse::Event::ButtonPressed(mouse::Button::Left)) => {
				if let Some(pos) = cursor.position().map(Pt::<Window>::from_iced_event) {
					if let Some(col_idx) = self.divider_at_cursor(state, bounds, pos, row_num_w) {
						state.resizing_col = Some(col_idx);
						state.resize_drag_start_x = pos.x();
						state.resize_drag_start_width = state.col_widths[col_idx];
						shell.request_redraw();
						return;
					}
					if cursor.is_over(v_thumb.into_iced()) {
						state.v_dragging_scrollbar = true;
						state.v_drag_start_y = pos.y();
						state.v_drag_start_offset = state.v_scroll_offset;
						shell.request_redraw();
					} else if cursor.is_over(h_thumb.into_iced()) {
						state.h_dragging_scrollbar = true;
						state.h_drag_start_x = pos.x();
						state.h_drag_start_offset = state.h_scroll_offset;
						shell.request_redraw();
					} else if let Some(cell) = self.hit_test_cell(state, bounds, pos, row_num_w) {
						let extend = state.modifiers.shift();
						if extend && let Some(sel) = &mut state.selection {
							sel.active = cell;
						} else {
							state.selection = Some(TableSelection {
								anchor: cell,
								active: cell,
							});
						}
						state.is_selecting = true;
						shell.request_redraw();
					} else if state.selection.is_some() {
						state.selection = None;
						shell.request_redraw();
					}
				}
			}
			Event::Mouse(mouse::Event::CursorMoved {
				position: iced_position,
			}) => {
				let position = Pt::<Window>::from_iced_event(*iced_position);
				if let Some(col_idx) = state.resizing_col {
					let delta = position.x() - state.resize_drag_start_x;
					state.col_widths[col_idx] =
						(state.resize_drag_start_width + delta).max(MIN_COL_WIDTH);
					let new_total_w = self.total_content_width(state);
					state.h_scroll_offset =
						clamp_horizontal_scroll(state.h_scroll_offset, new_total_w, viewport_w);
					shell.request_redraw();
				} else if state.v_dragging_scrollbar {
					let drag_delta = position.y() - state.v_drag_start_y;
					let track_h = bounds.height() - self.header_height() - H_SCROLLBAR_HEIGHT;
					let thumb_h = v_thumb.height();
					let scroll_ratio = drag_delta as f64 / (track_h - thumb_h).max(1.0) as f64;
					state.v_scroll_offset = (state.v_drag_start_offset
						+ scroll_ratio * max_v_scroll)
						.clamp(0.0, max_v_scroll);
					shell.request_redraw();
				} else if state.h_dragging_scrollbar {
					let drag_delta = position.x() - state.h_drag_start_x;
					let track_w = viewport_w;
					let thumb_w = h_thumb.width();
					let scroll_ratio = drag_delta as f64 / (track_w - thumb_w).max(1.0) as f64;
					state.h_scroll_offset = (state.h_drag_start_offset
						+ scroll_ratio * max_h_scroll)
						.clamp(0.0, max_h_scroll);
					shell.request_redraw();
				} else if state.is_selecting
					&& let Some(cell) = self.hit_test_cell(state, bounds, position, row_num_w)
					&& let Some(sel) = &mut state.selection
					&& sel.active != cell
				{
					sel.active = cell;
					shell.request_redraw();
				}
			}
			Event::Mouse(mouse::Event::ButtonReleased(mouse::Button::Left)) => {
				if state.resizing_col.is_some() {
					state.resizing_col = None;
					if let Some(store) = self.width_store {
						*store.borrow_mut() = Some(state.col_widths.clone());
					}
					shell.request_redraw();
				} else if state.v_dragging_scrollbar || state.h_dragging_scrollbar {
					state.v_dragging_scrollbar = false;
					state.h_dragging_scrollbar = false;
					shell.request_redraw();
				} else if state.is_selecting {
					state.is_selecting = false;
				}
			}
			Event::Mouse(mouse::Event::WheelScrolled { delta })
				if cursor.is_over(bounds.into_iced()) =>
			{
				match delta {
					ScrollDelta::Lines { x, y } => {
						if x.abs() > y.abs() {
							state.h_scroll_offset = (state.h_scroll_offset
								- (*x as f64) * MIN_COL_WIDTH as f64)
								.clamp(0.0, max_h_scroll);
						} else {
							state.v_scroll_offset = (state.v_scroll_offset
								- (*y as f64) * ROW_HEIGHT as f64)
								.clamp(0.0, max_v_scroll);
						}
					}
					ScrollDelta::Pixels { x, y } => {
						if x.abs() > y.abs() {
							state.h_scroll_offset =
								(state.h_scroll_offset - *x as f64).clamp(0.0, max_h_scroll);
						} else {
							state.v_scroll_offset =
								(state.v_scroll_offset - *y as f64).clamp(0.0, max_v_scroll);
						}
					}
				}
				shell.request_redraw();
			}
			Event::Keyboard(keyboard::Event::KeyPressed { key, modifiers, .. })
				if cursor.is_over(bounds.into_iced()) =>
			{
				let page_size = (viewport_h - self.header_height()) as f64;
				match key {
					keyboard::Key::Named(keyboard::key::Named::PageDown) => {
						state.v_scroll_offset =
							(state.v_scroll_offset + page_size).clamp(0.0, max_v_scroll);
					}
					keyboard::Key::Named(keyboard::key::Named::PageUp) => {
						state.v_scroll_offset =
							(state.v_scroll_offset - page_size).clamp(0.0, max_v_scroll);
					}
					keyboard::Key::Named(keyboard::key::Named::Home) => {
						state.v_scroll_offset = 0.0;
					}
					keyboard::Key::Named(keyboard::key::Named::End) => {
						state.v_scroll_offset = max_v_scroll;
					}
					keyboard::Key::Named(keyboard::key::Named::ArrowDown) => {
						state.v_scroll_offset =
							(state.v_scroll_offset + ROW_HEIGHT as f64).clamp(0.0, max_v_scroll);
					}
					keyboard::Key::Named(keyboard::key::Named::ArrowUp) => {
						state.v_scroll_offset =
							(state.v_scroll_offset - ROW_HEIGHT as f64).clamp(0.0, max_v_scroll);
					}
					keyboard::Key::Named(keyboard::key::Named::ArrowRight) => {
						state.h_scroll_offset =
							(state.h_scroll_offset + MIN_COL_WIDTH as f64).clamp(0.0, max_h_scroll);
					}
					keyboard::Key::Named(keyboard::key::Named::ArrowLeft) => {
						state.h_scroll_offset =
							(state.h_scroll_offset - MIN_COL_WIDTH as f64).clamp(0.0, max_h_scroll);
					}
					keyboard::Key::Character(c) if c.as_str() == "c" && modifiers.control() => {
						if let Some(sel) = &state.selection {
							let (min_col, max_col, min_row, max_row) = sel.range();
							let mut out = String::new();
							for row in min_row..=max_row {
								for col in min_col..=max_col {
									if col > min_col {
										out.push('\t');
									}
									out.push_str(&self.cell_str(col, row));
								}
								out.push('\n');
							}
							use iced::advanced::clipboard::Kind;
							_clipboard.write(Kind::Standard, out);
						}
					}
					keyboard::Key::Named(keyboard::key::Named::Escape)
						if state.selection.is_some() =>
					{
						state.selection = None;
						shell.request_redraw();
					}
					_ => {}
				}
				shell.request_redraw();
			}
			Event::Keyboard(keyboard::Event::ModifiersChanged(mods)) => {
				state.modifiers = *mods;
			}
			_ => {}
		}
	}

	fn draw(
		&self,
		tree: &Tree,
		renderer: &mut Renderer,
		_theme: &Theme,
		_style: &Style,
		layout: Layout<'_>,
		_cursor: Cursor,
		_viewport: &Rectangle,
	) {
		let state = tree.state.downcast_ref::<TableState>();
		if state.col_widths.is_empty() {
			return;
		}
		let bounds = Rect::<Window>::from_iced_layout(layout.bounds());
		let v_scroll = state.v_scroll_offset;
		let row_num_w = self.row_num_width(bounds, v_scroll);
		let viewport_w = bounds.width() - V_SCROLLBAR_WIDTH - row_num_w;
		let h_scroll = state.h_scroll_offset as f32;
		let header_h = self.header_height();
		renderer.fill_quad(
			renderer::Quad {
				bounds: bounds.into_iced(),
				border: border::rounded(4),
				..renderer::Quad::default()
			},
			skin_color(self.skin.body_background),
		);
		renderer.with_layer(bounds.into_iced(), |renderer| {
			renderer.fill_quad(
				renderer::Quad {
					bounds: Rectangle {
						x: bounds.x(),
						y: bounds.y(),
						width: row_num_w,
						height: header_h,
					},
					..renderer::Quad::default()
				},
				skin_color(self.skin.header_background),
			);
			draw_text(
				renderer,
				"#  ",
				Rectangle {
					x: bounds.x() + CELL_PADDING_X,
					y: bounds.y(),
					width: row_num_w - CELL_PADDING_X,
					height: header_h,
				},
				skin_color(self.skin.header_text),
				true,
				Horizontal::Center,
			);
			renderer.fill_quad(
				renderer::Quad {
					bounds: Rectangle {
						x: bounds.x() + row_num_w - 1.0,
						y: bounds.y(),
						width: 1.0,
						height: bounds.height() - H_SCROLLBAR_HEIGHT,
					},
					..renderer::Quad::default()
				},
				skin_color(self.skin.gridline),
			);
			let header_clip = Rectangle {
				x: bounds.x() + row_num_w,
				y: bounds.y(),
				width: viewport_w,
				height: header_h,
			};
			renderer.fill_quad(
				renderer::Quad {
					bounds: header_clip,
					..renderer::Quad::default()
				},
				skin_color(self.skin.header_background),
			);
			renderer.with_layer(header_clip, |renderer| {
				let col_widths = self.col_widths_ref(state);
				let mut cell_x = bounds.x() + row_num_w - h_scroll;
				for (col_idx, field) in self.data_frame.get_column_names().iter().enumerate() {
					let col_w = col_widths[col_idx];
					let alignment = self.column_alignment(col_idx).horizontal();
					if cell_x + col_w >= bounds.x() + row_num_w
						&& cell_x <= bounds.x() + row_num_w + viewport_w
					{
						if col_idx > 0 {
							renderer.fill_quad(
								renderer::Quad {
									bounds: Rectangle {
										x: cell_x,
										y: bounds.y(),
										width: 1.0,
										height: header_h,
									},
									..renderer::Quad::default()
								},
								skin_color(self.skin.gridline),
							);
						}
						draw_text(
							renderer,
							field.as_str(),
							text_bounds(cell_x, bounds.y(), col_w, HEADER_HEIGHT),
							skin_color(self.skin.header_text),
							true,
							alignment,
						);
						if self.show_column_types
							&& let Some(col) = self.data_frame.columns().get(col_idx)
						{
							let dtype_label = format!("{}", col.dtype());
							draw_text_sized(
								renderer,
								&dtype_label,
								text_bounds(
									cell_x,
									bounds.y() + HEADER_HEIGHT,
									col_w,
									header_h - HEADER_HEIGHT,
								),
								skin_color(self.skin.type_text),
								TYPE_LABEL_FONT_SIZE,
								alignment,
							);
						}
					}
					cell_x += col_w;
				}
				renderer.fill_quad(
					renderer::Quad {
						bounds: Rectangle {
							x: cell_x,
							y: bounds.y(),
							width: 1.0,
							height: header_h,
						},
						..renderer::Quad::default()
					},
					skin_color(self.skin.gridline),
				);
			});
			renderer.fill_quad(
				renderer::Quad {
					bounds: Rectangle {
						x: bounds.x(),
						y: bounds.y() + header_h - 1.0,
						width: bounds.width() - V_SCROLLBAR_WIDTH,
						height: 1.0,
					},
					..renderer::Quad::default()
				},
				skin_color(self.skin.gridline),
			);
			let first_visible = (v_scroll / ROW_HEIGHT as f64).floor() as usize;
			let visible_count = ((bounds.height() - header_h) / ROW_HEIGHT).ceil() as usize + 1;
			let loaded = self.loaded_row_count();
			let first_visible_y = bounds.y()
				+ header_h + (first_visible as f64 * ROW_HEIGHT as f64 - v_scroll)
				as f32;
			let row_num_clip = Rectangle {
				x: bounds.x(),
				y: bounds.y() + header_h,
				width: row_num_w,
				height: bounds.height() - header_h - H_SCROLLBAR_HEIGHT,
			};
			renderer.with_layer(row_num_clip, |renderer| {
				for row_offset in 0..=visible_count {
					let row_idx = first_visible + row_offset;
					if row_idx >= loaded {
						break;
					}
					let row_y = first_visible_y + row_offset as f32 * ROW_HEIGHT;
					if row_y + ROW_HEIGHT < bounds.y() + header_h {
						continue;
					}
					let abs_idx = self.row_offset + row_idx;
					renderer.fill_quad(
						renderer::Quad {
							bounds: Rectangle {
								x: bounds.x(),
								y: row_y,
								width: row_num_w,
								height: ROW_HEIGHT,
							},
							..renderer::Quad::default()
						},
						skin_color(self.skin.gutter_background),
					);
					renderer.fill_quad(
						renderer::Quad {
							bounds: Rectangle {
								x: bounds.x(),
								y: row_y + ROW_HEIGHT - 1.0,
								width: row_num_w,
								height: 1.0,
							},
							..renderer::Quad::default()
						},
						skin_color(self.skin.gridline),
					);
					draw_text(
						renderer,
						&(abs_idx + 1).to_string(),
						Rectangle {
							x: bounds.x() + CELL_PADDING_X,
							y: row_y,
							width: row_num_w - CELL_PADDING_X,
							height: ROW_HEIGHT,
						},
						skin_color(self.skin.gutter_text),
						true,
						Horizontal::Left,
					);
				}
			});
			let rows_clip = Rectangle {
				x: bounds.x() + row_num_w,
				y: bounds.y() + header_h,
				width: viewport_w,
				height: bounds.height() - header_h - H_SCROLLBAR_HEIGHT,
			};
			renderer.with_layer(rows_clip, |renderer| {
				let col_widths = self.col_widths_ref(state);
				for row_offset in 0..=visible_count {
					let row_idx = first_visible + row_offset;
					if row_idx >= loaded {
						break;
					}
					let row_y = first_visible_y + row_offset as f32 * ROW_HEIGHT;
					if row_y + ROW_HEIGHT < bounds.y() + header_h {
						continue;
					}
					let abs_idx = self.row_offset + row_idx;

					let row_bg = if abs_idx.is_multiple_of(2) {
						self.skin.row_even_background
					} else {
						self.skin.row_odd_background
					};
					renderer.fill_quad(
						renderer::Quad {
							bounds: Rectangle {
								x: bounds.x() + row_num_w,
								y: row_y,
								width: viewport_w,
								height: ROW_HEIGHT,
							},
							..renderer::Quad::default()
						},
						skin_color(row_bg),
					);
					renderer.fill_quad(
						renderer::Quad {
							bounds: Rectangle {
								x: bounds.x() + row_num_w,
								y: row_y + ROW_HEIGHT - 1.0,
								width: viewport_w,
								height: 1.0,
							},
							..renderer::Quad::default()
						},
						skin_color(self.skin.gridline),
					);
					let mut cell_x = bounds.x() + row_num_w - h_scroll;
					for (col_idx, &col_w) in col_widths.iter().enumerate() {
						let alignment = self.column_alignment(col_idx).horizontal();
						if cell_x + col_w >= bounds.x() + row_num_w
							&& cell_x <= bounds.x() + row_num_w + viewport_w
						{
							if col_idx > 0 {
								renderer.fill_quad(
									renderer::Quad {
										bounds: Rectangle {
											x: cell_x,
											y: row_y,
											width: 1.0,
											height: ROW_HEIGHT,
										},
										..renderer::Quad::default()
									},
									skin_color(self.skin.gridline),
								);
							}
							if state
								.selection
								.as_ref()
								.is_some_and(|s| s.contains(col_idx, row_idx))
							{
								renderer.fill_quad(
									renderer::Quad {
										bounds: Rectangle {
											x: cell_x,
											y: row_y,
											width: col_w,
											height: ROW_HEIGHT,
										},
										..renderer::Quad::default()
									},
									skin_color(self.skin.selection_fill),
								);
							}
							let text = self.cell_str(col_idx, row_idx);
							draw_text(
								renderer,
								&text,
								text_bounds(cell_x, row_y, col_w, ROW_HEIGHT),
								skin_color(self.skin.cell_text),
								false,
								alignment,
							);
						}
						cell_x += col_w;
					}
					renderer.fill_quad(
						renderer::Quad {
							bounds: Rectangle {
								x: cell_x,
								y: row_y,
								width: 1.0,
								height: ROW_HEIGHT,
							},
							..renderer::Quad::default()
						},
						skin_color(self.skin.gridline),
					);
				}
			});
			let total_h = self.total_content_height();
			if total_h > bounds.height() {
				let thumb = self.v_scrollbar_thumb_rect(bounds, v_scroll).into_iced();
				renderer.fill_quad(
					renderer::Quad {
						bounds: thumb,
						border: border::rounded(2),
						..renderer::Quad::default()
					},
					skin_color(self.skin.scrollbar_thumb),
				);
			}
			let total_w = self.total_content_width(state);
			if total_w > viewport_w {
				let h_thumb = self
					.h_scrollbar_thumb_rect(bounds, state.h_scroll_offset, state, row_num_w)
					.into_iced();
				renderer.fill_quad(
					renderer::Quad {
						bounds: h_thumb,
						border: border::rounded(2),
						..renderer::Quad::default()
					},
					skin_color(self.skin.scrollbar_thumb),
				);
			}
		});
	}
}

fn draw_text<Renderer>(
	renderer: &mut Renderer,
	content: &str,
	cell_bounds: Rectangle,
	color: Color,
	is_bold: bool,
	align_x: Horizontal,
) where
	Renderer: renderer::Renderer + TextRenderer<Font = iced::Font>,
{
	let font = if is_bold {
		iced::Font {
			weight: iced::font::Weight::Bold,
			..iced::Font::DEFAULT
		}
	} else {
		iced::Font::DEFAULT
	};
	let x = match align_x {
		Horizontal::Left => cell_bounds.x,
		Horizontal::Center => cell_bounds.x + cell_bounds.width / 2.0,
		Horizontal::Right => cell_bounds.x + cell_bounds.width,
	};
	renderer.fill_text(
		Text {
			content: content.to_string(),
			bounds: cell_bounds.size(),
			size: Pixels(FONT_SIZE),
			font,
			align_x: align_x.into(),
			align_y: Vertical::Center,
			line_height: text::LineHeight::default(),
			shaping: text::Shaping::Basic,
			wrapping: text::Wrapping::None,
		},
		Point {
			x,
			y: cell_bounds.y + cell_bounds.height / 2.0,
		},
		color,
		cell_bounds,
	);
}

fn draw_text_sized<Renderer>(
	renderer: &mut Renderer,
	content: &str,
	cell_bounds: Rectangle,
	color: Color,
	font_size: f32,
	align_x: Horizontal,
) where
	Renderer: renderer::Renderer + TextRenderer<Font = iced::Font>,
{
	let x = match align_x {
		Horizontal::Left => cell_bounds.x,
		Horizontal::Center => cell_bounds.x + cell_bounds.width / 2.0,
		Horizontal::Right => cell_bounds.x + cell_bounds.width,
	};
	renderer.fill_text(
		Text {
			content: content.to_string(),
			bounds: cell_bounds.size(),
			size: Pixels(font_size),
			font: iced::Font::DEFAULT,
			align_x: align_x.into(),
			align_y: Vertical::Center,
			line_height: text::LineHeight::default(),
			shaping: text::Shaping::Basic,
			wrapping: text::Wrapping::None,
		},
		Point {
			x,
			y: cell_bounds.y + cell_bounds.height / 2.0,
		},
		color,
		cell_bounds,
	);
}

impl<'a, Message, Theme, Renderer> From<Table<'a>> for Element<'a, Message, Theme, Renderer>
where
	Renderer: renderer::Renderer + TextRenderer<Font = iced::Font>,
{
	fn from(table: Table<'a>) -> Self {
		Self::new(table)
	}
}

/// PRODUCT-0057's test-only production-path seam. The evidence target supplies
/// a real iced/wgpu renderer; this helper owns only the private widget state
/// initialization needed to reach the ordinary [`Table::draw`] implementation.
#[cfg(feature = "product-0057-render-evidence")]
#[allow(dead_code)] // The named evidence target mirrors this private module; the binary does not.
pub(crate) struct Product0057TableEvidenceGeometry {
	pub(crate) row_number_width: f32,
	pub(crate) column_width: f32,
	pub(crate) header_height: f32,
}

#[cfg(feature = "product-0057-render-evidence")]
#[allow(dead_code)] // The named evidence target mirrors this private module; the binary does not.
pub(crate) fn draw_product_0057_table_evidence(
	renderer: &mut iced::Renderer,
	data_frame: &DataFrame,
	column_width: Option<f32>,
	bounds: Rectangle,
) -> Product0057TableEvidenceGeometry {
	let mut table = Table::new(data_frame, 0, TableSkin::DEFAULT);
	if let Some(column_width) = column_width {
		table = table.col_width(column_width);
	}
	let mut tree = Tree {
		tag: tree::Tag::of::<TableState>(),
		state: tree::State::new(TableState::default()),
		children: Vec::new(),
	};
	let state = tree.state.downcast_mut::<TableState>();
	table.col_widths(state, bounds.width);
	let row_number_width = table.row_num_width(
		Rect::<Window>::from_iced_layout(bounds),
		state.v_scroll_offset,
	);
	let resolved_column_width = table.col_widths_ref(state)[0];
	let node = Node::new(bounds.size()).move_to(bounds.position());
	<Table<'_> as Widget<(), (), iced::Renderer>>::draw(
		&table,
		&tree,
		renderer,
		&(),
		&Style::default(),
		Layout::new(&node),
		Cursor::Unavailable,
		&bounds,
	);
	Product0057TableEvidenceGeometry {
		row_number_width,
		column_width: resolved_column_width,
		header_height: table.header_height(),
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use iced::{Background, Font, Transformation};

	#[derive(Clone, Debug, PartialEq)]
	enum PaintOperation {
		Fill {
			bounds: Rectangle,
			background: Background,
		},
		Text {
			content: String,
			position: Point,
			color: Color,
			clip_bounds: Rectangle,
		},
	}

	#[derive(Default)]
	/// PRODUCT-0057 named oracle `gui::table::tests::RecordingRenderer` records
	/// requested quads and text, text clip bounds, and the active iced layer
	/// stack. It does not shape or rasterize glyphs and cannot prove wrap or fit.
	struct RecordingRenderer {
		operations: Vec<PaintOperation>,
		layers: Vec<Rectangle>,
		active_layers: Vec<Rectangle>,
		operation_layers: Vec<Vec<Rectangle>>,
	}

	impl renderer::Renderer for RecordingRenderer {
		fn start_layer(&mut self, bounds: Rectangle) {
			self.layers.push(bounds);
			self.active_layers.push(bounds);
		}

		fn end_layer(&mut self) {
			self.active_layers.pop();
		}

		fn start_transformation(&mut self, _transformation: Transformation) {}

		fn end_transformation(&mut self) {}

		fn fill_quad(&mut self, quad: renderer::Quad, background: impl Into<Background>) {
			self.operation_layers.push(self.active_layers.clone());
			self.operations.push(PaintOperation::Fill {
				bounds: quad.bounds,
				background: background.into(),
			});
		}

		fn reset(&mut self, _new_bounds: Rectangle) {}

		fn allocate_image(
			&mut self,
			_handle: &iced::advanced::image::Handle,
			callback: impl FnOnce(
				Result<iced::advanced::image::Allocation, iced::advanced::image::Error>,
			) + Send
			+ 'static,
		) {
			callback(Err(iced::advanced::image::Error::Unsupported));
		}
	}

	impl TextRenderer for RecordingRenderer {
		type Font = Font;
		type Paragraph = ();
		type Editor = ();

		const ICON_FONT: Font = Font::DEFAULT;
		const CHECKMARK_ICON: char = '0';
		const ARROW_DOWN_ICON: char = '0';
		const SCROLL_UP_ICON: char = '0';
		const SCROLL_DOWN_ICON: char = '0';
		const SCROLL_LEFT_ICON: char = '0';
		const SCROLL_RIGHT_ICON: char = '0';
		const ICED_LOGO: char = '0';

		fn default_font(&self) -> Self::Font {
			Font::DEFAULT
		}

		fn default_size(&self) -> Pixels {
			Pixels(FONT_SIZE)
		}

		fn fill_paragraph(
			&mut self,
			_paragraph: &Self::Paragraph,
			_position: Point,
			_color: Color,
			_clip_bounds: Rectangle,
		) {
		}

		fn fill_editor(
			&mut self,
			_editor: &Self::Editor,
			_position: Point,
			_color: Color,
			_clip_bounds: Rectangle,
		) {
		}

		fn fill_text(
			&mut self,
			text: Text<String, Self::Font>,
			position: Point,
			color: Color,
			clip_bounds: Rectangle,
		) {
			self.operation_layers.push(self.active_layers.clone());
			self.operations.push(PaintOperation::Text {
				content: text.content,
				position,
				color,
				clip_bounds,
			});
		}
	}

	struct RecordedTableDraw {
		operations: Vec<PaintOperation>,
		layers: Vec<Rectangle>,
		operation_layers: Vec<Vec<Rectangle>>,
		bounds: Rectangle,
	}

	fn recorded_table_draw(skin: TableSkin, show_column_types: Option<bool>) -> RecordedTableDraw {
		let frame = polars::df!(
			"category" => ["A", "B", "C", "D"],
			"value" => [1_i64, 2, 3, 4],
		)
		.expect("table draw fixture");
		let table = match show_column_types {
			Some(show) => Table::new(&frame, 0, skin).show_column_types(show),
			None => Table::new(&frame, 0, skin),
		};
		let bounds = Rectangle::new(Point::new(10.0, 20.0), Size::new(120.0, 120.0));
		let mut tree = Tree {
			tag: tree::Tag::of::<TableState>(),
			state: tree::State::new(TableState::default()),
			children: Vec::new(),
		};
		let state = tree.state.downcast_mut::<TableState>();
		table.col_widths(state, bounds.width);
		state.selection = Some(TableSelection {
			anchor: (0, 1),
			active: (0, 1),
		});

		let node = Node::new(bounds.size()).move_to(bounds.position());
		let layout = Layout::new(&node);
		let mut renderer = RecordingRenderer::default();
		<Table<'_> as Widget<(), (), RecordingRenderer>>::draw(
			&table,
			&tree,
			&mut renderer,
			&(),
			&Style::default(),
			layout,
			Cursor::Unavailable,
			&bounds,
		);
		RecordedTableDraw {
			operations: renderer.operations,
			layers: renderer.layers,
			operation_layers: renderer.operation_layers,
			bounds,
		}
	}

	fn product_0023_recorded_table_draw(skin: TableSkin) -> RecordedTableDraw {
		recorded_table_draw(skin, Some(true))
	}

	fn product_0023_recorded_table_operations(skin: TableSkin) -> Vec<PaintOperation> {
		product_0023_recorded_table_draw(skin).operations
	}

	fn product_0023_operation_signature(operations: &[PaintOperation]) -> String {
		let color = |color: Color| {
			format!(
				"#{:02x}{:02x}{:02x}/{:.2}",
				(color.r * 255.0).round() as u8,
				(color.g * 255.0).round() as u8,
				(color.b * 255.0).round() as u8,
				color.a,
			)
		};
		let rect = |bounds: Rectangle| {
			format!(
				"{:.2},{:.2},{:.2},{:.2}",
				bounds.x, bounds.y, bounds.width, bounds.height
			)
		};
		let point = |position: Point| format!("{:.2},{:.2}", position.x, position.y);
		operations
			.iter()
			.map(|operation| match operation {
				PaintOperation::Fill { bounds, background } => {
					let Background::Color(background) = background else {
						panic!("table oracle does not accept gradient fills")
					};
					format!("fill {} {}", rect(*bounds), color(*background))
				}
				PaintOperation::Text {
					content,
					position,
					color: text_color,
					clip_bounds,
				} => format!(
					"text {content:?} {} {} clip {}",
					point(*position),
					color(*text_color),
					rect(*clip_bounds)
				),
			})
			.collect::<Vec<_>>()
			.join("\n")
	}

	/// Baseline updated 2026-08-07 for the `CHAR_ADVANCE` 0.6 → 0.65 width
	/// estimate (the "margin" header-wrap fix): only measured column widths
	/// changed; every fill color, text color, and operation order is
	/// byte-identical to the pre-extraction recording. Widths were never part
	/// of PRODUCT-0023's skin scope (§7), so this is a premise update, not a
	/// parity regression.
	#[test]
	fn product_0023_pre_extraction_draw_operation_oracle() {
		let signature = product_0023_operation_signature(&product_0023_recorded_table_operations(
			TableSkin::DEFAULT,
		));
		const PRE_EXTRACTION_DRAW_ORACLE: &str = r##"fill 10.00,20.00,120.00,120.00 #010101/1.00
fill 10.00,20.00,28.00,48.00 #040404/1.00
text "#  " 28.00,44.00 #dcdcdc/1.00 clip 18.00,20.00,20.00,48.00
fill 37.00,20.00,1.00,108.00 #fafafa/0.10
fill 38.00,20.00,80.00,48.00 #040404/1.00
text "category" 46.00,36.00 #dcdcdc/1.00 clip 46.00,20.00,72.80,32.00
text "str" 46.00,60.00 #b496dc/1.00 clip 46.00,52.00,72.80,16.00
fill 188.30,20.00,1.00,48.00 #fafafa/0.10
fill 10.00,67.00,108.00,1.00 #fafafa/0.10
fill 10.00,68.00,28.00,28.00 #040404/1.00
fill 10.00,95.00,28.00,1.00 #fafafa/0.10
text "1" 18.00,82.00 #dcdcdc/1.00 clip 18.00,68.00,20.00,28.00
fill 10.00,96.00,28.00,28.00 #040404/1.00
fill 10.00,123.00,28.00,1.00 #fafafa/0.10
text "2" 18.00,110.00 #dcdcdc/1.00 clip 18.00,96.00,20.00,28.00
fill 10.00,124.00,28.00,28.00 #040404/1.00
fill 10.00,151.00,28.00,1.00 #fafafa/0.10
text "3" 18.00,138.00 #dcdcdc/1.00 clip 18.00,124.00,20.00,28.00
fill 10.00,152.00,28.00,28.00 #040404/1.00
fill 10.00,179.00,28.00,1.00 #fafafa/0.10
text "4" 18.00,166.00 #dcdcdc/1.00 clip 18.00,152.00,20.00,28.00
fill 38.00,68.00,80.00,28.00 #140826/1.00
fill 38.00,95.00,80.00,1.00 #fafafa/0.10
text "A" 46.00,82.00 #e6e6e6/1.00 clip 46.00,68.00,72.80,28.00
fill 188.30,68.00,1.00,28.00 #fafafa/0.10
fill 38.00,96.00,80.00,28.00 #281446/1.00
fill 38.00,123.00,80.00,1.00 #fafafa/0.10
fill 38.00,96.00,88.80,28.00 #8c00fa/0.25
text "B" 46.00,110.00 #e6e6e6/1.00 clip 46.00,96.00,72.80,28.00
fill 188.30,96.00,1.00,28.00 #fafafa/0.10
fill 38.00,124.00,80.00,28.00 #140826/1.00
fill 38.00,151.00,80.00,1.00 #fafafa/0.10
text "C" 46.00,138.00 #e6e6e6/1.00 clip 46.00,124.00,72.80,28.00
fill 188.30,124.00,1.00,28.00 #fafafa/0.10
fill 38.00,152.00,80.00,28.00 #281446/1.00
fill 38.00,179.00,80.00,1.00 #fafafa/0.10
text "D" 46.00,166.00 #e6e6e6/1.00 clip 46.00,152.00,72.80,28.00
fill 188.30,152.00,1.00,28.00 #fafafa/0.10
fill 116.00,68.00,8.00,32.14 #8c00fa/0.50
fill 38.00,130.00,42.58,8.00 #8c00fa/0.50"##;
		assert_eq!(signature, PRE_EXTRACTION_DRAW_ORACLE);
	}

	#[test]
	fn product_0023_skin_changes_every_color_role_and_nothing_else() {
		let assay = TableSkin {
			body_background: TableRgba::opaque(1, 2, 3),
			header_background: TableRgba::opaque(4, 5, 6),
			gutter_background: TableRgba::opaque(7, 8, 9),
			row_even_background: TableRgba::opaque(10, 11, 12),
			row_odd_background: TableRgba::opaque(13, 14, 15),
			gridline: TableRgba::translucent(16, 17, 18, 0.19),
			cell_text: TableRgba::opaque(20, 21, 22),
			header_text: TableRgba::opaque(23, 24, 25),
			type_text: TableRgba::opaque(26, 27, 28),
			gutter_text: TableRgba::opaque(29, 30, 31),
			selection_fill: TableRgba::translucent(32, 33, 34, 0.35),
			scrollbar_thumb: TableRgba::translucent(35, 36, 37, 0.38),
		};
		let default = product_0023_recorded_table_operations(TableSkin::DEFAULT);
		let styled = product_0023_recorded_table_operations(assay);

		let geometry = |operation: &PaintOperation| match operation {
			PaintOperation::Fill { bounds, .. } => {
				format!("fill {bounds:?}")
			}
			PaintOperation::Text {
				content,
				position,
				clip_bounds,
				..
			} => format!("text {content:?} {position:?} {clip_bounds:?}"),
		};
		assert_eq!(
			default.iter().map(geometry).collect::<Vec<_>>(),
			styled.iter().map(geometry).collect::<Vec<_>>()
		);

		let styled_colors = styled
			.iter()
			.map(|operation| match operation {
				PaintOperation::Fill {
					background: Background::Color(color),
					..
				} => *color,
				PaintOperation::Fill { .. } => panic!("table assay does not accept gradients"),
				PaintOperation::Text { color, .. } => *color,
			})
			.collect::<Vec<_>>();
		for role in [
			assay.body_background,
			assay.header_background,
			assay.gutter_background,
			assay.row_even_background,
			assay.row_odd_background,
			assay.gridline,
			assay.cell_text,
			assay.header_text,
			assay.type_text,
			assay.gutter_text,
			assay.selection_fill,
			assay.scrollbar_thumb,
		] {
			assert!(
				styled_colors.contains(&skin_color(role)),
				"missing styled role {role:?}"
			);
		}
	}

	#[test]
	fn product_0052_type_visibility_uses_the_existing_draw_path_and_keeps_default_pixels() {
		let default = recorded_table_draw(TableSkin::DEFAULT, None).operations;
		let explicit_hidden = recorded_table_draw(TableSkin::DEFAULT, Some(false)).operations;
		assert_eq!(
			product_0023_operation_signature(&default),
			product_0023_operation_signature(&explicit_hidden),
			"the default-false table draw must remain byte-identical"
		);

		let visible = recorded_table_draw(TableSkin::DEFAULT, Some(true)).operations;
		let type_label = visible.iter().find_map(|operation| match operation {
			PaintOperation::Text {
				content,
				color,
				position,
				..
			} if content == "str" => Some((*color, *position)),
			_ => None,
		});
		let (color, position) = type_label.expect("existing semantic type label");
		assert_eq!(color, skin_color(TableSkin::DEFAULT.type_text));
		assert_eq!(position.y, 60.0);
		assert!(default.iter().all(|operation| !matches!(
			operation,
			PaintOperation::Text { content, .. } if content == "str"
		)));
	}

	fn assert_close(actual: f32, expected: f32) {
		assert!(
			(actual - expected).abs() < 0.01,
			"expected {expected}, got {actual}"
		);
	}

	#[test]
	fn content_widths_are_independent_of_viewport_width() {
		let frame = polars::df!(
			"cat" => ["Cat1", "Cat2", "Cat3"],
			"val" => [1.1_f64, 2.2, 3.3],
		)
		.expect("table fixture");
		let table = Table::new(&frame, 0, TableSkin::DEFAULT);
		let mut narrow_state = TableState::default();
		let mut wide_state = TableState::default();

		let narrow = table.col_widths(&mut narrow_state, 200.0).to_vec();
		let wide = table.col_widths(&mut wide_state, 1_900.0).to_vec();

		assert_eq!(narrow, wide);
		assert!(wide.iter().all(|width| *width <= MAX_COL_WIDTH));
		assert!(wide.iter().sum::<f32>() < 1_900.0 / 4.0);

		let old_scale = 1_900.0 / narrow.iter().sum::<f32>();
		let old_total = narrow.iter().map(|width| width * old_scale).sum::<f32>();
		assert_close(old_total, 1_900.0);
	}

	#[test]
	fn measured_widths_include_symmetric_padding_and_stay_bounded() {
		let very_long = "x".repeat(100);
		let frame = polars::df!(
			"x" => [""],
			"four" => ["Cat1"],
			"very_long" => [very_long],
		)
		.expect("table fixture");
		let table = Table::new(&frame, 0, TableSkin::DEFAULT);

		assert_close(table.measure_col_width(0), MIN_COL_WIDTH);
		assert_close(
			table.measure_col_width(1),
			4.0 * FONT_SIZE * CHAR_ADVANCE + CELL_PADDING_X * 2.0,
		);
		assert_close(table.measure_col_width(2), MAX_COL_WIDTH);
	}

	struct MockClipboard {
		written: Option<String>,
	}

	impl Clipboard for MockClipboard {
		fn read(&self, _kind: iced::advanced::clipboard::Kind) -> Option<String> {
			None
		}
		fn write(&mut self, _kind: iced::advanced::clipboard::Kind, contents: String) {
			self.written = Some(contents);
		}
	}

	fn drive(
		table: &mut Table<'_>,
		tree: &mut Tree,
		node: &Node,
		offset: iced::Vector,
		event: &Event,
		cursor: Cursor,
		clipboard: &mut MockClipboard,
	) {
		let renderer = RecordingRenderer::default();
		let mut messages: Vec<()> = Vec::new();
		let mut shell = Shell::new(&mut messages);
		let bounds = Layout::with_offset(offset, node).bounds();
		<Table<'_> as Widget<(), iced::Theme, RecordingRenderer>>::update(
			table,
			tree,
			event,
			Layout::with_offset(offset, node),
			cursor,
			&renderer,
			clipboard,
			&mut shell,
			&bounds,
		);
	}

	fn ctrl_c_event() -> Event {
		Event::Keyboard(keyboard::Event::KeyPressed {
			key: keyboard::Key::Character("c".into()),
			modified_key: keyboard::Key::Character("c".into()),
			physical_key: keyboard::key::Physical::Code(keyboard::key::Code::KeyC),
			location: keyboard::Location::Standard,
			modifiers: keyboard::Modifiers::CTRL,
			text: None,
			repeat: false,
		})
	}

	/// The selection-and-copy contract had no test until 2026-08-07, when its
	/// possible regression was raised: click selects a cell, shift-click
	/// extends, dragging extends, Ctrl+C writes the selection as
	/// tab-separated rows to the system clipboard, and Escape clears. This
	/// drives the real `update` path with a mock clipboard so the contract is
	/// pinned rather than folklore.
	#[test]
	fn click_shift_drag_copy_and_escape_cover_the_selection_contract() {
		selection_contract_at(iced::Vector::new(0.0, 0.0));
	}

	/// The same contract at a nonzero layout offset. The origin test is blind
	/// to absolute-position math errors (widget-local and window coordinates
	/// coincide at the origin), which is the class behind every renderer bug
	/// this week; a live panel never sits at the origin.
	#[test]
	fn the_selection_contract_holds_at_a_nonzero_panel_origin() {
		selection_contract_at(iced::Vector::new(517.0, 293.0));
	}

	fn selection_contract_at(offset: iced::Vector) {
		let frame = polars::df!(
			"cat" => ["A", "B", "C"],
			"val" => [1_i64, 2, 3],
		)
		.expect("selection fixture");
		let mut table = Table::new(&frame, 0, TableSkin::DEFAULT);
		let mut tree = Tree {
			tag: <Table<'_> as Widget<(), iced::Theme, RecordingRenderer>>::tag(&table),
			state: <Table<'_> as Widget<(), iced::Theme, RecordingRenderer>>::state(&table),
			children: Vec::new(),
		};
		let node = Node::new(Size::new(400.0, 300.0));
		let bounds = Layout::with_offset(offset, &node).bounds();
		let mut clipboard = MockClipboard { written: None };
		let press = Event::Mouse(mouse::Event::ButtonPressed(mouse::Button::Left));
		let release = Event::Mouse(mouse::Event::ButtonReleased(mouse::Button::Left));

		let row_num_w = table.row_num_width(Rect::<Window>::from_iced_layout(bounds), 0.0);
		let header_h = table.header_height();

		// Click selects one cell. Column 0 starts at the row-number gutter,
		// so the seed point needs no measured widths.
		let seed = Point::new(bounds.x + row_num_w + 4.0, bounds.y + header_h + 4.0);
		drive(
			&mut table,
			&mut tree,
			&node,
			offset,
			&press,
			Cursor::Available(seed),
			&mut clipboard,
		);
		let state = tree.state.downcast_ref::<TableState>();
		let sel = state.selection.as_ref().expect("click selects");
		assert_eq!(sel.range(), (0, 0, 0, 0));
		let widths = state.col_widths.clone();
		let at = move |col: usize, row: usize| {
			Point::new(
				bounds.x + row_num_w + widths[..col].iter().sum::<f32>() + 4.0,
				bounds.y + header_h + row as f32 * ROW_HEIGHT + 4.0,
			)
		};
		drive(
			&mut table,
			&mut tree,
			&node,
			offset,
			&release,
			Cursor::Available(seed),
			&mut clipboard,
		);

		// Shift-click extends from the anchor.
		let shift = Event::Keyboard(keyboard::Event::ModifiersChanged(
			keyboard::Modifiers::SHIFT,
		));
		drive(
			&mut table,
			&mut tree,
			&node,
			offset,
			&shift,
			Cursor::Available(seed),
			&mut clipboard,
		);
		let far = at(1, 2);
		drive(
			&mut table,
			&mut tree,
			&node,
			offset,
			&press,
			Cursor::Available(far),
			&mut clipboard,
		);
		drive(
			&mut table,
			&mut tree,
			&node,
			offset,
			&release,
			Cursor::Available(far),
			&mut clipboard,
		);
		let state = tree.state.downcast_ref::<TableState>();
		assert_eq!(
			state.selection.as_ref().expect("shift extends").range(),
			(0, 1, 0, 2)
		);

		// Ctrl+C writes the selection as tab-separated rows.
		drive(
			&mut table,
			&mut tree,
			&node,
			offset,
			&ctrl_c_event(),
			Cursor::Available(far),
			&mut clipboard,
		);
		assert_eq!(
			clipboard.written.as_deref(),
			Some("A\t1\nB\t2\nC\t3\n"),
			"copy must reproduce the selected cells row-major"
		);

		// Dragging from a fresh click extends the selection.
		let unshift = Event::Keyboard(keyboard::Event::ModifiersChanged(
			keyboard::Modifiers::default(),
		));
		drive(
			&mut table,
			&mut tree,
			&node,
			offset,
			&unshift,
			Cursor::Available(seed),
			&mut clipboard,
		);
		drive(
			&mut table,
			&mut tree,
			&node,
			offset,
			&press,
			Cursor::Available(seed),
			&mut clipboard,
		);
		let mid = at(1, 1);
		drive(
			&mut table,
			&mut tree,
			&node,
			offset,
			&Event::Mouse(mouse::Event::CursorMoved { position: mid }),
			Cursor::Available(mid),
			&mut clipboard,
		);
		drive(
			&mut table,
			&mut tree,
			&node,
			offset,
			&release,
			Cursor::Available(mid),
			&mut clipboard,
		);
		let state = tree.state.downcast_ref::<TableState>();
		assert_eq!(
			state.selection.as_ref().expect("drag extends").range(),
			(0, 1, 0, 1)
		);

		// Escape clears.
		let escape = Event::Keyboard(keyboard::Event::KeyPressed {
			key: keyboard::Key::Named(keyboard::key::Named::Escape),
			modified_key: keyboard::Key::Named(keyboard::key::Named::Escape),
			physical_key: keyboard::key::Physical::Code(keyboard::key::Code::Escape),
			location: keyboard::Location::Standard,
			modifiers: keyboard::Modifiers::default(),
			text: None,
			repeat: false,
		});
		drive(
			&mut table,
			&mut tree,
			&node,
			offset,
			&escape,
			Cursor::Available(seed),
			&mut clipboard,
		);
		let state = tree.state.downcast_ref::<TableState>();
		assert!(state.selection.is_none(), "escape clears the selection");
	}

	#[test]
	fn headers_and_cells_share_schema_driven_alignment() {
		let frame = polars::df!(
			"text" => ["Cat1"],
			"integer" => [1_i64],
			"decimal" => [1.1_f64],
			"flag" => [true],
		)
		.expect("table fixture");
		let table = Table::new(&frame, 0, TableSkin::DEFAULT);

		assert_eq!(table.column_alignment(0), ColumnAlignment::Left);
		assert_eq!(table.column_alignment(1), ColumnAlignment::Right);
		assert_eq!(table.column_alignment(2), ColumnAlignment::Right);
		assert_eq!(table.column_alignment(3), ColumnAlignment::Left);
	}

	#[test]
	fn text_padding_is_symmetric_inside_the_column() {
		let bounds = text_bounds(100.0, 20.0, 80.0, ROW_HEIGHT);

		assert_close(bounds.x, 100.0 + CELL_PADDING_X);
		assert_close(bounds.width, 80.0 - CELL_PADDING_X * 2.0);
		assert_close(bounds.x + bounds.width / 2.0, 140.0);
		assert_close(bounds.height, ROW_HEIGHT);
	}

	#[test]
	fn horizontal_scroll_refits_when_the_viewport_grows() {
		assert_eq!(clamp_horizontal_scroll(350.0, 500.0, 100.0), 350.0);
		assert_eq!(clamp_horizontal_scroll(350.0, 500.0, 300.0), 200.0);
		assert_eq!(clamp_horizontal_scroll(350.0, 500.0, 600.0), 0.0);
	}

	#[test]
	fn product_0023_table_artifact_draw_is_clipped_to_its_content_bounds() {
		let recorded = product_0023_recorded_table_draw(TableSkin::DEFAULT);
		let contained_by = |inner: Rectangle, outer: Rectangle| {
			inner.x >= outer.x
				&& inner.y >= outer.y
				&& inner.x + inner.width <= outer.x + outer.width
				&& inner.y + inner.height <= outer.y + outer.height
		};
		assert_eq!(recorded.layers.first(), Some(&recorded.bounds));
		assert!(
			recorded
				.layers
				.iter()
				.all(|bounds| contained_by(*bounds, recorded.bounds)),
			"every nested draw layer must remain inside the table artifact clip"
		);
		assert!(matches!(
			recorded.operations.first(),
			Some(PaintOperation::Fill { bounds, .. }) if *bounds == recorded.bounds
		));
		assert_eq!(recorded.operations.len(), recorded.operation_layers.len());
		assert!(recorded.operation_layers[0].is_empty());
		assert!(
			recorded.operation_layers[1..]
				.iter()
				.all(|layers| layers.first() == Some(&recorded.bounds)),
			"every operation after the artifact background must execute inside the outer clip"
		);
	}
}
