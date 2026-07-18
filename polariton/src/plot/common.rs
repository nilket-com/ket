use crate::gui::messages::PlotMessage;
use crate::plot::colors::ColorTheme;
use iced::advanced::image;
use iced::advanced::mouse::Cursor;
use iced::alignment;
use iced::widget::canvas::{self, Frame, Geometry, Path, Program, Stroke, Style, Text};
use iced::{Color, Event, Point, Rectangle, Renderer, Theme};
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Orientation {
	Vertical,
	Horizontal,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TimeUnit {
	Nanoseconds,
	Microseconds,
	Milliseconds,
}

#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum AxisType {
	#[default]
	Linear,
	Date,
	Datetime(TimeUnit),
	Time,
}

#[allow(dead_code)]
#[derive(Clone)]
pub enum PlotLayout {
	Cartesian {
		x_range: (f64, f64),
		y_range: (f64, f64),
		x_axis_type: AxisType,
		y_axis_type: AxisType,
	},
	CategoricalX {
		categories: Vec<String>,
		y_range: (f64, f64),
	},
	CategoricalY {
		categories: Vec<String>,
		x_range: (f64, f64),
	},
	CategoricalXY {
		x_categories: Vec<String>,
		y_categories: Vec<String>,
	},
	Parallel {
		dimensions: Vec<String>,
		ranges: Vec<(f64, f64)>,
	},
	Radial,
}

pub struct CoordinateTransformer<'a> {
	pub layout: &'a PlotLayout,
	pub bounds: Rectangle,
}

impl<'a> CoordinateTransformer<'a> {
	pub fn new(layout: &'a PlotLayout, bounds: Rectangle) -> Self {
		Self { layout, bounds }
	}

	pub fn cartesian(&self, data_x: f64, data_y: f64) -> Point {
		if let PlotLayout::Cartesian {
			x_range, y_range, ..
		} = self.layout
		{
			let x_delta = (x_range.1 - x_range.0).abs().max(f64::EPSILON);
			let y_delta = (y_range.1 - y_range.0).abs().max(f64::EPSILON);
			let x_scale = self.bounds.width as f64 / x_delta;
			let y_scale = self.bounds.height as f64 / y_delta;
			let pixel_x = ((data_x - x_range.0) * x_scale) as f32;
			let pixel_y =
				self.bounds.height - ((data_y - y_range.0) * y_scale) as f32;
			Point::new(pixel_x, pixel_y)
		} else {
			Point::ORIGIN
		}
	}

	pub fn categorical(&self, category_index: usize, data_val: f64) -> (Point, f32) {
		match self.layout {
			PlotLayout::CategoricalX {
				categories,
				y_range,
			} => {
				let num_cats = categories.len().max(1) as f32;
				let band_width = self.bounds.width / num_cats;
				let center_x =
					(category_index as f32 * band_width) + (band_width / 2.0);
				let y_delta = (y_range.1 - y_range.0).abs().max(f64::EPSILON);
				let y_scale = self.bounds.height as f64 / y_delta;
				let pixel_y =
					self.bounds.height - ((data_val - y_range.0) * y_scale) as f32;
				(Point::new(center_x, pixel_y), band_width)
			}
			PlotLayout::CategoricalY {
				categories,
				x_range,
			} => {
				let num_cats = categories.len().max(1) as f32;
				let band_height = self.bounds.height / num_cats;
				let center_y = self.bounds.height
					- (category_index as f32 * band_height)
					- (band_height / 2.0);
				let x_delta = (x_range.1 - x_range.0).abs().max(f64::EPSILON);
				let x_scale = self.bounds.width as f64 / x_delta;
				let pixel_x = ((data_val - x_range.0) * x_scale) as f32;
				(Point::new(pixel_x, center_y), band_height)
			}
			PlotLayout::Parallel { dimensions, ranges } => {
				let num_dims = dimensions.len().max(1) as f32;
				let axis_spacing = self.bounds.width / (num_dims - 1.0).max(1.0);
				let axis_x = category_index as f32 * axis_spacing;
				let range = ranges.get(category_index).copied().unwrap_or((0.0, 1.0));
				let y_delta = (range.1 - range.0).abs().max(f64::EPSILON);
				let y_scale = self.bounds.height as f64 / y_delta;
				let pixel_y =
					self.bounds.height - ((data_val - range.0) * y_scale) as f32;
				(Point::new(axis_x, pixel_y), 0.0)
			}
			_ => (Point::ORIGIN, 0.0),
		}
	}

	pub fn categorical_2d(&self, x_index: usize, y_index: usize) -> (Point, f32, f32) {
		match self.layout {
			PlotLayout::CategoricalXY {
				x_categories,
				y_categories,
			} => {
				let num_x = x_categories.len().max(1) as f32;
				let num_y = y_categories.len().max(1) as f32;
				let band_width = self.bounds.width / num_x;
				let band_height = self.bounds.height / num_y;
				let center_x = (x_index as f32 * band_width) + (band_width / 2.0);
				let center_y = self.bounds.height
					- (y_index as f32 * band_height)
					- (band_height / 2.0);
				(Point::new(center_x, center_y), band_width, band_height)
			}
			_ => (Point::ORIGIN, 0.0, 0.0),
		}
	}

	#[allow(dead_code)]
	pub fn pixel_to_cartesian(&self, cursor_pos: Point) -> Option<(f64, f64)> {
		if let PlotLayout::Cartesian {
			x_range, y_range, ..
		} = self.layout
		{
			let x_delta = (x_range.1 - x_range.0).abs().max(f64::EPSILON);
			let y_delta = (y_range.1 - y_range.0).abs().max(f64::EPSILON);
			let x_scale = x_delta / self.bounds.width as f64;
			let y_scale = y_delta / self.bounds.height as f64;
			let data_x = x_range.0 + (cursor_pos.x as f64 * x_scale);
			let data_y =
				y_range.0 + ((self.bounds.height - cursor_pos.y) as f64 * y_scale);
			Some((data_x, data_y))
		} else {
			None
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GridLineStyle {
	Solid,
	Dashed,
	Dotted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ScatterRenderMode {
	#[default]
	Auto,
	Vector,
	Downsampled,
	Rasterized,
}

impl ScatterRenderMode {
	pub const ALL: [ScatterRenderMode; 4] = [
		ScatterRenderMode::Auto,
		ScatterRenderMode::Vector,
		ScatterRenderMode::Downsampled,
		ScatterRenderMode::Rasterized,
	];
}

impl std::fmt::Display for ScatterRenderMode {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			ScatterRenderMode::Auto => write!(f, "Auto"),
			ScatterRenderMode::Vector => write!(f, "Vector"),
			ScatterRenderMode::Downsampled => write!(f, "Downsampled"),
			ScatterRenderMode::Rasterized => write!(f, "Rasterized"),
		}
	}
}

impl GridLineStyle {
	pub const ALL: [GridLineStyle; 3] = [GridLineStyle::Solid, GridLineStyle::Dashed, GridLineStyle::Dotted];
}

impl std::fmt::Display for GridLineStyle {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			GridLineStyle::Solid => write!(f, "Solid"),
			GridLineStyle::Dashed => write!(f, "Dashed"),
			GridLineStyle::Dotted => write!(f, "Dotted"),
		}
	}
}

#[derive(Debug, Clone)]
pub struct PlotSettings {
	pub max_legend_rows: u32,
	pub legend_x: f32,
	pub legend_y: f32,
	pub x_label_rotation: f32,
	pub x_label_offset: f32,
	pub color_theme: ColorTheme,
	pub background_color: Color,
	pub decoration_color: Color,
	pub x_min: Option<f64>,
	pub x_max: Option<f64>,
	pub y_min: Option<f64>,
	pub y_max: Option<f64>,
	pub title: Option<Arc<String>>,
	pub subtitle: Option<Arc<String>>,
	pub x_label: Option<Arc<String>>,
	pub y_label: Option<Arc<String>>,
	pub title_offset: f32,
	pub subtitle_offset: f32,
	pub x_label_padding: f32,
	pub y_label_padding: f32,
	pub plot_padding_top: f32,
	pub plot_padding_bottom: f32,
	pub plot_padding_left: f32,
	pub plot_padding_right: f32,
	pub title_size: f32,
	pub subtitle_size: f32,
	pub x_label_size: f32,
	pub y_label_size: f32,
	pub x_tick_size: f32,
	pub y_tick_size: f32,
	pub legend_size: f32,
	pub x_ticks: u32,
	pub y_ticks: u32,
	pub x_minor_ticks: u32,
	pub y_minor_ticks: u32,
	pub show_x_minor_ticks: bool,
	pub show_y_minor_ticks: bool,
	pub show_x_major_grid: bool,
	pub show_y_major_grid: bool,
	pub show_x_minor_grid: bool,
	pub show_y_minor_grid: bool,
	pub x_major_grid_width: f32,
	pub y_major_grid_width: f32,
	pub x_minor_grid_width: f32,
	pub y_minor_grid_width: f32,
	pub x_major_grid_style: GridLineStyle,
	pub y_major_grid_style: GridLineStyle,
	pub x_minor_grid_style: GridLineStyle,
	pub y_minor_grid_style: GridLineStyle,
	pub scatter_render_mode: ScatterRenderMode,
	pub scatter_max_vector_points: u32,
	pub scatter_downsample_target: u32,
	pub scatter_raster_threshold: u32,
}

impl Default for PlotSettings {
	fn default() -> Self {
		Self {
			max_legend_rows: 10,
			legend_x: 0.95,
			legend_y: 0.05,
			x_label_rotation: 0.0,
			x_label_offset: 10.0,
			color_theme: ColorTheme::default(),
			background_color: Color::from_rgb(0.001, 0.001, 0.001),
			decoration_color: Color::WHITE,
			x_min: None,
			x_max: None,
			y_min: None,
			y_max: None,
			title: None,
			subtitle: None,
			x_label: None,
			y_label: None,
			title_offset: 20.0,
			subtitle_offset: 50.0,
			x_label_padding: 45.0,
			y_label_padding: 85.0,
			plot_padding_top: 50.0,
			plot_padding_bottom: 80.0,
			plot_padding_left: 100.0,
			plot_padding_right: 20.0,
			title_size: 28.0,
			subtitle_size: 20.0,
			x_label_size: 20.0,
			y_label_size: 20.0,
			x_tick_size: 18.0,
			y_tick_size: 18.0,
			legend_size: 14.0,
			x_ticks: 8,
			y_ticks: 8,
			x_minor_ticks: 4,
			y_minor_ticks: 4,
			show_x_minor_ticks: true,
			show_y_minor_ticks: true,
			show_x_major_grid: true,
			show_y_major_grid: true,
			show_x_minor_grid: true,
			show_y_minor_grid: true,
			x_major_grid_width: 1.0,
			y_major_grid_width: 1.0,
			x_minor_grid_width: 0.5,
			y_minor_grid_width: 0.5,
			x_major_grid_style: GridLineStyle::Solid,
			y_major_grid_style: GridLineStyle::Solid,
			x_minor_grid_style: GridLineStyle::Solid,
			y_minor_grid_style: GridLineStyle::Solid,
			scatter_render_mode: ScatterRenderMode::Auto,
			scatter_max_vector_points: 50_000,
			scatter_downsample_target: 200_000,
			scatter_raster_threshold: 1_000_000,
		}
	}
}

pub trait PathBuilder {
	fn move_to(&mut self, point: Point);
	fn line_to(&mut self, point: Point);
	fn arc_to(&mut self, center: Point, radius: f32, start_angle: f32, end_angle: f32);
	fn circle(&mut self, center: Point, radius: f32);
	fn rectangle(&mut self, top_left: Point, size: iced::Size);
	fn close(&mut self);
}

pub trait PlotBackend {
	fn stroke_path(&mut self, f: &dyn Fn(&mut dyn PathBuilder), stroke: Stroke);
	fn fill_path(&mut self, f: &dyn Fn(&mut dyn PathBuilder), color: Color);
	fn fill_rectangle(&mut self, top_left: Point, size: iced::Size, color: Color);
	fn fill_text(&mut self, text: Text);
	fn supports_embedded_raster(&self) -> bool { false }
	fn supports_native_image_handle(&self) -> bool { false }
	fn draw_image_handle(
		&mut self,
		_top_left: Point,
		_size: iced::Size,
		_handle: &image::Handle,
	) {
	}
	fn draw_image_rgba(
		&mut self,
		_top_left: Point,
		_size: iced::Size,
		_width: u32,
		_height: u32,
		_rgba: &[u8],
	) {
	}
	fn translate(&mut self, translation: iced::Vector);
	fn rotate(&mut self, angle: f32);
	fn with_save(&mut self, f: &mut dyn FnMut(&mut dyn PlotBackend));
	fn with_clip(&mut self, bounds: Rectangle, f: &mut dyn FnMut(&mut dyn PlotBackend));
}

pub struct IcedBackend<'a> {
	pub frame: &'a mut Frame,
}

struct IcedPathBuilder<'a> {
	builder: &'a mut canvas::path::Builder,
}

impl<'a> PathBuilder for IcedPathBuilder<'a> {
	fn move_to(&mut self, point: Point) { self.builder.move_to(point); }
	fn line_to(&mut self, point: Point) { self.builder.line_to(point); }
	fn arc_to(&mut self, center: Point, radius: f32, start: f32, end: f32) { 
		self.builder.arc(canvas::path::Arc { 
			center, 
			radius, 
			start_angle: iced::Radians(start), 
			end_angle: iced::Radians(end) 
		}); 
	}
	fn circle(&mut self, center: Point, radius: f32) { self.builder.circle(center, radius); }
	fn rectangle(&mut self, top_left: Point, size: iced::Size) { self.builder.rectangle(top_left, size); }
	fn close(&mut self) { self.builder.close(); }
}

impl<'a> PlotBackend for IcedBackend<'a> {
	fn stroke_path(&mut self, f: &dyn Fn(&mut dyn PathBuilder), stroke: Stroke) {
		let path = Path::new(|builder| {
			let mut ipb = IcedPathBuilder { builder };
			f(&mut ipb);
		});
		self.frame.stroke(&path, stroke);
	}
	fn fill_path(&mut self, f: &dyn Fn(&mut dyn PathBuilder), color: Color) {
		let path = Path::new(|builder| {
			let mut ipb = IcedPathBuilder { builder };
			f(&mut ipb);
		});
		self.frame.fill(&path, color);
	}
	fn fill_rectangle(&mut self, top_left: Point, size: iced::Size, color: Color) {
		self.fill_path(&|builder| {
			builder.rectangle(top_left, size);
		}, color);
	}
	fn fill_text(&mut self, text: Text) {
		self.frame.fill_text(text);
	}
	fn supports_embedded_raster(&self) -> bool { true }
	fn supports_native_image_handle(&self) -> bool { true }
	fn draw_image_handle(
		&mut self,
		top_left: Point,
		size: iced::Size,
		handle: &image::Handle,
	) {
		self.frame.draw_image(
			Rectangle {
				x: top_left.x,
				y: top_left.y,
				width: size.width,
				height: size.height,
			},
			image::Image::new(handle.clone()),
		);
	}
	fn draw_image_rgba(
		&mut self,
		top_left: Point,
		size: iced::Size,
		width: u32,
		height: u32,
		rgba: &[u8],
	) {
		let image = image::Handle::from_rgba(width, height, rgba.to_vec());
		self.frame.draw_image(
			Rectangle {
				x: top_left.x,
				y: top_left.y,
				width: size.width,
				height: size.height,
			},
			image::Image::new(image),
		);
	}
	fn translate(&mut self, translation: iced::Vector) {
		self.frame.translate(translation);
	}
	fn rotate(&mut self, angle: f32) {
		self.frame.rotate(angle);
	}
	fn with_save(&mut self, f: &mut dyn FnMut(&mut dyn PlotBackend)) {
		self.frame.with_save(|frame| {
			let mut backend = IcedBackend { frame };
			f(&mut backend);
		});
	}
	fn with_clip(&mut self, bounds: Rectangle, f: &mut dyn FnMut(&mut dyn PlotBackend)) {
		self.frame.with_clip(bounds, |frame| {
			let mut backend = IcedBackend { frame };
			f(&mut backend);
		});
	}
}

pub trait PlotKernel: Send + Sync {
	fn layout(&self, settings: PlotSettings) -> PlotLayout;

	fn plot(
		&self,
		backend: &mut dyn PlotBackend,
		bounds: Rectangle,
		transform: &CoordinateTransformer,
		cursor: Cursor,
		settings: PlotSettings,
	);

	fn draw_legend(&self, _backend: &mut dyn PlotBackend, _bounds: Rectangle, _settings: PlotSettings) {}

	fn hover(&self, transform: &CoordinateTransformer, cursor: Cursor) -> Option<String>;

	fn x_label(&self) -> String {
		String::new()
	}

	fn y_label(&self) -> String {
		String::new()
	}
}

pub struct PlotWidget<'a> {
	pub kernel: &'a dyn PlotKernel,
	pub title: String,
	pub padding: f32,
	pub settings: PlotSettings,
	pub render_revision: u64,
	pub resize_render_suspended: bool,
	pub layer: PlotRenderLayer,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlotRenderLayer {
	Data,
	OverlayInteractive,
	Full,
}

#[derive(Default)]
pub struct PlotCanvasState {
	last_click: Option<std::time::Instant>,
	last_hover: Option<String>,
	plot_cache: canvas::Cache,
	overlay_cache: canvas::Cache,
	render_revision: std::cell::Cell<u64>,
	stable_bounds: std::cell::RefCell<Option<Rectangle>>,
}

impl<'a> PlotWidget<'a> {
	fn plot_area(&self, bounds: Rectangle) -> Rectangle {
		let padding_top = self.padding + self.settings.plot_padding_top;
		let padding_bottom = self.padding + self.settings.plot_padding_bottom;
		let padding_left = self.padding + self.settings.plot_padding_left;
		let padding_right = self.padding + self.settings.plot_padding_right;
		Rectangle {
			x: padding_left,
			y: padding_top,
			width: bounds.width - padding_left - padding_right,
			height: bounds.height - padding_top - padding_bottom,
		}
	}

	pub fn render(&self, backend: &mut dyn PlotBackend, bounds: Rectangle) {
		self.render_plot_layer(backend, bounds);
		self.render_overlay_layer(backend, bounds);
	}

	pub fn render_plot_layer(&self, backend: &mut dyn PlotBackend, bounds: Rectangle) {
		backend.fill_rectangle(Point::ORIGIN, bounds.size(), self.settings.background_color);
		let plot_area = self.plot_area(bounds);
		let layout = self.kernel.layout(self.settings.clone());
		let transform = CoordinateTransformer::new(&layout, plot_area);
		backend.with_save(&mut |backend| {
			backend.translate(iced::Vector::new(plot_area.x, plot_area.y));
			self.kernel.plot(
				backend,
				plot_area,
				&transform,
				Cursor::Unavailable,
				self.settings.clone(),
			);
		});
	}

	pub fn render_overlay_layer(&self, backend: &mut dyn PlotBackend, bounds: Rectangle) {
		let plot_area = self.plot_area(bounds);
		let layout = self.kernel.layout(self.settings.clone());
		let transform = CoordinateTransformer::new(&layout, plot_area);
		backend.with_save(&mut |backend| {
			backend.translate(iced::Vector::new(plot_area.x, plot_area.y));
			match &layout {
				PlotLayout::Cartesian {
					x_range, y_range, ..
				} => {
					self.draw_cartesian_grid(backend, plot_area, &transform, *x_range, *y_range);
				}
				PlotLayout::CategoricalX {
					categories,
					y_range,
				} => {
					self.draw_categorical_grid(backend, plot_area, &transform, categories, *y_range);
				}
				PlotLayout::CategoricalY {
					categories,
					x_range,
				} => {
					self.draw_categorical_y_grid(
						backend, plot_area, &transform, categories, *x_range,
					);
				}
				_ => {}
			}
		});
		let bg = self.settings.background_color;
		let canvas_w = bounds.width;
		let canvas_h = bounds.height;
		backend.fill_rectangle(Point::ORIGIN, iced::Size::new(canvas_w, plot_area.y), bg);
		backend.fill_rectangle(
			Point::new(0.0, plot_area.y + plot_area.height),
			iced::Size::new(canvas_w, canvas_h - plot_area.y - plot_area.height),
			bg,
		);
		backend.fill_rectangle(
			Point::new(0.0, plot_area.y),
			iced::Size::new(plot_area.x, plot_area.height),
			bg,
		);
		backend.fill_rectangle(
			Point::new(plot_area.x + plot_area.width, plot_area.y),
			iced::Size::new(canvas_w - plot_area.x - plot_area.width, plot_area.height),
			bg,
		);
		backend.with_save(&mut |backend| {
			backend.translate(iced::Vector::new(plot_area.x, plot_area.y));
			match &layout {
				PlotLayout::Cartesian {
					x_range, y_range, ..
				} => {
					self.draw_cartesian_axes(backend, plot_area, &transform, *x_range, *y_range);
				}
				PlotLayout::CategoricalX {
					categories,
					y_range,
				} => {
					self.draw_categorical_axes(backend, plot_area, &transform, categories, *y_range);
				}
				PlotLayout::CategoricalY {
					categories,
					x_range,
				} => {
					self.draw_categorical_y_axes(
						backend, plot_area, &transform, categories, *x_range,
					);
				}
				PlotLayout::CategoricalXY {
					x_categories,
					y_categories,
				} => {
					self.draw_categorical_xy_axes(
						backend,
						plot_area,
						&transform,
						x_categories,
						y_categories,
					);
				}
				PlotLayout::Parallel { dimensions, ranges } => {
					self.draw_parallel_axes(backend, plot_area, &transform, dimensions, ranges);
				}
				PlotLayout::Radial => {}
			}
		});
		let size_only_bounds = Rectangle::new(Point::ORIGIN, bounds.size());
		self.kernel.draw_legend(backend, size_only_bounds, self.settings.clone());
		let title = self.settings.title.as_ref().map(|s: &Arc<String>| s.to_string()).unwrap_or(self.title.clone());
		let subtitle = self.settings.subtitle.as_ref().map(|s| s.as_ref().to_string());
		let x_label = self.settings.x_label.as_ref().map(|s: &Arc<String>| s.to_string()).unwrap_or(self.kernel.x_label());
		let y_label = self.settings.y_label.as_ref().map(|s: &Arc<String>| s.to_string()).unwrap_or(self.kernel.y_label());
		backend.fill_text(Text {
			content: title,
			position: Point::new(size_only_bounds.width / 2.0, self.settings.title_offset),
			color: self.settings.decoration_color,
			size: iced::Pixels(self.settings.title_size),
			align_x: alignment::Horizontal::Center.into(),
			align_y: alignment::Vertical::Top,
			..Default::default()
		});
		if let Some(st) = subtitle {
			backend.fill_text(Text {
				content: st,
				position: Point::new(size_only_bounds.width / 2.0, self.settings.subtitle_offset),
				color: self.settings.decoration_color,
				size: iced::Pixels(self.settings.subtitle_size),
				align_x: alignment::Horizontal::Center.into(),
				align_y: alignment::Vertical::Top,
				..Default::default()
			});
		}
		if !x_label.is_empty() {
			backend.fill_text(Text {
				content: x_label,
				position: Point::new(
					plot_area.x + plot_area.width / 2.0,
					plot_area.y + plot_area.height + self.settings.x_label_padding,
				),
				color: self.settings.decoration_color,
				size: iced::Pixels(self.settings.x_label_size),
				align_x: alignment::Horizontal::Center.into(),
				align_y: alignment::Vertical::Top,
				..Default::default()
			});
		}
		if !y_label.is_empty() {
			backend.with_save(&mut |backend| {
				backend.translate(iced::Vector::new(
					plot_area.x - self.settings.y_label_padding,
					plot_area.y + plot_area.height / 2.0,
				));
				backend.rotate(-std::f32::consts::FRAC_PI_2);
				backend.fill_text(Text {
					content: y_label.clone(),
					position: Point::ORIGIN,
					color: self.settings.decoration_color,
					size: iced::Pixels(self.settings.y_label_size),
					align_x: alignment::Horizontal::Center.into(),
					align_y: alignment::Vertical::Bottom,
					..Default::default()
				});
			});
		}
	}
}

impl<'a> Program<PlotMessage> for PlotWidget<'a> {
	type State = PlotCanvasState;

	fn draw(
		&self,
		state: &Self::State,
		renderer: &Renderer,
		_theme: &Theme,
		bounds: Rectangle,
		_cursor: Cursor,
	) -> Vec<Geometry> {
		let effective_bounds = if self.resize_render_suspended {
			state.stable_bounds.borrow().unwrap_or(bounds)
		} else {
			*state.stable_bounds.borrow_mut() = Some(bounds);
			bounds
		};
		if state.render_revision.get() != self.render_revision {
			state.plot_cache.clear();
			state.overlay_cache.clear();
			state.render_revision.set(self.render_revision);
		}
		match self.layer {
			PlotRenderLayer::Data => {
				let plot_geometry = state.plot_cache.draw(renderer, effective_bounds.size(), |frame| {
					let mut backend = IcedBackend { frame };
					self.render_plot_layer(&mut backend, effective_bounds);
				});
				vec![plot_geometry]
			}
			PlotRenderLayer::OverlayInteractive => {
				let overlay_geometry =
					state.overlay_cache.draw(renderer, effective_bounds.size(), |frame| {
						let mut backend = IcedBackend { frame };
						self.render_overlay_layer(&mut backend, effective_bounds);
					});
				vec![overlay_geometry]
			}
			PlotRenderLayer::Full => {
				let plot_geometry = state.plot_cache.draw(renderer, effective_bounds.size(), |frame| {
					let mut backend = IcedBackend { frame };
					self.render_plot_layer(&mut backend, effective_bounds);
				});
				let overlay_geometry =
					state.overlay_cache.draw(renderer, effective_bounds.size(), |frame| {
						let mut backend = IcedBackend { frame };
						self.render_overlay_layer(&mut backend, effective_bounds);
					});
				vec![plot_geometry, overlay_geometry]
			}
		}
	}

	fn update(
		&self,
		state: &mut Self::State,
		event: &Event,
		bounds: Rectangle,
		cursor: Cursor,
	) -> Option<canvas::Action<PlotMessage>> {
		if self.layer == PlotRenderLayer::Data {
			return None;
		}
		match event {
			Event::Mouse(iced::mouse::Event::CursorMoved { .. }) => {
				let padding_top = self.padding + self.settings.plot_padding_top;
				let padding_bottom = self.padding + self.settings.plot_padding_bottom;
				let padding_left = self.padding + self.settings.plot_padding_left;
				let padding_right = self.padding + self.settings.plot_padding_right;
				let plot_area = Rectangle {
					x: padding_left,
					y: padding_top,
					width: bounds.width - padding_left - padding_right,
					height: bounds.height - padding_top - padding_bottom,
				};
				let layout = self.kernel.layout(self.settings.clone());
				let transform = CoordinateTransformer::new(&layout, plot_area);
				let relative_cursor = match cursor.position() {
					Some(pos) => Cursor::Available(Point::new(pos.x - bounds.x, pos.y - bounds.y)),
					None => Cursor::Unavailable,
				};
				let plot_cursor = match relative_cursor.position() {
					Some(pos) => Cursor::Available(Point::new(pos.x - plot_area.x, pos.y - plot_area.y)),
					None => Cursor::Unavailable,
				};
				let hover = self.kernel.hover(&transform, plot_cursor);
				if hover != state.last_hover {
					state.last_hover = hover.clone();
					Some(canvas::Action::publish(PlotMessage::UpdateHover(hover)))
				} else {
					None
				}
			}
			Event::Mouse(iced::mouse::Event::ButtonPressed(iced::mouse::Button::Left)) => {
				if cursor.is_over(bounds) {
					let now = std::time::Instant::now();
					if let Some(last_click) = state.last_click
						&& now.duration_since(last_click) < std::time::Duration::from_millis(500) {
							state.last_click = None;
							return Some(canvas::Action::publish(PlotMessage::ToggleSettings));
						}
					state.last_click = Some(now);
				}
				None
			}
			_ => None,
		}
	}
}

impl<'a> PlotWidget<'a> {
	fn draw_cartesian_grid(
		&self,
		backend: &mut dyn PlotBackend,
		_area: Rectangle,
		transform: &CoordinateTransformer,
		x_range: (f64, f64),
		y_range: (f64, f64),
	) {
		let grid_stroke = |width: f32, style: GridLineStyle| -> Stroke {
			let mut s = Stroke {
				style: Style::Solid(Color {
					a: 0.2,
					..self.settings.decoration_color
				}),
				width,
				..Default::default()
			};
			match style {
				GridLineStyle::Solid => {}
				GridLineStyle::Dashed => s.line_dash = canvas::LineDash { segments: &[10.0, 5.0], offset: 0 },
				GridLineStyle::Dotted => s.line_dash = canvas::LineDash { segments: &[2.0, 2.0], offset: 0 },
			}
			s
		};
		let draw_line = |backend: &mut dyn PlotBackend, p1: Point, p2: Point, stroke: Stroke| {
			backend.stroke_path(
				&|builder| {
					builder.move_to(p1);
					builder.line_to(p2);
				},
				stroke,
			);
		};
		if self.settings.show_y_minor_grid && self.settings.y_minor_ticks > 0 {
			let stroke = grid_stroke(self.settings.y_minor_grid_width, self.settings.y_minor_grid_style);
			for i in 0..self.settings.y_ticks {
				for j in 1..=self.settings.y_minor_ticks {
					let t = (i as f64 + j as f64 / (self.settings.y_minor_ticks + 1) as f64) / self.settings.y_ticks as f64;
					if t > 1.0 { continue; }
					let data_y = y_range.0 + (y_range.1 - y_range.0) * t;
					draw_line(backend, transform.cartesian(x_range.0, data_y), transform.cartesian(x_range.1, data_y), stroke);
				}
			}
		}
		if self.settings.show_x_minor_grid && self.settings.x_minor_ticks > 0 {
			let stroke = grid_stroke(self.settings.x_minor_grid_width, self.settings.x_minor_grid_style);
			for i in 0..self.settings.x_ticks {
				for j in 1..=self.settings.x_minor_ticks {
					let t = (i as f64 + j as f64 / (self.settings.x_minor_ticks + 1) as f64) / self.settings.x_ticks as f64;
					if t > 1.0 { continue; }
					let data_x = x_range.0 + (x_range.1 - x_range.0) * t;
					draw_line(backend, transform.cartesian(data_x, y_range.0), transform.cartesian(data_x, y_range.1), stroke);
				}
			}
		}
		if self.settings.show_y_major_grid {
			let stroke = grid_stroke(self.settings.y_major_grid_width, self.settings.y_major_grid_style);
			for i in 0..=self.settings.y_ticks {
				let t = i as f64 / self.settings.y_ticks as f64;
				let data_y = y_range.0 + (y_range.1 - y_range.0) * t;
				draw_line(backend, transform.cartesian(x_range.0, data_y), transform.cartesian(x_range.1, data_y), stroke);
			}
		}
		if self.settings.show_x_major_grid {
			let stroke = grid_stroke(self.settings.x_major_grid_width, self.settings.x_major_grid_style);
			for i in 0..=self.settings.x_ticks {
				let t = i as f64 / self.settings.x_ticks as f64;
				let data_x = x_range.0 + (x_range.1 - x_range.0) * t;
				draw_line(backend, transform.cartesian(data_x, y_range.0), transform.cartesian(data_x, y_range.1), stroke);
			}
		}
	}

	fn draw_cartesian_axes(
		&self,
		backend: &mut dyn PlotBackend,
		_area: Rectangle,
		transform: &CoordinateTransformer,
		x_range: (f64, f64),
		y_range: (f64, f64),
	) {
		let x_axis_type = if let PlotLayout::Cartesian { x_axis_type, .. } = transform.layout {
			*x_axis_type
		} else {
			AxisType::Linear
		};
		let y_axis_type = if let PlotLayout::Cartesian { y_axis_type, .. } = transform.layout {
			*y_axis_type
		} else {
			AxisType::Linear
		};
		let halo_stroke = Stroke {
			style: Style::Solid(self.settings.background_color),
			width: 4.0,
			..Default::default()
		};
		let axis_stroke = Stroke {
			style: Style::Solid(self.settings.decoration_color),
			width: 2.0,
			..Default::default()
		};
		let axes_path = |builder: &mut dyn PathBuilder| {
			let origin = transform.cartesian(x_range.0, y_range.0);
			let x_max = transform.cartesian(x_range.1, y_range.0);
			let y_max = transform.cartesian(x_range.0, y_range.1);
			builder.move_to(y_max);
			builder.line_to(origin);
			builder.line_to(x_max);
		};
		backend.stroke_path(&axes_path, halo_stroke);
		backend.stroke_path(&axes_path, axis_stroke);
		for i in 0..=self.settings.y_ticks {
			let t = i as f64 / self.settings.y_ticks as f64;
			let data_y = y_range.0 + (y_range.1 - y_range.0) * t;
			let p_left = transform.cartesian(x_range.0, data_y);
			backend.stroke_path(
				&|builder| {
					builder.move_to(p_left);
					builder.line_to(Point::new(p_left.x - 5.0, p_left.y));
				},
				axis_stroke,
			);
			backend.fill_text(Text {
				content: format_label(data_y, y_axis_type),
				position: Point::new(p_left.x - 10.0, p_left.y),
				color: self.settings.decoration_color,
				size: iced::Pixels(self.settings.y_tick_size),
				align_x: alignment::Horizontal::Right.into(),
				align_y: alignment::Vertical::Center,
				..Default::default()
			});
		}
		if self.settings.show_y_minor_ticks && self.settings.y_minor_ticks > 0 {
			for i in 0..self.settings.y_ticks {
				for j in 1..=self.settings.y_minor_ticks {
					let t = (i as f64 + j as f64 / (self.settings.y_minor_ticks + 1) as f64) / self.settings.y_ticks as f64;
					if t > 1.0 { continue; }
					let data_y = y_range.0 + (y_range.1 - y_range.0) * t;
					let p_left = transform.cartesian(x_range.0, data_y);
					backend.stroke_path(
						&|builder| {
							builder.move_to(p_left);
							builder.line_to(Point::new(p_left.x - 3.0, p_left.y));
						},
						axis_stroke,
					);
				}
			}
		}
		for i in 0..=self.settings.x_ticks {
			let t = i as f64 / self.settings.x_ticks as f64;
			let data_x = x_range.0 + (x_range.1 - x_range.0) * t;
			let p_bottom = transform.cartesian(data_x, y_range.0);
			backend.stroke_path(
				&|builder| {
					builder.move_to(p_bottom);
					builder.line_to(Point::new(p_bottom.x, p_bottom.y + 5.0));
				},
				axis_stroke,
			);
			backend.with_save(&mut |backend| {
				backend.translate(iced::Vector::new(
					p_bottom.x,
					p_bottom.y + self.settings.x_label_offset,
				));
				backend.rotate(self.settings.x_label_rotation.to_radians());
				backend.fill_text(Text {
					content: format_label(data_x, x_axis_type),
					position: Point::ORIGIN,
					color: self.settings.decoration_color,
					size: iced::Pixels(self.settings.x_tick_size),
					align_x: alignment::Horizontal::Center.into(),
					..Default::default()
				});
			});
		}
		if self.settings.show_x_minor_ticks && self.settings.x_minor_ticks > 0 {
			for i in 0..self.settings.x_ticks {
				for j in 1..=self.settings.x_minor_ticks {
					let t = (i as f64 + j as f64 / (self.settings.x_minor_ticks + 1) as f64) / self.settings.x_ticks as f64;
					if t > 1.0 { continue; }
					let data_x = x_range.0 + (x_range.1 - x_range.0) * t;
					let p_bottom = transform.cartesian(data_x, y_range.0);
					backend.stroke_path(
						&|builder| {
							builder.move_to(p_bottom);
							builder.line_to(Point::new(p_bottom.x, p_bottom.y + 3.0));
						},
						axis_stroke,
					);
				}
			}
		}
	}

	fn draw_categorical_grid(
		&self,
		backend: &mut dyn PlotBackend,
		_area: Rectangle,
		transform: &CoordinateTransformer,
		categories: &[String],
		y_range: (f64, f64),
	) {
		let grid_stroke = |width: f32, style: GridLineStyle| -> Stroke {
			let mut s = Stroke {
				style: Style::Solid(Color {
					a: 0.2,
					..self.settings.decoration_color
				}),
				width,
				..Default::default()
			};
			match style {
				GridLineStyle::Solid => {}
				GridLineStyle::Dashed => s.line_dash = canvas::LineDash { segments: &[10.0, 5.0], offset: 0 },
				GridLineStyle::Dotted => s.line_dash = canvas::LineDash { segments: &[2.0, 2.0], offset: 0 },
			}
			s
		};
		let draw_line = |backend: &mut dyn PlotBackend, p1: Point, p2: Point, stroke: Stroke| {
			backend.stroke_path(
				&|builder| {
					builder.move_to(p1);
					builder.line_to(p2);
				},
				stroke,
			);
		};
		let (first_cat_center, band_width) = transform.categorical(0, y_range.1);
		let left_edge = first_cat_center.x - (band_width / 2.0);
		let (last_cat_center, _) = transform.categorical(categories.len() - 1, y_range.0);
		let right_edge = last_cat_center.x + (band_width / 2.0);
		if self.settings.show_y_minor_grid && self.settings.y_minor_ticks > 0 {
			let stroke = grid_stroke(self.settings.y_minor_grid_width, self.settings.y_minor_grid_style);
			for i in 0..self.settings.y_ticks {
				for j in 1..=self.settings.y_minor_ticks {
					let t = (i as f64 + j as f64 / (self.settings.y_minor_ticks + 1) as f64) / self.settings.y_ticks as f64;
					if t > 1.0 { continue; }
					let data_y = y_range.0 + (y_range.1 - y_range.0) * t;
					let p1 = Point::new(left_edge, transform.categorical(0, data_y).0.y);
					let p2 = Point::new(right_edge, p1.y);
					draw_line(backend, p1, p2, stroke);
				}
			}
		}
		if self.settings.show_y_major_grid {
			let stroke = grid_stroke(self.settings.y_major_grid_width, self.settings.y_major_grid_style);
			for i in 0..=self.settings.y_ticks {
				let t = i as f64 / self.settings.y_ticks as f64;
				let data_y = y_range.0 + (y_range.1 - y_range.0) * t;
				let p1 = Point::new(left_edge, transform.categorical(0, data_y).0.y);
				let p2 = Point::new(right_edge, p1.y);
				draw_line(backend, p1, p2, stroke);
			}
		}
	}

	fn draw_categorical_axes(
		&self,
		backend: &mut dyn PlotBackend,
		_area: Rectangle,
		transform: &CoordinateTransformer,
		categories: &[String],
		y_range: (f64, f64),
	) {
		let halo_stroke = Stroke {
			style: Style::Solid(self.settings.background_color),
			width: 4.0,
			..Default::default()
		};
		let axis_stroke = Stroke {
			style: Style::Solid(self.settings.decoration_color),
			width: 2.0,
			..Default::default()
		};
		let (first_cat_center, band_width) = transform.categorical(0, y_range.1);
		let left_edge = first_cat_center.x - (band_width / 2.0);
		let (last_cat_center, _) = transform.categorical(categories.len() - 1, y_range.0);
		let right_edge = last_cat_center.x + (band_width / 2.0);
		let axes_path = |builder: &mut dyn PathBuilder| {
			let top_y = first_cat_center.y;
			let bottom_y = last_cat_center.y;
			builder.move_to(Point::new(left_edge, top_y));
			builder.line_to(Point::new(left_edge, bottom_y));
			builder.line_to(Point::new(right_edge, bottom_y));
		};
		backend.stroke_path(&axes_path, halo_stroke);
		backend.stroke_path(&axes_path, axis_stroke);
		for i in 0..=self.settings.y_ticks {
			let t = i as f64 / self.settings.y_ticks as f64;
			let data_y = y_range.0 + (y_range.1 - y_range.0) * t;
			let (center, band_width) = transform.categorical(0, data_y);
			let left_edge = center.x - (band_width / 2.0);
			let p_left = Point::new(left_edge, center.y);
			backend.stroke_path(
				&|builder| {
					builder.move_to(p_left);
					builder.line_to(Point::new(p_left.x - 5.0, p_left.y));
				},
				axis_stroke,
			);
			backend.fill_text(Text {
				content: format_label(data_y, AxisType::Linear),
				position: Point::new(p_left.x - 10.0, p_left.y),
				color: self.settings.decoration_color,
				size: iced::Pixels(self.settings.y_tick_size),
				align_x: alignment::Horizontal::Right.into(),
				align_y: alignment::Vertical::Center,
				..Default::default()
			});
		}
		for (i, cat) in categories.iter().enumerate() {
			let (center_px, _band_width) = transform.categorical(i, y_range.0);
			backend.stroke_path(
				&|builder| {
					builder.move_to(center_px);
					builder.line_to(Point::new(center_px.x, center_px.y + 5.0));
				},
				axis_stroke,
			);
			backend.with_save(&mut |backend| {
				backend.translate(iced::Vector::new(
					center_px.x,
					center_px.y + self.settings.x_label_offset,
				));
				backend.rotate(self.settings.x_label_rotation.to_radians());
				backend.fill_text(Text {
					content: cat.clone(),
					position: Point::ORIGIN,
					color: self.settings.decoration_color,
					size: iced::Pixels(self.settings.x_tick_size),
					align_x: alignment::Horizontal::Center.into(),
					..Default::default()
				});
			});
		}
	}

	fn draw_categorical_y_grid(
		&self,
		backend: &mut dyn PlotBackend,
		_area: Rectangle,
		transform: &CoordinateTransformer,
		categories: &[String],
		x_range: (f64, f64),
	) {
		let grid_stroke = |width: f32, style: GridLineStyle| -> Stroke {
			let mut s = Stroke {
				style: Style::Solid(Color {
					a: 0.2,
					..self.settings.decoration_color
				}),
				width,
				..Default::default()
			};
			match style {
				GridLineStyle::Solid => {}
				GridLineStyle::Dashed => s.line_dash = canvas::LineDash { segments: &[10.0, 5.0], offset: 0 },
				GridLineStyle::Dotted => s.line_dash = canvas::LineDash { segments: &[2.0, 2.0], offset: 0 },
			}
			s
		};
		let draw_line = |backend: &mut dyn PlotBackend, p1: Point, p2: Point, stroke: Stroke| {
			backend.stroke_path(
				&|builder| {
					builder.move_to(p1);
					builder.line_to(p2);
				},
				stroke,
			);
		};
		let (first_cat_center, band_height) = transform.categorical(0, x_range.0);
		let bottom_edge = first_cat_center.y + (band_height / 2.0);
		let (last_cat_center, _) = transform.categorical(categories.len() - 1, x_range.1);
		let top_edge = last_cat_center.y - (band_height / 2.0);
		if self.settings.show_x_minor_grid && self.settings.x_minor_ticks > 0 {
			let stroke = grid_stroke(self.settings.x_minor_grid_width, self.settings.x_minor_grid_style);
			for i in 0..self.settings.x_ticks {
				for j in 1..=self.settings.x_minor_ticks {
					let t = (i as f64 + j as f64 / (self.settings.x_minor_ticks + 1) as f64) / self.settings.x_ticks as f64;
					if t > 1.0 { continue; }
					let data_x = x_range.0 + (x_range.1 - x_range.0) * t;
					let x = transform.categorical(0, data_x).0.x;
					draw_line(backend, Point::new(x, top_edge), Point::new(x, bottom_edge), stroke);
				}
			}
		}
		if self.settings.show_x_major_grid {
			let stroke = grid_stroke(self.settings.x_major_grid_width, self.settings.x_major_grid_style);
			for i in 0..=self.settings.x_ticks {
				let t = i as f64 / self.settings.x_ticks as f64;
				let data_x = x_range.0 + (x_range.1 - x_range.0) * t;
				let x = transform.categorical(0, data_x).0.x;
				draw_line(backend, Point::new(x, top_edge), Point::new(x, bottom_edge), stroke);
			}
		}
	}

	fn draw_categorical_y_axes(
		&self,
		backend: &mut dyn PlotBackend,
		_area: Rectangle,
		transform: &CoordinateTransformer,
		categories: &[String],
		x_range: (f64, f64),
	) {
		let halo_stroke = Stroke {
			style: Style::Solid(self.settings.background_color),
			width: 4.0,
			..Default::default()
		};
		let axis_stroke = Stroke {
			style: Style::Solid(self.settings.decoration_color),
			width: 2.0,
			..Default::default()
		};
		let (first_cat_center, band_height) = transform.categorical(0, x_range.0);
		let bottom_edge = first_cat_center.y + (band_height / 2.0);
		let (last_cat_center, _) = transform.categorical(categories.len() - 1, x_range.1);
		let top_edge = last_cat_center.y - (band_height / 2.0);
		let axes_path = |builder: &mut dyn PathBuilder| {
			let left_x = first_cat_center.x;
			let right_x = last_cat_center.x;
			builder.move_to(Point::new(left_x, top_edge));
			builder.line_to(Point::new(left_x, bottom_edge));
			builder.line_to(Point::new(right_x, bottom_edge));
		};
		backend.stroke_path(&axes_path, halo_stroke);
		backend.stroke_path(&axes_path, axis_stroke);
		for i in 0..=self.settings.x_ticks {
			let t = i as f64 / self.settings.x_ticks as f64;
			let data_x = x_range.0 + (x_range.1 - x_range.0) * t;
			let (center, band_height) = transform.categorical(0, data_x);
			let bottom_edge = center.y + (band_height / 2.0);
			let p_bottom = Point::new(center.x, bottom_edge);
			backend.stroke_path(
				&|builder| {
					builder.move_to(p_bottom);
					builder.line_to(Point::new(p_bottom.x, p_bottom.y + 5.0));
				},
				axis_stroke,
			);
			backend.fill_text(Text {
				content: format_label(data_x, AxisType::Linear),
				position: Point::new(p_bottom.x, p_bottom.y + 10.0),
				color: self.settings.decoration_color,
				size: iced::Pixels(self.settings.x_tick_size),
				align_x: alignment::Horizontal::Center.into(),
				..Default::default()
			});
		}
		for (i, cat) in categories.iter().enumerate() {
			let (center_px, _band_height) = transform.categorical(i, x_range.0);
			backend.stroke_path(
				&|builder| {
					builder.move_to(center_px);
					builder.line_to(Point::new(center_px.x - 5.0, center_px.y));
				},
				axis_stroke,
			);
			backend.fill_text(Text {
				content: cat.clone(),
				position: Point::new(center_px.x - 10.0, center_px.y),
				color: self.settings.decoration_color,
				size: iced::Pixels(self.settings.y_tick_size),
				align_x: alignment::Horizontal::Right.into(),
				align_y: alignment::Vertical::Center,
				..Default::default()
			});
		}
	}

	fn draw_categorical_xy_axes(
		&self,
		backend: &mut dyn PlotBackend,
		_area: Rectangle,
		transform: &CoordinateTransformer,
		x_categories: &[String],
		y_categories: &[String],
	) {
		let halo_stroke = Stroke {
			style: Style::Solid(self.settings.background_color),
			width: 4.0,
			..Default::default()
		};
		let axis_stroke = Stroke {
			style: Style::Solid(self.settings.decoration_color),
			width: 2.0,
			..Default::default()
		};
		let axes_path = |builder: &mut dyn PathBuilder| {
			let (first_p, _bw, bh) = transform.categorical_2d(0, 0);
			let (last_p, bw, _bh) =
				transform.categorical_2d(x_categories.len() - 1, y_categories.len() - 1);
			let left_x = first_p.x - bw / 2.0;
			let bottom_y = first_p.y + bh / 2.0;
			let right_x = last_p.x + bw / 2.0;
			let top_y = last_p.y - bh / 2.0;
			builder.move_to(Point::new(left_x, top_y));
			builder.line_to(Point::new(left_x, bottom_y));
			builder.line_to(Point::new(right_x, bottom_y));
		};
		backend.stroke_path(&axes_path, halo_stroke);
		backend.stroke_path(&axes_path, axis_stroke);
		for (i, cat) in x_categories.iter().enumerate() {
			let (p, _bw, bh) = transform.categorical_2d(i, 0);
			let tick_x = p.x;
			let tick_y = p.y + bh / 2.0;
			backend.stroke_path(
				&|builder| {
					builder.move_to(Point::new(tick_x, tick_y));
					builder.line_to(Point::new(tick_x, tick_y + 5.0));
				},
				axis_stroke,
			);
			backend.with_save(&mut |backend| {
				backend.translate(iced::Vector::new(
					tick_x,
					tick_y + self.settings.x_label_offset,
				));
				backend.rotate(self.settings.x_label_rotation.to_radians());
				backend.fill_text(Text {
					content: cat.clone(),
					position: Point::ORIGIN,
					color: self.settings.decoration_color,
					size: iced::Pixels(self.settings.x_tick_size),
					align_x: alignment::Horizontal::Center.into(),
					..Default::default()
				});
			});
		}
		for (i, cat) in y_categories.iter().enumerate() {
			let (p, bw, _bh) = transform.categorical_2d(0, i);
			let tick_x = p.x - bw / 2.0;
			let tick_y = p.y;
			backend.stroke_path(
				&|builder| {
					builder.move_to(Point::new(tick_x, tick_y));
					builder.line_to(Point::new(tick_x - 5.0, tick_y));
				},
				axis_stroke,
			);
			backend.fill_text(Text {
				content: cat.clone(),
				position: Point::new(tick_x - 10.0, tick_y),
				color: self.settings.decoration_color,
				size: iced::Pixels(self.settings.y_tick_size),
				align_x: alignment::Horizontal::Right.into(),
				align_y: alignment::Vertical::Center,
				..Default::default()
			});
		}
	}

	fn draw_parallel_axes(
		&self,
		backend: &mut dyn PlotBackend,
		_area: Rectangle,
		transform: &CoordinateTransformer,
		dimensions: &[String],
		ranges: &[(f64, f64)],
	) {
		let halo_stroke = Stroke {
			style: Style::Solid(self.settings.background_color),
			width: 10.0,
			..Default::default()
		};
		let axis_stroke = Stroke {
			style: Style::Solid(self.settings.decoration_color),
			width: 4.0,
			..Default::default()
		};
		let tick_stroke = Stroke {
			style: Style::Solid(Color {
				a: 0.6,
				..self.settings.decoration_color
			}),
			width: 1.5,
			..Default::default()
		};
		for (i, dim) in dimensions.iter().enumerate() {
			let range = ranges[i];
			let (top_px, _) = transform.categorical(i, range.1);
			let (bottom_px, _) = transform.categorical(i, range.0);
			let axis_path = |builder: &mut dyn PathBuilder| {
				builder.move_to(top_px);
				builder.line_to(bottom_px);
			};
			backend.stroke_path(&axis_path, halo_stroke);
			backend.stroke_path(&axis_path, axis_stroke);
			for j in 0..=self.settings.y_ticks {
				let t = j as f64 / self.settings.y_ticks as f64;
				let data_y = range.0 + (range.1 - range.0) * t;
				let (p, _) = transform.categorical(i, data_y);
				backend.stroke_path(
					&|builder| {
						builder.move_to(p);
						builder.line_to(Point::new(p.x - 6.0, p.y));
					},
					tick_stroke,
				);
				backend.fill_text(Text {
					content: format_label(data_y, AxisType::Linear),
					position: Point::new(p.x - 14.0, p.y),
					color: self.settings.decoration_color,
					size: iced::Pixels(self.settings.y_tick_size),
					align_x: alignment::Horizontal::Right.into(),
					align_y: alignment::Vertical::Center,
					..Default::default()
				});
			}
			if self.settings.show_y_minor_ticks && self.settings.y_minor_ticks > 0 {
				for j in 0..self.settings.y_ticks {
					for k in 1..=self.settings.y_minor_ticks {
						let t = (j as f64 + k as f64 / (self.settings.y_minor_ticks + 1) as f64) / self.settings.y_ticks as f64;
						if t > 1.0 { continue; }
						let data_y = range.0 + (range.1 - range.0) * t;
						let (p, _) = transform.categorical(i, data_y);
						backend.stroke_path(
							&|builder| {
								builder.move_to(p);
								builder.line_to(Point::new(p.x - 3.0, p.y));
							},
							tick_stroke,
						);
					}
				}
			}
			backend.fill_text(Text {
				content: dim.clone(),
				position: Point::new(top_px.x, top_px.y - 20.0),
				color: self.settings.decoration_color,
				size: iced::Pixels(22.0),
				align_x: alignment::Horizontal::Center.into(),
				align_y: alignment::Vertical::Bottom,
				..Default::default()
			});
		}
	}
}

pub fn polars_type_to_axis_type(dt: &polars::prelude::DataType) -> AxisType {
	match dt {
		polars::prelude::DataType::Date => AxisType::Date,
		polars::prelude::DataType::Datetime(unit, _) => {
			let tu = match unit {
				polars::prelude::TimeUnit::Nanoseconds => TimeUnit::Nanoseconds,
				polars::prelude::TimeUnit::Microseconds => TimeUnit::Microseconds,
				polars::prelude::TimeUnit::Milliseconds => TimeUnit::Milliseconds,
			};
			AxisType::Datetime(tu)
		}
		polars::prelude::DataType::Time => AxisType::Time,
		_ => AxisType::Linear,
	}
}

pub fn format_label(value: f64, axis_type: AxisType) -> String {
	match axis_type {
		AxisType::Linear => {
			if value.abs() >= 1e6 || (value.abs() < 1e-3 && value != 0.0) {
				format!("{:.1e}", value)
			} else if (value.round() - value).abs() < 1e-10 {
				format!("{:.0}", value.round())
			} else {
				format!("{:.1}", value)
			}
		}
		AxisType::Date => {
			let days = value as i64;
			if let Some(date) = chrono::NaiveDate::from_num_days_from_ce_opt(days as i32 + 719163) {
				date.format("%Y-%m-%d").to_string()
			} else {
				format!("{:.1}", value)
			}
		}
		AxisType::Datetime(unit) => {
			let timestamp = value as i64;
			let naive = match unit {
				TimeUnit::Milliseconds => chrono::DateTime::from_timestamp(
					timestamp / 1_000,
					(timestamp % 1_000) as u32 * 1_000_000,
				),
				TimeUnit::Microseconds => chrono::DateTime::from_timestamp(
					timestamp / 1_000_000,
					(timestamp % 1_000_000) as u32 * 1_000,
				),
				TimeUnit::Nanoseconds => chrono::DateTime::from_timestamp(
					timestamp / 1_000_000_000,
					(timestamp % 1_000_000_000) as u32,
				),
			};
			naive
				.map(|dt: chrono::DateTime<chrono::Utc>| dt.format("%Y-%m-%d %H:%M:%S").to_string())
				.unwrap_or_else(|| format!("{:.1}", value))
		}
		AxisType::Time => {
			let total_nanos = value as i64;
			let seconds = total_nanos / 1_000_000_000;
			let nanos = total_nanos % 1_000_000_000;
			let hours = seconds / 3600;
			let minutes = (seconds % 3600) / 60;
			let seconds = seconds % 60;
			format!(
				"{:02}:{:02}:{:02}.{:03}",
				hours,
				minutes,
				seconds,
				nanos / 1_000_000
			)
		}
	}
}
