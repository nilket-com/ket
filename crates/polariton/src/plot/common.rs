use crate::coordinate::{Plot, Pt, Rect, Widget, Window};
use crate::gui::messages::PlotMessage;
use crate::plot::chrome::PlotChrome;
use crate::plot::colors::ColorTheme;
use crate::plot::legend::{LegendContent, legend_layout, render_legend};
use crate::plot::spec::{
	BarWidth, CategoryGap, LegendPosition, PointSize, ResolvedPresence, StrokeWidth,
};
use crate::plot::theme::PlotSettings;
use iced::advanced::image;
use iced::advanced::mouse::Cursor;
use iced::alignment;
use iced::widget::canvas::{self, Frame, Geometry, Path, Program, Stroke, Style, Text};
use iced::{Color, Event, Point, Rectangle, Renderer, Theme};
use std::num::NonZeroU32;
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

/// Closed major-tick placement. Count preserves the legacy interval model;
/// Step places ticks on a zero-anchored numeric lattice.
#[allow(dead_code)] // PRODUCT-0038 is the first production Rune caller.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum TickPlacement {
	Count(NonZeroU32),
	Step(PositiveFinite),
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PositiveFinite(f64);

impl TickPlacement {
	pub const DEFAULT: Self = Self::Count(NonZeroU32::new(8).unwrap());

	pub fn count(intervals: u32) -> Option<Self> {
		NonZeroU32::new(intervals).map(Self::Count)
	}

	pub fn count_value(self) -> Option<u32> {
		match self {
			Self::Count(intervals) => Some(intervals.get()),
			Self::Step(_) => None,
		}
	}

	#[allow(dead_code)] // PRODUCT-0038 is the first production Rune caller.
	pub fn step(step: f64) -> Option<Self> {
		(step.is_finite() && step > 0.0).then_some(Self::Step(PositiveFinite(step)))
	}

	#[allow(dead_code)] // PRODUCT-0038 owns destination applicability diagnostics.
	pub fn positions_for_axis(
		self,
		axis_type: AxisType,
		range: (f64, f64),
	) -> Result<Vec<f64>, TickPlacementError> {
		if matches!(self, Self::Step(_)) && !matches!(axis_type, AxisType::Linear) {
			return Err(TickPlacementError::UnsupportedAxis { axis_type });
		}
		self.positions(range)
	}

	pub fn positions(self, range: (f64, f64)) -> Result<Vec<f64>, TickPlacementError> {
		if !range.0.is_finite() || !range.1.is_finite() || range.0 > range.1 {
			return Err(TickPlacementError::InvalidDomain { range });
		}
		match self {
			Self::Count(intervals) => Ok((0..=intervals.get())
				.map(|i| range.0 + (range.1 - range.0) * (i as f64 / intervals.get() as f64))
				.collect()),
			Self::Step(PositiveFinite(step)) => {
				let first = (range.0 / step).ceil();
				let last = (range.1 / step).floor();
				if first > last {
					return Ok(Vec::new());
				}
				let count = last - first + 1.0;
				if !count.is_finite() || count > MAX_GENERATED_TICK_POSITIONS as f64 {
					return Err(TickPlacementError::Capacity {
						step,
						range,
						requested: count,
						limit: MAX_GENERATED_TICK_POSITIONS,
					});
				}
				Ok((0..count as usize)
					.map(|offset| (first + offset as f64) * step)
					.collect())
			}
		}
	}

	pub fn minor_positions(
		self,
		major: &[f64],
		minor_intervals: u32,
	) -> Result<Vec<f64>, TickPlacementError> {
		if minor_intervals == 0 {
			return Ok(Vec::new());
		}
		let major_count = major.len();
		let interval_count = major_count.saturating_sub(1);
		let minor_count = interval_count
			.checked_mul(minor_intervals as usize)
			.unwrap_or(usize::MAX);
		let total = major_count.checked_add(minor_count).unwrap_or(usize::MAX);
		if total > MAX_GENERATED_TICK_POSITIONS {
			return Err(TickPlacementError::MinorCapacity {
				placement: self,
				minor_ticks: minor_intervals,
				range: (
					*major.first().unwrap_or(&0.0),
					*major.last().unwrap_or(&0.0),
				),
				requested: total,
				limit: MAX_GENERATED_TICK_POSITIONS,
			});
		}
		match self {
			Self::Count(intervals) => {
				let range = (
					*major.first().unwrap_or(&0.0),
					*major.last().unwrap_or(&0.0),
				);
				Ok((0..intervals.get())
					.flat_map(|index| {
						(1..=minor_intervals).filter_map(move |minor| {
							let t = (index as f64 + minor as f64 / (minor_intervals + 1) as f64)
								/ intervals.get() as f64;
							(t <= 1.0).then(|| range.0 + (range.1 - range.0) * t)
						})
					})
					.collect())
			}
			Self::Step(_) => Ok(major
				.windows(2)
				.flat_map(|window| {
					(1..=minor_intervals).map(move |index| {
						window[0]
							+ (window[1] - window[0])
								* (index as f64 / (minor_intervals + 1) as f64)
					})
				})
				.collect()),
		}
	}
}

pub const MAX_GENERATED_TICK_POSITIONS: usize = 10_000;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum TickPlacementError {
	InvalidDomain {
		range: (f64, f64),
	},
	Capacity {
		step: f64,
		range: (f64, f64),
		requested: f64,
		limit: usize,
	},
	MinorCapacity {
		placement: TickPlacement,
		minor_ticks: u32,
		range: (f64, f64),
		requested: usize,
		limit: usize,
	},
	UnsupportedAxis {
		axis_type: AxisType,
	},
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

	/// Resolve a prepared category's semantic identity into the slot owned by
	/// the transform's complete display domain. PRODUCT-0065 requires this
	/// indirection because a faceted slice may omit categories while its shared
	/// axis deliberately retains their empty slots.
	pub fn category_index(&self, category: &str) -> Option<usize> {
		match self.layout {
			PlotLayout::CategoricalX { categories, .. }
			| PlotLayout::CategoricalY { categories, .. } => categories
				.iter()
				.position(|candidate| candidate == category),
			_ => None,
		}
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
			let pixel_y = self.bounds.height - ((data_y - y_range.0) * y_scale) as f32;
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
				let center_x = (category_index as f32 * band_width) + (band_width / 2.0);
				let y_delta = (y_range.1 - y_range.0).abs().max(f64::EPSILON);
				let y_scale = self.bounds.height as f64 / y_delta;
				let pixel_y = self.bounds.height - ((data_val - y_range.0) * y_scale) as f32;
				(Point::new(center_x, pixel_y), band_width)
			}
			PlotLayout::CategoricalY {
				categories,
				x_range,
			} => {
				let num_cats = categories.len().max(1) as f32;
				let band_height = self.bounds.height / num_cats;
				let center_y = (category_index as f32 * band_height) + (band_height / 2.0);
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
				let pixel_y = self.bounds.height - ((data_val - range.0) * y_scale) as f32;
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
				let center_y = (y_index as f32 * band_height) + (band_height / 2.0);
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
			let data_y = y_range.0 + ((self.bounds.height - cursor_pos.y) as f64 * y_scale);
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
	pub const ALL: [GridLineStyle; 3] = [
		GridLineStyle::Solid,
		GridLineStyle::Dashed,
		GridLineStyle::Dotted,
	];
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
pub struct ResolvedPlotSettings {
	pub max_legend_rows: u32,
	pub legend_x: f32,
	pub legend_y: f32,
	pub legend: ResolvedPresence<LegendPosition>,
	pub x_label_rotation: f32,
	pub x_label_offset: f32,
	pub color_theme: ColorTheme,
	pub chrome: PlotChrome,
	pub mark_decoration_color: Color,
	pub surrounding_background_color: Color,
	pub surrounding_decoration_color: Color,
	pub x_min: Option<f64>,
	pub x_max: Option<f64>,
	pub y_min: Option<f64>,
	pub y_max: Option<f64>,
	pub title: ResolvedPresence<Arc<String>>,
	pub subtitle: ResolvedPresence<Arc<String>>,
	pub x_label: ResolvedPresence<Arc<String>>,
	pub y_label: ResolvedPresence<Arc<String>>,
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
	pub x_tick_placement: TickPlacement,
	pub y_tick_placement: TickPlacement,
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
	pub point_size: PointSize,
	pub stroke_width: StrokeWidth,
	pub bar_width: BarWidth,
	pub category_gap: CategoryGap,
}

impl Default for ResolvedPlotSettings {
	fn default() -> Self {
		Self {
			max_legend_rows: 10,
			legend_x: 0.95,
			legend_y: 0.05,
			legend: ResolvedPresence::Fallback,
			x_label_rotation: 0.0,
			x_label_offset: 10.0,
			color_theme: ColorTheme::default(),
			chrome: PlotChrome::DEFAULT,
			mark_decoration_color: Color::WHITE,
			surrounding_background_color: Color::from_rgb(0.001, 0.001, 0.001),
			surrounding_decoration_color: Color::WHITE,
			x_min: None,
			x_max: None,
			y_min: None,
			y_max: None,
			title: ResolvedPresence::Fallback,
			subtitle: ResolvedPresence::Fallback,
			x_label: ResolvedPresence::Fallback,
			y_label: ResolvedPresence::Fallback,
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
			x_tick_placement: TickPlacement::DEFAULT,
			y_tick_placement: TickPlacement::DEFAULT,
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
			point_size: PointSize::ONE,
			stroke_width: StrokeWidth::ONE,
			bar_width: BarWidth::DEFAULT,
			category_gap: CategoryGap::DEFAULT,
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

/// Backend-independent authored plot operations.
///
/// PRODUCT-0057 oracle license: this boundary can prove emitted primitives,
/// coordinates, content, and order only when a particular implementation
/// records those fields. It cannot by itself prove production compositing,
/// clipping enforcement, antialiasing, or realized glyph extents.
pub trait PlotBackend {
	fn stroke_path(&mut self, f: &dyn Fn(&mut dyn PathBuilder), stroke: Stroke);
	fn fill_path(&mut self, f: &dyn Fn(&mut dyn PathBuilder), color: Color);
	fn fill_rectangle(&mut self, top_left: Point, size: iced::Size, color: Color);
	fn fill_text(&mut self, text: Text);
	fn supports_embedded_raster(&self) -> bool {
		false
	}
	fn supports_native_image_handle(&self) -> bool {
		false
	}
	fn draw_image_handle(&mut self, _top_left: Point, _size: iced::Size, _handle: &image::Handle) {}
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

/// The production iced canvas adapter.
///
/// PRODUCT-0057 oracle license: an offscreen capture through this adapter can
/// prove realized iced/wgpu pixels for the recorded adapter and configuration;
/// it does not generalize to SVG, tiny-skia, another driver, or another GPU.
pub struct IcedBackend<'a> {
	pub frame: &'a mut Frame,
}

struct IcedPathBuilder<'a> {
	builder: &'a mut canvas::path::Builder,
}

impl<'a> PathBuilder for IcedPathBuilder<'a> {
	fn move_to(&mut self, point: Point) {
		self.builder.move_to(point);
	}
	fn line_to(&mut self, point: Point) {
		self.builder.line_to(point);
	}
	fn arc_to(&mut self, center: Point, radius: f32, start: f32, end: f32) {
		self.builder.arc(canvas::path::Arc {
			center,
			radius,
			start_angle: iced::Radians(start),
			end_angle: iced::Radians(end),
		});
	}
	fn circle(&mut self, center: Point, radius: f32) {
		self.builder.circle(center, radius);
	}
	fn rectangle(&mut self, top_left: Point, size: iced::Size) {
		self.builder.rectangle(top_left, size);
	}
	fn close(&mut self) {
		self.builder.close();
	}
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
		self.fill_path(
			&|builder| {
				builder.rectangle(top_left, size);
			},
			color,
		);
	}
	fn fill_text(&mut self, text: Text) {
		self.frame.fill_text(text);
	}
	fn supports_embedded_raster(&self) -> bool {
		true
	}
	fn supports_native_image_handle(&self) -> bool {
		true
	}
	fn draw_image_handle(&mut self, top_left: Point, size: iced::Size, handle: &image::Handle) {
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

/// Composite plot surfaces own their cell arrangement while retaining the
/// ordinary kernel contract for marks and hover. The default is `None`, so
/// existing kernels remain byte-for-byte on the ordinary PlotWidget path.
pub trait PlotComposite: Send + Sync {
	/// Whether this composite emitted the figure-level title and subtitle.
	/// Ordinary composites leave those headings to `PlotWidget` so existing
	/// kernels retain their current ownership.
	fn owns_global_headings(&self) -> bool {
		false
	}

	fn render_plot_layer(
		&self,
		backend: &mut dyn PlotBackend,
		bounds: Rect<Widget>,
		padding: f32,
		title: &str,
		settings: &PlotSettings,
	);

	fn render_overlay_layer(
		&self,
		backend: &mut dyn PlotBackend,
		bounds: Rect<Widget>,
		padding: f32,
		title: &str,
		settings: &PlotSettings,
	);

	fn hover(
		&self,
		bounds: Rect<Widget>,
		padding: f32,
		cursor: Pt<Widget>,
		settings: &PlotSettings,
	) -> Option<String>;
}

pub trait PlotKernel: Send + Sync {
	fn layout(&self, settings: ResolvedPlotSettings) -> PlotLayout;

	fn plot(
		&self,
		backend: &mut dyn PlotBackend,
		bounds: Rectangle,
		transform: &CoordinateTransformer,
		cursor: Cursor,
		settings: ResolvedPlotSettings,
	);

	/// PRODUCT-0017 seam: ordinary kernels still consume one flattened setting;
	/// a layered kernel may resolve the accepted layer tier per mark renderer.
	fn plot_with_settings(
		&self,
		backend: &mut dyn PlotBackend,
		bounds: Rectangle,
		transform: &CoordinateTransformer,
		cursor: Cursor,
		settings: &PlotSettings,
	) {
		self.plot(backend, bounds, transform, cursor, settings.resolve());
	}

	fn legend_content(&self, _settings: &ResolvedPlotSettings) -> Option<LegendContent> {
		None
	}

	fn hover(&self, transform: &CoordinateTransformer, cursor: Cursor) -> Option<String>;

	fn hover_with_settings(
		&self,
		transform: &CoordinateTransformer,
		cursor: Cursor,
		_settings: &PlotSettings,
	) -> Option<String> {
		self.hover(transform, cursor)
	}

	fn composite(&self) -> Option<&dyn PlotComposite> {
		None
	}

	fn x_label(&self) -> String {
		String::new()
	}

	fn y_label(&self) -> String {
		String::new()
	}
}

/// Geometry returned by a mark renderer. This is deliberately not hover prose:
/// PRODUCT-0013's temporary interaction bridge decides how a hit is presented.
#[derive(Clone, Copy, Debug, PartialEq)]
#[allow(dead_code)] // The first production caller is PRODUCT-0014.
pub struct MarkHit {
	pub data_x: f64,
	pub data_y: f64,
	pub mark_index: usize,
}

/// A plot layer's mark-only rendering boundary. It owns neither layout nor
/// guides, labels, or interaction policy.
#[allow(dead_code)] // Geometric hit-testing is test-reachable until PRODUCT-0014.
pub trait MarkRenderer: Send + Sync {
	fn draw(
		&self,
		backend: &mut dyn PlotBackend,
		bounds: Rectangle,
		transform: &CoordinateTransformer,
		settings: ResolvedPlotSettings,
	);

	fn hit_test(
		&self,
		transform: &CoordinateTransformer,
		cursor: Cursor,
		settings: &ResolvedPlotSettings,
	) -> Option<MarkHit>;
}

pub struct PlotWidget<'a> {
	pub kernel: &'a dyn PlotKernel,
	pub title: String,
	pub padding: f32,
	pub settings: ResolvedPlotSettings,
	pub settings_model: PlotSettings,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AxisTextVisibility {
	pub x: bool,
	pub y: bool,
}

impl AxisTextVisibility {
	pub const ALL: Self = Self { x: true, y: true };
}

fn resolve_canvas_text(
	presence: &ResolvedPresence<Arc<String>>,
	fallback: Option<String>,
) -> Option<String> {
	match presence {
		ResolvedPresence::Fallback => fallback,
		ResolvedPresence::Show(value) => Some(value.as_ref().to_owned()),
		ResolvedPresence::Hide => None,
	}
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
	fn major_tick_positions(&self, placement: TickPlacement, range: (f64, f64)) -> Vec<f64> {
		placement.positions(range).unwrap_or_default()
	}

	fn minor_tick_positions(
		&self,
		placement: TickPlacement,
		range: (f64, f64),
		minor_intervals: u32,
	) -> Vec<f64> {
		let major = self.major_tick_positions(placement, range);
		placement
			.minor_positions(&major, minor_intervals)
			.unwrap_or_default()
	}

	fn plot_area(&self, bounds: Rect<Widget>) -> Rect<Widget> {
		let padding_top = self.padding + self.settings.plot_padding_top;
		let padding_bottom = self.padding + self.settings.plot_padding_bottom;
		let padding_left = self.padding + self.settings.plot_padding_left;
		let padding_right = self.padding + self.settings.plot_padding_right;
		bounds.region(
			padding_left,
			padding_top,
			bounds.width() - padding_left - padding_right,
			bounds.height() - padding_top - padding_bottom,
		)
	}

	pub fn render(&self, backend: &mut dyn PlotBackend, bounds: Rectangle) {
		self.render_plot_layer(backend, bounds);
		self.render_overlay_layer(backend, bounds);
	}

	pub fn render_plot_layer(&self, backend: &mut dyn PlotBackend, bounds: Rectangle) {
		self.render_plot_layer_in_widget(backend, Rect::from_iced_frame(bounds));
	}

	fn render_plot_layer_in_widget(&self, backend: &mut dyn PlotBackend, bounds: Rect<Widget>) {
		if let Some(composite) = self.kernel.composite() {
			composite.render_plot_layer(
				backend,
				bounds,
				self.padding,
				&self.title,
				&self.settings_model,
			);
			return;
		}
		backend.fill_rectangle(
			Point::ORIGIN,
			bounds.size(),
			self.settings.chrome.canvas_background.iced(),
		);
		let legend_content = self
			.kernel
			.legend_content(&self.settings)
			.and_then(crate::plot::legend::apply_policy);
		let legend = legend_layout(bounds, legend_content.as_ref(), &self.settings);
		let plot_area = self.plot_area(legend.content_bounds).into_iced();
		let layout = self.kernel.layout(self.settings.clone());
		let transform = CoordinateTransformer::new(&layout, plot_area);
		backend.with_save(&mut |backend| {
			backend.translate(iced::Vector::new(plot_area.x, plot_area.y));
			self.draw_layout_grid(backend, plot_area, &transform, &layout);
			self.kernel.plot_with_settings(
				backend,
				plot_area,
				&transform,
				Cursor::Unavailable,
				&self.settings_model,
			);
		});
	}

	pub fn render_overlay_layer(&self, backend: &mut dyn PlotBackend, bounds: Rectangle) {
		self.render_overlay_layer_in_widget(backend, Rect::from_iced_frame(bounds));
	}

	fn render_overlay_layer_in_widget(&self, backend: &mut dyn PlotBackend, bounds: Rect<Widget>) {
		if let Some(composite) = self.kernel.composite() {
			composite.render_overlay_layer(
				backend,
				bounds,
				self.padding,
				&self.title,
				&self.settings_model,
			);
			if !composite.owns_global_headings() {
				self.render_global_headings(backend, bounds.into_iced());
			}
			return;
		}
		let legend_content = self
			.kernel
			.legend_content(&self.settings)
			.and_then(crate::plot::legend::apply_policy);
		let legend = legend_layout(bounds, legend_content.as_ref(), &self.settings);
		let plot_extent = self.plot_area(legend.content_bounds);
		let plot_area = plot_extent.into_iced();
		let bounds = bounds.into_iced();
		let layout = self.kernel.layout(self.settings.clone());
		let transform = CoordinateTransformer::new(&layout, plot_area);
		let bg = self.settings.chrome.canvas_background.iced();
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
					self.draw_cartesian_axes(
						backend,
						plot_area,
						&transform,
						*x_range,
						*y_range,
						AxisTextVisibility::ALL,
					);
				}
				PlotLayout::CategoricalX {
					categories,
					y_range,
				} => {
					self.draw_categorical_axes(
						backend,
						plot_area,
						&transform,
						categories,
						*y_range,
						AxisTextVisibility::ALL,
					);
				}
				PlotLayout::CategoricalY {
					categories,
					x_range,
				} => {
					self.draw_categorical_y_axes(
						backend,
						plot_area,
						&transform,
						categories,
						*x_range,
						AxisTextVisibility::ALL,
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
						AxisTextVisibility::ALL,
					);
				}
				PlotLayout::Parallel { dimensions, ranges } => {
					self.draw_parallel_axes(
						backend,
						plot_area,
						&transform,
						dimensions,
						ranges,
						AxisTextVisibility::ALL,
					);
				}
				PlotLayout::Radial => {}
			}
		});
		let size_only_bounds = Rectangle::new(Point::ORIGIN, bounds.size());
		if let (Some(content), Some(origin)) = (legend_content.as_ref(), legend.origin(plot_extent))
		{
			render_legend(backend, origin, content, &self.settings);
		}
		let title = resolve_canvas_text(&self.settings.title, Some(self.title.clone()));
		let subtitle = resolve_canvas_text(&self.settings.subtitle, None);
		if let Some(title) = title {
			backend.fill_text(Text {
				content: title,
				position: Point::new(size_only_bounds.width / 2.0, self.settings.title_offset),
				color: self.settings.chrome.title_text.iced(),
				size: iced::Pixels(self.settings.title_size),
				align_x: alignment::Horizontal::Center.into(),
				align_y: alignment::Vertical::Top,
				..Default::default()
			});
		}
		if let Some(st) = subtitle {
			backend.fill_text(Text {
				content: st,
				position: Point::new(size_only_bounds.width / 2.0, self.settings.subtitle_offset),
				color: self.settings.chrome.subtitle_text.iced(),
				size: iced::Pixels(self.settings.subtitle_size),
				align_x: alignment::Horizontal::Center.into(),
				align_y: alignment::Vertical::Top,
				..Default::default()
			});
		}
		self.draw_axis_labels(backend, plot_area);
	}

	fn render_global_headings(&self, backend: &mut dyn PlotBackend, bounds: Rectangle) {
		if let Some(title) = resolve_canvas_text(&self.settings.title, Some(self.title.clone())) {
			backend.fill_text(Text {
				content: title,
				position: Point::new(bounds.width / 2.0, self.settings.title_offset),
				color: self.settings.chrome.title_text.iced(),
				size: iced::Pixels(self.settings.title_size),
				align_x: alignment::Horizontal::Center.into(),
				align_y: alignment::Vertical::Top,
				..Default::default()
			});
		}
		if let Some(subtitle) = resolve_canvas_text(&self.settings.subtitle, None) {
			backend.fill_text(Text {
				content: subtitle,
				position: Point::new(bounds.width / 2.0, self.settings.subtitle_offset),
				color: self.settings.chrome.subtitle_text.iced(),
				size: iced::Pixels(self.settings.subtitle_size),
				align_x: alignment::Horizontal::Center.into(),
				align_y: alignment::Vertical::Top,
				..Default::default()
			});
		}
	}

	pub(crate) fn draw_axis_labels(&self, backend: &mut dyn PlotBackend, plot_area: Rectangle) {
		let x_label = resolve_canvas_text(&self.settings.x_label, Some(self.kernel.x_label()));
		let y_label = resolve_canvas_text(&self.settings.y_label, Some(self.kernel.y_label()));
		if let Some(x_label) = x_label.filter(|label| !label.is_empty()) {
			backend.fill_text(Text {
				content: x_label,
				position: Point::new(
					plot_area.x + plot_area.width / 2.0,
					plot_area.y + plot_area.height + self.settings.x_label_padding,
				),
				color: self.settings.chrome.axis_label_text.iced(),
				size: iced::Pixels(self.settings.x_label_size),
				align_x: alignment::Horizontal::Center.into(),
				align_y: alignment::Vertical::Top,
				..Default::default()
			});
		}
		if let Some(y_label) = y_label.filter(|label| !label.is_empty()) {
			backend.with_save(&mut |backend| {
				backend.translate(iced::Vector::new(
					plot_area.x - self.settings.y_label_padding,
					plot_area.y + plot_area.height / 2.0,
				));
				backend.rotate(-std::f32::consts::FRAC_PI_2);
				backend.fill_text(Text {
					content: y_label.clone(),
					position: Point::ORIGIN,
					color: self.settings.chrome.axis_label_text.iced(),
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
				let plot_geometry =
					state
						.plot_cache
						.draw(renderer, effective_bounds.size(), |frame| {
							let mut backend = IcedBackend { frame };
							self.render_plot_layer(&mut backend, effective_bounds);
						});
				vec![plot_geometry]
			}
			PlotRenderLayer::OverlayInteractive => {
				let overlay_geometry =
					state
						.overlay_cache
						.draw(renderer, effective_bounds.size(), |frame| {
							let mut backend = IcedBackend { frame };
							self.render_overlay_layer(&mut backend, effective_bounds);
						});
				vec![overlay_geometry]
			}
			PlotRenderLayer::Full => {
				let plot_geometry =
					state
						.plot_cache
						.draw(renderer, effective_bounds.size(), |frame| {
							let mut backend = IcedBackend { frame };
							self.render_plot_layer(&mut backend, effective_bounds);
						});
				let overlay_geometry =
					state
						.overlay_cache
						.draw(renderer, effective_bounds.size(), |frame| {
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
				let window_bounds = Rect::<Window>::from_iced_layout(bounds);
				let widget_bounds = Rect::<Widget>::from_iced_frame(bounds);
				let widget_cursor = cursor.position().map(|position| {
					Pt::<Widget>::from_window(
						Pt::<Window>::from_iced_event(position),
						window_bounds,
					)
				});
				if let Some(composite) = self.kernel.composite() {
					let hover = widget_cursor.and_then(|cursor| {
						composite.hover(widget_bounds, self.padding, cursor, &self.settings_model)
					});
					if hover != state.last_hover {
						state.last_hover = hover.clone();
						return Some(canvas::Action::publish(PlotMessage::UpdateHover(hover)));
					}
					return None;
				}
				let legend_content = self
					.kernel
					.legend_content(&self.settings)
					.and_then(crate::plot::legend::apply_policy);
				let legend = legend_layout(widget_bounds, legend_content.as_ref(), &self.settings);
				let plot_area = self.plot_area(legend.content_bounds);
				let layout = self.kernel.layout(self.settings.clone());
				let transform = CoordinateTransformer::new(&layout, plot_area.into_iced());
				let plot_cursor = widget_cursor
					.map(|cursor| Pt::<Plot>::from_widget(cursor, plot_area))
					.map_or(Cursor::Unavailable, |cursor| {
						Cursor::Available(cursor.into_iced())
					});
				let hover =
					self.kernel
						.hover_with_settings(&transform, plot_cursor, &self.settings_model);
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
						&& now.duration_since(last_click) < std::time::Duration::from_millis(500)
					{
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
	pub(crate) fn draw_layout_grid(
		&self,
		backend: &mut dyn PlotBackend,
		area: Rectangle,
		transform: &CoordinateTransformer,
		layout: &PlotLayout,
	) {
		match layout {
			PlotLayout::Cartesian {
				x_range, y_range, ..
			} => self.draw_cartesian_grid(backend, area, transform, *x_range, *y_range),
			PlotLayout::CategoricalX {
				categories,
				y_range,
			} => self.draw_categorical_grid(backend, area, transform, categories, *y_range),
			PlotLayout::CategoricalY {
				categories,
				x_range,
			} => self.draw_categorical_y_grid(backend, area, transform, categories, *x_range),
			_ => {}
		}
	}

	pub(crate) fn draw_cartesian_grid(
		&self,
		backend: &mut dyn PlotBackend,
		_area: Rectangle,
		transform: &CoordinateTransformer,
		x_range: (f64, f64),
		y_range: (f64, f64),
	) {
		let grid_stroke = |width: f32, style: GridLineStyle| -> Stroke {
			let mut s = Stroke {
				style: Style::Solid(self.settings.chrome.gridline.iced()),
				width,
				..Default::default()
			};
			match style {
				GridLineStyle::Solid => {}
				GridLineStyle::Dashed => {
					s.line_dash = canvas::LineDash {
						segments: &[10.0, 5.0],
						offset: 0,
					}
				}
				GridLineStyle::Dotted => {
					s.line_dash = canvas::LineDash {
						segments: &[2.0, 2.0],
						offset: 0,
					}
				}
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
			let stroke = grid_stroke(
				self.settings.y_minor_grid_width,
				self.settings.y_minor_grid_style,
			);
			for data_y in self.minor_tick_positions(
				self.settings.y_tick_placement,
				y_range,
				self.settings.y_minor_ticks,
			) {
				draw_line(
					backend,
					transform.cartesian(x_range.0, data_y),
					transform.cartesian(x_range.1, data_y),
					stroke,
				);
			}
		}
		if self.settings.show_x_minor_grid && self.settings.x_minor_ticks > 0 {
			let stroke = grid_stroke(
				self.settings.x_minor_grid_width,
				self.settings.x_minor_grid_style,
			);
			for data_x in self.minor_tick_positions(
				self.settings.x_tick_placement,
				x_range,
				self.settings.x_minor_ticks,
			) {
				draw_line(
					backend,
					transform.cartesian(data_x, y_range.0),
					transform.cartesian(data_x, y_range.1),
					stroke,
				);
			}
		}
		if self.settings.show_y_major_grid {
			let stroke = grid_stroke(
				self.settings.y_major_grid_width,
				self.settings.y_major_grid_style,
			);
			for data_y in self.major_tick_positions(self.settings.y_tick_placement, y_range) {
				draw_line(
					backend,
					transform.cartesian(x_range.0, data_y),
					transform.cartesian(x_range.1, data_y),
					stroke,
				);
			}
		}
		if self.settings.show_x_major_grid {
			let stroke = grid_stroke(
				self.settings.x_major_grid_width,
				self.settings.x_major_grid_style,
			);
			for data_x in self.major_tick_positions(self.settings.x_tick_placement, x_range) {
				draw_line(
					backend,
					transform.cartesian(data_x, y_range.0),
					transform.cartesian(data_x, y_range.1),
					stroke,
				);
			}
		}
	}

	pub(crate) fn draw_cartesian_axes(
		&self,
		backend: &mut dyn PlotBackend,
		_area: Rectangle,
		transform: &CoordinateTransformer,
		x_range: (f64, f64),
		y_range: (f64, f64),
		text: AxisTextVisibility,
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
			style: Style::Solid(self.settings.chrome.canvas_background.iced()),
			width: 4.0,
			..Default::default()
		};
		let axis_stroke = Stroke {
			style: Style::Solid(self.settings.chrome.axis_line.iced()),
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
		for data_y in self.major_tick_positions(self.settings.y_tick_placement, y_range) {
			let p_left = transform.cartesian(x_range.0, data_y);
			backend.stroke_path(
				&|builder| {
					builder.move_to(p_left);
					builder.line_to(Point::new(p_left.x - 5.0, p_left.y));
				},
				axis_stroke,
			);
			if text.y {
				backend.fill_text(Text {
					content: format_label(data_y, y_axis_type),
					position: Point::new(p_left.x - 10.0, p_left.y),
					color: self.settings.chrome.tick_label_text.iced(),
					size: iced::Pixels(self.settings.y_tick_size),
					align_x: alignment::Horizontal::Right.into(),
					align_y: alignment::Vertical::Center,
					..Default::default()
				});
			}
		}
		if self.settings.show_y_minor_ticks && self.settings.y_minor_ticks > 0 {
			for data_y in self.minor_tick_positions(
				self.settings.y_tick_placement,
				y_range,
				self.settings.y_minor_ticks,
			) {
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
		for data_x in self.major_tick_positions(self.settings.x_tick_placement, x_range) {
			let p_bottom = transform.cartesian(data_x, y_range.0);
			backend.stroke_path(
				&|builder| {
					builder.move_to(p_bottom);
					builder.line_to(Point::new(p_bottom.x, p_bottom.y + 5.0));
				},
				axis_stroke,
			);
			if text.x {
				backend.with_save(&mut |backend| {
					backend.translate(iced::Vector::new(
						p_bottom.x,
						p_bottom.y + self.settings.x_label_offset,
					));
					backend.rotate(self.settings.x_label_rotation.to_radians());
					backend.fill_text(Text {
						content: format_label(data_x, x_axis_type),
						position: Point::ORIGIN,
						color: self.settings.chrome.tick_label_text.iced(),
						size: iced::Pixels(self.settings.x_tick_size),
						align_x: alignment::Horizontal::Center.into(),
						..Default::default()
					});
				});
			}
		}
		if self.settings.show_x_minor_ticks && self.settings.x_minor_ticks > 0 {
			for data_x in self.minor_tick_positions(
				self.settings.x_tick_placement,
				x_range,
				self.settings.x_minor_ticks,
			) {
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

	pub(crate) fn draw_categorical_grid(
		&self,
		backend: &mut dyn PlotBackend,
		_area: Rectangle,
		transform: &CoordinateTransformer,
		categories: &[String],
		y_range: (f64, f64),
	) {
		let grid_stroke = |width: f32, style: GridLineStyle| -> Stroke {
			let mut s = Stroke {
				style: Style::Solid(self.settings.chrome.gridline.iced()),
				width,
				..Default::default()
			};
			match style {
				GridLineStyle::Solid => {}
				GridLineStyle::Dashed => {
					s.line_dash = canvas::LineDash {
						segments: &[10.0, 5.0],
						offset: 0,
					}
				}
				GridLineStyle::Dotted => {
					s.line_dash = canvas::LineDash {
						segments: &[2.0, 2.0],
						offset: 0,
					}
				}
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
			let stroke = grid_stroke(
				self.settings.y_minor_grid_width,
				self.settings.y_minor_grid_style,
			);
			for data_y in self.minor_tick_positions(
				self.settings.y_tick_placement,
				y_range,
				self.settings.y_minor_ticks,
			) {
				let p1 = Point::new(left_edge, transform.categorical(0, data_y).0.y);
				let p2 = Point::new(right_edge, p1.y);
				draw_line(backend, p1, p2, stroke);
			}
		}
		if self.settings.show_y_major_grid {
			let stroke = grid_stroke(
				self.settings.y_major_grid_width,
				self.settings.y_major_grid_style,
			);
			for data_y in self.major_tick_positions(self.settings.y_tick_placement, y_range) {
				let p1 = Point::new(left_edge, transform.categorical(0, data_y).0.y);
				let p2 = Point::new(right_edge, p1.y);
				draw_line(backend, p1, p2, stroke);
			}
		}
	}

	pub(crate) fn draw_categorical_axes(
		&self,
		backend: &mut dyn PlotBackend,
		_area: Rectangle,
		transform: &CoordinateTransformer,
		categories: &[String],
		y_range: (f64, f64),
		text: AxisTextVisibility,
	) {
		let halo_stroke = Stroke {
			style: Style::Solid(self.settings.chrome.canvas_background.iced()),
			width: 4.0,
			..Default::default()
		};
		let axis_stroke = Stroke {
			style: Style::Solid(self.settings.chrome.axis_line.iced()),
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
		for data_y in self.major_tick_positions(self.settings.y_tick_placement, y_range) {
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
			if text.y {
				backend.fill_text(Text {
					content: format_label(data_y, AxisType::Linear),
					position: Point::new(p_left.x - 10.0, p_left.y),
					color: self.settings.chrome.tick_label_text.iced(),
					size: iced::Pixels(self.settings.y_tick_size),
					align_x: alignment::Horizontal::Right.into(),
					align_y: alignment::Vertical::Center,
					..Default::default()
				});
			}
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
			if text.x {
				backend.with_save(&mut |backend| {
					backend.translate(iced::Vector::new(
						center_px.x,
						center_px.y + self.settings.x_label_offset,
					));
					backend.rotate(self.settings.x_label_rotation.to_radians());
					backend.fill_text(Text {
						content: cat.clone(),
						position: Point::ORIGIN,
						color: self.settings.chrome.tick_label_text.iced(),
						size: iced::Pixels(self.settings.x_tick_size),
						align_x: alignment::Horizontal::Center.into(),
						..Default::default()
					});
				});
			}
		}
	}

	pub(crate) fn draw_categorical_y_grid(
		&self,
		backend: &mut dyn PlotBackend,
		_area: Rectangle,
		transform: &CoordinateTransformer,
		_categories: &[String],
		x_range: (f64, f64),
	) {
		let grid_stroke = |width: f32, style: GridLineStyle| -> Stroke {
			let mut s = Stroke {
				style: Style::Solid(self.settings.chrome.gridline.iced()),
				width,
				..Default::default()
			};
			match style {
				GridLineStyle::Solid => {}
				GridLineStyle::Dashed => {
					s.line_dash = canvas::LineDash {
						segments: &[10.0, 5.0],
						offset: 0,
					}
				}
				GridLineStyle::Dotted => {
					s.line_dash = canvas::LineDash {
						segments: &[2.0, 2.0],
						offset: 0,
					}
				}
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
		let top_edge = 0.0;
		let bottom_edge = transform.bounds.height;
		if self.settings.show_x_minor_grid && self.settings.x_minor_ticks > 0 {
			let stroke = grid_stroke(
				self.settings.x_minor_grid_width,
				self.settings.x_minor_grid_style,
			);
			for data_x in self.minor_tick_positions(
				self.settings.x_tick_placement,
				x_range,
				self.settings.x_minor_ticks,
			) {
				let x = transform.categorical(0, data_x).0.x;
				draw_line(
					backend,
					Point::new(x, top_edge),
					Point::new(x, bottom_edge),
					stroke,
				);
			}
		}
		if self.settings.show_x_major_grid {
			let stroke = grid_stroke(
				self.settings.x_major_grid_width,
				self.settings.x_major_grid_style,
			);
			for data_x in self.major_tick_positions(self.settings.x_tick_placement, x_range) {
				let x = transform.categorical(0, data_x).0.x;
				draw_line(
					backend,
					Point::new(x, top_edge),
					Point::new(x, bottom_edge),
					stroke,
				);
			}
		}
	}

	pub(crate) fn draw_categorical_y_axes(
		&self,
		backend: &mut dyn PlotBackend,
		_area: Rectangle,
		transform: &CoordinateTransformer,
		categories: &[String],
		x_range: (f64, f64),
		text: AxisTextVisibility,
	) {
		let halo_stroke = Stroke {
			style: Style::Solid(self.settings.chrome.canvas_background.iced()),
			width: 4.0,
			..Default::default()
		};
		let axis_stroke = Stroke {
			style: Style::Solid(self.settings.chrome.axis_line.iced()),
			width: 2.0,
			..Default::default()
		};
		let top_edge = 0.0;
		let bottom_edge = transform.bounds.height;
		let (left_edge, _) = transform.categorical(0, x_range.0);
		let (right_edge, _) = transform.categorical(0, x_range.1);
		let axes_path = |builder: &mut dyn PathBuilder| {
			builder.move_to(Point::new(left_edge.x, top_edge));
			builder.line_to(Point::new(left_edge.x, bottom_edge));
			builder.line_to(Point::new(right_edge.x, bottom_edge));
		};
		backend.stroke_path(&axes_path, halo_stroke);
		backend.stroke_path(&axes_path, axis_stroke);
		for data_x in self.major_tick_positions(self.settings.x_tick_placement, x_range) {
			let (center, _) = transform.categorical(0, data_x);
			let p_bottom = Point::new(center.x, bottom_edge);
			backend.stroke_path(
				&|builder| {
					builder.move_to(p_bottom);
					builder.line_to(Point::new(p_bottom.x, p_bottom.y + 5.0));
				},
				axis_stroke,
			);
			if text.x {
				backend.fill_text(Text {
					content: format_label(data_x, AxisType::Linear),
					position: Point::new(p_bottom.x, p_bottom.y + 10.0),
					color: self.settings.chrome.tick_label_text.iced(),
					size: iced::Pixels(self.settings.x_tick_size),
					align_x: alignment::Horizontal::Center.into(),
					..Default::default()
				});
			}
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
			if text.y {
				backend.fill_text(Text {
					content: cat.clone(),
					position: Point::new(center_px.x - 10.0, center_px.y),
					color: self.settings.chrome.tick_label_text.iced(),
					size: iced::Pixels(self.settings.y_tick_size),
					align_x: alignment::Horizontal::Right.into(),
					align_y: alignment::Vertical::Center,
					..Default::default()
				});
			}
		}
	}

	pub(crate) fn draw_categorical_xy_axes(
		&self,
		backend: &mut dyn PlotBackend,
		_area: Rectangle,
		transform: &CoordinateTransformer,
		x_categories: &[String],
		y_categories: &[String],
		text: AxisTextVisibility,
	) {
		let halo_stroke = Stroke {
			style: Style::Solid(self.settings.chrome.canvas_background.iced()),
			width: 4.0,
			..Default::default()
		};
		let axis_stroke = Stroke {
			style: Style::Solid(self.settings.chrome.axis_line.iced()),
			width: 2.0,
			..Default::default()
		};
		let axes_path = |builder: &mut dyn PathBuilder| {
			let left_x = 0.0;
			let bottom_y = transform.bounds.height;
			let right_x = transform.bounds.width;
			let top_y = 0.0;
			builder.move_to(Point::new(left_x, top_y));
			builder.line_to(Point::new(left_x, bottom_y));
			builder.line_to(Point::new(right_x, bottom_y));
		};
		backend.stroke_path(&axes_path, halo_stroke);
		backend.stroke_path(&axes_path, axis_stroke);
		for (i, cat) in x_categories.iter().enumerate() {
			let (p, _bw, _bh) = transform.categorical_2d(i, 0);
			let tick_x = p.x;
			let tick_y = transform.bounds.height;
			backend.stroke_path(
				&|builder| {
					builder.move_to(Point::new(tick_x, tick_y));
					builder.line_to(Point::new(tick_x, tick_y + 5.0));
				},
				axis_stroke,
			);
			if text.x {
				backend.with_save(&mut |backend| {
					backend.translate(iced::Vector::new(
						tick_x,
						tick_y + self.settings.x_label_offset,
					));
					backend.rotate(self.settings.x_label_rotation.to_radians());
					backend.fill_text(Text {
						content: cat.clone(),
						position: Point::ORIGIN,
						color: self.settings.chrome.tick_label_text.iced(),
						size: iced::Pixels(self.settings.x_tick_size),
						align_x: alignment::Horizontal::Center.into(),
						..Default::default()
					});
				});
			}
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
			if text.y {
				backend.fill_text(Text {
					content: cat.clone(),
					position: Point::new(tick_x - 10.0, tick_y),
					color: self.settings.chrome.tick_label_text.iced(),
					size: iced::Pixels(self.settings.y_tick_size),
					align_x: alignment::Horizontal::Right.into(),
					align_y: alignment::Vertical::Center,
					..Default::default()
				});
			}
		}
	}

	pub(crate) fn draw_parallel_axes(
		&self,
		backend: &mut dyn PlotBackend,
		_area: Rectangle,
		transform: &CoordinateTransformer,
		dimensions: &[String],
		ranges: &[(f64, f64)],
		text: AxisTextVisibility,
	) {
		let halo_stroke = Stroke {
			style: Style::Solid(self.settings.chrome.canvas_background.iced()),
			width: 10.0,
			..Default::default()
		};
		let axis_stroke = Stroke {
			style: Style::Solid(self.settings.chrome.axis_line.iced()),
			width: 4.0,
			..Default::default()
		};
		let tick_stroke = Stroke {
			style: Style::Solid(Color {
				a: 0.6,
				..self.settings.chrome.axis_line.iced()
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
			for data_y in self.major_tick_positions(self.settings.y_tick_placement, range) {
				let (p, _) = transform.categorical(i, data_y);
				backend.stroke_path(
					&|builder| {
						builder.move_to(p);
						builder.line_to(Point::new(p.x - 6.0, p.y));
					},
					tick_stroke,
				);
				if text.y {
					backend.fill_text(Text {
						content: format_label(data_y, AxisType::Linear),
						position: Point::new(p.x - 14.0, p.y),
						color: self.settings.chrome.tick_label_text.iced(),
						size: iced::Pixels(self.settings.y_tick_size),
						align_x: alignment::Horizontal::Right.into(),
						align_y: alignment::Vertical::Center,
						..Default::default()
					});
				}
			}
			if self.settings.show_y_minor_ticks && self.settings.y_minor_ticks > 0 {
				for data_y in self.minor_tick_positions(
					self.settings.y_tick_placement,
					range,
					self.settings.y_minor_ticks,
				) {
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
			if text.x {
				backend.fill_text(Text {
					content: dim.clone(),
					position: Point::new(top_px.x, top_px.y - 20.0),
					color: self.settings.chrome.axis_label_text.iced(),
					size: iced::Pixels(22.0),
					align_x: alignment::Horizontal::Center.into(),
					align_y: alignment::Vertical::Bottom,
					..Default::default()
				});
			}
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

#[cfg(test)]
mod product_0031_pre_extraction_oracle {
	use super::*;
	use crate::plot::export::SvgBackend;
	use crate::plot::theme::PlotSettings;
	use iced::advanced::mouse::Cursor;
	use iced::widget::canvas::{Stroke, Style};

	struct OracleKernel {
		legend: bool,
	}

	impl PlotKernel for OracleKernel {
		fn layout(&self, _settings: ResolvedPlotSettings) -> PlotLayout {
			PlotLayout::Cartesian {
				x_range: (0.0, 1.0),
				y_range: (0.0, 1.0),
				x_axis_type: AxisType::Linear,
				y_axis_type: AxisType::Linear,
			}
		}

		fn plot(
			&self,
			backend: &mut dyn PlotBackend,
			_bounds: Rectangle,
			transform: &CoordinateTransformer,
			_cursor: Cursor,
			_settings: ResolvedPlotSettings,
		) {
			let from = transform.cartesian(0.0, 0.25);
			let to = transform.cartesian(1.0, 0.75);
			backend.stroke_path(
				&|builder| {
					builder.move_to(from);
					builder.line_to(to);
				},
				Stroke {
					style: Style::Solid(Color::from_rgb8(17, 34, 51)),
					width: 3.0,
					..Default::default()
				},
			);
		}

		fn legend_content(
			&self,
			_settings: &ResolvedPlotSettings,
		) -> Option<crate::plot::legend::LegendContent> {
			if !self.legend {
				return None;
			}
			let entries = crate::plot::spec::NonEmpty::new(crate::plot::legend::LegendEntry {
				label: "series".to_owned(),
				swatch: Color::from_rgb8(17, 34, 51),
				sample: crate::plot::legend::LegendSample::Swatch,
				provenance: crate::plot::legend::LegendProvenance::Named,
			});
			Some(crate::plot::legend::LegendContent::entries(entries))
		}

		fn hover(&self, _transform: &CoordinateTransformer, _cursor: Cursor) -> Option<String> {
			None
		}

		fn x_label(&self) -> String {
			"x axis".to_owned()
		}

		fn y_label(&self) -> String {
			"y axis".to_owned()
		}
	}

	fn oracle_svg_at(
		settings: ResolvedPlotSettings,
		legend: bool,
		width: f32,
		height: f32,
	) -> String {
		let settings_model = PlotSettings::from_resolved(settings.clone());
		let kernel = OracleKernel { legend };
		let widget = PlotWidget {
			kernel: &kernel,
			title: "fallback title".to_owned(),
			padding: 0.0,
			settings,
			settings_model,
			render_revision: 0,
			resize_render_suspended: false,
			layer: PlotRenderLayer::Full,
		};
		let mut backend = SvgBackend::new(width, height);
		widget.render(
			&mut backend,
			Rectangle::new(Point::ORIGIN, iced::Size::new(width, height)),
		);
		backend.finish()
	}

	fn oracle_svg_with_legend(settings: ResolvedPlotSettings, legend: bool) -> String {
		oracle_svg_at(settings, legend, 400.0, 300.0)
	}

	fn oracle_svg(settings: ResolvedPlotSettings) -> String {
		oracle_svg_with_legend(settings, true)
	}

	#[test]
	fn product_0031_banks_the_two_field_renderer_before_extraction() {
		let settings = ResolvedPlotSettings {
			title: ResolvedPresence::Show(Arc::new("authored title".to_owned())),
			subtitle: ResolvedPresence::Show(Arc::new("authored subtitle".to_owned())),
			x_tick_placement: TickPlacement::count(1).unwrap(),
			y_tick_placement: TickPlacement::count(1).unwrap(),
			show_x_minor_ticks: false,
			show_y_minor_ticks: false,
			show_x_minor_grid: false,
			show_y_minor_grid: false,
			..ResolvedPlotSettings::default()
		};
		let svg = oracle_svg(settings);
		assert!(svg.contains(">series</text>"));
		assert!(svg.contains("fill-opacity=\"0.60\""));
	}

	#[test]
	fn product_0050_canvas_presence_keeps_all_three_pixel_states_distinct() {
		let fallback = oracle_svg_with_legend(ResolvedPlotSettings::default(), false);
		assert!(fallback.contains(">fallback title</text>"));

		let mut shown = ResolvedPlotSettings::default();
		shown.title = ResolvedPresence::Show(Arc::new("shown title".to_owned()));
		let shown = oracle_svg_with_legend(shown, false);
		assert!(shown.contains(">shown title</text>"));
		assert!(!shown.contains(">fallback title</text>"));

		let mut hidden = ResolvedPlotSettings::default();
		hidden.title = ResolvedPresence::Hide;
		hidden.legend = ResolvedPresence::Hide;
		let hidden_with_content = oracle_svg_with_legend(hidden, true);
		let mut hidden_without_content = ResolvedPlotSettings::default();
		hidden_without_content.title = ResolvedPresence::Hide;
		let no_content = oracle_svg_with_legend(hidden_without_content, false);
		assert!(!hidden_with_content.contains(">fallback title</text>"));
		assert_eq!(hidden_with_content, no_content);
	}

	fn legend_background_origin(svg: &str) -> (f32, f32) {
		let path = svg
			.lines()
			.find(|line| line.contains("fill-opacity=\"0.60\""))
			.expect("legend background path");
		let coordinates = path
			.split_once("M ")
			.expect("path start")
			.1
			.split_whitespace()
			.take(2)
			.map(|value| value.parse::<f32>().expect("path coordinate"))
			.collect::<Vec<_>>();
		(coordinates[0], coordinates[1])
	}

	#[test]
	fn product_0051_default_legend_tracks_the_plot_top_across_headings_and_large_resizes() {
		let mut title_only = ResolvedPlotSettings::default();
		title_only.title = ResolvedPresence::Show(Arc::new("title".to_owned()));
		let mut title_and_subtitle = title_only.clone();
		title_and_subtitle.subtitle = ResolvedPresence::Show(Arc::new("subtitle".to_owned()));
		let mut neither = ResolvedPlotSettings::default();
		neither.title = ResolvedPresence::Hide;
		neither.subtitle = ResolvedPresence::Hide;

		for (width, height) in [(400.0, 300.0), (1200.0, 900.0)] {
			for settings in [
				title_only.clone(),
				title_and_subtitle.clone(),
				neither.clone(),
			] {
				let expected_y = settings.plot_padding_top;
				let svg = oracle_svg_at(settings, true, width, height);
				let (legend_x, legend_y) = legend_background_origin(&svg);
				assert_eq!(legend_x, width - 170.0);
				assert_eq!(legend_y, expected_y);
				assert_ne!(legend_y, (height - 45.0) * 0.14);
			}
		}
	}
}

#[cfg(test)]
mod product_0067_0068_strata_oracles {
	use super::*;
	use crate::plot::theme::PlotSettings;
	use iced::advanced::mouse::Cursor;
	use iced::widget::canvas::{Stroke, Text};

	const MARK: Color = Color::from_rgb(0.02, 0.78, 0.94);

	#[derive(Clone, Debug, PartialEq)]
	enum Operation {
		Fill(Color),
		Stroke(Color),
		Text,
	}

	#[derive(Default)]
	struct OperationBackend {
		operations: Vec<Operation>,
	}

	impl PlotBackend for OperationBackend {
		fn stroke_path(&mut self, _f: &dyn Fn(&mut dyn PathBuilder), stroke: Stroke) {
			let color = match stroke.style {
				Style::Solid(color) => color,
				Style::Gradient(_) => panic!("strata oracle expects solid plot strokes"),
			};
			self.operations.push(Operation::Stroke(color));
		}

		fn fill_path(&mut self, _f: &dyn Fn(&mut dyn PathBuilder), color: Color) {
			self.operations.push(Operation::Fill(color));
		}

		fn fill_rectangle(&mut self, _top_left: Point, _size: iced::Size, color: Color) {
			self.operations.push(Operation::Fill(color));
		}

		fn fill_text(&mut self, _text: Text) {
			self.operations.push(Operation::Text);
		}

		fn translate(&mut self, _translation: iced::Vector) {}

		fn rotate(&mut self, _angle: f32) {}

		fn with_save(&mut self, f: &mut dyn FnMut(&mut dyn PlotBackend)) {
			f(self);
		}

		fn with_clip(&mut self, _bounds: Rectangle, f: &mut dyn FnMut(&mut dyn PlotBackend)) {
			f(self);
		}
	}

	struct OrderKernel {
		layout: PlotLayout,
		mark: bool,
	}

	impl PlotKernel for OrderKernel {
		fn layout(&self, _settings: ResolvedPlotSettings) -> PlotLayout {
			self.layout.clone()
		}

		fn plot(
			&self,
			backend: &mut dyn PlotBackend,
			_bounds: Rectangle,
			_transform: &CoordinateTransformer,
			_cursor: Cursor,
			_settings: ResolvedPlotSettings,
		) {
			if self.mark {
				backend.fill_rectangle(Point::new(20.0, 20.0), iced::Size::new(40.0, 40.0), MARK);
			}
		}

		fn hover(&self, _transform: &CoordinateTransformer, _cursor: Cursor) -> Option<String> {
			None
		}
	}

	fn settings() -> ResolvedPlotSettings {
		ResolvedPlotSettings {
			title: ResolvedPresence::Hide,
			subtitle: ResolvedPresence::Hide,
			x_label: ResolvedPresence::Hide,
			y_label: ResolvedPresence::Hide,
			legend: ResolvedPresence::Hide,
			x_tick_placement: TickPlacement::count(1).unwrap(),
			y_tick_placement: TickPlacement::count(1).unwrap(),
			show_x_minor_ticks: false,
			show_y_minor_ticks: false,
			show_x_minor_grid: false,
			show_y_minor_grid: false,
			plot_padding_top: 0.0,
			plot_padding_bottom: 0.0,
			plot_padding_left: 0.0,
			plot_padding_right: 0.0,
			..ResolvedPlotSettings::default()
		}
	}

	fn render(layout: PlotLayout, settings: ResolvedPlotSettings, mark: bool) -> Vec<Operation> {
		let model = PlotSettings::from_resolved(settings.clone());
		let kernel = OrderKernel { layout, mark };
		let widget = PlotWidget {
			kernel: &kernel,
			title: String::new(),
			padding: 0.0,
			settings,
			settings_model: model,
			render_revision: 0,
			resize_render_suspended: false,
			layer: PlotRenderLayer::Full,
		};
		let mut backend = OperationBackend::default();
		widget.render(
			&mut backend,
			Rectangle::new(Point::ORIGIN, iced::Size::new(200.0, 120.0)),
		);
		backend.operations
	}

	fn render_without_plot_layer_grid_dispatch(
		layout: PlotLayout,
		settings: ResolvedPlotSettings,
	) -> Vec<Operation> {
		let model = PlotSettings::from_resolved(settings.clone());
		let kernel = OrderKernel { layout, mark: true };
		let widget = PlotWidget {
			kernel: &kernel,
			title: String::new(),
			padding: 0.0,
			settings: settings.clone(),
			settings_model: model.clone(),
			render_revision: 0,
			resize_render_suspended: false,
			layer: PlotRenderLayer::Full,
		};
		let bounds = Rectangle::new(Point::ORIGIN, iced::Size::new(200.0, 120.0));
		let plot_area = widget.plot_area(Rect::from_iced_frame(bounds)).into_iced();
		let layout = kernel.layout(settings.clone());
		let transform = CoordinateTransformer::new(&layout, plot_area);
		let mut backend = OperationBackend::default();
		backend.fill_rectangle(
			Point::ORIGIN,
			bounds.size(),
			settings.chrome.canvas_background.iced(),
		);
		backend.with_save(&mut |backend| {
			backend.translate(iced::Vector::new(plot_area.x, plot_area.y));
			kernel.plot_with_settings(backend, plot_area, &transform, Cursor::Unavailable, &model);
		});
		widget.render_overlay_layer(&mut backend, bounds);
		backend.operations
	}

	fn layouts() -> [PlotLayout; 3] {
		[
			PlotLayout::Cartesian {
				x_range: (0.0, 1.0),
				y_range: (0.0, 1.0),
				x_axis_type: AxisType::Linear,
				y_axis_type: AxisType::Linear,
			},
			PlotLayout::CategoricalX {
				categories: vec!["A".to_owned(), "B".to_owned()],
				y_range: (0.0, 1.0),
			},
			PlotLayout::CategoricalY {
				categories: vec!["A".to_owned(), "B".to_owned()],
				x_range: (0.0, 1.0),
			},
		]
	}

	#[test]
	fn product_0068_guides_precede_marks_and_furniture_follows_for_every_gridded_layout() {
		let settings = settings();
		let grid = settings.chrome.gridline.iced();
		let axis = settings.chrome.axis_line.iced();
		for layout in layouts() {
			let operations = render(layout, settings.clone(), true);
			let mark = operations
				.iter()
				.position(|operation| *operation == Operation::Fill(MARK))
				.expect("real mark operation");
			let grids = operations
				.iter()
				.enumerate()
				.filter_map(|(index, operation)| {
					(*operation == Operation::Stroke(grid)).then_some(index)
				})
				.collect::<Vec<_>>();
			let axes = operations
				.iter()
				.enumerate()
				.filter_map(|(index, operation)| {
					(*operation == Operation::Stroke(axis)).then_some(index)
				})
				.collect::<Vec<_>>();
			assert!(!grids.is_empty(), "fixture must draw real gridlines");
			assert!(!axes.is_empty(), "fixture must draw real axis furniture");
			assert!(
				grids.iter().all(|index| *index < mark),
				"PRODUCT-0068: guides must precede marks so data colours survive grid intersections: {operations:?}"
			);
			assert!(
				axes.iter().all(|index| *index > mark),
				"PRODUCT-0068: axes and tick furniture must remain above marks: {operations:?}"
			);
		}
	}

	#[test]
	fn product_0067_marks_free_render_changes_only_the_default_grid_alpha() {
		let current = settings();
		let mut retired = current.clone();
		retired.chrome.gridline = crate::plot::chrome::PlotRgba::translucent(1.0, 1.0, 1.0, 0.2);
		let current = render(layouts()[0].clone(), current, false);
		let retired = render(layouts()[0].clone(), retired, false);
		assert_eq!(current.len(), retired.len());
		let mut differences = 0;
		for (current, retired) in current.iter().zip(&retired) {
			if current == retired {
				continue;
			}
			differences += 1;
			let (Operation::Stroke(current), Operation::Stroke(retired)) = (current, retired)
			else {
				panic!("PRODUCT-0067 may change only grid strokes: {current:?} != {retired:?}");
			};
			assert_eq!(
				(current.r, current.g, current.b),
				(retired.r, retired.g, retired.b)
			);
			assert_eq!(current.a, 0.35);
			assert_eq!(retired.a, 0.2);
		}
		assert!(differences > 0, "marks-free oracle must contain gridlines");
	}

	#[test]
	fn product_0068_disabling_every_grid_is_byte_identical_to_the_retired_strata() {
		let mut settings = settings();
		settings.show_x_major_grid = false;
		settings.show_y_major_grid = false;
		settings.show_x_minor_grid = false;
		settings.show_y_minor_grid = false;
		for layout in layouts() {
			let current = render(layout.clone(), settings.clone(), true);
			let retired = render_without_plot_layer_grid_dispatch(layout, settings.clone());
			assert_eq!(
				current, retired,
				"PRODUCT-0068: with every guide disabled, moving the empty grid dispatch cannot alter any non-stacked-area draw operation"
			);
		}
	}
}

#[cfg(test)]
mod product_0037_count_position_oracle {
	use super::{AxisType, MAX_GENERATED_TICK_POSITIONS, TickPlacement, TickPlacementError};

	fn legacy_major_positions(range: (f64, f64), intervals: u32) -> Vec<f64> {
		(0..=intervals)
			.map(|i| range.0 + (range.1 - range.0) * (i as f64 / intervals as f64))
			.collect()
	}

	fn legacy_minor_positions(range: (f64, f64), intervals: u32, minor: u32) -> Vec<f64> {
		(0..intervals)
			.flat_map(|i| {
				(1..=minor).filter_map(move |j| {
					let t = (i as f64 + j as f64 / (minor + 1) as f64) / intervals as f64;
					(t <= 1.0).then(|| range.0 + (range.1 - range.0) * t)
				})
			})
			.collect()
	}

	#[test]
	fn product_0037_banks_count_eight_major_and_minor_positions_before_refactor() {
		let major = legacy_major_positions((0.0, 7.0), 8);
		assert_eq!(
			major,
			vec![0.0, 0.875, 1.75, 2.625, 3.5, 4.375, 5.25, 6.125, 7.0]
		);

		let non_zero = legacy_major_positions((1.4, 8.6), 8);
		assert_eq!(
			non_zero,
			vec![
				1.4,
				2.3,
				3.1999999999999997,
				4.1,
				5.0,
				5.9,
				6.799999999999999,
				7.699999999999999,
				8.6
			]
		);

		let minor = legacy_minor_positions((0.0, 7.0), 8, 4);
		assert_eq!(minor.len(), 32);
		assert_eq!(
			&minor[..5],
			&[
				0.17500000000000002,
				0.35000000000000003,
				0.525,
				0.7000000000000001,
				1.05
			]
		);
		assert_eq!(
			&minor[28..],
			&[6.3, 6.4750000000000005, 6.6499999999999995, 6.825]
		);
	}

	#[test]
	fn product_0037_count_generator_matches_the_banked_oracle() {
		let placement = TickPlacement::count(8).unwrap();
		assert_eq!(
			placement.positions((0.0, 7.0)).unwrap(),
			vec![0.0, 0.875, 1.75, 2.625, 3.5, 4.375, 5.25, 6.125, 7.0]
		);
		assert_eq!(
			placement.positions((1.4, 8.6)).unwrap(),
			vec![
				1.4,
				2.3,
				3.1999999999999997,
				4.1,
				5.0,
				5.9,
				6.799999999999999,
				7.699999999999999,
				8.6
			]
		);
		assert_eq!(
			placement
				.minor_positions(&placement.positions((0.0, 7.0)).unwrap(), 4)
				.unwrap(),
			legacy_minor_positions((0.0, 7.0), 8, 4)
		);
	}

	#[test]
	fn product_0037_step_is_zero_anchored_and_bounded() {
		let placement = TickPlacement::step(1.0).unwrap();
		assert_eq!(
			placement.positions((0.0, 7.0)).unwrap(),
			(0..=7).map(f64::from).collect::<Vec<_>>()
		);
		assert_eq!(
			placement.positions((1.4, 8.6)).unwrap(),
			(2..=8).map(f64::from).collect::<Vec<_>>()
		);
		assert_eq!(
			placement.positions((0.2, 7.2)).unwrap(),
			(1..=7).map(f64::from).collect::<Vec<_>>()
		);
		assert_eq!(
			placement.positions((-3.5, 2.5)).unwrap(),
			(-3..=2).map(f64::from).collect::<Vec<_>>()
		);
		assert_eq!(
			placement.positions((0.0, 10.0)).unwrap(),
			vec![0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]
		);
		assert_eq!(
			placement
				.minor_positions(&placement.positions((0.0, 7.0)).unwrap(), 1)
				.unwrap(),
			vec![0.5, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5]
		);
	}

	#[test]
	fn product_0037_step_rejects_invalid_values_and_capacity_without_truncation() {
		assert!(TickPlacement::step(0.0).is_none());
		assert!(TickPlacement::step(-1.0).is_none());
		assert!(TickPlacement::step(f64::NAN).is_none());
		assert!(TickPlacement::step(f64::INFINITY).is_none());
		let dense = TickPlacement::step(1e-9).unwrap();
		assert!(matches!(
			dense.positions((0.0, 7.0)),
			Err(TickPlacementError::Capacity { limit, .. }) if limit == MAX_GENERATED_TICK_POSITIONS
		));
		assert!(matches!(
			dense.positions_for_axis(AxisType::Date, (0.0, 7.0)),
			Err(TickPlacementError::UnsupportedAxis {
				axis_type: AxisType::Date
			})
		));
		assert!(matches!(
			dense.positions_for_axis(AxisType::Time, (0.0, 7.0)),
			Err(TickPlacementError::UnsupportedAxis {
				axis_type: AxisType::Time
			})
		));
	}

	#[test]
	fn product_0039_minor_subdivision_is_counted_and_capacity_checked() {
		for placement in [
			TickPlacement::count(4).unwrap(),
			TickPlacement::step(1.0).unwrap(),
		] {
			let major = placement.positions((0.0, 4.0)).unwrap();
			let minor = placement.minor_positions(&major, 1).unwrap();
			assert_eq!(minor, vec![0.5, 1.5, 2.5, 3.5]);
			assert_eq!(minor.len(), major.len() - 1);
			assert!(minor.iter().all(|position| !major.contains(position)));
			assert_eq!(
				placement.minor_positions(&major, 0).unwrap(),
				Vec::<f64>::new()
			);
		}

		let count = TickPlacement::count(8).unwrap();
		let major = count.positions((0.0, 7.0)).unwrap();
		assert!(matches!(
			count.minor_positions(&major, 1_249),
			Err(TickPlacementError::MinorCapacity {
				minor_ticks: 1_249,
				requested: 10_001,
				limit: MAX_GENERATED_TICK_POSITIONS,
				..
			})
		));
		assert!(matches!(
			count.minor_positions(&major, u32::MAX),
			Err(TickPlacementError::MinorCapacity {
				minor_ticks: u32::MAX,
				requested,
				limit: MAX_GENERATED_TICK_POSITIONS,
				..
			}) if requested > MAX_GENERATED_TICK_POSITIONS
		));
	}
}

#[cfg(test)]
mod product_0056_categorical_y_oracles {
	use super::*;
	use crate::plot::theme::PlotSettings;
	use iced::Size;

	struct FrameKernel;

	impl PlotKernel for FrameKernel {
		fn layout(&self, _settings: ResolvedPlotSettings) -> PlotLayout {
			PlotLayout::CategoricalY {
				categories: vec!["only".to_owned()],
				x_range: (0.0, 10.0),
			}
		}

		fn plot(
			&self,
			_backend: &mut dyn PlotBackend,
			_bounds: Rectangle,
			_transform: &CoordinateTransformer,
			_cursor: Cursor,
			_settings: ResolvedPlotSettings,
		) {
		}

		fn hover(&self, _transform: &CoordinateTransformer, _cursor: Cursor) -> Option<String> {
			None
		}
	}

	#[derive(Default)]
	/// PRODUCT-0057 named oracle `plot::common::tests::RecordingBackend` records
	/// stroke-point traces and text after accumulated translation. It discards
	/// fills, rectangles, rotation, and clip bounds; `with_clip` is flattened to
	/// `with_save`, so it is not licensed for any clip-scope claim.
	struct RecordingBackend {
		strokes: Vec<Vec<Point>>,
		texts: Vec<Text>,
		translation: iced::Vector,
	}

	struct RecordingPathBuilder {
		points: Vec<Point>,
		translation: iced::Vector,
	}

	impl RecordingPathBuilder {
		fn translated(&self, point: Point) -> Point {
			Point::new(point.x + self.translation.x, point.y + self.translation.y)
		}
	}

	impl PathBuilder for RecordingPathBuilder {
		fn move_to(&mut self, point: Point) {
			self.points.push(self.translated(point));
		}

		fn line_to(&mut self, point: Point) {
			self.points.push(self.translated(point));
		}

		fn arc_to(&mut self, center: Point, _radius: f32, _start: f32, _end: f32) {
			self.points.push(self.translated(center));
		}

		fn circle(&mut self, center: Point, _radius: f32) {
			self.points.push(self.translated(center));
		}

		fn rectangle(&mut self, top_left: Point, size: Size) {
			self.move_to(top_left);
			self.line_to(Point::new(top_left.x + size.width, top_left.y));
			self.line_to(Point::new(
				top_left.x + size.width,
				top_left.y + size.height,
			));
			self.line_to(Point::new(top_left.x, top_left.y + size.height));
		}

		fn close(&mut self) {}
	}

	impl PlotBackend for RecordingBackend {
		fn stroke_path(&mut self, f: &dyn Fn(&mut dyn PathBuilder), _stroke: Stroke) {
			let mut builder = RecordingPathBuilder {
				points: Vec::new(),
				translation: self.translation,
			};
			f(&mut builder);
			self.strokes.push(builder.points);
		}

		fn fill_path(&mut self, _f: &dyn Fn(&mut dyn PathBuilder), _color: Color) {}

		fn fill_rectangle(&mut self, _top_left: Point, _size: Size, _color: Color) {}

		fn fill_text(&mut self, mut text: Text) {
			text.position.x += self.translation.x;
			text.position.y += self.translation.y;
			self.texts.push(text);
		}

		fn translate(&mut self, translation: iced::Vector) {
			self.translation.x += translation.x;
			self.translation.y += translation.y;
		}

		fn rotate(&mut self, _angle: f32) {}

		fn with_save(&mut self, f: &mut dyn FnMut(&mut dyn PlotBackend)) {
			let saved = self.translation;
			f(self);
			self.translation = saved;
		}

		fn with_clip(&mut self, _bounds: Rectangle, f: &mut dyn FnMut(&mut dyn PlotBackend)) {
			self.with_save(f);
		}
	}

	fn widget<'a>(kernel: &'a FrameKernel) -> PlotWidget<'a> {
		let model = PlotSettings::default();
		PlotWidget {
			kernel,
			title: String::new(),
			padding: 0.0,
			settings: model.resolve(),
			settings_model: model,
			render_revision: 0,
			resize_render_suspended: false,
			layer: PlotRenderLayer::Full,
		}
	}

	fn assert_frame_path(path: &[Point], width: f32, height: f32) {
		assert_eq!(
			path,
			[
				Point::new(0.0, 0.0),
				Point::new(0.0, height),
				Point::new(width, height),
			]
		);
	}

	#[test]
	fn product_0056_transformers_map_every_categorical_y_index_top_down() {
		for (width, height) in [(120.0, 100.0), (360.0, 500.0)] {
			for count in [1, 2, 5, 6] {
				let categories = (0..count).map(|i| format!("c{i}")).collect::<Vec<_>>();
				let categorical_y = PlotLayout::CategoricalY {
					categories: categories.clone(),
					x_range: (0.0, 10.0),
				};
				let categorical_xy = PlotLayout::CategoricalXY {
					x_categories: vec!["x".to_owned()],
					y_categories: categories,
				};
				let bounds = Rectangle::new(Point::ORIGIN, Size::new(width, height));
				let y_transform = CoordinateTransformer::new(&categorical_y, bounds);
				let xy_transform = CoordinateTransformer::new(&categorical_xy, bounds);
				let band = height / count as f32;
				for index in 0..count {
					let expected = index as f32 * band + band / 2.0;
					assert_eq!(y_transform.categorical(index, 0.0).0.y, expected);
					assert_eq!(xy_transform.categorical_2d(0, index).0.y, expected);
					if matches!(count, 2 | 6) {
						let old = height - index as f32 * band - band / 2.0;
						assert_ne!(expected, old, "even counts have no reversal fixed point");
					}
				}
			}
		}
	}

	#[test]
	fn product_0056_categorical_frames_and_x_axes_ignore_domain_order_and_count() {
		let kernel = FrameKernel;
		let widget = widget(&kernel);
		for (width, height) in [(120.0, 100.0), (360.0, 500.0)] {
			for count in [1, 2, 5] {
				let mut categories = (0..count).map(|i| format!("c{i}")).collect::<Vec<_>>();
				for reversed in [false, true] {
					if reversed {
						categories.reverse();
					}
					let bounds = Rectangle::new(Point::ORIGIN, Size::new(width, height));
					let y_layout = PlotLayout::CategoricalY {
						categories: categories.clone(),
						x_range: (0.0, 10.0),
					};
					let y_transform = CoordinateTransformer::new(&y_layout, bounds);

					let mut grid = RecordingBackend::default();
					widget.draw_categorical_y_grid(
						&mut grid,
						bounds,
						&y_transform,
						&categories,
						(0.0, 10.0),
					);
					assert!(!grid.strokes.is_empty());
					assert!(grid.strokes.iter().all(|path| {
						path.len() == 2 && path[0].y == 0.0 && path[1].y == height
					}));

					let mut y_axes = RecordingBackend::default();
					widget.draw_categorical_y_axes(
						&mut y_axes,
						bounds,
						&y_transform,
						&categories,
						(0.0, 10.0),
						AxisTextVisibility::ALL,
					);
					assert_frame_path(&y_axes.strokes[0], width, height);
					for category in &categories {
						let text = y_axes
							.texts
							.iter()
							.find(|text| text.content == *category)
							.expect("every categorical-y label is rendered");
						let index = categories.iter().position(|item| item == category).unwrap();
						assert_eq!(text.position.y, y_transform.categorical(index, 0.0).0.y);
					}

					let xy_layout = PlotLayout::CategoricalXY {
						x_categories: vec!["x0".to_owned(), "x1".to_owned()],
						y_categories: categories.clone(),
					};
					let xy_transform = CoordinateTransformer::new(&xy_layout, bounds);
					let mut xy_axes = RecordingBackend::default();
					widget.draw_categorical_xy_axes(
						&mut xy_axes,
						bounds,
						&xy_transform,
						&["x0".to_owned(), "x1".to_owned()],
						&categories,
						AxisTextVisibility::ALL,
					);
					assert_frame_path(&xy_axes.strokes[0], width, height);
					for category in &categories {
						let text = xy_axes
							.texts
							.iter()
							.find(|text| text.content == *category)
							.expect("every heatmap row label is rendered");
						let index = categories.iter().position(|item| item == category).unwrap();
						assert_eq!(text.position.y, xy_transform.categorical_2d(0, index).0.y);
					}
				}
			}
		}
	}
}
