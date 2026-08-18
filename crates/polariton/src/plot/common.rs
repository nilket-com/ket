use crate::coordinate::{Plot, Pt, Rect, Widget, Window};
use crate::fonts::PLOT_TEXT_FONT;
use crate::gui::messages::PlotMessage;
use crate::plot::chrome::{PlotChrome, PlotMarkStyle};
use crate::plot::colors::ColorTheme;
use crate::plot::continuous_color::ContinuousRamps;
use crate::plot::legend::{LegendContent, legend_layout, render_legend};
use crate::plot::spec::{
	BarWidth, CategoryGap, FacetLabelTextSize, HoverTextSize, LegendPosition, LegendTextSize,
	PlotPaddingPixels, PointSize, ResolvedPresence, StrokeWidth, SubtitleTextSize, TickRotation,
	TitleTextSize, XLabelTextSize, XTickTextSize, YLabelTextSize, YTickTextSize,
};
use crate::plot::text_estimate::{estimate_ascii_label_width, estimate_formatted_label_width};
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
	/// PRODUCT-0072: a linear value scale expressed in percentage points.
	Percent,
	Date,
	Datetime(TimeUnit),
	Time,
}

/// Semantic provenance for a derived axis title. This is deliberately
/// separate from [`AxisType`], which controls tick formatting rather than the
/// transformation that produced the values.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AxisTitleTransform {
	PercentOfTotal,
}

pub fn derive_axis_title(base: &str, transform: AxisTitleTransform) -> String {
	if base.is_empty() {
		return String::new();
	}
	match transform {
		AxisTitleTransform::PercentOfTotal => format!("{base} (% of total)"),
	}
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

	// Retained renderer substrate and direct-test constructor after PRODUCT-0075
	// removes the dashboard control that authored count-based placement.
	#[allow(dead_code)]
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
		if matches!(self, Self::Step(_))
			&& !matches!(axis_type, AxisType::Linear | AxisType::Percent)
		{
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
		y_axis_type: AxisType,
	},
	CategoricalY {
		categories: Vec<String>,
		x_range: (f64, f64),
		x_axis_type: AxisType,
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

/// Layout-derived inputs for the bottom and left plot bands.
///
/// PRODUCT-0114 derives these before plot geometry, from the label producer
/// and its complete domain rather than from a realized tick set. The latter
/// would form a cycle: tick placement depends on the plot extent that the band
/// itself determines. PRODUCT-0127 carries each formatted axis's placement
/// alongside its domain, so the shared band authority never pairs a domain
/// with a policy from another layout.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct BandRequirements {
	pub x: Vec<AxisBandRequirement>,
	pub y: Vec<AxisBandRequirement>,
}

impl BandRequirements {
	pub(crate) const EMPTY: Self = Self {
		x: Vec::new(),
		y: Vec::new(),
	};

	pub(crate) fn from_layout(layout: &PlotLayout, settings: &ResolvedPlotSettings) -> Self {
		let (x, y) = match layout {
			PlotLayout::Cartesian {
				x_range,
				y_range,
				x_axis_type,
				y_axis_type,
			} => (
				Some(AxisBandRequirement::Formatted {
					domain: *x_range,
					axis_type: *x_axis_type,
					placement: settings.x_tick_placement,
				}),
				Some(AxisBandRequirement::Formatted {
					domain: *y_range,
					axis_type: *y_axis_type,
					placement: settings.y_tick_placement,
				}),
			),
			PlotLayout::CategoricalX {
				categories,
				y_range,
				y_axis_type,
			} => (
				Some(AxisBandRequirement::Categories(categories.clone())),
				Some(AxisBandRequirement::Formatted {
					domain: *y_range,
					axis_type: *y_axis_type,
					placement: settings.y_tick_placement,
				}),
			),
			PlotLayout::CategoricalY {
				categories,
				x_range,
				x_axis_type,
			} => (
				Some(AxisBandRequirement::Formatted {
					domain: *x_range,
					axis_type: *x_axis_type,
					placement: settings.x_tick_placement,
				}),
				Some(AxisBandRequirement::Categories(categories.clone())),
			),
			PlotLayout::CategoricalXY {
				x_categories,
				y_categories,
			} => (
				Some(AxisBandRequirement::Categories(x_categories.clone())),
				Some(AxisBandRequirement::Categories(y_categories.clone())),
			),
			PlotLayout::Parallel { .. } | PlotLayout::Radial => (None, None),
		};
		Self {
			x: x.into_iter().collect(),
			y: y.into_iter().collect(),
		}
	}

	pub(crate) fn across_layouts<'a>(
		layouts: impl IntoIterator<Item = &'a PlotLayout>,
		settings: &ResolvedPlotSettings,
	) -> Self {
		let mut requirements = Self::EMPTY;
		for layout in layouts {
			let local = Self::from_layout(layout, settings);
			requirements.x.extend(local.x);
			requirements.y.extend(local.y);
		}
		requirements
	}

	fn estimated_x_width(&self, font_size: f32) -> f32 {
		self.x
			.iter()
			.map(|requirement| requirement.estimated_width(font_size))
			.fold(0.0, f32::max)
	}

	fn estimated_y_width(&self, font_size: f32) -> f32 {
		self.y
			.iter()
			.map(|requirement| requirement.estimated_width(font_size))
			.fold(0.0, f32::max)
	}
}

/// The producer-side inputs from which PRODUCT-0114 later derives a longest
/// possible tick label. Requirements retain complete category domains and
/// formatted-axis domains so the eventual estimator is the sole authority that
/// decides which candidate is widest.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum AxisBandRequirement {
	Formatted {
		domain: (f64, f64),
		axis_type: AxisType,
		placement: TickPlacement,
	},
	Categories(Vec<String>),
}

impl AxisBandRequirement {
	fn estimated_width(&self, font_size: f32) -> f32 {
		match self {
			Self::Formatted {
				domain,
				axis_type,
				placement,
			} => placement
				.positions(*domain)
				.unwrap_or_default()
				.into_iter()
				.map(|value| {
					estimate_formatted_label_width(&format_label(value, *axis_type), font_size)
				})
				.fold(0.0, f32::max),
			Self::Categories(labels) => labels
				.iter()
				.map(|label| estimate_ascii_label_width(label, font_size))
				.fold(0.0, f32::max),
		}
	}
}

pub struct CoordinateTransformer<'a> {
	pub layout: &'a PlotLayout,
	pub bounds: Rectangle,
}

impl<'a> CoordinateTransformer<'a> {
	pub fn new(layout: &'a PlotLayout, bounds: Rectangle) -> Self {
		Self { layout, bounds }
	}

	/// Pair the transformer's bounds with a cursor in their shared widget space.
	///
	/// `PlotKernel::hover` remains an untyped iced boundary. PRODUCT-0097 makes
	/// the space expected after that boundary explicit without changing the
	/// transformer field or every kernel signature.
	pub(crate) fn widget_hit_geometry(&self, cursor: Pt<Widget>) -> (Rect<Widget>, Pt<Widget>) {
		(Rect::<Widget>::from_transformer_bounds(self.bounds), cursor)
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
				..
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
				..
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

// Renderer settings remain substrate after PRODUCT-0075 removes their manual
// dashboard editor. Direct kernels and future authored surfaces still consume
// the vocabulary.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GridLineStyle {
	Solid,
	Dashed,
	Dotted,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ScatterRenderMode {
	#[default]
	Auto,
	Vector,
	Downsampled,
	Rasterized,
}

impl ScatterRenderMode {
	#[allow(dead_code)]
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
	#[allow(dead_code)]
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
	pub x_tick_rotation: TickRotation,
	pub x_label_offset: f32,
	pub color_theme: ColorTheme,
	pub continuous_ramps: ContinuousRamps,
	pub chrome: PlotChrome,
	pub mark_style: PlotMarkStyle,
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
	/// PRODUCT-0120 tracks only explicit edge modifiers separately so derived
	/// guides can floor them without changing legacy atomic-padding geometry.
	pub authored_plot_padding_top: Option<PlotPaddingPixels>,
	pub authored_plot_padding_bottom: Option<PlotPaddingPixels>,
	pub authored_plot_padding_left: Option<PlotPaddingPixels>,
	pub authored_plot_padding_right: Option<PlotPaddingPixels>,
	pub title_size: TitleTextSize,
	pub subtitle_size: SubtitleTextSize,
	pub x_label_size: XLabelTextSize,
	pub y_label_size: YLabelTextSize,
	pub x_tick_size: XTickTextSize,
	pub y_tick_size: YTickTextSize,
	pub facet_label_size: FacetLabelTextSize,
	pub legend_size: LegendTextSize,
	pub hover_size: HoverTextSize,
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

impl ResolvedPlotSettings {
	/// PRODUCT-0120: zero is an explicit, round-trippable request but remains a
	/// geometry no-op, preserving the legacy edge when an author asks for none.
	pub(crate) fn effective_plot_padding_top(&self) -> f32 {
		self.authored_plot_padding_top
			.map(PlotPaddingPixels::pixels)
			.filter(|pixels| *pixels > 0.0)
			.unwrap_or(self.plot_padding_top)
	}

	pub(crate) fn effective_plot_padding_right(&self) -> f32 {
		self.authored_plot_padding_right
			.map(PlotPaddingPixels::pixels)
			.filter(|pixels| *pixels > 0.0)
			.unwrap_or(self.plot_padding_right)
	}

	pub(crate) fn explicit_plot_padding_right(&self) -> Option<f32> {
		self.authored_plot_padding_right
			.map(PlotPaddingPixels::pixels)
			.filter(|pixels| *pixels > 0.0)
	}
}

impl Default for ResolvedPlotSettings {
	fn default() -> Self {
		Self {
			max_legend_rows: 10,
			legend_x: 0.95,
			legend_y: 0.05,
			legend: ResolvedPresence::Fallback,
			x_tick_rotation: TickRotation::ZERO,
			x_label_offset: 10.0,
			color_theme: ColorTheme::default(),
			continuous_ramps: ContinuousRamps::default(),
			chrome: PlotChrome::DEFAULT,
			mark_style: PlotMarkStyle::DEFAULT,
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
			authored_plot_padding_top: None,
			authored_plot_padding_bottom: None,
			authored_plot_padding_left: None,
			authored_plot_padding_right: None,
			title_size: TitleTextSize::DEFAULT,
			subtitle_size: SubtitleTextSize::DEFAULT,
			x_label_size: XLabelTextSize::DEFAULT,
			y_label_size: YLabelTextSize::DEFAULT,
			x_tick_size: XTickTextSize::DEFAULT,
			y_tick_size: YTickTextSize::DEFAULT,
			facet_label_size: FacetLabelTextSize::DEFAULT,
			legend_size: LegendTextSize::DEFAULT,
			hover_size: HoverTextSize::DEFAULT,
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

/// Maximum geometric error used when a circular arc is realized as kurbo
/// cubic Bézier segments for iced's path builder.
pub(crate) const PATH_ARC_TOLERANCE: f64 = 0.1;

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct CircularArc {
	pub center: Point,
	pub radius: f32,
	pub start_angle: f32,
	pub end_angle: f32,
}

impl CircularArc {
	pub fn new(center: Point, radius: f32, start_angle: f32, end_angle: f32) -> Self {
		Self {
			center,
			radius,
			start_angle,
			end_angle,
		}
	}

	pub fn start(self) -> Point {
		Point::new(
			self.center.x + self.radius * self.start_angle.cos(),
			self.center.y + self.radius * self.start_angle.sin(),
		)
	}

	pub fn end(self) -> Point {
		Point::new(
			self.center.x + self.radius * self.end_angle.cos(),
			self.center.y + self.radius * self.end_angle.sin(),
		)
	}

	fn as_kurbo(self) -> kurbo::Arc {
		kurbo::Arc::new(
			(self.center.x as f64, self.center.y as f64),
			(self.radius as f64, self.radius as f64),
			self.start_angle as f64,
			(self.end_angle - self.start_angle) as f64,
			0.0,
		)
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ArcJoin {
	Move,
	Line,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum SemanticPathVerb {
	Move { to: Point },
	Line { from: Point, to: Point },
	Arc(CircularArc),
	Close { from: Point, to: Point },
}

#[derive(Default)]
pub(crate) struct ArcPathState {
	current: Option<Point>,
	subpath_start: Option<Point>,
	#[cfg(test)]
	trace: Option<Vec<SemanticPathVerb>>,
}

impl ArcPathState {
	#[cfg(test)]
	pub(crate) fn tracing() -> Self {
		Self {
			trace: Some(Vec::new()),
			..Self::default()
		}
	}

	#[cfg(test)]
	pub(crate) fn finish_trace(self) -> Vec<SemanticPathVerb> {
		self.trace.expect("PRODUCT-0101 tracing state")
	}

	pub(crate) fn move_to(&mut self, point: Point) {
		self.current = Some(point);
		self.subpath_start = Some(point);
		#[cfg(test)]
		if let Some(trace) = &mut self.trace {
			trace.push(SemanticPathVerb::Move { to: point });
		}
	}

	pub(crate) fn line_to(&mut self, point: Point) {
		let Some(_from) = self.current else {
			self.move_to(point);
			return;
		};
		self.current = Some(point);
		#[cfg(test)]
		if let Some(trace) = &mut self.trace {
			trace.push(SemanticPathVerb::Line {
				from: _from,
				to: point,
			});
		}
	}

	pub(crate) fn begin_arc(&mut self, arc: CircularArc) -> ArcJoin {
		let start = arc.start();
		let join = if self.current.is_some() {
			self.line_to(start);
			ArcJoin::Line
		} else {
			self.move_to(start);
			ArcJoin::Move
		};
		self.current = Some(arc.end());
		#[cfg(test)]
		if let Some(trace) = &mut self.trace {
			trace.push(SemanticPathVerb::Arc(arc));
		}
		join
	}

	pub(crate) fn close(&mut self) {
		#[cfg(test)]
		if let (Some(trace), Some(from), Some(to)) =
			(&mut self.trace, self.current, self.subpath_start)
		{
			trace.push(SemanticPathVerb::Close { from, to });
		}
		self.current = None;
		self.subpath_start = None;
	}

	pub(crate) fn finish_closed_shape(&mut self) {
		self.current = None;
		self.subpath_start = None;
	}
}

pub trait PathBuilder {
	fn move_to(&mut self, point: Point);
	fn line_to(&mut self, point: Point);
	/// Appends a circular arc from `start_angle` to `end_angle`. With no open sub-path, moves to the
	/// arc start; otherwise connects the current point to the arc start with a
	/// line. The current point becomes the arc end.
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
	/// Emits plot text through the one declared face PRODUCT-0117 binds to the
	/// static ASCII budget. Backends receive the already-resolved operation so
	/// operation traces and production pixels share the same font authority.
	fn fill_text(&mut self, mut text: Text) {
		text.font = PLOT_TEXT_FONT;
		self.fill_text_raw(text);
	}
	fn fill_text_raw(&mut self, text: Text);
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
	/// Draw inside a widget-coordinate clip. Like iced's production canvas,
	/// the closure starts with an identity transform; callers that draw in a
	/// local plot coordinate system must establish that origin inside it.
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
	state: ArcPathState,
}

impl<'a> PathBuilder for IcedPathBuilder<'a> {
	fn move_to(&mut self, point: Point) {
		self.state.move_to(point);
		self.builder.move_to(point);
	}
	fn line_to(&mut self, point: Point) {
		self.state.line_to(point);
		self.builder.line_to(point);
	}
	fn arc_to(&mut self, center: Point, radius: f32, start: f32, end: f32) {
		let arc = CircularArc::new(center, radius, start, end);
		match self.state.begin_arc(arc) {
			ArcJoin::Move => self.builder.move_to(arc.start()),
			ArcJoin::Line => self.builder.line_to(arc.start()),
		}
		for element in arc.as_kurbo().append_iter(PATH_ARC_TOLERANCE) {
			if let kurbo::PathEl::CurveTo(one, two, end) = element {
				self.builder.bezier_curve_to(
					Point::new(one.x as f32, one.y as f32),
					Point::new(two.x as f32, two.y as f32),
					Point::new(end.x as f32, end.y as f32),
				);
			}
		}
	}
	fn circle(&mut self, center: Point, radius: f32) {
		self.builder.circle(center, radius);
		self.state.finish_closed_shape();
	}
	fn rectangle(&mut self, top_left: Point, size: iced::Size) {
		self.builder.rectangle(top_left, size);
		self.state.finish_closed_shape();
	}
	fn close(&mut self) {
		self.state.close();
		self.builder.close();
	}
}

#[cfg(test)]
pub(crate) fn product_0101_iced_semantic_trace(
	f: &dyn Fn(&mut dyn PathBuilder),
) -> Vec<SemanticPathVerb> {
	let mut trace = None;
	let _ = Path::new(|builder| {
		let mut path = IcedPathBuilder {
			builder,
			state: ArcPathState::tracing(),
		};
		f(&mut path);
		trace = Some(path.state.finish_trace());
	});
	trace.expect("PRODUCT-0101 iced adapter emitted a semantic trace")
}

impl<'a> PlotBackend for IcedBackend<'a> {
	fn stroke_path(&mut self, f: &dyn Fn(&mut dyn PathBuilder), stroke: Stroke) {
		let path = Path::new(|builder| {
			let mut ipb = IcedPathBuilder {
				builder,
				state: ArcPathState::default(),
			};
			f(&mut ipb);
		});
		self.frame.stroke(&path, stroke);
	}
	fn fill_path(&mut self, f: &dyn Fn(&mut dyn PathBuilder), color: Color) {
		let path = Path::new(|builder| {
			let mut ipb = IcedPathBuilder {
				builder,
				state: ArcPathState::default(),
			};
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
	fn fill_text_raw(&mut self, text: Text) {
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
		title: &str,
		cursor: Pt<Widget>,
		settings: &PlotSettings,
	) -> Option<String>;
}

pub trait PlotKernel: Send + Sync {
	fn layout(&self, settings: ResolvedPlotSettings) -> PlotLayout;

	/// Whether the common renderer must place the grid and marks together in a
	/// plot-area clip. This is required when emitted geometry deliberately
	/// extends beyond authored bounds and must be clipped without introducing a
	/// nested iced child beneath the plot background.
	fn requires_plot_area_clip(&self) -> bool {
		false
	}

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
pub(crate) struct AxisTextAnchoring {
	pub x_min: bool,
	pub x_max: bool,
	pub y_min: bool,
	pub y_max: bool,
	/// Outer figure edges are distinct from PRODUCT-0077's occupied shared
	/// edges. PRODUCT-0115 alone uses these for rotated x-tick containment.
	pub figure_x_min: bool,
	pub figure_x_max: bool,
}

impl AxisTextAnchoring {
	pub const NONE: Self = Self {
		x_min: false,
		x_max: false,
		y_min: false,
		y_max: false,
		figure_x_min: true,
		figure_x_max: true,
	};

	fn bound_coincident(value: f64, bound: f64, range: (f64, f64)) -> bool {
		let tolerance = (range.1 - range.0).abs().max(1.0) * f64::EPSILON * 16.0;
		(value - bound).abs() <= tolerance
	}

	pub fn x_alignment(self, value: f64, range: (f64, f64)) -> alignment::Horizontal {
		if self.x_min && Self::bound_coincident(value, range.0, range) {
			alignment::Horizontal::Left
		} else if self.x_max && Self::bound_coincident(value, range.1, range) {
			alignment::Horizontal::Right
		} else {
			alignment::Horizontal::Center
		}
	}

	pub fn y_alignment(self, value: f64, range: (f64, f64)) -> alignment::Vertical {
		if self.y_min && Self::bound_coincident(value, range.0, range) {
			alignment::Vertical::Bottom
		} else if self.y_max && Self::bound_coincident(value, range.1, range) {
			alignment::Vertical::Top
		} else {
			alignment::Vertical::Center
		}
	}
}

/// PRODUCT-0116's one authority for rotated x-tick placement.
///
/// A nonzero rotation chooses its leading pivot from the transformed run, so
/// every text run begins at the tick baseline without a width-derived vertical
/// correction. PRODUCT-0119 keeps every run at its tick in both dimensions:
/// estimates reserve guide space but never place ink.
#[derive(Debug, Clone, Copy, PartialEq)]
struct XTickTextPlacement {
	alignment: alignment::Horizontal,
}

impl XTickTextPlacement {
	fn resolve(rotation: TickRotation, legacy_alignment: alignment::Horizontal) -> Self {
		if rotation == TickRotation::ZERO {
			return Self {
				alignment: legacy_alignment,
			};
		}
		let radians = rotation.radians();
		let alignment = Self::leading_alignment(radians);
		Self { alignment }
	}

	fn leading_alignment(radians: f32) -> alignment::Horizontal {
		let left_min_y = Self::minimum_vertical_extent(radians, alignment::Horizontal::Left, 1.0);
		let right_min_y = Self::minimum_vertical_extent(radians, alignment::Horizontal::Right, 1.0);
		if left_min_y >= right_min_y {
			alignment::Horizontal::Left
		} else {
			alignment::Horizontal::Right
		}
	}

	fn run(alignment: alignment::Horizontal, width: f32) -> (f32, f32) {
		match alignment {
			alignment::Horizontal::Left => (0.0, width),
			alignment::Horizontal::Center => (-width / 2.0, width / 2.0),
			alignment::Horizontal::Right => (-width, 0.0),
		}
	}

	fn minimum_vertical_extent(radians: f32, alignment: alignment::Horizontal, width: f32) -> f32 {
		let (run_start, run_end) = Self::run(alignment, width);
		(run_start * radians.sin()).min(run_end * radians.sin())
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AxisTextVisibility {
	pub x: bool,
	pub y: bool,
	pub anchoring: AxisTextAnchoring,
}

impl AxisTextVisibility {
	pub const ALL: Self = Self {
		x: true,
		y: true,
		anchoring: AxisTextAnchoring::NONE,
	};
}

pub(crate) fn resolve_canvas_text(
	presence: &ResolvedPresence<Arc<String>>,
	fallback: Option<String>,
) -> Option<String> {
	match presence {
		ResolvedPresence::Fallback => fallback,
		ResolvedPresence::Show(value) => Some(value.as_ref().to_owned()),
		ResolvedPresence::Hide => None,
	}
}

/// PRODUCT-0106's single authority for vertical guide reservations.
///
/// `plot_padding_top` remains the mark/axis guide allowance. The heading and
/// facet bands are semantic rows above it, so they compose by addition rather
/// than competing for the same pixels.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct VerticalGuideBands {
	heading: f32,
	facet: f32,
	left: f32,
	bottom: f32,
}

/// PRODUCT-0115's shared bottom-guide derivation.
///
/// The x-tick run and the optional axis title share a band but do not always
/// share a position. At zero rotation their legacy offsets are already
/// disjoint; once ticks rotate, the title must begin after the tick run rather
/// than merely compete with it for the band maximum. Keeping the title top and
/// final bottom here makes reservation and placement one authority.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct XAxisGuideLayout {
	pub(crate) tick_end: f32,
	pub(crate) title_top: Option<f32>,
	pub(crate) bottom: f32,
}

/// PRODUCT-0126's shared left-guide derivation.
///
/// The left edge has the same two-occupant shape as the bottom: a tick run and
/// an optional axis title that must clear it. PRODUCT-0114 treated them as
/// alternatives, so a long y-tick label and the rotated title competed for one
/// maximum and drew through each other. This mirrors [`XAxisGuideLayout`]
/// deliberately: two parallel types make an asymmetry between the edges a
/// visible diff rather than a silent omission.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct YAxisGuideLayout {
	pub(crate) tick_end: f32,
	pub(crate) title_x: Option<f32>,
	pub(crate) left: f32,
}

impl VerticalGuideBands {
	pub(crate) const FACET_LABEL_TOP_INSET: f32 = 8.0;

	pub(crate) fn resolve(
		fallback_title: &str,
		facet_label: Option<&str>,
		settings: &ResolvedPlotSettings,
	) -> Self {
		let mut bands = Self::resolve_with_requirements(
			fallback_title,
			facet_label,
			&BandRequirements::EMPTY,
			settings,
		);
		// Legacy callers do not yet have a layout-derived requirement. They are
		// limited to grid-cell facet placement and direct geometry tests; preserve
		// their historical constants until every producer reaches Stage 1.
		bands.left = settings.plot_padding_left;
		bands.bottom = settings.plot_padding_bottom;
		bands
	}

	pub(crate) fn resolve_with_requirements(
		fallback_title: &str,
		facet_label: Option<&str>,
		requirements: &BandRequirements,
		settings: &ResolvedPlotSettings,
	) -> Self {
		Self::resolve_with_requirements_and_x_label(
			fallback_title,
			facet_label,
			requirements,
			!matches!(&settings.x_label, ResolvedPresence::Hide),
			settings,
		)
	}

	pub(crate) fn resolve_with_requirements_and_x_label(
		fallback_title: &str,
		facet_label: Option<&str>,
		requirements: &BandRequirements,
		x_label_visible: bool,
		settings: &ResolvedPlotSettings,
	) -> Self {
		let title_visible = match &settings.title {
			ResolvedPresence::Fallback => !fallback_title.is_empty(),
			ResolvedPresence::Show(title) => !title.is_empty(),
			ResolvedPresence::Hide => false,
		};
		let subtitle_visible = match &settings.subtitle {
			ResolvedPresence::Show(subtitle) => !subtitle.is_empty(),
			ResolvedPresence::Fallback | ResolvedPresence::Hide => false,
		};
		let title = if title_visible {
			settings.title_offset + settings.title_size.pixels()
		} else {
			0.0
		};
		let subtitle = if subtitle_visible {
			settings.subtitle_offset + settings.subtitle_size.pixels()
		} else {
			0.0
		};
		let facet = facet_label
			.filter(|label| !label.is_empty())
			.map(|_| Self::FACET_LABEL_TOP_INSET + Self::facet_label_size(settings).pixels())
			.unwrap_or(0.0);
		let derived_bottom = Self::x_axis_guides(requirements, x_label_visible, settings).bottom;
		let bottom = settings
			.authored_plot_padding_bottom
			.map(PlotPaddingPixels::pixels)
			.map(|authored| derived_bottom.max(authored))
			.unwrap_or(derived_bottom);
		let derived_left = Self::y_axis_guides(
			requirements,
			!matches!(&settings.y_label, ResolvedPresence::Hide),
			settings,
		)
		.left;
		let left = settings
			.authored_plot_padding_left
			.map(PlotPaddingPixels::pixels)
			.map(|authored| derived_left.max(authored))
			.unwrap_or(derived_left);
		Self {
			heading: title.max(subtitle),
			facet,
			left,
			bottom,
		}
	}

	pub(crate) fn x_axis_guides(
		requirements: &BandRequirements,
		x_label_visible: bool,
		settings: &ResolvedPlotSettings,
	) -> XAxisGuideLayout {
		if requirements.x.is_empty() {
			return XAxisGuideLayout {
				tick_end: 0.0,
				title_top: None,
				bottom: 0.0,
			};
		}
		let rotation = settings.x_tick_rotation.radians();
		let rotated_tick_height = requirements.estimated_x_width(settings.x_tick_size.pixels())
			* rotation.sin().abs()
			+ settings.x_tick_size.pixels() * rotation.cos().abs();
		let tick_end = settings.x_label_offset + rotated_tick_height;
		let title_top = x_label_visible.then(|| {
			// At zero degrees this preserves the historical 45 px title top.
			// Once ticks extend farther, the title follows their run with the
			// same legacy clearance instead of occupying it.
			settings
				.x_label_padding
				.max(tick_end + Self::x_axis_title_gap(settings))
		});
		let bottom = title_top
			.map(|top| top + settings.x_label_size.pixels())
			.unwrap_or(tick_end);
		XAxisGuideLayout {
			tick_end,
			title_top,
			bottom,
		}
	}

	/// The gap between a tick's axis line and the right edge of its label ink.
	///
	/// Every shared-band y-tick draw site anchors right-aligned text at
	/// `tick.x - 10.0`; this is that same constant read by the reservation.
	pub(crate) const Y_TICK_LABEL_OFFSET: f32 = 10.0;

	/// Clearance between the y-tick run and the y-axis title.
	///
	/// Unlike the bottom edge, the left edge has no data-independent legacy
	/// tick extent to subtract from `y_label_padding`: a tick reaches into the
	/// bottom band by its font height but into the left band by its *width*.
	/// So this is banked rather than derived, and
	/// `product_0126_stage_0_banks_a_gap_the_legacy_short_label_geometry_affords`
	/// pins it below the clearance the accepted short-numeric geometry already
	/// leaves. It is a single guide-layout constant, not an authored setting:
	/// `y_label_padding` remains the floor, so an author who moves the title
	/// still moves it.
	pub(crate) const Y_AXIS_TITLE_GAP: f32 = 6.0;

	pub(crate) fn y_axis_guides(
		requirements: &BandRequirements,
		y_label_visible: bool,
		settings: &ResolvedPlotSettings,
	) -> YAxisGuideLayout {
		if requirements.y.is_empty() {
			return YAxisGuideLayout {
				tick_end: 0.0,
				title_x: None,
				left: 0.0,
			};
		}
		let tick_end = Self::Y_TICK_LABEL_OFFSET
			+ requirements.estimated_y_width(settings.y_tick_size.pixels());
		let title_x = y_label_visible.then(|| {
			// Below the legacy offset this preserves the historical 85 px title
			// position. Once the tick run reaches farther, the title follows it
			// outward instead of being drawn inside it.
			settings
				.y_label_padding
				.max(tick_end + Self::Y_AXIS_TITLE_GAP)
		});
		let left = title_x
			.map(|x| x + settings.y_label_size.pixels())
			.unwrap_or(tick_end);
		YAxisGuideLayout {
			tick_end,
			title_x,
			left,
		}
	}

	fn x_axis_title_gap(settings: &ResolvedPlotSettings) -> f32 {
		// Preserve the existing zero-degree title placement: ticks ended at
		// x_label_offset + x_tick_size, and the title began at x_label_padding.
		(settings.x_label_padding - (settings.x_label_offset + settings.x_tick_size.pixels()))
			.max(0.0)
	}

	#[cfg(test)]
	pub(crate) const fn heading_height(self) -> f32 {
		self.heading
	}

	#[cfg(test)]
	pub(crate) fn plot_top(self, settings: &ResolvedPlotSettings) -> f32 {
		self.heading + self.facet + settings.plot_padding_top
	}

	pub(crate) fn facet_label_size(settings: &ResolvedPlotSettings) -> FacetLabelTextSize {
		settings.facet_label_size
	}

	pub(crate) fn plot_area(
		self,
		bounds: Rect<Widget>,
		padding: f32,
		settings: &ResolvedPlotSettings,
	) -> Rect<Widget> {
		let left = padding + self.left;
		let right = padding + settings.effective_plot_padding_right();
		let top = padding + self.heading + self.facet + settings.effective_plot_padding_top();
		let bottom = padding + self.bottom;
		bounds.region(
			bounds.x() + left,
			bounds.y() + top,
			(bounds.width() - left - right).max(0.0),
			(bounds.height() - top - bottom).max(0.0),
		)
	}

	pub(crate) fn grid_tiled_area(
		self,
		outer: Rect<Widget>,
		settings: &ResolvedPlotSettings,
	) -> Rect<Widget> {
		let left = self.left.min(outer.width());
		let bottom = self.bottom.min(outer.height());
		let top = self.heading.min((outer.height() - bottom).max(0.0));
		// An explicit right edge belongs to the composed figure, so a grid
		// reserves it once before cells divide. Legacy right padding stays local
		// to preserve existing grid geometry when no edge is authored.
		let right = settings
			.explicit_plot_padding_right()
			.unwrap_or(0.0)
			.min((outer.width() - left).max(0.0));
		outer.region(
			outer.x() + left,
			outer.y() + top,
			(outer.width() - left - right).max(0.0),
			(outer.height() - top - bottom).max(0.0),
		)
	}

	pub(crate) fn grid_cell_plot_area(
		self,
		cell_bounds: Rect<Widget>,
		settings: &ResolvedPlotSettings,
	) -> Rect<Widget> {
		let top = (self.facet + settings.effective_plot_padding_top()).min(cell_bounds.height());
		let right = settings
			.explicit_plot_padding_right()
			.map(|_| 0.0)
			.unwrap_or(settings.plot_padding_right)
			.min(cell_bounds.width());
		cell_bounds.region(
			cell_bounds.x(),
			cell_bounds.y() + top,
			(cell_bounds.width() - right).max(0.0),
			(cell_bounds.height() - top).max(0.0),
		)
	}
}

#[derive(Default)]
pub struct PlotCanvasState {
	last_hover: Option<String>,
	plot_cache: canvas::Cache,
	overlay_cache: canvas::Cache,
	render_revision: std::cell::Cell<u64>,
	stable_bounds: std::cell::RefCell<Option<Rectangle>>,
}

impl<'a> PlotWidget<'a> {
	fn resolved_x_label(&self) -> Option<String> {
		resolve_canvas_text(&self.settings.x_label, Some(self.kernel.x_label()))
			.filter(|label| !label.is_empty())
	}

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

	fn plot_area(&self, bounds: Rect<Widget>, requirements: &BandRequirements) -> Rect<Widget> {
		VerticalGuideBands::resolve_with_requirements_and_x_label(
			&self.title,
			None,
			requirements,
			self.resolved_x_label().is_some(),
			&self.settings,
		)
		.plot_area(bounds, self.padding, &self.settings)
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
		let clipped_plot = self.kernel.requires_plot_area_clip();
		if clipped_plot {
			backend.with_clip(bounds.into_iced(), &mut |backend| {
				backend.fill_rectangle(
					bounds.origin().into_iced(),
					bounds.size(),
					self.settings.chrome.canvas_background.iced(),
				);
			});
		} else {
			backend.fill_rectangle(
				Point::ORIGIN,
				bounds.size(),
				self.settings.chrome.canvas_background.iced(),
			);
		}
		let legend_content = self
			.kernel
			.legend_content(&self.settings)
			.and_then(crate::plot::legend::apply_policy);
		let legend = legend_layout(bounds, legend_content.as_ref(), &self.settings);
		let layout = self.kernel.layout(self.settings.clone());
		let requirements = BandRequirements::from_layout(&layout, &self.settings);
		let plot_area = self
			.plot_area(legend.content_bounds, &requirements)
			.into_iced();
		let transform = CoordinateTransformer::new(&layout, plot_area);
		let draw_plot = &mut |backend: &mut dyn PlotBackend| {
			backend.translate(iced::Vector::new(plot_area.x, plot_area.y));
			self.draw_layout_grid(backend, plot_area, &transform, &layout);
			self.kernel.plot_with_settings(
				backend,
				plot_area,
				&transform,
				Cursor::Unavailable,
				&self.settings_model,
			);
		};
		if clipped_plot {
			backend.with_clip(plot_area, draw_plot);
		} else {
			backend.with_save(draw_plot);
		}
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
		let layout = self.kernel.layout(self.settings.clone());
		let requirements = BandRequirements::from_layout(&layout, &self.settings);
		let plot_extent = self.plot_area(legend.content_bounds, &requirements);
		let plot_area = plot_extent.into_iced();
		let bounds = bounds.into_iced();
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
		// PRODUCT-0115: axis text may cross facet-cell edges but never the figure
		// edge. This mirrors the composite overlay's figure-wide clip.
		backend.with_clip(bounds, &mut |backend| {
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
						..
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
						..
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
				size: iced::Pixels(self.settings.title_size.pixels()),
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
				size: iced::Pixels(self.settings.subtitle_size.pixels()),
				align_x: alignment::Horizontal::Center.into(),
				align_y: alignment::Vertical::Top,
				..Default::default()
			});
		}
		self.draw_axis_labels(backend, plot_area, &requirements);
	}

	fn render_global_headings(&self, backend: &mut dyn PlotBackend, bounds: Rectangle) {
		if let Some(title) = resolve_canvas_text(&self.settings.title, Some(self.title.clone())) {
			backend.fill_text(Text {
				content: title,
				position: Point::new(bounds.width / 2.0, self.settings.title_offset),
				color: self.settings.chrome.title_text.iced(),
				size: iced::Pixels(self.settings.title_size.pixels()),
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
				size: iced::Pixels(self.settings.subtitle_size.pixels()),
				align_x: alignment::Horizontal::Center.into(),
				align_y: alignment::Vertical::Top,
				..Default::default()
			});
		}
	}

	pub(crate) fn draw_axis_labels(
		&self,
		backend: &mut dyn PlotBackend,
		plot_area: Rectangle,
		requirements: &BandRequirements,
	) {
		// Radial and parallel layouts have no Cartesian axis band. Their kernels
		// may still expose field names for diagnostics or hover, but drawing those
		// names as axis titles has neither a placement nor a reservation.
		if requirements.x.is_empty() && requirements.y.is_empty() {
			return;
		}
		let x_label = self.resolved_x_label();
		let y_label = resolve_canvas_text(&self.settings.y_label, Some(self.kernel.y_label()));
		if let Some(x_label) = x_label {
			let guides = VerticalGuideBands::x_axis_guides(requirements, true, &self.settings);
			let title_top = guides
				.title_top
				.expect("visible x-axis title has a shared guide placement");
			backend.fill_text(Text {
				content: x_label,
				position: Point::new(
					plot_area.x + plot_area.width / 2.0,
					plot_area.y + plot_area.height + title_top,
				),
				color: self.settings.chrome.axis_label_text.iced(),
				size: iced::Pixels(self.settings.x_label_size.pixels()),
				align_x: alignment::Horizontal::Center.into(),
				align_y: alignment::Vertical::Top,
				..Default::default()
			});
		}
		if let Some(y_label) = y_label.filter(|label| !label.is_empty()) {
			let guides = VerticalGuideBands::y_axis_guides(requirements, true, &self.settings);
			let title_x = guides
				.title_x
				.expect("visible y-axis title has a shared guide placement");
			backend.with_save(&mut |backend| {
				backend.translate(iced::Vector::new(
					plot_area.x - title_x,
					plot_area.y + plot_area.height / 2.0,
				));
				backend.rotate(-std::f32::consts::FRAC_PI_2);
				backend.fill_text(Text {
					content: y_label.clone(),
					position: Point::ORIGIN,
					color: self.settings.chrome.axis_label_text.iced(),
					size: iced::Pixels(self.settings.y_label_size.pixels()),
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
						composite.hover(
							widget_bounds,
							self.padding,
							&self.title,
							cursor,
							&self.settings_model,
						)
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
				let layout = self.kernel.layout(self.settings.clone());
				let requirements = BandRequirements::from_layout(&layout, &self.settings);
				let plot_area = self.plot_area(legend.content_bounds, &requirements);
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
				..
			} => self.draw_categorical_grid(backend, area, transform, categories, *y_range),
			PlotLayout::CategoricalY {
				categories,
				x_range,
				..
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
					size: iced::Pixels(self.settings.y_tick_size.pixels()),
					align_x: alignment::Horizontal::Right.into(),
					align_y: text.anchoring.y_alignment(data_y, y_range),
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
				let label = format_label(data_x, x_axis_type);
				let placement = XTickTextPlacement::resolve(
					self.settings.x_tick_rotation,
					text.anchoring.x_alignment(data_x, x_range),
				);
				backend.with_save(&mut |backend| {
					backend.translate(iced::Vector::new(
						p_bottom.x,
						p_bottom.y + self.settings.x_label_offset,
					));
					backend.rotate(self.settings.x_tick_rotation.radians());
					backend.fill_text(Text {
						content: label.clone(),
						position: Point::ORIGIN,
						color: self.settings.chrome.tick_label_text.iced(),
						size: iced::Pixels(self.settings.x_tick_size.pixels()),
						align_x: placement.alignment.into(),
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
		let PlotLayout::CategoricalX { y_axis_type, .. } = transform.layout else {
			unreachable!("categorical-x chrome requires a categorical-x transform")
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
					content: format_label(data_y, *y_axis_type),
					position: Point::new(p_left.x - 10.0, p_left.y),
					color: self.settings.chrome.tick_label_text.iced(),
					size: iced::Pixels(self.settings.y_tick_size.pixels()),
					align_x: alignment::Horizontal::Right.into(),
					align_y: text.anchoring.y_alignment(data_y, y_range),
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
				let placement = XTickTextPlacement::resolve(
					self.settings.x_tick_rotation,
					alignment::Horizontal::Center,
				);
				backend.with_save(&mut |backend| {
					backend.translate(iced::Vector::new(
						center_px.x,
						center_px.y + self.settings.x_label_offset,
					));
					backend.rotate(self.settings.x_tick_rotation.radians());
					backend.fill_text(Text {
						content: cat.clone(),
						position: Point::ORIGIN,
						color: self.settings.chrome.tick_label_text.iced(),
						size: iced::Pixels(self.settings.x_tick_size.pixels()),
						align_x: placement.alignment.into(),
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
		let PlotLayout::CategoricalY { x_axis_type, .. } = transform.layout else {
			unreachable!("categorical-y chrome requires a categorical-y transform")
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
					content: format_label(data_x, *x_axis_type),
					position: Point::new(p_bottom.x, p_bottom.y + 10.0),
					color: self.settings.chrome.tick_label_text.iced(),
					size: iced::Pixels(self.settings.x_tick_size.pixels()),
					align_x: text.anchoring.x_alignment(data_x, x_range).into(),
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
					size: iced::Pixels(self.settings.y_tick_size.pixels()),
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
				let placement = XTickTextPlacement::resolve(
					self.settings.x_tick_rotation,
					alignment::Horizontal::Center,
				);
				backend.with_save(&mut |backend| {
					backend.translate(iced::Vector::new(
						tick_x,
						tick_y + self.settings.x_label_offset,
					));
					backend.rotate(self.settings.x_tick_rotation.radians());
					backend.fill_text(Text {
						content: cat.clone(),
						position: Point::ORIGIN,
						color: self.settings.chrome.tick_label_text.iced(),
						size: iced::Pixels(self.settings.x_tick_size.pixels()),
						align_x: placement.alignment.into(),
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
					size: iced::Pixels(self.settings.y_tick_size.pixels()),
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
						size: iced::Pixels(self.settings.y_tick_size.pixels()),
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
					position: Point::new(top_px.x, top_px.y - self.settings.x_label_size.pixels()),
					color: self.settings.chrome.axis_label_text.iced(),
					size: iced::Pixels(self.settings.x_label_size.pixels()),
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
		AxisType::Percent => format!("{}%", format_label(value, AxisType::Linear)),
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
mod product_0106_vertical_guide_band_oracles {
	use super::*;
	use std::sync::Arc;

	fn bounds() -> Rect<Widget> {
		Rect::<Widget>::from_iced_frame(Rectangle::new(
			Point::new(40.0, 30.0),
			iced::Size::new(600.0, 400.0),
		))
	}

	#[test]
	fn product_0114_band_requirements_are_layout_derived_and_move_only_their_bands() {
		let settings = ResolvedPlotSettings::default();
		let temporal_y = PlotLayout::Cartesian {
			x_range: (0.0, 1.0),
			y_range: (20_000.0, 20_010.0),
			x_axis_type: AxisType::Linear,
			y_axis_type: AxisType::Date,
		};
		let categorical_x = PlotLayout::CategoricalX {
			categories: vec!["short".to_owned(), "a deliberately longer label".to_owned()],
			y_range: (0.0, 1.0),
			y_axis_type: AxisType::Linear,
		};
		let temporal = BandRequirements::from_layout(&temporal_y, &settings);
		assert_eq!(
			temporal.y,
			vec![AxisBandRequirement::Formatted {
				domain: (20_000.0, 20_010.0),
				axis_type: AxisType::Date,
				placement: settings.y_tick_placement,
			}]
		);
		let categorical = BandRequirements::from_layout(&categorical_x, &settings);
		assert_eq!(
			categorical.x,
			vec![AxisBandRequirement::Categories(vec![
				"short".to_owned(),
				"a deliberately longer label".to_owned(),
			])]
		);

		let grid = BandRequirements::across_layouts([&temporal_y, &categorical_x], &settings);
		assert_eq!(grid.x.len(), 2, "grid requirements retain both producers");
		assert_eq!(grid.y.len(), 2, "the later estimator owns the maximum");

		let legacy =
			VerticalGuideBands::resolve("", None, &settings).plot_area(bounds(), 6.0, &settings);
		let staged = VerticalGuideBands::resolve_with_requirements("", None, &grid, &settings)
			.plot_area(bounds(), 6.0, &settings);
		// PRODUCT-0126 moved this fixture: its 135 px y-tick run overran the
		// legacy 85 px title offset, so the former 45 px delta was the width of
		// a band whose title drew inside its own ticks. The left edge now
		// sequences them, and the exact growth is banked rather than waived.
		assert_eq!(staged.x() - legacy.x(), 71.0);
		assert_eq!(legacy.width() - staged.width(), 71.0);
		assert_eq!(staged.height() - legacy.height(), 15.0);
		assert_eq!(staged.y(), legacy.y());
	}

	#[test]
	fn product_0115_bottom_guides_sequence_the_title_after_rotated_ticks() {
		let requirement = BandRequirements {
			x: vec![AxisBandRequirement::Categories(vec!["wide".to_owned()])],
			y: Vec::new(),
		};

		let zero = ResolvedPlotSettings::default();
		let zero_guides = VerticalGuideBands::x_axis_guides(&requirement, true, &zero);
		let banked_0114_max = (zero.x_label_offset + zero.x_tick_size.pixels())
			.max(zero.x_label_padding + zero.x_label_size.pixels());
		assert_eq!(
			zero_guides.bottom, banked_0114_max,
			"the sequenced guides reduce to PRODUCT-0114's banked maximum at zero degrees"
		);
		assert_eq!(zero_guides.title_top, Some(zero.x_label_padding));

		for rotation in [-90.0, -45.0, 45.0, 90.0] {
			let settings = ResolvedPlotSettings {
				x_tick_rotation: TickRotation::new(rotation).unwrap(),
				..ResolvedPlotSettings::default()
			};
			let guides = VerticalGuideBands::x_axis_guides(&requirement, true, &settings);
			let title_top = guides.title_top.expect("visible title has a placement");
			assert!(
				title_top >= guides.tick_end + VerticalGuideBands::x_axis_title_gap(&settings),
				"{rotation} degree title starts after the rotated tick run"
			);
			assert_eq!(guides.bottom, title_top + settings.x_label_size.pixels());
		}

		let no_title = VerticalGuideBands::x_axis_guides(&requirement, false, &zero);
		assert_eq!(no_title.title_top, None);
		assert_eq!(
			no_title.bottom, no_title.tick_end,
			"an absent title consumes neither title clearance nor title height"
		);
	}

	/// A short numeric axis whose selected tick run ends inside the legacy title
	/// offset. PRODUCT-0127 now derives this from the actual Count(8) labels,
	/// rather than from an unrelated format-label branch boundary.
	fn short_numeric_y() -> BandRequirements {
		BandRequirements {
			x: Vec::new(),
			y: vec![AxisBandRequirement::Formatted {
				domain: (1.0, 100.0),
				axis_type: AxisType::Linear,
				placement: TickPlacement::DEFAULT,
			}],
		}
	}

	#[test]
	fn product_0126_stage_0_banks_a_gap_the_legacy_short_label_geometry_affords() {
		let settings = ResolvedPlotSettings::default();
		let guides = VerticalGuideBands::y_axis_guides(&short_numeric_y(), true, &settings);
		let legacy_clearance = settings.y_label_padding - guides.tick_end;
		assert!(
			legacy_clearance > 0.0,
			"the banked short-label fixture must be one the legacy geometry \
			 actually placed without overlap, or every parity gate below is vacuous"
		);
		assert!(
			VerticalGuideBands::Y_AXIS_TITLE_GAP <= legacy_clearance,
			"a gap wider than {legacy_clearance} px would move short labels the \
			 record promises not to move"
		);
	}

	#[test]
	fn product_0126_left_guides_sequence_the_title_after_a_long_tick_run() {
		let settings = ResolvedPlotSettings::default();

		// Short labels reduce to the legacy geometry exactly.
		let short = VerticalGuideBands::y_axis_guides(&short_numeric_y(), true, &settings);
		assert_eq!(
			short.title_x,
			Some(settings.y_label_padding),
			"below the legacy offset the title keeps its accepted placement"
		);
		assert_eq!(
			short.left,
			settings.y_label_padding + settings.y_label_size.pixels()
		);

		// A tick run past the legacy offset pushes the title outside it. This is
		// the Multivariate heatmap's shape: a long categorical y label.
		let long = BandRequirements {
			x: Vec::new(),
			y: vec![AxisBandRequirement::Categories(vec![
				"Reliability".to_owned(),
			])],
		};
		let guides = VerticalGuideBands::y_axis_guides(&long, true, &settings);
		let title_x = guides.title_x.expect("a visible title has a placement");
		assert!(
			guides.tick_end > settings.y_label_padding,
			"the exhibit's tick run must actually overrun the legacy offset"
		);
		assert_eq!(
			title_x,
			guides.tick_end + VerticalGuideBands::Y_AXIS_TITLE_GAP,
			"the left edge is a sequence: a maximum of the old arms cannot pass"
		);
		assert!(
			title_x - guides.tick_end >= VerticalGuideBands::Y_AXIS_TITLE_GAP,
			"the tick run and the title are disjoint by at least the banked gap"
		);
		assert_eq!(guides.left, title_x + settings.y_label_size.pixels());

		// A hidden title spends no clearance on absent ink.
		let hidden = VerticalGuideBands::y_axis_guides(&long, false, &settings);
		assert_eq!(hidden.title_x, None);
		assert_eq!(
			hidden.left, hidden.tick_end,
			"an absent title consumes neither title clearance nor title width"
		);
	}

	#[test]
	fn product_0126_reservation_and_placement_read_one_left_guide() {
		let settings = ResolvedPlotSettings::default();
		let long = BandRequirements {
			x: Vec::new(),
			y: vec![AxisBandRequirement::Categories(vec![
				"Reliability".to_owned(),
			])],
		};
		let guides = VerticalGuideBands::y_axis_guides(&long, true, &settings);
		let reserved = VerticalGuideBands::resolve_with_requirements("", None, &long, &settings);
		assert_eq!(
			reserved.left, guides.left,
			"the band and the title placement derive from one value"
		);
		// The title's ink starts at `title_x` left of the plot and extends
		// `y_label_size` further out; the band must contain all of it.
		assert!(
			guides.title_x.expect("visible") + settings.y_label_size.pixels() <= reserved.left,
			"the reserved band contains the placement it authorized"
		);
	}

	#[test]
	fn product_0126_the_y_tick_estimate_reserves_the_band_without_placing_ink() {
		let settings = ResolvedPlotSettings::default();
		let short = VerticalGuideBands::y_axis_guides(&short_numeric_y(), true, &settings);
		let long = VerticalGuideBands::y_axis_guides(
			&BandRequirements {
				x: Vec::new(),
				y: vec![AxisBandRequirement::Categories(vec![
					"Reliability".to_owned(),
				])],
			},
			true,
			&settings,
		);
		assert!(long.left > short.left, "a longer run reserves a wider band");
		// The estimate enters the guide only through `tick_end`, and `tick_end`
		// is the fixed tick-relative offset plus the estimate. Nothing in the
		// guide scales or repositions an individual label.
		for (guides, requirements) in [
			(short, short_numeric_y()),
			(
				long,
				BandRequirements {
					x: Vec::new(),
					y: vec![AxisBandRequirement::Categories(vec![
						"Reliability".to_owned(),
					])],
				},
			),
		] {
			assert_eq!(
				guides.tick_end - requirements.estimated_y_width(settings.y_tick_size.pixels()),
				VerticalGuideBands::Y_TICK_LABEL_OFFSET,
				"a longer estimate grows the band and never the tick-relative offset"
			);
		}
	}

	#[test]
	fn product_0122_each_axis_and_tick_size_moves_only_its_reserved_band() {
		let x_requirement = BandRequirements {
			x: vec![AxisBandRequirement::Categories(vec!["x".to_owned()])],
			y: Vec::new(),
		};
		let y_requirement = BandRequirements {
			x: Vec::new(),
			y: vec![AxisBandRequirement::Formatted {
				domain: (0.0, 1.0),
				axis_type: AxisType::Linear,
				placement: TickPlacement::DEFAULT,
			}],
		};
		let base = ResolvedPlotSettings::default();

		let base_x_label = VerticalGuideBands::x_axis_guides(&x_requirement, true, &base).bottom;
		let mut x_label = base.clone();
		x_label.x_label_size = XLabelTextSize::new(48.0).unwrap();
		assert_eq!(
			VerticalGuideBands::x_axis_guides(&x_requirement, true, &x_label).bottom - base_x_label,
			28.0,
			"x-axis title size changes only the title arm of the bottom band"
		);

		let base_x_tick = VerticalGuideBands::x_axis_guides(&x_requirement, false, &base).bottom;
		let mut x_tick = base.clone();
		x_tick.x_tick_size = XTickTextSize::new(48.0).unwrap();
		assert_eq!(
			VerticalGuideBands::x_axis_guides(&x_requirement, false, &x_tick).bottom - base_x_tick,
			30.0,
			"x-tick size changes only the generated-tick arm of the bottom band"
		);

		let base_y_label =
			VerticalGuideBands::resolve_with_requirements("", None, &y_requirement, &base).left;
		let mut y_label = base.clone();
		y_label.y_label_size = YLabelTextSize::new(48.0).unwrap();
		assert_eq!(
			VerticalGuideBands::resolve_with_requirements("", None, &y_requirement, &y_label).left
				- base_y_label,
			28.0,
			"y-axis title size changes only the title arm of the left band"
		);

		let numeric = BandRequirements {
			x: Vec::new(),
			y: vec![AxisBandRequirement::Formatted {
				domain: (-999_999.9, 999_999.9),
				axis_type: AxisType::Linear,
				placement: TickPlacement::DEFAULT,
			}],
		};
		let mut y_tick_base = base.clone();
		y_tick_base.y_label = ResolvedPresence::Hide;
		let before =
			VerticalGuideBands::resolve_with_requirements("", None, &numeric, &y_tick_base).left;
		let mut y_tick = y_tick_base.clone();
		y_tick.y_tick_size = YTickTextSize::new(48.0).unwrap();
		let after = VerticalGuideBands::resolve_with_requirements("", None, &numeric, &y_tick).left;
		assert_eq!(
			after - before,
			numeric.estimated_y_width(y_tick.y_tick_size.pixels())
				- numeric.estimated_y_width(y_tick_base.y_tick_size.pixels()),
			"y-tick size changes only the generated-tick arm of the left band"
		);
	}

	#[test]
	fn product_0114_bottom_band_tracks_direct_rotation_without_a_surface() {
		let requirement = BandRequirements {
			x: vec![AxisBandRequirement::Categories(vec![
				"abcdefghij".to_owned(),
			])],
			y: Vec::new(),
		};
		let width = estimate_ascii_label_width("abcdefghij", 18.0);
		for (rotation, expected) in [
			(0.0, 28.0),
			(
				45.0,
				10.0 + (width + 18.0) * std::f32::consts::FRAC_1_SQRT_2,
			),
			(90.0, 10.0 + width),
		] {
			let settings = ResolvedPlotSettings {
				x_tick_rotation: TickRotation::new(rotation).unwrap(),
				..ResolvedPlotSettings::default()
			};
			let bands = VerticalGuideBands::resolve_with_requirements_and_x_label(
				"",
				None,
				&requirement,
				false,
				&settings,
			);
			assert!(
				(bands.bottom - expected).abs() < 0.001,
				"{rotation} degrees reserves the rotated tick extent"
			);
		}
	}

	#[test]
	fn product_0117_class_budget_moves_only_the_derived_category_band_delta() {
		let label = "International partner enablement and certification";
		let settings = ResolvedPlotSettings {
			x_tick_rotation: TickRotation::new(45.0).unwrap(),
			..ResolvedPlotSettings::default()
		};
		let requirements = BandRequirements {
			x: vec![AxisBandRequirement::Categories(vec![label.to_owned()])],
			y: Vec::new(),
		};
		let old_width = label.chars().count() as f32 * settings.x_tick_size.pixels();
		let new_width = estimate_ascii_label_width(label, settings.x_tick_size.pixels());
		assert!(
			new_width < old_width,
			"PRODUCT-0117 prose class budget improves the flat bound"
		);
		let guides = VerticalGuideBands::x_axis_guides(&requirements, true, &settings);
		let old_tick_end = settings.x_label_offset
			+ old_width * settings.x_tick_rotation.radians().sin().abs()
			+ settings.x_tick_size.pixels() * settings.x_tick_rotation.radians().cos().abs();
		let old_title_top = settings
			.x_label_padding
			.max(old_tick_end + VerticalGuideBands::x_axis_title_gap(&settings));
		let old_bottom = old_title_top + settings.x_label_size.pixels();
		assert!(
			(guides.bottom - old_bottom
				+ (old_width - new_width) * settings.x_tick_rotation.radians().sin().abs())
			.abs() < 0.001,
			"PRODUCT-0117 guide geometry moves by exactly the rotated estimate delta"
		);
	}

	#[test]
	fn product_0116_estimation_never_moves_rotated_tick_ink() {
		let width = 120.0;
		let zero = XTickTextPlacement::resolve(TickRotation::ZERO, alignment::Horizontal::Center);
		assert_eq!(zero.alignment, alignment::Horizontal::Center);

		for (rotation, expected_alignment) in [
			(
				TickRotation::new(45.0).unwrap(),
				alignment::Horizontal::Left,
			),
			(
				TickRotation::new(-45.0).unwrap(),
				alignment::Horizontal::Right,
			),
			(
				TickRotation::new(90.0).unwrap(),
				alignment::Horizontal::Left,
			),
			(
				TickRotation::new(-90.0).unwrap(),
				alignment::Horizontal::Right,
			),
		] {
			let radians = rotation.radians();
			let placement = XTickTextPlacement::resolve(rotation, alignment::Horizontal::Center);
			assert_eq!(
				placement.alignment, expected_alignment,
				"the pivot is selected from the transformed run, never from an endpoint policy"
			);
			for realized_width in [1.0, 37.5, width] {
				assert!(
					XTickTextPlacement::minimum_vertical_extent(
						radians,
						placement.alignment,
						realized_width,
					)
					.abs() < 0.001,
					"the chosen pivot has no negative vertical extent for any positive run width"
				);
			}
		}
	}

	#[test]
	fn product_0119_estimation_never_moves_x_tick_ink() {
		for (rotation, expected) in [
			(TickRotation::ZERO, alignment::Horizontal::Center),
			(
				TickRotation::new(-45.0).unwrap(),
				alignment::Horizontal::Right,
			),
			(
				TickRotation::new(45.0).unwrap(),
				alignment::Horizontal::Left,
			),
		] {
			assert_eq!(
				XTickTextPlacement::resolve(rotation, alignment::Horizontal::Center).alignment,
				expected,
				"PRODUCT-0119 retains only PRODUCT-0116's pivot selection at {rotation:?}"
			);
		}
	}

	#[test]
	fn product_0127_requirements_carry_placement_and_measure_selected_labels() {
		let settings = ResolvedPlotSettings::default();
		let temporal_y = PlotLayout::Cartesian {
			x_range: (0.0, 1.0),
			y_range: (20_000.0, 20_010.0),
			x_axis_type: AxisType::Linear,
			y_axis_type: AxisType::Date,
		};
		let categorical_y = PlotLayout::CategoricalY {
			categories: vec!["longest category".to_owned()],
			x_range: (20_000.0, 20_010.0),
			x_axis_type: AxisType::Date,
		};
		assert!(matches!(
			BandRequirements::from_layout(&temporal_y, &settings)
				.y
				.as_slice(),
			[AxisBandRequirement::Formatted {
				axis_type: AxisType::Date,
				placement: TickPlacement::DEFAULT,
				..
			}]
		));
		assert!(matches!(
			BandRequirements::from_layout(&categorical_y, &settings)
				.x
				.as_slice(),
			[AxisBandRequirement::Formatted {
				axis_type: AxisType::Date,
				placement: TickPlacement::DEFAULT,
				..
			}]
		));
		assert!(matches!(
			BandRequirements::from_layout(&categorical_y, &settings)
				.y
				.as_slice(),
			[AxisBandRequirement::Categories(_)]
		));

		let selected = AxisBandRequirement::Formatted {
			domain: (0.0, 100.0),
			axis_type: AxisType::Linear,
			placement: TickPlacement::DEFAULT,
		};
		assert_eq!(
			selected.estimated_width(18.0),
			54.0,
			"Count(8) selects 0, 12.5, ..., 100; its widest label is 12.5, not an unselected scientific branch boundary"
		);

		let stepped = AxisBandRequirement::Formatted {
			domain: (0.0, 100.0),
			axis_type: AxisType::Linear,
			placement: TickPlacement::Step(PositiveFinite(25.0)),
		};
		assert_eq!(
			stepped.estimated_width(18.0),
			40.5,
			"the requirement follows the step-selected 0, 25, ..., 100 labels"
		);

		for (requirement, message) in [
			(
				AxisBandRequirement::Formatted {
					domain: (1.0, 24.0),
					axis_type: AxisType::Linear,
					placement: TickPlacement::Step(PositiveFinite(25.0)),
				},
				"a valid empty step selection has no tick-label requirement",
			),
			(
				AxisBandRequirement::Formatted {
					domain: (0.0, 100.0),
					axis_type: AxisType::Linear,
					placement: TickPlacement::Step(PositiveFinite(0.000_001)),
				},
				"a refused over-capacity step selection has no tick-label requirement",
			),
		] {
			assert_eq!(requirement.estimated_width(18.0), 0.0, "{message}");
		}

		let short = AxisBandRequirement::Categories(vec!["aaaa".to_owned()]);
		let many_short = AxisBandRequirement::Categories(vec!["aaaa".to_owned(); 20]);
		let longer = AxisBandRequirement::Categories(vec!["aaaaa".to_owned()]);
		assert_eq!(
			short.estimated_width(18.0),
			many_short.estimated_width(18.0)
		);
		assert!(longer.estimated_width(18.0) > short.estimated_width(18.0));
	}

	#[test]
	fn product_0127_empty_selected_ticks_reserve_no_left_clearance() {
		let settings = ResolvedPlotSettings::default();
		for (domain, placement, legacy_candidate, expected_legacy_width, message) in [
			(
				(1.0, 24.0),
				TickPlacement::Step(PositiveFinite(25.0)),
				24.0,
				27.0,
				"a valid empty selection",
			),
			(
				(0.0, 100.0),
				TickPlacement::Step(PositiveFinite(0.000_001)),
				0.000_999,
				81.0,
				"a refused selection",
			),
		] {
			let requirement = BandRequirements {
				x: Vec::new(),
				y: vec![AxisBandRequirement::Formatted {
					domain,
					axis_type: AxisType::Linear,
					placement,
				}],
			};
			assert_eq!(
				requirement.estimated_y_width(settings.y_tick_size.pixels()),
				0.0,
				"{message} has no label requirement"
			);
			let guides = VerticalGuideBands::y_axis_guides(&requirement, true, &settings);
			assert_eq!(guides.tick_end, VerticalGuideBands::Y_TICK_LABEL_OFFSET);
			assert_eq!(guides.title_x, Some(settings.y_label_padding));
			assert_eq!(
				guides.left,
				settings.y_label_padding + settings.y_label_size.pixels(),
				"{message} spends no tick-derived title clearance"
			);
			assert_eq!(
				estimate_formatted_label_width(
					&format_label(legacy_candidate, AxisType::Linear),
					settings.y_tick_size.pixels(),
				),
				expected_legacy_width,
				"{message} previously reserved for a candidate the draw path did not emit"
			);
		}
	}

	fn assert_product_0127_matrix_entry(
		name: &str,
		requirement: AxisBandRequirement,
		legacy_width: f32,
		selected_width: f32,
		settings: &ResolvedPlotSettings,
	) {
		let actual = requirement.estimated_width(settings.y_tick_size.pixels());
		assert!(
			(actual - selected_width).abs() < 0.001,
			"{name} selected-label width: expected {selected_width}, got {actual}"
		);
		assert!(
			actual <= legacy_width + 0.001,
			"{name} never reserves less than the retired candidate requirement"
		);
		assert!(
			((actual - legacy_width) - (selected_width - legacy_width)).abs() < 0.001,
			"{name} keeps its banked direction and exact delta"
		);
		let requirements = BandRequirements {
			x: Vec::new(),
			y: vec![requirement],
		};
		let guides = VerticalGuideBands::y_axis_guides(&requirements, true, settings);
		assert_eq!(
			guides.tick_end - VerticalGuideBands::Y_TICK_LABEL_OFFSET,
			actual,
			"{name} reserves every selected label before it places the y-axis title"
		);
	}

	#[test]
	fn product_0127_banks_the_complete_selected_label_matrix() {
		let settings = ResolvedPlotSettings::default();
		for (name, requirement, legacy_width, selected_width) in [
			(
				"zero-based linear",
				AxisBandRequirement::Formatted {
					domain: (0.0, 100.0),
					axis_type: AxisType::Linear,
					placement: TickPlacement::DEFAULT,
				},
				81.0,
				54.0,
			),
			(
				"negative-crossing linear",
				AxisBandRequirement::Formatted {
					domain: (-50.0, 50.0),
					axis_type: AxisType::Linear,
					placement: TickPlacement::DEFAULT,
				},
				94.5,
				67.5,
			),
			(
				"scientific linear",
				AxisBandRequirement::Formatted {
					domain: (0.0, 5_000_000.0),
					axis_type: AxisType::Linear,
					placement: TickPlacement::DEFAULT,
				},
				108.0,
				81.0,
			),
			(
				"step linear",
				AxisBandRequirement::Formatted {
					domain: (0.0, 100.0),
					axis_type: AxisType::Linear,
					placement: TickPlacement::Step(PositiveFinite(25.0)),
				},
				81.0,
				40.5,
			),
			(
				"percent",
				AxisBandRequirement::Formatted {
					domain: (0.0, 1.0),
					axis_type: AxisType::Percent,
					placement: TickPlacement::DEFAULT,
				},
				94.5,
				54.0,
			),
			(
				"categorical y",
				AxisBandRequirement::Categories(vec!["YYYYYYYYYY".to_owned()]),
				108.0,
				108.0,
			),
			(
				"step capacity refusal",
				AxisBandRequirement::Formatted {
					domain: (0.0, 100.0),
					axis_type: AxisType::Linear,
					placement: TickPlacement::Step(PositiveFinite(0.000_001)),
				},
				81.0,
				0.0,
			),
			(
				"step valid empty",
				AxisBandRequirement::Formatted {
					domain: (1.0, 24.0),
					axis_type: AxisType::Linear,
					placement: TickPlacement::Step(PositiveFinite(25.0)),
				},
				27.0,
				0.0,
			),
		] {
			assert_product_0127_matrix_entry(
				name,
				requirement,
				legacy_width,
				selected_width,
				&settings,
			);
		}

		let layouts = [
			PlotLayout::CategoricalX {
				categories: vec!["A".to_owned()],
				y_range: (0.0, 100.0),
				y_axis_type: AxisType::Linear,
			},
			PlotLayout::CategoricalX {
				categories: vec!["B".to_owned()],
				y_range: (0.0, 5_000_000.0),
				y_axis_type: AxisType::Linear,
			},
		];
		let union = BandRequirements::across_layouts(layouts.iter(), &settings);
		assert_product_0127_matrix_entry(
			"faceted union",
			union.y[1].clone(),
			108.0,
			81.0,
			&settings,
		);
		assert_eq!(
			union.estimated_y_width(settings.y_tick_size.pixels()),
			81.0,
			"the grid requirement takes the maximum selected cell label once"
		);
	}

	#[test]
	fn product_0114_grid_cells_do_not_apply_a_second_shared_edge_inset() {
		let settings = ResolvedPlotSettings::default();
		let cell = Rect::<Widget>::from_iced_frame(Rectangle::new(
			Point::new(200.0, 100.0),
			iced::Size::new(300.0, 200.0),
		));
		let bands = VerticalGuideBands::resolve_with_requirements(
			"",
			Some("facet"),
			&BandRequirements {
				x: vec![AxisBandRequirement::Categories(vec!["long x".to_owned()])],
				y: vec![AxisBandRequirement::Categories(vec!["long y".to_owned()])],
			},
			&settings,
		);
		let local = bands.grid_cell_plot_area(cell, &settings);
		assert_eq!(
			local.x(),
			cell.x(),
			"the shared left band is not repeated per cell"
		);
		assert_eq!(
			local.bottom(),
			cell.bottom(),
			"the shared bottom band is not repeated per cell"
		);
	}

	#[test]
	fn product_0106_zero_title_and_facet_cases_preserve_the_previous_plot_area() {
		let settings = ResolvedPlotSettings::default();
		let area =
			VerticalGuideBands::resolve("", None, &settings).plot_area(bounds(), 6.0, &settings);
		assert_eq!(area.x(), 6.0 + settings.plot_padding_left);
		assert_eq!(area.y(), 6.0 + settings.plot_padding_top);
		assert_eq!(
			area.width(),
			600.0 - 12.0 - settings.plot_padding_left - settings.plot_padding_right
		);
		assert_eq!(
			area.height(),
			400.0 - 12.0 - settings.plot_padding_top - settings.plot_padding_bottom
		);
	}

	#[test]
	fn product_0120_padding_edges_floor_guides_and_move_only_their_boundaries() {
		let requirements = BandRequirements {
			x: vec![AxisBandRequirement::Categories(vec![
				"a deliberately long x label".to_owned(),
			])],
			y: vec![AxisBandRequirement::Categories(vec![
				"a deliberately long y label".to_owned(),
			])],
		};
		let pixels = |value| PlotPaddingPixels::new(value).unwrap();
		let base = ResolvedPlotSettings::default();
		let derived = VerticalGuideBands::resolve_with_requirements_and_x_label(
			"",
			None,
			&requirements,
			false,
			&base,
		);

		let low_floor = ResolvedPlotSettings {
			authored_plot_padding_left: Some(pixels(1.0)),
			authored_plot_padding_bottom: Some(pixels(1.0)),
			..base.clone()
		};
		let low = VerticalGuideBands::resolve_with_requirements_and_x_label(
			"",
			None,
			&requirements,
			false,
			&low_floor,
		);
		assert_eq!(low.left, derived.left);
		assert_eq!(low.bottom, derived.bottom);

		let high_floor = ResolvedPlotSettings {
			authored_plot_padding_left: Some(pixels(300.0)),
			authored_plot_padding_bottom: Some(pixels(300.0)),
			..base.clone()
		};
		let high = VerticalGuideBands::resolve_with_requirements_and_x_label(
			"",
			None,
			&requirements,
			false,
			&high_floor,
		);
		assert_eq!(high.left, 300.0);
		assert_eq!(high.bottom, 300.0);

		let no_padding = derived.plot_area(bounds(), 0.0, &base);
		let right_padding = ResolvedPlotSettings {
			authored_plot_padding_right: Some(pixels(120.0)),
			..base.clone()
		};
		let padded = derived.plot_area(bounds(), 0.0, &right_padding);
		assert_eq!(no_padding.right() - padded.right(), 100.0);
		assert_eq!(no_padding.x(), padded.x());
		assert_eq!(no_padding.y(), padded.y());
		assert_eq!(no_padding.bottom(), padded.bottom());
		let tiled_without = derived.grid_tiled_area(bounds(), &base);
		let tiled_with = derived.grid_tiled_area(bounds(), &right_padding);
		assert_eq!(
			tiled_without.width() - tiled_with.width(),
			120.0,
			"an authored grid edge is reserved once before its cells divide"
		);

		let zero = ResolvedPlotSettings {
			authored_plot_padding_right: Some(PlotPaddingPixels::ZERO),
			..base.clone()
		};
		assert_eq!(derived.plot_area(bounds(), 0.0, &zero), no_padding);
		assert_eq!(derived.grid_tiled_area(bounds(), &zero), tiled_without);

		let oversized = ResolvedPlotSettings {
			authored_plot_padding_right: Some(pixels(100_000.0)),
			..base
		};
		assert_eq!(derived.plot_area(bounds(), 0.0, &oversized).width(), 0.0);
		assert_eq!(
			oversized.authored_plot_padding_right,
			Some(pixels(100_000.0)),
			"the safety clamp never replaces the authored request"
		);
	}

	#[test]
	fn product_0106_title_subtitle_and_facet_reservations_are_resolved_once() {
		let mut title = ResolvedPlotSettings {
			title: ResolvedPresence::Show(Arc::new("Title".to_owned())),
			subtitle: ResolvedPresence::Hide,
			..ResolvedPlotSettings::default()
		};
		let title_bands = VerticalGuideBands::resolve("", None, &title);
		assert_eq!(
			title_bands.heading_height(),
			title.title_offset + title.title_size.pixels()
		);

		title.subtitle = ResolvedPresence::Show(Arc::new("Subtitle".to_owned()));
		let subtitle_bands = VerticalGuideBands::resolve("", None, &title);
		assert_eq!(
			subtitle_bands.heading_height(),
			title.subtitle_offset + title.subtitle_size.pixels()
		);
		assert_ne!(
			title_bands.heading_height(),
			subtitle_bands.heading_height()
		);

		let faceted = VerticalGuideBands::resolve("", Some("East"), &title);
		let plain = subtitle_bands.plot_area(bounds(), 0.0, &title);
		let facet = faceted.plot_area(bounds(), 0.0, &title);
		assert_eq!(
			facet.y() - plain.y(),
			VerticalGuideBands::FACET_LABEL_TOP_INSET
				+ VerticalGuideBands::facet_label_size(&title).pixels()
		);
		assert_eq!(plain.bottom(), facet.bottom());
	}

	#[test]
	fn product_0124_facet_label_reservation_tracks_its_own_typed_size() {
		let base = ResolvedPlotSettings::default();
		let enlarged = ResolvedPlotSettings {
			facet_label_size: FacetLabelTextSize::new(40.0).unwrap(),
			..base.clone()
		};
		let base_area =
			VerticalGuideBands::resolve("", Some("East"), &base).plot_area(bounds(), 0.0, &base);
		let enlarged_area = VerticalGuideBands::resolve("", Some("East"), &enlarged).plot_area(
			bounds(),
			0.0,
			&enlarged,
		);
		assert_eq!(
			enlarged_area.y() - base_area.y(),
			enlarged.facet_label_size.pixels() - base.facet_label_size.pixels(),
			"the facet row reservation follows the resolved facet-label value exactly"
		);
		assert_eq!(base.legend_size, enlarged.legend_size);
	}

	#[test]
	fn product_0106_reserved_bounds_keep_the_data_mapping_proportional() {
		let settings = ResolvedPlotSettings {
			title: ResolvedPresence::Show(Arc::new("Title".to_owned())),
			subtitle: ResolvedPresence::Show(Arc::new("Subtitle".to_owned())),
			..ResolvedPlotSettings::default()
		};
		let area = VerticalGuideBands::resolve("", Some("East"), &settings).plot_area(
			bounds(),
			0.0,
			&settings,
		);
		let layout = PlotLayout::Cartesian {
			x_range: (0.0, 100.0),
			y_range: (0.0, 200.0),
			x_axis_type: AxisType::Linear,
			y_axis_type: AxisType::Linear,
		};
		let transform = CoordinateTransformer::new(&layout, area.into_iced());
		let quarter = transform.cartesian(25.0, 50.0);
		assert_eq!(quarter.x / area.width(), 0.25);
		assert_eq!(quarter.y / area.height(), 0.75);
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
				let expected_y = VerticalGuideBands::resolve("fallback title", None, &settings)
					.plot_top(&settings);
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
		Text(String),
		ClipStart(Rectangle),
		ClipEnd,
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

		fn fill_text_raw(&mut self, text: Text) {
			self.operations.push(Operation::Text(text.content));
		}

		fn translate(&mut self, _translation: iced::Vector) {}

		fn rotate(&mut self, _angle: f32) {}

		fn with_save(&mut self, f: &mut dyn FnMut(&mut dyn PlotBackend)) {
			f(self);
		}

		fn with_clip(&mut self, bounds: Rectangle, f: &mut dyn FnMut(&mut dyn PlotBackend)) {
			self.operations.push(Operation::ClipStart(bounds));
			f(self);
			self.operations.push(Operation::ClipEnd);
		}
	}

	struct ClippedOrderKernel;

	impl PlotKernel for ClippedOrderKernel {
		fn layout(&self, _settings: ResolvedPlotSettings) -> PlotLayout {
			PlotLayout::Cartesian {
				x_range: (0.0, 1.0),
				y_range: (0.0, 1.0),
				x_axis_type: AxisType::Linear,
				y_axis_type: AxisType::Linear,
			}
		}

		fn requires_plot_area_clip(&self) -> bool {
			true
		}

		fn plot(
			&self,
			backend: &mut dyn PlotBackend,
			_bounds: Rectangle,
			_transform: &CoordinateTransformer,
			_cursor: Cursor,
			_settings: ResolvedPlotSettings,
		) {
			backend.fill_rectangle(Point::new(20.0, 20.0), iced::Size::new(40.0, 40.0), MARK);
		}

		fn hover(&self, _transform: &CoordinateTransformer, _cursor: Cursor) -> Option<String> {
			None
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

	#[test]
	fn product_0112_hover_size_leaves_the_canvas_draw_list_unchanged() {
		let default = settings();
		let mut enlarged = default.clone();
		enlarged.hover_size = HoverTextSize::new(72.0).expect("in-range hover size");

		assert_eq!(
			render(layouts()[0].clone(), default, true),
			render(layouts()[0].clone(), enlarged, true),
			"hover is an iced overlay, so its size cannot alter canvas operations"
		);
	}

	#[test]
	fn radial_layouts_never_attempt_to_draw_cartesian_axis_titles() {
		let mut radial_settings = settings();
		radial_settings.x_label = ResolvedPresence::Show(Arc::new("stage".to_owned()));
		radial_settings.y_label = ResolvedPresence::Show(Arc::new("value".to_owned()));

		let operations = render(PlotLayout::Radial, radial_settings, true);
		assert!(
			!operations
				.iter()
				.any(|operation| matches!(operation, Operation::Text(_))),
			"a radial layout has no shared Cartesian axis placement"
		);
	}

	#[test]
	fn product_0127_selected_requirement_matches_the_emitted_tick_text() {
		let mut settings = settings();
		settings.y_tick_placement = TickPlacement::DEFAULT;
		let layout = PlotLayout::CategoricalX {
			categories: vec!["A".to_owned(), "B".to_owned()],
			y_range: (0.0, 100.0),
			y_axis_type: AxisType::Linear,
		};
		let requirements = BandRequirements::from_layout(&layout, &settings);
		let AxisBandRequirement::Formatted {
			domain,
			axis_type,
			placement,
		} = requirements
			.y
			.as_slice()
			.first()
			.expect("numeric y requirement")
		else {
			panic!("categorical x owns one formatted y requirement")
		};
		let selected = placement
			.positions(*domain)
			.expect("default count selection")
			.into_iter()
			.map(|value| format_label(value, *axis_type))
			.collect::<Vec<_>>();
		let emitted = render(layout, settings, false)
			.into_iter()
			.filter_map(|operation| match operation {
				Operation::Text(text) if text.parse::<f64>().is_ok() => Some(text),
				_ => None,
			})
			.collect::<Vec<_>>();

		assert_eq!(
			emitted, selected,
			"the requirement measures exactly the pre-layout values the draw path emits"
		);
	}

	#[test]
	fn product_0127_empty_selections_change_only_the_requirement() {
		for (y_range, placement, message) in [
			(
				(1.0, 24.0),
				TickPlacement::step(25.0).expect("valid empty step"),
				"valid empty selection",
			),
			(
				(0.0, 100.0),
				TickPlacement::step(0.000_001).expect("over-capacity step"),
				"refused selection",
			),
		] {
			let mut settings = settings();
			settings.y_tick_placement = placement;
			let layout = PlotLayout::CategoricalX {
				categories: vec!["A".to_owned(), "B".to_owned()],
				y_range,
				y_axis_type: AxisType::Linear,
			};
			let requirements = BandRequirements::from_layout(&layout, &settings);
			assert_eq!(
				requirements.estimated_y_width(settings.y_tick_size.pixels()),
				0.0,
				"{message} reserves no tick-label width"
			);
			assert!(
				!render(layout, settings, false)
					.into_iter()
					.any(|operation| {
						matches!(operation, Operation::Text(text) if text.parse::<f64>().is_ok())
					}),
				"{message} already emitted no numeric tick labels; PRODUCT-0127 changes only its band requirement"
			);
		}
	}

	fn product_0127_y_tick_trace(
		domain: (f64, f64),
		axis_type: AxisType,
		placement: TickPlacement,
	) -> Vec<String> {
		let mut settings = settings();
		settings.y_tick_placement = placement;
		render(
			PlotLayout::CategoricalX {
				categories: vec!["PRODUCT-0127 category sentinel".to_owned()],
				y_range: domain,
				y_axis_type: axis_type,
			},
			settings,
			false,
		)
		.into_iter()
		.filter_map(|operation| match operation {
			Operation::Text(text) if text != "PRODUCT-0127 category sentinel" => Some(text),
			_ => None,
		})
		.collect()
	}

	#[test]
	fn product_0127_banks_each_temporal_variant_by_placement_policy() {
		for (name, domain, axis_type, selected_width, expected_trace) in [
			(
				"Date",
				(20_000.0, 20_010.0),
				AxisType::Date,
				135.0,
				&[
					"2024-10-04",
					"2024-10-05",
					"2024-10-06",
					"2024-10-07",
					"2024-10-09",
					"2024-10-10",
					"2024-10-11",
					"2024-10-12",
					"2024-10-14",
				][..],
			),
			(
				"negative-year Date",
				(-999_999.0, 20_000.0),
				AxisType::Date,
				148.5,
				&[
					"-0768-02-05",
					"-0419-03-07",
					"-0070-04-07",
					"0279-05-07",
					"0628-06-06",
					"0977-07-06",
					"1326-08-06",
					"1675-09-05",
					"2024-10-04",
				][..],
			),
			(
				"millisecond DateTime",
				(1_700_000_000_000.0, 1_700_000_010_000.0),
				AxisType::Datetime(TimeUnit::Milliseconds),
				256.5,
				&[
					"2023-11-14 22:13:20",
					"2023-11-14 22:13:21",
					"2023-11-14 22:13:22",
					"2023-11-14 22:13:23",
					"2023-11-14 22:13:25",
					"2023-11-14 22:13:26",
					"2023-11-14 22:13:27",
					"2023-11-14 22:13:28",
					"2023-11-14 22:13:30",
				][..],
			),
			(
				"epoch millisecond DateTime",
				(0.0, 10_000.0),
				AxisType::Datetime(TimeUnit::Milliseconds),
				256.5,
				&[
					"1970-01-01 00:00:00",
					"1970-01-01 00:00:01",
					"1970-01-01 00:00:02",
					"1970-01-01 00:00:03",
					"1970-01-01 00:00:05",
					"1970-01-01 00:00:06",
					"1970-01-01 00:00:07",
					"1970-01-01 00:00:08",
					"1970-01-01 00:00:10",
				][..],
			),
			(
				"microsecond DateTime",
				(0.0, 10_000_000.0),
				AxisType::Datetime(TimeUnit::Microseconds),
				256.5,
				&[
					"1970-01-01 00:00:00",
					"1970-01-01 00:00:01",
					"1970-01-01 00:00:02",
					"1970-01-01 00:00:03",
					"1970-01-01 00:00:05",
					"1970-01-01 00:00:06",
					"1970-01-01 00:00:07",
					"1970-01-01 00:00:08",
					"1970-01-01 00:00:10",
				][..],
			),
			(
				"nanosecond DateTime",
				(0.0, 10_000_000_000.0),
				AxisType::Datetime(TimeUnit::Nanoseconds),
				256.5,
				&[
					"1970-01-01 00:00:00",
					"1970-01-01 00:00:01",
					"1970-01-01 00:00:02",
					"1970-01-01 00:00:03",
					"1970-01-01 00:00:05",
					"1970-01-01 00:00:06",
					"1970-01-01 00:00:07",
					"1970-01-01 00:00:08",
					"1970-01-01 00:00:10",
				][..],
			),
			(
				"Time",
				(0.0, 1_000_000_000.0),
				AxisType::Time,
				162.0,
				&[
					"00:00:00.000",
					"00:00:00.125",
					"00:00:00.250",
					"00:00:00.375",
					"00:00:00.500",
					"00:00:00.625",
					"00:00:00.750",
					"00:00:00.875",
					"00:00:01.000",
				][..],
			),
		] {
			let requirement = AxisBandRequirement::Formatted {
				domain,
				axis_type,
				placement: TickPlacement::DEFAULT,
			};
			assert_eq!(
				requirement.estimated_width(18.0),
				selected_width,
				"{name} preserves its separately banked Count width"
			);
			assert_eq!(
				product_0127_y_tick_trace(domain, axis_type, TickPlacement::DEFAULT),
				expected_trace
					.iter()
					.map(|label| (*label).to_owned())
					.collect::<Vec<_>>(),
				"{name} preserves its separately banked Count operation trace"
			);
		}

		for (name, domain, axis_type, placement, previous_width, selected_width, expected_trace) in [
			(
				"wide Date Step",
				(-900_000.0, 900_000.0),
				AxisType::Date,
				TickPlacement::Step(PositiveFinite(700_000.0)),
				148.5,
				135.0,
				&["0053-06-19", "1970-01-01", "3886-07-15"][..],
			),
			(
				"past-chrono millisecond DateTime Step",
				(0.0, 1_000_000_000_000_000_000.0),
				AxisType::Datetime(TimeUnit::Milliseconds),
				TickPlacement::Step(PositiveFinite(300_000_000_000_000_000.0)),
				283.5,
				270.0,
				&[
					"1970-01-01 00:00:00",
					"300000000000000000.0",
					"600000000000000000.0",
					"900000000000000000.0",
				][..],
			),
		] {
			let requirement = AxisBandRequirement::Formatted {
				domain,
				axis_type,
				placement,
			};
			assert_eq!(
				requirement.estimated_width(18.0),
				selected_width,
				"{name} selected width"
			);
			assert_eq!(
				selected_width - previous_width,
				-13.5,
				"{name} preserves its separately banked Step delta"
			);
			assert_eq!(
				product_0127_y_tick_trace(domain, axis_type, placement),
				expected_trace
					.iter()
					.map(|label| (*label).to_owned())
					.collect::<Vec<_>>(),
				"{name} preserves its separately banked Step operation trace"
			);
		}

		assert_eq!(
			render(
				PlotLayout::CategoricalXY {
					x_categories: vec!["Quarter".to_owned()],
					y_categories: vec!["North".to_owned(), "South".to_owned()],
				},
				settings(),
				false,
			)
			.into_iter()
			.filter_map(|operation| match operation {
				Operation::Text(text) => Some(text),
				_ => None,
			})
			.collect::<Vec<_>>(),
			["Quarter", "North", "South"],
			"categorical labels retain their exact operation trace without a formatted requirement"
		);
	}

	#[test]
	fn product_0113_heading_band_is_the_maximum_of_typed_title_and_subtitle_contributions() {
		let title = |pixels| TitleTextSize::new(pixels).unwrap();
		let subtitle = |pixels| SubtitleTextSize::new(pixels).unwrap();
		let mut settings = ResolvedPlotSettings::default();

		settings.title = ResolvedPresence::Show(Arc::new("title".to_owned()));
		settings.subtitle = ResolvedPresence::Hide;
		settings.title_size = title(48.0);
		assert_eq!(
			VerticalGuideBands::resolve("", None, &settings).heading_height(),
			settings.title_offset + title(48.0).pixels()
		);

		settings.title = ResolvedPresence::Hide;
		settings.subtitle = ResolvedPresence::Show(Arc::new("subtitle".to_owned()));
		settings.subtitle_size = subtitle(36.0);
		assert_eq!(
			VerticalGuideBands::resolve("", None, &settings).heading_height(),
			settings.subtitle_offset + subtitle(36.0).pixels()
		);

		settings.title = ResolvedPresence::Show(Arc::new("title".to_owned()));
		settings.title_size = title(96.0);
		assert_eq!(
			VerticalGuideBands::resolve("", None, &settings).heading_height(),
			(settings.title_offset + title(96.0).pixels())
				.max(settings.subtitle_offset + subtitle(36.0).pixels())
		);

		settings.title_size = title(36.0);
		settings.subtitle_size = subtitle(96.0);
		assert_eq!(
			VerticalGuideBands::resolve("", None, &settings).heading_height(),
			settings.subtitle_offset + subtitle(96.0).pixels(),
			"a visible subtitle must win when its contribution exceeds the visible title"
		);
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
		let layout = kernel.layout(settings.clone());
		let requirements = BandRequirements::from_layout(&layout, &settings);
		let plot_area = widget
			.plot_area(Rect::from_iced_frame(bounds), &requirements)
			.into_iced();
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
				y_axis_type: AxisType::Linear,
			},
			PlotLayout::CategoricalY {
				categories: vec!["A".to_owned(), "B".to_owned()],
				x_range: (0.0, 1.0),
				x_axis_type: AxisType::Linear,
			},
		]
	}

	#[test]
	fn product_0079_plot_area_clip_keeps_background_below_grid_and_marks() {
		let settings = settings();
		let model = PlotSettings::from_resolved(settings.clone());
		let widget = PlotWidget {
			kernel: &ClippedOrderKernel,
			title: String::new(),
			padding: 0.0,
			settings,
			settings_model: model,
			render_revision: 0,
			resize_render_suspended: false,
			layer: PlotRenderLayer::Full,
		};
		let bounds = Rectangle::new(Point::ORIGIN, iced::Size::new(200.0, 120.0));
		let mut backend = OperationBackend::default();
		widget.render_plot_layer(&mut backend, bounds);

		assert_eq!(
			backend
				.operations
				.iter()
				.filter(|operation| matches!(operation, Operation::ClipStart(_)))
				.count(),
			2,
			"background and plot content are separate clipped children"
		);
		let background = backend
			.operations
			.iter()
			.position(|operation| {
				*operation
					== Operation::Fill(
						ResolvedPlotSettings::default()
							.chrome
							.canvas_background
							.iced(),
					)
			})
			.expect("background fill");
		let mark = backend
			.operations
			.iter()
			.position(|operation| *operation == Operation::Fill(MARK))
			.expect("mark fill");
		let first_clip_end = backend
			.operations
			.iter()
			.position(|operation| *operation == Operation::ClipEnd)
			.expect("background clip closes");
		let plot_clip_start = backend
			.operations
			.iter()
			.enumerate()
			.skip(first_clip_end + 1)
			.find_map(|(index, operation)| {
				matches!(operation, Operation::ClipStart(_)).then_some(index)
			})
			.expect("plot clip opens");
		assert!(background < first_clip_end);
		assert!(first_clip_end < plot_clip_start && plot_clip_start < mark);
		assert!(
			backend.operations[plot_clip_start..mark]
				.iter()
				.any(|operation| matches!(operation, Operation::Stroke(_))),
			"gridlines and marks share the plot clip, in that order"
		);
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
	fn product_0072_percent_axis_formats_percentage_points_and_admits_steps() {
		assert_eq!(format_label(0.0, AxisType::Percent), "0%");
		assert_eq!(format_label(12.5, AxisType::Percent), "12.5%");
		assert_eq!(format_label(100.0, AxisType::Percent), "100%");
		assert_eq!(
			TickPlacement::step(25.0)
				.unwrap()
				.positions_for_axis(AxisType::Percent, (0.0, 100.0))
				.unwrap(),
			[0.0, 25.0, 50.0, 75.0, 100.0]
		);

		let mut settings = settings();
		settings.y_tick_placement = TickPlacement::step(25.0).unwrap();
		let percent = render(
			PlotLayout::CategoricalX {
				categories: vec!["A".to_owned(), "B".to_owned()],
				y_range: (0.0, 100.0),
				y_axis_type: AxisType::Percent,
			},
			settings.clone(),
			false,
		);
		let percent_labels = percent
			.iter()
			.filter_map(|operation| match operation {
				Operation::Text(text) if text.ends_with('%') => Some(text.as_str()),
				_ => None,
			})
			.collect::<Vec<_>>();
		assert_eq!(percent_labels, ["0%", "25%", "50%", "75%", "100%"]);

		let absolute = render(
			PlotLayout::CategoricalX {
				categories: vec!["A".to_owned(), "B".to_owned()],
				y_range: (0.0, 100.0),
				y_axis_type: AxisType::Linear,
			},
			settings,
			false,
		);
		assert!(
			absolute.iter().all(|operation| !matches!(
				operation,
				Operation::Text(text) if text.ends_with('%')
			)),
			"the same step on an absolute layout keeps linear labels"
		);
	}

	#[test]
	fn product_0085_axis_title_derivation_is_semantic_and_presence_still_wins() {
		let derived = derive_axis_title("revenue", AxisTitleTransform::PercentOfTotal);
		assert_eq!(derived, "revenue (% of total)");
		assert_eq!(
			resolve_canvas_text(&ResolvedPresence::Fallback, Some(derived.clone())),
			Some(derived)
		);
		assert_eq!(
			resolve_canvas_text(
				&ResolvedPresence::Show(Arc::new(" Share ".to_owned())),
				Some("revenue (% of total)".to_owned()),
			),
			Some(" Share ".to_owned()),
			"selected cascade text replaces derivation byte-for-byte"
		);
		assert_eq!(
			resolve_canvas_text(
				&ResolvedPresence::Hide,
				Some("revenue (% of total)".to_owned()),
			),
			None,
			"resolved Hide remains stronger than a derived fallback"
		);
		assert_eq!(
			derive_axis_title("", AxisTitleTransform::PercentOfTotal),
			""
		);

		for (kernel_x, kernel_y) in [("category", "value"), ("value", "category")] {
			assert_eq!(
				resolve_canvas_text(
					&ResolvedPresence::Show(Arc::new("A".to_owned())),
					Some(kernel_x.to_owned()),
				),
				Some("A".to_owned())
			);
			assert_eq!(
				resolve_canvas_text(
					&ResolvedPresence::Show(Arc::new("B".to_owned())),
					Some(kernel_y.to_owned()),
				),
				Some("B".to_owned()),
				"authored x/y titles stay on rendered axes when kernel fallbacks swap"
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
				x_axis_type: AxisType::Linear,
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

		fn fill_text_raw(&mut self, mut text: Text) {
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

	#[test]
	fn product_0113_typed_title_and_subtitle_sizes_reach_canvas_text() {
		let kernel = FrameKernel;
		let default_settings = ResolvedPlotSettings::default();
		assert_eq!(default_settings.title_size, TitleTextSize::DEFAULT);
		assert_eq!(default_settings.subtitle_size, SubtitleTextSize::DEFAULT);
		let mut settings = ResolvedPlotSettings::default();
		settings.title = ResolvedPresence::Show(Arc::new("Title".to_owned()));
		settings.subtitle = ResolvedPresence::Show(Arc::new("Subtitle".to_owned()));
		settings.title_size = TitleTextSize::new(48.0).unwrap();
		settings.subtitle_size = SubtitleTextSize::new(36.0).unwrap();
		let settings_model = PlotSettings::from_resolved(settings.clone());
		let widget = PlotWidget {
			kernel: &kernel,
			title: String::new(),
			padding: 0.0,
			settings,
			settings_model,
			render_revision: 0,
			resize_render_suspended: false,
			layer: PlotRenderLayer::Full,
		};
		let mut backend = RecordingBackend::default();
		widget.render(
			&mut backend,
			Rectangle::new(Point::ORIGIN, Size::new(400.0, 300.0)),
		);
		let title = backend
			.texts
			.iter()
			.find(|text| text.content == "Title")
			.expect("title text");
		let subtitle = backend
			.texts
			.iter()
			.find(|text| text.content == "Subtitle")
			.expect("subtitle text");
		assert_eq!(title.size, iced::Pixels(48.0));
		assert_eq!(subtitle.size, iced::Pixels(36.0));
	}

	#[test]
	fn product_0113_parallel_header_uses_x_label_size_for_its_size_and_clearance() {
		let kernel = FrameKernel;
		let layout = PlotLayout::Parallel {
			dimensions: vec!["dimension".to_owned()],
			ranges: vec![(0.0, 1.0)],
		};
		let bounds = Rectangle::new(Point::ORIGIN, Size::new(240.0, 160.0));
		let header = |size| {
			let mut settings = ResolvedPlotSettings::default();
			settings.x_label_size = size;
			let settings_model = PlotSettings::from_resolved(settings.clone());
			let widget = PlotWidget {
				kernel: &kernel,
				title: String::new(),
				padding: 0.0,
				settings,
				settings_model,
				render_revision: 0,
				resize_render_suspended: false,
				layer: PlotRenderLayer::Full,
			};
			let transform = CoordinateTransformer::new(&layout, bounds);
			let mut backend = RecordingBackend::default();
			widget.draw_parallel_axes(
				&mut backend,
				bounds,
				&transform,
				&["dimension".to_owned()],
				&[(0.0, 1.0)],
				AxisTextVisibility::ALL,
			);
			backend
				.texts
				.into_iter()
				.find(|text| text.content == "dimension")
				.expect("parallel dimension header")
		};

		let default = header(XLabelTextSize::new(20.0).unwrap());
		let enlarged = header(XLabelTextSize::new(24.0).unwrap());
		assert_eq!(default.size, iced::Pixels(20.0));
		assert_eq!(enlarged.size, iced::Pixels(24.0));
		assert_eq!(default.position.y - enlarged.position.y, 4.0);

		let mut titled = ResolvedPlotSettings::default();
		titled.title = ResolvedPresence::Show(Arc::new("Title".to_owned()));
		titled.title_size = TitleTextSize::new(48.0).unwrap();
		let bands = VerticalGuideBands::resolve("", None, &titled);
		let plot_top = bands.plot_top(&titled);
		let header_top = plot_top - 2.0 * enlarged.size.0;
		assert!(
			header_top >= bands.heading_height(),
			"the enlarged header clears the title band instead of reclaiming it"
		);
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
					x_axis_type: AxisType::Linear,
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
						x_axis_type: AxisType::Linear,
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
