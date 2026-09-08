#![allow(dead_code, unused_imports)]

// This test target mirrors the binary crate root so its probes traverse the
// actual private production renderer seams without widening Polariton's API.
#[path = "../src/adapters/mod.rs"]
mod adapters;
#[path = "../src/configuration.rs"]
mod configuration;
#[path = "../src/coordinate.rs"]
mod coordinate;
#[path = "../src/core/mod.rs"]
mod core;
#[path = "../src/driver.rs"]
mod driver;
#[path = "../src/editor.rs"]
mod editor;
#[path = "../src/fonts/mod.rs"]
mod fonts;
#[path = "../src/gui/mod.rs"]
mod gui;
#[cfg(unix)]
#[path = "../src/live/mod.rs"]
mod live;
#[cfg(feature = "toron")]
#[path = "../src/local_workspace.rs"]
mod local_workspace;
#[path = "../src/plot/mod.rs"]
mod plot;
#[path = "../src/presentation_registry.rs"]
mod presentation_registry;
#[path = "../src/profile_store.rs"]
mod profile_store;
#[path = "../src/publication.rs"]
mod publication;
#[path = "../src/result_artifact.rs"]
mod result_artifact;
#[path = "../src/result_registry.rs"]
mod result_registry;
#[path = "../src/rune_host.rs"]
mod rune_host;
#[cfg(feature = "toron")]
#[path = "../src/rune_presentation.rs"]
mod rune_presentation;
#[cfg(feature = "toron")]
#[path = "../src/rune_relation_envelope.rs"]
mod rune_relation_envelope;
#[path = "../src/rune_view.rs"]
mod rune_view;
#[cfg(feature = "toron")]
#[path = "../src/rune_view_compiler.rs"]
mod rune_view_compiler;
#[path = "../src/selection.rs"]
mod selection;
#[cfg(feature = "toron")]
#[path = "../src/session_persistence.rs"]
mod session_persistence;
#[cfg(feature = "toron")]
#[path = "../src/settings_writeback.rs"]
mod settings_writeback;

#[path = "../src/first_run.rs"]
mod first_run;
#[path = "../src/reporting/mod.rs"]
mod reporting;

use coordinate::Rect;
use gui::table::{Product0057TableEvidenceGeometry, draw_product_0057_table_evidence};
use iced::advanced::graphics::geometry::Renderer as _;
use iced::advanced::renderer::{Headless as _, Renderer as _};
use iced::widget::canvas::{self, Path, Stroke, Text};
use iced::{Color, Font, Pixels, Point, Rectangle, Size, Vector};
use iced_wgpu::graphics::{Antialiasing, Shell};
use iced_wgpu::{Engine, Renderer as WgpuRenderer, wgpu};
use plot::common::{
	AxisType, BandRequirements, CoordinateTransformer, IcedBackend, Orientation, PathBuilder,
	PlotBackend, PlotKernel, PlotLayout, PlotRenderLayer, PlotWidget, ResolvedPlotSettings,
	TemporalAxisDomain, TickPlacement, TimeUnit, VerticalGuideBands,
};
use plot::composition::PlotComposition;
use plot::core::PlotType;
use plot::grid::{GridArrangement, PlotGrid, PlotGridKernel};
use plot::kernels::bar::{BarPlotKernel, BarPreparedData};
use plot::kernels::boxplot::{BoxPlotKernel, BoxPlotPreparedData};
use plot::kernels::boxplot_tukey::{TukeyBoxRow, TukeyBoxes};
use plot::kernels::histogram::{HistogramPlotKernel, HistogramPreparedData};
use plot::kernels::histogram_bins::{HistogramBin, HistogramBins};
use plot::kernels::line::{LinePlotKernel, LinePreparedData, SeriesData};
use plot::kernels::scatter::{ScatterPlotKernel, ScatterPreparedData, ScatterSeries};
use plot::kernels::stacked_area::{StackedAreaPlotKernel, StackedAreaPreparedData};
use plot::kernels::stacked_bar::{StackedBarPlotKernel, StackedBarPreparedData};
use plot::legend::LegendProvenance;
use plot::request::{PlotRequest, PlotSource, plan};
use plot::spec::{
	AuthoredPlotSpec, BarWidth, CategoryColorAssignment, CategoryGap, LayerSettings,
	PalettePosition, PlotProvenance, PointSize, ResolvedColor, ResolvedPresence,
	StackNormalization, StampedResult, StrokeWidth, TickRotation,
};
use plot::theme::PlotSettings;
use polars::prelude::{Float64Chunked, NewChunkedArray};
use std::borrow::Cow;
use std::sync::Arc;

const BACKGROUND: [u8; 3] = [0, 0, 0];
const RED: [u8; 3] = [230, 32, 48];
const BLUE: [u8; 3] = [40, 96, 230];
const GREEN: [u8; 3] = [32, 220, 96];
const WHITE: [u8; 3] = [255, 255, 255];
const PRODUCT_0077_BANKED_VERTICAL_SPILL: usize = 88;
const PRODUCT_0077_BANKED_HORIZONTAL_SPILL: usize = 111;
const PRODUCT_0083_BANKED_MEDIAN_DETAIL: usize = 0;
/// PRODUCT-0132 Stage 0, banked 2026-08-18 on the designated adapter.
///
/// With the x-axis title hidden, the rotated labels now ink the reserved band
/// on the production-shaped two-geometry render. The 31 px tick-mark residual
/// remains the floor that a naive nonzero oracle would mistake for labels.
const PRODUCT_0132_TICK_MARK_RESIDUAL: usize = 31;
const PRODUCT_0132_BANKED_ROTATED_TICK_INK: (usize, usize) = (2831, 3086);
const PRODUCT_0132_BANKED_UNROTATED_TICK_INK: usize = 2096;
// PRODUCT-0131's pre-bound negative control. These values are retained as
// evidence even after Stage 1 makes every corresponding plot positive.
const PRODUCT_0131_PRE_BOUND_HEIGHTS: [[f32; 3]; 2] = [[34.7, 5.9, 0.0], [0.0, 0.0, 0.0]];

struct Gpu {
	adapter: wgpu::Adapter,
	info: wgpu::AdapterInfo,
}

impl Gpu {
	async fn acquire() -> Self {
		let instance = wgpu::Instance::new(&wgpu::InstanceDescriptor {
			backends: wgpu::Backends::from_env().unwrap_or(wgpu::Backends::PRIMARY),
			flags: wgpu::InstanceFlags::empty(),
			..wgpu::InstanceDescriptor::default()
		});
		let adapter = instance
			.request_adapter(&wgpu::RequestAdapterOptions {
				power_preference: wgpu::PowerPreference::HighPerformance,
				force_fallback_adapter: false,
				compatible_surface: None,
			})
			.await
			.expect("PRODUCT-0057: the designated render-evidence adapter must be present");
		let info = adapter.get_info();
		Self { adapter, info }
	}

	async fn renderer(&self, antialiasing: Option<Antialiasing>) -> iced::Renderer {
		iced::advanced::graphics::text::font_system()
			.write()
			.expect("PRODUCT-0117 global font system lock")
			.load_font(Cow::Borrowed(fonts::POLARITON_PLOT_SANS_REGULAR));
		let (device, queue) = self
			.adapter
			.request_device(&wgpu::DeviceDescriptor {
				label: Some("PRODUCT-0057 render evidence"),
				required_features: wgpu::Features::empty(),
				required_limits: wgpu::Limits {
					max_bind_groups: 2,
					..wgpu::Limits::default()
				},
				memory_hints: wgpu::MemoryHints::MemoryUsage,
				trace: wgpu::Trace::Off,
				experimental_features: wgpu::ExperimentalFeatures::disabled(),
			})
			.await
			.expect("PRODUCT-0057: create headless iced/wgpu device");
		let engine = Engine::new(
			&self.adapter,
			device,
			queue,
			if iced_wgpu::graphics::color::GAMMA_CORRECTION {
				wgpu::TextureFormat::Rgba8UnormSrgb
			} else {
				wgpu::TextureFormat::Rgba8Unorm
			},
			antialiasing,
			Shell::headless(),
		);
		iced::Renderer::Primary(WgpuRenderer::new(engine, Font::DEFAULT, Pixels(16.0)))
	}
}

#[derive(Clone)]
struct Capture {
	width: u32,
	height: u32,
	rgba: Vec<u8>,
}

impl Capture {
	fn pixel(&self, x: u32, y: u32) -> [u8; 3] {
		assert!(x < self.width && y < self.height);
		let offset = ((y * self.width + x) * 4) as usize;
		[
			self.rgba[offset],
			self.rgba[offset + 1],
			self.rgba[offset + 2],
		]
	}

	fn point(&self, x: f32, y: f32) -> [u8; 3] {
		self.pixel(
			(x * self.width as f32).floor() as u32,
			(y * self.height as f32).floor() as u32,
		)
	}

	fn region_fraction(&self, bounds: [f32; 4], expected: [u8; 3], tolerance: u8) -> f32 {
		let x0 = (bounds[0] * self.width as f32).floor() as u32;
		let y0 = (bounds[1] * self.height as f32).floor() as u32;
		let x1 = (bounds[2] * self.width as f32).ceil() as u32;
		let y1 = (bounds[3] * self.height as f32).ceil() as u32;
		let mut matches = 0usize;
		let mut total = 0usize;
		for y in y0.min(self.height)..y1.min(self.height) {
			for x in x0.min(self.width)..x1.min(self.width) {
				total += 1;
				matches += usize::from(near(self.pixel(x, y), expected, tolerance));
			}
		}
		matches as f32 / total.max(1) as f32
	}

	fn luminous_pixels(&self, bounds: Rectangle) -> usize {
		let x0 = bounds.x.max(0.0).floor() as u32;
		let y0 = bounds.y.max(0.0).floor() as u32;
		let x1 = (bounds.x + bounds.width).ceil().max(0.0) as u32;
		let y1 = (bounds.y + bounds.height).ceil().max(0.0) as u32;
		let mut count = 0usize;
		for y in y0.min(self.height)..y1.min(self.height) {
			for x in x0.min(self.width)..x1.min(self.width) {
				let pixel = self.pixel(x, y);
				count += usize::from(pixel.iter().copied().max().unwrap_or_default() > 120);
			}
		}
		count
	}

	fn intermediate_pixels(&self) -> usize {
		self.rgba
			.chunks_exact(4)
			.filter(|pixel| pixel[0] > 16 && pixel[0] < 239)
			.count()
	}
}

fn near(actual: [u8; 3], expected: [u8; 3], tolerance: u8) -> bool {
	actual
		.into_iter()
		.zip(expected)
		.all(|(actual, expected)| actual.abs_diff(expected) <= tolerance)
}

fn color_distance(left: [u8; 3], right: [u8; 3]) -> u16 {
	left.into_iter()
		.zip(right)
		.map(|(left, right)| left.abs_diff(right) as u16)
		.sum()
}

fn assert_region(capture: &Capture, bounds: [f32; 4], expected: [u8; 3], label: &str) {
	let fraction = capture.region_fraction(bounds, expected, 12);
	assert!(
		fraction >= 0.95,
		"PRODUCT-0057 {label}: expected at least 95% matching pixels, got {fraction:.3}"
	);
}

fn reset(renderer: &mut iced::Renderer, size: Size<u32>) {
	renderer.reset(Rectangle::with_size(Size::new(
		size.width as f32,
		size.height as f32,
	)));
}

fn screenshot(renderer: &mut iced::Renderer, size: Size<u32>) -> Capture {
	Capture {
		width: size.width,
		height: size.height,
		rgba: renderer.screenshot(size, 1.0, Color::BLACK),
	}
}

fn render_frame(
	renderer: &mut iced::Renderer,
	size: Size<u32>,
	draw: impl FnOnce(&mut canvas::Frame),
) -> Capture {
	reset(renderer, size);
	let mut frame = canvas::Frame::new(renderer, Size::new(size.width as f32, size.height as f32));
	draw(&mut frame);
	renderer.draw_geometry(frame.into_geometry());
	screenshot(renderer, size)
}

/// Render the same two geometry layers that `PlotWidget::draw` returns for
/// `PlotRenderLayer::Full`. Keeping the caches separate matters for iced's
/// frame-local mesh composition, especially for clipped rotated text.
fn render_plot_and_overlay(
	renderer: &mut iced::Renderer,
	size: Size<u32>,
	plot: impl FnOnce(&mut dyn PlotBackend),
	overlay: impl FnOnce(&mut dyn PlotBackend),
) -> Capture {
	reset(renderer, size);
	let frame_size = Size::new(size.width as f32, size.height as f32);
	let mut plot_frame = canvas::Frame::new(renderer, frame_size);
	let mut plot_backend = IcedBackend {
		frame: &mut plot_frame,
	};
	plot(&mut plot_backend);
	let mut overlay_frame = canvas::Frame::new(renderer, frame_size);
	let mut overlay_backend = IcedBackend {
		frame: &mut overlay_frame,
	};
	overlay(&mut overlay_backend);
	renderer.draw_geometry(plot_frame.into_geometry());
	renderer.draw_geometry(overlay_frame.into_geometry());
	screenshot(renderer, size)
}

fn render_product_0117_font_probe(
	renderer: &mut iced::Renderer,
	font: Font,
	text: &str,
) -> Capture {
	render_frame(renderer, Size::new(640, 160), |frame| {
		frame.fill_text(Text {
			content: text.to_owned(),
			position: Point::new(24.0, 48.0),
			color: Color::WHITE,
			size: Pixels(28.0),
			font,
			..Text::default()
		});
	})
}

#[derive(Clone, Copy)]
enum Product0068StrataPrototype {
	Current,
	CorrectedOver,
	CorrectedUnder,
}

fn product_0068_color(color: plot::chrome::PlotRgba) -> Color {
	Color::from_rgba(color.red(), color.green(), color.blue(), color.alpha())
}

fn draw_product_0068_grid(frame: &mut canvas::Frame, bounds: Rectangle, color: Color) {
	for index in 0..=5 {
		let y = bounds.y + bounds.height * (1.0 - index as f32 / 5.0);
		let path = Path::line(
			Point::new(bounds.x, y),
			Point::new(bounds.x + bounds.width, y),
		);
		frame.stroke(&path, Stroke::default().with_color(color).with_width(1.0));
	}
}

fn draw_product_0068_areas(
	frame: &mut canvas::Frame,
	bounds: Rectangle,
	boundary: Color,
	current_boundary: bool,
) {
	let series = [
		[25.0, 35.0, 45.0, 55.0, 65.0, 75.0, 85.0],
		[30.0, 28.0, 40.0, 38.0, 55.0, 50.0, 60.0],
		[20.0, 35.0, 25.0, 45.0, 30.0, 55.0, 40.0],
	];
	let colors = [
		Color::from_rgb8(1, 205, 254),
		Color::from_rgb8(229, 9, 20),
		Color::from_rgb8(255, 32, 121),
	];
	let x = |index: usize| bounds.x + bounds.width * index as f32 / 6.0;
	let y = |value: f32| bounds.y + bounds.height * (1.0 - value / 250.0);
	let mut lower = [0.0f32; 7];
	for (values, fill) in series.into_iter().zip(colors) {
		let upper = std::array::from_fn::<_, 7, _>(|index| lower[index] + values[index]);
		let area = Path::new(|builder| {
			builder.move_to(Point::new(x(0), y(upper[0])));
			for index in 1..7 {
				builder.line_to(Point::new(x(index), y(upper[index])));
			}
			for index in (0..7).rev() {
				builder.line_to(Point::new(x(index), y(lower[index])));
			}
			builder.close();
		});
		frame.fill(&area, fill);
		let edge = Path::new(|builder| {
			builder.move_to(Point::new(x(0), y(upper[0])));
			for index in 1..7 {
				builder.line_to(Point::new(x(index), y(upper[index])));
			}
		});
		frame.stroke(
			&edge,
			Stroke::default()
				.with_color(if current_boundary {
					Color::from_rgba(0.0, 0.0, 0.0, 0.2)
				} else {
					boundary
				})
				.with_width(0.5),
		);
		lower = upper;
	}
}

fn draw_product_0068_bars(frame: &mut canvas::Frame, bounds: Rectangle) {
	let values = [
		[70.0, 55.0, 35.0],
		[120.0, 95.0, 75.0],
		[175.0, 140.0, 110.0],
		[215.0, 180.0, 145.0],
	];
	let colors = [
		Color::from_rgb8(1, 205, 254),
		Color::from_rgb8(229, 9, 20),
		Color::from_rgb8(255, 32, 121),
	];
	let band = bounds.width / values.len() as f32;
	let category_inset = band * 0.08;
	let usable = band - category_inset * 2.0;
	let slot = usable / colors.len() as f32;
	for (category, category_values) in values.into_iter().enumerate() {
		for (series, value) in category_values.into_iter().enumerate() {
			let width = slot * 0.78;
			let x = bounds.x
				+ category as f32 * band
				+ category_inset
				+ series as f32 * slot
				+ (slot - width) / 2.0;
			let height = bounds.height * value / 250.0;
			frame.fill_rectangle(
				Point::new(x, bounds.y + bounds.height - height),
				iced::Size::new(width, height),
				colors[series],
			);
		}
	}
}

fn draw_product_0068_axes(frame: &mut canvas::Frame, plot: Rectangle, axis: Color) {
	let x_axis = Path::line(
		Point::new(plot.x, plot.y + plot.height),
		Point::new(plot.x + plot.width, plot.y + plot.height),
	);
	let y_axis = Path::line(
		Point::new(plot.x, plot.y),
		Point::new(plot.x, plot.y + plot.height),
	);
	frame.stroke(&x_axis, Stroke::default().with_color(axis).with_width(1.0));
	frame.stroke(&y_axis, Stroke::default().with_color(axis).with_width(1.0));
}

fn draw_product_0068_strata_exhibit(frame: &mut canvas::Frame) {
	use plot::chrome::{PlotChrome, PlotChromeProjection};

	let chrome = PlotChrome::from_theme_name("greys_rs2", PlotChromeProjection::Ordered)
		.expect("PRODUCT-0068 Stage 0 greys skin");
	let canvas = product_0068_color(chrome.canvas_background);
	let axis = product_0068_color(chrome.axis_line);
	let corrected_grid = Color { a: 0.35, ..axis };
	let current_grid = Color::from_rgba(1.0, 1.0, 1.0, 0.2);
	let variants = [
		(
			Product0068StrataPrototype::Current,
			"CURRENT · grid over · weak boundary",
		),
		(
			Product0068StrataPrototype::CorrectedOver,
			"CORRECTED · grid over",
		),
		(
			Product0068StrataPrototype::CorrectedUnder,
			"CORRECTED · grid under",
		),
	];
	let panel_width = frame.width() / variants.len() as f32;
	for (index, (variant, label)) in variants.into_iter().enumerate() {
		let panel_x = index as f32 * panel_width;
		frame.fill_rectangle(
			Point::new(panel_x + 8.0, 8.0),
			iced::Size::new(panel_width - 16.0, frame.height() - 16.0),
			canvas,
		);
		frame.fill_text(Text {
			content: label.to_owned(),
			position: Point::new(panel_x + panel_width / 2.0, 32.0),
			color: product_0068_color(chrome.title_text),
			size: Pixels(18.0),
			align_x: iced::alignment::Horizontal::Center.into(),
			..Text::default()
		});
		frame.fill_text(Text {
			content: "STACKED AREA".to_owned(),
			position: Point::new(panel_x + panel_width / 2.0, 68.0),
			color: product_0068_color(chrome.subtitle_text),
			size: Pixels(13.0),
			align_x: iced::alignment::Horizontal::Center.into(),
			..Text::default()
		});
		let area_plot = Rectangle::new(
			Point::new(panel_x + 56.0, 68.0),
			iced::Size::new(panel_width - 86.0, 410.0),
		);
		if matches!(variant, Product0068StrataPrototype::CorrectedUnder) {
			draw_product_0068_grid(frame, area_plot, corrected_grid);
		}
		draw_product_0068_areas(
			frame,
			area_plot,
			axis,
			matches!(variant, Product0068StrataPrototype::Current),
		);
		match variant {
			Product0068StrataPrototype::Current => {
				draw_product_0068_grid(frame, area_plot, current_grid)
			}
			Product0068StrataPrototype::CorrectedOver => {
				draw_product_0068_grid(frame, area_plot, corrected_grid)
			}
			Product0068StrataPrototype::CorrectedUnder => {}
		}
		draw_product_0068_axes(frame, area_plot, axis);

		frame.fill_text(Text {
			content: "GROUPED BARS".to_owned(),
			position: Point::new(panel_x + panel_width / 2.0, 568.0),
			color: product_0068_color(chrome.subtitle_text),
			size: Pixels(13.0),
			align_x: iced::alignment::Horizontal::Center.into(),
			..Text::default()
		});
		let bar_plot = Rectangle::new(
			Point::new(panel_x + 56.0, 578.0),
			iced::Size::new(panel_width - 86.0, 410.0),
		);
		if matches!(variant, Product0068StrataPrototype::CorrectedUnder) {
			draw_product_0068_grid(frame, bar_plot, corrected_grid);
		}
		draw_product_0068_bars(frame, bar_plot);
		match variant {
			Product0068StrataPrototype::Current => {
				draw_product_0068_grid(frame, bar_plot, current_grid)
			}
			Product0068StrataPrototype::CorrectedOver => {
				draw_product_0068_grid(frame, bar_plot, corrected_grid)
			}
			Product0068StrataPrototype::CorrectedUnder => {}
		}
		draw_product_0068_axes(frame, bar_plot, axis);
	}
}

fn write_product_0068_png(capture: &Capture, path: &std::path::Path) {
	let file = std::fs::File::create(path).expect("create PRODUCT-0068 Stage 0 PNG");
	let mut encoder = png::Encoder::new(file, capture.width, capture.height);
	encoder.set_color(png::ColorType::Rgba);
	encoder.set_depth(png::BitDepth::Eight);
	encoder
		.write_header()
		.expect("write PRODUCT-0068 PNG header")
		.write_image_data(&capture.rgba)
		.expect("write PRODUCT-0068 PNG pixels");
}

fn product_0068_bar_fixture() -> (BarPlotKernel, PlotSettings) {
	let colors = [(1, 205, 254), (229, 9, 20), (255, 32, 121)];
	let names = ["Atlas", "Beacon", "Cinder"];
	let group_colors = names
		.into_iter()
		.zip(colors)
		.enumerate()
		.map(
			|(index, (name, (red, green, blue)))| CategoryColorAssignment {
				category: name.to_owned(),
				position: PalettePosition::new(index, 3).unwrap(),
				color: ResolvedColor {
					red,
					green,
					blue,
					alpha: u8::MAX,
				},
			},
		)
		.collect();
	let kernel = BarPlotKernel {
		prepared_data: Arc::new(BarPreparedData {
			categories: vec!["East".to_owned(), "North".to_owned(), "South".to_owned()],
			group_names: names.into_iter().map(str::to_owned).collect(),
			values: vec![
				vec![70.0, 55.0, 35.0],
				vec![150.0, 120.0, 90.0],
				vec![225.0, 185.0, 145.0],
			],
			y_range: (0.0, 250.0),
			x_label: String::new(),
			y_label: String::new(),
			group_provenance: LegendProvenance::Named,
			click_selectable: false,
			selection_candidates: Vec::new(),
		}),
		orientation: Orientation::Vertical,
		layer_settings: LayerSettings::default(),
		band_fills: Vec::new(),
		group_colors,
	};
	let resolved = ResolvedPlotSettings {
		title: ResolvedPresence::Hide,
		subtitle: ResolvedPresence::Hide,
		x_label: ResolvedPresence::Hide,
		y_label: ResolvedPresence::Hide,
		legend: ResolvedPresence::Hide,
		x_tick_placement: TickPlacement::count(1).unwrap(),
		y_tick_placement: TickPlacement::count(5).unwrap(),
		show_x_minor_ticks: false,
		show_y_minor_ticks: false,
		show_x_minor_grid: false,
		show_y_minor_grid: false,
		plot_padding_top: 0.0,
		plot_padding_bottom: 0.0,
		plot_padding_left: 0.0,
		plot_padding_right: 0.0,
		bar_width: BarWidth::new(0.78).unwrap(),
		category_gap: CategoryGap::new(0.16).unwrap(),
		..ResolvedPlotSettings::default()
	};
	(kernel, PlotSettings::from_resolved(resolved))
}

fn render_product_0068_bar(
	renderer: &mut iced::Renderer,
	size: Size<u32>,
	overlay_control: bool,
	show_grid: bool,
) -> Capture {
	let (kernel, mut settings) = product_0068_bar_fixture();
	if !show_grid {
		let mut resolved = settings.resolve();
		resolved.show_x_major_grid = false;
		resolved.show_y_major_grid = false;
		settings = PlotSettings::from_resolved(resolved);
	}
	let resolved = settings.resolve();
	let bounds = Rectangle::with_size(Size::new(size.width as f32, size.height as f32));
	let widget = PlotWidget::new(
		&kernel,
		String::new(),
		0.0,
		settings.clone(),
		0,
		false,
		PlotRenderLayer::Full,
	);
	if overlay_control {
		render_frame(renderer, size, |frame| {
			let mut backend = IcedBackend { frame };
			backend.fill_rectangle(
				Point::ORIGIN,
				bounds.size(),
				resolved.chrome.canvas_background.iced(),
			);
			let layout = kernel.layout(resolved.clone());
			let requirements = BandRequirements::from_layout(&layout, &resolved);
			let plot_area =
				VerticalGuideBands::resolve_with_requirements("", None, &requirements, &resolved)
					.plot_area(Rect::from_iced_frame(bounds), 0.0, &resolved)
					.into_iced();
			let transform = CoordinateTransformer::new(&layout, plot_area);
			kernel.plot_with_settings(
				&mut backend,
				plot_area,
				&transform,
				iced::advanced::mouse::Cursor::Unavailable,
				&settings,
			);
			widget.draw_layout_grid(&mut backend, plot_area, &transform, &layout);
			widget.render_overlay_layer(&mut backend, bounds);
		})
	} else {
		render_plot_and_overlay(
			renderer,
			size,
			|backend| widget.render_plot_layer(backend, bounds),
			|backend| widget.render_overlay_layer(backend, bounds),
		)
	}
}

fn product_rotation_fixture(
	rotation: f32,
	categories: &[&str],
	values: &[f64],
) -> (BarPlotKernel, PlotSettings) {
	let (mut kernel, _) = product_0068_bar_fixture();
	kernel.prepared_data = Arc::new(BarPreparedData {
		categories: categories
			.iter()
			.map(|category| (*category).to_owned())
			.collect(),
		group_names: vec!["Series".to_owned()],
		values: values.iter().map(|value| vec![*value]).collect(),
		y_range: (0.0, 90.0),
		x_label: String::new(),
		y_label: String::new(),
		group_provenance: LegendProvenance::Named,
		click_selectable: false,
		selection_candidates: Vec::new(),
	});
	kernel.group_colors.truncate(1);
	let mut resolved = ResolvedPlotSettings {
		title: ResolvedPresence::Hide,
		subtitle: ResolvedPresence::Hide,
		x_label: ResolvedPresence::Hide,
		y_label: ResolvedPresence::Hide,
		legend: ResolvedPresence::Hide,
		show_x_minor_ticks: false,
		show_y_minor_ticks: false,
		show_x_minor_grid: false,
		show_y_minor_grid: false,
		..ResolvedPlotSettings::default()
	};
	resolved.x_tick_rotation = TickRotation::new(rotation).expect("PRODUCT-0115 probe angle");
	(kernel, PlotSettings::from_resolved(resolved))
}

fn product_0115_rotation_fixture(rotation: f32) -> (BarPlotKernel, PlotSettings) {
	product_rotation_fixture(
		rotation,
		&[
			"International partner enablement and certification",
			"North American enterprise renewal commitments",
			"Regulated migration and governance readiness",
		],
		&[62.0, 74.0, 58.0],
	)
}

fn product_0116_rotation_fixture(rotation: f32) -> (BarPlotKernel, PlotSettings) {
	product_rotation_fixture(
		rotation,
		&["Edge", "Ship", "Ship delivery commitment", "Edge"],
		&[62.0, 74.0, 58.0, 66.0],
	)
}

fn product_0127_selected_tick_fixture() -> (BarPlotKernel, PlotSettings) {
	let (mut kernel, _) = product_0068_bar_fixture();
	kernel.prepared_data = Arc::new(BarPreparedData {
		categories: vec!["East".to_owned(), "North".to_owned(), "South".to_owned()],
		group_names: vec!["Value".to_owned()],
		values: vec![vec![82.0], vec![47.0], vec![61.0]],
		y_range: (0.0, 100.0),
		x_label: String::new(),
		y_label: "Revenue".to_owned(),
		group_provenance: LegendProvenance::Named,
		click_selectable: false,
		selection_candidates: Vec::new(),
	});
	kernel.group_colors.truncate(1);
	let resolved = ResolvedPlotSettings {
		title: ResolvedPresence::Hide,
		subtitle: ResolvedPresence::Hide,
		x_label: ResolvedPresence::Hide,
		y_label: ResolvedPresence::Show(Arc::new("Revenue".to_owned())),
		legend: ResolvedPresence::Hide,
		show_x_minor_ticks: false,
		show_y_minor_ticks: false,
		show_x_minor_grid: false,
		show_y_minor_grid: false,
		plot_padding_top: 0.0,
		plot_padding_bottom: 0.0,
		plot_padding_left: 0.0,
		plot_padding_right: 0.0,
		..ResolvedPlotSettings::default()
	};
	(kernel, PlotSettings::from_resolved(resolved))
}

fn render_product_0127_selected_ticks(
	renderer: &mut iced::Renderer,
	size: Size<u32>,
) -> (Capture, Rectangle, f32, f32) {
	let (kernel, settings_model) = product_0127_selected_tick_fixture();
	let settings = settings_model.resolve();
	let bounds = Rectangle::with_size(Size::new(size.width as f32, size.height as f32));
	let layout = kernel.layout(settings.clone());
	let requirements = BandRequirements::from_layout(&layout, &settings);
	let guides = VerticalGuideBands::y_axis_guides(&requirements, true, &settings);
	let plot_area = VerticalGuideBands::resolve_with_requirements_and_x_label(
		"",
		None,
		&requirements,
		false,
		&settings,
	)
	.plot_area(Rect::from_iced_frame(bounds), 0.0, &settings)
	.into_iced();
	let widget = PlotWidget::new(
		&kernel,
		String::new(),
		0.0,
		settings_model,
		0,
		false,
		PlotRenderLayer::Full,
	);
	let capture = render_frame(renderer, size, |frame| {
		let mut backend = IcedBackend { frame };
		widget.render(&mut backend, bounds);
	});
	(
		capture,
		plot_area,
		guides.left,
		guides.title_x.expect("visible y title"),
	)
}

fn product_0127_y_tick_ink_distances(capture: &Capture, plot_area: Rectangle) -> (u32, u32) {
	let mut distances = Vec::new();
	for distance in 9..=60 {
		let x = (plot_area.x - distance as f32).round() as u32;
		for y in plot_area.y.max(0.0).ceil() as u32
			..(plot_area.y + plot_area.height * 0.35).ceil() as u32
		{
			if capture
				.pixel(x, y)
				.iter()
				.copied()
				.max()
				.unwrap_or_default()
				> 120
			{
				distances.push(distance);
				break;
			}
		}
	}
	(
		*distances
			.first()
			.expect("PRODUCT-0127 y tick ink begins in the left band"),
		*distances
			.last()
			.expect("PRODUCT-0127 y tick ink ends in the left band"),
	)
}

fn render_product_0115_rotation(
	renderer: &mut iced::Renderer,
	size: Size<u32>,
	rotation: f32,
) -> (Capture, Rectangle) {
	render_product_rotation(renderer, size, product_0115_rotation_fixture(rotation))
}

fn render_product_0116_rotation(
	renderer: &mut iced::Renderer,
	size: Size<u32>,
	rotation: f32,
) -> (Capture, Rectangle) {
	render_product_rotation(renderer, size, product_0116_rotation_fixture(rotation))
}

fn render_product_rotation(
	renderer: &mut iced::Renderer,
	size: Size<u32>,
	(kernel, settings): (BarPlotKernel, PlotSettings),
) -> (Capture, Rectangle) {
	let resolved = settings.resolve();
	// Render a figure smaller than the target, anchored at the target origin.
	// Pixels in the unused frame prove whether an axis overlay escaped its own
	// explicit figure clip.
	let figure_bounds = Rectangle::with_size(Size::new(
		size.width as f32 - 160.0,
		size.height as f32 - 80.0,
	));
	let widget = PlotWidget::new(
		&kernel,
		String::new(),
		0.0,
		settings,
		0,
		false,
		PlotRenderLayer::Full,
	);
	let capture = render_plot_and_overlay(
		renderer,
		size,
		|backend| widget.render_plot_layer(backend, figure_bounds),
		|backend| widget.render_overlay_layer(backend, figure_bounds),
	);
	(capture, figure_bounds)
}

fn render_product_kernel_rotation(
	renderer: &mut iced::Renderer,
	size: Size<u32>,
	kernel: &dyn PlotKernel,
	settings: PlotSettings,
) -> (Capture, Rectangle, Rectangle) {
	let resolved = settings.resolve();
	let figure_bounds = Rectangle::with_size(Size::new(
		size.width as f32 - 160.0,
		size.height as f32 - 80.0,
	));
	let layout = kernel.layout(resolved.clone());
	let requirements = BandRequirements::from_layout(&layout, &resolved);
	let plot_area = VerticalGuideBands::resolve_with_requirements(
		"Count control",
		None,
		&requirements,
		&resolved,
	)
	.plot_area(Rect::from_iced_frame(figure_bounds), 0.0, &resolved)
	.into_iced();
	let settings_model = settings;
	let widget = PlotWidget::new(
		kernel,
		"Count control".to_owned(),
		0.0,
		settings_model,
		0,
		false,
		PlotRenderLayer::Full,
	);
	let capture = render_plot_and_overlay(
		renderer,
		size,
		|backend| widget.render_plot_layer(backend, figure_bounds),
		|backend| widget.render_overlay_layer(backend, figure_bounds),
	);
	(capture, figure_bounds, plot_area)
}

fn product_temporal_count_fixture(rotation: f32) -> (LinePlotKernel, PlotSettings) {
	let first = 1_709_164_800_000_i64;
	let last = 1_710_054_400_000_i64;
	let mut resolved = product_0068_cartesian_settings().resolve();
	resolved.title = ResolvedPresence::Show("Count control".to_owned().into());
	resolved.subtitle = ResolvedPresence::Show(
		"No tick_every: existing Count placement remains available"
			.to_owned()
			.into(),
	);
	resolved.x_tick_rotation = TickRotation::new(rotation).expect("temporal probe angle");
	let settings = PlotSettings::from_resolved(resolved);
	(
		LinePlotKernel::new(Arc::new(LinePreparedData {
			series: vec![SeriesData {
				identity: None,
				name: "activity".to_owned(),
				points: vec![[first as f64, 40.0], [last as f64, 70.0]],
				color_t: 0.0,
				resolved_color: Some(Color::WHITE),
				provenance: LegendProvenance::Named,
			}],
			x_range: (first as f64, last as f64),
			y_range: (0.0, 100.0),
			x_axis_type: AxisType::Datetime {
				unit: TimeUnit::Milliseconds,
				zone: Some(Arc::<str>::from("UTC")),
			},
			y_axis_type: AxisType::Linear,
			x_temporal_domain: Some(TemporalAxisDomain::Datetime {
				first_tick: first,
				last_tick: last,
				unit: TimeUnit::Milliseconds,
				zone: Some(Arc::<str>::from("UTC")),
			}),
			y_temporal_domain: None,
			x_label: String::new(),
			y_label: String::new(),
		})),
		settings,
	)
}

fn exterior_luminous_pixels(capture: &Capture, bounds: Rectangle) -> usize {
	// Iced/wgpu antialiases the clip boundary itself. Count only pixels beyond
	// that two-pixel fringe; an escaped label is hundreds of pixels wide.
	const CLIP_EDGE_FRINGE: f32 = 2.0;
	let mut count = 0;
	for y in 0..capture.height {
		for x in 0..capture.width {
			let inside = (x as f32) >= bounds.x - CLIP_EDGE_FRINGE
				&& (x as f32) < bounds.x + bounds.width + CLIP_EDGE_FRINGE
				&& (y as f32) >= bounds.y - CLIP_EDGE_FRINGE
				&& (y as f32) < bounds.y + bounds.height + CLIP_EDGE_FRINGE;
			if !inside {
				count += usize::from(
					capture
						.pixel(x, y)
						.iter()
						.copied()
						.max()
						.unwrap_or_default()
						> 120,
				);
			}
		}
	}
	count
}

/// PRODUCT-0116's production corroboration deliberately compares labels with
/// the same leading glyphs but radically different lengths. The operation
/// trace proves their logical origins exactly; this only checks that iced/wgpu
/// does not turn the removed estimate-driven vertical term into a visible
/// length-proportional cascade.
fn product_0116_tick_run_top(
	capture: &Capture,
	figure_bounds: Rectangle,
	category_index: usize,
) -> u32 {
	let (kernel, settings) = product_0116_rotation_fixture(90.0);
	let resolved = settings.resolve();
	let layout = kernel.layout(resolved.clone());
	let requirements = BandRequirements::from_layout(&layout, &resolved);
	let plot_area =
		VerticalGuideBands::resolve_with_requirements("", None, &requirements, &resolved)
			.plot_area(Rect::from_iced_frame(figure_bounds), 0.0, &resolved)
			.into_iced();
	let (categories, y_range) = match &layout {
		PlotLayout::CategoricalX {
			categories,
			y_range,
			..
		} => (categories, *y_range),
		_ => unreachable!("PRODUCT-0116 fixture stays categorical on x"),
	};
	let transform = CoordinateTransformer::new(&layout, plot_area);
	let (tick, _) = transform.categorical(category_index, y_range.0);
	let first_y = (tick.y + resolved.x_label_offset - 2.0).max(0.0).floor() as u32;
	let left = (tick.x - resolved.x_tick_size.pixels() - 4.0)
		.max(0.0)
		.floor() as u32;
	let right = (tick.x + resolved.x_tick_size.pixels() + 4.0)
		.min(capture.width as f32)
		.ceil() as u32;
	assert!(
		category_index < categories.len(),
		"PRODUCT-0116 probe selects an existing interior category"
	);
	for y in first_y
		..capture
			.height
			.min((figure_bounds.y + figure_bounds.height).ceil() as u32)
	{
		if (left..right).any(|x| {
			capture
				.pixel(x, y)
				.iter()
				.copied()
				.max()
				.unwrap_or_default()
				> 120
		}) {
			return y;
		}
	}
	panic!("PRODUCT-0116 probe must realize tick-label ink below the axis");
}

fn product_0068_cartesian_settings() -> PlotSettings {
	PlotSettings::from_resolved(ResolvedPlotSettings {
		title: ResolvedPresence::Hide,
		subtitle: ResolvedPresence::Hide,
		x_label: ResolvedPresence::Hide,
		y_label: ResolvedPresence::Hide,
		legend: ResolvedPresence::Hide,
		x_tick_placement: TickPlacement::count(2).unwrap(),
		y_tick_placement: TickPlacement::count(2).unwrap(),
		show_x_minor_ticks: false,
		show_y_minor_ticks: false,
		show_x_minor_grid: false,
		show_y_minor_grid: false,
		plot_padding_top: 0.0,
		plot_padding_bottom: 0.0,
		plot_padding_left: 0.0,
		plot_padding_right: 0.0,
		point_size: PointSize::new(3.0).unwrap(),
		stroke_width: StrokeWidth::new(3.0).unwrap(),
		..ResolvedPlotSettings::default()
	})
}

fn product_0068_line_fixture(color: Color) -> LinePlotKernel {
	LinePlotKernel::new(Arc::new(LinePreparedData {
		series: vec![SeriesData {
			identity: None,
			name: "diagonal".to_owned(),
			points: vec![[0.0, 0.0], [1.0, 1.0]],
			color_t: 0.0,
			resolved_color: Some(color),
			provenance: LegendProvenance::Named,
		}],
		x_range: (0.0, 1.0),
		y_range: (0.0, 1.0),
		x_axis_type: AxisType::Linear,
		y_axis_type: AxisType::Linear,
		x_temporal_domain: None,
		y_temporal_domain: None,
		x_label: String::new(),
		y_label: String::new(),
	}))
}

fn product_0068_scatter_fixture(color: Color) -> ScatterPlotKernel {
	ScatterPlotKernel::new(Arc::new(ScatterPreparedData {
		raster_subject: plot::raster::RasterSubject {
			publication: result_artifact::ArtifactToken::mint(),
			binding: plot::raster::RasterFieldBinding {
				x: "x".to_owned(),
				y: "y".to_owned(),
				color: None,
			},
		},
		series: vec![ScatterSeries {
			identity: None,
			name: "point".to_owned(),
			x_values: Float64Chunked::from_slice("x".into(), &[0.5]),
			y_values: Float64Chunked::from_slice("y".into(), &[0.5]),
			color_t: 0.0,
			resolved_color: Some(color),
			provenance: LegendProvenance::Named,
			selection_candidates: vec![None],
		}],
		x_categories: None,
		x_range: (0.0, 1.0),
		y_range: (0.0, 1.0),
		x_axis_type: AxisType::Linear,
		y_axis_type: AxisType::Linear,
		x_temporal_domain: None,
		y_temporal_domain: None,
		x_label: String::new(),
		y_label: String::new(),
		total_points: 1,
	}))
}

fn product_0068_stacked_area_fixture() -> (StackedAreaPlotKernel, PlotSettings, [[u8; 3]; 2]) {
	let mut resolved = product_0068_cartesian_settings().resolve();
	resolved.mark_style.decoration = plot::chrome::PlotRgba::from_iced(Color::WHITE);
	resolved.y_tick_placement = TickPlacement::count(5).unwrap();
	let colors = [
		resolved.color_theme.get_color(0.0),
		resolved.color_theme.get_color(1.0),
	]
	.map(|color| {
		[
			(color.r * 255.0).round() as u8,
			(color.g * 255.0).round() as u8,
			(color.b * 255.0).round() as u8,
		]
	});
	(
		StackedAreaPlotKernel {
			prepared_data: Arc::new(StackedAreaPreparedData {
				identities: Vec::new(),
				categories: vec!["Atlas".to_owned(), "Beacon".to_owned()],
				unique_xs: vec![0.0, 0.25, 0.5, 0.75, 1.0],
				category_values: vec![vec![2.0, 2.5, 2.0, 3.0, 2.5], vec![2.0, 1.5, 2.5, 2.0, 2.5]],
				totals: vec![4.0, 4.0, 4.5, 5.0, 5.0],
				x_range: (0.0, 1.0),
				y_range: (0.0, 10.0),
				x_axis_type: AxisType::Linear,
				y_axis_type: AxisType::Linear,
				x_temporal_domain: None,
				y_temporal_domain: None,
				x_label: String::new(),
				y_label: String::new(),
				series_provenance: plot::legend::LegendProvenance::Named,
			}),
			series_colors: Vec::new(),
			normalization: StackNormalization::Absolute,
		},
		PlotSettings::from_resolved(resolved),
		colors,
	)
}

fn render_product_0068_cartesian_mark(
	renderer: &mut iced::Renderer,
	size: Size<u32>,
	kernel: &dyn PlotKernel,
	settings: &PlotSettings,
	overlay_control: bool,
) -> Capture {
	let resolved = settings.resolve();
	let bounds = Rectangle::with_size(Size::new(size.width as f32, size.height as f32));
	let widget = PlotWidget::new(
		kernel,
		String::new(),
		0.0,
		settings.clone(),
		0,
		false,
		PlotRenderLayer::Full,
	);
	if overlay_control {
		render_frame(renderer, size, |frame| {
			let mut backend = IcedBackend { frame };
			backend.fill_rectangle(
				Point::ORIGIN,
				bounds.size(),
				resolved.chrome.canvas_background.iced(),
			);
			let layout = kernel.layout(resolved.clone());
			let transform = CoordinateTransformer::new(&layout, bounds);
			kernel.plot_with_settings(
				&mut backend,
				bounds,
				&transform,
				iced::advanced::mouse::Cursor::Unavailable,
				settings,
			);
			widget.draw_layout_grid(&mut backend, bounds, &transform, &layout);
			widget.render_overlay_layer(&mut backend, bounds);
		})
	} else {
		render_plot_and_overlay(
			renderer,
			size,
			|backend| widget.render_plot_layer(backend, bounds),
			|backend| widget.render_overlay_layer(backend, bounds),
		)
	}
}

fn assert_product_0068_cartesian_mark_control(
	label: &str,
	under: &Capture,
	overlay: &Capture,
	mark: [u8; 3],
) -> (usize, usize) {
	assert_eq!(under.width, overlay.width);
	assert_eq!(under.height, overlay.height);
	let mut preserved_intersections = 0usize;
	let mut grid_rows = std::collections::BTreeSet::new();
	for (index, (under_pixel, overlay_pixel)) in under
		.rgba
		.chunks_exact(4)
		.zip(overlay.rgba.chunks_exact(4))
		.enumerate()
	{
		let under_rgb = [under_pixel[0], under_pixel[1], under_pixel[2]];
		let overlay_rgb = [overlay_pixel[0], overlay_pixel[1], overlay_pixel[2]];
		if near(under_rgb, mark, 12) && color_distance(overlay_rgb, mark) >= 20 {
			preserved_intersections += 1;
			grid_rows.insert(index as u32 / under.width);
		}
	}
	assert!(
		preserved_intersections >= 2,
		"PRODUCT-0068 {label}: the real mark must cross and occlude a real grid in production; got {preserved_intersections} discriminating pixels"
	);
	let visible_grid = grid_rows
		.iter()
		.flat_map(|row| (0..under.width).map(move |x| under.pixel(x, *row)))
		.filter(|pixel| {
			let spread = pixel.iter().max().unwrap() - pixel.iter().min().unwrap();
			spread <= 3 && pixel[0] >= 24 && pixel[0] <= 120
		})
		.count();
	assert!(
		visible_grid >= 4,
		"PRODUCT-0068 {label}: the intersected gridline must remain visible on adjacent canvas; got {visible_grid} pixels"
	);
	(preserved_intersections, visible_grid)
}

fn assert_product_0068_stacked_area_control(
	under: &Capture,
	overlay: &Capture,
	fills: [[u8; 3]; 2],
) -> (usize, usize) {
	let mut exact_intersections = 0usize;
	for (under_pixel, overlay_pixel) in under.rgba.chunks_exact(4).zip(overlay.rgba.chunks_exact(4))
	{
		let under_rgb = [under_pixel[0], under_pixel[1], under_pixel[2]];
		let overlay_rgb = [overlay_pixel[0], overlay_pixel[1], overlay_pixel[2]];
		if fills.iter().any(|fill| near(under_rgb, *fill, 12))
			&& fills
				.iter()
				.all(|fill| color_distance(overlay_rgb, *fill) >= 20)
		{
			exact_intersections += 1;
		}
	}
	assert!(
		exact_intersections >= 20,
		"PRODUCT-0068 stacked area must retain exact resolved fills where the overlay control composites grids; got {exact_intersections} pixels"
	);
	for fill in fills {
		assert!(
			under
				.rgba
				.chunks_exact(4)
				.any(|pixel| near([pixel[0], pixel[1], pixel[2]], fill, 12)),
			"PRODUCT-0068 stacked-area fixture must realize resolved fill {fill:?}"
		);
	}
	let headroom_grid = (0..under.height / 3)
		.flat_map(|y| (0..under.width).map(move |x| under.pixel(x, y)))
		.filter(|pixel| {
			let spread = pixel.iter().max().unwrap() - pixel.iter().min().unwrap();
			spread <= 3 && pixel[0] >= 24 && pixel[0] <= 120
		})
		.count();
	assert!(
		headroom_grid >= 100,
		"PRODUCT-0068 stacked-area headroom must retain real gridlines; got {headroom_grid} pixels"
	);
	(exact_intersections, headroom_grid)
}

fn product_0072_percent_area_fixture() -> (StackedAreaPlotKernel, PlotSettings, [[u8; 3]; 2]) {
	let colors = [BLUE, RED];
	let assignments = ["Atlas", "Beacon"]
		.into_iter()
		.zip(colors)
		.enumerate()
		.map(
			|(index, (category, [red, green, blue]))| CategoryColorAssignment {
				category: category.to_owned(),
				position: PalettePosition::new(index, 2).unwrap(),
				color: ResolvedColor {
					red,
					green,
					blue,
					alpha: u8::MAX,
				},
			},
		)
		.collect::<Vec<_>>();
	let mut resolved = product_0068_cartesian_settings().resolve();
	resolved.mark_style.decoration = plot::chrome::PlotRgba::from_iced(Color::WHITE);
	resolved.y_tick_placement = TickPlacement::step(25.0).unwrap();
	(
		StackedAreaPlotKernel {
			prepared_data: Arc::new(StackedAreaPreparedData {
				identities: Vec::new(),
				categories: vec!["Atlas".to_owned(), "Beacon".to_owned()],
				unique_xs: vec![0.0, 0.25, 0.5, 0.75, 1.0],
				category_values: vec![vec![30.0; 5], vec![70.0; 5]],
				totals: vec![100.0; 5],
				x_range: (0.0, 1.0),
				y_range: (0.0, 105.0),
				x_axis_type: AxisType::Linear,
				y_axis_type: AxisType::Linear,
				x_temporal_domain: None,
				y_temporal_domain: None,
				x_label: String::new(),
				y_label: String::new(),
				series_provenance: LegendProvenance::Named,
			}),
			series_colors: assignments,
			normalization: StackNormalization::Percent,
		},
		PlotSettings::from_resolved(resolved),
		colors,
	)
}

fn product_0080_histogram_fixture(
	separator: plot::chrome::PlotRgba,
) -> (HistogramPlotKernel, PlotSettings) {
	let bins = [
		(0.0, 10.0, 3),
		(10.0, 20.0, 25),
		(20.0, 30.0, 25),
		(30.0, 40.0, 9),
		(40.0, 50.0, 2),
	]
	.into_iter()
	.map(|(start, end, count)| HistogramBin { start, end, count })
	.collect();
	let kernel = HistogramPlotKernel {
		prepared_data: Arc::new(HistogramPreparedData {
			bins: HistogramBins::new(bins).expect("PRODUCT-0080 exact test25 relation"),
			x_axis_type: AxisType::Linear,
			y_axis_type: AxisType::Linear,
			x_label: String::new(),
			y_label: String::new(),
			selection_candidates: Vec::new(),
		}),
	};
	let mut resolved = product_0068_cartesian_settings().resolve();
	resolved.mark_style.fill = plot::chrome::PlotRgba::opaque(1.0, 1.0, 1.0);
	resolved.mark_style.separator = separator;
	(kernel, PlotSettings::from_resolved(resolved))
}

fn render_product_0080_histogram(
	renderer: &mut iced::Renderer,
	size: Size<u32>,
	separator: plot::chrome::PlotRgba,
) -> Capture {
	let (kernel, settings) = product_0080_histogram_fixture(separator);
	render_product_0068_cartesian_mark(renderer, size, &kernel, &settings, false)
}

fn product_0080_separator_difference_pixels(accepted: &Capture, banked: &Capture) -> usize {
	assert_eq!(
		(accepted.width, accepted.height),
		(banked.width, banked.height),
		"PRODUCT-0080 controls must share one capture extent"
	);
	(0..accepted.height)
		.flat_map(|y| (0..accepted.width).map(move |x| (x, y)))
		.filter(|&(x, y)| {
			near(accepted.pixel(x, y), BACKGROUND, 12) && near(banked.pixel(x, y), WHITE, 12)
		})
		.count()
}

fn product_0080_filled_pixels_beside_separator(accepted: &Capture, banked: &Capture) -> usize {
	(3..accepted.height.saturating_sub(3))
		.flat_map(|y| (3..accepted.width.saturating_sub(3)).map(move |x| (x, y)))
		.filter(|&(x, y)| {
			near(accepted.pixel(x, y), BACKGROUND, 12)
				&& near(banked.pixel(x, y), WHITE, 12)
				&& near(accepted.pixel(x - 3, y), WHITE, 12)
				&& near(accepted.pixel(x + 3, y), WHITE, 12)
		})
		.count()
}

fn product_0083_box_fixture(fill_detail: plot::chrome::PlotRgba) -> (BoxPlotKernel, PlotSettings) {
	let boxes = TukeyBoxes::new(vec![TukeyBoxRow {
		category: "none".to_owned(),
		whisker_low: 20.0,
		q1: 21.0,
		median: 23.0,
		q3: 24.0,
		whisker_high: 25.0,
		outlier: None,
	}])
	.expect("PRODUCT-0083 exact non-degenerate test26 relation");
	let kernel = BoxPlotKernel {
		prepared_data: Arc::new(BoxPlotPreparedData::new(
			boxes,
			AxisType::Linear,
			String::new(),
			String::new(),
		)),
	};
	let mut resolved = product_0068_cartesian_settings().resolve();
	resolved.y_min = Some(0.0);
	resolved.y_max = Some(40.0);
	resolved.mark_style.fill_detail = fill_detail;
	(kernel, PlotSettings::from_resolved(resolved))
}

fn render_product_0083_box(
	renderer: &mut iced::Renderer,
	size: Size<u32>,
	fill_detail: plot::chrome::PlotRgba,
) -> (Capture, Rectangle) {
	let (kernel, settings) = product_0083_box_fixture(fill_detail);
	let resolved = settings.resolve();
	let bounds = Rectangle::with_size(Size::new(size.width as f32, size.height as f32));
	let layout = kernel.layout(resolved.clone());
	let requirements = BandRequirements::from_layout(&layout, &resolved);
	let plot_area = VerticalGuideBands::resolve_with_requirements_and_x_label(
		"",
		None,
		&requirements,
		false,
		&resolved,
	)
	.plot_area(Rect::from_iced_frame(bounds), 0.0, &resolved)
	.into_iced();
	let transform = CoordinateTransformer::new(&layout, plot_area);
	let (center, band_width) = transform.categorical(0, 0.0);
	let median_y = transform.categorical(0, 23.0).0.y;
	let box_width = band_width * 0.6;
	let census = Rectangle::new(
		Point::new(center.x - box_width * 0.25, median_y - 3.0),
		Size::new(box_width * 0.5, 6.0),
	);
	(
		render_product_0068_cartesian_mark(&mut *renderer, size, &kernel, &settings, false),
		census,
	)
}

fn product_0083_paper_pixels(capture: &Capture, bounds: Rectangle) -> usize {
	let x0 = bounds.x.floor().max(0.0) as u32;
	let y0 = bounds.y.floor().max(0.0) as u32;
	let x1 = (bounds.x + bounds.width).ceil().max(0.0) as u32;
	let y1 = (bounds.y + bounds.height).ceil().max(0.0) as u32;
	let mut count = 0;
	for y in y0.min(capture.height)..y1.min(capture.height) {
		for x in x0.min(capture.width)..x1.min(capture.width) {
			count += usize::from(near(capture.pixel(x, y), BACKGROUND, 12));
		}
	}
	count
}

fn product_0072_percent_bar_fixture(
	gapless: bool,
) -> (StackedBarPlotKernel, PlotSettings, [[u8; 3]; 2]) {
	let colors = [BLUE, RED];
	let assignments = ["Atlas", "Beacon"]
		.into_iter()
		.zip(colors)
		.enumerate()
		.map(
			|(index, (category, [red, green, blue]))| CategoryColorAssignment {
				category: category.to_owned(),
				position: PalettePosition::new(index, 2).unwrap(),
				color: ResolvedColor {
					red,
					green,
					blue,
					alpha: u8::MAX,
				},
			},
		)
		.collect::<Vec<_>>();
	let resolved = ResolvedPlotSettings {
		title: ResolvedPresence::Hide,
		subtitle: ResolvedPresence::Hide,
		x_label: ResolvedPresence::Hide,
		y_label: ResolvedPresence::Hide,
		legend: ResolvedPresence::Hide,
		x_tick_placement: TickPlacement::count(1).unwrap(),
		y_tick_placement: TickPlacement::step(25.0).unwrap(),
		show_x_minor_ticks: false,
		show_y_minor_ticks: false,
		show_x_minor_grid: false,
		show_y_minor_grid: false,
		plot_padding_top: 0.0,
		plot_padding_bottom: 0.0,
		plot_padding_left: 0.0,
		plot_padding_right: 0.0,
		bar_width: BarWidth::new(if gapless { 1.0 } else { 0.72 }).unwrap(),
		category_gap: CategoryGap::new(if gapless { 0.0 } else { 0.24 }).unwrap(),
		..ResolvedPlotSettings::default()
	};
	(
		StackedBarPlotKernel {
			prepared_data: Arc::new(StackedBarPreparedData {
				categories: vec!["East".to_owned(), "West".to_owned()],
				group_names: vec!["Atlas".to_owned(), "Beacon".to_owned()],
				category_values: vec![vec![30.0, 70.0], vec![60.0, 40.0]],
				totals: vec![100.0, 100.0],
				y_range: (0.0, 110.0),
				x_label: String::new(),
				y_label: String::new(),
				group_provenance: LegendProvenance::Named,
			}),
			orientation: Orientation::Vertical,
			layer_settings: LayerSettings::default(),
			band_fills: Vec::new(),
			group_colors: assignments,
			normalization: StackNormalization::Percent,
		},
		PlotSettings::from_resolved(resolved),
		colors,
	)
}

fn assert_product_0072_occlusion(
	label: &str,
	under: &Capture,
	overlay: &Capture,
	fills: [[u8; 3]; 2],
) -> usize {
	let changed_fill_pixels = under
		.rgba
		.chunks_exact(4)
		.zip(overlay.rgba.chunks_exact(4))
		.filter(|(under, overlay)| {
			let under = [under[0], under[1], under[2]];
			let overlay = [overlay[0], overlay[1], overlay[2]];
			fills.iter().any(|fill| near(under, *fill, 12))
				&& fills
					.iter()
					.all(|fill| color_distance(overlay, *fill) >= 20)
		})
		.count();
	assert!(
		changed_fill_pixels >= 100,
		"PRODUCT-0072 {label}: production grid-under must preserve resolved fill where an overlay-grid control visibly composites; got {changed_fill_pixels} pixels"
	);
	for fill in fills {
		assert!(
			under
				.rgba
				.chunks_exact(4)
				.any(|pixel| near([pixel[0], pixel[1], pixel[2]], fill, 12)),
			"PRODUCT-0072 {label}: the fully occupied fixture must realize fill {fill:?}"
		);
	}
	changed_fill_pixels
}

struct FixedPercentCellKernel;

impl PlotKernel for FixedPercentCellKernel {
	fn layout(&self, _settings: ResolvedPlotSettings) -> PlotLayout {
		PlotLayout::Cartesian {
			x_range: (0.0, 100.0),
			y_range: (0.0, 100.0),
			x_axis_type: AxisType::Percent,
			y_axis_type: AxisType::Percent,
			x_temporal_domain: None,
			y_temporal_domain: None,
		}
	}

	fn plot(
		&self,
		_backend: &mut dyn PlotBackend,
		_bounds: Rectangle,
		_transform: &CoordinateTransformer,
		_cursor: iced::advanced::mouse::Cursor,
		_settings: ResolvedPlotSettings,
	) {
	}

	fn hit(
		&self,
		_transform: &CoordinateTransformer,
		_cursor: iced::advanced::mouse::Cursor,
	) -> Option<crate::plot::common::PlotHit> {
		None
	}

	fn hover(
		&self,
		_transform: &CoordinateTransformer,
		_cursor: iced::advanced::mouse::Cursor,
	) -> Option<String> {
		None
	}
}

fn product_0077_percent_grid() -> PlotGridKernel {
	let data = Arc::new(
		polars::df!(
			"facet" => ["A", "B", "C", "D"],
			"x" => [0.0_f64, 1.0, 2.0, 3.0],
			"y" => [0.0_f64, 1.0, 2.0, 3.0],
		)
		.expect("PRODUCT-0077 percent-facet frame"),
	);
	let artifact = result_artifact::ResultArtifact::from_polars(Arc::clone(&data));
	let resolved = plan(PlotRequest {
		source: PlotSource::Materialized {
			name: "PRODUCT-0077 percent facets".to_owned(),
			data: Arc::clone(&artifact),
		},
		authored_spec: AuthoredPlotSpec::xy(PlotComposition::single(PlotType::Line), "x", "y"),
	})
	.expect("PRODUCT-0077 resolve facet body");
	let grid = PlotGrid::value_facet(
		Arc::new(resolved.resolved_spec),
		&StampedResult {
			data: artifact,
			provenance: PlotProvenance {
				source_name: "PRODUCT-0077 percent facets".to_owned(),
				revision: None,
			},
		},
		"facet",
		GridArrangement::new(2, 2).expect("PRODUCT-0077 arrangement"),
	)
	.expect("PRODUCT-0077 facet grid");
	PlotGridKernel::from_grid_with_factory(grid, 800, 600, |_spec, _data| {
		Ok(Arc::new(FixedPercentCellKernel))
	})
	.expect("PRODUCT-0077 percent grid kernel")
}

fn product_0077_settings(tick_label_text: plot::chrome::PlotRgba) -> PlotSettings {
	let mut resolved = ResolvedPlotSettings {
		title: ResolvedPresence::Hide,
		subtitle: ResolvedPresence::Hide,
		x_label: ResolvedPresence::Hide,
		y_label: ResolvedPresence::Hide,
		legend: ResolvedPresence::Hide,
		x_tick_placement: TickPlacement::step(25.0).expect("PRODUCT-0077 x step"),
		y_tick_placement: TickPlacement::step(25.0).expect("PRODUCT-0077 y step"),
		show_x_minor_ticks: false,
		show_y_minor_ticks: false,
		show_x_minor_grid: false,
		show_y_minor_grid: false,
		..ResolvedPlotSettings::default()
	};
	resolved.chrome.tick_label_text = tick_label_text;
	PlotSettings::from_resolved(resolved)
}

fn render_product_0077_percent_grid(
	renderer: &mut iced::Renderer,
	size: Size<u32>,
	tick_label_text: plot::chrome::PlotRgba,
) -> Capture {
	let kernel = product_0077_percent_grid();
	let settings = product_0077_settings(tick_label_text);
	let resolved = settings.resolve();
	let bounds = Rectangle::with_size(Size::new(size.width as f32, size.height as f32));
	let widget = PlotWidget::new(
		&kernel,
		String::new(),
		0.0,
		settings,
		0,
		false,
		PlotRenderLayer::Full,
	);
	render_plot_and_overlay(
		renderer,
		size,
		|backend| widget.render_plot_layer(backend, bounds),
		|backend| widget.render_overlay_layer(backend, bounds),
	)
}

fn product_0077_gutters(size: Size<u32>) -> (Rectangle, Rectangle) {
	let resolved =
		product_0077_settings(ResolvedPlotSettings::default().chrome.tick_label_text).resolve();
	let layout = PlotLayout::Cartesian {
		x_range: (0.0, 100.0),
		y_range: (0.0, 100.0),
		x_axis_type: AxisType::Percent,
		y_axis_type: AxisType::Percent,
		x_temporal_domain: None,
		y_temporal_domain: None,
	};
	let requirements = BandRequirements::from_layout(&layout, &resolved);
	let tiled = VerticalGuideBands::resolve_with_requirements_and_x_label(
		"",
		None,
		&requirements,
		false,
		&resolved,
	)
	.grid_tiled_area(
		Rect::from_iced_frame(Rectangle::with_size(Size::new(
			size.width as f32,
			size.height as f32,
		))),
		&resolved,
	);
	let tiled_x = tiled.x();
	let tiled_y = tiled.y();
	let tiled_width = tiled.width();
	let tiled_height = tiled.height();
	let gap = 12.0;
	let cell_width = (tiled_width - gap) / 2.0;
	let cell_height = (tiled_height - gap) / 2.0;
	(
		Rectangle::new(
			Point::new(tiled_x + cell_width, 0.0),
			Size::new(gap, size.height as f32),
		),
		Rectangle::new(
			Point::new(0.0, tiled_y + cell_height),
			Size::new(size.width as f32, gap),
		),
	)
}

fn product_0077_differing_pixels(left: &Capture, right: &Capture, bounds: Rectangle) -> usize {
	let x0 = bounds.x.floor().max(0.0) as u32;
	let y0 = bounds.y.floor().max(0.0) as u32;
	let x1 = (bounds.x + bounds.width).ceil().max(0.0) as u32;
	let y1 = (bounds.y + bounds.height).ceil().max(0.0) as u32;
	let mut count = 0;
	for y in y0.min(left.height)..y1.min(left.height) {
		for x in x0.min(left.width)..x1.min(left.width) {
			count += usize::from(color_distance(left.pixel(x, y), right.pixel(x, y)) >= 12);
		}
	}
	count
}

fn draw_overlap(frame: &mut canvas::Frame) {
	let size = frame.size();
	let mut backend = IcedBackend { frame };
	backend.fill_rectangle(
		Point::new(size.width * 0.45, size.height * 0.40),
		Size::new(size.width * 0.45, size.height * 0.45),
		Color::from_rgb8(BLUE[0], BLUE[1], BLUE[2]),
	);
	backend.with_clip(
		Rectangle::new(
			Point::new(size.width * 0.10, size.height * 0.10),
			Size::new(size.width * 0.55, size.height * 0.55),
		),
		&mut |child| {
			child.fill_rectangle(
				Point::new(size.width * 0.10, size.height * 0.10),
				Size::new(size.width * 0.55, size.height * 0.55),
				Color::from_rgb8(RED[0], RED[1], RED[2]),
			);
		},
	);
}

fn overlap_winner(capture: &Capture) -> &'static str {
	let overlap = capture.point(0.52, 0.52);
	let red = near(overlap, RED, 12);
	let blue = near(overlap, BLUE, 12);
	assert_ne!(
		red, blue,
		"PRODUCT-0057 overlap region must identify exactly one compositing winner: {overlap:?}"
	);
	if red { "clipped child" } else { "parent fill" }
}

fn draw_clip(frame: &mut canvas::Frame, clipped: bool) {
	let size = frame.size();
	let draw_transformed = &mut |backend: &mut dyn PlotBackend| {
		backend.translate(Vector::new(size.width * 0.08, size.height * 0.06));
		backend.fill_rectangle(
			Point::new(size.width * 0.05, size.height * 0.05),
			Size::new(size.width * 0.85, size.height * 0.85),
			Color::from_rgb8(GREEN[0], GREEN[1], GREEN[2]),
		);
	};
	let mut backend = IcedBackend { frame };
	if clipped {
		backend.with_clip(
			Rectangle::new(
				Point::new(size.width * 0.25, size.height * 0.25),
				Size::new(size.width * 0.50, size.height * 0.50),
			),
			draw_transformed,
		);
	} else {
		backend.with_save(draw_transformed);
	}
}

fn draw_diagonal(frame: &mut canvas::Frame) {
	let size = frame.size();
	let path = Path::new(|builder| {
		builder.move_to(Point::new(size.width * 0.15, size.height * 0.80));
		builder.line_to(Point::new(size.width * 0.85, size.height * 0.20));
	});
	frame.stroke(
		&path,
		Stroke::default().with_color(Color::WHITE).with_width(11.0),
	);
}

fn draw_integer_rectangle(frame: &mut canvas::Frame) {
	frame.fill_rectangle(Point::new(64.0, 64.0), Size::new(128.0, 96.0), Color::WHITE);
}

const PRODUCT_0101_SIZE: Size<u32> = Size::new(256, 192);
const PRODUCT_0101_CENTER: Point = Point::new(128.0, 112.0);
const PRODUCT_0101_RADIUS: f32 = 80.0;
const PRODUCT_0101_START: f32 = -2.65;
const PRODUCT_0101_END: f32 = 0.85;

fn product_0101_old_arc(
	builder: &mut canvas::path::Builder,
	center: Point,
	radius: f32,
	start: f32,
	end: f32,
) {
	builder.arc(canvas::path::Arc {
		center,
		radius,
		start_angle: iced::Radians(start),
		end_angle: iced::Radians(end),
	});
}

fn draw_product_0101_old_pie(frame: &mut canvas::Frame) {
	let path = Path::new(|builder| {
		builder.move_to(PRODUCT_0101_CENTER);
		product_0101_old_arc(
			builder,
			PRODUCT_0101_CENTER,
			PRODUCT_0101_RADIUS,
			PRODUCT_0101_START,
			PRODUCT_0101_END,
		);
		builder.close();
	});
	frame.fill(&path, Color::WHITE);
}

fn draw_product_0101_joined_pie(frame: &mut canvas::Frame) {
	let mut backend = IcedBackend { frame };
	backend.fill_path(
		&|builder| {
			builder.move_to(PRODUCT_0101_CENTER);
			builder.arc_to(
				PRODUCT_0101_CENTER,
				PRODUCT_0101_RADIUS,
				PRODUCT_0101_START,
				PRODUCT_0101_END,
			);
			builder.close();
		},
		Color::WHITE,
	);
}

fn draw_product_0101_kurbo_chord_pie(frame: &mut canvas::Frame, tolerance: f64) {
	let path = Path::new(|builder| {
		let start = Point::new(
			PRODUCT_0101_CENTER.x + PRODUCT_0101_RADIUS * PRODUCT_0101_START.cos(),
			PRODUCT_0101_CENTER.y + PRODUCT_0101_RADIUS * PRODUCT_0101_START.sin(),
		);
		builder.move_to(start);
		let arc = kurbo::Arc::new(
			(PRODUCT_0101_CENTER.x as f64, PRODUCT_0101_CENTER.y as f64),
			(PRODUCT_0101_RADIUS as f64, PRODUCT_0101_RADIUS as f64),
			PRODUCT_0101_START as f64,
			(PRODUCT_0101_END - PRODUCT_0101_START) as f64,
			0.0,
		);
		for element in arc.append_iter(tolerance) {
			if let kurbo::PathEl::CurveTo(one, two, end) = element {
				builder.bezier_curve_to(
					Point::new(one.x as f32, one.y as f32),
					Point::new(two.x as f32, two.y as f32),
					Point::new(end.x as f32, end.y as f32),
				);
			}
		}
		builder.close();
	});
	frame.fill(&path, Color::WHITE);
}

fn draw_product_0101_annulus(frame: &mut canvas::Frame, connected: bool) {
	let center = Point::new(128.0, 130.0);
	let start = std::f32::consts::PI;
	let end = 1.7 * std::f32::consts::PI;
	if connected {
		let mut backend = IcedBackend { frame };
		backend.fill_path(
			&|builder| {
				builder.arc_to(center, 80.0, start, end);
				builder.arc_to(center, 48.0, end, start);
				builder.close();
			},
			Color::WHITE,
		);
	} else {
		let path = Path::new(|builder| {
			product_0101_old_arc(builder, center, 80.0, start, end);
			product_0101_old_arc(builder, center, 48.0, end, start);
			builder.close();
		});
		frame.fill(&path, Color::WHITE);
	}
}

fn product_0101_differing_pixels(left: &Capture, right: &Capture) -> usize {
	left.rgba
		.chunks_exact(4)
		.zip(right.rgba.chunks_exact(4))
		.filter(|(left, right)| left != right)
		.count()
}

fn render_table(
	renderer: &mut iced::Renderer,
	size: Size<u32>,
	column_width: Option<f32>,
) -> (Capture, Product0057TableEvidenceGeometry) {
	reset(renderer, size);
	let data_frame = polars::df!("margin" => [1_i64]).expect("PRODUCT-0057 table fixture");
	let bounds = Rectangle::with_size(Size::new(size.width as f32, size.height as f32));
	let geometry = draw_product_0057_table_evidence(renderer, &data_frame, column_width, bounds);
	(screenshot(renderer, size), geometry)
}

fn assert_table_fit(capture: &Capture, geometry: &Product0057TableEvidenceGeometry) -> usize {
	let text_left = geometry.row_number_width + 8.0;
	let column_end = geometry.row_number_width + geometry.column_width;
	let ink = capture.luminous_pixels(Rectangle::new(
		Point::new(text_left, 0.0),
		Size::new(
			(column_end - text_left - 2.0).max(1.0),
			geometry.header_height,
		),
	));
	assert!(
		ink > 40,
		"PRODUCT-0057 accepted table header must realize glyph ink"
	);
	let second_line = capture.luminous_pixels(Rectangle::new(
		Point::new(text_left, geometry.header_height * 0.78),
		Size::new(
			(column_end - text_left - 2.0).max(1.0),
			geometry.header_height * 0.22,
		),
	));
	assert_eq!(
		second_line, 0,
		"PRODUCT-0057 accepted header must leave the second-line region empty"
	);
	let neighbor = capture.luminous_pixels(Rectangle::new(
		Point::new(column_end + 3.0, 0.0),
		Size::new(24.0, geometry.header_height),
	));
	assert_eq!(
		neighbor, 0,
		"PRODUCT-0057 accepted header must not paint the neighboring-cell region"
	);
	ink
}

fn main() {
	tokio::runtime::Builder::new_multi_thread()
		.enable_all()
		.build()
		.expect("PRODUCT-0057 evidence runtime")
		.block_on(product_0057_renderer_oracle_licenses_are_discriminating());
}

async fn product_0057_renderer_oracle_licenses_are_discriminating() {
	product_0132_stage_0(&Gpu::acquire().await).await;

	let host = std::fs::read_to_string("/etc/hostname")
		.expect("PRODUCT-0057: read render-evidence host identity")
		.trim()
		.to_owned();
	let gpu = Gpu::acquire().await;
	assert_eq!(
		gpu.info.backend,
		wgpu::Backend::Vulkan,
		"PRODUCT-0057 semantic thresholds require the Vulkan backend"
	);
	assert!(
		matches!(
			gpu.info.device_type,
			wgpu::DeviceType::IntegratedGpu | wgpu::DeviceType::DiscreteGpu
		),
		"PRODUCT-0057 requires a hardware adapter, got {:?}",
		gpu.info.device_type
	);
	println!(
		"PRODUCT_0057 adapter host={host} name={:?} vendor={:#06x} device={:#06x} device_type={:?} backend={:?} driver={:?} driver_info={:?}",
		gpu.info.name,
		gpu.info.vendor,
		gpu.info.device,
		gpu.info.device_type,
		gpu.info.backend,
		gpu.info.driver,
		gpu.info.driver_info,
	);

	let base = Size::new(256, 192);
	let large = Size::new(448, 320);
	let mut msaa = gpu.renderer(Some(Antialiasing::MSAAx4)).await;
	let mut no_aa = gpu.renderer(None).await;
	product_0159_mark_batch(&mut msaa);
	let product_0117_sample = "International partner enablement";
	let product_0117_generic =
		render_product_0117_font_probe(&mut msaa, Font::DEFAULT, product_0117_sample);
	let product_0117_pinned =
		render_product_0117_font_probe(&mut msaa, fonts::PLOT_TEXT_FONT, product_0117_sample);
	assert_eq!(
		product_0117_generic.rgba, product_0117_pinned.rgba,
		"PRODUCT-0117 development host's generic sans must bank as the same Noto glyph capture as Polariton Plot Sans"
	);
	let product_0117_cjk = render_product_0117_font_probe(&mut msaa, fonts::PLOT_TEXT_FONT, "雪");
	assert!(
		product_0117_cjk.luminous_pixels(Rectangle::with_size(Size::new(640.0, 160.0))) > 40,
		"PRODUCT-0117 pinned plot font must render the CJK fallback probe"
	);
	println!(
		"PRODUCT_0117 plot_font disposition=production generic_noto_parity=exact cjk_fallback_luminous={}",
		product_0117_cjk.luminous_pixels(Rectangle::with_size(Size::new(640.0, 160.0)))
	);
	let product_0101_old_msaa =
		render_frame(&mut msaa, PRODUCT_0101_SIZE, draw_product_0101_old_pie);
	let product_0101_old_none =
		render_frame(&mut no_aa, PRODUCT_0101_SIZE, draw_product_0101_old_pie);
	let product_0101_joined_msaa =
		render_frame(&mut msaa, PRODUCT_0101_SIZE, draw_product_0101_joined_pie);
	let product_0101_joined_none =
		render_frame(&mut no_aa, PRODUCT_0101_SIZE, draw_product_0101_joined_pie);
	let full_bounds = Rectangle::with_size(Size::new(256.0, 192.0));
	assert_eq!(
		product_0101_old_none.luminous_pixels(full_bounds),
		12_336,
		"PRODUCT-0101 Stage 0 chord-bounded pie bank"
	);
	assert_eq!(
		product_0101_joined_none.luminous_pixels(full_bounds),
		11_191,
		"PRODUCT-0101 repaired pie is the smaller centre-bounded sector"
	);
	assert_eq!(
		product_0101_differing_pixels(&product_0101_old_msaa, &product_0101_joined_msaa),
		1_301
	);
	assert_eq!(
		product_0101_differing_pixels(&product_0101_old_none, &product_0101_joined_none),
		1_147
	);

	let mut chord_msaa = Vec::new();
	let mut chord_none = Vec::new();
	for tolerance in [1.0, 0.25, 0.1, 0.01, 0.001] {
		chord_msaa.push(render_frame(&mut msaa, PRODUCT_0101_SIZE, |frame| {
			draw_product_0101_kurbo_chord_pie(frame, tolerance)
		}));
		chord_none.push(render_frame(&mut no_aa, PRODUCT_0101_SIZE, |frame| {
			draw_product_0101_kurbo_chord_pie(frame, tolerance)
		}));
	}
	assert!(
		chord_msaa[..4]
			.windows(2)
			.all(|pair| pair[0].rgba == pair[1].rgba)
	);
	assert!(
		chord_none[..4]
			.windows(2)
			.all(|pair| pair[0].rgba == pair[1].rgba)
	);
	assert_ne!(chord_msaa[3].rgba, chord_msaa[4].rgba);
	assert_ne!(chord_none[3].rgba, chord_none[4].rgba);
	assert_eq!(
		product_0101_differing_pixels(&product_0101_old_msaa, &chord_msaa[2]),
		76
	);
	assert_eq!(
		product_0101_differing_pixels(&product_0101_old_none, &chord_none[2]),
		25
	);

	let product_0101_old_annulus = render_frame(&mut msaa, PRODUCT_0101_SIZE, |frame| {
		draw_product_0101_annulus(frame, false)
	});
	let product_0101_joined_annulus = render_frame(&mut msaa, PRODUCT_0101_SIZE, |frame| {
		draw_product_0101_annulus(frame, true)
	});
	assert!(
		product_0101_differing_pixels(&product_0101_old_annulus, &product_0101_joined_annulus)
			> 500,
		"PRODUCT-0101 annular-sector repair must be visually discriminating"
	);
	let annulus_center = Point::new(128.0, 130.0);
	let annulus_mid = 1.35 * std::f32::consts::PI;
	let point_at = |radius: f32| {
		product_0101_joined_annulus.pixel(
			(annulus_center.x + radius * annulus_mid.cos()).round() as u32,
			(annulus_center.y + radius * annulus_mid.sin()).round() as u32,
		)
	};
	assert!(near(point_at(64.0), WHITE, 12), "annular band interior");
	assert!(
		near(point_at(30.0), BACKGROUND, 12),
		"annular hole remains empty"
	);
	println!(
		"PRODUCT_0101 arc_contract disposition=automated pie_luminous=12336/11191 joined_diff=1301/1147 decomposition_diff=76/25 tolerance=0.1 annulus=connected"
	);

	let product_0115_size = Size::new(960, 900);
	let (product_0115_zero, product_0115_bounds) =
		render_product_0115_rotation(&mut msaa, product_0115_size, 0.0);
	let (product_0115_positive, _) =
		render_product_0115_rotation(&mut msaa, product_0115_size, 45.0);
	let (product_0115_negative, _) =
		render_product_0115_rotation(&mut msaa, product_0115_size, -45.0);
	assert_ne!(
		product_0115_zero.rgba, product_0115_positive.rgba,
		"PRODUCT-0115 positive rotation must reach the production renderer"
	);
	assert_ne!(
		product_0115_zero.rgba, product_0115_negative.rgba,
		"PRODUCT-0115 negative rotation must reach the production renderer"
	);
	for (name, capture) in [
		("zero", &product_0115_zero),
		("positive", &product_0115_positive),
		("negative", &product_0115_negative),
	] {
		assert_eq!(
			exterior_luminous_pixels(capture, product_0115_bounds),
			0,
			"PRODUCT-0115 {name} figure clip must contain the axis overlay"
		);
	}
	println!(
		"PRODUCT_0115 x_tick_rotation disposition=automated zero/positive/negative=contained/contained/contained"
	);
	// PRODUCT-0131 Stage 0: a 19-character timestamp label reserves a bottom band
	// that outgrows its own figure. `plot_area` clamps the remainder at zero, so
	// the plot silently disappears while ink still reaches the reserved band.
	// The 480x360 / 36-degree row is the discriminating nonzero control: without
	// it the collapse rows would read zero in both the present and any repaired
	// state, and prove nothing.
	for (size_index, size) in [Size::new(480, 360), Size::new(320, 240)]
		.into_iter()
		.enumerate()
	{
		for (rotation_index, rotation) in [36.0, 45.0, 90.0].into_iter().enumerate() {
			let (kernel, settings) = product_temporal_count_fixture(rotation);
			let (capture, figure_bounds, plot_area) =
				render_product_kernel_rotation(&mut msaa, size, &kernel, settings);
			let expected_height = figure_bounds.height * VerticalGuideBands::PLOT_MIN_SHARE;
			let pre_bound_height = PRODUCT_0131_PRE_BOUND_HEIGHTS[size_index][rotation_index];
			let bottom_band = Rectangle::new(
				Point::new(figure_bounds.x, plot_area.y + plot_area.height),
				Size::new(
					figure_bounds.width,
					(figure_bounds.y + figure_bounds.height - (plot_area.y + plot_area.height))
						.max(0.0),
				),
			);
			let bottom_ink = capture.luminous_pixels(bottom_band);
			assert!(
				expected_height + 0.001 >= pre_bound_height,
				"PRODUCT-0131 bounded plot must not lose extent: pre-bound={pre_bound_height:.1}, bounded={expected_height:.1}",
			);
			assert!(
				(plot_area.height - expected_height).abs() < 0.1,
				"PRODUCT-0131 {}x{} at {rotation} degrees banks a {expected_height} px plot, got {:.1}; pre-bound baseline was {pre_bound_height:.1} px",
				size.width,
				size.height,
				plot_area.height,
			);
			assert!(
				bottom_ink > 0,
				"PRODUCT-0131 {}x{} at {rotation} degrees must still draw tick ink into the reserved band; \
				 a zero reading would make the collapse rows vacuous",
				size.width,
				size.height,
			);
			println!(
				"PRODUCT_0130 temporal_count_rotation size={}x{} rotation={rotation} plot_height={:.1} bottom_ink={bottom_ink}",
				size.width, size.height, plot_area.height,
			);
		}
	}
	let (product_0116_vertical, product_0116_bounds) =
		render_product_0116_rotation(&mut msaa, product_0115_size, 90.0);
	let product_0116_short_top =
		product_0116_tick_run_top(&product_0116_vertical, product_0116_bounds, 1);
	let product_0116_long_top =
		product_0116_tick_run_top(&product_0116_vertical, product_0116_bounds, 2);
	assert!(
		product_0116_short_top.abs_diff(product_0116_long_top) <= 4,
		"PRODUCT-0116 production rotation must not turn label length into a vertical cascade: short={product_0116_short_top}, long={product_0116_long_top}"
	);
	println!(
		"PRODUCT_0116 estimation_never_moves_ink disposition=production short_top={product_0116_short_top} long_top={product_0116_long_top}"
	);
	let product_0127_size = Size::new(900, 480);
	let (product_0127_capture, product_0127_plot_area, product_0127_left, product_0127_title_x) =
		render_product_0127_selected_ticks(&mut msaa, product_0127_size);
	assert_eq!(
		(
			product_0127_left,
			product_0127_title_x,
			product_0127_plot_area.x
		),
		(105.0, 85.0, 105.0),
		"PRODUCT-0127 selected (0, 100) ticks restore the banked 105 px left band and 85 px title placement"
	);
	assert_eq!(
		product_0127_y_tick_ink_distances(&product_0127_capture, product_0127_plot_area),
		(12, 45),
		"PRODUCT-0127 changes the reservation only; realized y-tick ink remains at its banked 12..45 px distances from the plot edge"
	);
	println!(
		"PRODUCT_0127 selected_tick_requirement disposition=production left/title/tick_ink=105/85/12..45"
	);

	let product_0077_size = Size::new(800, 600);
	let product_0077_sentinel = plot::chrome::PlotRgba::opaque(1.0, 0.0, 1.0);
	let product_0077_background = ResolvedPlotSettings::default().chrome.canvas_background;
	let product_0077_visible =
		render_product_0077_percent_grid(&mut msaa, product_0077_size, product_0077_sentinel);
	let product_0077_hidden =
		render_product_0077_percent_grid(&mut msaa, product_0077_size, product_0077_background);
	let (product_0077_vertical_gutter, product_0077_horizontal_gutter) =
		product_0077_gutters(product_0077_size);
	let product_0077_vertical_spill = product_0077_differing_pixels(
		&product_0077_visible,
		&product_0077_hidden,
		product_0077_vertical_gutter,
	);
	let product_0077_horizontal_spill = product_0077_differing_pixels(
		&product_0077_visible,
		&product_0077_hidden,
		product_0077_horizontal_gutter,
	);
	assert!(
		PRODUCT_0077_BANKED_VERTICAL_SPILL > 0 && PRODUCT_0077_BANKED_HORIZONTAL_SPILL > 0,
		"PRODUCT-0077 Stage 0 must bank a discriminating nonzero control"
	);
	assert_eq!(
		(product_0077_vertical_spill, product_0077_horizontal_spill),
		(0, 0),
		"PRODUCT-0077 bound-coincident tick text must contribute no realized glyph ink to either gutter"
	);
	println!(
		"PRODUCT_0077 facet_tick_containment disposition=automated banked_vertical/horizontal={PRODUCT_0077_BANKED_VERTICAL_SPILL}/{PRODUCT_0077_BANKED_HORIZONTAL_SPILL} accepted_vertical/horizontal={product_0077_vertical_spill}/{product_0077_horizontal_spill}"
	);

	let product_0080_size = Size::new(500, 300);
	let product_0080_style = ResolvedPlotSettings::default().mark_style;
	let product_0080_accepted =
		render_product_0080_histogram(&mut msaa, product_0080_size, product_0080_style.separator);
	let product_0080_banked =
		render_product_0080_histogram(&mut msaa, product_0080_size, product_0080_style.fill);
	let product_0080_accepted_separator =
		product_0080_separator_difference_pixels(&product_0080_accepted, &product_0080_banked);
	assert!(
		product_0080_accepted_separator >= 200,
		"PRODUCT-0080 resolved separator must replace same-fill control pixels; got {product_0080_accepted_separator} pixels"
	);
	let product_0080_adjacent_fill =
		product_0080_filled_pixels_beside_separator(&product_0080_accepted, &product_0080_banked);
	assert!(
		product_0080_adjacent_fill >= 100,
		"PRODUCT-0080 separator must divide two retained fill bodies; got {product_0080_adjacent_fill} shared neighbors"
	);
	println!(
		"PRODUCT_0080 histogram_separator disposition=automated same_fill_replaced={product_0080_accepted_separator} adjacent_fill={product_0080_adjacent_fill}"
	);

	let product_0083_size = Size::new(500, 300);
	let product_0083_style = ResolvedPlotSettings::default().mark_style;
	let (product_0083_accepted, product_0083_census) =
		render_product_0083_box(&mut msaa, product_0083_size, product_0083_style.fill_detail);
	let (product_0083_banked, product_0083_banked_census) =
		render_product_0083_box(&mut msaa, product_0083_size, product_0083_style.fill);
	assert_eq!(product_0083_census, product_0083_banked_census);
	let product_0083_banked_detail =
		product_0083_paper_pixels(&product_0083_banked, product_0083_banked_census);
	let product_0083_accepted_detail =
		product_0083_paper_pixels(&product_0083_accepted, product_0083_census);
	assert_eq!(
		product_0083_banked_detail, PRODUCT_0083_BANKED_MEDIAN_DETAIL,
		"PRODUCT-0083 Stage 0 must bank the invisible median as a discriminating zero-paper control"
	);
	assert!(
		product_0083_accepted_detail >= 200,
		"PRODUCT-0083 fill-detail median must realize paper-counterpart ink in its outline-free interior band"
	);
	println!(
		"PRODUCT_0083 median_detail disposition=automated banked/accepted={product_0083_banked_detail}/{product_0083_accepted_detail}"
	);

	let percent_size = Size::new(500, 360);
	let (percent_area, percent_area_settings, percent_area_fills) =
		product_0072_percent_area_fixture();
	let percent_area_under = render_product_0068_cartesian_mark(
		&mut msaa,
		percent_size,
		&percent_area,
		&percent_area_settings,
		false,
	);
	let percent_area_overlay = render_product_0068_cartesian_mark(
		&mut msaa,
		percent_size,
		&percent_area,
		&percent_area_settings,
		true,
	);
	let area_occluded = assert_product_0072_occlusion(
		"fully positive area",
		&percent_area_under,
		&percent_area_overlay,
		percent_area_fills,
	);
	let mut no_boundary = percent_area_settings.resolve();
	no_boundary.mark_style.decoration = plot::chrome::PlotRgba::from_iced(Color::TRANSPARENT);
	let percent_area_without_boundary = render_product_0068_cartesian_mark(
		&mut msaa,
		percent_size,
		&percent_area,
		&PlotSettings::from_resolved(no_boundary),
		false,
	);
	let area_boundary = percent_area_under
		.rgba
		.chunks_exact(4)
		.zip(percent_area_without_boundary.rgba.chunks_exact(4))
		.filter(|(boundary, plain)| {
			color_distance(
				[boundary[0], boundary[1], boundary[2]],
				[plain[0], plain[1], plain[2]],
			) >= 20
		})
		.count();
	assert!(
		area_boundary >= 100,
		"PRODUCT-0072 percent area keeps an above-mark resolved boundary; got {area_boundary} discriminating pixels"
	);

	let (percent_wall, percent_wall_settings, percent_bar_fills) =
		product_0072_percent_bar_fixture(true);
	let percent_wall_under = render_product_0068_cartesian_mark(
		&mut msaa,
		percent_size,
		&percent_wall,
		&percent_wall_settings,
		false,
	);
	let percent_wall_overlay = render_product_0068_cartesian_mark(
		&mut msaa,
		percent_size,
		&percent_wall,
		&percent_wall_settings,
		true,
	);
	let wall_occluded = assert_product_0072_occlusion(
		"gapless full-width bar wall",
		&percent_wall_under,
		&percent_wall_overlay,
		percent_bar_fills,
	);

	let (percent_gaps, percent_gap_settings, _) = product_0072_percent_bar_fixture(false);
	let percent_gaps_with_grid = render_product_0068_cartesian_mark(
		&mut msaa,
		percent_size,
		&percent_gaps,
		&percent_gap_settings,
		false,
	);
	let mut no_grid = percent_gap_settings.resolve();
	no_grid.show_x_major_grid = false;
	no_grid.show_y_major_grid = false;
	let percent_gaps_without_grid = render_product_0068_cartesian_mark(
		&mut msaa,
		percent_size,
		&percent_gaps,
		&PlotSettings::from_resolved(no_grid),
		false,
	);
	let exposed_grid = percent_gaps_with_grid
		.rgba
		.chunks_exact(4)
		.zip(percent_gaps_without_grid.rgba.chunks_exact(4))
		.filter(|(grid, canvas)| {
			let grid = [grid[0], grid[1], grid[2]];
			let canvas = [canvas[0], canvas[1], canvas[2]];
			color_distance(grid, canvas) >= 20 && near(canvas, BACKGROUND, 12)
		})
		.count();
	assert!(
		exposed_grid >= 100,
		"PRODUCT-0072 bar gaps must retain the same real grid over exposed canvas; got {exposed_grid} pixels"
	);
	println!(
		"PRODUCT_0072 percent_strata disposition=automated area_occluded={area_occluded} area_boundary={area_boundary} wall_occluded={wall_occluded} exposed_grid={exposed_grid}"
	);

	let bar_size = Size::new(600, 400);
	let bar_under = render_product_0068_bar(&mut msaa, bar_size, false, true);
	let bar_overlay = render_product_0068_bar(&mut msaa, bar_size, true, true);
	let bar_without_grid = render_product_0068_bar(&mut msaa, bar_size, false, false);
	let cyan = [1, 205, 254];
	let (crossing_x, crossing_y) = (0..bar_under.height)
		.flat_map(|y| (0..bar_under.width).map(move |x| (x, y)))
		.find(|&(x, y)| {
			near(bar_under.pixel(x, y), cyan, 12)
				&& color_distance(bar_overlay.pixel(x, y), cyan) >= 20
		})
		.expect("PRODUCT-0068 grid must cross a resolved cyan bar");
	let under_mark = bar_under.pixel(crossing_x, crossing_y);
	let overlay_mark = bar_overlay.pixel(crossing_x, crossing_y);
	assert!(
		near(under_mark, cyan, 12),
		"PRODUCT-0068 grid-under must preserve the exact resolved bar colour: {under_mark:?}"
	);
	assert!(
		color_distance(overlay_mark, cyan) >= 20,
		"PRODUCT-0068 overlay control must visibly tint the same bar pixel: {overlay_mark:?}"
	);
	let (gap_grid, gap_canvas) = (0..bar_under.height)
		.flat_map(|y| (0..bar_under.width).map(move |x| (x, y)))
		.find_map(|(x, y)| {
			let grid = bar_under.pixel(x, y);
			let canvas = bar_without_grid.pixel(x, y);
			(near(canvas, BACKGROUND, 12) && color_distance(grid, canvas) >= 20)
				.then_some((grid, canvas))
		})
		.expect("PRODUCT-0068 category gaps must expose a grid pixel over canvas");
	assert!(
		color_distance(gap_grid, gap_canvas) >= 20,
		"PRODUCT-0068 category gap must retain a real gridline: grid={gap_grid:?} canvas={gap_canvas:?}"
	);
	for expected in [[1, 205, 254], [229, 9, 20], [255, 32, 121]] {
		assert!(
			bar_under.rgba.chunks_exact(4).any(|pixel| near(
				[pixel[0], pixel[1], pixel[2]],
				expected,
				12
			)),
			"PRODUCT-0068 fixture must realize every saturated series colour {expected:?}"
		);
	}
	println!(
		"PRODUCT_0068 bar_strata disposition=automated crossing=({crossing_x},{crossing_y}) under_mark={under_mark:?} overlay_mark={overlay_mark:?} gap_grid={gap_grid:?} gap_canvas={gap_canvas:?}"
	);
	let cartesian_size = Size::new(400, 300);
	let cartesian_settings = product_0068_cartesian_settings();
	let cyan_color = Color::from_rgb8(cyan[0], cyan[1], cyan[2]);
	let line = product_0068_line_fixture(cyan_color);
	let line_under = render_product_0068_cartesian_mark(
		&mut msaa,
		cartesian_size,
		&line,
		&cartesian_settings,
		false,
	);
	let line_overlay = render_product_0068_cartesian_mark(
		&mut msaa,
		cartesian_size,
		&line,
		&cartesian_settings,
		true,
	);
	let line_counts =
		assert_product_0068_cartesian_mark_control("line", &line_under, &line_overlay, cyan);
	let scatter = product_0068_scatter_fixture(cyan_color);
	let scatter_under = render_product_0068_cartesian_mark(
		&mut msaa,
		cartesian_size,
		&scatter,
		&cartesian_settings,
		false,
	);
	let scatter_overlay = render_product_0068_cartesian_mark(
		&mut msaa,
		cartesian_size,
		&scatter,
		&cartesian_settings,
		true,
	);
	let scatter_counts = assert_product_0068_cartesian_mark_control(
		"scatter",
		&scatter_under,
		&scatter_overlay,
		cyan,
	);
	println!(
		"PRODUCT_0068 cartesian_controls disposition=automated line_intersection/grid={}/{} scatter_intersection/grid={}/{}",
		line_counts.0, line_counts.1, scatter_counts.0, scatter_counts.1,
	);
	let (stacked_area, stacked_area_settings, stacked_area_fills) =
		product_0068_stacked_area_fixture();
	let stacked_area_under = render_product_0068_cartesian_mark(
		&mut msaa,
		cartesian_size,
		&stacked_area,
		&stacked_area_settings,
		false,
	);
	let stacked_area_overlay = render_product_0068_cartesian_mark(
		&mut msaa,
		cartesian_size,
		&stacked_area,
		&stacked_area_settings,
		true,
	);
	let stacked_area_counts = assert_product_0068_stacked_area_control(
		&stacked_area_under,
		&stacked_area_overlay,
		stacked_area_fills,
	);
	println!(
		"PRODUCT_0068 stacked_area disposition=automated exact_intersections={} headroom_grid={}",
		stacked_area_counts.0, stacked_area_counts.1,
	);
	if let Some(path) = std::env::var_os("PRODUCT_0068_STAGE0_PNG") {
		let exhibit = render_frame(
			&mut msaa,
			Size::new(1500, 1050),
			draw_product_0068_strata_exhibit,
		);
		write_product_0068_png(&exhibit, std::path::Path::new(&path));
		println!(
			"PRODUCT_0068 Stage 0 exhibit={}",
			std::path::Path::new(&path).display()
		);
	}

	let overlap = render_frame(&mut msaa, base, draw_overlap);
	let overlap_repeat = render_frame(&mut msaa, base, draw_overlap);
	let overlap_large = render_frame(&mut msaa, large, draw_overlap);
	for capture in [&overlap, &overlap_repeat, &overlap_large] {
		assert_region(capture, [0.18, 0.18, 0.28, 0.28], RED, "child-only control");
		assert_region(
			capture,
			[0.72, 0.70, 0.82, 0.80],
			BLUE,
			"parent-only control",
		);
	}
	let winner = overlap_winner(&overlap);
	assert_eq!(winner, overlap_winner(&overlap_repeat));
	assert_eq!(winner, overlap_winner(&overlap_large));
	println!(
		"PRODUCT_0057 overlap disposition=automated repeat_bytes={} scale_stable=true winner={winner:?}",
		overlap.rgba == overlap_repeat.rgba,
	);

	let clipped = render_frame(&mut msaa, base, |frame| draw_clip(frame, true));
	let clipped_repeat = render_frame(&mut msaa, base, |frame| draw_clip(frame, true));
	let clipped_large = render_frame(&mut msaa, large, |frame| draw_clip(frame, true));
	let unclipped = render_frame(&mut msaa, base, |frame| draw_clip(frame, false));
	for capture in [&clipped, &clipped_repeat, &clipped_large] {
		assert_region(capture, [0.40, 0.40, 0.60, 0.60], GREEN, "clip interior");
		for outside in [
			[0.17, 0.45, 0.22, 0.55],
			[0.78, 0.45, 0.83, 0.55],
			[0.45, 0.17, 0.55, 0.22],
			[0.45, 0.78, 0.55, 0.83],
		] {
			assert_region(capture, outside, BACKGROUND, "clip exterior");
		}
	}
	for outside in [
		[0.17, 0.45, 0.22, 0.55],
		[0.78, 0.45, 0.83, 0.55],
		[0.45, 0.17, 0.55, 0.22],
		[0.45, 0.78, 0.55, 0.83],
	] {
		assert_region(
			&unclipped,
			outside,
			GREEN,
			"unclipped falsifiability control",
		);
	}
	println!(
		"PRODUCT_0057 clip disposition=automated repeat_bytes={} scale_stable=true controls=inside+four_edges+unclipped",
		clipped.rgba == clipped_repeat.rgba,
	);

	let diagonal_msaa = render_frame(&mut msaa, base, draw_diagonal);
	let diagonal_msaa_repeat = render_frame(&mut msaa, base, draw_diagonal);
	let diagonal_none = render_frame(&mut no_aa, base, draw_diagonal);
	let diagonal_msaa_large = render_frame(&mut msaa, large, draw_diagonal);
	let diagonal_none_large = render_frame(&mut no_aa, large, draw_diagonal);
	let msaa_intermediate = diagonal_msaa.intermediate_pixels();
	let msaa_repeat_intermediate = diagonal_msaa_repeat.intermediate_pixels();
	let none_intermediate = diagonal_none.intermediate_pixels();
	let msaa_intermediate_large = diagonal_msaa_large.intermediate_pixels();
	let none_intermediate_large = diagonal_none_large.intermediate_pixels();
	assert_eq!(
		none_intermediate, 0,
		"PRODUCT-0057 disabled-AA diagonal must have no intermediate blend run"
	);
	assert!(
		msaa_intermediate > 100,
		"PRODUCT-0057 MSAA diagonal must expose an intermediate blend run; got {msaa_intermediate}"
	);
	assert!(msaa_repeat_intermediate > 100);
	assert_eq!(none_intermediate_large, 0);
	assert!(msaa_intermediate_large > msaa_intermediate);
	let rect_msaa = render_frame(&mut msaa, base, draw_integer_rectangle);
	let rect_none = render_frame(&mut no_aa, base, draw_integer_rectangle);
	assert_eq!(rect_msaa.intermediate_pixels(), 0);
	assert_eq!(rect_none.intermediate_pixels(), 0);
	assert_eq!(
		rect_msaa.rgba, rect_none.rgba,
		"PRODUCT-0057 integer-device rectangle must remain nondiscriminating"
	);
	println!(
		"PRODUCT_0057 msaa disposition=automated repeat_bytes={} scale_stable=true intermediate_base={msaa_intermediate}/0 intermediate_large={msaa_intermediate_large}/0 integer_control=identical",
		diagonal_msaa.rgba == diagonal_msaa_repeat.rgba,
	);

	let (text, text_geometry) = render_table(&mut msaa, Size::new(240, 96), None);
	let (text_repeat, repeat_geometry) = render_table(&mut msaa, Size::new(240, 96), None);
	let (text_large, large_geometry) = render_table(&mut msaa, Size::new(420, 160), None);
	let (narrow, narrow_geometry) = render_table(&mut msaa, Size::new(240, 96), Some(28.0));
	let accepted_ink = assert_table_fit(&text, &text_geometry);
	let repeated_ink = assert_table_fit(&text_repeat, &repeat_geometry);
	let large_ink = assert_table_fit(&text_large, &large_geometry);
	let narrow_left = narrow_geometry.row_number_width + 8.0;
	let narrow_ink = narrow.luminous_pixels(Rectangle::new(
		Point::new(narrow_left, 0.0),
		Size::new(
			(narrow_geometry.column_width - 10.0).max(1.0),
			narrow_geometry.header_height,
		),
	));
	assert!(
		narrow_ink < accepted_ink,
		"PRODUCT-0057 narrow control must realize a distinguishable text extent: accepted={accepted_ink} narrow={narrow_ink}"
	);
	assert!(
		narrow_ink * 4 < accepted_ink * 3,
		"PRODUCT-0057 narrow control must be materially distinct"
	);
	println!(
		"PRODUCT_0057 text disposition=automated repeat_bytes={} scale_stable=true accepted_ink={accepted_ink}/{repeated_ink}/{large_ink} narrow_ink={narrow_ink} measured_width={:.2}",
		text.rgba == text_repeat.rgba,
		text_geometry.column_width,
	);
}

/// PRODUCT-0132 Stage 0. A rotated x-tick label is drawn two transforms deep
/// inside a clip; a rotated y-axis title is drawn once, unnested and unclipped.
/// This fixture renders both in one figure so the difference is a measurement
/// rather than an argument. The x-axis title is hidden so that bottom-band ink
/// is tick ink and nothing else -- its residual is what would let a naive
/// nonzero check pass.
fn product_0132_fixture(rotation: f32, y_title: bool) -> (LinePlotKernel, PlotSettings) {
	let (kernel, model) = product_temporal_count_fixture(rotation);
	let mut resolved = model.resolve();
	resolved.title = ResolvedPresence::Hide;
	resolved.subtitle = ResolvedPresence::Hide;
	resolved.x_label = ResolvedPresence::Hide;
	resolved.y_label = if y_title {
		ResolvedPresence::Show(std::sync::Arc::new("Active accounts".to_owned()))
	} else {
		ResolvedPresence::Hide
	};
	resolved.x_tick_rotation = TickRotation::new(rotation).expect("stage 0 angle");
	(kernel, PlotSettings::from_resolved(resolved))
}

/// Ink below the plot area: with the x-axis title hidden, this is rotated tick
/// text or nothing.
fn product_0132_tick_ink(renderer: &mut iced::Renderer, rotation: f32) -> usize {
	let (kernel, settings) = product_0132_fixture(rotation, false);
	let (capture, figure, plot) =
		render_product_kernel_rotation(renderer, Size::new(820, 520), &kernel, settings);
	capture.luminous_pixels(Rectangle::new(
		Point::new(figure.x, plot.y + plot.height),
		Size::new(
			figure.width,
			(figure.y + figure.height - (plot.y + plot.height)).max(0.0),
		),
	))
}

/// Ink left of the plot area, with and without the rotated y title. The
/// difference is the title's own ink, isolated without any geometry reasoning.
fn product_0132_y_title_ink(renderer: &mut iced::Renderer, rotation: f32) -> usize {
	let mut measured = [0usize; 2];
	for (index, y_title) in [true, false].into_iter().enumerate() {
		let (kernel, settings) = product_0132_fixture(rotation, y_title);
		let (capture, figure, plot) =
			render_product_kernel_rotation(renderer, Size::new(820, 520), &kernel, settings);
		measured[index] = capture.luminous_pixels(Rectangle::new(
			Point::new(figure.x, figure.y),
			Size::new((plot.x - figure.x).max(0.0), figure.height),
		));
	}
	measured[0].saturating_sub(measured[1])
}

async fn product_0132_stage_0(gpu: &Gpu) {
	let mut r = gpu.renderer(Some(Antialiasing::MSAAx4)).await;
	let unrotated_ticks = product_0132_tick_ink(&mut r, 0.0);
	let rotated_ticks_positive = product_0132_tick_ink(&mut r, 45.0);
	let rotated_ticks_negative = product_0132_tick_ink(&mut r, -45.0);
	let intermediate = [1.0_f32, 5.0, 24.0]
		.into_iter()
		.map(|angle| (angle, product_0132_tick_ink(&mut r, angle)))
		.collect::<Vec<_>>();
	let rotated_title = product_0132_y_title_ink(&mut r, 45.0);
	println!(
		"PRODUCT_0132 stage_0 disposition=production unrotated_ticks={unrotated_ticks} \
		 rotated_ticks=+45:{rotated_ticks_positive}/-45:{rotated_ticks_negative} \
			 rotated_y_title={rotated_title} intermediate={intermediate:?}"
	);

	// Positive control 1: unrotated tick text rasterizes, so the measurement
	// region and the threshold are both pointed at something real.
	assert_eq!(
		unrotated_ticks, PRODUCT_0132_BANKED_UNROTATED_TICK_INK,
		"PRODUCT-0132 the unrotated control must ink the band it measures"
	);
	assert!(
		unrotated_ticks > PRODUCT_0132_TICK_MARK_RESIDUAL * 10,
		"PRODUCT-0132 gate 1's threshold must sit far above the tick-mark residual"
	);
	// Positive control 2: rotated text rasterizes elsewhere in the same figure,
	// so the defect is the nested clipped path and not rotation as such.
	assert!(
		rotated_title > 100,
		"PRODUCT-0132 the rotated y title must rasterize, got {rotated_title}"
	);
	assert_eq!(
		(rotated_ticks_positive, rotated_ticks_negative),
		PRODUCT_0132_BANKED_ROTATED_TICK_INK,
		"PRODUCT-0132 rotated tick labels must rasterize in both signs"
	);
}

/// PRODUCT-0159 gates 2 and 3 on the production renderer. The batch and the
/// pre-cut compound path draw the same opaque marks; where marks do not
/// overlap the captures must be identical at scale 1 and 2, and where they
/// overlap every difference must sit within one physical pixel of a mark
/// boundary. A translucent pair composites per mark, darker at the overlap,
/// and the compound union is the negative control.
fn product_0159_mark_batch(renderer: &mut iced::Renderer) {
	let radius = 3.0_f32;
	let opaque = Color::from_rgb(0.85, 0.85, 0.85);
	let spaced: Vec<Point> = (0..8)
		.flat_map(|i| {
			(0..5).map(move |j| Point::new(20.0 + i as f32 * 24.0, 20.0 + j as f32 * 24.0))
		})
		.collect();
	let dense: Vec<Point> = (0..60)
		.flat_map(|i| {
			(0..30).map(move |j| Point::new(30.0 + i as f32 * 1.5, 30.0 + j as f32 * 1.5))
		})
		.collect();
	let render = |renderer: &mut iced::Renderer,
	              marks: &[Point],
	              scale: f32,
	              batched: bool,
	              color: Color| {
		let size = Size::new((220.0 * scale) as u32, (160.0 * scale) as u32);
		render_frame(renderer, size, |frame| {
			frame.scale(scale);
			let mut backend = IcedBackend { frame };
			if batched {
				backend.fill_marks(marks, radius, color);
			} else {
				backend.fill_path(
					&|builder| {
						for centre in marks {
							builder.circle(*centre, radius);
						}
					},
					color,
				);
			}
		})
	};
	let mut nonoverlap_identical = true;
	for scale in [1.0_f32, 2.0] {
		let batch = render(renderer, &spaced, scale, true, opaque);
		let compound = render(renderer, &spaced, scale, false, opaque);
		nonoverlap_identical &= batch.rgba == compound.rgba;
		assert!(
			batch.luminous_pixels(Rectangle::with_size(Size::new(
				batch.width as f32,
				batch.height as f32
			))) > 100,
			"positive control: the marks rasterized at scale {scale}"
		);
	}
	// Overlapping opaque marks: differences confined to one physical pixel of
	// a mark boundary, widened by lyon's flattening tolerance; never interior
	// or background.
	let mut worst_band_violation = 0usize;
	let mut differing = 0usize;
	let mut max_delta = 0u8;
	for scale in [1.0_f32, 2.0] {
		let batch = render(renderer, &dense, scale, true, opaque);
		let compound = render(renderer, &dense, scale, false, opaque);
		let band = 1.0 + 0.1 * scale;
		for y in 0..batch.height {
			for x in 0..batch.width {
				let a = batch.pixel(x, y);
				let b = compound.pixel(x, y);
				if a == b {
					continue;
				}
				differing += 1;
				max_delta = max_delta.max(
					a.into_iter()
						.zip(b)
						.map(|(a, b)| a.abs_diff(b))
						.max()
						.unwrap_or(0),
				);
				let px = (x as f32 + 0.5) / scale;
				let py = (y as f32 + 0.5) / scale;
				let nearest_edge = dense
					.iter()
					.map(|centre| ((px - centre.x).powi(2) + (py - centre.y).powi(2)).sqrt())
					.map(|distance| (distance - radius).abs() * scale)
					.fold(f32::INFINITY, f32::min);
				if nearest_edge > band {
					worst_band_violation += 1;
				}
			}
		}
	}
	// Translucent overlap: per-mark compositing on the desktop, darker where
	// two marks overlap; the compound path is a union and is not.
	let translucent = Color::from_rgba(1.0, 1.0, 1.0, 0.5);
	let pair = [Point::new(60.0, 60.0), Point::new(64.0, 60.0)];
	let batch = render(renderer, &pair, 1.0, true, translucent);
	let union = render(renderer, &pair, 1.0, false, translucent);
	let single = batch.pixel(58, 60)[0];
	let overlap = batch.pixel(62, 60)[0];
	let union_overlap = union.pixel(62, 60)[0];
	let union_single = union.pixel(58, 60)[0];
	// The same pair through the SVG backend and resvg: the two rasterizers
	// agree on the composite within PRODUCT-0057's tolerance at both sample
	// points; pixel-for-pixel agreement is not licensed and not claimed.
	let mut png = plot::export::PngBackend::new(220, 160);
	png.fill_marks(&pair, radius, translucent);
	let pixmap = png.pixmap();
	let resvg_single = pixmap.pixel(58, 60).map(|p| p.red()).unwrap_or(0);
	let resvg_overlap = pixmap.pixel(62, 60).map(|p| p.red()).unwrap_or(0);
	println!(
		"PRODUCT_0159 mark_batch disposition=automated nonoverlap_identical={nonoverlap_identical} overlap_differing_pixels={differing} overlap_max_channel_delta={max_delta} band_violations={worst_band_violation} translucent_single={single} translucent_overlap={overlap} union_single={union_single} union_overlap={union_overlap} resvg_single={resvg_single} resvg_overlap={resvg_overlap}"
	);
	assert!(
		single.abs_diff(resvg_single) <= 12 && overlap.abs_diff(resvg_overlap) <= 12,
		"PRODUCT-0159 gate 3: desktop and resvg agree on the composite within the 0057 tolerance"
	);
	assert!(
		nonoverlap_identical,
		"PRODUCT-0159 gate 2: non-overlapping opaque marks are identical"
	);
	assert_eq!(
		worst_band_violation, 0,
		"PRODUCT-0159 gate 2: differences stay at mark boundaries"
	);
	assert!(
		overlap > single + 40,
		"PRODUCT-0159 gate 3: the overlap composites per mark"
	);
	assert!(
		union_overlap.abs_diff(union_single) <= 8,
		"PRODUCT-0159 gate 3 control: the union does not"
	);
}

#[path = "../src/first_run.rs"]
mod first_run;
#[path = "../src/reporting/mod.rs"]
mod reporting;
