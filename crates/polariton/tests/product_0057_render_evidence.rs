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
#[path = "../src/editor.rs"]
mod editor;
#[path = "../src/fonts/mod.rs"]
mod fonts;
#[path = "../src/gui/mod.rs"]
mod gui;
#[cfg(feature = "toron")]
#[path = "../src/local_workspace.rs"]
mod local_workspace;
#[path = "../src/plot/mod.rs"]
mod plot;
#[path = "../src/result_registry.rs"]
mod result_registry;
#[path = "../src/rune_host.rs"]
mod rune_host;
#[cfg(feature = "toron")]
#[path = "../src/rune_relation_envelope.rs"]
mod rune_relation_envelope;
#[path = "../src/rune_view.rs"]
mod rune_view;
#[cfg(feature = "toron")]
#[path = "../src/session_persistence.rs"]
mod session_persistence;

use gui::table::{Product0057TableEvidenceGeometry, draw_product_0057_table_evidence};
use iced::advanced::graphics::geometry::Renderer as _;
use iced::advanced::renderer::{Headless as _, Renderer as _};
use iced::widget::canvas::{self, Path, Stroke, Text};
use iced::{Color, Font, Pixels, Point, Rectangle, Size, Vector};
use iced_wgpu::graphics::{Antialiasing, Shell};
use iced_wgpu::{Engine, Renderer as WgpuRenderer, wgpu};
use plot::common::{
	AxisType, CoordinateTransformer, IcedBackend, Orientation, PlotBackend, PlotKernel,
	PlotRenderLayer, PlotWidget, ResolvedPlotSettings, TickPlacement,
};
use plot::kernels::bar::{BarPlotKernel, BarPreparedData};
use plot::kernels::line::{LinePlotKernel, LinePreparedData, SeriesData};
use plot::kernels::scatter::{ScatterPlotKernel, ScatterPreparedData, ScatterSeries};
use plot::kernels::stacked_area::{StackedAreaPlotKernel, StackedAreaPreparedData};
use plot::kernels::stacked_bar::{StackedBarPlotKernel, StackedBarPreparedData};
use plot::legend::LegendProvenance;
use plot::spec::{
	BarWidth, CategoryColorAssignment, CategoryGap, LayerSettings, PalettePosition, PointSize,
	ResolvedColor, ResolvedPresence, StackNormalization, StrokeWidth,
};
use plot::theme::PlotSettings;
use polars::prelude::{Float64Chunked, NewChunkedArray};
use std::sync::Arc;

const BACKGROUND: [u8; 3] = [0, 0, 0];
const RED: [u8; 3] = [230, 32, 48];
const BLUE: [u8; 3] = [40, 96, 230];
const GREEN: [u8; 3] = [32, 220, 96];
const WHITE: [u8; 3] = [255, 255, 255];

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
) -> Capture {
	let (kernel, settings) = product_0068_bar_fixture();
	let resolved = settings.resolve();
	let bounds = Rectangle::with_size(Size::new(size.width as f32, size.height as f32));
	let widget = PlotWidget {
		kernel: &kernel,
		title: String::new(),
		padding: 0.0,
		settings: resolved.clone(),
		settings_model: settings.clone(),
		render_revision: 0,
		resize_render_suspended: false,
		layer: PlotRenderLayer::Full,
	};
	render_frame(renderer, size, |frame| {
		let mut backend = IcedBackend { frame };
		if overlay_control {
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
				&settings,
			);
			widget.draw_layout_grid(&mut backend, bounds, &transform, &layout);
			widget.render_overlay_layer(&mut backend, bounds);
		} else {
			widget.render_plot_layer(&mut backend, bounds);
			widget.render_overlay_layer(&mut backend, bounds);
		}
	})
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
		x_label: String::new(),
		y_label: String::new(),
	}))
}

fn product_0068_scatter_fixture(color: Color) -> ScatterPlotKernel {
	ScatterPlotKernel::new(Arc::new(ScatterPreparedData {
		series: vec![ScatterSeries {
			name: "point".to_owned(),
			x_values: Float64Chunked::from_slice("x".into(), &[0.5]),
			y_values: Float64Chunked::from_slice("y".into(), &[0.5]),
			color_t: 0.0,
			resolved_color: Some(color),
			provenance: LegendProvenance::Named,
		}],
		x_categories: None,
		x_range: (0.0, 1.0),
		y_range: (0.0, 1.0),
		x_axis_type: AxisType::Linear,
		y_axis_type: AxisType::Linear,
		x_label: String::new(),
		y_label: String::new(),
		total_points: 1,
	}))
}

fn product_0068_stacked_area_fixture() -> (StackedAreaPlotKernel, PlotSettings, [[u8; 3]; 2]) {
	let mut resolved = product_0068_cartesian_settings().resolve();
	resolved.mark_decoration_color = Color::WHITE;
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
				categories: vec!["Atlas".to_owned(), "Beacon".to_owned()],
				unique_xs: vec![0.0, 0.25, 0.5, 0.75, 1.0],
				category_values: vec![vec![2.0, 2.5, 2.0, 3.0, 2.5], vec![2.0, 1.5, 2.5, 2.0, 2.5]],
				totals: vec![4.0, 4.0, 4.5, 5.0, 5.0],
				x_range: (0.0, 1.0),
				y_range: (0.0, 10.0),
				x_axis_type: AxisType::Linear,
				y_axis_type: AxisType::Linear,
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
	let widget = PlotWidget {
		kernel,
		title: String::new(),
		padding: 0.0,
		settings: resolved.clone(),
		settings_model: settings.clone(),
		render_revision: 0,
		resize_render_suspended: false,
		layer: PlotRenderLayer::Full,
	};
	render_frame(renderer, size, |frame| {
		let mut backend = IcedBackend { frame };
		if overlay_control {
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
		} else {
			widget.render_plot_layer(&mut backend, bounds);
			widget.render_overlay_layer(&mut backend, bounds);
		}
	})
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
	resolved.mark_decoration_color = Color::WHITE;
	resolved.y_tick_placement = TickPlacement::step(25.0).unwrap();
	(
		StackedAreaPlotKernel {
			prepared_data: Arc::new(StackedAreaPreparedData {
				categories: vec!["Atlas".to_owned(), "Beacon".to_owned()],
				unique_xs: vec![0.0, 0.25, 0.5, 0.75, 1.0],
				category_values: vec![vec![30.0; 5], vec![70.0; 5]],
				totals: vec![100.0; 5],
				x_range: (0.0, 1.0),
				y_range: (0.0, 105.0),
				x_axis_type: AxisType::Linear,
				y_axis_type: AxisType::Linear,
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
	no_boundary.mark_decoration_color = Color::TRANSPARENT;
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
	let bar_under = render_product_0068_bar(&mut msaa, bar_size, false);
	let bar_overlay = render_product_0068_bar(&mut msaa, bar_size, true);
	let cyan = [1, 205, 254];
	let crossing_row = [319, 320]
		.into_iter()
		.max_by_key(|row| color_distance(bar_overlay.pixel(40, *row), cyan))
		.expect("PRODUCT-0068 grid crossing row");
	let under_mark = bar_under.pixel(40, crossing_row);
	let overlay_mark = bar_overlay.pixel(40, crossing_row);
	assert!(
		near(under_mark, cyan, 12),
		"PRODUCT-0068 grid-under must preserve the exact resolved bar colour: {under_mark:?}"
	);
	assert!(
		color_distance(overlay_mark, cyan) >= 20,
		"PRODUCT-0068 overlay control must visibly tint the same bar pixel: {overlay_mark:?}"
	);
	let gap_grid = bar_under.pixel(195, crossing_row);
	let gap_canvas = bar_under.pixel(195, 300);
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
		"PRODUCT_0068 bar_strata disposition=automated crossing_row={crossing_row} under_mark={under_mark:?} overlay_mark={overlay_mark:?} gap_grid={gap_grid:?} gap_canvas={gap_canvas:?}"
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
