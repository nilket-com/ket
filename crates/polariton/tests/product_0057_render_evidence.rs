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
#[cfg(feature = "toron")]
#[path = "../src/session_persistence.rs"]
mod session_persistence;

use gui::table::{Product0057TableEvidenceGeometry, draw_product_0057_table_evidence};
use iced::advanced::graphics::geometry::Renderer as _;
use iced::advanced::renderer::{Headless as _, Renderer as _};
use iced::widget::canvas::{self, Path, Stroke};
use iced::{Color, Font, Pixels, Point, Rectangle, Size, Vector};
use iced_wgpu::graphics::{Antialiasing, Shell};
use iced_wgpu::{Engine, Renderer as WgpuRenderer, wgpu};
use plot::common::{IcedBackend, PlotBackend};

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
