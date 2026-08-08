//! PRODUCT-0057's test-only production-renderer oracle.
//!
//! These probes deliberately assert semantic regions and boundary censuses,
//! never complete-image bytes across machines. The one byte equality below is
//! only a local determinism observation under one adapter and configuration.

use super::common::PlotBackend;
use super::export::SvgBackend;
use iced::advanced::text::{self, Text};
use iced::widget::canvas;
use iced::{Color, Font, Pixels, Point, Rectangle, Size, Vector, alignment};
use iced_wgpu::graphics::Antialiasing;
use iced_wgpu::graphics::Viewport;
use iced_wgpu::graphics::geometry::Renderer as _;
use iced_wgpu::wgpu;
use iced_wgpu::{Engine, Renderer};
use std::borrow::Cow;

const BASE: u32 = 128;
const BACKGROUND: Color = Color::BLACK;
const WHITE: [u8; 3] = [255, 255, 255];
const BLACK: [u8; 3] = [0, 0, 0];
const RED: [u8; 3] = [255, 0, 0];
const GREEN: [u8; 3] = [0, 255, 0];

struct HeadlessHarness {
	_instance: wgpu::Instance,
	adapter: wgpu::Adapter,
	device: wgpu::Device,
	queue: wgpu::Queue,
	info: wgpu::AdapterInfo,
}

impl HeadlessHarness {
	async fn new() -> Self {
		iced_wgpu::graphics::text::font_system()
			.write()
			.expect("PRODUCT-0057 production font system")
			.load_font(Cow::Borrowed(crate::fonts::DEJAVU_SANS_MONO));
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
			.expect(
				"PRODUCT-0057 requires a headless wgpu adapter; use the named manual protocol if this environment has none",
			);
		let info = adapter.get_info();
		let (device, queue) = adapter
			.request_device(&wgpu::DeviceDescriptor {
				label: Some("polariton PRODUCT-0057 headless oracle"),
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
			.expect("PRODUCT-0057 headless adapter must create a device");
		Self {
			_instance: instance,
			adapter,
			device,
			queue,
			info,
		}
	}

	fn renderer(&self, antialiasing: Option<Antialiasing>) -> Renderer {
		let format = if iced_wgpu::graphics::color::GAMMA_CORRECTION {
			wgpu::TextureFormat::Rgba8UnormSrgb
		} else {
			wgpu::TextureFormat::Rgba8Unorm
		};
		let engine = Engine::new(
			&self.adapter,
			self.device.clone(),
			self.queue.clone(),
			format,
			antialiasing,
			iced_wgpu::graphics::Shell::headless(),
		);
		Renderer::new(engine, Font::MONOSPACE, Pixels(16.0))
	}

	fn capture(
		&self,
		size: u32,
		antialiasing: Option<Antialiasing>,
		draw: fn(&mut Renderer, Size),
	) -> Raster {
		let mut renderer = self.renderer(antialiasing);
		draw(&mut renderer, Size::new(size as f32, size as f32));
		let viewport = Viewport::with_physical_size(Size::new(size, size), 1.0);
		Raster {
			rgba: renderer.screenshot(&viewport, BACKGROUND),
			width: size,
			height: size,
		}
	}
}

struct Raster {
	rgba: Vec<u8>,
	width: u32,
	height: u32,
}

impl Raster {
	fn pixel(&self, x: u32, y: u32) -> [u8; 3] {
		assert!(x < self.width && y < self.height);
		let index = ((y * self.width + x) * 4) as usize;
		[self.rgba[index], self.rgba[index + 1], self.rgba[index + 2]]
	}

	fn assert_color(&self, x: u32, y: u32, expected: [u8; 3], label: &str) {
		let actual = self.pixel(x, y);
		assert!(
			actual
				.iter()
				.zip(expected)
				.all(|(actual, expected)| actual.abs_diff(expected) <= 8),
			"{label}: expected {expected:?} near ({x}, {y}), got {actual:?}",
		);
	}

	fn ink_count(&self, bounds: PixelRegion) -> usize {
		bounds
			.pixels()
			.filter(|&(x, y)| self.pixel(x, y).iter().any(|channel| *channel > 24))
			.count()
	}

	fn blend_count(&self) -> usize {
		self.rgba
			.chunks_exact(4)
			.filter(|pixel| {
				let value = pixel[0];
				value > 8
					&& value < 247 && pixel[0].abs_diff(pixel[1]) <= 2
					&& pixel[1].abs_diff(pixel[2]) <= 2
			})
			.count()
	}
}

#[derive(Clone, Copy)]
struct PixelRegion {
	x0: u32,
	y0: u32,
	x1: u32,
	y1: u32,
}

impl PixelRegion {
	fn pixels(self) -> impl Iterator<Item = (u32, u32)> {
		(self.y0..self.y1).flat_map(move |y| (self.x0..self.x1).map(move |x| (x, y)))
	}

	fn scaled(self, factor: u32) -> Self {
		Self {
			x0: self.x0 * factor,
			y0: self.y0 * factor,
			x1: self.x1 * factor,
			y1: self.y1 * factor,
		}
	}
}

fn canvas_frame(renderer: &Renderer, size: Size) -> canvas::Frame<Renderer> {
	canvas::Frame::new(renderer, size)
}

fn finish_frame(renderer: &mut Renderer, frame: canvas::Frame<Renderer>) {
	renderer.draw_geometry(frame.into_geometry());
}

fn draw_solid_fixture(renderer: &mut Renderer, size: Size) {
	let mut frame = canvas_frame(renderer, size);
	frame.fill_rectangle(
		Point::new(size.width * 0.25, size.height * 0.25),
		Size::new(size.width * 0.5, size.height * 0.5),
		Color::WHITE,
	);
	finish_frame(renderer, frame);
}

fn draw_overlap_fixture(renderer: &mut Renderer, size: Size) {
	let scale = size.width / BASE as f32;
	let mut frame = canvas_frame(renderer, size);
	frame.fill_rectangle(
		Point::new(20.0 * scale, 20.0 * scale),
		Size::new(70.0 * scale, 70.0 * scale),
		Color::from_rgb8(0, 255, 0),
	);
	// Authored second. iced_wgpu 0.14 realizes this clipped child below the
	// parent mesh that is only completed afterward, despite the source order.
	frame.with_clip(
		Rectangle::new(
			Point::new(50.0 * scale, 50.0 * scale),
			Size::new(70.0 * scale, 70.0 * scale),
		),
		|frame| {
			frame.fill_rectangle(
				Point::new(50.0 * scale, 50.0 * scale),
				Size::new(70.0 * scale, 70.0 * scale),
				Color::from_rgb8(255, 0, 0),
			);
		},
	);
	finish_frame(renderer, frame);
}

fn draw_clipped_fixture(renderer: &mut Renderer, size: Size) {
	draw_clip_fixture(renderer, size, true);
}

fn draw_unclipped_fixture(renderer: &mut Renderer, size: Size) {
	draw_clip_fixture(renderer, size, false);
}

fn draw_clip_fixture(renderer: &mut Renderer, size: Size, clipped: bool) {
	let scale = size.width / BASE as f32;
	let mut frame = canvas_frame(renderer, size);
	let draw = |frame: &mut canvas::Frame<Renderer>| {
		frame.translate(Vector::new(10.0 * scale, -5.0 * scale));
		frame.fill_rectangle(
			Point::new(20.0 * scale, 30.0 * scale),
			Size::new(80.0 * scale, 80.0 * scale),
			Color::from_rgb8(0, 255, 0),
		);
	};
	if clipped {
		frame.with_clip(
			Rectangle::new(
				Point::new(40.0 * scale, 40.0 * scale),
				Size::new(40.0 * scale, 40.0 * scale),
			),
			draw,
		);
	} else {
		draw(&mut frame);
	}
	finish_frame(renderer, frame);
}

fn draw_diagonal_fixture(renderer: &mut Renderer, size: Size) {
	let scale = size.width / BASE as f32;
	let mut frame = canvas_frame(renderer, size);
	let path = canvas::Path::line(
		Point::new(16.0 * scale, 20.0 * scale),
		Point::new(112.0 * scale, 104.0 * scale),
	);
	frame.stroke(
		&path,
		canvas::Stroke::default()
			.with_color(Color::WHITE)
			.with_width(3.0 * scale),
	);
	finish_frame(renderer, frame);
}

fn draw_integer_rectangle_fixture(renderer: &mut Renderer, size: Size) {
	let scale = size.width / BASE as f32;
	let mut frame = canvas_frame(renderer, size);
	frame.fill_rectangle(
		Point::new(32.0 * scale, 32.0 * scale),
		Size::new(64.0 * scale, 64.0 * scale),
		Color::WHITE,
	);
	finish_frame(renderer, frame);
}

fn draw_margin_accepted(renderer: &mut Renderer, size: Size) {
	draw_margin(renderer, size, 96.0);
}

fn draw_margin_narrow(renderer: &mut Renderer, size: Size) {
	draw_margin(renderer, size, 28.0);
}

fn draw_margin(renderer: &mut Renderer, size: Size, cell_width: f32) {
	let scale = size.width / BASE as f32;
	let cell_x = 16.0 * scale;
	let cell_y = 16.0 * scale;
	let cell_width = cell_width * scale;
	let cell_height = 32.0 * scale;
	let padding = 8.0 * scale;
	let text_bounds = Rectangle::new(
		Point::new(cell_x + padding, cell_y),
		Size::new(cell_width - padding * 2.0, cell_height),
	);
	iced::advanced::text::Renderer::fill_text(
		renderer,
		Text {
			content: "margin".to_owned(),
			bounds: text_bounds.size(),
			size: Pixels(14.0 * scale),
			font: Font {
				family: iced::font::Family::Name("DejaVu Sans Mono"),
				weight: iced::font::Weight::Bold,
				..Font::DEFAULT
			},
			align_x: alignment::Horizontal::Left.into(),
			align_y: alignment::Vertical::Center,
			line_height: text::LineHeight::default(),
			shaping: text::Shaping::Basic,
			wrapping: text::Wrapping::None,
		},
		text_bounds.position(),
		Color::WHITE,
		text_bounds,
	);
}

fn assert_solid_semantics(raster: &Raster, factor: u32) {
	raster.assert_color(64 * factor, 64 * factor, WHITE, "solid interior");
	raster.assert_color(8 * factor, 8 * factor, BLACK, "solid exterior");
}

fn assert_overlap_semantics(raster: &Raster, factor: u32) {
	raster.assert_color(30 * factor, 30 * factor, GREEN, "parent-fill control");
	raster.assert_color(105 * factor, 105 * factor, RED, "clipped-child control");
	raster.assert_color(
		60 * factor,
		60 * factor,
		GREEN,
		"iced_wgpu 0.14 realized overlap winner",
	);
	raster.assert_color(8 * factor, 8 * factor, BLACK, "overlap background");
}

fn assert_clip_semantics(clipped: &Raster, unclipped: &Raster, factor: u32) {
	clipped.assert_color(60 * factor, 60 * factor, GREEN, "clip interior");
	for (x, y, edge) in [
		(35, 60, "left"),
		(85, 60, "right"),
		(60, 35, "top"),
		(60, 85, "bottom"),
	] {
		clipped.assert_color(x * factor, y * factor, BLACK, edge);
		unclipped.assert_color(
			x * factor,
			y * factor,
			GREEN,
			&format!("unclipped {edge} control"),
		);
	}
}

fn assert_text_semantics(accepted: &Raster, narrow: &Raster, factor: u32) {
	let accepted_ink = accepted.ink_count(
		PixelRegion {
			x0: 16,
			y0: 16,
			x1: 112,
			y1: 48,
		}
		.scaled(factor),
	);
	let narrow_ink = narrow.ink_count(
		PixelRegion {
			x0: 16,
			y0: 16,
			x1: 112,
			y1: 48,
		}
		.scaled(factor),
	);
	assert!(
		accepted_ink > narrow_ink + 20 * factor as usize,
		"the narrow production-text control must have a distinguishable realized extent: accepted={accepted_ink}, narrow={narrow_ink}",
	);
	assert_eq!(
		accepted.ink_count(
			PixelRegion {
				x0: 16,
				y0: 50,
				x1: 112,
				y1: 76,
			}
			.scaled(factor),
		),
		0,
		"Wrapping::None accepted case must leave the second-line region free of ink",
	);
	assert_eq!(
		accepted.ink_count(
			PixelRegion {
				x0: 112,
				y0: 16,
				x1: 124,
				y1: 48,
			}
			.scaled(factor),
		),
		0,
		"accepted header must leave the neighboring-cell region free of ink",
	);
}

// Keep the four probe dispositions under one current-thread owner. They must
// compare one adapter, device, production font system, and serialized renderer
// configuration; ordinary independent tests could run concurrently and turn
// GPU contention into a false rendering failure. The named fixture functions
// and assertion messages retain per-probe failure attribution inside this test.
#[tokio::test(flavor = "current_thread")]
async fn product_0057_headless_wgpu_probes_have_discriminating_controls() {
	let harness = HeadlessHarness::new().await;
	println!(
		"PRODUCT-0057 adapter: {} {:?} {:?}; driver {} {}",
		harness.info.name,
		harness.info.backend,
		harness.info.device_type,
		harness.info.driver,
		harness.info.driver_info,
	);

	let stable_a = harness.capture(BASE, Some(Antialiasing::MSAAx4), draw_solid_fixture);
	let stable_b = harness.capture(BASE, Some(Antialiasing::MSAAx4), draw_solid_fixture);
	assert_eq!(
		stable_a.rgba, stable_b.rgba,
		"local determinism compares identical viewports and configurations only",
	);
	assert_solid_semantics(&stable_a, 1);
	let scaled = harness.capture(BASE * 3, Some(Antialiasing::MSAAx4), draw_solid_fixture);
	assert_solid_semantics(&scaled, 3);

	for factor in [1, 3] {
		let overlap = harness.capture(
			BASE * factor,
			Some(Antialiasing::MSAAx4),
			draw_overlap_fixture,
		);
		assert_overlap_semantics(&overlap, factor);

		let clipped = harness.capture(
			BASE * factor,
			Some(Antialiasing::MSAAx4),
			draw_clipped_fixture,
		);
		let unclipped = harness.capture(
			BASE * factor,
			Some(Antialiasing::MSAAx4),
			draw_unclipped_fixture,
		);
		assert_clip_semantics(&clipped, &unclipped, factor);

		let no_aa = harness.capture(BASE * factor, None, draw_diagonal_fixture);
		let msaa = harness.capture(
			BASE * factor,
			Some(Antialiasing::MSAAx4),
			draw_diagonal_fixture,
		);
		let no_aa_blends = no_aa.blend_count();
		let msaa_blends = msaa.blend_count();
		assert_eq!(no_aa_blends, 0, "disabled diagonal control is a hard edge");
		assert!(
			msaa_blends >= 100 * factor as usize,
			"MSAA diagonal must produce a wide blend census: no-AA={no_aa_blends}, MSAA={msaa_blends}",
		);

		let integer_none = harness.capture(BASE * factor, None, draw_integer_rectangle_fixture);
		let integer_msaa = harness.capture(
			BASE * factor,
			Some(Antialiasing::MSAAx4),
			draw_integer_rectangle_fixture,
		);
		assert_eq!(integer_none.blend_count(), 0);
		assert_eq!(
			integer_msaa.blend_count(),
			0,
			"integer-device-coordinate axis-aligned control must be blind in both AA states",
		);
		integer_msaa.assert_color(64 * factor, 64 * factor, WHITE, "rectangle interior");
		integer_msaa.assert_color(16 * factor, 16 * factor, BLACK, "rectangle exterior");

		let accepted = harness.capture(
			BASE * factor,
			Some(Antialiasing::MSAAx4),
			draw_margin_accepted,
		);
		let narrow = harness.capture(
			BASE * factor,
			Some(Antialiasing::MSAAx4),
			draw_margin_narrow,
		);
		assert_text_semantics(&accepted, &narrow, factor);
	}

	let mut svg = SvgBackend::new(BASE as f32, BASE as f32);
	svg.fill_rectangle(
		Point::new(20.0, 20.0),
		Size::new(70.0, 70.0),
		Color::from_rgb8(0, 255, 0),
	);
	svg.with_clip(
		Rectangle::new(Point::new(50.0, 50.0), Size::new(70.0, 70.0)),
		&mut |backend| {
			backend.fill_rectangle(
				Point::new(50.0, 50.0),
				Size::new(70.0, 70.0),
				Color::from_rgb8(255, 0, 0),
			);
		},
	);
	let svg = svg.finish();
	let green = svg.find("rgb(0, 255, 0)").expect("green parent fill");
	let red = svg.find("rgb(255, 0, 0)").expect("red clipped child");
	assert!(
		green < red,
		"SVG is licensed to prove authored command order, not iced/wgpu's realized composite winner",
	);
}
