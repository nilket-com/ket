use crate::plot::common::{PathBuilder, PlotBackend};
use base64::Engine;
use iced::widget::canvas::{Stroke, Text};
use iced::{advanced::text::Alignment, alignment::Vertical, Color, Point, Rectangle};
use kurbo::Affine;
use png::{BitDepth, ColorType, Encoder};
use std::io::Cursor;
use std::path::Path as StdPath;
use std::sync::Arc;

pub struct SvgBackend {
	pub svg: String,
	current_transform: Affine,
	clip_count: usize,
}

struct SvgPathBuilder {
	path: String,
	transform: Affine,
}

impl PathBuilder for SvgPathBuilder {
	fn move_to(&mut self, point: Point) {
		let p = self.transform * kurbo::Point::new(point.x as f64, point.y as f64);
		self.path.push_str(&format!("M {:.2} {:.2} ", p.x, p.y));
	}
	fn line_to(&mut self, point: Point) {
		let p = self.transform * kurbo::Point::new(point.x as f64, point.y as f64);
		self.path.push_str(&format!("L {:.2} {:.2} ", p.x, p.y));
	}
	fn arc_to(&mut self, center: Point, radius: f32, start: f32, end: f32) {
		let p1 = Point::new(
			center.x + radius * start.cos(),
			center.y + radius * start.sin(),
		);
		let p2 = Point::new(center.x + radius * end.cos(), center.y + radius * end.sin());
		let p1 = self.transform * kurbo::Point::new(p1.x as f64, p1.y as f64);
		let p2 = self.transform * kurbo::Point::new(p2.x as f64, p2.y as f64);
		let large_arc = if (end - start).abs() > std::f32::consts::PI {
			1
		} else {
			0
		};
		self.path.push_str(&format!(
			"M {:.2} {:.2} A {:.2} {:.2} 0 {} 1 {:.2} {:.2} ",
			p1.x, p1.y, radius, radius, large_arc, p2.x, p2.y
		));
	}
	fn circle(&mut self, center: Point, radius: f32) {
		let p = self.transform * kurbo::Point::new(center.x as f64, center.y as f64);
		let r = radius as f64;
		self.path.push_str(&format!(
			"M {:.2} {:.2} A {:.2} {:.2} 0 1 1 {:.2} {:.2} ",
			p.x + r,
			p.y,
			r,
			r,
			p.x - r,
			p.y
		));
		self.path.push_str(&format!(
			"A {:.2} {:.2} 0 1 1 {:.2} {:.2} Z ",
			r,
			r,
			p.x + r,
			p.y
		));
	}
	fn rectangle(&mut self, top_left: Point, size: iced::Size) {
		self.move_to(top_left);
		self.line_to(Point::new(top_left.x + size.width, top_left.y));
		self.line_to(Point::new(
			top_left.x + size.width,
			top_left.y + size.height,
		));
		self.line_to(Point::new(top_left.x, top_left.y + size.height));
		self.close();
	}
	fn close(&mut self) {
		self.path.push_str("Z ");
	}
}

impl SvgBackend {
	pub fn new(width: f32, height: f32) -> Self {
		let mut svg = format!(
			r#"<svg width="{}" height="{}" viewBox="0 0 {} {}" xmlns="http://www.w3.org/2000/svg">"#,
			width, height, width, height
		);
		svg.push('\n');
		Self {
			svg,
			current_transform: Affine::IDENTITY,
			clip_count: 0,
		}
	}

	pub fn finish(mut self) -> String {
		self.svg.push_str("</svg>");
		self.svg
	}

	fn color_to_svg(&self, color: Color) -> String {
		format!(
			"rgb({}, {}, {})",
			(color.r * 255.0) as u8,
			(color.g * 255.0) as u8,
			(color.b * 255.0) as u8
		)
	}
}

impl PlotBackend for SvgBackend {
	fn stroke_path(&mut self, f: &dyn Fn(&mut dyn PathBuilder), stroke: Stroke) {
		let mut builder = SvgPathBuilder {
			path: String::new(),
			transform: self.current_transform,
		};
		f(&mut builder);
		let color = match stroke.style {
			iced::widget::canvas::Style::Solid(c) => self.color_to_svg(c),
			_ => "black".to_string(),
		};
		let opacity = match stroke.style {
			iced::widget::canvas::Style::Solid(c) => c.a,
			_ => 1.0,
		};
		self.svg.push_str(&format!(
			r#"<path d="{}" stroke="{}" stroke-width="{}" stroke-opacity="{:.2}" fill="none" />"#,
			builder.path, color, stroke.width, opacity
		));
		self.svg.push('\n');
	}

	fn fill_path(&mut self, f: &dyn Fn(&mut dyn PathBuilder), color: Color) {
		let mut builder = SvgPathBuilder {
			path: String::new(),
			transform: self.current_transform,
		};
		f(&mut builder);
		self.svg.push_str(&format!(
			r#"<path d="{}" fill="{}" fill-opacity="{:.2}" />"#,
			builder.path,
			self.color_to_svg(color),
			color.a
		));
		self.svg.push('\n');
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
		let anchor = match text.align_x {
			Alignment::Left | Alignment::Default | Alignment::Justified => "start",
			Alignment::Center => "middle",
			Alignment::Right => "end",
		};
		let baseline = match text.align_y {
			Vertical::Top => "hanging",
			Vertical::Center => "central",
			Vertical::Bottom => "alphabetic",
		};
		let c = self.current_transform.as_coeffs();
		let matrix = format!(
			"matrix({:.4} {:.4} {:.4} {:.4} {:.4} {:.4})",
			c[0], c[1], c[2], c[3], c[4], c[5]
		);

		self.svg.push_str(&format!(
			r#"<text transform="{}" x="{}" y="{}" fill="{}" font-size="{}" text-anchor="{}" dominant-baseline="{}">{}</text>"#,
			matrix,
			text.position.x,
			text.position.y,
			self.color_to_svg(text.color),
			text.size.0,
			anchor,
			baseline,
			text.content
		));
		self.svg.push('\n');
	}

	fn supports_embedded_raster(&self) -> bool { true }

	fn draw_image_rgba(
		&mut self,
		top_left: Point,
		size: iced::Size,
		width: u32,
		height: u32,
		rgba: &[u8],
	) {
		let mut png_bytes = Vec::new();
		{
			let cursor = Cursor::new(&mut png_bytes);
			let mut encoder = Encoder::new(cursor, width, height);
			encoder.set_color(ColorType::Rgba);
			encoder.set_depth(BitDepth::Eight);
			let mut writer = encoder.write_header().unwrap();
			writer.write_image_data(rgba).unwrap();
		}

		let c = self.current_transform.as_coeffs();
		let matrix = format!(
			"matrix({:.4} {:.4} {:.4} {:.4} {:.4} {:.4})",
			c[0], c[1], c[2], c[3], c[4], c[5]
		);
		let encoded = base64::engine::general_purpose::STANDARD.encode(png_bytes);
		self.svg.push_str(&format!(
			r#"<image transform="{}" x="{:.2}" y="{:.2}" width="{:.2}" height="{:.2}" href="data:image/png;base64,{}" preserveAspectRatio="none" />"#,
			matrix,
			top_left.x,
			top_left.y,
			size.width,
			size.height,
			encoded
		));
		self.svg.push('\n');
	}

	fn translate(&mut self, translation: iced::Vector) {
		self.current_transform *= Affine::translate((translation.x as f64, translation.y as f64));
	}

	fn rotate(&mut self, angle: f32) {
		self.current_transform *= Affine::rotate(angle as f64);
	}

	fn with_save(&mut self, f: &mut dyn FnMut(&mut dyn PlotBackend)) {
		let prev = self.current_transform;
		f(self);
		self.current_transform = prev;
	}

	fn with_clip(&mut self, bounds: Rectangle, f: &mut dyn FnMut(&mut dyn PlotBackend)) {
		let prev = self.current_transform;
		let clip_id = format!("clip_{}", self.clip_count);
		self.clip_count += 1;

		self.svg.push_str(&format!(
			r#"<defs><clipPath id="{}"><rect x="{:.2}" y="{:.2}" width="{:.2}" height="{:.2}" /></clipPath></defs>"#,
			clip_id, bounds.x, bounds.y, bounds.width, bounds.height
		));
		self.svg
			.push_str(&format!(r#"<g clip-path="url(#{})">"#, clip_id));
		self.svg.push('\n');
		f(self);
		self.svg.push_str("</g>\n");
		self.current_transform = prev;
	}
}

pub struct PngBackend {
	svg_backend: SvgBackend,
}

impl PngBackend {
	pub fn new(width: u32, height: u32) -> Self {
		Self {
			svg_backend: SvgBackend::new(width as f32, height as f32),
		}
	}

	pub fn save(self, path: &StdPath) {
		let svg_str = self.svg_backend.finish();
		let mut fontdb = resvg::usvg::fontdb::Database::new();
		fontdb.load_system_fonts();
		let options = resvg::usvg::Options {
			fontdb: Arc::new(fontdb),
			..resvg::usvg::Options::default()
		};
		let rtree = resvg::usvg::Tree::from_str(&svg_str, &options).unwrap();
		let mut pixmap =
			tiny_skia::Pixmap::new(rtree.size().width() as u32, rtree.size().height() as u32)
				.unwrap();
		resvg::render(
			&rtree,
			tiny_skia::Transform::identity(),
			&mut pixmap.as_mut(),
		);
		pixmap.save_png(path).unwrap();
	}
}

pub struct AvifBackend {
	svg_backend: SvgBackend,
}

impl AvifBackend {
	pub fn new(width: u32, height: u32) -> Self {
		Self {
			svg_backend: SvgBackend::new(width as f32, height as f32),
		}
	}

	pub fn save(self, path: &StdPath) {
		let svg_str = self.svg_backend.finish();
		let mut fontdb = resvg::usvg::fontdb::Database::new();
		fontdb.load_system_fonts();
		let options = resvg::usvg::Options {
			fontdb: Arc::new(fontdb),
			..resvg::usvg::Options::default()
		};
		let rtree = resvg::usvg::Tree::from_str(&svg_str, &options).unwrap();
		let w = rtree.size().width() as u32;
		let h = rtree.size().height() as u32;
		let mut pixmap = tiny_skia::Pixmap::new(w, h).unwrap();
		resvg::render(&rtree, tiny_skia::Transform::identity(), &mut pixmap.as_mut());
		let pixels: Vec<ravif::RGBA8> = pixmap
			.pixels()
			.iter()
			.map(|p| {
				let a = p.alpha();
				if a == 0 {
					ravif::RGBA8 { r: 0, g: 0, b: 0, a: 0 }
				} else if a == 255 {
					ravif::RGBA8 { r: p.red(), g: p.green(), b: p.blue(), a: 255 }
				} else {
					let factor = 255.0 / a as f32;
					ravif::RGBA8 {
						r: (p.red() as f32 * factor).round() as u8,
						g: (p.green() as f32 * factor).round() as u8,
						b: (p.blue() as f32 * factor).round() as u8,
						a,
					}
				}
			})
			.collect();
		let img = ravif::Img::new(pixels.as_slice(), w as usize, h as usize);
		let encoded = ravif::Encoder::new()
			.with_quality(90.0)
			.with_speed(4)
			.encode_rgba(img)
			.unwrap();
		std::fs::write(path, &encoded.avif_file).unwrap();
	}
}

impl PlotBackend for AvifBackend {
	fn stroke_path(&mut self, f: &dyn Fn(&mut dyn PathBuilder), stroke: Stroke) {
		self.svg_backend.stroke_path(f, stroke);
	}

	fn fill_path(&mut self, f: &dyn Fn(&mut dyn PathBuilder), color: Color) {
		self.svg_backend.fill_path(f, color);
	}

	fn fill_rectangle(&mut self, top_left: Point, size: iced::Size, color: Color) {
		self.svg_backend.fill_rectangle(top_left, size, color);
	}

	fn fill_text(&mut self, text: Text) {
		self.svg_backend.fill_text(text);
	}

	fn supports_embedded_raster(&self) -> bool {
		self.svg_backend.supports_embedded_raster()
	}

	fn draw_image_rgba(
		&mut self,
		top_left: Point,
		size: iced::Size,
		width: u32,
		height: u32,
		rgba: &[u8],
	) {
		self.svg_backend
			.draw_image_rgba(top_left, size, width, height, rgba);
	}

	fn translate(&mut self, translation: iced::Vector) {
		self.svg_backend.translate(translation);
	}

	fn rotate(&mut self, angle: f32) {
		self.svg_backend.rotate(angle);
	}

	fn with_save(&mut self, f: &mut dyn FnMut(&mut dyn PlotBackend)) {
		let prev = self.svg_backend.current_transform;
		f(self);
		self.svg_backend.current_transform = prev;
	}

	fn with_clip(&mut self, bounds: Rectangle, f: &mut dyn FnMut(&mut dyn PlotBackend)) {
		self.svg_backend.with_clip(bounds, f);
	}
}

impl PlotBackend for PngBackend {
	fn stroke_path(&mut self, f: &dyn Fn(&mut dyn PathBuilder), stroke: Stroke) {
		self.svg_backend.stroke_path(f, stroke);
	}

	fn fill_path(&mut self, f: &dyn Fn(&mut dyn PathBuilder), color: Color) {
		self.svg_backend.fill_path(f, color);
	}

	fn fill_rectangle(&mut self, top_left: Point, size: iced::Size, color: Color) {
		self.svg_backend.fill_rectangle(top_left, size, color);
	}

	fn fill_text(&mut self, text: Text) {
		self.svg_backend.fill_text(text);
	}

	fn supports_embedded_raster(&self) -> bool {
		self.svg_backend.supports_embedded_raster()
	}

	fn draw_image_rgba(
		&mut self,
		top_left: Point,
		size: iced::Size,
		width: u32,
		height: u32,
		rgba: &[u8],
	) {
		self.svg_backend
			.draw_image_rgba(top_left, size, width, height, rgba);
	}

	fn translate(&mut self, translation: iced::Vector) {
		self.svg_backend.translate(translation);
	}

	fn rotate(&mut self, angle: f32) {
		self.svg_backend.rotate(angle);
	}

	fn with_save(&mut self, f: &mut dyn FnMut(&mut dyn PlotBackend)) {
		let prev = self.svg_backend.current_transform;
		f(self);
		self.svg_backend.current_transform = prev;
	}

	fn with_clip(&mut self, bounds: Rectangle, f: &mut dyn FnMut(&mut dyn PlotBackend)) {
		self.svg_backend.with_clip(bounds, f);
	}
}
