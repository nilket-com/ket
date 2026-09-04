use crate::plot::common::{ArcJoin, ArcPathState, CircularArc, PathBuilder, PlotBackend};
use base64::Engine;
use iced::widget::canvas::{Stroke, Text};
use iced::{Color, Point, Rectangle, advanced::text::Alignment, alignment::Vertical};
use kurbo::Affine;
use png::{BitDepth, ColorType, Encoder};
use std::io::Cursor;
use std::path::Path as StdPath;
use std::sync::Arc;

/// Serialized SVG oracle.
///
/// PRODUCT-0057 license: emitted primitives, authored order, coordinates,
/// paths, colors, text, transforms, clip declarations, and clip nesting. SVG
/// cannot prove production-canvas compositing, clip enforcement, MSAA, or
/// realized glyph measurement and fit.
/// PRODUCT-0154 §6.1: every data-derived string enters the document escaped.
pub fn xml_escape(text: &str) -> String {
	let mut out = String::with_capacity(text.len());
	for ch in text.chars() {
		match ch {
			'&' => out.push_str("&amp;"),
			'<' => out.push_str("&lt;"),
			'>' => out.push_str("&gt;"),
			'"' => out.push_str("&quot;"),
			'\'' => out.push_str("&apos;"),
			_ => out.push(ch),
		}
	}
	out
}

pub struct SvgBackend {
	pub svg: String,
	series_groups: bool,
	current_transform: Affine,
	clip_count: usize,
}

/// PRODUCT-0156: exact metadata stays in memory, separate from the SVG's
/// opaque ordinal attributes. Package serialization is a later contract.
pub struct SvgFigure {
	pub svg: String,
	pub series: crate::plot::series::SeriesRegistry,
}

pub fn render_svg(widget: &crate::plot::common::PlotWidget<'_>, bounds: Rectangle) -> SvgFigure {
	let mut backend = SvgBackend::new(bounds.width, bounds.height);
	widget.render(&mut backend, bounds);
	SvgFigure {
		svg: backend.finish(),
		series: widget.series_registry().clone(),
	}
}

struct SvgPathBuilder {
	path: String,
	transform: Affine,
	state: ArcPathState,
}

impl PathBuilder for SvgPathBuilder {
	fn move_to(&mut self, point: Point) {
		self.state.move_to(point);
		let p = self.transform * kurbo::Point::new(point.x as f64, point.y as f64);
		self.path.push_str(&format!("M {:.2} {:.2} ", p.x, p.y));
	}
	fn line_to(&mut self, point: Point) {
		self.state.line_to(point);
		let p = self.transform * kurbo::Point::new(point.x as f64, point.y as f64);
		self.path.push_str(&format!("L {:.2} {:.2} ", p.x, p.y));
	}
	fn arc_to(&mut self, center: Point, radius: f32, start: f32, end: f32) {
		let arc = CircularArc::new(center, radius, start, end);
		let join = self.state.begin_arc(arc);
		let p1 = arc.start();
		let p2 = arc.end();
		let p1 = self.transform * kurbo::Point::new(p1.x as f64, p1.y as f64);
		let p2 = self.transform * kurbo::Point::new(p2.x as f64, p2.y as f64);
		let large_arc = if (end - start).abs() > std::f32::consts::PI {
			1
		} else {
			0
		};
		let join = match join {
			ArcJoin::Move => 'M',
			ArcJoin::Line => 'L',
		};
		let sweep = u8::from(end >= start);
		self.path.push_str(&format!(
			"{} {:.2} {:.2} A {:.2} {:.2} 0 {} {} {:.2} {:.2} ",
			join, p1.x, p1.y, radius, radius, large_arc, sweep, p2.x, p2.y
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
		self.state.finish_closed_shape();
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
		self.state.close();
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
			series_groups: true,
			current_transform: Affine::IDENTITY,
			clip_count: 0,
		}
	}

	pub fn finish(mut self) -> String {
		self.svg.push_str("</svg>");
		self.svg
	}

	fn for_raster(width: f32, height: f32) -> Self {
		let mut backend = Self::new(width, height);
		backend.series_groups = false;
		backend
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
	fn with_series(
		&mut self,
		group: crate::plot::series::SeriesGroup,
		f: &mut dyn FnMut(&mut dyn PlotBackend),
	) {
		if !self.series_groups {
			f(self);
			return;
		}
		self.svg
			.push_str(&format!("<g data-series=\"s{}\">", group.ordinal()));
		f(self);
		self.svg.push_str("</g>");
	}
	fn stroke_path(&mut self, f: &dyn Fn(&mut dyn PathBuilder), stroke: Stroke) {
		let mut builder = SvgPathBuilder {
			path: String::new(),
			transform: self.current_transform,
			state: ArcPathState::default(),
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
			state: ArcPathState::default(),
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

	fn fill_text_raw(&mut self, text: Text) {
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
			r#"<text transform="{}" x="{}" y="{}" fill="{}" font-size="{}" font-family="{}" text-anchor="{}" dominant-baseline="{}">{}</text>"#,
			matrix,
			text.position.x,
			text.position.y,
			self.color_to_svg(text.color),
			text.size.0,
			crate::publication::FONT_FAMILY,
			anchor,
			baseline,
			xml_escape(&text.content)
		));
		self.svg.push('\n');
	}

	fn supports_embedded_raster(&self) -> bool {
		true
	}

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
		self.current_transform = Affine::IDENTITY;
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

#[cfg(test)]
fn product_0101_svg_semantic_trace(
	f: &dyn Fn(&mut dyn PathBuilder),
) -> (Vec<crate::plot::common::SemanticPathVerb>, String) {
	let mut builder = SvgPathBuilder {
		path: String::new(),
		transform: Affine::IDENTITY,
		state: ArcPathState::tracing(),
	};
	f(&mut builder);
	let trace = builder.state.finish_trace();
	(trace, builder.path)
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod product_0101_tests {
	use super::*;
	use crate::plot::common::{CircularArc, SemanticPathVerb, product_0101_iced_semantic_trace};

	fn annulus(builder: &mut dyn PathBuilder) {
		let center = Point::new(40.0, 30.0);
		builder.arc_to(center, 20.0, 0.0, std::f32::consts::PI);
		builder.arc_to(center, 12.0, std::f32::consts::PI, 0.0);
		builder.close();
	}

	fn pie(builder: &mut dyn PathBuilder) {
		let center = Point::new(40.0, 30.0);
		builder.move_to(center);
		builder.arc_to(center, 20.0, -1.0, 1.2);
		builder.close();
	}

	fn radial(center: Point, from: Point, to: Point) -> bool {
		let left = (from.x - center.x, from.y - center.y);
		let right = (to.x - center.x, to.y - center.y);
		(left.0 * right.1 - left.1 * right.0).abs() <= 1e-4
	}

	#[test]
	fn product_0101_both_adapters_emit_one_connected_annulus_with_radial_caps() {
		let iced = product_0101_iced_semantic_trace(&annulus);
		let (svg, path) = product_0101_svg_semantic_trace(&annulus);
		assert_eq!(iced, svg, "both adapters share one semantic topology");
		assert_eq!(svg.len(), 5);
		let center = Point::new(40.0, 30.0);
		assert!(matches!(svg[0], SemanticPathVerb::Move { .. }));
		assert!(matches!(svg[1], SemanticPathVerb::Arc(_)));
		let SemanticPathVerb::Line { from, to } = svg[2] else {
			panic!("the inner arc must connect to the outer arc")
		};
		assert!(radial(center, from, to));
		assert!(matches!(svg[3], SemanticPathVerb::Arc(_)));
		let SemanticPathVerb::Close { from, to } = svg[4] else {
			panic!("the annulus must close its second radial cap")
		};
		assert!(radial(center, from, to));
		assert_eq!(path.matches("M ").count(), 1);
		assert_eq!(path.matches("L ").count(), 1);
		assert_eq!(path.matches(" A ").count(), 2);
		assert_eq!(path.matches(" 0 0 1 ").count(), 1);
		assert_eq!(path.matches(" 0 0 0 ").count(), 1);
		assert!(path.ends_with("Z "));
	}

	#[test]
	fn product_0101_pie_connection_lone_arc_and_current_endpoint_are_distinct() {
		let iced = product_0101_iced_semantic_trace(&pie);
		let (svg, connected_path) = product_0101_svg_semantic_trace(&pie);
		assert_eq!(iced, svg);
		assert!(matches!(
			svg.as_slice(),
			[
				SemanticPathVerb::Move { to },
				SemanticPathVerb::Line { from, .. },
				SemanticPathVerb::Arc(_),
				SemanticPathVerb::Close { to: close_to, .. },
			] if *to == Point::new(40.0, 30.0)
				&& *from == Point::new(40.0, 30.0)
				&& *close_to == Point::new(40.0, 30.0)
		));
		assert_eq!(connected_path.matches("M ").count(), 1);
		assert_eq!(connected_path.matches("L ").count(), 1);

		let lone = |builder: &mut dyn PathBuilder| {
			builder.arc_to(Point::new(5.0, 7.0), 3.0, 0.25, 1.5);
			builder.line_to(Point::new(12.0, 13.0));
		};
		let lone_trace = product_0101_iced_semantic_trace(&lone);
		let expected_arc = CircularArc::new(Point::new(5.0, 7.0), 3.0, 0.25, 1.5);
		assert!(matches!(
			lone_trace.as_slice(),
			[
				SemanticPathVerb::Move { to },
				SemanticPathVerb::Arc(actual),
				SemanticPathVerb::Line { from, to: line_to },
			] if *to == expected_arc.start()
				&& *actual == expected_arc
				&& *from == expected_arc.end()
				&& *line_to == Point::new(12.0, 13.0)
		));

		let without_center = |builder: &mut dyn PathBuilder| {
			builder.arc_to(Point::new(40.0, 30.0), 20.0, -1.0, 1.2);
			builder.close();
		};
		let (without_trace, without_path) = product_0101_svg_semantic_trace(&without_center);
		assert_ne!(svg, without_trace, "pie's move_to(center) is load-bearing");
		assert_ne!(connected_path, without_path);
		assert!(!without_path.contains(" L "));
	}
}

/// SVG-derived PNG exporter. PRODUCT-0057 licenses this only to prove resvg's
/// rasterization of [`SvgBackend`] output, never an iced production-canvas
/// property.
pub struct PngBackend {
	svg_backend: SvgBackend,
}

impl PngBackend {
	pub fn new(width: u32, height: u32) -> Self {
		Self {
			svg_backend: SvgBackend::for_raster(width as f32, height as f32),
		}
	}

	/// The export rasterization itself: the SVG through resvg with the bundled
	/// face loaded first. PRODUCT-0154 gate 5 compares against exactly this.
	pub fn pixmap(self) -> tiny_skia::Pixmap {
		let svg_str = self.svg_backend.finish();
		let mut fontdb = resvg::usvg::fontdb::Database::new();
		fontdb.load_font_data(crate::fonts::POLARITON_PLOT_SANS_REGULAR.to_vec());
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
		pixmap
	}

	pub fn save(self, path: &StdPath) {
		self.pixmap().save_png(path).unwrap();
	}
}

/// SVG-derived AVIF exporter. PRODUCT-0057 licenses this only to prove resvg's
/// rasterization of [`SvgBackend`] output, never an iced production-canvas
/// property.
pub struct AvifBackend {
	svg_backend: SvgBackend,
}

impl AvifBackend {
	pub fn new(width: u32, height: u32) -> Self {
		Self {
			svg_backend: SvgBackend::for_raster(width as f32, height as f32),
		}
	}

	pub fn save(self, path: &StdPath) {
		let svg_str = self.svg_backend.finish();
		let mut fontdb = resvg::usvg::fontdb::Database::new();
		fontdb.load_font_data(crate::fonts::POLARITON_PLOT_SANS_REGULAR.to_vec());
		fontdb.load_system_fonts();
		let options = resvg::usvg::Options {
			fontdb: Arc::new(fontdb),
			..resvg::usvg::Options::default()
		};
		let rtree = resvg::usvg::Tree::from_str(&svg_str, &options).unwrap();
		let w = rtree.size().width() as u32;
		let h = rtree.size().height() as u32;
		let mut pixmap = tiny_skia::Pixmap::new(w, h).unwrap();
		resvg::render(
			&rtree,
			tiny_skia::Transform::identity(),
			&mut pixmap.as_mut(),
		);
		let pixels: Vec<ravif::RGBA8> = pixmap
			.pixels()
			.iter()
			.map(|p| {
				let a = p.alpha();
				if a == 0 {
					ravif::RGBA8 {
						r: 0,
						g: 0,
						b: 0,
						a: 0,
					}
				} else if a == 255 {
					ravif::RGBA8 {
						r: p.red(),
						g: p.green(),
						b: p.blue(),
						a: 255,
					}
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

	fn fill_text_raw(&mut self, text: Text) {
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

	fn fill_text_raw(&mut self, text: Text) {
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
