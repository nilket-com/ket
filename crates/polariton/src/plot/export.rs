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
	/// PRODUCT-0159 §3: one definition per translucent mark batch.
	mark_count: usize,
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
			mark_count: 0,
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
	fn with_legend_entry(
		&mut self,
		group: crate::plot::series::SeriesGroup,
		f: &mut dyn FnMut(&mut dyn PlotBackend),
	) {
		if !self.series_groups {
			f(self);
			return;
		}
		self.svg
			.push_str(&format!("<g data-legend=\"s{}\">", group.ordinal()));
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

	/// PRODUCT-0159 §3. An opaque batch is one `<path>` of circle subpaths,
	/// byte-for-byte what the compound path emitted, because for an opaque
	/// colour the union and the per-mark composite are the same pixels. A
	/// translucent batch defines one circle and places it per mark with
	/// `<use>`, so the SVG composites per mark exactly as the desktop does.
	fn fill_marks(&mut self, centres: &[Point], radius: f32, color: Color) {
		if color.a >= 1.0 {
			self.fill_path(
				&|builder| {
					for centre in centres {
						builder.circle(*centre, radius);
					}
				},
				color,
			);
			return;
		}
		let id = format!("mark_{}", self.mark_count);
		self.mark_count += 1;
		self.svg.push_str(&format!(
			r#"<defs><circle id="{}" r="{:.2}" fill="{}" fill-opacity="{:.2}" /></defs>"#,
			id,
			radius,
			self.color_to_svg(color),
			color.a
		));
		self.svg.push('\n');
		for centre in centres {
			let p = self.current_transform * kurbo::Point::new(centre.x as f64, centre.y as f64);
			self.svg.push_str(&format!(
				r##"<use href="#{}" x="{:.2}" y="{:.2}" />"##,
				id, p.x, p.y
			));
			self.svg.push('\n');
		}
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

	fn fill_marks(&mut self, centres: &[Point], radius: f32, color: Color) {
		self.svg_backend.fill_marks(centres, radius, color);
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

	fn fill_marks(&mut self, centres: &[Point], radius: f32, color: Color) {
		self.svg_backend.fill_marks(centres, radius, color);
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

/// PRODUCT-0159 §3: the SVG backend's batch.
#[cfg(test)]
mod product_0159_tests {
	use super::*;

	fn lattice() -> Vec<Point> {
		(0..40)
			.flat_map(|i| {
				(0..20).map(move |j| Point::new(10.0 + i as f32 * 1.5, 10.0 + j as f32 * 1.5))
			})
			.collect()
	}

	#[test]
	fn product_0159_opaque_batch_is_byte_identical_to_the_compound_path() {
		let marks = lattice();
		let color = Color::from_rgb(0.2, 0.4, 0.8);
		let mut batch = SvgBackend::new(100.0, 60.0);
		batch.translate(iced::Vector::new(3.0, 5.0));
		batch.fill_marks(&marks, 3.0, color);
		let mut compound = SvgBackend::new(100.0, 60.0);
		compound.translate(iced::Vector::new(3.0, 5.0));
		compound.fill_path(
			&|builder| {
				for centre in &marks {
					builder.circle(*centre, 3.0);
				}
			},
			color,
		);
		let batch = batch.finish();
		assert_eq!(batch, compound.finish());
		assert_eq!(batch.matches("<path ").count(), 1);
		assert!(!batch.contains("<use "));
	}

	#[test]
	fn product_0159_translucent_batch_composites_per_mark_in_svg_and_png() {
		let marks = [Point::new(30.0, 30.0), Point::new(36.0, 30.0)];
		let color = Color::from_rgba(1.0, 1.0, 1.0, 0.5);
		let mut svg = SvgBackend::new(80.0, 60.0);
		svg.translate(iced::Vector::new(2.0, 0.0));
		svg.fill_marks(&marks, 6.0, color);
		let svg = svg.finish();
		assert_eq!(svg.matches("<defs><circle id=\"mark_0\"").count(), 1);
		assert_eq!(svg.matches("<use href=\"#mark_0\"").count(), 2);
		assert!(
			svg.contains("x=\"32.00\" y=\"30.00\""),
			"the transform applies: {svg}"
		);
		assert!(!svg.contains("<path "));

		// The desktop's compositing, through resvg: the overlap is darker than
		// a single mark, and the pre-cut union is the negative control.
		let render = |batched: bool| {
			let mut png = PngBackend::new(80, 60);
			if batched {
				png.fill_marks(&marks, 6.0, color);
			} else {
				png.fill_path(
					&|builder| {
						for centre in &marks {
							builder.circle(*centre, 6.0);
						}
					},
					color,
				);
			}
			png.pixmap()
		};
		let sample = |pixmap: &tiny_skia::Pixmap, x: u32, y: u32| {
			pixmap.pixel(x, y).map(|p| p.red()).unwrap_or(0)
		};
		let batched = render(true);
		let union = render(false);
		let single = sample(&batched, 26, 30);
		let overlap = sample(&batched, 33, 30);
		assert!(
			single > 100 && single < 160,
			"one mark at half alpha: {single}"
		);
		assert!(
			overlap > single + 40,
			"overlap composites twice: {overlap} vs {single}"
		);
		assert_eq!(
			sample(&union, 33, 30),
			sample(&union, 26, 30),
			"negative control: the compound path is a union"
		);
	}

	/// The pre-cut behaviour, as a borrowing adapter: every batch becomes one
	/// compound path, so the comparison is against what the tree drew before
	/// PRODUCT-0159, through the same widget and the same backend. Each
	/// grouping callback wraps the backend reference the inner backend
	/// supplies, so no reference is ever reconstructed while another is live.
	struct CompoundMarks<'a>(&'a mut dyn PlotBackend);
	impl PlotBackend for CompoundMarks<'_> {
		fn with_series(
			&mut self,
			group: crate::plot::series::SeriesGroup,
			f: &mut dyn FnMut(&mut dyn PlotBackend),
		) {
			self.0
				.with_series(group, &mut |inner| f(&mut CompoundMarks(inner)));
		}
		fn with_legend_entry(
			&mut self,
			group: crate::plot::series::SeriesGroup,
			f: &mut dyn FnMut(&mut dyn PlotBackend),
		) {
			self.0
				.with_legend_entry(group, &mut |inner| f(&mut CompoundMarks(inner)));
		}
		fn translate(&mut self, translation: iced::Vector) {
			self.0.translate(translation);
		}
		fn rotate(&mut self, angle: f32) {
			self.0.rotate(angle);
		}
		fn with_save(&mut self, f: &mut dyn FnMut(&mut dyn PlotBackend)) {
			self.0.with_save(&mut |inner| f(&mut CompoundMarks(inner)));
		}
		fn with_clip(&mut self, bounds: Rectangle, f: &mut dyn FnMut(&mut dyn PlotBackend)) {
			self.0
				.with_clip(bounds, &mut |inner| f(&mut CompoundMarks(inner)));
		}
		fn stroke_path(&mut self, f: &dyn Fn(&mut dyn PathBuilder), stroke: Stroke) {
			self.0.stroke_path(f, stroke);
		}
		fn fill_path(&mut self, f: &dyn Fn(&mut dyn PathBuilder), color: Color) {
			self.0.fill_path(f, color);
		}
		fn fill_marks(&mut self, centres: &[Point], radius: f32, color: Color) {
			self.0.fill_path(
				&|builder| {
					for centre in centres {
						builder.circle(*centre, radius);
					}
				},
				color,
			);
		}
		fn fill_rectangle(&mut self, top_left: Point, size: iced::Size, color: Color) {
			self.0.fill_rectangle(top_left, size, color);
		}
		fn fill_text_raw(&mut self, text: Text) {
			self.0.fill_text_raw(text);
		}
		fn supports_embedded_raster(&self) -> bool {
			self.0.supports_embedded_raster()
		}
		fn draw_image_rgba(
			&mut self,
			top_left: Point,
			size: iced::Size,
			width: u32,
			height: u32,
			rgba: &[u8],
		) {
			self.0.draw_image_rgba(top_left, size, width, height, rgba);
		}
	}

	fn showcase_scatter() -> Arc<crate::plot::kernels::scatter::ScatterPreparedData> {
		// The Trends showcase relation (uat/plot_trends_showcase.sql), the
		// rows its scatter view draws.
		let frame = polars::df!(
			"period" => [1_i64, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8],
			"series" => ["Northstar"; 8].iter().chain(["Horizon"; 8].iter()).chain(["Pulse"; 8].iter()).copied().collect::<Vec<_>>(),
			"value" => [32.0, 38.0, 44.0, 52.0, 60.0, 66.0, 73.0, 81.0, 61.0, 58.0, 55.0, 53.0, 49.0, 46.0, 44.0, 41.0, 20.0, 25.0, 22.0, 31.0, 39.0, 35.0, 49.0, 57.0]
		)
		.expect("frame");
		Arc::new(crate::plot::kernels::scatter::prepare_scatter_data(
			&frame, "series", "period", "value",
		))
	}

	fn dense_scatter() -> Arc<crate::plot::kernels::scatter::ScatterPreparedData> {
		let points = 105 * 105 * 105;
		let mut state = 0x9E37_79B9_7F4A_7C15_u64;
		let mut next = || {
			state ^= state << 13;
			state ^= state >> 7;
			state ^= state << 17;
			(state >> 11) as f64 / (1u64 << 53) as f64
		};
		let xs: Vec<f64> = (0..points).map(|_| next()).collect();
		let ys: Vec<f64> = (0..points).map(|_| next()).collect();
		let series: Vec<&str> = (0..points)
			.map(|i| ["Atlas", "Beacon", "Cinder"][i % 3])
			.collect();
		let frame = polars::df!("x" => xs, "y" => ys, "series" => series).expect("frame");
		Arc::new(crate::plot::kernels::scatter::prepare_scatter_data(
			&frame, "series", "x", "y",
		))
	}

	/// Gate 2's export half: the showcase scatter and the fixture scatter at
	/// three pane sizes, rendered through the widget to the SVG and PNG
	/// backends, are byte-identical with the batch and with the pre-cut
	/// compound path.
	#[test]
	fn product_0159_showcase_and_fixture_exports_are_byte_identical_to_the_compound_path() {
		use crate::plot::common::{PlotKernel, PlotRenderLayer, PlotWidget};
		use crate::plot::kernels::scatter::ScatterPlotKernel;
		let mut cases: Vec<(&str, Arc<dyn PlotKernel + Send + Sync>, f32, f32)> = vec![(
			"trend-scatter",
			Arc::new(ScatterPlotKernel::new(showcase_scatter())),
			320.0,
			220.0,
		)];
		let dense = dense_scatter();
		for (width, height) in [(320.0_f32, 220.0_f32), (950.0, 420.0), (360.0, 360.0)] {
			cases.push((
				"fixture-raster-scatter",
				Arc::new(ScatterPlotKernel::new(Arc::clone(&dense))),
				width,
				height,
			));
		}
		for (label, kernel, width, height) in cases {
			let widget = PlotWidget::new(
				kernel.as_ref(),
				label.to_owned(),
				20.0,
				crate::plot::theme::PlotSettings::default(),
				0,
				false,
				PlotRenderLayer::Full,
			);
			let bounds = Rectangle::new(Point::ORIGIN, iced::Size::new(width, height));
			let mut batch = SvgBackend::new(width, height);
			widget.render(&mut batch, bounds);
			let mut compound = SvgBackend::new(width, height);
			widget.render(&mut CompoundMarks(&mut compound), bounds);
			let batch_svg = batch.finish();
			assert_eq!(
				batch_svg,
				compound.finish(),
				"{label} {width}x{height}: SVG bytes"
			);
			assert!(
				batch_svg.contains("<path ") || batch_svg.contains("<image "),
				"{label}: positive control, marks were emitted"
			);
			let mut batch_png = PngBackend::new(width as u32, height as u32);
			widget.render(&mut batch_png, bounds);
			let mut compound_png = PngBackend::new(width as u32, height as u32);
			widget.render(&mut CompoundMarks(&mut compound_png), bounds);
			assert_eq!(
				batch_png.pixmap().data(),
				compound_png.pixmap().data(),
				"{label} {width}x{height}: PNG pixels"
			);
		}
	}
}
