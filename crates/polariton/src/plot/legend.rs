//! The closed legend-content model and its single renderer.

use crate::coordinate::{Pt, Rect, Widget};
use crate::plot::common::{PlotBackend, ResolvedPlotSettings};
use crate::plot::continuous_color::ContinuousColorScale;
use crate::plot::gradient_stops::GradientStops;
use crate::plot::spec::{NonEmpty, ResolvedPresence};
use iced::widget::canvas::{Stroke, Style, Text};
use iced::{Color, Point, Size, alignment};

#[derive(Clone, Debug, PartialEq)]
pub enum LegendSample {
	Swatch,
	LineSample,
	PointSample,
}

/// Whether the label was read from the relation or manufactured by a kernel.
///
/// This is content provenance, not a rendering preference: PRODUCT-0044's
/// policy needs it to distinguish a one-entry named series from a synthetic
/// fallback such as `All Data` without inspecting the label text.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LegendProvenance {
	Named,
	Synthetic,
}

#[derive(Clone, Debug, PartialEq)]
pub struct LegendEntry {
	pub label: String,
	pub swatch: Color,
	pub sample: LegendSample,
	pub provenance: LegendProvenance,
}

#[derive(Clone, Debug, PartialEq)]
pub enum LegendSection {
	Entries(NonEmpty<LegendEntry>),
	Gradient { label: String, stops: GradientStops },
	SizeSample { label: String, low: f64, high: f64 },
}

#[derive(Clone, Debug, PartialEq)]
pub struct LegendContent(pub NonEmpty<LegendSection>);

impl LegendContent {
	pub fn entries(entries: NonEmpty<LegendEntry>) -> Self {
		Self(NonEmpty::new(LegendSection::Entries(entries)))
	}

	pub fn sections(&self) -> impl Iterator<Item = &LegendSection> {
		self.0.iter()
	}

	pub fn push(&mut self, section: LegendSection) {
		self.0.push(section);
	}
}

/// Construct one honest continuous guide from a producer's data domain.
///
/// Zero-width and non-finite domains say nothing and are omitted. A descending
/// finite domain violates the ordered-range invariant shared by all current
/// producers and must not be hidden as an omission.
pub(crate) fn gradient_section(
	label: String,
	scale: &ContinuousColorScale,
) -> Option<LegendSection> {
	if scale.is_degenerate() {
		return None;
	}
	let stops = GradientStops::projected(scale)
		.expect("PRODUCT-0087 canonical positions project to an ascending domain");
	Some(LegendSection::Gradient { label, stops })
}

/// Apply PRODUCT-0044's content policy to one legend.
///
/// A lone synthetic entry describes no data identity, so it does not earn the
/// chrome required to render it. PRODUCT-0087 additionally omits gradients at
/// construction when their domain is zero-width or non-finite.
pub fn apply_policy(content: LegendContent) -> Option<LegendContent> {
	let (first, rest) = content.0.into_parts();
	let mut sections = Vec::with_capacity(1 + rest.len());
	sections.push(first);
	sections.extend(rest);
	let mut retained = sections.into_iter().filter(|section| {
		!matches!(section, LegendSection::Entries(entries)
			if entries.len() == 1
				&& entries.first().provenance == LegendProvenance::Synthetic)
	});
	let first = retained.next()?;
	let mut content = LegendContent(NonEmpty::new(first));
	for section in retained {
		content.push(section);
	}
	Some(content)
}

/// Combine content contributed in paint or cell order.
///
/// Entry identity is precisely the printed triple: label, swatch, and sample.
/// Provenance deliberately does not participate.  If identical entries arrive
/// with different provenance, a data-read name wins over a manufactured one.
pub fn union<I>(contents: I) -> Option<LegendContent>
where
	I: IntoIterator<Item = LegendContent>,
{
	let mut sections = Vec::new();
	let mut entry_section = None;
	for content in contents {
		for section in content.0.iter().cloned() {
			match section {
				LegendSection::Entries(entries) => {
					let index = *entry_section.get_or_insert_with(|| {
						sections.push(LegendSection::Entries(entries.clone()));
						sections.len() - 1
					});
					if let LegendSection::Entries(existing) = &mut sections[index] {
						for candidate in entries.iter().cloned() {
							let current = existing.iter().position(|entry| {
								entry.label == candidate.label
									&& entry.swatch == candidate.swatch
									&& entry.sample == candidate.sample
							});
							if let Some(current) = current {
								let (first, rest) = existing.clone().into_parts();
								let mut values = vec![first];
								values.extend(rest);
								if candidate.provenance == LegendProvenance::Named {
									values[current].provenance = LegendProvenance::Named;
								}
								let mut rebuilt = NonEmpty::new(values.remove(0));
								for value in values {
									rebuilt.push(value);
								}
								*existing = rebuilt;
							} else {
								existing.push(candidate);
							}
						}
					}
				}
				other => sections.push(other),
			}
		}
	}
	let mut sections = sections.into_iter();
	let first = sections.next()?;
	let mut content = LegendContent(NonEmpty::new(first));
	for section in sections {
		content.push(section);
	}
	apply_policy(content)
}

pub fn entries_from_labels<I, S>(
	labels: I,
	settings: &ResolvedPlotSettings,
) -> Option<LegendContent>
where
	I: IntoIterator<Item = S>,
	S: Into<String>,
{
	entries_from_labels_with_sample_and_provenance(
		labels,
		settings,
		LegendSample::Swatch,
		LegendProvenance::Named,
	)
}

pub fn entries_from_labels_with_sample_and_provenance<I, S>(
	labels: I,
	settings: &ResolvedPlotSettings,
	sample: LegendSample,
	provenance: LegendProvenance,
) -> Option<LegendContent>
where
	I: IntoIterator<Item = S>,
	S: Into<String>,
{
	let labels: Vec<String> = labels.into_iter().map(Into::into).collect();
	let mut iter = labels.into_iter().enumerate();
	let first = iter.next()?;
	let total = first.0 + 1 + iter.size_hint().0;
	let mut entries = NonEmpty::new(LegendEntry {
		label: first.1,
		swatch: settings
			.color_theme
			.get_color(if total > 1 { 0.0 } else { 0.5 }),
		sample: sample.clone(),
		provenance,
	});
	for (index, label) in iter {
		let t = if total > 1 {
			index as f32 / (total - 1) as f32
		} else {
			0.5
		};
		entries.push(LegendEntry {
			label,
			swatch: settings.color_theme.get_color(t),
			sample: sample.clone(),
			provenance,
		});
	}
	Some(LegendContent::entries(entries))
}

pub fn entries_from_pairs_with_sample_and_provenance<I, S>(
	pairs: I,
	sample: LegendSample,
	provenance: LegendProvenance,
) -> Option<LegendContent>
where
	I: IntoIterator<Item = (S, Color)>,
	S: Into<String>,
{
	let mut iter = pairs.into_iter();
	let (label, swatch) = iter.next()?;
	let mut entries = NonEmpty::new(LegendEntry {
		label: label.into(),
		swatch,
		sample: sample.clone(),
		provenance,
	});
	for (label, swatch) in iter {
		entries.push(LegendEntry {
			label: label.into(),
			swatch,
			sample: sample.clone(),
			provenance,
		});
	}
	Some(LegendContent::entries(entries))
}

fn draw_text(
	backend: &mut dyn PlotBackend,
	content: String,
	position: Point,
	settings: &ResolvedPlotSettings,
) {
	draw_text_at(
		backend,
		content,
		position,
		settings,
		alignment::Horizontal::Center,
		alignment::Vertical::Center,
		settings.legend_size,
	);
}

fn draw_text_at(
	backend: &mut dyn PlotBackend,
	content: String,
	position: Point,
	settings: &ResolvedPlotSettings,
	align_x: alignment::Horizontal,
	align_y: alignment::Vertical,
	size: f32,
) {
	backend.fill_text(Text {
		content,
		position,
		color: settings.chrome.legend_text.iced(),
		size: iced::Pixels(size),
		align_x: align_x.into(),
		align_y,
		..Default::default()
	});
}

fn sample_gradient(stops: &GradientStops, t: f32) -> Color {
	stops.sample_normalized(t)
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct LegendLayout {
	pub(crate) content_bounds: Rect<Widget>,
	origin: Option<LegendOrigin>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum LegendOrigin {
	Gutter(Pt<Widget>),
	Overlay(Pt<Widget>),
}

impl LegendLayout {
	/// Resolve the gutter's pending vertical anchor from the authoritative plot
	/// extent. Authored overlays are already final in full-figure coordinates.
	pub(crate) fn origin(self, plot_extent: Rect<Widget>) -> Option<Pt<Widget>> {
		match self.origin? {
			LegendOrigin::Gutter(origin) => Some(origin.offset(0.0, plot_extent.y() - origin.y())),
			LegendOrigin::Overlay(origin) => Some(origin),
		}
	}
}

pub(crate) fn measure_legend(content: &LegendContent, settings: &ResolvedPlotSettings) -> Size {
	let sections: Vec<&LegendSection> = content.sections().collect();
	if sections.len() == 2
		&& matches!(
			sections.as_slice(),
			[
				LegendSection::Gradient { .. },
				LegendSection::SizeSample { .. }
			]
		) {
		return Size::new(250.0, 240.0);
	}
	let padding = 10.0;
	let width = if sections.len() == 1 {
		match sections[0] {
			LegendSection::Entries(entries) => {
				let max_rows = settings.max_legend_rows.max(1) as usize;
				entries.len().div_ceil(max_rows) as f32 * 150.0 + padding * 2.0
			}
			LegendSection::Gradient { .. } => 60.0,
			LegendSection::SizeSample { .. } => 180.0,
		}
	} else {
		250.0
	};
	let height = sections
		.iter()
		.map(|section| match section {
			LegendSection::Entries(entries) => {
				let max_rows = settings.max_legend_rows.max(1) as usize;
				entries.len().min(max_rows) as f32 * 25.0 + padding * 2.0
			}
			LegendSection::Gradient { .. } => 200.0,
			LegendSection::SizeSample { .. } => 180.0,
		})
		.sum();
	Size::new(width, height)
}

pub(crate) fn legend_layout(
	bounds: Rect<Widget>,
	content: Option<&LegendContent>,
	settings: &ResolvedPlotSettings,
) -> LegendLayout {
	let Some(content) = content else {
		return LegendLayout {
			content_bounds: bounds,
			origin: None,
		};
	};
	let size = measure_legend(content, settings);
	match settings.legend {
		ResolvedPresence::Hide => LegendLayout {
			content_bounds: bounds,
			origin: None,
		},
		ResolvedPresence::Show(position) => LegendLayout {
			content_bounds: bounds,
			origin: Some(LegendOrigin::Overlay(bounds.origin().offset(
				(bounds.width() - size.width) * position.x(),
				(bounds.height() - size.height) * position.y(),
			))),
		},
		ResolvedPresence::Fallback => {
			let content_width = (bounds.width() - size.width).max(0.0);
			LegendLayout {
				content_bounds: bounds.region(
					bounds.x(),
					bounds.y(),
					content_width,
					bounds.height(),
				),
				origin: Some(LegendOrigin::Gutter(
					bounds.origin().offset(content_width, 0.0),
				)),
			}
		}
	}
}

/// Render any closed legend content using the shared chrome roles.
pub fn render_legend(
	backend: &mut dyn PlotBackend,
	origin: Pt<Widget>,
	content: &LegendContent,
	settings: &ResolvedPlotSettings,
) {
	let sections: Vec<&LegendSection> = content.sections().collect();
	if sections.is_empty() {
		return;
	}
	let section_count = sections.len();
	let padding = 10.0;
	let x = origin.x();
	let y = origin.y();
	// Bubble's two semantic sections were historically laid out as one
	// horizontal guide: size samples on the left and the color gradient on the
	// right. Keep that geometry while representing the content as two ordinary
	// sections; PRODUCT-0044 may later choose a different policy.
	if sections.len() == 2
		&& let [
			LegendSection::Gradient {
				label: color_label,
				stops,
			},
			LegendSection::SizeSample {
				label: size_label,
				low: size_low,
				high: size_high,
			},
		] = sections.as_slice()
	{
		let width = 250.0;
		let height = 240.0;
		backend.fill_rectangle(
			Point::new(x, y),
			Size::new(width, height),
			Color {
				a: 0.7,
				..settings.chrome.legend_background.iced()
			},
		);
		let bar_x = x + width - 70.0;
		let bar_y = y + 50.0;
		let bar_height = height - 80.0;
		for i in 0..50 {
			let t = i as f32 / 49.0;
			let step_height = bar_height / 50.0;
			backend.fill_rectangle(
				Point::new(bar_x, bar_y + bar_height - (i as f32 + 1.0) * step_height),
				Size::new(15.0, step_height + 0.5),
				sample_gradient(stops, t),
			);
		}
		let low = stops.first_value();
		let high = stops.last_value();
		let label_x = bar_x + 23.0;
		draw_text_at(
			backend,
			format!("{high:.1}"),
			Point::new(label_x, bar_y),
			settings,
			alignment::Horizontal::Left,
			alignment::Vertical::Top,
			settings.legend_size,
		);
		draw_text_at(
			backend,
			format!("{low:.1}"),
			Point::new(label_x, bar_y + bar_height),
			settings,
			alignment::Horizontal::Left,
			alignment::Vertical::Bottom,
			settings.legend_size,
		);
		draw_text_at(
			backend,
			color_label.clone(),
			Point::new(bar_x + 7.5, y + 15.0),
			settings,
			alignment::Horizontal::Center,
			alignment::Vertical::Top,
			16.0,
		);
		let size_x = x + 20.0;
		draw_text_at(
			backend,
			size_label.clone(),
			Point::new(size_x + 40.0, y + 15.0),
			settings,
			alignment::Horizontal::Center,
			alignment::Vertical::Top,
			16.0,
		);
		for i in 0..3 {
			let t = i as f32 / 2.0;
			let radius = 2.0 + t * 28.0;
			let center = Point::new(
				size_x + 35.0,
				(bar_y + bar_height - 2.0) - t * (bar_height - 30.0 - 2.0),
			);
			backend.fill_path(
				&|builder| builder.circle(center, radius),
				settings.chrome.legend_swatch_fill.iced(),
			);
			backend.stroke_path(
				&|builder| builder.circle(center, radius),
				Stroke {
					style: Style::Solid(settings.chrome.legend_swatch_border.iced()),
					width: 1.0,
					..Default::default()
				},
			);
			draw_text_at(
				backend,
				format!("{:.1}", size_low + (size_high - size_low) * t as f64),
				Point::new(size_x + 75.0, center.y),
				settings,
				alignment::Horizontal::Left,
				alignment::Vertical::Center,
				settings.legend_size,
			);
		}
		return;
	}
	let size = measure_legend(content, settings);
	let width = size.width;
	let height = size.height;
	backend.fill_rectangle(
		Point::new(x, y),
		Size::new(width, height),
		Color {
			a: 0.6,
			..settings.chrome.legend_background.iced()
		},
	);

	let mut offset = 0.0;
	for section in sections {
		match section {
			LegendSection::Entries(entries) => {
				let max_rows = settings.max_legend_rows.max(1) as usize;
				for (index, entry) in entries.iter().enumerate() {
					let col = index / max_rows;
					let row = index % max_rows;
					let item_x = x + padding + col as f32 * 150.0;
					let item_y = y + offset + padding + row as f32 * 25.0;
					match entry.sample {
						LegendSample::Swatch => backend.fill_rectangle(
							Point::new(item_x, item_y + 5.0),
							Size::new(15.0, 15.0),
							entry.swatch,
						),
						LegendSample::LineSample => backend.stroke_path(
							&|builder| {
								builder.move_to(Point::new(item_x, item_y + 12.5));
								builder.line_to(Point::new(item_x + 20.0, item_y + 12.5));
							},
							Stroke {
								style: Style::Solid(entry.swatch),
								width: 3.0,
								..Default::default()
							},
						),
						LegendSample::PointSample => backend.fill_path(
							&|builder| {
								builder.circle(Point::new(item_x + 6.0, item_y + 12.5), 6.0);
							},
							entry.swatch,
						),
					}
					let text_offset = match entry.sample {
						LegendSample::Swatch => 25.0,
						LegendSample::LineSample => 30.0,
						LegendSample::PointSample => 22.0,
					};
					backend.fill_text(Text {
						content: entry.label.clone(),
						position: Point::new(item_x + text_offset, item_y + 12.5),
						color: settings.chrome.legend_text.iced(),
						size: iced::Pixels(settings.legend_size),
						align_x: alignment::Horizontal::Left.into(),
						align_y: alignment::Vertical::Center,
						..Default::default()
					});
				}
				offset += entries.len().min(max_rows) as f32 * 25.0 + padding * 2.0;
			}
			LegendSection::Gradient { label, stops } => {
				let bar_x = x + padding;
				let bar_y = y + offset + 35.0;
				let bar_height = 145.0;
				for i in 0..50 {
					let t = i as f32 / 49.0;
					backend.fill_rectangle(
						Point::new(
							bar_x,
							bar_y + bar_height - (i as f32 + 1.0) * bar_height / 50.0,
						),
						Size::new(15.0, bar_height / 50.0 + 0.5),
						sample_gradient(stops, t),
					);
				}
				let low = stops.first_value();
				let high = stops.last_value();
				backend.stroke_path(
					&|builder| {
						builder.move_to(Point::new(bar_x, bar_y));
						builder.line_to(Point::new(bar_x + 15.0, bar_y));
						builder.line_to(Point::new(bar_x + 15.0, bar_y + bar_height));
						builder.line_to(Point::new(bar_x, bar_y + bar_height));
						builder.close();
					},
					Stroke {
						style: Style::Solid(settings.chrome.legend_swatch_border.iced()),
						width: 1.0,
						..Default::default()
					},
				);
				draw_text_at(
					backend,
					format!("{high:.1}"),
					Point::new(bar_x + 20.0, bar_y),
					settings,
					alignment::Horizontal::Left,
					alignment::Vertical::Top,
					settings.legend_size,
				);
				draw_text_at(
					backend,
					format!("{low:.1}"),
					Point::new(bar_x + 20.0, bar_y + bar_height),
					settings,
					alignment::Horizontal::Left,
					alignment::Vertical::Bottom,
					settings.legend_size,
				);
				if section_count > 1 || label != "Value" {
					draw_text_at(
						backend,
						label.clone(),
						Point::new(x + width / 2.0, y + offset + 10.0),
						settings,
						alignment::Horizontal::Center,
						alignment::Vertical::Top,
						settings.legend_size,
					);
				}
				offset += 200.0;
			}
			LegendSection::SizeSample { label, low, high } => {
				draw_text(
					backend,
					label.clone(),
					Point::new(x + width / 2.0, y + offset + 15.0),
					settings,
				);
				for i in 0..3 {
					let t = i as f32 / 2.0;
					let radius = 2.0 + t * 28.0;
					let center = Point::new(x + 45.0, y + offset + 45.0 + i as f32 * 48.0);
					backend.fill_path(
						&|builder| builder.circle(center, radius),
						settings.chrome.legend_swatch_fill.iced(),
					);
					backend.stroke_path(
						&|builder| builder.circle(center, radius),
						Stroke {
							style: Style::Solid(settings.chrome.legend_swatch_border.iced()),
							width: 1.0,
							..Default::default()
						},
					);
					draw_text(
						backend,
						format!("{:.1}", low + (high - low) * t as f64),
						Point::new(x + 100.0, center.y),
						settings,
					);
				}
				offset += 180.0;
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::plot::colors::ColorTheme;
	use crate::plot::common::{PathBuilder, PlotBackend};
	use crate::plot::continuous_color::{ContinuousAnchor, ContinuousRamps, ContinuousScaleIntent};
	use crate::plot::spec::{ColorJob, LegendPosition, ResolvedPresence};
	use iced::widget::canvas::{Stroke, Text};
	use iced::{Point, Rectangle};

	#[derive(Default)]
	struct RectangleBackend {
		rectangles: Vec<(Point, Size, Color)>,
	}

	impl PlotBackend for RectangleBackend {
		fn stroke_path(&mut self, _f: &dyn Fn(&mut dyn PathBuilder), _stroke: Stroke) {}

		fn fill_path(&mut self, _f: &dyn Fn(&mut dyn PathBuilder), _color: Color) {}

		fn fill_rectangle(&mut self, point: Point, size: Size, color: Color) {
			self.rectangles.push((point, size, color));
		}

		fn fill_text(&mut self, _text: Text) {}

		fn translate(&mut self, _translation: iced::Vector) {}

		fn rotate(&mut self, _angle: f32) {}

		fn with_save(&mut self, f: &mut dyn FnMut(&mut dyn PlotBackend)) {
			f(self);
		}

		fn with_clip(&mut self, _bounds: Rectangle, f: &mut dyn FnMut(&mut dyn PlotBackend)) {
			f(self);
		}
	}

	fn gradient(theme: ColorTheme) -> LegendSection {
		let scale = ContinuousColorScale::new(
			(10.0, 50.0),
			ContinuousScaleIntent::SEQUENTIAL,
			ContinuousRamps {
				sequential: theme,
				diverging: ColorTheme::BlueWhiteRed,
			},
		)
		.expect("ascending non-degenerate scale");
		gradient_section("Value".to_owned(), &scale).expect("ascending non-degenerate gradient")
	}

	fn gradient_stops(section: &LegendSection) -> &GradientStops {
		let LegendSection::Gradient { stops, .. } = section else {
			panic!("gradient section")
		};
		stops
	}

	fn rgb8(color: Color) -> [u8; 3] {
		[
			(color.r * 255.0).round() as u8,
			(color.g * 255.0).round() as u8,
			(color.b * 255.0).round() as u8,
		]
	}

	fn ulp_distance(a: f32, b: f32) -> u32 {
		a.to_bits().abs_diff(b.to_bits())
	}

	fn entry(label: &str, sample: LegendSample, provenance: LegendProvenance) -> LegendEntry {
		LegendEntry {
			label: label.to_owned(),
			swatch: Color::from_rgb8(12, 34, 56),
			sample,
			provenance,
		}
	}

	fn content(entry: LegendEntry) -> LegendContent {
		LegendContent::entries(NonEmpty::new(entry))
	}

	#[test]
	fn product_0044_policy_omits_only_lone_synthetic_entries() {
		assert!(
			apply_policy(content(entry(
				"All Data",
				LegendSample::PointSample,
				LegendProvenance::Synthetic,
			)))
			.is_none()
		);
		assert!(
			apply_policy(content(entry(
				"Adelie",
				LegendSample::PointSample,
				LegendProvenance::Named,
			)))
			.is_some()
		);
	}

	#[test]
	fn product_0044_union_deduplicates_the_printed_triple_and_named_wins() {
		let merged = union([
			content(entry(
				"Value",
				LegendSample::Swatch,
				LegendProvenance::Synthetic,
			)),
			content(entry(
				"Value",
				LegendSample::Swatch,
				LegendProvenance::Named,
			)),
		])
		.expect("named duplicate survives the merge");
		let LegendSection::Entries(entries) = merged.0.first() else {
			panic!("entries section");
		};
		assert_eq!(entries.len(), 1);
		assert_eq!(entries.first().provenance, LegendProvenance::Named);

		let distinct_samples = union([
			content(entry(
				"series",
				LegendSample::LineSample,
				LegendProvenance::Named,
			)),
			content(entry(
				"series",
				LegendSample::PointSample,
				LegendProvenance::Named,
			)),
		])
		.expect("different samples remain distinct");
		let LegendSection::Entries(entries) = distinct_samples.0.first() else {
			panic!("entries section");
		};
		assert_eq!(entries.len(), 2);
	}

	#[test]
	fn product_0050_layout_distinguishes_gutter_overlay_and_hide() {
		let content = content(entry(
			"series",
			LegendSample::LineSample,
			LegendProvenance::Named,
		));
		let bounds =
			Rect::<Widget>::from_iced_frame(Rectangle::new(Point::ORIGIN, Size::new(400.0, 300.0)));
		let plot_extent = bounds.region(40.0, 50.0, 170.0, 210.0);
		let mut settings = ResolvedPlotSettings::default();

		let gutter = legend_layout(bounds, Some(&content), &settings);
		assert_eq!(gutter.content_bounds.width(), 230.0);
		assert_eq!(
			gutter
				.origin(plot_extent)
				.map(|point| (point.x(), point.y())),
			Some((230.0, 50.0))
		);

		settings.legend = ResolvedPresence::Show(LegendPosition::new(0.98, 0.02).unwrap());
		let overlay = legend_layout(bounds, Some(&content), &settings);
		assert_eq!(overlay.content_bounds, bounds);
		assert_eq!(
			overlay
				.origin(plot_extent)
				.map(|point| (point.x(), point.y())),
			Some(((400.0 - 170.0) * 0.98, (300.0 - 45.0) * 0.02))
		);

		settings.legend = ResolvedPresence::Show(LegendPosition::new(1.5, -0.5).unwrap());
		let outside = legend_layout(bounds, Some(&content), &settings);
		let outside_origin = outside.origin(plot_extent).unwrap();
		assert!(outside_origin.x() + measure_legend(&content, &settings).width > bounds.right());
		assert!(outside_origin.y() < bounds.y());

		settings.legend = ResolvedPresence::Hide;
		assert_eq!(
			legend_layout(bounds, Some(&content), &settings),
			legend_layout(bounds, None, &ResolvedPlotSettings::default())
		);
	}

	#[test]
	fn product_0051_tall_legends_keep_the_existing_row_budget_and_may_outlive_the_plot_extent() {
		let mut entries = NonEmpty::new(entry(
			"series-0",
			LegendSample::LineSample,
			LegendProvenance::Named,
		));
		for index in 1..12 {
			entries.push(entry(
				&format!("series-{index}"),
				LegendSample::LineSample,
				LegendProvenance::Named,
			));
		}
		let content = LegendContent::entries(entries);
		let settings = ResolvedPlotSettings::default();
		assert_eq!(measure_legend(&content, &settings), Size::new(320.0, 270.0));
		let labels = content
			.sections()
			.flat_map(|section| match section {
				LegendSection::Entries(entries) => entries
					.iter()
					.map(|entry| entry.label.as_str())
					.collect::<Vec<_>>(),
				_ => Vec::new(),
			})
			.collect::<Vec<_>>();
		assert_eq!(
			labels,
			(0..12)
				.map(|index| format!("series-{index}"))
				.collect::<Vec<_>>()
		);

		let bounds =
			Rect::<Widget>::from_iced_frame(Rectangle::new(Point::ORIGIN, Size::new(500.0, 180.0)));
		let plot_extent = bounds.region(40.0, 50.0, 100.0, 90.0);
		let layout = legend_layout(bounds, Some(&content), &settings);
		let origin = layout.origin(plot_extent).expect("default gutter");
		assert_eq!(origin.y(), plot_extent.y());
		assert!(origin.y() + measure_legend(&content, &settings).height > plot_extent.bottom());
	}

	#[test]
	fn product_0087_gradient_constructor_distinguishes_omission_from_invariant_failure() {
		let degenerate = ContinuousColorScale::new(
			(0.0, 0.0),
			ContinuousScaleIntent::SEQUENTIAL,
			ContinuousRamps::default(),
		)
		.unwrap();
		assert!(gradient_section("Value".to_owned(), &degenerate).is_none());
		let valid = ContinuousColorScale::new(
			(0.0, 1.0),
			ContinuousScaleIntent::SEQUENTIAL,
			ContinuousRamps::default(),
		)
		.unwrap();
		assert!(gradient_section("Value".to_owned(), &valid).is_some());
		for domain in [(f64::NAN, 1.0), (0.0, f64::INFINITY), (1.0, 0.0)] {
			assert!(
				ContinuousColorScale::new(
					domain,
					ContinuousScaleIntent {
						job: ColorJob::Diverging,
						anchor: ContinuousAnchor::None,
					},
					ContinuousRamps::default(),
				)
				.is_err()
			);
		}
	}

	#[test]
	fn product_0087_stops_are_data_values_and_midpoint_sampling_restores_the_ramp() {
		let green_purples = gradient(ColorTheme::GreenPurples);
		let stops = gradient_stops(&green_purples);
		assert_eq!(stops.len(), 3);
		assert_eq!(stops.values().collect::<Vec<_>>(), [10.0, 30.0, 50.0]);
		assert_eq!(sample_gradient(stops, 0.5), Color::WHITE);

		let viridis = gradient(ColorTheme::Viridis);
		let stops = gradient_stops(&viridis);
		assert_eq!(stops.len(), 5);
		assert_eq!(
			stops.values().collect::<Vec<_>>(),
			[10.0, 20.0, 30.0, 40.0, 50.0]
		);
		assert_eq!(
			sample_gradient(stops, 0.5),
			Color::from_rgb(0.129, 0.569, 0.553)
		);
	}

	#[test]
	fn product_0087_both_render_paths_use_the_same_fifty_stop_samples() {
		let section = gradient(ColorTheme::GreenPurples);
		let stops = gradient_stops(&section);
		let ordinary = LegendContent(NonEmpty::new(section.clone()));
		let mut paired = LegendContent(NonEmpty::new(section.clone()));
		paired.push(LegendSection::SizeSample {
			label: "Size".to_owned(),
			low: 1.0,
			high: 3.0,
		});
		let settings = ResolvedPlotSettings::default();
		let mut paths = Vec::new();
		for content in [&ordinary, &paired] {
			let mut backend = RectangleBackend::default();
			let origin = Rect::<Widget>::from_iced_frame(Rectangle::new(
				Point::ORIGIN,
				Size::new(400.0, 300.0),
			))
			.origin();
			render_legend(&mut backend, origin, content, &settings);
			let colors = backend.rectangles[1..51]
				.iter()
				.map(|(_, _, color)| *color)
				.collect::<Vec<_>>();
			assert_eq!(colors.len(), 50);
			assert_eq!(rgb8(colors[24]), [251, 252, 250]);
			assert_eq!(rgb8(colors[25]), [252, 250, 252]);
			for (index, color) in colors.iter().enumerate() {
				let expected = ColorTheme::GreenPurples.get_color(index as f32 / 49.0);
				for (actual, expected) in [
					(color.r, expected.r),
					(color.g, expected.g),
					(color.b, expected.b),
					(color.a, expected.a),
				] {
					assert!(
						ulp_distance(actual, expected) <= 4,
						"index {index}: {actual} != {expected}"
					);
				}
			}
			paths.push(colors);
		}
		assert_eq!(paths[0], paths[1]);
		assert_eq!(paths[0][0], sample_gradient(stops, 0.0));
		assert_eq!(paths[0][49], sample_gradient(stops, 1.0));
	}

	#[test]
	fn product_0087_every_theme_matches_marks_across_the_realized_non_unit_domain() {
		for theme in ColorTheme::ALL {
			let section = gradient(theme);
			let stops = gradient_stops(&section);
			for index in 0..50 {
				let t = index as f32 / 49.0;
				let actual = sample_gradient(stops, t);
				let expected = theme.get_color(t);
				for (actual, expected) in [
					(actual.r, expected.r),
					(actual.g, expected.g),
					(actual.b, expected.b),
					(actual.a, expected.a),
				] {
					assert!(
						ulp_distance(actual, expected) <= 4,
						"{theme:?} index {index}: {actual} != {expected}"
					);
				}
			}
		}
	}

	#[test]
	fn product_0089_anchored_guide_uses_the_inverse_and_matches_marks_on_both_paths() {
		let scale = ContinuousColorScale::new(
			(-50.0, 100.0),
			ContinuousScaleIntent {
				job: ColorJob::Diverging,
				anchor: ContinuousAnchor::DefaultZero,
			},
			ContinuousRamps {
				sequential: ColorTheme::Magma,
				diverging: ColorTheme::Viridis,
			},
		)
		.unwrap();
		let section = gradient_section("Value".to_owned(), &scale).unwrap();
		let stops = gradient_stops(&section);
		assert_eq!(
			stops.values().collect::<Vec<_>>(),
			[-50.0, -25.0, 0.0, 50.0, 100.0]
		);
		let ordinary = LegendContent(NonEmpty::new(section.clone()));
		let mut paired = ordinary.clone();
		paired.push(LegendSection::SizeSample {
			label: "Size".to_owned(),
			low: 1.0,
			high: 3.0,
		});
		let settings = ResolvedPlotSettings::default();
		let mut paths = Vec::new();
		for content in [&ordinary, &paired] {
			let mut backend = RectangleBackend::default();
			let origin = Rect::<Widget>::from_iced_frame(Rectangle::new(
				Point::ORIGIN,
				Size::new(400.0, 300.0),
			))
			.origin();
			render_legend(&mut backend, origin, content, &settings);
			let colors = backend.rectangles[1..51]
				.iter()
				.map(|(_, _, color)| *color)
				.collect::<Vec<_>>();
			for (index, actual) in colors.iter().enumerate() {
				let t = index as f64 / 49.0;
				let expected = scale.color(scale.value_at_normalized(t));
				for (actual, expected) in [
					(actual.r, expected.r),
					(actual.g, expected.g),
					(actual.b, expected.b),
					(actual.a, expected.a),
				] {
					assert!(ulp_distance(actual, expected) <= 4);
				}
			}
			paths.push(colors);
		}
		assert_eq!(paths[0], paths[1]);
	}
}
