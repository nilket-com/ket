//! The closed legend-content model and its single renderer.

use crate::coordinate::{Pt, Rect, Widget};
use crate::plot::common::{PlotBackend, ResolvedPlotSettings};
use crate::plot::continuous_color::ContinuousColorScale;
use crate::plot::gradient_stops::GradientStops;
use crate::plot::spec::{NonEmpty, ResolvedPresence};
use crate::plot::text_estimate::{ascii_label_advance_em, estimate_formatted_label_width};
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
	pub identity: Option<crate::plot::series::SeriesIdentity>,
	pub label: String,
	pub swatch: Color,
	pub sample: LegendSample,
	pub provenance: LegendProvenance,
}

#[derive(Clone, Debug, PartialEq)]
pub enum LegendSection {
	Entries(NonEmpty<LegendEntry>),
	Gradient {
		label: String,
		stops: GradientStops,
	},
	/// A closed integer count guide. Its two endpoint labels are exactly `0`
	/// and `maximum` in unabbreviated decimal, with no separators or
	/// intermediate ticks. The maximum also drives the source-stable width
	/// envelope required by PRODUCT-0143.
	CountGradient {
		label: String,
		stops: GradientStops,
		maximum: u64,
	},
	SizeSample {
		label: String,
		low: f64,
		high: f64,
	},
}

#[derive(Clone, Debug, PartialEq)]
pub struct LegendContent(pub NonEmpty<LegendSection>);

/// Attach exact preparation identities before printed union. This is strictly
/// positional: matching labels here would collapse distinct source values.
pub fn attach_identities(
	content: &mut LegendContent,
	identities: impl IntoIterator<Item = Option<crate::plot::series::SeriesIdentity>>,
) {
	let mut identities = identities.into_iter();
	let (first, rest) = content.0.clone().into_parts();
	let mut sections = std::iter::once(first).chain(rest).map(|section| {
		if let LegendSection::Entries(entries) = section {
			let (first, rest) = entries.into_parts();
			let mut entries = std::iter::once(first).chain(rest).map(|mut entry| {
				entry.identity = identities.next().flatten();
				entry
			});
			let mut result = NonEmpty::new(entries.next().unwrap());
			for entry in entries {
				result.push(entry);
			}
			LegendSection::Entries(result)
		} else {
			section
		}
	});
	let mut result = NonEmpty::new(sections.next().unwrap());
	for section in sections {
		result.push(section);
	}
	content.0 = result;
}

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

/// Construct PRODUCT-0143's deliberately closed count-guide contract.
pub(crate) fn count_gradient_section(
	label: String,
	scale: &ContinuousColorScale,
	maximum: u64,
) -> Option<LegendSection> {
	if maximum == 0 || scale.is_degenerate() {
		return None;
	}
	let stops = GradientStops::projected(scale)
		.expect("PRODUCT-0087 canonical positions project to an ascending domain");
	Some(LegendSection::CountGradient {
		label,
		stops,
		maximum,
	})
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
						sections.push(LegendSection::Entries(NonEmpty::new(
							entries.first().clone(),
						)));
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
								if values[current].identity != candidate.identity {
									values[current].identity = None;
								}
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

#[allow(dead_code)] // Retained for fixed-position compatibility tests.
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

#[allow(dead_code)] // Retained for fixed-position compatibility tests.
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
		identity: None,
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
			identity: None,
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
		identity: None,
		label: label.into(),
		swatch,
		sample: sample.clone(),
		provenance,
	});
	for (label, swatch) in iter {
		entries.push(LegendEntry {
			identity: None,
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
		settings.legend_size.pixels(),
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

const LEGEND_PADDING: f32 = 10.0;

fn entry_rectangle(origin: Point, offset: f32, index: usize, max_rows: usize) -> iced::Rectangle {
	iced::Rectangle::new(
		Point::new(
			origin.x + LEGEND_PADDING + (index / max_rows) as f32 * 150.0,
			origin.y + offset + LEGEND_PADDING + (index % max_rows) as f32 * 25.0,
		),
		Size::new(150.0, 25.0),
	)
}

#[derive(Clone, Debug)]
pub struct LegendHitGeometry {
	pub bounds: iced::Rectangle,
	pub entries: Vec<(iced::Rectangle, Option<crate::plot::series::SeriesIdentity>)>,
}

impl LegendHitGeometry {
	pub fn entry_at(
		&self,
		point: Point,
	) -> Option<&(iced::Rectangle, Option<crate::plot::series::SeriesIdentity>)> {
		self.entries
			.iter()
			.find(|(bounds, _)| bounds.contains(point))
	}
}

pub fn legend_hit_geometry(
	origin: Pt<Widget>,
	content: &LegendContent,
	settings: &ResolvedPlotSettings,
) -> LegendHitGeometry {
	let geometry = legend_geometry(content, settings);
	let origin = Point::new(origin.x(), origin.y());
	let mut entries = Vec::new();
	if let LegendGeometryKind::Stacked {
		section_offsets, ..
	} = &geometry.kind
	{
		for (section, offset) in content.sections().zip(section_offsets) {
			if let LegendSection::Entries(items) = section {
				for (index, entry) in items.iter().enumerate() {
					entries.push((
						entry_rectangle(
							origin,
							*offset,
							index,
							settings.max_legend_rows.max(1) as usize,
						),
						entry.identity.clone(),
					));
				}
			}
		}
	}
	LegendHitGeometry {
		bounds: iced::Rectangle::new(origin, geometry.size),
		entries,
	}
}
const GRADIENT_BAR_WIDTH: f32 = 15.0;
const GRADIENT_TEXT_GAP: f32 = 5.0;

#[derive(Clone, Debug, PartialEq)]
struct GradientLabels {
	low: String,
	high: String,
}

impl GradientLabels {
	fn new(stops: &GradientStops) -> Self {
		Self {
			low: format!("{:.1}", stops.first_value()),
			high: format!("{:.1}", stops.last_value()),
		}
	}

	fn estimated_width(&self, font_size: f32) -> f32 {
		estimate_formatted_label_width(&self.low, font_size)
			.max(estimate_formatted_label_width(&self.high, font_size))
	}

	fn count(maximum: u64) -> Self {
		Self {
			low: "0".to_string(),
			high: maximum.to_string(),
		}
	}

	fn count_envelope_width(maximum: u64, font_size: f32) -> f32 {
		let positions = maximum.to_string().len().max(1) as f32;
		let widest_digit = ('0'..='9')
			.map(ascii_label_advance_em)
			.fold(0.0_f32, f32::max);
		positions * widest_digit * font_size
	}
}

#[derive(Clone, Debug, PartialEq)]
struct GradientGeometry {
	bar: Point,
	bar_height: f32,
	endpoint_x: f32,
	title: Point,
	labels: GradientLabels,
}

#[derive(Clone, Debug, PartialEq)]
struct PairedLegendGeometry {
	gradient: GradientGeometry,
	size_x: f32,
	size_title: Point,
}

#[derive(Clone, Debug, PartialEq)]
enum LegendGeometryKind {
	Paired(PairedLegendGeometry),
	Stacked {
		section_offsets: Vec<f32>,
		gradients: Vec<Option<GradientGeometry>>,
	},
}

#[derive(Clone, Debug, PartialEq)]
struct LegendGeometry {
	size: Size,
	kind: LegendGeometryKind,
}

fn gradient_required_width(labels: &GradientLabels, font_size: f32) -> f32 {
	LEGEND_PADDING
		+ GRADIENT_BAR_WIDTH
		+ GRADIENT_TEXT_GAP
		+ labels.estimated_width(font_size)
		+ LEGEND_PADDING
}

fn count_gradient_required_width(maximum: u64, font_size: f32) -> f32 {
	LEGEND_PADDING
		+ GRADIENT_BAR_WIDTH
		+ GRADIENT_TEXT_GAP
		+ GradientLabels::count_envelope_width(maximum, font_size)
		+ LEGEND_PADDING
}

fn legend_geometry(content: &LegendContent, settings: &ResolvedPlotSettings) -> LegendGeometry {
	let sections: Vec<&LegendSection> = content.sections().collect();
	if let [
		LegendSection::Gradient { stops, .. },
		LegendSection::SizeSample { .. },
	] = sections.as_slice()
	{
		let labels = GradientLabels::new(stops);
		let bar_x = 180.0;
		let endpoint_x = bar_x + GRADIENT_BAR_WIDTH + 8.0;
		let width = 250.0_f32.max(
			endpoint_x + labels.estimated_width(settings.legend_size.pixels()) + LEGEND_PADDING,
		);
		return LegendGeometry {
			size: Size::new(width, 240.0),
			kind: LegendGeometryKind::Paired(PairedLegendGeometry {
				gradient: GradientGeometry {
					bar: Point::new(bar_x, 50.0),
					bar_height: 160.0,
					endpoint_x,
					title: Point::new(bar_x + GRADIENT_BAR_WIDTH / 2.0, 15.0),
					labels,
				},
				size_x: 20.0,
				size_title: Point::new(60.0, 15.0),
			}),
		};
	}

	let base_width = if sections.len() == 1 {
		match sections[0] {
			LegendSection::Entries(entries) => {
				let max_rows = settings.max_legend_rows.max(1) as usize;
				entries.len().div_ceil(max_rows) as f32 * 150.0 + LEGEND_PADDING * 2.0
			}
			LegendSection::Gradient { .. } => 60.0,
			LegendSection::CountGradient { .. } => 60.0,
			LegendSection::SizeSample { .. } => 180.0,
		}
	} else {
		250.0
	};
	let gradient_width = sections
		.iter()
		.filter_map(|section| match section {
			LegendSection::Gradient { stops, .. } => Some(gradient_required_width(
				&GradientLabels::new(stops),
				settings.legend_size.pixels(),
			)),
			LegendSection::CountGradient { maximum, .. } => Some(count_gradient_required_width(
				*maximum,
				settings.legend_size.pixels(),
			)),
			_ => None,
		})
		.fold(0.0_f32, f32::max);
	let width = base_width.max(gradient_width);
	let mut offset = 0.0;
	let mut section_offsets = Vec::with_capacity(sections.len());
	let mut gradients = Vec::with_capacity(sections.len());
	for section in &sections {
		section_offsets.push(offset);
		let gradient = match section {
			LegendSection::Gradient { stops, .. } => Some(GradientGeometry {
				bar: Point::new(LEGEND_PADDING, offset + 35.0),
				bar_height: 145.0,
				endpoint_x: LEGEND_PADDING + GRADIENT_BAR_WIDTH + GRADIENT_TEXT_GAP,
				title: Point::new(width / 2.0, offset + 10.0),
				labels: GradientLabels::new(stops),
			}),
			LegendSection::CountGradient {
				stops: _, maximum, ..
			} => Some(GradientGeometry {
				bar: Point::new(LEGEND_PADDING, offset + 35.0),
				bar_height: 145.0,
				endpoint_x: LEGEND_PADDING + GRADIENT_BAR_WIDTH + GRADIENT_TEXT_GAP,
				title: Point::new(width / 2.0, offset + 10.0),
				labels: GradientLabels::count(*maximum),
			}),
			_ => None,
		};
		gradients.push(gradient);
		offset += match section {
			LegendSection::Entries(entries) => {
				let max_rows = settings.max_legend_rows.max(1) as usize;
				entries.len().min(max_rows) as f32 * 25.0 + LEGEND_PADDING * 2.0
			}
			LegendSection::Gradient { .. } => 200.0,
			LegendSection::CountGradient { .. } => 200.0,
			LegendSection::SizeSample { .. } => 180.0,
		};
	}
	LegendGeometry {
		size: Size::new(width, offset),
		kind: LegendGeometryKind::Stacked {
			section_offsets,
			gradients,
		},
	}
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
	legend_geometry(content, settings).size
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

/// Resolve the displayed guide origin without letting viewport-derived content
/// resize a fallback gutter. Authored overlays do not reserve plot width, so
/// they retain their existing actual-content anchoring.
pub(crate) fn legend_origin_for_display(
	bounds: Rect<Widget>,
	reservation: LegendLayout,
	plot_extent: Rect<Widget>,
	content: &LegendContent,
	settings: &ResolvedPlotSettings,
) -> Option<Pt<Widget>> {
	match settings.legend {
		ResolvedPresence::Show(_) => {
			legend_layout(bounds, Some(content), settings).origin(plot_extent)
		}
		ResolvedPresence::Hide | ResolvedPresence::Fallback => reservation.origin(plot_extent),
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
	let geometry = legend_geometry(content, settings);
	let section_count = sections.len();
	let x = origin.x();
	let y = origin.y();
	// Bubble's two semantic sections were historically laid out as one
	// horizontal guide: size samples on the left and the color gradient on the
	// right. Keep that geometry while representing the content as two ordinary
	// sections; PRODUCT-0044 may later choose a different policy.
	if let LegendGeometryKind::Paired(pair) = &geometry.kind
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
		let width = geometry.size.width;
		let height = geometry.size.height;
		backend.fill_rectangle(
			Point::new(x, y),
			Size::new(width, height),
			Color {
				a: 0.7,
				..settings.chrome.legend_background.iced()
			},
		);
		let gradient = &pair.gradient;
		let bar_x = x + gradient.bar.x;
		let bar_y = y + gradient.bar.y;
		let bar_height = gradient.bar_height;
		for i in 0..50 {
			let t = i as f32 / 49.0;
			let step_height = bar_height / 50.0;
			backend.fill_rectangle(
				Point::new(bar_x, bar_y + bar_height - (i as f32 + 1.0) * step_height),
				Size::new(15.0, step_height + 0.5),
				sample_gradient(stops, t),
			);
		}
		let label_x = x + gradient.endpoint_x;
		draw_text_at(
			backend,
			gradient.labels.high.clone(),
			Point::new(label_x, bar_y),
			settings,
			alignment::Horizontal::Left,
			alignment::Vertical::Top,
			settings.legend_size.pixels(),
		);
		draw_text_at(
			backend,
			gradient.labels.low.clone(),
			Point::new(label_x, bar_y + bar_height),
			settings,
			alignment::Horizontal::Left,
			alignment::Vertical::Bottom,
			settings.legend_size.pixels(),
		);
		draw_text_at(
			backend,
			color_label.clone(),
			Point::new(x + gradient.title.x, y + gradient.title.y),
			settings,
			alignment::Horizontal::Center,
			alignment::Vertical::Top,
			settings.legend_size.pixels(),
		);
		let size_x = x + pair.size_x;
		draw_text_at(
			backend,
			size_label.clone(),
			Point::new(x + pair.size_title.x, y + pair.size_title.y),
			settings,
			alignment::Horizontal::Center,
			alignment::Vertical::Top,
			settings.legend_size.pixels(),
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
				settings.legend_size.pixels(),
			);
		}
		return;
	}
	let LegendGeometryKind::Stacked {
		section_offsets,
		gradients,
	} = &geometry.kind
	else {
		unreachable!("PRODUCT-0093 paired content uses paired legend geometry");
	};
	let width = geometry.size.width;
	let height = geometry.size.height;
	backend.fill_rectangle(
		Point::new(x, y),
		Size::new(width, height),
		Color {
			a: 0.6,
			..settings.chrome.legend_background.iced()
		},
	);

	for ((section, offset), gradient_geometry) in sections
		.into_iter()
		.zip(section_offsets.iter().copied())
		.zip(gradients.iter())
	{
		match section {
			LegendSection::Entries(entries) => {
				let max_rows = settings.max_legend_rows.max(1) as usize;
				for (index, entry) in entries.iter().enumerate() {
					let item = entry_rectangle(Point::new(x, y), offset, index, max_rows);
					let item_x = item.x;
					let item_y = item.y;
					let swatch = if settings.hidden_series.is_visible(entry.identity.as_ref()) {
						entry.swatch
					} else {
						Color {
							a: entry.swatch.a * 0.25,
							..entry.swatch
						}
					};
					match entry.sample {
						LegendSample::Swatch => backend.fill_rectangle(
							Point::new(item_x, item_y + 5.0),
							Size::new(15.0, 15.0),
							swatch,
						),
						LegendSample::LineSample => backend.stroke_path(
							&|builder| {
								builder.move_to(Point::new(item_x, item_y + 12.5));
								builder.line_to(Point::new(item_x + 20.0, item_y + 12.5));
							},
							Stroke {
								style: Style::Solid(swatch),
								width: 3.0,
								..Default::default()
							},
						),
						LegendSample::PointSample => backend.fill_path(
							&|builder| {
								builder.circle(Point::new(item_x + 6.0, item_y + 12.5), 6.0);
							},
							swatch,
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
						size: iced::Pixels(settings.legend_size.pixels()),
						align_x: alignment::Horizontal::Left.into(),
						align_y: alignment::Vertical::Center,
						..Default::default()
					});
				}
			}
			LegendSection::Gradient { label, stops }
			| LegendSection::CountGradient { label, stops, .. } => {
				let gradient = gradient_geometry
					.as_ref()
					.expect("PRODUCT-0093 gradient sections carry gradient geometry");
				let bar_x = x + gradient.bar.x;
				let bar_y = y + gradient.bar.y;
				let bar_height = gradient.bar_height;
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
					gradient.labels.high.clone(),
					Point::new(x + gradient.endpoint_x, bar_y),
					settings,
					alignment::Horizontal::Left,
					alignment::Vertical::Top,
					settings.legend_size.pixels(),
				);
				draw_text_at(
					backend,
					gradient.labels.low.clone(),
					Point::new(x + gradient.endpoint_x, bar_y + bar_height),
					settings,
					alignment::Horizontal::Left,
					alignment::Vertical::Bottom,
					settings.legend_size.pixels(),
				);
				if section_count > 1 || label != "Value" {
					draw_text_at(
						backend,
						label.clone(),
						Point::new(x + gradient.title.x, y + gradient.title.y),
						settings,
						alignment::Horizontal::Center,
						alignment::Vertical::Top,
						settings.legend_size.pixels(),
					);
				}
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
		texts: Vec<Text>,
	}

	impl PlotBackend for RectangleBackend {
		fn stroke_path(&mut self, _f: &dyn Fn(&mut dyn PathBuilder), _stroke: Stroke) {}

		fn fill_path(&mut self, _f: &dyn Fn(&mut dyn PathBuilder), _color: Color) {}

		fn fill_rectangle(&mut self, point: Point, size: Size, color: Color) {
			self.rectangles.push((point, size, color));
		}

		fn fill_text_raw(&mut self, text: Text) {
			self.texts.push(text);
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

	fn gradient_for_domain(theme: ColorTheme, domain: (f64, f64)) -> LegendSection {
		let scale = ContinuousColorScale::new(
			domain,
			ContinuousScaleIntent::SEQUENTIAL,
			ContinuousRamps {
				sequential: theme,
				diverging: ColorTheme::BlueWhiteRed,
			},
		)
		.expect("ascending non-degenerate scale");
		gradient_section("Value".to_owned(), &scale).expect("ascending non-degenerate gradient")
	}

	fn gradient(theme: ColorTheme) -> LegendSection {
		gradient_for_domain(theme, (10.0, 50.0))
	}

	fn gradient_contents(domain: (f64, f64)) -> (LegendContent, LegendContent) {
		let section = gradient_for_domain(ColorTheme::Viridis, domain);
		let ordinary = LegendContent(NonEmpty::new(section.clone()));
		let mut paired = LegendContent(NonEmpty::new(section));
		paired.push(LegendSection::SizeSample {
			label: "Size".to_owned(),
			low: 1.0,
			high: 3.0,
		});
		(ordinary, paired)
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
			identity: None,
			label: label.to_owned(),
			swatch: Color::from_rgb8(12, 34, 56),
			sample,
			provenance,
		}
	}

	#[test]
	fn product_0156_printed_union_preserves_ambiguity_in_every_order() {
		use crate::plot::series::SeriesIdentity;
		use crate::selection::{ExactScalar, FieldRef, ScalarType};
		let identity = |value| SeriesIdentity {
			field: FieldRef::new(
				"series",
				crate::plot::spec::SemanticType::Nominal,
				ScalarType::Int,
			),
			value: ExactScalar::Int(value),
		};
		let make = |identity| {
			let mut entry = entry(
				"same label",
				LegendSample::LineSample,
				LegendProvenance::Named,
			);
			entry.identity = identity;
			LegendContent::entries(NonEmpty::new(entry))
		};
		for identities in [
			[Some(identity(1)), Some(identity(2)), Some(identity(1))],
			[Some(identity(2)), Some(identity(1)), Some(identity(2))],
			[None, Some(identity(1)), Some(identity(1))],
			[Some(identity(1)), None, Some(identity(1))],
		] {
			let content = union(identities.into_iter().map(make)).unwrap();
			let LegendSection::Entries(entries) = content.0.first() else {
				panic!("entries")
			};
			assert_eq!(entries.len(), 1);
			assert!(entries.first().identity.is_none());
		}
		let content = union([make(Some(identity(1))), make(Some(identity(1)))]).unwrap();
		let LegendSection::Entries(entries) = content.0.first() else {
			panic!("entries")
		};
		assert_eq!(entries.first().identity, Some(identity(1)));
		let mut first = entry(
			"same label",
			LegendSample::LineSample,
			LegendProvenance::Named,
		);
		first.identity = Some(identity(1));
		let mut second = first.clone();
		second.identity = Some(identity(2));
		let mut entries = NonEmpty::new(first);
		entries.push(second);
		let content = union([LegendContent::entries(entries)]).unwrap();
		let LegendSection::Entries(entries) = content.0.first() else {
			panic!("entries")
		};
		assert_eq!(entries.len(), 1);
		assert!(entries.first().identity.is_none());
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

	fn assert_gradient_endpoints_fit(content: &LegendContent, settings: &ResolvedPlotSettings) {
		let geometry = legend_geometry(content, settings);
		match &geometry.kind {
			LegendGeometryKind::Paired(pair) => {
				let gradient = &pair.gradient;
				assert!(
					gradient.endpoint_x
						+ gradient
							.labels
							.estimated_width(settings.legend_size.pixels())
						+ LEGEND_PADDING <= geometry.size.width
				);
			}
			LegendGeometryKind::Stacked { gradients, .. } => {
				for gradient in gradients.iter().flatten() {
					assert!(
						gradient.endpoint_x
							+ gradient
								.labels
								.estimated_width(settings.legend_size.pixels())
							+ LEGEND_PADDING <= geometry.size.width
					);
				}
			}
		}
	}

	#[test]
	fn product_0093_gradient_widths_follow_negative_large_and_short_content_on_both_paths() {
		let settings = ResolvedPlotSettings::default();
		let negative = gradient_contents((-30.0, 40.0));
		let large = gradient_contents((5.0, 123_456.0));
		let short = gradient_contents((5.0, 9.0));

		for content in [
			&negative.0,
			&negative.1,
			&large.0,
			&large.1,
			&short.0,
			&short.1,
		] {
			assert_gradient_endpoints_fit(content, &settings);
		}
		assert!(measure_legend(&negative.0, &settings).width > 60.0);
		assert!(measure_legend(&negative.1, &settings).width > 250.0);
		assert!(
			measure_legend(&short.0, &settings).width < measure_legend(&large.0, &settings).width
		);
		assert!(
			measure_legend(&short.1, &settings).width < measure_legend(&large.1, &settings).width
		);
	}

	#[test]
	fn product_0093_measurement_and_rendering_share_geometry_on_both_paths() {
		let settings = ResolvedPlotSettings::default();
		let (ordinary, paired) = gradient_contents((-30.0, 40.0));
		for content in [&ordinary, &paired] {
			let expected = legend_geometry(content, &settings);
			let mut backend = RectangleBackend::default();
			render_legend(
				&mut backend,
				Rect::<Widget>::from_iced_frame(Rectangle::new(
					Point::ORIGIN,
					Size::new(400.0, 300.0),
				))
				.origin(),
				content,
				&settings,
			);
			assert_eq!(measure_legend(content, &settings), expected.size);
			assert_eq!(backend.rectangles[0].0, Point::ORIGIN);
			assert_eq!(backend.rectangles[0].1, expected.size);
			assert!(backend.texts.iter().any(|text| text.content == "-30.0"));
			assert!(backend.texts.iter().any(|text| text.content == "40.0"));
		}

		let ordinary_geometry = legend_geometry(&ordinary, &settings);
		let LegendGeometryKind::Stacked { gradients, .. } = ordinary_geometry.kind else {
			panic!("ordinary gradient geometry");
		};
		let ordinary_gradient = gradients[0].as_ref().unwrap();
		assert_eq!(ordinary_gradient.bar, Point::new(10.0, 35.0));

		let paired_geometry = legend_geometry(&paired, &settings);
		let LegendGeometryKind::Paired(pair) = paired_geometry.kind else {
			panic!("paired gradient geometry");
		};
		assert_eq!(pair.gradient.bar, Point::new(180.0, 50.0));
	}

	#[test]
	fn product_0093_estimator_honours_legend_size_and_the_declared_advance() {
		let (ordinary, paired) = gradient_contents((5.0, 123_456.0));
		let small = ResolvedPlotSettings {
			legend_size: crate::plot::spec::LegendTextSize::new(7.0).unwrap(),
			..ResolvedPlotSettings::default()
		};
		let normal = ResolvedPlotSettings::default();
		let large = ResolvedPlotSettings {
			legend_size: crate::plot::spec::LegendTextSize::new(28.0).unwrap(),
			..ResolvedPlotSettings::default()
		};
		for content in [&ordinary, &paired] {
			let small_width = measure_legend(content, &small).width;
			let normal_width = measure_legend(content, &normal).width;
			let large_width = measure_legend(content, &large).width;
			assert!(small_width < normal_width);
			assert!(normal_width < large_width);
		}

		assert_eq!(estimate_formatted_label_width("-30.0", 14.0), 52.5);
		assert_eq!(estimate_formatted_label_width("123456.0", 14.0), 84.0);
		assert_eq!(estimate_formatted_label_width("5.0", 14.0), 31.5);
	}

	#[test]
	fn product_0093_fixed_precision_endpoints_bound_every_mixed_domain_label() {
		let low = -30.0_f64;
		let high = 40.0_f64;
		let endpoint_width = format!("{low:.1}").len().max(format!("{high:.1}").len());
		for index in 0..50 {
			let value = low + (high - low) * index as f64 / 49.0;
			assert!(format!("{value:.1}").len() <= endpoint_width);
		}
	}

	fn count_content(maximum: u64) -> LegendContent {
		let scale = ContinuousColorScale::new(
			(0.0, maximum as f64),
			ContinuousScaleIntent::SEQUENTIAL,
			ContinuousRamps::default(),
		)
		.unwrap();
		LegendContent(NonEmpty::new(
			count_gradient_section("Count".to_owned(), &scale, maximum).unwrap(),
		))
	}

	#[test]
	fn product_0143_count_envelope_bounds_power_of_ten_and_widest_digit_traps() {
		let settings = ResolvedPlotSettings::default();
		for (source_count, actual_maximum) in [(1_000_u64, 999_u64), (10_000, 8_888)] {
			let reservation = count_content(source_count);
			let actual = count_content(actual_maximum);
			assert!(
				measure_legend(&actual, &settings).width
					<= measure_legend(&reservation, &settings).width
			);
		}
		assert_eq!(
			GradientLabels::count_envelope_width(1_000, 20.0),
			4.0 * ('0'..='9')
				.map(ascii_label_advance_em)
				.fold(0.0_f32, f32::max)
				* 20.0
		);
	}

	#[test]
	fn product_0143_count_guide_renders_only_closed_integer_endpoints() {
		let settings = ResolvedPlotSettings::default();
		let content = count_content(9_999);
		let mut backend = RectangleBackend::default();
		render_legend(
			&mut backend,
			Rect::<Widget>::from_iced_frame(Rectangle::new(Point::ORIGIN, Size::new(400.0, 300.0)))
				.origin(),
			&content,
			&settings,
		);
		let labels = backend
			.texts
			.iter()
			.map(|text| text.content.as_str())
			.collect::<Vec<_>>();
		assert_eq!(labels, ["9999", "0", "Count"]);
	}
}
