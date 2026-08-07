//! The closed legend-content model and its single renderer.

use crate::plot::common::{PlotBackend, ResolvedPlotSettings};
use crate::plot::spec::NonEmpty;
use iced::widget::canvas::{Stroke, Style, Text};
use iced::{Color, Point, Rectangle, Size, alignment};

#[derive(Clone, Debug, PartialEq)]
pub enum LegendSample {
	Swatch,
	LineSample,
	PointSample,
}

#[derive(Clone, Debug, PartialEq)]
pub struct LegendEntry {
	pub label: String,
	pub swatch: Color,
	pub sample: LegendSample,
}

#[derive(Clone, Debug, PartialEq)]
pub enum LegendSection {
	Entries(NonEmpty<LegendEntry>),
	Gradient {
		label: String,
		low: f64,
		high: f64,
		low_color: Color,
		high_color: Color,
	},
	SizeSample {
		label: String,
		low: f64,
		high: f64,
	},
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

pub fn entries_from_labels<I, S>(
	labels: I,
	settings: &ResolvedPlotSettings,
) -> Option<LegendContent>
where
	I: IntoIterator<Item = S>,
	S: Into<String>,
{
	entries_from_labels_with_sample(labels, settings, LegendSample::Swatch)
}

pub fn entries_from_labels_with_sample<I, S>(
	labels: I,
	settings: &ResolvedPlotSettings,
	sample: LegendSample,
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
		});
	}
	Some(LegendContent::entries(entries))
}

pub fn entries_from_pairs<I, S>(pairs: I) -> Option<LegendContent>
where
	I: IntoIterator<Item = (S, Color)>,
	S: Into<String>,
{
	entries_from_pairs_with_sample(pairs, LegendSample::Swatch)
}

pub fn entries_from_pairs_with_sample<I, S>(pairs: I, sample: LegendSample) -> Option<LegendContent>
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
	});
	for (label, swatch) in iter {
		entries.push(LegendEntry {
			label: label.into(),
			swatch,
			sample: sample.clone(),
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

fn lerp(a: Color, b: Color, t: f32) -> Color {
	Color {
		r: a.r + (b.r - a.r) * t,
		g: a.g + (b.g - a.g) * t,
		b: a.b + (b.b - a.b) * t,
		a: a.a + (b.a - a.a) * t,
	}
}

/// Render any closed legend content using the shared chrome roles.
pub fn render_legend(
	backend: &mut dyn PlotBackend,
	bounds: Rectangle,
	content: &LegendContent,
	settings: &ResolvedPlotSettings,
) {
	let sections: Vec<&LegendSection> = content.sections().collect();
	if sections.is_empty() {
		return;
	}
	let section_count = sections.len();
	let padding = 10.0;
	// Bubble's two semantic sections were historically laid out as one
	// horizontal guide: size samples on the left and the color gradient on the
	// right. Keep that geometry while representing the content as two ordinary
	// sections; PRODUCT-0044 may later choose a different policy.
	if sections.len() == 2
		&& let [
			LegendSection::Gradient {
				label: color_label,
				low,
				high,
				low_color,
				high_color,
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
		let x = (bounds.width - width) * settings.legend_x;
		let y = (bounds.height - height) * settings.legend_y;
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
				lerp(*low_color, *high_color, t),
			);
		}
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
	let height: f32 = sections
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
	let x = (bounds.width - width) * settings.legend_x;
	let y = (bounds.height - height) * settings.legend_y;
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
			LegendSection::Gradient {
				label,
				low,
				high,
				low_color,
				high_color,
			} => {
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
						lerp(*low_color, *high_color, t),
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
