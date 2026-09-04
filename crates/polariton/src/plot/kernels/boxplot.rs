use crate::plot::common::{
	AxisType, CoordinateTransformer, PlotBackend, PlotKernel, PlotLayout, ResolvedPlotSettings,
	format_label,
};
use crate::plot::exact_numeric::{ExactF64Error, exact_f64};
use crate::plot::kernels::boxplot_tukey::{TukeyBox, TukeyBoxRow, TukeyBoxes, TukeyBoxesError};
use crate::plot::spec::BoxPlotFields;
use iced::advanced::mouse::Cursor;
use iced::widget::canvas::{Stroke, Style};
use iced::{Point, Rectangle};
use polars::prelude::{AnyValue, DataFrame};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct BoxSelectionCandidates {
	pub box_candidate: crate::selection::SelectionCandidate,
	pub outlier_candidates: Vec<crate::selection::SelectionCandidate>,
}

const OUTLIER_RADIUS: f32 = 3.0;
const OUTLIER_OUTLINE_WIDTH: f32 = 1.0;
const OUTLIER_FOOTPRINT_RADIUS: f32 = OUTLIER_RADIUS + OUTLIER_OUTLINE_WIDTH / 2.0;

pub struct BoxPlotKernel {
	pub prepared_data: Arc<BoxPlotPreparedData>,
}

impl BoxPlotKernel {
	fn hit_outlier<'a>(
		&'a self,
		transform: &CoordinateTransformer<'_>,
		cursor: Point,
	) -> Option<OutlierHit<'a>> {
		let mut nearest: Option<(f32, OutlierHit<'a>)> = None;
		self.for_each_outlier(transform, |point, category| {
			let dx = cursor.x - point.center.x;
			let dy = cursor.y - point.center.y;
			let distance = dx * dx + dy * dy;
			if distance > OUTLIER_RADIUS * OUTLIER_RADIUS {
				return;
			}
			let replace = nearest.as_ref().is_none_or(|(best, hit)| {
				distance < *best || (distance == *best && point.ordinal < hit.ordinal)
			});
			if replace {
				nearest = Some((
					distance,
					OutlierHit {
						category,
						value: point.value,
						ordinal: point.ordinal,
					},
				));
			}
		});
		nearest.map(|(_, hit)| hit)
	}

	/// PRODUCT-0155 §3: the box stage's geometry — each category's closed
	/// band between its whiskers — shared by the hit and the region export.
	fn for_each_box<'a>(
		&'a self,
		transform: &CoordinateTransformer<'_>,
		mut visit: impl FnMut(usize, &'a str, &'a TukeyBox, BandEdges) -> bool,
	) {
		let PlotLayout::CategoricalX { categories, .. } = transform.layout else {
			return;
		};
		for (category, stats) in self.prepared_data.boxes.iter() {
			let Some(shared_index) = categories
				.iter()
				.position(|candidate| candidate == category)
			else {
				continue;
			};
			let (center, band_width) = transform.categorical(shared_index, 0.0);
			let low = transform.categorical(shared_index, stats.whisker_low()).0.y;
			let high = transform
				.categorical(shared_index, stats.whisker_high())
				.0
				.y;
			let band = BandEdges {
				left: center.x - band_width / 2.0,
				right: center.x + band_width / 2.0,
				top: low.min(high),
				bottom: low.max(high),
			};
			if !visit(shared_index, category, stats, band) {
				return;
			}
		}
	}

	/// The outlier stage's geometry: every outlier's centre with its category
	/// and ordinal, in the order the nearest scan visits them.
	fn for_each_outlier<'a>(
		&'a self,
		transform: &CoordinateTransformer<'_>,
		mut visit: impl FnMut(OutlierPoint, &'a str),
	) {
		let PlotLayout::CategoricalX { categories, .. } = transform.layout else {
			return;
		};
		for (category, stats) in self.prepared_data.boxes.iter() {
			let Some(shared_index) = categories
				.iter()
				.position(|candidate| candidate == category)
			else {
				continue;
			};
			for point in outlier_points(transform, shared_index, stats) {
				visit(point, category);
			}
		}
	}

	fn outlier_plot_hit(&self, hit: OutlierHit<'_>) -> crate::plot::common::PlotHit {
		let plot_hit = crate::plot::common::PlotHit::new(
			crate::plot::common::MarkFamily::BOXPLOT,
			crate::plot::common::HitDimensionality::Point,
			0,
		)
		.labeled(
			"Category",
			crate::plot::spec::SemanticType::Nominal,
			crate::plot::common::HitValue::nominal(hit.category),
		)
		.labeled(
			"Outlier",
			crate::plot::spec::SemanticType::Quantitative,
			crate::plot::common::axis_hit_value(
				hit.value,
				&self.prepared_data.y_axis_type,
				format_label(hit.value, &self.prepared_data.y_axis_type),
			),
		);
		let candidate = self
			.prepared_data
			.selection_candidates
			.get(hit.category)
			.and_then(|candidates| candidates.outlier_candidates.get(hit.ordinal))
			.cloned();
		match candidate {
			Some(candidate) => plot_hit.with_candidate(candidate),
			None => plot_hit,
		}
	}

	fn box_plot_hit(
		&self,
		shared_index: usize,
		category: &str,
		stats: &TukeyBox,
	) -> crate::plot::common::PlotHit {
		let yt = self.prepared_data.y_axis_type.clone();
		let labeled =
			|value: f64| crate::plot::common::axis_hit_value(value, &yt, format_label(value, &yt));
		let plot_hit = crate::plot::common::PlotHit::new(
			crate::plot::common::MarkFamily::BOXPLOT,
			crate::plot::common::HitDimensionality::Area,
			shared_index,
		)
		.labeled(
			"Category",
			crate::plot::spec::SemanticType::Nominal,
			crate::plot::common::HitValue::nominal(category),
		)
		.labeled(
			"Whisker high",
			crate::plot::spec::SemanticType::Quantitative,
			labeled(stats.whisker_high()),
		)
		.labeled(
			"Q3",
			crate::plot::spec::SemanticType::Quantitative,
			labeled(stats.q3()),
		)
		.labeled(
			"Median",
			crate::plot::spec::SemanticType::Quantitative,
			labeled(stats.median()),
		)
		.labeled(
			"Q1",
			crate::plot::spec::SemanticType::Quantitative,
			labeled(stats.q1()),
		)
		.labeled(
			"Whisker low",
			crate::plot::spec::SemanticType::Quantitative,
			labeled(stats.whisker_low()),
		);
		let candidate = self
			.prepared_data
			.selection_candidates
			.get(category)
			.map(|candidates| candidates.box_candidate.clone());
		match candidate {
			Some(candidate) => plot_hit.with_candidate(candidate),
			None => plot_hit,
		}
	}
}

impl PlotKernel for BoxPlotKernel {
	fn project_predicate(
		&self,
		predicate: &crate::selection::Predicate,
	) -> Option<Vec<Box<dyn crate::plot::common::EmphasisAnchor>>> {
		let mut anchors: Vec<Box<dyn crate::plot::common::EmphasisAnchor>> = Vec::new();
		for (category_index, (category, stats)) in self.prepared_data.boxes.iter().enumerate() {
			let candidates = self.prepared_data.selection_candidates.get(category)?;
			if predicate.matches_candidate(&candidates.box_candidate) {
				anchors.push(Box::new(BoxBandEmphasisAnchor {
					category_index,
					low: stats.whisker_low(),
					high: stats.whisker_high(),
				}));
			}
			for (ordinal, candidate) in candidates.outlier_candidates.iter().enumerate() {
				if predicate.matches_candidate(candidate)
					&& let Some(value) = stats.outliers().get(ordinal)
				{
					anchors.push(Box::new(BoxOutlierEmphasisAnchor {
						category_index,
						value: *value,
						ordinal,
					}));
				}
			}
		}
		Some(anchors)
	}

	fn layout(&self, settings: ResolvedPlotSettings) -> PlotLayout {
		PlotLayout::CategoricalX {
			categories: self.prepared_data.boxes.categories().to_vec(),
			y_axis_type: self.prepared_data.y_axis_type.clone(),
			y_range: (
				settings.y_min.unwrap_or(self.prepared_data.y_range.0),
				settings.y_max.unwrap_or(self.prepared_data.y_range.1),
			),
		}
	}

	fn plot(
		&self,
		backend: &mut dyn PlotBackend,
		_bounds: Rectangle,
		transform: &CoordinateTransformer,
		_cursor: Cursor,
		settings: ResolvedPlotSettings,
	) {
		for (category, stats) in self.prepared_data.boxes.iter() {
			let Some(shared_index) = transform.category_index(category) else {
				continue;
			};
			let (center, band_width) = transform.categorical(shared_index, 0.0);
			let box_width = band_width * 0.6;
			let left = center.x - box_width / 2.0;
			let right = center.x + box_width / 2.0;
			let color_iced = settings.mark_style.fill.iced();
			let detail_color = settings.mark_style.fill_detail.iced();
			let decoration_color = settings.mark_style.decoration.iced();
			let (q1_px, _) = transform.categorical(shared_index, stats.q1());
			let (q3_px, _) = transform.categorical(shared_index, stats.q3());
			let box_rect = Rectangle {
				x: left,
				y: q3_px.y,
				width: box_width,
				height: (q1_px.y - q3_px.y).abs().max(1.0),
			};
			backend.fill_rectangle(box_rect.position(), box_rect.size(), color_iced);
			let outline_stroke = Stroke {
				style: Style::Solid(detail_color),
				width: 1.0,
				..Default::default()
			};
			backend.stroke_path(
				&|builder| {
					builder.rectangle(box_rect.position(), box_rect.size());
				},
				outline_stroke,
			);
			let (median_px, _) = transform.categorical(shared_index, stats.median());
			let median_stroke = Stroke {
				style: Style::Solid(detail_color),
				width: 3.5,
				..Default::default()
			};
			backend.stroke_path(
				&|builder| {
					builder.move_to(iced::Point::new(left, median_px.y));
					builder.line_to(iced::Point::new(right, median_px.y));
				},
				median_stroke,
			);
			let whisker_stroke = Stroke {
				style: Style::Solid(decoration_color),
				width: 1.0,
				..Default::default()
			};
			let (min_px, _) = transform.categorical(shared_index, stats.whisker_low());
			let (max_px, _) = transform.categorical(shared_index, stats.whisker_high());
			backend.stroke_path(
				&|builder| {
					builder.move_to(iced::Point::new(center.x, min_px.y));
					builder.line_to(iced::Point::new(center.x, q1_px.y));
					builder.move_to(iced::Point::new(center.x, q3_px.y));
					builder.line_to(iced::Point::new(center.x, max_px.y));
				},
				whisker_stroke,
			);
			let cap_width = box_width * 0.4;
			let cap_stroke = Stroke {
				style: Style::Solid(decoration_color),
				width: 3.5,
				..Default::default()
			};
			backend.stroke_path(
				&|builder| {
					builder.move_to(iced::Point::new(center.x - cap_width / 2.0, min_px.y));
					builder.line_to(iced::Point::new(center.x + cap_width / 2.0, min_px.y));
					builder.move_to(iced::Point::new(center.x - cap_width / 2.0, max_px.y));
					builder.line_to(iced::Point::new(center.x + cap_width / 2.0, max_px.y));
				},
				cap_stroke,
			);
			for outlier in outlier_points(transform, shared_index, stats) {
				backend.fill_path(
					&|builder| builder.circle(outlier.center, OUTLIER_RADIUS),
					settings.mark_style.fill.iced(),
				);
				backend.stroke_path(
					&|builder| builder.circle(outlier.center, OUTLIER_RADIUS),
					Stroke {
						style: Style::Solid(settings.mark_style.fill_detail.iced()),
						width: OUTLIER_OUTLINE_WIDTH,
						..Default::default()
					},
				);
			}
		}
	}

	fn hit(
		&self,
		transform: &CoordinateTransformer,
		cursor: Cursor,
	) -> Option<crate::plot::common::PlotHit> {
		let cursor_pos = cursor.position()?;
		if let Some(hit) = self.hit_outlier(transform, cursor_pos) {
			return Some(self.outlier_plot_hit(hit));
		}
		let mut found = None;
		self.for_each_box(transform, |shared_index, category, stats, band| {
			if cursor_pos.x >= band.left
				&& cursor_pos.x <= band.right
				&& cursor_pos.y >= band.top
				&& cursor_pos.y <= band.bottom
			{
				found = Some(self.box_plot_hit(shared_index, category, stats));
				return false;
			}
			true
		});
		found
	}
	fn hit_regions(
		&self,
		transform: &CoordinateTransformer,
		_settings: &crate::plot::theme::PlotSettings,
	) -> Option<Vec<crate::plot::common::ResolutionStage>> {
		let mut outlier_regions = Vec::new();
		self.for_each_outlier(transform, |point, category| {
			outlier_regions.push(crate::plot::common::RegionEntry {
				region: crate::plot::common::HitRegion::Circle {
					center: point.center,
					radius: OUTLIER_RADIUS,
				},
				ordinal: point.ordinal as u32,
				hit: self.outlier_plot_hit(OutlierHit {
					category,
					value: point.value,
					ordinal: point.ordinal,
				}),
			});
		});
		let mut box_regions = Vec::new();
		self.for_each_box(transform, |shared_index, category, stats, band| {
			box_regions.push(crate::plot::common::RegionEntry {
				region: crate::plot::common::HitRegion::Rectangle {
					x0: band.left,
					y0: band.top,
					x1: band.right,
					y1: band.bottom,
					inclusive_max: true,
				},
				ordinal: shared_index as u32,
				hit: self.box_plot_hit(shared_index, category, stats),
			});
			true
		});
		let stage = |policy, tie, regions| crate::plot::common::ResolutionStage {
			policy,
			tie,
			within: None,
			origin: iced::Point::ORIGIN,
			regions,
		};
		Some(vec![
			stage(
				crate::plot::common::ResolutionPolicy::NearestCentreWithin,
				crate::plot::common::TieRule::LowerOrdinalWins,
				outlier_regions,
			),
			stage(
				crate::plot::common::ResolutionPolicy::FirstInOrder,
				crate::plot::common::TieRule::LowerOrdinalWins,
				box_regions,
			),
		])
	}

	fn legend_content(
		&self,
		_settings: &ResolvedPlotSettings,
	) -> Option<crate::plot::legend::LegendContent> {
		None
	}

	fn x_label(&self) -> String {
		self.prepared_data.x_label.clone()
	}

	fn y_label(&self) -> String {
		self.prepared_data.y_label.clone()
	}
}

struct BoxBandEmphasisAnchor {
	category_index: usize,
	low: f64,
	high: f64,
}

impl crate::plot::common::EmphasisAnchor for BoxBandEmphasisAnchor {
	fn reproject(
		&self,
		transform: &CoordinateTransformer,
	) -> Option<crate::plot::common::EmphasisShape> {
		let (low, band) = transform.categorical(self.category_index, self.low);
		let (high, _) = transform.categorical(self.category_index, self.high);
		Some(crate::plot::common::EmphasisShape::Rectangle(Rectangle {
			x: low.x - band / 2.0,
			y: low.y.min(high.y),
			width: band,
			height: (low.y - high.y).abs().max(1.0),
		}))
	}
}

struct BoxOutlierEmphasisAnchor {
	category_index: usize,
	value: f64,
	ordinal: usize,
}

impl crate::plot::common::EmphasisAnchor for BoxOutlierEmphasisAnchor {
	fn reproject(
		&self,
		transform: &CoordinateTransformer,
	) -> Option<crate::plot::common::EmphasisShape> {
		let PlotLayout::CategoricalX { .. } = transform.layout else {
			return None;
		};
		let (_, stats_width) = transform.categorical(self.category_index, self.value);
		let mut center = transform.categorical(self.category_index, self.value).0;
		// Duplicate outliers are jittered by the renderer. The ordinal keeps the
		// anchor stable even though the exact source value is shared.
		center.x += ((self.ordinal % 3) as f32 - 1.0) * (stats_width * 0.02).min(5.0);
		Some(crate::plot::common::EmphasisShape::Circle {
			center,
			radius: 6.0,
		})
	}
}

pub struct BoxPlotPreparedData {
	pub boxes: TukeyBoxes,
	pub y_range: (f64, f64),
	pub y_axis_type: AxisType,
	pub x_label: String,
	pub y_label: String,
	pub selection_candidates: std::collections::HashMap<String, BoxSelectionCandidates>,
}

impl BoxPlotPreparedData {
	pub fn new(boxes: TukeyBoxes, y_axis_type: AxisType, x_label: String, y_label: String) -> Self {
		let y_range = boxes.ranges();
		Self {
			boxes,
			y_range,
			y_axis_type,
			x_label,
			y_label,
			selection_candidates: std::collections::HashMap::new(),
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BoxPlotFieldRole {
	Category,
	WhiskerLow,
	Q1,
	Median,
	Q3,
	WhiskerHigh,
	Outlier,
}

impl BoxPlotFieldRole {
	pub const fn name(self) -> &'static str {
		match self {
			Self::Category => "category",
			Self::WhiskerLow => "whisker_low",
			Self::Q1 => "q1",
			Self::Median => "median",
			Self::Q3 => "q3",
			Self::WhiskerHigh => "whisker_high",
			Self::Outlier => "outlier",
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BoxPlotPreparationError {
	Field {
		role: BoxPlotFieldRole,
		row: usize,
		value: String,
		reason: &'static str,
	},
	Shape(TukeyBoxesError),
}

impl BoxPlotPreparationError {
	pub fn field_role(&self) -> Option<BoxPlotFieldRole> {
		match self {
			Self::Field { role, .. } => Some(*role),
			Self::Shape(TukeyBoxesError::NonFiniteSummary { field, .. }) => match *field {
				"whisker_low" => Some(BoxPlotFieldRole::WhiskerLow),
				"q1" => Some(BoxPlotFieldRole::Q1),
				"median" => Some(BoxPlotFieldRole::Median),
				"q3" => Some(BoxPlotFieldRole::Q3),
				"whisker_high" => Some(BoxPlotFieldRole::WhiskerHigh),
				_ => None,
			},
			Self::Shape(TukeyBoxesError::NonFiniteOutlier { .. }) => {
				Some(BoxPlotFieldRole::Outlier)
			}
			Self::Shape(_) => None,
		}
	}
}

impl std::fmt::Display for BoxPlotPreparationError {
	fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Field {
				role,
				row,
				value,
				reason,
			} => write!(
				formatter,
				"box {} at row {} has value `{value}`: {reason}",
				role.name(),
				row + 1
			),
			Self::Shape(error) => error.fmt(formatter),
		}
	}
}

impl std::error::Error for BoxPlotPreparationError {}

pub fn prepare_box_plot_data(
	df: &DataFrame,
	fields: &BoxPlotFields,
) -> Result<BoxPlotPreparedData, BoxPlotPreparationError> {
	let columns = fields
		.all()
		.map(|field| df.column(field).expect("resolved box field is present"));
	let mut rows = Vec::with_capacity(df.height());
	let category_type = crate::selection::ScalarType::from_dtype(columns[0].dtype());
	let outlier_type = crate::selection::ScalarType::from_dtype(columns[6].dtype());
	let category_exact = crate::selection::exact_column_values(columns[0]);
	let outlier_exact = crate::selection::exact_column_values(columns[6]);
	let mut selection_candidates = std::collections::HashMap::new();
	for row in 0..df.height() {
		let category_value = columns[0].get(row).expect("row is in bounds");
		if matches!(category_value, AnyValue::Null) {
			return Err(BoxPlotPreparationError::Field {
				role: BoxPlotFieldRole::Category,
				row,
				value: "null".to_owned(),
				reason: "category must be non-null",
			});
		}
		let category = match category_value {
			AnyValue::String(value) => value.to_owned(),
			AnyValue::StringOwned(value) => value.as_str().to_owned(),
			other => other.to_string(),
		};
		let numeric = |index: usize, role: BoxPlotFieldRole| {
			exact_box_value(
				columns[index].get(row).expect("row is in bounds"),
				role,
				row,
			)
		};
		let whisker_low = numeric(1, BoxPlotFieldRole::WhiskerLow)?;
		let q1 = numeric(2, BoxPlotFieldRole::Q1)?;
		let median = numeric(3, BoxPlotFieldRole::Median)?;
		let q3 = numeric(4, BoxPlotFieldRole::Q3)?;
		let whisker_high = numeric(5, BoxPlotFieldRole::WhiskerHigh)?;
		let outlier_value = columns[6].get(row).expect("row is in bounds");
		let outlier = if matches!(outlier_value, AnyValue::Null) {
			None
		} else {
			Some(exact_box_value(
				outlier_value,
				BoxPlotFieldRole::Outlier,
				row,
			)?)
		};
		if let (Some(category_type), Some(category_values), Some(category_value)) = (
			&category_type,
			&category_exact,
			category_exact
				.as_ref()
				.and_then(|values| values.get(row))
				.and_then(Clone::clone),
		) {
			let category_constraint = (
				crate::selection::FieldRef::new(
					&fields.category,
					crate::plot::spec::SemanticType::Nominal,
					category_type.clone(),
				),
				category_value,
			);
			let entry = selection_candidates
				.entry(category.clone())
				.or_insert_with(|| BoxSelectionCandidates {
					box_candidate: crate::selection::SelectionCandidate {
						constraints: vec![category_constraint.clone()],
					},
					outlier_candidates: Vec::new(),
				});
			let _ = category_values;
			if outlier.is_some()
				&& let (Some(outlier_type), Some(outlier_value)) = (
					outlier_type.as_ref(),
					outlier_exact
						.as_ref()
						.and_then(|values| values.get(row))
						.and_then(Clone::clone),
				) {
				entry
					.outlier_candidates
					.push(crate::selection::SelectionCandidate {
						constraints: vec![
							category_constraint,
							(
								crate::selection::FieldRef::new(
									&fields.outlier,
									crate::plot::spec::SemanticType::Quantitative,
									outlier_type.clone(),
								),
								outlier_value,
							),
						],
					});
			}
		}
		rows.push(TukeyBoxRow {
			category,
			whisker_low,
			q1,
			median,
			q3,
			whisker_high,
			outlier,
		});
	}
	let boxes = TukeyBoxes::new(rows).map_err(BoxPlotPreparationError::Shape)?;
	let mut prepared = BoxPlotPreparedData::new(
		boxes,
		AxisType::Linear,
		fields.category.clone(),
		String::new(),
	);
	prepared.selection_candidates = selection_candidates;
	Ok(prepared)
}

fn exact_box_value(
	value: AnyValue<'_>,
	role: BoxPlotFieldRole,
	row: usize,
) -> Result<f64, BoxPlotPreparationError> {
	let original = value.to_string();
	exact_f64(value).map_err(|error| BoxPlotPreparationError::Field {
		role,
		row,
		value: original,
		reason: match error {
			ExactF64Error::Null => "summary value must be non-null",
			ExactF64Error::Inexact => "value cannot round-trip through f64 exactly",
		},
	})
}

#[derive(Clone, Copy, Debug)]
struct OutlierPoint {
	center: Point,
	value: f64,
	ordinal: usize,
}

#[derive(Clone, Copy, Debug)]
struct OutlierHit<'a> {
	category: &'a str,
	value: f64,
	ordinal: usize,
}

fn outlier_points(
	transform: &CoordinateTransformer<'_>,
	shared_index: usize,
	stats: &TukeyBox,
) -> Vec<OutlierPoint> {
	let (_, band_width) = transform.categorical(shared_index, 0.0);
	let usable_span = (band_width - 2.0 * OUTLIER_FOOTPRINT_RADIUS).max(0.0);
	stats
		.outliers()
		.iter()
		.copied()
		.enumerate()
		.map(|(ordinal, value)| {
			let cluster_size = stats
				.outliers()
				.iter()
				.filter(|candidate| **candidate == value)
				.count();
			let cluster_index = stats.outliers()[..ordinal]
				.iter()
				.filter(|candidate| **candidate == value)
				.count();
			let spacing = if cluster_size == 1 {
				0.0
			} else {
				(2.5 * OUTLIER_RADIUS).min(usable_span / (cluster_size - 1) as f32)
			};
			let offset = (cluster_index as f32 - (cluster_size - 1) as f32 / 2.0) * spacing;
			let (mut center, _) = transform.categorical(shared_index, value);
			center.x += offset;
			OutlierPoint {
				center,
				value,
				ordinal,
			}
		})
		.collect()
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::plot::chrome::{PlotMarkStyle, PlotRgba};
	use crate::plot::common::PathBuilder;
	use crate::plot::kernels::boxplot_tukey::TukeyBoxRow;
	use iced::{Color, Size, Vector};
	use polars::df;

	const BANKED_PRE_TUKEY_HOVER: &str = "A\nMax: 4\nQ3: 3\nMedian: 2\nQ1: 1\nMin: 0";

	#[derive(Default)]
	struct RecordingPath {
		circles: Vec<(Point, f32)>,
		rectangles: Vec<(Point, Size)>,
	}

	impl PathBuilder for RecordingPath {
		fn move_to(&mut self, _point: Point) {}
		fn line_to(&mut self, _point: Point) {}
		fn arc_to(&mut self, _center: Point, _radius: f32, _start_angle: f32, _end_angle: f32) {}
		fn circle(&mut self, center: Point, radius: f32) {
			self.circles.push((center, radius));
		}
		fn rectangle(&mut self, top_left: Point, size: Size) {
			self.rectangles.push((top_left, size));
		}
		fn close(&mut self) {}
	}

	#[derive(Default)]
	struct RecordingBackend {
		filled_rectangles: Vec<(Point, Size, Color)>,
		filled_circles: Vec<(Point, f32, Color)>,
		stroked_circles: Vec<(Point, f32, Color, f32)>,
		stroke_colors: Vec<Color>,
		stroke_widths: Vec<f32>,
	}

	impl PlotBackend for RecordingBackend {
		fn stroke_path(&mut self, f: &dyn Fn(&mut dyn PathBuilder), stroke: Stroke) {
			let mut path = RecordingPath::default();
			f(&mut path);
			let color = match stroke.style {
				Style::Solid(color) => color,
				Style::Gradient(_) => panic!("box decoration is solid"),
			};
			self.stroke_colors.push(color);
			self.stroke_widths.push(stroke.width);
			self.stroked_circles.extend(
				path.circles
					.into_iter()
					.map(|(center, radius)| (center, radius, color, stroke.width)),
			);
		}

		fn fill_path(&mut self, f: &dyn Fn(&mut dyn PathBuilder), color: Color) {
			let mut path = RecordingPath::default();
			f(&mut path);
			self.filled_circles.extend(
				path.circles
					.into_iter()
					.map(|(center, radius)| (center, radius, color)),
			);
		}

		fn fill_rectangle(&mut self, top_left: Point, size: Size, color: Color) {
			self.filled_rectangles.push((top_left, size, color));
		}

		fn fill_text_raw(&mut self, _text: iced::widget::canvas::Text) {}
		fn translate(&mut self, _translation: Vector) {}
		fn rotate(&mut self, _angle: f32) {}
		fn with_save(&mut self, f: &mut dyn FnMut(&mut dyn PlotBackend)) {
			f(self);
		}
		fn with_clip(&mut self, _bounds: Rectangle, f: &mut dyn FnMut(&mut dyn PlotBackend)) {
			f(self);
		}
	}

	fn row(category: &str, outlier: Option<f64>) -> TukeyBoxRow {
		TukeyBoxRow {
			category: category.to_owned(),
			whisker_low: 0.0,
			q1: 1.0,
			median: 2.0,
			q3: 3.0,
			whisker_high: 4.0,
			outlier,
		}
	}

	fn make_kernel(rows: Vec<TukeyBoxRow>) -> BoxPlotKernel {
		BoxPlotKernel {
			prepared_data: Arc::new(BoxPlotPreparedData::new(
				TukeyBoxes::new(rows).expect("valid Tukey rows"),
				AxisType::Linear,
				"category".to_owned(),
				"value".to_owned(),
			)),
		}
	}

	fn make_transform<'a>(
		layout: &'a PlotLayout,
		width: f32,
		height: f32,
	) -> CoordinateTransformer<'a> {
		CoordinateTransformer::new(
			layout,
			Rectangle::new(Point::ORIGIN, Size::new(width, height)),
		)
	}

	fn render(
		kernel: &BoxPlotKernel,
		settings: ResolvedPlotSettings,
		bounds: Rectangle,
	) -> RecordingBackend {
		let layout = kernel.layout(settings.clone());
		let transform = CoordinateTransformer::new(&layout, bounds);
		let mut backend = RecordingBackend::default();
		kernel.plot(
			&mut backend,
			bounds,
			&transform,
			Cursor::Unavailable,
			settings,
		);
		backend
	}

	#[test]
	fn product_0081_hover_diff_changes_only_the_two_whisker_labels() {
		let kernel = make_kernel(vec![row("A", None)]);
		let layout = kernel.layout(ResolvedPlotSettings::default());
		let bounds = Rectangle::new(Point::ORIGIN, Size::new(200.0, 200.0));
		let transform = CoordinateTransformer::new(&layout, bounds);
		let cursor = Cursor::Available(transform.categorical(0, 2.0).0);
		let expected = BANKED_PRE_TUKEY_HOVER
			.replacen("Max", "Whisker high", 1)
			.replacen("Min", "Whisker low", 1);
		assert_eq!(
			kernel.hover(&transform, cursor).as_deref(),
			Some(expected.as_str())
		);
	}

	#[test]
	fn product_0081_coincident_offsets_preserve_values_and_contain_outlined_footprints() {
		let twin = make_kernel(vec![row("twin", Some(99.0)), row("twin", Some(99.0))]);
		let layout = twin.layout(ResolvedPlotSettings::default());
		let transform = make_transform(&layout, 10.0, 100.0);
		let (_, stats) = twin.prepared_data.boxes.iter().next().unwrap();
		let points = outlier_points(&transform, 0, stats);
		assert_eq!(points.len(), 2);
		assert_eq!(points[0].center.y, points[1].center.y);
		assert_eq!([points[0].center.x, points[1].center.x], [3.5, 6.5]);
		for point in &points {
			assert!((point.center.x - 5.0).abs() + OUTLIER_FOOTPRINT_RADIUS <= 5.0);
		}

		let triple = make_kernel(vec![
			row("triple", Some(99.0)),
			row("triple", Some(99.0)),
			row("triple", Some(99.0)),
		]);
		let layout = triple.layout(ResolvedPlotSettings::default());
		let transform = make_transform(&layout, 40.0, 100.0);
		let (_, stats) = triple.prepared_data.boxes.iter().next().unwrap();
		let points = outlier_points(&transform, 0, stats);
		assert_eq!(
			points
				.iter()
				.map(|point| point.center.x)
				.collect::<Vec<_>>(),
			[12.5, 20.0, 27.5]
		);

		let distinct = make_kernel(vec![
			row("distinct", Some(-50.0)),
			row("distinct", Some(200.0)),
		]);
		let layout = distinct.layout(ResolvedPlotSettings::default());
		let transform = make_transform(&layout, 40.0, 100.0);
		let (_, stats) = distinct.prepared_data.boxes.iter().next().unwrap();
		assert!(
			outlier_points(&transform, 0, stats)
				.iter()
				.all(|point| point.center.x == 20.0)
		);

		let signed_zero = TukeyBoxRow {
			whisker_low: 1.0,
			q1: 2.0,
			median: 3.0,
			q3: 4.0,
			whisker_high: 5.0,
			outlier: Some(-0.0),
			..row("zero", None)
		};
		let positive_zero = TukeyBoxRow {
			outlier: Some(0.0),
			..signed_zero.clone()
		};
		let zeroes = make_kernel(vec![signed_zero, positive_zero]);
		let layout = zeroes.layout(ResolvedPlotSettings::default());
		let transform = make_transform(&layout, 40.0, 100.0);
		let (_, stats) = zeroes.prepared_data.boxes.iter().next().unwrap();
		let points = outlier_points(&transform, 0, stats);
		assert_ne!(points[0].center.x, points[1].center.x);
	}

	#[test]
	fn product_0081_outlier_points_use_resolved_mark_style_and_pinned_geometry() {
		let kernel = make_kernel(vec![row("A", Some(9.0))]);
		let bounds = Rectangle::new(Point::ORIGIN, Size::new(200.0, 200.0));
		let first = render(&kernel, ResolvedPlotSettings::default(), bounds);
		let second_settings = ResolvedPlotSettings {
			mark_style: PlotMarkStyle::legacy_from_iced(
				Color::from_rgb(0.2, 0.3, 0.4),
				Color::from_rgb(0.7, 0.6, 0.5),
			),
			..ResolvedPlotSettings::default()
		};
		let second = render(&kernel, second_settings, bounds);
		assert_eq!(first.filled_circles.len(), 1);
		assert_eq!(first.stroked_circles.len(), 1);
		assert_eq!(first.filled_circles[0].1, OUTLIER_RADIUS);
		assert_eq!(first.stroked_circles[0].1, OUTLIER_RADIUS);
		assert_eq!(first.stroked_circles[0].3, OUTLIER_OUTLINE_WIDTH);
		assert_ne!(first.filled_circles[0].2, second.filled_circles[0].2);
		assert_ne!(first.stroked_circles[0].2, second.stroked_circles[0].2);
	}

	#[test]
	fn product_0083_box_elements_route_through_distinct_semantic_roles() {
		let kernel = make_kernel(vec![row("A", Some(9.0))]);
		let fill = PlotRgba::opaque(0.1, 0.2, 0.3);
		let decoration = PlotRgba::opaque(0.2, 0.3, 0.4);
		let fill_detail = PlotRgba::opaque(0.3, 0.4, 0.5);
		let label_text = PlotRgba::opaque(0.4, 0.5, 0.6);
		let separator = PlotRgba::opaque(0.5, 0.6, 0.7);
		let settings = ResolvedPlotSettings {
			mark_style: PlotMarkStyle {
				fill,
				decoration,
				fill_detail,
				label_text,
				separator,
			},
			..ResolvedPlotSettings::default()
		};
		let rendered = render(
			&kernel,
			settings,
			Rectangle::new(Point::ORIGIN, Size::new(200.0, 200.0)),
		);
		assert_eq!(rendered.filled_rectangles[0].2, fill.iced());
		assert_eq!(rendered.filled_circles[0].2, fill.iced());
		assert_eq!(
			rendered.stroke_colors,
			vec![
				fill_detail.iced(),
				fill_detail.iced(),
				decoration.iced(),
				decoration.iced(),
				fill_detail.iced(),
			],
			"outline, median, whisker, cap, and outlier outline must retain distinct semantic routing"
		);
		assert_eq!(
			rendered.stroke_widths,
			vec![1.0, 3.5, 1.0, 3.5, OUTLIER_OUTLINE_WIDTH],
			"PRODUCT-0083 changes role selection without changing geometry"
		);
		assert!(!rendered.stroke_colors.contains(&label_text.iced()));
		assert!(!rendered.stroke_colors.contains(&separator.iced()));
	}

	#[test]
	fn product_0081_point_hover_wins_and_gaps_do_not_report_a_box() {
		let kernel = make_kernel(vec![row("A", Some(9.0)), row("A", Some(9.1))]);
		let layout = kernel.layout(ResolvedPlotSettings::default());
		let transform = make_transform(&layout, 10.0, 100.0);
		let (_, stats) = kernel.prepared_data.boxes.iter().next().unwrap();
		let points = outlier_points(&transform, 0, stats);
		for point in &points {
			assert_eq!(
				kernel
					.hover(&transform, Cursor::Available(point.center))
					.as_deref(),
				Some(if point.value == 9.0 {
					"A\nOutlier: 9"
				} else {
					"A\nOutlier: 9.1"
				})
			);
		}
		let midpoint = Point::new(
			points[0].center.x,
			(points[0].center.y + points[1].center.y) / 2.0,
		);
		assert_eq!(
			kernel
				.hover(&transform, Cursor::Available(midpoint))
				.as_deref(),
			Some("A\nOutlier: 9"),
			"equidistant distinct singletons tie to producer ordinal zero"
		);
		assert_eq!(kernel.hit_outlier(&transform, midpoint).unwrap().ordinal, 0);
		let gap = transform.categorical(0, 6.0).0;
		assert_eq!(kernel.hover(&transform, Cursor::Available(gap)), None);

		let twins = make_kernel(vec![row("twins", Some(9.0)), row("twins", Some(9.0))]);
		let layout = twins.layout(ResolvedPlotSettings::default());
		let transform = make_transform(&layout, 10.0, 100.0);
		let (_, stats) = twins.prepared_data.boxes.iter().next().unwrap();
		let points = outlier_points(&transform, 0, stats);
		let midpoint = Point::new(
			(points[0].center.x + points[1].center.x) / 2.0,
			points[0].center.y,
		);
		assert_eq!(
			twins.hit_outlier(&transform, midpoint).unwrap().ordinal,
			0,
			"equidistant members of one cluster tie to producer ordinal zero"
		);

		let adjacent = make_kernel(vec![row("edge", Some(4.1))]);
		let layout = adjacent.layout(ResolvedPlotSettings::default());
		let transform = make_transform(&layout, 200.0, 100.0);
		let whisker_edge = transform.categorical(0, 4.0).0;
		assert_eq!(
			adjacent
				.hover(&transform, Cursor::Available(whisker_edge))
				.as_deref(),
			Some("edge\nOutlier: 4.1"),
			"point hit wins where its radius overlaps the box span"
		);
	}

	#[test]
	fn product_0081_flat_and_single_boxes_keep_the_one_pixel_floor() {
		for category in ["flat", "single"] {
			let flat = TukeyBoxRow {
				category: category.to_owned(),
				whisker_low: 5.0,
				q1: 5.0,
				median: 5.0,
				q3: 5.0,
				whisker_high: 5.0,
				outlier: None,
			};
			let kernel = make_kernel(vec![flat]);
			let rendered = render(
				&kernel,
				ResolvedPlotSettings::default(),
				Rectangle::new(Point::ORIGIN, Size::new(200.0, 200.0)),
			);
			assert_eq!(rendered.filled_rectangles[0].1.height, 1.0);
		}
	}

	#[test]
	fn product_0081_box_probe_covers_all_five_traced_groups_end_to_end() {
		let summary = |category: &str,
		               whisker_low: f64,
		               q1: f64,
		               median: f64,
		               q3: f64,
		               whisker_high: f64,
		               outlier: Option<f64>| TukeyBoxRow {
			category: category.to_owned(),
			whisker_low,
			q1,
			median,
			q3,
			whisker_high,
			outlier,
		};
		let kernel = make_kernel(vec![
			summary("flat", 5.0, 5.0, 5.0, 5.0, 5.0, None),
			summary("lowhigh", 30.0, 31.0, 33.0, 34.0, 35.0, Some(-50.0)),
			summary("lowhigh", 30.0, 31.0, 33.0, 34.0, 35.0, Some(200.0)),
			summary("none", 20.0, 21.0, 23.0, 24.0, 25.0, None),
			summary("single", 7.0, 7.0, 7.0, 7.0, 7.0, None),
			summary("twin", 10.0, 12.0, 14.0, 16.0, 16.0, Some(99.0)),
			summary("twin", 10.0, 12.0, 14.0, 16.0, 16.0, Some(99.0)),
		]);
		assert_eq!(
			kernel.prepared_data.boxes.categories(),
			["flat", "lowhigh", "none", "single", "twin"]
		);
		let bounds = Rectangle::new(Point::ORIGIN, Size::new(500.0, 300.0));
		let settings = ResolvedPlotSettings::default();
		let layout = kernel.layout(settings.clone());
		let transform = CoordinateTransformer::new(&layout, bounds);
		let rendered = render(&kernel, settings, bounds);
		assert_eq!(rendered.filled_rectangles.len(), 5);
		assert_eq!(rendered.filled_circles.len(), 4);
		for (index, (category, stats)) in kernel.prepared_data.boxes.iter().enumerate() {
			let summary_cursor = transform.categorical(index, stats.median()).0;
			assert!(
				kernel
					.hover(&transform, Cursor::Available(summary_cursor))
					.expect("every traced box is hoverable")
					.starts_with(category)
			);
			for point in outlier_points(&transform, index, stats) {
				assert!(
					kernel
						.hover(&transform, Cursor::Available(point.center))
						.expect("every traced outlier is hoverable")
						.contains("Outlier:")
				);
			}
		}
	}

	fn aliased_fields() -> BoxPlotFields {
		BoxPlotFields {
			category: "group".to_owned(),
			whisker_low: "lo".to_owned(),
			q1: "p25".to_owned(),
			median: "mid".to_owned(),
			q3: "p75".to_owned(),
			whisker_high: "hi".to_owned(),
			outlier: "point".to_owned(),
		}
	}

	#[test]
	fn product_0082_positional_ingestion_accepts_both_outlier_shapes_and_mixed_dtypes() {
		let data = df!(
			"group" => ["none", "all"],
			"lo" => [0_i32, 10],
			"p25" => [1.0_f32, 11.0],
			"mid" => [2_i64, 12],
			"p75" => [3.0_f64, 13.0],
			"hi" => [4_u32, 14],
			"point" => [None, Some(20.0_f64)],
		)
		.unwrap();
		let prepared = prepare_box_plot_data(&data, &aliased_fields()).expect("valid box relation");
		assert_eq!(prepared.boxes.categories(), ["none", "all"]);
		assert!(
			prepared
				.boxes
				.iter()
				.next()
				.unwrap()
				.1
				.outliers()
				.is_empty()
		);
		assert_eq!(prepared.boxes.iter().nth(1).unwrap().1.outliers(), [20.0]);
		assert_eq!(prepared.x_label, "group");
		assert_eq!(prepared.y_label, "");
		assert_eq!(prepared.y_axis_type, AxisType::Linear);
	}

	#[test]
	fn product_0151_box_and_outlier_candidates_have_the_closed_field_sets() {
		let data = df!(
			"group" => ["all"],
			"lo" => [0_i64],
			"p25" => [1_i64],
			"mid" => [2_i64],
			"p75" => [3_i64],
			"hi" => [4_i64],
			"point" => [Some(20_i64)],
		)
		.unwrap();
		let prepared = prepare_box_plot_data(&data, &aliased_fields()).expect("valid box relation");
		let candidates = prepared
			.selection_candidates
			.get("all")
			.expect("category candidates");
		assert_eq!(
			candidates
				.box_candidate
				.constraints
				.iter()
				.map(|(field, _)| field.name.as_str())
				.collect::<Vec<_>>(),
			["group"]
		);
		assert_eq!(
			candidates.outlier_candidates[0]
				.constraints
				.iter()
				.map(|(field, _)| field.name.as_str())
				.collect::<Vec<_>>(),
			["group", "point"]
		);

		let predicate =
			crate::selection::Predicate::from_candidate(&candidates.outlier_candidates[0])
				.expect("outlier predicate");
		let kernel = BoxPlotKernel {
			prepared_data: Arc::new(prepared),
		};
		assert_eq!(
			kernel
				.project_predicate(&predicate)
				.expect("box plot admits selection")
				.len(),
			1,
			"the two-field outlier predicate emphasizes the outlier, not the whole band"
		);
	}

	#[test]
	fn product_0082_lossless_integer_boundary_is_representability_not_magnitude() {
		let make = |value: i64| {
			df!(
				"group" => ["exact"],
				"lo" => [value],
				"p25" => [value],
				"mid" => [value],
				"p75" => [value],
				"hi" => [value],
				"point" => [None::<i64>],
			)
			.unwrap()
		};
		for value in [9_007_199_254_740_992_i64, 9_007_199_254_740_994_i64] {
			let prepared = prepare_box_plot_data(&make(value), &aliased_fields()).unwrap();
			assert_eq!(
				prepared.boxes.iter().next().unwrap().1.median(),
				value as f64
			);
		}
		let error = match prepare_box_plot_data(&make(9_007_199_254_740_993_i64), &aliased_fields())
		{
			Ok(_) => panic!("inexact integer was accepted"),
			Err(error) => error,
		};
		assert!(matches!(
			error,
			BoxPlotPreparationError::Field {
				role: BoxPlotFieldRole::WhiskerLow,
				row: 0,
				ref value,
				..
			} if value == "9007199254740993"
		));
	}

	#[test]
	fn product_0082_box_fill_is_one_resolved_role_and_legend_is_absent() {
		let kernel = make_kernel(vec![row("A", None), row("B", None), row("C", None)]);
		let settings = ResolvedPlotSettings {
			mark_style: PlotMarkStyle::legacy_from_iced(
				Color::from_rgb(0.1, 0.2, 0.3),
				Color::from_rgb(0.7, 0.4, 0.2),
			),
			..ResolvedPlotSettings::default()
		};
		let expected = settings.mark_style.fill.iced();
		let rendered = render(
			&kernel,
			settings.clone(),
			Rectangle::new(Point::ORIGIN, Size::new(300.0, 200.0)),
		);
		assert_eq!(rendered.filled_rectangles.len(), 3);
		assert!(
			rendered
				.filled_rectangles
				.iter()
				.all(|entry| entry.2 == expected)
		);
		assert_eq!(kernel.legend_content(&settings), None);
	}
}

/// A category band's closed edges, exactly as the hit test compares them.
struct BandEdges {
	left: f32,
	right: f32,
	top: f32,
	bottom: f32,
}
