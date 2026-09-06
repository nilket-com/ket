use crate::plot::common::{
	CoordinateTransformer, Orientation, PlotBackend, PlotKernel, PlotLayout, ResolvedPlotSettings,
};
use crate::plot::legend::LegendProvenance;
use crate::plot::spec::{BandFillAssignment, CategoryColorAssignment, LayerSettings};
use crate::plot::theme::PlotSettings;
use iced::advanced::mouse::Cursor;
use iced::{Color, Rectangle};
use polars::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;

pub struct BarPlotKernel {
	pub prepared_data: Arc<BarPreparedData>,
	pub orientation: Orientation,
	pub layer_settings: LayerSettings,
	pub band_fills: Vec<BandFillAssignment>,
	/// PRODUCT-0029 §1: the renderer consumes only resolved colors. An authored
	/// categorical channel supplies one assignment per series; sampling
	/// `ColorTheme` by render index is the sin that record's Rejected list names.
	pub group_colors: Vec<CategoryColorAssignment>,
}

impl BarPlotKernel {
	/// The exact resolved colour of one series, when an authored channel
	/// assigned one. Bars with neither band fill nor an authored channel keep
	/// the legacy single-treatment ramp sample.
	fn group_color(&self, group_index: usize) -> Option<Color> {
		let name = self.prepared_data.group_names.get(group_index)?;
		let assignment = self
			.group_colors
			.iter()
			.find(|assignment| assignment.category == *name)?;
		Some(Color::from_rgba8(
			assignment.color.red,
			assignment.color.green,
			assignment.color.blue,
			assignment.color.alpha as f32 / u8::MAX as f32,
		))
	}
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct BarRectangle {
	rect: Rectangle,
	category_index: usize,
	group_index: usize,
}

fn category_label(value: AnyValue<'_>) -> String {
	if let AnyValue::String(value) = value {
		value.to_string()
	} else {
		value.to_string().replace('"', "")
	}
}

impl BarPlotKernel {
	fn bar_rectangles(
		&self,
		transform: &CoordinateTransformer,
		width_fraction: f32,
		category_gap_fraction: f32,
	) -> Vec<BarRectangle> {
		let num_groups = self.prepared_data.group_names.len();
		let mut bars = Vec::with_capacity(
			self.prepared_data
				.categories
				.len()
				.saturating_mul(num_groups),
		);
		match self.orientation {
			Orientation::Vertical => {
				let y_base = if let PlotLayout::CategoricalX { y_range, .. } = transform.layout {
					y_range.0
				} else {
					0.0
				};
				for (category_index, category) in self.prepared_data.categories.iter().enumerate() {
					let Some(shared_index) = transform.category_index(category) else {
						continue;
					};
					let (center, band) = transform.categorical(shared_index, y_base);
					let cluster = band * (1.0 - category_gap_fraction);
					let category_inset = (band - cluster) / 2.0;
					let slot = cluster / num_groups as f32;
					let padding = slot * (1.0 - width_fraction) / 2.0;
					let category_left = center.x - band / 2.0;
					for group_index in 0..num_groups {
						let value = self.prepared_data.values[category_index][group_index];
						if value <= y_base {
							continue;
						}
						let (top, _) = transform.categorical(shared_index, value);
						let (bottom, _) = transform.categorical(shared_index, y_base);
						bars.push(BarRectangle {
							rect: Rectangle {
								x: category_left
									+ category_inset + group_index as f32 * slot
									+ padding,
								y: top.y,
								width: slot * width_fraction,
								height: (bottom.y - top.y).max(1.0),
							},
							category_index,
							group_index,
						});
					}
				}
			}
			Orientation::Horizontal => {
				let x_base = if let PlotLayout::CategoricalY { x_range, .. } = transform.layout {
					x_range.0
				} else {
					0.0
				};
				for (category_index, category) in self.prepared_data.categories.iter().enumerate() {
					let Some(shared_index) = transform.category_index(category) else {
						continue;
					};
					let (left, band) = transform.categorical(shared_index, x_base);
					let cluster = band * (1.0 - category_gap_fraction);
					let category_inset = (band - cluster) / 2.0;
					let slot = cluster / num_groups as f32;
					let padding = slot * (1.0 - width_fraction) / 2.0;
					let category_top = left.y - band / 2.0;
					for group_index in 0..num_groups {
						let value = self.prepared_data.values[category_index][group_index];
						if value <= x_base {
							continue;
						}
						let (right, _) = transform.categorical(shared_index, value);
						bars.push(BarRectangle {
							rect: Rectangle {
								x: left.x,
								y: category_top
									+ category_inset + group_index as f32 * slot
									+ padding,
								width: (right.x - left.x).max(1.0),
								height: slot * width_fraction,
							},
							category_index,
							group_index,
						});
					}
				}
			}
		}
		bars
	}

	fn hit_resolved(
		&self,
		transform: &CoordinateTransformer,
		cursor: Cursor,
		settings: &ResolvedPlotSettings,
	) -> Option<crate::plot::common::PlotHit> {
		let cursor_pos = cursor.position()?;
		let hit = self
			.bar_rectangles(
				transform,
				settings.bar_width.fraction(),
				settings.category_gap.fraction(),
			)
			.into_iter()
			.find(|bar| bar.rect.contains(cursor_pos))?;
		let category = &self.prepared_data.categories[hit.category_index];
		let group = &self.prepared_data.group_names[hit.group_index];
		let approximate_value = match transform.layout {
			PlotLayout::CategoricalX { y_range, .. } => {
				let scale = transform.bounds.height as f64 / (y_range.1 - y_range.0);
				y_range.0 + (transform.bounds.height - cursor_pos.y) as f64 / scale
			}
			PlotLayout::CategoricalY { x_range, .. } => {
				let scale = transform.bounds.width as f64 / (x_range.1 - x_range.0);
				x_range.0 + cursor_pos.x as f64 / scale
			}
			_ => return None,
		};
		let plot_hit = crate::plot::common::PlotHit::new(
			crate::plot::common::MarkFamily::BAR,
			crate::plot::common::HitDimensionality::Area,
			hit.category_index * self.prepared_data.group_names.len() + hit.group_index,
		)
		.labeled(
			"Category",
			crate::plot::spec::SemanticType::Nominal,
			crate::plot::common::HitValue::nominal(category),
		)
		.labeled(
			"Group",
			crate::plot::spec::SemanticType::Nominal,
			crate::plot::common::HitValue::nominal(group),
		)
		.labeled(
			"Value",
			crate::plot::spec::SemanticType::Quantitative,
			crate::plot::common::HitValue::quantitative(
				approximate_value,
				format!("~{approximate_value:.2}"),
			),
		);
		let candidate = self
			.prepared_data
			.selection_candidates
			.get(hit.category_index)
			.and_then(|groups| groups.get(hit.group_index))
			.and_then(Clone::clone);
		Some(match candidate {
			Some(candidate) if self.prepared_data.click_selectable => {
				plot_hit.with_candidate(candidate)
			}
			_ => plot_hit,
		})
	}
}

impl PlotKernel for BarPlotKernel {
	fn project_predicate(
		&self,
		predicate: &crate::selection::Predicate,
	) -> Option<Vec<Box<dyn crate::plot::common::EmphasisAnchor>>> {
		if !self.prepared_data.click_selectable {
			return None;
		}
		let group_count = self.prepared_data.group_names.len();
		let mut anchors: Vec<Box<dyn crate::plot::common::EmphasisAnchor>> = Vec::new();
		for (category_index, groups) in self.prepared_data.selection_candidates.iter().enumerate() {
			for (group_index, candidate) in groups.iter().enumerate() {
				if candidate
					.as_ref()
					.is_some_and(|candidate| predicate.matches_candidate(candidate))
				{
					anchors.push(Box::new(BarEmphasisAnchor {
						category_index,
						group_index,
						group_count,
						orientation: self.orientation,
						value: self.prepared_data.values[category_index][group_index],
					}));
				}
			}
		}
		Some(anchors)
	}

	fn layout(&self, settings: ResolvedPlotSettings) -> PlotLayout {
		match self.orientation {
			Orientation::Vertical => PlotLayout::CategoricalX {
				categories: self.prepared_data.categories.clone(),
				y_axis_type: crate::plot::common::AxisType::Linear,
				y_range: (
					settings.y_min.unwrap_or(self.prepared_data.y_range.0),
					settings.y_max.unwrap_or(self.prepared_data.y_range.1),
				),
			},
			Orientation::Horizontal => PlotLayout::CategoricalY {
				categories: self.prepared_data.categories.clone(),
				x_axis_type: crate::plot::common::AxisType::Linear,
				x_range: (
					settings.x_min.unwrap_or(self.prepared_data.y_range.0),
					settings.x_max.unwrap_or(self.prepared_data.y_range.1),
				),
			},
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
		let num_groups = self.prepared_data.group_names.len();
		for bar in self.bar_rectangles(
			transform,
			settings.bar_width.fraction(),
			settings.category_gap.fraction(),
		) {
			let category = &self.prepared_data.categories[bar.category_index];
			let color = if let Some(assignment) = self
				.band_fills
				.iter()
				.find(|assignment| assignment.category == *category)
			{
				Color::from_rgba8(
					assignment.color.red,
					assignment.color.green,
					assignment.color.blue,
					assignment.color.alpha as f32 / u8::MAX as f32,
				)
			} else if let Some(resolved) = self.group_color(bar.group_index) {
				resolved
			} else {
				let t = if num_groups > 1 {
					bar.group_index as f32 / (num_groups - 1) as f32
				} else {
					0.5
				};
				settings.color_theme.get_color(t)
			};
			backend.fill_rectangle(bar.rect.position(), bar.rect.size(), color);
		}
	}

	fn plot_with_settings(
		&self,
		backend: &mut dyn PlotBackend,
		bounds: Rectangle,
		transform: &CoordinateTransformer,
		cursor: Cursor,
		settings: &PlotSettings,
	) {
		self.plot(
			backend,
			bounds,
			transform,
			cursor,
			settings.resolve_for_layer(&self.layer_settings),
		);
	}

	fn hit(
		&self,
		transform: &CoordinateTransformer,
		cursor: Cursor,
	) -> Option<crate::plot::common::PlotHit> {
		self.hit_resolved(transform, cursor, &ResolvedPlotSettings::default())
	}

	fn hit_with_settings(
		&self,
		transform: &CoordinateTransformer,
		cursor: Cursor,
		settings: &PlotSettings,
	) -> Option<crate::plot::common::PlotHit> {
		self.hit_resolved(
			transform,
			cursor,
			&settings.resolve_for_layer(&self.layer_settings),
		)
	}

	fn legend_content(
		&self,
		settings: &ResolvedPlotSettings,
	) -> Option<crate::plot::legend::LegendContent> {
		if !self.band_fills.is_empty() {
			return None;
		}
		let total = self.prepared_data.group_names.len();
		let pairs = self
			.prepared_data
			.group_names
			.iter()
			.enumerate()
			.map(|(i, name)| {
				let color = self.group_color(i).unwrap_or_else(|| {
					settings.color_theme.get_color(if total > 1 {
						i as f32 / (total - 1) as f32
					} else {
						0.5
					})
				});
				(name.clone(), color)
			});
		crate::plot::legend::entries_from_pairs_with_sample_and_provenance(
			pairs,
			crate::plot::legend::LegendSample::Swatch,
			self.prepared_data.group_provenance,
		)
	}

	fn x_label(&self) -> String {
		match self.orientation {
			Orientation::Vertical => self.prepared_data.x_label.clone(),
			Orientation::Horizontal => self.prepared_data.y_label.clone(),
		}
	}

	fn y_label(&self) -> String {
		match self.orientation {
			Orientation::Vertical => self.prepared_data.y_label.clone(),
			Orientation::Horizontal => self.prepared_data.x_label.clone(),
		}
	}
}

struct BarEmphasisAnchor {
	category_index: usize,
	group_index: usize,
	group_count: usize,
	orientation: Orientation,
	value: f64,
}

impl crate::plot::common::EmphasisAnchor for BarEmphasisAnchor {
	fn reproject(
		&self,
		transform: &CoordinateTransformer,
	) -> Option<crate::plot::common::EmphasisShape> {
		let groups = self.group_count.max(1) as f32;
		match (self.orientation, transform.layout) {
			(Orientation::Vertical, PlotLayout::CategoricalX { y_range, .. }) => {
				let (base, band) = transform.categorical(self.category_index, y_range.0);
				let (tip, _) = transform.categorical(self.category_index, self.value);
				let slot = band / groups;
				Some(crate::plot::common::EmphasisShape::Rectangle(Rectangle {
					x: base.x - band / 2.0 + self.group_index as f32 * slot,
					y: tip.y.min(base.y),
					width: slot,
					height: (base.y - tip.y).abs().max(1.0),
				}))
			}
			(Orientation::Horizontal, PlotLayout::CategoricalY { x_range, .. }) => {
				let (base, band) = transform.categorical(self.category_index, x_range.0);
				let (tip, _) = transform.categorical(self.category_index, self.value);
				let slot = band / groups;
				Some(crate::plot::common::EmphasisShape::Rectangle(Rectangle {
					x: tip.x.min(base.x),
					y: base.y - band / 2.0 + self.group_index as f32 * slot,
					width: (tip.x - base.x).abs().max(1.0),
					height: slot,
				}))
			}
			_ => None,
		}
	}
}

pub struct BarPreparedData {
	pub categories: Vec<String>,
	pub group_names: Vec<String>,
	pub values: Vec<Vec<f64>>,
	pub y_range: (f64, f64),
	pub x_label: String,
	pub y_label: String,
	pub group_provenance: LegendProvenance,
	pub click_selectable: bool,
	pub selection_candidates: Vec<Vec<Option<crate::selection::SelectionCandidate>>>,
}

/// The distinct labels of `col` in sorted order, or `fallback` alone when the
/// column is unnamed or absent. Strings label themselves; every other value
/// labels as its unquoted display form.
fn sorted_unique_labels(df: &DataFrame, col: &str, fallback: &str) -> Vec<String> {
	if col.is_empty() {
		return vec![fallback.to_string()];
	}
	match df.column(col) {
		Ok(c) => {
			let series = c
				.unique()
				.unwrap_or_else(|_| c.clone())
				.sort(Default::default())
				.unwrap_or_else(|_| c.clone());
			series
				.as_materialized_series()
				.iter()
				.map(|v| {
					if let AnyValue::String(s) = v {
						s.to_string()
					} else {
						v.to_string().replace("\"", "")
					}
				})
				.collect()
		}
		Err(_) => vec![fallback.to_string()],
	}
}

fn bar_value_column(df: &DataFrame, val_col: &str) -> Column {
	match df.column(val_col) {
		Ok(c) => c.cast(&DataType::Float64).unwrap_or_else(|_| {
			Column::from(Series::new_empty(val_col.into(), &DataType::Float64))
		}),
		Err(_) => Column::from(Series::new_empty(val_col.into(), &DataType::Float64)),
	}
}

/// The value axis a bar mark resolves: zero up to the largest value with ten
/// percent headroom, the unit range when no value is present.
fn bar_value_range(values: &Float64Chunked) -> (f64, f64) {
	(0.0, values.max().unwrap_or(1.0) * 1.1)
}

/// The layout a bar mark resolves over `cat_col` and `val_col` — its band
/// categories and value range — without preparing a single bar. Preparation
/// and authoring-time layout resolution share this one derivation.
pub fn bar_column_extent(
	df: &DataFrame,
	cat_col: &str,
	val_col: &str,
) -> (Vec<String>, (f64, f64)) {
	if df.height() == 0 || val_col.is_empty() {
		return (vec!["No Data".to_string()], (0.0, 1.0));
	}
	let categories = sorted_unique_labels(df, cat_col, "All Data");
	let values = bar_value_column(df, val_col);
	(categories, bar_value_range(values.f64().unwrap()))
}

pub fn prepare_bar_data(
	df: &DataFrame,
	cat_col: &str,
	group_col: &str,
	val_col: &str,
) -> BarPreparedData {
	if df.height() == 0 || val_col.is_empty() {
		return BarPreparedData {
			categories: vec!["No Data".to_string()],
			group_names: vec!["Value".to_string()],
			values: vec![vec![0.0]],
			y_range: (0.0, 1.0),
			x_label: cat_col.to_string(),
			y_label: val_col.to_string(),
			group_provenance: LegendProvenance::Synthetic,
			click_selectable: false,
			selection_candidates: vec![vec![None]],
		};
	}
	let (categories, y_range) = bar_column_extent(df, cat_col, val_col);
	let group_names = sorted_unique_labels(df, group_col, "Value");
	let num_cats = categories.len();
	let num_groups = group_names.len();
	let vals_col_series = bar_value_column(df, val_col);
	let vals_f64 = vals_col_series.f64().unwrap();
	let mut values = vec![vec![0.0f64; num_groups]; num_cats];
	let mut assigned = vec![vec![false; num_groups]; num_cats];
	let mut click_selectable = !cat_col.is_empty();
	let mut selection_candidates = vec![vec![None; num_groups]; num_cats];
	let category_indices: HashMap<&str, usize> = categories
		.iter()
		.enumerate()
		.map(|(index, value)| (value.as_str(), index))
		.collect();
	let group_indices: HashMap<&str, usize> = group_names
		.iter()
		.enumerate()
		.map(|(index, value)| (value.as_str(), index))
		.collect();
	let category_column = (!cat_col.is_empty())
		.then(|| df.column(cat_col).ok())
		.flatten();
	let group_column = (!group_col.is_empty())
		.then(|| df.column(group_col).ok())
		.flatten();
	let category_exact = category_column.and_then(crate::selection::exact_column_values);
	let category_type =
		category_column.and_then(|column| crate::selection::ScalarType::from_dtype(column.dtype()));
	let group_exact = group_column.and_then(crate::selection::exact_column_values);
	let group_type =
		group_column.and_then(|column| crate::selection::ScalarType::from_dtype(column.dtype()));
	for row in 0..df.height() {
		let category_idx = if cat_col.is_empty() {
			Some(0)
		} else {
			category_column
				.and_then(|column| column.get(row).ok())
				.map(category_label)
				.and_then(|value| category_indices.get(value.as_str()).copied())
		};
		let group_idx = if group_col.is_empty() {
			Some(0)
		} else {
			group_column
				.and_then(|column| column.get(row).ok())
				.map(category_label)
				.and_then(|value| group_indices.get(value.as_str()).copied())
		};
		let (Some(category_idx), Some(group_idx)) = (category_idx, group_idx) else {
			continue;
		};
		if !assigned[category_idx][group_idx] {
			values[category_idx][group_idx] = vals_f64.get(row).unwrap_or(0.0);
			assigned[category_idx][group_idx] = true;
			let category = category_type.as_ref().and_then(|scalar_type| {
				let value = category_exact.as_ref()?.get(row)?.clone()?;
				Some((
					crate::selection::FieldRef::new(
						cat_col,
						crate::plot::spec::SemanticType::Nominal,
						scalar_type.clone(),
					),
					value,
				))
			});
			if let Some(category) = category {
				let mut constraints = vec![category];
				if group_column.is_some() {
					let group = group_type.as_ref().zip(group_exact.as_ref()).and_then(
						|(scalar_type, values)| {
							Some((
								crate::selection::FieldRef::new(
									group_col,
									crate::plot::spec::SemanticType::Nominal,
									scalar_type.clone(),
								),
								values.get(row)?.clone()?,
							))
						},
					);
					if let Some(group) = group {
						constraints.push(group);
					} else {
						click_selectable = false;
					}
				}
				selection_candidates[category_idx][group_idx] =
					Some(crate::selection::SelectionCandidate { constraints });
			} else {
				click_selectable = false;
			}
		} else {
			// The renderer keeps only the first row for this pair. A predicate on
			// the pair would select data the bar never depicted.
			click_selectable = false;
		}
	}
	BarPreparedData {
		categories,
		group_names,
		values,
		y_range,
		x_label: cat_col.to_string(),
		y_label: val_col.to_string(),
		group_provenance: if group_col.is_empty() {
			LegendProvenance::Synthetic
		} else {
			LegendProvenance::Named
		},
		click_selectable,
		selection_candidates,
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::plot::legend::LegendSection;
	use crate::plot::spec::{
		BarWidth, CategoryColorAssignment, CategoryGap, PalettePosition, ResolvedColor,
	};
	use iced::{Point, Size};

	fn grouped_kernel(orientation: Orientation) -> BarPlotKernel {
		let frame = polars::df!(
			"cat" => ["A", "A", "B", "B"],
			"group" => ["left", "right", "left", "right"],
			"val" => [10_i64, 11, 20, 21],
		)
		.expect("grouped bar fixture");
		BarPlotKernel {
			prepared_data: Arc::new(prepare_bar_data(&frame, "cat", "group", "val")),
			orientation,
			layer_settings: LayerSettings::default(),
			band_fills: Vec::new(),
			group_colors: Vec::new(),
		}
	}

	#[test]
	fn product_0151_duplicate_bar_pairs_keep_painting_first_but_refuse_click_selection() {
		let frame = polars::df!(
			"cat" => ["A", "A"],
			"group" => ["left", "left"],
			"val" => [10_i64, 99],
		)
		.expect("duplicate bar fixture");
		let prepared = prepare_bar_data(&frame, "cat", "group", "val");
		assert_eq!(
			prepared.values,
			vec![vec![10.0]],
			"paint keeps the first row"
		);
		assert!(!prepared.click_selectable);
		let kernel = BarPlotKernel {
			prepared_data: Arc::new(prepared),
			orientation: Orientation::Vertical,
			layer_settings: LayerSettings::default(),
			band_fills: Vec::new(),
			group_colors: Vec::new(),
		};
		let candidate = kernel.prepared_data.selection_candidates[0][0]
			.as_ref()
			.expect("exact candidate retained for the painted row");
		let predicate = crate::selection::Predicate::from_candidate(candidate).expect("predicate");
		assert!(kernel.project_predicate(&predicate).is_none());
	}

	#[test]
	fn product_0151_bar_candidate_names_category_and_bound_group() {
		let kernel = grouped_kernel(Orientation::Vertical);
		assert!(kernel.prepared_data.click_selectable);
		let candidate = kernel.prepared_data.selection_candidates[0][0]
			.as_ref()
			.expect("unique grouped bar is selectable");
		assert_eq!(
			candidate
				.constraints
				.iter()
				.map(|(field, _)| field.name.as_str())
				.collect::<Vec<_>>(),
			["cat", "group"]
		);
		let predicate = crate::selection::Predicate::from_candidate(candidate).expect("predicate");
		assert_eq!(
			kernel
				.project_predicate(&predicate)
				.expect("bar admits selection")
				.len(),
			1
		);
	}

	fn single_series_kernel(orientation: Orientation) -> BarPlotKernel {
		let frame = polars::df!(
			"cat" => ["A", "B"],
			"val" => [10_i64, 20],
		)
		.expect("single-series bar fixture");
		BarPlotKernel {
			prepared_data: Arc::new(prepare_bar_data(&frame, "cat", "", "val")),
			orientation,
			layer_settings: LayerSettings::default(),
			band_fills: Vec::new(),
			group_colors: Vec::new(),
		}
	}

	fn categorical_interval(rect: Rectangle, orientation: Orientation) -> (f32, f32) {
		match orientation {
			Orientation::Vertical => (rect.x, rect.x + rect.width),
			Orientation::Horizontal => (rect.y, rect.y + rect.height),
		}
	}

	fn assert_close(actual: f32, expected: f32) {
		assert!(
			(actual - expected).abs() < 0.0001,
			"expected {expected}, got {actual}"
		);
	}

	#[test]
	fn values_follow_category_identity_after_labels_are_sorted() {
		let frame = polars::df!(
			"cat" => ["Cat1", "Cat2", "Cat3", "Cat4", "Cat5", "Cat6"],
			"val" => [1.1_f64, 2.2, 3.3, 4.4, 5.5, 6.6],
		)
		.expect("bar fixture");

		let prepared = prepare_bar_data(&frame, "cat", "", "val");

		assert_eq!(
			prepared.categories,
			["Cat1", "Cat2", "Cat3", "Cat4", "Cat5", "Cat6"]
		);
		assert_eq!(
			prepared.values,
			vec![
				vec![1.1],
				vec![2.2],
				vec![3.3],
				vec![4.4],
				vec![5.5],
				vec![6.6]
			]
		);
	}

	#[test]
	fn values_follow_both_category_and_group_identity() {
		let frame = polars::df!(
			"cat" => ["B", "A", "B", "A", "A"],
			"group" => ["right", "left", "left", "right", "left"],
			"val" => [21_i64, 10, 20, 11, 999],
		)
		.expect("grouped bar fixture");

		let prepared = prepare_bar_data(&frame, "cat", "group", "val");

		assert_eq!(prepared.categories, ["A", "B"]);
		assert_eq!(prepared.group_names, ["left", "right"]);
		assert_eq!(prepared.values, vec![vec![10.0, 11.0], vec![20.0, 21.0]]);
	}

	#[test]
	fn product_0053_clusters_follow_transform_centers_and_gap_equations_when_resized() {
		for orientation in [Orientation::Vertical, Orientation::Horizontal] {
			for axis_extent in [100.0, 500.0] {
				let kernel = grouped_kernel(orientation);
				let bounds = match orientation {
					Orientation::Vertical => {
						Rectangle::new(Point::ORIGIN, Size::new(axis_extent, 100.0))
					}
					Orientation::Horizontal => {
						Rectangle::new(Point::ORIGIN, Size::new(100.0, axis_extent))
					}
				};
				let layout = kernel.layout(ResolvedPlotSettings::default());
				let transform = CoordinateTransformer::new(&layout, bounds);
				let rectangles = kernel.bar_rectangles(&transform, 0.5, 0.2);
				let band = axis_extent / 2.0;
				let slot = band * 0.8 / 2.0;
				let within = slot * 0.5;

				for category_index in 0..2 {
					let mut intervals = rectangles
						.iter()
						.filter(|bar| bar.category_index == category_index)
						.map(|bar| categorical_interval(bar.rect, orientation))
						.collect::<Vec<_>>();
					intervals.sort_by(|left, right| left.0.total_cmp(&right.0));
					let cluster_center = (intervals[0].0 + intervals[1].1) / 2.0;
					let transformed = transform.categorical(category_index, 0.0).0;
					let transformed_center = match orientation {
						Orientation::Vertical => transformed.x,
						Orientation::Horizontal => transformed.y,
					};
					assert_close(cluster_center, transformed_center);
					assert_close(intervals[1].0 - intervals[0].1, within);
				}

				let mut clusters = (0..2)
					.map(|category_index| {
						let intervals = rectangles
							.iter()
							.filter(|bar| bar.category_index == category_index)
							.map(|bar| categorical_interval(bar.rect, orientation))
							.collect::<Vec<_>>();
						(
							intervals
								.iter()
								.map(|interval| interval.0)
								.fold(f32::INFINITY, f32::min),
							intervals
								.iter()
								.map(|interval| interval.1)
								.fold(f32::NEG_INFINITY, f32::max),
						)
					})
					.collect::<Vec<_>>();
				clusters.sort_by(|left, right| left.0.total_cmp(&right.0));
				assert_close(clusters[1].0 - clusters[0].1, band * 0.2 + within);
			}
		}
	}

	#[test]
	fn product_0053_full_width_isolates_the_centered_category_reserve() {
		for orientation in [Orientation::Vertical, Orientation::Horizontal] {
			let kernel = grouped_kernel(orientation);
			let layout = kernel.layout(ResolvedPlotSettings::default());
			let transform = CoordinateTransformer::new(
				&layout,
				Rectangle::new(Point::ORIGIN, Size::new(100.0, 100.0)),
			);
			let rectangles = kernel.bar_rectangles(&transform, 1.0, 0.2);
			let mut intervals = rectangles
				.iter()
				.map(|bar| categorical_interval(bar.rect, orientation))
				.collect::<Vec<_>>();
			intervals.sort_by(|left, right| left.0.total_cmp(&right.0));
			assert_close(intervals[0].0, 5.0);
			assert_close(intervals[1].0 - intervals[0].1, 0.0);
			assert_close(intervals[2].0 - intervals[1].1, 10.0);
			assert_close(intervals[3].0 - intervals[2].1, 0.0);
			assert_close(100.0 - intervals[3].1, 5.0);
		}
	}

	#[test]
	fn product_0053_single_series_composition_is_exactly_redundant() {
		for orientation in [Orientation::Vertical, Orientation::Horizontal] {
			let kernel = single_series_kernel(orientation);
			let layout = kernel.layout(ResolvedPlotSettings::default());
			let transform = CoordinateTransformer::new(
				&layout,
				Rectangle::new(Point::ORIGIN, Size::new(128.0, 128.0)),
			);
			assert_eq!(
				kernel.bar_rectangles(&transform, 0.5, 0.5),
				kernel.bar_rectangles(&transform, 0.25, 0.0)
			);
		}
	}

	#[test]
	fn product_0053_paint_and_hit_share_visible_fill_and_category_reserve() {
		for orientation in [Orientation::Vertical, Orientation::Horizontal] {
			let kernel = grouped_kernel(orientation);
			let layout = kernel.layout(ResolvedPlotSettings::default());
			let transform = CoordinateTransformer::new(
				&layout,
				Rectangle::new(Point::ORIGIN, Size::new(100.0, 100.0)),
			);
			let settings = ResolvedPlotSettings {
				bar_width: BarWidth::new(1.0).unwrap(),
				category_gap: CategoryGap::new(0.2).unwrap(),
				..ResolvedPlotSettings::default()
			};
			let rectangles = kernel.bar_rectangles(&transform, 1.0, 0.2);
			let visible = rectangles[0].rect.center();
			assert!(
				kernel
					.hit_resolved(&transform, Cursor::Available(visible), &settings)
					.is_some()
			);
			let reserve = match orientation {
				Orientation::Vertical => Point::new(1.0, visible.y),
				Orientation::Horizontal => Point::new(visible.x, 51.0),
			};
			assert_eq!(
				kernel.hit_resolved(&transform, Cursor::Available(reserve), &settings),
				None
			);
		}
	}

	#[test]
	fn product_0056_horizontal_labels_payloads_and_legend_share_top_down_order() {
		let mut kernel = grouped_kernel(Orientation::Horizontal);
		kernel.group_colors = vec![
			CategoryColorAssignment {
				category: "left".to_owned(),
				position: PalettePosition::new(0, 2).unwrap(),
				color: ResolvedColor {
					red: 11,
					green: 22,
					blue: 33,
					alpha: 255,
				},
			},
			CategoryColorAssignment {
				category: "right".to_owned(),
				position: PalettePosition::new(1, 2).unwrap(),
				color: ResolvedColor {
					red: 44,
					green: 55,
					blue: 66,
					alpha: 255,
				},
			},
		];
		let settings = ResolvedPlotSettings {
			bar_width: BarWidth::new(0.8).unwrap(),
			category_gap: CategoryGap::new(0.2).unwrap(),
			..ResolvedPlotSettings::default()
		};
		let layout = kernel.layout(settings.clone());
		let bounds = Rectangle::new(Point::ORIGIN, Size::new(231.0, 200.0));
		let transform = CoordinateTransformer::new(&layout, bounds);
		let mut rectangles = kernel.bar_rectangles(&transform, 0.8, 0.2);
		rectangles.sort_by(|left, right| left.rect.y.total_cmp(&right.rect.y));

		let observed = rectangles
			.iter()
			.map(|bar| {
				let category = &kernel.prepared_data.categories[bar.category_index];
				let group = &kernel.prepared_data.group_names[bar.group_index];
				let value = kernel.prepared_data.values[bar.category_index][bar.group_index];
				let color = kernel.group_color(bar.group_index).unwrap();
				assert_close(bar.rect.width, value as f32 * 10.0);
				assert!(
					kernel
						.hit_resolved(&transform, Cursor::Available(bar.rect.center()), &settings)
						.unwrap()
						.legacy_presentation()
						.starts_with(&format!("{category}: {group}"))
				);
				(category.as_str(), group.as_str(), value, color)
			})
			.collect::<Vec<_>>();
		assert_eq!(
			observed,
			vec![
				("A", "left", 10.0, Color::from_rgb8(11, 22, 33)),
				("A", "right", 11.0, Color::from_rgb8(44, 55, 66)),
				("B", "left", 20.0, Color::from_rgb8(11, 22, 33)),
				("B", "right", 21.0, Color::from_rgb8(44, 55, 66)),
			]
		);

		let legend = kernel.legend_content(&settings).unwrap();
		let LegendSection::Entries(entries) = legend.0.first() else {
			panic!("grouped bars produce named entries");
		};
		assert_eq!(
			entries
				.iter()
				.map(|entry| (entry.label.as_str(), entry.swatch))
				.collect::<Vec<_>>(),
			[
				("left", Color::from_rgb8(11, 22, 33)),
				("right", Color::from_rgb8(44, 55, 66)),
			]
		);
	}
}
