use crate::plot::common::{
	AxisTitleTransform, CoordinateTransformer, Orientation, PlotBackend, PlotKernel, PlotLayout,
	ResolvedPlotSettings, derive_axis_title,
};
use crate::plot::legend::LegendProvenance;
use crate::plot::spec::{
	BandFillAssignment, CategoryColorAssignment, LayerSettings, StackNormalization,
};
use crate::plot::theme::PlotSettings;
use iced::Color;
use iced::Rectangle;
use iced::advanced::mouse::Cursor;
use polars::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;

pub struct StackedBarPlotKernel {
	pub prepared_data: Arc<StackedBarPreparedData>,
	pub orientation: Orientation,
	pub layer_settings: LayerSettings,
	/// PRODUCT-0064 §4: two-field bars retain their resolved category fills
	/// when grouped geometry becomes stacked geometry.
	pub band_fills: Vec<BandFillAssignment>,
	/// PRODUCT-0064 §4: authored stacked bars consume the resolved categorical
	/// assignments. Automatic dashboard plots carry an empty vector and retain
	/// the legacy ramp fallback.
	pub group_colors: Vec<CategoryColorAssignment>,
	pub normalization: StackNormalization,
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct StackedBarRectangle {
	rect: Rectangle,
	category_index: usize,
	group_index: usize,
	value: f64,
	running_total: f64,
	total: f64,
}

fn category_label(value: AnyValue<'_>) -> String {
	if let AnyValue::String(value) = value {
		value.to_string()
	} else {
		value.to_string().replace('"', "")
	}
}

impl StackedBarPlotKernel {
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

	/// PRODUCT-0064 §§5-8: one positive-segment sequence owns paint and hit
	/// geometry. Every stack starts at data zero; authored bounds clip it but
	/// never rebase its values.
	fn stacked_rectangles(
		&self,
		transform: &CoordinateTransformer,
		width_fraction: f32,
		category_gap_fraction: f32,
	) -> Vec<StackedBarRectangle> {
		let num_cats = self.prepared_data.categories.len();
		let num_groups = self.prepared_data.group_names.len();
		let mut bars = Vec::with_capacity(num_cats.saturating_mul(num_groups));
		for category_index in 0..num_cats {
			let total = self.prepared_data.totals[category_index];
			let Some(shared_index) =
				transform.category_index(&self.prepared_data.categories[category_index])
			else {
				continue;
			};
			let (category_center, band) = transform.categorical(shared_index, 0.0);
			let available = band * (1.0 - category_gap_fraction);
			let bar_extent = available * width_fraction;
			let mut running_total = 0.0;
			for group_index in 0..num_groups {
				let value = self.prepared_data.category_values[category_index][group_index];
				debug_assert!(value >= 0.0, "prepared stack values are canonicalized");
				if value == 0.0 {
					continue;
				}
				let next_total = running_total + value;
				let rendered_start = self.normalization.project(running_total, total);
				let rendered_end = self.normalization.project(next_total, total);
				let (start, _) = transform.categorical(shared_index, rendered_start);
				let (end, _) = transform.categorical(shared_index, rendered_end);
				let rect = match self.orientation {
					Orientation::Vertical => Rectangle {
						x: category_center.x - bar_extent / 2.0,
						y: end.y,
						width: bar_extent,
						height: match self.normalization {
							StackNormalization::Absolute => (start.y - end.y).max(1.0),
							StackNormalization::Percent => start.y - end.y,
						},
					},
					Orientation::Horizontal => Rectangle {
						x: start.x,
						y: category_center.y - bar_extent / 2.0,
						width: match self.normalization {
							StackNormalization::Absolute => (end.x - start.x).max(1.0),
							StackNormalization::Percent => end.x - start.x,
						},
						height: bar_extent,
					},
				};
				bars.push(StackedBarRectangle {
					rect,
					category_index,
					group_index,
					value,
					running_total: next_total,
					total,
				});
				running_total = next_total;
			}
		}
		bars
	}

	fn hover_resolved(
		&self,
		transform: &CoordinateTransformer,
		cursor: Cursor,
		settings: &ResolvedPlotSettings,
	) -> Option<String> {
		let cursor = cursor.position()?;
		let hit = self
			.stacked_rectangles(
				transform,
				settings.bar_width.fraction(),
				settings.category_gap.fraction(),
			)
			.into_iter()
			.find(|bar| bar.rect.contains(cursor))?;
		match self.normalization {
			StackNormalization::Absolute => Some(format!(
				"{}: {} (Value: {:.2}, Total: {:.2})",
				self.prepared_data.categories[hit.category_index],
				self.prepared_data.group_names[hit.group_index],
				hit.value,
				hit.running_total,
			)),
			StackNormalization::Percent => Some(format!(
				"{}: {} (Value: {:.2}, Share: {:.1}%, Cumulative: {:.1}%, Total: {:.2})",
				self.prepared_data.categories[hit.category_index],
				self.prepared_data.group_names[hit.group_index],
				hit.value,
				self.normalization.project(hit.value, hit.total),
				self.normalization.project(hit.running_total, hit.total),
				hit.total,
			)),
		}
	}
}

impl PlotKernel for StackedBarPlotKernel {
	fn layout(&self, settings: ResolvedPlotSettings) -> PlotLayout {
		let (range, axis_type) = match self.normalization {
			StackNormalization::Absolute => (
				self.prepared_data.y_range,
				crate::plot::common::AxisType::Linear,
			),
			StackNormalization::Percent => ((0.0, 100.0), crate::plot::common::AxisType::Percent),
		};
		match self.orientation {
			Orientation::Vertical => PlotLayout::CategoricalX {
				categories: self.prepared_data.categories.clone(),
				y_axis_type: axis_type,
				y_range: if matches!(self.normalization, StackNormalization::Percent) {
					range
				} else {
					(
						settings.y_min.unwrap_or(range.0),
						settings.y_max.unwrap_or(range.1),
					)
				},
			},
			Orientation::Horizontal => PlotLayout::CategoricalY {
				categories: self.prepared_data.categories.clone(),
				x_axis_type: axis_type,
				x_range: if matches!(self.normalization, StackNormalization::Percent) {
					range
				} else {
					(
						settings.x_min.unwrap_or(range.0),
						settings.x_max.unwrap_or(range.1),
					)
				},
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
		for bar in self.stacked_rectangles(
			transform,
			settings.bar_width.fraction(),
			settings.category_gap.fraction(),
		) {
			let category = &self.prepared_data.categories[bar.category_index];
			let color = self
				.band_fills
				.iter()
				.find(|assignment| assignment.category == *category)
				.map(|assignment| {
					Color::from_rgba8(
						assignment.color.red,
						assignment.color.green,
						assignment.color.blue,
						assignment.color.alpha as f32 / u8::MAX as f32,
					)
				})
				.or_else(|| self.group_color(bar.group_index))
				.unwrap_or_else(|| {
					settings.color_theme.get_color(if num_groups > 1 {
						bar.group_index as f32 / (num_groups - 1) as f32
					} else {
						0.5
					})
				});
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

	fn hover(&self, transform: &CoordinateTransformer, cursor: Cursor) -> Option<String> {
		self.hover_resolved(transform, cursor, &ResolvedPlotSettings::default())
	}

	fn hover_with_settings(
		&self,
		transform: &CoordinateTransformer,
		cursor: Cursor,
		settings: &PlotSettings,
	) -> Option<String> {
		self.hover_resolved(
			transform,
			cursor,
			&settings.resolve_for_layer(&self.layer_settings),
		)
	}

	fn legend_content(
		&self,
		settings: &ResolvedPlotSettings,
	) -> Option<crate::plot::legend::LegendContent> {
		let total = self.prepared_data.group_names.len();
		let pairs = self
			.prepared_data
			.group_names
			.iter()
			.enumerate()
			.map(|(index, name)| {
				let color = self.group_color(index).unwrap_or_else(|| {
					settings.color_theme.get_color(if total > 1 {
						index as f32 / (total - 1) as f32
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
			Orientation::Horizontal => self.value_axis_title(),
		}
	}

	fn y_label(&self) -> String {
		match self.orientation {
			Orientation::Vertical => self.value_axis_title(),
			Orientation::Horizontal => self.prepared_data.x_label.clone(),
		}
	}
}

impl StackedBarPlotKernel {
	fn value_axis_title(&self) -> String {
		match self.normalization {
			StackNormalization::Absolute => self.prepared_data.y_label.clone(),
			StackNormalization::Percent => derive_axis_title(
				&self.prepared_data.y_label,
				AxisTitleTransform::PercentOfTotal,
			),
		}
	}
}

pub struct StackedBarPreparedData {
	pub categories: Vec<String>,
	pub group_names: Vec<String>,
	pub category_values: Vec<Vec<f64>>,
	pub totals: Vec<f64>,
	pub y_range: (f64, f64),
	pub x_label: String,
	pub y_label: String,
	pub group_provenance: LegendProvenance,
}

pub fn prepare_stacked_bar_data(
	df: &DataFrame,
	cat_col: &str,
	group_col: &str,
	val_col: &str,
) -> StackedBarPreparedData {
	if df.height() == 0 || val_col.is_empty() {
		return StackedBarPreparedData {
			categories: vec!["No Data".to_string()],
			group_names: vec!["Value".to_string()],
			category_values: vec![vec![0.0]],
			totals: vec![0.0],
			y_range: (0.0, 1.0),
			x_label: cat_col.to_string(),
			y_label: val_col.to_string(),
			group_provenance: LegendProvenance::Synthetic,
		};
	}
	let (categories, _categories_series) = if cat_col.is_empty() {
		(
			vec!["All Data".to_string()],
			Series::new("dummy_cat".into(), &["All Data"]),
		)
	} else {
		match df.column(cat_col) {
			Ok(c) => {
				let series = c
					.unique()
					.unwrap_or_else(|_| c.clone())
					.sort(Default::default())
					.unwrap_or_else(|_| c.clone());
				let cats = series
					.as_materialized_series()
					.iter()
					.map(|v| {
						if let AnyValue::String(s) = v {
							s.to_string()
						} else {
							v.to_string().replace("\"", "")
						}
					})
					.collect();
				(cats, series.as_materialized_series().clone())
			}
			Err(_) => (
				vec!["All Data".to_string()],
				Series::new("dummy_cat".into(), &["All Data"]),
			),
		}
	};
	let (group_names, _groups_series) = if group_col.is_empty() {
		(
			vec!["Value".to_string()],
			Series::new("dummy_group".into(), &["Value"]),
		)
	} else {
		match df.column(group_col) {
			Ok(c) => {
				let series = c
					.unique()
					.unwrap_or_else(|_| c.clone())
					.sort(Default::default())
					.unwrap_or_else(|_| c.clone());
				let groups = series
					.as_materialized_series()
					.iter()
					.map(|v| {
						if let AnyValue::String(s) = v {
							s.to_string()
						} else {
							v.to_string().replace("\"", "")
						}
					})
					.collect();
				(groups, series.as_materialized_series().clone())
			}
			Err(_) => (
				vec!["Value".to_string()],
				Series::new("dummy_group".into(), &["Value"]),
			),
		}
	};
	let category_idx_map: HashMap<&str, usize> = categories
		.iter()
		.enumerate()
		.map(|(index, value)| (value.as_str(), index))
		.collect();
	let group_idx_map: HashMap<&str, usize> = group_names
		.iter()
		.enumerate()
		.map(|(index, value)| (value.as_str(), index))
		.collect();
	let num_cats = categories.len();
	let num_groups = group_names.len();
	let mut category_values = vec![vec![0.0f64; num_groups]; num_cats];
	let mut assigned = vec![vec![false; num_groups]; num_cats];
	let vals_col_series = match df.column(val_col) {
		Ok(column) => column.cast(&DataType::Float64).unwrap_or_else(|_| {
			Column::from(Series::new_empty(val_col.into(), &DataType::Float64))
		}),
		Err(_) => Column::from(Series::new_empty(val_col.into(), &DataType::Float64)),
	};
	let vals_f64 = vals_col_series.f64().expect("value column was cast to f64");
	let category_column = (!cat_col.is_empty())
		.then(|| df.column(cat_col).ok())
		.flatten();
	let group_column = (!group_col.is_empty())
		.then(|| df.column(group_col).ok())
		.flatten();
	for row in 0..df.height() {
		let category_idx = if cat_col.is_empty() {
			Some(0)
		} else {
			category_column
				.and_then(|column| column.get(row).ok())
				.map(category_label)
				.and_then(|value| category_idx_map.get(value.as_str()).copied())
		};
		let group_idx = if group_col.is_empty() {
			Some(0)
		} else {
			group_column
				.and_then(|column| column.get(row).ok())
				.map(category_label)
				.and_then(|value| group_idx_map.get(value.as_str()).copied())
		};
		let (Some(category_idx), Some(group_idx)) = (category_idx, group_idx) else {
			continue;
		};
		if !assigned[category_idx][group_idx] {
			let value = vals_f64.get(row).unwrap_or(0.0);
			// PRODUCT-0064 §8: the canonical sequence contains positive finite
			// segments only. Domain, paint, hit, and hover all consume this value.
			category_values[category_idx][group_idx] = if value.is_finite() && value > 0.0 {
				value
			} else {
				0.0
			};
			assigned[category_idx][group_idx] = true;
		}
	}
	let totals = category_values
		.iter()
		.map(|values| values.iter().sum::<f64>())
		.collect::<Vec<_>>();
	let max_sum = totals.iter().copied().fold(0.0, f64::max);
	let y_min = 0.0f64;
	let y_range = (y_min, max_sum * 1.1);
	StackedBarPreparedData {
		categories,
		group_names,
		category_values,
		totals,
		y_range,
		x_label: cat_col.to_string(),
		y_label: val_col.to_string(),
		group_provenance: if group_col.is_empty() {
			LegendProvenance::Synthetic
		} else {
			LegendProvenance::Named
		},
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::plot::common::PathBuilder;
	use crate::plot::legend::{LegendSection, apply_policy};
	use crate::plot::spec::{PalettePosition, ResolvedColor};
	use iced::widget::canvas::{Stroke, Text};
	use iced::{Color, Point, Size};

	#[derive(Default)]
	/// PRODUCT-0057 named oracle
	/// `plot::kernels::stacked_bar::tests::RectangleBackend` records rectangle
	/// tuples only; its shared short name grants no other license.
	struct RectangleBackend {
		rectangles: Vec<(Point, Size, Color)>,
	}

	impl PlotBackend for RectangleBackend {
		fn stroke_path(&mut self, _f: &dyn Fn(&mut dyn PathBuilder), _stroke: Stroke) {}

		fn fill_path(&mut self, _f: &dyn Fn(&mut dyn PathBuilder), _color: Color) {}

		fn fill_rectangle(&mut self, top_left: Point, size: Size, color: Color) {
			self.rectangles.push((top_left, size, color));
		}

		fn fill_text_raw(&mut self, _text: Text) {}

		fn translate(&mut self, _translation: iced::Vector) {}

		fn rotate(&mut self, _angle: f32) {}

		fn with_save(&mut self, f: &mut dyn FnMut(&mut dyn PlotBackend)) {
			f(self);
		}

		fn with_clip(&mut self, _bounds: Rectangle, f: &mut dyn FnMut(&mut dyn PlotBackend)) {
			f(self);
		}
	}

	#[test]
	fn stacks_follow_category_and_group_identity() {
		let frame = polars::df!(
			"cat" => ["B", "A", "B", "A", "A"],
			"group" => ["right", "left", "left", "right", "left"],
			"val" => [21_i64, 10, 20, 11, 999],
		)
		.expect("stacked bar fixture");

		let prepared = prepare_stacked_bar_data(&frame, "cat", "group", "val");

		assert_eq!(prepared.categories, ["A", "B"]);
		assert_eq!(prepared.group_names, ["left", "right"]);
		assert_eq!(
			prepared.category_values,
			vec![vec![10.0, 11.0], vec![20.0, 21.0]]
		);
		assert_eq!(prepared.y_range, (0.0, 45.1));
	}

	#[test]
	fn product_0056_horizontal_stacks_join_top_down_labels_to_asymmetric_segments() {
		let frame = polars::df!(
			"cat" => ["A", "A", "B", "B"],
			"group" => ["left", "right", "left", "right"],
			"val" => [10_i64, 11, 20, 21],
		)
		.unwrap();
		let kernel = StackedBarPlotKernel {
			prepared_data: Arc::new(prepare_stacked_bar_data(&frame, "cat", "group", "val")),
			orientation: Orientation::Horizontal,
			layer_settings: LayerSettings::default(),
			band_fills: Vec::new(),
			group_colors: Vec::new(),
			normalization: StackNormalization::Absolute,
		};
		let settings = ResolvedPlotSettings::default();
		let layout = kernel.layout(settings.clone());
		let bounds = Rectangle::new(Point::ORIGIN, Size::new(451.0, 200.0));
		let transform = CoordinateTransformer::new(&layout, bounds);
		let mut backend = RectangleBackend::default();
		kernel.plot(
			&mut backend,
			bounds,
			&transform,
			Cursor::Unavailable,
			settings.clone(),
		);
		assert_eq!(backend.rectangles.len(), 4);

		for (index, (category, group, value)) in [
			("A", "left", 10.0_f32),
			("A", "right", 11.0),
			("B", "left", 20.0),
			("B", "right", 21.0),
		]
		.into_iter()
		.enumerate()
		{
			let (top_left, size, color) = backend.rectangles[index];
			let category_index = index / 2;
			let group_index = index % 2;
			let expected_center = transform.categorical(category_index, 0.0).0.y;
			assert_eq!(top_left.y + size.height / 2.0, expected_center);
			assert!((size.width - value * 10.0).abs() < 0.0001);
			assert_eq!(
				color,
				settings.color_theme.get_color(group_index as f32),
				"stack segment color keeps group identity"
			);
			let cursor = Point::new(top_left.x + size.width / 2.0, expected_center);
			assert!(
				kernel
					.hover(&transform, Cursor::Available(cursor))
					.unwrap()
					.starts_with(&format!("{category}: {group}"))
			);
		}
		assert!(backend.rectangles[0].0.y < backend.rectangles[2].0.y);
	}

	fn assignment(category: &str, index: usize, color: ResolvedColor) -> CategoryColorAssignment {
		CategoryColorAssignment {
			category: category.to_owned(),
			position: PalettePosition::new(index, 2).unwrap(),
			color,
		}
	}

	#[test]
	fn product_0064_substrate_consumes_resolved_colors_in_marks_and_legend() {
		let frame = polars::df!(
			"cat" => ["A", "A"],
			"group" => ["left", "right"],
			"val" => [10_i64, 20],
		)
		.unwrap();
		let left = ResolvedColor {
			red: 9,
			green: 19,
			blue: 29,
			alpha: 255,
		};
		let right = ResolvedColor {
			red: 199,
			green: 109,
			blue: 39,
			alpha: 255,
		};
		let kernel = StackedBarPlotKernel {
			prepared_data: Arc::new(prepare_stacked_bar_data(&frame, "cat", "group", "val")),
			orientation: Orientation::Vertical,
			layer_settings: LayerSettings::default(),
			band_fills: Vec::new(),
			group_colors: vec![assignment("left", 0, left), assignment("right", 1, right)],
			normalization: StackNormalization::Absolute,
		};
		let settings = ResolvedPlotSettings::default();
		let layout = kernel.layout(settings.clone());
		let bounds = Rectangle::new(Point::ORIGIN, Size::new(200.0, 330.0));
		let transform = CoordinateTransformer::new(&layout, bounds);
		let mut backend = RectangleBackend::default();
		kernel.plot(
			&mut backend,
			bounds,
			&transform,
			Cursor::Unavailable,
			settings.clone(),
		);
		let expected = [left, right].map(|color| {
			Color::from_rgba8(
				color.red,
				color.green,
				color.blue,
				color.alpha as f32 / u8::MAX as f32,
			)
		});
		assert_eq!(
			backend
				.rectangles
				.iter()
				.map(|(_, _, color)| *color)
				.collect::<Vec<_>>(),
			expected,
			"the dormant stacked kernel consumes synthetic resolved assignments before Rune exposes it"
		);

		let legend = kernel
			.legend_content(&settings)
			.expect("named groups earn a legend");
		let LegendSection::Entries(entries) = legend.0.first() else {
			panic!("stacked groups produce a named-entry legend")
		};
		assert_eq!(
			entries.iter().map(|entry| entry.swatch).collect::<Vec<_>>(),
			expected,
			"legend and marks consume the same resolved assignments"
		);

		let band_frame = polars::df!("cat" => ["A", "B"], "val" => [10_i64, 20]).unwrap();
		let band_kernel = StackedBarPlotKernel {
			prepared_data: Arc::new(prepare_stacked_bar_data(&band_frame, "cat", "", "val")),
			orientation: Orientation::Vertical,
			layer_settings: LayerSettings::default(),
			band_fills: vec![
				BandFillAssignment {
					category: "A".to_owned(),
					position: PalettePosition::new(0, 2).unwrap(),
					color: left,
				},
				BandFillAssignment {
					category: "B".to_owned(),
					position: PalettePosition::new(1, 2).unwrap(),
					color: right,
				},
			],
			group_colors: Vec::new(),
			normalization: StackNormalization::Absolute,
		};
		let layout = band_kernel.layout(settings.clone());
		let transform = CoordinateTransformer::new(&layout, bounds);
		let mut backend = RectangleBackend::default();
		band_kernel.plot(
			&mut backend,
			bounds,
			&transform,
			Cursor::Unavailable,
			settings,
		);
		assert_eq!(
			backend
				.rectangles
				.iter()
				.map(|(_, _, color)| *color)
				.collect::<Vec<_>>(),
			expected,
			"the degenerate stacked path consumes exact resolved category band fills"
		);
	}

	#[test]
	fn product_0064_positive_sequence_owns_domain_rectangles_hit_and_hover() {
		let frame = polars::df!(
			"cat" => ["A", "A", "A", "A", "A"],
			"group" => ["a", "b", "c", "d", "a"],
			"val" => [10.0, -5.0, 0.0, 20.0, 999.0],
		)
		.unwrap();
		let prepared = prepare_stacked_bar_data(&frame, "cat", "group", "val");
		assert_eq!(
			prepared.category_values,
			vec![vec![10.0, 0.0, 0.0, 20.0]],
			"first-row-wins is retained before the canonical positive filter"
		);
		assert_eq!(
			prepared.y_range,
			(0.0, 33.0),
			"the display domain derives from the painted total and retains 10% headroom"
		);
		let kernel = StackedBarPlotKernel {
			prepared_data: Arc::new(prepared),
			orientation: Orientation::Vertical,
			layer_settings: LayerSettings::default(),
			band_fills: Vec::new(),
			group_colors: Vec::new(),
			normalization: StackNormalization::Absolute,
		};
		let settings = ResolvedPlotSettings::default();
		let layout = kernel.layout(settings.clone());
		let bounds = Rectangle::new(Point::ORIGIN, Size::new(100.0, 330.0));
		let transform = CoordinateTransformer::new(&layout, bounds);
		let bars = kernel.stacked_rectangles(
			&transform,
			settings.bar_width.fraction(),
			settings.category_gap.fraction(),
		);
		assert_eq!(
			bars.iter()
				.map(|bar| (bar.group_index, bar.value, bar.running_total))
				.collect::<Vec<_>>(),
			vec![(0, 10.0, 10.0), (3, 20.0, 30.0)],
			"domain, rectangles, hit, and hover share one positive segment sequence"
		);
		for (bar, expected) in bars.iter().zip([
			"A: a (Value: 10.00, Total: 10.00)",
			"A: d (Value: 20.00, Total: 30.00)",
		]) {
			let cursor = Point::new(
				bar.rect.x + bar.rect.width / 2.0,
				bar.rect.y + bar.rect.height / 2.0,
			);
			assert_eq!(
				kernel
					.hover(&transform, Cursor::Available(cursor))
					.as_deref(),
				Some(expected),
				"hover reports the exact segment whose shared rectangle was hit"
			);
		}
	}

	#[test]
	fn product_0064_stacks_start_at_zero_under_nonzero_bounds_in_both_orientations() {
		let frame = polars::df!(
			"cat" => ["A", "A"],
			"group" => ["first", "second"],
			"val" => [10.0, 30.0],
		)
		.unwrap();
		for orientation in [Orientation::Vertical, Orientation::Horizontal] {
			let kernel = StackedBarPlotKernel {
				prepared_data: Arc::new(prepare_stacked_bar_data(&frame, "cat", "group", "val")),
				orientation,
				layer_settings: LayerSettings::default(),
				band_fills: Vec::new(),
				group_colors: Vec::new(),
				normalization: StackNormalization::Absolute,
			};
			let mut settings = ResolvedPlotSettings::default();
			match orientation {
				Orientation::Vertical => {
					settings.y_min = Some(20.0);
					settings.y_max = Some(150.0);
				}
				Orientation::Horizontal => {
					settings.x_min = Some(20.0);
					settings.x_max = Some(150.0);
				}
			}
			let layout = kernel.layout(settings.clone());
			let bounds = Rectangle::new(Point::ORIGIN, Size::new(260.0, 260.0));
			let transform = CoordinateTransformer::new(&layout, bounds);
			let bars = kernel.stacked_rectangles(
				&transform,
				settings.bar_width.fraction(),
				settings.category_gap.fraction(),
			);
			assert_eq!(
				bars.iter()
					.map(|bar| (bar.value, bar.running_total))
					.collect::<Vec<_>>(),
				vec![(10.0, 10.0), (30.0, 40.0)],
				"authored bounds clip a zero-based stack instead of rebasing it"
			);
			let visible = bars[1];
			let data_point = transform.categorical(0, 25.0).0;
			assert!(visible.rect.contains(data_point));
			assert_eq!(
				kernel
					.hover_resolved(&transform, Cursor::Available(data_point), &settings)
					.as_deref(),
				Some("A: second (Value: 30.00, Total: 40.00)"),
				"paint and hover agree inside the visible portion of a clipped stack"
			);
		}
	}

	#[test]
	fn product_0085_0072_percent_uses_one_geometry_and_a_truthful_oriented_title() {
		let frame = polars::df!(
			"cat" => ["East", "East", "East"],
			"group" => ["Atlas", "Beacon", "Atlas"],
			"val" => [30.0, 90.0, 300.0],
		)
		.unwrap();
		let prepared = prepare_stacked_bar_data(&frame, "cat", "group", "val");
		assert_eq!(prepared.category_values, [vec![30.0, 90.0]]);
		assert_eq!(prepared.totals, [120.0]);
		let kernel = StackedBarPlotKernel {
			prepared_data: Arc::new(prepared),
			orientation: Orientation::Vertical,
			layer_settings: LayerSettings::default(),
			band_fills: Vec::new(),
			group_colors: Vec::new(),
			normalization: StackNormalization::Percent,
		};
		let settings = ResolvedPlotSettings::default();
		let layout = kernel.layout(settings.clone());
		let PlotLayout::CategoricalX {
			y_range,
			y_axis_type,
			..
		} = layout
		else {
			panic!("vertical percent stack uses categorical x")
		};
		assert_eq!(y_range, (0.0, 100.0));
		assert_eq!(y_axis_type, crate::plot::common::AxisType::Percent);
		assert_eq!(kernel.x_label(), "cat");
		assert_eq!(kernel.y_label(), "val (% of total)");
		let layout = kernel.layout(settings.clone());
		let bounds = Rectangle::new(Point::ORIGIN, Size::new(100.0, 100.0));
		let transform = CoordinateTransformer::new(&layout, bounds);
		let bars = kernel.stacked_rectangles(
			&transform,
			settings.bar_width.fraction(),
			settings.category_gap.fraction(),
		);
		assert_eq!(bars.len(), 2);
		assert_eq!(
			(bars[0].value, bars[0].running_total, bars[0].total),
			(30.0, 30.0, 120.0)
		);
		assert_eq!(bars[0].rect.y, 75.0);
		assert_eq!(
			bars[1].rect.y, 0.0,
			"the retained total projects exactly to 100%"
		);
		let cursor = Point::new(
			bars[0].rect.x + bars[0].rect.width / 2.0,
			bars[0].rect.y + bars[0].rect.height / 2.0,
		);
		assert_eq!(
			kernel
				.hover(&transform, Cursor::Available(cursor))
				.as_deref(),
			Some("East: Atlas (Value: 30.00, Share: 25.0%, Cumulative: 25.0%, Total: 120.00)")
		);

		let horizontal = StackedBarPlotKernel {
			prepared_data: kernel.prepared_data.clone(),
			orientation: Orientation::Horizontal,
			layer_settings: LayerSettings::default(),
			band_fills: Vec::new(),
			group_colors: Vec::new(),
			normalization: StackNormalization::Percent,
		};
		let layout = horizontal.layout(settings.clone());
		let PlotLayout::CategoricalY {
			x_range,
			x_axis_type,
			..
		} = layout
		else {
			panic!("horizontal percent stack uses categorical y")
		};
		assert_eq!(x_range, (0.0, 100.0));
		assert_eq!(x_axis_type, crate::plot::common::AxisType::Percent);
		assert_eq!(horizontal.x_label(), "val (% of total)");
		assert_eq!(horizontal.y_label(), "cat");
		let transform = CoordinateTransformer::new(&layout, bounds);
		let bars = horizontal.stacked_rectangles(
			&transform,
			settings.bar_width.fraction(),
			settings.category_gap.fraction(),
		);
		assert_eq!(bars[0].rect.x, 0.0);
		assert_eq!(bars[0].rect.width, 25.0);
		assert_eq!(bars[1].rect.x + bars[1].rect.width, 100.0);

		let narrow = polars::df!(
			"cat" => ["East", "East"],
			"group" => ["Atlas", "Beacon"],
			"val" => [999.0, 1.0],
		)
		.unwrap();
		let narrow = StackedBarPlotKernel {
			prepared_data: Arc::new(prepare_stacked_bar_data(&narrow, "cat", "group", "val")),
			orientation: Orientation::Horizontal,
			layer_settings: LayerSettings::default(),
			band_fills: Vec::new(),
			group_colors: Vec::new(),
			normalization: StackNormalization::Percent,
		};
		let layout = narrow.layout(settings.clone());
		let transform = CoordinateTransformer::new(&layout, bounds);
		let bars = narrow.stacked_rectangles(
			&transform,
			settings.bar_width.fraction(),
			settings.category_gap.fraction(),
		);
		assert!(
			bars[1].rect.width < 1.0,
			"percent geometry has no minimum-pixel distortion"
		);
		assert_eq!(
			bars[1].rect.x + bars[1].rect.width,
			100.0,
			"even a subpixel final segment ends at the exact retained-total projection"
		);
	}

	#[test]
	fn product_0072_invalid_samples_are_zero_and_zero_totals_invent_no_share() {
		let frame = polars::df!(
			"cat" => ["mixed", "mixed", "mixed", "mixed", "mixed", "mixed",
				"empty", "empty", "empty", "empty", "empty", "empty"],
			"group" => ["negative", "zero", "infinite", "nan", "missing", "positive",
				"negative", "zero", "infinite", "nan", "missing", "positive"],
			"val" => [
				Some(-5.0), Some(0.0), Some(f64::INFINITY), Some(f64::NAN), None, Some(40.0),
				Some(-1.0), Some(0.0), Some(f64::INFINITY), Some(f64::NAN), None, Some(0.0),
			],
		)
		.unwrap();
		let prepared = prepare_stacked_bar_data(&frame, "cat", "group", "val");
		assert_eq!(prepared.categories, ["empty", "mixed"]);
		assert_eq!(
			prepared.group_names,
			["infinite", "missing", "nan", "negative", "positive", "zero"]
		);
		assert_eq!(prepared.totals, [0.0, 40.0]);
		assert_eq!(
			prepared.category_values,
			[
				vec![0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
				vec![0.0, 0.0, 0.0, 0.0, 40.0, 0.0],
			],
			"negative, zero, non-finite, and missing samples enter neither numerator nor denominator"
		);
		let kernel = StackedBarPlotKernel {
			prepared_data: Arc::new(prepared),
			orientation: Orientation::Vertical,
			layer_settings: LayerSettings::default(),
			band_fills: Vec::new(),
			group_colors: Vec::new(),
			normalization: StackNormalization::Percent,
		};
		let settings = ResolvedPlotSettings::default();
		let layout = kernel.layout(settings.clone());
		assert!(matches!(
			layout,
			PlotLayout::CategoricalX {
				y_range: (0.0, 100.0),
				y_axis_type: crate::plot::common::AxisType::Percent,
				..
			}
		));
		let transform = CoordinateTransformer::new(
			&layout,
			Rectangle::new(Point::ORIGIN, Size::new(200.0, 100.0)),
		);
		let bars = kernel.stacked_rectangles(
			&transform,
			settings.bar_width.fraction(),
			settings.category_gap.fraction(),
		);
		assert_eq!(bars.len(), 1, "the zero-total category emits no rectangles");
		assert_eq!(bars[0].value, 40.0);
		assert_eq!(bars[0].total, 40.0);
		assert_eq!(bars[0].rect.height, 100.0);
		let empty_center = transform.categorical(0, 50.0).0;
		assert_eq!(
			kernel.hover(&transform, Cursor::Available(empty_center)),
			None,
			"an absent denominator must not manufacture 0% share prose"
		);

		let one = polars::df!("cat" => ["A", "B"], "val" => [2.0, 3.0]).unwrap();
		let one = StackedBarPlotKernel {
			prepared_data: Arc::new(prepare_stacked_bar_data(&one, "cat", "", "val")),
			orientation: Orientation::Vertical,
			layer_settings: LayerSettings::default(),
			band_fills: Vec::new(),
			group_colors: Vec::new(),
			normalization: StackNormalization::Percent,
		};
		let layout = one.layout(settings.clone());
		let transform = CoordinateTransformer::new(
			&layout,
			Rectangle::new(Point::ORIGIN, Size::new(200.0, 100.0)),
		);
		let bars = one.stacked_rectangles(
			&transform,
			settings.bar_width.fraction(),
			settings.category_gap.fraction(),
		);
		assert_eq!(bars.len(), 2);
		assert!(bars.iter().all(|bar| bar.rect.height == 100.0));
		let content = one
			.legend_content(&settings)
			.expect("synthetic one-series content remains observable");
		let LegendSection::Entries(entries) = content.sections().next().unwrap() else {
			panic!("synthetic bar legend content is categorical")
		};
		assert_eq!(entries.first().provenance, LegendProvenance::Synthetic);
		assert!(
			apply_policy(content).is_none(),
			"a positive two-field 100% stack does not manufacture a legend"
		);
	}
}
