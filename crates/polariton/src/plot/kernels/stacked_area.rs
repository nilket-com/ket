use crate::plot::common::{
	AxisTitleTransform, AxisType, CoordinateTransformer, PathBuilder, PlotBackend, PlotKernel,
	PlotLayout, ResolvedPlotSettings, TemporalAxisDomain, derive_axis_title, format_label,
	polars_type_to_axis_type, temporal_axis_domain,
};
use crate::plot::legend::{
	LegendProvenance, LegendSample, entries_from_pairs_with_sample_and_provenance,
};
use crate::plot::spec::{CategoryColorAssignment, StackNormalization};
use iced::Rectangle;
use iced::advanced::mouse::Cursor;
use iced::widget::canvas::{Stroke, Style};
use polars::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;

pub struct StackedAreaPlotKernel {
	pub prepared_data: Arc<StackedAreaPreparedData>,
	pub series_colors: Vec<CategoryColorAssignment>,
	pub normalization: StackNormalization,
}

impl StackedAreaPlotKernel {
	fn series_color(&self, category_index: usize, settings: &ResolvedPlotSettings) -> iced::Color {
		let category = &self.prepared_data.categories[category_index];
		self.series_colors
			.iter()
			.find(|assignment| assignment.category == *category)
			.map(|assignment| {
				iced::Color::from_rgba8(
					assignment.color.red,
					assignment.color.green,
					assignment.color.blue,
					assignment.color.alpha as f32 / u8::MAX as f32,
				)
			})
			.unwrap_or_else(|| {
				if self.series_colors.is_empty() {
					let total = self.prepared_data.categories.len();
					settings.color_theme.get_color(if total > 1 {
						category_index as f32 / (total - 1) as f32
					} else {
						0.5
					})
				} else {
					panic!("resolved area series {category:?} has no color assignment")
				}
			})
	}
}

impl PlotKernel for StackedAreaPlotKernel {
	fn layout(&self, settings: ResolvedPlotSettings) -> PlotLayout {
		let percent = matches!(self.normalization, StackNormalization::Percent);
		PlotLayout::Cartesian {
			x_range: (
				settings.x_min.unwrap_or(self.prepared_data.x_range.0),
				settings.x_max.unwrap_or(self.prepared_data.x_range.1),
			),
			y_range: if percent {
				(0.0, 100.0)
			} else {
				(
					settings.y_min.unwrap_or(self.prepared_data.y_range.0),
					settings.y_max.unwrap_or(self.prepared_data.y_range.1),
				)
			},
			x_axis_type: self.prepared_data.x_axis_type.clone(),
			y_axis_type: if percent {
				AxisType::Percent
			} else {
				self.prepared_data.y_axis_type.clone()
			},
			x_temporal_domain: self.prepared_data.x_temporal_domain.clone(),
			y_temporal_domain: (!percent)
				.then(|| self.prepared_data.y_temporal_domain.clone())
				.flatten(),
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
		let num_cats = self.prepared_data.categories.len();
		let num_xs = self.prepared_data.unique_xs.len();
		if num_xs < 2 {
			return;
		}
		let mut prev_stacked_values = vec![0.0f64; num_xs];
		let mut current_stacked_values = vec![0.0f64; num_xs];
		for cat_idx in 0..num_cats {
			if self.prepared_data.category_values[cat_idx]
				.iter()
				.all(|value| *value == 0.0)
			{
				continue;
			}
			let color = self.series_color(cat_idx, &settings);
			for x_idx in 0..num_xs {
				current_stacked_values[x_idx] =
					prev_stacked_values[x_idx] + self.prepared_data.category_values[cat_idx][x_idx];
			}
			let area_closure = |builder: &mut dyn PathBuilder| {
				for (x_idx, current) in current_stacked_values.iter().enumerate() {
					let rendered = self
						.normalization
						.project(*current, self.prepared_data.totals[x_idx]);
					let p = transform.cartesian(self.prepared_data.unique_xs[x_idx], rendered);
					if x_idx == 0 {
						builder.move_to(p);
					} else {
						builder.line_to(p);
					}
				}
				for x_idx in (0..num_xs).rev() {
					let rendered = self
						.normalization
						.project(prev_stacked_values[x_idx], self.prepared_data.totals[x_idx]);
					let p = transform.cartesian(self.prepared_data.unique_xs[x_idx], rendered);
					builder.line_to(p);
				}
				builder.close();
			};
			backend.fill_path(&area_closure, color);
			let stroke = Stroke {
				style: Style::Solid(settings.mark_style.decoration.iced()),
				width: 0.5,
				..Default::default()
			};
			backend.stroke_path(&area_closure, stroke);
			prev_stacked_values.copy_from_slice(&current_stacked_values);
		}
	}

	fn hover(&self, transform: &CoordinateTransformer, cursor: Cursor) -> Option<String> {
		if let Some(cursor_pos) = cursor.position()
			&& let Some((x, y)) = transform.pixel_to_cartesian(cursor_pos)
		{
			let (x_min, x_max) = self.prepared_data.x_range;
			if x < x_min || x > x_max {
				return None;
			}
			let xs = &self.prepared_data.unique_xs;
			if xs.len() < 2 {
				return None;
			}
			let idx = match xs.binary_search_by(|val| val.partial_cmp(&x).unwrap()) {
				Ok(i) => i,
				Err(i) => {
					if i == 0 {
						0
					} else if i == xs.len() {
						xs.len() - 1
					} else if (xs[i] - x).abs() < (xs[i - 1] - x).abs() {
						i
					} else {
						i - 1
					}
				}
			};
			let actual_x = xs[idx];
			let total = self.prepared_data.totals[idx];
			let mut current_stack_value = 0.0;
			for (j, cat_vals) in self.prepared_data.category_values.iter().enumerate() {
				let val = cat_vals[idx];
				let next_stack_value = current_stack_value + val;
				let lower = self.normalization.project(current_stack_value, total);
				let upper = self.normalization.project(next_stack_value, total);
				if val > 0.0 && y >= lower && y <= upper {
					return match self.normalization {
						StackNormalization::Absolute => Some(format!(
							"X: {}\n{}: {:.2}\nTotal: {:.2}",
							format_label(actual_x, &self.prepared_data.x_axis_type),
							self.prepared_data.categories[j],
							val,
							next_stack_value
						)),
						StackNormalization::Percent => Some(format!(
							"X: {}\n{}: {:.2}\nShare: {:.1}%\nCumulative: {:.1}%\nTotal: {:.2}",
							format_label(actual_x, &self.prepared_data.x_axis_type),
							self.prepared_data.categories[j],
							val,
							self.normalization.project(val, total),
							upper,
							total
						)),
					};
				}
				current_stack_value = next_stack_value;
			}
			return Some(format!(
				"X: {}, Total Sum: {:.2}",
				format_label(actual_x, &self.prepared_data.x_axis_type),
				current_stack_value
			));
		}
		None
	}

	fn legend_content(
		&self,
		settings: &ResolvedPlotSettings,
	) -> Option<crate::plot::legend::LegendContent> {
		entries_from_pairs_with_sample_and_provenance(
			self.prepared_data
				.categories
				.iter()
				.enumerate()
				.map(|(index, category)| (category.clone(), self.series_color(index, settings))),
			LegendSample::Swatch,
			self.prepared_data.series_provenance,
		)
	}

	fn x_label(&self) -> String {
		self.prepared_data.x_label.clone()
	}

	fn y_label(&self) -> String {
		match self.normalization {
			StackNormalization::Absolute => self.prepared_data.y_label.clone(),
			StackNormalization::Percent => derive_axis_title(
				&self.prepared_data.y_label,
				AxisTitleTransform::PercentOfTotal,
			),
		}
	}
}

pub struct StackedAreaPreparedData {
	pub categories: Vec<String>,
	pub unique_xs: Vec<f64>,
	pub category_values: Vec<Vec<f64>>,
	pub totals: Vec<f64>,
	pub x_range: (f64, f64),
	pub y_range: (f64, f64),
	pub x_axis_type: AxisType,
	pub y_axis_type: AxisType,
	pub x_temporal_domain: Option<TemporalAxisDomain>,
	pub y_temporal_domain: Option<TemporalAxisDomain>,
	pub x_label: String,
	pub y_label: String,
	pub series_provenance: LegendProvenance,
}

pub fn prepare_stacked_area_data(
	df: &DataFrame,
	cat_col: &str,
	x_col: &str,
	y_col: &str,
) -> StackedAreaPreparedData {
	prepare_stacked_area_data_inner(df, cat_col, x_col, y_col, None)
}

/// PRODUCT-0069 §1: resolved categorical assignments own display-domain
/// order. Their colour values remain deliberately unconsumed until
/// PRODUCT-0070 makes the area renderer authorable.
pub fn prepare_stacked_area_data_with_resolved_order(
	df: &DataFrame,
	cat_col: &str,
	x_col: &str,
	y_col: &str,
	assignments: &[CategoryColorAssignment],
) -> StackedAreaPreparedData {
	prepare_stacked_area_data_inner(df, cat_col, x_col, y_col, Some(assignments))
}

fn category_label(value: AnyValue<'_>) -> String {
	if let AnyValue::String(value) = value {
		value.to_owned()
	} else if value.is_null() {
		"Null".to_owned()
	} else {
		value.to_string().replace('"', "")
	}
}

fn prepare_stacked_area_data_inner(
	df: &DataFrame,
	cat_col: &str,
	x_col: &str,
	y_col: &str,
	assignments: Option<&[CategoryColorAssignment]>,
) -> StackedAreaPreparedData {
	if df.height() == 0 || x_col.is_empty() || y_col.is_empty() {
		return StackedAreaPreparedData {
			categories: vec!["No Data".to_string()],
			unique_xs: vec![0.0, 1.0],
			category_values: vec![vec![0.0, 0.0]],
			totals: vec![0.0, 0.0],
			x_range: (0.0, 1.0),
			y_range: (0.0, 1.0),
			x_axis_type: AxisType::Linear,
			y_axis_type: AxisType::Linear,
			x_temporal_domain: None,
			y_temporal_domain: None,
			x_label: x_col.to_string(),
			y_label: y_col.to_string(),
			series_provenance: if cat_col.is_empty() {
				LegendProvenance::Synthetic
			} else {
				LegendProvenance::Named
			},
		};
	}
	let x_dtype = df
		.column(x_col)
		.map(|c| c.dtype().clone())
		.unwrap_or(DataType::Float64);
	let y_dtype = df
		.column(y_col)
		.map(|c| c.dtype().clone())
		.unwrap_or(DataType::Float64);
	let x_axis_type = polars_type_to_axis_type(&x_dtype);
	let y_axis_type = polars_type_to_axis_type(&y_dtype);
	let x_temporal_domain = df
		.column(x_col)
		.ok()
		.and_then(|column| temporal_axis_domain(column, &x_axis_type));
	let y_temporal_domain = df
		.column(y_col)
		.ok()
		.and_then(|column| temporal_axis_domain(column, &y_axis_type));
	let categories = if cat_col.is_empty() {
		vec!["All Data".to_string()]
	} else if let Some(assignments) = assignments {
		assignments
			.iter()
			.map(|assignment| assignment.category.clone())
			.collect()
	} else {
		match df.column(cat_col) {
			Ok(column) => (0..df.height())
				.filter_map(|row| column.get(row).ok())
				.map(category_label)
				.fold(Vec::new(), |mut seen, label| {
					if !seen.contains(&label) {
						seen.push(label);
					}
					seen
				}),
			Err(_) => vec!["All Data".to_string()],
		}
	};
	let unique_xs_series_res = df
		.column(x_col)
		.and_then(|c| c.unique())
		.and_then(|c| c.sort(Default::default()));
	let unique_xs_series =
		unique_xs_series_res.unwrap_or_else(|_| Series::new(x_col.into(), &[0.0, 1.0]).into());
	let unique_xs_f64 = unique_xs_series
		.cast(&DataType::Float64)
		.unwrap_or_else(|_| Series::new(x_col.into(), &[0.0, 1.0]).into());
	let unique_xs: Vec<f64> = unique_xs_f64.f64().unwrap().into_no_null_iter().collect();
	let num_cats = categories.len();
	let num_xs = unique_xs.len();
	if num_xs < 2 || num_cats == 0 {
		return StackedAreaPreparedData {
			categories,
			unique_xs: if num_xs < 2 {
				vec![0.0, 1.0]
			} else {
				unique_xs
			},
			category_values: vec![vec![0.0; num_xs.max(2)]; num_cats.max(1)],
			totals: vec![0.0; num_xs.max(2)],
			x_range: (0.0, 1.0),
			y_range: (0.0, 1.0),
			x_axis_type,
			y_axis_type,
			x_temporal_domain,
			y_temporal_domain,
			x_label: x_col.to_string(),
			y_label: y_col.to_string(),
			series_provenance: if cat_col.is_empty() {
				LegendProvenance::Synthetic
			} else {
				LegendProvenance::Named
			},
		};
	}
	let mut category_values = vec![vec![0.0f64; num_xs]; num_cats];
	let mut assigned = vec![vec![false; num_xs]; num_cats];
	let cat_to_idx: HashMap<String, usize> = categories
		.iter()
		.enumerate()
		.map(|(i, s)| (s.clone(), i))
		.collect();
	let x_to_idx: HashMap<u64, usize> = unique_xs
		.iter()
		.enumerate()
		.map(|(i, &x)| (x.to_bits(), i))
		.collect();
	let binding_x = df
		.column(x_col)
		.map(|c| {
			c.cast(&DataType::Float64).unwrap_or_else(|_| {
				Column::from(Series::new_empty(x_col.into(), &DataType::Float64))
			})
		})
		.unwrap_or_else(|_| Column::from(Series::new_empty(x_col.into(), &DataType::Float64)));
	let p_x = binding_x.f64().unwrap();
	let p_cat = if cat_col.is_empty() {
		None
	} else {
		df.column(cat_col).ok()
	};
	let binding_y = df
		.column(y_col)
		.map(|c| {
			c.cast(&DataType::Float64).unwrap_or_else(|_| {
				Column::from(Series::new_empty(y_col.into(), &DataType::Float64))
			})
		})
		.unwrap_or_else(|_| Column::from(Series::new_empty(y_col.into(), &DataType::Float64)));
	let p_y = binding_y.f64().unwrap();
	for i in 0..df.height() {
		let x = p_x.get(i).unwrap_or(0.0);
		let cat_str = if let Some(p_cat) = &p_cat {
			let cat_val = p_cat.get(i).unwrap_or(AnyValue::Null);
			category_label(cat_val)
		} else {
			"All Data".to_string()
		};
		let y = p_y.get(i).unwrap_or(0.0);
		if let (Some(&xi), Some(&ci)) = (x_to_idx.get(&x.to_bits()), cat_to_idx.get(&cat_str)) {
			if !assigned[ci][xi] {
				category_values[ci][xi] = if y.is_finite() && y > 0.0 { y } else { 0.0 };
				assigned[ci][xi] = true;
			}
		}
	}
	let mut totals = vec![0.0f64; num_xs];
	let mut max_sum = 0.0f64;
	for x_idx in 0..num_xs {
		let mut current_sum = 0.0f64;
		for values in category_values.iter().take(num_cats) {
			current_sum += values[x_idx];
		}
		totals[x_idx] = current_sum;
		if current_sum > max_sum {
			max_sum = current_sum;
		}
	}
	let x_range = (unique_xs[0], unique_xs[num_xs - 1]);
	let y_range = (0.0, max_sum.max(0.001) * 1.05);
	StackedAreaPreparedData {
		categories,
		unique_xs,
		category_values,
		totals,
		x_range,
		y_range,
		x_axis_type,
		y_axis_type,
		x_temporal_domain,
		y_temporal_domain,
		x_label: x_col.to_string(),
		y_label: y_col.to_string(),
		series_provenance: if cat_col.is_empty() {
			LegendProvenance::Synthetic
		} else {
			LegendProvenance::Named
		},
	}
}

#[cfg(test)]
mod product_0068_tests {
	use super::*;
	use iced::Color;
	use iced::widget::canvas::Text;

	#[derive(Default)]
	struct BoundaryBackend {
		fills: usize,
		strokes: Vec<(Color, f32)>,
	}

	impl PlotBackend for BoundaryBackend {
		fn stroke_path(&mut self, _f: &dyn Fn(&mut dyn PathBuilder), stroke: Stroke) {
			let color = match stroke.style {
				Style::Solid(color) => color,
				Style::Gradient(_) => panic!("stacked-area boundary must be solid"),
			};
			self.strokes.push((color, stroke.width));
		}

		fn fill_path(&mut self, _f: &dyn Fn(&mut dyn PathBuilder), _color: Color) {
			self.fills += 1;
		}

		fn fill_rectangle(&mut self, _top_left: iced::Point, _size: iced::Size, _color: Color) {}

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
	fn stacked_area_boundaries_use_resolved_mark_decoration_at_the_existing_width() {
		let kernel = StackedAreaPlotKernel {
			prepared_data: Arc::new(StackedAreaPreparedData {
				categories: vec!["Atlas".to_owned(), "Beacon".to_owned()],
				unique_xs: vec![0.0, 1.0, 2.0],
				category_values: vec![vec![1.0, 2.0, 3.0], vec![2.0, 1.0, 2.0]],
				totals: vec![3.0, 3.0, 5.0],
				x_range: (0.0, 2.0),
				y_range: (0.0, 5.0),
				x_axis_type: AxisType::Linear,
				y_axis_type: AxisType::Linear,
				x_temporal_domain: None,
				y_temporal_domain: None,
				x_label: "x".to_owned(),
				y_label: "y".to_owned(),
				series_provenance: LegendProvenance::Named,
			}),
			series_colors: Vec::new(),
			normalization: StackNormalization::Absolute,
		};
		let decoration = Color::from_rgb8(12, 34, 56);
		let mut settings = ResolvedPlotSettings::default();
		settings.mark_style.decoration = crate::plot::chrome::PlotRgba::from_iced(decoration);
		let layout = kernel.layout(settings.clone());
		let bounds = Rectangle::new(iced::Point::ORIGIN, iced::Size::new(200.0, 100.0));
		let transform = CoordinateTransformer::new(&layout, bounds);
		let mut backend = BoundaryBackend::default();
		kernel.plot(
			&mut backend,
			bounds,
			&transform,
			Cursor::Unavailable,
			settings,
		);

		assert_eq!(backend.fills, 2, "both stacked-area fills must render");
		assert_eq!(backend.strokes.len(), 2, "every fill keeps one boundary");
		for (color, width) in backend.strokes {
			assert_eq!(width, 0.5, "PRODUCT-0068 preserves boundary geometry");
			assert_eq!(color, decoration);
			assert_ne!(
				color,
				Color::from_rgba(0.0, 0.0, 0.0, 0.2),
				"the theme-blind pre-PRODUCT-0068 boundary must be absent"
			);
		}
	}
}

#[cfg(test)]
mod product_0069_tests {
	use super::*;
	use crate::plot::export::SvgBackend;
	use crate::plot::legend::{LegendSection, apply_policy};
	use crate::plot::spec::{PalettePosition, ResolvedColor};
	use iced::widget::canvas::Text;
	use iced::{Color, Point, Size};

	fn assignment(category: &str, index: usize, total: usize) -> CategoryColorAssignment {
		CategoryColorAssignment {
			category: category.to_owned(),
			position: PalettePosition::new(index, total).unwrap(),
			color: ResolvedColor {
				red: 200_u8.saturating_sub(index as u8),
				green: 10 + index as u8,
				blue: 20 + index as u8,
				alpha: 255,
			},
		}
	}

	fn legend_labels(kernel: &StackedAreaPlotKernel) -> Vec<String> {
		let content = kernel
			.legend_content(&ResolvedPlotSettings::default())
			.expect("stacked area fixture has legend content");
		let section = content.sections().next().unwrap();
		let LegendSection::Entries(entries) = section else {
			panic!("stacked area legend must be categorical entries");
		};
		entries.iter().map(|entry| entry.label.clone()).collect()
	}

	#[test]
	fn product_0069_resolved_and_automatic_orders_are_both_semantic() {
		let frame = polars::df!(
			"x" => [0_i64, 0, 1, 1],
			"series" => ["Zulu", "Alpha", "Zulu", "Alpha"],
			"y" => [2.0_f64, 5.0, 3.0, 7.0],
		)
		.unwrap();

		let automatic = prepare_stacked_area_data(&frame, "series", "x", "y");
		assert_eq!(
			automatic.categories,
			["Zulu", "Alpha"],
			"automatic areas follow first appearance, never alphabetical order"
		);
		let automatic = StackedAreaPlotKernel {
			prepared_data: Arc::new(automatic),
			series_colors: Vec::new(),
			normalization: StackNormalization::Absolute,
		};
		assert_eq!(legend_labels(&automatic), ["Zulu", "Alpha"]);

		let assignments = [assignment("Alpha", 0, 2), assignment("Zulu", 1, 2)];
		let resolved =
			prepare_stacked_area_data_with_resolved_order(&frame, "series", "x", "y", &assignments);
		assert_eq!(
			resolved.categories,
			["Alpha", "Zulu"],
			"resolved display-domain order must own both stacking and legend order"
		);
		assert_eq!(resolved.category_values, [vec![5.0, 7.0], vec![2.0, 3.0]]);
		let resolved = StackedAreaPlotKernel {
			prepared_data: Arc::new(resolved),
			series_colors: Vec::new(),
			normalization: StackNormalization::Absolute,
		};
		assert_eq!(legend_labels(&resolved), ["Alpha", "Zulu"]);
	}

	#[derive(Default)]
	struct GeometryBackend {
		fills: Vec<Vec<Point>>,
		fill_colors: Vec<Color>,
		strokes: usize,
	}

	#[derive(Default)]
	struct GeometryPath {
		points: Vec<Point>,
	}

	impl PathBuilder for GeometryPath {
		fn move_to(&mut self, point: Point) {
			self.points.push(point);
		}

		fn line_to(&mut self, point: Point) {
			self.points.push(point);
		}

		fn arc_to(&mut self, center: Point, _radius: f32, _start: f32, _end: f32) {
			self.points.push(center);
		}

		fn circle(&mut self, center: Point, _radius: f32) {
			self.points.push(center);
		}

		fn rectangle(&mut self, top_left: Point, size: Size) {
			self.points.push(top_left);
			self.points.push(Point::new(
				top_left.x + size.width,
				top_left.y + size.height,
			));
		}

		fn close(&mut self) {}
	}

	impl PlotBackend for GeometryBackend {
		fn stroke_path(&mut self, _f: &dyn Fn(&mut dyn PathBuilder), _stroke: Stroke) {
			self.strokes += 1;
		}

		fn fill_path(&mut self, f: &dyn Fn(&mut dyn PathBuilder), color: Color) {
			let mut path = GeometryPath::default();
			f(&mut path);
			self.fills.push(path.points);
			self.fill_colors.push(color);
		}

		fn fill_rectangle(&mut self, _top_left: Point, _size: Size, _color: Color) {}

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
	fn product_0069_first_row_and_clamped_matrix_own_every_consumer() {
		let frame = polars::df!(
			"x" => [0_i64, 0, 0, 0, 0, 1, 1, 1, 1],
			"series" => [
				"Atlas", "Atlas", "Beacon", "Cinder", "Dross",
				"Atlas", "Beacon", "Cinder", "Dross",
			],
			"y" => [2.0_f64, 5.0, -4.0, 0.0, f64::NAN, 0.0, 3.0, 4.0, -1.0],
		)
		.unwrap();
		let prepared = prepare_stacked_area_data(&frame, "series", "x", "y");

		assert_eq!(prepared.categories, ["Atlas", "Beacon", "Cinder", "Dross"]);
		assert_eq!(
			prepared.category_values,
			[
				vec![2.0, 0.0],
				vec![0.0, 3.0],
				vec![0.0, 4.0],
				vec![0.0, 0.0],
			],
			"first-row-wins must replace both the old sum 7 and plausible last-row 5; non-positive and non-finite samples clamp once"
		);
		assert!((prepared.y_range.1 - 7.0 * 1.05).abs() < f64::EPSILON);
		assert_ne!(
			prepared.category_values[0][0], 7.0,
			"implicit sum must be absent"
		);
		assert_ne!(
			prepared.category_values[0][0], 5.0,
			"last-row mis-repair must be absent"
		);

		let kernel = StackedAreaPlotKernel {
			prepared_data: Arc::new(prepared),
			series_colors: Vec::new(),
			normalization: StackNormalization::Absolute,
		};
		let settings = ResolvedPlotSettings::default();
		let bounds = Rectangle::new(Point::ORIGIN, Size::new(200.0, 140.0));
		let layout = kernel.layout(settings.clone());
		let transform = CoordinateTransformer::new(&layout, bounds);
		let mut backend = GeometryBackend::default();
		kernel.plot(
			&mut backend,
			bounds,
			&transform,
			Cursor::Unavailable,
			settings,
		);

		assert_eq!(
			backend.fills.len(),
			3,
			"an all-zero series emits no polygon"
		);
		assert_eq!(
			backend.strokes, 3,
			"only emitted polygons receive boundaries"
		);
		for path in &backend.fills {
			assert_eq!(path.len(), 4, "two x samples retain one polygon per series");
			for x_index in 0..2 {
				let top = path[x_index];
				let bottom = path[3 - x_index];
				assert_eq!(top.x, bottom.x);
				assert!(
					top.y <= bottom.y,
					"a canonical non-negative band may pinch but cannot fold beneath its baseline"
				);
			}
		}

		let on_beacon = transform.cartesian(1.0, 1.0);
		let hover = kernel
			.hover(&transform, Cursor::Available(on_beacon))
			.expect("positive Beacon band is hoverable");
		assert!(hover.contains("Beacon: 3.00"));
		assert!(hover.contains("Total: 3.00"));
		assert!(
			!hover.contains("Atlas"),
			"Atlas's zero sample owns no hover interval"
		);
	}

	#[test]
	fn product_0069_coincident_fallback_keeps_the_banked_ramp_and_headroom() {
		let frame = polars::df!(
			"x" => [0_i64, 0, 1, 1],
			"series" => ["Atlas", "Beacon", "Atlas", "Beacon"],
			"y" => [2.0_f64, 3.0, 4.0, 5.0],
		)
		.unwrap();
		let prepared = prepare_stacked_area_data(&frame, "series", "x", "y");
		let banked = StackedAreaPreparedData {
			categories: vec!["Atlas".to_owned(), "Beacon".to_owned()],
			unique_xs: vec![0.0, 1.0],
			category_values: vec![vec![2.0, 4.0], vec![3.0, 5.0]],
			totals: vec![5.0, 9.0],
			x_range: (0.0, 1.0),
			y_range: (0.0, 9.0 * 1.05),
			x_axis_type: AxisType::Linear,
			y_axis_type: AxisType::Linear,
			x_temporal_domain: None,
			y_temporal_domain: None,
			x_label: "x".to_owned(),
			y_label: "y".to_owned(),
			series_provenance: LegendProvenance::Named,
		};
		let kernel = StackedAreaPlotKernel {
			prepared_data: Arc::new(prepared),
			series_colors: Vec::new(),
			normalization: StackNormalization::Absolute,
		};
		assert_eq!(kernel.prepared_data.categories, ["Atlas", "Beacon"]);
		assert_eq!(
			kernel.prepared_data.category_values,
			[vec![2.0, 4.0], vec![3.0, 5.0]]
		);
		assert_eq!(kernel.prepared_data.y_range, (0.0, 9.0 * 1.05));

		let settings = ResolvedPlotSettings::default();
		let layout = kernel.layout(settings.clone());
		let bounds = Rectangle::new(Point::ORIGIN, Size::new(200.0, 140.0));
		let transform = CoordinateTransformer::new(&layout, bounds);
		let mut backend = GeometryBackend::default();
		kernel.plot(
			&mut backend,
			bounds,
			&transform,
			Cursor::Unavailable,
			settings.clone(),
		);
		assert_eq!(
			backend.fill_colors,
			[
				settings.color_theme.get_color(0.0),
				settings.color_theme.get_color(1.0),
			],
			"PRODUCT-0069 consumes resolved order only; fallback colour bytes stay banked"
		);

		let render_svg = |kernel: &StackedAreaPlotKernel| {
			let settings = ResolvedPlotSettings::default();
			let layout = kernel.layout(settings.clone());
			let transform = CoordinateTransformer::new(&layout, bounds);
			let mut backend = SvgBackend::new(bounds.width, bounds.height);
			kernel.plot(
				&mut backend,
				bounds,
				&transform,
				Cursor::Unavailable,
				settings,
			);
			backend.finish()
		};
		let banked = StackedAreaPlotKernel {
			prepared_data: Arc::new(banked),
			series_colors: Vec::new(),
			normalization: StackNormalization::Absolute,
		};
		assert_eq!(
			render_svg(&kernel),
			render_svg(&banked),
			"alphabetical, unique, positive stacked areas remain byte-identical to the banked preparation"
		);
	}

	#[test]
	fn product_0070_resolved_colors_reach_fills_and_legend_while_two_field_is_synthetic() {
		let frame = polars::df!(
			"x" => [0_i64, 0, 1, 1],
			"series" => ["Beacon", "Atlas", "Beacon", "Atlas"],
			"y" => [2.0_f64, 5.0, 3.0, 7.0],
		)
		.unwrap();
		let assignments = [assignment("Atlas", 0, 2), assignment("Beacon", 1, 2)];
		let prepared =
			prepare_stacked_area_data_with_resolved_order(&frame, "series", "x", "y", &assignments);
		let kernel = StackedAreaPlotKernel {
			prepared_data: Arc::new(prepared),
			series_colors: assignments.to_vec(),
			normalization: StackNormalization::Absolute,
		};
		let settings = ResolvedPlotSettings::default();
		let bounds = Rectangle::new(Point::ORIGIN, Size::new(200.0, 140.0));
		let layout = kernel.layout(settings.clone());
		let transform = CoordinateTransformer::new(&layout, bounds);
		let mut backend = GeometryBackend::default();
		kernel.plot(
			&mut backend,
			bounds,
			&transform,
			Cursor::Unavailable,
			settings.clone(),
		);
		let exact = assignments
			.iter()
			.map(|assignment| {
				Color::from_rgba8(
					assignment.color.red,
					assignment.color.green,
					assignment.color.blue,
					1.0,
				)
			})
			.collect::<Vec<_>>();
		assert_eq!(
			backend.fill_colors, exact,
			"area fills consume resolved assignments rather than reconstructing the ramp"
		);
		let legend = kernel.legend_content(&settings).unwrap();
		let LegendSection::Entries(entries) = legend.sections().next().unwrap() else {
			panic!("named area legend is categorical")
		};
		assert_eq!(
			entries
				.iter()
				.map(|entry| entry.label.as_str())
				.collect::<Vec<_>>(),
			["Atlas", "Beacon"]
		);
		assert_eq!(
			entries.iter().map(|entry| entry.swatch).collect::<Vec<_>>(),
			exact,
			"legend swatches and fills must share the same resolved colors"
		);

		let one = polars::df!("x" => [0_i64, 1], "y" => [2.0_f64, 3.0]).unwrap();
		let one = StackedAreaPlotKernel {
			prepared_data: Arc::new(prepare_stacked_area_data(&one, "", "x", "y")),
			series_colors: Vec::new(),
			normalization: StackNormalization::Absolute,
		};
		let content = one
			.legend_content(&settings)
			.expect("synthetic content remains observable");
		let LegendSection::Entries(entries) = content.sections().next().unwrap() else {
			panic!("synthetic area legend is categorical")
		};
		assert_eq!(entries.first().provenance, LegendProvenance::Synthetic);
		assert!(
			apply_policy(content).is_none(),
			"the unnamed two-field area must not retain an All Data legend"
		);
	}

	#[test]
	fn product_0085_0072_percent_area_uses_local_totals_and_transform_title_provenance() {
		let frame = polars::df!(
			"x" => [0_i64, 0, 1, 1, 0],
			"series" => ["Atlas", "Beacon", "Atlas", "Beacon", "Atlas"],
			"y" => [30.0, 90.0, 20.0, 20.0, 300.0],
		)
		.unwrap();
		let prepared = prepare_stacked_area_data(&frame, "series", "x", "y");
		assert_eq!(
			prepared.category_values,
			[vec![30.0, 20.0], vec![90.0, 20.0]]
		);
		assert_eq!(prepared.totals, [120.0, 40.0]);
		let kernel = StackedAreaPlotKernel {
			prepared_data: Arc::new(prepared),
			series_colors: Vec::new(),
			normalization: StackNormalization::Percent,
		};
		let settings = ResolvedPlotSettings::default();
		let layout = kernel.layout(settings.clone());
		let PlotLayout::Cartesian {
			y_range,
			y_axis_type,
			..
		} = layout
		else {
			panic!("percent area remains Cartesian")
		};
		assert_eq!(y_range, (0.0, 100.0));
		assert_eq!(y_axis_type, AxisType::Percent);
		assert_eq!(kernel.x_label(), "x");
		assert_eq!(kernel.y_label(), "y (% of total)");
		let layout = kernel.layout(settings);
		let bounds = Rectangle::new(Point::ORIGIN, Size::new(100.0, 100.0));
		let transform = CoordinateTransformer::new(&layout, bounds);
		assert_eq!(
			kernel
				.hover(&transform, Cursor::Available(Point::new(0.0, 87.5)))
				.as_deref(),
			Some("X: 0\nAtlas: 30.00\nShare: 25.0%\nCumulative: 25.0%\nTotal: 120.00")
		);
		assert_eq!(
			kernel
				.hover(&transform, Cursor::Available(Point::new(100.0, 75.0)))
				.as_deref(),
			Some("X: 1\nAtlas: 20.00\nShare: 50.0%\nCumulative: 50.0%\nTotal: 40.00"),
			"each x uses its own denominator rather than a figure-wide total"
		);

		let mut formatted_only = prepare_stacked_area_data(&frame, "series", "x", "y");
		formatted_only.y_axis_type = AxisType::Percent;
		let formatted_only = StackedAreaPlotKernel {
			prepared_data: Arc::new(formatted_only),
			series_colors: Vec::new(),
			normalization: StackNormalization::Absolute,
		};
		assert_eq!(formatted_only.y_label(), "y");
		assert!(matches!(
			formatted_only.layout(ResolvedPlotSettings::default()),
			PlotLayout::Cartesian {
				y_axis_type: AxisType::Percent,
				..
			}
		));
	}

	#[test]
	fn product_0072_area_invalid_samples_pinch_zero_total_without_inventing_share() {
		let frame = polars::df!(
			"x" => [0_i64, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1],
			"series" => [
				"negative", "zero", "infinite", "nan", "missing", "positive",
				"negative", "zero", "infinite", "nan", "missing", "positive",
			],
			"y" => [
				Some(-5.0), Some(0.0), Some(f64::INFINITY), Some(f64::NAN), None, Some(0.0),
				Some(-1.0), Some(0.0), Some(f64::INFINITY), Some(f64::NAN), None, Some(40.0),
			],
		)
		.unwrap();
		let prepared = prepare_stacked_area_data(&frame, "series", "x", "y");
		assert_eq!(prepared.totals, [0.0, 40.0]);
		assert!(
			prepared
				.category_values
				.iter()
				.all(|values| values[0] == 0.0),
			"every band pinches to zero at the zero-total sample"
		);
		assert_eq!(
			prepared
				.category_values
				.iter()
				.map(|values| values[1])
				.collect::<Vec<_>>(),
			[0.0, 0.0, 0.0, 0.0, 0.0, 40.0]
		);
		let kernel = StackedAreaPlotKernel {
			prepared_data: Arc::new(prepared),
			series_colors: Vec::new(),
			normalization: StackNormalization::Percent,
		};
		let settings = ResolvedPlotSettings::default();
		let layout = kernel.layout(settings.clone());
		assert!(matches!(
			layout,
			PlotLayout::Cartesian {
				y_range: (0.0, 100.0),
				y_axis_type: AxisType::Percent,
				..
			}
		));
		let bounds = Rectangle::new(Point::ORIGIN, Size::new(100.0, 100.0));
		let transform = CoordinateTransformer::new(&layout, bounds);
		let mut backend = GeometryBackend::default();
		kernel.plot(
			&mut backend,
			bounds,
			&transform,
			Cursor::Unavailable,
			settings,
		);
		assert_eq!(
			backend.fills.len(),
			1,
			"only the positive series emits a polygon"
		);
		let path = &backend.fills[0];
		assert_eq!(
			path[0], path[3],
			"the upper and lower boundaries meet at x=0"
		);
		assert_eq!(
			path[1].y, 0.0,
			"the positive x=1 boundary reaches exactly 100%"
		);
		assert_eq!(
			kernel
				.hover(
					&transform,
					Cursor::Available(transform.cartesian(0.0, 50.0))
				)
				.as_deref(),
			Some("X: 0, Total Sum: 0.00"),
			"zero total may be reported absolutely but never as an invented share"
		);
		assert!(
			!kernel
				.hover(
					&transform,
					Cursor::Available(transform.cartesian(0.0, 50.0))
				)
				.unwrap()
				.contains('%')
		);

		let one = polars::df!("x" => [0_i64, 1], "y" => [2.0, 3.0]).unwrap();
		let one = StackedAreaPlotKernel {
			prepared_data: Arc::new(prepare_stacked_area_data(&one, "", "x", "y")),
			series_colors: Vec::new(),
			normalization: StackNormalization::Percent,
		};
		let layout = one.layout(ResolvedPlotSettings::default());
		let transform = CoordinateTransformer::new(&layout, bounds);
		assert_eq!(
			one.hover(
				&transform,
				Cursor::Available(transform.cartesian(1.0, 50.0))
			)
			.as_deref(),
			Some("X: 1\nAll Data: 3.00\nShare: 100.0%\nCumulative: 100.0%\nTotal: 3.00")
		);
		let content = one
			.legend_content(&ResolvedPlotSettings::default())
			.expect("synthetic one-band content remains observable");
		assert!(
			apply_policy(content).is_none(),
			"a positive two-field 100% area does not manufacture a legend"
		);
	}
}
