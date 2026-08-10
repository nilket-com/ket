use crate::plot::common::{
	AxisType, CoordinateTransformer, PathBuilder, PlotBackend, PlotKernel, PlotLayout,
	ResolvedPlotSettings, format_label, polars_type_to_axis_type,
};
use crate::plot::spec::CategoryColorAssignment;
use iced::Rectangle;
use iced::advanced::mouse::Cursor;
use iced::widget::canvas::{Stroke, Style};
use polars::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;

pub struct StackedAreaPlotKernel {
	pub prepared_data: Arc<StackedAreaPreparedData>,
}

impl PlotKernel for StackedAreaPlotKernel {
	fn layout(&self, settings: ResolvedPlotSettings) -> PlotLayout {
		PlotLayout::Cartesian {
			x_range: (
				settings.x_min.unwrap_or(self.prepared_data.x_range.0),
				settings.x_max.unwrap_or(self.prepared_data.x_range.1),
			),
			y_range: (
				settings.y_min.unwrap_or(self.prepared_data.y_range.0),
				settings.y_max.unwrap_or(self.prepared_data.y_range.1),
			),
			x_axis_type: self.prepared_data.x_axis_type,
			y_axis_type: self.prepared_data.y_axis_type,
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
		let mut prev_stacked_ys = vec![0.0f64; num_xs];
		let mut current_stacked_ys = vec![0.0f64; num_xs];
		for cat_idx in 0..num_cats {
			if self.prepared_data.category_values[cat_idx]
				.iter()
				.all(|value| *value == 0.0)
			{
				continue;
			}
			let t = if num_cats > 1 {
				cat_idx as f32 / (num_cats - 1) as f32
			} else {
				0.5
			};
			let color = settings.color_theme.get_color(t);
			for x_idx in 0..num_xs {
				current_stacked_ys[x_idx] =
					prev_stacked_ys[x_idx] + self.prepared_data.category_values[cat_idx][x_idx];
			}
			let area_closure = |builder: &mut dyn PathBuilder| {
				for (x_idx, csy) in current_stacked_ys.iter().enumerate() {
					let p = transform.cartesian(self.prepared_data.unique_xs[x_idx], *csy);
					if x_idx == 0 {
						builder.move_to(p);
					} else {
						builder.line_to(p);
					}
				}
				for x_idx in (0..num_xs).rev() {
					let p = transform
						.cartesian(self.prepared_data.unique_xs[x_idx], prev_stacked_ys[x_idx]);
					builder.line_to(p);
				}
				builder.close();
			};
			backend.fill_path(&area_closure, color);
			let stroke = Stroke {
				style: Style::Solid(settings.mark_decoration_color),
				width: 0.5,
				..Default::default()
			};
			backend.stroke_path(&area_closure, stroke);
			prev_stacked_ys.copy_from_slice(&current_stacked_ys);
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
			let mut current_stack_y = 0.0;
			for (j, cat_vals) in self.prepared_data.category_values.iter().enumerate() {
				let val = cat_vals[idx];
				if val > 0.0 && y >= current_stack_y && y <= current_stack_y + val {
					return Some(format!(
						"X: {}\n{}: {:.2}\nTotal: {:.2}",
						format_label(actual_x, self.prepared_data.x_axis_type),
						self.prepared_data.categories[j],
						val,
						current_stack_y + val
					));
				}
				current_stack_y += val;
			}
			return Some(format!(
				"X: {}, Total Sum: {:.2}",
				format_label(actual_x, self.prepared_data.x_axis_type),
				current_stack_y
			));
		}
		None
	}

	fn legend_content(
		&self,
		settings: &ResolvedPlotSettings,
	) -> Option<crate::plot::legend::LegendContent> {
		crate::plot::legend::entries_from_labels(
			self.prepared_data.categories.iter().cloned(),
			settings,
		)
	}

	fn x_label(&self) -> String {
		self.prepared_data.x_label.clone()
	}

	fn y_label(&self) -> String {
		self.prepared_data.y_label.clone()
	}
}

pub struct StackedAreaPreparedData {
	pub categories: Vec<String>,
	pub unique_xs: Vec<f64>,
	pub category_values: Vec<Vec<f64>>,
	pub x_range: (f64, f64),
	pub y_range: (f64, f64),
	pub x_axis_type: AxisType,
	pub y_axis_type: AxisType,
	pub x_label: String,
	pub y_label: String,
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
			x_range: (0.0, 1.0),
			y_range: (0.0, 1.0),
			x_axis_type: AxisType::Linear,
			y_axis_type: AxisType::Linear,
			x_label: x_col.to_string(),
			y_label: y_col.to_string(),
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
			x_range: (0.0, 1.0),
			y_range: (0.0, 1.0),
			x_axis_type,
			y_axis_type,
			x_label: x_col.to_string(),
			y_label: y_col.to_string(),
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
	let mut max_sum = 0.0f64;
	for x_idx in 0..num_xs {
		let mut current_sum = 0.0f64;
		for values in category_values.iter().take(num_cats) {
			current_sum += values[x_idx];
		}
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
		x_range,
		y_range,
		x_axis_type,
		y_axis_type,
		x_label: x_col.to_string(),
		y_label: y_col.to_string(),
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

	#[test]
	fn stacked_area_boundaries_use_resolved_mark_decoration_at_the_existing_width() {
		let kernel = StackedAreaPlotKernel {
			prepared_data: Arc::new(StackedAreaPreparedData {
				categories: vec!["Atlas".to_owned(), "Beacon".to_owned()],
				unique_xs: vec![0.0, 1.0, 2.0],
				category_values: vec![vec![1.0, 2.0, 3.0], vec![2.0, 1.0, 2.0]],
				x_range: (0.0, 2.0),
				y_range: (0.0, 5.0),
				x_axis_type: AxisType::Linear,
				y_axis_type: AxisType::Linear,
				x_label: "x".to_owned(),
				y_label: "y".to_owned(),
			}),
		};
		let decoration = Color::from_rgb8(12, 34, 56);
		let settings = ResolvedPlotSettings {
			mark_decoration_color: decoration,
			..ResolvedPlotSettings::default()
		};
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
	use crate::plot::legend::LegendSection;
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
			x_range: (0.0, 1.0),
			y_range: (0.0, 9.0 * 1.05),
			x_axis_type: AxisType::Linear,
			y_axis_type: AxisType::Linear,
			x_label: "x".to_owned(),
			y_label: "y".to_owned(),
		};
		let kernel = StackedAreaPlotKernel {
			prepared_data: Arc::new(prepared),
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
		};
		assert_eq!(
			render_svg(&kernel),
			render_svg(&banked),
			"alphabetical, unique, positive stacked areas remain byte-identical to the banked preparation"
		);
	}
}
