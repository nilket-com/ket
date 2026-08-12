use crate::plot::common::{
	CoordinateTransformer, PlotBackend, PlotKernel, PlotLayout, ResolvedPlotSettings,
};
use iced::advanced::mouse::Cursor;
use iced::widget::canvas::{Stroke, Style};
use iced::{Color, Point, Rectangle, Size};
use polars::prelude::*;
use std::sync::Arc;

pub struct HeatmapPlotKernel {
	pub prepared_data: Arc<HeatmapPreparedData>,
}

impl PlotKernel for HeatmapPlotKernel {
	fn layout(&self, _settings: ResolvedPlotSettings) -> PlotLayout {
		PlotLayout::CategoricalXY {
			x_categories: self.prepared_data.x_categories.clone(),
			y_categories: self.prepared_data.y_categories.clone(),
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
		let num_x = self.prepared_data.x_categories.len();
		let num_y = self.prepared_data.y_categories.len();
		let max_val = self.prepared_data.max_val;
		for i in 0..num_x {
			for j in 0..num_y {
				let val = self.prepared_data.values[i][j];
				let t = if max_val > 0.0 {
					(val / max_val) as f32
				} else {
					0.0
				};
				let color = settings.color_theme.get_color(t);
				let (center, bw, bh) = transform.categorical_2d(i, j);
				let rect_x = center.x - bw / 2.0;
				let rect_y = center.y - bh / 2.0;
				backend.fill_rectangle(Point::new(rect_x, rect_y), Size::new(bw, bh), color);
				backend.stroke_path(
					&|builder| {
						builder.move_to(Point::new(rect_x, rect_y));
						builder.line_to(Point::new(rect_x + bw, rect_y));
						builder.line_to(Point::new(rect_x + bw, rect_y + bh));
						builder.line_to(Point::new(rect_x, rect_y + bh));
						builder.close();
					},
					Stroke {
						style: Style::Solid(Color::from_rgba(0.0, 0.0, 0.0, 0.1)),
						width: 1.0,
						..Default::default()
					},
				);
			}
		}
	}

	fn hover(&self, transform: &CoordinateTransformer, cursor: Cursor) -> Option<String> {
		if let Some(cursor_pos) = cursor.position() {
			let num_x = self.prepared_data.x_categories.len();
			let num_y = self.prepared_data.y_categories.len();
			for i in 0..num_x {
				for j in 0..num_y {
					let (center, bw, bh) = transform.categorical_2d(i, j);
					let rect = Rectangle {
						x: center.x - bw / 2.0,
						y: center.y - bh / 2.0,
						width: bw,
						height: bh,
					};
					if rect.contains(cursor_pos) {
						let x_cat = &self.prepared_data.x_categories[i];
						let y_cat = &self.prepared_data.y_categories[j];
						let val = self.prepared_data.values[i][j];
						return Some(format!("X: {}\nY: {}\nValue: {:.2}", x_cat, y_cat, val));
					}
				}
			}
		}
		None
	}

	fn legend_content(
		&self,
		settings: &ResolvedPlotSettings,
	) -> Option<crate::plot::legend::LegendContent> {
		crate::plot::legend::gradient_section(
			"Value".to_owned(),
			0.0,
			self.prepared_data.max_val,
			settings.color_theme,
		)
		.map(|section| {
			crate::plot::legend::LegendContent(crate::plot::spec::NonEmpty::new(section))
		})
	}

	fn x_label(&self) -> String {
		self.prepared_data.x_label.clone()
	}

	fn y_label(&self) -> String {
		self.prepared_data.y_label.clone()
	}
}

pub struct HeatmapPreparedData {
	pub x_categories: Vec<String>,
	pub y_categories: Vec<String>,
	pub values: Vec<Vec<f64>>,
	pub max_val: f64,
	pub x_label: String,
	pub y_label: String,
}

pub fn prepare_heatmap_data(
	df: &DataFrame,
	x_col: &str,
	y_col: &str,
	val_col: &str,
) -> HeatmapPreparedData {
	if df.height() == 0 || val_col.is_empty() {
		return HeatmapPreparedData {
			x_categories: vec!["No Data".to_string()],
			y_categories: vec!["No Data".to_string()],
			values: vec![vec![0.0]],
			max_val: 1.0,
			x_label: x_col.to_string(),
			y_label: y_col.to_string(),
		};
	}
	let x_cats_series = if x_col.is_empty() {
		Series::new("dummy_x".into(), &["X"])
	} else {
		match df.column(x_col) {
			Ok(c) => c
				.unique()
				.unwrap_or_else(|_| c.clone())
				.sort(Default::default())
				.unwrap_or_else(|_| c.clone())
				.as_materialized_series()
				.clone(),
			Err(_) => Series::new("dummy_x".into(), &["X"]),
		}
	};
	let x_categories: Vec<String> = x_cats_series
		.iter()
		.map(|v: AnyValue| v.to_string().replace("\"", ""))
		.collect();
	let y_cats_series = if y_col.is_empty() {
		Series::new("dummy_y".into(), &["Y"])
	} else {
		match df.column(y_col) {
			Ok(c) => c
				.unique()
				.unwrap_or_else(|_| c.clone())
				.sort(Default::default())
				.unwrap_or_else(|_| c.clone())
				.as_materialized_series()
				.clone(),
			Err(_) => Series::new("dummy_y".into(), &["Y"]),
		}
	};
	let y_categories: Vec<String> = y_cats_series
		.iter()
		.map(|v: AnyValue| v.to_string().replace("\"", ""))
		.collect();
	let num_x = x_categories.len();
	let num_y = y_categories.len();
	let mut values = vec![vec![0.0f64; num_y]; num_x];
	let mut max_val = 0.0f64;
	let x_to_idx: std::collections::HashMap<String, usize> = x_categories
		.iter()
		.enumerate()
		.map(|(i, s): (usize, &String)| (s.clone(), i))
		.collect();
	let y_to_idx: std::collections::HashMap<String, usize> = y_categories
		.iter()
		.enumerate()
		.map(|(i, s): (usize, &String)| (s.clone(), i))
		.collect();
	let binding_val = match df.column(val_col) {
		Ok(c) => c.cast(&DataType::Float64).unwrap_or_else(|_| {
			Column::from(Series::new_empty(val_col.into(), &DataType::Float64))
		}),
		Err(_) => Column::from(Series::new_empty(val_col.into(), &DataType::Float64)),
	};
	let p_val = binding_val.f64().unwrap();
	let p_x = if x_col.is_empty() {
		Series::new("dummy_x".into(), vec!["X"; df.height()])
	} else {
		match df.column(x_col) {
			Ok(c) => c.as_materialized_series().clone(),
			Err(_) => Series::new("dummy_x".into(), vec!["X"; df.height()]),
		}
	};
	let p_y = if y_col.is_empty() {
		Series::new("dummy_y".into(), vec!["Y"; df.height()])
	} else {
		match df.column(y_col) {
			Ok(c) => c.as_materialized_series().clone(),
			Err(_) => Series::new("dummy_y".into(), vec!["Y"; df.height()]),
		}
	};
	for i in 0..df.height() {
		let x_v = p_x
			.get(i)
			.unwrap_or(AnyValue::Null)
			.to_string()
			.replace("\"", "");
		let y_v = p_y
			.get(i)
			.unwrap_or(AnyValue::Null)
			.to_string()
			.replace("\"", "");
		let val = p_val.get(i).unwrap_or(0.0);
		if let (Some(&xi), Some(&yi)) = (x_to_idx.get(&x_v), y_to_idx.get(&y_v)) {
			values[xi][yi] = val;
			if val > max_val {
				max_val = val;
			}
		}
	}
	HeatmapPreparedData {
		x_categories,
		y_categories,
		values,
		max_val,
		x_label: x_col.to_string(),
		y_label: y_col.to_string(),
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::plot::common::{PathBuilder, PlotBackend};
	use iced::widget::canvas::{Stroke, Text};

	#[derive(Default)]
	/// PRODUCT-0057 named oracle `plot::kernels::heatmap::tests::RectangleBackend`
	/// records rectangle tuples only; its shared short name grants no other license.
	struct RectangleBackend {
		rectangles: Vec<(Point, Size, Color)>,
	}

	impl PlotBackend for RectangleBackend {
		fn stroke_path(&mut self, _f: &dyn Fn(&mut dyn PathBuilder), _stroke: Stroke) {}

		fn fill_path(&mut self, _f: &dyn Fn(&mut dyn PathBuilder), _color: Color) {}

		fn fill_rectangle(&mut self, top_left: Point, size: Size, color: Color) {
			self.rectangles.push((top_left, size, color));
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

	#[test]
	fn product_0056_asymmetric_heatmap_rows_join_labels_cells_colors_and_hover() {
		let frame = polars::df!(
			"x" => ["c2", "c0", "c1", "c0", "c2", "c1"],
			"y" => ["row-b", "row-a", "row-b", "row-b", "row-a", "row-a"],
			"value" => [32_i64, 1, 16, 8, 4, 2],
		)
		.unwrap();
		let kernel = HeatmapPlotKernel {
			prepared_data: Arc::new(prepare_heatmap_data(&frame, "x", "y", "value")),
		};
		assert_eq!(kernel.prepared_data.x_categories, ["c0", "c1", "c2"]);
		assert_eq!(kernel.prepared_data.y_categories, ["row-a", "row-b"]);
		assert_eq!(
			kernel.prepared_data.values,
			vec![vec![1.0, 8.0], vec![2.0, 16.0], vec![4.0, 32.0]]
		);

		let settings = ResolvedPlotSettings::default();
		let layout = kernel.layout(settings.clone());
		let bounds = Rectangle::new(Point::ORIGIN, Size::new(300.0, 200.0));
		let transform = CoordinateTransformer::new(&layout, bounds);
		let mut backend = RectangleBackend::default();
		kernel.plot(
			&mut backend,
			bounds,
			&transform,
			Cursor::Unavailable,
			settings.clone(),
		);
		assert_eq!(backend.rectangles.len(), 6);

		for x_index in 0..3 {
			for y_index in 0..2 {
				let index = x_index * 2 + y_index;
				let (top_left, size, color) = backend.rectangles[index];
				let (center, band_width, band_height) = transform.categorical_2d(x_index, y_index);
				assert_eq!(top_left, Point::new(center.x - 50.0, center.y - 50.0));
				assert_eq!(size, Size::new(band_width, band_height));
				let value = kernel.prepared_data.values[x_index][y_index];
				assert_eq!(
					color,
					settings
						.color_theme
						.get_color((value / kernel.prepared_data.max_val) as f32)
				);
				let x = &kernel.prepared_data.x_categories[x_index];
				let y = &kernel.prepared_data.y_categories[y_index];
				assert_eq!(
					kernel.hover(&transform, Cursor::Available(center)),
					Some(format!("X: {x}\nY: {y}\nValue: {value:.2}"))
				);
			}
		}
		assert!(backend.rectangles[0].0.y < backend.rectangles[1].0.y);
	}

	#[test]
	fn product_0087_heatmap_omits_zero_domain_and_banks_signed_clamping_for_b2() {
		let zero = HeatmapPlotKernel {
			prepared_data: Arc::new(HeatmapPreparedData {
				x_categories: vec!["x".to_owned()],
				y_categories: vec!["y".to_owned()],
				values: vec![vec![0.0]],
				max_val: 0.0,
				x_label: "x".to_owned(),
				y_label: "y".to_owned(),
			}),
		};
		assert!(
			zero.legend_content(&ResolvedPlotSettings::default())
				.is_none()
		);

		let signed = HeatmapPlotKernel {
			prepared_data: Arc::new(HeatmapPreparedData {
				x_categories: vec!["x".to_owned()],
				y_categories: vec!["y".to_owned()],
				values: vec![vec![-10.0]],
				max_val: 10.0,
				x_label: "x".to_owned(),
				y_label: "y".to_owned(),
			}),
		};
		let settings = ResolvedPlotSettings::default();
		let layout = signed.layout(settings.clone());
		let bounds = Rectangle::new(Point::ORIGIN, Size::new(100.0, 100.0));
		let transform = CoordinateTransformer::new(&layout, bounds);
		let mut backend = RectangleBackend::default();
		signed.plot(
			&mut backend,
			bounds,
			&transform,
			Cursor::Unavailable,
			settings.clone(),
		);
		assert_eq!(backend.rectangles[0].2, settings.color_theme.get_color(0.0));
		let content = signed
			.legend_content(&settings)
			.expect("positive max guide");
		let crate::plot::legend::LegendSection::Gradient { stops, .. } = content.0.first() else {
			panic!("gradient section")
		};
		assert_eq!(stops.first_value(), 0.0);
		assert!(-10.0 < stops.first_value());
	}
}
