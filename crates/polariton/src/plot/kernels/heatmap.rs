use crate::plot::common::{
	CoordinateTransformer, PlotBackend, PlotKernel, PlotLayout, ResolvedPlotSettings,
};
use crate::plot::continuous_color::{ContinuousColorScale, ContinuousScaleIntent};
use crate::plot::kernels::heatmap_cells::{HeatmapCellRow, HeatmapCells, HeatmapCellsError};
use iced::advanced::mouse::Cursor;
use iced::widget::canvas::{Stroke, Style};
use iced::{Color, Point, Rectangle, Size};
use polars::prelude::*;
use std::sync::Arc;

pub struct HeatmapPlotKernel {
	pub prepared_data: Arc<HeatmapPreparedData>,
}

impl HeatmapPlotKernel {
	fn continuous_scale(&self, settings: &ResolvedPlotSettings) -> ContinuousColorScale {
		ContinuousColorScale::new(
			self.prepared_data.cells.domain(),
			self.prepared_data.color_intent,
			settings.continuous_ramps,
		)
		.expect("PRODUCT-0089 planning validated this heatmap continuous colour scale")
	}
}

impl PlotKernel for HeatmapPlotKernel {
	fn layout(&self, _settings: ResolvedPlotSettings) -> PlotLayout {
		PlotLayout::CategoricalXY {
			x_categories: self.prepared_data.cells.x_categories().to_vec(),
			y_categories: self.prepared_data.cells.y_categories().to_vec(),
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
		let num_x = self.prepared_data.cells.x_categories().len();
		let num_y = self.prepared_data.cells.y_categories().len();
		let scale = self.continuous_scale(&settings);
		for i in 0..num_x {
			for j in 0..num_y {
				let Some(value) = self.prepared_data.cells.value(i, j) else {
					continue;
				};
				let color = scale.color(value);
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
			let num_x = self.prepared_data.cells.x_categories().len();
			let num_y = self.prepared_data.cells.y_categories().len();
			for i in 0..num_x {
				for j in 0..num_y {
					let Some(value) = self.prepared_data.cells.value(i, j) else {
						continue;
					};
					let (center, bw, bh) = transform.categorical_2d(i, j);
					let rect = Rectangle {
						x: center.x - bw / 2.0,
						y: center.y - bh / 2.0,
						width: bw,
						height: bh,
					};
					if rect.contains(cursor_pos) {
						let x_cat = &self.prepared_data.cells.x_categories()[i];
						let y_cat = &self.prepared_data.cells.y_categories()[j];
						return Some(format!("X: {}\nY: {}\nValue: {:.2}", x_cat, y_cat, value));
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
		let scale = self.continuous_scale(settings);
		crate::plot::legend::gradient_section("Value".to_owned(), &scale).map(|section| {
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

#[derive(Debug)]
pub struct HeatmapPreparedData {
	pub cells: HeatmapCells,
	pub color_intent: ContinuousScaleIntent,
	pub x_label: String,
	pub y_label: String,
}

pub(crate) fn prepare_heatmap_data(
	df: &DataFrame,
	x_col: &str,
	y_col: &str,
	val_col: &str,
) -> Result<HeatmapPreparedData, HeatmapCellsError> {
	let x_values = df
		.column(x_col)
		.expect("resolved heatmap x field is present");
	let y_values = df
		.column(y_col)
		.expect("resolved heatmap y field is present");
	let values = df
		.column(val_col)
		.expect("resolved heatmap value field is present")
		.cast(&DataType::Float64)
		.expect("resolved heatmap value is quantitative");
	let values = values
		.f64()
		.expect("resolved heatmap value casts to Float64");
	let mut rows = Vec::with_capacity(df.height());
	for row in 0..df.height() {
		rows.push(HeatmapCellRow {
			x: category_key(x_values.get(row).expect("heatmap x row is in bounds")),
			y: category_key(y_values.get(row).expect("heatmap y row is in bounds")),
			value: values.get(row),
		});
	}
	Ok(HeatmapPreparedData {
		cells: HeatmapCells::from_rows(rows)?,
		color_intent: ContinuousScaleIntent::SEQUENTIAL,
		x_label: x_col.to_owned(),
		y_label: y_col.to_owned(),
	})
}

fn category_key(value: AnyValue<'_>) -> Option<String> {
	match value {
		AnyValue::Null => None,
		AnyValue::String(value) => Some(value.to_owned()),
		AnyValue::StringOwned(value) => Some(value.as_str().to_owned()),
		other => Some(other.to_string()),
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::plot::colors::ColorTheme;
	use crate::plot::common::{PathBuilder, PlotBackend};
	use crate::plot::legend::LegendSection;
	use iced::widget::canvas::{Stroke, Text};

	#[derive(Default)]
	/// PRODUCT-0057 named oracle `plot::kernels::heatmap::tests::RectangleBackend`
	/// records rectangle tuples only; its shared short name grants no other license.
	struct RectangleBackend {
		rectangles: Vec<(Point, Size, Color)>,
		strokes: usize,
	}

	impl PlotBackend for RectangleBackend {
		fn stroke_path(&mut self, _f: &dyn Fn(&mut dyn PathBuilder), _stroke: Stroke) {
			self.strokes += 1;
		}

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

	fn kernel(frame: &DataFrame) -> Result<HeatmapPlotKernel, HeatmapCellsError> {
		Ok(HeatmapPlotKernel {
			prepared_data: Arc::new(prepare_heatmap_data(frame, "x", "y", "value")?),
		})
	}

	fn render(kernel: &HeatmapPlotKernel, settings: ResolvedPlotSettings) -> RectangleBackend {
		let layout = kernel.layout(settings.clone());
		let bounds = Rectangle::new(Point::ORIGIN, Size::new(300.0, 200.0));
		let transform = CoordinateTransformer::new(&layout, bounds);
		let mut backend = RectangleBackend::default();
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
	fn product_0056_asymmetric_heatmap_rows_join_labels_cells_colors_and_hover() {
		let frame = polars::df!(
			"x" => ["c2", "c0", "c1", "c0", "c2", "c1"],
			"y" => ["row-b", "row-a", "row-b", "row-b", "row-a", "row-a"],
			"value" => [32_i64, 1, 16, 8, 4, 2],
		)
		.unwrap();
		let kernel = kernel(&frame).unwrap();
		assert_eq!(
			kernel.prepared_data.cells.x_categories(),
			["c0", "c1", "c2"]
		);
		assert_eq!(
			kernel.prepared_data.cells.y_categories(),
			["row-a", "row-b"]
		);
		assert_eq!(kernel.prepared_data.cells.domain(), (1.0, 32.0));

		let settings = ResolvedPlotSettings::default();
		let backend = render(&kernel, settings.clone());
		let layout = kernel.layout(settings.clone());
		let bounds = Rectangle::new(Point::ORIGIN, Size::new(300.0, 200.0));
		let transform = CoordinateTransformer::new(&layout, bounds);
		assert_eq!(backend.rectangles.len(), 6);
		assert_eq!(backend.strokes, 6);

		for x_index in 0..3 {
			for y_index in 0..2 {
				let index = x_index * 2 + y_index;
				let (top_left, size, color) = backend.rectangles[index];
				let (center, band_width, band_height) = transform.categorical_2d(x_index, y_index);
				assert_eq!(top_left, Point::new(center.x - 50.0, center.y - 50.0));
				assert_eq!(size, Size::new(band_width, band_height));
				let value = kernel.prepared_data.cells.value(x_index, y_index).unwrap();
				assert_eq!(
					color,
					settings
						.color_theme
						.get_color(((value - 1.0) / 31.0) as f32)
				);
				let x = &kernel.prepared_data.cells.x_categories()[x_index];
				let y = &kernel.prepared_data.cells.y_categories()[y_index];
				assert_eq!(
					kernel.hover(&transform, Cursor::Available(center)),
					Some(format!("X: {x}\nY: {y}\nValue: {value:.2}"))
				);
			}
		}
		assert!(backend.rectangles[0].0.y < backend.rectangles[1].0.y);
	}

	#[test]
	fn product_0088_duplicate_order_bank_now_has_one_typed_refusal() {
		// Stage 0 banked last-write outcomes 9.0 forward and 1.0 reversed.
		for values in [[1.0, 9.0], [9.0, 1.0], [1.0, 1.0]] {
			let frame = polars::df!(
				"x" => ["x", "x"],
				"y" => ["y", "y"],
				"value" => values,
			)
			.unwrap();
			let error = prepare_heatmap_data(&frame, "x", "y", "value").unwrap_err();
			assert!(matches!(
				error,
				HeatmapCellsError::DuplicateCell {
					first_row: 0,
					second_row: 1,
					..
				}
			));
			let message = error.to_string();
			assert!(message.contains("(`x`, `y`)"));
			assert!(message.contains("aggregate duplicate cells in the relation"));
		}
	}

	#[test]
	fn product_0088_null_keys_nonfinite_values_and_literal_null_are_distinct() {
		let null_x = polars::df!(
			"x" => [None, Some("null")],
			"y" => [Some("u"), Some("v")],
			"value" => [1.0, 2.0],
		)
		.unwrap();
		assert!(matches!(
			prepare_heatmap_data(&null_x, "x", "y", "value"),
			Err(HeatmapCellsError::NullX { row: 0 })
		));

		let literal = polars::df!("x" => ["null"], "y" => ["u"], "value" => [1.0]).unwrap();
		let literal = kernel(&literal).unwrap();
		assert_eq!(literal.prepared_data.cells.x_categories(), ["null"]);

		for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
			let frame = polars::df!("x" => ["a"], "y" => ["u"], "value" => [value]).unwrap();
			assert!(matches!(
				prepare_heatmap_data(&frame, "x", "y", "value"),
				Err(HeatmapCellsError::NonFiniteValue { row: 0, .. })
			));
		}
		let finite = polars::df!("x" => ["a"], "y" => ["u"], "value" => [1.0]).unwrap();
		assert!(kernel(&finite).is_ok());
	}

	#[test]
	fn product_0088_absence_survives_axes_paint_outline_hover_and_domain() {
		let frame = polars::df!(
			"x" => ["a", "b", "c"],
			"y" => ["u", "v", "u"],
			"value" => [None, Some(5.0), Some(7.0)],
		)
		.unwrap();
		let kernel = kernel(&frame).unwrap();
		assert_eq!(kernel.prepared_data.cells.x_categories(), ["a", "b", "c"]);
		assert_eq!(kernel.prepared_data.cells.y_categories(), ["u", "v"]);
		assert_eq!(kernel.prepared_data.cells.domain(), (5.0, 7.0));
		let settings = ResolvedPlotSettings::default();
		let backend = render(&kernel, settings.clone());
		let layout = kernel.layout(settings);
		let bounds = Rectangle::new(Point::ORIGIN, Size::new(300.0, 200.0));
		let transform = CoordinateTransformer::new(&layout, bounds);
		assert_eq!(backend.rectangles.len(), 2);
		assert_eq!(backend.strokes, 2);

		let (absent, _, _) = transform.categorical_2d(0, 0);
		let (observed, _, _) = transform.categorical_2d(1, 1);
		assert_eq!(kernel.hover(&transform, Cursor::Available(absent)), None);
		assert_eq!(
			kernel.hover(&transform, Cursor::Available(observed)),
			Some("X: b\nY: v\nValue: 5.00".to_owned())
		);
	}

	#[test]
	fn product_0088_signed_domains_use_both_ramp_ends() {
		for values in [[-50.0, 100.0], [-40.0, -10.0]] {
			let frame = polars::df!(
				"x" => ["a", "b"],
				"y" => ["u", "u"],
				"value" => values,
			)
			.unwrap();
			let kernel = kernel(&frame).unwrap();
			assert_eq!(kernel.prepared_data.cells.domain(), (values[0], values[1]));
			let settings = ResolvedPlotSettings::default();
			let backend = render(&kernel, settings.clone());
			assert_eq!(backend.rectangles[0].2, settings.color_theme.get_color(0.0));
			assert_eq!(backend.rectangles[1].2, settings.color_theme.get_color(1.0));
		}
	}

	#[test]
	fn product_0088_constant_and_no_data_share_only_gradient_omission() {
		let constant = polars::df!(
			"x" => ["a", "b"],
			"y" => ["u", "u"],
			"value" => [4.0, 4.0],
		)
		.unwrap();
		let constant = kernel(&constant).unwrap();
		let settings = ResolvedPlotSettings::default();
		let painted = render(&constant, settings.clone());
		assert_eq!(constant.prepared_data.cells.domain(), (4.0, 4.0));
		assert_eq!(painted.rectangles.len(), 2);
		assert!(
			painted
				.rectangles
				.iter()
				.all(|(_, _, color)| *color == settings.color_theme.get_color(0.0))
		);
		assert!(constant.legend_content(&settings).is_none());

		let empty = DataFrame::new(
			0,
			vec![
				Series::new("x".into(), Vec::<String>::new()).into(),
				Series::new("y".into(), Vec::<String>::new()).into(),
				Series::new("value".into(), Vec::<f64>::new()).into(),
			],
		)
		.unwrap();
		let empty = kernel(&empty).unwrap();
		assert_eq!(empty.prepared_data.cells.x_categories(), ["No Data"]);
		assert_eq!(empty.prepared_data.cells.y_categories(), ["No Data"]);
		assert_eq!(empty.prepared_data.cells.domain(), (0.0, 0.0));
		let unpainted = render(&empty, settings.clone());
		let layout = empty.layout(settings.clone());
		let bounds = Rectangle::new(Point::ORIGIN, Size::new(300.0, 200.0));
		let transform = CoordinateTransformer::new(&layout, bounds);
		assert!(unpainted.rectangles.is_empty());
		assert_eq!(unpainted.strokes, 0);
		let (center, _, _) = transform.categorical_2d(0, 0);
		assert_eq!(empty.hover(&transform, Cursor::Available(center)), None);
		assert!(empty.legend_content(&settings).is_none());
	}

	#[test]
	fn product_0089_heatmap_diverging_neutral_moves_from_twenty_five_to_zero() {
		let frame = polars::df!(
			"x" => ["a", "b", "c"],
			"y" => ["u", "u", "u"],
			"value" => [-50.0, 0.0, 100.0],
		)
		.unwrap();
		let mut kernel = kernel(&frame).unwrap();
		Arc::get_mut(&mut kernel.prepared_data)
			.expect("test owns the prepared heatmap")
			.color_intent = ContinuousScaleIntent {
			job: crate::plot::spec::ColorJob::Diverging,
			anchor: crate::plot::continuous_color::ContinuousAnchor::DefaultZero,
		};
		let mut cascade = crate::plot::theme::PlotSettings::default();
		cascade.application_theme.theme =
			crate::plot::spec::Override::Set(crate::plot::theme::PlotThemeSpec {
				sequential_ramp: ColorTheme::Magma,
				diverging_ramp: ColorTheme::BlueWhiteRed,
				..crate::plot::theme::PlotThemeSpec::default()
			});
		cascade.pane_override.theme =
			crate::plot::spec::Override::Set(crate::plot::theme::PlotThemeSpec {
				sequential_ramp: ColorTheme::Inferno,
				diverging_ramp: ColorTheme::GreenPurples,
				..crate::plot::theme::PlotThemeSpec::default()
			});
		let settings = cascade.resolve();
		assert_eq!(settings.color_theme, ColorTheme::Inferno);
		assert_eq!(
			settings.continuous_ramps.diverging,
			ColorTheme::GreenPurples
		);
		let backend = render(&kernel, settings.clone());
		assert_eq!(
			backend.rectangles[0].2,
			ColorTheme::GreenPurples.get_color(0.0)
		);
		assert_eq!(backend.rectangles[1].2, Color::WHITE);
		assert_eq!(
			backend.rectangles[2].2,
			ColorTheme::GreenPurples.get_color(1.0)
		);
		let guide = kernel
			.legend_content(&settings)
			.expect("non-degenerate guide");
		let LegendSection::Gradient { stops, .. } = guide.0.first() else {
			panic!("gradient section")
		};
		assert!(stops.values().any(|value| value == 0.0));
		assert_eq!(stops.sample_normalized(0.5), backend.rectangles[1].2);
	}
}
