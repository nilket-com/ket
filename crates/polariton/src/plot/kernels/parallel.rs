use crate::plot::common::{
	CoordinateTransformer, PlotBackend, PlotKernel, PlotLayout, ResolvedPlotSettings,
};
use crate::plot::kernels::multivariate_rows::MultivariateRows;
use crate::plot::spec::CategoryColorAssignment;
use iced::Rectangle;
use iced::advanced::mouse::Cursor;
use iced::widget::canvas::{Stroke, Style};
use std::sync::Arc;

pub struct ParallelPlotKernel {
	pub prepared_data: Arc<MultivariateRows>,
	/// Present only for grouped authored figures; ungrouped and automatic
	/// figures deliberately retain the positional compatibility path.
	pub category_colors: Option<Vec<CategoryColorAssignment>>,
}

impl ParallelPlotKernel {
	fn display_ranges(&self) -> Vec<(f64, f64)> {
		self.prepared_data
			.extrema()
			.map(<[(f64, f64)]>::to_vec)
			.unwrap_or_else(|| vec![(0.0, 1.0); self.prepared_data.dimensions().len()])
	}

	fn category_color(
		&self,
		category: Option<&str>,
		settings: &ResolvedPlotSettings,
	) -> iced::Color {
		if let Some(assignments) = &self.category_colors {
			let category = category
				.expect("PRODUCT-0105 authored multivariate assignments have named categories");
			let assignment = assignments
				.iter()
				.find(|assignment| assignment.category == category)
				.expect("PRODUCT-0105 authored parallel categories have complete assignments");
			return iced::Color::from_rgba8(
				assignment.color.red,
				assignment.color.green,
				assignment.color.blue,
				assignment.color.alpha as f32 / u8::MAX as f32,
			);
		}
		let count = self.prepared_data.categories().len();
		let index = self
			.prepared_data
			.category_position(category)
			.expect("PRODUCT-0104 validated every row category against its catalog");
		let position = if count > 1 {
			index as f32 / (count - 1) as f32
		} else {
			0.5
		};
		settings.color_theme.get_color(position)
	}
}

impl PlotKernel for ParallelPlotKernel {
	fn layout(&self, _settings: ResolvedPlotSettings) -> PlotLayout {
		PlotLayout::Parallel {
			dimensions: self.prepared_data.dimensions().to_vec(),
			ranges: self.display_ranges(),
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
		let num_dims = self.prepared_data.dimensions().len();
		if num_dims < 2 {
			return;
		}
		for row in self.prepared_data.iter() {
			let color = self.category_color(row.category(), &settings);
			let stroke = Stroke {
				style: Style::Solid(color),
				width: 1.5,
				..Default::default()
			};
			backend.stroke_path(
				&|builder| {
					for (d, &val) in row.values().iter().enumerate() {
						let (p, _) = transform.categorical(d, val);
						if d == 0 {
							builder.move_to(p);
						} else {
							builder.line_to(p);
						}
					}
				},
				stroke,
			);
		}
	}

	fn legend_content(
		&self,
		settings: &ResolvedPlotSettings,
	) -> Option<crate::plot::legend::LegendContent> {
		crate::plot::legend::entries_from_pairs_with_sample_and_provenance(
			self.prepared_data.categories().iter().map(|category| {
				(
					category.as_deref().unwrap_or("All Data").to_owned(),
					self.category_color(category.as_deref(), settings),
				)
			}),
			crate::plot::legend::LegendSample::Swatch,
			crate::plot::legend::LegendProvenance::Named,
		)
	}

	fn hover(&self, transform: &CoordinateTransformer, cursor: Cursor) -> Option<String> {
		if let Some(cursor_pos) = cursor.position() {
			let num_dims = self.prepared_data.dimensions().len();
			if num_dims < 2 {
				return None;
			}
			let spacing = transform.bounds.width / (num_dims - 1) as f32;
			let axis_idx =
				((cursor_pos.x - transform.bounds.x + spacing / 2.0) / spacing).floor() as i32;
			if axis_idx >= 0 && (axis_idx as usize) < num_dims {
				let i = axis_idx as usize;
				let range = self.display_ranges()[i];
				let y_scale = (range.1 - range.0) / transform.bounds.height as f64;
				let val = range.1 - (cursor_pos.y - transform.bounds.y) as f64 * y_scale;
				return Some(format!(
					"{}: {:.2}",
					self.prepared_data.dimensions()[i],
					val
				));
			}
		}
		None
	}

	fn x_label(&self) -> String {
		"Dimension".to_string()
	}

	fn y_label(&self) -> String {
		"Value".to_string()
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::plot::export::SvgBackend;
	use crate::plot::kernels::multivariate_rows::prepare_multivariate_rows;
	use iced::{Point, Size};
	use polars::df;

	#[test]
	fn product_0104_parallel_valid_nonempty_render_matches_the_stage_zero_oracle() {
		let frame = df!(
			"group" => ["B", "A", "B"],
			"second" => [10.0, 20.0, 15.0],
			"first" => [1.0, 3.0, 2.0],
		)
		.expect("parallel oracle frame");
		let dimensions = vec!["second".to_owned(), "first".to_owned()];
		let kernel = ParallelPlotKernel {
			prepared_data: Arc::new(
				prepare_multivariate_rows(&frame, &dimensions, Some("group"))
					.expect("valid parallel oracle"),
			),
			category_colors: None,
		};
		let settings = ResolvedPlotSettings::default();
		let layout = kernel.layout(settings.clone());
		let PlotLayout::Parallel {
			dimensions: actual_dimensions,
			ranges,
		} = &layout
		else {
			panic!("parallel kernel must retain parallel layout")
		};
		assert_eq!(actual_dimensions, &dimensions);
		assert_eq!(ranges, &[(10.0, 20.0), (1.0, 3.0)]);
		let bounds = Rectangle::new(Point::ORIGIN, Size::new(200.0, 100.0));
		let transform = CoordinateTransformer::new(&layout, bounds);
		let mut backend = SvgBackend::new(200.0, 100.0);
		kernel.plot(
			&mut backend,
			bounds,
			&transform,
			Cursor::Unavailable,
			settings,
		);
		let svg = backend.finish();
		assert_eq!(
			svg,
			concat!(
				"<svg width=\"200\" height=\"100\" viewBox=\"0 0 200 100\" xmlns=\"http://www.w3.org/2000/svg\">\n",
				"<path d=\"M 0.00 100.00 L 200.00 100.00 \" stroke=\"rgb(253, 231, 36)\" stroke-width=\"1.5\" stroke-opacity=\"1.00\" fill=\"none\" />\n",
				"<path d=\"M 0.00 0.00 L 200.00 0.00 \" stroke=\"rgb(68, 1, 83)\" stroke-width=\"1.5\" stroke-opacity=\"1.00\" fill=\"none\" />\n",
				"<path d=\"M 0.00 50.00 L 200.00 50.00 \" stroke=\"rgb(253, 231, 36)\" stroke-width=\"1.5\" stroke-opacity=\"1.00\" fill=\"none\" />\n",
				"</svg>",
			)
		);
	}

	#[test]
	fn product_0104_parallel_empty_keeps_axis_ranges_without_series() {
		let fields = crate::plot::spec::MultivariateFields::new(
			vec!["left".to_owned(), "right".to_owned()],
			None,
		);
		let kernel = ParallelPlotKernel {
			prepared_data: Arc::new(
				MultivariateRows::from_rows(&fields, Vec::new()).expect("empty rows"),
			),
			category_colors: None,
		};
		let layout = kernel.layout(ResolvedPlotSettings::default());
		let PlotLayout::Parallel { dimensions, ranges } = layout else {
			panic!("parallel kernel must retain parallel layout")
		};
		assert_eq!(dimensions, ["left", "right"]);
		assert_eq!(ranges, [(0.0, 1.0), (0.0, 1.0)]);
	}

	#[test]
	fn product_0104_parallel_renders_every_row_in_a_repeated_category() {
		let frame = df!(
			"group" => ["A", "A", "A"],
			"left" => [1.0, 2.0, 3.0],
			"right" => [3.0, 2.0, 1.0],
		)
		.expect("repeated-category frame");
		let dimensions = vec!["left".to_owned(), "right".to_owned()];
		let kernel = ParallelPlotKernel {
			prepared_data: Arc::new(
				prepare_multivariate_rows(&frame, &dimensions, Some("group"))
					.expect("all repeated-category observations remain valid"),
			),
			category_colors: None,
		};
		let settings = ResolvedPlotSettings::default();
		let layout = kernel.layout(settings.clone());
		let bounds = Rectangle::new(Point::ORIGIN, Size::new(200.0, 100.0));
		let transform = CoordinateTransformer::new(&layout, bounds);
		let mut backend = SvgBackend::new(200.0, 100.0);
		kernel.plot(
			&mut backend,
			bounds,
			&transform,
			Cursor::Unavailable,
			settings,
		);
		assert_eq!(backend.finish().matches("<path ").count(), 3);
	}

	#[test]
	fn product_0105_parallel_hover_uses_non_origin_widget_bounds() {
		let fields = crate::plot::spec::MultivariateFields::new(
			vec!["left".to_owned(), "right".to_owned()],
			None,
		);
		let kernel = ParallelPlotKernel {
			prepared_data: Arc::new(
				MultivariateRows::from_rows(
					&fields,
					vec![
						crate::plot::kernels::multivariate_rows::MultivariateInputRow {
							category: None,
							values: vec![
							crate::plot::kernels::multivariate_rows::MultivariateValueCell::Exact(0.0),
							crate::plot::kernels::multivariate_rows::MultivariateValueCell::Exact(100.0),
						],
						},
					],
				)
				.expect("parallel hover rows"),
			),
			category_colors: None,
		};
		let layout = kernel.layout(ResolvedPlotSettings::default());
		let bounds = Rectangle::new(Point::new(240.0, 130.0), Size::new(200.0, 100.0));
		let transform = CoordinateTransformer::new(&layout, bounds);
		assert_eq!(
			kernel.hover(&transform, Cursor::Available(Point::new(440.0, 130.0))),
			Some("right: 100.00".to_owned())
		);
	}
}
