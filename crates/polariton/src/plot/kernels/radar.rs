use crate::plot::common::{
	AxisType, CoordinateTransformer, PathBuilder, PlotBackend, PlotKernel, PlotLayout,
	ResolvedPlotSettings, format_label,
};
use crate::plot::kernels::multivariate_rows::MultivariateRows;
use crate::plot::spec::CategoryColorAssignment;
use iced::advanced::mouse::Cursor;
use iced::widget::canvas::{Stroke, Style};
use iced::{Color, Point, Rectangle};
use std::sync::Arc;

pub struct RadarPlotKernel {
	pub prepared_data: Arc<MultivariateRows>,
	pub category_colors: Option<Vec<CategoryColorAssignment>>,
}

impl RadarPlotKernel {
	fn display_ranges(&self) -> Vec<(f64, f64)> {
		self.prepared_data
			.extrema()
			.map(|extrema| {
				extrema
					.iter()
					.map(|&(min, max)| {
						let difference = (max - min).max(1e-6);
						(min - difference * 0.1, max + difference * 0.1)
					})
					.collect()
			})
			.unwrap_or_else(|| vec![(0.0, 1.0); self.prepared_data.dimensions().len()])
	}

	fn category_color(&self, category: Option<&str>, settings: &ResolvedPlotSettings) -> Color {
		if let Some(assignments) = &self.category_colors {
			let category = category
				.expect("PRODUCT-0105 authored multivariate assignments have named categories");
			let assignment = assignments
				.iter()
				.find(|assignment| assignment.category == category)
				.expect("PRODUCT-0105 authored radar categories have complete assignments");
			return Color::from_rgba8(
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
			(index as f32 / (count - 1) as f32).clamp(0.1, 0.9)
		} else {
			0.5
		};
		settings.color_theme.get_color(position)
	}
}

impl PlotKernel for RadarPlotKernel {
	fn layout(&self, _settings: ResolvedPlotSettings) -> PlotLayout {
		PlotLayout::Radial
	}

	fn plot(
		&self,
		backend: &mut dyn PlotBackend,
		bounds: Rectangle,
		_transform: &CoordinateTransformer,
		_cursor: Cursor,
		settings: ResolvedPlotSettings,
	) {
		let num_dims = self.prepared_data.dimensions().len();
		if num_dims < 3 {
			return;
		}
		let center = Point::new(bounds.width / 2.0, bounds.height / 2.0);
		let max_radius = (bounds.width.min(bounds.height) / 2.0 - 80.0).max(10.0);
		let grid_stroke = Stroke {
			style: Style::Solid(Color::from_rgba(0.5, 0.5, 0.5, 0.2)),
			width: 1.0,
			..Default::default()
		};
		let num_circles = settings.x_tick_placement.count_value().unwrap_or(8);
		for i in 1..=num_circles {
			let radius = max_radius * (i as f32 / num_circles as f32);
			backend.stroke_path(
				&|builder| {
					for j in 0..=num_dims {
						let angle = (j as f32 / num_dims as f32) * 2.0 * std::f32::consts::PI
							- std::f32::consts::FRAC_PI_2;
						let p = Point::new(
							center.x + radius * angle.cos(),
							center.y + radius * angle.sin(),
						);
						if j == 0 {
							builder.move_to(p);
						} else {
							builder.line_to(p);
						}
					}
				},
				grid_stroke,
			);
		}
		let axis_stroke = Stroke {
			style: Style::Solid(Color::from_rgba(1.0, 1.0, 1.0, 0.4)),
			width: 1.5,
			..Default::default()
		};
		let display_ranges = self.display_ranges();
		for i in 0..num_dims {
			let angle = (i as f32 / num_dims as f32) * 2.0 * std::f32::consts::PI
				- std::f32::consts::FRAC_PI_2;
			let p = Point::new(
				center.x + max_radius * angle.cos(),
				center.y + max_radius * angle.sin(),
			);
			backend.stroke_path(
				&|builder| {
					builder.move_to(center);
					builder.line_to(p);
				},
				axis_stroke,
			);
			backend.fill_text(iced::widget::canvas::Text {
				content: self.prepared_data.dimensions()[i].clone(),
				position: Point::new(
					center.x + (max_radius + 15.0) * angle.cos(),
					center.y + (max_radius + 15.0) * angle.sin(),
				),
				color: settings.chrome.axis_label_text.iced(),
				size: iced::Pixels(settings.x_label_size.pixels()),
				align_x: if angle.cos().abs() < 0.01 {
					iced::alignment::Horizontal::Center.into()
				} else if angle.cos() > 0.0 {
					iced::alignment::Horizontal::Left.into()
				} else {
					iced::alignment::Horizontal::Right.into()
				},
				align_y: if angle.sin().abs() < 0.01 {
					iced::alignment::Vertical::Center
				} else if angle.sin() > 0.0 {
					iced::alignment::Vertical::Top
				} else {
					iced::alignment::Vertical::Bottom
				},
				..Default::default()
			});
			if i == 0 && num_circles > 0 {
				let range = display_ranges[i];
				for c in 1..=num_circles {
					let radius = max_radius * (c as f32 / num_circles as f32);
					let val = range.0 + (range.1 - range.0) * (c as f64 / num_circles as f64);
					let label_pos = Point::new(
						center.x + radius * angle.cos() + 10.0,
						center.y + radius * angle.sin() + 2.0,
					);
					backend.fill_text(iced::widget::canvas::Text {
						content: format_label(val, &AxisType::Linear),
						position: label_pos,
						color: Color {
							a: 0.7,
							..settings.chrome.tick_label_text.iced()
						},
						size: iced::Pixels(settings.x_tick_size.pixels()),
						align_x: iced::alignment::Horizontal::Left.into(),
						align_y: iced::alignment::Vertical::Center,
						..Default::default()
					});
				}
			}
		}
		for row in self.prepared_data.iter() {
			let color = self.category_color(row.category(), &settings);
			let mut fill_color = color;
			fill_color.a = 0.3;
			let path_closure = |builder: &mut dyn PathBuilder| {
				for (d, &val) in row.values().iter().enumerate() {
					let angle = (d as f32 / num_dims as f32) * 2.0 * std::f32::consts::PI
						- std::f32::consts::FRAC_PI_2;
					let range = display_ranges[d];
					let norm_val = ((val - range.0) / (range.1 - range.0).max(1e-6)) as f32;
					let radius = max_radius * (0.15 + norm_val * 0.8);
					let p = Point::new(
						center.x + radius * angle.cos(),
						center.y + radius * angle.sin(),
					);
					if d == 0 {
						builder.move_to(p);
					} else {
						builder.line_to(p);
					}
				}
				builder.close();
			};
			backend.fill_path(&path_closure, fill_color);
			backend.stroke_path(
				&path_closure,
				Stroke {
					style: Style::Solid(color),
					width: 2.0,
					..Default::default()
				},
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

	fn hit(
		&self,
		_transform: &CoordinateTransformer,
		_cursor: Cursor,
	) -> Option<crate::plot::common::PlotHit> {
		None
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::plot::common::TickPlacement;
	use crate::plot::export::SvgBackend;
	use crate::plot::kernels::multivariate_rows::{MultivariateRows, prepare_multivariate_rows};
	use crate::plot::spec::MultivariateFields;
	use iced::Size;
	use polars::df;

	#[test]
	fn product_0104_radar_valid_nonempty_render_matches_the_stage_zero_oracle() {
		let frame = df!(
			"group" => ["B", "A"],
			"third" => [100.0, 200.0],
			"first" => [1.0, 3.0],
			"second" => [10.0, 20.0],
		)
		.expect("radar oracle frame");
		let dimensions = vec!["third".to_owned(), "first".to_owned(), "second".to_owned()];
		let kernel = RadarPlotKernel {
			prepared_data: Arc::new(
				prepare_multivariate_rows(&frame, &dimensions, Some("group"))
					.expect("valid radar oracle"),
			),
			category_colors: None,
		};
		assert_eq!(
			kernel.display_ranges(),
			[(90.0, 210.0), (0.8, 3.2), (9.0, 21.0)]
		);
		let bounds = Rectangle::new(Point::ORIGIN, Size::new(300.0, 300.0));
		let layout = PlotLayout::Radial;
		let transform = CoordinateTransformer::new(&layout, bounds);
		let mut settings = ResolvedPlotSettings::default();
		settings.x_tick_placement = TickPlacement::count(1).expect("one radar interval");
		let mut backend = SvgBackend::new(300.0, 300.0);
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
				"<svg width=\"300\" height=\"300\" viewBox=\"0 0 300 300\" xmlns=\"http://www.w3.org/2000/svg\">\n",
				"<path d=\"M 150.00 80.00 L 210.62 185.00 L 89.38 185.00 L 150.00 80.00 \" stroke=\"rgb(127, 127, 127)\" stroke-width=\"1\" stroke-opacity=\"0.20\" fill=\"none\" />\n",
				"<path d=\"M 150.00 150.00 L 150.00 80.00 \" stroke=\"rgb(255, 255, 255)\" stroke-width=\"1.5\" stroke-opacity=\"0.40\" fill=\"none\" />\n",
				"<text transform=\"matrix(1.0000 0.0000 0.0000 1.0000 0.0000 0.0000)\" x=\"150\" y=\"65\" fill=\"rgb(255, 255, 255)\" font-size=\"20\" font-family=\"Polariton Plot Sans\" text-anchor=\"middle\" dominant-baseline=\"alphabetic\">third</text>\n",
				"<text transform=\"matrix(1.0000 0.0000 0.0000 1.0000 0.0000 0.0000)\" x=\"160\" y=\"82\" fill=\"rgb(255, 255, 255)\" font-size=\"18\" font-family=\"Polariton Plot Sans\" text-anchor=\"start\" dominant-baseline=\"central\">210</text>\n",
				"<path d=\"M 150.00 150.00 L 210.62 185.00 \" stroke=\"rgb(255, 255, 255)\" stroke-width=\"1.5\" stroke-opacity=\"0.40\" fill=\"none\" />\n",
				"<text transform=\"matrix(1.0000 0.0000 0.0000 1.0000 0.0000 0.0000)\" x=\"223.61215\" y=\"192.5\" fill=\"rgb(255, 255, 255)\" font-size=\"20\" font-family=\"Polariton Plot Sans\" text-anchor=\"start\" dominant-baseline=\"hanging\">first</text>\n",
				"<path d=\"M 150.00 150.00 L 89.38 185.00 \" stroke=\"rgb(255, 255, 255)\" stroke-width=\"1.5\" stroke-opacity=\"0.40\" fill=\"none\" />\n",
				"<text transform=\"matrix(1.0000 0.0000 0.0000 1.0000 0.0000 0.0000)\" x=\"76.38784\" y=\"192.5\" fill=\"rgb(255, 255, 255)\" font-size=\"20\" font-family=\"Polariton Plot Sans\" text-anchor=\"end\" dominant-baseline=\"hanging\">second</text>\n",
				"<path d=\"M 150.00 134.83 L 163.13 157.58 L 136.87 157.58 Z \" fill=\"rgb(189, 219, 61)\" fill-opacity=\"0.30\" />\n",
				"<path d=\"M 150.00 134.83 L 163.13 157.58 L 136.87 157.58 Z \" stroke=\"rgb(189, 219, 61)\" stroke-width=\"2\" stroke-opacity=\"1.00\" fill=\"none\" />\n",
				"<path d=\"M 150.00 88.17 L 203.55 180.92 L 96.45 180.92 Z \" fill=\"rgb(64, 33, 105)\" fill-opacity=\"0.30\" />\n",
				"<path d=\"M 150.00 88.17 L 203.55 180.92 L 96.45 180.92 Z \" stroke=\"rgb(64, 33, 105)\" stroke-width=\"2\" stroke-opacity=\"1.00\" fill=\"none\" />\n",
				"</svg>",
			)
		);
	}

	#[test]
	fn product_0106_radar_tick_ladder_follows_the_first_authored_dimension() {
		fn render(dimensions: [&str; 3]) -> String {
			let frame = df!(
				"third" => [100.0, 200.0],
				"first" => [1.0, 3.0],
				"second" => [10.0, 20.0],
			)
			.expect("radar tick fixture");
			let dimensions = dimensions.map(str::to_owned).to_vec();
			let kernel = RadarPlotKernel {
				prepared_data: Arc::new(
					prepare_multivariate_rows(&frame, &dimensions, None)
						.expect("valid radar tick fixture"),
				),
				category_colors: None,
			};
			let bounds = Rectangle::new(Point::ORIGIN, Size::new(300.0, 300.0));
			let layout = PlotLayout::Radial;
			let transform = CoordinateTransformer::new(&layout, bounds);
			let mut settings = ResolvedPlotSettings::default();
			settings.x_tick_placement = TickPlacement::count(1).expect("one radar interval");
			let mut backend = SvgBackend::new(300.0, 300.0);
			kernel.plot(
				&mut backend,
				bounds,
				&transform,
				Cursor::Unavailable,
				settings,
			);
			backend.finish()
		}

		let third_first = render(["third", "first", "second"]);
		assert!(third_first.contains(">210</text>"));
		assert!(!third_first.contains(">3.2</text>"));
		assert!(!third_first.contains(">21</text>"));

		let first_first = render(["first", "third", "second"]);
		assert!(first_first.contains(">3.2</text>"));
		assert!(!first_first.contains(">210</text>"));
		assert!(!first_first.contains(">21</text>"));
	}

	#[test]
	fn product_0104_radar_empty_keeps_padded_axis_substrate_without_series() {
		let fields =
			MultivariateFields::new(vec!["a".to_owned(), "b".to_owned(), "c".to_owned()], None);
		let kernel = RadarPlotKernel {
			prepared_data: Arc::new(
				MultivariateRows::from_rows(&fields, Vec::new()).expect("empty rows"),
			),
			category_colors: None,
		};
		assert_eq!(kernel.display_ranges(), [(0.0, 1.0); 3]);
		let bounds = Rectangle::new(Point::ORIGIN, Size::new(300.0, 300.0));
		let layout = PlotLayout::Radial;
		let transform = CoordinateTransformer::new(&layout, bounds);
		let mut settings = ResolvedPlotSettings::default();
		settings.x_tick_placement = TickPlacement::count(1).expect("one radar interval");
		let mut backend = SvgBackend::new(300.0, 300.0);
		kernel.plot(
			&mut backend,
			bounds,
			&transform,
			Cursor::Unavailable,
			settings,
		);
		assert_eq!(backend.finish().matches("<path ").count(), 4);
	}

	#[test]
	fn product_0105_radar_hover_remains_absent_in_a_non_origin_cell() {
		let fields =
			MultivariateFields::new(vec!["a".to_owned(), "b".to_owned(), "c".to_owned()], None);
		let kernel = RadarPlotKernel {
			prepared_data: Arc::new(
				MultivariateRows::from_rows(
					&fields,
					vec![
						crate::plot::kernels::multivariate_rows::MultivariateInputRow {
							category: None,
							values: vec![
							crate::plot::kernels::multivariate_rows::MultivariateValueCell::Exact(1.0),
							crate::plot::kernels::multivariate_rows::MultivariateValueCell::Exact(2.0),
							crate::plot::kernels::multivariate_rows::MultivariateValueCell::Exact(3.0),
						],
						},
					],
				)
				.expect("radar hover rows"),
			),
			category_colors: None,
		};
		let bounds = Rectangle::new(Point::new(240.0, 130.0), Size::new(200.0, 200.0));
		let transform = CoordinateTransformer::new(&PlotLayout::Radial, bounds);
		assert_eq!(
			kernel.hover(&transform, Cursor::Available(Point::new(340.0, 230.0))),
			None
		);
	}
}
