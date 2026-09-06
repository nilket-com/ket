use crate::plot::common::{
	AxisType, CartesianExtent, CoordinateTransformer, MarkHit, MarkRenderer, PlotBackend,
	PlotKernel, PlotLayout, ResolvedPlotSettings, TemporalAxisDomain, cartesian_column_extent,
	format_label,
};
use crate::plot::legend::LegendProvenance;
use crate::plot::spec::{CategoryColorAssignment, LayerSettings, ResolvedColor};
use crate::plot::theme::PlotSettings;
use iced::advanced::mouse::Cursor;
use iced::widget::canvas::{Stroke, Style};
use iced::{Color, Point, Rectangle};
use polars::prelude::*;
use std::sync::Arc;

pub struct LinePlotKernel {
	pub prepared_data: Arc<LinePreparedData>,
	layer_settings: Option<LayerSettings>,
}

pub struct LineMarkRenderer {
	pub prepared_data: Arc<LinePreparedData>,
}

impl LineMarkRenderer {
	pub fn new(prepared_data: Arc<LinePreparedData>) -> Self {
		Self { prepared_data }
	}
}

impl LinePlotKernel {
	fn structured_hit(
		&self,
		transform: &CoordinateTransformer,
		cursor: Cursor,
		settings: &ResolvedPlotSettings,
	) -> Option<crate::plot::common::PlotHit> {
		let hit = LineMarkRenderer::new(Arc::clone(&self.prepared_data))
			.hit_test(transform, cursor, settings)?;
		Some(
			crate::plot::common::PlotHit::new(
				crate::plot::common::MarkFamily::LINE,
				crate::plot::common::HitDimensionality::Line,
				hit.mark_index,
			)
			.labeled(
				"X",
				crate::plot::common::axis_hit_semantic_type(&self.prepared_data.x_axis_type),
				crate::plot::common::axis_hit_value(
					hit.data_x,
					&self.prepared_data.x_axis_type,
					format_label(hit.data_x, &self.prepared_data.x_axis_type),
				),
			)
			.labeled(
				"Y",
				crate::plot::common::axis_hit_semantic_type(&self.prepared_data.y_axis_type),
				crate::plot::common::axis_hit_value(
					hit.data_y,
					&self.prepared_data.y_axis_type,
					format_label(hit.data_y, &self.prepared_data.y_axis_type),
				),
			),
		)
	}

	pub fn new(prepared_data: Arc<LinePreparedData>) -> Self {
		Self {
			prepared_data,
			layer_settings: None,
		}
	}

	pub fn with_layer_settings(
		prepared_data: Arc<LinePreparedData>,
		layer_settings: LayerSettings,
	) -> Self {
		Self {
			prepared_data,
			layer_settings: Some(layer_settings),
		}
	}
}

impl MarkRenderer for LineMarkRenderer {
	fn draw(
		&self,
		backend: &mut dyn PlotBackend,
		_bounds: Rectangle,
		transform: &CoordinateTransformer,
		settings: ResolvedPlotSettings,
	) {
		for series in &self.prepared_data.series {
			let color = series_color(series, &settings);
			let stroke = Stroke {
				style: Style::Solid(color),
				width: settings.stroke_width.width_px(),
				..Default::default()
			};
			crate::plot::series::draw_series(
				backend,
				series.identity.as_ref(),
				&settings,
				&mut |backend| {
					backend.stroke_path(
						&|builder| {
							for (i, p) in series.points.iter().enumerate() {
								let pixel_p = transform.cartesian(p[0], p[1]);
								if i == 0 {
									builder.move_to(pixel_p);
								} else {
									builder.line_to(pixel_p);
								}
							}
						},
						stroke.clone(),
					)
				},
			);
		}
	}

	fn hit_test(
		&self,
		transform: &CoordinateTransformer,
		cursor: Cursor,
		settings: &ResolvedPlotSettings,
	) -> Option<MarkHit> {
		const INTERACTION_TOLERANCE_PX: f32 = 5.0;
		let hit_radius = settings.stroke_width.width_px() / 2.0 + INTERACTION_TOLERANCE_PX;
		let cursor = cursor.position()?;
		let mut nearest = None;
		let mut nearest_distance = hit_radius * hit_radius;
		let mut mark_index = 0;

		for series in &self.prepared_data.series {
			if !settings.hidden_series.is_visible(series.identity.as_ref()) {
				mark_index +=
					series.points.len().saturating_sub(1) + usize::from(series.points.len() == 1);
				continue;
			}
			if series.points.len() == 1 {
				let point = series.points[0];
				let pixel = transform.cartesian(point[0], point[1]);
				let distance = squared_distance(cursor, pixel);
				if distance <= nearest_distance {
					nearest_distance = distance;
					nearest = Some(MarkHit {
						data_x: point[0],
						data_y: point[1],
						mark_index,
					});
				}
			}

			for points in series.points.windows(2) {
				let start = transform.cartesian(points[0][0], points[0][1]);
				let end = transform.cartesian(points[1][0], points[1][1]);
				let (distance, t) = squared_distance_to_segment(cursor, start, end);
				if distance <= nearest_distance {
					nearest_distance = distance;
					nearest = Some(MarkHit {
						data_x: points[0][0] + (points[1][0] - points[0][0]) * t as f64,
						data_y: points[0][1] + (points[1][1] - points[0][1]) * t as f64,
						mark_index,
					});
				}
				mark_index += 1;
			}
			mark_index += usize::from(series.points.len() == 1);
		}
		nearest
	}
}

#[allow(dead_code)] // Geometric hit-testing is test-reachable until PRODUCT-0014.
fn squared_distance(first: Point, second: Point) -> f32 {
	let dx = first.x - second.x;
	let dy = first.y - second.y;
	dx * dx + dy * dy
}

#[allow(dead_code)] // Geometric hit-testing is test-reachable until PRODUCT-0014.
fn squared_distance_to_segment(point: Point, start: Point, end: Point) -> (f32, f32) {
	let dx = end.x - start.x;
	let dy = end.y - start.y;
	let length_squared = dx * dx + dy * dy;
	if length_squared <= f32::EPSILON {
		return (squared_distance(point, start), 0.0);
	}
	let t =
		(((point.x - start.x) * dx + (point.y - start.y) * dy) / length_squared).clamp(0.0, 1.0);
	let projection = Point::new(start.x + t * dx, start.y + t * dy);
	(squared_distance(point, projection), t)
}

impl PlotKernel for LinePlotKernel {
	fn series_identities(&self) -> Vec<crate::plot::series::SeriesIdentity> {
		self.prepared_data
			.series
			.iter()
			.filter_map(|series| series.identity.clone())
			.collect()
	}
	fn series_labels(&self) -> Vec<crate::plot::series::SeriesLabel> {
		self.prepared_data
			.series
			.iter()
			.filter_map(|series| {
				crate::plot::series::SeriesLabel::pair(series.identity.as_ref(), &series.name)
			})
			.collect()
	}
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
			x_axis_type: self.prepared_data.x_axis_type.clone(),
			y_axis_type: self.prepared_data.y_axis_type.clone(),
			x_temporal_domain: self.prepared_data.x_temporal_domain.clone(),
			y_temporal_domain: self.prepared_data.y_temporal_domain.clone(),
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
		LineMarkRenderer::new(Arc::clone(&self.prepared_data))
			.draw(backend, _bounds, transform, settings);
	}

	fn plot_with_settings(
		&self,
		backend: &mut dyn PlotBackend,
		bounds: Rectangle,
		transform: &CoordinateTransformer,
		_cursor: Cursor,
		settings: &PlotSettings,
	) {
		let resolved = self.layer_settings.as_ref().map_or_else(
			|| settings.resolve(),
			|layer| settings.resolve_for_layer(layer),
		);
		LineMarkRenderer::new(Arc::clone(&self.prepared_data))
			.draw(backend, bounds, transform, resolved);
	}

	fn hit(
		&self,
		transform: &CoordinateTransformer,
		cursor: Cursor,
	) -> Option<crate::plot::common::PlotHit> {
		let resolved = self.layer_settings.as_ref().map_or_else(
			|| PlotSettings::default().resolve(),
			|layer| PlotSettings::default().resolve_for_layer(layer),
		);
		self.structured_hit(transform, cursor, &resolved)
	}

	fn hit_with_settings(
		&self,
		transform: &CoordinateTransformer,
		cursor: Cursor,
		settings: &PlotSettings,
	) -> Option<crate::plot::common::PlotHit> {
		let resolved = self.layer_settings.as_ref().map_or_else(
			|| settings.resolve(),
			|layer| settings.resolve_for_layer(layer),
		);
		self.structured_hit(transform, cursor, &resolved)
	}

	fn legend_content(
		&self,
		settings: &ResolvedPlotSettings,
	) -> Option<crate::plot::legend::LegendContent> {
		let mut content = crate::plot::legend::entries_from_pairs_with_sample_and_provenance(
			self.prepared_data
				.series
				.iter()
				.map(|series| (series.name.clone(), series_color(series, settings))),
			crate::plot::legend::LegendSample::LineSample,
			self.prepared_data
				.series
				.first()
				.map_or(LegendProvenance::Named, |series| series.provenance),
		)?;
		crate::plot::legend::attach_identities(
			&mut content,
			self.prepared_data
				.series
				.iter()
				.map(|series| series.identity.clone()),
		);
		Some(content)
	}

	fn x_label(&self) -> String {
		self.prepared_data.x_label.clone()
	}

	fn y_label(&self) -> String {
		self.prepared_data.y_label.clone()
	}
}

#[allow(dead_code)]
pub struct SeriesData {
	pub identity: Option<crate::plot::series::SeriesIdentity>,
	pub name: String,
	pub points: Vec<[f64; 2]>,
	pub color_t: f32,
	pub resolved_color: Option<Color>,
	pub provenance: LegendProvenance,
}

pub struct LinePreparedData {
	pub series: Vec<SeriesData>,
	pub x_range: (f64, f64),
	pub y_range: (f64, f64),
	pub x_axis_type: AxisType,
	pub y_axis_type: AxisType,
	pub x_temporal_domain: Option<TemporalAxisDomain>,
	pub y_temporal_domain: Option<TemporalAxisDomain>,
	pub x_label: String,
	pub y_label: String,
}

pub fn prepare_line_data(
	df: &DataFrame,
	cat_col: &str,
	x_col: &str,
	y_col: &str,
) -> LinePreparedData {
	prepare_line_data_inner(df, cat_col, x_col, y_col, None)
}

pub fn prepare_line_data_with_colors(
	df: &DataFrame,
	cat_col: &str,
	x_col: &str,
	y_col: &str,
	colors: &[CategoryColorAssignment],
) -> LinePreparedData {
	prepare_line_data_inner(df, cat_col, x_col, y_col, Some(colors))
}

/// The layout a line mark resolves over `x_col` and `y_col`: the data extent
/// padded by five percent of its span on each side. Preparation and
/// authoring-time layout resolution share this one derivation.
pub fn line_column_extent(df: &DataFrame, x_col: &str, y_col: &str) -> CartesianExtent {
	cartesian_column_extent(df, x_col, y_col, |span| span * 0.05)
}

fn prepare_line_data_inner(
	df: &DataFrame,
	cat_col: &str,
	x_col: &str,
	y_col: &str,
	colors: Option<&[CategoryColorAssignment]>,
) -> LinePreparedData {
	let CartesianExtent {
		x_range,
		y_range,
		x_axis_type,
		y_axis_type,
		x_temporal_domain,
		y_temporal_domain,
	} = line_column_extent(df, x_col, y_col);
	if df.height() == 0 || x_col.is_empty() || y_col.is_empty() {
		return LinePreparedData {
			series: vec![],
			x_range,
			y_range,
			x_axis_type,
			y_axis_type,
			x_temporal_domain,
			y_temporal_domain,
			x_label: x_col.to_string(),
			y_label: y_col.to_string(),
		};
	}
	let mut series_list = Vec::new();
	let partitions = if cat_col.is_empty() {
		vec![df.clone()]
	} else {
		df.partition_by([cat_col], true)
			.unwrap_or_else(|_| vec![df.clone()])
	};
	let num_partitions = partitions.len();
	for (i, group_df) in partitions.into_iter().enumerate() {
		let cat_name = if cat_col.is_empty() {
			"All Data".to_string()
		} else {
			let cat_val = group_df
				.column(cat_col)
				.and_then(|c| c.get(0))
				.unwrap_or(AnyValue::Null);
			if let AnyValue::String(s) = cat_val {
				s.to_string()
			} else if cat_val.is_null() {
				"Null".to_string()
			} else {
				cat_val.to_string().replace("\"", "")
			}
		};
		let xs_col = match group_df.column(x_col) {
			Ok(c) => c.cast(&DataType::Float64).unwrap_or_else(|_| {
				Column::from(Series::new_empty(x_col.into(), &DataType::Float64))
			}),
			Err(_) => Column::from(Series::new_empty(x_col.into(), &DataType::Float64)),
		};
		let ys_col = match group_df.column(y_col) {
			Ok(c) => c.cast(&DataType::Float64).unwrap_or_else(|_| {
				Column::from(Series::new_empty(y_col.into(), &DataType::Float64))
			}),
			Err(_) => Column::from(Series::new_empty(y_col.into(), &DataType::Float64)),
		};
		let xs = xs_col.f64().unwrap();
		let ys = ys_col.f64().unwrap();
		let t = if num_partitions > 1 {
			i as f32 / (num_partitions - 1) as f32
		} else {
			0.5
		};
		let mut points = Vec::with_capacity(group_df.height());
		for j in 0..group_df.height() {
			points.push([xs.get(j).unwrap(), ys.get(j).unwrap()]);
		}
		let resolved_color = colors.map(|colors| {
			iced_color(
				colors
					.iter()
					.find(|assignment| assignment.category == cat_name)
					.unwrap_or_else(|| {
						panic!("resolved line series {cat_name:?} has no color assignment")
					})
					.color,
			)
		});
		series_list.push(SeriesData {
			identity: group_df.column(cat_col).ok().and_then(|column| {
				crate::plot::series::SeriesIdentity::from_column(
					column,
					0,
					crate::plot::spec::SemanticType::Nominal,
				)
			}),
			name: cat_name,
			points,
			color_t: t,
			resolved_color,
			provenance: if cat_col.is_empty() {
				LegendProvenance::Synthetic
			} else {
				LegendProvenance::Named
			},
		});
	}
	if !cat_col.is_empty() {
		let first_appearance = colors.map_or_else(
			|| {
				df.column(cat_col)
					.ok()
					.into_iter()
					.flat_map(|column| {
						(0..df.height()).filter_map(move |index| column.get(index).ok())
					})
					.map(|value| {
						if let AnyValue::String(value) = value {
							value.to_owned()
						} else if value.is_null() {
							"Null".to_owned()
						} else {
							value.to_string().replace('"', "")
						}
					})
					.fold(Vec::new(), |mut seen, label| {
						if !seen.contains(&label) {
							seen.push(label);
						}
						seen
					})
			},
			|colors| {
				colors
					.iter()
					.map(|assignment| assignment.category.clone())
					.collect()
			},
		);
		series_list.sort_by_key(|series| {
			first_appearance
				.iter()
				.position(|label| label == &series.name)
				.unwrap_or(usize::MAX)
		});
		let total = series_list.len();
		for (index, series) in series_list.iter_mut().enumerate() {
			series.color_t = if total > 1 {
				index as f32 / (total - 1) as f32
			} else {
				0.5
			};
		}
	}
	LinePreparedData {
		series: series_list,
		x_range,
		y_range,
		x_axis_type,
		y_axis_type,
		x_temporal_domain,
		y_temporal_domain,
		x_label: x_col.to_string(),
		y_label: y_col.to_string(),
	}
}

pub(crate) fn series_color(series: &SeriesData, settings: &ResolvedPlotSettings) -> Color {
	series
		.resolved_color
		.unwrap_or_else(|| settings.color_theme.get_color(series.color_t))
}

fn iced_color(color: ResolvedColor) -> Color {
	Color::from_rgba8(
		color.red,
		color.green,
		color.blue,
		color.alpha as f32 / 255.0,
	)
}

#[cfg(test)]
mod product_0054_tests {
	use super::*;
	use crate::plot::export::SvgBackend;
	use crate::plot::legend::{LegendSample, LegendSection, apply_policy};
	use crate::plot::spec::{Override, PalettePosition, StrokeWidth};
	use iced::Size;
	use polars::df;

	fn assignments(names: &[&str]) -> Vec<CategoryColorAssignment> {
		let colors = [
			ResolvedColor {
				red: 12,
				green: 34,
				blue: 56,
				alpha: 255,
			},
			ResolvedColor {
				red: 78,
				green: 90,
				blue: 123,
				alpha: 255,
			},
			ResolvedColor {
				red: 145,
				green: 167,
				blue: 189,
				alpha: 255,
			},
			ResolvedColor {
				red: 210,
				green: 220,
				blue: 230,
				alpha: 255,
			},
		];
		names
			.iter()
			.enumerate()
			.map(|(index, name)| CategoryColorAssignment {
				category: (*name).to_owned(),
				position: PalettePosition::new(index, names.len()).unwrap(),
				color: colors[index],
			})
			.collect()
	}

	#[test]
	fn interleaved_rows_partition_without_crossing_or_implicit_sorting() {
		let frame = df!(
			"series" => ["Atlas", "Beacon", "Cinder", "Delta", "Atlas", "Beacon", "Cinder", "Delta", "Atlas", "Beacon", "Cinder", "Delta"],
			"x" => [3.0, 30.0, 300.0, 3000.0, 1.0, 10.0, 100.0, 1000.0, 2.0, 20.0, 200.0, 2000.0],
			"y" => [3.5, 30.5, 300.5, 3000.5, 1.5, 10.5, 100.5, 1000.5, 2.5, 20.5, 200.5, 2000.5]
		)
		.expect("interleaved line frame");
		let prepared = prepare_line_data(&frame, "series", "x", "y");

		assert_eq!(
			prepared
				.series
				.iter()
				.map(|series| series.name.as_str())
				.collect::<Vec<_>>(),
			["Atlas", "Beacon", "Cinder", "Delta"]
		);
		assert_eq!(
			prepared.series[0].points,
			[[3.0, 3.5], [1.0, 1.5], [2.0, 2.5]]
		);
		assert_eq!(
			prepared.series[1].points,
			[[30.0, 30.5], [10.0, 10.5], [20.0, 20.5]]
		);
		assert_eq!(
			prepared.series[2].points,
			[[300.0, 300.5], [100.0, 100.5], [200.0, 200.5]]
		);
		assert_eq!(
			prepared.series[3].points,
			[[3000.0, 3000.5], [1000.0, 1000.5], [2000.0, 2000.5]]
		);
		assert!(
			prepared
				.series
				.iter()
				.all(|series| series.points.len() == 3)
		);
	}

	#[test]
	fn product_0156_line_visibility_preserves_geometry_and_hit_ordinals() {
		use crate::plot::series::{SeriesRegistry, SeriesVisibilityAction};
		let frame = df!("series" => ["a", "a", "b", "b"], "x" => [0.0, 1.0, 0.0, 1.0], "y" => [0.0, 0.0, 1.0, 1.0]).unwrap();
		let prepared = Arc::new(prepare_line_data(&frame, "series", "x", "y"));
		let kernel = LinePlotKernel::new(Arc::clone(&prepared));
		let renderer = LineMarkRenderer::new(Arc::clone(&prepared));
		let mut settings = ResolvedPlotSettings::default();
		settings.series_registry = SeriesRegistry::new(kernel.series_identities());
		let identities = settings.series_registry.identities().to_vec();
		let bounds = Rectangle::new(Point::ORIGIN, iced::Size::new(200.0, 100.0));
		let layout = kernel.layout(settings.clone());
		let transform = CoordinateTransformer::new(&layout, bounds);
		let render = |settings: &ResolvedPlotSettings| {
			let mut backend = crate::plot::export::SvgBackend::new(200.0, 100.0);
			renderer.draw(&mut backend, bounds, &transform, settings.clone());
			backend.finish()
		};
		let before = render(&settings);
		assert!(before.contains("data-series=\"s0\""));
		assert!(before.contains("data-series=\"s1\""));
		let cursor_a = Cursor::Available(transform.cartesian(0.5, 0.0));
		let cursor_b = Cursor::Available(transform.cartesian(0.5, 1.0));
		assert_eq!(
			renderer
				.hit_test(&transform, cursor_a, &settings)
				.unwrap()
				.mark_index,
			0
		);
		assert!(settings.hidden_series.apply(
			&SeriesVisibilityAction::Hide(identities[0].clone()),
			&identities
		));
		let hidden = render(&settings);
		assert!(!hidden.contains("data-series=\"s0\""));
		let surviving = before
			.split("<g data-series=\"s1\">")
			.nth(1)
			.unwrap()
			.split("</g>")
			.next()
			.unwrap();
		assert!(hidden.contains(surviving));
		assert!(renderer.hit_test(&transform, cursor_a, &settings).is_none());
		assert_eq!(
			renderer
				.hit_test(&transform, cursor_b, &settings)
				.unwrap()
				.mark_index,
			1
		);
		assert!(Arc::ptr_eq(&prepared, &kernel.prepared_data));
		settings
			.hidden_series
			.apply(&SeriesVisibilityAction::RestoreAll, &identities);
		assert_eq!(before, render(&settings));
	}

	#[test]
	fn paint_and_named_legend_consume_the_same_exact_resolved_assignments() {
		let frame = df!(
			"series" => ["Atlas", "Beacon", "Atlas", "Beacon"],
			"x" => [1.0, 1.0, 2.0, 2.0],
			"y" => [1.0, 2.0, 3.0, 4.0]
		)
		.expect("series line frame");
		let assignments = assignments(&["Beacon", "Atlas"]);
		let prepared = Arc::new(prepare_line_data_with_colors(
			&frame,
			"series",
			"x",
			"y",
			&assignments,
		));
		let kernel = LinePlotKernel::new(prepared);
		let settings = ResolvedPlotSettings::default();
		let bounds = Rectangle::new(Point::ORIGIN, Size::new(100.0, 100.0));
		let layout = kernel.layout(settings.clone());
		let transform = CoordinateTransformer::new(&layout, bounds);
		let mut backend = SvgBackend::new(100.0, 100.0);
		kernel.plot(
			&mut backend,
			bounds,
			&transform,
			Cursor::Unavailable,
			settings.clone(),
		);
		let svg = backend.finish();
		assert!(svg.contains("stroke=\"rgb(12, 34, 56)\""), "{svg}");
		assert!(svg.contains("stroke=\"rgb(78, 90, 123)\""), "{svg}");

		let content = kernel.legend_content(&settings).expect("named line legend");
		let LegendSection::Entries(entries) = content.0.first() else {
			panic!("series line must emit entry samples")
		};
		assert_eq!(entries.len(), 2);
		assert_eq!(entries.first().label, "Beacon");
		assert_eq!(entries.first().sample, LegendSample::LineSample);
		assert_eq!(entries.first().swatch, iced_color(assignments[0].color));
		assert_eq!(entries.last().label, "Atlas");
		assert_eq!(entries.last().swatch, iced_color(assignments[1].color));
	}

	#[test]
	#[should_panic(expected = "resolved line series \"Beacon\" has no color assignment")]
	fn authored_series_never_falls_back_when_a_resolved_assignment_is_missing() {
		let frame = df!(
			"series" => ["Atlas", "Beacon"],
			"x" => [1.0, 2.0],
			"y" => [2.0, 4.0]
		)
		.expect("inconsistent resolved fixture");
		let incomplete = assignments(&["Atlas"]);
		let _ = prepare_line_data_with_colors(&frame, "series", "x", "y", &incomplete);
	}

	#[test]
	fn named_single_series_keeps_one_entry_while_synthetic_control_is_omitted() {
		let frame = df!("series" => ["Atlas", "Atlas"], "x" => [1.0, 2.0], "y" => [2.0, 4.0])
			.expect("one-series frame");
		let settings = ResolvedPlotSettings::default();
		let named = LinePlotKernel::new(Arc::new(prepare_line_data(&frame, "series", "x", "y")));
		let named = apply_policy(named.legend_content(&settings).unwrap())
			.expect("a data-read identity earns a legend");
		let LegendSection::Entries(entries) = named.0.first() else {
			panic!("named series legend")
		};
		assert_eq!(entries.len(), 1);
		assert_eq!(entries.first().label, "Atlas");

		let synthetic = LinePlotKernel::new(Arc::new(prepare_line_data(&frame, "", "x", "y")));
		assert!(apply_policy(synthetic.legend_content(&settings).unwrap()).is_none());
	}

	#[test]
	fn native_series_modifier_controls_stroke_and_keeps_coordinate_only_hover() {
		let frame = df!(
			"series" => ["Atlas", "Atlas", "Beacon", "Beacon"],
			"x" => [1.0, 2.0, 1.0, 2.0],
			"y" => [1.0, 1.0, 2.0, 2.0]
		)
		.expect("styled series frame");
		let prepared = Arc::new(prepare_line_data(&frame, "series", "x", "y"));
		let layer = LayerSettings {
			stroke_width: Override::Set(StrokeWidth::new(2.0).unwrap()),
			..LayerSettings::default()
		};
		let kernel = LinePlotKernel::with_layer_settings(prepared, layer);
		let settings = PlotSettings::default();
		let bounds = Rectangle::new(Point::ORIGIN, Size::new(100.0, 100.0));
		let layout = kernel.layout(settings.resolve());
		let transform = CoordinateTransformer::new(&layout, bounds);
		let mut backend = SvgBackend::new(100.0, 100.0);
		kernel.plot_with_settings(
			&mut backend,
			bounds,
			&transform,
			Cursor::Unavailable,
			&settings,
		);
		let svg = backend.finish();
		assert_eq!(svg.matches("stroke-width=\"4\"").count(), 2, "{svg}");

		let on_atlas = transform.cartesian(1.5, 1.0);
		let hover = kernel
			.hover_with_settings(&transform, Cursor::Available(on_atlas), &settings)
			.expect("styled path hit");
		assert_eq!(hover, "X: 1.5, Y: 1");
		assert!(!hover.contains("Atlas"));
		assert!(!hover.contains("Beacon"));

		let two_field = LinePlotKernel::with_layer_settings(
			Arc::new(prepare_line_data(&frame, "", "x", "y")),
			LayerSettings {
				stroke_width: Override::Set(StrokeWidth::new(2.0).unwrap()),
				..LayerSettings::default()
			},
		);
		assert_eq!(
			two_field.hover_with_settings(&transform, Cursor::Available(on_atlas), &settings),
			Some(hover),
			"the new partition must not enter accepted hover prose"
		);
	}
}
