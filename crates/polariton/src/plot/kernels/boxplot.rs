use crate::plot::common::{
	AxisType, CoordinateTransformer, PlotBackend, PlotKernel, PlotLayout, ResolvedPlotSettings,
	format_label,
};
use crate::plot::kernels::boxplot_tukey::{TukeyBox, TukeyBoxes};
use iced::advanced::mouse::Cursor;
use iced::widget::canvas::{Stroke, Style};
use iced::{Point, Rectangle};
use std::sync::Arc;

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
		let PlotLayout::CategoricalX { categories, .. } = transform.layout else {
			return None;
		};
		let mut nearest: Option<(f32, OutlierHit<'a>)> = None;
		for (category, stats) in self.prepared_data.boxes.iter() {
			let Some(shared_index) = categories
				.iter()
				.position(|candidate| candidate == category)
			else {
				continue;
			};
			for point in outlier_points(transform, shared_index, stats) {
				let dx = cursor.x - point.center.x;
				let dy = cursor.y - point.center.y;
				let distance = dx * dx + dy * dy;
				if distance > OUTLIER_RADIUS * OUTLIER_RADIUS {
					continue;
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
			}
		}
		nearest.map(|(_, hit)| hit)
	}
}

impl PlotKernel for BoxPlotKernel {
	fn layout(&self, settings: ResolvedPlotSettings) -> PlotLayout {
		PlotLayout::CategoricalX {
			categories: self.prepared_data.boxes.categories().to_vec(),
			y_axis_type: self.prepared_data.y_axis_type,
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
		let num_cats = self.prepared_data.boxes.categories().len();
		for (i, (category, stats)) in self.prepared_data.boxes.iter().enumerate() {
			let Some(shared_index) = transform.category_index(category) else {
				continue;
			};
			let (center, band_width) = transform.categorical(shared_index, 0.0);
			let box_width = band_width * 0.6;
			let left = center.x - box_width / 2.0;
			let right = center.x + box_width / 2.0;
			let t = if num_cats > 1 {
				i as f32 / (num_cats - 1) as f32
			} else {
				0.5
			};
			let color = settings.color_theme.get_color(t);
			let color_iced = color;
			let line_color = settings.mark_style.decoration.iced();
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
				style: Style::Solid(line_color),
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
				style: Style::Solid(line_color),
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
				style: Style::Solid(line_color),
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
				style: Style::Solid(line_color),
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
						style: Style::Solid(settings.mark_style.decoration.iced()),
						width: OUTLIER_OUTLINE_WIDTH,
						..Default::default()
					},
				);
			}
		}
	}

	fn hover(&self, transform: &CoordinateTransformer, cursor: Cursor) -> Option<String> {
		if let Some(cursor_pos) = cursor.position()
			&& let PlotLayout::CategoricalX { categories, .. } = transform.layout
		{
			if let Some(hit) = self.hit_outlier(transform, cursor_pos) {
				return Some(format!(
					"{}\nOutlier: {}",
					hit.category,
					format_label(hit.value, self.prepared_data.y_axis_type)
				));
			}
			for (category, stats) in self.prepared_data.boxes.iter() {
				let Some(shared_index) = categories
					.iter()
					.position(|candidate| candidate == category)
				else {
					continue;
				};
				let (center, band_width) = transform.categorical(shared_index, 0.0);
				let left = center.x - band_width / 2.0;
				let right = center.x + band_width / 2.0;
				if cursor_pos.x >= left && cursor_pos.x <= right {
					let low = transform.categorical(shared_index, stats.whisker_low()).0.y;
					let high = transform
						.categorical(shared_index, stats.whisker_high())
						.0
						.y;
					if cursor_pos.y >= low.min(high) && cursor_pos.y <= low.max(high) {
						let yt = self.prepared_data.y_axis_type;
						return Some(format!(
							"{}\nWhisker high: {}\nQ3: {}\nMedian: {}\nQ1: {}\nWhisker low: {}",
							category,
							format_label(stats.whisker_high(), yt),
							format_label(stats.q3(), yt),
							format_label(stats.median(), yt),
							format_label(stats.q1(), yt),
							format_label(stats.whisker_low(), yt)
						));
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
		crate::plot::legend::entries_from_labels(
			self.prepared_data.boxes.categories().iter().cloned(),
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

pub struct BoxPlotPreparedData {
	pub boxes: TukeyBoxes,
	pub y_range: (f64, f64),
	pub y_axis_type: AxisType,
	pub x_label: String,
	pub y_label: String,
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
		}
	}
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
	use crate::plot::chrome::PlotMarkStyle;
	use crate::plot::common::PathBuilder;
	use crate::plot::kernels::boxplot_tukey::TukeyBoxRow;
	use iced::{Color, Size, Vector};

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

		fn fill_text(&mut self, _text: iced::widget::canvas::Text) {}
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
}
