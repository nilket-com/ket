use crate::plot::common::{
	AxisType, CoordinateTransformer, PlotBackend, PlotKernel, PlotLayout, ResolvedPlotSettings,
	format_label,
};
use crate::plot::kernels::histogram_bins::HistogramBin;
use crate::plot::kernels::histogram_bins::HistogramBins;
use iced::Rectangle;
use iced::advanced::mouse::Cursor;
use polars::prelude::{AnyValue, DataFrame, DataType};
use std::sync::Arc;

pub struct HistogramPlotKernel {
	pub prepared_data: Arc<HistogramPreparedData>,
}

impl PlotKernel for HistogramPlotKernel {
	fn requires_plot_area_clip(&self) -> bool {
		true
	}

	fn layout(&self, settings: ResolvedPlotSettings) -> PlotLayout {
		let (x_range, y_range) = self.prepared_data.bins.ranges();
		PlotLayout::Cartesian {
			x_range: (
				settings.x_min.unwrap_or(x_range.0),
				settings.x_max.unwrap_or(x_range.1),
			),
			y_range: (
				settings.y_min.unwrap_or(y_range.0),
				settings.y_max.unwrap_or(y_range.1),
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
		if self.prepared_data.bins.max_positive_count().is_none() {
			return;
		}
		for bin in self.prepared_data.bins.iter().filter(|bin| bin.count > 0) {
			let top_left = transform.cartesian(bin.start, bin.count as f64);
			let bottom_right = transform.cartesian(bin.end, 0.0);
			let rect = Rectangle {
				x: top_left.x,
				y: top_left.y,
				width: (bottom_right.x - top_left.x).max(1.0),
				height: (bottom_right.y - top_left.y).max(1.0),
			};
			backend.fill_rectangle(
				rect.position(),
				rect.size(),
				settings.chrome.mark_fill.iced(),
			);
		}
	}

	fn legend_content(
		&self,
		_settings: &ResolvedPlotSettings,
	) -> Option<crate::plot::legend::LegendContent> {
		None
	}

	fn hover(&self, transform: &CoordinateTransformer, cursor: Cursor) -> Option<String> {
		let cursor_pos = cursor.position()?;
		let (x, y) = transform.pixel_to_cartesian(cursor_pos)?;
		let bin = self.prepared_data.bins.find(x)?;
		Some(format!(
			"Range: [{}, {})\nCount: {}\nY-Value: {:.2}",
			format_label(bin.start, self.prepared_data.x_axis_type),
			format_label(bin.end, self.prepared_data.x_axis_type),
			bin.count,
			y
		))
	}

	fn x_label(&self) -> String {
		self.prepared_data.x_label.clone()
	}

	fn y_label(&self) -> String {
		self.prepared_data.y_label.clone()
	}
}

#[derive(Debug)]
pub struct HistogramPreparedData {
	pub bins: HistogramBins,
	pub x_axis_type: AxisType,
	pub y_axis_type: AxisType,
	pub x_label: String,
	pub y_label: String,
}

/// PRODUCT-0079's single ingestion path from an already-aggregated relation.
/// It retains row indices for diagnostics and delegates interval invariants to
/// PRODUCT-0078's sole authority, `HistogramBins::new`.
pub fn prepare_histogram_data(
	df: &DataFrame,
	bin_start: &str,
	bin_end: &str,
	count: &str,
) -> Result<HistogramPreparedData, String> {
	let start_column = df
		.column(bin_start)
		.map_err(|error| format!("histogram bin_start `{bin_start}`: {error}"))?;
	let end_column = df
		.column(bin_end)
		.map_err(|error| format!("histogram bin_end `{bin_end}`: {error}"))?;
	let count_column = df
		.column(count)
		.map_err(|error| format!("histogram count `{count}`: {error}"))?;
	if !matches!(
		count_column.dtype(),
		DataType::Int8
			| DataType::Int16
			| DataType::Int32
			| DataType::Int64
			| DataType::UInt8
			| DataType::UInt16
			| DataType::UInt32
			| DataType::UInt64
	) {
		return Err(format!(
			"histogram count column `{count}` requires an integer dtype through u64, got {:?}",
			count_column.dtype()
		));
	}
	let starts = start_column
		.cast(&DataType::Float64)
		.map_err(|error| format!("histogram bin_start `{bin_start}`: {error}"))?;
	let ends = end_column
		.cast(&DataType::Float64)
		.map_err(|error| format!("histogram bin_end `{bin_end}`: {error}"))?;
	let starts = starts.f64().map_err(|error| error.to_string())?;
	let ends = ends.f64().map_err(|error| error.to_string())?;
	let mut bins = Vec::with_capacity(df.height());
	for row in 0..df.height() {
		let start = starts
			.get(row)
			.ok_or_else(|| format!("histogram bin_start `{bin_start}` is null at row {row}"))?;
		let end = ends
			.get(row)
			.ok_or_else(|| format!("histogram bin_end `{bin_end}` is null at row {row}"))?;
		let value = count_column
			.get(row)
			.map_err(|error| format!("histogram count `{count}` at row {row}: {error}"))?;
		let count = histogram_count(value, count, row)?;
		bins.push(HistogramBin { start, end, count });
	}
	let bins = HistogramBins::new(bins).map_err(|error| match error {
		crate::plot::kernels::histogram_bins::HistogramBinsError::Unordered { index } => format!(
			"{error}; sort the relation by `{bin_start}` ascending before histogram(...) (offending index {index})"
		),
		other => other.to_string(),
	})?;
	Ok(HistogramPreparedData {
		bins,
		x_axis_type: AxisType::Linear,
		y_axis_type: AxisType::Linear,
		x_label: bin_start.to_owned(),
		y_label: count.to_owned(),
	})
}

fn histogram_count(value: AnyValue<'_>, field: &str, row: usize) -> Result<u64, String> {
	let negative =
		|value: i128| format!("histogram count `{field}` is negative at row {row}: {value}");
	match value {
		AnyValue::Null => Err(format!("histogram count `{field}` is null at row {row}")),
		AnyValue::Int8(value) => u64::try_from(value).map_err(|_| negative(value.into())),
		AnyValue::Int16(value) => u64::try_from(value).map_err(|_| negative(value.into())),
		AnyValue::Int32(value) => u64::try_from(value).map_err(|_| negative(value.into())),
		AnyValue::Int64(value) => u64::try_from(value).map_err(|_| negative(value.into())),
		AnyValue::UInt8(value) => Ok(u64::from(value)),
		AnyValue::UInt16(value) => Ok(u64::from(value)),
		AnyValue::UInt32(value) => Ok(u64::from(value)),
		AnyValue::UInt64(value) => Ok(value),
		other => Err(format!(
			"histogram count `{field}` has unsupported value {other:?} at row {row}"
		)),
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::plot::common::{PathBuilder, PlotBackend};
	use crate::plot::kernels::histogram_bins::{HistogramBin, HistogramBins, HistogramBinsError};
	use iced::{Color, Point, Size, Vector};

	#[derive(Default)]
	struct RectangleBackend {
		rectangles: Vec<(Point, Size)>,
		colors: Vec<Color>,
		strokes: usize,
		clips: Vec<Rectangle>,
	}

	impl PlotBackend for RectangleBackend {
		fn stroke_path(
			&mut self,
			_f: &dyn Fn(&mut dyn PathBuilder),
			_stroke: iced::widget::canvas::Stroke,
		) {
			self.strokes += 1;
		}

		fn fill_path(&mut self, _f: &dyn Fn(&mut dyn PathBuilder), _color: Color) {}

		fn fill_rectangle(&mut self, top_left: Point, size: Size, color: Color) {
			self.rectangles.push((top_left, size));
			self.colors.push(color);
		}

		fn fill_text(&mut self, _text: iced::widget::canvas::Text) {}

		fn translate(&mut self, _translation: Vector) {}

		fn rotate(&mut self, _angle: f32) {}

		fn with_save(&mut self, f: &mut dyn FnMut(&mut dyn PlotBackend)) {
			f(self);
		}

		fn with_clip(&mut self, bounds: Rectangle, f: &mut dyn FnMut(&mut dyn PlotBackend)) {
			self.clips.push(bounds);
			f(self);
		}
	}

	fn bin(start: f64, end: f64, count: u64) -> HistogramBin {
		HistogramBin { start, end, count }
	}

	fn kernel(bins: Vec<HistogramBin>) -> HistogramPlotKernel {
		HistogramPlotKernel {
			prepared_data: Arc::new(HistogramPreparedData {
				bins: HistogramBins::new(bins).expect("valid histogram bins"),
				x_axis_type: AxisType::Linear,
				y_axis_type: AxisType::Linear,
				x_label: "value".to_owned(),
				y_label: "Frequency".to_owned(),
			}),
		}
	}

	fn render_rectangles(
		kernel: &HistogramPlotKernel,
		settings: ResolvedPlotSettings,
	) -> RectangleBackend {
		let bounds = Rectangle::new(Point::ORIGIN, Size::new(400.0, 200.0));
		let layout = kernel.layout(settings.clone());
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

	fn cursor_at(transform: &CoordinateTransformer<'_>, x: f64, y: f64) -> Cursor {
		Cursor::Available(transform.cartesian(x, y))
	}

	#[test]
	fn product_0078_explicit_unequal_intervals_drive_paint_and_preserve_gaps() {
		let kernel = kernel(vec![bin(0.0, 1.0, 4), bin(2.0, 4.0, 8), bin(4.0, 7.0, 12)]);
		let rectangles = render_rectangles(&kernel, ResolvedPlotSettings::default()).rectangles;
		assert_eq!(rectangles.len(), 3, "a gap must not synthesize a mark");
		let widths = rectangles
			.iter()
			.map(|(_, size)| size.width)
			.collect::<Vec<_>>();
		assert!((widths[1] / widths[0] - 2.0).abs() < 0.001);
		assert!((widths[2] / widths[0] - 3.0).abs() < 0.001);
	}

	#[test]
	fn product_0078_hover_obeys_gaps_edges_and_half_open_intervals() {
		let histogram = kernel(vec![bin(0.0, 1.0, 4), bin(2.0, 4.0, 8)]);
		let settings = ResolvedPlotSettings::default();
		let layout = histogram.layout(settings);
		let bounds = Rectangle::new(Point::ORIGIN, Size::new(400.0, 200.0));
		let transform = CoordinateTransformer::new(&layout, bounds);
		for x in [-0.01, 1.0, 1.5, 4.0, 4.01] {
			assert_eq!(
				histogram.hover(&transform, cursor_at(&transform, x, 1.0)),
				None
			);
		}
		assert!(
			histogram
				.hover(&transform, cursor_at(&transform, 0.999, 1.0))
				.expect("first bin")
				.contains("Count: 4")
		);
		assert!(
			histogram
				.hover(&transform, cursor_at(&transform, 2.0, 1.0))
				.expect("second bin")
				.contains("Count: 8")
		);

		let adjacent = kernel(vec![bin(0.0, 1.0, 4), bin(1.0, 2.0, 8)]);
		let layout = adjacent.layout(ResolvedPlotSettings::default());
		let transform = CoordinateTransformer::new(&layout, bounds);
		assert!(
			adjacent
				.hover(&transform, cursor_at(&transform, 1.0, 1.0))
				.expect("shared edge belongs to next half-open interval")
				.contains("Count: 8")
		);
	}

	#[test]
	fn product_0078_validation_names_six_distinct_invalid_shapes() {
		for (bins, expected) in [
			(
				vec![bin(2.0, 3.0, 1), bin(0.0, 1.0, 1)],
				HistogramBinsError::Unordered { index: 1 },
			),
			(
				vec![bin(0.0, 2.0, 1), bin(1.0, 3.0, 1)],
				HistogramBinsError::Overlapping { index: 1 },
			),
			(
				vec![bin(1.0, 1.0, 1)],
				HistogramBinsError::EmptyInterval { index: 0 },
			),
			(
				vec![bin(2.0, 1.0, 1)],
				HistogramBinsError::ReversedInterval { index: 0 },
			),
			(
				vec![bin(f64::NAN, 1.0, 1)],
				HistogramBinsError::NonFiniteStart { index: 0 },
			),
			(
				vec![bin(0.0, f64::INFINITY, 1)],
				HistogramBinsError::NonFiniteEnd { index: 0 },
			),
		] {
			assert_eq!(HistogramBins::new(bins), Err(expected));
		}
	}

	#[test]
	fn product_0078_domain_is_derived_and_counts_keep_database_width() {
		let small = HistogramBins::new(vec![bin(1.0, 2.0, 3)]).unwrap();
		let large_count = u64::from(u32::MAX) + 17;
		let large = HistogramBins::new(vec![bin(-4.0, 9.0, large_count)]).unwrap();
		assert_eq!(small.ranges(), ((1.0, 2.0), (0.0, 3.0)));
		assert_eq!(large.ranges(), ((-4.0, 9.0), (0.0, large_count as f64)));
		assert_eq!(large.find(0.0).unwrap().count, large_count);

		let histogram = kernel(vec![bin(-4.0, 9.0, large_count)]);
		let layout = histogram.layout(ResolvedPlotSettings::default());
		let transform = CoordinateTransformer::new(
			&layout,
			Rectangle::new(Point::ORIGIN, Size::new(400.0, 200.0)),
		);
		assert!(
			histogram
				.hover(&transform, cursor_at(&transform, 0.0, 1.0))
				.expect("large-count interval hover")
				.contains(&format!("Count: {large_count}"))
		);
	}

	#[test]
	fn product_0078_true_zero_geometry_is_distinct_from_realized_clipping() {
		let kernel = kernel(vec![bin(0.0, 1.0, 40), bin(1.0, 2.0, 80)]);
		let settings = ResolvedPlotSettings {
			y_min: Some(20.0),
			y_max: Some(100.0),
			..ResolvedPlotSettings::default()
		};
		let rendered = render_rectangles(&kernel, settings);
		let rectangles = rendered.rectangles;
		assert_eq!(rectangles.len(), 2);
		assert!((rectangles[1].1.height / rectangles[0].1.height - 2.0).abs() < 0.001);
		assert_eq!(
			rendered.clips,
			Vec::new(),
			"the common plot stratum owns clipping so the kernel cannot create a nested child beneath the background"
		);
		assert!(kernel.requires_plot_area_clip());
		let visible = rectangles
			.iter()
			.map(|(point, size)| (point.y + size.height).min(200.0) - point.y.max(0.0))
			.collect::<Vec<_>>();
		assert!((visible[1] / visible[0] - 3.0).abs() < 0.001);
	}

	#[test]
	fn product_0078_empty_and_zero_sequences_have_pinned_domains_and_no_legend() {
		let empty = kernel(vec![]);
		let zero = kernel(vec![bin(4.0, 6.0, 0), bin(8.0, 9.0, 0)]);
		for (kernel, expected_x) in [(&empty, (0.0, 1.0)), (&zero, (4.0, 9.0))] {
			let settings = ResolvedPlotSettings::default();
			assert_eq!(
				render_rectangles(&kernel, settings.clone()).rectangles,
				Vec::new()
			);
			assert_eq!(kernel.legend_content(&settings), None);
			assert!(matches!(
				kernel.layout(ResolvedPlotSettings::default()),
				PlotLayout::Cartesian { x_range, y_range, .. }
					if x_range == expected_x && y_range == (0.0, 1.0)
			));
		}

		let bounds = Rectangle::new(Point::ORIGIN, Size::new(400.0, 200.0));
		let empty_layout = empty.layout(ResolvedPlotSettings::default());
		let empty_transform = CoordinateTransformer::new(&empty_layout, bounds);
		assert_eq!(
			empty.hover(&empty_transform, cursor_at(&empty_transform, 0.5, 0.5)),
			None
		);
		let zero_layout = zero.layout(ResolvedPlotSettings::default());
		let zero_transform = CoordinateTransformer::new(&zero_layout, bounds);
		assert!(
			zero.hover(&zero_transform, cursor_at(&zero_transform, 5.0, 0.5))
				.expect("reported zero-count interval remains hoverable")
				.contains("Count: 0")
		);
	}

	#[test]
	fn product_0079_count_is_height_only_and_mark_fill_is_exact() {
		let histogram = kernel(vec![bin(0.0, 1.0, 4), bin(1.0, 2.0, 11)]);
		let mut settings = ResolvedPlotSettings::default();
		settings.chrome.mark_fill = crate::plot::chrome::PlotRgba::opaque(0.2, 0.4, 0.8);
		let rendered = render_rectangles(&histogram, settings.clone());
		assert_eq!(rendered.colors, vec![settings.chrome.mark_fill.iced(); 2]);
		assert_ne!(
			rendered.rectangles[0].1.height,
			rendered.rectangles[1].1.height
		);
		assert_eq!(
			rendered.strokes, 0,
			"PRODUCT-0079 ships no histogram outline"
		);
		assert_eq!(histogram.legend_content(&settings), None);

		let fills = ["nord", "viridis_r"].map(|name| {
			let mut settings = ResolvedPlotSettings::default();
			settings.chrome = crate::plot::chrome::PlotChrome::from_theme_name(
				name,
				crate::plot::chrome::PlotChromeProjection::Ordered,
			)
			.unwrap();
			render_rectangles(&histogram, settings).colors[0]
		});
		assert_ne!(
			fills[0], fills[1],
			"the emitted histogram fill must follow the resolved skin"
		);
	}

	#[test]
	fn product_0079_ingestion_refuses_nulls_and_negatives_and_keeps_integer_extremes() {
		use polars::prelude::{Column, DataFrame};
		let frame = |starts: Column, ends: Column, counts: Column| {
			DataFrame::new(starts.len(), vec![starts, ends, counts]).expect("frame")
		};
		let negative = frame(
			Column::new("start".into(), [0.0]),
			Column::new("end".into(), [1.0]),
			Column::new("count".into(), [-1_i64]),
		);
		let error = prepare_histogram_data(&negative, "start", "end", "count").unwrap_err();
		assert!(error.contains("negative") && error.contains("row 0") && error.contains("-1"));

		for (starts, ends, counts, field) in [
			(
				Column::new("start".into(), [None::<f64>]),
				Column::new("end".into(), [Some(1.0)]),
				Column::new("count".into(), [Some(1_u64)]),
				"start",
			),
			(
				Column::new("start".into(), [Some(0.0)]),
				Column::new("end".into(), [None::<f64>]),
				Column::new("count".into(), [Some(1_u64)]),
				"end",
			),
			(
				Column::new("start".into(), [Some(0.0)]),
				Column::new("end".into(), [Some(1.0)]),
				Column::new("count".into(), [None::<u64>]),
				"count",
			),
		] {
			let error =
				prepare_histogram_data(&frame(starts, ends, counts), "start", "end", "count")
					.unwrap_err();
			assert!(error.contains(field) && error.contains("null") && error.contains("row 0"));
		}

		let signed = frame(
			Column::new("start".into(), [0.0]),
			Column::new("end".into(), [1.0]),
			Column::new("count".into(), [i64::MAX]),
		);
		assert_eq!(
			prepare_histogram_data(&signed, "start", "end", "count")
				.unwrap()
				.bins
				.iter()
				.next()
				.unwrap()
				.count,
			i64::MAX as u64
		);
		let unsigned = frame(
			Column::new("start".into(), [0.0]),
			Column::new("end".into(), [1.0]),
			Column::new("count".into(), [u64::MAX]),
		);
		assert_eq!(
			prepare_histogram_data(&unsigned, "start", "end", "count")
				.unwrap()
				.bins
				.iter()
				.next()
				.unwrap()
				.count,
			u64::MAX
		);
	}
}
