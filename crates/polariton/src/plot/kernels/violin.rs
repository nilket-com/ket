use crate::plot::common::{
	AxisType, CoordinateTransformer, PathBuilder, PlotBackend, PlotKernel, PlotLayout,
	ResolvedPlotSettings,
};
use crate::plot::exact_numeric::{ExactF64Error, exact_f64};
use crate::plot::kernels::violin_density::{
	ViolinDensity, ViolinDensityCell, ViolinDensityError, ViolinDensityInputRow,
	ViolinWidthNormalization,
};
use iced::advanced::mouse::Cursor;
use iced::widget::canvas::{Stroke, Style};
use iced::{Color, Rectangle};
use polars::prelude::{AnyValue, DataFrame};
use std::sync::Arc;

/// The final rendering projection of PRODUCT-0107's validated density
/// relation. Statistics are deliberately absent: Toron produces them before
/// this kernel sees a row (PRODUCT-0108 §7).
pub struct ViolinPlotKernel {
	pub prepared_data: Arc<ViolinDensity>,
	pub x_label: String,
	pub y_label: String,
}

impl PlotKernel for ViolinPlotKernel {
	fn layout(&self, settings: ResolvedPlotSettings) -> PlotLayout {
		let grid = self.prepared_data.grid();
		let y_range = match (grid.first(), grid.last()) {
			(Some(minimum), Some(maximum)) => (*minimum, *maximum),
			_ => (0.0, 1.0),
		};
		PlotLayout::CategoricalX {
			categories: self
				.prepared_data
				.curves()
				.map(|curve| curve.label().to_owned())
				.collect(),
			y_axis_type: AxisType::Linear,
			y_range: (
				settings.y_min.unwrap_or(y_range.0),
				settings.y_max.unwrap_or(y_range.1),
			),
		}
	}

	fn plot(
		&self,
		backend: &mut dyn PlotBackend,
		_bounds: Rectangle,
		transform: &CoordinateTransformer,
		cursor: Cursor,
		settings: ResolvedPlotSettings,
	) {
		if self.prepared_data.peak_density() <= 0.0 {
			return;
		}
		let widths = self
			.prepared_data
			.normalized_widths(ViolinWidthNormalization::Figure);
		let curves = self.prepared_data.curves().collect::<Vec<_>>();
		let grid = self.prepared_data.grid();
		for (index, curve) in curves.iter().enumerate() {
			let Some(shared_index) = transform.category_index(curve.label()) else {
				continue;
			};
			let Some(widths) = widths.get(index) else {
				continue;
			};
			let Some((first, last)) = visible_extent(widths) else {
				continue;
			};
			let (_, band_width) = transform.categorical(shared_index, 0.0);
			let width_scale = band_width * 0.4;
			let t = if curves.len() > 1 {
				index as f32 / (curves.len() - 1) as f32
			} else {
				0.5
			};
			let color = settings.color_theme.get_color(t);
			let violin_path = |builder: &mut dyn PathBuilder| {
				for point in first..=last {
					let (position, _) = transform.categorical(shared_index, grid[point]);
					let x = position.x - widths[point] as f32 * width_scale;
					if point == first {
						builder.move_to(iced::Point::new(x, position.y));
					} else {
						builder.line_to(iced::Point::new(x, position.y));
					}
				}
				for point in (first..=last).rev() {
					let (position, _) = transform.categorical(shared_index, grid[point]);
					builder.line_to(iced::Point::new(
						position.x + widths[point] as f32 * width_scale,
						position.y,
					));
				}
				builder.close();
			};
			backend.fill_path(&violin_path, color);
			backend.stroke_path(
				&violin_path,
				Stroke {
					style: Style::Solid(settings.mark_style.decoration.iced()),
					width: 2.5,
					..Default::default()
				},
			);
			let median_index = nearest_grid_index(grid, curve.median());
			let (median, _) = transform.categorical(shared_index, curve.median());
			let half_width = widths[median_index] as f32 * width_scale;
			backend.stroke_path(
				&|builder| {
					builder.move_to(iced::Point::new(median.x - half_width, median.y));
					builder.line_to(iced::Point::new(median.x + half_width, median.y));
				},
				Stroke {
					style: Style::Solid(settings.mark_style.decoration.iced()),
					width: 4.0,
					..Default::default()
				},
			);
		}
		if let Some(cursor) = cursor.position() {
			for curve in curves {
				let Some(index) = transform.category_index(curve.label()) else {
					continue;
				};
				let (center, band_width) = transform.categorical(index, 0.0);
				if cursor.x >= center.x - band_width / 2.0
					&& cursor.x <= center.x + band_width / 2.0
				{
					let (median, _) = transform.categorical(index, curve.median());
					backend.stroke_path(
						&|builder| builder.circle(median, 5.0),
						Stroke {
							style: Style::Solid(Color::from_rgb(1.0, 0.2, 0.2)),
							width: 2.0,
							..Default::default()
						},
					);
					break;
				}
			}
		}
	}

	fn legend_content(
		&self,
		settings: &ResolvedPlotSettings,
	) -> Option<crate::plot::legend::LegendContent> {
		crate::plot::legend::entries_from_labels(
			self.prepared_data
				.curves()
				.map(|curve| curve.label().to_owned()),
			settings,
		)
	}

	fn hover(&self, transform: &CoordinateTransformer, cursor: Cursor) -> Option<String> {
		let cursor = cursor.position()?;
		for curve in self.prepared_data.curves() {
			let index = transform.category_index(curve.label())?;
			let (center, band_width) = transform.categorical(index, 0.0);
			if cursor.x >= center.x - band_width / 2.0 && cursor.x <= center.x + band_width / 2.0 {
				return Some(format!("{}: Median {:.2}", curve.label(), curve.median()));
			}
		}
		None
	}

	fn x_label(&self) -> String {
		self.x_label.clone()
	}

	fn y_label(&self) -> String {
		self.y_label.clone()
	}
}

fn visible_extent(widths: &[f64]) -> Option<(usize, usize)> {
	let first = widths.iter().position(|width| *width > 0.0)?;
	let last = widths.iter().rposition(|width| *width > 0.0)?;
	(first < last).then_some((first, last))
}

fn nearest_grid_index(grid: &[f64], value: f64) -> usize {
	grid.iter()
		.enumerate()
		.min_by(|(_, left), (_, right)| {
			(value - **left)
				.abs()
				.partial_cmp(&(value - **right).abs())
				.expect("validated violin grid is finite")
		})
		.map(|(index, _)| index)
		.expect("rendered violin has a non-empty grid")
}

pub(crate) fn prepare_violin_density_data(
	df: &DataFrame,
	group_field: &str,
	evaluation_field: &str,
	density_field: &str,
	median_field: &str,
) -> Result<ViolinDensity, ViolinDensityError> {
	let group = df
		.column(group_field)
		.expect("PRODUCT-0108 resolution validated the group field");
	let evaluation = df
		.column(evaluation_field)
		.expect("PRODUCT-0108 resolution validated the evaluation field");
	let density = df
		.column(density_field)
		.expect("PRODUCT-0108 resolution validated the density field");
	let median = df
		.column(median_field)
		.expect("PRODUCT-0108 resolution validated the median field");
	let rows = (0..df.height())
		.map(|index| ViolinDensityInputRow {
			group: group_label(group.get(index).expect("row is in bounds")),
			evaluation_value: numeric_cell(evaluation.get(index).expect("row is in bounds")),
			density: numeric_cell(density.get(index).expect("row is in bounds")),
			median: numeric_cell(median.get(index).expect("row is in bounds")),
		})
		.collect();
	ViolinDensity::from_rows(rows)
}

fn group_label(value: AnyValue<'_>) -> Option<String> {
	match value {
		AnyValue::Null => None,
		AnyValue::String(value) => Some(value.to_owned()),
		AnyValue::StringOwned(value) => Some(value.as_str().to_owned()),
		AnyValue::Boolean(value) => Some(value.to_string()),
		other => Some(other.to_string().replace('"', "")),
	}
}

fn numeric_cell(value: AnyValue<'_>) -> ViolinDensityCell {
	let original = value.to_string();
	match exact_f64(value) {
		Ok(value) => ViolinDensityCell::Exact(value),
		Err(ExactF64Error::Null) => ViolinDensityCell::Null,
		Err(ExactF64Error::Inexact) => ViolinDensityCell::Inexact(original),
	}
}
