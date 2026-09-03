use crate::plot::common::{
	AxisType, CoordinateTransformer, PathBuilder, PlotBackend, PlotKernel, PlotLayout,
	ResolvedPlotSettings,
};
use crate::plot::exact_numeric::{ExactF64Error, exact_f64};
use crate::plot::kernels::violin_density::{
	ViolinDensity, ViolinDensityCell, ViolinDensityError, ViolinDensityInputRow,
	ViolinWidthNormalization,
};
use crate::plot::spec::CategoryColorAssignment;
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
	/// Authored violins carry one resolved assignment per group. `None` is kept
	/// only for direct legacy/test construction; production violin is authored.
	pub category_colors: Option<Vec<CategoryColorAssignment>>,
	pub x_label: String,
	pub y_label: String,
	pub selection_candidates:
		std::collections::HashMap<String, crate::selection::SelectionCandidate>,
}

impl ViolinPlotKernel {
	fn curve_color(&self, index: usize, settings: &ResolvedPlotSettings) -> Color {
		if let Some(assignments) = &self.category_colors {
			let category = self
				.prepared_data
				.curves()
				.nth(index)
				.expect("PRODUCT-0108 rendered violin index is a curve")
				.label();
			let assignment = assignments
				.iter()
				.find(|assignment| assignment.category == category)
				.expect("PRODUCT-0108 authored violin groups have complete colour assignments");
			return Color::from_rgba8(
				assignment.color.red,
				assignment.color.green,
				assignment.color.blue,
				assignment.color.alpha as f32 / u8::MAX as f32,
			);
		}
		let total = self.prepared_data.curves().count();
		let position = if total > 1 {
			index as f32 / (total - 1) as f32
		} else {
			0.5
		};
		settings.color_theme.get_color(position)
	}
}

impl PlotKernel for ViolinPlotKernel {
	fn project_predicate(
		&self,
		predicate: &crate::selection::Predicate,
	) -> Option<Vec<Box<dyn crate::plot::common::EmphasisAnchor>>> {
		let mut anchors: Vec<Box<dyn crate::plot::common::EmphasisAnchor>> = Vec::new();
		for (index, curve) in self.prepared_data.curves().enumerate() {
			let candidate = self.selection_candidates.get(curve.label())?;
			if predicate.matches_candidate(candidate) {
				anchors.push(Box::new(ViolinEmphasisAnchor { index }));
			}
		}
		Some(anchors)
	}

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
			let color = self.curve_color(index, &settings);
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
		crate::plot::legend::entries_from_pairs_with_sample_and_provenance(
			self.prepared_data
				.curves()
				.enumerate()
				.map(|(index, curve)| {
					(curve.label().to_owned(), self.curve_color(index, settings))
				}),
			crate::plot::legend::LegendSample::Swatch,
			crate::plot::legend::LegendProvenance::Named,
		)
	}

	fn hit(
		&self,
		transform: &CoordinateTransformer,
		cursor: Cursor,
	) -> Option<crate::plot::common::PlotHit> {
		let cursor = cursor.position()?;
		for curve in self.prepared_data.curves() {
			let index = transform.category_index(curve.label())?;
			let (center, band_width) = transform.categorical(index, 0.0);
			if cursor.x >= center.x - band_width / 2.0 && cursor.x <= center.x + band_width / 2.0 {
				let hit = crate::plot::common::PlotHit::new(
					crate::plot::common::MarkFamily::VIOLIN,
					crate::plot::common::HitDimensionality::Area,
					index,
				)
				.labeled(
					"Category",
					crate::plot::spec::SemanticType::Nominal,
					crate::plot::common::HitValue::nominal(curve.label()),
				)
				.labeled(
					"Median",
					crate::plot::spec::SemanticType::Quantitative,
					crate::plot::common::HitValue::quantitative(
						curve.median(),
						format!("{:.2}", curve.median()),
					),
				);
				return Some(
					match self.selection_candidates.get(curve.label()).cloned() {
						Some(candidate) => hit.with_candidate(candidate),
						None => hit,
					},
				);
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

struct ViolinEmphasisAnchor {
	index: usize,
}

impl crate::plot::common::EmphasisAnchor for ViolinEmphasisAnchor {
	fn reproject(
		&self,
		transform: &CoordinateTransformer,
	) -> Option<crate::plot::common::EmphasisShape> {
		let PlotLayout::CategoricalX { y_range, .. } = transform.layout else {
			return None;
		};
		let (low, band) = transform.categorical(self.index, y_range.0);
		let (high, _) = transform.categorical(self.index, y_range.1);
		Some(crate::plot::common::EmphasisShape::Rectangle(Rectangle {
			x: low.x - band / 2.0,
			y: low.y.min(high.y),
			width: band,
			height: (low.y - high.y).abs(),
		}))
	}
}

pub(crate) fn violin_selection_candidates(
	df: &DataFrame,
	group_field: &str,
) -> std::collections::HashMap<String, crate::selection::SelectionCandidate> {
	let Ok(column) = df.column(group_field) else {
		return Default::default();
	};
	let Some(scalar_type) = crate::selection::ScalarType::from_dtype(column.dtype()) else {
		return Default::default();
	};
	let Some(exact) = crate::selection::exact_column_values(column) else {
		return Default::default();
	};
	let mut candidates = std::collections::HashMap::new();
	for row in 0..df.height() {
		let Some(label) = group_label(column.get(row).expect("violin group row")) else {
			continue;
		};
		let Some(value) = exact.get(row).and_then(Clone::clone) else {
			continue;
		};
		candidates
			.entry(label)
			.or_insert_with(|| crate::selection::SelectionCandidate {
				constraints: vec![(
					crate::selection::FieldRef::new(
						group_field,
						crate::plot::spec::SemanticType::Nominal,
						scalar_type.clone(),
					),
					value,
				)],
			});
	}
	candidates
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

#[cfg(test)]
mod tests {
	use super::*;
	use crate::plot::legend::LegendSection;
	use crate::plot::spec::{PalettePosition, ResolvedColor};

	fn row(group: &str, evaluation_value: f64, density: f64, median: f64) -> ViolinDensityInputRow {
		ViolinDensityInputRow {
			group: Some(group.to_owned()),
			evaluation_value: ViolinDensityCell::Exact(evaluation_value),
			density: ViolinDensityCell::Exact(density),
			median: ViolinDensityCell::Exact(median),
		}
	}

	fn assignment(group: &str, index: usize, rgb: (u8, u8, u8)) -> CategoryColorAssignment {
		CategoryColorAssignment {
			category: group.to_owned(),
			position: PalettePosition::new(index, 2).expect("two-color palette position"),
			color: ResolvedColor {
				red: rgb.0,
				green: rgb.1,
				blue: rgb.2,
				alpha: u8::MAX,
			},
		}
	}

	#[test]
	fn product_0108_authored_palette_is_the_one_violin_and_legend_authority() {
		let kernel = ViolinPlotKernel {
			prepared_data: Arc::new(
				ViolinDensity::from_rows(vec![
					row("Aurora", 0.0, 0.2, 0.5),
					row("Aurora", 1.0, 0.2, 0.5),
					row("Boreal", 0.0, 0.2, 0.5),
					row("Boreal", 1.0, 0.2, 0.5),
				])
				.expect("shared-grid violin fixture"),
			),
			category_colors: Some(vec![
				assignment("Aurora", 0, (229, 57, 53)),
				assignment("Boreal", 1, (30, 136, 229)),
			]),
			x_label: "group".to_owned(),
			y_label: "value".to_owned(),
			selection_candidates: Default::default(),
		};
		let settings = ResolvedPlotSettings::default();
		let aurora = Color::from_rgb8(229, 57, 53);
		let boreal = Color::from_rgb8(30, 136, 229);

		assert_eq!(kernel.curve_color(0, &settings), aurora);
		assert_eq!(kernel.curve_color(1, &settings), boreal);

		let content = kernel
			.legend_content(&settings)
			.expect("named groups earn a legend");
		let LegendSection::Entries(entries) = content.sections().next().expect("entry section")
		else {
			panic!("violin legend is categorical entries");
		};
		assert_eq!(entries.first().label, "Aurora");
		assert_eq!(entries.first().swatch, aurora);
		assert_eq!(entries.iter().nth(1).expect("second entry").swatch, boreal);
	}

	#[test]
	fn product_0151_violin_candidate_is_the_exact_group_field() {
		let frame = polars::df!("group" => ["Aurora", "Boreal", "Aurora"]).expect("violin groups");
		let candidates = violin_selection_candidates(&frame, "group");
		assert_eq!(candidates.len(), 2);
		let aurora = candidates.get("Aurora").expect("candidate");
		assert_eq!(aurora.constraints.len(), 1);
		assert_eq!(aurora.constraints[0].0.name.as_str(), "group");
		assert_eq!(
			aurora.constraints[0].1,
			crate::selection::ExactScalar::Str("Aurora".into())
		);
	}
}
