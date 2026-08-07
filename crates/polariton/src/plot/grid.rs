//! PRODUCT-0041's internal plot-grid substrate.
//!
//! The grid is deliberately not a Rune surface.  PRODUCT-0042 will decide how
//! an author asks for one and who owns rows/columns; this module owns the
//! resolved cell model, the accepted value-facet producer, and the composite
//! renderer used by the test and future surface paths.

#![allow(dead_code)]

use crate::plot::common::{
	CoordinateTransformer, PlotBackend, PlotComposite, PlotKernel, PlotLayout, PlotWidget,
	ResolvedPlotSettings,
};
use crate::plot::factory::{create_plot, create_styled_plot};
use crate::plot::spec::{LayerSettings, NonEmpty, ResolvedPlotSpec, StampedResult};
use crate::plot::theme::PlotSettings;
use iced::advanced::mouse::Cursor;
use iced::{Point, Rectangle as IcedRectangle};
use polars::frame::DataFrame;
use polars::prelude::AnyValue;
use std::fmt;
use std::sync::Arc;

pub const MAX_GRID_CELLS: usize = 512;
const GRID_GAP: f32 = 12.0;

pub type DomainGroupId = u32;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct GridPosition {
	pub row: usize,
	pub column: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GridArrangement {
	pub rows: usize,
	pub columns: usize,
}

impl GridArrangement {
	pub fn new(rows: usize, columns: usize) -> Result<Self, GridError> {
		if rows == 0 || columns == 0 {
			return Err(GridError::InvalidArrangement { rows, columns });
		}
		Ok(Self { rows, columns })
	}

	/// PRODUCT-0042's derived facet arrangement: floor-sqrt rows and a
	/// sufficient number of columns, yielding a deliberately wider grid.
	pub fn for_facet_cell_count(count: usize) -> Self {
		let count = count.max(1);
		let rows = (count as f64).sqrt().floor().max(1.0) as usize;
		let columns = count.div_ceil(rows);
		Self { rows, columns }
	}

	fn capacity(self) -> usize {
		self.rows.saturating_mul(self.columns)
	}
}

#[derive(Clone)]
pub struct GridCell {
	pub position: GridPosition,
	pub body: Arc<ResolvedPlotSpec>,
	pub data: Arc<DataFrame>,
	pub x_group: DomainGroupId,
	pub y_group: DomainGroupId,
	pub label: Option<String>,
}

#[derive(Clone)]
pub struct PlotGrid {
	pub cells: NonEmpty<GridCell>,
	pub arrangement: GridArrangement,
	pub partition_field: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GridError {
	InvalidArrangement {
		rows: usize,
		columns: usize,
	},
	ArrangementTooSmall {
		cells: usize,
		capacity: usize,
	},
	TooManyCells {
		field: String,
		count: usize,
		limit: usize,
	},
	DifferentAxisGroup {
		axis: &'static str,
		expected: DomainGroupId,
		found: DomainGroupId,
		cell: usize,
	},
	DifferentBody {
		cell: usize,
	},
	MissingPartitionField {
		field: String,
	},
	DuplicatePosition {
		position: GridPosition,
	},
}

impl fmt::Display for GridError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::InvalidArrangement { rows, columns } => {
				write!(
					f,
					"grid arrangement must have positive rows and columns, got {rows}x{columns}"
				)
			}
			Self::ArrangementTooSmall { cells, capacity } => {
				write!(
					f,
					"grid arrangement capacity {capacity} cannot hold {cells} cells"
				)
			}
			Self::TooManyCells {
				field,
				count,
				limit,
			} => {
				write!(
					f,
					"facet field `{field}` produced {count} cells, exceeding the {limit}-cell limit"
				)
			}
			Self::DifferentAxisGroup {
				axis,
				expected,
				found,
				cell,
			} => write!(
				f,
				"grid cell {cell} uses {axis} domain group {found}, expected {expected}"
			),
			Self::DifferentBody { cell } => {
				write!(f, "grid cell {cell} carries a different body template")
			}
			Self::MissingPartitionField { field } => {
				write!(
					f,
					"facet field `{field}` is not present in the stamped result"
				)
			}
			Self::DuplicatePosition { position } => write!(
				f,
				"grid position ({}, {}) is occupied more than once",
				position.row, position.column
			),
		}
	}
}

impl std::error::Error for GridError {}

impl PlotGrid {
	pub fn single(body: Arc<ResolvedPlotSpec>, data: Arc<DataFrame>) -> Self {
		let cell = GridCell {
			position: GridPosition { row: 0, column: 0 },
			body,
			data,
			x_group: 0,
			y_group: 0,
			label: None,
		};
		Self::new(
			NonEmpty::new(cell),
			GridArrangement {
				rows: 1,
				columns: 1,
			},
		)
		.expect("the one-cell grid arrangement is valid")
	}

	pub fn value_facet_derived(
		body: Arc<ResolvedPlotSpec>,
		stamped: &StampedResult,
		field: impl Into<String>,
	) -> Result<Self, GridError> {
		let probe = GridArrangement::new(MAX_GRID_CELLS, MAX_GRID_CELLS)
			.expect("the facet probe arrangement is positive");
		let mut grid = Self::value_facet(body, stamped, field, probe)?;
		let arrangement = GridArrangement::for_facet_cell_count(grid.cells.len());
		let (mut first, mut rest) = grid.cells.into_parts();
		for (index, cell) in std::iter::once(&mut first)
			.chain(rest.iter_mut())
			.enumerate()
		{
			cell.position = GridPosition {
				row: index / arrangement.columns,
				column: index % arrangement.columns,
			};
		}
		let mut cells = NonEmpty::new(first);
		for cell in rest {
			cells.push(cell);
		}
		grid.cells = cells;
		grid.arrangement = arrangement;
		Ok(grid)
	}

	pub fn new(cells: NonEmpty<GridCell>, arrangement: GridArrangement) -> Result<Self, GridError> {
		let count = cells.len();
		if arrangement.capacity() < count {
			return Err(GridError::ArrangementTooSmall {
				cells: count,
				capacity: arrangement.capacity(),
			});
		}
		if count > MAX_GRID_CELLS {
			return Err(GridError::TooManyCells {
				field: "<grid>".to_owned(),
				count,
				limit: MAX_GRID_CELLS,
			});
		}
		let first = cells.first();
		let mut positions = std::collections::HashSet::with_capacity(count);
		for (index, cell) in cells.iter().enumerate() {
			if !positions.insert(cell.position) {
				return Err(GridError::DuplicatePosition {
					position: cell.position,
				});
			}
			if cell.x_group != 0 {
				return Err(GridError::DifferentAxisGroup {
					axis: "x",
					expected: 0,
					found: cell.x_group,
					cell: index,
				});
			}
			if cell.y_group != 0 {
				return Err(GridError::DifferentAxisGroup {
					axis: "y",
					expected: 0,
					found: cell.y_group,
					cell: index,
				});
			}
			if !Arc::ptr_eq(&first.body, &cell.body) {
				return Err(GridError::DifferentBody { cell: index });
			}
			if cell.position.row >= arrangement.rows || cell.position.column >= arrangement.columns
			{
				return Err(GridError::ArrangementTooSmall {
					cells: count,
					capacity: arrangement.capacity(),
				});
			}
		}
		Ok(Self {
			cells,
			arrangement,
			partition_field: None,
		})
	}

	pub fn value_facet(
		body: Arc<ResolvedPlotSpec>,
		stamped: &StampedResult,
		field: impl Into<String>,
		arrangement: GridArrangement,
	) -> Result<Self, GridError> {
		let field = field.into();
		if stamped.data.column(&field).is_err() {
			return Err(GridError::MissingPartitionField { field });
		}
		let mut partitions = if stamped.data.height() == 0 {
			vec![stamped.data.as_ref().clone()]
		} else {
			stamped
				.data
				.partition_by([field.as_str()], true)
				.unwrap_or_else(|_| vec![stamped.data.as_ref().clone()])
		};
		// `partition_by` is free to choose a hash-map order.  A facet is a
		// display operation, so restore the first-appearance order from the
		// stamped relation before assigning cell positions.
		if partitions.len() > 1 {
			let first_appearance: Vec<String> = stamped
				.data
				.column(&field)
				.ok()
				.into_iter()
				.flat_map(|column| {
					(0..stamped.data.height()).filter_map(move |index| column.get(index).ok())
				})
				.filter_map(any_value_label)
				.fold(Vec::<String>::new(), |mut seen, label| {
					if !seen.contains(&label) {
						seen.push(label);
					}
					seen
				});
			partitions.sort_by_key(|partition| {
				partition
					.column(&field)
					.ok()
					.and_then(|column| column.get(0).ok())
					.and_then(any_value_label)
					.and_then(|label| first_appearance.iter().position(|seen| seen == &label))
					.unwrap_or(usize::MAX)
			});
		}
		if partitions.len() > MAX_GRID_CELLS {
			return Err(GridError::TooManyCells {
				field,
				count: partitions.len(),
				limit: MAX_GRID_CELLS,
			});
		}
		let mut cells: Option<NonEmpty<GridCell>> = None;
		for (index, partition) in partitions.into_iter().enumerate() {
			let position = GridPosition {
				row: index / arrangement.columns.max(1),
				column: index % arrangement.columns.max(1),
			};
			let label = partition
				.column(&field)
				.ok()
				.and_then(|column| column.get(0).ok())
				.and_then(any_value_label);
			let cell = GridCell {
				position,
				body: Arc::clone(&body),
				data: Arc::new(partition),
				x_group: 0,
				y_group: 0,
				label,
			};
			if let Some(cells) = cells.as_mut() {
				cells.push(cell);
			} else {
				cells = Some(NonEmpty::new(cell));
			}
		}
		let cells = cells.expect("empty relation creates the degenerate one-cell grid");
		let mut grid = Self::new(cells, arrangement)?;
		grid.partition_field = Some(field);
		Ok(grid)
	}
}

fn any_value_label(value: AnyValue<'_>) -> Option<String> {
	if value.is_null() {
		None
	} else {
		Some(value.to_string().trim_matches('"').to_owned())
	}
}

struct PreparedGridCell {
	cell: GridCell,
	kernel: Arc<dyn PlotKernel + Send + Sync>,
}

pub struct PlotGridKernel {
	cells: NonEmpty<PreparedGridCell>,
	arrangement: GridArrangement,
}

impl PlotGridKernel {
	pub fn from_grid(grid: PlotGrid, width: u32, height: u32) -> Self {
		Self::from_grid_with_factory(grid, width, height, |spec, data| {
			Ok(create_plot(spec, data, width, height))
		})
		.expect("plain plot grid kernels are infallible")
	}

	pub fn from_grid_with_factory(
		grid: PlotGrid,
		_width: u32,
		_height: u32,
		mut factory: impl FnMut(
			&ResolvedPlotSpec,
			&DataFrame,
		) -> Result<Arc<dyn PlotKernel + Send + Sync>, String>,
	) -> Result<Self, String> {
		let (first, rest) = grid.cells.into_parts();
		let mut prepare = |cell: GridCell| -> Result<PreparedGridCell, String> {
			let kernel = factory(&cell.body, &cell.data)?;
			Ok(PreparedGridCell { kernel, cell })
		};
		let mut cells = NonEmpty::new(prepare(first)?);
		for cell in rest {
			cells.push(prepare(cell)?);
		}
		Ok(Self {
			cells,
			arrangement: grid.arrangement,
		})
	}

	pub fn from_styled_grid(
		grid: PlotGrid,
		layer_settings: LayerSettings,
		width: u32,
		height: u32,
	) -> Result<Self, String> {
		Self::from_grid_with_factory(grid, width, height, |spec, data| {
			create_styled_plot(spec, data, layer_settings.clone())
		})
	}

	fn cell_layouts(&self, settings: ResolvedPlotSettings) -> Vec<(GridPosition, PlotLayout)> {
		let local: Vec<_> = self
			.cells
			.iter()
			.map(|cell| (cell.cell.position, cell.kernel.layout(settings.clone())))
			.collect();
		let shared = shared_layout(&local);
		local
			.into_iter()
			.map(|(position, layout)| (position, merge_layout(layout, &shared)))
			.collect()
	}
}

impl PlotKernel for PlotGridKernel {
	fn layout(&self, settings: ResolvedPlotSettings) -> PlotLayout {
		self.cells.first().kernel.layout(settings)
	}

	fn plot(
		&self,
		_backend: &mut dyn PlotBackend,
		_bounds: IcedRectangle,
		_transform: &CoordinateTransformer,
		_cursor: Cursor,
		_settings: ResolvedPlotSettings,
	) {
		// PlotWidget dispatches the composite seam instead of this ordinary path.
	}

	fn hover(&self, _transform: &CoordinateTransformer, _cursor: Cursor) -> Option<String> {
		None
	}

	fn composite(&self) -> Option<&dyn PlotComposite> {
		Some(self)
	}
}

impl PlotComposite for PlotGridKernel {
	fn owns_global_headings(&self) -> bool {
		self.cells.len() == 1
	}

	fn render_plot_layer(
		&self,
		backend: &mut dyn PlotBackend,
		bounds: IcedRectangle,
		padding: f32,
		title: &str,
		settings: &PlotSettings,
	) {
		// The backend frame is widget-local; `bounds` carries the widget's
		// absolute window position. Like PlotWidget::plot_area, the grid
		// consumes only the size.
		let bounds = IcedRectangle::new(Point::ORIGIN, bounds.size());
		if self.cells.len() == 1 {
			let cell = self.cells.first();
			let helper = PlotWidget {
				kernel: cell.kernel.as_ref(),
				title: title.to_owned(),
				padding,
				settings: settings.resolve(),
				settings_model: settings.clone(),
				render_revision: 0,
				resize_render_suspended: false,
				layer: crate::plot::common::PlotRenderLayer::Full,
			};
			helper.render_plot_layer(backend, bounds);
			return;
		}
		let resolved = settings.resolve();
		// iced_wgpu 0.14 converts a frame's direct fills into meshes only at
		// frame completion, AFTER `with_clip` children have been pasted, so a
		// parent-level fill z-orders ABOVE every clipped child on the live
		// renderer even though SVG and tiny-skia preserve call order. No
		// parent-level draw may coexist with clipped children: the canvas
		// background is therefore emitted as the first clipped child itself.
		backend.with_clip(bounds, &mut |backend| {
			backend.fill_rectangle(
				Point::ORIGIN,
				bounds.size(),
				resolved.chrome.canvas_background.iced(),
			);
		});
		let outer = grid_outer_area(bounds, padding);
		let layouts = self.cell_layouts(resolved.clone());
		for (index, cell) in self.cells.iter().enumerate() {
			let (position, layout) = &layouts[index];
			let cell_bounds = grid_cell_bounds(outer, self.arrangement, *position);
			let inner = plot_area(cell_bounds, 0.0, &resolved);
			let transform = CoordinateTransformer::new(layout, inner);
			backend.with_clip(cell_bounds, &mut |backend| {
				backend.fill_rectangle(
					Point::new(cell_bounds.x, cell_bounds.y),
					cell_bounds.size(),
					resolved.chrome.canvas_background.iced(),
				);
				backend.with_save(&mut |backend| {
					backend.translate(iced::Vector::new(inner.x, inner.y));
					cell.kernel.plot_with_settings(
						backend,
						inner,
						&transform,
						Cursor::Unavailable,
						settings,
					);
				});
			});
		}
	}

	fn render_overlay_layer(
		&self,
		backend: &mut dyn PlotBackend,
		bounds: IcedRectangle,
		padding: f32,
		title: &str,
		settings: &PlotSettings,
	) {
		let bounds = IcedRectangle::new(Point::ORIGIN, bounds.size());
		if self.cells.len() == 1 {
			let cell = self.cells.first();
			let helper = PlotWidget {
				kernel: cell.kernel.as_ref(),
				title: title.to_owned(),
				padding,
				settings: settings.resolve(),
				settings_model: settings.clone(),
				render_revision: 0,
				resize_render_suspended: false,
				layer: crate::plot::common::PlotRenderLayer::Full,
			};
			helper.render_overlay_layer(backend, bounds);
			return;
		}
		let resolved = settings.resolve();
		let outer = grid_outer_area(bounds, padding);
		let layouts = self.cell_layouts(resolved.clone());
		for (index, cell) in self.cells.iter().enumerate() {
			let (position, layout) = &layouts[index];
			let cell_bounds = grid_cell_bounds(outer, self.arrangement, *position);
			let inner = plot_area(cell_bounds, 0.0, &resolved);
			let transform = CoordinateTransformer::new(layout, inner);
			let helper = PlotWidget {
				kernel: cell.kernel.as_ref(),
				title: String::new(),
				padding: 0.0,
				settings: resolved.clone(),
				settings_model: settings.clone(),
				render_revision: 0,
				resize_render_suspended: false,
				layer: crate::plot::common::PlotRenderLayer::Full,
			};
			backend.with_clip(cell_bounds, &mut |backend| {
				backend.with_save(&mut |backend| {
					backend.translate(iced::Vector::new(inner.x, inner.y));
					match layout {
						PlotLayout::Cartesian {
							x_range, y_range, ..
						} => {
							helper.draw_cartesian_grid(
								backend, inner, &transform, *x_range, *y_range,
							);
							helper.draw_cartesian_axes(
								backend, inner, &transform, *x_range, *y_range,
							);
						}
						PlotLayout::CategoricalX {
							categories,
							y_range,
						} => {
							helper.draw_categorical_grid(
								backend, inner, &transform, categories, *y_range,
							);
							helper.draw_categorical_axes(
								backend, inner, &transform, categories, *y_range,
							);
						}
						PlotLayout::CategoricalY {
							categories,
							x_range,
						} => {
							helper.draw_categorical_y_grid(
								backend, inner, &transform, categories, *x_range,
							);
							helper.draw_categorical_y_axes(
								backend, inner, &transform, categories, *x_range,
							);
						}
						PlotLayout::CategoricalXY {
							x_categories,
							y_categories,
						} => {
							helper.draw_categorical_xy_axes(
								backend,
								inner,
								&transform,
								x_categories,
								y_categories,
							);
						}
						PlotLayout::Parallel { dimensions, ranges } => {
							helper
								.draw_parallel_axes(backend, inner, &transform, dimensions, ranges);
						}
						PlotLayout::Radial => {}
					}
				});
				helper.draw_axis_labels(backend, inner);
				backend.with_save(&mut |backend| {
					backend.translate(iced::Vector::new(cell_bounds.x, cell_bounds.y));
					cell.kernel.draw_legend(
						backend,
						IcedRectangle::new(Point::ORIGIN, cell_bounds.size()),
						resolved.clone(),
					);
				});
				if let Some(label) = &cell.cell.label {
					backend.fill_text(iced::widget::canvas::Text {
						content: label.clone(),
						// A facet label is a per-cell heading, not a corner annotation.
						// Center it over the actual plot area so it reads like a
						// miniature plot title while leaving the shared figure heading
						// independent.
						position: Point::new(inner.x + inner.width / 2.0, cell_bounds.y + 8.0),
						color: resolved.chrome.title_text.iced(),
						size: iced::Pixels((resolved.legend_size + 2.0).max(10.0)),
						align_x: iced::alignment::Horizontal::Center.into(),
						align_y: iced::alignment::Vertical::Top,
						..Default::default()
					});
				}
			});
		}
	}

	fn hover(
		&self,
		bounds: IcedRectangle,
		padding: f32,
		cursor: Cursor,
		settings: &PlotSettings,
	) -> Option<String> {
		// The caller supplies a widget-local cursor; cell rects must be
		// computed in the same local space.
		let bounds = IcedRectangle::new(Point::ORIGIN, bounds.size());
		let cursor = cursor.position()?;
		let resolved = settings.resolve();
		let outer = grid_outer_area(bounds, padding);
		let layouts = self.cell_layouts(resolved.clone());
		for (index, cell) in self.cells.iter().enumerate() {
			let (position, layout) = &layouts[index];
			let cell_bounds = grid_cell_bounds(outer, self.arrangement, *position);
			if !cell_bounds.contains(cursor) {
				continue;
			}
			let inner = plot_area(cell_bounds, 0.0, &resolved);
			let transform = CoordinateTransformer::new(layout, inner);
			let local = Point::new(cursor.x - inner.x, cursor.y - inner.y);
			return cell
				.kernel
				.hover_with_settings(&transform, Cursor::Available(local), settings);
		}
		None
	}
}

fn plot_area(
	bounds: IcedRectangle,
	padding: f32,
	settings: &ResolvedPlotSettings,
) -> IcedRectangle {
	let left = padding + settings.plot_padding_left;
	let right = padding + settings.plot_padding_right;
	let top = padding + settings.plot_padding_top;
	let bottom = padding + settings.plot_padding_bottom;
	IcedRectangle {
		x: bounds.x + left,
		y: bounds.y + top,
		width: (bounds.width - left - right).max(0.0),
		height: (bounds.height - top - bottom).max(0.0),
	}
}

fn grid_outer_area(bounds: IcedRectangle, padding: f32) -> IcedRectangle {
	IcedRectangle {
		x: bounds.x + padding,
		y: bounds.y + padding,
		width: (bounds.width - 2.0 * padding).max(0.0),
		height: (bounds.height - 2.0 * padding).max(0.0),
	}
}

pub fn grid_cell_bounds(
	outer: IcedRectangle,
	arrangement: GridArrangement,
	position: GridPosition,
) -> IcedRectangle {
	let columns = arrangement.columns.max(1) as f32;
	let rows = arrangement.rows.max(1) as f32;
	let gap_x = GRID_GAP.min(outer.width / columns.max(1.0));
	let gap_y = GRID_GAP.min(outer.height / rows.max(1.0));
	let width = (outer.width - gap_x * (columns - 1.0)).max(0.0) / columns;
	let height = (outer.height - gap_y * (rows - 1.0)).max(0.0) / rows;
	IcedRectangle {
		x: outer.x + position.column as f32 * (width + gap_x),
		y: outer.y + position.row as f32 * (height + gap_y),
		width,
		height,
	}
}

fn shared_layout(local: &[(GridPosition, PlotLayout)]) -> Option<PlotLayout> {
	let first = local.first()?.1.clone();
	match first {
		PlotLayout::Cartesian {
			x_axis_type,
			y_axis_type,
			..
		} => {
			let mut x = (f64::INFINITY, f64::NEG_INFINITY);
			let mut y = (f64::INFINITY, f64::NEG_INFINITY);
			for (_, layout) in local {
				if let PlotLayout::Cartesian {
					x_range, y_range, ..
				} = layout
				{
					x.0 = x.0.min(x_range.0);
					x.1 = x.1.max(x_range.1);
					y.0 = y.0.min(y_range.0);
					y.1 = y.1.max(y_range.1);
				}
			}
			Some(PlotLayout::Cartesian {
				x_range: x,
				y_range: y,
				x_axis_type,
				y_axis_type,
			})
		}
		_ => Some(first),
	}
}

fn merge_layout(layout: PlotLayout, shared: &Option<PlotLayout>) -> PlotLayout {
	match (layout, shared) {
		(
			PlotLayout::Cartesian {
				x_axis_type,
				y_axis_type,
				..
			},
			Some(PlotLayout::Cartesian {
				x_range, y_range, ..
			}),
		) => PlotLayout::Cartesian {
			x_range: *x_range,
			y_range: *y_range,
			x_axis_type,
			y_axis_type,
		},
		(layout, _) => layout,
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::plot::common::{PlotRenderLayer, PlotWidget};
	use crate::plot::composition::PlotComposition;
	use crate::plot::core::PlotType;
	use crate::plot::export::SvgBackend;
	use crate::plot::request::{PlotRequest, PlotSource, plan};
	use crate::plot::spec::{AuthoredPlotSpec, Channel, PlotProvenance};
	use polars::df;

	fn fixture() -> (Arc<ResolvedPlotSpec>, Arc<DataFrame>, StampedResult) {
		let data = Arc::new(
			df!(
				"facet" => ["A", "B", "A", "B"],
				"x" => [0.0_f64, 1.0, 2.0, 3.0],
				"y" => [1.0_f64, 2.0, 3.0, 4.0]
			)
			.expect("fixture frame"),
		);
		let resolved = plan(PlotRequest {
			source: PlotSource::Materialized {
				name: "grid fixture".to_owned(),
				data: Arc::clone(&data),
			},
			authored_spec: AuthoredPlotSpec::xy(PlotComposition::single(PlotType::Line), "x", "y"),
		})
		.expect("fixture resolves");
		let body = Arc::new(resolved.resolved_spec);
		let stamped = StampedResult {
			data: Arc::clone(&data),
			provenance: PlotProvenance {
				source_name: "grid fixture".to_owned(),
				revision: None,
			},
		};
		(body, data, stamped)
	}

	fn rendered_tick_paths(svg: &str, clip_id: usize, origin: Point) -> Vec<[f32; 4]> {
		let marker = format!(r#"<g clip-path="url(#{})">"#, format!("clip_{clip_id}"));
		let body = svg
			.split_once(&marker)
			.and_then(|(_, rest)| rest.split_once("</g>"))
			.map(|(body, _)| body)
			.expect("overlay clip body");
		let mut paths: Vec<[f32; 4]> = body
			.split("<path d=\"")
			.skip(1)
			.filter_map(|fragment| {
				if !fragment.contains("stroke-width=\"2\"")
					|| !fragment.contains("stroke-opacity=\"1.00\"")
				{
					return None;
				}
				let d = fragment.split('\"').next()?;
				let tokens: Vec<_> = d.split_whitespace().collect();
				if tokens.len() != 6 || tokens[0] != "M" || tokens[3] != "L" {
					return None;
				}
				Some([
					round_coordinate(tokens[1].parse::<f32>().ok()? - origin.x),
					round_coordinate(tokens[2].parse::<f32>().ok()? - origin.y),
					round_coordinate(tokens[4].parse::<f32>().ok()? - origin.x),
					round_coordinate(tokens[5].parse::<f32>().ok()? - origin.y),
				])
			})
			.collect();
		paths.sort_by(|left, right| left.partial_cmp(right).expect("finite tick coordinates"));
		paths
	}

	fn round_coordinate(value: f32) -> f32 {
		(value * 100.0).round() / 100.0
	}

	fn rendered_single_mark(svg: &str, clip_id: usize) -> Point {
		let marker = format!(r#"<g clip-path="url(#clip_{clip_id})">"#);
		let body = svg
			.split_once(&marker)
			.and_then(|(_, rest)| rest.split_once("</g>"))
			.map(|(body, _)| body)
			.expect("data clip body");
		body.split("<path d=\"")
			.skip(1)
			.find_map(|fragment| {
				if !fragment.contains("stroke-width=\"2\"")
					|| !fragment.contains("stroke-opacity=\"1.00\"")
				{
					return None;
				}
				let tokens: Vec<_> = fragment.split('"').next()?.split_whitespace().collect();
				if tokens.len() == 3 && tokens[0] == "M" {
					Some(Point::new(tokens[1].parse().ok()?, tokens[2].parse().ok()?))
				} else {
					None
				}
			})
			.expect("single-point mark")
	}

	#[test]
	fn value_facet_preserves_partition_order_and_exact_slices() {
		let (body, _data, stamped) = fixture();
		let arrangement = GridArrangement::new(1, 2).expect("arrangement");
		let grid = PlotGrid::value_facet(body, &stamped, "facet", arrangement).expect("facet");
		assert_eq!(grid.cells.len(), 2);
		let cells: Vec<_> = grid.cells.iter().collect();
		assert_eq!(cells[0].label.as_deref(), Some("A"));
		assert_eq!(cells[1].label.as_deref(), Some("B"));
		assert_eq!(cells[0].position, GridPosition { row: 0, column: 0 });
		assert_eq!(cells[1].position, GridPosition { row: 0, column: 1 });
		assert_eq!(cells[0].data.height(), 2);
		assert_eq!(cells[1].data.height(), 2);
		let first_x: Vec<_> = cells[0]
			.data
			.column("x")
			.expect("x column")
			.f64()
			.expect("f64 x")
			.into_no_null_iter()
			.collect();
		let second_x: Vec<_> = cells[1]
			.data
			.column("x")
			.expect("x column")
			.f64()
			.expect("f64 x")
			.into_no_null_iter()
			.collect();
		assert_eq!(first_x, vec![0.0, 2.0]);
		assert_eq!(second_x, vec![1.0, 3.0]);
	}

	#[test]
	fn value_facet_rederives_order_after_sort_or_filter() {
		let (body, _data, stamped) = fixture();
		let reordered = Arc::new(
			df!(
				"facet" => ["B", "A", "C"],
				"x" => [0.0_f64, 1.0, 2.0],
				"y" => [1.0_f64, 2.0, 3.0]
			)
			.expect("reordered frame"),
		);
		let reordered_grid = PlotGrid::value_facet(
			Arc::clone(&body),
			&StampedResult {
				data: reordered,
				provenance: stamped.provenance.clone(),
			},
			"facet",
			GridArrangement::new(2, 2).expect("arrangement"),
		)
		.expect("reordered facet");
		let labels: Vec<_> = reordered_grid
			.cells
			.iter()
			.map(|cell| cell.label.as_deref())
			.collect();
		assert_eq!(labels, vec![Some("B"), Some("A"), Some("C")]);

		let filtered = Arc::new(
			df!(
				"facet" => ["B", "C"],
				"x" => [0.0_f64, 2.0],
				"y" => [1.0_f64, 3.0]
			)
			.expect("filtered frame"),
		);
		let filtered_grid = PlotGrid::value_facet(
			body,
			&StampedResult {
				data: filtered,
				provenance: stamped.provenance,
			},
			"facet",
			GridArrangement::new(1, 2).expect("arrangement"),
		)
		.expect("filtered facet");
		let labels: Vec<_> = filtered_grid
			.cells
			.iter()
			.map(|cell| cell.label.as_deref())
			.collect();
		assert_eq!(labels, vec![Some("B"), Some("C")]);
	}

	#[test]
	fn faceting_by_the_authored_color_field_renders_sticky_colors() {
		let data = Arc::new(
			df!(
				"facet" => ["A", "B", "A", "B"],
				"color" => ["shared", "shared", "shared", "shared"],
				"x" => [0.0_f64, 1.0, 2.0, 3.0],
				"y" => [1.0_f64, 2.0, 3.0, 4.0]
			)
			.expect("colored frame"),
		);
		let resolved = plan(PlotRequest {
			source: PlotSource::Materialized {
				name: "colored grid fixture".to_owned(),
				data: Arc::clone(&data),
			},
			authored_spec: AuthoredPlotSpec::xy_color(
				PlotComposition::single(PlotType::Scatter),
				"x",
				"y",
				Some("color".to_owned()),
			),
		})
		.expect("colored plot resolves");
		let body = Arc::new(resolved.resolved_spec);
		let expected = body
			.channel(Channel::Color)
			.and_then(|channel| {
				channel
					.colors
					.iter()
					.find(|entry| entry.category == "shared")
			})
			.map(|entry| {
				format!(
					"rgb({}, {}, {})",
					entry.color.red, entry.color.green, entry.color.blue
				)
			})
			.expect("shared color assignment");
		let grid = PlotGrid::value_facet(
			body,
			&StampedResult {
				data,
				provenance: PlotProvenance {
					source_name: "colored grid fixture".to_owned(),
					revision: None,
				},
			},
			"facet",
			GridArrangement::new(1, 2).expect("arrangement"),
		)
		.expect("colored facet");
		let kernel = PlotGridKernel::from_grid(grid, 800, 400);
		let model = PlotSettings::default();
		let mut backend = SvgBackend::new(800.0, 400.0);
		let widget = PlotWidget {
			kernel: &kernel,
			title: String::new(),
			padding: 0.0,
			settings: model.resolve(),
			settings_model: model,
			render_revision: 0,
			resize_render_suspended: false,
			layer: PlotRenderLayer::Full,
		};
		widget.render(
			&mut backend,
			IcedRectangle::new(Point::ORIGIN, iced::Size::new(800.0, 400.0)),
		);
		assert!(backend.finish().matches(&expected).count() >= 2);
	}

	#[test]
	fn empty_relation_is_the_degenerate_one_cell_grid() {
		let (body, _data, mut stamped) = fixture();
		stamped.data = Arc::new(
			df!(
				"facet" => Vec::<&str>::new(),
				"x" => Vec::<f64>::new(),
				"y" => Vec::<f64>::new()
			)
			.expect("empty frame"),
		);
		let grid = PlotGrid::value_facet(
			body,
			&stamped,
			"facet",
			GridArrangement::new(1, 1).expect("arrangement"),
		)
		.expect("empty facet");
		assert_eq!(grid.cells.len(), 1);
		assert_eq!(grid.cells.first().data.height(), 0);
		let (_, _, stamped) = fixture();
		assert!(matches!(
			PlotGrid::value_facet(
				Arc::new(fixture().0.as_ref().clone()),
				&stamped,
				"missing",
				GridArrangement::new(1, 1).expect("arrangement")
			),
			Err(GridError::MissingPartitionField { field }) if field == "missing"
		));
	}

	#[test]
	fn grid_rejects_mixed_groups_bodies_and_positions() {
		let (body, data, _stamped) = fixture();
		let cell = |position, body: Arc<ResolvedPlotSpec>| GridCell {
			position,
			body,
			data: Arc::clone(&data),
			x_group: 0,
			y_group: 0,
			label: None,
		};
		let mut groups = NonEmpty::new(cell(GridPosition { row: 0, column: 0 }, Arc::clone(&body)));
		groups.push(GridCell {
			x_group: 1,
			..cell(GridPosition { row: 0, column: 1 }, Arc::clone(&body))
		});
		assert!(matches!(
			PlotGrid::new(groups, GridArrangement::new(1, 2).expect("arrangement")),
			Err(GridError::DifferentAxisGroup { axis: "x", .. })
		));
		let mut groups_y =
			NonEmpty::new(cell(GridPosition { row: 0, column: 0 }, Arc::clone(&body)));
		groups_y.push(GridCell {
			y_group: 2,
			..cell(GridPosition { row: 0, column: 1 }, Arc::clone(&body))
		});
		assert!(matches!(
			PlotGrid::new(groups_y, GridArrangement::new(1, 2).expect("arrangement")),
			Err(GridError::DifferentAxisGroup { axis: "y", .. })
		));

		let other = Arc::new(
			plan(PlotRequest {
				source: PlotSource::Materialized {
					name: "other".to_owned(),
					data: Arc::clone(&data),
				},
				authored_spec: AuthoredPlotSpec::xy(
					PlotComposition::single(PlotType::Scatter),
					"x",
					"y",
				),
			})
			.expect("other resolves")
			.resolved_spec,
		);
		let bodies = NonEmpty::new(cell(GridPosition { row: 0, column: 0 }, body));
		let mut bodies = bodies;
		bodies.push(cell(GridPosition { row: 0, column: 1 }, other));
		assert!(matches!(
			PlotGrid::new(bodies, GridArrangement::new(1, 2).expect("arrangement")),
			Err(GridError::DifferentBody { cell: 1 })
		));

		let duplicate = NonEmpty::new(cell(
			GridPosition { row: 0, column: 0 },
			Arc::new(fixture().0.as_ref().clone()),
		));
		let mut duplicate = duplicate;
		duplicate.push(cell(
			GridPosition { row: 0, column: 0 },
			Arc::clone(&duplicate.first().body),
		));
		assert!(matches!(
			PlotGrid::new(duplicate, GridArrangement::new(1, 2).expect("arrangement")),
			Err(GridError::DuplicatePosition { .. })
		));
	}

	#[test]
	fn faceted_cartesian_cells_share_their_domain() {
		let (body, _data, stamped) = fixture();
		let grid = PlotGrid::value_facet(
			body,
			&stamped,
			"facet",
			GridArrangement::new(1, 2).expect("arrangement"),
		)
		.expect("facet");
		let kernel = PlotGridKernel::from_grid(grid, 400, 300);
		let layouts = kernel.cell_layouts(ResolvedPlotSettings::default());
		let ranges: Vec<_> = layouts
			.iter()
			.map(|(_, layout)| match layout {
				PlotLayout::Cartesian {
					x_range, y_range, ..
				} => (*x_range, *y_range),
				_ => panic!("expected Cartesian layout"),
			})
			.collect();
		assert_eq!(ranges[0], ranges[1]);
	}

	#[test]
	fn grid_regions_are_disjoint_and_multi_cell_rendering_is_composite() {
		let (body, _data, stamped) = fixture();
		let grid = PlotGrid::value_facet(
			body,
			&stamped,
			"facet",
			GridArrangement::new(1, 2).expect("arrangement"),
		)
		.expect("facet");
		let arrangement = grid.arrangement;
		let outer = IcedRectangle::new(Point::ORIGIN, iced::Size::new(800.0, 400.0));
		let left = grid_cell_bounds(outer, arrangement, GridPosition { row: 0, column: 0 });
		let right = grid_cell_bounds(outer, arrangement, GridPosition { row: 0, column: 1 });
		assert!(left.x + left.width <= right.x);

		let kernel = PlotGridKernel::from_grid(grid, 800, 400);
		let model = PlotSettings::default();
		let mut backend = SvgBackend::new(800.0, 400.0);
		let widget = PlotWidget {
			kernel: &kernel,
			title: String::new(),
			padding: 0.0,
			settings: model.resolve(),
			settings_model: model,
			render_revision: 0,
			resize_render_suspended: false,
			layer: PlotRenderLayer::Full,
		};
		widget.render(
			&mut backend,
			IcedRectangle::new(Point::ORIGIN, iced::Size::new(800.0, 400.0)),
		);
		let svg = backend.finish();
		assert_eq!(svg.matches("<clipPath").count(), 5);
		assert!(svg.matches("<path").count() >= 2);
	}

	#[test]
	fn four_cell_value_facet_renders_four_regions() {
		let (body, _data, stamped) = fixture();
		let data = Arc::new(
			df!(
				"facet" => ["A", "B", "C", "D"],
				"x" => [0.0_f64, 1.0, 2.0, 3.0],
				"y" => [1.0_f64, 2.0, 3.0, 4.0]
			)
			.expect("four-cell frame"),
		);
		let stamped = StampedResult {
			data,
			provenance: stamped.provenance,
		};
		let grid = PlotGrid::value_facet(
			body,
			&stamped,
			"facet",
			GridArrangement::new(2, 2).expect("arrangement"),
		)
		.expect("facet");
		let kernel = PlotGridKernel::from_grid(grid, 800, 600);
		let layouts = kernel.cell_layouts(ResolvedPlotSettings::default());
		let model = PlotSettings::default();
		let mut backend = SvgBackend::new(800.0, 600.0);
		let widget = PlotWidget {
			kernel: &kernel,
			title: String::new(),
			padding: 0.0,
			settings: model.resolve(),
			settings_model: model,
			render_revision: 0,
			resize_render_suspended: false,
			layer: PlotRenderLayer::Full,
		};
		widget.render(
			&mut backend,
			IcedRectangle::new(Point::ORIGIN, iced::Size::new(800.0, 600.0)),
		);
		let svg = backend.finish();
		assert_eq!(svg.matches("<clipPath").count(), 9);
		for label in ["A", "B", "C", "D"] {
			assert!(svg.contains(label), "missing panel label {label}");
		}
		let origins = [
			Point::new(0.0, 0.0),
			Point::new(406.0, 0.0),
			Point::new(0.0, 306.0),
			Point::new(406.0, 306.0),
		];
		let ticks: Vec<_> = origins
			.iter()
			.enumerate()
			.map(|(index, origin)| rendered_tick_paths(&svg, 5 + index, *origin))
			.collect();
		assert!(ticks.iter().all(|paths| !paths.is_empty()));
		assert!(ticks[1..].iter().all(|paths| paths == &ticks[0]));
		let outer = grid_outer_area(
			IcedRectangle::new(Point::ORIGIN, iced::Size::new(800.0, 600.0)),
			0.0,
		);
		for (index, ((position, layout), (x, y))) in layouts
			.iter()
			.zip([(0.0, 1.0), (1.0, 2.0), (2.0, 3.0), (3.0, 4.0)])
			.enumerate()
		{
			let cell_bounds =
				grid_cell_bounds(outer, GridArrangement::new(2, 2).unwrap(), *position);
			let inner = plot_area(cell_bounds, 0.0, &ResolvedPlotSettings::default());
			let expected = CoordinateTransformer::new(layout, inner).cartesian(x, y);
			let actual = rendered_single_mark(&svg, 1 + index);
			assert!((actual.x - (inner.x + expected.x)).abs() < 0.01);
			assert!((actual.y - (inner.y + expected.y)).abs() < 0.01);
		}
	}

	#[test]
	fn multi_cell_layers_draw_nothing_outside_clip_groups() {
		// iced_wgpu 0.14 z-orders a parent frame's direct fills ABOVE its
		// pasted `with_clip` children, so any parent-level draw in a
		// multi-cell layer paints over the cells on the live renderer. The
		// invariant that keeps backends equivalent is structural: every draw
		// in a multi-cell layer lives inside a clip group. SVG preserves
		// call order, so a parent-level draw shows up outside every <g>.
		let (body, _data, stamped) = fixture();
		let grid = PlotGrid::value_facet(
			body,
			&stamped,
			"facet",
			GridArrangement::new(1, 2).expect("arrangement"),
		)
		.expect("facet");
		let kernel = PlotGridKernel::from_grid(grid, 800, 400);
		let model = PlotSettings::default();
		let mut backend = SvgBackend::new(800.0, 400.0);
		let widget = PlotWidget {
			kernel: &kernel,
			title: String::new(),
			padding: 0.0,
			settings: model.resolve(),
			settings_model: model,
			render_revision: 0,
			resize_render_suspended: false,
			layer: PlotRenderLayer::Full,
		};
		widget.render(
			&mut backend,
			IcedRectangle::new(Point::ORIGIN, iced::Size::new(800.0, 400.0)),
		);
		let svg = backend.finish();
		let mut clip_depth = 0usize;
		let mut in_defs = false;
		for (index, fragment) in svg.split('<').enumerate().skip(1) {
			if fragment.starts_with("defs") {
				in_defs = true;
			} else if fragment.starts_with("/defs") {
				in_defs = false;
			} else if fragment.starts_with("g clip-path") {
				clip_depth += 1;
			} else if fragment.starts_with("/g") {
				clip_depth = clip_depth.saturating_sub(1);
			} else if clip_depth == 0
				&& !in_defs && (fragment.starts_with("path")
				|| fragment.starts_with("rect")
				|| fragment.starts_with("image"))
			{
				panic!("draw operation outside every clip group at fragment {index}: <{fragment}");
			}
		}
	}

	#[test]
	fn grid_rendering_is_invariant_to_bounds_origin() {
		let (body, _data, stamped) = fixture();
		let grid = PlotGrid::value_facet(
			body,
			&stamped,
			"facet",
			GridArrangement::new(1, 2).expect("arrangement"),
		)
		.expect("facet");
		let kernel = PlotGridKernel::from_grid(grid, 800, 400);
		let model = PlotSettings::default();
		let render_at = |origin: Point| {
			let mut backend = SvgBackend::new(800.0, 400.0);
			let widget = PlotWidget {
				kernel: &kernel,
				title: String::new(),
				padding: 0.0,
				settings: model.resolve(),
				settings_model: model.clone(),
				render_revision: 0,
				resize_render_suspended: false,
				layer: PlotRenderLayer::Full,
			};
			widget.render(
				&mut backend,
				IcedRectangle::new(origin, iced::Size::new(800.0, 400.0)),
			);
			backend.finish()
		};
		let at_origin = render_at(Point::ORIGIN);
		let at_panel = render_at(Point::new(500.0, 40.0));
		assert_eq!(
			at_origin, at_panel,
			"a widget-local frame must render identically wherever the panel sits"
		);
	}

	#[test]
	fn grid_hover_hits_cells_under_nonzero_bounds_origin() {
		let (body, _data, stamped) = fixture();
		let grid = PlotGrid::value_facet(
			body,
			&stamped,
			"facet",
			GridArrangement::new(1, 2).expect("arrangement"),
		)
		.expect("facet");
		let arrangement = grid.arrangement;
		let kernel = PlotGridKernel::from_grid(grid, 800, 400);
		let model = PlotSettings::default();
		let resolved = model.resolve();
		let layouts = kernel.cell_layouts(resolved.clone());
		let local = IcedRectangle::new(Point::ORIGIN, iced::Size::new(800.0, 400.0));
		let outer = grid_outer_area(local, 0.0);
		let (position, layout) = &layouts[0];
		let cell_bounds = grid_cell_bounds(outer, arrangement, *position);
		let inner = plot_area(cell_bounds, 0.0, &resolved);
		let transform = CoordinateTransformer::new(layout, inner);
		let p0 = transform.cartesian(0.0, 1.0);
		let p1 = transform.cartesian(2.0, 3.0);
		let cursor = Point::new(inner.x + (p0.x + p1.x) / 2.0, inner.y + (p0.y + p1.y) / 2.0);
		let panel = IcedRectangle::new(Point::new(500.0, 40.0), iced::Size::new(800.0, 400.0));
		let hover = PlotComposite::hover(&kernel, panel, 0.0, Cursor::Available(cursor), &model);
		assert!(
			hover.is_some(),
			"a widget-local cursor over cell A's line must hit regardless of panel origin"
		);
		// Cell kernels keep their legacy hover semantics (x-nearest for a
		// line), so the composite's own miss case is a cursor outside every
		// cell region, not one merely off the marks.
		let miss = PlotComposite::hover(
			&kernel,
			panel,
			0.0,
			Cursor::Available(Point::new(cursor.x, 401.0)),
			&model,
		);
		assert!(
			miss.is_none(),
			"a cursor outside every cell region must miss"
		);
	}

	#[test]
	fn value_facet_capacity_is_a_work_bound_with_no_truncation() {
		let (body, _data, stamped) = fixture();
		let admissible_values: Vec<String> = (0..128).map(|index| format!("v{index}")).collect();
		let admissible_x: Vec<f64> = (0..128).map(|index| index as f64).collect();
		let admissible_y = admissible_x.clone();
		let admissible = StampedResult {
			data: Arc::new(
				df!("facet" => admissible_values, "x" => admissible_x, "y" => admissible_y)
					.expect("admissible frame"),
			),
			provenance: stamped.provenance.clone(),
		};
		let admissible_grid = PlotGrid::value_facet(
			Arc::clone(&body),
			&admissible,
			"facet",
			GridArrangement::new(11, 12).expect("arrangement"),
		)
		.expect("hundreds of cells remain admissible");
		assert_eq!(admissible_grid.cells.len(), 128);
		let admissible_kernel = PlotGridKernel::from_grid(admissible_grid, 640, 480);
		let model = PlotSettings::default();
		let mut backend = SvgBackend::new(640.0, 480.0);
		let widget = PlotWidget {
			kernel: &admissible_kernel,
			title: String::new(),
			padding: 0.0,
			settings: model.resolve(),
			settings_model: model,
			render_revision: 0,
			resize_render_suspended: false,
			layer: PlotRenderLayer::Full,
		};
		widget.render(
			&mut backend,
			IcedRectangle::new(Point::ORIGIN, iced::Size::new(640.0, 480.0)),
		);
		assert_eq!(backend.finish().matches("<clipPath").count(), 257);
		let values: Vec<String> = (0..513).map(|index| format!("v{index}")).collect();
		let x: Vec<f64> = (0..513).map(|index| index as f64).collect();
		let y = x.clone();
		let data = Arc::new(df!("facet" => values, "x" => x, "y" => y).expect("large frame"));
		let stamped = StampedResult {
			data,
			provenance: stamped.provenance,
		};
		let arrangement = GridArrangement::new(23, 23).expect("arrangement");
		let error = match PlotGrid::value_facet(body, &stamped, "facet", arrangement) {
			Ok(_) => panic!("513 cells exceed the declared capacity"),
			Err(error) => error,
		};
		assert!(matches!(
			error,
			GridError::TooManyCells {
				count: 513,
				limit: MAX_GRID_CELLS,
				..
			}
		));
	}

	#[test]
	fn one_cell_grid_matches_the_ordinary_svg_path() {
		let (body, data, stamped) = fixture();
		let one_value = Arc::new(
			df!(
				"facet" => ["A", "A", "A", "A"],
				"x" => [0.0_f64, 1.0, 2.0, 3.0],
				"y" => [1.0_f64, 2.0, 3.0, 4.0]
			)
			.expect("one-value frame"),
		);
		let stamped = StampedResult {
			data: one_value,
			provenance: stamped.provenance,
		};
		let grid = PlotGrid::value_facet(
			Arc::clone(&body),
			&stamped,
			"facet",
			GridArrangement::new(1, 1).expect("arrangement"),
		)
		.expect("facet");
		let grid_kernel = PlotGridKernel::from_grid(grid, 400, 300);
		let ordinary = create_plot(&body, &data, 400, 300);
		let model = PlotSettings::default();
		let make_svg = |kernel: &dyn PlotKernel| {
			let mut backend = SvgBackend::new(400.0, 300.0);
			let widget = PlotWidget {
				kernel,
				title: "fallback title".to_owned(),
				padding: 0.0,
				settings: model.resolve(),
				settings_model: model.clone(),
				render_revision: 0,
				resize_render_suspended: false,
				layer: PlotRenderLayer::Full,
			};
			widget.render(
				&mut backend,
				IcedRectangle::new(Point::ORIGIN, iced::Size::new(400.0, 300.0)),
			);
			backend.finish()
		};
		assert_eq!(make_svg(ordinary.as_ref()), make_svg(&grid_kernel));
	}

	#[test]
	fn product_0042_facet_arrangement_is_wide_and_row_major() {
		assert_eq!(
			GridArrangement::for_facet_cell_count(1),
			GridArrangement {
				rows: 1,
				columns: 1
			}
		);
		assert_eq!(
			GridArrangement::for_facet_cell_count(3),
			GridArrangement {
				rows: 1,
				columns: 3
			}
		);
		assert_eq!(
			GridArrangement::for_facet_cell_count(4),
			GridArrangement {
				rows: 2,
				columns: 2
			}
		);
		assert_eq!(
			GridArrangement::for_facet_cell_count(5),
			GridArrangement {
				rows: 2,
				columns: 3
			}
		);
		assert_eq!(
			GridArrangement::for_facet_cell_count(7),
			GridArrangement {
				rows: 2,
				columns: 4
			}
		);
	}

	#[test]
	fn product_0042_faceted_bars_retain_their_values() {
		let data = Arc::new(
			df!(
				"region" => ["East", "East", "North", "North"],
				"product" => ["Atlas", "Delta", "Atlas", "Delta"],
				"revenue" => [141.2_f64, 58.4, 128.4, 51.6]
			)
			.expect("bar fixture"),
		);
		let resolved = plan(PlotRequest {
			source: PlotSource::Materialized {
				name: "bar facet fixture".to_owned(),
				data: Arc::clone(&data),
			},
			authored_spec: AuthoredPlotSpec::xy(
				PlotComposition::single(PlotType::Bar),
				"product",
				"revenue",
			),
		})
		.expect("bar resolves");
		let stamped = StampedResult {
			data,
			provenance: resolved.resolved_spec.provenance.clone(),
		};
		let grid =
			PlotGrid::value_facet_derived(Arc::new(resolved.resolved_spec), &stamped, "region")
				.expect("bar facet");
		let kernel = PlotGridKernel::from_grid(grid, 640, 480);
		let model = PlotSettings::default();
		let mut backend = SvgBackend::new(640.0, 480.0);
		let widget = PlotWidget {
			kernel: &kernel,
			title: String::new(),
			padding: 0.0,
			settings: model.resolve(),
			settings_model: model,
			render_revision: 0,
			resize_render_suspended: false,
			layer: PlotRenderLayer::Full,
		};
		widget.render(
			&mut backend,
			IcedRectangle::new(Point::ORIGIN, iced::Size::new(640.0, 480.0)),
		);
		let svg = backend.finish();
		assert!(
			svg.matches("<rect").count() >= 4,
			"faceted bars disappeared: {svg}"
		);
	}

	/// PRODUCT-0042 gate 10: this oracle is deliberately banked before the
	/// Rune head routes unfaceted figures through the grid path.
	#[test]
	fn bank_unfaceted_one_cell_parity_oracle() {
		one_cell_grid_matches_the_ordinary_svg_path();
	}
}
