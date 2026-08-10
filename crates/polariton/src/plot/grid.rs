//! PRODUCT-0041's internal plot-grid substrate.
//!
//! The grid is deliberately not a Rune surface.  PRODUCT-0042 will decide how
//! an author asks for one and who owns rows/columns; this module owns the
//! resolved cell model, the accepted value-facet producer, and the composite
//! renderer used by the test and future surface paths.

#![allow(dead_code)]

use crate::coordinate::{Cell, Plot, Pt, Rect, Widget};
use crate::plot::common::{
	AxisTextVisibility, CoordinateTransformer, PlotBackend, PlotComposite, PlotKernel, PlotLayout,
	PlotWidget, ResolvedPlotSettings,
};
use crate::plot::factory::{create_plot, create_styled_plot};
use crate::plot::spec::{
	Channel, LayerSettings, NonEmpty, ResolvedPlotSpec, ResolvedPresence, ScaleDomain,
	StampedResult,
};
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

#[derive(Clone, Copy)]
struct AxisTextCell {
	position: GridPosition,
	x_group: DomainGroupId,
	y_group: DomainGroupId,
}

fn axis_text_visibility(cell: AxisTextCell, cells: &[AxisTextCell]) -> AxisTextVisibility {
	let left = cell
		.position
		.column
		.checked_sub(1)
		.map(|column| GridPosition {
			row: cell.position.row,
			column,
		});
	let below = GridPosition {
		row: cell.position.row.saturating_add(1),
		column: cell.position.column,
	};
	AxisTextVisibility {
		y: !cells
			.iter()
			.any(|other| Some(other.position) == left && other.y_group == cell.y_group),
		x: !cells
			.iter()
			.any(|other| other.position == below && other.x_group == cell.x_group),
	}
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
		let shared = shared_layout(&local, &self.cells.first().cell.body);
		local
			.into_iter()
			.map(|(position, layout)| (position, merge_layout(layout, &shared)))
			.collect()
	}

	fn axis_text_cells(&self) -> Vec<AxisTextCell> {
		self.cells
			.iter()
			.map(|cell| AxisTextCell {
				position: cell.cell.position,
				x_group: cell.cell.x_group,
				y_group: cell.cell.y_group,
			})
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
		bounds: Rect<Widget>,
		padding: f32,
		title: &str,
		settings: &PlotSettings,
	) {
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
			helper.render_plot_layer(backend, bounds.into_iced());
			return;
		}
		let resolved = settings.resolve();
		// iced_wgpu 0.14 converts a frame's direct fills into meshes only at
		// frame completion, AFTER `with_clip` children have been pasted, so a
		// parent-level fill z-orders ABOVE every clipped child on the live
		// renderer even though SVG and tiny-skia preserve call order. No
		// parent-level draw may coexist with clipped children: the canvas
		// background is therefore emitted as the first clipped child itself.
		backend.with_clip(bounds.into_iced(), &mut |backend| {
			backend.fill_rectangle(
				Point::ORIGIN,
				bounds.size(),
				resolved.chrome.canvas_background.iced(),
			);
		});
		let figure_legend = crate::plot::legend::union(
			self.cells
				.iter()
				.filter_map(|cell| cell.kernel.legend_content(&resolved)),
		);
		let legend = crate::plot::legend::legend_layout(bounds, figure_legend.as_ref(), &resolved);
		let outer = grid_outer_area(legend.content_bounds, padding);
		let tiled = grid_tiled_area(outer, &resolved);
		let layouts = self.cell_layouts(resolved.clone());
		for (index, cell) in self.cells.iter().enumerate() {
			let (position, layout) = &layouts[index];
			let cell_bounds = grid_cell_bounds(tiled, self.arrangement, *position);
			let inner = grid_plot_area(cell_bounds, &resolved);
			let transform = CoordinateTransformer::new(layout, inner.into_iced());
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
			backend.with_clip(cell_bounds.into_iced(), &mut |backend| {
				backend.fill_rectangle(
					cell_bounds.origin().into_iced(),
					cell_bounds.size(),
					resolved.chrome.canvas_background.iced(),
				);
				backend.with_save(&mut |backend| {
					backend.translate(iced::Vector::new(inner.x(), inner.y()));
					helper.draw_layout_grid(backend, inner.into_iced(), &transform, layout);
					cell.kernel.plot_with_settings(
						backend,
						inner.into_iced(),
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
		bounds: Rect<Widget>,
		padding: f32,
		title: &str,
		settings: &PlotSettings,
	) {
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
			helper.render_overlay_layer(backend, bounds.into_iced());
			return;
		}
		let resolved = settings.resolve();
		let figure_legend = crate::plot::legend::union(
			self.cells
				.iter()
				.filter_map(|cell| cell.kernel.legend_content(&resolved)),
		);
		let legend = crate::plot::legend::legend_layout(bounds, figure_legend.as_ref(), &resolved);
		let outer = grid_outer_area(legend.content_bounds, padding);
		let tiled = grid_tiled_area(outer, &resolved);
		let layouts = self.cell_layouts(resolved.clone());
		let axis_cells = self.axis_text_cells();
		for (index, cell) in self.cells.iter().enumerate() {
			let (position, layout) = &layouts[index];
			let cell_bounds = grid_cell_bounds(tiled, self.arrangement, *position);
			let inner = grid_plot_area(cell_bounds, &resolved);
			let transform = CoordinateTransformer::new(layout, inner.into_iced());
			let axis_text = axis_text_visibility(axis_cells[index], &axis_cells);
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
			// Edge text deliberately extends out of its occupied slot: into the
			// shared gutters, or into the vacant below-slot of a partial row.
			// Keep the backend's composite draw inside a child clip while using
			// the figure bounds as that clip so the text is not orphaned.
			backend.with_clip(bounds.into_iced(), &mut |backend| {
				backend.with_save(&mut |backend| {
					backend.translate(iced::Vector::new(inner.x(), inner.y()));
					match layout {
						PlotLayout::Cartesian {
							x_range, y_range, ..
						} => {
							helper.draw_cartesian_axes(
								backend,
								inner.into_iced(),
								&transform,
								*x_range,
								*y_range,
								axis_text,
							);
						}
						PlotLayout::CategoricalX {
							categories,
							y_range,
						} => {
							helper.draw_categorical_axes(
								backend,
								inner.into_iced(),
								&transform,
								categories,
								*y_range,
								axis_text,
							);
						}
						PlotLayout::CategoricalY {
							categories,
							x_range,
						} => {
							helper.draw_categorical_y_axes(
								backend,
								inner.into_iced(),
								&transform,
								categories,
								*x_range,
								axis_text,
							);
						}
						PlotLayout::CategoricalXY {
							x_categories,
							y_categories,
						} => {
							helper.draw_categorical_xy_axes(
								backend,
								inner.into_iced(),
								&transform,
								x_categories,
								y_categories,
								axis_text,
							);
						}
						PlotLayout::Parallel { dimensions, ranges } => {
							helper.draw_parallel_axes(
								backend,
								inner.into_iced(),
								&transform,
								dimensions,
								ranges,
								axis_text,
							);
						}
						PlotLayout::Radial => {}
					}
				});
				if let Some(label) = &cell.cell.label {
					backend.fill_text(iced::widget::canvas::Text {
						content: label.clone(),
						// A facet label is a per-cell heading, not a corner annotation.
						// Center it over the actual plot area so it reads like a
						// miniature plot title while leaving the shared figure heading
						// independent.
						position: Point::new(
							inner.x() + inner.width() / 2.0,
							cell_bounds.y() + PANEL_LABEL_TOP_INSET,
						),
						color: resolved.chrome.title_text.iced(),
						size: iced::Pixels((resolved.legend_size + 2.0).max(10.0)),
						align_x: iced::alignment::Horizontal::Center.into(),
						align_y: iced::alignment::Vertical::Top,
						..Default::default()
					});
				}
			});
		}
		let shared_extent = grid_plot_extent(tiled, self.arrangement, &resolved);
		let first = self.cells.first();
		let helper = PlotWidget {
			kernel: first.kernel.as_ref(),
			title: String::new(),
			padding: 0.0,
			settings: resolved.clone(),
			settings_model: settings.clone(),
			render_revision: 0,
			resize_render_suspended: false,
			layer: crate::plot::common::PlotRenderLayer::Full,
		};
		helper.draw_axis_labels(backend, shared_extent.into_iced());
		if let (Some(content), Some(origin)) =
			(figure_legend.as_ref(), legend.origin(shared_extent))
		{
			crate::plot::legend::render_legend(backend, origin, content, &resolved);
		}
	}

	fn hover(
		&self,
		bounds: Rect<Widget>,
		padding: f32,
		cursor: Pt<Widget>,
		settings: &PlotSettings,
	) -> Option<String> {
		let resolved = settings.resolve();
		let figure_legend = crate::plot::legend::union(
			self.cells
				.iter()
				.filter_map(|cell| cell.kernel.legend_content(&resolved)),
		);
		let legend = crate::plot::legend::legend_layout(bounds, figure_legend.as_ref(), &resolved);
		let outer = grid_outer_area(legend.content_bounds, padding);
		let tiled = grid_tiled_area(outer, &resolved);
		let layouts = self.cell_layouts(resolved.clone());
		for (index, cell) in self.cells.iter().enumerate() {
			let (position, layout) = &layouts[index];
			let cell_bounds = grid_cell_bounds(tiled, self.arrangement, *position);
			if !cell_bounds.contains(cursor) {
				continue;
			}
			// Cell localization is explicit even though the legacy kernel seam
			// below consumes the still-more-local Plot point.
			let _cell_cursor = Pt::<Cell>::from_widget(cursor, cell_bounds);
			let inner = grid_plot_area(cell_bounds, &resolved);
			let transform = CoordinateTransformer::new(layout, inner.into_iced());
			let local = Pt::<Plot>::from_widget(cursor, inner);
			return cell.kernel.hover_with_settings(
				&transform,
				Cursor::Available(local.into_iced()),
				settings,
			);
		}
		None
	}
}

fn plot_area(bounds: Rect<Widget>, padding: f32, settings: &ResolvedPlotSettings) -> Rect<Widget> {
	let left = padding + settings.plot_padding_left;
	let right = padding + settings.plot_padding_right;
	let top = padding + settings.plot_padding_top;
	let bottom = padding + settings.plot_padding_bottom;
	bounds.region(
		bounds.x() + left,
		bounds.y() + top,
		(bounds.width() - left - right).max(0.0),
		(bounds.height() - top - bottom).max(0.0),
	)
}

fn grid_outer_area(bounds: Rect<Widget>, padding: f32) -> Rect<Widget> {
	bounds.region(
		bounds.x() + padding,
		bounds.y() + padding,
		(bounds.width() - 2.0 * padding).max(0.0),
		(bounds.height() - 2.0 * padding).max(0.0),
	)
}

fn grid_tiled_area(outer: Rect<Widget>, settings: &ResolvedPlotSettings) -> Rect<Widget> {
	let left = settings.plot_padding_left.min(outer.width());
	let bottom = settings.plot_padding_bottom.min(outer.height());
	let top = grid_heading_band(settings).min((outer.height() - bottom).max(0.0));
	outer.region(
		outer.x() + left,
		outer.y() + top,
		(outer.width() - left).max(0.0),
		(outer.height() - top - bottom).max(0.0),
	)
}

const PANEL_LABEL_TOP_INSET: f32 = 8.0;

fn grid_heading_band(settings: &ResolvedPlotSettings) -> f32 {
	let title = if matches!(settings.title, ResolvedPresence::Hide) {
		0.0
	} else {
		settings.plot_padding_top
	};
	let subtitle = if matches!(settings.subtitle, ResolvedPresence::Show(_)) {
		settings.subtitle_offset + settings.subtitle_size
	} else {
		0.0
	};
	title.max(subtitle)
}

fn grid_plot_area(cell_bounds: Rect<Widget>, settings: &ResolvedPlotSettings) -> Rect<Widget> {
	let top = settings.plot_padding_top.min(cell_bounds.height());
	let right = settings.plot_padding_right.min(cell_bounds.width());
	cell_bounds.region(
		cell_bounds.x(),
		cell_bounds.y() + top,
		(cell_bounds.width() - right).max(0.0),
		(cell_bounds.height() - top).max(0.0),
	)
}

fn grid_plot_extent(
	tiled: Rect<Widget>,
	arrangement: GridArrangement,
	settings: &ResolvedPlotSettings,
) -> Rect<Widget> {
	let first = grid_plot_area(
		grid_cell_bounds(tiled, arrangement, GridPosition { row: 0, column: 0 }),
		settings,
	);
	let last = grid_plot_area(
		grid_cell_bounds(
			tiled,
			arrangement,
			GridPosition {
				row: arrangement.rows.saturating_sub(1),
				column: arrangement.columns.saturating_sub(1),
			},
		),
		settings,
	);
	tiled.region(
		first.x(),
		first.y(),
		(last.right() - first.x()).max(0.0),
		(last.bottom() - first.y()).max(0.0),
	)
}

pub(crate) fn grid_cell_bounds(
	outer: Rect<Widget>,
	arrangement: GridArrangement,
	position: GridPosition,
) -> Rect<Widget> {
	let columns = arrangement.columns.max(1) as f32;
	let rows = arrangement.rows.max(1) as f32;
	let gap_x = GRID_GAP.min(outer.width() / columns.max(1.0));
	let gap_y = GRID_GAP.min(outer.height() / rows.max(1.0));
	let width = (outer.width() - gap_x * (columns - 1.0)).max(0.0) / columns;
	let height = (outer.height() - gap_y * (rows - 1.0)).max(0.0) / rows;
	outer.region(
		outer.x() + position.column as f32 * (width + gap_x),
		outer.y() + position.row as f32 * (height + gap_y),
		width,
		height,
	)
}

fn resolved_categories(body: &ResolvedPlotSpec, channel: Channel) -> Vec<String> {
	let resolved = body
		.channel(channel)
		.unwrap_or_else(|| panic!("categorical grid has no resolved {channel} channel"));
	let ScaleDomain::Categorical(categories) = &resolved.domain else {
		panic!("categorical grid's resolved {channel} channel is not categorical")
	};
	categories.clone()
}

fn shared_layout(
	local: &[(GridPosition, PlotLayout)],
	body: &ResolvedPlotSpec,
) -> Option<PlotLayout> {
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
		PlotLayout::CategoricalX { .. } => {
			let mut y = (f64::INFINITY, f64::NEG_INFINITY);
			for (_, layout) in local {
				if let PlotLayout::CategoricalX { y_range, .. } = layout {
					y.0 = y.0.min(y_range.0);
					y.1 = y.1.max(y_range.1);
				}
			}
			Some(PlotLayout::CategoricalX {
				categories: resolved_categories(body, Channel::X),
				y_range: y,
			})
		}
		PlotLayout::CategoricalY { .. } => {
			let mut x = (f64::INFINITY, f64::NEG_INFINITY);
			for (_, layout) in local {
				if let PlotLayout::CategoricalY { x_range, .. } = layout {
					x.0 = x.0.min(x_range.0);
					x.1 = x.1.max(x_range.1);
				}
			}
			Some(PlotLayout::CategoricalY {
				categories: resolved_categories(body, Channel::Y),
				x_range: x,
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
		(
			PlotLayout::CategoricalX { .. },
			Some(PlotLayout::CategoricalX {
				categories,
				y_range,
			}),
		) => PlotLayout::CategoricalX {
			categories: categories.clone(),
			y_range: *y_range,
		},
		(
			PlotLayout::CategoricalY { .. },
			Some(PlotLayout::CategoricalY {
				categories,
				x_range,
			}),
		) => PlotLayout::CategoricalY {
			categories: categories.clone(),
			x_range: *x_range,
		},
		(layout, _) => layout,
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::coordinate::Window;
	use crate::plot::common::{
		AxisType, PathBuilder, PlotBackend, PlotRenderLayer, PlotWidget, TickPlacement,
	};
	use crate::plot::composition::PlotComposition;
	use crate::plot::core::PlotType;
	use crate::plot::export::SvgBackend;
	use crate::plot::request::{PlotRequest, PlotSource, plan};
	use crate::plot::spec::{AuthoredPlotSpec, Channel, PlotProvenance};
	use iced::widget::canvas::{Stroke, Text};
	use iced::{Color, Size};
	use polars::df;

	#[derive(Default)]
	/// PRODUCT-0057 named oracle `plot::grid::tests::RectangleBackend` records
	/// rectangle tuples and fill-path points only; its shared short name grants
	/// no compositing, clipping, antialiasing, or text-metric license.
	struct RectangleBackend {
		rectangles: Vec<(Point, Size, Color)>,
		paths: Vec<Vec<Point>>,
	}

	#[derive(Default)]
	struct PointPathBuilder {
		points: Vec<Point>,
	}

	impl PathBuilder for PointPathBuilder {
		fn move_to(&mut self, point: Point) {
			self.points.push(point);
		}

		fn line_to(&mut self, point: Point) {
			self.points.push(point);
		}

		fn arc_to(&mut self, center: Point, _radius: f32, _start_angle: f32, _end_angle: f32) {
			self.points.push(center);
		}

		fn circle(&mut self, center: Point, _radius: f32) {
			self.points.push(center);
		}

		fn rectangle(&mut self, top_left: Point, size: Size) {
			self.points.push(top_left);
			self.points.push(Point::new(
				top_left.x + size.width,
				top_left.y + size.height,
			));
		}

		fn close(&mut self) {}
	}

	impl PlotBackend for RectangleBackend {
		fn stroke_path(&mut self, _f: &dyn Fn(&mut dyn PathBuilder), _stroke: Stroke) {}

		fn fill_path(&mut self, f: &dyn Fn(&mut dyn PathBuilder), _color: Color) {
			let mut builder = PointPathBuilder::default();
			f(&mut builder);
			self.paths.push(builder.points);
		}

		fn fill_rectangle(&mut self, top_left: Point, size: Size, color: Color) {
			self.rectangles.push((top_left, size, color));
		}

		fn fill_text(&mut self, _text: Text) {}

		fn translate(&mut self, _translation: iced::Vector) {}

		fn rotate(&mut self, _angle: f32) {}

		fn with_save(&mut self, f: &mut dyn FnMut(&mut dyn PlotBackend)) {
			f(self);
		}

		fn with_clip(&mut self, _bounds: IcedRectangle, f: &mut dyn FnMut(&mut dyn PlotBackend)) {
			f(self);
		}
	}

	#[derive(Clone, Copy, Debug, PartialEq)]
	struct StrataOperation {
		kind: StrataOperationKind,
		clip: Option<IcedRectangle>,
	}

	#[derive(Clone, Copy, Debug, PartialEq)]
	enum StrataOperationKind {
		Fill(Color),
		Stroke(Color),
	}

	#[derive(Default)]
	struct StrataBackend {
		operations: Vec<StrataOperation>,
		clips: Vec<IcedRectangle>,
	}

	impl StrataBackend {
		fn record(&mut self, kind: StrataOperationKind) {
			self.operations.push(StrataOperation {
				kind,
				clip: self.clips.last().copied(),
			});
		}
	}

	impl PlotBackend for StrataBackend {
		fn stroke_path(&mut self, _f: &dyn Fn(&mut dyn PathBuilder), stroke: Stroke) {
			let color = match stroke.style {
				iced::widget::canvas::Style::Solid(color) => color,
				iced::widget::canvas::Style::Gradient(_) => {
					panic!("PRODUCT-0068 strata fixture expects solid strokes")
				}
			};
			self.record(StrataOperationKind::Stroke(color));
		}

		fn fill_path(&mut self, _f: &dyn Fn(&mut dyn PathBuilder), color: Color) {
			self.record(StrataOperationKind::Fill(color));
		}

		fn fill_rectangle(&mut self, _top_left: Point, _size: Size, color: Color) {
			self.record(StrataOperationKind::Fill(color));
		}

		fn fill_text(&mut self, _text: Text) {}

		fn translate(&mut self, _translation: iced::Vector) {}

		fn rotate(&mut self, _angle: f32) {}

		fn with_save(&mut self, f: &mut dyn FnMut(&mut dyn PlotBackend)) {
			f(self);
		}

		fn with_clip(&mut self, bounds: IcedRectangle, f: &mut dyn FnMut(&mut dyn PlotBackend)) {
			self.clips.push(bounds);
			f(self);
			self.clips.pop();
		}
	}

	struct Product0068CellKernel {
		mark: Color,
	}

	impl PlotKernel for Product0068CellKernel {
		fn layout(&self, _settings: ResolvedPlotSettings) -> PlotLayout {
			PlotLayout::Cartesian {
				x_range: (0.0, 1.0),
				y_range: (0.0, 1.0),
				x_axis_type: AxisType::Linear,
				y_axis_type: AxisType::Linear,
			}
		}

		fn plot(
			&self,
			backend: &mut dyn PlotBackend,
			_bounds: IcedRectangle,
			_transform: &CoordinateTransformer,
			_cursor: Cursor,
			_settings: ResolvedPlotSettings,
		) {
			backend.fill_rectangle(Point::new(10.0, 10.0), Size::new(20.0, 20.0), self.mark);
		}

		fn hover(&self, _transform: &CoordinateTransformer, _cursor: Cursor) -> Option<String> {
			None
		}
	}

	fn widget_rect(raw: IcedRectangle) -> Rect<Widget> {
		Rect::<Widget>::from_iced_frame(raw).region(raw.x, raw.y, raw.width, raw.height)
	}

	fn widget_point(raw: Point) -> Pt<Widget> {
		let window =
			Rect::<Window>::from_iced_layout(IcedRectangle::new(Point::ORIGIN, iced::Size::ZERO));
		Pt::<Widget>::from_window(Pt::<Window>::from_iced_event(raw), window)
	}

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

	fn rendered_facet(
		count: usize,
		arrangement: GridArrangement,
		width: f32,
		height: f32,
	) -> (PlotGridKernel, String) {
		let facets: Vec<String> = (0..count).map(|index| format!("panel-{index}")).collect();
		let x: Vec<f64> = (0..count).map(|index| index as f64).collect();
		let y: Vec<f64> = (0..count).map(|index| index as f64 + 10.0).collect();
		let data = Arc::new(df!("facet" => facets, "x" => x, "y" => y).expect("facet frame"));
		let resolved = plan(PlotRequest {
			source: PlotSource::Materialized {
				name: "shared-edge fixture".to_owned(),
				data: Arc::clone(&data),
			},
			authored_spec: AuthoredPlotSpec::xy(PlotComposition::single(PlotType::Line), "x", "y"),
		})
		.expect("facet resolves");
		let grid = PlotGrid::value_facet(
			Arc::new(resolved.resolved_spec),
			&StampedResult {
				data,
				provenance: PlotProvenance {
					source_name: "shared-edge fixture".to_owned(),
					revision: None,
				},
			},
			"facet",
			arrangement,
		)
		.expect("facet grid");
		let kernel = PlotGridKernel::from_grid(grid, width as u32, height as u32);
		let model = PlotSettings::default();
		let mut backend = SvgBackend::new(width, height);
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
			IcedRectangle::new(Point::ORIGIN, iced::Size::new(width, height)),
		);
		(kernel, backend.finish())
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

	fn clipped_text(svg: &str, clip_id: usize) -> Vec<String> {
		let marker = format!(r#"<g clip-path="url(#clip_{clip_id})">"#);
		let body = svg
			.split_once(&marker)
			.and_then(|(_, rest)| rest.split_once("</g>"))
			.map(|(body, _)| body)
			.expect("overlay clip body");
		body.split("<text ")
			.skip(1)
			.filter_map(|fragment| {
				fragment
					.split_once('>')
					.and_then(|(_, text)| text.split_once("</text>"))
					.map(|(text, _)| text.to_owned())
			})
			.collect()
	}

	fn text_element<'a>(svg: &'a str, content: &str) -> &'a str {
		svg.split("<text ")
			.skip(1)
			.find(|fragment| fragment.contains(&format!(">{content}</text>")))
			.unwrap_or_else(|| panic!("missing text element `{content}`"))
	}

	fn text_transform_translation(element: &str) -> Point {
		let coefficients = element
			.split_once("matrix(")
			.and_then(|(_, rest)| rest.split_once(')'))
			.map(|(matrix, _)| matrix)
			.expect("text transform matrix")
			.split_whitespace()
			.map(|value| value.parse::<f32>().expect("matrix coefficient"))
			.collect::<Vec<_>>();
		assert_eq!(coefficients.len(), 6);
		Point::new(coefficients[4], coefficients[5])
	}

	fn text_coordinate(element: &str, axis: char) -> f32 {
		let marker = format!(r#"{axis}=""#);
		element
			.split_once(&marker)
			.and_then(|(_, rest)| rest.split_once('"'))
			.map(|(value, _)| value.parse::<f32>().expect("text coordinate"))
			.expect("text coordinate attribute")
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

	struct LegendOnlyKernel {
		content: crate::plot::legend::LegendContent,
	}

	impl PlotKernel for LegendOnlyKernel {
		fn layout(&self, _settings: ResolvedPlotSettings) -> PlotLayout {
			PlotLayout::Cartesian {
				x_range: (0.0, 1.0),
				y_range: (0.0, 1.0),
				x_axis_type: crate::plot::common::AxisType::Linear,
				y_axis_type: crate::plot::common::AxisType::Linear,
			}
		}

		fn plot(
			&self,
			_backend: &mut dyn PlotBackend,
			_bounds: IcedRectangle,
			_transform: &CoordinateTransformer,
			_cursor: Cursor,
			_settings: ResolvedPlotSettings,
		) {
		}

		fn legend_content(
			&self,
			_settings: &ResolvedPlotSettings,
		) -> Option<crate::plot::legend::LegendContent> {
			Some(self.content.clone())
		}

		fn hover(&self, _transform: &CoordinateTransformer, _cursor: Cursor) -> Option<String> {
			None
		}

		fn x_label(&self) -> String {
			"x".to_owned()
		}

		fn y_label(&self) -> String {
			"y".to_owned()
		}
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
	fn product_0044_grid_renders_one_figure_legend_from_cell_union() {
		let (body, _data, stamped) = fixture();
		// Mirror test16.rn's four product facets so the placement gate covers
		// the exhibit that triggered the reserved gutter.
		let stamped = StampedResult {
			data: Arc::new(
				df!(
					"facet" => ["Atlas", "Beacon", "Cinder", "Delta"],
					"x" => [0.0_f64, 1.0, 2.0, 3.0],
					"y" => [1.0_f64, 2.0, 3.0, 4.0]
				)
				.expect("test16-shaped fixture"),
			),
			provenance: stamped.provenance,
		};
		let grid = PlotGrid::value_facet(
			body,
			&stamped,
			"facet",
			GridArrangement::new(2, 2).expect("arrangement"),
		)
		.expect("facet");
		let kernel = PlotGridKernel::from_grid_with_factory(grid, 400, 300, |_spec, data| {
			let cell = data
				.column("facet")
				.expect("facet column")
				.get(0)
				.expect("facet value")
				.to_string()
				.trim_matches('"')
				.to_owned();
			let mut entries = NonEmpty::new(crate::plot::legend::LegendEntry {
				label: "shared".to_owned(),
				swatch: iced::Color::from_rgb8(10, 20, 30),
				sample: crate::plot::legend::LegendSample::PointSample,
				provenance: crate::plot::legend::LegendProvenance::Named,
			});
			entries.push(crate::plot::legend::LegendEntry {
				label: format!("series-{cell}"),
				swatch: iced::Color::from_rgb8(40, 50, 60),
				sample: crate::plot::legend::LegendSample::PointSample,
				provenance: crate::plot::legend::LegendProvenance::Named,
			});
			Ok(Arc::new(LegendOnlyKernel {
				content: crate::plot::legend::LegendContent::entries(entries),
			}))
		})
		.expect("grid kernel");
		let model = PlotSettings::default();
		let resolved = model.resolve();
		let mut backend = SvgBackend::new(400.0, 300.0);
		let widget = PlotWidget {
			kernel: &kernel,
			title: String::new(),
			padding: 0.0,
			settings: resolved.clone(),
			settings_model: model,
			render_revision: 0,
			resize_render_suspended: false,
			layer: PlotRenderLayer::Full,
		};
		widget.render(
			&mut backend,
			IcedRectangle::new(Point::ORIGIN, iced::Size::new(400.0, 300.0)),
		);
		let svg = backend.finish();
		assert_eq!(svg.matches("fill-opacity=\"0.60\"").count(), 1, "{svg}");
		assert_eq!(svg.matches(">shared</text>").count(), 1, "{svg}");
		for product in ["Atlas", "Beacon", "Cinder", "Delta"] {
			assert_eq!(
				svg.matches(&format!(">series-{product}</text>")).count(),
				1,
				"{svg}"
			);
		}

		let content = crate::plot::legend::union(
			kernel
				.cells
				.iter()
				.filter_map(|cell| cell.kernel.legend_content(&resolved)),
		)
		.expect("figure legend");
		let bounds = widget_rect(IcedRectangle::new(
			Point::ORIGIN,
			iced::Size::new(400.0, 300.0),
		));
		let legend = crate::plot::legend::legend_layout(bounds, Some(&content), &resolved);
		let tiled = grid_tiled_area(grid_outer_area(legend.content_bounds, 0.0), &resolved);
		let shared_extent = grid_plot_extent(tiled, kernel.arrangement, &resolved);
		let legend_origin = legend.origin(shared_extent).expect("default gutter origin");
		let cell_plots = kernel
			.cells
			.iter()
			.map(|cell| {
				grid_plot_area(
					grid_cell_bounds(tiled, kernel.arrangement, cell.cell.position),
					&resolved,
				)
			})
			.collect::<Vec<_>>();
		assert!(
			cell_plots
				.iter()
				.all(|plot| plot.right() <= legend_origin.x()),
			"the figure legend must begin to the right of every cell plot"
		);
		assert_eq!(
			legend_origin.y(),
			shared_extent.y(),
			"the default figure legend must use the panel-aware plot extent"
		);
		assert!(
			shared_extent.y() > tiled.y(),
			"the authoritative extent includes per-cell top space"
		);
		assert!(
			cell_plots
				.windows(2)
				.all(|pair| pair[0].width() == pair[1].width()
					&& pair[0].height() == pair[1].height()),
			"the figure gutter must preserve cell congruence"
		);
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
		let outer = widget_rect(outer);
		let left = grid_cell_bounds(outer, arrangement, GridPosition { row: 0, column: 0 });
		let right = grid_cell_bounds(outer, arrangement, GridPosition { row: 0, column: 1 });
		assert!(left.right() <= right.x());

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
	fn product_0047_faceted_series_cells_keep_their_own_stable_colors() {
		// PRODUCT-0041 §11 with PRODUCT-0029 §1: faceting by the series field
		// gives every cell one series, and each cell paints that series' stable
		// assignment. Cells share one resolved body whose assignments span the
		// whole domain, so the colour is panel-blind by construction — never a
		// per-cell re-derivation and never a render-index ramp sample.
		use crate::plot::spec::{
			CategoricalPaletteSelection, ColorIdentity, resolve_channels,
			resolve_domains_with_categorical_palette,
		};

		let frame = df!(
			"cat" => ["A", "B", "A", "B", "A", "B", "A", "B"],
			"series" => ["w", "w", "x", "x", "y", "y", "z", "z"],
			"val" => [1.0_f64, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]
		)
		.expect("faceted series frame");
		let authored = AuthoredPlotSpec::xy_color(
			PlotComposition::single(PlotType::Bar),
			"cat",
			"val",
			Some("series".to_owned()),
		);
		let binding = resolve_channels(&authored, frame.schema().as_ref()).expect("channels");
		let stamped = StampedResult {
			data: Arc::new(frame.clone()),
			provenance: PlotProvenance {
				source_name: "product-0047".to_owned(),
				revision: Some(1),
			},
		};
		let selection = CategoricalPaletteSelection::from_theme_name("tab20c").expect("palette");
		let (resolved, _) = resolve_domains_with_categorical_palette(
			binding,
			&stamped,
			&ColorIdentity::default(),
			&selection,
		)
		.expect("series bar resolution");
		let assignments = resolved
			.channel(Channel::Color)
			.expect("authored channel")
			.colors
			.clone();
		assert_eq!(assignments.len(), 4, "the domain spans every cell");

		let grid = PlotGrid::value_facet(
			Arc::new(resolved),
			&stamped,
			"series",
			GridArrangement::new(2, 2).expect("arrangement"),
		)
		.expect("faceted grid");
		let kernel = PlotGridKernel::from_grid(grid, 800, 600);
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

		for assignment in &assignments {
			let fill = format!(
				"fill=\"rgb({}, {}, {})\"",
				assignment.color.red, assignment.color.green, assignment.color.blue
			);
			assert!(
				svg.contains(&fill),
				"cell {} must paint its own stable {fill}",
				assignment.category
			);
		}
	}

	#[test]
	fn product_0055_faceted_horizontal_bars_keep_shared_domains_colors_and_one_legend() {
		use crate::plot::legend::LegendSection;
		use crate::plot::spec::{
			CategoricalPaletteSelection, ColorIdentity, resolve_channels,
			resolve_domains_with_categorical_palette,
		};

		let frame = df!(
			"series" => ["Atlas", "Atlas", "Beacon", "Beacon", "Cinder", "Cinder", "Delta", "Delta"],
			"category" => ["East", "West", "East", "West", "East", "West", "East", "West"],
			"value" => [2.0_f64, 4.0, 3.0, 5.0, 4.0, 6.0, 5.0, 7.0]
		)
		.unwrap();
		let authored = AuthoredPlotSpec::xy_color(
			PlotComposition::single(PlotType::Bar),
			"value",
			"category",
			Some("series".to_owned()),
		);
		let binding = resolve_channels(&authored, frame.schema().as_ref()).unwrap();
		let stamped = StampedResult {
			data: Arc::new(frame.clone()),
			provenance: PlotProvenance {
				source_name: "product-0055".to_owned(),
				revision: Some(1),
			},
		};
		let selection = CategoricalPaletteSelection::from_theme_name("tab20c").unwrap();
		let (resolved, _) = resolve_domains_with_categorical_palette(
			binding,
			&stamped,
			&ColorIdentity::default(),
			&selection,
		)
		.unwrap();
		assert_eq!(
			resolved.composition.primary_plot_type(),
			PlotType::HorizontalBar
		);
		let assignments = resolved.channel(Channel::Color).unwrap().colors.clone();
		let grid = PlotGrid::value_facet(
			Arc::new(resolved),
			&stamped,
			"series",
			GridArrangement::new(2, 2).unwrap(),
		)
		.unwrap();
		let kernel = PlotGridKernel::from_grid(grid, 800, 600);
		let settings = ResolvedPlotSettings::default();
		assert!(kernel.cells.iter().all(|cell| matches!(
			cell.kernel.layout(settings.clone()),
			PlotLayout::CategoricalY { .. }
		)));
		let legend = crate::plot::legend::union(
			kernel
				.cells
				.iter()
				.filter_map(|cell| cell.kernel.legend_content(&settings)),
		)
		.expect("one figure legend");
		let LegendSection::Entries(entries) = legend.0.first() else {
			panic!("horizontal grid must union named swatches");
		};
		assert_eq!(entries.len(), assignments.len());
		for (entry, assignment) in entries.iter().zip(&assignments) {
			assert_eq!(entry.label, assignment.category);
			assert_eq!(entry.sample, crate::plot::legend::LegendSample::Swatch);
			assert_eq!(
				entry.swatch,
				iced::Color::from_rgba8(
					assignment.color.red,
					assignment.color.green,
					assignment.color.blue,
					assignment.color.alpha as f32 / u8::MAX as f32,
				)
			);
		}

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
		for assignment in assignments {
			let fill = format!(
				"fill=\"rgb({}, {}, {})\"",
				assignment.color.red, assignment.color.green, assignment.color.blue
			);
			assert!(svg.contains(&fill), "missing panel-blind {fill}");
		}
	}

	#[test]
	fn product_0065_categorical_x_shares_ragged_domain_and_payload_slots() {
		let data = Arc::new(
			df!(
				"facet" => ["A", "B", "A", "B"],
				"category" => ["X", "Y", "Z", "X"],
				"value" => [2.0_f64, 20.0, 3.0, 2.0],
			)
			.unwrap(),
		);
		let resolved = plan(PlotRequest {
			source: PlotSource::Materialized {
				name: "product-0065-x".into(),
				data: Arc::clone(&data),
			},
			authored_spec: AuthoredPlotSpec::xy(
				PlotComposition::single(PlotType::Bar),
				"category",
				"value",
			),
		})
		.unwrap();
		let body = Arc::new(resolved.resolved_spec);
		let expected_categories = match &body.channel(Channel::X).unwrap().domain {
			crate::plot::spec::ScaleDomain::Categorical(categories) => categories.clone(),
			_ => panic!("resolved bar category domain"),
		};
		assert_eq!(expected_categories, ["X", "Y", "Z"]);
		let grid = PlotGrid::value_facet(
			Arc::clone(&body),
			&StampedResult {
				data,
				provenance: PlotProvenance {
					source_name: "product-0065-x".into(),
					revision: Some(1),
				},
			},
			"facet",
			GridArrangement::new(1, 2).unwrap(),
		)
		.unwrap();
		let kernel = PlotGridKernel::from_grid(grid, 600, 240);
		let settings = ResolvedPlotSettings::default();
		let local = kernel
			.cells
			.iter()
			.map(|cell| cell.kernel.layout(settings.clone()))
			.collect::<Vec<_>>();
		let local_domains = local
			.iter()
			.map(|layout| match layout {
				PlotLayout::CategoricalX {
					categories,
					y_range,
				} => (categories.clone(), *y_range),
				_ => panic!("vertical bar cell"),
			})
			.collect::<Vec<_>>();
		assert_ne!(
			local_domains[0].0, local_domains[1].0,
			"the fixture must contain unequal local category domains"
		);
		assert_ne!(
			local_domains[0].1, local_domains[1].1,
			"the fixture must contain unequal local numeric ranges"
		);
		assert!(
			local_domains
				.iter()
				.all(|(categories, _)| categories.len() < 3),
			"every partition deliberately omits a category"
		);
		let expected_range = (
			local_domains
				.iter()
				.map(|(_, range)| range.0)
				.fold(f64::INFINITY, f64::min),
			local_domains
				.iter()
				.map(|(_, range)| range.1)
				.fold(f64::NEG_INFINITY, f64::max),
		);
		let merged = kernel.cell_layouts(settings.clone());
		for (_, layout) in &merged {
			let PlotLayout::CategoricalX {
				categories,
				y_range,
			} = layout
			else {
				panic!("merged categorical x")
			};
			assert_eq!(categories, &expected_categories);
			assert_eq!(*y_range, expected_range);
		}

		let bounds = IcedRectangle::new(Point::ORIGIN, Size::new(300.0, 180.0));
		let placement = TickPlacement::count(4).unwrap();
		let major = placement.positions(expected_range).unwrap();
		let minor = placement.minor_positions(&major, 2).unwrap();
		let reference_transform = CoordinateTransformer::new(&merged[0].1, bounds);
		let reference_ticks = major
			.iter()
			.chain(&minor)
			.map(|value| reference_transform.categorical(0, *value).0.y)
			.collect::<Vec<_>>();
		for (_, layout) in merged.iter().skip(1) {
			let transform = CoordinateTransformer::new(layout, bounds);
			assert_eq!(
				major
					.iter()
					.chain(&minor)
					.map(|value| transform.categorical(0, *value).0.y)
					.collect::<Vec<_>>(),
				reference_ticks,
				"major and minor numeric tick pixels are cell-congruent"
			);
		}
		let mut shared_x_top = Vec::new();
		for ((cell, local_layout), (_, merged_layout)) in
			kernel.cells.iter().zip(&local).zip(&merged)
		{
			let PlotLayout::CategoricalX {
				categories: local_categories,
				..
			} = local_layout
			else {
				unreachable!()
			};
			let transform = CoordinateTransformer::new(merged_layout, bounds);
			let mut backend = RectangleBackend::default();
			cell.kernel.plot(
				&mut backend,
				bounds,
				&transform,
				Cursor::Unavailable,
				settings.clone(),
			);
			assert_eq!(backend.rectangles.len(), local_categories.len());
			for ((top_left, size, _), category) in backend.rectangles.iter().zip(local_categories) {
				let shared_index = expected_categories
					.iter()
					.position(|candidate| candidate == category)
					.unwrap();
				assert_eq!(
					top_left.x + size.width / 2.0,
					transform.categorical(shared_index, 0.0).0.x,
					"{category} payload occupies the band carrying its label"
				);
				if category == "X" {
					shared_x_top.push(top_left.y);
				}
			}
		}
		assert_eq!(
			shared_x_top[0], shared_x_top[1],
			"the same value maps to the same cell-relative pixel under one shared range"
		);
	}

	#[test]
	fn product_0065_categorical_y_shares_ragged_domain_and_payload_slots() {
		let data = Arc::new(
			df!(
				"facet" => ["Left", "Right", "Left", "Right"],
				"category" => ["North", "East", "South", "North"],
				"value" => [2.0_f64, 40.0, 4.0, 2.0],
			)
			.unwrap(),
		);
		let resolved = plan(PlotRequest {
			source: PlotSource::Materialized {
				name: "product-0065-y".into(),
				data: Arc::clone(&data),
			},
			authored_spec: AuthoredPlotSpec::xy(
				PlotComposition::single(PlotType::Bar),
				"value",
				"category",
			),
		})
		.unwrap();
		let body = Arc::new(resolved.resolved_spec);
		let expected_categories = match &body.channel(Channel::Y).unwrap().domain {
			crate::plot::spec::ScaleDomain::Categorical(categories) => categories.clone(),
			_ => panic!("resolved horizontal category domain"),
		};
		assert_eq!(expected_categories, ["North", "East", "South"]);
		let grid = PlotGrid::value_facet(
			Arc::clone(&body),
			&StampedResult {
				data,
				provenance: PlotProvenance {
					source_name: "product-0065-y".into(),
					revision: Some(1),
				},
			},
			"facet",
			GridArrangement::new(1, 2).unwrap(),
		)
		.unwrap();
		let kernel = PlotGridKernel::from_grid(grid, 600, 240);
		let settings = ResolvedPlotSettings::default();
		let local = kernel
			.cells
			.iter()
			.map(|cell| cell.kernel.layout(settings.clone()))
			.collect::<Vec<_>>();
		let local_domains = local
			.iter()
			.map(|layout| match layout {
				PlotLayout::CategoricalY {
					categories,
					x_range,
				} => (categories.clone(), *x_range),
				_ => panic!("horizontal bar cell"),
			})
			.collect::<Vec<_>>();
		assert_ne!(local_domains[0].0, local_domains[1].0);
		assert_ne!(local_domains[0].1, local_domains[1].1);
		assert!(
			local_domains
				.iter()
				.all(|(categories, _)| categories.len() < 3),
			"every partition deliberately omits a category"
		);
		let expected_range = (
			local_domains
				.iter()
				.map(|(_, range)| range.0)
				.fold(f64::INFINITY, f64::min),
			local_domains
				.iter()
				.map(|(_, range)| range.1)
				.fold(f64::NEG_INFINITY, f64::max),
		);
		let merged = kernel.cell_layouts(settings.clone());
		for (_, layout) in &merged {
			let PlotLayout::CategoricalY {
				categories,
				x_range,
			} = layout
			else {
				panic!("merged categorical y")
			};
			assert_eq!(categories, &expected_categories);
			assert_eq!(*x_range, expected_range);
		}

		let bounds = IcedRectangle::new(Point::ORIGIN, Size::new(300.0, 180.0));
		let placement = TickPlacement::count(4).unwrap();
		let major = placement.positions(expected_range).unwrap();
		let minor = placement.minor_positions(&major, 2).unwrap();
		let reference_transform = CoordinateTransformer::new(&merged[0].1, bounds);
		let reference_ticks = major
			.iter()
			.chain(&minor)
			.map(|value| reference_transform.categorical(0, *value).0.x)
			.collect::<Vec<_>>();
		for (_, layout) in merged.iter().skip(1) {
			let transform = CoordinateTransformer::new(layout, bounds);
			assert_eq!(
				major
					.iter()
					.chain(&minor)
					.map(|value| transform.categorical(0, *value).0.x)
					.collect::<Vec<_>>(),
				reference_ticks,
				"major and minor numeric tick pixels are cell-congruent"
			);
		}
		let mut shared_north_width = Vec::new();
		for ((cell, local_layout), (_, merged_layout)) in
			kernel.cells.iter().zip(&local).zip(&merged)
		{
			let PlotLayout::CategoricalY {
				categories: local_categories,
				..
			} = local_layout
			else {
				unreachable!()
			};
			let transform = CoordinateTransformer::new(merged_layout, bounds);
			let mut backend = RectangleBackend::default();
			cell.kernel.plot(
				&mut backend,
				bounds,
				&transform,
				Cursor::Unavailable,
				settings.clone(),
			);
			assert_eq!(backend.rectangles.len(), local_categories.len());
			for ((top_left, size, _), category) in backend.rectangles.iter().zip(local_categories) {
				let shared_index = expected_categories
					.iter()
					.position(|candidate| candidate == category)
					.unwrap();
				assert_eq!(
					top_left.y + size.height / 2.0,
					transform.categorical(shared_index, 0.0).0.y,
					"{category} payload occupies the band carrying its label"
				);
				if category == "North" {
					shared_north_width.push(size.width);
				}
			}
		}
		assert_eq!(
			shared_north_width[0], shared_north_width[1],
			"the same value maps to the same cell-relative pixel under one shared range"
		);
	}

	#[test]
	fn product_0065_box_and_violin_address_shared_slots_by_category_identity() {
		use crate::plot::kernels::boxplot::{BoxPlotKernel, prepare_box_plot_data};
		use crate::plot::kernels::violin::{ViolinPlotKernel, prepare_violin_data};

		let data = df!(
			"category" => ["X", "X", "X", "X", "X", "Z", "Z", "Z", "Z", "Z"],
			"value" => [1.0_f64, 2.0, 2.5, 3.0, 4.0, 5.0, 6.0, 6.5, 7.0, 8.0],
		)
		.unwrap();
		let shared_categories = vec!["X".to_owned(), "Y".to_owned(), "Z".to_owned()];
		let bounds = IcedRectangle::new(Point::ORIGIN, Size::new(300.0, 180.0));
		let settings = ResolvedPlotSettings::default();

		let box_prepared = Arc::new(prepare_box_plot_data(&data, "category", "value"));
		assert_eq!(box_prepared.categories, ["X", "Z"]);
		let box_layout = PlotLayout::CategoricalX {
			categories: shared_categories.clone(),
			y_range: box_prepared.y_range,
		};
		let box_transform = CoordinateTransformer::new(&box_layout, bounds);
		let box_kernel = BoxPlotKernel {
			prepared_data: Arc::clone(&box_prepared),
		};
		let mut box_backend = RectangleBackend::default();
		box_kernel.plot(
			&mut box_backend,
			bounds,
			&box_transform,
			Cursor::Unavailable,
			settings.clone(),
		);
		assert_eq!(box_backend.rectangles.len(), 2);
		assert_eq!(
			box_backend.rectangles[1].0.x + box_backend.rectangles[1].1.width / 2.0,
			box_transform.categorical(2, 0.0).0.x,
			"Z box leaves Y's shared slot empty"
		);
		let z_box_center = box_transform.categorical(2, box_prepared.stats[1].median).0;
		assert!(
			box_kernel
				.hover(&box_transform, Cursor::Available(z_box_center))
				.unwrap()
				.starts_with("Z\n"),
			"box hover joins the shared Z band to Z's local statistics"
		);

		let violin_prepared = Arc::new(prepare_violin_data(&data, "category", "value", None));
		assert_eq!(violin_prepared.categories, ["X", "Z"]);
		let violin_layout = PlotLayout::CategoricalX {
			categories: shared_categories,
			y_range: violin_prepared.y_range,
		};
		let violin_transform = CoordinateTransformer::new(&violin_layout, bounds);
		let violin_kernel = ViolinPlotKernel {
			prepared_data: Arc::clone(&violin_prepared),
		};
		let mut violin_backend = RectangleBackend::default();
		violin_kernel.plot(
			&mut violin_backend,
			bounds,
			&violin_transform,
			Cursor::Unavailable,
			settings,
		);
		assert_eq!(violin_backend.paths.len(), 2);
		let z_path_mean_x = violin_backend.paths[1]
			.iter()
			.map(|point| point.x)
			.sum::<f32>()
			/ violin_backend.paths[1].len() as f32;
		assert!(
			(z_path_mean_x - violin_transform.categorical(2, 0.0).0.x).abs() < 0.01,
			"Z violin leaves Y's shared slot empty"
		);
		let z_violin_center = violin_transform.categorical(2, 0.0).0;
		assert_eq!(
			violin_kernel.hover(&violin_transform, Cursor::Available(z_violin_center)),
			Some("Z: Median 6.50".to_owned()),
			"violin hover joins the shared Z band to Z's local median"
		);
	}

	#[test]
	fn product_0065_categorical_scatter_already_uses_the_resolved_domain() {
		let data = Arc::new(
			df!(
				"facet" => ["A", "B", "A", "B"],
				"category" => ["X", "Y", "Z", "X"],
				"value" => [2.0_f64, 20.0, 3.0, 2.0],
			)
			.unwrap(),
		);
		let resolved = plan(PlotRequest {
			source: PlotSource::Materialized {
				name: "product-0065-scatter".into(),
				data: Arc::clone(&data),
			},
			authored_spec: AuthoredPlotSpec::xy(
				PlotComposition::single(PlotType::Scatter),
				"category",
				"value",
			),
		})
		.unwrap();
		let body = Arc::new(resolved.resolved_spec);
		let expected_categories = vec!["X".to_owned(), "Y".to_owned(), "Z".to_owned()];
		let grid = PlotGrid::value_facet(
			Arc::clone(&body),
			&StampedResult {
				data,
				provenance: PlotProvenance {
					source_name: "product-0065-scatter".into(),
					revision: Some(1),
				},
			},
			"facet",
			GridArrangement::new(1, 2).unwrap(),
		)
		.unwrap();
		let kernel = PlotGridKernel::from_grid(grid, 600, 240);
		let settings = ResolvedPlotSettings::default();
		let merged = kernel.cell_layouts(settings.clone());
		let bounds = IcedRectangle::new(Point::ORIGIN, Size::new(300.0, 180.0));
		for (cell, (_, merged_layout)) in kernel.cells.iter().zip(&merged) {
			let local_layout = cell.kernel.layout(settings.clone());
			let PlotLayout::CategoricalX { categories, .. } = &local_layout else {
				panic!("categorical scatter layout")
			};
			assert_eq!(
				categories, &expected_categories,
				"scatter preparation already consumes the resolved global domain"
			);
			let transform = CoordinateTransformer::new(merged_layout, bounds);
			let expected_slots = cell
				.cell
				.data
				.column("category")
				.unwrap()
				.as_materialized_series()
				.iter()
				.map(|value| {
					let value = value.to_string().trim_matches('"').to_owned();
					expected_categories
						.iter()
						.position(|category| category == &value)
						.unwrap()
				})
				.collect::<Vec<_>>();
			let mut backend = RectangleBackend::default();
			cell.kernel.plot(
				&mut backend,
				bounds,
				&transform,
				Cursor::Unavailable,
				settings.clone(),
			);
			let observed = backend.paths.iter().flatten().collect::<Vec<_>>();
			assert_eq!(observed.len(), expected_slots.len());
			for (point, expected_slot) in observed.into_iter().zip(expected_slots) {
				assert_eq!(
					point.x,
					transform.categorical(expected_slot, 0.0).0.x,
					"scatter payload uses its named shared category slot"
				);
			}
		}
	}

	#[test]
	fn product_0064_faceted_stacks_keep_shared_domains_panel_blind_colors_and_one_legend() {
		use crate::plot::legend::LegendSection;
		use crate::plot::spec::{
			CategoricalPaletteSelection, ColorIdentity, resolve_channels,
			resolve_domains_with_categorical_palette,
		};

		let frame = df!(
			"facet" => ["North", "North", "North", "North", "South", "South", "South", "South"],
			"category" => ["East", "East", "West", "West", "East", "East", "West", "West"],
			"series" => ["Atlas", "Beacon", "Atlas", "Beacon", "Atlas", "Beacon", "Atlas", "Beacon"],
			"value" => [2.0_f64, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0],
		)
		.unwrap();
		let authored = AuthoredPlotSpec::xy_color(
			PlotComposition::single(PlotType::Bar),
			"category",
			"value",
			Some("series".to_owned()),
		)
		.with_stacked_bar();
		let binding = resolve_channels(&authored, frame.schema().as_ref()).unwrap();
		let stamped = StampedResult {
			data: Arc::new(frame.clone()),
			provenance: PlotProvenance {
				source_name: "product-0064".to_owned(),
				revision: Some(1),
			},
		};
		let selection = CategoricalPaletteSelection::from_theme_name("blues").unwrap();
		let (resolved, _) = resolve_domains_with_categorical_palette(
			binding,
			&stamped,
			&ColorIdentity::default(),
			&selection,
		)
		.unwrap();
		assert_eq!(
			resolved.composition.primary_plot_type(),
			PlotType::StackedBar
		);
		let assignments = resolved.channel(Channel::Color).unwrap().colors.clone();
		let grid = PlotGrid::value_facet(
			Arc::new(resolved),
			&stamped,
			"facet",
			GridArrangement::new(1, 2).unwrap(),
		)
		.unwrap();
		let kernel =
			PlotGridKernel::from_styled_grid(grid, LayerSettings::default(), 800, 400).unwrap();
		let settings = ResolvedPlotSettings::default();
		let layouts = kernel.cell_layouts(settings.clone());
		assert_eq!(layouts.len(), 2);
		let ranges = layouts
			.iter()
			.map(|(_, layout)| match layout {
				PlotLayout::CategoricalX { y_range, .. } => *y_range,
				_ => panic!("stacked facet remains categorical x"),
			})
			.collect::<Vec<_>>();
		assert!(
			ranges.windows(2).all(|pair| pair[0] == pair[1]),
			"faceted stacks consume one shared summed value domain"
		);
		let legend = crate::plot::legend::union(
			kernel
				.cells
				.iter()
				.filter_map(|cell| cell.kernel.legend_content(&settings)),
		)
		.expect("one unioned stacked legend");
		let LegendSection::Entries(entries) = legend.0.first() else {
			panic!("stacked grid legend entries")
		};
		assert_eq!(entries.len(), assignments.len());
		for (entry, assignment) in entries.iter().zip(&assignments) {
			assert_eq!(entry.label, assignment.category);
			assert_eq!(
				entry.swatch,
				iced::Color::from_rgba8(
					assignment.color.red,
					assignment.color.green,
					assignment.color.blue,
					assignment.color.alpha as f32 / u8::MAX as f32,
				),
				"one panel-blind assignment feeds every cell and the unioned legend"
			);
		}

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
		for assignment in assignments {
			let fill = format!(
				"fill=\"rgb({}, {}, {})\"",
				assignment.color.red, assignment.color.green, assignment.color.blue
			);
			assert!(
				svg.matches(&fill).count() >= 2,
				"both cells must contain {fill}"
			);
		}
	}

	#[test]
	fn product_0056_faceted_horizontal_labels_payloads_and_colors_flip_together() {
		use crate::plot::legend::LegendSection;
		use crate::plot::spec::{
			CategoricalPaletteSelection, ColorIdentity, resolve_channels,
			resolve_domains_with_categorical_palette,
		};

		let frame = df!(
			"series" => ["Atlas", "Atlas", "Beacon", "Beacon"],
			"category" => ["East", "West", "East", "West"],
			"value" => [10.0_f64, 30.0, 20.0, 40.0]
		)
		.unwrap();
		let authored = AuthoredPlotSpec::xy_color(
			PlotComposition::single(PlotType::Bar),
			"value",
			"category",
			Some("series".to_owned()),
		);
		let binding = resolve_channels(&authored, frame.schema().as_ref()).unwrap();
		let stamped = StampedResult {
			data: Arc::new(frame.clone()),
			provenance: PlotProvenance {
				source_name: "product-0056".to_owned(),
				revision: Some(1),
			},
		};
		let selection = CategoricalPaletteSelection::from_theme_name("tab20c").unwrap();
		let (resolved, _) = resolve_domains_with_categorical_palette(
			binding,
			&stamped,
			&ColorIdentity::default(),
			&selection,
		)
		.unwrap();
		let assignments = resolved.channel(Channel::Color).unwrap().colors.clone();
		let grid = PlotGrid::value_facet(
			Arc::new(resolved),
			&stamped,
			"series",
			GridArrangement::new(1, 2).unwrap(),
		)
		.unwrap();
		let kernel = PlotGridKernel::from_grid(grid, 800, 400);
		let settings = ResolvedPlotSettings::default();
		let legend = crate::plot::legend::union(
			kernel
				.cells
				.iter()
				.filter_map(|cell| cell.kernel.legend_content(&settings)),
		)
		.unwrap();
		let LegendSection::Entries(entries) = legend.0.first() else {
			panic!("faceted grouped bars keep one named legend");
		};
		assert_eq!(
			entries
				.iter()
				.map(|entry| entry.label.as_str())
				.collect::<Vec<_>>(),
			["Atlas", "Beacon"]
		);

		for cell in kernel.cells.iter() {
			let layout = cell.kernel.layout(settings.clone());
			let PlotLayout::CategoricalY { categories, .. } = &layout else {
				panic!("horizontal facet cell keeps categorical y");
			};
			assert_eq!(categories, &["East", "West"]);
			let bounds = IcedRectangle::new(Point::ORIGIN, Size::new(440.0, 200.0));
			let transform = CoordinateTransformer::new(&layout, bounds);
			let mut backend = RectangleBackend::default();
			cell.kernel.plot(
				&mut backend,
				bounds,
				&transform,
				Cursor::Unavailable,
				settings.clone(),
			);
			assert_eq!(backend.rectangles.len(), 2);
			backend
				.rectangles
				.sort_by(|left, right| left.0.y.total_cmp(&right.0.y));
			assert!(backend.rectangles[0].1.width < backend.rectangles[1].1.width);
			for (index, (top_left, size, color)) in backend.rectangles.iter().enumerate() {
				assert_eq!(
					top_left.y + size.height / 2.0,
					transform.categorical(index, 0.0).0.y,
					"{} label and payload share one top-down band",
					categories[index]
				);
				let label = cell.cell.label.as_deref().unwrap();
				let assignment = assignments
					.iter()
					.find(|assignment| assignment.category == label)
					.unwrap();
				assert_eq!(
					*color,
					Color::from_rgba8(
						assignment.color.red,
						assignment.color.green,
						assignment.color.blue,
						assignment.color.alpha as f32 / u8::MAX as f32,
					),
					"facet label retains its panel-blind series color"
				);
			}
		}
	}

	#[test]
	fn product_0054_faceted_series_lines_keep_panel_blind_colors_and_one_union_legend() {
		use crate::plot::legend::LegendSection;
		use crate::plot::spec::{
			CategoricalPaletteSelection, ColorIdentity, resolve_channels,
			resolve_domains_with_categorical_palette,
		};

		let frame = df!(
			"series" => ["Atlas", "Atlas", "Beacon", "Beacon", "Cinder", "Cinder", "Delta", "Delta"],
			"x" => [1.0_f64, 2.0, 1.0, 2.0, 1.0, 2.0, 1.0, 2.0],
			"y" => [2.0_f64, 4.0, 3.0, 5.0, 4.0, 6.0, 5.0, 7.0]
		)
		.expect("faceted series-line frame");
		let authored = AuthoredPlotSpec::xy_color(
			PlotComposition::single(PlotType::Line),
			"x",
			"y",
			Some("series".to_owned()),
		);
		let binding = resolve_channels(&authored, frame.schema().as_ref()).expect("channels");
		let stamped = StampedResult {
			data: Arc::new(frame.clone()),
			provenance: PlotProvenance {
				source_name: "product-0054".to_owned(),
				revision: Some(1),
			},
		};
		let selection = CategoricalPaletteSelection::from_theme_name("tab20c").unwrap();
		let (resolved, _) = resolve_domains_with_categorical_palette(
			binding,
			&stamped,
			&ColorIdentity::default(),
			&selection,
		)
		.expect("series-line resolution");
		let assignments = resolved
			.channel(Channel::Color)
			.expect("authored series channel")
			.colors
			.clone();
		let grid = PlotGrid::value_facet(
			Arc::new(resolved),
			&stamped,
			"series",
			GridArrangement::new(2, 2).unwrap(),
		)
		.expect("series-line facet");
		let kernel = PlotGridKernel::from_grid(grid, 800, 600);
		let model = PlotSettings::default();
		let resolved_settings = model.resolve();
		let legend = crate::plot::legend::union(
			kernel
				.cells
				.iter()
				.filter_map(|cell| cell.kernel.legend_content(&resolved_settings)),
		)
		.expect("figure legend union");
		let LegendSection::Entries(entries) = legend.0.first() else {
			panic!("series-line grid must union entry legends")
		};
		assert_eq!(entries.len(), assignments.len());
		for (entry, assignment) in entries.iter().zip(&assignments) {
			assert_eq!(entry.label, assignment.category);
			assert_eq!(entry.sample, crate::plot::legend::LegendSample::LineSample);
		}

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
		for assignment in assignments {
			let stroke = format!(
				"stroke=\"rgb({}, {}, {})\"",
				assignment.color.red, assignment.color.green, assignment.color.blue
			);
			assert!(
				svg.contains(&stroke),
				"cell {} must paint its globally resolved {stroke}",
				assignment.category
			);
		}
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
		let resolved = ResolvedPlotSettings::default();
		let outer = grid_outer_area(
			widget_rect(IcedRectangle::new(
				Point::ORIGIN,
				iced::Size::new(800.0, 600.0),
			)),
			0.0,
		);
		let tiled = grid_tiled_area(outer, &resolved);
		let origins = layouts
			.iter()
			.map(|(position, _)| {
				let cell_bounds = grid_cell_bounds(tiled, kernel.arrangement, *position);
				cell_bounds.origin().into_iced()
			})
			.collect::<Vec<_>>();
		let ticks: Vec<_> = origins
			.iter()
			.enumerate()
			.map(|(index, origin)| rendered_tick_paths(&svg, 5 + index, *origin))
			.collect();
		assert!(ticks.iter().all(|paths| !paths.is_empty()));
		assert!(ticks[1..].iter().all(|paths| paths == &ticks[0]));
		for (index, ((position, layout), (x, y))) in layouts
			.iter()
			.zip([(0.0, 1.0), (1.0, 2.0), (2.0, 3.0), (3.0, 4.0)])
			.enumerate()
		{
			let cell_bounds = grid_cell_bounds(tiled, kernel.arrangement, *position);
			let inner = grid_plot_area(cell_bounds, &resolved);
			let expected = CoordinateTransformer::new(layout, inner.into_iced()).cartesian(x, y);
			let actual = rendered_single_mark(&svg, 1 + index);
			assert!((actual.x - (inner.x() + expected.x)).abs() < 0.01);
			assert!((actual.y - (inner.y() + expected.y)).abs() < 0.01);
		}
	}

	#[test]
	fn product_0066_global_heading_band_separates_first_row_facet_labels() {
		for height in [300.0_f32, 900.0] {
			let (kernel, _) = rendered_facet(4, GridArrangement::new(2, 2).unwrap(), 800.0, height);
			let mut resolved = ResolvedPlotSettings::default();
			resolved.title =
				crate::plot::spec::ResolvedPresence::Show(Arc::new("Figure heading".to_owned()));
			let model = PlotSettings::from_resolved(resolved.clone());
			let mut backend = SvgBackend::new(800.0, height);
			let widget = PlotWidget {
				kernel: &kernel,
				title: String::new(),
				padding: 0.0,
				settings: resolved.clone(),
				settings_model: model,
				render_revision: 0,
				resize_render_suspended: false,
				layer: PlotRenderLayer::Full,
			};
			widget.render(
				&mut backend,
				IcedRectangle::new(Point::ORIGIN, iced::Size::new(800.0, height)),
			);
			let svg = backend.finish();
			let title_y = text_coordinate(text_element(&svg, "Figure heading"), 'y');
			let panel_y = text_coordinate(text_element(&svg, "panel-0"), 'y');
			assert_eq!(
				title_y, resolved.title_offset,
				"the existing global title position must not move"
			);
			assert!(
				panel_y >= title_y + resolved.title_size,
				"the first facet label must begin below the global title at every viewport: {panel_y} vs {title_y}"
			);

			let tiled = grid_tiled_area(
				widget_rect(IcedRectangle::new(
					Point::ORIGIN,
					iced::Size::new(800.0, height),
				)),
				&resolved,
			);
			assert_eq!(tiled.y(), resolved.plot_padding_top);
			let mut cells = Vec::new();
			for row in 0..2 {
				for column in 0..2 {
					cells.push(grid_plot_area(
						grid_cell_bounds(tiled, kernel.arrangement, GridPosition { row, column }),
						&resolved,
					));
				}
			}
			assert!(cells.iter().all(|cell| {
				cell.width() == cells[0].width() && cell.height() == cells[0].height()
			}));
		}

		let mut subtitle = ResolvedPlotSettings::default();
		subtitle.title = crate::plot::spec::ResolvedPresence::Hide;
		subtitle.subtitle =
			crate::plot::spec::ResolvedPresence::Show(Arc::new("Subtitle".to_owned()));
		assert_eq!(
			grid_heading_band(&subtitle),
			subtitle.subtitle_offset + subtitle.subtitle_size
		);
		subtitle.subtitle = crate::plot::spec::ResolvedPresence::Hide;
		assert_eq!(
			grid_heading_band(&subtitle),
			0.0,
			"fully hidden global headings reclaim their band"
		);
	}

	#[test]
	fn product_0048_shared_edges_suppress_only_repeated_tick_text() {
		let (kernel, svg) = rendered_facet(
			4,
			GridArrangement::new(2, 2).expect("arrangement"),
			800.0,
			600.0,
		);
		// clip 0 is the figure background, 1..=4 are data cells, and
		// 5..=8 are the corresponding overlay cells.
		let tick_text: Vec<_> = (5..=8)
			.map(|clip| {
				clipped_text(&svg, clip)
					.into_iter()
					.filter(|text| !text.starts_with("panel-"))
					.collect::<Vec<_>>()
			})
			.collect();
		let y_count = tick_text[0].len();
		let x_count = tick_text[3].len();
		assert!(x_count > 0 && y_count > 0);
		assert!(
			tick_text[1].is_empty(),
			"top-right is interior on both axes"
		);
		assert_eq!(tick_text[0].len(), y_count, "top-left keeps only y text");
		assert_eq!(
			tick_text[2].len(),
			x_count + y_count,
			"bottom-left keeps both edge texts"
		);
		assert_eq!(
			tick_text[3].len(),
			x_count,
			"bottom-right keeps only x text"
		);
		assert_eq!(svg.matches(">x</text>").count(), 1, "one shared x title");
		assert_eq!(svg.matches(">y</text>").count(), 1, "one shared y title");
		for index in 0..4 {
			assert_eq!(
				svg.matches(&format!(">panel-{index}</text>")).count(),
				1,
				"panel labels remain per cell"
			);
		}

		let resolved = ResolvedPlotSettings::default();
		let bounds = IcedRectangle::new(Point::ORIGIN, iced::Size::new(800.0, 600.0));
		let tiled = grid_tiled_area(grid_outer_area(widget_rect(bounds), 0.0), &resolved);
		let shared = grid_plot_extent(tiled, kernel.arrangement, &resolved);
		let x_title = text_element(&svg, "x");
		assert!((text_coordinate(x_title, 'x') - (shared.x() + shared.width() / 2.0)).abs() < 0.01);
		let y_title = text_element(&svg, "y");
		let y_translation = text_transform_translation(y_title);
		assert!((y_translation.y - (shared.y() + shared.height() / 2.0)).abs() < 0.01);
	}

	#[test]
	fn product_0048_partial_row_text_lands_directly_in_the_empty_below_slot() {
		let (kernel, svg) = rendered_facet(
			5,
			GridArrangement::new(2, 3).expect("arrangement"),
			900.0,
			600.0,
		);
		// Five data clips make the top-right overlay clip 8. It has no
		// below neighbor, so its x tick text must be present even though it
		// is not on the arrangement's final row.
		let tick_text = |clip| {
			clipped_text(&svg, clip)
				.into_iter()
				.filter(|text| !text.starts_with("panel-"))
				.collect::<Vec<_>>()
		};
		let top_right_text = tick_text(8);
		let bottom_middle_text = tick_text(10);
		assert!(!top_right_text.is_empty());
		assert_eq!(
			top_right_text.len(),
			bottom_middle_text.len(),
			"both cells carry exactly the shared x tick labels"
		);

		let resolved = ResolvedPlotSettings::default();
		let bounds = IcedRectangle::new(Point::ORIGIN, iced::Size::new(900.0, 600.0));
		let tiled = grid_tiled_area(grid_outer_area(widget_rect(bounds), 0.0), &resolved);
		let top_right = grid_cell_bounds(
			tiled,
			kernel.arrangement,
			GridPosition { row: 0, column: 2 },
		);
		let first_tick = svg
			.split_once(r#"<g clip-path="url(#clip_8)">"#)
			.and_then(|(_, rest)| rest.split_once("</g>"))
			.and_then(|(body, _)| body.split("<text ").nth(1))
			.expect("top-right rendered x tick");
		let tick_origin = text_transform_translation(first_tick);
		assert!(tick_origin.y > top_right.bottom());
		assert!(
			tick_origin.y - top_right.bottom() <= resolved.x_label_offset + 0.01,
			"x text belongs immediately below the occupied cell: {tick_origin:?} vs {top_right:?}"
		);
	}

	#[test]
	fn product_0048_gutter_geometry_is_congruent_and_reclaims_space() {
		let arrangement = GridArrangement::new(2, 2).expect("arrangement");
		let settings = ResolvedPlotSettings::default();
		let outer = IcedRectangle::new(Point::ORIGIN, iced::Size::new(800.0, 600.0));
		let outer = widget_rect(outer);
		let tiled = grid_tiled_area(outer, &settings);
		let mut inners = Vec::new();
		let mut legacy = Vec::new();
		for row in 0..arrangement.rows {
			for column in 0..arrangement.columns {
				let position = GridPosition { row, column };
				inners.push(grid_plot_area(
					grid_cell_bounds(tiled, arrangement, position),
					&settings,
				));
				legacy.push(plot_area(
					grid_cell_bounds(outer, arrangement, position),
					0.0,
					&settings,
				));
			}
		}
		assert!(
			inners
				.iter()
				.all(|inner| inner.width() == inners[0].width()
					&& inner.height() == inners[0].height()),
			"shared domains must map over congruent pixel extents"
		);
		assert!(inners[0].width() > legacy[0].width());
		assert!(inners[0].height() > legacy[0].height());
	}

	#[test]
	fn product_0048_group_comparison_restores_text_for_differing_groups() {
		let left = AxisTextCell {
			position: GridPosition { row: 0, column: 0 },
			x_group: 0,
			y_group: 0,
		};
		let right = AxisTextCell {
			position: GridPosition { row: 0, column: 1 },
			x_group: 0,
			y_group: 1,
		};
		let cells = [left, right];
		assert!(axis_text_visibility(left, &cells).y);
		assert!(
			axis_text_visibility(right, &cells).y,
			"a different-group left neighbor does not shadow y text"
		);
		let same_group_right = AxisTextCell {
			y_group: 0,
			..right
		};
		assert!(
			!axis_text_visibility(same_group_right, &[left, same_group_right]).y,
			"the comparison, not column position, suppresses repeated text"
		);
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
		let outer = grid_outer_area(widget_rect(local), 0.0);
		let tiled = grid_tiled_area(outer, &resolved);
		let (position, layout) = &layouts[0];
		let cell_bounds = grid_cell_bounds(tiled, arrangement, *position);
		let inner = grid_plot_area(cell_bounds, &resolved);
		let transform = CoordinateTransformer::new(layout, inner.into_iced());
		let p0 = transform.cartesian(0.0, 1.0);
		let p1 = transform.cartesian(2.0, 3.0);
		let cursor = Point::new(
			inner.x() + (p0.x + p1.x) / 2.0,
			inner.y() + (p0.y + p1.y) / 2.0,
		);
		let panel = IcedRectangle::new(Point::new(500.0, 40.0), iced::Size::new(800.0, 400.0));
		let hover = PlotComposite::hover(
			&kernel,
			Rect::<Widget>::from_iced_frame(panel),
			0.0,
			widget_point(cursor),
			&model,
		);
		assert!(
			hover.is_some(),
			"a widget-local cursor over cell A's line must hit regardless of panel origin"
		);
		// Cell kernels keep their legacy hover semantics (x-nearest for a
		// line), so the composite's own miss case is a cursor outside every
		// cell region, not one merely off the marks.
		let miss = PlotComposite::hover(
			&kernel,
			Rect::<Widget>::from_iced_frame(panel),
			0.0,
			widget_point(Point::new(cursor.x, 401.0)),
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
	fn product_0068_each_facet_cell_clips_grid_then_mark_before_shared_furniture() {
		let (body, _data, stamped) = fixture();
		let grid = PlotGrid::value_facet(
			body,
			&stamped,
			"facet",
			GridArrangement::new(1, 2).expect("arrangement"),
		)
		.expect("two-cell facet");
		let mark = Color::from_rgb8(1, 205, 254);
		let kernel = PlotGridKernel::from_grid_with_factory(grid, 600, 300, |_spec, _data| {
			Ok(Arc::new(Product0068CellKernel { mark }))
		})
		.expect("strata grid");
		let resolved = ResolvedPlotSettings {
			title: ResolvedPresence::Hide,
			subtitle: ResolvedPresence::Hide,
			x_label: ResolvedPresence::Hide,
			y_label: ResolvedPresence::Hide,
			legend: ResolvedPresence::Hide,
			x_tick_placement: TickPlacement::count(1).expect("one x interval"),
			y_tick_placement: TickPlacement::count(1).expect("one y interval"),
			show_x_minor_ticks: false,
			show_y_minor_ticks: false,
			show_x_minor_grid: false,
			show_y_minor_grid: false,
			plot_padding_top: 0.0,
			plot_padding_bottom: 0.0,
			plot_padding_left: 0.0,
			plot_padding_right: 0.0,
			..ResolvedPlotSettings::default()
		};
		let grid_color = resolved.chrome.gridline.iced();
		let axis_color = resolved.chrome.axis_line.iced();
		let model = PlotSettings::from_resolved(resolved.clone());
		let widget = PlotWidget {
			kernel: &kernel,
			title: String::new(),
			padding: 0.0,
			settings: resolved,
			settings_model: model,
			render_revision: 0,
			resize_render_suspended: false,
			layer: PlotRenderLayer::Full,
		};
		let mut backend = StrataBackend::default();
		let bounds = IcedRectangle::new(Point::ORIGIN, Size::new(600.0, 300.0));
		widget.render(&mut backend, bounds);

		let marks = backend
			.operations
			.iter()
			.enumerate()
			.filter_map(|(index, operation)| {
				(operation.kind == StrataOperationKind::Fill(mark))
					.then_some((index, operation.clip.expect("cell mark must be clipped")))
			})
			.collect::<Vec<_>>();
		assert_eq!(
			marks.len(),
			2,
			"fixture must draw one real mark in each cell"
		);
		assert_ne!(marks[0].1, marks[1].1, "each cell owns a distinct clip");
		for (mark_index, cell_clip) in &marks {
			assert_ne!(
				*cell_clip, bounds,
				"cell guide clip must not widen to the figure"
			);
			let grids = backend
				.operations
				.iter()
				.enumerate()
				.filter(|(_, operation)| {
					operation.kind == StrataOperationKind::Stroke(grid_color)
						&& operation.clip == Some(*cell_clip)
				})
				.map(|(index, _)| index)
				.collect::<Vec<_>>();
			assert!(
				!grids.is_empty(),
				"each cell clip must contain real gridlines"
			);
			assert!(
				grids.iter().all(|grid_index| grid_index < mark_index),
				"PRODUCT-0068: every cell must paint guides before its marks inside the same clip"
			);
		}
		let cell_clips = marks.iter().map(|(_, clip)| *clip).collect::<Vec<_>>();
		assert!(
			backend.operations.iter().all(|operation| {
				operation.kind != StrataOperationKind::Stroke(grid_color)
					|| operation
						.clip
						.is_some_and(|clip| cell_clips.contains(&clip))
			}),
			"PRODUCT-0068: no facet grid may paint in a gutter or neighboring clip"
		);
		let last_mark = marks.iter().map(|(index, _)| *index).max().expect("marks");
		assert!(
			backend
				.operations
				.iter()
				.enumerate()
				.any(|(index, operation)| {
					index > last_mark && operation.kind == StrataOperationKind::Stroke(axis_color)
				}),
			"PRODUCT-0068: shared facet furniture must remain above all cell marks"
		);
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

		let content = crate::plot::legend::LegendContent::entries(NonEmpty::new(
			crate::plot::legend::LegendEntry {
				label: "authored series".to_owned(),
				swatch: iced::Color::from_rgb8(10, 20, 30),
				sample: crate::plot::legend::LegendSample::PointSample,
				provenance: crate::plot::legend::LegendProvenance::Named,
			},
		));
		let direct = LegendOnlyKernel {
			content: content.clone(),
		};
		let legend_grid = PlotGrid::value_facet(
			Arc::clone(&body),
			&stamped,
			"facet",
			GridArrangement::new(1, 1).expect("arrangement"),
		)
		.expect("one-cell legend facet");
		let legend_grid =
			PlotGridKernel::from_grid_with_factory(legend_grid, 400, 300, move |_spec, _data| {
				Ok(Arc::new(LegendOnlyKernel {
					content: content.clone(),
				}))
			})
			.expect("one-cell legend grid");
		assert_eq!(
			make_svg(&direct),
			make_svg(&legend_grid),
			"one-cell parity includes the default legend gutter"
		);
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
