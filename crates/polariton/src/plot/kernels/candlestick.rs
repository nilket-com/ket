use crate::plot::common::{
	AxisType, CoordinateTransformer, PlotBackend, PlotKernel, PlotLayout, ResolvedPlotSettings,
	TemporalAxisDomain, axis_hit_semantic_type, axis_hit_value, format_label,
	polars_type_to_axis_type, temporal_axis_domain,
};
use crate::plot::exact_numeric::{ExactF64Error, exact_f64};
use iced::advanced::mouse::Cursor;
use iced::widget::canvas::{Stroke, Style};
use iced::{Point, Rectangle, Size};
use polars::prelude::*;
use std::fmt;
use std::sync::Arc;

const WICK_WIDTH: f32 = 3.0;
const BODY_OUTLINE_WIDTH: f32 = 1.0;
const DOJI_WIDTH: f32 = 1.0;
const MIN_BODY_HEIGHT: f32 = 1.0;
const BODY_WIDTH_FRACTION: f32 = 0.7;
const MIN_HIT_RADIUS: f32 = 10.0;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CandlestickRole {
	X,
	Open,
	High,
	Low,
	Close,
}

impl fmt::Display for CandlestickRole {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		formatter.write_str(match self {
			Self::X => "x",
			Self::Open => "open",
			Self::High => "high",
			Self::Low => "low",
			Self::Close => "close",
		})
	}
}

#[derive(Clone, Debug, PartialEq)]
pub enum CandlestickPreparationError {
	MissingColumn {
		role: CandlestickRole,
		field: String,
	},
	TemporalColumn {
		role: CandlestickRole,
		field: String,
		dtype: DataType,
	},
	UnsupportedColumn {
		role: CandlestickRole,
		field: String,
		dtype: DataType,
	},
	NullValue {
		role: CandlestickRole,
		field: String,
		row: usize,
	},
	NonFiniteValue {
		role: CandlestickRole,
		field: String,
		row: usize,
		value: f64,
	},
	InexactValue {
		role: CandlestickRole,
		field: String,
		row: usize,
		value: String,
	},
	UnorderedX {
		role: CandlestickRole,
		field: String,
		previous_row: usize,
		row: usize,
	},
	LowerBoundViolation {
		role: CandlestickRole,
		field: String,
		row: usize,
		open: f64,
		high: f64,
		low: f64,
		close: f64,
	},
	UpperBoundViolation {
		role: CandlestickRole,
		field: String,
		row: usize,
		open: f64,
		high: f64,
		low: f64,
		close: f64,
	},
}

#[allow(dead_code)] // PRODUCT-0148 maps the carried role and field to authored spans.
impl CandlestickPreparationError {
	pub fn role(&self) -> CandlestickRole {
		match self {
			Self::MissingColumn { role, .. }
			| Self::TemporalColumn { role, .. }
			| Self::UnsupportedColumn { role, .. }
			| Self::NullValue { role, .. }
			| Self::NonFiniteValue { role, .. }
			| Self::InexactValue { role, .. }
			| Self::UnorderedX { role, .. }
			| Self::LowerBoundViolation { role, .. }
			| Self::UpperBoundViolation { role, .. } => *role,
		}
	}

	pub fn field(&self) -> &str {
		match self {
			Self::MissingColumn { field, .. }
			| Self::TemporalColumn { field, .. }
			| Self::UnsupportedColumn { field, .. }
			| Self::NullValue { field, .. }
			| Self::NonFiniteValue { field, .. }
			| Self::InexactValue { field, .. }
			| Self::UnorderedX { field, .. }
			| Self::LowerBoundViolation { field, .. }
			| Self::UpperBoundViolation { field, .. } => field,
		}
	}
}

impl fmt::Display for CandlestickPreparationError {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::MissingColumn { role, field } => {
				write!(formatter, "candlestick {role} field `{field}` is absent")
			}
			Self::TemporalColumn { role, field, dtype } => write!(
				formatter,
				"candlestick {role} field `{field}` requires a quantitative dtype, got temporal {dtype:?}"
			),
			Self::UnsupportedColumn { role, field, dtype } => {
				let requirement = if *role == CandlestickRole::X {
					"a quantitative or temporal dtype"
				} else {
					"a quantitative dtype"
				};
				write!(
					formatter,
					"candlestick {role} field `{field}` requires {requirement}, got {dtype:?}"
				)
			}
			Self::NullValue { role, field, row } => {
				write!(
					formatter,
					"candlestick {role} field `{field}` is null at row {row}"
				)
			}
			Self::NonFiniteValue {
				role,
				field,
				row,
				value,
			} => write!(
				formatter,
				"candlestick {role} field `{field}` is non-finite at row {row}: {value}"
			),
			Self::InexactValue {
				role,
				field,
				row,
				value,
			} => write!(
				formatter,
				"candlestick {role} field `{field}` cannot be represented exactly at row {row}: {value}"
			),
			Self::UnorderedX {
				field,
				previous_row,
				row,
				..
			} => write!(
				formatter,
				"candlestick x field `{field}` must increase strictly: row {row} is not after row {previous_row}"
			),
			Self::LowerBoundViolation {
				row,
				open,
				high,
				low,
				close,
				..
			} => write!(
				formatter,
				"candlestick OHLC lower bound fails at row {row}: low {low} exceeds min(open {open}, close {close}); high is {high}"
			),
			Self::UpperBoundViolation {
				row,
				open,
				high,
				low,
				close,
				..
			} => write!(
				formatter,
				"candlestick OHLC upper bound fails at row {row}: max(open {open}, close {close}) exceeds high {high}; low is {low}"
			),
		}
	}
}

impl std::error::Error for CandlestickPreparationError {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Direction {
	Up,
	Down,
	Unchanged,
}

impl Direction {
	fn from_values(open: f64, close: f64) -> Self {
		if close > open {
			Self::Up
		} else if close < open {
			Self::Down
		} else {
			Self::Unchanged
		}
	}

	fn label(self) -> &'static str {
		match self {
			Self::Up => "up",
			Self::Down => "down",
			Self::Unchanged => "unchanged",
		}
	}
}

pub struct CandlestickPlotKernel {
	pub prepared_data: Arc<CandlestickPreparedData>,
}

impl CandlestickPlotKernel {
	fn body_width(&self, transform: &CoordinateTransformer) -> f32 {
		let Some(minimum_delta) = self.prepared_data.minimum_x_delta else {
			return transform.bounds.width * BODY_WIDTH_FRACTION;
		};
		let PlotLayout::Cartesian { x_range, .. } = transform.layout else {
			return MIN_BODY_HEIGHT;
		};
		let active_span = (x_range.1 - x_range.0).abs().max(f64::EPSILON);
		((transform.bounds.width as f64 * minimum_delta / active_span) as f32 * BODY_WIDTH_FRACTION)
			.max(1.0)
	}
}

impl PlotKernel for CandlestickPlotKernel {
	fn project_predicate(
		&self,
		predicate: &crate::selection::Predicate,
	) -> Option<Vec<Box<dyn crate::plot::common::EmphasisAnchor>>> {
		let mut anchors: Vec<Box<dyn crate::plot::common::EmphasisAnchor>> = Vec::new();
		for (index, candidate) in self.prepared_data.selection_candidates.iter().enumerate() {
			if predicate.matches_candidate(candidate) {
				anchors.push(Box::new(CandlestickEmphasisAnchor {
					x: self.prepared_data.x[index],
					low: self.prepared_data.low[index],
					high: self.prepared_data.high[index],
					width_fraction: self.prepared_data.minimum_x_delta,
				}));
			}
		}
		Some(anchors)
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
			y_axis_type: AxisType::Linear,
			x_temporal_domain: self.prepared_data.x_temporal_domain.clone(),
			y_temporal_domain: None,
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
		let candle_width = self.body_width(transform);
		let body_color = settings.mark_style.fill.iced();
		let wick_color = settings.mark_style.decoration.iced();
		for index in 0..self.prepared_data.x.len() {
			let x = self.prepared_data.x[index];
			let open = self.prepared_data.open[index];
			let high = self.prepared_data.high[index];
			let low = self.prepared_data.low[index];
			let close = self.prepared_data.close[index];
			let p_high = transform.cartesian(x, high);
			let p_low = transform.cartesian(x, low);
			let p_open = transform.cartesian(x, open);
			let p_close = transform.cartesian(x, close);
			let raw_body_top = p_open.y.min(p_close.y);
			let raw_body_bottom = p_open.y.max(p_close.y);
			let raw_body_height = raw_body_bottom - raw_body_top;
			let (body_top, body_bottom) = if raw_body_height < MIN_BODY_HEIGHT {
				let middle = (raw_body_top + raw_body_bottom) / 2.0;
				(
					middle - MIN_BODY_HEIGHT / 2.0,
					middle + MIN_BODY_HEIGHT / 2.0,
				)
			} else {
				(raw_body_top, raw_body_bottom)
			};
			let body_x = p_open.x - candle_width / 2.0;
			let direction = Direction::from_values(open, close);
			match direction {
				Direction::Up | Direction::Down => {
					backend.stroke_path(
						&|builder| {
							builder.move_to(p_high);
							builder.line_to(Point::new(p_high.x, body_top));
							builder.move_to(Point::new(p_low.x, body_bottom));
							builder.line_to(p_low);
						},
						Stroke {
							style: Style::Solid(wick_color),
							width: WICK_WIDTH,
							..Default::default()
						},
					);
					let body_height = body_bottom - body_top;
					if direction == Direction::Up {
						backend.stroke_path(
							&|builder| {
								builder.move_to(Point::new(body_x, body_top));
								builder.line_to(Point::new(body_x + candle_width, body_top));
								builder.line_to(Point::new(
									body_x + candle_width,
									body_top + body_height,
								));
								builder.line_to(Point::new(body_x, body_top + body_height));
								builder.close();
							},
							Stroke {
								style: Style::Solid(body_color),
								width: BODY_OUTLINE_WIDTH,
								..Default::default()
							},
						);
					} else {
						backend.fill_rectangle(
							Point::new(body_x, body_top),
							Size::new(candle_width, body_height),
							body_color,
						);
					}
				}
				Direction::Unchanged => {
					backend.stroke_path(
						&|builder| {
							builder.move_to(p_high);
							builder.line_to(p_low);
						},
						Stroke {
							style: Style::Solid(wick_color),
							width: WICK_WIDTH,
							..Default::default()
						},
					);
					backend.stroke_path(
						&|builder| {
							builder.move_to(Point::new(body_x, p_open.y));
							builder.line_to(Point::new(body_x + candle_width, p_open.y));
						},
						Stroke {
							style: Style::Solid(body_color),
							width: DOJI_WIDTH,
							..Default::default()
						},
					);
				}
			}
		}
	}

	fn hit(
		&self,
		transform: &CoordinateTransformer,
		cursor: Cursor,
	) -> Option<crate::plot::common::PlotHit> {
		let cursor_pos = cursor.position()?;
		let (cursor_x, _) = transform.pixel_to_cartesian(cursor_pos)?;
		let xs = &self.prepared_data.x;
		if xs.is_empty() {
			return None;
		}
		let insertion = xs.partition_point(|value| *value < cursor_x);
		let index = if insertion < xs.len() && xs[insertion] == cursor_x {
			insertion
		} else if insertion == 0 {
			0
		} else if insertion == xs.len() {
			xs.len() - 1
		} else if (xs[insertion] - cursor_x).abs() < (xs[insertion - 1] - cursor_x).abs() {
			insertion
		} else {
			insertion - 1
		};
		let center = transform.cartesian(xs[index], self.prepared_data.low[index]);
		let acceptance_radius = MIN_HIT_RADIUS.max(self.body_width(transform) / 2.0);
		if (center.x - cursor_pos.x).abs() > acceptance_radius {
			return None;
		}
		let direction = Direction::from_values(
			self.prepared_data.open[index],
			self.prepared_data.close[index],
		);
		let hit = crate::plot::common::PlotHit::new(
			crate::plot::common::MarkFamily::CANDLESTICK,
			crate::plot::common::HitDimensionality::Area,
			index,
		)
		.labeled(
			"X",
			axis_hit_semantic_type(&self.prepared_data.x_axis_type),
			axis_hit_value(
				xs[index],
				&self.prepared_data.x_axis_type,
				format_label(xs[index], &self.prepared_data.x_axis_type),
			),
		)
		.labeled(
			"Open",
			crate::plot::spec::SemanticType::Quantitative,
			crate::plot::common::HitValue::quantitative(
				self.prepared_data.open[index],
				format!("{:.2}", self.prepared_data.open[index]),
			),
		)
		.labeled(
			"High",
			crate::plot::spec::SemanticType::Quantitative,
			crate::plot::common::HitValue::quantitative(
				self.prepared_data.high[index],
				format!("{:.2}", self.prepared_data.high[index]),
			),
		)
		.labeled(
			"Low",
			crate::plot::spec::SemanticType::Quantitative,
			crate::plot::common::HitValue::quantitative(
				self.prepared_data.low[index],
				format!("{:.2}", self.prepared_data.low[index]),
			),
		)
		.labeled(
			"Close",
			crate::plot::spec::SemanticType::Quantitative,
			crate::plot::common::HitValue::quantitative(
				self.prepared_data.close[index],
				format!("{:.2}", self.prepared_data.close[index]),
			),
		)
		.labeled(
			"Direction",
			crate::plot::spec::SemanticType::Nominal,
			crate::plot::common::HitValue::nominal(direction.label()),
		);
		Some(
			match self.prepared_data.selection_candidates.get(index).cloned() {
				Some(candidate) => hit.with_candidate(candidate),
				None => hit,
			},
		)
	}

	fn x_label(&self) -> String {
		self.prepared_data.x_label.clone()
	}

	fn y_label(&self) -> String {
		self.prepared_data.y_label.clone()
	}
}

struct CandlestickEmphasisAnchor {
	x: f64,
	low: f64,
	high: f64,
	width_fraction: Option<f64>,
}

impl crate::plot::common::EmphasisAnchor for CandlestickEmphasisAnchor {
	fn reproject(
		&self,
		transform: &CoordinateTransformer,
	) -> Option<crate::plot::common::EmphasisShape> {
		let low = transform.cartesian(self.x, self.low);
		let high = transform.cartesian(self.x, self.high);
		let width = match (self.width_fraction, transform.layout) {
			(Some(delta), PlotLayout::Cartesian { x_range, .. }) => {
				(transform.bounds.width as f64 * delta
					/ (x_range.1 - x_range.0).abs().max(f64::EPSILON)) as f32
					* BODY_WIDTH_FRACTION
			}
			_ => transform.bounds.width * BODY_WIDTH_FRACTION,
		}
		.max(1.0);
		Some(crate::plot::common::EmphasisShape::Rectangle(Rectangle {
			x: low.x - width / 2.0,
			y: high.y.min(low.y),
			width,
			height: (low.y - high.y).abs().max(1.0),
		}))
	}
}

#[derive(Clone, Debug, PartialEq)]
pub struct CandlestickPreparedData {
	pub x: Vec<f64>,
	pub open: Vec<f64>,
	pub high: Vec<f64>,
	pub low: Vec<f64>,
	pub close: Vec<f64>,
	pub x_range: (f64, f64),
	pub y_range: (f64, f64),
	pub x_axis_type: AxisType,
	pub x_temporal_domain: Option<TemporalAxisDomain>,
	pub x_label: String,
	pub y_label: String,
	pub(crate) minimum_x_delta: Option<f64>,
	pub selection_candidates: Vec<crate::selection::SelectionCandidate>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum OrderedX {
	Quantitative(f64),
	Temporal(i64),
}

impl OrderedX {
	fn strictly_after(self, previous: Self) -> bool {
		match (self, previous) {
			(Self::Quantitative(current), Self::Quantitative(previous)) => current > previous,
			(Self::Temporal(current), Self::Temporal(previous)) => current > previous,
			_ => unreachable!("one candlestick x column has one semantic type"),
		}
	}

	fn positive_delta(self, previous: Self) -> f64 {
		match (self, previous) {
			(Self::Quantitative(current), Self::Quantitative(previous)) => current - previous,
			(Self::Temporal(current), Self::Temporal(previous)) => {
				(i128::from(current) - i128::from(previous)) as f64
			}
			_ => unreachable!("one candlestick x column has one semantic type"),
		}
	}
}

fn temporal_dtype(dtype: &DataType) -> bool {
	matches!(
		dtype,
		DataType::Date | DataType::Datetime(_, _) | DataType::Time
	)
}

fn temporal_cell(value: AnyValue<'_>) -> Option<i64> {
	match value {
		AnyValue::Date(value) => Some(i64::from(value)),
		AnyValue::Datetime(value, _, _) | AnyValue::DatetimeOwned(value, _, _) => Some(value),
		AnyValue::Time(value) => Some(value),
		_ => None,
	}
}

fn quantitative_cell(
	series: &Series,
	row_index: usize,
	role: CandlestickRole,
	field: &str,
) -> Result<f64, CandlestickPreparationError> {
	let value = series
		.get(row_index)
		.expect("candlestick row is in dataframe bounds");
	let original = value.to_string();
	let value = match exact_f64(value) {
		Ok(value) => value,
		Err(ExactF64Error::Null) => {
			return Err(CandlestickPreparationError::NullValue {
				role,
				field: field.to_owned(),
				row: row_index + 1,
			});
		}
		Err(ExactF64Error::Inexact) => {
			return Err(CandlestickPreparationError::InexactValue {
				role,
				field: field.to_owned(),
				row: row_index + 1,
				value: original,
			});
		}
	};
	if !value.is_finite() {
		return Err(CandlestickPreparationError::NonFiniteValue {
			role,
			field: field.to_owned(),
			row: row_index + 1,
			value,
		});
	}
	Ok(value)
}

fn representable_padded_range(minimum: f64, maximum: f64) -> (f64, f64) {
	if minimum == maximum {
		let lower = minimum.next_down();
		let upper = maximum.next_up();
		return match (lower.is_finite(), upper.is_finite()) {
			(true, true) => (lower, upper),
			(true, false) => (lower, maximum),
			(false, true) => (minimum, upper),
			(false, false) => unreachable!("a validated finite value has a finite neighbour"),
		};
	}
	let span = maximum - minimum;
	if !span.is_finite() {
		return (minimum, maximum);
	}
	let padding = span.max(0.1) * 0.001;
	let expanded = (minimum - padding, maximum + padding);
	if expanded.0 < expanded.1 && expanded.0.is_finite() && expanded.1.is_finite() {
		expanded
	} else {
		(minimum, maximum)
	}
}

pub fn prepare_candlestick_data(
	df: &DataFrame,
	x_field: &str,
	open_field: &str,
	high_field: &str,
	low_field: &str,
	close_field: &str,
) -> Result<CandlestickPreparedData, CandlestickPreparationError> {
	let fields = [
		(CandlestickRole::X, x_field),
		(CandlestickRole::Open, open_field),
		(CandlestickRole::High, high_field),
		(CandlestickRole::Low, low_field),
		(CandlestickRole::Close, close_field),
	];
	let mut columns = Vec::with_capacity(fields.len());
	for (role, field) in fields {
		let column = df
			.column(field)
			.map_err(|_| CandlestickPreparationError::MissingColumn {
				role,
				field: field.to_owned(),
			})?;
		if role != CandlestickRole::X && temporal_dtype(column.dtype()) {
			return Err(CandlestickPreparationError::TemporalColumn {
				role,
				field: field.to_owned(),
				dtype: column.dtype().clone(),
			});
		}
		if !column.dtype().is_numeric()
			&& !(role == CandlestickRole::X && temporal_dtype(column.dtype()))
		{
			return Err(CandlestickPreparationError::UnsupportedColumn {
				role,
				field: field.to_owned(),
				dtype: column.dtype().clone(),
			});
		}
		columns.push(column.as_materialized_series());
	}

	let x_dtype = columns[0].dtype().clone();
	let x_is_temporal = temporal_dtype(&x_dtype);
	let mut x = Vec::with_capacity(df.height());
	let mut ordered_x = Vec::with_capacity(df.height());
	let mut open = Vec::with_capacity(df.height());
	let mut high = Vec::with_capacity(df.height());
	let mut low = Vec::with_capacity(df.height());
	let mut close = Vec::with_capacity(df.height());
	for row_index in 0..df.height() {
		if x_is_temporal {
			let value = columns[0]
				.get(row_index)
				.expect("candlestick temporal x row is in dataframe bounds");
			if matches!(value, AnyValue::Null) {
				return Err(CandlestickPreparationError::NullValue {
					role: CandlestickRole::X,
					field: x_field.to_owned(),
					row: row_index + 1,
				});
			}
			let typed =
				temporal_cell(value).expect("validated temporal dtype yields temporal cells");
			x.push(typed as f64);
			ordered_x.push(OrderedX::Temporal(typed));
		} else {
			let value = quantitative_cell(columns[0], row_index, CandlestickRole::X, x_field)?;
			x.push(value);
			ordered_x.push(OrderedX::Quantitative(value));
		}
		open.push(quantitative_cell(
			columns[1],
			row_index,
			CandlestickRole::Open,
			open_field,
		)?);
		high.push(quantitative_cell(
			columns[2],
			row_index,
			CandlestickRole::High,
			high_field,
		)?);
		low.push(quantitative_cell(
			columns[3],
			row_index,
			CandlestickRole::Low,
			low_field,
		)?);
		close.push(quantitative_cell(
			columns[4],
			row_index,
			CandlestickRole::Close,
			close_field,
		)?);
	}

	let mut minimum_x_delta: Option<f64> = None;
	for row_index in 0..df.height() {
		if row_index > 0 {
			if !ordered_x[row_index].strictly_after(ordered_x[row_index - 1]) {
				return Err(CandlestickPreparationError::UnorderedX {
					role: CandlestickRole::X,
					field: x_field.to_owned(),
					previous_row: row_index,
					row: row_index + 1,
				});
			}
			let delta = ordered_x[row_index].positive_delta(ordered_x[row_index - 1]);
			minimum_x_delta = Some(minimum_x_delta.map_or(delta, |current| current.min(delta)));
		}
		let body_minimum = open[row_index].min(close[row_index]);
		let body_maximum = open[row_index].max(close[row_index]);
		if low[row_index] > body_minimum {
			return Err(CandlestickPreparationError::LowerBoundViolation {
				role: CandlestickRole::Low,
				field: low_field.to_owned(),
				row: row_index + 1,
				open: open[row_index],
				high: high[row_index],
				low: low[row_index],
				close: close[row_index],
			});
		}
		if body_maximum > high[row_index] {
			return Err(CandlestickPreparationError::UpperBoundViolation {
				role: CandlestickRole::High,
				field: high_field.to_owned(),
				row: row_index + 1,
				open: open[row_index],
				high: high[row_index],
				low: low[row_index],
				close: close[row_index],
			});
		}
	}

	let (x_range, y_range) = if x.is_empty() {
		((0.0, 1.0), (0.0, 1.0))
	} else {
		let x_min = x.iter().copied().fold(f64::INFINITY, f64::min);
		let x_max = x.iter().copied().fold(f64::NEG_INFINITY, f64::max);
		let y_min = low.iter().copied().fold(f64::INFINITY, f64::min);
		let y_max = high.iter().copied().fold(f64::NEG_INFINITY, f64::max);
		(
			representable_padded_range(x_min, x_max),
			representable_padded_range(y_min, y_max),
		)
	};
	let x_axis_type = polars_type_to_axis_type(&x_dtype);
	let x_temporal_domain = temporal_axis_domain(
		df.column(x_field).expect("validated x column"),
		&x_axis_type,
	);
	let x_exact =
		crate::selection::exact_column_values(df.column(x_field).expect("validated x column"))
			.unwrap_or_default();
	let x_scalar_type = crate::selection::ScalarType::from_dtype(&x_dtype);
	let x_semantic_type = crate::selection::semantic_type_for_dtype(&x_dtype);
	let selection_candidates = x_scalar_type
		.zip(x_semantic_type)
		.map(|(scalar_type, semantic_type)| {
			x_exact
				.into_iter()
				.filter_map(|value| {
					Some(crate::selection::SelectionCandidate {
						constraints: vec![(
							crate::selection::FieldRef::new(
								x_field,
								semantic_type,
								scalar_type.clone(),
							),
							value?,
						)],
					})
				})
				.collect()
		})
		.unwrap_or_default();
	Ok(CandlestickPreparedData {
		x,
		open,
		high,
		low,
		close,
		x_range,
		y_range,
		x_axis_type,
		x_temporal_domain,
		x_label: x_field.to_owned(),
		y_label: format!("{open_field} / {high_field} / {low_field} / {close_field}"),
		minimum_x_delta,
		selection_candidates,
	})
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::plot::common::{HitValue, PathBuilder};
	use iced::widget::canvas::Text;
	use iced::{Color, Vector};

	fn numeric_frame() -> DataFrame {
		df!(
			"x" => [1.0_f64, 2.0, 3.0],
			"open" => [10.0_f64, 13.0, 12.0],
			"high" => [14.0_f64, 14.0, 13.0],
			"low" => [9.0_f64, 11.0, 11.0],
			"close" => [13.0_f64, 12.0, 12.0],
		)
		.expect("candlestick fixture")
	}

	fn prepare(frame: &DataFrame) -> Result<CandlestickPreparedData, CandlestickPreparationError> {
		prepare_candlestick_data(frame, "x", "open", "high", "low", "close")
	}

	#[test]
	fn product_0151_candlestick_candidate_uses_x_and_projects_one_column() {
		let prepared = prepare(&numeric_frame()).expect("prepared candle");
		let candidate = &prepared.selection_candidates[1];
		assert_eq!(candidate.constraints.len(), 1);
		assert_eq!(candidate.constraints[0].0.name.as_str(), "x");
		let predicate = crate::selection::Predicate::from_candidate(candidate).expect("predicate");
		let kernel = CandlestickPlotKernel {
			prepared_data: Arc::new(prepared),
		};
		let anchors = kernel.project_predicate(&predicate).expect("admitted");
		assert_eq!(anchors.len(), 1);
		let layout = kernel.layout(ResolvedPlotSettings::default());
		let transform = CoordinateTransformer::new(
			&layout,
			Rectangle::new(Point::ORIGIN, Size::new(400.0, 200.0)),
		);
		assert!(matches!(
			anchors[0].reproject(&transform),
			Some(crate::plot::common::EmphasisShape::Rectangle(_))
		));
	}

	#[test]
	fn product_0147_validation_precedes_structure_and_parallel_vectors_are_unrepresentable() {
		let malformed = df!(
			"x" => [2.0_f64, 1.0],
			"open" => [10.0_f64, 11.0],
			"high" => [12.0_f64, 13.0],
			"low" => [9.0_f64, 10.0],
			"close" => [Some(11.0_f64), None],
		)
		.unwrap();
		let error = prepare(&malformed).expect_err("cell fault precedes ordering");
		assert!(matches!(
			error,
			CandlestickPreparationError::NullValue {
				role: CandlestickRole::Close,
				row: 2,
				..
			}
		));
		assert_eq!(error.role(), CandlestickRole::Close);
		assert_eq!(error.field(), "close");

		let missing = malformed.drop("close").unwrap();
		assert!(matches!(
			prepare(&missing),
			Err(CandlestickPreparationError::MissingColumn {
				role: CandlestickRole::Close,
				..
			})
		));
	}

	#[test]
	fn product_0147_column_and_numeric_faults_are_role_attributed() {
		let temporal_open = Column::new("open".into(), [1_i32])
			.cast(&DataType::Date)
			.unwrap();
		let temporal = DataFrame::new(
			1,
			vec![
				Column::new("x".into(), [1.0_f64]),
				temporal_open,
				Column::new("high".into(), [2.0_f64]),
				Column::new("low".into(), [0.0_f64]),
				Column::new("close".into(), [1.5_f64]),
			],
		)
		.unwrap();
		assert!(matches!(
			prepare(&temporal),
			Err(CandlestickPreparationError::TemporalColumn {
				role: CandlestickRole::Open,
				..
			})
		));

		let unsupported = df!(
			"x" => ["session"],
			"open" => [1.0_f64],
			"high" => [2.0_f64],
			"low" => [0.0_f64],
			"close" => [1.5_f64],
		)
		.unwrap();
		assert!(matches!(
			prepare(&unsupported),
			Err(CandlestickPreparationError::UnsupportedColumn {
				role: CandlestickRole::X,
				..
			})
		));

		for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
			let frame = df!(
				"x" => [1.0_f64], "open" => [1.0_f64], "high" => [2.0_f64],
				"low" => [value], "close" => [1.5_f64]
			)
			.unwrap();
			assert!(matches!(
				prepare(&frame),
				Err(CandlestickPreparationError::NonFiniteValue {
					role: CandlestickRole::Low,
					row: 1,
					..
				})
			));
		}

		let inexact = df!(
			"x" => [1.0_f64], "open" => [9_007_199_254_740_993_i64],
			"high" => [9_007_199_254_740_994_i64], "low" => [0_i64],
			"close" => [9_007_199_254_740_992_i64]
		)
		.unwrap();
		assert!(matches!(
			prepare(&inexact),
			Err(CandlestickPreparationError::InexactValue {
				role: CandlestickRole::Open,
				row: 1,
				..
			})
		));
	}

	#[test]
	fn product_0147_structure_refuses_order_and_attributes_each_ohlc_bound() {
		for xs in [[1.0_f64, 1.0, 3.0], [2.0_f64, 1.0, 3.0]] {
			let mut unordered = numeric_frame();
			unordered
				.replace("x", Series::new("x".into(), xs).into())
				.unwrap();
			assert!(matches!(
				prepare(&unordered),
				Err(CandlestickPreparationError::UnorderedX {
					role: CandlestickRole::X,
					previous_row: 1,
					row: 2,
					..
				})
			));
		}

		let lower = df!(
			"x" => [1.0_f64], "open" => [10.0_f64], "high" => [12.0_f64],
			"low" => [11.0_f64], "close" => [12.0_f64]
		)
		.unwrap();
		let error = prepare(&lower).unwrap_err();
		assert!(matches!(
			error,
			CandlestickPreparationError::LowerBoundViolation {
				role: CandlestickRole::Low,
				row: 1,
				..
			}
		));

		let upper = df!(
			"x" => [1.0_f64], "open" => [10.0_f64], "high" => [11.0_f64],
			"low" => [9.0_f64], "close" => [12.0_f64]
		)
		.unwrap();
		assert!(matches!(
			prepare(&upper),
			Err(CandlestickPreparationError::UpperBoundViolation {
				role: CandlestickRole::High,
				row: 1,
				..
			})
		));

		let touching = df!(
			"x" => [1.0_f64], "open" => [10.0_f64], "high" => [12.0_f64],
			"low" => [10.0_f64], "close" => [12.0_f64]
		)
		.unwrap();
		assert!(prepare(&touching).is_ok());
		let zero_range_doji = df!(
			"x" => [1.0_f64], "open" => [10.0_f64], "high" => [10.0_f64],
			"low" => [10.0_f64], "close" => [10.0_f64]
		)
		.unwrap();
		assert!(prepare(&zero_range_doji).is_ok());
	}

	#[test]
	fn product_0147_temporal_order_empty_identity_and_representable_domain_are_honest() {
		let first = 1_704_067_200_000_000_000_i64;
		let zone = TimeZone::opt_try_new(Some("America/New_York"))
			.expect("zone syntax")
			.expect("named zone");
		let datetime = Column::new("when".into(), [first, first + 1])
			.cast(&DataType::Datetime(TimeUnit::Nanoseconds, Some(zone)))
			.unwrap();
		let frame = DataFrame::new(
			2,
			vec![
				datetime,
				Column::new("open".into(), [10.0_f64, 11.0]),
				Column::new("high".into(), [12.0_f64, 13.0]),
				Column::new("low".into(), [9.0_f64, 10.0]),
				Column::new("close".into(), [11.0_f64, 12.0]),
			],
		)
		.unwrap();
		let prepared = prepare_candlestick_data(&frame, "when", "open", "high", "low", "close")
			.expect("typed adjacent nanoseconds validate");
		assert_eq!(prepared.x[0], prepared.x[1]);
		assert_eq!(prepared.minimum_x_delta, Some(1.0));
		assert!(prepared.x_range.0 < prepared.x_range.1);
		assert_eq!(prepared.x_range.0, prepared.x[0].next_down());
		assert_eq!(prepared.x_range.1, prepared.x[0].next_up());
		assert!(matches!(
			prepared.x_temporal_domain,
			Some(TemporalAxisDomain::Datetime {
				first_tick,
				last_tick,
				..
			}) if first_tick == first && last_tick == first + 1
		));
		assert_ne!(
			prepared.selection_candidates[0].constraints[0].1,
			prepared.selection_candidates[1].constraints[0].1,
			"selection retains adjacent source ticks even when paint projection collapses"
		);
		let kernel = CandlestickPlotKernel {
			prepared_data: Arc::new(prepared),
		};
		let bounds = Rectangle::new(Point::ORIGIN, Size::new(200.0, 100.0));
		let layout = kernel.layout(ResolvedPlotSettings::default());
		let transform = CoordinateTransformer::new(&layout, bounds);
		let hit = kernel
			.hit(
				&transform,
				Cursor::Available(transform.cartesian(first as f64, 10.0)),
			)
			.expect("collapsed adjacent nanoseconds remain hittable");
		assert_eq!(hit.sibling_ordinal, 0);
		assert!(matches!(
			hit.candidate.as_ref().map(|candidate| &candidate.constraints[0].1),
			Some(crate::selection::ExactScalar::Datetime { ticks, .. }) if *ticks == first
		));
		assert_eq!(
			hit.fields[0].semantic_type,
			crate::plot::spec::SemanticType::Temporal
		);

		let empty = frame.head(Some(0));
		let empty =
			prepare_candlestick_data(&empty, "when", "open", "high", "low", "close").unwrap();
		assert!(empty.x.is_empty());
		assert_eq!(empty.x_range, (0.0, 1.0));
		assert_eq!(empty.y_range, (0.0, 1.0));
		assert!(matches!(empty.x_axis_type, AxisType::Datetime { .. }));
		assert_eq!(empty.x_temporal_domain, None);
	}

	#[derive(Clone, Debug, PartialEq)]
	enum Verb {
		Move(Point),
		Line(Point),
		Close,
	}

	#[derive(Default)]
	struct BankPath {
		verbs: Vec<Verb>,
	}

	impl PathBuilder for BankPath {
		fn move_to(&mut self, point: Point) {
			self.verbs.push(Verb::Move(point));
		}
		fn line_to(&mut self, point: Point) {
			self.verbs.push(Verb::Line(point));
		}
		fn arc_to(&mut self, _center: Point, _radius: f32, _start_angle: f32, _end_angle: f32) {
			unreachable!("candlestick has no arcs")
		}
		fn circle(&mut self, _center: Point, _radius: f32) {
			unreachable!("candlestick has no circles")
		}
		fn rectangle(&mut self, top_left: Point, size: Size) {
			self.move_to(top_left);
			self.line_to(Point::new(top_left.x + size.width, top_left.y));
			self.line_to(Point::new(
				top_left.x + size.width,
				top_left.y + size.height,
			));
			self.line_to(Point::new(top_left.x, top_left.y + size.height));
			self.close();
		}
		fn close(&mut self) {
			self.verbs.push(Verb::Close);
		}
	}

	#[derive(Clone, Debug, PartialEq)]
	enum Operation {
		Stroke {
			verbs: Vec<Verb>,
			color: Color,
			width: f32,
		},
		FillRectangle {
			top_left: Point,
			size: Size,
			color: Color,
		},
	}

	#[derive(Default)]
	struct PaintBank {
		operations: Vec<Operation>,
	}

	impl PlotBackend for PaintBank {
		fn stroke_path(&mut self, path: &dyn Fn(&mut dyn PathBuilder), stroke: Stroke) {
			let mut builder = BankPath::default();
			path(&mut builder);
			let Style::Solid(color) = stroke.style else {
				panic!("PRODUCT-0147 uses opaque resolved solid roles")
			};
			self.operations.push(Operation::Stroke {
				verbs: builder.verbs,
				color,
				width: stroke.width,
			});
		}
		fn fill_path(&mut self, _path: &dyn Fn(&mut dyn PathBuilder), _color: Color) {
			unreachable!("candlestick has no filled paths")
		}
		fn fill_rectangle(&mut self, top_left: Point, size: Size, color: Color) {
			self.operations.push(Operation::FillRectangle {
				top_left,
				size,
				color,
			});
		}
		fn fill_text_raw(&mut self, _text: Text) {}
		fn translate(&mut self, _translation: Vector) {}
		fn rotate(&mut self, _angle: f32) {}
		fn with_save(&mut self, callback: &mut dyn FnMut(&mut dyn PlotBackend)) {
			callback(self);
		}
		fn with_clip(
			&mut self,
			_bounds: Rectangle,
			callback: &mut dyn FnMut(&mut dyn PlotBackend),
		) {
			callback(self);
		}
	}

	fn paint(
		prepared: CandlestickPreparedData,
		settings: ResolvedPlotSettings,
		bounds: Rectangle,
	) -> (PaintBank, PlotLayout) {
		let kernel = CandlestickPlotKernel {
			prepared_data: Arc::new(prepared),
		};
		let layout = kernel.layout(settings.clone());
		let transform = CoordinateTransformer::new(&layout, bounds);
		let mut bank = PaintBank::default();
		kernel.plot(&mut bank, bounds, &transform, Cursor::Unavailable, settings);
		(bank, layout)
	}

	#[test]
	fn product_0147_direction_is_geometry_with_resolved_colour_and_split_wicks() {
		let settings = ResolvedPlotSettings::default();
		let bounds = Rectangle::new(Point::ORIGIN, Size::new(300.0, 200.0));
		let (bank, _) = paint(prepare(&numeric_frame()).unwrap(), settings.clone(), bounds);
		assert_eq!(bank.operations.len(), 6);
		assert!(matches!(
			&bank.operations[0],
			Operation::Stroke { verbs, color, width }
				if verbs.iter().filter(|verb| matches!(verb, Verb::Move(_))).count() == 2
				&& *color == settings.mark_style.decoration.iced() && *width == WICK_WIDTH
		));
		assert!(matches!(
			&bank.operations[1],
			Operation::Stroke { verbs, color, width }
				if verbs.last() == Some(&Verb::Close)
				&& *color == settings.mark_style.fill.iced() && *width == BODY_OUTLINE_WIDTH
		));
		assert!(matches!(
			&bank.operations[3],
			Operation::FillRectangle { color, .. } if *color == settings.mark_style.fill.iced()
		));
		assert!(matches!(
			&bank.operations[4],
			Operation::Stroke { verbs, width, .. }
				if verbs.iter().filter(|verb| matches!(verb, Verb::Move(_))).count() == 1
				&& *width == WICK_WIDTH
		));
		assert!(matches!(
			&bank.operations[5],
			Operation::Stroke { width, .. } if *width == DOJI_WIDTH
		));
		assert!(
			!include_str!("candlestick.rs")
				.split("#[cfg(test)]")
				.next()
				.unwrap()
				.contains("get_color")
		);

		let skin = crate::plot::chrome::ResolvedPlotSkin::from_theme_name(
			"black",
			crate::plot::chrome::PlotChromeProjection::Ordered,
		)
		.unwrap();
		let mut constant = settings;
		constant.chrome = skin.chrome;
		constant.mark_style = skin.mark_style;
		assert_eq!(constant.mark_style.fill, constant.chrome.canvas_background);
		assert_eq!(
			constant.mark_style.decoration,
			constant.chrome.canvas_background
		);
		let (constant_bank, _) =
			paint(prepare(&numeric_frame()).unwrap(), constant.clone(), bounds);
		assert_eq!(
			constant_bank
				.operations
				.iter()
				.map(std::mem::discriminant)
				.collect::<Vec<_>>(),
			bank.operations
				.iter()
				.map(std::mem::discriminant)
				.collect::<Vec<_>>()
		);
		for operation in &constant_bank.operations {
			let color = match operation {
				Operation::Stroke { color, .. } | Operation::FillRectangle { color, .. } => color,
			};
			assert_eq!(*color, constant.chrome.canvas_background.iced());
		}
	}

	#[test]
	fn product_0147_four_pixel_hollow_body_has_a_canvas_interior() {
		let frame = df!(
			"x" => [1.0_f64], "open" => [10.0_f64], "high" => [20.0_f64],
			"low" => [0.0_f64], "close" => [14.0_f64]
		)
		.unwrap();
		let settings = ResolvedPlotSettings {
			y_min: Some(0.0),
			y_max: Some(100.0),
			..Default::default()
		};
		let bounds = Rectangle::new(Point::ORIGIN, Size::new(200.0, 100.0));
		let (bank, _) = paint(prepare(&frame).unwrap(), settings, bounds);
		let Operation::Stroke { verbs, width, .. } = &bank.operations[1] else {
			panic!("rising candle emits an outlined body")
		};
		let ys = verbs
			.iter()
			.filter_map(|verb| match verb {
				Verb::Move(point) | Verb::Line(point) => Some(point.y),
				Verb::Close => None,
			})
			.collect::<Vec<_>>();
		let height = ys.iter().copied().fold(f32::NEG_INFINITY, f32::max)
			- ys.iter().copied().fold(f32::INFINITY, f32::min);
		assert!((height - 4.0).abs() < 0.001);
		assert_eq!(*width, 1.0);
		assert!(
			height - 2.0 * width >= 1.0,
			"four pixels retain canvas interior"
		);
	}

	#[test]
	fn product_0147_width_uses_minimum_delta_active_layout_and_plot_space_fallback() {
		let gap = df!(
			"x" => [0.0_f64, 10.0, 11.0], "open" => [10.0_f64, 10.0, 10.0],
			"high" => [12.0_f64, 12.0, 12.0], "low" => [9.0_f64, 9.0, 9.0],
			"close" => [11.0_f64, 11.0, 11.0]
		)
		.unwrap();
		let prepared = prepare(&gap).unwrap();
		assert_eq!(prepared.minimum_x_delta, Some(1.0));
		let kernel = CandlestickPlotKernel {
			prepared_data: Arc::new(prepared),
		};
		let settings = ResolvedPlotSettings {
			x_min: Some(0.0),
			x_max: Some(20.0),
			..Default::default()
		};
		let bounds = Rectangle::new(Point::ORIGIN, Size::new(200.0, 100.0));
		let layout = kernel.layout(settings);
		let transform = CoordinateTransformer::new(&layout, bounds);
		assert!((kernel.body_width(&transform) - 7.0).abs() < 0.001);
		let dense = df!(
			"x" => [0.0_f64, 0.000_001], "open" => [10.0_f64, 10.0],
			"high" => [12.0_f64, 12.0], "low" => [9.0_f64, 9.0],
			"close" => [11.0_f64, 11.0]
		)
		.unwrap();
		let dense = CandlestickPlotKernel {
			prepared_data: Arc::new(prepare(&dense).unwrap()),
		};
		let transform = CoordinateTransformer::new(&layout, bounds);
		assert_eq!(dense.body_width(&transform), 1.0);

		let first = 1_704_067_200_000_000_000_i64;
		let datetime = Column::new("when".into(), [first])
			.cast(&DataType::Datetime(TimeUnit::Nanoseconds, None))
			.unwrap();
		let single = DataFrame::new(
			1,
			vec![
				datetime,
				Column::new("open".into(), [10.0_f64]),
				Column::new("high".into(), [12.0_f64]),
				Column::new("low".into(), [9.0_f64]),
				Column::new("close".into(), [11.0_f64]),
			],
		)
		.unwrap();
		let prepared =
			prepare_candlestick_data(&single, "when", "open", "high", "low", "close").unwrap();
		assert!(prepared.x_range.0 < prepared.x_range.1);
		let kernel = CandlestickPlotKernel {
			prepared_data: Arc::new(prepared),
		};
		let settings = ResolvedPlotSettings {
			x_min: Some((first as f64).next_down()),
			x_max: Some((first as f64).next_up()),
			..Default::default()
		};
		let layout = kernel.layout(settings);
		let transform = CoordinateTransformer::new(&layout, bounds);
		assert_eq!(kernel.body_width(&transform), 140.0);
	}

	#[test]
	fn product_0147_hit_is_a_wide_column_with_axis_type_direction_and_first_collapse() {
		let bounds = Rectangle::new(Point::ORIGIN, Size::new(200.0, 100.0));
		for (open, close, expected) in [
			(10.0, 11.0, "up"),
			(11.0, 10.0, "down"),
			(10.0, 10.0, "unchanged"),
		] {
			let frame = df!(
				"x" => [1.0_f64], "open" => [open], "high" => [12.0_f64],
				"low" => [9.0_f64], "close" => [close]
			)
			.unwrap();
			let kernel = CandlestickPlotKernel {
				prepared_data: Arc::new(prepare(&frame).unwrap()),
			};
			let layout = kernel.layout(ResolvedPlotSettings::default());
			let transform = CoordinateTransformer::new(&layout, bounds);
			let center = transform.cartesian(1.0, 10.0);
			let hit = kernel
				.hit(
					&transform,
					Cursor::Available(Point::new(center.x + 69.0, bounds.height)),
				)
				.expect("wide candle edge belongs to its column at any y");
			let direction = hit
				.fields
				.iter()
				.find(|field| field.label == "Direction")
				.unwrap();
			assert_eq!(
				direction.semantic_type,
				crate::plot::spec::SemanticType::Nominal
			);
			assert_eq!(direction.value, HitValue::Nominal(expected.to_owned()));
			assert!(
				kernel
					.legend_content(&ResolvedPlotSettings::default())
					.is_none()
			);
			assert!(
				kernel
					.legend_reservation_content(&ResolvedPlotSettings::default())
					.is_none()
			);
			assert_eq!(kernel.y_label(), "open / high / low / close");
		}
	}
}
