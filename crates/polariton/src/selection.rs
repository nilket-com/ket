//! PRODUCT-0151's producer-neutral selection vocabulary and context algebra.
//!
//! Selection is deliberately expressed in physical source values. Plot hits
//! may still carry formatted and projected values for presentation, but those
//! values never enter a predicate or a staleness decision.

use crate::plot::spec::SemanticType;
// The input identity and the publication live beside the artifact token they
// pair with, so the artifact module never depends on the selection module.
use crate::result_artifact::ArtifactToken;
pub use crate::result_artifact::{InputIdentity, Publication};
use polars::prelude::{
	AnyValue, BooleanChunked, Column, DataFrame, DataType, NewChunkedArray,
	TimeUnit as PolarsTimeUnit,
};
use std::cmp::Ordering as CmpOrdering;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ScalarTimeUnit {
	Nanoseconds,
	Microseconds,
	Milliseconds,
}

impl From<PolarsTimeUnit> for ScalarTimeUnit {
	fn from(value: PolarsTimeUnit) -> Self {
		match value {
			PolarsTimeUnit::Nanoseconds => Self::Nanoseconds,
			PolarsTimeUnit::Microseconds => Self::Microseconds,
			PolarsTimeUnit::Milliseconds => Self::Milliseconds,
		}
	}
}

impl From<crate::plot::common::TimeUnit> for ScalarTimeUnit {
	fn from(value: crate::plot::common::TimeUnit) -> Self {
		match value {
			crate::plot::common::TimeUnit::Nanoseconds => Self::Nanoseconds,
			crate::plot::common::TimeUnit::Microseconds => Self::Microseconds,
			crate::plot::common::TimeUnit::Milliseconds => Self::Milliseconds,
		}
	}
}

/// The physical type of an exact source value. Parameters are part of the
/// identity: milliseconds and nanoseconds, or zoned and unzoned datetimes, do
/// not silently compare.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum ScalarType {
	Bool,
	Int,
	UInt,
	Float,
	Str,
	Date,
	Datetime {
		unit: ScalarTimeUnit,
		zone: Option<String>,
	},
	Time,
	Duration {
		unit: ScalarTimeUnit,
	},
	Decimal {
		scale: u8,
	},
}

impl ScalarType {
	pub fn from_dtype(dtype: &DataType) -> Option<Self> {
		match dtype {
			DataType::Boolean => Some(Self::Bool),
			DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => Some(Self::Int),
			DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
				Some(Self::UInt)
			}
			DataType::Float32 | DataType::Float64 => Some(Self::Float),
			DataType::String | DataType::Categorical(_, _) | DataType::Enum(_, _) => {
				Some(Self::Str)
			}
			DataType::Date => Some(Self::Date),
			DataType::Datetime(unit, zone) => Some(Self::Datetime {
				unit: (*unit).into(),
				zone: zone.as_ref().map(ToString::to_string),
			}),
			DataType::Time => Some(Self::Time),
			DataType::Duration(unit) => Some(Self::Duration {
				unit: (*unit).into(),
			}),
			DataType::Decimal(_, scale) => u8::try_from(*scale)
				.ok()
				.map(|scale| Self::Decimal { scale }),
			_ => None,
		}
	}
}

/// Exact source scalar. Floating equality and hashing use the canonical bit
/// representation established by `float`: negative zero is folded and every
/// non-finite value refuses before construction.
#[derive(Clone, Debug)]
pub enum ExactScalar {
	Bool(bool),
	Int(i64),
	UInt(u64),
	Float(f64),
	Str(String),
	Date {
		days: i32,
	},
	Datetime {
		ticks: i64,
		unit: ScalarTimeUnit,
		zone: Option<String>,
	},
	Time {
		nanos: i64,
	},
	Duration {
		ticks: i64,
		unit: ScalarTimeUnit,
	},
	Decimal {
		mantissa: i128,
		scale: u8,
	},
}

impl ExactScalar {
	pub fn float(value: f64) -> Option<Self> {
		if !value.is_finite() {
			return None;
		}
		Some(Self::Float(if value == 0.0 { 0.0 } else { value }))
	}

	pub fn scalar_type(&self) -> ScalarType {
		match self {
			Self::Bool(_) => ScalarType::Bool,
			Self::Int(_) => ScalarType::Int,
			Self::UInt(_) => ScalarType::UInt,
			Self::Float(_) => ScalarType::Float,
			Self::Str(_) => ScalarType::Str,
			Self::Date { .. } => ScalarType::Date,
			Self::Datetime { unit, zone, .. } => ScalarType::Datetime {
				unit: *unit,
				zone: zone.clone(),
			},
			Self::Time { .. } => ScalarType::Time,
			Self::Duration { unit, .. } => ScalarType::Duration { unit: *unit },
			Self::Decimal { scale, .. } => ScalarType::Decimal { scale: *scale },
		}
	}

	pub(crate) fn plot_value(&self) -> Option<f64> {
		Some(match self {
			Self::Int(value) => *value as f64,
			Self::UInt(value) => *value as f64,
			Self::Float(value) => *value,
			Self::Date { days } => f64::from(*days),
			Self::Datetime { ticks, .. } | Self::Duration { ticks, .. } => *ticks as f64,
			Self::Time { nanos } => *nanos as f64,
			Self::Decimal { mantissa, scale } => *mantissa as f64 / 10_f64.powi(i32::from(*scale)),
			Self::Bool(_) | Self::Str(_) => return None,
		})
	}

	pub fn from_any_value(value: AnyValue<'_>, dtype: &DataType) -> Option<Self> {
		match value {
			AnyValue::Boolean(value) => Some(Self::Bool(value)),
			AnyValue::Int8(value) => Some(Self::Int(i64::from(value))),
			AnyValue::Int16(value) => Some(Self::Int(i64::from(value))),
			AnyValue::Int32(value) => Some(Self::Int(i64::from(value))),
			AnyValue::Int64(value) => Some(Self::Int(value)),
			AnyValue::UInt8(value) => Some(Self::UInt(u64::from(value))),
			AnyValue::UInt16(value) => Some(Self::UInt(u64::from(value))),
			AnyValue::UInt32(value) => Some(Self::UInt(u64::from(value))),
			AnyValue::UInt64(value) => Some(Self::UInt(value)),
			AnyValue::Float32(value) => Self::float(f64::from(value)),
			AnyValue::Float64(value) => Self::float(value),
			AnyValue::String(value) => Some(Self::Str(value.to_owned())),
			AnyValue::StringOwned(value) => Some(Self::Str(value.as_str().to_owned())),
			AnyValue::Date(days) => Some(Self::Date { days }),
			AnyValue::Datetime(ticks, unit, zone) => Some(Self::Datetime {
				ticks,
				unit: unit.into(),
				zone: zone.map(|zone| zone.to_string()),
			}),
			AnyValue::DatetimeOwned(ticks, unit, zone) => Some(Self::Datetime {
				ticks,
				unit: unit.into(),
				zone: zone.map(|zone| zone.to_string()),
			}),
			AnyValue::Time(nanos) => Some(Self::Time { nanos }),
			AnyValue::Duration(ticks, unit) => Some(Self::Duration {
				ticks,
				unit: unit.into(),
			}),
			AnyValue::Decimal(mantissa, _precision, scale) => u8::try_from(scale)
				.ok()
				.map(|scale| Self::Decimal { mantissa, scale }),
			// Categorical and enum access varies with Polars' mapping lifetime.
			// Casting to String is the caller's lossless route; accepting their
			// physical indices here would violate PRODUCT-0151 §1.
			_ if matches!(dtype, DataType::Categorical(_, _) | DataType::Enum(_, _)) => None,
			_ => None,
		}
	}

	fn partial_cmp_same_type(&self, other: &Self) -> Option<CmpOrdering> {
		match (self, other) {
			(Self::Bool(a), Self::Bool(b)) => Some(a.cmp(b)),
			(Self::Int(a), Self::Int(b)) => Some(a.cmp(b)),
			(Self::UInt(a), Self::UInt(b)) => Some(a.cmp(b)),
			(Self::Float(a), Self::Float(b)) => a.partial_cmp(b),
			(Self::Str(a), Self::Str(b)) => Some(a.cmp(b)),
			(Self::Date { days: a }, Self::Date { days: b }) => Some(a.cmp(b)),
			(
				Self::Datetime {
					ticks: at,
					unit: au,
					zone: az,
				},
				Self::Datetime {
					ticks: bt,
					unit: bu,
					zone: bz,
				},
			) if au == bu && az == bz => Some(at.cmp(bt)),
			(Self::Time { nanos: a }, Self::Time { nanos: b }) => Some(a.cmp(b)),
			(
				Self::Duration {
					ticks: at,
					unit: au,
				},
				Self::Duration {
					ticks: bt,
					unit: bu,
				},
			) if au == bu => Some(at.cmp(bt)),
			(
				Self::Decimal {
					mantissa: am,
					scale: as_,
				},
				Self::Decimal {
					mantissa: bm,
					scale: bs,
				},
			) if as_ == bs => Some(am.cmp(bm)),
			_ => None,
		}
	}
}

impl PartialEq for ExactScalar {
	fn eq(&self, other: &Self) -> bool {
		match (self, other) {
			(Self::Bool(a), Self::Bool(b)) => a == b,
			(Self::Int(a), Self::Int(b)) => a == b,
			(Self::UInt(a), Self::UInt(b)) => a == b,
			(Self::Float(a), Self::Float(b)) => a.to_bits() == b.to_bits(),
			(Self::Str(a), Self::Str(b)) => a == b,
			(Self::Date { days: a }, Self::Date { days: b }) => a == b,
			(
				Self::Datetime {
					ticks: at,
					unit: au,
					zone: az,
				},
				Self::Datetime {
					ticks: bt,
					unit: bu,
					zone: bz,
				},
			) => at == bt && au == bu && az == bz,
			(Self::Time { nanos: a }, Self::Time { nanos: b }) => a == b,
			(
				Self::Duration {
					ticks: at,
					unit: au,
				},
				Self::Duration {
					ticks: bt,
					unit: bu,
				},
			) => at == bt && au == bu,
			(
				Self::Decimal {
					mantissa: am,
					scale: as_,
				},
				Self::Decimal {
					mantissa: bm,
					scale: bs,
				},
			) => am == bm && as_ == bs,
			_ => false,
		}
	}
}

impl Eq for ExactScalar {}

impl Hash for ExactScalar {
	fn hash<H: Hasher>(&self, state: &mut H) {
		std::mem::discriminant(self).hash(state);
		match self {
			Self::Bool(value) => value.hash(state),
			Self::Int(value) => value.hash(state),
			Self::UInt(value) => value.hash(state),
			Self::Float(value) => value.to_bits().hash(state),
			Self::Str(value) => value.hash(state),
			Self::Date { days } => days.hash(state),
			Self::Datetime { ticks, unit, zone } => {
				ticks.hash(state);
				unit.hash(state);
				zone.hash(state);
			}
			Self::Time { nanos } => nanos.hash(state),
			Self::Duration { ticks, unit } => {
				ticks.hash(state);
				unit.hash(state);
			}
			Self::Decimal { mantissa, scale } => {
				mantissa.hash(state);
				scale.hash(state);
			}
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SourceField(String);

impl SourceField {
	pub fn new(name: impl Into<String>) -> Self {
		Self(name.into())
	}

	pub fn as_str(&self) -> &str {
		&self.0
	}
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct FieldRef {
	pub name: SourceField,
	pub semantic_type: SemanticType,
	pub scalar_type: ScalarType,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SelectionAxisFields {
	pub x: Option<FieldRef>,
	pub y: Option<FieldRef>,
}

impl SelectionAxisFields {
	pub fn from_recipe(
		recipe: &crate::plot::recipe::PlotRecipe,
		kernel: &dyn crate::plot::common::PlotKernel,
	) -> Self {
		let Ok(frame) = recipe.artifact.projection() else {
			return Self::default();
		};
		let channel = |channel: crate::plot::spec::Channel| match &recipe.construction {
			crate::plot::recipe::PlotConstruction::Single { spec, .. } => spec.channel(channel),
			crate::plot::recipe::PlotConstruction::Facet { spec, .. } => spec.channel(channel),
			crate::plot::recipe::PlotConstruction::Layered { .. } => None,
		};
		let field = |name: String, semantic_type: Option<SemanticType>| {
			if name.is_empty() {
				return None;
			}
			let column = frame.column(&name).ok()?;
			Some(FieldRef::new(
				name,
				semantic_type.or_else(|| semantic_type_for_dtype(column.dtype()))?,
				ScalarType::from_dtype(column.dtype())?,
			))
		};
		let x = channel(crate::plot::spec::Channel::X);
		let y = channel(crate::plot::spec::Channel::Y);
		Self {
			x: field(
				x.map(|binding| binding.binding.field.clone())
					.unwrap_or_else(|| kernel.x_label()),
				x.map(|binding| binding.binding.semantic_type),
			),
			y: field(
				y.map(|binding| binding.binding.field.clone())
					.unwrap_or_else(|| kernel.y_label()),
				y.map(|binding| binding.binding.semantic_type),
			),
		}
	}
}

impl FieldRef {
	pub fn new(
		name: impl Into<String>,
		semantic_type: SemanticType,
		scalar_type: ScalarType,
	) -> Self {
		Self {
			name: SourceField::new(name),
			semantic_type,
			scalar_type,
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SelectionCandidate {
	pub constraints: Vec<(FieldRef, ExactScalar)>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConstraintForm {
	Values(HashSet<ExactScalar>),
	/// `lower > upper` represents the deliberately empty inward-rounded
	/// interval. Endpoints always have `field.scalar_type`.
	Interval {
		lower: ExactScalar,
		upper: ExactScalar,
	},
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FieldConstraint {
	pub field: FieldRef,
	pub form: ConstraintForm,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Predicate {
	pub fields: Vec<FieldConstraint>,
}

impl Predicate {
	pub fn from_candidate(candidate: &SelectionCandidate) -> Result<Self, SelectionRefusal> {
		let mut fields = Vec::<FieldConstraint>::new();
		for (field, value) in &candidate.constraints {
			if value.scalar_type() != field.scalar_type {
				return Err(SelectionRefusal::ValueTypeMismatch(field.name.clone()));
			}
			if let Some(existing) = fields.iter_mut().find(|item| item.field == *field) {
				let ConstraintForm::Values(values) = &mut existing.form else {
					unreachable!("candidate construction only creates value sets")
				};
				values.insert(value.clone());
			} else {
				fields.push(FieldConstraint {
					field: field.clone(),
					form: ConstraintForm::Values(HashSet::from([value.clone()])),
				});
			}
		}
		Ok(Self { fields })
	}

	pub fn field(&self, name: &SourceField) -> Option<&FieldConstraint> {
		self.fields.iter().find(|field| &field.field.name == name)
	}

	pub fn without_field(&self, name: &SourceField) -> Self {
		Self {
			fields: self
				.fields
				.iter()
				.filter(|item| &item.field.name != name)
				.cloned()
				.collect(),
		}
	}

	pub fn accepts_value(&self, field: &SourceField, value: &ExactScalar) -> bool {
		self.field(field)
			.is_none_or(|constraint| match &constraint.form {
				ConstraintForm::Values(values) => values.contains(value),
				ConstraintForm::Interval { lower, upper } => matches!(
					(
						value.partial_cmp_same_type(lower),
						value.partial_cmp_same_type(upper)
					),
					(
						Some(CmpOrdering::Equal | CmpOrdering::Greater),
						Some(CmpOrdering::Equal | CmpOrdering::Less)
					)
				),
			})
	}

	pub fn filter_frame(&self, frame: &DataFrame) -> Result<DataFrame, SelectionRefusal> {
		let columns = self
			.fields
			.iter()
			.map(|constraint| {
				let column = frame.column(constraint.field.name.as_str()).map_err(|_| {
					SelectionRefusal::MissingField {
						view: SourceView::HostPanel(0),
						field: constraint.field.name.clone(),
					}
				})?;
				let actual = ScalarType::from_dtype(column.dtype()).ok_or_else(|| {
					SelectionRefusal::IncompatibleField {
						view: SourceView::HostPanel(0),
						field: constraint.field.name.clone(),
					}
				})?;
				if actual != constraint.field.scalar_type {
					return Err(SelectionRefusal::IncompatibleField {
						view: SourceView::HostPanel(0),
						field: constraint.field.name.clone(),
					});
				}
				let column = if matches!(
					column.dtype(),
					DataType::Categorical(_, _) | DataType::Enum(_, _)
				) {
					column.cast(&DataType::String).map_err(|_| {
						SelectionRefusal::IncompatibleField {
							view: SourceView::HostPanel(0),
							field: constraint.field.name.clone(),
						}
					})?
				} else {
					column.clone()
				};
				Ok((constraint, column))
			})
			.collect::<Result<Vec<_>, SelectionRefusal>>()?;
		let mask = BooleanChunked::from_iter_values(
			"selection".into(),
			(0..frame.height()).map(|row| {
				columns.iter().all(|(constraint, column)| {
					let Ok(value) = column.get(row) else {
						return false;
					};
					let Some(value) = ExactScalar::from_any_value(value, column.dtype()) else {
						return false;
					};
					match &constraint.form {
						ConstraintForm::Values(values) => values.contains(&value),
						ConstraintForm::Interval { lower, upper } => {
							matches!(
								(
									value.partial_cmp_same_type(lower),
									value.partial_cmp_same_type(upper)
								),
								(
									Some(CmpOrdering::Equal | CmpOrdering::Greater),
									Some(CmpOrdering::Equal | CmpOrdering::Less)
								)
							)
						}
					}
				})
			}),
		);
		frame
			.filter(&mask)
			.map_err(|_| SelectionRefusal::UnknownSource)
	}

	pub fn matches_candidate(&self, candidate: &SelectionCandidate) -> bool {
		!self.fields.is_empty()
			&& self.fields.iter().all(|constraint| {
				candidate
					.constraints
					.iter()
					.find(|(field, _)| field.name == constraint.field.name)
					.is_some_and(|(field, value)| {
						field.scalar_type == constraint.field.scalar_type
							&& constraint_accepts(&constraint.form, value)
					})
			})
	}

	pub fn matches_frame_row(&self, frame: &DataFrame, row: usize) -> bool {
		self.fields.iter().all(|constraint| {
			let Ok(column) = frame.column(constraint.field.name.as_str()) else {
				return false;
			};
			let owned;
			let column = if matches!(
				column.dtype(),
				DataType::Categorical(_, _) | DataType::Enum(_, _)
			) {
				let Ok(cast) = column.cast(&DataType::String) else {
					return false;
				};
				owned = cast;
				&owned
			} else {
				column
			};
			let Ok(value) = column.get(row) else {
				return false;
			};
			let Some(value) = ExactScalar::from_any_value(value, column.dtype()) else {
				return false;
			};
			match &constraint.form {
				ConstraintForm::Values(values) => values.contains(&value),
				ConstraintForm::Interval { lower, upper } => matches!(
					(
						value.partial_cmp_same_type(lower),
						value.partial_cmp_same_type(upper)
					),
					(
						Some(CmpOrdering::Equal | CmpOrdering::Greater),
						Some(CmpOrdering::Equal | CmpOrdering::Less)
					)
				),
			}
		})
	}
}

pub fn frame_scalar_types(frame: &DataFrame) -> HashMap<SourceField, ScalarType> {
	frame
		.columns()
		.iter()
		.filter_map(|column| {
			Some((
				SourceField::new(column.name().to_string()),
				ScalarType::from_dtype(column.dtype())?,
			))
		})
		.collect()
}

pub fn semantic_type_for_dtype(dtype: &DataType) -> Option<SemanticType> {
	match dtype {
		DataType::String
		| DataType::Categorical(_, _)
		| DataType::Enum(_, _)
		| DataType::Boolean => Some(SemanticType::Nominal),
		DataType::Date | DataType::Datetime(_, _) | DataType::Time => Some(SemanticType::Temporal),
		dtype if dtype.is_numeric() => Some(SemanticType::Quantitative),
		_ => None,
	}
}

/// Retain a column's exact values once at preparation. Categorical and enum
/// columns cross their lossless string projection here; their physical codes
/// are process-local implementation details and never become predicates.
pub fn exact_column_values(column: &Column) -> Option<Vec<Option<ExactScalar>>> {
	let owned;
	let source = if matches!(
		column.dtype(),
		DataType::Categorical(_, _) | DataType::Enum(_, _)
	) {
		owned = column.cast(&DataType::String).ok()?;
		&owned
	} else {
		column
	};
	Some(
		(0..source.len())
			.map(|row| {
				source
					.get(row)
					.ok()
					.and_then(|value| ExactScalar::from_any_value(value, source.dtype()))
			})
			.collect(),
	)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SelectionRevision(u64);

impl SelectionRevision {
	pub const ZERO: Self = Self(0);

	pub fn next(self) -> Self {
		Self(self.0.wrapping_add(1))
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SurfaceIncarnation(u64);

static NEXT_SURFACE_INCARNATION: AtomicU64 = AtomicU64::new(1);

impl SurfaceIncarnation {
	pub fn mint() -> Self {
		Self(NEXT_SURFACE_INCARNATION.fetch_add(1, Ordering::Relaxed))
	}

	/// PRODUCT-0158 §3: the opaque token a command names a figure by. It is
	/// the incarnation and nothing else; a receipt prints it and a command
	/// carries it back.
	pub fn token(self) -> u64 {
		self.0
	}

	#[cfg(test)]
	pub fn from_token(token: u64) -> Self {
		Self(token)
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RebuildStamp {
	pub publication: Publication,
	pub surface_incarnation: SurfaceIncarnation,
	pub selection_revision: SelectionRevision,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SelectionDisplay {
	#[default]
	Ready,
	Pending,
}

#[derive(Clone)]
pub struct SelectionRebuildJob {
	pub stamp: RebuildStamp,
	pub recipe: crate::plot::recipe::PlotRecipe,
	pub predicate: Predicate,
}

#[derive(Clone)]
pub struct SelectionRebuildCompletion {
	pub stamp: RebuildStamp,
	pub predicate: Predicate,
	pub result: Result<Arc<dyn crate::plot::common::PlotKernel + Send + Sync>, String>,
}

impl std::fmt::Debug for SelectionRebuildCompletion {
	fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		formatter
			.debug_struct("SelectionRebuildCompletion")
			.field("stamp", &self.stamp)
			.field("predicate", &self.predicate)
			.field("result", &self.result.as_ref().map(|_| "kernel"))
			.finish()
	}
}

pub async fn run_selection_rebuild(job: SelectionRebuildJob) -> SelectionRebuildCompletion {
	let stamp = job.stamp;
	let predicate = job.predicate.clone();
	let recipe = job.recipe;
	let result = tokio::task::spawn_blocking(move || recipe.rebuild(&predicate))
		.await
		.map_err(|error| format!("selection rebuild task panicked: {error}"))
		.and_then(|result| result);
	SelectionRebuildCompletion {
		stamp,
		predicate: job.predicate,
		result,
	}
}

#[derive(Clone)]
pub struct TableRebuildJob {
	pub stamp: RebuildStamp,
	pub artifact: Arc<crate::result_artifact::ResultArtifact>,
	pub predicate: Predicate,
}

#[derive(Clone, Debug)]
pub struct TableRebuildCompletion {
	pub stamp: RebuildStamp,
	pub predicate: Predicate,
	pub result: Result<DataFrame, String>,
}

pub async fn run_table_rebuild(job: TableRebuildJob) -> TableRebuildCompletion {
	let stamp = job.stamp;
	let predicate = job.predicate.clone();
	let artifact = job.artifact;
	let result = tokio::task::spawn_blocking(move || {
		let frame = artifact.projection().map_err(|error| error.to_string())?;
		predicate
			.filter_frame(&frame)
			.map_err(|error| error.to_string())
	})
	.await
	.map_err(|error| format!("selection table rebuild task panicked: {error}"))
	.and_then(|result| result);
	TableRebuildCompletion {
		stamp,
		predicate: job.predicate,
		result,
	}
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum SourceView {
	HostPanel(u64),
	NotebookCell(libaryon::uast::projection::MarkdownCellIdentity),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SelectionOperation {
	Replace,
	Add,
	Subtract,
	Clear,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SelectionIntent {
	pub publication: Publication,
	pub source_view: SourceView,
	pub predicate: Predicate,
	pub operation: SelectionOperation,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SelectionRefusal {
	StalePublication,
	UnknownSource,
	MissingField {
		view: SourceView,
		field: SourceField,
	},
	IncompatibleField {
		view: SourceView,
		field: SourceField,
	},
	ValueTypeMismatch(SourceField),
	MultiFieldAdd,
	DisjunctiveRange(SourceField),
	SubtractNotCategorical(SourceField),
	MultiColumnTable,
	UnrepresentableCell(SourceField),
}

impl std::fmt::Display for SelectionRefusal {
	fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		formatter.write_str("Selection · ")?;
		match self {
			Self::StalePublication => formatter.write_str(
				"Selection expired because the source relation was refreshed; select again.",
			),
			Self::UnknownSource => formatter.write_str("the source view is no longer live"),
			Self::MissingField { view, field } => write!(
				formatter,
				"view {view:?} does not carry field `{}`",
				field.as_str()
			),
			Self::IncompatibleField { view, field } => write!(
				formatter,
				"view {view:?} carries field `{}` with an incompatible physical type",
				field.as_str()
			),
			Self::ValueTypeMismatch(field) => write!(
				formatter,
				"field `{}` received a value of another physical type",
				field.as_str()
			),
			Self::MultiFieldAdd => {
				formatter.write_str("adding a multi-field selection would widen it")
			}
			Self::DisjunctiveRange(field) => write!(
				formatter,
				"field `{}` already has an interval; disjunctive ranges are not supported",
				field.as_str()
			),
			Self::SubtractNotCategorical(field) => write!(
				formatter,
				"field `{}` is not nominal or ordinal and cannot be subtracted",
				field.as_str()
			),
			Self::MultiColumnTable => {
				formatter.write_str("a table selection must span exactly one column")
			}
			Self::UnrepresentableCell(field) => write!(
				formatter,
				"field `{}` contains a null or unrepresentable selected value",
				field.as_str(),
			),
		}
	}
}

pub fn table_event_predicate(
	frame: &DataFrame,
	event: &crate::gui::table::TableSelectionEvent,
) -> Result<(Predicate, SelectionOperation), SelectionRefusal> {
	let crate::gui::table::TableSelectionEvent::Selected {
		min_col,
		max_col,
		min_row,
		max_row,
	} = event
	else {
		return Ok((Predicate::default(), SelectionOperation::Clear));
	};
	if min_col != max_col {
		return Err(SelectionRefusal::MultiColumnTable);
	}
	let column = frame
		.select_at_idx(*min_col)
		.ok_or(SelectionRefusal::UnknownSource)?;
	let name = SourceField::new(column.name().to_string());
	let scalar_type = ScalarType::from_dtype(column.dtype())
		.ok_or_else(|| SelectionRefusal::UnrepresentableCell(name.clone()))?;
	let semantic_type = semantic_type_for_dtype(column.dtype())
		.ok_or_else(|| SelectionRefusal::UnrepresentableCell(name.clone()))?;
	let exact = exact_column_values(column)
		.ok_or_else(|| SelectionRefusal::UnrepresentableCell(name.clone()))?;
	let mut values = HashSet::new();
	for row in *min_row..=*max_row {
		let value = exact
			.get(row)
			.and_then(Clone::clone)
			.ok_or_else(|| SelectionRefusal::UnrepresentableCell(name.clone()))?;
		values.insert(value);
	}
	Ok((
		Predicate {
			fields: vec![FieldConstraint {
				field: FieldRef {
					name,
					semantic_type,
					scalar_type,
				},
				form: ConstraintForm::Values(values),
			}],
		},
		SelectionOperation::Replace,
	))
}

#[derive(Clone, Debug)]
pub struct LiveSelectionView {
	pub publication: Publication,
	pub fields: HashMap<SourceField, ScalarType>,
}

#[derive(Clone, Debug)]
struct BoundPredicate {
	token: ArtifactToken,
	predicate: Predicate,
}

#[derive(Clone, Debug)]
struct SelectionContext {
	revision: SelectionRevision,
	views: HashMap<SourceView, LiveSelectionView>,
	predicates: HashMap<SourceView, BoundPredicate>,
}

impl Default for SelectionContext {
	fn default() -> Self {
		Self {
			revision: SelectionRevision::ZERO,
			views: HashMap::new(),
			predicates: HashMap::new(),
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SelectionChange {
	pub input: InputIdentity,
	pub revision: SelectionRevision,
	pub rebuild: Vec<SourceView>,
	pub emphasize: Option<SourceView>,
}

#[derive(Default)]
pub struct SelectionContexts {
	contexts: HashMap<InputIdentity, SelectionContext>,
}

impl SelectionContexts {
	pub fn register(&mut self, view: SourceView, live: LiveSelectionView) {
		self.contexts
			.entry(live.publication.input)
			.or_default()
			.views
			.insert(view, live);
	}

	pub fn apply(&mut self, intent: SelectionIntent) -> Result<SelectionChange, SelectionRefusal> {
		let context = self
			.contexts
			.get_mut(&intent.publication.input)
			.ok_or(SelectionRefusal::UnknownSource)?;
		let source = context
			.views
			.get(&intent.source_view)
			.ok_or(SelectionRefusal::UnknownSource)?;
		if source.publication != intent.publication {
			return Err(SelectionRefusal::StalePublication);
		}
		if intent.operation == SelectionOperation::Clear
			&& !context.predicates.contains_key(&intent.source_view)
		{
			return Ok(SelectionChange {
				input: intent.publication.input,
				revision: context.revision,
				rebuild: Vec::new(),
				emphasize: None,
			});
		}

		let previous = context
			.predicates
			.get(&intent.source_view)
			.map(|bound| bound.predicate.clone())
			.unwrap_or_default();
		let proposed = apply_operation(previous, &intent.predicate, intent.operation)?;
		preflight(context, &intent.source_view, &proposed)?;

		match intent.operation {
			SelectionOperation::Clear => {
				context.predicates.remove(&intent.source_view);
			}
			_ => {
				context.predicates.insert(
					intent.source_view.clone(),
					BoundPredicate {
						token: intent.publication.token,
						predicate: proposed,
					},
				);
			}
		}
		context.revision = context.revision.next();
		Ok(SelectionChange {
			input: intent.publication.input,
			revision: context.revision,
			rebuild: context
				.views
				.keys()
				.filter(|view| **view != intent.source_view)
				.cloned()
				.collect(),
			emphasize: Some(intent.source_view),
		})
	}

	/// Retire only predicates minted by `publication`; parallel publications
	/// sharing the input remain live. Every survivor is returned for a rebuild
	/// against the now-current conjunction.
	pub fn retire(&mut self, publication: Publication) -> Option<SelectionChange> {
		let context = self.contexts.get_mut(&publication.input)?;
		let retired_views: HashSet<_> = context
			.views
			.iter()
			.filter(|(_, view)| view.publication == publication)
			.map(|(id, _)| id.clone())
			.collect();
		let retires_predicate = context
			.predicates
			.values()
			.any(|bound| bound.token == publication.token);
		if retired_views.is_empty() && !retires_predicate {
			return None;
		}
		context
			.predicates
			.retain(|_, bound| bound.token != publication.token);
		context.views.retain(|id, _| !retired_views.contains(id));
		context.revision = context.revision.next();
		Some(SelectionChange {
			input: publication.input,
			revision: context.revision,
			rebuild: context.views.keys().cloned().collect(),
			emphasize: None,
		})
	}

	pub fn predicate_for(&self, input: InputIdentity, target: &SourceView) -> Predicate {
		let Some(context) = self.contexts.get(&input) else {
			return Predicate::default();
		};
		context
			.predicates
			.iter()
			.filter(|(view, _)| *view != target)
			.fold(Predicate::default(), |combined, (_, bound)| {
				conjoin_predicates(combined, &bound.predicate)
			})
	}

	pub fn own_predicate(&self, input: InputIdentity, source: &SourceView) -> Option<&Predicate> {
		self.contexts
			.get(&input)?
			.predicates
			.get(source)
			.map(|bound| &bound.predicate)
	}

	pub fn revision(&self, input: InputIdentity) -> Option<SelectionRevision> {
		self.contexts.get(&input).map(|context| context.revision)
	}
}

fn constraint_accepts(form: &ConstraintForm, value: &ExactScalar) -> bool {
	match form {
		ConstraintForm::Values(values) => values.contains(value),
		ConstraintForm::Interval { lower, upper } => matches!(
			(
				value.partial_cmp_same_type(lower),
				value.partial_cmp_same_type(upper)
			),
			(
				Some(CmpOrdering::Equal | CmpOrdering::Greater),
				Some(CmpOrdering::Equal | CmpOrdering::Less)
			)
		),
	}
}

fn conjoin_predicates(mut combined: Predicate, incoming: &Predicate) -> Predicate {
	for next in &incoming.fields {
		let Some(existing) = combined
			.fields
			.iter_mut()
			.find(|field| field.field.name == next.field.name)
		else {
			combined.fields.push(next.clone());
			continue;
		};
		existing.form = match (&existing.form, &next.form) {
			(ConstraintForm::Values(left), ConstraintForm::Values(right)) => {
				ConstraintForm::Values(left.intersection(right).cloned().collect())
			}
			(ConstraintForm::Values(values), interval @ ConstraintForm::Interval { .. })
			| (interval @ ConstraintForm::Interval { .. }, ConstraintForm::Values(values)) => {
				ConstraintForm::Values(
					values
						.iter()
						.filter(|value| constraint_accepts(interval, value))
						.cloned()
						.collect(),
				)
			}
			(
				ConstraintForm::Interval {
					lower: left_lower,
					upper: left_upper,
				},
				ConstraintForm::Interval {
					lower: right_lower,
					upper: right_upper,
				},
			) => {
				let lower =
					if left_lower.partial_cmp_same_type(right_lower) == Some(CmpOrdering::Less) {
						right_lower.clone()
					} else {
						left_lower.clone()
					};
				let upper = if left_upper.partial_cmp_same_type(right_upper)
					== Some(CmpOrdering::Greater)
				{
					right_upper.clone()
				} else {
					left_upper.clone()
				};
				ConstraintForm::Interval { lower, upper }
			}
		};
	}
	combined
}

fn preflight(
	context: &SelectionContext,
	_source: &SourceView,
	proposed: &Predicate,
) -> Result<(), SelectionRefusal> {
	for (view_id, view) in &context.views {
		for constraint in &proposed.fields {
			let Some(scalar_type) = view.fields.get(&constraint.field.name) else {
				return Err(SelectionRefusal::MissingField {
					view: view_id.clone(),
					field: constraint.field.name.clone(),
				});
			};
			if scalar_type != &constraint.field.scalar_type {
				return Err(SelectionRefusal::IncompatibleField {
					view: view_id.clone(),
					field: constraint.field.name.clone(),
				});
			}
		}
	}
	Ok(())
}

fn apply_operation(
	mut own: Predicate,
	incoming: &Predicate,
	operation: SelectionOperation,
) -> Result<Predicate, SelectionRefusal> {
	match operation {
		SelectionOperation::Replace => return Ok(incoming.clone()),
		SelectionOperation::Clear => return Ok(Predicate::default()),
		SelectionOperation::Add | SelectionOperation::Subtract if incoming.fields.len() > 1 => {
			return Err(SelectionRefusal::MultiFieldAdd);
		}
		_ => {}
	}

	for incoming_field in &incoming.fields {
		let existing = own
			.fields
			.iter_mut()
			.find(|field| field.field.name == incoming_field.field.name);
		match (operation, existing, &incoming_field.form) {
			(SelectionOperation::Add, None, _) => own.fields.push(incoming_field.clone()),
			(
				SelectionOperation::Add,
				Some(FieldConstraint {
					form: ConstraintForm::Values(existing),
					..
				}),
				ConstraintForm::Values(incoming),
			) => existing.extend(incoming.iter().cloned()),
			(SelectionOperation::Add, Some(_), _) => {
				return Err(SelectionRefusal::DisjunctiveRange(
					incoming_field.field.name.clone(),
				));
			}
			(
				SelectionOperation::Subtract,
				Some(FieldConstraint {
					field,
					form: ConstraintForm::Values(existing),
				}),
				ConstraintForm::Values(incoming),
			) => {
				if !matches!(
					field.semantic_type,
					SemanticType::Nominal | SemanticType::Ordinal
				) {
					return Err(SelectionRefusal::SubtractNotCategorical(field.name.clone()));
				}
				existing.retain(|value| !incoming.contains(value));
			}
			(SelectionOperation::Subtract, _, _) => {
				return Err(SelectionRefusal::SubtractNotCategorical(
					incoming_field.field.name.clone(),
				));
			}
			(SelectionOperation::Replace | SelectionOperation::Clear, _, _) => unreachable!(),
		}
	}
	Ok(own)
}

#[cfg(test)]
mod product_0151_selection_tests {
	use super::*;
	use crate::result_artifact::ResultArtifact;
	use polars::prelude::TimeUnit;
	use std::sync::Arc;

	fn publication(input: InputIdentity, value: i32) -> Publication {
		let artifact =
			ResultArtifact::from_polars(Arc::new(polars::df!("x" => [value]).expect("frame")));
		Publication {
			input,
			token: artifact.artifact_token(),
		}
	}

	fn field(name: &str, semantic_type: SemanticType, scalar_type: ScalarType) -> FieldRef {
		FieldRef::new(name, semantic_type, scalar_type)
	}

	fn values(field: FieldRef, values: impl IntoIterator<Item = ExactScalar>) -> Predicate {
		Predicate {
			fields: vec![FieldConstraint {
				field,
				form: ConstraintForm::Values(values.into_iter().collect()),
			}],
		}
	}

	#[test]
	fn product_0151_exact_scalars_preserve_physical_identity() {
		let big =
			ExactScalar::from_any_value(AnyValue::Int64(9_007_199_254_740_993), &DataType::Int64)
				.expect("exact int");
		assert_eq!(big, ExactScalar::Int(9_007_199_254_740_993));
		assert_eq!(ExactScalar::float(-0.0), ExactScalar::float(0.0));
		assert!(ExactScalar::float(f64::NAN).is_none());

		let zone = polars::prelude::TimeZone::opt_try_new(Some("America/Chicago"))
			.expect("valid zone")
			.expect("present zone");
		let first = ExactScalar::from_any_value(
			AnyValue::DatetimeOwned(
				1_800_000_000_000_000_000,
				TimeUnit::Nanoseconds,
				Some(Arc::new(zone.clone())),
			),
			&DataType::Datetime(TimeUnit::Nanoseconds, Some(zone.clone())),
		)
		.expect("first timestamp");
		let second = ExactScalar::from_any_value(
			AnyValue::DatetimeOwned(
				1_800_000_000_000_000_001,
				TimeUnit::Nanoseconds,
				Some(Arc::new(zone.clone())),
			),
			&DataType::Datetime(TimeUnit::Nanoseconds, Some(zone)),
		)
		.expect("second timestamp");
		assert_ne!(first, second, "adjacent nanoseconds stay distinct");
	}

	#[test]
	fn product_0151_projection_matches_the_predicate_subset_of_a_candidate() {
		let x = field("x", SemanticType::Quantitative, ScalarType::Int);
		let y = field("y", SemanticType::Quantitative, ScalarType::Int);
		let candidate = SelectionCandidate {
			constraints: vec![(x.clone(), ExactScalar::Int(3)), (y, ExactScalar::Int(7))],
		};
		assert!(values(x.clone(), [ExactScalar::Int(3)]).matches_candidate(&candidate));
		assert!(!values(x, [ExactScalar::Int(4)]).matches_candidate(&candidate));
		assert!(
			!Predicate::default().matches_candidate(&candidate),
			"an empty predicate is not an emphasis request"
		);
	}

	#[test]
	fn product_0151_preflight_is_total_and_units_are_part_of_type() {
		let input = InputIdentity::mint();
		let source_publication = publication(input, 1);
		let sibling_publication = publication(input, 2);
		let source = SourceView::HostPanel(1);
		let sibling = SourceView::HostPanel(2);
		let ns = ScalarType::Datetime {
			unit: ScalarTimeUnit::Nanoseconds,
			zone: Some("UTC".into()),
		};
		let ms = ScalarType::Datetime {
			unit: ScalarTimeUnit::Milliseconds,
			zone: Some("UTC".into()),
		};
		let mut contexts = SelectionContexts::default();
		contexts.register(
			source.clone(),
			LiveSelectionView {
				publication: source_publication,
				fields: HashMap::from([(SourceField::new("stamp"), ns.clone())]),
			},
		);
		contexts.register(
			sibling.clone(),
			LiveSelectionView {
				publication: sibling_publication,
				fields: HashMap::from([(SourceField::new("stamp"), ms)]),
			},
		);
		let result = contexts.apply(SelectionIntent {
			publication: source_publication,
			source_view: source,
			predicate: values(
				field("stamp", SemanticType::Temporal, ns),
				[ExactScalar::Datetime {
					ticks: 1,
					unit: ScalarTimeUnit::Nanoseconds,
					zone: Some("UTC".into()),
				}],
			),
			operation: SelectionOperation::Replace,
		});
		assert!(matches!(
			result,
			Err(SelectionRefusal::IncompatibleField { view, .. }) if view == sibling
		));
		assert_eq!(contexts.revision(input), Some(SelectionRevision::ZERO));
	}

	#[test]
	fn product_0151_algebra_is_or_within_and_across_and_semantic_for_subtract() {
		let category = field("category", SemanticType::Ordinal, ScalarType::Str);
		let value = field("value", SemanticType::Quantitative, ScalarType::Int);
		let own = Predicate {
			fields: vec![
				values(category.clone(), [ExactScalar::Str("a".into())])
					.fields
					.remove(0),
				values(value.clone(), [ExactScalar::Int(4)])
					.fields
					.remove(0),
			],
		};
		let added = apply_operation(
			own.clone(),
			&values(category.clone(), [ExactScalar::Str("b".into())]),
			SelectionOperation::Add,
		)
		.expect("categorical union");
		let ConstraintForm::Values(categories) = &added.field(&category.name).unwrap().form else {
			panic!("values")
		};
		assert_eq!(categories.len(), 2);
		assert!(
			added.field(&value.name).is_some(),
			"other field is conjoined"
		);

		let subtracted = apply_operation(
			added,
			&values(category.clone(), [ExactScalar::Str("a".into())]),
			SelectionOperation::Subtract,
		)
		.expect("ordinal subtract");
		let ConstraintForm::Values(categories) = &subtracted.field(&category.name).unwrap().form
		else {
			panic!("values")
		};
		assert_eq!(categories, &HashSet::from([ExactScalar::Str("b".into())]));

		assert!(matches!(
			apply_operation(
				own,
				&values(value, [ExactScalar::Int(4)]),
				SelectionOperation::Subtract,
			),
			Err(SelectionRefusal::SubtractNotCategorical(_))
		));
	}

	#[test]
	fn product_0151_stale_publications_refuse_and_retirement_rebuilds_survivors() {
		let input = InputIdentity::mint();
		let old = publication(input, 1);
		let current = publication(input, 2);
		let sibling_publication = publication(input, 3);
		let source = SourceView::HostPanel(1);
		let sibling = SourceView::HostPanel(2);
		let field = field("category", SemanticType::Nominal, ScalarType::Str);
		let mut contexts = SelectionContexts::default();
		for (view, publication) in [
			(source.clone(), current),
			(sibling.clone(), sibling_publication),
		] {
			contexts.register(
				view,
				LiveSelectionView {
					publication,
					fields: HashMap::from([(field.name.clone(), ScalarType::Str)]),
				},
			);
		}
		let stale = contexts.apply(SelectionIntent {
			publication: old,
			source_view: source.clone(),
			predicate: values(field.clone(), [ExactScalar::Str("a".into())]),
			operation: SelectionOperation::Replace,
		});
		assert_eq!(stale, Err(SelectionRefusal::StalePublication));

		contexts
			.apply(SelectionIntent {
				publication: current,
				source_view: source,
				predicate: values(field, [ExactScalar::Str("a".into())]),
				operation: SelectionOperation::Replace,
			})
			.expect("current intent");
		let retirement = contexts.retire(current).expect("context");
		assert_eq!(retirement.rebuild, vec![sibling]);
		assert!(
			contexts
				.predicate_for(input, &SourceView::HostPanel(2))
				.fields
				.is_empty()
		);
		let revision = contexts.revision(input);
		assert!(
			contexts.retire(current).is_none(),
			"a second dispatch-side retirement is idempotent"
		);
		assert_eq!(contexts.revision(input), revision);
	}

	#[test]
	fn product_0151_retirement_rebuild_predicate_contains_only_surviving_intents() {
		let input = InputIdentity::mint();
		let category_view = SourceView::HostPanel(1);
		let value_view = SourceView::HostPanel(2);
		let target = SourceView::HostPanel(3);
		let category_publication = publication(input, 1);
		let value_publication = publication(input, 2);
		let target_publication = publication(input, 3);
		let fields = HashMap::from([
			(SourceField::new("category"), ScalarType::Str),
			(SourceField::new("value"), ScalarType::Int),
		]);
		let mut contexts = SelectionContexts::default();
		for (view, publication) in [
			(category_view.clone(), category_publication),
			(value_view.clone(), value_publication),
			(target.clone(), target_publication),
		] {
			contexts.register(
				view,
				LiveSelectionView {
					publication,
					fields: fields.clone(),
				},
			);
		}
		contexts
			.apply(SelectionIntent {
				publication: category_publication,
				source_view: category_view,
				predicate: values(
					field("category", SemanticType::Nominal, ScalarType::Str),
					[ExactScalar::Str("a".into())],
				),
				operation: SelectionOperation::Replace,
			})
			.expect("category intent");
		contexts
			.apply(SelectionIntent {
				publication: value_publication,
				source_view: value_view.clone(),
				predicate: Predicate {
					fields: vec![FieldConstraint {
						field: field("value", SemanticType::Quantitative, ScalarType::Int),
						form: ConstraintForm::Interval {
							lower: ExactScalar::Int(2),
							upper: ExactScalar::Int(4),
						},
					}],
				},
				operation: SelectionOperation::Replace,
			})
			.expect("value intent");

		let before = contexts.predicate_for(input, &target);
		assert_eq!(before.fields.len(), 2);
		let retirement = contexts
			.retire(category_publication)
			.expect("publication retires");
		assert!(retirement.rebuild.contains(&value_view));
		assert!(retirement.rebuild.contains(&target));
		let surviving = contexts.predicate_for(input, &target);
		assert_eq!(surviving.fields.len(), 1);
		assert_eq!(surviving.fields[0].field.name.as_str(), "value");
		let frame = polars::df!(
			"category" => ["a", "b", "a", "b"],
			"value" => [1_i64, 2, 3, 5],
		)
		.expect("survivor fixture");
		let displayed = surviving.filter_frame(&frame).expect("survivor rebuild");
		assert_eq!(
			displayed
				.column("value")
				.expect("value")
				.i64()
				.expect("int")
				.into_no_null_iter()
				.collect::<Vec<_>>(),
			vec![2, 3]
		);
	}

	#[test]
	fn product_0151_table_emits_only_exact_single_column_values() {
		let frame = polars::df!(
			"category" => [Some("b"), Some("a"), None],
			"value" => [9_007_199_254_740_993_i64, 2, 3],
		)
		.expect("table fixture");
		let event = crate::gui::table::TableSelectionEvent::Selected {
			min_col: 1,
			max_col: 1,
			min_row: 0,
			max_row: 0,
		};
		let (predicate, operation) = table_event_predicate(&frame, &event).expect("exact cell");
		assert_eq!(operation, SelectionOperation::Replace);
		assert_eq!(predicate.fields.len(), 1);
		assert!(predicate.matches_frame_row(&frame, 0));
		assert!(!predicate.matches_frame_row(&frame, 1));
		let ConstraintForm::Values(values) = &predicate.fields[0].form else {
			panic!("table emits a value set")
		};
		assert_eq!(
			values,
			&HashSet::from([ExactScalar::Int(9_007_199_254_740_993)])
		);

		let multiple = crate::gui::table::TableSelectionEvent::Selected {
			min_col: 0,
			max_col: 1,
			min_row: 0,
			max_row: 0,
		};
		assert_eq!(
			table_event_predicate(&frame, &multiple),
			Err(SelectionRefusal::MultiColumnTable)
		);
		let null = crate::gui::table::TableSelectionEvent::Selected {
			min_col: 0,
			max_col: 0,
			min_row: 2,
			max_row: 2,
		};
		assert!(matches!(
			table_event_predicate(&frame, &null),
			Err(SelectionRefusal::UnrepresentableCell(field)) if field.as_str() == "category"
		));
		assert_eq!(
			table_event_predicate(&frame, &crate::gui::table::TableSelectionEvent::Clear),
			Ok((Predicate::default(), SelectionOperation::Clear))
		);
	}

	#[test]
	fn product_0151_table_emphasis_follows_values_after_reordering() {
		let predicate = values(
			field("category", SemanticType::Nominal, ScalarType::Str),
			[ExactScalar::Str("a".into())],
		);
		let reordered = polars::df!(
			"category" => ["b", "a", "c"],
			"value" => [2_i64, 1, 3],
		)
		.expect("reordered table");
		assert!(!predicate.matches_frame_row(&reordered, 0));
		assert!(predicate.matches_frame_row(&reordered, 1));
		assert!(!predicate.matches_frame_row(&reordered, 2));
	}

	#[test]
	fn product_0151_or_within_and_across_filters_from_the_unfiltered_frame() {
		let frame = polars::df!(
			"category" => ["a", "b", "c", "a"],
			"value" => [1_i64, 2, 3, 4],
		)
		.expect("filter fixture");
		let predicate = Predicate {
			fields: vec![
				FieldConstraint {
					field: field("category", SemanticType::Nominal, ScalarType::Str),
					form: ConstraintForm::Values(HashSet::from([
						ExactScalar::Str("a".into()),
						ExactScalar::Str("b".into()),
					])),
				},
				FieldConstraint {
					field: field("value", SemanticType::Quantitative, ScalarType::Int),
					form: ConstraintForm::Interval {
						lower: ExactScalar::Int(2),
						upper: ExactScalar::Int(4),
					},
				},
			],
		};
		let filtered = predicate.filter_frame(&frame).expect("local filtering");
		assert_eq!(filtered.height(), 2);
		assert_eq!(
			filtered
				.column("category")
				.unwrap()
				.str()
				.unwrap()
				.into_no_null_iter()
				.collect::<Vec<_>>(),
			vec!["b", "a"]
		);
	}

	#[test]
	fn product_0151_refused_algebra_is_atomic_and_prefixed() {
		let category = field("category", SemanticType::Nominal, ScalarType::Str);
		let value = field("value", SemanticType::Quantitative, ScalarType::Int);
		let interval = Predicate {
			fields: vec![FieldConstraint {
				field: value.clone(),
				form: ConstraintForm::Interval {
					lower: ExactScalar::Int(1),
					upper: ExactScalar::Int(2),
				},
			}],
		};
		assert!(matches!(
			apply_operation(interval.clone(), &interval, SelectionOperation::Add),
			Err(SelectionRefusal::DisjunctiveRange(_))
		));
		let multi = Predicate {
			fields: vec![
				values(category, [ExactScalar::Str("a".into())])
					.fields
					.remove(0),
				values(value.clone(), [ExactScalar::Int(1)])
					.fields
					.remove(0),
			],
		};
		assert_eq!(
			apply_operation(Predicate::default(), &multi, SelectionOperation::Add),
			Err(SelectionRefusal::MultiFieldAdd)
		);
		let refusal = SelectionRefusal::SubtractNotCategorical(value.name).to_string();
		assert!(refusal.starts_with("Selection · "));
	}

	#[test]
	fn product_0151_context_predicates_exclude_self_and_preflight_every_sibling() {
		let input = InputIdentity::mint();
		let source = SourceView::HostPanel(1);
		let sibling = SourceView::HostPanel(2);
		let third = SourceView::HostPanel(3);
		let source_publication = publication(input, 1);
		let sibling_publication = publication(input, 2);
		let third_publication = publication(input, 3);
		let mut contexts = SelectionContexts::default();
		for (view, publication, fields) in [
			(
				source.clone(),
				source_publication,
				HashMap::from([(SourceField::new("category"), ScalarType::Str)]),
			),
			(
				sibling.clone(),
				sibling_publication,
				HashMap::from([(SourceField::new("category"), ScalarType::Str)]),
			),
			(third.clone(), third_publication, HashMap::new()),
		] {
			contexts.register(
				view,
				LiveSelectionView {
					publication,
					fields,
				},
			);
		}
		let intent = SelectionIntent {
			publication: source_publication,
			source_view: source.clone(),
			predicate: values(
				field("category", SemanticType::Nominal, ScalarType::Str),
				[ExactScalar::Str("a".into())],
			),
			operation: SelectionOperation::Replace,
		};
		assert!(matches!(
			contexts.apply(intent.clone()),
			Err(SelectionRefusal::MissingField { view, field })
				if view == third && field.as_str() == "category"
		));
		assert_eq!(contexts.revision(input), Some(SelectionRevision::ZERO));
		contexts
			.contexts
			.get_mut(&input)
			.unwrap()
			.views
			.remove(&third);
		contexts.apply(intent).expect("all siblings compatible");
		assert!(contexts.predicate_for(input, &source).fields.is_empty());
		assert_eq!(contexts.predicate_for(input, &sibling).fields.len(), 1);
	}

	#[test]
	fn product_0151_candidate_without_source_provenance_refuses_atomically() {
		let input = InputIdentity::mint();
		let publication = publication(input, 1);
		let source = SourceView::HostPanel(1);
		let mut contexts = SelectionContexts::default();
		contexts.register(
			source.clone(),
			LiveSelectionView {
				publication,
				fields: HashMap::from([
					(SourceField::new("x"), ScalarType::Float),
					(SourceField::new("y"), ScalarType::Float),
				]),
			},
		);
		let result = contexts.apply(SelectionIntent {
			publication,
			source_view: source.clone(),
			predicate: values(
				field("Count", SemanticType::Quantitative, ScalarType::UInt),
				[ExactScalar::UInt(4)],
			),
			operation: SelectionOperation::Replace,
		});
		assert!(matches!(
			result,
			Err(SelectionRefusal::MissingField { view, field })
				if view == source && field.as_str() == "Count"
		));
		assert_eq!(contexts.revision(input), Some(SelectionRevision::ZERO));
	}

	#[test]
	fn product_0151_clear_without_a_bound_predicate_is_a_true_no_op() {
		let input = InputIdentity::mint();
		let source = SourceView::HostPanel(1);
		let sibling = SourceView::HostPanel(2);
		let source_publication = publication(input, 1);
		let mut contexts = SelectionContexts::default();
		for (view, publication) in [
			(source.clone(), source_publication),
			(sibling, publication(input, 2)),
		] {
			contexts.register(
				view,
				LiveSelectionView {
					publication,
					fields: HashMap::from([(SourceField::new("x"), ScalarType::Int)]),
				},
			);
		}
		let change = contexts
			.apply(SelectionIntent {
				publication: source_publication,
				source_view: source,
				predicate: Predicate::default(),
				operation: SelectionOperation::Clear,
			})
			.expect("empty clear is accepted as a no-op");
		assert_eq!(change.revision, SelectionRevision::ZERO);
		assert!(change.rebuild.is_empty());
		assert_eq!(change.emphasize, None);
		assert_eq!(contexts.revision(input), Some(SelectionRevision::ZERO));
	}
}
