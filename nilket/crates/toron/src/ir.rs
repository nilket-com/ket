use std::collections::HashMap;
use std::fmt;

use crate::catalog::TableRef;
pub use crate::distributed::identity::{QueryId, StageId};
use arrow::datatypes::{DataType, Field, IntervalUnit, TimeUnit};
use serde::{Deserialize, Serialize};
use wendelon_client::{
	LOGICAL_TYPE_RESERVED_STATIC_MAX_ID, LogicalTypeDefinition, LogicalTypePhysical,
	LogicalTypeRefinementPredicate, LogicalTypeUnit,
};

#[path = "ir/execution.rs"]
pub mod execution;

pub use execution::*;

/// Fixed hash-table bookkeeping charged by the direct in-memory join guard for each
/// build row. Keep this estimate in IR: both binder admission and execution must use
/// the same boundary when deciding whether a local hash build fits a task budget.
pub(crate) const HASH_JOIN_BUILD_ROW_OVERHEAD_BYTES: usize = 112;

/// The direct hash-build guard's conservative key-width estimate.
pub(crate) fn hash_join_build_key_width(data_type: &DataType) -> usize {
	match data_type {
		DataType::Boolean | DataType::Int8 | DataType::UInt8 => 1,
		DataType::Int16 | DataType::UInt16 => 2,
		DataType::Int32 | DataType::UInt32 | DataType::Float32 | DataType::Date32 => 4,
		DataType::Int64 | DataType::UInt64 | DataType::Float64 | DataType::Timestamp(_, _) => 8,
		DataType::Utf8 => 32,
		_ => 16,
	}
}

/// The raw task-memory admission estimate for an in-memory hash join build.
///
/// This must remain the single source of truth for binder-side local admission and
/// executor-side spill fallback. It deliberately estimates hash-table state, not the
/// physical size of an input batch or a spill artifact.
pub(crate) fn estimated_hash_join_build_bytes(rows: usize, key_width: usize) -> u64 {
	u64::try_from(rows.saturating_mul(HASH_JOIN_BUILD_ROW_OVERHEAD_BYTES + key_width))
		.unwrap_or(u64::MAX)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LogicalTypeId(pub u32);

impl LogicalTypeId {
	pub const NAKED: Self = Self(0);

	pub fn is_naked(self) -> bool {
		self == Self::NAKED
	}
}

pub const CUSTOMER_ID_LOGICAL_TYPE: LogicalTypeId = LogicalTypeId(1);
pub const PRODUCT_ID_LOGICAL_TYPE: LogicalTypeId = LogicalTypeId(2);
pub const DURABLE_LOGICAL_TYPE_MIN_ID: u32 = LOGICAL_TYPE_RESERVED_STATIC_MAX_ID + 1;
pub const LOGICAL_TYPE_ID_METADATA_KEY: &str = "nilket.toron.logical_type_id";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalTypeSpec {
	pub id: LogicalTypeId,
	pub name: String,
	pub physical: DataType,
	pub refinement: Option<LogicalTypeRefinementPredicate>,
	pub unit: Option<LogicalTypeUnit>,
}

pub fn seed_logical_type_spec(id: LogicalTypeId) -> Option<LogicalTypeSpec> {
	match id {
		LogicalTypeId::NAKED => Some(LogicalTypeSpec {
			id,
			name: "naked".to_string(),
			physical: DataType::Null,
			refinement: None,
			unit: None,
		}),
		CUSTOMER_ID_LOGICAL_TYPE => Some(LogicalTypeSpec {
			id,
			name: "CustomerId".to_string(),
			physical: DataType::Int64,
			refinement: None,
			unit: None,
		}),
		PRODUCT_ID_LOGICAL_TYPE => Some(LogicalTypeSpec {
			id,
			name: "ProductId".to_string(),
			physical: DataType::Int64,
			refinement: None,
			unit: None,
		}),
		_ => None,
	}
}

pub fn logical_type_spec(id: LogicalTypeId) -> Option<LogicalTypeSpec> {
	LogicalTypeResolver::seed_only().spec(id)
}

pub fn logical_type_spec_by_name(name: &str) -> Option<LogicalTypeSpec> {
	LogicalTypeResolver::seed_only().spec_by_name(name)
}

pub fn seed_logical_type_spec_by_name(name: &str) -> Option<LogicalTypeSpec> {
	[CUSTOMER_ID_LOGICAL_TYPE, PRODUCT_ID_LOGICAL_TYPE]
		.into_iter()
		.find_map(|id| seed_logical_type_spec(id).filter(|spec| spec.name == name))
}

pub fn logical_type_name(id: LogicalTypeId) -> String {
	logical_type_spec(id)
		.map(|spec| spec.name)
		.unwrap_or_else(|| format!("LogicalTypeId({})", id.0))
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LogicalTypeRegistry {
	by_id: HashMap<LogicalTypeId, LogicalTypeSpec>,
	by_name: HashMap<String, LogicalTypeId>,
}

impl LogicalTypeRegistry {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn from_definitions(
		definitions: impl IntoIterator<Item = LogicalTypeDefinition>,
	) -> Result<Self, LogicalTypeRegistryError> {
		let mut registry = Self::new();
		for definition in definitions {
			registry.register_definition(definition)?;
		}
		Ok(registry)
	}

	pub fn register_definition(
		&mut self,
		definition: LogicalTypeDefinition,
	) -> Result<LogicalTypeSpec, LogicalTypeRegistryError> {
		if definition.logical_type_id < DURABLE_LOGICAL_TYPE_MIN_ID {
			return Err(LogicalTypeRegistryError::ReservedId {
				id: definition.logical_type_id,
			});
		}
		if definition.name.eq_ignore_ascii_case("naked") {
			return Err(LogicalTypeRegistryError::ReservedName {
				name: definition.name,
			});
		}
		let spec = LogicalTypeSpec {
			id: LogicalTypeId(definition.logical_type_id),
			name: definition.name,
			physical: logical_type_physical_to_data_type(definition.physical),
			refinement: definition.refinement,
			unit: definition.unit,
		};
		if let Some(existing) = self.by_id.get(&spec.id) {
			if existing == &spec {
				return Ok(existing.clone());
			}
			return Err(LogicalTypeRegistryError::DuplicateId { id: spec.id.0 });
		}
		if let Some(existing_id) = self.by_name.get(&spec.name) {
			if *existing_id == spec.id {
				return Ok(spec);
			}
			return Err(LogicalTypeRegistryError::DuplicateName { name: spec.name });
		}
		self.by_name.insert(spec.name.clone(), spec.id);
		self.by_id.insert(spec.id, spec.clone());
		Ok(spec)
	}

	pub fn spec(&self, id: LogicalTypeId) -> Option<LogicalTypeSpec> {
		self.by_id.get(&id).cloned()
	}

	pub fn spec_by_name(&self, name: &str) -> Option<LogicalTypeSpec> {
		self.by_name
			.get(name)
			.and_then(|id| self.by_id.get(id))
			.cloned()
	}

	pub fn specs_sorted(&self) -> Vec<LogicalTypeSpec> {
		let mut specs = self.by_id.values().cloned().collect::<Vec<_>>();
		specs.sort_by(|left, right| {
			left.id
				.0
				.cmp(&right.id.0)
				.then_with(|| left.name.cmp(&right.name))
		});
		specs
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogicalTypeRegistryError {
	ReservedId { id: u32 },
	ReservedName { name: String },
	DuplicateId { id: u32 },
	DuplicateName { name: String },
}

impl std::fmt::Display for LogicalTypeRegistryError {
	fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::ReservedId { id } => write!(
				formatter,
				"logical type id {id} is in the reserved static seed range"
			),
			Self::ReservedName { name } => {
				write!(formatter, "logical type name {name:?} is reserved")
			}
			Self::DuplicateId { id } => {
				write!(
					formatter,
					"logical type id {id} already has a different definition"
				)
			}
			Self::DuplicateName { name } => write!(
				formatter,
				"logical type name {name:?} already has a different definition"
			),
		}
	}
}

impl std::error::Error for LogicalTypeRegistryError {}

#[derive(Debug, Clone, Copy)]
pub struct LogicalTypeResolver<'a> {
	registry: Option<&'a LogicalTypeRegistry>,
	include_seed: bool,
}

impl<'a> LogicalTypeResolver<'a> {
	pub fn seed_only() -> Self {
		Self {
			registry: None,
			include_seed: true,
		}
	}

	pub fn with_registry(registry: &'a LogicalTypeRegistry) -> Self {
		Self {
			registry: Some(registry),
			include_seed: true,
		}
	}

	pub fn durable_only(registry: &'a LogicalTypeRegistry) -> Self {
		Self {
			registry: Some(registry),
			include_seed: false,
		}
	}

	pub fn spec(self, id: LogicalTypeId) -> Option<LogicalTypeSpec> {
		if self.include_seed {
			seed_logical_type_spec(id)
		} else {
			None
		}
		.or_else(|| self.registry.and_then(|registry| registry.spec(id)))
	}

	pub fn spec_by_name(self, name: &str) -> Option<LogicalTypeSpec> {
		if self.include_seed {
			seed_logical_type_spec_by_name(name)
		} else {
			None
		}
		.or_else(|| {
			self.registry
				.and_then(|registry| registry.spec_by_name(name))
		})
	}
}

pub fn logical_type_physical_to_data_type(physical: LogicalTypePhysical) -> DataType {
	match physical {
		LogicalTypePhysical::Int64 => DataType::Int64,
		LogicalTypePhysical::Int32 => DataType::Int32,
		LogicalTypePhysical::Float64 => DataType::Float64,
		LogicalTypePhysical::Float32 => DataType::Float32,
		LogicalTypePhysical::Utf8 => DataType::Utf8,
		LogicalTypePhysical::Boolean => DataType::Boolean,
	}
}

pub fn data_type_to_logical_type_physical(data_type: &DataType) -> Option<LogicalTypePhysical> {
	match data_type {
		DataType::Int64 => Some(LogicalTypePhysical::Int64),
		DataType::Int32 => Some(LogicalTypePhysical::Int32),
		DataType::Float64 => Some(LogicalTypePhysical::Float64),
		DataType::Float32 => Some(LogicalTypePhysical::Float32),
		DataType::Utf8 => Some(LogicalTypePhysical::Utf8),
		DataType::Boolean => Some(LogicalTypePhysical::Boolean),
		_ => None,
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogicalTypeMetadataError {
	InvalidDecimal {
		field: String,
		value_preview: String,
	},
	UnknownId {
		field: String,
		id: u32,
	},
	PhysicalMismatch {
		field: String,
		logical_type: String,
		expected: DataType,
		actual: DataType,
	},
}

impl std::fmt::Display for LogicalTypeMetadataError {
	fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::InvalidDecimal {
				field,
				value_preview,
			} => write!(
				formatter,
				"logical type metadata for column {field} must be a decimal u32, got {value_preview:?}"
			),
			Self::UnknownId { field, id } => write!(
				formatter,
				"logical type metadata for column {field} references unknown id {id}"
			),
			Self::PhysicalMismatch {
				field,
				logical_type,
				expected,
				actual,
			} => write!(
				formatter,
				"logical type metadata for column {field} declares {logical_type}({expected:?}) on {actual:?}"
			),
		}
	}
}

impl std::error::Error for LogicalTypeMetadataError {}

pub fn decode_column_logical_type(
	field: &Field,
) -> Result<LogicalTypeId, LogicalTypeMetadataError> {
	let Some(raw) = field.metadata().get(LOGICAL_TYPE_ID_METADATA_KEY) else {
		return Ok(LogicalTypeId::NAKED);
	};
	let id = raw.parse::<u32>().map(LogicalTypeId).map_err(|_| {
		LogicalTypeMetadataError::InvalidDecimal {
			field: field.name().clone(),
			value_preview: metadata_value_preview(raw),
		}
	})?;
	if id.is_naked() {
		return Ok(id);
	}
	if let Some(spec) = seed_logical_type_spec(id) {
		if spec.physical != *field.data_type() {
			return Err(LogicalTypeMetadataError::PhysicalMismatch {
				field: field.name().clone(),
				logical_type: spec.name,
				expected: spec.physical,
				actual: field.data_type().clone(),
			});
		}
		return Ok(id);
	}
	if id.0 < DURABLE_LOGICAL_TYPE_MIN_ID {
		return Err(LogicalTypeMetadataError::UnknownId {
			field: field.name().clone(),
			id: id.0,
		});
	}
	Ok(id)
}

pub fn decode_column_logical_type_with_resolver(
	field: &Field,
	resolver: LogicalTypeResolver<'_>,
) -> Result<LogicalTypeId, LogicalTypeMetadataError> {
	let Some(raw) = field.metadata().get(LOGICAL_TYPE_ID_METADATA_KEY) else {
		return Ok(LogicalTypeId::NAKED);
	};
	let id = raw.parse::<u32>().map(LogicalTypeId).map_err(|_| {
		LogicalTypeMetadataError::InvalidDecimal {
			field: field.name().clone(),
			value_preview: metadata_value_preview(raw),
		}
	})?;
	if id.is_naked() {
		return Ok(id);
	}
	let Some(spec) = resolver.spec(id) else {
		return Err(LogicalTypeMetadataError::UnknownId {
			field: field.name().clone(),
			id: id.0,
		});
	};
	if spec.physical != *field.data_type() {
		return Err(LogicalTypeMetadataError::PhysicalMismatch {
			field: field.name().clone(),
			logical_type: spec.name,
			expected: spec.physical,
			actual: field.data_type().clone(),
		});
	}
	Ok(id)
}

pub fn field_with_logical_type_metadata(
	name: impl Into<String>,
	data_type: DataType,
	nullable: bool,
	logical_type: LogicalTypeId,
) -> Field {
	let field = Field::new(name, data_type, nullable);
	if logical_type.is_naked() {
		return field;
	}
	field.with_metadata(HashMap::from([(
		LOGICAL_TYPE_ID_METADATA_KEY.to_string(),
		logical_type.0.to_string(),
	)]))
}

fn metadata_value_preview(value: &str) -> String {
	const MAX_PREVIEW_CHARS: usize = 64;
	let mut chars = value.chars();
	let mut preview = chars.by_ref().take(MAX_PREVIEW_CHARS).collect::<String>();
	if chars.next().is_some() {
		preview.push_str("...");
	}
	preview
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MorselId(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum QueryProfileHint {
	PointRead,
	Scan,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CreateTableRowIdentity {
	AppendOnly,
	PrimaryKey { column: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateTableSchema {
	pub columns: Vec<CreateTableColumn>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateTableColumn {
	pub name: String,
	pub nullable: bool,
	pub column_type: CreateTableColumnType,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CreateTableColumnType {
	Physical(DataType),
	LogicalName(String),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ScalarValue {
	Boolean(Option<bool>),
	Int32(Option<i32>),
	Int64(Option<i64>),
	Date32(Option<i32>),
	Timestamp {
		value: Option<i64>,
		unit: TimeUnit,
	},
	Interval(Option<i64>),
	CalendarMonthInterval(Option<i32>),
	Float32(Option<f32>),
	Float64(Option<f64>),
	/// ADR 263: a fixed-point decimal mirroring Arrow `Decimal128(precision, scale)` (`i128` storage).
	Decimal128 {
		value: Option<i128>,
		precision: u8,
		scale: i8,
	},
	Utf8(Option<String>),
}

pub fn interval_data_type() -> DataType {
	DataType::Duration(TimeUnit::Nanosecond)
}

pub fn calendar_month_interval_data_type() -> DataType {
	DataType::Interval(IntervalUnit::YearMonth)
}

pub fn nanos_per_timestamp_unit(unit: TimeUnit) -> Option<i64> {
	match unit {
		TimeUnit::Second => Some(1_000_000_000),
		TimeUnit::Millisecond => Some(1_000_000),
		TimeUnit::Microsecond => Some(1_000),
		TimeUnit::Nanosecond => Some(1),
	}
}

impl ScalarValue {
	pub fn data_type(&self) -> DataType {
		match self {
			Self::Boolean(_) => DataType::Boolean,
			Self::Int32(_) => DataType::Int32,
			Self::Int64(_) => DataType::Int64,
			Self::Date32(_) => DataType::Date32,
			Self::Timestamp { unit, .. } => crate::timestamp::utc_timestamp_data_type(*unit),
			Self::Interval(_) => interval_data_type(),
			Self::CalendarMonthInterval(_) => calendar_month_interval_data_type(),
			Self::Float32(_) => DataType::Float32,
			Self::Float64(_) => DataType::Float64,
			Self::Decimal128 {
				precision, scale, ..
			} => DataType::Decimal128(*precision, *scale),
			Self::Utf8(_) => DataType::Utf8,
		}
	}
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum ScalarLiteralKey {
	Boolean(bool),
	Int32(i32),
	Int64(i64),
	Date32(i32),
	Timestamp(TimeUnit, i64),
	Float32(u32),
	Float64(u64),
	Utf8(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScalarLiteralKeyClassification {
	Key(ScalarLiteralKey),
	Null,
	Unsupported,
}

pub fn scalar_literal_key(value: &ScalarValue) -> ScalarLiteralKeyClassification {
	match value {
		ScalarValue::Boolean(Some(value)) => {
			ScalarLiteralKeyClassification::Key(ScalarLiteralKey::Boolean(*value))
		}
		ScalarValue::Boolean(None) => ScalarLiteralKeyClassification::Null,
		ScalarValue::Int32(Some(value)) => {
			ScalarLiteralKeyClassification::Key(ScalarLiteralKey::Int32(*value))
		}
		ScalarValue::Int32(None) => ScalarLiteralKeyClassification::Null,
		ScalarValue::Int64(Some(value)) => {
			ScalarLiteralKeyClassification::Key(ScalarLiteralKey::Int64(*value))
		}
		ScalarValue::Int64(None) => ScalarLiteralKeyClassification::Null,
		ScalarValue::Date32(Some(value)) => {
			ScalarLiteralKeyClassification::Key(ScalarLiteralKey::Date32(*value))
		}
		ScalarValue::Date32(None) => ScalarLiteralKeyClassification::Null,
		ScalarValue::Timestamp {
			value: Some(value),
			unit,
		} => ScalarLiteralKeyClassification::Key(ScalarLiteralKey::Timestamp(*unit, *value)),
		ScalarValue::Timestamp { value: None, .. } => ScalarLiteralKeyClassification::Null,
		ScalarValue::Interval(Some(_)) => ScalarLiteralKeyClassification::Unsupported,
		ScalarValue::Interval(None) => ScalarLiteralKeyClassification::Null,
		ScalarValue::CalendarMonthInterval(Some(_)) => ScalarLiteralKeyClassification::Unsupported,
		ScalarValue::CalendarMonthInterval(None) => ScalarLiteralKeyClassification::Null,
		ScalarValue::Float32(Some(value)) => {
			if !value.is_finite() {
				return ScalarLiteralKeyClassification::Unsupported;
			}
			let value = if *value == 0.0 { 0.0 } else { *value };
			ScalarLiteralKeyClassification::Key(ScalarLiteralKey::Float32(value.to_bits()))
		}
		ScalarValue::Float32(None) => ScalarLiteralKeyClassification::Null,
		ScalarValue::Float64(Some(value)) => {
			if !value.is_finite() {
				return ScalarLiteralKeyClassification::Unsupported;
			}
			let value = if *value == 0.0 { 0.0 } else { *value };
			ScalarLiteralKeyClassification::Key(ScalarLiteralKey::Float64(value.to_bits()))
		}
		ScalarValue::Float64(None) => ScalarLiteralKeyClassification::Null,
		ScalarValue::Utf8(Some(value)) => {
			ScalarLiteralKeyClassification::Key(ScalarLiteralKey::Utf8(value.clone()))
		}
		ScalarValue::Utf8(None) => ScalarLiteralKeyClassification::Null,
		// ADR 275: a Decimal GROUP BY key now groups in the executor (group_value/group_column). This arm is the
		// separate literal/stat-key path — a Decimal stat key feeds the Wendelon-produced storage-fact decode,
		// a distinct cross-ring cut — so a Decimal literal key stays unsupported here.
		ScalarValue::Decimal128 { value: Some(_), .. } => {
			ScalarLiteralKeyClassification::Unsupported
		}
		ScalarValue::Decimal128 { value: None, .. } => ScalarLiteralKeyClassification::Null,
	}
}

impl fmt::Display for ScalarValue {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::Boolean(Some(value)) => write!(f, "{value}"),
			Self::Boolean(None) => f.write_str("NULL"),
			Self::Int32(Some(value)) => write!(f, "{value}"),
			Self::Int32(None) => f.write_str("NULL"),
			Self::Int64(Some(value)) => write!(f, "{value}"),
			Self::Int64(None) => f.write_str("NULL"),
			Self::Date32(Some(value)) => f.write_str(&crate::date32::format_date_ymd(*value)),
			Self::Date32(None) => f.write_str("NULL"),
			Self::Timestamp {
				value: Some(value),
				unit,
			} => f.write_str(&crate::timestamp::format_utc_timestamp(*value, *unit)),
			Self::Timestamp { value: None, .. } => f.write_str("NULL"),
			Self::Interval(Some(value)) => write!(f, "{value}ns"),
			Self::Interval(None) => f.write_str("NULL"),
			Self::CalendarMonthInterval(Some(value)) => {
				let unit = if *value == 1 || *value == -1 {
					"month"
				} else {
					"months"
				};
				write!(f, "{value} {unit}")
			}
			Self::CalendarMonthInterval(None) => f.write_str("NULL"),
			Self::Float32(Some(value)) => write!(f, "{value}"),
			Self::Float32(None) => f.write_str("NULL"),
			Self::Float64(Some(value)) => write!(f, "{value}"),
			Self::Float64(None) => f.write_str("NULL"),
			Self::Decimal128 {
				value: Some(value),
				scale,
				..
			} => {
				let scale = *scale;
				if scale <= 0 {
					write!(f, "{value}e{}", -i32::from(scale))
				} else {
					let scale = scale as u32;
					let divisor = 10u128.pow(scale);
					let sign = if *value < 0 { "-" } else { "" };
					let abs = value.unsigned_abs();
					write!(
						f,
						"{sign}{}.{:0width$}",
						abs / divisor,
						abs % divisor,
						width = scale as usize
					)
				}
			}
			Self::Decimal128 { value: None, .. } => f.write_str("NULL"),
			Self::Utf8(Some(value)) => f.write_str(value),
			Self::Utf8(None) => f.write_str("NULL"),
		}
	}
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ColumnRef {
	pub qualifier: Option<TableRef>,
	pub name: ColumnName,
}

impl ColumnRef {
	pub fn unqualified(name: impl Into<String>) -> Self {
		Self {
			qualifier: None,
			name: ColumnName(name.into()),
		}
	}

	pub fn qualified(qualifier: TableRef, name: impl Into<String>) -> Self {
		Self {
			qualifier: Some(qualifier),
			name: ColumnName(name.into()),
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TemporalExtractField {
	Year,
	IsoYear,
	Quarter,
	Month,
	Week,
	IsoWeek,
	Day,
	DayOfWeek,
	DayOfYear,
	Hour,
	Minute,
	Second,
	Millisecond,
	Microsecond,
	Nanosecond,
}

impl TemporalExtractField {
	pub fn parse_name(value: &str) -> Option<Self> {
		match normalized_temporal_field_name(value).as_str() {
			"year" | "years" => Some(Self::Year),
			"isoyear" => Some(Self::IsoYear),
			"quarter" | "quarters" => Some(Self::Quarter),
			"month" | "months" => Some(Self::Month),
			"week" | "weeks" => Some(Self::Week),
			"isoweek" => Some(Self::IsoWeek),
			"day" | "days" => Some(Self::Day),
			"dow" | "dayofweek" => Some(Self::DayOfWeek),
			"doy" | "dayofyear" => Some(Self::DayOfYear),
			"hour" | "hours" => Some(Self::Hour),
			"minute" | "minutes" => Some(Self::Minute),
			"second" | "seconds" => Some(Self::Second),
			"millisecond" | "milliseconds" => Some(Self::Millisecond),
			"microsecond" | "microseconds" => Some(Self::Microsecond),
			"nanosecond" | "nanoseconds" => Some(Self::Nanosecond),
			_ => None,
		}
	}

	pub fn as_str(self) -> &'static str {
		match self {
			Self::Year => "year",
			Self::IsoYear => "isoyear",
			Self::Quarter => "quarter",
			Self::Month => "month",
			Self::Week => "week",
			Self::IsoWeek => "isoweek",
			Self::Day => "day",
			Self::DayOfWeek => "dow",
			Self::DayOfYear => "doy",
			Self::Hour => "hour",
			Self::Minute => "minute",
			Self::Second => "second",
			Self::Millisecond => "millisecond",
			Self::Microsecond => "microsecond",
			Self::Nanosecond => "nanosecond",
		}
	}

	pub fn is_date32_supported(self) -> bool {
		matches!(
			self,
			Self::Year
				| Self::IsoYear
				| Self::Quarter
				| Self::Month
				| Self::Week | Self::IsoWeek
				| Self::Day | Self::DayOfWeek
				| Self::DayOfYear
		)
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TemporalTruncField {
	Year,
	Month,
	Day,
	Hour,
	Minute,
	Second,
}

impl TemporalTruncField {
	pub fn parse_name(value: &str) -> Option<Self> {
		match normalized_temporal_field_name(value).as_str() {
			"year" | "years" => Some(Self::Year),
			"month" | "months" => Some(Self::Month),
			"day" | "days" => Some(Self::Day),
			"hour" | "hours" => Some(Self::Hour),
			"minute" | "minutes" => Some(Self::Minute),
			"second" | "seconds" => Some(Self::Second),
			_ => None,
		}
	}

	pub fn as_str(self) -> &'static str {
		match self {
			Self::Year => "year",
			Self::Month => "month",
			Self::Day => "day",
			Self::Hour => "hour",
			Self::Minute => "minute",
			Self::Second => "second",
		}
	}
}

fn normalized_temporal_field_name(value: &str) -> String {
	value
		.trim()
		.chars()
		.filter(|character| !matches!(character, '_' | '-' | ' '))
		.flat_map(char::to_lowercase)
		.collect()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Expr {
	Column(ColumnRef),
	Literal(ScalarValue),
	UntypedNull,
	LogicalAscription {
		expr: Box<Expr>,
		logical_type: LogicalTypeId,
	},
	SqlLogicalCast {
		expr: Box<Expr>,
		logical_name: String,
		try_cast: bool,
	},
	SqlConvert {
		expr: Box<Expr>,
		target_logical_name: String,
	},
	Cast(Box<Expr>, DataType),
	TryCast(Box<Expr>, DataType),
	Add(Box<Expr>, Box<Expr>),
	Sub(Box<Expr>, Box<Expr>),
	Mul(Box<Expr>, Box<Expr>),
	Div(Box<Expr>, Box<Expr>),
	Eq(Box<Expr>, Box<Expr>),
	Neq(Box<Expr>, Box<Expr>),
	Lt(Box<Expr>, Box<Expr>),
	Lte(Box<Expr>, Box<Expr>),
	Gt(Box<Expr>, Box<Expr>),
	Gte(Box<Expr>, Box<Expr>),
	And(Box<Expr>, Box<Expr>),
	Or(Box<Expr>, Box<Expr>),
	Not(Box<Expr>),
	InList {
		expr: Box<Expr>,
		list: Vec<Expr>,
		negated: bool,
	},
	Like {
		expr: Box<Expr>,
		pattern: String,
		negated: bool,
		case_insensitive: bool,
	},
	Substring {
		input: Box<Expr>,
		from: i64,
		length: Option<u64>,
	},
	Case {
		when_then: Vec<(Box<Expr>, Box<Expr>)>,
		else_expr: Option<Box<Expr>>,
	},
	IsNull(Box<Expr>),
	IsNotNull(Box<Expr>),
	Floor(Box<Expr>),
	TimeBucket(std::time::Duration, Box<Expr>),
	AtTimeZone(Box<Expr>, String),
	Extract(TemporalExtractField, Box<Expr>),
	DateTrunc(TemporalTruncField, Box<Expr>),
}

impl Expr {
	pub fn column(name: impl Into<String>) -> Self {
		Self::Column(ColumnRef::unqualified(name))
	}

	pub fn qualified_column(qualifier: TableRef, name: impl Into<String>) -> Self {
		Self::Column(ColumnRef::qualified(qualifier, name))
	}

	pub fn is_deterministic(&self) -> bool {
		true
	}

	pub fn is_pbc_pushdown_eligible(&self) -> bool {
		match self {
			Self::Column(_) | Self::Literal(_) => true,
			Self::UntypedNull => false,
			Self::LogicalAscription { expr, .. } => expr.is_pbc_pushdown_eligible(),
			Self::SqlLogicalCast { .. }
			| Self::SqlConvert { .. }
			| Self::Cast(_, _)
			| Self::TryCast(_, _)
			| Self::Add(_, _)
			| Self::Sub(_, _)
			| Self::Mul(_, _)
			| Self::Div(_, _)
			| Self::Like { .. }
			| Self::Substring { .. }
			| Self::Case { .. }
			| Self::Floor(_)
			| Self::TimeBucket(_, _)
			| Self::AtTimeZone(_, _)
			| Self::Extract(_, _)
			| Self::DateTrunc(_, _) => false,
			Self::Eq(left, right)
			| Self::Neq(left, right)
			| Self::Lt(left, right)
			| Self::Lte(left, right)
			| Self::Gt(left, right)
			| Self::Gte(left, right)
			| Self::And(left, right)
			| Self::Or(left, right) => left.is_pbc_pushdown_eligible() && right.is_pbc_pushdown_eligible(),
			Self::Not(inner) | Self::IsNull(inner) | Self::IsNotNull(inner) => {
				inner.is_pbc_pushdown_eligible()
			}
			Self::InList { expr, list, .. } => {
				expr.is_pbc_pushdown_eligible() && list.iter().all(Self::is_pbc_pushdown_eligible)
			}
		}
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AggregateFunction {
	CountAll,
	Count,
	Sum,
	Min,
	Max,
	Avg,
	LinearRegress,
	ApproxQuantile(f64),
}

/// Window functions over one `OVER (...)` spec. Ranking functions (ADR 241) depend only on the partition
/// + order. `Aggregate` (ADR 242) folds a frame (default `RANGE UNBOUNDED PRECEDING AND CURRENT ROW`, or
/// an explicit `ROWS`/`RANGE`/`GROUPS` frame, ADR 243–245). `Value` (ADR 246) returns an expression's
/// value at another row of the ordered partition — positional for `LAG`/`LEAD`, frame-boundary for
/// `FirstValue`/`LastValue`/`NthValue`. `Ntile` (ADR 249) divides the ordered partition into a fixed
/// number of near-equal buckets, like the ranking functions depending only on partition geometry.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum WindowFunction {
	RowNumber,
	Rank,
	DenseRank,
	/// ADR 249: divide the ordered partition into `buckets` near-equal groups (the first `len % buckets`
	/// hold one extra row) and emit the 1-based bucket index per row. Frame-independent.
	Ntile {
		buckets: u64,
	},
	Aggregate(AggregateFunction),
	Value(ValueFunction),
}

/// ADR 246: value/navigation window functions. The value expression rides on the window call's argument.
/// `Lag`/`Lead` are positional (offset from the current row, frame-independent) with a constant `default`
/// returned at the partition edge (`None` = NULL); `FirstValue`/`LastValue`/`NthValue` are frame-aware
/// (the first/last/nth row of the active frame).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ValueFunction {
	Lag {
		offset: u64,
		default: Option<ScalarValue>,
	},
	Lead {
		offset: u64,
		default: Option<ScalarValue>,
	},
	FirstValue,
	LastValue,
	NthValue {
		n: u64,
	},
}

/// ADR 243: an explicit `ROWS` window-frame bound. `Preceding(n)`/`Following(n)` are physical row offsets
/// from the current row; `Unbounded*` reach the partition edge.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, rune::Any)]
pub enum WindowFrameBound {
	UnboundedPreceding,
	Preceding(WindowFrameDelta),
	CurrentRow,
	Following(WindowFrameDelta),
	UnboundedFollowing,
}

/// ADR 243/244/245: the offset carried by a `Preceding`/`Following` frame bound. `Rows` is a physical row
/// count (ADR 243 `ROWS` frames); `Range` is a value distance in the ORDER BY key domain (ADR 244 `RANGE`
/// frames) — a numeric `ScalarValue` for a numeric key or an `Interval`/`CalendarMonthInterval` for a
/// temporal key; `Groups` is a peer-group count (ADR 245 `GROUPS` frames). The unit on the enclosing
/// `WindowFrame` selects which the bounds carry.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum WindowFrameDelta {
	Rows(u64),
	Range(ScalarValue),
	Groups(u64),
}

/// ADR 243/244/245: the frame unit. `Rows` = physical row offsets (243); `Range` = value-distance offsets
/// in the ORDER BY key domain (244); `Groups` = peer-group-count offsets (245). This completes the
/// explicit frame family.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum WindowFrameUnit {
	Rows,
	Range,
	Groups,
}

/// ADR 243/244/245: an explicit `<unit> BETWEEN <start> AND <end>` frame on an aggregate window call.
/// `ROWS` slides a physical `[start, end]` slice clamped to the partition; `RANGE` selects the contiguous
/// run of rows whose ORDER BY value lies in the value interval `[key − start, key + end]`; `GROUPS`
/// selects the contiguous rows spanning peer groups `[group − start, group + end]`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, rune::Any)]
pub struct WindowFrame {
	pub unit: WindowFrameUnit,
	pub start: WindowFrameBound,
	pub end: WindowFrameBound,
}

/// A single window function call in a `Window` node's projection: the function, the output column name
/// it appends (the SELECT alias or a derived name), its arguments (ranking + `COUNT(*)`: none; other
/// aggregate windows: exactly one), and an optional explicit `ROWS` frame (ADR 243; `None` = the 242
/// default frame).
#[derive(Debug, Clone, Serialize, Deserialize, rune::Any)]
pub struct WindowFunctionCall {
	pub function: WindowFunction,
	pub output_name: String,
	pub args: Vec<Expr>,
	pub frame: Option<WindowFrame>,
	/// ADR 252: `IGNORE NULLS` skips NULL values when navigating; `false` = `RESPECT NULLS` (the
	/// default, today's behavior). Only meaningful for value functions (ADR 246).
	pub ignore_nulls: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JoinKind {
	Inner,
	LeftOuter,
	RightOuter,
	FullOuter,
	LeftSemi,
	RightSemi,
	LeftAnti,
	RightAnti,
	/// Keyless cartesian product (ADR 239). Lowered from SQL `CROSS JOIN` and comma joins with no
	/// `ON`; output schema is the inner-join concatenation (left columns then right). Distributed
	/// execution is broadcast-only (never an empty-key hash partition).
	Cross,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SetOpAllKind {
	Intersect,
	Except,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JoinStrategy {
	HashPartitioned,
	Broadcast,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BroadcastBuildSide {
	Left,
	Right,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JoinStrategyIntent {
	Auto,
	Pinned {
		strategy: JoinStrategy,
		source: JoinStrategyPinSource,
		build_side: Option<BroadcastBuildSide>,
	},
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JoinStrategyPinSource {
	Direct,
	SqlHint,
	Rune,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SortExpr {
	pub expr: Expr,
	pub descending: bool,
	pub nulls_first: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum InsertLiteral {
	Null,
	Boolean(bool),
	Int64(i64),
	UInt64(u64),
	Date32(i32),
	Timestamp { value: i64, unit: TimeUnit },
	Float64(f64),
	Utf8(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReturningProjection {
	Star,
	Exprs {
		exprs: Vec<Expr>,
		output_names: Vec<Option<String>>,
	},
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogicalPlan {
	Scan {
		table: TableId,
		projection: Option<Vec<String>>,
	},
	ScanNamed {
		table: TableRef,
		projection: Option<Vec<String>>,
	},
	Alias {
		input: Box<LogicalPlan>,
		alias: TableRef,
		columns: Vec<String>,
	},
	Filter {
		input: Box<LogicalPlan>,
		predicate: Expr,
	},
	Project {
		input: Box<LogicalPlan>,
		exprs: Vec<Expr>,
		output_names: Vec<Option<String>>,
	},
	Distinct {
		input: Box<LogicalPlan>,
	},
	Union {
		left: Box<LogicalPlan>,
		right: Box<LogicalPlan>,
	},
	SetOpAll {
		left: Box<LogicalPlan>,
		right: Box<LogicalPlan>,
		kind: SetOpAllKind,
	},
	Repeat {
		input: Box<LogicalPlan>,
		count: Expr,
	},
	Aggregate {
		input: Box<LogicalPlan>,
		group_expr: Vec<Expr>,
		aggr_expr: Vec<(AggregateFunction, Vec<Expr>)>,
	},
	Exchange {
		input: Box<LogicalPlan>,
		partition_count: u32,
		key: Vec<Expr>,
	},
	Sort {
		input: Box<LogicalPlan>,
		order_by: Vec<SortExpr>,
		limit: Option<usize>,
		offset: usize,
	},
	/// ADR 241: ranking window functions over one `OVER (PARTITION BY ... ORDER BY ...)` spec.
	/// Row-preserving — appends one column per `functions` entry. The planner places an `Exchange`
	/// on `partition_by` beneath this node; the operator orders each co-located partition internally.
	Window {
		input: Box<LogicalPlan>,
		partition_by: Vec<Expr>,
		order_by: Vec<SortExpr>,
		functions: Vec<WindowFunctionCall>,
	},
	Take {
		input: Box<LogicalPlan>,
		limit: usize,
	},
	Join {
		left: Box<LogicalPlan>,
		right: Box<LogicalPlan>,
		kind: JoinKind,
		on: Vec<(Expr, Expr)>,
		/// ADR 308: transient pre-bind ON residual for side-local pushdown. The binder rewrites supported
		/// residuals into a `Filter` below this key-only join, or rejects residuals that need full
		/// theta-join match semantics. No `ExecutionPlan::Join` carries this field.
		on_filter: Option<Expr>,
		null_safe_keys: bool,
		strategy_intent: JoinStrategyIntent,
	},
	/// ADR 279: a correlated EXISTS/NOT EXISTS the frontend could not classify syntactically (unqualified
	/// correlated refs). A purely transient, pre-bind node: the binder resolves each `predicates` conjunct's
	/// column scope (catalog-aware) into the real `LeftSemi`/`LeftAnti` `on` keys + inner residuals and emits a
	/// `Join`, so no later pass, the executor, or the fragmenter ever sees this variant. `negated` → `LeftAnti`.
	DeferredExists {
		outer: Box<LogicalPlan>,
		inner: Box<LogicalPlan>,
		predicates: Vec<Expr>,
		negated: bool,
	},
	/// ADR 282: a correlated scalar subquery the frontend could not classify syntactically (an unqualified
	/// correlated ref), the scalar analogue of [`LogicalPlan::DeferredExists`]. Transient + pre-bind: the binder
	/// resolves each `predicates` conjunct's column scope (catalog-aware) into the correlation keys, builds the
	/// grouped-aggregate + `LeftOuter` recombine ADR 260 builds (a global aggregate when no correlation pair is
	/// found), and emits a `Join` exposing `<alias>.value` — so no later pass, the executor, or the fragmenter
	/// ever sees this variant. The projection's scalar wrapper (e.g. `0.2 *`) already rides the rewritten outer
	/// expression in `outer` (which references `<alias>.value`); the subplan computes the bare `aggregate`.
	DeferredScalarSubquery {
		outer: Box<LogicalPlan>,
		inner: Box<LogicalPlan>,
		predicates: Vec<Expr>,
		aggregate: (AggregateFunction, Vec<Expr>),
		alias: String,
	},
	Explain {
		input: Box<LogicalPlan>,
	},
	ExplainAnalyze {
		input: Box<LogicalPlan>,
	},
	CreateTable {
		name: TableRef,
		schema: CreateTableSchema,
		row_identity: CreateTableRowIdentity,
		if_not_exists: bool,
		or_replace: bool,
	},
	CreateLogicalType {
		name: String,
		physical: DataType,
		refinement: Option<LogicalTypeRefinementPredicate>,
		unit: Option<LogicalTypeUnit>,
	},
	CreateTableAsSelect {
		table: TableRef,
		/// Canonical SQL request bytes retained for durable CTAS replay identity.
		intent_request: String,
		if_not_exists: bool,
		or_replace: bool,
		source: Box<LogicalPlan>,
	},
	DropTable {
		name: TableRef,
		if_exists: bool,
	},
	Delete {
		table: TableRef,
		predicate: Option<Expr>,
		returning: Option<ReturningProjection>,
	},
	Update {
		table: TableRef,
		assignments: Vec<(String, Expr)>,
		predicate: Option<Expr>,
		returning: Option<ReturningProjection>,
	},
	Insert {
		table: TableRef,
		columns: Option<Vec<String>>,
		rows: Vec<Vec<InsertLiteral>>,
		returning: Option<ReturningProjection>,
	},
	InsertSelect {
		table: TableRef,
		columns: Option<Vec<String>>,
		source: Box<LogicalPlan>,
		returning: Option<ReturningProjection>,
	},
	/// ADR 255: a recursive CTE — `WITH RECURSIVE name(cols) AS (seed UNION ALL recursive)`. The
	/// `recursive` term contains one or more `RecursiveRef` leaves resolving to the working table. This
	/// first cut is `UNION ALL` only (no `all` flag) and executes as a local fixpoint.
	RecursiveCte {
		name: String,
		column_names: Vec<String>,
		seed: Box<LogicalPlan>,
		recursive: Box<LogicalPlan>,
	},
	/// ADR 255: the self-reference to a recursive CTE inside its own recursive term. Resolves to the
	/// working table at execution; carries only column names (types come from the seed at bind).
	RecursiveRef {
		name: String,
		column_names: Vec<String>,
	},
}

pub fn project_plan(
	input: LogicalPlan,
	exprs: Vec<Expr>,
	output_names: Vec<Option<String>>,
) -> LogicalPlan {
	debug_assert_eq!(
		exprs.len(),
		output_names.len(),
		"Project exprs/output_names length mismatch"
	);
	LogicalPlan::Project {
		input: Box::new(input),
		exprs,
		output_names,
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn adr490_hash_build_estimate_pins_m06_raw_budget_boundary() {
		let m06_rows = 12_000_000;
		let int64_width = hash_join_build_key_width(&DataType::Int64);
		let estimate = estimated_hash_join_build_bytes(m06_rows, int64_width);

		assert_eq!(int64_width, 8);
		assert_eq!(estimate, 1_440_000_000);
		assert!(estimate > 64 * 1024 * 1024);
		assert!(estimate <= 2 * 1024 * 1024 * 1024);
	}

	#[test]
	fn try_cast_is_not_pbc_pushdown_eligible() {
		let try_cast = Expr::TryCast(Box::new(Expr::column("amount")), DataType::Int64);

		assert!(!try_cast.is_pbc_pushdown_eligible());
	}
}
