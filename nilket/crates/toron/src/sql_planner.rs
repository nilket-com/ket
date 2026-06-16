use crate::aggregate_kernel;
use crate::catalog::TableRef;
use crate::date32::parse_date_ymd;
use crate::error::FrontendError;
use crate::ir::{
	AggregateFunction, BroadcastBuildSide, CreateTableColumn, CreateTableColumnType,
	CreateTableRowIdentity, CreateTableSchema, Expr, InsertLiteral, JoinKind, JoinStrategy,
	JoinStrategyIntent, JoinStrategyPinSource, LogicalPlan, ReturningProjection, ScalarValue,
	SetOpAllKind, SortExpr, TemporalExtractField, TemporalTruncField, ValueFunction, WindowFrame,
	WindowFrameBound, WindowFrameDelta, WindowFrameUnit, WindowFunction, WindowFunctionCall,
	data_type_to_logical_type_physical,
};
use crate::timestamp::{parse_explicit_timestamptz, utc_timestamp_data_type};
use arrow::datatypes::{DataType, TimeUnit};
use sqlparser::ast::{
	Assignment, AssignmentTarget, BinaryOperator, CaseWhen, CastKind, ColumnDef, ColumnOption,
	ColumnOptionDef, CreateTable, CreateTableOptions, DataType as SqlDataType, DateTimeField,
	Delete, DescribeAlias, Distinct, DuplicateTreatment, ExactNumberInfo, Expr as SqlExpr,
	FromTable, Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments,
	GroupByExpr, HiveDistributionStyle, Ident, Insert, Interval as SqlInterval, JoinConstraint,
	JoinOperator, LimitClause, NamedWindowDefinition, NamedWindowExpr, ObjectName, ObjectNamePart,
	ObjectType, OptimizerHint, OptimizerHintStyle, OrderBy, OrderByKind, Query, Select, SelectItem,
	SetExpr, SetOperator, SetQuantifier, Statement, TableAlias, TableConstraint, TableFactor,
	TableObject, TimezoneInfo, TypedString, UnaryOperator, Value, WindowSpec, WindowType,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use wendelon::schema::{
	STORAGE_ROW_HLC_COLUMN, STORAGE_ROW_KEY_ID_COLUMN, STORAGE_ROW_ROUTING_LOCALITY_COLUMN,
	STORAGE_ROW_TABLE_OBJECT_ID_COLUMN, STORAGE_ROW_TOMBSTONE_COLUMN,
};
use wendelon_client::{
	LogicalTypePhysical, LogicalTypeRefinementComparisonOp, LogicalTypeRefinementPredicate,
	LogicalTypeRefinementValue, LogicalTypeUnit, UnitOffset, UnitQuantityKind, UnitScale,
};

fn unsupported(message: impl Into<String>) -> FrontendError {
	FrontendError::Unsupported {
		message: message.into(),
	}
}

fn wrap_distinct_if_needed(plan: LogicalPlan, distinct: bool) -> LogicalPlan {
	if distinct {
		LogicalPlan::Distinct {
			input: Box::new(plan),
		}
	} else {
		plan
	}
}

const DEFAULT_JOIN_PARTITION_COUNT: u32 = 4;
// P0.3: grouped SQL reuses the join shuffle fanout until aggregate fanout has separate evidence.
const DEFAULT_AGGREGATE_PARTITION_COUNT: u32 = DEFAULT_JOIN_PARTITION_COUNT;
const MAX_IN_LIST_ITEMS: usize = 32;
const AGGREGATE_ARGUMENT_SHAPE_MESSAGE: &str = "unsupported aggregate argument";
const NESTED_AGGREGATE_ARGUMENT_MESSAGE: &str =
	"aggregate arguments must not contain aggregate calls";
const STORAGE_SYNTHETIC_COLUMN_NAMES: &[&str] = &[
	STORAGE_ROW_HLC_COLUMN,
	STORAGE_ROW_KEY_ID_COLUMN,
	STORAGE_ROW_ROUTING_LOCALITY_COLUMN,
	STORAGE_ROW_TABLE_OBJECT_ID_COLUMN,
	STORAGE_ROW_TOMBSTONE_COLUMN,
];

#[derive(Debug, Clone, PartialEq, Eq)]
struct BroadcastJoinHint {
	target: TableRef,
	consumed: bool,
}

fn object_name_part_ident<'a>(
	part: &'a ObjectNamePart,
	context: &str,
) -> Result<&'a Ident, FrontendError> {
	part.as_ident().ok_or_else(|| {
		unsupported(format!(
			"{context} does not support function-derived object name parts"
		))
	})
}

fn object_name_part_value(part: &ObjectNamePart, context: &str) -> Result<String, FrontendError> {
	Ok(object_name_part_ident(part, context)?.value.clone())
}

fn sql_value(expr: &SqlExpr) -> Option<&Value> {
	match expr {
		SqlExpr::Value(value) => Some(&value.value),
		_ => None,
	}
}

fn stripped_sql_value(expr: &SqlExpr) -> Option<&Value> {
	sql_value(strip_nested_sql_expr(expr))
}

fn expr_is_null_literal(expr: &SqlExpr) -> bool {
	matches!(stripped_sql_value(expr), Some(Value::Null))
}

fn parse_broadcast_join_hint(
	hints: &[OptimizerHint],
) -> Result<Option<BroadcastJoinHint>, FrontendError> {
	if hints.is_empty() {
		return Ok(None);
	}
	if hints.len() != 1 {
		return Err(unsupported(
			"only one /*+ BROADCAST(alias) */ optimizer hint is supported",
		));
	}

	let hint = &hints[0];
	if !hint.prefix.is_empty() || !matches!(&hint.style, OptimizerHintStyle::MultiLine) {
		return Err(unsupported(
			"only /*+ BROADCAST(alias) */ optimizer hints are supported",
		));
	}

	Ok(Some(BroadcastJoinHint {
		target: parse_broadcast_hint_text(&hint.text)?,
		consumed: false,
	}))
}

fn parse_broadcast_hint_text(text: &str) -> Result<TableRef, FrontendError> {
	const BROADCAST: &str = "BROADCAST";
	let text = text.trim();
	if text.len() < BROADCAST.len() {
		return Err(unsupported(
			"only /*+ BROADCAST(alias) */ optimizer hints are supported",
		));
	}
	let (keyword, rest) = text.split_at(BROADCAST.len());
	if !keyword.eq_ignore_ascii_case(BROADCAST) {
		return Err(unsupported(
			"only /*+ BROADCAST(alias) */ optimizer hints are supported",
		));
	}
	let rest = rest.trim_start();
	let Some(rest) = rest.strip_prefix('(') else {
		return Err(unsupported(
			"only /*+ BROADCAST(alias) */ optimizer hints are supported",
		));
	};
	let Some(close_paren) = rest.find(')') else {
		return Err(unsupported(
			"only /*+ BROADCAST(alias) */ optimizer hints are supported",
		));
	};
	let target = rest[..close_paren].trim();
	let trailing = rest[close_paren + 1..].trim();
	if !trailing.is_empty() || !is_unquoted_ascii_identifier(target) {
		return Err(unsupported(
			"only /*+ BROADCAST(alias) */ optimizer hints are supported",
		));
	}
	Ok(TableRef::new(target.to_string()))
}

fn is_unquoted_ascii_identifier(value: &str) -> bool {
	let mut bytes = value.bytes();
	let Some(first) = bytes.next() else {
		return false;
	};
	if !(first.is_ascii_alphabetic() || first == b'_') {
		return false;
	}
	bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

fn is_row_value_expr(expr: &SqlExpr) -> bool {
	matches!(strip_nested_sql_expr(expr), SqlExpr::Tuple(_))
}

fn validate_simple_aggregate_arg(arg: &FunctionArg) -> Result<(), FrontendError> {
	match arg {
		FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => validate_aggregate_argument(expr),
		_ => Err(unsupported(AGGREGATE_ARGUMENT_SHAPE_MESSAGE)),
	}
}

fn validate_aggregate_argument(expr: &SqlExpr) -> Result<(), FrontendError> {
	reject_nested_aggregate(expr)
}

fn reject_nested_aggregate(expr: &SqlExpr) -> Result<(), FrontendError> {
	match expr {
		SqlExpr::Function(function) if aggregate_function_name(&function.name).is_some() => {
			Err(unsupported(NESTED_AGGREGATE_ARGUMENT_MESSAGE))
		}
		SqlExpr::Function(function) => {
			if let FunctionArguments::List(list) = &function.args {
				for arg in &list.args {
					match arg {
						FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
						| FunctionArg::Named {
							arg: FunctionArgExpr::Expr(expr),
							..
						}
						| FunctionArg::ExprNamed {
							arg: FunctionArgExpr::Expr(expr),
							..
						} => reject_nested_aggregate(expr)?,
						FunctionArg::Unnamed(FunctionArgExpr::Wildcard)
						| FunctionArg::Unnamed(FunctionArgExpr::QualifiedWildcard(_)) => {}
						_ => {}
					}
				}
			}
			Ok(())
		}
		SqlExpr::BinaryOp { left, right, .. } => {
			reject_nested_aggregate(left)?;
			reject_nested_aggregate(right)
		}
		SqlExpr::Case {
			operand,
			conditions,
			else_result,
			..
		} => {
			if let Some(operand) = operand {
				reject_nested_aggregate(operand)?;
			}
			for case_when in conditions {
				reject_nested_aggregate(&case_when.condition)?;
				reject_nested_aggregate(&case_when.result)?;
			}
			if let Some(else_result) = else_result {
				reject_nested_aggregate(else_result)?;
			}
			Ok(())
		}
		SqlExpr::Cast { expr, .. }
		| SqlExpr::Extract { expr, .. }
		| SqlExpr::Interval(SqlInterval { value: expr, .. })
		| SqlExpr::UnaryOp { expr, .. } => reject_nested_aggregate(expr),
		SqlExpr::AtTimeZone {
			timestamp,
			time_zone,
		} => {
			reject_nested_aggregate(timestamp)?;
			reject_nested_aggregate(time_zone)
		}
		SqlExpr::IsNull(inner) | SqlExpr::IsNotNull(inner) | SqlExpr::Nested(inner) => {
			reject_nested_aggregate(inner)
		}
		_ => Ok(()),
	}
}

enum LoweredCastTarget {
	Physical(DataType),
	LogicalName(String),
}

fn lower_cast_target(data_type: &SqlDataType) -> Result<LoweredCastTarget, FrontendError> {
	match data_type {
		SqlDataType::Custom(name, modifiers) => Ok(LoweredCastTarget::LogicalName(
			lower_cast_logical_type_name(name, modifiers)?,
		)),
		other => Ok(LoweredCastTarget::Physical(lower_cast_target_type(other)?)),
	}
}

fn lower_cast_target_type(data_type: &SqlDataType) -> Result<DataType, FrontendError> {
	match data_type {
		SqlDataType::Int(_)
		| SqlDataType::Integer(_)
		| SqlDataType::Int4(_)
		| SqlDataType::Int32 => Ok(DataType::Int32),
		SqlDataType::BigInt(_) | SqlDataType::Int64 => Ok(DataType::Int64),
		SqlDataType::Real | SqlDataType::Float4 | SqlDataType::Float32 => Ok(DataType::Float32),
		SqlDataType::Double(ExactNumberInfo::None)
		| SqlDataType::DoublePrecision
		| SqlDataType::Float8
		| SqlDataType::Float64 => Ok(DataType::Float64),
		SqlDataType::Date | SqlDataType::Date32 => Ok(DataType::Date32),
		SqlDataType::Timestamp(precision, TimezoneInfo::WithTimeZone | TimezoneInfo::Tz) => Ok(
			utc_timestamp_data_type(timestamp_unit_for_precision(*precision)?),
		),
		SqlDataType::Timestamp(_, TimezoneInfo::None | TimezoneInfo::WithoutTimeZone)
		| SqlDataType::TimestampNtz(_) => Err(unsupported(
			"TIMESTAMP WITHOUT TIME ZONE is not supported; use TIMESTAMPTZ",
		)),
		SqlDataType::Float(_) => Err(unsupported(
			"unsupported CAST target type: FLOAT (precision-parameter ambiguous; use REAL, DOUBLE PRECISION, FLOAT32, or FLOAT64)",
		)),
		other => Err(unsupported(format!(
			"unsupported CAST target type: {other}"
		))),
	}
}

fn is_date_sql_type(data_type: &SqlDataType) -> bool {
	matches!(data_type, SqlDataType::Date | SqlDataType::Date32)
}

fn timestamp_unit_for_precision(precision: Option<u64>) -> Result<TimeUnit, FrontendError> {
	match precision {
		None | Some(9) => Ok(TimeUnit::Nanosecond),
		Some(6) => Ok(TimeUnit::Microsecond),
		Some(3) => Ok(TimeUnit::Millisecond),
		Some(other) => Err(unsupported(format!(
			"unsupported TIMESTAMPTZ precision {other}; use 3, 6, or 9"
		))),
	}
}

fn timestamptz_sql_unit(data_type: &SqlDataType) -> Result<Option<TimeUnit>, FrontendError> {
	match data_type {
		SqlDataType::Timestamp(precision, TimezoneInfo::WithTimeZone | TimezoneInfo::Tz) => {
			Ok(Some(timestamp_unit_for_precision(*precision)?))
		}
		SqlDataType::Timestamp(_, TimezoneInfo::None | TimezoneInfo::WithoutTimeZone)
		| SqlDataType::TimestampNtz(_) => Err(unsupported(
			"TIMESTAMP WITHOUT TIME ZONE is not supported; use TIMESTAMPTZ",
		)),
		_ => Ok(None),
	}
}

fn typed_string_value(typed: &TypedString) -> Result<String, FrontendError> {
	typed
		.value
		.clone()
		.into_string()
		.ok_or_else(|| unsupported("typed string literal body must be a string literal"))
}

fn function_name_eq(name: &ObjectName, expected: &str) -> bool {
	name.to_string().eq_ignore_ascii_case(expected)
}

fn function_arg_list<'a>(
	args: &'a FunctionArguments,
	error_message: &'static str,
) -> Result<&'a [FunctionArg], FrontendError> {
	match args {
		FunctionArguments::List(list) => Ok(&list.args),
		_ => Err(unsupported(error_message)),
	}
}

fn string_literal_function_arg(
	arg: &FunctionArg,
	error_message: &'static str,
) -> Result<String, FrontendError> {
	match arg {
		FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
			sql_string_literal_expr(expr, error_message)
		}
		_ => Err(unsupported(error_message)),
	}
}

fn sql_string_literal_expr(
	expr: &SqlExpr,
	error_message: &'static str,
) -> Result<String, FrontendError> {
	match sql_value(expr) {
		Some(Value::SingleQuotedString(value)) => Ok(value.clone()),
		_ => Err(unsupported(error_message)),
	}
}

fn lower_temporal_extract_name(value: &str) -> Result<TemporalExtractField, FrontendError> {
	TemporalExtractField::parse_name(value)
		.ok_or_else(|| unsupported(format!("unsupported EXTRACT field: {value}")))
}

fn lower_temporal_trunc_name(value: &str) -> Result<TemporalTruncField, FrontendError> {
	TemporalTruncField::parse_name(value)
		.ok_or_else(|| unsupported(format!("unsupported date_trunc field: {value}")))
}

fn lower_extract_datetime_field(
	field: &DateTimeField,
) -> Result<TemporalExtractField, FrontendError> {
	match field {
		DateTimeField::Year | DateTimeField::Years => Ok(TemporalExtractField::Year),
		DateTimeField::Isoyear => Ok(TemporalExtractField::IsoYear),
		DateTimeField::Quarter => Ok(TemporalExtractField::Quarter),
		DateTimeField::Month | DateTimeField::Months => Ok(TemporalExtractField::Month),
		DateTimeField::Week(None) | DateTimeField::Weeks => Ok(TemporalExtractField::Week),
		DateTimeField::Week(Some(_)) => {
			Err(unsupported(format!("unsupported EXTRACT field: {field}")))
		}
		DateTimeField::IsoWeek => Ok(TemporalExtractField::IsoWeek),
		DateTimeField::Day | DateTimeField::Days => Ok(TemporalExtractField::Day),
		DateTimeField::Dow | DateTimeField::DayOfWeek => Ok(TemporalExtractField::DayOfWeek),
		DateTimeField::Doy | DateTimeField::DayOfYear => Ok(TemporalExtractField::DayOfYear),
		DateTimeField::Hour | DateTimeField::Hours => Ok(TemporalExtractField::Hour),
		DateTimeField::Minute | DateTimeField::Minutes => Ok(TemporalExtractField::Minute),
		DateTimeField::Second | DateTimeField::Seconds => Ok(TemporalExtractField::Second),
		DateTimeField::Millisecond | DateTimeField::Milliseconds => {
			Ok(TemporalExtractField::Millisecond)
		}
		DateTimeField::Microsecond | DateTimeField::Microseconds => {
			Ok(TemporalExtractField::Microsecond)
		}
		DateTimeField::Nanosecond | DateTimeField::Nanoseconds => {
			Ok(TemporalExtractField::Nanosecond)
		}
		DateTimeField::Century
		| DateTimeField::Decade
		| DateTimeField::Epoch
		| DateTimeField::Isodow
		| DateTimeField::Julian
		| DateTimeField::Millenium
		| DateTimeField::Millennium
		| DateTimeField::Date
		| DateTimeField::Datetime
		| DateTimeField::Time
		| DateTimeField::Timezone
		| DateTimeField::TimezoneAbbr
		| DateTimeField::TimezoneHour
		| DateTimeField::TimezoneMinute
		| DateTimeField::TimezoneRegion
		| DateTimeField::NoDateTime
		| DateTimeField::Custom(_) => Err(unsupported(format!("unsupported EXTRACT field: {field}"))),
	}
}

fn lower_interval_literal(interval: &SqlInterval) -> Result<Expr, FrontendError> {
	if interval.last_field.is_some()
		|| interval.leading_precision.is_some()
		|| interval.fractional_seconds_precision.is_some()
	{
		return Err(unsupported(
			"INTERVAL ranges and precision are not supported; use a single supported unit",
		));
	}
	let value = match &interval.leading_field {
		Some(field) => {
			let amount = interval_literal_integer(&interval.value)?;
			interval_field_value(amount, field)
		}
		None => {
			let value = interval_literal_string(&interval.value)?;
			parse_simple_interval_string(&value)
		}
	}?;
	Ok(Expr::Literal(match value {
		LoweredIntervalLiteral::FixedNanos(nanos) => ScalarValue::Interval(Some(nanos)),
		LoweredIntervalLiteral::CalendarMonths(months) => {
			ScalarValue::CalendarMonthInterval(Some(months))
		}
	}))
}

enum LoweredIntervalLiteral {
	FixedNanos(i64),
	CalendarMonths(i32),
}

fn interval_literal_integer(expr: &SqlExpr) -> Result<i64, FrontendError> {
	match strip_nested_sql_expr(expr) {
		SqlExpr::Value(value) => match &value.value {
			Value::Number(value, _) => parse_interval_i64(value),
			Value::SingleQuotedString(value) => parse_interval_i64(value.trim()),
			_ => Err(unsupported("INTERVAL value must be an integer literal")),
		},
		SqlExpr::UnaryOp {
			op: UnaryOperator::Minus,
			expr,
		} => interval_literal_integer(expr).and_then(|value| {
			value
				.checked_neg()
				.ok_or_else(|| unsupported("INTERVAL value overflows i64 range"))
		}),
		SqlExpr::UnaryOp {
			op: UnaryOperator::Plus,
			expr,
		} => interval_literal_integer(expr),
		_ => Err(unsupported("INTERVAL value must be a literal")),
	}
}

fn interval_literal_string(expr: &SqlExpr) -> Result<String, FrontendError> {
	match strip_nested_sql_expr(expr) {
		SqlExpr::Value(value) => match &value.value {
			Value::SingleQuotedString(value) => Ok(value.clone()),
			_ => Err(unsupported(
				"INTERVAL without an explicit unit expects a string literal like '7 days'",
			)),
		},
		_ => Err(unsupported(
			"INTERVAL without an explicit unit expects a string literal like '7 days'",
		)),
	}
}

fn parse_simple_interval_string(value: &str) -> Result<LoweredIntervalLiteral, FrontendError> {
	let parts = value.split_whitespace().collect::<Vec<_>>();
	if parts.len() != 2 {
		return Err(unsupported(
			"INTERVAL string must be a single amount and unit",
		));
	}
	let amount = parse_interval_i64(parts[0])?;
	interval_word_value(amount, parts[1])
}

fn parse_interval_i64(value: &str) -> Result<i64, FrontendError> {
	value
		.parse::<i64>()
		.map_err(|_| unsupported("INTERVAL value must be an integer literal within i64 range"))
}

fn checked_interval_nanos(amount: i64, nanos_per_unit: i64) -> Result<i64, FrontendError> {
	(i128::from(amount) * i128::from(nanos_per_unit))
		.try_into()
		.map_err(|_| unsupported("INTERVAL literal overflows i64 nanoseconds"))
}

fn checked_interval_months(amount: i64, months_per_unit: i64) -> Result<i32, FrontendError> {
	(i128::from(amount) * i128::from(months_per_unit))
		.try_into()
		.map_err(|_| unsupported("INTERVAL literal overflows i32 calendar months"))
}

fn interval_field_value(
	amount: i64,
	field: &DateTimeField,
) -> Result<LoweredIntervalLiteral, FrontendError> {
	match field {
		DateTimeField::Nanosecond | DateTimeField::Nanoseconds => Ok(
			LoweredIntervalLiteral::FixedNanos(checked_interval_nanos(amount, 1)?),
		),
		DateTimeField::Microsecond | DateTimeField::Microseconds => Ok(
			LoweredIntervalLiteral::FixedNanos(checked_interval_nanos(amount, 1_000)?),
		),
		DateTimeField::Millisecond | DateTimeField::Milliseconds => Ok(
			LoweredIntervalLiteral::FixedNanos(checked_interval_nanos(amount, 1_000_000)?),
		),
		DateTimeField::Second | DateTimeField::Seconds => Ok(LoweredIntervalLiteral::FixedNanos(
			checked_interval_nanos(amount, 1_000_000_000)?,
		)),
		DateTimeField::Minute | DateTimeField::Minutes => Ok(LoweredIntervalLiteral::FixedNanos(
			checked_interval_nanos(amount, 60 * 1_000_000_000)?,
		)),
		DateTimeField::Hour | DateTimeField::Hours => Ok(LoweredIntervalLiteral::FixedNanos(
			checked_interval_nanos(amount, 60 * 60 * 1_000_000_000)?,
		)),
		DateTimeField::Day | DateTimeField::Days => Ok(LoweredIntervalLiteral::FixedNanos(
			checked_interval_nanos(amount, 24 * 60 * 60 * 1_000_000_000)?,
		)),
		DateTimeField::Month | DateTimeField::Months => Ok(LoweredIntervalLiteral::CalendarMonths(
			checked_interval_months(amount, 1)?,
		)),
		DateTimeField::Year | DateTimeField::Years => Ok(LoweredIntervalLiteral::CalendarMonths(
			checked_interval_months(amount, 12)?,
		)),
		DateTimeField::Week(_) | DateTimeField::Weeks | DateTimeField::Quarter => {
			Err(unsupported(format!(
				"calendar INTERVAL unit {field} is not supported; use month/year or day-and-smaller fixed intervals"
			)))
		}
		other => Err(unsupported(format!("unsupported INTERVAL unit: {other}"))),
	}
}

fn interval_word_value(amount: i64, unit: &str) -> Result<LoweredIntervalLiteral, FrontendError> {
	match unit.to_ascii_lowercase().as_str() {
		"nanosecond" | "nanoseconds" | "ns" => Ok(LoweredIntervalLiteral::FixedNanos(
			checked_interval_nanos(amount, 1)?,
		)),
		"microsecond" | "microseconds" | "us" => Ok(LoweredIntervalLiteral::FixedNanos(
			checked_interval_nanos(amount, 1_000)?,
		)),
		"millisecond" | "milliseconds" | "ms" => Ok(LoweredIntervalLiteral::FixedNanos(
			checked_interval_nanos(amount, 1_000_000)?,
		)),
		"second" | "seconds" | "s" => Ok(LoweredIntervalLiteral::FixedNanos(
			checked_interval_nanos(amount, 1_000_000_000)?,
		)),
		"minute" | "minutes" | "min" | "mins" => Ok(LoweredIntervalLiteral::FixedNanos(
			checked_interval_nanos(amount, 60 * 1_000_000_000)?,
		)),
		"hour" | "hours" | "h" => Ok(LoweredIntervalLiteral::FixedNanos(checked_interval_nanos(
			amount,
			60 * 60 * 1_000_000_000,
		)?)),
		"day" | "days" | "d" => Ok(LoweredIntervalLiteral::FixedNanos(checked_interval_nanos(
			amount,
			24 * 60 * 60 * 1_000_000_000,
		)?)),
		"month" | "months" => Ok(LoweredIntervalLiteral::CalendarMonths(
			checked_interval_months(amount, 1)?,
		)),
		"year" | "years" => Ok(LoweredIntervalLiteral::CalendarMonths(
			checked_interval_months(amount, 12)?,
		)),
		"week" | "weeks" | "quarter" | "quarters" => Err(unsupported(format!(
			"calendar INTERVAL unit {unit} is not supported; use month/year or day-and-smaller fixed intervals"
		))),
		_ => Err(unsupported(format!("unsupported INTERVAL unit: {unit}"))),
	}
}

fn take_sql_word(input: &str) -> Option<(&str, &str)> {
	let trimmed = input.trim_start();
	if trimmed.is_empty() {
		return None;
	}
	let end = trimmed
		.char_indices()
		.find_map(|(index, character)| character.is_whitespace().then_some(index))
		.unwrap_or(trimmed.len());
	Some((&trimmed[..end], &trimmed[end..]))
}

fn try_lower_create_logical_type(sql: &str) -> Result<Option<LogicalPlan>, FrontendError> {
	let trimmed = sql.trim();
	let statement = trimmed.strip_suffix(';').unwrap_or(trimmed).trim();
	let Some((create_keyword, rest)) = take_sql_word(statement) else {
		return Ok(None);
	};
	let Some((logical_keyword, rest)) = take_sql_word(rest) else {
		return Ok(None);
	};
	let Some((type_keyword, rest)) = take_sql_word(rest) else {
		return Ok(None);
	};
	if !create_keyword.eq_ignore_ascii_case("CREATE")
		|| !logical_keyword.eq_ignore_ascii_case("LOGICAL")
		|| !type_keyword.eq_ignore_ascii_case("TYPE")
	{
		return Ok(None);
	}
	let Some((name, rest)) = take_sql_word(rest) else {
		return Err(unsupported(
			"CREATE LOGICAL TYPE syntax is CREATE LOGICAL TYPE <Name> AS <primitive> [CHECK (<predicate>)]",
		));
	};
	let Some((as_keyword, rest)) = take_sql_word(rest) else {
		return Err(unsupported(
			"CREATE LOGICAL TYPE syntax is CREATE LOGICAL TYPE <Name> AS <primitive> [CHECK (<predicate>)]",
		));
	};
	let Some((physical_token, rest)) = take_sql_word(rest) else {
		return Err(unsupported(
			"CREATE LOGICAL TYPE syntax is CREATE LOGICAL TYPE <Name> AS <primitive> [CHECK (<predicate>)]",
		));
	};
	if !as_keyword.eq_ignore_ascii_case("AS") {
		return Err(unsupported(
			"CREATE LOGICAL TYPE syntax is CREATE LOGICAL TYPE <Name> AS <primitive> [CHECK (<predicate>)]",
		));
	}
	if name.contains('.') {
		return Err(unsupported(
			"CREATE LOGICAL TYPE supports one-part tenant-global names only",
		));
	}
	if name.eq_ignore_ascii_case("naked") {
		return Err(unsupported(
			"CREATE LOGICAL TYPE cannot define the naked sentinel",
		));
	}
	if name
		.bytes()
		.any(|byte| !(byte.is_ascii_alphanumeric() || byte == b'_'))
	{
		return Err(unsupported(
			"CREATE LOGICAL TYPE names must be unquoted ASCII identifiers",
		));
	}
	let physical = match physical_token.to_ascii_uppercase().as_str() {
		"BIGINT" | "INT64" => DataType::Int64,
		"INT" | "INTEGER" | "INT32" => DataType::Int32,
		"DOUBLE" | "DOUBLEPRECISION" | "FLOAT64" => DataType::Float64,
		"REAL" | "FLOAT32" => DataType::Float32,
		"TEXT" | "STRING" | "UTF8" => DataType::Utf8,
		"BOOLEAN" | "BOOL" => DataType::Boolean,
		other => {
			return Err(unsupported(format!(
				"unsupported CREATE LOGICAL TYPE physical type: {other}"
			)));
		}
	};
	let physical_carrier = data_type_to_logical_type_physical(&physical).ok_or_else(|| {
		unsupported("CREATE LOGICAL TYPE physical type must be a primitive logical-type carrier")
	})?;
	let (unit, refinement) = parse_create_logical_type_clauses(rest.trim(), physical_carrier)?;
	if let Some(refinement) = &refinement {
		refinement
			.validate_for_physical(physical_carrier)
			.map_err(|err| unsupported(format!("invalid CREATE LOGICAL TYPE CHECK: {err}")))?;
	}
	Ok(Some(LogicalPlan::CreateLogicalType {
		name: name.to_string(),
		physical,
		refinement,
		unit,
	}))
}

fn parse_create_logical_type_clauses(
	clause: &str,
	physical: LogicalTypePhysical,
) -> Result<
	(
		Option<LogicalTypeUnit>,
		Option<LogicalTypeRefinementPredicate>,
	),
	FrontendError,
> {
	let clause = clause.trim();
	if clause.is_empty() {
		return Ok((None, None));
	}
	let Some(after_dimension) = strip_sql_keyword_prefix(clause, "DIMENSION") else {
		let refinement = parse_create_logical_type_refinement_clause(clause, physical)?;
		return Ok((None, refinement));
	};
	let Some((dimension, rest)) = take_sql_word(after_dimension) else {
		return Err(unsupported(
			"CREATE LOGICAL TYPE DIMENSION requires an unquoted ASCII identifier",
		));
	};
	validate_logical_type_unit_dimension(dimension)?;
	let Some(after_scale) = strip_sql_keyword_prefix(rest, "SCALE_TO_BASE") else {
		return Err(unsupported(
			"CREATE LOGICAL TYPE DIMENSION requires SCALE_TO_BASE <decimal>",
		));
	};
	let Some((scale, rest)) = take_sql_word(after_scale) else {
		return Err(unsupported(
			"CREATE LOGICAL TYPE SCALE_TO_BASE requires a positive decimal",
		));
	};
	if !matches!(physical, LogicalTypePhysical::Float64) {
		return Err(unsupported(
			"CREATE LOGICAL TYPE unit metadata requires DOUBLE/FLOAT64 physical type",
		));
	}
	let scale_to_base = parse_positive_decimal_scale(scale)?;
	let mut rest = rest.trim();
	let mut offset_to_base = UnitOffset::ZERO;
	let mut offset_explicit = false;
	if let Some(after_offset) = strip_sql_keyword_prefix(rest, "OFFSET_TO_BASE") {
		let Some((offset, after_offset_value)) = take_sql_word(after_offset) else {
			return Err(unsupported(
				"CREATE LOGICAL TYPE OFFSET_TO_BASE requires a signed decimal",
			));
		};
		offset_to_base = parse_signed_decimal_offset(offset)?;
		offset_explicit = true;
		rest = after_offset_value.trim();
	}
	let mut quantity_kind = UnitQuantityKind::Delta;
	let mut delta_unit = None;
	if let Some((kind, after_kind)) = take_sql_word(rest) {
		if kind.eq_ignore_ascii_case("POINT") {
			quantity_kind = UnitQuantityKind::Point;
			rest = after_kind.trim();
		} else if kind.eq_ignore_ascii_case("DELTA") {
			quantity_kind = UnitQuantityKind::Delta;
			rest = after_kind.trim();
		}
	}
	if let Some(after_delta_unit) = strip_sql_keyword_prefix(rest, "DELTA_UNIT") {
		if quantity_kind != UnitQuantityKind::Point {
			return Err(unsupported("CREATE LOGICAL TYPE DELTA_UNIT requires POINT"));
		}
		let Some((delta_unit_name, after_delta_unit_name)) = take_sql_word(after_delta_unit) else {
			return Err(unsupported(
				"CREATE LOGICAL TYPE DELTA_UNIT requires an unquoted logical type identifier",
			));
		};
		delta_unit = Some(validate_sql_logical_type_name(
			"CREATE LOGICAL TYPE DELTA_UNIT",
			delta_unit_name,
		)?);
		rest = after_delta_unit_name.trim();
	}
	if offset_explicit && !offset_to_base.is_zero() && quantity_kind == UnitQuantityKind::Delta {
		return Err(unsupported(
			"CREATE LOGICAL TYPE nonzero OFFSET_TO_BASE requires POINT",
		));
	}
	let unit = LogicalTypeUnit {
		dimension: dimension.to_string(),
		scale_to_base,
		quantity_kind,
		offset_to_base,
		delta_unit,
	};
	let refinement = parse_create_logical_type_refinement_clause(rest.trim(), physical)?;
	Ok((Some(unit), refinement))
}

fn parse_create_logical_type_refinement_clause(
	clause: &str,
	physical: LogicalTypePhysical,
) -> Result<Option<LogicalTypeRefinementPredicate>, FrontendError> {
	if clause.is_empty() {
		return Ok(None);
	}
	let Some(after_check) = strip_sql_keyword_prefix(clause, "CHECK") else {
		return Err(unsupported(
			"CREATE LOGICAL TYPE supports only an optional CHECK (<predicate>) clause after the physical type",
		));
	};
	let check = after_check.trim();
	if !check.starts_with('(') || !check.ends_with(')') {
		return Err(unsupported(
			"CREATE LOGICAL TYPE CHECK must wrap the predicate in parentheses",
		));
	}
	let inner = check[1..check.len() - 1].trim();
	if inner.is_empty() {
		return Err(unsupported("CREATE LOGICAL TYPE CHECK cannot be empty"));
	}
	parse_logical_type_refinement_predicate(inner, physical).map(Some)
}

fn validate_logical_type_unit_dimension(value: &str) -> Result<(), FrontendError> {
	if value.is_empty()
		|| value.eq_ignore_ascii_case("naked")
		|| value.contains('.')
		|| value
			.bytes()
			.any(|byte| !(byte.is_ascii_alphanumeric() || byte == b'_'))
	{
		return Err(unsupported(
			"CREATE LOGICAL TYPE unit dimension must be a one-part unquoted ASCII identifier",
		));
	}
	Ok(())
}

fn parse_positive_decimal_scale(value: &str) -> Result<UnitScale, FrontendError> {
	if value.contains(['e', 'E']) || value.starts_with(['+', '-']) {
		return Err(unsupported(
			"CREATE LOGICAL TYPE SCALE_TO_BASE supports positive decimal literals only",
		));
	}
	let mut parts = value.split('.');
	let whole = parts.next().unwrap_or_default();
	let fraction = parts.next();
	if parts.next().is_some()
		|| whole.is_empty()
		|| !whole.bytes().all(|byte| byte.is_ascii_digit())
		|| fraction
			.map(|part| part.is_empty() || !part.bytes().all(|byte| byte.is_ascii_digit()))
			.unwrap_or(false)
	{
		return Err(unsupported(
			"CREATE LOGICAL TYPE SCALE_TO_BASE supports positive decimal literals only",
		));
	}
	let mut numerator = whole
		.parse::<u64>()
		.map_err(|_| unsupported("CREATE LOGICAL TYPE SCALE_TO_BASE decimal is out of range"))?;
	let mut denominator = 1_u64;
	if let Some(fraction) = fraction {
		let scale = 10_u64.checked_pow(fraction.len() as u32).ok_or_else(|| {
			unsupported("CREATE LOGICAL TYPE SCALE_TO_BASE decimal is out of range")
		})?;
		let fraction_value = fraction.parse::<u64>().map_err(|_| {
			unsupported("CREATE LOGICAL TYPE SCALE_TO_BASE decimal is out of range")
		})?;
		numerator = numerator
			.checked_mul(scale)
			.and_then(|value| value.checked_add(fraction_value))
			.ok_or_else(|| {
				unsupported("CREATE LOGICAL TYPE SCALE_TO_BASE decimal is out of range")
			})?;
		denominator = scale;
	}
	if numerator == 0 {
		return Err(unsupported(
			"CREATE LOGICAL TYPE SCALE_TO_BASE must be greater than zero",
		));
	}
	let divisor = gcd_u64(numerator, denominator);
	Ok(UnitScale {
		numerator: numerator / divisor,
		denominator: denominator / divisor,
	})
}

fn parse_signed_decimal_offset(value: &str) -> Result<UnitOffset, FrontendError> {
	if value.contains(['e', 'E']) {
		return Err(unsupported(
			"CREATE LOGICAL TYPE OFFSET_TO_BASE supports signed decimal literals only",
		));
	}
	let (negative, digits) = if let Some(rest) = value.strip_prefix('-') {
		(true, rest)
	} else if let Some(rest) = value.strip_prefix('+') {
		(false, rest)
	} else {
		(false, value)
	};
	let mut parts = digits.split('.');
	let whole = parts.next().unwrap_or_default();
	let fraction = parts.next();
	if parts.next().is_some()
		|| whole.is_empty()
		|| !whole.bytes().all(|byte| byte.is_ascii_digit())
		|| fraction
			.map(|part| part.is_empty() || !part.bytes().all(|byte| byte.is_ascii_digit()))
			.unwrap_or(false)
	{
		return Err(unsupported(
			"CREATE LOGICAL TYPE OFFSET_TO_BASE supports signed decimal literals only",
		));
	}
	let mut numerator = whole
		.parse::<u64>()
		.map_err(|_| unsupported("CREATE LOGICAL TYPE OFFSET_TO_BASE decimal is out of range"))?;
	let mut denominator = 1_u64;
	if let Some(fraction) = fraction {
		let scale = 10_u64.checked_pow(fraction.len() as u32).ok_or_else(|| {
			unsupported("CREATE LOGICAL TYPE OFFSET_TO_BASE decimal is out of range")
		})?;
		let fraction_value = fraction.parse::<u64>().map_err(|_| {
			unsupported("CREATE LOGICAL TYPE OFFSET_TO_BASE decimal is out of range")
		})?;
		numerator = numerator
			.checked_mul(scale)
			.and_then(|value| value.checked_add(fraction_value))
			.ok_or_else(|| {
				unsupported("CREATE LOGICAL TYPE OFFSET_TO_BASE decimal is out of range")
			})?;
		denominator = scale;
	}
	if numerator == 0 {
		return Ok(UnitOffset::ZERO);
	}
	let divisor = gcd_u64(numerator, denominator);
	let numerator = numerator / divisor;
	let denominator = denominator / divisor;
	let max_positive = i64::MAX as u64;
	let signed_numerator = if negative {
		if numerator == max_positive + 1 {
			i64::MIN
		} else if numerator <= max_positive {
			-(numerator as i64)
		} else {
			return Err(unsupported(
				"CREATE LOGICAL TYPE OFFSET_TO_BASE decimal is out of range",
			));
		}
	} else if numerator <= max_positive {
		numerator as i64
	} else {
		return Err(unsupported(
			"CREATE LOGICAL TYPE OFFSET_TO_BASE decimal is out of range",
		));
	};
	Ok(UnitOffset {
		numerator: signed_numerator,
		denominator,
	})
}

fn gcd_u64(mut left: u64, mut right: u64) -> u64 {
	while right != 0 {
		let next = left % right;
		left = right;
		right = next;
	}
	left
}

fn strip_sql_keyword_prefix<'a>(input: &'a str, keyword: &str) -> Option<&'a str> {
	let trimmed = input.trim_start();
	if !trimmed
		.get(..keyword.len())
		.is_some_and(|prefix| prefix.eq_ignore_ascii_case(keyword))
	{
		return None;
	}
	let rest = &trimmed[keyword.len()..];
	if rest
		.chars()
		.next()
		.is_some_and(|character| character.is_ascii_alphanumeric() || character == '_')
	{
		return None;
	}
	Some(rest)
}

fn parse_logical_type_refinement_predicate(
	input: &str,
	physical: LogicalTypePhysical,
) -> Result<LogicalTypeRefinementPredicate, FrontendError> {
	if contains_sql_keyword_outside_string(input, "OR") {
		return Err(unsupported(
			"CREATE LOGICAL TYPE CHECK does not support OR predicates",
		));
	}
	if contains_sql_keyword_outside_string(input, "NOT") {
		return Err(unsupported(
			"CREATE LOGICAL TYPE CHECK does not support NOT predicates",
		));
	}
	let terms = split_sql_keyword_outside_string(input, "AND")?;
	if terms.len() > 1 {
		let mut predicates = Vec::with_capacity(terms.len());
		for term in terms {
			predicates.push(parse_logical_type_refinement_comparison(term, physical)?);
		}
		return Ok(LogicalTypeRefinementPredicate::And(predicates));
	}
	parse_logical_type_refinement_comparison(input, physical)
}

fn parse_logical_type_refinement_comparison(
	input: &str,
	physical: LogicalTypePhysical,
) -> Result<LogicalTypeRefinementPredicate, FrontendError> {
	let atom = strip_balanced_outer_parentheses(input.trim());
	for (operator, op) in [
		(">=", LogicalTypeRefinementComparisonOp::Ge),
		("<=", LogicalTypeRefinementComparisonOp::Le),
		("<>", LogicalTypeRefinementComparisonOp::Ne),
		("!=", LogicalTypeRefinementComparisonOp::Ne),
		("=", LogicalTypeRefinementComparisonOp::Eq),
		(">", LogicalTypeRefinementComparisonOp::Gt),
		("<", LogicalTypeRefinementComparisonOp::Lt),
	] {
		if let Some(index) = find_token_outside_string(atom, operator) {
			let left = atom[..index].trim();
			let right = atom[index + operator.len()..].trim();
			if left.is_empty() || right.is_empty() {
				break;
			}
			return Ok(LogicalTypeRefinementPredicate::Comparison {
				left: parse_logical_type_refinement_value(left, physical)?,
				op,
				right: parse_logical_type_refinement_value(right, physical)?,
			});
		}
	}
	Err(unsupported(
		"CREATE LOGICAL TYPE CHECK supports only VALUE comparisons joined by AND",
	))
}

fn parse_logical_type_refinement_value(
	input: &str,
	physical: LogicalTypePhysical,
) -> Result<LogicalTypeRefinementValue, FrontendError> {
	let value = strip_balanced_outer_parentheses(input.trim());
	if value.eq_ignore_ascii_case("VALUE") {
		return Ok(LogicalTypeRefinementValue::Value);
	}
	match physical {
		LogicalTypePhysical::Int64 => value
			.parse::<i64>()
			.map(LogicalTypeRefinementValue::Int64)
			.map_err(|_| unsupported("CREATE LOGICAL TYPE CHECK requires an INT64 literal")),
		LogicalTypePhysical::Int32 => value
			.parse::<i32>()
			.map(LogicalTypeRefinementValue::Int32)
			.map_err(|_| unsupported("CREATE LOGICAL TYPE CHECK requires an INT32 literal")),
		LogicalTypePhysical::Float64 => {
			let parsed = value.parse::<f64>().map_err(|_| {
				unsupported("CREATE LOGICAL TYPE CHECK requires a finite FLOAT64 literal")
			})?;
			if !parsed.is_finite() {
				return Err(unsupported(
					"CREATE LOGICAL TYPE CHECK requires a finite FLOAT64 literal",
				));
			}
			Ok(LogicalTypeRefinementValue::Float64(parsed.to_bits()))
		}
		LogicalTypePhysical::Float32 => {
			let parsed = value.parse::<f32>().map_err(|_| {
				unsupported("CREATE LOGICAL TYPE CHECK requires a finite FLOAT32 literal")
			})?;
			if !parsed.is_finite() {
				return Err(unsupported(
					"CREATE LOGICAL TYPE CHECK requires a finite FLOAT32 literal",
				));
			}
			Ok(LogicalTypeRefinementValue::Float32(parsed.to_bits()))
		}
		LogicalTypePhysical::Utf8 => parse_sql_string_literal(value)
			.map(LogicalTypeRefinementValue::Utf8)
			.ok_or_else(|| {
				unsupported("CREATE LOGICAL TYPE CHECK requires a single-quoted UTF8 literal")
			}),
		LogicalTypePhysical::Boolean => match value.to_ascii_uppercase().as_str() {
			"TRUE" => Ok(LogicalTypeRefinementValue::Boolean(true)),
			"FALSE" => Ok(LogicalTypeRefinementValue::Boolean(false)),
			_ => Err(unsupported(
				"CREATE LOGICAL TYPE CHECK requires a BOOLEAN literal",
			)),
		},
	}
}

fn parse_sql_string_literal(input: &str) -> Option<String> {
	if !input.starts_with('\'') || !input.ends_with('\'') || input.len() < 2 {
		return None;
	}
	let mut output = String::new();
	let mut chars = input[1..input.len() - 1].chars().peekable();
	while let Some(character) = chars.next() {
		if character == '\'' {
			if chars.peek() == Some(&'\'') {
				chars.next();
				output.push('\'');
			} else {
				return None;
			}
		} else {
			output.push(character);
		}
	}
	Some(output)
}

fn strip_balanced_outer_parentheses(mut input: &str) -> &str {
	loop {
		let trimmed = input.trim();
		if trimmed.len() < 2 || !trimmed.starts_with('(') || !trimmed.ends_with(')') {
			return trimmed;
		}
		if !outer_parentheses_wrap(trimmed) {
			return trimmed;
		}
		input = &trimmed[1..trimmed.len() - 1];
	}
}

fn outer_parentheses_wrap(input: &str) -> bool {
	let mut depth = 0i32;
	let mut in_string = false;
	let mut chars = input.char_indices().peekable();
	while let Some((index, character)) = chars.next() {
		if in_string {
			if character == '\'' {
				if chars.peek().is_some_and(|(_, next)| *next == '\'') {
					chars.next();
				} else {
					in_string = false;
				}
			}
			continue;
		}
		match character {
			'\'' => in_string = true,
			'(' => depth += 1,
			')' => {
				depth -= 1;
				if depth == 0 && index != input.len() - 1 {
					return false;
				}
				if depth < 0 {
					return false;
				}
			}
			_ => {}
		}
	}
	depth == 0 && !in_string
}

fn contains_sql_keyword_outside_string(input: &str, keyword: &str) -> bool {
	find_sql_keyword_outside_string(input, keyword, 0).is_some()
}

fn split_sql_keyword_outside_string<'a>(
	input: &'a str,
	keyword: &str,
) -> Result<Vec<&'a str>, FrontendError> {
	let mut parts = Vec::new();
	let mut start = 0usize;
	let mut search_from = 0usize;
	while let Some(index) = find_sql_keyword_outside_string(input, keyword, search_from) {
		let part = input[start..index].trim();
		if part.is_empty() {
			return Err(unsupported(
				"CREATE LOGICAL TYPE CHECK has an empty predicate around AND",
			));
		}
		parts.push(part);
		start = index + keyword.len();
		search_from = start;
	}
	let tail = input[start..].trim();
	if tail.is_empty() {
		return Err(unsupported(
			"CREATE LOGICAL TYPE CHECK has an empty predicate around AND",
		));
	}
	parts.push(tail);
	Ok(parts)
}

fn find_sql_keyword_outside_string(
	input: &str,
	keyword: &str,
	search_from: usize,
) -> Option<usize> {
	let mut depth = 0i32;
	let mut in_string = false;
	let mut chars = input.char_indices().peekable();
	while let Some((index, character)) = chars.next() {
		if index < search_from {
			continue;
		}
		if in_string {
			if character == '\'' {
				if chars.peek().is_some_and(|(_, next)| *next == '\'') {
					chars.next();
				} else {
					in_string = false;
				}
			}
			continue;
		}
		match character {
			'\'' => in_string = true,
			'(' => depth += 1,
			')' => depth -= 1,
			_ if depth == 0 && sql_keyword_at(input, index, keyword) => return Some(index),
			_ => {}
		}
	}
	None
}

fn find_token_outside_string(input: &str, token: &str) -> Option<usize> {
	let mut in_string = false;
	let mut chars = input.char_indices().peekable();
	while let Some((index, character)) = chars.next() {
		if in_string {
			if character == '\'' {
				if chars.peek().is_some_and(|(_, next)| *next == '\'') {
					chars.next();
				} else {
					in_string = false;
				}
			}
			continue;
		}
		if character == '\'' {
			in_string = true;
			continue;
		}
		if input[index..].starts_with(token) {
			return Some(index);
		}
	}
	None
}

fn sql_keyword_at(input: &str, index: usize, keyword: &str) -> bool {
	if !input
		.get(index..index + keyword.len())
		.is_some_and(|candidate| candidate.eq_ignore_ascii_case(keyword))
	{
		return false;
	}
	let before = input[..index].chars().next_back();
	let after = input[index + keyword.len()..].chars().next();
	!before.is_some_and(is_sql_identifier_character)
		&& !after.is_some_and(is_sql_identifier_character)
}

fn is_sql_identifier_character(character: char) -> bool {
	character.is_ascii_alphanumeric() || character == '_'
}

fn lower_ddl_table_name(name: &ObjectName) -> Result<TableRef, FrontendError> {
	match name.0.as_slice() {
		[table] => Ok(TableRef::new(object_name_part_value(
			table,
			"DDL table name",
		)?)),
		[schema, table] => Ok(TableRef::with_schema(
			object_name_part_value(schema, "DDL table name")?,
			object_name_part_value(table, "DDL table name")?,
		)),
		_ => Err(unsupported(
			"DDL table names support only one- or two-part table names",
		)),
	}
}

fn lower_table_ref_object_name(
	name: &ObjectName,
	context: &'static str,
) -> Result<TableRef, FrontendError> {
	match name.0.as_slice() {
		[table] => Ok(TableRef::new(object_name_part_value(table, context)?)),
		[schema, table] => Ok(TableRef::with_schema(
			object_name_part_value(schema, context)?,
			object_name_part_value(table, context)?,
		)),
		_ => Err(unsupported(format!(
			"{context} supports only one- or two-part table names"
		))),
	}
}

fn lower_insert_target_name(target: &TableObject) -> Result<TableRef, FrontendError> {
	match target {
		TableObject::TableName(name) => match name.0.as_slice() {
			[table] => Ok(TableRef::new(object_name_part_value(table, "INSERT")?)),
			[schema, table] => Ok(TableRef::with_schema(
				object_name_part_value(schema, "INSERT")?,
				object_name_part_value(table, "INSERT")?,
			)),
			_ => Err(unsupported(
				"INSERT supports only one- or two-part table names",
			)),
		},
		TableObject::TableFunction(_) => Err(unsupported(
			"INSERT target must be a table name; table functions are not supported",
		)),
		TableObject::TableQuery(_) => Err(unsupported(
			"INSERT target must be a table name; table queries are not supported",
		)),
	}
}

fn lower_insert_column_list(columns: &[ObjectName]) -> Result<Option<Vec<String>>, FrontendError> {
	if columns.is_empty() {
		Ok(None)
	} else {
		columns
			.iter()
			.map(|name| match name.0.as_slice() {
				[column] => object_name_part_value(column, "INSERT column list"),
				_ => Err(unsupported(
					"unsupported INSERT column list: qualified column",
				)),
			})
			.collect::<Result<Vec<_>, _>>()
			.map(Some)
	}
}

fn lower_update_target_table(
	table: &sqlparser::ast::TableWithJoins,
) -> Result<TableRef, FrontendError> {
	let TableFactor::Table {
		name,
		alias,
		args,
		with_hints,
		index_hints,
		version,
		with_ordinality,
		partitions,
		json_path,
		sample,
	} = &table.relation
	else {
		return Err(unsupported("UPDATE target must be a single table"));
	};

	if alias.is_some() {
		return Err(unsupported("unsupported UPDATE target: table alias"));
	}
	if args.is_some() {
		return Err(unsupported("unsupported UPDATE target: table arguments"));
	}
	if !with_hints.is_empty() {
		return Err(unsupported("unsupported UPDATE target: table hints"));
	}
	if !index_hints.is_empty() {
		return Err(unsupported("unsupported UPDATE target: index hints"));
	}
	if version.is_some() {
		return Err(unsupported("unsupported UPDATE target: table version"));
	}
	if *with_ordinality {
		return Err(unsupported("unsupported UPDATE target: WITH ORDINALITY"));
	}
	if !partitions.is_empty() {
		return Err(unsupported("unsupported UPDATE target: partitions"));
	}
	if json_path.is_some() {
		return Err(unsupported("unsupported UPDATE target: json path"));
	}
	if sample.is_some() {
		return Err(unsupported("unsupported UPDATE target: sample"));
	}

	lower_table_ref_object_name(name, "UPDATE")
}

fn lower_update_assignments(
	planner: &SqlPlanner,
	assignments: &[Assignment],
) -> Result<Vec<(String, Expr)>, FrontendError> {
	if assignments.is_empty() {
		return Err(unsupported("UPDATE requires at least one assignment"));
	}

	let mut seen = HashSet::new();
	let mut lowered = Vec::with_capacity(assignments.len());
	for assignment in assignments {
		let column = lower_update_assignment_target(&assignment.target)?;
		if !seen.insert(column.clone()) {
			return Err(unsupported(format!(
				"duplicate UPDATE assignment target: {column}"
			)));
		}
		let value = lower_update_assignment_expr(planner, &assignment.value)?;
		lowered.push((column, value));
	}
	Ok(lowered)
}

fn lower_update_assignment_target(target: &AssignmentTarget) -> Result<String, FrontendError> {
	match target {
		AssignmentTarget::ColumnName(name) => match name.0.as_slice() {
			[column] => object_name_part_value(column, "UPDATE assignment target"),
			_ => Err(unsupported(
				"unsupported UPDATE assignment target: qualified column",
			)),
		},
		AssignmentTarget::Tuple(_) => Err(unsupported(
			"unsupported UPDATE assignment target: tuple assignment",
		)),
	}
}

fn lower_update_assignment_expr(
	planner: &SqlPlanner,
	expr: &SqlExpr,
) -> Result<Expr, FrontendError> {
	if expr_contains_aggregate_call(expr) {
		return Err(unsupported(
			"unsupported UPDATE assignment expression: aggregate functions",
		));
	}
	if expr_contains_subquery(expr) {
		return Err(unsupported(
			"unsupported UPDATE assignment expression: subqueries",
		));
	}
	if expr_contains_window_function(expr) {
		return Err(unsupported(
			"unsupported UPDATE assignment expression: window functions",
		));
	}
	planner.lower_expr(expr)
}

enum LoweredInsertSource {
	Values(Vec<Vec<InsertLiteral>>),
	Select(LogicalPlan),
}

#[derive(Debug, Clone, Copy)]
struct LoweredLimitOffset<'a> {
	limit: Option<&'a SqlExpr>,
	offset: Option<&'a SqlExpr>,
}

fn lower_limit_clause(
	limit_clause: Option<&LimitClause>,
) -> Result<LoweredLimitOffset<'_>, FrontendError> {
	let Some(limit_clause) = limit_clause else {
		return Ok(LoweredLimitOffset {
			limit: None,
			offset: None,
		});
	};
	match limit_clause {
		LimitClause::LimitOffset {
			limit,
			offset,
			limit_by,
		} => {
			if !limit_by.is_empty() {
				return Err(unsupported("LIMIT BY is not supported"));
			}
			Ok(LoweredLimitOffset {
				limit: limit.as_ref(),
				offset: offset.as_ref().map(|offset| &offset.value),
			})
		}
		LimitClause::OffsetCommaLimit { .. } => {
			Err(unsupported("LIMIT offset, count syntax is not supported"))
		}
	}
}

fn query_has_limit_or_offset(limit_clause: Option<&LimitClause>) -> bool {
	match limit_clause {
		Some(LimitClause::LimitOffset {
			limit,
			offset,
			limit_by,
		}) => limit.is_some() || offset.is_some() || !limit_by.is_empty(),
		Some(LimitClause::OffsetCommaLimit { .. }) => true,
		None => false,
	}
}

fn lower_insert_source(
	planner: &SqlPlanner,
	source: Option<&Query>,
) -> Result<LoweredInsertSource, FrontendError> {
	let query = source.ok_or_else(|| unsupported("INSERT requires a VALUES clause"))?;

	let values = match query.body.as_ref() {
		SetExpr::Values(values) => {
			if query.with.is_some()
				|| query.order_by.is_some()
				|| query.limit_clause.is_some()
				|| query.fetch.is_some()
				|| !query.locks.is_empty()
				|| query.for_clause.is_some()
				|| query.settings.is_some()
				|| query.format_clause.is_some()
				|| !query.pipe_operators.is_empty()
			{
				return Err(unsupported(
					"unsupported INSERT source: must be VALUES only",
				));
			}
			values
		}
		_ => return planner.lower_query(query).map(LoweredInsertSource::Select),
	};
	if values.explicit_row {
		return Err(unsupported(
			"unsupported INSERT clause: explicit ROW keyword",
		));
	}
	if values.rows.is_empty() {
		return Err(unsupported("INSERT VALUES must contain at least one row"));
	}
	if values.rows.iter().any(|row| row.is_empty()) {
		return Err(unsupported(
			"INSERT VALUES row must contain at least one literal",
		));
	}

	values
		.rows
		.iter()
		.map(|row| {
			row.iter()
				.map(lower_insert_literal)
				.collect::<Result<Vec<_>, _>>()
		})
		.collect::<Result<Vec<_>, _>>()
		.map(LoweredInsertSource::Values)
}

fn lower_insert_literal(expr: &SqlExpr) -> Result<InsertLiteral, FrontendError> {
	match expr {
		SqlExpr::TypedString(typed) if is_date_sql_type(&typed.data_type) => {
			let value = typed_string_value(typed)?;
			parse_date_ymd(&value)
				.map(InsertLiteral::Date32)
				.map_err(|error| unsupported(error.to_string()))
		}
		SqlExpr::TypedString(typed) => {
			let Some(unit) = timestamptz_sql_unit(&typed.data_type)? else {
				return Err(unsupported(
					"INSERT VALUES must be literals; complex expressions not supported",
				));
			};
			let value = typed_string_value(typed)?;
			parse_explicit_timestamptz(&value, unit)
				.map(|value| InsertLiteral::Timestamp { value, unit })
				.map_err(|error| unsupported(error.to_string()))
		}
		SqlExpr::Value(value) => match &value.value {
			Value::Null => Ok(InsertLiteral::Null),
			Value::Boolean(value) => Ok(InsertLiteral::Boolean(*value)),
			Value::Number(text, _) => parse_insert_number(text, false),
			Value::SingleQuotedString(value) => Ok(InsertLiteral::Utf8(value.clone())),
			_ => Err(unsupported(
				"INSERT VALUES must be literals; complex expressions not supported",
			)),
		},
		SqlExpr::UnaryOp {
			op: UnaryOperator::Minus,
			expr,
		} => match expr.as_ref() {
			SqlExpr::Value(value) => match &value.value {
				Value::Number(text, _) => parse_insert_number(text, true),
				_ => Err(unsupported(
					"INSERT VALUES must be literals; complex expressions not supported",
				)),
			},
			_ => Err(unsupported(
				"INSERT VALUES must be literals; complex expressions not supported",
			)),
		},
		_ => Err(unsupported(
			"INSERT VALUES must be literals; complex expressions not supported",
		)),
	}
}

fn parse_insert_number(text: &str, negate: bool) -> Result<InsertLiteral, FrontendError> {
	if negate {
		if let Ok(value) = text.parse::<i64>() {
			return value
				.checked_neg()
				.map(InsertLiteral::Int64)
				.ok_or_else(|| {
					unsupported(format!("INSERT VALUES literal not representable: -{text}"))
				});
		}
		if let Ok(value) = text.parse::<u64>() {
			if value == (i64::MAX as u64) + 1 {
				return Ok(InsertLiteral::Int64(i64::MIN));
			}
			if value <= i64::MAX as u64 {
				return Ok(InsertLiteral::Int64(-(value as i64)));
			}
		}
	} else {
		if let Ok(value) = text.parse::<i64>() {
			return Ok(InsertLiteral::Int64(value));
		}
		if let Ok(value) = text.parse::<u64>() {
			return Ok(InsertLiteral::UInt64(value));
		}
	}

	text.parse::<f64>()
		.map(|value| InsertLiteral::Float64(if negate { -value } else { value }))
		.map_err(|_| unsupported(format!("INSERT VALUES literal not representable: {text}")))
}

struct LoweredCreateTableColumn {
	column: CreateTableColumn,
	primary_key: bool,
}

fn lower_create_table_column(
	column: &ColumnDef,
) -> Result<LoweredCreateTableColumn, FrontendError> {
	let column_name = &column.name.value;

	if matches!(&column.data_type, SqlDataType::Unspecified) {
		return Err(unsupported("CREATE TABLE column type is required"));
	}
	if STORAGE_SYNTHETIC_COLUMN_NAMES.contains(&column_name.as_str()) {
		return Err(unsupported(format!(
			"CREATE TABLE column name is reserved for storage metadata: {column_name}"
		)));
	}

	let column_type = lower_create_table_data_type(&column.data_type)?;
	let (nullable, primary_key) = lower_create_table_column_options(&column.options)?;

	Ok(LoweredCreateTableColumn {
		column: CreateTableColumn {
			name: column_name.clone(),
			nullable,
			column_type,
		},
		primary_key,
	})
}

fn lower_create_table_column_options(
	options: &[ColumnOptionDef],
) -> Result<(bool, bool), FrontendError> {
	let mut nullable = None;
	let mut primary_key = false;
	for option_def in options {
		if option_def.name.is_some() {
			return Err(unsupported(
				"unsupported CREATE TABLE column option: named constraint",
			));
		}
		let new_nullable = match &option_def.option {
			ColumnOption::Null => true,
			ColumnOption::NotNull => false,
			ColumnOption::Default(_) => {
				return Err(unsupported(
					"unsupported CREATE TABLE column option: DEFAULT",
				));
			}
			ColumnOption::PrimaryKey(primary_key_constraint) => {
				let sqlparser::ast::PrimaryKeyConstraint {
					name,
					index_name,
					index_type,
					columns,
					index_options,
					characteristics,
				} = primary_key_constraint;
				if name.is_some()
					|| index_name.is_some()
					|| index_type.is_some()
					|| !columns.is_empty()
					|| !index_options.is_empty()
					|| characteristics.is_some()
				{
					return Err(unsupported(
						"unsupported CREATE TABLE column option: PRIMARY KEY characteristics",
					));
				}
				if primary_key {
					return Err(unsupported(
						"CREATE TABLE specifies PRIMARY KEY more than once",
					));
				}
				primary_key = true;
				continue;
			}
			ColumnOption::Unique(_) => {
				return Err(unsupported(
					"unsupported CREATE TABLE column option: UNIQUE",
				));
			}
			ColumnOption::ForeignKey(_) => {
				return Err(unsupported(
					"unsupported CREATE TABLE column option: REFERENCES",
				));
			}
			ColumnOption::Check(_) => {
				return Err(unsupported("unsupported CREATE TABLE column option: CHECK"));
			}
			ColumnOption::Generated {
				generated_as: _,
				sequence_options: _,
				generation_expr: _,
				generation_expr_mode: _,
				generated_keyword: _,
			} => {
				return Err(unsupported(
					"unsupported CREATE TABLE column option: GENERATED",
				));
			}
			ColumnOption::Materialized(_)
			| ColumnOption::Ephemeral(_)
			| ColumnOption::Alias(_)
			| ColumnOption::DialectSpecific(_)
			| ColumnOption::CharacterSet(_)
			| ColumnOption::Collation(_)
			| ColumnOption::Comment(_)
			| ColumnOption::OnUpdate(_)
			| ColumnOption::Options(_)
			| ColumnOption::Identity(_)
			| ColumnOption::OnConflict(_)
			| ColumnOption::Policy(_)
			| ColumnOption::Tags(_)
			| ColumnOption::Srid(_)
			| ColumnOption::Invisible => {
				return Err(unsupported("unsupported CREATE TABLE column option"));
			}
		};
		if nullable.is_some() {
			return Err(unsupported(
				"CREATE TABLE column nullability is specified more than once",
			));
		}
		nullable = Some(new_nullable);
	}
	Ok((nullable.unwrap_or(true), primary_key))
}

fn lower_create_table_data_type(
	data_type: &SqlDataType,
) -> Result<CreateTableColumnType, FrontendError> {
	match data_type {
		SqlDataType::Int(_)
		| SqlDataType::Integer(_)
		| SqlDataType::Int4(_)
		| SqlDataType::Int32 => Ok(CreateTableColumnType::Physical(DataType::Int32)),
		SqlDataType::BigInt(_) | SqlDataType::Int8(_) | SqlDataType::Int64 => {
			Ok(CreateTableColumnType::Physical(DataType::Int64))
		}
		SqlDataType::Real | SqlDataType::Float4 | SqlDataType::Float32 => {
			Ok(CreateTableColumnType::Physical(DataType::Float32))
		}
		SqlDataType::Double(_)
		| SqlDataType::DoublePrecision
		| SqlDataType::Float8
		| SqlDataType::Float64 => Ok(CreateTableColumnType::Physical(DataType::Float64)),
		SqlDataType::Date | SqlDataType::Date32 => {
			Ok(CreateTableColumnType::Physical(DataType::Date32))
		}
		SqlDataType::Timestamp(precision, TimezoneInfo::WithTimeZone | TimezoneInfo::Tz) => {
			Ok(CreateTableColumnType::Physical(utc_timestamp_data_type(
				timestamp_unit_for_precision(*precision)?,
			)))
		}
		SqlDataType::Timestamp(_, TimezoneInfo::None | TimezoneInfo::WithoutTimeZone)
		| SqlDataType::TimestampNtz(_) => Err(unsupported(
			"TIMESTAMP WITHOUT TIME ZONE is not supported; use TIMESTAMPTZ",
		)),
		SqlDataType::Text
		| SqlDataType::String(_)
		| SqlDataType::Varchar(_)
		| SqlDataType::Char(_)
		| SqlDataType::Character(_)
		| SqlDataType::CharVarying(_)
		| SqlDataType::CharacterVarying(_) => Ok(CreateTableColumnType::Physical(DataType::Utf8)),
		SqlDataType::Boolean | SqlDataType::Bool => {
			Ok(CreateTableColumnType::Physical(DataType::Boolean))
		}
		SqlDataType::Custom(name, modifiers) => Ok(CreateTableColumnType::LogicalName(
			lower_create_table_logical_type_name(name, modifiers)?,
		)),
		SqlDataType::Decimal(_) | SqlDataType::Numeric(_) => {
			Err(unsupported("unsupported CREATE TABLE column type: DECIMAL"))
		}
		SqlDataType::Float(_) => Err(unsupported(
			"unsupported CREATE TABLE column type: FLOAT (precision ambiguous; use REAL or DOUBLE)",
		)),
		other => Err(unsupported(format!(
			"unsupported CREATE TABLE column type: {other}"
		))),
	}
}

fn lower_create_table_logical_type_name(
	name: &ObjectName,
	modifiers: &[String],
) -> Result<String, FrontendError> {
	lower_sql_logical_type_name("CREATE TABLE", name, modifiers)
}

fn lower_cast_logical_type_name(
	name: &ObjectName,
	modifiers: &[String],
) -> Result<String, FrontendError> {
	lower_sql_logical_type_name("CAST", name, modifiers)
}

fn lower_convert_target_logical_name(expr: &SqlExpr) -> Result<String, FrontendError> {
	match expr {
		SqlExpr::Identifier(ident) => validate_sql_logical_type_ident("CONVERT", ident),
		SqlExpr::Value(value) => match &value.value {
			Value::SingleQuotedString(name) => validate_sql_logical_type_name("CONVERT", name),
			_ => Err(unsupported(
				"CONVERT target must be an unquoted logical type identifier",
			)),
		},
		_ => Err(unsupported(
			"CONVERT target must be an unquoted logical type identifier",
		)),
	}
}

fn lower_convert_data_type_target_logical_name(
	data_type: &SqlDataType,
) -> Result<String, FrontendError> {
	match lower_cast_target(data_type)? {
		LoweredCastTarget::LogicalName(logical_name) => Ok(logical_name),
		LoweredCastTarget::Physical(_) => Err(unsupported(
			"CONVERT target must be a dimensioned logical type identifier",
		)),
	}
}

fn lower_sql_logical_type_name(
	context: &'static str,
	name: &ObjectName,
	modifiers: &[String],
) -> Result<String, FrontendError> {
	if !modifiers.is_empty() {
		return Err(unsupported(format!(
			"{context} logical type names do not support modifiers"
		)));
	}
	let [part] = name.0.as_slice() else {
		return Err(unsupported(format!(
			"{context} logical type names support one-part names only"
		)));
	};
	let ident = object_name_part_ident(part, &format!("{context} logical type name"))?;
	validate_sql_logical_type_ident(context, ident)
}

fn validate_sql_logical_type_ident(
	context: &'static str,
	ident: &Ident,
) -> Result<String, FrontendError> {
	if ident.quote_style.is_some() {
		return Err(unsupported(format!(
			"{context} logical type names must be unquoted ASCII identifiers"
		)));
	}
	validate_sql_logical_type_name(context, &ident.value)
}

fn validate_sql_logical_type_name(
	context: &'static str,
	name: &str,
) -> Result<String, FrontendError> {
	if name.eq_ignore_ascii_case("naked") {
		return Err(unsupported(format!(
			"{context} logical type name cannot be the naked sentinel"
		)));
	}
	if name
		.bytes()
		.any(|byte| !(byte.is_ascii_alphanumeric() || byte == b'_'))
	{
		return Err(unsupported(format!(
			"{context} logical type names must be unquoted ASCII identifiers"
		)));
	}
	Ok(name.to_string())
}

fn check_unique_column_names(columns: &[CreateTableColumn]) -> Result<(), FrontendError> {
	let mut seen = HashSet::new();
	for column in columns {
		if !seen.insert(column.name.as_str()) {
			return Err(unsupported("CREATE TABLE column names must be unique"));
		}
	}
	Ok(())
}

fn lower_create_table_schema_and_identity(
	columns: &[ColumnDef],
	constraints: &[TableConstraint],
	primary_key: Option<&SqlExpr>,
) -> Result<(CreateTableSchema, CreateTableRowIdentity), FrontendError> {
	let mut lowered_columns = Vec::with_capacity(columns.len());
	let mut primary_key_declarations = Vec::new();

	for column in columns {
		let lowered = lower_create_table_column(column)?;
		if lowered.primary_key {
			primary_key_declarations.push(column.name.value.clone());
		}
		lowered_columns.push(lowered.column);
	}

	for constraint in constraints {
		match constraint {
			TableConstraint::PrimaryKey(primary_key_constraint) => {
				let sqlparser::ast::PrimaryKeyConstraint {
					name,
					index_name,
					index_type,
					columns,
					index_options,
					characteristics,
				} = primary_key_constraint;
				if name.is_some() {
					return Err(unsupported(
						"unsupported CREATE TABLE clause: named PRIMARY KEY constraint",
					));
				}
				if index_name.is_some()
					|| index_type.is_some()
					|| !index_options.is_empty()
					|| characteristics.is_some()
				{
					return Err(unsupported(
						"unsupported CREATE TABLE clause: PRIMARY KEY options",
					));
				}
				let [column] = columns.as_slice() else {
					return Err(unsupported(
						"CREATE TABLE PRIMARY KEY must reference exactly one column",
					));
				};
				primary_key_declarations.push(lower_primary_key_index_column(column)?);
			}
			TableConstraint::Unique(_) => {
				return Err(unsupported(
					"unsupported CREATE TABLE clause: table-level UNIQUE",
				));
			}
			_ => {
				return Err(unsupported(
					"unsupported CREATE TABLE clause: table-level constraints",
				));
			}
		}
	}

	if let Some(primary_key) = primary_key {
		primary_key_declarations.push(lower_create_table_primary_key_expr(primary_key)?);
	}

	check_unique_column_names(&lowered_columns)?;
	let schema = CreateTableSchema {
		columns: lowered_columns,
	};

	let row_identity = match primary_key_declarations.as_slice() {
		[] => CreateTableRowIdentity::AppendOnly,
		[column] => {
			let (_, primary_key_column) = schema
				.columns
				.iter()
				.enumerate()
				.find(|(_, candidate)| candidate.name == *column)
				.ok_or_else(|| {
					unsupported(format!(
						"CREATE TABLE PRIMARY KEY column does not exist: {column}"
					))
				})?;
			if primary_key_column.nullable {
				return Err(unsupported("PRIMARY KEY column must be declared NOT NULL"));
			}
			if let CreateTableColumnType::Physical(data_type) = &primary_key_column.column_type
				&& !primary_key_type_is_supported(data_type)
			{
				return Err(unsupported(
					"unsupported CREATE TABLE PRIMARY KEY column type",
				));
			}
			CreateTableRowIdentity::PrimaryKey {
				column: column.clone(),
			}
		}
		_ => {
			return Err(unsupported(
				"CREATE TABLE specifies PRIMARY KEY more than once",
			));
		}
	};

	Ok((schema, row_identity))
}

fn lower_primary_key_index_column(
	column: &sqlparser::ast::IndexColumn,
) -> Result<String, FrontendError> {
	if column.operator_class.is_some() {
		return Err(unsupported(
			"unsupported CREATE TABLE clause: PRIMARY KEY operator class",
		));
	}
	if column.column.with_fill.is_some()
		|| column.column.options.asc.is_some()
		|| column.column.options.nulls_first.is_some()
	{
		return Err(unsupported(
			"unsupported CREATE TABLE clause: PRIMARY KEY options",
		));
	}
	match &column.column.expr {
		SqlExpr::Identifier(ident) => Ok(ident.value.clone()),
		SqlExpr::CompoundIdentifier(_) => Err(unsupported(
			"CREATE TABLE PRIMARY KEY column must be unqualified",
		)),
		_ => Err(unsupported(
			"unsupported CREATE TABLE clause: PRIMARY KEY expression",
		)),
	}
}

fn reject_create_table_options(options: &CreateTableOptions) -> Result<(), FrontendError> {
	match options {
		CreateTableOptions::None => Ok(()),
		CreateTableOptions::With(_) => {
			Err(unsupported("unsupported CREATE TABLE clause: WITH options"))
		}
		CreateTableOptions::Options(_) => {
			Err(unsupported("unsupported CREATE TABLE clause: OPTIONS"))
		}
		CreateTableOptions::Plain(_) => {
			Err(unsupported("unsupported CREATE TABLE clause: options"))
		}
		CreateTableOptions::TableProperties(_) => Err(unsupported(
			"unsupported CREATE TABLE clause: TBLPROPERTIES",
		)),
	}
}

fn lower_create_table_primary_key_expr(expr: &SqlExpr) -> Result<String, FrontendError> {
	match expr {
		SqlExpr::Identifier(ident) => Ok(ident.value.clone()),
		SqlExpr::Nested(inner) => lower_create_table_primary_key_expr(inner),
		SqlExpr::Tuple(exprs) => {
			let [expr] = exprs.as_slice() else {
				return Err(unsupported(
					"CREATE TABLE PRIMARY KEY must reference exactly one column",
				));
			};
			lower_create_table_primary_key_expr(expr)
		}
		SqlExpr::CompoundIdentifier(_) => Err(unsupported(
			"CREATE TABLE PRIMARY KEY column must be unqualified",
		)),
		_ => Err(unsupported(
			"unsupported CREATE TABLE clause: PRIMARY KEY expression",
		)),
	}
}

fn primary_key_type_is_supported(data_type: &DataType) -> bool {
	matches!(
		data_type,
		DataType::Int32 | DataType::Int64 | DataType::Utf8
	)
}

fn relation_plan_is_terminal(plan: &LogicalPlan) -> bool {
	matches!(
		plan,
		LogicalPlan::CreateTable { .. }
			| LogicalPlan::CreateLogicalType { .. }
			| LogicalPlan::CreateTableAsSelect { .. }
			| LogicalPlan::DropTable { .. }
			| LogicalPlan::Delete { .. }
			| LogicalPlan::Update { .. }
			| LogicalPlan::Insert { .. }
			| LogicalPlan::InsertSelect { .. }
	)
}

pub struct SqlPlanner;

impl Default for SqlPlanner {
	fn default() -> Self {
		Self::new()
	}
}

impl SqlPlanner {
	pub fn new() -> Self {
		Self
	}

	pub fn plan(&self, sql: &str) -> Result<LogicalPlan, FrontendError> {
		if let Some(plan) = try_lower_create_logical_type(sql)? {
			return Ok(plan);
		}
		let dialect = GenericDialect {};
		let ast = Parser::parse_sql(&dialect, sql).map_err(|e| FrontendError::SqlParse {
			message: e.to_string(),
		})?;

		if ast.is_empty() {
			return Err(unsupported("empty SQL statement"));
		}

		self.lower_statement(&ast[0])
	}

	pub fn plan_relation(&self, sql: &str) -> Result<LogicalPlan, FrontendError> {
		let statement = self.parse_one_statement(sql)?;
		let plan = self.lower_statement(&statement)?;
		if relation_plan_is_terminal(&plan) {
			return Err(unsupported("ctx.sql accepts single query relations only"));
		}
		Ok(plan)
	}

	fn parse_one_statement(&self, sql: &str) -> Result<Statement, FrontendError> {
		let dialect = GenericDialect {};
		let mut ast = Parser::parse_sql(&dialect, sql).map_err(|e| FrontendError::SqlParse {
			message: e.to_string(),
		})?;

		if ast.is_empty() {
			return Err(unsupported("ctx.sql accepts single query relations only"));
		}
		if ast.len() != 1 {
			return Err(unsupported(
				"ctx.sql accepts single query relations only; exactly one SQL statement is required",
			));
		}

		Ok(ast.remove(0))
	}

	fn lower_statement(&self, statement: &Statement) -> Result<LogicalPlan, FrontendError> {
		match statement {
			Statement::Query(query) => self.lower_query(query),
			Statement::Explain {
				describe_alias,
				analyze,
				verbose,
				query_plan,
				estimate,
				statement,
				format,
				options,
			} => self.lower_explain_statement(
				*describe_alias,
				*analyze,
				*verbose,
				*query_plan,
				*estimate,
				statement,
				format,
				options,
			),
			Statement::ExplainTable { .. } => Err(unsupported("EXPLAIN TABLE is not supported")),
			Statement::CreateTable(create_table) => self.lower_create_table(create_table),
			Statement::Insert(insert) => self.lower_insert(insert),
			Statement::Delete(delete) => self.lower_delete(delete),
			Statement::Update(update) => self.lower_update(update),
			Statement::Drop { .. } => self.lower_drop_table(statement),
			_ => Err(unsupported(
				"unsupported SQL statement; only SELECT, EXPLAIN, CREATE TABLE, INSERT, UPDATE, DELETE, and DROP TABLE are supported",
			)),
		}
	}

	fn lower_explain_statement(
		&self,
		describe_alias: DescribeAlias,
		analyze: bool,
		verbose: bool,
		query_plan: bool,
		estimate: bool,
		statement: &Statement,
		format: &Option<sqlparser::ast::AnalyzeFormatKind>,
		options: &Option<Vec<sqlparser::ast::UtilityOption>>,
	) -> Result<LogicalPlan, FrontendError> {
		if describe_alias != DescribeAlias::Explain {
			return Err(unsupported(
				"DESC and DESCRIBE aliases are not supported for EXPLAIN",
			));
		}
		if verbose {
			return Err(unsupported("EXPLAIN VERBOSE is not supported"));
		}
		if query_plan {
			return Err(unsupported("EXPLAIN QUERY PLAN is not supported"));
		}
		if estimate {
			return Err(unsupported("EXPLAIN ESTIMATE is not supported"));
		}
		if format.is_some() {
			return Err(unsupported("EXPLAIN FORMAT is not supported"));
		}
		if options.is_some() {
			return Err(unsupported("EXPLAIN options are not supported"));
		}

		match statement {
			Statement::Query(query) => {
				let input = Box::new(self.lower_query(query)?);
				if analyze {
					Ok(LogicalPlan::ExplainAnalyze { input })
				} else {
					Ok(LogicalPlan::Explain { input })
				}
			}
			Statement::Explain { .. } | Statement::ExplainTable { .. } => {
				Err(unsupported("nested EXPLAIN is not supported"))
			}
			_ => Err(unsupported("EXPLAIN supports SELECT queries only")),
		}
	}

	fn lower_update(&self, update: &sqlparser::ast::Update) -> Result<LogicalPlan, FrontendError> {
		let sqlparser::ast::Update {
			update_token,
			optimizer_hints,
			table,
			assignments,
			from,
			selection,
			returning,
			output,
			or,
			order_by,
			limit,
		} = update;
		let _parser_metadata_only = update_token;

		if !optimizer_hints.is_empty() {
			return Err(unsupported("unsupported UPDATE clause: optimizer hints"));
		}
		if from.is_some() {
			return Err(unsupported("unsupported UPDATE clause: FROM"));
		}
		if output.is_some() {
			return Err(unsupported("unsupported UPDATE clause: OUTPUT"));
		}
		if or.is_some() {
			return Err(unsupported(
				"unsupported UPDATE clause: OR (ROLLBACK/ABORT/REPLACE/...)",
			));
		}
		if !order_by.is_empty() {
			return Err(unsupported("unsupported UPDATE clause: ORDER BY"));
		}
		if limit.is_some() {
			return Err(unsupported("unsupported UPDATE clause: LIMIT"));
		}
		if table.joins.is_empty() {
			let target = lower_update_target_table(table)?;
			let assignments = lower_update_assignments(self, assignments)?;
			let predicate = selection
				.as_ref()
				.map(|expr| self.lower_expr(expr))
				.transpose()?;
			let returning = self.lower_returning_projection(returning.as_deref())?;
			Ok(LogicalPlan::Update {
				table: target,
				assignments,
				predicate,
				returning,
			})
		} else {
			Err(unsupported(
				"unsupported UPDATE target: joins not supported",
			))
		}
	}

	fn lower_delete(&self, delete: &Delete) -> Result<LogicalPlan, FrontendError> {
		let Delete {
			delete_token,
			optimizer_hints,
			tables,
			from,
			using,
			selection,
			returning,
			output,
			order_by,
			limit,
		} = delete;
		let _parser_metadata_only = delete_token;

		if !optimizer_hints.is_empty() {
			return Err(unsupported("unsupported DELETE clause: optimizer hints"));
		}
		if !tables.is_empty() {
			return Err(unsupported("DELETE supports exactly one target table"));
		}
		if using.is_some() {
			return Err(unsupported("unsupported DELETE clause: USING"));
		}
		if output.is_some() {
			return Err(unsupported("unsupported DELETE clause: OUTPUT"));
		}
		if !order_by.is_empty() {
			return Err(unsupported("unsupported DELETE clause: ORDER BY"));
		}
		if limit.is_some() {
			return Err(unsupported("unsupported DELETE clause: LIMIT"));
		}

		let target = match from {
			FromTable::WithFromKeyword(target) => target,
			FromTable::WithoutKeyword(_) => {
				return Err(unsupported("DELETE requires FROM keyword"));
			}
		};
		if target.len() != 1 {
			return Err(unsupported("DELETE supports exactly one target table"));
		}
		let table_with_joins = &target[0];
		if !table_with_joins.joins.is_empty() {
			return Err(unsupported(
				"unsupported DELETE target: joins not supported",
			));
		}

		let TableFactor::Table {
			name,
			alias,
			args,
			with_hints,
			index_hints,
			version,
			with_ordinality,
			partitions,
			json_path,
			sample,
		} = &table_with_joins.relation
		else {
			return Err(unsupported("DELETE target must be a single table"));
		};

		if alias.is_some() {
			return Err(unsupported("unsupported DELETE target: table alias"));
		}
		if args.is_some() {
			return Err(unsupported("unsupported DELETE target: table arguments"));
		}
		if !with_hints.is_empty() {
			return Err(unsupported("unsupported DELETE target: table hints"));
		}
		if !index_hints.is_empty() {
			return Err(unsupported("unsupported DELETE target: index hints"));
		}
		if version.is_some() {
			return Err(unsupported("unsupported DELETE target: table version"));
		}
		if *with_ordinality {
			return Err(unsupported("unsupported DELETE target: WITH ORDINALITY"));
		}
		if !partitions.is_empty() {
			return Err(unsupported("unsupported DELETE target: partitions"));
		}
		if json_path.is_some() {
			return Err(unsupported("unsupported DELETE target: json path"));
		}
		if sample.is_some() {
			return Err(unsupported("unsupported DELETE target: sample"));
		}

		let table = lower_table_ref_object_name(name, "DELETE")?;
		let predicate = selection
			.as_ref()
			.map(|expr| self.lower_expr(expr))
			.transpose()?;
		let returning = self.lower_returning_projection(returning.as_deref())?;
		Ok(LogicalPlan::Delete {
			table,
			predicate,
			returning,
		})
	}

	fn lower_insert(&self, insert: &Insert) -> Result<LogicalPlan, FrontendError> {
		let Insert {
			insert_token,
			optimizer_hints,
			or,
			ignore,
			into,
			table,
			table_alias,
			columns,
			overwrite,
			source,
			assignments,
			partitioned,
			after_columns,
			has_table_keyword,
			on,
			returning,
			output,
			replace_into,
			priority,
			insert_alias,
			settings,
			format_clause,
			multi_table_insert_type,
			multi_table_into_clauses,
			multi_table_when_clauses,
			multi_table_else_clause,
		} = insert;
		let _parser_metadata_only = insert_token;

		if !optimizer_hints.is_empty() {
			return Err(unsupported("unsupported INSERT clause: optimizer hints"));
		}
		if or.is_some() {
			return Err(unsupported(
				"unsupported INSERT clause: OR (REPLACE/IGNORE/...)",
			));
		}
		if *ignore {
			return Err(unsupported("unsupported INSERT clause: IGNORE"));
		}
		if *overwrite {
			return Err(unsupported("unsupported INSERT clause: OVERWRITE"));
		}
		if !*into {
			return Err(unsupported(
				"unsupported INSERT syntax: missing INTO keyword",
			));
		}
		if *has_table_keyword {
			return Err(unsupported("unsupported INSERT clause: TABLE keyword"));
		}
		if table_alias.is_some() {
			return Err(unsupported("unsupported INSERT clause: target alias"));
		}
		if !assignments.is_empty() {
			return Err(unsupported("unsupported INSERT clause: SET assignments"));
		}
		if partitioned.is_some() {
			return Err(unsupported("unsupported INSERT clause: PARTITION"));
		}
		if !after_columns.is_empty() {
			return Err(unsupported("unsupported INSERT clause: AFTER columns"));
		}
		if on.is_some() {
			return Err(unsupported(
				"unsupported INSERT clause: ON CONFLICT / ON DUPLICATE KEY",
			));
		}
		if output.is_some() {
			return Err(unsupported("unsupported INSERT clause: OUTPUT"));
		}
		if *replace_into {
			return Err(unsupported("unsupported INSERT clause: REPLACE INTO"));
		}
		if priority.is_some() {
			return Err(unsupported("unsupported INSERT clause: priority"));
		}
		if insert_alias.is_some() {
			return Err(unsupported("unsupported INSERT clause: insert alias"));
		}
		if settings.is_some() {
			return Err(unsupported("unsupported INSERT clause: SETTINGS"));
		}
		if format_clause.is_some() {
			return Err(unsupported("unsupported INSERT clause: FORMAT"));
		}
		if multi_table_insert_type.is_some()
			|| !multi_table_into_clauses.is_empty()
			|| !multi_table_when_clauses.is_empty()
			|| multi_table_else_clause.is_some()
		{
			return Err(unsupported("unsupported INSERT clause: multi-table INSERT"));
		}

		let table = lower_insert_target_name(table)?;
		let columns = lower_insert_column_list(columns)?;
		let returning = self.lower_returning_projection(returning.as_deref())?;
		match lower_insert_source(self, source.as_deref())? {
			LoweredInsertSource::Values(rows) => Ok(LogicalPlan::Insert {
				table,
				columns,
				rows,
				returning,
			}),
			LoweredInsertSource::Select(source) => Ok(LogicalPlan::InsertSelect {
				table,
				columns,
				source: Box::new(source),
				returning,
			}),
		}
	}

	fn lower_returning_projection(
		&self,
		projection: Option<&[SelectItem]>,
	) -> Result<Option<ReturningProjection>, FrontendError> {
		let Some(projection) = projection else {
			return Ok(None);
		};
		if projection.is_empty() {
			return Err(unsupported(
				"RETURNING requires at least one projection item",
			));
		}
		if matches!(projection, [SelectItem::Wildcard(_)]) {
			return Ok(Some(ReturningProjection::Star));
		}

		let mut exprs = Vec::with_capacity(projection.len());
		let mut output_names = Vec::with_capacity(projection.len());
		for item in projection {
			let (sql_expr, alias) = match item {
				SelectItem::UnnamedExpr(expr) => (expr, None),
				SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias)),
				SelectItem::ExprWithAliases {
					expr: _,
					aliases: _,
				} => {
					return Err(unsupported(
						"multi-alias RETURNING projections are not supported",
					));
				}
				SelectItem::Wildcard(_) => {
					return Err(unsupported(
						"mixed wildcard RETURNING projections are not supported",
					));
				}
				SelectItem::QualifiedWildcard(_, _) => {
					return Err(unsupported(
						"qualified wildcard RETURNING projections are not supported",
					));
				}
			};
			if expr_contains_aggregate_call(sql_expr) {
				return Err(unsupported(
					"aggregate functions are not supported in RETURNING",
				));
			}
			if expr_contains_window_function(sql_expr) {
				return Err(unsupported(
					"window functions are not supported in RETURNING",
				));
			}
			if expr_contains_subquery(sql_expr) {
				return Err(unsupported("subqueries are not supported in RETURNING"));
			}
			let expr = self.lower_expr(sql_expr)?;
			if expr_contains_qualified_column(&expr) {
				return Err(unsupported("qualified RETURNING columns are not supported"));
			}
			exprs.push(expr);
			output_names.push(alias.map(|alias| alias.value.clone()));
		}

		Ok(Some(ReturningProjection::Exprs {
			exprs,
			output_names,
		}))
	}

	fn lower_create_table(&self, create_table: &CreateTable) -> Result<LogicalPlan, FrontendError> {
		let CreateTable {
			or_replace,
			temporary,
			external,
			dynamic,
			global,
			if_not_exists,
			transient,
			volatile,
			iceberg,
			snapshot,
			name,
			columns,
			constraints,
			hive_distribution,
			hive_formats,
			table_options,
			file_format,
			location,
			query,
			without_rowid,
			like,
			clone,
			version,
			comment,
			on_commit,
			on_cluster,
			primary_key,
			order_by,
			partition_by,
			cluster_by,
			clustered_by,
			inherits,
			partition_of,
			for_values,
			strict,
			copy_grants,
			enable_schema_evolution,
			change_tracking,
			data_retention_time_in_days,
			max_data_extension_time_in_days,
			default_ddl_collation,
			with_aggregation_policy,
			with_row_access_policy,
			with_storage_lifecycle_policy,
			with_tags,
			external_volume,
			base_location,
			catalog,
			catalog_sync,
			storage_serialization_policy,
			target_lag,
			warehouse,
			refresh_mode,
			initialize,
			require_user,
			diststyle,
			distkey,
			sortkey,
			backup,
		} = create_table;

		if *or_replace && *if_not_exists {
			return Err(unsupported(
				"CREATE TABLE cannot combine OR REPLACE and IF NOT EXISTS",
			));
		}
		if *temporary {
			return Err(unsupported("unsupported CREATE TABLE clause: TEMPORARY"));
		}
		if *external {
			return Err(unsupported("unsupported CREATE TABLE clause: EXTERNAL"));
		}
		if global.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: GLOBAL/LOCAL"));
		}
		if *transient {
			return Err(unsupported("unsupported CREATE TABLE clause: TRANSIENT"));
		}
		if *volatile {
			return Err(unsupported("unsupported CREATE TABLE clause: VOLATILE"));
		}
		if *dynamic {
			return Err(unsupported("unsupported CREATE TABLE clause: DYNAMIC"));
		}
		if *iceberg {
			return Err(unsupported("unsupported CREATE TABLE clause: ICEBERG"));
		}
		if *snapshot {
			return Err(unsupported("unsupported CREATE TABLE clause: SNAPSHOT"));
		}
		if hive_formats.as_ref().is_some_and(|formats| {
			formats.row_format.is_some()
				|| formats
					.serde_properties
					.as_ref()
					.is_some_and(|properties| !properties.is_empty())
				|| formats.storage.is_some()
				|| formats.location.is_some()
		}) {
			return Err(unsupported("unsupported CREATE TABLE clause: HIVE FORMATS"));
		}
		reject_create_table_options(table_options)?;
		if file_format.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: STORED AS"));
		}
		if location.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: LOCATION"));
		}
		if query.is_some() {
			return self.lower_create_table_as_select(create_table);
		}
		if *without_rowid {
			return Err(unsupported(
				"unsupported CREATE TABLE clause: WITHOUT ROWID",
			));
		}
		if like.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: LIKE"));
		}
		if clone.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: CLONE"));
		}
		if version.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: VERSION"));
		}
		if comment.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: COMMENT"));
		}
		if on_commit.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: ON COMMIT"));
		}
		if on_cluster.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: ON CLUSTER"));
		}
		if order_by.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: ORDER BY"));
		}
		if partition_by.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: PARTITION BY"));
		}
		if cluster_by.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: CLUSTER BY"));
		}
		if clustered_by.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: CLUSTERED BY"));
		}
		if inherits.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: INHERITS"));
		}
		if partition_of.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: PARTITION OF"));
		}
		if for_values.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: FOR VALUES"));
		}
		if *strict {
			return Err(unsupported("unsupported CREATE TABLE clause: STRICT"));
		}
		if *copy_grants {
			return Err(unsupported("unsupported CREATE TABLE clause: COPY GRANTS"));
		}
		if enable_schema_evolution.is_some() {
			return Err(unsupported(
				"unsupported CREATE TABLE clause: ENABLE SCHEMA EVOLUTION",
			));
		}
		if change_tracking.is_some() {
			return Err(unsupported(
				"unsupported CREATE TABLE clause: CHANGE TRACKING",
			));
		}
		if data_retention_time_in_days.is_some() {
			return Err(unsupported(
				"unsupported CREATE TABLE clause: DATA RETENTION",
			));
		}
		if max_data_extension_time_in_days.is_some() {
			return Err(unsupported(
				"unsupported CREATE TABLE clause: MAX DATA EXTENSION",
			));
		}
		if default_ddl_collation.is_some() {
			return Err(unsupported(
				"unsupported CREATE TABLE clause: DEFAULT DDL COLLATION",
			));
		}
		if with_aggregation_policy.is_some() {
			return Err(unsupported(
				"unsupported CREATE TABLE clause: AGGREGATION POLICY",
			));
		}
		if with_row_access_policy.is_some() {
			return Err(unsupported(
				"unsupported CREATE TABLE clause: ROW ACCESS POLICY",
			));
		}
		if with_storage_lifecycle_policy.is_some() {
			return Err(unsupported(
				"unsupported CREATE TABLE clause: STORAGE LIFECYCLE POLICY",
			));
		}
		if with_tags.as_ref().is_some_and(|tags| !tags.is_empty()) {
			return Err(unsupported("unsupported CREATE TABLE clause: WITH TAGS"));
		}
		if external_volume.is_some() {
			return Err(unsupported(
				"unsupported CREATE TABLE clause: EXTERNAL_VOLUME",
			));
		}
		if base_location.is_some() {
			return Err(unsupported(
				"unsupported CREATE TABLE clause: BASE_LOCATION",
			));
		}
		if catalog.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: CATALOG"));
		}
		if catalog_sync.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: CATALOG_SYNC"));
		}
		if storage_serialization_policy.is_some() {
			return Err(unsupported(
				"unsupported CREATE TABLE clause: STORAGE_SERIALIZATION_POLICY",
			));
		}
		if target_lag.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: TARGET_LAG"));
		}
		if warehouse.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: WAREHOUSE"));
		}
		if refresh_mode.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: REFRESH_MODE"));
		}
		if initialize.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: INITIALIZE"));
		}
		if *require_user {
			return Err(unsupported("unsupported CREATE TABLE clause: REQUIRE USER"));
		}
		if diststyle.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: DISTSTYLE"));
		}
		if distkey.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: DISTKEY"));
		}
		if sortkey.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: SORTKEY"));
		}
		if backup.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: BACKUP"));
		}
		if !matches!(hive_distribution, HiveDistributionStyle::NONE) {
			return Err(unsupported(
				"unsupported CREATE TABLE clause: Hive distribution",
			));
		}
		if columns.is_empty() {
			return Err(unsupported("CREATE TABLE requires at least one column"));
		}

		let table_ref = lower_ddl_table_name(name)?;
		let (schema, row_identity) =
			lower_create_table_schema_and_identity(columns, constraints, primary_key.as_deref())?;

		Ok(LogicalPlan::CreateTable {
			name: table_ref,
			schema,
			row_identity,
			if_not_exists: *if_not_exists,
			or_replace: *or_replace,
		})
	}

	fn lower_create_table_as_select(
		&self,
		create_table: &CreateTable,
	) -> Result<LogicalPlan, FrontendError> {
		let CreateTable {
			or_replace,
			temporary,
			external,
			dynamic,
			global,
			if_not_exists,
			transient,
			volatile,
			iceberg,
			snapshot,
			name,
			columns,
			constraints,
			hive_distribution,
			hive_formats,
			table_options,
			file_format,
			location,
			query,
			without_rowid,
			like,
			clone,
			version,
			comment,
			on_commit,
			on_cluster,
			primary_key,
			order_by,
			partition_by,
			cluster_by,
			clustered_by,
			inherits,
			partition_of,
			for_values,
			strict,
			copy_grants,
			enable_schema_evolution,
			change_tracking,
			data_retention_time_in_days,
			max_data_extension_time_in_days,
			default_ddl_collation,
			with_aggregation_policy,
			with_row_access_policy,
			with_storage_lifecycle_policy,
			with_tags,
			external_volume,
			base_location,
			catalog,
			catalog_sync,
			storage_serialization_policy,
			target_lag,
			warehouse,
			refresh_mode,
			initialize,
			require_user,
			diststyle,
			distkey,
			sortkey,
			backup,
		} = create_table;

		if *or_replace && *if_not_exists {
			return Err(unsupported(
				"CREATE TABLE cannot combine OR REPLACE and IF NOT EXISTS",
			));
		}
		if !columns.is_empty() {
			return Err(unsupported(
				"CTAS does not support explicit column lists; use SELECT aliases for column names",
			));
		}
		if *temporary {
			return Err(unsupported("unsupported CREATE TABLE clause: TEMPORARY"));
		}
		if *external {
			return Err(unsupported("unsupported CREATE TABLE clause: EXTERNAL"));
		}
		if global.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: GLOBAL/LOCAL"));
		}
		if *transient {
			return Err(unsupported("unsupported CREATE TABLE clause: TRANSIENT"));
		}
		if *volatile {
			return Err(unsupported("unsupported CREATE TABLE clause: VOLATILE"));
		}
		if *dynamic {
			return Err(unsupported("unsupported CREATE TABLE clause: DYNAMIC"));
		}
		if *iceberg {
			return Err(unsupported("unsupported CREATE TABLE clause: ICEBERG"));
		}
		if *snapshot {
			return Err(unsupported("unsupported CREATE TABLE clause: SNAPSHOT"));
		}
		if !constraints.is_empty() {
			return Err(unsupported(
				"unsupported CREATE TABLE clause: table-level constraints",
			));
		}
		if hive_formats.as_ref().is_some_and(|formats| {
			formats.row_format.is_some()
				|| formats
					.serde_properties
					.as_ref()
					.is_some_and(|properties| !properties.is_empty())
				|| formats.storage.is_some()
				|| formats.location.is_some()
		}) {
			return Err(unsupported("unsupported CREATE TABLE clause: HIVE FORMATS"));
		}
		reject_create_table_options(table_options)?;
		if file_format.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: STORED AS"));
		}
		if location.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: LOCATION"));
		}
		if *without_rowid {
			return Err(unsupported(
				"unsupported CREATE TABLE clause: WITHOUT ROWID",
			));
		}
		if like.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: LIKE"));
		}
		if clone.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: CLONE"));
		}
		if version.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: VERSION"));
		}
		if comment.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: COMMENT"));
		}
		if on_commit.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: ON COMMIT"));
		}
		if on_cluster.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: ON CLUSTER"));
		}
		if primary_key.is_some() {
			return Err(unsupported(
				"unsupported CREATE TABLE clause: table-level PRIMARY KEY",
			));
		}
		if order_by.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: ORDER BY"));
		}
		if partition_by.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: PARTITION BY"));
		}
		if cluster_by.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: CLUSTER BY"));
		}
		if clustered_by.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: CLUSTERED BY"));
		}
		if inherits.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: INHERITS"));
		}
		if partition_of.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: PARTITION OF"));
		}
		if for_values.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: FOR VALUES"));
		}
		if *strict {
			return Err(unsupported("unsupported CREATE TABLE clause: STRICT"));
		}
		if *copy_grants {
			return Err(unsupported("unsupported CREATE TABLE clause: COPY GRANTS"));
		}
		if enable_schema_evolution.is_some() {
			return Err(unsupported(
				"unsupported CREATE TABLE clause: ENABLE SCHEMA EVOLUTION",
			));
		}
		if change_tracking.is_some() {
			return Err(unsupported(
				"unsupported CREATE TABLE clause: CHANGE TRACKING",
			));
		}
		if data_retention_time_in_days.is_some() {
			return Err(unsupported(
				"unsupported CREATE TABLE clause: DATA RETENTION",
			));
		}
		if max_data_extension_time_in_days.is_some() {
			return Err(unsupported(
				"unsupported CREATE TABLE clause: MAX DATA EXTENSION",
			));
		}
		if default_ddl_collation.is_some() {
			return Err(unsupported(
				"unsupported CREATE TABLE clause: DEFAULT DDL COLLATION",
			));
		}
		if with_aggregation_policy.is_some() {
			return Err(unsupported(
				"unsupported CREATE TABLE clause: AGGREGATION POLICY",
			));
		}
		if with_row_access_policy.is_some() {
			return Err(unsupported(
				"unsupported CREATE TABLE clause: ROW ACCESS POLICY",
			));
		}
		if with_storage_lifecycle_policy.is_some() {
			return Err(unsupported(
				"unsupported CREATE TABLE clause: STORAGE LIFECYCLE POLICY",
			));
		}
		if with_tags.as_ref().is_some_and(|tags| !tags.is_empty()) {
			return Err(unsupported("unsupported CREATE TABLE clause: WITH TAGS"));
		}
		if external_volume.is_some() {
			return Err(unsupported(
				"unsupported CREATE TABLE clause: EXTERNAL_VOLUME",
			));
		}
		if base_location.is_some() {
			return Err(unsupported(
				"unsupported CREATE TABLE clause: BASE_LOCATION",
			));
		}
		if catalog.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: CATALOG"));
		}
		if catalog_sync.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: CATALOG_SYNC"));
		}
		if storage_serialization_policy.is_some() {
			return Err(unsupported(
				"unsupported CREATE TABLE clause: STORAGE_SERIALIZATION_POLICY",
			));
		}
		if target_lag.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: TARGET_LAG"));
		}
		if warehouse.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: WAREHOUSE"));
		}
		if refresh_mode.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: REFRESH_MODE"));
		}
		if initialize.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: INITIALIZE"));
		}
		if *require_user {
			return Err(unsupported("unsupported CREATE TABLE clause: REQUIRE USER"));
		}
		if diststyle.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: DISTSTYLE"));
		}
		if distkey.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: DISTKEY"));
		}
		if sortkey.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: SORTKEY"));
		}
		if backup.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: BACKUP"));
		}
		if !matches!(hive_distribution, HiveDistributionStyle::NONE) {
			return Err(unsupported(
				"unsupported CREATE TABLE clause: Hive distribution",
			));
		}

		let query = query
			.as_deref()
			.ok_or_else(|| unsupported("CREATE TABLE AS SELECT requires a query"))?;
		let table = lower_ddl_table_name(name)?;
		let source = self.lower_query(query)?;
		Ok(LogicalPlan::CreateTableAsSelect {
			table,
			if_not_exists: *if_not_exists,
			or_replace: *or_replace,
			source: Box::new(source),
		})
	}

	fn lower_drop_table(&self, statement: &Statement) -> Result<LogicalPlan, FrontendError> {
		let Statement::Drop {
			object_type,
			if_exists,
			names,
			cascade,
			restrict,
			purge,
			temporary,
			table,
		} = statement
		else {
			return Err(unsupported(
				"internal planner error: expected DROP statement",
			));
		};

		if !matches!(object_type, ObjectType::Table) {
			return Err(unsupported(
				"unsupported DROP object type: only TABLE is supported",
			));
		}
		if names.len() != 1 {
			return Err(unsupported("DROP TABLE supports exactly one target table"));
		}
		if *cascade {
			return Err(unsupported("unsupported DROP TABLE clause: CASCADE"));
		}
		if *restrict {
			return Err(unsupported("unsupported DROP TABLE clause: RESTRICT"));
		}
		if *purge {
			return Err(unsupported("unsupported DROP TABLE clause: PURGE"));
		}
		if *temporary {
			return Err(unsupported("unsupported DROP TABLE clause: TEMPORARY"));
		}
		if table.is_some() {
			return Err(unsupported("unsupported DROP TABLE clause: ON"));
		}

		let name = lower_ddl_table_name(&names[0])?;
		Ok(LogicalPlan::DropTable {
			name,
			if_exists: *if_exists,
		})
	}

	fn lower_query(&self, query: &Query) -> Result<LogicalPlan, FrontendError> {
		let Query {
			with,
			body,
			order_by,
			limit_clause,
			fetch,
			locks,
			for_clause,
			settings,
			format_clause,
			pipe_operators,
		} = query;

		if with.is_some() {
			return Err(unsupported("WITH is not supported"));
		}
		if fetch.is_some() {
			return Err(unsupported("FETCH is not supported"));
		}
		if !locks.is_empty() {
			return Err(unsupported("locking clauses are not supported"));
		}
		if for_clause.is_some() {
			return Err(unsupported("FOR clause is not supported"));
		}
		if settings.is_some() {
			return Err(unsupported("query settings are not supported"));
		}
		if format_clause.is_some() {
			return Err(unsupported("FORMAT clause is not supported"));
		}
		if !pipe_operators.is_empty() {
			return Err(unsupported("pipe operators are not supported"));
		}
		let LoweredSelect {
			plan,
			aggregate_output_catalog,
		} = self.lower_query_body(body)?;

		self.apply_order_by_limit_tail(
			plan,
			aggregate_output_catalog.as_ref(),
			order_by.as_ref(),
			limit_clause.as_ref(),
		)
	}

	fn lower_query_body(&self, body: &SetExpr) -> Result<LoweredSelect, FrontendError> {
		match body {
			SetExpr::Select(select) => self.lower_select(select),
			SetExpr::SetOperation {
				left,
				op,
				set_quantifier,
				right,
			} => {
				let left = self.lower_query_body(left)?.plan;
				let right = self.lower_query_body(right)?.plan;
				let plan = self.lower_set_operation(left, right, op, set_quantifier)?;
				Ok(LoweredSelect {
					plan,
					aggregate_output_catalog: None,
				})
			}
			SetExpr::Query(_) => Err(unsupported(
				"parenthesized set-operation arms with ORDER BY/LIMIT are not supported",
			)),
			_ => Err(unsupported("unsupported query body")),
		}
	}

	fn lower_set_operation(
		&self,
		left: LogicalPlan,
		right: LogicalPlan,
		op: &SetOperator,
		quantifier: &SetQuantifier,
	) -> Result<LogicalPlan, FrontendError> {
		if matches!(
			quantifier,
			SetQuantifier::ByName | SetQuantifier::AllByName | SetQuantifier::DistinctByName
		) {
			return Err(unsupported("set operations BY NAME are not supported"));
		}
		match op {
			SetOperator::Union => match quantifier {
				SetQuantifier::All => Ok(LogicalPlan::Union {
					left: Box::new(left),
					right: Box::new(right),
				}),
				SetQuantifier::None | SetQuantifier::Distinct => Ok(LogicalPlan::Distinct {
					input: Box::new(LogicalPlan::Union {
						left: Box::new(left),
						right: Box::new(right),
					}),
				}),
				SetQuantifier::ByName
				| SetQuantifier::AllByName
				| SetQuantifier::DistinctByName => unreachable!("BY NAME rejected above"),
			},
			SetOperator::Intersect => {
				if *quantifier == SetQuantifier::All {
					return Ok(LogicalPlan::SetOpAll {
						left: Box::new(left),
						right: Box::new(right),
						kind: SetOpAllKind::Intersect,
					});
				}
				self.lower_distinct_membership_set_op(left, right, JoinKind::LeftSemi)
			}
			SetOperator::Except | SetOperator::Minus => {
				if *quantifier == SetQuantifier::All {
					return Ok(LogicalPlan::SetOpAll {
						left: Box::new(left),
						right: Box::new(right),
						kind: SetOpAllKind::Except,
					});
				}
				self.lower_distinct_membership_set_op(left, right, JoinKind::LeftAnti)
			}
		}
	}

	fn lower_distinct_membership_set_op(
		&self,
		left: LogicalPlan,
		right: LogicalPlan,
		kind: JoinKind,
	) -> Result<LogicalPlan, FrontendError> {
		Ok(LogicalPlan::Distinct {
			input: Box::new(LogicalPlan::Join {
				left: Box::new(left),
				right: Box::new(right),
				kind,
				on: Vec::new(),
				null_safe_keys: true,
				strategy_intent: JoinStrategyIntent::Auto,
			}),
		})
	}

	fn apply_order_by_limit_tail(
		&self,
		plan: LogicalPlan,
		aggregate_output_catalog: Option<&AggregateOutputCatalog>,
		order_by: Option<&OrderBy>,
		limit_clause: Option<&LimitClause>,
	) -> Result<LogicalPlan, FrontendError> {
		let limit_offset = lower_limit_clause(limit_clause)?;
		if order_by.is_none() && limit_offset.offset.is_some() {
			return Err(unsupported(
				"LIMIT/OFFSET without ORDER BY is not supported",
			));
		}

		if let Some(order_by) = order_by {
			let order_by = self.lower_order_by(order_by, aggregate_output_catalog)?;
			let (sort_input, order_by, final_projection) =
				if aggregate_output_catalog.is_some() && sort_needs_hidden_projection(&order_by) {
					add_hidden_sort_projection(plan, order_by, aggregate_output_catalog)?
				} else {
					(plan, order_by, None)
				};
			let limit = limit_offset
				.limit
				.map(parse_non_negative_usize_literal)
				.transpose()?;
			let offset = limit_offset
				.offset
				.map(parse_non_negative_usize_literal)
				.transpose()?
				.unwrap_or(0);
			let sorted = LogicalPlan::Sort {
				input: Box::new(LogicalPlan::Exchange {
					input: Box::new(sort_input),
					partition_count: 1,
					key: Vec::new(),
				}),
				order_by,
				limit,
				offset,
			};
			if let Some(final_projection) = final_projection {
				Ok(LogicalPlan::Project {
					input: Box::new(sorted),
					exprs: final_projection.exprs,
					output_names: final_projection.output_names,
				})
			} else {
				Ok(sorted)
			}
		} else if let Some(limit) = limit_offset.limit {
			Ok(LogicalPlan::Take {
				input: Box::new(plan),
				limit: parse_non_negative_usize_literal(limit)?,
			})
		} else {
			Ok(plan)
		}
	}

	fn lower_select(&self, select: &Select) -> Result<LoweredSelect, FrontendError> {
		let Select {
			select_token,
			optimizer_hints,
			distinct,
			select_modifiers,
			top,
			top_before_distinct,
			projection,
			exclude,
			into,
			from,
			lateral_views,
			prewhere,
			connect_by,
			selection,
			group_by,
			cluster_by,
			distribute_by,
			sort_by,
			having,
			named_window,
			qualify,
			window_before_qualify,
			value_table_mode,
			flavor,
		} = select;
		let _parser_metadata_only = (select_token, top_before_distinct, window_before_qualify);

		let mut broadcast_hint = parse_broadcast_join_hint(optimizer_hints)?;
		let distinct_requested = match distinct {
			None => false,
			Some(Distinct::Distinct) => true,
			Some(Distinct::On(_)) => return Err(unsupported("DISTINCT ON is not supported")),
			Some(Distinct::All) => return Err(unsupported("SELECT ALL is not supported")),
		};
		if select_modifiers.is_some() {
			return Err(unsupported("SELECT modifiers are not supported"));
		}
		if top.is_some() {
			return Err(unsupported("TOP is not supported"));
		}
		if exclude.is_some() {
			return Err(unsupported("SELECT EXCLUDE is not supported"));
		}
		if into.is_some() {
			return Err(unsupported("SELECT INTO is not supported"));
		}
		if !lateral_views.is_empty() {
			return Err(unsupported("LATERAL VIEW is not supported"));
		}
		if prewhere.is_some() {
			return Err(unsupported("PREWHERE is not supported"));
		}
		if !cluster_by.is_empty() {
			return Err(unsupported("CLUSTER BY is not supported"));
		}
		if !distribute_by.is_empty() {
			return Err(unsupported("DISTRIBUTE BY is not supported"));
		}
		if !sort_by.is_empty() {
			return Err(unsupported("SORT BY is not supported"));
		}
		// ADR 250: collect the WINDOW clause definitions into a name -> spec map for `OVER w` resolution
		// in lower_select_window. A definition that is itself a reference (`WINDOW w2 AS w1`) is window
		// inheritance — a spec-merge mechanism, deferred.
		let mut named_windows: NamedWindowMap = HashMap::new();
		for NamedWindowDefinition(name, expr) in named_window {
			let spec = match expr {
				NamedWindowExpr::WindowSpec(spec) => spec,
				NamedWindowExpr::NamedWindow(_) => {
					return Err(unsupported(
						"window inheritance (WINDOW w AS other_window) is not supported",
					));
				}
			};
			if named_windows.insert(name.value.clone(), spec).is_some() {
				return Err(unsupported(format!(
					"duplicate WINDOW definition '{}'",
					name.value
				)));
			}
		}
		if qualify.is_some() {
			return Err(unsupported("QUALIFY is not supported"));
		}
		if value_table_mode.is_some() {
			return Err(unsupported("SELECT AS VALUE/STRUCT is not supported"));
		}
		if !connect_by.is_empty() {
			return Err(unsupported("CONNECT BY is not supported"));
		}
		if !matches!(flavor, sqlparser::ast::SelectFlavor::Standard) {
			return Err(unsupported("FROM-first SELECT is not supported"));
		}
		let group_keys = if group_by_is_empty(group_by) {
			None
		} else {
			Some(lower_group_by_keys(group_by)?)
		};
		if distinct_requested && group_keys.is_some() {
			return Err(unsupported("DISTINCT with GROUP BY is not supported"));
		}
		if distinct_requested && having.is_some() {
			return Err(unsupported("DISTINCT with HAVING is not supported"));
		}
		if distinct_requested && projection_contains_aggregate_call(projection) {
			return Err(unsupported(
				"DISTINCT with aggregate projections is not supported",
			));
		}
		if distinct_requested && projection_contains_window_function(projection) {
			return Err(unsupported(
				"DISTINCT with window functions is not supported",
			));
		}
		if having.is_some()
			&& group_keys.is_none()
			&& !projection_contains_aggregate_call(projection)
		{
			return Err(unsupported(
				"HAVING requires GROUP BY or an aggregate projection",
			));
		}

		// 1. Handle FROM (Scan)
		if from.is_empty() {
			return Err(unsupported("SELECT without FROM is not supported"));
		}

		// ADR 239: comma joins lower to a left-deep keyless `Cross` fold over the FROM list.
		// The inner-join WHERE pushdown only applies to a single FROM entry's join chain; with a
		// comma list the WHERE filters the full cartesian (no equi-extraction across the comma).
		let is_comma_join = from.len() > 1;
		let table_with_joins = &from[0];
		let all_inner_join_chain = !is_comma_join
			&& table_with_joins.joins.iter().all(|join| {
				join_operator_kind_for_filter_pushdown(&join.join_operator) == Some(JoinKind::Inner)
			});
		let contains_derived_join_factor = table_factor_is_derived(&table_with_joins.relation)
			|| table_with_joins
				.joins
				.iter()
				.any(|join| table_factor_is_derived(&join.relation));
		if contains_derived_join_factor
			&& !table_with_joins.joins.is_empty()
			&& !all_inner_join_chain
		{
			return Err(unsupported(
				"derived tables in joins are only supported in all-inner chains",
			));
		}
		let mut pending_where_conjuncts = match (all_inner_join_chain, selection.as_ref()) {
			(true, Some(selection)) => split_top_level_and(selection),
			_ => Vec::new(),
		};
		let mut pushed_leftmost_filter = false;
		let mut plan = self.lower_table_factor(&table_with_joins.relation)?;
		for join in &table_with_joins.joins {
			let mut right = self.lower_table_factor(&join.relation)?;
			let (kind, on) = self.lower_join_operator(&join.join_operator)?;
			if all_inner_join_chain {
				let right_qualifiers = plan_qualifiers(&right);
				if !pushed_leftmost_filter {
					let left_qualifiers = plan_qualifiers(&plan);
					plan = self.push_side_local_join_filters(
						plan,
						&mut pending_where_conjuncts,
						&left_qualifiers,
						&right_qualifiers,
						JoinFilterTarget::Left,
					)?;
					pushed_leftmost_filter = true;
				}
				let left_qualifiers = plan_qualifiers(&plan);
				right = self.push_side_local_join_filters(
					right,
					&mut pending_where_conjuncts,
					&left_qualifiers,
					&right_qualifiers,
					JoinFilterTarget::Right,
				)?;
			}
			let strategy_intent =
				join_strategy_intent_for_hint(&mut broadcast_hint, &plan, &right)?;
			plan = match strategy_intent {
				JoinStrategyIntent::Auto => build_auto_join_plan(plan, right, kind, on),
				JoinStrategyIntent::Pinned {
					strategy,
					source,
					build_side,
				} => build_join_plan(plan, right, kind, on, strategy, source, build_side)?,
			};
		}
		// ADR 239: fold any comma-separated FROM entries left-deep as keyless `Cross` joins. Each
		// entry's own explicit join chain is lowered first (honoring broadcast hints); the entry is
		// then cross-joined onto the running plan with `on: []`, leaving the binder to force the
		// broadcast-only strategy for `Cross`.
		for table_with_joins in from.iter().skip(1) {
			let mut right = self.lower_table_factor(&table_with_joins.relation)?;
			for join in &table_with_joins.joins {
				let inner = self.lower_table_factor(&join.relation)?;
				let (kind, on) = self.lower_join_operator(&join.join_operator)?;
				let strategy_intent =
					join_strategy_intent_for_hint(&mut broadcast_hint, &right, &inner)?;
				right = match strategy_intent {
					JoinStrategyIntent::Auto => build_auto_join_plan(right, inner, kind, on),
					JoinStrategyIntent::Pinned {
						strategy,
						source,
						build_side,
					} => build_join_plan(right, inner, kind, on, strategy, source, build_side)?,
				};
			}
			plan = build_auto_join_plan(plan, right, JoinKind::Cross, Vec::new());
		}
		reject_unused_broadcast_hint(&broadcast_hint)?;

		// 2. Handle WHERE (Filter / correlated EXISTS decorrelation)
		if let Some(selection) = selection {
			if all_inner_join_chain {
				if let Some(selection) = combine_and_sql_exprs(&pending_where_conjuncts) {
					plan = self.lower_where(plan, &selection)?;
				}
			} else {
				plan = self.lower_where(plan, selection)?;
			}
		}
		// 3. Handle SELECT list (Project or Aggregate)
		if let Some(group_keys) = group_keys {
			let group_expr = group_keys
				.iter()
				.map(|key| key.expr.clone())
				.collect::<Vec<_>>();
			let AggregateProjection {
				aggr_expr,
				catalog,
				final_projection,
				distinct_rewrite,
			} = self.lower_aggregate_projection(
				projection,
				having.as_ref(),
				&group_keys,
				AggregateSqlMode::Grouped,
			)?;
			let having_predicate = having
				.as_ref()
				.map(|having| self.lower_having_predicate(having, &catalog))
				.transpose()?;
			if aggr_expr.is_empty() {
				return Err(unsupported("GROUP BY requires at least one aggregate call"));
			}
			let aggregate = if let Some(rewrite) = distinct_rewrite {
				// ADR 240: the rewrite fans out + recombines into a plan whose schema matches a single
				// grouped Aggregate, so the having/final-projection tail below is reused unchanged.
				rewrite.into_aggregated_plan(plan)
			} else {
				LogicalPlan::Aggregate {
					input: Box::new(LogicalPlan::Exchange {
						input: Box::new(plan),
						partition_count: DEFAULT_AGGREGATE_PARTITION_COUNT,
						key: group_expr.clone(),
					}),
					group_expr,
					aggr_expr,
				}
			};
			let plan = if let Some(predicate) = having_predicate {
				LogicalPlan::Filter {
					input: Box::new(aggregate),
					predicate,
				}
			} else {
				aggregate
			};
			let plan = if let Some(final_projection) = final_projection {
				LogicalPlan::Project {
					input: Box::new(plan),
					exprs: final_projection.exprs,
					output_names: final_projection.output_names,
				}
			} else {
				plan
			};
			return Ok(LoweredSelect {
				plan,
				aggregate_output_catalog: Some(catalog),
			});
		}

		if projection_contains_aggregate_call(projection) {
			// P0.5: accepted global aggregates keep the pre-ADR single-stage shape; no
			// distributed exchange is inserted for the accounting refactor.
			let AggregateProjection {
				aggr_expr,
				catalog,
				final_projection,
				distinct_rewrite,
			} = self.lower_aggregate_projection(
				projection,
				having.as_ref(),
				&[],
				AggregateSqlMode::Global,
			)?;
			let aggregate = if let Some(rewrite) = distinct_rewrite {
				// ADR 240: global recombine fans out + Cross-joins one-row branches (scalar-local).
				rewrite.into_aggregated_plan(plan)
			} else {
				LogicalPlan::Aggregate {
					input: Box::new(plan),
					group_expr: vec![],
					aggr_expr,
				}
			};
			let plan = if let Some(having) = having.as_ref() {
				let predicate = self.lower_having_predicate(having, &catalog)?;
				LogicalPlan::Filter {
					input: Box::new(aggregate),
					predicate,
				}
			} else {
				aggregate
			};
			let plan = if let Some(final_projection) = final_projection {
				LogicalPlan::Project {
					input: Box::new(plan),
					exprs: final_projection.exprs,
					output_names: final_projection.output_names,
				}
			} else {
				plan
			};
			return Ok(LoweredSelect {
				plan,
				aggregate_output_catalog: Some(catalog),
			});
		}

		if matches!(projection.as_slice(), [SelectItem::Wildcard(_)]) {
			return Ok(LoweredSelect {
				plan: wrap_distinct_if_needed(plan, distinct_requested),
				aggregate_output_catalog: None,
			});
		}

		// ADR 241: extract SELECT-projection ranking window functions into a `Window` node beneath the
		// projection (evaluated post-HAVING/pre-projection). Returns the rewritten projection where each
		// window call is replaced by a reference to its appended rank column.
		let (plan, window_projection) =
			self.lower_select_window(plan, projection, &named_windows)?;
		let projection = window_projection.as_slice();

		let mut exprs = Vec::new();
		let mut output_names = Vec::new();
		for item in projection {
			match item {
				SelectItem::UnnamedExpr(sql_expr) => {
					exprs.push(self.lower_expr(sql_expr)?);
					output_names.push(None);
				}
				SelectItem::ExprWithAlias { expr, alias } => {
					exprs.push(self.lower_expr(expr)?);
					output_names.push(Some(alias.value.clone()));
				}
				SelectItem::ExprWithAliases {
					expr: _,
					aliases: _,
				} => return Err(unsupported("multi-alias projections are not supported")),
				SelectItem::Wildcard(_) => {
					return Err(unsupported("mixed wildcard projections are not supported"));
				}
				SelectItem::QualifiedWildcard(_, _) => {
					return Err(unsupported(
						"qualified wildcard projections are not supported",
					));
				}
			}
		}

		let plan = LogicalPlan::Project {
			input: Box::new(plan),
			exprs,
			output_names,
		};
		Ok(LoweredSelect {
			plan: wrap_distinct_if_needed(plan, distinct_requested),
			aggregate_output_catalog: None,
		})
	}

	/// ADR 241: lower SELECT-projection ranking window functions into a single `Window` node over an
	/// `Exchange` on the partition key. All window calls in the projection must share one `OVER` spec
	/// (multiple distinct specs are deferred — the seed fragmenter allows one exchange boundary). The
	/// returned projection replaces each window call with a reference to its appended rank column.
	fn lower_select_window(
		&self,
		plan: LogicalPlan,
		projection: &[SelectItem],
		named_windows: &NamedWindowMap<'_>,
	) -> Result<(LogicalPlan, Vec<SelectItem>), FrontendError> {
		if !projection_contains_window_function(projection) {
			return Ok((plan, projection.to_vec()));
		}

		// ADR 248: group window calls by their (PARTITION BY, ORDER BY) spec — the frame is NOT part of
		// the key (it rides each call, ADR 246), so calls that share partition+order but differ in frame
		// share one node. Each distinct spec lowers to its own `Window`-over-`Exchange`; groups are kept
		// in first-seen order. Internal output names are assigned from a global counter so columns from
		// different specs never collide in the final schema.
		let mut groups: Vec<WindowSpecGroup> = Vec::new();
		let mut rewritten: Vec<SelectItem> = Vec::with_capacity(projection.len());
		let mut window_index = 0_usize;

		for item in projection {
			let (expr, alias) = match item {
				SelectItem::UnnamedExpr(expr) => (expr, None),
				SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.clone())),
				_ => {
					return Err(unsupported("window functions require simple SELECT items"));
				}
			};

			match self.extract_window_call(expr, alias.clone(), named_windows)? {
				Some(window) => {
					let internal_name = window_internal_output_name(window_index);
					window_index += 1;
					rewritten.push(SelectItem::ExprWithAlias {
						expr: SqlExpr::Identifier(Ident::new(internal_name.clone())),
						alias: Ident::new(window.output_name.clone()),
					});
					let call = WindowFunctionCall {
						function: window.function,
						output_name: internal_name,
						args: window.args,
						frame: window.frame,
					};
					match groups.iter_mut().find(|group| {
						window_specs_match(
							&group.partition_by,
							&group.order_by,
							&window.partition_by,
							&window.order_by,
						)
					}) {
						Some(group) => group.functions.push(call),
						None => groups.push(WindowSpecGroup {
							partition_by: window.partition_by,
							order_by: window.order_by,
							functions: vec![call],
						}),
					}
				}
				None => {
					if expr_contains_window_function(expr) {
						return Err(unsupported(
							"window functions inside compound expressions are not supported",
						));
					}
					rewritten.push(item.clone());
				}
			}
		}

		// Stack the groups first-seen-innermost: the first distinct spec wraps the input plan, later
		// specs wrap outward. Each spec re-partitions on its own key (empty PARTITION BY gathers to one
		// partition); every `Window` node is row-preserving, so the final schema carries the input
		// columns plus every spec's window columns regardless of stack order.
		let mut node = plan;
		for group in groups {
			let exchange = LogicalPlan::Exchange {
				input: Box::new(node),
				partition_count: if group.partition_by.is_empty() {
					1
				} else {
					DEFAULT_AGGREGATE_PARTITION_COUNT
				},
				key: group.partition_by.clone(),
			};
			node = LogicalPlan::Window {
				input: Box::new(exchange),
				partition_by: group.partition_by,
				order_by: group.order_by,
				functions: group.functions,
			};
		}
		Ok((node, rewritten))
	}

	/// Extract a ranking window function from a SELECT expression, or `None` if `expr` is not itself a
	/// window call. Named windows and non-ranking/non-aggregate functions are rejected; explicit `ROWS`
	/// frames (ADR 243) lower on aggregate calls, `RANGE`/`GROUPS` stay rejected.
	fn extract_window_call(
		&self,
		expr: &SqlExpr,
		alias: Option<String>,
		named_windows: &NamedWindowMap<'_>,
	) -> Result<Option<ExtractedWindow>, FrontendError> {
		let SqlExpr::Function(function) = expr else {
			return Ok(None);
		};
		let Some(over) = &function.over else {
			return Ok(None);
		};
		// ADR 250: `OVER w` resolves to the `WINDOW w AS (...)` definition; a resolved spec is then
		// processed exactly like an inline one (it carries no further name).
		let spec = match over {
			WindowType::WindowSpec(spec) => spec,
			WindowType::NamedWindow(name) => named_windows
				.get(name.value.as_str())
				.copied()
				.ok_or_else(|| unsupported(format!("unknown named window '{}'", name.value)))?,
		};
		// The `OVER (w ...)` extension form (a named base merged with added clauses) is a spec-merge
		// mechanism, deferred — only the bare `OVER w` reference resolves.
		if spec.window_name.is_some() {
			return Err(unsupported(
				"window reference inside OVER (...) is not supported; use OVER <name>",
			));
		}
		// ADR 241 ranking + ADR 242 aggregate windows. Ranking takes no arguments; aggregate windows
		// reuse the ordinary aggregate lowering (COUNT(*)/COUNT(x)/SUM/MIN/MAX/AVG) over the SQL default
		// frame. Explicit frames, DISTINCT, and experimental aggregates stay rejected.
		let ranking = if function_name_eq(&function.name, "ROW_NUMBER") {
			Some(WindowFunction::RowNumber)
		} else if function_name_eq(&function.name, "RANK") {
			Some(WindowFunction::Rank)
		} else if function_name_eq(&function.name, "DENSE_RANK") {
			Some(WindowFunction::DenseRank)
		} else {
			None
		};
		let (window_function, window_args) = if let Some(ranking) = ranking {
			if !window_function_args_are_empty(&function.args) {
				return Err(unsupported("ranking window functions take no arguments"));
			}
			(ranking, Vec::new())
		} else if aggregate_function_name(&function.name).is_some() {
			// Reuse the ordinary aggregate lowering with the window modifier stripped — the OVER spec is
			// extracted separately below. FILTER / non-DISTINCT modifiers stay rejected by that path;
			// DISTINCT is detected and rejected here.
			let mut bare = function.clone();
			bare.over = None;
			let lowered = self.lower_aggregate_function(&bare, AggregateSqlMode::Global)?;
			if lowered.distinct {
				return Err(unsupported("DISTINCT window aggregates are not supported"));
			}
			match lowered.function {
				AggregateFunction::CountAll => (
					WindowFunction::Aggregate(AggregateFunction::CountAll),
					Vec::new(),
				),
				AggregateFunction::Count
				| AggregateFunction::Sum
				| AggregateFunction::Min
				| AggregateFunction::Max
				| AggregateFunction::Avg => (WindowFunction::Aggregate(lowered.function), lowered.args),
				AggregateFunction::LinearRegress | AggregateFunction::ApproxQuantile(_) => {
					return Err(unsupported(
						"only COUNT, SUM, MIN, MAX, and AVG window aggregate functions are supported",
					));
				}
			}
		} else if let Some((value_fn, value_args)) = self.extract_value_function(function)? {
			(WindowFunction::Value(value_fn), value_args)
		} else if function_name_eq(&function.name, "NTILE") {
			// ADR 249: NTILE(n) — n a strictly positive integer literal. Frame-independent, no bound
			// argument (the bucket count rides in the variant, like ranking).
			if function.null_treatment.is_some() {
				return Err(unsupported(
					"IGNORE/RESPECT NULLS is not supported for window functions",
				));
			}
			if function.filter.is_some() {
				return Err(unsupported("FILTER is not supported for window functions"));
			}
			let buckets = match &function.args {
				FunctionArguments::List(list) if list.args.len() == 1 => match list.args.first() {
					Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))) => {
						ntile_bucket_count(expr)?
					}
					_ => {
						return Err(unsupported(
							"NTILE requires a positive integer bucket count",
						));
					}
				},
				_ => {
					return Err(unsupported(
						"NTILE requires exactly one positive integer bucket count",
					));
				}
			};
			(WindowFunction::Ntile { buckets }, Vec::new())
		} else {
			return Err(unsupported(
				"only ROW_NUMBER, RANK, DENSE_RANK, NTILE, COUNT/SUM/MIN/MAX/AVG, and LAG/LEAD/FIRST_VALUE/LAST_VALUE/NTH_VALUE window functions are supported",
			));
		};
		// LAG/LEAD are positional navigation — meaningless without an order.
		if matches!(
			window_function,
			WindowFunction::Value(ValueFunction::Lag { .. } | ValueFunction::Lead { .. })
		) && spec.order_by.is_empty()
		{
			return Err(unsupported("LAG/LEAD require an ORDER BY"));
		}
		let partition_by = spec
			.partition_by
			.iter()
			.map(|expr| self.lower_expr(expr))
			.collect::<Result<Vec<_>, _>>()?;
		let order_by = spec
			.order_by
			.iter()
			.map(|order| {
				if order.with_fill.is_some() {
					return Err(unsupported("ORDER BY WITH FILL is not supported"));
				}
				Ok(SortExpr {
					expr: self.lower_expr(&order.expr)?,
					descending: order.options.asc == Some(false),
					nulls_first: order.options.nulls_first.unwrap_or(false),
				})
			})
			.collect::<Result<Vec<_>, FrontendError>>()?;
		// ADR 243/244/245: lower an explicit `ROWS`/`RANGE`/`GROUPS BETWEEN …` frame on an aggregate
		// window. Frames on ranking functions are rejected. RANGE value offsets need exactly one ORDER BY
		// column; GROUPS count offsets need an ORDER BY (to define peer groups), but any key/count.
		let frame = match &spec.window_frame {
			Some(frame) => {
				if matches!(
					window_function,
					WindowFunction::RowNumber
						| WindowFunction::Rank
						| WindowFunction::DenseRank
						| WindowFunction::Ntile { .. }
				) {
					return Err(unsupported(
						"window frames are not supported on ranking or NTILE functions",
					));
				}
				let lowered = self.lower_window_frame(frame)?;
				if frame_has_offset_bound(&lowered) {
					if lowered.unit == WindowFrameUnit::Range && spec.order_by.len() != 1 {
						return Err(unsupported(
							"RANGE value offsets require exactly one ORDER BY column",
						));
					}
					if lowered.unit == WindowFrameUnit::Groups && spec.order_by.is_empty() {
						return Err(unsupported("GROUPS count offsets require an ORDER BY"));
					}
				}
				Some(lowered)
			}
			None => None,
		};
		let output_name =
			alias.unwrap_or_else(|| window_function_default_name(&window_function).to_string());
		Ok(Some(ExtractedWindow {
			function: window_function,
			output_name,
			partition_by,
			order_by,
			args: window_args,
			frame,
		}))
	}

	/// ADR 246: recognise a value/navigation window function (`LAG`/`LEAD`/`FIRST_VALUE`/`LAST_VALUE`/
	/// `NTH_VALUE`) and lower it to `(ValueFunction, [value_expr])`. `None` if the call is not one of these.
	/// `IGNORE/RESPECT NULLS`, `FILTER`, and non-constant `LAG`/`LEAD` defaults stay rejected.
	fn extract_value_function(
		&self,
		function: &Function,
	) -> Result<Option<(ValueFunction, Vec<Expr>)>, FrontendError> {
		let name = if function_name_eq(&function.name, "LAG") {
			"LAG"
		} else if function_name_eq(&function.name, "LEAD") {
			"LEAD"
		} else if function_name_eq(&function.name, "FIRST_VALUE") {
			"FIRST_VALUE"
		} else if function_name_eq(&function.name, "LAST_VALUE") {
			"LAST_VALUE"
		} else if function_name_eq(&function.name, "NTH_VALUE") {
			"NTH_VALUE"
		} else {
			return Ok(None);
		};
		if function.null_treatment.is_some() {
			return Err(unsupported(
				"IGNORE/RESPECT NULLS is not supported for window functions",
			));
		}
		if function.filter.is_some() {
			return Err(unsupported("FILTER is not supported for window functions"));
		}
		let FunctionArguments::List(list) = &function.args else {
			return Err(unsupported(
				"value window functions require an argument list",
			));
		};
		let args = &list.args;
		let value_expr = self.lower_expr(value_function_arg_expr(args.first())?)?;
		let value_fn = match name {
			"LAG" | "LEAD" => {
				let offset = match args.get(1) {
					Some(arg) => window_frame_offset(value_function_arg_expr(Some(arg))?)?,
					None => 1,
				};
				let default = match args.get(2) {
					Some(arg) => {
						Some(self.lower_value_default(value_function_arg_expr(Some(arg))?)?)
					}
					None => None,
				};
				if args.len() > 3 {
					return Err(unsupported("LAG/LEAD take at most three arguments"));
				}
				if name == "LAG" {
					ValueFunction::Lag { offset, default }
				} else {
					ValueFunction::Lead { offset, default }
				}
			}
			"FIRST_VALUE" => {
				if args.len() != 1 {
					return Err(unsupported("FIRST_VALUE takes one argument"));
				}
				ValueFunction::FirstValue
			}
			"LAST_VALUE" => {
				if args.len() != 1 {
					return Err(unsupported("LAST_VALUE takes one argument"));
				}
				ValueFunction::LastValue
			}
			_ => {
				let position = value_function_arg_expr(args.get(1))?;
				let n = window_frame_offset(position)?;
				if n == 0 {
					return Err(unsupported("NTH_VALUE position must be at least 1"));
				}
				if args.len() != 2 {
					return Err(unsupported("NTH_VALUE takes two arguments"));
				}
				ValueFunction::NthValue { n }
			}
		};
		Ok(Some((value_fn, vec![value_expr])))
	}

	/// ADR 246: lower a `LAG`/`LEAD` default to a constant `ScalarValue`. A bare `-1` parses as a unary
	/// minus over a numeric literal, so strip and negate it; other non-literal defaults are rejected.
	fn lower_value_default(&self, expr: &SqlExpr) -> Result<ScalarValue, FrontendError> {
		let (negate, inner) = match expr {
			SqlExpr::UnaryOp {
				op: sqlparser::ast::UnaryOperator::Minus,
				expr,
			} => (true, expr.as_ref()),
			other => (false, other),
		};
		match self.lower_expr(inner)? {
			Expr::Literal(value) => {
				if negate {
					negate_scalar_literal(value)
				} else {
					Ok(value)
				}
			}
			_ => Err(unsupported("LAG/LEAD default must be a constant literal")),
		}
	}

	/// ADR 243/244: lower a sqlparser `<unit> BETWEEN …` frame to the IR `WindowFrame`. `GROUPS` stays
	/// rejected (→ a follow-on); the `end_bound = None` shorthand means `… AND CURRENT ROW`.
	fn lower_window_frame(
		&self,
		frame: &sqlparser::ast::WindowFrame,
	) -> Result<WindowFrame, FrontendError> {
		let unit = match frame.units {
			sqlparser::ast::WindowFrameUnits::Rows => WindowFrameUnit::Rows,
			sqlparser::ast::WindowFrameUnits::Range => WindowFrameUnit::Range,
			sqlparser::ast::WindowFrameUnits::Groups => WindowFrameUnit::Groups,
		};
		let start = self.lower_window_frame_bound(&frame.start_bound, &unit)?;
		let end = match &frame.end_bound {
			Some(bound) => self.lower_window_frame_bound(bound, &unit)?,
			None => WindowFrameBound::CurrentRow,
		};
		if matches!(start, WindowFrameBound::UnboundedFollowing) {
			return Err(unsupported(
				"window frame start cannot be UNBOUNDED FOLLOWING",
			));
		}
		if matches!(end, WindowFrameBound::UnboundedPreceding) {
			return Err(unsupported(
				"window frame end cannot be UNBOUNDED PRECEDING",
			));
		}
		if window_frame_bound_rank(&start) > window_frame_bound_rank(&end) {
			return Err(unsupported(
				"window frame start must not come after frame end",
			));
		}
		Ok(WindowFrame { unit, start, end })
	}

	fn lower_window_frame_bound(
		&self,
		bound: &sqlparser::ast::WindowFrameBound,
		unit: &WindowFrameUnit,
	) -> Result<WindowFrameBound, FrontendError> {
		Ok(match bound {
			sqlparser::ast::WindowFrameBound::CurrentRow => WindowFrameBound::CurrentRow,
			sqlparser::ast::WindowFrameBound::Preceding(None) => {
				WindowFrameBound::UnboundedPreceding
			}
			sqlparser::ast::WindowFrameBound::Following(None) => {
				WindowFrameBound::UnboundedFollowing
			}
			sqlparser::ast::WindowFrameBound::Preceding(Some(expr)) => {
				WindowFrameBound::Preceding(self.lower_frame_delta(expr, unit)?)
			}
			sqlparser::ast::WindowFrameBound::Following(Some(expr)) => {
				WindowFrameBound::Following(self.lower_frame_delta(expr, unit)?)
			}
		})
	}

	/// A `ROWS` offset is a non-negative integer row count; a `RANGE` offset is a typed value literal
	/// (a number or an `INTERVAL …`) whose type the binder checks against the ORDER BY key.
	fn lower_frame_delta(
		&self,
		expr: &SqlExpr,
		unit: &WindowFrameUnit,
	) -> Result<WindowFrameDelta, FrontendError> {
		match unit {
			WindowFrameUnit::Rows => Ok(WindowFrameDelta::Rows(window_frame_offset(expr)?)),
			WindowFrameUnit::Groups => Ok(WindowFrameDelta::Groups(window_frame_offset(expr)?)),
			WindowFrameUnit::Range => match self.lower_expr(expr)? {
				Expr::Literal(value) => {
					if !range_offset_is_non_negative(&value) {
						return Err(unsupported(
							"RANGE frame offset must be a non-negative value literal",
						));
					}
					Ok(WindowFrameDelta::Range(value))
				}
				_ => Err(unsupported("RANGE frame offset must be a literal value")),
			},
		}
	}

	fn lower_order_by(
		&self,
		order_by: &OrderBy,
		scope: Option<&AggregateOutputCatalog>,
	) -> Result<Vec<SortExpr>, FrontendError> {
		if order_by.interpolate.is_some() {
			return Err(unsupported("ORDER BY INTERPOLATE is not supported"));
		}
		let exprs = match &order_by.kind {
			OrderByKind::Expressions(exprs) => exprs,
			OrderByKind::All(_) => return Err(unsupported("ORDER BY ALL is not supported")),
		};
		if exprs.is_empty() {
			return Err(unsupported("ORDER BY requires at least one expression"));
		}

		exprs
			.iter()
			.map(|expr| {
				if expr.with_fill.is_some() {
					return Err(unsupported("ORDER BY WITH FILL is not supported"));
				}
				let sql_expr = if let Some(scope) = scope {
					self.rewrite_grouped_aggregate_calls(&expr.expr, scope, "ORDER BY")?
				} else {
					expr.expr.clone()
				};
				Ok(SortExpr {
					expr: self.lower_expr(&sql_expr)?,
					descending: expr.options.asc == Some(false),
					nulls_first: expr.options.nulls_first.unwrap_or(false),
				})
			})
			.collect()
	}

	fn lower_join_operator(
		&self,
		operator: &JoinOperator,
	) -> Result<(JoinKind, Vec<(Expr, Expr)>), FrontendError> {
		let (kind, constraint) = match operator {
			JoinOperator::Join(constraint) | JoinOperator::Inner(constraint) => {
				(JoinKind::Inner, constraint)
			}
			JoinOperator::Left(constraint) | JoinOperator::LeftOuter(constraint) => {
				(JoinKind::LeftOuter, constraint)
			}
			JoinOperator::Right(constraint) | JoinOperator::RightOuter(constraint) => {
				(JoinKind::RightOuter, constraint)
			}
			JoinOperator::FullOuter(constraint) => (JoinKind::FullOuter, constraint),
			JoinOperator::Semi(constraint) | JoinOperator::LeftSemi(constraint) => {
				(JoinKind::LeftSemi, constraint)
			}
			JoinOperator::RightSemi(constraint) => (JoinKind::RightSemi, constraint),
			JoinOperator::Anti(constraint) | JoinOperator::LeftAnti(constraint) => {
				(JoinKind::LeftAnti, constraint)
			}
			JoinOperator::RightAnti(constraint) => (JoinKind::RightAnti, constraint),
			// CROSS JOIN lowers to a keyless cartesian (ADR 239). A bare `CROSS JOIN`
			// carries `JoinConstraint::None`; `ON`/`USING`/`NATURAL` on a cross join is
			// not valid and falls through to the reject arm below.
			JoinOperator::CrossJoin(JoinConstraint::None) => {
				return Ok((JoinKind::Cross, Vec::new()));
			}
			JoinOperator::CrossJoin(_)
			| JoinOperator::CrossApply
			| JoinOperator::OuterApply
			| JoinOperator::AsOf { .. }
			| JoinOperator::StraightJoin(_)
			| JoinOperator::ArrayJoin
			| JoinOperator::LeftArrayJoin
			| JoinOperator::InnerArrayJoin => {
				return Err(unsupported(
					"only INNER/OUTER/SEMI/ANTI JOIN ... ON and CROSS JOIN are supported",
				));
			}
		};

		match constraint {
			JoinConstraint::On(expr) => Ok((kind, self.lower_join_on(expr)?)),
			_ => Err(unsupported("JOIN requires ON equality predicates")),
		}
	}

	fn lower_arg(&self, arg: &FunctionArg) -> Result<Expr, FrontendError> {
		match arg {
			FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(e)) => self.lower_expr(e),
			_ => Err(unsupported("unsupported function argument")),
		}
	}

	fn lower_aggregate_projection(
		&self,
		projection: &[SelectItem],
		having: Option<&SqlExpr>,
		group_keys: &[GroupKey],
		mode: AggregateSqlMode,
	) -> Result<AggregateProjection, FrontendError> {
		if matches!(mode, AggregateSqlMode::Global) {
			reject_global_aggregate_projection_columns(projection)?;
		}
		let mut catalog = AggregateOutputCatalog::new(group_keys);
		for item in projection {
			self.discover_select_aggregates(item, mode, &mut catalog)?;
		}
		if let Some(having) = having {
			self.discover_aggregate_calls(having, mode, AggregateSource::Having, &mut catalog)?;
		}
		catalog.reject_having_alias_references(having)?;
		let distinct_rewrite = build_distinct_aggregate_rewrite(&catalog)?;

		let final_projection = self.lower_final_aggregate_projection(projection, mode, &catalog)?;
		let project_required = final_projection.required;
		// ADR 240: when a distinct rewrite is present it builds its own fan-out plan, so this flat
		// `aggr_expr` is consumed only on the non-rewrite path (and the `aggr_expr.is_empty()` guard).
		let aggr_expr = catalog.aggr_expr();
		let final_output_names = if project_required {
			final_projection.derived_output_names()
		} else {
			catalog.natural_output_names()
		};
		catalog.set_final_output_names(final_output_names);

		Ok(AggregateProjection {
			aggr_expr,
			catalog,
			final_projection: project_required.then_some(FinalProjection {
				exprs: final_projection.exprs,
				output_names: final_projection.output_names,
			}),
			distinct_rewrite,
		})
	}

	fn lower_having_predicate(
		&self,
		having: &SqlExpr,
		scope: &AggregateOutputCatalog,
	) -> Result<Expr, FrontendError> {
		let rewritten = self.rewrite_grouped_aggregate_calls(having, scope, "HAVING")?;
		self.lower_expr(&rewritten)
	}

	fn rewrite_grouped_aggregate_calls(
		&self,
		expr: &SqlExpr,
		scope: &AggregateOutputCatalog,
		clause: &'static str,
	) -> Result<SqlExpr, FrontendError> {
		match expr {
			SqlExpr::Function(function) if aggregate_function_name(&function.name).is_some() => {
				let output_name =
					self.resolve_grouped_aggregate_reference(function, scope, clause)?;
				Ok(SqlExpr::Identifier(Ident::new(output_name)))
			}
			SqlExpr::Function(function)
				if conditional_sugar_function_name(&function.name).is_some() =>
			{
				let mut rewritten = function.clone();
				rewritten.args = rewrite_function_argument_exprs(&function.args, |expr| {
					self.rewrite_grouped_aggregate_calls(expr, scope, clause)
				})?;
				Ok(SqlExpr::Function(rewritten))
			}
			SqlExpr::BinaryOp { left, op, right } => Ok(SqlExpr::BinaryOp {
				left: Box::new(self.rewrite_grouped_aggregate_calls(left, scope, clause)?),
				op: op.clone(),
				right: Box::new(self.rewrite_grouped_aggregate_calls(right, scope, clause)?),
			}),
			SqlExpr::Case {
				case_token,
				end_token,
				operand,
				conditions,
				else_result,
			} => Ok(SqlExpr::Case {
				case_token: case_token.clone(),
				end_token: end_token.clone(),
				operand: operand
					.as_ref()
					.map(|expr| self.rewrite_grouped_aggregate_calls(expr, scope, clause))
					.transpose()?
					.map(Box::new),
				conditions: conditions
					.iter()
					.map(|case_when| {
						Ok(CaseWhen {
							condition: self.rewrite_grouped_aggregate_calls(
								&case_when.condition,
								scope,
								clause,
							)?,
							result: self.rewrite_grouped_aggregate_calls(
								&case_when.result,
								scope,
								clause,
							)?,
						})
					})
					.collect::<Result<Vec<_>, FrontendError>>()?,
				else_result: else_result
					.as_ref()
					.map(|expr| self.rewrite_grouped_aggregate_calls(expr, scope, clause))
					.transpose()?
					.map(Box::new),
			}),
			SqlExpr::Cast {
				kind,
				expr,
				data_type,
				array,
				format,
			} => {
				if *array {
					return Err(unsupported("CAST AS ARRAY is not supported"));
				}
				Ok(SqlExpr::Cast {
					kind: kind.clone(),
					expr: Box::new(self.rewrite_grouped_aggregate_calls(expr, scope, clause)?),
					data_type: data_type.clone(),
					array: *array,
					format: format.clone(),
				})
			}
			SqlExpr::IsNull(inner) => Ok(SqlExpr::IsNull(Box::new(
				self.rewrite_grouped_aggregate_calls(inner, scope, clause)?,
			))),
			SqlExpr::IsNotNull(inner) => Ok(SqlExpr::IsNotNull(Box::new(
				self.rewrite_grouped_aggregate_calls(inner, scope, clause)?,
			))),
			SqlExpr::Nested(inner) => Ok(SqlExpr::Nested(Box::new(
				self.rewrite_grouped_aggregate_calls(inner, scope, clause)?,
			))),
			_ => Ok(expr.clone()),
		}
	}

	fn resolve_grouped_aggregate_reference(
		&self,
		function: &Function,
		scope: &AggregateOutputCatalog,
		clause: &'static str,
	) -> Result<String, FrontendError> {
		let call = self.lower_aggregate_function(function, AggregateSqlMode::Grouped)?;
		let signature = grouped_aggregate_signature(&call.function, &call.args, call.distinct)
			.ok_or_else(|| unsupported(aggregate_call_not_in_select_message(clause)))?;
		let outcome = match clause {
			"HAVING" => scope.resolve_for_having(&signature),
			"ORDER BY" => scope.resolve_for_order_by(&signature),
			_ => unreachable!("unsupported grouped aggregate clause: {clause}"),
		};
		match outcome {
			ResolveOutcome::Found(output_name) => Ok(output_name),
			ResolveOutcome::NotInSelect => {
				Err(unsupported(aggregate_call_not_in_select_message(clause)))
			}
			ResolveOutcome::Ambiguous => Err(unsupported(ambiguous_aggregate_call_message(clause))),
		}
	}

	fn discover_select_aggregates(
		&self,
		item: &SelectItem,
		mode: AggregateSqlMode,
		catalog: &mut AggregateOutputCatalog,
	) -> Result<(), FrontendError> {
		let (expr, alias) = select_item_expr_and_alias(item)?;
		if let Some(alias) = alias {
			catalog.add_select_alias(alias);
		}
		if let SqlExpr::Function(function) = strip_nested_sql_expr(expr)
			&& aggregate_function_name(&function.name).is_some()
		{
			let call = self.lower_aggregate_function(function, mode)?;
			let visible_name = alias
				.map(|alias| alias.value.clone())
				.unwrap_or_else(|| aggregate_output_name(&call.function).to_string());
			let index = catalog.add_requirement(call, AggregateSource::Select);
			catalog.mark_order_by_visible(index, visible_name);
			return Ok(());
		}
		self.discover_aggregate_calls(expr, mode, AggregateSource::Select, catalog)
	}

	fn discover_aggregate_calls(
		&self,
		expr: &SqlExpr,
		mode: AggregateSqlMode,
		source: AggregateSource,
		catalog: &mut AggregateOutputCatalog,
	) -> Result<(), FrontendError> {
		match strip_nested_sql_expr(expr) {
			SqlExpr::Function(function) if aggregate_function_name(&function.name).is_some() => {
				let call = self.lower_aggregate_function(function, mode)?;
				catalog.add_requirement(call, source);
				Ok(())
			}
			SqlExpr::Function(function) => {
				if let FunctionArguments::List(list) = &function.args {
					for arg in &list.args {
						match arg {
							FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
							| FunctionArg::Named {
								arg: FunctionArgExpr::Expr(expr),
								..
							}
							| FunctionArg::ExprNamed {
								arg: FunctionArgExpr::Expr(expr),
								..
							} => self.discover_aggregate_calls(expr, mode, source, catalog)?,
							FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => {}
							_ => {}
						}
					}
				}
				Ok(())
			}
			SqlExpr::BinaryOp { left, right, .. } => {
				self.discover_aggregate_calls(left, mode, source, catalog)?;
				self.discover_aggregate_calls(right, mode, source, catalog)
			}
			SqlExpr::Case {
				operand,
				conditions,
				else_result,
				..
			} => {
				if let Some(operand) = operand {
					self.discover_aggregate_calls(operand, mode, source, catalog)?;
				}
				for case_when in conditions {
					self.discover_aggregate_calls(&case_when.condition, mode, source, catalog)?;
					self.discover_aggregate_calls(&case_when.result, mode, source, catalog)?;
				}
				if let Some(else_result) = else_result {
					self.discover_aggregate_calls(else_result, mode, source, catalog)?;
				}
				Ok(())
			}
			SqlExpr::Cast { expr, .. } => {
				self.discover_aggregate_calls(expr, mode, source, catalog)
			}
			SqlExpr::Convert { expr, styles, .. } => {
				self.discover_aggregate_calls(expr, mode, source, catalog)?;
				for style in styles {
					self.discover_aggregate_calls(style, mode, source, catalog)?;
				}
				Ok(())
			}
			SqlExpr::UnaryOp { expr, .. }
			| SqlExpr::IsNull(expr)
			| SqlExpr::IsNotNull(expr)
			| SqlExpr::Nested(expr) => self.discover_aggregate_calls(expr, mode, source, catalog),
			_ => Ok(()),
		}
	}

	fn lower_final_aggregate_projection(
		&self,
		projection: &[SelectItem],
		mode: AggregateSqlMode,
		catalog: &AggregateOutputCatalog,
	) -> Result<LoweredAggregateProjection, FrontendError> {
		let has_alias = projection
			.iter()
			.map(select_item_expr_and_alias)
			.collect::<Result<Vec<_>, _>>()?
			.iter()
			.any(|(_, alias)| alias.is_some());
		let has_computed_expression = projection
			.iter()
			.map(select_item_expr_and_alias)
			.collect::<Result<Vec<_>, _>>()?
			.iter()
			.any(|(expr, _)| select_expr_is_computed(expr));
		let natural_projection =
			aggregate_projection_matches_natural(projection, mode, catalog, self)?;
		let has_having_only_aggregate = catalog
			.entries
			.iter()
			.any(|entry| entry.in_having && !entry.in_select);
		let required = has_alias
			|| has_computed_expression
			|| !natural_projection
			|| has_having_only_aggregate;

		if !required {
			return Ok(LoweredAggregateProjection {
				exprs: Vec::new(),
				output_names: Vec::new(),
				required,
			});
		}

		let mut exprs = Vec::with_capacity(projection.len());
		let mut output_names = Vec::with_capacity(projection.len());
		for item in projection {
			let (sql_expr, alias) = select_item_expr_and_alias(item)?;
			let rewritten = self.rewrite_aggregate_calls_for_projection(sql_expr, mode, catalog)?;
			exprs.push(self.lower_expr(&rewritten)?);
			output_names.push(alias.map(|alias| alias.value.clone()));
		}

		Ok(LoweredAggregateProjection {
			exprs,
			output_names,
			required,
		})
	}

	fn rewrite_aggregate_calls_for_projection(
		&self,
		expr: &SqlExpr,
		mode: AggregateSqlMode,
		catalog: &AggregateOutputCatalog,
	) -> Result<SqlExpr, FrontendError> {
		match expr {
			SqlExpr::Function(function) if aggregate_function_name(&function.name).is_some() => {
				let call = self.lower_aggregate_function(function, mode)?;
				let signature =
					grouped_aggregate_signature(&call.function, &call.args, call.distinct)
						.ok_or_else(|| unsupported("unsupported aggregate projection item"))?;
				match catalog.resolve_for_projection(&signature) {
					ResolveOutcome::Found(output_name) => {
						Ok(SqlExpr::Identifier(Ident::new(output_name)))
					}
					ResolveOutcome::NotInSelect => {
						Err(unsupported("unsupported aggregate projection item"))
					}
					ResolveOutcome::Ambiguous => Err(unsupported("ambiguous aggregate projection")),
				}
			}
			SqlExpr::Function(function)
				if conditional_sugar_function_name(&function.name).is_some() =>
			{
				let mut rewritten = function.clone();
				rewritten.args = rewrite_function_argument_exprs(&function.args, |expr| {
					self.rewrite_aggregate_calls_for_projection(expr, mode, catalog)
				})?;
				Ok(SqlExpr::Function(rewritten))
			}
			SqlExpr::Function(function) if function_name_eq(&function.name, "CONVERT") => {
				let mut rewritten = function.clone();
				rewritten.args = rewrite_function_argument_exprs(&function.args, |expr| {
					self.rewrite_aggregate_calls_for_projection(expr, mode, catalog)
				})?;
				Ok(SqlExpr::Function(rewritten))
			}
			SqlExpr::BinaryOp { left, op, right } => Ok(SqlExpr::BinaryOp {
				left: Box::new(self.rewrite_aggregate_calls_for_projection(left, mode, catalog)?),
				op: op.clone(),
				right: Box::new(self.rewrite_aggregate_calls_for_projection(right, mode, catalog)?),
			}),
			SqlExpr::Case {
				case_token,
				end_token,
				operand,
				conditions,
				else_result,
			} => Ok(SqlExpr::Case {
				case_token: case_token.clone(),
				end_token: end_token.clone(),
				operand: operand
					.as_ref()
					.map(|expr| self.rewrite_aggregate_calls_for_projection(expr, mode, catalog))
					.transpose()?
					.map(Box::new),
				conditions: conditions
					.iter()
					.map(|case_when| {
						Ok(CaseWhen {
							condition: self.rewrite_aggregate_calls_for_projection(
								&case_when.condition,
								mode,
								catalog,
							)?,
							result: self.rewrite_aggregate_calls_for_projection(
								&case_when.result,
								mode,
								catalog,
							)?,
						})
					})
					.collect::<Result<Vec<_>, FrontendError>>()?,
				else_result: else_result
					.as_ref()
					.map(|expr| self.rewrite_aggregate_calls_for_projection(expr, mode, catalog))
					.transpose()?
					.map(Box::new),
			}),
			SqlExpr::Cast {
				kind,
				expr,
				data_type,
				array,
				format,
			} => {
				if *array {
					return Err(unsupported("CAST AS ARRAY is not supported"));
				}
				Ok(SqlExpr::Cast {
					kind: kind.clone(),
					expr: Box::new(
						self.rewrite_aggregate_calls_for_projection(expr, mode, catalog)?,
					),
					data_type: data_type.clone(),
					array: *array,
					format: format.clone(),
				})
			}
			SqlExpr::Convert {
				is_try,
				expr,
				data_type,
				charset,
				target_before_value,
				styles,
			} => Ok(SqlExpr::Convert {
				is_try: *is_try,
				expr: Box::new(self.rewrite_aggregate_calls_for_projection(expr, mode, catalog)?),
				data_type: data_type.clone(),
				charset: charset.clone(),
				target_before_value: *target_before_value,
				styles: styles
					.iter()
					.map(|style| self.rewrite_aggregate_calls_for_projection(style, mode, catalog))
					.collect::<Result<Vec<_>, FrontendError>>()?,
			}),
			SqlExpr::IsNull(inner) => Ok(SqlExpr::IsNull(Box::new(
				self.rewrite_aggregate_calls_for_projection(inner, mode, catalog)?,
			))),
			SqlExpr::IsNotNull(inner) => Ok(SqlExpr::IsNotNull(Box::new(
				self.rewrite_aggregate_calls_for_projection(inner, mode, catalog)?,
			))),
			SqlExpr::Nested(inner) => Ok(SqlExpr::Nested(Box::new(
				self.rewrite_aggregate_calls_for_projection(inner, mode, catalog)?,
			))),
			_ => Ok(expr.clone()),
		}
	}

	fn lower_aggregate_function(
		&self,
		function: &Function,
		mode: AggregateSqlMode,
	) -> Result<LoweredAggregateCall, FrontendError> {
		let name = aggregate_function_name(&function.name)
			.ok_or_else(|| unsupported("unsupported aggregate function"))?;
		let AggregateFunctionArgs { args, distinct } = aggregate_function_args(function)?;

		let (function, args) = match name {
			"COUNT" => self.lower_count_aggregate(args),
			"SUM" => self.lower_sum_aggregate(args),
			"MIN" => self.lower_min_aggregate(args),
			"MAX" => self.lower_max_aggregate(args),
			"AVG" => self.lower_avg_aggregate(args),
			"LINEAR_REGRESS" => {
				if matches!(mode, AggregateSqlMode::Grouped) {
					return Err(unsupported(
						"experimental aggregate functions are not supported with GROUP BY",
					));
				}
				self.lower_linear_regress_aggregate(args)
			}
			"APPROX_QUANTILE" => {
				if matches!(mode, AggregateSqlMode::Grouped) {
					return Err(unsupported(
						"experimental aggregate functions are not supported with GROUP BY",
					));
				}
				self.lower_approx_quantile_aggregate(args)
			}
			_ => Err(unsupported("unsupported aggregate function")),
		}?;
		Ok(LoweredAggregateCall {
			function,
			args,
			distinct,
		})
	}

	fn lower_count_aggregate(
		&self,
		args: &[FunctionArg],
	) -> Result<(AggregateFunction, Vec<Expr>), FrontendError> {
		if args.len() != 1 {
			return Err(unsupported("COUNT requires one argument"));
		}
		match &args[0] {
			FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => {
				Ok((AggregateFunction::CountAll, vec![Expr::column("count")]))
			}
			FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
				validate_aggregate_argument(expr)?;
				Ok((AggregateFunction::Count, vec![self.lower_expr(expr)?]))
			}
			_ => Err(unsupported(AGGREGATE_ARGUMENT_SHAPE_MESSAGE)),
		}
	}

	fn lower_sum_aggregate(
		&self,
		args: &[FunctionArg],
	) -> Result<(AggregateFunction, Vec<Expr>), FrontendError> {
		if args.len() != 1 {
			return Err(unsupported("SUM requires one argument"));
		}
		validate_simple_aggregate_arg(&args[0])?;
		Ok((AggregateFunction::Sum, vec![self.lower_arg(&args[0])?]))
	}

	fn lower_min_aggregate(
		&self,
		args: &[FunctionArg],
	) -> Result<(AggregateFunction, Vec<Expr>), FrontendError> {
		if args.len() != 1 {
			return Err(unsupported("MIN requires one argument"));
		}
		validate_simple_aggregate_arg(&args[0])?;
		Ok((AggregateFunction::Min, vec![self.lower_arg(&args[0])?]))
	}

	fn lower_max_aggregate(
		&self,
		args: &[FunctionArg],
	) -> Result<(AggregateFunction, Vec<Expr>), FrontendError> {
		if args.len() != 1 {
			return Err(unsupported("MAX requires one argument"));
		}
		validate_simple_aggregate_arg(&args[0])?;
		Ok((AggregateFunction::Max, vec![self.lower_arg(&args[0])?]))
	}

	fn lower_avg_aggregate(
		&self,
		args: &[FunctionArg],
	) -> Result<(AggregateFunction, Vec<Expr>), FrontendError> {
		if args.len() != 1 {
			return Err(unsupported("AVG requires one argument"));
		}
		validate_simple_aggregate_arg(&args[0])?;
		Ok((AggregateFunction::Avg, vec![self.lower_arg(&args[0])?]))
	}

	fn lower_linear_regress_aggregate(
		&self,
		args: &[FunctionArg],
	) -> Result<(AggregateFunction, Vec<Expr>), FrontendError> {
		if args.len() != 2 {
			return Err(unsupported("LINEAR_REGRESS requires two arguments"));
		}
		Ok((
			AggregateFunction::LinearRegress,
			vec![self.lower_arg(&args[0])?, self.lower_arg(&args[1])?],
		))
	}

	fn lower_approx_quantile_aggregate(
		&self,
		args: &[FunctionArg],
	) -> Result<(AggregateFunction, Vec<Expr>), FrontendError> {
		if args.len() != 2 {
			return Err(unsupported("APPROX_QUANTILE requires two arguments"));
		}
		let val = self.lower_arg(&args[0])?;
		let q = match &args[1] {
			FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => match sql_value(expr) {
				Some(Value::Number(n, _)) => {
					n.parse::<f64>().map_err(|e| unsupported(e.to_string()))?
				}
				_ => {
					return Err(unsupported(
						"APPROX_QUANTILE expects a numeric literal for quantile",
					));
				}
			},
			_ => {
				return Err(unsupported(
					"APPROX_QUANTILE expects a numeric literal for quantile",
				));
			}
		};
		Ok((AggregateFunction::ApproxQuantile(q), vec![val]))
	}

	fn lower_derived_table(
		&self,
		subquery: &Query,
		alias: &TableAlias,
	) -> Result<LogicalPlan, FrontendError> {
		let select = validate_derived_query_shape(subquery)?;
		validate_derived_select_shape(select)?;
		let LoweredSelect {
			plan,
			aggregate_output_catalog,
		} = self.lower_select(select)?;
		let plan = self.apply_order_by_limit_tail(
			plan,
			aggregate_output_catalog.as_ref(),
			subquery.order_by.as_ref(),
			subquery.limit_clause.as_ref(),
		)?;
		alias_plan(plan, Some(alias))
	}

	fn lower_table_factor(&self, factor: &TableFactor) -> Result<LogicalPlan, FrontendError> {
		match factor {
			TableFactor::Table {
				name,
				alias,
				args,
				with_hints,
				index_hints,
				version,
				with_ordinality,
				partitions,
				json_path,
				sample,
			} => {
				if args.is_some() {
					return Err(unsupported("unsupported table factor: table arguments"));
				}
				if !with_hints.is_empty() {
					return Err(unsupported("unsupported table factor: table hints"));
				}
				if !index_hints.is_empty() {
					return Err(unsupported("unsupported table factor: index hints"));
				}
				if version.is_some() {
					return Err(unsupported("unsupported table factor: table version"));
				}
				if *with_ordinality {
					return Err(unsupported("unsupported table factor: WITH ORDINALITY"));
				}
				if !partitions.is_empty() {
					return Err(unsupported("unsupported table factor: partitions"));
				}
				if json_path.is_some() {
					return Err(unsupported("unsupported table factor: json path"));
				}
				if sample.is_some() {
					return Err(unsupported("unsupported table factor: sample"));
				}
				let table = lower_table_ref_object_name(name, "SELECT")?;
				let plan = LogicalPlan::ScanNamed {
					table,
					projection: None,
				};
				alias_plan(plan, alias.as_ref())
			}
			TableFactor::Derived {
				lateral,
				subquery,
				alias,
				sample,
			} => {
				if *lateral {
					return Err(unsupported("lateral derived tables are not supported"));
				}
				if sample.is_some() {
					return Err(unsupported("sampled derived tables are not supported"));
				}
				let alias = alias
					.as_ref()
					.ok_or_else(|| unsupported("derived tables require an alias"))?;
				if alias.at.is_some() {
					return Err(unsupported("derived table alias AT is not supported"));
				}
				self.lower_derived_table(subquery, alias)
			}
			_ => Err(unsupported("unsupported table factor")),
		}
	}

	fn push_side_local_join_filters<'a>(
		&self,
		input: LogicalPlan,
		pending: &mut Vec<&'a SqlExpr>,
		left_qualifiers: &[TableRef],
		right_qualifiers: &[TableRef],
		target: JoinFilterTarget,
	) -> Result<LogicalPlan, FrontendError> {
		let mut pushed = Vec::new();
		let mut residual = Vec::with_capacity(pending.len());
		for conjunct in pending.drain(..) {
			if join_filter_conjunct_matches(conjunct, left_qualifiers, right_qualifiers, target) {
				let predicate = self.lower_expr(conjunct)?;
				pushed.push(predicate);
			} else {
				residual.push(conjunct);
			}
		}
		*pending = residual;

		let Some(predicate) = combine_and_exprs(pushed) else {
			return Ok(input);
		};
		Ok(LogicalPlan::Filter {
			input: Box::new(input),
			predicate,
		})
	}

	fn lower_where(
		&self,
		mut plan: LogicalPlan,
		selection: &SqlExpr,
	) -> Result<LogicalPlan, FrontendError> {
		let mut residuals = Vec::new();
		for conjunct in split_top_level_and(selection) {
			match strip_nested_sql_expr(conjunct) {
				SqlExpr::Exists { subquery, negated } => {
					plan = self.lower_exists_join(plan, subquery, *negated)?;
				}
				_ => residuals.push(self.lower_expr(conjunct)?),
			}
		}

		if let Some(predicate) = combine_and_exprs(residuals) {
			plan = LogicalPlan::Filter {
				input: Box::new(plan),
				predicate,
			};
		}

		Ok(plan)
	}

	fn lower_exists_join(
		&self,
		outer_plan: LogicalPlan,
		subquery: &Query,
		negated: bool,
	) -> Result<LogicalPlan, FrontendError> {
		validate_exists_query_shape(subquery)?;
		let SetExpr::Select(select) = &*subquery.body else {
			return Err(unsupported("unsupported EXISTS subquery body"));
		};
		validate_exists_select_shape(select)?;
		validate_exists_projection(&select.projection)?;

		let table_with_joins = select
			.from
			.first()
			.expect("exists select shape has one FROM item");
		let mut inner_plan = self.lower_table_factor(&table_with_joins.relation)?;
		let outer_qualifiers = plan_qualifiers(&outer_plan);
		let inner_qualifiers = plan_qualifiers(&inner_plan);
		let mut on = Vec::new();
		let mut inner_residuals = Vec::new();

		if let Some(selection) = &select.selection {
			for conjunct in split_top_level_and(selection) {
				match self.classify_exists_conjunct(
					conjunct,
					&outer_qualifiers,
					&inner_qualifiers,
				)? {
					ExistsConjunct::CorrelatedPair { outer, inner } => on.push((outer, inner)),
					ExistsConjunct::InnerResidual(predicate) => inner_residuals.push(predicate),
				}
			}
		}

		if on.is_empty() {
			return Err(unsupported("uncorrelated EXISTS is not supported"));
		}

		if let Some(predicate) = combine_and_exprs(inner_residuals) {
			inner_plan = LogicalPlan::Filter {
				input: Box::new(inner_plan),
				predicate,
			};
		}

		build_join_plan(
			outer_plan,
			inner_plan,
			if negated {
				JoinKind::LeftAnti
			} else {
				JoinKind::LeftSemi
			},
			on,
			JoinStrategy::HashPartitioned,
			JoinStrategyPinSource::Direct,
			None,
		)
	}

	fn classify_exists_conjunct(
		&self,
		conjunct: &SqlExpr,
		outer_qualifiers: &[TableRef],
		inner_qualifiers: &[TableRef],
	) -> Result<ExistsConjunct, FrontendError> {
		let conjunct = strip_nested_sql_expr(conjunct);
		if let SqlExpr::BinaryOp { left, op, right } = conjunct
			&& *op == BinaryOperator::Eq
		{
			let left_column = scoped_column_expr(left, outer_qualifiers, inner_qualifiers)?;
			let right_column = scoped_column_expr(right, outer_qualifiers, inner_qualifiers)?;
			match (left_column, right_column) {
				(Some((ColumnScope::Outer, outer)), Some((ColumnScope::Inner, inner)))
				| (Some((ColumnScope::Inner, inner)), Some((ColumnScope::Outer, outer))) => {
					return Ok(ExistsConjunct::CorrelatedPair { outer, inner });
				}
				_ => {}
			}
		}

		let mut scope_refs = ScopeRefs::default();
		collect_scope_refs(
			conjunct,
			outer_qualifiers,
			inner_qualifiers,
			&mut scope_refs,
		)?;
		if scope_refs.outer || scope_refs.unsupported {
			return Err(unsupported("unsupported EXISTS subquery predicate"));
		}

		Ok(ExistsConjunct::InnerResidual(self.lower_expr(conjunct)?))
	}

	fn lower_join_on(&self, expr: &SqlExpr) -> Result<Vec<(Expr, Expr)>, FrontendError> {
		match expr {
			SqlExpr::BinaryOp { left, op, right } if *op == BinaryOperator::And => {
				let mut pairs = self.lower_join_on(left)?;
				pairs.extend(self.lower_join_on(right)?);
				Ok(pairs)
			}
			SqlExpr::BinaryOp { left, op, right } if *op == BinaryOperator::Eq => {
				Ok(vec![(self.lower_expr(left)?, self.lower_expr(right)?)])
			}
			_ => Err(unsupported("join ON only supports equality predicates")),
		}
	}

	fn lower_expr(&self, sql_expr: &SqlExpr) -> Result<Expr, FrontendError> {
		match sql_expr {
			SqlExpr::Identifier(ident) => Ok(Expr::column(ident.value.clone())),
			SqlExpr::CompoundIdentifier(idents) if idents.len() == 2 => Ok(Expr::qualified_column(
				TableRef::new(idents[0].value.clone()),
				idents[1].value.clone(),
			)),
			SqlExpr::Value(val) => match &val.value {
				Value::Number(n, _) => {
					if let Ok(i) = n.parse::<i32>() {
						Ok(Expr::Literal(ScalarValue::Int32(Some(i))))
					} else if let Ok(f) = n.parse::<f64>() {
						Ok(Expr::Literal(ScalarValue::Float64(Some(f))))
					} else {
						Err(unsupported(format!("unsupported number literal: {n}")))
					}
				}
				Value::SingleQuotedString(s) => {
					Ok(Expr::Literal(ScalarValue::Utf8(Some(s.clone()))))
				}
				Value::Boolean(b) => Ok(Expr::Literal(ScalarValue::Boolean(Some(*b)))),
				Value::Null => Ok(Expr::UntypedNull),
				_ => Err(unsupported(format!("unsupported value: {val:?}"))),
			},
			SqlExpr::TypedString(typed) if is_date_sql_type(&typed.data_type) => {
				let value = typed_string_value(typed)?;
				parse_date_ymd(&value)
					.map(|value| Expr::Literal(ScalarValue::Date32(Some(value))))
					.map_err(|error| unsupported(error.to_string()))
			}
			SqlExpr::TypedString(typed) => {
				let Some(unit) = timestamptz_sql_unit(&typed.data_type)? else {
					return Err(unsupported(format!("unsupported typed literal: {typed:?}")));
				};
				let value = typed_string_value(typed)?;
				parse_explicit_timestamptz(&value, unit)
					.map(|value| {
						Expr::Literal(ScalarValue::Timestamp {
							value: Some(value),
							unit,
						})
					})
					.map_err(|error| unsupported(error.to_string()))
			}
			SqlExpr::Interval(interval) => lower_interval_literal(interval),
			SqlExpr::BinaryOp { left, op, right } => {
				let l = Box::new(self.lower_expr(left)?);
				let r = Box::new(self.lower_expr(right)?);
				match op {
					BinaryOperator::Eq => Ok(Expr::Eq(l, r)),
					BinaryOperator::NotEq => Ok(Expr::Neq(l, r)),
					BinaryOperator::Lt => Ok(Expr::Lt(l, r)),
					BinaryOperator::LtEq => Ok(Expr::Lte(l, r)),
					BinaryOperator::Gt => Ok(Expr::Gt(l, r)),
					BinaryOperator::GtEq => Ok(Expr::Gte(l, r)),
					BinaryOperator::And => Ok(Expr::And(l, r)),
					BinaryOperator::Or => Ok(Expr::Or(l, r)),
					BinaryOperator::Plus => Ok(Expr::Add(l, r)),
					BinaryOperator::Minus => Ok(Expr::Sub(l, r)),
					BinaryOperator::Multiply => Ok(Expr::Mul(l, r)),
					BinaryOperator::Divide => Ok(Expr::Div(l, r)),
					BinaryOperator::PGLikeMatch
					| BinaryOperator::PGILikeMatch
					| BinaryOperator::PGNotLikeMatch
					| BinaryOperator::PGNotILikeMatch => Err(unsupported(
						"PostgreSQL LIKE operator spellings are not supported",
					)),
					_ => Err(unsupported(format!("unsupported operator: {op:?}"))),
				}
			}
			SqlExpr::InList {
				expr,
				list,
				negated,
			} => self.lower_in_list(expr, list, *negated),
			SqlExpr::Like {
				negated,
				any,
				expr,
				pattern,
				escape_char,
			} => self.lower_like(expr, pattern, *negated, false, *any, escape_char.as_ref()),
			SqlExpr::ILike {
				negated,
				any,
				expr,
				pattern,
				escape_char,
			} => self.lower_like(expr, pattern, *negated, true, *any, escape_char.as_ref()),
			SqlExpr::Between {
				expr,
				negated,
				low,
				high,
			} => self.lower_between(expr, low, high, *negated),
			SqlExpr::Case {
				operand,
				conditions,
				else_result,
				..
			} => self.lower_case(operand.as_deref(), conditions, else_result.as_deref()),
			SqlExpr::InSubquery { .. } => Err(unsupported("IN subquery is not supported")),
			SqlExpr::InUnnest { .. } => Err(unsupported("IN UNNEST is not supported")),
			SqlExpr::SimilarTo { .. } => Err(unsupported("SIMILAR TO is not supported")),
			SqlExpr::RLike { .. } => Err(unsupported("RLIKE is not supported")),
			SqlExpr::Convert {
				is_try,
				expr,
				data_type,
				charset,
				target_before_value,
				styles,
			} => self.lower_convert_ast(
				*is_try,
				expr,
				data_type.as_ref(),
				charset.as_ref(),
				*target_before_value,
				styles,
			),
			SqlExpr::Cast {
				kind,
				expr,
				data_type,
				array,
				format,
			} => {
				if *array {
					return Err(unsupported("CAST AS ARRAY is not supported"));
				}
				if format.is_some() {
					return Err(unsupported("CAST FORMAT clause is not supported"));
				}
				let target = lower_cast_target(data_type)?;
				match (kind, target) {
					(CastKind::Cast, LoweredCastTarget::Physical(data_type)) => {
						Ok(Expr::Cast(Box::new(self.lower_expr(expr)?), data_type))
					}
					(
						CastKind::TryCast | CastKind::SafeCast,
						LoweredCastTarget::Physical(data_type),
					) => Ok(Expr::TryCast(Box::new(self.lower_expr(expr)?), data_type)),
					(CastKind::Cast, LoweredCastTarget::LogicalName(logical_name)) => {
						Ok(Expr::SqlLogicalCast {
							expr: Box::new(self.lower_expr(expr)?),
							logical_name,
							try_cast: false,
						})
					}
					(
						CastKind::TryCast | CastKind::SafeCast,
						LoweredCastTarget::LogicalName(logical_name),
					) => Ok(Expr::SqlLogicalCast {
						expr: Box::new(self.lower_expr(expr)?),
						logical_name,
						try_cast: true,
					}),
					(CastKind::DoubleColon, _) => Err(unsupported(
						"unsupported cast syntax: ::; only CAST(... AS ...) is supported",
					)),
				}
			}
			SqlExpr::IsNull(expr) => Ok(Expr::IsNull(Box::new(self.lower_expr(expr)?))),
			SqlExpr::IsNotNull(expr) => Ok(Expr::IsNotNull(Box::new(self.lower_expr(expr)?))),
			SqlExpr::Extract { field, expr, .. } => Ok(Expr::Extract(
				lower_extract_datetime_field(field)?,
				Box::new(self.lower_expr(expr)?),
			)),
			SqlExpr::AtTimeZone {
				timestamp,
				time_zone,
			} => Ok(Expr::AtTimeZone(
				Box::new(self.lower_expr(timestamp)?),
				sql_string_literal_expr(
					time_zone,
					"AT TIME ZONE expects a string literal timezone",
				)?,
			)),
			SqlExpr::Nested(expr) => self.lower_expr(expr),
			SqlExpr::Function(f) => match conditional_sugar_function_name(&f.name) {
				Some(ConditionalSugarFunction::Coalesce) => self.lower_coalesce(f),
				Some(ConditionalSugarFunction::If) | Some(ConditionalSugarFunction::Iif) => {
					self.lower_if(f)
				}
				Some(ConditionalSugarFunction::Nullif) => self.lower_nullif(f),
				Some(ConditionalSugarFunction::Ifnull) | Some(ConditionalSugarFunction::Nvl) => {
					self.lower_ifnull_nvl(f)
				}
				Some(ConditionalSugarFunction::Nvl2) => self.lower_nvl2(f),
				None if function_name_eq(&f.name, "CONVERT") => self.lower_convert(f),
				None if function_name_eq(&f.name, "DATE_PART") => {
					let args = function_arg_list(&f.args, "date_part expects arguments")?;
					if args.len() != 2 {
						return Err(unsupported("date_part expects exactly two arguments"));
					}
					let field = string_literal_function_arg(
						&args[0],
						"date_part expects a string literal field",
					)?;
					let field = lower_temporal_extract_name(&field)?;
					let expr = self.lower_arg(&args[1])?;
					Ok(Expr::Extract(field, Box::new(expr)))
				}
				None if function_name_eq(&f.name, "DATE_TRUNC") => {
					let args = function_arg_list(&f.args, "date_trunc expects arguments")?;
					if args.len() != 2 {
						return Err(unsupported("date_trunc expects exactly two arguments"));
					}
					let field = string_literal_function_arg(
						&args[0],
						"date_trunc expects a string literal field",
					)?;
					let field = lower_temporal_trunc_name(&field)?;
					let expr = self.lower_arg(&args[1])?;
					Ok(Expr::DateTrunc(field, Box::new(expr)))
				}
				None if function_name_eq(&f.name, "TIME_BUCKET") => {
					let args = match &f.args {
						sqlparser::ast::FunctionArguments::List(l) => &l.args,
						_ => return Err(unsupported("TIME_BUCKET expects arguments")),
					};
					if args.len() != 2 {
						return Err(unsupported("TIME_BUCKET expects exactly two arguments"));
					}
					let ts = self.lower_arg(&args[0])?;
					let interval = match &args[1] {
						FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(expr)) => {
							match sql_value(expr) {
								Some(Value::SingleQuotedString(s)) => match s.as_str() {
									"1m" => std::time::Duration::from_secs(60),
									"1h" => std::time::Duration::from_secs(3600),
									"1d" => std::time::Duration::from_secs(86400),
									_ => {
										return Err(unsupported(format!(
											"unsupported time_bucket interval: {s}"
										)));
									}
								},
								_ => {
									return Err(unsupported(
										"time_bucket expects a string literal interval",
									));
								}
							}
						}
						_ => {
							return Err(unsupported(
								"time_bucket expects a string literal interval",
							));
						}
					};
					Ok(Expr::TimeBucket(interval, Box::new(ts)))
				}
				None => Err(unsupported(format!("unsupported expression: {sql_expr:?}"))),
			},
			_ => Err(unsupported(format!("unsupported expression: {sql_expr:?}"))),
		}
	}

	fn lower_if(&self, function: &Function) -> Result<Expr, FrontendError> {
		let args = function_positional_args(function, "IF/IIF")?;
		let [condition, truthy, falsy] = args.as_slice() else {
			return Err(unsupported("IF/IIF requires exactly three arguments"));
		};
		Ok(Expr::Case {
			when_then: vec![(
				Box::new(self.lower_expr(condition)?),
				Box::new(self.lower_expr(truthy)?),
			)],
			else_expr: Some(Box::new(self.lower_expr(falsy)?)),
		})
	}

	fn lower_convert(&self, function: &Function) -> Result<Expr, FrontendError> {
		let args = function_positional_args(function, "CONVERT")?;
		let [expr, target] = args.as_slice() else {
			return Err(unsupported("CONVERT requires exactly two arguments"));
		};
		Ok(Expr::SqlConvert {
			expr: Box::new(self.lower_expr(expr)?),
			target_logical_name: lower_convert_target_logical_name(target)?,
		})
	}

	fn lower_convert_ast(
		&self,
		is_try: bool,
		expr: &SqlExpr,
		data_type: Option<&SqlDataType>,
		charset: Option<&ObjectName>,
		target_before_value: bool,
		styles: &[SqlExpr],
	) -> Result<Expr, FrontendError> {
		if is_try {
			return Err(unsupported("TRY_CONVERT to logical units is not supported"));
		}
		if target_before_value {
			return Err(unsupported(
				"CONVERT target must follow the source expression",
			));
		}
		if charset.is_some() {
			return Err(unsupported("CONVERT CHARACTER SET is not supported"));
		}
		if !styles.is_empty() {
			return Err(unsupported("CONVERT style arguments are not supported"));
		}
		let data_type = data_type
			.ok_or_else(|| unsupported("CONVERT requires a target logical type identifier"))?;
		let target_logical_name = lower_convert_data_type_target_logical_name(data_type)?;
		Ok(Expr::SqlConvert {
			expr: Box::new(self.lower_expr(expr)?),
			target_logical_name,
		})
	}

	fn lower_coalesce(&self, function: &Function) -> Result<Expr, FrontendError> {
		let args = function_positional_args(function, "COALESCE")?;
		let (last, init) = args
			.split_last()
			.ok_or_else(|| unsupported("COALESCE requires at least one argument"))?;
		if init.is_empty() {
			return self.lower_expr(last);
		}

		let when_then = init
			.iter()
			.map(|arg| {
				let lowered = self.lower_expr(arg)?;
				Ok((
					Box::new(Expr::IsNotNull(Box::new(lowered.clone()))),
					Box::new(lowered),
				))
			})
			.collect::<Result<Vec<_>, FrontendError>>()?;
		Ok(Expr::Case {
			when_then,
			else_expr: Some(Box::new(self.lower_expr(last)?)),
		})
	}

	fn lower_ifnull_nvl(&self, function: &Function) -> Result<Expr, FrontendError> {
		let args = function_positional_args(function, "IFNULL/NVL")?;
		let [first, fallback] = args.as_slice() else {
			return Err(unsupported("IFNULL/NVL requires exactly two arguments"));
		};
		let first = self.lower_expr(first)?;
		Ok(Expr::Case {
			when_then: vec![(
				Box::new(Expr::IsNotNull(Box::new(first.clone()))),
				Box::new(first),
			)],
			else_expr: Some(Box::new(self.lower_expr(fallback)?)),
		})
	}

	fn lower_nullif(&self, function: &Function) -> Result<Expr, FrontendError> {
		let args = function_positional_args(function, "NULLIF")?;
		let [first, second] = args.as_slice() else {
			return Err(unsupported("NULLIF requires exactly two arguments"));
		};
		let first = self.lower_expr(first)?;
		Ok(Expr::Case {
			when_then: vec![(
				Box::new(Expr::Eq(
					Box::new(first.clone()),
					Box::new(self.lower_expr(second)?),
				)),
				Box::new(Expr::UntypedNull),
			)],
			else_expr: Some(Box::new(first)),
		})
	}

	fn lower_nvl2(&self, function: &Function) -> Result<Expr, FrontendError> {
		let args = function_positional_args(function, "NVL2")?;
		let [probe, when_not_null, when_null] = args.as_slice() else {
			return Err(unsupported("NVL2 requires exactly three arguments"));
		};
		let probe = self.lower_expr(probe)?;
		Ok(Expr::Case {
			when_then: vec![(
				Box::new(Expr::IsNotNull(Box::new(probe.clone()))),
				Box::new(self.lower_expr(when_not_null)?),
			)],
			else_expr: Some(Box::new(self.lower_expr(when_null)?)),
		})
	}

	fn lower_case(
		&self,
		operand: Option<&SqlExpr>,
		conditions: &[CaseWhen],
		else_result: Option<&SqlExpr>,
	) -> Result<Expr, FrontendError> {
		if conditions.is_empty() {
			return Err(unsupported("CASE must have at least one WHEN branch"));
		}
		let operand = operand.map(|expr| self.lower_expr(expr)).transpose()?;
		let mut when_then = Vec::with_capacity(conditions.len());
		for case_when in conditions {
			let condition = if let Some(operand) = &operand {
				Expr::Eq(
					Box::new(operand.clone()),
					Box::new(self.lower_expr(&case_when.condition)?),
				)
			} else {
				self.lower_expr(&case_when.condition)?
			};
			when_then.push((
				Box::new(condition),
				Box::new(self.lower_expr(&case_when.result)?),
			));
		}
		let else_expr = else_result
			.map(|expr| self.lower_expr(expr).map(Box::new))
			.transpose()?;
		Ok(Expr::Case {
			when_then,
			else_expr,
		})
	}

	fn lower_like(
		&self,
		expr: &SqlExpr,
		pattern: &SqlExpr,
		negated: bool,
		case_insensitive: bool,
		any: bool,
		escape_char: Option<&sqlparser::ast::ValueWithSpan>,
	) -> Result<Expr, FrontendError> {
		if any {
			return Err(unsupported("LIKE ANY is not supported"));
		}
		if escape_char.is_some() {
			return Err(unsupported("LIKE ESCAPE is not supported"));
		}
		if expr_is_null_literal(pattern) {
			return Err(unsupported("NULL LIKE patterns are not supported"));
		}
		let Some(Value::SingleQuotedString(pattern)) = stripped_sql_value(pattern) else {
			return Err(unsupported("LIKE pattern must be a string literal"));
		};
		Ok(Expr::Like {
			expr: Box::new(self.lower_expr(expr)?),
			pattern: pattern.clone(),
			negated,
			case_insensitive,
		})
	}

	fn lower_in_list(
		&self,
		expr: &SqlExpr,
		list: &[SqlExpr],
		negated: bool,
	) -> Result<Expr, FrontendError> {
		if list.is_empty() {
			return Err(unsupported("IN list must not be empty"));
		}
		if list.len() > MAX_IN_LIST_ITEMS {
			return Err(unsupported(format!(
				"IN list exceeds {MAX_IN_LIST_ITEMS} items"
			)));
		}
		if is_row_value_expr(expr) || list.iter().any(is_row_value_expr) {
			return Err(unsupported("row-value IN is not supported"));
		}
		Ok(Expr::InList {
			expr: Box::new(self.lower_expr(expr)?),
			list: list
				.iter()
				.map(|item| self.lower_expr(item))
				.collect::<Result<Vec<_>, _>>()?,
			negated,
		})
	}

	fn lower_between(
		&self,
		expr: &SqlExpr,
		low: &SqlExpr,
		high: &SqlExpr,
		negated: bool,
	) -> Result<Expr, FrontendError> {
		let expr = self.lower_expr(expr)?;
		let low = self.lower_expr(low)?;
		let high = self.lower_expr(high)?;
		if negated {
			Ok(Expr::Or(
				Box::new(Expr::Lt(Box::new(expr.clone()), Box::new(low))),
				Box::new(Expr::Gt(Box::new(expr), Box::new(high))),
			))
		} else {
			Ok(Expr::And(
				Box::new(Expr::Gte(Box::new(expr.clone()), Box::new(low))),
				Box::new(Expr::Lte(Box::new(expr), Box::new(high))),
			))
		}
	}
}

pub(crate) fn build_join_plan(
	left: LogicalPlan,
	right: LogicalPlan,
	kind: JoinKind,
	on: Vec<(Expr, Expr)>,
	strategy: JoinStrategy,
	source: JoinStrategyPinSource,
	build_side: Option<BroadcastBuildSide>,
) -> Result<LogicalPlan, FrontendError> {
	let (left_keys, right_keys) = derive_join_exchange_keys(&on, &left, &right)?;
	match strategy {
		JoinStrategy::HashPartitioned if build_side.is_some() => Err(unsupported(
			"hash join pins must not carry a broadcast build side",
		)),
		JoinStrategy::HashPartitioned => Ok(LogicalPlan::Join {
			left: Box::new(LogicalPlan::Exchange {
				input: Box::new(left),
				partition_count: DEFAULT_JOIN_PARTITION_COUNT,
				key: left_keys,
			}),
			right: Box::new(LogicalPlan::Exchange {
				input: Box::new(right),
				partition_count: DEFAULT_JOIN_PARTITION_COUNT,
				key: right_keys,
			}),
			kind,
			on,
			null_safe_keys: false,
			strategy_intent: JoinStrategyIntent::Pinned {
				strategy,
				source,
				build_side: None,
			},
		}),
		JoinStrategy::Broadcast => {
			let Some(build_side) = build_side else {
				return Err(unsupported(
					"broadcast join pins require an explicit build side",
				));
			};
			Ok(LogicalPlan::Join {
				left: Box::new(left),
				right: Box::new(right),
				kind,
				on,
				null_safe_keys: false,
				strategy_intent: JoinStrategyIntent::Pinned {
					strategy,
					source,
					build_side: Some(build_side),
				},
			})
		}
	}
}

pub(crate) fn build_auto_join_plan(
	left: LogicalPlan,
	right: LogicalPlan,
	kind: JoinKind,
	on: Vec<(Expr, Expr)>,
) -> LogicalPlan {
	LogicalPlan::Join {
		left: Box::new(left),
		right: Box::new(right),
		kind,
		on,
		null_safe_keys: false,
		strategy_intent: JoinStrategyIntent::Auto,
	}
}

pub(crate) fn expr_to_join_keys(expr: &Expr) -> Result<Vec<(Expr, Expr)>, FrontendError> {
	match expr {
		Expr::And(left, right) => {
			let mut keys = expr_to_join_keys(left)?;
			keys.extend(expr_to_join_keys(right)?);
			Ok(keys)
		}
		Expr::Eq(left, right) => Ok(vec![((*left.as_ref()).clone(), (*right.as_ref()).clone())]),
		_ => Err(unsupported(
			"JOIN predicate requires equality key predicates",
		)),
	}
}

fn derive_join_exchange_keys(
	on: &[(Expr, Expr)],
	left: &LogicalPlan,
	right: &LogicalPlan,
) -> Result<(Vec<Expr>, Vec<Expr>), FrontendError> {
	let left_qualifiers = plan_qualifiers(left);
	let right_qualifiers = plan_qualifiers(right);
	let mut left_keys = Vec::with_capacity(on.len());
	let mut right_keys = Vec::with_capacity(on.len());

	for (first, second) in on {
		let first_side = expr_side(first, &left_qualifiers, &right_qualifiers);
		let second_side = expr_side(second, &left_qualifiers, &right_qualifiers);
		match (first_side, second_side) {
			(Some(JoinSide::Left), Some(JoinSide::Right))
			| (Some(JoinSide::Left), None)
			| (None, Some(JoinSide::Right))
			| (None, None) => {
				left_keys.push(first.clone());
				right_keys.push(second.clone());
			}
			(Some(JoinSide::Right), Some(JoinSide::Left))
			| (Some(JoinSide::Right), None)
			| (None, Some(JoinSide::Left)) => {
				left_keys.push(second.clone());
				right_keys.push(first.clone());
			}
			_ => {
				return Err(unsupported(
					"join ON equality must reference one column from each side",
				));
			}
		}
	}

	Ok((left_keys, right_keys))
}

fn sort_needs_hidden_projection(order_by: &[SortExpr]) -> bool {
	order_by
		.iter()
		.any(|key| !sort_key_is_direct_output_reference(&key.expr))
}

fn sort_key_is_direct_output_reference(expr: &Expr) -> bool {
	matches!(expr, Expr::Column(_) | Expr::Literal(_) | Expr::UntypedNull)
		|| matches!(
			expr,
			Expr::LogicalAscription { expr, .. }
				| Expr::SqlLogicalCast { expr, .. }
				| Expr::SqlConvert { expr, .. }
				if sort_key_is_direct_output_reference(expr)
		)
}

fn validate_hidden_sort_key_refs(
	expr: &Expr,
	visible_names: &HashSet<String>,
) -> Result<(), FrontendError> {
	match expr {
		Expr::Column(column) => {
			if !visible_names.contains(&column.name.0) {
				return Err(unsupported(
					"ORDER BY computed aggregate expression must reference visible output columns",
				));
			}
			Ok(())
		}
		Expr::Literal(_) | Expr::UntypedNull => Ok(()),
		Expr::LogicalAscription { expr: inner, .. } => {
			validate_hidden_sort_key_refs(inner, visible_names)
		}
		Expr::SqlLogicalCast { expr: inner, .. }
		| Expr::SqlConvert { expr: inner, .. }
		| Expr::Cast(inner, _)
		| Expr::TryCast(inner, _) => validate_hidden_sort_key_refs(inner, visible_names),
		Expr::Add(left, right)
		| Expr::Sub(left, right)
		| Expr::Mul(left, right)
		| Expr::Div(left, right)
		| Expr::Eq(left, right)
		| Expr::Neq(left, right)
		| Expr::Lt(left, right)
		| Expr::Lte(left, right)
		| Expr::Gt(left, right)
		| Expr::Gte(left, right)
		| Expr::And(left, right)
		| Expr::Or(left, right) => {
			validate_hidden_sort_key_refs(left, visible_names)?;
			validate_hidden_sort_key_refs(right, visible_names)
		}
		Expr::Not(inner)
		| Expr::IsNull(inner)
		| Expr::IsNotNull(inner)
		| Expr::Like { expr: inner, .. }
		| Expr::TimeBucket(_, inner)
		| Expr::AtTimeZone(inner, _)
		| Expr::Extract(_, inner)
		| Expr::DateTrunc(_, inner) => validate_hidden_sort_key_refs(inner, visible_names),
		Expr::InList { expr, list, .. } => {
			validate_hidden_sort_key_refs(expr, visible_names)?;
			for item in list {
				validate_hidden_sort_key_refs(item, visible_names)?;
			}
			Ok(())
		}
		Expr::Case {
			when_then,
			else_expr,
		} => {
			for (condition, result) in when_then {
				validate_hidden_sort_key_refs(condition, visible_names)?;
				validate_hidden_sort_key_refs(result, visible_names)?;
			}
			if let Some(else_expr) = else_expr {
				validate_hidden_sort_key_refs(else_expr, visible_names)?;
			}
			Ok(())
		}
	}
}

fn add_hidden_sort_projection(
	plan: LogicalPlan,
	order_by: Vec<SortExpr>,
	catalog: Option<&AggregateOutputCatalog>,
) -> Result<(LogicalPlan, Vec<SortExpr>, Option<FinalProjection>), FrontendError> {
	let output_names = visible_output_names(&plan, catalog).ok_or_else(|| {
		unsupported("ORDER BY computed aggregate expression is not supported for this plan shape")
	})?;
	if has_duplicate_names(&output_names) {
		return Err(unsupported(
			"ORDER BY computed aggregate expression requires unique output names",
		));
	}

	let visible_names = output_names.iter().cloned().collect::<HashSet<_>>();
	let mut used_names = output_names.iter().cloned().collect::<HashSet<_>>();
	let mut projected_exprs = output_names
		.iter()
		.cloned()
		.map(Expr::column)
		.collect::<Vec<_>>();
	let mut projected_output_names = output_names
		.iter()
		.cloned()
		.map(Some)
		.collect::<Vec<Option<String>>>();
	let mut rewritten_order_by = Vec::with_capacity(order_by.len());

	for (idx, key) in order_by.into_iter().enumerate() {
		if sort_key_is_direct_output_reference(&key.expr) {
			rewritten_order_by.push(key);
			continue;
		}

		validate_hidden_sort_key_refs(&key.expr, &visible_names)?;
		let hidden_name = unique_hidden_sort_name(idx, &mut used_names);
		projected_exprs.push(key.expr);
		projected_output_names.push(Some(hidden_name.clone()));
		rewritten_order_by.push(SortExpr {
			expr: Expr::column(hidden_name),
			descending: key.descending,
			nulls_first: key.nulls_first,
		});
	}

	let final_projection = FinalProjection {
		exprs: output_names
			.iter()
			.cloned()
			.map(Expr::column)
			.collect::<Vec<_>>(),
		output_names: output_names.into_iter().map(Some).collect(),
	};
	Ok((
		LogicalPlan::Project {
			input: Box::new(plan),
			exprs: projected_exprs,
			output_names: projected_output_names,
		},
		rewritten_order_by,
		Some(final_projection),
	))
}

fn visible_output_names(
	plan: &LogicalPlan,
	catalog: Option<&AggregateOutputCatalog>,
) -> Option<Vec<String>> {
	match plan {
		LogicalPlan::Project {
			exprs,
			output_names,
			..
		} => Some(
			exprs
				.iter()
				.enumerate()
				.map(|(idx, expr)| {
					output_names[idx]
						.clone()
						.unwrap_or_else(|| logical_expr_output_name(expr, idx))
				})
				.collect(),
		),
		LogicalPlan::Filter { input, .. } => visible_output_names(input, catalog),
		LogicalPlan::Aggregate { .. } => catalog.map(AggregateOutputCatalog::natural_output_names),
		_ => None,
	}
}

fn has_duplicate_names(names: &[String]) -> bool {
	let mut seen = HashSet::new();
	names.iter().any(|name| !seen.insert(name))
}

fn unique_hidden_sort_name(idx: usize, used_names: &mut HashSet<String>) -> String {
	let mut candidate = format!("__toron_order_key_{idx}");
	let mut suffix = 0usize;
	while used_names.contains(&candidate) {
		suffix += 1;
		candidate = format!("__toron_order_key_{idx}_{suffix}");
	}
	used_names.insert(candidate.clone());
	candidate
}

#[derive(Debug, Clone)]
struct GroupKey {
	name: String,
	expr: Expr,
}

#[derive(Debug)]
struct LoweredSelect {
	plan: LogicalPlan,
	aggregate_output_catalog: Option<AggregateOutputCatalog>,
}

#[derive(Debug)]
struct AggregateProjection {
	aggr_expr: Vec<(AggregateFunction, Vec<Expr>)>,
	catalog: AggregateOutputCatalog,
	final_projection: Option<FinalProjection>,
	distinct_rewrite: Option<DistinctAggregateRewrite>,
}

#[derive(Debug)]
struct FinalProjection {
	exprs: Vec<Expr>,
	output_names: Vec<Option<String>>,
}

/// ADR 240 multi-dedup fan-out + recombine. The single-dedup case (ADR 234) is subsumed as one
/// dedup branch with no plain branch (a recombine of one = the branch itself). The builder emits a
/// plan whose output schema is exactly `[group keys..., catalog entries in order]` — identical to
/// what a single `Aggregate` would produce — so all downstream final-projection/having logic is
/// reused unchanged.
#[derive(Debug, Clone)]
struct DistinctAggregateRewrite {
	group_keys: Vec<GroupKey>,
	branches: Vec<RecombineBranch>,
	/// For each catalog entry (in catalog order): `(branch index, agg position within that branch)`.
	entry_slots: Vec<(usize, usize)>,
	/// `[group key names..., entry natural names...]` — the normalized output schema (catalog order).
	output_names: Vec<String>,
}

#[derive(Debug, Clone)]
struct RecombineBranch {
	/// `Some(arg)` => a dedup branch over that distinct argument; `None` => the plain (non-distinct)
	/// branch aggregated directly over the base input.
	dedup_arg: Option<Expr>,
	/// The aggregate calls computed by this branch, in slot order. A dedup branch applies each
	/// function to the deduped argument column; the plain branch applies each entry's own args.
	aggs: Vec<(AggregateFunction, Vec<Expr>)>,
}

impl DistinctAggregateRewrite {
	fn is_global(&self) -> bool {
		self.group_keys.is_empty()
	}

	/// Build the full fan-out + recombine plan from the already-lowered base input.
	fn into_aggregated_plan(&self, base: LogicalPlan) -> LogicalPlan {
		let branch_plans: Vec<LogicalPlan> = self
			.branches
			.iter()
			.enumerate()
			.map(|(index, branch)| self.build_branch(base.clone(), index, branch))
			.collect();

		let mut recombined = branch_plans
			.into_iter()
			.enumerate()
			.reduce(|(_, left), (right_index, right)| {
				(right_index, self.recombine(left, right, right_index))
			})
			.map(|(_, plan)| plan)
			.expect("a distinct-aggregate rewrite has at least one branch");

		// Normalize to `[group keys..., entries in catalog order]` so downstream logic is unchanged.
		let mut exprs = Vec::with_capacity(self.group_keys.len() + self.entry_slots.len());
		for key in 0..self.group_keys.len() {
			exprs.push(Expr::column(branch_group_key_name(0, key)));
		}
		for (branch, slot) in &self.entry_slots {
			exprs.push(Expr::column(branch_agg_name(*branch, *slot)));
		}
		recombined = LogicalPlan::Project {
			input: Box::new(recombined),
			exprs,
			output_names: self.output_names.iter().cloned().map(Some).collect(),
		};
		recombined
	}

	fn build_branch(
		&self,
		base: LogicalPlan,
		index: usize,
		branch: &RecombineBranch,
	) -> LogicalPlan {
		let global = self.is_global();
		// 1. Aggregate input + the (group key exprs, arg refs) the branch's aggregate references.
		let (agg_input, group_expr, arg_refs) = match &branch.dedup_arg {
			Some(arg) => {
				// Dedup branch: project `[group keys (named) + arg]`, then DISTINCT.
				let arg_name = branch_dedup_arg_name(index);
				let mut exprs: Vec<Expr> =
					self.group_keys.iter().map(|key| key.expr.clone()).collect();
				let mut names: Vec<Option<String>> = self
					.group_keys
					.iter()
					.map(|key| Some(key.name.clone()))
					.collect();
				exprs.push(arg.clone());
				names.push(Some(arg_name.clone()));
				let deduped = LogicalPlan::Distinct {
					input: Box::new(LogicalPlan::Project {
						input: Box::new(base),
						exprs,
						output_names: names,
					}),
				};
				let group_expr = self
					.group_keys
					.iter()
					.map(|key| Expr::column(key.name.clone()))
					.collect::<Vec<_>>();
				(deduped, group_expr, ArgRefs::Shared(arg_name))
			}
			None => {
				// Plain branch: aggregate directly over the base; group by the declared key exprs.
				let group_expr = self
					.group_keys
					.iter()
					.map(|key| key.expr.clone())
					.collect::<Vec<_>>();
				(base, group_expr, ArgRefs::PerEntry)
			}
		};

		let aggr_expr = branch
			.aggs
			.iter()
			.map(|(function, args)| match &arg_refs {
				ArgRefs::Shared(name) => (function.clone(), vec![Expr::column(name.clone())]),
				ArgRefs::PerEntry => (function.clone(), args.clone()),
			})
			.collect::<Vec<_>>();

		// 2. Aggregate (grouped queries exchange on the group key first; global is single-stage).
		let aggregate_input = if global {
			agg_input
		} else {
			LogicalPlan::Exchange {
				input: Box::new(agg_input),
				partition_count: DEFAULT_AGGREGATE_PARTITION_COUNT,
				key: group_expr.clone(),
			}
		};
		let aggregate = LogicalPlan::Aggregate {
			input: Box::new(aggregate_input),
			group_expr,
			aggr_expr,
		};

		// 3. Rename to branch-unique columns so the recombine join/cross is unambiguous.
		let mut exprs = Vec::with_capacity(self.group_keys.len() + branch.aggs.len());
		let mut names = Vec::with_capacity(self.group_keys.len() + branch.aggs.len());
		for (key_index, key) in self.group_keys.iter().enumerate() {
			exprs.push(Expr::column(key.name.clone()));
			names.push(Some(branch_group_key_name(index, key_index)));
		}
		for (slot, (function, _)) in branch.aggs.iter().enumerate() {
			exprs.push(Expr::column(aggregate_output_name(function).to_string()));
			names.push(Some(branch_agg_name(index, slot)));
		}
		LogicalPlan::Project {
			input: Box::new(aggregate),
			exprs,
			output_names: names,
		}
	}

	fn recombine(&self, left: LogicalPlan, right: LogicalPlan, right_index: usize) -> LogicalPlan {
		if self.is_global() {
			// Each global branch is one row; ADR 239 scalar-local Cross binds this as a local cartesian.
			return build_auto_join_plan(left, right, JoinKind::Cross, Vec::new());
		}
		// Grouped: null-safe equi-join on the group keys (ADR 235), so `GROUP BY NULL` recombines.
		let on = (0..self.group_keys.len())
			.map(|key| {
				(
					Expr::column(branch_group_key_name(0, key)),
					Expr::column(branch_group_key_name(right_index, key)),
				)
			})
			.collect::<Vec<_>>();
		LogicalPlan::Join {
			left: Box::new(left),
			right: Box::new(right),
			kind: JoinKind::Inner,
			on,
			null_safe_keys: true,
			strategy_intent: JoinStrategyIntent::Auto,
		}
	}
}

enum ArgRefs {
	/// Dedup branch: every aggregate applies to the single deduped argument column.
	Shared(String),
	/// Plain branch: each entry keeps its own argument expressions.
	PerEntry,
}

fn branch_group_key_name(branch: usize, key: usize) -> String {
	format!("__md_b{branch}_g{key}")
}

fn branch_agg_name(branch: usize, slot: usize) -> String {
	format!("__md_b{branch}_a{slot}")
}

fn branch_dedup_arg_name(branch: usize) -> String {
	format!("__md_b{branch}_arg")
}

#[derive(Debug)]
struct LoweredAggregateProjection {
	exprs: Vec<Expr>,
	output_names: Vec<Option<String>>,
	required: bool,
}

impl LoweredAggregateProjection {
	fn derived_output_names(&self) -> Vec<String> {
		self.exprs
			.iter()
			.enumerate()
			.map(|(idx, expr)| {
				self.output_names[idx]
					.clone()
					.unwrap_or_else(|| logical_expr_output_name(expr, idx))
			})
			.collect()
	}
}

#[derive(Debug, Clone)]
struct LoweredAggregateCall {
	function: AggregateFunction,
	args: Vec<Expr>,
	distinct: bool,
}

#[derive(Debug)]
struct AggregateOutputCatalog {
	group_keys: Vec<GroupKey>,
	entries: Vec<AggregateOutputEntry>,
	natural_output_name_counts: HashMap<String, usize>,
	final_output_name_counts: HashMap<String, usize>,
	select_aliases: HashSet<String>,
}

impl AggregateOutputCatalog {
	fn new(group_keys: &[GroupKey]) -> Self {
		let mut natural_output_name_counts = HashMap::new();
		for key in group_keys {
			*natural_output_name_counts
				.entry(key.name.clone())
				.or_insert(0) += 1;
		}

		Self {
			group_keys: group_keys.to_vec(),
			entries: Vec::new(),
			natural_output_name_counts,
			final_output_name_counts: HashMap::new(),
			select_aliases: HashSet::new(),
		}
	}

	fn add_requirement(&mut self, call: LoweredAggregateCall, source: AggregateSource) -> usize {
		let LoweredAggregateCall {
			function,
			args,
			distinct,
		} = call;
		let signature = grouped_aggregate_signature(&function, &args, distinct);
		if let Some(signature) = signature.as_ref()
			&& let Some(index) = self
				.entries
				.iter()
				.position(|entry| entry.signature.as_ref() == Some(signature))
		{
			self.entries[index].mark_source(source);
			return index;
		}

		let natural_name = aggregate_output_name(&function).to_string();
		*self
			.natural_output_name_counts
			.entry(natural_name.clone())
			.or_insert(0) += 1;
		let index = self.entries.len();
		self.entries.push(AggregateOutputEntry {
			function,
			args,
			distinct,
			signature,
			natural_name,
			in_select: matches!(source, AggregateSource::Select),
			in_having: matches!(source, AggregateSource::Having),
			order_by_visible_names: Vec::new(),
		});
		index
	}

	fn add_select_alias(&mut self, alias: &Ident) {
		self.select_aliases.insert(alias.value.clone());
	}

	fn mark_order_by_visible(&mut self, index: usize, output_name: String) {
		if let Some(entry) = self.entries.get_mut(index)
			&& !entry.order_by_visible_names.contains(&output_name)
		{
			entry.order_by_visible_names.push(output_name);
		}
	}

	fn aggr_expr(&self) -> Vec<(AggregateFunction, Vec<Expr>)> {
		self.entries
			.iter()
			.map(|entry| (entry.function.clone(), entry.args.clone()))
			.collect()
	}

	fn natural_output_names(&self) -> Vec<String> {
		self.group_keys
			.iter()
			.map(|key| key.name.clone())
			.chain(self.entries.iter().map(|entry| entry.natural_name.clone()))
			.collect()
	}

	fn set_final_output_names(&mut self, names: Vec<String>) {
		self.final_output_name_counts.clear();
		for name in names {
			*self.final_output_name_counts.entry(name).or_insert(0) += 1;
		}
	}

	fn resolve_for_projection(&self, signature: &GroupedAggregateSignature) -> ResolveOutcome {
		self.resolve_natural(signature)
	}

	fn resolve_for_having(&self, signature: &GroupedAggregateSignature) -> ResolveOutcome {
		self.resolve_natural(signature)
	}

	fn resolve_natural(&self, signature: &GroupedAggregateSignature) -> ResolveOutcome {
		let Some(entry) = self.entry_for_signature(signature) else {
			return ResolveOutcome::NotInSelect;
		};
		if self
			.natural_output_name_counts
			.get(&entry.natural_name)
			.copied()
			.unwrap_or(0)
			> 1
		{
			return ResolveOutcome::Ambiguous;
		}
		ResolveOutcome::Found(entry.natural_name.clone())
	}

	fn resolve_for_order_by(&self, signature: &GroupedAggregateSignature) -> ResolveOutcome {
		let Some(entry) = self.entry_for_signature(signature) else {
			return ResolveOutcome::NotInSelect;
		};
		let [output_name] = entry.order_by_visible_names.as_slice() else {
			return if entry.order_by_visible_names.is_empty() {
				ResolveOutcome::NotInSelect
			} else {
				ResolveOutcome::Ambiguous
			};
		};
		if self
			.final_output_name_counts
			.get(output_name)
			.copied()
			.unwrap_or(0)
			> 1
		{
			return ResolveOutcome::Ambiguous;
		}
		ResolveOutcome::Found(output_name.clone())
	}

	fn entry_for_signature(
		&self,
		signature: &GroupedAggregateSignature,
	) -> Option<&AggregateOutputEntry> {
		self.entries
			.iter()
			.find(|entry| entry.signature.as_ref() == Some(signature))
	}

	fn reject_having_alias_references(
		&self,
		having: Option<&SqlExpr>,
	) -> Result<(), FrontendError> {
		let Some(having) = having else {
			return Ok(());
		};
		reject_having_alias_references(having, &self.select_aliases)
	}
}

fn build_distinct_aggregate_rewrite(
	catalog: &AggregateOutputCatalog,
) -> Result<Option<DistinctAggregateRewrite>, FrontendError> {
	if catalog.entries.iter().all(|entry| !entry.distinct) {
		return Ok(None);
	}

	// ADR 240: fan out one dedup branch per structural distinct-argument identity, plus plain
	// branch(es) for non-distinct aggregates. Plain branches shard only when duplicate aggregate
	// output names would make normalization ambiguous. Record each catalog entry's (branch, slot)
	// so recombine can be normalized back to `[group keys..., entries in catalog order]`.
	let mut branches: Vec<RecombineBranch> = Vec::new();
	let mut dedup_identities: Vec<(Expr, usize)> = Vec::new();
	let mut plain_branches: Vec<usize> = Vec::new();
	let mut entry_slots = Vec::with_capacity(catalog.entries.len());

	for entry in &catalog.entries {
		let branch_index = if entry.distinct {
			let arg = distinct_aggregate_arg(entry)?;
			let identity = canonical_aggregate_signature_expr(&arg);
			match dedup_identities
				.iter()
				.find(|(existing, _)| expr_structural_eq(existing, &identity))
			{
				Some((_, index)) => *index,
				None => {
					let index = branches.len();
					branches.push(RecombineBranch {
						dedup_arg: Some(arg),
						aggs: Vec::new(),
					});
					dedup_identities.push((identity, index));
					index
				}
			}
		} else {
			let output_name = aggregate_output_name(&entry.function);
			match plain_branches.iter().copied().find(|index| {
				branches[*index]
					.aggs
					.iter()
					.all(|(function, _)| aggregate_output_name(function) != output_name)
			}) {
				Some(index) => index,
				None => {
					let index = branches.len();
					branches.push(RecombineBranch {
						dedup_arg: None,
						aggs: Vec::new(),
					});
					plain_branches.push(index);
					index
				}
			}
		};
		let slot = branches[branch_index].aggs.len();
		branches[branch_index]
			.aggs
			.push((entry.function.clone(), entry.args.clone()));
		entry_slots.push((branch_index, slot));
	}

	Ok(Some(DistinctAggregateRewrite {
		group_keys: catalog.group_keys.clone(),
		branches,
		entry_slots,
		output_names: catalog.natural_output_names(),
	}))
}

fn distinct_aggregate_arg(entry: &AggregateOutputEntry) -> Result<Expr, FrontendError> {
	match entry.function {
		AggregateFunction::Count
		| AggregateFunction::Sum
		| AggregateFunction::Min
		| AggregateFunction::Max
		| AggregateFunction::Avg => {}
		_ => {
			return Err(unsupported("unsupported DISTINCT aggregate function"));
		}
	}
	// ADR 240 D3: computed distinct arguments are projected before dedup, so any single argument
	// expression is accepted (the dedup branch projects it to a generated column).
	let [arg] = entry.args.as_slice() else {
		return Err(unsupported("DISTINCT aggregate requires a single argument"));
	};
	Ok(arg.clone())
}

#[derive(Debug)]
struct AggregateOutputEntry {
	function: AggregateFunction,
	args: Vec<Expr>,
	distinct: bool,
	signature: Option<GroupedAggregateSignature>,
	natural_name: String,
	in_select: bool,
	in_having: bool,
	order_by_visible_names: Vec<String>,
}

impl AggregateOutputEntry {
	fn mark_source(&mut self, source: AggregateSource) {
		match source {
			AggregateSource::Select => self.in_select = true,
			AggregateSource::Having => self.in_having = true,
		}
	}
}

#[derive(Debug, Clone)]
struct GroupedAggregateSignature {
	kernel: aggregate_kernel::StableAggregateKernel,
	args: Vec<Expr>,
	distinct: bool,
}

impl PartialEq for GroupedAggregateSignature {
	fn eq(&self, other: &Self) -> bool {
		self.kernel == other.kernel
			&& self.distinct == other.distinct
			&& self.args.len() == other.args.len()
			&& self
				.args
				.iter()
				.zip(&other.args)
				.all(|(left, right)| expr_structural_eq(left, right))
	}
}

impl Eq for GroupedAggregateSignature {}

impl Hash for GroupedAggregateSignature {
	fn hash<H: Hasher>(&self, state: &mut H) {
		self.kernel.hash(state);
		self.distinct.hash(state);
		self.args.len().hash(state);
		for arg in &self.args {
			hash_expr_structural(arg, state);
		}
	}
}

fn hash_expr_structural(expr: &Expr, state: &mut impl Hasher) {
	match expr {
		Expr::Column(column) => {
			0u8.hash(state);
			column.hash(state);
		}
		Expr::Literal(value) => {
			1u8.hash(state);
			hash_scalar_structural(value, state);
		}
		Expr::UntypedNull => {
			25u8.hash(state);
		}
		Expr::Cast(inner, data_type) => {
			2u8.hash(state);
			hash_expr_structural(inner, state);
			hash_data_type_structural(data_type, state);
		}
		Expr::TryCast(inner, data_type) => {
			3u8.hash(state);
			hash_expr_structural(inner, state);
			hash_data_type_structural(data_type, state);
		}
		Expr::Add(left, right) => {
			4u8.hash(state);
			hash_expr_structural(left, state);
			hash_expr_structural(right, state);
		}
		Expr::Sub(left, right) => {
			5u8.hash(state);
			hash_expr_structural(left, state);
			hash_expr_structural(right, state);
		}
		Expr::Mul(left, right) => {
			6u8.hash(state);
			hash_expr_structural(left, state);
			hash_expr_structural(right, state);
		}
		Expr::Div(left, right) => {
			7u8.hash(state);
			hash_expr_structural(left, state);
			hash_expr_structural(right, state);
		}
		Expr::Eq(left, right) => {
			8u8.hash(state);
			hash_expr_structural(left, state);
			hash_expr_structural(right, state);
		}
		Expr::Neq(left, right) => {
			9u8.hash(state);
			hash_expr_structural(left, state);
			hash_expr_structural(right, state);
		}
		Expr::Lt(left, right) => {
			10u8.hash(state);
			hash_expr_structural(left, state);
			hash_expr_structural(right, state);
		}
		Expr::Lte(left, right) => {
			11u8.hash(state);
			hash_expr_structural(left, state);
			hash_expr_structural(right, state);
		}
		Expr::Gt(left, right) => {
			12u8.hash(state);
			hash_expr_structural(left, state);
			hash_expr_structural(right, state);
		}
		Expr::Gte(left, right) => {
			13u8.hash(state);
			hash_expr_structural(left, state);
			hash_expr_structural(right, state);
		}
		Expr::And(left, right) => {
			14u8.hash(state);
			hash_expr_structural(left, state);
			hash_expr_structural(right, state);
		}
		Expr::Or(left, right) => {
			15u8.hash(state);
			hash_expr_structural(left, state);
			hash_expr_structural(right, state);
		}
		Expr::Not(inner) => {
			16u8.hash(state);
			hash_expr_structural(inner, state);
		}
		Expr::IsNull(inner) => {
			17u8.hash(state);
			hash_expr_structural(inner, state);
		}
		Expr::IsNotNull(inner) => {
			18u8.hash(state);
			hash_expr_structural(inner, state);
		}
		Expr::TimeBucket(interval, inner) => {
			19u8.hash(state);
			interval.hash(state);
			hash_expr_structural(inner, state);
		}
		Expr::InList {
			expr,
			list,
			negated,
		} => {
			20u8.hash(state);
			negated.hash(state);
			hash_expr_structural(expr, state);
			list.len().hash(state);
			for item in list {
				hash_expr_structural(item, state);
			}
		}
		Expr::Like {
			expr,
			pattern,
			negated,
			case_insensitive,
		} => {
			21u8.hash(state);
			hash_expr_structural(expr, state);
			pattern.hash(state);
			negated.hash(state);
			case_insensitive.hash(state);
		}
		Expr::LogicalAscription { expr, logical_type } => {
			22u8.hash(state);
			logical_type.hash(state);
			hash_expr_structural(expr, state);
		}
		Expr::SqlLogicalCast {
			expr,
			logical_name,
			try_cast,
		} => {
			23u8.hash(state);
			logical_name.hash(state);
			try_cast.hash(state);
			hash_expr_structural(expr, state);
		}
		Expr::SqlConvert {
			expr,
			target_logical_name,
		} => {
			28u8.hash(state);
			target_logical_name.hash(state);
			hash_expr_structural(expr, state);
		}
		Expr::Case {
			when_then,
			else_expr,
		} => {
			24u8.hash(state);
			when_then.len().hash(state);
			for (condition, result) in when_then {
				hash_expr_structural(condition, state);
				hash_expr_structural(result, state);
			}
			else_expr.is_some().hash(state);
			if let Some(else_expr) = else_expr {
				hash_expr_structural(else_expr, state);
			}
		}
		Expr::AtTimeZone(inner, zone) => {
			25u8.hash(state);
			zone.hash(state);
			hash_expr_structural(inner, state);
		}
		Expr::Extract(field, inner) => {
			26u8.hash(state);
			field.hash(state);
			hash_expr_structural(inner, state);
		}
		Expr::DateTrunc(field, inner) => {
			27u8.hash(state);
			field.hash(state);
			hash_expr_structural(inner, state);
		}
	}
}

fn expr_structural_eq(left: &Expr, right: &Expr) -> bool {
	match (left, right) {
		(Expr::Column(left), Expr::Column(right)) => left == right,
		(Expr::Literal(left), Expr::Literal(right)) => scalar_structural_eq(left, right),
		(Expr::UntypedNull, Expr::UntypedNull) => true,
		(Expr::Cast(left_inner, left_type), Expr::Cast(right_inner, right_type))
		| (Expr::TryCast(left_inner, left_type), Expr::TryCast(right_inner, right_type)) => {
			expr_structural_eq(left_inner, right_inner)
				&& data_type_structural_eq(left_type, right_type)
		}
		(
			Expr::LogicalAscription {
				expr: left_inner,
				logical_type: left_logical_type,
			},
			Expr::LogicalAscription {
				expr: right_inner,
				logical_type: right_logical_type,
			},
		) => left_logical_type == right_logical_type && expr_structural_eq(left_inner, right_inner),
		(
			Expr::SqlLogicalCast {
				expr: left_inner,
				logical_name: left_logical_name,
				try_cast: left_try_cast,
			},
			Expr::SqlLogicalCast {
				expr: right_inner,
				logical_name: right_logical_name,
				try_cast: right_try_cast,
			},
		) => {
			left_logical_name == right_logical_name
				&& left_try_cast == right_try_cast
				&& expr_structural_eq(left_inner, right_inner)
		}
		(
			Expr::SqlConvert {
				expr: left_inner,
				target_logical_name: left_logical_name,
			},
			Expr::SqlConvert {
				expr: right_inner,
				target_logical_name: right_logical_name,
			},
		) => left_logical_name == right_logical_name && expr_structural_eq(left_inner, right_inner),
		(Expr::Add(left_left, left_right), Expr::Add(right_left, right_right))
		| (Expr::Sub(left_left, left_right), Expr::Sub(right_left, right_right))
		| (Expr::Mul(left_left, left_right), Expr::Mul(right_left, right_right))
		| (Expr::Div(left_left, left_right), Expr::Div(right_left, right_right))
		| (Expr::Eq(left_left, left_right), Expr::Eq(right_left, right_right))
		| (Expr::Neq(left_left, left_right), Expr::Neq(right_left, right_right))
		| (Expr::Lt(left_left, left_right), Expr::Lt(right_left, right_right))
		| (Expr::Lte(left_left, left_right), Expr::Lte(right_left, right_right))
		| (Expr::Gt(left_left, left_right), Expr::Gt(right_left, right_right))
		| (Expr::Gte(left_left, left_right), Expr::Gte(right_left, right_right))
		| (Expr::And(left_left, left_right), Expr::And(right_left, right_right))
		| (Expr::Or(left_left, left_right), Expr::Or(right_left, right_right)) => {
			expr_structural_eq(left_left, right_left) && expr_structural_eq(left_right, right_right)
		}
		(Expr::Not(left), Expr::Not(right))
		| (Expr::IsNull(left), Expr::IsNull(right))
		| (Expr::IsNotNull(left), Expr::IsNotNull(right)) => expr_structural_eq(left, right),
		(Expr::TimeBucket(left_interval, left), Expr::TimeBucket(right_interval, right)) => {
			left_interval == right_interval && expr_structural_eq(left, right)
		}
		(Expr::AtTimeZone(left, left_zone), Expr::AtTimeZone(right, right_zone)) => {
			left_zone == right_zone && expr_structural_eq(left, right)
		}
		(Expr::Extract(left_field, left), Expr::Extract(right_field, right)) => {
			left_field == right_field && expr_structural_eq(left, right)
		}
		(Expr::DateTrunc(left_field, left), Expr::DateTrunc(right_field, right)) => {
			left_field == right_field && expr_structural_eq(left, right)
		}
		(
			Expr::Like {
				expr: left_expr,
				pattern: left_pattern,
				negated: left_negated,
				case_insensitive: left_case_insensitive,
			},
			Expr::Like {
				expr: right_expr,
				pattern: right_pattern,
				negated: right_negated,
				case_insensitive: right_case_insensitive,
			},
		) => {
			left_pattern == right_pattern
				&& left_negated == right_negated
				&& left_case_insensitive == right_case_insensitive
				&& expr_structural_eq(left_expr, right_expr)
		}
		(
			Expr::InList {
				expr: left_expr,
				list: left_list,
				negated: left_negated,
			},
			Expr::InList {
				expr: right_expr,
				list: right_list,
				negated: right_negated,
			},
		) => {
			left_negated == right_negated
				&& expr_structural_eq(left_expr, right_expr)
				&& left_list.len() == right_list.len()
				&& left_list
					.iter()
					.zip(right_list.iter())
					.all(|(left, right)| expr_structural_eq(left, right))
		}
		(
			Expr::Case {
				when_then: left_when_then,
				else_expr: left_else,
			},
			Expr::Case {
				when_then: right_when_then,
				else_expr: right_else,
			},
		) => {
			left_when_then.len() == right_when_then.len()
				&& left_when_then.iter().zip(right_when_then.iter()).all(
					|((left_condition, left_result), (right_condition, right_result))| {
						expr_structural_eq(left_condition, right_condition)
							&& expr_structural_eq(left_result, right_result)
					},
				) && match (left_else, right_else) {
				(Some(left_else), Some(right_else)) => expr_structural_eq(left_else, right_else),
				(None, None) => true,
				_ => false,
			}
		}
		_ => false,
	}
}

fn hash_scalar_structural(value: &ScalarValue, state: &mut impl Hasher) {
	match value {
		ScalarValue::Boolean(value) => {
			0u8.hash(state);
			value.hash(state);
		}
		ScalarValue::Int32(value) => {
			1u8.hash(state);
			value.hash(state);
		}
		ScalarValue::Int64(value) => {
			2u8.hash(state);
			value.hash(state);
		}
		ScalarValue::Date32(value) => {
			3u8.hash(state);
			value.hash(state);
		}
		ScalarValue::Timestamp { value, unit } => {
			4u8.hash(state);
			unit.hash(state);
			value.hash(state);
		}
		ScalarValue::Interval(value) => {
			5u8.hash(state);
			value.hash(state);
		}
		ScalarValue::CalendarMonthInterval(value) => {
			6u8.hash(state);
			value.hash(state);
		}
		ScalarValue::Float32(value) => {
			7u8.hash(state);
			value.map(f32::to_bits).hash(state);
		}
		ScalarValue::Float64(value) => {
			8u8.hash(state);
			value.map(f64::to_bits).hash(state);
		}
		ScalarValue::Utf8(value) => {
			9u8.hash(state);
			value.hash(state);
		}
	}
}

fn scalar_structural_eq(left: &ScalarValue, right: &ScalarValue) -> bool {
	match (left, right) {
		(ScalarValue::Boolean(left), ScalarValue::Boolean(right)) => left == right,
		(ScalarValue::Int32(left), ScalarValue::Int32(right)) => left == right,
		(ScalarValue::Int64(left), ScalarValue::Int64(right)) => left == right,
		(ScalarValue::Date32(left), ScalarValue::Date32(right)) => left == right,
		(
			ScalarValue::Timestamp {
				value: left_value,
				unit: left_unit,
			},
			ScalarValue::Timestamp {
				value: right_value,
				unit: right_unit,
			},
		) => left_unit == right_unit && left_value == right_value,
		(ScalarValue::Interval(left), ScalarValue::Interval(right)) => left == right,
		(ScalarValue::CalendarMonthInterval(left), ScalarValue::CalendarMonthInterval(right)) => {
			left == right
		}
		(ScalarValue::Float32(left), ScalarValue::Float32(right)) => {
			left.map(f32::to_bits) == right.map(f32::to_bits)
		}
		(ScalarValue::Float64(left), ScalarValue::Float64(right)) => {
			left.map(f64::to_bits) == right.map(f64::to_bits)
		}
		(ScalarValue::Utf8(left), ScalarValue::Utf8(right)) => left == right,
		_ => false,
	}
}

fn hash_data_type_structural(data_type: &DataType, state: &mut impl Hasher) {
	match data_type {
		DataType::Int32 => 0u8.hash(state),
		DataType::Int64 => 1u8.hash(state),
		DataType::Date32 => 2u8.hash(state),
		DataType::Timestamp(unit, Some(timezone)) if timezone.as_ref() == "UTC" => {
			3u8.hash(state);
			unit.hash(state);
		}
		DataType::Float32 => 4u8.hash(state),
		DataType::Float64 => 5u8.hash(state),
		other => {
			255u8.hash(state);
			format!("{other:?}").hash(state);
		}
	}
}

fn data_type_structural_eq(left: &DataType, right: &DataType) -> bool {
	left == right
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ResolveOutcome {
	Found(String),
	NotInSelect,
	Ambiguous,
}

#[derive(Debug, Clone, Copy)]
enum AggregateSource {
	Select,
	Having,
}

#[derive(Debug, Clone, Copy)]
enum AggregateSqlMode {
	Global,
	Grouped,
}

fn lower_group_by_keys(group_by: &GroupByExpr) -> Result<Vec<GroupKey>, FrontendError> {
	// P0.1: sqlparser stores plain keys in Expressions and ClickHouse-style modifiers
	// beside them; GROUP BY ALL and every modifier are outside ADR 011.
	match group_by {
		GroupByExpr::Expressions(exprs, modifiers) => {
			if !modifiers.is_empty() {
				return Err(unsupported("GROUP BY modifiers are not supported"));
			}
			if exprs.is_empty() {
				return Err(unsupported("GROUP BY requires at least one key"));
			}
			exprs.iter().map(lower_group_by_key).collect()
		}
		GroupByExpr::All(_) => Err(unsupported("GROUP BY ALL is not supported")),
	}
}

fn lower_group_by_key(expr: &SqlExpr) -> Result<GroupKey, FrontendError> {
	match expr {
		SqlExpr::Identifier(ident) => {
			let name = ident.value.clone();
			Ok(GroupKey {
				name: name.clone(),
				expr: Expr::column(name),
			})
		}
		SqlExpr::CompoundIdentifier(idents) if idents.len() == 2 => {
			let qualifier = TableRef::new(idents[0].value.clone());
			let name = idents[1].value.clone();
			Ok(GroupKey {
				name: name.clone(),
				expr: Expr::qualified_column(qualifier, name),
			})
		}
		_ => Err(unsupported(
			"GROUP BY keys must be simple column references",
		)),
	}
}

fn reject_having_alias_references(
	expr: &SqlExpr,
	select_aliases: &HashSet<String>,
) -> Result<(), FrontendError> {
	match strip_nested_sql_expr(expr) {
		SqlExpr::Identifier(ident) if select_aliases.contains(&ident.value) => {
			Err(unsupported(having_alias_message()))
		}
		SqlExpr::Function(function) if aggregate_function_name(&function.name).is_some() => Ok(()),
		SqlExpr::BinaryOp { left, right, .. } => {
			reject_having_alias_references(left, select_aliases)?;
			reject_having_alias_references(right, select_aliases)
		}
		SqlExpr::Case {
			operand,
			conditions,
			else_result,
			..
		} => {
			if let Some(operand) = operand {
				reject_having_alias_references(operand, select_aliases)?;
			}
			for case_when in conditions {
				reject_having_alias_references(&case_when.condition, select_aliases)?;
				reject_having_alias_references(&case_when.result, select_aliases)?;
			}
			if let Some(else_result) = else_result {
				reject_having_alias_references(else_result, select_aliases)?;
			}
			Ok(())
		}
		SqlExpr::Cast { expr, .. } => reject_having_alias_references(expr, select_aliases),
		SqlExpr::UnaryOp { expr, .. }
		| SqlExpr::IsNull(expr)
		| SqlExpr::IsNotNull(expr)
		| SqlExpr::Nested(expr) => reject_having_alias_references(expr, select_aliases),
		_ => Ok(()),
	}
}

fn having_alias_message() -> &'static str {
	"HAVING does not resolve SELECT aliases; spell the aggregate explicitly"
}

fn aggregate_call_not_in_select_message(clause: &str) -> &'static str {
	match clause {
		"HAVING" => "HAVING aggregate-call must appear in SELECT list",
		"ORDER BY" => {
			"ORDER BY <aggregate-call> requires the aggregate to appear in the SELECT list"
		}
		other => unreachable!("unsupported grouped aggregate clause: {other}"),
	}
}

fn ambiguous_aggregate_call_message(clause: &str) -> &'static str {
	match clause {
		"HAVING" => "ambiguous HAVING aggregate-call",
		"ORDER BY" => "ambiguous ORDER BY aggregate-call",
		other => unreachable!("unsupported grouped aggregate clause: {other}"),
	}
}

fn select_item_expr_and_alias(
	item: &SelectItem,
) -> Result<(&SqlExpr, Option<&Ident>), FrontendError> {
	match item {
		SelectItem::UnnamedExpr(expr) => Ok((expr, None)),
		SelectItem::ExprWithAlias { expr, alias } => Ok((expr, Some(alias))),
		SelectItem::ExprWithAliases { .. } => {
			Err(unsupported("multi-alias projections are not supported"))
		}
		SelectItem::Wildcard(_) => Err(unsupported("mixed wildcard projections are not supported")),
		SelectItem::QualifiedWildcard(_, _) => Err(unsupported(
			"qualified wildcard projections are not supported",
		)),
	}
}

fn select_expr_is_computed(expr: &SqlExpr) -> bool {
	match strip_nested_sql_expr(expr) {
		SqlExpr::Identifier(_) | SqlExpr::CompoundIdentifier(_) => false,
		SqlExpr::Function(function) => aggregate_function_name(&function.name).is_none(),
		_ => true,
	}
}

fn logical_expr_output_name(expr: &Expr, idx: usize) -> String {
	match expr {
		Expr::Column(column) => column.name.0.clone(),
		Expr::LogicalAscription { expr, .. }
		| Expr::SqlLogicalCast { expr, .. }
		| Expr::SqlConvert { expr, .. } => logical_expr_output_name(expr, idx),
		_ => format!("col_{idx}"),
	}
}

fn expr_contains_qualified_column(expr: &Expr) -> bool {
	match expr {
		Expr::Column(column) => column.qualifier.is_some(),
		Expr::Literal(_) | Expr::UntypedNull => false,
		Expr::LogicalAscription { expr: inner, .. }
		| Expr::SqlLogicalCast { expr: inner, .. }
		| Expr::SqlConvert { expr: inner, .. }
		| Expr::Cast(inner, _)
		| Expr::TryCast(inner, _)
		| Expr::Not(inner)
		| Expr::IsNull(inner)
		| Expr::IsNotNull(inner)
		| Expr::Like { expr: inner, .. }
		| Expr::TimeBucket(_, inner)
		| Expr::AtTimeZone(inner, _)
		| Expr::Extract(_, inner)
		| Expr::DateTrunc(_, inner) => expr_contains_qualified_column(inner),
		Expr::InList { expr, list, .. } => {
			expr_contains_qualified_column(expr) || list.iter().any(expr_contains_qualified_column)
		}
		Expr::Case {
			when_then,
			else_expr,
		} => {
			when_then.iter().any(|(condition, result)| {
				expr_contains_qualified_column(condition) || expr_contains_qualified_column(result)
			}) || else_expr
				.as_deref()
				.is_some_and(expr_contains_qualified_column)
		}
		Expr::Add(left, right)
		| Expr::Sub(left, right)
		| Expr::Mul(left, right)
		| Expr::Div(left, right)
		| Expr::Eq(left, right)
		| Expr::Neq(left, right)
		| Expr::Lt(left, right)
		| Expr::Lte(left, right)
		| Expr::Gt(left, right)
		| Expr::Gte(left, right)
		| Expr::And(left, right)
		| Expr::Or(left, right) => {
			expr_contains_qualified_column(left) || expr_contains_qualified_column(right)
		}
	}
}

fn projection_contains_aggregate_call(projection: &[SelectItem]) -> bool {
	projection.iter().any(select_item_contains_aggregate_call)
}

/// ADR 241: a ranking window function extracted from a SELECT item (its function, the output column
/// name it appends, and its lowered partition/order spec).
struct ExtractedWindow {
	function: WindowFunction,
	output_name: String,
	partition_by: Vec<Expr>,
	order_by: Vec<SortExpr>,
	args: Vec<Expr>,
	frame: Option<WindowFrame>,
}

/// ADR 248: window calls sharing one `(PARTITION BY, ORDER BY)` spec, collected into a single
/// `Window` node. Distinct specs each get their own group (and node); the frame is per-call, so it
/// is not part of the grouping key.
struct WindowSpecGroup {
	partition_by: Vec<Expr>,
	order_by: Vec<SortExpr>,
	functions: Vec<WindowFunctionCall>,
}

/// ADR 250: `WINDOW w AS (...)` definitions in scope for a SELECT, keyed by window name, for
/// resolving `OVER w` references. Window inheritance/extension is a spec-merge mechanism, deferred.
type NamedWindowMap<'a> = HashMap<String, &'a WindowSpec>;

/// ADR 243: lower a sqlparser `ROWS BETWEEN …` frame to the IR `WindowFrame`. `RANGE`/`GROUPS` units stay
/// rejected (→ ADR 244); the `end_bound = None` shorthand (`ROWS n PRECEDING`) means `… AND CURRENT ROW`.
/// Total order over frame bounds for the start ≤ end validation, by signed offset magnitude:
/// `UNBOUNDED PRECEDING < n PRECEDING < CURRENT ROW < n FOLLOWING < UNBOUNDED FOLLOWING`.
fn window_frame_bound_rank(bound: &WindowFrameBound) -> f64 {
	match bound {
		WindowFrameBound::UnboundedPreceding => f64::NEG_INFINITY,
		WindowFrameBound::Preceding(delta) => -frame_delta_magnitude(delta),
		WindowFrameBound::CurrentRow => 0.0,
		WindowFrameBound::Following(delta) => frame_delta_magnitude(delta),
		WindowFrameBound::UnboundedFollowing => f64::INFINITY,
	}
}

fn frame_delta_magnitude(delta: &WindowFrameDelta) -> f64 {
	match delta {
		WindowFrameDelta::Rows(n) | WindowFrameDelta::Groups(n) => *n as f64,
		WindowFrameDelta::Range(value) => scalar_offset_magnitude(value),
	}
}

/// A coarse non-negative magnitude of a RANGE offset literal for the start ≤ end ordering check (the
/// binder validates the offset's type against the key; this only orders bounds within one frame).
fn scalar_offset_magnitude(value: &ScalarValue) -> f64 {
	match value {
		ScalarValue::Int32(Some(v)) => f64::from(*v),
		ScalarValue::Int64(Some(v)) => *v as f64,
		ScalarValue::Float32(Some(v)) => f64::from(*v),
		ScalarValue::Float64(Some(v)) => *v,
		ScalarValue::Interval(Some(v)) => *v as f64,
		ScalarValue::CalendarMonthInterval(Some(v)) => f64::from(*v),
		_ => 0.0,
	}
}

fn range_offset_is_non_negative(value: &ScalarValue) -> bool {
	match value {
		ScalarValue::Int32(Some(v)) => *v >= 0,
		ScalarValue::Int64(Some(v)) => *v >= 0,
		ScalarValue::Float32(Some(v)) => *v >= 0.0,
		ScalarValue::Float64(Some(v)) => *v >= 0.0,
		ScalarValue::Interval(Some(v)) => *v >= 0,
		ScalarValue::CalendarMonthInterval(Some(v)) => *v >= 0,
		_ => false,
	}
}

fn window_frame_offset(expr: &SqlExpr) -> Result<u64, FrontendError> {
	let invalid = || unsupported("ROWS frame offset must be a non-negative integer");
	match stripped_sql_value(expr) {
		Some(Value::Number(number, _))
			if !number.starts_with('-')
				&& !number.contains('.')
				&& !number.contains('e')
				&& !number.contains('E') =>
		{
			number.parse::<u64>().map_err(|_| invalid())
		}
		_ => Err(invalid()),
	}
}

/// ADR 249: parse an `NTILE(n)` bucket count — a strictly positive integer literal.
fn ntile_bucket_count(expr: &SqlExpr) -> Result<u64, FrontendError> {
	let invalid = || unsupported("NTILE requires a positive integer bucket count");
	let count = match stripped_sql_value(expr) {
		Some(Value::Number(number, _))
			if !number.starts_with('-')
				&& !number.contains('.')
				&& !number.contains('e')
				&& !number.contains('E') =>
		{
			number.parse::<u64>().map_err(|_| invalid())?
		}
		_ => return Err(invalid()),
	};
	if count == 0 {
		return Err(invalid());
	}
	Ok(count)
}

/// Whether a frame carries an explicit offset bound (a `Preceding`/`Following` with a magnitude) — a RANGE
/// value distance or a GROUPS peer-group count; `UNBOUNDED`/`CURRENT ROW` bounds carry none.
fn frame_has_offset_bound(frame: &WindowFrame) -> bool {
	matches!(
		frame.start,
		WindowFrameBound::Preceding(_) | WindowFrameBound::Following(_)
	) || matches!(
		frame.end,
		WindowFrameBound::Preceding(_) | WindowFrameBound::Following(_)
	)
}

/// Extract the expression from an unnamed value-window-function argument.
/// Negate a numeric `ScalarValue` literal (for a `LAG`/`LEAD` `-n` default).
fn negate_scalar_literal(value: ScalarValue) -> Result<ScalarValue, FrontendError> {
	match value {
		ScalarValue::Int32(Some(v)) => Ok(ScalarValue::Int32(Some(-v))),
		ScalarValue::Int64(Some(v)) => Ok(ScalarValue::Int64(Some(-v))),
		ScalarValue::Float32(Some(v)) => Ok(ScalarValue::Float32(Some(-v))),
		ScalarValue::Float64(Some(v)) => Ok(ScalarValue::Float64(Some(-v))),
		_ => Err(unsupported(
			"LAG/LEAD numeric default expected after unary minus",
		)),
	}
}

fn value_function_arg_expr(arg: Option<&FunctionArg>) -> Result<&SqlExpr, FrontendError> {
	match arg {
		Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))) => Ok(expr),
		Some(_) => Err(unsupported(
			"value window function arguments must be expressions",
		)),
		None => Err(unsupported(
			"value window function is missing a required argument",
		)),
	}
}

fn window_function_default_name(function: &WindowFunction) -> &'static str {
	match function {
		WindowFunction::RowNumber => "row_number",
		WindowFunction::Rank => "rank",
		WindowFunction::DenseRank => "dense_rank",
		WindowFunction::Ntile { .. } => "ntile",
		WindowFunction::Aggregate(function) => aggregate_kernel::stable_spec_for_function(function)
			.map(|spec| spec.output_name)
			.unwrap_or("window"),
		WindowFunction::Value(value) => match value {
			ValueFunction::Lag { .. } => "lag",
			ValueFunction::Lead { .. } => "lead",
			ValueFunction::FirstValue => "first_value",
			ValueFunction::LastValue => "last_value",
			ValueFunction::NthValue { .. } => "nth_value",
		},
	}
}

fn window_internal_output_name(index: usize) -> String {
	format!("__toron_window_{index}")
}

fn window_function_args_are_empty(args: &FunctionArguments) -> bool {
	match args {
		FunctionArguments::None => true,
		FunctionArguments::List(list) => list.args.is_empty(),
		_ => false,
	}
}

/// Two `OVER` specs match when their partition keys and order keys are structurally identical — used to
/// require a single distinct `OVER` spec per query (ADR 241 D6).
fn window_specs_match(
	left_partition: &[Expr],
	left_order: &[SortExpr],
	right_partition: &[Expr],
	right_order: &[SortExpr],
) -> bool {
	left_partition.len() == right_partition.len()
		&& left_order.len() == right_order.len()
		&& left_partition
			.iter()
			.zip(right_partition)
			.all(|(left, right)| expr_structural_eq(left, right))
		&& left_order.iter().zip(right_order).all(|(left, right)| {
			left.descending == right.descending
				&& left.nulls_first == right.nulls_first
				&& expr_structural_eq(&left.expr, &right.expr)
		})
}

fn projection_contains_window_function(projection: &[SelectItem]) -> bool {
	projection.iter().any(select_item_contains_window_function)
}

fn select_item_contains_aggregate_call(item: &SelectItem) -> bool {
	match item {
		SelectItem::UnnamedExpr(expr) => expr_contains_aggregate_call(expr),
		SelectItem::ExprWithAlias { expr, alias: _ } => expr_contains_aggregate_call(expr),
		SelectItem::ExprWithAliases { .. } => false,
		SelectItem::QualifiedWildcard(_, _) | SelectItem::Wildcard(_) => false,
	}
}

fn select_item_contains_window_function(item: &SelectItem) -> bool {
	match item {
		SelectItem::UnnamedExpr(expr) => expr_contains_window_function(expr),
		SelectItem::ExprWithAlias { expr, alias: _ } => expr_contains_window_function(expr),
		SelectItem::ExprWithAliases { .. } => false,
		SelectItem::QualifiedWildcard(_, _) | SelectItem::Wildcard(_) => false,
	}
}

fn expr_contains_aggregate_call(expr: &SqlExpr) -> bool {
	match strip_nested_sql_expr(expr) {
		// A windowed aggregate (`SUM(x) OVER (...)`, ADR 242) is a window call, not a GROUP BY
		// aggregate — the window-lowering path handles it.
		SqlExpr::Function(function) => {
			function.over.is_none() && aggregate_function_name(&function.name).is_some()
		}
		SqlExpr::BinaryOp { left, right, .. } => {
			expr_contains_aggregate_call(left) || expr_contains_aggregate_call(right)
		}
		SqlExpr::Case {
			operand,
			conditions,
			else_result,
			..
		} => {
			operand.as_deref().is_some_and(expr_contains_aggregate_call)
				|| conditions.iter().any(|case_when| {
					expr_contains_aggregate_call(&case_when.condition)
						|| expr_contains_aggregate_call(&case_when.result)
				}) || else_result
				.as_deref()
				.is_some_and(expr_contains_aggregate_call)
		}
		SqlExpr::Cast { expr, .. } | SqlExpr::Extract { expr, .. } => {
			expr_contains_aggregate_call(expr)
		}
		SqlExpr::Convert { expr, styles, .. } => {
			expr_contains_aggregate_call(expr) || styles.iter().any(expr_contains_aggregate_call)
		}
		SqlExpr::Interval(SqlInterval { value, .. }) => expr_contains_aggregate_call(value),
		SqlExpr::AtTimeZone {
			timestamp,
			time_zone,
		} => expr_contains_aggregate_call(timestamp) || expr_contains_aggregate_call(time_zone),
		SqlExpr::UnaryOp { expr, .. }
		| SqlExpr::IsNull(expr)
		| SqlExpr::IsNotNull(expr)
		| SqlExpr::Nested(expr) => expr_contains_aggregate_call(expr),
		_ => false,
	}
}

fn expr_contains_subquery(expr: &SqlExpr) -> bool {
	match strip_nested_sql_expr(expr) {
		SqlExpr::Subquery(_) | SqlExpr::Exists { .. } => true,
		SqlExpr::BinaryOp { left, right, .. } => {
			expr_contains_subquery(left) || expr_contains_subquery(right)
		}
		SqlExpr::Case {
			operand,
			conditions,
			else_result,
			..
		} => {
			operand.as_deref().is_some_and(expr_contains_subquery)
				|| conditions.iter().any(|case_when| {
					expr_contains_subquery(&case_when.condition)
						|| expr_contains_subquery(&case_when.result)
				}) || else_result.as_deref().is_some_and(expr_contains_subquery)
		}
		SqlExpr::Cast { expr, .. } | SqlExpr::Extract { expr, .. } => expr_contains_subquery(expr),
		SqlExpr::Interval(SqlInterval { value, .. }) => expr_contains_subquery(value),
		SqlExpr::AtTimeZone {
			timestamp,
			time_zone,
		} => expr_contains_subquery(timestamp) || expr_contains_subquery(time_zone),
		SqlExpr::UnaryOp { expr, .. }
		| SqlExpr::IsNull(expr)
		| SqlExpr::IsNotNull(expr)
		| SqlExpr::Nested(expr) => expr_contains_subquery(expr),
		SqlExpr::Function(function) => function_args_contain_subquery(function),
		_ => false,
	}
}

fn expr_contains_window_function(expr: &SqlExpr) -> bool {
	match strip_nested_sql_expr(expr) {
		SqlExpr::Function(function) => {
			function.over.is_some()
				|| function
					.filter
					.as_deref()
					.is_some_and(expr_contains_window_function)
				|| function_args_contain_window_function(function)
		}
		SqlExpr::BinaryOp { left, right, .. } => {
			expr_contains_window_function(left) || expr_contains_window_function(right)
		}
		SqlExpr::Case {
			operand,
			conditions,
			else_result,
			..
		} => {
			operand
				.as_deref()
				.is_some_and(expr_contains_window_function)
				|| conditions.iter().any(|case_when| {
					expr_contains_window_function(&case_when.condition)
						|| expr_contains_window_function(&case_when.result)
				}) || else_result
				.as_deref()
				.is_some_and(expr_contains_window_function)
		}
		SqlExpr::Cast { expr, .. } | SqlExpr::Extract { expr, .. } => {
			expr_contains_window_function(expr)
		}
		SqlExpr::Interval(SqlInterval { value, .. }) => expr_contains_window_function(value),
		SqlExpr::AtTimeZone {
			timestamp,
			time_zone,
		} => expr_contains_window_function(timestamp) || expr_contains_window_function(time_zone),
		SqlExpr::UnaryOp { expr, .. }
		| SqlExpr::IsNull(expr)
		| SqlExpr::IsNotNull(expr)
		| SqlExpr::Nested(expr) => expr_contains_window_function(expr),
		_ => false,
	}
}

fn function_args_contain_subquery(function: &Function) -> bool {
	match &function.args {
		FunctionArguments::Subquery(_) => true,
		FunctionArguments::List(list) => list.args.iter().any(|arg| match arg {
			FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
			| FunctionArg::Named {
				arg: FunctionArgExpr::Expr(expr),
				..
			}
			| FunctionArg::ExprNamed {
				arg: FunctionArgExpr::Expr(expr),
				..
			} => expr_contains_subquery(expr),
			_ => false,
		}),
		_ => false,
	}
}

fn function_args_contain_window_function(function: &Function) -> bool {
	let FunctionArguments::List(list) = &function.args else {
		return false;
	};
	list.args.iter().any(|arg| match arg {
		FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
		| FunctionArg::Named {
			arg: FunctionArgExpr::Expr(expr),
			..
		}
		| FunctionArg::ExprNamed {
			arg: FunctionArgExpr::Expr(expr),
			..
		} => expr_contains_window_function(expr),
		_ => false,
	})
}

fn reject_global_aggregate_projection_columns(
	projection: &[SelectItem],
) -> Result<(), FrontendError> {
	for item in projection {
		let (expr, _) = select_item_expr_and_alias(item)?;
		if expr_contains_nonaggregate_column_reference(expr) {
			return Err(unsupported("unsupported aggregate projection item"));
		}
	}
	Ok(())
}

fn expr_contains_nonaggregate_column_reference(expr: &SqlExpr) -> bool {
	match strip_nested_sql_expr(expr) {
		SqlExpr::Identifier(_) | SqlExpr::CompoundIdentifier(_) => true,
		SqlExpr::Function(function) if aggregate_function_name(&function.name).is_some() => false,
		SqlExpr::Function(function) => {
			function_args_contain_nonaggregate_column_reference(function)
		}
		SqlExpr::BinaryOp { left, right, .. } => {
			expr_contains_nonaggregate_column_reference(left)
				|| expr_contains_nonaggregate_column_reference(right)
		}
		SqlExpr::Case {
			operand,
			conditions,
			else_result,
			..
		} => {
			operand
				.as_deref()
				.is_some_and(expr_contains_nonaggregate_column_reference)
				|| conditions.iter().any(|case_when| {
					expr_contains_nonaggregate_column_reference(&case_when.condition)
						|| expr_contains_nonaggregate_column_reference(&case_when.result)
				}) || else_result
				.as_deref()
				.is_some_and(expr_contains_nonaggregate_column_reference)
		}
		SqlExpr::Cast { expr, .. } | SqlExpr::Extract { expr, .. } => {
			expr_contains_nonaggregate_column_reference(expr)
		}
		SqlExpr::Convert { expr, styles, .. } => {
			expr_contains_nonaggregate_column_reference(expr)
				|| styles
					.iter()
					.any(expr_contains_nonaggregate_column_reference)
		}
		SqlExpr::Interval(SqlInterval { value, .. }) => {
			expr_contains_nonaggregate_column_reference(value)
		}
		SqlExpr::AtTimeZone {
			timestamp,
			time_zone,
		} => {
			expr_contains_nonaggregate_column_reference(timestamp)
				|| expr_contains_nonaggregate_column_reference(time_zone)
		}
		SqlExpr::UnaryOp { expr, .. }
		| SqlExpr::IsNull(expr)
		| SqlExpr::IsNotNull(expr)
		| SqlExpr::Nested(expr) => expr_contains_nonaggregate_column_reference(expr),
		_ => false,
	}
}

fn function_args_contain_nonaggregate_column_reference(function: &Function) -> bool {
	let FunctionArguments::List(list) = &function.args else {
		return false;
	};
	list.args.iter().any(|arg| match arg {
		FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
		| FunctionArg::Named {
			arg: FunctionArgExpr::Expr(expr),
			..
		}
		| FunctionArg::ExprNamed {
			arg: FunctionArgExpr::Expr(expr),
			..
		} => expr_contains_nonaggregate_column_reference(expr),
		_ => false,
	})
}

fn aggregate_projection_matches_natural(
	projection: &[SelectItem],
	mode: AggregateSqlMode,
	catalog: &AggregateOutputCatalog,
	planner: &SqlPlanner,
) -> Result<bool, FrontendError> {
	if projection.len() != catalog.group_keys.len() + catalog.entries.len() {
		return Ok(false);
	}

	for item in projection {
		if matches!(item, SelectItem::ExprWithAlias { .. }) {
			return Ok(false);
		}
	}

	for (item, key) in projection.iter().zip(catalog.group_keys.iter()) {
		let SelectItem::UnnamedExpr(expr) = item else {
			return Ok(false);
		};
		let expr = strip_nested_sql_expr(expr);
		if !matches!(
			expr,
			SqlExpr::Identifier(_) | SqlExpr::CompoundIdentifier(_)
		) {
			return Ok(false);
		}
		let projected = planner.lower_expr(expr)?;
		if !expr_structural_eq(
			&canonical_aggregate_signature_expr(&projected),
			&canonical_aggregate_signature_expr(&key.expr),
		) {
			return Ok(false);
		}
	}

	for (item, entry) in projection
		.iter()
		.skip(catalog.group_keys.len())
		.zip(&catalog.entries)
	{
		let SelectItem::UnnamedExpr(expr) = item else {
			return Ok(false);
		};
		let SqlExpr::Function(function) = strip_nested_sql_expr(expr) else {
			return Ok(false);
		};
		if aggregate_function_name(&function.name).is_none() {
			return Ok(false);
		}
		let call = planner.lower_aggregate_function(function, mode)?;
		let signature = grouped_aggregate_signature(&call.function, &call.args, call.distinct);
		if entry.signature.is_some() && signature != entry.signature {
			return Ok(false);
		}
		if entry.signature.is_none() && signature.is_some() {
			return Ok(false);
		}
	}

	Ok(true)
}

fn aggregate_function_name(name: &ObjectName) -> Option<&'static str> {
	let [part] = name.0.as_slice() else {
		return None;
	};
	let ident = part.as_ident()?;
	let upper = ident.value.to_ascii_uppercase();
	if aggregate_kernel::is_stable_aggregate_sql_name(&upper) {
		return aggregate_kernel::stable_aggregate_sql_name(&upper);
	}
	aggregate_kernel::experimental_aggregate_sql_name(&upper)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConditionalSugarFunction {
	Coalesce,
	Nullif,
	If,
	Iif,
	Ifnull,
	Nvl,
	Nvl2,
}

fn conditional_sugar_function_name(name: &ObjectName) -> Option<ConditionalSugarFunction> {
	let [part] = name.0.as_slice() else {
		return None;
	};
	let ident = part.as_ident()?;
	match ident.value.to_ascii_uppercase().as_str() {
		"COALESCE" => Some(ConditionalSugarFunction::Coalesce),
		"NULLIF" => Some(ConditionalSugarFunction::Nullif),
		"IF" => Some(ConditionalSugarFunction::If),
		"IIF" => Some(ConditionalSugarFunction::Iif),
		"IFNULL" => Some(ConditionalSugarFunction::Ifnull),
		"NVL" => Some(ConditionalSugarFunction::Nvl),
		"NVL2" => Some(ConditionalSugarFunction::Nvl2),
		_ => None,
	}
}

fn function_positional_args<'a>(
	function: &'a Function,
	name: &str,
) -> Result<Vec<&'a SqlExpr>, FrontendError> {
	if function.uses_odbc_syntax
		|| !matches!(function.parameters, FunctionArguments::None)
		|| function.filter.is_some()
		|| function.null_treatment.is_some()
		|| function.over.is_some()
		|| !function.within_group.is_empty()
	{
		return Err(unsupported(format!(
			"{name} does not support function modifiers"
		)));
	}

	let FunctionArguments::List(FunctionArgumentList {
		duplicate_treatment,
		args,
		clauses,
	}) = &function.args
	else {
		return Err(unsupported(format!("{name} requires positional arguments")));
	};
	if duplicate_treatment.is_some() || !clauses.is_empty() {
		return Err(unsupported(format!(
			"{name} does not support argument clauses"
		)));
	}
	args.iter()
		.map(|arg| match arg {
			FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => Ok(expr),
			_ => Err(unsupported(format!(
				"{name} supports only plain positional arguments"
			))),
		})
		.collect()
}

fn rewrite_function_argument_exprs<F>(
	args: &FunctionArguments,
	mut rewrite: F,
) -> Result<FunctionArguments, FrontendError>
where
	F: FnMut(&SqlExpr) -> Result<SqlExpr, FrontendError>,
{
	match args {
		FunctionArguments::List(list) => {
			let args = list
				.args
				.iter()
				.map(|arg| match arg {
					FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
						Ok(FunctionArg::Unnamed(FunctionArgExpr::Expr(rewrite(expr)?)))
					}
					FunctionArg::Named {
						name,
						arg: FunctionArgExpr::Expr(expr),
						operator,
					} => Ok(FunctionArg::Named {
						name: name.clone(),
						arg: FunctionArgExpr::Expr(rewrite(expr)?),
						operator: operator.clone(),
					}),
					FunctionArg::ExprNamed {
						name,
						arg: FunctionArgExpr::Expr(expr),
						operator,
					} => Ok(FunctionArg::ExprNamed {
						name: rewrite(name)?,
						arg: FunctionArgExpr::Expr(rewrite(expr)?),
						operator: operator.clone(),
					}),
					_ => Ok(arg.clone()),
				})
				.collect::<Result<Vec<_>, FrontendError>>()?;
			Ok(FunctionArguments::List(FunctionArgumentList {
				duplicate_treatment: list.duplicate_treatment.clone(),
				args,
				clauses: list.clauses.clone(),
			}))
		}
		_ => Ok(args.clone()),
	}
}

fn aggregate_output_name(function: &AggregateFunction) -> &'static str {
	if let Some(spec) = aggregate_kernel::stable_spec_for_function(function) {
		return spec.output_name;
	}
	match function {
		AggregateFunction::LinearRegress => "linreg",
		AggregateFunction::ApproxQuantile(_) => "quantile",
		AggregateFunction::CountAll
		| AggregateFunction::Count
		| AggregateFunction::Sum
		| AggregateFunction::Min
		| AggregateFunction::Max
		| AggregateFunction::Avg => unreachable!("stable aggregate metadata missing"),
	}
}

fn grouped_aggregate_signature(
	function: &AggregateFunction,
	args: &[Expr],
	distinct: bool,
) -> Option<GroupedAggregateSignature> {
	let kernel = aggregate_kernel::stable_kernel_for_function(function)?;
	let spec = aggregate_kernel::stable_spec(kernel);
	if !spec.grouped_supported {
		return None;
	}
	match spec.arity {
		aggregate_kernel::AggregateArity::Zero => Some(GroupedAggregateSignature {
			kernel,
			args: Vec::new(),
			distinct,
		}),
		aggregate_kernel::AggregateArity::One if args.len() == 1 => {
			Some(GroupedAggregateSignature {
				kernel,
				args: vec![canonical_aggregate_signature_expr(&args[0])],
				distinct,
			})
		}
		_ => None,
	}
}

fn canonical_aggregate_signature_expr(expr: &Expr) -> Expr {
	match expr {
		Expr::Column(column) => Expr::column(column.name.0.clone()),
		Expr::Literal(_) | Expr::UntypedNull => expr.clone(),
		Expr::LogicalAscription { expr, logical_type } => Expr::LogicalAscription {
			expr: Box::new(canonical_aggregate_signature_expr(expr)),
			logical_type: *logical_type,
		},
		Expr::SqlLogicalCast {
			expr,
			logical_name,
			try_cast,
		} => Expr::SqlLogicalCast {
			expr: Box::new(canonical_aggregate_signature_expr(expr)),
			logical_name: logical_name.clone(),
			try_cast: *try_cast,
		},
		Expr::SqlConvert {
			expr,
			target_logical_name,
		} => Expr::SqlConvert {
			expr: Box::new(canonical_aggregate_signature_expr(expr)),
			target_logical_name: target_logical_name.clone(),
		},
		Expr::Cast(inner, data_type) => Expr::Cast(
			Box::new(canonical_aggregate_signature_expr(inner)),
			data_type.clone(),
		),
		Expr::TryCast(inner, data_type) => Expr::TryCast(
			Box::new(canonical_aggregate_signature_expr(inner)),
			data_type.clone(),
		),
		Expr::Add(left, right) => canonical_binary_signature_expr(Expr::Add, left, right),
		Expr::Sub(left, right) => canonical_binary_signature_expr(Expr::Sub, left, right),
		Expr::Mul(left, right) => canonical_binary_signature_expr(Expr::Mul, left, right),
		Expr::Div(left, right) => canonical_binary_signature_expr(Expr::Div, left, right),
		Expr::Eq(left, right) => canonical_binary_signature_expr(Expr::Eq, left, right),
		Expr::Neq(left, right) => canonical_binary_signature_expr(Expr::Neq, left, right),
		Expr::Lt(left, right) => canonical_binary_signature_expr(Expr::Lt, left, right),
		Expr::Lte(left, right) => canonical_binary_signature_expr(Expr::Lte, left, right),
		Expr::Gt(left, right) => canonical_binary_signature_expr(Expr::Gt, left, right),
		Expr::Gte(left, right) => canonical_binary_signature_expr(Expr::Gte, left, right),
		Expr::And(left, right) => canonical_binary_signature_expr(Expr::And, left, right),
		Expr::Or(left, right) => canonical_binary_signature_expr(Expr::Or, left, right),
		Expr::Not(inner) => Expr::Not(Box::new(canonical_aggregate_signature_expr(inner))),
		Expr::InList {
			expr,
			list,
			negated,
		} => Expr::InList {
			expr: Box::new(canonical_aggregate_signature_expr(expr)),
			list: list
				.iter()
				.map(canonical_aggregate_signature_expr)
				.collect(),
			negated: *negated,
		},
		Expr::Like {
			expr,
			pattern,
			negated,
			case_insensitive,
		} => Expr::Like {
			expr: Box::new(canonical_aggregate_signature_expr(expr)),
			pattern: pattern.clone(),
			negated: *negated,
			case_insensitive: *case_insensitive,
		},
		Expr::Case {
			when_then,
			else_expr,
		} => Expr::Case {
			when_then: when_then
				.iter()
				.map(|(condition, result)| {
					(
						Box::new(canonical_aggregate_signature_expr(condition)),
						Box::new(canonical_aggregate_signature_expr(result)),
					)
				})
				.collect(),
			else_expr: else_expr
				.as_ref()
				.map(|expr| Box::new(canonical_aggregate_signature_expr(expr))),
		},
		Expr::IsNull(inner) => Expr::IsNull(Box::new(canonical_aggregate_signature_expr(inner))),
		Expr::IsNotNull(inner) => {
			Expr::IsNotNull(Box::new(canonical_aggregate_signature_expr(inner)))
		}
		Expr::TimeBucket(interval, inner) => Expr::TimeBucket(
			*interval,
			Box::new(canonical_aggregate_signature_expr(inner)),
		),
		Expr::AtTimeZone(inner, zone) => Expr::AtTimeZone(
			Box::new(canonical_aggregate_signature_expr(inner)),
			zone.clone(),
		),
		Expr::Extract(field, inner) => {
			Expr::Extract(*field, Box::new(canonical_aggregate_signature_expr(inner)))
		}
		Expr::DateTrunc(field, inner) => {
			Expr::DateTrunc(*field, Box::new(canonical_aggregate_signature_expr(inner)))
		}
	}
}

fn canonical_binary_signature_expr(
	constructor: fn(Box<Expr>, Box<Expr>) -> Expr,
	left: &Expr,
	right: &Expr,
) -> Expr {
	constructor(
		Box::new(canonical_aggregate_signature_expr(left)),
		Box::new(canonical_aggregate_signature_expr(right)),
	)
}

struct AggregateFunctionArgs<'a> {
	args: &'a [FunctionArg],
	distinct: bool,
}

fn aggregate_function_args(
	function: &Function,
) -> Result<AggregateFunctionArgs<'_>, FrontendError> {
	// P0.4: global experimental aggregate calls still lower to the existing IR; the binder's
	// allow_experimental_kernels gate remains the authority for executing those kernels.
	if function.uses_odbc_syntax
		|| !matches!(function.parameters, FunctionArguments::None)
		|| function.filter.is_some()
		|| function.null_treatment.is_some()
		|| function.over.is_some()
		|| !function.within_group.is_empty()
	{
		return Err(unsupported(
			"aggregate function modifiers are not supported",
		));
	}

	let FunctionArguments::List(FunctionArgumentList {
		duplicate_treatment,
		args,
		clauses,
	}) = &function.args
	else {
		return Err(unsupported("aggregate function requires arguments"));
	};
	let distinct = match duplicate_treatment {
		None => false,
		Some(DuplicateTreatment::Distinct) => true,
		Some(DuplicateTreatment::All) => {
			return Err(unsupported(
				"aggregate function modifiers are not supported",
			));
		}
	};
	if !clauses.is_empty() {
		return Err(unsupported(
			"aggregate function modifiers are not supported",
		));
	}
	Ok(AggregateFunctionArgs { args, distinct })
}

fn table_factor_is_derived(factor: &TableFactor) -> bool {
	matches!(factor, TableFactor::Derived { .. })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JoinSide {
	Left,
	Right,
}

#[derive(Debug)]
enum ExistsConjunct {
	CorrelatedPair { outer: Expr, inner: Expr },
	InnerResidual(Expr),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ColumnScope {
	Outer,
	Inner,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JoinFilterTarget {
	Left,
	Right,
}

#[derive(Default)]
struct ScopeRefs {
	outer: bool,
	inner: bool,
	unsupported: bool,
}

fn split_top_level_and(expr: &SqlExpr) -> Vec<&SqlExpr> {
	let expr = strip_nested_sql_expr(expr);
	match expr {
		SqlExpr::BinaryOp { left, op, right } if *op == BinaryOperator::And => {
			let mut conjuncts = split_top_level_and(left);
			conjuncts.extend(split_top_level_and(right));
			conjuncts
		}
		_ => vec![expr],
	}
}

fn combine_and_exprs(exprs: Vec<Expr>) -> Option<Expr> {
	let mut exprs = exprs.into_iter();
	let first = exprs.next()?;
	Some(exprs.fold(first, |left, right| {
		Expr::And(Box::new(left), Box::new(right))
	}))
}

fn combine_and_sql_exprs(exprs: &[&SqlExpr]) -> Option<SqlExpr> {
	let mut exprs = exprs.iter().copied();
	let first = exprs.next()?.clone();
	Some(exprs.fold(first, |left, right| SqlExpr::BinaryOp {
		left: Box::new(left),
		op: BinaryOperator::And,
		right: Box::new(right.clone()),
	}))
}

fn join_operator_kind_for_filter_pushdown(operator: &JoinOperator) -> Option<JoinKind> {
	match operator {
		JoinOperator::Join(_) | JoinOperator::Inner(_) => Some(JoinKind::Inner),
		JoinOperator::Left(_) | JoinOperator::LeftOuter(_) => Some(JoinKind::LeftOuter),
		JoinOperator::Right(_) | JoinOperator::RightOuter(_) => Some(JoinKind::RightOuter),
		JoinOperator::FullOuter(_) => Some(JoinKind::FullOuter),
		JoinOperator::Semi(_) | JoinOperator::LeftSemi(_) => Some(JoinKind::LeftSemi),
		JoinOperator::RightSemi(_) => Some(JoinKind::RightSemi),
		JoinOperator::Anti(_) | JoinOperator::LeftAnti(_) => Some(JoinKind::LeftAnti),
		JoinOperator::RightAnti(_) => Some(JoinKind::RightAnti),
		JoinOperator::CrossJoin(_)
		| JoinOperator::CrossApply
		| JoinOperator::OuterApply
		| JoinOperator::AsOf { .. }
		| JoinOperator::StraightJoin(_)
		| JoinOperator::ArrayJoin
		| JoinOperator::LeftArrayJoin
		| JoinOperator::InnerArrayJoin => None,
	}
}

fn validate_exists_query_shape(query: &Query) -> Result<(), FrontendError> {
	if query.with.is_some()
		|| query.order_by.is_some()
		|| query_has_limit_or_offset(query.limit_clause.as_ref())
		|| query.fetch.is_some()
		|| !query.locks.is_empty()
		|| query.for_clause.is_some()
		|| query.settings.is_some()
		|| query.format_clause.is_some()
		|| !query.pipe_operators.is_empty()
	{
		return Err(unsupported("unsupported EXISTS subquery shape"));
	}
	Ok(())
}

fn validate_derived_query_shape(query: &Query) -> Result<&Select, FrontendError> {
	if query.with.is_some()
		|| query.fetch.is_some()
		|| !query.locks.is_empty()
		|| query.for_clause.is_some()
		|| query.settings.is_some()
		|| query.format_clause.is_some()
		|| !query.pipe_operators.is_empty()
	{
		return Err(unsupported("unsupported derived table query shape"));
	}

	match query.body.as_ref() {
		SetExpr::Select(select) => Ok(select.as_ref()),
		_ => Err(unsupported("unsupported derived table query body")),
	}
}

fn validate_derived_select_shape(select: &Select) -> Result<(), FrontendError> {
	if select.top.is_some()
		|| select.into.is_some()
		|| !select.lateral_views.is_empty()
		|| select.prewhere.is_some()
		|| !select.cluster_by.is_empty()
		|| !select.distribute_by.is_empty()
		|| !select.sort_by.is_empty()
		|| !select.named_window.is_empty()
		|| select.qualify.is_some()
		|| select.value_table_mode.is_some()
		|| !select.connect_by.is_empty()
		|| !select.optimizer_hints.is_empty()
		|| select.select_modifiers.is_some()
		|| select.exclude.is_some()
		|| !matches!(select.flavor, sqlparser::ast::SelectFlavor::Standard)
	{
		return Err(unsupported("unsupported derived table select shape"));
	}
	if projection_contains_window_function(&select.projection) {
		return Err(unsupported("unsupported derived table projection"));
	}
	if select.from.len() != 1 {
		return Err(unsupported(
			"derived table subquery must have exactly one FROM source",
		));
	}
	let source = &select.from[0];
	if !source.joins.is_empty() {
		return Err(unsupported("joins inside derived tables are not supported"));
	}
	match &source.relation {
		TableFactor::Table { .. } => Ok(()),
		TableFactor::Derived { .. } => Err(unsupported("nested derived tables are not supported")),
		_ => Err(unsupported("unsupported derived table source")),
	}
}

fn validate_exists_select_shape(select: &Select) -> Result<(), FrontendError> {
	if select.top.is_some()
		|| select.into.is_some()
		|| select.from.len() != 1
		|| !select.from[0].joins.is_empty()
		|| !select.lateral_views.is_empty()
		|| select.prewhere.is_some()
		|| !group_by_is_empty(&select.group_by)
		|| !select.cluster_by.is_empty()
		|| !select.distribute_by.is_empty()
		|| !select.sort_by.is_empty()
		|| select.having.is_some()
		|| !select.named_window.is_empty()
		|| select.qualify.is_some()
		|| select.value_table_mode.is_some()
		|| !select.connect_by.is_empty()
		|| !select.optimizer_hints.is_empty()
		|| select.select_modifiers.is_some()
		|| select.exclude.is_some()
		|| !matches!(select.flavor, sqlparser::ast::SelectFlavor::Standard)
	{
		return Err(unsupported("unsupported EXISTS subquery shape"));
	}
	Ok(())
}

fn validate_exists_projection(projection: &[SelectItem]) -> Result<(), FrontendError> {
	for item in projection {
		match item {
			SelectItem::Wildcard(_) => {}
			SelectItem::UnnamedExpr(expr) if exists_projection_expr_supported(expr) => {}
			_ => return Err(unsupported("unsupported EXISTS subquery projection")),
		}
	}
	Ok(())
}

fn exists_projection_expr_supported(expr: &SqlExpr) -> bool {
	match strip_nested_sql_expr(expr) {
		SqlExpr::Identifier(_) | SqlExpr::CompoundIdentifier(_) => true,
		SqlExpr::Value(value) => matches!(
			&value.value,
			Value::Number(_, _) | Value::SingleQuotedString(_) | Value::Boolean(_)
		),
		_ => false,
	}
}

fn group_by_is_empty(group_by: &GroupByExpr) -> bool {
	match group_by {
		GroupByExpr::Expressions(exprs, modifiers) => exprs.is_empty() && modifiers.is_empty(),
		GroupByExpr::All(_) => false,
	}
}

fn parse_non_negative_usize_literal(expr: &SqlExpr) -> Result<usize, FrontendError> {
	match stripped_sql_value(expr) {
		Some(Value::Number(number, _)) => {
			if number.starts_with('-')
				|| number.contains('.')
				|| number.contains('e')
				|| number.contains('E')
			{
				return Err(unsupported("LIMIT/OFFSET expects a non-negative integer"));
			}
			number
				.parse::<usize>()
				.map_err(|_| unsupported("LIMIT/OFFSET expects a non-negative integer"))
		}
		_ => Err(unsupported("LIMIT/OFFSET expects a non-negative integer")),
	}
}

fn strip_nested_sql_expr(expr: &SqlExpr) -> &SqlExpr {
	match expr {
		SqlExpr::Nested(inner) => strip_nested_sql_expr(inner),
		_ => expr,
	}
}

fn scoped_column_expr(
	expr: &SqlExpr,
	outer_qualifiers: &[TableRef],
	inner_qualifiers: &[TableRef],
) -> Result<Option<(ColumnScope, Expr)>, FrontendError> {
	let Some((qualifier, column)) = sql_column_ref(expr)? else {
		return Ok(None);
	};
	let scope = resolve_column_scope(&qualifier, outer_qualifiers, inner_qualifiers)?;
	Ok(Some((scope, Expr::qualified_column(qualifier, column))))
}

fn sql_column_ref(expr: &SqlExpr) -> Result<Option<(TableRef, String)>, FrontendError> {
	match strip_nested_sql_expr(expr) {
		SqlExpr::CompoundIdentifier(idents) if idents.len() == 2 => Ok(Some((
			TableRef::new(idents[0].value.clone()),
			idents[1].value.clone(),
		))),
		SqlExpr::Identifier(_) | SqlExpr::CompoundIdentifier(_) => {
			Err(unsupported("unsupported EXISTS column reference"))
		}
		_ => Ok(None),
	}
}

fn resolve_column_scope(
	qualifier: &TableRef,
	outer_qualifiers: &[TableRef],
	inner_qualifiers: &[TableRef],
) -> Result<ColumnScope, FrontendError> {
	let outer_matches = outer_qualifiers
		.iter()
		.filter(|candidate| *candidate == qualifier)
		.count();
	let inner_matches = inner_qualifiers
		.iter()
		.filter(|candidate| *candidate == qualifier)
		.count();
	match (outer_matches, inner_matches) {
		(1, 0) => Ok(ColumnScope::Outer),
		(0, 1) => Ok(ColumnScope::Inner),
		_ => Err(unsupported("unsupported EXISTS column scope")),
	}
}

fn collect_scope_refs(
	expr: &SqlExpr,
	outer_qualifiers: &[TableRef],
	inner_qualifiers: &[TableRef],
	refs: &mut ScopeRefs,
) -> Result<(), FrontendError> {
	match strip_nested_sql_expr(expr) {
		SqlExpr::Identifier(_) | SqlExpr::CompoundIdentifier(_) => {
			let Some((qualifier, _)) = sql_column_ref(expr)? else {
				return Ok(());
			};
			match resolve_column_scope(&qualifier, outer_qualifiers, inner_qualifiers) {
				Ok(ColumnScope::Outer) => refs.outer = true,
				Ok(ColumnScope::Inner) => refs.inner = true,
				Err(_) => refs.unsupported = true,
			}
		}
		SqlExpr::Exists { .. } | SqlExpr::Subquery(_) => refs.unsupported = true,
		SqlExpr::BinaryOp { left, right, .. } => {
			collect_scope_refs(left, outer_qualifiers, inner_qualifiers, refs)?;
			collect_scope_refs(right, outer_qualifiers, inner_qualifiers, refs)?;
		}
		SqlExpr::InList { expr, list, .. } => {
			collect_scope_refs(expr, outer_qualifiers, inner_qualifiers, refs)?;
			for item in list {
				collect_scope_refs(item, outer_qualifiers, inner_qualifiers, refs)?;
			}
		}
		SqlExpr::Like { expr, pattern, .. } | SqlExpr::ILike { expr, pattern, .. } => {
			collect_scope_refs(expr, outer_qualifiers, inner_qualifiers, refs)?;
			collect_scope_refs(pattern, outer_qualifiers, inner_qualifiers, refs)?;
		}
		SqlExpr::Between {
			expr, low, high, ..
		} => {
			collect_scope_refs(expr, outer_qualifiers, inner_qualifiers, refs)?;
			collect_scope_refs(low, outer_qualifiers, inner_qualifiers, refs)?;
			collect_scope_refs(high, outer_qualifiers, inner_qualifiers, refs)?;
		}
		SqlExpr::Case {
			operand,
			conditions,
			else_result,
			..
		} => {
			if let Some(operand) = operand {
				collect_scope_refs(operand, outer_qualifiers, inner_qualifiers, refs)?;
			}
			for case_when in conditions {
				collect_scope_refs(
					&case_when.condition,
					outer_qualifiers,
					inner_qualifiers,
					refs,
				)?;
				collect_scope_refs(&case_when.result, outer_qualifiers, inner_qualifiers, refs)?;
			}
			if let Some(else_result) = else_result {
				collect_scope_refs(else_result, outer_qualifiers, inner_qualifiers, refs)?;
			}
		}
		SqlExpr::Cast { expr, .. } => {
			collect_scope_refs(expr, outer_qualifiers, inner_qualifiers, refs)?;
		}
		SqlExpr::IsNull(inner) | SqlExpr::IsNotNull(inner) => {
			collect_scope_refs(inner, outer_qualifiers, inner_qualifiers, refs)?;
		}
		SqlExpr::Function(function) => match &function.args {
			sqlparser::ast::FunctionArguments::List(list) => {
				for arg in &list.args {
					match arg {
						FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
						| FunctionArg::Named {
							arg: FunctionArgExpr::Expr(expr),
							..
						}
						| FunctionArg::ExprNamed {
							arg: FunctionArgExpr::Expr(expr),
							..
						} => {
							collect_scope_refs(expr, outer_qualifiers, inner_qualifiers, refs)?;
						}
						FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => {}
						_ => refs.unsupported = true,
					}
				}
			}
			sqlparser::ast::FunctionArguments::Subquery(_) => refs.unsupported = true,
			_ => {}
		},
		SqlExpr::Value(_) => {}
		_ => {}
	}
	Ok(())
}

fn join_filter_conjunct_matches(
	conjunct: &SqlExpr,
	left_qualifiers: &[TableRef],
	right_qualifiers: &[TableRef],
	target: JoinFilterTarget,
) -> bool {
	let mut refs = ScopeRefs::default();
	if collect_scope_refs(conjunct, left_qualifiers, right_qualifiers, &mut refs).is_err() {
		return false;
	}
	match target {
		JoinFilterTarget::Left => refs.outer && !refs.inner && !refs.unsupported,
		JoinFilterTarget::Right => refs.inner && !refs.outer && !refs.unsupported,
	}
}

fn alias_column_names(alias: &TableAlias) -> Result<Vec<String>, FrontendError> {
	if alias
		.columns
		.iter()
		.any(|column| column.data_type.is_some())
	{
		return Err(unsupported(
			"table alias column type annotations are not supported",
		));
	}
	Ok(alias
		.columns
		.iter()
		.map(|column| column.name.value.clone())
		.collect())
}

fn alias_plan(plan: LogicalPlan, alias: Option<&TableAlias>) -> Result<LogicalPlan, FrontendError> {
	match alias {
		Some(alias) => Ok(LogicalPlan::Alias {
			input: Box::new(plan),
			alias: TableRef::new(alias.name.value.clone()),
			columns: alias_column_names(alias)?,
		}),
		None => Ok(plan),
	}
}

fn plan_qualifiers(plan: &LogicalPlan) -> Vec<TableRef> {
	match plan {
		LogicalPlan::ScanNamed { table, .. } => vec![table.clone()],
		LogicalPlan::Alias { alias, .. } => vec![alias.clone()],
		LogicalPlan::Filter { input, .. }
		| LogicalPlan::Project { input, .. }
		| LogicalPlan::Distinct { input }
		| LogicalPlan::Aggregate { input, .. }
		| LogicalPlan::Exchange { input, .. }
		| LogicalPlan::Sort { input, .. }
		| LogicalPlan::Window { input, .. }
		| LogicalPlan::Take { input, .. }
		| LogicalPlan::Repeat { input, .. } => plan_qualifiers(input),
		LogicalPlan::Join {
			left, right, kind, ..
		} => match kind {
			JoinKind::LeftSemi | JoinKind::LeftAnti => plan_qualifiers(left),
			JoinKind::RightSemi | JoinKind::RightAnti => plan_qualifiers(right),
			JoinKind::Inner
			| JoinKind::Cross
			| JoinKind::LeftOuter
			| JoinKind::RightOuter
			| JoinKind::FullOuter => {
				let mut qualifiers = plan_qualifiers(left);
				qualifiers.extend(plan_qualifiers(right));
				qualifiers
			}
		},
		LogicalPlan::Union { .. } | LogicalPlan::SetOpAll { .. } => Vec::new(),
		LogicalPlan::Scan { .. }
		| LogicalPlan::Explain { .. }
		| LogicalPlan::ExplainAnalyze { .. }
		| LogicalPlan::CreateTable { .. }
		| LogicalPlan::CreateLogicalType { .. }
		| LogicalPlan::CreateTableAsSelect { .. }
		| LogicalPlan::DropTable { .. }
		| LogicalPlan::Delete { .. }
		| LogicalPlan::Update { .. }
		| LogicalPlan::Insert { .. }
		| LogicalPlan::InsertSelect { .. } => Vec::new(),
	}
}

fn expr_side(expr: &Expr, left: &[TableRef], right: &[TableRef]) -> Option<JoinSide> {
	let Expr::Column(column_ref) = expr else {
		return None;
	};
	let qualifier = column_ref.qualifier.as_ref()?;
	if left.iter().any(|left_ref| left_ref == qualifier) {
		Some(JoinSide::Left)
	} else if right.iter().any(|right_ref| right_ref == qualifier) {
		Some(JoinSide::Right)
	} else {
		None
	}
}

fn join_strategy_intent_for_hint(
	broadcast_hint: &mut Option<BroadcastJoinHint>,
	left: &LogicalPlan,
	right: &LogicalPlan,
) -> Result<JoinStrategyIntent, FrontendError> {
	let Some(hint) = broadcast_hint.as_mut() else {
		return Ok(JoinStrategyIntent::Auto);
	};
	if hint.consumed {
		return Ok(JoinStrategyIntent::Auto);
	}

	let left_qualifiers = plan_qualifiers(left);
	let right_qualifiers = plan_qualifiers(right);
	let left_matches = qualifier_match_count(&left_qualifiers, &hint.target);
	let right_matches = qualifier_match_count(&right_qualifiers, &hint.target);

	if left_matches + right_matches > 1 {
		return Err(unsupported(format!(
			"broadcast hint target `{}` is ambiguous",
			hint.target.table
		)));
	}
	if left_matches > 0 {
		if left_qualifiers.len() != 1 {
			return Err(unsupported(format!(
				"broadcast hint target `{}` is inside a composite left join input",
				hint.target.table
			)));
		}
		hint.consumed = true;
		return Ok(JoinStrategyIntent::Pinned {
			strategy: JoinStrategy::Broadcast,
			source: JoinStrategyPinSource::SqlHint,
			build_side: Some(BroadcastBuildSide::Left),
		});
	}
	if right_matches > 0 {
		if right_qualifiers.len() != 1 {
			return Err(unsupported(format!(
				"broadcast hint target `{}` is inside a composite right join input",
				hint.target.table
			)));
		}
		hint.consumed = true;
		return Ok(JoinStrategyIntent::Pinned {
			strategy: JoinStrategy::Broadcast,
			source: JoinStrategyPinSource::SqlHint,
			build_side: Some(BroadcastBuildSide::Right),
		});
	}

	Ok(JoinStrategyIntent::Auto)
}

fn qualifier_match_count(qualifiers: &[TableRef], target: &TableRef) -> usize {
	qualifiers
		.iter()
		.filter(|qualifier| *qualifier == target)
		.count()
}

fn reject_unused_broadcast_hint(
	broadcast_hint: &Option<BroadcastJoinHint>,
) -> Result<(), FrontendError> {
	if let Some(hint) = broadcast_hint
		&& !hint.consumed
	{
		return Err(unsupported(format!(
			"broadcast hint target `{}` did not match a left/build join input",
			hint.target.table
		)));
	}
	Ok(())
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn insert_table_function_target_rejects_after_parse() {
		let dialect = sqlparser::dialect::ClickHouseDialect {};
		let ast = Parser::parse_sql(&dialect, "INSERT INTO FUNCTION f() VALUES (1)")
			.expect("parse table-function insert");
		let Statement::Insert(insert) = &ast[0] else {
			panic!("expected insert");
		};

		let err = SqlPlanner::new()
			.lower_insert(insert)
			.expect_err("table function target should reject");

		assert!(
			err.to_string()
				.contains("table functions are not supported")
		);
	}

	#[test]
	fn create_table_timestamptz_lowers_to_utc_timestamp() {
		let plan = SqlPlanner::new()
			.plan("CREATE TABLE events (ts TIMESTAMPTZ)")
			.expect("plan timestamptz table");
		let LogicalPlan::CreateTable { schema, .. } = plan else {
			panic!("expected CREATE TABLE");
		};
		let CreateTableColumnType::Physical(data_type) = &schema.columns[0].column_type else {
			panic!("expected physical column");
		};

		assert_eq!(
			data_type,
			&DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
		);
	}

	#[test]
	fn timestamp_without_timezone_is_rejected() {
		let err = SqlPlanner::new()
			.plan("CREATE TABLE events (ts TIMESTAMP)")
			.expect_err("naive timestamp should reject");

		assert!(err.to_string().contains("TIMESTAMP WITHOUT TIME ZONE"));
	}

	#[test]
	fn timestamptz_typed_literal_requires_explicit_offset() {
		let err = SqlPlanner::new()
			.plan("SELECT TIMESTAMPTZ '2024-01-01 00:00:00' FROM events")
			.expect_err("offset-less timestamptz should reject");

		let message = err.to_string();
		assert!(message.contains("explicit offset"), "{message}");
	}

	#[test]
	fn classifier_recognizes_correlated_equi_pair() {
		let planner = SqlPlanner::new();
		let classified = planner
			.classify_exists_conjunct(
				&parse_selection("SELECT * FROM fact WHERE dim.k = fact.k"),
				&[TableRef::new("fact")],
				&[TableRef::new("dim")],
			)
			.expect("classify");

		match classified {
			ExistsConjunct::CorrelatedPair { outer, inner } => {
				assert_qualified_column(&outer, "fact", "k");
				assert_qualified_column(&inner, "dim", "k");
			}
			ExistsConjunct::InnerResidual(_) => panic!("expected correlated pair"),
		}
	}

	#[test]
	fn classifier_recognizes_inner_only_residual_including_literals() {
		let planner = SqlPlanner::new();
		for sql in [
			"SELECT * FROM fact WHERE dim.active = true",
			"SELECT * FROM fact WHERE dim.score > 10",
		] {
			let classified = planner
				.classify_exists_conjunct(
					&parse_selection(sql),
					&[TableRef::new("fact")],
					&[TableRef::new("dim")],
				)
				.expect("classify residual");

			assert!(
				matches!(classified, ExistsConjunct::InnerResidual(_)),
				"{sql}"
			);
		}
	}

	#[test]
	fn classifier_rejects_outer_only_subquery_predicate() {
		assert_classifier_rejects("SELECT * FROM fact WHERE fact.flag = true");
	}

	#[test]
	fn classifier_rejects_non_equi_and_computed_correlation() {
		assert_classifier_rejects("SELECT * FROM fact WHERE dim.k > fact.k");
		assert_classifier_rejects("SELECT * FROM fact WHERE dim.k + 1 = fact.k");
	}

	#[test]
	fn classifier_rejects_unqualified_ambiguous_correlation() {
		assert_classifier_rejects("SELECT * FROM fact WHERE k = fact.k");
		let planner = SqlPlanner::new();
		let err = planner
			.classify_exists_conjunct(
				&parse_selection("SELECT * FROM fact WHERE t.k = t.k"),
				&[TableRef::new("t")],
				&[TableRef::new("t")],
			)
			.expect_err("same-table unaliased self-correlation rejects");
		assert!(err.to_string().contains("unsupported"));

		let err = planner
			.classify_exists_conjunct(
				&parse_selection("SELECT * FROM fact WHERE dim.k = fact.k"),
				&[TableRef::new("fact"), TableRef::new("fact")],
				&[TableRef::new("dim")],
			)
			.expect_err("duplicate outer qualifier rejects");
		assert!(err.to_string().contains("unsupported"));
	}

	#[test]
	fn sql_inner_join_emits_auto_intent_without_exchanges() {
		let plan = SqlPlanner::new()
			.plan("SELECT * FROM orders INNER JOIN customers ON orders.customer_id = customers.id")
			.expect("plan join");

		match plan {
			LogicalPlan::Join {
				left,
				right,
				kind,
				strategy_intent,
				on,
				..
			} => {
				assert_eq!(kind, JoinKind::Inner);
				assert_eq!(strategy_intent, JoinStrategyIntent::Auto);
				assert_eq!(on.len(), 1);
				assert!(!matches!(*left, LogicalPlan::Exchange { .. }));
				assert!(!matches!(*right, LogicalPlan::Exchange { .. }));
			}
			other => panic!("expected join plan, got {other:?}"),
		}
	}

	#[test]
	fn sql_inner_join_pushes_right_local_conjunct_to_right_input() {
		let plan = SqlPlanner::new()
			.plan(
				"SELECT * FROM orders o \
				 INNER JOIN customers c ON o.customer_id = c.id \
				 WHERE c.region = 'north'",
			)
			.expect("plan pushed right filter");

		match plan {
			LogicalPlan::Join { right, kind, .. } => {
				assert_eq!(kind, JoinKind::Inner);
				let predicate = assert_filter_on_alias(&right, "c");
				assert_expr_references(predicate, "c", "region");
				assert_eq!(filter_count(&right), 1);
			}
			other => panic!("expected join plan, got {other:?}"),
		}
	}

	#[test]
	fn sql_inner_join_pushes_right_local_like_conjunct_to_right_input() {
		let plan = SqlPlanner::new()
			.plan(
				"SELECT * FROM orders o \
				 INNER JOIN customers c ON o.customer_id = c.id \
				 WHERE c.region LIKE 'n%'",
			)
			.expect("plan pushed right LIKE filter");

		match plan {
			LogicalPlan::Join { right, kind, .. } => {
				assert_eq!(kind, JoinKind::Inner);
				let predicate = assert_filter_on_alias(&right, "c");
				assert_expr_references(predicate, "c", "region");
				assert_eq!(filter_count(&right), 1);
			}
			other => panic!("expected join plan, got {other:?}"),
		}
	}

	#[test]
	fn sql_inner_join_pushes_leftmost_local_conjunct_to_left_input() {
		let plan = SqlPlanner::new()
			.plan(
				"SELECT * FROM customers c \
				 INNER JOIN orders o ON c.id = o.customer_id \
				 WHERE c.region = 'north'",
			)
			.expect("plan pushed leftmost filter");

		match plan {
			LogicalPlan::Join { left, kind, .. } => {
				assert_eq!(kind, JoinKind::Inner);
				let predicate = assert_filter_on_alias(&left, "c");
				assert_expr_references(predicate, "c", "region");
				assert_eq!(filter_count(&left), 1);
			}
			other => panic!("expected join plan, got {other:?}"),
		}
	}

	#[test]
	fn sql_derived_join_digest_matches_hand_authored_logical_plan() {
		let observed = SqlPlanner::new()
			.plan(
				"SELECT o.id, c.amount \
				 FROM orders o \
				 INNER JOIN (SELECT id, amount FROM customers WHERE amount BETWEEN 10 AND 19) c \
				 ON o.customer_id = c.id",
			)
			.expect("plan SQL derived join");
		let hand_authored = LogicalPlan::Project {
			input: Box::new(LogicalPlan::Join {
				left: Box::new(LogicalPlan::Alias {
					input: Box::new(LogicalPlan::ScanNamed {
						table: TableRef::new("orders"),
						projection: None,
					}),
					alias: TableRef::new("o"),
					columns: Vec::new(),
				}),
				right: Box::new(LogicalPlan::Alias {
					input: Box::new(LogicalPlan::Project {
						input: Box::new(LogicalPlan::Filter {
							input: Box::new(LogicalPlan::ScanNamed {
								table: TableRef::new("customers"),
								projection: None,
							}),
							predicate: Expr::And(
								Box::new(Expr::Gte(
									Box::new(Expr::column("amount")),
									Box::new(Expr::Literal(ScalarValue::Int32(Some(10)))),
								)),
								Box::new(Expr::Lte(
									Box::new(Expr::column("amount")),
									Box::new(Expr::Literal(ScalarValue::Int32(Some(19)))),
								)),
							),
						}),
						exprs: vec![Expr::column("id"), Expr::column("amount")],
						output_names: vec![None, None],
					}),
					alias: TableRef::new("c"),
					columns: Vec::new(),
				}),
				kind: JoinKind::Inner,
				on: vec![(
					Expr::qualified_column(TableRef::new("o"), "customer_id"),
					Expr::qualified_column(TableRef::new("c"), "id"),
				)],
				null_safe_keys: false,
				strategy_intent: JoinStrategyIntent::Auto,
			}),
			exprs: vec![
				Expr::qualified_column(TableRef::new("o"), "id"),
				Expr::qualified_column(TableRef::new("c"), "amount"),
			],
			output_names: vec![None, None],
		};

		assert_eq!(format!("{observed:?}"), format!("{hand_authored:?}"));
	}

	#[test]
	fn sql_outer_join_around_derived_table_stays_rejected() {
		assert_plan_rejects(
			"SELECT * FROM orders o \
			 LEFT JOIN (SELECT id FROM customers) c ON o.customer_id = c.id",
			"derived tables in joins are only supported in all-inner chains",
		);
	}

	#[test]
	fn sql_join_inside_derived_table_stays_rejected() {
		assert_plan_rejects(
			"SELECT * FROM orders o \
			 INNER JOIN (SELECT c.id FROM customers c INNER JOIN orders o2 ON c.id = o2.customer_id) d \
			 ON o.customer_id = d.id",
			"joins inside derived tables are not supported",
		);
	}

	#[test]
	fn sql_nested_lateral_sampled_derived_join_fences_hold() {
		assert_plan_rejects(
			"SELECT * FROM orders o \
			 INNER JOIN (SELECT id FROM (SELECT id FROM customers) nested) c \
			 ON o.customer_id = c.id",
			"nested derived tables are not supported",
		);
		assert_plan_rejects(
			"SELECT * FROM orders o \
			 INNER JOIN LATERAL (SELECT id FROM customers) c ON o.customer_id = c.id",
			"lateral derived tables are not supported",
		);
		assert_plan_rejects(
			"SELECT * FROM orders o \
			 INNER JOIN (SELECT id FROM customers) c TABLESAMPLE SYSTEM (10) \
			 ON o.customer_id = c.id",
			"sampled derived tables are not supported",
		);
	}

	#[test]
	fn sql_multi_dim_star_pushes_each_filter_to_tightest_input() {
		let plan = SqlPlanner::new()
			.plan(
				"SELECT * FROM orders o \
				 INNER JOIN customers c1 ON o.customer_id = c1.id \
				 INNER JOIN customers c2 ON o.customer_id = c2.id \
				 WHERE c1.region = 'west' AND c2.region = 'east'",
			)
			.expect("plan multi-dim pushed filters");

		match plan {
			LogicalPlan::Join { left, right, .. } => {
				let outer_predicate = assert_filter_on_alias(&right, "c2");
				assert_expr_references(outer_predicate, "c2", "region");
				match *left {
					LogicalPlan::Join { right, .. } => {
						let inner_predicate = assert_filter_on_alias(&right, "c1");
						assert_expr_references(inner_predicate, "c1", "region");
					}
					other => panic!("expected nested join on left, got {other:?}"),
				}
			}
			other => panic!("expected join plan, got {other:?}"),
		}
	}

	#[test]
	fn sql_cross_side_conjunct_remains_residual() {
		let plan = SqlPlanner::new()
			.plan(
				"SELECT * FROM orders o \
				 INNER JOIN customers c ON o.customer_id = c.id \
				 WHERE o.id = c.id",
			)
			.expect("plan cross-side residual");

		match plan {
			LogicalPlan::Filter { input, predicate } => {
				assert_expr_references(&predicate, "o", "id");
				assert_expr_references(&predicate, "c", "id");
				assert!(matches!(*input, LogicalPlan::Join { .. }));
			}
			other => panic!("expected residual filter above join, got {other:?}"),
		}
	}

	#[test]
	fn sql_unqualified_conjunct_remains_residual() {
		let sql = "SELECT * FROM orders o INNER JOIN customers c ON o.customer_id = c.id WHERE region = 'north'";
		let plan = SqlPlanner::new().plan(sql).expect("plan residual filter");
		assert!(
			matches!(plan, LogicalPlan::Filter { .. }),
			"{sql}: {plan:?}"
		);
		assert_eq!(filter_count(&plan), 1, "{sql}: {plan:?}");
	}

	#[test]
	fn sql_constant_conjunct_remains_residual() {
		let sql =
			"SELECT * FROM orders o INNER JOIN customers c ON o.customer_id = c.id WHERE 1 = 1";
		let plan = SqlPlanner::new().plan(sql).expect("plan residual filter");
		assert!(
			matches!(plan, LogicalPlan::Filter { .. }),
			"{sql}: {plan:?}"
		);
		assert_eq!(filter_count(&plan), 1, "{sql}: {plan:?}");
	}

	#[test]
	fn sql_outer_join_chain_disables_filter_pushdown() {
		let plan = SqlPlanner::new()
			.plan(
				"SELECT c.id, b.x \
				 FROM a INNER JOIN b ON a.k = b.k \
				 RIGHT OUTER JOIN c ON c.j = b.j \
				 WHERE b.x = 1",
			)
			.expect("plan outer-chain residual");

		assert_eq!(filter_count(&plan), 1, "{plan:?}");
		match plan {
			LogicalPlan::Project { input, .. } => {
				assert!(matches!(*input, LogicalPlan::Filter { .. }));
			}
			other => panic!("expected project over residual filter, got {other:?}"),
		}
	}

	#[test]
	fn generic_dialect_preserves_select_optimizer_hints() {
		let select = parse_select(
			"SELECT /*+ BROADCAST(c) */ * FROM customers c INNER JOIN orders o ON c.id = o.customer_id",
		);

		assert_eq!(select.optimizer_hints.len(), 1);
		let hint = &select.optimizer_hints[0];
		assert_eq!(hint.prefix, "");
		assert_eq!(hint.text.trim(), "BROADCAST(c)");
		assert!(matches!(hint.style, OptimizerHintStyle::MultiLine));
	}

	#[test]
	fn sql_broadcast_hint_emits_broadcast_join_shape() {
		let plan = SqlPlanner::new()
			.plan(
				"SELECT /*+ BROADCAST(c) */ * FROM customers c INNER JOIN orders o ON c.id = o.customer_id",
			)
			.expect("plan broadcast join");

		match plan {
			LogicalPlan::Join {
				left,
				right,
				kind,
				strategy_intent,
				on,
				..
			} => {
				assert_eq!(kind, JoinKind::Inner);
				assert_eq!(
					strategy_intent,
					JoinStrategyIntent::Pinned {
						strategy: JoinStrategy::Broadcast,
						source: JoinStrategyPinSource::SqlHint,
						build_side: Some(BroadcastBuildSide::Left),
					}
				);
				assert_eq!(on.len(), 1);
				assert!(matches!(
					*left,
					LogicalPlan::Alias {
						ref alias,
						..
					} if alias == &TableRef::new("c")
				));
				assert!(matches!(
					*right,
					LogicalPlan::Alias {
						ref alias,
						..
					} if alias == &TableRef::new("o")
				));
			}
			other => panic!("expected join plan, got {other:?}"),
		}
	}

	#[test]
	fn sql_filter_pushdown_preserves_broadcast_hint_resolution() {
		let plan = SqlPlanner::new()
			.plan(
				"SELECT /*+ BROADCAST(c) */ * FROM customers c \
				 INNER JOIN orders o ON c.id = o.customer_id \
				 WHERE c.region = 'west'",
			)
			.expect("plan hinted pushed filter");

		match plan {
			LogicalPlan::Join {
				left,
				kind,
				strategy_intent,
				..
			} => {
				assert_eq!(kind, JoinKind::Inner);
				assert_eq!(
					strategy_intent,
					JoinStrategyIntent::Pinned {
						strategy: JoinStrategy::Broadcast,
						source: JoinStrategyPinSource::SqlHint,
						build_side: Some(BroadcastBuildSide::Left),
					}
				);
				match *left {
					LogicalPlan::Filter { ref predicate, .. } => {
						assert_expr_references(predicate, "c", "region");
					}
					other => panic!("expected pushed filter, got {other:?}"),
				}
			}
			other => panic!("expected hinted join plan, got {other:?}"),
		}
	}

	#[test]
	fn sql_broadcast_hint_targeting_probe_rejects() {
		let plan = SqlPlanner::new()
			.plan(
				"SELECT /*+ BROADCAST(o) */ * FROM customers c INNER JOIN orders o ON c.id = o.customer_id",
			)
			.expect("right-side inner broadcast hint should plan");
		let LogicalPlan::Join {
			strategy_intent, ..
		} = plan
		else {
			panic!("expected hinted join");
		};
		assert_eq!(
			strategy_intent,
			JoinStrategyIntent::Pinned {
				strategy: JoinStrategy::Broadcast,
				source: JoinStrategyPinSource::SqlHint,
				build_side: Some(BroadcastBuildSide::Right),
			}
		);
	}

	#[test]
	fn sql_broadcast_hint_non_inner_resolves_side_before_binder_validation() {
		let plan = SqlPlanner::new()
			.plan(
				"SELECT /*+ BROADCAST(c) */ * FROM customers c LEFT JOIN orders o ON c.id = o.customer_id",
			)
			.expect("frontend should resolve side before binder validates kind");
		let LogicalPlan::Join {
			kind,
			strategy_intent,
			..
		} = plan
		else {
			panic!("expected hinted join");
		};
		assert_eq!(kind, JoinKind::LeftOuter);
		assert_eq!(
			strategy_intent,
			JoinStrategyIntent::Pinned {
				strategy: JoinStrategy::Broadcast,
				source: JoinStrategyPinSource::SqlHint,
				build_side: Some(BroadcastBuildSide::Left),
			}
		);
	}

	#[test]
	fn sql_broadcast_hint_malformed_unknown_duplicate_rejects() {
		for sql in [
			"SELECT /*+ FOO(c) */ * FROM customers c INNER JOIN orders o ON c.id = o.customer_id",
			"SELECT /*+ BROADCAST() */ * FROM customers c INNER JOIN orders o ON c.id = o.customer_id",
			"SELECT /*+ BROADCAST(c, o) */ * FROM customers c INNER JOIN orders o ON c.id = o.customer_id",
			"SELECT /*+ BROADCAST(c) extra */ * FROM customers c INNER JOIN orders o ON c.id = o.customer_id",
			"SELECT /*toron+ BROADCAST(c) */ * FROM customers c INNER JOIN orders o ON c.id = o.customer_id",
			"SELECT --+ BROADCAST(c)\n * FROM customers c INNER JOIN orders o ON c.id = o.customer_id",
			"SELECT /*+ BROADCAST(c) */ /*+ BROADCAST(o) */ * FROM customers c INNER JOIN orders o ON c.id = o.customer_id",
		] {
			assert_plan_rejects(sql, "BROADCAST");
		}
	}

	#[test]
	fn sql_broadcast_hint_unused_rejects() {
		assert_plan_rejects(
			"SELECT /*+ BROADCAST(c) */ * FROM customers o INNER JOIN orders r ON o.id = r.customer_id",
			"did not match",
		);
		assert_plan_rejects(
			"SELECT /*+ BROADCAST(c) */ * FROM customers c",
			"did not match",
		);
	}

	#[test]
	fn sql_broadcast_hint_composite_left_rejects() {
		let left = LogicalPlan::Join {
			left: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("customers"),
				projection: None,
			}),
			right: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("orders"),
				projection: None,
			}),
			kind: JoinKind::Inner,
			on: vec![],
			null_safe_keys: false,
			strategy_intent: JoinStrategyIntent::Pinned {
				strategy: JoinStrategy::HashPartitioned,
				source: JoinStrategyPinSource::Direct,
				build_side: None,
			},
		};
		let right = LogicalPlan::ScanNamed {
			table: TableRef::new("lineitem"),
			projection: None,
		};
		let mut hint = Some(BroadcastJoinHint {
			target: TableRef::new("customers"),
			consumed: false,
		});

		let err = join_strategy_intent_for_hint(&mut hint, &left, &right)
			.expect_err("composite left broadcast should reject");

		assert!(err.to_string().contains("composite left"), "{err}");
	}

	#[test]
	fn build_join_plan_accepts_broadcast_non_inner_without_frontend_exchange() {
		let plan = build_join_plan(
			LogicalPlan::ScanNamed {
				table: TableRef::new("customers"),
				projection: None,
			},
			LogicalPlan::ScanNamed {
				table: TableRef::new("orders"),
				projection: None,
			},
			JoinKind::LeftOuter,
			vec![(
				Expr::qualified_column(TableRef::new("customers"), "id"),
				Expr::qualified_column(TableRef::new("orders"), "customer_id"),
			)],
			JoinStrategy::Broadcast,
			JoinStrategyPinSource::Direct,
			Some(BroadcastBuildSide::Right),
		)
		.expect("frontend should leave non-inner broadcast validation to binder");

		let LogicalPlan::Join {
			left,
			right,
			kind,
			strategy_intent,
			..
		} = plan
		else {
			panic!("expected logical join");
		};
		assert_eq!(kind, JoinKind::LeftOuter);
		assert!(matches!(*left, LogicalPlan::ScanNamed { .. }));
		assert!(matches!(*right, LogicalPlan::ScanNamed { .. }));
		assert_eq!(
			strategy_intent,
			JoinStrategyIntent::Pinned {
				strategy: JoinStrategy::Broadcast,
				source: JoinStrategyPinSource::Direct,
				build_side: Some(BroadcastBuildSide::Right),
			}
		);
	}

	fn assert_classifier_rejects(sql: &str) {
		let planner = SqlPlanner::new();
		let err = planner
			.classify_exists_conjunct(
				&parse_selection(sql),
				&[TableRef::new("fact")],
				&[TableRef::new("dim")],
			)
			.expect_err("classifier should reject");
		assert!(err.to_string().contains("unsupported"), "{sql}: {err}");
	}

	fn assert_plan_rejects(sql: &str, expected: &str) {
		let err = SqlPlanner::new().plan(sql).expect_err("SQL should reject");
		assert!(
			err.to_string().contains(expected),
			"{sql}: expected `{expected}` in {err}"
		);
	}

	fn parse_select(sql: &str) -> Select {
		let dialect = GenericDialect {};
		let mut ast = Parser::parse_sql(&dialect, sql).expect("parse sql");
		let Statement::Query(query) = ast.remove(0) else {
			panic!("expected query");
		};
		let SetExpr::Select(select) = *query.body else {
			panic!("expected select");
		};
		*select
	}

	fn parse_selection(sql: &str) -> SqlExpr {
		let dialect = GenericDialect {};
		let ast = Parser::parse_sql(&dialect, sql).expect("parse sql");
		let Statement::Query(query) = ast.into_iter().next().expect("statement") else {
			panic!("expected query");
		};
		let SetExpr::Select(select) = *query.body else {
			panic!("expected select");
		};
		select.selection.expect("selection")
	}

	fn assert_qualified_column(expr: &Expr, qualifier: &str, name: &str) {
		let Expr::Column(column) = expr else {
			panic!("expected column, got {expr:?}");
		};
		assert_eq!(column.qualifier.as_ref(), Some(&TableRef::new(qualifier)));
		assert_eq!(column.name.0, name);
	}

	fn assert_filter_on_alias<'a>(plan: &'a LogicalPlan, alias: &str) -> &'a Expr {
		let LogicalPlan::Filter { input, predicate } = plan else {
			panic!("expected filter on {alias}, got {plan:?}");
		};
		assert!(matches!(
			input.as_ref(),
			LogicalPlan::Alias { alias: observed, .. } if observed == &TableRef::new(alias)
		));
		predicate
	}

	fn assert_expr_references(expr: &Expr, qualifier: &str, name: &str) {
		assert!(
			expr_references(expr, qualifier, name),
			"expected {expr:?} to reference {qualifier}.{name}"
		);
	}

	fn expr_references(expr: &Expr, qualifier: &str, name: &str) -> bool {
		match expr {
			Expr::Column(column) => {
				column.qualifier.as_ref() == Some(&TableRef::new(qualifier))
					&& column.name.0 == name
			}
			Expr::LogicalAscription { expr, .. }
			| Expr::SqlLogicalCast { expr, .. }
			| Expr::SqlConvert { expr, .. }
			| Expr::Not(expr)
			| Expr::IsNull(expr)
			| Expr::IsNotNull(expr)
			| Expr::TimeBucket(_, expr)
			| Expr::AtTimeZone(expr, _)
			| Expr::Extract(_, expr)
			| Expr::DateTrunc(_, expr)
			| Expr::Cast(expr, _)
			| Expr::TryCast(expr, _) => expr_references(expr, qualifier, name),
			Expr::Add(left, right)
			| Expr::Sub(left, right)
			| Expr::Mul(left, right)
			| Expr::Div(left, right)
			| Expr::Eq(left, right)
			| Expr::Neq(left, right)
			| Expr::Lt(left, right)
			| Expr::Lte(left, right)
			| Expr::Gt(left, right)
			| Expr::Gte(left, right)
			| Expr::And(left, right)
			| Expr::Or(left, right) => {
				expr_references(left, qualifier, name) || expr_references(right, qualifier, name)
			}
			Expr::InList { expr, list, .. } => {
				expr_references(expr, qualifier, name)
					|| list
						.iter()
						.any(|expr| expr_references(expr, qualifier, name))
			}
			Expr::Like { expr, .. } => expr_references(expr, qualifier, name),
			Expr::Case {
				when_then,
				else_expr,
			} => {
				when_then.iter().any(|(when, then)| {
					expr_references(when, qualifier, name) || expr_references(then, qualifier, name)
				}) || else_expr
					.as_ref()
					.is_some_and(|expr| expr_references(expr, qualifier, name))
			}
			Expr::Literal(_) | Expr::UntypedNull => false,
		}
	}

	fn filter_count(plan: &LogicalPlan) -> usize {
		match plan {
			LogicalPlan::Filter { input, .. } => 1 + filter_count(input),
			LogicalPlan::Alias { input, .. }
			| LogicalPlan::Project { input, .. }
			| LogicalPlan::Distinct { input }
			| LogicalPlan::Aggregate { input, .. }
			| LogicalPlan::Exchange { input, .. }
			| LogicalPlan::Sort { input, .. }
			| LogicalPlan::Window { input, .. }
			| LogicalPlan::Take { input, .. }
			| LogicalPlan::Repeat { input, .. }
			| LogicalPlan::Explain { input }
			| LogicalPlan::ExplainAnalyze { input } => filter_count(input),
			LogicalPlan::Join { left, right, .. } => filter_count(left) + filter_count(right),
			LogicalPlan::Union { left, right } | LogicalPlan::SetOpAll { left, right, .. } => {
				filter_count(left) + filter_count(right)
			}
			LogicalPlan::CreateTableAsSelect { source, .. } => filter_count(source),
			LogicalPlan::Scan { .. }
			| LogicalPlan::ScanNamed { .. }
			| LogicalPlan::CreateTable { .. }
			| LogicalPlan::CreateLogicalType { .. }
			| LogicalPlan::DropTable { .. }
			| LogicalPlan::Delete { .. }
			| LogicalPlan::Update { .. }
			| LogicalPlan::Insert { .. }
			| LogicalPlan::InsertSelect { .. } => 0,
		}
	}
}
