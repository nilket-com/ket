use std::fmt;

use arrow::datatypes::TimeUnit;
use chrono::{
	DateTime, LocalResult, NaiveDate, NaiveDateTime, NaiveTime, SecondsFormat, TimeZone, Utc,
};
use chrono_tz::Tz;

const NANOS_PER_SECOND: i128 = 1_000_000_000;
const NANOS_PER_MILLISECOND: i128 = 1_000_000;
const NANOS_PER_MICROSECOND: i128 = 1_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TimestampParseError {
	value: String,
	reason: String,
}

impl TimestampParseError {
	fn new(value: &str, reason: impl Into<String>) -> Self {
		Self {
			value: value.to_string(),
			reason: reason.into(),
		}
	}
}

impl fmt::Display for TimestampParseError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(
			f,
			"invalid timestamp literal {:?}: {}",
			self.value, self.reason
		)
	}
}

impl std::error::Error for TimestampParseError {}

pub(crate) fn utc_timestamp_data_type(unit: TimeUnit) -> arrow::datatypes::DataType {
	arrow::datatypes::DataType::Timestamp(unit, Some("UTC".into()))
}

pub(crate) fn is_supported_utc_timestamp(data_type: &arrow::datatypes::DataType) -> bool {
	matches!(
		data_type,
		arrow::datatypes::DataType::Timestamp(
			TimeUnit::Millisecond | TimeUnit::Microsecond | TimeUnit::Nanosecond,
			Some(timezone),
		) if timezone.as_ref() == "UTC"
	)
}

pub(crate) fn parse_session_timezone(value: &str) -> Result<Tz, TimestampParseError> {
	value
		.parse::<Tz>()
		.map_err(|_| TimestampParseError::new(value, "expected IANA timezone name"))
}

pub(crate) fn parse_explicit_timestamptz(
	value: &str,
	unit: TimeUnit,
) -> Result<i64, TimestampParseError> {
	let normalized = normalize_rfc3339_separator(value.trim());
	let parsed = DateTime::parse_from_rfc3339(&normalized).map_err(|_| {
		TimestampParseError::new(value, "expected RFC3339 timestamp with explicit offset")
	})?;
	ticks_from_utc_datetime(parsed.with_timezone(&Utc), unit, value)
}

pub(crate) fn parse_wall_clock_timestamp(
	value: &str,
	timezone: Tz,
	unit: TimeUnit,
) -> Result<i64, TimestampParseError> {
	let naive = parse_local_naive_timestamp(value)?;
	let local = match timezone.from_local_datetime(&naive) {
		LocalResult::Single(value) => value,
		LocalResult::Ambiguous(left, right) => {
			if left.with_timezone(&Utc) <= right.with_timezone(&Utc) {
				left
			} else {
				right
			}
		}
		LocalResult::None => {
			return Err(TimestampParseError::new(
				value,
				format!("wall-clock time does not exist in timezone {timezone}"),
			));
		}
	};
	ticks_from_utc_datetime(local.with_timezone(&Utc), unit, value)
}

pub(crate) fn parse_timestamp_text(
	value: &str,
	timezone: Tz,
	unit: TimeUnit,
) -> Result<i64, TimestampParseError> {
	if has_explicit_offset(value.trim()) {
		parse_explicit_timestamptz(value, unit)
	} else {
		parse_wall_clock_timestamp(value, timezone, unit)
	}
}

pub(crate) fn format_utc_timestamp(value: i64, unit: TimeUnit) -> String {
	format_timestamp_in_zone(value, unit, chrono_tz::UTC)
}

pub(crate) fn format_timestamp_in_zone(value: i64, unit: TimeUnit, timezone: Tz) -> String {
	let Some(datetime) = utc_datetime_from_ticks(value, unit) else {
		return value.to_string();
	};
	datetime
		.with_timezone(&timezone)
		.to_rfc3339_opts(SecondsFormat::AutoSi, true)
}

fn parse_local_naive_timestamp(value: &str) -> Result<NaiveDateTime, TimestampParseError> {
	let value = value.trim();
	if value.is_empty() {
		return Err(TimestampParseError::new(
			value,
			"expected YYYY-MM-DD[ T]HH:MM:SS[.fraction]",
		));
	}
	if has_explicit_offset(value) {
		return Err(TimestampParseError::new(
			value,
			"bare timestamp strings must not include an offset",
		));
	}
	let (date_part, time_part) = match value
		.char_indices()
		.find_map(|(index, character)| matches!(character, 'T' | ' ').then_some(index))
	{
		Some(index) => (&value[..index], Some(value[index + 1..].trim())),
		None => (value, None),
	};
	let date = NaiveDate::parse_from_str(date_part, "%Y-%m-%d")
		.map_err(|_| TimestampParseError::new(value, "expected YYYY-MM-DD date"))?;
	let time = match time_part {
		Some(time) if !time.is_empty() => NaiveTime::parse_from_str(time, "%H:%M:%S%.f")
			.map_err(|_| TimestampParseError::new(value, "expected HH:MM:SS[.fraction] time"))?,
		Some(_) => {
			return Err(TimestampParseError::new(
				value,
				"expected HH:MM:SS[.fraction] time",
			));
		}
		None => NaiveTime::from_hms_opt(0, 0, 0).expect("midnight is valid"),
	};
	Ok(NaiveDateTime::new(date, time))
}

fn normalize_rfc3339_separator(value: &str) -> String {
	let mut normalized = value.to_string();
	if normalized.as_bytes().get(10).copied() == Some(b' ') {
		normalized.replace_range(10..11, "T");
	}
	normalized
}

fn has_explicit_offset(value: &str) -> bool {
	if value.ends_with('Z') || value.ends_with('z') {
		return true;
	}
	value
		.char_indices()
		.skip_while(|(index, _)| *index <= 10)
		.any(|(_, character)| matches!(character, '+' | '-'))
}

fn ticks_from_utc_datetime(
	datetime: DateTime<Utc>,
	unit: TimeUnit,
	original: &str,
) -> Result<i64, TimestampParseError> {
	let total_nanos = i128::from(datetime.timestamp()) * NANOS_PER_SECOND
		+ i128::from(datetime.timestamp_subsec_nanos());
	match unit {
		TimeUnit::Nanosecond => i64::try_from(total_nanos)
			.map_err(|_| TimestampParseError::new(original, "timestamp out of nanosecond range")),
		TimeUnit::Microsecond => checked_divisible_i64(
			total_nanos,
			NANOS_PER_MICROSECOND,
			original,
			"timestamp is not exactly representable at microsecond precision",
		),
		TimeUnit::Millisecond => checked_divisible_i64(
			total_nanos,
			NANOS_PER_MILLISECOND,
			original,
			"timestamp is not exactly representable at millisecond precision",
		),
		TimeUnit::Second => checked_divisible_i64(
			total_nanos,
			NANOS_PER_SECOND,
			original,
			"timestamp is not exactly representable at second precision",
		),
	}
}

fn checked_divisible_i64(
	value: i128,
	divisor: i128,
	original: &str,
	reason: &'static str,
) -> Result<i64, TimestampParseError> {
	if value % divisor != 0 {
		return Err(TimestampParseError::new(original, reason));
	}
	i64::try_from(value / divisor)
		.map_err(|_| TimestampParseError::new(original, "timestamp out of range"))
}

fn utc_datetime_from_ticks(value: i64, unit: TimeUnit) -> Option<DateTime<Utc>> {
	let total_nanos = match unit {
		TimeUnit::Second => i128::from(value).checked_mul(NANOS_PER_SECOND)?,
		TimeUnit::Millisecond => i128::from(value).checked_mul(NANOS_PER_MILLISECOND)?,
		TimeUnit::Microsecond => i128::from(value).checked_mul(NANOS_PER_MICROSECOND)?,
		TimeUnit::Nanosecond => i128::from(value),
	};
	let seconds = total_nanos.div_euclid(NANOS_PER_SECOND);
	let nanos = total_nanos.rem_euclid(NANOS_PER_SECOND);
	let seconds = i64::try_from(seconds).ok()?;
	let nanos = u32::try_from(nanos).ok()?;
	DateTime::<Utc>::from_timestamp(seconds, nanos)
}
