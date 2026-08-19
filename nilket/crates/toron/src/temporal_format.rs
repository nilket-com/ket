//! Temporal format patterns: one compiler, one token table, two directions.
//!
//! Rendering and parsing share [`crate::ir::TemporalFormatPattern`] so a token
//! cannot mean one thing on the way out and another on the way in. The compiler
//! runs at bind time; the evaluator only walks an already-compiled pattern.

use chrono::{Datelike, NaiveDate, TimeZone, Timelike};
use chrono_tz::Tz;

use crate::ir::{TemporalFormatPattern, TemporalFormatPiece, TemporalFormatToken};

/// The calendar and clock fields a pattern can carry.
///
/// Omitted time fields default to zero when parsing. Omitted date fields have no
/// defensible default, which is why a pattern without them is rejected before a
/// value is ever read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct TemporalFields {
	pub year: i32,
	pub month: u32,
	pub day: u32,
	pub hour: u32,
	pub minute: u32,
	pub second: u32,
	pub nanosecond: u32,
}

/// Compiles pattern text into the shared representation.
///
/// An alphanumeric run that is not a known token is an error naming the run, so
/// a typo fails loudly rather than rendering as itself. Literal alphanumeric
/// text uses a double-quoted escape.
pub(crate) fn compile_pattern(pattern: &str) -> Result<TemporalFormatPattern, String> {
	let chars: Vec<char> = pattern.chars().collect();
	let mut pieces = Vec::new();
	let mut index = 0usize;
	while index < chars.len() {
		if chars[index] == '"' {
			let mut literal = String::new();
			index += 1;
			loop {
				let Some(current) = chars.get(index) else {
					return Err(format!(
						"unterminated escape in temporal format pattern {pattern:?}"
					));
				};
				if *current == '"' {
					index += 1;
					break;
				}
				literal.push(*current);
				index += 1;
			}
			pieces.push(TemporalFormatPiece::Literal(literal.into_boxed_str()));
			continue;
		}
		if chars[index].is_alphanumeric() {
			let matched = TemporalFormatToken::ALL.iter().find(|token| {
				let spelling: Vec<char> = token.spelling().chars().collect();
				chars[index..].starts_with(&spelling)
			});
			if let Some(token) = matched {
				pieces.push(TemporalFormatPiece::Token(*token));
				index += token.spelling().chars().count();
				continue;
			}
			let start = index;
			while index < chars.len() && chars[index].is_alphanumeric() {
				index += 1;
			}
			let run: String = chars[start..index].iter().collect();
			return Err(format!(
				"unknown temporal format token {run:?}; quote it to emit it literally"
			));
		}
		let start = index;
		while index < chars.len() && !chars[index].is_alphanumeric() && chars[index] != '"' {
			index += 1;
		}
		let literal: String = chars[start..index].iter().collect();
		pieces.push(TemporalFormatPiece::Literal(literal.into_boxed_str()));
	}
	Ok(TemporalFormatPattern { pieces })
}

/// Renders fields through a compiled pattern.
pub(crate) fn render_fields(fields: &TemporalFields, pattern: &TemporalFormatPattern) -> String {
	let mut rendered = String::new();
	for piece in &pattern.pieces {
		match piece {
			TemporalFormatPiece::Literal(text) => rendered.push_str(text),
			TemporalFormatPiece::Token(token) => match token {
				TemporalFormatToken::Year4 => {
					rendered.push_str(&format!("{:04}", fields.year));
				}
				TemporalFormatToken::Month2 => {
					rendered.push_str(&format!("{:02}", fields.month));
				}
				TemporalFormatToken::Day2 => rendered.push_str(&format!("{:02}", fields.day)),
				TemporalFormatToken::Hour24 => rendered.push_str(&format!("{:02}", fields.hour)),
				TemporalFormatToken::Minute2 => {
					rendered.push_str(&format!("{:02}", fields.minute));
				}
				TemporalFormatToken::Second2 => {
					rendered.push_str(&format!("{:02}", fields.second));
				}
				// Always nine digits, so rendered width does not depend on the
				// value. Parsing accepts fewer, which keeps the round trip whole.
				TemporalFormatToken::Fraction9 => {
					rendered.push_str(&format!("{:09}", fields.nanosecond));
				}
			},
		}
	}
	rendered
}

/// Reads fields from text through a compiled pattern.
pub(crate) fn parse_fields(
	text: &str,
	pattern: &TemporalFormatPattern,
) -> Result<TemporalFields, String> {
	let chars: Vec<char> = text.chars().collect();
	let mut index = 0usize;
	let mut fields = TemporalFields {
		day: 1,
		month: 1,
		..TemporalFields::default()
	};
	for piece in &pattern.pieces {
		match piece {
			TemporalFormatPiece::Literal(literal) => {
				let expected: Vec<char> = literal.chars().collect();
				if !chars[index.min(chars.len())..].starts_with(&expected) {
					return Err(format!("expected {literal:?} in {text:?}"));
				}
				index += expected.len();
			}
			TemporalFormatPiece::Token(token) => {
				let (min_digits, max_digits) = match token {
					TemporalFormatToken::Year4 => (4, 4),
					TemporalFormatToken::Fraction9 => (1, 9),
					_ => (2, 2),
				};
				let start = index;
				while index < chars.len()
					&& chars[index].is_ascii_digit()
					&& index - start < max_digits
				{
					index += 1;
				}
				let digits: String = chars[start..index].iter().collect();
				if digits.chars().count() < min_digits {
					return Err(format!(
						"expected {} digits for {} in {text:?}",
						min_digits,
						token.spelling()
					));
				}
				let value: u32 = digits
					.parse()
					.map_err(|_| format!("{digits:?} is not a number in {text:?}"))?;
				match token {
					TemporalFormatToken::Year4 => {
						fields.year = i32::try_from(value)
							.map_err(|_| format!("year out of range in {text:?}"))?;
					}
					TemporalFormatToken::Month2 => fields.month = value,
					TemporalFormatToken::Day2 => fields.day = value,
					TemporalFormatToken::Hour24 => fields.hour = value,
					TemporalFormatToken::Minute2 => fields.minute = value,
					TemporalFormatToken::Second2 => fields.second = value,
					// A fraction is a decimal fraction of a second, so three
					// digits mean milliseconds rather than nanoseconds.
					TemporalFormatToken::Fraction9 => {
						let scale = 9u32.saturating_sub(digits.chars().count() as u32);
						fields.nanosecond = value.saturating_mul(10u32.pow(scale));
					}
				}
			}
		}
	}
	if index < chars.len() {
		let remainder: String = chars[index..].iter().collect();
		return Err(format!("unexpected trailing {remainder:?} in {text:?}"));
	}
	Ok(fields)
}

/// Turns parsed fields into a UTC instant, resolving against the session zone.
pub(crate) fn fields_to_utc_nanos(fields: &TemporalFields, timezone: Tz) -> Result<i64, String> {
	let date = NaiveDate::from_ymd_opt(fields.year, fields.month, fields.day).ok_or_else(|| {
		format!(
			"{:04}-{:02}-{:02} is not a date",
			fields.year, fields.month, fields.day
		)
	})?;
	let naive = date
		.and_hms_nano_opt(fields.hour, fields.minute, fields.second, fields.nanosecond)
		.ok_or_else(|| {
			format!(
				"{:02}:{:02}:{:02} is not a time",
				fields.hour, fields.minute, fields.second
			)
		})?;
	let local = timezone
		.from_local_datetime(&naive)
		.single()
		.ok_or_else(|| format!("{naive} does not exist exactly once in {timezone}"))?;
	local
		.timestamp_nanos_opt()
		.ok_or_else(|| format!("{naive} is outside the representable instant range"))
}

/// Reads a UTC instant into fields in the given zone.
pub(crate) fn utc_nanos_to_fields(value: i64, timezone: Tz) -> TemporalFields {
	let instant = chrono::DateTime::from_timestamp_nanos(value).with_timezone(&timezone);
	TemporalFields {
		year: instant.year(),
		month: instant.month(),
		day: instant.day(),
		hour: instant.hour(),
		minute: instant.minute(),
		second: instant.second(),
		nanosecond: instant.nanosecond(),
	}
}

/// Reads a `Date32` day count into fields, leaving the clock at zero.
pub(crate) fn date32_to_fields(days: i32) -> TemporalFields {
	let (year, month, day) = crate::date32::civil_from_days(i64::from(days));
	TemporalFields {
		year: year as i32,
		month: month as u32,
		day: day as u32,
		..TemporalFields::default()
	}
}
