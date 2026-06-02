use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Date32ParseError {
	value: String,
	reason: &'static str,
}

impl Date32ParseError {
	fn new(value: &str, reason: &'static str) -> Self {
		Self {
			value: value.to_string(),
			reason,
		}
	}
}

impl fmt::Display for Date32ParseError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(
			f,
			"invalid Date32 literal {:?}: {}",
			self.value, self.reason
		)
	}
}

impl std::error::Error for Date32ParseError {}

pub(crate) fn parse_date_ymd(value: &str) -> Result<i32, Date32ParseError> {
	let bytes = value.as_bytes();
	if bytes.len() != 10 {
		return Err(Date32ParseError::new(value, "expected YYYY-MM-DD"));
	}
	if bytes[4] != b'-' || bytes[7] != b'-' {
		return Err(Date32ParseError::new(value, "expected YYYY-MM-DD"));
	}
	if !bytes[..4].iter().all(u8::is_ascii_digit)
		|| !bytes[5..7].iter().all(u8::is_ascii_digit)
		|| !bytes[8..10].iter().all(u8::is_ascii_digit)
	{
		return Err(Date32ParseError::new(value, "expected YYYY-MM-DD"));
	}

	let year = parse_decimal(&bytes[..4]);
	let month = parse_decimal(&bytes[5..7]);
	let day = parse_decimal(&bytes[8..10]);
	if !(1..=12).contains(&month) {
		return Err(Date32ParseError::new(value, "month out of range"));
	}
	let max_day = days_in_month(year, month);
	if day == 0 || day > max_day {
		return Err(Date32ParseError::new(value, "day out of range"));
	}

	i32::try_from(days_from_civil(
		i64::from(year),
		i64::from(month),
		i64::from(day),
	))
	.map_err(|_| Date32ParseError::new(value, "day count out of i32 range"))
}

pub(crate) fn format_date_ymd(days: i32) -> String {
	let (year, month, day) = civil_from_days(i64::from(days));
	if (0..=9999).contains(&year) {
		format!("{year:04}-{month:02}-{day:02}")
	} else {
		format!("{year}-{month:02}-{day:02}")
	}
}

fn parse_decimal(bytes: &[u8]) -> u32 {
	bytes
		.iter()
		.fold(0u32, |acc, byte| acc * 10 + u32::from(byte - b'0'))
}

fn days_in_month(year: u32, month: u32) -> u32 {
	match month {
		1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
		4 | 6 | 9 | 11 => 30,
		2 if is_leap_year(year) => 29,
		2 => 28,
		_ => 0,
	}
}

fn is_leap_year(year: u32) -> bool {
	(year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
	let year = year - i64::from(month <= 2);
	let era = if year >= 0 { year } else { year - 399 } / 400;
	let year_of_era = year - era * 400;
	let day_of_year = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
	let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
	era * 146_097 + day_of_era - 719_468
}

fn civil_from_days(days: i64) -> (i64, u32, u32) {
	let days = days + 719_468;
	let era = if days >= 0 { days } else { days - 146_096 } / 146_097;
	let day_of_era = days - era * 146_097;
	let year_of_era =
		(day_of_era - day_of_era / 1460 + day_of_era / 36524 - day_of_era / 146096) / 365;
	let mut year = year_of_era + era * 400;
	let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
	let month_prime = (5 * day_of_year + 2) / 153;
	let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
	let month = month_prime + if month_prime < 10 { 3 } else { -9 };
	year += i64::from(month <= 2);
	(
		year,
		u32::try_from(month).expect("month in range"),
		u32::try_from(day).expect("day in range"),
	)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn parses_and_formats_date32_edges() {
		assert_eq!(parse_date_ymd("1970-01-01").unwrap(), 0);
		assert_eq!(parse_date_ymd("1969-12-31").unwrap(), -1);
		assert_eq!(
			format_date_ymd(parse_date_ymd("9999-12-31").unwrap()),
			"9999-12-31"
		);
		assert_eq!(
			format_date_ymd(parse_date_ymd("2024-02-29").unwrap()),
			"2024-02-29"
		);
	}

	#[test]
	fn rejects_noncanonical_or_invalid_dates() {
		for value in [
			"2026-02-29",
			"2026-2-09",
			" 2026-02-09",
			"2026-02-09 ",
			"+2026-02-09",
			"2026-02-09T00:00:00Z",
			"2026-13-01",
			"2026-00-01",
			"2026-01-00",
		] {
			assert!(parse_date_ymd(value).is_err(), "{value}");
		}
	}
}
