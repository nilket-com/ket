//! Engine-free temporal interval literals shared by Toron's frontends and
//! consumers that need the same fixed/calendar distinction.

use std::fmt;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TemporalInterval {
	FixedNanos(i64),
	CalendarMonths(i32),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TemporalIntervalError(pub(crate) String);

impl fmt::Display for TemporalIntervalError {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		formatter.write_str(&self.0)
	}
}

impl std::error::Error for TemporalIntervalError {}

impl TemporalInterval {
	/// Parse Toron's one-amount, one-unit interval literal grammar.
	pub fn parse(value: &str) -> Result<Self, TemporalIntervalError> {
		let parts = value.split_whitespace().collect::<Vec<_>>();
		if parts.len() != 2 {
			return Err(TemporalIntervalError(
				"INTERVAL string must be a single amount and unit".to_owned(),
			));
		}
		let amount = parts[0].parse::<i64>().map_err(|_| {
			TemporalIntervalError(
				"INTERVAL value must be an integer literal within i64 range".to_owned(),
			)
		})?;
		Self::from_amount_and_unit(amount, parts[1])
	}

	/// Lower an already-parsed integer amount and SQL/Rune unit word.
	pub fn from_amount_and_unit(amount: i64, unit: &str) -> Result<Self, TemporalIntervalError> {
		let fixed = |nanos_per_unit| {
			(i128::from(amount) * i128::from(nanos_per_unit))
				.try_into()
				.map(Self::FixedNanos)
				.map_err(|_| {
					TemporalIntervalError("INTERVAL literal overflows i64 nanoseconds".to_owned())
				})
		};
		let calendar = |months_per_unit| {
			(i128::from(amount) * i128::from(months_per_unit))
				.try_into()
				.map(Self::CalendarMonths)
				.map_err(|_| {
					TemporalIntervalError(
						"INTERVAL literal overflows i32 calendar months".to_owned(),
					)
				})
		};
		match unit.to_ascii_lowercase().as_str() {
			"nanosecond" | "nanoseconds" | "ns" => fixed(1),
			"microsecond" | "microseconds" | "us" => fixed(1_000),
			"millisecond" | "milliseconds" | "ms" => fixed(1_000_000),
			"second" | "seconds" | "s" => fixed(1_000_000_000),
			"minute" | "minutes" | "min" | "mins" => fixed(60_i64 * 1_000_000_000),
			"hour" | "hours" | "h" => fixed(60_i64 * 60 * 1_000_000_000),
			"day" | "days" | "d" => fixed(24_i64 * 60 * 60 * 1_000_000_000),
			"month" | "months" => calendar(1),
			"year" | "years" => calendar(12),
			"week" | "weeks" | "quarter" | "quarters" => Err(TemporalIntervalError(format!(
				"calendar INTERVAL unit {unit} is not supported; use month/year or day-and-smaller fixed intervals"
			))),
			_ => Err(TemporalIntervalError(format!(
				"unsupported INTERVAL unit: {unit}"
			))),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::TemporalInterval;

	#[test]
	fn grammar_preserves_signed_and_zero_deltas() {
		assert_eq!(
			TemporalInterval::parse("-1 day"),
			Ok(TemporalInterval::FixedNanos(-86_400_000_000_000))
		);
		assert_eq!(
			TemporalInterval::parse("0 month"),
			Ok(TemporalInterval::CalendarMonths(0))
		);
		assert_eq!(
			TemporalInterval::parse("3 mins"),
			Ok(TemporalInterval::FixedNanos(180_000_000_000))
		);
		assert!(TemporalInterval::parse("1 week").is_err());
		assert!(TemporalInterval::parse("1.5 day").is_err());
		assert!(TemporalInterval::parse("1 day 2 hours").is_err());
	}
}
