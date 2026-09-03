//! PRODUCT-0152 gate 4: the PRODUCT-0130 exhibit intervals parse to the same
//! variants through Polariton's own dependency edge to `boson` as they did
//! through the Toron client.

use boson::TemporalInterval;

#[test]
fn product_0152_exhibit_intervals_keep_their_variants() {
	const HOUR: i64 = 60 * 60 * 1_000_000_000;
	assert_eq!(
		TemporalInterval::parse("1 day"),
		Ok(TemporalInterval::FixedNanos(24 * HOUR))
	);
	assert_eq!(
		TemporalInterval::parse("3 hours"),
		Ok(TemporalInterval::FixedNanos(3 * HOUR))
	);
	assert_eq!(
		TemporalInterval::parse("6 hours"),
		Ok(TemporalInterval::FixedNanos(6 * HOUR))
	);
	assert_eq!(
		TemporalInterval::parse("1 month"),
		Ok(TemporalInterval::CalendarMonths(1))
	);
	assert!(TemporalInterval::parse("1 week").is_err());
}
