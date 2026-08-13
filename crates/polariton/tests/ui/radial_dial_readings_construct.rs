#[path = "../../src/plot/kernels/radial_dial_readings.rs"]
mod radial_dial_readings;

use radial_dial_readings::{
	RadialDialNumericCell, RadialDialReadingRow, RadialDialReadings,
};

fn main() {
	let readings = RadialDialReadings::from_rows(
		Some("category"),
		"value",
		"maximum",
		vec![RadialDialReadingRow {
			category: Some("A".to_owned()),
			value: RadialDialNumericCell::Exact(4.0),
			maximum: RadialDialNumericCell::Exact(8.0),
		}],
	)
	.expect("valid radial-dial reading");
	assert_eq!(readings.iter().len(), 1);
}
