#![allow(dead_code)] // standalone fixture: compiles a partial module by design
#[path = "../../src/plot/kernels/radial_dial_readings.rs"]
mod radial_dial_readings;

use radial_dial_readings::RadialDialReadings;

fn main() {
	let _ = RadialDialReadings {
		readings: Vec::new(),
	};
}
