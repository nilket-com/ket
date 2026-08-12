#[path = "../../src/plot/gradient_stops.rs"]
mod gradient_stops;

use gradient_stops::GradientStops;

fn main() {
	let _ = GradientStops { stops: Vec::new() };
}
