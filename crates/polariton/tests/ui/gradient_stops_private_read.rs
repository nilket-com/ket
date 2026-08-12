#[path = "../../src/plot/gradient_stops.rs"]
mod gradient_stops;

use gradient_stops::GradientStops;
use iced::Color;

fn main() {
	let stops = GradientStops::new(vec![(10.0, Color::BLACK), (50.0, Color::WHITE)])
		.expect("ascending finite stops");
	let _ = stops.stops;
}
