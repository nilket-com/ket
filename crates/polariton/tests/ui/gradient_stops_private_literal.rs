#![allow(dead_code)] // standalone fixture: compiles a partial module by design
mod plot {
	pub mod continuous_color {
		use iced::Color;
		pub struct ContinuousColorScale;
		pub struct Ramp;
		impl ContinuousColorScale {
			pub fn ramp(&self) -> Ramp { Ramp }
			pub fn value_at_normalized(&self, t: f64) -> f64 { t }
		}
		impl Ramp {
			pub fn canonical_stops(&self) -> Vec<(f32, Color)> {
				vec![(0.0, Color::BLACK), (1.0, Color::WHITE)]
			}
			pub fn uses_reversed_sampling(&self) -> bool { false }
		}
	}
}

#[path = "../../src/plot/gradient_stops.rs"]
mod gradient_stops;

use gradient_stops::GradientStops;

fn main() {
	let _ = GradientStops { stops: Vec::new() };
}
