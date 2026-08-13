#[path = "../../src/plot/kernels/bubble_observations.rs"]
mod bubble_observations;

use bubble_observations::BubbleObservations;

fn main() {
	let _ = BubbleObservations {
		observations: Vec::new(),
		x_range: (0.0, 1.0),
		y_range: (0.0, 1.0),
		size_range: (0.0, 0.0),
		color_range: (0.0, 0.0),
	};
}
