#![allow(dead_code)] // standalone fixture: compiles a partial module by design
#[path = "../../src/plot/spec/bubble_fields.rs"]
mod product_bubble_fields;
mod plot {
	pub(crate) mod spec {
		pub(crate) use crate::product_bubble_fields::{BubbleFieldNames, BubbleNumericField};
	}
}

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
