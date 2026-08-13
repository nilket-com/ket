#[path = "../../src/plot/kernels/bubble_observations.rs"]
mod bubble_observations;

use bubble_observations::{BubbleFieldNames, BubbleObservationRow, BubbleObservations};

fn main() {
	let observations = BubbleObservations::from_rows(
		BubbleFieldNames::new("x", "y", "size", "color"),
		vec![BubbleObservationRow {
			x: Some(1.0),
			y: Some(2.0),
			size: Some(3.0),
			color: Some(4.0),
			label: Some("point".to_owned()),
		}],
	)
	.expect("valid bubble observation");
	assert_eq!(observations.color_range(), (4.0, 4.0));
}
