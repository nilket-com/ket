#![allow(dead_code)] // standalone fixture: compiles a partial module by design
#[path = "../../src/plot/kernels/pie_slices.rs"]
mod pie_slices;

use pie_slices::{PieSliceRow, PieSlices, PieValueCell};

fn main() {
	let slices = PieSlices::from_rows(
		Some("category"),
		"value",
		vec![PieSliceRow {
			category: Some("A".to_owned()),
			value: PieValueCell::Exact(4.0),
		}],
	)
	.expect("valid pie slice");
	assert_eq!(slices.iter().len(), 1);
}
