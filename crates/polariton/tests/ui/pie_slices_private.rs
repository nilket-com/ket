#![allow(dead_code)] // standalone fixture: compiles a partial module by design
#[path = "../../src/plot/kernels/pie_slices.rs"]
mod pie_slices;

use pie_slices::PieSlices;

fn main() {
	let _ = PieSlices {
		slices: Vec::new(),
		total: None,
		boundaries: Vec::new(),
	};
}
