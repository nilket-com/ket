#![allow(dead_code)] // standalone fixture: compiles a partial module by design

mod plot {
	pub(crate) mod exact_numeric {
		include!("../../src/plot/exact_numeric.rs");
	}
}

#[path = "../../src/plot/kernels/multivariate_rows.rs"]
mod multivariate_rows;

use multivariate_rows::MultivariateRows;

fn main() {
	let _ = MultivariateRows {
		dimensions: Vec::new(),
		rows: Vec::new(),
		categories: Vec::new(),
		extrema: None,
	};
}
