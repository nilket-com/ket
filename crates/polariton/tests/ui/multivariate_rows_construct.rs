#![allow(dead_code)] // standalone fixture: compiles a partial module by design

mod plot {
	pub(crate) mod exact_numeric {
		include!("../../src/plot/exact_numeric.rs");
	}
}

#[path = "../../src/plot/kernels/multivariate_rows.rs"]
mod multivariate_rows;

use multivariate_rows::{
	MultivariateFields, MultivariateInputRow, MultivariateRows, MultivariateValueCell,
};

fn main() {
	let fields = MultivariateFields::new(&["value".to_owned()], Some("group"));
	let rows = MultivariateRows::from_rows(
		&fields,
		vec![MultivariateInputRow {
			category: Some("A".to_owned()),
			values: vec![MultivariateValueCell::Exact(4.0)],
		}],
	)
	.expect("valid multivariate row");
	assert_eq!(rows.iter().len(), 1);
}
