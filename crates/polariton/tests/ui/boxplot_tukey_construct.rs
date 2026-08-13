#![allow(dead_code)] // standalone fixture: compiles a partial module by design
#[path = "../../src/plot/kernels/boxplot_tukey.rs"]
mod boxplot_tukey;

use boxplot_tukey::{TukeyBoxRow, TukeyBoxes};

fn main() {
	let boxes = TukeyBoxes::new(vec![TukeyBoxRow {
		category: "A".to_owned(),
		whisker_low: 0.0,
		q1: 1.0,
		median: 2.0,
		q3: 3.0,
		whisker_high: 4.0,
		outlier: None,
	}])
	.unwrap();
	assert_eq!(boxes.categories(), ["A"]);
}
