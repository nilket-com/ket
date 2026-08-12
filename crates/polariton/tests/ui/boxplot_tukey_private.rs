#[path = "../../src/plot/kernels/boxplot_tukey.rs"]
mod boxplot_tukey;

use boxplot_tukey::TukeyBox;

fn main() {
	let _ = TukeyBox {
		whisker_low: 0.0,
		q1: 1.0,
		median: 2.0,
		q3: 3.0,
		whisker_high: 4.0,
		outliers: Vec::new(),
	};
}
