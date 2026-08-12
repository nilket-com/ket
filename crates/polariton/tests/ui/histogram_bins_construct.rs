#[path = "../../src/plot/kernels/histogram_bins.rs"]
mod histogram_bins;

use histogram_bins::{HistogramBin, HistogramBins};

fn main() {
	let bins = HistogramBins::new(vec![HistogramBin {
		start: 0.0,
		end: 1.0,
		count: 7,
	}])
	.expect("valid interval sequence");
	assert_eq!(bins.find(0.5).map(|bin| bin.count), Some(7));
}
