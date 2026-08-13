#![allow(dead_code)] // standalone fixture: compiles a partial module by design
#[path = "../../src/plot/kernels/histogram_bins.rs"]
mod histogram_bins;

use histogram_bins::HistogramBins;

fn main() {
	let _ = HistogramBins { bins: Vec::new() };
}
