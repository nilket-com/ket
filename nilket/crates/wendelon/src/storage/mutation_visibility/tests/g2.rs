//! Requested-allocation tracking below the existing cfg(test) authority test tree.
use super::b1_assay::{r0_counters, r0_reset_peak};
use crate::test_support::g2::{Phase as Markers, emit};
use serde_json::{Value, json};
use std::time::Instant;

pub(crate) struct Phase {
	markers: Markers,
	base: [usize; 4],
	start: Instant,
}
impl Phase {
	pub(crate) fn begin(name: &'static str) -> Self {
		let markers = Markers::begin(name);
		r0_reset_peak();
		let base = r0_counters();
		Self {
			markers,
			base,
			start: Instant::now(),
		}
	}
	pub(crate) fn end(self) -> Value {
		// Capture before marker/file/JSON allocation; caller retains its output until ack.
		let elapsed_ns = self.start.elapsed().as_nanos();
		let end = r0_counters();
		let metric = self.markers.end();
		json!({"event":"phase","phase":metric["phase"],"elapsed_ns":elapsed_ns,
            "live_before":self.base[0],"live_output":end[0],"peak_live":end[1],
            "additional_peak_bytes":end[1].saturating_sub(self.base[0]),
            "retained_delta_bytes":end[0] as i128-self.base[0] as i128,
            "allocations":end[2]-self.base[2],"requested_bytes":end[3]-self.base[3],
            "scope":"process-wide requested Rust allocation; not RSS"})
	}
}
#[test]
#[ignore = "G2 calibration: only by reviewed isolated runner"]
fn g2_tracker_calibration() {
	let p = Phase::begin("empty");
	emit(p.end());
	let p = Phase::begin("allocation");
	let allocation = std::hint::black_box(vec![7u8; 1 << 20]);
	let metric = p.end();
	assert!(metric["additional_peak_bytes"].as_u64().unwrap() >= 1 << 20);
	assert_eq!(allocation.len(), 1 << 20);
	emit(metric);
	drop(allocation);
}
