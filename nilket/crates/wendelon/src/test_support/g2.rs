//! G2 synthetic assay phase protocol; cfg(test) parent, explicit ignored entry points only.
use crate::storage::mutation_visibility::{r0_counters, r0_reset_peak};
use serde_json::{Value, json};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::time::{Duration, Instant};

pub(crate) fn case() -> Value {
	serde_json::from_str(&std::env::var("WENDELON_G2_CASE").expect("frozen case JSON")).unwrap()
}
pub(crate) fn emit(value: Value) {
	println!("G2_JSON {value}");
}
fn control() -> PathBuf {
	let path = PathBuf::from(std::env::var_os("WENDELON_G2_CONTROL").expect("owned control root"));
	assert!(path.is_absolute() && path.is_dir());
	path
}
fn write_new(path: &std::path::Path, bytes: &[u8]) {
	let mut f = OpenOptions::new()
		.write(true)
		.create_new(true)
		.open(path)
		.unwrap();
	f.write_all(bytes).unwrap();
}
fn wait_ack(path: &std::path::Path) {
	let start = Instant::now();
	while !path.exists() {
		assert!(
			start.elapsed() < Duration::from_secs(60),
			"sampler acknowledgment timeout"
		);
		std::thread::sleep(Duration::from_millis(1));
	}
	assert_eq!(fs::read(path).unwrap(), b"ack\n");
}
pub(crate) struct Phase {
	name: &'static str,
	done: PathBuf,
	end_ack: PathBuf,
	base: [usize; 4],
	start: Instant,
}
impl Phase {
	pub(crate) fn begin(name: &'static str) -> Self {
		let dir = control();
		let ready = dir.join(format!("{name}.ready"));
		let ack = dir.join(format!("{name}.ack"));
		let done = dir.join(format!("{name}.done"));
		let end_ack = dir.join(format!("{name}.end-ack"));
		write_new(&ready, b"ready\n");
		wait_ack(&ack);
		r0_reset_peak();
		let base = r0_counters();
		let start = Instant::now();
		Self {
			name,
			done,
			end_ack,
			base,
			start,
		}
	}
	/// Caller keeps its output live through this acknowledgment. JSON/file allocation follows
	/// counter capture and is excluded. Phase object/path bookkeeping belongs to the baseline.
	pub(crate) fn end(self) -> Value {
		let elapsed_ns = self.start.elapsed().as_nanos();
		let end = r0_counters();
		write_new(&self.done, b"done\n");
		wait_ack(&self.end_ack);
		json!({"event":"phase", "phase":self.name, "elapsed_ns":elapsed_ns,
			"live_before":self.base[0], "live_output":end[0], "peak_live":end[1],
			"additional_peak_bytes":end[1].saturating_sub(self.base[0]),
			"retained_delta_bytes":end[0] as i128-self.base[0] as i128,
			"allocations":end[2]-self.base[2], "requested_bytes":end[3]-self.base[3],
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
