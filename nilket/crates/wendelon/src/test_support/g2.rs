//! G2 synthetic assay phase protocol; cfg(test) parent, explicit ignored entry points only.
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
		let start = Instant::now();
		Self {
			name,
			done,
			end_ack,
			start,
		}
	}
	/// Caller keeps its output live through this acknowledgment. This layer provides only
	/// markers and elapsed time; allocator capture belongs to the authority test helper.
	pub(crate) fn end(self) -> Value {
		let elapsed_ns = self.start.elapsed().as_nanos();
		write_new(&self.done, b"done\n");
		wait_ack(&self.end_ack);
		json!({"event":"phase", "phase":self.name, "elapsed_ns":elapsed_ns,
            "scope":"phase markers and elapsed only; external RSS; no allocator metric"})
	}
}
