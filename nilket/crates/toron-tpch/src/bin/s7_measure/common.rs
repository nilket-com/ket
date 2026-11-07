//! Process identity, clocks and file conventions shared by the node child,
//! the driver and the evaluator. Both processes stamp `CLOCK_MONOTONIC`
//! nanoseconds, so their timestamps share one clock domain on one host.

use serde::{Deserialize, Serialize};
use std::io::Write;
use std::path::{Path, PathBuf};

/// The arm compiled into this binary: `--cfg s7_fix` selects the fix.
pub const ARM: &str = if cfg!(s7_fix) { "fix" } else { "base" };

pub fn mono_ns() -> u64 {
	let mut ts = libc::timespec {
		tv_sec: 0,
		tv_nsec: 0,
	};
	// SAFETY: `ts` is a valid, writable timespec for the duration of the call.
	let rc = unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts) };
	assert_eq!(rc, 0, "clock_gettime(CLOCK_MONOTONIC)");
	ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
}

/// Field 22 of `/proc/<pid>/stat` (start time in clock ticks since boot):
/// with the PID it identifies one process incarnation.
pub fn pid_start_time(pid: u32) -> std::io::Result<u64> {
	let stat = std::fs::read_to_string(format!("/proc/{pid}/stat"))?;
	parse_stat_start_time(&stat).ok_or_else(|| {
		std::io::Error::new(
			std::io::ErrorKind::InvalidData,
			format!("malformed /proc/{pid}/stat"),
		)
	})
}

pub fn parse_stat_start_time(stat: &str) -> Option<u64> {
	// The command name may contain spaces and parentheses; fields resume
	// after the last ')'. Field 3 (state) is the first after it.
	let rest = &stat[stat.rfind(')')? + 1..];
	rest.split_whitespace().nth(22 - 3)?.parse().ok()
}

/// `VmRSS` of `/proc/self/status`, in KiB.
pub fn self_rss_kib() -> std::io::Result<u64> {
	let status = std::fs::read_to_string("/proc/self/status")?;
	status
		.lines()
		.find_map(|line| line.strip_prefix("VmRSS:"))
		.and_then(|v| v.trim().trim_end_matches("kB").trim().parse().ok())
		.ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "no VmRSS"))
}

/// Write `bytes` to `path` atomically (temp file, fsync, rename).
pub fn write_atomic(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
	let tmp = path.with_extension("tmp");
	{
		let mut f = std::fs::File::create(&tmp)?;
		f.write_all(bytes)?;
		f.sync_all()?;
	}
	std::fs::rename(&tmp, path)
}

pub fn write_json_atomic<T: Serialize>(path: &Path, value: &T) -> std::io::Result<()> {
	let bytes = serde_json::to_vec_pretty(value)
		.map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
	write_atomic(path, &bytes)
}

pub fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> std::io::Result<T> {
	let bytes = std::fs::read(path)?;
	serde_json::from_slice(&bytes).map_err(|e| {
		std::io::Error::new(
			std::io::ErrorKind::InvalidData,
			format!("{}: {e}", path.display()),
		)
	})
}

pub fn sha256_hex(bytes: &[u8]) -> String {
	use sha2::Digest;
	hex(&sha2::Sha256::digest(bytes))
}

pub fn hex(bytes: &[u8]) -> String {
	bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// The block directory layout.
#[derive(Clone, Debug)]
pub struct BlockPaths {
	pub root: PathBuf,
}

impl BlockPaths {
	pub fn new(root: impl Into<PathBuf>) -> Self {
		Self { root: root.into() }
	}
	pub fn data_dir(&self) -> PathBuf {
		self.root.join("store")
	}
	pub fn l2_dir(&self) -> PathBuf {
		self.root.join("l2")
	}
	pub fn pki_dir(&self) -> PathBuf {
		self.root.join("pki")
	}
	pub fn control_dir(&self) -> PathBuf {
		self.root.join("control")
	}
	pub fn control_request(&self) -> PathBuf {
		self.control_dir().join("request.json")
	}
	pub fn control_ack(&self, seq: u64) -> PathBuf {
		self.control_dir().join(format!("ack-{seq}.json"))
	}
	pub fn observations(&self) -> PathBuf {
		self.root.join("observations.jsonl")
	}
	pub fn node_ready(&self) -> PathBuf {
		self.root.join("node-ready.json")
	}
	pub fn effective_config(&self) -> PathBuf {
		self.root.join("effective-config.json")
	}
	pub fn driver_record(&self) -> PathBuf {
		self.root.join("driver.json")
	}
	pub fn driver_batches(&self) -> PathBuf {
		self.root.join("driver-batches.jsonl")
	}
	pub fn verdict(&self) -> PathBuf {
		self.root.join("verdict.json")
	}
	/// Written by run.sh before the node starts: the block's unique identity.
	pub fn block_id_file(&self) -> PathBuf {
		self.root.join("block-id.json")
	}
	/// Written by run.sh after both processes end: their outcomes.
	pub fn process(&self) -> PathBuf {
		self.root.join("process.json")
	}

	/// The block identity every record carries; absent, nothing runs.
	pub fn block_id(&self) -> std::io::Result<String> {
		let v: serde_json::Value = read_json(&self.block_id_file())?;
		match v["block_id"].as_str() {
			Some(id) if !id.is_empty() => Ok(id.to_string()),
			_ => Err(std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				"block-id.json has no block_id",
			)),
		}
	}
}

/// A control request the driver writes before signalling the node: SIGUSR1
/// carries `reset`, SIGUSR2 carries `final` or `diagnostic`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlRequest {
	pub seq: u64,
	pub kind: String,
	/// The node incarnation the request is addressed to.
	pub pid: u32,
	pub pid_start_time: u64,
}

/// What the node child publishes once it serves.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeReady {
	pub arm: String,
	pub label: String,
	pub pid: u32,
	pub pid_start_time: u64,
	pub address: String,
	pub cluster_domain: String,
	pub node_id: u32,
	/// Operator identity: tenant, table and retention administration.
	pub admin_cert: PathBuf,
	pub admin_key: PathBuf,
	/// `data_node` identity: table writes (node-required at router ingress).
	pub writer_cert: PathBuf,
	pub writer_key: PathBuf,
	pub ca_cert: PathBuf,
	pub t_mono_ns: u64,
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn start_time_survives_spaces_and_parens_in_comm() {
		let stat = "4242 (s7 (x) y) S 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 987654 20 21";
		assert_eq!(parse_stat_start_time(stat), Some(987654));
	}
}
