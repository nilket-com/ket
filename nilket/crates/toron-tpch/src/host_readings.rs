//! ADR 593: host readings on every perf child, and the driver-side journal collection and
//! scope record. Readings are diagnostic evidence: every source is optional and independent, a
//! source that cannot be read is `null` with its reason, never a zero and never a refusal.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Where the sources live; a test hands a fabricated tree.
#[derive(Debug, Clone)]
pub struct HostReadingRoots {
	/// `/sys/class/hwmon`
	pub hwmon: PathBuf,
	/// `/sys/devices/system/cpu`
	pub cpu: PathBuf,
	/// `/proc/thread-self/status`
	pub thread_status: PathBuf,
}

impl HostReadingRoots {
	pub fn system() -> Self {
		Self {
			hwmon: PathBuf::from("/sys/class/hwmon"),
			cpu: PathBuf::from("/sys/devices/system/cpu"),
			thread_status: PathBuf::from("/proc/thread-self/status"),
		}
	}
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct CtxtSwitches {
	pub voluntary: u64,
	pub nonvoluntary: u64,
}

/// One reading: some twenty sequential reads, its first and last timestamps on it, so it is an
/// interval and says so.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct HostReading {
	pub unix_ms_first: f64,
	pub monotonic_ns_first: u64,
	pub unix_ms_last: f64,
	pub monotonic_ns_last: u64,
	pub pid: u32,
	pub tid: u32,
	pub package_temp_millic: Option<i64>,
	pub cpu_freq_khz: Option<BTreeMap<usize, u64>>,
	pub ctxt_switches: Option<CtxtSwitches>,
	pub elapsed_us: u64,
	pub missing: Vec<String>,
}

fn unix_ms_now() -> f64 {
	std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.map(|d| d.as_secs_f64() * 1000.0)
		.unwrap_or(f64::NAN)
}

fn monotonic_ns_now() -> u64 {
	let mut ts = libc::timespec {
		tv_sec: 0,
		tv_nsec: 0,
	};
	// SAFETY: clock_gettime writes a timespec; CLOCK_MONOTONIC always exists on Linux.
	unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts) };
	(ts.tv_sec as u64) * 1_000_000_000 + ts.tv_nsec as u64
}

fn current_tid() -> u32 {
	// SAFETY: gettid has no preconditions.
	unsafe { libc::gettid() as u32 }
}

/// The `Package id 0` input of the `coretemp` hwmon device, found by name and label at any index.
pub fn read_package_temp_millic(hwmon_root: &Path) -> Result<i64, String> {
	let entries = std::fs::read_dir(hwmon_root)
		.map_err(|error| format!("hwmon root {}: {error}", hwmon_root.display()))?;
	let mut coretemp = None;
	for entry in entries.flatten() {
		let dir = entry.path();
		if std::fs::read_to_string(dir.join("name"))
			.map(|name| name.trim() == "coretemp")
			.unwrap_or(false)
		{
			coretemp = Some(dir);
			break;
		}
	}
	let dir = coretemp.ok_or_else(|| "no hwmon device named coretemp".to_string())?;
	let labels = std::fs::read_dir(&dir)
		.map_err(|error| format!("coretemp dir {}: {error}", dir.display()))?;
	for entry in labels.flatten() {
		let name = entry.file_name().to_string_lossy().to_string();
		if name.starts_with("temp") && name.ends_with("_label") {
			let label = std::fs::read_to_string(entry.path()).unwrap_or_default();
			if label.trim() == "Package id 0" {
				let input = dir.join(name.replace("_label", "_input"));
				let text = std::fs::read_to_string(&input)
					.map_err(|error| format!("{}: {error}", input.display()))?;
				return text
					.trim()
					.parse::<i64>()
					.map_err(|error| format!("{}: {error}", input.display()));
			}
		}
	}
	Err("coretemp has no input labelled 'Package id 0'".to_string())
}

/// `scaling_cur_freq` for each CPU in `cpus`; a CPU whose file is unreadable fails the whole
/// source with that CPU named, so a partial map never masquerades as a complete one.
pub fn read_cpu_freq_khz(
	cpu_root: &Path,
	cpus: &BTreeSet<usize>,
) -> Result<BTreeMap<usize, u64>, String> {
	let mut map = BTreeMap::new();
	for &cpu in cpus {
		let path = cpu_root.join(format!("cpu{cpu}/cpufreq/scaling_cur_freq"));
		let text = std::fs::read_to_string(&path)
			.map_err(|error| format!("cpu{cpu}: {}: {error}", path.display()))?;
		let khz = text
			.trim()
			.parse::<u64>()
			.map_err(|error| format!("cpu{cpu}: {}: {error}", path.display()))?;
		map.insert(cpu, khz);
	}
	Ok(map)
}

/// The reading thread's own counters from its `status` file.
pub fn read_ctxt_switches(status_path: &Path) -> Result<CtxtSwitches, String> {
	let text = std::fs::read_to_string(status_path)
		.map_err(|error| format!("{}: {error}", status_path.display()))?;
	let mut voluntary = None;
	let mut nonvoluntary = None;
	for line in text.lines() {
		if let Some(value) = line.strip_prefix("voluntary_ctxt_switches:") {
			voluntary = value.trim().parse::<u64>().ok();
		} else if let Some(value) = line.strip_prefix("nonvoluntary_ctxt_switches:") {
			nonvoluntary = value.trim().parse::<u64>().ok();
		}
	}
	match (voluntary, nonvoluntary) {
		(Some(voluntary), Some(nonvoluntary)) => Ok(CtxtSwitches {
			voluntary,
			nonvoluntary,
		}),
		_ => Err(format!(
			"{} lacks voluntary/nonvoluntary_ctxt_switches",
			status_path.display()
		)),
	}
}

/// Take one reading over `cpus` (the process's affinity mask) from `roots`.
pub fn take_reading(roots: &HostReadingRoots, cpus: &BTreeSet<usize>) -> HostReading {
	let started = Instant::now();
	let unix_ms_first = unix_ms_now();
	let monotonic_ns_first = monotonic_ns_now();
	let mut missing = Vec::new();
	let package_temp_millic = match read_package_temp_millic(&roots.hwmon) {
		Ok(value) => Some(value),
		Err(reason) => {
			missing.push(format!("package_temp_millic: {reason}"));
			None
		}
	};
	let cpu_freq_khz = match read_cpu_freq_khz(&roots.cpu, cpus) {
		Ok(value) => Some(value),
		Err(reason) => {
			missing.push(format!("cpu_freq_khz: {reason}"));
			None
		}
	};
	let ctxt_switches = match read_ctxt_switches(&roots.thread_status) {
		Ok(value) => Some(value),
		Err(reason) => {
			missing.push(format!("ctxt_switches: {reason}"));
			None
		}
	};
	let unix_ms_last = unix_ms_now();
	let monotonic_ns_last = monotonic_ns_now();
	HostReading {
		unix_ms_first,
		monotonic_ns_first,
		unix_ms_last,
		monotonic_ns_last,
		pid: std::process::id(),
		tid: current_tid(),
		package_temp_millic,
		cpu_freq_khz,
		ctxt_switches,
		elapsed_us: started.elapsed().as_micros() as u64,
		missing,
	}
}

/// The two readings that bracket one execute span.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct HostReadingsPair {
	pub start: HostReading,
	pub end: HostReading,
}

impl HostReadingsPair {
	/// The reading thread's context-switch delta, reported only when both readings are the same
	/// thread and neither counter decreased; otherwise `None` with the reason.
	pub fn ctxt_switch_delta(&self) -> Result<CtxtSwitches, String> {
		if self.start.pid != self.end.pid || self.start.tid != self.end.tid {
			return Err(format!(
				"readings are from different threads (pid/tid {}/{} then {}/{})",
				self.start.pid, self.start.tid, self.end.pid, self.end.tid
			));
		}
		let (Some(start), Some(end)) = (&self.start.ctxt_switches, &self.end.ctxt_switches) else {
			return Err("a reading lacks ctxt_switches".to_string());
		};
		if end.voluntary < start.voluntary || end.nonvoluntary < start.nonvoluntary {
			return Err(format!(
				"a counter decreased ({}/{} then {}/{})",
				start.voluntary, start.nonvoluntary, end.voluntary, end.nonvoluntary
			));
		}
		Ok(CtxtSwitches {
			voluntary: end.voluntary - start.voluntary,
			nonvoluntary: end.nonvoluntary - start.nonvoluntary,
		})
	}

	pub fn json_value(&self) -> serde_json::Value {
		let delta = match self.ctxt_switch_delta() {
			Ok(delta) => {
				serde_json::json!({"reportable": true, "voluntary": delta.voluntary, "nonvoluntary": delta.nonvoluntary,
				"scope": "the reading thread, over the interval between the two counter reads"})
			}
			Err(reason) => serde_json::json!({"reportable": false, "reason": reason}),
		};
		serde_json::json!({
			"start": self.start,
			"end": self.end,
			"interval_ns": self.end.monotonic_ns_first.saturating_sub(self.start.monotonic_ns_last),
			"ctxt_switch_delta": delta,
		})
	}
}

// The perf child executes exactly one query per process, so the readings taken around that
// execute are handed to the row through this slot. It is armed only by `perf-run-one`; every
// other caller of the executing function pays nothing.
static ENABLED: std::sync::OnceLock<()> = std::sync::OnceLock::new();
static LAST: Mutex<Option<HostReadingsPair>> = Mutex::new(None);

pub fn enable() {
	let _ = ENABLED.set(());
}

pub fn enabled() -> bool {
	ENABLED.get().is_some()
}

/// Take the start reading if enabled; the caller passes it back to [`finish`] after the span.
pub fn begin() -> Option<HostReading> {
	if !enabled() {
		return None;
	}
	let cpus = crate::affinity::current_affinity().unwrap_or_default();
	Some(take_reading(&HostReadingRoots::system(), &cpus))
}

pub fn finish(start: Option<HostReading>) {
	let Some(start) = start else {
		return;
	};
	let cpus = crate::affinity::current_affinity().unwrap_or_default();
	let end = take_reading(&HostReadingRoots::system(), &cpus);
	*LAST.lock().expect("host readings slot") = Some(HostReadingsPair { start, end });
}

/// The readings of the one execute this process ran, taken out of the slot.
pub fn take_last() -> Option<HostReadingsPair> {
	LAST.lock().expect("host readings slot").take()
}

// -------------------------------------------------------------------------------------------
// Journal collection, once per run, joined to per-child windows afterwards.
// -------------------------------------------------------------------------------------------

pub const JOURNAL_ENTRY_CAP: usize = 2_000;
pub const JOURNAL_DURATION_BOUND: Duration = Duration::from_secs(30);
pub const JOURNAL_MESSAGE_CHARS: usize = 200;
/// Bytes of journal output read before the collection is cut off, independent of the entry cap.
pub const JOURNAL_BYTE_CAP: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct JournalEntry {
	pub realtime_us: u64,
	pub monotonic_us: Option<u64>,
	pub boot_id: Option<String>,
	pub source: Option<String>,
	pub message: String,
}

/// What happened to the collection. `Collected` is claimed only when the command ran to
/// completion inside every bound and every record parsed; anything less is named.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case", tag = "state", content = "detail")]
pub enum JournalState {
	Collected,
	/// A bound was hit (entries, bytes, or duration): the entries kept are a prefix.
	Truncated(String),
	/// The command ran but some of its output could not be used (a read failure or malformed
	/// records): the entries kept are incomplete in an unknown way.
	Degraded(String),
	/// Nothing usable: the command could not run or failed.
	Unavailable(String),
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct JournalCollection {
	pub since_unix_ms: f64,
	pub until_unix_ms: f64,
	pub state: JournalState,
	pub entries: Vec<JournalEntry>,
	pub malformed_records: usize,
	pub bytes_read: usize,
	pub entry_cap: usize,
	pub byte_cap: usize,
	pub duration_bound_ms: u64,
	pub command: String,
}

fn message_text(value: Option<&serde_json::Value>) -> String {
	let text = match value {
		Some(serde_json::Value::String(text)) => text.clone(),
		Some(serde_json::Value::Array(bytes)) => {
			let raw: Vec<u8> = bytes
				.iter()
				.filter_map(serde_json::Value::as_u64)
				.map(|b| b as u8)
				.collect();
			String::from_utf8_lossy(&raw).to_string()
		}
		_ => String::new(),
	};
	text.chars().take(JOURNAL_MESSAGE_CHARS).collect()
}

/// Parse one `journalctl -o json` record; `None` for a record that is not one.
pub fn parse_journal_record(line: &str) -> Option<JournalEntry> {
	let value = serde_json::from_str::<serde_json::Value>(line).ok()?;
	let realtime_us = value
		.get("__REALTIME_TIMESTAMP")
		.and_then(serde_json::Value::as_str)
		.and_then(|s| s.parse::<u64>().ok())?;
	let text_field = |field: &str| {
		value
			.get(field)
			.and_then(serde_json::Value::as_str)
			.map(str::to_string)
	};
	Some(JournalEntry {
		realtime_us,
		monotonic_us: text_field("__MONOTONIC_TIMESTAMP").and_then(|s| s.parse().ok()),
		boot_id: text_field("_BOOT_ID"),
		source: text_field("_SYSTEMD_UNIT")
			.or_else(|| text_field("SYSLOG_IDENTIFIER"))
			.or_else(|| text_field("_COMM")),
		message: message_text(value.get("MESSAGE")),
	})
}

/// Parse `journalctl -o json` text already in hand (tests, and small inputs): entries kept, the
/// number of malformed records, and whether the entry cap cut the input short.
pub fn parse_journal_lines(text: &str) -> (Vec<JournalEntry>, usize, bool) {
	let mut entries = Vec::new();
	let mut malformed = 0;
	let mut truncated = false;
	for line in text.lines().filter(|line| !line.trim().is_empty()) {
		if entries.len() >= JOURNAL_ENTRY_CAP {
			truncated = true;
			break;
		}
		match parse_journal_record(line) {
			Some(entry) => entries.push(entry),
			None => malformed += 1,
		}
	}
	(entries, malformed, truncated)
}

/// How a collection is run: the program and its leading arguments, and the bounds. The system
/// collector is `journalctl`; a test hands a script and small bounds.
#[derive(Debug, Clone)]
pub struct JournalCollector {
	pub program: String,
	pub leading_args: Vec<String>,
	pub entry_cap: usize,
	pub byte_cap: usize,
	pub duration_bound: Duration,
}

impl JournalCollector {
	pub fn system() -> Self {
		Self {
			program: "journalctl".to_string(),
			leading_args: ["--system", "-o", "json", "-q", "--no-pager"]
				.iter()
				.map(|s| s.to_string())
				.collect(),
			entry_cap: JOURNAL_ENTRY_CAP,
			byte_cap: JOURNAL_BYTE_CAP,
			duration_bound: JOURNAL_DURATION_BOUND,
		}
	}

	/// Collect the journal for `[since, until]` (unix ms). Output is read in bounded chunks with
	/// the byte cap enforced before anything is buffered, the entry cap applied per record, and
	/// the deadline observed by every reader through `poll`, so a descendant that outlives the
	/// command holding a pipe cannot hold the collection past the bound; stderr is drained with
	/// only a bounded prefix retained; the deadline is enforced until the command has exited and
	/// both pipes have reached EOF; stdout EOF while the command is still finishing is not a
	/// termination, a cap or a read failure is.
	pub fn collect(&self, since_unix_ms: f64, until_unix_ms: f64) -> JournalCollection {
		let since = format!("@{:.3}", since_unix_ms / 1000.0);
		let until = format!("@{:.3}", until_unix_ms / 1000.0);
		let mut args = self.leading_args.clone();
		args.extend(["--since".to_string(), since, "--until".to_string(), until]);
		let command = format!("{} {}", self.program, args.join(" "));
		let mut collection = JournalCollection {
			since_unix_ms,
			until_unix_ms,
			state: JournalState::Unavailable("not run".to_string()),
			entries: Vec::new(),
			malformed_records: 0,
			bytes_read: 0,
			entry_cap: self.entry_cap,
			byte_cap: self.byte_cap,
			duration_bound_ms: self.duration_bound.as_millis() as u64,
			command,
		};
		let started = Instant::now();
		let deadline = started + self.duration_bound;
		// The command runs in its own process group so terminating it terminates whatever it
		// spawned as well.
		let mut child = match {
			let mut command = std::process::Command::new(&self.program);
			command
				.args(&args)
				.stdin(std::process::Stdio::null())
				.stdout(std::process::Stdio::piped())
				.stderr(std::process::Stdio::piped());
			std::os::unix::process::CommandExt::process_group(&mut command, 0);
			command.spawn()
		} {
			Ok(child) => child,
			Err(error) => {
				collection.state =
					JournalState::Unavailable(format!("spawn {}: {error}", self.program));
				return collection;
			}
		};
		let stderr = child.stderr.take().expect("piped stderr");
		let stderr_reader = std::thread::spawn(move || drain_bounded(stderr, 2_000, deadline));
		let stdout = child.stdout.take().expect("piped stdout");
		let entry_cap = self.entry_cap;
		let byte_cap = self.byte_cap;
		// Why the stdout reader stopped, visible before the join: EOF is not a reason to
		// terminate a command still finishing; a cap, an error, or the deadline is.
		let stop_reason = std::sync::Arc::new(std::sync::atomic::AtomicU8::new(STOP_RUNNING));
		let reader_stop = stop_reason.clone();
		let stdout_reader = std::thread::spawn(move || {
			let result = read_records(stdout, entry_cap, byte_cap, deadline);
			let reason = if result.cut.is_some() || result.error.is_some() || result.deadline_hit {
				STOP_CUT
			} else {
				STOP_EOF
			};
			reader_stop.store(reason, std::sync::atomic::Ordering::SeqCst);
			result
		});
		// Enforce the deadline until the command has exited and both readers have finished. A
		// reader that stopped on a cap or an error while the command still runs terminates the
		// command; a reader at EOF does not, since the command may still be finishing.
		let mut status: Option<std::process::ExitStatus> = None;
		let mut timed_out = false;
		let mut wait_error = None;
		loop {
			if status.is_none() {
				match child.try_wait() {
					Ok(Some(exited)) => status = Some(exited),
					Ok(None) => {}
					Err(error) => {
						wait_error = Some(format!("wait: {error}"));
						terminate_group(&mut child);
						status = child.wait().ok();
					}
				}
			}
			let stdout_done = stdout_reader.is_finished();
			let stderr_done = stderr_reader.is_finished();
			if status.is_some() && stdout_done && stderr_done {
				break;
			}
			if status.is_none() && stop_reason.load(std::sync::atomic::Ordering::SeqCst) == STOP_CUT
			{
				// A cap or a read failure stopped the reader while the command still runs:
				// terminate the group now rather than at the deadline.
				terminate_group(&mut child);
				status = child.wait().ok();
				continue;
			}
			if Instant::now() >= deadline {
				timed_out = true;
				terminate_group(&mut child);
				status = status.or_else(|| child.wait().ok());
				break;
			}
			std::thread::sleep(Duration::from_millis(10));
		}
		let stdout_result = stdout_reader
			.join()
			.unwrap_or_else(|_| RecordRead::failed("reader thread panicked"));
		if status.is_none() {
			// The command is still running after a reader stopped (a cap or a read failure), or
			// the deadline passed: terminate and reap it.
			terminate_group(&mut child);
			status = child.wait().ok();
		}
		let stderr_result = stderr_reader.join().unwrap_or_else(|_| Drained {
			prefix: String::new(),
			deadline_hit: false,
			error: Some("stderr drain thread panicked".to_string()),
		});
		if stderr_result.deadline_hit || stderr_result.error.is_some() {
			// Something still held stderr at the deadline, or the drain failed: whatever remains
			// of the group is terminated, and the collection cannot claim a clean completion.
			terminate_group(&mut child);
			let _ = child.try_wait();
		}
		let stderr_text = stderr_result.prefix;
		collection.entries = stdout_result.entries;
		collection.malformed_records = stdout_result.malformed;
		collection.bytes_read = stdout_result.bytes;
		let kept = collection.entries.len();
		collection.state = if let Some(error) = wait_error {
			JournalState::Unavailable(error)
		} else if timed_out || stdout_result.deadline_hit || stderr_result.deadline_hit {
			JournalState::Truncated(format!(
				"duration bound {} ms hit{}; {kept} entries kept",
				self.duration_bound.as_millis(),
				if stderr_result.deadline_hit && !stdout_result.deadline_hit && !timed_out {
					" (stderr still open at the deadline)"
				} else {
					""
				}
			))
		} else if let Some(cut) = stdout_result.cut {
			JournalState::Truncated(format!("{cut}; {kept} entries kept"))
		} else if let Some(error) = stdout_result.error {
			JournalState::Degraded(error)
		} else if let Some(error) = stderr_result.error {
			JournalState::Degraded(format!("stderr drain: {error}"))
		} else if kept == 0 && status.is_some_and(|s| !s.success()) {
			JournalState::Unavailable(format!(
				"{} exited {}: {}",
				self.program,
				status.map(|s| s.to_string()).unwrap_or_default(),
				stderr_text.trim()
			))
		} else if stdout_result.malformed > 0 {
			JournalState::Degraded(format!(
				"{} malformed records skipped",
				stdout_result.malformed
			))
		} else if status.is_some_and(|s| !s.success()) {
			JournalState::Degraded(format!(
				"{} exited {} after {kept} entries: {}",
				self.program,
				status.map(|s| s.to_string()).unwrap_or_default(),
				stderr_text.trim()
			))
		} else {
			JournalState::Collected
		};
		collection
	}
}

const STOP_RUNNING: u8 = 0;
const STOP_EOF: u8 = 1;
const STOP_CUT: u8 = 2;

/// What the stdout reader found and why it stopped.
struct RecordRead {
	entries: Vec<JournalEntry>,
	malformed: usize,
	bytes: usize,
	/// A cap was hit (entries or bytes), named; the entries are a prefix.
	cut: Option<String>,
	/// The deadline passed while the pipe was still open.
	deadline_hit: bool,
	/// A read failed.
	error: Option<String>,
}

impl RecordRead {
	fn failed(reason: &str) -> Self {
		Self {
			entries: Vec::new(),
			malformed: 0,
			bytes: 0,
			cut: None,
			deadline_hit: false,
			error: Some(reason.to_string()),
		}
	}
}

/// Wait for the fd to be readable, until `deadline`. `Ok(true)` readable, `Ok(false)` deadline.
fn poll_readable(fd: i32, deadline: Instant) -> Result<bool, String> {
	loop {
		let remaining = deadline.saturating_duration_since(Instant::now());
		if remaining.is_zero() {
			return Ok(false);
		}
		let mut fds = libc::pollfd {
			fd,
			events: libc::POLLIN,
			revents: 0,
		};
		let timeout = remaining.as_millis().min(50) as i32;
		// SAFETY: one pollfd, its count, and a timeout; poll writes revents only.
		let ready = unsafe { libc::poll(&mut fds, 1, timeout) };
		if ready < 0 {
			let error = std::io::Error::last_os_error();
			if error.kind() == std::io::ErrorKind::Interrupted {
				continue;
			}
			return Err(format!("poll: {error}"));
		}
		if ready > 0 {
			return Ok(true);
		}
	}
}

/// Read one chunk into `buffer`; `Ok(0)` is EOF.
fn read_chunk(fd: i32, buffer: &mut [u8]) -> Result<usize, String> {
	loop {
		// SAFETY: the buffer is valid for its length; read writes at most that many bytes.
		let n = unsafe { libc::read(fd, buffer.as_mut_ptr().cast(), buffer.len()) };
		if n < 0 {
			let error = std::io::Error::last_os_error();
			if error.kind() == std::io::ErrorKind::Interrupted {
				continue;
			}
			return Err(format!("read: {error}"));
		}
		return Ok(n as usize);
	}
}

/// Read `journalctl -o json` records from the pipe in bounded chunks. The byte cap is checked
/// on the bytes read, before any of them are buffered, so a record without a newline cannot
/// grow the buffer past the cap; whitespace counts like anything else. Records are cut at the
/// entry cap. The deadline is observed between chunks through `poll`.
fn read_records(
	stdout: std::process::ChildStdout,
	entry_cap: usize,
	byte_cap: usize,
	deadline: Instant,
) -> RecordRead {
	use std::os::unix::io::AsRawFd;
	let fd = stdout.as_raw_fd();
	let mut chunk = vec![0u8; 64 * 1024];
	let mut pending: Vec<u8> = Vec::new();
	let mut result = RecordRead {
		entries: Vec::new(),
		malformed: 0,
		bytes: 0,
		cut: None,
		deadline_hit: false,
		error: None,
	};
	let consume_line = |line: &[u8], result: &mut RecordRead| -> bool {
		let text = String::from_utf8_lossy(line);
		if text.trim().is_empty() {
			return true;
		}
		if result.entries.len() >= entry_cap {
			result.cut = Some(format!("entry cap {entry_cap} hit"));
			return false;
		}
		match parse_journal_record(&text) {
			Some(entry) => result.entries.push(entry),
			None => result.malformed += 1,
		}
		true
	};
	'outer: loop {
		match poll_readable(fd, deadline) {
			Ok(true) => {}
			Ok(false) => {
				result.deadline_hit = true;
				break;
			}
			Err(error) => {
				result.error = Some(error);
				break;
			}
		}
		let n = match read_chunk(fd, &mut chunk) {
			Ok(n) => n,
			Err(error) => {
				result.error = Some(error);
				break;
			}
		};
		if n == 0 {
			// EOF: whatever is pending is the last record.
			if !pending.is_empty() {
				let last = std::mem::take(&mut pending);
				consume_line(&last, &mut result);
			}
			break;
		}
		if result.bytes + n > byte_cap {
			// Refused before buffering: the cap bounds memory, not only the tally.
			result.bytes = byte_cap;
			result.cut = Some(format!("byte cap {byte_cap} hit"));
			break;
		}
		result.bytes += n;
		let mut start = 0;
		for (index, byte) in chunk[..n].iter().enumerate() {
			if *byte == b'\n' {
				pending.extend_from_slice(&chunk[start..index]);
				let line = std::mem::take(&mut pending);
				if !consume_line(&line, &mut result) {
					break 'outer;
				}
				start = index + 1;
			}
		}
		pending.extend_from_slice(&chunk[start..n]);
	}
	// Dropping `stdout` closes our read end.
	drop(stdout);
	result
}

/// What the stderr drain kept and why it stopped: EOF, the deadline, or a failure. A drain that
/// stopped at the deadline means the pipe was still open, which is a descendant holding it,
/// and the collection must say so rather than report a clean completion.
struct Drained {
	prefix: String,
	deadline_hit: bool,
	error: Option<String>,
}

/// Drain a pipe to EOF or the deadline, retaining only the first `keep` bytes as text.
fn drain_bounded(stderr: std::process::ChildStderr, keep: usize, deadline: Instant) -> Drained {
	use std::os::unix::io::AsRawFd;
	let fd = stderr.as_raw_fd();
	let mut chunk = vec![0u8; 64 * 1024];
	let mut kept: Vec<u8> = Vec::new();
	let mut deadline_hit = false;
	let mut error = None;
	loop {
		match poll_readable(fd, deadline) {
			Ok(true) => {}
			Ok(false) => {
				deadline_hit = true;
				break;
			}
			Err(reason) => {
				error = Some(reason);
				break;
			}
		}
		match read_chunk(fd, &mut chunk) {
			Ok(0) => break,
			Err(reason) => {
				error = Some(reason);
				break;
			}
			Ok(n) => {
				if kept.len() < keep {
					let take = (keep - kept.len()).min(n);
					kept.extend_from_slice(&chunk[..take]);
				}
			}
		}
	}
	drop(stderr);
	Drained {
		prefix: String::from_utf8_lossy(&kept).to_string(),
		deadline_hit,
		error,
	}
}

/// Kill the command's whole process group, so grandchildren holding the pipes die with it.
fn terminate_group(child: &mut std::process::Child) {
	let pid = child.id() as i32;
	// SAFETY: a negative pid addresses the process group the command was started in.
	unsafe {
		libc::kill(-pid, libc::SIGKILL);
	}
	let _ = child.kill();
}

/// Collect the system journal for `[since, until]` (unix ms) with the system collector.
pub fn collect_journal(since_unix_ms: f64, until_unix_ms: f64) -> JournalCollection {
	JournalCollector::system().collect(since_unix_ms, until_unix_ms)
}

/// The entries of `collection` whose realtime lies in `[since, until]` (unix ms).
pub fn join_window<'a>(
	collection: &'a JournalCollection,
	since_unix_ms: f64,
	until_unix_ms: f64,
) -> Vec<&'a JournalEntry> {
	collection
		.entries
		.iter()
		.filter(|entry| {
			let ms = entry.realtime_us as f64 / 1000.0;
			ms >= since_unix_ms && ms <= until_unix_ms
		})
		.collect()
}

// -------------------------------------------------------------------------------------------
// The driver's own scope: cgroup, sessions, unit settings.
// -------------------------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct DriverScope {
	pub cgroup: String,
	pub unit: Option<String>,
	pub unix_session_id: i32,
	pub audit_session_id: Option<String>,
	pub unit_settings: Option<BTreeMap<String, String>>,
	pub unit_settings_unavailable: Option<String>,
}

pub const UNIT_SETTINGS: [&str; 7] = [
	"CPUQuotaPerSecUSec",
	"CPUWeight",
	"MemoryMax",
	"TasksMax",
	"Nice",
	"CPUAffinity",
	"OOMScoreAdjust",
];

/// Refuse a cgroup path inside a login session scope; return the unit name at its tail.
pub fn check_cgroup(cgroup_text: &str) -> Result<(String, Option<String>), String> {
	let line = cgroup_text
		.lines()
		.find(|line| line.starts_with("0::"))
		.ok_or_else(|| "cgroup file has no unified (0::) line".to_string())?;
	let path = line.trim_start_matches("0::").trim().to_string();
	if path
		.split('/')
		.any(|part| part.starts_with("session-") && part.ends_with(".scope"))
	{
		return Err(format!(
			"driver runs inside a login session scope ({path}); start it as a transient service"
		));
	}
	let unit = path
		.rsplit('/')
		.next()
		.filter(|tail| tail.ends_with(".service") || tail.ends_with(".scope"))
		.map(str::to_string);
	Ok((path, unit))
}

pub fn observe_driver_scope() -> Result<DriverScope, String> {
	let cgroup_text = std::fs::read_to_string("/proc/self/cgroup")
		.map_err(|error| format!("/proc/self/cgroup: {error}"))?;
	let (cgroup, unit) = check_cgroup(&cgroup_text)?;
	// SAFETY: getsid(0) queries the calling process.
	let unix_session_id = unsafe { libc::getsid(0) };
	let audit_session_id = std::fs::read_to_string("/proc/self/sessionid")
		.ok()
		.map(|s| s.trim().to_string());
	let (unit_settings, unit_settings_unavailable) = match &unit {
		Some(unit) => {
			let output = std::process::Command::new("systemctl")
				.args(["--user", "show", unit, "-p", &UNIT_SETTINGS.join(",")])
				.output();
			match output {
				Ok(output) if output.status.success() => {
					let text = String::from_utf8_lossy(&output.stdout);
					let map = text
						.lines()
						.filter_map(|line| line.split_once('='))
						.map(|(k, v)| (k.to_string(), v.to_string()))
						.collect();
					(Some(map), None)
				}
				Ok(output) => (
					None,
					Some(format!(
						"systemctl --user show {unit}: {}",
						String::from_utf8_lossy(&output.stderr).trim()
					)),
				),
				Err(error) => (None, Some(format!("systemctl: {error}"))),
			}
		}
		None => (None, Some("cgroup tail is not a unit".to_string())),
	};
	Ok(DriverScope {
		cgroup,
		unit,
		unix_session_id,
		audit_session_id,
		unit_settings,
		unit_settings_unavailable,
	})
}

#[cfg(test)]
mod tests {
	use super::*;

	fn tree(name: &str) -> PathBuf {
		let dir = std::env::temp_dir().join(format!("adr593-{name}-{}", std::process::id()));
		let _ = std::fs::remove_dir_all(&dir);
		std::fs::create_dir_all(dir.join("hwmon/hwmon0")).unwrap();
		std::fs::write(dir.join("hwmon/hwmon0/name"), "acpitz\n").unwrap();
		std::fs::write(dir.join("hwmon/hwmon0/temp1_input"), "27800\n").unwrap();
		std::fs::create_dir_all(dir.join("hwmon/hwmon5")).unwrap();
		std::fs::write(dir.join("hwmon/hwmon5/name"), "coretemp\n").unwrap();
		std::fs::write(dir.join("hwmon/hwmon5/temp1_label"), "Package id 0\n").unwrap();
		std::fs::write(dir.join("hwmon/hwmon5/temp1_input"), "42000\n").unwrap();
		std::fs::write(dir.join("hwmon/hwmon5/temp2_label"), "Core 0\n").unwrap();
		std::fs::write(dir.join("hwmon/hwmon5/temp2_input"), "39000\n").unwrap();
		for cpu in 0..4 {
			let d = dir.join(format!("cpu/cpu{cpu}/cpufreq"));
			std::fs::create_dir_all(&d).unwrap();
			std::fs::write(d.join("scaling_cur_freq"), format!("{}\n", 5_000_000 + cpu)).unwrap();
		}
		std::fs::write(
			dir.join("status"),
			"Name:\tx\nPid:\t1\nvoluntary_ctxt_switches:\t10\nnonvoluntary_ctxt_switches:\t3\n",
		)
		.unwrap();
		dir
	}

	fn roots(dir: &Path) -> HostReadingRoots {
		HostReadingRoots {
			hwmon: dir.join("hwmon"),
			cpu: dir.join("cpu"),
			thread_status: dir.join("status"),
		}
	}

	#[test]
	fn a_complete_tree_yields_every_field_and_only_the_masked_cpus() {
		let dir = tree("complete");
		let cpus: BTreeSet<usize> = [0, 2].into_iter().collect();
		let reading = take_reading(&roots(&dir), &cpus);
		assert_eq!(reading.package_temp_millic, Some(42000));
		assert_eq!(
			reading.cpu_freq_khz.as_ref().unwrap(),
			&[(0usize, 5_000_000u64), (2, 5_000_002)]
				.into_iter()
				.collect()
		);
		assert_eq!(
			reading.ctxt_switches,
			Some(CtxtSwitches {
				voluntary: 10,
				nonvoluntary: 3
			})
		);
		assert!(reading.missing.is_empty(), "{:?}", reading.missing);
		assert!(reading.monotonic_ns_last >= reading.monotonic_ns_first);
		assert_eq!(reading.pid, std::process::id());
		assert_eq!(reading.tid, current_tid());
		std::fs::remove_dir_all(&dir).unwrap();
	}

	#[test]
	fn each_missing_source_is_null_with_its_reason_and_the_others_intact() {
		let dir = tree("missing");
		let cpus: BTreeSet<usize> = [0, 1].into_iter().collect();
		// No coretemp device at all.
		std::fs::write(dir.join("hwmon/hwmon5/name"), "other\n").unwrap();
		let reading = take_reading(&roots(&dir), &cpus);
		assert_eq!(reading.package_temp_millic, None);
		assert!(
			reading
				.missing
				.iter()
				.any(|m| m.starts_with("package_temp_millic: no hwmon device named coretemp"))
		);
		assert!(reading.cpu_freq_khz.is_some() && reading.ctxt_switches.is_some());
		std::fs::write(dir.join("hwmon/hwmon5/name"), "coretemp\n").unwrap();
		// coretemp without the package label.
		std::fs::write(dir.join("hwmon/hwmon5/temp1_label"), "Core 9\n").unwrap();
		let reading = take_reading(&roots(&dir), &cpus);
		assert!(
			reading
				.missing
				.iter()
				.any(|m| m.contains("no input labelled"))
		);
		std::fs::write(dir.join("hwmon/hwmon5/temp1_label"), "Package id 0\n").unwrap();
		// One masked CPU's cpufreq file is gone: the whole source is null, the CPU named.
		std::fs::remove_file(dir.join("cpu/cpu1/cpufreq/scaling_cur_freq")).unwrap();
		let reading = take_reading(&roots(&dir), &cpus);
		assert_eq!(reading.cpu_freq_khz, None);
		assert!(
			reading
				.missing
				.iter()
				.any(|m| m.starts_with("cpu_freq_khz: cpu1:"))
		);
		assert_eq!(reading.package_temp_millic, Some(42000));
		// No status file.
		std::fs::remove_file(dir.join("status")).unwrap();
		let reading = take_reading(&roots(&dir), &cpus);
		assert_eq!(reading.ctxt_switches, None);
		assert!(
			reading
				.missing
				.iter()
				.any(|m| m.starts_with("ctxt_switches:"))
		);
		std::fs::remove_dir_all(&dir).unwrap();
	}

	#[test]
	fn coretemp_is_found_by_name_and_label_at_any_index() {
		let dir = tree("index");
		std::fs::rename(dir.join("hwmon/hwmon5"), dir.join("hwmon/hwmon11")).unwrap();
		assert_eq!(read_package_temp_millic(&dir.join("hwmon")).unwrap(), 42000);
		std::fs::remove_dir_all(&dir).unwrap();
	}

	fn reading(pid: u32, tid: u32, v: u64, nv: u64) -> HostReading {
		HostReading {
			unix_ms_first: 0.0,
			monotonic_ns_first: 0,
			unix_ms_last: 0.0,
			monotonic_ns_last: 0,
			pid,
			tid,
			package_temp_millic: None,
			cpu_freq_khz: None,
			ctxt_switches: Some(CtxtSwitches {
				voluntary: v,
				nonvoluntary: nv,
			}),
			elapsed_us: 0,
			missing: Vec::new(),
		}
	}

	#[test]
	fn the_ctxt_switch_delta_is_suppressed_across_threads_or_decreasing_counters() {
		let pair = HostReadingsPair {
			start: reading(7, 8, 10, 3),
			end: reading(7, 8, 14, 3),
		};
		assert_eq!(
			pair.ctxt_switch_delta().unwrap(),
			CtxtSwitches {
				voluntary: 4,
				nonvoluntary: 0
			}
		);
		let other_thread = HostReadingsPair {
			start: reading(7, 8, 10, 3),
			end: reading(7, 9, 14, 3),
		};
		assert!(
			other_thread
				.ctxt_switch_delta()
				.unwrap_err()
				.contains("different threads")
		);
		let decreased = HostReadingsPair {
			start: reading(7, 8, 10, 3),
			end: reading(7, 8, 9, 3),
		};
		assert!(
			decreased
				.ctxt_switch_delta()
				.unwrap_err()
				.contains("decreased")
		);
		let json = decreased.json_value();
		assert_eq!(json["ctxt_switch_delta"]["reportable"], false);
	}

	#[test]
	fn journal_lines_parse_with_timestamps_boot_id_and_the_cap() {
		let line = r#"{"__REALTIME_TIMESTAMP":"1789499669000000","__MONOTONIC_TIMESTAMP":"77931525589","_BOOT_ID":"882b9409","_SYSTEMD_UNIT":"anacron.service","MESSAGE":"Started anacron.service"}"#;
		let bytes = r#"{"__REALTIME_TIMESTAMP":"1789499670000000","SYSLOG_IDENTIFIER":"kernel","MESSAGE":[104,105]}"#;
		let (entries, malformed, truncated) =
			parse_journal_lines(&format!("{line}\n{bytes}\nnot json\n"));
		assert!(!truncated);
		assert_eq!(
			malformed, 1,
			"the non-JSON line is counted, not dropped silently"
		);
		assert_eq!(entries.len(), 2);
		assert_eq!(entries[0].monotonic_us, Some(77931525589));
		assert_eq!(entries[0].boot_id.as_deref(), Some("882b9409"));
		assert_eq!(entries[0].source.as_deref(), Some("anacron.service"));
		assert_eq!(entries[1].source.as_deref(), Some("kernel"));
		assert_eq!(entries[1].message, "hi");
		let many: String = (0..JOURNAL_ENTRY_CAP + 5)
			.map(|i| {
				format!(
					r#"{{"__REALTIME_TIMESTAMP":"{}","MESSAGE":"m"}}"#,
					1_000_000 + i * 1000
				)
			})
			.collect::<Vec<_>>()
			.join("\n");
		let (entries, malformed, truncated) = parse_journal_lines(&many);
		assert!(truncated);
		assert_eq!(malformed, 0);
		assert_eq!(entries.len(), JOURNAL_ENTRY_CAP);
		let collection = JournalCollection {
			since_unix_ms: 0.0,
			until_unix_ms: 1e15,
			state: JournalState::Collected,
			entries,
			malformed_records: 0,
			bytes_read: 0,
			entry_cap: JOURNAL_ENTRY_CAP,
			byte_cap: JOURNAL_BYTE_CAP,
			duration_bound_ms: 30_000,
			command: String::new(),
		};
		let joined = join_window(&collection, 1000.0, 1002.0);
		assert_eq!(joined.len(), 3, "entries at 1000, 1001, 1002 ms");
	}

	fn script_collector(
		script: &str,
		entry_cap: usize,
		byte_cap: usize,
		bound_ms: u64,
	) -> JournalCollector {
		JournalCollector {
			program: "sh".to_string(),
			leading_args: vec!["-c".to_string(), script.to_string(), "sh".to_string()],
			entry_cap,
			byte_cap,
			duration_bound: Duration::from_millis(bound_ms),
		}
	}

	const RECORD: &str = r#"{"__REALTIME_TIMESTAMP":"1789499669000000","MESSAGE":"m"}"#;

	#[test]
	fn the_collector_bounds_reading_and_reports_every_way_a_collection_falls_short() {
		// Oversized output: the entry cap stops reading while the command still writes; the
		// command is terminated and reaped, the kept entries are a prefix, the state says so.
		let flood = format!("while :; do echo '{RECORD}'; done");
		let started = Instant::now();
		let collection = script_collector(&flood, 50, 1 << 20, 5_000).collect(0.0, 1000.0);
		assert!(
			started.elapsed() < Duration::from_secs(4),
			"terminated at the cap, not the bound"
		);
		assert_eq!(collection.entries.len(), 50);
		assert!(
			matches!(&collection.state, JournalState::Truncated(d) if d.contains("entry cap 50")),
			"{:?}",
			collection.state
		);
		// Byte cap, likewise, and the tally never exceeds the cap.
		let collection = script_collector(&flood, 1_000_000, 4_096, 5_000).collect(0.0, 1000.0);
		assert!(
			matches!(&collection.state, JournalState::Truncated(d) if d.contains("byte cap 4096")),
			"{:?}",
			collection.state
		);
		assert!(collection.bytes_read <= 4_096);
		// A single record without a newline larger than the byte cap is refused before it is
		// buffered: the cap bounds memory, not only the count of complete lines.
		let one_line = "head -c 300000 /dev/zero | tr '\\0' 'x'; echo";
		let collection = script_collector(one_line, 2000, 65_536, 5_000).collect(0.0, 1000.0);
		assert!(
			matches!(&collection.state, JournalState::Truncated(d) if d.contains("byte cap 65536")),
			"{:?}",
			collection.state
		);
		assert!(collection.bytes_read <= 65_536);
		assert!(collection.entries.is_empty());
		// Whitespace-only lines count against the byte cap like anything else.
		let spaces = "while :; do echo '        '; done";
		let collection = script_collector(spaces, 2000, 8_192, 5_000).collect(0.0, 1000.0);
		assert!(
			matches!(&collection.state, JournalState::Truncated(d) if d.contains("byte cap 8192")),
			"{:?}",
			collection.state
		);
		// Stderr saturation: a command that floods stderr while emitting records must not stall,
		// and only a bounded prefix of stderr is retained (surfaced through a failure reason).
		let noisy = format!(
			"head -c 10000000 /dev/zero | tr '\\0' 'e' 1>&2; echo '{RECORD}'; echo '{RECORD}'"
		);
		let collection = script_collector(&noisy, 2000, 1 << 24, 20_000).collect(0.0, 1000.0);
		assert_eq!(
			collection.state,
			JournalState::Collected,
			"{:?}",
			collection.state
		);
		assert_eq!(collection.entries.len(), 2);
		let noisy_failure = "head -c 10000000 /dev/zero | tr '\\0' 'e' 1>&2; exit 1";
		let collection =
			script_collector(noisy_failure, 2000, 1 << 24, 20_000).collect(0.0, 1000.0);
		match &collection.state {
			JournalState::Unavailable(detail) => {
				assert!(detail.len() < 2_200, "bounded prefix: {}", detail.len())
			}
			other => panic!("{other:?}"),
		}
		// Timeout: a command that hangs is terminated at the duration bound and reaped.
		let hang = format!("echo '{RECORD}'; sleep 30");
		let started = Instant::now();
		let collection = script_collector(&hang, 2000, 1 << 24, 300).collect(0.0, 1000.0);
		assert!(
			started.elapsed() < Duration::from_secs(5),
			"terminated at the bound"
		);
		assert!(
			matches!(&collection.state, JournalState::Truncated(d) if d.contains("duration bound")),
			"{:?}",
			collection.state
		);
		assert_eq!(collection.entries.len(), 1);
		// A descendant that outlives the command holding the pipe: the command exits at once,
		// `sleep` keeps stdout open, and the bound still holds; the collection cannot claim EOF.
		let orphan = format!("echo '{RECORD}'; sleep 30 & exit 0");
		let started = Instant::now();
		let collection = script_collector(&orphan, 2000, 1 << 24, 300).collect(0.0, 1000.0);
		assert!(
			started.elapsed() < Duration::from_secs(5),
			"bound held past the command's exit"
		);
		assert!(
			matches!(&collection.state, JournalState::Truncated(d) if d.contains("duration bound")),
			"{:?}",
			collection.state
		);
		assert_eq!(collection.entries.len(), 1);
		// A descendant holding only stderr: stdout reaches EOF and the command exits at once,
		// but the drain cannot reach EOF; the deadline classifies it, never Collected.
		let stderr_orphan = format!("echo '{RECORD}'; sleep 30 >/dev/null & exit 0");
		let mut clean_verdicts = 0;
		for _ in 0..5 {
			let started = Instant::now();
			let collection =
				script_collector(&stderr_orphan, 2000, 1 << 24, 100).collect(0.0, 1000.0);
			assert!(
				started.elapsed() < Duration::from_secs(5),
				"bound held with only stderr open"
			);
			assert_eq!(collection.entries.len(), 1);
			match &collection.state {
				JournalState::Truncated(d) if d.contains("duration bound") => {}
				JournalState::Collected => clean_verdicts += 1,
				other => panic!("{other:?}"),
			}
		}
		assert_eq!(
			clean_verdicts, 0,
			"a stderr-only orphan must never read as Collected"
		);
		// Conversely, stdout reaching EOF while the command is still legitimately finishing is
		// not a termination: it closes stdout, works on, exits 0 inside the bound, and the
		// collection is clean.
		let finishing = format!("echo '{RECORD}'; exec 1>&-; sleep 0.3; exit 0");
		let collection = script_collector(&finishing, 2000, 1 << 24, 5_000).collect(0.0, 1000.0);
		assert_eq!(
			collection.state,
			JournalState::Collected,
			"{:?}",
			collection.state
		);
		assert_eq!(collection.entries.len(), 1);
		// Malformed records: counted and the state degraded, never Collected.
		let malformed = format!(
			"echo '{RECORD}'; echo 'not json'; echo '{{\"no\":\"timestamp\"}}'; echo '{RECORD}'"
		);
		let collection = script_collector(&malformed, 2000, 1 << 24, 5_000).collect(0.0, 1000.0);
		assert_eq!(collection.entries.len(), 2);
		assert_eq!(collection.malformed_records, 2);
		assert!(
			matches!(&collection.state, JournalState::Degraded(d) if d.contains("2 malformed")),
			"{:?}",
			collection.state
		);
		// A failing command with no output is unavailable, with its stderr in the reason.
		let failing = "echo 'no permission' 1>&2; exit 1";
		let collection = script_collector(failing, 2000, 1 << 24, 5_000).collect(0.0, 1000.0);
		assert!(
			matches!(&collection.state, JournalState::Unavailable(d) if d.contains("no permission")),
			"{:?}",
			collection.state
		);
		// A command that fails after emitting records is degraded, not collected.
		let partial = format!("echo '{RECORD}'; exit 3");
		let collection = script_collector(&partial, 2000, 1 << 24, 5_000).collect(0.0, 1000.0);
		assert!(
			matches!(&collection.state, JournalState::Degraded(d) if d.contains("exited")),
			"{:?}",
			collection.state
		);
		// A missing program is unavailable.
		let collection = JournalCollector {
			program: "/nonexistent/journalctl".to_string(),
			..script_collector("", 1, 1, 1)
		}
		.collect(0.0, 1000.0);
		assert!(matches!(&collection.state, JournalState::Unavailable(d) if d.contains("spawn")));
	}

	#[test]
	fn an_unavailable_journal_is_named_not_empty() {
		// A window in the far future on a host without journal access, or with it, must never
		// come back as a silent empty collection: the state says what happened.
		let collection = collect_journal(1e15, 1e15 + 1000.0);
		assert!(matches!(
			collection.state,
			JournalState::Collected
				| JournalState::Unavailable(_)
				| JournalState::Truncated(_)
				| JournalState::Degraded(_)
		));
		assert_eq!(collection.entry_cap, JOURNAL_ENTRY_CAP);
	}

	#[test]
	fn the_readings_bracket_the_span_and_their_cost_stays_outside_it() {
		enable();
		let start = begin().expect("enabled");
		let execute_start = Instant::now();
		std::thread::sleep(Duration::from_millis(5));
		let execute_ms = execute_start.elapsed().as_secs_f64() * 1000.0;
		finish(Some(start));
		let pair = take_last().expect("a pair was recorded");
		assert!(take_last().is_none(), "the slot is emptied by take_last");
		assert!(pair.end.monotonic_ns_first >= pair.start.monotonic_ns_last);
		let interval_ms = pair.json_value()["interval_ns"].as_u64().unwrap() as f64 / 1e6;
		assert!(
			interval_ms >= 5.0 && interval_ms >= execute_ms,
			"{interval_ms} vs {execute_ms}"
		);
		// The readings' own time is on the readings, not in the span measured between them.
		assert!(
			execute_ms < 5.0 + (pair.start.elapsed_us + pair.end.elapsed_us) as f64 / 1000.0 + 50.0
		);
		assert_eq!(pair.start.pid, pair.end.pid);
	}

	#[test]
	fn a_login_session_scope_is_refused_and_a_service_is_named() {
		let error =
			check_cgroup("0::/user.slice/user-1002.slice/session-2101.scope\n").unwrap_err();
		assert!(error.contains("login session scope"), "{error}");
		let (path, unit) = check_cgroup(
			"0::/user.slice/user-1002.slice/user@1002.service/app.slice/adr593-decomposition.service\n",
		)
		.unwrap();
		assert!(path.ends_with("adr593-decomposition.service"));
		assert_eq!(unit.as_deref(), Some("adr593-decomposition.service"));
		assert!(
			check_cgroup("1:name=systemd:/x\n")
				.unwrap_err()
				.contains("no unified")
		);
	}
}
