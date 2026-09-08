//! PRODUCT-0161: opt-in, explicitly addressed journey reports. No discovery.
mod spool;

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::{
	Arc, Mutex,
	atomic::{AtomicU64, Ordering},
	mpsc,
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Destination {
	pub endpoint: String,
	/// Directory containing client.pem, client.key, ca.pem and server_name.
	pub pki: PathBuf,
}
impl Destination {
	pub fn validate(&self) -> Result<(), String> {
		if self.endpoint.parse::<std::net::SocketAddr>().is_err() || self.pki.as_os_str().is_empty()
		{
			return Err("reporting requires an explicit IP:port and PKI directory".into());
		}
		#[cfg(not(feature = "toron"))]
		return Err("reporting requires the Toron feature".into());
		#[cfg(feature = "toron")]
		Ok(())
	}
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Step {
	#[serde(rename = "run.opened")]
	RunOpened,
	#[serde(rename = "first_run.shown")]
	FirstRunShown,
	#[serde(rename = "example.ran")]
	ExampleRan,
	#[serde(rename = "workspace.offered")]
	WorkspaceOffered,
	#[serde(rename = "workspace.started")]
	WorkspaceStarted,
	#[serde(rename = "workspace.failed")]
	WorkspaceFailed,
	#[serde(rename = "own_file.opened")]
	OwnFileOpened,
	#[serde(rename = "first_analysis.kept")]
	FirstAnalysisKept,
	#[serde(rename = "reporting.enabled")]
	ReportingEnabled,
	#[serde(rename = "reporting.disabled")]
	ReportingDisabled,
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Reason {
	#[serde(rename = "file.not_found")]
	FileNotFound,
	#[serde(rename = "file.unsupported")]
	FileUnsupported,
	#[serde(rename = "column.not_plottable")]
	ColumnNotPlottable,
	#[serde(rename = "workspace.launcher_missing")]
	WorkspaceLauncherMissing,
	#[serde(rename = "workspace.start_timeout")]
	WorkspaceStartTimeout,
	#[serde(rename = "workspace.exited")]
	WorkspaceExited,
	#[serde(rename = "example.seed_failed")]
	ExampleSeedFailed,
	#[serde(rename = "source.parquet")]
	Parquet,
	#[serde(rename = "source.csv")]
	Csv,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct Report {
	pub report_id: String,
	pub install_id: String,
	pub run_id: String,
	pub sequence: u64,
	pub journey_id: Option<String>,
	pub step: Step,
	pub outcome: String,
	pub reason: Option<Reason>,
	pub dropped: u64,
	pub platform: String,
	pub app_version: String,
	pub at_ms: u64,
}
fn id() -> String {
	uuid::Uuid::new_v4().to_string()
}
fn now() -> u64 {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.unwrap_or_default()
		.as_millis() as u64
}
pub(crate) fn state_dir() -> Option<PathBuf> {
	std::env::var_os("XDG_STATE_HOME")
		.map(PathBuf::from)
		.or_else(|| std::env::var_os("LOCALAPPDATA").map(PathBuf::from))
		.or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".local/state")))
		.map(|p| p.join("polariton"))
}

struct Event {
	sequence: u64,
	step: Step,
	reason: Option<Reason>,
	failed: bool,
	at: u64,
}
#[derive(Clone, Default)]
pub(crate) struct Reporter {
	sender: Option<mpsc::SyncSender<Event>>,
	sequence: Arc<AtomicU64>,
	dropped: Arc<AtomicU64>,
	status: Arc<Mutex<Option<String>>>,
}
impl Reporter {
	pub fn start(root: PathBuf, destination: Option<Destination>) -> Self {
		let mut reporter = Self::default();
		let status = reporter.status.clone();
		let dropped = reporter.dropped.clone();
		let Some(destination) = destination else {
			// Off has no reporting channel or writer. Only a previously enabled
			// installation needs a background revocation/cleanup task.
			std::thread::spawn(move || {
				let root = root.join("reporting");
				if !root.exists() {
					return;
				}
				let result = (|| {
					let runtime = tokio::runtime::Builder::new_current_thread()
						.enable_all()
						.build()
						.map_err(|e| e.to_string())?;
					spool::disable(&root, |d, report| {
						runtime.block_on(send(d, report, Duration::from_secs(2)))
					})
				})();
				if let Err(e) = result {
					diagnose(&status, e);
				}
			});
			return reporter;
		};
		let (tx, rx) = mpsc::sync_channel(256);
		reporter.sender = Some(tx);
		std::thread::spawn(move || {
			let result = worker(
				root.join("reporting"),
				destination,
				rx,
				dropped,
				status.clone(),
			);
			if let Err(error) = result {
				diagnose(&status, error);
			}
		});
		reporter.emit(Step::ReportingEnabled, None, false);
		reporter
	}
	pub fn emit(&self, step: Step, reason: Option<Reason>, failed: bool) {
		let Some(sender) = &self.sender else {
			return;
		};
		let event = Event {
			sequence: self.sequence.fetch_add(1, Ordering::Relaxed),
			step,
			reason,
			failed,
			at: now(),
		};
		if sender.try_send(event).is_err() {
			self.dropped.fetch_add(1, Ordering::Relaxed);
		}
	}
	pub fn detail(&self) -> Option<String> {
		self.status.try_lock().ok().and_then(|v| v.clone())
	}
}
fn diagnose(status: &Mutex<Option<String>>, error: impl std::fmt::Display) {
	let message = format!("Reporting: {error}");
	eprintln!("{message}");
	if let Ok(mut value) = status.lock() {
		*value = Some(message);
	}
}
fn worker(
	root: PathBuf,
	destination: Destination,
	rx: mpsc::Receiver<Event>,
	dropped: Arc<AtomicU64>,
	status: Arc<Mutex<Option<String>>>,
) -> Result<(), String> {
	let runtime = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()
		.map_err(|e| e.to_string())?;
	destination.validate()?;
	let mut spool = spool::Spool::open(root, destination.clone())?;
	let run = id();
	let mut segment = 0;
	let mut persisting = true;
	loop {
		let mut batch = Vec::new();
		let mut disconnected = false;
		match rx.recv_timeout(Duration::from_secs(10)) {
			Ok(event) => batch.push(event),
			Err(mpsc::RecvTimeoutError::Disconnected) => disconnected = true,
			Err(mpsc::RecvTimeoutError::Timeout) => (),
		}
		while batch.len() < 256 {
			match rx.try_recv() {
				Ok(e) => batch.push(e),
				Err(_) => break,
			}
		}
		if !persisting {
			dropped.fetch_add(batch.len() as u64, Ordering::Relaxed);
			if disconnected {
				return Ok(());
			}
			continue;
		}
		let reports: Vec<_> = batch
			.into_iter()
			.map(|e| Report {
				report_id: id(),
				install_id: spool.generation.install.clone(),
				run_id: run.clone(),
				sequence: e.sequence,
				journey_id: None,
				step: e.step,
				outcome: if e.failed { "failed" } else { "ok" }.into(),
				reason: e.reason,
				dropped: dropped.swap(0, Ordering::Relaxed),
				platform: format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH),
				app_version: env!("CARGO_PKG_VERSION").into(),
				at_ms: e.at,
			})
			.collect();
		// Seal each writer batch early, splitting at the byte bound. No
		// subsequent batch can append past a failed or abandoned reservation.
		let mut start = 0;
		while start < reports.len() {
			let mut end = start;
			let mut size = 256; // more than the fixed header
			while end < reports.len() {
				let length = bytes(&reports[end])?.len() + 1;
				if size + length > 64 * 1024 {
					break;
				}
				size += length;
				end += 1;
			}
			if end == start {
				return Err("report exceeds segment bound".into());
			}
			match spool.persist(&run, segment, &reports[start..end]) {
				Ok(evicted) => {
					dropped.fetch_add(evicted, Ordering::Relaxed);
				}
				Err(e) => {
					diagnose(&status, e);
					dropped.fetch_add(
						(reports.len() - start) as u64
							+ reports[start..].iter().map(|r| r.dropped).sum::<u64>(),
						Ordering::Relaxed,
					);
					persisting = false;
					break;
				}
			}
			segment += 1;
			start = end;
		}
		match spool.drain(|r| runtime.block_on(send(&destination, r, Duration::from_secs(5)))) {
			Ok(()) => (),
			Err(e) => diagnose(&status, e),
		}
		dropped.fetch_add(spool.take_dropped(), Ordering::Relaxed);
		if !spool.current()? || disconnected {
			return Ok(());
		}
	}
}
pub(crate) fn drain_once(destination: Option<Destination>) -> Result<(), String> {
	let destination = destination.ok_or("reporting is disabled")?;
	let root = state_dir()
		.ok_or("no application state directory")?
		.join("reporting");
	let mut spool = spool::Spool::existing(root, &destination)?;
	let runtime = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()
		.map_err(|e| e.to_string())?;
	spool.drain(|r| runtime.block_on(send(&destination, r, Duration::from_secs(5))))
}
#[cfg(feature = "toron")]
async fn send(destination: &Destination, report: &Report, timeout: Duration) -> Result<(), String> {
	use toron_client::{ToronClient, ToronClientTls};
	let profile = &destination.pki;
	let tls = ToronClientTls {
		server_name: std::fs::read_to_string(profile.join("server_name"))
			.map_err(|e| e.to_string())?
			.trim()
			.into(),
		cert_path: profile.join("client.pem"),
		key_path: profile.join("client.key"),
		ca_path: profile.join("ca.pem"),
	};
	let client =
		ToronClient::authenticated(&destination.endpoint, &tls).map_err(|e| e.to_string())?;
	tokio::time::timeout(timeout, client.execute(&insert_sql(report)))
		.await
		.map_err(|_| "report outcome unknown: timeout".to_string())?
		.map(|_| ())
		.map_err(|e| e.to_string())
}
#[cfg(not(feature = "toron"))]
async fn send(_: &Destination, _: &Report, _: Duration) -> Result<(), String> {
	Err("reporting requires Toron".into())
}
#[cfg(any(feature = "toron", test))]
fn insert_sql(report: &Report) -> String {
	let value = serde_json::to_value(report).expect("closed report schema");
	let fields = [
		"report_id",
		"install_id",
		"run_id",
		"sequence",
		"journey_id",
		"step",
		"outcome",
		"reason",
		"dropped",
		"platform",
		"app_version",
		"at_ms",
	];
	let values: Vec<_> = fields
		.iter()
		.map(|k| match &value[*k] {
			serde_json::Value::Null => "NULL".into(),
			serde_json::Value::String(s) => format!("'{}'", s.replace('\'', "''")),
			n => n.to_string(),
		})
		.collect();
	format!(
		"INSERT INTO journey_reports ({}) VALUES ({})",
		fields.join(","),
		values.join(",")
	)
}
fn read<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T, String> {
	serde_json::from_slice(&std::fs::read(path).map_err(|e| e.to_string())?)
		.map_err(|e| e.to_string())
}
fn bytes<T: Serialize>(value: &T) -> Result<Vec<u8>, String> {
	serde_json::to_vec(value).map_err(|e| e.to_string())
}
fn decode<T: for<'de> Deserialize<'de>>(data: &[u8]) -> Result<T, String> {
	serde_json::from_slice(data).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
	use super::*;
	#[test]
	fn product_0161_source_reason_wire_names_are_stable() {
		for (reason, expected) in [
			(Reason::Parquet, "source.parquet"),
			(Reason::Csv, "source.csv"),
		] {
			assert_eq!(serde_json::to_value(reason).unwrap(), expected);
			assert_eq!(
				serde_json::from_value::<Reason>(expected.into()).unwrap(),
				reason
			);
		}
	}
	#[test]
	fn product_0161_submission_is_bounded_and_off_is_silent() {
		let (sender, receiver) = mpsc::sync_channel(256);
		let reporter = Reporter {
			sender: Some(sender),
			..Reporter::default()
		};
		for _ in 0..300 {
			reporter.emit(Step::RunOpened, None, false);
		}
		assert_eq!(reporter.dropped.load(Ordering::Relaxed), 44);
		for i in 0..256 {
			assert_eq!(receiver.try_recv().unwrap().sequence, i);
		}
		assert!(receiver.try_recv().is_err());
		let off = Reporter::default();
		off.emit(Step::RunOpened, None, false);
		assert_eq!(off.sequence.load(Ordering::Relaxed), 0);
		assert_eq!(off.dropped.load(Ordering::Relaxed), 0);
		assert!(off.sender.is_none());
	}
}
