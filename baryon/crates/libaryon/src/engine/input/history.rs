use crate::core::write_private_state_atomically;
#[cfg(not(test))]
use std::env;
use std::ffi::OsString;
use std::fs::{self, OpenOptions};
use std::io::Write;
#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::{Arc, mpsc};
use std::thread;

pub(super) const HISTORY_LIMIT: usize = 1_000;
const HISTORY_COMPACT_KEEP: usize = 500;
const HISTORY_WRITE_QUEUE_LIMIT: usize = 2_048;
const HISTORY_ENTRY_BYTE_LIMIT: usize = 64 * 1024;

#[derive(Debug, Clone, Default)]
pub(super) struct HistoryStore {
	command_path: Option<PathBuf>,
	search_path: Option<PathBuf>,
	writer: Option<Arc<HistoryWriter>>,
}

#[derive(Debug)]
struct HistoryWriter {
	tx: Option<mpsc::SyncSender<HistoryWrite>>,
	worker: Option<thread::JoinHandle<()>>,
}

#[derive(Debug)]
enum HistoryWrite {
	Append {
		path: PathBuf,
		entry: String,
	},
	#[cfg(test)]
	Flush(mpsc::Sender<Result<(), String>>),
}

#[derive(Debug, Clone, Default)]
pub(super) struct LoadedHistory {
	pub(super) store: HistoryStore,
	pub(super) command_history: Vec<String>,
	pub(super) search_history: Vec<String>,
}

impl LoadedHistory {
	pub(super) fn empty() -> Self {
		Self::default()
	}

	#[cfg(not(test))]
	pub(super) fn load_default() -> Self {
		Self::load_from_state_dir(default_state_dir())
	}

	pub(super) fn load_from_state_dir(state_dir: Option<PathBuf>) -> Self {
		let store = HistoryStore::from_state_dir(state_dir);
		let command_history = store
			.command_path
			.as_deref()
			.map(read_history_file)
			.unwrap_or_default();
		let search_history = store
			.search_path
			.as_deref()
			.map(read_history_file)
			.unwrap_or_default();
		Self {
			store,
			command_history,
			search_history,
		}
	}
}

impl HistoryStore {
	fn from_state_dir(state_dir: Option<PathBuf>) -> Self {
		let Some(state_dir) = state_dir else {
			return Self::default();
		};
		Self {
			command_path: Some(state_dir.join("command_history")),
			search_path: Some(state_dir.join("search_history")),
			writer: Some(Arc::new(HistoryWriter::spawn())),
		}
	}

	pub(super) fn append_command_entry(&self, entry: &str) -> Result<(), std::io::Error> {
		if let Some(path) = self.command_path.as_deref() {
			self.enqueue(path, entry)?;
		}
		Ok(())
	}

	pub(super) fn append_search_entry(&self, entry: &str) -> Result<(), std::io::Error> {
		if let Some(path) = self.search_path.as_deref() {
			self.enqueue(path, entry)?;
		}
		Ok(())
	}

	fn enqueue(&self, path: &Path, entry: &str) -> Result<(), std::io::Error> {
		if entry.len() > HISTORY_ENTRY_BYTE_LIMIT {
			return Err(std::io::Error::new(
				std::io::ErrorKind::InvalidInput,
				format!("history entry exceeds {HISTORY_ENTRY_BYTE_LIMIT} bytes"),
			));
		}
		let Some(writer) = self.writer.as_deref() else {
			return Ok(());
		};
		writer.enqueue(HistoryWrite::Append {
			path: path.to_path_buf(),
			entry: entry.to_string(),
		})
	}

	#[cfg(test)]
	pub(super) fn flush(&self) -> Result<(), std::io::Error> {
		self.writer.as_deref().map_or(Ok(()), HistoryWriter::flush)
	}
}

impl HistoryWriter {
	fn spawn() -> Self {
		let (tx, rx) = mpsc::sync_channel(HISTORY_WRITE_QUEUE_LIMIT);
		let worker = thread::Builder::new()
			.name("history-writer".to_string())
			.spawn(move || Self::run(rx))
			.expect("failed to spawn history writer");
		Self {
			tx: Some(tx),
			worker: Some(worker),
		}
	}

	fn enqueue(&self, write: HistoryWrite) -> Result<(), std::io::Error> {
		let Some(tx) = self.tx.as_ref() else {
			return Err(std::io::Error::new(
				std::io::ErrorKind::BrokenPipe,
				"history writer has stopped",
			));
		};
		tx.try_send(write).map_err(|err| match err {
			mpsc::TrySendError::Full(_) => std::io::Error::new(
				std::io::ErrorKind::WouldBlock,
				"history writer queue is full",
			),
			mpsc::TrySendError::Disconnected(_) => {
				std::io::Error::new(std::io::ErrorKind::BrokenPipe, "history writer has stopped")
			}
		})
	}

	fn run(rx: mpsc::Receiver<HistoryWrite>) {
		let mut first_error = None;
		while let Ok(write) = rx.recv() {
			match write {
				HistoryWrite::Append { path, entry } => {
					if let Err(err) = append_history_file(&path, &entry)
						&& first_error.is_none()
					{
						first_error = Some(err.to_string());
					}
				}
				#[cfg(test)]
				HistoryWrite::Flush(reply) => {
					let result = first_error.take().map_or(Ok(()), Err);
					let _ = reply.send(result);
				}
			}
		}
	}

	#[cfg(test)]
	fn flush(&self) -> Result<(), std::io::Error> {
		let (reply, result) = mpsc::channel();
		self.tx
			.as_ref()
			.ok_or_else(|| {
				std::io::Error::new(std::io::ErrorKind::BrokenPipe, "history writer has stopped")
			})?
			.send(HistoryWrite::Flush(reply))
			.map_err(|_| {
				std::io::Error::new(std::io::ErrorKind::BrokenPipe, "history writer has stopped")
			})?;
		result
			.recv()
			.map_err(|_| {
				std::io::Error::new(std::io::ErrorKind::BrokenPipe, "history writer has stopped")
			})?
			.map_err(std::io::Error::other)
	}
}

impl Drop for HistoryWriter {
	fn drop(&mut self) {
		self.tx.take();
		if let Some(worker) = self.worker.take() {
			let _ = worker.join();
		}
	}
}

pub(super) fn append_history_entry(entry: &str, history: &mut Vec<String>) -> bool {
	if entry.is_empty() || history.last().is_some_and(|last| last == entry) {
		return false;
	}
	history.push(entry.to_string());
	truncate_old_history(history);
	true
}

fn read_history_file(path: &Path) -> Vec<String> {
	match read_complete_history_entries(path) {
		Ok(entries) => normalize_history(entries),
		Err(_) => Vec::new(),
	}
}

fn append_history_file(path: &Path, entry: &str) -> Result<(), std::io::Error> {
	if let Some(parent) = path.parent() {
		fs::create_dir_all(parent)?;
	}

	with_history_file_lock(path, || {
		repair_incomplete_history_file(path)?;
		append_history_line(path, entry)?;
		compact_history_file_if_needed(path)
	})
}

fn with_history_file_lock(
	path: &Path,
	action: impl FnOnce() -> Result<(), std::io::Error>,
) -> Result<(), std::io::Error> {
	let lock_path = path.with_extension("lock");

	#[cfg(unix)]
	tighten_history_file_permissions(&lock_path)?;

	let mut options = OpenOptions::new();
	options.create(true).read(true).write(true);
	#[cfg(unix)]
	options.mode(0o600);

	let lock = options.open(lock_path)?;
	lock.lock()?;
	let result = action();
	let unlock_result = lock.unlock();
	result.and(unlock_result)
}

fn append_history_line(path: &Path, entry: &str) -> Result<(), std::io::Error> {
	#[cfg(unix)]
	tighten_history_file_permissions(path)?;

	let mut options = OpenOptions::new();
	options.create(true).append(true);
	#[cfg(unix)]
	options.mode(0o600);

	let mut file = options.open(path)?;
	file.write_all(entry.as_bytes())?;
	file.write_all(b"\n")?;
	file.flush()
}

fn compact_history_file_if_needed(path: &Path) -> Result<(), std::io::Error> {
	let entries = read_complete_history_entries(path)?;
	if entries.len() <= HISTORY_LIMIT {
		return Ok(());
	}

	let mut history = normalize_history(entries);
	truncate_history_to_limit(&mut history, HISTORY_COMPACT_KEEP);
	write_compacted_history_file(path, &history)
}

fn repair_incomplete_history_file(path: &Path) -> Result<(), std::io::Error> {
	match fs::read_to_string(path) {
		Ok(contents) if !contents.is_empty() && !contents.ends_with('\n') => {
			let history = normalize_history(complete_history_entries(&contents));
			write_compacted_history_file(path, &history)
		}
		Ok(_) => Ok(()),
		Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
		Err(err) => Err(err),
	}
}

fn read_complete_history_entries(path: &Path) -> Result<Vec<String>, std::io::Error> {
	let contents = fs::read_to_string(path)?;
	Ok(complete_history_entries(&contents))
}

fn complete_history_entries(contents: &str) -> Vec<String> {
	let complete_contents = if contents.ends_with('\n') {
		contents
	} else {
		contents
			.rsplit_once('\n')
			.map(|(complete, _)| complete)
			.unwrap_or("")
	};
	complete_contents.lines().map(str::to_string).collect()
}

fn write_compacted_history_file(path: &Path, history: &[String]) -> Result<(), std::io::Error> {
	let mut contents = String::new();
	for entry in history {
		contents.push_str(entry);
		contents.push('\n');
	}
	write_private_state_atomically(path, contents.as_bytes())
}

#[cfg(unix)]
fn tighten_history_file_permissions(path: &Path) -> Result<(), std::io::Error> {
	match fs::metadata(path) {
		Ok(metadata) if metadata.permissions().mode() & 0o777 != 0o600 => {
			fs::set_permissions(path, fs::Permissions::from_mode(0o600))
		}
		Ok(_) => Ok(()),
		Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
		Err(err) => Err(err),
	}
}

fn normalize_history(entries: impl IntoIterator<Item = String>) -> Vec<String> {
	let mut history = Vec::new();
	for entry in entries {
		append_history_entry(&entry, &mut history);
	}
	history
}

fn truncate_old_history(history: &mut Vec<String>) {
	truncate_history_to_limit(history, HISTORY_LIMIT);
}

fn truncate_history_to_limit(history: &mut Vec<String>, limit: usize) {
	if history.len() > limit {
		let extra = history.len() - limit;
		history.drain(0..extra);
	}
}

#[cfg(not(test))]
fn default_state_dir() -> Option<PathBuf> {
	default_state_dir_for_platform(HistoryPlatform::current(), HistoryPathEnv::current())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HistoryPlatform {
	Xdg,
	Macos,
	Windows,
}

impl HistoryPlatform {
	#[cfg(not(test))]
	fn current() -> Self {
		if cfg!(target_os = "windows") {
			Self::Windows
		} else if cfg!(target_os = "macos") {
			Self::Macos
		} else {
			Self::Xdg
		}
	}
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct HistoryPathEnv {
	xdg_state_home: Option<OsString>,
	home: Option<OsString>,
	localappdata: Option<OsString>,
	appdata: Option<OsString>,
	userprofile: Option<OsString>,
}

impl HistoryPathEnv {
	#[cfg(not(test))]
	fn current() -> Self {
		Self {
			xdg_state_home: env::var_os("XDG_STATE_HOME"),
			home: env::var_os("HOME"),
			localappdata: env::var_os("LOCALAPPDATA"),
			appdata: env::var_os("APPDATA"),
			userprofile: env::var_os("USERPROFILE"),
		}
	}
}

fn default_state_dir_for_platform(
	platform: HistoryPlatform,
	env: HistoryPathEnv,
) -> Option<PathBuf> {
	match platform {
		HistoryPlatform::Xdg => xdg_state_dir(&env),
		HistoryPlatform::Macos => non_empty_path(env.home).map(|home| {
			home.join("Library")
				.join("Application Support")
				.join("baryon")
				.join("state")
		}),
		HistoryPlatform::Windows => non_empty_path(env.localappdata)
			.or_else(|| non_empty_path(env.appdata))
			.or_else(|| {
				non_empty_path(env.userprofile).map(|home| home.join("AppData").join("Local"))
			})
			.map(|dir| dir.join("baryon")),
	}
}

fn xdg_state_dir(env: &HistoryPathEnv) -> Option<PathBuf> {
	if let Some(xdg) = non_empty_path(env.xdg_state_home.clone()) {
		return Some(xdg.join("baryon"));
	}

	non_empty_path(env.home.clone()).map(|home| home.join(".local").join("state").join("baryon"))
}

fn non_empty_path(value: Option<OsString>) -> Option<PathBuf> {
	value.filter(|value| !value.is_empty()).map(PathBuf::from)
}

#[cfg(test)]
mod tests {
	use super::{
		HISTORY_COMPACT_KEEP, HISTORY_LIMIT, HistoryPathEnv, HistoryPlatform, LoadedHistory,
		append_history_entry, default_state_dir_for_platform, normalize_history,
	};
	use std::ffi::OsString;
	#[cfg(unix)]
	use std::os::unix::fs::PermissionsExt;
	use std::path::PathBuf;

	fn temp_history_dir(name: &str) -> PathBuf {
		let unique = std::time::SystemTime::now()
			.duration_since(std::time::UNIX_EPOCH)
			.expect("time")
			.as_nanos();
		let path = std::env::temp_dir().join(format!(
			"baryon-history-{}-{name}-{unique}",
			std::process::id()
		));
		std::fs::create_dir_all(&path).expect("create temp history dir");
		path
	}

	#[test]
	fn xdg_state_home_takes_precedence_for_history_dir() {
		let path = default_state_dir_for_platform(
			HistoryPlatform::Xdg,
			HistoryPathEnv {
				xdg_state_home: Some(OsString::from("/tmp/xdg-state")),
				home: Some(OsString::from("/home/test")),
				..HistoryPathEnv::default()
			},
		)
		.expect("state dir");
		assert_eq!(path, PathBuf::from("/tmp/xdg-state/baryon"));
	}

	#[test]
	fn xdg_state_dir_falls_back_to_home_local_state() {
		let path = default_state_dir_for_platform(
			HistoryPlatform::Xdg,
			HistoryPathEnv {
				home: Some(OsString::from("/home/test")),
				..HistoryPathEnv::default()
			},
		)
		.expect("state dir");
		assert_eq!(path, PathBuf::from("/home/test/.local/state/baryon"));
	}

	#[test]
	fn macos_state_dir_uses_application_support_state() {
		let path = default_state_dir_for_platform(
			HistoryPlatform::Macos,
			HistoryPathEnv {
				home: Some(OsString::from("/Users/test")),
				..HistoryPathEnv::default()
			},
		)
		.expect("state dir");
		assert_eq!(
			path,
			PathBuf::from("/Users/test/Library/Application Support/baryon/state")
		);
	}

	#[test]
	fn windows_state_dir_prefers_localappdata() {
		let localappdata = PathBuf::from(r"C:\Users\Test\AppData\Local");
		let path = default_state_dir_for_platform(
			HistoryPlatform::Windows,
			HistoryPathEnv {
				localappdata: Some(localappdata.as_os_str().to_os_string()),
				appdata: Some(OsString::from(r"C:\Users\Test\AppData\Roaming")),
				userprofile: Some(OsString::from(r"C:\Users\Test")),
				..HistoryPathEnv::default()
			},
		)
		.expect("state dir");
		assert_eq!(path, localappdata.join("baryon"));
	}

	#[test]
	fn history_entries_are_deduped_and_limited() {
		let mut history = normalize_history(vec![
			"".to_string(),
			"e one.md".to_string(),
			"e one.md".to_string(),
			"e two.md".to_string(),
		]);
		assert_eq!(history, vec!["e one.md", "e two.md"]);

		for i in 0..(HISTORY_LIMIT + 5) {
			append_history_entry(&format!("cmd {i}"), &mut history);
		}
		assert_eq!(history.len(), HISTORY_LIMIT);
		assert_eq!(history.last().map(String::as_str), Some("cmd 1004"));
	}

	#[test]
	fn loaded_history_round_trips_command_and_search_files() {
		let dir = temp_history_dir("roundtrip");
		let loaded = LoadedHistory::load_from_state_dir(Some(dir.clone()));
		loaded
			.store
			.append_command_entry("e one.md")
			.expect("append command");
		loaded
			.store
			.append_command_entry("w")
			.expect("append command");
		loaded
			.store
			.append_search_entry("needle")
			.expect("append search");
		loaded.store.flush().expect("flush history");

		let reloaded = LoadedHistory::load_from_state_dir(Some(dir.clone()));
		assert_eq!(reloaded.command_history, vec!["e one.md", "w"]);
		assert_eq!(reloaded.search_history, vec!["needle"]);

		let _ = std::fs::remove_dir_all(dir);
	}

	#[test]
	fn append_returns_while_the_history_file_lock_is_held() {
		let dir = temp_history_dir("nonblocking");
		let loaded = LoadedHistory::load_from_state_dir(Some(dir.clone()));
		let lock_path = dir.join("command_history.lock");
		let lock = std::fs::OpenOptions::new()
			.create(true)
			.truncate(false)
			.read(true)
			.write(true)
			.open(lock_path)
			.expect("open history lock");
		lock.lock().expect("hold history lock");

		let store = loaded.store.clone();
		let (done, result) = std::sync::mpsc::channel();
		let append = std::thread::spawn(move || {
			let _ = done.send(store.append_command_entry("w"));
		});
		result
			.recv_timeout(std::time::Duration::from_secs(1))
			.expect("append must only enqueue")
			.expect("enqueue command");
		append.join().expect("join append caller");

		lock.unlock().expect("release history lock");
		loaded.store.flush().expect("flush history");
		assert_eq!(
			std::fs::read_to_string(dir.join("command_history")).expect("history contents"),
			"w\n"
		);
		let _ = std::fs::remove_dir_all(dir);
	}

	#[test]
	fn history_entry_byte_cap_rejects_before_enqueue() {
		let dir = temp_history_dir("entry-cap");
		let loaded = LoadedHistory::load_from_state_dir(Some(dir.clone()));
		let err = loaded
			.store
			.append_command_entry(&"x".repeat(super::HISTORY_ENTRY_BYTE_LIMIT + 1))
			.expect_err("oversized history entry");
		assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
		loaded.store.flush().expect("flush empty history queue");
		assert!(!dir.join("command_history").exists());
		let _ = std::fs::remove_dir_all(dir);
	}

	#[test]
	fn appending_from_two_loaded_instances_preserves_both_entries() {
		let dir = temp_history_dir("multi-instance");
		let first = LoadedHistory::load_from_state_dir(Some(dir.clone()));
		let second = LoadedHistory::load_from_state_dir(Some(dir.clone()));

		first
			.store
			.append_command_entry("w")
			.expect("append first command");
		first.store.flush().expect("flush first command");
		second
			.store
			.append_command_entry("q")
			.expect("append second command");
		second.store.flush().expect("flush second command");

		let reloaded = LoadedHistory::load_from_state_dir(Some(dir.clone()));
		assert_eq!(reloaded.command_history, vec!["w", "q"]);

		let _ = std::fs::remove_dir_all(dir);
	}

	#[test]
	fn append_compacts_history_file_to_keep_limit() {
		let dir = temp_history_dir("compact-limit");
		let loaded = LoadedHistory::load_from_state_dir(Some(dir.clone()));

		for i in 0..(HISTORY_LIMIT + 5) {
			loaded
				.store
				.append_command_entry(&format!("cmd {i}"))
				.expect("append command");
		}
		loaded.store.flush().expect("flush history");

		let contents =
			std::fs::read_to_string(dir.join("command_history")).expect("command history file");
		let lines: Vec<_> = contents.lines().collect();
		assert_eq!(lines.len(), HISTORY_COMPACT_KEEP + 4);
		assert_eq!(lines.first(), Some(&"cmd 501"));
		assert_eq!(lines.last(), Some(&"cmd 1004"));

		let reloaded = LoadedHistory::load_from_state_dir(Some(dir.clone()));
		assert_eq!(reloaded.command_history.len(), HISTORY_COMPACT_KEEP + 4);
		assert_eq!(
			reloaded.command_history.first().map(String::as_str),
			Some("cmd 501")
		);
		assert_eq!(
			reloaded.command_history.last().map(String::as_str),
			Some("cmd 1004")
		);

		let _ = std::fs::remove_dir_all(dir);
	}

	#[test]
	fn compaction_preserves_entries_from_two_loaded_instances() {
		let dir = temp_history_dir("compact-multi-instance");
		std::fs::write(
			dir.join("command_history"),
			(0..(HISTORY_LIMIT - 1))
				.map(|n| format!("cmd {n}\n"))
				.collect::<String>(),
		)
		.expect("seed history");

		let first = LoadedHistory::load_from_state_dir(Some(dir.clone()));
		let second = LoadedHistory::load_from_state_dir(Some(dir.clone()));

		first
			.store
			.append_command_entry("w")
			.expect("append first command");
		first.store.flush().expect("flush first command");
		second
			.store
			.append_command_entry("q")
			.expect("append second command");
		second.store.flush().expect("flush second command");

		let contents =
			std::fs::read_to_string(dir.join("command_history")).expect("command history file");
		let lines: Vec<_> = contents.lines().collect();
		assert_eq!(lines.len(), HISTORY_COMPACT_KEEP);
		assert_eq!(lines[HISTORY_COMPACT_KEEP - 2], "w");
		assert_eq!(lines[HISTORY_COMPACT_KEEP - 1], "q");

		let _ = std::fs::remove_dir_all(dir);
	}

	#[test]
	fn load_ignores_incomplete_trailing_history_line() {
		let dir = temp_history_dir("partial-line");
		std::fs::write(dir.join("command_history"), "w\npartial").expect("write history");

		let reloaded = LoadedHistory::load_from_state_dir(Some(dir.clone()));
		assert_eq!(reloaded.command_history, vec!["w"]);

		let _ = std::fs::remove_dir_all(dir);
	}

	#[test]
	fn append_drops_incomplete_trailing_history_line() {
		let dir = temp_history_dir("repair-partial-line");
		std::fs::write(dir.join("command_history"), "w\npartial").expect("write history");
		let loaded = LoadedHistory::load_from_state_dir(Some(dir.clone()));

		loaded
			.store
			.append_command_entry("q")
			.expect("append command");
		loaded.store.flush().expect("flush history");

		let contents =
			std::fs::read_to_string(dir.join("command_history")).expect("command history file");
		assert_eq!(contents, "w\nq\n");

		let _ = std::fs::remove_dir_all(dir);
	}

	#[cfg(unix)]
	#[test]
	fn history_file_is_private_on_create() {
		let dir = temp_history_dir("permissions");
		let loaded = LoadedHistory::load_from_state_dir(Some(dir.clone()));

		loaded
			.store
			.append_command_entry("s/old_password/new_password/g")
			.expect("append command");
		loaded.store.flush().expect("flush history");

		let mode = std::fs::metadata(dir.join("command_history"))
			.expect("history metadata")
			.permissions()
			.mode() & 0o777;
		assert_eq!(mode, 0o600);

		let _ = std::fs::remove_dir_all(dir);
	}
}
