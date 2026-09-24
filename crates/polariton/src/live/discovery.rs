//! Per-window advertisements. A PID alone is never evidence of liveness.
//!
//! ADR 0166 §4: the runtime directory is private to the running user on
//! every platform. Unix keeps `0700` ownership; Windows keeps a protected
//! owner-only DACL that its files inherit. The endpoint a window advertises
//! is a Unix socket path or a Windows named pipe name, and a client checks
//! the process behind it against the advertised pid and start token before
//! sending anything.

use serde::{Deserialize, Serialize};
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct Advertisement {
	pub(crate) window: String,
	/// The Unix socket path, or on Windows the named pipe name.
	pub(crate) socket: PathBuf,
	pub(crate) pid: u32,
	pub(crate) process_start: String,
	pub(crate) session: Option<String>,
	pub(crate) sources: Vec<PathBuf>,
	pub(crate) started_at: u64,
	pub(crate) accepting: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Selector {
	Window(String),
	Session(String),
	Source(PathBuf),
}

#[cfg(unix)]
pub(crate) fn runtime_directory() -> io::Result<PathBuf> {
	if let Some(root) = std::env::var_os("XDG_RUNTIME_DIR").filter(|s| !s.is_empty()) {
		return Ok(PathBuf::from(root).join("polariton"));
	}
	if let Some(root) = std::env::var_os("XDG_STATE_HOME").filter(|s| !s.is_empty()) {
		return Ok(PathBuf::from(root).join("polariton/run"));
	}
	let home = std::env::var_os("HOME")
		.filter(|s| !s.is_empty())
		.map(PathBuf::from)
		.ok_or_else(|| io::Error::other("no user runtime or state directory"))?;
	// macOS caps socket paths at 104 bytes; under `Library/Application Support`
	// a long user name leaves no room for `<window>.sock`.
	Ok(if cfg!(target_os = "macos") {
		home.join(".polariton/run")
	} else {
		home.join(".local/state/polariton/run")
	})
}

#[cfg(windows)]
pub(crate) fn runtime_directory() -> io::Result<PathBuf> {
	let root = std::env::var_os("LOCALAPPDATA")
		.filter(|s| !s.is_empty())
		.map(PathBuf::from)
		.ok_or_else(|| io::Error::other("no LOCALAPPDATA directory"))?;
	Ok(root.join("polariton").join("run"))
}

/// A fresh runtime root for tests. macOS caps socket paths at 104 bytes and
/// its per-user temp directory alone takes about half, so tests use `/tmp`.
#[cfg(test)]
pub(crate) fn test_runtime_root(label: &str) -> PathBuf {
	let base = if cfg!(target_os = "macos") {
		PathBuf::from("/tmp")
	} else {
		std::env::temp_dir()
	};
	base.join(format!("p16-{label}-{}", &mint_id()[..12]))
}

/// The transport endpoint a window with this id advertises under `root`.
pub(crate) fn endpoint_for(root: &Path, window: &str) -> PathBuf {
	#[cfg(unix)]
	{
		root.join(format!("{window}.sock"))
	}
	#[cfg(windows)]
	{
		let _ = root;
		PathBuf::from(format!(r"\\.\pipe\polariton-{window}"))
	}
}

pub(crate) fn secure_directory(root: &Path) -> io::Result<()> {
	key_guard::ensure_private_directory(root).map_err(io::Error::other)
}

/// A manifest file is trusted only when it is the running user's own and
/// nobody else can read it: the runtime directory's contract, per file.
fn owned_manifest(path: &Path, metadata: &fs::Metadata) -> bool {
	#[cfg(unix)]
	{
		let _ = path;
		// SAFETY: geteuid has no preconditions and cannot fail.
		metadata.uid() == unsafe { libc::geteuid() }
	}
	#[cfg(windows)]
	{
		let _ = metadata;
		key_guard::open_private_key_owner_only(path).is_ok()
	}
}

pub(crate) fn process_start(pid: u32) -> Option<String> {
	crate::process_identity::process_start(pid)
}

fn mint_id() -> String {
	uuid::Uuid::new_v4().simple().to_string()
}

pub(crate) struct Window {
	pub(crate) advertisement: Advertisement,
	root: PathBuf,
}

impl Window {
	pub(crate) fn create(
		root: PathBuf,
		session: Option<String>,
		sources: Vec<PathBuf>,
	) -> io::Result<Self> {
		secure_directory(&root)?;
		let window = mint_id();
		let pid = std::process::id();
		let advertisement = Advertisement {
			socket: endpoint_for(&root, &window),
			window,
			pid,
			process_start: process_start(pid)
				.ok_or_else(|| io::Error::other("cannot read own process start token"))?,
			session,
			sources,
			accepting: false,
			started_at: std::time::SystemTime::now()
				.duration_since(std::time::UNIX_EPOCH)
				.unwrap_or_default()
				.as_secs(),
		};
		let owner = Self {
			advertisement,
			root,
		};
		owner.write()?;
		Ok(owner)
	}

	pub(crate) fn write(&self) -> io::Result<()> {
		let path = self
			.root
			.join(format!("{}.json", self.advertisement.window));
		let temporary = path.with_extension("tmp");
		let mut options = OpenOptions::new();
		options.write(true).create_new(true);
		#[cfg(unix)]
		options.mode(0o600);
		let mut file = options.open(&temporary)?;
		let result = (|| {
			file.write_all(&super::encode(&self.advertisement).map_err(io::Error::other)?)?;
			fs::rename(&temporary, &path)
		})();
		if result.is_err() {
			let _ = fs::remove_file(temporary);
		}
		result
	}
}

impl Drop for Window {
	fn drop(&mut self) {
		let _ = fs::remove_file(
			self.root
				.join(format!("{}.json", self.advertisement.window)),
		);
		#[cfg(unix)]
		let _ = fs::remove_file(&self.advertisement.socket);
	}
}

pub(crate) fn discover(root: &Path) -> io::Result<Vec<Advertisement>> {
	if !root.exists() {
		return Ok(Vec::new());
	}
	secure_directory(root)?;
	let mut windows = Vec::new();
	for entry in fs::read_dir(root)? {
		let path = entry?.path();
		if path.extension().is_none_or(|extension| extension != "json") {
			continue;
		}
		// Ignore malformed and foreign entries; never follow a manifest's
		// arbitrary path when pruning its endpoint.
		let metadata = fs::symlink_metadata(&path)?;
		if !metadata.is_file()
			|| !owned_manifest(&path, &metadata)
			|| metadata.len() > super::MAX_LINE as u64
		{
			continue;
		}
		let Ok(window) = super::decode::<Advertisement>(&fs::read(&path)?) else {
			continue;
		};
		if window.window.len() != 32
			|| !window.window.bytes().all(|b| b.is_ascii_hexdigit())
			|| path.file_stem().and_then(|s| s.to_str()) != Some(&window.window)
			|| window.socket != endpoint_for(root, &window.window)
		{
			continue;
		}
		if process_start(window.pid).as_deref() == Some(&window.process_start) {
			windows.push(window);
		} else {
			let _ = fs::remove_file(&path);
			#[cfg(unix)]
			let _ = fs::remove_file(endpoint_for(root, &window.window));
		}
	}
	windows.sort_by(|a, b| a.window.cmp(&b.window));
	Ok(windows)
}

pub(crate) fn select(
	windows: &[Advertisement],
	selector: &Selector,
) -> Result<Advertisement, String> {
	let candidates: Vec<_> = windows
		.iter()
		.filter(|window| match selector {
			Selector::Window(id) => window.window == *id,
			Selector::Session(id) => window.session.as_ref() == Some(id),
			Selector::Source(path) => window
				.sources
				.iter()
				.any(|source| normalize(source) == normalize(path)),
		})
		.collect();
	match candidates.as_slice() {
		[window] => Ok((*window).clone()),
		_ => Err(format!(
			"{}: {}",
			if candidates.is_empty() {
				"no matching window; live windows"
			} else {
				"ambiguous window; candidates"
			},
			String::from_utf8_lossy(
				&super::encode(&if candidates.is_empty() {
					windows.iter().collect::<Vec<_>>()
				} else {
					candidates
				})
				.unwrap_or_default()
			)
			.trim()
		)),
	}
}

pub(crate) fn normalize(path: &Path) -> PathBuf {
	libaryon::core::canonicalize(path).unwrap_or_else(|_| {
		if path.is_absolute() {
			path.to_owned()
		} else {
			std::env::current_dir().unwrap_or_default().join(path)
		}
	})
}

#[cfg(test)]
mod tests {
	use super::*;
	#[test]
	fn product_0160_discovery_is_unique_and_prunes_reused_pids() {
		let root = test_runtime_root("discovery");
		let a = Window::create(
			root.clone(),
			None,
			vec![normalize(Path::new("showcase.rn"))],
		)
		.unwrap();
		let mut b = Window::create(root.clone(), Some("session".into()), vec![]).unwrap();
		let found = discover(&root).unwrap();
		assert_eq!(found.len(), 2);
		// Two windows share the same source: selecting by it is ambiguous and
		// the refusal names both candidates.
		let shared = Window::create(
			root.clone(),
			None,
			vec![normalize(Path::new("showcase.rn"))],
		)
		.unwrap();
		let three = discover(&root).unwrap();
		let error = select(&three, &Selector::Source(PathBuf::from("showcase.rn"))).unwrap_err();
		assert!(
			error.contains(&a.advertisement.window) && error.contains(&shared.advertisement.window)
		);
		drop(shared);
		#[cfg(unix)]
		{
			use std::os::unix::fs::PermissionsExt;
			assert_eq!(
				fs::metadata(&root).unwrap().permissions().mode() & 0o777,
				0o700
			);
		}
		// The endpoint is bound only when agents are accepted, never at creation.
		#[cfg(unix)]
		assert!(!a.advertisement.socket.exists());
		assert!(
			select(&found, &Selector::Source(PathBuf::from("showcase.rn")))
				.unwrap()
				.window == a.advertisement.window
		);
		assert_eq!(
			select(&found, &Selector::Session("session".into()))
				.unwrap()
				.window,
			b.advertisement.window
		);
		// A manifest whose start token no longer matches its pid is pruned:
		// the pid was recycled or the process is gone.
		b.advertisement.process_start = "not-this-incarnation".into();
		let _ = fs::remove_file(root.join(format!("{}.json", b.advertisement.window)));
		b.write().unwrap();
		let found = discover(&root).unwrap();
		assert_eq!(found.len(), 1);
		assert_eq!(found[0].window, a.advertisement.window);
		assert!(select(&found, &Selector::Session("session".into())).is_err());
		drop(a);
		drop(b);
		let _ = fs::remove_dir_all(&root);
	}
}
