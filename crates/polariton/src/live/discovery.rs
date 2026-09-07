//! Per-window advertisements. A PID alone is never evidence of liveness.

use serde::{Deserialize, Serialize};
use std::fs::{self, DirBuilder, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct Advertisement {
	pub(crate) window: String,
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

pub(crate) fn runtime_directory() -> io::Result<PathBuf> {
	if let Some(root) = std::env::var_os("XDG_RUNTIME_DIR").filter(|s| !s.is_empty()) {
		return Ok(PathBuf::from(root).join("polariton"));
	}
	let root = std::env::var_os("XDG_STATE_HOME")
		.filter(|s| !s.is_empty())
		.map(PathBuf::from)
		.or_else(|| {
			std::env::var_os("HOME").map(|home| {
				if cfg!(target_os = "macos") {
					PathBuf::from(home).join("Library/Application Support")
				} else {
					PathBuf::from(home).join(".local/state")
				}
			})
		})
		.ok_or_else(|| io::Error::other("no user runtime or state directory"))?;
	Ok(root.join("polariton/run"))
}

pub(crate) fn uid() -> u32 {
	// SAFETY: geteuid has no arguments or memory preconditions.
	unsafe { libc::geteuid() }
}

pub(crate) fn secure_directory(root: &Path) -> io::Result<()> {
	DirBuilder::new().recursive(true).mode(0o700).create(root)?;
	let metadata = fs::symlink_metadata(root)?;
	if !metadata.is_dir() || metadata.uid() != uid() {
		return Err(io::Error::other(
			"agent runtime directory must be owned by this user and not a symlink",
		));
	}
	fs::set_permissions(root, fs::Permissions::from_mode(0o700))
}

#[cfg(target_os = "linux")]
pub(crate) fn process_start(pid: u32) -> Option<String> {
	let stat = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
	// comm may itself contain spaces and parentheses; fields after its last
	// closing parenthesis begin with field 3. Start time is field 22.
	stat.rsplit_once(')')?
		.1
		.split_whitespace()
		.nth(19)
		.map(str::to_owned)
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn process_start(pid: u32) -> Option<String> {
	let output = std::process::Command::new("ps")
		.args(["-o", "lstart=", "-p", &pid.to_string()])
		.output()
		.ok()?;
	let token = String::from_utf8(output.stdout).ok()?.trim().to_owned();
	(output.status.success() && !token.is_empty()).then_some(token)
}

fn mint_id() -> io::Result<String> {
	let mut bytes = [0_u8; 16];
	fs::File::open("/dev/urandom")?.read_exact(&mut bytes)?;
	Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
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
		let window = mint_id()?;
		let pid = std::process::id();
		let advertisement = Advertisement {
			socket: root.join(format!("{window}.sock")),
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
		let mut file = OpenOptions::new()
			.write(true)
			.create_new(true)
			.mode(0o600)
			.open(&temporary)?;
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
		// arbitrary path when pruning its socket.
		let metadata = fs::symlink_metadata(&path)?;
		if !metadata.is_file() || metadata.uid() != uid() || metadata.len() > super::MAX_LINE as u64
		{
			continue;
		}
		let Ok(window) = super::decode::<Advertisement>(&fs::read(&path)?) else {
			continue;
		};
		if window.window.len() != 32
			|| !window.window.bytes().all(|b| b.is_ascii_hexdigit())
			|| path.file_stem().and_then(|s| s.to_str()) != Some(&window.window)
			|| window.socket != root.join(format!("{}.sock", window.window))
		{
			continue;
		}
		if process_start(window.pid).as_deref() == Some(&window.process_start) {
			windows.push(window);
		} else {
			let _ = fs::remove_file(&path);
			let _ = fs::remove_file(root.join(format!("{}.sock", window.window)));
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
	fs::canonicalize(path).unwrap_or_else(|_| {
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
		let root = std::env::temp_dir().join(format!("p16-{}", mint_id().unwrap()));
		let a = Window::create(
			root.clone(),
			None,
			vec![normalize(Path::new("showcase.rn"))],
		)
		.unwrap();
		let mut b = Window::create(
			root.clone(),
			Some("session".into()),
			a.advertisement.sources.clone(),
		)
		.unwrap();
		let windows = discover(&root).unwrap();
		assert_eq!(windows.len(), 2);
		let error = select(&windows, &Selector::Source("showcase.rn".into())).unwrap_err();
		assert!(error.contains(&a.advertisement.window) && error.contains(&b.advertisement.window));
		assert_eq!(
			select(&windows, &Selector::Window(a.advertisement.window.clone())).unwrap(),
			a.advertisement
		);
		assert_eq!(
			select(&windows, &Selector::Session("session".into())).unwrap(),
			b.advertisement
		);
		assert_eq!(fs::metadata(&root).unwrap().mode() & 0o777, 0o700);
		assert!(!a.advertisement.socket.exists());
		b.advertisement.process_start.push_str("-different-process");
		b.write().unwrap();
		assert_eq!(discover(&root).unwrap(), vec![a.advertisement.clone()]);
		assert!(
			!root
				.join(format!("{}.json", b.advertisement.window))
				.exists()
		);
		drop((a, b));
		fs::remove_dir_all(root).unwrap();
	}
}
