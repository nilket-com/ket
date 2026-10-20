//! Owned scratch directories for tests, under `TMPDIR`. Never adopts or removes an existing path.
use std::fs;
use std::ops::Deref;
use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT: AtomicU64 = AtomicU64::new(0);

#[derive(Debug)]
pub(crate) struct TestDir(PathBuf);

impl TestDir {
	pub(crate) fn new(prefix: &str) -> Self {
		assert!(!prefix.is_empty() && !prefix.contains('/'));
		let nanos = std::time::SystemTime::now()
			.duration_since(std::time::UNIX_EPOCH)
			.map_or(0, |d| d.as_nanos());
		for _ in 0..8 {
			let path = std::env::temp_dir().join(format!(
				"{prefix}-{}-{nanos}-{}",
				std::process::id(),
				NEXT.fetch_add(1, Ordering::Relaxed)
			));
			match fs::DirBuilder::new().mode(0o700).create(&path) {
				Ok(()) => return Self(path),
				Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
				Err(error) => panic!("{}: {error}", path.display()),
			}
		}
		panic!("no fresh scratch directory for {prefix} in 8 attempts")
	}
}

impl Deref for TestDir {
	type Target = Path;
	fn deref(&self) -> &Path {
		&self.0
	}
}

impl Drop for TestDir {
	fn drop(&mut self) {
		// A test may leave a directory without write or search permission; restore it first.
		let _ = fs::set_permissions(&self.0, fs::Permissions::from_mode(0o700));
		let _ = fs::remove_dir_all(&self.0);
	}
}
