//! Owned scratch directories for unit tests. Never adopts or removes an existing path.
use std::fs;
use std::ops::Deref;
use std::os::unix::fs::DirBuilderExt;
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub(crate) struct TestDir(PathBuf);

impl TestDir {
	pub(crate) fn new(prefix: &str) -> Self {
		assert!(!prefix.is_empty() && !prefix.contains('/') && !prefix.contains('\\'));
		for _ in 0..8 {
			let path = std::env::temp_dir().join(format!(
				"{prefix}-{}-{}",
				std::process::id(),
				uuid::Uuid::new_v4()
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
impl AsRef<Path> for TestDir {
	fn as_ref(&self) -> &Path {
		&self.0
	}
}
impl Drop for TestDir {
	fn drop(&mut self) {
		let _ = fs::remove_dir_all(&self.0);
	}
}
