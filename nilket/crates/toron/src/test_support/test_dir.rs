//! Test-only scratch ownership. Pending async teardown retains on every incomplete exit.
use std::fmt::Debug;
use std::future::Future;
use std::ops::Deref;
use std::os::unix::fs::DirBuilderExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

pub(crate) const TEST_TEARDOWN_BOUND: Duration = Duration::from_secs(60);

pub(crate) struct TestDir(Option<PathBuf>);

impl TestDir {
	pub(crate) fn new(prefix: &str) -> Self {
		assert!(!prefix.is_empty() && !prefix.contains('/') && !prefix.contains('\\'));
		for _ in 0..8 {
			let path = std::env::temp_dir().join(format!(
				"{prefix}-{}-{}",
				std::process::id(),
				uuid::Uuid::new_v4()
			));
			match std::fs::DirBuilder::new().mode(0o700).create(&path) {
				Ok(()) => return Self(Some(path)),
				Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
				Err(error) => panic!("{}: {error}", path.display()),
			}
		}
		panic!("no fresh scratch directory for {prefix} in 8 attempts")
	}

	pub(crate) fn retain(mut self) -> PathBuf {
		let path = self.0.take().expect("scratch root owned");
		eprintln!("retained {}", path.display());
		path
	}

	pub(crate) fn pending(self) -> PendingTeardown {
		PendingTeardown(Some(self))
	}
}

impl Deref for TestDir {
	type Target = Path;
	fn deref(&self) -> &Path {
		self.0.as_deref().expect("scratch root owned")
	}
}

impl AsRef<Path> for TestDir {
	fn as_ref(&self) -> &Path {
		self
	}
}

impl Drop for TestDir {
	fn drop(&mut self) {
		if let Some(path) = self.0.take() {
			if std::thread::panicking() {
				eprintln!("retained {}", path.display());
			} else {
				let _ = std::fs::remove_dir_all(path);
			}
		}
	}
}

pub(crate) struct PendingTeardown(Option<TestDir>);

impl PendingTeardown {
	pub(crate) async fn complete<E: Debug>(mut self, future: impl Future<Output = Result<(), E>>) {
		match tokio::time::timeout(TEST_TEARDOWN_BOUND, future).await {
			Ok(Ok(())) => drop(self.0.take()),
			other => panic!("teardown unproven; scratch retained: {other:?}"),
		}
	}
}

impl Drop for PendingTeardown {
	fn drop(&mut self) {
		if let Some(dir) = self.0.take() {
			dir.retain();
		}
	}
}
