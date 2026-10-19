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

// Synthetic measurement tooling; never compiled into the ordinary library.
pub(crate) mod g2;

/// Spawn a child that stays between fork and exec for `hold`, so it holds a
/// copy of every descriptor this process has open. Returns once the child is
/// inside that window; the join handle completes when the child has exec'd.
pub(crate) fn hold_child_in_fork_window(
	marker: std::path::PathBuf,
	hold: std::time::Duration,
) -> std::thread::JoinHandle<()> {
	use std::os::unix::process::CommandExt;
	let probe = marker.clone();
	let thread = std::thread::spawn(move || {
		let path = std::ffi::CString::new(probe.as_os_str().as_encoded_bytes()).unwrap();
		let pause = libc::timespec {
			tv_sec: hold.as_secs() as libc::time_t,
			tv_nsec: hold.subsec_nanos() as libc::c_long,
		};
		let mut command = std::process::Command::new("true");
		unsafe {
			command.pre_exec(move || {
				// Async-signal-safe only: open, close, nanosleep.
				let fd = libc::open(path.as_ptr(), libc::O_CREAT | libc::O_WRONLY, 0o600);
				if fd < 0 {
					return Err(std::io::Error::last_os_error());
				}
				libc::close(fd);
				libc::nanosleep(&pause, std::ptr::null_mut());
				Ok(())
			});
		}
		command.status().unwrap();
	});
	// Bounded: a child that never reaches the marker fails the test, not hangs it.
	let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
	while !marker.exists() {
		assert!(
			!thread.is_finished(),
			"child exited before reaching its fork window"
		);
		assert!(
			std::time::Instant::now() < deadline,
			"child never reached its fork window"
		);
		std::thread::yield_now();
	}
	thread
}
