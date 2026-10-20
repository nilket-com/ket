//! Test-only helpers shared by the unit tests.
mod test_dir;
pub(crate) use test_dir::TestDir;

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
