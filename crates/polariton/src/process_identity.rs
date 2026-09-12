//! A process-start token: with a pid, it names one process incarnation, so
//! a recycled pid never impersonates a window or a reporting writer that is
//! gone. Linux reads the start tick from `/proc`; Windows reads the creation
//! time from the process object, which stays openable after exit while any
//! handle survives, so liveness is checked too; elsewhere `ps` answers.

/// The token for `pid`, or `None` when the process is not running.
pub(crate) fn process_start(pid: u32) -> Option<String> {
	platform::process_start(pid)
}

#[cfg(target_os = "linux")]
mod platform {
	pub(super) fn process_start(pid: u32) -> Option<String> {
		let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
		// comm may itself contain spaces and parentheses; fields after its last
		// closing parenthesis begin with field 3. Start time is field 22.
		stat.rsplit_once(')')?
			.1
			.split_whitespace()
			.nth(19)
			.map(str::to_owned)
	}
}

#[cfg(windows)]
mod platform {
	use std::ffi::c_void;

	#[repr(C)]
	#[derive(Clone, Copy, Default)]
	struct FileTime {
		low: u32,
		high: u32,
	}
	unsafe extern "system" {
		fn OpenProcess(desired_access: u32, inherit_handle: i32, pid: u32) -> *mut c_void;
		fn GetProcessTimes(
			process: *mut c_void,
			creation: *mut FileTime,
			exit: *mut FileTime,
			kernel: *mut FileTime,
			user: *mut FileTime,
		) -> i32;
		fn GetExitCodeProcess(process: *mut c_void, exit_code: *mut u32) -> i32;
		fn CloseHandle(handle: *mut c_void) -> i32;
	}
	const PROCESS_QUERY_LIMITED_INFORMATION: u32 = 0x1000;
	const STILL_ACTIVE: u32 = 259;

	pub(super) fn process_start(pid: u32) -> Option<String> {
		// SAFETY: querying a process object needs no memory preconditions.
		let process = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
		if process.is_null() {
			return None;
		}
		let mut exit_code = 0_u32;
		// SAFETY: process is a valid handle from OpenProcess; out-pointer is valid.
		let running = unsafe { GetExitCodeProcess(process, &mut exit_code) } != 0
			&& exit_code == STILL_ACTIVE;
		let mut times = [FileTime::default(); 4];
		let [creation, exit, kernel, user] = &mut times;
		// SAFETY: as above; the four out-structs are valid for writes.
		let ok = running && unsafe { GetProcessTimes(process, creation, exit, kernel, user) } != 0;
		// SAFETY: closing the handle we opened.
		unsafe { CloseHandle(process) };
		ok.then(|| {
			format!(
				"{:016x}",
				(u64::from(creation.high) << 32) | u64::from(creation.low)
			)
		})
	}
}

#[cfg(not(any(target_os = "linux", windows)))]
mod platform {
	pub(super) fn process_start(pid: u32) -> Option<String> {
		let output = std::process::Command::new("ps")
			.args(["-o", "lstart=", "-p", &pid.to_string()])
			.output()
			.ok()?;
		let token = String::from_utf8(output.stdout).ok()?.trim().to_owned();
		(output.status.success() && !token.is_empty()).then_some(token)
	}
}

#[cfg(test)]
mod tests {
	use super::process_start;

	#[test]
	fn the_running_process_has_a_stable_token_and_a_dead_pid_has_none() {
		let own = std::process::id();
		let first = process_start(own).expect("own token");
		assert_eq!(process_start(own).as_deref(), Some(first.as_str()));
		let mut child = std::process::Command::new(std::env::current_exe().unwrap())
			.arg("--version")
			.stdout(std::process::Stdio::null())
			.stderr(std::process::Stdio::null())
			.spawn()
			.expect("spawn a short-lived child");
		let pid = child.id();
		child.wait().expect("child exits");
		// The Child handle is still held here, so the process object may
		// still be openable; a token must not be reported for it anyway.
		assert_eq!(process_start(pid), None, "a finished process has no token");
	}
}
