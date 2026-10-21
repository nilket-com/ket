//! Bounded subprocess receipts for exact-test reexecution. Regular-file capture
//! avoids pipe backpressure; the guard always kills and reaps a live child.
use super::TestDir;
use std::fs::File;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Output};
use std::time::{Duration, Instant};

struct ChildGuard {
	child: Option<Child>,
	_logs: TestDir,
	stdout: PathBuf,
	stderr: PathBuf,
}
impl ChildGuard {
	fn spawn(command: &mut Command) -> io::Result<Self> {
		let logs = TestDir::new("wendelon-child-output");
		let stdout = logs.join("stdout");
		let stderr = logs.join("stderr");
		let child = command
			.stdout(File::create(&stdout)?)
			.stderr(File::create(&stderr)?)
			.spawn()?;
		Ok(Self {
			child: Some(child),
			_logs: logs,
			stdout,
			stderr,
		})
	}
	fn terminate_and_reap(&mut self) -> io::Result<()> {
		if let Some(child) = self.child.as_mut() {
			if !matches!(child.try_wait(), Ok(Some(_))) {
				// Exit can race kill; wait still reaps either normal or killed exit.
				let _ = child.kill();
				loop {
					match child.wait() {
						Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
						result => {
							result?;
							break;
						}
					}
				}
			}
			self.child.take();
		}
		Ok(())
	}
}
impl Drop for ChildGuard {
	fn drop(&mut self) {
		// Unlike an unjoined blocking output waiter, dropping the async caller
		// cannot leave a child using its data directory after test cleanup.
		let _ = self.terminate_and_reap();
	}
}

async fn wait_until(guard: &mut ChildGuard, deadline: Instant) -> io::Result<ExitStatus> {
	loop {
		if Instant::now() >= deadline {
			return Err(io::Error::new(
				io::ErrorKind::TimedOut,
				"exact-test child exceeded its overall hang deadline",
			));
		}
		if let Some(status) = guard
			.child
			.as_mut()
			.expect("child has not been reaped")
			.try_wait()?
		{
			return Ok(status);
		}
		// Scheduling poll only; producer correctness is established by its own
		// explicit staging gate, never by this process-exit observation interval.
		tokio::time::sleep(Duration::from_millis(10)).await;
	}
}
fn read_diagnostics(path: &Path) -> io::Result<Vec<u8>> {
	// A broken child may print continuously. Bound in-memory diagnostics too.
	let mut bytes = Vec::new();
	File::open(path)?
		.take(4 * 1024 * 1024)
		.read_to_end(&mut bytes)?;
	Ok(bytes)
}

pub(crate) async fn run_test_child(command: &mut Command, bound: Duration) -> io::Result<Output> {
	let deadline = Instant::now() + bound;
	let mut guard = ChildGuard::spawn(command)?;
	let status = match wait_until(&mut guard, deadline).await {
		Ok(status) => status,
		Err(error) => {
			guard.terminate_and_reap()?;
			return Err(io::Error::new(
				error.kind(),
				format!(
					"{error}\nstdout: {}\nstderr: {}",
					String::from_utf8_lossy(&read_diagnostics(&guard.stdout)?),
					String::from_utf8_lossy(&read_diagnostics(&guard.stderr)?)
				),
			));
		}
	};
	// try_wait already reaped this child; Drop must not signal a reused pid.
	guard.child.take();
	Ok(Output {
		status,
		stdout: read_diagnostics(&guard.stdout)?,
		stderr: read_diagnostics(&guard.stderr)?,
	})
}

#[tokio::test]
async fn guard_reaps_on_deadline_and_async_cancellation() {
	const CHILD: &str = "WENDELON_CHILD_GUARD_CONTROL";
	if std::env::var_os(CHILD).is_some() {
		loop {
			std::thread::park();
		}
	}
	let test = concat!(
		module_path!(),
		"::guard_reaps_on_deadline_and_async_cancellation"
	)
	.trim_start_matches("wendelon::");
	let command = move || {
		let mut command = Command::new(std::env::current_exe().unwrap());
		command
			.args([test, "--exact", "--nocapture"])
			.env(CHILD, "1");
		command
	};
	let mut guard = ChildGuard::spawn(&mut command()).unwrap();
	let pid = guard.child.as_ref().unwrap().id();
	assert_eq!(
		wait_until(&mut guard, Instant::now())
			.await
			.unwrap_err()
			.kind(),
		io::ErrorKind::TimedOut
	);
	guard.terminate_and_reap().unwrap();
	assert!(guard.child.is_none());
	assert_reaped(pid);
	let (sent, received) = tokio::sync::oneshot::channel();
	let task = tokio::spawn(async move {
		let mut guard = ChildGuard::spawn(&mut command()).unwrap();
		sent.send(guard.child.as_ref().unwrap().id()).unwrap();
		std::future::pending::<()>().await;
		guard.terminate_and_reap().unwrap();
	});
	let pid = received.await.unwrap();
	task.abort();
	assert!(task.await.unwrap_err().is_cancelled());
	assert_reaped(pid);
}
#[cfg(test)]
fn assert_reaped(pid: u32) {
	// SAFETY: waitpid with WNOHANG does not block, accepts a null status, and
	// this pid names the child just killed and reaped by this test's guard.
	assert_eq!(
		unsafe { libc::waitpid(pid as libc::pid_t, std::ptr::null_mut(), libc::WNOHANG) },
		-1
	);
	assert_eq!(
		io::Error::last_os_error().raw_os_error(),
		Some(libc::ECHILD)
	);
}
