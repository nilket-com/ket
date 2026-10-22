//! Process-level shutdown deadline for the Wendelon daemon.
//!
//! A shutdown that cannot finish must not leave a process holding the data
//! directory lock. The watchdog terminates the process without running
//! destructors once the deadline passes; process death releases the lock
//! safely because every writer dies with it, and the next start finds no
//! clean-shutdown record and boots in recovery mode.

/// The whole daemon shutdown may take the node's quiescence bound plus its
/// storage-close bound, plus a margin for Raft, task joins, listeners and the
/// bounded runtime teardown.
pub fn daemon_shutdown_deadline(
	storage_shutdown_timeout: std::time::Duration,
) -> std::time::Duration {
	storage_shutdown_timeout * 2 + std::time::Duration::from_secs(25)
}

/// Terminate immediately without running destructors (they may write, e.g. a
/// redb database flushes on drop). Process death releases the data-directory
/// lock safely because every writer dies with the process.
pub fn terminate_unclean(reason: &str) -> ! {
	let line = format!("wendelon: terminating after incomplete shutdown: {reason}\n");
	// Best effort, non-blocking enough for a diagnostic; never retried.
	unsafe {
		// Write the diagnostic only if stderr can take it now: a full pipe or a
		// stalled log consumer must not stop the process from exiting.
		let mut pollfd = libc::pollfd {
			fd: 2,
			events: libc::POLLOUT,
			revents: 0,
		};
		if libc::poll(&mut pollfd, 1, 100) == 1 && pollfd.revents & libc::POLLOUT != 0 {
			libc::write(2, line.as_ptr().cast(), line.len());
		}
		libc::_exit(crate::storage::ownership::UNCLEAN_SHUTDOWN_EXIT_CODE)
	}
}

/// An OS-thread deadline over the whole shutdown, independent of the async
/// runtime so a wedged runtime cannot hold it off.
pub struct ShutdownWatchdog {
	done: std::sync::Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>,
	thread: Option<std::thread::JoinHandle<()>>,
}

impl ShutdownWatchdog {
	pub fn arm(deadline: std::time::Duration) -> Self {
		Self::arm_with_owner(deadline, None)
	}
	/// Startup cancellation/unwind must also be bounded. A later successful
	/// explicit close suppresses this timer without reading a possibly wedged
	/// mutex: only actual LOCK release sets the owner signal.
	pub(crate) fn arm_for_owner(
		deadline: std::time::Duration,
		closed: std::sync::Arc<std::sync::atomic::AtomicBool>,
	) -> Self {
		Self::arm_with_owner(deadline, Some(closed))
	}
	fn arm_with_owner(
		deadline: std::time::Duration,
		closed: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
	) -> Self {
		let done = std::sync::Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
		let state = done.clone();
		let thread = std::thread::Builder::new()
			.name("wendelon-shutdown-watchdog".to_string())
			.spawn(move || {
				let (lock, signal) = &*state;
				let finished = lock.lock().unwrap_or_else(|poison| poison.into_inner());
				let (finished, timeout) = signal
					.wait_timeout_while(finished, deadline, |finished| !*finished)
					.unwrap_or_else(|poison| poison.into_inner());
				if timeout.timed_out()
					&& !*finished && !closed
					.as_ref()
					.is_some_and(|closed| closed.load(std::sync::atomic::Ordering::Acquire))
				{
					terminate_unclean(&format!("shutdown exceeded {deadline:?}"));
				}
			})
			.expect("spawn shutdown watchdog");
		Self {
			done,
			thread: Some(thread),
		}
	}

	pub fn disarm(mut self) {
		let (lock, signal) = &*self.done;
		*lock.lock().unwrap_or_else(|poison| poison.into_inner()) = true;
		signal.notify_all();
		if let Some(thread) = self.thread.take() {
			let _ = thread.join();
		}
	}
}
