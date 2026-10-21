//! Test-only helpers shared by the unit tests.
mod child;
pub(crate) use child::run_test_child;

mod test_dir;
pub(crate) use test_dir::TestDir;

// Synthetic measurement tooling; never compiled into the ordinary library.
pub(crate) mod g2;

/// Spawn a child that stays in the fork-to-exec window for `hold`, so it holds a
/// copy of every descriptor this process has open. Returns once the child is
/// inside that window; the join handle completes when the child has exited.
///
/// The child is a `clone(CLONE_VM | CLONE_VFORK)` without `CLONE_FILES`: it gets
/// its own copy of the descriptor table, exactly as `fork` gives, but shares the
/// address space instead of duplicating it. A real `fork` of the test process
/// must account a private copy of every writable mapping; with 1024 test threads
/// at a 64 MiB `RUST_MIN_STACK` that is tens of GiB, and heuristic overcommit
/// refuses it with ENOMEM. The descriptors are the only thing these tests need
/// the child to hold.
pub(crate) fn hold_child_in_fork_window(
	marker: std::path::PathBuf,
	hold: std::time::Duration,
) -> std::thread::JoinHandle<()> {
	let probe = marker.clone();
	let thread = std::thread::spawn(move || hold_descriptor_copy(&probe, hold));
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

struct ForkWindow {
	marker: std::ffi::CString,
	pause: libc::timespec,
}

// Runs in the vfork-style child on its own stack while the parent thread is
// suspended. Raw syscalls only, no glibc wrappers: no allocation, no locks, no
// unwinding, nothing that touches state the suspended parent owns beyond the
// `ForkWindow` it lent. `libc::syscall` may store errno on failure; the parent
// thread is suspended until the child exits and reads no errno across it.
extern "C" fn fork_window_child(arg: *mut libc::c_void) -> libc::c_int {
	// SAFETY: `arg` is the `&ForkWindow` the parent passed to clone; the parent
	// keeps it alive and unmoved because CLONE_VFORK suspends it until we exit.
	let window = unsafe { &*(arg as *const ForkWindow) };
	// SAFETY: openat with a NUL-terminated path owned by `window`.
	let fd = unsafe {
		libc::syscall(
			libc::SYS_openat,
			libc::AT_FDCWD,
			window.marker.as_ptr(),
			libc::O_CREAT | libc::O_WRONLY | libc::O_CLOEXEC,
			0o600,
		)
	};
	let code = if fd < 0 { 1 } else { 0 };
	if fd >= 0 {
		// SAFETY: closes only the descriptor this child just opened, then sleeps
		// on a timespec owned by `window`; neither reads or writes other memory.
		unsafe {
			libc::syscall(libc::SYS_close, fd);
			libc::syscall(
				libc::SYS_clock_nanosleep,
				libc::CLOCK_MONOTONIC,
				0,
				&window.pause as *const libc::timespec,
				std::ptr::null_mut::<libc::timespec>(),
			);
		}
	}
	// SAFETY: exit_group ends this child process only (it shares memory with
	// the parent but is not in its thread group); it never returns.
	unsafe { libc::syscall(libc::SYS_exit_group, code) };
	unreachable!("exit_group returned")
}

fn hold_descriptor_copy(marker: &std::path::Path, hold: std::time::Duration) {
	let window = ForkWindow {
		marker: std::ffi::CString::new(marker.as_os_str().as_encoded_bytes()).unwrap(),
		pause: libc::timespec {
			tv_sec: hold.as_secs() as libc::time_t,
			tv_nsec: hold.subsec_nanos() as libc::c_long,
		},
	};
	let mut stack = vec![0u8; 64 * 1024];
	// The stack grows down on every Linux target this crate supports.
	// SAFETY: one past the end of the allocation, which `add` permits; the child
	// only ever uses addresses below it, and `stack` outlives the child.
	let top = unsafe { stack.as_mut_ptr().add(stack.len()) };
	let top = (top as usize & !15) as *mut libc::c_void;
	// Block every signal so no handler runs in the child on the shared memory;
	// the child inherits this thread's mask.
	// SAFETY: sigset_t is a plain bit set; all-zero is a valid empty set.
	let mut all: libc::sigset_t = unsafe { std::mem::zeroed() };
	// SAFETY: as above; pthread_sigmask overwrites it with the previous mask.
	let mut previous: libc::sigset_t = unsafe { std::mem::zeroed() };
	// SAFETY: fills and installs a signal set this thread owns; the previous
	// mask is restored right after the clone below.
	unsafe {
		libc::sigfillset(&mut all);
		libc::pthread_sigmask(libc::SIG_SETMASK, &all, &mut previous);
	}
	// CLONE_VFORK: this thread resumes only once the child has exited.
	// SAFETY: the child runs `fork_window_child` on `stack`, which is 16-byte
	// aligned at `top` and stays allocated until after waitpid; `window` stays
	// borrowed for the child's whole life because this thread is suspended.
	// Without CLONE_FILES the child gets its own copy of the descriptor table.
	let pid = unsafe {
		libc::clone(
			fork_window_child,
			top,
			libc::CLONE_VM | libc::CLONE_VFORK | libc::SIGCHLD,
			&window as *const ForkWindow as *mut libc::c_void,
		)
	};
	let clone_error = std::io::Error::last_os_error();
	// SAFETY: restores the mask saved above on this same thread.
	unsafe { libc::pthread_sigmask(libc::SIG_SETMASK, &previous, std::ptr::null_mut()) };
	assert!(
		pid > 0,
		"clone of the fork-window child failed: {clone_error}"
	);
	let mut status = 0;
	// SAFETY: reaps the child this thread created, into a local status word.
	let reaped = unsafe { libc::waitpid(pid, &mut status, 0) };
	assert_eq!(reaped, pid, "{}", std::io::Error::last_os_error());
	assert!(
		libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0,
		"fork-window child failed with status {status:#x}"
	);
	drop(stack);
}
