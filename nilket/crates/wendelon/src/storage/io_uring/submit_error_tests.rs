//! Real pending socket I/O at the enter-error boundary; no process-global signal handlers.
use super::*;
use crate::storage::finality_observation::{
	FinalityObservationRegistry, Unavailable, WorkKind, WorkScope,
};
use std::io::Write;
use std::os::fd::OwnedFd;
use std::os::unix::net::UnixStream;
use std::sync::atomic::AtomicUsize;
use std::time::{Duration, Instant};

mod callback_panic;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interrupted_enter_keeps_the_read_until_its_real_completion() {
	read_across_enter_error(libc::EINTR).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resource_pressure_enter_errors_keep_pending_reads() {
	for errno in [libc::EAGAIN, libc::EBUSY] {
		read_across_enter_error(errno).await;
	}
}

async fn read_across_enter_error(errno: i32) {
	let handle = spawn_io_thread(8).unwrap();
	let (reader, mut writer) = UnixStream::pair().unwrap();
	let file = Arc::new(std::fs::File::from(OwnedFd::from(reader)));
	let (submitted, ready) = std::sync::mpsc::sync_channel(1);
	let (release, released) = std::sync::mpsc::sync_channel(1);
	*handle.submit_fault.lock().unwrap() = Some(SubmitFault {
		repeats: 0,
		observed: None,
		entered: false,
		errno,
		submitted,
		release: released,
	});
	let reply = handle
		.submit(IoOp::Read, &file, AlignedBuf::new(DIRECT_IO_ALIGN), 0)
		.await
		.unwrap();
	drop(file);
	tokio::task::spawn_blocking(move || ready.recv_timeout(Duration::from_secs(10)).unwrap())
		.await
		.unwrap();
	// The kernel has the read, but its peer has sent nothing. An enter error
	// must not turn that outstanding read into an error completion with a live buffer.
	release.send(()).unwrap();
	writer.write_all(&[0x5a; 32]).unwrap();
	let completion = tokio::time::timeout(Duration::from_secs(10), reply)
		.await
		.unwrap()
		.unwrap();
	if completion.result < 0 {
		// Keep a broken implementation's returned allocation alive during the
		// red run: the very defect under test is unproved kernel cessation.
		let result = completion.result;
		std::mem::forget(completion);
		panic!("enter error escaped instead of waiting for the CQE: {result}");
	}
	assert_eq!(completion.result, 32);
	assert_eq!(&completion.buf.unwrap().as_slice()[..32], &[0x5a; 32]);
	handle.close_and_join().await.unwrap();
}

/// Quarantine deliberately retains resources until process exit. Isolate these
/// tests in child processes so proving retention does not leak fds/locks into
/// other tests. This does not serialize them or use process-global signal hooks.
fn quarantine_child(mode: &str, test: &str) -> bool {
	if std::env::var("WENDELON_URING_QUARANTINE_CHILD").as_deref() == Ok(mode) {
		return true;
	}
	let output = std::process::Command::new(std::env::current_exe().unwrap())
		.args([test, "--exact", "--nocapture"])
		.env("WENDELON_URING_QUARANTINE_CHILD", mode)
		.output()
		.unwrap();
	assert!(
		output.status.success(),
		"child failed: {}\n{}",
		String::from_utf8_lossy(&output.stdout),
		String::from_utf8_lossy(&output.stderr)
	);
	assert!(String::from_utf8_lossy(&output.stdout).contains("quarantine control passed"));
	false
}

struct PendingRead {
	ring: IoUring,
	pending: InFlightRequests,
	reply: oneshot::Receiver<IoCompletion>,
	_writer: UnixStream,
	file: std::sync::Weak<std::fs::File>,
	buffer_dropped: Arc<AtomicBool>,
}

fn pending_read(
	owner: &DataDirectoryOwner,
	registry: &Arc<FinalityObservationRegistry>,
) -> PendingRead {
	let (reader, writer) = UnixStream::pair().unwrap();
	let file = Arc::new(std::fs::File::from(OwnedFd::from(reader)));
	let weak = Arc::downgrade(&file);
	let buffer_dropped = Arc::new(AtomicBool::new(false));
	let mut buf = AlignedBuf::new(DIRECT_IO_ALIGN);
	buf.drop_probe = Some(buffer_dropped.clone());
	let (reply, receiver) = oneshot::channel();
	let request = QueuedIoRequest {
		request: IoRequest {
			op: IoOp::Read,
			fd: file.as_raw_fd(),
			buf: Some(buf),
			offset: 0,
			reply,
		},
		file,
		observation: Some(registry.begin(WorkScope::Node, WorkKind::StoragePublication)),
		owner: Some(owner.clone()),
		// Also exercise the write-permit lifetime, independently of this read.
		permit: Some(owner.write_permit().unwrap()),
	};
	let mut ring = IoUring::new(8).unwrap();
	let mut pending = InFlightRequests::default();
	push_request(&mut ring, &mut 1, &mut pending, request);
	assert_eq!(ring.submit().unwrap(), 1);
	assert_eq!(ring.completion().len(), 0);
	PendingRead {
		ring,
		pending,
		reply: receiver,
		_writer: writer,
		file: weak,
		buffer_dropped,
	}
}

fn sync_cancel_supported() -> bool {
	let ring = IoUring::new(2).unwrap();
	match ring.submitter().register_sync_cancel(
		Some(types::Timespec::from(Duration::from_secs(1))),
		types::CancelBuilder::any(),
	) {
		Ok(()) => true,
		Err(error) if error.raw_os_error() == Some(libc::ENOENT) => true,
		Err(error)
			if matches!(
				error.raw_os_error(),
				Some(libc::EINVAL | libc::EOPNOTSUPP | libc::ENOSYS)
			) =>
		{
			false
		}
		Err(error) => panic!("unexpected sync-cancel support probe error: {error}"),
	}
}

#[tokio::test]
async fn fatal_enter_cancels_before_returning_buffers_and_releasing_owner() {
	if !quarantine_child(
		"cancel-real",
		concat!(
			module_path!(),
			"::fatal_enter_cancels_before_returning_buffers_and_releasing_owner"
		)
		.trim_start_matches("wendelon::"),
	) {
		return;
	}
	let supports_cancel = sync_cancel_supported();
	let dir = crate::test_support::TestDir::new("uring-fatal-cancel");
	let owner = DataDirectoryOwner::acquire_existing(&dir).unwrap();
	let registry = FinalityObservationRegistry::new();
	let PendingRead {
		ring,
		pending,
		reply,
		_writer,
		file: weak,
		buffer_dropped,
	} = pending_read(&owner, &registry);
	fail_ring(
		ring,
		pending,
		IoUringClass::Generic,
		&std::io::Error::from_raw_os_error(libc::EIO),
		|ring| {
			// Prove that real kernel I/O, not only a synthetic map entry,
			// is cancelled before the helper can hand out its allocation.
			let result = cancel_outstanding(ring);
			assert!(weak.upgrade().is_some());
			result
		},
	);
	let completion = reply.await.unwrap();
	assert_eq!(completion.result, -libc::EIO);
	assert!(!buffer_dropped.load(Ordering::Acquire));
	assert!(
		!supports_cancel || completion.buf.is_some(),
		"supported sync cancellation did not return the completed read buffer"
	);
	if completion.buf.is_some() {
		drop(completion);
		assert!(buffer_dropped.load(Ordering::Acquire));
		assert!(weak.upgrade().is_none());
		owner.close(Duration::from_secs(2)).await.unwrap();
		DataDirectoryOwner::acquire_existing(&dir).unwrap();
	} else {
		// Kernels without synchronous cancellation must exercise retention,
		// not introduce a minimum-kernel requirement for the production fix.
		assert!(weak.upgrade().is_some());
		drop(owner);
		assert!(DataDirectoryOwner::acquire_existing(&dir).is_err());
	}
	assert_eq!(
		registry.test_state(),
		(0, 0, Some(Unavailable::UnreadableState))
	);
	println!("quarantine control passed");
}

#[test]
fn failed_cancellation_keeps_kernel_resources_and_lock() {
	if !quarantine_child(
		"cancel",
		concat!(
			module_path!(),
			"::failed_cancellation_keeps_kernel_resources_and_lock"
		)
		.trim_start_matches("wendelon::"),
	) {
		return;
	}
	quarantine_case(false);
}

#[test]
fn unwinding_worker_keeps_kernel_resources_and_lock() {
	if !quarantine_child(
		"unwind",
		concat!(
			module_path!(),
			"::unwinding_worker_keeps_kernel_resources_and_lock"
		)
		.trim_start_matches("wendelon::"),
	) {
		return;
	}
	quarantine_case(true);
}

#[test]
fn successful_cancel_without_a_cqe_does_not_release_resources() {
	if !quarantine_child(
		"cancel-no-cqe",
		concat!(
			module_path!(),
			"::successful_cancel_without_a_cqe_does_not_release_resources"
		)
		.trim_start_matches("wendelon::"),
	) {
		return;
	}
	quarantine_case(false);
}

fn quarantine_case(unwind: bool) {
	let dir = crate::test_support::TestDir::new("uring-quarantine");
	let owner = DataDirectoryOwner::acquire_existing(&dir).unwrap();
	let registry = FinalityObservationRegistry::new();
	let PendingRead {
		ring,
		pending,
		mut reply,
		_writer,
		file: weak,
		buffer_dropped,
	} = pending_read(&owner, &registry);
	drop(owner);
	if unwind {
		let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
			let _ring = ring;
			let _pending = pending;
			panic!("simulated worker unwind with kernel work pending");
		}));
		assert!(result.is_err());
	} else {
		fail_ring(
			ring,
			pending,
			IoUringClass::Generic,
			&std::io::Error::from_raw_os_error(libc::EIO),
			|_| {
				if std::env::var("WENDELON_URING_QUARANTINE_CHILD").as_deref()
					== Ok("cancel-no-cqe")
				{
					Ok(()) // No CQE: this must not authorize returning the buffer.
				} else {
					Err(std::io::Error::from_raw_os_error(libc::EOPNOTSUPP))
				}
			},
		);
	}
	let completion = reply.try_recv().unwrap();
	assert_eq!(completion.result, -libc::EIO);
	assert!(
		completion.buf.is_none(),
		"a live allocation must not escape"
	);
	assert!(
		!buffer_dropped.load(Ordering::Acquire),
		"a live allocation must not be freed"
	);
	assert!(
		weak.upgrade().is_some(),
		"file ownership must remain retained"
	);
	assert!(
		DataDirectoryOwner::acquire_existing(&dir).is_err(),
		"LOCK must remain held"
	);
	assert_eq!(
		registry.test_state(),
		(0, 0, Some(Unavailable::UnreadableState))
	);
	println!("quarantine control passed");
}

#[tokio::test]
async fn a_known_cqe_releases_resources_without_cancellation_support() {
	let dir = crate::test_support::TestDir::new("uring-known-cqe");
	let owner = DataDirectoryOwner::acquire_existing(&dir).unwrap();
	let registry = FinalityObservationRegistry::new();
	let PendingRead {
		mut ring,
		pending,
		reply,
		mut _writer,
		file,
		buffer_dropped,
	} = pending_read(&owner, &registry);
	_writer.write_all(&[0x6b; 32]).unwrap();
	assert_eq!(ring.submit_and_wait(1).unwrap(), 0);
	assert_eq!(ring.completion().len(), 1);
	fail_ring(
		ring,
		pending,
		IoUringClass::Generic,
		&std::io::Error::from_raw_os_error(libc::EIO),
		|_| Err(std::io::Error::from_raw_os_error(libc::EOPNOTSUPP)),
	);
	let completion = reply.await.unwrap();
	assert_eq!(completion.result, -libc::EIO);
	assert_eq!(
		&completion.buf.as_ref().unwrap().as_slice()[..32],
		&[0x6b; 32]
	);
	assert!(!buffer_dropped.load(Ordering::Acquire));
	assert!(file.upgrade().is_none());
	drop(completion);
	assert!(buffer_dropped.load(Ordering::Acquire));
	owner.close(Duration::from_secs(2)).await.unwrap();
	DataDirectoryOwner::acquire_existing(&dir).unwrap();
}

#[tokio::test(start_paused = true)]
async fn quarantined_write_permit_refuses_clean_shutdown() {
	if !quarantine_child(
		"close-quarantine",
		concat!(
			module_path!(),
			"::quarantined_write_permit_refuses_clean_shutdown"
		)
		.trim_start_matches("wendelon::"),
	) {
		return;
	}
	let dir = crate::test_support::TestDir::new("uring-close-quarantine");
	let owner = DataDirectoryOwner::acquire_existing(&dir).unwrap();
	let registry = FinalityObservationRegistry::new();
	let PendingRead {
		ring,
		pending,
		reply,
		_writer,
		file,
		buffer_dropped,
	} = pending_read(&owner, &registry);
	fail_ring(
		ring,
		pending,
		IoUringClass::Generic,
		&std::io::Error::from_raw_os_error(libc::EIO),
		|_| Err(std::io::Error::from_raw_os_error(libc::EOPNOTSUPP)),
	);
	assert!(reply.await.unwrap().buf.is_none());
	let error = owner.close(Duration::from_secs(1)).await.unwrap_err();
	assert_eq!(
		error
			.get_ref()
			.unwrap()
			.downcast_ref::<crate::storage::ownership::ShutdownIncomplete>()
			.unwrap()
			.outstanding,
		1
	);
	assert!(!dir.join("CLEAN_SHUTDOWN").exists());
	assert!(owner.write_permit().is_err());
	assert!(file.upgrade().is_some());
	assert!(!buffer_dropped.load(Ordering::Acquire));
	drop(owner);
	assert!(DataDirectoryOwner::acquire_existing(&dir).is_err());
	println!("quarantine control passed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn terminal_worker_error_closes_admission_and_drops_queued_requests() {
	// Isolate any conservative quarantine on kernels without sync cancellation.
	if !quarantine_child(
		"worker-terminal",
		concat!(
			module_path!(),
			"::terminal_worker_error_closes_admission_and_drops_queued_requests"
		)
		.trim_start_matches("wendelon::"),
	) {
		return;
	}
	let handle = spawn_io_thread(8).unwrap();
	let (reader, _writer) = UnixStream::pair().unwrap();
	let file = Arc::new(std::fs::File::from(OwnedFd::from(reader)));
	let (submitted, ready) = std::sync::mpsc::sync_channel(1);
	let (release, released) = std::sync::mpsc::sync_channel(1);
	*handle.submit_fault.lock().unwrap() = Some(SubmitFault {
		repeats: 0,
		observed: None,
		entered: false,
		errno: libc::EIO,
		submitted,
		release: released,
	});
	let pending = handle
		.submit(IoOp::Read, &file, AlignedBuf::new(DIRECT_IO_ALIGN), 0)
		.await
		.unwrap();
	tokio::task::spawn_blocking(move || ready.recv_timeout(Duration::from_secs(10)).unwrap())
		.await
		.unwrap();
	// Worker is gated after submitting the first read. This second read stays
	// queued: the terminal branch must drop it without submitting to the kernel.
	let (reader, _queued_writer) = UnixStream::pair().unwrap();
	let queued_file = Arc::new(std::fs::File::from(OwnedFd::from(reader)));
	let queued_weak = Arc::downgrade(&queued_file);
	let dropped = Arc::new(AtomicBool::new(false));
	let mut buf = AlignedBuf::new(DIRECT_IO_ALIGN);
	buf.drop_probe = Some(dropped.clone());
	let queued = handle
		.submit(IoOp::Read, &queued_file, buf, 0)
		.await
		.unwrap();
	drop(queued_file);
	release.send(()).unwrap();
	let completion = tokio::time::timeout(Duration::from_secs(10), pending)
		.await
		.unwrap()
		.unwrap();
	assert_eq!(completion.result, -libc::EIO);
	assert!(
		tokio::time::timeout(Duration::from_secs(10), queued)
			.await
			.unwrap()
			.is_err()
	);
	assert!(
		handle
			.submit(IoOp::Read, &file, AlignedBuf::new(DIRECT_IO_ALIGN), 0)
			.await
			.is_err()
	);
	tokio::time::timeout(Duration::from_secs(10), handle.close_and_join())
		.await
		.unwrap()
		.unwrap();
	assert!(dropped.load(Ordering::Acquire));
	assert!(queued_weak.upgrade().is_none());
	println!("quarantine control passed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persistent_retryable_enter_errors_terminate_without_completion() {
	if !quarantine_child(
		"retry-budget",
		concat!(
			module_path!(),
			"::persistent_retryable_enter_errors_terminate_without_completion"
		)
		.trim_start_matches("wendelon::"),
	) {
		return;
	}
	for errno in [libc::EAGAIN, libc::EBUSY] {
		let handle = spawn_io_thread(8).unwrap();
		let attempts = Arc::new(AtomicUsize::new(0));
		let (reader, _writer) = UnixStream::pair().unwrap();
		let file = Arc::new(std::fs::File::from(OwnedFd::from(reader)));
		let (submitted, ready) = std::sync::mpsc::sync_channel(1);
		let (release, released) = std::sync::mpsc::sync_channel(1);
		*handle.submit_fault.lock().unwrap() = Some(SubmitFault {
			errno,
			repeats: usize::MAX,
			observed: Some(attempts.clone()),
			entered: false,
			submitted,
			release: released,
		});
		let reply = handle
			.submit(IoOp::Read, &file, AlignedBuf::new(DIRECT_IO_ALIGN), 0)
			.await
			.unwrap();
		tokio::task::spawn_blocking(move || ready.recv_timeout(Duration::from_secs(10)).unwrap())
			.await
			.unwrap();
		let started = Instant::now();
		release.send(()).unwrap();
		// No peer data is ever sent. A retry budget must stop this worker even
		// when none of its operations can complete; this is a hang detector.
		let result = tokio::time::timeout(Duration::from_secs(5), reply).await;
		if result.is_err() {
			// Let the red worker exit after the assertion, without leaking a
			// spinning thread into a subsequent test in this child process.
			*handle.submit_fault.lock().unwrap() = None;
			drop(_writer);
			handle.close_and_join().await.unwrap();
			panic!("persistent enter error {errno} never exhausted its retry budget");
		}
		assert_eq!(result.unwrap().unwrap().result, -errno);
		assert!(
			started.elapsed() >= Duration::from_secs(2),
			"pressure failure exhausted before the no-progress time budget"
		);
		assert!(
			attempts.load(Ordering::SeqCst) <= 64,
			"retry path spun instead of backing off"
		);
		tokio::time::timeout(Duration::from_secs(5), handle.close_and_join())
			.await
			.unwrap()
			.unwrap();
	}
	println!("quarantine control passed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repeated_interrupted_waits_do_not_fail_a_pending_read() {
	let handle = spawn_io_thread(8).unwrap();
	let (reader, mut writer) = UnixStream::pair().unwrap();
	let file = Arc::new(std::fs::File::from(OwnedFd::from(reader)));
	let attempts = Arc::new(AtomicUsize::new(0));
	let (submitted, ready) = std::sync::mpsc::sync_channel(1);
	let (release, released) = std::sync::mpsc::sync_channel(1);
	*handle.submit_fault.lock().unwrap() = Some(SubmitFault {
		errno: libc::EINTR,
		repeats: 1023,
		observed: Some(attempts.clone()),
		entered: false,
		submitted,
		release: released,
	});
	let mut reply = handle
		.submit(IoOp::Read, &file, AlignedBuf::new(DIRECT_IO_ALIGN), 0)
		.await
		.unwrap();
	tokio::task::spawn_blocking(move || ready.recv_timeout(Duration::from_secs(10)).unwrap())
		.await
		.unwrap();
	release.send(()).unwrap();
	tokio::time::timeout(Duration::from_secs(5), async {
		while attempts.load(Ordering::SeqCst) < 1024 {
			assert!(
				matches!(
					reply.try_recv(),
					Err(tokio::sync::oneshot::error::TryRecvError::Empty)
				),
				"EINTR killed the pending read"
			);
			tokio::task::yield_now().await;
		}
	})
	.await
	.unwrap();
	assert!(matches!(
		reply.try_recv(),
		Err(tokio::sync::oneshot::error::TryRecvError::Empty)
	));
	writer.write_all(&[0x7a; 32]).unwrap();
	let result = tokio::time::timeout(Duration::from_secs(10), reply)
		.await
		.unwrap()
		.unwrap();
	assert_eq!(result.result, 32);
	assert_eq!(&result.buf.unwrap().as_slice()[..32], &[0x7a; 32]);
	handle.close_and_join().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transient_pressure_recovers_and_resumes_sqe_admission() {
	for errno in [libc::EAGAIN, libc::EBUSY] {
		let handle = spawn_io_thread(8).unwrap();
		for _cycle in 0..2 {
			let (reader, mut writer) = UnixStream::pair().unwrap();
			let file = Arc::new(std::fs::File::from(OwnedFd::from(reader)));
			let attempts = Arc::new(AtomicUsize::new(0));
			let (submitted, ready) = std::sync::mpsc::sync_channel(1);
			let (release, released) = std::sync::mpsc::sync_channel(1);
			*handle.submit_fault.lock().unwrap() = Some(SubmitFault {
				errno,
				repeats: 3,
				observed: Some(attempts.clone()),
				entered: false,
				submitted,
				release: released,
			});
			let first = handle
				.submit(IoOp::Read, &file, AlignedBuf::new(DIRECT_IO_ALIGN), 0)
				.await
				.unwrap();
			tokio::task::spawn_blocking(move || {
				ready.recv_timeout(Duration::from_secs(10)).unwrap()
			})
			.await
			.unwrap();
			let (next_reader, mut next_writer) = UnixStream::pair().unwrap();
			let next_file = Arc::new(std::fs::File::from(OwnedFd::from(next_reader)));
			let second = handle
				.submit(IoOp::Read, &next_file, AlignedBuf::new(DIRECT_IO_ALIGN), 0)
				.await
				.unwrap();
			release.send(()).unwrap();
			tokio::time::timeout(Duration::from_secs(5), async {
				while attempts.load(Ordering::SeqCst) < 4 {
					tokio::task::yield_now().await;
				}
			})
			.await
			.unwrap();
			// The second request was queued behind the first error. A real CQE
			// must reset retry mode so its SQE is admitted on the next iteration.
			writer.write_all(&[0x61; 32]).unwrap();
			next_writer.write_all(&[0x62; 16]).unwrap();
			let first = tokio::time::timeout(Duration::from_secs(10), first)
				.await
				.unwrap()
				.unwrap();
			let second = tokio::time::timeout(Duration::from_secs(10), second)
				.await
				.unwrap()
				.unwrap();
			assert_eq!(first.result, 32);
			assert_eq!(second.result, 16);
			assert_eq!(&first.buf.unwrap().as_slice()[..32], &[0x61; 32]);
			assert_eq!(&second.buf.unwrap().as_slice()[..16], &[0x62; 16]);
		}
		handle.close_and_join().await.unwrap();
	}
}
