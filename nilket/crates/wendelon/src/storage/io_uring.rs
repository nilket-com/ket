//! Dedicated `io_uring` submission thread (ADR 008 / ADR 009).
//!
//! One instance of [`IoRingHandle`] maps one-to-one to a single OS
//! thread and a single `io_uring::IoUring` instance. Wendelon can
//! then compose those handles into separate durability-class workers
//! such as a dedicated WAL ring and one or more slab rings.
//!
//! Threading model:
//!  * `tokio` tasks submit [`IoRequest`]s over an MPSC channel.
//!  * The dedicated OS thread drains the channel, pushes SQEs with a
//!    monotonic `user_data` cookie, calls `submit_and_wait(1)`, reaps
//!    the CQEs, and fires the matching `oneshot::Sender`.
//!  * Buffers are kept alive on the I/O thread (inside `in_flight`)
//!    for the full kernel op, then returned to the caller via the
//!    oneshot reply so the memory can be reused without copies.
//!
//! This file deliberately does NOT manage slab files, file descriptors,
//! indexing, or recovery; those belong to a higher layer that uses
//! this driver as its backing.

use crate::metrics::{IoUringClass, global as metrics};
use crate::storage::ownership::{
	DataDirectoryOwner, StorageWorker, WorkerJoins, WritePermit, closed_error,
};
use io_uring::{IoUring, opcode, types};
use std::alloc::{self, Layout};
use std::os::unix::io::{AsRawFd, RawFd};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use tokio::sync::{mpsc, oneshot};

mod lifetime;
use lifetime::{InFlightRequests, cancel_outstanding, fail_ring, retryable_enter_error};

/// Default SQ / CQ depth. Large enough to keep a PCIe 4/5 NVMe saturated.
pub const DEFAULT_RING_DEPTH: u32 = 1024;

/// Alignment required for `O_DIRECT` buffers on common filesystems.
pub const DIRECT_IO_ALIGN: usize = 4096;

// ============================================================
//  ALIGNED BUFFER
//
//  A heap buffer aligned to [`DIRECT_IO_ALIGN`], suitable for
//  `O_DIRECT` reads and writes. Length must also be a multiple
//  of the alignment — enforced by [`AlignedBuf::new`].
// ============================================================
pub struct AlignedBuf {
	ptr: *mut u8,
	len: usize,
	#[cfg(test)]
	drop_probe: Option<Arc<AtomicBool>>,
}

// Safety: `AlignedBuf` owns its allocation exclusively; ownership
// transfer across threads is sound because no aliasing occurs.
unsafe impl Send for AlignedBuf {}
unsafe impl Sync for AlignedBuf {}

impl AlignedBuf {
	/// Allocate a zeroed page-aligned buffer of `len` bytes.
	/// Panics if `len` is zero or not a multiple of [`DIRECT_IO_ALIGN`].
	pub fn new(len: usize) -> Self {
		assert!(len > 0, "AlignedBuf length must be non-zero");
		assert!(
			len % DIRECT_IO_ALIGN == 0,
			"AlignedBuf length must be a multiple of {DIRECT_IO_ALIGN} for O_DIRECT"
		);
		let layout =
			Layout::from_size_align(len, DIRECT_IO_ALIGN).expect("valid page-aligned layout");
		// Safety: layout size is non-zero and alignment is a power of two.
		let ptr = unsafe { alloc::alloc_zeroed(layout) };
		if ptr.is_null() {
			alloc::handle_alloc_error(layout);
		}
		Self {
			ptr,
			len,
			#[cfg(test)]
			drop_probe: None,
		}
	}

	pub fn len(&self) -> usize {
		self.len
	}

	pub fn is_empty(&self) -> bool {
		self.len == 0
	}

	pub fn as_ptr(&self) -> *const u8 {
		self.ptr
	}

	pub fn as_mut_ptr(&mut self) -> *mut u8 {
		self.ptr
	}

	pub fn as_slice(&self) -> &[u8] {
		// Safety: we own the allocation for `len` bytes.
		unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
	}

	pub fn as_mut_slice(&mut self) -> &mut [u8] {
		// Safety: we own the allocation for `len` bytes and hold &mut self.
		unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
	}
}

impl Drop for AlignedBuf {
	fn drop(&mut self) {
		#[cfg(test)]
		if let Some(probe) = &self.drop_probe {
			probe.store(true, Ordering::Release);
		}
		let layout =
			Layout::from_size_align(self.len, DIRECT_IO_ALIGN).expect("valid page-aligned layout");
		// Safety: `ptr` was allocated with this layout by `AlignedBuf::new`.
		unsafe { alloc::dealloc(self.ptr, layout) };
	}
}

// ============================================================
//  REQUEST / COMPLETION
// ============================================================

/// Kind of I/O operation to submit.
#[derive(Clone, Copy, Debug)]
pub enum IoOp {
	Write,
	Read,
	/// Issue an `fsync(2)` on the fd.
	///
	/// Ordering caveat (from the io_uring manpage): fsync CQEs are
	/// not ordered against in-flight write CQEs. Callers wanting a
	/// durability barrier must await their write completions
	/// before submitting the fsync — which is the pattern
	/// [`crate::storage::core::LocalSlabStore::fsync`] follows.
	Fsync,
	/// Issue an `fdatasync(2)` on the fd. The completion ordering
	/// caveat is identical to [`IoOp::Fsync`]; callers must await
	/// prior write completions before using it as a durability
	/// barrier.
	Fdatasync,
}

/// One I/O submission unit. The buffer is retained by the I/O
/// thread until the kernel completes the operation.
pub struct IoRequest {
	pub op: IoOp,
	pub fd: RawFd,
	pub buf: Option<AlignedBuf>,
	pub offset: u64,
	pub reply: oneshot::Sender<IoCompletion>,
}

// Keep the public request shape unchanged; ownership travels in the private queue.
struct QueuedIoRequest {
	request: IoRequest,
	/// Keeps `request.fd` open until the kernel has finished with it. A bare
	/// descriptor number in a queue is a dangling reference: if the caller's
	/// last handle closes it before the ring submits, the number can be reused
	/// by any other open in the process and the operation lands in that file.
	file: Arc<std::fs::File>,
	observation: Option<crate::storage::finality_observation::WorkToken>,
	owner: Option<DataDirectoryOwner>,
	permit: Option<WritePermit>,
}

enum IoMessage {
	Request(QueuedIoRequest),
	Close,
}

struct RingWorker {
	tx: mpsc::Sender<IoMessage>,
	closed: AtomicBool,
	admission: std::sync::Mutex<()>,
	joins: Arc<WorkerJoins>,
}

#[async_trait::async_trait]
impl StorageWorker for RingWorker {
	async fn close_and_join(&self) -> std::io::Result<()> {
		{
			let _admission = self.admission.lock().unwrap();
			self.closed.store(true, Ordering::SeqCst);
		}
		let _ = self.tx.send(IoMessage::Close).await;
		self.joins.join().await
	}
}

struct InFlightObservation {
	token: Option<crate::storage::finality_observation::WorkToken>,
	completed: bool,
}

impl Drop for InFlightObservation {
	fn drop(&mut self) {
		// A fatal engine exit/panic is not proof that kernel work ceased. Refuse
		// observation instead of introducing a cancellation/IO recovery policy.
		if !self.completed {
			if let Some(token) = &self.token {
				token.uncertain();
			}
		}
	}
}

/// Result handed back to the waiting tokio task.
pub struct IoCompletion {
	/// The buffer originally submitted, returned for reuse.
	/// `None` for sync-only requests.
	pub buf: Option<AlignedBuf>,
	/// Kernel return value: bytes transferred if >= 0, negative errno otherwise.
	pub result: i32,
}

struct InFlightRequest {
	observation: InFlightObservation,
	/// Held until the CQE is reaped; see `QueuedIoRequest::file`.
	_file: Arc<std::fs::File>,
	_owner: Option<DataDirectoryOwner>,
	_permit: Option<WritePermit>,
	op: IoOp,
	buf: Option<AlignedBuf>,
	reply: oneshot::Sender<IoCompletion>,
}

// ============================================================
//  TOKIO-SIDE HANDLE
// ============================================================

/// Cloneable handle to a dedicated `io_uring` thread. All clones
/// feed the same SQ.
#[cfg(test)]
type ObservationCompletionGate = Arc<
	std::sync::Mutex<
		Option<(
			std::sync::mpsc::SyncSender<()>,
			std::sync::mpsc::Receiver<()>,
		)>,
	>,
>;

#[cfg(test)]
struct SubmitFault {
	errno: i32,
	repeats: usize,
	entered: bool,
	submitted: std::sync::mpsc::SyncSender<()>,
	release: std::sync::mpsc::Receiver<()>,
}
#[cfg(test)]
type SubmitFaultSlot = Arc<std::sync::Mutex<Option<SubmitFault>>>;

#[cfg(test)]
mod submit_error_tests;

#[derive(Clone)]
pub struct IoRingHandle {
	#[cfg(test)]
	cqe_gate: ObservationCompletionGate,
	#[cfg(test)]
	submit_fault: SubmitFaultSlot,
	worker: Arc<RingWorker>,
	class: IoUringClass,
	pending_submissions: Arc<AtomicU64>,
	owner: Option<DataDirectoryOwner>,
}

impl IoRingHandle {
	/// Keep the node owner alive in queued and in-flight kernel operations.
	pub(crate) fn with_data_directory_owner(mut self, owner: DataDirectoryOwner) -> Self {
		owner
			.register_worker(self.worker.clone())
			.expect("register active storage worker");
		self.owner = Some(owner);
		self
	}

	pub(crate) fn try_with_data_directory_owner(
		mut self,
		owner: DataDirectoryOwner,
	) -> std::io::Result<Self> {
		owner.register_worker(self.worker.clone())?;
		self.owner = Some(owner);
		Ok(self)
	}

	pub(crate) fn data_directory_owner(&self) -> Option<DataDirectoryOwner> {
		self.owner.clone()
	}

	pub(crate) fn ensure_open(&self) -> std::io::Result<()> {
		if self.worker.closed.load(Ordering::SeqCst) {
			return Err(closed_error());
		}
		Ok(())
	}

	pub async fn close_and_join(&self) -> std::io::Result<()> {
		self.worker.close_and_join().await
	}

	/// Submit a request. The returned `oneshot::Receiver` fires
	/// when the kernel completes the I/O.
	/// Queue `op` against `file`. The ring holds `file` open until the
	/// operation's completion is reaped, so dropping every caller handle while
	/// the request waits cannot redirect it to a different file.
	pub async fn submit(
		&self,
		op: IoOp,
		file: &Arc<std::fs::File>,
		buf: AlignedBuf,
		offset: u64,
	) -> std::io::Result<oneshot::Receiver<IoCompletion>> {
		let fd = file.as_raw_fd();
		self.ensure_open()?;
		let slot = self.worker.tx.reserve().await.map_err(|_| closed_error())?;
		let _admission = self.worker.admission.lock().unwrap();
		self.ensure_open()?;
		let permit = if matches!(op, IoOp::Read) {
			None
		} else {
			self.owner
				.as_ref()
				.map(DataDirectoryOwner::write_permit)
				.transpose()?
		};
		let (reply_tx, reply_rx) = oneshot::channel();
		let req = IoRequest {
			op,
			fd,
			buf: Some(buf),
			offset,
			reply: reply_tx,
		};
		self.pending_submissions.fetch_add(1, Ordering::Relaxed);
		metrics().set_io_uring_submission_queue_depth(
			self.class,
			self.pending_submissions.load(Ordering::Relaxed),
		);
		let req = QueuedIoRequest {
			request: req,
			file: Arc::clone(file),
			observation: crate::storage::finality_observation::current_work(),
			owner: self.owner.clone(),
			permit,
		};
		slot.send(IoMessage::Request(req));
		Ok(reply_rx)
	}

	/// See [`IoOp::Fsync`] for the ordering caveat — callers must
	/// have already awaited every prior write completion on `fd`
	/// for this fsync to be a meaningful durability barrier.
	///
	/// [`submit`]: Self::submit
	pub async fn submit_fsync(
		&self,
		file: &Arc<std::fs::File>,
	) -> std::io::Result<oneshot::Receiver<IoCompletion>> {
		self.submit_sync(IoOp::Fsync, file).await
	}

	/// Submit an `fdatasync(2)` against `fd`.
	pub async fn submit_fdatasync(
		&self,
		file: &Arc<std::fs::File>,
	) -> std::io::Result<oneshot::Receiver<IoCompletion>> {
		self.submit_sync(IoOp::Fdatasync, file).await
	}

	async fn submit_sync(
		&self,
		op: IoOp,
		file: &Arc<std::fs::File>,
	) -> std::io::Result<oneshot::Receiver<IoCompletion>> {
		let fd = file.as_raw_fd();
		self.ensure_open()?;
		let slot = self.worker.tx.reserve().await.map_err(|_| closed_error())?;
		let _admission = self.worker.admission.lock().unwrap();
		self.ensure_open()?;
		let permit = self
			.owner
			.as_ref()
			.map(DataDirectoryOwner::write_permit)
			.transpose()?;
		let (reply_tx, reply_rx) = oneshot::channel();
		let req = IoRequest {
			op,
			fd,
			buf: None,
			offset: 0,
			reply: reply_tx,
		};
		self.pending_submissions.fetch_add(1, Ordering::Relaxed);
		metrics().set_io_uring_submission_queue_depth(
			self.class,
			self.pending_submissions.load(Ordering::Relaxed),
		);
		let req = QueuedIoRequest {
			request: req,
			file: Arc::clone(file),
			observation: crate::storage::finality_observation::current_work(),
			owner: self.owner.clone(),
			permit,
		};
		slot.send(IoMessage::Request(req));
		Ok(reply_rx)
	}
}

// ============================================================
//  SPAWN
// ============================================================

/// Spawn a dedicated OS thread owning a fresh `io_uring` with
/// `depth` SQ entries. The returned handle feeds that thread.
pub fn spawn_io_thread(depth: u32) -> std::io::Result<IoRingHandle> {
	spawn_io_thread_named(depth, "wendelon-io-uring")
}

/// Spawn a dedicated OS thread owning a fresh `io_uring` with
/// `depth` SQ entries and a caller-supplied thread name.
pub fn spawn_io_thread_named(
	depth: u32,
	thread_name: impl Into<String>,
) -> std::io::Result<IoRingHandle> {
	spawn_io_thread_named_for_class(depth, thread_name, IoUringClass::Generic)
}

/// Spawn a dedicated OS thread owning a fresh `io_uring` with
/// `depth` SQ entries, a caller-supplied thread name, and an
/// explicit durability class for metrics.
pub fn spawn_io_thread_named_for_class(
	depth: u32,
	thread_name: impl Into<String>,
	class: IoUringClass,
) -> std::io::Result<IoRingHandle> {
	let ring = IoUring::new(depth)?;
	// Channel capacity: 2× depth gives slack for bursty submitters
	// without unbounded memory growth.
	let (tx, rx) = mpsc::channel::<IoMessage>((depth as usize).saturating_mul(2).max(1));
	#[cfg(test)]
	let cqe_gate = Arc::new(std::sync::Mutex::new(None));
	#[cfg(test)]
	let submit_fault = Arc::new(std::sync::Mutex::new(None));
	let pending_submissions = Arc::new(AtomicU64::new(0));
	let thread_name = thread_name.into();
	metrics().set_io_uring_submission_queue_depth(class, 0);
	metrics().set_io_uring_completion_queue_depth(class, 0);
	let thread = thread::Builder::new().name(thread_name).spawn({
		let pending_submissions = pending_submissions.clone();
		#[cfg(test)]
		let thread_gate = cqe_gate.clone();
		#[cfg(test)]
		let thread_fault = submit_fault.clone();
		move || {
			io_thread_loop(
				ring,
				rx,
				class,
				pending_submissions,
				#[cfg(test)]
				thread_gate,
				#[cfg(test)]
				thread_fault,
			)
		}
	})?;
	Ok(IoRingHandle {
		#[cfg(test)]
		cqe_gate,
		#[cfg(test)]
		submit_fault,
		worker: Arc::new(RingWorker {
			tx,
			closed: AtomicBool::new(false),
			admission: Default::default(),
			joins: WorkerJoins::new(vec![thread]),
		}),
		class,
		pending_submissions,
		owner: None,
	})
}

// ============================================================
//  IO THREAD MAIN LOOP
//
//  Loop shape:
//   1. If nothing is in flight, block on the channel for a
//      request (or observe shutdown via channel close).
//   2. Drain any additional requests non-blocking, up to the
//      point where the SQ is full.
//   3. Call `submit_and_wait(1)` to push SQEs and wait for at
//      least one CQE.
//   4. Reap all available CQEs, firing oneshots.
//
//  Known limitation: while the thread is blocked inside
//  `submit_and_wait`, newly-arriving requests from the channel
//  are not enqueued until the next CQE fires. Under sustained
//  load this is never observable; under idle→burst transitions
//  the first request in a burst may see a small scheduling
//  delay. A future revision can register an eventfd with the
//  ring to wake the submit call on channel activity.
// ============================================================
fn io_thread_loop(
	mut ring: IoUring,
	mut rx: mpsc::Receiver<IoMessage>,
	class: IoUringClass,
	pending_submissions: Arc<AtomicU64>,
	#[cfg(test)] cqe_gate: ObservationCompletionGate,
	#[cfg(test)] submit_fault: SubmitFaultSlot,
) {
	let mut cookie: u64 = 1;
	let mut in_flight = InFlightRequests::default();
	let mut enter_retries = 0usize;
	const MAX_ENTER_RETRIES: usize = 64;

	loop {
		// 1. Block when idle. Shutdown signalled by channel close.
		if in_flight.is_empty() {
			match rx.blocking_recv() {
				Some(IoMessage::Close) => {
					rx.close();
					continue;
				}
				Some(IoMessage::Request(req)) => {
					decrement_pending_submissions(&pending_submissions, class);
					push_request(&mut ring, &mut cookie, &mut in_flight, req);
				}
				None => {
					metrics().set_io_uring_completion_queue_depth(class, 0);
					return;
				}
			}
		}

		// 2. Stop adding work while recovering from an enter error.
		while enter_retries == 0 {
			if ring.submission().is_full() {
				break;
			}
			match rx.try_recv() {
				Ok(IoMessage::Close) => {
					rx.close();
				}
				Ok(IoMessage::Request(req)) => {
					decrement_pending_submissions(&pending_submissions, class);
					push_request(&mut ring, &mut cookie, &mut in_flight, req);
				}
				Err(mpsc::error::TryRecvError::Empty) => break,
				Err(mpsc::error::TryRecvError::Disconnected) => {
					// No more new work will arrive, but we must still
					// reap anything already in flight before exiting.
					break;
				}
			}
		}

		metrics().set_io_uring_completion_queue_depth(class, in_flight.len() as u64);
		let span = tracing::trace_span!(
			"storage.io_uring.batch",
			class = class.as_str(),
			submission_queue_depth = pending_submissions.load(Ordering::Relaxed),
			completion_queue_depth = in_flight.len() as u64,
			cq_ready = tracing::field::Empty,
		);
		let _enter = span.enter();

		// 3. Submit + wait for at least one completion.
		match submit_and_wait(
			&ring,
			#[cfg(test)]
			&submit_fault,
		) {
			Ok(_) => enter_retries = 0,
			// An enter error is not a CQE. Reap anything already completed
			// below, then retry with every other request's resources intact.
			Err(e) if retryable_enter_error(&e) && enter_retries < MAX_ENTER_RETRIES => {
				metrics().observe_io_uring_error(class, "submit", e.raw_os_error().unwrap());
				enter_retries += 1;
			}
			Err(e) => {
				// Stop admission and prove cancellation before releasing resources.
				// If that proof fails, fail_ring quarantines them until process exit.
				rx.close();
				let errno = e.raw_os_error().unwrap_or(5);
				metrics().observe_io_uring_error(class, "submit", errno);
				tracing::warn!(
					class = class.as_str(),
					op = "submit",
					errno,
					"io_uring submit_and_wait failed"
				);
				fail_ring(ring, in_flight, class, &e, |ring| cancel_outstanding(ring));
				metrics().set_io_uring_completion_queue_depth(class, 0);
				return;
			}
		}

		// 4. Reap CQEs.
		let mut cq = ring.completion();
		let cq_ready = cq.len();
		span.record("cq_ready", cq.len() as i64);
		while let Some(cqe) = cq.next() {
			if let Some(mut request) = in_flight.remove(&cqe.user_data()) {
				#[cfg(test)]
				{
					let gate = cqe_gate
						.lock()
						.expect("CQE observation test gate poisoned")
						.take();
					if let Some((ready, release)) = gate {
						ready.send(()).unwrap();
						release.recv().unwrap();
					}
				}
				request.observation.completed = true;
				if cqe.result() < 0 {
					let errno = -cqe.result();
					metrics().observe_io_uring_error(class, request.op.as_str(), errno);
					tracing::warn!(
						class = class.as_str(),
						op = request.op.as_str(),
						errno,
						"io_uring operation failed"
					);
				}
				let _ = request.reply.send(IoCompletion {
					buf: request.buf,
					result: cqe.result(),
				});
			}
		}
		metrics().set_io_uring_completion_queue_depth(class, in_flight.len() as u64);

		drop(cq);
		if enter_retries != 0 {
			if cq_ready != 0 {
				enter_retries = 0;
			} else {
				// EAGAIN can mean no SQE was consumed, so a wait-only enter
				// could block forever. Back off after reaping, and exhaust a
				// finite retry budget before safe cancellation/quarantine.
				std::thread::sleep(std::time::Duration::from_millis(1));
			}
		}

		// If the channel is closed and nothing remains in flight, exit.
		if rx.is_closed() && rx.is_empty() && in_flight.is_empty() {
			metrics().set_io_uring_completion_queue_depth(class, 0);
			return;
		}
	}
}

fn submit_and_wait(ring: &IoUring, #[cfg(test)] fault: &SubmitFaultSlot) -> std::io::Result<usize> {
	#[cfg(test)]
	{
		let mut slot = fault.lock().unwrap();
		if let Some(mut fault) = slot.take() {
			if !fault.entered {
				// Submit real pending work, then gate the first injected error.
				ring.submit()?;
				fault.submitted.send(()).unwrap();
				fault.release.recv().unwrap();
				fault.entered = true;
			}
			let errno = fault.errno;
			if fault.repeats > 0 {
				fault.repeats -= 1;
				*slot = Some(fault);
			}
			return Err(std::io::Error::from_raw_os_error(errno));
		}
	}
	ring.submit_and_wait(1)
}

/// Destructure an [`IoRequest`], build the SQE, and register the
/// buffer + reply in `in_flight` before pushing to the SQ.
fn push_request(
	ring: &mut IoUring,
	cookie: &mut u64,
	in_flight: &mut InFlightRequests,
	req: QueuedIoRequest,
) {
	let id = *cookie;
	*cookie = cookie.wrapping_add(1).max(1); // never let cookie be 0

	let QueuedIoRequest {
		request,
		file,
		observation,
		owner,
		permit,
	} = req;
	let IoRequest {
		op,
		fd,
		buf,
		offset,
		reply,
	} = request;

	let entry = match op {
		IoOp::Write => {
			let buf_ref = buf
				.as_ref()
				.expect("io_uring write requests must carry an aligned buffer");
			opcode::Write::new(types::Fd(fd), buf_ref.ptr as *const _, buf_ref.len as u32)
				.offset(offset)
				.build()
				.user_data(id)
		}
		IoOp::Read => {
			let buf_ref = buf
				.as_ref()
				.expect("io_uring read requests must carry an aligned buffer");
			opcode::Read::new(types::Fd(fd), buf_ref.ptr, buf_ref.len as u32)
				.offset(offset)
				.build()
				.user_data(id)
		}
		IoOp::Fsync => opcode::Fsync::new(types::Fd(fd)).build().user_data(id),
		IoOp::Fdatasync => opcode::Fsync::new(types::Fd(fd))
			.flags(types::FsyncFlags::DATASYNC)
			.build()
			.user_data(id),
	};

	in_flight.insert(
		id,
		InFlightRequest {
			_file: file,
			op,
			buf,
			reply,
			_owner: owner,
			_permit: permit,
			observation: InFlightObservation {
				token: observation,
				completed: false,
			},
		},
	);

	// Safety: the buffer referenced by `entry` is kept alive in
	// `in_flight` until the matching CQE is reaped.
	unsafe {
		ring.submission()
			.push(&entry)
			.expect("SQ push: checked !is_full() above");
	}
}

fn decrement_pending_submissions(pending_submissions: &AtomicU64, class: IoUringClass) {
	let mut current = pending_submissions.load(Ordering::Relaxed);
	loop {
		debug_assert!(current > 0, "pending submission count underflow");
		let next = current.saturating_sub(1);
		match pending_submissions.compare_exchange_weak(
			current,
			next,
			Ordering::Relaxed,
			Ordering::Relaxed,
		) {
			Ok(_) => {
				metrics().set_io_uring_submission_queue_depth(class, next);
				return;
			}
			Err(observed) => current = observed,
		}
	}
}

impl IoOp {
	fn as_str(self) -> &'static str {
		match self {
			IoOp::Write => "write",
			IoOp::Read => "read",
			IoOp::Fsync => "fsync",
			IoOp::Fdatasync => "fdatasync",
		}
	}
}

#[cfg(test)]
mod adr629_p2b_observed_ring_tests {
	use super::*;
	use crate::storage::finality_observation::{
		FinalityObservationRegistry, Unavailable, WorkKind, WorkScope, with_work,
	};

	#[tokio::test]
	async fn close_drains_queued_kernel_write_and_revokes_retained_ring_clone() {
		use std::time::Duration;
		let dir = std::env::temp_dir().join(format!("owner-ring-close-{}", uuid::Uuid::new_v4()));
		std::fs::create_dir(&dir).unwrap();
		let owner = DataDirectoryOwner::acquire_existing(&dir).unwrap();
		let handle = spawn_io_thread(8)
			.unwrap()
			.with_data_directory_owner(owner.clone());
		let file = Arc::new(
			std::fs::OpenOptions::new()
				.create_new(true)
				.write(true)
				.open(dir.join("data"))
				.unwrap(),
		);
		let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
		let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
		*handle.cqe_gate.lock().unwrap() = Some((ready_tx, release_rx));
		let mut buf = AlignedBuf::new(DIRECT_IO_ALIGN);
		buf.as_mut_slice().fill(0x61);
		let reply = handle.submit(IoOp::Write, &file, buf, 0).await.unwrap();
		tokio::task::spawn_blocking(move || ready_rx.recv().unwrap())
			.await
			.unwrap();
		// The worker is parked at the first completion; the second accepted
		// write is still in its channel when close begins.
		let mut queued = AlignedBuf::new(DIRECT_IO_ALIGN);
		queued.as_mut_slice().fill(0x62);
		let queued_reply = handle
			.submit(IoOp::Write, &file, queued, DIRECT_IO_ALIGN as u64)
			.await
			.unwrap();
		let closing = {
			let owner = owner.clone();
			tokio::spawn(async move { owner.close(Duration::from_secs(5)).await })
		};
		owner.wait_until_closing().await;
		// Permit remains with the kernel completion even though the caller
		// retains only the receiver. Release the explicit CQE gate, then join.
		release_tx.send(()).unwrap();
		assert_eq!(reply.await.unwrap().result, DIRECT_IO_ALIGN as i32);
		assert_eq!(queued_reply.await.unwrap().result, DIRECT_IO_ALIGN as i32);
		closing.await.unwrap().unwrap();
		assert_eq!(
			std::fs::read(dir.join("data")).unwrap(),
			[vec![0x61; DIRECT_IO_ALIGN], vec![0x62; DIRECT_IO_ALIGN]].concat()
		);
		DataDirectoryOwner::acquire_existing(&dir).unwrap();
		let error = handle.submit_fsync(&file).await.err().unwrap();
		assert!(
			error
				.get_ref()
				.unwrap()
				.is::<crate::storage::ownership::Closed>()
		);
		std::fs::remove_dir_all(dir).unwrap();
	}

	#[tokio::test]
	async fn adr629_p2b_queue_and_inflight_own_work_after_the_reply_receiver_is_dropped() {
		let registry = FinalityObservationRegistry::new();
		let (tx, mut rx) = mpsc::channel(1);
		let handle = IoRingHandle {
			cqe_gate: Arc::new(std::sync::Mutex::new(None)),
			submit_fault: Arc::new(std::sync::Mutex::new(None)),
			worker: Arc::new(RingWorker {
				tx,
				closed: AtomicBool::new(false),
				admission: Default::default(),
				joins: WorkerJoins::new(vec![]),
			}),
			class: IoUringClass::Generic,
			pending_submissions: Arc::new(AtomicU64::new(0)),
			owner: None,
		};
		let owner = registry.begin(WorkScope::Node, WorkKind::StoragePublication);
		// Real submission queue/ownership path, synthetic CQE lifecycle; no kernel claim.
		let reply = with_work(
			owner,
			handle.submit(
				IoOp::Write,
				&Arc::new(std::fs::File::open("/dev/null").unwrap()),
				AlignedBuf::new(DIRECT_IO_ALIGN),
				0,
			),
		)
		.await
		.unwrap();
		drop(reply);
		assert_eq!(registry.test_state(), (1, 0, None));
		let IoMessage::Request(queued) = rx.recv().await.unwrap() else {
			panic!("expected request")
		};
		decrement_pending_submissions(&handle.pending_submissions, handle.class);
		let flight = InFlightObservation {
			token: queued.observation,
			completed: false,
		};
		drop(queued.request);
		assert_eq!(registry.test_state(), (1, 0, None));
		let mut flight = flight;
		flight.completed = true;
		drop(flight);
		assert_eq!(registry.test_state(), (0, 0, None));
	}

	#[test]
	fn adr629_p2b_unknown_inflight_exit_withholds_instead_of_claiming_kernel_cessation() {
		let registry = FinalityObservationRegistry::new();
		let owner = registry.begin(WorkScope::Node, WorkKind::StoragePublication);
		drop(InFlightObservation {
			token: Some(owner),
			completed: false,
		});
		assert_eq!(
			registry.test_state(),
			(0, 0, Some(Unavailable::UnreadableState))
		);
	}
	/// A write queued behind a busy ring must land in the file it was issued
	/// for, even if every caller handle to that file is dropped before the ring
	/// submits it: the descriptor number must not be closed and handed to another
	/// open in the meantime.
	#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
	async fn a_queued_write_lands_in_its_own_file_after_the_caller_drops_it() {
		use std::os::fd::AsRawFd;
		let dir = crate::test_support::TestDir::new("wendelon-ring-fd-lifetime");
		let handle = spawn_io_thread(8).unwrap();
		let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
		let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
		*handle.cqe_gate.lock().unwrap() = Some((ready_tx, release_rx));
		let open = |name: &str| {
			Arc::new(
				std::fs::OpenOptions::new()
					.read(true)
					.write(true)
					.create_new(true)
					.open(dir.join(name))
					.unwrap(),
			)
		};
		// Park the ring worker on the completion of a first write.
		let parker = open("parker");
		let parked = handle
			.submit(IoOp::Write, &parker, AlignedBuf::new(DIRECT_IO_ALIGN), 0)
			.await
			.unwrap();
		tokio::task::spawn_blocking(move || ready_rx.recv().unwrap())
			.await
			.unwrap();
		// Queue a write for `intended`, then drop the caller's only handle.
		let intended = open("intended");
		let mut page = AlignedBuf::new(DIRECT_IO_ALIGN);
		page.as_mut_slice().fill(0x5a);
		let queued = handle
			.submit(IoOp::Write, &intended, page, 0)
			.await
			.unwrap();
		let intended_fd = intended.as_raw_fd();
		drop(intended);
		// An unrelated file opened now takes the lowest free descriptor.
		let bystander = open("bystander");
		let reused = bystander.as_raw_fd() == intended_fd;
		release_tx.send(()).unwrap();
		parked.await.unwrap();
		let completion = queued.await.unwrap();
		assert_eq!(completion.result, DIRECT_IO_ALIGN as i32);
		assert!(
			std::fs::read(dir.join("bystander")).unwrap().is_empty(),
			"the queued write landed in an unrelated file (descriptor reused: {reused})"
		);
		assert_eq!(
			std::fs::read(dir.join("intended")).unwrap(),
			vec![0x5a; DIRECT_IO_ALIGN]
		);
		drop(bystander);
		drop(parker);
	}

	#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
	async fn adr629_p2b_real_kernel_write_keeps_owner_through_cqe_after_waiter_abort() {
		let registry = FinalityObservationRegistry::new();
		let owner = registry.begin(WorkScope::Node, WorkKind::StoragePublication);
		let handle = spawn_io_thread(8).unwrap();
		let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
		let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
		*handle.cqe_gate.lock().unwrap() = Some((ready_tx, release_rx));
		let path = std::env::temp_dir().join(format!("adr629-p2b-ring-{}", uuid::Uuid::new_v4()));
		let file = Arc::new(
			std::fs::OpenOptions::new()
				.write(true)
				.create_new(true)
				.open(&path)
				.unwrap(),
		);
		let submitted = Arc::clone(&file);
		let driver = handle.clone();
		let caller = tokio::spawn(with_work(owner, async move {
			let mut buf = AlignedBuf::new(DIRECT_IO_ALIGN);
			buf.as_mut_slice().fill(0x29);
			let reply = driver
				.submit(IoOp::Write, &submitted, buf, 0)
				.await
				.unwrap();
			reply.await
		}));
		tokio::time::timeout(
			std::time::Duration::from_secs(10),
			tokio::task::spawn_blocking(move || ready_rx.recv().unwrap()),
		)
		.await
		.unwrap()
		.unwrap();
		caller.abort();
		let _ = caller.await;
		assert_eq!(registry.test_state(), (1, 0, None));
		assert_eq!(std::fs::read(&path).unwrap(), vec![0x29; DIRECT_IO_ALIGN]);
		release_tx.send(()).unwrap();
		tokio::time::timeout(std::time::Duration::from_secs(10), async {
			while registry.test_state().0 != 0 {
				tokio::task::yield_now().await;
			}
		})
		.await
		.unwrap();
		assert_eq!(registry.test_state().2, None);
		drop(file);
		drop(handle);
		std::fs::remove_file(path).unwrap();
	}
}
