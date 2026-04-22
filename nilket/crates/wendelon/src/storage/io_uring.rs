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

use io_uring::{IoUring, opcode, types};
use std::alloc::{self, Layout};
use std::collections::HashMap;
use std::os::unix::io::RawFd;
use std::thread;
use tokio::sync::{mpsc, oneshot};

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
		Self { ptr, len }
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

/// Result handed back to the waiting tokio task.
pub struct IoCompletion {
	/// The buffer originally submitted, returned for reuse.
	/// `None` for sync-only requests.
	pub buf: Option<AlignedBuf>,
	/// Kernel return value: bytes transferred if >= 0, negative errno otherwise.
	pub result: i32,
}

// ============================================================
//  TOKIO-SIDE HANDLE
// ============================================================

/// Cloneable handle to a dedicated `io_uring` thread. All clones
/// feed the same SQ.
#[derive(Clone)]
pub struct IoRingHandle {
	tx: mpsc::Sender<IoRequest>,
}

impl IoRingHandle {
	/// Submit a request. The returned `oneshot::Receiver` fires
	/// when the kernel completes the I/O.
	pub async fn submit(
		&self,
		op: IoOp,
		fd: RawFd,
		buf: AlignedBuf,
		offset: u64,
	) -> std::io::Result<oneshot::Receiver<IoCompletion>> {
		let (reply_tx, reply_rx) = oneshot::channel();
		let req = IoRequest {
			op,
			fd,
			buf: Some(buf),
			offset,
			reply: reply_tx,
		};
		self.tx.send(req).await.map_err(|_| {
			std::io::Error::new(std::io::ErrorKind::BrokenPipe, "io_uring thread is gone")
		})?;
		Ok(reply_rx)
	}

	/// See [`IoOp::Fsync`] for the ordering caveat — callers must
	/// have already awaited every prior write completion on `fd`
	/// for this fsync to be a meaningful durability barrier.
	///
	/// [`submit`]: Self::submit
	pub async fn submit_fsync(
		&self,
		fd: RawFd,
	) -> std::io::Result<oneshot::Receiver<IoCompletion>> {
		self.submit_sync(IoOp::Fsync, fd).await
	}

	/// Submit an `fdatasync(2)` against `fd`.
	pub async fn submit_fdatasync(
		&self,
		fd: RawFd,
	) -> std::io::Result<oneshot::Receiver<IoCompletion>> {
		self.submit_sync(IoOp::Fdatasync, fd).await
	}

	async fn submit_sync(
		&self,
		op: IoOp,
		fd: RawFd,
	) -> std::io::Result<oneshot::Receiver<IoCompletion>> {
		let (reply_tx, reply_rx) = oneshot::channel();
		let req = IoRequest {
			op,
			fd,
			buf: None,
			offset: 0,
			reply: reply_tx,
		};
		self.tx.send(req).await.map_err(|_| {
			std::io::Error::new(std::io::ErrorKind::BrokenPipe, "io_uring thread is gone")
		})?;
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
	let ring = IoUring::new(depth)?;
	// Channel capacity: 2× depth gives slack for bursty submitters
	// without unbounded memory growth.
	let (tx, rx) = mpsc::channel::<IoRequest>((depth as usize).saturating_mul(2).max(1));
	let thread_name = thread_name.into();
	thread::Builder::new()
		.name(thread_name)
		.spawn(move || io_thread_loop(ring, rx))?;
	Ok(IoRingHandle { tx })
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
fn io_thread_loop(mut ring: IoUring, mut rx: mpsc::Receiver<IoRequest>) {
	let mut cookie: u64 = 1;
	let mut in_flight: HashMap<u64, (Option<AlignedBuf>, oneshot::Sender<IoCompletion>)> =
		HashMap::new();

	loop {
		// 1. Block when idle. Shutdown signalled by channel close.
		if in_flight.is_empty() {
			match rx.blocking_recv() {
				Some(req) => push_request(&mut ring, &mut cookie, &mut in_flight, req),
				None => return,
			}
		}

		// 2. Drain additional ready requests.
		loop {
			if ring.submission().is_full() {
				break;
			}
			match rx.try_recv() {
				Ok(req) => push_request(&mut ring, &mut cookie, &mut in_flight, req),
				Err(mpsc::error::TryRecvError::Empty) => break,
				Err(mpsc::error::TryRecvError::Disconnected) => {
					// No more new work will arrive, but we must still
					// reap anything already in flight before exiting.
					break;
				}
			}
		}

		// 3. Submit + wait for at least one completion.
		match ring.submit_and_wait(1) {
			Ok(_) => {}
			Err(e) => {
				// Fatal: tear down all pending oneshots with the error.
				drain_with_error(&mut in_flight, &e);
				return;
			}
		}

		// 4. Reap CQEs.
		let mut cq = ring.completion();
		while let Some(cqe) = cq.next() {
			if let Some((buf, reply)) = in_flight.remove(&cqe.user_data()) {
				let _ = reply.send(IoCompletion {
					buf,
					result: cqe.result(),
				});
			}
		}

		// If the channel is closed and nothing remains in flight, exit.
		if rx.is_closed() && in_flight.is_empty() {
			return;
		}
	}
}

/// Destructure an [`IoRequest`], build the SQE, and register the
/// buffer + reply in `in_flight` before pushing to the SQ.
fn push_request(
	ring: &mut IoUring,
	cookie: &mut u64,
	in_flight: &mut HashMap<u64, (Option<AlignedBuf>, oneshot::Sender<IoCompletion>)>,
	req: IoRequest,
) {
	let id = *cookie;
	*cookie = cookie.wrapping_add(1).max(1); // never let cookie be 0

	let IoRequest {
		op,
		fd,
		buf,
		offset,
		reply,
	} = req;

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

	in_flight.insert(id, (buf, reply));

	// Safety: the buffer referenced by `entry` is kept alive in
	// `in_flight` until the matching CQE is reaped.
	unsafe {
		ring.submission()
			.push(&entry)
			.expect("SQ push: checked !is_full() above");
	}
}

/// Send an error result to every pending oneshot, dropping their buffers.
fn drain_with_error(
	in_flight: &mut HashMap<u64, (Option<AlignedBuf>, oneshot::Sender<IoCompletion>)>,
	err: &std::io::Error,
) {
	// EIO on Linux; used as a generic fallback when the std::io::Error
	// carries no OS errno (avoiding a direct `libc` dep).
	const GENERIC_IO_ERRNO: i32 = 5;
	for (_id, (buf, reply)) in in_flight.drain() {
		let _ = reply.send(IoCompletion {
			buf,
			result: -err.raw_os_error().unwrap_or(GENERIC_IO_ERRNO),
		});
	}
}
