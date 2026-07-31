use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::Instant;
use tokio::io::{AsyncRead, ReadBuf};
use wendelon::network::{BufPool, read_frame};
use wendelon::schema::{FrameHeader, MsgType};

struct CountingAllocator;

static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);

#[global_allocator]
static GLOBAL_ALLOCATOR: CountingAllocator = CountingAllocator;

// SAFETY: This wrapper delegates every allocation primitive directly
// to `System` while updating a monotonically increasing counter.
unsafe impl GlobalAlloc for CountingAllocator {
	unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
		ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
		unsafe { System.alloc(layout) }
	}

	unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
		unsafe { System.dealloc(ptr, layout) }
	}

	unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
		ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
		unsafe { System.alloc_zeroed(layout) }
	}

	unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
		ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
		unsafe { System.realloc(ptr, layout, new_size) }
	}
}

fn reset_allocations() {
	ALLOCATIONS.store(0, Ordering::Relaxed);
}

fn allocations() -> usize {
	ALLOCATIONS.load(Ordering::Relaxed)
}

fn encoded_frame() -> Vec<u8> {
	let meta = vec![0x11; 256];
	let arrow = vec![0x22; 32 * 1024];
	let trailing = vec![0x33; 1024];
	let header = FrameHeader {
		magic: wendelon::network::HRFT_MAGIC,
		version: 1,
		msg_type: MsgType::STATUS_REQ.0,
		flags: 0,
		stream_id: 7,
		meta_len: meta.len() as u32,
		arrow_len: arrow.len() as u32,
		trailing_len: trailing.len() as u32,
	};
	let mut encoded = Vec::with_capacity(
		std::mem::size_of::<FrameHeader>() + meta.len() + arrow.len() + trailing.len(),
	);
	encoded.extend_from_slice(bytemuck::bytes_of(&header));
	encoded.extend_from_slice(&meta);
	encoded.extend_from_slice(&arrow);
	encoded.extend_from_slice(&trailing);
	encoded
}

struct SliceReader<'a> {
	bytes: &'a [u8],
	offset: usize,
}

impl<'a> SliceReader<'a> {
	fn new(bytes: &'a [u8]) -> Self {
		Self { bytes, offset: 0 }
	}
}

impl AsyncRead for SliceReader<'_> {
	fn poll_read(
		mut self: std::pin::Pin<&mut Self>,
		_cx: &mut Context<'_>,
		buf: &mut ReadBuf<'_>,
	) -> Poll<std::io::Result<()>> {
		let remaining = &self.bytes[self.offset..];
		if remaining.is_empty() {
			return Poll::Ready(Ok(()));
		}
		let read_len = remaining.len().min(buf.remaining());
		buf.put_slice(&remaining[..read_len]);
		self.offset += read_len;
		Poll::Ready(Ok(()))
	}
}

async fn warm_read(payload: &[u8], pool: &Arc<BufPool>) {
	let mut reader = SliceReader::new(payload);
	let frame = read_frame(&mut reader, pool).await.unwrap();
	drop(frame);
}

/// Bench the transport-agnostic frame decoder that sits directly on
/// the QUIC hot path. Real `quinn` transport/auth is covered by the
/// integration tests; this benchmark isolates the allocator-count
/// claim itself and does not benchmark `quinn::RecvStream` directly.
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
	let pool = BufPool::new(8, 512 * 1024);
	let payload = encoded_frame();

	warm_read(&payload, &pool).await;

	const ITERS: usize = 256;
	let mut total = std::time::Duration::ZERO;
	let mut max_allocations = 0usize;

	for _ in 0..ITERS {
		let mut reader = SliceReader::new(&payload);
		reset_allocations();
		let started = Instant::now();
		let frame = read_frame(&mut reader, &pool)
			.await
			.map_err(|err| format!("read_frame on the QUIC hot path: {err}"))?;
		total += started.elapsed();
		max_allocations = max_allocations.max(allocations());
		drop(frame);
	}

	println!(
		"quic_hot_path iterations={ITERS} frame_bytes={} avg_read_ns={} max_allocations_per_read={max_allocations}",
		payload.len(),
		total.as_nanos() / ITERS as u128,
	);

	if max_allocations != 0 {
		return Err(format!(
			"hot-path QUIC read_frame allocated after warmup (max_allocations_per_read={max_allocations})",
		)
		.into());
	}

	Ok(())
}
