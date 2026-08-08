//! ADR 541's bounded encode quanta and the blocking-slot pool they run on.
//!
//! Carved out of the `data_server` root under ADR 077's bounded-root rule, alongside
//! `query_clocks`: encoding is a self-contained mechanism with its own invariants — a
//! quantum bounded by rows *and* bytes, and blocking work bounded by a permit held for
//! the true lifetime of the work — and the root must ratchet down rather than absorb
//! each new one.

use std::sync::OnceLock;

use arrow::array::RecordBatch;
use tokio::sync::Semaphore;
use tokio::task::spawn_blocking;

use super::QueryClocks;
use crate::metrics::NetworkCompressionPath;
use crate::network::ipc::wire_encode_batch_for_path;
use crate::schema::ReadStatus;

/// At most this many rows are Arrow-encoded as one indivisible unit of work. Encoding is
/// synchronous CPU work that cannot be preempted mid-call, so the only way to bound it is
/// to keep each call small.
pub(crate) const ENCODE_QUANTUM_ROWS: usize = 4_096;

/// ...and at most this much column data per unit. A row bound alone does not bound the
/// work: a handful of rows carrying large variable-width values is arbitrarily expensive,
/// so a row-only quantum leaves exactly the hole it was meant to close. Whichever bound
/// binds first decides the quantum.
pub(crate) const ENCODE_QUANTUM_BYTES: usize = 8 * 1024 * 1024;

/// How many encode quanta may occupy blocking threads at once, node-wide.
pub(crate) fn encode_blocking_slot_count() -> usize {
	std::thread::available_parallelism()
		.map(|threads| threads.get())
		.unwrap_or(4)
}

/// Bounds how much blocking encode work exists at once across every query on this node.
///
/// The permit is acquired *inside* the raced future and held *inside* the blocking
/// closure, so it is released when the work truly finishes — not when a timed-out caller
/// stops waiting. That ordering is the whole point: `spawn_blocking` cannot be cancelled,
/// so a detached task alone would let each expiry pile another unbounded encode onto the
/// blocking pool. Bounding the slots converts "abandoned and unbounded" into "abandoned
/// and bounded".
fn encode_blocking_slots() -> &'static Semaphore {
	static SLOTS: OnceLock<Semaphore> = OnceLock::new();
	SLOTS.get_or_init(|| Semaphore::new(encode_blocking_slot_count()))
}

/// Bytes the rows *in this slice* logically occupy.
///
/// Deliberately not `get_array_memory_size`: that reports the allocated backing buffers,
/// so a one-row slice of a large batch still reports the whole buffer. Dividing it by the
/// row count then assumes every row is the same width, and a skewed batch — sixty-three
/// tiny strings and one enormous one — defeats that average, grouping the wide row with
/// its neighbours precisely when it must not be. `get_slice_memory_size` is offset- and
/// length-aware, so it prices the rows actually being encoded.
///
/// An unmeasurable column is priced as unbounded: refusing is the safe direction when the
/// whole point of the number is to bound work.
fn slice_memory_size(batch: &RecordBatch) -> usize {
	batch
		.columns()
		.iter()
		.map(|column| {
			column
				.to_data()
				.get_slice_memory_size()
				.unwrap_or(usize::MAX)
		})
		.fold(0usize, |total, size| total.saturating_add(size))
}

/// The largest contiguous run starting at `offset` that satisfies both the row bound and
/// the byte bound, or `None` when the single row at `offset` exceeds the byte bound on
/// its own.
///
/// `None` is a refusal, not a licence. There is nothing beneath one row to split, so a
/// row wider than the bound cannot be encoded within it — and "knowingly exceeds the
/// bound" is not a bound. The caller turns this into a typed `QUERY_BUDGET_EXCEEDED`
/// before any encode work is spawned.
pub(crate) fn encode_quantum_len(batch: &RecordBatch, offset: usize) -> Option<usize> {
	let remaining = batch.num_rows().saturating_sub(offset);
	if remaining == 0 {
		return Some(0);
	}
	let mut hi = ENCODE_QUANTUM_ROWS.min(remaining);
	if slice_memory_size(&batch.slice(offset, hi)) <= ENCODE_QUANTUM_BYTES {
		return Some(hi);
	}
	if slice_memory_size(&batch.slice(offset, 1)) > ENCODE_QUANTUM_BYTES {
		return None;
	}
	// Slice size grows with length, so the largest satisfying length is a binary search.
	let mut lo = 1usize;
	while lo < hi {
		let mid = lo + (hi - lo).div_ceil(2);
		if slice_memory_size(&batch.slice(offset, mid)) <= ENCODE_QUANTUM_BYTES {
			lo = mid;
		} else {
			hi = mid - 1;
		}
	}
	Some(lo)
}

/// Run one unit of synchronous work on a blocking thread, under both clocks and the
/// node's blocking-slot bound.
///
/// Split out from the encode loop so the invariant is testable on its own: the caller
/// injects the work, so a fixture can hold slots occupied and prove that a queued attempt
/// never enters blocking work, that expiry is attributed to the clock that fired, and
/// that permits recover once the work truly ends.
pub(crate) async fn bounded_blocking_encode<T, F>(
	clocks: &mut QueryClocks,
	work: F,
) -> Result<std::io::Result<T>, ReadStatus>
where
	F: FnOnce() -> std::io::Result<T> + Send + 'static,
	T: Send + 'static,
{
	let raced = async move {
		let permit = encode_blocking_slots()
			.acquire()
			.await
			.expect("encode slot semaphore is never closed");
		spawn_blocking(move || {
			let _permit = permit;
			work()
		})
		.await
	};
	match clocks.bound(raced).await {
		Ok(joined) => Ok(joined.unwrap_or_else(|error| Err(std::io::Error::other(error)))),
		Err(expired) => Err(expired),
	}
}

pub(crate) async fn encode_query_stream_batches_with_deadline(
	batches: Vec<RecordBatch>,
	clocks: &mut QueryClocks,
) -> std::io::Result<Result<Vec<Vec<u8>>, ReadStatus>> {
	// Preserve the query protocol's all-or-nothing status model by
	// finishing Arrow serialization before the first QUERY_RESP frame.
	// Once the first frame is on the wire, continuations have no
	// status surface for a terminal expiry.
	//
	// Polling between whole batches could not bound this work: encoding is synchronous,
	// so one oversize batch would overrun the stall bound with nothing able to observe
	// it, and a slow-but-successful encode would then reset the clock and hide the
	// overrun. Each quantum is therefore bounded in rows *and* raced against both clocks
	// on a concurrency-capped blocking thread, so expiry stays attributable to the clock
	// that fired and completion is what marks progress.
	let mut encoded = Vec::with_capacity(batches.len());
	for batch in batches {
		let rows = batch.num_rows();
		let mut offset = 0usize;
		loop {
			if let Some(expired) = clocks.expired() {
				return Ok(Err(expired));
			}
			// A zero-row batch still encodes once: its schema is the payload.
			let quantum = if rows == 0 {
				batch.clone()
			} else {
				let Some(len) = encode_quantum_len(&batch, offset) else {
					// One row wider than the byte bound cannot be bounded. Refuse with
					// a typed status *before* spawning encode work, rather than
					// exceeding the bound and calling it a policy.
					return Ok(Err(ReadStatus::QUERY_BUDGET_EXCEEDED));
				};
				batch.slice(offset, len)
			};
			let quantum_rows = quantum.num_rows();
			let arrow = match bounded_blocking_encode(clocks, move || {
				wire_encode_batch_for_path(&quantum, NetworkCompressionPath::QueryResponse)
			})
			.await
			{
				Ok(result) => result?,
				Err(expired) => return Ok(Err(expired)),
			};
			encoded.push(arrow);
			if rows == 0 {
				break;
			}
			offset += quantum_rows;
			if offset >= rows {
				break;
			}
		}
	}
	Ok(Ok(encoded))
}
