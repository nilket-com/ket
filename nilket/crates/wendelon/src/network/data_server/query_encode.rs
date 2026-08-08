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

/// Rows per encode quantum for this batch, bounded by rows *and* by bytes.
///
/// Policy for a row wider than the byte bound: one row is the indivisible unit of
/// encoding — there is nothing beneath it to split — so such a row is encoded alone and
/// the byte bound is knowingly exceeded for it. Saying so here makes that a declared
/// limit of the mechanism rather than a silent one.
pub(crate) fn encode_quantum_rows(batch: &RecordBatch) -> usize {
	let rows = batch.num_rows();
	if rows == 0 {
		return 0;
	}
	let bytes_per_row = batch.get_array_memory_size().div_ceil(rows).max(1);
	(ENCODE_QUANTUM_BYTES / bytes_per_row)
		.max(1)
		.min(ENCODE_QUANTUM_ROWS)
		.min(rows)
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
		let quantum_limit = encode_quantum_rows(&batch);
		let mut offset = 0usize;
		loop {
			if let Some(expired) = clocks.expired() {
				return Ok(Err(expired));
			}
			// A zero-row batch still encodes once: its schema is the payload.
			let quantum = if rows == 0 {
				batch.clone()
			} else {
				batch.slice(offset, quantum_limit.min(rows - offset))
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
