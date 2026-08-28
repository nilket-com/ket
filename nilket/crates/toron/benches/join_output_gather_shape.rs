//! What output gathering costs, by selection cardinality and locality.
//!
//! ADR 554 named 6,379 ms of board-wide output gathering. Before choosing among
//! contiguous-slice reuse, a specialized gather, or avoiding materialization,
//! this prices the axes those remedies actually turn on.
//!
//! `materialize_join_selection` builds a `Vec<Option<u32>>` per side, converts it
//! to a `UInt32Array`, and calls `arrow::compute::take` once per column. Three
//! axes follow from that shape:
//!
//! - **Cardinality** — output rows. A join can emit far more or far fewer rows
//!   than it reads, so output cost does not track input cost, and the two
//!   materialization subterms cannot be assumed to share a fix.
//! - **Locality** — `take` with ascending indices is a near-copy; with scattered
//!   indices it is a random gather bounded by cache misses. This is the axis
//!   contiguous-slice reuse would exploit, and it only pays if real selections
//!   are runs rather than scatter.
//! - **Nullability** — Inner and Cross selections are all `Some`, yet the index
//!   array is built nullable and carries a validity bitmap regardless. Whether
//!   that costs anything is what decides if "specialized gathering" is worth
//!   scoping at all.
//!
//! Reports ns per OUTPUT row. The index-vector build is timed separately from
//! `take` because they have different remedies: the build is Toron's own code,
//! `take` is Arrow's.

use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{
	Array, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray, UInt32Array,
};
use arrow::compute::take;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

const SOURCE_ROWS: usize = 4_000_000;
const SELECTIONS: [usize; 3] = [100_000, 1_000_000, 4_000_000];
const REPLICATES: usize = 5;
/// Run length for the clustered pattern: long enough to be a real run, short
/// enough that a 4M-row source is not walked sequentially end to end.
const RUN: usize = 64;

#[derive(Clone, Copy)]
enum Locality {
	/// Ascending, no gaps. The best case a contiguous-slice remedy could reach.
	Sequential,
	/// Ascending runs of `RUN` rows, each run starting at a random offset.
	Clustered,
	/// Uniformly scattered. The worst case, and what a hash join's probe order
	/// produces when the build side is not sorted by the join key.
	Scattered,
}

impl Locality {
	fn label(self) -> &'static str {
		match self {
			Self::Sequential => "sequential",
			Self::Clustered => "clustered",
			Self::Scattered => "scattered",
		}
	}

	fn indices(self, count: usize) -> Vec<u32> {
		// A deterministic LCG: `Math.random` equivalents would make a reading
		// unreproducible, and this bench is a falsification rail.
		let mut state = 0x2545_F491_4F6C_DD1Du64;
		let mut next = move || {
			state ^= state << 13;
			state ^= state >> 7;
			state ^= state << 17;
			state
		};
		match self {
			Self::Sequential => (0..count).map(|row| (row % SOURCE_ROWS) as u32).collect(),
			Self::Clustered => {
				let mut out = Vec::with_capacity(count);
				while out.len() < count {
					let start = (next() as usize) % SOURCE_ROWS.saturating_sub(RUN).max(1);
					for offset in 0..RUN.min(count - out.len()) {
						out.push((start + offset) as u32);
					}
				}
				out
			}
			Self::Scattered => (0..count)
				.map(|_| ((next() as usize) % SOURCE_ROWS) as u32)
				.collect(),
		}
	}
}

fn schema() -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new("l_orderkey", DataType::Int64, false),
		Field::new("l_partkey", DataType::Int64, false),
		Field::new("l_quantity", DataType::Float64, false),
		Field::new("l_shipdate", DataType::Int32, false),
		Field::new("l_shipinstruct", DataType::Utf8, false),
	]))
}

fn source(schema: &SchemaRef) -> RecordBatch {
	RecordBatch::try_new(
		schema.clone(),
		vec![
			Arc::new(Int64Array::from_iter_values(
				(0..SOURCE_ROWS).map(|row| row as i64),
			)),
			Arc::new(Int64Array::from_iter_values(
				(0..SOURCE_ROWS).map(|row| (row % 200_000) as i64),
			)),
			Arc::new(Float64Array::from_iter_values(
				(0..SOURCE_ROWS).map(|row| (row % 50) as f64),
			)),
			Arc::new(Int32Array::from_iter_values(
				(0..SOURCE_ROWS).map(|row| 9_000 + (row % 2_500) as i32),
			)),
			Arc::new(StringArray::from_iter_values((0..SOURCE_ROWS).map(|row| {
				if row % 2 == 0 {
					"DELIVER IN PERSON"
				} else {
					"NONE"
				}
			}))),
		],
	)
	.expect("source batch")
}

fn median(mut runs: Vec<Duration>) -> Duration {
	runs.sort_unstable();
	runs[runs.len() / 2]
}

/// Time the index-array construction as `materialize_join_selection` does it:
/// through `Option<u32>`, producing a nullable array.
fn time_nullable_build(raw: &[u32]) -> Duration {
	median(
		(0..REPLICATES)
			.map(|_| {
				let started = Instant::now();
				let indices: Vec<Option<u32>> = raw.iter().map(|index| Some(*index)).collect();
				let array = UInt32Array::from(indices);
				let elapsed = started.elapsed();
				black_box(array);
				elapsed
			})
			.collect(),
	)
}

/// The same array built without the `Option` round trip.
fn time_plain_build(raw: &[u32]) -> Duration {
	median(
		(0..REPLICATES)
			.map(|_| {
				let started = Instant::now();
				let array = UInt32Array::from(raw.to_vec());
				let elapsed = started.elapsed();
				black_box(array);
				elapsed
			})
			.collect(),
	)
}

fn time_take(batch: &RecordBatch, indices: &UInt32Array) -> Duration {
	for column in batch.columns() {
		black_box(take(column, indices, None).expect("warm take"));
	}
	median(
		(0..REPLICATES)
			.map(|_| {
				let started = Instant::now();
				for column in batch.columns() {
					black_box(take(column, indices, None).expect("take"));
				}
				started.elapsed()
			})
			.collect(),
	)
}

fn main() {
	let schema = schema();
	let batch = source(&schema);
	println!(
		"take over a {SOURCE_ROWS}-row x {}-column source, {REPLICATES} replicates, median",
		schema.fields().len()
	);
	println!(
		"{:>12}  {:>10}  {:>12}  {:>12}  {:>12}  {:>12}",
		"locality", "out_rows", "take_ns/row", "nullbuild/row", "plainbuild/row", "null_penalty"
	);
	for locality in [
		Locality::Sequential,
		Locality::Clustered,
		Locality::Scattered,
	] {
		for count in SELECTIONS {
			let raw = locality.indices(count);
			assert_eq!(raw.len(), count, "index fixture must match the cardinality");
			let nullable = UInt32Array::from(raw.iter().map(|i| Some(*i)).collect::<Vec<_>>());
			let plain = UInt32Array::from(raw.clone());
			assert_eq!(nullable.null_count(), 0, "an inner selection has no nulls");

			let take_nullable = time_take(&batch, &nullable);
			let take_plain = time_take(&batch, &plain);
			let build_nullable = time_nullable_build(&raw);
			let build_plain = time_plain_build(&raw);

			let per = |d: Duration| d.as_nanos() as f64 / count as f64;
			println!(
				"{:>12}  {:>10}  {:>12.2}  {:>12.2}  {:>12.2}  {:>11.2}x",
				locality.label(),
				count,
				per(take_nullable),
				per(build_nullable),
				per(build_plain),
				take_nullable.as_nanos() as f64 / take_plain.as_nanos().max(1) as f64
			);
		}
	}
	println!(
		"\nnull_penalty = nullable-index take / non-null-index take. Near 1.00 means \
		 a specialized non-null gather buys nothing on the take itself."
	);
}
