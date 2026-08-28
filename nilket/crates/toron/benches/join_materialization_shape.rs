//! What batch-count shape costs at a join's `concat_or_empty`.
//!
//! ADR 554 named 7,146 ms of input concatenation and 6,379 ms of output
//! gathering board-wide. The obvious cut — a single-batch fast path in
//! `concat_or_empty` — is already dead: `arrow_select::concat` short-circuits
//! `arrays.len() == 1` to `array.slice(0, len)`, a zero-copy view, so a
//! one-batch input already costs O(columns) and not O(rows).
//!
//! What that leaves open is whether the measured cost is the per-row copy (which
//! only eliminating the materialization can remove) or per-batch overhead (which
//! upstream chunking could reduce). This bench separates them: total rows are
//! held FIXED while batch count varies, so any difference across rows of the
//! table below is per-batch overhead and the floor is the per-row copy.
//!
//! Column mix approximates a TPC-H join input — integer keys, a decimal-like
//! float, a short string, a date — because concat cost is per column and a
//! single-Int64 fixture would price the cheapest possible case.

use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Float64Array, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

const TOTAL_ROWS: usize = 4_000_000;
const BATCH_COUNTS: [usize; 6] = [1, 8, 64, 512, 4_096, 32_768];
const REPLICATES: usize = 9;

fn schema() -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new("l_orderkey", DataType::Int64, false),
		Field::new("l_partkey", DataType::Int64, false),
		Field::new("l_quantity", DataType::Float64, false),
		Field::new("l_shipdate", DataType::Int32, false),
		Field::new("l_shipinstruct", DataType::Utf8, false),
	]))
}

fn batches(schema: &SchemaRef, batch_count: usize) -> Vec<RecordBatch> {
	let per_batch = TOTAL_ROWS.div_ceil(batch_count);
	let mut out = Vec::with_capacity(batch_count);
	let mut produced = 0usize;
	while produced < TOTAL_ROWS {
		let rows = per_batch.min(TOTAL_ROWS - produced);
		let base = produced as i64;
		out.push(
			RecordBatch::try_new(
				schema.clone(),
				vec![
					Arc::new(Int64Array::from_iter_values(
						(0..rows).map(|row| base + row as i64),
					)),
					Arc::new(Int64Array::from_iter_values(
						(0..rows).map(|row| (base + row as i64) % 200_000),
					)),
					Arc::new(Float64Array::from_iter_values(
						(0..rows).map(|row| (row % 50) as f64),
					)),
					Arc::new(Int32Array::from_iter_values(
						(0..rows).map(|row| 9_000 + (row % 2_500) as i32),
					)),
					Arc::new(StringArray::from_iter_values((0..rows).map(|row| {
						if row % 2 == 0 {
							"DELIVER IN PERSON"
						} else {
							"NONE"
						}
					}))),
				],
			)
			.expect("fixture batch"),
		);
		produced += rows;
	}
	out
}

/// Median of `REPLICATES` timed passes. One pass is not a measurement: the first
/// reading of this bench had the 32k-batch case appearing FASTER than the 8-batch
/// case, which is noise, not a curve.
fn time(schema: &SchemaRef, input: &[RecordBatch]) -> Duration {
	black_box(concat_batches(schema, input).expect("warm concat"));
	let mut runs = Vec::with_capacity(REPLICATES);
	for _ in 0..REPLICATES {
		let started = Instant::now();
		let out = concat_batches(schema, input).expect("concat");
		let elapsed = started.elapsed();
		assert_eq!(out.num_rows(), TOTAL_ROWS);
		black_box(out);
		runs.push(elapsed);
	}
	runs.sort_unstable();
	runs[REPLICATES / 2]
}

fn main() {
	let schema = schema();
	println!(
		"concat_batches over {TOTAL_ROWS} rows x {} columns",
		schema.fields().len()
	);
	println!(
		"{:>10}  {:>12}  {:>12}  {:>10}  {:>12}  {:>13}",
		"batches", "rows/batch", "median_ns", "ns/row", "total_rows", "input_bytes"
	);
	let mut single_batch_ns_per_row = 0.0_f64;
	for batch_count in BATCH_COUNTS {
		let input = batches(&schema, batch_count);
		// Equal work is asserted, not assumed: if a configuration carried fewer rows
		// or fewer bytes, every ns/row above would be comparing different fixtures.
		let rows: usize = input.iter().map(RecordBatch::num_rows).sum();
		let bytes: usize = input
			.iter()
			.map(|batch| batch.get_array_memory_size())
			.sum();
		assert_eq!(
			rows, TOTAL_ROWS,
			"every configuration must carry the same rows"
		);
		let elapsed = time(&schema, &input);
		let ns_per_row = elapsed.as_nanos() as f64 / TOTAL_ROWS as f64;
		if batch_count == 1 {
			single_batch_ns_per_row = ns_per_row;
		}
		println!(
			"{:>10}  {:>12}  {:>12}  {:>10.2}  {:>12}  {:>13}",
			input.len(),
			TOTAL_ROWS.div_ceil(batch_count),
			elapsed.as_nanos(),
			ns_per_row,
			rows,
			bytes
		);
	}
	println!(
		"\nsingle-batch floor {single_batch_ns_per_row:.2} ns/row — the zero-copy \
		 short-circuit in arrow_select::concat. Anything above it is real copying."
	);
}
