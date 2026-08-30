//! What a join's probe pipeline costs with and without concatenating its input.
//!
//! ADR 580 measured 6,424.8 ms of board time inside `concat_batches` on the probe
//! side, and authorized ONE design to be priced before anything is built:
//! probing the 4-7 probe batches one at a time, with batch-local row indices,
//! instead of concatenating them first. `execute_hash_join` already returns
//! `Vec<RecordBatch>` and `probe_in_memory_join_rows` already takes one probe
//! batch, so that design needs no new row addressing.
//!
//! **This prices the whole path, not a component.** The last forecast this line
//! took from a component bench was falsified in direction by the engine, so both
//! arms run end to end — concat, key derivation, probe, materialize — and the
//! only structural difference between them is where the batch boundary sits.
//!
//! **What it is and is not.** The engine's join internals are `pub(super)`, so
//! this is a faithful model of the pipeline rather than a call into it: the same
//! build table, the same probe order, the same output rows, the same whole-source
//! slice rule the engine applies. Absolute milliseconds are therefore model
//! numbers. The DELTA between two arms that share every component and differ only
//! in batch structure is the decision input, and that is what the table reports.
//!
//! **Shapes are per INVOCATION, not per task.** ADR 580's hot node runs four
//! concatenations of four batches, not one of sixteen; a bench built on the task
//! total would measure a shape the engine never runs. Rows per invocation are
//! assumed even across a node's invocations — the telemetry keeps invocation
//! counts and batch histograms but only aggregate rows, so that evenness is an
//! assumption, stated here and not measured.

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::HashMap;
use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use arrow::array::{
	Array, ArrayRef, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray, UInt32Array,
};
use arrow::compute::{concat_batches, take};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

/// Counts live bytes and the high-water mark, resettable between arms.
///
/// A process high-water mark cannot be reused across arms — the first arm's peak
/// would mask the second's — so the peak is reset to the current live figure
/// before each arm and read as a delta over it.
struct Counting;

static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
	unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
		let pointer = unsafe { System.alloc(layout) };
		if !pointer.is_null() {
			let live = LIVE.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
			PEAK.fetch_max(live, Ordering::Relaxed);
		}
		pointer
	}

	unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
		LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
		unsafe { System.dealloc(pointer, layout) };
	}
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

fn reset_peak() -> usize {
	let live = LIVE.load(Ordering::Relaxed);
	PEAK.store(live, Ordering::Relaxed);
	live
}

fn peak_delta(baseline: usize) -> usize {
	PEAK.load(Ordering::Relaxed).saturating_sub(baseline)
}

/// One probe-side invocation, as ADR 580's artifact records them.
struct Shape {
	label: &'static str,
	batches: usize,
	rows_per_batch: usize,
}

/// Per-invocation shapes from `adr580_input_shape_sf8.jsonl`, pair 0:
/// q09 s0/t0 is 4 concats x 4 batches x ~7.15 M rows; q08 6 x 4 x ~4.2 M;
/// q17 1 x 4 x ~12 M.
const SHAPES: [Shape; 3] = [
	Shape {
		label: "q08-like",
		batches: 4,
		rows_per_batch: 4_200_000,
	},
	Shape {
		label: "q09-like",
		batches: 4,
		rows_per_batch: 7_150_000,
	},
	Shape {
		label: "q17-like",
		batches: 4,
		rows_per_batch: 12_000_000,
	},
];

const BUILD_ROWS: usize = 200_000;
const REPLICATES: usize = 3;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
	/// Batch-local throughout: no state crosses a probe batch.
	Inner,
	/// Join-wide state: unmatched build rows are emitted after the last probe
	/// batch, so the matched-build bitmap must outlive each batch. Priced because
	/// arguing that hoist from the code is not the same as measuring it.
	LeftOuter,
}

impl Kind {
	fn label(self) -> &'static str {
		match self {
			Kind::Inner => "inner",
			Kind::LeftOuter => "leftouter",
		}
	}
}

#[derive(Clone, Copy)]
enum Row {
	Paired { left: u32, right: u32 },
	LeftFill { left: u32 },
}

fn probe_schema() -> SchemaRef {
	// A TPC-H-ish probe row: two integer keys, a decimal-like float, a short
	// string, a date. Concat and take are both per column, so a single-Int64
	// fixture would price the cheapest possible case.
	Arc::new(Schema::new(vec![
		Field::new("p_key", DataType::Int64, false),
		Field::new("p_id", DataType::Int64, false),
		Field::new("p_amount", DataType::Float64, false),
		Field::new("p_tag", DataType::Utf8, false),
		Field::new("p_date", DataType::Int32, false),
	]))
}

fn build_schema() -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new("b_key", DataType::Int64, false),
		Field::new("b_name", DataType::Utf8, false),
	]))
}

fn probe_batch(schema: &SchemaRef, rows: usize, offset: usize) -> RecordBatch {
	let keys: Vec<i64> = (0..rows)
		.map(|row| ((row + offset) % BUILD_ROWS) as i64)
		.collect();
	RecordBatch::try_new(
		schema.clone(),
		vec![
			Arc::new(Int64Array::from(keys)),
			Arc::new(Int64Array::from_iter_values(
				(0..rows).map(|row| (row + offset) as i64),
			)),
			Arc::new(Float64Array::from_iter_values(
				(0..rows).map(|row| row as f64 * 1.5),
			)),
			Arc::new(StringArray::from_iter_values(
				(0..rows).map(|row| format!("t{:07}", row % 1_000_000)),
			)),
			Arc::new(Int32Array::from_iter_values(
				(0..rows).map(|row| (row % 2_557) as i32),
			)),
		],
	)
	.expect("probe batch")
}

fn build_batch(schema: &SchemaRef) -> RecordBatch {
	RecordBatch::try_new(
		schema.clone(),
		vec![
			Arc::new(Int64Array::from_iter_values(
				(0..BUILD_ROWS).map(|row| row as i64),
			)),
			Arc::new(StringArray::from_iter_values(
				(0..BUILD_ROWS).map(|row| format!("b{row:07}")),
			)),
		],
	)
	.expect("build batch")
}

/// The build table both arms share. Built once, outside every timed region: this
/// bench prices the probe side, and ADR 580 did not authorize the build side.
fn build_table(batch: &RecordBatch) -> HashMap<i64, Vec<u32>> {
	let keys = batch
		.column(0)
		.as_any()
		.downcast_ref::<Int64Array>()
		.expect("build keys");
	let mut table: HashMap<i64, Vec<u32>> = HashMap::with_capacity(BUILD_ROWS);
	for row in 0..keys.len() {
		table.entry(keys.value(row)).or_default().push(row as u32);
	}
	table
}

fn derive_keys(batch: &RecordBatch) -> ArrayRef {
	// The engine derives key arrays per probe batch through `key_arrays`; both
	// arms pay this, the difference being how many times.
	batch.column(0).clone()
}

fn probe(
	table: &HashMap<i64, Vec<u32>>,
	keys: &ArrayRef,
	matched: Option<&mut [u64]>,
	selection: &mut Vec<Row>,
) {
	let keys = keys
		.as_any()
		.downcast_ref::<Int64Array>()
		.expect("probe keys");
	let mut matched = matched;
	for row in 0..keys.len() {
		if let Some(build_rows) = table.get(&keys.value(row)) {
			for build in build_rows {
				if let Some(bitmap) = matched.as_deref_mut() {
					bitmap[*build as usize / 64] |= 1 << (*build as usize % 64);
				}
				selection.push(Row::Paired {
					left: *build,
					right: row as u32,
				});
			}
		}
	}
}

/// Gather one side, preferring the whole-source slice the engine now takes.
fn gather(batch: &RecordBatch, indices: &[Option<u32>], columns: &mut Vec<ArrayRef>) {
	let whole_source = indices.len() == batch.num_rows()
		&& indices
			.iter()
			.enumerate()
			.all(|(position, index)| *index == Some(position as u32));
	if whole_source {
		for column in batch.columns() {
			columns.push(column.slice(0, indices.len()));
		}
		return;
	}
	let array = UInt32Array::from(indices.to_vec());
	for column in batch.columns() {
		columns.push(take(column, &array, None).expect("take"));
	}
}

fn materialize(
	build: &RecordBatch,
	probe_batch: &RecordBatch,
	selection: &[Row],
	schema: &SchemaRef,
) -> RecordBatch {
	let mut left = Vec::with_capacity(selection.len());
	let mut right = Vec::with_capacity(selection.len());
	for row in selection {
		match *row {
			Row::Paired { left: l, right: r } => {
				left.push(Some(l));
				right.push(Some(r));
			}
			Row::LeftFill { left: l } => {
				left.push(Some(l));
				right.push(None);
			}
		}
	}
	let mut columns = Vec::with_capacity(build.num_columns() + probe_batch.num_columns());
	gather(build, &left, &mut columns);
	gather(probe_batch, &right, &mut columns);
	RecordBatch::try_new(schema.clone(), columns).expect("output batch")
}

fn output_schema(build: &SchemaRef, probe: &SchemaRef) -> SchemaRef {
	let mut fields: Vec<Field> = build
		.fields()
		.iter()
		.map(|field| field.as_ref().clone())
		.collect();
	fields.extend(probe.fields().iter().map(|field| {
		// The probe side can be nulled by an unmatched build row.
		Field::new(field.name(), field.data_type().clone(), true)
	}));
	Arc::new(Schema::new(fields))
}

#[derive(Default, Clone, Copy)]
struct Phases {
	concat: Duration,
	keys: Duration,
	probe: Duration,
	gather: Duration,
	total: Duration,
	out_batches: usize,
	peak_delta: usize,
}

fn unmatched_build_rows(matched: &[u64], rows: usize, selection: &mut Vec<Row>) {
	for row in 0..rows {
		if matched[row / 64] & (1 << (row % 64)) == 0 {
			selection.push(Row::LeftFill { left: row as u32 });
		}
	}
}

/// Today's path: concatenate the probe batches, then run one key derivation, one
/// probe and one materialization over the whole thing.
fn current_arm(
	kind: Kind,
	build: &RecordBatch,
	table: &HashMap<i64, Vec<u32>>,
	batches: &[RecordBatch],
	probe_schema: &SchemaRef,
	out_schema: &SchemaRef,
) -> (Phases, Vec<RecordBatch>) {
	let baseline = reset_peak();
	let mut phases = Phases::default();
	let started = Instant::now();

	let concat_started = Instant::now();
	let probe_batch = concat_batches(probe_schema, batches).expect("concat");
	phases.concat = concat_started.elapsed();

	let keys_started = Instant::now();
	let keys = derive_keys(&probe_batch);
	phases.keys = keys_started.elapsed();

	let probe_started = Instant::now();
	let mut matched = (kind == Kind::LeftOuter).then(|| vec![0u64; build.num_rows().div_ceil(64)]);
	let mut selection = Vec::new();
	probe(table, &keys, matched.as_deref_mut(), &mut selection);
	if let Some(bitmap) = matched.as_deref() {
		unmatched_build_rows(bitmap, build.num_rows(), &mut selection);
	}
	phases.probe = probe_started.elapsed();

	let gather_started = Instant::now();
	let output = vec![materialize(build, &probe_batch, &selection, out_schema)];
	phases.gather = gather_started.elapsed();

	phases.total = started.elapsed();
	phases.out_batches = output.len();
	phases.peak_delta = peak_delta(baseline);
	(phases, output)
}

/// The candidate: no concatenation. Each probe batch derives its own keys,
/// probes, and materializes with batch-local rows; the matched-build bitmap
/// outlives the loop and its unmatched rows are emitted once, at the end.
///
/// `reuse_selection` is the third arm, and it is not a detail. True allocates
/// the selection vector once and clears it per batch, keeping its capacity;
/// false allocates a fresh vector per batch and regrows it from nothing — which
/// is what `probe_in_memory_join_rows` does today, because it owns and returns
/// its output vector.
///
/// The engine cut measured at `14e87160` was therefore the FALSE arm, while the
/// candidate this bench priced — the one that earned authorization — was the
/// TRUE one. The two were never separated, so the engine's +2,370.9 ms of probe
/// time has never been attributed to either the batch-local structure or the
/// allocation it accidentally introduced. This isolates them.
fn batch_local_arm(
	kind: Kind,
	build: &RecordBatch,
	table: &HashMap<i64, Vec<u32>>,
	batches: &[RecordBatch],
	out_schema: &SchemaRef,
	reuse_selection: bool,
) -> (Phases, Vec<RecordBatch>) {
	let baseline = reset_peak();
	let mut phases = Phases::default();
	let started = Instant::now();

	let mut matched = (kind == Kind::LeftOuter).then(|| vec![0u64; build.num_rows().div_ceil(64)]);
	let mut output = Vec::with_capacity(batches.len() + 1);
	let mut selection = Vec::new();

	for batch in batches {
		let keys_started = Instant::now();
		let keys = derive_keys(batch);
		phases.keys += keys_started.elapsed();

		let probe_started = Instant::now();
		if reuse_selection {
			selection.clear();
		} else {
			// A fresh vector per batch — the allocation and regrowth the engine
			// pays, because its probe function creates the `Vec` it returns.
			selection = Vec::new();
		}
		probe(table, &keys, matched.as_deref_mut(), &mut selection);
		phases.probe += probe_started.elapsed();

		let gather_started = Instant::now();
		output.push(materialize(build, batch, &selection, out_schema));
		phases.gather += gather_started.elapsed();
	}

	// The join-wide tail: unmatched build rows belong to no probe batch, so they
	// are emitted after the last one, against an empty probe side.
	if let Some(bitmap) = matched.as_deref() {
		let probe_started = Instant::now();
		selection.clear();
		unmatched_build_rows(bitmap, build.num_rows(), &mut selection);
		phases.probe += probe_started.elapsed();

		if !selection.is_empty() {
			let gather_started = Instant::now();
			let empty = RecordBatch::new_empty(batches[0].schema());
			output.push(materialize(build, &empty, &selection, out_schema));
			phases.gather += gather_started.elapsed();
		}
	}

	phases.total = started.elapsed();
	phases.out_batches = output.len();
	phases.peak_delta = peak_delta(baseline);
	(phases, output)
}

/// Both arms must produce the same rows in the same order. Checked OUTSIDE every
/// timed region, on the first replicate only: a wrong batch boundary returns the
/// right row count with the wrong rows, and no timing would show it.
fn assert_same_rows(current: &[RecordBatch], candidate: &[RecordBatch], schema: &SchemaRef) {
	let current = concat_batches(schema, current).expect("concat current");
	let candidate = concat_batches(schema, candidate).expect("concat candidate");
	assert_eq!(
		current.num_rows(),
		candidate.num_rows(),
		"row counts differ between arms"
	);
	for column in 0..current.num_columns() {
		assert_eq!(
			current.column(column).to_data(),
			candidate.column(column).to_data(),
			"column {column} differs between arms"
		);
	}
}

fn median(mut runs: Vec<Phases>) -> Phases {
	runs.sort_by_key(|phases| phases.total);
	runs[runs.len() / 2]
}

fn main() {
	let probe_schema = probe_schema();
	let build_schema = build_schema();
	let out_schema = output_schema(&build_schema, &probe_schema);
	let build = build_batch(&build_schema);
	let table = build_table(&build);

	println!(
		"probe pipeline with and without input concatenation; build {BUILD_ROWS} rows, \
		 {REPLICATES} interleaved replicates, median by total"
	);
	println!(
		"{:>10}  {:>10}  {:>9}  {:>8}  {:>8}  {:>8}  {:>9}  {:>8}  {:>7}  {:>9}",
		"shape", "kind", "arm", "concat", "keys", "probe", "gather", "total", "out", "peak_MB"
	);

	for shape in SHAPES {
		let batches: Vec<RecordBatch> = (0..shape.batches)
			.map(|batch| probe_batch(&probe_schema, shape.rows_per_batch, batch * 7))
			.collect();

		for kind in [Kind::Inner, Kind::LeftOuter] {
			let mut current_runs = Vec::with_capacity(REPLICATES);
			let mut reused_runs = Vec::with_capacity(REPLICATES);
			let mut fresh_runs = Vec::with_capacity(REPLICATES);

			for replicate in 0..REPLICATES {
				// Interleaved, not batched by arm: a machine that drifts during the
				// run drifts through all three arms rather than through one.
				let (current, current_output) =
					current_arm(kind, &build, &table, &batches, &probe_schema, &out_schema);
				let (reused, reused_output) =
					batch_local_arm(kind, &build, &table, &batches, &out_schema, true);
				let (fresh, fresh_output) =
					batch_local_arm(kind, &build, &table, &batches, &out_schema, false);

				if replicate == 0 {
					assert_same_rows(&current_output, &reused_output, &out_schema);
					// The allocation strategy must not change a single row: if it
					// did, the two arms would be measuring different work and the
					// comparison would mean nothing.
					assert_same_rows(&current_output, &fresh_output, &out_schema);
				}
				black_box(&current_output);
				black_box(&reused_output);
				black_box(&fresh_output);
				current_runs.push(current);
				reused_runs.push(reused);
				fresh_runs.push(fresh);
			}

			for (arm, phases) in [
				("current", median(current_runs)),
				("reused-vec", median(reused_runs)),
				("fresh-vec", median(fresh_runs)),
			] {
				let ms = |duration: Duration| duration.as_secs_f64() * 1000.0;
				println!(
					"{:>10}  {:>10}  {:>9}  {:>8.1}  {:>8.1}  {:>8.1}  {:>9.1}  {:>8.1}  \
					 {:>7}  {:>9.0}",
					shape.label,
					kind.label(),
					arm,
					ms(phases.concat),
					ms(phases.keys),
					ms(phases.probe),
					ms(phases.gather),
					ms(phases.total),
					phases.out_batches,
					phases.peak_delta as f64 / (1024.0 * 1024.0)
				);
			}
		}
	}

	println!(
		"\nAbsolute times are model numbers: the engine's join internals are private, so \
		 both arms share a faithful model of the path rather than calling it. The DELTA is \
		 the decision input. peak_MB is the allocation high-water mark above each arm's own \
		 entry point, reset per arm."
	);
}
