//! Where the post-ADR-0089 scan spends its time.
//!
//! ADR 0089 coalesced the physical read and the decode across the rows of one
//! packed record: a 13,000-row scan now decodes four records instead of
//! 13,000. What it did not touch is the tail — everything the descriptor-backed
//! whole-record path does once per *index entry* after the decode is served
//! from the cache. That tail slivers each row back out of the shared record
//! into its own one-row `RecordBatch`, rebuilding a schema per row, and runs
//! the PBC program on that one row.
//!
//! This bench prices the tail against the same work done once per record. It
//! is a lower bound on the real per-row cost: `resolve_record_placement` and
//! `table_record_matches_descriptor` live behind the data server's private
//! surface and are not called here.

use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{
	BooleanArray, FixedSizeBinaryBuilder, Int64Array, RecordBatch, StringArray, UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Schema};
use wendelon::schema::pbc::{
	OP_GTE, OP_PROJECT, OP_PUSH_COL, OP_PUSH_INT, Program, execute_program,
};
use wendelon::schema::{
	RecordRowIndex, STORAGE_ROW_HLC_COLUMN, STORAGE_ROW_KEY_ID_COLUMN,
	STORAGE_ROW_ROUTING_LOCALITY_COLUMN, STORAGE_ROW_TABLE_OBJECT_ID_COLUMN,
	STORAGE_ROW_TOMBSTONE_COLUMN,
};
use wendelon::storage::multirow::{
	has_storage_synthetic_columns, materialize_record_location_batch,
	storage_row_table_object_id_at, strip_storage_synthetic_columns,
};

struct BenchCase {
	label: &'static str,
	rows: usize,
	iterations: usize,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
	// 4,096 is the packing target ADR 0089 writes to, so it is the record
	// shape a packed table's scan actually meets. The smaller rungs show how
	// the amortization scales with rows-per-record.
	let cases = [
		BenchCase {
			label: "record-256",
			rows: 256,
			iterations: 256,
		},
		BenchCase {
			label: "record-1024",
			rows: 1_024,
			iterations: 128,
		},
		BenchCase {
			label: "record-4096",
			rows: 4_096,
			iterations: 64,
		},
	];

	let program = Program::compile(&program_bytecode())?;

	for case in cases {
		let record = storage_record(case.rows)?;
		// Premise pin: the fixture really is a packed multi-row storage
		// record. Without the synthetic columns the tail's strip and probe
		// work all short-circuits and the comparison measures nothing.
		assert!(
			has_storage_synthetic_columns(&record),
			"fixture must carry storage synthetic columns"
		);
		assert_eq!(record.num_rows(), case.rows);

		let per_row = bench_per_row_tail(&program, &record, case.iterations)?;
		let per_record = bench_per_record_tail(&program, &record, case.iterations)?;
		print_measurement(case.label, "per-row-tail", case.rows, per_row);
		print_measurement(case.label, "per-record-tail", case.rows, per_record);
		println!(
			"record_scan_tail case={} rows={} amortization={:.2}x",
			case.label,
			case.rows,
			per_row.as_secs_f64() / per_record.as_secs_f64()
		);

		// The phases of the per-row tail, so the cost has a named owner
		// rather than a total.
		let probe = bench_phase(
			case.iterations,
			case.rows,
			|record, index| {
				black_box(has_storage_synthetic_columns(record));
				black_box(storage_row_table_object_id_at(record, index)?);
				Ok(())
			},
			&record,
		)?;
		let materialize = bench_phase(
			case.iterations,
			case.rows,
			|record, index| {
				black_box(materialize_record_location_batch(record, index)?);
				Ok(())
			},
			&record,
		)?;
		print_measurement(case.label, "phase-synthetic-probes", case.rows, probe);
		print_measurement(case.label, "phase-materialize", case.rows, materialize);
	}

	Ok(())
}

/// The descriptor-backed whole-record path as it stands: one index entry at a
/// time, each row slivered into its own batch and run through the program.
fn bench_per_row_tail(
	program: &Program,
	record: &RecordBatch,
	iterations: usize,
) -> std::io::Result<Duration> {
	let started = Instant::now();
	for _ in 0..iterations {
		for row in 0..record.num_rows() {
			let index = RecordRowIndex::new(row as u32);
			black_box(has_storage_synthetic_columns(record));
			black_box(storage_row_table_object_id_at(record, index)?);
			let batch = materialize_record_location_batch(record, index)?;
			black_box(execute_program(program, &batch)?);
		}
	}
	Ok(started.elapsed() / iterations as u32)
}

/// The same result computed once for the whole record: strip the synthetic
/// columns once, run the program once over every row.
fn bench_per_record_tail(
	program: &Program,
	record: &RecordBatch,
	iterations: usize,
) -> std::io::Result<Duration> {
	let started = Instant::now();
	for _ in 0..iterations {
		black_box(has_storage_synthetic_columns(record));
		let batch = strip_storage_synthetic_columns(record)?;
		black_box(execute_program(program, &batch)?);
	}
	Ok(started.elapsed() / iterations as u32)
}

fn bench_phase(
	iterations: usize,
	rows: usize,
	mut phase: impl FnMut(&RecordBatch, RecordRowIndex) -> std::io::Result<()>,
	record: &RecordBatch,
) -> std::io::Result<Duration> {
	let started = Instant::now();
	for _ in 0..iterations {
		for row in 0..rows {
			phase(record, RecordRowIndex::new(row as u32))?;
		}
	}
	Ok(started.elapsed() / iterations as u32)
}

/// A packed append-only storage record: user columns plus the full synthetic
/// set the scan tail probes and strips, including ADR 045 Phase 8's optional
/// table-object-id column that append-only records carry.
fn storage_record(rows: usize) -> std::io::Result<RecordBatch> {
	let schema = Arc::new(Schema::new(vec![
		Field::new("x", DataType::Int64, false),
		Field::new("label", DataType::Utf8, false),
		Field::new(STORAGE_ROW_HLC_COLUMN, DataType::FixedSizeBinary(16), false),
		Field::new(
			STORAGE_ROW_KEY_ID_COLUMN,
			DataType::FixedSizeBinary(32),
			false,
		),
		Field::new(STORAGE_ROW_ROUTING_LOCALITY_COLUMN, DataType::UInt64, false),
		Field::new(
			STORAGE_ROW_TABLE_OBJECT_ID_COLUMN,
			DataType::FixedSizeBinary(16),
			false,
		),
		Field::new(STORAGE_ROW_TOMBSTONE_COLUMN, DataType::Boolean, false),
	]));

	let x = Int64Array::from((0..rows).map(|i| i as i64).collect::<Vec<_>>());
	let label = StringArray::from((0..rows).map(|i| format!("row-{i:08}")).collect::<Vec<_>>());
	let mut hlc = FixedSizeBinaryBuilder::with_capacity(rows, 16);
	let mut key_id = FixedSizeBinaryBuilder::with_capacity(rows, 32);
	let mut object_id = FixedSizeBinaryBuilder::with_capacity(rows, 16);
	let mut locality = UInt64Builder::with_capacity(rows);
	for i in 0..rows {
		hlc.append_value((i as u128).to_be_bytes())
			.map_err(arrow_err)?;
		let mut key = [0u8; 32];
		key[..8].copy_from_slice(&(i as u64).to_be_bytes());
		key_id.append_value(key).map_err(arrow_err)?;
		object_id.append_value([7u8; 16]).map_err(arrow_err)?;
		locality.append_value(i as u64);
	}
	let tombstone = BooleanArray::from(vec![false; rows]);

	RecordBatch::try_new(
		schema,
		vec![
			Arc::new(x),
			Arc::new(label),
			Arc::new(hlc.finish()),
			Arc::new(key_id.finish()),
			Arc::new(locality.finish()),
			Arc::new(object_id.finish()),
			Arc::new(tombstone),
		],
	)
	.map_err(arrow_err)
}

/// `SELECT x, label WHERE x >= 0` — every row survives, so the tail does the
/// full amount of work a scan of the record does.
fn program_bytecode() -> Vec<u8> {
	let mut out = Vec::new();
	// Columns are addressed by index into the materialized batch, whose
	// synthetic columns are already stripped: 0 = x, 1 = label.
	out.push(OP_PUSH_COL);
	out.extend_from_slice(&0u32.to_le_bytes());
	out.push(OP_PUSH_INT);
	out.extend_from_slice(&0i64.to_le_bytes());
	out.push(OP_GTE);
	out.push(OP_PROJECT);
	out.extend_from_slice(&2u16.to_le_bytes());
	out.extend_from_slice(&0u32.to_le_bytes());
	out.extend_from_slice(&1u32.to_le_bytes());
	out
}

fn arrow_err(err: impl std::fmt::Display) -> std::io::Error {
	std::io::Error::new(std::io::ErrorKind::InvalidData, err.to_string())
}

fn print_measurement(label: &str, mode: &str, rows: usize, avg: Duration) {
	let per_row_ns = avg.as_nanos() as f64 / rows as f64;
	let rows_per_second = rows as f64 / avg.as_secs_f64();
	println!(
		"record_scan_tail case={label} mode={mode} rows={rows} avg_ns={} per_row_ns={per_row_ns:.1} rows_per_second={rows_per_second:.0}",
		avg.as_nanos(),
	);
}
