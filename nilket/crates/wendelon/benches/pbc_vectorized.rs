use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Array, BooleanArray, Int64Array, StringArray};
use arrow::compute;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use wendelon::schema::pbc::{
	OP_AND, OP_EQ, OP_GTE, OP_IN_LIST, OP_PROJECT, OP_PUSH_COL, OP_PUSH_INT, OP_PUSH_STR, Program,
	execute_program,
};

#[derive(Clone, Copy)]
enum Workload {
	IntGte,
	StringEq,
	Combined,
	ProjectionOnly,
	AllFalse,
}

struct BenchCase {
	label: &'static str,
	workload: Workload,
	rows: usize,
	iterations: usize,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
	let cases = [
		BenchCase {
			label: "int-gte-1k",
			workload: Workload::IntGte,
			rows: 1_024,
			iterations: 128,
		},
		BenchCase {
			label: "string-eq-64k",
			workload: Workload::StringEq,
			rows: 64 * 1024,
			iterations: 32,
		},
		BenchCase {
			label: "combined-64k",
			workload: Workload::Combined,
			rows: 64 * 1024,
			iterations: 32,
		},
		BenchCase {
			label: "projection-only-64k",
			workload: Workload::ProjectionOnly,
			rows: 64 * 1024,
			iterations: 64,
		},
		BenchCase {
			label: "all-false-64k",
			workload: Workload::AllFalse,
			rows: 64 * 1024,
			iterations: 32,
		},
		BenchCase {
			label: "combined-1m",
			workload: Workload::Combined,
			rows: 1_000_000,
			iterations: 8,
		},
	];

	for case in cases {
		let batch = bench_batch(case.rows)?;
		let bytecode = bytecode_for(case.workload);
		let program = Program::compile(&bytecode)?;
		let vector = bench_vectorized(&program, &batch, case.iterations)?;
		let row_loop = bench_scalar_interpreter(&batch, case.workload, case.iterations)?;
		print_measurement(case.label, "vectorized", case.rows, vector);
		print_measurement(
			case.label,
			"scalar-interpreter-baseline",
			case.rows,
			row_loop,
		);
		println!(
			"pbc_vectorized case={} rows={} speedup_vs_scalar_interpreter={:.2}",
			case.label,
			case.rows,
			row_loop.as_secs_f64() / vector.as_secs_f64()
		);
	}

	Ok(())
}

fn bench_batch(rows: usize) -> std::io::Result<RecordBatch> {
	let schema = Arc::new(Schema::new(vec![
		Field::new("name", DataType::Utf8, false),
		Field::new("age", DataType::Int64, true),
		Field::new("status", DataType::Utf8, true),
	]));
	let names = (0..rows)
		.map(|idx| format!("user-{idx:08}"))
		.collect::<Vec<_>>();
	let ages = (0..rows)
		.map(|idx| {
			if idx % 97 == 0 {
				None
			} else {
				Some((idx % 83) as i64)
			}
		})
		.collect::<Vec<_>>();
	let statuses = (0..rows)
		.map(|idx| match idx % 5 {
			0 => Some("active"),
			1 => Some("pending"),
			2 => Some("inactive"),
			3 => Some("archived"),
			_ => None,
		})
		.collect::<Vec<_>>();
	RecordBatch::try_new(
		schema,
		vec![
			Arc::new(StringArray::from(names)),
			Arc::new(Int64Array::from(ages)),
			Arc::new(StringArray::from(statuses)),
		],
	)
	.map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

fn bytecode_for(workload: Workload) -> Vec<u8> {
	match workload {
		Workload::IntGte => assemble(&[Instr::PushCol(1), Instr::PushInt(18), Instr::Raw(OP_GTE)]),
		Workload::StringEq => assemble(&[
			Instr::PushCol(2),
			Instr::PushStr("active"),
			Instr::Raw(OP_EQ),
		]),
		Workload::Combined => assemble(&[
			Instr::PushCol(1),
			Instr::PushInt(18),
			Instr::Raw(OP_GTE),
			Instr::PushCol(2),
			Instr::PushStr("active"),
			Instr::PushStr("pending"),
			Instr::InList(2),
			Instr::Raw(OP_AND),
			Instr::Project(&[0, 1]),
		]),
		Workload::ProjectionOnly => assemble(&[Instr::Project(&[0, 1])]),
		Workload::AllFalse => assemble(&[
			Instr::PushCol(1),
			Instr::PushInt(i64::MAX),
			Instr::Raw(OP_GTE),
		]),
	}
}

fn bench_vectorized(
	program: &Program,
	batch: &RecordBatch,
	iterations: usize,
) -> std::io::Result<Duration> {
	let _ = execute_program(program, batch)?;
	let mut elapsed = Duration::ZERO;
	for _ in 0..iterations {
		let started = Instant::now();
		let out = execute_program(black_box(program), black_box(batch))?;
		elapsed += started.elapsed();
		black_box(out.num_rows());
	}
	Ok(elapsed / iterations as u32)
}

fn bench_scalar_interpreter(
	batch: &RecordBatch,
	workload: Workload,
	iterations: usize,
) -> std::io::Result<Duration> {
	let _ = execute_scalar_interpreter_baseline(batch, workload)?;
	let mut elapsed = Duration::ZERO;
	for _ in 0..iterations {
		let started = Instant::now();
		let out = execute_scalar_interpreter_baseline(black_box(batch), workload)?;
		elapsed += started.elapsed();
		black_box(out.num_rows());
	}
	Ok(elapsed / iterations as u32)
}

fn execute_scalar_interpreter_baseline(
	batch: &RecordBatch,
	workload: Workload,
) -> std::io::Result<RecordBatch> {
	let filtered = match workload {
		Workload::ProjectionOnly => batch.clone(),
		_ => {
			let mask = scalar_interpreter_mask(batch, workload)?;
			compute::filter_record_batch(batch, &mask)
				.map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?
		}
	};
	match workload {
		Workload::Combined | Workload::ProjectionOnly => filtered
			.project(&[0, 1])
			.map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e)),
		_ => Ok(filtered),
	}
}

fn scalar_interpreter_mask(
	batch: &RecordBatch,
	workload: Workload,
) -> std::io::Result<BooleanArray> {
	let ops = scalar_ops_for(workload);
	let mut mask = Vec::with_capacity(batch.num_rows());
	for row in 0..batch.num_rows() {
		mask.push(eval_scalar_row(&ops, batch, row));
	}
	Ok(BooleanArray::from(mask))
}

fn scalar_ops_for(workload: Workload) -> Vec<BenchOp> {
	match workload {
		Workload::IntGte => vec![BenchOp::PushCol(1), BenchOp::PushInt(18), BenchOp::Gte],
		Workload::StringEq => vec![BenchOp::PushCol(2), BenchOp::PushStr("active"), BenchOp::Eq],
		Workload::Combined => vec![
			BenchOp::PushCol(1),
			BenchOp::PushInt(18),
			BenchOp::Gte,
			BenchOp::PushCol(2),
			BenchOp::PushStr("active"),
			BenchOp::PushStr("pending"),
			BenchOp::InList(2),
			BenchOp::And,
		],
		Workload::AllFalse => vec![
			BenchOp::PushCol(1),
			BenchOp::PushInt(i64::MAX),
			BenchOp::Gte,
		],
		Workload::ProjectionOnly => Vec::new(),
	}
}

#[derive(Clone, Copy)]
enum BenchOp {
	PushCol(usize),
	PushInt(i64),
	PushStr(&'static str),
	Eq,
	Gte,
	And,
	InList(usize),
}

#[derive(Clone, Copy)]
enum BenchValue<'a> {
	Null,
	Int(i64),
	Str(&'a str),
	Bool(bool),
}

fn eval_scalar_row(ops: &[BenchOp], batch: &RecordBatch, row: usize) -> bool {
	let mut stack = Vec::with_capacity(16);
	let mut fuel = 1024usize;
	for op in ops {
		if fuel == 0 {
			return false;
		}
		fuel -= 1;
		match *op {
			BenchOp::PushCol(col) => {
				let Some(value) = read_bench_column(batch, col, row) else {
					return false;
				};
				stack.push(value);
			}
			BenchOp::PushInt(value) => stack.push(BenchValue::Int(value)),
			BenchOp::PushStr(value) => stack.push(BenchValue::Str(value)),
			BenchOp::Eq | BenchOp::Gte => {
				let Some(rhs) = stack.pop() else {
					return false;
				};
				let Some(lhs) = stack.pop() else {
					return false;
				};
				match compare_bench_values(lhs, rhs, *op) {
					Some(value) => stack.push(value),
					None => return false,
				}
			}
			BenchOp::And => {
				let Some(rhs) = stack.pop() else {
					return false;
				};
				let Some(lhs) = stack.pop() else {
					return false;
				};
				match (lhs, rhs) {
					(BenchValue::Bool(x), BenchValue::Bool(y)) => {
						stack.push(BenchValue::Bool(x && y))
					}
					(BenchValue::Null, BenchValue::Bool(false))
					| (BenchValue::Bool(false), BenchValue::Null) => {
						stack.push(BenchValue::Bool(false));
					}
					(BenchValue::Null, _) | (_, BenchValue::Null) => stack.push(BenchValue::Null),
					_ => return false,
				}
			}
			BenchOp::InList(count) => {
				if stack.len() < count + 1 {
					return false;
				}
				let split = stack.len() - count;
				let items = stack.drain(split..).collect::<Vec<_>>();
				let Some(target) = stack.pop() else {
					return false;
				};
				if matches!(target, BenchValue::Null) {
					stack.push(BenchValue::Null);
					continue;
				}
				let mut found = false;
				for item in items {
					if matches!(
						compare_bench_values(target, item, BenchOp::Eq),
						Some(BenchValue::Bool(true))
					) {
						found = true;
						break;
					}
				}
				stack.push(BenchValue::Bool(found));
			}
		}
		if stack.len() > 64 {
			return false;
		}
	}
	matches!(stack.last(), Some(BenchValue::Bool(true)))
}

fn read_bench_column(batch: &RecordBatch, col: usize, row: usize) -> Option<BenchValue<'_>> {
	let array = batch.column(col);
	if array.is_null(row) {
		return Some(BenchValue::Null);
	}
	match array.data_type() {
		DataType::Int64 => array
			.as_any()
			.downcast_ref::<Int64Array>()
			.map(|a| BenchValue::Int(a.value(row))),
		DataType::Utf8 => array
			.as_any()
			.downcast_ref::<StringArray>()
			.map(|a| BenchValue::Str(a.value(row))),
		_ => None,
	}
}

fn compare_bench_values<'a>(
	lhs: BenchValue<'a>,
	rhs: BenchValue<'a>,
	op: BenchOp,
) -> Option<BenchValue<'a>> {
	if matches!(lhs, BenchValue::Null) || matches!(rhs, BenchValue::Null) {
		return Some(BenchValue::Null);
	}
	use std::cmp::Ordering;
	let ord = match (lhs, rhs) {
		(BenchValue::Int(x), BenchValue::Int(y)) => x.cmp(&y),
		(BenchValue::Str(x), BenchValue::Str(y)) => x.as_bytes().cmp(y.as_bytes()),
		_ => return None,
	};
	let result = match op {
		BenchOp::Eq => ord == Ordering::Equal,
		BenchOp::Gte => ord != Ordering::Less,
		_ => return None,
	};
	Some(BenchValue::Bool(result))
}

fn print_measurement(label: &str, mode: &str, rows: usize, avg: Duration) {
	let rows_per_second = rows as f64 / avg.as_secs_f64();
	println!(
		"pbc_vectorized case={label} mode={mode} rows={rows} avg_ns={} rows_per_second={rows_per_second:.2}",
		avg.as_nanos(),
	);
}

fn assemble(parts: &[Instr]) -> Vec<u8> {
	let mut out = Vec::new();
	for part in parts {
		match part {
			Instr::PushCol(col) => {
				out.push(OP_PUSH_COL);
				out.extend_from_slice(&col.to_le_bytes());
			}
			Instr::PushInt(value) => {
				out.push(OP_PUSH_INT);
				out.extend_from_slice(&value.to_le_bytes());
			}
			Instr::PushStr(value) => {
				out.push(OP_PUSH_STR);
				out.extend_from_slice(&(value.len() as u16).to_le_bytes());
				out.extend_from_slice(value.as_bytes());
			}
			Instr::InList(count) => {
				out.push(OP_IN_LIST);
				out.extend_from_slice(&count.to_le_bytes());
			}
			Instr::Project(indices) => {
				out.push(OP_PROJECT);
				out.extend_from_slice(&(indices.len() as u16).to_le_bytes());
				for index in *indices {
					out.extend_from_slice(&index.to_le_bytes());
				}
			}
			Instr::Raw(op) => out.push(*op),
		}
	}
	out
}

enum Instr<'a> {
	PushCol(u32),
	PushInt(i64),
	PushStr(&'a str),
	InList(u16),
	Project(&'a [u32]),
	Raw(u8),
}
