//! ADR 053 wire-side Arrow IPC body compression benchmark.
//!
//! `--quick` keeps the run small for smoke checks:
//! `cargo bench --bench wire_arrow_ipc_body_compression -- --quick`.
//!
//! `--full` runs the ADR 053 promotion matrix, including the 10 MiB
//! payload case.

use arrow::array::{BooleanBuilder, Int32Array, LargeBinaryBuilder, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};
use wendelon::metrics::NetworkCompressionPath;
use wendelon::network::ipc::{wire_decode_batch, wire_encode_batch, wire_encode_batch_for_path};
use wendelon::schema::RecordBodyCompressionPolicy;
use wendelon::storage::ipc::{encode_record_batch_bytes, record_batch_compression_policy_from_ipc};

const KIB: usize = 1024;
const MIB: usize = 1024 * 1024;
const QUICK_TARGETS: &[usize] = &[KIB, 4 * KIB];
const DEFAULT_TARGETS: &[usize] = &[KIB, 4 * KIB, MIB];
const FULL_TARGETS: &[usize] = &[KIB, 4 * KIB, MIB, 10 * MIB];
const QUICK_MEASURE_ITERS: usize = 2;
const DEFAULT_MEASURE_ITERS: usize = 3;
const FULL_MEASURE_ITERS: usize = 5;
const TRANSMIT_ESTIMATE_LINK_MBPS: &[u64] = &[1_000, 10_000];

#[derive(Clone, Debug)]
struct BenchConfig {
	targets: Vec<usize>,
	shapes: Vec<PayloadShape>,
	measure_iters: usize,
	warmup_iters: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PayloadShape {
	NarrowInt,
	WideLowCardinality,
	WideHighCardinality,
	HighEntropyBinary,
	MixedNullable,
}

impl PayloadShape {
	const ALL: [Self; 5] = [
		Self::NarrowInt,
		Self::WideLowCardinality,
		Self::WideHighCardinality,
		Self::HighEntropyBinary,
		Self::MixedNullable,
	];

	fn label(self) -> &'static str {
		match self {
			Self::NarrowInt => "narrow_int",
			Self::WideLowCardinality => "wide_low_cardinality",
			Self::WideHighCardinality => "wide_high_cardinality",
			Self::HighEntropyBinary => "high_entropy_binary",
			Self::MixedNullable => "mixed_nullability",
		}
	}

	fn parse(value: &str) -> Option<Self> {
		Self::ALL.into_iter().find(|shape| shape.label() == value)
	}
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
	let config = BenchConfig::parse()?;
	for target_bytes in config.targets.iter().copied() {
		for shape in config.shapes.iter().copied() {
			let batch = bench_batch(target_bytes, shape)?;
			bench_case(&config, target_bytes, shape, &batch)?;
		}
	}
	bench_rpc_shapes(&config)?;
	Ok(())
}

impl BenchConfig {
	fn parse() -> Result<Self, Box<dyn std::error::Error>> {
		let mut targets = DEFAULT_TARGETS.to_vec();
		let mut shapes = PayloadShape::ALL.to_vec();
		let mut measure_iters = DEFAULT_MEASURE_ITERS;
		let mut warmup_iters = 1;
		let mut args = std::env::args().skip(1);
		while let Some(arg) = args.next() {
			match arg.as_str() {
				"--quick" => {
					targets = QUICK_TARGETS.to_vec();
					shapes = vec![
						PayloadShape::NarrowInt,
						PayloadShape::WideLowCardinality,
						PayloadShape::HighEntropyBinary,
					];
					measure_iters = QUICK_MEASURE_ITERS;
					warmup_iters = 0;
				}
				"--full" => {
					targets = FULL_TARGETS.to_vec();
					shapes = PayloadShape::ALL.to_vec();
					measure_iters = FULL_MEASURE_ITERS;
					warmup_iters = 1;
				}
				"--target-bytes" => {
					let value = args.next().ok_or("--target-bytes requires a value")?;
					targets = vec![value.parse()?];
				}
				"--shape" => {
					let value = args.next().ok_or("--shape requires a value")?;
					shapes = vec![PayloadShape::parse(&value).ok_or("unknown --shape value")?];
				}
				"--iters" => {
					let value = args.next().ok_or("--iters requires a value")?;
					measure_iters = value.parse()?;
				}
				"--bench" => {}
				_ => {}
			}
		}
		Ok(Self {
			targets,
			shapes,
			measure_iters,
			warmup_iters,
		})
	}
}

fn bench_case(
	config: &BenchConfig,
	target_bytes: usize,
	shape: PayloadShape,
	batch: &RecordBatch,
) -> std::io::Result<()> {
	let uncompressed = encode_record_batch_bytes(batch, RecordBodyCompressionPolicy::Uncompressed)?;
	let wire = wire_encode_batch(batch)?;
	let policy = record_batch_compression_policy_from_ipc(&wire)?;
	assert_eq!(policy, RecordBodyCompressionPolicy::Lz4Frame);
	let ratio = wire.len() as f64 / uncompressed.len().max(1) as f64;
	println!(
		"wire_arrow_ipc_size target_bytes={target_bytes} shape={} rows={} uncompressed_bytes={} compressed_bytes={} ratio={:.4}",
		shape.label(),
		batch.num_rows(),
		uncompressed.len(),
		wire.len(),
		ratio,
	);
	report_transmit_estimate(
		"transmit_default",
		target_bytes,
		shape.label(),
		uncompressed.len(),
	);
	report_transmit_estimate("transmit_wire_lz4", target_bytes, shape.label(), wire.len());

	bench_io(
		config,
		"encode_default",
		target_bytes,
		shape.label(),
		uncompressed.len(),
		|| encode_record_batch_bytes(batch, RecordBodyCompressionPolicy::Uncompressed).map(drop),
	)?;
	bench_io(
		config,
		"encode_wire_lz4",
		target_bytes,
		shape.label(),
		uncompressed.len(),
		|| wire_encode_batch(batch).map(drop),
	)?;
	bench_io(
		config,
		"decode_default",
		target_bytes,
		shape.label(),
		uncompressed.len(),
		|| wire_decode_batch(&uncompressed).map(drop),
	)?;
	bench_io(
		config,
		"decode_wire_lz4",
		target_bytes,
		shape.label(),
		wire.len(),
		|| wire_decode_batch(&wire).map(drop),
	)?;
	Ok(())
}

fn bench_rpc_shapes(config: &BenchConfig) -> std::io::Result<()> {
	let small = *config
		.targets
		.iter()
		.find(|target| **target >= KIB)
		.unwrap_or(&KIB);
	let largest = config.targets.iter().copied().max().unwrap_or(4 * KIB);
	let medium = largest.max(4 * KIB);

	let table_delete = bench_batch(small, PayloadShape::WideHighCardinality)?;
	bench_rpc_encode_decode(
		config,
		"table_delete_req",
		"client_batch",
		NetworkCompressionPath::ClientBatch,
		&table_delete,
	)?;

	let query = bench_batch(medium, PayloadShape::WideLowCardinality)?;
	bench_rpc_encode_decode(
		config,
		"table_query_resp",
		"query_response",
		NetworkCompressionPath::QueryResponse,
		&query,
	)?;

	let migrate = bench_batch(medium, PayloadShape::MixedNullable)?;
	bench_rpc_encode_decode(
		config,
		"migrate_resp",
		"migration_records",
		NetworkCompressionPath::MigrationRecords,
		&migrate,
	)?;

	let read = bench_batch(medium, PayloadShape::WideLowCardinality)?;
	let pass_through = encode_record_batch_bytes(&read, RecordBodyCompressionPolicy::Lz4Frame)?;
	bench_io(
		config,
		"rpc_read_resp_pass_through_decode_only",
		medium,
		"read_response",
		pass_through.len(),
		|| wire_decode_batch(&pass_through).map(drop),
	)?;
	println!(
		"wire_arrow_ipc_pass_through path=read_response target_bytes={medium} encoded_bytes={} wire_helper_encode_in_timed_path=false",
		pass_through.len()
	);
	Ok(())
}

fn bench_rpc_encode_decode(
	config: &BenchConfig,
	rpc: &'static str,
	path: &'static str,
	metric_path: NetworkCompressionPath,
	batch: &RecordBatch,
) -> std::io::Result<()> {
	let uncompressed = encode_record_batch_bytes(batch, RecordBodyCompressionPolicy::Uncompressed)?;
	let compressed = wire_encode_batch_for_path(batch, metric_path)?;
	let ratio = compressed.len() as f64 / uncompressed.len().max(1) as f64;
	println!(
		"wire_arrow_ipc_rpc_size rpc={rpc} path={path} uncompressed_bytes={} compressed_bytes={} ratio={:.4}",
		uncompressed.len(),
		compressed.len(),
		ratio,
	);
	report_transmit_estimate(
		&format!("rpc_{rpc}_default_transmit"),
		uncompressed.len(),
		path,
		uncompressed.len(),
	);
	report_transmit_estimate(
		&format!("rpc_{rpc}_wire_lz4_transmit"),
		compressed.len(),
		path,
		compressed.len(),
	);
	bench_io(
		config,
		&format!("rpc_{rpc}_default_encode_decode"),
		uncompressed.len(),
		path,
		uncompressed.len(),
		|| {
			let encoded =
				encode_record_batch_bytes(batch, RecordBodyCompressionPolicy::Uncompressed)?;
			wire_decode_batch(&encoded).map(drop)
		},
	)?;
	bench_io(
		config,
		&format!("rpc_{rpc}_wire_lz4_encode_decode"),
		compressed.len(),
		path,
		compressed.len(),
		|| {
			let encoded = wire_encode_batch_for_path(batch, metric_path)?;
			wire_decode_batch(&encoded).map(drop)
		},
	)
}

fn bench_io<F>(
	config: &BenchConfig,
	name: &str,
	target_bytes: usize,
	shape: &str,
	bytes_per_op: usize,
	mut f: F,
) -> std::io::Result<()>
where
	F: FnMut() -> std::io::Result<()>,
{
	for _ in 0..config.warmup_iters {
		black_box(f()?);
	}
	let mut elapsed = Duration::ZERO;
	for _ in 0..config.measure_iters {
		let started = Instant::now();
		black_box(f()?);
		elapsed += started.elapsed();
	}
	report(
		name,
		target_bytes,
		shape,
		bytes_per_op,
		config.measure_iters,
		elapsed,
	);
	Ok(())
}

fn report(
	name: &str,
	target_bytes: usize,
	shape: &str,
	bytes_per_op: usize,
	iterations: usize,
	elapsed: Duration,
) {
	let total_bytes = bytes_per_op.saturating_mul(iterations);
	let micros_per_op = elapsed.as_secs_f64() * 1_000_000.0 / iterations.max(1) as f64;
	let mib_per_s = if elapsed.is_zero() {
		0.0
	} else {
		total_bytes as f64 / MIB as f64 / elapsed.as_secs_f64()
	};
	println!(
		"wire_arrow_ipc_bench op={name} target_bytes={target_bytes} shape={shape} iterations={iterations} bytes_per_op={bytes_per_op} total_ms={:.3} micros_per_op={:.3} mib_per_s={:.2}",
		elapsed.as_secs_f64() * 1_000.0,
		micros_per_op,
		mib_per_s,
	);
}

fn report_transmit_estimate(name: &str, target_bytes: usize, shape: &str, bytes_per_op: usize) {
	for link_mbps in TRANSMIT_ESTIMATE_LINK_MBPS {
		let micros_per_op = bytes_per_op as f64 * 8.0 / *link_mbps as f64;
		println!(
			"wire_arrow_ipc_transmit_estimate op={name} target_bytes={target_bytes} shape={shape} bytes_per_op={bytes_per_op} link_mbps={link_mbps} micros_per_op={micros_per_op:.3}",
		);
	}
}

fn bench_batch(target_bytes: usize, shape: PayloadShape) -> std::io::Result<RecordBatch> {
	match shape {
		PayloadShape::NarrowInt => narrow_int_batch(target_bytes),
		PayloadShape::WideLowCardinality => binary_batch(target_bytes, shape, 128),
		PayloadShape::WideHighCardinality => binary_batch(target_bytes, shape, 128),
		PayloadShape::HighEntropyBinary => binary_batch(target_bytes, shape, 128),
		PayloadShape::MixedNullable => mixed_nullable_batch(target_bytes),
	}
}

fn narrow_int_batch(target_bytes: usize) -> std::io::Result<RecordBatch> {
	let rows = (target_bytes / 12).max(1);
	let ids = Arc::new(UInt64Array::from_iter_values(
		(0..rows).map(|row| row as u64),
	));
	let values = Arc::new(Int32Array::from_iter_values(
		(0..rows).map(|row| (row % 1024) as i32),
	));
	let schema = Arc::new(Schema::new(vec![
		Field::new("id", DataType::UInt64, false),
		Field::new("value", DataType::Int32, false),
	]));
	RecordBatch::try_new(schema, vec![ids, values]).map_err(invalid_data)
}

fn binary_batch(
	target_bytes: usize,
	shape: PayloadShape,
	width: usize,
) -> std::io::Result<RecordBatch> {
	let rows = (target_bytes / width.max(1)).max(1);
	let ids = Arc::new(UInt64Array::from_iter_values(
		(0..rows).map(|row| row as u64),
	));
	let mut payload = LargeBinaryBuilder::new();
	for row in 0..rows {
		payload.append_value(payload_bytes(shape, row, width));
	}
	let schema = Arc::new(Schema::new(vec![
		Field::new("id", DataType::UInt64, false),
		Field::new("payload", DataType::LargeBinary, false),
	]));
	RecordBatch::try_new(schema, vec![ids, Arc::new(payload.finish())]).map_err(invalid_data)
}

fn mixed_nullable_batch(target_bytes: usize) -> std::io::Result<RecordBatch> {
	let width = 96;
	let rows = (target_bytes / width).max(1);
	let ids = Arc::new(UInt64Array::from_iter_values(
		(0..rows).map(|row| row as u64),
	));
	let mut payload = LargeBinaryBuilder::new();
	let mut flag = BooleanBuilder::new();
	for row in 0..rows {
		if row % 7 == 0 {
			payload.append_null();
		} else {
			payload.append_value(payload_bytes(PayloadShape::WideHighCardinality, row, width));
		}
		if row % 11 == 0 {
			flag.append_null();
		} else {
			flag.append_value(row % 2 == 0);
		}
	}
	let schema = Arc::new(Schema::new(vec![
		Field::new("id", DataType::UInt64, false),
		Field::new("payload", DataType::LargeBinary, true),
		Field::new("flag", DataType::Boolean, true),
	]));
	RecordBatch::try_new(
		schema,
		vec![ids, Arc::new(payload.finish()), Arc::new(flag.finish())],
	)
	.map_err(invalid_data)
}

fn payload_bytes(shape: PayloadShape, row: usize, width: usize) -> Vec<u8> {
	match shape {
		PayloadShape::WideLowCardinality => vec![b'A' + (row % 4) as u8; width],
		PayloadShape::WideHighCardinality | PayloadShape::MixedNullable => {
			let mut out = vec![0_u8; width];
			let seed = format!("tenant=53 table=wire row={row:016x};");
			for (idx, byte) in out.iter_mut().enumerate() {
				*byte = seed.as_bytes()[idx % seed.len()] ^ ((row + idx) as u8 & 0x0f);
			}
			out
		}
		PayloadShape::HighEntropyBinary => {
			let mut state = (row as u64)
				.wrapping_mul(0x9e37_79b9_7f4a_7c15)
				.wrapping_add(0x0530_5305_3053_0530);
			let mut out = vec![0_u8; width];
			for byte in &mut out {
				state ^= state << 13;
				state ^= state >> 7;
				state ^= state << 17;
				*byte = state as u8;
			}
			out
		}
		PayloadShape::NarrowInt => unreachable!("narrow_int has no binary payload"),
	}
}

fn invalid_data(err: arrow::error::ArrowError) -> std::io::Error {
	std::io::Error::new(std::io::ErrorKind::InvalidData, err)
}
