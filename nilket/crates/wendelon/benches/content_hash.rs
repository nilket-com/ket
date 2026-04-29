//! ADR 049 content-hash row identity benchmark.
//!
//! `--quick` keeps the run small for smoke checks:
//! `cargo bench --bench content_hash -- --quick`.

use std::collections::HashSet;
use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{ArrayRef, Int64Array, LargeBinaryArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use wendelon::schema::{
	ClientWriteId, DurabilityLevel, HybridTimestamp, LocalityPolicyV1, MultiRowLayoutDescriptor,
	NodeId, RecordLayoutPolicy, RowIdentityPolicy, SchemaEvolutionPolicyV1, TableObjectId,
	TableSchemaDescriptorV1, TableSpreadLocalityDescriptorV1, TableWriteRequestMeta, TenantId,
	WriterNodeId, encode_table_write_row_count, validate_table_write_request_batch,
};
use wendelon::storage::content_hash::encode_row_canonical;

const DEFAULT_ROWS: &[usize] = &[100_000, 1_000_000];
const QUICK_ROWS: &[usize] = &[256];
const DEFAULT_PAYLOAD_BYTES: usize = 256;
const MAX_BATCH_ROWS: usize = 65_536;
const WARMUP_ITERS: usize = 2;
const MEASURE_ITERS: usize = 5;

#[derive(Clone, Debug)]
struct BenchConfig {
	rows: Vec<usize>,
	payload_bytes: usize,
	measure_iters: usize,
}

impl BenchConfig {
	fn parse() -> Self {
		let mut rows = DEFAULT_ROWS.to_vec();
		let mut payload_bytes = DEFAULT_PAYLOAD_BYTES;
		let mut measure_iters = MEASURE_ITERS;
		let mut args = std::env::args().skip(1);
		while let Some(arg) = args.next() {
			match arg.as_str() {
				"--quick" => {
					rows = QUICK_ROWS.to_vec();
					measure_iters = 2;
				}
				"--rows" => {
					if let Some(value) = args.next() {
						rows = vec![value.parse().expect("--rows expects a positive integer")];
					}
				}
				"--payload-bytes" => {
					if let Some(value) = args.next() {
						payload_bytes = value
							.parse()
							.expect("--payload-bytes expects a non-negative integer");
					}
				}
				"--bench" => {}
				_ => {}
			}
		}
		Self {
			rows,
			payload_bytes,
			measure_iters,
		}
	}
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
	let config = BenchConfig::parse();
	let identity_descriptor = descriptor(LocalityPolicyV1::IdentityProjection)?;
	let spread_descriptor = descriptor(LocalityPolicyV1::TableSpreadProjection(
		TableSpreadLocalityDescriptorV1 {
			spread_factor: 4,
			prefix_bits: 16,
		},
	))?;
	for rows in config.rows.iter().copied() {
		let batch = batch(rows, config.payload_bytes);
		let identity_chunks = chunked_batches(&identity_descriptor, &batch)?;
		let spread_chunks = chunked_batches(&spread_descriptor, &batch)?;

		bench_canonical_encode(
			"canonical-encode",
			&identity_descriptor,
			&batch,
			WARMUP_ITERS,
			config.measure_iters,
		)?;
		let admission = bench_admission(
			"admission-identity",
			rows,
			&identity_descriptor,
			&identity_chunks,
			WARMUP_ITERS,
			config.measure_iters,
		)?;
		bench_admission(
			"admission-table-spread",
			rows,
			&spread_descriptor,
			&spread_chunks,
			WARMUP_ITERS,
			config.measure_iters,
		)?;
		bench_duplicate_lookup(
			"duplicate-lookup-half-visible",
			&admission,
			WARMUP_ITERS,
			config.measure_iters,
		);
	}

	Ok(())
}

fn descriptor(
	locality: LocalityPolicyV1,
) -> Result<TableSchemaDescriptorV1, Box<dyn std::error::Error>> {
	let schema = Schema::new(vec![
		Field::new("event_id", DataType::Int64, false),
		Field::new("payload", DataType::LargeBinary, true),
	]);
	Ok(TableSchemaDescriptorV1::new_with_layout(
		TableObjectId::new([0x49; 16]),
		1,
		"bench",
		"content_hash",
		&schema,
		RowIdentityPolicy::AppendOnlyContentHash,
		locality,
		RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::default()),
		SchemaEvolutionPolicyV1::AdditiveOnly,
	)?)
}

fn batch(rows: usize, payload_bytes: usize) -> RecordBatch {
	let schema = Arc::new(Schema::new(vec![
		Field::new("event_id", DataType::Int64, false),
		Field::new("payload", DataType::LargeBinary, true),
	]));
	let ids = (0..rows as i64).collect::<Vec<_>>();
	let payloads = (0..rows)
		.map(|row| {
			let mut payload = vec![0_u8; payload_bytes];
			if !payload.is_empty() {
				payload[0] = (row & 0xff) as u8;
			}
			Some(payload)
		})
		.collect::<Vec<_>>();
	let payload_refs = payloads
		.iter()
		.map(|payload| payload.as_ref().map(Vec::as_slice))
		.collect::<Vec<_>>();
	RecordBatch::try_new(
		schema,
		vec![
			Arc::new(Int64Array::from(ids)) as ArrayRef,
			Arc::new(LargeBinaryArray::from(payload_refs)) as ArrayRef,
		],
	)
	.unwrap()
}

fn bench_meta(
	descriptor: &TableSchemaDescriptorV1,
	rows: usize,
) -> Result<TableWriteRequestMeta, Box<dyn std::error::Error>> {
	Ok(TableWriteRequestMeta {
		parent_write_id: ClientWriteId {
			client_uuid: [0x49; 16],
			sequence_no: 1,
			idempotency_ttl_ms: 30_000,
			_pad: 0,
		},
		client_hlc: HybridTimestamp {
			physical_ms: 10,
			logical: 0,
			node_id: 1,
		},
		expected_epoch: 1,
		tenant_id: TenantId::new(49),
		table_object_id: descriptor.table_object_id,
		schema_generation: descriptor.schema_generation,
		child_ordinal_base: 0,
		proxy_source: NodeId {
			id: 0,
			_reserved: 0,
		},
		row_count: encode_table_write_row_count(rows)?,
		proxy_hop: 0,
		durability: DurabilityLevel::QUORUM,
		await_finalization: 0,
		_pad: [0; 3],
	})
}

struct AdmissionChunk {
	meta: TableWriteRequestMeta,
	batch: RecordBatch,
}

fn chunked_batches(
	descriptor: &TableSchemaDescriptorV1,
	batch: &RecordBatch,
) -> Result<Vec<AdmissionChunk>, Box<dyn std::error::Error>> {
	let mut chunks = Vec::new();
	let mut offset = 0usize;
	while offset < batch.num_rows() {
		let len = (batch.num_rows() - offset).min(MAX_BATCH_ROWS);
		let mut meta = bench_meta(descriptor, len)?;
		meta.child_ordinal_base = offset as u64;
		chunks.push(AdmissionChunk {
			meta,
			batch: batch.slice(offset, len),
		});
		offset += len;
	}
	Ok(chunks)
}

fn bench_canonical_encode(
	label: &str,
	descriptor: &TableSchemaDescriptorV1,
	batch: &RecordBatch,
	warmup_iters: usize,
	measure_iters: usize,
) -> Result<(), Box<dyn std::error::Error>> {
	for _ in 0..warmup_iters {
		for row in 0..batch.num_rows() {
			black_box(encode_row_canonical(descriptor, batch, row)?);
		}
	}
	let started = Instant::now();
	for _ in 0..measure_iters {
		for row in 0..batch.num_rows() {
			black_box(encode_row_canonical(descriptor, batch, row)?);
		}
	}
	let operations = batch.num_rows() * measure_iters;
	print_result(
		label,
		batch.num_rows(),
		measure_iters,
		operations,
		started.elapsed(),
	);
	Ok(())
}

fn bench_admission(
	label: &str,
	rows: usize,
	descriptor: &TableSchemaDescriptorV1,
	chunks: &[AdmissionChunk],
	warmup_iters: usize,
	measure_iters: usize,
) -> Result<Vec<wendelon::schema::Identity>, Box<dyn std::error::Error>> {
	for _ in 0..warmup_iters {
		for chunk in chunks {
			black_box(validate_table_write_request_batch(
				&chunk.meta,
				&chunk.batch,
				descriptor,
				WriterNodeId::ZERO,
				chunk.meta.client_hlc,
			)?);
		}
	}
	let started = Instant::now();
	let mut last_keys = Vec::new();
	for _ in 0..measure_iters {
		last_keys.clear();
		for chunk in chunks {
			let admission = validate_table_write_request_batch(
				&chunk.meta,
				&chunk.batch,
				descriptor,
				WriterNodeId::ZERO,
				chunk.meta.client_hlc,
			)?;
			last_keys.extend(admission.key_identities);
		}
	}
	print_result(
		label,
		rows,
		measure_iters,
		rows * measure_iters,
		started.elapsed(),
	);
	Ok(black_box(last_keys))
}

fn bench_duplicate_lookup(
	label: &str,
	keys: &[wendelon::schema::Identity],
	warmup_iters: usize,
	measure_iters: usize,
) {
	let visible = keys
		.iter()
		.enumerate()
		.filter_map(|(idx, key)| (idx % 2 == 0).then_some(*key))
		.collect::<HashSet<_>>();
	for _ in 0..warmup_iters {
		let count = keys.iter().filter(|key| visible.contains(*key)).count();
		black_box(count);
	}
	let started = Instant::now();
	let mut absorbed = 0_usize;
	for _ in 0..measure_iters {
		absorbed += keys.iter().filter(|key| visible.contains(*key)).count();
	}
	black_box(absorbed);
	print_result(
		label,
		keys.len(),
		measure_iters,
		keys.len() * measure_iters,
		started.elapsed(),
	);
}

fn print_result(label: &str, rows: usize, iterations: usize, operations: usize, elapsed: Duration) {
	println!(
		"content_hash case={label} rows={rows} iterations={iterations} total_ns={} ns_per_row={} rows_per_sec={:.2}",
		elapsed.as_nanos(),
		ns_per_operation(elapsed, operations),
		rows_per_second(elapsed, operations),
	);
}

fn ns_per_operation(elapsed: Duration, operations: usize) -> u128 {
	if operations == 0 {
		0
	} else {
		elapsed.as_nanos() / operations as u128
	}
}

fn rows_per_second(elapsed: Duration, operations: usize) -> f64 {
	let secs = elapsed.as_secs_f64();
	if secs == 0.0 {
		0.0
	} else {
		operations as f64 / secs
	}
}
