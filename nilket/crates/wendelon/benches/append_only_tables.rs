//! ADR 045 Phase 7 benchmark: append-only table ingest CPU.
//!
//! Measures the operational metrics ADR 045 calls out:
//!
//! - Append-only ingest throughput (rows / s) vs PK-keyed
//!   admission baseline at matched batch sizes.
//! - Identical-payload append cost (no short-circuit; payload-
//!   hash dominates).
//! - Identity-synthesis CPU per row (BLAKE3 over the ADR 045 §4
//!   preimage).
//! - Multi-row packing throughput on append-only batches.
//!
//! `--quick` runs a small batch sweep for `cargo bench --bench
//! append_only_tables -- --quick` smoke runs.

use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{ArrayRef, LargeBinaryArray, TimestampMillisecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use wendelon::schema::{
	ClientWriteId, DurabilityLevel, HybridTimestamp, IntraBatchIndex, LocalityPolicyV1,
	MultiRowLayoutDescriptor, NodeId, PrimaryKeyDescriptorV1, RecordLayoutPolicy,
	RowIdentityPolicy, SchemaEvolutionPolicyV1, TableObjectId, TableSchemaDescriptorV1,
	TableWriteRequestMeta, TenantId, WriterNodeId, encode_table_write_row_count,
	synthesize_append_only_row_id, validate_table_write_request_batch,
};
use wendelon::storage::pack_record_batch_by_bucket;

const DEFAULT_BATCH_ROWS: usize = 4096;
const QUICK_BATCH_ROWS: usize = 256;
const DEFAULT_PAYLOAD_BYTES: usize = 256;
const PAYLOAD_SWEEP: &[usize] = &[0, 32, 256, 1024];
const WARMUP_ITERS: usize = 2;
const MEASURE_ITERS: usize = 8;

#[derive(Clone, Debug)]
struct BenchConfig {
	rows: usize,
	measure_iters: usize,
	mode: BenchMode,
}

#[derive(Clone, Debug)]
enum BenchMode {
	/// Single-payload-size run (legacy default).
	Single { payload_bytes: usize },
	/// Sweep across `PAYLOAD_SWEEP` to surface payload-hash
	/// CPU scaling vs identity-synthesis.
	Sweep,
}

impl BenchConfig {
	fn parse() -> Self {
		let mut rows = DEFAULT_BATCH_ROWS;
		let mut measure_iters = MEASURE_ITERS;
		let mut payload_bytes = DEFAULT_PAYLOAD_BYTES;
		let mut sweep = false;
		let mut iter = std::env::args().skip(1);
		while let Some(arg) = iter.next() {
			match arg.as_str() {
				"--quick" => {
					rows = QUICK_BATCH_ROWS;
					measure_iters = 2;
				}
				"--sweep" => sweep = true,
				"--payload-bytes" => {
					if let Some(value) = iter.next() {
						payload_bytes = value
							.parse::<usize>()
							.expect("--payload-bytes expects a non-negative integer");
					}
				}
				"--rows" => {
					if let Some(value) = iter.next() {
						rows = value
							.parse::<usize>()
							.expect("--rows expects a positive integer");
					}
				}
				_ => {}
			}
		}
		let mode = if sweep {
			BenchMode::Sweep
		} else {
			BenchMode::Single { payload_bytes }
		};
		Self {
			rows,
			measure_iters,
			mode,
		}
	}
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
	let config = BenchConfig::parse();
	match &config.mode {
		BenchMode::Single { payload_bytes } => run_payload_size(&config, *payload_bytes)?,
		BenchMode::Sweep => {
			for &payload_bytes in PAYLOAD_SWEEP {
				run_payload_size(&config, payload_bytes)?;
			}
		}
	}
	Ok(())
}

fn run_payload_size(
	config: &BenchConfig,
	payload_bytes: usize,
) -> Result<(), Box<dyn std::error::Error>> {
	let descriptor = append_only_descriptor()?;
	let pk_descriptor = pk_descriptor()?;
	let batch = append_only_batch(config.rows, payload_bytes);
	let pk_batch = pk_batch_matching_shape(&batch);
	let meta = bench_meta(&descriptor, batch.num_rows())?;
	let pk_meta = bench_meta(&pk_descriptor, pk_batch.num_rows())?;

	let label_suffix = format!("payload_bytes={payload_bytes}");

	bench_admission(
		&format!("append-only {label_suffix}"),
		&meta,
		&batch,
		&descriptor,
		WARMUP_ITERS,
		config.measure_iters,
	)?;
	bench_admission(
		&format!("primary-key {label_suffix}"),
		&pk_meta,
		&pk_batch,
		&pk_descriptor,
		WARMUP_ITERS,
		config.measure_iters,
	)?;
	bench_identical_payload(
		&format!("append-only-identical-payload {label_suffix}"),
		&meta,
		&descriptor,
		config.rows,
		payload_bytes,
		WARMUP_ITERS,
		config.measure_iters,
	)?;
	bench_synthesize_identity(
		&format!("identity-synthesis {label_suffix}"),
		config.rows,
		WARMUP_ITERS,
		config.measure_iters,
	)?;
	bench_multirow_pack(
		&format!("append-only-multirow-pack {label_suffix}"),
		&meta,
		&batch,
		&descriptor,
		WARMUP_ITERS,
		config.measure_iters,
	)?;

	Ok(())
}

fn append_only_descriptor() -> Result<TableSchemaDescriptorV1, Box<dyn std::error::Error>> {
	let schema = Schema::new(vec![
		Field::new(
			"event_time",
			DataType::Timestamp(TimeUnit::Millisecond, None),
			false,
		),
		Field::new("payload", DataType::LargeBinary, true),
	]);
	Ok(TableSchemaDescriptorV1::new_with_layout(
		TableObjectId::new([0x45; 16]),
		1,
		"bench",
		"events",
		&schema,
		RowIdentityPolicy::AppendOnly,
		LocalityPolicyV1::default_table_spread(),
		RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::default()),
		SchemaEvolutionPolicyV1::AdditiveOnly,
	)?)
}

fn pk_descriptor() -> Result<TableSchemaDescriptorV1, Box<dyn std::error::Error>> {
	let schema = Schema::new(vec![
		Field::new("id", DataType::LargeBinary, false),
		Field::new("payload", DataType::LargeBinary, true),
	]);
	Ok(TableSchemaDescriptorV1::new_with_layout(
		TableObjectId::new([0x44; 16]),
		1,
		"bench",
		"events_pk",
		&schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
		LocalityPolicyV1::default_table_spread(),
		RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::default()),
		SchemaEvolutionPolicyV1::AdditiveOnly,
	)?)
}

fn append_only_batch(rows: usize, payload_bytes: usize) -> RecordBatch {
	let times = (0..rows)
		.map(|i| 1_700_000_000_000_i64 + i as i64)
		.collect::<Vec<_>>();
	let payloads = (0..rows)
		.map(|i| {
			if payload_bytes == 0 {
				Some(Vec::new())
			} else {
				let mut bytes = vec![0u8; payload_bytes];
				let prefix_len = payload_bytes.min(8);
				bytes[..prefix_len].copy_from_slice(&(i as u64).to_le_bytes()[..prefix_len]);
				Some(bytes)
			}
		})
		.collect::<Vec<_>>();
	let payload_refs: Vec<Option<&[u8]>> = payloads.iter().map(|opt| opt.as_deref()).collect();
	RecordBatch::try_new(
		Arc::new(Schema::new(vec![
			Field::new(
				"event_time",
				DataType::Timestamp(TimeUnit::Millisecond, None),
				false,
			),
			Field::new("payload", DataType::LargeBinary, true),
		])),
		vec![
			Arc::new(TimestampMillisecondArray::from(times)) as ArrayRef,
			Arc::new(LargeBinaryArray::from(payload_refs)) as ArrayRef,
		],
	)
	.unwrap()
}

fn pk_batch_matching_shape(append_only: &RecordBatch) -> RecordBatch {
	let rows = append_only.num_rows();
	let ids: Vec<Vec<u8>> = (0..rows)
		.map(|i| (i as u64).to_be_bytes().to_vec())
		.collect();
	let id_refs: Vec<&[u8]> = ids.iter().map(Vec::as_slice).collect();
	let payload_array = append_only
		.column_by_name("payload")
		.unwrap()
		.as_any()
		.downcast_ref::<LargeBinaryArray>()
		.unwrap()
		.clone();
	RecordBatch::try_new(
		Arc::new(Schema::new(vec![
			Field::new("id", DataType::LargeBinary, false),
			Field::new("payload", DataType::LargeBinary, true),
		])),
		vec![
			Arc::new(LargeBinaryArray::from(id_refs)) as ArrayRef,
			Arc::new(payload_array) as ArrayRef,
		],
	)
	.unwrap()
}

fn bench_meta(
	descriptor: &TableSchemaDescriptorV1,
	rows: usize,
) -> Result<TableWriteRequestMeta, Box<dyn std::error::Error>> {
	let mut meta: TableWriteRequestMeta = bytemuck::Zeroable::zeroed();
	meta.tenant_id = TenantId::new(7);
	meta.table_object_id = descriptor.table_object_id;
	meta.schema_generation = descriptor.schema_generation;
	meta.row_count = encode_table_write_row_count(rows)?;
	meta.client_hlc = HybridTimestamp {
		physical_ms: 1_700_000_000_000,
		logical: 0,
		node_id: 1,
	};
	meta.proxy_source = NodeId {
		id: 1,
		_reserved: 0,
	};
	meta.expected_epoch = 1;
	meta.parent_write_id = ClientWriteId {
		client_uuid: [0xab; 16],
		sequence_no: 1,
		idempotency_ttl_ms: 30_000,
		_pad: 0,
	};
	meta.durability = DurabilityLevel::QUORUM;
	Ok(meta)
}

fn bench_admission(
	label: &str,
	meta: &TableWriteRequestMeta,
	batch: &RecordBatch,
	descriptor: &TableSchemaDescriptorV1,
	warmup: usize,
	measure: usize,
) -> Result<(), Box<dyn std::error::Error>> {
	for _ in 0..warmup {
		let admission = validate_table_write_request_batch(
			meta,
			batch,
			descriptor,
			WriterNodeId::ZERO,
			meta.client_hlc,
		)?;
		black_box(admission);
	}
	let started = Instant::now();
	let mut admitted_rows: u64 = 0;
	for _ in 0..measure {
		let admission = validate_table_write_request_batch(
			meta,
			batch,
			descriptor,
			WriterNodeId::ZERO,
			meta.client_hlc,
		)?;
		admitted_rows += admission.key_identities.len() as u64;
		black_box(admission);
	}
	let elapsed = started.elapsed();
	report_throughput(label, elapsed, admitted_rows, batch.num_rows());
	Ok(())
}

fn bench_identical_payload(
	label: &str,
	meta: &TableWriteRequestMeta,
	descriptor: &TableSchemaDescriptorV1,
	rows: usize,
	payload_bytes: usize,
	warmup: usize,
	measure: usize,
) -> Result<(), Box<dyn std::error::Error>> {
	let payload = vec![0xCDu8; payload_bytes];
	let payloads = (0..rows)
		.map(|_| Some(payload.as_slice()))
		.collect::<Vec<_>>();
	let times = (0..rows).map(|_| 1_700_000_000_000_i64).collect::<Vec<_>>();
	let batch = RecordBatch::try_new(
		Arc::new(Schema::new(vec![
			Field::new(
				"event_time",
				DataType::Timestamp(TimeUnit::Millisecond, None),
				false,
			),
			Field::new("payload", DataType::LargeBinary, true),
		])),
		vec![
			Arc::new(TimestampMillisecondArray::from(times)) as ArrayRef,
			Arc::new(LargeBinaryArray::from(payloads)) as ArrayRef,
		],
	)?;
	for _ in 0..warmup {
		let admission = validate_table_write_request_batch(
			meta,
			&batch,
			descriptor,
			WriterNodeId::ZERO,
			meta.client_hlc,
		)?;
		black_box(admission);
	}
	let started = Instant::now();
	let mut admitted_rows: u64 = 0;
	let mut distinct_identities = 0usize;
	for _ in 0..measure {
		let admission = validate_table_write_request_batch(
			meta,
			&batch,
			descriptor,
			WriterNodeId::ZERO,
			meta.client_hlc,
		)?;
		admitted_rows += admission.key_identities.len() as u64;
		// Sanity-check that identical-payload admission still produces
		// distinct identities — the bench fails fast if this regresses.
		let mut sorted = admission.key_identities.clone();
		sorted.sort();
		sorted.dedup();
		distinct_identities = sorted.len();
		black_box(admission);
	}
	let elapsed = started.elapsed();
	report_throughput(label, elapsed, admitted_rows, batch.num_rows());
	if distinct_identities != batch.num_rows() {
		return Err(format!(
			"identical-payload admission produced {distinct_identities} distinct identities for \
			 {} rows — ADR 045 §4 contract regressed",
			batch.num_rows()
		)
		.into());
	}
	Ok(())
}

fn bench_synthesize_identity(
	label: &str,
	rows: usize,
	warmup: usize,
	measure: usize,
) -> Result<(), Box<dyn std::error::Error>> {
	let tenant = TenantId::new(7);
	let table = TableObjectId::new([0x45; 16]);
	let writer = WriterNodeId::new([0xab; 16]);
	let hlc = HybridTimestamp {
		physical_ms: 1_700_000_000_000,
		logical: 0,
		node_id: 1,
	};
	for _ in 0..warmup {
		for row in 0..rows {
			let idx = IntraBatchIndex::try_new(row as u32)?;
			black_box(synthesize_append_only_row_id(
				tenant, table, writer, hlc, idx,
			));
		}
	}
	let started = Instant::now();
	let mut count: u64 = 0;
	for _ in 0..measure {
		for row in 0..rows {
			let idx = IntraBatchIndex::try_new(row as u32)?;
			let id = synthesize_append_only_row_id(tenant, table, writer, hlc, idx);
			black_box(id);
			count += 1;
		}
	}
	let elapsed = started.elapsed();
	report_throughput(label, elapsed, count, rows);
	Ok(())
}

fn bench_multirow_pack(
	label: &str,
	meta: &TableWriteRequestMeta,
	batch: &RecordBatch,
	descriptor: &TableSchemaDescriptorV1,
	warmup: usize,
	measure: usize,
) -> Result<(), Box<dyn std::error::Error>> {
	for _ in 0..warmup {
		let packed = pack_record_batch_by_bucket(
			meta,
			descriptor,
			batch,
			WriterNodeId::ZERO,
			meta.client_hlc,
		)?;
		black_box(packed);
	}
	let started = Instant::now();
	let mut packed_records: u64 = 0;
	let mut packed_rows: u64 = 0;
	for _ in 0..measure {
		let packed = pack_record_batch_by_bucket(
			meta,
			descriptor,
			batch,
			WriterNodeId::ZERO,
			meta.client_hlc,
		)?;
		packed_records += packed.len() as u64;
		// Each iteration packs the full input batch; rows = batch
		// rows × iterations.
		packed_rows += batch.num_rows() as u64;
		black_box(packed);
	}
	let elapsed = started.elapsed();
	// Per-row throughput so the number is comparable to the
	// admission and identity-synthesis rows. records_per_iter is
	// emitted on a sibling line so the shape of the multi-row
	// packing is still observable.
	report_throughput(label, elapsed, packed_rows, batch.num_rows());
	let records_per_iter = if measure == 0 {
		0.0
	} else {
		packed_records as f64 / measure as f64
	};
	println!(
		"append_only_tables case={label} records_per_iter={records_per_iter:.1} \
		 records_total={packed_records}"
	);
	Ok(())
}

fn report_throughput(label: &str, elapsed: Duration, items: u64, rows_per_iter: usize) {
	let micros = elapsed.as_secs_f64() * 1_000_000.0;
	let per_item = if items == 0 {
		0.0
	} else {
		micros / items as f64
	};
	let throughput = if elapsed.as_secs_f64() == 0.0 {
		0.0
	} else {
		items as f64 / elapsed.as_secs_f64()
	};
	println!(
		"append_only_tables case={label} items={items} rows_per_iter={rows_per_iter} \
		 elapsed_micros={micros:.1} per_item_micros={per_item:.3} throughput_per_sec={throughput:.0}"
	);
}
