//! ADR 052 secondary-index cold-path benchmark.
//!
//! `--quick` keeps the run small for smoke checks:
//! `cargo bench --bench secondary_index -- --quick`.
//!
//! `--full` uses the ADR 052 promotion-gate row counts:
//! `cargo bench --bench secondary_index -- --full`.
//!
//! This harness covers key encoding, segment build/codec, query filtering, and
//! candidate revalidation. Hybrid-plan crossover, compaction rebuild, and
//! revalidation cost partitioning belong in the full bench-results matrix.

use std::hint::black_box;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{Int64Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use wendelon::schema::{
	HybridTimestamp, Identity, IndexArrowTypeTag, IndexKeyEncoding, IndexObjectId, Locality,
	LocalityPolicyV1, PrimaryKeyDescriptorV1, RecordRowIndex, RowIdentityPolicy,
	SchemaEvolutionPolicyV1, SecondaryIndexDescriptor, SecondaryIndexKeyDescriptor, SlabId,
	TableObjectId, TableSchemaDescriptorV1, TableWriteAdmission, TenantId,
};
use wendelon::storage::{
	IndexBound, IndexQuery, RecordLocation, SecondaryIndexCandidateValidation,
	SecondaryIndexIdentity, SecondaryIndexSegment, append_storage_synthetic_columns,
	revalidate_secondary_index_candidate, secondary_index_entries_from_storage_batch,
	secondary_index_entry_matches_query,
};

const DEFAULT_ROWS: &[usize] = &[4_096, 65_536];
const QUICK_ROWS: &[usize] = &[1_024];
const FULL_ROWS: &[usize] = &[100_000, 1_000_000];
const MEASURE_ITERS: usize = 5;
const QUICK_MEASURE_ITERS: usize = 2;

#[derive(Clone, Debug)]
struct BenchConfig {
	rows: Vec<usize>,
	measure_iters: usize,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
	let config = BenchConfig::parse()?;
	let table = table_descriptor()?;
	let descriptor = secondary_index_descriptor(&table);
	let row_hlc = hlc(1_700_000_052_000, 0);

	for rows in config.rows.iter().copied() {
		let batch = indexed_batch(rows, row_hlc)?;
		let base_location = location(SlabId::new(52), row_hlc, RecordRowIndex::ZERO);
		let entries =
			secondary_index_entries_from_storage_batch(&descriptor, &batch, base_location)?;
		let segment = SecondaryIndexSegment::build(
			SecondaryIndexIdentity::from_descriptor(TenantId::new(52), &descriptor),
			base_location.slab_id,
			descriptor.table_schema_generation,
			descriptor.key.arrow_type_tag,
			entries.clone(),
		)?;

		bench_entry_extraction(&config, rows, &descriptor, &batch, base_location)?;
		bench_segment_build(&config, rows, &descriptor, &entries)?;
		bench_segment_codec(&config, rows, &segment)?;
		bench_query_filter(&config, rows, &segment);
		bench_candidate_revalidation(&config, rows, &descriptor, &batch, &segment)?;
	}

	Ok(())
}

impl BenchConfig {
	fn parse() -> Result<Self, Box<dyn std::error::Error>> {
		let mut rows = DEFAULT_ROWS.to_vec();
		let mut measure_iters = MEASURE_ITERS;
		let mut args = std::env::args().skip(1);
		while let Some(arg) = args.next() {
			match arg.as_str() {
				"--quick" => {
					rows = QUICK_ROWS.to_vec();
					measure_iters = QUICK_MEASURE_ITERS;
				}
				"--full" => {
					rows = FULL_ROWS.to_vec();
					measure_iters = MEASURE_ITERS;
				}
				"--rows" => {
					let value = args.next().ok_or("--rows requires a value")?;
					rows = vec![value.parse()?];
				}
				"--bench" => {}
				_ => {}
			}
		}
		Ok(Self {
			rows,
			measure_iters,
		})
	}
}

fn bench_entry_extraction(
	config: &BenchConfig,
	rows: usize,
	descriptor: &SecondaryIndexDescriptor,
	batch: &RecordBatch,
	base_location: RecordLocation,
) -> std::io::Result<()> {
	let started = Instant::now();
	let mut emitted = 0usize;
	for _ in 0..config.measure_iters {
		let entries = secondary_index_entries_from_storage_batch(descriptor, batch, base_location)?;
		emitted = emitted.saturating_add(entries.len());
		black_box(entries);
	}
	report("entry-extraction", rows, emitted, started.elapsed());
	Ok(())
}

fn bench_segment_build(
	config: &BenchConfig,
	rows: usize,
	descriptor: &SecondaryIndexDescriptor,
	entries: &[wendelon::storage::SecondaryIndexEntry],
) -> std::io::Result<()> {
	let identity = SecondaryIndexIdentity::from_descriptor(TenantId::new(52), descriptor);
	let started = Instant::now();
	for _ in 0..config.measure_iters {
		let segment = SecondaryIndexSegment::build(
			identity,
			SlabId::new(52),
			descriptor.table_schema_generation,
			descriptor.key.arrow_type_tag,
			entries.to_vec(),
		)?;
		black_box(segment);
	}
	report(
		"segment-build",
		rows,
		entries.len().saturating_mul(config.measure_iters),
		started.elapsed(),
	);
	Ok(())
}

fn bench_segment_codec(
	config: &BenchConfig,
	rows: usize,
	segment: &SecondaryIndexSegment,
) -> std::io::Result<()> {
	let started = Instant::now();
	let mut bytes = 0usize;
	for _ in 0..config.measure_iters {
		let encoded = segment.encode()?;
		bytes = bytes.saturating_add(encoded.len());
		let decoded = SecondaryIndexSegment::decode(&encoded)?;
		black_box(decoded);
	}
	report("segment-codec", rows, bytes, started.elapsed());
	Ok(())
}

fn bench_query_filter(config: &BenchConfig, rows: usize, segment: &SecondaryIndexSegment) {
	let lower_idx = segment.entries.len() / 3;
	let upper_idx = segment.entries.len().saturating_mul(2) / 3;
	let query = IndexQuery::Range {
		lower: segment.entries.get(lower_idx).map(|entry| IndexBound {
			encoded_key: entry.encoded_key.clone(),
			inclusive: true,
		}),
		upper: segment.entries.get(upper_idx).map(|entry| IndexBound {
			encoded_key: entry.encoded_key.clone(),
			inclusive: true,
		}),
	};
	let started = Instant::now();
	let mut matched = 0usize;
	for _ in 0..config.measure_iters {
		matched = matched.saturating_add(
			segment
				.entries
				.iter()
				.filter(|entry| secondary_index_entry_matches_query(entry, &query))
				.count(),
		);
	}
	report("query-filter", rows, matched, started.elapsed());
	black_box(matched);
}

fn bench_candidate_revalidation(
	config: &BenchConfig,
	rows: usize,
	descriptor: &SecondaryIndexDescriptor,
	batch: &RecordBatch,
	segment: &SecondaryIndexSegment,
) -> std::io::Result<()> {
	let identity = segment.identity();
	let candidates = segment.entries.iter().take(1024).collect::<Vec<_>>();
	let started = Instant::now();
	let mut accepted = 0usize;
	for _ in 0..config.measure_iters {
		for entry in &candidates {
			if revalidate_secondary_index_candidate(
				descriptor,
				identity,
				entry,
				batch,
				Some(entry.record_location),
			)? == SecondaryIndexCandidateValidation::Accepted
			{
				accepted = accepted.saturating_add(1);
			}
		}
	}
	report(
		"candidate-revalidation",
		rows,
		candidates.len().saturating_mul(config.measure_iters),
		started.elapsed(),
	);
	black_box(accepted);
	Ok(())
}

fn table_descriptor() -> Result<TableSchemaDescriptorV1, Box<dyn std::error::Error>> {
	Ok(TableSchemaDescriptorV1::new(
		TableObjectId::new([0x52; 16]),
		1,
		"bench",
		"secondary_index",
		&Schema::new(vec![
			Field::new("id", DataType::UInt64, false),
			Field::new("age", DataType::Int64, true),
		]),
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
		LocalityPolicyV1::default_table_spread(),
		SchemaEvolutionPolicyV1::AdditiveOnly,
	)?)
}

fn secondary_index_descriptor(table: &TableSchemaDescriptorV1) -> SecondaryIndexDescriptor {
	SecondaryIndexDescriptor {
		index_object_id: IndexObjectId::new([0x53; 16]),
		table_object_id: table.table_object_id,
		namespace: table.namespace.clone(),
		index_name: "secondary_index_age_idx".to_owned(),
		table_schema_generation: table.schema_generation,
		logical_schema_fingerprint: table.logical_schema_fingerprint,
		key: SecondaryIndexKeyDescriptor {
			column_name: "age".to_owned(),
			column_ordinal: 1,
			arrow_type_tag: IndexArrowTypeTag::Int64,
			encoding: IndexKeyEncoding::OrderedScalar,
		},
	}
}

fn indexed_batch(rows: usize, row_hlc: HybridTimestamp) -> std::io::Result<RecordBatch> {
	let ids = UInt64Array::from_iter_values(0..rows as u64);
	let ages = Int64Array::from(
		(0..rows)
			.map(|idx| {
				if idx % 17 == 0 {
					None
				} else {
					Some((idx as i64 % 2_000) - 1_000)
				}
			})
			.collect::<Vec<_>>(),
	);
	let batch = RecordBatch::try_new(
		Arc::new(Schema::new(vec![
			Field::new("id", DataType::UInt64, false),
			Field::new("age", DataType::Int64, true),
		])),
		vec![Arc::new(ids), Arc::new(ages)],
	)
	.map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
	let row_indices = (0..batch.num_rows()).collect::<Vec<_>>();
	append_storage_synthetic_columns(
		&batch,
		&row_indices,
		&TableWriteAdmission {
			canonical_primary_keys: (0..rows)
				.map(|idx| (idx as u64).to_le_bytes().to_vec())
				.collect(),
			key_identities: (0..rows).map(identity).collect(),
			localities: (0..rows).map(|idx| Locality::new(idx as u64)).collect(),
		},
		row_hlc,
		Some(TableObjectId::new([0x52; 16])),
	)
}

fn identity(idx: usize) -> Identity {
	let mut bytes = [0u8; 32];
	bytes[..8].copy_from_slice(&(idx as u64).to_le_bytes());
	Identity::new(bytes)
}

fn location(slab_id: SlabId, row_hlc: HybridTimestamp, row: RecordRowIndex) -> RecordLocation {
	RecordLocation {
		slab_id,
		offset: 0,
		length: 512,
		hlc: row_hlc,
		record_row_index: row,
		is_tombstone: false,
	}
}

fn hlc(physical_ms: u64, node_id: u32) -> HybridTimestamp {
	HybridTimestamp {
		physical_ms,
		logical: 0,
		node_id,
	}
}

fn report(name: &str, rows: usize, logical_ops: usize, elapsed: std::time::Duration) {
	let elapsed_ns = elapsed.as_nanos().max(1);
	let ns_per_op = elapsed_ns as f64 / logical_ops.max(1) as f64;
	println!(
		"{name:>24} rows={rows:>8} ops={logical_ops:>10} elapsed_ms={:.3} ns_per_op={:.2}",
		elapsed.as_secs_f64() * 1000.0,
		ns_per_op,
	);
}
