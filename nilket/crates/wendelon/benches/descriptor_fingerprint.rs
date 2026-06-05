//! ADR 051 logical / physical descriptor fingerprint benchmark.
//!
//! `--quick` keeps the run small for smoke checks:
//! `cargo bench --bench descriptor_fingerprint -- --quick`.

use std::hint::black_box;
use std::time::{Duration, Instant};

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use wendelon::schema::{
	ColumnBloomSpecV1, ColumnHllSpecV1, ColumnSmallSetSpecV1, ColumnStatsPolicyV1, LocalityPolicy,
	MultiRowLayoutDescriptor, PrimaryKeyDescriptorV1, RecordLayoutPolicy, RowIdentityPolicy,
	SchemaEvolutionPolicyV1, TableObjectId, TableSchemaDescriptor,
};

const DEFAULT_ITERS: usize = 100_000;
const QUICK_ITERS: usize = 1_000;
const WARMUP_ITERS: usize = 2;
const MEASURE_ITERS: usize = 5;

#[derive(Clone, Debug)]
struct BenchConfig {
	iterations: usize,
	measure_iters: usize,
}

impl BenchConfig {
	fn parse() -> Self {
		let mut iterations = DEFAULT_ITERS;
		let mut measure_iters = MEASURE_ITERS;
		let mut args = std::env::args().skip(1);
		while let Some(arg) = args.next() {
			match arg.as_str() {
				"--quick" => {
					iterations = QUICK_ITERS;
					measure_iters = 2;
				}
				"--iters" => {
					if let Some(value) = args.next() {
						iterations = value.parse().expect("--iters expects a positive integer");
					}
				}
				"--bench" => {}
				_ => {}
			}
		}
		Self {
			iterations,
			measure_iters,
		}
	}
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
	let config = BenchConfig::parse();
	for case in descriptor_cases()? {
		bench_logical(&config, case.label, &case.descriptor)?;
		bench_physical(&config, case.label, &case.descriptor)?;
		bench_refresh(&config, case.label, &case.descriptor)?;
		bench_validate(&config, case.label, &case.descriptor)?;
		bench_equality(&config, case.label, &case.descriptor);
	}
	Ok(())
}

struct DescriptorCase {
	label: &'static str,
	descriptor: TableSchemaDescriptor,
}

fn descriptor_cases() -> Result<Vec<DescriptorCase>, Box<dyn std::error::Error>> {
	Ok(vec![
		DescriptorCase {
			label: "single-column-pk",
			descriptor: pk_descriptor(0x51, pk_schema())?,
		},
		DescriptorCase {
			label: "wide-pk",
			descriptor: pk_descriptor(0x52, wide_schema())?,
		},
		DescriptorCase {
			label: "append-only-multirow",
			descriptor: append_only_descriptor()?,
		},
		DescriptorCase {
			label: "content-hash",
			descriptor: content_hash_descriptor()?,
		},
		DescriptorCase {
			label: "stats-policy",
			descriptor: stats_policy_descriptor()?,
		},
	])
}

fn pk_schema() -> Schema {
	Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("region", DataType::LargeUtf8, true),
	])
}

fn wide_schema() -> Schema {
	let mut fields = vec![Field::new("id", DataType::Int64, false)];
	for idx in 0..64 {
		let data_type = match idx % 5 {
			0 => DataType::Int64,
			1 => DataType::UInt64,
			2 => DataType::LargeUtf8,
			3 => DataType::Boolean,
			_ => DataType::Timestamp(TimeUnit::Millisecond, None),
		};
		fields.push(Field::new(format!("field_{idx:02}"), data_type, true));
	}
	Schema::new(fields)
}

fn pk_descriptor(
	id_byte: u8,
	schema: Schema,
) -> Result<TableSchemaDescriptor, Box<dyn std::error::Error>> {
	Ok(TableSchemaDescriptor::new(
		TableObjectId::new([id_byte; 16]),
		1,
		"bench",
		"descriptor_fingerprint",
		&schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
		LocalityPolicy::default_table_spread(),
		SchemaEvolutionPolicyV1::AdditiveOnly,
	)?)
}

fn append_only_descriptor() -> Result<TableSchemaDescriptor, Box<dyn std::error::Error>> {
	let schema = Schema::new(vec![
		Field::new(
			"event_time",
			DataType::Timestamp(TimeUnit::Millisecond, None),
			false,
		),
		Field::new("payload", DataType::LargeBinary, true),
	]);
	Ok(TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([0x53; 16]),
		1,
		"bench",
		"descriptor_fingerprint_append_only",
		&schema,
		RowIdentityPolicy::AppendOnly,
		LocalityPolicy::default_table_spread(),
		RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::default()),
		SchemaEvolutionPolicyV1::AdditiveOnly,
	)?)
}

fn content_hash_descriptor() -> Result<TableSchemaDescriptor, Box<dyn std::error::Error>> {
	let schema = Schema::new(vec![
		Field::new("event_id", DataType::Int64, false),
		Field::new("payload", DataType::LargeBinary, true),
	]);
	Ok(TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([0x54; 16]),
		1,
		"bench",
		"descriptor_fingerprint_content_hash",
		&schema,
		RowIdentityPolicy::AppendOnlyContentHash,
		LocalityPolicy::IdentityProjection,
		RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::default()),
		SchemaEvolutionPolicyV1::AdditiveOnly,
	)?)
}

fn stats_policy_descriptor() -> Result<TableSchemaDescriptor, Box<dyn std::error::Error>> {
	let schema = Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("region", DataType::LargeUtf8, true),
		Field::new("value", DataType::UInt64, true),
	]);
	let mut descriptor = TableSchemaDescriptor::new(
		TableObjectId::new([0x55; 16]),
		1,
		"bench",
		"descriptor_fingerprint_stats",
		&schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
		LocalityPolicy::default_table_spread(),
		SchemaEvolutionPolicyV1::AdditiveOnly,
	)?;
	descriptor.stats_policy = ColumnStatsPolicyV1 {
		bloom_filters: vec![ColumnBloomSpecV1::new("region")],
		hll_distincts: vec![ColumnHllSpecV1::new("region")],
		small_sets: vec![ColumnSmallSetSpecV1::new("region")],
		heavy_hitters: Vec::new(),
		range_distributions: Vec::new(),
		column_group_top_k: Vec::new(),
	};
	descriptor.refresh_descriptor_fingerprints()?;
	Ok(descriptor)
}

fn bench_logical(
	config: &BenchConfig,
	shape: &str,
	descriptor: &TableSchemaDescriptor,
) -> Result<(), Box<dyn std::error::Error>> {
	bench_result(config, "logical", shape, || {
		black_box(descriptor.compute_logical_schema_fingerprint())
	})
}

fn bench_physical(
	config: &BenchConfig,
	shape: &str,
	descriptor: &TableSchemaDescriptor,
) -> Result<(), Box<dyn std::error::Error>> {
	let logical = descriptor.logical_schema_fingerprint;
	bench_result(config, "physical", shape, || {
		black_box(descriptor.compute_physical_descriptor_fingerprint(logical))
	})
}

fn bench_refresh(
	config: &BenchConfig,
	shape: &str,
	descriptor: &TableSchemaDescriptor,
) -> Result<(), Box<dyn std::error::Error>> {
	bench_result(config, "refresh", shape, || {
		let mut descriptor = descriptor.clone();
		black_box(descriptor.refresh_descriptor_fingerprints())
	})
}

fn bench_validate(
	config: &BenchConfig,
	shape: &str,
	descriptor: &TableSchemaDescriptor,
) -> Result<(), Box<dyn std::error::Error>> {
	bench_result(config, "validate", shape, || {
		black_box(descriptor.validate())
	})
}

fn bench_equality(config: &BenchConfig, shape: &str, descriptor: &TableSchemaDescriptor) {
	let logical = descriptor.logical_schema_fingerprint;
	let schema_generation = descriptor.schema_generation;
	bench_bool(config, "logical-equality", shape, || {
		black_box(
			black_box(descriptor.logical_schema_fingerprint) == black_box(logical)
				&& black_box(descriptor.schema_generation) == black_box(schema_generation),
		)
	});
}

fn bench_result<F, T>(
	config: &BenchConfig,
	mode: &str,
	shape: &str,
	mut f: F,
) -> Result<(), Box<dyn std::error::Error>>
where
	F: FnMut() -> std::io::Result<T>,
	T: Copy,
{
	for _ in 0..WARMUP_ITERS {
		for _ in 0..config.iterations {
			black_box(f()?);
		}
	}
	let mut elapsed = Duration::ZERO;
	for _ in 0..config.measure_iters {
		let started = Instant::now();
		for _ in 0..config.iterations {
			black_box(f()?);
		}
		elapsed += started.elapsed();
	}
	print_measurement(mode, shape, config, elapsed);
	Ok(())
}

fn bench_bool<F>(config: &BenchConfig, mode: &str, shape: &str, mut f: F)
where
	F: FnMut() -> bool,
{
	for _ in 0..WARMUP_ITERS {
		for _ in 0..config.iterations {
			black_box(f());
		}
	}
	let mut elapsed = Duration::ZERO;
	for _ in 0..config.measure_iters {
		let started = Instant::now();
		for _ in 0..config.iterations {
			black_box(f());
		}
		elapsed += started.elapsed();
	}
	print_measurement(mode, shape, config, elapsed);
}

fn print_measurement(mode: &str, shape: &str, config: &BenchConfig, elapsed: Duration) {
	let total_ops = config.iterations * config.measure_iters;
	let nanos_per_op = elapsed.as_nanos() as f64 / total_ops as f64;
	println!(
		"descriptor_fingerprint mode={mode} shape={shape} iterations={total_ops} total_ms={:.3} ns_per_op={:.1}",
		elapsed.as_secs_f64() * 1_000.0,
		nanos_per_op
	);
}
