//! ADR 056 row-group fetch benchmark.
//!
//! `cargo bench --bench row_group_fetch -- --quick`.
//!
//! This harness isolates the pure pieces ADR 056 needs before the full
//! promotion matrix: writer overhead, page-directory planning, Phase B
//! survival-mask evaluation, and masked Phase C materialization.

use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::hint::black_box;
use std::io::{Error, ErrorKind};
use std::sync::Arc;
use std::time::{Duration, Instant};
use wendelon::schema::pbc::{OP_GT, OP_PUSH_COL, OP_PUSH_INT, Program};
use wendelon::schema::{
	HybridTimestamp, Identity, Locality, LocalityPolicyV1, MultiRowLayoutDescriptor,
	PrimaryKeyDescriptorV1, RecordBodyByteLen, RecordBodyByteOffset, RecordBodyCompressionPolicy,
	RecordLayoutPolicy, RowCount, RowGroupIndex, RowGroupRowCount, RowIdentityPolicy,
	SchemaEvolutionPolicyV1, SlabId, TableObjectId,
};
use wendelon::storage::multirow::MultiRowSourceRow;
use wendelon::storage::{
	ColumnFetchRequirement, ColumnOrdinalSet, PhaseBOutcome, PlaintextByteRange,
	RecordBufferDirectory, RowGroupWriterConfig, TieredRecordId,
	apply_phase_b_masks_to_projected_row_groups, build_page_directory_for_ipc_stream,
	default_stats_supported_types, encode_multirow_from_survivors_with_row_group_config,
	encode_record_batches_bytes, parse_record_buffer_directory,
	project_record_batch_for_requirement, row_group_stats_for_batch,
	selected_row_group_record_body_ranges, serialize_page_directory_value,
};

fn main() -> std::io::Result<()> {
	let config = BenchConfig::from_args();
	for case in config.cases.iter().copied() {
		let batch = bench_batch(case.rows, case.columns, case.shape)?;
		let descriptor = descriptor(batch.schema().as_ref(), case.columns)?;
		bench_writer(&config, case, &batch, &descriptor)?;
		bench_writer_target_sweep(&config, case, &batch, &descriptor)?;
		bench_stats_compute(&config, case, &batch)?;
		bench_directory_build_serialize(&config, case, &batch, &descriptor)?;
		let fixture = encoded_fixture(
			case.rows,
			&batch,
			&descriptor,
			RecordBodyCompressionPolicy::Uncompressed,
			&forced_row_group_config_for(true, 4_096, true),
		)?;
		bench_directory_size(case, &fixture)?;
		bench_selected_bytes(case, &fixture)?;
		bench_planner(&config, case, &fixture.directory)?;
		bench_phase_b_mask(&config, case, &batch)?;
		bench_masked_materialization(&config, case, &batch, &fixture.directory)?;
	}
	bench_compression_ratio(&config)?;
	Ok(())
}

#[derive(Clone, Copy, Debug)]
enum BenchDataShape {
	Int64,
	Float64,
	Utf8,
	Mixed,
}

impl BenchDataShape {
	fn as_str(self) -> &'static str {
		match self {
			Self::Int64 => "int64",
			Self::Float64 => "float64",
			Self::Utf8 => "utf8",
			Self::Mixed => "mixed",
		}
	}
}

#[derive(Clone, Copy, Debug)]
struct BenchCase {
	rows: usize,
	columns: usize,
	shape: BenchDataShape,
}

#[derive(Clone, Debug)]
struct EncodedFixture {
	directory: RecordBufferDirectory,
	encoded_bytes: u64,
	plaintext_bytes: u64,
	directory_value_bytes: usize,
}

#[derive(Clone, Debug)]
struct BenchConfig {
	cases: Vec<BenchCase>,
	iterations: usize,
}

impl BenchConfig {
	fn from_args() -> Self {
		let quick = std::env::args().any(|arg| arg == "--quick");
		if quick {
			Self {
				cases: vec![BenchCase {
					rows: 8_192,
					columns: 8,
					shape: BenchDataShape::Int64,
				}],
				iterations: 25,
			}
		} else {
			Self {
				cases: vec![
					BenchCase {
						rows: 8_192,
						columns: 50,
						shape: BenchDataShape::Int64,
					},
					BenchCase {
						rows: 16_384,
						columns: 50,
						shape: BenchDataShape::Int64,
					},
					// The production MultiRow descriptor caps one record body at
					// 8 MiB. Keep the max-row-count case legal while still
					// exercising the 65,536-row directory and mask paths.
					BenchCase {
						rows: 65_536,
						columns: 4,
						shape: BenchDataShape::Int64,
					},
				],
				iterations: 100,
			}
		}
	}
}

fn descriptor(
	schema: &Schema,
	columns: usize,
) -> std::io::Result<wendelon::schema::TableSchemaDescriptorV1> {
	wendelon::schema::TableSchemaDescriptorV1::new_with_layout(
		TableObjectId::new([0x56; 16]),
		1,
		"bench",
		"row_group_fetch",
		schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("c0")),
		LocalityPolicyV1::IdentityProjection,
		RecordLayoutPolicy::MultiRow(
			MultiRowLayoutDescriptor::new(4_096, 4_096, 8 * 1024 * 1024).map_err(|err| {
				Error::new(
					ErrorKind::InvalidInput,
					format!("bench multi-row layout rejected: {err}"),
				)
			})?,
		),
		SchemaEvolutionPolicyV1::AdditiveOnly,
	)
	.map_err(|err| {
		Error::new(
			ErrorKind::InvalidInput,
			format!("bench descriptor rejected for {columns} columns: {err}"),
		)
	})
}

fn bench_batch(rows: usize, columns: usize, shape: BenchDataShape) -> std::io::Result<RecordBatch> {
	let fields = (0..columns)
		.map(|column| Field::new(format!("c{column}"), column_type(shape, column), false))
		.collect::<Vec<_>>();
	let arrays = (0..columns)
		.map(|column| bench_array(rows, column_type(shape, column), column))
		.collect::<Vec<_>>();
	RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
		.map_err(|err| Error::new(ErrorKind::InvalidData, format!("bench batch: {err}")))
}

fn column_type(shape: BenchDataShape, column: usize) -> DataType {
	match shape {
		BenchDataShape::Int64 => DataType::Int64,
		BenchDataShape::Float64 => {
			if column == 0 {
				DataType::Int64
			} else {
				DataType::Float64
			}
		}
		BenchDataShape::Utf8 => {
			if column == 0 {
				DataType::Int64
			} else {
				DataType::Utf8
			}
		}
		BenchDataShape::Mixed => match column % 4 {
			0 => DataType::Int64,
			1 => DataType::Float64,
			2 => DataType::Utf8,
			_ => DataType::Boolean,
		},
	}
}

fn bench_array(rows: usize, data_type: DataType, column: usize) -> ArrayRef {
	match data_type {
		DataType::Int64 => {
			let values = (0..rows)
				.map(|row| ((row as i64) * 17) + column as i64)
				.collect::<Vec<_>>();
			Arc::new(Int64Array::from(values))
		}
		DataType::Float64 => {
			let values = (0..rows)
				.map(|row| ((row as f64) * 1.25) + column as f64)
				.collect::<Vec<_>>();
			Arc::new(Float64Array::from(values))
		}
		DataType::Utf8 => {
			let values = (0..rows)
				.map(|row| format!("value-{}-{}", column, row % 1024))
				.collect::<Vec<_>>();
			Arc::new(StringArray::from(values))
		}
		DataType::Boolean => {
			let values = (0..rows)
				.map(|row| (row + column) % 3 == 0)
				.collect::<Vec<_>>();
			Arc::new(BooleanArray::from(values))
		}
		_ => unreachable!("bench data type is pinned"),
	}
}

fn source_rows(rows: usize) -> Vec<MultiRowSourceRow> {
	(0..rows)
		.map(|row| MultiRowSourceRow {
			source_row_index: row,
			key_identity: Identity::new(*blake3::hash(&(row as u64).to_le_bytes()).as_bytes()),
			routing_locality: Locality::new((row % 16) as u64),
			row_hlc: HybridTimestamp {
				physical_ms: 1,
				logical: row as u32,
				node_id: 1,
			},
			is_tombstone: false,
			table_object_id: Some(TableObjectId::new([0x56; 16])),
		})
		.collect()
}

fn row_group_config_for(
	enabled: bool,
	target_row_group_rows: u32,
	stats_enabled: bool,
) -> RowGroupWriterConfig {
	let stats_supported_types = if stats_enabled {
		default_stats_supported_types()
	} else {
		Default::default()
	};
	RowGroupWriterConfig {
		enabled,
		target_row_group_rows: RowGroupRowCount::new(target_row_group_rows),
		max_row_group_rows: RowGroupRowCount::new(target_row_group_rows),
		stats_supported_types,
		..RowGroupWriterConfig::default()
	}
}

fn forced_row_group_config_for(
	enabled: bool,
	target_row_group_rows: u32,
	stats_enabled: bool,
) -> RowGroupWriterConfig {
	RowGroupWriterConfig {
		min_paged_record_rows: RowCount::new(4_096),
		min_paged_record_bytes: RecordBodyByteLen::new(1),
		..row_group_config_for(enabled, target_row_group_rows, stats_enabled)
	}
}

fn bench_writer(
	config: &BenchConfig,
	case: BenchCase,
	batch: &RecordBatch,
	descriptor: &wendelon::schema::TableSchemaDescriptorV1,
) -> std::io::Result<()> {
	let source_rows = source_rows(case.rows);
	for (scenario, row_group_config) in [
		("writer_disabled", row_group_config_for(false, 4_096, false)),
		(
			"writer_enabled_stats",
			row_group_config_for(true, 4_096, true),
		),
		(
			"writer_enabled_no_stats",
			row_group_config_for(true, 4_096, false),
		),
	] {
		let elapsed = bench(config.iterations, || {
			let encoded = encode_multirow_from_survivors_with_row_group_config(
				black_box(batch),
				black_box(&source_rows),
				black_box(descriptor),
				RecordBodyCompressionPolicy::Uncompressed,
				black_box(&row_group_config),
			)
			.expect("bench writer encode");
			black_box(encoded.frame.len());
		});
		let fixture = encoded_fixture(
			case.rows,
			batch,
			descriptor,
			RecordBodyCompressionPolicy::Uncompressed,
			&row_group_config,
		)?;
		let row_groups = fixture
			.directory
			.page_directory
			.as_ref()
			.map(|directory| directory.row_groups.len())
			.unwrap_or(0);
		print_writer_result(
			scenario,
			case,
			row_group_config.enabled,
			row_groups,
			config.iterations,
			elapsed,
		);
	}
	Ok(())
}

fn bench_writer_target_sweep(
	config: &BenchConfig,
	case: BenchCase,
	batch: &RecordBatch,
	descriptor: &wendelon::schema::TableSchemaDescriptorV1,
) -> std::io::Result<()> {
	let source_rows = source_rows(case.rows);
	for target in [1_024, 4_096, 8_192, 16_384] {
		let row_group_config = forced_row_group_config_for(true, target, true);
		let elapsed = bench(config.iterations, || {
			let encoded = encode_multirow_from_survivors_with_row_group_config(
				black_box(batch),
				black_box(&source_rows),
				black_box(descriptor),
				RecordBodyCompressionPolicy::Uncompressed,
				black_box(&row_group_config),
			)
			.expect("bench target writer encode");
			black_box(encoded.frame.len());
		});
		let fixture = encoded_fixture(
			case.rows,
			batch,
			descriptor,
			RecordBodyCompressionPolicy::Uncompressed,
			&row_group_config,
		)?;
		let row_groups = fixture
			.directory
			.page_directory
			.as_ref()
			.map(|directory| directory.row_groups.len())
			.unwrap_or(0);
		print_target_result(
			"writer_target_sweep",
			case,
			target,
			row_groups,
			config.iterations,
			elapsed,
		);
	}
	Ok(())
}

fn bench_stats_compute(
	config: &BenchConfig,
	case: BenchCase,
	batch: &RecordBatch,
) -> std::io::Result<()> {
	let batches = split_batches(batch, 4_096);
	let stats_supported_types = default_stats_supported_types();
	let elapsed = bench(config.iterations, || {
		let mut stats_count = 0_usize;
		for (index, row_group) in batches.iter().enumerate() {
			let stats = row_group_stats_for_batch(
				RowGroupIndex::new(index as u32),
				black_box(row_group),
				black_box(&stats_supported_types),
			)
			.expect("bench row-group stats");
			stats_count += stats.len();
		}
		black_box(stats_count);
	});
	print_result("stats_compute", case, true, config.iterations, elapsed);
	Ok(())
}

fn bench_directory_build_serialize(
	config: &BenchConfig,
	case: BenchCase,
	batch: &RecordBatch,
	descriptor: &wendelon::schema::TableSchemaDescriptorV1,
) -> std::io::Result<()> {
	let batches = split_batches(batch, 4_096);
	let bytes = encode_record_batches_bytes(&batches, RecordBodyCompressionPolicy::Uncompressed)?;
	for (scenario, stats_supported_types) in [
		(
			"directory_build_serialize_stats",
			default_stats_supported_types(),
		),
		("directory_build_serialize_no_stats", Default::default()),
	] {
		let elapsed = bench(config.iterations, || {
			let directory = build_page_directory_for_ipc_stream(
				TieredRecordId {
					slab_id: SlabId::new(56),
					record_offset: RecordBodyByteOffset::ZERO,
				},
				black_box(&bytes),
				black_box(&batches),
				descriptor.logical_schema_fingerprint,
				black_box(&stats_supported_types),
			)
			.expect("bench directory build");
			let value =
				serialize_page_directory_value(&directory).expect("bench directory serialize");
			black_box(value.len());
		});
		print_result(scenario, case, true, config.iterations, elapsed);
	}
	Ok(())
}

fn bench_directory_size(case: BenchCase, fixture: &EncodedFixture) -> std::io::Result<()> {
	let Some(page_directory) = fixture.directory.page_directory.as_ref() else {
		return Ok(());
	};
	let directory_fraction_micros = fraction_micros(
		fixture.directory_value_bytes as u64,
		fixture.encoded_bytes.max(1),
	);
	let probe_fraction_micros = fraction_micros(fixture.directory_value_bytes as u64, 64 * 1024);
	println!(
		"row_group_fetch_measure scenario=directory_size rows={} columns={} shape={} row_groups={} buffer_pages={} stats={} record_bytes={} plaintext_bytes={} directory_value_bytes={} directory_fraction_micros={} probe_64k_fraction_micros={}",
		case.rows,
		case.columns,
		case.shape.as_str(),
		page_directory.row_groups.len(),
		page_directory.buffer_pages.len(),
		page_directory.row_group_stats.len(),
		fixture.encoded_bytes,
		fixture.plaintext_bytes,
		fixture.directory_value_bytes,
		directory_fraction_micros,
		probe_fraction_micros,
	);
	Ok(())
}

fn bench_selected_bytes(case: BenchCase, fixture: &EncodedFixture) -> std::io::Result<()> {
	let Some(page_directory) = fixture.directory.page_directory.as_ref() else {
		return Ok(());
	};
	let total_groups = page_directory.row_groups.len();
	let group_counts = unique_counts([
		1,
		total_groups.saturating_add(3) / 4,
		total_groups.saturating_add(1) / 2,
		total_groups,
	]);
	let projection_counts = unique_counts([
		1,
		5_usize.min(case.columns),
		25_usize.min(case.columns),
		case.columns,
	]);
	for projection_count in projection_counts {
		let requirement = ColumnFetchRequirement {
			output_columns: ColumnOrdinalSet::from_columns(0..projection_count as u32),
			..ColumnFetchRequirement::default()
		};
		for group_count in &group_counts {
			let selected_groups = (0..*group_count)
				.map(|index| RowGroupIndex::new(index as u32))
				.collect::<Vec<_>>();
			let ranges = selected_row_group_record_body_ranges(
				&fixture.directory,
				page_directory,
				&requirement,
				&selected_groups,
			)
			.map_err(|err| Error::new(ErrorKind::InvalidData, format!("selected ranges: {err}")))?;
			let selected_bytes = ranges.iter().map(|range| range.len.get()).sum::<u64>();
			println!(
				"row_group_fetch_measure scenario=selected_bytes rows={} columns={} shape={} projection_columns={} selected_groups={} total_groups={} selected_ranges={} selected_bytes={} record_bytes={} selected_fraction_micros={}",
				case.rows,
				case.columns,
				case.shape.as_str(),
				projection_count,
				group_count,
				total_groups,
				ranges.len(),
				selected_bytes,
				fixture.encoded_bytes,
				fraction_micros(selected_bytes, fixture.encoded_bytes.max(1)),
			);
		}
	}
	Ok(())
}

fn encoded_fixture(
	rows: usize,
	batch: &RecordBatch,
	descriptor: &wendelon::schema::TableSchemaDescriptorV1,
	policy: RecordBodyCompressionPolicy,
	row_group_config: &RowGroupWriterConfig,
) -> std::io::Result<EncodedFixture> {
	let encoded = encode_multirow_from_survivors_with_row_group_config(
		batch,
		&source_rows(rows),
		descriptor,
		policy,
		row_group_config,
	)?;
	let directory = parse_record_buffer_directory(
		TieredRecordId {
			slab_id: SlabId::new(56),
			record_offset: RecordBodyByteOffset::ZERO,
		},
		PlaintextByteRange {
			start: 0,
			len: encoded.frame.len() as u64,
		},
		&encoded.frame,
		descriptor,
	)
	.map_err(|err| {
		Error::new(
			ErrorKind::InvalidData,
			format!("bench directory parse: {err}"),
		)
	})?;
	let directory_value_bytes = directory
		.page_directory
		.as_ref()
		.map(serialize_page_directory_value)
		.transpose()?
		.map(|value| value.len())
		.unwrap_or(0);
	Ok(EncodedFixture {
		directory,
		encoded_bytes: encoded.encoded_byte_len,
		plaintext_bytes: encoded.plaintext_byte_len,
		directory_value_bytes,
	})
}

fn bench_planner(
	config: &BenchConfig,
	case: BenchCase,
	directory: &RecordBufferDirectory,
) -> std::io::Result<()> {
	let page_directory = directory
		.page_directory
		.as_ref()
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "bench missing page directory"))?;
	let requirement = ColumnFetchRequirement {
		output_columns: ColumnOrdinalSet::from_columns([1]),
		..ColumnFetchRequirement::default()
	};
	let selected_groups = [RowGroupIndex::new(0)];
	let elapsed = bench(config.iterations, || {
		let ranges = selected_row_group_record_body_ranges(
			black_box(directory),
			black_box(page_directory),
			black_box(&requirement),
			black_box(&selected_groups),
		)
		.expect("bench row-group range selection");
		black_box(ranges.len());
	});
	print_result("planner_one_group", case, true, config.iterations, elapsed);
	Ok(())
}

fn bench_phase_b_mask(
	config: &BenchConfig,
	case: BenchCase,
	batch: &RecordBatch,
) -> std::io::Result<()> {
	let mut bytecode = vec![OP_PUSH_COL];
	bytecode.extend_from_slice(&0_u32.to_le_bytes());
	bytecode.push(OP_PUSH_INT);
	bytecode.extend_from_slice(&((case.rows as i64) / 2).to_le_bytes());
	bytecode.push(OP_GT);
	let program = Program::compile(&bytecode)?;
	let elapsed = bench(config.iterations, || {
		let mask = wendelon::schema::pbc::survival_mask(black_box(&program), black_box(batch))
			.expect("bench Phase B survival mask");
		black_box(mask.true_count());
	});
	print_result("phase_b_mask", case, true, config.iterations, elapsed);
	Ok(())
}

fn bench_masked_materialization(
	config: &BenchConfig,
	case: BenchCase,
	batch: &RecordBatch,
	directory: &RecordBufferDirectory,
) -> std::io::Result<()> {
	let page_directory = directory
		.page_directory
		.as_ref()
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "bench missing page directory"))?;
	let row_group_batch = batch.slice(0, 4_096.min(case.rows));
	let requirement = ColumnFetchRequirement {
		output_columns: ColumnOrdinalSet::from_columns([1]),
		..ColumnFetchRequirement::default()
	};
	let projected = project_record_batch_for_requirement(&row_group_batch, &requirement)?;
	let group = RowGroupIndex::new(0);
	let mut phase_b = PhaseBOutcome::default();
	phase_b
		.decoded_predicate_batches
		.insert(group, row_group_batch.clone());
	phase_b.survival_masks.insert(
		group,
		BooleanArray::from_iter((0..row_group_batch.num_rows()).map(|row| row % 4 == 0)),
	);
	let elapsed = bench(config.iterations, || {
		let out = apply_phase_b_masks_to_projected_row_groups(
			vec![(group, black_box(projected.clone()))],
			black_box(&phase_b),
			black_box(&[group]),
			black_box(page_directory),
		)
		.expect("bench masked materialization");
		black_box(out.batch.num_rows());
	});
	print_result(
		"masked_materialization",
		case,
		true,
		config.iterations,
		elapsed,
	);
	Ok(())
}

fn bench_compression_ratio(config: &BenchConfig) -> std::io::Result<()> {
	for shape in [
		BenchDataShape::Int64,
		BenchDataShape::Float64,
		BenchDataShape::Utf8,
		BenchDataShape::Mixed,
	] {
		let case = BenchCase {
			rows: 8_192,
			columns: 16,
			shape,
		};
		let batch = bench_batch(case.rows, case.columns, case.shape)?;
		let descriptor = descriptor(batch.schema().as_ref(), case.columns)?;
		let disabled = row_group_config_for(false, 4_096, false);
		let enabled = forced_row_group_config_for(true, 4_096, true);
		let single = encode_multirow_from_survivors_with_row_group_config(
			&batch,
			&source_rows(case.rows),
			&descriptor,
			RecordBodyCompressionPolicy::Lz4Frame,
			&disabled,
		)?;
		let row_group = encode_multirow_from_survivors_with_row_group_config(
			&batch,
			&source_rows(case.rows),
			&descriptor,
			RecordBodyCompressionPolicy::Lz4Frame,
			&enabled,
		)?;
		let loss_micros = if single.encoded_byte_len == 0 {
			0
		} else {
			((row_group.encoded_byte_len as i128 - single.encoded_byte_len as i128) * 1_000_000
				/ single.encoded_byte_len as i128) as i64
		};
		println!(
			"row_group_fetch_measure scenario=compression_ratio rows={} columns={} shape={} iterations={} single_lz4_bytes={} row_group_lz4_bytes={} single_plaintext_bytes={} row_group_plaintext_bytes={} row_group_loss_micros={}",
			case.rows,
			case.columns,
			case.shape.as_str(),
			config.iterations,
			single.encoded_byte_len,
			row_group.encoded_byte_len,
			single.plaintext_byte_len,
			row_group.plaintext_byte_len,
			loss_micros,
		);
	}
	Ok(())
}

fn split_batches(batch: &RecordBatch, target_rows: usize) -> Vec<RecordBatch> {
	let mut batches = Vec::new();
	let mut offset = 0_usize;
	while offset < batch.num_rows() {
		let len = target_rows.min(batch.num_rows() - offset);
		batches.push(batch.slice(offset, len));
		offset += len;
	}
	batches
}

fn unique_counts<const N: usize>(counts: [usize; N]) -> Vec<usize> {
	let mut counts = counts
		.into_iter()
		.filter(|count| *count > 0)
		.collect::<Vec<_>>();
	counts.sort_unstable();
	counts.dedup();
	counts
}

fn fraction_micros(numerator: u64, denominator: u64) -> u64 {
	numerator.saturating_mul(1_000_000) / denominator.max(1)
}

fn bench(iterations: usize, mut f: impl FnMut()) -> Duration {
	f();
	let started = Instant::now();
	for _ in 0..iterations {
		f();
	}
	started.elapsed()
}

fn print_result(
	scenario: &'static str,
	case: BenchCase,
	row_groups_enabled: bool,
	iterations: usize,
	elapsed: Duration,
) {
	let micros_per_op = elapsed.as_secs_f64() * 1_000_000.0 / iterations as f64;
	println!(
		"row_group_fetch_bench scenario={scenario} rows={} columns={} shape={} row_groups_enabled={row_groups_enabled} iterations={iterations} total_ms={:.3} micros_per_op={:.3}",
		case.rows,
		case.columns,
		case.shape.as_str(),
		elapsed.as_secs_f64() * 1_000.0,
		micros_per_op
	);
}

fn print_writer_result(
	scenario: &'static str,
	case: BenchCase,
	row_groups_enabled: bool,
	row_groups: usize,
	iterations: usize,
	elapsed: Duration,
) {
	let micros_per_op = elapsed.as_secs_f64() * 1_000_000.0 / iterations as f64;
	println!(
		"row_group_fetch_bench scenario={scenario} rows={} columns={} shape={} row_groups_enabled={row_groups_enabled} row_groups={} iterations={iterations} total_ms={:.3} micros_per_op={:.3}",
		case.rows,
		case.columns,
		case.shape.as_str(),
		row_groups,
		elapsed.as_secs_f64() * 1_000.0,
		micros_per_op
	);
}

fn print_target_result(
	scenario: &'static str,
	case: BenchCase,
	target_row_group_rows: u32,
	row_groups: usize,
	iterations: usize,
	elapsed: Duration,
) {
	let micros_per_op = elapsed.as_secs_f64() * 1_000_000.0 / iterations as f64;
	println!(
		"row_group_fetch_bench scenario={scenario} rows={} columns={} shape={} target_row_group_rows={} row_groups={} iterations={iterations} total_ms={:.3} micros_per_op={:.3}",
		case.rows,
		case.columns,
		case.shape.as_str(),
		target_row_group_rows,
		row_groups,
		elapsed.as_secs_f64() * 1_000.0,
		micros_per_op
	);
}
