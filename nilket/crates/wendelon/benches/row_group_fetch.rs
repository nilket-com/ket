//! ADR 056 row-group fetch benchmark.
//!
//! `cargo bench --bench row_group_fetch -- --quick`.
//!
//! This harness isolates the pure pieces ADR 056 needs before the full
//! promotion matrix: writer overhead, page-directory planning, Phase B
//! survival-mask evaluation, and masked Phase C materialization.

use arrow::array::{ArrayRef, BooleanArray, Int64Array};
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
	apply_phase_b_masks_to_projected_row_groups,
	encode_multirow_from_survivors_with_row_group_config, parse_record_buffer_directory,
	project_record_batch_for_requirement, selected_row_group_record_body_ranges,
};

fn main() -> std::io::Result<()> {
	let config = BenchConfig::from_args();
	for rows in config.rows.iter().copied() {
		let batch = bench_batch(rows, config.columns)?;
		let descriptor = descriptor(batch.schema().as_ref(), config.columns)?;
		bench_writer(&config, rows, &batch, &descriptor)?;
		let directory = encoded_directory(rows, &batch, &descriptor)?;
		bench_planner(&config, rows, &directory)?;
		bench_phase_b_mask(&config, rows, &batch)?;
		bench_masked_materialization(&config, rows, &batch, &directory)?;
	}
	Ok(())
}

#[derive(Clone, Debug)]
struct BenchConfig {
	rows: Vec<usize>,
	columns: usize,
	iterations: usize,
}

impl BenchConfig {
	fn from_args() -> Self {
		let quick = std::env::args().any(|arg| arg == "--quick");
		if quick {
			Self {
				rows: vec![8_192],
				columns: 8,
				iterations: 25,
			}
		} else {
			Self {
				rows: vec![8_192, 65_536],
				columns: 50,
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

fn bench_batch(rows: usize, columns: usize) -> std::io::Result<RecordBatch> {
	let fields = (0..columns)
		.map(|column| Field::new(format!("c{column}"), DataType::Int64, false))
		.collect::<Vec<_>>();
	let arrays = (0..columns)
		.map(|column| {
			let values = (0..rows)
				.map(|row| ((row as i64) * 17) + column as i64)
				.collect::<Vec<_>>();
			Arc::new(Int64Array::from(values)) as ArrayRef
		})
		.collect::<Vec<_>>();
	RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
		.map_err(|err| Error::new(ErrorKind::InvalidData, format!("bench batch: {err}")))
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

fn row_group_config(enabled: bool) -> RowGroupWriterConfig {
	RowGroupWriterConfig {
		enabled,
		target_row_group_rows: RowGroupRowCount::new(4_096),
		max_row_group_rows: RowGroupRowCount::new(4_096),
		min_paged_record_rows: RowCount::new(4_096),
		min_paged_record_bytes: RecordBodyByteLen::new(1),
		..RowGroupWriterConfig::default()
	}
}

fn bench_writer(
	config: &BenchConfig,
	rows: usize,
	batch: &RecordBatch,
	descriptor: &wendelon::schema::TableSchemaDescriptorV1,
) -> std::io::Result<()> {
	let source_rows = source_rows(rows);
	for enabled in [false, true] {
		let row_group_config = row_group_config(enabled);
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
		print_result("writer", rows, enabled, config.iterations, elapsed);
	}
	Ok(())
}

fn encoded_directory(
	rows: usize,
	batch: &RecordBatch,
	descriptor: &wendelon::schema::TableSchemaDescriptorV1,
) -> std::io::Result<RecordBufferDirectory> {
	let encoded = encode_multirow_from_survivors_with_row_group_config(
		batch,
		&source_rows(rows),
		descriptor,
		RecordBodyCompressionPolicy::Uncompressed,
		&row_group_config(true),
	)?;
	parse_record_buffer_directory(
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
	})
}

fn bench_planner(
	config: &BenchConfig,
	rows: usize,
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
	print_result("planner_one_group", rows, true, config.iterations, elapsed);
	Ok(())
}

fn bench_phase_b_mask(
	config: &BenchConfig,
	rows: usize,
	batch: &RecordBatch,
) -> std::io::Result<()> {
	let mut bytecode = vec![OP_PUSH_COL];
	bytecode.extend_from_slice(&0_u32.to_le_bytes());
	bytecode.push(OP_PUSH_INT);
	bytecode.extend_from_slice(&((rows as i64) / 2).to_le_bytes());
	bytecode.push(OP_GT);
	let program = Program::compile(&bytecode)?;
	let elapsed = bench(config.iterations, || {
		let mask = wendelon::schema::pbc::survival_mask(black_box(&program), black_box(batch))
			.expect("bench Phase B survival mask");
		black_box(mask.true_count());
	});
	print_result("phase_b_mask", rows, true, config.iterations, elapsed);
	Ok(())
}

fn bench_masked_materialization(
	config: &BenchConfig,
	rows: usize,
	batch: &RecordBatch,
	directory: &RecordBufferDirectory,
) -> std::io::Result<()> {
	let page_directory = directory
		.page_directory
		.as_ref()
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "bench missing page directory"))?;
	let row_group_batch = batch.slice(0, 4_096.min(rows));
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
		rows,
		true,
		config.iterations,
		elapsed,
	);
	Ok(())
}

fn bench(iterations: usize, mut f: impl FnMut()) -> Duration {
	let started = Instant::now();
	for _ in 0..iterations {
		f();
	}
	started.elapsed()
}

fn print_result(
	scenario: &'static str,
	rows: usize,
	row_groups_enabled: bool,
	iterations: usize,
	elapsed: Duration,
) {
	let micros_per_op = elapsed.as_secs_f64() * 1_000_000.0 / iterations as f64;
	println!(
		"row_group_fetch_bench scenario={scenario} rows={rows} row_groups_enabled={row_groups_enabled} iterations={iterations} total_ms={:.3} micros_per_op={:.3}",
		elapsed.as_secs_f64() * 1_000.0,
		micros_per_op
	);
}
