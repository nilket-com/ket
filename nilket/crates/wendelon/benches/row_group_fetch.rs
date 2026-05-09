//! ADR 056 row-group fetch benchmark.
//!
//! `cargo bench --bench row_group_fetch -- --quick`.
//!
//! This harness isolates the pure pieces ADR 056 needs before the full
//! promotion matrix: writer overhead, page-directory planning, Phase B
//! survival-mask evaluation, and masked Phase C materialization.

use arrow::array::{
	Array, ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray, UInt8Array, UInt16Array,
	UInt32Array,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::BTreeSet;
use std::hint::black_box;
use std::io::{Error, ErrorKind};
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use wendelon::crypto::{
	SLAB_AEAD_TAG_BYTES, SLAB_ENCRYPTION_CHUNK_SIZES, TieredOffsetIndexEntry, TieredPlaintextRange,
	TieredSlabObjectMetadata, test_support::seeded_dev_crypto_manager,
};
use wendelon::schema::pbc::{ColumnPredicateLiteralV1, OP_GT, OP_PUSH_COL, OP_PUSH_INT, Program};
use wendelon::schema::{
	BaseValue, BitsPerValue, BufferValueCount, ChunkSizeBytes, DictValueCount, HybridTimestamp,
	Identity, IdentityPrefix, IpcBufferIndex, IpcColumnOrdinal, Locality, LocalityPolicyV1,
	MultiRowLayoutDescriptor, PrimaryKeyDescriptorV1, RecordBodyByteLen, RecordBodyByteOffset,
	RecordBodyCompressionPolicy, RecordLayoutPolicy, RowCount, RowGroupIndex, RowGroupRowCount,
	RowIdentityPolicy, RunCount, SchemaEvolutionPolicyV1, SimdPageValueCount, SlabId,
	TableObjectId, TenantId,
};
use wendelon::storage::encoding::compute::{
	EncodingComputeConfig, PredicateAtom, PredicateAtomOp, evaluator_kind_for_cascade,
};
use wendelon::storage::encoding::directory::{
	page_stats_cascade_eligible, serialize_encoding_directory_value,
};
use wendelon::storage::encoding::page::expand_page_descriptors;
use wendelon::storage::encoding::predicate_page_fetch::{
	PagePredicateClassification, classify_page_stats_for_atom,
};
use wendelon::storage::multirow::MultiRowSourceRow;
use wendelon::storage::stats::fixed_width_page_stats_supported_type;
use wendelon::storage::{
	ColumnFetchRequirement, ColumnOrdinalSet, EncodingPageDescriptor, PhaseBOutcome,
	PlaintextByteRange, ROW_GROUP_POINT_READ_MIN_PAGED_RECORD_ROWS,
	ROW_GROUP_POINT_READ_TARGET_ROWS, ROW_GROUP_SCAN_MAX_ROWS, ROW_GROUP_SCAN_TARGET_ROWS,
	RecordBufferDirectory, RowGroupSizingProfile, RowGroupWriterConfig, TieredRecordId,
	apply_phase_b_masks_to_projected_row_groups, build_page_directory_for_ipc_stream,
	decode_projected_row_groups_from_plaintext_ranges, default_stats_supported_types,
	encode_multirow_from_survivors_with_row_group_and_encoding_config,
	encode_multirow_from_survivors_with_row_group_config, encode_record_batches_bytes,
	parse_record_buffer_directory, project_record_batch_for_requirement, row_group_stats_for_batch,
	selected_row_group_record_body_ranges, serialize_page_directory_value,
};
use wendelon::storage::{
	EncodingCascade, EncodingCascadeKind, EncodingLeafCascade, EncodingWriterConfig,
	MockObjectStore, MockObjectStoreConfig, ObjectMetadata, ObjectStore, RecordBodyByteRange,
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
	bench_sparse_projection_planner(&config)?;
	bench_sparse_projection_runtime(&config)?;
	bench_sparse_projection_runtime_projection_counts(&config)?;
	bench_sparse_projection_runtime_mixed_cascade(&config)?;
	bench_sparse_projection_runtime_fragmentation(&config)?;
	bench_sparse_projection_runtime_point_read(&config)?;
	bench_sparse_projection_runtime_determinism(&config)?;
	bench_adr061_chunk_layout_matrix(&config)?;
	bench_adr062_row_group_sizing_profile_matrix(&config)?;
	bench_adr063_page_level_predicate_stats_matrix(&config)?;
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

fn ordered_source_rows(rows: usize) -> Vec<MultiRowSourceRow> {
	(0..rows)
		.map(|row| {
			let mut key = [0_u8; 32];
			key[..8].copy_from_slice(&(row as u64).to_be_bytes());
			MultiRowSourceRow {
				source_row_index: row,
				key_identity: Identity::new(key),
				routing_locality: Locality::ZERO,
				row_hlc: HybridTimestamp {
					physical_ms: 60,
					logical: row as u32,
					node_id: 1,
				},
				is_tombstone: false,
				table_object_id: Some(TableObjectId::new([0x60; 16])),
			}
		})
		.collect()
}

#[derive(Clone, Copy, Debug)]
enum SparseProjectionCascadeFixture {
	BitPacked,
	Dictionary,
	Rle,
	Plain,
	Delta,
	BitPackedPlusPlain,
}

impl SparseProjectionCascadeFixture {
	const ALL: [Self; 5] = [
		Self::BitPacked,
		Self::Dictionary,
		Self::Rle,
		Self::Plain,
		Self::Delta,
	];

	fn as_str(self) -> &'static str {
		match self {
			Self::BitPacked => "bit_packed",
			Self::Dictionary => "dictionary",
			Self::Rle => "rle",
			Self::Plain => "plain",
			Self::Delta => "delta",
			Self::BitPackedPlusPlain => "bit_packed_plus_plain",
		}
	}

	fn enabled_cascades(self) -> BTreeSet<EncodingCascadeKind> {
		let cascades = match self {
			Self::BitPacked => vec![EncodingCascadeKind::Plain, EncodingCascadeKind::BitPacked],
			Self::Dictionary => vec![EncodingCascadeKind::Plain, EncodingCascadeKind::Dictionary],
			Self::Rle => vec![EncodingCascadeKind::Plain, EncodingCascadeKind::Rle],
			Self::Plain => vec![EncodingCascadeKind::Plain, EncodingCascadeKind::Plain],
			Self::Delta => vec![EncodingCascadeKind::Plain, EncodingCascadeKind::Delta],
			Self::BitPackedPlusPlain => {
				vec![EncodingCascadeKind::Plain, EncodingCascadeKind::BitPacked]
			}
		};
		cascades.into_iter().collect()
	}
}

#[derive(Clone, Copy, Debug)]
enum SparseProjectionMaskShape {
	SingleFirstRow,
	ClusteredFirstPage,
	ClusteredMiddleOnePercent,
	ClusteredFivePercent,
	ClusteredTwentyFivePercent,
	RandomOnePercent,
	All,
	None,
}

impl SparseProjectionMaskShape {
	const ALL: [Self; 7] = [
		Self::ClusteredFirstPage,
		Self::ClusteredMiddleOnePercent,
		Self::ClusteredFivePercent,
		Self::ClusteredTwentyFivePercent,
		Self::RandomOnePercent,
		Self::All,
		Self::None,
	];

	fn as_str(self) -> &'static str {
		match self {
			Self::SingleFirstRow => "single_first_row",
			Self::ClusteredFirstPage => "clustered_first_page",
			Self::ClusteredMiddleOnePercent => "clustered_middle_1p",
			Self::ClusteredFivePercent => "clustered_5p",
			Self::ClusteredTwentyFivePercent => "clustered_25p",
			Self::RandomOnePercent => "random_1p",
			Self::All => "all",
			Self::None => "none",
		}
	}
}

fn sparse_projection_bench_batch(
	rows: usize,
	cascade: SparseProjectionCascadeFixture,
) -> std::io::Result<RecordBatch> {
	let schema = Schema::new(vec![
		Field::new("c0", DataType::Int64, false),
		Field::new("c1", DataType::Int64, false),
		Field::new("c2", DataType::Int64, false),
	]);
	let payload = (0..rows)
		.map(|row| match cascade {
			SparseProjectionCascadeFixture::BitPacked => (row % 1024) as i64,
			SparseProjectionCascadeFixture::Dictionary => (row % 4) as i64,
			SparseProjectionCascadeFixture::Rle => 0,
			SparseProjectionCascadeFixture::Plain => {
				let mixed = (row as u64)
					.wrapping_mul(0x9E37_79B9_7F4A_7C15)
					.rotate_left(17);
				i64::from_le_bytes(mixed.to_le_bytes())
			}
			SparseProjectionCascadeFixture::BitPackedPlusPlain => 7,
			SparseProjectionCascadeFixture::Delta => row as i64,
		})
		.collect::<Vec<_>>();
	RecordBatch::try_new(
		Arc::new(schema),
		vec![
			Arc::new(Int64Array::from_iter_values(0..rows as i64)) as ArrayRef,
			Arc::new(Int64Array::from_iter_values(
				(0..rows).map(|row| row as i64),
			)) as ArrayRef,
			Arc::new(Int64Array::from_iter_values(payload)) as ArrayRef,
		],
	)
	.map_err(|err| Error::new(ErrorKind::InvalidData, format!("sparse bench batch: {err}")))
}

fn row_group_config_for(
	enabled: bool,
	target_row_group_rows: u32,
	stats_enabled: bool,
) -> RowGroupWriterConfig {
	let defaults = RowGroupWriterConfig::point_read();
	row_group_config_for_values(
		enabled,
		target_row_group_rows,
		defaults.min_paged_record_rows(),
		defaults.min_paged_record_bytes(),
		stats_enabled,
	)
}

fn row_group_config_for_values(
	enabled: bool,
	target_row_group_rows: u32,
	min_paged_record_rows: RowCount,
	min_paged_record_bytes: RecordBodyByteLen,
	stats_enabled: bool,
) -> RowGroupWriterConfig {
	let stats_supported_types = if stats_enabled {
		default_stats_supported_types()
	} else {
		Default::default()
	};
	let mut config = RowGroupWriterConfig::point_read_with_overrides(
		RowGroupRowCount::new(target_row_group_rows),
		RowGroupRowCount::new(target_row_group_rows),
		min_paged_record_rows,
		min_paged_record_bytes,
	)
	.with_enabled(enabled);
	config.stats_supported_types = stats_supported_types;
	config
}

fn row_group_profile_config_for(
	profile: RowGroupSizingProfile,
	enabled: bool,
	stats_enabled: bool,
) -> RowGroupWriterConfig {
	let mut config = RowGroupWriterConfig::with_profile(profile).with_enabled(enabled);
	if !stats_enabled {
		config.stats_supported_types = BTreeSet::new();
	}
	config
}

fn forced_row_group_config_for(
	enabled: bool,
	target_row_group_rows: u32,
	stats_enabled: bool,
) -> RowGroupWriterConfig {
	row_group_config_for_values(
		enabled,
		target_row_group_rows,
		RowCount::new(4_096),
		RecordBodyByteLen::new(1),
		stats_enabled,
	)
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

#[derive(Clone, Debug)]
struct SparseProjectionEncodedFixture {
	row_group_rows: usize,
	dense_bytes: u64,
	pages: Vec<EncodingPageDescriptor>,
	actual_cascade: &'static str,
}

#[derive(Clone, Copy, Debug)]
struct SparseProjectionEstimate {
	sparse_bytes: u64,
	sparse_ranges: usize,
	selected_simd_pages: usize,
	sparse_selected: bool,
	decline: &'static str,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct BenchObjectStoreSnapshot {
	get_range_calls: usize,
	get_range_bytes: u64,
	get_bytes_calls: usize,
	get_bytes_bytes: u64,
}

impl BenchObjectStoreSnapshot {
	fn delta_since(self, earlier: Self) -> Self {
		Self {
			get_range_calls: self.get_range_calls.saturating_sub(earlier.get_range_calls),
			get_range_bytes: self.get_range_bytes.saturating_sub(earlier.get_range_bytes),
			get_bytes_calls: self.get_bytes_calls.saturating_sub(earlier.get_bytes_calls),
			get_bytes_bytes: self.get_bytes_bytes.saturating_sub(earlier.get_bytes_bytes),
		}
	}
}

struct CountingBenchObjectStore {
	inner: Arc<dyn ObjectStore>,
	get_range_calls: AtomicUsize,
	get_range_bytes: AtomicU64,
	get_bytes_calls: AtomicUsize,
	get_bytes_bytes: AtomicU64,
}

impl CountingBenchObjectStore {
	fn new(inner: Arc<dyn ObjectStore>) -> Self {
		Self {
			inner,
			get_range_calls: AtomicUsize::new(0),
			get_range_bytes: AtomicU64::new(0),
			get_bytes_calls: AtomicUsize::new(0),
			get_bytes_bytes: AtomicU64::new(0),
		}
	}

	fn snapshot(&self) -> BenchObjectStoreSnapshot {
		BenchObjectStoreSnapshot {
			get_range_calls: self.get_range_calls.load(Ordering::Relaxed),
			get_range_bytes: self.get_range_bytes.load(Ordering::Relaxed),
			get_bytes_calls: self.get_bytes_calls.load(Ordering::Relaxed),
			get_bytes_bytes: self.get_bytes_bytes.load(Ordering::Relaxed),
		}
	}
}

#[async_trait]
impl ObjectStore for CountingBenchObjectStore {
	async fn put(&self, key: &str, bytes: &[u8]) -> std::io::Result<String> {
		self.inner.put(key, bytes).await
	}

	async fn head(&self, uri: &str) -> std::io::Result<ObjectMetadata> {
		self.inner.head(uri).await
	}

	async fn get_range(&self, uri: &str, range: Range<u64>) -> std::io::Result<Bytes> {
		self.get_range_calls.fetch_add(1, Ordering::Relaxed);
		self.get_range_bytes
			.fetch_add(range.end.saturating_sub(range.start), Ordering::Relaxed);
		self.inner.get_range(uri, range).await
	}

	async fn get_bytes(&self, uri: &str) -> std::io::Result<Bytes> {
		let bytes = self.inner.get_bytes(uri).await?;
		self.get_bytes_calls.fetch_add(1, Ordering::Relaxed);
		self.get_bytes_bytes
			.fetch_add(bytes.len() as u64, Ordering::Relaxed);
		Ok(bytes)
	}

	async fn list(&self, prefix: &str) -> std::io::Result<Vec<ObjectMetadata>> {
		self.inner.list(prefix).await
	}

	async fn delete(&self, uri: &str) -> std::io::Result<()> {
		self.inner.delete(uri).await
	}
}

struct SparseProjectionRuntimeFixture {
	row_group_rows: usize,
	record_plaintext_range: Range<usize>,
	frame: Vec<u8>,
	directory: RecordBufferDirectory,
	batch: RecordBatch,
	requirement: ColumnFetchRequirement,
	payload_columns: Vec<SparseProjectionRuntimeColumn>,
}

struct SparseProjectionRuntimeColumn {
	physical_ordinal: usize,
	buffer_range: RecordBodyByteRange,
	encoding: Option<wendelon::storage::BufferEncoding>,
	cascade: Option<EncodingCascade>,
	pages: Vec<EncodingPageDescriptor>,
}

struct SparseProjectionRuntimeDataset {
	rows: usize,
	plaintext: Vec<u8>,
	fixtures: Vec<SparseProjectionRuntimeFixture>,
}

struct SparseProjectionRuntimeOutcome {
	elapsed: Duration,
	output_rows: usize,
	get_range_calls: usize,
	get_range_bytes: u64,
	get_bytes_calls: usize,
	projection_plaintext_bytes: u64,
	projection_ciphertext_bytes: u64,
	selected_ranges: usize,
	selected_simd_pages: usize,
}

impl SparseProjectionRuntimeOutcome {
	fn accumulate(&mut self, other: Self) {
		self.elapsed = self.elapsed.saturating_add(other.elapsed);
		self.output_rows = self.output_rows.saturating_add(other.output_rows);
		self.get_range_calls = self.get_range_calls.saturating_add(other.get_range_calls);
		self.get_range_bytes = self.get_range_bytes.saturating_add(other.get_range_bytes);
		self.get_bytes_calls = self.get_bytes_calls.saturating_add(other.get_bytes_calls);
		self.projection_plaintext_bytes = self
			.projection_plaintext_bytes
			.saturating_add(other.projection_plaintext_bytes);
		self.projection_ciphertext_bytes = self
			.projection_ciphertext_bytes
			.saturating_add(other.projection_ciphertext_bytes);
		self.selected_ranges = self.selected_ranges.saturating_add(other.selected_ranges);
		self.selected_simd_pages = self
			.selected_simd_pages
			.saturating_add(other.selected_simd_pages);
	}
}

impl Default for SparseProjectionRuntimeOutcome {
	fn default() -> Self {
		Self {
			elapsed: Duration::ZERO,
			output_rows: 0,
			get_range_calls: 0,
			get_range_bytes: 0,
			get_bytes_calls: 0,
			projection_plaintext_bytes: 0,
			projection_ciphertext_bytes: 0,
			selected_ranges: 0,
			selected_simd_pages: 0,
		}
	}
}

const SPARSE_PROJECTION_RUNTIME_PAYLOAD_BYTES: usize = 4;
const SPARSE_PROJECTION_RUNTIME_RECORD_ROWS: usize = 65_536;

fn sparse_projection_descriptor(
	schema: &Schema,
) -> std::io::Result<wendelon::schema::TableSchemaDescriptorV1> {
	wendelon::schema::TableSchemaDescriptorV1::new_with_layout(
		TableObjectId::new([0x60; 16]),
		1,
		"bench",
		"sparse_projection_fetch",
		schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("c0")),
		LocalityPolicyV1::IdentityProjection,
		RecordLayoutPolicy::MultiRow(
			MultiRowLayoutDescriptor::new(4_096, 4_096, 8 * 1024 * 1024).map_err(|err| {
				Error::new(
					ErrorKind::InvalidInput,
					format!("sparse bench multi-row layout rejected: {err}"),
				)
			})?,
		),
		SchemaEvolutionPolicyV1::AdditiveOnly,
	)
	.map_err(|err| {
		Error::new(
			ErrorKind::InvalidInput,
			format!("sparse bench descriptor rejected: {err}"),
		)
	})
}

fn sparse_projection_encoded_fixture(
	rows: usize,
	cascade: SparseProjectionCascadeFixture,
) -> std::io::Result<SparseProjectionEncodedFixture> {
	let batch = sparse_projection_bench_batch(rows, cascade)?;
	let descriptor = sparse_projection_descriptor(batch.schema().as_ref())?;
	let row_group_rows = 1_024.min(rows);
	let mut row_group_config = RowGroupWriterConfig::point_read_with_overrides(
		RowGroupRowCount::new(1_024),
		RowGroupRowCount::new(1_024),
		RowCount::new(1_024),
		RecordBodyByteLen::new(1),
	)
	.with_enabled(true);
	row_group_config.stats_supported_types = BTreeSet::new();
	let encoding_config = EncodingWriterConfig {
		enabled: true,
		target_simd_page_values: wendelon::schema::SimdPageValueCount::new(256),
		min_buffer_bytes_for_encoding: RecordBodyByteLen::new(1),
		enabled_cascades: cascade.enabled_cascades(),
		..EncodingWriterConfig::default()
	};
	let encoded = encode_multirow_from_survivors_with_row_group_and_encoding_config(
		&batch,
		&ordered_source_rows(rows),
		&descriptor,
		RecordBodyCompressionPolicy::Uncompressed,
		&row_group_config,
		&encoding_config,
	)?;
	let directory = parse_record_buffer_directory(
		TieredRecordId {
			slab_id: SlabId::new(60),
			record_offset: RecordBodyByteOffset::ZERO,
		},
		PlaintextByteRange {
			start: 0,
			len: encoded.frame.len() as u64,
		},
		&encoded.frame,
		&descriptor,
	)
	.map_err(|err| {
		Error::new(
			ErrorKind::InvalidData,
			format!("sparse bench directory parse: {err}"),
		)
	})?;
	let page_directory = directory.page_directory.as_ref().ok_or_else(|| {
		Error::new(
			ErrorKind::InvalidData,
			"sparse bench missing page directory",
		)
	})?;
	let encoding_directory = directory.encoding_directory.as_ref();
	let requirement = ColumnFetchRequirement {
		output_columns: ColumnOrdinalSet::from_columns([2]),
		..ColumnFetchRequirement::default()
	};
	let selected_groups = [RowGroupIndex::ZERO];
	let dense_body_ranges = selected_row_group_record_body_ranges(
		&directory,
		page_directory,
		&requirement,
		&selected_groups,
	)
	.map_err(|err| Error::new(ErrorKind::InvalidData, format!("sparse bench dense: {err}")))?;
	let dense_bytes = dense_body_ranges
		.iter()
		.map(|range| range.len.get())
		.sum::<u64>();
	let Some(encoding_directory) = encoding_directory else {
		return Ok(SparseProjectionEncodedFixture {
			row_group_rows,
			dense_bytes,
			pages: Vec::new(),
			actual_cascade: "plain",
		});
	};
	let value_buffer_ordinal = directory
		.column_to_buffers
		.get(2)
		.and_then(|mapping| mapping.first_buffer_ordinal.get().checked_add(1))
		.map(wendelon::schema::IpcBufferIndex::new)
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "sparse bench payload buffer"))?;
	let payload_page = page_directory
		.buffer_pages
		.iter()
		.find(|page| {
			page.row_group_index == RowGroupIndex::ZERO
				&& page.ipc_column_ordinal == wendelon::schema::IpcColumnOrdinal::new(2)
				&& page.ipc_buffer_ordinal == value_buffer_ordinal
				&& page.range.len.get() > 0
		})
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "sparse bench payload page"))?;
	let payload_encoding = wendelon::storage::encoding::materialize::find_buffer_encoding(
		encoding_directory,
		RowGroupIndex::ZERO,
		value_buffer_ordinal,
		payload_page.range,
	);
	let Some(payload_encoding) = payload_encoding else {
		return Ok(SparseProjectionEncodedFixture {
			row_group_rows,
			dense_bytes,
			pages: Vec::new(),
			actual_cascade: "plain",
		});
	};
	let pages = wendelon::storage::encoding::page::expand_page_descriptors(payload_encoding)
		.map_err(|code| {
			Error::new(
				ErrorKind::InvalidData,
				format!("sparse bench page expansion rejected: {code:?}"),
			)
		})?;
	let actual_cascade = encoding_directory
		.cascades
		.get(payload_encoding.cascade_index.get() as usize)
		.map(|cascade| cascade.kind().as_str())
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "sparse bench payload cascade"))?;
	Ok(SparseProjectionEncodedFixture {
		row_group_rows,
		dense_bytes,
		pages,
		actual_cascade,
	})
}

fn sparse_projection_runtime_batch(
	rows: usize,
	cascade: SparseProjectionCascadeFixture,
	projection_columns: usize,
) -> std::io::Result<RecordBatch> {
	let mut fields = vec![
		Field::new("c0", DataType::UInt16, false),
		Field::new("c1", DataType::UInt16, false),
	];
	fields.extend(
		(0..projection_columns)
			.map(|index| Field::new(format!("c{}", index + 2), DataType::UInt32, false)),
	);
	let schema = Schema::new(fields);
	let mut columns = vec![
		Arc::new(UInt16Array::from_iter_values(
			(0..rows).map(|row| (row % 251) as u16),
		)) as ArrayRef,
		Arc::new(UInt16Array::from_iter_values(
			(0..rows).map(|row| (row % 251) as u16),
		)) as ArrayRef,
	];
	for projection_index in 0..projection_columns {
		let payload = (0..rows)
			.map(|row| sparse_projection_runtime_payload_value(row, projection_index, cascade))
			.collect::<Vec<_>>();
		columns.push(Arc::new(UInt32Array::from_iter_values(payload)) as ArrayRef);
	}
	RecordBatch::try_new(Arc::new(schema), columns).map_err(|err| {
		Error::new(
			ErrorKind::InvalidData,
			format!("sparse runtime batch: {err}"),
		)
	})
}

fn sparse_projection_runtime_payload_value(
	row: usize,
	projection_index: usize,
	cascade: SparseProjectionCascadeFixture,
) -> u32 {
	match cascade {
		SparseProjectionCascadeFixture::BitPacked => {
			((row + projection_index.saturating_mul(17)) % 1024) as u32
		}
		SparseProjectionCascadeFixture::BitPackedPlusPlain if projection_index == 0 => {
			(row % 1024) as u32
		}
		SparseProjectionCascadeFixture::Dictionary => ((row + projection_index) % 4) as u32,
		SparseProjectionCascadeFixture::Rle => projection_index as u32,
		SparseProjectionCascadeFixture::BitPackedPlusPlain => (row as u64)
			.wrapping_mul(37)
			.wrapping_add((projection_index as u64).wrapping_mul(101))
			.rotate_left(11) as u32,
		SparseProjectionCascadeFixture::Plain => (row as u64)
			.wrapping_mul(37)
			.wrapping_add((projection_index as u64).wrapping_mul(101))
			.rotate_left(11) as u32,
		SparseProjectionCascadeFixture::Delta => row.wrapping_add(projection_index) as u32,
	}
}

fn sparse_projection_runtime_fixture_at(
	rows: usize,
	cascade: SparseProjectionCascadeFixture,
	projection_columns: usize,
	record_plaintext_start: usize,
) -> std::io::Result<SparseProjectionRuntimeFixture> {
	let batch = sparse_projection_runtime_batch(rows, cascade, projection_columns)?;
	let descriptor = sparse_projection_descriptor(batch.schema().as_ref())?;
	let row_group_rows = 1_024.min(rows);
	let mut row_group_config = RowGroupWriterConfig::point_read_with_overrides(
		RowGroupRowCount::new(1_024),
		RowGroupRowCount::new(1_024),
		RowCount::new(1_024),
		RecordBodyByteLen::new(1),
	)
	.with_enabled(true);
	row_group_config.stats_supported_types = BTreeSet::new();
	let encoding_config = EncodingWriterConfig {
		enabled: true,
		target_simd_page_values: wendelon::schema::SimdPageValueCount::new(256),
		min_buffer_bytes_for_encoding: RecordBodyByteLen::new(1),
		enabled_cascades: cascade.enabled_cascades(),
		..EncodingWriterConfig::default()
	};
	let encoded = encode_multirow_from_survivors_with_row_group_and_encoding_config(
		&batch,
		&ordered_source_rows(rows),
		&descriptor,
		RecordBodyCompressionPolicy::Uncompressed,
		&row_group_config,
		&encoding_config,
	)?;
	let record_plaintext_start_u64 = u64::try_from(record_plaintext_start).map_err(|_| {
		Error::new(
			ErrorKind::InvalidInput,
			"sparse runtime record plaintext start exceeds u64",
		)
	})?;
	let directory = parse_record_buffer_directory(
		TieredRecordId {
			slab_id: SlabId::new(61),
			record_offset: RecordBodyByteOffset::new(record_plaintext_start_u64),
		},
		PlaintextByteRange {
			start: record_plaintext_start_u64,
			len: encoded.frame.len() as u64,
		},
		&encoded.frame,
		&descriptor,
	)
	.map_err(|err| {
		Error::new(
			ErrorKind::InvalidData,
			format!("sparse runtime directory parse: {err}"),
		)
	})?;
	let page_directory = directory.page_directory.as_ref().ok_or_else(|| {
		Error::new(
			ErrorKind::InvalidData,
			"sparse runtime missing page directory",
		)
	})?;
	let encoding_directory = directory.encoding_directory.as_ref().ok_or_else(|| {
		Error::new(
			ErrorKind::InvalidData,
			"sparse runtime missing encoding directory",
		)
	})?;
	let output_columns = (2..2 + projection_columns)
		.map(|column| {
			u32::try_from(column).map_err(|_| {
				Error::new(
					ErrorKind::InvalidInput,
					"sparse runtime projection column exceeds u32",
				)
			})
		})
		.collect::<std::io::Result<Vec<_>>>()?;
	let requirement = ColumnFetchRequirement {
		output_columns: ColumnOrdinalSet::from_columns(output_columns.iter().copied()),
		..ColumnFetchRequirement::default()
	};
	let mut payload_columns = Vec::with_capacity(projection_columns);
	for physical_ordinal in 2..2 + projection_columns {
		let value_buffer_ordinal = directory
			.column_to_buffers
			.get(physical_ordinal)
			.and_then(|mapping| mapping.first_buffer_ordinal.get().checked_add(1))
			.map(wendelon::schema::IpcBufferIndex::new)
			.ok_or_else(|| Error::new(ErrorKind::InvalidData, "sparse runtime payload buffer"))?;
		let ipc_column_ordinal = wendelon::schema::IpcColumnOrdinal::new(
			u32::try_from(physical_ordinal).map_err(|_| {
				Error::new(
					ErrorKind::InvalidInput,
					"sparse runtime physical ordinal exceeds u32",
				)
			})?,
		);
		let payload_page = page_directory
			.buffer_pages
			.iter()
			.find(|page| {
				page.row_group_index == RowGroupIndex::ZERO
					&& page.ipc_column_ordinal == ipc_column_ordinal
					&& page.ipc_buffer_ordinal == value_buffer_ordinal
					&& page.range.len.get() > 0
			})
			.ok_or_else(|| Error::new(ErrorKind::InvalidData, "sparse runtime payload page"))?;
		let payload_encoding = wendelon::storage::encoding::materialize::find_buffer_encoding(
			encoding_directory,
			RowGroupIndex::ZERO,
			value_buffer_ordinal,
			payload_page.range,
		);
		let (encoding, cascade, pages) = if let Some(payload_encoding) = payload_encoding {
			let payload_cascade = encoding_directory
				.cascades
				.get(payload_encoding.cascade_index.get() as usize)
				.cloned()
				.ok_or_else(|| {
					Error::new(ErrorKind::InvalidData, "sparse runtime payload cascade")
				})?;
			let pages =
				wendelon::storage::encoding::page::expand_page_descriptors(payload_encoding)
					.map_err(|code| {
						Error::new(
							ErrorKind::InvalidData,
							format!("sparse runtime page expansion rejected: {code:?}"),
						)
					})?;
			(Some(payload_encoding.clone()), Some(payload_cascade), pages)
		} else {
			(None, None, Vec::new())
		};
		payload_columns.push(SparseProjectionRuntimeColumn {
			physical_ordinal,
			buffer_range: payload_page.range,
			encoding,
			cascade,
			pages,
		});
	}
	Ok(SparseProjectionRuntimeFixture {
		row_group_rows,
		record_plaintext_range: record_plaintext_start
			..record_plaintext_start.saturating_add(encoded.frame.len()),
		frame: encoded.frame,
		directory,
		batch,
		requirement,
		payload_columns,
	})
}

fn sparse_projection_runtime_dataset(
	rows: usize,
	cascade: SparseProjectionCascadeFixture,
	projection_columns: usize,
) -> std::io::Result<SparseProjectionRuntimeDataset> {
	let mut remaining = rows;
	let mut plaintext = Vec::new();
	let mut fixtures = Vec::new();
	while remaining > 0 {
		let record_rows = remaining.min(SPARSE_PROJECTION_RUNTIME_RECORD_ROWS);
		let fixture = sparse_projection_runtime_fixture_at(
			record_rows,
			cascade,
			projection_columns,
			plaintext.len(),
		)?;
		plaintext.extend_from_slice(&fixture.frame);
		fixtures.push(fixture);
		remaining -= record_rows;
	}
	Ok(SparseProjectionRuntimeDataset {
		rows,
		plaintext,
		fixtures,
	})
}

fn sparse_projection_mask(shape: SparseProjectionMaskShape, rows: usize) -> BooleanArray {
	BooleanArray::from_iter((0..rows).map(|row| match shape {
		SparseProjectionMaskShape::SingleFirstRow => row == 0,
		SparseProjectionMaskShape::ClusteredFirstPage => row < 256.min(rows),
		SparseProjectionMaskShape::ClusteredMiddleOnePercent => {
			let selected = rows.div_ceil(100);
			let start = rows.saturating_sub(selected) / 2;
			row >= start && row < start.saturating_add(selected)
		}
		SparseProjectionMaskShape::ClusteredFivePercent => {
			row < rows.saturating_mul(5).div_ceil(100)
		}
		SparseProjectionMaskShape::ClusteredTwentyFivePercent => {
			row < rows.saturating_mul(25).div_ceil(100)
		}
		SparseProjectionMaskShape::RandomOnePercent => row % 100 == 0,
		SparseProjectionMaskShape::All => true,
		SparseProjectionMaskShape::None => false,
	}))
}

fn encoded_page_body_range(
	buffer_range: RecordBodyByteRange,
	encoding: &wendelon::storage::BufferEncoding,
	page: EncodingPageDescriptor,
) -> RecordBodyByteRange {
	RecordBodyByteRange {
		start: RecordBodyByteOffset::new(
			buffer_range
				.start
				.get()
				.saturating_add(u64::from(encoding.encoded_range.start.get()))
				.saturating_add(u64::from(page.byte_range.start.get())),
		),
		len: page.byte_range.len,
	}
}

fn coalesce_body_ranges(ranges: &mut Vec<RecordBodyByteRange>) -> std::io::Result<()> {
	ranges.sort_by_key(|range| range.start.get());
	let mut coalesced = Vec::<RecordBodyByteRange>::with_capacity(ranges.len());
	for range in ranges.drain(..) {
		if range.len.get() == 0 {
			continue;
		}
		let Some(last) = coalesced.last_mut() else {
			coalesced.push(range);
			continue;
		};
		let last_end = last.end()?;
		let range_end = range.end()?;
		if range.start.get() <= last_end {
			last.len = RecordBodyByteLen::new(range_end.max(last_end) - last.start.get());
		} else {
			coalesced.push(range);
		}
	}
	*ranges = coalesced;
	Ok(())
}

fn extract_from_plaintext_ranges<B: AsRef<[u8]>>(
	range: RecordBodyByteRange,
	selected_ranges: &[TieredPlaintextRange],
	plaintext_extracts: &[B],
) -> std::io::Result<Vec<u8>> {
	for (candidate, extract) in selected_ranges.iter().zip(plaintext_extracts.iter()) {
		let start = range.start.get();
		let end = range.end()?;
		let candidate_start = candidate.start.get();
		let candidate_end = candidate_start.saturating_add(candidate.len.get());
		if start >= candidate_start && end <= candidate_end {
			let relative_start = usize::try_from(start - candidate_start).map_err(|_| {
				Error::new(
					ErrorKind::InvalidData,
					"sparse runtime relative start exceeds usize",
				)
			})?;
			let relative_end = usize::try_from(end - candidate_start).map_err(|_| {
				Error::new(
					ErrorKind::InvalidData,
					"sparse runtime relative end exceeds usize",
				)
			})?;
			let extract = extract.as_ref();
			if relative_end > extract.len() {
				return Err(Error::new(
					ErrorKind::InvalidData,
					"sparse runtime page exceeds plaintext extract",
				));
			}
			return Ok(extract[relative_start..relative_end].to_vec());
		}
	}
	Err(Error::new(
		ErrorKind::InvalidData,
		"sparse runtime page range was not fetched",
	))
}

fn sparse_projection_page_selected(
	shape: SparseProjectionMaskShape,
	page: &EncodingPageDescriptor,
	rows: usize,
) -> bool {
	let page_start = page.first_value_index.get() as usize;
	let page_end = page_start.saturating_add(page.value_count.get() as usize);
	match shape {
		SparseProjectionMaskShape::SingleFirstRow => page_start == 0,
		SparseProjectionMaskShape::ClusteredFirstPage => page_start < 256,
		SparseProjectionMaskShape::ClusteredMiddleOnePercent => {
			let selected = rows.div_ceil(100);
			let start = rows.saturating_sub(selected) / 2;
			let end = start.saturating_add(selected);
			page_start < end && page_end > start
		}
		SparseProjectionMaskShape::ClusteredFivePercent => {
			page_start < rows.saturating_mul(5).div_ceil(100)
		}
		SparseProjectionMaskShape::ClusteredTwentyFivePercent => {
			page_start < rows.saturating_mul(25).div_ceil(100)
		}
		SparseProjectionMaskShape::RandomOnePercent => {
			let mut row = page_start.div_ceil(100) * 100;
			while row < page_end.min(rows) {
				if row % 100 == 0 {
					return true;
				}
				row = row.saturating_add(100);
			}
			false
		}
		SparseProjectionMaskShape::All => true,
		SparseProjectionMaskShape::None => false,
	}
}

fn coalesced_sparse_range_count(pages: &[&EncodingPageDescriptor]) -> usize {
	let mut ranges = pages
		.iter()
		.map(|page| {
			let start = u64::from(page.byte_range.start.get());
			let end = start.saturating_add(page.byte_range.len.get());
			(start, end)
		})
		.collect::<Vec<_>>();
	ranges.sort_unstable_by_key(|(start, _)| *start);
	let mut count = 0_usize;
	let mut current_end = 0_u64;
	for (start, end) in ranges {
		if count == 0 || start > current_end {
			count = count.saturating_add(1);
			current_end = end;
		} else {
			current_end = current_end.max(end);
		}
	}
	count
}

fn estimate_sparse_projection(
	fixture: &SparseProjectionEncodedFixture,
	_rows: usize,
	shape: SparseProjectionMaskShape,
	projection_columns: usize,
	per_buffer_range_cap: usize,
) -> SparseProjectionEstimate {
	let rows = fixture.row_group_rows;
	let scale = projection_columns as u64;
	if matches!(shape, SparseProjectionMaskShape::None) {
		return SparseProjectionEstimate {
			sparse_bytes: 0,
			sparse_ranges: 0,
			selected_simd_pages: 0,
			sparse_selected: true,
			decline: "none_mask_short_circuit",
		};
	}
	if matches!(shape, SparseProjectionMaskShape::All) {
		return SparseProjectionEstimate {
			sparse_bytes: fixture.dense_bytes.saturating_mul(scale),
			sparse_ranges: projection_columns,
			selected_simd_pages: fixture.pages.len().saturating_mul(projection_columns),
			sparse_selected: false,
			decline: "all_rows_mask",
		};
	}
	if !matches!(fixture.actual_cascade, "bit_packed" | "dictionary") {
		return SparseProjectionEstimate {
			sparse_bytes: fixture.dense_bytes.saturating_mul(scale),
			sparse_ranges: projection_columns,
			selected_simd_pages: fixture.pages.len().saturating_mul(projection_columns),
			sparse_selected: false,
			decline: "cascade_not_projection_eligible",
		};
	}
	let selected_pages = fixture
		.pages
		.iter()
		.filter(|page| sparse_projection_page_selected(shape, page, rows))
		.collect::<Vec<_>>();
	let sparse_bytes = selected_pages
		.iter()
		.map(|page| page.byte_range.len.get())
		.sum::<u64>();
	let sparse_ranges =
		coalesced_sparse_range_count(&selected_pages).saturating_mul(projection_columns);
	let selected_simd_pages = selected_pages.len().saturating_mul(projection_columns);
	let sparse_bytes = sparse_bytes.saturating_mul(scale);
	let dense_bytes = fixture.dense_bytes.saturating_mul(scale);
	let (sparse_selected, decline) = if selected_pages.len() > per_buffer_range_cap {
		(false, "per_buffer_fragmentation_cap_exceeded")
	} else if selected_simd_pages > 64 {
		(false, "simd_page_cap_exceeded")
	} else if sparse_ranges > 16 {
		(false, "range_cap_exceeded")
	} else if sparse_bytes >= dense_bytes {
		(false, "byte_estimate_not_sparse")
	} else {
		(true, "none")
	};
	SparseProjectionEstimate {
		sparse_bytes,
		sparse_ranges,
		selected_simd_pages,
		sparse_selected,
		decline,
	}
}

fn runtime_plaintext_ranges(ranges: &[RecordBodyByteRange]) -> Vec<TieredPlaintextRange> {
	ranges
		.iter()
		.map(|range| TieredPlaintextRange::new(range.start, range.len))
		.collect()
}

fn runtime_tail_range(object_size: u64) -> Range<u64> {
	object_size.saturating_sub(64 * 1024)..object_size
}

fn runtime_fetch_and_decrypt(
	runtime: &tokio::runtime::Runtime,
	store: Arc<CountingBenchObjectStore>,
	uri: &str,
	crypto: &wendelon::crypto::CryptoManager,
	slab_id: SlabId,
	object_size: u64,
	record_plaintext_range: Range<usize>,
	body_ranges: &[RecordBodyByteRange],
) -> std::io::Result<(Vec<TieredPlaintextRange>, Vec<Vec<u8>>, u64)> {
	let plaintext_ranges = runtime_plaintext_ranges(body_ranges);
	let tail_range = runtime_tail_range(object_size);
	let tail_window = runtime.block_on(store.get_range(uri, tail_range))?;
	let additional_metadata_range =
		crypto.additional_tiered_metadata_range(slab_id, object_size, tail_window.as_ref())?;
	let additional_metadata = match additional_metadata_range {
		Some(range) => Some(runtime.block_on(store.get_range(uri, range))?),
		None => None,
	};
	let tiered_plan = crypto.plan_tiered_multi_range_read(
		slab_id,
		object_size,
		tail_window.as_ref(),
		additional_metadata.as_deref(),
		record_plaintext_range,
		&plaintext_ranges,
	)?;
	let projection_ciphertext_bytes = tiered_plan
		.ciphertext_ranges_in_object()
		.into_iter()
		.map(|range| range.end.saturating_sub(range.start))
		.sum::<u64>();
	let ciphertext_runs = runtime.block_on(async {
		let mut out = Vec::new();
		for range in tiered_plan.ciphertext_ranges_in_object() {
			out.push(store.get_range(uri, range).await?);
		}
		Ok::<_, std::io::Error>(out)
	})?;
	let plaintext_extracts = tiered_plan.decrypt(&ciphertext_runs)?;
	Ok((
		plaintext_ranges,
		plaintext_extracts,
		projection_ciphertext_bytes,
	))
}

fn dense_projection_runtime(
	fixture: &SparseProjectionRuntimeFixture,
	mask: &BooleanArray,
	runtime: &tokio::runtime::Runtime,
	store: Arc<CountingBenchObjectStore>,
	uri: &str,
	crypto: &wendelon::crypto::CryptoManager,
	slab_id: SlabId,
	object_size: u64,
) -> std::io::Result<SparseProjectionRuntimeOutcome> {
	let page_directory = fixture.directory.page_directory.as_ref().ok_or_else(|| {
		Error::new(
			ErrorKind::InvalidData,
			"sparse runtime missing page directory",
		)
	})?;
	let selected_groups = [RowGroupIndex::ZERO];
	let dense_ranges = selected_row_group_record_body_ranges(
		&fixture.directory,
		page_directory,
		&fixture.requirement,
		&selected_groups,
	)
	.map_err(|err| Error::new(ErrorKind::InvalidData, format!("dense ranges: {err}")))?;
	let projection_plaintext_bytes = dense_ranges
		.iter()
		.map(|range| range.len.get())
		.sum::<u64>();
	let before = store.snapshot();
	let started = Instant::now();
	let (plaintext_ranges, plaintext_extracts, projection_ciphertext_bytes) =
		runtime_fetch_and_decrypt(
			runtime,
			store.clone(),
			uri,
			crypto,
			slab_id,
			object_size,
			fixture.record_plaintext_range.clone(),
			&dense_ranges,
		)?;
	let dense = decode_projected_row_groups_from_plaintext_ranges(
		&fixture.directory,
		page_directory,
		&fixture.requirement,
		&plaintext_ranges,
		&plaintext_extracts,
		&selected_groups,
	)
	.map_err(|err| Error::new(ErrorKind::InvalidData, format!("dense decode: {err}")))?;
	let mut outcome = PhaseBOutcome::default();
	outcome.decoded_predicate_batches.insert(
		RowGroupIndex::ZERO,
		fixture.batch.slice(0, fixture.row_group_rows),
	);
	outcome
		.survival_masks
		.insert(RowGroupIndex::ZERO, mask.clone());
	let filtered = apply_phase_b_masks_to_projected_row_groups(
		vec![(RowGroupIndex::ZERO, dense)],
		&outcome,
		&selected_groups,
		page_directory,
	)?;
	let elapsed = started.elapsed();
	let delta = store.snapshot().delta_since(before);
	Ok(SparseProjectionRuntimeOutcome {
		elapsed,
		output_rows: filtered.batch.num_rows(),
		get_range_calls: delta.get_range_calls,
		get_range_bytes: delta.get_range_bytes,
		get_bytes_calls: delta.get_bytes_calls,
		projection_plaintext_bytes,
		projection_ciphertext_bytes,
		selected_ranges: dense_ranges.len(),
		selected_simd_pages: fixture
			.payload_columns
			.iter()
			.map(|column| column.pages.len())
			.sum(),
	})
}

fn sparse_projection_runtime(
	fixture: &SparseProjectionRuntimeFixture,
	mask: &BooleanArray,
	shape: SparseProjectionMaskShape,
	runtime: &tokio::runtime::Runtime,
	store: Arc<CountingBenchObjectStore>,
	uri: &str,
	crypto: &wendelon::crypto::CryptoManager,
	slab_id: SlabId,
	object_size: u64,
) -> std::io::Result<(SparseProjectionRuntimeOutcome, Vec<u8>)> {
	if fixture
		.payload_columns
		.iter()
		.any(|column| column.encoding.is_none() || column.cascade.is_none())
	{
		return Err(Error::new(
			ErrorKind::InvalidData,
			"sparse runtime requested for dense-only projected column",
		));
	}
	let selected_pages_by_column = fixture
		.payload_columns
		.iter()
		.map(|column| {
			column
				.pages
				.iter()
				.copied()
				.filter(|page| sparse_projection_page_selected(shape, page, mask.len()))
				.collect::<Vec<_>>()
		})
		.collect::<Vec<_>>();
	let selected_simd_pages = selected_pages_by_column.iter().map(Vec::len).sum();
	let mut sparse_ranges = fixture
		.payload_columns
		.iter()
		.zip(selected_pages_by_column.iter())
		.flat_map(|(column, selected_pages)| {
			let encoding = column
				.encoding
				.as_ref()
				.expect("sparse runtime prechecked projected column encoding");
			selected_pages
				.iter()
				.map(move |page| encoded_page_body_range(column.buffer_range, encoding, *page))
		})
		.collect::<Vec<_>>();
	coalesce_body_ranges(&mut sparse_ranges)?;
	let projection_plaintext_bytes = sparse_ranges
		.iter()
		.map(|range| range.len.get())
		.sum::<u64>();
	let before = store.snapshot();
	let started = Instant::now();
	let (plaintext_ranges, plaintext_extracts, projection_ciphertext_bytes) =
		runtime_fetch_and_decrypt(
			runtime,
			store.clone(),
			uri,
			crypto,
			slab_id,
			object_size,
			fixture.record_plaintext_range.clone(),
			&sparse_ranges,
		)?;
	let mut values = Vec::new();
	for (column, selected_pages) in fixture
		.payload_columns
		.iter()
		.zip(selected_pages_by_column.iter())
	{
		let encoding = column
			.encoding
			.as_ref()
			.expect("sparse runtime prechecked projected column encoding");
		let cascade = column
			.cascade
			.as_ref()
			.expect("sparse runtime prechecked projected column cascade");
		for page in selected_pages {
			let body_range = encoded_page_body_range(column.buffer_range, encoding, *page);
			let encoded =
				extract_from_plaintext_ranges(body_range, &plaintext_ranges, &plaintext_extracts)?;
			let expected_bytes = u64::from(page.value_count.get());
			let decoded = wendelon::storage::encoding::cascade::decode_page(
				&encoded,
				&DataType::UInt32,
				cascade,
				RecordBodyByteLen::new(
					expected_bytes.saturating_mul(SPARSE_PROJECTION_RUNTIME_PAYLOAD_BYTES as u64),
				),
				BufferValueCount::new(page.value_count.get()),
			)
			.map_err(|code| {
				Error::new(
					ErrorKind::InvalidData,
					format!("sparse runtime page decode rejected: {code:?}"),
				)
			})?;
			let start = page.first_value_index.get() as usize;
			for (offset, value) in decoded
				.chunks_exact(SPARSE_PROJECTION_RUNTIME_PAYLOAD_BYTES)
				.enumerate()
			{
				let row = start + offset;
				if row < mask.len() && mask.is_valid(row) && mask.value(row) {
					values.extend_from_slice(value);
				}
			}
		}
	}
	let elapsed = started.elapsed();
	let delta = store.snapshot().delta_since(before);
	Ok((
		SparseProjectionRuntimeOutcome {
			elapsed,
			output_rows: values.len()
				/ SPARSE_PROJECTION_RUNTIME_PAYLOAD_BYTES
				/ fixture.payload_columns.len().max(1),
			get_range_calls: delta.get_range_calls,
			get_range_bytes: delta.get_range_bytes,
			get_bytes_calls: delta.get_bytes_calls,
			projection_plaintext_bytes,
			projection_ciphertext_bytes,
			selected_ranges: sparse_ranges.len(),
			selected_simd_pages,
		},
		values,
	))
}

fn dense_projection_values(
	fixture: &SparseProjectionRuntimeFixture,
	mask: &BooleanArray,
) -> std::io::Result<Vec<u8>> {
	let mut out = Vec::new();
	let group_rows = fixture.row_group_rows;
	for column in &fixture.payload_columns {
		let payload = fixture
			.batch
			.column(column.physical_ordinal)
			.as_any()
			.downcast_ref::<UInt32Array>()
			.ok_or_else(|| Error::new(ErrorKind::InvalidData, "expected UInt32 payload"))?;
		for row in 0..group_rows {
			if row < mask.len() && mask.is_valid(row) && mask.value(row) {
				out.extend_from_slice(&payload.value(row).to_le_bytes());
			}
		}
	}
	Ok(out)
}

fn dense_projection_dataset_values(
	dataset: &SparseProjectionRuntimeDataset,
	shape: SparseProjectionMaskShape,
) -> std::io::Result<Vec<u8>> {
	let mut out = Vec::new();
	for fixture in &dataset.fixtures {
		let mask = sparse_projection_mask(shape, fixture.row_group_rows);
		out.extend(dense_projection_values(fixture, &mask)?);
	}
	Ok(out)
}

fn dense_projection_dataset_runtime(
	dataset: &SparseProjectionRuntimeDataset,
	shape: SparseProjectionMaskShape,
	runtime: &tokio::runtime::Runtime,
	store: Arc<CountingBenchObjectStore>,
	uri: &str,
	crypto: &wendelon::crypto::CryptoManager,
	slab_id: SlabId,
	object_size: u64,
) -> std::io::Result<SparseProjectionRuntimeOutcome> {
	let mut total = SparseProjectionRuntimeOutcome::default();
	for fixture in &dataset.fixtures {
		let mask = sparse_projection_mask(shape, fixture.row_group_rows);
		total.accumulate(dense_projection_runtime(
			fixture,
			&mask,
			runtime,
			store.clone(),
			uri,
			crypto,
			slab_id,
			object_size,
		)?);
	}
	Ok(total)
}

fn sparse_projection_dataset_runtime(
	dataset: &SparseProjectionRuntimeDataset,
	shape: SparseProjectionMaskShape,
	runtime: &tokio::runtime::Runtime,
	store: Arc<CountingBenchObjectStore>,
	uri: &str,
	crypto: &wendelon::crypto::CryptoManager,
	slab_id: SlabId,
	object_size: u64,
) -> std::io::Result<(SparseProjectionRuntimeOutcome, Vec<u8>)> {
	let mut total = SparseProjectionRuntimeOutcome::default();
	let mut values = Vec::new();
	for fixture in &dataset.fixtures {
		let mask = sparse_projection_mask(shape, fixture.row_group_rows);
		let (outcome, record_values) = sparse_projection_runtime(
			fixture,
			&mask,
			shape,
			runtime,
			store.clone(),
			uri,
			crypto,
			slab_id,
			object_size,
		)?;
		total.accumulate(outcome);
		values.extend(record_values);
	}
	Ok((total, values))
}

fn sparse_projection_matrix_rows(config: &BenchConfig) -> &'static [usize] {
	if config.iterations <= 25 {
		&[8_192]
	} else {
		&[8_192, 65_536]
	}
}

fn sparse_projection_projection_counts(config: &BenchConfig) -> &'static [usize] {
	if config.iterations <= 25 {
		&[1]
	} else {
		&[1, 4, 16, 64]
	}
}

fn sparse_projection_runtime_rows(config: &BenchConfig) -> &'static [usize] {
	if config.iterations <= 25 {
		&[8_192]
	} else {
		&[8_192, 65_536, 1_000_000]
	}
}

fn sparse_projection_runtime_shapes(config: &BenchConfig) -> &'static [SparseProjectionMaskShape] {
	if config.iterations <= 25 {
		&[
			SparseProjectionMaskShape::ClusteredFirstPage,
			SparseProjectionMaskShape::ClusteredFivePercent,
			SparseProjectionMaskShape::All,
			SparseProjectionMaskShape::None,
		]
	} else {
		&[
			SparseProjectionMaskShape::ClusteredFirstPage,
			SparseProjectionMaskShape::ClusteredMiddleOnePercent,
			SparseProjectionMaskShape::ClusteredFivePercent,
			SparseProjectionMaskShape::ClusteredTwentyFivePercent,
			SparseProjectionMaskShape::RandomOnePercent,
			SparseProjectionMaskShape::All,
			SparseProjectionMaskShape::None,
		]
	}
}

fn sparse_projection_runtime_cascades(
	config: &BenchConfig,
) -> &'static [SparseProjectionCascadeFixture] {
	if config.iterations <= 25 {
		&[
			SparseProjectionCascadeFixture::BitPacked,
			SparseProjectionCascadeFixture::Dictionary,
		]
	} else {
		&[
			SparseProjectionCascadeFixture::BitPacked,
			SparseProjectionCascadeFixture::Dictionary,
			SparseProjectionCascadeFixture::Rle,
		]
	}
}

fn sparse_projection_runtime_iterations(config: &BenchConfig, rows: usize) -> usize {
	if rows >= 1_000_000 {
		config.iterations.min(3).max(1)
	} else {
		config.iterations.min(10).max(1)
	}
}

fn sparse_projection_runtime_sparse_enabled(cascade: SparseProjectionCascadeFixture) -> bool {
	matches!(
		cascade,
		SparseProjectionCascadeFixture::BitPacked | SparseProjectionCascadeFixture::Dictionary
	)
}

fn sparse_projection_runtime_actual_cascade(
	dataset: &SparseProjectionRuntimeDataset,
) -> &'static str {
	fn column_cascade(column: &SparseProjectionRuntimeColumn) -> &'static str {
		column
			.cascade
			.as_ref()
			.map(|cascade| cascade.kind().as_str())
			.unwrap_or("plain")
	}

	let Some(first_column) = dataset
		.fixtures
		.first()
		.and_then(|fixture| fixture.payload_columns.first())
	else {
		return "none";
	};
	let first = column_cascade(first_column);
	if dataset.fixtures.iter().all(|fixture| {
		fixture
			.payload_columns
			.iter()
			.all(|column| column_cascade(column) == first)
	}) {
		first
	} else {
		"mixed"
	}
}

fn sparse_projection_runtime_projection_columns(dataset: &SparseProjectionRuntimeDataset) -> usize {
	dataset
		.fixtures
		.first()
		.map(|fixture| fixture.payload_columns.len())
		.unwrap_or(1)
		.max(1)
}

fn sparse_projection_runtime_selected_pages_per_column(
	dataset: &SparseProjectionRuntimeDataset,
	shape: SparseProjectionMaskShape,
) -> usize {
	dataset
		.fixtures
		.first()
		.and_then(|fixture| {
			fixture
				.payload_columns
				.iter()
				.find(|column| !column.pages.is_empty())
				.map(|column| (fixture, column))
		})
		.map(|(fixture, column)| {
			column
				.pages
				.iter()
				.filter(|page| sparse_projection_page_selected(shape, page, fixture.row_group_rows))
				.count()
		})
		.unwrap_or(0)
}

fn sparse_projection_runtime_route(
	dataset: &SparseProjectionRuntimeDataset,
	cascade_sparse_enabled: bool,
	shape: SparseProjectionMaskShape,
	per_buffer_range_cap: usize,
) -> &'static str {
	if matches!(shape, SparseProjectionMaskShape::All) {
		return "dense_decline_all";
	}
	if matches!(shape, SparseProjectionMaskShape::None) {
		return "sparse_none_short_circuit";
	}
	if !cascade_sparse_enabled {
		return "dense_decline_cascade";
	}
	let projection_columns = sparse_projection_runtime_projection_columns(dataset);
	let selected_pages = sparse_projection_runtime_selected_pages_per_column(dataset, shape);
	if selected_pages > per_buffer_range_cap {
		return "dense_decline_fragmentation_cap";
	}
	let sparse_ranges = selected_pages.saturating_mul(projection_columns);
	if sparse_ranges > 16 {
		return "dense_decline_range_cap";
	}
	let selected_simd_pages = selected_pages.saturating_mul(projection_columns);
	if selected_simd_pages > 64 {
		return "dense_decline_simd_page_cap";
	}
	"sparse"
}

struct SparseProjectionRuntimeMeasurement {
	dense: SparseProjectionRuntimeOutcome,
	sparse: SparseProjectionRuntimeOutcome,
	route: &'static str,
	expected_rows: usize,
}

struct SparseProjectionRuntimeEnv<'a> {
	runtime: &'a tokio::runtime::Runtime,
	store: Arc<CountingBenchObjectStore>,
	uri: &'a str,
	crypto: &'a wendelon::crypto::CryptoManager,
	slab_id: SlabId,
	object_size: u64,
}

fn measure_sparse_projection_runtime_case(
	dataset: &SparseProjectionRuntimeDataset,
	shape: SparseProjectionMaskShape,
	iterations: usize,
	env: &SparseProjectionRuntimeEnv<'_>,
	cascade_sparse_enabled: bool,
	per_buffer_range_cap: usize,
) -> std::io::Result<SparseProjectionRuntimeMeasurement> {
	let expected = dense_projection_dataset_values(dataset, shape)?;
	let expected_rows = expected.len()
		/ SPARSE_PROJECTION_RUNTIME_PAYLOAD_BYTES
		/ sparse_projection_runtime_projection_columns(dataset);
	let mut last_dense = None;
	let dense_elapsed = bench(iterations, || {
		let outcome = dense_projection_dataset_runtime(
			black_box(dataset),
			black_box(shape),
			env.runtime,
			env.store.clone(),
			env.uri,
			env.crypto,
			env.slab_id,
			env.object_size,
		)
		.expect("dense projection runtime");
		last_dense = Some(outcome);
	});
	let mut dense = last_dense.expect("dense runtime outcome");
	dense.elapsed = dense_elapsed;
	let route = sparse_projection_runtime_route(
		dataset,
		cascade_sparse_enabled,
		shape,
		per_buffer_range_cap,
	);
	let (sparse, sparse_values) = match route {
		"sparse" => {
			let mut last_sparse = None;
			let sparse_elapsed = bench(iterations, || {
				let (outcome, values) = sparse_projection_dataset_runtime(
					black_box(dataset),
					black_box(shape),
					env.runtime,
					env.store.clone(),
					env.uri,
					env.crypto,
					env.slab_id,
					env.object_size,
				)
				.expect("sparse projection runtime");
				last_sparse = Some((outcome, values));
			});
			let (mut outcome, values) = last_sparse.expect("sparse runtime outcome");
			outcome.elapsed = sparse_elapsed;
			(outcome, values)
		}
		"sparse_none_short_circuit" => (
			SparseProjectionRuntimeOutcome {
				elapsed: Duration::ZERO,
				output_rows: 0,
				get_range_calls: 0,
				get_range_bytes: 0,
				get_bytes_calls: 0,
				projection_plaintext_bytes: 0,
				projection_ciphertext_bytes: 0,
				selected_ranges: 0,
				selected_simd_pages: 0,
			},
			Vec::new(),
		),
		_ => (
			SparseProjectionRuntimeOutcome {
				elapsed: dense.elapsed,
				output_rows: dense.output_rows,
				get_range_calls: dense.get_range_calls,
				get_range_bytes: dense.get_range_bytes,
				get_bytes_calls: dense.get_bytes_calls,
				projection_plaintext_bytes: dense.projection_plaintext_bytes,
				projection_ciphertext_bytes: dense.projection_ciphertext_bytes,
				selected_ranges: dense.selected_ranges,
				selected_simd_pages: dense.selected_simd_pages,
			},
			expected.clone(),
		),
	};
	if dense.output_rows != expected_rows
		|| sparse.output_rows != expected_rows
		|| sparse_values != expected
	{
		let mismatch = sparse_values
			.iter()
			.zip(expected.iter())
			.position(|(left, right)| left != right)
			.unwrap_or_else(|| sparse_values.len().min(expected.len()));
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"sparse runtime output mismatch rows={} shape={} route={} dense_rows={} sparse_rows={} expected_rows={} sparse_bytes={} expected_bytes={} first_mismatch={}",
				dataset.rows,
				shape.as_str(),
				route,
				dense.output_rows,
				sparse.output_rows,
				expected_rows,
				sparse_values.len(),
				expected.len(),
				mismatch
			),
		));
	}
	Ok(SparseProjectionRuntimeMeasurement {
		dense,
		sparse,
		route,
		expected_rows,
	})
}

fn sparse_projection_speedup_micros(
	dense: &SparseProjectionRuntimeOutcome,
	sparse: &SparseProjectionRuntimeOutcome,
) -> u64 {
	let dense_micros = dense.elapsed.as_secs_f64() * 1_000_000.0;
	let sparse_micros = sparse.elapsed.as_secs_f64() * 1_000_000.0;
	if sparse_micros > 0.0 {
		(dense_micros / sparse_micros * 1_000_000.0) as u64
	} else if dense_micros > 0.0 {
		u64::MAX
	} else {
		1_000_000
	}
}

fn print_sparse_projection_runtime_measure(
	scenario: &str,
	dataset: &SparseProjectionRuntimeDataset,
	actual_cascade: &str,
	projected_cascade: &str,
	shape: SparseProjectionMaskShape,
	iterations: usize,
	measurement: &SparseProjectionRuntimeMeasurement,
	extra: &str,
) {
	println!(
		"row_group_fetch_measure scenario={} rows={} records={} columns={} shape=uint32_payload_clustered projected_cascade={} actual_cascade={} mask_shape={} route={} output_rows={} equality=true iterations={} dense_total_ms={:.3} sparse_total_ms={:.3} speedup_micros={} dense_get_range_calls={} sparse_get_range_calls={} dense_get_range_bytes={} sparse_get_range_bytes={} dense_projection_plaintext_bytes={} sparse_projection_plaintext_bytes={} dense_projection_ciphertext_bytes={} sparse_projection_ciphertext_bytes={} dense_selected_ranges={} sparse_selected_ranges={} sparse_selected_simd_pages={} get_bytes_calls={}{}",
		scenario,
		dataset.rows,
		dataset.fixtures.len(),
		sparse_projection_runtime_projection_columns(dataset).saturating_add(2),
		projected_cascade,
		actual_cascade,
		shape.as_str(),
		measurement.route,
		measurement.expected_rows,
		iterations,
		measurement.dense.elapsed.as_secs_f64() * 1_000.0,
		measurement.sparse.elapsed.as_secs_f64() * 1_000.0,
		sparse_projection_speedup_micros(&measurement.dense, &measurement.sparse),
		measurement.dense.get_range_calls,
		measurement.sparse.get_range_calls,
		measurement.dense.get_range_bytes,
		measurement.sparse.get_range_bytes,
		measurement.dense.projection_plaintext_bytes,
		measurement.sparse.projection_plaintext_bytes,
		measurement.dense.projection_ciphertext_bytes,
		measurement.sparse.projection_ciphertext_bytes,
		measurement.dense.selected_ranges,
		measurement.sparse.selected_ranges,
		measurement.sparse.selected_simd_pages,
		measurement
			.dense
			.get_bytes_calls
			.saturating_add(measurement.sparse.get_bytes_calls),
		extra,
	);
}

fn sparse_runtime_encrypted_dataset(
	dataset: &SparseProjectionRuntimeDataset,
	crypto: &wendelon::crypto::CryptoManager,
	slab_id: SlabId,
) -> std::io::Result<wendelon::crypto::EncryptedTieredSlab> {
	let mut offset_index = Vec::with_capacity(dataset.fixtures.len());
	for (index, fixture) in dataset.fixtures.iter().enumerate() {
		let mut key = [0x60; 32];
		key[31] = u8::try_from(index % 256).unwrap_or(0);
		offset_index.push(TieredOffsetIndexEntry {
			key_locator: IdentityPrefix::from(Identity::new(key)),
			byte_offset: u64::try_from(fixture.record_plaintext_range.start).map_err(|_| {
				Error::new(
					ErrorKind::InvalidInput,
					"sparse runtime record offset exceeds u64",
				)
			})?,
			byte_length: u32::try_from(fixture.frame.len()).map_err(|_| {
				Error::new(
					ErrorKind::InvalidInput,
					"sparse runtime frame exceeds tiered offset index u32 length",
				)
			})?,
			flags: 0,
		});
	}
	let metadata = TieredSlabObjectMetadata {
		tenant_id: TenantId::ZERO,
		created_hlc: 0,
		chunk_size_bytes: wendelon::crypto::SLAB_ENCRYPTION_DEFAULT_CHUNK_SIZE,
		offset_index,
		column_stats_block: None,
		has_row_tombstone_records: false,
	};
	crypto.encrypt_tiered_slab_object(slab_id, &metadata, &dataset.plaintext)
}

fn bench_sparse_projection_runtime(config: &BenchConfig) -> std::io::Result<()> {
	let runtime = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()?;
	let mut emitted = 0_usize;
	for rows in sparse_projection_runtime_rows(config) {
		let iterations = sparse_projection_runtime_iterations(config, *rows);
		for cascade in sparse_projection_runtime_cascades(config) {
			let dataset = sparse_projection_runtime_dataset(*rows, *cascade, 1)?;
			let slab_id = SlabId::new(6_000 + *rows as u64 + (*cascade as u64) * 1_000_000);
			let crypto = seeded_dev_crypto_manager(TenantId::ZERO)?;
			let encrypted = sparse_runtime_encrypted_dataset(&dataset, &crypto, slab_id)?;
			let store = Arc::new(CountingBenchObjectStore::new(Arc::new(
				MockObjectStore::new(MockObjectStoreConfig::default()),
			)));
			let uri = runtime.block_on(store.put(
				&format!("sparse-runtime/{}_{}.wnd", rows, cascade.as_str()),
				&encrypted.bytes,
			))?;
			let object_size = encrypted.bytes.len() as u64;
			let actual_cascade = sparse_projection_runtime_actual_cascade(&dataset);
			for shape in sparse_projection_runtime_shapes(config) {
				let expected = dense_projection_dataset_values(&dataset, *shape)?;
				let expected_rows = expected.len()
					/ SPARSE_PROJECTION_RUNTIME_PAYLOAD_BYTES
					/ dataset
						.fixtures
						.first()
						.map(|fixture| fixture.payload_columns.len())
						.unwrap_or(1)
						.max(1);
				let mut last_dense = None;
				let dense_elapsed = bench(iterations, || {
					let outcome = dense_projection_dataset_runtime(
						black_box(&dataset),
						black_box(*shape),
						&runtime,
						store.clone(),
						&uri,
						&crypto,
						slab_id,
						object_size,
					)
					.expect("dense projection runtime");
					last_dense = Some(outcome);
				});
				let mut dense = last_dense.expect("dense runtime outcome");
				dense.elapsed = dense_elapsed;
				let (sparse, sparse_values, route) =
					if matches!(shape, SparseProjectionMaskShape::All) {
						(
							SparseProjectionRuntimeOutcome {
								elapsed: dense.elapsed,
								output_rows: dense.output_rows,
								get_range_calls: dense.get_range_calls,
								get_range_bytes: dense.get_range_bytes,
								get_bytes_calls: dense.get_bytes_calls,
								projection_plaintext_bytes: dense.projection_plaintext_bytes,
								projection_ciphertext_bytes: dense.projection_ciphertext_bytes,
								selected_ranges: dense.selected_ranges,
								selected_simd_pages: dense.selected_simd_pages,
							},
							expected.clone(),
							"dense_decline_all",
						)
					} else if matches!(shape, SparseProjectionMaskShape::None) {
						(
							SparseProjectionRuntimeOutcome {
								elapsed: Duration::ZERO,
								output_rows: 0,
								get_range_calls: 0,
								get_range_bytes: 0,
								get_bytes_calls: 0,
								projection_plaintext_bytes: 0,
								projection_ciphertext_bytes: 0,
								selected_ranges: 0,
								selected_simd_pages: 0,
							},
							Vec::new(),
							"sparse_none_short_circuit",
						)
					} else if !sparse_projection_runtime_sparse_enabled(*cascade) {
						(
							SparseProjectionRuntimeOutcome {
								elapsed: dense.elapsed,
								output_rows: dense.output_rows,
								get_range_calls: dense.get_range_calls,
								get_range_bytes: dense.get_range_bytes,
								get_bytes_calls: dense.get_bytes_calls,
								projection_plaintext_bytes: dense.projection_plaintext_bytes,
								projection_ciphertext_bytes: dense.projection_ciphertext_bytes,
								selected_ranges: dense.selected_ranges,
								selected_simd_pages: dense.selected_simd_pages,
							},
							expected.clone(),
							"dense_decline_cascade",
						)
					} else {
						let mut last_sparse = None;
						let sparse_elapsed = bench(iterations, || {
							let (outcome, values) = sparse_projection_dataset_runtime(
								black_box(&dataset),
								black_box(*shape),
								&runtime,
								store.clone(),
								&uri,
								&crypto,
								slab_id,
								object_size,
							)
							.expect("sparse projection runtime");
							last_sparse = Some((outcome, values));
						});
						let (mut outcome, values) = last_sparse.expect("sparse runtime outcome");
						outcome.elapsed = sparse_elapsed;
						(outcome, values, "sparse")
					};
				if dense.output_rows != expected_rows
					|| sparse.output_rows != expected_rows
					|| sparse_values != expected
				{
					let mismatch = sparse_values
						.iter()
						.zip(expected.iter())
						.position(|(left, right)| left != right)
						.unwrap_or_else(|| sparse_values.len().min(expected.len()));
					return Err(Error::new(
						ErrorKind::InvalidData,
						format!(
							"sparse runtime output mismatch rows={} cascade={} shape={} dense_rows={} sparse_rows={} expected_rows={} sparse_bytes={} expected_bytes={} first_mismatch={}",
							rows,
							cascade.as_str(),
							shape.as_str(),
							dense.output_rows,
							sparse.output_rows,
							expected_rows,
							sparse_values.len(),
							expected.len(),
							mismatch
						),
					));
				}
				let dense_micros = dense.elapsed.as_secs_f64() * 1_000_000.0;
				let sparse_micros = sparse.elapsed.as_secs_f64() * 1_000_000.0;
				let speedup_micros = if sparse_micros > 0.0 {
					(dense_micros / sparse_micros * 1_000_000.0) as u64
				} else if dense_micros > 0.0 {
					u64::MAX
				} else {
					1_000_000
				};
				println!(
					"row_group_fetch_measure scenario=sparse_projection_runtime rows={} records={} columns=3 shape=uint32_payload_clustered projected_cascade={} actual_cascade={} mask_shape={} route={} output_rows={} equality=true iterations={} dense_total_ms={:.3} sparse_total_ms={:.3} speedup_micros={} dense_get_range_calls={} sparse_get_range_calls={} dense_get_range_bytes={} sparse_get_range_bytes={} dense_projection_plaintext_bytes={} sparse_projection_plaintext_bytes={} dense_projection_ciphertext_bytes={} sparse_projection_ciphertext_bytes={} dense_selected_ranges={} sparse_selected_ranges={} sparse_selected_simd_pages={} get_bytes_calls={}",
					dataset.rows,
					dataset.fixtures.len(),
					cascade.as_str(),
					actual_cascade,
					shape.as_str(),
					route,
					expected_rows,
					iterations,
					dense.elapsed.as_secs_f64() * 1_000.0,
					sparse.elapsed.as_secs_f64() * 1_000.0,
					speedup_micros,
					dense.get_range_calls,
					sparse.get_range_calls,
					dense.get_range_bytes,
					sparse.get_range_bytes,
					dense.projection_plaintext_bytes,
					sparse.projection_plaintext_bytes,
					dense.projection_ciphertext_bytes,
					sparse.projection_ciphertext_bytes,
					dense.selected_ranges,
					sparse.selected_ranges,
					sparse.selected_simd_pages,
					dense.get_bytes_calls.saturating_add(sparse.get_bytes_calls),
				);
				emitted = emitted.saturating_add(1);
			}
		}
	}
	println!(
		"row_group_fetch_bench scenario=sparse_projection_runtime rows=mixed columns=3 shape=uint32_payload_clustered matrix_rows={} emitted_rows={} iterations=max10_1m3",
		sparse_projection_runtime_rows(config)
			.iter()
			.map(|rows| rows.to_string())
			.collect::<Vec<_>>()
			.join(","),
		emitted,
	);
	Ok(())
}

fn bench_sparse_projection_runtime_projection_counts(config: &BenchConfig) -> std::io::Result<()> {
	if config.iterations <= 25 {
		return Ok(());
	}
	let runtime = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()?;
	let rows = 8_192;
	let iterations = sparse_projection_runtime_iterations(config, rows);
	let mut emitted = 0_usize;
	for cascade in [
		SparseProjectionCascadeFixture::BitPacked,
		SparseProjectionCascadeFixture::Dictionary,
	] {
		for projection_columns in [1_usize, 4, 16, 64] {
			let dataset = sparse_projection_runtime_dataset(rows, cascade, projection_columns)?;
			let slab_id =
				SlabId::new(7_000 + rows as u64 + projection_columns as u64 + cascade as u64);
			let crypto = seeded_dev_crypto_manager(TenantId::ZERO)?;
			let encrypted = sparse_runtime_encrypted_dataset(&dataset, &crypto, slab_id)?;
			let store = Arc::new(CountingBenchObjectStore::new(Arc::new(
				MockObjectStore::new(MockObjectStoreConfig::default()),
			)));
			let uri = runtime.block_on(store.put(
				&format!(
					"sparse-runtime-projection-count/{}_{}_{}.wnd",
					rows,
					cascade.as_str(),
					projection_columns
				),
				&encrypted.bytes,
			))?;
			let env = SparseProjectionRuntimeEnv {
				runtime: &runtime,
				store,
				uri: &uri,
				crypto: &crypto,
				slab_id,
				object_size: encrypted.bytes.len() as u64,
			};
			let measurement = measure_sparse_projection_runtime_case(
				&dataset,
				SparseProjectionMaskShape::ClusteredFirstPage,
				iterations,
				&env,
				sparse_projection_runtime_sparse_enabled(cascade),
				8,
			)?;
			let extra = format!(
				" projection_columns={} per_buffer_range_cap=8",
				projection_columns
			);
			print_sparse_projection_runtime_measure(
				"sparse_projection_runtime_projection_count",
				&dataset,
				sparse_projection_runtime_actual_cascade(&dataset),
				cascade.as_str(),
				SparseProjectionMaskShape::ClusteredFirstPage,
				iterations,
				&measurement,
				&extra,
			);
			emitted = emitted.saturating_add(1);
		}
	}
	println!(
		"row_group_fetch_bench scenario=sparse_projection_runtime_projection_count rows={} emitted_rows={} iterations={}",
		rows, emitted, iterations,
	);
	Ok(())
}

fn bench_sparse_projection_runtime_mixed_cascade(config: &BenchConfig) -> std::io::Result<()> {
	if config.iterations <= 25 {
		return Ok(());
	}
	let runtime = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()?;
	let rows = 8_192;
	let iterations = sparse_projection_runtime_iterations(config, rows);
	let dataset = sparse_projection_runtime_dataset(
		rows,
		SparseProjectionCascadeFixture::BitPackedPlusPlain,
		2,
	)?;
	let slab_id = SlabId::new(7_500 + rows as u64);
	let crypto = seeded_dev_crypto_manager(TenantId::ZERO)?;
	let encrypted = sparse_runtime_encrypted_dataset(&dataset, &crypto, slab_id)?;
	let store = Arc::new(CountingBenchObjectStore::new(Arc::new(
		MockObjectStore::new(MockObjectStoreConfig::default()),
	)));
	let uri = runtime.block_on(store.put(
		&format!("sparse-runtime-mixed-cascade/{}.wnd", rows),
		&encrypted.bytes,
	))?;
	let env = SparseProjectionRuntimeEnv {
		runtime: &runtime,
		store,
		uri: &uri,
		crypto: &crypto,
		slab_id,
		object_size: encrypted.bytes.len() as u64,
	};
	let measurement = measure_sparse_projection_runtime_case(
		&dataset,
		SparseProjectionMaskShape::ClusteredFirstPage,
		iterations,
		&env,
		false,
		8,
	)?;
	print_sparse_projection_runtime_measure(
		"sparse_projection_runtime_mixed_cascade",
		&dataset,
		sparse_projection_runtime_actual_cascade(&dataset),
		SparseProjectionCascadeFixture::BitPackedPlusPlain.as_str(),
		SparseProjectionMaskShape::ClusteredFirstPage,
		iterations,
		&measurement,
		" projection_columns=2 per_buffer_range_cap=8",
	);
	println!(
		"row_group_fetch_bench scenario=sparse_projection_runtime_mixed_cascade rows={} emitted_rows=1 iterations={}",
		rows, iterations,
	);
	Ok(())
}

fn bench_sparse_projection_runtime_fragmentation(config: &BenchConfig) -> std::io::Result<()> {
	if config.iterations <= 25 {
		return Ok(());
	}
	let runtime = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()?;
	let rows = 8_192;
	let iterations = sparse_projection_runtime_iterations(config, rows);
	let dataset =
		sparse_projection_runtime_dataset(rows, SparseProjectionCascadeFixture::BitPacked, 1)?;
	let slab_id = SlabId::new(8_000 + rows as u64);
	let crypto = seeded_dev_crypto_manager(TenantId::ZERO)?;
	let encrypted = sparse_runtime_encrypted_dataset(&dataset, &crypto, slab_id)?;
	let store = Arc::new(CountingBenchObjectStore::new(Arc::new(
		MockObjectStore::new(MockObjectStoreConfig::default()),
	)));
	let uri = runtime.block_on(store.put(
		&format!("sparse-runtime-fragmentation/{}.wnd", rows),
		&encrypted.bytes,
	))?;
	let env = SparseProjectionRuntimeEnv {
		runtime: &runtime,
		store,
		uri: &uri,
		crypto: &crypto,
		slab_id,
		object_size: encrypted.bytes.len() as u64,
	};
	let mut emitted = 0_usize;
	for cap in [2_usize, 4, 8, 16] {
		let measurement = measure_sparse_projection_runtime_case(
			&dataset,
			SparseProjectionMaskShape::RandomOnePercent,
			iterations,
			&env,
			true,
			cap,
		)?;
		let extra = format!(" projection_columns=1 per_buffer_range_cap={}", cap);
		print_sparse_projection_runtime_measure(
			"sparse_projection_runtime_fragmentation_budget",
			&dataset,
			sparse_projection_runtime_actual_cascade(&dataset),
			SparseProjectionCascadeFixture::BitPacked.as_str(),
			SparseProjectionMaskShape::RandomOnePercent,
			iterations,
			&measurement,
			&extra,
		);
		emitted = emitted.saturating_add(1);
	}
	println!(
		"row_group_fetch_bench scenario=sparse_projection_runtime_fragmentation_budget rows={} emitted_rows={} iterations={}",
		rows, emitted, iterations,
	);
	Ok(())
}

fn bench_sparse_projection_runtime_point_read(config: &BenchConfig) -> std::io::Result<()> {
	if config.iterations <= 25 {
		return Ok(());
	}
	let runtime = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()?;
	let rows = 8_192;
	let iterations = sparse_projection_runtime_iterations(config, rows);
	let mut emitted = 0_usize;
	for cascade in [
		SparseProjectionCascadeFixture::BitPacked,
		SparseProjectionCascadeFixture::Dictionary,
	] {
		let dataset = sparse_projection_runtime_dataset(rows, cascade, 1)?;
		let slab_id = SlabId::new(9_000 + rows as u64 + cascade as u64);
		let crypto = seeded_dev_crypto_manager(TenantId::ZERO)?;
		let encrypted = sparse_runtime_encrypted_dataset(&dataset, &crypto, slab_id)?;
		let store = Arc::new(CountingBenchObjectStore::new(Arc::new(
			MockObjectStore::new(MockObjectStoreConfig::default()),
		)));
		let uri = runtime.block_on(store.put(
			&format!(
				"sparse-runtime-point-read/{}_{}.wnd",
				rows,
				cascade.as_str()
			),
			&encrypted.bytes,
		))?;
		let env = SparseProjectionRuntimeEnv {
			runtime: &runtime,
			store,
			uri: &uri,
			crypto: &crypto,
			slab_id,
			object_size: encrypted.bytes.len() as u64,
		};
		let measurement = measure_sparse_projection_runtime_case(
			&dataset,
			SparseProjectionMaskShape::SingleFirstRow,
			iterations,
			&env,
			true,
			8,
		)?;
		print_sparse_projection_runtime_measure(
			"sparse_projection_runtime_point_read_comparator",
			&dataset,
			sparse_projection_runtime_actual_cascade(&dataset),
			cascade.as_str(),
			SparseProjectionMaskShape::SingleFirstRow,
			iterations,
			&measurement,
			" projection_columns=1 comparator=adr052_simd_page_lower_bound",
		);
		emitted = emitted.saturating_add(1);
	}
	println!(
		"row_group_fetch_bench scenario=sparse_projection_runtime_point_read_comparator rows={} emitted_rows={} iterations={}",
		rows, emitted, iterations,
	);
	Ok(())
}

fn bench_sparse_projection_runtime_determinism(config: &BenchConfig) -> std::io::Result<()> {
	if config.iterations <= 25 {
		return Ok(());
	}
	let runtime = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()?;
	let rows = 8_192;
	let iterations = 1;
	let shape = SparseProjectionMaskShape::ClusteredFivePercent;
	let left =
		sparse_projection_runtime_dataset(rows, SparseProjectionCascadeFixture::BitPacked, 1)?;
	let right =
		sparse_projection_runtime_dataset(rows, SparseProjectionCascadeFixture::BitPacked, 1)?;
	let left_crypto = seeded_dev_crypto_manager(TenantId::ZERO)?;
	let right_crypto = seeded_dev_crypto_manager(TenantId::ZERO)?;
	let left_slab = SlabId::new(10_000 + rows as u64);
	let right_slab = SlabId::new(10_100 + rows as u64);
	let left_encrypted = sparse_runtime_encrypted_dataset(&left, &left_crypto, left_slab)?;
	let right_encrypted = sparse_runtime_encrypted_dataset(&right, &right_crypto, right_slab)?;
	let left_store = Arc::new(CountingBenchObjectStore::new(Arc::new(
		MockObjectStore::new(MockObjectStoreConfig::default()),
	)));
	let right_store = Arc::new(CountingBenchObjectStore::new(Arc::new(
		MockObjectStore::new(MockObjectStoreConfig::default()),
	)));
	let left_uri = runtime.block_on(left_store.put(
		&format!("sparse-runtime-determinism/left_{}.wnd", rows),
		&left_encrypted.bytes,
	))?;
	let right_uri = runtime.block_on(right_store.put(
		&format!("sparse-runtime-determinism/right_{}.wnd", rows),
		&right_encrypted.bytes,
	))?;
	let left_env = SparseProjectionRuntimeEnv {
		runtime: &runtime,
		store: left_store,
		uri: &left_uri,
		crypto: &left_crypto,
		slab_id: left_slab,
		object_size: left_encrypted.bytes.len() as u64,
	};
	let right_env = SparseProjectionRuntimeEnv {
		runtime: &runtime,
		store: right_store,
		uri: &right_uri,
		crypto: &right_crypto,
		slab_id: right_slab,
		object_size: right_encrypted.bytes.len() as u64,
	};
	let left_measure =
		measure_sparse_projection_runtime_case(&left, shape, iterations, &left_env, true, 8)?;
	let right_measure =
		measure_sparse_projection_runtime_case(&right, shape, iterations, &right_env, true, 8)?;
	let deterministic = left_measure.route == right_measure.route
		&& left_measure.expected_rows == right_measure.expected_rows
		&& left_measure.sparse.projection_plaintext_bytes
			== right_measure.sparse.projection_plaintext_bytes
		&& left_measure.sparse.selected_ranges == right_measure.sparse.selected_ranges
		&& left_measure.sparse.selected_simd_pages == right_measure.sparse.selected_simd_pages;
	print_sparse_projection_runtime_measure(
		"sparse_projection_runtime_cross_replica_determinism",
		&left,
		sparse_projection_runtime_actual_cascade(&left),
		SparseProjectionCascadeFixture::BitPacked.as_str(),
		shape,
		iterations,
		&left_measure,
		if deterministic {
			" projection_columns=1 deterministic=true"
		} else {
			" projection_columns=1 deterministic=false"
		},
	);
	if !deterministic {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"sparse runtime cross-replica determinism mismatch",
		));
	}
	println!(
		"row_group_fetch_bench scenario=sparse_projection_runtime_cross_replica_determinism rows={} emitted_rows=1 iterations={}",
		rows, iterations,
	);
	Ok(())
}

fn bench_sparse_projection_planner(config: &BenchConfig) -> std::io::Result<()> {
	let started = Instant::now();
	let mut emitted = 0_usize;
	for rows in sparse_projection_matrix_rows(config) {
		for cascade in SparseProjectionCascadeFixture::ALL {
			let fixture = sparse_projection_encoded_fixture(*rows, cascade)?;
			for projection_columns in sparse_projection_projection_counts(config) {
				for shape in SparseProjectionMaskShape::ALL {
					let estimate =
						estimate_sparse_projection(&fixture, *rows, shape, *projection_columns, 8);
					println!(
						"row_group_fetch_measure scenario=sparse_projection_page_estimate rows={} columns=3 shape=int64_clustered projected_cascade={} actual_cascade={} projection_columns={} mask_shape={} selected_groups={} dense_projection_bytes={} sparse_projection_bytes={} sparse_ranges={} selected_simd_pages={} per_buffer_range_cap=8 sparse_selected={} decline={}",
						rows,
						cascade.as_str(),
						fixture.actual_cascade,
						projection_columns,
						shape.as_str(),
						if matches!(shape, SparseProjectionMaskShape::None) {
							0
						} else {
							1
						},
						fixture
							.dense_bytes
							.saturating_mul(*projection_columns as u64),
						estimate.sparse_bytes,
						estimate.sparse_ranges,
						estimate.selected_simd_pages,
						estimate.sparse_selected,
						estimate.decline,
					);
					emitted = emitted.saturating_add(1);
				}
			}
		}
		let eligible =
			sparse_projection_encoded_fixture(*rows, SparseProjectionCascadeFixture::BitPacked)?;
		let dense_only =
			sparse_projection_encoded_fixture(*rows, SparseProjectionCascadeFixture::Plain)?;
		let dense_bytes = eligible.dense_bytes.saturating_add(dense_only.dense_bytes);
		println!(
			"row_group_fetch_measure scenario=sparse_projection_mixed_cascade rows={} columns=3 shape=int64_clustered projected_cascade=bit_packed_plus_plain actual_cascade=bit_packed,plain projection_columns=2 mask_shape=clustered_first_page selected_groups=1 dense_projection_bytes={} sparse_projection_bytes={} sparse_ranges=2 selected_simd_pages={} per_buffer_range_cap=8 sparse_selected=false decline=cascade_not_projection_eligible",
			rows,
			dense_bytes,
			dense_bytes,
			eligible.pages.len().saturating_add(dense_only.pages.len()),
		);
		emitted = emitted.saturating_add(1);
		for per_buffer_range_cap in [2_usize, 4, 8, 16] {
			let estimate = estimate_sparse_projection(
				&eligible,
				*rows,
				SparseProjectionMaskShape::RandomOnePercent,
				1,
				per_buffer_range_cap,
			);
			println!(
				"row_group_fetch_measure scenario=sparse_projection_fragmentation_budget rows={} columns=3 shape=int64_clustered projected_cascade=bit_packed actual_cascade={} projection_columns=1 mask_shape=random_1p selected_groups=1 dense_projection_bytes={} sparse_projection_bytes={} sparse_ranges={} selected_simd_pages={} per_buffer_range_cap={} sparse_selected={} decline={}",
				rows,
				eligible.actual_cascade,
				eligible.dense_bytes,
				estimate.sparse_bytes,
				estimate.sparse_ranges,
				estimate.selected_simd_pages,
				per_buffer_range_cap,
				estimate.sparse_selected,
				estimate.decline,
			);
			emitted = emitted.saturating_add(1);
		}
	}
	let elapsed = bench(config.iterations, || {
		black_box(emitted);
	});
	let micros_per_op = elapsed.as_secs_f64() * 1_000_000.0 / config.iterations as f64;
	println!(
		"row_group_fetch_bench scenario=sparse_projection_page_estimate rows=mixed columns=3 shape=int64_clustered matrix_rows={} emitted_rows={} iterations={} total_ms={:.3} micros_per_op={:.3} wall_ms={:.3}",
		sparse_projection_matrix_rows(config)
			.iter()
			.map(|rows| rows.to_string())
			.collect::<Vec<_>>()
			.join(","),
		emitted,
		config.iterations,
		elapsed.as_secs_f64() * 1_000.0,
		micros_per_op,
		started.elapsed().as_secs_f64() * 1_000.0,
	);
	Ok(())
}

fn bench_adr061_chunk_layout_matrix(config: &BenchConfig) -> std::io::Result<()> {
	bench_adr061_macro_block_runtime(config)?;
	bench_adr061_warehouse_model();
	Ok(())
}

fn bench_adr062_row_group_sizing_profile_matrix(config: &BenchConfig) -> std::io::Result<()> {
	bench_adr062_candidate_sweep();
	bench_adr062_scan_profile_actual(config)?;
	bench_adr062_scan_scale_model();
	bench_adr062_sparse_projection_model();
	bench_adr062_stats_prune_model();
	bench_adr062_compression_loss(config)?;
	bench_adr062_cross_replica_determinism()?;
	Ok(())
}

fn bench_adr063_page_level_predicate_stats_matrix(config: &BenchConfig) -> std::io::Result<()> {
	bench_adr063_predicate_selectivity_matrix()?;
	bench_adr063_boundary_matrix()?;
	bench_adr063_type_gate_matrix();
	bench_adr063_cascade_gate_matrix();
	bench_adr063_writer_cost(config)?;
	bench_adr063_cross_replica_determinism()?;
	Ok(())
}

#[derive(Clone, Copy, Debug)]
enum Adr063Distribution {
	Clustered,
	RandomUniform,
	AllDisjoint,
	AllExact,
}

impl Adr063Distribution {
	fn as_str(self) -> &'static str {
		match self {
			Self::Clustered => "clustered",
			Self::RandomUniform => "random_uniform",
			Self::AllDisjoint => "all_disjoint",
			Self::AllExact => "all_exact",
		}
	}
}

#[derive(Clone, Debug, Default)]
struct Adr063PageStatsAnalysis {
	row_group_directory_bytes: usize,
	encoding_directory_bytes_without_page_stats: usize,
	encoding_directory_bytes_with_page_stats: usize,
	combined_metadata_bytes: usize,
	page_stats_bytes: usize,
	page_stats_count: usize,
	page_count: usize,
	dense_predicate_plaintext_bytes: u64,
	narrowed_predicate_plaintext_bytes: u64,
	narrowed_predicate_ciphertext_bytes: u64,
	dense_predicate_ciphertext_bytes: u64,
	selected_range_count: usize,
	disjoint_pages: usize,
	exact_pages: usize,
	maybe_pages: usize,
	exact_match_rows: u64,
	actual_cascade: &'static str,
}

fn bench_adr063_predicate_selectivity_matrix() -> std::io::Result<()> {
	for profile in [
		RowGroupSizingProfile::Scan,
		RowGroupSizingProfile::PointRead,
	] {
		let rows = match profile {
			RowGroupSizingProfile::Scan => ROW_GROUP_SCAN_TARGET_ROWS as usize,
			RowGroupSizingProfile::PointRead => ROW_GROUP_POINT_READ_MIN_PAGED_RECORD_ROWS as usize,
		};
		for (distribution, selectivity) in [
			(Adr063Distribution::Clustered, 1_u64),
			(Adr063Distribution::Clustered, 5),
			(Adr063Distribution::Clustered, 10),
			(Adr063Distribution::Clustered, 25),
			(Adr063Distribution::RandomUniform, 1),
			(Adr063Distribution::RandomUniform, 5),
			(Adr063Distribution::RandomUniform, 10),
			(Adr063Distribution::RandomUniform, 25),
		] {
			let fixture = adr063_fixture(rows, 1, distribution, selectivity, profile, true)?;
			let atom = PredicateAtom {
				op: PredicateAtomOp::Eq,
				literal: Some(ColumnPredicateLiteralV1::Int(1)),
			};
			let analysis = adr063_analyze_predicate_pages(&fixture.directory, &atom)?;
			adr063_print_predicate_analysis(
				"profile_selectivity",
				profile,
				rows,
				1,
				distribution,
				selectivity,
				&analysis,
			);
		}
		for (distribution, atom) in [
			(
				Adr063Distribution::AllDisjoint,
				PredicateAtom {
					op: PredicateAtomOp::Eq,
					literal: Some(ColumnPredicateLiteralV1::Int(1)),
				},
			),
			(
				Adr063Distribution::AllExact,
				PredicateAtom {
					op: PredicateAtomOp::Eq,
					literal: Some(ColumnPredicateLiteralV1::Int(1)),
				},
			),
		] {
			let fixture = adr063_fixture(rows, 1, distribution, 100, profile, true)?;
			let analysis = adr063_analyze_predicate_pages(&fixture.directory, &atom)?;
			adr063_print_predicate_analysis(
				"boundary_classification",
				profile,
				rows,
				1,
				distribution,
				100,
				&analysis,
			);
		}
	}
	Ok(())
}

#[allow(clippy::too_many_arguments)]
fn adr063_print_predicate_analysis(
	scenario_suffix: &str,
	profile: RowGroupSizingProfile,
	rows: usize,
	columns: usize,
	distribution: Adr063Distribution,
	selectivity: u64,
	analysis: &Adr063PageStatsAnalysis,
) {
	let route = if analysis.narrowed_predicate_plaintext_bytes
		>= analysis.dense_predicate_plaintext_bytes
		&& analysis.dense_predicate_plaintext_bytes != 0
	{
		"dense_fallback"
	} else {
		"page_narrowed"
	};
	let reduction_micros = fraction_micros(
		analysis.dense_predicate_plaintext_bytes,
		analysis.narrowed_predicate_plaintext_bytes,
	);
	println!(
		"row_group_fetch_measure scenario=adr063_{} profile={} rows={} columns={} distribution={} selectivity_percent={} cascade={} route={} page_stats={} page_count={} disjoint_pages={} exact_pages={} maybe_pages={} exact_match_rows={} selected_ranges={} dense_predicate_plaintext_bytes={} narrowed_predicate_plaintext_bytes={} dense_predicate_ciphertext_bytes={} narrowed_predicate_ciphertext_bytes={} predicate_byte_reduction_micros={} row_group_directory_bytes={} encoding_directory_bytes_before_page_stats={} encoding_directory_bytes_after_page_stats={} page_stats_bytes={} combined_metadata_bytes={} probe_64k_fit={}",
		scenario_suffix,
		profile.as_str(),
		rows,
		columns,
		distribution.as_str(),
		selectivity,
		analysis.actual_cascade,
		route,
		analysis.page_stats_count,
		analysis.page_count,
		analysis.disjoint_pages,
		analysis.exact_pages,
		analysis.maybe_pages,
		analysis.exact_match_rows,
		analysis.selected_range_count,
		analysis.dense_predicate_plaintext_bytes,
		analysis.narrowed_predicate_plaintext_bytes,
		analysis.dense_predicate_ciphertext_bytes,
		analysis.narrowed_predicate_ciphertext_bytes,
		reduction_micros,
		analysis.row_group_directory_bytes,
		analysis.encoding_directory_bytes_without_page_stats,
		analysis.encoding_directory_bytes_with_page_stats,
		analysis.page_stats_bytes,
		analysis.combined_metadata_bytes,
		analysis.combined_metadata_bytes <= 64 * 1024,
	);
}

fn bench_adr063_boundary_matrix() -> std::io::Result<()> {
	for (profile, rows) in [
		(
			RowGroupSizingProfile::PointRead,
			ROW_GROUP_POINT_READ_TARGET_ROWS as usize,
		),
		(
			RowGroupSizingProfile::Scan,
			(ROW_GROUP_SCAN_TARGET_ROWS - 1) as usize,
		),
		(
			RowGroupSizingProfile::Scan,
			ROW_GROUP_SCAN_TARGET_ROWS as usize,
		),
		(
			RowGroupSizingProfile::Scan,
			(ROW_GROUP_SCAN_TARGET_ROWS + 1) as usize,
		),
	] {
		let fixture = adr063_fixture(rows, 1, Adr063Distribution::Clustered, 5, profile, true)?;
		let page_directory = fixture.directory.page_directory.as_ref();
		let page_stats_count = fixture
			.directory
			.encoding_directory
			.as_ref()
			.map(|directory| {
				directory
					.buffers
					.iter()
					.map(|buffer| buffer.page_stats.len())
					.sum::<usize>()
			})
			.unwrap_or(0);
		println!(
			"row_group_fetch_measure scenario=adr063_boundary profile={} rows={} columns=1 distribution={} row_groups={} directory_present={} encoding_directory_present={} page_stats={} record_bytes={} plaintext_bytes={} row_group_directory_bytes={}",
			profile.as_str(),
			rows,
			Adr063Distribution::Clustered.as_str(),
			page_directory
				.map(|directory| directory.row_groups.len())
				.unwrap_or(0),
			page_directory.is_some(),
			fixture.directory.encoding_directory.is_some(),
			page_stats_count,
			fixture.encoded_bytes,
			fixture.plaintext_bytes,
			fixture.directory_value_bytes,
		);
	}
	Ok(())
}

fn bench_adr063_type_gate_matrix() {
	let cases = [
		("int8", DataType::Int8),
		("int16", DataType::Int16),
		("int32", DataType::Int32),
		("int64", DataType::Int64),
		("uint8", DataType::UInt8),
		("uint16", DataType::UInt16),
		("uint32", DataType::UInt32),
		("uint64", DataType::UInt64),
		(
			"timestamp_ms",
			DataType::Timestamp(TimeUnit::Millisecond, None),
		),
		(
			"timestamp_us",
			DataType::Timestamp(TimeUnit::Microsecond, None),
		),
		(
			"timestamp_ns",
			DataType::Timestamp(TimeUnit::Nanosecond, None),
		),
		("date32", DataType::Date32),
		("date64", DataType::Date64),
		("boolean", DataType::Boolean),
		("float64", DataType::Float64),
		("utf8", DataType::Utf8),
	];
	for (label, data_type) in cases {
		println!(
			"row_group_fetch_measure scenario=adr063_type_gate type={} fixed_width_page_stats_supported={}",
			label,
			fixed_width_page_stats_supported_type(&data_type),
		);
	}
}

fn bench_adr063_cascade_gate_matrix() {
	let cascades = [
		(
			"bit_packed",
			EncodingCascade::BitPacked {
				bits: BitsPerValue::new(8),
				base: BaseValue::ZERO,
			},
		),
		(
			"dictionary_plain",
			EncodingCascade::Dictionary {
				dict_byte_len: RecordBodyByteLen::new(16),
				dict_value_count: DictValueCount::new(2),
				inner: Box::new(EncodingLeafCascade::Plain),
			},
		),
		(
			"dictionary_bit_packed",
			EncodingCascade::Dictionary {
				dict_byte_len: RecordBodyByteLen::new(16),
				dict_value_count: DictValueCount::new(2),
				inner: Box::new(EncodingLeafCascade::BitPacked {
					bits: BitsPerValue::new(1),
					base: BaseValue::ZERO,
				}),
			},
		),
		(
			"rle_plain",
			EncodingCascade::Rle {
				run_count: RunCount::new(4),
				inner: Box::new(EncodingLeafCascade::Plain),
			},
		),
		(
			"delta",
			EncodingCascade::Delta {
				base: BaseValue::ZERO,
				inner: Box::new(EncodingLeafCascade::Plain),
			},
		),
		("plain", EncodingCascade::Plain),
		("lz4_frame", EncodingCascade::Lz4Frame),
	];
	let default_compute = EncodingComputeConfig::default();
	for (label, cascade) in cascades {
		let evaluator = evaluator_kind_for_cascade(&cascade);
		let allow_listed = evaluator
			.map(|kind| default_compute.predicate_evaluator_enabled(kind))
			.unwrap_or(false);
		println!(
			"row_group_fetch_measure scenario=adr063_cascade_gate cascade={} evaluator={} allow_listed={} emits_pages_in_v1={} page_stats_cascade_eligible={}",
			label,
			evaluator.map(|kind| kind.as_str()).unwrap_or("none"),
			allow_listed,
			cascade.emits_pages_in_v1(),
			page_stats_cascade_eligible(&cascade),
		);
	}
}

fn bench_adr063_writer_cost(config: &BenchConfig) -> std::io::Result<()> {
	let rows = ROW_GROUP_SCAN_TARGET_ROWS as usize;
	let columns = 1;
	let iterations = config.iterations.min(10).max(1);
	let batch = adr063_bench_batch(rows, columns, Adr063Distribution::Clustered, 5)?;
	let table_descriptor = descriptor(batch.schema().as_ref(), columns)?;
	let source_rows = ordered_source_rows(rows);
	let encoding_config = adr063_encoding_config();
	for stats_enabled in [false, true] {
		let row_group_config =
			row_group_profile_config_for(RowGroupSizingProfile::Scan, true, stats_enabled);
		let elapsed = bench(iterations, || {
			let encoded = encode_multirow_from_survivors_with_row_group_and_encoding_config(
				black_box(&batch),
				black_box(&source_rows),
				black_box(&table_descriptor),
				RecordBodyCompressionPolicy::Uncompressed,
				black_box(&row_group_config),
				black_box(&encoding_config),
			)
			.expect("ADR 063 writer-cost encode");
			black_box(encoded.frame.len());
		});
		let fixture = adr063_fixture(
			rows,
			columns,
			Adr063Distribution::Clustered,
			5,
			RowGroupSizingProfile::Scan,
			stats_enabled,
		)?;
		let page_stats_count = fixture
			.directory
			.encoding_directory
			.as_ref()
			.map(|directory| {
				directory
					.buffers
					.iter()
					.map(|buffer| buffer.page_stats.len())
					.sum::<usize>()
			})
			.unwrap_or(0);
		println!(
			"row_group_fetch_bench scenario=adr063_writer_cost profile=scan rows={} columns={} distribution={} stats_enabled={} page_stats={} iterations={} total_ms={:.3} micros_per_op={:.3} record_bytes={} plaintext_bytes={}",
			rows,
			columns,
			Adr063Distribution::Clustered.as_str(),
			stats_enabled,
			page_stats_count,
			iterations,
			elapsed.as_secs_f64() * 1_000.0,
			elapsed.as_secs_f64() * 1_000_000.0 / iterations as f64,
			fixture.encoded_bytes,
			fixture.plaintext_bytes,
		);
	}
	Ok(())
}

fn bench_adr063_cross_replica_determinism() -> std::io::Result<()> {
	for profile in [
		RowGroupSizingProfile::PointRead,
		RowGroupSizingProfile::Scan,
	] {
		let rows = match profile {
			RowGroupSizingProfile::PointRead => ROW_GROUP_POINT_READ_MIN_PAGED_RECORD_ROWS as usize,
			RowGroupSizingProfile::Scan => ROW_GROUP_SCAN_TARGET_ROWS as usize,
		};
		let columns = 1;
		let batch = adr063_bench_batch(rows, columns, Adr063Distribution::Clustered, 5)?;
		let table_descriptor = descriptor(batch.schema().as_ref(), columns)?;
		let source_rows = ordered_source_rows(rows);
		let row_group_config = row_group_profile_config_for(profile, true, true);
		let encoding_config = adr063_encoding_config();
		let left = encode_multirow_from_survivors_with_row_group_and_encoding_config(
			&batch,
			&source_rows,
			&table_descriptor,
			RecordBodyCompressionPolicy::Uncompressed,
			&row_group_config,
			&encoding_config,
		)?;
		let right = encode_multirow_from_survivors_with_row_group_and_encoding_config(
			&batch,
			&source_rows,
			&table_descriptor,
			RecordBodyCompressionPolicy::Uncompressed,
			&row_group_config,
			&encoding_config,
		)?;
		let fixture = adr063_fixture(
			rows,
			columns,
			Adr063Distribution::Clustered,
			5,
			profile,
			true,
		)?;
		let page_stats_count = fixture
			.directory
			.encoding_directory
			.as_ref()
			.map(|directory| {
				directory
					.buffers
					.iter()
					.map(|buffer| buffer.page_stats.len())
					.sum::<usize>()
			})
			.unwrap_or(0);
		println!(
			"row_group_fetch_measure scenario=adr063_cross_replica_determinism profile={} rows={} columns={} distribution={} page_stats={} byte_equal={} digest={} bytes={}",
			profile.as_str(),
			rows,
			columns,
			Adr063Distribution::Clustered.as_str(),
			page_stats_count,
			left.frame == right.frame,
			blake3::hash(&left.frame).to_hex(),
			left.frame.len(),
		);
	}
	Ok(())
}

fn adr063_fixture(
	rows: usize,
	columns: usize,
	distribution: Adr063Distribution,
	selectivity: u64,
	profile: RowGroupSizingProfile,
	stats_enabled: bool,
) -> std::io::Result<EncodedFixture> {
	let batch = adr063_bench_batch(rows, columns, distribution, selectivity)?;
	let table_descriptor = descriptor(batch.schema().as_ref(), columns)?;
	let row_group_config = row_group_profile_config_for(profile, true, stats_enabled);
	let encoding_config = adr063_encoding_config();
	let encoded = encode_multirow_from_survivors_with_row_group_and_encoding_config(
		&batch,
		&ordered_source_rows(rows),
		&table_descriptor,
		RecordBodyCompressionPolicy::Uncompressed,
		&row_group_config,
		&encoding_config,
	)?;
	let directory = parse_record_buffer_directory(
		TieredRecordId {
			slab_id: SlabId::new(63),
			record_offset: RecordBodyByteOffset::ZERO,
		},
		PlaintextByteRange {
			start: 0,
			len: encoded.frame.len() as u64,
		},
		&encoded.frame,
		&table_descriptor,
	)
	.map_err(|err| {
		Error::new(
			ErrorKind::InvalidData,
			format!("ADR 063 directory parse: {err}"),
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

fn adr063_encoding_config() -> EncodingWriterConfig {
	EncodingWriterConfig {
		enabled: true,
		target_simd_page_values: SimdPageValueCount::new(1_024),
		min_buffer_bytes_for_encoding: RecordBodyByteLen::new(1),
		enabled_cascades: [EncodingCascadeKind::Plain, EncodingCascadeKind::Dictionary]
			.into_iter()
			.collect(),
		..EncodingWriterConfig::default()
	}
}

fn adr063_bench_batch(
	rows: usize,
	columns: usize,
	distribution: Adr063Distribution,
	selectivity: u64,
) -> std::io::Result<RecordBatch> {
	let fields = (0..columns)
		.map(|column| {
			let data_type = if column == 0 {
				DataType::UInt8
			} else {
				DataType::Utf8
			};
			Field::new(format!("c{column}"), data_type, false)
		})
		.collect::<Vec<_>>();
	let arrays = (0..columns)
		.map(|column| {
			if column == 0 {
				let values = (0..rows)
					.map(|row| adr063_value_for_row(row, rows, distribution, selectivity))
					.collect::<Vec<_>>();
				Arc::new(UInt8Array::from(values)) as ArrayRef
			} else {
				let values = (0..rows)
					.map(|row| format!("p{:02}-{:08}-{:05}", column, row, row % 100_000))
					.collect::<Vec<_>>();
				Arc::new(StringArray::from(values)) as ArrayRef
			}
		})
		.collect::<Vec<_>>();
	RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
		.map_err(|err| Error::new(ErrorKind::InvalidData, format!("ADR 063 batch: {err}")))
}

fn adr063_value_for_row(
	row: usize,
	rows: usize,
	distribution: Adr063Distribution,
	selectivity: u64,
) -> u8 {
	let selected_rows = rows
		.saturating_mul(selectivity.min(100) as usize)
		.saturating_add(99)
		/ 100;
	match distribution {
		Adr063Distribution::RandomUniform => {
			let mixed = (row as u64)
				.wrapping_mul(6_364_136_223_846_793_005)
				.wrapping_add(1_442_695_040_888_963_407)
				.rotate_left(17);
			u8::from(mixed % 100 < selectivity.min(100))
		}
		Adr063Distribution::Clustered => u8::from(row < selected_rows),
		Adr063Distribution::AllDisjoint => 0,
		Adr063Distribution::AllExact => 1,
	}
}

fn adr063_analyze_predicate_pages(
	directory: &RecordBufferDirectory,
	atom: &PredicateAtom,
) -> std::io::Result<Adr063PageStatsAnalysis> {
	let page_directory = directory
		.page_directory
		.as_ref()
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "ADR 063 missing page directory"))?;
	let encoding_directory = directory
		.encoding_directory
		.as_ref()
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "ADR 063 missing encoding directory"))?;
	let encoding_directory_bytes_with_page_stats =
		serialize_encoding_directory_value(encoding_directory)?.len();
	let mut without_page_stats = encoding_directory.clone();
	for buffer in &mut without_page_stats.buffers {
		buffer.page_stats.clear();
	}
	let encoding_directory_bytes_without_page_stats =
		serialize_encoding_directory_value(&without_page_stats)?.len();
	let row_group_directory_bytes = serialize_page_directory_value(page_directory)?.len();
	let (physical_ordinal, value_buffer_ordinal) = adr063_value_buffer_ordinal(directory, 0)?;
	let mut analysis = Adr063PageStatsAnalysis {
		row_group_directory_bytes,
		encoding_directory_bytes_without_page_stats,
		encoding_directory_bytes_with_page_stats,
		combined_metadata_bytes: row_group_directory_bytes
			+ encoding_directory_bytes_with_page_stats,
		page_stats_bytes: encoding_directory_bytes_with_page_stats
			.saturating_sub(encoding_directory_bytes_without_page_stats),
		..Adr063PageStatsAnalysis::default()
	};
	let mut selected_ranges = Vec::<RecordBodyByteRange>::new();
	for group in &page_directory.row_groups {
		let page = page_directory
			.buffer_pages
			.iter()
			.find(|page| {
				page.row_group_index == group.row_group_index
					&& page.ipc_column_ordinal == IpcColumnOrdinal::new(physical_ordinal as u32)
					&& page.ipc_buffer_ordinal == value_buffer_ordinal
					&& page.range.len.get() > 0
			})
			.ok_or_else(|| Error::new(ErrorKind::InvalidData, "ADR 063 value page missing"))?;
		let encoding = wendelon::storage::encoding::materialize::find_buffer_encoding(
			encoding_directory,
			group.row_group_index,
			value_buffer_ordinal,
			page.range,
		)
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "ADR 063 value encoding missing"))?;
		let cascade = encoding_directory
			.cascades
			.get(encoding.cascade_index.get() as usize)
			.ok_or_else(|| Error::new(ErrorKind::InvalidData, "ADR 063 cascade missing"))?;
		analysis.actual_cascade = adr063_cascade_label(cascade);
		analysis.dense_predicate_plaintext_bytes = analysis
			.dense_predicate_plaintext_bytes
			.saturating_add(encoding.encoded_range.len.get());
		analysis.page_stats_count = analysis
			.page_stats_count
			.saturating_add(encoding.page_stats.len());
		let pages = expand_page_descriptors(encoding).map_err(|code| {
			Error::new(
				ErrorKind::InvalidData,
				format!("ADR 063 page expansion rejected: {code:?}"),
			)
		})?;
		analysis.page_count = analysis.page_count.saturating_add(pages.len());
		for (page_descriptor, stat) in pages.iter().zip(&encoding.page_stats) {
			let classification = classify_page_stats_for_atom(
				stat,
				page_descriptor.value_count.get(),
				&DataType::UInt8,
				atom,
			)
			.ok_or_else(|| {
				Error::new(
					ErrorKind::InvalidData,
					"ADR 063 predicate page classification unsupported",
				)
			})?;
			match classification {
				PagePredicateClassification::Disjoint => {
					analysis.disjoint_pages = analysis.disjoint_pages.saturating_add(1);
				}
				PagePredicateClassification::ExactMatch => {
					analysis.exact_pages = analysis.exact_pages.saturating_add(1);
					analysis.exact_match_rows = analysis
						.exact_match_rows
						.saturating_add(u64::from(page_descriptor.value_count.get()));
				}
				PagePredicateClassification::Maybe => {
					analysis.maybe_pages = analysis.maybe_pages.saturating_add(1);
					analysis.narrowed_predicate_plaintext_bytes = analysis
						.narrowed_predicate_plaintext_bytes
						.saturating_add(page_descriptor.byte_range.len.get());
					selected_ranges.push(adr063_record_body_range_for_page(
						page.range,
						encoding.encoded_range.start.get(),
						page_descriptor,
					)?);
				}
			}
		}
	}
	coalesce_body_ranges(&mut selected_ranges)?;
	analysis.selected_range_count = selected_ranges.len();
	analysis.dense_predicate_ciphertext_bytes =
		chunk_floor_fetch_bytes(analysis.dense_predicate_plaintext_bytes as usize, 16 * 1024);
	analysis.narrowed_predicate_ciphertext_bytes = selected_ranges
		.iter()
		.map(|range| chunk_floor_fetch_bytes(range.len.get() as usize, 16 * 1024))
		.sum();
	Ok(analysis)
}

fn adr063_record_body_range_for_page(
	buffer_range: RecordBodyByteRange,
	encoded_start: u32,
	page: &EncodingPageDescriptor,
) -> std::io::Result<RecordBodyByteRange> {
	let start = buffer_range
		.start
		.get()
		.checked_add(u64::from(encoded_start))
		.and_then(|start| start.checked_add(u64::from(page.byte_range.start.get())))
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "ADR 063 page range overflow"))?;
	Ok(RecordBodyByteRange {
		start: RecordBodyByteOffset::new(start),
		len: page.byte_range.len,
	})
}

fn adr063_value_buffer_ordinal(
	directory: &RecordBufferDirectory,
	physical_ordinal: usize,
) -> std::io::Result<(usize, IpcBufferIndex)> {
	let mapping = directory
		.column_to_buffers
		.get(physical_ordinal)
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "ADR 063 column buffers missing"))?;
	if mapping.buffer_count != 2 {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"ADR 063 fixed-width value buffer mapping changed",
		));
	}
	let value_buffer = mapping
		.first_buffer_ordinal
		.get()
		.checked_add(1)
		.map(IpcBufferIndex::new)
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "ADR 063 buffer ordinal overflow"))?;
	Ok((physical_ordinal, value_buffer))
}

fn adr063_cascade_label(cascade: &EncodingCascade) -> &'static str {
	match cascade {
		EncodingCascade::Plain => "plain",
		EncodingCascade::Lz4Frame => "lz4_frame",
		EncodingCascade::BitPacked { .. } => "bit_packed",
		EncodingCascade::Rle { inner, .. } => match inner.as_ref() {
			EncodingLeafCascade::Plain => "rle_plain",
			EncodingLeafCascade::Lz4Frame => "rle_lz4_frame",
			EncodingLeafCascade::BitPacked { .. } => "rle_bit_packed",
		},
		EncodingCascade::Dictionary { inner, .. } => match inner.as_ref() {
			EncodingLeafCascade::Plain => "dictionary_plain",
			EncodingLeafCascade::Lz4Frame => "dictionary_lz4_frame",
			EncodingLeafCascade::BitPacked { .. } => "dictionary_bit_packed",
		},
		EncodingCascade::Delta { .. } => "delta",
		EncodingCascade::Fsst { .. } => "fsst",
		EncodingCascade::Alp { .. } => "alp",
	}
}

fn adr062_schema_cases() -> [BenchCase; 4] {
	[
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
		BenchCase {
			rows: ROW_GROUP_SCAN_TARGET_ROWS as usize,
			columns: 4,
			shape: BenchDataShape::Int64,
		},
		BenchCase {
			rows: ROW_GROUP_SCAN_TARGET_ROWS as usize,
			columns: 4,
			shape: BenchDataShape::Mixed,
		},
	]
}

fn model_record_body_bytes(case: BenchCase) -> u64 {
	let bytes_per_row = match case.shape {
		BenchDataShape::Int64 | BenchDataShape::Float64 => case.columns as u64 * 8,
		BenchDataShape::Utf8 => case.columns as u64 * 24,
		BenchDataShape::Mixed => (0..case.columns)
			.map(|column| match column % 4 {
				0 | 1 => 8_u64,
				2 => 24_u64,
				_ => 1_u64,
			})
			.sum(),
	};
	case.rows as u64 * bytes_per_row
}

fn bench_adr062_candidate_sweep() {
	for case in adr062_schema_cases() {
		let record_bytes = model_record_body_bytes(case);
		for target in [4_096_usize, 8_192, 16_384, 32_768, 65_536, 131_072] {
			let row_groups = if case.rows >= target && record_bytes >= 1024 * 1024 {
				case.rows.div_ceil(target)
			} else {
				0
			};
			let row_group_plaintext_bytes = if row_groups == 0 {
				0
			} else {
				record_bytes.div_ceil(row_groups as u64)
			};
			println!(
				"row_group_fetch_measure scenario=adr062_scan_target_candidate profile=scan_candidate rows={} columns={} shape={} target_row_group_rows={} max_row_group_rows={} min_paged_record_rows={} record_bytes={} row_groups={} row_group_plaintext_bytes={} exact_one_entry_candidate={}",
				case.rows,
				case.columns,
				case.shape.as_str(),
				target,
				target * 2,
				target,
				record_bytes,
				row_groups,
				row_group_plaintext_bytes,
				row_groups == 1 && case.rows == target
			);
		}
	}
}

fn bench_adr062_scan_profile_actual(config: &BenchConfig) -> std::io::Result<()> {
	let case = BenchCase {
		rows: ROW_GROUP_SCAN_TARGET_ROWS as usize,
		columns: 4,
		shape: BenchDataShape::Int64,
	};
	let batch = bench_batch(case.rows, case.columns, case.shape)?;
	let table_descriptor = descriptor(batch.schema().as_ref(), case.columns)?;
	let source_rows = source_rows(case.rows);
	let disabled = row_group_profile_config_for(RowGroupSizingProfile::PointRead, false, false);
	let scan = row_group_profile_config_for(RowGroupSizingProfile::Scan, true, true);
	let point_read = row_group_profile_config_for(RowGroupSizingProfile::PointRead, true, true);

	for (profile, row_group_config) in [
		("disabled", &disabled),
		("point_read", &point_read),
		("scan", &scan),
	] {
		let elapsed = bench(config.iterations, || {
			let encoded = encode_multirow_from_survivors_with_row_group_config(
				black_box(&batch),
				black_box(&source_rows),
				black_box(&table_descriptor),
				RecordBodyCompressionPolicy::Uncompressed,
				black_box(row_group_config),
			)
			.expect("ADR 062 writer-cost encode");
			black_box(encoded.frame.len());
		});
		let micros_per_op = elapsed.as_secs_f64() * 1_000_000.0 / config.iterations as f64;
		let fixture = encoded_fixture(
			case.rows,
			&batch,
			&table_descriptor,
			RecordBodyCompressionPolicy::Uncompressed,
			row_group_config,
		)?;
		let row_groups = fixture
			.directory
			.page_directory
			.as_ref()
			.map(|directory| directory.row_groups.len())
			.unwrap_or(0);
		println!(
			"row_group_fetch_bench scenario=adr062_writer_cost profile={} rows={} columns={} shape={} row_groups={} iterations={} total_ms={:.3} micros_per_op={:.3}",
			profile,
			case.rows,
			case.columns,
			case.shape.as_str(),
			row_groups,
			config.iterations,
			elapsed.as_secs_f64() * 1_000.0,
			micros_per_op,
		);
	}

	let fixture = encoded_fixture(
		case.rows,
		&batch,
		&table_descriptor,
		RecordBodyCompressionPolicy::Uncompressed,
		&scan,
	)?;
	let page_directory = fixture
		.directory
		.page_directory
		.as_ref()
		.expect("Scan profile should emit one-entry page directory");
	let row_group_plaintext_lengths = page_directory
		.row_groups
		.iter()
		.map(|entry| entry.body_range.len.get())
		.collect::<Vec<_>>();
	let p50_plaintext = percentile_u64(row_group_plaintext_lengths.clone(), 50);
	let p95_plaintext = percentile_u64(row_group_plaintext_lengths, 95);
	let dense_p50_get_bytes = chunk_floor_fetch_bytes(p50_plaintext as usize, 16 * 1024);
	let dense_p95_get_bytes = chunk_floor_fetch_bytes(p95_plaintext as usize, 16 * 1024);
	println!(
		"row_group_fetch_measure scenario=adr062_scan_profile_directory profile=scan rows={} columns={} shape={} target_row_group_rows={} max_row_group_rows={} row_groups={} buffer_pages={} stats={} record_bytes={} plaintext_bytes={} directory_value_bytes={} directory_fraction_micros={} dense_p50_get_bytes={} dense_p95_get_bytes={}",
		case.rows,
		case.columns,
		case.shape.as_str(),
		ROW_GROUP_SCAN_TARGET_ROWS,
		ROW_GROUP_SCAN_MAX_ROWS,
		page_directory.row_groups.len(),
		page_directory.buffer_pages.len(),
		page_directory.row_group_stats.len(),
		fixture.encoded_bytes,
		fixture.plaintext_bytes,
		fixture.directory_value_bytes,
		fraction_micros(
			fixture.directory_value_bytes as u64,
			fixture.encoded_bytes.max(1)
		),
		dense_p50_get_bytes,
		dense_p95_get_bytes,
	);

	let requirement = ColumnFetchRequirement {
		output_columns: ColumnOrdinalSet::from_columns([0]),
		..ColumnFetchRequirement::default()
	};
	let selected_groups = vec![RowGroupIndex::new(0)];
	let ranges = selected_row_group_record_body_ranges(
		&fixture.directory,
		page_directory,
		&requirement,
		&selected_groups,
	)
	.map_err(|err| {
		Error::new(
			ErrorKind::InvalidData,
			format!("ADR 062 selected bytes: {err}"),
		)
	})?;
	let selected_bytes = ranges.iter().map(|range| range.len.get()).sum::<u64>();
	println!(
		"row_group_fetch_measure scenario=adr062_secondary_index_point_read_selected_bytes profile=scan rows={} columns={} shape={} selected_groups=1 total_groups={} selected_ranges={} selected_bytes={} record_bytes={} selected_fraction_micros={}",
		case.rows,
		case.columns,
		case.shape.as_str(),
		page_directory.row_groups.len(),
		ranges.len(),
		selected_bytes,
		fixture.encoded_bytes,
		fraction_micros(selected_bytes, fixture.encoded_bytes.max(1)),
	);

	for rows in [
		ROW_GROUP_POINT_READ_TARGET_ROWS as usize,
		(ROW_GROUP_SCAN_TARGET_ROWS - 1) as usize,
		ROW_GROUP_SCAN_TARGET_ROWS as usize,
		(ROW_GROUP_SCAN_TARGET_ROWS + 1) as usize,
	] {
		let boundary_case = BenchCase {
			rows,
			columns: 4,
			shape: BenchDataShape::Int64,
		};
		let boundary_batch = bench_batch(rows, boundary_case.columns, boundary_case.shape)?;
		let boundary_descriptor =
			descriptor(boundary_batch.schema().as_ref(), boundary_case.columns)?;
		let boundary_fixture = encoded_fixture(
			rows,
			&boundary_batch,
			&boundary_descriptor,
			RecordBodyCompressionPolicy::Uncompressed,
			&scan,
		)?;
		let row_groups = boundary_fixture
			.directory
			.page_directory
			.as_ref()
			.map(|directory| directory.row_groups.len())
			.unwrap_or(0);
		println!(
			"row_group_fetch_measure scenario=adr062_scan_boundary profile=scan rows={} columns={} shape={} target_row_group_rows={} row_groups={} directory_present={} record_bytes={} directory_value_bytes={}",
			rows,
			boundary_case.columns,
			boundary_case.shape.as_str(),
			ROW_GROUP_SCAN_TARGET_ROWS,
			row_groups,
			boundary_fixture.directory.page_directory.is_some(),
			boundary_fixture.encoded_bytes,
			boundary_fixture.directory_value_bytes,
		);
	}
	Ok(())
}

fn bench_adr062_scan_scale_model() {
	const RECORD_BYTES: usize = 4 * 1024 * 1024;
	for rows in [16_777_216_usize, 100_000_000] {
		let records = rows.div_ceil(ROW_GROUP_SCAN_TARGET_ROWS as usize);
		for chunk_size in SLAB_ENCRYPTION_CHUNK_SIZES {
			let dense_get_bytes = chunk_floor_fetch_bytes(RECORD_BYTES, chunk_size);
			println!(
				"row_group_fetch_measure scenario=adr062_scan_scale_model profile=scan rows={} records={} row_group_rows={} record_bytes={} chunk_size={} dense_get_ranges=1 dense_p50_get_bytes={} dense_p95_get_bytes={} get_count_regression_micros=1000000",
				rows,
				records,
				ROW_GROUP_SCAN_TARGET_ROWS,
				RECORD_BYTES,
				chunk_size,
				dense_get_bytes,
				dense_get_bytes,
			);
		}
	}
}

fn bench_adr062_sparse_projection_model() {
	const RECORD_BYTES: usize = 4 * 1024 * 1024;
	const SELECTED_PAGE_BYTES: usize = 4 * 1024;
	const BASELINE_CHUNK_BYTES: usize = 64 * 1024;
	for selectivity in [1_u64, 5] {
		let selected_pages = selectivity;
		let baseline_sparse_ciphertext =
			selected_pages * chunk_floor_fetch_bytes(SELECTED_PAGE_BYTES, BASELINE_CHUNK_BYTES);
		let scan_sparse_ciphertext =
			selected_pages * chunk_floor_fetch_bytes(SELECTED_PAGE_BYTES, 16 * 1024);
		println!(
			"row_group_fetch_measure scenario=adr062_sparse_projection_model profile=scan rows={} row_group_rows={} record_bytes={} mask_shape=clustered_{}p route=sparse chunk_size=16384 scan_sparse_ciphertext_bytes={} baseline_64k_sparse_ciphertext_bytes={} sparse_reduction_micros={} wall_clock_regression_micros=1000000",
			ROW_GROUP_SCAN_TARGET_ROWS,
			ROW_GROUP_SCAN_TARGET_ROWS,
			RECORD_BYTES,
			selectivity,
			scan_sparse_ciphertext,
			baseline_sparse_ciphertext,
			fraction_micros(baseline_sparse_ciphertext, scan_sparse_ciphertext),
		);
	}
}

fn bench_adr062_stats_prune_model() {
	let point_read_groups = (ROW_GROUP_SCAN_TARGET_ROWS / ROW_GROUP_POINT_READ_TARGET_ROWS) as u64;
	for (predicate, point_read_survivors, scan_survivors) in [
		("clustered_broad_75p", point_read_groups * 3 / 4, 1_u64),
		("sorted_broad_75p", point_read_groups * 3 / 4, 1_u64),
		("random_uniform_recorded", point_read_groups, 1_u64),
	] {
		let point_read_survival_micros = fraction_micros(point_read_survivors, point_read_groups);
		let scan_survival_micros = fraction_micros(scan_survivors, 1);
		println!(
			"row_group_fetch_measure scenario=adr062_stats_prune_model predicate={} point_read_groups={} point_read_survivors={} point_read_survival_micros={} scan_groups=1 scan_survivors={} scan_survival_micros={} scan_vs_point_read_survival_ratio_micros={}",
			predicate,
			point_read_groups,
			point_read_survivors,
			point_read_survival_micros,
			scan_survivors,
			scan_survival_micros,
			fraction_micros(scan_survival_micros, point_read_survival_micros),
		);
	}
}

fn bench_adr062_compression_loss(config: &BenchConfig) -> std::io::Result<()> {
	for shape in [
		BenchDataShape::Int64,
		BenchDataShape::Float64,
		BenchDataShape::Utf8,
		BenchDataShape::Mixed,
	] {
		let case = BenchCase {
			rows: ROW_GROUP_SCAN_TARGET_ROWS as usize,
			columns: 4,
			shape,
		};
		let batch = bench_batch(case.rows, case.columns, case.shape)?;
		let descriptor = descriptor(batch.schema().as_ref(), case.columns)?;
		let disabled = row_group_profile_config_for(RowGroupSizingProfile::PointRead, false, false);
		let scan = row_group_profile_config_for(RowGroupSizingProfile::Scan, true, true);
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
			&scan,
		)?;
		let loss_micros = if single.encoded_byte_len == 0 {
			0
		} else {
			((row_group.encoded_byte_len as i128 - single.encoded_byte_len as i128) * 1_000_000
				/ single.encoded_byte_len as i128) as i64
		};
		println!(
			"row_group_fetch_measure scenario=adr062_scan_compression_loss profile=scan rows={} columns={} shape={} iterations={} single_lz4_bytes={} scan_lz4_bytes={} single_plaintext_bytes={} scan_plaintext_bytes={} scan_loss_micros={}",
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

fn bench_adr062_cross_replica_determinism() -> std::io::Result<()> {
	let case = BenchCase {
		rows: ROW_GROUP_SCAN_TARGET_ROWS as usize,
		columns: 4,
		shape: BenchDataShape::Int64,
	};
	let batch = bench_batch(case.rows, case.columns, case.shape)?;
	let descriptor = descriptor(batch.schema().as_ref(), case.columns)?;
	let rows = source_rows(case.rows);
	for profile in [
		RowGroupSizingProfile::PointRead,
		RowGroupSizingProfile::Scan,
	] {
		let config = row_group_profile_config_for(profile, true, true);
		let left = encode_multirow_from_survivors_with_row_group_config(
			&batch,
			&rows,
			&descriptor,
			RecordBodyCompressionPolicy::Uncompressed,
			&config,
		)?;
		let right = encode_multirow_from_survivors_with_row_group_config(
			&batch,
			&rows,
			&descriptor,
			RecordBodyCompressionPolicy::Uncompressed,
			&config,
		)?;
		let equal = left.frame == right.frame;
		let digest = blake3::hash(&left.frame).to_hex().to_string();
		println!(
			"row_group_fetch_measure scenario=adr062_cross_replica_determinism profile={} rows={} columns={} shape={} byte_equal={} digest={} bytes={}",
			profile.as_str(),
			case.rows,
			case.columns,
			case.shape.as_str(),
			equal,
			digest,
			left.frame.len(),
		);
	}
	Ok(())
}

fn bench_adr061_macro_block_runtime(config: &BenchConfig) -> std::io::Result<()> {
	const PLAINTEXT_BYTES: usize = 100 * 1024 * 1024;
	const REQUEST_BYTES: usize = 8 * 1024 * 1024;
	const WAREHOUSE_RUNTIME_ROWS: usize = 16_777_216;
	let plaintext = vec![0x5a; PLAINTEXT_BYTES];
	let crypto = seeded_dev_crypto_manager(TenantId::ZERO)?;
	let requested = contiguous_plaintext_ranges(PLAINTEXT_BYTES, REQUEST_BYTES);

	for chunk_size in SLAB_ENCRYPTION_CHUNK_SIZES {
		let chunk_size_bytes = ChunkSizeBytes::new(chunk_size as u32);
		let slab_id = SlabId::new(61_000 + chunk_size as u64);
		let started = Instant::now();
		let encrypted = crypto.encrypt_tiered_slab_object(
			slab_id,
			&TieredSlabObjectMetadata {
				tenant_id: TenantId::ZERO,
				created_hlc: 61,
				chunk_size_bytes,
				offset_index: vec![TieredOffsetIndexEntry {
					key_locator: IdentityPrefix::ZERO,
					byte_offset: 0,
					byte_length: PLAINTEXT_BYTES as u32,
					flags: 0,
				}],
				column_stats_block: None,
				has_row_tombstone_records: false,
			},
			&plaintext,
		)?;
		let encrypt_elapsed = started.elapsed();
		let object_size = encrypted.bytes.len() as u64;
		let tail_start = encrypted.bytes.len().saturating_sub(64 * 1024);
		let plan = crypto.plan_tiered_multi_range_read(
			slab_id,
			object_size,
			&encrypted.bytes[tail_start..],
			None,
			0..PLAINTEXT_BYTES,
			&requested,
		)?;
		let ciphertext_ranges = plan.ciphertext_ranges_in_object();
		let range_lengths = ciphertext_ranges
			.iter()
			.map(|range| range.end.saturating_sub(range.start))
			.collect::<Vec<_>>();
		let ciphertext = ciphertext_ranges
			.iter()
			.map(|range| &encrypted.bytes[range.start as usize..range.end as usize])
			.collect::<Vec<_>>();
		let started = Instant::now();
		let decrypted = plan.decrypt(&ciphertext)?;
		let decrypt_elapsed = started.elapsed();
		verify_decrypted_segments(&plaintext, &decrypted)?;
		let p50_get_bytes = percentile_u64(range_lengths.clone(), 50);
		let p95_get_bytes = percentile_u64(range_lengths, 95);
		let dense_get_range_bytes = ciphertext_ranges
			.iter()
			.map(|range| range.end.saturating_sub(range.start))
			.sum::<u64>();
		println!(
			"row_group_fetch_measure scenario=adr061_macro_block_runtime rows={} records=1 columns=50 shape=warehouse_100mib chunk_size={} chunk_count={} plaintext_bytes={} requested_ranges={} chunk_runs={} dense_get_ranges={} dense_get_range_bytes={} p50_get_bytes={} p95_get_bytes={} encrypt_ms={:.3} decrypt_ms={:.3} aead_calls={} iterations={}",
			WAREHOUSE_RUNTIME_ROWS,
			chunk_size,
			encrypted.chunk_count,
			PLAINTEXT_BYTES,
			requested.len(),
			plan.chunk_runs().len(),
			ciphertext_ranges.len(),
			dense_get_range_bytes,
			p50_get_bytes,
			p95_get_bytes,
			encrypt_elapsed.as_secs_f64() * 1_000.0,
			decrypt_elapsed.as_secs_f64() * 1_000.0,
			encrypted.chunk_count,
			config.iterations
		);
	}
	Ok(())
}

fn bench_adr061_warehouse_model() {
	const ROW_GROUP_ROWS: usize = 65_536;
	const RECORD_BYTES: usize = 4 * 1024 * 1024;
	const SELECTED_PAGE_BYTES: usize = 4 * 1024;
	const BASELINE_CHUNK_BYTES: usize = 64 * 1024;
	for rows in [16_777_216_usize, 100_000_000] {
		let records = rows.div_ceil(ROW_GROUP_ROWS);
		for projection_columns in [1_usize, 4, 16] {
			let baseline_sparse_ciphertext = projection_columns as u64
				* chunk_floor_fetch_bytes(SELECTED_PAGE_BYTES, BASELINE_CHUNK_BYTES);
			for chunk_size in SLAB_ENCRYPTION_CHUNK_SIZES {
				let clustered_sparse_ciphertext = projection_columns as u64
					* chunk_floor_fetch_bytes(SELECTED_PAGE_BYTES, chunk_size);
				let dense_get_bytes = chunk_floor_fetch_bytes(RECORD_BYTES, chunk_size);
				println!(
					"row_group_fetch_measure scenario=adr061_warehouse_model rows={} records={} row_group_rows={} record_bytes={} projection_columns={} mask_shape=clustered_first_page route=sparse chunk_size={} dense_p50_get_bytes={} dense_p95_get_bytes={} clustered_sparse_ciphertext_bytes={} baseline_sparse_ciphertext_bytes={} sparse_reduction_micros={} get_count_regression_micros=1000000",
					rows,
					records,
					ROW_GROUP_ROWS,
					RECORD_BYTES,
					projection_columns,
					chunk_size,
					dense_get_bytes,
					dense_get_bytes,
					clustered_sparse_ciphertext,
					baseline_sparse_ciphertext,
					fraction_micros(baseline_sparse_ciphertext, clustered_sparse_ciphertext)
				);
				println!(
					"row_group_fetch_measure scenario=adr061_warehouse_model rows={} records={} row_group_rows={} record_bytes={} projection_columns={} mask_shape=random_1p route=dense_decline chunk_size={} dense_p50_get_bytes={} dense_p95_get_bytes={} clustered_sparse_ciphertext_bytes=0 baseline_sparse_ciphertext_bytes=0 sparse_reduction_micros=1000000 get_count_regression_micros=1000000",
					rows,
					records,
					ROW_GROUP_ROWS,
					RECORD_BYTES,
					projection_columns,
					chunk_size,
					dense_get_bytes,
					dense_get_bytes,
				);
			}
		}
	}
}

fn contiguous_plaintext_ranges(len: usize, step: usize) -> Vec<TieredPlaintextRange> {
	let mut out = Vec::new();
	let mut offset = 0_usize;
	while offset < len {
		let range_len = step.min(len - offset);
		out.push(TieredPlaintextRange::new(
			RecordBodyByteOffset::new(offset as u64),
			RecordBodyByteLen::new(range_len as u64),
		));
		offset += range_len;
	}
	out
}

fn verify_decrypted_segments(expected: &[u8], segments: &[Vec<u8>]) -> std::io::Result<()> {
	let mut cursor = 0_usize;
	for segment in segments {
		let end = cursor.saturating_add(segment.len());
		if end > expected.len() || segment.as_slice() != &expected[cursor..end] {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"ADR 061 macro-block runtime decrypted bytes differ from plaintext",
			));
		}
		cursor = end;
	}
	if cursor != expected.len() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"ADR 061 macro-block runtime decrypted byte count mismatch",
		));
	}
	Ok(())
}

fn percentile_u64(mut values: Vec<u64>, percentile: u64) -> u64 {
	if values.is_empty() {
		return 0;
	}
	values.sort_unstable();
	let index = (values.len() as u64)
		.saturating_mul(percentile)
		.saturating_add(99)
		.saturating_div(100)
		.saturating_sub(1)
		.min(values.len().saturating_sub(1) as u64) as usize;
	values[index]
}

fn chunk_floor_fetch_bytes(plaintext_bytes: usize, chunk_size: usize) -> u64 {
	let chunk_count = plaintext_bytes.div_ceil(chunk_size);
	chunk_count as u64 * (chunk_size as u64 + SLAB_AEAD_TAG_BYTES as u64)
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
