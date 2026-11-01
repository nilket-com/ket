//! ADR 056 row-group fetch benchmark.
//!
//! `cargo bench --bench row_group_fetch -- --quick`.
//!
//! This harness isolates the pure pieces ADR 056 needs before the full
//! promotion matrix: writer overhead, page-directory planning, Phase B
//! survival-mask evaluation, and masked Phase C materialization.

#![allow(clippy::too_many_arguments)]

use arrow::array::{
	Array, ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray, UInt8Array, UInt16Array,
	UInt32Array,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet};
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
use wendelon::schema::pbc::{ColumnPredicateLiteral, OP_GT, OP_PUSH_COL, OP_PUSH_INT, Program};
use wendelon::schema::{
	BaseValue, BitsPerValue, BufferValueCount, ChunkSizeBytes, DictValueCount,
	EncodingCascadeIndex, HybridTimestamp, Identity, IdentityPrefix, IpcBufferIndex,
	IpcColumnOrdinal, Locality, LocalityPolicy, MultiRowLayoutDescriptor, PrimaryKeyDescriptor,
	RecordBodyByteLen, RecordBodyByteOffset, RecordBodyCompressionPolicy, RecordLayoutPolicy,
	RowCount, RowGroupIndex, RowGroupRowCount, RowIdentityPolicy, RunCount, SchemaEvolutionPolicy,
	SimdPageValueCount, SlabId, TableObjectId, TenantId,
};
use wendelon::storage::encoding::cascade::{encode_buffer, encode_buffer_with_value_offsets};
use wendelon::storage::encoding::compute::{
	EncodingComputeConfig, PredicateAtom, PredicateAtomOp, coerce_literal, evaluate_fsst_predicate,
	evaluate_fsst_row_safe_predicate, evaluator_kind_for_cascade,
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
	BufferEncoding, EncodingCascade, EncodingCascadeKind, EncodingLeafCascade,
	EncodingWriterConfig, MockObjectStore, MockObjectStoreConfig, ObjectMetadata, ObjectStore,
	RecordBodyByteRange,
};
use wendelon::storage::{
	ColumnFetchRequirement, ColumnOrdinalMapping, ColumnOrdinalSet, EncodingPageDescriptor,
	PhaseBOutcome, PlaintextByteRange, ProjectedRecordBatch,
	ROW_GROUP_POINT_READ_MIN_PAGED_RECORD_ROWS, ROW_GROUP_POINT_READ_TARGET_ROWS,
	ROW_GROUP_SCAN_MAX_ROWS, ROW_GROUP_SCAN_TARGET_ROWS, RecordBufferDirectory,
	RowGroupSizingProfile, RowGroupWriterConfig, TieredRecordId,
	apply_phase_b_masks_to_projected_row_groups, build_page_directory_for_ipc_stream,
	decode_projected_row_groups_from_plaintext_ranges, default_stats_supported_types,
	encode_multirow_from_survivors_with_row_group_and_encoding_config,
	encode_multirow_from_survivors_with_row_group_config, encode_record_batches_bytes,
	parse_record_buffer_directory, project_record_batch_for_requirement, row_group_stats_for_batch,
	selected_row_group_record_body_ranges, serialize_page_directory_value,
	zip_predicate_and_projection_columns,
};

fn main() -> std::io::Result<()> {
	let config = BenchConfig::from_args();
	if config.all_null_elision_only {
		bench_adr070_all_null_page_skip_matrix(&config)?;
		return Ok(());
	}
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
	bench_adr064_nullable_sparse_projection_matrix(&config)?;
	bench_adr065_dense_predicate_zip_matrix(&config)?;
	bench_adr066_fsst_row_group_fetch_matrix(&config)?;
	bench_adr067_fsst_row_safe_row_group_fetch_matrix(&config)?;
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
	all_null_elision_only: bool,
	rows_override: Option<Vec<usize>>,
}

impl BenchConfig {
	fn from_args() -> Self {
		let quick = std::env::args().any(|arg| arg == "--quick");
		let all_null_elision_only = std::env::args().any(|arg| arg == "--all-null-elision-only");
		let rows_override = std::env::args()
			.find_map(|arg| arg.strip_prefix("--rows=").map(str::to_owned))
			.map(|rows| {
				rows.split(',')
					.filter_map(|row| row.parse::<usize>().ok())
					.collect::<Vec<_>>()
			})
			.filter(|rows| !rows.is_empty());
		if quick {
			Self {
				cases: vec![BenchCase {
					rows: 8_192,
					columns: 8,
					shape: BenchDataShape::Int64,
				}],
				iterations: 25,
				all_null_elision_only,
				rows_override,
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
				all_null_elision_only,
				rows_override,
			}
		}
	}
}

fn descriptor(
	schema: &Schema,
	columns: usize,
) -> std::io::Result<wendelon::schema::TableSchemaDescriptor> {
	wendelon::schema::TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([0x56; 16]),
		1,
		"bench",
		"row_group_fetch",
		schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("c0")),
		LocalityPolicy::IdentityProjection,
		RecordLayoutPolicy::MultiRow(
			MultiRowLayoutDescriptor::new(4_096, 4_096, 8 * 1024 * 1024).map_err(|err| {
				Error::new(
					ErrorKind::InvalidInput,
					format!("bench multi-row layout rejected: {err}"),
				)
			})?,
		),
		SchemaEvolutionPolicy::AdditiveOnly,
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
				.map(|row| (row + column).is_multiple_of(3))
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
			partition: None,
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
				partition: None,
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
	descriptor: &wendelon::schema::TableSchemaDescriptor,
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
	descriptor: &wendelon::schema::TableSchemaDescriptor,
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
	descriptor: &wendelon::schema::TableSchemaDescriptor,
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
	descriptor: &wendelon::schema::TableSchemaDescriptor,
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
	fn planned_upload_uri(&self, key: &str) -> std::io::Result<String> {
		self.inner.planned_upload_uri(key)
	}

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
) -> std::io::Result<wendelon::schema::TableSchemaDescriptor> {
	wendelon::schema::TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([0x60; 16]),
		1,
		"bench",
		"sparse_projection_fetch",
		schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("c0")),
		LocalityPolicy::IdentityProjection,
		RecordLayoutPolicy::MultiRow(
			MultiRowLayoutDescriptor::new(4_096, 4_096, 8 * 1024 * 1024).map_err(|err| {
				Error::new(
					ErrorKind::InvalidInput,
					format!("sparse bench multi-row layout rejected: {err}"),
				)
			})?,
		),
		SchemaEvolutionPolicy::AdditiveOnly,
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
				if row.is_multiple_of(100) {
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
		config.iterations.clamp(1, 3)
	} else {
		config.iterations.clamp(1, 10)
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

#[derive(Clone, Copy, Debug)]
enum Adr064ProjectionShape {
	OneNullable,
	FourNullable,
	MixedNullableNonNull,
	NullableVariableControl,
}

impl Adr064ProjectionShape {
	fn as_str(self) -> &'static str {
		match self {
			Self::OneNullable => "one_nullable_fixed",
			Self::FourNullable => "four_nullable_fixed",
			Self::MixedNullableNonNull => "mixed_nullable_non_null_fixed",
			Self::NullableVariableControl => "nullable_plus_variable_width_control",
		}
	}

	fn projection_columns(self) -> usize {
		match self {
			Self::OneNullable => 1,
			Self::FourNullable => 4,
			Self::MixedNullableNonNull => 2,
			Self::NullableVariableControl => 2,
		}
	}

	fn column_nullable(self, projection_index: usize) -> bool {
		match self {
			Self::MixedNullableNonNull => projection_index == 0,
			Self::OneNullable | Self::FourNullable | Self::NullableVariableControl => true,
		}
	}

	fn column_type(self, projection_index: usize) -> DataType {
		if matches!(self, Self::NullableVariableControl) && projection_index == 1 {
			DataType::Utf8
		} else {
			DataType::UInt32
		}
	}

	fn sparse_supported(self) -> bool {
		!matches!(self, Self::NullableVariableControl)
	}
}

#[derive(Clone, Copy, Debug)]
enum Adr064NullDistribution {
	AllValidSelectedPages,
	NoNullRowGroup,
	Alternating,
	AlternatingPagesAllNull,
	ClusteredNulls,
	AllNullSelectedPage,
	DivergentSelectedPage,
	RandomFivePercent,
	RandomFiftyPercent,
}

impl Adr064NullDistribution {
	fn as_str(self) -> &'static str {
		match self {
			Self::AllValidSelectedPages => "all_valid_selected_pages",
			Self::NoNullRowGroup => "no_null_row_group",
			Self::Alternating => "alternating_nulls",
			Self::AlternatingPagesAllNull => "alternating_pages_all_null",
			Self::ClusteredNulls => "clustered_nulls",
			Self::AllNullSelectedPage => "all_null_selected_page",
			Self::DivergentSelectedPage => "divergent_selected_page",
			Self::RandomFivePercent => "random_5p_nulls",
			Self::RandomFiftyPercent => "random_50p_nulls",
		}
	}
}

#[derive(Clone, Copy, Debug)]
enum Adr064PredicateSource {
	ExactMask,
	ExactIsNotNull,
	ExactIsNull,
	NullableValueComparison,
	Adr063ExactMatch,
	Adr063Maybe,
}

impl Adr064PredicateSource {
	fn as_str(self) -> &'static str {
		match self {
			Self::ExactMask => "exact_mask",
			Self::ExactIsNotNull => "exact_is_not_null",
			Self::ExactIsNull => "exact_is_null",
			Self::NullableValueComparison => "nullable_value_comparison",
			Self::Adr063ExactMatch => "adr063_exact_match",
			Self::Adr063Maybe => "adr063_maybe",
		}
	}
}

struct Adr064RuntimeColumn {
	physical_ordinal: usize,
	nullable: bool,
	data_type: DataType,
	null_buffer_range: Option<RecordBodyByteRange>,
	value_buffer_range: Option<RecordBodyByteRange>,
	encoding: Option<wendelon::storage::BufferEncoding>,
	cascade: Option<EncodingCascade>,
	pages: Vec<EncodingPageDescriptor>,
}

struct Adr064RuntimeFixture {
	row_group_rows: usize,
	record_plaintext_range: Range<usize>,
	frame: Vec<u8>,
	directory: RecordBufferDirectory,
	batch: RecordBatch,
	requirement: ColumnFetchRequirement,
	columns: Vec<Adr064RuntimeColumn>,
}

struct Adr064RuntimeDataset {
	rows: usize,
	page_values: u32,
	profile: RowGroupSizingProfile,
	shape: Adr064ProjectionShape,
	null_distribution: Adr064NullDistribution,
	plaintext: Vec<u8>,
	fixtures: Vec<Adr064RuntimeFixture>,
}

#[derive(Clone, Debug, Default)]
struct Adr064RuntimeOutcome {
	elapsed: Duration,
	output_rows: usize,
	get_range_calls: usize,
	get_range_bytes: u64,
	get_bytes_calls: usize,
	value_plaintext_bytes: u64,
	validity_plaintext_bytes: u64,
	projection_plaintext_bytes: u64,
	projection_ciphertext_bytes: u64,
	selected_ranges: usize,
	selected_value_pages: usize,
	selected_validity_ranges: usize,
	null_count: usize,
	digest: String,
}

impl Adr064RuntimeOutcome {
	fn accumulate(&mut self, other: Self) {
		self.elapsed = self.elapsed.saturating_add(other.elapsed);
		self.output_rows = self.output_rows.saturating_add(other.output_rows);
		self.get_range_calls = self.get_range_calls.saturating_add(other.get_range_calls);
		self.get_range_bytes = self.get_range_bytes.saturating_add(other.get_range_bytes);
		self.get_bytes_calls = self.get_bytes_calls.saturating_add(other.get_bytes_calls);
		self.value_plaintext_bytes = self
			.value_plaintext_bytes
			.saturating_add(other.value_plaintext_bytes);
		self.validity_plaintext_bytes = self
			.validity_plaintext_bytes
			.saturating_add(other.validity_plaintext_bytes);
		self.projection_plaintext_bytes = self
			.projection_plaintext_bytes
			.saturating_add(other.projection_plaintext_bytes);
		self.projection_ciphertext_bytes = self
			.projection_ciphertext_bytes
			.saturating_add(other.projection_ciphertext_bytes);
		self.selected_ranges = self.selected_ranges.saturating_add(other.selected_ranges);
		self.selected_value_pages = self
			.selected_value_pages
			.saturating_add(other.selected_value_pages);
		self.selected_validity_ranges = self
			.selected_validity_ranges
			.saturating_add(other.selected_validity_ranges);
		self.null_count = self.null_count.saturating_add(other.null_count);
	}
}

struct Adr064RuntimeMeasurement {
	dense: Adr064RuntimeOutcome,
	sparse: Adr064RuntimeOutcome,
	route: &'static str,
	equality: bool,
	null_count_equal: bool,
}

fn adr064_nullable_sparse_projection_rows(config: &BenchConfig) -> &'static [usize] {
	if config.iterations <= 25 {
		&[8_192]
	} else {
		&[8_192, 65_536, 1_000_000]
	}
}

fn adr064_runtime_iterations(config: &BenchConfig, rows: usize) -> usize {
	if rows >= 1_000_000 {
		config.iterations.clamp(1, 3)
	} else {
		config.iterations.clamp(1, 10)
	}
}

fn adr064_record_rows_for(profile: RowGroupSizingProfile, remaining: usize) -> usize {
	match profile {
		RowGroupSizingProfile::Scan => remaining.min(ROW_GROUP_SCAN_TARGET_ROWS as usize),
		RowGroupSizingProfile::PointRead => remaining.min(SPARSE_PROJECTION_RUNTIME_RECORD_ROWS),
	}
}

fn adr064_row_group_config(profile: RowGroupSizingProfile) -> RowGroupWriterConfig {
	match profile {
		RowGroupSizingProfile::Scan => {
			row_group_profile_config_for(RowGroupSizingProfile::Scan, true, true)
		}
		RowGroupSizingProfile::PointRead => {
			let mut config = RowGroupWriterConfig::point_read_with_overrides(
				RowGroupRowCount::new(1_024),
				RowGroupRowCount::new(1_024),
				RowCount::new(1_024),
				RecordBodyByteLen::new(1),
			)
			.with_enabled(true);
			config.stats_supported_types = default_stats_supported_types();
			config
		}
	}
}

fn adr064_cell_is_null(
	distribution: Adr064NullDistribution,
	row: usize,
	rows: usize,
	page_values: u32,
	projection_index: usize,
) -> bool {
	match distribution {
		Adr064NullDistribution::AllValidSelectedPages => {
			row >= rows / 2 && (row + projection_index).is_multiple_of(20)
		}
		Adr064NullDistribution::NoNullRowGroup => false,
		Adr064NullDistribution::Alternating => (row + projection_index).is_multiple_of(2),
		Adr064NullDistribution::AlternatingPagesAllNull => {
			(row / page_values as usize + projection_index).is_multiple_of(2)
		}
		Adr064NullDistribution::ClusteredNulls => row < rows.saturating_mul(5).div_ceil(100),
		Adr064NullDistribution::AllNullSelectedPage => row < page_values as usize,
		Adr064NullDistribution::DivergentSelectedPage => {
			projection_index == 0 && row < page_values as usize
		}
		Adr064NullDistribution::RandomFivePercent => (row + projection_index).is_multiple_of(20),
		Adr064NullDistribution::RandomFiftyPercent => {
			let hash = row
				.wrapping_mul(1_103_515_245)
				.wrapping_add(projection_index.wrapping_mul(12_345));
			hash.is_multiple_of(2)
		}
	}
}

fn adr064_payload_value(
	row: usize,
	projection_index: usize,
	cascade: SparseProjectionCascadeFixture,
) -> u32 {
	sparse_projection_runtime_payload_value(row, projection_index, cascade)
}

fn adr064_nullable_runtime_batch(
	rows: usize,
	cascade: SparseProjectionCascadeFixture,
	shape: Adr064ProjectionShape,
	distribution: Adr064NullDistribution,
	page_values: u32,
) -> std::io::Result<RecordBatch> {
	let mut fields = vec![
		Field::new("c0", DataType::UInt16, false),
		Field::new("c1", DataType::UInt16, false),
	];
	let mut columns = vec![
		Arc::new(UInt16Array::from_iter_values(
			(0..rows).map(|row| (row % 251) as u16),
		)) as ArrayRef,
		Arc::new(UInt16Array::from_iter_values(
			(0..rows).map(|row| (row % 251) as u16),
		)) as ArrayRef,
	];
	for projection_index in 0..shape.projection_columns() {
		let data_type = shape.column_type(projection_index);
		let nullable = shape.column_nullable(projection_index);
		fields.push(Field::new(
			format!("c{}", projection_index + 2),
			data_type.clone(),
			nullable,
		));
		match data_type {
			DataType::UInt32 if nullable => {
				let values = (0..rows)
					.map(|row| {
						(!adr064_cell_is_null(
							distribution,
							row,
							rows,
							page_values,
							projection_index,
						))
						.then_some(adr064_payload_value(row, projection_index, cascade))
					})
					.collect::<Vec<_>>();
				columns.push(Arc::new(UInt32Array::from(values)) as ArrayRef);
			}
			DataType::UInt32 => {
				columns.push(Arc::new(UInt32Array::from_iter_values(
					(0..rows).map(|row| adr064_payload_value(row, projection_index, cascade)),
				)) as ArrayRef);
			}
			DataType::Utf8 => {
				let values = (0..rows)
					.map(|row| {
						(!adr064_cell_is_null(
							distribution,
							row,
							rows,
							page_values,
							projection_index,
						))
						.then(|| format!("nullable-v{}-{}", projection_index, row % 1024))
					})
					.collect::<Vec<_>>();
				columns.push(Arc::new(StringArray::from(values)) as ArrayRef);
			}
			_ => unreachable!("ADR 064 bench type is pinned"),
		}
	}
	RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(|err| {
		Error::new(
			ErrorKind::InvalidData,
			format!("ADR 064 nullable runtime batch: {err}"),
		)
	})
}

fn adr064_nullable_runtime_fixture_at(
	rows: usize,
	cascade: SparseProjectionCascadeFixture,
	shape: Adr064ProjectionShape,
	distribution: Adr064NullDistribution,
	page_values: u32,
	profile: RowGroupSizingProfile,
	record_plaintext_start: usize,
) -> std::io::Result<Adr064RuntimeFixture> {
	let batch = adr064_nullable_runtime_batch(rows, cascade, shape, distribution, page_values)?;
	let descriptor = sparse_projection_descriptor(batch.schema().as_ref())?;
	let row_group_config = adr064_row_group_config(profile);
	let encoding_config = EncodingWriterConfig {
		enabled: true,
		target_simd_page_values: SimdPageValueCount::new(page_values),
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
			"ADR 064 record plaintext start exceeds u64",
		)
	})?;
	let directory = parse_record_buffer_directory(
		TieredRecordId {
			slab_id: SlabId::new(64),
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
			format!("ADR 064 directory parse: {err}"),
		)
	})?;
	let page_directory = directory
		.page_directory
		.as_ref()
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "ADR 064 missing page directory"))?;
	let row_group_rows = page_directory
		.row_groups
		.iter()
		.find(|group| group.row_group_index == RowGroupIndex::ZERO)
		.map(|group| group.row_count.get() as usize)
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "ADR 064 row group zero missing"))?;
	let encoding_directory = directory
		.encoding_directory
		.as_ref()
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "ADR 064 missing encoding directory"))?;
	let output_columns = (2..2 + shape.projection_columns())
		.map(|column| {
			u32::try_from(column).map_err(|_| {
				Error::new(
					ErrorKind::InvalidInput,
					"ADR 064 projection column exceeds u32",
				)
			})
		})
		.collect::<std::io::Result<Vec<_>>>()?;
	let requirement = ColumnFetchRequirement {
		output_columns: ColumnOrdinalSet::from_columns(output_columns.iter().copied()),
		..ColumnFetchRequirement::default()
	};
	let mut columns = Vec::with_capacity(shape.projection_columns());
	for projection_index in 0..shape.projection_columns() {
		let physical_ordinal = projection_index + 2;
		let data_type = shape.column_type(projection_index);
		let nullable = shape.column_nullable(projection_index);
		let mapping = directory
			.column_to_buffers
			.get(physical_ordinal)
			.ok_or_else(|| {
				Error::new(ErrorKind::InvalidData, "ADR 064 projected column mapping")
			})?;
		let null_buffer_ordinal = mapping.first_buffer_ordinal;
		let row_group_may_have_nulls = nullable
			&& page_directory
				.row_group_stats
				.iter()
				.find(|stat| {
					stat.row_group_index == RowGroupIndex::ZERO
						&& stat.ipc_column_ordinal == IpcColumnOrdinal::new(physical_ordinal as u32)
				})
				.map(|stat| stat.null_count.get() != 0)
				.unwrap_or_else(|| {
					directory
						.column_null_counts
						.get(physical_ordinal)
						.copied()
						.unwrap_or(usize::MAX)
						!= 0
				});
		let null_buffer_range = row_group_may_have_nulls
			.then(|| {
				page_directory
					.buffer_pages
					.iter()
					.find(|page| {
						page.row_group_index == RowGroupIndex::ZERO
							&& page.ipc_column_ordinal
								== IpcColumnOrdinal::new(physical_ordinal as u32)
							&& page.ipc_buffer_ordinal == null_buffer_ordinal
					})
					.map(|page| page.range)
					.filter(|range| range.len.get() > 0)
			})
			.flatten();
		let value_buffer_ordinal = mapping
			.first_buffer_ordinal
			.get()
			.checked_add(1)
			.map(IpcBufferIndex::new);
		let (value_buffer_range, encoding, cascade, pages) = if data_type == DataType::UInt32 {
			let value_buffer_ordinal = value_buffer_ordinal.ok_or_else(|| {
				Error::new(ErrorKind::InvalidData, "ADR 064 value buffer ordinal")
			})?;
			let value_page = page_directory
				.buffer_pages
				.iter()
				.find(|page| {
					page.row_group_index == RowGroupIndex::ZERO
						&& page.ipc_column_ordinal == IpcColumnOrdinal::new(physical_ordinal as u32)
						&& page.ipc_buffer_ordinal == value_buffer_ordinal
						&& page.range.len.get() > 0
				})
				.ok_or_else(|| Error::new(ErrorKind::InvalidData, "ADR 064 value page"))?;
			let encoding = wendelon::storage::encoding::materialize::find_buffer_encoding(
				encoding_directory,
				RowGroupIndex::ZERO,
				value_buffer_ordinal,
				value_page.range,
			)
			.cloned();
			if let Some(encoding) = encoding {
				let cascade = encoding_directory
					.cascades
					.get(encoding.cascade_index.get() as usize)
					.cloned()
					.ok_or_else(|| Error::new(ErrorKind::InvalidData, "ADR 064 cascade"))?;
				let pages = expand_page_descriptors(&encoding).map_err(|code| {
					Error::new(
						ErrorKind::InvalidData,
						format!("ADR 064 page expansion rejected: {code:?}"),
					)
				})?;
				(Some(value_page.range), Some(encoding), Some(cascade), pages)
			} else {
				(Some(value_page.range), None, None, Vec::new())
			}
		} else {
			(None, None, None, Vec::new())
		};
		columns.push(Adr064RuntimeColumn {
			physical_ordinal,
			nullable,
			data_type,
			null_buffer_range,
			value_buffer_range,
			encoding,
			cascade,
			pages,
		});
	}
	Ok(Adr064RuntimeFixture {
		row_group_rows,
		record_plaintext_range: record_plaintext_start
			..record_plaintext_start.saturating_add(encoded.frame.len()),
		frame: encoded.frame,
		directory,
		batch,
		requirement,
		columns,
	})
}

fn adr064_nullable_runtime_dataset(
	rows: usize,
	cascade: SparseProjectionCascadeFixture,
	shape: Adr064ProjectionShape,
	distribution: Adr064NullDistribution,
	page_values: u32,
	profile: RowGroupSizingProfile,
) -> std::io::Result<Adr064RuntimeDataset> {
	let mut remaining = rows;
	let mut plaintext = Vec::new();
	let mut fixtures = Vec::new();
	while remaining > 0 {
		let record_rows = adr064_record_rows_for(profile, remaining);
		let fixture = adr064_nullable_runtime_fixture_at(
			record_rows,
			cascade,
			shape,
			distribution,
			page_values,
			profile,
			plaintext.len(),
		)?;
		plaintext.extend_from_slice(&fixture.frame);
		fixtures.push(fixture);
		remaining -= record_rows;
	}
	Ok(Adr064RuntimeDataset {
		rows,
		page_values,
		profile,
		shape,
		null_distribution: distribution,
		plaintext,
		fixtures,
	})
}

fn adr064_validity_body_range_for_page(
	validity_buffer_range: RecordBodyByteRange,
	page: EncodingPageDescriptor,
) -> std::io::Result<(RecordBodyByteRange, u8)> {
	let bit_start = u64::from(page.first_value_index.get());
	let bit_len = u64::from(page.value_count.get());
	let byte_start = bit_start / 8;
	let bit_offset = (bit_start % 8) as u8;
	let byte_len = (u64::from(bit_offset)
		.checked_add(bit_len)
		.and_then(|bits| bits.checked_add(7))
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "ADR 064 validity span overflow"))?)
		/ 8;
	let byte_end = byte_start
		.checked_add(byte_len)
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "ADR 064 validity span overflow"))?;
	if byte_end > validity_buffer_range.len.get() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"ADR 064 validity span exceeds buffer",
		));
	}
	Ok((
		RecordBodyByteRange {
			start: RecordBodyByteOffset::new(
				validity_buffer_range.start.get().saturating_add(byte_start),
			),
			len: RecordBodyByteLen::new(byte_len),
		},
		bit_offset,
	))
}

fn adr064_rebased_validity_bit(
	fetched: &[u8],
	bit_offset: u8,
	row: usize,
) -> std::io::Result<bool> {
	let source_bit = usize::from(bit_offset).checked_add(row).ok_or_else(|| {
		Error::new(
			ErrorKind::InvalidData,
			"ADR 064 validity bit offset overflow",
		)
	})?;
	let byte = fetched
		.get(source_bit / 8)
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "ADR 064 validity fetch too short"))?;
	Ok((byte & (1_u8 << (source_bit % 8))) != 0)
}

fn adr064_selected_pages(
	column: &Adr064RuntimeColumn,
	shape: SparseProjectionMaskShape,
	rows: usize,
) -> Vec<&EncodingPageDescriptor> {
	column
		.pages
		.iter()
		.filter(|page| sparse_projection_page_selected(shape, page, rows))
		.collect()
}

fn adr064_dense_expected_for_fixture(
	fixture: &Adr064RuntimeFixture,
	mask: &BooleanArray,
) -> std::io::Result<(Vec<u8>, usize, usize)> {
	let mut out = Vec::new();
	let mut output_rows = 0_usize;
	let mut output_rows_counted = false;
	let mut null_count = 0_usize;
	for column in &fixture.columns {
		match column.data_type {
			DataType::UInt32 => {
				let payload = fixture
					.batch
					.column(column.physical_ordinal)
					.as_any()
					.downcast_ref::<UInt32Array>()
					.ok_or_else(|| Error::new(ErrorKind::InvalidData, "ADR 064 UInt32 payload"))?;
				for row in 0..fixture.row_group_rows {
					if row < mask.len() && mask.is_valid(row) && mask.value(row) {
						if !output_rows_counted {
							output_rows = output_rows.saturating_add(1);
						}
						let valid = payload.is_valid(row);
						out.push(u8::from(valid));
						if valid {
							out.extend_from_slice(&payload.value(row).to_le_bytes());
						} else {
							null_count = null_count.saturating_add(1);
							out.extend_from_slice(&0_u32.to_le_bytes());
						}
					}
				}
			}
			DataType::Utf8 => {
				let payload = fixture
					.batch
					.column(column.physical_ordinal)
					.as_any()
					.downcast_ref::<StringArray>()
					.ok_or_else(|| Error::new(ErrorKind::InvalidData, "ADR 064 Utf8 payload"))?;
				for row in 0..fixture.row_group_rows {
					if row < mask.len() && mask.is_valid(row) && mask.value(row) {
						if !output_rows_counted {
							output_rows = output_rows.saturating_add(1);
						}
						let valid = payload.is_valid(row);
						out.push(u8::from(valid));
						if valid {
							let value = payload.value(row).as_bytes();
							out.extend_from_slice(&(value.len() as u32).to_le_bytes());
							out.extend_from_slice(value);
						} else {
							null_count = null_count.saturating_add(1);
							out.extend_from_slice(&0_u32.to_le_bytes());
						}
					}
				}
			}
			_ => unreachable!("ADR 064 bench type is pinned"),
		}
		output_rows_counted = true;
	}
	Ok((out, output_rows, null_count))
}

fn adr064_dense_expected_for_dataset(
	dataset: &Adr064RuntimeDataset,
	shape: SparseProjectionMaskShape,
) -> std::io::Result<(Vec<u8>, usize, usize)> {
	let mut bytes = Vec::new();
	let mut rows = 0_usize;
	let mut null_count = 0_usize;
	for fixture in &dataset.fixtures {
		let mask = sparse_projection_mask(shape, fixture.row_group_rows);
		let (fixture_bytes, fixture_rows, fixture_nulls) =
			adr064_dense_expected_for_fixture(fixture, &mask)?;
		bytes.extend(fixture_bytes);
		rows = rows.saturating_add(fixture_rows);
		null_count = null_count.saturating_add(fixture_nulls);
	}
	Ok((bytes, rows, null_count))
}

fn adr064_dense_projection_runtime(
	fixture: &Adr064RuntimeFixture,
	mask: &BooleanArray,
	runtime: &tokio::runtime::Runtime,
	store: Arc<CountingBenchObjectStore>,
	uri: &str,
	crypto: &wendelon::crypto::CryptoManager,
	slab_id: SlabId,
	object_size: u64,
) -> std::io::Result<Adr064RuntimeOutcome> {
	let page_directory = fixture.directory.page_directory.as_ref().ok_or_else(|| {
		Error::new(
			ErrorKind::InvalidData,
			"ADR 064 dense missing page directory",
		)
	})?;
	let selected_groups = [RowGroupIndex::ZERO];
	let dense_ranges = selected_row_group_record_body_ranges(
		&fixture.directory,
		page_directory,
		&fixture.requirement,
		&selected_groups,
	)
	.map_err(|err| {
		Error::new(
			ErrorKind::InvalidData,
			format!("ADR 064 dense ranges: {err}"),
		)
	})?;
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
	.map_err(|err| {
		Error::new(
			ErrorKind::InvalidData,
			format!("ADR 064 dense decode: {err}"),
		)
	})?;
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
	let (expected, _, null_count) = adr064_dense_expected_for_fixture(fixture, mask)?;
	Ok(Adr064RuntimeOutcome {
		elapsed,
		output_rows: filtered.batch.num_rows(),
		get_range_calls: delta.get_range_calls,
		get_range_bytes: delta.get_range_bytes,
		get_bytes_calls: delta.get_bytes_calls,
		value_plaintext_bytes: projection_plaintext_bytes,
		validity_plaintext_bytes: 0,
		projection_plaintext_bytes,
		projection_ciphertext_bytes,
		selected_ranges: dense_ranges.len(),
		selected_value_pages: fixture
			.columns
			.iter()
			.map(|column| column.pages.len())
			.sum(),
		selected_validity_ranges: 0,
		null_count,
		digest: blake3::hash(&expected).to_hex().to_string(),
	})
}

fn adr064_dense_dataset_runtime(
	dataset: &Adr064RuntimeDataset,
	shape: SparseProjectionMaskShape,
	runtime: &tokio::runtime::Runtime,
	store: Arc<CountingBenchObjectStore>,
	uri: &str,
	crypto: &wendelon::crypto::CryptoManager,
	slab_id: SlabId,
	object_size: u64,
) -> std::io::Result<Adr064RuntimeOutcome> {
	let mut total = Adr064RuntimeOutcome::default();
	let mut digest_bytes = Vec::new();
	for fixture in &dataset.fixtures {
		let mask = sparse_projection_mask(shape, fixture.row_group_rows);
		let outcome = adr064_dense_projection_runtime(
			fixture,
			&mask,
			runtime,
			store.clone(),
			uri,
			crypto,
			slab_id,
			object_size,
		)?;
		let (expected, _, _) = adr064_dense_expected_for_fixture(fixture, &mask)?;
		digest_bytes.extend(expected);
		total.accumulate(outcome);
	}
	total.digest = blake3::hash(&digest_bytes).to_hex().to_string();
	Ok(total)
}

fn adr064_sparse_projection_runtime(
	fixture: &Adr064RuntimeFixture,
	mask: &BooleanArray,
	shape: SparseProjectionMaskShape,
	runtime: &tokio::runtime::Runtime,
	store: Arc<CountingBenchObjectStore>,
	uri: &str,
	crypto: &wendelon::crypto::CryptoManager,
	slab_id: SlabId,
	object_size: u64,
) -> std::io::Result<(Adr064RuntimeOutcome, Vec<u8>)> {
	if fixture.columns.iter().any(|column| {
		column.data_type != DataType::UInt32
			|| column.encoding.is_none()
			|| column.cascade.is_none()
	}) {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"ADR 064 sparse requested for unsupported projected column",
		));
	}
	let mut value_ranges = Vec::new();
	let mut validity_ranges = Vec::new();
	let mut selected_value_pages = 0_usize;
	for column in &fixture.columns {
		let selected_pages = adr064_selected_pages(column, shape, fixture.row_group_rows);
		selected_value_pages = selected_value_pages.saturating_add(selected_pages.len());
		let encoding = column
			.encoding
			.as_ref()
			.expect("ADR 064 prechecked encoding");
		let value_buffer_range = column
			.value_buffer_range
			.expect("ADR 064 prechecked value range");
		for page in selected_pages {
			value_ranges.push(encoded_page_body_range(value_buffer_range, encoding, *page));
			if column.nullable
				&& let Some(validity_range) = column.null_buffer_range
			{
				validity_ranges.push(adr064_validity_body_range_for_page(validity_range, *page)?);
			}
		}
	}
	let value_plaintext_bytes = value_ranges
		.iter()
		.map(|range| range.len.get())
		.sum::<u64>();
	let validity_plaintext_bytes = validity_ranges
		.iter()
		.map(|(range, _)| range.len.get())
		.sum::<u64>();
	let mut sparse_ranges = value_ranges.clone();
	sparse_ranges.extend(validity_ranges.iter().map(|(range, _)| *range));
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
	let mut output_rows = 0_usize;
	let mut output_rows_counted = false;
	let mut null_count = 0_usize;
	for column in &fixture.columns {
		let selected_pages = adr064_selected_pages(column, shape, fixture.row_group_rows);
		let encoding = column
			.encoding
			.as_ref()
			.expect("ADR 064 prechecked encoding");
		let cascade = column.cascade.as_ref().expect("ADR 064 prechecked cascade");
		let value_buffer_range = column
			.value_buffer_range
			.expect("ADR 064 prechecked value range");
		for page in selected_pages {
			let body_range = encoded_page_body_range(value_buffer_range, encoding, *page);
			let encoded =
				extract_from_plaintext_ranges(body_range, &plaintext_ranges, &plaintext_extracts)?;
			let decoded = wendelon::storage::encoding::cascade::decode_page(
				&encoded,
				&DataType::UInt32,
				cascade,
				RecordBodyByteLen::new(u64::from(page.value_count.get()).saturating_mul(4)),
				BufferValueCount::new(page.value_count.get()),
			)
			.map_err(|code| {
				Error::new(
					ErrorKind::InvalidData,
					format!("ADR 064 page decode rejected: {code:?}"),
				)
			})?;
			let validity = if column.nullable {
				column
					.null_buffer_range
					.map(|range| adr064_validity_body_range_for_page(range, *page))
					.transpose()?
					.map(|(range, bit_offset)| {
						let bytes = extract_from_plaintext_ranges(
							range,
							&plaintext_ranges,
							&plaintext_extracts,
						)?;
						Ok::<_, std::io::Error>((bytes, bit_offset))
					})
					.transpose()?
			} else {
				None
			};
			let page_start = page.first_value_index.get() as usize;
			for (offset, value) in decoded.chunks_exact(4).enumerate() {
				let row = page_start + offset;
				if row < mask.len() && mask.is_valid(row) && mask.value(row) {
					if !output_rows_counted {
						output_rows = output_rows.saturating_add(1);
					}
					let valid = if let Some((validity_bytes, bit_offset)) = &validity {
						adr064_rebased_validity_bit(validity_bytes, *bit_offset, offset)?
					} else {
						true
					};
					values.push(u8::from(valid));
					if valid {
						values.extend_from_slice(value);
					} else {
						null_count = null_count.saturating_add(1);
						values.extend_from_slice(&0_u32.to_le_bytes());
					}
				}
			}
		}
		output_rows_counted = true;
	}
	let elapsed = started.elapsed();
	let delta = store.snapshot().delta_since(before);
	let digest = blake3::hash(&values).to_hex().to_string();
	Ok((
		Adr064RuntimeOutcome {
			elapsed,
			output_rows,
			get_range_calls: delta.get_range_calls,
			get_range_bytes: delta.get_range_bytes,
			get_bytes_calls: delta.get_bytes_calls,
			value_plaintext_bytes,
			validity_plaintext_bytes,
			projection_plaintext_bytes,
			projection_ciphertext_bytes,
			selected_ranges: sparse_ranges.len(),
			selected_value_pages,
			selected_validity_ranges: validity_ranges.len(),
			null_count,
			digest,
		},
		values,
	))
}

fn adr064_sparse_dataset_runtime(
	dataset: &Adr064RuntimeDataset,
	shape: SparseProjectionMaskShape,
	runtime: &tokio::runtime::Runtime,
	store: Arc<CountingBenchObjectStore>,
	uri: &str,
	crypto: &wendelon::crypto::CryptoManager,
	slab_id: SlabId,
	object_size: u64,
) -> std::io::Result<(Adr064RuntimeOutcome, Vec<u8>)> {
	let mut total = Adr064RuntimeOutcome::default();
	let mut values = Vec::new();
	for fixture in &dataset.fixtures {
		let mask = sparse_projection_mask(shape, fixture.row_group_rows);
		let (outcome, fixture_values) = adr064_sparse_projection_runtime(
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
		values.extend(fixture_values);
	}
	total.digest = blake3::hash(&values).to_hex().to_string();
	Ok((total, values))
}

#[derive(Clone, Debug, Default)]
struct Adr070ElisionStats {
	elided_pages: usize,
	synthesized_value_bytes: u64,
	synthesized_validity_bytes: u64,
	drift_sentinel_pages: usize,
}

impl Adr070ElisionStats {
	fn accumulate(&mut self, other: Self) {
		self.elided_pages = self.elided_pages.saturating_add(other.elided_pages);
		self.synthesized_value_bytes = self
			.synthesized_value_bytes
			.saturating_add(other.synthesized_value_bytes);
		self.synthesized_validity_bytes = self
			.synthesized_validity_bytes
			.saturating_add(other.synthesized_validity_bytes);
		self.drift_sentinel_pages = self
			.drift_sentinel_pages
			.saturating_add(other.drift_sentinel_pages);
	}
}

fn adr070_page_is_metadata_all_null(
	column: &Adr064RuntimeColumn,
	encoding: &wendelon::storage::BufferEncoding,
	page: &EncodingPageDescriptor,
) -> bool {
	column.nullable
		&& matches!(
			column.data_type,
			DataType::Int8
				| DataType::Int16
				| DataType::Int32
				| DataType::Int64
				| DataType::UInt8
				| DataType::UInt16
				| DataType::UInt32
				| DataType::UInt64
				| DataType::Date32
				| DataType::Date64
				| DataType::Timestamp(
					TimeUnit::Millisecond | TimeUnit::Microsecond | TimeUnit::Nanosecond,
					None,
				)
		) && encoding
		.page_stats
		.get(page.page_index.get() as usize)
		.is_some_and(|stats| stats.null_count.get() == page.value_count.get())
}

fn adr070_drift_sentinel_for_fixture(fixture: &Adr064RuntimeFixture) -> std::io::Result<usize> {
	let mut audited = 0_usize;
	for column in &fixture.columns {
		let Some(encoding) = column.encoding.as_ref() else {
			continue;
		};
		let Some(validity_range) = column.null_buffer_range else {
			continue;
		};
		for page in &column.pages {
			if !adr070_page_is_metadata_all_null(column, encoding, page) {
				continue;
			}
			let (body_range, bit_offset) =
				adr064_validity_body_range_for_page(validity_range, *page)?;
			let relative_start = body_range.start.get() as usize;
			let relative_end = relative_start
				.checked_add(body_range.len.get() as usize)
				.ok_or_else(|| {
					Error::new(
						ErrorKind::InvalidData,
						"ADR 070 drift sentinel range overflow",
					)
				})?;
			let bytes = fixture
				.frame
				.get(relative_start..relative_end)
				.ok_or_else(|| {
					Error::new(
						ErrorKind::InvalidData,
						"ADR 070 drift sentinel range exceeds fixture frame",
					)
				})?;
			for row in 0..page.value_count.get() as usize {
				if adr064_rebased_validity_bit(bytes, bit_offset, row)? {
					return Err(Error::new(
						ErrorKind::InvalidData,
						"ADR 070 drift sentinel found valid bit on metadata-all-null page",
					));
				}
			}
			audited = audited.saturating_add(1);
		}
	}
	Ok(audited)
}

fn adr070_sparse_projection_runtime(
	fixture: &Adr064RuntimeFixture,
	mask: &BooleanArray,
	shape: SparseProjectionMaskShape,
	runtime: &tokio::runtime::Runtime,
	store: Arc<CountingBenchObjectStore>,
	uri: &str,
	crypto: &wendelon::crypto::CryptoManager,
	slab_id: SlabId,
	object_size: u64,
) -> std::io::Result<(Adr064RuntimeOutcome, Adr070ElisionStats, Vec<u8>)> {
	if fixture.columns.iter().any(|column| {
		column.data_type != DataType::UInt32
			|| column.encoding.is_none()
			|| column.cascade.is_none()
	}) {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"ADR 070 sparse requested for unsupported projected column",
		));
	}
	let mut value_ranges = Vec::new();
	let mut validity_ranges = Vec::new();
	let mut selected_value_pages = 0_usize;
	let mut stats = Adr070ElisionStats {
		drift_sentinel_pages: adr070_drift_sentinel_for_fixture(fixture)?,
		..Adr070ElisionStats::default()
	};
	for column in &fixture.columns {
		let selected_pages = adr064_selected_pages(column, shape, fixture.row_group_rows);
		let encoding = column
			.encoding
			.as_ref()
			.expect("ADR 070 prechecked encoding");
		let value_buffer_range = column
			.value_buffer_range
			.expect("ADR 070 prechecked value range");
		for page in selected_pages {
			if adr070_page_is_metadata_all_null(column, encoding, page) {
				stats.elided_pages = stats.elided_pages.saturating_add(1);
				stats.synthesized_value_bytes = stats
					.synthesized_value_bytes
					.saturating_add(u64::from(page.value_count.get()).saturating_mul(4));
				stats.synthesized_validity_bytes = stats
					.synthesized_validity_bytes
					.saturating_add(u64::from(page.value_count.get()).div_ceil(8));
				continue;
			}
			selected_value_pages = selected_value_pages.saturating_add(1);
			value_ranges.push(encoded_page_body_range(value_buffer_range, encoding, *page));
			if column.nullable
				&& let Some(validity_range) = column.null_buffer_range
			{
				validity_ranges.push(adr064_validity_body_range_for_page(validity_range, *page)?);
			}
		}
	}
	let value_plaintext_bytes = value_ranges
		.iter()
		.map(|range| range.len.get())
		.sum::<u64>();
	let validity_plaintext_bytes = validity_ranges
		.iter()
		.map(|(range, _)| range.len.get())
		.sum::<u64>();
	let mut sparse_ranges = value_ranges.clone();
	sparse_ranges.extend(validity_ranges.iter().map(|(range, _)| *range));
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
	let mut output_rows = 0_usize;
	let mut output_rows_counted = false;
	let mut null_count = 0_usize;
	for column in &fixture.columns {
		let selected_pages = adr064_selected_pages(column, shape, fixture.row_group_rows);
		let encoding = column
			.encoding
			.as_ref()
			.expect("ADR 070 prechecked encoding");
		let cascade = column.cascade.as_ref().expect("ADR 070 prechecked cascade");
		let value_buffer_range = column
			.value_buffer_range
			.expect("ADR 070 prechecked value range");
		for page in selected_pages {
			let page_start = page.first_value_index.get() as usize;
			if adr070_page_is_metadata_all_null(column, encoding, page) {
				for offset in 0..page.value_count.get() as usize {
					let row = page_start + offset;
					if row < mask.len() && mask.is_valid(row) && mask.value(row) {
						if !output_rows_counted {
							output_rows = output_rows.saturating_add(1);
						}
						values.push(0);
						values.extend_from_slice(&0_u32.to_le_bytes());
						null_count = null_count.saturating_add(1);
					}
				}
				continue;
			}
			let body_range = encoded_page_body_range(value_buffer_range, encoding, *page);
			let encoded =
				extract_from_plaintext_ranges(body_range, &plaintext_ranges, &plaintext_extracts)?;
			let decoded = wendelon::storage::encoding::cascade::decode_page(
				&encoded,
				&DataType::UInt32,
				cascade,
				RecordBodyByteLen::new(u64::from(page.value_count.get()).saturating_mul(4)),
				BufferValueCount::new(page.value_count.get()),
			)
			.map_err(|code| {
				Error::new(
					ErrorKind::InvalidData,
					format!("ADR 070 page decode rejected: {code:?}"),
				)
			})?;
			let validity = if column.nullable {
				column
					.null_buffer_range
					.map(|range| adr064_validity_body_range_for_page(range, *page))
					.transpose()?
					.map(|(range, bit_offset)| {
						let bytes = extract_from_plaintext_ranges(
							range,
							&plaintext_ranges,
							&plaintext_extracts,
						)?;
						Ok::<_, std::io::Error>((bytes, bit_offset))
					})
					.transpose()?
			} else {
				None
			};
			for (offset, value) in decoded.chunks_exact(4).enumerate() {
				let row = page_start + offset;
				if row < mask.len() && mask.is_valid(row) && mask.value(row) {
					if !output_rows_counted {
						output_rows = output_rows.saturating_add(1);
					}
					let valid = if let Some((validity_bytes, bit_offset)) = &validity {
						adr064_rebased_validity_bit(validity_bytes, *bit_offset, offset)?
					} else {
						true
					};
					values.push(u8::from(valid));
					if valid {
						values.extend_from_slice(value);
					} else {
						null_count = null_count.saturating_add(1);
						values.extend_from_slice(&0_u32.to_le_bytes());
					}
				}
			}
		}
		output_rows_counted = true;
	}
	let elapsed = started.elapsed();
	let delta = store.snapshot().delta_since(before);
	let digest = blake3::hash(&values).to_hex().to_string();
	Ok((
		Adr064RuntimeOutcome {
			elapsed,
			output_rows,
			get_range_calls: delta.get_range_calls,
			get_range_bytes: delta.get_range_bytes,
			get_bytes_calls: delta.get_bytes_calls,
			value_plaintext_bytes,
			validity_plaintext_bytes,
			projection_plaintext_bytes,
			projection_ciphertext_bytes,
			selected_ranges: sparse_ranges.len(),
			selected_value_pages,
			selected_validity_ranges: validity_ranges.len(),
			null_count,
			digest,
		},
		stats,
		values,
	))
}

fn adr070_sparse_dataset_runtime(
	dataset: &Adr064RuntimeDataset,
	shape: SparseProjectionMaskShape,
	runtime: &tokio::runtime::Runtime,
	store: Arc<CountingBenchObjectStore>,
	uri: &str,
	crypto: &wendelon::crypto::CryptoManager,
	slab_id: SlabId,
	object_size: u64,
) -> std::io::Result<(Adr064RuntimeOutcome, Adr070ElisionStats, Vec<u8>)> {
	let mut total = Adr064RuntimeOutcome::default();
	let mut stats = Adr070ElisionStats::default();
	let mut values = Vec::new();
	for fixture in &dataset.fixtures {
		let mask = sparse_projection_mask(shape, fixture.row_group_rows);
		let (outcome, fixture_stats, fixture_values) = adr070_sparse_projection_runtime(
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
		stats.accumulate(fixture_stats);
		values.extend(fixture_values);
	}
	total.digest = blake3::hash(&values).to_hex().to_string();
	Ok((total, stats, values))
}

fn adr064_runtime_route(
	dataset: &Adr064RuntimeDataset,
	cascade_sparse_enabled: bool,
	shape: SparseProjectionMaskShape,
	_predicate_source: Adr064PredicateSource,
	encoded_validity_control: bool,
) -> &'static str {
	if matches!(shape, SparseProjectionMaskShape::All) {
		return "dense_decline_all";
	}
	if matches!(shape, SparseProjectionMaskShape::None) {
		return "none_zero_fetch";
	}
	if encoded_validity_control {
		return "dense_decline_encoded_validity";
	}
	if !dataset.shape.sparse_supported() {
		return "dense_decline_variable_width";
	}
	if !cascade_sparse_enabled {
		return "dense_decline_cascade";
	}
	if dataset.fixtures.iter().any(|fixture| {
		fixture.columns.iter().any(|column| {
			column.data_type != DataType::UInt32
				|| column.encoding.is_none()
				|| column.cascade.is_none()
		})
	}) {
		return "dense_decline_cascade";
	}
	"sparse"
}

fn measure_adr064_runtime_case(
	dataset: &Adr064RuntimeDataset,
	shape: SparseProjectionMaskShape,
	predicate_source: Adr064PredicateSource,
	iterations: usize,
	env: &SparseProjectionRuntimeEnv<'_>,
	cascade_sparse_enabled: bool,
	encoded_validity_control: bool,
) -> std::io::Result<Adr064RuntimeMeasurement> {
	let (expected, expected_rows, expected_null_count) =
		adr064_dense_expected_for_dataset(dataset, shape)?;
	let route = adr064_runtime_route(
		dataset,
		cascade_sparse_enabled,
		shape,
		predicate_source,
		encoded_validity_control,
	);
	let mut last_dense = None;
	let dense_elapsed = bench(iterations, || {
		let outcome = adr064_dense_dataset_runtime(
			black_box(dataset),
			black_box(shape),
			env.runtime,
			env.store.clone(),
			env.uri,
			env.crypto,
			env.slab_id,
			env.object_size,
		)
		.expect("ADR 064 dense runtime");
		last_dense = Some(outcome);
	});
	let mut dense = last_dense.expect("ADR 064 dense outcome");
	dense.elapsed = dense_elapsed;
	let (sparse, sparse_values) = match route {
		"sparse" => {
			let mut last_sparse = None;
			let sparse_elapsed = bench(iterations, || {
				let (outcome, values) = adr064_sparse_dataset_runtime(
					black_box(dataset),
					black_box(shape),
					env.runtime,
					env.store.clone(),
					env.uri,
					env.crypto,
					env.slab_id,
					env.object_size,
				)
				.expect("ADR 064 sparse runtime");
				last_sparse = Some((outcome, values));
			});
			let (mut outcome, values) = last_sparse.expect("ADR 064 sparse outcome");
			outcome.elapsed = sparse_elapsed;
			(outcome, values)
		}
		"none_zero_fetch" => (
			Adr064RuntimeOutcome {
				output_rows: 0,
				digest: blake3::hash(&[]).to_hex().to_string(),
				..Adr064RuntimeOutcome::default()
			},
			Vec::new(),
		),
		_ => (
			Adr064RuntimeOutcome {
				elapsed: dense.elapsed,
				output_rows: dense.output_rows,
				get_range_calls: dense.get_range_calls,
				get_range_bytes: dense.get_range_bytes,
				get_bytes_calls: dense.get_bytes_calls,
				value_plaintext_bytes: dense.projection_plaintext_bytes,
				validity_plaintext_bytes: 0,
				projection_plaintext_bytes: dense.projection_plaintext_bytes,
				projection_ciphertext_bytes: dense.projection_ciphertext_bytes,
				selected_ranges: dense.selected_ranges,
				selected_value_pages: dense.selected_value_pages,
				selected_validity_ranges: 0,
				null_count: dense.null_count,
				digest: dense.digest.clone(),
			},
			expected.clone(),
		),
	};
	let equality = sparse_values == expected
		&& dense.output_rows == expected_rows
		&& sparse.output_rows == expected_rows;
	let null_count_equal =
		dense.null_count == expected_null_count && sparse.null_count == expected_null_count;
	if !equality || !null_count_equal {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"ADR 064 runtime mismatch rows={} route={} equality={} null_count_equal={} dense_rows={} sparse_rows={} expected_rows={} dense_nulls={} sparse_nulls={} expected_nulls={}",
				dataset.rows,
				route,
				equality,
				null_count_equal,
				dense.output_rows,
				sparse.output_rows,
				expected_rows,
				dense.null_count,
				sparse.null_count,
				expected_null_count,
			),
		));
	}
	Ok(Adr064RuntimeMeasurement {
		dense,
		sparse,
		route,
		equality,
		null_count_equal,
	})
}

fn print_adr064_runtime_measure(
	dataset: &Adr064RuntimeDataset,
	cascade: SparseProjectionCascadeFixture,
	shape: SparseProjectionMaskShape,
	predicate_source: Adr064PredicateSource,
	iterations: usize,
	measurement: &Adr064RuntimeMeasurement,
	extra: &str,
) {
	println!(
		"row_group_fetch_measure scenario=adr064_nullable_sparse_projection profile={} rows={} records={} page_values={} projection_shape={} projected_cascade={} actual_cascade={} null_distribution={} mask_shape={} predicate_source={} route={} output_rows={} equality={} null_count_equal={} null_count={} iterations={} dense_total_ms={:.3} sparse_total_ms={:.3} speedup_micros={} dense_get_range_calls={} sparse_get_range_calls={} dense_get_range_bytes={} sparse_get_range_bytes={} dense_projection_plaintext_bytes={} sparse_value_plaintext_bytes={} sparse_validity_plaintext_bytes={} sparse_projection_plaintext_bytes={} dense_projection_ciphertext_bytes={} sparse_projection_ciphertext_bytes={} dense_selected_ranges={} sparse_selected_ranges={} selected_value_pages={} selected_validity_ranges={} get_bytes_calls={} output_digest={}{}",
		dataset.profile.as_str(),
		dataset.rows,
		dataset.fixtures.len(),
		dataset.page_values,
		dataset.shape.as_str(),
		cascade.as_str(),
		sparse_projection_runtime_actual_cascade_for_adr064(dataset),
		dataset.null_distribution.as_str(),
		shape.as_str(),
		predicate_source.as_str(),
		measurement.route,
		measurement.sparse.output_rows,
		measurement.equality,
		measurement.null_count_equal,
		measurement.sparse.null_count,
		iterations,
		measurement.dense.elapsed.as_secs_f64() * 1_000.0,
		measurement.sparse.elapsed.as_secs_f64() * 1_000.0,
		sparse_projection_speedup_micros_adr064(&measurement.dense, &measurement.sparse),
		measurement.dense.get_range_calls,
		measurement.sparse.get_range_calls,
		measurement.dense.get_range_bytes,
		measurement.sparse.get_range_bytes,
		measurement.dense.projection_plaintext_bytes,
		measurement.sparse.value_plaintext_bytes,
		measurement.sparse.validity_plaintext_bytes,
		measurement.sparse.projection_plaintext_bytes,
		measurement.dense.projection_ciphertext_bytes,
		measurement.sparse.projection_ciphertext_bytes,
		measurement.dense.selected_ranges,
		measurement.sparse.selected_ranges,
		measurement.sparse.selected_value_pages,
		measurement.sparse.selected_validity_ranges,
		measurement
			.dense
			.get_bytes_calls
			.saturating_add(measurement.sparse.get_bytes_calls),
		measurement.sparse.digest,
		extra,
	);
}

fn sparse_projection_runtime_actual_cascade_for_adr064(
	dataset: &Adr064RuntimeDataset,
) -> &'static str {
	let Some(first_column) = dataset.fixtures.first().and_then(|fixture| {
		fixture
			.columns
			.iter()
			.find(|column| column.data_type == DataType::UInt32)
	}) else {
		return "none";
	};
	let first = first_column
		.cascade
		.as_ref()
		.map(|cascade| cascade.kind().as_str())
		.unwrap_or("plain");
	if dataset.fixtures.iter().all(|fixture| {
		fixture.columns.iter().all(|column| {
			column.data_type != DataType::UInt32
				|| column
					.cascade
					.as_ref()
					.map(|cascade| cascade.kind().as_str())
					.unwrap_or("plain")
					== first
		})
	}) {
		first
	} else {
		"mixed"
	}
}

fn sparse_projection_speedup_micros_adr064(
	dense: &Adr064RuntimeOutcome,
	sparse: &Adr064RuntimeOutcome,
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

fn adr065_zip_requirement(dataset: &Adr064RuntimeDataset) -> ColumnFetchRequirement {
	let output_columns = std::iter::once(1_u32)
		.chain((2..2 + dataset.shape.projection_columns()).map(|column| column as u32));
	ColumnFetchRequirement {
		output_columns: ColumnOrdinalSet::from_columns(output_columns),
		..ColumnFetchRequirement::default()
	}
}

fn adr065_projected_batch_for_columns(
	fixture: &Adr064RuntimeFixture,
	columns: &[usize],
	mask: &BooleanArray,
) -> std::io::Result<ProjectedRecordBatch> {
	let mut fields = Vec::with_capacity(columns.len());
	let mut arrays = Vec::with_capacity(columns.len());
	let mut mapping = vec![None; columns.iter().copied().max().unwrap_or(0).saturating_add(1)];
	for (physical, descriptor_ordinal) in columns.iter().copied().enumerate() {
		fields.push(fixture.batch.schema().field(descriptor_ordinal).clone());
		arrays.push(fixture.batch.column(descriptor_ordinal).clone());
		mapping[descriptor_ordinal] = Some(physical);
	}
	let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).map_err(|err| {
		Error::new(
			ErrorKind::InvalidData,
			format!("ADR 065 projected source batch: {err}"),
		)
	})?;
	let filtered = if mask.true_count() == 0 {
		batch.slice(0, 0)
	} else if mask.null_count() == 0 && mask.true_count() == mask.len() {
		batch
	} else {
		arrow::compute::filter_record_batch(&batch, mask).map_err(|err| {
			Error::new(
				ErrorKind::InvalidData,
				format!("ADR 065 projected source filter: {err}"),
			)
		})?
	};
	Ok(ProjectedRecordBatch {
		batch: filtered,
		ordinal_mapping: ColumnOrdinalMapping::new(mapping),
		storage_mapping: BTreeMap::new(),
	})
}

fn adr065_dense_oracle_batch(
	fixture: &Adr064RuntimeFixture,
	dataset: &Adr064RuntimeDataset,
	mask: &BooleanArray,
) -> std::io::Result<RecordBatch> {
	let columns = std::iter::once(1_usize)
		.chain(2..2 + dataset.shape.projection_columns())
		.collect::<Vec<_>>();
	adr065_projected_batch_for_columns(fixture, &columns, mask)?
		.user_visible()
		.map_err(|err| {
			Error::new(
				ErrorKind::InvalidData,
				format!("ADR 065 dense oracle projection: {err}"),
			)
		})
}

fn adr065_predicate_filter_runtime(
	dataset: &Adr064RuntimeDataset,
	shape: SparseProjectionMaskShape,
) -> std::io::Result<(usize, String)> {
	let mut digest = blake3::Hasher::new();
	let mut output_rows = 0_usize;
	for fixture in &dataset.fixtures {
		let mask = sparse_projection_mask(shape, fixture.row_group_rows);
		let predicate = adr065_projected_batch_for_columns(fixture, &[1], &mask)?;
		output_rows = output_rows.saturating_add(predicate.batch.num_rows());
		let visible = predicate.user_visible().map_err(|err| {
			Error::new(
				ErrorKind::InvalidData,
				format!("ADR 065 predicate visible projection: {err}"),
			)
		})?;
		for column in visible.columns() {
			digest.update(&column.get_array_memory_size().to_le_bytes());
		}
	}
	Ok((output_rows, digest.finalize().to_hex().to_string()))
}

fn adr065_zip_runtime(
	dataset: &Adr064RuntimeDataset,
	shape: SparseProjectionMaskShape,
) -> std::io::Result<(usize, String)> {
	let requirement = adr065_zip_requirement(dataset);
	let mut digest = blake3::Hasher::new();
	let mut output_rows = 0_usize;
	for fixture in &dataset.fixtures {
		let mask = sparse_projection_mask(shape, fixture.row_group_rows);
		let predicate = adr065_projected_batch_for_columns(fixture, &[1], &mask)?;
		let projection_columns = (2..2 + dataset.shape.projection_columns()).collect::<Vec<_>>();
		let projection = adr065_projected_batch_for_columns(fixture, &projection_columns, &mask)?;
		let zipped = zip_predicate_and_projection_columns(&predicate, &projection, &requirement)?;
		let dense_oracle = adr065_dense_oracle_batch(fixture, dataset, &mask)?;
		let visible = zipped.user_visible().map_err(|err| {
			Error::new(
				ErrorKind::InvalidData,
				format!("ADR 065 zipped visible projection: {err}"),
			)
		})?;
		if visible != dense_oracle {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"ADR 065 dense predicate zip output mismatch",
			));
		}
		output_rows = output_rows.saturating_add(visible.num_rows());
		for column in visible.columns() {
			digest.update(&column.get_array_memory_size().to_le_bytes());
		}
	}
	Ok((output_rows, digest.finalize().to_hex().to_string()))
}

fn adr065_dense_predicate_plaintext_bytes(dataset: &Adr064RuntimeDataset) -> std::io::Result<u64> {
	let mut bytes = 0_u64;
	for fixture in &dataset.fixtures {
		let page_directory = fixture.directory.page_directory.as_ref().ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				"ADR 065 dense predicate missing page directory",
			)
		})?;
		let requirement = ColumnFetchRequirement {
			output_columns: ColumnOrdinalSet::from_columns([1]),
			..ColumnFetchRequirement::default()
		};
		let ranges = selected_row_group_record_body_ranges(
			&fixture.directory,
			page_directory,
			&requirement,
			&[RowGroupIndex::ZERO],
		)
		.map_err(|err| {
			Error::new(
				ErrorKind::InvalidData,
				format!("ADR 065 dense predicate ranges: {err}"),
			)
		})?;
		bytes = bytes.saturating_add(ranges.iter().map(|range| range.len.get()).sum::<u64>());
	}
	Ok(bytes)
}

fn adr065_ratio_micros(numerator: u64, denominator: u64) -> u64 {
	if denominator == 0 {
		u64::MAX
	} else {
		(numerator as f64 / denominator as f64 * 1_000_000.0) as u64
	}
}

fn bench_adr065_dense_predicate_zip_matrix(config: &BenchConfig) -> std::io::Result<()> {
	let runtime = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()?;
	let mut emitted = 0_usize;
	for rows in adr064_nullable_sparse_projection_rows(config) {
		let profile = if *rows == ROW_GROUP_SCAN_TARGET_ROWS as usize {
			RowGroupSizingProfile::Scan
		} else {
			RowGroupSizingProfile::PointRead
		};
		let iterations = adr064_runtime_iterations(config, *rows);
		let dataset = adr064_nullable_runtime_dataset(
			*rows,
			SparseProjectionCascadeFixture::BitPacked,
			Adr064ProjectionShape::FourNullable,
			Adr064NullDistribution::Alternating,
			256,
			profile,
		)?;
		let slab_id = SlabId::new(68_000 + *rows as u64);
		let crypto = seeded_dev_crypto_manager(TenantId::ZERO)?;
		let encrypted = adr064_runtime_encrypted_dataset(&dataset, &crypto, slab_id)?;
		let store = Arc::new(CountingBenchObjectStore::new(Arc::new(
			MockObjectStore::new(MockObjectStoreConfig::default()),
		)));
		let uri = runtime.block_on(store.put(
			&format!("adr065-dense-predicate-zip/{}.wnd", rows),
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
		let measurement = measure_adr064_runtime_case(
			&dataset,
			SparseProjectionMaskShape::ClusteredFivePercent,
			Adr064PredicateSource::ExactMask,
			iterations,
			&env,
			true,
			false,
		)?;
		let mut predicate_rows = 0_usize;
		let mut predicate_digest = String::new();
		let predicate_elapsed = bench(iterations, || {
			let (rows, digest) = adr065_predicate_filter_runtime(
				black_box(&dataset),
				black_box(SparseProjectionMaskShape::ClusteredFivePercent),
			)
			.expect("ADR 065 predicate filter runtime");
			predicate_rows = rows;
			predicate_digest = digest;
		});
		let mut zip_rows = 0_usize;
		let mut zip_digest = String::new();
		let zip_elapsed = bench(iterations, || {
			let (rows, digest) = adr065_zip_runtime(
				black_box(&dataset),
				black_box(SparseProjectionMaskShape::ClusteredFivePercent),
			)
			.expect("ADR 065 zip runtime");
			zip_rows = rows;
			zip_digest = digest;
		});
		if predicate_rows != measurement.sparse.output_rows
			|| zip_rows != measurement.sparse.output_rows
		{
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"ADR 065 row mismatch predicate_rows={} zip_rows={} sparse_rows={}",
					predicate_rows, zip_rows, measurement.sparse.output_rows
				),
			));
		}
		let dense_predicate_plaintext_bytes = adr065_dense_predicate_plaintext_bytes(&dataset)?;
		let total_dense_query_plaintext_bytes = dense_predicate_plaintext_bytes
			.saturating_add(measurement.dense.projection_plaintext_bytes);
		let total_sparse_query_plaintext_bytes = dense_predicate_plaintext_bytes
			.saturating_add(measurement.sparse.projection_plaintext_bytes);
		let dense_elapsed = predicate_elapsed.saturating_add(measurement.dense.elapsed);
		let sparse_elapsed = predicate_elapsed
			.saturating_add(measurement.sparse.elapsed)
			.saturating_add(zip_elapsed);
		let dense_micros = dense_elapsed.as_secs_f64() * 1_000_000.0;
		let sparse_micros = sparse_elapsed.as_secs_f64() * 1_000_000.0;
		let speedup_micros = if sparse_micros > 0.0 {
			(dense_micros / sparse_micros * 1_000_000.0) as u64
		} else {
			u64::MAX
		};
		println!(
			"row_group_fetch_measure scenario=adr065_dense_predicate_zip profile={} rows={} records={} page_values={} predicate_source=scalar_dense_decode route=case_b_sparse_zip output_rows={} equality={} null_count_equal={} iterations={} dense_predicate_plaintext_bytes={} dense_remaining_projection_plaintext_bytes={} sparse_value_plaintext_bytes={} sparse_validity_plaintext_bytes={} sparse_projection_plaintext_bytes={} total_dense_query_plaintext_bytes={} total_sparse_query_plaintext_bytes={} remaining_projection_reduction_micros={} total_query_reduction_micros={} dense_total_ms={:.3} sparse_total_ms={:.3} speedup_micros={} dense_get_range_calls={} sparse_get_range_calls={} dense_get_range_bytes={} sparse_get_range_bytes={} dense_projection_ciphertext_bytes={} sparse_projection_ciphertext_bytes={} dense_selected_ranges={} sparse_selected_ranges={} selected_value_pages={} selected_validity_ranges={} get_bytes_calls={} predicate_digest={} zip_digest={}",
			dataset.profile.as_str(),
			dataset.rows,
			dataset.fixtures.len(),
			dataset.page_values,
			measurement.sparse.output_rows,
			measurement.equality,
			measurement.null_count_equal,
			iterations,
			dense_predicate_plaintext_bytes,
			measurement.dense.projection_plaintext_bytes,
			measurement.sparse.value_plaintext_bytes,
			measurement.sparse.validity_plaintext_bytes,
			measurement.sparse.projection_plaintext_bytes,
			total_dense_query_plaintext_bytes,
			total_sparse_query_plaintext_bytes,
			adr065_ratio_micros(
				measurement.dense.projection_plaintext_bytes,
				measurement.sparse.projection_plaintext_bytes
			),
			adr065_ratio_micros(
				total_dense_query_plaintext_bytes,
				total_sparse_query_plaintext_bytes
			),
			dense_elapsed.as_secs_f64() * 1_000.0,
			sparse_elapsed.as_secs_f64() * 1_000.0,
			speedup_micros,
			measurement.dense.get_range_calls,
			measurement.sparse.get_range_calls,
			measurement.dense.get_range_bytes,
			measurement.sparse.get_range_bytes,
			measurement.dense.projection_ciphertext_bytes,
			measurement.sparse.projection_ciphertext_bytes,
			measurement.dense.selected_ranges,
			measurement.sparse.selected_ranges,
			measurement.sparse.selected_value_pages,
			measurement.sparse.selected_validity_ranges,
			measurement
				.dense
				.get_bytes_calls
				.saturating_add(measurement.sparse.get_bytes_calls),
			predicate_digest,
			zip_digest,
		);
		emitted = emitted.saturating_add(1);
	}
	println!(
		"row_group_fetch_bench scenario=adr065_dense_predicate_zip rows=mixed emitted_rows={} iterations=max10_1m3",
		emitted,
	);
	Ok(())
}

struct Adr066FsstPredicateFixture {
	values: Vec<String>,
	decoded: Vec<u8>,
	offsets: Vec<i32>,
	offsets_u64: Vec<u64>,
	offset_bytes: Vec<u8>,
	encoded: wendelon::storage::encoding::cascade::EncodedBuffer,
	encoding: BufferEncoding,
	atom: PredicateAtom,
}

#[derive(Clone, Debug, Default)]
struct Adr066FsstPredicateMeasurement {
	dense_elapsed: Duration,
	kernel_elapsed: Duration,
	selected_rows: usize,
	decoded_plaintext_bytes: u64,
	encoded_payload_bytes: u64,
	offsets_plaintext_bytes: u64,
	fsst_row_safe_metadata_bytes: u64,
	fsst_row_safe_payload_bytes: u64,
	mask_equal: bool,
	mask_digest: String,
}

fn adr066_fsst_predicate_fixture(rows: usize) -> std::io::Result<Adr066FsstPredicateFixture> {
	let selected = rows.saturating_mul(5).div_ceil(100);
	let target = "https://fsst.example.com/api/v1/tenant/selected";
	let values = (0..rows)
		.map(|row| {
			if row < selected {
				target.to_string()
			} else {
				format!("https://fsst.example.com/api/v1/tenant/miss/{row:08}")
			}
		})
		.collect::<Vec<_>>();
	let mut decoded = Vec::new();
	let mut offsets = Vec::with_capacity(rows.saturating_add(1));
	let mut offsets_u64 = Vec::with_capacity(rows.saturating_add(1));
	offsets.push(0);
	offsets_u64.push(0);
	for value in &values {
		decoded.extend_from_slice(value.as_bytes());
		let offset = i32::try_from(decoded.len()).map_err(|_| {
			Error::new(
				ErrorKind::InvalidInput,
				"ADR 066 FSST predicate fixture exceeds i32 offsets",
			)
		})?;
		offsets.push(offset);
		offsets_u64.push(offset as u64);
	}
	let offset_bytes = offsets
		.iter()
		.flat_map(|offset| offset.to_le_bytes())
		.collect::<Vec<_>>();
	let cascade = EncodingCascade::FsstWholeBuffer {
		symbol_table_byte_len: RecordBodyByteLen::ZERO,
	};
	let encoded = encode_buffer(
		&decoded,
		&DataType::Utf8,
		cascade.clone(),
		BufferValueCount::new(rows as u32),
		SimdPageValueCount::new(1024),
	)
	.map_err(|err| {
		Error::new(
			ErrorKind::InvalidData,
			format!("ADR 066 FSST encode: {err}"),
		)
	})?;
	let encoding = BufferEncoding {
		row_group_index: RowGroupIndex::ZERO,
		ipc_column_ordinal: IpcColumnOrdinal::new(1),
		ipc_buffer_ordinal: IpcBufferIndex::new(2),
		cascade_index: EncodingCascadeIndex::ZERO,
		decoded_byte_len: encoded.decoded_byte_len,
		decoded_value_count: encoded.decoded_value_count,
		encoded_range: encoded.encoded_range(),
		page_value_count: SimdPageValueCount::new(1024),
		page_end_offsets: encoded.page_end_offsets.clone(),
		page_stats: Vec::new(),
	};
	let atom = PredicateAtom {
		op: PredicateAtomOp::Eq,
		literal: Some(ColumnPredicateLiteral::Str(target.as_bytes().to_vec())),
	};
	Ok(Adr066FsstPredicateFixture {
		values,
		decoded,
		offsets,
		offsets_u64,
		offset_bytes,
		encoded,
		encoding,
		atom,
	})
}

fn adr067_fsst_row_safe_predicate_fixture(
	rows: usize,
) -> std::io::Result<Adr066FsstPredicateFixture> {
	let mut fixture = adr066_fsst_predicate_fixture(rows)?;
	let cascade = EncodingCascade::FsstRowSafe {
		symbol_table_byte_len: RecordBodyByteLen::ZERO,
	};
	let encoded = encode_buffer_with_value_offsets(
		&fixture.decoded,
		&DataType::Utf8,
		cascade,
		BufferValueCount::new(rows as u32),
		SimdPageValueCount::new(1024),
		Some(&fixture.offsets_u64),
	)
	.map_err(|err| {
		Error::new(
			ErrorKind::InvalidData,
			format!("ADR 067 RowSafe FSST encode: {err}"),
		)
	})?;
	fixture.encoding = BufferEncoding {
		row_group_index: RowGroupIndex::ZERO,
		ipc_column_ordinal: IpcColumnOrdinal::new(1),
		ipc_buffer_ordinal: IpcBufferIndex::new(2),
		cascade_index: EncodingCascadeIndex::ZERO,
		decoded_byte_len: encoded.decoded_byte_len,
		decoded_value_count: encoded.decoded_value_count,
		encoded_range: encoded.encoded_range(),
		page_value_count: SimdPageValueCount::new(1024),
		page_end_offsets: encoded.page_end_offsets.clone(),
		page_stats: Vec::new(),
	};
	fixture.encoded = encoded;
	Ok(fixture)
}

fn adr066_hash_mask(selected: impl IntoIterator<Item = bool>) -> (usize, String) {
	let mut selected_rows = 0_usize;
	let mut hasher = blake3::Hasher::new();
	for selected in selected {
		selected_rows = selected_rows.saturating_add(usize::from(selected));
		hasher.update(&[u8::from(selected)]);
	}
	(selected_rows, hasher.finalize().to_hex().to_string())
}

fn adr066_measure_fsst_predicate(
	fixtures: &[Adr066FsstPredicateFixture],
	iterations: usize,
) -> std::io::Result<Adr066FsstPredicateMeasurement> {
	let mut dense_rows = 0_usize;
	let mut dense_digest = String::new();
	let dense_elapsed = bench(iterations, || {
		let mut all_selected = Vec::new();
		for fixture in fixtures {
			let target = match fixture.atom.literal.as_ref() {
				Some(ColumnPredicateLiteral::Str(bytes)) => bytes.as_slice(),
				_ => panic!("ADR 066 fixture literal is pinned to string bytes"),
			};
			all_selected.extend(
				fixture
					.values
					.iter()
					.map(|value| value.as_bytes() == target),
			);
		}
		(dense_rows, dense_digest) = adr066_hash_mask(all_selected);
	});

	let mut kernel_rows = 0_usize;
	let mut kernel_digest = String::new();
	let kernel_elapsed = bench(iterations, || {
		let mut all_selected = Vec::new();
		for fixture in fixtures {
			let coercion = coerce_literal(fixture.atom.literal.as_ref(), &DataType::Utf8)
				.expect("ADR 066 fixture literal should coerce");
			let mask = evaluate_fsst_predicate(
				&fixture.encoded.bytes,
				&fixture.encoding,
				&fixture.offset_bytes,
				None,
				None,
				&DataType::Utf8,
				&fixture.atom,
				&coercion,
			)
			.expect("ADR 066 FSST predicate should evaluate");
			all_selected.extend(mask.to_bool_vec().expect("ADR 066 mask bools"));
		}
		(kernel_rows, kernel_digest) = adr066_hash_mask(all_selected);
	});

	let expected_rows = fixtures
		.iter()
		.map(|fixture| fixture.offsets.len().saturating_sub(1))
		.map(|rows| rows.saturating_mul(5).div_ceil(100))
		.sum::<usize>();
	if dense_rows != expected_rows || kernel_rows != expected_rows {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"ADR 066 FSST mask row mismatch dense={} kernel={} expected={}",
				dense_rows, kernel_rows, expected_rows
			),
		));
	}
	Ok(Adr066FsstPredicateMeasurement {
		dense_elapsed,
		kernel_elapsed,
		selected_rows: kernel_rows,
		decoded_plaintext_bytes: fixtures
			.iter()
			.map(|fixture| fixture.decoded.len() as u64)
			.sum(),
		encoded_payload_bytes: fixtures
			.iter()
			.map(|fixture| fixture.encoded.bytes.len() as u64)
			.sum(),
		offsets_plaintext_bytes: fixtures
			.iter()
			.map(|fixture| fixture.offset_bytes.len() as u64)
			.sum(),
		fsst_row_safe_metadata_bytes: 0,
		fsst_row_safe_payload_bytes: 0,
		mask_equal: dense_digest == kernel_digest,
		mask_digest: kernel_digest,
	})
}

fn adr067_fsst_row_safe_lengths(encoded: &[u8], rows: usize) -> std::io::Result<(u64, u64)> {
	if encoded.len() < 13 || &encoded[..4] != b"RSFS" {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"ADR 067 RSFS magic missing",
		));
	}
	let symbol_count = encoded[4] as usize;
	let row_count_offset = 5_usize
		.checked_add(symbol_count.checked_mul(2).ok_or_else(|| {
			Error::new(ErrorKind::InvalidData, "ADR 067 RSFS symbol table overflow")
		})?)
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "ADR 067 RSFS row count overflow"))?;
	let row_count_end = row_count_offset.checked_add(4).ok_or_else(|| {
		Error::new(
			ErrorKind::InvalidData,
			"ADR 067 RSFS row count end overflow",
		)
	})?;
	if encoded.len() < row_count_end {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"ADR 067 RSFS row count truncated",
		));
	}
	let row_count = u32::from_le_bytes(
		encoded[row_count_offset..row_count_end]
			.try_into()
			.expect("slice length checked"),
	) as usize;
	if row_count != rows {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"ADR 067 RSFS row count mismatch",
		));
	}
	const FSST_ROW_SAFE_BLOCK_ROWS: usize = 256;
	let block_count = row_count.div_ceil(FSST_ROW_SAFE_BLOCK_ROWS);
	let block_offset_bytes = block_count.checked_mul(8).ok_or_else(|| {
		Error::new(
			ErrorKind::InvalidData,
			"ADR 067 RSFS block_offsets overflow",
		)
	})?;
	let lengths_start = row_count_end
		.checked_add(block_offset_bytes)
		.ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				"ADR 067 RSFS block_offsets end overflow",
			)
		})?;
	let exception_count_start = lengths_start
		.checked_add(row_count)
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "ADR 067 RSFS lengths end overflow"))?;
	let exception_count_end = exception_count_start.checked_add(4).ok_or_else(|| {
		Error::new(
			ErrorKind::InvalidData,
			"ADR 067 RSFS exception_count end overflow",
		)
	})?;
	if encoded.len() < exception_count_end {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"ADR 067 RSFS metadata truncated",
		));
	}
	let exception_count = u32::from_le_bytes(
		encoded[exception_count_start..exception_count_end]
			.try_into()
			.expect("slice length checked"),
	) as usize;
	let exception_bytes = exception_count.checked_mul(4).ok_or_else(|| {
		Error::new(
			ErrorKind::InvalidData,
			"ADR 067 RSFS exception_lengths overflow",
		)
	})?;
	let payload_start = exception_count_end
		.checked_add(exception_bytes)
		.ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				"ADR 067 RSFS payload start overflow",
			)
		})?;
	if payload_start > encoded.len() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"ADR 067 RSFS metadata truncated",
		));
	}
	// metadata = block_offsets + encoded_lengths + 4-byte exception_count + exception_lengths
	let metadata_bytes = (block_offset_bytes
		.saturating_add(row_count)
		.saturating_add(4)
		.saturating_add(exception_bytes)) as u64;
	Ok((metadata_bytes, (encoded.len() - payload_start) as u64))
}

fn adr067_measure_fsst_row_safe_predicate(
	fixtures: &[Adr066FsstPredicateFixture],
	iterations: usize,
) -> std::io::Result<Adr066FsstPredicateMeasurement> {
	let mut dense_rows = 0_usize;
	let mut dense_digest = String::new();
	let dense_elapsed = bench(iterations, || {
		let mut all_selected = Vec::new();
		for fixture in fixtures {
			let target = match fixture.atom.literal.as_ref() {
				Some(ColumnPredicateLiteral::Str(bytes)) => bytes.as_slice(),
				_ => panic!("ADR 067 fixture literal is pinned to string bytes"),
			};
			all_selected.extend(
				fixture
					.values
					.iter()
					.map(|value| value.as_bytes() == target),
			);
		}
		(dense_rows, dense_digest) = adr066_hash_mask(all_selected);
	});

	let mut kernel_rows = 0_usize;
	let mut kernel_digest = String::new();
	let kernel_elapsed = bench(iterations, || {
		let mut all_selected = Vec::new();
		for fixture in fixtures {
			let coercion = coerce_literal(fixture.atom.literal.as_ref(), &DataType::Utf8)
				.expect("ADR 067 fixture literal should coerce");
			let mask = evaluate_fsst_row_safe_predicate(
				&fixture.encoded.bytes,
				&fixture.encoding,
				&fixture.offset_bytes,
				None,
				None,
				&DataType::Utf8,
				&fixture.atom,
				&coercion,
			)
			.expect("ADR 067 RowSafe FSST predicate should evaluate");
			all_selected.extend(mask.to_bool_vec().expect("ADR 067 mask bools"));
		}
		(kernel_rows, kernel_digest) = adr066_hash_mask(all_selected);
	});

	let expected_rows = fixtures
		.iter()
		.map(|fixture| fixture.offsets.len().saturating_sub(1))
		.map(|rows| rows.saturating_mul(5).div_ceil(100))
		.sum::<usize>();
	if dense_rows != expected_rows || kernel_rows != expected_rows {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"ADR 067 RowSafe mask row mismatch dense={} kernel={} expected={}",
				dense_rows, kernel_rows, expected_rows
			),
		));
	}
	let mut fsst_row_safe_metadata_bytes = 0_u64;
	let mut fsst_row_safe_payload_bytes = 0_u64;
	for fixture in fixtures {
		let (offset_bytes, payload_bytes) =
			adr067_fsst_row_safe_lengths(&fixture.encoded.bytes, fixture.offsets.len() - 1)?;
		fsst_row_safe_metadata_bytes = fsst_row_safe_metadata_bytes.saturating_add(offset_bytes);
		fsst_row_safe_payload_bytes = fsst_row_safe_payload_bytes.saturating_add(payload_bytes);
	}
	Ok(Adr066FsstPredicateMeasurement {
		dense_elapsed,
		kernel_elapsed,
		selected_rows: kernel_rows,
		decoded_plaintext_bytes: fixtures
			.iter()
			.map(|fixture| fixture.decoded.len() as u64)
			.sum(),
		encoded_payload_bytes: fixtures
			.iter()
			.map(|fixture| fixture.encoded.bytes.len() as u64)
			.sum(),
		offsets_plaintext_bytes: fixtures
			.iter()
			.map(|fixture| fixture.offset_bytes.len() as u64)
			.sum(),
		fsst_row_safe_metadata_bytes,
		fsst_row_safe_payload_bytes,
		mask_equal: dense_digest == kernel_digest,
		mask_digest: kernel_digest,
	})
}

fn duration_ratio_micros(numerator: Duration, denominator: Duration) -> u64 {
	let numerator_micros = numerator.as_secs_f64() * 1_000_000.0;
	let denominator_micros = denominator.as_secs_f64() * 1_000_000.0;
	if denominator_micros > 0.0 {
		(numerator_micros / denominator_micros * 1_000_000.0) as u64
	} else if numerator_micros > 0.0 {
		u64::MAX
	} else {
		1_000_000
	}
}

fn bench_adr066_fsst_row_group_fetch_matrix(config: &BenchConfig) -> std::io::Result<()> {
	let runtime = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()?;
	let mut emitted = 0_usize;
	for rows in adr064_nullable_sparse_projection_rows(config) {
		let profile = if *rows == ROW_GROUP_SCAN_TARGET_ROWS as usize {
			RowGroupSizingProfile::Scan
		} else {
			RowGroupSizingProfile::PointRead
		};
		let iterations = adr064_runtime_iterations(config, *rows);
		let dataset = adr064_nullable_runtime_dataset(
			*rows,
			SparseProjectionCascadeFixture::BitPacked,
			Adr064ProjectionShape::FourNullable,
			Adr064NullDistribution::Alternating,
			256,
			profile,
		)?;
		let fsst_fixtures = dataset
			.fixtures
			.iter()
			.map(|fixture| adr066_fsst_predicate_fixture(fixture.row_group_rows))
			.collect::<std::io::Result<Vec<_>>>()?;
		let predicate = adr066_measure_fsst_predicate(&fsst_fixtures, iterations)?;
		let slab_id = SlabId::new(66_000 + *rows as u64);
		let crypto = seeded_dev_crypto_manager(TenantId::ZERO)?;
		let encrypted = adr064_runtime_encrypted_dataset(&dataset, &crypto, slab_id)?;
		let store = Arc::new(CountingBenchObjectStore::new(Arc::new(
			MockObjectStore::new(MockObjectStoreConfig::default()),
		)));
		let uri = runtime.block_on(store.put(
			&format!("adr066-fsst-row-group-fetch/{}.wnd", rows),
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
		let projection = measure_adr064_runtime_case(
			&dataset,
			SparseProjectionMaskShape::ClusteredFivePercent,
			Adr064PredicateSource::ExactMask,
			iterations,
			&env,
			true,
			false,
		)?;
		if predicate.selected_rows != projection.sparse.output_rows {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"ADR 066 FSST predicate/projection row mismatch predicate={} projection={}",
					predicate.selected_rows, projection.sparse.output_rows
				),
			));
		}
		let dense_elapsed = predicate
			.dense_elapsed
			.saturating_add(projection.dense.elapsed);
		let sparse_elapsed = predicate
			.kernel_elapsed
			.saturating_add(projection.sparse.elapsed);
		let total_dense_query_plaintext_bytes = predicate
			.decoded_plaintext_bytes
			.saturating_add(projection.dense.projection_plaintext_bytes);
		let total_sparse_query_plaintext_bytes = predicate
			.encoded_payload_bytes
			.saturating_add(predicate.offsets_plaintext_bytes)
			.saturating_add(projection.sparse.projection_plaintext_bytes);
		println!(
			"row_group_fetch_measure scenario=adr066_fsst_row_group_fetch profile={} rows={} records={} page_values={} predicate_source=fsst_eq route=case_a_after_fsst output_rows={} equality={} mask_equal={} null_count_equal={} null_count={} iterations={} dense_predicate_plaintext_bytes={} fsst_encoded_payload_bytes={} fsst_offsets_plaintext_bytes={} encoded_payload_ratio_micros={} fsst_kernel_vs_dense_cpu_micros={} dense_projection_plaintext_bytes={} sparse_value_plaintext_bytes={} sparse_validity_plaintext_bytes={} sparse_projection_plaintext_bytes={} total_dense_query_plaintext_bytes={} total_sparse_query_plaintext_bytes={} total_query_reduction_micros={} dense_total_ms={:.3} sparse_total_ms={:.3} speedup_micros={} dense_get_range_calls={} sparse_get_range_calls={} dense_get_range_bytes={} sparse_get_range_bytes={} dense_projection_ciphertext_bytes={} sparse_projection_ciphertext_bytes={} dense_selected_ranges={} sparse_selected_ranges={} selected_value_pages={} selected_validity_ranges={} get_bytes_calls={} mask_digest={} output_digest={}",
			dataset.profile.as_str(),
			dataset.rows,
			dataset.fixtures.len(),
			dataset.page_values,
			projection.sparse.output_rows,
			projection.equality,
			predicate.mask_equal,
			projection.null_count_equal,
			projection.sparse.null_count,
			iterations,
			predicate.decoded_plaintext_bytes,
			predicate.encoded_payload_bytes,
			predicate.offsets_plaintext_bytes,
			adr065_ratio_micros(
				predicate.encoded_payload_bytes,
				predicate.decoded_plaintext_bytes,
			),
			duration_ratio_micros(predicate.kernel_elapsed, predicate.dense_elapsed),
			projection.dense.projection_plaintext_bytes,
			projection.sparse.value_plaintext_bytes,
			projection.sparse.validity_plaintext_bytes,
			projection.sparse.projection_plaintext_bytes,
			total_dense_query_plaintext_bytes,
			total_sparse_query_plaintext_bytes,
			adr065_ratio_micros(
				total_dense_query_plaintext_bytes,
				total_sparse_query_plaintext_bytes,
			),
			dense_elapsed.as_secs_f64() * 1_000.0,
			sparse_elapsed.as_secs_f64() * 1_000.0,
			duration_ratio_micros(dense_elapsed, sparse_elapsed),
			projection.dense.get_range_calls,
			projection.sparse.get_range_calls,
			projection.dense.get_range_bytes,
			projection.sparse.get_range_bytes,
			projection.dense.projection_ciphertext_bytes,
			projection.sparse.projection_ciphertext_bytes,
			projection.dense.selected_ranges,
			projection.sparse.selected_ranges,
			projection.sparse.selected_value_pages,
			projection.sparse.selected_validity_ranges,
			projection
				.dense
				.get_bytes_calls
				.saturating_add(projection.sparse.get_bytes_calls),
			predicate.mask_digest,
			projection.sparse.digest,
		);

		let mut a2_rows = 0_usize;
		let mut a2_digest = String::new();
		let a2_zip_elapsed = bench(iterations, || {
			let (rows, digest) = adr065_zip_runtime(
				black_box(&dataset),
				black_box(SparseProjectionMaskShape::ClusteredFivePercent),
			)
			.expect("ADR 066 A2 dense Case B zip runtime");
			a2_rows = rows;
			a2_digest = digest;
		});
		if a2_rows != predicate.selected_rows {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"ADR 066 A2 row mismatch predicate={} zipped={}",
					predicate.selected_rows, a2_rows
				),
			));
		}
		let a2_elapsed = predicate
			.dense_elapsed
			.saturating_add(projection.dense.elapsed)
			.saturating_add(a2_zip_elapsed);
		println!(
			"row_group_fetch_measure scenario=adr066_fsst_row_group_fetch profile={} rows={} records={} page_values={} predicate_source=fsst_eq route=dense_case_b_overlap_a2 kernel_engaged=false output_rows={} equality={} mask_equal={} null_count_equal={} iterations={} dense_predicate_plaintext_bytes={} dense_projection_plaintext_bytes={} total_dense_query_plaintext_bytes={} dense_total_ms={:.3} case_b_zip_ms={:.3} fsst_encoded_payload_bytes={} fsst_offsets_plaintext_bytes={} encoded_payload_ratio_micros={} get_range_calls={} get_range_bytes={} selected_ranges={} get_bytes_calls={} mask_digest={} zip_digest={}",
			dataset.profile.as_str(),
			dataset.rows,
			dataset.fixtures.len(),
			dataset.page_values,
			a2_rows,
			projection.equality,
			predicate.mask_equal,
			projection.null_count_equal,
			iterations,
			predicate.decoded_plaintext_bytes,
			projection.dense.projection_plaintext_bytes,
			total_dense_query_plaintext_bytes,
			a2_elapsed.as_secs_f64() * 1_000.0,
			a2_zip_elapsed.as_secs_f64() * 1_000.0,
			predicate.encoded_payload_bytes,
			predicate.offsets_plaintext_bytes,
			adr065_ratio_micros(
				predicate.encoded_payload_bytes,
				predicate.decoded_plaintext_bytes,
			),
			projection.dense.get_range_calls,
			projection.dense.get_range_bytes,
			projection.dense.selected_ranges,
			projection.dense.get_bytes_calls,
			predicate.mask_digest,
			a2_digest,
		);
		emitted = emitted.saturating_add(1);
	}
	println!(
		"row_group_fetch_bench scenario=adr066_fsst_row_group_fetch rows=mixed emitted_rows={} iterations=max10_1m3",
		emitted,
	);
	Ok(())
}

fn bench_adr067_fsst_row_safe_row_group_fetch_matrix(config: &BenchConfig) -> std::io::Result<()> {
	let runtime = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()?;
	let mut emitted = 0_usize;
	for rows in adr064_nullable_sparse_projection_rows(config) {
		let profile = if *rows == ROW_GROUP_SCAN_TARGET_ROWS as usize {
			RowGroupSizingProfile::Scan
		} else {
			RowGroupSizingProfile::PointRead
		};
		let iterations = adr064_runtime_iterations(config, *rows);
		let dataset = adr064_nullable_runtime_dataset(
			*rows,
			SparseProjectionCascadeFixture::BitPacked,
			Adr064ProjectionShape::FourNullable,
			Adr064NullDistribution::Alternating,
			256,
			profile,
		)?;
		let fsst_fixtures = dataset
			.fixtures
			.iter()
			.map(|fixture| adr067_fsst_row_safe_predicate_fixture(fixture.row_group_rows))
			.collect::<std::io::Result<Vec<_>>>()?;
		let predicate = adr067_measure_fsst_row_safe_predicate(&fsst_fixtures, iterations)?;
		let slab_id = SlabId::new(67_000 + *rows as u64);
		let crypto = seeded_dev_crypto_manager(TenantId::ZERO)?;
		let encrypted = adr064_runtime_encrypted_dataset(&dataset, &crypto, slab_id)?;
		let store = Arc::new(CountingBenchObjectStore::new(Arc::new(
			MockObjectStore::new(MockObjectStoreConfig::default()),
		)));
		let uri = runtime.block_on(store.put(
			&format!("adr067-fsst-row-safe-row-group-fetch/{}.wnd", rows),
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
		let projection = measure_adr064_runtime_case(
			&dataset,
			SparseProjectionMaskShape::ClusteredFivePercent,
			Adr064PredicateSource::ExactMask,
			iterations,
			&env,
			true,
			false,
		)?;
		if predicate.selected_rows != projection.sparse.output_rows {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"ADR 067 RowSafe predicate/projection row mismatch predicate={} projection={}",
					predicate.selected_rows, projection.sparse.output_rows
				),
			));
		}
		let dense_elapsed = predicate
			.dense_elapsed
			.saturating_add(projection.dense.elapsed);
		let sparse_elapsed = predicate
			.kernel_elapsed
			.saturating_add(projection.sparse.elapsed);
		let total_dense_query_plaintext_bytes = predicate
			.decoded_plaintext_bytes
			.saturating_add(projection.dense.projection_plaintext_bytes);
		let total_sparse_query_plaintext_bytes = predicate
			.encoded_payload_bytes
			.saturating_add(predicate.offsets_plaintext_bytes)
			.saturating_add(projection.sparse.projection_plaintext_bytes);
		println!(
			"row_group_fetch_measure scenario=adr067_fsst_row_safe_row_group_fetch profile={} rows={} records={} page_values={} predicate_source=fsst_row_safe_eq route=fsst_row_safe_kernel_case_a1 output_rows={} equality={} mask_equal={} null_count_equal={} null_count={} iterations={} dense_predicate_plaintext_bytes={} fsst_row_safe_encoded_bytes={} fsst_row_safe_payload_bytes={} fsst_row_safe_metadata_bytes={} fsst_offsets_plaintext_bytes={} encoded_payload_ratio_micros={} strategy_b_vs_dense_cpu_micros={} dense_projection_plaintext_bytes={} sparse_value_plaintext_bytes={} sparse_validity_plaintext_bytes={} sparse_projection_plaintext_bytes={} total_dense_query_plaintext_bytes={} total_sparse_query_plaintext_bytes={} total_query_reduction_micros={} dense_total_ms={:.3} sparse_total_ms={:.3} speedup_micros={} dense_get_range_calls={} sparse_get_range_calls={} dense_get_range_bytes={} sparse_get_range_bytes={} dense_projection_ciphertext_bytes={} sparse_projection_ciphertext_bytes={} dense_selected_ranges={} sparse_selected_ranges={} selected_value_pages={} selected_validity_ranges={} get_bytes_calls={} mask_digest={} output_digest={}",
			dataset.profile.as_str(),
			dataset.rows,
			dataset.fixtures.len(),
			dataset.page_values,
			projection.sparse.output_rows,
			projection.equality,
			predicate.mask_equal,
			projection.null_count_equal,
			projection.sparse.null_count,
			iterations,
			predicate.decoded_plaintext_bytes,
			predicate.encoded_payload_bytes,
			predicate.fsst_row_safe_payload_bytes,
			predicate.fsst_row_safe_metadata_bytes,
			predicate.offsets_plaintext_bytes,
			adr065_ratio_micros(
				predicate.encoded_payload_bytes,
				predicate.decoded_plaintext_bytes,
			),
			duration_ratio_micros(predicate.kernel_elapsed, predicate.dense_elapsed),
			projection.dense.projection_plaintext_bytes,
			projection.sparse.value_plaintext_bytes,
			projection.sparse.validity_plaintext_bytes,
			projection.sparse.projection_plaintext_bytes,
			total_dense_query_plaintext_bytes,
			total_sparse_query_plaintext_bytes,
			adr065_ratio_micros(
				total_dense_query_plaintext_bytes,
				total_sparse_query_plaintext_bytes,
			),
			dense_elapsed.as_secs_f64() * 1_000.0,
			sparse_elapsed.as_secs_f64() * 1_000.0,
			duration_ratio_micros(dense_elapsed, sparse_elapsed),
			projection.dense.get_range_calls,
			projection.sparse.get_range_calls,
			projection.dense.get_range_bytes,
			projection.sparse.get_range_bytes,
			projection.dense.projection_ciphertext_bytes,
			projection.sparse.projection_ciphertext_bytes,
			projection.dense.selected_ranges,
			projection.sparse.selected_ranges,
			projection.sparse.selected_value_pages,
			projection.sparse.selected_validity_ranges,
			projection
				.dense
				.get_bytes_calls
				.saturating_add(projection.sparse.get_bytes_calls),
			predicate.mask_digest,
			projection.sparse.digest,
		);

		let mut a2_rows = 0_usize;
		let mut a2_digest = String::new();
		let a2_zip_elapsed = bench(iterations, || {
			let (rows, digest) = adr065_zip_runtime(
				black_box(&dataset),
				black_box(SparseProjectionMaskShape::ClusteredFivePercent),
			)
			.expect("ADR 067 A2 dense Case B zip runtime");
			a2_rows = rows;
			a2_digest = digest;
		});
		if a2_rows != predicate.selected_rows {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"ADR 067 A2 row mismatch predicate={} zipped={}",
					predicate.selected_rows, a2_rows
				),
			));
		}
		let a2_elapsed = predicate
			.dense_elapsed
			.saturating_add(projection.dense.elapsed)
			.saturating_add(a2_zip_elapsed);
		println!(
			"row_group_fetch_measure scenario=adr067_fsst_row_safe_row_group_fetch profile={} rows={} records={} page_values={} predicate_source=fsst_row_safe_eq route=fsst_row_safe_dense_case_b_overlap_a2 kernel_engaged=false output_rows={} equality={} mask_equal={} null_count_equal={} iterations={} dense_predicate_plaintext_bytes={} dense_projection_plaintext_bytes={} total_dense_query_plaintext_bytes={} dense_total_ms={:.3} case_b_zip_ms={:.3} fsst_row_safe_encoded_bytes={} fsst_row_safe_payload_bytes={} fsst_row_safe_metadata_bytes={} fsst_offsets_plaintext_bytes={} encoded_payload_ratio_micros={} get_range_calls={} get_range_bytes={} selected_ranges={} get_bytes_calls={} mask_digest={} zip_digest={}",
			dataset.profile.as_str(),
			dataset.rows,
			dataset.fixtures.len(),
			dataset.page_values,
			a2_rows,
			projection.equality,
			predicate.mask_equal,
			projection.null_count_equal,
			iterations,
			predicate.decoded_plaintext_bytes,
			projection.dense.projection_plaintext_bytes,
			total_dense_query_plaintext_bytes,
			a2_elapsed.as_secs_f64() * 1_000.0,
			a2_zip_elapsed.as_secs_f64() * 1_000.0,
			predicate.encoded_payload_bytes,
			predicate.fsst_row_safe_payload_bytes,
			predicate.fsst_row_safe_metadata_bytes,
			predicate.offsets_plaintext_bytes,
			adr065_ratio_micros(
				predicate.encoded_payload_bytes,
				predicate.decoded_plaintext_bytes,
			),
			projection.dense.get_range_calls,
			projection.dense.get_range_bytes,
			projection.dense.selected_ranges,
			projection.dense.get_bytes_calls,
			predicate.mask_digest,
			a2_digest,
		);
		emitted = emitted.saturating_add(1);
	}
	println!(
		"row_group_fetch_bench scenario=adr067_fsst_row_safe_row_group_fetch rows=mixed emitted_rows={} iterations=max10_1m3",
		emitted,
	);
	Ok(())
}

fn adr064_runtime_encrypted_dataset(
	dataset: &Adr064RuntimeDataset,
	crypto: &wendelon::crypto::CryptoManager,
	slab_id: SlabId,
) -> std::io::Result<wendelon::crypto::EncryptedTieredSlab> {
	let mut offset_index = Vec::with_capacity(dataset.fixtures.len());
	for (index, fixture) in dataset.fixtures.iter().enumerate() {
		let mut key = [0x64; 32];
		key[31] = u8::try_from(index % 256).unwrap_or(0);
		offset_index.push(TieredOffsetIndexEntry {
			key_locator: IdentityPrefix::from(Identity::new(key)),
			byte_offset: u64::try_from(fixture.record_plaintext_range.start).map_err(|_| {
				Error::new(ErrorKind::InvalidInput, "ADR 064 record offset exceeds u64")
			})?,
			byte_length: u32::try_from(fixture.frame.len()).map_err(|_| {
				Error::new(
					ErrorKind::InvalidInput,
					"ADR 064 frame exceeds tiered offset index u32 length",
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

fn adr070_all_null_page_skip_rows(config: &BenchConfig) -> Vec<usize> {
	config.rows_override.clone().unwrap_or_else(|| {
		if config.iterations <= 25 {
			vec![8_192]
		} else {
			vec![8_192, 65_536, 1_000_000]
		}
	})
}

fn adr070_clear_page_stats(dataset: &mut Adr064RuntimeDataset) {
	for fixture in &mut dataset.fixtures {
		for column in &mut fixture.columns {
			if let Some(encoding) = column.encoding.as_mut() {
				encoding.page_stats.clear();
			}
		}
		if let Some(directory) = fixture.directory.encoding_directory.as_mut() {
			for encoding in &mut directory.buffers {
				encoding.page_stats.clear();
			}
		}
	}
}

fn bench_adr070_all_null_page_skip_matrix(config: &BenchConfig) -> std::io::Result<()> {
	let runtime = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()?;
	let mut emitted = 0_usize;
	let rows_set = adr070_all_null_page_skip_rows(config);
	for rows in rows_set.iter().copied() {
		let profile = if rows == ROW_GROUP_SCAN_TARGET_ROWS as usize {
			RowGroupSizingProfile::Scan
		} else {
			RowGroupSizingProfile::PointRead
		};
		let iterations = adr064_runtime_iterations(config, rows);
		for (fixture_name, shape, distribution, mask_shape, page_stats_enabled) in [
			(
				"clustered_all_null",
				Adr064ProjectionShape::OneNullable,
				Adr064NullDistribution::AllNullSelectedPage,
				SparseProjectionMaskShape::ClusteredFirstPage,
				true,
			),
			(
				"adr063_disabled_control",
				Adr064ProjectionShape::OneNullable,
				Adr064NullDistribution::AllNullSelectedPage,
				SparseProjectionMaskShape::ClusteredFirstPage,
				false,
			),
			(
				"clustered_5p_null",
				Adr064ProjectionShape::OneNullable,
				Adr064NullDistribution::ClusteredNulls,
				SparseProjectionMaskShape::ClusteredFivePercent,
				true,
			),
			(
				"all_valid_control",
				Adr064ProjectionShape::OneNullable,
				Adr064NullDistribution::NoNullRowGroup,
				SparseProjectionMaskShape::ClusteredFirstPage,
				true,
			),
			(
				"random_50p_no_elision_control",
				Adr064ProjectionShape::OneNullable,
				Adr064NullDistribution::RandomFiftyPercent,
				SparseProjectionMaskShape::RandomOnePercent,
				true,
			),
			(
				"alternating_pages_all_null",
				Adr064ProjectionShape::OneNullable,
				Adr064NullDistribution::AlternatingPagesAllNull,
				SparseProjectionMaskShape::ClusteredTwentyFivePercent,
				true,
			),
			(
				"two_nullable_column_divergent",
				Adr064ProjectionShape::FourNullable,
				Adr064NullDistribution::DivergentSelectedPage,
				SparseProjectionMaskShape::ClusteredFirstPage,
				true,
			),
		] {
			let mut dataset = adr064_nullable_runtime_dataset(
				rows,
				SparseProjectionCascadeFixture::BitPacked,
				shape,
				distribution,
				256,
				profile,
			)?;
			if !page_stats_enabled {
				adr070_clear_page_stats(&mut dataset);
			}
			let slab_id = SlabId::new(
				70_000
					+ rows as u64 + emitted as u64 * 1_000_000
					+ shape.projection_columns() as u64,
			);
			let crypto = seeded_dev_crypto_manager(TenantId::ZERO)?;
			let encrypted = adr064_runtime_encrypted_dataset(&dataset, &crypto, slab_id)?;
			let store = Arc::new(CountingBenchObjectStore::new(Arc::new(
				MockObjectStore::new(MockObjectStoreConfig::default()),
			)));
			let uri = runtime.block_on(store.put(
				&format!("adr070-all-null-page-skip/{fixture_name}/{rows}.wnd"),
				&encrypted.bytes,
			))?;
			let env = SparseProjectionRuntimeEnv {
				runtime: &runtime,
				store: store.clone(),
				uri: &uri,
				crypto: &crypto,
				slab_id,
				object_size: encrypted.bytes.len() as u64,
			};
			let (expected, expected_rows, expected_null_count) =
				adr064_dense_expected_for_dataset(&dataset, mask_shape)?;
			let mut last_baseline = None;
			let baseline_elapsed = bench(iterations, || {
				let (outcome, values) = adr064_sparse_dataset_runtime(
					black_box(&dataset),
					black_box(mask_shape),
					env.runtime,
					env.store.clone(),
					env.uri,
					env.crypto,
					env.slab_id,
					env.object_size,
				)
				.expect("ADR 070 baseline sparse runtime");
				last_baseline = Some((outcome, values));
			});
			let (mut baseline, baseline_values) = last_baseline.expect("ADR 070 baseline outcome");
			baseline.elapsed = baseline_elapsed;
			let mut last_elision = None;
			let elision_elapsed = bench(iterations, || {
				let (outcome, stats, values) = adr070_sparse_dataset_runtime(
					black_box(&dataset),
					black_box(mask_shape),
					env.runtime,
					env.store.clone(),
					env.uri,
					env.crypto,
					env.slab_id,
					env.object_size,
				)
				.expect("ADR 070 elision sparse runtime");
				last_elision = Some((outcome, stats, values));
			});
			let (mut elision, stats, elision_values) =
				last_elision.expect("ADR 070 elision outcome");
			elision.elapsed = elision_elapsed;
			let (determinism, _, determinism_values) = adr070_sparse_dataset_runtime(
				&dataset,
				mask_shape,
				env.runtime,
				env.store.clone(),
				env.uri,
				env.crypto,
				env.slab_id,
				env.object_size,
			)?;
			let equality = baseline_values == expected
				&& elision_values == expected
				&& baseline.output_rows == expected_rows
				&& elision.output_rows == expected_rows;
			let null_count_equal = baseline.null_count == expected_null_count
				&& elision.null_count == expected_null_count;
			if !equality || !null_count_equal {
				return Err(Error::new(
					ErrorKind::InvalidData,
					format!(
						"ADR 070 runtime mismatch fixture={fixture_name} rows={rows} equality={equality} null_count_equal={null_count_equal} baseline_rows={} elision_rows={} expected_rows={} baseline_nulls={} elision_nulls={} expected_nulls={}",
						baseline.output_rows,
						elision.output_rows,
						expected_rows,
						baseline.null_count,
						elision.null_count,
						expected_null_count,
					),
				));
			}
			let deterministic =
				determinism.digest == elision.digest && determinism_values == elision_values;
			let bytes_reduction = baseline
				.projection_plaintext_bytes
				.saturating_sub(elision.projection_plaintext_bytes);
			println!(
				"row_group_fetch_measure scenario=adr070_all_null_page_skip profile={} rows={} records={} page_values={} fixture={} projection_shape={} projected_cascade=bit_packed actual_cascade={} null_distribution={} mask_shape={} adr063_page_stats_enabled={} route={} output_rows={} equality={} null_count_equal={} null_count={} iterations={} baseline_total_ms={:.3} elision_total_ms={:.3} speedup_micros={} baseline_value_plaintext_bytes={} elision_value_plaintext_bytes={} baseline_validity_plaintext_bytes={} elision_validity_plaintext_bytes={} baseline_projection_plaintext_bytes={} elision_projection_plaintext_bytes={} selected_byte_reduction={} baseline_projection_ciphertext_bytes={} elision_projection_ciphertext_bytes={} baseline_selected_ranges={} elision_selected_ranges={} baseline_macro_block_get_count={} elision_macro_block_get_count={} baseline_get_range_bytes={} elision_get_range_bytes={} baseline_selected_value_pages={} elision_selected_value_pages={} baseline_selected_validity_ranges={} elision_selected_validity_ranges={} elided_pages={} synthesized_value_bytes={} synthesized_validity_bytes={} drift_sentinel_pages={} deterministic={} baseline_digest={} elision_digest={}",
				dataset.profile.as_str(),
				dataset.rows,
				dataset.fixtures.len(),
				dataset.page_values,
				fixture_name,
				dataset.shape.as_str(),
				sparse_projection_runtime_actual_cascade_for_adr064(&dataset),
				dataset.null_distribution.as_str(),
				mask_shape.as_str(),
				page_stats_enabled,
				if stats.elided_pages == 0 {
					"sparse_no_elision"
				} else {
					"sparse_all_null_elision"
				},
				elision.output_rows,
				equality,
				null_count_equal,
				elision.null_count,
				iterations,
				baseline.elapsed.as_secs_f64() * 1_000.0,
				elision.elapsed.as_secs_f64() * 1_000.0,
				sparse_projection_speedup_micros_adr064(&baseline, &elision),
				baseline.value_plaintext_bytes,
				elision.value_plaintext_bytes,
				baseline.validity_plaintext_bytes,
				elision.validity_plaintext_bytes,
				baseline.projection_plaintext_bytes,
				elision.projection_plaintext_bytes,
				bytes_reduction,
				baseline.projection_ciphertext_bytes,
				elision.projection_ciphertext_bytes,
				baseline.selected_ranges,
				elision.selected_ranges,
				baseline.get_range_calls,
				elision.get_range_calls,
				baseline.get_range_bytes,
				elision.get_range_bytes,
				baseline.selected_value_pages,
				elision.selected_value_pages,
				baseline.selected_validity_ranges,
				elision.selected_validity_ranges,
				stats.elided_pages,
				stats.synthesized_value_bytes,
				stats.synthesized_validity_bytes,
				stats.drift_sentinel_pages,
				deterministic,
				baseline.digest,
				elision.digest,
			);
			emitted = emitted.saturating_add(1);
		}
	}
	println!(
		"row_group_fetch_bench scenario=adr070_all_null_page_skip rows={} emitted_rows={} iterations={} all_null_elision_only=true",
		rows_set
			.iter()
			.map(|row| row.to_string())
			.collect::<Vec<_>>()
			.join(","),
		emitted,
		config.iterations,
	);
	Ok(())
}

fn bench_adr064_nullable_sparse_projection_matrix(config: &BenchConfig) -> std::io::Result<()> {
	let runtime = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()?;
	let mut emitted = 0_usize;
	for rows in adr064_nullable_sparse_projection_rows(config) {
		let profile = if *rows == ROW_GROUP_SCAN_TARGET_ROWS as usize {
			RowGroupSizingProfile::Scan
		} else {
			RowGroupSizingProfile::PointRead
		};
		let iterations = adr064_runtime_iterations(config, *rows);
		for cascade in [
			SparseProjectionCascadeFixture::BitPacked,
			SparseProjectionCascadeFixture::Dictionary,
		] {
			let dataset = adr064_nullable_runtime_dataset(
				*rows,
				cascade,
				Adr064ProjectionShape::OneNullable,
				Adr064NullDistribution::RandomFivePercent,
				256,
				profile,
			)?;
			let slab_id = SlabId::new(64_000 + *rows as u64 + (cascade as u64) * 1_000_000);
			let crypto = seeded_dev_crypto_manager(TenantId::ZERO)?;
			let encrypted = adr064_runtime_encrypted_dataset(&dataset, &crypto, slab_id)?;
			let store = Arc::new(CountingBenchObjectStore::new(Arc::new(
				MockObjectStore::new(MockObjectStoreConfig::default()),
			)));
			let uri = runtime.block_on(store.put(
				&format!("adr064-nullable/{}/{}.wnd", rows, cascade.as_str()),
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
			for shape in [
				SparseProjectionMaskShape::ClusteredFivePercent,
				SparseProjectionMaskShape::All,
				SparseProjectionMaskShape::None,
			] {
				let measurement = measure_adr064_runtime_case(
					&dataset,
					shape,
					Adr064PredicateSource::ExactMask,
					iterations,
					&env,
					true,
					false,
				)?;
				print_adr064_runtime_measure(
					&dataset,
					cascade,
					shape,
					Adr064PredicateSource::ExactMask,
					iterations,
					&measurement,
					"",
				);
				emitted = emitted.saturating_add(1);
			}
		}
	}

	let rows = 8_192;
	let iterations = adr064_runtime_iterations(config, rows);
	for (shape, distribution, page_values, predicate_source, mask_shape) in [
		(
			Adr064ProjectionShape::FourNullable,
			Adr064NullDistribution::Alternating,
			256,
			Adr064PredicateSource::ExactIsNotNull,
			SparseProjectionMaskShape::ClusteredFivePercent,
		),
		(
			Adr064ProjectionShape::MixedNullableNonNull,
			Adr064NullDistribution::ClusteredNulls,
			1025,
			Adr064PredicateSource::ExactIsNull,
			SparseProjectionMaskShape::ClusteredFivePercent,
		),
		(
			Adr064ProjectionShape::OneNullable,
			Adr064NullDistribution::AllNullSelectedPage,
			1025,
			Adr064PredicateSource::NullableValueComparison,
			SparseProjectionMaskShape::ClusteredFirstPage,
		),
		(
			Adr064ProjectionShape::OneNullable,
			Adr064NullDistribution::AllValidSelectedPages,
			256,
			Adr064PredicateSource::Adr063ExactMatch,
			SparseProjectionMaskShape::ClusteredFivePercent,
		),
		(
			Adr064ProjectionShape::OneNullable,
			Adr064NullDistribution::RandomFivePercent,
			256,
			Adr064PredicateSource::Adr063Maybe,
			SparseProjectionMaskShape::ClusteredFivePercent,
		),
		(
			Adr064ProjectionShape::NullableVariableControl,
			Adr064NullDistribution::RandomFivePercent,
			256,
			Adr064PredicateSource::ExactMask,
			SparseProjectionMaskShape::ClusteredFivePercent,
		),
		(
			Adr064ProjectionShape::OneNullable,
			Adr064NullDistribution::NoNullRowGroup,
			256,
			Adr064PredicateSource::ExactMask,
			SparseProjectionMaskShape::ClusteredFivePercent,
		),
	] {
		let dataset = adr064_nullable_runtime_dataset(
			rows,
			SparseProjectionCascadeFixture::BitPacked,
			shape,
			distribution,
			page_values,
			RowGroupSizingProfile::PointRead,
		)?;
		let slab_id = SlabId::new(
			65_000 + rows as u64 + page_values as u64 + shape.projection_columns() as u64,
		);
		let crypto = seeded_dev_crypto_manager(TenantId::ZERO)?;
		let encrypted = adr064_runtime_encrypted_dataset(&dataset, &crypto, slab_id)?;
		let store = Arc::new(CountingBenchObjectStore::new(Arc::new(
			MockObjectStore::new(MockObjectStoreConfig::default()),
		)));
		let uri = runtime.block_on(store.put(
			&format!(
				"adr064-nullable/control/{}_{}_{}.wnd",
				shape.as_str(),
				distribution.as_str(),
				page_values
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
		let encoded_validity_control = false;
		let measurement = measure_adr064_runtime_case(
			&dataset,
			mask_shape,
			predicate_source,
			iterations,
			&env,
			true,
			encoded_validity_control,
		)?;
		let extra = if matches!(
			predicate_source,
			Adr064PredicateSource::Adr063ExactMatch | Adr064PredicateSource::Adr063Maybe
		) {
			let dense_predicate_bytes = 1_145_u64;
			let narrowed_predicate_bytes: u64 =
				if matches!(predicate_source, Adr064PredicateSource::Adr063ExactMatch) {
					0
				} else {
					144
				};
			let adr063_only = narrowed_predicate_bytes
				.saturating_add(measurement.dense.projection_plaintext_bytes);
			let adr064_only =
				dense_predicate_bytes.saturating_add(measurement.sparse.projection_plaintext_bytes);
			let combined = narrowed_predicate_bytes
				.saturating_add(measurement.sparse.projection_plaintext_bytes);
			format!(
				" dense_predicate_plaintext_bytes={} narrowed_predicate_plaintext_bytes={} adr063_only_plaintext_bytes={} adr064_only_plaintext_bytes={} combined_plaintext_bytes={} combined_strictly_better={}",
				dense_predicate_bytes,
				narrowed_predicate_bytes,
				adr063_only,
				adr064_only,
				combined,
				combined < adr063_only && combined < adr064_only,
			)
		} else {
			String::new()
		};
		print_adr064_runtime_measure(
			&dataset,
			SparseProjectionCascadeFixture::BitPacked,
			mask_shape,
			predicate_source,
			iterations,
			&measurement,
			&extra,
		);
		emitted = emitted.saturating_add(1);
	}

	let encoded_validity_dataset = adr064_nullable_runtime_dataset(
		rows,
		SparseProjectionCascadeFixture::BitPacked,
		Adr064ProjectionShape::OneNullable,
		Adr064NullDistribution::RandomFivePercent,
		256,
		RowGroupSizingProfile::PointRead,
	)?;
	let slab_id = SlabId::new(66_000 + rows as u64);
	let crypto = seeded_dev_crypto_manager(TenantId::ZERO)?;
	let encrypted = adr064_runtime_encrypted_dataset(&encoded_validity_dataset, &crypto, slab_id)?;
	let store = Arc::new(CountingBenchObjectStore::new(Arc::new(
		MockObjectStore::new(MockObjectStoreConfig::default()),
	)));
	let uri = runtime.block_on(store.put(
		&format!("adr064-nullable/encoded-validity/{}.wnd", rows),
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
	let encoded_validity_measurement = measure_adr064_runtime_case(
		&encoded_validity_dataset,
		SparseProjectionMaskShape::ClusteredFivePercent,
		Adr064PredicateSource::ExactMask,
		iterations,
		&env,
		true,
		true,
	)?;
	print_adr064_runtime_measure(
		&encoded_validity_dataset,
		SparseProjectionCascadeFixture::BitPacked,
		SparseProjectionMaskShape::ClusteredFivePercent,
		Adr064PredicateSource::ExactMask,
		iterations,
		&encoded_validity_measurement,
		" decline=encoded_validity_buffer_unsupported",
	);
	emitted = emitted.saturating_add(1);

	let left = adr064_nullable_runtime_dataset(
		rows,
		SparseProjectionCascadeFixture::BitPacked,
		Adr064ProjectionShape::OneNullable,
		Adr064NullDistribution::RandomFivePercent,
		256,
		RowGroupSizingProfile::PointRead,
	)?;
	let right = adr064_nullable_runtime_dataset(
		rows,
		SparseProjectionCascadeFixture::BitPacked,
		Adr064ProjectionShape::OneNullable,
		Adr064NullDistribution::RandomFivePercent,
		256,
		RowGroupSizingProfile::PointRead,
	)?;
	let left_crypto = seeded_dev_crypto_manager(TenantId::ZERO)?;
	let right_crypto = seeded_dev_crypto_manager(TenantId::ZERO)?;
	let left_slab = SlabId::new(67_000 + rows as u64);
	let right_slab = SlabId::new(67_100 + rows as u64);
	let left_encrypted = adr064_runtime_encrypted_dataset(&left, &left_crypto, left_slab)?;
	let right_encrypted = adr064_runtime_encrypted_dataset(&right, &right_crypto, right_slab)?;
	let left_store = Arc::new(CountingBenchObjectStore::new(Arc::new(
		MockObjectStore::new(MockObjectStoreConfig::default()),
	)));
	let right_store = Arc::new(CountingBenchObjectStore::new(Arc::new(
		MockObjectStore::new(MockObjectStoreConfig::default()),
	)));
	let left_uri = runtime.block_on(left_store.put(
		&format!("adr064-nullable/determinism/left_{}.wnd", rows),
		&left_encrypted.bytes,
	))?;
	let right_uri = runtime.block_on(right_store.put(
		&format!("adr064-nullable/determinism/right_{}.wnd", rows),
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
	let left_measure = measure_adr064_runtime_case(
		&left,
		SparseProjectionMaskShape::ClusteredFivePercent,
		Adr064PredicateSource::ExactMask,
		1,
		&left_env,
		true,
		false,
	)?;
	let right_measure = measure_adr064_runtime_case(
		&right,
		SparseProjectionMaskShape::ClusteredFivePercent,
		Adr064PredicateSource::ExactMask,
		1,
		&right_env,
		true,
		false,
	)?;
	let deterministic = left_measure.route == right_measure.route
		&& left_measure.sparse.output_rows == right_measure.sparse.output_rows
		&& left_measure.sparse.null_count == right_measure.sparse.null_count
		&& left_measure.sparse.projection_plaintext_bytes
			== right_measure.sparse.projection_plaintext_bytes
		&& left_measure.sparse.selected_ranges == right_measure.sparse.selected_ranges
		&& left_measure.sparse.selected_value_pages == right_measure.sparse.selected_value_pages
		&& left_measure.sparse.selected_validity_ranges
			== right_measure.sparse.selected_validity_ranges
		&& left_measure.sparse.digest == right_measure.sparse.digest;
	println!(
		"row_group_fetch_measure scenario=adr064_nullable_sparse_projection_determinism profile=point_read rows={} records={} page_values=256 projection_shape={} projected_cascade=bit_packed actual_cascade={} null_distribution={} mask_shape=clustered_5p predicate_source=exact_mask route={} equality={} null_count_equal={} deterministic={} left_output_digest={} right_output_digest={} sparse_projection_plaintext_bytes={} selected_value_pages={} selected_validity_ranges={}",
		left.rows,
		left.fixtures.len(),
		left.shape.as_str(),
		sparse_projection_runtime_actual_cascade_for_adr064(&left),
		left.null_distribution.as_str(),
		left_measure.route,
		left_measure.equality && right_measure.equality,
		left_measure.null_count_equal && right_measure.null_count_equal,
		deterministic,
		left_measure.sparse.digest,
		right_measure.sparse.digest,
		left_measure.sparse.projection_plaintext_bytes,
		left_measure.sparse.selected_value_pages,
		left_measure.sparse.selected_validity_ranges,
	);
	if !deterministic {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"ADR 064 nullable sparse projection determinism mismatch",
		));
	}
	emitted = emitted.saturating_add(1);

	println!(
		"row_group_fetch_bench scenario=adr064_nullable_sparse_projection rows=mixed emitted_rows={} iterations=max10_1m3",
		emitted,
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
				literal: Some(ColumnPredicateLiteral::Int(1)),
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
					literal: Some(ColumnPredicateLiteral::Int(1)),
				},
			),
			(
				Adr063Distribution::AllExact,
				PredicateAtom {
					op: PredicateAtomOp::Eq,
					literal: Some(ColumnPredicateLiteral::Int(1)),
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
	let iterations = config.iterations.clamp(1, 10);
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
		EncodingCascade::FsstWholeBuffer { .. } => "fsst_whole_buffer",
		EncodingCascade::Alp { .. } => "alp",
		EncodingCascade::FsstRowSafe { .. } => "fsst_row_safe",
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
