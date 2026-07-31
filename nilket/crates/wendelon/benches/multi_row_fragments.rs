use std::hint::black_box;
use std::io::{Error, ErrorKind};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arrow::array::{ArrayRef, Int64Array, UInt64Array};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use wendelon::schema::pbc::{
	OP_GTE, OP_PROJECT, OP_PUSH_COL, OP_PUSH_INT, Program, execute_program,
};
use wendelon::schema::{
	AGG_FORMAT_VERSION, AggregateBudgets, AggregateExpression, AggregateFunctionTag,
	AggregateRequest, AggregateRequestId, Consistency, DurabilityLevel, HybridTimestamp,
	LocalityPolicy, LocalityRange, MultiRowLayoutDescriptor, PrimaryKeyDescriptor,
	RecordBodyCompressionPolicy, RecordLayoutPolicy, RowIdentityPolicy, SchemaEvolutionPolicy,
	TableObjectId, TableSchemaDescriptor, TableSpreadLocalityDescriptor, TableWriteRequestMeta,
	TenantId, WriterNodeId, encode_table_write_row_count,
};
use wendelon::storage::{
	LocalFsObjectStore, ObjectStore, aggregate_record_batches, decode_batch, encode_batch,
	encode_batch_with_policy, has_storage_synthetic_columns, pack_record_batch_by_bucket,
	strip_storage_synthetic_columns, take_rows,
};

const DEFAULT_ROWS: usize = 65_536;
const WRITE_PREP_ITERS: usize = 1;
const FULL_SCAN_ITERS: usize = 2;
const POINT_READ_ITERS: usize = 256;
const REWRITE_ITERS: usize = 4;

#[derive(Clone, Debug)]
struct BenchConfig {
	rows: usize,
}

#[derive(Clone)]
struct PreparedFrames {
	frames: Vec<Vec<u8>>,
	logical_rows: usize,
}

impl PreparedFrames {
	fn physical_records(&self) -> usize {
		self.frames.len()
	}

	fn frame_bytes(&self) -> usize {
		self.frames.iter().map(Vec::len).sum()
	}
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
	let config = BenchConfig::parse()?;
	let batch = bench_batch(config.rows)?;
	let single_descriptor = bench_descriptor(RecordLayoutPolicy::SingleRow)?;
	let default_multi_layout = MultiRowLayoutDescriptor::default();
	let multi_descriptor = bench_descriptor(RecordLayoutPolicy::MultiRow(default_multi_layout))?;
	let multi_uncompressed_descriptor = bench_descriptor_with_policy(
		RecordLayoutPolicy::MultiRow(default_multi_layout),
		RecordBodyCompressionPolicy::Uncompressed,
	)?;
	let meta = table_meta(&multi_descriptor, batch.num_rows())?;
	let uncompressed_meta = table_meta(&multi_uncompressed_descriptor, batch.num_rows())?;

	let single_write = bench_write_prep("single-row", WRITE_PREP_ITERS, || {
		build_single_row_frames(&batch)
	})?;
	let multi_uncompressed_write =
		bench_write_prep("multi-row-uncompressed", WRITE_PREP_ITERS, || {
			build_multirow_frames(&batch, &uncompressed_meta, &multi_uncompressed_descriptor)
		})?;
	let multi_write = bench_write_prep("multi-row-default", WRITE_PREP_ITERS, || {
		build_multirow_frames(&batch, &meta, &multi_descriptor)
	})?;
	print_compression_ratio("multi-row-default", &multi_uncompressed_write, &multi_write);
	let reduction = single_write.physical_records() as f64 / multi_write.physical_records() as f64;
	println!(
		"multi_row_fragments case=record-reduction rows={} single_records={} multi_records={} reduction={reduction:.2}",
		config.rows,
		single_write.physical_records(),
		multi_write.physical_records(),
	);
	if reduction < 10.0 {
		return Err(format!("multi-row record reduction below target: {reduction:.2}x").into());
	}

	bench_full_scan_pbc("single-row", &single_write)?;
	bench_full_scan_pbc("multi-row-uncompressed", &multi_uncompressed_write)?;
	bench_full_scan_pbc("multi-row-default", &multi_write)?;
	bench_aggregate("single-row", &single_descriptor, &single_write)?;
	bench_aggregate(
		"multi-row-uncompressed",
		&multi_uncompressed_descriptor,
		&multi_uncompressed_write,
	)?;
	bench_aggregate("multi-row-default", &multi_descriptor, &multi_write)?;
	bench_entropy_fixtures(config.rows.min(16_384), default_multi_layout)?;
	bench_l2_cold_read("multi-row-uncompressed", &multi_uncompressed_write)?;
	bench_l2_cold_read("multi-row-default", &multi_write)?;

	for max_record_bytes in [256 * 1024_u64, 1024 * 1024, 8 * 1024 * 1024] {
		let layout = MultiRowLayoutDescriptor::new(65_536, 65_536, max_record_bytes)?;
		let descriptor = bench_descriptor(RecordLayoutPolicy::MultiRow(layout))?;
		let frames = build_multirow_frames(&batch, &meta, &descriptor)?;
		bench_point_read(max_record_bytes, &frames)?;
	}

	for keep_percent in [10_u32, 50, 90] {
		bench_retention_rewrite_with_policy(
			keep_percent,
			&multi_write,
			multi_descriptor.record_body_compression_policy,
		)?;
	}
	bench_presorted_recompact(
		default_multi_layout,
		&multi_write,
		multi_descriptor.record_body_compression_policy,
	)?;

	Ok(())
}

impl BenchConfig {
	fn parse() -> Result<Self, Box<dyn std::error::Error>> {
		let mut rows = DEFAULT_ROWS;
		let mut args = std::env::args().skip(1);
		while let Some(arg) = args.next() {
			match arg.as_str() {
				"--rows" => {
					let value = args.next().ok_or("--rows requires a value")?;
					rows = value.parse()?;
				}
				"--quick" => rows = 8_192,
				"--bench" => {}
				other => return Err(format!("unknown argument: {other}").into()),
			}
		}
		Ok(Self { rows })
	}
}

fn bench_write_prep<F>(
	label: &'static str,
	iterations: usize,
	mut build: F,
) -> Result<PreparedFrames, Box<dyn std::error::Error>>
where
	F: FnMut() -> std::io::Result<PreparedFrames>,
{
	let mut elapsed = Duration::ZERO;
	let mut latest = None;
	for _ in 0..iterations {
		let started = Instant::now();
		let frames = build()?;
		elapsed += started.elapsed();
		black_box(frames.frame_bytes());
		latest = Some(frames);
	}
	let frames = latest.expect("at least one iteration");
	println!(
		"multi_row_fragments case=write-prep layout={label} rows={} physical_records={} frame_bytes={} iterations={} avg_ns={}",
		frames.logical_rows,
		frames.physical_records(),
		frames.frame_bytes(),
		iterations,
		avg_ns(elapsed, iterations),
	);
	Ok(frames)
}

fn print_compression_ratio(
	label: &'static str,
	baseline: &PreparedFrames,
	encoded: &PreparedFrames,
) {
	let ratio = encoded.frame_bytes() as f64 / baseline.frame_bytes().max(1) as f64;
	println!(
		"multi_row_fragments case=compression-ratio layout={label} rows={} uncompressed_bytes={} encoded_bytes={} ratio={ratio:.3}",
		encoded.logical_rows,
		baseline.frame_bytes(),
		encoded.frame_bytes(),
	);
}

fn bench_full_scan_pbc(label: &'static str, frames: &PreparedFrames) -> std::io::Result<()> {
	let program = Program::compile(&assemble(&[
		Instr::PushCol(1),
		Instr::PushInt(-45_000),
		Instr::Raw(OP_GTE),
		Instr::Project(&[0, 1]),
	]))?;
	let mut elapsed = Duration::ZERO;
	let mut output_rows = 0;
	for _ in 0..FULL_SCAN_ITERS {
		let started = Instant::now();
		let batch = decode_user_batches(frames)?;
		let filtered = execute_program(black_box(&program), black_box(&batch))?;
		output_rows = filtered.num_rows();
		elapsed += started.elapsed();
		black_box(output_rows);
	}
	println!(
		"multi_row_fragments case=full-scan-pbc layout={label} rows={} physical_records={} output_rows={} iterations={} avg_ns={}",
		frames.logical_rows,
		frames.physical_records(),
		output_rows,
		FULL_SCAN_ITERS,
		avg_ns(elapsed, FULL_SCAN_ITERS),
	);
	Ok(())
}

fn bench_aggregate(
	label: &'static str,
	descriptor: &TableSchemaDescriptor,
	frames: &PreparedFrames,
) -> std::io::Result<()> {
	let request = AggregateRequest {
		format_version: AGG_FORMAT_VERSION,
		request_id: AggregateRequestId::new(42),
		tenant_id: tenant(),
		table_object_id: descriptor.table_object_id,
		schema_generation: descriptor.schema_generation,
		logical_schema_fingerprint: descriptor.logical_schema_fingerprint,
		consistency: Consistency::SERIALIZABLE,
		min_hlc: hlc(1),
		as_of_hlc: hlc(2),
		locality_ranges: vec![LocalityRange::full()],
		predicate_program: None,
		aggregates: vec![
			AggregateExpression::count_star(),
			AggregateExpression {
				tag: AggregateFunctionTag::MIN,
				column_ordinal: 1,
				parameter_block: Vec::new(),
			},
			AggregateExpression {
				tag: AggregateFunctionTag::MAX,
				column_ordinal: 1,
				parameter_block: Vec::new(),
			},
		],
		budgets: AggregateBudgets::default(),
	};
	let mut elapsed = Duration::ZERO;
	let mut partials = 0;
	for _ in 0..FULL_SCAN_ITERS {
		let started = Instant::now();
		let batch = decode_user_batches(frames)?;
		let result = aggregate_record_batches(&request, &[batch], false)
			.map_err(|err| Error::new(ErrorKind::InvalidData, format!("aggregate: {err:?}")))?;
		partials = result.response.partials.len();
		elapsed += started.elapsed();
		black_box(partials);
	}
	println!(
		"multi_row_fragments case=aggregate-count-min-max layout={label} rows={} physical_records={} partials={} iterations={} avg_ns={}",
		frames.logical_rows,
		frames.physical_records(),
		partials,
		FULL_SCAN_ITERS,
		avg_ns(elapsed, FULL_SCAN_ITERS),
	);
	Ok(())
}

fn bench_point_read(max_record_bytes: u64, frames: &PreparedFrames) -> std::io::Result<()> {
	let (frame, row_index, record_rows) = widest_frame(frames)?;
	let mut elapsed = Duration::ZERO;
	let mut materialized_rows = 0;
	for _ in 0..POINT_READ_ITERS {
		let started = Instant::now();
		let batch = materialize_point_row(black_box(frame), black_box(row_index))?;
		elapsed += started.elapsed();
		materialized_rows = batch.num_rows();
		black_box(materialized_rows);
	}
	println!(
		"multi_row_fragments case=point-read max_record_bytes={max_record_bytes} rows={} physical_records={} record_rows={} amplified_frame_bytes={} materialized_rows={} iterations={} avg_ns={}",
		frames.logical_rows,
		frames.physical_records(),
		record_rows,
		frame.len(),
		materialized_rows,
		POINT_READ_ITERS,
		avg_ns(elapsed, POINT_READ_ITERS),
	);
	Ok(())
}

fn bench_retention_rewrite_with_policy(
	keep_percent: u32,
	frames: &PreparedFrames,
	policy: RecordBodyCompressionPolicy,
) -> std::io::Result<()> {
	let mut elapsed = Duration::ZERO;
	let mut output_records = 0;
	let mut output_rows = 0;
	let mut output_bytes = 0;
	for _ in 0..REWRITE_ITERS {
		let started = Instant::now();
		let rewritten = rewrite_survivors(frames, keep_percent, policy)?;
		elapsed += started.elapsed();
		output_records = rewritten.physical_records();
		output_rows = rewritten.logical_rows;
		output_bytes = rewritten.frame_bytes();
		black_box(output_bytes);
	}
	println!(
		"multi_row_fragments case=retention-rewrite keep_percent={keep_percent} input_rows={} output_rows={} output_records={} output_bytes={} iterations={} avg_ns={}",
		frames.logical_rows,
		output_rows,
		output_records,
		output_bytes,
		REWRITE_ITERS,
		avg_ns(elapsed, REWRITE_ITERS),
	);
	Ok(())
}

fn bench_presorted_recompact(
	layout: MultiRowLayoutDescriptor,
	frames: &PreparedFrames,
	policy: RecordBodyCompressionPolicy,
) -> std::io::Result<()> {
	let mut elapsed = Duration::ZERO;
	let mut output_records = 0;
	let mut output_bytes = 0;
	for _ in 0..REWRITE_ITERS {
		let started = Instant::now();
		let compacted = recompact_presorted_storage_batches(frames, layout, policy)?;
		elapsed += started.elapsed();
		output_records = compacted.physical_records();
		output_bytes = compacted.frame_bytes();
		black_box(output_bytes);
	}
	println!(
		"multi_row_fragments case=presorted-recompact rows={} output_records={} output_bytes={} iterations={} avg_ns={}",
		frames.logical_rows,
		output_records,
		output_bytes,
		REWRITE_ITERS,
		avg_ns(elapsed, REWRITE_ITERS),
	);
	Ok(())
}

fn bench_l2_cold_read(
	label: &'static str,
	frames: &PreparedFrames,
) -> Result<(), Box<dyn std::error::Error>> {
	let runtime = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()?;
	let root = std::env::temp_dir().join(format!(
		"wendelon-l2-bench-{}-{}",
		std::process::id(),
		SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
	));
	std::fs::create_dir_all(&root)?;
	let store: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&root)?);
	let uris = runtime.block_on(async {
		let mut uris = Vec::with_capacity(frames.physical_records());
		for (index, frame) in frames.frames.iter().enumerate() {
			let uri = store
				.put(&format!("{label}/record_{index:08}.ipc"), frame)
				.await?;
			uris.push(uri);
		}
		Ok::<Vec<String>, std::io::Error>(uris)
	})?;
	let lengths = frames
		.frames
		.iter()
		.map(|frame| frame.len() as u64)
		.collect::<Vec<_>>();

	let mut elapsed = Duration::ZERO;
	let mut fetched_record_body_bytes = 0;
	let mut decoded_rows = 0;
	for _ in 0..FULL_SCAN_ITERS {
		let started = Instant::now();
		fetched_record_body_bytes = 0;
		decoded_rows = 0;
		for (uri, length) in uris.iter().zip(lengths.iter().copied()) {
			let bytes = runtime.block_on(store.get_range(uri, 0..length))?;
			fetched_record_body_bytes += bytes.len();
			let decoded = decode_batch(bytes.as_ref())?;
			let user_batch = strip_storage_synthetic_columns(&decoded)?;
			decoded_rows += user_batch.num_rows();
		}
		elapsed += started.elapsed();
		black_box((fetched_record_body_bytes, decoded_rows));
	}
	println!(
		"multi_row_fragments case=l2-cold-read layout={label} rows={} physical_records={} fetched_record_body_bytes={} decoded_rows={} iterations={} avg_ns={}",
		frames.logical_rows,
		frames.physical_records(),
		fetched_record_body_bytes,
		decoded_rows,
		FULL_SCAN_ITERS,
		avg_ns(elapsed, FULL_SCAN_ITERS),
	);

	for uri in uris {
		runtime.block_on(store.delete(&uri))?;
	}
	let _ = std::fs::remove_dir_all(root);
	Ok(())
}

fn build_single_row_frames(batch: &RecordBatch) -> std::io::Result<PreparedFrames> {
	let mut frames = Vec::with_capacity(batch.num_rows());
	for row in 0..batch.num_rows() {
		frames.push(encode_batch(&batch.slice(row, 1))?);
	}
	Ok(PreparedFrames {
		frames,
		logical_rows: batch.num_rows(),
	})
}

fn build_multirow_frames(
	batch: &RecordBatch,
	meta: &TableWriteRequestMeta,
	descriptor: &TableSchemaDescriptor,
) -> std::io::Result<PreparedFrames> {
	let records =
		pack_record_batch_by_bucket(meta, descriptor, batch, WriterNodeId::ZERO, meta.client_hlc)
			.map_err(|err| Error::new(ErrorKind::InvalidData, err))?;
	let frames = records
		.into_iter()
		.map(|record| record.frame_bytes)
		.collect::<Vec<_>>();
	Ok(PreparedFrames {
		frames,
		logical_rows: batch.num_rows(),
	})
}

fn rewrite_survivors(
	frames: &PreparedFrames,
	keep_percent: u32,
	policy: RecordBodyCompressionPolicy,
) -> std::io::Result<PreparedFrames> {
	let mut rewritten = Vec::new();
	let mut logical_rows = 0;
	for frame in &frames.frames {
		let batch = decode_batch(frame)?;
		let keep = (0..batch.num_rows())
			.filter(|row| ((*row as u32).wrapping_mul(37).wrapping_add(11)) % 100 < keep_percent)
			.collect::<Vec<_>>();
		if keep.is_empty() {
			continue;
		}
		let survivor = take_rows(&batch, &keep)?;
		logical_rows += survivor.num_rows();
		rewritten.push(encode_batch_with_policy(&survivor, policy)?.bytes);
	}
	Ok(PreparedFrames {
		frames: rewritten,
		logical_rows,
	})
}

fn recompact_presorted_storage_batches(
	frames: &PreparedFrames,
	layout: MultiRowLayoutDescriptor,
	policy: RecordBodyCompressionPolicy,
) -> std::io::Result<PreparedFrames> {
	let mut batches = Vec::with_capacity(frames.physical_records());
	for frame in &frames.frames {
		batches.push(decode_batch(frame)?);
	}
	let schema = batches
		.first()
		.ok_or_else(|| Error::new(ErrorKind::InvalidInput, "no frames to recompact"))?
		.schema();
	let merged = concat_batches(&schema, &batches)
		.map_err(|err| Error::new(ErrorKind::InvalidData, format!("concat: {err}")))?;
	let mut output = Vec::new();
	let mut cursor = 0;
	while cursor < merged.num_rows() {
		let take = (merged.num_rows() - cursor)
			.min(layout.target_rows.get() as usize)
			.min(layout.max_rows.get() as usize);
		let batch = merged.slice(cursor, take);
		output.push(encode_batch_with_policy(&batch, policy)?.bytes);
		cursor += take;
	}
	Ok(PreparedFrames {
		frames: output,
		logical_rows: merged.num_rows(),
	})
}

fn decode_user_batches(frames: &PreparedFrames) -> std::io::Result<RecordBatch> {
	let mut batches = Vec::with_capacity(frames.physical_records());
	for frame in &frames.frames {
		let decoded = decode_batch(frame)?;
		batches.push(strip_storage_synthetic_columns(&decoded)?);
	}
	let schema = batches
		.first()
		.ok_or_else(|| Error::new(ErrorKind::InvalidInput, "no frames to decode"))?
		.schema();
	concat_batches(&schema, &batches)
		.map_err(|err| Error::new(ErrorKind::InvalidData, format!("concat: {err}")))
}

fn materialize_point_row(frame: &[u8], row_index: usize) -> std::io::Result<RecordBatch> {
	let decoded = decode_batch(frame)?;
	if !has_storage_synthetic_columns(&decoded) {
		return Ok(decoded);
	}
	if row_index >= decoded.num_rows() {
		return Err(Error::new(
			ErrorKind::InvalidInput,
			"point row index out of bounds",
		));
	}
	strip_storage_synthetic_columns(&decoded.slice(row_index, 1))
}

fn widest_frame(frames: &PreparedFrames) -> std::io::Result<(&[u8], usize, usize)> {
	let mut best = None;
	for frame in &frames.frames {
		let rows = decode_batch(frame)?.num_rows();
		if best
			.as_ref()
			.map(|(_, _, best_rows)| rows > *best_rows)
			.unwrap_or(true)
		{
			best = Some((frame.as_slice(), rows / 2, rows));
		}
	}
	best.ok_or_else(|| Error::new(ErrorKind::InvalidInput, "no frames"))
}

fn bench_descriptor(
	record_layout_policy: RecordLayoutPolicy,
) -> std::io::Result<TableSchemaDescriptor> {
	let compression_policy = RecordBodyCompressionPolicy::default_for_layout(&record_layout_policy);
	bench_descriptor_with_policy(record_layout_policy, compression_policy)
}

fn bench_descriptor_with_policy(
	record_layout_policy: RecordLayoutPolicy,
	compression_policy: RecordBodyCompressionPolicy,
) -> std::io::Result<TableSchemaDescriptor> {
	let mut descriptor = TableSchemaDescriptor::new(
		TableObjectId::new([0x42; 16]),
		1,
		"analytics",
		"events",
		&bench_schema(),
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
		LocalityPolicy::TableSpreadProjection(TableSpreadLocalityDescriptor {
			spread_factor: 1,
			prefix_bits: 63,
		}),
		SchemaEvolutionPolicy::AdditiveOnly,
	)?;
	descriptor.record_layout_policy = record_layout_policy;
	descriptor.record_body_compression_policy = compression_policy;
	descriptor.refresh_descriptor_fingerprints()?;
	descriptor.validate()?;
	Ok(descriptor)
}

fn bench_schema() -> Schema {
	let mut fields = vec![
		Field::new("id", DataType::Int64, false),
		Field::new("metric", DataType::Int64, false),
	];
	for ordinal in 0..8 {
		fields.push(Field::new(
			format!("payload_{ordinal}"),
			DataType::UInt64,
			false,
		));
	}
	Schema::new(fields)
}

fn bench_batch(rows: usize) -> std::io::Result<RecordBatch> {
	bench_batch_with_entropy(rows, EntropyFixture::Medium)
}

#[derive(Clone, Copy)]
enum EntropyFixture {
	Low,
	Medium,
	High,
}

impl EntropyFixture {
	const fn label(self) -> &'static str {
		match self {
			Self::Low => "low",
			Self::Medium => "medium",
			Self::High => "high",
		}
	}
}

fn bench_entropy_fixtures(
	rows: usize,
	layout: MultiRowLayoutDescriptor,
) -> Result<(), Box<dyn std::error::Error>> {
	for fixture in [
		EntropyFixture::Low,
		EntropyFixture::Medium,
		EntropyFixture::High,
	] {
		let batch = bench_batch_with_entropy(rows, fixture)?;
		let uncompressed_descriptor = bench_descriptor_with_policy(
			RecordLayoutPolicy::MultiRow(layout),
			RecordBodyCompressionPolicy::Uncompressed,
		)?;
		let lz4_descriptor = bench_descriptor_with_policy(
			RecordLayoutPolicy::MultiRow(layout),
			RecordBodyCompressionPolicy::Lz4Frame,
		)?;
		let uncompressed_meta = table_meta(&uncompressed_descriptor, batch.num_rows())?;
		let lz4_meta = table_meta(&lz4_descriptor, batch.num_rows())?;
		let uncompressed =
			build_multirow_frames(&batch, &uncompressed_meta, &uncompressed_descriptor)?;
		let lz4 = build_multirow_frames(&batch, &lz4_meta, &lz4_descriptor)?;
		let ratio = lz4.frame_bytes() as f64 / uncompressed.frame_bytes().max(1) as f64;
		println!(
			"multi_row_fragments case=compression-entropy entropy={} rows={} uncompressed_bytes={} encoded_bytes={} ratio={ratio:.3}",
			fixture.label(),
			rows,
			uncompressed.frame_bytes(),
			lz4.frame_bytes(),
		);
	}
	Ok(())
}

fn bench_batch_with_entropy(rows: usize, fixture: EntropyFixture) -> std::io::Result<RecordBatch> {
	let mut columns: Vec<ArrayRef> = Vec::with_capacity(10);
	columns.push(Arc::new(Int64Array::from(
		(0..rows).map(|idx| idx as i64).collect::<Vec<_>>(),
	)) as ArrayRef);
	columns.push(Arc::new(Int64Array::from(
		(0..rows)
			.map(|idx| (idx as i64 % 100_003) - 50_000)
			.collect::<Vec<_>>(),
	)) as ArrayRef);
	for ordinal in 0..8_u64 {
		columns.push(Arc::new(UInt64Array::from(
			(0..rows)
				.map(|idx| payload_value(fixture, idx as u64, ordinal))
				.collect::<Vec<_>>(),
		)) as ArrayRef);
	}
	RecordBatch::try_new(Arc::new(bench_schema()), columns)
		.map_err(|err| Error::new(ErrorKind::InvalidData, format!("bench batch: {err}")))
}

fn payload_value(fixture: EntropyFixture, row: u64, ordinal: u64) -> u64 {
	match fixture {
		EntropyFixture::Low => ordinal,
		EntropyFixture::Medium => row.wrapping_mul(31).wrapping_add(ordinal),
		EntropyFixture::High => splitmix64(row ^ ordinal.wrapping_mul(0x9E37_79B9_7F4A_7C15)),
	}
}

fn splitmix64(mut value: u64) -> u64 {
	value = value.wrapping_add(0x9E37_79B9_7F4A_7C15);
	value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
	value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
	value ^ (value >> 31)
}

fn table_meta(
	descriptor: &TableSchemaDescriptor,
	row_count: usize,
) -> std::io::Result<TableWriteRequestMeta> {
	let mut meta: TableWriteRequestMeta = bytemuck::Zeroable::zeroed();
	meta.client_hlc = hlc(1);
	meta.expected_epoch = 1;
	meta.tenant_id = tenant();
	meta.table_object_id = descriptor.table_object_id;
	meta.schema_generation = descriptor.schema_generation;
	meta.row_count = encode_table_write_row_count(row_count)?;
	meta.durability = DurabilityLevel::QUORUM;
	Ok(meta)
}

fn hlc(ms: u64) -> HybridTimestamp {
	HybridTimestamp {
		physical_ms: ms,
		logical: 0,
		node_id: 1,
	}
}

fn tenant() -> TenantId {
	TenantId::ZERO
}

fn avg_ns(duration: Duration, iterations: usize) -> u128 {
	duration.as_nanos() / iterations.max(1) as u128
}

enum Instr<'a> {
	Raw(u8),
	PushCol(u32),
	PushInt(i64),
	Project(&'a [u32]),
}

fn assemble(instrs: &[Instr<'_>]) -> Vec<u8> {
	let mut out = Vec::new();
	for instr in instrs {
		match instr {
			Instr::Raw(op) => out.push(*op),
			Instr::PushCol(col) => {
				out.push(OP_PUSH_COL);
				out.extend_from_slice(&col.to_le_bytes());
			}
			Instr::PushInt(value) => {
				out.push(OP_PUSH_INT);
				out.extend_from_slice(&value.to_le_bytes());
			}
			Instr::Project(cols) => {
				out.push(OP_PROJECT);
				out.extend_from_slice(&(cols.len() as u16).to_le_bytes());
				for col in *cols {
					out.extend_from_slice(&col.to_le_bytes());
				}
			}
		}
	}
	out
}
