//! ADR 047 row-tombstone cold-path benchmark.
//!
//! The full storage/query path is covered by targeted tests. This bench
//! isolates the steady-state costs ADR 047 calls out: latest-as-of
//! tombstone classification, tombstone-only multi-row packing, and
//! visible-domain digest subtraction.
//!
//! `--quick` keeps the run small for smoke checks:
//! `cargo bench --bench row_tombstones -- --quick`.

use std::hint::black_box;
use std::time::{Duration, Instant};

use wendelon::hash::state_digest::{StateDigestAccumulator, visible_row_element};
use wendelon::schema::{
	HybridTimestamp, Identity, IntraBatchIndex, Locality, MultiRowLayoutDescriptor,
	RecordBodyCompressionPolicy, RecordRowIndex, SlabId, TableObjectId, TenantId, WriterNodeId,
	synthesize_append_only_row_id,
};
use wendelon::storage::{
	MultiRowSourceRow, RecordLocation, pack_tombstone_multirow_source_rows_with_policy,
};

const DEFAULT_ROWS: usize = 100_000;
const QUICK_ROWS: usize = 4_096;
const DEFAULT_DENSITIES: &[u8] = &[0, 1, 10];
const WARMUP_ITERS: usize = 1;
const MEASURE_ITERS: usize = 5;

#[derive(Clone, Debug)]
struct BenchConfig {
	rows: usize,
	measure_iters: usize,
	densities: Vec<u8>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
	let config = BenchConfig::parse()?;
	let tenant = TenantId::new(47);
	let table = TableObjectId::new([0x47; 16]);
	let writer = WriterNodeId::new([0x47; 16]);
	let row_hlc = hlc(1_700_000_047_000, 0, 47);
	let delete_hlc = hlc(1_700_000_047_100, 0, 47);
	let row_key_ids = row_key_ids(config.rows, tenant, table, writer, row_hlc)?;

	for density in config.densities.iter().copied() {
		let tombstone_rows = tombstone_rows(&row_key_ids, density, delete_hlc);
		let chains = version_chains(&row_key_ids, density, row_hlc, delete_hlc);
		bench_visibility_classification(&config, density, &chains);
		bench_tombstone_pack(&config, density, &tombstone_rows)?;
		bench_visible_digest(&config, density, tenant, table, &row_key_ids);
	}

	Ok(())
}

impl BenchConfig {
	fn parse() -> Result<Self, Box<dyn std::error::Error>> {
		let mut rows = DEFAULT_ROWS;
		let mut measure_iters = MEASURE_ITERS;
		let mut densities = DEFAULT_DENSITIES.to_vec();
		let mut args = std::env::args().skip(1);
		while let Some(arg) = args.next() {
			match arg.as_str() {
				"--quick" => {
					rows = QUICK_ROWS;
					measure_iters = 2;
				}
				"--rows" => {
					let value = args.next().ok_or("--rows requires a value")?;
					rows = value.parse()?;
				}
				"--density" => {
					let value = args.next().ok_or("--density requires a value")?;
					densities = vec![value.parse()?];
				}
				"--bench" => {}
				_ => {}
			}
		}
		if densities.iter().any(|density| *density > 100) {
			return Err("density must be between 0 and 100".into());
		}
		Ok(Self {
			rows,
			measure_iters,
			densities,
		})
	}
}

fn bench_visibility_classification(
	config: &BenchConfig,
	density_percent: u8,
	chains: &[Vec<RecordLocation>],
) {
	for _ in 0..WARMUP_ITERS {
		black_box(classify_visible(chains));
	}
	let started = Instant::now();
	let mut visible = 0usize;
	for _ in 0..config.measure_iters {
		visible = visible.saturating_add(classify_visible(chains));
	}
	let elapsed = started.elapsed();
	black_box(visible);
	report_rows(
		"latest-as-of-classify",
		density_percent,
		config.rows,
		config.measure_iters,
		elapsed,
	);
}

fn bench_tombstone_pack(
	config: &BenchConfig,
	density_percent: u8,
	rows: &[MultiRowSourceRow],
) -> Result<(), Box<dyn std::error::Error>> {
	if rows.is_empty() {
		println!(
			"row_tombstones case=tombstone-pack density_percent={density_percent} rows={} tombstones=0 skipped=true",
			config.rows
		);
		return Ok(());
	}
	let layout = MultiRowLayoutDescriptor::default();
	for _ in 0..WARMUP_ITERS {
		let packed = pack_tombstone_multirow_source_rows_with_policy(
			layout,
			rows,
			RecordBodyCompressionPolicy::Uncompressed,
		)?;
		black_box(packed);
	}
	let started = Instant::now();
	let mut records = 0usize;
	for _ in 0..config.measure_iters {
		let packed = pack_tombstone_multirow_source_rows_with_policy(
			layout,
			rows,
			RecordBodyCompressionPolicy::Uncompressed,
		)?;
		records = records.saturating_add(packed.len());
		black_box(packed);
	}
	let elapsed = started.elapsed();
	report_tombstones(
		"tombstone-pack",
		density_percent,
		config.rows,
		rows.len(),
		records,
		config.measure_iters,
		elapsed,
	);
	Ok(())
}

fn bench_visible_digest(
	config: &BenchConfig,
	density_percent: u8,
	tenant: TenantId,
	table: TableObjectId,
	row_key_ids: &[Identity],
) {
	for _ in 0..WARMUP_ITERS {
		black_box(visible_digest(tenant, table, row_key_ids, density_percent));
	}
	let started = Instant::now();
	let mut logical_ops = 0usize;
	for _ in 0..config.measure_iters {
		let digest = visible_digest(tenant, table, row_key_ids, density_percent);
		logical_ops = logical_ops
			.saturating_add(row_key_ids.len())
			.saturating_add(tombstone_count(row_key_ids.len(), density_percent));
		black_box(digest);
	}
	let elapsed = started.elapsed();
	println!(
		"row_tombstones case=visible-digest density_percent={density_percent} rows={} logical_ops={logical_ops} total_ns={} ns_per_op={} ops_per_sec={:.2}",
		config.rows,
		elapsed.as_nanos(),
		ns_per_item(elapsed, logical_ops),
		items_per_sec(elapsed, logical_ops),
	);
}

fn classify_visible(chains: &[Vec<RecordLocation>]) -> usize {
	chains
		.iter()
		.filter(|versions| {
			versions
				.last()
				.map(|location| !location.is_tombstone)
				.unwrap_or(false)
		})
		.count()
}

fn visible_digest(
	tenant: TenantId,
	table: TableObjectId,
	row_key_ids: &[Identity],
	density_percent: u8,
) -> wendelon::hash::state_digest::StateDigest {
	let mut accumulator = StateDigestAccumulator::new();
	for row_key_id in row_key_ids {
		accumulator.add_element(visible_row_element(tenant, table, *row_key_id));
	}
	for (idx, row_key_id) in row_key_ids.iter().enumerate() {
		if is_tombstoned(idx, density_percent) {
			accumulator.subtract_element(visible_row_element(tenant, table, *row_key_id));
		}
	}
	accumulator.finish()
}

fn row_key_ids(
	rows: usize,
	tenant: TenantId,
	table: TableObjectId,
	writer: WriterNodeId,
	row_hlc: HybridTimestamp,
) -> Result<Vec<Identity>, Box<dyn std::error::Error>> {
	let mut out = Vec::with_capacity(rows);
	for row in 0..rows {
		out.push(synthesize_append_only_row_id(
			tenant,
			table,
			writer,
			row_hlc,
			IntraBatchIndex::try_new(row as u32)?,
		));
	}
	Ok(out)
}

fn version_chains(
	row_key_ids: &[Identity],
	density_percent: u8,
	row_hlc: HybridTimestamp,
	delete_hlc: HybridTimestamp,
) -> Vec<Vec<RecordLocation>> {
	row_key_ids
		.iter()
		.enumerate()
		.map(|(idx, _row_key_id)| {
			let mut versions = vec![record_location(1, idx as u64, row_hlc, false)];
			if is_tombstoned(idx, density_percent) {
				versions.push(record_location(2, idx as u64, delete_hlc, true));
			}
			versions
		})
		.collect()
}

fn tombstone_rows(
	row_key_ids: &[Identity],
	density_percent: u8,
	delete_hlc: HybridTimestamp,
) -> Vec<MultiRowSourceRow> {
	row_key_ids
		.iter()
		.enumerate()
		.filter(|(idx, _row_key_id)| is_tombstoned(*idx, density_percent))
		.enumerate()
		.map(|(source_row_index, (_idx, row_key_id))| MultiRowSourceRow {
			source_row_index,
			key_identity: *row_key_id,
			routing_locality: Locality::from(*row_key_id),
			row_hlc: delete_hlc,
			is_tombstone: true,
			table_object_id: Some(TableObjectId::new([0x47; 16])),
		})
		.collect()
}

fn record_location(
	slab_id: u64,
	offset_ordinal: u64,
	hlc: HybridTimestamp,
	is_tombstone: bool,
) -> RecordLocation {
	RecordLocation {
		slab_id: SlabId::new(slab_id),
		offset: offset_ordinal.saturating_mul(4096),
		length: 128,
		hlc,
		record_row_index: RecordRowIndex::ZERO,
		is_tombstone,
	}
}

fn is_tombstoned(idx: usize, density_percent: u8) -> bool {
	density_percent > 0 && (idx % 100) < usize::from(density_percent)
}

fn tombstone_count(rows: usize, density_percent: u8) -> usize {
	(0..rows)
		.filter(|idx| is_tombstoned(*idx, density_percent))
		.count()
}

fn hlc(physical_ms: u64, logical: u32, node_id: u32) -> HybridTimestamp {
	HybridTimestamp {
		physical_ms,
		logical,
		node_id,
	}
}

fn report_rows(case: &str, density_percent: u8, rows: usize, iters: usize, elapsed: Duration) {
	let inspected = rows.saturating_mul(iters);
	println!(
		"row_tombstones case={case} density_percent={density_percent} rows={rows} iterations={iters} total_ns={} ns_per_row={} rows_per_sec={:.2}",
		elapsed.as_nanos(),
		ns_per_item(elapsed, inspected),
		items_per_sec(elapsed, inspected),
	);
}

fn report_tombstones(
	case: &str,
	density_percent: u8,
	rows: usize,
	tombstones_per_iter: usize,
	records: usize,
	iters: usize,
	elapsed: Duration,
) {
	let tombstones = tombstones_per_iter.saturating_mul(iters);
	println!(
		"row_tombstones case={case} density_percent={density_percent} rows={rows} tombstones_per_iter={tombstones_per_iter} records={records} total_ns={} ns_per_tombstone={} tombstones_per_sec={:.2}",
		elapsed.as_nanos(),
		ns_per_item(elapsed, tombstones),
		items_per_sec(elapsed, tombstones),
	);
}

fn ns_per_item(elapsed: Duration, items: usize) -> u128 {
	elapsed.as_nanos() / items.max(1) as u128
}

fn items_per_sec(elapsed: Duration, items: usize) -> f64 {
	items as f64 / elapsed.as_secs_f64().max(f64::EPSILON)
}
