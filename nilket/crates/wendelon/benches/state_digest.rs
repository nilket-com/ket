//! ADR 046 cold-path StateDigest benchmark.
//!
//! `--quick` keeps the run small for CI smoke checks. The default
//! run exercises the ADR shapes: 10k, 100k, and 1M visible-row
//! elements.

use std::hint::black_box;
use std::time::{Duration, Instant};

use wendelon::hash::state_digest::{
	DOMAIN_TABLE_ROW_VISIBLE, StateDigestAccumulator, accumulate_projected_scalars,
	projected_scalar_bytes, visible_row_element,
};
use wendelon::schema::{Identity, TableObjectId, TenantId};

const DEFAULT_ROWS: &[usize] = &[10_000, 100_000, 1_000_000];
const QUICK_ROWS: &[usize] = &[1_024, 4_096];
const SPLIT_PARTITIONS: usize = 16;
const LTHASH_WORDS: usize = 1024;
const LTHASH_BYTES: usize = LTHASH_WORDS * 2;
const ELEMENT_PROJECTION_DOMAIN: &[u8] = b"state-digest.element";

#[derive(Clone, Debug)]
struct BenchConfig {
	rows: Vec<usize>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
	let config = BenchConfig::parse()?;
	let max_rows = config.rows.iter().copied().max().unwrap_or(0);
	let row_key_ids = row_key_ids(max_rows);
	let tenant = TenantId::new(46);
	let table = TableObjectId::new([0x46; 16]);

	for rows in config.rows {
		let row_key_ids = &row_key_ids[..rows];
		bench_per_element(rows, tenant, table, row_key_ids);
		let scalars = bench_projection(rows, tenant, table, row_key_ids);
		bench_batched_accumulation(rows, &scalars);
		bench_split_merge(rows, tenant, table, row_key_ids);
		bench_subtraction(rows, tenant, table, row_key_ids);
		bench_lthash(rows, tenant, table, row_key_ids);
	}

	Ok(())
}

impl BenchConfig {
	fn parse() -> Result<Self, Box<dyn std::error::Error>> {
		let mut rows = DEFAULT_ROWS.to_vec();
		let mut args = std::env::args().skip(1);
		while let Some(arg) = args.next() {
			match arg.as_str() {
				"--quick" => rows = QUICK_ROWS.to_vec(),
				"--rows" => {
					let value = args.next().ok_or("--rows requires a value")?;
					rows = vec![value.parse()?];
				}
				"--bench" => {}
				_ => {}
			}
		}
		Ok(Self { rows })
	}
}

fn bench_per_element(
	rows: usize,
	tenant: TenantId,
	table: TableObjectId,
	row_key_ids: &[Identity],
) {
	let started = Instant::now();
	let mut accumulator = StateDigestAccumulator::new();
	for row_key_id in row_key_ids {
		accumulator.add_element(visible_row_element(tenant, table, *row_key_id));
	}
	let digest = accumulator.finish();
	let elapsed = started.elapsed();
	black_box(digest);
	println!(
		"state_digest case=per-element rows={rows} total_ns={} ns_per_element={} elements_per_sec={:.2}",
		elapsed.as_nanos(),
		ns_per_element(elapsed, rows),
		elements_per_sec(elapsed, rows),
	);
}

fn bench_projection(
	rows: usize,
	tenant: TenantId,
	table: TableObjectId,
	row_key_ids: &[Identity],
) -> Vec<[u8; 32]> {
	let started = Instant::now();
	let scalars = row_key_ids
		.iter()
		.map(|row_key_id| {
			let preimage = visible_preimage(tenant, table, *row_key_id);
			projected_scalar_bytes(DOMAIN_TABLE_ROW_VISIBLE, &preimage)
		})
		.collect::<Vec<_>>();
	let elapsed = started.elapsed();
	black_box(&scalars);
	println!(
		"state_digest case=projection rows={rows} total_ns={} ns_per_element={} elements_per_sec={:.2}",
		elapsed.as_nanos(),
		ns_per_element(elapsed, rows),
		elements_per_sec(elapsed, rows),
	);
	scalars
}

fn bench_batched_accumulation(rows: usize, scalars: &[[u8; 32]]) {
	let started = Instant::now();
	let digest = accumulate_projected_scalars(scalars.iter().copied());
	let elapsed = started.elapsed();
	black_box(digest);
	println!(
		"state_digest case=batched-accumulate rows={rows} total_ns={} ns_per_element={} elements_per_sec={:.2}",
		elapsed.as_nanos(),
		ns_per_element(elapsed, rows),
		elements_per_sec(elapsed, rows),
	);

	let digest = accumulate_projected_scalars(scalars.iter().copied());
	let validation_iters = 1024_u128;
	let started = Instant::now();
	for _ in 0..validation_iters {
		black_box(digest).decompress().unwrap();
	}
	let validation_elapsed = started.elapsed();
	println!(
		"state_digest case=compressed-validation rows={rows} iterations={validation_iters} avg_ns={}",
		validation_elapsed.as_nanos() / validation_iters,
	);
}

fn bench_split_merge(
	rows: usize,
	tenant: TenantId,
	table: TableObjectId,
	row_key_ids: &[Identity],
) {
	let started = Instant::now();
	let chunk_size = rows.div_ceil(SPLIT_PARTITIONS).max(1);
	let mut merged = StateDigestAccumulator::new();
	for chunk in row_key_ids.chunks(chunk_size) {
		let mut part = StateDigestAccumulator::new();
		for row_key_id in chunk {
			part.add_element(visible_row_element(tenant, table, *row_key_id));
		}
		merged.merge_digest(part.finish()).unwrap();
	}
	let digest = merged.finish();
	let elapsed = started.elapsed();
	black_box(digest);
	println!(
		"state_digest case=split-merge rows={rows} partitions={SPLIT_PARTITIONS} total_ns={} ns_per_element={} elements_per_sec={:.2}",
		elapsed.as_nanos(),
		ns_per_element(elapsed, rows),
		elements_per_sec(elapsed, rows),
	);
}

fn bench_subtraction(
	rows: usize,
	tenant: TenantId,
	table: TableObjectId,
	row_key_ids: &[Identity],
) {
	let started = Instant::now();
	let mut accumulator = StateDigestAccumulator::new();
	for row_key_id in row_key_ids {
		accumulator.add_element(visible_row_element(tenant, table, *row_key_id));
	}
	for row_key_id in &row_key_ids[..rows / 10] {
		accumulator.subtract_element(visible_row_element(tenant, table, *row_key_id));
	}
	let digest = accumulator.finish();
	let elapsed = started.elapsed();
	black_box(digest);
	println!(
		"state_digest case=subtract-10-percent rows={rows} total_ns={} ns_per_element={} elements_per_sec={:.2}",
		elapsed.as_nanos(),
		ns_per_element(elapsed, rows),
		elements_per_sec(elapsed, rows),
	);
}

fn bench_lthash(rows: usize, tenant: TenantId, table: TableObjectId, row_key_ids: &[Identity]) {
	let started = Instant::now();
	let mut baseline = LtHashBaseline::new();
	for row_key_id in row_key_ids {
		let preimage = visible_preimage(tenant, table, *row_key_id);
		baseline.add(&preimage);
	}
	let elapsed = started.elapsed();
	black_box(baseline);
	println!(
		"state_digest case=lthash-baseline rows={rows} state_bytes={LTHASH_BYTES} total_ns={} ns_per_element={} elements_per_sec={:.2}",
		elapsed.as_nanos(),
		ns_per_element(elapsed, rows),
		elements_per_sec(elapsed, rows),
	);
}

fn row_key_ids(rows: usize) -> Vec<Identity> {
	(0..rows)
		.map(|i| {
			let mut preimage = [0u8; 16];
			preimage[..8].copy_from_slice(&(i as u64).to_le_bytes());
			preimage[8..].copy_from_slice(&(rows as u64).to_le_bytes());
			Identity::blake3(&preimage)
		})
		.collect()
}

fn visible_preimage(
	tenant_id: TenantId,
	table_object_id: TableObjectId,
	row_key_id: Identity,
) -> [u8; 64] {
	let mut preimage = [0u8; 64];
	preimage[..8].copy_from_slice(&tenant_id.get().to_le_bytes());
	preimage[16..32].copy_from_slice(table_object_id.as_bytes());
	preimage[32..64].copy_from_slice(row_key_id.as_bytes());
	preimage
}

fn ns_per_element(elapsed: Duration, rows: usize) -> u128 {
	elapsed.as_nanos() / rows.max(1) as u128
}

fn elements_per_sec(elapsed: Duration, rows: usize) -> f64 {
	rows as f64 / elapsed.as_secs_f64().max(f64::EPSILON)
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct LtHashBaseline {
	state: [u16; LTHASH_WORDS],
}

impl LtHashBaseline {
	fn new() -> Self {
		Self {
			state: [0; LTHASH_WORDS],
		}
	}

	fn add(&mut self, preimage: &[u8]) {
		let mut bytes = [0u8; LTHASH_BYTES];
		let mut hasher = blake3::Hasher::new();
		hasher.update(ELEMENT_PROJECTION_DOMAIN);
		hasher.update(&(DOMAIN_TABLE_ROW_VISIBLE.tag().len() as u32).to_be_bytes());
		hasher.update(DOMAIN_TABLE_ROW_VISIBLE.tag());
		hasher.update(&(preimage.len() as u64).to_be_bytes());
		hasher.update(preimage);
		hasher.finalize_xof().fill(&mut bytes);
		for (slot, word) in self.state.iter_mut().zip(bytes.chunks_exact(2)) {
			let value = u16::from_le_bytes([word[0], word[1]]);
			*slot = slot.wrapping_add(value);
		}
	}
}
