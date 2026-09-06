//! ADR 050 PK-delete supersession cold-path benchmark.
//!
//! `--quick` keeps the run small for smoke checks:
//! `cargo bench --bench pk_delete_supersession -- --quick`.

use std::hint::black_box;
use std::time::{Duration, Instant};

use wendelon::hash::state_digest::{StateDigestAccumulator, visible_row_element};
use wendelon::schema::{
	HybridTimestamp, Identity, Locality, MultiRowLayoutDescriptor, PlacementSemantics,
	RecordBodyCompressionPolicy, RecordRowIndex, SlabId, TableObjectId, TenantId,
};
use wendelon::storage::{
	MultiRowSourceRow, RecordLocation, pack_tombstone_multirow_source_rows_with_policy,
};

const DEFAULT_ROWS: &[usize] = &[100_000, 1_000_000];
const QUICK_ROWS: &[usize] = &[4_096, 16_384];
const MEASURE_ITERS: usize = 5;
const QUICK_MEASURE_ITERS: usize = 2;

#[derive(Clone, Debug)]
struct BenchConfig {
	rows: Vec<usize>,
	measure_iters: usize,
}

#[derive(Clone, Copy, Debug, Default)]
struct Classification {
	deleted: usize,
	already_deleted: usize,
	not_found: usize,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
	let config = BenchConfig::parse()?;
	let tenant = TenantId::new(50);
	let table = TableObjectId::new([0x50; 16]);
	let data_hlc = hlc(1_700_000_050_000, 0);
	let delete_hlc = hlc(1_700_000_050_100, 0);
	let resurrection_hlc = hlc(1_700_000_050_200, 0);

	for rows in config.rows.iter().copied() {
		let keys = row_key_ids(rows);
		let mixed = mixed_chains(&keys, data_hlc, delete_hlc);
		bench_delete_classification(&config, rows, &mixed);
		bench_tombstone_pack(&config, rows, &keys, delete_hlc)?;
		bench_digest_transitions(
			&config,
			rows,
			tenant,
			table,
			&keys,
			delete_hlc,
			resurrection_hlc,
		);
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

fn bench_delete_classification(config: &BenchConfig, rows: usize, chains: &[Vec<RecordLocation>]) {
	black_box(classify_delete(chains));
	let started = Instant::now();
	for _ in 0..config.measure_iters {
		let classified = classify_delete(chains);
		black_box(classified);
	}
	report(
		"delete-classification",
		rows,
		rows.saturating_mul(config.measure_iters),
		started.elapsed(),
	);
}

fn bench_tombstone_pack(
	config: &BenchConfig,
	rows: usize,
	keys: &[Identity],
	delete_hlc: HybridTimestamp,
) -> Result<(), Box<dyn std::error::Error>> {
	let tombstones = tombstone_rows(keys, delete_hlc);
	let layout = MultiRowLayoutDescriptor::default();
	let started = Instant::now();
	let mut records = 0usize;
	for _ in 0..config.measure_iters {
		let packed = pack_tombstone_multirow_source_rows_with_policy(
			layout,
			&tombstones,
			RecordBodyCompressionPolicy::Lz4Frame,
		)?;
		records = records.saturating_add(packed.len());
		black_box(packed);
	}
	report(
		"tombstone-pack",
		rows,
		rows.saturating_mul(config.measure_iters),
		started.elapsed(),
	);
	black_box(records);
	Ok(())
}

fn bench_digest_transitions(
	config: &BenchConfig,
	rows: usize,
	tenant: TenantId,
	table: TableObjectId,
	keys: &[Identity],
	delete_hlc: HybridTimestamp,
	resurrection_hlc: HybridTimestamp,
) {
	let started = Instant::now();
	for _ in 0..config.measure_iters {
		let mut accumulator = StateDigestAccumulator::new();
		for key in keys {
			accumulator.add_element(visible_row_element(tenant, table, *key));
		}
		for key in keys.iter().step_by(2) {
			accumulator.subtract_element(visible_row_element(tenant, table, *key));
		}
		for key in keys.iter().step_by(4) {
			accumulator.add_element(visible_row_element(tenant, table, *key));
		}
		black_box(accumulator.finish());
	}
	let logical_ops = rows
		.saturating_add(rows / 2)
		.saturating_add(rows / 4)
		.saturating_mul(config.measure_iters);
	report(
		"visible-digest-transitions",
		rows,
		logical_ops,
		started.elapsed(),
	);
	black_box((delete_hlc, resurrection_hlc));
}

fn classify_delete(chains: &[Vec<RecordLocation>]) -> Classification {
	let mut out = Classification::default();
	for chain in chains {
		match chain.last() {
			Some(location) if location.is_tombstone => {
				out.already_deleted = out.already_deleted.saturating_add(1);
			}
			Some(_) => out.deleted = out.deleted.saturating_add(1),
			None => out.not_found = out.not_found.saturating_add(1),
		}
	}
	out
}

fn mixed_chains(
	keys: &[Identity],
	data_hlc: HybridTimestamp,
	delete_hlc: HybridTimestamp,
) -> Vec<Vec<RecordLocation>> {
	keys.iter()
		.enumerate()
		.map(|(idx, _key)| {
			if idx % 10 == 0 {
				Vec::new()
			} else if idx % 3 == 0 {
				vec![
					location(idx as u64, data_hlc, false),
					location(idx as u64, delete_hlc, true),
				]
			} else {
				vec![location(idx as u64, data_hlc, false)]
			}
		})
		.collect()
}

fn tombstone_rows(keys: &[Identity], delete_hlc: HybridTimestamp) -> Vec<MultiRowSourceRow> {
	keys.iter()
		.enumerate()
		.map(|(source_row_index, key_identity)| MultiRowSourceRow {
			source_row_index,
			key_identity: *key_identity,
			routing_locality: Locality::from(*key_identity),
			row_hlc: delete_hlc,
			is_tombstone: true,
			partition: None,
			table_object_id: Some(TableObjectId::new([0x50; 16])),
		})
		.collect()
}

fn row_key_ids(rows: usize) -> Vec<Identity> {
	(0..rows)
		.map(|idx| {
			let mut bytes = [0u8; 32];
			bytes[..8].copy_from_slice(&(idx as u64).to_le_bytes());
			Identity::new(bytes)
		})
		.collect()
}

fn location(offset: u64, hlc: HybridTimestamp, is_tombstone: bool) -> RecordLocation {
	RecordLocation {
		slab_id: SlabId::new(1),
		offset,
		length: 64,
		hlc,
		record_row_index: RecordRowIndex::ZERO,
		is_tombstone,
		placement: PlacementSemantics::RawLocality,
		partition: None,
	}
}

fn hlc(physical_ms: u64, logical: u32) -> HybridTimestamp {
	HybridTimestamp {
		physical_ms,
		logical,
		node_id: 50,
	}
}

fn report(case: &str, rows: usize, ops: usize, elapsed: Duration) {
	println!(
		"pk_delete_supersession case={case} rows={rows} ops={ops} total_ns={} ns_per_op={} ops_per_sec={:.2}",
		elapsed.as_nanos(),
		ns_per_item(elapsed, ops),
		items_per_sec(elapsed, ops),
	);
}

fn ns_per_item(elapsed: Duration, items: usize) -> u128 {
	if items == 0 {
		return 0;
	}
	elapsed.as_nanos() / items as u128
}

fn items_per_sec(elapsed: Duration, items: usize) -> f64 {
	let secs = elapsed.as_secs_f64();
	if secs == 0.0 {
		return 0.0;
	}
	items as f64 / secs
}
