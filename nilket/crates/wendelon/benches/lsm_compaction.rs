//! ADR 055 LSM compaction substrate benchmark.
//!
//! `--quick` keeps the run small for smoke checks:
//! `cargo bench --bench lsm_compaction -- --quick`.
//!
//! This harness isolates the pure and local pieces that are already part of
//! the substrate: picker scan cost, survivor elision, and output-run planning.
//! Full sidecar rebuild and visible-digest cold-rebuild costs belong in the
//! ADR 055 bench-results matrix once those install paths are wired.

use std::hint::black_box;
use std::time::Instant;

use wendelon::schema::{
	ByteCount, CompactionLevelIndex, HybridTimestamp, Identity, ManifestEpoch, RecordRowIndex,
	RowCount, SlabId, TenantId,
};
use wendelon::storage::{
	LsmConfig, LsmRunSummary, ManifestSnapshot, RecordLocation, VersionEntry, VersionLifecycle,
	apply_elision, pick_compaction, plan_output_runs, survivor_records,
};

const DEFAULT_ROWS: &[usize] = &[16_384, 131_072];
const QUICK_ROWS: &[usize] = &[4_096];
const FULL_ROWS: &[usize] = &[100_000, 1_000_000];
const DEFAULT_RUNS: &[usize] = &[16, 128, 1_024];
const QUICK_RUNS: &[usize] = &[16];
const FULL_RUNS: &[usize] = &[1_024, 8_192];
const MEASURE_ITERS: usize = 5;
const QUICK_MEASURE_ITERS: usize = 2;

#[derive(Clone, Debug)]
struct BenchConfig {
	rows: Vec<usize>,
	runs: Vec<usize>,
	measure_iters: usize,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
	let config = BenchConfig::parse()?;
	let lsm_config = LsmConfig {
		lsm_l1_slab_size_target_bytes: 256 * 1024,
		..LsmConfig::default()
	};
	let now = hlc(1_700_000_055_000);

	for runs in config.runs.iter().copied() {
		bench_picker(&config, runs, lsm_config, now);
	}
	for rows in config.rows.iter().copied() {
		let versions = version_entries(rows);
		bench_survivor_elision(&config, rows, &versions);
		bench_output_planning(&config, rows, &versions, lsm_config)?;
	}

	Ok(())
}

impl BenchConfig {
	fn parse() -> Result<Self, Box<dyn std::error::Error>> {
		let mut rows = DEFAULT_ROWS.to_vec();
		let mut runs = DEFAULT_RUNS.to_vec();
		let mut measure_iters = MEASURE_ITERS;
		let mut args = std::env::args().skip(1);
		while let Some(arg) = args.next() {
			match arg.as_str() {
				"--quick" => {
					rows = QUICK_ROWS.to_vec();
					runs = QUICK_RUNS.to_vec();
					measure_iters = QUICK_MEASURE_ITERS;
				}
				"--full" => {
					rows = FULL_ROWS.to_vec();
					runs = FULL_RUNS.to_vec();
					measure_iters = MEASURE_ITERS;
				}
				"--rows" => {
					let value = args.next().ok_or("--rows requires a value")?;
					rows = vec![value.parse()?];
				}
				"--runs" => {
					let value = args.next().ok_or("--runs requires a value")?;
					runs = vec![value.parse()?];
				}
				"--bench" => {}
				_ => {}
			}
		}
		Ok(Self {
			rows,
			runs,
			measure_iters,
		})
	}
}

fn bench_picker(config: &BenchConfig, runs: usize, lsm_config: LsmConfig, now: HybridTimestamp) {
	let manifest = ManifestSnapshot::new(ManifestEpoch::new(55), run_summaries(runs));
	let started = Instant::now();
	let mut picked = 0usize;
	for _ in 0..config.measure_iters {
		if pick_compaction(&manifest, &lsm_config, now).is_some() {
			picked = picked.saturating_add(1);
		}
	}
	report("picker", runs, picked, started.elapsed());
}

fn bench_survivor_elision(config: &BenchConfig, rows: usize, versions: &[VersionEntry]) {
	let retention_floor = hlc(1_700_000_055_000 + rows as u64 / 2);
	let started = Instant::now();
	let mut survivors = 0usize;
	for _ in 0..config.measure_iters {
		let decisions = apply_elision(versions.to_vec(), retention_floor);
		survivors = survivors.saturating_add(survivor_records(&decisions).len());
		black_box(decisions);
	}
	report("survivor-elision", rows, survivors, started.elapsed());
}

fn bench_output_planning(
	config: &BenchConfig,
	rows: usize,
	versions: &[VersionEntry],
	lsm_config: LsmConfig,
) -> std::io::Result<()> {
	let output_ids = (0..rows)
		.map(|idx| SlabId::new(10_000 + idx as u64))
		.collect::<Vec<_>>();
	let target = lsm_config
		.output_run_target_bytes(CompactionLevelIndex::L1)
		.unwrap();
	let started = Instant::now();
	let mut output_runs = 0usize;
	for _ in 0..config.measure_iters {
		let planned = plan_output_runs(
			versions.to_vec(),
			CompactionLevelIndex::L1,
			target,
			output_ids.iter().copied(),
		)?;
		output_runs = output_runs.saturating_add(planned.len());
		black_box(planned);
	}
	report("output-planning", rows, output_runs, started.elapsed());
	Ok(())
}

fn run_summaries(runs: usize) -> Vec<LsmRunSummary> {
	(0..runs)
		.map(|idx| {
			let level = if idx < 8 {
				CompactionLevelIndex::L0
			} else if idx % 3 == 0 {
				CompactionLevelIndex::L2
			} else {
				CompactionLevelIndex::L1
			};
			let min_hlc = hlc(1_700_000_000_000 + idx as u64 * 10);
			let max_hlc = hlc(min_hlc.physical_ms + 100);
			LsmRunSummary::new(
				SlabId::new(idx as u64 + 1),
				level,
				min_hlc,
				max_hlc,
				ByteCount::new(128 * 1024 + idx as u64 * 64),
				RowCount::new(512),
				ManifestEpoch::new(55),
			)
		})
		.collect()
}

fn version_entries(rows: usize) -> Vec<VersionEntry> {
	(0..rows)
		.map(|idx| {
			let row_hlc = hlc(1_700_000_000_000 + idx as u64);
			let key_id = key_identity((idx % 4096) as u64);
			VersionEntry {
				tenant_id: TenantId::new(55),
				row_key_id: key_id,
				row_hlc,
				record_row_index: RecordRowIndex::ZERO,
				location: RecordLocation {
					slab_id: SlabId::new(1 + (idx / 2048) as u64),
					offset: (idx as u64) * 192,
					length: 192,
					hlc: row_hlc,
					record_row_index: RecordRowIndex::ZERO,
					is_tombstone: false,
				},
				lifecycle: VersionLifecycle::PrimaryKey,
			}
		})
		.collect()
}

fn key_identity(seed: u64) -> Identity {
	let mut bytes = [0u8; 32];
	bytes[..8].copy_from_slice(&seed.to_le_bytes());
	Identity::new(bytes)
}

fn hlc(ms: u64) -> HybridTimestamp {
	HybridTimestamp {
		physical_ms: ms,
		logical: 0,
		node_id: 55,
	}
}

fn report(case: &str, units: usize, work: usize, elapsed: std::time::Duration) {
	let total_ns = elapsed.as_nanos();
	let per_unit = if units == 0 {
		0
	} else {
		total_ns / units as u128
	};
	let per_work = if work == 0 {
		0
	} else {
		total_ns / work as u128
	};
	println!(
		"lsm_compaction case={case} units={units} work={work} total_ns={total_ns} ns_per_unit={per_unit} ns_per_work={per_work}"
	);
}
