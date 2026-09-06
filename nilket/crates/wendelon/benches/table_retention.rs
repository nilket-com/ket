//! ADR 048 per-table retention cold-path benchmark.
//!
//! The full L1/L2 storage paths are covered by targeted tests. This
//! bench isolates the steady-state costs called out by ADR 048 Phase 7:
//! table-aware retention classification, mixed-policy rewrite packing,
//! read admission, visible-domain digest reversal, and tombstone pair
//! composition.
//!
//! `--quick` keeps the run small for smoke checks:
//! `cargo bench --bench table_retention -- --quick`.

use std::collections::BTreeMap;
use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{ArrayRef, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use wendelon::hash::state_digest::{
	StateDigestAccumulator, apply_retention_eviction_to_visible_digest, visible_row_element,
};
use wendelon::schema::{
	HybridTimestamp, Identity, IntraBatchIndex, Locality, MultiRowLayoutDescriptor,
	PolicyGeneration, RecordBodyCompressionPolicy, RetentionWindowMillis, TableLifecycleCatalog,
	TableObjectId, TableRetentionPolicy, TenantId, WriterNodeId, synthesize_append_only_row_id,
};
use wendelon::storage::{
	MultiRowSourceRow, MvccRetentionPolicy, check_read_against_retention,
	effective_table_retention, pack_multirow_source_rows_with_policy, row_visible_under_retention,
};

const DEFAULT_ROWS: &[usize] = &[100_000, 1_000_000];
const QUICK_ROWS: &[usize] = &[4_096, 16_384];
const DEFAULT_DENSITIES: &[u8] = &[0, 1, 10];
const WARMUP_ITERS: usize = 1;
const MEASURE_ITERS: usize = 5;
const QUICK_MEASURE_ITERS: usize = 2;
const DAY_MS: u64 = 24 * 60 * 60 * 1000;
const SHORT_WINDOW_MS: u64 = 30 * DAY_MS;
const LONG_WINDOW_MS: u64 = 90 * DAY_MS;
const CLUSTER_WINDOW_MS: u64 = 60 * DAY_MS;
const NOW_MS: u64 = 1_700_000_048_000;
const APPROX_ROW_BYTES: u64 = 160;

#[derive(Clone, Debug)]
struct BenchConfig {
	rows: Vec<usize>,
	measure_iters: usize,
	densities: Vec<u8>,
}

#[derive(Clone, Copy, Debug)]
struct Tables {
	short: TableObjectId,
	long: TableObjectId,
	disabled: TableObjectId,
	inherit: TableObjectId,
}

#[derive(Clone, Copy, Debug)]
struct BenchRow {
	source_row_index: usize,
	key_identity: Identity,
	routing_locality: Locality,
	row_hlc: HybridTimestamp,
	is_tombstone: bool,
	table_object_id: Option<TableObjectId>,
}

#[derive(Clone, Copy, Debug)]
struct RetentionBenchContext<'a> {
	config: &'a BenchConfig,
	tenant: TenantId,
	catalog: &'a TableLifecycleCatalog,
	cluster: MvccRetentionPolicy,
}

#[derive(Clone, Copy, Debug)]
struct RetentionBenchCase<'a> {
	case: &'a str,
	rows: usize,
	density_percent: u8,
	stored_rows: &'a [BenchRow],
}

#[derive(Clone, Debug, Default)]
struct RetentionOutcome {
	inspected_rows: usize,
	survivor_rows: usize,
	data_pruned: usize,
	tombstone_pruned: usize,
	pair_drops: usize,
	ambiguous_retained: usize,
	reclaimed_bytes: u64,
}

#[derive(Clone, Debug, Default)]
struct RewriteOutcome {
	retired: bool,
	frames: usize,
	encoded_bytes: u64,
	classification: RetentionOutcome,
}

#[derive(Clone, Debug, Default)]
struct ReadAdmissionOutcome {
	ops: usize,
	admitted: usize,
	rejected: usize,
	filtered_rows: usize,
	csb_direct: usize,
	csb_zero: usize,
	csb_row_filter: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CsbDecision {
	Direct,
	Zero,
	RowFilter,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
	let config = BenchConfig::parse()?;
	let tenant = TenantId::new(48);
	let tables = Tables {
		short: TableObjectId::new([0x30; 16]),
		long: TableObjectId::new([0x90; 16]),
		disabled: TableObjectId::new([0xDD; 16]),
		inherit: TableObjectId::new([0x48; 16]),
	};
	let catalog = lifecycle_catalog(tenant, tables)?;
	let cluster = MvccRetentionPolicy::FixedWindow(Duration::from_millis(CLUSTER_WINDOW_MS));
	let layout = MultiRowLayoutDescriptor::new(512, 4096, 2 * 1024 * 1024)?;
	let bench_context = RetentionBenchContext {
		config: &config,
		tenant,
		catalog: &catalog,
		cluster,
	};

	for rows in config.rows.iter().copied() {
		bench_read_admission(&config, rows, tenant, tables, &catalog, cluster);
		bench_pk_cluster_retention(&config, rows, cluster);
		bench_pair_composition(&config, rows, tenant, tables.short, &catalog, cluster);

		let single = append_rows(tenant, &[tables.short], rows, 0, OwnershipMode::Known)?;
		bench_l1_pruning(
			bench_context,
			retention_bench_case("single-fixed", rows, 0, &single),
		);
		bench_digest_reverse(
			&config,
			"single-fixed",
			rows,
			tenant,
			&single,
			&catalog,
			cluster,
		);

		let disabled = constant_age_rows(
			tenant,
			tables.disabled,
			rows,
			120 * DAY_MS,
			0,
			OwnershipMode::Known,
		)?;
		bench_l1_pruning(
			bench_context,
			retention_bench_case("disabled", rows, 0, &disabled),
		);
		bench_l2_rewrite(
			bench_context,
			retention_bench_case("disabled-retain-all", rows, 0, &disabled),
			layout,
		)?;

		let retire_all = constant_age_rows(
			tenant,
			tables.short,
			rows,
			120 * DAY_MS,
			0,
			OwnershipMode::Known,
		)?;
		bench_l2_rewrite(
			bench_context,
			retention_bench_case("retire-all", rows, 0, &retire_all),
			layout,
		)?;

		let ambiguous = constant_age_rows(
			tenant,
			tables.short,
			rows,
			120 * DAY_MS,
			0,
			OwnershipMode::AmbiguousEvery(1),
		)?;
		bench_l1_pruning(
			bench_context,
			retention_bench_case("ambiguous-retain", rows, 0, &ambiguous),
		);
		bench_l2_rewrite(
			bench_context,
			retention_bench_case("ambiguous-retain-all", rows, 0, &ambiguous),
			layout,
		)?;

		for density in config.densities.iter().copied() {
			let mixed = append_rows(
				tenant,
				&[tables.short, tables.long, tables.disabled, tables.inherit],
				rows,
				density,
				OwnershipMode::Known,
			)?;
			bench_l1_pruning(
				bench_context,
				retention_bench_case("mixed", rows, density, &mixed),
			);
			bench_l2_rewrite(
				bench_context,
				retention_bench_case("mixed-partial", rows, density, &mixed),
				layout,
			)?;
			bench_digest_reverse(&config, "mixed", rows, tenant, &mixed, &catalog, cluster);
		}
	}

	Ok(())
}

impl BenchConfig {
	fn parse() -> Result<Self, Box<dyn std::error::Error>> {
		let mut rows = DEFAULT_ROWS.to_vec();
		let mut measure_iters = MEASURE_ITERS;
		let mut densities = DEFAULT_DENSITIES.to_vec();
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

fn lifecycle_catalog(
	tenant: TenantId,
	tables: Tables,
) -> Result<TableLifecycleCatalog, Box<dyn std::error::Error>> {
	let mut catalog = TableLifecycleCatalog::default();
	catalog.set_next_generation(
		tenant,
		tables.short,
		PolicyGeneration::ZERO,
		TableRetentionPolicy::FixedWindow(RetentionWindowMillis::try_new(SHORT_WINDOW_MS)?),
		hlc(NOW_MS, 0, 48),
	)?;
	catalog.set_next_generation(
		tenant,
		tables.long,
		PolicyGeneration::ZERO,
		TableRetentionPolicy::FixedWindow(RetentionWindowMillis::try_new(LONG_WINDOW_MS)?),
		hlc(NOW_MS, 0, 48),
	)?;
	catalog.set_next_generation(
		tenant,
		tables.disabled,
		PolicyGeneration::ZERO,
		TableRetentionPolicy::Disabled,
		hlc(NOW_MS, 0, 48),
	)?;
	catalog.ensure_inherit_cluster(tenant, tables.inherit, hlc(NOW_MS, 0, 48));
	Ok(catalog)
}

fn retention_bench_case<'a>(
	case: &'a str,
	rows: usize,
	density_percent: u8,
	stored_rows: &'a [BenchRow],
) -> RetentionBenchCase<'a> {
	RetentionBenchCase {
		case,
		rows,
		density_percent,
		stored_rows,
	}
}

fn bench_l1_pruning(context: RetentionBenchContext<'_>, input: RetentionBenchCase<'_>) {
	let RetentionBenchContext {
		config,
		tenant,
		catalog,
		cluster,
	} = context;
	let RetentionBenchCase {
		case,
		rows,
		density_percent,
		stored_rows,
	} = input;
	for _ in 0..WARMUP_ITERS {
		black_box(mark_append_retention(stored_rows, tenant, catalog, cluster));
	}
	let started = Instant::now();
	let mut outcome = RetentionOutcome::default();
	for _ in 0..config.measure_iters {
		let (_, next) = mark_append_retention(stored_rows, tenant, catalog, cluster);
		outcome.inspected_rows = outcome.inspected_rows.saturating_add(next.inspected_rows);
		outcome.survivor_rows = outcome.survivor_rows.saturating_add(next.survivor_rows);
		outcome.data_pruned = outcome.data_pruned.saturating_add(next.data_pruned);
		outcome.tombstone_pruned = outcome
			.tombstone_pruned
			.saturating_add(next.tombstone_pruned);
		outcome.pair_drops = outcome.pair_drops.saturating_add(next.pair_drops);
		outcome.ambiguous_retained = outcome
			.ambiguous_retained
			.saturating_add(next.ambiguous_retained);
		outcome.reclaimed_bytes = outcome.reclaimed_bytes.saturating_add(next.reclaimed_bytes);
	}
	let elapsed = started.elapsed();
	black_box(&outcome);
	println!(
		"table_retention case=l1-prune policy={case} rows={rows} stored_rows={} density_percent={density_percent} iterations={} pruned_data={} pruned_tombstones={} pair_drops={} ambiguous_retained={} reclaimed_bytes={} total_ns={} ns_per_row={} rows_per_sec={:.2}",
		stored_rows.len(),
		config.measure_iters,
		outcome.data_pruned,
		outcome.tombstone_pruned,
		outcome.pair_drops,
		outcome.ambiguous_retained,
		outcome.reclaimed_bytes,
		elapsed.as_nanos(),
		ns_per_item(elapsed, outcome.inspected_rows),
		items_per_sec(elapsed, outcome.inspected_rows),
	);
}

fn bench_l2_rewrite(
	context: RetentionBenchContext<'_>,
	input: RetentionBenchCase<'_>,
	layout: MultiRowLayoutDescriptor,
) -> Result<(), Box<dyn std::error::Error>> {
	let RetentionBenchContext {
		config,
		tenant,
		catalog,
		cluster,
	} = context;
	let RetentionBenchCase {
		case,
		rows,
		density_percent,
		stored_rows,
	} = input;
	let batch = user_batch(stored_rows.len())?;
	for _ in 0..WARMUP_ITERS {
		black_box(simulate_l2_rewrite(
			&batch,
			stored_rows,
			tenant,
			catalog,
			cluster,
			layout,
		)?);
	}
	let started = Instant::now();
	let mut outcome = RewriteOutcome::default();
	for _ in 0..config.measure_iters {
		let next = simulate_l2_rewrite(&batch, stored_rows, tenant, catalog, cluster, layout)?;
		outcome.retired |= next.retired;
		outcome.frames = outcome.frames.saturating_add(next.frames);
		outcome.encoded_bytes = outcome.encoded_bytes.saturating_add(next.encoded_bytes);
		outcome.classification.inspected_rows = outcome
			.classification
			.inspected_rows
			.saturating_add(next.classification.inspected_rows);
		outcome.classification.survivor_rows = outcome
			.classification
			.survivor_rows
			.saturating_add(next.classification.survivor_rows);
		outcome.classification.data_pruned = outcome
			.classification
			.data_pruned
			.saturating_add(next.classification.data_pruned);
		outcome.classification.tombstone_pruned = outcome
			.classification
			.tombstone_pruned
			.saturating_add(next.classification.tombstone_pruned);
		outcome.classification.pair_drops = outcome
			.classification
			.pair_drops
			.saturating_add(next.classification.pair_drops);
		outcome.classification.ambiguous_retained = outcome
			.classification
			.ambiguous_retained
			.saturating_add(next.classification.ambiguous_retained);
		outcome.classification.reclaimed_bytes = outcome
			.classification
			.reclaimed_bytes
			.saturating_add(next.classification.reclaimed_bytes);
	}
	let elapsed = started.elapsed();
	black_box(&outcome);
	println!(
		"table_retention case=l2-rewrite policy={case} rows={rows} stored_rows={} density_percent={density_percent} iterations={} retired={} frames={} encoded_bytes={} survivors={} pruned_data={} pruned_tombstones={} ambiguous_retained={} total_ns={} ns_per_row={} rows_per_sec={:.2}",
		stored_rows.len(),
		config.measure_iters,
		outcome.retired,
		outcome.frames,
		outcome.encoded_bytes,
		outcome.classification.survivor_rows,
		outcome.classification.data_pruned,
		outcome.classification.tombstone_pruned,
		outcome.classification.ambiguous_retained,
		elapsed.as_nanos(),
		ns_per_item(elapsed, outcome.classification.inspected_rows),
		items_per_sec(elapsed, outcome.classification.inspected_rows),
	);
	Ok(())
}

fn simulate_l2_rewrite(
	batch: &RecordBatch,
	stored_rows: &[BenchRow],
	tenant: TenantId,
	catalog: &TableLifecycleCatalog,
	cluster: MvccRetentionPolicy,
	layout: MultiRowLayoutDescriptor,
) -> std::io::Result<RewriteOutcome> {
	let (reclaim, classification) = mark_append_retention(stored_rows, tenant, catalog, cluster);
	let survivors = stored_rows
		.iter()
		.zip(reclaim.iter())
		.filter(|(_, reclaim)| !**reclaim)
		.map(|(row, _)| source_row(*row))
		.collect::<Vec<_>>();
	if survivors.is_empty() {
		return Ok(RewriteOutcome {
			retired: true,
			classification,
			..RewriteOutcome::default()
		});
	}
	let frames = pack_multirow_source_rows_with_policy(
		layout,
		batch,
		&survivors,
		RecordBodyCompressionPolicy::Uncompressed,
	)?;
	let encoded_bytes = frames
		.iter()
		.map(|frame| frame.encoded.encoded_byte_len)
		.sum::<u64>();
	Ok(RewriteOutcome {
		retired: false,
		frames: frames.len(),
		encoded_bytes,
		classification,
	})
}

fn bench_read_admission(
	config: &BenchConfig,
	rows: usize,
	tenant: TenantId,
	tables: Tables,
	catalog: &TableLifecycleCatalog,
	cluster: MvccRetentionPolicy,
) {
	let table_ids = [tables.short, tables.long, tables.disabled, tables.inherit];
	for case in [
		ReadCase::Latest,
		ReadCase::ValidAsOf,
		ReadCase::ExpiredAsOf,
		ReadCase::MultiTableConjunction,
		ReadCase::CsbStraddleGate,
	] {
		for _ in 0..WARMUP_ITERS {
			black_box(run_read_case(
				case, rows, tenant, &table_ids, catalog, cluster,
			));
		}
		let started = Instant::now();
		let mut outcome = ReadAdmissionOutcome::default();
		for _ in 0..config.measure_iters {
			let next = run_read_case(case, rows, tenant, &table_ids, catalog, cluster);
			outcome.ops = outcome.ops.saturating_add(next.ops);
			outcome.admitted = outcome.admitted.saturating_add(next.admitted);
			outcome.rejected = outcome.rejected.saturating_add(next.rejected);
			outcome.filtered_rows = outcome.filtered_rows.saturating_add(next.filtered_rows);
			outcome.csb_direct = outcome.csb_direct.saturating_add(next.csb_direct);
			outcome.csb_zero = outcome.csb_zero.saturating_add(next.csb_zero);
			outcome.csb_row_filter = outcome.csb_row_filter.saturating_add(next.csb_row_filter);
		}
		let elapsed = started.elapsed();
		black_box(&outcome);
		println!(
			"table_retention case=read-admission mode={} rows={rows} iterations={} admitted={} rejected={} filtered_rows={} csb_direct={} csb_zero={} csb_row_filter={} total_ns={} ns_per_op={} ops_per_sec={:.2}",
			case.label(),
			config.measure_iters,
			outcome.admitted,
			outcome.rejected,
			outcome.filtered_rows,
			outcome.csb_direct,
			outcome.csb_zero,
			outcome.csb_row_filter,
			elapsed.as_nanos(),
			ns_per_item(elapsed, outcome.ops),
			items_per_sec(elapsed, outcome.ops),
		);
	}
}

#[derive(Clone, Copy, Debug)]
enum ReadCase {
	Latest,
	ValidAsOf,
	ExpiredAsOf,
	MultiTableConjunction,
	CsbStraddleGate,
}

impl ReadCase {
	const fn label(self) -> &'static str {
		match self {
			Self::Latest => "latest",
			Self::ValidAsOf => "valid-as-of",
			Self::ExpiredAsOf => "expired-as-of",
			Self::MultiTableConjunction => "multi-table-conjunction",
			Self::CsbStraddleGate => "csb-straddle-gate",
		}
	}
}

fn run_read_case(
	case: ReadCase,
	rows: usize,
	tenant: TenantId,
	table_ids: &[TableObjectId],
	catalog: &TableLifecycleCatalog,
	cluster: MvccRetentionPolicy,
) -> ReadAdmissionOutcome {
	let mut outcome = ReadAdmissionOutcome::default();
	for idx in 0..rows {
		match case {
			ReadCase::Latest | ReadCase::ValidAsOf | ReadCase::ExpiredAsOf => {
				let table = table_ids[idx % table_ids.len()];
				let effective = effective_table_retention(
					&cluster,
					catalog.retention_policy(tenant, table),
					NOW_MS,
				);
				let as_of_hlc = match case {
					ReadCase::Latest => hlc(0, 0, 0),
					ReadCase::ValidAsOf => hlc(NOW_MS - 10 * DAY_MS, 0, 48),
					ReadCase::ExpiredAsOf => hlc(NOW_MS - 120 * DAY_MS, 0, 48),
					_ => unreachable!(),
				};
				outcome.ops += 1;
				if check_read_against_retention(as_of_hlc, &effective).is_ok() {
					outcome.admitted += 1;
				} else {
					outcome.rejected += 1;
				}
				if !row_visible_under_retention(hlc(NOW_MS - 120 * DAY_MS, 0, 48), &effective) {
					outcome.filtered_rows += 1;
				}
			}
			ReadCase::MultiTableConjunction => {
				outcome.ops += table_ids.len();
				let as_of_hlc = hlc(NOW_MS - 60 * DAY_MS, 0, 48);
				let admitted = table_ids.iter().all(|table| {
					let effective = effective_table_retention(
						&cluster,
						catalog.retention_policy(tenant, *table),
						NOW_MS,
					);
					check_read_against_retention(as_of_hlc, &effective).is_ok()
				});
				if admitted {
					outcome.admitted += 1;
				} else {
					outcome.rejected += 1;
				}
			}
			ReadCase::CsbStraddleGate => {
				let table = table_ids[idx % table_ids.len()];
				let effective = effective_table_retention(
					&cluster,
					catalog.retention_policy(tenant, table),
					NOW_MS,
				);
				let (min_hlc, max_hlc) = match idx % 3 {
					0 => (
						hlc(NOW_MS - 10 * DAY_MS, 0, 48),
						hlc(NOW_MS - 5 * DAY_MS, 0, 48),
					),
					1 => (
						hlc(NOW_MS - 140 * DAY_MS, 0, 48),
						hlc(NOW_MS - 120 * DAY_MS, 0, 48),
					),
					_ => (
						hlc(NOW_MS - 120 * DAY_MS, 0, 48),
						hlc(NOW_MS - 10 * DAY_MS, 0, 48),
					),
				};
				outcome.ops += 1;
				match csb_retention_decision(min_hlc, max_hlc, &effective) {
					CsbDecision::Direct => outcome.csb_direct += 1,
					CsbDecision::Zero => outcome.csb_zero += 1,
					CsbDecision::RowFilter => outcome.csb_row_filter += 1,
				}
			}
		}
	}
	outcome
}

fn bench_pk_cluster_retention(config: &BenchConfig, rows: usize, cluster: MvccRetentionPolicy) {
	let chains = pk_version_chains(rows);
	for _ in 0..WARMUP_ITERS {
		black_box(classify_pk_chains(&chains, cluster));
	}
	let started = Instant::now();
	let mut pruned = 0usize;
	for _ in 0..config.measure_iters {
		pruned = pruned.saturating_add(classify_pk_chains(&chains, cluster));
	}
	let elapsed = started.elapsed();
	black_box(pruned);
	println!(
		"table_retention case=pk-cluster-retention rows={rows} chains={} versions={} iterations={} pruned_versions={} total_ns={} ns_per_version={} versions_per_sec={:.2}",
		chains.len(),
		chains.len() * 3,
		config.measure_iters,
		pruned,
		elapsed.as_nanos(),
		ns_per_item(elapsed, chains.len() * 3 * config.measure_iters),
		items_per_sec(elapsed, chains.len() * 3 * config.measure_iters),
	);
}

fn classify_pk_chains(chains: &[[HybridTimestamp; 3]], cluster: MvccRetentionPolicy) -> usize {
	let Some(floor) = effective_table_retention(&cluster, None, NOW_MS).floor else {
		return 0;
	};
	chains
		.iter()
		.map(|chain| {
			let older_versions = chain.partition_point(|hlc| *hlc < floor);
			older_versions.saturating_sub(1)
		})
		.sum()
}

fn bench_digest_reverse(
	config: &BenchConfig,
	case: &str,
	rows: usize,
	tenant: TenantId,
	stored_rows: &[BenchRow],
	catalog: &TableLifecycleCatalog,
	cluster: MvccRetentionPolicy,
) {
	let (reclaim, _) = mark_append_retention(stored_rows, tenant, catalog, cluster);
	let evicted = stored_rows
		.iter()
		.zip(reclaim.iter())
		.filter_map(|(row, reclaim)| (*reclaim).then_some(*row))
		.collect::<Vec<_>>();

	for _ in 0..WARMUP_ITERS {
		black_box(digest_reverse_only(tenant, &evicted));
	}
	let started = Instant::now();
	let mut ops = 0usize;
	for _ in 0..config.measure_iters {
		let digest = digest_reverse_only(tenant, &evicted);
		ops = ops.saturating_add(evicted.len());
		black_box(digest);
	}
	let elapsed = started.elapsed();
	println!(
		"table_retention case=visible-digest-reverse policy={case} rows={rows} evicted_per_iter={} iterations={} total_ns={} ns_per_eviction={} evictions_per_sec={:.2}",
		evicted.len(),
		config.measure_iters,
		elapsed.as_nanos(),
		ns_per_item(elapsed, ops),
		items_per_sec(elapsed, ops),
	);

	for _ in 0..WARMUP_ITERS {
		black_box(digest_visible_add(tenant, stored_rows));
	}
	let started = Instant::now();
	let mut add_ops = 0usize;
	for _ in 0..config.measure_iters {
		let digest = digest_visible_add(tenant, stored_rows);
		add_ops = add_ops.saturating_add(stored_rows.len());
		black_box(digest);
	}
	let elapsed = started.elapsed();
	println!(
		"table_retention case=visible-digest-add policy={case} rows={rows} stored_rows={} iterations={} total_ns={} ns_per_row={} rows_per_sec={:.2}",
		stored_rows.len(),
		config.measure_iters,
		elapsed.as_nanos(),
		ns_per_item(elapsed, add_ops),
		items_per_sec(elapsed, add_ops),
	);
}

fn digest_reverse_only(
	tenant: TenantId,
	evicted: &[BenchRow],
) -> wendelon::hash::state_digest::StateDigest {
	let mut accumulator = StateDigestAccumulator::new();
	for row in evicted {
		if let Some(table_object_id) = row.table_object_id {
			apply_retention_eviction_to_visible_digest(
				&mut accumulator,
				tenant,
				table_object_id,
				row.key_identity,
				row.is_tombstone,
			);
		}
	}
	accumulator.finish()
}

fn digest_visible_add(
	tenant: TenantId,
	stored_rows: &[BenchRow],
) -> wendelon::hash::state_digest::StateDigest {
	let mut accumulator = StateDigestAccumulator::new();
	for row in stored_rows {
		if let Some(table_object_id) = row.table_object_id {
			let element = visible_row_element(tenant, table_object_id, row.key_identity);
			if row.is_tombstone {
				accumulator.subtract_element(element);
			} else {
				accumulator.add_element(element);
			}
		}
	}
	accumulator.finish()
}

fn bench_pair_composition(
	config: &BenchConfig,
	rows: usize,
	tenant: TenantId,
	table: TableObjectId,
	catalog: &TableLifecycleCatalog,
	cluster: MvccRetentionPolicy,
) {
	let pairs = pair_rows(tenant, table, rows).expect("pair rows");
	for _ in 0..WARMUP_ITERS {
		black_box(classify_pairs(&pairs, tenant, table, catalog, cluster));
	}
	let started = Instant::now();
	let mut outcome = RetentionOutcome::default();
	for _ in 0..config.measure_iters {
		let next = classify_pairs(&pairs, tenant, table, catalog, cluster);
		outcome.inspected_rows = outcome.inspected_rows.saturating_add(next.inspected_rows);
		outcome.survivor_rows = outcome.survivor_rows.saturating_add(next.survivor_rows);
		outcome.data_pruned = outcome.data_pruned.saturating_add(next.data_pruned);
		outcome.tombstone_pruned = outcome
			.tombstone_pruned
			.saturating_add(next.tombstone_pruned);
		outcome.pair_drops = outcome.pair_drops.saturating_add(next.pair_drops);
	}
	let elapsed = started.elapsed();
	black_box(&outcome);
	println!(
		"table_retention case=pair-gc-composition rows={rows} pairs={} iterations={} pair_drops={} pruned_data={} pruned_tombstones={} total_ns={} ns_per_pair={} pairs_per_sec={:.2}",
		pairs.len(),
		config.measure_iters,
		outcome.pair_drops,
		outcome.data_pruned,
		outcome.tombstone_pruned,
		elapsed.as_nanos(),
		ns_per_item(elapsed, pairs.len() * config.measure_iters),
		items_per_sec(elapsed, pairs.len() * config.measure_iters),
	);
}

fn classify_pairs(
	pairs: &[(HybridTimestamp, HybridTimestamp)],
	tenant: TenantId,
	table: TableObjectId,
	catalog: &TableLifecycleCatalog,
	cluster: MvccRetentionPolicy,
) -> RetentionOutcome {
	let effective =
		effective_table_retention(&cluster, catalog.retention_policy(tenant, table), NOW_MS);
	let mut outcome = RetentionOutcome {
		inspected_rows: pairs.len().saturating_mul(2),
		..RetentionOutcome::default()
	};
	let Some(floor) = effective.floor else {
		outcome.survivor_rows = outcome.inspected_rows;
		return outcome;
	};
	for (_data_hlc, delete_hlc) in pairs {
		if *delete_hlc < floor {
			outcome.pair_drops += 1;
			outcome.data_pruned += 1;
			outcome.tombstone_pruned += 1;
		} else {
			outcome.survivor_rows += 2;
		}
	}
	outcome
}

fn mark_append_retention(
	stored_rows: &[BenchRow],
	tenant: TenantId,
	catalog: &TableLifecycleCatalog,
	cluster: MvccRetentionPolicy,
) -> (Vec<bool>, RetentionOutcome) {
	let mut reclaim = vec![false; stored_rows.len()];
	let mut outcome = RetentionOutcome {
		inspected_rows: stored_rows.len(),
		..RetentionOutcome::default()
	};
	let mut pairs: BTreeMap<Identity, (Option<usize>, Option<usize>)> = BTreeMap::new();

	for (idx, row) in stored_rows.iter().enumerate() {
		let Some(table_object_id) = row.table_object_id else {
			outcome.ambiguous_retained += 1;
			continue;
		};
		if row.is_tombstone {
			pairs.entry(row.key_identity).or_default().1 = Some(idx);
		} else {
			pairs.entry(row.key_identity).or_default().0 = Some(idx);
		}
		let effective = effective_table_retention(
			&cluster,
			catalog.retention_policy(tenant, table_object_id),
			NOW_MS,
		);
		if !row_visible_under_retention(row.row_hlc, &effective) {
			reclaim[idx] = true;
		}
	}

	for (data_idx, tombstone_idx) in pairs.values().copied() {
		let (Some(data_idx), Some(tombstone_idx)) = (data_idx, tombstone_idx) else {
			continue;
		};
		let tombstone = stored_rows[tombstone_idx];
		let Some(table_object_id) = tombstone.table_object_id else {
			continue;
		};
		let effective = effective_table_retention(
			&cluster,
			catalog.retention_policy(tenant, table_object_id),
			NOW_MS,
		);
		let Some(floor) = effective.floor else {
			reclaim[data_idx] = false;
			reclaim[tombstone_idx] = false;
			continue;
		};
		if tombstone.row_hlc < floor {
			reclaim[data_idx] = true;
			reclaim[tombstone_idx] = true;
			outcome.pair_drops += 1;
		} else if reclaim[data_idx] {
			reclaim[data_idx] = false;
		}
	}

	for (row, reclaim) in stored_rows.iter().zip(reclaim.iter()) {
		if *reclaim {
			if row.is_tombstone {
				outcome.tombstone_pruned += 1;
			} else {
				outcome.data_pruned += 1;
			}
			outcome.reclaimed_bytes = outcome.reclaimed_bytes.saturating_add(APPROX_ROW_BYTES);
		} else {
			outcome.survivor_rows += 1;
		}
	}

	(reclaim, outcome)
}

#[derive(Clone, Copy, Debug)]
enum OwnershipMode {
	Known,
	AmbiguousEvery(usize),
}

fn append_rows(
	tenant: TenantId,
	tables: &[TableObjectId],
	rows: usize,
	density_percent: u8,
	ownership: OwnershipMode,
) -> Result<Vec<BenchRow>, Box<dyn std::error::Error>> {
	let writer = WriterNodeId::new([0x48; 16]);
	let mut out = Vec::with_capacity(rows + tombstone_count(rows, density_percent));
	for row in 0..rows {
		let table = tables[row % tables.len()];
		let age_ms = match row % 6 {
			0 => 120 * DAY_MS,
			1 => 95 * DAY_MS,
			2 => 75 * DAY_MS,
			3 => 45 * DAY_MS,
			4 => 15 * DAY_MS,
			_ => 5 * DAY_MS,
		};
		let row_hlc = hlc(NOW_MS.saturating_sub(age_ms), 0, 48);
		let key_identity = synthesize_append_only_row_id(
			tenant,
			table,
			writer,
			row_hlc,
			IntraBatchIndex::try_new(row as u32)?,
		);
		let table_object_id = table_for_ownership(table, row, ownership);
		push_row(&mut out, key_identity, row_hlc, false, table_object_id);

		if is_tombstoned(row, density_percent) {
			let delete_age_ms = if row % 2 == 0 {
				120 * DAY_MS
			} else {
				15 * DAY_MS
			};
			push_row(
				&mut out,
				key_identity,
				hlc(NOW_MS.saturating_sub(delete_age_ms), 0, 48),
				true,
				table_object_id,
			);
		}
	}
	Ok(out)
}

fn constant_age_rows(
	tenant: TenantId,
	table: TableObjectId,
	rows: usize,
	age_ms: u64,
	density_percent: u8,
	ownership: OwnershipMode,
) -> Result<Vec<BenchRow>, Box<dyn std::error::Error>> {
	let writer = WriterNodeId::new([0x48; 16]);
	let mut out = Vec::with_capacity(rows + tombstone_count(rows, density_percent));
	for row in 0..rows {
		let row_hlc = hlc(NOW_MS.saturating_sub(age_ms), 0, 48);
		let key_identity = synthesize_append_only_row_id(
			tenant,
			table,
			writer,
			row_hlc,
			IntraBatchIndex::try_new(row as u32)?,
		);
		let table_object_id = table_for_ownership(table, row, ownership);
		push_row(&mut out, key_identity, row_hlc, false, table_object_id);
		if is_tombstoned(row, density_percent) {
			push_row(
				&mut out,
				key_identity,
				hlc(NOW_MS.saturating_sub(age_ms / 2), 0, 48),
				true,
				table_object_id,
			);
		}
	}
	Ok(out)
}

fn pair_rows(
	tenant: TenantId,
	table: TableObjectId,
	rows: usize,
) -> Result<Vec<(HybridTimestamp, HybridTimestamp)>, Box<dyn std::error::Error>> {
	let writer = WriterNodeId::new([0x48; 16]);
	let mut out = Vec::with_capacity(rows);
	for row in 0..rows {
		let data_hlc = hlc(NOW_MS.saturating_sub(120 * DAY_MS), 0, 48);
		let _identity = synthesize_append_only_row_id(
			tenant,
			table,
			writer,
			data_hlc,
			IntraBatchIndex::try_new(row as u32)?,
		);
		let delete_age_ms = match row % 3 {
			0 => 120 * DAY_MS,
			1 => 45 * DAY_MS,
			_ => 15 * DAY_MS,
		};
		out.push((data_hlc, hlc(NOW_MS.saturating_sub(delete_age_ms), 0, 48)));
	}
	Ok(out)
}

fn pk_version_chains(rows: usize) -> Vec<[HybridTimestamp; 3]> {
	(0..rows)
		.map(|row| {
			let jitter = (row % 10) as u64;
			[
				hlc(NOW_MS.saturating_sub((120 + jitter) * DAY_MS), 0, 48),
				hlc(NOW_MS.saturating_sub((90 + jitter) * DAY_MS), 0, 48),
				hlc(NOW_MS.saturating_sub((20 + jitter) * DAY_MS), 0, 48),
			]
		})
		.collect()
}

fn table_for_ownership(
	table: TableObjectId,
	row: usize,
	ownership: OwnershipMode,
) -> Option<TableObjectId> {
	match ownership {
		OwnershipMode::Known => Some(table),
		OwnershipMode::AmbiguousEvery(stride) if stride > 0 && row.is_multiple_of(stride) => None,
		OwnershipMode::AmbiguousEvery(_) => Some(table),
	}
}

fn push_row(
	rows: &mut Vec<BenchRow>,
	key_identity: Identity,
	row_hlc: HybridTimestamp,
	is_tombstone: bool,
	table_object_id: Option<TableObjectId>,
) {
	let source_row_index = rows.len();
	rows.push(BenchRow {
		source_row_index,
		key_identity,
		routing_locality: Locality::from(key_identity),
		row_hlc,
		is_tombstone,
		table_object_id,
	});
}

fn source_row(row: BenchRow) -> MultiRowSourceRow {
	MultiRowSourceRow {
		source_row_index: row.source_row_index,
		key_identity: row.key_identity,
		routing_locality: row.routing_locality,
		row_hlc: row.row_hlc,
		is_tombstone: row.is_tombstone,
		// A raw-locality row: the bench prices retention over legacy frames.
		partition: None,
		table_object_id: row.table_object_id,
	}
}

fn user_batch(rows: usize) -> std::io::Result<RecordBatch> {
	let values = (0..rows).map(|row| row as u64).collect::<Vec<_>>();
	RecordBatch::try_new(
		Arc::new(Schema::new(vec![Field::new(
			"value",
			DataType::UInt64,
			false,
		)])),
		vec![Arc::new(UInt64Array::from(values)) as ArrayRef],
	)
	.map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))
}

fn csb_retention_decision(
	min_hlc: HybridTimestamp,
	max_hlc: HybridTimestamp,
	effective: &wendelon::storage::EffectiveRetention,
) -> CsbDecision {
	let Some(floor) = effective.floor else {
		return CsbDecision::Direct;
	};
	if min_hlc >= floor {
		CsbDecision::Direct
	} else if max_hlc < floor {
		CsbDecision::Zero
	} else {
		CsbDecision::RowFilter
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

fn ns_per_item(elapsed: Duration, items: usize) -> u128 {
	elapsed.as_nanos() / items.max(1) as u128
}

fn items_per_sec(elapsed: Duration, items: usize) -> f64 {
	items as f64 / elapsed.as_secs_f64().max(f64::EPSILON)
}
