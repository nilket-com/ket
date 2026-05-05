//! ADR 054 CDC subscription substrate benchmark.
//!
//! `--quick` keeps the run small for smoke checks:
//! `cargo bench --bench cdc_subscription -- --quick`.
//!
//! `--full` runs the small / medium / large promotion-evidence
//! matrix. It still needs to be run on representative hardware
//! before ADR 054 can be called shipped.
//!
//! Add `--acceptance-one-way-quic-rtt-micros N` to emit pass/fail
//! threshold lines against the measured one-way QUIC RTT for the
//! representative environment.

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::runtime::Builder;
use wendelon::metrics::NetworkCompressionPath;
use wendelon::network::cdc::{CdcSubscriptionRegistry, cdc_emit_checkpoint, cdc_emit_frame};
use wendelon::network::ipc::wire_encode_batch_for_path;
use wendelon::schema::{
	CdcCheckpointMeta, CdcCursor, CdcDigestDomain, CdcFrameMeta, CdcFrameSource, CdcSubscribeBound,
	CdcSubscriptionId, HybridTimestamp, Identity, Locality, RecordRowIndex, RowCount,
	TableObjectId, TenantId, cdc_cursor_in_catchup_window, cdc_visible_rows_digest,
	default_kv_table_descriptor_v1,
};
use wendelon::storage::{
	CdcFinalizationObserver, CdcLiveEntry, CdcLiveTailRing, CdcLocalityFrontiers,
};

#[derive(Clone, Copy, Debug)]
struct BenchConfig {
	rows: usize,
	iters: usize,
}

#[derive(Clone, Copy, Debug)]
enum BenchPlan {
	Single(BenchConfig),
	Matrix(&'static [BenchConfig]),
}

#[derive(Clone, Copy, Debug)]
struct BenchRun {
	plan: BenchPlan,
	acceptance_one_way_quic_rtt_micros: Option<u128>,
}

#[derive(Clone, Copy, Debug)]
struct BenchSuiteMetrics {
	live_tail_p99_nanos: u128,
	max_fanout_p99_nanos: u128,
	catchup_decoded_mib_per_sec: f64,
	ring_overflow: RingOverflowBenchResult,
	admission_isolation: AdmissionIsolationBenchResult,
}

#[derive(Clone, Copy, Debug)]
struct LatencyBenchResult {
	p99_nanos: u128,
}

#[derive(Clone, Copy, Debug)]
struct FanoutBenchResult {
	p99_nanos: u128,
}

#[derive(Clone, Copy, Debug)]
struct RingOverflowBenchResult {
	first_overflow_at: Option<usize>,
	evicted: usize,
	frontier_present: bool,
}

#[derive(Clone, Copy, Debug)]
struct AdmissionIsolationBenchResult {
	baseline: AdmissionBenchResult,
	with_100: AdmissionBenchResult,
	regression_percent: Option<f64>,
}

const FULL_MATRIX: &[BenchConfig] = &[
	BenchConfig {
		rows: 1_000,
		iters: 10,
	},
	BenchConfig {
		rows: 10_000,
		iters: 10,
	},
	BenchConfig {
		rows: 100_000,
		iters: 5,
	},
];

impl BenchRun {
	fn parse() -> Self {
		let mut config = BenchConfig {
			rows: 4096,
			iters: 5,
		};
		let mut matrix = false;
		let mut acceptance_one_way_quic_rtt_micros = None;
		let mut args = std::env::args().skip(1);
		while let Some(arg) = args.next() {
			match arg.as_str() {
				"--quick" => {
					config.rows = 1024;
					config.iters = 2;
				}
				"--full" => {
					matrix = true;
				}
				"--rows" => {
					config.rows = args
						.next()
						.expect("--rows requires a value")
						.parse()
						.expect("--rows must be usize");
				}
				"--iters" => {
					config.iters = args
						.next()
						.expect("--iters requires a value")
						.parse()
						.expect("--iters must be usize");
				}
				"--acceptance-one-way-quic-rtt-micros" => {
					acceptance_one_way_quic_rtt_micros = Some(
						args.next()
							.expect("--acceptance-one-way-quic-rtt-micros requires a value")
							.parse()
							.expect("--acceptance-one-way-quic-rtt-micros must be u128"),
					);
				}
				"--bench" => {}
				_ => {}
			}
		}
		let plan = if matrix {
			BenchPlan::Matrix(FULL_MATRIX)
		} else {
			BenchPlan::Single(config)
		};
		Self {
			plan,
			acceptance_one_way_quic_rtt_micros,
		}
	}
}

impl BenchConfig {
	fn fanout_subscribers(self) -> &'static [usize] {
		if self.rows <= 1024 {
			&[1, 10, 100]
		} else {
			&[1, 10, 100, 1000]
		}
	}
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
	let run = BenchRun::parse();
	let runtime = Builder::new_current_thread().enable_all().build()?;
	runtime.block_on(async {
		let mut acceptance_passed = true;
		match run.plan {
			BenchPlan::Single(config) => {
				let metrics = run_bench_suite(config).await?;
				if let Some(one_way_quic_rtt_micros) = run.acceptance_one_way_quic_rtt_micros {
					acceptance_passed &= metrics.report_acceptance(config, one_way_quic_rtt_micros);
				}
			}
			BenchPlan::Matrix(configs) => {
				for config in configs {
					println!(
						"cdc_bench_matrix_case rows={} iters={}",
						config.rows, config.iters
					);
					let metrics = run_bench_suite(*config).await?;
					if let Some(one_way_quic_rtt_micros) = run.acceptance_one_way_quic_rtt_micros {
						acceptance_passed &=
							metrics.report_acceptance(*config, one_way_quic_rtt_micros);
					}
				}
			}
		}
		if !acceptance_passed {
			return Err(std::io::Error::other("CDC acceptance thresholds failed"));
		}
		Ok::<(), std::io::Error>(())
	})?;
	Ok(())
}

async fn run_bench_suite(config: BenchConfig) -> std::io::Result<BenchSuiteMetrics> {
	let live_tail = bench_live_tail_latency(config).await;
	let fanout = bench_live_tail_fanout(config).await;
	let catchup_decoded_mib_per_sec = bench_catchup_throughput(config);
	let ring_overflow = bench_ring_overflow_threshold(config).await;
	bench_checkpoint_cost(config).await?;
	let admission_isolation = bench_admission_isolation(config).await;
	bench_schema_fingerprint_change(config);
	bench_wire_compression(config)?;
	bench_ring_stage_finalize(config).await;
	bench_stage_release_drain_decomposition(config).await;
	bench_safe_emission_watermark_hold(config).await;
	bench_frame_encode(config).await?;
	Ok(BenchSuiteMetrics {
		live_tail_p99_nanos: live_tail.p99_nanos,
		max_fanout_p99_nanos: fanout
			.iter()
			.map(|result| result.p99_nanos)
			.max()
			.unwrap_or(0),
		catchup_decoded_mib_per_sec,
		ring_overflow,
		admission_isolation,
	})
}

impl BenchSuiteMetrics {
	fn report_acceptance(self, config: BenchConfig, one_way_quic_rtt_micros: u128) -> bool {
		let threshold_nanos = one_way_quic_rtt_micros
			.saturating_mul(1_000)
			.saturating_mul(5);
		let observed_live_tail_p99_nanos = self.live_tail_p99_nanos.max(self.max_fanout_p99_nanos);
		let live_tail_pass = observed_live_tail_p99_nanos <= threshold_nanos;
		let catchup_pass = self.catchup_decoded_mib_per_sec >= 100.0;
		let ring_overflow_pass = self.ring_overflow.first_overflow_at.is_some()
			&& self.ring_overflow.evicted > 0
			&& self.ring_overflow.frontier_present;
		let admission_pass = self
			.admission_isolation
			.regression_percent
			.map(|regression| regression <= 5.0)
			.unwrap_or(false);

		println!(
			"cdc_acceptance_live_tail_latency rows={} iters={} one_way_quic_rtt_micros={} threshold_nanos={} observed_p99_nanos={} status={}",
			config.rows,
			config.iters,
			one_way_quic_rtt_micros,
			threshold_nanos,
			observed_live_tail_p99_nanos,
			status(live_tail_pass)
		);
		println!(
			"cdc_acceptance_catchup_throughput rows={} iters={} threshold_mib_per_sec=100.00 observed_mib_per_sec={:.2} status={}",
			config.rows,
			config.iters,
			self.catchup_decoded_mib_per_sec,
			status(catchup_pass)
		);
		println!(
			"cdc_acceptance_ring_overflow_nonblocking rows={} iters={} first_overflow_at={} evicted={} frontier_present={} status={}",
			config.rows,
			config.iters,
			self.ring_overflow.first_overflow_at.unwrap_or(0),
			self.ring_overflow.evicted,
			self.ring_overflow.frontier_present,
			status(ring_overflow_pass)
		);
		println!(
			"cdc_acceptance_admission_isolation rows={} iters={} threshold_percent=5.00 observed_percent={} p99_0_subs_nanos={} p99_100_subs_nanos={} status={}",
			config.rows,
			config.iters,
			regression_percent_label(self.admission_isolation.regression_percent),
			self.admission_isolation.baseline.p99_nanos,
			self.admission_isolation.with_100.p99_nanos,
			status(admission_pass)
		);

		live_tail_pass && catchup_pass && ring_overflow_pass && admission_pass
	}
}

async fn bench_live_tail_latency(config: BenchConfig) -> LatencyBenchResult {
	let tenant_id = TenantId::new(1);
	let table_object_id = TableObjectId::new([2; 16]);
	let registry = CdcSubscriptionRegistry::new();
	let (handle, mut recv) = registry.register(
		tenant_id,
		table_object_id,
		CdcCursor::MIN,
		CdcSubscribeBound::Live,
		default_kv_table_descriptor_v1()
			.unwrap()
			.logical_schema_fingerprint,
	);
	let mut samples = Vec::with_capacity(config.rows * config.iters);
	for iter in 0..config.iters {
		for row in 0..config.rows {
			let entry = Arc::new(entry(cursor(iter as u64, row as u64), row as i64));
			let started = Instant::now();
			entry.mark_finalized();
			let outcome = registry.fanout_finalized_entry(entry.clone());
			assert_eq!(outcome.sent, 1);
			let received = recv.recv().await.expect("cdc live-tail receiver open");
			handle.cursor_high_water.advance_max(received.cursor);
			samples.push(started.elapsed().as_nanos());
		}
	}
	let p50_micros = percentile_micros(&mut samples, 50);
	let p95_micros = percentile_micros(&mut samples, 95);
	let p99_micros = percentile_micros(&mut samples, 99);
	let p99_nanos = percentile_nanos(&mut samples, 99);
	println!(
		"cdc_live_tail_latency rows={} iters={} p50_micros={} p95_micros={} p99_micros={} p99_nanos={}",
		config.rows, config.iters, p50_micros, p95_micros, p99_micros, p99_nanos
	);
	LatencyBenchResult { p99_nanos }
}

async fn bench_live_tail_fanout(config: BenchConfig) -> Vec<FanoutBenchResult> {
	let mut results = Vec::new();
	for &subscribers in config.fanout_subscribers() {
		let tenant_id = TenantId::new(1);
		let table_object_id = TableObjectId::new([2; 16]);
		let registry = CdcSubscriptionRegistry::new();
		let mut receivers = Vec::with_capacity(subscribers);
		for _ in 0..subscribers {
			let (_, recv) = registry.register(
				tenant_id,
				table_object_id,
				CdcCursor::MIN,
				CdcSubscribeBound::Live,
				default_kv_table_descriptor_v1()
					.unwrap()
					.logical_schema_fingerprint,
			);
			receivers.push(recv);
		}
		let mut samples = Vec::with_capacity(config.rows * config.iters);
		for iter in 0..config.iters {
			for row in 0..config.rows {
				let entry = Arc::new(
					entry(cursor(iter as u64, row as u64), row as i64).with_finalized(true),
				);
				let started = Instant::now();
				let outcome = registry.fanout_finalized_entry(entry);
				assert_eq!(outcome.sent, subscribers);
				for recv in &mut receivers {
					let _ = recv.try_recv().expect("cdc fanout receiver drained");
				}
				samples.push(started.elapsed().as_nanos());
			}
		}
		let p99_micros = percentile_micros(&mut samples, 99);
		let p99_nanos = percentile_nanos(&mut samples, 99);
		println!(
			"cdc_live_tail_fanout subscribers={} rows={} iters={} p99_micros={} p99_nanos={}",
			subscribers, config.rows, config.iters, p99_micros, p99_nanos
		);
		results.push(FanoutBenchResult { p99_nanos });
	}
	results
}

fn bench_catchup_throughput(config: BenchConfig) -> f64 {
	let cursors: Vec<_> = (0..config.rows as u64).map(|row| cursor(0, row)).collect();
	let start_cursor = cursor(0, (config.rows / 4) as u64);
	let upper_cursor = cursor(0, (config.rows - config.rows / 4) as u64);
	let started = Instant::now();
	let mut matched = 0usize;
	for _ in 0..config.iters {
		matched += cursors
			.iter()
			.filter(|cursor| cdc_cursor_in_catchup_window(**cursor, start_cursor, upper_cursor))
			.count();
	}
	let elapsed = started.elapsed();
	let synthetic_bytes = matched.saturating_mul(std::mem::size_of::<CdcCursor>());
	let decoded_mib_per_sec = mib_per_second(synthetic_bytes, elapsed);
	println!(
		"cdc_catchup_throughput rows={} iters={} matched={} decoded_mib_per_sec={:.2}",
		config.rows, config.iters, matched, decoded_mib_per_sec
	);
	decoded_mib_per_sec
}

async fn bench_ring_overflow_threshold(config: BenchConfig) -> RingOverflowBenchResult {
	let cap = entry(cursor(0, 0), 0)
		.estimated_bytes()
		.saturating_mul(32)
		.max(1);
	let ring = CdcLiveTailRing::new(Locality::new(1), cap);
	let mut first_overflow_at = None;
	let mut evicted = 0usize;
	for row in 0..config.rows {
		let evictions = ring.push(entry(cursor(0, row as u64), row as i64)).await;
		if !evictions.is_empty() && first_overflow_at.is_none() {
			first_overflow_at = Some(row + 1);
		}
		evicted += evictions.len();
	}
	let frontier = ring
		.evicted_cursor_frontier(TenantId::new(1), TableObjectId::new([2; 16]))
		.await;
	let frontier_present = frontier.is_some();
	println!(
		"cdc_ring_overflow_threshold cap_bytes={} rows={} first_overflow_at={} evicted={} frontier_present={}",
		cap,
		config.rows,
		first_overflow_at.unwrap_or(0),
		evicted,
		frontier_present
	);
	RingOverflowBenchResult {
		first_overflow_at,
		evicted,
		frontier_present,
	}
}

async fn bench_checkpoint_cost(config: BenchConfig) -> std::io::Result<()> {
	let tenant_id = TenantId::new(1);
	let table_object_id = TableObjectId::new([2; 16]);
	let row_keys: Vec<_> = (0..config.rows as u64)
		.map(|row| Identity::new([row as u8; 32]))
		.collect();
	let mut total = Duration::ZERO;
	for iter in 0..config.iters {
		let (_client, mut server) = tokio::io::duplex(64 * 1024);
		let started = Instant::now();
		let digest = cdc_visible_rows_digest(tenant_id, table_object_id, row_keys.iter().copied());
		let checkpoint = CdcCheckpointMeta {
			subscription_id: CdcSubscriptionId::new(1),
			cursor: cursor(iter as u64, config.rows as u64),
			visible_state_digest: digest,
			digest_domain_raw: CdcDigestDomain::TableRowVisible.as_raw(),
			_pad: [0; 4],
		};
		cdc_emit_checkpoint(&mut server, 0, &checkpoint, CdcFrameSource::Ring).await?;
		total += started.elapsed();
	}
	println!(
		"cdc_checkpoint_cost rows={} iters={} avg_micros={}",
		config.rows,
		config.iters,
		total.as_micros() / config.iters as u128
	);
	Ok(())
}

async fn bench_admission_isolation(config: BenchConfig) -> AdmissionIsolationBenchResult {
	let baseline = bench_admission_for_subscribers(config, 0).await;
	let with_100 = bench_admission_for_subscribers(config, 100).await;
	let regression_percent = if baseline.p99_nanos == 0 {
		None
	} else {
		Some(
			((with_100.p99_nanos as f64 - baseline.p99_nanos as f64) / baseline.p99_nanos as f64)
				* 100.0,
		)
	};
	println!(
		"cdc_admission_isolation rows={} iters={} p99_0_subs_micros={} p99_100_subs_micros={} p99_0_subs_nanos={} p99_100_subs_nanos={} regression_percent={}",
		config.rows,
		config.iters,
		baseline.p99_micros,
		with_100.p99_micros,
		baseline.p99_nanos,
		with_100.p99_nanos,
		regression_percent_label(regression_percent)
	);
	AdmissionIsolationBenchResult {
		baseline,
		with_100,
		regression_percent,
	}
}

#[derive(Clone, Copy, Debug)]
struct AdmissionBenchResult {
	p99_micros: u128,
	p99_nanos: u128,
}

async fn bench_admission_for_subscribers(
	config: BenchConfig,
	subscribers: usize,
) -> AdmissionBenchResult {
	let tenant_id = TenantId::new(1);
	let table_object_id = TableObjectId::new([2; 16]);
	let registry = CdcSubscriptionRegistry::new();
	let ring = CdcLiveTailRing::new(Locality::new(1), 256 * 1024 * 1024);
	for _ in 0..subscribers {
		let (_, recv) = registry.register(
			tenant_id,
			table_object_id,
			CdcCursor::MIN,
			CdcSubscribeBound::Live,
			default_kv_table_descriptor_v1()
				.unwrap()
				.logical_schema_fingerprint,
		);
		black_box(recv);
	}
	assert_eq!(
		registry.active_count(tenant_id, table_object_id),
		subscribers
	);
	let mut samples = Vec::with_capacity(config.rows * config.iters);
	for iter in 0..config.iters {
		for row in 0..config.rows {
			let entry =
				Arc::new(entry(cursor(iter as u64, row as u64), row as i64).with_finalized(true));
			let started = Instant::now();
			ring.push_arc(entry).await;
			samples.push(started.elapsed().as_nanos());
		}
	}
	AdmissionBenchResult {
		p99_micros: percentile_micros(&mut samples, 99),
		p99_nanos: percentile_nanos(&mut samples, 99),
	}
}

fn bench_schema_fingerprint_change(config: BenchConfig) {
	let a = [1; 32];
	let b = [2; 32];
	let mut current = a;
	let mut changes = 0usize;
	let started = Instant::now();
	for row in 0..(config.rows * config.iters) {
		let incoming = if row % 257 == 0 { b } else { a };
		if incoming != current {
			current = incoming;
			changes += 1;
		}
		black_box(current);
	}
	println!(
		"cdc_schema_fingerprint_change rows={} iters={} changes={} avg_nanos={}",
		config.rows,
		config.iters,
		changes,
		started.elapsed().as_nanos() / (config.rows * config.iters) as u128
	);
}

fn bench_wire_compression(config: BenchConfig) -> std::io::Result<()> {
	let batch = bench_batch(config.rows)?;
	let mut total = Duration::ZERO;
	let mut encoded_bytes = 0usize;
	for _ in 0..config.iters {
		let started = Instant::now();
		let encoded = wire_encode_batch_for_path(&batch, NetworkCompressionPath::CdcEvent)?;
		total += started.elapsed();
		encoded_bytes = encoded.len();
		black_box(encoded);
	}
	println!(
		"cdc_wire_compression rows={} iters={} encoded_bytes={} avg_micros={}",
		config.rows,
		config.iters,
		encoded_bytes,
		total.as_micros() / config.iters as u128
	);
	Ok(())
}

async fn bench_ring_stage_finalize(config: BenchConfig) {
	let ring = Arc::new(CdcLiveTailRing::new(Locality::new(1), 256 * 1024 * 1024));
	let observer = CdcFinalizationObserver::new(ring.clone());
	let mut total = Duration::ZERO;
	for iter in 0..config.iters {
		let started = Instant::now();
		for row in 0..config.rows {
			let cursor = cursor(iter as u64, row as u64);
			ring.push(entry(cursor, row as i64)).await;
			observer.stage(row as u64, cursor).await;
			black_box(observer.on_finalized(row as u64).await);
			let _ = ring.evict_cursor(cursor).await;
		}
		total += started.elapsed();
	}
	println!(
		"cdc_ring_stage_finalize rows={} iters={} avg_micros={}",
		config.rows,
		config.iters,
		total.as_micros() / config.iters as u128
	);
}

async fn bench_stage_release_drain_decomposition(config: BenchConfig) {
	let tenant_id = TenantId::new(1);
	let table_object_id = TableObjectId::new([2; 16]);
	let ring = Arc::new(CdcLiveTailRing::new(Locality::new(1), 256 * 1024 * 1024));
	let observer = CdcFinalizationObserver::new(ring.clone());
	let registry = CdcSubscriptionRegistry::new();
	let (handle, mut recv) = registry.register(
		tenant_id,
		table_object_id,
		CdcCursor::MIN,
		CdcSubscribeBound::Live,
		default_kv_table_descriptor_v1()
			.unwrap()
			.logical_schema_fingerprint,
	);
	let mut push_total = Duration::ZERO;
	let mut stage_total = Duration::ZERO;
	let mut release_total = Duration::ZERO;
	let mut drain_total = Duration::ZERO;
	for iter in 0..config.iters {
		for row in 0..config.rows {
			let cursor = cursor(iter as u64, row as u64);
			let started = Instant::now();
			ring.push(entry(cursor, row as i64)).await;
			push_total += started.elapsed();

			let started = Instant::now();
			observer.stage(row as u64, cursor).await;
			stage_total += started.elapsed();

			let started = Instant::now();
			black_box(observer.on_finalized(row as u64).await);
			release_total += started.elapsed();

			let started = Instant::now();
			for entry in ring
				.eligible_entries_after(
					tenant_id,
					table_object_id,
					handle.cursor_high_water.load(),
					CdcCursor::MAX.row_hlc,
					1,
				)
				.await
			{
				let outcome = registry.fanout_finalized_entry(entry);
				assert_eq!(outcome.sent, 1);
				let received = recv.recv().await.expect("cdc drain receiver open");
				handle.cursor_high_water.advance_max(received.cursor);
				let _ = ring.evict_cursor(received.cursor).await;
			}
			drain_total += started.elapsed();
		}
	}
	let samples = (config.rows * config.iters) as u128;
	println!(
		"cdc_stage_release_drain rows={} iters={} push_avg_nanos={} stage_avg_nanos={} release_avg_nanos={} drain_avg_nanos={}",
		config.rows,
		config.iters,
		push_total.as_nanos() / samples,
		stage_total.as_nanos() / samples,
		release_total.as_nanos() / samples,
		drain_total.as_nanos() / samples
	);
}

async fn bench_safe_emission_watermark_hold(config: BenchConfig) {
	let tenant_id = TenantId::new(1);
	let table_object_id = TableObjectId::new([2; 16]);
	let frontiers = CdcLocalityFrontiers::new();
	let locality_a = Locality::new(1);
	let locality_b = Locality::new(2);
	let leading_frontier = 60 + config.rows as u64 + 10;
	frontiers.advance(locality_a, hlc(leading_frontier));
	frontiers.advance(locality_b, hlc(50));
	let ring = CdcLiveTailRing::new(locality_a, 256 * 1024 * 1024);
	for row in 0..config.rows {
		ring.push(
			entry(cursor_at_ms(60 + row as u64, row as u64), row as i64).with_finalized(true),
		)
		.await;
	}
	let skewed_safe = frontiers
		.min_for_localities([locality_a, locality_b])
		.expect("frontiers seeded");
	let blocked = ring
		.eligible_entries_after(
			tenant_id,
			table_object_id,
			CdcCursor::MIN,
			skewed_safe,
			usize::MAX,
		)
		.await
		.len();
	frontiers.advance(locality_b, hlc(leading_frontier));
	let heartbeat_safe = frontiers
		.min_for_localities([locality_a, locality_b])
		.expect("frontiers advanced");
	let released = ring
		.eligible_entries_after(
			tenant_id,
			table_object_id,
			CdcCursor::MIN,
			heartbeat_safe,
			usize::MAX,
		)
		.await
		.len();
	println!(
		"cdc_safe_emission_watermark_hold rows={} initial_safe_ms={} blocked_before_heartbeat={} released_after_heartbeat={}",
		config.rows,
		skewed_safe.physical_ms,
		config.rows.saturating_sub(blocked),
		released
	);
}

async fn bench_frame_encode(config: BenchConfig) -> std::io::Result<()> {
	let batch = bench_batch(config.rows)?;
	let meta = CdcFrameMeta {
		subscription_id: CdcSubscriptionId::new(1),
		frame_low_cursor: cursor(0, 0),
		frame_high_cursor: cursor(0, config.rows as u64),
		logical_schema_fingerprint: [7; 32],
		event_count: RowCount::new(config.rows as u64),
		source_raw: CdcFrameSource::Ring.as_raw(),
		_pad: [0; 7],
	};
	let mut total = Duration::ZERO;
	for _ in 0..config.iters {
		let (_client, mut server) = tokio::io::duplex(128 * 1024 * 1024);
		let started = Instant::now();
		cdc_emit_frame(&mut server, 0, &meta, &batch, 0).await?;
		total += started.elapsed();
	}
	println!(
		"cdc_frame_encode rows={} iters={} avg_micros={}",
		config.rows,
		config.iters,
		total.as_micros() / config.iters as u128
	);
	Ok(())
}

fn percentile_micros(samples: &mut [u128], percentile: usize) -> u128 {
	percentile_nanos(samples, percentile) / 1_000
}

fn percentile_nanos(samples: &mut [u128], percentile: usize) -> u128 {
	if samples.is_empty() {
		return 0;
	}
	samples.sort_unstable();
	let index = ((samples.len() - 1) * percentile) / 100;
	samples[index]
}

fn mib_per_second(bytes: usize, elapsed: Duration) -> f64 {
	let seconds = elapsed.as_secs_f64();
	if seconds == 0.0 {
		return 0.0;
	}
	(bytes as f64 / 1_048_576.0) / seconds
}

fn status(pass: bool) -> &'static str {
	if pass { "pass" } else { "fail" }
}

fn regression_percent_label(value: Option<f64>) -> String {
	value
		.map(|regression| format!("{regression:.2}"))
		.unwrap_or_else(|| "resolution_limited".to_string())
}

fn bench_batch(rows: usize) -> std::io::Result<RecordBatch> {
	let schema = Arc::new(Schema::new(vec![Field::new(
		"value",
		DataType::Int64,
		false,
	)]));
	let values: Vec<i64> = (0..rows).map(|row| row as i64).collect();
	RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))])
		.map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))
}

fn entry(cursor: CdcCursor, value: i64) -> CdcLiveEntry {
	let schema = Arc::new(Schema::new(vec![Field::new(
		"value",
		DataType::Int64,
		false,
	)]));
	let batch =
		RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![value]))]).unwrap();
	CdcLiveEntry::new(
		cursor,
		TenantId::new(1),
		TableObjectId::new([2; 16]),
		default_kv_table_descriptor_v1()
			.unwrap()
			.logical_schema_fingerprint,
		Arc::new(batch),
	)
}

fn cursor(iter: u64, row: u64) -> CdcCursor {
	CdcCursor::new(
		hlc_with_logical(1_700_000_000_000 + iter, row as u32),
		Identity::new([row as u8; 32]),
		RecordRowIndex::ZERO,
	)
}

fn cursor_at_ms(ms: u64, row: u64) -> CdcCursor {
	CdcCursor::new(
		hlc(ms),
		Identity::new([row as u8; 32]),
		RecordRowIndex::ZERO,
	)
}

fn hlc(ms: u64) -> HybridTimestamp {
	hlc_with_logical(ms, 0)
}

fn hlc_with_logical(ms: u64, logical: u32) -> HybridTimestamp {
	HybridTimestamp {
		physical_ms: ms,
		logical,
		node_id: 1,
	}
}
