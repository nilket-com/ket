use crate::schema::WriteStatus;
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

const MICROS_PER_SECOND: u64 = 1_000_000;

const GROUP_COMMIT_WINDOW_BUCKETS_MICROS: &[u64] =
	&[500, 1_000, 2_000, 5_000, 10_000, 25_000, 50_000];
const LOCAL_READ_LATENCY_BUCKETS_MICROS: &[u64] =
	&[500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000];
const L2_RANGE_READ_LATENCY_BUCKETS_MICROS: &[u64] = &[
	1_000, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 500_000, 1_000_000,
];
const L2_UPLOAD_LATENCY_BUCKETS_MICROS: &[u64] = &[
	10_000, 25_000, 50_000, 100_000, 250_000, 500_000, 1_000_000, 2_500_000, 5_000_000,
];
const DURABLE_QUORUM_LATENCY_BUCKETS_MICROS: &[u64] = &[
	500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 500_000, 1_000_000,
	2_500_000, 5_000_000,
];
const PENDING_FINALIZATION_REPLAY_LATENCY_BUCKETS_MICROS: &[u64] = &[
	500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 500_000, 1_000_000,
	2_500_000, 5_000_000,
];
const MIGRATION_PROXY_READ_LATENCY_BUCKETS_MICROS: &[u64] = &[
	500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 500_000, 1_000_000,
	2_500_000, 5_000_000,
];
const WAL_APPEND_LATENCY_BUCKETS_MICROS: &[u64] = &[
	100, 250, 500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 500_000,
];
const WAL_FSYNC_LATENCY_BUCKETS_MICROS: &[u64] = &[
	100, 250, 500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 500_000,
	1_000_000,
];
const PBC_EXEC_BUCKETS_MICROS: &[u64] = &[
	25, 50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000,
];
const PBC_LOCALITY_BUCKETS_MICROS: &[u64] = &[
	10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000,
];
const PBC_SLAB_STATS_BUCKETS_MICROS: &[u64] = &[
	10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000,
];
const PBC_SLAB_STATS_BLOCK_BYTES_BUCKETS: &[u64] =
	&[1_024, 4_096, 16_384, 65_536, 262_144, 1_048_576, 4_194_304];

pub fn global() -> &'static MetricsRegistry {
	static METRICS: OnceLock<MetricsRegistry> = OnceLock::new();
	METRICS.get_or_init(MetricsRegistry::default)
}

#[derive(Default)]
pub struct Counter(AtomicU64);

impl Counter {
	pub fn inc(&self) {
		self.0.fetch_add(1, Ordering::Relaxed);
	}

	pub fn add(&self, value: u64) {
		self.0.fetch_add(value, Ordering::Relaxed);
	}

	pub fn get(&self) -> u64 {
		self.0.load(Ordering::Relaxed)
	}
}

#[derive(Default)]
pub struct Gauge(AtomicU64);

impl Gauge {
	pub fn set(&self, value: u64) {
		self.0.store(value, Ordering::Relaxed);
	}

	pub fn get(&self) -> u64 {
		self.0.load(Ordering::Relaxed)
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PbcRuntime {
	Vectorized,
	ScalarReference,
}

impl PbcRuntime {
	fn as_str(self) -> &'static str {
		match self {
			Self::Vectorized => "vectorized",
			Self::ScalarReference => "scalar_reference",
		}
	}
}

#[derive(Default)]
struct PbcRuntimeCounters {
	vectorized: Counter,
	scalar_reference: Counter,
}

impl PbcRuntimeCounters {
	fn counter(&self, runtime: PbcRuntime) -> &Counter {
		match runtime {
			PbcRuntime::Vectorized => &self.vectorized,
			PbcRuntime::ScalarReference => &self.scalar_reference,
		}
	}

	fn add(&self, runtime: PbcRuntime, value: u64) {
		self.counter(runtime).add(value);
	}

	fn total(&self) -> u64 {
		self.vectorized.get() + self.scalar_reference.get()
	}

	fn render(&self, out: &mut String, name: &str, help: &str) {
		render_counter_family_header(out, name, help);
		for runtime in [PbcRuntime::Vectorized, PbcRuntime::ScalarReference] {
			render_counter_with_labels(
				out,
				name,
				&[("runtime", runtime.as_str())],
				self.counter(runtime).get(),
			);
		}
	}
}

struct PbcRuntimeHistograms {
	vectorized: Histogram,
	scalar_reference: Histogram,
}

impl Default for PbcRuntimeHistograms {
	fn default() -> Self {
		Self {
			vectorized: Histogram::new(PBC_EXEC_BUCKETS_MICROS),
			scalar_reference: Histogram::new(PBC_EXEC_BUCKETS_MICROS),
		}
	}
}

impl PbcRuntimeHistograms {
	fn histogram(&self, runtime: PbcRuntime) -> &Histogram {
		match runtime {
			PbcRuntime::Vectorized => &self.vectorized,
			PbcRuntime::ScalarReference => &self.scalar_reference,
		}
	}

	fn observe_duration(&self, runtime: PbcRuntime, duration: Duration) {
		self.histogram(runtime).observe_duration(duration);
	}

	fn render(&self, out: &mut String, name: &str, help: &str) {
		let _ = writeln!(out, "# HELP {name} {help}");
		let _ = writeln!(out, "# TYPE {name} histogram");
		render_histogram_micros_series(
			out,
			name,
			&[("runtime", PbcRuntime::Vectorized.as_str())],
			&self.vectorized.snapshot(),
		);
		render_histogram_micros_series(
			out,
			name,
			&[("runtime", PbcRuntime::ScalarReference.as_str())],
			&self.scalar_reference.snapshot(),
		);
	}
}

pub struct Histogram {
	bounds_micros: &'static [u64],
	state: Mutex<HistogramState>,
}

#[derive(Default)]
struct HistogramState {
	bucket_counts: Vec<u64>,
	sum_micros: u64,
	count: u64,
}

pub struct HistogramSnapshot<'a> {
	bounds_micros: &'a [u64],
	bucket_counts: Vec<u64>,
	sum_micros: u64,
	count: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PendingFinalizationReplayPath {
	DuplicateReplay,
	Reconciler,
	OrphanWitnessImport,
}

impl PendingFinalizationReplayPath {
	fn as_str(self) -> &'static str {
		match self {
			Self::DuplicateReplay => "duplicate_replay",
			Self::Reconciler => "reconciler",
			Self::OrphanWitnessImport => "orphan_witness_import",
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MigrationProxyReadOutcome {
	Hit,
	AuthoritativeMiss,
	Incomplete,
}

impl MigrationProxyReadOutcome {
	fn as_str(self) -> &'static str {
		match self {
			Self::Hit => "hit",
			Self::AuthoritativeMiss => "authoritative_miss",
			Self::Incomplete => "incomplete",
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IoUringClass {
	Generic,
	Wal,
	Slab,
}

impl IoUringClass {
	const ALL: [Self; 3] = [Self::Generic, Self::Wal, Self::Slab];

	pub fn as_str(self) -> &'static str {
		match self {
			Self::Generic => "generic",
			Self::Wal => "wal",
			Self::Slab => "slab",
		}
	}
}

#[derive(Default)]
struct IoUringQueueDepthGauges {
	generic: Gauge,
	wal: Gauge,
	slab: Gauge,
}

impl IoUringQueueDepthGauges {
	fn gauge(&self, class: IoUringClass) -> &Gauge {
		match class {
			IoUringClass::Generic => &self.generic,
			IoUringClass::Wal => &self.wal,
			IoUringClass::Slab => &self.slab,
		}
	}

	fn set(&self, class: IoUringClass, value: u64) {
		self.gauge(class).set(value);
	}

	fn render(&self, out: &mut String, name: &str, help: &str) {
		render_gauge_family_header(out, name, help);
		for class in IoUringClass::ALL {
			render_gauge_with_labels(
				out,
				name,
				&[("class", class.as_str())],
				self.gauge(class).get(),
			);
		}
	}
}

#[derive(Default)]
struct IoUringErrorCounters(Mutex<BTreeMap<(String, String, String), u64>>);

impl IoUringErrorCounters {
	fn observe(&self, class: IoUringClass, op: &str, errno: i32) {
		let mut counters = self
			.0
			.lock()
			.expect("io_uring error counter mutex poisoned");
		let key = (
			class.as_str().to_string(),
			op.to_string(),
			errno.to_string(),
		);
		*counters.entry(key).or_default() += 1;
	}

	fn render(&self, out: &mut String) {
		render_counter_family_header(
			out,
			"wendelon_io_uring_errors_total",
			"io_uring failures broken down by durability class, operation, and errno.",
		);
		for ((class, op, errno), count) in self
			.0
			.lock()
			.expect("io_uring error counter mutex poisoned")
			.iter()
		{
			render_counter_with_labels(
				out,
				"wendelon_io_uring_errors_total",
				&[
					("class", class.as_str()),
					("op", op.as_str()),
					("errno", errno.as_str()),
				],
				*count,
			);
		}
	}
}

#[derive(Default)]
struct WriteOutcomeCounters {
	ok: Counter,
	epoch_stale: Counter,
	not_leader: Counter,
	duplicate: Counter,
	proxy_limit: Counter,
	payload_too_large: Counter,
	lease_expired: Counter,
	invalid_lease_signature: Counter,
	internal_error: Counter,
	quota_exceeded: Counter,
	tenant_suspended: Counter,
	tenant_unavailable: Counter,
	authorization_failed: Counter,
	finalization_pending: Counter,
	backpressured: Counter,
	unknown: Mutex<BTreeMap<u8, u64>>,
}

impl WriteOutcomeCounters {
	fn observe(&self, status: WriteStatus) {
		match status {
			s if s == WriteStatus::OK => self.ok.inc(),
			s if s == WriteStatus::EPOCH_STALE => self.epoch_stale.inc(),
			s if s == WriteStatus::NOT_LEADER => self.not_leader.inc(),
			s if s == WriteStatus::DUPLICATE => self.duplicate.inc(),
			s if s == WriteStatus::PROXY_LIMIT => self.proxy_limit.inc(),
			s if s == WriteStatus::PAYLOAD_TOO_LARGE => self.payload_too_large.inc(),
			s if s == WriteStatus::LEASE_EXPIRED => self.lease_expired.inc(),
			s if s == WriteStatus::INVALID_LEASE_SIGNATURE => {
				self.invalid_lease_signature.inc();
			}
			s if s == WriteStatus::INTERNAL_ERROR => self.internal_error.inc(),
			s if s == WriteStatus::QUOTA_EXCEEDED => self.quota_exceeded.inc(),
			s if s == WriteStatus::TENANT_SUSPENDED => self.tenant_suspended.inc(),
			s if s == WriteStatus::TENANT_UNAVAILABLE => self.tenant_unavailable.inc(),
			s if s == WriteStatus::AUTHORIZATION_FAILED => self.authorization_failed.inc(),
			s if s == WriteStatus::FINALIZATION_PENDING => self.finalization_pending.inc(),
			s if s == WriteStatus::BACKPRESSURED => self.backpressured.inc(),
			_ => {
				let mut unknown = self
					.unknown
					.lock()
					.expect("write outcome unknown counter mutex poisoned");
				*unknown.entry(status.0).or_default() += 1;
			}
		}
	}

	fn render(&self, out: &mut String) {
		render_counter_family_header(
			out,
			"wendelon_data_ring_write_outcome_total",
			"Write responses emitted by the Data Ring, grouped by final status.",
		);
		render_counter_with_labels(
			out,
			"wendelon_data_ring_write_outcome_total",
			&[("outcome", "ok")],
			self.ok.get(),
		);
		render_counter_with_labels(
			out,
			"wendelon_data_ring_write_outcome_total",
			&[("outcome", "epoch_stale")],
			self.epoch_stale.get(),
		);
		render_counter_with_labels(
			out,
			"wendelon_data_ring_write_outcome_total",
			&[("outcome", "not_leader")],
			self.not_leader.get(),
		);
		render_counter_with_labels(
			out,
			"wendelon_data_ring_write_outcome_total",
			&[("outcome", "duplicate")],
			self.duplicate.get(),
		);
		render_counter_with_labels(
			out,
			"wendelon_data_ring_write_outcome_total",
			&[("outcome", "proxy_limit")],
			self.proxy_limit.get(),
		);
		render_counter_with_labels(
			out,
			"wendelon_data_ring_write_outcome_total",
			&[("outcome", "payload_too_large")],
			self.payload_too_large.get(),
		);
		render_counter_with_labels(
			out,
			"wendelon_data_ring_write_outcome_total",
			&[("outcome", "lease_expired")],
			self.lease_expired.get(),
		);
		render_counter_with_labels(
			out,
			"wendelon_data_ring_write_outcome_total",
			&[("outcome", "invalid_lease_signature")],
			self.invalid_lease_signature.get(),
		);
		render_counter_with_labels(
			out,
			"wendelon_data_ring_write_outcome_total",
			&[("outcome", "internal_error")],
			self.internal_error.get(),
		);
		render_counter_with_labels(
			out,
			"wendelon_data_ring_write_outcome_total",
			&[("outcome", "quota_exceeded")],
			self.quota_exceeded.get(),
		);
		render_counter_with_labels(
			out,
			"wendelon_data_ring_write_outcome_total",
			&[("outcome", "tenant_suspended")],
			self.tenant_suspended.get(),
		);
		render_counter_with_labels(
			out,
			"wendelon_data_ring_write_outcome_total",
			&[("outcome", "tenant_unavailable")],
			self.tenant_unavailable.get(),
		);
		render_counter_with_labels(
			out,
			"wendelon_data_ring_write_outcome_total",
			&[("outcome", "authorization_failed")],
			self.authorization_failed.get(),
		);
		render_counter_with_labels(
			out,
			"wendelon_data_ring_write_outcome_total",
			&[("outcome", "finalization_pending")],
			self.finalization_pending.get(),
		);
		render_counter_with_labels(
			out,
			"wendelon_data_ring_write_outcome_total",
			&[("outcome", "backpressured")],
			self.backpressured.get(),
		);
		for (status, count) in self
			.unknown
			.lock()
			.expect("write outcome unknown counter mutex poisoned")
			.iter()
		{
			let outcome = format!("unknown_{status}");
			render_counter_with_labels(
				out,
				"wendelon_data_ring_write_outcome_total",
				&[("outcome", outcome.as_str())],
				*count,
			);
		}
	}
}

#[derive(Default)]
struct MigrationProxyReadCounters {
	hit: Counter,
	authoritative_miss: Counter,
	incomplete: Counter,
}

impl MigrationProxyReadCounters {
	fn observe(&self, outcome: MigrationProxyReadOutcome) {
		match outcome {
			MigrationProxyReadOutcome::Hit => self.hit.inc(),
			MigrationProxyReadOutcome::AuthoritativeMiss => self.authoritative_miss.inc(),
			MigrationProxyReadOutcome::Incomplete => self.incomplete.inc(),
		}
	}

	fn render(&self, out: &mut String) {
		render_counter_family_header(
			out,
			"wendelon_data_ring_migration_proxy_reads_total",
			"Lazy-migration point-read proxy attempts by outcome.",
		);
		render_counter_with_labels(
			out,
			"wendelon_data_ring_migration_proxy_reads_total",
			&[("outcome", MigrationProxyReadOutcome::Hit.as_str())],
			self.hit.get(),
		);
		render_counter_with_labels(
			out,
			"wendelon_data_ring_migration_proxy_reads_total",
			&[(
				"outcome",
				MigrationProxyReadOutcome::AuthoritativeMiss.as_str(),
			)],
			self.authoritative_miss.get(),
		);
		render_counter_with_labels(
			out,
			"wendelon_data_ring_migration_proxy_reads_total",
			&[("outcome", MigrationProxyReadOutcome::Incomplete.as_str())],
			self.incomplete.get(),
		);
	}
}

#[derive(Default)]
struct PendingFinalizationReplayCounters {
	duplicate_replay: Counter,
	reconciler: Counter,
	orphan_witness_import: Counter,
}

impl PendingFinalizationReplayCounters {
	fn observe(&self, path: PendingFinalizationReplayPath) {
		match path {
			PendingFinalizationReplayPath::DuplicateReplay => self.duplicate_replay.inc(),
			PendingFinalizationReplayPath::Reconciler => self.reconciler.inc(),
			PendingFinalizationReplayPath::OrphanWitnessImport => {
				self.orphan_witness_import.inc();
			}
		}
	}

	fn render(&self, out: &mut String) {
		render_counter_family_header(
			out,
			"wendelon_data_ring_pending_finalization_replay_total",
			"Pending-finalization recovery work attempted by path.",
		);
		render_counter_with_labels(
			out,
			"wendelon_data_ring_pending_finalization_replay_total",
			&[(
				"path",
				PendingFinalizationReplayPath::DuplicateReplay.as_str(),
			)],
			self.duplicate_replay.get(),
		);
		render_counter_with_labels(
			out,
			"wendelon_data_ring_pending_finalization_replay_total",
			&[("path", PendingFinalizationReplayPath::Reconciler.as_str())],
			self.reconciler.get(),
		);
		render_counter_with_labels(
			out,
			"wendelon_data_ring_pending_finalization_replay_total",
			&[(
				"path",
				PendingFinalizationReplayPath::OrphanWitnessImport.as_str(),
			)],
			self.orphan_witness_import.get(),
		);
	}
}

impl Histogram {
	pub fn new(bounds_micros: &'static [u64]) -> Self {
		Self {
			bounds_micros,
			state: Mutex::new(HistogramState {
				bucket_counts: vec![0; bounds_micros.len() + 1],
				sum_micros: 0,
				count: 0,
			}),
		}
	}

	fn observe_micros(&self, value_micros: u64) {
		let mut state = self.state.lock().expect("histogram mutex poisoned");
		state.count = state.count.saturating_add(1);
		state.sum_micros = state.sum_micros.saturating_add(value_micros);
		let index = self
			.bounds_micros
			.iter()
			.position(|bound| value_micros <= *bound)
			.unwrap_or(self.bounds_micros.len());
		state.bucket_counts[index] += 1;
	}

	pub fn observe_value(&self, value: u64) {
		self.observe_micros(value);
	}

	pub fn observe_duration(&self, duration: Duration) {
		self.observe_micros(duration_to_micros(duration));
	}

	pub fn snapshot(&self) -> HistogramSnapshot<'_> {
		let state = self.state.lock().expect("histogram mutex poisoned");
		HistogramSnapshot {
			bounds_micros: self.bounds_micros,
			bucket_counts: state.bucket_counts.clone(),
			sum_micros: state.sum_micros,
			count: state.count,
		}
	}
}

pub struct MetricsRegistry {
	pub metrics_scrapes_total: Counter,

	pub raft_membership_epoch: Gauge,
	raft_current_leader_id: Mutex<Option<String>>,

	pub liveness_tracked_peers: Gauge,
	pub liveness_stale_peers: Gauge,
	pub clock_drift_checks_total: Counter,
	pub clock_drift_insufficient_samples_total: Counter,
	pub clock_drift_exceeded_total: Counter,
	pub clock_drift_offset_ms: Gauge,
	pub eviction_sweeps_total: Counter,
	pub eviction_refused_quorum_total: Counter,
	pub evictions_total: Counter,
	pub eviction_advancer_failures_total: Counter,

	pub quic_handshake_failures_transport_total: Counter,
	pub quic_handshake_failures_identity_total: Counter,
	pub quic_handshake_failures_authorization_total: Counter,
	pub transport_admission_rejections_connection_limit_total: Counter,
	pub transport_admission_rejections_connection_per_ip_limit_total: Counter,
	pub transport_admission_rejections_stream_limit_total: Counter,
	pub transport_admission_rejections_task_limit_total: Counter,
	pub transport_active_connections: Gauge,
	pub transport_active_stream_handlers: Gauge,

	pub data_ring_write_requests_total: Counter,
	pub data_ring_read_requests_total: Counter,
	pub data_ring_query_requests_total: Counter,
	pub data_ring_migrate_requests_total: Counter,
	data_ring_write_outcomes: WriteOutcomeCounters,
	pub data_ring_lease_refresh_passes_total: Counter,
	pub data_ring_lease_grants_total: Counter,
	pub data_ring_lease_denials_total: Counter,
	pub data_ring_lease_errors_total: Counter,
	pub data_ring_lease_cache_entries: Gauge,
	pub data_ring_pending_finalization_backlog: Gauge,
	data_ring_pending_finalization_replay: PendingFinalizationReplayCounters,
	data_ring_migration_proxy_reads: MigrationProxyReadCounters,
	pub data_ring_backpressure_rejections_tenant_total: Counter,
	pub data_ring_backpressure_rejections_actor_total: Counter,
	pub data_ring_backpressure_active_tenant_requests: Gauge,
	pub data_ring_backpressure_active_actor_requests: Gauge,
	pbc_rows_scanned: PbcRuntimeCounters,
	pbc_rows_returned: PbcRuntimeCounters,
	pbc_exec_micros: PbcRuntimeHistograms,
	pbc_kernel_error_total: PbcRuntimeCounters,
	pbc_intermediate_arrays: PbcRuntimeCounters,
	pbc_locality_arcs_scanned_total: Counter,
	pbc_range_decomposition_overflow_total: Counter,
	pbc_locality_decomposition_micros: Histogram,
	pbc_locality_encode_micros: Histogram,
	pub pbc_slab_stats_consulted_total: Counter,
	pub pbc_slab_stats_pruned_total: Counter,
	pub pbc_slab_stats_missing_total: Counter,
	pub pbc_slab_stats_unsupported_total: Counter,
	pub pbc_slab_stats_predicate_unrepresentable_total: Counter,
	pub pbc_slab_stats_decrypt_failed_total: Counter,
	pub pbc_slab_stats_pointer_drift_total: Counter,
	pub pbc_slab_stats_cache_hits_total: Counter,
	pub pbc_slab_stats_cache_misses_total: Counter,
	pub pbc_slab_stats_skipped_no_capability_total: Counter,
	pbc_slab_stats_build_micros: Histogram,
	pbc_slab_stats_decrypt_micros: Histogram,
	pbc_slab_stats_block_bytes: Histogram,

	pub group_commit_window_seconds: Histogram,
	pub data_ring_durable_quorum_latency_seconds: Histogram,
	pub data_ring_pending_finalization_replay_latency_seconds: Histogram,
	pub data_ring_migration_proxy_read_latency_seconds: Histogram,
	pub l1_read_latency_seconds: Histogram,
	pub l2_upload_latency_seconds: Histogram,
	pub l2_range_read_latency_seconds: Histogram,
	io_uring_submission_queue_depth: IoUringQueueDepthGauges,
	io_uring_completion_queue_depth: IoUringQueueDepthGauges,
	io_uring_errors: IoUringErrorCounters,

	pub storage_tiering_passes_total: Counter,
	pub storage_tiering_pass_errors_total: Counter,
	pub storage_tiered_slabs_total: Counter,
	pub storage_tiering_failures_total: Counter,
	pub storage_tiering_skipped_small_total: Counter,
	pub storage_tiering_skipped_already_tiered_total: Counter,
	pub storage_wal_blocking_queue_depth: Gauge,
	pub storage_slab_blocking_queue_depth: Gauge,
	pub storage_wal_append_latency_seconds: Histogram,
	pub storage_wal_fsync_latency_seconds: Histogram,
	pub storage_rehydration_guard_skips_total: Counter,
	pub storage_rehydrated_reclaims_total: Counter,
	pub storage_migration_reclaimed_slabs_total: Counter,
	pub storage_migration_reclaimed_bytes_total: Counter,

	pub tenant_kek_cache_hits_total: Counter,
	pub tenant_kek_cache_misses_total: Counter,

	pub audit_materialization_lag_entries: Gauge,
	pub audit_emit_node_local_total: Counter,
	pub audit_emit_raft_log_total: Counter,
	pub audit_sink_failures_local_total: Counter,
	pub audit_sink_failures_archive_total: Counter,
	pub audit_sink_failures_archive_queue_dropped_total: Counter,
	pub audit_sink_failures_syslog_total: Counter,
	pub audit_sink_failures_syslog_queue_dropped_total: Counter,
	pub audit_sink_failures_kafka_total: Counter,
	pub audit_sink_failures_kafka_queue_dropped_total: Counter,
	pub audit_archive_pending_records: Gauge,
	pub audit_archived_segments_total: Counter,
	pub audit_archived_bytes_total: Counter,
	pub otlp_spans_dropped_total: Counter,
	pub fips_module_self_test_ok_total: Counter,

	pub crypto_decrypt_audit_emit_failures_total: Counter,
}

impl Default for MetricsRegistry {
	fn default() -> Self {
		Self {
			metrics_scrapes_total: Counter::default(),

			raft_membership_epoch: Gauge::default(),
			raft_current_leader_id: Mutex::new(None),

			liveness_tracked_peers: Gauge::default(),
			liveness_stale_peers: Gauge::default(),
			clock_drift_checks_total: Counter::default(),
			clock_drift_insufficient_samples_total: Counter::default(),
			clock_drift_exceeded_total: Counter::default(),
			clock_drift_offset_ms: Gauge::default(),
			eviction_sweeps_total: Counter::default(),
			eviction_refused_quorum_total: Counter::default(),
			evictions_total: Counter::default(),
			eviction_advancer_failures_total: Counter::default(),

			quic_handshake_failures_transport_total: Counter::default(),
			quic_handshake_failures_identity_total: Counter::default(),
			quic_handshake_failures_authorization_total: Counter::default(),
			transport_admission_rejections_connection_limit_total: Counter::default(),
			transport_admission_rejections_connection_per_ip_limit_total: Counter::default(),
			transport_admission_rejections_stream_limit_total: Counter::default(),
			transport_admission_rejections_task_limit_total: Counter::default(),
			transport_active_connections: Gauge::default(),
			transport_active_stream_handlers: Gauge::default(),

			data_ring_write_requests_total: Counter::default(),
			data_ring_read_requests_total: Counter::default(),
			data_ring_query_requests_total: Counter::default(),
			data_ring_migrate_requests_total: Counter::default(),
			data_ring_write_outcomes: WriteOutcomeCounters::default(),
			data_ring_lease_refresh_passes_total: Counter::default(),
			data_ring_lease_grants_total: Counter::default(),
			data_ring_lease_denials_total: Counter::default(),
			data_ring_lease_errors_total: Counter::default(),
			data_ring_lease_cache_entries: Gauge::default(),
			data_ring_pending_finalization_backlog: Gauge::default(),
			data_ring_pending_finalization_replay: PendingFinalizationReplayCounters::default(),
			data_ring_migration_proxy_reads: MigrationProxyReadCounters::default(),
			data_ring_backpressure_rejections_tenant_total: Counter::default(),
			data_ring_backpressure_rejections_actor_total: Counter::default(),
			data_ring_backpressure_active_tenant_requests: Gauge::default(),
			data_ring_backpressure_active_actor_requests: Gauge::default(),
			pbc_rows_scanned: PbcRuntimeCounters::default(),
			pbc_rows_returned: PbcRuntimeCounters::default(),
			pbc_exec_micros: PbcRuntimeHistograms::default(),
			pbc_kernel_error_total: PbcRuntimeCounters::default(),
			pbc_intermediate_arrays: PbcRuntimeCounters::default(),
			pbc_locality_arcs_scanned_total: Counter::default(),
			pbc_range_decomposition_overflow_total: Counter::default(),
			pbc_locality_decomposition_micros: Histogram::new(PBC_LOCALITY_BUCKETS_MICROS),
			pbc_locality_encode_micros: Histogram::new(PBC_LOCALITY_BUCKETS_MICROS),
			pbc_slab_stats_consulted_total: Counter::default(),
			pbc_slab_stats_pruned_total: Counter::default(),
			pbc_slab_stats_missing_total: Counter::default(),
			pbc_slab_stats_unsupported_total: Counter::default(),
			pbc_slab_stats_predicate_unrepresentable_total: Counter::default(),
			pbc_slab_stats_decrypt_failed_total: Counter::default(),
			pbc_slab_stats_pointer_drift_total: Counter::default(),
			pbc_slab_stats_cache_hits_total: Counter::default(),
			pbc_slab_stats_cache_misses_total: Counter::default(),
			pbc_slab_stats_skipped_no_capability_total: Counter::default(),
			pbc_slab_stats_build_micros: Histogram::new(PBC_SLAB_STATS_BUCKETS_MICROS),
			pbc_slab_stats_decrypt_micros: Histogram::new(PBC_SLAB_STATS_BUCKETS_MICROS),
			pbc_slab_stats_block_bytes: Histogram::new(PBC_SLAB_STATS_BLOCK_BYTES_BUCKETS),

			group_commit_window_seconds: Histogram::new(GROUP_COMMIT_WINDOW_BUCKETS_MICROS),
			data_ring_durable_quorum_latency_seconds: Histogram::new(
				DURABLE_QUORUM_LATENCY_BUCKETS_MICROS,
			),
			data_ring_pending_finalization_replay_latency_seconds: Histogram::new(
				PENDING_FINALIZATION_REPLAY_LATENCY_BUCKETS_MICROS,
			),
			data_ring_migration_proxy_read_latency_seconds: Histogram::new(
				MIGRATION_PROXY_READ_LATENCY_BUCKETS_MICROS,
			),
			l1_read_latency_seconds: Histogram::new(LOCAL_READ_LATENCY_BUCKETS_MICROS),
			l2_upload_latency_seconds: Histogram::new(L2_UPLOAD_LATENCY_BUCKETS_MICROS),
			l2_range_read_latency_seconds: Histogram::new(L2_RANGE_READ_LATENCY_BUCKETS_MICROS),
			io_uring_submission_queue_depth: IoUringQueueDepthGauges::default(),
			io_uring_completion_queue_depth: IoUringQueueDepthGauges::default(),
			io_uring_errors: IoUringErrorCounters::default(),

			storage_tiering_passes_total: Counter::default(),
			storage_tiering_pass_errors_total: Counter::default(),
			storage_tiered_slabs_total: Counter::default(),
			storage_tiering_failures_total: Counter::default(),
			storage_tiering_skipped_small_total: Counter::default(),
			storage_tiering_skipped_already_tiered_total: Counter::default(),
			storage_wal_blocking_queue_depth: Gauge::default(),
			storage_slab_blocking_queue_depth: Gauge::default(),
			storage_wal_append_latency_seconds: Histogram::new(WAL_APPEND_LATENCY_BUCKETS_MICROS),
			storage_wal_fsync_latency_seconds: Histogram::new(WAL_FSYNC_LATENCY_BUCKETS_MICROS),
			storage_rehydration_guard_skips_total: Counter::default(),
			storage_rehydrated_reclaims_total: Counter::default(),
			storage_migration_reclaimed_slabs_total: Counter::default(),
			storage_migration_reclaimed_bytes_total: Counter::default(),

			tenant_kek_cache_hits_total: Counter::default(),
			tenant_kek_cache_misses_total: Counter::default(),

			audit_materialization_lag_entries: Gauge::default(),
			audit_emit_node_local_total: Counter::default(),
			audit_emit_raft_log_total: Counter::default(),
			audit_sink_failures_local_total: Counter::default(),
			audit_sink_failures_archive_total: Counter::default(),
			audit_sink_failures_archive_queue_dropped_total: Counter::default(),
			audit_sink_failures_syslog_total: Counter::default(),
			audit_sink_failures_syslog_queue_dropped_total: Counter::default(),
			audit_sink_failures_kafka_total: Counter::default(),
			audit_sink_failures_kafka_queue_dropped_total: Counter::default(),
			audit_archive_pending_records: Gauge::default(),
			audit_archived_segments_total: Counter::default(),
			audit_archived_bytes_total: Counter::default(),
			otlp_spans_dropped_total: Counter::default(),
			fips_module_self_test_ok_total: Counter::default(),

			crypto_decrypt_audit_emit_failures_total: Counter::default(),
		}
	}
}

impl MetricsRegistry {
	pub fn observe_write_outcome(&self, status: WriteStatus) {
		self.data_ring_write_outcomes.observe(status);
	}

	pub fn set_pending_finalization_backlog(&self, value: u64) {
		self.data_ring_pending_finalization_backlog.set(value);
	}

	pub fn observe_pending_finalization_replay(
		&self,
		path: PendingFinalizationReplayPath,
		duration: Duration,
	) {
		self.data_ring_pending_finalization_replay.observe(path);
		self.data_ring_pending_finalization_replay_latency_seconds
			.observe_duration(duration);
	}

	pub fn observe_migration_proxy_read(
		&self,
		outcome: MigrationProxyReadOutcome,
		duration: Duration,
	) {
		self.data_ring_migration_proxy_reads.observe(outcome);
		self.data_ring_migration_proxy_read_latency_seconds
			.observe_duration(duration);
	}

	pub fn set_io_uring_submission_queue_depth(&self, class: IoUringClass, value: u64) {
		self.io_uring_submission_queue_depth.set(class, value);
	}

	pub fn set_io_uring_completion_queue_depth(&self, class: IoUringClass, value: u64) {
		self.io_uring_completion_queue_depth.set(class, value);
	}

	pub fn observe_io_uring_error(&self, class: IoUringClass, op: &str, errno: i32) {
		self.io_uring_errors.observe(class, op, errno);
	}

	pub fn observe_durable_quorum_latency(&self, duration: Duration) {
		self.data_ring_durable_quorum_latency_seconds
			.observe_duration(duration);
	}

	pub fn observe_wal_append_latency(&self, duration: Duration) {
		self.storage_wal_append_latency_seconds
			.observe_duration(duration);
	}

	pub fn observe_wal_fsync_latency(&self, duration: Duration) {
		self.storage_wal_fsync_latency_seconds
			.observe_duration(duration);
	}

	pub fn set_raft_observability_state(&self, epoch: u64, leader_id: Option<u32>) {
		self.raft_membership_epoch.set(epoch);
		*self
			.raft_current_leader_id
			.lock()
			.expect("raft leader gauge mutex poisoned") = leader_id.map(|id| format!("n-{id:02}"));
	}

	pub fn observe_pbc_execution(
		&self,
		runtime: PbcRuntime,
		rows_scanned: u64,
		rows_returned: u64,
		duration: Duration,
		kernel_errors: u64,
		intermediate_arrays: u64,
	) {
		self.pbc_rows_scanned.add(runtime, rows_scanned);
		self.pbc_rows_returned.add(runtime, rows_returned);
		self.pbc_exec_micros.observe_duration(runtime, duration);
		self.pbc_kernel_error_total.add(runtime, kernel_errors);
		self.pbc_intermediate_arrays
			.add(runtime, intermediate_arrays);
	}

	pub fn observe_pbc_locality_arcs_scanned(&self, arcs: u64) {
		self.pbc_locality_arcs_scanned_total.add(arcs);
	}

	pub fn observe_pbc_range_decomposition_overflow(&self) {
		self.pbc_range_decomposition_overflow_total.inc();
	}

	pub fn observe_pbc_locality_decomposition(&self, duration: Duration) {
		self.pbc_locality_decomposition_micros
			.observe_duration(duration);
	}

	pub fn observe_pbc_locality_encode(&self, duration: Duration) {
		self.pbc_locality_encode_micros.observe_duration(duration);
	}

	pub fn observe_pbc_slab_stats_build(&self, duration: Duration) {
		self.pbc_slab_stats_build_micros.observe_duration(duration);
	}

	pub fn observe_pbc_slab_stats_decrypt(&self, duration: Duration) {
		self.pbc_slab_stats_decrypt_micros
			.observe_duration(duration);
	}

	pub fn observe_pbc_slab_stats_block_bytes(&self, bytes: u64) {
		self.pbc_slab_stats_block_bytes.observe_value(bytes);
	}

	pub fn render_prometheus(&self) -> String {
		let mut out = String::with_capacity(8192);
		render_counter(
			&mut out,
			"wendelon_metrics_scrapes_total",
			"Total successful metrics scrapes.",
			self.metrics_scrapes_total.get(),
		);
		render_gauge(
			&mut out,
			"wendelon_raft_membership_epoch",
			"Current applied Control Ring membership epoch.",
			self.raft_membership_epoch.get(),
		);
		render_gauge_family_header(
			&mut out,
			"wendelon_raft_current_leader",
			"Current Control Ring leader for this node's latest snapshot.",
		);
		if let Some(leader_id) = self
			.raft_current_leader_id
			.lock()
			.expect("raft leader gauge mutex poisoned")
			.clone()
		{
			render_gauge_with_labels(
				&mut out,
				"wendelon_raft_current_leader",
				&[("leader_id", leader_id.as_str())],
				1,
			);
		}
		render_gauge(
			&mut out,
			"wendelon_liveness_tracked_peers",
			"Peers currently tracked for liveness.",
			self.liveness_tracked_peers.get(),
		);
		render_gauge(
			&mut out,
			"wendelon_liveness_stale_peers",
			"Peers currently classified as stale by the liveness watchdog.",
			self.liveness_stale_peers.get(),
		);
		render_counter(
			&mut out,
			"wendelon_clock_drift_checks_total",
			"Clock drift checks executed.",
			self.clock_drift_checks_total.get(),
		);
		render_counter(
			&mut out,
			"wendelon_clock_drift_insufficient_samples_total",
			"Clock drift checks skipped because the cluster lacked enough samples.",
			self.clock_drift_insufficient_samples_total.get(),
		);
		render_counter(
			&mut out,
			"wendelon_clock_drift_exceeded_total",
			"Clock drift checks that exceeded the allowed bound.",
			self.clock_drift_exceeded_total.get(),
		);
		render_gauge(
			&mut out,
			"wendelon_clock_drift_offset_ms",
			"Last observed absolute clock drift offset in milliseconds.",
			self.clock_drift_offset_ms.get(),
		);
		render_counter(
			&mut out,
			"wendelon_eviction_sweeps_total",
			"Eviction watchdog sweeps executed.",
			self.eviction_sweeps_total.get(),
		);
		render_counter(
			&mut out,
			"wendelon_eviction_refused_quorum_total",
			"Eviction sweeps refused because they would drop below quorum.",
			self.eviction_refused_quorum_total.get(),
		);
		render_counter(
			&mut out,
			"wendelon_evictions_total",
			"Peers evicted from the Control Ring.",
			self.evictions_total.get(),
		);
		render_counter(
			&mut out,
			"wendelon_eviction_advancer_failures_total",
			"Eviction proposals that failed to advance the Control Ring membership.",
			self.eviction_advancer_failures_total.get(),
		);
		render_counter_family_header(
			&mut out,
			"wendelon_quic_handshake_failures_total",
			"QUIC handshakes that failed before an authenticated transport session was established.",
		);
		render_counter_with_labels(
			&mut out,
			"wendelon_quic_handshake_failures_total",
			&[("reason", "transport")],
			self.quic_handshake_failures_transport_total.get(),
		);
		render_counter_with_labels(
			&mut out,
			"wendelon_quic_handshake_failures_total",
			&[("reason", "identity")],
			self.quic_handshake_failures_identity_total.get(),
		);
		render_counter_with_labels(
			&mut out,
			"wendelon_quic_handshake_failures_total",
			&[("reason", "authorization")],
			self.quic_handshake_failures_authorization_total.get(),
		);
		render_counter_family_header(
			&mut out,
			"wendelon_transport_admission_rejections_total",
			"Inbound transport requests rejected by server-side admission control before handler execution.",
		);
		render_counter_with_labels(
			&mut out,
			"wendelon_transport_admission_rejections_total",
			&[("reason", "connection_limit")],
			self.transport_admission_rejections_connection_limit_total
				.get(),
		);
		render_counter_with_labels(
			&mut out,
			"wendelon_transport_admission_rejections_total",
			&[("reason", "connection_per_ip_limit")],
			self.transport_admission_rejections_connection_per_ip_limit_total
				.get(),
		);
		render_counter_with_labels(
			&mut out,
			"wendelon_transport_admission_rejections_total",
			&[("reason", "stream_limit")],
			self.transport_admission_rejections_stream_limit_total.get(),
		);
		render_counter_with_labels(
			&mut out,
			"wendelon_transport_admission_rejections_total",
			&[("reason", "task_limit")],
			self.transport_admission_rejections_task_limit_total.get(),
		);
		render_gauge(
			&mut out,
			"wendelon_transport_active_connections",
			"Inbound transport connections currently admitted by the server listener.",
			self.transport_active_connections.get(),
		);
		render_gauge(
			&mut out,
			"wendelon_transport_active_stream_handlers",
			"Inbound stream handler tasks currently admitted by the transport server.",
			self.transport_active_stream_handlers.get(),
		);
		render_counter(
			&mut out,
			"wendelon_data_ring_write_requests_total",
			"Data Ring write requests handled.",
			self.data_ring_write_requests_total.get(),
		);
		render_counter(
			&mut out,
			"wendelon_data_ring_read_requests_total",
			"Data Ring read requests handled.",
			self.data_ring_read_requests_total.get(),
		);
		render_counter(
			&mut out,
			"wendelon_data_ring_query_requests_total",
			"Data Ring query requests handled.",
			self.data_ring_query_requests_total.get(),
		);
		render_counter(
			&mut out,
			"wendelon_data_ring_migrate_requests_total",
			"Data Ring migration requests handled.",
			self.data_ring_migrate_requests_total.get(),
		);
		self.data_ring_write_outcomes.render(&mut out);
		render_counter(
			&mut out,
			"wendelon_data_ring_lease_refresh_passes_total",
			"Lease refresh passes executed by the Data Ring lease manager.",
			self.data_ring_lease_refresh_passes_total.get(),
		);
		render_counter(
			&mut out,
			"wendelon_data_ring_lease_grants_total",
			"Lease grants accepted into the local Data Ring cache.",
			self.data_ring_lease_grants_total.get(),
		);
		render_counter(
			&mut out,
			"wendelon_data_ring_lease_denials_total",
			"Lease refresh attempts denied by the current leader.",
			self.data_ring_lease_denials_total.get(),
		);
		render_counter(
			&mut out,
			"wendelon_data_ring_lease_errors_total",
			"Lease refresh attempts that failed with an error.",
			self.data_ring_lease_errors_total.get(),
		);
		render_gauge(
			&mut out,
			"wendelon_data_ring_lease_cache_entries",
			"Lease descriptors currently cached by the local Data Ring.",
			self.data_ring_lease_cache_entries.get(),
		);
		render_gauge(
			&mut out,
			"wendelon_data_ring_pending_finalization_backlog",
			"Durably persisted pending-finalization records currently awaiting reconciliation.",
			self.data_ring_pending_finalization_backlog.get(),
		);
		self.data_ring_pending_finalization_replay.render(&mut out);
		self.data_ring_migration_proxy_reads.render(&mut out);
		render_counter_family_header(
			&mut out,
			"wendelon_data_ring_backpressure_rejections_total",
			"Post-auth Data Ring requests rejected by bounded per-principal admission control.",
		);
		render_counter_with_labels(
			&mut out,
			"wendelon_data_ring_backpressure_rejections_total",
			&[("scope", "tenant")],
			self.data_ring_backpressure_rejections_tenant_total.get(),
		);
		render_counter_with_labels(
			&mut out,
			"wendelon_data_ring_backpressure_rejections_total",
			&[("scope", "actor")],
			self.data_ring_backpressure_rejections_actor_total.get(),
		);
		render_gauge(
			&mut out,
			"wendelon_data_ring_backpressure_active_tenant_requests",
			"Tenant-scoped Data Ring requests currently admitted by post-auth backpressure control.",
			self.data_ring_backpressure_active_tenant_requests.get(),
		);
		render_gauge(
			&mut out,
			"wendelon_data_ring_backpressure_active_actor_requests",
			"Actor-scoped Data Ring requests currently admitted by post-auth backpressure control.",
			self.data_ring_backpressure_active_actor_requests.get(),
		);
		self.pbc_rows_scanned.render(
			&mut out,
			"wendelon_pbc_rows_scanned_total",
			"Rows scanned by PBC execution.",
		);
		self.pbc_rows_returned.render(
			&mut out,
			"wendelon_pbc_rows_returned_total",
			"Rows returned after PBC filtering.",
		);
		// Derived from the scanned/returned counters instead of stored as
		// a separate hot-path metric.
		render_gauge_f64(
			&mut out,
			"wendelon_pbc_mask_selectivity",
			"Aggregate PBC mask selectivity, computed as rows_returned / rows_scanned.",
			mask_selectivity(
				self.pbc_rows_scanned.total(),
				self.pbc_rows_returned.total(),
			),
		);
		self.pbc_exec_micros.render(
			&mut out,
			"wendelon_pbc_exec_micros",
			"PBC execution duration in microseconds.",
		);
		self.pbc_kernel_error_total.render(
			&mut out,
			"wendelon_pbc_kernel_error_total",
			"PBC vector kernel or runtime-trap errors normalized during execution.",
		);
		self.pbc_intermediate_arrays.render(
			&mut out,
			"wendelon_pbc_intermediate_arrays_total",
			"Intermediate Arrow arrays materialized by PBC execution.",
		);
		render_counter(
			&mut out,
			"wendelon_pbc_locality_arcs_scanned_total",
			"Locality arcs admitted for PBC scan planning.",
			self.pbc_locality_arcs_scanned_total.get(),
		);
		render_counter(
			&mut out,
			"wendelon_pbc_range_decomposition_overflow_total",
			"Column-projection range decompositions that exceeded the arc or node budget and fell back to full table arcs.",
			self.pbc_range_decomposition_overflow_total.get(),
		);
		render_histogram_micros(
			&mut out,
			"wendelon_pbc_locality_decomposition_micros",
			"Column-projection locality range decomposition duration in microseconds.",
			&self.pbc_locality_decomposition_micros.snapshot(),
		);
		render_histogram_micros(
			&mut out,
			"wendelon_pbc_locality_encode_micros",
			"Column-projection write-locality encoding duration in microseconds.",
			&self.pbc_locality_encode_micros.snapshot(),
		);
		render_counter(
			&mut out,
			"wendelon_pbc_slab_stats_consulted_total",
			"Slab stats blocks consulted during scan planning.",
			self.pbc_slab_stats_consulted_total.get(),
		);
		render_counter(
			&mut out,
			"wendelon_pbc_slab_stats_pruned_total",
			"Candidate slabs pruned by ADR 039 column statistics.",
			self.pbc_slab_stats_pruned_total.get(),
		);
		render_counter(
			&mut out,
			"wendelon_pbc_slab_stats_missing_total",
			"Candidate slabs scanned because stats were unavailable.",
			self.pbc_slab_stats_missing_total.get(),
		);
		render_counter(
			&mut out,
			"wendelon_pbc_slab_stats_unsupported_total",
			"Candidate slabs scanned because stats did not cover the predicate columns.",
			self.pbc_slab_stats_unsupported_total.get(),
		);
		render_counter(
			&mut out,
			"wendelon_pbc_slab_stats_predicate_unrepresentable_total",
			"Candidate slabs scanned because the predicate shape was not stats-representable.",
			self.pbc_slab_stats_predicate_unrepresentable_total.get(),
		);
		render_counter(
			&mut out,
			"wendelon_pbc_slab_stats_decrypt_failed_total",
			"Stats blocks whose AEAD authentication failed after footer validation.",
			self.pbc_slab_stats_decrypt_failed_total.get(),
		);
		render_counter(
			&mut out,
			"wendelon_pbc_slab_stats_pointer_drift_total",
			"TieredSlab stats advisories that disagreed with the slab footer.",
			self.pbc_slab_stats_pointer_drift_total.get(),
		);
		render_counter(
			&mut out,
			"wendelon_pbc_slab_stats_cache_hits_total",
			"Parsed slab-stats cache hits.",
			self.pbc_slab_stats_cache_hits_total.get(),
		);
		render_counter(
			&mut out,
			"wendelon_pbc_slab_stats_cache_misses_total",
			"Parsed slab-stats cache misses.",
			self.pbc_slab_stats_cache_misses_total.get(),
		);
		render_counter(
			&mut out,
			"wendelon_pbc_slab_stats_skipped_no_capability_total",
			"Queries that disabled stats pruning because the chosen target peer does not advertise the SlabStatsPruning capability.",
			self.pbc_slab_stats_skipped_no_capability_total.get(),
		);
		render_histogram_micros(
			&mut out,
			"wendelon_pbc_slab_stats_build_micros",
			"Slab stats build duration in microseconds.",
			&self.pbc_slab_stats_build_micros.snapshot(),
		);
		render_histogram_micros(
			&mut out,
			"wendelon_pbc_slab_stats_decrypt_micros",
			"Stats block decrypt duration in microseconds.",
			&self.pbc_slab_stats_decrypt_micros.snapshot(),
		);
		render_histogram_raw(
			&mut out,
			"wendelon_pbc_slab_stats_block_bytes",
			"Stats block ciphertext size in bytes.",
			&self.pbc_slab_stats_block_bytes.snapshot(),
		);
		render_histogram(
			&mut out,
			"wendelon_group_commit_window_seconds",
			"Time writes spent coalescing in the group-commit window before fdatasync.",
			&self.group_commit_window_seconds.snapshot(),
		);
		render_histogram(
			&mut out,
			"wendelon_data_ring_durable_quorum_latency_seconds",
			"Time from write admission to durable quorum satisfaction on the data branch.",
			&self.data_ring_durable_quorum_latency_seconds.snapshot(),
		);
		render_histogram(
			&mut out,
			"wendelon_data_ring_pending_finalization_replay_latency_seconds",
			"Time spent reconciling or importing pending-finalization work.",
			&self
				.data_ring_pending_finalization_replay_latency_seconds
				.snapshot(),
		);
		render_histogram(
			&mut out,
			"wendelon_data_ring_migration_proxy_read_latency_seconds",
			"Time spent resolving a point read through the lazy-migration proxy path.",
			&self
				.data_ring_migration_proxy_read_latency_seconds
				.snapshot(),
		);
		render_histogram(
			&mut out,
			"wendelon_l1_read_latency_seconds",
			"Storage-side latency for reads served from local L1 slabs.",
			&self.l1_read_latency_seconds.snapshot(),
		);
		render_histogram(
			&mut out,
			"wendelon_l2_upload_latency_seconds",
			"Latency for uploading a tiered slab object into L2 storage.",
			&self.l2_upload_latency_seconds.snapshot(),
		);
		render_histogram(
			&mut out,
			"wendelon_l2_range_read_latency_seconds",
			"Storage-side latency for reads served from tiered L2 slabs.",
			&self.l2_range_read_latency_seconds.snapshot(),
		);
		self.io_uring_submission_queue_depth.render(
			&mut out,
			"wendelon_io_uring_submission_queue_depth",
			"Pending Wendelon-side io_uring submissions queued for each durability class before kernel dispatch.",
		);
		self.io_uring_completion_queue_depth.render(
			&mut out,
			"wendelon_io_uring_completion_queue_depth",
			"In-flight io_uring operations awaiting completion for each durability class.",
		);
		self.io_uring_errors.render(&mut out);
		render_counter(
			&mut out,
			"wendelon_storage_tiering_passes_total",
			"Tiering sweeper passes completed.",
			self.storage_tiering_passes_total.get(),
		);
		render_counter(
			&mut out,
			"wendelon_storage_tiering_pass_errors_total",
			"Tiering sweeper passes that returned an error.",
			self.storage_tiering_pass_errors_total.get(),
		);
		render_counter(
			&mut out,
			"wendelon_storage_tiered_slabs_total",
			"Sealed slabs tiered from L1 to L2.",
			self.storage_tiered_slabs_total.get(),
		);
		render_counter(
			&mut out,
			"wendelon_storage_tiering_failures_total",
			"Per-slab tiering failures observed by the storage sweeper.",
			self.storage_tiering_failures_total.get(),
		);
		render_counter(
			&mut out,
			"wendelon_storage_tiering_skipped_small_total",
			"Sealed slabs skipped because they were below the tiering threshold.",
			self.storage_tiering_skipped_small_total.get(),
		);
		render_counter(
			&mut out,
			"wendelon_storage_tiering_skipped_already_tiered_total",
			"Sealed slabs skipped because the tier pivot was already applied.",
			self.storage_tiering_skipped_already_tiered_total.get(),
		);
		render_gauge(
			&mut out,
			"wendelon_storage_wal_blocking_queue_depth",
			"Queued WAL/control metadata file operations waiting on the isolated blocking executor.",
			self.storage_wal_blocking_queue_depth.get(),
		);
		render_gauge(
			&mut out,
			"wendelon_storage_slab_blocking_queue_depth",
			"Queued slab metadata file operations waiting on the isolated blocking executor.",
			self.storage_slab_blocking_queue_depth.get(),
		);
		render_histogram(
			&mut out,
			"wendelon_storage_wal_append_latency_seconds",
			"Time spent issuing durable Control Ring WAL append writes before fdatasync.",
			&self.storage_wal_append_latency_seconds.snapshot(),
		);
		render_histogram(
			&mut out,
			"wendelon_storage_wal_fsync_latency_seconds",
			"Time spent awaiting Control Ring WAL durability flushes.",
			&self.storage_wal_fsync_latency_seconds.snapshot(),
		);
		render_counter(
			&mut out,
			"wendelon_storage_rehydration_guard_skips_total",
			"Rehydrated slabs retained in L1 because the thrash guard was still active.",
			self.storage_rehydration_guard_skips_total.get(),
		);
		render_counter(
			&mut out,
			"wendelon_storage_rehydrated_reclaims_total",
			"Rehydrated slabs reclaimed from L1 after cooling.",
			self.storage_rehydrated_reclaims_total.get(),
		);
		render_counter(
			&mut out,
			"wendelon_storage_migration_reclaimed_slabs_total",
			"Tombstoned slabs reclaimed after migration completion.",
			self.storage_migration_reclaimed_slabs_total.get(),
		);
		render_counter(
			&mut out,
			"wendelon_storage_migration_reclaimed_bytes_total",
			"Bytes reclaimed from tombstoned slabs after migration completion.",
			self.storage_migration_reclaimed_bytes_total.get(),
		);
		render_counter_family_header(
			&mut out,
			"wendelon_tenant_kek_cache_hit_total",
			"Tenant KEK cache lookups by outcome.",
		);
		render_counter_with_labels(
			&mut out,
			"wendelon_tenant_kek_cache_hit_total",
			&[("outcome", "hit")],
			self.tenant_kek_cache_hits_total.get(),
		);
		render_counter_with_labels(
			&mut out,
			"wendelon_tenant_kek_cache_hit_total",
			&[("outcome", "miss")],
			self.tenant_kek_cache_misses_total.get(),
		);
		render_gauge(
			&mut out,
			"wendelon_audit_materialization_lag_entries",
			"Per-node local audit-chain lag behind Raft-authority audit materialization.",
			self.audit_materialization_lag_entries.get(),
		);
		render_counter_family_header(
			&mut out,
			"wendelon_audit_emit_total",
			"Audit records durably materialized into the local chain.",
		);
		render_counter_with_labels(
			&mut out,
			"wendelon_audit_emit_total",
			&[("authority", "node_local"), ("outcome", "ok")],
			self.audit_emit_node_local_total.get(),
		);
		render_counter_with_labels(
			&mut out,
			"wendelon_audit_emit_total",
			&[("authority", "raft_log"), ("outcome", "ok")],
			self.audit_emit_raft_log_total.get(),
		);
		render_counter_family_header(
			&mut out,
			"wendelon_audit_sink_failure_total",
			"Audit sink failures while attempting to ship or persist audit records.",
		);
		render_counter_with_labels(
			&mut out,
			"wendelon_audit_sink_failure_total",
			&[("sink", "local"), ("reason", "emit_error")],
			self.audit_sink_failures_local_total.get(),
		);
		render_counter_with_labels(
			&mut out,
			"wendelon_audit_sink_failure_total",
			&[("sink", "archive"), ("reason", "emit_error")],
			self.audit_sink_failures_archive_total.get(),
		);
		render_counter_with_labels(
			&mut out,
			"wendelon_audit_sink_failure_total",
			&[("sink", "archive"), ("reason", "queue_drop")],
			self.audit_sink_failures_archive_queue_dropped_total.get(),
		);
		render_counter_with_labels(
			&mut out,
			"wendelon_audit_sink_failure_total",
			&[("sink", "syslog"), ("reason", "emit_error")],
			self.audit_sink_failures_syslog_total.get(),
		);
		render_counter_with_labels(
			&mut out,
			"wendelon_audit_sink_failure_total",
			&[("sink", "syslog"), ("reason", "queue_drop")],
			self.audit_sink_failures_syslog_queue_dropped_total.get(),
		);
		render_counter_with_labels(
			&mut out,
			"wendelon_audit_sink_failure_total",
			&[("sink", "kafka"), ("reason", "emit_error")],
			self.audit_sink_failures_kafka_total.get(),
		);
		render_counter_with_labels(
			&mut out,
			"wendelon_audit_sink_failure_total",
			&[("sink", "kafka"), ("reason", "queue_drop")],
			self.audit_sink_failures_kafka_queue_dropped_total.get(),
		);
		render_gauge(
			&mut out,
			"wendelon_audit_archive_pending_records",
			"Audit records queued locally for archive mirroring after archive sink failures.",
			self.audit_archive_pending_records.get(),
		);
		render_counter(
			&mut out,
			"wendelon_audit_archived_segments_total",
			"Immutable audit archive segments finalized by the local archive mirror.",
			self.audit_archived_segments_total.get(),
		);
		render_counter(
			&mut out,
			"wendelon_audit_archived_bytes_total",
			"Bytes finalized into immutable audit archive segments.",
			self.audit_archived_bytes_total.get(),
		);
		render_counter(
			&mut out,
			"wendelon_otlp_spans_dropped_total",
			"Spans dropped by the OTLP exporter before they could be enqueued for export.",
			self.otlp_spans_dropped_total.get(),
		);
		#[cfg(feature = "fips")]
		{
			render_counter_family_header(
				&mut out,
				"wendelon_fips_module_self_test_total",
				"FIPS module self-test invocations by outcome.",
			);
			render_counter_with_labels(
				&mut out,
				"wendelon_fips_module_self_test_total",
				&[("outcome", "ok")],
				self.fips_module_self_test_ok_total.get(),
			);
			render_gauge_family_header(
				&mut out,
				"wendelon_fips_module_version_info",
				"FIPS module version derived from the pinned aws-lc-rs build.",
			);
			render_gauge_with_labels(
				&mut out,
				"wendelon_fips_module_version_info",
				&[("version", crate::build_info::aws_lc_rs_version())],
				1,
			);
		}
		render_counter(
			&mut out,
			"wendelon_crypto_decrypt_audit_emit_failures_total",
			"crypto.decrypt_rejected audit events that the sink refused to durably persist.",
			self.crypto_decrypt_audit_emit_failures_total.get(),
		);
		out
	}
}

fn render_counter(out: &mut String, name: &str, help: &str, value: u64) {
	let _ = writeln!(out, "# HELP {name} {help}");
	let _ = writeln!(out, "# TYPE {name} counter");
	let _ = writeln!(out, "{name} {value}");
}

fn render_gauge(out: &mut String, name: &str, help: &str, value: u64) {
	let _ = writeln!(out, "# HELP {name} {help}");
	let _ = writeln!(out, "# TYPE {name} gauge");
	let _ = writeln!(out, "{name} {value}");
}

fn render_gauge_f64(out: &mut String, name: &str, help: &str, value: f64) {
	let _ = writeln!(out, "# HELP {name} {help}");
	let _ = writeln!(out, "# TYPE {name} gauge");
	let _ = writeln!(out, "{name} {value:.6}");
}

fn render_counter_family_header(out: &mut String, name: &str, help: &str) {
	let _ = writeln!(out, "# HELP {name} {help}");
	let _ = writeln!(out, "# TYPE {name} counter");
}

fn render_gauge_family_header(out: &mut String, name: &str, help: &str) {
	let _ = writeln!(out, "# HELP {name} {help}");
	let _ = writeln!(out, "# TYPE {name} gauge");
}

fn render_counter_with_labels(out: &mut String, name: &str, labels: &[(&str, &str)], value: u64) {
	let _ = writeln!(out, "{name}{{{}}} {value}", render_labels(labels));
}

fn render_gauge_with_labels(out: &mut String, name: &str, labels: &[(&str, &str)], value: u64) {
	let _ = writeln!(out, "{name}{{{}}} {value}", render_labels(labels));
}

fn render_histogram(out: &mut String, name: &str, help: &str, snapshot: &HistogramSnapshot<'_>) {
	let _ = writeln!(out, "# HELP {name} {help}");
	let _ = writeln!(out, "# TYPE {name} histogram");
	let mut cumulative = 0u64;
	for (idx, bound_micros) in snapshot.bounds_micros.iter().copied().enumerate() {
		cumulative += snapshot.bucket_counts[idx];
		let _ = writeln!(
			out,
			"{name}_bucket{{le=\"{}\"}} {cumulative}",
			render_seconds_from_micros(bound_micros)
		);
	}
	cumulative += snapshot.bucket_counts[snapshot.bounds_micros.len()];
	let _ = writeln!(out, "{name}_bucket{{le=\"+Inf\"}} {cumulative}");
	let _ = writeln!(
		out,
		"{name}_sum {}",
		render_seconds_from_micros(snapshot.sum_micros)
	);
	let _ = writeln!(out, "{name}_count {}", snapshot.count);
}

fn render_histogram_micros(
	out: &mut String,
	name: &str,
	help: &str,
	snapshot: &HistogramSnapshot<'_>,
) {
	let _ = writeln!(out, "# HELP {name} {help}");
	let _ = writeln!(out, "# TYPE {name} histogram");
	let mut cumulative = 0u64;
	for (idx, bound_micros) in snapshot.bounds_micros.iter().copied().enumerate() {
		cumulative += snapshot.bucket_counts[idx];
		let _ = writeln!(out, "{name}_bucket{{le=\"{bound_micros}\"}} {cumulative}");
	}
	cumulative += snapshot.bucket_counts[snapshot.bounds_micros.len()];
	let _ = writeln!(out, "{name}_bucket{{le=\"+Inf\"}} {cumulative}");
	let _ = writeln!(out, "{name}_sum {}", snapshot.sum_micros);
	let _ = writeln!(out, "{name}_count {}", snapshot.count);
}

fn render_histogram_raw(
	out: &mut String,
	name: &str,
	help: &str,
	snapshot: &HistogramSnapshot<'_>,
) {
	let _ = writeln!(out, "# HELP {name} {help}");
	let _ = writeln!(out, "# TYPE {name} histogram");
	let mut cumulative = 0u64;
	for (idx, bound) in snapshot.bounds_micros.iter().copied().enumerate() {
		cumulative += snapshot.bucket_counts[idx];
		let _ = writeln!(out, "{name}_bucket{{le=\"{bound}\"}} {cumulative}");
	}
	cumulative += snapshot.bucket_counts[snapshot.bounds_micros.len()];
	let _ = writeln!(out, "{name}_bucket{{le=\"+Inf\"}} {cumulative}");
	let _ = writeln!(out, "{name}_sum {}", snapshot.sum_micros);
	let _ = writeln!(out, "{name}_count {}", snapshot.count);
}

fn render_histogram_micros_series(
	out: &mut String,
	name: &str,
	labels: &[(&str, &str)],
	snapshot: &HistogramSnapshot<'_>,
) {
	let rendered_labels = render_labels(labels);
	let mut cumulative = 0u64;
	for (idx, bound_micros) in snapshot.bounds_micros.iter().copied().enumerate() {
		cumulative += snapshot.bucket_counts[idx];
		let _ = writeln!(
			out,
			"{name}_bucket{{{rendered_labels},le=\"{bound_micros}\"}} {cumulative}",
		);
	}
	cumulative += snapshot.bucket_counts[snapshot.bounds_micros.len()];
	let _ = writeln!(
		out,
		"{name}_bucket{{{rendered_labels},le=\"+Inf\"}} {cumulative}"
	);
	let _ = writeln!(
		out,
		"{name}_sum{{{rendered_labels}}} {}",
		snapshot.sum_micros
	);
	let _ = writeln!(out, "{name}_count{{{rendered_labels}}} {}", snapshot.count);
}

fn render_labels(labels: &[(&str, &str)]) -> String {
	let mut rendered = String::new();
	for (idx, (key, value)) in labels.iter().enumerate() {
		if idx > 0 {
			rendered.push(',');
		}
		let _ = write!(rendered, "{key}=\"{value}\"");
	}
	rendered
}

fn duration_to_micros(duration: Duration) -> u64 {
	duration.as_micros().min(u64::MAX as u128) as u64
}

fn render_seconds_from_micros(value_micros: u64) -> String {
	let seconds = value_micros / MICROS_PER_SECOND;
	let fractional_micros = value_micros % MICROS_PER_SECOND;
	if fractional_micros == 0 {
		return seconds.to_string();
	}

	let mut fractional = format!("{fractional_micros:06}");
	while fractional.ends_with('0') {
		fractional.pop();
	}
	format!("{seconds}.{fractional}")
}

fn mask_selectivity(rows_scanned: u64, rows_returned: u64) -> f64 {
	if rows_scanned == 0 {
		0.0
	} else {
		rows_returned as f64 / rows_scanned as f64
	}
}

#[cfg(test)]
mod tests {
	use super::{
		Histogram, IoUringClass, MetricsRegistry, MigrationProxyReadOutcome,
		PendingFinalizationReplayPath, PbcRuntime, render_histogram,
	};
	use crate::schema::WriteStatus;
	use std::time::Duration;

	const TEST_BUCKETS_MICROS: &[u64] = &[500, 1_000, 2_500];

	#[test]
	fn histogram_renders_prometheus_seconds_from_integer_micros() {
		let histogram = Histogram::new(TEST_BUCKETS_MICROS);
		histogram.observe_duration(Duration::from_micros(500));
		histogram.observe_duration(Duration::from_micros(750));

		let snapshot = histogram.snapshot();
		let mut rendered = String::new();
		render_histogram(
			&mut rendered,
			"wendelon_test_latency_seconds",
			"test",
			&snapshot,
		);

		assert!(rendered.contains("wendelon_test_latency_seconds_bucket{le=\"0.0005\"} 1"));
		assert!(rendered.contains("wendelon_test_latency_seconds_bucket{le=\"0.001\"} 2"));
		assert!(rendered.contains("wendelon_test_latency_seconds_bucket{le=\"0.0025\"} 2"));
		assert!(rendered.contains("wendelon_test_latency_seconds_bucket{le=\"+Inf\"} 2"));
		assert!(rendered.contains("wendelon_test_latency_seconds_sum 0.00125"));
		assert!(rendered.contains("wendelon_test_latency_seconds_count 2"));
	}

	#[test]
	fn registry_renders_d2_write_and_wal_metrics() {
		let registry = MetricsRegistry::default();
		registry.observe_write_outcome(WriteStatus::OK);
		registry.observe_write_outcome(WriteStatus::FINALIZATION_PENDING);
		registry.observe_write_outcome(WriteStatus::NOT_LEADER);
		registry.observe_write_outcome(WriteStatus(250));
		registry.set_pending_finalization_backlog(3);
		registry.observe_pending_finalization_replay(
			PendingFinalizationReplayPath::DuplicateReplay,
			Duration::from_micros(900),
		);
		registry.observe_migration_proxy_read(
			MigrationProxyReadOutcome::Hit,
			Duration::from_micros(3_400),
		);
		registry.set_io_uring_submission_queue_depth(IoUringClass::Wal, 4);
		registry.set_io_uring_completion_queue_depth(IoUringClass::Slab, 7);
		registry.observe_io_uring_error(IoUringClass::Wal, "write", 5);
		registry.observe_durable_quorum_latency(Duration::from_micros(1_500));
		registry.observe_wal_append_latency(Duration::from_micros(400));
		registry.observe_wal_fsync_latency(Duration::from_micros(2_200));
		registry.observe_pbc_execution(
			PbcRuntime::Vectorized,
			10,
			4,
			Duration::from_micros(125),
			1,
			3,
		);
		registry.observe_pbc_locality_arcs_scanned(6);
		registry.observe_pbc_range_decomposition_overflow();
		registry.observe_pbc_locality_decomposition(Duration::from_micros(80));
		registry.observe_pbc_locality_encode(Duration::from_micros(40));
		registry.pbc_slab_stats_consulted_total.add(3);
		registry.pbc_slab_stats_pruned_total.inc();
		registry.pbc_slab_stats_missing_total.inc();
		registry.pbc_slab_stats_unsupported_total.inc();
		registry
			.pbc_slab_stats_predicate_unrepresentable_total
			.inc();
		registry.pbc_slab_stats_decrypt_failed_total.inc();
		registry.pbc_slab_stats_pointer_drift_total.inc();
		registry.pbc_slab_stats_cache_hits_total.inc();
		registry.pbc_slab_stats_cache_misses_total.inc();
		registry.pbc_slab_stats_skipped_no_capability_total.inc();
		registry.observe_pbc_slab_stats_build(Duration::from_micros(70));
		registry.observe_pbc_slab_stats_decrypt(Duration::from_micros(30));
		registry.observe_pbc_slab_stats_block_bytes(16_384);

		let rendered = registry.render_prometheus();
		assert!(rendered.contains("wendelon_data_ring_write_outcome_total{outcome=\"ok\"} 1"));
		assert!(rendered.contains(
			"wendelon_data_ring_write_outcome_total{outcome=\"finalization_pending\"} 1"
		));
		assert!(
			rendered.contains("wendelon_data_ring_write_outcome_total{outcome=\"not_leader\"} 1")
		);
		assert!(rendered.contains("wendelon_pbc_rows_scanned_total{runtime=\"vectorized\"} 10"));
		assert!(rendered.contains("wendelon_pbc_rows_returned_total{runtime=\"vectorized\"} 4"));
		assert!(rendered.contains("wendelon_pbc_mask_selectivity 0.400000"));
		assert!(
			rendered
				.contains("wendelon_pbc_exec_micros_bucket{runtime=\"vectorized\",le=\"250\"} 1")
		);
		assert!(rendered.contains("wendelon_pbc_kernel_error_total{runtime=\"vectorized\"} 1"));
		assert!(
			rendered.contains("wendelon_pbc_intermediate_arrays_total{runtime=\"vectorized\"} 3")
		);
		assert!(rendered.contains("wendelon_pbc_locality_arcs_scanned_total 6"));
		assert!(rendered.contains("wendelon_pbc_range_decomposition_overflow_total 1"));
		assert!(
			rendered.contains("wendelon_pbc_locality_decomposition_micros_bucket{le=\"100\"} 1")
		);
		assert!(rendered.contains("wendelon_pbc_locality_decomposition_micros_count 1"));
		assert!(rendered.contains("wendelon_pbc_locality_encode_micros_bucket{le=\"50\"} 1"));
		assert!(rendered.contains("wendelon_pbc_locality_encode_micros_count 1"));
		assert!(rendered.contains("wendelon_pbc_slab_stats_consulted_total 3"));
		assert!(rendered.contains("wendelon_pbc_slab_stats_pruned_total 1"));
		assert!(rendered.contains("wendelon_pbc_slab_stats_missing_total 1"));
		assert!(rendered.contains("wendelon_pbc_slab_stats_unsupported_total 1"));
		assert!(rendered.contains("wendelon_pbc_slab_stats_predicate_unrepresentable_total 1"));
		assert!(rendered.contains("wendelon_pbc_slab_stats_decrypt_failed_total 1"));
		assert!(rendered.contains("wendelon_pbc_slab_stats_pointer_drift_total 1"));
		assert!(rendered.contains("wendelon_pbc_slab_stats_cache_hits_total 1"));
		assert!(rendered.contains("wendelon_pbc_slab_stats_cache_misses_total 1"));
		assert!(rendered.contains("wendelon_pbc_slab_stats_skipped_no_capability_total 1"));
		assert!(rendered.contains("wendelon_pbc_slab_stats_build_micros_bucket{le=\"100\"} 1"));
		assert!(rendered.contains("wendelon_pbc_slab_stats_decrypt_micros_bucket{le=\"50\"} 1"));
		assert!(rendered.contains("wendelon_pbc_slab_stats_block_bytes_bucket{le=\"16384\"} 1"));
		assert!(
			rendered.contains("wendelon_data_ring_write_outcome_total{outcome=\"unknown_250\"} 1")
		);
		assert!(rendered.contains("wendelon_data_ring_pending_finalization_backlog 3"));
		assert!(rendered.contains(
			"wendelon_data_ring_pending_finalization_replay_total{path=\"duplicate_replay\"} 1"
		));
		assert!(
			rendered.contains("wendelon_data_ring_migration_proxy_reads_total{outcome=\"hit\"} 1")
		);
		assert!(
			rendered.contains("wendelon_data_ring_migration_proxy_read_latency_seconds_count 1")
		);
		assert!(rendered.contains("wendelon_io_uring_submission_queue_depth{class=\"wal\"} 4"));
		assert!(rendered.contains("wendelon_io_uring_completion_queue_depth{class=\"slab\"} 7"));
		assert!(
			rendered.contains(
				"wendelon_io_uring_errors_total{class=\"wal\",op=\"write\",errno=\"5\"} 1"
			)
		);
		assert!(rendered.contains("wendelon_data_ring_durable_quorum_latency_seconds_count 1"));
		assert!(rendered.contains("wendelon_storage_wal_append_latency_seconds_count 1"));
		assert!(rendered.contains("wendelon_storage_wal_fsync_latency_seconds_count 1"));
	}
}
