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

	pub data_ring_write_requests_total: Counter,
	pub data_ring_read_requests_total: Counter,
	pub data_ring_query_requests_total: Counter,
	pub data_ring_migrate_requests_total: Counter,
	pub data_ring_lease_refresh_passes_total: Counter,
	pub data_ring_lease_grants_total: Counter,
	pub data_ring_lease_denials_total: Counter,
	pub data_ring_lease_errors_total: Counter,
	pub data_ring_lease_cache_entries: Gauge,

	pub group_commit_window_seconds: Histogram,
	pub l1_read_latency_seconds: Histogram,
	pub l2_upload_latency_seconds: Histogram,
	pub l2_range_read_latency_seconds: Histogram,

	pub storage_tiering_passes_total: Counter,
	pub storage_tiering_pass_errors_total: Counter,
	pub storage_tiered_slabs_total: Counter,
	pub storage_tiering_failures_total: Counter,
	pub storage_tiering_skipped_small_total: Counter,
	pub storage_tiering_skipped_already_tiered_total: Counter,
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
	pub audit_sink_failures_syslog_total: Counter,
	pub audit_sink_failures_syslog_queue_dropped_total: Counter,
	pub audit_sink_failures_kafka_total: Counter,
	pub audit_sink_failures_kafka_queue_dropped_total: Counter,
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

			data_ring_write_requests_total: Counter::default(),
			data_ring_read_requests_total: Counter::default(),
			data_ring_query_requests_total: Counter::default(),
			data_ring_migrate_requests_total: Counter::default(),
			data_ring_lease_refresh_passes_total: Counter::default(),
			data_ring_lease_grants_total: Counter::default(),
			data_ring_lease_denials_total: Counter::default(),
			data_ring_lease_errors_total: Counter::default(),
			data_ring_lease_cache_entries: Gauge::default(),

			group_commit_window_seconds: Histogram::new(GROUP_COMMIT_WINDOW_BUCKETS_MICROS),
			l1_read_latency_seconds: Histogram::new(LOCAL_READ_LATENCY_BUCKETS_MICROS),
			l2_upload_latency_seconds: Histogram::new(L2_UPLOAD_LATENCY_BUCKETS_MICROS),
			l2_range_read_latency_seconds: Histogram::new(L2_RANGE_READ_LATENCY_BUCKETS_MICROS),

			storage_tiering_passes_total: Counter::default(),
			storage_tiering_pass_errors_total: Counter::default(),
			storage_tiered_slabs_total: Counter::default(),
			storage_tiering_failures_total: Counter::default(),
			storage_tiering_skipped_small_total: Counter::default(),
			storage_tiering_skipped_already_tiered_total: Counter::default(),
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
			audit_sink_failures_syslog_total: Counter::default(),
			audit_sink_failures_syslog_queue_dropped_total: Counter::default(),
			audit_sink_failures_kafka_total: Counter::default(),
			audit_sink_failures_kafka_queue_dropped_total: Counter::default(),
			otlp_spans_dropped_total: Counter::default(),
			fips_module_self_test_ok_total: Counter::default(),

			crypto_decrypt_audit_emit_failures_total: Counter::default(),
		}
	}
}

impl MetricsRegistry {
	pub fn set_raft_observability_state(&self, epoch: u64, leader_id: Option<u32>) {
		self.raft_membership_epoch.set(epoch);
		*self
			.raft_current_leader_id
			.lock()
			.expect("raft leader gauge mutex poisoned") = leader_id.map(|id| format!("n-{id:02}"));
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
		render_histogram(
			&mut out,
			"wendelon_group_commit_window_seconds",
			"Time writes spent coalescing in the group-commit window before fdatasync.",
			&self.group_commit_window_seconds.snapshot(),
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

#[cfg(test)]
mod tests {
	use super::{Histogram, render_histogram};
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
}
