use std::fmt::Write as _;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

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

#[derive(Default)]
pub struct MetricsRegistry {
	pub metrics_scrapes_total: Counter,

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

	pub data_ring_write_requests_total: Counter,
	pub data_ring_read_requests_total: Counter,
	pub data_ring_query_requests_total: Counter,
	pub data_ring_migrate_requests_total: Counter,
	pub data_ring_lease_refresh_passes_total: Counter,
	pub data_ring_lease_grants_total: Counter,
	pub data_ring_lease_denials_total: Counter,
	pub data_ring_lease_errors_total: Counter,
	pub data_ring_lease_cache_entries: Gauge,

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
}

impl MetricsRegistry {
	pub fn render_prometheus(&self) -> String {
		let mut out = String::with_capacity(4096);
		render_counter(
			&mut out,
			"wendelon_metrics_scrapes_total",
			"Total successful metrics scrapes.",
			self.metrics_scrapes_total.get(),
		);
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
