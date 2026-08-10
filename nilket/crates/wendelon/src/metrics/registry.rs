use crate::schema::{
	CdcRejectCode, CompactionLevelIndex, IndexObjectId, RecordBodyCompressionPolicy, TableObjectId,
	TenantId, WriteStatus,
};
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
const AGG_EXEC_BUCKETS_MICROS: &[u64] = &[
	25, 50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000,
];
const AGG_PARTIAL_BYTES_BUCKETS: &[u64] = &[
	8, 32, 128, 512, 2_048, 8_192, 32_768, 131_072, 524_288, 2_097_152,
];
const MULTIROW_ROWS_PER_RECORD_BUCKETS: &[u64] =
	&[1, 2, 4, 8, 16, 32, 64, 128, 256, 1024, 4096, 65536];
const L2_COLUMN_FETCH_CHUNK_RUN_BUCKETS: &[u64] = &[1, 2, 4, 8, 16, 32, 64];
const COMPRESSION_RATIO_MILLI_BUCKETS: &[u64] = &[250, 500, 750, 1000, 1250, 1500, 2000];
const PK_ADMISSION_VISIBILITY_BUCKETS_MICROS: &[u64] =
	&[1, 2, 5, 10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000];

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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TableRetentionMetricPolicy {
	InheritCluster,
	Disabled,
	FixedWindow,
}

impl TableRetentionMetricPolicy {
	fn as_str(self) -> &'static str {
		match self {
			Self::InheritCluster => "inherit_cluster",
			Self::Disabled => "disabled",
			Self::FixedWindow => "fixed_window",
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NetworkCompressionPath {
	ClientBatch,
	MigrationRecords,
	QueryResponse,
	ReadResponse,
	CdcEvent,
}

impl NetworkCompressionPath {
	fn as_str(self) -> &'static str {
		match self {
			Self::ClientBatch => "client_batch",
			Self::MigrationRecords => "migration_records",
			Self::QueryResponse => "query_response",
			Self::ReadResponse => "read_response",
			Self::CdcEvent => "cdc_event",
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CdcMetricSource {
	Ring,
	Catchup,
}

impl CdcMetricSource {
	pub const ALL: [Self; 2] = [Self::Ring, Self::Catchup];

	fn as_str(self) -> &'static str {
		match self {
			Self::Ring => "ring",
			Self::Catchup => "catchup",
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CdcSubscriptionMetricResult {
	Opened,
	Closed,
	Rejected,
	Cancelled,
}

impl CdcSubscriptionMetricResult {
	pub const ALL: [Self; 4] = [Self::Opened, Self::Closed, Self::Rejected, Self::Cancelled];

	fn as_str(self) -> &'static str {
		match self {
			Self::Opened => "opened",
			Self::Closed => "closed",
			Self::Rejected => "rejected",
			Self::Cancelled => "cancelled",
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LsmCompactionResult {
	Ok,
	ManifestRace,
	SidecarDigestMismatch,
	VisibleDigestInvariance,
	Aborted,
}

impl LsmCompactionResult {
	pub const ALL: [Self; 5] = [
		Self::Ok,
		Self::ManifestRace,
		Self::SidecarDigestMismatch,
		Self::VisibleDigestInvariance,
		Self::Aborted,
	];

	fn as_str(self) -> &'static str {
		match self {
			Self::Ok => "ok",
			Self::ManifestRace => "manifest_race",
			Self::SidecarDigestMismatch => "sidecar_digest_mismatch",
			Self::VisibleDigestInvariance => "visible_digest_invariance",
			Self::Aborted => "aborted",
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LsmDigestInvariantKind {
	VisibleDomain,
	VersionEntry,
}

impl LsmDigestInvariantKind {
	pub const ALL: [Self; 2] = [Self::VisibleDomain, Self::VersionEntry];

	fn as_str(self) -> &'static str {
		match self {
			Self::VisibleDomain => "visible_domain",
			Self::VersionEntry => "version_entry",
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NetworkCompressionResult {
	Compressed,
	Error,
}

impl NetworkCompressionResult {
	fn as_str(self) -> &'static str {
		match self {
			Self::Compressed => "compressed",
			Self::Error => "error",
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TableRetentionRowKind {
	Data,
	Tombstone,
}

impl TableRetentionRowKind {
	fn as_str(self) -> &'static str {
		match self {
			Self::Data => "data",
			Self::Tombstone => "tombstone",
		}
	}
}

#[derive(Default)]
struct TableRetentionPolicyCounters {
	inherit_cluster: Counter,
	disabled: Counter,
	fixed_window: Counter,
}

impl TableRetentionPolicyCounters {
	fn counter(&self, policy: TableRetentionMetricPolicy) -> &Counter {
		match policy {
			TableRetentionMetricPolicy::InheritCluster => &self.inherit_cluster,
			TableRetentionMetricPolicy::Disabled => &self.disabled,
			TableRetentionMetricPolicy::FixedWindow => &self.fixed_window,
		}
	}

	fn inc(&self, policy: TableRetentionMetricPolicy) {
		self.counter(policy).inc();
	}

	fn render(&self, out: &mut String, name: &str, help: &str) {
		render_counter_family_header(out, name, help);
		for policy in [
			TableRetentionMetricPolicy::InheritCluster,
			TableRetentionMetricPolicy::Disabled,
			TableRetentionMetricPolicy::FixedWindow,
		] {
			render_counter_with_labels(
				out,
				name,
				&[("policy", policy.as_str())],
				self.counter(policy).get(),
			);
		}
	}
}

#[derive(Default)]
struct TableRetentionRowKindCounters {
	data: Counter,
	tombstone: Counter,
}

impl TableRetentionRowKindCounters {
	fn counter(&self, kind: TableRetentionRowKind) -> &Counter {
		match kind {
			TableRetentionRowKind::Data => &self.data,
			TableRetentionRowKind::Tombstone => &self.tombstone,
		}
	}

	fn add(&self, kind: TableRetentionRowKind, value: u64) {
		self.counter(kind).add(value);
	}

	fn render(&self, out: &mut String, name: &str, help: &str) {
		render_counter_family_header(out, name, help);
		for kind in [
			TableRetentionRowKind::Data,
			TableRetentionRowKind::Tombstone,
		] {
			render_counter_with_labels(
				out,
				name,
				&[("kind", kind.as_str())],
				self.counter(kind).get(),
			);
		}
	}
}

#[derive(Default)]
struct TableRetentionActiveTableGauges {
	inherit: Gauge,
	disabled: Gauge,
	fixed: Gauge,
}

impl TableRetentionActiveTableGauges {
	fn set(&self, inherit: u64, disabled: u64, fixed: u64) {
		self.inherit.set(inherit);
		self.disabled.set(disabled);
		self.fixed.set(fixed);
	}

	fn render(&self, out: &mut String, name: &str, help: &str) {
		render_gauge_family_header(out, name, help);
		render_gauge_with_labels(out, name, &[("mode", "inherit")], self.inherit.get());
		render_gauge_with_labels(out, name, &[("mode", "disabled")], self.disabled.get());
		render_gauge_with_labels(out, name, &[("mode", "fixed")], self.fixed.get());
	}
}

#[derive(Default)]
struct SecondaryIndexCounters(Mutex<BTreeMap<(u64, String, String), u64>>);

impl SecondaryIndexCounters {
	fn initialize(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		index_object_id: IndexObjectId,
	) {
		let mut counters = self
			.0
			.lock()
			.expect("secondary-index counter mutex poisoned");
		counters
			.entry((
				tenant_id.get(),
				table_object_id_label(table_object_id),
				index_object_id_label(index_object_id),
			))
			.or_insert(0);
	}

	fn add(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		index_object_id: IndexObjectId,
		value: u64,
	) {
		if value == 0 {
			return;
		}
		let mut counters = self
			.0
			.lock()
			.expect("secondary-index counter mutex poisoned");
		*counters
			.entry((
				tenant_id.get(),
				table_object_id_label(table_object_id),
				index_object_id_label(index_object_id),
			))
			.or_insert(0) += value;
	}

	fn render(&self, out: &mut String, name: &str, help: &str) {
		render_counter_family_header(out, name, help);
		let counters = self
			.0
			.lock()
			.expect("secondary-index counter mutex poisoned");
		for ((tenant, table, index), value) in counters.iter() {
			render_counter_with_labels(
				out,
				name,
				&[
					("tenant", &tenant.to_string()),
					("table", table.as_str()),
					("index", index.as_str()),
				],
				*value,
			);
		}
	}
}

#[derive(Default)]
struct SecondaryIndexReasonCounters(Mutex<BTreeMap<(u64, String, String, String), u64>>);

impl SecondaryIndexReasonCounters {
	fn add(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		index_object_id: IndexObjectId,
		reason: &str,
		value: u64,
	) {
		if value == 0 {
			return;
		}
		let mut counters = self
			.0
			.lock()
			.expect("secondary-index reason counter mutex poisoned");
		*counters
			.entry((
				tenant_id.get(),
				table_object_id_label(table_object_id),
				index_object_id_label(index_object_id),
				reason.to_string(),
			))
			.or_insert(0) += value;
	}

	fn render(&self, out: &mut String, name: &str, help: &str) {
		render_counter_family_header(out, name, help);
		let counters = self
			.0
			.lock()
			.expect("secondary-index reason counter mutex poisoned");
		for ((tenant, table, index, reason), value) in counters.iter() {
			render_counter_with_labels(
				out,
				name,
				&[
					("tenant", &tenant.to_string()),
					("table", table.as_str()),
					("index", index.as_str()),
					("reason", reason.as_str()),
				],
				*value,
			);
		}
	}
}

#[derive(Default)]
struct SecondaryIndexModeCounters(Mutex<BTreeMap<(u64, String, String, String), u64>>);

impl SecondaryIndexModeCounters {
	fn add(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		index_object_id: IndexObjectId,
		mode: &str,
		value: u64,
	) {
		if value == 0 {
			return;
		}
		let mut counters = self
			.0
			.lock()
			.expect("secondary-index mode counter mutex poisoned");
		*counters
			.entry((
				tenant_id.get(),
				table_object_id_label(table_object_id),
				index_object_id_label(index_object_id),
				mode.to_string(),
			))
			.or_insert(0) += value;
	}

	fn render(&self, out: &mut String, name: &str, help: &str) {
		render_counter_family_header(out, name, help);
		let counters = self
			.0
			.lock()
			.expect("secondary-index mode counter mutex poisoned");
		for ((tenant, table, index, mode), value) in counters.iter() {
			render_counter_with_labels(
				out,
				name,
				&[
					("tenant", &tenant.to_string()),
					("table", table.as_str()),
					("index", index.as_str()),
					("mode", mode.as_str()),
				],
				*value,
			);
		}
	}
}

#[derive(Default)]
struct SecondaryIndexTransitionCounters(
	Mutex<BTreeMap<(u64, String, String, String, String), u64>>,
);

impl SecondaryIndexTransitionCounters {
	fn add(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		index_object_id: IndexObjectId,
		from: &str,
		to: &str,
	) {
		let mut counters = self
			.0
			.lock()
			.expect("secondary-index transition counter mutex poisoned");
		*counters
			.entry((
				tenant_id.get(),
				table_object_id_label(table_object_id),
				index_object_id_label(index_object_id),
				from.to_string(),
				to.to_string(),
			))
			.or_insert(0) += 1;
	}

	fn render(&self, out: &mut String, name: &str, help: &str) {
		render_counter_family_header(out, name, help);
		let counters = self
			.0
			.lock()
			.expect("secondary-index transition counter mutex poisoned");
		for ((tenant, table, index, from, to), value) in counters.iter() {
			render_counter_with_labels(
				out,
				name,
				&[
					("tenant", &tenant.to_string()),
					("table", table.as_str()),
					("index", index.as_str()),
					("from", from.as_str()),
					("to", to.as_str()),
				],
				*value,
			);
		}
	}
}

#[derive(Default)]
struct SecondaryIndexGauges(Mutex<BTreeMap<(u64, String, String), u64>>);

impl SecondaryIndexGauges {
	fn set(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		index_object_id: IndexObjectId,
		value: u64,
	) {
		let mut gauges = self.0.lock().expect("secondary-index gauge mutex poisoned");
		gauges.insert(
			(
				tenant_id.get(),
				table_object_id_label(table_object_id),
				index_object_id_label(index_object_id),
			),
			value,
		);
	}

	fn render(&self, out: &mut String, name: &str, help: &str) {
		render_gauge_family_header(out, name, help);
		let gauges = self.0.lock().expect("secondary-index gauge mutex poisoned");
		for ((tenant, table, index), value) in gauges.iter() {
			render_gauge_with_labels(
				out,
				name,
				&[
					("tenant", &tenant.to_string()),
					("table", table.as_str()),
					("index", index.as_str()),
				],
				*value,
			);
		}
	}
}

struct SecondaryIndexDurationHistograms(Mutex<BTreeMap<(u64, String, String), Histogram>>);

impl Default for SecondaryIndexDurationHistograms {
	fn default() -> Self {
		Self(Mutex::new(BTreeMap::new()))
	}
}

impl SecondaryIndexDurationHistograms {
	fn observe(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		index_object_id: IndexObjectId,
		duration: Duration,
	) {
		let mut histograms = self
			.0
			.lock()
			.expect("secondary-index duration histogram mutex poisoned");
		histograms
			.entry((
				tenant_id.get(),
				table_object_id_label(table_object_id),
				index_object_id_label(index_object_id),
			))
			.or_insert_with(|| Histogram::new(PBC_EXEC_BUCKETS_MICROS))
			.observe_duration(duration);
	}

	fn render(&self, out: &mut String, name: &str, help: &str) {
		let histograms = self
			.0
			.lock()
			.expect("secondary-index duration histogram mutex poisoned");
		render_histogram_family_header(out, name, help);
		for ((tenant, table, index), histogram) in histograms.iter() {
			render_histogram_series(
				out,
				name,
				&[
					("tenant", &tenant.to_string()),
					("table", table.as_str()),
					("index", index.as_str()),
				],
				&histogram.snapshot(),
			);
		}
	}
}

#[derive(Default)]
struct ContentHashAdmissionCounters(Mutex<BTreeMap<(u64, String, String), u64>>);

impl ContentHashAdmissionCounters {
	fn add(&self, tenant_id: TenantId, table_object_id: TableObjectId, outcome: &str, value: u64) {
		if value == 0 {
			return;
		}
		let mut counters = self
			.0
			.lock()
			.expect("content-hash admission counter mutex poisoned");
		*counters
			.entry((
				tenant_id.get(),
				table_object_id_label(table_object_id),
				outcome.to_string(),
			))
			.or_insert(0) += value;
	}

	fn render(&self, out: &mut String, name: &str, help: &str) {
		render_counter_family_header(out, name, help);
		let counters = self
			.0
			.lock()
			.expect("content-hash admission counter mutex poisoned");
		for ((tenant, table, outcome), value) in counters.iter() {
			render_counter_with_labels(
				out,
				name,
				&[
					("tenant", &tenant.to_string()),
					("table", table.as_str()),
					("outcome", outcome.as_str()),
				],
				*value,
			);
		}
	}
}

#[derive(Default)]
struct ContentHashTableCounters(Mutex<BTreeMap<(u64, String), u64>>);

impl ContentHashTableCounters {
	fn add(&self, tenant_id: TenantId, table_object_id: TableObjectId, value: u64) {
		if value == 0 {
			return;
		}
		let mut counters = self
			.0
			.lock()
			.expect("content-hash table counter mutex poisoned");
		*counters
			.entry((tenant_id.get(), table_object_id_label(table_object_id)))
			.or_insert(0) += value;
	}

	fn render(&self, out: &mut String, name: &str, help: &str) {
		render_counter_family_header(out, name, help);
		let counters = self
			.0
			.lock()
			.expect("content-hash table counter mutex poisoned");
		for ((tenant, table), value) in counters.iter() {
			render_counter_with_labels(
				out,
				name,
				&[("tenant", &tenant.to_string()), ("table", table.as_str())],
				*value,
			);
		}
	}
}

#[derive(Default)]
struct DescriptorFingerprintTableCounters(Mutex<BTreeMap<(u64, String), u64>>);

impl DescriptorFingerprintTableCounters {
	fn add(&self, tenant_id: TenantId, table_object_id: TableObjectId, value: u64) {
		if value == 0 {
			return;
		}
		let mut counters = self
			.0
			.lock()
			.expect("descriptor fingerprint table counter mutex poisoned");
		*counters
			.entry((tenant_id.get(), table_object_id_label(table_object_id)))
			.or_insert(0) += value;
	}

	#[cfg(test)]
	fn get(&self, tenant_id: TenantId, table_object_id: TableObjectId) -> u64 {
		let counters = self
			.0
			.lock()
			.expect("descriptor fingerprint table counter mutex poisoned");
		counters
			.get(&(tenant_id.get(), table_object_id_label(table_object_id)))
			.copied()
			.unwrap_or(0)
	}

	fn render(&self, out: &mut String, name: &str, help: &str) {
		render_counter_family_header(out, name, help);
		let counters = self
			.0
			.lock()
			.expect("descriptor fingerprint table counter mutex poisoned");
		for ((tenant, table), value) in counters.iter() {
			render_counter_with_labels(
				out,
				name,
				&[("tenant", &tenant.to_string()), ("table", table.as_str())],
				*value,
			);
		}
	}
}

#[derive(Default)]
struct DescriptorFingerprintReasonCounters(Mutex<BTreeMap<(u64, String, String), u64>>);

impl DescriptorFingerprintReasonCounters {
	fn add(&self, tenant_id: TenantId, table_object_id: TableObjectId, reason: &str, value: u64) {
		if value == 0 {
			return;
		}
		let mut counters = self
			.0
			.lock()
			.expect("descriptor fingerprint reason counter mutex poisoned");
		*counters
			.entry((
				tenant_id.get(),
				table_object_id_label(table_object_id),
				reason.to_string(),
			))
			.or_insert(0) += value;
	}

	fn render(&self, out: &mut String, name: &str, help: &str) {
		render_counter_family_header(out, name, help);
		let counters = self
			.0
			.lock()
			.expect("descriptor fingerprint reason counter mutex poisoned");
		for ((tenant, table, reason), value) in counters.iter() {
			render_counter_with_labels(
				out,
				name,
				&[
					("tenant", &tenant.to_string()),
					("table", table.as_str()),
					("reason", reason.as_str()),
				],
				*value,
			);
		}
	}
}

#[derive(Default)]
struct DescriptorFingerprintTableHistograms(Mutex<BTreeMap<(u64, String), Histogram>>);

impl DescriptorFingerprintTableHistograms {
	fn observe_duration(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		duration: Duration,
	) {
		let mut histograms = self
			.0
			.lock()
			.expect("descriptor fingerprint table histogram mutex poisoned");
		histograms
			.entry((tenant_id.get(), table_object_id_label(table_object_id)))
			.or_insert_with(|| Histogram::new(PBC_SLAB_STATS_BUCKETS_MICROS))
			.observe_duration(duration);
	}

	fn render(&self, out: &mut String, name: &str, help: &str) {
		let _ = writeln!(out, "# HELP {name} {help}");
		let _ = writeln!(out, "# TYPE {name} histogram");
		let histograms = self
			.0
			.lock()
			.expect("descriptor fingerprint table histogram mutex poisoned");
		for ((tenant, table), histogram) in histograms.iter() {
			let tenant = tenant.to_string();
			render_histogram_series(
				out,
				name,
				&[("tenant", tenant.as_str()), ("table", table.as_str())],
				&histogram.snapshot(),
			);
		}
	}
}

#[derive(Default)]
struct PkDeleteOutcomeCounters(Mutex<BTreeMap<(u64, String, String), u64>>);

impl PkDeleteOutcomeCounters {
	fn add(&self, tenant_id: TenantId, table_object_id: TableObjectId, outcome: &str, value: u64) {
		if value == 0 {
			return;
		}
		let mut counters = self
			.0
			.lock()
			.expect("PK delete outcome counter mutex poisoned");
		*counters
			.entry((
				tenant_id.get(),
				table_object_id_label(table_object_id),
				outcome.to_string(),
			))
			.or_insert(0) += value;
	}

	fn render(&self, out: &mut String, name: &str, help: &str) {
		render_counter_family_header(out, name, help);
		let counters = self
			.0
			.lock()
			.expect("PK delete outcome counter mutex poisoned");
		for ((tenant, table, outcome), value) in counters.iter() {
			render_counter_with_labels(
				out,
				name,
				&[
					("tenant", &tenant.to_string()),
					("table", table.as_str()),
					("outcome", outcome.as_str()),
				],
				*value,
			);
		}
	}
}

#[derive(Default)]
struct PkTableCounters(Mutex<BTreeMap<(u64, String), u64>>);

impl PkTableCounters {
	fn add(&self, tenant_id: TenantId, table_object_id: TableObjectId, value: u64) {
		if value == 0 {
			return;
		}
		let mut counters = self.0.lock().expect("PK table counter mutex poisoned");
		*counters
			.entry((tenant_id.get(), table_object_id_label(table_object_id)))
			.or_insert(0) += value;
	}

	fn render(&self, out: &mut String, name: &str, help: &str) {
		render_counter_family_header(out, name, help);
		let counters = self.0.lock().expect("PK table counter mutex poisoned");
		for ((tenant, table), value) in counters.iter() {
			render_counter_with_labels(
				out,
				name,
				&[("tenant", &tenant.to_string()), ("table", table.as_str())],
				*value,
			);
		}
	}
}

#[derive(Default)]
struct PkAdmissionVisibilityHistograms(Mutex<BTreeMap<(u64, String), Histogram>>);

impl PkAdmissionVisibilityHistograms {
	fn observe_duration(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		duration: Duration,
	) {
		let mut histograms = self
			.0
			.lock()
			.expect("PK visibility histogram mutex poisoned");
		histograms
			.entry((tenant_id.get(), table_object_id_label(table_object_id)))
			.or_insert_with(|| Histogram::new(PK_ADMISSION_VISIBILITY_BUCKETS_MICROS))
			.observe_duration(duration);
	}

	fn render(&self, out: &mut String, name: &str, help: &str) {
		let _ = writeln!(out, "# HELP {name} {help}");
		let _ = writeln!(out, "# TYPE {name} histogram");
		let histograms = self
			.0
			.lock()
			.expect("PK visibility histogram mutex poisoned");
		for ((tenant, table), histogram) in histograms.iter() {
			let tenant = tenant.to_string();
			render_histogram_series(
				out,
				name,
				&[("tenant", tenant.as_str()), ("table", table.as_str())],
				&histogram.snapshot(),
			);
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

#[derive(Default)]
struct CompressionCodecCounters {
	uncompressed: Counter,
	lz4_frame: Counter,
}

impl CompressionCodecCounters {
	fn counter(&self, codec: RecordBodyCompressionPolicy) -> &Counter {
		match codec {
			RecordBodyCompressionPolicy::Uncompressed => &self.uncompressed,
			RecordBodyCompressionPolicy::Lz4Frame => &self.lz4_frame,
		}
	}

	fn inc(&self, codec: RecordBodyCompressionPolicy) {
		self.counter(codec).inc();
	}

	fn add(&self, codec: RecordBodyCompressionPolicy, value: u64) {
		self.counter(codec).add(value);
	}

	fn render(&self, out: &mut String, name: &str, help: &str) {
		render_counter_family_header(out, name, help);
		for codec in [
			RecordBodyCompressionPolicy::Uncompressed,
			RecordBodyCompressionPolicy::Lz4Frame,
		] {
			render_counter_with_labels(
				out,
				name,
				&[("codec", codec.codec_label())],
				self.counter(codec).get(),
			);
		}
	}
}

struct CompressionCodecHistograms {
	uncompressed: Histogram,
	lz4_frame: Histogram,
}

impl CompressionCodecHistograms {
	fn new(bounds: &'static [u64]) -> Self {
		Self {
			uncompressed: Histogram::new(bounds),
			lz4_frame: Histogram::new(bounds),
		}
	}

	fn histogram(&self, codec: RecordBodyCompressionPolicy) -> &Histogram {
		match codec {
			RecordBodyCompressionPolicy::Uncompressed => &self.uncompressed,
			RecordBodyCompressionPolicy::Lz4Frame => &self.lz4_frame,
		}
	}

	fn observe_duration(&self, codec: RecordBodyCompressionPolicy, duration: Duration) {
		self.histogram(codec).observe_duration(duration);
	}

	fn observe_value(&self, codec: RecordBodyCompressionPolicy, value: u64) {
		self.histogram(codec).observe_value(value);
	}

	fn render_micros(&self, out: &mut String, name: &str, help: &str) {
		let _ = writeln!(out, "# HELP {name} {help}");
		let _ = writeln!(out, "# TYPE {name} histogram");
		for codec in [
			RecordBodyCompressionPolicy::Uncompressed,
			RecordBodyCompressionPolicy::Lz4Frame,
		] {
			render_histogram_micros_series(
				out,
				name,
				&[("codec", codec.codec_label())],
				&self.histogram(codec).snapshot(),
			);
		}
	}

	fn render_ratio(&self, out: &mut String, name: &str, help: &str) {
		let _ = writeln!(out, "# HELP {name} {help}");
		let _ = writeln!(out, "# TYPE {name} histogram");
		for codec in [
			RecordBodyCompressionPolicy::Uncompressed,
			RecordBodyCompressionPolicy::Lz4Frame,
		] {
			render_histogram_ratio_series(
				out,
				name,
				&[("codec", codec.codec_label())],
				&self.histogram(codec).snapshot(),
			);
		}
	}
}

#[derive(Default)]
struct NetworkCompressionBatchCounters(
	Mutex<BTreeMap<(&'static str, &'static str, &'static str), u64>>,
);

impl NetworkCompressionBatchCounters {
	fn inc(
		&self,
		codec: RecordBodyCompressionPolicy,
		path: NetworkCompressionPath,
		result: NetworkCompressionResult,
	) {
		let mut counters = self
			.0
			.lock()
			.expect("network compression batch counter mutex poisoned");
		*counters
			.entry((codec.codec_label(), path.as_str(), result.as_str()))
			.or_insert(0) += 1;
	}

	fn render(&self, out: &mut String, name: &str, help: &str) {
		render_counter_family_header(out, name, help);
		let counters = self
			.0
			.lock()
			.expect("network compression batch counter mutex poisoned");
		for ((codec, path, result), value) in counters.iter() {
			render_counter_with_labels(
				out,
				name,
				&[("codec", *codec), ("path", *path), ("result", *result)],
				*value,
			);
		}
	}
}

#[derive(Default)]
struct NetworkCompressionCodecPathCounters(Mutex<BTreeMap<(&'static str, &'static str), u64>>);

impl NetworkCompressionCodecPathCounters {
	fn add(&self, codec: RecordBodyCompressionPolicy, path: NetworkCompressionPath, value: u64) {
		if value == 0 {
			return;
		}
		let mut counters = self
			.0
			.lock()
			.expect("network compression codec/path counter mutex poisoned");
		*counters
			.entry((codec.codec_label(), path.as_str()))
			.or_insert(0) += value;
	}

	fn render(&self, out: &mut String, name: &str, help: &str) {
		render_counter_family_header(out, name, help);
		let counters = self
			.0
			.lock()
			.expect("network compression codec/path counter mutex poisoned");
		for ((codec, path), value) in counters.iter() {
			render_counter_with_labels(out, name, &[("codec", *codec), ("path", *path)], *value);
		}
	}
}

struct NetworkCompressionCodecPathHistograms {
	bounds: &'static [u64],
	histograms: Mutex<BTreeMap<(&'static str, &'static str), Histogram>>,
}

impl NetworkCompressionCodecPathHistograms {
	fn new(bounds: &'static [u64]) -> Self {
		Self {
			bounds,
			histograms: Mutex::new(BTreeMap::new()),
		}
	}

	fn observe_duration(
		&self,
		codec: RecordBodyCompressionPolicy,
		path: NetworkCompressionPath,
		duration: Duration,
	) {
		let mut histograms = self
			.histograms
			.lock()
			.expect("network compression codec/path histogram mutex poisoned");
		histograms
			.entry((codec.codec_label(), path.as_str()))
			.or_insert_with(|| Histogram::new(self.bounds))
			.observe_duration(duration);
	}

	fn observe_value(
		&self,
		codec: RecordBodyCompressionPolicy,
		path: NetworkCompressionPath,
		value: u64,
	) {
		let mut histograms = self
			.histograms
			.lock()
			.expect("network compression codec/path histogram mutex poisoned");
		histograms
			.entry((codec.codec_label(), path.as_str()))
			.or_insert_with(|| Histogram::new(self.bounds))
			.observe_value(value);
	}

	fn render_micros(&self, out: &mut String, name: &str, help: &str) {
		let _ = writeln!(out, "# HELP {name} {help}");
		let _ = writeln!(out, "# TYPE {name} histogram");
		let histograms = self
			.histograms
			.lock()
			.expect("network compression codec/path histogram mutex poisoned");
		for ((codec, path), histogram) in histograms.iter() {
			render_histogram_micros_series(
				out,
				name,
				&[("codec", *codec), ("path", *path)],
				&histogram.snapshot(),
			);
		}
	}

	fn render_ratio(&self, out: &mut String, name: &str, help: &str) {
		let _ = writeln!(out, "# HELP {name} {help}");
		let _ = writeln!(out, "# TYPE {name} histogram");
		let histograms = self
			.histograms
			.lock()
			.expect("network compression codec/path histogram mutex poisoned");
		for ((codec, path), histogram) in histograms.iter() {
			render_histogram_ratio_series(
				out,
				name,
				&[("codec", *codec), ("path", *path)],
				&histogram.snapshot(),
			);
		}
	}
}

#[derive(Default)]
struct CdcSubscriptionResultCounters {
	opened: Counter,
	closed: Counter,
	rejected: Counter,
	cancelled: Counter,
}

impl CdcSubscriptionResultCounters {
	fn counter(&self, result: CdcSubscriptionMetricResult) -> &Counter {
		match result {
			CdcSubscriptionMetricResult::Opened => &self.opened,
			CdcSubscriptionMetricResult::Closed => &self.closed,
			CdcSubscriptionMetricResult::Rejected => &self.rejected,
			CdcSubscriptionMetricResult::Cancelled => &self.cancelled,
		}
	}

	fn inc(&self, result: CdcSubscriptionMetricResult) {
		self.counter(result).inc();
	}

	fn render(&self, out: &mut String, name: &str, help: &str) {
		render_counter_family_header(out, name, help);
		for result in CdcSubscriptionMetricResult::ALL {
			render_counter_with_labels(
				out,
				name,
				&[("result", result.as_str())],
				self.counter(result).get(),
			);
		}
	}
}

#[derive(Default)]
struct CdcSourceCounters {
	ring: Counter,
	catchup: Counter,
}

impl CdcSourceCounters {
	fn counter(&self, source: CdcMetricSource) -> &Counter {
		match source {
			CdcMetricSource::Ring => &self.ring,
			CdcMetricSource::Catchup => &self.catchup,
		}
	}

	fn inc(&self, source: CdcMetricSource) {
		self.counter(source).inc();
	}

	fn add(&self, source: CdcMetricSource, value: u64) {
		self.counter(source).add(value);
	}

	fn render(&self, out: &mut String, name: &str, help: &str) {
		render_counter_family_header(out, name, help);
		for source in CdcMetricSource::ALL {
			render_counter_with_labels(
				out,
				name,
				&[("source", source.as_str())],
				self.counter(source).get(),
			);
		}
	}
}

#[derive(Default)]
struct CdcRejectCounters {
	below_retention_floor: Counter,
	ring_overflow_during_catchup: Counter,
	schema_fingerprint_incompatible: Counter,
	checkpoint_digest_mismatch: Counter,
	unknown_table: Counter,
	tenant_auth_denied: Counter,
	table_not_replicated_here: Counter,
	internal_engine_error: Counter,
}

impl CdcRejectCounters {
	fn counter(&self, code: CdcRejectCode) -> &Counter {
		match code {
			CdcRejectCode::BelowRetentionFloor => &self.below_retention_floor,
			CdcRejectCode::RingOverflowDuringCatchup => &self.ring_overflow_during_catchup,
			CdcRejectCode::SchemaFingerprintIncompatible => &self.schema_fingerprint_incompatible,
			CdcRejectCode::CheckpointDigestMismatch => &self.checkpoint_digest_mismatch,
			CdcRejectCode::UnknownTable => &self.unknown_table,
			CdcRejectCode::TenantAuthDenied => &self.tenant_auth_denied,
			CdcRejectCode::TableNotReplicatedHere => &self.table_not_replicated_here,
			CdcRejectCode::InternalEngineError => &self.internal_engine_error,
		}
	}

	fn inc(&self, code: CdcRejectCode) {
		self.counter(code).inc();
	}

	fn render(&self, out: &mut String, name: &str, help: &str) {
		render_counter_family_header(out, name, help);
		for code in CdcRejectCode::ALL {
			render_counter_with_labels(
				out,
				name,
				&[("code", code.as_str())],
				self.counter(code).get(),
			);
		}
	}
}

struct CdcSourceHistograms {
	ring: Mutex<Histogram>,
	catchup: Mutex<Histogram>,
}

impl CdcSourceHistograms {
	fn new(bounds: &'static [u64]) -> Self {
		Self {
			ring: Mutex::new(Histogram::new(bounds)),
			catchup: Mutex::new(Histogram::new(bounds)),
		}
	}

	fn histogram(&self, source: CdcMetricSource) -> &Mutex<Histogram> {
		match source {
			CdcMetricSource::Ring => &self.ring,
			CdcMetricSource::Catchup => &self.catchup,
		}
	}

	fn observe_duration(&self, source: CdcMetricSource, duration: Duration) {
		self.histogram(source)
			.lock()
			.expect("cdc source histogram mutex poisoned")
			.observe_duration(duration);
	}

	fn render_micros(&self, out: &mut String, name: &str, help: &str) {
		let _ = writeln!(out, "# HELP {name} {help}");
		let _ = writeln!(out, "# TYPE {name} histogram");
		for source in CdcMetricSource::ALL {
			let guard = self
				.histogram(source)
				.lock()
				.expect("cdc source histogram mutex poisoned");
			let snapshot = guard.snapshot();
			render_histogram_micros_series(out, name, &[("source", source.as_str())], &snapshot);
		}
	}
}

#[derive(Default)]
struct LsmCompactionCounters(Mutex<BTreeMap<(u8, &'static str), u64>>);

impl LsmCompactionCounters {
	fn inc(&self, compaction_level: CompactionLevelIndex, result: LsmCompactionResult) {
		if !compaction_level.is_compaction_source() {
			return;
		}
		let mut values = self
			.0
			.lock()
			.expect("lsm compaction counter mutex poisoned");
		*values
			.entry((compaction_level.get(), result.as_str()))
			.or_default() += 1;
	}

	fn render(&self, out: &mut String, name: &str, help: &str) {
		render_counter_family_header(out, name, help);
		let values = self
			.0
			.lock()
			.expect("lsm compaction counter mutex poisoned");
		for level in CompactionLevelIndex::COMPACTION_SOURCES {
			for result in LsmCompactionResult::ALL {
				render_counter_with_labels(
					out,
					name,
					&[("level", level_label(level)), ("result", result.as_str())],
					values
						.get(&(level.get(), result.as_str()))
						.copied()
						.unwrap_or(0),
				);
			}
		}
	}
}

#[derive(Default)]
struct LsmSourceLevelGauges(Mutex<BTreeMap<u8, u64>>);

impl LsmSourceLevelGauges {
	fn set(&self, compaction_level: CompactionLevelIndex, value: u64) {
		if !compaction_level.is_compaction_source() {
			return;
		}
		self.0
			.lock()
			.expect("lsm source-level gauge mutex poisoned")
			.insert(compaction_level.get(), value);
	}

	fn render(&self, out: &mut String, name: &str, help: &str) {
		render_gauge_family_header(out, name, help);
		let values = self
			.0
			.lock()
			.expect("lsm source-level gauge mutex poisoned");
		for level in CompactionLevelIndex::COMPACTION_SOURCES {
			render_gauge_with_labels(
				out,
				name,
				&[("level", level_label(level))],
				values.get(&level.get()).copied().unwrap_or(0),
			);
		}
	}
}

#[derive(Default)]
struct LsmAllLevelGauges(Mutex<BTreeMap<u8, u64>>);

impl LsmAllLevelGauges {
	fn set(&self, compaction_level: CompactionLevelIndex, value: u64) {
		self.0
			.lock()
			.expect("lsm level gauge mutex poisoned")
			.insert(compaction_level.get(), value);
	}

	fn render(&self, out: &mut String, name: &str, help: &str) {
		render_gauge_family_header(out, name, help);
		let values = self.0.lock().expect("lsm level gauge mutex poisoned");
		for level in CompactionLevelIndex::ALL {
			render_gauge_with_labels(
				out,
				name,
				&[("level", level_label(level))],
				values.get(&level.get()).copied().unwrap_or(0),
			);
		}
	}
}

#[derive(Default)]
struct LsmDigestInvariantCounters(Mutex<BTreeMap<&'static str, u64>>);

impl LsmDigestInvariantCounters {
	fn inc(&self, kind: LsmDigestInvariantKind) {
		let mut values = self
			.0
			.lock()
			.expect("lsm digest invariant counter mutex poisoned");
		*values.entry(kind.as_str()).or_default() += 1;
	}

	fn render(&self, out: &mut String, name: &str, help: &str) {
		render_counter_family_header(out, name, help);
		let values = self
			.0
			.lock()
			.expect("lsm digest invariant counter mutex poisoned");
		for kind in LsmDigestInvariantKind::ALL {
			render_counter_with_labels(
				out,
				name,
				&[("kind", kind.as_str())],
				values.get(kind.as_str()).copied().unwrap_or(0),
			);
		}
	}
}

fn level_label(level: CompactionLevelIndex) -> &'static str {
	match level {
		CompactionLevelIndex::L0 => "0",
		CompactionLevelIndex::L1 => "1",
		CompactionLevelIndex::L2 => "2",
		CompactionLevelIndex::L3 => "3",
		_ => unreachable!("CompactionLevelIndex rejects values outside 0..=3"),
	}
}

#[derive(Default)]
struct L2ColumnFetchFallbackCounters {
	disabled: Counter,
	single_row: Counter,
	full_projection: Counter,
	small_record: Counter,
	too_many_bytes: Counter,
	too_many_ranges: Counter,
	metadata_probe_cap: Counter,
	unsupported_arrow_layout: Counter,
	rehydration_available: Counter,
}

impl L2ColumnFetchFallbackCounters {
	fn counter(&self, reason: &str) -> Option<&Counter> {
		match reason {
			"disabled" => Some(&self.disabled),
			"single_row" => Some(&self.single_row),
			"full_projection" => Some(&self.full_projection),
			"small_record" => Some(&self.small_record),
			"too_many_bytes" => Some(&self.too_many_bytes),
			"too_many_ranges" => Some(&self.too_many_ranges),
			"metadata_probe_cap" => Some(&self.metadata_probe_cap),
			"unsupported_arrow_layout" => Some(&self.unsupported_arrow_layout),
			"rehydration_available" => Some(&self.rehydration_available),
			_ => None,
		}
	}

	fn inc(&self, reason: &str) {
		if let Some(counter) = self.counter(reason) {
			counter.inc();
		}
	}

	fn render(&self, out: &mut String, name: &str, help: &str) {
		render_counter_family_header(out, name, help);
		for reason in [
			"disabled",
			"single_row",
			"full_projection",
			"small_record",
			"too_many_bytes",
			"too_many_ranges",
			"metadata_probe_cap",
			"unsupported_arrow_layout",
			"rehydration_available",
		] {
			let value = self
				.counter(reason)
				.expect("static L2 column fetch fallback reason")
				.get();
			render_counter_with_labels(out, name, &[("reason", reason)], value);
		}
	}
}

#[derive(Default)]
struct StaticLabelCounters(Mutex<BTreeMap<&'static str, u64>>);

impl StaticLabelCounters {
	fn add(&self, label: &'static str, value: u64) {
		let mut values = self.0.lock().expect("static label counter mutex poisoned");
		*values.entry(label).or_default() += value;
	}

	fn inc(&self, label: &'static str) {
		self.add(label, 1);
	}

	fn render(
		&self,
		out: &mut String,
		name: &str,
		help: &str,
		label_name: &str,
		labels: &[&'static str],
	) {
		render_counter_family_header(out, name, help);
		let values = self.0.lock().expect("static label counter mutex poisoned");
		for label in labels {
			render_counter_with_labels(
				out,
				name,
				&[(label_name, label)],
				values.get(label).copied().unwrap_or(0),
			);
		}
	}
}

const TABLE_QUERY_CANCELLATION_OUTCOMES: &[&str] = &[
	"accepted",
	"terminal",
	"cancelled_response",
	"stream_dropped",
];

const ROW_GROUP_FETCH_STRATEGIES: &[&str] = &["row_group", "columnar_buffer", "whole_record"];
const RUNTIME_FILTER_EVENTS: &[&str] = &["present", "applied"];
const RUNTIME_FILTER_DROP_REASONS: &[&str] = &[
	"decode_failed",
	"fingerprint_mismatch",
	"unsupported_shape",
	"materialization_incompatible",
];
const RUNTIME_FILTER_ROW_KINDS: &[&str] = &["input", "surviving"];
const ROW_GROUP_FETCH_FALLBACK_REASONS: &[&str] = &[
	"disabled",
	"single_row",
	"full_projection",
	"small_record",
	"too_few_rows",
	"too_many_row_groups",
	"too_many_bytes",
	"too_many_ranges",
	"metadata_probe_cap",
	"unsupported_arrow_layout",
	"rehydration_available",
	"unpaged_record",
];
const ROW_GROUP_FETCH_DECISIONS: &[&str] = &["skip", "keep", "predicate_fetch", "no_survivors"];
const ROW_GROUP_FETCH_BYTE_KINDS: &[&str] = &["directory", "phase_b", "phase_c", "avoided"];
const ROW_GROUP_INVARIANT_KINDS: &[&str] = &[
	"page_directory_parse_error",
	"row_group_range_overflow",
	"layout_unsupported",
];
const ENCODING_CASCADE_LABELS: &[&str] = &[
	"plain",
	"lz4_frame",
	"bit_packed",
	"rle",
	"dictionary",
	"delta",
	"fsst_whole_buffer",
	"alp",
	"fsst_row_safe",
];
const ENCODING_FALLBACK_REASONS: &[&str] = &[
	"reader_disabled",
	"directory_missing",
	"unsupported_arrow_layout",
	"too_many_pages",
	"too_many_bytes",
	"dense_selection",
	"plain_buffer",
];
const ENCODING_BYTE_KINDS: &[&str] = &["page", "buffer", "avoided"];
const ENCODING_INVARIANT_KINDS: &[&str] = &[
	"encoding_directory_parse",
	"buffer_encoding_mismatch",
	"page_range_overflow",
	"cascade_unsupported",
	"decoded_length_mismatch",
];
const ENCODING_COMPUTE_PLAN_DECISIONS: &[&str] = &["exact", "prefilter", "dense_fallback"];
const ENCODING_COMPUTE_FALLBACK_REASONS: &[&str] = &[
	"none",
	"reader_disabled",
	"directory_missing",
	"unsupported_program",
	"unsupported_cascade",
	"unsupported_arrow_layout",
	"dense_selection",
	"too_many_mask_bytes",
	"too_many_pages",
	"aggregate_unsupported",
];
const ENCODING_COMPUTE_KERNEL_LABELS: &[&str] = &[
	"bit_packed",
	"rle_plain",
	"rle_bit_packed",
	"dictionary_plain",
	"dictionary_bit_packed",
	"delta",
	"fsst_whole_buffer",
	"fsst_row_safe",
];
const ENCODING_COMPUTE_CASCADE_LABELS: &[&str] = &[
	"bit_packed",
	"rle",
	"dictionary",
	"delta",
	"fsst_whole_buffer",
	"fsst_row_safe",
];
const ENCODING_COMPUTE_ROW_KINDS: &[&str] = &["scanned", "selected", "avoided", "materialized"];
const ENCODING_COMPUTE_AGGREGATES: &[&str] = &[
	"count_star",
	"count_non_null",
	"min",
	"max",
	"signed_sum",
	"unsigned_sum",
	"signed_avg",
	"unsigned_avg",
];
const ENCODING_COMPUTE_INVARIANT_KINDS: &[&str] = &[
	"mask_length_mismatch",
	"encoded_value_overflow",
	"kernel_decoded_length_mismatch",
	"aggregate_overflow_mismatch",
	"selection_range_overflow",
];

fn bounded_row_group_label(value: &str, labels: &[&'static str]) -> Option<&'static str> {
	labels.iter().copied().find(|label| *label == value)
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

struct EncodingCascadeHistograms {
	histograms: [Histogram; 9],
}

impl EncodingCascadeHistograms {
	fn new(bounds: &'static [u64]) -> Self {
		Self {
			histograms: std::array::from_fn(|_| Histogram::new(bounds)),
		}
	}

	fn histogram(&self, cascade: &str) -> Option<&Histogram> {
		ENCODING_CASCADE_LABELS
			.iter()
			.position(|label| *label == cascade)
			.map(|index| &self.histograms[index])
	}

	fn observe_duration(&self, cascade: &str, duration: Duration) {
		if let Some(histogram) = self.histogram(cascade) {
			histogram.observe_duration(duration);
		}
	}

	fn observe_value(&self, cascade: &str, value: u64) {
		if let Some(histogram) = self.histogram(cascade) {
			histogram.observe_value(value);
		}
	}

	fn render_micros(&self, out: &mut String, name: &str, help: &str) {
		let _ = writeln!(out, "# HELP {name} {help}");
		let _ = writeln!(out, "# TYPE {name} histogram");
		for (index, cascade) in ENCODING_CASCADE_LABELS.iter().enumerate() {
			render_histogram_micros_series(
				out,
				name,
				&[("cascade", *cascade)],
				&self.histograms[index].snapshot(),
			);
		}
	}

	fn render_ratio(&self, out: &mut String, name: &str, help: &str) {
		let _ = writeln!(out, "# HELP {name} {help}");
		let _ = writeln!(out, "# TYPE {name} histogram");
		for (index, cascade) in ENCODING_CASCADE_LABELS.iter().enumerate() {
			render_histogram_ratio_series(
				out,
				name,
				&[("cascade", *cascade)],
				&self.histograms[index].snapshot(),
			);
		}
	}
}

#[derive(Default)]
struct EncodingComputePlanCounters(Mutex<BTreeMap<(&'static str, &'static str), u64>>);

impl EncodingComputePlanCounters {
	fn inc(&self, decision: &'static str, reason: &'static str) {
		let mut values = self
			.0
			.lock()
			.expect("Encoding compute plan counter mutex poisoned");
		*values.entry((decision, reason)).or_default() += 1;
	}

	fn render(&self, out: &mut String, name: &str, help: &str) {
		render_counter_family_header(out, name, help);
		let values = self
			.0
			.lock()
			.expect("Encoding compute plan counter mutex poisoned");
		for decision in ENCODING_COMPUTE_PLAN_DECISIONS {
			for reason in ENCODING_COMPUTE_FALLBACK_REASONS {
				render_counter_with_labels(
					out,
					name,
					&[("decision", decision), ("reason", reason)],
					values.get(&(*decision, *reason)).copied().unwrap_or(0),
				);
			}
		}
	}
}

struct EncodingComputeKernelHistograms {
	histograms: Vec<Histogram>,
}

impl EncodingComputeKernelHistograms {
	fn new(bounds: &'static [u64]) -> Self {
		let len = ENCODING_COMPUTE_KERNEL_LABELS.len() * ENCODING_COMPUTE_CASCADE_LABELS.len();
		Self {
			histograms: (0..len).map(|_| Histogram::new(bounds)).collect(),
		}
	}

	fn index(kernel: &str, cascade: &str) -> Option<usize> {
		let kernel = ENCODING_COMPUTE_KERNEL_LABELS
			.iter()
			.position(|label| *label == kernel)?;
		let cascade = ENCODING_COMPUTE_CASCADE_LABELS
			.iter()
			.position(|label| *label == cascade)?;
		Some(kernel * ENCODING_COMPUTE_CASCADE_LABELS.len() + cascade)
	}

	fn observe_duration(&self, kernel: &str, cascade: &str, duration: Duration) {
		if let Some(index) = Self::index(kernel, cascade) {
			self.histograms[index].observe_duration(duration);
		}
	}

	fn render_micros(&self, out: &mut String, name: &str, help: &str) {
		let _ = writeln!(out, "# HELP {name} {help}");
		let _ = writeln!(out, "# TYPE {name} histogram");
		for (kernel_index, kernel) in ENCODING_COMPUTE_KERNEL_LABELS.iter().enumerate() {
			for (cascade_index, cascade) in ENCODING_COMPUTE_CASCADE_LABELS.iter().enumerate() {
				let index = kernel_index * ENCODING_COMPUTE_CASCADE_LABELS.len() + cascade_index;
				render_histogram_micros_series(
					out,
					name,
					&[("kernel", *kernel), ("cascade", *cascade)],
					&self.histograms[index].snapshot(),
				);
			}
		}
	}
}

#[derive(Default)]
struct EncodingComputeAggregateCounters(
	Mutex<BTreeMap<(&'static str, &'static str, &'static str), u64>>,
);

impl EncodingComputeAggregateCounters {
	fn inc(&self, aggregate: &'static str, decision: &'static str, reason: &'static str) {
		let mut values = self
			.0
			.lock()
			.expect("Encoding compute aggregate counter mutex poisoned");
		*values.entry((aggregate, decision, reason)).or_default() += 1;
	}

	fn render(&self, out: &mut String, name: &str, help: &str) {
		render_counter_family_header(out, name, help);
		let values = self
			.0
			.lock()
			.expect("Encoding compute aggregate counter mutex poisoned");
		for aggregate in ENCODING_COMPUTE_AGGREGATES {
			for decision in ENCODING_COMPUTE_PLAN_DECISIONS {
				for reason in ENCODING_COMPUTE_FALLBACK_REASONS {
					render_counter_with_labels(
						out,
						name,
						&[
							("aggregate", aggregate),
							("decision", decision),
							("reason", reason),
						],
						values
							.get(&(*aggregate, *decision, *reason))
							.copied()
							.unwrap_or(0),
					);
				}
			}
		}
	}
}

pub struct MetricsRegistry {
	pub metrics_scrapes_total: Counter,

	pub raft_membership_epoch: Gauge,
	pub raft_entries_applied_total: Counter,
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
	pub control_descriptor_get_requests_total: Counter,
	pub control_descriptor_get_success_total: Counter,
	pub control_descriptor_get_not_found_total: Counter,
	pub control_descriptor_get_not_leader_total: Counter,
	pub control_descriptor_get_authorization_failed_total: Counter,
	pub control_descriptor_get_invalid_request_total: Counter,
	pub control_optimizer_facts_get_requests_total: Counter,
	pub control_optimizer_facts_get_success_total: Counter,
	pub control_optimizer_facts_get_not_found_total: Counter,
	pub control_optimizer_facts_get_not_leader_total: Counter,
	pub control_optimizer_facts_get_authorization_failed_total: Counter,
	pub control_optimizer_facts_get_invalid_request_total: Counter,
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
	table_query_cancellations: StaticLabelCounters,
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
	pub agg_request_total: Counter,
	pub agg_request_rejected_total: Counter,
	pub agg_rows_scanned_total: Counter,
	pub agg_rows_admitted_total: Counter,
	pub agg_rows_filtered_out_total: Counter,
	pub agg_partials_merged_total: Counter,
	pub agg_csb_short_circuit_total: Counter,
	pub agg_csb_short_circuit_skipped_tombstone_metadata_untracked_total: Counter,
	pub agg_csb_short_circuit_skipped_supersession_metadata_untracked_total: Counter,
	storage_csb_merge_logical_fingerprint_mismatch: DescriptorFingerprintTableCounters,
	storage_csb_payload_coverage_fall_through: DescriptorFingerprintReasonCounters,
	storage_csb_merge_pre_adr_051_cliff: DescriptorFingerprintTableCounters,
	storage_aggregate_cache_logical_fingerprint_hits: DescriptorFingerprintTableCounters,
	storage_aggregate_cache_logical_fingerprint_misses: DescriptorFingerprintTableCounters,
	storage_descriptor_recompute_seconds: DescriptorFingerprintTableHistograms,
	storage_descriptor_validate_seconds: DescriptorFingerprintTableHistograms,
	storage_descriptor_logical_only_mutations: DescriptorFingerprintTableCounters,
	storage_descriptor_physical_only_mutations: DescriptorFingerprintTableCounters,
	storage_descriptor_combined_mutations: DescriptorFingerprintTableCounters,
	pub agg_int_overflow_total: Counter,
	pub agg_skipped_no_capability_total: Counter,
	agg_partial_bytes: Histogram,
	agg_exec_micros: Histogram,
	agg_merge_micros: Histogram,

	pub group_commit_window_seconds: Histogram,
	pub data_ring_durable_quorum_latency_seconds: Histogram,
	pub data_ring_pending_finalization_replay_latency_seconds: Histogram,
	pub data_ring_migration_proxy_read_latency_seconds: Histogram,
	pub l1_read_latency_seconds: Histogram,
	pub l2_upload_latency_seconds: Histogram,
	pub l2_range_read_latency_seconds: Histogram,
	storage_l2_column_fetch_plan_micros: Histogram,
	storage_l2_column_fetch_selected_chunk_runs: Histogram,
	storage_l2_column_fetch_fallback: L2ColumnFetchFallbackCounters,
	storage_l2_row_group_fetch_requests: StaticLabelCounters,
	storage_l2_row_group_fetch_fallback: StaticLabelCounters,
	storage_l2_row_group_fetch_row_groups: StaticLabelCounters,
	storage_l2_row_group_fetch_bytes: StaticLabelCounters,
	storage_l2_row_group_fetch_plan_micros: Histogram,
	storage_l2_row_group_fetch_selected_ranges: Histogram,
	storage_l2_row_group_fetch_directory_bytes: Histogram,
	storage_l2_row_group_fetch_invariant_violations: StaticLabelCounters,
	storage_l2_encoding_buffer_encodings: StaticLabelCounters,
	storage_l2_encoding_materialize_micros: EncodingCascadeHistograms,
	storage_l2_encoding_simd_pages_fetched: StaticLabelCounters,
	storage_l2_encoding_compression_ratio: EncodingCascadeHistograms,
	storage_l2_encoding_cascade_select_micros: Histogram,
	storage_l2_encoding_directory_bytes: Histogram,
	storage_l2_encoding_fallback: StaticLabelCounters,
	storage_l2_encoding_bytes: StaticLabelCounters,
	storage_l2_encoding_invariant_violations: StaticLabelCounters,
	storage_l2_encoding_compute_plan: EncodingComputePlanCounters,
	storage_l2_encoding_compute_kernel_micros: EncodingComputeKernelHistograms,
	storage_l2_encoding_compute_rows: StaticLabelCounters,
	storage_l2_encoding_compute_mask_bytes: Histogram,
	storage_l2_encoding_compute_pages_fetched: StaticLabelCounters,
	storage_l2_encoding_compute_aggregate: EncodingComputeAggregateCounters,
	storage_l2_encoding_compute_invariant_violations: StaticLabelCounters,
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
	pub storage_multirow_records_written_total: Counter,
	pub storage_multirow_rows_written_total: Counter,
	pub storage_multirow_rows_materialized_total: Counter,
	pub storage_multirow_point_read_amplification_bytes: Counter,
	pub storage_multirow_recompact_total: Counter,
	pub storage_multirow_uncompacted_row_records_total: Counter,
	storage_multirow_rows_per_record: Histogram,
	storage_multirow_decode_micros: Histogram,
	// ADR 045 — append-only table metrics.
	pub storage_append_only_batches_total: Counter,
	pub storage_append_only_rows_written_total: Counter,
	pub storage_append_only_intra_batch_index_exhaustion_total: Counter,
	pub storage_append_only_supersession_metadata_skipped_total: Counter,
	storage_content_hash_admissions: ContentHashAdmissionCounters,
	storage_content_hash_duplicate_absorptions: ContentHashTableCounters,
	storage_pk_delete_outcomes: PkDeleteOutcomeCounters,
	storage_pk_delete_requests: PkTableCounters,
	storage_pk_delete_rows_deleted: PkTableCounters,
	storage_pk_delete_rows_already_deleted: PkTableCounters,
	storage_pk_delete_rows_not_found: PkTableCounters,
	storage_pk_delete_resurrections: PkTableCounters,
	storage_pk_delete_tombstone_boundary_versions_retained: PkTableCounters,
	storage_pk_upsert_no_digest_add: PkTableCounters,
	storage_pk_admission_visibility_lookup_seconds: PkAdmissionVisibilityHistograms,
	storage_table_retention_policy_updates: TableRetentionPolicyCounters,
	pub storage_table_retention_read_floor_rejects_total: Counter,
	pub storage_table_retention_read_filtered_rows_total: Counter,
	storage_table_retention_l1_rows_pruned: TableRetentionRowKindCounters,
	pub storage_table_retention_l1_bytes_reclaimed_total: Counter,
	storage_table_retention_l2_rows_pruned: TableRetentionRowKindCounters,
	pub storage_table_retention_l2_bytes_reclaimed_total: Counter,
	pub storage_table_retention_pair_drops_total: Counter,
	pub storage_table_retention_ambiguous_rows_retained_total: Counter,
	pub storage_table_retention_lag_seconds: Gauge,
	storage_table_retention_policy_active_tables: TableRetentionActiveTableGauges,
	storage_runtime_filter_events: StaticLabelCounters,
	storage_runtime_filter_advisory_dropped: StaticLabelCounters,
	storage_runtime_filter_rows: StaticLabelCounters,
	lsm_compactions: LsmCompactionCounters,
	lsm_compaction_input_slabs: LsmSourceLevelGauges,
	lsm_compaction_output_bytes: LsmSourceLevelGauges,
	lsm_compaction_duration_millis: LsmSourceLevelGauges,
	lsm_level_slab_count: LsmAllLevelGauges,
	lsm_level_total_bytes: LsmAllLevelGauges,
	pub lsm_pending_delete_slabs: Gauge,
	pub lsm_retired_slab_pinned_bytes: Gauge,
	lsm_digest_invariance_violations: LsmDigestInvariantCounters,
	storage_secondary_index_create: SecondaryIndexCounters,
	storage_secondary_index_drop: SecondaryIndexCounters,
	storage_secondary_index_lifecycle_transitions: SecondaryIndexTransitionCounters,
	storage_secondary_index_segments_built: SecondaryIndexCounters,
	storage_secondary_index_segments_failed: SecondaryIndexReasonCounters,
	storage_secondary_index_entries_indexed: SecondaryIndexCounters,
	storage_secondary_index_active_buffer_failures: SecondaryIndexReasonCounters,
	storage_secondary_index_query_plans: SecondaryIndexModeCounters,
	storage_secondary_index_candidate_rows: SecondaryIndexCounters,
	storage_secondary_index_visible: SecondaryIndexCounters,
	storage_secondary_index_superseded: SecondaryIndexCounters,
	storage_secondary_index_stale_indexed_value: SecondaryIndexCounters,
	storage_secondary_index_uncovered_slabs: SecondaryIndexGauges,
	storage_secondary_index_coverage_gap_slabs: SecondaryIndexGauges,
	storage_secondary_index_lookup_refusals: SecondaryIndexCounters,
	storage_secondary_index_backfill_seconds: SecondaryIndexDurationHistograms,
	storage_secondary_index_segment_state_digest_mismatch: SecondaryIndexCounters,
	storage_secondary_index_logical_fingerprint_mismatch: SecondaryIndexCounters,
	storage_secondary_index_corruption_failures: SecondaryIndexCounters,
	storage_secondary_index_drop_gc_seconds: SecondaryIndexDurationHistograms,
	storage_compression_records_written: CompressionCodecCounters,
	storage_compression_plaintext_bytes: CompressionCodecCounters,
	storage_compression_encoded_bytes: CompressionCodecCounters,
	storage_compression_encode_micros: CompressionCodecHistograms,
	storage_compression_decode_micros: CompressionCodecHistograms,
	storage_compression_decompressed_bytes: CompressionCodecCounters,
	storage_compression_decode_error: CompressionCodecCounters,
	storage_compression_l2_record_body_bytes_fetched: CompressionCodecCounters,
	storage_compression_ratio: CompressionCodecHistograms,
	pub storage_compression_uncompressed_resident_bytes: Gauge,
	network_compression_batches: NetworkCompressionBatchCounters,
	network_compression_encoded_bytes: NetworkCompressionCodecPathCounters,
	network_compression_encode_micros: NetworkCompressionCodecPathHistograms,
	network_compression_ratio: NetworkCompressionCodecPathHistograms,
	network_compression_decode_error: CompressionCodecCounters,
	cdc_subscriptions: CdcSubscriptionResultCounters,
	cdc_events_emitted: CdcSourceCounters,
	cdc_event_bytes_emitted: CdcSourceCounters,
	cdc_checkpoints_emitted: CdcSourceCounters,
	cdc_rejects: CdcRejectCounters,
	cdc_subscription_lag_micros: CdcSourceHistograms,
	pub cdc_ring_overflow_total: Counter,
	pub cdc_catchup_to_live_handoff_micros: Histogram,
	pub cdc_staged_entries_in_ring: Gauge,
	pub cdc_pending_finalization_evicted_total: Counter,
	pub cdc_locality_heartbeats_committed_total: Counter,

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
			raft_entries_applied_total: Counter::default(),
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
			control_descriptor_get_requests_total: Counter::default(),
			control_descriptor_get_success_total: Counter::default(),
			control_descriptor_get_not_found_total: Counter::default(),
			control_descriptor_get_not_leader_total: Counter::default(),
			control_descriptor_get_authorization_failed_total: Counter::default(),
			control_descriptor_get_invalid_request_total: Counter::default(),
			control_optimizer_facts_get_requests_total: Counter::default(),
			control_optimizer_facts_get_success_total: Counter::default(),
			control_optimizer_facts_get_not_found_total: Counter::default(),
			control_optimizer_facts_get_not_leader_total: Counter::default(),
			control_optimizer_facts_get_authorization_failed_total: Counter::default(),
			control_optimizer_facts_get_invalid_request_total: Counter::default(),
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
			table_query_cancellations: StaticLabelCounters::default(),
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
			agg_request_total: Counter::default(),
			agg_request_rejected_total: Counter::default(),
			agg_rows_scanned_total: Counter::default(),
			agg_rows_admitted_total: Counter::default(),
			agg_rows_filtered_out_total: Counter::default(),
			agg_partials_merged_total: Counter::default(),
			agg_csb_short_circuit_total: Counter::default(),
			agg_csb_short_circuit_skipped_tombstone_metadata_untracked_total: Counter::default(),
			agg_csb_short_circuit_skipped_supersession_metadata_untracked_total: Counter::default(),
			storage_csb_merge_logical_fingerprint_mismatch:
				DescriptorFingerprintTableCounters::default(),
			storage_csb_payload_coverage_fall_through: DescriptorFingerprintReasonCounters::default(
			),
			storage_csb_merge_pre_adr_051_cliff: DescriptorFingerprintTableCounters::default(),
			storage_aggregate_cache_logical_fingerprint_hits:
				DescriptorFingerprintTableCounters::default(),
			storage_aggregate_cache_logical_fingerprint_misses:
				DescriptorFingerprintTableCounters::default(),
			storage_descriptor_recompute_seconds: DescriptorFingerprintTableHistograms::default(),
			storage_descriptor_validate_seconds: DescriptorFingerprintTableHistograms::default(),
			storage_descriptor_logical_only_mutations: DescriptorFingerprintTableCounters::default(
			),
			storage_descriptor_physical_only_mutations: DescriptorFingerprintTableCounters::default(
			),
			storage_descriptor_combined_mutations: DescriptorFingerprintTableCounters::default(),
			agg_int_overflow_total: Counter::default(),
			agg_skipped_no_capability_total: Counter::default(),
			agg_partial_bytes: Histogram::new(AGG_PARTIAL_BYTES_BUCKETS),
			agg_exec_micros: Histogram::new(AGG_EXEC_BUCKETS_MICROS),
			agg_merge_micros: Histogram::new(AGG_EXEC_BUCKETS_MICROS),

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
			storage_l2_column_fetch_plan_micros: Histogram::new(PBC_SLAB_STATS_BUCKETS_MICROS),
			storage_l2_column_fetch_selected_chunk_runs: Histogram::new(
				L2_COLUMN_FETCH_CHUNK_RUN_BUCKETS,
			),
			storage_l2_column_fetch_fallback: L2ColumnFetchFallbackCounters::default(),
			storage_l2_row_group_fetch_requests: StaticLabelCounters::default(),
			storage_l2_row_group_fetch_fallback: StaticLabelCounters::default(),
			storage_l2_row_group_fetch_row_groups: StaticLabelCounters::default(),
			storage_l2_row_group_fetch_bytes: StaticLabelCounters::default(),
			storage_l2_row_group_fetch_plan_micros: Histogram::new(PBC_SLAB_STATS_BUCKETS_MICROS),
			storage_l2_row_group_fetch_selected_ranges: Histogram::new(
				L2_COLUMN_FETCH_CHUNK_RUN_BUCKETS,
			),
			storage_l2_row_group_fetch_directory_bytes: Histogram::new(
				PBC_SLAB_STATS_BLOCK_BYTES_BUCKETS,
			),
			storage_l2_row_group_fetch_invariant_violations: StaticLabelCounters::default(),
			storage_l2_encoding_buffer_encodings: StaticLabelCounters::default(),
			storage_l2_encoding_materialize_micros: EncodingCascadeHistograms::new(
				PBC_SLAB_STATS_BUCKETS_MICROS,
			),
			storage_l2_encoding_simd_pages_fetched: StaticLabelCounters::default(),
			storage_l2_encoding_compression_ratio: EncodingCascadeHistograms::new(
				COMPRESSION_RATIO_MILLI_BUCKETS,
			),
			storage_l2_encoding_cascade_select_micros: Histogram::new(
				PBC_SLAB_STATS_BUCKETS_MICROS,
			),
			storage_l2_encoding_directory_bytes: Histogram::new(PBC_SLAB_STATS_BLOCK_BYTES_BUCKETS),
			storage_l2_encoding_fallback: StaticLabelCounters::default(),
			storage_l2_encoding_bytes: StaticLabelCounters::default(),
			storage_l2_encoding_invariant_violations: StaticLabelCounters::default(),
			storage_l2_encoding_compute_plan: EncodingComputePlanCounters::default(),
			storage_l2_encoding_compute_kernel_micros: EncodingComputeKernelHistograms::new(
				PBC_SLAB_STATS_BUCKETS_MICROS,
			),
			storage_l2_encoding_compute_rows: StaticLabelCounters::default(),
			storage_l2_encoding_compute_mask_bytes: Histogram::new(
				PBC_SLAB_STATS_BLOCK_BYTES_BUCKETS,
			),
			storage_l2_encoding_compute_pages_fetched: StaticLabelCounters::default(),
			storage_l2_encoding_compute_aggregate: EncodingComputeAggregateCounters::default(),
			storage_l2_encoding_compute_invariant_violations: StaticLabelCounters::default(),
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
			storage_multirow_records_written_total: Counter::default(),
			storage_multirow_rows_written_total: Counter::default(),
			storage_multirow_rows_materialized_total: Counter::default(),
			storage_multirow_point_read_amplification_bytes: Counter::default(),
			storage_multirow_recompact_total: Counter::default(),
			storage_multirow_uncompacted_row_records_total: Counter::default(),
			storage_multirow_rows_per_record: Histogram::new(MULTIROW_ROWS_PER_RECORD_BUCKETS),
			storage_multirow_decode_micros: Histogram::new(PBC_EXEC_BUCKETS_MICROS),
			storage_append_only_batches_total: Counter::default(),
			storage_append_only_rows_written_total: Counter::default(),
			storage_append_only_intra_batch_index_exhaustion_total: Counter::default(),
			storage_append_only_supersession_metadata_skipped_total: Counter::default(),
			storage_content_hash_admissions: ContentHashAdmissionCounters::default(),
			storage_content_hash_duplicate_absorptions: ContentHashTableCounters::default(),
			storage_pk_delete_outcomes: PkDeleteOutcomeCounters::default(),
			storage_pk_delete_requests: PkTableCounters::default(),
			storage_pk_delete_rows_deleted: PkTableCounters::default(),
			storage_pk_delete_rows_already_deleted: PkTableCounters::default(),
			storage_pk_delete_rows_not_found: PkTableCounters::default(),
			storage_pk_delete_resurrections: PkTableCounters::default(),
			storage_pk_delete_tombstone_boundary_versions_retained: PkTableCounters::default(),
			storage_pk_upsert_no_digest_add: PkTableCounters::default(),
			storage_pk_admission_visibility_lookup_seconds:
				PkAdmissionVisibilityHistograms::default(),
			storage_table_retention_policy_updates: TableRetentionPolicyCounters::default(),
			storage_table_retention_read_floor_rejects_total: Counter::default(),
			storage_table_retention_read_filtered_rows_total: Counter::default(),
			storage_table_retention_l1_rows_pruned: TableRetentionRowKindCounters::default(),
			storage_table_retention_l1_bytes_reclaimed_total: Counter::default(),
			storage_table_retention_l2_rows_pruned: TableRetentionRowKindCounters::default(),
			storage_table_retention_l2_bytes_reclaimed_total: Counter::default(),
			storage_table_retention_pair_drops_total: Counter::default(),
			storage_table_retention_ambiguous_rows_retained_total: Counter::default(),
			storage_table_retention_lag_seconds: Gauge::default(),
			storage_table_retention_policy_active_tables: TableRetentionActiveTableGauges::default(
			),
			storage_runtime_filter_events: StaticLabelCounters::default(),
			storage_runtime_filter_advisory_dropped: StaticLabelCounters::default(),
			storage_runtime_filter_rows: StaticLabelCounters::default(),
			lsm_compactions: LsmCompactionCounters::default(),
			lsm_compaction_input_slabs: LsmSourceLevelGauges::default(),
			lsm_compaction_output_bytes: LsmSourceLevelGauges::default(),
			lsm_compaction_duration_millis: LsmSourceLevelGauges::default(),
			lsm_level_slab_count: LsmAllLevelGauges::default(),
			lsm_level_total_bytes: LsmAllLevelGauges::default(),
			lsm_pending_delete_slabs: Gauge::default(),
			lsm_retired_slab_pinned_bytes: Gauge::default(),
			lsm_digest_invariance_violations: LsmDigestInvariantCounters::default(),
			storage_secondary_index_create: SecondaryIndexCounters::default(),
			storage_secondary_index_drop: SecondaryIndexCounters::default(),
			storage_secondary_index_lifecycle_transitions:
				SecondaryIndexTransitionCounters::default(),
			storage_secondary_index_segments_built: SecondaryIndexCounters::default(),
			storage_secondary_index_segments_failed: SecondaryIndexReasonCounters::default(),
			storage_secondary_index_entries_indexed: SecondaryIndexCounters::default(),
			storage_secondary_index_active_buffer_failures: SecondaryIndexReasonCounters::default(),
			storage_secondary_index_query_plans: SecondaryIndexModeCounters::default(),
			storage_secondary_index_candidate_rows: SecondaryIndexCounters::default(),
			storage_secondary_index_visible: SecondaryIndexCounters::default(),
			storage_secondary_index_superseded: SecondaryIndexCounters::default(),
			storage_secondary_index_stale_indexed_value: SecondaryIndexCounters::default(),
			storage_secondary_index_uncovered_slabs: SecondaryIndexGauges::default(),
			storage_secondary_index_coverage_gap_slabs: SecondaryIndexGauges::default(),
			storage_secondary_index_lookup_refusals: SecondaryIndexCounters::default(),
			storage_secondary_index_backfill_seconds: SecondaryIndexDurationHistograms::default(),
			storage_secondary_index_segment_state_digest_mismatch: SecondaryIndexCounters::default(
			),
			storage_secondary_index_logical_fingerprint_mismatch: SecondaryIndexCounters::default(),
			storage_secondary_index_corruption_failures: SecondaryIndexCounters::default(),
			storage_secondary_index_drop_gc_seconds: SecondaryIndexDurationHistograms::default(),
			storage_compression_records_written: CompressionCodecCounters::default(),
			storage_compression_plaintext_bytes: CompressionCodecCounters::default(),
			storage_compression_encoded_bytes: CompressionCodecCounters::default(),
			storage_compression_encode_micros: CompressionCodecHistograms::new(
				PBC_EXEC_BUCKETS_MICROS,
			),
			storage_compression_decode_micros: CompressionCodecHistograms::new(
				PBC_EXEC_BUCKETS_MICROS,
			),
			storage_compression_decompressed_bytes: CompressionCodecCounters::default(),
			storage_compression_decode_error: CompressionCodecCounters::default(),
			storage_compression_l2_record_body_bytes_fetched: CompressionCodecCounters::default(),
			storage_compression_ratio: CompressionCodecHistograms::new(
				COMPRESSION_RATIO_MILLI_BUCKETS,
			),
			storage_compression_uncompressed_resident_bytes: Gauge::default(),
			network_compression_batches: NetworkCompressionBatchCounters::default(),
			network_compression_encoded_bytes: NetworkCompressionCodecPathCounters::default(),
			network_compression_encode_micros: NetworkCompressionCodecPathHistograms::new(
				PBC_EXEC_BUCKETS_MICROS,
			),
			network_compression_ratio: NetworkCompressionCodecPathHistograms::new(
				COMPRESSION_RATIO_MILLI_BUCKETS,
			),
			network_compression_decode_error: CompressionCodecCounters::default(),
			cdc_subscriptions: CdcSubscriptionResultCounters::default(),
			cdc_events_emitted: CdcSourceCounters::default(),
			cdc_event_bytes_emitted: CdcSourceCounters::default(),
			cdc_checkpoints_emitted: CdcSourceCounters::default(),
			cdc_rejects: CdcRejectCounters::default(),
			cdc_subscription_lag_micros: CdcSourceHistograms::new(
				LOCAL_READ_LATENCY_BUCKETS_MICROS,
			),
			cdc_ring_overflow_total: Counter::default(),
			cdc_catchup_to_live_handoff_micros: Histogram::new(LOCAL_READ_LATENCY_BUCKETS_MICROS),
			cdc_staged_entries_in_ring: Gauge::default(),
			cdc_pending_finalization_evicted_total: Counter::default(),
			cdc_locality_heartbeats_committed_total: Counter::default(),

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

	pub fn observe_table_query_cancellation(&self, outcome: &'static str) {
		self.table_query_cancellations.inc(outcome);
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

	pub fn observe_agg_execution(
		&self,
		rows_scanned: u64,
		rows_admitted: u64,
		duration: Duration,
		partial_bytes: u64,
		_overflow: bool,
	) {
		self.agg_request_total.inc();
		self.agg_rows_scanned_total.add(rows_scanned);
		self.agg_rows_admitted_total.add(rows_admitted);
		self.agg_rows_filtered_out_total
			.add(rows_scanned.saturating_sub(rows_admitted));
		self.agg_exec_micros.observe_duration(duration);
		self.agg_partial_bytes.observe_value(partial_bytes);
	}

	pub fn observe_agg_rejected(&self) {
		self.agg_request_total.inc();
		self.agg_request_rejected_total.inc();
	}

	pub fn observe_agg_merge(&self, partials: u64, duration: Duration) {
		self.agg_partials_merged_total.add(partials);
		self.agg_merge_micros.observe_duration(duration);
	}

	pub fn observe_agg_int_overflow(&self) {
		self.agg_int_overflow_total.inc();
	}

	pub fn observe_agg_csb_short_circuit_skipped_untracked_tombstone_metadata(&self) {
		self.agg_csb_short_circuit_skipped_tombstone_metadata_untracked_total
			.inc();
	}

	pub fn observe_agg_csb_short_circuit_skipped_supersession_metadata_untracked(&self) {
		self.agg_csb_short_circuit_skipped_supersession_metadata_untracked_total
			.inc();
	}

	pub fn observe_storage_csb_logical_fingerprint_mismatch(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
	) {
		self.storage_csb_merge_logical_fingerprint_mismatch
			.add(tenant_id, table_object_id, 1);
	}

	pub fn observe_storage_csb_payload_coverage_fall_through(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		reason: &str,
	) {
		self.storage_csb_payload_coverage_fall_through
			.add(tenant_id, table_object_id, reason, 1);
	}

	pub fn observe_storage_csb_pre_adr_051_cliff(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
	) {
		self.storage_csb_merge_pre_adr_051_cliff
			.add(tenant_id, table_object_id, 1);
	}

	pub fn observe_storage_aggregate_cache_logical_fingerprint_hit(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
	) {
		self.storage_aggregate_cache_logical_fingerprint_hits
			.add(tenant_id, table_object_id, 1);
	}

	pub fn observe_storage_aggregate_cache_logical_fingerprint_miss(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
	) {
		self.storage_aggregate_cache_logical_fingerprint_misses
			.add(tenant_id, table_object_id, 1);
	}

	pub fn observe_storage_descriptor_recompute(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		duration: Duration,
	) {
		self.storage_descriptor_recompute_seconds.observe_duration(
			tenant_id,
			table_object_id,
			duration,
		);
	}

	pub fn observe_storage_descriptor_validate(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		duration: Duration,
	) {
		self.storage_descriptor_validate_seconds.observe_duration(
			tenant_id,
			table_object_id,
			duration,
		);
	}

	#[cfg(test)]
	pub fn storage_csb_merge_logical_fingerprint_mismatch_total_for_test(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
	) -> u64 {
		self.storage_csb_merge_logical_fingerprint_mismatch
			.get(tenant_id, table_object_id)
	}

	#[cfg(test)]
	pub fn storage_csb_pre_adr_051_cliff_total_for_test(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
	) -> u64 {
		self.storage_csb_merge_pre_adr_051_cliff
			.get(tenant_id, table_object_id)
	}

	#[cfg(test)]
	pub fn storage_aggregate_cache_logical_fingerprint_hit_total_for_test(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
	) -> u64 {
		self.storage_aggregate_cache_logical_fingerprint_hits
			.get(tenant_id, table_object_id)
	}

	#[cfg(test)]
	pub fn storage_aggregate_cache_logical_fingerprint_miss_total_for_test(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
	) -> u64 {
		self.storage_aggregate_cache_logical_fingerprint_misses
			.get(tenant_id, table_object_id)
	}

	pub fn observe_storage_descriptor_logical_only_mutation(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
	) {
		self.storage_descriptor_logical_only_mutations
			.add(tenant_id, table_object_id, 1);
	}

	pub fn observe_storage_descriptor_physical_only_mutation(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
	) {
		self.storage_descriptor_physical_only_mutations
			.add(tenant_id, table_object_id, 1);
	}

	pub fn observe_storage_descriptor_combined_mutation(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
	) {
		self.storage_descriptor_combined_mutations
			.add(tenant_id, table_object_id, 1);
	}

	pub fn observe_storage_multirow_record_written(&self, rows: u64) {
		self.storage_multirow_records_written_total.inc();
		self.storage_multirow_rows_written_total.add(rows);
		self.storage_multirow_rows_per_record.observe_value(rows);
	}

	pub fn observe_storage_multirow_decode(&self, rows: u64, bytes: u64, duration: Duration) {
		self.storage_multirow_rows_materialized_total.add(rows);
		self.storage_multirow_point_read_amplification_bytes
			.add(bytes);
		self.storage_multirow_decode_micros
			.observe_duration(duration);
	}

	pub fn observe_storage_multirow_recompact(&self, records: u64) {
		self.storage_multirow_recompact_total.add(records);
	}

	/// ADR 045 — observe an admitted append-only batch on the
	/// server's success path.
	///
	/// Called by the data-server *after* lease gate, idempotency
	/// replay, and quota all clear, and after
	/// `bifurcate_table_write` has succeeded for the request.
	/// Admission itself is side-effect-free: client-side preview
	/// admission and aborted server attempts MUST NOT increment
	/// these counters.
	pub fn observe_storage_append_only_batch(&self, rows: u64) {
		self.storage_append_only_batches_total.inc();
		self.storage_append_only_rows_written_total.add(rows);
	}

	pub fn observe_storage_append_only_intra_batch_index_exhaustion(&self) {
		self.storage_append_only_intra_batch_index_exhaustion_total
			.inc();
	}

	pub fn observe_storage_append_only_supersession_metadata_skipped(&self) {
		self.storage_append_only_supersession_metadata_skipped_total
			.inc();
	}

	pub fn observe_storage_content_hash_admission(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		outcome: &str,
		rows: u64,
	) {
		self.storage_content_hash_admissions
			.add(tenant_id, table_object_id, outcome, rows);
	}

	pub fn observe_storage_content_hash_duplicate_absorption(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		rows: u64,
	) {
		self.storage_content_hash_duplicate_absorptions
			.add(tenant_id, table_object_id, rows);
	}

	pub fn observe_storage_pk_delete_outcomes(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		deleted: u64,
		already_deleted: u64,
		not_found: u64,
	) {
		self.storage_pk_delete_outcomes
			.add(tenant_id, table_object_id, "deleted", deleted);
		self.storage_pk_delete_rows_deleted
			.add(tenant_id, table_object_id, deleted);
		self.storage_pk_delete_outcomes.add(
			tenant_id,
			table_object_id,
			"already_deleted",
			already_deleted,
		);
		self.storage_pk_delete_rows_already_deleted.add(
			tenant_id,
			table_object_id,
			already_deleted,
		);
		self.storage_pk_delete_outcomes
			.add(tenant_id, table_object_id, "not_found", not_found);
		self.storage_pk_delete_rows_not_found
			.add(tenant_id, table_object_id, not_found);
	}

	pub fn observe_storage_pk_delete_request(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
	) {
		self.storage_pk_delete_requests
			.add(tenant_id, table_object_id, 1);
	}

	pub fn observe_storage_pk_delete_resurrections(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		rows: u64,
	) {
		self.storage_pk_delete_resurrections
			.add(tenant_id, table_object_id, rows);
	}

	pub fn observe_storage_pk_delete_tombstone_boundary_versions_retained(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		rows: u64,
	) {
		self.storage_pk_delete_tombstone_boundary_versions_retained
			.add(tenant_id, table_object_id, rows);
	}

	pub fn observe_storage_pk_upsert_no_digest_add(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		rows: u64,
	) {
		self.storage_pk_upsert_no_digest_add
			.add(tenant_id, table_object_id, rows);
	}

	pub fn observe_storage_pk_admission_visibility_lookup(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		duration: Duration,
	) {
		self.storage_pk_admission_visibility_lookup_seconds
			.observe_duration(tenant_id, table_object_id, duration);
	}

	pub fn observe_storage_table_retention_policy_update(
		&self,
		policy: TableRetentionMetricPolicy,
	) {
		self.storage_table_retention_policy_updates.inc(policy);
	}

	pub fn observe_storage_table_retention_read_floor_reject(&self) {
		self.storage_table_retention_read_floor_rejects_total.inc();
	}

	pub fn observe_storage_table_retention_read_filtered_rows(&self, rows: u64) {
		self.storage_table_retention_read_filtered_rows_total
			.add(rows);
	}

	pub fn observe_storage_table_retention_l1_prune(
		&self,
		data_rows: u64,
		tombstone_rows: u64,
		bytes: u64,
		pair_drops: u64,
		ambiguous_rows_retained: u64,
		max_lag_seconds: u64,
	) {
		self.storage_table_retention_l1_rows_pruned
			.add(TableRetentionRowKind::Data, data_rows);
		self.storage_table_retention_l1_rows_pruned
			.add(TableRetentionRowKind::Tombstone, tombstone_rows);
		self.storage_table_retention_l1_bytes_reclaimed_total
			.add(bytes);
		self.storage_table_retention_pair_drops_total
			.add(pair_drops);
		self.storage_table_retention_ambiguous_rows_retained_total
			.add(ambiguous_rows_retained);
		self.storage_table_retention_lag_seconds
			.set(max_lag_seconds);
	}

	pub fn observe_storage_table_retention_l2_prune(
		&self,
		data_rows: u64,
		tombstone_rows: u64,
		bytes: u64,
		pair_drops: u64,
		ambiguous_rows_retained: u64,
		max_lag_seconds: u64,
	) {
		self.storage_table_retention_l2_rows_pruned
			.add(TableRetentionRowKind::Data, data_rows);
		self.storage_table_retention_l2_rows_pruned
			.add(TableRetentionRowKind::Tombstone, tombstone_rows);
		self.storage_table_retention_l2_bytes_reclaimed_total
			.add(bytes);
		self.storage_table_retention_pair_drops_total
			.add(pair_drops);
		self.storage_table_retention_ambiguous_rows_retained_total
			.add(ambiguous_rows_retained);
		self.storage_table_retention_lag_seconds
			.set(max_lag_seconds);
	}

	pub fn set_storage_table_retention_policy_active_tables(
		&self,
		inherit: u64,
		disabled: u64,
		fixed: u64,
	) {
		self.storage_table_retention_policy_active_tables
			.set(inherit, disabled, fixed);
	}

	pub fn observe_storage_runtime_filter_present(&self) {
		self.storage_runtime_filter_events.inc("present");
	}

	pub fn observe_storage_runtime_filter_applied(&self) {
		self.storage_runtime_filter_events.inc("applied");
	}

	pub fn observe_storage_runtime_filter_advisory_dropped(&self, reason: &'static str) {
		self.storage_runtime_filter_advisory_dropped.inc(reason);
	}

	pub fn observe_storage_runtime_filter_rows(&self, input_rows: u64, surviving_rows: u64) {
		self.storage_runtime_filter_rows.add("input", input_rows);
		self.storage_runtime_filter_rows
			.add("surviving", surviving_rows);
	}

	pub fn observe_lsm_compaction(
		&self,
		source_level: CompactionLevelIndex,
		result: LsmCompactionResult,
		input_slabs: u64,
		output_bytes: u64,
		duration: Duration,
	) {
		self.lsm_compactions.inc(source_level, result);
		self.lsm_compaction_input_slabs
			.set(source_level, input_slabs);
		self.lsm_compaction_output_bytes
			.set(source_level, output_bytes);
		self.lsm_compaction_duration_millis.set(
			source_level,
			u64::try_from(duration.as_millis()).unwrap_or(u64::MAX),
		);
	}

	pub fn set_lsm_level_summary(
		&self,
		compaction_level: CompactionLevelIndex,
		slab_count: u64,
		total_bytes: u64,
	) {
		self.lsm_level_slab_count.set(compaction_level, slab_count);
		self.lsm_level_total_bytes
			.set(compaction_level, total_bytes);
	}

	pub fn set_lsm_gc_pressure(&self, pending_delete_slabs: u64, retired_slab_pinned_bytes: u64) {
		self.lsm_pending_delete_slabs.set(pending_delete_slabs);
		self.lsm_retired_slab_pinned_bytes
			.set(retired_slab_pinned_bytes);
	}

	pub fn observe_lsm_digest_invariance_violation(&self, kind: LsmDigestInvariantKind) {
		self.lsm_digest_invariance_violations.inc(kind);
	}

	pub fn observe_storage_secondary_index_create(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		index_object_id: IndexObjectId,
	) {
		self.storage_secondary_index_create
			.add(tenant_id, table_object_id, index_object_id, 1);
		self.storage_secondary_index_lifecycle_transitions.add(
			tenant_id,
			table_object_id,
			index_object_id,
			"absent",
			"building",
		);
		// ADR 081: declaration makes both confirmation axes observable from the
		// first scrape. A counter family with no labelled entry is absence, not a
		// zero baseline, so initialize it explicitly rather than adding zero.
		self.storage_secondary_index_coverage_gap_slabs.set(
			tenant_id,
			table_object_id,
			index_object_id,
			0,
		);
		self.storage_secondary_index_uncovered_slabs.set(
			tenant_id,
			table_object_id,
			index_object_id,
			0,
		);
		self.storage_secondary_index_lookup_refusals.initialize(
			tenant_id,
			table_object_id,
			index_object_id,
		);
	}

	pub fn observe_storage_secondary_index_drop(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		index_object_id: IndexObjectId,
		from: &str,
	) {
		self.storage_secondary_index_drop
			.add(tenant_id, table_object_id, index_object_id, 1);
		self.storage_secondary_index_lifecycle_transitions.add(
			tenant_id,
			table_object_id,
			index_object_id,
			from,
			"dropping",
		);
	}

	pub fn observe_storage_secondary_index_lifecycle_transition(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		index_object_id: IndexObjectId,
		from: &str,
		to: &str,
	) {
		self.storage_secondary_index_lifecycle_transitions.add(
			tenant_id,
			table_object_id,
			index_object_id,
			from,
			to,
		);
	}

	pub fn observe_storage_secondary_index_segment_built(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		index_object_id: IndexObjectId,
		entries: u64,
	) {
		self.storage_secondary_index_segments_built.add(
			tenant_id,
			table_object_id,
			index_object_id,
			1,
		);
		self.storage_secondary_index_entries_indexed.add(
			tenant_id,
			table_object_id,
			index_object_id,
			entries,
		);
	}

	pub fn observe_storage_secondary_index_segment_failed(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		index_object_id: IndexObjectId,
		reason: &str,
	) {
		self.storage_secondary_index_segments_failed.add(
			tenant_id,
			table_object_id,
			index_object_id,
			reason,
			1,
		);
	}

	pub fn observe_storage_secondary_index_active_buffer_failure(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		index_object_id: IndexObjectId,
		reason: &str,
	) {
		self.storage_secondary_index_active_buffer_failures.add(
			tenant_id,
			table_object_id,
			index_object_id,
			reason,
			1,
		);
	}

	pub fn observe_storage_secondary_index_query_plan(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		index_object_id: IndexObjectId,
		mode: &str,
	) {
		self.storage_secondary_index_query_plans.add(
			tenant_id,
			table_object_id,
			index_object_id,
			mode,
			1,
		);
	}

	pub fn observe_storage_secondary_index_lookup(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		index_object_id: IndexObjectId,
		candidates: u64,
		visible: u64,
		superseded: u64,
		stale_indexed_value: u64,
	) {
		self.storage_secondary_index_candidate_rows.add(
			tenant_id,
			table_object_id,
			index_object_id,
			candidates,
		);
		self.storage_secondary_index_visible.add(
			tenant_id,
			table_object_id,
			index_object_id,
			visible,
		);
		self.storage_secondary_index_superseded.add(
			tenant_id,
			table_object_id,
			index_object_id,
			superseded,
		);
		self.storage_secondary_index_stale_indexed_value.add(
			tenant_id,
			table_object_id,
			index_object_id,
			stale_indexed_value,
		);
	}

	pub fn set_storage_secondary_index_uncovered_slabs(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		index_object_id: IndexObjectId,
		uncovered: u64,
	) {
		self.storage_secondary_index_uncovered_slabs.set(
			tenant_id,
			table_object_id,
			index_object_id,
			uncovered,
		);
	}

	/// The invariant axis, distinct from `uncovered_slabs`: historical source slabs
	/// lacking a durable segment, i.e. exactly what the read gate refuses on. The
	/// uncovered gauge counts this process's failure records, which empty on restart
	/// and are absent during an ordinary rotation window — watching it alone shows
	/// health while every lookup refuses.
	pub fn set_storage_secondary_index_coverage_gap_slabs(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		index_object_id: IndexObjectId,
		gaps: u64,
	) {
		self.storage_secondary_index_coverage_gap_slabs.set(
			tenant_id,
			table_object_id,
			index_object_id,
			gaps,
		);
	}

	/// Ensure every declared per-index series exists, without disturbing values.
	///
	/// Declaration initializes these once, but a process restart recovers indexes from
	/// the durable catalog without replaying declaration — so a recovered index would
	/// emit no refusal series at all. A counter family with no labelled entry is
	/// *absence*, not a zero baseline: the operator cannot tell "no refusals" from
	/// "not reporting", and a confirmation run cannot record a refusal delta it never
	/// observes. Idempotent, so the coverage driver may call it on every tick.
	pub fn initialize_storage_secondary_index_series(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		index_object_id: IndexObjectId,
	) {
		self.storage_secondary_index_lookup_refusals.initialize(
			tenant_id,
			table_object_id,
			index_object_id,
		);
	}

	pub fn observe_storage_secondary_index_lookup_refusal(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		index_object_id: IndexObjectId,
	) {
		self.storage_secondary_index_lookup_refusals.add(
			tenant_id,
			table_object_id,
			index_object_id,
			1,
		);
	}

	pub fn observe_storage_secondary_index_backfill_seconds(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		index_object_id: IndexObjectId,
		duration: Duration,
	) {
		self.storage_secondary_index_backfill_seconds.observe(
			tenant_id,
			table_object_id,
			index_object_id,
			duration,
		);
	}

	pub fn observe_storage_secondary_index_segment_state_digest_mismatch(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		index_object_id: IndexObjectId,
	) {
		self.storage_secondary_index_segment_state_digest_mismatch
			.add(tenant_id, table_object_id, index_object_id, 1);
	}

	pub fn observe_storage_secondary_index_logical_fingerprint_mismatch(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		index_object_id: IndexObjectId,
	) {
		self.storage_secondary_index_logical_fingerprint_mismatch
			.add(tenant_id, table_object_id, index_object_id, 1);
	}

	pub fn observe_storage_secondary_index_corruption_failure(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		index_object_id: IndexObjectId,
	) {
		self.storage_secondary_index_corruption_failures.add(
			tenant_id,
			table_object_id,
			index_object_id,
			1,
		);
	}

	pub fn observe_storage_secondary_index_drop_gc_seconds(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		index_object_id: IndexObjectId,
		duration: Duration,
	) {
		self.storage_secondary_index_drop_gc_seconds.observe(
			tenant_id,
			table_object_id,
			index_object_id,
			duration,
		);
	}

	pub fn observe_storage_l2_column_fetch_plan(&self, duration: Duration) {
		self.storage_l2_column_fetch_plan_micros
			.observe_duration(duration);
	}

	pub fn observe_storage_l2_column_fetch_selected_chunk_runs(&self, chunk_runs: u64) {
		self.storage_l2_column_fetch_selected_chunk_runs
			.observe_value(chunk_runs);
	}

	pub fn observe_storage_l2_column_fetch_fallback(&self, reason: &str) {
		self.storage_l2_column_fetch_fallback.inc(reason);
	}

	pub fn observe_storage_l2_row_group_fetch_request(&self, strategy: &str) {
		if let Some(label) = bounded_row_group_label(strategy, ROW_GROUP_FETCH_STRATEGIES) {
			self.storage_l2_row_group_fetch_requests.inc(label);
		}
	}

	pub fn observe_storage_l2_row_group_fetch_fallback(&self, reason: &str) {
		if let Some(label) = bounded_row_group_label(reason, ROW_GROUP_FETCH_FALLBACK_REASONS) {
			self.storage_l2_row_group_fetch_fallback.inc(label);
		}
	}

	pub fn observe_storage_l2_row_group_fetch_row_groups(&self, decision: &str, value: u64) {
		if let Some(label) = bounded_row_group_label(decision, ROW_GROUP_FETCH_DECISIONS) {
			self.storage_l2_row_group_fetch_row_groups.add(label, value);
		}
	}

	pub fn observe_storage_l2_row_group_fetch_bytes(&self, kind: &str, value: u64) {
		if let Some(label) = bounded_row_group_label(kind, ROW_GROUP_FETCH_BYTE_KINDS) {
			self.storage_l2_row_group_fetch_bytes.add(label, value);
		}
	}

	pub fn observe_storage_l2_row_group_fetch_plan(&self, duration: Duration) {
		self.storage_l2_row_group_fetch_plan_micros
			.observe_duration(duration);
	}

	pub fn observe_storage_l2_row_group_fetch_selected_ranges(&self, ranges: u64) {
		self.storage_l2_row_group_fetch_selected_ranges
			.observe_value(ranges);
	}

	pub fn observe_storage_l2_row_group_fetch_directory_bytes(&self, bytes: u64) {
		self.storage_l2_row_group_fetch_directory_bytes
			.observe_value(bytes);
	}

	pub fn observe_storage_l2_row_group_fetch_invariant_violation(&self, violation_kind: &str) {
		if let Some(label) = bounded_row_group_label(violation_kind, ROW_GROUP_INVARIANT_KINDS) {
			self.storage_l2_row_group_fetch_invariant_violations
				.inc(label);
		}
	}

	pub fn observe_storage_l2_encoding_buffer_encoding(&self, cascade: &str) {
		if let Some(label) = bounded_row_group_label(cascade, ENCODING_CASCADE_LABELS) {
			self.storage_l2_encoding_buffer_encodings.inc(label);
		}
	}

	pub fn observe_storage_l2_encoding_materialize(&self, cascade: &str, duration: Duration) {
		self.storage_l2_encoding_materialize_micros
			.observe_duration(cascade, duration);
	}

	pub fn observe_storage_l2_encoding_simd_pages_fetched(&self, cascade: &str, pages: u64) {
		if let Some(label) = bounded_row_group_label(cascade, ENCODING_CASCADE_LABELS) {
			self.storage_l2_encoding_simd_pages_fetched
				.add(label, pages);
		}
	}

	pub fn observe_storage_l2_encoding_compression_ratio(
		&self,
		cascade: &str,
		decoded: u64,
		encoded: u64,
	) {
		if decoded == 0 {
			return;
		}
		let ratio_milli = encoded.saturating_mul(1000) / decoded;
		self.storage_l2_encoding_compression_ratio
			.observe_value(cascade, ratio_milli);
	}

	pub fn observe_storage_l2_encoding_cascade_select(&self, duration: Duration) {
		self.storage_l2_encoding_cascade_select_micros
			.observe_duration(duration);
	}

	pub fn observe_storage_l2_encoding_directory_bytes(&self, bytes: u64) {
		self.storage_l2_encoding_directory_bytes
			.observe_value(bytes);
	}

	pub fn observe_storage_l2_encoding_fallback(&self, reason: &str) {
		if let Some(label) = bounded_row_group_label(reason, ENCODING_FALLBACK_REASONS) {
			self.storage_l2_encoding_fallback.inc(label);
		}
	}

	pub fn observe_storage_l2_encoding_bytes(&self, kind: &str, bytes: u64) {
		if let Some(label) = bounded_row_group_label(kind, ENCODING_BYTE_KINDS) {
			self.storage_l2_encoding_bytes.add(label, bytes);
		}
	}

	pub fn observe_storage_l2_encoding_invariant_violation(&self, violation_kind: &str) {
		if let Some(label) = bounded_row_group_label(violation_kind, ENCODING_INVARIANT_KINDS) {
			self.storage_l2_encoding_invariant_violations.inc(label);
		}
	}

	pub fn observe_storage_l2_encoding_compute_plan(&self, decision: &str, reason: &str) {
		if let (Some(decision), Some(reason)) = (
			bounded_row_group_label(decision, ENCODING_COMPUTE_PLAN_DECISIONS),
			bounded_row_group_label(reason, ENCODING_COMPUTE_FALLBACK_REASONS),
		) {
			self.storage_l2_encoding_compute_plan.inc(decision, reason);
		}
	}

	pub fn observe_storage_l2_encoding_compute_kernel(
		&self,
		kernel: &str,
		cascade: &str,
		duration: Duration,
	) {
		self.storage_l2_encoding_compute_kernel_micros
			.observe_duration(kernel, cascade, duration);
	}

	pub fn observe_storage_l2_encoding_compute_rows(&self, kind: &str, rows: u64) {
		if let Some(label) = bounded_row_group_label(kind, ENCODING_COMPUTE_ROW_KINDS) {
			self.storage_l2_encoding_compute_rows.add(label, rows);
		}
	}

	pub fn observe_storage_l2_encoding_compute_mask_bytes(&self, bytes: u64) {
		self.storage_l2_encoding_compute_mask_bytes
			.observe_value(bytes);
	}

	pub fn observe_storage_l2_encoding_compute_pages_fetched(&self, kind: &str, pages: u64) {
		if let Some(label) = bounded_row_group_label(kind, ENCODING_COMPUTE_ROW_KINDS) {
			self.storage_l2_encoding_compute_pages_fetched
				.add(label, pages);
		}
	}

	pub fn observe_storage_l2_encoding_compute_aggregate(
		&self,
		aggregate: &str,
		decision: &str,
		reason: &str,
	) {
		if let (Some(aggregate), Some(decision), Some(reason)) = (
			bounded_row_group_label(aggregate, ENCODING_COMPUTE_AGGREGATES),
			bounded_row_group_label(decision, ENCODING_COMPUTE_PLAN_DECISIONS),
			bounded_row_group_label(reason, ENCODING_COMPUTE_FALLBACK_REASONS),
		) {
			self.storage_l2_encoding_compute_aggregate
				.inc(aggregate, decision, reason);
		}
	}

	pub fn observe_storage_l2_encoding_compute_invariant_violation(&self, violation_kind: &str) {
		if let Some(label) =
			bounded_row_group_label(violation_kind, ENCODING_COMPUTE_INVARIANT_KINDS)
		{
			self.storage_l2_encoding_compute_invariant_violations
				.inc(label);
		}
	}

	pub fn observe_storage_compression_encode(
		&self,
		codec: RecordBodyCompressionPolicy,
		plaintext_bytes: u64,
		encoded_bytes: u64,
		duration: Duration,
	) {
		self.storage_compression_records_written.inc(codec);
		self.storage_compression_plaintext_bytes
			.add(codec, plaintext_bytes);
		self.storage_compression_encoded_bytes
			.add(codec, encoded_bytes);
		self.storage_compression_encode_micros
			.observe_duration(codec, duration);
		if let Some(ratio_milli) = encoded_bytes
			.saturating_mul(1000)
			.checked_div(plaintext_bytes)
		{
			self.storage_compression_ratio
				.observe_value(codec, ratio_milli);
		}
	}

	pub fn observe_storage_compression_decode(
		&self,
		codec: RecordBodyCompressionPolicy,
		_encoded_bytes: u64,
		decompressed_bytes: u64,
		duration: Duration,
	) {
		self.storage_compression_decode_micros
			.observe_duration(codec, duration);
		self.storage_compression_decompressed_bytes
			.add(codec, decompressed_bytes);
	}

	pub fn observe_storage_compression_decode_error(&self, codec: RecordBodyCompressionPolicy) {
		self.storage_compression_decode_error.inc(codec);
	}

	pub fn observe_storage_compression_l2_record_body_fetch(
		&self,
		codec: RecordBodyCompressionPolicy,
		bytes: u64,
	) {
		self.storage_compression_l2_record_body_bytes_fetched
			.add(codec, bytes);
	}

	pub fn set_storage_compression_uncompressed_resident_bytes(&self, bytes: u64) {
		self.storage_compression_uncompressed_resident_bytes
			.set(bytes);
	}

	pub fn observe_network_compression_encode(
		&self,
		codec: RecordBodyCompressionPolicy,
		path: NetworkCompressionPath,
		encoded_bytes: u64,
		duration: Duration,
		ratio_milli: Option<u64>,
	) {
		self.network_compression_batches
			.inc(codec, path, NetworkCompressionResult::Compressed);
		self.network_compression_encoded_bytes
			.add(codec, path, encoded_bytes);
		self.network_compression_encode_micros
			.observe_duration(codec, path, duration);
		if let Some(ratio_milli) = ratio_milli {
			self.network_compression_ratio
				.observe_value(codec, path, ratio_milli);
		}
	}

	pub fn observe_network_compression_encode_error(
		&self,
		codec: RecordBodyCompressionPolicy,
		path: NetworkCompressionPath,
	) {
		self.network_compression_batches
			.inc(codec, path, NetworkCompressionResult::Error);
	}

	pub fn observe_network_compression_decode_error(&self, codec: RecordBodyCompressionPolicy) {
		self.network_compression_decode_error.inc(codec);
	}

	pub fn observe_cdc_subscription(&self, result: CdcSubscriptionMetricResult) {
		self.cdc_subscriptions.inc(result);
	}

	pub fn observe_cdc_event_emitted(
		&self,
		source: CdcMetricSource,
		event_count: u64,
		encoded_bytes: u64,
	) {
		self.cdc_events_emitted.add(source, event_count);
		self.cdc_event_bytes_emitted.add(source, encoded_bytes);
	}

	pub fn observe_cdc_checkpoint_emitted(&self, source: CdcMetricSource) {
		self.cdc_checkpoints_emitted.inc(source);
	}

	pub fn observe_cdc_reject(&self, code: CdcRejectCode) {
		self.cdc_rejects.inc(code);
	}

	pub fn observe_cdc_subscription_lag(&self, source: CdcMetricSource, duration: Duration) {
		self.cdc_subscription_lag_micros
			.observe_duration(source, duration);
	}

	pub fn observe_cdc_ring_overflow(&self) {
		self.cdc_ring_overflow_total.inc();
	}

	pub fn observe_cdc_catchup_to_live_handoff(&self, duration: Duration) {
		self.cdc_catchup_to_live_handoff_micros
			.observe_duration(duration);
	}

	pub fn set_cdc_staged_entries_in_ring(&self, value: u64) {
		self.cdc_staged_entries_in_ring.set(value);
	}

	pub fn observe_cdc_pending_finalization_evicted(&self) {
		self.cdc_pending_finalization_evicted_total.inc();
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
		render_counter(
			&mut out,
			"wendelon_raft_entries_applied_total",
			"Control Ring entries applied by the live state machine process.",
			self.raft_entries_applied_total.get(),
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
			"wendelon_control_descriptor_get_requests_total",
			"Control Ring table descriptor get requests handled.",
			self.control_descriptor_get_requests_total.get(),
		);
		render_counter(
			&mut out,
			"wendelon_control_descriptor_get_success_total",
			"Control Ring table descriptor get requests that returned a descriptor.",
			self.control_descriptor_get_success_total.get(),
		);
		render_counter(
			&mut out,
			"wendelon_control_descriptor_get_not_found_total",
			"Control Ring table descriptor get requests for absent table names.",
			self.control_descriptor_get_not_found_total.get(),
		);
		render_counter(
			&mut out,
			"wendelon_control_descriptor_get_not_leader_total",
			"Control Ring table descriptor get requests rejected by non-leaders.",
			self.control_descriptor_get_not_leader_total.get(),
		);
		render_counter(
			&mut out,
			"wendelon_control_descriptor_get_authorization_failed_total",
			"Control Ring table descriptor get requests rejected by authorization.",
			self.control_descriptor_get_authorization_failed_total.get(),
		);
		render_counter(
			&mut out,
			"wendelon_control_descriptor_get_invalid_request_total",
			"Control Ring table descriptor get requests rejected as malformed.",
			self.control_descriptor_get_invalid_request_total.get(),
		);
		render_counter(
			&mut out,
			"wendelon_control_optimizer_facts_get_requests_total",
			"Control Ring table optimizer facts get requests handled.",
			self.control_optimizer_facts_get_requests_total.get(),
		);
		render_counter(
			&mut out,
			"wendelon_control_optimizer_facts_get_success_total",
			"Control Ring table optimizer facts get requests that were granted.",
			self.control_optimizer_facts_get_success_total.get(),
		);
		render_counter(
			&mut out,
			"wendelon_control_optimizer_facts_get_not_found_total",
			"Control Ring table optimizer facts get requests for absent table names.",
			self.control_optimizer_facts_get_not_found_total.get(),
		);
		render_counter(
			&mut out,
			"wendelon_control_optimizer_facts_get_not_leader_total",
			"Control Ring table optimizer facts get requests rejected by non-leaders.",
			self.control_optimizer_facts_get_not_leader_total.get(),
		);
		render_counter(
			&mut out,
			"wendelon_control_optimizer_facts_get_authorization_failed_total",
			"Control Ring table optimizer facts get requests rejected by authorization.",
			self.control_optimizer_facts_get_authorization_failed_total
				.get(),
		);
		render_counter(
			&mut out,
			"wendelon_control_optimizer_facts_get_invalid_request_total",
			"Control Ring table optimizer facts get requests rejected as malformed.",
			self.control_optimizer_facts_get_invalid_request_total.get(),
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
		self.table_query_cancellations.render(
			&mut out,
			"wendelon_table_query_cancellations_total",
			"TABLE_QUERY_REQ cancellations observed by the Data Ring.",
			"outcome",
			TABLE_QUERY_CANCELLATION_OUTCOMES,
		);
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
		render_counter(
			&mut out,
			"wendelon_agg_request_total",
			"Aggregate pushdown requests executed or rejected.",
			self.agg_request_total.get(),
		);
		render_counter(
			&mut out,
			"wendelon_agg_request_rejected_total",
			"Aggregate pushdown requests rejected before execution.",
			self.agg_request_rejected_total.get(),
		);
		render_counter(
			&mut out,
			"wendelon_agg_rows_scanned_total",
			"Rows scanned by aggregate pushdown before predicate admission.",
			self.agg_rows_scanned_total.get(),
		);
		render_counter(
			&mut out,
			"wendelon_agg_rows_admitted_total",
			"Rows admitted into aggregate accumulators after predicate filtering.",
			self.agg_rows_admitted_total.get(),
		);
		render_counter(
			&mut out,
			"wendelon_agg_rows_filtered_out_total",
			"Rows removed by the aggregate request's embedded PBC predicate.",
			self.agg_rows_filtered_out_total.get(),
		);
		render_counter(
			&mut out,
			"wendelon_agg_partials_merged_total",
			"Aggregate partial states merged by coordinators.",
			self.agg_partials_merged_total.get(),
		);
		render_counter(
			&mut out,
			"wendelon_agg_csb_short_circuit_total",
			"Aggregate slab answers derived directly from column statistics blocks.",
			self.agg_csb_short_circuit_total.get(),
		);
		render_counter(
			&mut out,
			"wendelon_agg_csb_short_circuit_skipped_tombstone_metadata_untracked_total",
			"Aggregate CSB direct-answer attempts skipped because tombstone metadata is untracked.",
			self.agg_csb_short_circuit_skipped_tombstone_metadata_untracked_total
				.get(),
		);
		render_counter(
			&mut out,
			"wendelon_agg_csb_short_circuit_skipped_supersession_metadata_untracked_total",
			"Aggregate CSB direct-answer attempts skipped because supersession metadata is untracked.",
			self.agg_csb_short_circuit_skipped_supersession_metadata_untracked_total
				.get(),
		);
		self.storage_csb_merge_logical_fingerprint_mismatch.render(
			&mut out,
			"wendelon_storage_csb_merge_logical_fingerprint_mismatch_total",
			"CSB direct-answer or merge candidates rejected because logical schema fingerprints differed.",
		);
		self.storage_csb_payload_coverage_fall_through.render(
			&mut out,
			"wendelon_storage_csb_payload_coverage_fall_through_total",
			"CSB direct-answer candidates that fell through because parsed stats payload coverage was insufficient.",
		);
		self.storage_csb_merge_pre_adr_051_cliff.render(
			&mut out,
			"wendelon_storage_csb_merge_pre_adr_051_cliff_total",
			"CSB candidates whose table and generation matched but whose fingerprint slot did not match the ADR 051 logical fingerprint.",
		);
		self.storage_aggregate_cache_logical_fingerprint_hits
			.render(
				&mut out,
				"wendelon_storage_aggregate_cache_logical_fingerprint_hit_total",
				"Aggregate response cross-bind checks whose logical schema fingerprint matched the request.",
			);
		self.storage_aggregate_cache_logical_fingerprint_misses.render(
			&mut out,
			"wendelon_storage_aggregate_cache_logical_fingerprint_miss_total",
			"Aggregate response cross-bind checks whose logical schema fingerprint differed from the request.",
		);
		self.storage_descriptor_recompute_seconds.render(
			&mut out,
			"wendelon_storage_descriptor_recompute_seconds",
			"Descriptor logical and physical fingerprint recompute duration.",
		);
		self.storage_descriptor_validate_seconds.render(
			&mut out,
			"wendelon_storage_descriptor_validate_seconds",
			"Descriptor validation duration on the control schema-update submission path.",
		);
		self.storage_descriptor_logical_only_mutations.render(
			&mut out,
			"wendelon_storage_descriptor_logical_only_mutations_total",
			"Accepted descriptor mutations whose logical fingerprint changed while physical fingerprint did not. Expected to stay zero under ADR 051 stacked physical preimages; nonzero indicates an invariant breach or future preimage change.",
		);
		self.storage_descriptor_physical_only_mutations.render(
			&mut out,
			"wendelon_storage_descriptor_physical_only_mutations_total",
			"Accepted descriptor mutations whose physical descriptor fingerprint changed while logical fingerprint stayed stable.",
		);
		self.storage_descriptor_combined_mutations.render(
			&mut out,
			"wendelon_storage_descriptor_combined_mutations_total",
			"Accepted descriptor mutations whose logical and physical descriptor fingerprints both changed.",
		);
		render_counter(
			&mut out,
			"wendelon_agg_int_overflow_total",
			"Aggregate integer accumulator overflows observed.",
			self.agg_int_overflow_total.get(),
		);
		render_counter(
			&mut out,
			"wendelon_agg_skipped_no_capability_total",
			"Aggregate requests skipped because a selected peer lacks PartialAggregatePushdown.",
			self.agg_skipped_no_capability_total.get(),
		);
		render_histogram_raw(
			&mut out,
			"wendelon_agg_partial_bytes",
			"Aggregate partial-state bytes returned by storage execution.",
			&self.agg_partial_bytes.snapshot(),
		);
		render_histogram_micros(
			&mut out,
			"wendelon_agg_exec_micros",
			"Aggregate execution duration in microseconds.",
			&self.agg_exec_micros.snapshot(),
		);
		render_histogram_micros(
			&mut out,
			"wendelon_agg_merge_micros",
			"Aggregate partial merge duration in microseconds.",
			&self.agg_merge_micros.snapshot(),
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
		let l2_column_fetch_plan_micros = namespaced_metric("storage_l2_column_fetch_plan_micros");
		render_histogram_micros(
			&mut out,
			&l2_column_fetch_plan_micros,
			"ADR 044 L2 column fetch strategy planning duration in microseconds.",
			&self.storage_l2_column_fetch_plan_micros.snapshot(),
		);
		let l2_column_fetch_selected_chunk_runs =
			namespaced_metric("storage_l2_column_fetch_selected_chunk_runs");
		render_histogram_raw(
			&mut out,
			&l2_column_fetch_selected_chunk_runs,
			"Encrypted chunk runs selected by ADR 044 L2 column fetch plans.",
			&self.storage_l2_column_fetch_selected_chunk_runs.snapshot(),
		);
		let l2_column_fetch_fallback_total =
			namespaced_metric("storage_l2_column_fetch_fallback_total");
		self.storage_l2_column_fetch_fallback.render(
			&mut out,
			&l2_column_fetch_fallback_total,
			"ADR 044 L2 column fetch strategy fallbacks by planner reason.",
		);
		let row_group_requests = namespaced_metric("storage_l2_row_group_fetch_requests_total");
		self.storage_l2_row_group_fetch_requests.render(
			&mut out,
			&row_group_requests,
			"ADR 056 L2 row-group fetch requests by selected strategy.",
			"strategy",
			ROW_GROUP_FETCH_STRATEGIES,
		);
		let row_group_fallback = namespaced_metric("storage_l2_row_group_fetch_fallback_total");
		self.storage_l2_row_group_fetch_fallback.render(
			&mut out,
			&row_group_fallback,
			"ADR 056 L2 row-group fetch fallbacks by planner reason.",
			"reason",
			ROW_GROUP_FETCH_FALLBACK_REASONS,
		);
		let row_group_decisions = namespaced_metric("storage_l2_row_group_fetch_row_groups_total");
		self.storage_l2_row_group_fetch_row_groups.render(
			&mut out,
			&row_group_decisions,
			"ADR 056 row groups classified by planner decision.",
			"decision",
			ROW_GROUP_FETCH_DECISIONS,
		);
		let row_group_bytes = namespaced_metric("storage_l2_row_group_fetch_bytes_total");
		self.storage_l2_row_group_fetch_bytes.render(
			&mut out,
			&row_group_bytes,
			"ADR 056 row-group fetch bytes by bounded kind.",
			"kind",
			ROW_GROUP_FETCH_BYTE_KINDS,
		);
		let row_group_plan = namespaced_metric("storage_l2_row_group_fetch_plan_micros");
		render_histogram_micros(
			&mut out,
			&row_group_plan,
			"ADR 056 row-group fetch strategy planning duration in microseconds.",
			&self.storage_l2_row_group_fetch_plan_micros.snapshot(),
		);
		let row_group_selected_ranges =
			namespaced_metric("storage_l2_row_group_fetch_selected_ranges");
		render_histogram_raw(
			&mut out,
			&row_group_selected_ranges,
			"ADR 056 row-group fetch selected L2 range count.",
			&self.storage_l2_row_group_fetch_selected_ranges.snapshot(),
		);
		let row_group_directory_bytes =
			namespaced_metric("storage_l2_row_group_fetch_directory_bytes");
		render_histogram_raw(
			&mut out,
			&row_group_directory_bytes,
			"ADR 056 row-group page-directory metadata bytes.",
			&self.storage_l2_row_group_fetch_directory_bytes.snapshot(),
		);
		let row_group_invariants =
			namespaced_metric("storage_l2_row_group_fetch_invariant_violations_total");
		self.storage_l2_row_group_fetch_invariant_violations.render(
			&mut out,
			&row_group_invariants,
			"ADR 056 row-group invariant violations by bounded kind.",
			"violation_kind",
			ROW_GROUP_INVARIANT_KINDS,
		);
		let encoding_buffer_encodings =
			namespaced_metric("storage_l2_encoding_buffer_encodings_total");
		self.storage_l2_encoding_buffer_encodings.render(
			&mut out,
			&encoding_buffer_encodings,
			"ADR 057 Encoding buffer encodings by cascade.",
			"cascade",
			ENCODING_CASCADE_LABELS,
		);
		let encoding_materialize = namespaced_metric("storage_l2_encoding_materialize_micros");
		self.storage_l2_encoding_materialize_micros.render_micros(
			&mut out,
			&encoding_materialize,
			"ADR 057 Encoding materialization duration in microseconds.",
		);
		let encoding_pages = namespaced_metric("storage_l2_encoding_simd_pages_fetched_total");
		self.storage_l2_encoding_simd_pages_fetched.render(
			&mut out,
			&encoding_pages,
			"ADR 057 Encoding SIMD pages fetched by cascade.",
			"cascade",
			ENCODING_CASCADE_LABELS,
		);
		let encoding_ratio = namespaced_metric("storage_l2_encoding_compression_ratio");
		self.storage_l2_encoding_compression_ratio.render_ratio(
			&mut out,
			&encoding_ratio,
			"ADR 057 Encoding encoded/decoded byte ratio in thousandths.",
		);
		let encoding_select = namespaced_metric("storage_l2_encoding_cascade_select_micros");
		render_histogram_micros(
			&mut out,
			&encoding_select,
			"ADR 057 Encoding cascade selection duration in microseconds.",
			&self.storage_l2_encoding_cascade_select_micros.snapshot(),
		);
		let encoding_directory_bytes = namespaced_metric("storage_l2_encoding_directory_bytes");
		render_histogram_raw(
			&mut out,
			&encoding_directory_bytes,
			"ADR 057 Encoding directory metadata bytes.",
			&self.storage_l2_encoding_directory_bytes.snapshot(),
		);
		let encoding_fallback = namespaced_metric("storage_l2_encoding_fallback_total");
		self.storage_l2_encoding_fallback.render(
			&mut out,
			&encoding_fallback,
			"ADR 057 Encoding reader fallbacks by bounded reason.",
			"reason",
			ENCODING_FALLBACK_REASONS,
		);
		let encoding_bytes = namespaced_metric("storage_l2_encoding_bytes_total");
		self.storage_l2_encoding_bytes.render(
			&mut out,
			&encoding_bytes,
			"ADR 057 Encoding bytes by bounded kind.",
			"kind",
			ENCODING_BYTE_KINDS,
		);
		let encoding_invariants =
			namespaced_metric("storage_l2_encoding_invariant_violations_total");
		self.storage_l2_encoding_invariant_violations.render(
			&mut out,
			&encoding_invariants,
			"ADR 057 Encoding invariant violations by bounded kind.",
			"violation_kind",
			ENCODING_INVARIANT_KINDS,
		);
		let encoding_compute_plan = namespaced_metric("storage_l2_encoding_compute_plan_total");
		self.storage_l2_encoding_compute_plan.render(
			&mut out,
			&encoding_compute_plan,
			"ADR 058 Encoding compute planner decisions by bounded reason.",
		);
		let encoding_compute_kernel =
			namespaced_metric("storage_l2_encoding_compute_kernel_micros");
		self.storage_l2_encoding_compute_kernel_micros
			.render_micros(
				&mut out,
				&encoding_compute_kernel,
				"ADR 058 Encoding compute kernel duration in microseconds.",
			);
		let encoding_compute_rows = namespaced_metric("storage_l2_encoding_compute_rows_total");
		self.storage_l2_encoding_compute_rows.render(
			&mut out,
			&encoding_compute_rows,
			"ADR 058 Encoding compute rows by bounded kind.",
			"kind",
			ENCODING_COMPUTE_ROW_KINDS,
		);
		let encoding_compute_mask_bytes =
			namespaced_metric("storage_l2_encoding_compute_mask_bytes");
		render_histogram_raw(
			&mut out,
			&encoding_compute_mask_bytes,
			"ADR 058 Encoding compute mask bytes.",
			&self.storage_l2_encoding_compute_mask_bytes.snapshot(),
		);
		let encoding_compute_pages =
			namespaced_metric("storage_l2_encoding_compute_pages_fetched_total");
		self.storage_l2_encoding_compute_pages_fetched.render(
			&mut out,
			&encoding_compute_pages,
			"ADR 058 Encoding compute pages fetched by bounded kind.",
			"kind",
			ENCODING_COMPUTE_ROW_KINDS,
		);
		let encoding_compute_aggregate =
			namespaced_metric("storage_l2_encoding_compute_aggregate_total");
		self.storage_l2_encoding_compute_aggregate.render(
			&mut out,
			&encoding_compute_aggregate,
			"ADR 058 Encoding compute aggregate decisions by bounded reason.",
		);
		let encoding_compute_invariants =
			namespaced_metric("storage_l2_encoding_compute_invariant_violations_total");
		self.storage_l2_encoding_compute_invariant_violations
			.render(
				&mut out,
				&encoding_compute_invariants,
				"ADR 058 Encoding compute invariant violations by bounded kind.",
				"violation_kind",
				ENCODING_COMPUTE_INVARIANT_KINDS,
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
		render_counter(
			&mut out,
			"wendelon_storage_multirow_records_written_total",
			"Physical multi-row table records written to slab storage.",
			self.storage_multirow_records_written_total.get(),
		);
		render_counter(
			&mut out,
			"wendelon_storage_multirow_rows_written_total",
			"Logical table rows written inside physical multi-row records.",
			self.storage_multirow_rows_written_total.get(),
		);
		render_histogram_raw(
			&mut out,
			"wendelon_storage_multirow_rows_per_record",
			"Logical rows per physical multi-row table record.",
			&self.storage_multirow_rows_per_record.snapshot(),
		);
		render_histogram_micros(
			&mut out,
			"wendelon_storage_multirow_decode_micros",
			"Time spent decoding and materializing rows from multi-row records.",
			&self.storage_multirow_decode_micros.snapshot(),
		);
		render_counter(
			&mut out,
			"wendelon_storage_multirow_rows_materialized_total",
			"Logical rows materialized from physical multi-row records.",
			self.storage_multirow_rows_materialized_total.get(),
		);
		render_counter(
			&mut out,
			"wendelon_storage_multirow_point_read_amplification_bytes",
			"Physical bytes decoded while materializing rows from multi-row records.",
			self.storage_multirow_point_read_amplification_bytes.get(),
		);
		render_counter(
			&mut out,
			"wendelon_storage_multirow_recompact_total",
			"Background multi-row recompaction operations completed.",
			self.storage_multirow_recompact_total.get(),
		);
		render_counter(
			&mut out,
			"wendelon_storage_multirow_uncompacted_row_records_total",
			"Single-row table records observed as pending multi-row recompaction.",
			self.storage_multirow_uncompacted_row_records_total.get(),
		);
		// ADR 045 — append-only table operational metrics.
		render_counter(
			&mut out,
			"wendelon_storage_append_only_batches_total",
			"Append-only write batches admitted (ADR 045).",
			self.storage_append_only_batches_total.get(),
		);
		render_counter(
			&mut out,
			"wendelon_storage_append_only_rows_written_total",
			"Logical rows admitted into append-only tables.",
			self.storage_append_only_rows_written_total.get(),
		);
		render_counter(
			&mut out,
			"wendelon_storage_append_only_intra_batch_index_exhaustion_total",
			"Append-only writes rejected because the IntraBatchIndex u32 capacity was exhausted.",
			self.storage_append_only_intra_batch_index_exhaustion_total
				.get(),
		);
		render_counter(
			&mut out,
			"wendelon_storage_append_only_supersession_metadata_skipped_total",
			"Sealed append-only slabs whose CSB short-circuit skipped supersession-metadata inspection.",
			self.storage_append_only_supersession_metadata_skipped_total
				.get(),
		);
		self.storage_content_hash_admissions.render(
			&mut out,
			"wendelon_storage_content_hash_admissions_total",
			"Content-hash table rows admitted by outcome.",
		);
		self.storage_content_hash_duplicate_absorptions.render(
			&mut out,
			"wendelon_storage_content_hash_duplicate_absorptions_total",
			"Content-hash table duplicate rows absorbed at admission.",
		);
		self.storage_pk_delete_outcomes.render(
			&mut out,
			"wendelon_storage_pk_delete_outcomes_total",
			"PK-keyed table delete rows classified by supersession admission outcome.",
		);
		self.storage_pk_delete_requests.render(
			&mut out,
			"wendelon_storage_pk_delete_requests_total",
			"PK-keyed table delete requests admitted to supersession classification.",
		);
		self.storage_pk_delete_rows_deleted.render(
			&mut out,
			"wendelon_storage_pk_delete_rows_deleted_total",
			"PK-keyed table delete rows that wrote tombstone supersession markers.",
		);
		self.storage_pk_delete_rows_already_deleted.render(
			&mut out,
			"wendelon_storage_pk_delete_rows_already_deleted_total",
			"PK-keyed table delete rows absorbed because the latest version was already a tombstone.",
		);
		self.storage_pk_delete_rows_not_found.render(
			&mut out,
			"wendelon_storage_pk_delete_rows_not_found_total",
			"PK-keyed table delete rows absorbed because no version existed at the delete HLC.",
		);
		self.storage_pk_delete_resurrections.render(
			&mut out,
			"wendelon_storage_pk_delete_resurrections_total",
			"PK writes that resurrected a row whose latest prior version was a tombstone.",
		);
		self.storage_pk_delete_tombstone_boundary_versions_retained
			.render(
				&mut out,
				"wendelon_storage_pk_delete_tombstone_boundary_versions_retained_total",
				"PK tombstone boundary versions retained by cluster-MVCC prefix retention.",
			);
		self.storage_pk_upsert_no_digest_add.render(
			&mut out,
			"wendelon_storage_pk_upsert_no_digest_add_total",
			"PK upsert rows whose visible-row digest add was skipped because the key was already visible.",
		);
		self.storage_pk_admission_visibility_lookup_seconds.render(
			&mut out,
			"wendelon_storage_pk_admission_visibility_lookup_seconds",
			"Time spent resolving PK visible membership during write and delete admission.",
		);
		self.storage_table_retention_policy_updates.render(
			&mut out,
			"wendelon_storage_table_retention_policy_updates_total",
			"Per-table retention policy updates accepted through the typed lifecycle mutation surface.",
		);
		render_counter(
			&mut out,
			"wendelon_storage_table_retention_read_floor_rejects_total",
			"Table reads rejected because the requested snapshot predates the effective retention floor.",
			self.storage_table_retention_read_floor_rejects_total.get(),
		);
		render_counter(
			&mut out,
			"wendelon_storage_table_retention_read_filtered_rows_total",
			"Logical rows skipped because they are below the table's logical retention floor.",
			self.storage_table_retention_read_filtered_rows_total.get(),
		);
		self.storage_table_retention_l1_rows_pruned.render(
			&mut out,
			"wendelon_storage_table_retention_l1_rows_pruned_total",
			"Logical row versions pruned by table-aware L1 retention compaction.",
		);
		render_counter(
			&mut out,
			"wendelon_storage_table_retention_l1_bytes_reclaimed_total",
			"Bytes reclaimed by table-aware L1 retention compaction.",
			self.storage_table_retention_l1_bytes_reclaimed_total.get(),
		);
		self.storage_table_retention_l2_rows_pruned.render(
			&mut out,
			"wendelon_storage_table_retention_l2_rows_pruned_total",
			"Logical row versions pruned by table-aware L2 retention rewrites.",
		);
		render_counter(
			&mut out,
			"wendelon_storage_table_retention_l2_bytes_reclaimed_total",
			"Bytes reclaimed by table-aware L2 retention rewrites.",
			self.storage_table_retention_l2_bytes_reclaimed_total.get(),
		);
		render_counter(
			&mut out,
			"wendelon_storage_table_retention_pair_drops_total",
			"Data/tombstone row pairs jointly dropped after the effective table floor crossed the delete HLC.",
			self.storage_table_retention_pair_drops_total.get(),
		);
		render_counter(
			&mut out,
			"wendelon_storage_table_retention_ambiguous_rows_retained_total",
			"Rows retained because table ownership or lifecycle policy resolution was ambiguous.",
			self.storage_table_retention_ambiguous_rows_retained_total
				.get(),
		);
		render_gauge(
			&mut out,
			"wendelon_storage_table_retention_lag_seconds",
			"Maximum observed age in seconds of rows still physically present below an effective retention floor.",
			self.storage_table_retention_lag_seconds.get(),
		);
		self.storage_table_retention_policy_active_tables.render(
			&mut out,
			"wendelon_storage_table_retention_policy_active_tables_total",
			"Current table lifecycle catalog entries by retention mode.",
		);
		self.storage_runtime_filter_events.render(
			&mut out,
			"wendelon_storage_runtime_filter_events_total",
			"Bounded runtime-filter descriptor events for table queries.",
			"event",
			RUNTIME_FILTER_EVENTS,
		);
		self.storage_runtime_filter_advisory_dropped.render(
			&mut out,
			"wendelon_storage_runtime_filter_advisory_dropped_total",
			"Advisory runtime-filter drops by bounded reason.",
			"reason",
			RUNTIME_FILTER_DROP_REASONS,
		);
		self.storage_runtime_filter_rows.render(
			&mut out,
			"wendelon_storage_runtime_filter_rows_total",
			"Rows entering and surviving applied runtime filters.",
			"kind",
			RUNTIME_FILTER_ROW_KINDS,
		);
		self.lsm_compactions.render(
			&mut out,
			"wendelon_lsm_compactions_total",
			"ADR 055 LSM compactions by source compaction level and bounded result.",
		);
		self.lsm_compaction_input_slabs.render(
			&mut out,
			"wendelon_lsm_compaction_input_slabs",
			"Input slabs selected by the most recent ADR 055 compaction at each source level.",
		);
		self.lsm_compaction_output_bytes.render(
			&mut out,
			"wendelon_lsm_compaction_output_bytes",
			"Output bytes written by the most recent ADR 055 compaction at each source level.",
		);
		self.lsm_compaction_duration_millis.render(
			&mut out,
			"wendelon_lsm_compaction_duration_millis",
			"Duration in milliseconds of the most recent ADR 055 compaction at each source level.",
		);
		self.lsm_level_slab_count.render(
			&mut out,
			"wendelon_lsm_level_slab_count",
			"Live slab count by ADR 055 compaction level.",
		);
		self.lsm_level_total_bytes.render(
			&mut out,
			"wendelon_lsm_level_total_bytes",
			"Live slab bytes by ADR 055 compaction level.",
		);
		render_gauge(
			&mut out,
			"wendelon_lsm_pending_delete_slabs",
			"Retired ADR 055 slabs that have reached delete-pending state.",
			self.lsm_pending_delete_slabs.get(),
		);
		render_gauge(
			&mut out,
			"wendelon_lsm_retired_slab_pinned_bytes",
			"Retired ADR 055 slab bytes still protected by active reader pins.",
			self.lsm_retired_slab_pinned_bytes.get(),
		);
		self.lsm_digest_invariance_violations.render(
			&mut out,
			"wendelon_lsm_digest_invariance_violations_total",
			"ADR 055 digest invariance violations by bounded digest kind.",
		);
		self.storage_secondary_index_create.render(
			&mut out,
			"wendelon_storage_secondary_index_create_total",
			"Secondary indexes accepted through the replicated catalog create path.",
		);
		self.storage_secondary_index_drop.render(
			&mut out,
			"wendelon_storage_secondary_index_drop_total",
			"Secondary indexes accepted through the replicated catalog drop path.",
		);
		self.storage_secondary_index_lifecycle_transitions.render(
			&mut out,
			"wendelon_storage_secondary_index_lifecycle_transitions_total",
			"Secondary index catalog lifecycle transitions.",
		);
		self.storage_secondary_index_segments_built.render(
			&mut out,
			"wendelon_storage_secondary_index_segments_built_total",
			"Secondary index sidecar segments successfully built.",
		);
		self.storage_secondary_index_segments_failed.render(
			&mut out,
			"wendelon_storage_secondary_index_segments_failed_total",
			"Secondary index sidecar segment build or persistence failures.",
		);
		self.storage_secondary_index_entries_indexed.render(
			&mut out,
			"wendelon_storage_secondary_index_entries_indexed_total",
			"Secondary index entries emitted into sidecar segments.",
		);
		self.storage_secondary_index_active_buffer_failures.render(
			&mut out,
			"wendelon_storage_secondary_index_active_buffer_failures_total",
			"Secondary index active-buffer admission failures by reason.",
		);
		self.storage_secondary_index_query_plans.render(
			&mut out,
			"wendelon_storage_secondary_index_query_plans_total",
			"Table query plans by secondary-index planner mode.",
		);
		self.storage_secondary_index_candidate_rows.render(
			&mut out,
			"wendelon_storage_secondary_index_candidate_rows_total",
			"Secondary-index candidate rows before MVCC and predicate revalidation.",
		);
		self.storage_secondary_index_visible.render(
			&mut out,
			"wendelon_storage_secondary_index_visible_total",
			"Secondary-index candidate rows accepted after revalidation.",
		);
		self.storage_secondary_index_superseded.render(
			&mut out,
			"wendelon_storage_secondary_index_superseded_total",
			"Secondary-index candidate rows rejected because a newer visible version or tombstone superseded them.",
		);
		self.storage_secondary_index_stale_indexed_value.render(
			&mut out,
			"wendelon_storage_secondary_index_stale_indexed_value_total",
			"Secondary-index candidate rows rejected because the indexed column value no longer matches.",
		);
		self.storage_secondary_index_uncovered_slabs.render(
			&mut out,
			"wendelon_storage_secondary_index_uncovered_slabs_total",
			"Current uncovered source slabs per local secondary-index coverage tracker.",
		);
		self.storage_secondary_index_coverage_gap_slabs.render(
			&mut out,
			"wendelon_storage_secondary_index_coverage_gap_slabs",
			"Historical source slabs lacking a durable segment per secondary index — the invariant the read gate refuses on; distinct from uncovered_slabs_total, which counts in-process failure records only.",
		);
		self.storage_secondary_index_lookup_refusals.render(
			&mut out,
			"wendelon_storage_secondary_index_lookup_refusals_total",
			"Secondary-index lookups refused because a historical source slab lacks a durable segment.",
		);
		self.storage_secondary_index_backfill_seconds.render(
			&mut out,
			"wendelon_storage_secondary_index_backfill_seconds",
			"Duration of local secondary-index backfill work.",
		);
		self.storage_secondary_index_segment_state_digest_mismatch
			.render(
				&mut out,
				"wendelon_storage_secondary_index_segment_state_digest_mismatch_total",
				"Secondary-index sidecar segment state-digest validation failures.",
			);
		self.storage_secondary_index_logical_fingerprint_mismatch
			.render(
				&mut out,
				"wendelon_storage_secondary_index_logical_fingerprint_mismatch_total",
				"Secondary-index descriptor or sidecar logical-fingerprint mismatches.",
			);
		self.storage_secondary_index_corruption_failures.render(
			&mut out,
			"wendelon_storage_secondary_index_corruption_failures_total",
			"Secondary-index sidecar corruption failures that force planner fallback.",
		);
		self.storage_secondary_index_drop_gc_seconds.render(
			&mut out,
			"wendelon_storage_secondary_index_drop_gc_seconds",
			"Duration of local secondary-index drop-GC cleanup work.",
		);
		let compression_records_written_total =
			namespaced_metric("storage_compression_records_written_total");
		self.storage_compression_records_written.render(
			&mut out,
			&compression_records_written_total,
			"Storage record bodies encoded by compression codec.",
		);
		let compression_plaintext_bytes_total =
			namespaced_metric("storage_compression_plaintext_bytes_total");
		self.storage_compression_plaintext_bytes.render(
			&mut out,
			&compression_plaintext_bytes_total,
			"Uncompressed Arrow IPC record-body bytes before storage compression.",
		);
		let compression_encoded_bytes_total =
			namespaced_metric("storage_compression_encoded_bytes_total");
		self.storage_compression_encoded_bytes.render(
			&mut out,
			&compression_encoded_bytes_total,
			"Encoded Arrow IPC record-body bytes written to slab storage.",
		);
		let compression_encode_micros = namespaced_metric("storage_compression_encode_micros");
		self.storage_compression_encode_micros.render_micros(
			&mut out,
			&compression_encode_micros,
			"Storage record-body Arrow IPC encode duration in microseconds, including the uncompressed measurement encode and final codec encode.",
		);
		let compression_decode_micros = namespaced_metric("storage_compression_decode_micros");
		self.storage_compression_decode_micros.render_micros(
			&mut out,
			&compression_decode_micros,
			"Storage record-body Arrow IPC decode duration in microseconds.",
		);
		let compression_decompressed_bytes_total =
			namespaced_metric("storage_compression_decompressed_bytes_total");
		self.storage_compression_decompressed_bytes.render(
			&mut out,
			&compression_decompressed_bytes_total,
			"Uncompressed Arrow array bytes materialized after storage record-body decode.",
		);
		let compression_decode_error_total =
			namespaced_metric("storage_compression_decode_error_total");
		self.storage_compression_decode_error.render(
			&mut out,
			&compression_decode_error_total,
			"Storage record-body Arrow IPC decode errors by compression codec.",
		);
		let compression_l2_record_body_bytes_fetched_total =
			namespaced_metric("storage_compression_l2_record_body_bytes_fetched_total");
		self.storage_compression_l2_record_body_bytes_fetched
			.render(
				&mut out,
				&compression_l2_record_body_bytes_fetched_total,
				"Admitted L2 record-body bytes fetched by compression codec.",
			);
		let compression_ratio = namespaced_metric("storage_compression_ratio");
		self.storage_compression_ratio.render_ratio(
			&mut out,
			&compression_ratio,
			"Storage record-body encoded/plaintext byte ratio.",
		);
		let compression_uncompressed_resident_bytes =
			namespaced_metric("storage_compression_uncompressed_resident_bytes");
		render_gauge(
			&mut out,
			&compression_uncompressed_resident_bytes,
			"Resident uncompressed IPC bytes awaiting compression-policy convergence.",
			self.storage_compression_uncompressed_resident_bytes.get(),
		);
		let network_compression_batches_total =
			namespaced_metric("network_compression_batches_total");
		self.network_compression_batches.render(
			&mut out,
			&network_compression_batches_total,
			"Transport Arrow IPC batches encoded by codec, path, and result.",
		);
		let network_compression_encoded_bytes_total =
			namespaced_metric("network_compression_encoded_bytes_total");
		self.network_compression_encoded_bytes.render(
			&mut out,
			&network_compression_encoded_bytes_total,
			"Transport Arrow IPC encoded body bytes emitted by codec and path.",
		);
		let network_compression_encode_micros =
			namespaced_metric("network_compression_encode_micros");
		self.network_compression_encode_micros.render_micros(
			&mut out,
			&network_compression_encode_micros,
			"Transport Arrow IPC encode duration in microseconds by codec and path.",
		);
		let network_compression_ratio = namespaced_metric("network_compression_ratio");
		self.network_compression_ratio.render_ratio(
			&mut out,
			&network_compression_ratio,
			"Transport Arrow IPC encoded/plaintext byte ratio when available.",
		);
		let network_compression_decode_error_total =
			namespaced_metric("network_compression_decode_error_total");
		self.network_compression_decode_error.render(
			&mut out,
			&network_compression_decode_error_total,
			"Transport Arrow IPC decode errors by compression codec.",
		);
		self.cdc_subscriptions.render(
			&mut out,
			"wendelon_cdc_subscriptions_total",
			"CDC subscription lifecycle transitions by bounded result.",
		);
		self.cdc_events_emitted.render(
			&mut out,
			"wendelon_cdc_events_emitted_total",
			"CDC events emitted by source.",
		);
		self.cdc_event_bytes_emitted.render(
			&mut out,
			"wendelon_cdc_event_bytes_emitted_total",
			"CDC encoded event bytes emitted by source.",
		);
		self.cdc_checkpoints_emitted.render(
			&mut out,
			"wendelon_cdc_checkpoints_emitted_total",
			"CDC checkpoint frames emitted by source.",
		);
		self.cdc_rejects.render(
			&mut out,
			"wendelon_cdc_rejects_total",
			"CDC terminal rejects by bounded code.",
		);
		self.cdc_subscription_lag_micros.render_micros(
			&mut out,
			"wendelon_cdc_subscription_lag_micros",
			"CDC subscription lag in microseconds by source.",
		);
		render_counter(
			&mut out,
			"wendelon_cdc_ring_overflow_total",
			"CDC live-tail ring FIFO overflow events.",
			self.cdc_ring_overflow_total.get(),
		);
		render_histogram_micros(
			&mut out,
			"wendelon_cdc_catchup_to_live_handoff_micros",
			"CDC catchup-to-live handoff duration in microseconds.",
			&self.cdc_catchup_to_live_handoff_micros.snapshot(),
		);
		render_gauge(
			&mut out,
			"wendelon_cdc_staged_entries_in_ring",
			"CDC staged entries currently retained in live-tail rings.",
			self.cdc_staged_entries_in_ring.get(),
		);
		render_counter(
			&mut out,
			"wendelon_cdc_pending_finalization_evicted_total",
			"CDC staged entries evicted before finalization cleanup observed them.",
			self.cdc_pending_finalization_evicted_total.get(),
		);
		render_counter(
			&mut out,
			"wendelon_cdc_locality_heartbeats_committed_total",
			"CDC locality heartbeat finalizations accepted by Raft.",
			self.cdc_locality_heartbeats_committed_total.get(),
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

fn namespaced_metric(segment: &str) -> String {
	format!("wendelon_{segment}")
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

fn render_histogram_family_header(out: &mut String, name: &str, help: &str) {
	let _ = writeln!(out, "# HELP {name} {help}");
	let _ = writeln!(out, "# TYPE {name} histogram");
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

fn render_histogram_series(
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
			"{name}_bucket{{{rendered_labels},le=\"{}\"}} {cumulative}",
			render_seconds_from_micros(bound_micros),
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
		render_seconds_from_micros(snapshot.sum_micros)
	);
	let _ = writeln!(out, "{name}_count{{{rendered_labels}}} {}", snapshot.count);
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

fn render_histogram_ratio_series(
	out: &mut String,
	name: &str,
	labels: &[(&str, &str)],
	snapshot: &HistogramSnapshot<'_>,
) {
	let rendered_labels = render_labels(labels);
	let mut cumulative = 0u64;
	for (idx, bound_milli) in snapshot.bounds_micros.iter().copied().enumerate() {
		cumulative += snapshot.bucket_counts[idx];
		let _ = writeln!(
			out,
			"{name}_bucket{{{rendered_labels},le=\"{}\"}} {cumulative}",
			render_ratio_milli(bound_milli),
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
		render_ratio_milli(snapshot.sum_micros),
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

fn table_object_id_label(table_object_id: TableObjectId) -> String {
	let mut rendered = String::with_capacity(32);
	for byte in table_object_id.as_bytes() {
		let _ = write!(rendered, "{byte:02x}");
	}
	rendered
}

fn index_object_id_label(index_object_id: IndexObjectId) -> String {
	let mut rendered = String::with_capacity(32);
	for byte in index_object_id.as_bytes() {
		let _ = write!(rendered, "{byte:02x}");
	}
	rendered
}

fn render_ratio_milli(value_milli: u64) -> String {
	let whole = value_milli / 1000;
	let fractional_milli = value_milli % 1000;
	if fractional_milli == 0 {
		return whole.to_string();
	}
	let mut fractional = format!("{fractional_milli:03}");
	while fractional.ends_with('0') {
		fractional.pop();
	}
	format!("{whole}.{fractional}")
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
		NetworkCompressionPath, PbcRuntime, PendingFinalizationReplayPath,
		TableRetentionMetricPolicy, render_histogram, table_object_id_label,
	};
	use crate::schema::{IndexObjectId, TableObjectId, TenantId, WriteStatus};
	use std::time::Duration;

	const TEST_BUCKETS_MICROS: &[u64] = &[500, 1_000, 2_500];

	fn namespaced_test_metric(segment: &str) -> String {
		format!("{}{}", "wendelon_", segment)
	}

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
	fn registry_renders_storage_compression_metric_values() {
		let registry = MetricsRegistry::default();
		registry.observe_storage_compression_encode(
			crate::schema::RecordBodyCompressionPolicy::Uncompressed,
			100,
			100,
			Duration::from_micros(8),
		);
		registry.observe_storage_compression_encode(
			crate::schema::RecordBodyCompressionPolicy::Lz4Frame,
			1_000,
			400,
			Duration::from_micros(10),
		);
		registry.observe_storage_compression_decode(
			crate::schema::RecordBodyCompressionPolicy::Lz4Frame,
			400,
			2_048,
			Duration::from_micros(15),
		);
		registry.observe_storage_compression_decode_error(
			crate::schema::RecordBodyCompressionPolicy::Lz4Frame,
		);
		registry.observe_storage_compression_l2_record_body_fetch(
			crate::schema::RecordBodyCompressionPolicy::Lz4Frame,
			456,
		);
		registry.set_storage_compression_uncompressed_resident_bytes(1_234);

		let rendered = registry.render_prometheus();
		let records_written = namespaced_test_metric("storage_compression_records_written_total");
		assert!(rendered.contains(&format!("{records_written}{{codec=\"uncompressed\"}} 1")));
		assert!(rendered.contains(&format!("{records_written}{{codec=\"lz4_frame\"}} 1")));
		assert!(rendered.contains(&format!(
			"{}{{codec=\"lz4_frame\"}} 1000",
			namespaced_test_metric("storage_compression_plaintext_bytes_total")
		)));
		assert!(rendered.contains(&format!(
			"{}{{codec=\"lz4_frame\"}} 400",
			namespaced_test_metric("storage_compression_encoded_bytes_total")
		)));
		assert!(rendered.contains(&format!(
			"{}_sum{{codec=\"lz4_frame\"}} 10",
			namespaced_test_metric("storage_compression_encode_micros")
		)));
		assert!(rendered.contains(&format!(
			"{}_sum{{codec=\"lz4_frame\"}} 15",
			namespaced_test_metric("storage_compression_decode_micros")
		)));
		assert!(rendered.contains(&format!(
			"{}{{codec=\"lz4_frame\"}} 2048",
			namespaced_test_metric("storage_compression_decompressed_bytes_total")
		)));
		assert!(rendered.contains(&format!(
			"{}{{codec=\"lz4_frame\"}} 1",
			namespaced_test_metric("storage_compression_decode_error_total")
		)));
		assert!(rendered.contains(&format!(
			"{}{{codec=\"lz4_frame\"}} 456",
			namespaced_test_metric("storage_compression_l2_record_body_bytes_fetched_total")
		)));
		assert!(rendered.contains(&format!(
			"{}_sum{{codec=\"lz4_frame\"}} 0.4",
			namespaced_test_metric("storage_compression_ratio")
		)));
		assert!(rendered.contains(&format!(
			"{} 1234",
			namespaced_test_metric("storage_compression_uncompressed_resident_bytes")
		)));
	}

	#[test]
	fn registry_renders_network_compression_metric_values() {
		let registry = MetricsRegistry::default();
		registry.observe_network_compression_encode(
			crate::schema::RecordBodyCompressionPolicy::Lz4Frame,
			NetworkCompressionPath::QueryResponse,
			321,
			Duration::from_micros(11),
			Some(650),
		);
		registry.observe_network_compression_encode_error(
			crate::schema::RecordBodyCompressionPolicy::Lz4Frame,
			NetworkCompressionPath::ReadResponse,
		);
		registry.observe_network_compression_decode_error(
			crate::schema::RecordBodyCompressionPolicy::Lz4Frame,
		);

		let rendered = registry.render_prometheus();
		let batches = namespaced_test_metric("network_compression_batches_total");
		assert!(rendered.contains(&format!(
			"{batches}{{codec=\"lz4_frame\",path=\"query_response\",result=\"compressed\"}} 1"
		)));
		assert!(rendered.contains(&format!(
			"{batches}{{codec=\"lz4_frame\",path=\"read_response\",result=\"error\"}} 1"
		)));
		assert!(rendered.contains(&format!(
			"{}{{codec=\"lz4_frame\",path=\"query_response\"}} 321",
			namespaced_test_metric("network_compression_encoded_bytes_total")
		)));
		assert!(rendered.contains(&format!(
			"{}_sum{{codec=\"lz4_frame\",path=\"query_response\"}} 11",
			namespaced_test_metric("network_compression_encode_micros")
		)));
		assert!(rendered.contains(&format!(
			"{}_sum{{codec=\"lz4_frame\",path=\"query_response\"}} 0.65",
			namespaced_test_metric("network_compression_ratio")
		)));
		let decode_errors = namespaced_test_metric("network_compression_decode_error_total");
		assert!(rendered.contains(&format!("{decode_errors}{{codec=\"lz4_frame\"}} 1")));
		assert!(!rendered.contains(concat!("network_compression_plain", "text_bytes_total")));
		assert!(!rendered.contains(concat!("network_compression_decode", "_micros")));
		assert!(!rendered.contains(concat!(
			"network_compression_batches_total{codec=\"lz4_frame\",path=\"read_response\",result=\"",
			"uncompressed_",
			"passthrough\"}"
		)));
		assert!(!rendered.contains(concat!(
			"network_compression_decode_error_total{codec=\"lz4_frame\",",
			"reason="
		)));
	}

	#[test]
	fn registry_renders_l2_column_fetch_metric_values() {
		let registry = MetricsRegistry::default();
		registry.observe_storage_l2_column_fetch_plan(Duration::from_micros(7));
		registry.observe_storage_l2_column_fetch_selected_chunk_runs(3);
		registry.observe_storage_l2_column_fetch_fallback("too_many_bytes");

		let rendered = registry.render_prometheus();
		assert!(rendered.contains(&format!(
			"{}_sum 7",
			namespaced_test_metric("storage_l2_column_fetch_plan_micros")
		)));
		assert!(rendered.contains(&format!(
			"{}_sum 3",
			namespaced_test_metric("storage_l2_column_fetch_selected_chunk_runs")
		)));
		assert!(rendered.contains(&format!(
			"{}{{reason=\"too_many_bytes\"}} 1",
			namespaced_test_metric("storage_l2_column_fetch_fallback_total")
		)));
	}

	#[test]
	fn registry_renders_table_retention_metric_values() {
		let registry = MetricsRegistry::default();
		registry
			.observe_storage_table_retention_policy_update(TableRetentionMetricPolicy::FixedWindow);
		registry
			.observe_storage_table_retention_policy_update(TableRetentionMetricPolicy::Disabled);
		registry.observe_storage_table_retention_read_floor_reject();
		registry.observe_storage_table_retention_read_filtered_rows(3);
		registry.observe_storage_table_retention_l1_prune(5, 2, 1024, 1, 4, 30);
		registry.observe_storage_table_retention_l2_prune(7, 3, 2048, 2, 6, 90);
		registry.set_storage_table_retention_policy_active_tables(11, 12, 13);

		let rendered = registry.render_prometheus();
		assert!(rendered.contains(&format!(
			"{}{{policy=\"fixed_window\"}} 1",
			namespaced_test_metric("storage_table_retention_policy_updates_total")
		)));
		assert!(rendered.contains(&format!(
			"{}{{policy=\"disabled\"}} 1",
			namespaced_test_metric("storage_table_retention_policy_updates_total")
		)));
		assert!(rendered.contains(&format!(
			"{} 1",
			namespaced_test_metric("storage_table_retention_read_floor_rejects_total")
		)));
		assert!(rendered.contains(&format!(
			"{} 3",
			namespaced_test_metric("storage_table_retention_read_filtered_rows_total")
		)));
		assert!(rendered.contains(&format!(
			"{}{{kind=\"data\"}} 5",
			namespaced_test_metric("storage_table_retention_l1_rows_pruned_total")
		)));
		assert!(rendered.contains(&format!(
			"{}{{kind=\"tombstone\"}} 2",
			namespaced_test_metric("storage_table_retention_l1_rows_pruned_total")
		)));
		assert!(rendered.contains(&format!(
			"{} 1024",
			namespaced_test_metric("storage_table_retention_l1_bytes_reclaimed_total")
		)));
		assert!(rendered.contains(&format!(
			"{}{{kind=\"data\"}} 7",
			namespaced_test_metric("storage_table_retention_l2_rows_pruned_total")
		)));
		assert!(rendered.contains(&format!(
			"{}{{kind=\"tombstone\"}} 3",
			namespaced_test_metric("storage_table_retention_l2_rows_pruned_total")
		)));
		assert!(rendered.contains(&format!(
			"{} 2048",
			namespaced_test_metric("storage_table_retention_l2_bytes_reclaimed_total")
		)));
		assert!(rendered.contains(&format!(
			"{} 3",
			namespaced_test_metric("storage_table_retention_pair_drops_total")
		)));
		assert!(rendered.contains(&format!(
			"{} 10",
			namespaced_test_metric("storage_table_retention_ambiguous_rows_retained_total")
		)));
		assert!(rendered.contains(&format!(
			"{} 90",
			namespaced_test_metric("storage_table_retention_lag_seconds")
		)));
		assert!(rendered.contains(&format!(
			"{}{{mode=\"inherit\"}} 11",
			namespaced_test_metric("storage_table_retention_policy_active_tables_total")
		)));
		assert!(rendered.contains(&format!(
			"{}{{mode=\"disabled\"}} 12",
			namespaced_test_metric("storage_table_retention_policy_active_tables_total")
		)));
		assert!(rendered.contains(&format!(
			"{}{{mode=\"fixed\"}} 13",
			namespaced_test_metric("storage_table_retention_policy_active_tables_total")
		)));
	}

	#[test]
	fn registry_renders_secondary_index_metric_values() {
		let registry = MetricsRegistry::default();
		let tenant = TenantId::new(52);
		let table = TableObjectId::new([0x52; 16]);
		let index = IndexObjectId::new([0x9a; 16]);

		registry.observe_storage_secondary_index_create(tenant, table, index);
		registry.observe_storage_secondary_index_drop(tenant, table, index, "ready");
		registry.observe_storage_secondary_index_lifecycle_transition(
			tenant, table, index, "building", "ready",
		);
		registry.observe_storage_secondary_index_segment_built(tenant, table, index, 7);
		registry.observe_storage_secondary_index_segment_failed(
			tenant,
			table,
			index,
			"persist_failure",
		);
		registry.observe_storage_secondary_index_active_buffer_failure(
			tenant,
			table,
			index,
			"active_buffer_overflow",
		);
		registry.observe_storage_secondary_index_query_plan(tenant, table, index, "indexed");
		registry.observe_storage_secondary_index_lookup(tenant, table, index, 11, 5, 3, 2);
		registry.set_storage_secondary_index_uncovered_slabs(tenant, table, index, 4);
		registry.set_storage_secondary_index_coverage_gap_slabs(tenant, table, index, 2);
		registry.observe_storage_secondary_index_lookup_refusal(tenant, table, index);
		registry.observe_storage_secondary_index_backfill_seconds(
			tenant,
			table,
			index,
			Duration::from_millis(9),
		);
		registry
			.observe_storage_secondary_index_segment_state_digest_mismatch(tenant, table, index);
		registry.observe_storage_secondary_index_logical_fingerprint_mismatch(tenant, table, index);
		registry.observe_storage_secondary_index_corruption_failure(tenant, table, index);
		registry.observe_storage_secondary_index_drop_gc_seconds(
			tenant,
			table,
			index,
			Duration::from_millis(7),
		);

		let rendered = registry.render_prometheus();
		assert!(rendered.contains(&namespaced_test_metric(
			"storage_secondary_index_create_total"
		)));
		assert!(rendered.contains("from=\"building\",to=\"ready\""));
		assert!(rendered.contains(&format!(
			"{}{{tenant=\"52\",table=\"52525252525252525252525252525252\",index=\"9a9a9a9a9a9a9a9a9a9a9a9a9a9a9a9a\"}} 7",
			namespaced_test_metric("storage_secondary_index_entries_indexed_total")
		)));
		assert!(rendered.contains(&format!(
			"{}{{tenant=\"52\",table=\"52525252525252525252525252525252\",index=\"9a9a9a9a9a9a9a9a9a9a9a9a9a9a9a9a\",reason=\"active_buffer_overflow\"}} 1",
			namespaced_test_metric("storage_secondary_index_active_buffer_failures_total")
		)));
		assert!(rendered.contains(&format!(
			"{}{{tenant=\"52\",table=\"52525252525252525252525252525252\",index=\"9a9a9a9a9a9a9a9a9a9a9a9a9a9a9a9a\",mode=\"indexed\"}} 1",
			namespaced_test_metric("storage_secondary_index_query_plans_total")
		)));
		assert!(rendered.contains(&format!(
			"{}{{tenant=\"52\",table=\"52525252525252525252525252525252\",index=\"9a9a9a9a9a9a9a9a9a9a9a9a9a9a9a9a\"}} 4",
			namespaced_test_metric("storage_secondary_index_uncovered_slabs_total")
		)));
		assert!(rendered.contains(&format!(
			"{}{{tenant=\"52\",table=\"52525252525252525252525252525252\",index=\"9a9a9a9a9a9a9a9a9a9a9a9a9a9a9a9a\"}} 2",
			namespaced_test_metric("storage_secondary_index_coverage_gap_slabs")
		)));
		assert!(rendered.contains(&format!(
			"{}{{tenant=\"52\",table=\"52525252525252525252525252525252\",index=\"9a9a9a9a9a9a9a9a9a9a9a9a9a9a9a9a\"}} 1",
			namespaced_test_metric("storage_secondary_index_lookup_refusals_total")
		)));
		assert!(rendered.contains(&format!(
			"{}_sum{{tenant=\"52\",table=\"52525252525252525252525252525252\",index=\"9a9a9a9a9a9a9a9a9a9a9a9a9a9a9a9a\"}} 0.009",
			namespaced_test_metric("storage_secondary_index_backfill_seconds")
		)));
		assert!(rendered.contains(&namespaced_test_metric(
			"storage_secondary_index_drop_gc_seconds"
		)));
	}

	#[test]
	fn secondary_index_declaration_emits_zero_baselines_before_driver_or_lookup() {
		let registry = MetricsRegistry::default();
		let tenant = TenantId::new(81);
		let table = TableObjectId::new([0x81; 16]);
		let index = IndexObjectId::new([0x18; 16]);
		registry.observe_storage_secondary_index_create(tenant, table, index);

		let rendered = registry.render_prometheus();
		let labels = "tenant=\"81\",table=\"81818181818181818181818181818181\",index=\"18181818181818181818181818181818\"";
		assert!(rendered.contains(&format!(
			"{}{{{labels}}} 0",
			namespaced_test_metric("storage_secondary_index_coverage_gap_slabs")
		)));
		assert!(rendered.contains(&format!(
			"{}{{{labels}}} 0",
			namespaced_test_metric("storage_secondary_index_lookup_refusals_total")
		)));
	}

	#[test]
	fn recovered_secondary_index_initializer_is_idempotent_and_never_resets_refusals() {
		let registry = MetricsRegistry::default();
		let tenant = TenantId::new(82);
		let table = TableObjectId::new([0x82; 16]);
		let index = IndexObjectId::new([0x28; 16]);
		let labels = "tenant=\"82\",table=\"82828282828282828282828282828282\",index=\"28282828282828282828282828282828\"";

		// A recovered catalog row has no declaration event in this process. The
		// driver initializer must still make zero observable immediately.
		registry.initialize_storage_secondary_index_series(tenant, table, index);
		let rendered = registry.render_prometheus();
		assert!(rendered.contains(&format!(
			"{}{{{labels}}} 0",
			namespaced_test_metric("storage_secondary_index_lookup_refusals_total")
		)));

		// Reasserting the series on every driver tick must not reset a counter.
		registry.observe_storage_secondary_index_lookup_refusal(tenant, table, index);
		registry.initialize_storage_secondary_index_series(tenant, table, index);
		registry.initialize_storage_secondary_index_series(tenant, table, index);
		let rendered = registry.render_prometheus();
		assert!(rendered.contains(&format!(
			"{}{{{labels}}} 1",
			namespaced_test_metric("storage_secondary_index_lookup_refusals_total")
		)));
	}

	#[test]
	fn registry_renders_pk_delete_outcome_metric_values() {
		let registry = MetricsRegistry::default();
		let table = TableObjectId::new([0x50; 16]);
		registry.observe_storage_pk_delete_request(TenantId::new(50), table);
		registry.observe_storage_pk_delete_outcomes(TenantId::new(50), table, 3, 2, 1);
		registry.observe_storage_pk_delete_resurrections(TenantId::new(50), table, 4);
		registry.observe_storage_pk_delete_tombstone_boundary_versions_retained(
			TenantId::new(50),
			table,
			5,
		);
		registry.observe_storage_pk_upsert_no_digest_add(TenantId::new(50), table, 6);
		registry.observe_storage_pk_admission_visibility_lookup(
			TenantId::new(50),
			table,
			Duration::from_micros(7),
		);

		let rendered = registry.render_prometheus();
		let metric = namespaced_test_metric("storage_pk_delete_outcomes_total");
		let table_label = table_object_id_label(table);
		assert!(rendered.contains(&format!(
			"{metric}{{tenant=\"50\",table=\"{table_label}\",outcome=\"deleted\"}} 3"
		)));
		assert!(rendered.contains(&format!(
			"{metric}{{tenant=\"50\",table=\"{table_label}\",outcome=\"already_deleted\"}} 2"
		)));
		assert!(rendered.contains(&format!(
			"{metric}{{tenant=\"50\",table=\"{table_label}\",outcome=\"not_found\"}} 1"
		)));
		assert!(rendered.contains(&format!(
			"{}{{tenant=\"50\",table=\"{table_label}\"}} 1",
			namespaced_test_metric("storage_pk_delete_requests_total")
		)));
		assert!(rendered.contains(&format!(
			"{}{{tenant=\"50\",table=\"{table_label}\"}} 3",
			namespaced_test_metric("storage_pk_delete_rows_deleted_total")
		)));
		assert!(rendered.contains(&format!(
			"{}{{tenant=\"50\",table=\"{table_label}\"}} 2",
			namespaced_test_metric("storage_pk_delete_rows_already_deleted_total")
		)));
		assert!(rendered.contains(&format!(
			"{}{{tenant=\"50\",table=\"{table_label}\"}} 1",
			namespaced_test_metric("storage_pk_delete_rows_not_found_total")
		)));
		assert!(rendered.contains(&format!(
			"{}{{tenant=\"50\",table=\"{table_label}\"}} 4",
			namespaced_test_metric("storage_pk_delete_resurrections_total")
		)));
		assert!(rendered.contains(&format!(
			"{}{{tenant=\"50\",table=\"{table_label}\"}} 5",
			namespaced_test_metric("storage_pk_delete_tombstone_boundary_versions_retained_total")
		)));
		assert!(rendered.contains(&format!(
			"{}{{tenant=\"50\",table=\"{table_label}\"}} 6",
			namespaced_test_metric("storage_pk_upsert_no_digest_add_total")
		)));
		assert!(rendered.contains(&format!(
			"{}_sum{{tenant=\"50\",table=\"{table_label}\"}} 0.000007",
			namespaced_test_metric("storage_pk_admission_visibility_lookup_seconds")
		)));
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
		registry.observe_agg_execution(20, 12, Duration::from_micros(90), 128, false);
		registry.observe_agg_merge(2, Duration::from_micros(35));
		registry.observe_agg_int_overflow();
		registry.agg_csb_short_circuit_total.inc();
		registry.observe_agg_csb_short_circuit_skipped_untracked_tombstone_metadata();
		registry.observe_agg_csb_short_circuit_skipped_supersession_metadata_untracked();
		let descriptor_table = TableObjectId::new([0x51; 16]);
		let descriptor_table_label = table_object_id_label(descriptor_table);
		registry
			.observe_storage_csb_logical_fingerprint_mismatch(TenantId::new(9), descriptor_table);
		registry.observe_storage_csb_payload_coverage_fall_through(
			TenantId::new(9),
			descriptor_table,
			"missing_min_max",
		);
		registry.observe_storage_csb_pre_adr_051_cliff(TenantId::new(9), descriptor_table);
		registry.observe_storage_aggregate_cache_logical_fingerprint_hit(
			TenantId::new(9),
			descriptor_table,
		);
		registry.observe_storage_aggregate_cache_logical_fingerprint_miss(
			TenantId::new(9),
			descriptor_table,
		);
		registry.observe_storage_descriptor_recompute(
			TenantId::new(9),
			descriptor_table,
			Duration::from_micros(21),
		);
		registry.observe_storage_descriptor_validate(
			TenantId::new(9),
			descriptor_table,
			Duration::from_micros(34),
		);
		registry
			.observe_storage_descriptor_logical_only_mutation(TenantId::new(9), descriptor_table);
		registry
			.observe_storage_descriptor_physical_only_mutation(TenantId::new(9), descriptor_table);
		registry.observe_storage_descriptor_combined_mutation(TenantId::new(9), descriptor_table);
		registry.agg_skipped_no_capability_total.inc();

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
		assert!(rendered.contains("wendelon_agg_request_total 1"));
		assert!(rendered.contains("wendelon_agg_rows_scanned_total 20"));
		assert!(rendered.contains("wendelon_agg_rows_admitted_total 12"));
		assert!(rendered.contains("wendelon_agg_rows_filtered_out_total 8"));
		assert!(rendered.contains("wendelon_agg_partials_merged_total 2"));
		assert!(rendered.contains("wendelon_agg_csb_short_circuit_total 1"));
		assert!(rendered.contains(
			"wendelon_agg_csb_short_circuit_skipped_tombstone_metadata_untracked_total 1"
		));
		assert!(rendered.contains(
			"wendelon_agg_csb_short_circuit_skipped_supersession_metadata_untracked_total 1"
		));
		assert!(rendered.contains(&format!(
			"wendelon_storage_csb_merge_logical_fingerprint_mismatch_total{{tenant=\"9\",table=\"{descriptor_table_label}\"}} 1"
		)));
		assert!(rendered.contains(&format!(
			"wendelon_storage_csb_payload_coverage_fall_through_total{{tenant=\"9\",table=\"{descriptor_table_label}\",reason=\"missing_min_max\"}} 1"
		)));
		assert!(rendered.contains(&format!(
			"wendelon_storage_csb_merge_pre_adr_051_cliff_total{{tenant=\"9\",table=\"{descriptor_table_label}\"}} 1"
		)));
		assert!(rendered.contains(&format!(
			"wendelon_storage_aggregate_cache_logical_fingerprint_hit_total{{tenant=\"9\",table=\"{descriptor_table_label}\"}} 1"
		)));
		assert!(rendered.contains(&format!(
			"wendelon_storage_aggregate_cache_logical_fingerprint_miss_total{{tenant=\"9\",table=\"{descriptor_table_label}\"}} 1"
		)));
		assert!(rendered.contains(&format!(
			"wendelon_storage_descriptor_recompute_seconds_bucket{{tenant=\"9\",table=\"{descriptor_table_label}\",le=\"0.000025\"}} 1"
		)));
		assert!(rendered.contains(&format!(
			"wendelon_storage_descriptor_validate_seconds_bucket{{tenant=\"9\",table=\"{descriptor_table_label}\",le=\"0.00005\"}} 1"
		)));
		assert!(rendered.contains(&format!(
			"wendelon_storage_descriptor_logical_only_mutations_total{{tenant=\"9\",table=\"{descriptor_table_label}\"}} 1"
		)));
		assert!(rendered.contains(&format!(
			"wendelon_storage_descriptor_physical_only_mutations_total{{tenant=\"9\",table=\"{descriptor_table_label}\"}} 1"
		)));
		assert!(rendered.contains(&format!(
			"wendelon_storage_descriptor_combined_mutations_total{{tenant=\"9\",table=\"{descriptor_table_label}\"}} 1"
		)));
		assert!(rendered.contains("wendelon_agg_int_overflow_total 1"));
		assert!(rendered.contains("wendelon_agg_skipped_no_capability_total 1"));
		assert!(rendered.contains("wendelon_agg_partial_bytes_bucket{le=\"128\"} 1"));
		assert!(rendered.contains("wendelon_agg_exec_micros_bucket{le=\"100\"} 1"));
		assert!(rendered.contains("wendelon_agg_merge_micros_bucket{le=\"50\"} 1"));
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
