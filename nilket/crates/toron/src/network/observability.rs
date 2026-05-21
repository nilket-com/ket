use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{RwLock, oneshot};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_rustls::TlsAcceptor;

use crate::catalog::{DescriptorRefreshMetrics, DescriptorSyncMetrics};
use crate::config::ObservabilityListeners;
use crate::distributed::budget::{
	AdmissionResource, ByteCount, TenantAdmissionController, TenantAdmissionRejectionCounters,
	TenantBudgetPolicy,
};
use crate::distributed::cancellation::{
	CancellationMetrics, CancellationOutcome, CancellationTrigger,
};
use crate::network::connection::SqlConnectionLimiter;
use crate::network::observability_http::{READ_REQUEST_TIMEOUT, read_request, write_response};
use crate::storage::TenantId;

pub const HEALTH_PATH: &str = "/health";
pub const READY_PATH: &str = "/ready";
pub const METRICS_PATH: &str = "/metrics";
pub const TEXT_CONTENT_TYPE: &str = "text/plain; charset=utf-8";
pub const JSON_CONTENT_TYPE: &str = "application/json; charset=utf-8";
pub const METRICS_CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObservabilityEndpoints {
	pub health: SocketAddr,
	pub ready: SocketAddr,
	pub metrics: SocketAddr,
}

pub struct ObservabilityHandles {
	pub health: ListenerHandle,
	pub ready: ListenerHandle,
	pub metrics: ListenerHandle,
}

pub struct ListenerHandle {
	pub local_addr: SocketAddr,
	pub task: JoinHandle<()>,
}

pub struct ObservabilityShutdownReceivers {
	pub health: oneshot::Receiver<()>,
	pub ready: oneshot::Receiver<()>,
	pub metrics: oneshot::Receiver<()>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize)]
pub struct ReadinessSnapshot {
	pub ready: bool,
	pub worker_listener_bound: bool,
	pub sql_listener_bound: bool,
	pub observability_listeners_bound: bool,
	pub descriptors_loaded: u64,
	pub static_workers_configured: u32,
	pub uptime_seconds: u64,
	pub degraded_reason: Option<String>,
}

impl ReadinessSnapshot {
	pub fn is_ready(&self) -> bool {
		self.ready
			&& self.worker_listener_bound
			&& self.sql_listener_bound
			&& self.observability_listeners_bound
			&& self.degraded_reason.is_none()
	}

	pub fn http_status(&self) -> &'static str {
		if self.is_ready() {
			"200 OK"
		} else {
			"503 Service Unavailable"
		}
	}

	pub fn to_json(&self) -> String {
		let degraded_reason = self
			.degraded_reason
			.as_ref()
			.map(|reason| serde_json::to_string(reason).expect("serialize degraded reason"))
			.unwrap_or_else(|| "null".to_string());
		format!(
			"{{\"ready\":{},\"worker_listener_bound\":{},\"sql_listener_bound\":{},\"observability_listeners_bound\":{},\"descriptors_loaded\":{},\"static_workers_configured\":{},\"uptime_seconds\":{},\"degraded_reason\":{}}}",
			self.ready,
			self.worker_listener_bound,
			self.sql_listener_bound,
			self.observability_listeners_bound,
			self.descriptors_loaded,
			self.static_workers_configured,
			self.uptime_seconds,
			degraded_reason
		)
	}
}

pub struct ReadinessState {
	started_at: Instant,
	worker_listener_bound: bool,
	sql_listener_bound: bool,
	observability_listeners_bound: bool,
	descriptors_loaded: AtomicU64,
	static_workers_configured: AtomicU64,
	degraded_reason: RwLock<Option<String>>,
}

impl ReadinessState {
	pub fn new(
		worker_listener_bound: bool,
		sql_listener_bound: bool,
		observability_listeners_bound: bool,
		descriptors_loaded: u64,
		static_workers_configured: u64,
	) -> Self {
		Self {
			started_at: Instant::now(),
			worker_listener_bound,
			sql_listener_bound,
			observability_listeners_bound,
			descriptors_loaded: AtomicU64::new(descriptors_loaded),
			static_workers_configured: AtomicU64::new(static_workers_configured),
			degraded_reason: RwLock::new(None),
		}
	}

	pub async fn snapshot(&self) -> ReadinessSnapshot {
		let degraded_reason = self.degraded_reason.read().await.clone();
		let ready = degraded_reason.is_none()
			&& self.worker_listener_bound
			&& self.sql_listener_bound
			&& self.observability_listeners_bound;
		ReadinessSnapshot {
			ready,
			worker_listener_bound: self.worker_listener_bound,
			sql_listener_bound: self.sql_listener_bound,
			observability_listeners_bound: self.observability_listeners_bound,
			descriptors_loaded: self.descriptors_loaded.load(Ordering::Relaxed),
			static_workers_configured: self.static_workers_configured.load(Ordering::Relaxed)
				as u32,
			uptime_seconds: self.started_at.elapsed().as_secs(),
			degraded_reason,
		}
	}

	pub async fn set_degraded_reason(&self, reason: Option<String>) {
		*self.degraded_reason.write().await = reason;
	}
}

pub struct MetricsRegistry {
	started_at: Instant,
	listener_accepts: ListenerCounters,
	listener_rejects: ListenerRejectCounters,
	metrics_scrapes_total: AtomicU64,
	descriptors_loaded: AtomicU64,
	static_workers_configured: AtomicU64,
	descriptor_sync_metrics: Arc<DescriptorSyncMetrics>,
	descriptor_refresh_metrics: Arc<DescriptorRefreshMetrics>,
	cancellation_metrics: Arc<CancellationMetrics>,
	sql_connection_limiter: Option<SqlConnectionLimiter>,
	resource_metrics: ResourceMetrics,
}

pub struct MetricsRegistryConfig {
	pub descriptors_loaded: u64,
	pub static_workers_configured: u64,
	pub tenant_budgets: TenantBudgetPolicy,
	pub descriptor_sync_metrics: Arc<DescriptorSyncMetrics>,
	pub descriptor_refresh_metrics: Arc<DescriptorRefreshMetrics>,
	pub cancellation_metrics: Arc<CancellationMetrics>,
	pub sql_connection_limiter: Option<SqlConnectionLimiter>,
	pub tenant_admission: TenantAdmissionController,
	pub admission_rejections: TenantAdmissionRejectionCounters,
}

impl MetricsRegistry {
	pub fn new(config: MetricsRegistryConfig) -> Self {
		Self {
			started_at: Instant::now(),
			listener_accepts: ListenerCounters::default(),
			listener_rejects: ListenerRejectCounters::default(),
			metrics_scrapes_total: AtomicU64::new(0),
			descriptors_loaded: AtomicU64::new(config.descriptors_loaded),
			static_workers_configured: AtomicU64::new(config.static_workers_configured),
			descriptor_sync_metrics: config.descriptor_sync_metrics,
			descriptor_refresh_metrics: config.descriptor_refresh_metrics,
			cancellation_metrics: config.cancellation_metrics,
			sql_connection_limiter: config.sql_connection_limiter,
			resource_metrics: ResourceMetrics {
				tenant_budgets: config.tenant_budgets,
				tenant_admission: config.tenant_admission,
				admission_rejections: config.admission_rejections,
			},
		}
	}

	pub fn increment_accept(&self, listener: ListenerName) {
		self.listener_accepts.increment(listener);
	}

	pub fn increment_reject(&self, listener: ListenerName, reason: RejectReason) {
		self.listener_rejects.increment(listener, reason);
	}

	pub fn increment_metrics_scrapes(&self) {
		self.metrics_scrapes_total.fetch_add(1, Ordering::Relaxed);
	}

	pub fn record_cancellation(&self, trigger: CancellationTrigger, outcome: CancellationOutcome) {
		self.cancellation_metrics.record(trigger, outcome);
	}

	pub fn render_text(&self, snapshot: &ReadinessSnapshot) -> String {
		let ready = u8::from(snapshot.is_ready());
		let mut out = String::new();
		out.push_str("# TYPE daemon_ready gauge\n");
		out.push_str(&format!("daemon_ready {ready}\n"));
		out.push_str("# TYPE daemon_uptime_seconds gauge\n");
		out.push_str(&format!(
			"daemon_uptime_seconds {}\n",
			self.started_at.elapsed().as_secs()
		));
		for listener in ListenerName::ALL {
			out.push_str(&format!(
				"listener_up{{listener=\"{}\"}} 1\n",
				listener.as_str()
			));
		}
		for listener in ListenerName::ALL {
			out.push_str(&format!(
				"listener_accepts_total{{listener=\"{}\"}} {}\n",
				listener.as_str(),
				self.listener_accepts.get(listener)
			));
		}
		for listener in ListenerName::ALL {
			for reason in RejectReason::ALL {
				out.push_str(&format!(
					"listener_rejects_total{{listener=\"{}\",reason=\"{}\"}} {}\n",
					listener.as_str(),
					reason.as_str(),
					self.listener_rejects.get(listener, reason)
				));
			}
		}
		out.push_str(&format!(
			"metrics_scrapes_total {}\n",
			self.metrics_scrapes_total.load(Ordering::Relaxed)
		));
		out.push_str(&format!(
			"descriptors_loaded {}\n",
			self.descriptors_loaded.load(Ordering::Relaxed)
		));
		out.push_str(&format!(
			"static_workers_configured {}\n",
			self.static_workers_configured.load(Ordering::Relaxed)
		));
		self.render_descriptor_sync_metrics(&mut out);
		self.render_descriptor_refresh_metrics(&mut out);
		self.render_cancellation_metrics(&mut out);
		self.render_sql_connection_metrics(&mut out);
		self.resource_metrics.render_text(&mut out);
		out
	}

	fn render_descriptor_sync_metrics(&self, out: &mut String) {
		let snapshot = self.descriptor_sync_metrics.snapshot();
		out.push_str("# TYPE catalog_descriptor_sync_requests_total counter\n");
		for (outcome, count) in [
			("success", snapshot.success),
			("not_found", snapshot.not_found),
			("throttled", snapshot.throttled),
			("error", snapshot.error),
		] {
			out.push_str(&format!(
				"catalog_descriptor_sync_requests_total{{outcome=\"{outcome}\"}} {count}\n"
			));
		}
	}

	fn render_descriptor_refresh_metrics(&self, out: &mut String) {
		let snapshot = self.descriptor_refresh_metrics.snapshot();
		out.push_str("# TYPE catalog_descriptor_refresh_requests_total counter\n");
		for (outcome, count) in [
			("invalidated", snapshot.invalidated),
			("missing", snapshot.missing),
			("replacement", snapshot.replacement),
		] {
			out.push_str(&format!(
				"catalog_descriptor_refresh_requests_total{{outcome=\"{outcome}\"}} {count}\n"
			));
		}
	}

	fn render_cancellation_metrics(&self, out: &mut String) {
		out.push_str("# TYPE query_cancellations_total counter\n");
		for trigger in CancellationTrigger::ALL {
			for outcome in CancellationOutcome::ALL {
				out.push_str(&format!(
					"query_cancellations_total{{trigger=\"{}\",outcome=\"{}\"}} {}\n",
					trigger.as_str(),
					outcome.as_str(),
					self.cancellation_metrics.get(trigger, outcome)
				));
			}
		}
	}

	fn render_sql_connection_metrics(&self, out: &mut String) {
		let Some(limiter) = &self.sql_connection_limiter else {
			return;
		};
		let global = limiter.global_metrics();
		out.push_str("# TYPE sql_connections_active gauge\n");
		out.push_str("# TYPE sql_connection_rejections_total counter\n");
		out.push_str(&format!("sql_connections_active {}\n", global.active));
		out.push_str(&format!(
			"sql_connection_rejections_total {}\n",
			global.rejections
		));
		out.push_str("# TYPE tenant_sql_connections_active gauge\n");
		out.push_str("# TYPE tenant_sql_connection_rejections_total counter\n");
		for slot in limiter.tenant_metric_slots() {
			let label = slot.label;
			out.push_str(&format!(
				"tenant_sql_connections_active{{tenant_id=\"{label}\"}} {}\n",
				slot.active
			));
			out.push_str(&format!(
				"tenant_sql_connection_rejections_total{{tenant_id=\"{label}\"}} {}\n",
				slot.rejections
			));
		}
	}
}

struct ResourceMetrics {
	tenant_budgets: TenantBudgetPolicy,
	tenant_admission: TenantAdmissionController,
	admission_rejections: TenantAdmissionRejectionCounters,
}

impl ResourceMetrics {
	fn render_text(&self, out: &mut String) {
		out.push_str("# TYPE tenant_memory_bytes_used gauge\n");
		out.push_str("# TYPE tenant_memory_bytes_limit gauge\n");
		out.push_str("# TYPE tenant_concurrency_active gauge\n");
		out.push_str("# TYPE tenant_concurrency_limit gauge\n");
		out.push_str("# TYPE tenant_admission_rejections_total counter\n");
		out.push_str("# TYPE tenant_admission_queue_depth gauge\n");
		out.push_str("# TYPE tenant_admission_borrowed_memory_bytes gauge\n");
		out.push_str("# TYPE tenant_admission_borrowed_concurrency gauge\n");
		out.push_str("# TYPE tenant_admission_queue_timeouts_total counter\n");
		out.push_str("# TYPE tenant_admission_queue_full_rejections_total counter\n");

		let slots = self.resource_slots();
		for slot in &slots {
			let label = &slot.label;
			out.push_str(&format!(
				"tenant_memory_bytes_used{{tenant_id=\"{label}\"}} {}\n",
				slot.memory_used.get()
			));
		}
		for slot in &slots {
			let label = &slot.label;
			out.push_str(&format!(
				"tenant_memory_bytes_limit{{tenant_id=\"{label}\"}} {}\n",
				slot.memory_limit.get()
			));
		}
		for slot in &slots {
			let label = &slot.label;
			out.push_str(&format!(
				"tenant_concurrency_active{{tenant_id=\"{label}\"}} {}\n",
				slot.concurrency_active
			));
		}
		for slot in &slots {
			let label = &slot.label;
			out.push_str(&format!(
				"tenant_concurrency_limit{{tenant_id=\"{label}\"}} {}\n",
				slot.concurrency_limit
			));
		}
		for slot in &slots {
			let label = &slot.label;
			for (resource, resource_label) in [
				(AdmissionResource::Memory, "memory"),
				(AdmissionResource::Concurrency, "concurrency"),
				(AdmissionResource::Rate, "rate"),
			] {
				out.push_str(&format!(
					"tenant_admission_rejections_total{{tenant_id=\"{label}\",resource=\"{resource_label}\"}} {}\n",
					self.admission_rejections.get(slot.tenant_id, resource)
				));
			}
		}
		for slot in self.tenant_admission.metric_slots() {
			let label = &slot.label;
			out.push_str(&format!(
				"tenant_admission_queue_depth{{tenant_id=\"{label}\"}} {}\n",
				slot.queue_depth
			));
			out.push_str(&format!(
				"tenant_admission_borrowed_memory_bytes{{tenant_id=\"{label}\"}} {}\n",
				slot.borrowed_memory.get()
			));
			out.push_str(&format!(
				"tenant_admission_borrowed_concurrency{{tenant_id=\"{label}\"}} {}\n",
				slot.borrowed_concurrency
			));
			out.push_str(&format!(
				"tenant_admission_queue_timeouts_total{{tenant_id=\"{label}\"}} {}\n",
				slot.queue_timeouts
			));
			out.push_str(&format!(
				"tenant_admission_queue_full_rejections_total{{tenant_id=\"{label}\"}} {}\n",
				slot.queue_full_rejections
			));
		}
	}

	fn resource_slots(&self) -> Vec<TenantResourceSlot> {
		let memory_used = self.tenant_admission.memory_snapshot();
		let concurrency_active = self.tenant_admission.concurrency_snapshot();
		let mut slots = Vec::new();
		for tenant_id in self.tenant_budgets.configured_tenant_ids() {
			let budget = self.tenant_budgets.budget_for(tenant_id);
			slots.push(TenantResourceSlot {
				label: tenant_id.0.to_string(),
				memory_used: memory_used
					.get(&tenant_id)
					.copied()
					.unwrap_or(ByteCount::ZERO),
				memory_limit: budget.aggregate_memory,
				concurrency_active: u64::from(
					concurrency_active.get(&tenant_id).copied().unwrap_or(0),
				),
				concurrency_limit: u64::from(budget.max_concurrent_queries.get()),
				tenant_id: Some(tenant_id),
			});
		}

		let other_memory = memory_used
			.iter()
			.filter(|(tenant_id, _)| !self.tenant_budgets.is_configured_tenant(**tenant_id))
			.fold(ByteCount::ZERO, |total, (_, used)| {
				total.saturating_add(*used)
			});
		let other_concurrency = concurrency_active
			.iter()
			.filter(|(tenant_id, _)| !self.tenant_budgets.is_configured_tenant(**tenant_id))
			.fold(0_u64, |total, (_, active)| {
				total.saturating_add(u64::from(*active))
			});
		slots.push(TenantResourceSlot {
			label: "other".to_string(),
			memory_used: other_memory,
			memory_limit: self.tenant_budgets.default.aggregate_memory,
			concurrency_active: other_concurrency,
			concurrency_limit: u64::from(self.tenant_budgets.default.max_concurrent_queries.get()),
			tenant_id: None,
		});
		slots
	}
}

struct TenantResourceSlot {
	label: String,
	memory_used: ByteCount,
	memory_limit: ByteCount,
	concurrency_active: u64,
	concurrency_limit: u64,
	tenant_id: Option<TenantId>,
}

#[derive(Default)]
struct ListenerCounters {
	worker: AtomicU64,
	sql: AtomicU64,
	health: AtomicU64,
	ready: AtomicU64,
	metrics: AtomicU64,
}

impl ListenerCounters {
	fn increment(&self, listener: ListenerName) {
		self.counter(listener).fetch_add(1, Ordering::Relaxed);
	}

	fn get(&self, listener: ListenerName) -> u64 {
		self.counter(listener).load(Ordering::Relaxed)
	}

	fn counter(&self, listener: ListenerName) -> &AtomicU64 {
		match listener {
			ListenerName::Worker => &self.worker,
			ListenerName::Sql => &self.sql,
			ListenerName::Health => &self.health,
			ListenerName::Ready => &self.ready,
			ListenerName::Metrics => &self.metrics,
		}
	}
}

#[derive(Default)]
struct ListenerRejectCounters {
	worker: RejectCounters,
	sql: RejectCounters,
	health: RejectCounters,
	ready: RejectCounters,
	metrics: RejectCounters,
}

impl ListenerRejectCounters {
	fn increment(&self, listener: ListenerName, reason: RejectReason) {
		self.counters(listener).increment(reason);
	}

	fn get(&self, listener: ListenerName, reason: RejectReason) -> u64 {
		self.counters(listener).get(reason)
	}

	fn counters(&self, listener: ListenerName) -> &RejectCounters {
		match listener {
			ListenerName::Worker => &self.worker,
			ListenerName::Sql => &self.sql,
			ListenerName::Health => &self.health,
			ListenerName::Ready => &self.ready,
			ListenerName::Metrics => &self.metrics,
		}
	}
}

#[derive(Default)]
struct RejectCounters {
	mtls_handshake_failure: AtomicU64,
	peer_unknown_ca: AtomicU64,
	san_shape_rejected: AtomicU64,
	san_role_rejected: AtomicU64,
	bind_unavailable: AtomicU64,
	request_timeout: AtomicU64,
	connection_limit: AtomicU64,
	request_frame_too_large: AtomicU64,
	statement_too_large: AtomicU64,
}

impl RejectCounters {
	fn increment(&self, reason: RejectReason) {
		self.counter(reason).fetch_add(1, Ordering::Relaxed);
	}

	fn get(&self, reason: RejectReason) -> u64 {
		self.counter(reason).load(Ordering::Relaxed)
	}

	fn counter(&self, reason: RejectReason) -> &AtomicU64 {
		match reason {
			RejectReason::MtlsHandshakeFailure => &self.mtls_handshake_failure,
			RejectReason::PeerUnknownCa => &self.peer_unknown_ca,
			RejectReason::SanShapeRejected => &self.san_shape_rejected,
			RejectReason::SanRoleRejected => &self.san_role_rejected,
			RejectReason::BindUnavailable => &self.bind_unavailable,
			RejectReason::RequestTimeout => &self.request_timeout,
			RejectReason::ConnectionLimit => &self.connection_limit,
			RejectReason::RequestFrameTooLarge => &self.request_frame_too_large,
			RejectReason::StatementTooLarge => &self.statement_too_large,
		}
	}
}

#[derive(Clone, Copy)]
pub enum ListenerName {
	Worker,
	Sql,
	Health,
	Ready,
	Metrics,
}

impl ListenerName {
	const ALL: [Self; 5] = [
		Self::Worker,
		Self::Sql,
		Self::Health,
		Self::Ready,
		Self::Metrics,
	];

	fn as_str(self) -> &'static str {
		match self {
			Self::Worker => "worker",
			Self::Sql => "sql",
			Self::Health => "health",
			Self::Ready => "ready",
			Self::Metrics => "metrics",
		}
	}
}

#[derive(Clone, Copy)]
pub enum RejectReason {
	MtlsHandshakeFailure,
	PeerUnknownCa,
	SanShapeRejected,
	SanRoleRejected,
	BindUnavailable,
	RequestTimeout,
	ConnectionLimit,
	RequestFrameTooLarge,
	StatementTooLarge,
}

impl RejectReason {
	const ALL: [Self; 9] = [
		Self::MtlsHandshakeFailure,
		Self::PeerUnknownCa,
		Self::SanShapeRejected,
		Self::SanRoleRejected,
		Self::BindUnavailable,
		Self::RequestTimeout,
		Self::ConnectionLimit,
		Self::RequestFrameTooLarge,
		Self::StatementTooLarge,
	];

	fn as_str(self) -> &'static str {
		match self {
			Self::MtlsHandshakeFailure => "mtls_handshake_failure",
			Self::PeerUnknownCa => "peer_unknown_ca",
			Self::SanShapeRejected => "san_shape_rejected",
			Self::SanRoleRejected => "san_role_rejected",
			Self::BindUnavailable => "bind_unavailable",
			Self::RequestTimeout => "request_timeout",
			Self::ConnectionLimit => "connection_limit",
			Self::RequestFrameTooLarge => "request_frame_too_large",
			Self::StatementTooLarge => "statement_too_large",
		}
	}
}

pub async fn spawn(
	addresses: ObservabilityListeners,
	acceptor: TlsAcceptor,
	readiness: Arc<ReadinessState>,
	metrics: Arc<MetricsRegistry>,
	shutdown: ObservabilityShutdownReceivers,
) -> std::io::Result<ObservabilityHandles> {
	let health_listener = TcpListener::bind(addresses.health).await?;
	let ready_listener = TcpListener::bind(addresses.ready).await?;
	let metrics_listener = TcpListener::bind(addresses.metrics).await?;
	let health = spawn_health(health_listener, Arc::clone(&metrics), shutdown.health).await?;
	let ready = spawn_ready(
		ready_listener,
		acceptor.clone(),
		Arc::clone(&readiness),
		Arc::clone(&metrics),
		shutdown.ready,
	)
	.await?;
	let metrics_handle = spawn_metrics(
		metrics_listener,
		acceptor,
		readiness,
		metrics,
		shutdown.metrics,
	)
	.await?;
	Ok(ObservabilityHandles {
		health,
		ready,
		metrics: metrics_handle,
	})
}

async fn spawn_health(
	listener: TcpListener,
	metrics: Arc<MetricsRegistry>,
	mut shutdown: oneshot::Receiver<()>,
) -> std::io::Result<ListenerHandle> {
	let local_addr = listener.local_addr()?;
	let task = tokio::spawn(async move {
		loop {
			tokio::select! {
				_ = &mut shutdown => break,
				accepted = listener.accept() => match accepted {
					Ok((stream, _peer)) => {
						metrics.increment_accept(ListenerName::Health);
						let metrics = Arc::clone(&metrics);
						tokio::spawn(async move {
							let _ = handle_health_connection(stream, metrics).await;
						});
					}
					Err(error) => {
						tracing::error!(error = %error, "observability health accept failed");
						break;
					}
				},
			}
		}
	});
	Ok(ListenerHandle { local_addr, task })
}

async fn spawn_ready(
	listener: TcpListener,
	acceptor: TlsAcceptor,
	readiness: Arc<ReadinessState>,
	metrics: Arc<MetricsRegistry>,
	mut shutdown: oneshot::Receiver<()>,
) -> std::io::Result<ListenerHandle> {
	let local_addr = listener.local_addr()?;
	let task = tokio::spawn(async move {
		loop {
			tokio::select! {
				_ = &mut shutdown => break,
				accepted = listener.accept() => match accepted {
					Ok((stream, _peer)) => {
						metrics.increment_accept(ListenerName::Ready);
						let acceptor = acceptor.clone();
						let readiness = Arc::clone(&readiness);
						let metrics = Arc::clone(&metrics);
						tokio::spawn(async move {
							let _ = handle_ready_connection(stream, acceptor, readiness, metrics).await;
						});
					}
					Err(error) => {
						tracing::error!(error = %error, "observability ready accept failed");
						break;
					}
				},
			}
		}
	});
	Ok(ListenerHandle { local_addr, task })
}

async fn spawn_metrics(
	listener: TcpListener,
	acceptor: TlsAcceptor,
	readiness: Arc<ReadinessState>,
	metrics: Arc<MetricsRegistry>,
	mut shutdown: oneshot::Receiver<()>,
) -> std::io::Result<ListenerHandle> {
	let local_addr = listener.local_addr()?;
	let task = tokio::spawn(async move {
		loop {
			tokio::select! {
				_ = &mut shutdown => break,
				accepted = listener.accept() => match accepted {
					Ok((stream, _peer)) => {
						metrics.increment_accept(ListenerName::Metrics);
						let acceptor = acceptor.clone();
						let readiness = Arc::clone(&readiness);
						let metrics = Arc::clone(&metrics);
						tokio::spawn(async move {
							let _ = handle_metrics_connection(stream, acceptor, readiness, metrics).await;
						});
					}
					Err(error) => {
						tracing::error!(error = %error, "observability metrics accept failed");
						break;
					}
				},
			}
		}
	});
	Ok(ListenerHandle { local_addr, task })
}

async fn handle_health_connection(
	mut stream: TcpStream,
	metrics: Arc<MetricsRegistry>,
) -> std::io::Result<()> {
	let Some(req) = request_or_close(&mut stream, ListenerName::Health, &metrics).await? else {
		return Ok(());
	};
	write_route_response(&mut stream, &req, HEALTH_PATH, TEXT_CONTENT_TYPE, "ok\n").await
}

async fn handle_ready_connection(
	stream: TcpStream,
	acceptor: TlsAcceptor,
	readiness: Arc<ReadinessState>,
	metrics: Arc<MetricsRegistry>,
) -> std::io::Result<()> {
	let mut stream = match acceptor.accept(stream).await {
		Ok(stream) => stream,
		Err(error) => {
			metrics.increment_reject(ListenerName::Ready, RejectReason::MtlsHandshakeFailure);
			return Err(std::io::Error::new(
				std::io::ErrorKind::PermissionDenied,
				error,
			));
		}
	};
	let Some(req) = request_or_close(&mut stream, ListenerName::Ready, &metrics).await? else {
		return Ok(());
	};
	let line = crate::network::observability_http::parse_request_line(&req)?;
	match (line.method, line.path) {
		("GET", READY_PATH) => {
			let snapshot = readiness.snapshot().await;
			write_response(
				&mut stream,
				snapshot.http_status(),
				JSON_CONTENT_TYPE,
				&snapshot.to_json(),
			)
			.await
		}
		(method, READY_PATH) if method != "GET" => {
			write_response(
				&mut stream,
				"405 Method Not Allowed",
				TEXT_CONTENT_TYPE,
				"method not allowed\n",
			)
			.await
		}
		_ => {
			write_response(
				&mut stream,
				"404 Not Found",
				TEXT_CONTENT_TYPE,
				"not found\n",
			)
			.await
		}
	}
}

async fn handle_metrics_connection(
	stream: TcpStream,
	acceptor: TlsAcceptor,
	readiness: Arc<ReadinessState>,
	metrics: Arc<MetricsRegistry>,
) -> std::io::Result<()> {
	let mut stream = match acceptor.accept(stream).await {
		Ok(stream) => stream,
		Err(error) => {
			metrics.increment_reject(ListenerName::Metrics, RejectReason::MtlsHandshakeFailure);
			return Err(std::io::Error::new(
				std::io::ErrorKind::PermissionDenied,
				error,
			));
		}
	};
	let Some(req) = request_or_close(&mut stream, ListenerName::Metrics, &metrics).await? else {
		return Ok(());
	};
	let line = crate::network::observability_http::parse_request_line(&req)?;
	match (line.method, line.path) {
		("GET", METRICS_PATH) => {
			let snapshot = readiness.snapshot().await;
			metrics.increment_metrics_scrapes();
			let body = metrics.render_text(&snapshot);
			write_response(&mut stream, "200 OK", METRICS_CONTENT_TYPE, &body).await
		}
		(method, METRICS_PATH) if method != "GET" => {
			write_response(
				&mut stream,
				"405 Method Not Allowed",
				TEXT_CONTENT_TYPE,
				"method not allowed\n",
			)
			.await
		}
		_ => {
			write_response(
				&mut stream,
				"404 Not Found",
				TEXT_CONTENT_TYPE,
				"not found\n",
			)
			.await
		}
	}
}

async fn request_or_close<S: AsyncRead + Unpin>(
	stream: &mut S,
	listener: ListenerName,
	metrics: &MetricsRegistry,
) -> std::io::Result<Option<Vec<u8>>> {
	match timeout(READ_REQUEST_TIMEOUT, read_request(stream)).await {
		Ok(Ok(req)) => Ok(Some(req)),
		Ok(Err(_)) => Ok(None),
		Err(_) => {
			metrics.increment_reject(listener, RejectReason::RequestTimeout);
			Ok(None)
		}
	}
}

async fn write_route_response<S: AsyncRead + AsyncWrite + Unpin>(
	stream: &mut S,
	req: &[u8],
	expected_path: &str,
	content_type: &str,
	body: &str,
) -> std::io::Result<()> {
	let line = crate::network::observability_http::parse_request_line(req)?;
	match (line.method, line.path) {
		("GET", path) if path == expected_path => {
			write_response(stream, "200 OK", content_type, body).await
		}
		(method, path) if path == expected_path && method != "GET" => {
			write_response(
				stream,
				"405 Method Not Allowed",
				TEXT_CONTENT_TYPE,
				"method not allowed\n",
			)
			.await
		}
		_ => write_response(stream, "404 Not Found", TEXT_CONTENT_TYPE, "not found\n").await,
	}
}
