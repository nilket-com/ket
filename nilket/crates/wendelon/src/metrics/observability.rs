use crate::audit::{
	AuditAuthority, AuditEvent, AuditOperation, AuditSchema, AuditSink, LocalSecuritySchema,
	LocalSecurityStage, LocalSecuritySurface,
};
use crate::consensus::raft_storage::RevocationSnapshot;
use crate::consensus::retention::RetentionStatus;
use crate::metrics::global;
use crate::network::tls::{
	ActorPermission, ActorTrustRoots, authorize_actor,
	verified_peer_actor_identity_from_chain_with_trust_roots_and_revocations,
};
use crate::schema::NodeId;
use async_trait::async_trait;
use serde_json::{Map, Value};
use std::io::{Error, ErrorKind};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_rustls::TlsAcceptor;

pub const HEALTH_PATH: &str = "/health";
pub const READY_PATH: &str = "/ready";
pub const METRICS_PATH: &str = "/metrics";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListenerAddresses {
	pub health: String,
	pub ready: String,
	pub metrics: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DrainState {
	None,
	Draining,
	Drained,
}

impl DrainState {
	fn as_json(self) -> &'static str {
		match self {
			Self::None => "none",
			Self::Draining => "draining",
			Self::Drained => "drained",
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadinessSnapshot {
	pub node_id: u32,
	pub epoch: u64,
	pub leader_id: Option<u32>,
	pub cluster_members: Vec<u32>,
	pub drain_state: DrainState,
	pub audit_materialization_lag_entries: u64,
	pub degraded_reason: Option<String>,
	pub fips_mode: bool,
	pub fips_self_test_last_ts: Option<u64>,
	/// Control log retention: configured mode, startup history check and
	/// any latched refusal, reported separately.
	pub control_retention: RetentionStatus,
	/// The timing the control Raft instance was built with.
	pub consensus_timing: crate::consensus::ConsensusTimingStatus,
}

#[async_trait]
pub trait ReadinessReporter: Send + Sync {
	async fn snapshot(&self) -> ReadinessSnapshot;
}

pub struct ListenerHandle {
	pub local_addr: SocketAddr,
	pub task: JoinHandle<()>,
}

pub struct ObservabilityHandles {
	pub health: ListenerHandle,
	pub ready: ListenerHandle,
	pub metrics: ListenerHandle,
}

pub async fn spawn(
	addresses: ListenerAddresses,
	acceptor: TlsAcceptor,
	reporter: Arc<dyn ReadinessReporter>,
	local_node_id: u32,
	cluster_domain: impl Into<String>,
	trust_roots: Arc<ActorTrustRoots>,
	audit_sink: Arc<dyn AuditSink>,
	revocations: Option<watch::Receiver<Arc<RevocationSnapshot>>>,
) -> std::io::Result<ObservabilityHandles> {
	let cluster_domain = cluster_domain.into();
	let health_listener = TcpListener::bind(&addresses.health).await?;
	let ready_listener = TcpListener::bind(&addresses.ready).await?;
	let metrics_listener = TcpListener::bind(&addresses.metrics).await?;
	let health = spawn_health(health_listener).await?;
	let ready = spawn_ready(
		ready_listener,
		acceptor.clone(),
		reporter.clone(),
		local_node_id,
		cluster_domain.clone(),
		trust_roots.clone(),
		audit_sink.clone(),
		revocations.clone(),
	)
	.await?;
	let metrics = spawn_metrics(
		metrics_listener,
		acceptor,
		reporter,
		local_node_id,
		cluster_domain,
		trust_roots,
		audit_sink,
		revocations,
	)
	.await?;
	Ok(ObservabilityHandles {
		health,
		ready,
		metrics,
	})
}

pub async fn spawn_health(listener: TcpListener) -> std::io::Result<ListenerHandle> {
	let local_addr = listener.local_addr()?;
	let task = tokio::spawn(async move {
		loop {
			let Ok((stream, _peer)) = listener.accept().await else {
				return;
			};
			tokio::spawn(async move {
				let _ = handle_health_connection(stream).await;
			});
		}
	});
	Ok(ListenerHandle { local_addr, task })
}

pub async fn spawn_ready(
	listener: TcpListener,
	acceptor: TlsAcceptor,
	reporter: Arc<dyn ReadinessReporter>,
	local_node_id: u32,
	cluster_domain: impl Into<String>,
	trust_roots: Arc<ActorTrustRoots>,
	audit_sink: Arc<dyn AuditSink>,
	revocations: Option<watch::Receiver<Arc<RevocationSnapshot>>>,
) -> std::io::Result<ListenerHandle> {
	let cluster_domain = cluster_domain.into();
	let local_addr = listener.local_addr()?;
	let task = tokio::spawn(async move {
		loop {
			let Ok((stream, _peer)) = listener.accept().await else {
				return;
			};
			let acceptor = acceptor.clone();
			let reporter = reporter.clone();
			let cluster_domain = cluster_domain.clone();
			let trust_roots = trust_roots.clone();
			let audit_sink = audit_sink.clone();
			let revocations = revocations.clone();
			tokio::spawn(async move {
				let _ = handle_ready_connection(
					stream,
					acceptor,
					reporter,
					local_node_id,
					&cluster_domain,
					&trust_roots,
					&*audit_sink,
					revocations.as_ref(),
				)
				.await;
			});
		}
	});
	Ok(ListenerHandle { local_addr, task })
}

pub async fn spawn_metrics(
	listener: TcpListener,
	acceptor: TlsAcceptor,
	reporter: Arc<dyn ReadinessReporter>,
	local_node_id: u32,
	cluster_domain: impl Into<String>,
	trust_roots: Arc<ActorTrustRoots>,
	audit_sink: Arc<dyn AuditSink>,
	revocations: Option<watch::Receiver<Arc<RevocationSnapshot>>>,
) -> std::io::Result<ListenerHandle> {
	let cluster_domain = cluster_domain.into();
	let local_addr = listener.local_addr()?;
	let task = tokio::spawn(async move {
		loop {
			let Ok((stream, _peer)) = listener.accept().await else {
				return;
			};
			let acceptor = acceptor.clone();
			let reporter = reporter.clone();
			let cluster_domain = cluster_domain.clone();
			let trust_roots = trust_roots.clone();
			let audit_sink = audit_sink.clone();
			let revocations = revocations.clone();
			tokio::spawn(async move {
				let _ = handle_metrics_connection(
					stream,
					acceptor,
					reporter,
					local_node_id,
					&cluster_domain,
					&trust_roots,
					&*audit_sink,
					revocations.as_ref(),
				)
				.await;
			});
		}
	});
	Ok(ListenerHandle { local_addr, task })
}

async fn handle_health_connection(mut stream: TcpStream) -> std::io::Result<()> {
	let req = read_request(&mut stream).await?;
	match parse_request_line(&req)? {
		("GET", HEALTH_PATH) => {
			write_response(&mut stream, "200 OK", "text/plain; charset=utf-8", "ok\n").await
		}
		_ => {
			write_response(
				&mut stream,
				"404 Not Found",
				"text/plain; charset=utf-8",
				"not found\n",
			)
			.await
		}
	}
}

async fn handle_ready_connection(
	stream: TcpStream,
	acceptor: TlsAcceptor,
	reporter: Arc<dyn ReadinessReporter>,
	local_node_id: u32,
	cluster_domain: &str,
	trust_roots: &ActorTrustRoots,
	audit_sink: &dyn AuditSink,
	revocations: Option<&watch::Receiver<Arc<RevocationSnapshot>>>,
) -> std::io::Result<()> {
	let (mut stream, actor) = accept_tls(
		stream,
		acceptor,
		cluster_domain,
		trust_roots,
		ActorPermission::ObservabilityReady,
		revocations,
		audit_sink,
		local_node_id,
		READY_PATH,
	)
	.await?;
	let req = read_request(&mut stream).await?;
	match parse_request_line(&req)? {
		("GET", READY_PATH) => {
			let snapshot = reporter.snapshot().await;
			emit_observability_audit(
				audit_sink,
				local_node_id,
				&actor,
				AuditOperation::ObservabilityReady,
				"served /ready snapshot",
			)?;
			write_response(
				&mut stream,
				snapshot.http_status(),
				"application/json; charset=utf-8",
				&snapshot.to_json(),
			)
			.await
		}
		_ => {
			write_response(
				&mut stream,
				"404 Not Found",
				"text/plain; charset=utf-8",
				"not found\n",
			)
			.await
		}
	}
}

async fn handle_metrics_connection(
	stream: TcpStream,
	acceptor: TlsAcceptor,
	reporter: Arc<dyn ReadinessReporter>,
	local_node_id: u32,
	cluster_domain: &str,
	trust_roots: &ActorTrustRoots,
	audit_sink: &dyn AuditSink,
	revocations: Option<&watch::Receiver<Arc<RevocationSnapshot>>>,
) -> std::io::Result<()> {
	let (mut stream, actor) = accept_tls(
		stream,
		acceptor,
		cluster_domain,
		trust_roots,
		ActorPermission::ObservabilityMetrics,
		revocations,
		audit_sink,
		local_node_id,
		METRICS_PATH,
	)
	.await?;
	let req = read_request(&mut stream).await?;
	match parse_request_line(&req)? {
		("GET", METRICS_PATH) => {
			let snapshot = reporter.snapshot().await;
			global().set_raft_observability_state(snapshot.epoch, snapshot.leader_id);
			global().metrics_scrapes_total.inc();
			let body = global().render_prometheus();
			emit_observability_audit(
				audit_sink,
				local_node_id,
				&actor,
				AuditOperation::ObservabilityMetrics,
				"served /metrics scrape",
			)?;
			write_response(
				&mut stream,
				"200 OK",
				"text/plain; version=0.0.4; charset=utf-8",
				&body,
			)
			.await
		}
		_ => {
			write_response(
				&mut stream,
				"404 Not Found",
				"text/plain; charset=utf-8",
				"not found\n",
			)
			.await
		}
	}
}

async fn accept_tls(
	stream: TcpStream,
	acceptor: TlsAcceptor,
	cluster_domain: &str,
	trust_roots: &ActorTrustRoots,
	permission: ActorPermission,
	revocations: Option<&watch::Receiver<Arc<RevocationSnapshot>>>,
	audit_sink: &dyn AuditSink,
	local_node_id: u32,
	path: &'static str,
) -> std::io::Result<(
	tokio_rustls::server::TlsStream<TcpStream>,
	crate::network::tls::ActorIdentity,
)> {
	let stream = acceptor.accept(stream).await.map_err(|e| {
		emit_local_security_failure_audit(
			audit_sink,
			local_node_id,
			path,
			permission,
			LocalSecurityStage::TlsHandshake,
			e.to_string(),
			None,
		);
		Error::new(ErrorKind::PermissionDenied, e)
	})?;
	let certs = stream.get_ref().1.peer_certificates().ok_or_else(|| {
		emit_local_security_failure_audit(
			audit_sink,
			local_node_id,
			path,
			permission,
			LocalSecurityStage::PeerIdentity,
			"peer presented no TLS identity".to_string(),
			None,
		);
		Error::new(
			ErrorKind::PermissionDenied,
			"peer presented no TLS identity",
		)
	})?;
	let actor = verified_peer_actor_identity_from_chain_with_trust_roots_and_revocations(
		certs,
		cluster_domain,
		Some(trust_roots),
		revocations.map(|rx| rx.borrow().clone()).as_deref(),
	)
	.map_err(|err| {
		emit_local_security_failure_audit(
			audit_sink,
			local_node_id,
			path,
			permission,
			LocalSecurityStage::PeerIdentity,
			err.to_string(),
			None,
		);
		err
	})?;
	authorize_actor(&actor, permission).map_err(|err| {
		emit_local_security_failure_audit(
			audit_sink,
			local_node_id,
			path,
			permission,
			LocalSecurityStage::Authorization,
			err.to_string(),
			Some(&actor),
		);
		err
	})?;
	Ok((stream, actor))
}

fn emit_observability_audit(
	sink: &dyn AuditSink,
	node_id: u32,
	actor: &crate::network::tls::ActorIdentity,
	operation: AuditOperation,
	message: &str,
) -> std::io::Result<()> {
	sink.emit(
		AuditEvent::new(
			AuditAuthority::NodeLocal,
			operation,
			NodeId {
				id: node_id,
				_reserved: 0,
			},
			message,
		)
		.with_actor(actor),
	)
}

fn emit_local_security_failure_audit(
	sink: &dyn AuditSink,
	node_id: u32,
	path: &'static str,
	permission: ActorPermission,
	stage: LocalSecurityStage,
	failure_reason: String,
	actor: Option<&crate::network::tls::ActorIdentity>,
) {
	let operation = match stage {
		LocalSecurityStage::Authorization => AuditOperation::IdentityAuthorizationDenied,
		LocalSecurityStage::TlsHandshake | LocalSecurityStage::PeerIdentity => {
			AuditOperation::IdentityHandshakeDenied
		}
	};
	let mut event = AuditEvent::new(
		AuditAuthority::NodeLocal,
		operation,
		NodeId {
			id: node_id,
			_reserved: 0,
		},
		format!(
			"denied observability access for {} at {}: {}",
			permission.as_str(),
			path,
			failure_reason
		),
	)
	.with_outcome(crate::audit::AuditOutcome::Denied)
	.with_schema(AuditSchema::LocalSecurity(LocalSecuritySchema {
		surface: LocalSecuritySurface::Observability,
		stage,
		path: Some(path.to_string()),
		required_permission: Some(permission.as_str().to_string()),
		failure_reason: Some(failure_reason),
	}));
	if let Some(actor) = actor {
		event = event
			.with_actor(actor)
			.with_outcome(crate::audit::AuditOutcome::Denied);
	}
	let _ = sink.emit(event);
}

async fn read_request<S: AsyncRead + Unpin>(stream: &mut S) -> std::io::Result<String> {
	// Intentionally tiny probe parser: one read, no header folding,
	// no request-body support, and not a general RFC-compliant HTTP parser.
	let mut buf = [0u8; 4096];
	let n = stream.read(&mut buf).await?;
	if n == 0 {
		return Err(Error::new(ErrorKind::UnexpectedEof, "empty HTTP request"));
	}
	std::str::from_utf8(&buf[..n])
		.map(|s| s.to_string())
		.map_err(|err| {
			Error::new(
				ErrorKind::InvalidData,
				format!("invalid HTTP request: {err}"),
			)
		})
}

fn parse_request_line(req: &str) -> std::io::Result<(&str, &str)> {
	let first_line = req.lines().next().unwrap_or_default();
	let mut parts = first_line.split_whitespace();
	let method = parts.next().unwrap_or_default();
	let path = parts.next().unwrap_or_default();
	if method.is_empty() || path.is_empty() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"malformed HTTP request line",
		));
	}
	Ok((method, path))
}

async fn write_response<S: AsyncWrite + Unpin>(
	stream: &mut S,
	status: &str,
	content_type: &str,
	body: &str,
) -> std::io::Result<()> {
	let resp = format!(
		"HTTP/1.1 {status}\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
		body.len()
	);
	stream.write_all(resp.as_bytes()).await?;
	stream.flush().await
}

impl ReadinessSnapshot {
	pub fn is_ready(&self) -> bool {
		self.degraded_reason.is_none()
			&& self.audit_materialization_lag_entries == 0
			&& self.control_retention.violation.is_none()
	}

	pub fn http_status(&self) -> &'static str {
		if self.is_ready() {
			"200 OK"
		} else {
			"503 Service Unavailable"
		}
	}

	pub fn to_json(&self) -> String {
		let mut cluster_members = self.cluster_members.clone();
		cluster_members.sort_unstable();
		let mut body = Map::new();
		body.insert(
			"node_id".into(),
			Value::String(display_node_id(self.node_id)),
		);
		body.insert("epoch".into(), Value::from(self.epoch));
		body.insert(
			"leader_id".into(),
			self.leader_id
				.map(display_node_id)
				.map(Value::String)
				.unwrap_or(Value::Null),
		);
		body.insert(
			"cluster_members".into(),
			Value::Array(
				cluster_members
					.into_iter()
					.map(display_node_id)
					.map(Value::String)
					.collect(),
			),
		);
		body.insert(
			"drain_state".into(),
			Value::String(self.drain_state.as_json().to_string()),
		);
		body.insert(
			"audit_materialization_lag_entries".into(),
			Value::from(self.audit_materialization_lag_entries),
		);
		body.insert(
			"degraded_reason".into(),
			self.degraded_reason
				.as_ref()
				.cloned()
				.map(Value::String)
				.unwrap_or(Value::Null),
		);
		body.insert("control_retention".into(), self.control_retention.to_json());
		body.insert("consensus_timing".into(), self.consensus_timing.to_json());
		#[cfg(feature = "fips")]
		{
			body.insert("fips_mode".into(), Value::Bool(self.fips_mode));
			body.insert(
				"fips_self_test_last_ts".into(),
				self.fips_self_test_last_ts
					.map(Value::from)
					.unwrap_or(Value::Null),
			);
		}
		Value::Object(body).to_string()
	}
}

fn display_node_id(node_id: u32) -> String {
	format!("n-{node_id:02}")
}
