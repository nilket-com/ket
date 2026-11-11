//! HRFT dispatch server, transport-agnostic above the listener.
//!
//! The supported day-one transport is QUIC. Data Ring, Control
//! Ring, and Raft internal RPCs are multiplexed on message-type
//! discriminants inside the frame header rather than separated by
//! port. The accept loop in [`serve`] is the QUIC-specific piece
//! in this file; every other function operates on the
//! [`TransportConnection`] / [`TransportStream`] seam kept by
//! ADR 011 so compatibility paths or future transports can be
//! added without rewriting the dispatch layer.
//!
//! ## Request lifecycle
//!
//! 1. [`serve`] loops on `endpoint.accept()`. Each accepted
//!    `quinn::Incoming` is handed to [`tokio::spawn`] and the loop
//!    returns to `accept()` immediately.
//! 2. [`serve_quic_connection`] awaits the QUIC handshake, wraps
//!    the resulting `quinn::Connection` in a
//!    [`QuicTransportConnection`], and hands it to
//!    [`handle_connection`] as `Arc<dyn TransportConnection>`.
//! 3. [`handle_connection`] drains `accept_bi()` calls. Most bidi
//!    streams are one logical RPC: client sends one [`HrftFrame`],
//!    server replies with zero or one frame. Bulk client-streaming
//!    RPCs override this via [`HrftService::handle_stream`] and
//!    exchange multiple frames on the same stream.
//! 4. Unary streams are parsed with [`read_frame`] and routed
//!    through [`HrftService::handle_frame`]. If the service
//!    returns a response frame, [`write_frame`] sends it, then
//!    the stream's `finish` cleanly signals end-of-response.
//!
//! ## Why one frame per stream by default
//!
//! QUIC bidi streams are cheap; a new stream per RPC gives free
//! per-request isolation (one slow handler does not block others)
//! and avoids needing an application-level correlation id. Bulk
//! streams that carry many frames (for example
//! `BULK_WRITE_REQ`) opt into a dedicated handler without forcing
//! the rest of the protocol off the simpler unary path.
//!
//! ## Per-stream peer identity
//!
//! The server extracts the real peer id from the validated client
//! certificate's canonical DNS SAN before constructing the
//! `QuicTransportConnection`. Peer-aware routing can therefore rely
//! on [`TransportConnection::remote_id`] without a second trust
//! lookup.
//!
//! ## What is not here yet
//!
//! * Unidirectional stream handling (needed for
//!   [`crate::schema::MsgType::STREAM_FIN_EVT`] push).
//! * 0-RTT or datagram paths.
//! * Backpressure / per-tenant quotas.

use crate::consensus::raft_storage::RevocationSnapshot;
use crate::metrics::global as metrics;
use crate::network::core::{HrftFrame, read_frame, write_frame};
use crate::network::pool::BufPool;
use crate::network::quic_transport::QuicTransportConnection;
use crate::network::tcp_tls_transport::InboundTcpTlsConnection;
use crate::network::tls::{ActorIdentity, ActorPermission, ActorTrustRoots, authorize_actor};
use crate::network::tls::{
	verified_peer_actor_identity_from_chain_with_trust_roots_and_revocations,
	verified_peer_actor_identity_with_trust_roots_and_revocations,
	verified_transport_peer_from_chain_with_revocations, verified_transport_peer_with_revocations,
};
use crate::network::transport::{TransportConnection, TransportStream};
use crate::schema::NodeId;
use async_trait::async_trait;
use quinn::Endpoint;
use std::collections::HashMap;
use std::io::{Error, ErrorKind};
use std::net::IpAddr;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};
use tokio_rustls::TlsAcceptor;

// ============================================================
//  LIFECYCLE
// ============================================================

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequestContext {
	pub remote: Option<NodeId>,
	pub actor: ActorIdentity,
}

impl RequestContext {
	/// Return the authenticated node identity for node-only ingress.
	///
	/// A SAN alone is not enough: the actor must also carry the `DataNode`
	/// role, and both independently-derived identities must agree.
	pub fn require_authenticated_node(&self, operation: &str) -> std::io::Result<NodeId> {
		authorize_actor(&self.actor, ActorPermission::InterNodeTransport).map_err(|error| {
			Error::new(
				ErrorKind::PermissionDenied,
				format!("{operation} requires an authenticated data node: {error}"),
			)
		})?;
		let remote = self.remote.ok_or_else(|| {
			Error::new(
				ErrorKind::PermissionDenied,
				format!("{operation} requires an authenticated node identity"),
			)
		})?;
		if self.actor.node_id != Some(remote) {
			return Err(Error::new(
				ErrorKind::PermissionDenied,
				format!("{operation} node identity disagrees with the authenticated actor"),
			));
		}
		Ok(remote)
	}
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct LifecycleState {
	draining: bool,
	in_flight: usize,
}

struct LifecycleInner {
	state: Mutex<LifecycleState>,
	tx: watch::Sender<LifecycleState>,
}

/// Shared request-lifecycle gate for a published HRFT surface.
///
/// `begin_drain()` flips the surface into a "no new work" mode.
/// Existing requests keep running until their guards drop; callers
/// awaiting [`wait_for_quiescence`](Self::wait_for_quiescence)
/// observe completion only after every in-flight guard has drained.
#[derive(Clone)]
pub struct ServerLifecycle {
	inner: Arc<LifecycleInner>,
}

impl Default for ServerLifecycle {
	fn default() -> Self {
		Self::new()
	}
}

impl ServerLifecycle {
	pub fn new() -> Self {
		let initial = LifecycleState::default();
		let (tx, _rx) = watch::channel(initial);
		Self {
			inner: Arc::new(LifecycleInner {
				state: Mutex::new(initial),
				tx,
			}),
		}
	}

	pub fn begin_drain(&self) {
		self.update_state(|state| state.draining = true);
	}

	pub fn is_draining(&self) -> bool {
		self.snapshot().draining
	}

	pub fn in_flight(&self) -> usize {
		self.snapshot().in_flight
	}

	fn subscribe(&self) -> watch::Receiver<LifecycleState> {
		self.inner.tx.subscribe()
	}

	fn lock_state(&self) -> std::sync::MutexGuard<'_, LifecycleState> {
		match self.inner.state.lock() {
			Ok(guard) => guard,
			Err(poisoned) => {
				tracing::error!("recovering poisoned server lifecycle state");
				poisoned.into_inner()
			}
		}
	}

	pub async fn wait_for_quiescence(&self) {
		let mut rx = self.subscribe();
		loop {
			let state = *rx.borrow();
			if state.in_flight == 0 {
				return;
			}
			if rx.changed().await.is_err() {
				return;
			}
		}
	}

	pub(crate) fn track_work(&self) -> LifecycleGuard {
		self.update_state(|state| state.in_flight += 1);
		LifecycleGuard {
			lifecycle: self.clone(),
		}
	}

	fn try_track_request(&self) -> std::io::Result<LifecycleGuard> {
		let mut guard = self.lock_state();
		if guard.draining {
			return Err(draining_error());
		}
		guard.in_flight += 1;
		let snapshot = *guard;
		drop(guard);
		let _ = self.inner.tx.send_replace(snapshot);
		Ok(LifecycleGuard {
			lifecycle: self.clone(),
		})
	}

	fn finish_work(&self) {
		self.update_state(|state| {
			state.in_flight = state.in_flight.saturating_sub(1);
		});
	}

	fn snapshot(&self) -> LifecycleState {
		*self.lock_state()
	}

	fn update_state(&self, mutator: impl FnOnce(&mut LifecycleState)) {
		let mut guard = self.lock_state();
		mutator(&mut guard);
		let snapshot = *guard;
		drop(guard);
		let _ = self.inner.tx.send_replace(snapshot);
	}
}

pub(crate) struct LifecycleGuard {
	lifecycle: ServerLifecycle,
}

impl Drop for LifecycleGuard {
	fn drop(&mut self) {
		self.lifecycle.finish_work();
	}
}

fn draining_error() -> std::io::Error {
	Error::new(
		ErrorKind::ConnectionAborted,
		"server is draining; refusing new work",
	)
}

// ============================================================
//  ADMISSION
// ============================================================

const DEFAULT_MAX_INBOUND_CONNECTIONS: usize = 1024;
const DEFAULT_MAX_INBOUND_CONNECTIONS_PER_IP: usize = 64;
const DEFAULT_MAX_INBOUND_STREAMS_PER_CONNECTION: usize = 64;
const DEFAULT_MAX_CONCURRENT_STREAM_TASKS: usize = 4096;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServerAdmissionConfig {
	pub max_inbound_connections: usize,
	pub max_inbound_connections_per_ip: usize,
	pub max_inbound_streams_per_connection: usize,
	pub max_concurrent_stream_tasks: usize,
}

impl Default for ServerAdmissionConfig {
	fn default() -> Self {
		Self {
			max_inbound_connections: DEFAULT_MAX_INBOUND_CONNECTIONS,
			max_inbound_connections_per_ip: DEFAULT_MAX_INBOUND_CONNECTIONS_PER_IP,
			max_inbound_streams_per_connection: DEFAULT_MAX_INBOUND_STREAMS_PER_CONNECTION,
			max_concurrent_stream_tasks: DEFAULT_MAX_CONCURRENT_STREAM_TASKS,
		}
	}
}

impl ServerAdmissionConfig {
	pub fn validate(&self) -> std::io::Result<()> {
		if self.max_inbound_connections == 0 {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"transport_admission.max_inbound_connections must be greater than zero",
			));
		}
		if self.max_inbound_connections_per_ip == 0 {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"transport_admission.max_inbound_connections_per_ip must be greater than zero",
			));
		}
		if self.max_inbound_streams_per_connection == 0 {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"transport_admission.max_inbound_streams_per_connection must be greater than zero",
			));
		}
		if self.max_concurrent_stream_tasks == 0 {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"transport_admission.max_concurrent_stream_tasks must be greater than zero",
			));
		}
		Ok(())
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AdmissionRejectionReason {
	ConnectionLimit,
	PerIpConnectionLimit,
	PerConnectionStreamLimit,
	GlobalStreamTaskLimit,
}

#[derive(Clone)]
struct AdmissionController {
	config: ServerAdmissionConfig,
	connection_slots: Arc<Semaphore>,
	per_ip_counts: Arc<Mutex<HashMap<IpAddr, usize>>>,
	stream_task_slots: Arc<Semaphore>,
}

impl AdmissionController {
	fn new(config: ServerAdmissionConfig) -> Self {
		let controller = Self {
			connection_slots: Arc::new(Semaphore::new(config.max_inbound_connections)),
			per_ip_counts: Arc::new(Mutex::new(HashMap::new())),
			stream_task_slots: Arc::new(Semaphore::new(config.max_concurrent_stream_tasks)),
			config,
		};
		controller.refresh_metrics();
		controller
	}

	fn try_acquire_connection_for_peer(
		&self,
		peer_ip: Option<IpAddr>,
	) -> Result<ConnectionAdmissionGuard, AdmissionRejectionReason> {
		let permit = self
			.connection_slots
			.clone()
			.try_acquire_owned()
			.map_err(|_| AdmissionRejectionReason::ConnectionLimit)?;
		if let Some(peer_ip) = peer_ip {
			let mut counts = self.per_ip_counts.lock().unwrap();
			let current = counts.get(&peer_ip).copied().unwrap_or_default();
			if current >= self.config.max_inbound_connections_per_ip {
				drop(counts);
				drop(permit);
				return Err(AdmissionRejectionReason::PerIpConnectionLimit);
			}
			counts.insert(peer_ip, current.saturating_add(1));
		}
		let guard = ConnectionAdmissionGuard {
			controller: self.clone(),
			_permit: permit,
			peer_ip,
			stream_slots: Arc::new(Semaphore::new(
				self.config.max_inbound_streams_per_connection,
			)),
		};
		self.refresh_metrics();
		Ok(guard)
	}

	fn try_acquire_stream(
		&self,
		connection: &ConnectionAdmissionGuard,
	) -> Result<StreamAdmissionGuard, AdmissionRejectionReason> {
		let connection_stream_permit = connection
			.stream_slots
			.clone()
			.try_acquire_owned()
			.map_err(|_| AdmissionRejectionReason::PerConnectionStreamLimit)?;
		let task_permit = self
			.stream_task_slots
			.clone()
			.try_acquire_owned()
			.map_err(|_| AdmissionRejectionReason::GlobalStreamTaskLimit)?;
		let guard = StreamAdmissionGuard {
			controller: self.clone(),
			_connection_stream_permit: connection_stream_permit,
			_task_permit: task_permit,
		};
		self.refresh_metrics();
		Ok(guard)
	}

	fn record_rejection(&self, reason: AdmissionRejectionReason) {
		match reason {
			AdmissionRejectionReason::ConnectionLimit => {
				metrics()
					.transport_admission_rejections_connection_limit_total
					.inc();
			}
			AdmissionRejectionReason::PerIpConnectionLimit => {
				metrics()
					.transport_admission_rejections_connection_per_ip_limit_total
					.inc();
			}
			AdmissionRejectionReason::PerConnectionStreamLimit => {
				metrics()
					.transport_admission_rejections_stream_limit_total
					.inc();
			}
			AdmissionRejectionReason::GlobalStreamTaskLimit => {
				metrics()
					.transport_admission_rejections_task_limit_total
					.inc();
			}
		}
	}

	fn refresh_metrics(&self) {
		metrics().transport_active_connections.set(
			self.config
				.max_inbound_connections
				.saturating_sub(self.connection_slots.available_permits()) as u64,
		);
		metrics().transport_active_stream_handlers.set(
			self.config
				.max_concurrent_stream_tasks
				.saturating_sub(self.stream_task_slots.available_permits()) as u64,
		);
	}
}

struct ConnectionAdmissionGuard {
	controller: AdmissionController,
	_permit: OwnedSemaphorePermit,
	peer_ip: Option<IpAddr>,
	stream_slots: Arc<Semaphore>,
}

impl Drop for ConnectionAdmissionGuard {
	fn drop(&mut self) {
		if let Some(peer_ip) = self.peer_ip {
			let mut counts = self.controller.per_ip_counts.lock().unwrap();
			if let Some(count) = counts.get_mut(&peer_ip) {
				*count = count.saturating_sub(1);
				if *count == 0 {
					counts.remove(&peer_ip);
				}
			}
		}
		self.controller.refresh_metrics();
	}
}

struct StreamAdmissionGuard {
	controller: AdmissionController,
	_connection_stream_permit: OwnedSemaphorePermit,
	_task_permit: OwnedSemaphorePermit,
}

impl Drop for StreamAdmissionGuard {
	fn drop(&mut self) {
		self.controller.refresh_metrics();
	}
}

/// Drain-aware wrapper around any [`HrftService`].
///
/// This is the node-facing gate: published services and loopback
/// harnesses can reject new requests through the same lifecycle
/// object even when they do not pass through the QUIC listener.
pub struct LifecycleService {
	inner: Arc<dyn HrftService>,
	lifecycle: ServerLifecycle,
}

impl LifecycleService {
	pub fn new(inner: Arc<dyn HrftService>, lifecycle: ServerLifecycle) -> Self {
		Self { inner, lifecycle }
	}
}

#[async_trait]
impl HrftService for LifecycleService {
	async fn handle_frame(
		&self,
		context: RequestContext,
		frame: HrftFrame,
	) -> std::io::Result<Option<HrftFrame>> {
		let _guard = self.lifecycle.try_track_request()?;
		self.inner.handle_frame(context, frame).await
	}

	async fn handle_stream(
		&self,
		stream: Box<dyn TransportStream>,
		pool: Arc<BufPool>,
		context: RequestContext,
	) -> std::io::Result<()> {
		let _guard = self.lifecycle.try_track_request()?;
		self.inner.handle_stream(stream, pool, context).await
	}
}

// ============================================================
//  SERVICE TRAIT
// ============================================================

/// One-frame-in / at-most-one-frame-out RPC handler.
///
/// Implementors route on `frame.header.msg_type` to the right
/// business logic (Data Ring write, Control Ring lease, Raft
/// AppendEntries, etc.) and return the encoded response frame.
///
/// Returning `Ok(None)` means the request is fire-and-forget; the
/// server will close the bidi stream without sending bytes back.
/// Returning `Err` causes the connection task to log and drop the
/// stream — it does not tear down the whole connection.
#[async_trait]
pub trait HrftService: Send + Sync + 'static {
	async fn handle_frame(
		&self,
		context: RequestContext,
		frame: HrftFrame,
	) -> std::io::Result<Option<HrftFrame>>;

	/// Per-stream entry point. The default implementation keeps the
	/// unary "one frame in, at most one frame out" behavior; routers
	/// can override it for client-streaming RPCs such as bulk write.
	async fn handle_stream(
		&self,
		mut stream: Box<dyn TransportStream>,
		pool: Arc<BufPool>,
		context: RequestContext,
	) -> std::io::Result<()> {
		let frame = read_frame(&mut *stream, &pool).await?;

		match self.handle_frame(context, frame).await? {
			Some(resp) => {
				write_frame(
					&mut *stream,
					&resp.header,
					&resp.meta,
					resp.arrow_payload.as_deref(),
					resp.trailing.as_deref(),
				)
				.await?;
				stream.finish().await?;
			}
			None => {
				stream.finish().await?;
			}
		}

		Ok(())
	}
}

// ============================================================
//  SERVER ENTRY POINT
// ============================================================

/// Accept loop. Runs until the endpoint is closed.
///
/// This function never returns `Ok(())` in normal operation; it
/// either runs forever or returns the I/O error that caused the
/// accept loop to fail. Spawn it as a top-level task.
///
/// The `pool` is shared across every stream accepted on `endpoint`;
/// all HRFT frame payloads (meta / arrow / trailing) and any
/// postcard-encoded response bodies recycle through it.
///
/// QUIC is currently the only listener; an RDMA listener (when it
/// arrives) will get its own `serve_rdma` entry point that wraps
/// each accepted queue pair into a `RdmaTransportConnection` and
/// then delegates to [`handle_connection`] just like this one does.
pub async fn serve<S: HrftService>(
	endpoint: Endpoint,
	service: Arc<S>,
	pool: Arc<BufPool>,
	lifecycle: ServerLifecycle,
	cluster_domain: impl Into<String>,
) -> std::io::Result<()> {
	serve_with_admission(
		endpoint,
		service,
		pool,
		lifecycle,
		ServerAdmissionConfig::default(),
		cluster_domain,
		None,
		None,
	)
	.await
}

pub async fn serve_with_admission<S: HrftService>(
	endpoint: Endpoint,
	service: Arc<S>,
	pool: Arc<BufPool>,
	lifecycle: ServerLifecycle,
	admission: ServerAdmissionConfig,
	cluster_domain: impl Into<String>,
	revocations: Option<watch::Receiver<Arc<RevocationSnapshot>>>,
	trust_roots: Option<Arc<ActorTrustRoots>>,
) -> std::io::Result<()> {
	admission.validate()?;
	let cluster_domain = cluster_domain.into();
	let admission = AdmissionController::new(admission);
	let mut drain_rx = lifecycle.subscribe();
	loop {
		if drain_rx.borrow().draining {
			break;
		}
		tokio::select! {
			changed = drain_rx.changed() => {
				if changed.is_err() || drain_rx.borrow().draining {
					break;
				}
			}
			incoming = endpoint.accept() => {
				let Some(incoming) = incoming else {
					break;
				};
				let connection_admission = match admission.try_acquire_connection_for_peer(Some(incoming.remote_address().ip())) {
					Ok(guard) => guard,
					Err(reason) => {
						admission.record_rejection(reason);
						incoming.refuse();
						continue;
					}
				};
				let service = service.clone();
				let pool = pool.clone();
				let lifecycle = lifecycle.clone();
				let admission = admission.clone();
				let cluster_domain = cluster_domain.clone();
				let revocations = revocations.clone();
				let trust_roots = trust_roots.clone();
				let _connection_guard = lifecycle.track_work();
				tokio::spawn(async move {
					let _connection_guard = _connection_guard;
					let _connection_admission = connection_admission;
					if let Err(e) =
						serve_quic_connection(
							incoming,
							service,
							pool,
							lifecycle,
							admission,
							_connection_admission,
							cluster_domain,
							revocations,
							trust_roots,
						)
							.await
					{
						// One bad connection should not kill the server.
						// A structured logger will go here once the tracing
						// macros are threaded through.
						let _ = e;
					}
				});
			}
		}
	}
	lifecycle.wait_for_quiescence().await;
	Ok(())
}

pub async fn serve_tcp_tls<S: HrftService>(
	listener: TcpListener,
	acceptor: TlsAcceptor,
	service: Arc<S>,
	pool: Arc<BufPool>,
	lifecycle: ServerLifecycle,
	cluster_domain: impl Into<String>,
) -> std::io::Result<()> {
	serve_tcp_tls_with_admission(
		listener,
		acceptor,
		service,
		pool,
		lifecycle,
		ServerAdmissionConfig::default(),
		cluster_domain,
		None,
		None,
	)
	.await
}

pub async fn serve_tcp_tls_with_admission<S: HrftService>(
	listener: TcpListener,
	acceptor: TlsAcceptor,
	service: Arc<S>,
	pool: Arc<BufPool>,
	lifecycle: ServerLifecycle,
	admission: ServerAdmissionConfig,
	cluster_domain: impl Into<String>,
	revocations: Option<watch::Receiver<Arc<RevocationSnapshot>>>,
	trust_roots: Option<Arc<ActorTrustRoots>>,
) -> std::io::Result<()> {
	admission.validate()?;
	let cluster_domain = cluster_domain.into();
	let admission = AdmissionController::new(admission);
	let mut drain_rx = lifecycle.subscribe();
	loop {
		if drain_rx.borrow().draining {
			break;
		}
		tokio::select! {
			changed = drain_rx.changed() => {
				if changed.is_err() || drain_rx.borrow().draining {
					break;
				}
			}
			accepted = listener.accept() => {
				let Ok((stream, peer)) = accepted else {
					break;
				};
				let connection_admission = match admission.try_acquire_connection_for_peer(Some(peer.ip())) {
					Ok(guard) => guard,
					Err(reason) => {
						admission.record_rejection(reason);
						drop(stream);
						continue;
					}
				};
				let acceptor = acceptor.clone();
				let service = service.clone();
				let pool = pool.clone();
				let lifecycle = lifecycle.clone();
				let admission = admission.clone();
				let cluster_domain = cluster_domain.clone();
				let revocations = revocations.clone();
				let trust_roots = trust_roots.clone();
				let _connection_guard = lifecycle.track_work();
				tokio::spawn(async move {
					let _connection_guard = _connection_guard;
					let _connection_admission = connection_admission;
					if let Err(e) = serve_tcp_tls_connection(
						stream,
						acceptor,
						service,
						pool,
						lifecycle,
						admission,
						_connection_admission,
						cluster_domain,
						revocations,
						trust_roots,
					)
					.await
					{
						let _ = e;
					}
				});
			}
		}
	}
	lifecycle.wait_for_quiescence().await;
	Ok(())
}

// ============================================================
//  PER-CONNECTION TASKS
// ============================================================

/// Complete the QUIC handshake on `incoming`, wrap the resulting
/// connection in a [`QuicTransportConnection`], and hand it off
/// to the trait-typed [`handle_connection`].
///
/// This is the only function in this file that names quinn types;
/// when other transports land they each get their own
/// `serve_<tier>_connection` shim with the same shape.
async fn serve_quic_connection<S: HrftService>(
	incoming: quinn::Incoming,
	service: Arc<S>,
	pool: Arc<BufPool>,
	lifecycle: ServerLifecycle,
	admission: AdmissionController,
	connection_admission: ConnectionAdmissionGuard,
	cluster_domain: String,
	revocations: Option<watch::Receiver<Arc<RevocationSnapshot>>>,
	trust_roots: Option<Arc<ActorTrustRoots>>,
) -> std::io::Result<()> {
	let connection = incoming.await.map_err(|e| {
		metrics().quic_handshake_failures_transport_total.inc();
		Error::new(ErrorKind::ConnectionAborted, e)
	})?;
	let revocation_snapshot = revocations.as_ref().map(|rx| rx.borrow().clone());
	let actor = if let Some(trust_roots) = trust_roots.as_deref() {
		verified_peer_actor_identity_with_trust_roots_and_revocations(
			&connection,
			&cluster_domain,
			Some(trust_roots),
			revocation_snapshot.as_deref(),
		)
	} else {
		verified_transport_peer_with_revocations(
			&connection,
			&cluster_domain,
			revocation_snapshot.as_deref(),
		)
		.map(|peer| peer.actor_identity)
	}
	.map_err(|err| {
		match err.kind() {
			ErrorKind::PermissionDenied => {
				metrics().quic_handshake_failures_authorization_total.inc()
			}
			_ => metrics().quic_handshake_failures_identity_total.inc(),
		}
		err
	})?;
	let connection: Arc<dyn TransportConnection> =
		Arc::new(QuicTransportConnection::new_inbound(connection, actor));
	handle_connection(
		connection,
		service,
		pool,
		lifecycle,
		admission,
		connection_admission,
	)
	.await
}

async fn serve_tcp_tls_connection<S: HrftService>(
	stream: TcpStream,
	acceptor: TlsAcceptor,
	service: Arc<S>,
	pool: Arc<BufPool>,
	lifecycle: ServerLifecycle,
	admission: AdmissionController,
	connection_admission: ConnectionAdmissionGuard,
	cluster_domain: String,
	revocations: Option<watch::Receiver<Arc<RevocationSnapshot>>>,
	trust_roots: Option<Arc<ActorTrustRoots>>,
) -> std::io::Result<()> {
	let stream = acceptor
		.accept(stream)
		.await
		.map_err(|e| Error::new(ErrorKind::ConnectionAborted, e))?;
	let certs = stream.get_ref().1.peer_certificates().ok_or_else(|| {
		Error::new(
			ErrorKind::PermissionDenied,
			"peer presented no TLS identity",
		)
	})?;
	let revocation_snapshot = revocations.as_ref().map(|rx| rx.borrow().clone());
	let actor = if let Some(trust_roots) = trust_roots.as_deref() {
		verified_peer_actor_identity_from_chain_with_trust_roots_and_revocations(
			certs,
			&cluster_domain,
			Some(trust_roots),
			revocation_snapshot.as_deref(),
		)?
	} else {
		verified_transport_peer_from_chain_with_revocations(
			certs,
			&cluster_domain,
			revocation_snapshot.as_deref(),
		)?
		.actor_identity
	};
	let connection: Arc<dyn TransportConnection> =
		Arc::new(InboundTcpTlsConnection::new(actor, stream));
	handle_connection(
		connection,
		service,
		pool,
		lifecycle,
		admission,
		connection_admission,
	)
	.await
}

/// Per-connection accept loop. Spawns one task per inbound bidi
/// stream and exits when the connection is closed (cleanly or
/// otherwise).
///
/// Errors from `accept_bi` collapse the loop. The QUIC wrapper
/// already maps clean-close into `ErrorKind::ConnectionAborted`
/// and transport faults into `ErrorKind::ConnectionReset`, so a
/// future logging hook can branch on the kind without touching
/// quinn's enum.
async fn handle_connection<S: HrftService>(
	connection: Arc<dyn TransportConnection>,
	service: Arc<S>,
	pool: Arc<BufPool>,
	lifecycle: ServerLifecycle,
	admission: AdmissionController,
	connection_admission: ConnectionAdmissionGuard,
) -> std::io::Result<()> {
	let context = RequestContext {
		remote: connection.remote_id(),
		actor: connection.actor_identity().clone(),
	};
	let mut drain_rx = lifecycle.subscribe();
	loop {
		if drain_rx.borrow().draining {
			return Ok(());
		}
		tokio::select! {
			changed = drain_rx.changed() => {
				if changed.is_err() || drain_rx.borrow().draining {
					return Ok(());
				}
			}
			stream = connection.accept_bi() => {
				let stream = match stream {
					Ok(s) => s,
					Err(_) => return Ok(()),
				};
				let stream_admission = match admission.try_acquire_stream(&connection_admission) {
					Ok(guard) => guard,
					Err(reason) => {
						admission.record_rejection(reason);
						reject_stream(stream).await;
						continue;
					}
				};
				let service = service.clone();
				let pool = pool.clone();
				let context = context.clone();
				let _stream_guard = lifecycle.track_work();
				// Keep `_stream_admission` inside the spawned task so the
				// per-connection stream slot and the global task permit are
				// released together when the handler returns. Rejected
				// streams never create this guard, so their permits drop
				// immediately in the local reject path above.
				tokio::spawn(async move {
					let _stream_guard = _stream_guard;
					let _stream_admission = stream_admission;
					if let Err(error) = handle_stream(stream, service, pool, context.clone()).await {
						tracing::warn!(
							remote_node_id = ?context.remote.map(|node| node.id),
							actor_role = %context.actor.role,
							%error,
							"wendelon request stream failed"
						);
					}
				});
			}
		}
	}
}

// ============================================================
//  PER-STREAM TASK
// ============================================================

async fn handle_stream<S: HrftService>(
	stream: Box<dyn TransportStream>,
	service: Arc<S>,
	pool: Arc<BufPool>,
	context: RequestContext,
) -> std::io::Result<()> {
	service.handle_stream(stream, pool, context).await
}

async fn reject_stream(mut stream: Box<dyn TransportStream>) {
	let _ = stream.reject().await;
}

// ============================================================
//  NO-OP SERVICE (compilation harness)
//
//  A drop-in service that rejects every frame with
//  `ErrorKind::Unsupported`. Lets binaries wire up the server and
//  endpoint plumbing ahead of the real handler and fail loudly if
//  traffic arrives before the dispatcher has been installed.
// ============================================================

pub struct UnroutedService;

#[async_trait]
impl HrftService for UnroutedService {
	async fn handle_frame(
		&self,
		_context: RequestContext,
		frame: HrftFrame,
	) -> std::io::Result<Option<HrftFrame>> {
		Err(Error::new(
			ErrorKind::Unsupported,
			format!(
				"no HRFT handler installed for msg_type=0x{:02x}",
				frame.header.msg_type
			),
		))
	}
}

/// Test-only: one inbound connection served by the production accept loop,
/// with the admission it was given: what a control outside this module needs
/// to see that a stream's handler still holds its stream permit.
#[cfg(test)]
pub(crate) struct ServedConnectionForTest {
	admission: AdmissionController,
	_task: tokio::task::JoinHandle<std::io::Result<()>>,
}

#[cfg(test)]
impl ServedConnectionForTest {
	/// Serves `connection` with `service` under `lifecycle` and `config`, as
	/// a listener does once a connection is admitted.
	pub(crate) fn serve(
		connection: Arc<dyn TransportConnection>,
		service: Arc<LifecycleService>,
		pool: Arc<BufPool>,
		lifecycle: ServerLifecycle,
		config: ServerAdmissionConfig,
	) -> Self {
		let admission = AdmissionController::new(config);
		let connection_admission = admission
			.try_acquire_connection_for_peer(None)
			.expect("a fresh controller admits one connection");
		let task = tokio::spawn(handle_connection(
			connection,
			service,
			pool,
			lifecycle,
			admission.clone(),
			connection_admission,
		));
		Self {
			admission,
			_task: task,
		}
	}

	/// Stream-task permits taken now.
	pub(crate) fn stream_tasks_admitted(&self) -> usize {
		self.admission
			.config
			.max_concurrent_stream_tasks
			.saturating_sub(self.admission.stream_task_slots.available_permits())
	}
}

#[cfg(test)]
mod tests {
	use super::{
		AdmissionController, AdmissionRejectionReason, HrftService, RequestContext,
		ServerAdmissionConfig, ServerLifecycle, handle_connection,
	};
	use crate::network::pool::BufPool;
	use crate::network::tls::ActorIdentity;
	use crate::network::transport::{TransportConnection, TransportStream, TransportTier};
	use crate::schema::NodeId;
	use async_trait::async_trait;
	use std::collections::VecDeque;
	use std::io::{Error, ErrorKind};
	use std::panic::AssertUnwindSafe;
	use std::pin::Pin;
	use std::sync::Arc;
	use std::sync::Mutex;
	use std::sync::atomic::{AtomicUsize, Ordering};
	use std::task::{Context, Poll};
	use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
	use tokio::sync::Notify;

	#[derive(Default)]
	struct FakeStreamState {
		finish_calls: AtomicUsize,
		reject_calls: AtomicUsize,
		finished: Notify,
	}

	struct FakeStream {
		state: Arc<FakeStreamState>,
	}

	impl FakeStream {
		fn new(state: Arc<FakeStreamState>) -> Self {
			Self { state }
		}
	}

	#[async_trait]
	impl TransportStream for FakeStream {
		fn stream_id(&self) -> u64 {
			0
		}

		async fn finish(&mut self) -> std::io::Result<()> {
			self.state.finish_calls.fetch_add(1, Ordering::SeqCst);
			self.state.finished.notify_waiters();
			Ok(())
		}

		async fn reject(&mut self) -> std::io::Result<()> {
			self.state.reject_calls.fetch_add(1, Ordering::SeqCst);
			self.state.finished.notify_waiters();
			Ok(())
		}
	}

	impl AsyncRead for FakeStream {
		fn poll_read(
			self: Pin<&mut Self>,
			_cx: &mut Context<'_>,
			_buf: &mut ReadBuf<'_>,
		) -> Poll<std::io::Result<()>> {
			Poll::Ready(Ok(()))
		}
	}

	impl AsyncWrite for FakeStream {
		fn poll_write(
			self: Pin<&mut Self>,
			_cx: &mut Context<'_>,
			buf: &[u8],
		) -> Poll<std::io::Result<usize>> {
			Poll::Ready(Ok(buf.len()))
		}

		fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
			Poll::Ready(Ok(()))
		}

		fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
			Poll::Ready(Ok(()))
		}
	}

	struct FakeConnection {
		remote: NodeId,
		actor: ActorIdentity,
		streams: Mutex<VecDeque<Box<dyn TransportStream>>>,
	}

	impl FakeConnection {
		fn new(
			remote: NodeId,
			actor: ActorIdentity,
			streams: impl IntoIterator<Item = Box<dyn TransportStream>>,
		) -> Self {
			Self {
				remote,
				actor,
				streams: Mutex::new(streams.into_iter().collect()),
			}
		}
	}

	#[async_trait]
	impl TransportConnection for FakeConnection {
		async fn open_bi(&self) -> std::io::Result<Box<dyn TransportStream>> {
			Err(Error::new(
				ErrorKind::Unsupported,
				"fake inbound connection does not open outbound streams",
			))
		}

		async fn accept_bi(&self) -> std::io::Result<Box<dyn TransportStream>> {
			self.streams.lock().unwrap().pop_front().ok_or_else(|| {
				Error::new(
					ErrorKind::ConnectionAborted,
					"fake connection has no more inbound streams",
				)
			})
		}

		fn remote_id(&self) -> Option<NodeId> {
			Some(self.remote)
		}

		fn actor_identity(&self) -> &ActorIdentity {
			&self.actor
		}

		fn transport_type(&self) -> TransportTier {
			TransportTier::QuicIoUring
		}

		fn is_closed(&self) -> bool {
			self.streams.lock().unwrap().is_empty()
		}
	}

	struct BlockingStreamService {
		started: Arc<Notify>,
		release: Arc<Notify>,
		calls: AtomicUsize,
	}

	#[async_trait]
	impl HrftService for BlockingStreamService {
		async fn handle_frame(
			&self,
			_context: RequestContext,
			_frame: crate::network::core::HrftFrame,
		) -> std::io::Result<Option<crate::network::core::HrftFrame>> {
			Err(Error::new(
				ErrorKind::Unsupported,
				"test service only handles streams",
			))
		}

		async fn handle_stream(
			&self,
			_stream: Box<dyn TransportStream>,
			_pool: Arc<BufPool>,
			_context: RequestContext,
		) -> std::io::Result<()> {
			self.calls.fetch_add(1, Ordering::SeqCst);
			self.started.notify_waiters();
			self.release.notified().await;
			Ok(())
		}
	}

	#[tokio::test]
	async fn per_connection_stream_limit_rejects_excess_streams() {
		let remote = NodeId {
			id: 91,
			_reserved: 0,
		};
		let first_stream_state = Arc::new(FakeStreamState::default());
		let second_stream_state = Arc::new(FakeStreamState::default());
		let connection: Arc<dyn TransportConnection> = Arc::new(FakeConnection::new(
			remote,
			ActorIdentity::data_node(remote),
			[
				Box::new(FakeStream::new(first_stream_state)) as Box<dyn TransportStream>,
				Box::new(FakeStream::new(second_stream_state.clone())) as Box<dyn TransportStream>,
			],
		));
		let service = Arc::new(BlockingStreamService {
			started: Arc::new(Notify::new()),
			release: Arc::new(Notify::new()),
			calls: AtomicUsize::new(0),
		});
		let lifecycle = ServerLifecycle::new();
		let admission = AdmissionController::new(ServerAdmissionConfig {
			max_inbound_connections: 1,
			max_inbound_connections_per_ip: 1,
			max_inbound_streams_per_connection: 1,
			max_concurrent_stream_tasks: 4,
		});
		let connection_admission = admission.try_acquire_connection_for_peer(None).unwrap();
		let started = service.started.clone();
		let release = service.release.clone();
		let task = tokio::spawn(handle_connection(
			connection,
			service.clone(),
			BufPool::new(8, 1024),
			lifecycle,
			admission,
			connection_admission,
		));

		started.notified().await;
		tokio::time::timeout(std::time::Duration::from_secs(1), async {
			while second_stream_state.reject_calls.load(Ordering::SeqCst) == 0 {
				tokio::task::yield_now().await;
			}
		})
		.await
		.unwrap();
		assert_eq!(service.calls.load(Ordering::SeqCst), 1);
		assert_eq!(second_stream_state.reject_calls.load(Ordering::SeqCst), 1);
		assert_eq!(second_stream_state.finish_calls.load(Ordering::SeqCst), 0);

		release.notify_waiters();
		task.await.unwrap().unwrap();
	}

	#[tokio::test]
	async fn global_stream_task_limit_rejects_additional_connections() {
		let remote_a = NodeId {
			id: 92,
			_reserved: 0,
		};
		let remote_b = NodeId {
			id: 93,
			_reserved: 0,
		};
		let first_rejected_state = Arc::new(FakeStreamState::default());
		let second_rejected_state = Arc::new(FakeStreamState::default());
		let connection_a: Arc<dyn TransportConnection> = Arc::new(FakeConnection::new(
			remote_a,
			ActorIdentity::data_node(remote_a),
			[Box::new(FakeStream::new(first_rejected_state)) as Box<dyn TransportStream>],
		));
		let connection_b: Arc<dyn TransportConnection> = Arc::new(FakeConnection::new(
			remote_b,
			ActorIdentity::data_node(remote_b),
			[Box::new(FakeStream::new(second_rejected_state.clone()))
				as Box<dyn TransportStream>],
		));
		let service = Arc::new(BlockingStreamService {
			started: Arc::new(Notify::new()),
			release: Arc::new(Notify::new()),
			calls: AtomicUsize::new(0),
		});
		let admission = AdmissionController::new(ServerAdmissionConfig {
			max_inbound_connections: 2,
			max_inbound_connections_per_ip: 2,
			max_inbound_streams_per_connection: 4,
			max_concurrent_stream_tasks: 1,
		});
		let lifecycle = ServerLifecycle::new();
		let started = service.started.clone();
		let release = service.release.clone();

		let task_a = tokio::spawn(handle_connection(
			connection_a,
			service.clone(),
			BufPool::new(8, 1024),
			lifecycle.clone(),
			admission.clone(),
			admission.try_acquire_connection_for_peer(None).unwrap(),
		));
		started.notified().await;
		let task_b = tokio::spawn(handle_connection(
			connection_b,
			service.clone(),
			BufPool::new(8, 1024),
			lifecycle,
			admission.clone(),
			admission.try_acquire_connection_for_peer(None).unwrap(),
		));

		tokio::time::timeout(std::time::Duration::from_secs(1), async {
			while second_rejected_state.reject_calls.load(Ordering::SeqCst) == 0 {
				tokio::task::yield_now().await;
			}
		})
		.await
		.unwrap();
		assert_eq!(service.calls.load(Ordering::SeqCst), 1);
		assert_eq!(second_rejected_state.reject_calls.load(Ordering::SeqCst), 1);
		assert_eq!(second_rejected_state.finish_calls.load(Ordering::SeqCst), 0);

		release.notify_waiters();
		task_a.await.unwrap().unwrap();
		task_b.await.unwrap().unwrap();
	}

	#[test]
	fn per_ip_connection_limit_rejects_excess_connections_from_same_peer() {
		let admission = AdmissionController::new(ServerAdmissionConfig {
			max_inbound_connections: 4,
			max_inbound_connections_per_ip: 1,
			max_inbound_streams_per_connection: 4,
			max_concurrent_stream_tasks: 4,
		});
		let first = admission
			.try_acquire_connection_for_peer(Some("127.0.0.1".parse().unwrap()))
			.unwrap();
		let err = admission
			.try_acquire_connection_for_peer(Some("127.0.0.1".parse().unwrap()))
			.err()
			.unwrap();
		assert_eq!(err, AdmissionRejectionReason::PerIpConnectionLimit);
		let second = admission
			.try_acquire_connection_for_peer(Some("127.0.0.2".parse().unwrap()))
			.unwrap();
		drop(first);
		drop(second);
	}

	#[test]
	fn server_admission_config_rejects_zero_limits() {
		let err = ServerAdmissionConfig {
			max_inbound_connections: 0,
			max_inbound_connections_per_ip: 1,
			max_inbound_streams_per_connection: 1,
			max_concurrent_stream_tasks: 1,
		}
		.validate()
		.unwrap_err();
		assert!(
			err.to_string()
				.contains("transport_admission.max_inbound_connections")
		);
		let err = ServerAdmissionConfig {
			max_inbound_connections: 1,
			max_inbound_connections_per_ip: 0,
			max_inbound_streams_per_connection: 1,
			max_concurrent_stream_tasks: 1,
		}
		.validate()
		.unwrap_err();
		assert!(
			err.to_string()
				.contains("transport_admission.max_inbound_connections_per_ip")
		);
	}

	#[test]
	fn lifecycle_recovers_after_state_mutex_poison() {
		let lifecycle = ServerLifecycle::new();
		let _ = std::panic::catch_unwind(AssertUnwindSafe(|| {
			let _guard = lifecycle.inner.state.lock().unwrap();
			panic!("poison lifecycle state");
		}));

		let guard = lifecycle
			.try_track_request()
			.expect("poisoned lifecycle state should recover");
		assert_eq!(lifecycle.in_flight(), 1);
		drop(guard);
		assert_eq!(lifecycle.in_flight(), 0);

		lifecycle.begin_drain();
		assert!(lifecycle.is_draining());
	}
}
