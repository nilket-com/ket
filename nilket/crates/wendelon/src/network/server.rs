//! HRFT dispatch server, transport-agnostic above the listener.
//!
//! A single QUIC endpoint terminates all inbound Wendelon traffic
//! today — Data Ring, Control Ring, and Raft internal RPCs are
//! multiplexed on message-type discriminants inside the frame
//! header rather than separated by port. The accept loop in
//! [`serve`] is the only QUIC-specific piece left in this file:
//! every other function operates on the
//! [`TransportConnection`] / [`TransportStream`] trait abstractions
//! introduced for ADR 011's Auto-Adaptive Multi-Transport.
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

use crate::network::core::{HrftFrame, read_frame, write_frame};
use crate::metrics::global as metrics;
use crate::network::pool::BufPool;
use crate::network::quic_transport::QuicTransportConnection;
use crate::network::tcp_tls_transport::InboundTcpTlsConnection;
use crate::network::tls::ActorIdentity;
use crate::network::tls::{verified_transport_peer, verified_transport_peer_from_chain};
use crate::network::transport::{TransportConnection, TransportStream};
use crate::schema::NodeId;
use async_trait::async_trait;
use quinn::Endpoint;
use std::io::{Error, ErrorKind};
use std::sync::Arc;
use std::sync::Mutex;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio_rustls::TlsAcceptor;

// ============================================================
//  LIFECYCLE
// ============================================================

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequestContext {
	pub remote: NodeId,
	pub actor: ActorIdentity,
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

	fn track_work(&self) -> LifecycleGuard {
		self.update_state(|state| state.in_flight += 1);
		LifecycleGuard {
			lifecycle: self.clone(),
		}
	}

	fn try_track_request(&self) -> std::io::Result<LifecycleGuard> {
		let mut guard = self
			.inner
			.state
			.lock()
			.expect("server lifecycle state poisoned");
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
		*self
			.inner
			.state
			.lock()
			.expect("server lifecycle state poisoned")
	}

	fn update_state(&self, mutator: impl FnOnce(&mut LifecycleState)) {
		let mut guard = self
			.inner
			.state
			.lock()
			.expect("server lifecycle state poisoned");
		mutator(&mut guard);
		let snapshot = *guard;
		drop(guard);
		let _ = self.inner.tx.send_replace(snapshot);
	}
}

struct LifecycleGuard {
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
	let cluster_domain = cluster_domain.into();
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
				let service = service.clone();
				let pool = pool.clone();
				let lifecycle = lifecycle.clone();
				let cluster_domain = cluster_domain.clone();
				let _connection_guard = lifecycle.track_work();
				tokio::spawn(async move {
					let _connection_guard = _connection_guard;
					if let Err(e) =
						serve_quic_connection(incoming, service, pool, lifecycle, cluster_domain)
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
	let cluster_domain = cluster_domain.into();
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
				let Ok((stream, _peer)) = accepted else {
					break;
				};
				let acceptor = acceptor.clone();
				let service = service.clone();
				let pool = pool.clone();
				let lifecycle = lifecycle.clone();
				let cluster_domain = cluster_domain.clone();
				let _connection_guard = lifecycle.track_work();
				tokio::spawn(async move {
					let _connection_guard = _connection_guard;
					if let Err(e) = serve_tcp_tls_connection(
						stream,
						acceptor,
						service,
						pool,
						lifecycle,
						cluster_domain,
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
	cluster_domain: String,
) -> std::io::Result<()> {
	let connection = incoming
		.await
		.map_err(|e| {
			metrics().quic_handshake_failures_transport_total.inc();
			Error::new(ErrorKind::ConnectionAborted, e)
		})?;
	let peer = verified_transport_peer(&connection, &cluster_domain).map_err(|err| {
		match err.kind() {
			ErrorKind::PermissionDenied => metrics().quic_handshake_failures_authorization_total.inc(),
			_ => metrics().quic_handshake_failures_identity_total.inc(),
		}
		err
	})?;
	let connection: Arc<dyn TransportConnection> = Arc::new(QuicTransportConnection::new(
		connection,
		peer.node_id,
		peer.actor_identity,
	));
	handle_connection(connection, service, pool, lifecycle).await
}

async fn serve_tcp_tls_connection<S: HrftService>(
	stream: TcpStream,
	acceptor: TlsAcceptor,
	service: Arc<S>,
	pool: Arc<BufPool>,
	lifecycle: ServerLifecycle,
	cluster_domain: String,
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
	let peer = verified_transport_peer_from_chain(certs, &cluster_domain)?;
	let connection: Arc<dyn TransportConnection> = Arc::new(InboundTcpTlsConnection::new(
		peer.node_id,
		peer.actor_identity,
		stream,
	));
	handle_connection(connection, service, pool, lifecycle).await
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
				let service = service.clone();
				let pool = pool.clone();
				let context = context.clone();
				let _stream_guard = lifecycle.track_work();
				tokio::spawn(async move {
					let _stream_guard = _stream_guard;
					let _ = handle_stream(stream, service, pool, context).await;
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
