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
//! * Graceful shutdown coordination — today the serve loop only
//!   exits when the endpoint itself is closed externally.

use crate::network::core::{HrftFrame, read_frame, write_frame};
use crate::network::pool::BufPool;
use crate::network::quic_transport::QuicTransportConnection;
use crate::network::tls::{DEFAULT_CLUSTER_DOMAIN, verified_peer_node_id};
use crate::network::transport::{TransportConnection, TransportStream};
use crate::schema::NodeId;
use async_trait::async_trait;
use quinn::Endpoint;
use std::io::{Error, ErrorKind};
use std::sync::Arc;

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
		remote: NodeId,
		frame: HrftFrame,
	) -> std::io::Result<Option<HrftFrame>>;

	/// Per-stream entry point. The default implementation keeps the
	/// unary "one frame in, at most one frame out" behavior; routers
	/// can override it for client-streaming RPCs such as bulk write.
	async fn handle_stream(
		&self,
		mut stream: Box<dyn TransportStream>,
		pool: Arc<BufPool>,
		remote: NodeId,
	) -> std::io::Result<()> {
		let frame = read_frame(&mut *stream, &pool).await?;

		match self.handle_frame(remote, frame).await? {
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
) -> std::io::Result<()> {
	loop {
		let Some(incoming) = endpoint.accept().await else {
			// Endpoint has been closed: exit cleanly.
			return Ok(());
		};
		let service = service.clone();
		let pool = pool.clone();
		tokio::spawn(async move {
			if let Err(e) = serve_quic_connection(incoming, service, pool).await {
				// One bad connection should not kill the server.
				// A structured logger will go here once the tracing
				// macros are threaded through.
				let _ = e;
			}
		});
	}
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
) -> std::io::Result<()> {
	let connection = incoming
		.await
		.map_err(|e| Error::new(ErrorKind::ConnectionAborted, e))?;
	let remote_id = verified_peer_node_id(&connection, DEFAULT_CLUSTER_DOMAIN)?;
	let connection: Arc<dyn TransportConnection> =
		Arc::new(QuicTransportConnection::new(connection, remote_id));
	handle_connection(connection, service, pool).await
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
) -> std::io::Result<()> {
	let remote = connection.remote_id();
	loop {
		let stream = match connection.accept_bi().await {
			Ok(s) => s,
			Err(_) => return Ok(()),
		};
		let service = service.clone();
		let pool = pool.clone();
		let remote = remote;
		tokio::spawn(async move {
			let _ = handle_stream(stream, service, pool, remote).await;
		});
	}
}

// ============================================================
//  PER-STREAM TASK
// ============================================================

async fn handle_stream<S: HrftService>(
	stream: Box<dyn TransportStream>,
	service: Arc<S>,
	pool: Arc<BufPool>,
	remote: NodeId,
) -> std::io::Result<()> {
	service.handle_stream(stream, pool, remote).await
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
		_remote: NodeId,
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
