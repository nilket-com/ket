//! QUIC transport provider for the traits in
//! [`crate::network::transport`].
//!
//! This is a thin wrapper over `quinn` that keeps higher layers
//! from depending on `quinn::{Connection, SendStream, RecvStream}`
//! directly. This is the supported day-one transport path. The
//! goals are:
//!
//! * **No behavioral change** vs. the pre-refactor call sites. The
//!   QUIC wrapper simply delegates: `open_bi` / `accept_bi` forward
//!   to `quinn::Connection`'s identically-named methods, and the
//!   combined stream implements [`AsyncRead`] / [`AsyncWrite`] by
//!   pinning and polling the recv / send halves respectively.
//! * **Extension seam, not overclaim.**
//!   [`QuicTransportConnection::transport_type`] returns
//!   [`TransportTier::QuicIoUring`], and the surrounding seam keeps
//!   future transports possible without implying they already ship.
//! * **Dyn-object yield.** All public methods return the trait
//!   object forms (`Box<dyn TransportStream>`) that
//!   [`TransportConnection`] demands, so call sites see a single
//!   stream shape regardless of fabric.

use crate::consensus::raft_storage::RevocationSnapshot;
use crate::network::tls::{
	ActorIdentity, DEFAULT_CLUSTER_DOMAIN, TlsMaterial, client_config, server_config,
	verified_transport_peer_with_revocations,
};
use crate::network::transport::{
	TransportConnection, TransportProvider, TransportStream, TransportTier,
};
use crate::schema::NodeId;
use async_trait::async_trait;
use std::io::{Error, ErrorKind};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::watch;

// ============================================================
//  TLS NAME RESOLUTION
// ============================================================

/// Resolves the TLS SNI to present when dialing a given Raft peer.
///
/// Wendelon's mTLS posture (see `plans/network/security.md`) binds
/// every node's `NodeId` to a SAN on its server cert, so the SNI
/// must be derived from the target id rather than being a fixed
/// constant. The canonical first-wave template is
/// `node-<id>.<cluster_domain>`.
pub type ServerNameResolver = Arc<dyn Fn(u32) -> String + Send + Sync>;

/// Build a resolver that maps a target node id to the canonical
/// DNS SAN `node-<id>.<cluster_domain>`.
pub fn canonical_server_name_resolver(cluster_domain: impl Into<String>) -> ServerNameResolver {
	let cluster_domain = cluster_domain.into();
	Arc::new(move |node_id| crate::network::tls::canonical_node_dns_name(node_id, &cluster_domain))
}

/// Convenience resolver for the default first-wave cluster domain.
pub fn default_server_name_resolver() -> ServerNameResolver {
	canonical_server_name_resolver(crate::network::tls::DEFAULT_CLUSTER_DOMAIN)
}

// ============================================================
//  CONNECTION
// ============================================================

/// QUIC-backed [`TransportConnection`]. Holds one
/// `quinn::Connection`; the inner handle is `Arc`-backed inside
/// quinn so the wrapper is cheap to share and cheap to drop.
pub struct QuicTransportConnection {
	inner: quinn::Connection,
	/// Peer's Wendelon identity — filled at factory time from the
	/// Raft config (or, on the server side, from the validated
	/// client certificate SAN). Exposed via
	/// [`TransportConnection::remote_id`].
	remote_id: NodeId,
	actor_identity: ActorIdentity,
}

impl QuicTransportConnection {
	pub fn new(inner: quinn::Connection, remote_id: NodeId, actor_identity: ActorIdentity) -> Self {
		Self {
			inner,
			remote_id,
			actor_identity,
		}
	}

	/// Access the underlying `quinn::Connection`. Intended for the
	/// migration period while call sites still reach for
	/// quinn-specific methods; new code should go through the
	/// trait. Removing this once Slice 2 completes is expected.
	pub fn inner(&self) -> &quinn::Connection {
		&self.inner
	}
}

#[async_trait]
impl TransportConnection for QuicTransportConnection {
	async fn open_bi(&self) -> std::io::Result<Box<dyn TransportStream>> {
		let (send, recv) = self.inner.open_bi().await.map_err(map_conn_err)?;
		Ok(Box::new(QuicTransportStream::new(send, recv)))
	}

	async fn accept_bi(&self) -> std::io::Result<Box<dyn TransportStream>> {
		let (send, recv) = self.inner.accept_bi().await.map_err(map_conn_err)?;
		Ok(Box::new(QuicTransportStream::new(send, recv)))
	}

	fn remote_id(&self) -> NodeId {
		self.remote_id
	}

	fn actor_identity(&self) -> &ActorIdentity {
		&self.actor_identity
	}

	fn transport_type(&self) -> TransportTier {
		TransportTier::QuicIoUring
	}

	fn is_closed(&self) -> bool {
		self.inner.close_reason().is_some()
	}
}

/// Map a `quinn::ConnectionError` onto an `io::Error` with an
/// `ErrorKind` that lets callers distinguish a clean shutdown from
/// a transport fault. The accept loop in `crate::network::server`
/// treats both as "stop accepting," but a future logging /
/// metrics hook can branch on the kind without re-parsing the
/// quinn enum.
fn map_conn_err(e: quinn::ConnectionError) -> Error {
	match e {
		quinn::ConnectionError::ApplicationClosed(_)
		| quinn::ConnectionError::LocallyClosed
		| quinn::ConnectionError::ConnectionClosed(_) => Error::new(ErrorKind::ConnectionAborted, e),
		_ => Error::new(ErrorKind::ConnectionReset, e),
	}
}

// ============================================================
//  PROVIDER (DIAL SIDE)
// ============================================================

/// QUIC implementation of [`TransportProvider`]. One per process —
/// holds the single `quinn::Endpoint` that all outbound dials
/// share, plus the SNI resolver bound to the cluster's TLS
/// material.
pub struct QuicTransportProvider {
	endpoint: Arc<quinn::Endpoint>,
	server_name: ServerNameResolver,
	cluster_domain: String,
	revocations: Option<watch::Receiver<Arc<RevocationSnapshot>>>,
}

impl QuicTransportProvider {
	pub fn new(endpoint: Arc<quinn::Endpoint>, server_name: ServerNameResolver) -> Self {
		Self::with_cluster_domain(endpoint, server_name, DEFAULT_CLUSTER_DOMAIN)
	}

	pub fn with_cluster_domain(
		endpoint: Arc<quinn::Endpoint>,
		server_name: ServerNameResolver,
		cluster_domain: impl Into<String>,
	) -> Self {
		Self {
			endpoint,
			server_name,
			cluster_domain: cluster_domain.into(),
			revocations: None,
		}
	}

	pub fn with_revocations(
		mut self,
		revocations: watch::Receiver<Arc<RevocationSnapshot>>,
	) -> Self {
		self.revocations = Some(revocations);
		self
	}
}

/// Bind a `quinn::Endpoint` with Wendelon's transport mTLS posture:
///
/// * the endpoint presents `material` as both its server and client
///   identity,
/// * inbound peers must present a cert chaining to the trusted CA,
/// * outbound dials always use client-auth as well.
pub fn endpoint_from_tls(
	material: &TlsMaterial,
	bind_addr: SocketAddr,
) -> std::io::Result<quinn::Endpoint> {
	let mut endpoint = quinn::Endpoint::server(server_config(material)?, bind_addr)?;
	endpoint.set_default_client_config(client_config(material)?);
	Ok(endpoint)
}

#[async_trait]
impl TransportProvider for QuicTransportProvider {
	async fn connect(
		&self,
		target: NodeId,
		address: &str,
	) -> std::io::Result<Arc<dyn TransportConnection>> {
		let server_name = (self.server_name)(target.id);
		let addr: SocketAddr = address.parse().map_err(|e| {
			Error::new(
				ErrorKind::InvalidInput,
				format!("peer address {address:?} is not a SocketAddr: {e}"),
			)
		})?;
		let connecting = self
			.endpoint
			.connect(addr, &server_name)
			.map_err(|e| Error::new(ErrorKind::ConnectionRefused, e))?;
		let conn = connecting
			.await
			.map_err(|e| Error::new(ErrorKind::ConnectionRefused, e))?;
		let peer = verified_transport_peer_with_revocations(
			&conn,
			&self.cluster_domain,
			self.revocations
				.as_ref()
				.map(|rx| rx.borrow().clone())
				.as_deref(),
		)?;
		if peer.node_id != target {
			return Err(Error::new(
				ErrorKind::PermissionDenied,
				format!(
					"peer SAN bound to node {} but dial target was {}",
					peer.node_id.id, target.id
				),
			));
		}
		Ok(Arc::new(QuicTransportConnection::new(
			conn,
			peer.node_id,
			peer.actor_identity,
		)))
	}
}

// ============================================================
//  STREAM
// ============================================================

/// Fused `(SendStream, RecvStream)` pair that presents a single
/// bidirectional [`AsyncRead`] + [`AsyncWrite`] surface.
///
/// `quinn::SendStream` and `quinn::RecvStream` are both already
/// `Unpin`, so the composite is `Unpin` too and the pin projections
/// can use `Pin::new(&mut ...)` directly without `pin-project`.
pub struct QuicTransportStream {
	send: quinn::SendStream,
	recv: quinn::RecvStream,
	/// Cached numeric stream id, snapshot at construction. Reading
	/// it later (e.g., from tracing hooks) does not require
	/// re-locking the quinn inner state.
	stream_id: u64,
}

impl QuicTransportStream {
	pub fn new(send: quinn::SendStream, recv: quinn::RecvStream) -> Self {
		let stream_id = u64::from(send.id());
		Self {
			send,
			recv,
			stream_id,
		}
	}
}

#[async_trait]
impl TransportStream for QuicTransportStream {
	fn stream_id(&self) -> u64 {
		self.stream_id
	}

	async fn finish(&mut self) -> std::io::Result<()> {
		// `SendStream::finish` is synchronous in quinn 0.11; the
		// post-finish drain happens as part of normal writes on
		// the still-open stream. `ClosedStream` means the peer
		// already tore the write half down — we surface that as
		// `BrokenPipe` to match the existing `WendelonRaftNetwork`
		// error mapping.
		self.send
			.finish()
			.map_err(|e| Error::new(ErrorKind::BrokenPipe, e))
	}

	async fn reject(&mut self) -> std::io::Result<()> {
		let _ = self.send.reset(0u32.into());
		let _ = self.recv.stop(0u32.into());
		Ok(())
	}
}

impl AsyncRead for QuicTransportStream {
	fn poll_read(
		self: Pin<&mut Self>,
		cx: &mut Context<'_>,
		buf: &mut ReadBuf<'_>,
	) -> Poll<std::io::Result<()>> {
		let this = self.get_mut();
		// Fully-qualified dispatch: `quinn::RecvStream` has an
		// inherent `poll_read` that returns `Result<(), ReadError>`,
		// which would shadow the tokio trait method and produce a
		// type-mismatch on the `std::io::Result<()>` return.
		<quinn::RecvStream as AsyncRead>::poll_read(Pin::new(&mut this.recv), cx, buf)
	}
}

impl AsyncWrite for QuicTransportStream {
	fn poll_write(
		self: Pin<&mut Self>,
		cx: &mut Context<'_>,
		buf: &[u8],
	) -> Poll<std::io::Result<usize>> {
		let this = self.get_mut();
		// Same reason as in `AsyncRead::poll_read` above — the
		// inherent quinn `poll_write` returns `WriteError` rather
		// than `std::io::Error`, so we invoke the trait method
		// explicitly.
		<quinn::SendStream as AsyncWrite>::poll_write(Pin::new(&mut this.send), cx, buf)
	}

	fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
		let this = self.get_mut();
		<quinn::SendStream as AsyncWrite>::poll_flush(Pin::new(&mut this.send), cx)
	}

	fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
		let this = self.get_mut();
		<quinn::SendStream as AsyncWrite>::poll_shutdown(Pin::new(&mut this.send), cx)
	}
}
