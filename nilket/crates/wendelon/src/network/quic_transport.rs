//! Tier 1 (`QuicIoUring`) provider for the Auto-Adaptive Transport
//! traits in [`crate::network::transport`].
//!
//! This is a thin wrapper over `quinn` that keeps higher layers
//! from depending on `quinn::{Connection, SendStream, RecvStream}`
//! directly. The goals are:
//!
//! * **No behavioral change** vs. the pre-refactor call sites. The
//!   QUIC wrapper simply delegates: `open_bi` / `accept_bi` forward
//!   to `quinn::Connection`'s identically-named methods, and the
//!   combined stream implements [`AsyncRead`] / [`AsyncWrite`] by
//!   pinning and polling the recv / send halves respectively.
//! * **Room for RDMA.** [`QuicTransportConnection::transport_type`]
//!   returns [`TransportTier::QuicIoUring`]; when Tier 0 lands, the
//!   factory can swap this out without any changes above the trait.
//! * **Dyn-object yield.** All public methods return the trait
//!   object forms (`Box<dyn TransportStream>`) that
//!   [`TransportConnection`] demands, so call sites see a single
//!   stream shape regardless of fabric.

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

// ============================================================
//  TLS NAME RESOLUTION
// ============================================================

/// Resolves the TLS SNI to present when dialing a given Raft peer.
///
/// Wendelon's mTLS posture (see `plans/network/security.md`) binds
/// every node's `NodeId` to a SAN on its server cert, so the SNI
/// must be derived from the target id rather than being a fixed
/// constant. The resolver takes any `Fn(u32) -> String + Send + Sync`
/// — deployments can wire this to DNS (`format!("node-{id}.{zone}")`)
/// or to a cluster-membership lookup.
pub type ServerNameResolver = Arc<dyn Fn(u32) -> String + Send + Sync>;

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
}

impl QuicTransportConnection {
    pub fn new(inner: quinn::Connection, remote_id: NodeId) -> Self {
        Self { inner, remote_id }
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
        | quinn::ConnectionError::ConnectionClosed(_) => {
            Error::new(ErrorKind::ConnectionAborted, e)
        }
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
}

impl QuicTransportProvider {
    pub fn new(endpoint: Arc<quinn::Endpoint>, server_name: ServerNameResolver) -> Self {
        Self {
            endpoint,
            server_name,
        }
    }
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
        Ok(Arc::new(QuicTransportConnection::new(conn, target)))
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
