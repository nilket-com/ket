//! Optional TCP+TLS compatibility transport provider.
//!
//! This path uses one TLS 1.3 TCP socket per logical HRFT stream.
//! It is deliberately less ambitious than QUIC's multiplexed
//! session model and is not the primary transport support claim.
//! Its job is to preserve the transport seam and provide an
//! explicit compatibility path where operators choose to wire it.

use crate::network::quic_transport::ServerNameResolver;
use crate::network::tls::{
	ActorIdentity, DEFAULT_CLUSTER_DOMAIN, VerifiedTransportPeer,
	verified_transport_peer_from_chain,
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
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream as ClientTlsStream;
use tokio_rustls::rustls::ClientConfig;
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_rustls::{TlsConnector, server::TlsStream as ServerTlsStream};

enum TcpTlsIo {
	Client(ClientTlsStream<TcpStream>),
	Server(ServerTlsStream<TcpStream>),
}

pub struct TcpTlsTransportStream {
	inner: TcpTlsIo,
	stream_id: u64,
}

impl TcpTlsTransportStream {
	pub fn client(stream: ClientTlsStream<TcpStream>, stream_id: u64) -> Self {
		Self {
			inner: TcpTlsIo::Client(stream),
			stream_id,
		}
	}

	pub fn server(stream: ServerTlsStream<TcpStream>, stream_id: u64) -> Self {
		Self {
			inner: TcpTlsIo::Server(stream),
			stream_id,
		}
	}
}

#[async_trait]
impl TransportStream for TcpTlsTransportStream {
	fn stream_id(&self) -> u64 {
		self.stream_id
	}

	async fn finish(&mut self) -> std::io::Result<()> {
		use tokio::io::AsyncWriteExt;
		self.shutdown().await
	}
}

impl AsyncRead for TcpTlsTransportStream {
	fn poll_read(
		self: Pin<&mut Self>,
		cx: &mut Context<'_>,
		buf: &mut ReadBuf<'_>,
	) -> Poll<std::io::Result<()>> {
		let this = self.get_mut();
		match &mut this.inner {
			TcpTlsIo::Client(stream) => Pin::new(stream).poll_read(cx, buf),
			TcpTlsIo::Server(stream) => Pin::new(stream).poll_read(cx, buf),
		}
	}
}

impl AsyncWrite for TcpTlsTransportStream {
	fn poll_write(
		self: Pin<&mut Self>,
		cx: &mut Context<'_>,
		buf: &[u8],
	) -> Poll<std::io::Result<usize>> {
		let this = self.get_mut();
		match &mut this.inner {
			TcpTlsIo::Client(stream) => Pin::new(stream).poll_write(cx, buf),
			TcpTlsIo::Server(stream) => Pin::new(stream).poll_write(cx, buf),
		}
	}

	fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
		let this = self.get_mut();
		match &mut this.inner {
			TcpTlsIo::Client(stream) => Pin::new(stream).poll_flush(cx),
			TcpTlsIo::Server(stream) => Pin::new(stream).poll_flush(cx),
		}
	}

	fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
		let this = self.get_mut();
		match &mut this.inner {
			TcpTlsIo::Client(stream) => Pin::new(stream).poll_shutdown(cx),
			TcpTlsIo::Server(stream) => Pin::new(stream).poll_shutdown(cx),
		}
	}
}

pub struct TcpTlsTransportConnection {
	remote_id: NodeId,
	actor_identity: ActorIdentity,
	address: String,
	connector: TlsConnector,
	server_name: String,
	cluster_domain: String,
	pending: Mutex<Option<TcpTlsTransportStream>>,
	next_stream_id: AtomicU64,
	closed: AtomicBool,
}

impl TcpTlsTransportConnection {
	// This fallback transport is intentionally not multiplexed:
	// one TCP+TLS connection is established per logical HRFT stream.
	fn new(
		remote_id: NodeId,
		actor_identity: ActorIdentity,
		address: String,
		connector: TlsConnector,
		server_name: String,
		cluster_domain: String,
		pending: TcpTlsTransportStream,
	) -> Self {
		Self {
			remote_id,
			actor_identity,
			address,
			connector,
			server_name,
			cluster_domain,
			pending: Mutex::new(Some(pending)),
			next_stream_id: AtomicU64::new(1),
			closed: AtomicBool::new(false),
		}
	}

	async fn dial_stream(&self) -> std::io::Result<TcpTlsTransportStream> {
		let addr: SocketAddr = self.address.parse().map_err(|e| {
			Error::new(
				ErrorKind::InvalidInput,
				format!("peer address {:?} is not a SocketAddr: {e}", self.address),
			)
		})?;
		let tcp = TcpStream::connect(addr).await?;
		let server_name = ServerName::try_from(self.server_name.clone()).map_err(|e| {
			Error::new(
				ErrorKind::InvalidInput,
				format!("invalid TLS server name {:?}: {e}", self.server_name),
			)
		})?;
		let tls = self
			.connector
			.connect(server_name, tcp)
			.await
			.map_err(|e| Error::new(ErrorKind::ConnectionRefused, e))?;
		let peer = peer_from_client_stream(&tls, &self.cluster_domain)?;
		if peer.node_id != self.remote_id {
			return Err(Error::new(
				ErrorKind::PermissionDenied,
				format!(
					"peer SAN bound to node {} but dial target was {}",
					peer.node_id.id, self.remote_id.id
				),
			));
		}
		Ok(TcpTlsTransportStream::client(
			tls,
			self.next_stream_id.fetch_add(1, Ordering::Relaxed),
		))
	}
}

#[async_trait]
impl TransportConnection for TcpTlsTransportConnection {
	async fn open_bi(&self) -> std::io::Result<Box<dyn TransportStream>> {
		if let Some(stream) = self.pending.lock().unwrap().take() {
			return Ok(Box::new(stream));
		}
		match self.dial_stream().await {
			Ok(stream) => Ok(Box::new(stream)),
			Err(err) => {
				self.closed.store(true, Ordering::Relaxed);
				Err(err)
			}
		}
	}

	async fn accept_bi(&self) -> std::io::Result<Box<dyn TransportStream>> {
		Err(Error::new(
			ErrorKind::Unsupported,
			"TCP+TLS dial-side connection does not accept inbound streams",
		))
	}

	fn remote_id(&self) -> NodeId {
		self.remote_id
	}

	fn actor_identity(&self) -> &ActorIdentity {
		&self.actor_identity
	}

	fn transport_type(&self) -> TransportTier {
		TransportTier::TcpTls
	}

	fn is_closed(&self) -> bool {
		self.closed.load(Ordering::Relaxed)
	}
}

pub struct InboundTcpTlsConnection {
	remote_id: NodeId,
	actor_identity: ActorIdentity,
	pending: Mutex<Option<TcpTlsTransportStream>>,
	closed: AtomicBool,
}

impl InboundTcpTlsConnection {
	pub fn new(
		remote_id: NodeId,
		actor_identity: ActorIdentity,
		stream: ServerTlsStream<TcpStream>,
	) -> Self {
		Self {
			remote_id,
			actor_identity,
			pending: Mutex::new(Some(TcpTlsTransportStream::server(stream, 0))),
			closed: AtomicBool::new(false),
		}
	}
}

#[async_trait]
impl TransportConnection for InboundTcpTlsConnection {
	async fn open_bi(&self) -> std::io::Result<Box<dyn TransportStream>> {
		Err(Error::new(
			ErrorKind::Unsupported,
			"TCP+TLS accepted connection does not open outbound streams",
		))
	}

	async fn accept_bi(&self) -> std::io::Result<Box<dyn TransportStream>> {
		let stream = self.pending.lock().unwrap().take().ok_or_else(|| {
			Error::new(
				ErrorKind::ConnectionAborted,
				"TCP+TLS accepted connection has no more streams",
			)
		})?;
		self.closed.store(true, Ordering::Relaxed);
		Ok(Box::new(stream))
	}

	fn remote_id(&self) -> NodeId {
		self.remote_id
	}

	fn actor_identity(&self) -> &ActorIdentity {
		&self.actor_identity
	}

	fn transport_type(&self) -> TransportTier {
		TransportTier::TcpTls
	}

	fn is_closed(&self) -> bool {
		self.closed.load(Ordering::Relaxed)
	}
}

pub struct TcpTlsTransportProvider {
	connector: TlsConnector,
	server_name: ServerNameResolver,
	cluster_domain: String,
}

impl TcpTlsTransportProvider {
	pub fn new(client_config: ClientConfig, server_name: ServerNameResolver) -> Self {
		Self::with_cluster_domain(client_config, server_name, DEFAULT_CLUSTER_DOMAIN)
	}

	pub fn with_cluster_domain(
		client_config: ClientConfig,
		server_name: ServerNameResolver,
		cluster_domain: impl Into<String>,
	) -> Self {
		Self {
			connector: TlsConnector::from(Arc::new(client_config)),
			server_name,
			cluster_domain: cluster_domain.into(),
		}
	}
}

#[async_trait]
impl TransportProvider for TcpTlsTransportProvider {
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
		let tcp = TcpStream::connect(addr).await?;
		let tls_server_name = ServerName::try_from(server_name.clone()).map_err(|e| {
			Error::new(
				ErrorKind::InvalidInput,
				format!("invalid TLS server name {server_name:?}: {e}"),
			)
		})?;
		let tls = self
			.connector
			.connect(tls_server_name, tcp)
			.await
			.map_err(|e| Error::new(ErrorKind::ConnectionRefused, e))?;
		let peer = peer_from_client_stream(&tls, &self.cluster_domain)?;
		if peer.node_id != target {
			return Err(Error::new(
				ErrorKind::PermissionDenied,
				format!(
					"peer SAN bound to node {} but dial target was {}",
					peer.node_id.id, target.id
				),
			));
		}
		Ok(Arc::new(TcpTlsTransportConnection::new(
			peer.node_id,
			peer.actor_identity,
			address.to_string(),
			self.connector.clone(),
			server_name,
			self.cluster_domain.clone(),
			TcpTlsTransportStream::client(tls, 0),
		)))
	}
}

fn peer_from_client_stream(
	stream: &ClientTlsStream<TcpStream>,
	cluster_domain: &str,
) -> std::io::Result<VerifiedTransportPeer> {
	let certs = stream.get_ref().1.peer_certificates().ok_or_else(|| {
		Error::new(
			ErrorKind::PermissionDenied,
			"peer presented no TLS identity",
		)
	})?;
	verified_transport_peer_from_chain(certs, cluster_domain)
}
