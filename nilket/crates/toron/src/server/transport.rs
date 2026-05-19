use crate::error::QueryError;
use crate::quic::QuicEndpoint;
use async_trait::async_trait;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;

/// Transport-agnostic bidirectional byte stream, mirroring Wendelon's TransportStream.
pub trait ToronStream: AsyncRead + AsyncWrite + Unpin + Send {}

impl<Stream> ToronStream for Stream where Stream: AsyncRead + AsyncWrite + Unpin + Send {}

/// Abstract transport layer for Toron, allowing future RDMA zero-copy paths.
#[async_trait]
pub trait ToronTransport: Send + Sync {
	/// Dials a peer and opens a bidirectional stream.
	async fn connect(&self, address: &str) -> Result<Box<dyn ToronStream>, QueryError>;
}

#[async_trait]
impl<Transport> ToronTransport for Arc<Transport>
where
	Transport: ToronTransport + ?Sized,
{
	async fn connect(&self, address: &str) -> Result<Box<dyn ToronStream>, QueryError> {
		self.as_ref().connect(address).await
	}
}

#[derive(Debug, Default, Clone, Copy)]
pub struct TcpTransport;

impl TcpTransport {
	pub fn new() -> Self {
		Self
	}
}

#[async_trait]
impl ToronTransport for TcpTransport {
	async fn connect(&self, address: &str) -> Result<Box<dyn ToronStream>, QueryError> {
		Ok(Box::new(TcpStream::connect(address).await?))
	}
}

#[derive(Clone)]
pub struct QuicTransport {
	endpoint: QuicEndpoint,
}

impl QuicTransport {
	pub fn new() -> Result<Self, QueryError> {
		Ok(Self {
			endpoint: QuicEndpoint::client()?,
		})
	}
}

#[async_trait]
impl ToronTransport for QuicTransport {
	async fn connect(&self, address: &str) -> Result<Box<dyn ToronStream>, QueryError> {
		let remote: SocketAddr = address.parse().map_err(|error| QueryError::Internal {
			message: format!("invalid QUIC endpoint {address:?}: {error}"),
		})?;
		Ok(Box::new(self.endpoint.connect_stream(remote).await?))
	}
}
