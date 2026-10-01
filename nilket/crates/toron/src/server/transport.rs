use crate::distributed::identity::WorkerId;
use crate::distributed::worker_registry::{WorkerEndpoint, WorkerEntry};
use crate::error::QueryError;
use crate::network::tls::canonical_daemon_dns_name;
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

	/// Dials a peer worker by its stable identity and endpoint. A transport that authenticates
	/// peers derives the expected server identity from `worker_id`; one that does not just dials
	/// the endpoint.
	async fn connect_peer(
		&self,
		worker_id: &WorkerId,
		endpoint: &WorkerEndpoint,
	) -> Result<Box<dyn ToronStream>, QueryError> {
		let _ = worker_id;
		self.connect(endpoint.as_str()).await
	}

	/// Dials a worker with access to its stable identity.
	async fn connect_worker(
		&self,
		worker: &WorkerEntry,
	) -> Result<Box<dyn ToronStream>, QueryError> {
		self.connect_peer(&worker.worker_id, &worker.endpoint).await
	}
}

#[async_trait]
impl<Transport> ToronTransport for Arc<Transport>
where
	Transport: ToronTransport + ?Sized,
{
	async fn connect(&self, address: &str) -> Result<Box<dyn ToronStream>, QueryError> {
		self.as_ref().connect(address).await
	}

	async fn connect_peer(
		&self,
		worker_id: &WorkerId,
		endpoint: &WorkerEndpoint,
	) -> Result<Box<dyn ToronStream>, QueryError> {
		self.as_ref().connect_peer(worker_id, endpoint).await
	}

	async fn connect_worker(
		&self,
		worker: &WorkerEntry,
	) -> Result<Box<dyn ToronStream>, QueryError> {
		self.as_ref().connect_worker(worker).await
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
	loopback_endpoint: QuicEndpoint,
	authenticated_endpoint: Option<QuicEndpoint>,
	cluster_domain: Option<String>,
}

impl QuicTransport {
	pub fn new() -> Result<Self, QueryError> {
		Ok(Self {
			loopback_endpoint: QuicEndpoint::client_loopback()?,
			authenticated_endpoint: None,
			cluster_domain: None,
		})
	}

	pub fn authenticated(
		client_config: quinn::ClientConfig,
		cluster_domain: impl Into<String>,
	) -> Result<Self, QueryError> {
		Ok(Self {
			loopback_endpoint: QuicEndpoint::client_loopback()?,
			authenticated_endpoint: Some(QuicEndpoint::client_authenticated(client_config)?),
			cluster_domain: Some(cluster_domain.into()),
		})
	}
}

#[async_trait]
impl ToronTransport for QuicTransport {
	async fn connect(&self, address: &str) -> Result<Box<dyn ToronStream>, QueryError> {
		if self.cluster_domain.is_some() {
			return Err(QueryError::Internal {
				message: "authenticated QUIC transport requires worker identity".to_string(),
			});
		}
		let remote: SocketAddr = address.parse().map_err(|error| QueryError::Internal {
			message: format!("invalid QUIC endpoint {address:?}: {error}"),
		})?;
		Ok(Box::new(
			self.loopback_endpoint.connect_stream(remote).await?,
		))
	}

	async fn connect_worker(
		&self,
		worker: &WorkerEntry,
	) -> Result<Box<dyn ToronStream>, QueryError> {
		self.connect_peer(&worker.worker_id, &worker.endpoint).await
	}

	async fn connect_peer(
		&self,
		worker_id: &WorkerId,
		endpoint: &WorkerEndpoint,
	) -> Result<Box<dyn ToronStream>, QueryError> {
		let address = endpoint.as_str();
		let remote: SocketAddr = address.parse().map_err(|error| QueryError::Internal {
			message: format!("invalid QUIC endpoint {address:?}: {error}"),
		})?;
		if remote.ip().is_loopback() {
			return Ok(Box::new(
				self.loopback_endpoint.connect_stream(remote).await?,
			));
		}
		if let Some(cluster_domain) = &self.cluster_domain {
			let server_name = canonical_daemon_dns_name(worker_id, cluster_domain);
			let Some(endpoint) = &self.authenticated_endpoint else {
				return Err(QueryError::Internal {
					message: "authenticated QUIC endpoint is not configured".to_string(),
				});
			};
			return Ok(Box::new(
				endpoint
					.connect_stream_with_server_name(remote, &server_name)
					.await?,
			));
		}
		Ok(Box::new(
			self.loopback_endpoint.connect_stream(remote).await?,
		))
	}
}
