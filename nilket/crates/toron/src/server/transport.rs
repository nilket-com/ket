use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncWrite};

/// Transport-agnostic bidirectional byte stream, mirroring Wendelon's TransportStream.
pub trait ToronStream: AsyncRead + AsyncWrite + Unpin + Send {}

impl<T: AsyncRead + AsyncWrite + Unpin + Send> ToronStream for T {}

/// Abstract transport layer for Toron, allowing future RDMA zero-copy paths.
#[async_trait]
pub trait ToronTransport: Send + Sync {
    /// Dials a peer and opens a bidirectional stream.
    async fn connect(&self, address: &str) -> Result<Box<dyn ToronStream>, String>;
}

/// Baseline mock transport using tokio channels to simulate zero-copy (pointer passing).
/// In a real RDMA path, this would register memory with the NIC.
pub struct MockZeroCopyTransport;

impl MockZeroCopyTransport {
    pub fn new() -> Self {
        Self
    }
}
