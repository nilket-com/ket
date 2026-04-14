# Refactoring Plan: Auto-Adaptive Multi-Transport (ADR 011)

This document outlines the structural changes required to move from a QUIC-only transport to the Auto-Adaptive Multi-Transport architecture. 

## 1. Core Abstractions (`src/network/transport.rs`)

We must decouple our high-level services (`DataRing`, `ControlRing`) from the specific `quinn` types.

### 1.1 The `TransportStream` Trait
An abstraction over a bi-directional communication channel.
```rust
#[async_trait]
pub trait TransportStream: AsyncRead + AsyncWrite + Unpin + Send {
    /// Returns the unique ID of this stream within the connection.
    fn stream_id(&self) -> u32;
    /// Gracefully closes the write side of the stream.
    async fn finish(&mut self) -> std::io::Result<()>;
}
```

### 1.2 The `TransportConnection` Trait
An abstraction over a session between two nodes.
```rust
#[async_trait]
pub trait TransportConnection: Send + Sync {
    /// Opens a new bi-directional stream.
    async fn open_bi(&self) -> std::io::Result<Box<dyn TransportStream>>;
    /// Accepts an incoming bi-directional stream.
    async fn accept_bi(&self) -> std::io::Result<Box<dyn TransportStream>>;
    /// Returns the remote node identity.
    fn remote_id(&self) -> NodeId;
    /// Returns the type of transport currently in use (QUIC, RDMA, etc).
    fn transport_type(&self) -> TransportTier;
}
```

## 2. Capability Advertising & Discovery

### 2.1 Node Capabilities
Add a `TransportTier` bitmask to the node's Raft metadata (`RaftEpochEntry`).
```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TransportTier {
    RdmaRoceV2 = 0,
    QuicIoUring = 1,
    TcpTls = 2,
}
```

### 2.2 The Negotiation Sequence
1.  **Baseline Handshake:** Node A opens a Tier 1 (QUIC) connection to Node B.
2.  **Capability Exchange:** Over the Tier 1 stream, Node A and B exchange `TransportTier` support.
3.  **Upgrade (Optional):** If both support Tier 0 (RDMA), they use the QUIC stream to exchange RDMA Memory Keys (RKeys) and Queue Pair (QP) numbers.
4.  **Promotion:** The data-plane operations (`DataRing`) switch to using the RDMA fabric for subsequent `HRFT` frames.

## 3. Integrating with `BufPool` (Claude's Foundation)

Claude's recent implementation of `src/network/pool.rs` is **architecturally perfect** for this.
- RDMA requires memory to be "registered" (pinned) with the NIC.
- The `BufPool` should be updated to support **Registered Buffers**.
- `PooledBuf` will need to carry an optional `LKey` (Local Key) for RDMA zero-copy operations.

## 4. Implementation Steps for Claude

1.  **Refactor `read_frame` / `write_frame`:** Make them work with the `TransportStream` trait rather than raw `AsyncRead/Write` if necessary, or ensure `HrftFrame` stays transport-agnostic.
2.  **Introduce `TransportProviderFactory`:** A central registry that:
    - Maintains the pool of `TransportConnection` objects.
    - Handles the logic: `get_connection(target_node_id) -> Connection`.
    - Implements the "Adaptive Selection" (Tier 0 -> Tier 1 -> Tier 2).
3.  **QUIC Provider Implementation:** Move the current `quinn` logic into a `QuicTransportProvider` that implements the new traits.
4.  **RDMA Stub:** Create `src/network/rdma.rs` as a placeholder for the future Tier 0 provider.

## 5. Verification Boundary
- `DataRing` implementation should not care if it is calling `quinn` or `ibverbs` under the hood.
- `BufPool` must remain the single source of memory for both transports to ensure zero-copy throughput.
