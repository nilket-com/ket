# ADR 004: Zero-Copy Custom Transport (Rejecting gRPC/Protobuf)

## Status
Accepted

## Context
Moving massive amounts of data requires minimizing CPU overhead. Standard RPC frameworks like gRPC rely on HTTP/2 and Protobuf. Protobuf mandates serialization/deserialization steps, which involve memory allocations and branch-heavy parsing. At Wendelon's target throughput (millions of ops/sec or gigabytes/sec), the CPU time spent parsing protobuf varints becomes a critical bottleneck.

## Decision
Wendelon will abandon gRPC and Protobuf. We will build a **Custom Framed Transport over QUIC (`quinn`)**. 
- **Metadata** will be defined as strict `#[repr(C)]` Plain-Old-Data (POD) structs using the `bytemuck` crate, allowing 0ns pointer casting directly from the network buffer.
- **Payloads** will use Apache Arrow IPC formats for zero-copy columnar data.

## Rationale
- **Absolute Zero-Copy:** `bytemuck` casting means zero CPU cycles are spent decoding metadata on the hot path. Arrow IPC means data payloads are mapped directly to memory and submitted to `io_uring` exactly as they arrived on the wire.
- **QUIC Native:** QUIC solves HTTP/2 head-of-line blocking and provides native 0-RTT connection establishment, crucial for low-latency internal routing.

## Consequences (The "Negatives" / Rejected Ideas)
- **Rejected Ecosystem Tooling:** We explicitly reject the massive ecosystem of gRPC tooling (automatic client generation across dozens of languages, standard load balancers, etc.). 
- **Maintenance Burden:** We are taking on ownership of the framing protocol (`HRFT`), the RPC trait definitions, and the connection pooling.
- **Client SDKs:** We will have to hand-write native client SDKs (Rust first, others later) or rely on a proxy daemon (like PGWire) to bridge the gap.