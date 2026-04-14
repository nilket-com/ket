# ADR 011: Auto-Adaptive Multi-Transport Architecture

## Status
Accepted

## Context
Wendelon aims for "Elite" performance. Traditional database networking (TCP/gRPC) is insufficient for 100Gbps+ datacenter fabrics where RDMA/RoCE provide zero-copy, zero-CPU data transfer. However, RDMA is hardware-dependent and unavailable in standard cloud environments or for external clients. QUIC (via `quinn`) provides a superior "Cloud-Elite" baseline (TLS 1.3, multiplexing, congestion control) but cannot match RDMA's raw latency and throughput on bare metal.

## Decision
Wendelon will implement an **Auto-Adaptive Multi-Transport** architecture. 

1.  **Transport Abstraction:** The network layer will be refactored into a `TransportProvider` trait. The `DataRing` and `ControlRing` services will operate over this abstraction rather than being hard-coded to QUIC.
2.  **Capability Advertising:** Every node will probe its local hardware at boot and advertise its supported transports (e.g., `RDMA_ROCE_V2`, `QUIC_IO_URING`, `TCP_TLS`) via Raft metadata in the `ring(E)` snapshot.
3.  **Dynamic Negotiation:** 
    - Connections will be established using the highest-priority transport supported by **both** the dialer and the listener.
    - QUIC will serve as the "Universal Baseline" and the control-plane signaling channel used to negotiate "Elite" upgrades (e.g., exchanging RDMA memory keys).
4.  **Priority Tiers:**
    - Tier 0: **RDMA/RoCE** (Physical-Elite) - Zero-copy kernel bypass.
    - Tier 1: **QUIC + io_uring + GSO** (Cloud-Elite) - Optimized syscall bypass.
    - Tier 2: **TCP + TLS 1.3** (Normie Fallback) - Guaranteed compatibility.

## Rationale
- **Absolute Eliteness:** Enables Wendelon to saturate local NVMe/NIC throughput on bare-metal clusters without CPU overhead.
- **Universal Portability:** "Just works" on developer laptops, standard cloud instances, and high-end datacenter fabrics without code changes or configuration flags.
- **Smart Clients:** Downstream consumers (Baryon/Polariton) inherit this adaptivity, automatically using the fastest available path based on their location relative to the Data Nodes.

## Consequences (The "Negatives")
- **Refactoring Overhead:** Requires moving current network logic into a provider-factory pattern.
- **Complexity:** Managing multiple transport implementations (e.g., `ibverbs` for RDMA alongside `quinn` for QUIC) increases the testing and maintenance surface.
- **Leaky Abstractions:** RDMA's memory-registration model differs fundamentally from QUIC's stream model; the `TransportProvider` must be carefully designed to allow zero-copy buffer passing without regressing to the lowest common denominator.
