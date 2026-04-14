# Plan: Implement The Smart Client SDK and Routing Layer

## Summary
This plan details the creation of the Wendelon Smart Client SDK. Currently, Wendelon is a headless distributed storage engine. This SDK will serve as the entry point for external analytics engines (like Polariton/DataFusion) to interact with the cluster. It will handle topology-aware routing, dynamic transport negotiation, and zero-allocation streaming of WPBC (Wendelon Pushdown Bytecode) queries and Arrow IPC responses.

## Motivation
To be useful to the Nilket ecosystem, Wendelon requires a client that natively understands its custom protocols. A "dumb" client would introduce unnecessary network hops or require proxying on the server side. A "smart" client SDK that maintains a localized view of the Unified Hash Ring (ADR 002) and natively implements the Auto-Adaptive Transport (ADR 011) and WPBC execution (ADR 005) ensures we maintain our zero-copy, low-latency performance characteristics all the way to the application layer.

## Goals
- **Topology Awareness:** Implement a background subscription to the Raft Control Ring's `ring(E)` snapshot to maintain an up-to-date client-side routing table.
- **Transport Negotiation:** Integrate the existing `TransportProviderFactory` logic into the client to negotiate the best available transport tier (RDMA, QUIC, TCP/TLS) with the target data nodes.
- **Zero-Allocation Query API:** Provide an API surface for clients to construct WPBC queries, dispatch them to the correct partition owners, and stream the resulting Arrow IPC batches without allocating intermediate buffers.

## Non-Goals
- Integration directly into DataFusion/Polariton (the SDK is standalone first; integrations come later).
- Client-side data caching or complex retry logic beyond basic idempotency handling.
- Background L1->L2 Sweeping (this is Target #3).

## Implementation
1. **Client Skeleton:** Create a new module (e.g., `src/client/mod.rs` and `src/client/core.rs`) for the SDK.
2. **Topology Subscriber:** Implement a task that connects to the Control Ring (using a known bootstrap node), fetches the `ClusterStatusResponseMeta` and ring topology, and listens for epoch advancements to update a local `ReplicaRouter`.
3. **Transport Layer:** Reuse `src/network/factory.rs` on the client side. The client will initialize its own `TransportTierSet` and use it to dial data nodes directly via the custom HRFT framing protocol.
4. **Query Interface:** Implement a `WendelonClient::query(wpbc_ast, key_range)` method.
   - Use the `ReplicaRouter` to determine the target nodes.
   - Open HRFT streams via the negotiated transport.
   - Serialize the WPBC query.
   - Stream back `RecordBatch` results by passing the zero-copy Arrow IPC bytes directly to the caller.

## Risks and Mitigations
- **Risk:** Stale topology routing leading to excessive `E_SAFE_BEHIND` or proxy hop errors.
  - **Mitigation:** The client should seamlessly handle redirect hints (`leader_hint` or equivalent) returned by data nodes and immediately force a topology refresh when encountered.
- **Risk:** Code duplication between server-side routing and client-side routing.
  - **Mitigation:** Extract shared routing abstractions (like `ReplicaRouter` introduced in Target #1) into a common location if necessary.

## Timeline
This is Target #2. To be implemented following the stabilization of the Stellarator Write Path.

## References
- `plans/adr/002_unified_hash_ring.md`
- `plans/adr/005_compute_pushdown_wpbc.md`
- `plans/adr/011_auto_adaptive_transport.md`
