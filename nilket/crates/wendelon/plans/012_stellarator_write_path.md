# Plan: Implement The Stellarator Write Path

## Summary
This plan details the implementation of the distributed replica fan-out and finalization logic for the Stellarator Write Path in `src/network/data_server.rs`. It transforms the existing local `io_uring` slab persistence (RF=1) into a fully distributed quorum write that interacts with the `TransportProviderFactory` and the Unified Hash Ring to achieve high-performance, bifurcated data replication.

## Motivation
Wendelon's core performance claim rests on the Stellarator Model (ADR 003): massive bulk data (Arrow IPC) travels directly to NVMe on the Data Ring, while only an 88-byte `FinalizationMarker` metadata goes through the Raft Control Ring to advance the `E_safe` watermark. With foundational components built (local slab storage, network transports, openraft integration), wiring up the actual replica fan-out is necessary to validate that the bifurcated write path correctly achieves quorum and safely finalizes across the cluster.

## Goals
- **Replica Fan-out:** Modify `WendelonDataServer::write` to determine the target replica set (using a routing abstraction based on the Unified Hash Ring from ADR 002).
- **Concurrent Quorum Transmission:** Use `TransportProviderFactory::get_connection` to acquire dynamically negotiated sessions for the target `NodeId`s. Stream the Arrow IPC payload concurrently to all replicas using the custom HRFT framing protocol (ADR 004).
- **Quorum Acknowledgement:** Await durable acks from a quorum (majority) of the replica set.
- **Raft Finalization:** Once quorum is achieved, construct the 88-byte `FinalizationMarker` and submit it to the `openraft` control ring via `self.raft.client_write` to advance the `E_safe` watermark.

## Non-Goals
- Implementing the Client SDK (Target #2).
- Implementing the Background Sweeper (Target #3).
- Dynamically altering replication factor per request (assume a static RF for now, e.g., RF=3).

## Implementation
1. **Routing Abstraction:** Introduce a `ReplicaRouter` trait and a `StaticReplicaRouter` implementation within `WendelonDataServer` to resolve a `key_hash` to a replica set of `NodeId`s based on the Unified Hash Ring (ADR 002).
2. **Network Wiring:** Inject `Arc<TransportProviderFactory>` into `WendelonDataServer::new`.
3. **Refactor `write`:**
    - Perform the existing epoch validation and idempotency checks.
    - Resolve the target `NodeId`s. If the local node is one of them, route to `self.slab.append` directly.
    - For remote nodes, spawn concurrent futures. For each:
        - Call `factory.get_connection(target_node)`.
        - Open a stream (e.g., `conn.open_bi()`).
        - Transmit the framed Arrow IPC bytes using HRFT framing.
        - Await the remote acknowledgement.
4. **Quorum Gathering:** Use `tokio::task::JoinSet` to collect results. Proceed to finalization *only* when a majority (`(RF / 2) + 1`) of replicas (including the local node, if applicable) have acknowledged the durable write.
5. **Finalization:** Keep the existing step that builds the `FinalizationMarker` and calls `self.raft.client_write(WendelonRequest::Finalize(marker))`. If quorum was not reached, return a `WriteStatus::INTERNAL_ERROR` or similar, and do not finalize.
6. **Error Handling:** Ensure that partial failures do not leave the system in an inconsistent state (unfinalized slabs are acceptable per the lazy sweeping model).

## Risks and Mitigations
- **Risk:** High latency due to head-of-line blocking during replica fan-out.
  - **Mitigation:** Ensure all remote writes are executed concurrently without blocking the main tokio worker thread.
- **Risk:** Race conditions during concurrent `TransportProviderFactory` dials.
  - **Mitigation:** `TransportProviderFactory` already handles double-check locking internally to prevent duplicate dials. Simply rely on its caching behavior.

## References
- `plans/adr/003_stellarator_bifurcated_writes.md`
- `plans/adr/004_zero_copy_transport.md`
- `src/network/data_server.rs`
- `src/network/factory.rs`
