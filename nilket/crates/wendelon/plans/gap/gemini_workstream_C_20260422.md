# Wendelon Workstream C: Transport, Admission Control & Runtime Stability (WS2)

**Author:** Lead Software Architect (Gemini)
**Date:** April 22, 2026
**Reference:** Mapping to WS2 in `plans/gap/execution_20260422.md`

## 1. Architectural Stance & Opinion

A distributed system without admission control is not a highly available system; it is a system waiting for its first busy tenant to bring down the whole cluster. The current state of Wendelon's QUIC transport and request admission is dangerously naive. It assumes well-behaved clients and unlimited resources.

Furthermore, we are lying in our own architecture docs. If the code says "QUIC on top of io_uring + GSO" but the implementation uses a default `quinn::Endpoint` with heavy buffering and global mutexes, we are setting false expectations for latency and scale. My stance is: **Truth in advertising, and survival under hostile load.** We must implement strict, tenant-aware backpressure, bounded task fan-out, and we must decouple our `io_uring` topology so that Raft WAL commits are never blocked by massive slab appends.

## 2. Scope & Deliverables

### 2.1 The "Scale & Survive" QUIC Listener
*   **The Mandate:** Implement strict admission controls at the listener level. Max concurrent connections, max streams per connection, and bounded task fan-out.
*   **Tenant-Aware Backpressure:** After authentication, requests must be routed to per-tenant queues. If a single tenant spikes, only their queue fills up, triggering HTTP 429 / QUIC backpressure without affecting other tenants.
*   **Exit Criteria:** A chaos test simulating a DoS attack from a single tenant. The targeted tenant experiences 100% rejection/backpressure, while all other tenants maintain their baseline P99 latency within a 5% margin.

### 2.2 `io_uring` Decoupling & Storage Isolation
*   **The Mandate:** The single shared `io_uring` thread is a massive tail-latency choke point. We must split execution by durability class: one dedicated ring for the Raft WAL, and one dedicated ring per physical NVMe device for slab I/O.
*   **Exit Criteria:** Benchmarks proving that Raft commit latency remains perfectly flat even when the slab layer is completely saturated with heavy append operations.

### 2.3 `SmState` Clone Elimination
*   **The Mandate:** Stop cloning the entire replicated state machine (`SmState`) on every request just to check the current epoch or tenant status.
*   **Implementation:** Introduce a narrow, immutable `tokio::sync::watch` cache for admission-critical fields.
*   **Exit Criteria:** Request admission allocation profiling shows a >80% reduction in bytes allocated per request.

## 3. Milestones & Gating

*   **Phase 1 (Correctness Locked):** `SmState` clone eliminated. QUIC admission limits configured. Live-path panic inventory completed.
*   **Phase 2 (Scale & Survive):** `io_uring` topology successfully split. Tenant-aware backpressure active.
*   **Phase 3 (Procurement Ready):** Transport tier docs updated to match reality (retracting GSO claims if unfulfilled).

## 4. Open Questions

1.  **Transport Truth-Up:** Do we spend the engineering capital to actually implement GSO/GRO and `io_uring` QUIC integration now, or do we simply retract the claim from the docs and rely on `quinn`'s default performance for the pilot? (Recommendation: Retract for pilot, re-evaluate post-GA).
2.  **Buffer Pooling:** Is the global mutexed `BufPool` going to become the next bottleneck once `io_uring` is split? Should we move to a thread-local or sharded allocation scheme immediately?
3.  **Connection Fairness:** In a multi-tenant environment, how do we guarantee fair connection establishment if the QUIC listener is saturated before the TLS handshake even authenticates the tenant?