# Workstream F: Resource Control & Fault Injection
**Date:** 2026-04-17
**Author:** Gemini CLI

## Goal
Prevent predictable overload failures by strictly bounding resources, and mathematically prove the system's correctness under deterministic fault injection.

## Scope & Tasks

### 1. Memory Bounding & Backpressure
*   **Target Files:** `src/network/data_server.rs`, `src/network/control_server.rs`, `src/network/server.rs`
*   **Tasks:**
    *   Implement TTL sweeps and eviction policies for unbounded `DashMap` caches (e.g., Idempotency Cache, Lease Cache).
    *   Add per-peer connection caps in the QUIC spawn loop.
    *   Implement in-flight write caps to trigger read/write backpressure (e.g., `OVERLOADED` status) instead of OOM under heavy load.

### 2. Fault Injection Proving
*   **Target Files:** `tests/`
*   **Tasks:**
    *   Introduce a deterministic network simulator harness (`madsim` or `turmoil`).
    *   Validate linearizability under majority/minority network partitions.
    *   Validate consistency when a leader crashes during snapshot installations.
    *   Validate L1->L2 tiering crashes at each step of the two-phase pivot.
    *   Implement a long-running soak test (thousands of logical operations under random fault injection).

## Exit Criteria
*   The system degrades predictably under pressure (backpressure, not OOM).
*   Deterministic partition/crash tests exist and pass for core cluster invariants.
*   Soak tests complete without linearizability violations.

---
## Open Questions
*   Which fault injection framework (`madsim`, `turmoil`, or custom) will provide the highest fidelity given our use of `io_uring` and `quinn`?