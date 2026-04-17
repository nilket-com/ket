# High-Level Execution Plan: Wendelon Gap Closure
**Date:** 2026-04-17
**Author:** Gemini CLI
**Derived from:** `plans/gap/gemini_assessment_20260417.md`
**Peer inputs incorporated:** Claude execution plan (2026-04-17), Codex execution plan (2026-04-17)

This document outlines a phased execution plan to transform Wendelon from an architectural prototype into a commercially viable, enterprise-ready distributed storage fabric. This plan directly addresses the P0 blockers and critical architectural gaps identified in our joint gap assessments, incorporating the integration-first and structured hardening strategies of the broader team.

---

## Guiding Principles

1. **Integration Gate First:** The highest-leverage addition is a real-network smoke test that runs on every PR. Build this gate first so that all subsequent fixes are verified end-to-end.
2. **Shipped Daemon Path is the Definition of Done:** A subsystem is not complete until it is exercised through the shipped `main.rs` daemon path using TOML configuration.
3. **Fix the Footgun, Then Harden:** Small correctness and security defects (like rogue admission or disk leaks) should not wait behind large design programs.
4. **ADR First for Design-Heavy Changes:** Encryption at rest, authorization (RBAC), and major interface changes (like multi-tenancy) require Architecture Decision Records (ADRs) before broad implementation.
5. **Small, Independently Verifiable Units:** Prefer narrow slices that either pass or fail a clear validation step.

---

## Phase 0: Integration Gate & Production Path Bring-Up
**Goal:** Make the stock daemon a real, testable cluster process and establish an end-to-end testing gate.

*   **Workstream 0.A: Public Config & Daemon Wiring:**
    *   Wire `config.rs` to load TLS material (`cert_path`, `key_path`, `ca_path`) and L2 configs from TOML, removing hardcoded disabled states (e.g., `l2: None`).
    *   Wire `main.rs` to construct a real `quinn::Endpoint`, call `serve_quic()`, support public `cluster.mode = "join"` startup (removing the `ErrorKind::Unsupported` gate in `node.rs`), and initialize minimal tracing for bring-up visibility.
*   **Workstream 0.B: Integration Smoke Test Gate:**
    *   Create a real-network integration harness (`tests/integration_smoke.rs`) with 3 nodes using real QUIC endpoints + mTLS.
    *   Exercise JOIN, WRITE, READ, and node restart. Add this to CI standard verification.

---

## Phase 1: Rapid Correctness Fixes
**Goal:** Land the small, high-leverage fixes that close obvious correctness, resource, and security holes quickly before larger structural work.

*   **Workstream 1.A: Rogue-Node Admission Fix:** Modify the `JOIN_REQ` handler (`src/network/control_server.rs`) to strictly use the authenticated `remote_id` extracted from the peer's mTLS certificate SAN. Implement a negative test to verify this.
*   **Workstream 1.B: Tier Observer Wiring:** Attach `ManagerTierObserver` in `WendelonStateMachine` bootstrap (`src/node.rs`) so follower-side L1 cleanup (unlinking tiered slabs) is real, closing the silent disk leak.
*   **Workstream 1.C: Dev-Secret Hygiene:** Add explicit warnings and sharper isolation for deterministic dev signing material in operator-facing flows (`render_example_config`).

---

## Phase 2: Security Floor
**Goal:** Establish the minimum security and compliance posture required for a serious pilot deployment.

*   **Workstream 2.A: Encryption at Rest (ADR & Implementation):**
    *   Write an ADR for an envelope encryption scheme and KMS integration.
    *   Implement encryption for L1 slabs (`src/storage/core.rs`), WAL pages, snapshots, and L2 objects using AES-GCM or similar.
*   **Workstream 2.B: Control Plane Authorization:** Distinguish operator authority from data-node authority based on certificate claims (OID extensions or distinct CAs).
*   **Workstream 2.C: Audit & Key Lifecycle:** Add an append-only audit stream for privileged actions and introduce basic key/cert rotation support without requiring full cluster restarts.

---

## Phase 3: Durability Lifecycle Closure
**Goal:** Finish the control/storage lifecycle so durability claims hold in steady state, not only during recovery tests.

*   **Workstream 3.A: Live Snapshot Persistence:** Implement durable writes for the `snapshot.wnd` file during the live Raft state machine snapshot process. Add snapshot chunking to prevent memory bloat.
*   **Workstream 3.B: WAL Compaction & Checkpoints:** Emit `REC_CHECKPOINT` records at runtime. Implement actual WAL truncation on disk (file rewrite/hole-punch) rather than just dropping entries from the in-memory cache.
*   **Workstream 3.C: Background Scrubbing:** Add background verification tasks to opportunistically detect bit-rot and corruption in cold L1/L2 slabs based on manifest hashes.

---

## Phase 4: Tiered Storage Closure
**Goal:** Turn L2 from an implemented subsystem into a leak-free, production-integrated one.

*   **Workstream 4.A: Configuration & Pivot Convergence:** Ensure L2 is enabled from config and exercised through the shipped daemon. Add pending-upload journals for crash-safe L1->L2 transitions.
*   **Workstream 4.B: Orphan GC & Remote Reclaim:** Implement a background reconciliation loop comparing `tiered_slabs` Raft state against bucket contents to clean up orphans from mid-upload crashes.
*   **Workstream 4.C: Range-GET & Multipart Upload:** Modify the `RemoteObjectStore` trait to support range reads and multipart uploads, avoiding 64MB+ full-slab downloads for point-lookups. (Note: May require a breaking trait change).

---

## Phase 5: Resource Control & Fault Injection
**Goal:** Prevent predictable overload failures and start proving behavior under deterministic faults.

*   **Workstream 5.A: Memory Bounding & Backpressure:** Implement TTL sweeps for unbounded caches (e.g., Idempotency Cache, Lease Cache). Add per-peer connection caps (`src/network/server.rs`) and in-flight write caps to trigger backpressure instead of OOM.
*   **Workstream 5.B: Fault Injection Proving:** Introduce a deterministic network simulator harness (e.g., `madsim` or `turmoil`). Validate linearizability under majority/minority partitions, leader crashes during snapshots, and mid-tiering failures. Add a long-running soak test.

---

## Phase 6: Operations & Observability
**Goal:** Make the product operable and observable by SREs for pilot readiness.

*   **Workstream 6.A: Observability:** Add distributed tracing (OTLP exporter, span propagation) and robust dimensional metrics (latency histograms).
*   **Workstream 6.B: Operator Tooling:** Build an operator CLI (`wendelon backup`, `restore`, `cluster-status`, `drain`). Add k8s health/readiness endpoints and systemd integration.

---

## Phase 7: Multi-Tenancy & Policy Expansion
**Goal:** Expand from pilot-grade security into broader commercial isolation features (Deferred from initial pilot blockers).

*   **Workstream 7.A: Isolation & Quotas:** Design and implement namespace-prefixed schema/routing, per-tenant storage/throughput quotas, and per-tenant L2 bucket routing.
*   **Workstream 7.B: Key Isolation:** Extend Phase 2 encryption with per-tenant encryption keys wired to the KMS.

---

## Deliverables for Future Planning Iterations
The subsequent passes should split this scaffold into dedicated, detailed plans:
1. **Runtime and transport integration plan** (Phase 0)
2. **Security and encryption-at-rest plan** (Phases 1 Security + 2)
3. **Durability lifecycle plan** (Phase 3)
4. **Tiered storage closure plan** (Phase 4)
5. **Verification and fault-injection plan** (Phase 5)
6. **Operations and pilot-readiness plan** (Phase 6)
7. **Multi-tenancy plan** (Phase 7)

---

## Immediate Recommendation for Next Planning Pass
Start with a detailed **Phase 0 and Phase 1 (Runtime Integration & Rapid Fixes) plan**. This includes the public config wiring, QUIC/TLS bootstrap and join flow, real-network integration gate (`tests/integration_smoke.rs`), join identity binding fix, production `TierObserver` wiring, and minimal observability initialization. This is the narrowest high-leverage next step, turning the rest of the roadmap into testable product-path execution.

---

## Open Questions for Next Iteration
1. **Encryption ADR Scope:** Which algorithm (AES-256-GCM vs XChaCha20-Poly1305)? What are the performance implications for the `io_uring` + `O_DIRECT` alignment? Should we inject encryption at the `src/storage/core.rs` append path, or at the serialization layer?
2. **Authorization Model:** Should we use OID extensions in X.509 certs, a separate operator CA, or token-based auth for Control Ring RPCs?
3. **Fault Injection Framework:** Do we use `madsim`, `turmoil`, or a custom deterministic simulator?
4. **ObjectStore Trait Redesign:** Range-GET changes the trait shape. Should we batch streaming put, multipart upload, and list into a single breaking change early on?
5. **Slab fsync vs O_DIRECT guarantees:** Is `O_DIRECT` without an explicit `fsync` group-commit sufficient, assuming target hardware utilizes PLP NVMe?