# Gap Analysis Report: Wendelon Storage Fabric (Synthesized)
**Date:** 2026-04-17
**Reviewer:** Gemini CLI (Principal Database Architect) & Peers (Claude, Codex)

## Executive Summary
Wendelon is a sophisticated, high-performance distributed storage fabric leveraging modern systems primitives (`io_uring`, `O_DIRECT`, `QUIC`, `openraft`). Its "Stellarator" architecture is a sound choice for scaling workloads. 

Following a recent ~48-hour development push, several core mechanisms are genuinely implemented (WAL durability, quorum write semantics). However, across our joint evaluations, it is clear that the system is currently a highly capable architectural prototype, not a commercially viable product. Critical gaps—most notably the production binary failing to wire QUIC or L2 tiering, a severe join-admission security flaw, a silent disk leak on followers, and the total absence of encryption at rest—must be addressed before it can be considered ready for mission-critical Fortune 500 deployments.

Based on combined peer review, the **overall readiness score is revised to 35%**.

---

## 1. Durability and Data Integrity
*   **WAL and Raft Consensus:** The implementation in `src/consensus/raft_storage.rs` is robust. It correctly employs `O_DIRECT` and ensures `submit_fsync` is awaited before acknowledging log flushes.
*   **Crash Recovery:** The Raft WAL and local Slab index implement on-disk recovery. Slabs are scanned at startup to rebuild the index, and records carry `blake3` checksums.
*   **Gaps:**
    *   **Incomplete Snapshot/Checkpoint Lifecycle (Codex):** While recovery loads `snapshot.wnd`, the live path does not appear to durably persist it to disk. Checkpoint emission is also missing, and Raft log truncation only mutates the in-memory cache, meaning WAL growth is effectively unbounded.
    *   **Background Scrubbing (Claude/Gemini):** There is no background mechanism to detect bit-rot in cold slabs. Checksums are only validated opportunistically on read.
    *   **Group-Commit Slab fsync (Gemini):** A formal `fsync` group-commit policy is not yet implemented for the slab append path, which limits throughput under high concurrency.
    *   **No Partition Testing (Claude):** The system lacks Jepsen-style / fault-injection testing (e.g., Jepsen, MadSim) to validate linearizability under split-brain scenarios.

## 2. Object Storage Tiering (S3/GCS)
*   **Transactional Intent:** The `SlabSweeper` implements a two-phase pivot (upload to L2, then Raft commit).
*   **Gaps:**
    *   **CRITICAL: Follower Disk Leak & Missing Node Wiring (Gemini/Codex):** The `WendelonStateMachine` in `src/node.rs` is NOT wired with a `TierObserver`. Followers apply the Raft entry but never trigger the local `unlink`, resulting in followers' NVMe disks filling up.
    *   **CRITICAL: Hardcoded Disabled L2 (Claude/Codex):** `src/config.rs` hardcodes `l2: None`. The shipped binary cannot actually be configured to use S3 without code changes.
    *   **Follower Uploads (Codex):** Followers may redundantly pay the full upload cost before failing the commit because tiering uploads happen before calling the Raft committer.
    *   **Orphan GC (Claude):** Mid-upload crashes can leave orphaned objects in S3 since there is no background reconciliation loop comparing Raft state to bucket contents.
    *   **Performance (Gemini/Claude):** L2 reads currently pull the entire slab blob (up to 64MB+). The lack of range-GET support for point-lookups will cause massive latency and egress costs.

## 3. High Availability and Operations
*   **Node Lifecycle:** Node join and bootstrap flows (protocol version negotiation) exist in the library.
*   **Gaps:**
    *   **CRITICAL: `main.rs` is not distributed (Claude/Codex):** The production binary `main.rs` does not construct a `quinn::Endpoint`, load TLS material, or call `serve_quic()`. `bootstrap()` explicitly rejects cluster join unless heavily customized. The daemon is effectively single-node.
    *   **Observability (Claude):** Basic metrics exist, but the system lacks distributed tracing (OpenTelemetry) and structured audit logging, which are mandatory for enterprise SRE teams.
    *   **Rebalancing (Gemini):** There is no autonomous rebalancing mechanism for L1 data when nodes are added or removed, only lazy migration.
    *   **DashMap Memory Leaks (Claude/Codex):** The idempotency cache and lease cache are unbounded `DashMap`s with no TTL eviction, guaranteeing eventual OOM under sustained load.

## 4. Performance and Concurrency
*   **I/O Stack:** `io_uring` and `O_DIRECT` usage positions the project for top-tier NVMe performance.
*   **Gaps:**
    *   **Single-threaded io_uring (Codex):** The `io_uring` thread is single-threaded per ring, which may become a bottleneck for multi-NVMe deployments.
    *   **Memory Bounding (Gemini/Claude):** `BufPool` is a free-list rather than a strict cap. There is no bounded admission control, per-peer connection cap, or backpressure, leaving the system vulnerable to memory exhaustion.
    *   **Full-batch Serialization (Codex):** The write path serializes full Arrow `RecordBatch`es into a single `Vec<u8>`, causing allocator hotspots.

## 5. Security and Compliance
*   **Network Security:** mTLS 1.3 is enforced with `NodeId` binding to certificate SANs.
*   **Gaps:**
    *   **CRITICAL: Encryption at Rest (All):** There is NO implementation of encryption at rest for either local NVMe slabs, WAL entries, or remote S3 objects. This is a complete blocker for regulated environments (GDPR, SOC2, HIPAA).
    *   **CRITICAL: Rogue-Node Admission Flaw (Codex):** While mTLS authenticates the transport, the `JOIN_REQ` control handler blindly trusts the `req.node_id` from the payload instead of enforcing the authenticated `remote: NodeId` from the certificate SAN. A node with any valid cert can spoof another node's ID.
    *   **Secret Management & Key Rotation (Claude/Gemini):** Signing keys are deterministically generated in dev templates and lack integration with KMS/Vault. There is no mechanism for hot certificate or Ed25519 key rotation.
    *   **Control Ring Authorization (Claude):** No RBAC on Control Ring RPCs. Any node with an mTLS cert can issue leadership transfers or epoch advances.

---

## 6. Verdict and Roadmap

### **Overall Readiness Score: 35%** (Downgraded due to critical runtime and security gaps)

### **Critical Blockers (P0) - Must Resolve Before Pilot**
1.  **Production Distributed Runtime:** Wire `main.rs` to load TLS material, start the QUIC endpoint, and support actual cluster joins. Add real-network multi-node integration tests.
2.  **Fix Join Admission Binding:** Enforce that `JOIN_REQ` requests strictly bind to the authenticated transport identity from the mTLS SAN.
3.  **L1 Disk Leak & L2 Hardcoding:** Wire the `ManagerTierObserver` in `node.rs` so followers unlink L1 slabs. Fix the TOML loader so `l2` config is respected.
4.  **Encryption at Rest:** Implement AES-GCM or KMS-backed envelope encryption for slab records, WAL entries, and L2 objects.
5.  **Durability Completion:** Implement durable `snapshot.wnd` persistence and WAL log compaction (not just in-memory truncation).
6.  **Bounded Memory & Backpressure:** Implement a TTL for the idempotency cache and strict admission control to prevent OOM under load.
7.  **S3 Range-GETs:** Implement range-reads for `ObjectStore` to avoid downloading entire slabs for point lookups.

### **Conclusion**
Wendelon has a high-quality "engine" (Stellarator, io_uring) but lacks the "chassis" for enterprise use. While theoretical durability is sound, the shipped daemon is not yet a fully wired distributed node, L2 tiering is disconnected, and severe security flaws (missing at-rest encryption, join spoofing) exist. Addressing the P0 blockers listed above is required to move the project from a credible prototype to a viable commercial storage layer.