# Codex Answers to Aggregated Open Questions
**Date:** 2026-04-17
**Source:** [plans/gap/open_questions_20260417.md](/home/me/work/wendelon/plans/gap/open_questions_20260417.md)

This is the lead-architect answer set, optimized for system quality rather than implementation convenience. Where a crisp answer exists, this document makes a decision. Where it does not, it says so plainly.

---

## 1. Integration Gate & Bring-Up
- **Test topology:** The gate of record should run separate daemon processes. In-process nodes are still valuable, but only as a faster secondary suite. The same scenario corpus should run against both executors so divergences become bugs, not separate test universes. The authoritative signal should exercise real process lifecycle, config loading, stderr, signal handling, and kernel networking.
- **Parallel CI port allocation:** Each test suite should run inside its own network namespace and allocate listener ports via bind-to-port-0, never via hard-coded port ranges. If namespace isolation is unavailable, a test-local port broker is the fallback, not the primary design.
- **Test certificate strategy:** Generate a fresh test PKI at runtime. Check in templates and profiles if needed, but not live cert/key pairs. A perfect test harness can shape SANs, expiries, and trust chains on demand.
- **L2 config loading timing:** Land the full config schema early, even if tiering is feature-gated off initially. Parsing, validation, and effective-config rendering should never lag the runtime feature.
- **Minimum bring-up observability:** Start with `tracing-subscriber`, env filtering, correlation IDs, and dual human/JSON output. Bare `fmt` is beneath the quality bar because it creates a migration later and weakens early failure analysis.

## 2. Immediate Correctness & Safety Fixes
- **Concurrency controls:** Build the general admission-control framework now and make QUIC stream caps its first policy. The default per-peer inbound stream ceiling should be 64, but the real answer is budget-driven scheduling across memory, CPU, and disk, not a magic constant in isolation.
- **Deterministic dev secrets:** Stop emitting deterministic secrets in default examples. If an insecure test profile is needed, it must be opt-in, loudly named, and physically separated from normal example generation.
- **Scope of the rapid fix phase:** Pull every silent safety defect forward now: temporary-file leaks, partial snapshot leftovers, abandoned upload state, non-expiring in-flight metadata, insecure defaults, and any unbounded resource path. "Small but severe" is exactly what should not be deferred.

## 3. Raft Log, Checkpoint, and Snapshot Lifecycle
- **WAL compaction strategy:** Use segmented WAL rotation with immutable closed segments and atomic manifest updates. Do not hole-punch, and do not rewrite in place. Rotation is the cleanest path for durability, forensic clarity, and `O_DIRECT` correctness.
- **`openraft` ownership boundary:** `openraft` should own logical Raft semantics. Wendelon should own the physical persistence contract: segment layout, checksums, staging, fsync ordering, snapshot chunking, compression, and garbage collection. The implementation should honor `openraft`'s contracts, not invent a parallel lifecycle.
- **Snapshot trigger policy:** Use a hybrid policy: log-count threshold, wall-clock interval, and state-growth threshold. The policy should be Wendelon-owned and explicit, with `openraft` acting as the execution substrate.
- **Snapshot thresholds:** The real target is bounded replay time, not a numerology contest. Defaults should be benchmark-derived against reference hardware, with replay comfortably under 30 seconds and thresholds adjusted from there. For a control-plane-heavy workload, the likely steady-state default is closer to the tens-of-thousands to low-hundreds-of-thousands of entries range than to ultra-aggressive minute-scale snapshot churn.
- **Checkpoint timing:** Tie checkpoints to snapshot boundaries. One compaction boundary is better than two competing recovery constructs.
- **Snapshot transport and layout:** Use semantic snapshot sections internally, transported as fixed-size content chunks. Large-snapshot chunking is required from the first implementation because a snapshot protocol that assumes "small enough" is already broken.
- **Snapshot compression:** Use `zstd` by default. `lz4` is only justified if profiling proves compression CPU is the dominant bottleneck.

## 4. Tiered Object Storage (L2)
- **Range-read priority:** Range-read support is required for the first real L2 integration. Without it, the object layout and trait shape will optimize for the wrong abstraction and create avoidable rework.
- **`ObjectStore` breaking-change scope:** Batch the trait redesign into one capability-complete break: range reads, streaming put, multipart upload, and listing. Repeated trait churn is architectural sloppiness.
- **Range-read index placement:** Embed a self-describing footer index inside the slab object itself so readers can fetch a small tail range, resolve offsets, and then issue the exact payload read. Object metadata is too constrained and sidecars create split-brain risk. A sidecar cache may exist later, but not as the source of truth.
- **Pending-upload recovery model:** Use a hybrid with a single source of truth. Raft should record authoritative upload intent and terminal outcome; any local journal must be strictly non-authoritative scratch state for node-local retries, temp-file cleanup, and cloud-upload resume metadata. Recovery decisions come from Raft, not from reconciling two peers of equal authority.
- **Orphan cleanup authority:** Make cleanup authority leader-owned. Followers may execute physical deletes if instructed, but the reachability decision should be serialized through the freshest control-plane view.
- **Multipart threshold:** Trigger multipart once slab size crosses the provider's efficient single-PUT envelope, with a nominal starting point around 64 MiB and final tuning by benchmark. If slabs are normally produced at or above that size, use multipart for the whole slab class so the upload path stays uniform instead of branching unpredictably.
- **Remote reclaim scope:** Implement full correctness-critical reclaim now: reachability analysis, delete intents, grace windows, idempotent retries, and crash-safe resume. Archive/retention policy sophistication can wait; correctness cannot.

## 5. Encryption, Key Management, and L2 Crypto
- **Standard cipher:** Standardize on AES-256-GCM-SIV when the validated backend supports it. If it does not, plain AES-256-GCM is acceptable only under a nonce scheme whose uniqueness follows directly from the data model: fresh per-slab DEKs plus chunk-index-derived nonces inside that slab. If that construction cannot be specified and audited cleanly, the feature should block rather than ship on hand-waving.
- **Encryption granularity:** Use per-slab DEKs with independently encrypted fixed-size chunks inside the slab. That preserves range-readability, avoids pathological KMS fan-out, and still supports strong tenant isolation when paired with tenant-aware key hierarchies and placement.
- **Alignment and performance model:** The storage stack should emit fixed-size aligned ciphertext extents. In practice, encryption belongs above logical records but below device-page issuance, with nonce/tag overhead packed into a page-framed codec so `O_DIRECT` never sees misaligned writes.
- **L2 encryption mode:** Client-side envelope encryption is the authoritative and only architectural trust model. Provider SSE-KMS is not part of Wendelon's logical security model; at most it may be enabled as an operator-controlled bucket-level compliance overlay on already encrypted ciphertext, without introducing a second Wendelon key lifecycle.
- **DEK caching policy:** Cache unwrapped DEKs in locked memory with a 5-minute idle TTL, a 30-minute absolute TTL, strict zeroization on eviction, and invalidation on key version change. Anything less disciplined is hand-waving.
- **FIPS compatibility:** In FIPS mode, the crypto path should use the best validated implementation available in the chosen module: AES-256-GCM-SIV if supported, otherwise AES-256-GCM with the documented nonce construction above. If no validated path satisfies the design, FIPS mode is unavailable. No partial promises.
- **Migration scope:** Background re-encryption is in scope. A permanent mixed plaintext/ciphertext fleet is not a perfect system.

## 6. Authorization, Audit, and Compliance
- **Authorization mechanism:** Use mTLS with separate trust domains for nodes and operators, and bind role/identity claims into certificates. Bearer tokens should not be the primary control-plane trust primitive.
- **Pilot authorization scope:** The minimum credible pilot is deny-by-default RBAC on every mutating control-plane RPC, at least one read-only observer role, one operator role, one break-glass admin role, and full audit attribution. Anything narrower is a demo, not a pilot.
- **Audit sink model:** Define a canonical structured audit event model from day one. Persist it locally in append-only files and also support exporter sinks such as syslog/SIEM. The abstraction should be first-class immediately.
- **Audit tamper evidence:** Yes. Every record stream should be hash-chained, and each segment should periodically roll up to a signed digest or anchored root.
- **FIPS target:** The perfection target is full FIPS 140-3 certification for the supported binary/deployment profile, not merely linking against a validated module. Module-only support is an interim state, not the finish line.
- **Rate limiting identity:** Rate limit by authenticated principal identity first, node ID second, and IP only as an abuse backstop. Network address should never be the primary security identity.

## 7. Observability, CLI, and Operator Surface
- **Trace export protocol:** OTLP only. If a shop wants Jaeger, the collector can translate. Native dual-protocol support is wasted surface area.
- **CLI packaging:** Ship a separate admin binary as the official operator interface. The daemon may keep local-maintenance subcommands, but the product-grade admin surface should be independently deployable.
- **CLI minimum surface:** Required for a pilot: `cluster status`, `node list/status`, `cordon`, `drain`, `uncordon`, membership change submission, snapshot status/trigger, repair inspect/trigger, config validate/effective-config, cert/trust inspection, and audit/event query. Nice-to-haves such as interactive shells or report generation can wait.
- **Health endpoint security:** Use two planes: unauthenticated local-only health on loopback or a Unix socket for orchestrator probes, and authenticated remote health for operators. Unauthenticated network-exposed health is an avoidable compromise.
- **Health versus metrics listeners:** Separate them. Health is for liveness semantics; metrics are for observability and often deserve stronger auth and different exposure policy.
- **Metrics hardening boundary:** Solve secure metrics exposure now, not as a deferred cleanup. Observability debt becomes production debt immediately.
- **Observability depth:** Pilot readiness should include end-to-end trace propagation across RPCs, Raft, storage, tiering, KMS, and background jobs, plus structured metrics and logs keyed by stable correlation IDs. Thin observability is incompatible with serious operations.

## 8. Fault Injection and Deterministic Correctness Testing
- **Harness shape:** Both. The simulator is the deterministic truth engine; the process-level chaos harness is the realism check. A perfect validation strategy uses both.
- **Simulator choice:** Given `io_uring` and `quinn`, the best answer is probably a custom deterministic harness with explicit abstractions for clock, network, storage, and external object storage. Existing simulators may still contribute ideas, but they should not dictate the architecture.
- **Storage-path abstraction:** Yes, and it must. `src/storage/io_uring.rs` should sit behind a narrow I/O contract with both real and fault-injectable implementations.
- **Tiering/S3 simulation:** The authoritative fault test path should use a deterministic `ObjectStore` test implementation. A simulated HTTP/S3 endpoint is a secondary compatibility suite, not the core correctness harness.
- **Correctness checker strategy:** Use a real linearizability checker, not just a monotonic-counter approximation. Domain-specific invariants can supplement it, but not replace it.
- **Coverage targets:** CI should run at least 10,000 operations per scenario across multiple seeds and fault schedules. Nightly should run 1,000,000-plus operations across topology, fault, and timing variants.

## 9. Durability, Repair, and Resource Control
- **Durability model:** Implement explicit fsync/fdatasync group commit. Do not make correctness depend on assumed PLP characteristics of deployed hardware.
- **Group-commit tuning:** Use an adaptive window with a 200 microsecond floor, elastic growth up to roughly 2 milliseconds under load, and immediate flush on barriers or administrative commits. Fixed windows are too blunt.
- **Persistent local index:** Use `redb`. It is the cleanest option among the candidates for a correctness-first embedded index. Periodic checkpoint files are too weak, and `sled` is harder to justify today.
- **Repair automation:** Automatic repair should be the default for clearly repairable faults, with policy guards, rate limits, and auditability. Ambiguous cases can still require operator approval.
- **Memory budget enforcement:** Use a hierarchical permit-based budget manager, not a naked global atomic counter. Scarce resources should be reservable, queueable, and observable.
- **Admission-control breadth:** Make the first admission-control layer broad: memory, streams, WAL pressure, compaction, snapshotting, repair, L2 uploads, and KMS concurrency. The system should degrade intentionally, not accidentally.
- **Buffer-path priorities:** Zero-whole-buffer operation is a blocker on the steady-state hot path: network ingress/egress, slab append, L2 upload/download, and snapshot streaming. Offline tooling can tolerate copies longer than the serving path can.

## 10. Multi-Tenancy and Tenant-Aware Policy
- **Pilot requirement:** Yes. Even if the first customer exercises one tenant, the architecture should already be tenant-aware. Retrofitting multi-tenancy later is how products acquire permanent scar tissue.
- **Namespace model:** Use first-class tenant keyspaces, not ad hoc key prefixes. Tenant identity should participate in hash derivation and placement, but isolation should exist as a typed architectural primitive, not a string convention.
- **Tenant identity propagation:** Derive tenant identity from the authenticated connection principal, then bind it into request context. Caller-supplied headers are acceptable only for explicitly authorized delegation flows.
- **Policy enforcement boundary:** Authoritative policy state should live in the control plane / Raft state machine, while the data path enforces compiled local policy caches for low-latency decisions. It is not either/or.
- **Quota enforcement:** Use both. Fast admission checks belong in the data path, and authoritative accounting belongs in Raft-managed state.
- **Tenant lifecycle:** Tenants should be created, suspended, resumed, and deleted exclusively through authenticated control-plane APIs with explicit state transitions and asynchronous garbage collection. Not via static config files.
- **Existing plan alignment:** Follow the core of [plans/ops/multi_tenancy.md](/home/me/work/wendelon/plans/ops/multi_tenancy.md), especially tenant-aware hashing and Raft-managed quota state, but supersede its JWT-centric identity model with certificate-bound or otherwise strongly authenticated principals.
- **Per-tenant topology and isolation:** Yes, tenants should be able to vary replication factor, L2 routing policy, and key-isolation policy. The safe staging is to make the policy schema expressible from the start, even if only a subset is enabled initially.
- **KMS fan-out cost:** Model it explicitly as a scheduling dimension: cache-hit ratio, unwrap latency, context cardinality per batch, and queue contention. Also design the write path to prefer tenant-homogeneous batching so KMS fan-out is minimized by architecture, not merely measured after the fact.
