# Codex Answers to Aggregated Open Questions
**Date:** 2026-04-17
**Source:** [plans/gap/open_questions_20260417.md](/home/me/work/wendelon/plans/gap/open_questions_20260417.md)

This is the lead-architect answer set, optimized for system quality rather than implementation convenience. Where a crisp answer exists, this document makes a decision. Where it does not, it says so plainly.

---

## 1. Integration Gate & Bring-Up
- **Test topology:** The gate of record should run separate daemon processes. In-process nodes are still valuable, but only as a faster secondary suite. The authoritative signal should exercise real process lifecycle, config loading, stderr, signal handling, and kernel networking.
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
- **Snapshot thresholds:** Initial target should be replay time under 10 seconds. A strong starting policy is snapshot at 10,000 entries, 5 minutes, or 64 MiB of logical state delta, whichever comes first. These are defaults, not eternal constants.
- **Checkpoint timing:** Tie checkpoints to snapshot boundaries. One compaction boundary is better than two competing recovery constructs.
- **Snapshot transport and layout:** Use semantic snapshot sections internally, transported as fixed-size content chunks. Large-snapshot chunking is required from the first implementation because a snapshot protocol that assumes "small enough" is already broken.
- **Snapshot compression:** Use `zstd` by default. `lz4` is only justified if profiling proves compression CPU is the dominant bottleneck.

## 4. Tiered Object Storage (L2)
- **Range-read priority:** Range-read support is required for the first real L2 integration. Without it, the object layout and trait shape will optimize for the wrong abstraction and create avoidable rework.
- **`ObjectStore` breaking-change scope:** Batch the trait redesign into one capability-complete break: range reads, streaming put, multipart upload, and listing. Repeated trait churn is architectural sloppiness.
- **Range-read index placement:** Embed a self-describing offset index inside the slab object itself. Object metadata is too constrained and sidecars create split-brain risk. A sidecar cache may exist later, but not as the source of truth.
- **Pending-upload recovery model:** Use a hybrid. Raft should record authoritative upload intent and terminal outcome; a local journal should track node-local execution details needed for idempotent recovery and cleanup.
- **Orphan cleanup authority:** Make cleanup authority leader-owned. Followers may execute physical deletes if instructed, but the reachability decision should be serialized through the freshest control-plane view.
- **Multipart threshold:** Start multipart uploads at 128 MiB, with part sizes chosen to keep retry cost bounded and parallelism useful. The threshold should be below `slab_max_bytes` whenever slabs can exceed single-PUT sweet spots.
- **Remote reclaim scope:** Implement full correctness-critical reclaim now: reachability analysis, delete intents, grace windows, idempotent retries, and crash-safe resume. Archive/retention policy sophistication can wait; correctness cannot.

## 5. Encryption, Key Management, and L2 Crypto
- **Standard cipher:** Standardize on AES-256-GCM. It aligns better with hardware acceleration, enterprise expectations, and FIPS-constrained deployments. Nonce management must be deterministic and structural, not random and hopeful.
- **Encryption granularity:** Encrypt per record. It is the cleanest answer for tenant isolation, selective re-encryption, and future policy richness, even though it is the hardest implementation.
- **Alignment and performance model:** The storage stack should emit fixed-size aligned ciphertext extents. In practice, encryption belongs above logical records but below device-page issuance, with nonce/tag overhead packed into a page-framed codec so `O_DIRECT` never sees misaligned writes.
- **L2 encryption mode:** Support both client-side envelope encryption and provider SSE-KMS, but treat client-side encryption as authoritative. Provider-managed encryption is defense in depth, not the root trust model.
- **DEK caching policy:** Cache unwrapped DEKs in locked memory with a 5-minute idle TTL, a 30-minute absolute TTL, strict zeroization on eviction, and invalidation on key version change. Anything less disciplined is hand-waving.
- **FIPS compatibility:** In FIPS mode, the crypto path should collapse to the AES-256-GCM implementation backed by the validated module. If that path is unavailable, FIPS mode is unavailable. No partial promises.
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
