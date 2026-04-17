# Aggregated Open Questions
**Date:** 2026-04-17

This document consolidates the open questions from the Claude, Codex, and Gemini workstream notes into semantic decision clusters. Near-duplicates are merged, but distinct scope, timing, and implementation questions are preserved so this can serve as the planning agenda for the next pass.

---

## 1. Integration Gate & Bring-Up
- **Test topology:** Should the first real-network integration gate run separate daemon processes or in-process nodes with real QUIC endpoints?
- **Parallel CI port allocation:** How should the CI environment avoid port-binding conflicts when real QUIC endpoints run across parallel test suites?
- **Test certificate strategy:** Should tests generate certs with `rcgen` at runtime or use checked-in fixture certs?
- **L2 config loading timing:** Should L2 config loading land as part of the initial bring-up gate even if tiering is disabled at first, or wait for the L2 workstream?
- **Minimum bring-up observability:** What is the smallest tracing/logging installation that still makes bring-up debuggable: bare `fmt`, or `tracing-subscriber` with env filtering and optional JSON output from day one?

## 2. Immediate Correctness & Safety Fixes
- **Concurrency controls:** What default stream concurrency limit is reasonable given the expected stream-per-write fan-out, and should the first cap be limited to inbound QUIC streams or introduced inside a broader admission-control layer?
- **Deterministic dev secrets:** Is warning text in example config enough, or should example generation stop emitting deterministic secrets more aggressively?
- **Scope of the rapid fix phase:** Which other small-but-severe issues should move into the immediate fix tranche now, including crash-leaked temporary artifacts from interrupted uploads?

## 3. Raft Log, Checkpoint, and Snapshot Lifecycle
- **WAL compaction strategy:** Should WAL compaction use file rotation, hole-punching, or full rewrite, and how do those choices interact with `io_uring`, `O_DIRECT`, and `fallocate`?
- **`openraft` ownership boundary:** Which purge, compaction, and snapshot responsibilities should stay with `openraft` versus explicit Wendelon durability code?
- **Snapshot trigger policy:** Who decides when to take snapshots, and should the policy be driven by log count, time interval, or both?
- **Snapshot thresholds:** What explicit log-count and interval thresholds make sense for a control-plane-heavy workload where each entry is an operational event rather than a data write?
- **Checkpoint timing:** Should checkpoint emission be tied to snapshot boundaries first, or exist as an independent periodic mechanism?
- **Snapshot transport and layout:** Should snapshot chunking use fixed byte ranges or semantic boundaries, and is large-snapshot chunking required for pilot readiness or acceptable as a bounded follow-up?
- **Snapshot compression:** Should snapshots use `zstd`, `lz4`, or no compression initially?

## 4. Tiered Object Storage (L2)
- **Range-read priority:** Is range-read support required for the first production L2 integration, or can correctness closure land first with the interface change immediately after?
- **`ObjectStore` breaking-change scope:** Should range GET, streaming put, multipart upload, and list support land in one coordinated breaking change?
- **Range-read index placement:** Should slab record offsets live in object metadata, a sidecar manifest, or a header embedded in the slab object itself?
- **Pending-upload recovery model:** Should pending uploads be tracked in Raft-managed state, a separate local journal, an additional record type in the existing WAL, or a hybrid?
- **Orphan cleanup authority:** Should orphan GC run leader-only for freshness, or on any node with an explicit quorum/freshness check?
- **Multipart threshold:** At what slab size should multipart uploads begin, and should that threshold align with `slab_max_bytes` or store/provider defaults?
- **Remote reclaim scope:** How much remote object cleanup belongs in this workstream versus a later retention or archival policy pass?

## 5. Encryption, Key Management, and L2 Crypto
- **Standard cipher:** Should the system standardize on AES-256-GCM or XChaCha20-Poly1305?
- **Encryption granularity:** Should encryption happen per record, per page, or per slab?
- **Alignment and performance model:** Should encryption sit above or below aligned I/O buffers, and what alignment or overhead implications does that create under `io_uring` plus `O_DIRECT`?
- **L2 encryption mode:** Should the first tiering implementation use client-side envelope encryption, provider-managed SSE-KMS, or both from the start?
- **DEK caching policy:** How long should unwrapped DEKs remain cached in memory?
- **FIPS compatibility:** If FIPS mode is enabled, is the selected cipher/path supported by the intended validated crypto module?
- **Migration scope:** Is background re-encryption for existing plaintext data in scope for this phase or deferred?

## 6. Authorization, Audit, and Compliance
- **Authorization mechanism:** Should control-plane authorization rely on cert OID extensions, a separate operator CA, token-based auth, or some combination?
- **Pilot authorization scope:** How narrow can the initial control-plane authorization model be while still being credible for a pilot?
- **Audit sink model:** Should audit output start as local append-only/NDJSON files, or should a structured event stream abstraction exist from the start? Should direct syslog/SIEM forwarding ship immediately?
- **Audit tamper evidence:** Should audit records include a hash chain or similar tamper-evident linkage?
- **FIPS target:** Is the goal use of a FIPS-validated crypto module, or full FIPS 140-3 certification of the Wendelon binary?
- **Rate limiting identity:** Should rate limiting be per IP, per node ID, or per connection?

## 7. Observability, CLI, and Operator Surface
- **Trace export protocol:** Should observability support OTLP only, or OTLP plus Jaeger-native export?
- **CLI packaging:** Should the operator CLI ship as a separate admin binary, as a daemon subcommand, or in staged form across both?
- **CLI minimum surface:** Which operator commands are required for the first pilot, and which can wait?
- **Health endpoint security:** Should health/readiness remain unauthenticated for probe compatibility, or require authentication from the start?
- **Health versus metrics listeners:** Should health and metrics share a listener/port or use separate listeners?
- **Metrics hardening boundary:** Should secure metrics exposure be solved fully in this workstream, or partially in the security work with final hardening later?
- **Observability depth:** How much OTEL/tracing coverage is actually required for pilot readiness versus later operational maturity?

## 8. Fault Injection and Deterministic Correctness Testing
- **Harness shape:** Should the first deterministic fault program use a simulator, a process-level chaos harness, or both in stages?
- **Simulator choice:** Which simulator approach has the best fidelity for the current stack: `madsim`, `turmoil`, or a custom framework?
- **Storage-path abstraction:** Can `src/storage/io_uring.rs` be swapped for a simulated backend that injects disk latency, write faults, and partial writes?
- **Tiering/S3 simulation:** Should tiering fault tests use a mocked `ObjectStore` implementation or a simulated HTTP/S3 endpoint?
- **Correctness checker strategy:** Should the project adopt an existing linearizability checker such as Knossos, or begin with a narrower monotonic-counter checker?
- **Coverage targets:** How many operations belong in CI versus nightly fault runs?

## 9. Durability, Repair, and Resource Control
- **Durability model:** Is `O_DIRECT` on PLP NVMe sufficient for target deployments, or must WAL and slab paths implement explicit `fsync` group commit for general-purpose SSDs?
- **Group-commit tuning:** What coalescing window should the system target, and should it be fixed or adaptive?
- **Persistent local index:** Should the persistent index layer use `redb`, `sled`, periodic checkpoint files, or another mechanism?
- **Repair automation:** Should scrubbers only detect and alert, or automatically trigger replica repair? How much repair automation belongs in this phase versus later operational tooling?
- **Memory budget enforcement:** Should resource accounting use a global atomic counter, semaphore-style permits, or another mechanism?
- **Admission-control breadth:** How broad should the first resource-control layer be before it starts interfering with useful bring-up and debugging?
- **Buffer-path priorities:** Which whole-buffer paths are true pilot blockers versus later performance work?

## 10. Multi-Tenancy and Tenant-Aware Policy
- **Pilot requirement:** Is multi-tenancy required for the first real pilot, or should it remain explicitly post-pilot?
- **Namespace model:** Should tenants be isolated by key prefixes or by separate keyspaces / slab-per-tenant routing?
- **Tenant identity propagation:** Should tenant identity come from client cert material, request metadata, or authenticated connection identity?
- **Policy enforcement boundary:** Which tenant policy should be enforced in the control plane, the data path, or the Raft state machine?
- **Quota enforcement:** Should quotas be enforced at data-server admission time, at the Raft state machine, or both?
- **Tenant lifecycle:** How are tenants created, suspended, and deleted?
- **Existing plan alignment:** Should new decisions follow or explicitly supersede [plans/multi_tenancy.md](/home/me/work/wendelon/plans/multi_tenancy.md)?
- **Per-tenant topology and isolation:** Can tenants vary replication factor, L2 routing, or key-isolation policy independently, and if so what staging is safe?
- **KMS fan-out cost:** How should the system model the performance penalty of multiple encryption contexts within a single I/O batch?
