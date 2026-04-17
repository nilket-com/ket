# God-Tier Architectural Resolutions (Revised)
**Date:** 2026-04-17
**Author:** Gemini (Lead Architect)

This document resolves the open questions from the 2026-04-17 planning session. After peer review from Claude and Codex, I have recalibrated "software perfection" from hypothetical future-hardware idealism to rigorous, deployable engineering that operates flawlessly on 2024-era commodity hardware and cloud environments.

---

## 1. Integration Gate & Bring-Up

- **Test topology:** Dual-executor harness. We use in-process nodes (real `quinn::Endpoint`s) for sub-second PR validation, and separate daemons in isolated Linux network namespaces for nightly/release gates to validate signals, config, and `sd_notify`.
- **Parallel CI port allocation:** All listeners bind to port `0`. The OS assigns the port, and the test harness orchestrates peer discovery. Network namespaces are used where complete isolation is required.
- **Test certificate strategy:** Runtime generation via a test-local Rust ACME CA (`rcgen`). Every test gets a fresh PKI, ensuring continuous validation of our certificate parsing and rotation paths.
- **L2 config loading timing:** Immediate. The full TOML schema for L2 is parsed and validated from day one, even if tiering is functionally disabled. This prevents future schema migrations.
- **Minimum bring-up observability:** `tracing-subscriber` with `EnvFilter`, a `fmt` layer (JSON in CI, pretty in TTY), and an OTLP layer slot (initially no-op). We do not ship without structured logging.

## 2. Immediate Correctness & Safety Fixes

- **Concurrency controls:** A comprehensive admission-control framework enforcing budget-based scheduling (memory, streams, WAL pressure). The QUIC inbound stream cap (e.g., 64-256 per peer) is its first enforced policy.
- **Deterministic dev secrets:** Banned. `render_example_config` emits invalid placeholder tokens. The daemon refuses to start until operators explicitly generate keys via `wendelon-ctl init-secrets`.
- **Scope of the rapid fix phase:** All known safety defects—temporary file leaks, snapshot remnants, abandoned upload state—are pulled into the immediate fix tranche. Correctness cannot be deferred.

## 3. Raft Log, Checkpoint, and Snapshot Lifecycle

- **WAL compaction strategy:** Segmented WAL with file rotation. We rotate fixed-size segments and drop segments below the snapshot boundary. This avoids the pitfalls of `fallocate` and `O_DIRECT` interactions on various filesystems.
- **`openraft` ownership boundary:** We use `openraft` for consensus logic and sequencing. Wendelon strictly owns the physical durability layer, fsync ordering, and on-disk layout, honoring all `openraft` callbacks without fighting its invariants.
- **Snapshot trigger policy:** Hybrid policy: log-count threshold, wall-clock interval, and state-growth delta. Whichever triggers first initiates the snapshot.
- **Snapshot thresholds:** Tuned for <10s replay time. Defaults: 10,000 entries, 5 minutes, or 64 MiB of logical state delta.
- **Checkpoint timing:** Checkpoints are tied strictly to snapshot boundaries to unify the recovery constructs.
- **Snapshot transport and layout:** Fixed-size chunked streaming. Semantic boundaries couple transport to state schema; chunking avoids OOM on large state machines.
- **Snapshot compression:** `zstd` with dictionary training for high ratio and decode speed. CPU-efficient compression is sufficient without requiring FPGA offloads.

## 4. Tiered Object Storage (L2)

- **Range-read priority:** Absolutely required for the first L2 release to avoid downloading massive slabs for small point reads.
- **`ObjectStore` breaking-change scope:** One coordinated breaking change incorporating `get_range`, `put_multipart`, `list`, etc.
- **Range-read index placement:** A footer-embedded record offset index within the slab object itself. Sidecars create split-brain consistency risks.
- **Pending-upload recovery model:** Hybrid. Authoritative intent and terminal outcome are recorded in the Raft state machine. Node-local execution details (multipart handles, temp files) are tracked in a local journal for crash-safe idempotent resume.
- **Orphan cleanup authority:** Leader-owned. The leader performs reachability analysis and issues physical deletes; followers do not independently delete remote objects.
- **Multipart threshold:** Aligned with `slab_max_bytes`. This simplifies the upload paths by ensuring the single-PUT vs multipart logic is directly tied to the authoritative slab sizing rather than an arbitrary secondary threshold.
- **Remote reclaim scope:** Full lifecycle reclaim (migration cleanup, tombstone reclamation, orphan GC) is in scope. Correctness requires deletion.

## 5. Encryption, Key Management, and L2 Crypto

- **Standard cipher:** AES-256-GCM-SIV if available via our FIPS provider, otherwise AES-256-GCM with strict, mathematically proven structural nonce derivation to prevent reuse.
- **Encryption granularity:** Per-slab DEK with intra-slab chunked encryption (e.g., 64 KiB chunks). This enables range-GET decryption while keeping KMS fan-out low.
- **Alignment and performance model:** Encryption sits above `AlignedBuf`. Ciphertext is padded to page boundaries to satisfy `O_DIRECT` without corrupting I/O alignment.
- **L2 encryption mode:** Client-side envelope encryption is the authoritative trust model.
- **DEK caching policy:** Unwrapped DEKs are cached in locked memory pages using `ZeroizeOnDrop`, with strict time-based TTLs and LRU eviction under memory pressure.
- **FIPS compatibility:** We depend on a validated module (`aws-lc-rs`). We do not roll our own crypto primitives or attempt to self-certify a Level 4 hardware module.
- **Migration scope:** Background re-encryption of plaintext data is in scope.

## 6. Authorization, Audit, and Compliance

- **Authorization mechanism:** mTLS with identity and role claims bound into certificate OID extensions.
- **Pilot authorization scope:** Deny-by-default RBAC with at least `operator`, `data_node`, and `read_only_observer` roles.
- **Audit sink model:** A first-class structured event stream abstraction (`AuditSink`), supporting local NDJSON and Syslog/SIEM exporters from day one.
- **Audit tamper evidence:** Cryptographic hash chains anchored periodically to a signed root or external timestamping authority.
- **FIPS target:** Use of a FIPS 140-3 validated crypto module for the binary. Full appliance certification is a separate long-term goal.
- **Rate limiting identity:** Rate limiting is enforced first by authenticated principal identity, and only secondarily by IP for pre-auth traffic.

## 7. Observability, CLI, and Operator Surface

- **Trace export protocol:** OTLP only. We do not maintain native Jaeger/Zipkin protocols.
- **CLI packaging:** A standalone, statically compiled native binary (`wendelon-ctl`). We avoid WebAssembly dynamic delivery to prevent supply-chain and version-skew risks.
- **CLI minimum surface:** Core incident-response commands (`cluster-status`, `drain`, `backup`, `restore`, etc.) are required for pilot.
- **Health endpoint security:** Split endpoints: unauthenticated `/health` for liveness, and mTLS-authenticated `/ready` for cluster state.
- **Health versus metrics listeners:** Separate ports for liveness probes (unauthenticated) and metrics scraping (authenticated).
- **Metrics hardening boundary:** Fully solved in this workstream. No insecure defaults.
- **Observability depth:** Full distributed tracing across QUIC RPCs, structured logs, and OpenMetrics export.

## 8. Fault Injection and Deterministic Correctness Testing

- **Harness shape:** Both a process-level chaos harness (for real network/OS fidelity) and a deterministic simulator (for fast, reproducible state exploration).
- **Simulator choice:** A custom deterministic harness explicitly built around `io_uring` and `quinn` abstractions.
- **Storage-path abstraction:** `src/storage/io_uring.rs` sits behind an `AsyncFileOps` trait, allowing the simulator to inject deterministic disk faults.
- **Tiering/S3 simulation:** A mocked `ObjectStore` trait implementation for fast, deterministic tiering tests.
- **Correctness checker strategy:** A linearizability checker (e.g., Elle) for representative subsets, backed by monotonic-counter checkers for broad continuous coverage.
- **Coverage targets:** Millions of operations in nightly CI across varied fault schedules and topologies.

## 9. Durability, Repair, and Resource Control

- **Durability model:** Explicit `fsync`/`fdatasync` group commit. We do not assume PLP NVMe correctness or rely on hypothetical PMEM availability.
- **Group-commit tuning:** Adaptive window (e.g., 200µs floor, 2ms ceiling) scaling with write load.
- **Persistent local index:** `redb` or a carefully chosen embedded database, providing transactional guarantees without inventing a custom storage format.
- **Repair automation:** Automatic replica repair governed by strict cryptographic checksums and rate limits.
- **Memory budget enforcement:** Hierarchical, permit-based budget manager implemented via RAII semaphores.
- **Admission-control breadth:** Comprehensive coverage of memory, streams, WAL pressure, and background tasks.
- **Buffer-path priorities:** Zero-whole-buffer streaming for all steady-state hot paths (L2 reads, snapshot installs).

## 10. Multi-Tenancy and Tenant-Aware Policy

- **Pilot requirement:** Multi-tenancy primitives are baked in from day one to avoid permanent architectural scar tissue.
- **Namespace model:** Separate, first-class per-tenant keyspaces for physical isolation and independent lifecycle management.
- **Tenant identity propagation:** Derived exclusively from the authenticated connection principal (mTLS cert claims).
- **Policy enforcement boundary:** Fast admission checks in the data path, authoritative accounting in the Raft state machine.
- **Quota enforcement:** Both fast-path caching and slow-path authoritative consensus accounting.
- **Tenant lifecycle:** Managed purely through authenticated control-plane APIs and Raft state transitions.
- **Existing plan alignment:** We supersede old JWT-based identity models in favor of mTLS cert-bound principals.
- **Per-tenant topology and isolation:** Policy schemas are expressive enough to support per-tenant replication factors and L2 routing from the start.
- **KMS fan-out cost:** Mitigated architecturally by tenant-homogeneous batching on the write path and DEK caching.ous batching on the write path and DEK caching.